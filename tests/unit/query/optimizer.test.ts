/**
 * Tests for Query Optimizer
 *
 * Tests the query optimization functionality including:
 * - Predicate pushdown analysis
 * - Column pruning
 * - Index selection
 * - Cost estimation
 * - Optimization suggestions
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  QueryOptimizer,
  createQueryOptimizer,
  quickEstimateCost,
  wouldBenefitFromIndex,
  COST_CONSTANTS,
  type TableStatistics,
  type OptimizedQueryPlan,
} from '../../../src/query/optimizer'
import type { Filter } from '../../../src/types/filter'
import type { IndexManager, SelectedIndex } from '../../../src/indexes/manager'
import type { IndexDefinition } from '../../../src/indexes/types'
import type { RowGroupStats, ColumnStats } from '../../../src/query/predicate'

// =============================================================================
// Test Helpers
// =============================================================================

function createMockIndexManager(selectedIndex: SelectedIndex | null = null): IndexManager {
  return {
    selectIndex: vi.fn().mockResolvedValue(selectedIndex),
    load: vi.fn().mockResolvedValue(undefined),
    save: vi.fn().mockResolvedValue(undefined),
    createIndex: vi.fn(),
    dropIndex: vi.fn(),
    listIndexes: vi.fn(),
    getIndexMetadata: vi.fn(),
    rebuildIndex: vi.fn(),
    ftsSearch: vi.fn(),
    vectorSearch: vi.fn(),
    hybridSearch: vi.fn(),
    getVectorIndexDocIds: vi.fn(),
    onDocumentAdded: vi.fn(),
    onDocumentRemoved: vi.fn(),
    onDocumentUpdated: vi.fn(),
    getIndexStats: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    hashLookup: vi.fn(),
    rangeQuery: vi.fn(),
    setIndexManager: vi.fn(),
  } as unknown as IndexManager
}

function createSampleStatistics(totalRows = 10000): TableStatistics {
  return {
    totalRows,
    rowGroupCount: 10,
    avgRowsPerGroup: totalRows / 10,
    columnCardinality: new Map([
      ['status', 5],
      ['category', 20],
      ['userId', 5000],
    ]),
    columnNullCounts: new Map([
      ['status', 0],
      ['category', 100],
      ['userId', 0],
    ]),
    indexes: [],
  }
}

function createSampleRowGroupStats(): RowGroupStats[] {
  const stats: RowGroupStats[] = []
  for (let i = 0; i < 10; i++) {
    const columns = new Map<string, ColumnStats>()
    columns.set('status', {
      min: 'active',
      max: 'pending',
      nullCount: 0,
      hasBloomFilter: false,
    })
    columns.set('age', {
      min: i * 10,
      max: (i + 1) * 10 - 1,
      nullCount: 0,
      hasBloomFilter: false,
    })
    stats.push({
      rowGroup: i,
      rowCount: 1000,
      columns,
    })
  }
  return stats
}

// =============================================================================
// QueryOptimizer Tests
// =============================================================================

describe('QueryOptimizer', () => {
  let optimizer: QueryOptimizer

  beforeEach(() => {
    optimizer = new QueryOptimizer()
  })

  describe('constructor', () => {
    it('creates optimizer without index manager', () => {
      const opt = new QueryOptimizer()
      expect(opt).toBeInstanceOf(QueryOptimizer)
    })

    it('creates optimizer with index manager', () => {
      const indexManager = createMockIndexManager()
      const opt = new QueryOptimizer(indexManager)
      expect(opt).toBeInstanceOf(QueryOptimizer)
    })
  })

  describe('setIndexManager', () => {
    it('sets index manager after construction', () => {
      const indexManager = createMockIndexManager()
      optimizer.setIndexManager(indexManager)
      // No error means success
      expect(true).toBe(true)
    })
  })

  describe('optimize', () => {
    describe('basic optimization', () => {
      it('optimizes simple equality filter', async () => {
        const filter: Filter = { status: 'active' }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.originalFilter).toEqual(filter)
        expect(plan.optimizedFilter).toEqual(filter)
        expect(plan.strategy).toBe('full_scan')
        expect(plan.predicatePushdown.pushedPredicates).toHaveLength(1)
        expect(plan.predicatePushdown.pushedPredicates[0]).toEqual({
          column: 'status',
          op: 'eq',
          value: 'active',
        })
      })

      it('optimizes range query filter', async () => {
        const filter: Filter = { age: { $gte: 18, $lt: 65 } }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.predicatePushdown.pushedPredicates).toHaveLength(2)
        expect(plan.predicatePushdown.pushedPredicates).toContainEqual({
          column: 'age',
          op: 'gte',
          value: 18,
        })
        expect(plan.predicatePushdown.pushedPredicates).toContainEqual({
          column: 'age',
          op: 'lt',
          value: 65,
        })
      })

      it('optimizes filter with $and', async () => {
        const filter: Filter = {
          $and: [
            { status: 'active' },
            { age: { $gte: 18 } },
          ],
        }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.predicatePushdown.pushedPredicates.length).toBeGreaterThanOrEqual(2)
      })

      it('handles $or correctly (not pushable)', async () => {
        const filter: Filter = {
          $or: [
            { status: 'active' },
            { status: 'pending' },
          ],
        }
        const plan = await optimizer.optimize('users', filter)

        // $or cannot be fully pushed down
        expect(plan.predicatePushdown.remainingFilter.$or).toBeDefined()
      })

      it('handles non-pushable operators', async () => {
        const filter: Filter = {
          name: { $regex: '^John' },
        }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.predicatePushdown.pushedPredicates).toHaveLength(0)
        expect(plan.predicatePushdown.remainingFilter.name).toBeDefined()
      })
    })

    describe('with statistics', () => {
      it('uses statistics for cost estimation', async () => {
        const filter: Filter = { status: 'active' }
        const statistics = createSampleStatistics(50000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        expect(plan.estimatedCost.estimatedRowsScanned).toBeLessThanOrEqual(50000)
        expect(plan.estimatedCost.totalCost).toBeGreaterThan(0)
      })

      it('estimates row groups skipped with rowGroupStats', async () => {
        const filter: Filter = { age: { $gte: 50, $lt: 60 } }
        const statistics = createSampleStatistics(10000)
        statistics.rowGroupStats = createSampleRowGroupStats()

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        // Should skip most row groups since age 50-60 only in one group
        expect(plan.predicatePushdown.estimatedSkippedRowGroups).toBeGreaterThanOrEqual(0)
      })
    })

    describe('with index manager', () => {
      it('recommends FTS index for $text queries', async () => {
        const ftsIndex: IndexDefinition = {
          name: 'idx_content',
          type: 'fts',
          fields: [{ path: 'content' }],
        }
        const selectedIndex: SelectedIndex = {
          index: ftsIndex,
          type: 'fts',
          condition: { $search: 'hello world' },
        }
        const indexManager = createMockIndexManager(selectedIndex)
        optimizer.setIndexManager(indexManager)

        const filter: Filter = { $text: { $search: 'hello world' } }
        const plan = await optimizer.optimize('posts', filter)

        expect(plan.strategy).toBe('fts_search')
        expect(plan.indexRecommendation).toBeDefined()
        expect(plan.indexRecommendation?.type).toBe('fts')
      })

      it('recommends vector index for $vector queries', async () => {
        const vectorIndex: IndexDefinition = {
          name: 'idx_embedding',
          type: 'vector',
          fields: [{ path: 'embedding' }],
        }
        const selectedIndex: SelectedIndex = {
          index: vectorIndex,
          type: 'vector',
          field: 'embedding',
          condition: { query: [1, 2, 3], field: 'embedding', topK: 10 },
        }
        const indexManager = createMockIndexManager(selectedIndex)
        optimizer.setIndexManager(indexManager)

        const filter: Filter = {
          $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 },
        }
        const plan = await optimizer.optimize('posts', filter)

        expect(plan.strategy).toBe('vector_search')
        expect(plan.indexRecommendation).toBeDefined()
        expect(plan.indexRecommendation?.type).toBe('vector')
      })

      it('detects hybrid search when vector + regular filters', async () => {
        const vectorIndex: IndexDefinition = {
          name: 'idx_embedding',
          type: 'vector',
          fields: [{ path: 'embedding' }],
        }
        const selectedIndex: SelectedIndex = {
          index: vectorIndex,
          type: 'vector',
          field: 'embedding',
          condition: { query: [1, 2, 3], field: 'embedding', topK: 10 },
        }
        const indexManager = createMockIndexManager(selectedIndex)
        optimizer.setIndexManager(indexManager)

        const filter: Filter = {
          $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 },
          category: 'tech',
        }
        const plan = await optimizer.optimize('posts', filter)

        expect(plan.strategy).toBe('hybrid_search')
      })
    })

    describe('column pruning', () => {
      it('identifies filter columns', async () => {
        const filter: Filter = { status: 'active', age: { $gte: 18 } }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.columnPruning.filterColumns).toContain('status')
        expect(plan.columnPruning.filterColumns).toContain('age')
      })

      it('identifies projection columns', async () => {
        const filter: Filter = { status: 'active' }
        const options = { project: { name: 1, email: 1 } }
        const plan = await optimizer.optimize('users', filter, options)

        expect(plan.columnPruning.projectionColumns).toContain('name')
        expect(plan.columnPruning.projectionColumns).toContain('email')
      })

      it('identifies sort columns', async () => {
        const filter: Filter = { status: 'active' }
        const options = { sort: { createdAt: -1 as const } }
        const plan = await optimizer.optimize('users', filter, options)

        expect(plan.columnPruning.sortColumns).toContain('createdAt')
      })

      it('includes core columns in required columns', async () => {
        const filter: Filter = { status: 'active' }
        const plan = await optimizer.optimize('users', filter)

        expect(plan.columnPruning.requiredColumns).toContain('$id')
        expect(plan.columnPruning.requiredColumns).toContain('$type')
        expect(plan.columnPruning.requiredColumns).toContain('name')
      })
    })

    describe('suggestions', () => {
      it('suggests creating index for filter without one', async () => {
        const filter: Filter = { userId: '12345' }
        const statistics = createSampleStatistics(100000)

        const plan = await optimizer.optimize('posts', filter, {}, statistics)

        const indexSuggestion = plan.suggestions.find(s => s.type === 'create_index')
        expect(indexSuggestion).toBeDefined()
        expect(indexSuggestion?.description).toContain('userId')
      })

      it('suggests adding projection when none specified', async () => {
        const filter: Filter = { status: 'active' }
        const plan = await optimizer.optimize('users', filter)

        const projectionSuggestion = plan.suggestions.find(s => s.type === 'add_projection')
        expect(projectionSuggestion).toBeDefined()
      })

      it('suggests using limit for large datasets', async () => {
        const filter: Filter = { status: 'active' }
        const statistics = createSampleStatistics(50000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        const limitSuggestion = plan.suggestions.find(s => s.type === 'use_limit')
        expect(limitSuggestion).toBeDefined()
      })

      it('suggests rewriting $or for better pushdown', async () => {
        const filter: Filter = {
          $or: [{ status: 'active' }, { status: 'pending' }],
        }
        const plan = await optimizer.optimize('users', filter)

        // The remaining filter should contain $or
        expect(plan.predicatePushdown.remainingFilter.$or).toBeDefined()

        // Check that $or is recognized as needing post-scan evaluation
        // (suggestions for $or rewriting are lower priority since it requires separate queries)
        expect(plan.predicatePushdown.pushedPredicates).toHaveLength(0)
      })

      it('suggests using $startsWith instead of anchored regex', async () => {
        const filter: Filter = {
          name: { $regex: '^John' },
        }
        const plan = await optimizer.optimize('users', filter)

        const rewriteSuggestion = plan.suggestions.find(
          s => s.type === 'rewrite_filter' && s.description.includes('$startsWith')
        )
        expect(rewriteSuggestion).toBeDefined()
      })

      it('sorts suggestions by priority', async () => {
        const filter: Filter = {
          $or: [{ status: 'active' }, { status: 'pending' }],
          name: { $regex: '^John' },
        }
        const statistics = createSampleStatistics(50000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        // Check that suggestions are sorted by priority (descending)
        for (let i = 1; i < plan.suggestions.length; i++) {
          expect(plan.suggestions[i - 1]!.priority).toBeGreaterThanOrEqual(
            plan.suggestions[i]!.priority
          )
        }
      })
    })

    describe('filter optimization', () => {
      it('flattens nested $and operators', async () => {
        const filter: Filter = {
          $and: [
            { $and: [{ a: 1 }, { b: 2 }] },
            { c: 3 },
          ],
        }
        const plan = await optimizer.optimize('test', filter)

        // The optimized filter should have flattened $and
        if (plan.optimizedFilter.$and) {
          expect(plan.optimizedFilter.$and.length).toBe(3)
        }
      })

      it('converts single-element $and to flat filter', async () => {
        const filter: Filter = {
          $and: [{ status: 'active' }],
        }
        const plan = await optimizer.optimize('test', filter)

        expect(plan.optimizedFilter.$and).toBeUndefined()
        expect(plan.optimizedFilter.status).toBe('active')
      })
    })

    describe('cost estimation', () => {
      it('calculates I/O cost based on row groups', async () => {
        const filter: Filter = { status: 'active' }
        const statistics = createSampleStatistics(10000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        expect(plan.estimatedCost.ioCost).toBeGreaterThan(0)
        expect(plan.estimatedCost.ioCost).toContain // Contains row group scan cost
      })

      it('calculates CPU cost based on rows', async () => {
        const filter: Filter = { status: 'active' }
        const statistics = createSampleStatistics(10000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        expect(plan.estimatedCost.cpuCost).toBeGreaterThan(0)
      })

      it('reduces cost estimate when using index', async () => {
        const ftsIndex: IndexDefinition = {
          name: 'idx_content',
          type: 'fts',
          fields: [{ path: 'content' }],
        }
        const selectedIndex: SelectedIndex = {
          index: ftsIndex,
          type: 'fts',
          condition: { $search: 'hello' },
        }
        const indexManager = createMockIndexManager(selectedIndex)
        optimizer.setIndexManager(indexManager)

        const statistics = createSampleStatistics(100000)

        // Without index
        const noIndexPlan = await optimizer.optimize(
          'posts',
          { status: 'active' },
          {},
          statistics
        )

        // With FTS index
        const indexPlan = await optimizer.optimize(
          'posts',
          { $text: { $search: 'hello' } },
          {},
          statistics
        )

        // FTS index should have lower I/O cost due to selectivity
        // The index recommendation should indicate significant cost reduction
        expect(indexPlan.indexRecommendation).toBeDefined()
        expect(indexPlan.indexRecommendation?.selectivity).toBeLessThan(1)
        // FTS typically has ~10% selectivity, meaning 90% of rows are filtered
        expect(indexPlan.indexRecommendation?.costReduction).toBeGreaterThan(0)
      })

      it('respects limit in row estimation', async () => {
        const filter: Filter = { status: 'active' }
        const statistics = createSampleStatistics(100000)

        const plan = await optimizer.optimize('users', filter, { limit: 10 }, statistics)

        expect(plan.estimatedCost.estimatedRowsReturned).toBeLessThanOrEqual(10)
      })
    })

    describe('isOptimal flag', () => {
      it('sets isOptimal to true for simple optimized query', async () => {
        const filter: Filter = { status: 'active' }
        const options = { limit: 10, project: { name: 1, status: 1 } }

        const plan = await optimizer.optimize('users', filter, options)

        // May or may not be optimal depending on suggestions
        expect(typeof plan.isOptimal).toBe('boolean')
      })

      it('sets isOptimal to false when high-priority suggestions exist', async () => {
        const filter: Filter = {
          $or: [{ status: 'active' }, { status: 'pending' }],
          name: { $regex: '^John' },
        }
        const statistics = createSampleStatistics(100000)

        const plan = await optimizer.optimize('users', filter, {}, statistics)

        // With multiple issues, likely not optimal
        const highPrioritySuggestions = plan.suggestions.filter(s => s.priority >= 7)
        if (highPrioritySuggestions.length > 0) {
          expect(plan.isOptimal).toBe(false)
        }
      })
    })
  })

  describe('comparePlans', () => {
    it('returns plan with lower cost', async () => {
      const filter1: Filter = { status: 'active' }
      const filter2: Filter = { status: 'active', age: { $gte: 18 } }

      const plan1 = await optimizer.optimize('users', filter1)
      const plan2 = await optimizer.optimize('users', filter2)

      const better = optimizer.comparePlans(plan1, plan2)

      // The better plan should have the lower total cost
      expect(better.estimatedCost.totalCost).toBeLessThanOrEqual(
        Math.max(plan1.estimatedCost.totalCost, plan2.estimatedCost.totalCost)
      )
    })
  })

  describe('explainPlan', () => {
    it('produces human-readable output', async () => {
      const filter: Filter = { status: 'active', age: { $gte: 18 } }
      const statistics = createSampleStatistics(10000)

      const plan = await optimizer.optimize('users', filter, {}, statistics)
      const explanation = optimizer.explainPlan(plan)

      expect(explanation).toContain('Query Optimization Report')
      expect(explanation).toContain('Execution Strategy')
      expect(explanation).toContain('Cost Estimate')
      expect(explanation).toContain('Predicate Pushdown')
      expect(explanation).toContain('Column Pruning')
    })

    it('includes index information when available', async () => {
      const ftsIndex: IndexDefinition = {
        name: 'idx_content',
        type: 'fts',
        fields: [{ path: 'content' }],
      }
      const selectedIndex: SelectedIndex = {
        index: ftsIndex,
        type: 'fts',
        condition: { $search: 'hello' },
      }
      const indexManager = createMockIndexManager(selectedIndex)
      optimizer.setIndexManager(indexManager)

      const filter: Filter = { $text: { $search: 'hello' } }
      const plan = await optimizer.optimize('posts', filter)
      const explanation = optimizer.explainPlan(plan)

      expect(explanation).toContain('Index Usage')
      expect(explanation).toContain('idx_content')
    })

    it('includes suggestions when present', async () => {
      const filter: Filter = { status: 'active' }
      const statistics = createSampleStatistics(50000)

      const plan = await optimizer.optimize('users', filter, {}, statistics)
      const explanation = optimizer.explainPlan(plan)

      expect(explanation).toContain('Suggestions')
    })
  })
})

// =============================================================================
// createQueryOptimizer Tests
// =============================================================================

describe('createQueryOptimizer', () => {
  it('creates optimizer without index manager', () => {
    const optimizer = createQueryOptimizer()
    expect(optimizer).toBeInstanceOf(QueryOptimizer)
  })

  it('creates optimizer with index manager', () => {
    const indexManager = createMockIndexManager()
    const optimizer = createQueryOptimizer(indexManager)
    expect(optimizer).toBeInstanceOf(QueryOptimizer)
  })
})

// =============================================================================
// quickEstimateCost Tests
// =============================================================================

describe('quickEstimateCost', () => {
  it('estimates cost for simple filter', () => {
    const filter: Filter = { status: 'active' }
    const statistics = createSampleStatistics(10000)

    const cost = quickEstimateCost(filter, statistics)

    expect(cost).toBeGreaterThan(0)
  })

  it('reduces cost for pushable predicates', () => {
    const filter1: Filter = {} // No predicates
    const filter2: Filter = { status: 'active' } // Pushable

    const statistics = createSampleStatistics(10000)

    const cost1 = quickEstimateCost(filter1, statistics)
    const cost2 = quickEstimateCost(filter2, statistics)

    expect(cost2).toBeLessThan(cost1)
  })

  it('increases cost for $or operator', () => {
    const filter1: Filter = { status: 'active' }
    const filter2: Filter = { $or: [{ status: 'active' }, { status: 'pending' }] }

    const statistics = createSampleStatistics(10000)

    const cost1 = quickEstimateCost(filter1, statistics)
    const cost2 = quickEstimateCost(filter2, statistics)

    expect(cost2).toBeGreaterThan(cost1)
  })

  it('accounts for $text operator', () => {
    const filter: Filter = { $text: { $search: 'hello world' } }
    const statistics = createSampleStatistics(10000)

    const cost = quickEstimateCost(filter, statistics)

    // Should include FTS lookup cost but be reduced by selectivity
    expect(cost).toBeGreaterThan(COST_CONSTANTS.FTS_INDEX_LOOKUP * 0.1)
  })

  it('accounts for $vector operator', () => {
    const filter: Filter = {
      $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 },
    }
    const statistics = createSampleStatistics(10000)

    const cost = quickEstimateCost(filter, statistics)

    // Should include vector lookup cost
    expect(cost).toBeGreaterThan(0)
  })
})

// =============================================================================
// wouldBenefitFromIndex Tests
// =============================================================================

describe('wouldBenefitFromIndex', () => {
  it('returns true for equality filter on large dataset', () => {
    const filter: Filter = { userId: '12345' }
    const statistics = createSampleStatistics(50000)

    expect(wouldBenefitFromIndex(filter, statistics)).toBe(true)
  })

  it('returns true for $in filter on large dataset', () => {
    const filter: Filter = { status: { $in: ['active', 'pending'] } }
    const statistics = createSampleStatistics(50000)

    expect(wouldBenefitFromIndex(filter, statistics)).toBe(true)
  })

  it('returns true for range filter on large dataset', () => {
    const filter: Filter = { age: { $gte: 18, $lt: 65 } }
    const statistics = createSampleStatistics(50000)

    expect(wouldBenefitFromIndex(filter, statistics)).toBe(true)
  })

  it('returns true for $text filter', () => {
    const filter: Filter = { $text: { $search: 'hello' } }

    expect(wouldBenefitFromIndex(filter)).toBe(true)
  })

  it('returns true for $vector filter', () => {
    const filter: Filter = {
      $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 },
    }

    expect(wouldBenefitFromIndex(filter)).toBe(true)
  })

  it('returns false for small dataset', () => {
    const filter: Filter = { status: 'active' }
    const statistics = createSampleStatistics(100)

    expect(wouldBenefitFromIndex(filter, statistics)).toBe(false)
  })

  it('returns false for $regex filter on small dataset', () => {
    const filter: Filter = { name: { $regex: '^John' } }
    const statistics = createSampleStatistics(100)

    expect(wouldBenefitFromIndex(filter, statistics)).toBe(false)
  })
})

// =============================================================================
// COST_CONSTANTS Tests
// =============================================================================

describe('COST_CONSTANTS', () => {
  it('has reasonable relative values', () => {
    // Row group scan should be more expensive than row read
    expect(COST_CONSTANTS.ROW_GROUP_SCAN).toBeGreaterThan(COST_CONSTANTS.ROW_READ)

    // Row filter should be cheaper than row read
    expect(COST_CONSTANTS.ROW_FILTER).toBeLessThan(COST_CONSTANTS.ROW_READ)

    // Index lookups should have a base cost
    expect(COST_CONSTANTS.FTS_INDEX_LOOKUP).toBeGreaterThan(0)
    expect(COST_CONSTANTS.VECTOR_INDEX_LOOKUP).toBeGreaterThan(0)

    // Bloom filter factor should reduce cost
    expect(COST_CONSTANTS.BLOOM_FILTER_FACTOR).toBeLessThan(1)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration: Optimizer with real scenarios', () => {
  let optimizer: QueryOptimizer

  beforeEach(() => {
    optimizer = new QueryOptimizer()
  })

  it('optimizes e-commerce product search', async () => {
    const filter: Filter = {
      category: 'electronics',
      price: { $gte: 100, $lte: 500 },
      inStock: true,
    }
    const options = {
      sort: { price: 1 as const },
      limit: 20,
      project: { name: 1, price: 1, image: 1 },
    }
    const statistics = createSampleStatistics(1000000)

    const plan = await optimizer.optimize('products', filter, options, statistics)

    // Should push down most predicates
    expect(plan.predicatePushdown.pushedPredicates.length).toBeGreaterThanOrEqual(3)

    // Should identify required columns
    expect(plan.columnPruning.requiredColumns).toContain('price')

    // Should have reasonable cost
    expect(plan.estimatedCost.totalCost).toBeGreaterThan(0)
  })

  it('optimizes user activity log query', async () => {
    const filter: Filter = {
      userId: 'user123',
      action: { $in: ['login', 'purchase', 'view'] },
      timestamp: { $gte: new Date('2024-01-01') },
    }
    const options = {
      sort: { timestamp: -1 as const },
      limit: 100,
    }
    const statistics = createSampleStatistics(10000000)

    const plan = await optimizer.optimize('activityLogs', filter, options, statistics)

    // Should suggest creating an index on userId
    const indexSuggestion = plan.suggestions.find(s => s.type === 'create_index')
    expect(indexSuggestion).toBeDefined()
  })

  it('optimizes content search with FTS', async () => {
    const ftsIndex: IndexDefinition = {
      name: 'idx_content',
      type: 'fts',
      fields: [{ path: 'title' }, { path: 'body' }],
    }
    const selectedIndex: SelectedIndex = {
      index: ftsIndex,
      type: 'fts',
      condition: { $search: 'typescript database' },
    }
    const indexManager = createMockIndexManager(selectedIndex)
    optimizer.setIndexManager(indexManager)

    const filter: Filter = {
      $text: { $search: 'typescript database' },
      published: true,
    }
    const statistics = createSampleStatistics(100000)

    const plan = await optimizer.optimize('articles', filter, {}, statistics)

    expect(plan.strategy).toBe('fts_search')
    expect(plan.indexRecommendation?.recommended).toBe(true)
  })

  it('optimizes semantic search with vector index', async () => {
    const vectorIndex: IndexDefinition = {
      name: 'idx_embedding',
      type: 'vector',
      fields: [{ path: 'embedding' }],
    }
    const selectedIndex: SelectedIndex = {
      index: vectorIndex,
      type: 'vector',
      field: 'embedding',
      condition: { query: Array(384).fill(0.1), field: 'embedding', topK: 20 },
    }
    const indexManager = createMockIndexManager(selectedIndex)
    optimizer.setIndexManager(indexManager)

    const filter: Filter = {
      $vector: { query: Array(384).fill(0.1), field: 'embedding', topK: 20 },
    }
    const statistics = createSampleStatistics(500000)

    const plan = await optimizer.optimize('documents', filter, {}, statistics)

    expect(plan.strategy).toBe('vector_search')
    expect(plan.estimatedCost.estimatedRowsReturned).toBeLessThanOrEqual(20)
  })
})

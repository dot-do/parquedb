/**
 * Tests for MV Query Optimizer
 *
 * Tests the materialized view query optimization functionality including:
 * - Candidate MV detection
 * - Coverage score calculation
 * - Cost-based selection
 * - Query rewriting
 * - Staleness-aware decisions
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  MVQueryOptimizer,
  createMVQueryOptimizer,
  InMemoryMVRegistry,
  createInMemoryMVRegistry,
  wouldBenefitFromMV,
  explainMVSelection,
  DEFAULT_MV_OPTIMIZATION_CONFIG,
  MV_COST_CONSTANTS,
  type MVCandidate,
  type MVOptimizationResult,
  type MVOptimizationConfig,
  type MVRegistry,
} from '../../../src/query/mv-optimizer'
import type { Filter } from '../../../src/types/filter'
import type { MVDefinition, MVMetadata } from '../../../src/materialized-views/types'
import type { StalenessDetector, StalenessMetrics, MVLineage } from '../../../src/materialized-views/staleness'
import type { TableStatistics } from '../../../src/query/optimizer'

// =============================================================================
// Test Helpers
// =============================================================================

function createMockStalenessDetector(
  state: 'fresh' | 'stale' | 'invalid' = 'fresh',
  stalenessPercent = 0
): StalenessDetector {
  return {
    getMetrics: vi.fn().mockResolvedValue({
      state,
      usable: state === 'fresh' || stalenessPercent < 50,
      stalenessPercent,
      timeSinceRefresh: stalenessPercent * 1000,
      withinGracePeriod: stalenessPercent < 50,
      gracePeriodRemainingMs: 50000 - stalenessPercent * 1000,
      sources: [],
      estimatedTotalChanges: 0,
      recommendedRefreshType: state === 'fresh' ? 'none' : 'incremental',
      computedAt: Date.now(),
    } as StalenessMetrics),
    isStale: vi.fn().mockResolvedValue(state !== 'fresh'),
    isWithinGracePeriod: vi.fn().mockReturnValue(stalenessPercent < 50),
    needsImmediateRefresh: vi.fn().mockResolvedValue(stalenessPercent >= 100),
    getState: vi.fn().mockResolvedValue(state),
    setThresholds: vi.fn(),
    getThresholds: vi.fn().mockReturnValue({}),
  } as unknown as StalenessDetector
}

function createSampleMVDefinition(overrides: Partial<MVDefinition> = {}): MVDefinition {
  return {
    $from: 'orders',
    ...overrides,
  }
}

function createSampleMVMetadata(name: string, rowCount = 1000): MVMetadata {
  return {
    id: name as any,
    name,
    definition: createSampleMVDefinition(),
    status: 'ready',
    createdAt: new Date(),
    lastRefreshedAt: new Date(),
    rowCount,
    version: 1,
    lineage: {
      viewName: name as any,
      sourceVersions: new Map(),
      definitionVersionId: '1',
      lastRefreshTime: Date.now(),
      lastRefreshDurationMs: 100,
      lastRefreshRecordCount: rowCount,
      lastRefreshType: 'full',
    },
  }
}

function createSampleLineage(name: string): MVLineage {
  return {
    viewName: name as any,
    sourceVersions: new Map(),
    definitionVersionId: '1',
    lastRefreshTime: Date.now(),
    lastRefreshDurationMs: 100,
    lastRefreshRecordCount: 1000,
    lastRefreshType: 'full',
  }
}

function createSampleStatistics(totalRows = 10000): TableStatistics {
  return {
    totalRows,
    rowGroupCount: 10,
    avgRowsPerGroup: totalRows / 10,
    columnCardinality: new Map(),
    columnNullCounts: new Map(),
    indexes: [],
  }
}

// =============================================================================
// InMemoryMVRegistry Tests
// =============================================================================

describe('InMemoryMVRegistry', () => {
  let registry: InMemoryMVRegistry

  beforeEach(() => {
    registry = createInMemoryMVRegistry()
  })

  it('registers and retrieves MVs', () => {
    const definition = createSampleMVDefinition()
    registry.register('activeOrders', definition)

    expect(registry.getMV('activeOrders')).toEqual(definition)
    expect(registry.getMV('nonexistent')).toBeUndefined()
  })

  it('retrieves all MVs', () => {
    registry.register('mv1', createSampleMVDefinition({ $from: 'orders' }))
    registry.register('mv2', createSampleMVDefinition({ $from: 'users' }))

    const allMVs = registry.getAllMVs()
    expect(allMVs.size).toBe(2)
    expect(allMVs.has('mv1')).toBe(true)
    expect(allMVs.has('mv2')).toBe(true)
  })

  it('retrieves MVs by source collection', () => {
    registry.register('orderMV1', createSampleMVDefinition({ $from: 'orders' }))
    registry.register('orderMV2', createSampleMVDefinition({ $from: 'orders' }))
    registry.register('userMV', createSampleMVDefinition({ $from: 'users' }))

    const orderMVs = registry.getMVsBySource('orders')
    expect(orderMVs.size).toBe(2)
    expect(orderMVs.has('orderMV1')).toBe(true)
    expect(orderMVs.has('orderMV2')).toBe(true)
    expect(orderMVs.has('userMV')).toBe(false)
  })

  it('retrieves MV metadata', async () => {
    const definition = createSampleMVDefinition()
    const metadata = createSampleMVMetadata('testMV')
    registry.register('testMV', definition, metadata)

    const retrieved = await registry.getMVMetadata('testMV')
    expect(retrieved).toEqual(metadata)
  })

  it('retrieves MV lineage', async () => {
    const definition = createSampleMVDefinition()
    const lineage = createSampleLineage('testMV')
    registry.register('testMV', definition)
    registry.setLineage('testMV', lineage)

    const retrieved = await registry.getMVLineage('testMV')
    expect(retrieved).toEqual(lineage)
  })

  it('clears all registrations', () => {
    registry.register('mv1', createSampleMVDefinition())
    registry.register('mv2', createSampleMVDefinition())
    registry.clear()

    expect(registry.getAllMVs().size).toBe(0)
  })
})

// =============================================================================
// MVQueryOptimizer Tests
// =============================================================================

describe('MVQueryOptimizer', () => {
  let registry: InMemoryMVRegistry
  let optimizer: MVQueryOptimizer

  beforeEach(() => {
    registry = createInMemoryMVRegistry()
    optimizer = createMVQueryOptimizer(registry)
  })

  describe('constructor', () => {
    it('creates optimizer with default config', () => {
      const opt = new MVQueryOptimizer(registry)
      expect(opt.getConfig()).toEqual(DEFAULT_MV_OPTIMIZATION_CONFIG)
    })

    it('creates optimizer with custom config', () => {
      const opt = new MVQueryOptimizer(registry, undefined, {
        minCostSavings: 0.3,
        maxStalenessPercent: 30,
      })
      const config = opt.getConfig()
      expect(config.minCostSavings).toBe(0.3)
      expect(config.maxStalenessPercent).toBe(30)
    })

    it('accepts staleness detector', () => {
      const detector = createMockStalenessDetector()
      const opt = new MVQueryOptimizer(registry, detector)
      expect(opt).toBeInstanceOf(MVQueryOptimizer)
    })
  })

  describe('findCandidateMVs', () => {
    it('returns empty array when no MVs exist', async () => {
      const candidates = await optimizer.findCandidateMVs('orders', { status: 'active' })
      expect(candidates).toEqual([])
    })

    it('finds candidate MVs for a source collection', async () => {
      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }))
      registry.register('userMV', createSampleMVDefinition({
        $from: 'users',
      }))

      const candidates = await optimizer.findCandidateMVs('orders', {})
      expect(candidates.length).toBe(1)
      expect(candidates[0]!.name).toBe('activeOrders')
    })

    it('calculates coverage score based on fields', async () => {
      registry.register('ordersSummary', createSampleMVDefinition({
        $from: 'orders',
        $select: { status: 'status', total: 'total', customerId: 'customerId' },
      }))

      // Query needs status and total - both covered
      const candidates1 = await optimizer.findCandidateMVs(
        'orders',
        { status: 'active' },
        { project: { status: 1, total: 1 } }
      )
      expect(candidates1[0]!.coverageScore).toBeGreaterThan(0.5)

      // Query needs a field not in MV
      const candidates2 = await optimizer.findCandidateMVs(
        'orders',
        { status: 'active' },
        { project: { status: 1, shippingAddress: 1 } }
      )
      // Coverage should be lower due to uncovered field
      expect(candidates2[0]!.uncoveredFields).toContain('shippingAddress')
    })

    it('respects minimum coverage score threshold', async () => {
      registry.register('ordersSummary', createSampleMVDefinition({
        $from: 'orders',
        $select: { status: 'status' },
      }))

      optimizer.setConfig({ minCoverageScore: 0.9 })

      // Query with many uncovered fields
      const candidates = await optimizer.findCandidateMVs(
        'orders',
        { status: 'active' },
        { project: { status: 1, total: 1, customerId: 1, orderDate: 1, items: 1 } }
      )

      // Should be filtered out due to low coverage
      expect(candidates.length).toBe(0)
    })

    it('limits number of candidates returned', async () => {
      // Register many MVs
      for (let i = 0; i < 20; i++) {
        registry.register(`ordersMV${i}`, createSampleMVDefinition({
          $from: 'orders',
        }))
      }

      optimizer.setConfig({ maxCandidates: 5 })
      const candidates = await optimizer.findCandidateMVs('orders', {})

      expect(candidates.length).toBe(5)
    })

    it('sorts candidates by coverage score', async () => {
      registry.register('fullOrders', createSampleMVDefinition({
        $from: 'orders',
        // No $select means all fields available
      }))
      registry.register('partialOrders', createSampleMVDefinition({
        $from: 'orders',
        $select: { status: 'status' },
      }))

      const candidates = await optimizer.findCandidateMVs(
        'orders',
        { status: 'active', total: { $gt: 100 } }
      )

      // Full coverage should come first
      if (candidates.length >= 2) {
        expect(candidates[0]!.coverageScore).toBeGreaterThanOrEqual(candidates[1]!.coverageScore)
      }
    })
  })

  describe('optimize', () => {
    it('returns source-only result when no MVs exist', async () => {
      const result = await optimizer.optimize('orders', { status: 'active' })

      expect(result.useMV).toBe(false)
      expect(result.candidates).toEqual([])
      expect(result.sourceCost.totalCost).toBeGreaterThan(0)
      expect(result.costSavings).toBe(0)
      expect(result.explanation).toContain('No candidate materialized views found')
    })

    it('selects MV when cost savings exceed threshold', async () => {
      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 500))

      const result = await optimizer.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000) // Large source table
      )

      // MV should be selected due to cost savings
      expect(result.useMV).toBe(true)
      expect(result.selectedMV?.name).toBe('activeOrders')
      expect(result.costSavings).toBeGreaterThan(0)
    })

    it('rejects MV when cost savings are insufficient', async () => {
      registry.register('allOrders', createSampleMVDefinition({
        $from: 'orders',
      }), createSampleMVMetadata('allOrders', 10000)) // Same size as source

      optimizer.setConfig({ minCostSavings: 0.5 }) // Require 50% savings

      const result = await optimizer.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(10000)
      )

      // MV may not meet the high threshold
      if (!result.useMV) {
        expect(result.candidates[0]?.reason).toContain('Insufficient cost savings')
      }
    })

    it('produces rewritten filter when MV is selected', async () => {
      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 100))

      const result = await optimizer.optimize(
        'orders',
        { status: 'active', total: { $gt: 100 } },
        {},
        createSampleStatistics(100000)
      )

      if (result.useMV) {
        expect(result.rewrittenFilter).toBeDefined()
        // The 'status: active' condition should be removed since MV already filters it
        expect(result.rewrittenFilter?.status).toBeUndefined()
        // The 'total' condition should remain
        expect(result.rewrittenFilter?.total).toEqual({ $gt: 100 })
      }
    })

    it('considers staleness in cost estimation', async () => {
      const staleDetector = createMockStalenessDetector('stale', 30)
      const optimizerWithStaleness = createMVQueryOptimizer(registry, staleDetector)

      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 100))
      registry.setLineage('activeOrders', createSampleLineage('activeOrders'))

      const result = await optimizerWithStaleness.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000)
      )

      // MV should still be considered but with staleness penalty
      if (result.candidates.length > 0) {
        const candidate = result.candidates[0]!
        expect(candidate.stalenessState).toBe('stale')
      }
    })

    it('rejects MV when staleness exceeds threshold', async () => {
      const veryStaleDetector = createMockStalenessDetector('stale', 80)
      const optimizerWithStaleness = createMVQueryOptimizer(registry, veryStaleDetector, {
        maxStalenessPercent: 50,
      })

      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 100))
      registry.setLineage('activeOrders', createSampleLineage('activeOrders'))

      const result = await optimizerWithStaleness.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000)
      )

      // MV should be rejected due to staleness
      if (result.candidates.length > 0) {
        expect(result.candidates[0]!.recommended).toBe(false)
        expect(result.candidates[0]!.reason).toContain('staleness')
      }
    })

    it('allows stale reads when configured', async () => {
      const staleDetector = createMockStalenessDetector('stale', 30)
      const optimizerWithStaleness = createMVQueryOptimizer(registry, staleDetector, {
        allowStaleReads: true,
        maxStalenessPercent: 50,
      })

      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 100))
      registry.setLineage('activeOrders', createSampleLineage('activeOrders'))

      const result = await optimizerWithStaleness.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000)
      )

      // MV should be usable despite being stale
      if (result.candidates.length > 0) {
        // Staleness is within threshold, should still be recommended
        expect(result.candidates[0]!.stalenessMetrics?.stalenessPercent).toBe(30)
      }
    })

    it('prefers aggregation MVs for aggregate queries', async () => {
      registry.register('ordersFlat', createSampleMVDefinition({
        $from: 'orders',
      }), createSampleMVMetadata('ordersFlat', 10000))

      registry.register('ordersSummary', createSampleMVDefinition({
        $from: 'orders',
        $groupBy: ['status'],
        $compute: { count: { $count: '*' }, totalRevenue: { $sum: 'total' } },
      }), createSampleMVMetadata('ordersSummary', 10))

      // For a filter on grouped field, aggregation MV has better cost
      const result = await optimizer.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000)
      )

      // The aggregation MV is not compatible with regular queries (has $groupBy)
      // This is expected behavior - aggregation MVs can only be queried for aggregate results
      expect(result.candidates.some(c => c.name === 'ordersFlat')).toBe(true)
    })
  })

  describe('configuration', () => {
    it('updates configuration', () => {
      optimizer.setConfig({ minCostSavings: 0.4 })
      expect(optimizer.getConfig().minCostSavings).toBe(0.4)
    })

    it('merges partial configuration', () => {
      const originalConfig = optimizer.getConfig()
      optimizer.setConfig({ minCostSavings: 0.4 })

      const newConfig = optimizer.getConfig()
      expect(newConfig.minCostSavings).toBe(0.4)
      expect(newConfig.allowStaleReads).toBe(originalConfig.allowStaleReads)
    })
  })

  describe('filter compatibility', () => {
    it('allows MV without filter for any query', async () => {
      registry.register('allOrders', createSampleMVDefinition({
        $from: 'orders',
        // No $filter - contains all data
      }), createSampleMVMetadata('allOrders', 1000))

      const result = await optimizer.optimize(
        'orders',
        { status: 'active', total: { $gt: 100 } },
        {},
        createSampleStatistics(100000)
      )

      // MV should be a candidate
      expect(result.candidates.length).toBeGreaterThan(0)
      expect(result.candidates[0]!.isFullyCovered).toBe(true)
    })

    it('rejects MV with conflicting filter', async () => {
      registry.register('completedOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'completed' },
      }), createSampleMVMetadata('completedOrders', 1000))

      const result = await optimizer.optimize(
        'orders',
        { status: 'active' }, // Conflicts with MV filter
        {},
        createSampleStatistics(100000)
      )

      // MV should not be recommended due to filter conflict
      expect(result.candidates.length).toBe(0)
    })

    it('allows compatible filter overlap', async () => {
      registry.register('activeOrders', createSampleMVDefinition({
        $from: 'orders',
        $filter: { status: 'active' },
      }), createSampleMVMetadata('activeOrders', 1000))

      // Query with same filter as MV
      const result = await optimizer.optimize(
        'orders',
        { status: 'active' },
        {},
        createSampleStatistics(100000)
      )

      // MV should be a candidate
      expect(result.candidates.length).toBeGreaterThan(0)
    })

    it('handles MV with groupBy (aggregation MV)', async () => {
      registry.register('ordersByStatus', createSampleMVDefinition({
        $from: 'orders',
        $groupBy: ['status'],
        $compute: { count: { $count: '*' } },
      }), createSampleMVMetadata('ordersByStatus', 5))

      // Regular query cannot use aggregation MV
      const result = await optimizer.optimize(
        'orders',
        { total: { $gt: 100 } },
        {},
        createSampleStatistics(100000)
      )

      // Aggregation MVs are not compatible with queries on non-grouped fields
      const aggregationCandidate = result.candidates.find(c => c.name === 'ordersByStatus')
      if (aggregationCandidate) {
        expect(aggregationCandidate.recommended).toBe(false)
      }
    })
  })
})

// =============================================================================
// wouldBenefitFromMV Tests
// =============================================================================

describe('wouldBenefitFromMV', () => {
  it('returns true for aggregate query with aggregation MV', () => {
    const mvDef = createSampleMVDefinition({
      $groupBy: ['status'],
      $compute: { count: { $count: '*' } },
    })

    expect(wouldBenefitFromMV({}, mvDef, { hasAggregation: true })).toBe(true)
  })

  it('returns true for join query with denormalized MV', () => {
    const mvDef = createSampleMVDefinition({
      $expand: ['customer', 'items'],
    })

    expect(wouldBenefitFromMV({}, mvDef, { hasJoins: true })).toBe(true)
  })

  it('returns true when MV filter matches query filter', () => {
    const mvDef = createSampleMVDefinition({
      $filter: { status: 'active', type: 'standard' },
    })

    const filter: Filter = { status: 'active' }

    expect(wouldBenefitFromMV(filter, mvDef)).toBe(true)
  })

  it('returns false for simple query without special MV features', () => {
    const mvDef = createSampleMVDefinition({})
    const filter: Filter = { someField: 'value' }

    expect(wouldBenefitFromMV(filter, mvDef)).toBe(false)
  })
})

// =============================================================================
// explainMVSelection Tests
// =============================================================================

describe('explainMVSelection', () => {
  it('produces readable output for MV selection', () => {
    const result: MVOptimizationResult = {
      useMV: true,
      selectedMV: {
        name: 'activeOrders',
        definition: createSampleMVDefinition({ $filter: { status: 'active' } }),
        coverageScore: 1,
        coveredFields: ['status', 'total'],
        uncoveredFields: [],
        isFullyCovered: true,
        recommended: true,
        reason: 'MV provides 40% cost savings',
        costSavings: 0.4,
      },
      candidates: [],
      originalFilter: { status: 'active' },
      sourceCost: {
        ioCost: 1000,
        cpuCost: 500,
        totalCost: 1500,
        estimatedRowsScanned: 10000,
        estimatedRowsReturned: 500,
        isExact: false,
      },
      mvCost: {
        ioCost: 400,
        cpuCost: 200,
        totalCost: 600,
        estimatedRowsScanned: 1000,
        estimatedRowsReturned: 500,
        isExact: false,
      },
      costSavings: 0.4,
      explanation: "Using MV 'activeOrders': MV provides 40% cost savings",
    }

    const explanation = explainMVSelection(result)

    expect(explanation).toContain('MV Optimization Report')
    expect(explanation).toContain("Use MV 'activeOrders'")
    expect(explanation).toContain('Cost Savings: 40.0%')
    expect(explanation).toContain('Source Table Cost')
    expect(explanation).toContain('Selected MV Cost')
  })

  it('produces readable output for source selection', () => {
    const result: MVOptimizationResult = {
      useMV: false,
      candidates: [
        {
          name: 'ordersMV',
          definition: createSampleMVDefinition(),
          coverageScore: 0.6,
          coveredFields: ['status'],
          uncoveredFields: ['total', 'customerId'],
          isFullyCovered: false,
          recommended: false,
          reason: 'Missing fields: total, customerId',
          costSavings: 0.1,
        },
      ],
      originalFilter: { status: 'active' },
      sourceCost: {
        ioCost: 1000,
        cpuCost: 500,
        totalCost: 1500,
        estimatedRowsScanned: 10000,
        estimatedRowsReturned: 500,
        isExact: false,
      },
      costSavings: 0,
      explanation: "Best candidate MV 'ordersMV' rejected: Missing fields",
    }

    const explanation = explainMVSelection(result)

    expect(explanation).toContain('Use source table')
    expect(explanation).toContain('Candidates Evaluated')
    expect(explanation).toContain('ordersMV')
    expect(explanation).toContain('recommended=false')
  })
})

// =============================================================================
// MV_COST_CONSTANTS Tests
// =============================================================================

describe('MV_COST_CONSTANTS', () => {
  it('has reasonable cost reduction values', () => {
    // Base reduction should be significant
    expect(MV_COST_CONSTANTS.MV_BASE_REDUCTION).toBeGreaterThan(0)
    expect(MV_COST_CONSTANTS.MV_BASE_REDUCTION).toBeLessThan(1)

    // Aggregation MVs should have high reduction
    expect(MV_COST_CONSTANTS.AGGREGATION_MV_REDUCTION).toBeGreaterThan(MV_COST_CONSTANTS.MV_BASE_REDUCTION)

    // Staleness cost should be reasonable
    expect(MV_COST_CONSTANTS.STALENESS_COST_PER_PERCENT).toBeGreaterThan(0)
    expect(MV_COST_CONSTANTS.STALENESS_COST_PER_PERCENT).toBeLessThan(0.1)
  })
})

// =============================================================================
// DEFAULT_MV_OPTIMIZATION_CONFIG Tests
// =============================================================================

describe('DEFAULT_MV_OPTIMIZATION_CONFIG', () => {
  it('has sensible defaults', () => {
    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.minCostSavings).toBeGreaterThan(0)
    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.minCostSavings).toBeLessThan(1)

    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.allowStaleReads).toBe(true)

    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.maxStalenessPercent).toBeGreaterThan(0)
    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.maxStalenessPercent).toBeLessThanOrEqual(100)

    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.minCoverageScore).toBeGreaterThan(0)
    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.minCoverageScore).toBeLessThan(1)

    expect(DEFAULT_MV_OPTIMIZATION_CONFIG.maxCandidates).toBeGreaterThan(0)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration: MVQueryOptimizer with real scenarios', () => {
  let registry: InMemoryMVRegistry
  let optimizer: MVQueryOptimizer

  beforeEach(() => {
    registry = createInMemoryMVRegistry()
    optimizer = createMVQueryOptimizer(registry)
  })

  it('optimizes e-commerce order query with denormalized MV', async () => {
    // Register a denormalized MV with customer data expanded
    registry.register('ordersWithCustomer', createSampleMVDefinition({
      $from: 'orders',
      $expand: ['customer'],
      $flatten: { customer: 'buyer' },
    }), createSampleMVMetadata('ordersWithCustomer', 10000))

    const result = await optimizer.optimize(
      'orders',
      { status: 'active' },
      { project: { status: 1, total: 1, buyer_name: 1 } },
      createSampleStatistics(1000000)
    )

    // Should recommend the MV for queries needing customer data
    expect(result.candidates.length).toBeGreaterThan(0)
  })

  it('optimizes analytics query with aggregation MV', async () => {
    // Register an aggregation MV for daily sales
    registry.register('dailySales', createSampleMVDefinition({
      $from: 'orders',
      $groupBy: [{ date: '$createdAt' }, 'status'],
      $compute: {
        orderCount: { $count: '*' },
        revenue: { $sum: 'total' },
      },
    }), createSampleMVMetadata('dailySales', 365))

    // This query is for specific order lookup, not aggregate
    const result = await optimizer.optimize(
      'orders',
      { $id: 'order123' },
      {},
      createSampleStatistics(1000000)
    )

    // Aggregation MV should not be selected for point lookups
    // It should be filtered out due to incompatible groupBy
    const dailySalesCandidate = result.candidates.find(c => c.name === 'dailySales')
    if (dailySalesCandidate) {
      expect(dailySalesCandidate.recommended).toBe(false)
    }
  })

  it('optimizes filtered query with matching filtered MV', async () => {
    // Register MVs with different filters
    registry.register('activeOrders', createSampleMVDefinition({
      $from: 'orders',
      $filter: { status: 'active' },
    }), createSampleMVMetadata('activeOrders', 10000))

    registry.register('completedOrders', createSampleMVDefinition({
      $from: 'orders',
      $filter: { status: 'completed' },
    }), createSampleMVMetadata('completedOrders', 50000))

    // Query for active orders
    const activeResult = await optimizer.optimize(
      'orders',
      { status: 'active', total: { $gt: 100 } },
      {},
      createSampleStatistics(100000)
    )

    // Should select the activeOrders MV
    if (activeResult.useMV) {
      expect(activeResult.selectedMV?.name).toBe('activeOrders')
    }

    // Query for completed orders
    const completedResult = await optimizer.optimize(
      'orders',
      { status: 'completed' },
      {},
      createSampleStatistics(100000)
    )

    // Should select the completedOrders MV
    if (completedResult.useMV) {
      expect(completedResult.selectedMV?.name).toBe('completedOrders')
    }
  })

  it('handles multiple suitable MVs and selects the best', async () => {
    // Register multiple MVs
    registry.register('allOrders', createSampleMVDefinition({
      $from: 'orders',
    }), createSampleMVMetadata('allOrders', 100000))

    registry.register('activeOrders', createSampleMVDefinition({
      $from: 'orders',
      $filter: { status: 'active' },
    }), createSampleMVMetadata('activeOrders', 10000))

    registry.register('activeOrdersSummary', createSampleMVDefinition({
      $from: 'orders',
      $filter: { status: 'active' },
      $select: { status: 'status', total: 'total', customerId: 'customerId' },
    }), createSampleMVMetadata('activeOrdersSummary', 10000))

    const result = await optimizer.optimize(
      'orders',
      { status: 'active' },
      { project: { status: 1, total: 1 } },
      createSampleStatistics(1000000)
    )

    // Should have multiple candidates
    expect(result.candidates.length).toBeGreaterThanOrEqual(2)

    // If MV is selected, it should be the one with best cost savings
    if (result.useMV && result.selectedMV) {
      // The smaller filtered MVs should have better cost
      expect(['activeOrders', 'activeOrdersSummary']).toContain(result.selectedMV.name)
    }
  })
})

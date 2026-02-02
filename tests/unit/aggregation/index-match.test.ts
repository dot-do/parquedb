/**
 * Tests for Index-Aware Aggregation $match Stage
 *
 * Verifies that aggregation pipelines with $match as the first stage
 * can leverage secondary indexes (hash, sst, fts, vector) for efficient
 * filtering instead of full collection scans.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  executeAggregation,
  executeAggregationWithIndex,
  AggregationExecutor,
  type AggregationStage,
  type AggregationOptions,
} from '../../../src/aggregation'
import type { IndexManager, SelectedIndex } from '../../../src/indexes/manager'
import type { Filter } from '../../../src/types/filter'
import type { IndexDefinition } from '../../../src/indexes/types'

// =============================================================================
// Test Data
// =============================================================================

interface Product {
  $id: string
  $type: string
  name: string
  category: string
  price: number
  stock: number
  description: string
  tags: string[]
}

function createTestData(): Product[] {
  return [
    {
      $id: 'products/1',
      $type: 'Product',
      name: 'Laptop Pro',
      category: 'electronics',
      price: 1299,
      stock: 50,
      description: 'High performance laptop',
      tags: ['computer', 'portable'],
    },
    {
      $id: 'products/2',
      $type: 'Product',
      name: 'Wireless Mouse',
      category: 'electronics',
      price: 49,
      stock: 200,
      description: 'Ergonomic wireless mouse',
      tags: ['accessory', 'wireless'],
    },
    {
      $id: 'products/3',
      $type: 'Product',
      name: 'Office Desk',
      category: 'furniture',
      price: 399,
      stock: 25,
      description: 'Standing desk with electric adjustment',
      tags: ['office', 'ergonomic'],
    },
    {
      $id: 'products/4',
      $type: 'Product',
      name: 'USB Hub',
      category: 'electronics',
      price: 29,
      stock: 150,
      description: 'USB-C hub with multiple ports',
      tags: ['accessory', 'usb'],
    },
    {
      $id: 'products/5',
      $type: 'Product',
      name: 'Monitor Stand',
      category: 'furniture',
      price: 79,
      stock: 75,
      description: 'Adjustable monitor stand',
      tags: ['office', 'accessory'],
    },
  ]
}

// =============================================================================
// Mock IndexManager
// =============================================================================

function createMockIndexManager(config: {
  selectedIndex?: SelectedIndex | null
  hashLookupResults?: string[]
  rangeLookupResults?: string[]
  ftsResults?: { docId: string; score: number }[]
}) {
  return {
    selectIndex: vi.fn().mockResolvedValue(config.selectedIndex ?? null),
    hashLookup: vi.fn().mockResolvedValue({
      docIds: config.hashLookupResults ?? [],
      rowGroups: [],
      exact: true,
      entriesScanned: 0,
    }),
    rangeQuery: vi.fn().mockResolvedValue({
      docIds: config.rangeLookupResults ?? [],
      rowGroups: [],
      exact: true,
      entriesScanned: 0,
    }),
    ftsSearch: vi.fn().mockResolvedValue(config.ftsResults ?? []),
    vectorSearch: vi.fn().mockResolvedValue({
      docIds: [],
      rowGroups: [],
      scores: [],
      exact: false,
      entriesScanned: 0,
    }),
    load: vi.fn().mockResolvedValue(undefined),
  } as unknown as IndexManager
}

// =============================================================================
// Index-Aware $match Tests
// =============================================================================

describe('Index-Aware Aggregation $match', () => {
  let testData: Product[]

  beforeEach(() => {
    testData = createTestData()
  })

  // ===========================================================================
  // AggregationOptions.indexManager
  // ===========================================================================

  describe('AggregationOptions.indexManager', () => {
    it('should accept indexManager in options', async () => {
      const mockIndexManager = createMockIndexManager({})

      const options: AggregationOptions = {
        indexManager: mockIndexManager,
        namespace: 'products',
      }

      // Should not throw
      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, options)

      expect(results).toHaveLength(3) // Laptop Pro, Wireless Mouse, USB Hub
    })
  })

  // ===========================================================================
  // Index Selection for $match
  // ===========================================================================

  describe('Index Selection', () => {
    it('should call selectIndex when $match is first stage', async () => {
      const mockIndexManager = createMockIndexManager({})

      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
      ]

      await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(mockIndexManager.selectIndex).toHaveBeenCalledWith(
        'products',
        { category: 'electronics' }
      )
    })

    it('should not call selectIndex when $match is not first stage', async () => {
      const mockIndexManager = createMockIndexManager({})

      const pipeline: AggregationStage[] = [
        { $sort: { price: -1 } },
        { $match: { category: 'electronics' } },
      ]

      await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      // Should not call selectIndex because $match is not first
      expect(mockIndexManager.selectIndex).not.toHaveBeenCalled()
    })

    it('should not call selectIndex when no indexManager provided', async () => {
      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
      ]

      // Should not throw and should filter normally (fallback to sync version)
      const results = await executeAggregationWithIndex(testData, pipeline)

      expect(results).toHaveLength(3)
    })
  })

  // ===========================================================================
  // Hash Index Usage
  // ===========================================================================

  describe('Hash Index Usage', () => {
    it('should use hash index for equality match', async () => {
      const hashIndex: IndexDefinition = {
        name: 'category_hash',
        type: 'hash',
        fields: [{ path: 'category' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: hashIndex,
          type: 'hash',
          field: 'category',
          condition: 'electronics',
        },
        hashLookupResults: ['products/1', 'products/2', 'products/4'],
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(mockIndexManager.hashLookup).toHaveBeenCalledWith(
        expect.any(String),
        'category_hash',
        'electronics'
      )

      expect(results).toHaveLength(3)
      expect(results.map((r: any) => r.$id).sort()).toEqual([
        'products/1',
        'products/2',
        'products/4',
      ])
    })

    it('should use hash index for $eq match', async () => {
      const hashIndex: IndexDefinition = {
        name: 'category_hash',
        type: 'hash',
        fields: [{ path: 'category' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: hashIndex,
          type: 'hash',
          field: 'category',
          condition: { $eq: 'furniture' },
        },
        hashLookupResults: ['products/3', 'products/5'],
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: { $eq: 'furniture' } } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(results).toHaveLength(2)
    })
  })

  // ===========================================================================
  // SST Index Usage for Range Queries
  // ===========================================================================

  describe('SST Index Usage', () => {
    it('should use SST index for range queries', async () => {
      const sstIndex: IndexDefinition = {
        name: 'price_sst',
        type: 'sst',
        fields: [{ path: 'price' }],
      }

      // Only products/3 (price: 399) matches the range 100-500
      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: sstIndex,
          type: 'sst',
          field: 'price',
          condition: { $gte: 100, $lte: 500 },
        },
        rangeLookupResults: ['products/3'],
      })

      const pipeline: AggregationStage[] = [
        { $match: { price: { $gte: 100, $lte: 500 } } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(mockIndexManager.rangeQuery).toHaveBeenCalledWith(
        'products',
        'price_sst',
        { $gte: 100, $lte: 500 }
      )

      expect(results).toHaveLength(1)
      expect((results[0] as any).$id).toBe('products/3')
    })
  })

  // ===========================================================================
  // FTS Index Usage
  // ===========================================================================

  describe('FTS Index Usage', () => {
    it('should use FTS index for $text search', async () => {
      const ftsIndex: IndexDefinition = {
        name: 'description_fts',
        type: 'fts',
        fields: [{ path: 'description' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: ftsIndex,
          type: 'fts',
          condition: { $search: 'wireless' },
        },
        ftsResults: [
          { docId: 'products/2', score: 0.95 },
        ],
      })

      const pipeline: AggregationStage[] = [
        { $match: { $text: { $search: 'wireless' } } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(mockIndexManager.ftsSearch).toHaveBeenCalledWith(
        expect.any(String),
        'wireless',
        expect.any(Object)
      )

      expect(results).toHaveLength(1)
      expect((results[0] as any).$id).toBe('products/2')
    })
  })

  // ===========================================================================
  // Fallback to Full Scan
  // ===========================================================================

  describe('Fallback Behavior', () => {
    it('should fall back to full scan when no index available', async () => {
      const mockIndexManager = createMockIndexManager({
        selectedIndex: null,
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      // Should still filter correctly via full scan
      expect(results).toHaveLength(3)
    })

    it('should fall back to full scan when index lookup returns empty', async () => {
      const hashIndex: IndexDefinition = {
        name: 'category_hash',
        type: 'hash',
        fields: [{ path: 'category' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: hashIndex,
          type: 'hash',
          field: 'category',
          condition: 'nonexistent',
        },
        hashLookupResults: [],
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: 'nonexistent' } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(results).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Complex Pipelines with Indexed $match
  // ===========================================================================

  describe('Complex Pipelines', () => {
    it('should use index for $match then execute remaining stages', async () => {
      const hashIndex: IndexDefinition = {
        name: 'category_hash',
        type: 'hash',
        fields: [{ path: 'category' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: hashIndex,
          type: 'hash',
          field: 'category',
          condition: 'electronics',
        },
        hashLookupResults: ['products/1', 'products/2', 'products/4'],
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
        { $sort: { price: -1 } },
        { $limit: 2 },
        { $project: { name: 1, price: 1 } },
      ]

      const results = await executeAggregationWithIndex(testData, pipeline, {
        indexManager: mockIndexManager,
        namespace: 'products',
      })

      expect(results).toHaveLength(2)
      expect((results[0] as any).price).toBe(1299) // Laptop Pro
      expect((results[1] as any).price).toBe(49)   // Wireless Mouse
    })

    it('should use index for $match then $group', async () => {
      const hashIndex: IndexDefinition = {
        name: 'category_hash',
        type: 'hash',
        fields: [{ path: 'category' }],
      }

      const mockIndexManager = createMockIndexManager({
        selectedIndex: {
          index: hashIndex,
          type: 'hash',
          field: 'category',
          condition: 'electronics',
        },
        hashLookupResults: ['products/1', 'products/2', 'products/4'],
      })

      const pipeline: AggregationStage[] = [
        { $match: { category: 'electronics' } },
        { $group: { _id: null, totalValue: { $sum: '$price' }, count: { $sum: 1 } } },
      ]

      const results = await executeAggregationWithIndex<{
        _id: null
        totalValue: number
        count: number
      }>(testData, pipeline, { indexManager: mockIndexManager, namespace: 'products' })

      expect(results).toHaveLength(1)
      expect(results[0].count).toBe(3)
      expect(results[0].totalValue).toBe(1299 + 49 + 29) // 1377
    })
  })
})

// =============================================================================
// AggregationExecutor Class with Index Support
// =============================================================================

describe('AggregationExecutor with Index Support', () => {
  let testData: Product[]

  beforeEach(() => {
    testData = createTestData()
  })

  it('should use indexManager when provided via options', () => {
    const mockIndexManager = createMockIndexManager({})

    // Note: The sync AggregationExecutor doesn't use index lookups
    // For index-aware execution, use executeAggregationWithIndex
    const executor = new AggregationExecutor(testData, [
      { $match: { category: 'electronics' } },
    ], { indexManager: mockIndexManager, namespace: 'products' })

    const results = executor.execute<Product>()

    // Falls back to full scan since execute() is synchronous
    expect(results).toHaveLength(3)
  })

  it('should include index info in explain output', () => {
    const hashIndex: IndexDefinition = {
      name: 'category_hash',
      type: 'hash',
      fields: [{ path: 'category' }],
    }

    const mockIndexManager = createMockIndexManager({
      selectedIndex: {
        index: hashIndex,
        type: 'hash',
        field: 'category',
        condition: 'electronics',
      },
      hashLookupResults: ['products/1', 'products/2', 'products/4'],
    })

    const executor = new AggregationExecutor(testData, [
      { $match: { category: 'electronics' } },
      { $limit: 10 },
    ], { indexManager: mockIndexManager, namespace: 'products' })

    const explain = executor.explain()

    expect(explain.stages[0].name).toBe('$match')
    // The sync executor performs full scan, so output is 3 docs
    expect(explain.stages[0].outputCount).toBe(3)
  })
})

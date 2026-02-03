/**
 * Tests for QueryExecutor MV Routing Integration
 *
 * Tests that the QueryExecutor properly integrates with the MVRouter
 * to route queries to Materialized Views when appropriate.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  QueryExecutor,
  type ParquetReader,
  type ParquetMetadata,
  type BloomFilterReader,
} from '../../../src/query/executor'
import {
  MVRouter,
  InMemoryMVMetadataProvider,
  createMVRouter,
  createInMemoryMVMetadataProvider,
  type MVRoutingMetadata,
} from '../../../src/query/optimizer'
import type { StorageBackend } from '../../../src/types/storage'
import type { MVDefinition } from '../../../src/materialized-views/types'

// =============================================================================
// Mock ParquetReader
// =============================================================================

function createMockReader(data: Record<string, unknown[]> = {}): ParquetReader {
  return {
    readMetadata: vi.fn().mockImplementation(async (path: string) => {
      const ns = path.split('/')[1] // Extract namespace from path
      const rows = data[ns!] ?? []
      return {
        schema: {},
        rowGroups: rows.length > 0
          ? [{ numRows: rows.length, columns: [] }]
          : [],
        keyValueMetadata: [],
      } as unknown as ParquetMetadata
    }),

    readRowGroups: vi.fn().mockImplementation(async <T>(path: string, rowGroups: number[]) => {
      const ns = path.split('/')[1]
      const rows = data[ns!] ?? []
      return rows as T[]
    }),

    readAll: vi.fn().mockImplementation(async <T>(path: string) => {
      const ns = path.split('/')[1]
      const rows = data[ns!] ?? []
      return rows as T[]
    }),

    getBloomFilter: vi.fn().mockResolvedValue(null as BloomFilterReader | null),
  }
}

// =============================================================================
// Mock Storage Backend
// =============================================================================

function createMockStorage(): StorageBackend {
  return {
    read: vi.fn().mockResolvedValue(new Uint8Array()),
    write: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    exists: vi.fn().mockResolvedValue(false),
    list: vi.fn().mockResolvedValue([]),
    readText: vi.fn().mockResolvedValue(''),
    readJson: vi.fn().mockResolvedValue(null),
    writeText: vi.fn().mockResolvedValue(undefined),
    writeJson: vi.fn().mockResolvedValue(undefined),
  } as unknown as StorageBackend
}

// =============================================================================
// Helper Functions
// =============================================================================

function createMVMetadata(
  name: string,
  definition: MVDefinition,
  options: {
    stalenessState?: 'fresh' | 'stale' | 'invalid'
    usable?: boolean
    rowCount?: number
  } = {}
): MVRoutingMetadata {
  return {
    name,
    definition,
    stalenessState: options.stalenessState ?? 'fresh',
    usable: options.usable ?? true,
    rowCount: options.rowCount,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('QueryExecutor MV Routing Integration', () => {
  let provider: InMemoryMVMetadataProvider
  let router: MVRouter
  let storage: StorageBackend

  beforeEach(() => {
    provider = createInMemoryMVMetadataProvider()
    router = createMVRouter(provider)
    storage = createMockStorage()
  })

  describe('setMVRouter', () => {
    it('allows setting the MV router', () => {
      const reader = createMockReader()
      const executor = new QueryExecutor(reader, storage)

      // Should not throw
      expect(() => executor.setMVRouter(router)).not.toThrow()
    })
  })

  describe('execute with MV routing', () => {
    it('routes query to MV when MV is available and can satisfy the query', async () => {
      // Setup: MV with completed orders
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 200 },
          { $id: '3', status: 'completed', amount: 300 },
        ],
        CompletedOrders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '3', status: 'completed', amount: 300 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register MV for completed orders
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }
      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      // Execute query
      const result = await executor.execute('orders', { status: 'completed' })

      // Should use MV
      expect(result.stats.indexUsed).toBe('mv:CompletedOrders')
      expect(result.rows).toHaveLength(2)
    })

    it('falls back to source when no MV is available', async () => {
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 200 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // No MV registered
      const result = await executor.execute('orders', { status: 'completed' })

      // Should not use MV
      expect(result.stats.indexUsed).toBeUndefined()
    })

    it('falls back to source when MV is stale and unusable', async () => {
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
        StaleOrders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register stale MV
      const mvDef: MVDefinition = {
        $from: 'orders',
      }
      provider.registerMV(createMVMetadata('StaleOrders', mvDef, {
        stalenessState: 'stale',
        usable: false, // Outside grace period
      }))

      const result = await executor.execute('orders', {})

      // Should not use stale MV
      expect(result.stats.indexUsed).toBeUndefined()
    })

    it('uses stale MV when still usable (within grace period)', async () => {
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
        AllOrders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register stale but usable MV
      const mvDef: MVDefinition = {
        $from: 'orders',
      }
      provider.registerMV(createMVMetadata('AllOrders', mvDef, {
        stalenessState: 'stale',
        usable: true, // Within grace period
      }))

      const result = await executor.execute('orders', {})

      // Should use the MV despite being stale
      expect(result.stats.indexUsed).toBe('mv:AllOrders')
    })

    it('applies post-filter when MV data needs additional filtering', async () => {
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 200 },
          { $id: '3', status: 'completed', amount: 300 },
        ],
        AllOrders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 200 },
          { $id: '3', status: 'completed', amount: 300 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register MV without filter
      const mvDef: MVDefinition = {
        $from: 'orders',
      }
      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      // Query with filter - MV has all data, needs post-filtering
      const result = await executor.execute('orders', { status: 'completed' })

      // Should use MV with post-filter
      expect(result.stats.indexUsed).toBe('mv:AllOrders')
      expect(result.rows).toHaveLength(2) // Only completed orders
    })

    it('applies sort and limit options', async () => {
      const sourceData = {
        AllOrders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 300 },
          { $id: '3', status: 'completed', amount: 200 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      const mvDef: MVDefinition = {
        $from: 'orders',
      }
      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await executor.execute('orders', {}, {
        sort: { amount: -1 },
        limit: 2,
      })

      expect(result.stats.indexUsed).toBe('mv:AllOrders')
      expect(result.rows).toHaveLength(2)
      expect((result.rows[0] as { amount: number }).amount).toBe(300)
    })

    it('rejects aggregation MV for regular query', async () => {
      const sourceData = {
        orders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
        OrderStats: [
          { status: 'completed', count: 1, total: 100 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register aggregation MV
      const mvDef: MVDefinition = {
        $from: 'orders',
        $groupBy: ['status'],
        $compute: {
          count: { $count: '*' },
          total: { $sum: 'amount' },
        },
      }
      provider.registerMV(createMVMetadata('OrderStats', mvDef))

      const result = await executor.execute('orders', { status: 'completed' })

      // Should not use aggregation MV for regular query
      // indexUsed should either be undefined or not contain 'OrderStats'
      if (result.stats.indexUsed) {
        expect(result.stats.indexUsed).not.toContain('OrderStats')
      } else {
        expect(result.stats.indexUsed).toBeUndefined()
      }
    })
  })

  describe('constructor with MVRouter', () => {
    it('accepts MVRouter in constructor', async () => {
      const sourceData = {
        TestMV: [
          { $id: '1', value: 'test' },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      const mvDef: MVDefinition = {
        $from: 'items',
      }
      provider.registerMV(createMVMetadata('TestMV', mvDef))

      const result = await executor.execute('items', {})

      expect(result.stats.indexUsed).toBe('mv:TestMV')
    })
  })

  describe('MV selection priority', () => {
    it('prefers MV with exact filter match over MV without filter', async () => {
      const sourceData = {
        AllOrders: [
          { $id: '1', status: 'completed', amount: 100 },
          { $id: '2', status: 'pending', amount: 200 },
        ],
        CompletedOrders: [
          { $id: '1', status: 'completed', amount: 100 },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      // Register both MVs
      provider.registerMV(createMVMetadata('AllOrders', { $from: 'orders' }))
      provider.registerMV(createMVMetadata('CompletedOrders', {
        $from: 'orders',
        $filter: { status: 'completed' },
      }))

      const result = await executor.execute('orders', { status: 'completed' })

      // Should prefer CompletedOrders (exact match, no post-filter needed)
      expect(result.stats.indexUsed).toBe('mv:CompletedOrders')
    })

    it('prefers fresh MV over stale MV', async () => {
      const sourceData = {
        FreshMV: [
          { $id: '1', value: 'fresh' },
        ],
        StaleMV: [
          { $id: '1', value: 'stale' },
        ],
      }

      const reader = createMockReader(sourceData)
      const executor = new QueryExecutor(reader, storage, undefined, router)

      provider.registerMV(createMVMetadata('StaleMV', { $from: 'items' }, {
        stalenessState: 'stale',
        usable: true,
      }))
      provider.registerMV(createMVMetadata('FreshMV', { $from: 'items' }, {
        stalenessState: 'fresh',
        usable: true,
      }))

      const result = await executor.execute('items', {})

      // Should prefer fresh MV
      expect(result.stats.indexUsed).toBe('mv:FreshMV')
    })
  })

  describe('error handling', () => {
    it('falls back to source when MV read fails', async () => {
      const reader = createMockReader({
        orders: [{ $id: '1', value: 'source' }],
      })

      // Make MV read fail
      reader.readMetadata = vi.fn().mockImplementation(async (path: string) => {
        if (path.includes('FailingMV')) {
          throw new Error('MV read failed')
        }
        return {
          schema: {},
          rowGroups: [{ numRows: 1, columns: [] }],
          keyValueMetadata: [],
        }
      })

      reader.readRowGroups = vi.fn().mockImplementation(async <T>(path: string) => {
        if (path.includes('FailingMV')) {
          throw new Error('MV read failed')
        }
        return [{ $id: '1', value: 'source' }] as T[]
      })

      const executor = new QueryExecutor(reader, storage, undefined, router)

      provider.registerMV(createMVMetadata('FailingMV', { $from: 'orders' }))

      const result = await executor.execute('orders', {})

      // Should fall back to source
      expect(result.stats.indexUsed).toBeUndefined()
    })
  })
})

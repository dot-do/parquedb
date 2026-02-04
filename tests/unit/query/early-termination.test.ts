/**
 * Tests for QueryExecutor Early Termination Optimization
 *
 * Tests that limit queries without sorting terminate early once enough rows are found,
 * rather than scanning the entire dataset.
 *
 * This addresses the critical bug where limit=1 queries still scanned 100K rows.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  QueryExecutor,
  type ParquetReader,
  type ParquetMetadata,
  type BloomFilterReader,
} from '../../../src/query/executor'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Mock ParquetReader with Row Group Tracking
// =============================================================================

interface MockReaderOptions {
  data: Record<string, unknown[]>
  rowsPerGroup?: number
}

function createMockReaderWithTracking(options: MockReaderOptions): {
  reader: ParquetReader
  getReadStats: () => { rowGroupsRead: number; rowsRead: number }
} {
  const { data, rowsPerGroup = 1000 } = options
  let rowGroupsRead = 0
  let rowsRead = 0

  const reader: ParquetReader = {
    readMetadata: vi.fn().mockImplementation(async (path: string) => {
      const ns = path.split('/')[1] // Extract namespace from path
      const rows = data[ns!] ?? []
      const numRowGroups = Math.ceil(rows.length / rowsPerGroup)

      return {
        schema: {},
        rowGroups: Array.from({ length: numRowGroups }, (_, i) => ({
          numRows: Math.min(rowsPerGroup, rows.length - i * rowsPerGroup),
          columns: [],
        })),
        keyValueMetadata: [],
      } as unknown as ParquetMetadata
    }),

    readRowGroups: vi.fn().mockImplementation(async <T>(path: string, rowGroups: number[]) => {
      const ns = path.split('/')[1]
      const rows = data[ns!] ?? []

      const result: unknown[] = []
      for (const rgIndex of rowGroups) {
        rowGroupsRead++
        const start = rgIndex * rowsPerGroup
        const end = Math.min(start + rowsPerGroup, rows.length)
        const groupRows = rows.slice(start, end)
        rowsRead += groupRows.length
        result.push(...groupRows)
      }

      return result as T[]
    }),

    readAll: vi.fn().mockImplementation(async <T>(path: string) => {
      const ns = path.split('/')[1]
      const rows = data[ns!] ?? []
      rowsRead += rows.length
      return rows as T[]
    }),

    getBloomFilter: vi.fn().mockResolvedValue(null as BloomFilterReader | null),
  }

  return {
    reader,
    getReadStats: () => ({ rowGroupsRead, rowsRead }),
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
// Test Data Generation
// =============================================================================

function generateTestData(count: number): unknown[] {
  return Array.from({ length: count }, (_, i) => ({
    $id: `id-${i}`,
    name: `Item ${i}`,
    status: i % 2 === 0 ? 'active' : 'inactive',
    value: i * 10,
  }))
}

// =============================================================================
// Tests
// =============================================================================

describe('QueryExecutor Early Termination', () => {
  let storage: StorageBackend

  beforeEach(() => {
    storage = createMockStorage()
  })

  describe('limit queries without sort', () => {
    it('terminates early when limit=1 on a large dataset', async () => {
      // Create a dataset with 10,000 rows across 10 row groups
      const data = {
        items: generateTestData(10000),
      }

      const { reader, getReadStats } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 1000,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with limit=1
      const result = await executor.execute('items', {}, { limit: 1 })

      const stats = getReadStats()

      // Should return exactly 1 row
      expect(result.rows).toHaveLength(1)

      // Should have used early termination
      expect(result.stats.usedEarlyTermination).toBe(true)

      // Critical: should NOT have read all 10,000 rows
      // With DEFAULT_CONCURRENCY=4, we may read up to 4 row groups in the first batch
      // but definitely NOT all 10 row groups
      expect(stats.rowGroupsRead).toBeLessThanOrEqual(4)
      expect(result.stats.rowsScanned).toBeLessThan(10000)

      // The important thing is we didn't scan the whole dataset
      expect(result.stats.rowsScanned).toBeLessThanOrEqual(4000) // At most 4 row groups * 1000 rows
    })

    it('terminates early with filter + limit', async () => {
      // Create dataset where only even indices match the filter
      const data = {
        items: generateTestData(10000),
      }

      const { reader, getReadStats } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 1000,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with filter and limit=5
      const result = await executor.execute(
        'items',
        { status: 'active' }, // Matches ~50% of rows (even indices)
        { limit: 5 }
      )

      // Should return exactly 5 rows
      expect(result.rows).toHaveLength(5)

      // All returned rows should match the filter
      for (const row of result.rows as Array<{ status: string }>) {
        expect(row.status).toBe('active')
      }

      // Should have used early termination
      expect(result.stats.usedEarlyTermination).toBe(true)

      // Should NOT have scanned all 10,000 rows
      expect(result.stats.rowsScanned).toBeLessThan(10000)
    })

    it('handles limit with skip correctly', async () => {
      const data = {
        items: generateTestData(10000),
      }

      const { reader } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 1000,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with skip=10 and limit=5
      const result = await executor.execute('items', {}, { skip: 10, limit: 5 })

      // Should return exactly 5 rows
      expect(result.rows).toHaveLength(5)

      // Should have used early termination
      expect(result.stats.usedEarlyTermination).toBe(true)

      // First row should be the 11th item (0-indexed: 10)
      expect((result.rows[0] as { $id: string }).$id).toBe('id-10')
    })

    it('correctly reports hasMore when more results exist', async () => {
      const data = {
        items: generateTestData(100),
      }

      const { reader } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 50,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with limit=10
      const result = await executor.execute('items', {}, { limit: 10 })

      // Should return 10 rows
      expect(result.rows).toHaveLength(10)

      // Should indicate there are more results
      expect(result.hasMore).toBe(true)
    })

    it('correctly reports hasMore=false when at end of data', async () => {
      const data = {
        items: generateTestData(5),
      }

      const { reader } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 10,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with limit=10 (more than total rows)
      const result = await executor.execute('items', {}, { limit: 10 })

      // Should return all 5 rows
      expect(result.rows).toHaveLength(5)

      // Should indicate no more results
      expect(result.hasMore).toBe(false)

      // Should NOT have used early termination (didn't find enough rows)
      expect(result.stats.usedEarlyTermination).toBe(false)
    })
  })

  describe('limit queries WITH sort (no early termination)', () => {
    it('does not use early termination when sort is specified', async () => {
      const data = {
        items: generateTestData(1000),
      }

      const { reader, getReadStats } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 100,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with limit and sort
      const result = await executor.execute(
        'items',
        {},
        { limit: 1, sort: { value: -1 } }
      )

      const stats = getReadStats()

      // Should return 1 row
      expect(result.rows).toHaveLength(1)

      // Should NOT have used early termination (sort requires all rows)
      expect(result.stats.usedEarlyTermination).toBeUndefined()

      // Should have read all row groups to sort properly
      expect(stats.rowGroupsRead).toBe(10) // 1000 rows / 100 per group = 10 groups

      // The result should be the row with highest value
      expect((result.rows[0] as { $id: string }).$id).toBe('id-999')
    })
  })

  describe('queries without limit', () => {
    it('does not use early termination when no limit specified', async () => {
      const data = {
        items: generateTestData(100),
      }

      const { reader, getReadStats } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 50,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute without limit
      const result = await executor.execute('items', {})

      const stats = getReadStats()

      // Should return all rows
      expect(result.rows).toHaveLength(100)

      // Should NOT have used early termination
      expect(result.stats.usedEarlyTermination).toBeUndefined()

      // Should have read all row groups
      expect(stats.rowGroupsRead).toBe(2)
    })
  })

  describe('soft delete handling', () => {
    it('respects includeDeleted=false during early termination', async () => {
      // Create data where some items are soft-deleted
      const data = {
        items: Array.from({ length: 100 }, (_, i) => ({
          $id: `id-${i}`,
          name: `Item ${i}`,
          deletedAt: i < 50 ? new Date().toISOString() : null, // First 50 are deleted
        })),
      }

      const { reader } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 20,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with limit=5, default includeDeleted=false
      const result = await executor.execute('items', {}, { limit: 5 })

      // Should return 5 non-deleted rows
      expect(result.rows).toHaveLength(5)

      // All returned rows should NOT be deleted
      for (const row of result.rows as Array<{ deletedAt: string | null }>) {
        expect(row.deletedAt).toBeNull()
      }
    })

    it('includes deleted rows when includeDeleted=true during early termination', async () => {
      const data = {
        items: Array.from({ length: 100 }, (_, i) => ({
          $id: `id-${i}`,
          name: `Item ${i}`,
          deletedAt: i < 50 ? new Date().toISOString() : null,
        })),
      }

      const { reader } = createMockReaderWithTracking({
        data,
        rowsPerGroup: 20,
      })

      const executor = new QueryExecutor(reader, storage)

      // Execute with includeDeleted=true
      const result = await executor.execute('items', {}, { limit: 5, includeDeleted: true })

      // Should return 5 rows (may include deleted ones)
      expect(result.rows).toHaveLength(5)
    })
  })
})

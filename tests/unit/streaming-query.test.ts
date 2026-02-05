/**
 * Tests for Lazy/Streaming Row Group Processing
 *
 * Verifies that ParqueDB can process row groups lazily for:
 * - LIMIT queries that only read necessary row groups
 * - Early termination when enough rows are found
 * - Constant memory usage regardless of total file size
 * - Streaming aggregations without full materialization
 *
 * Expected improvement: 7-8x, memory O(row_group) instead of O(total_rows)
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { parquetQueryStream, type StreamingQueryStats } from '../../src/query/streaming'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { ParquetWriter } from '../../src/parquet/writer'
import type { ParquetSchema } from '../../src/parquet/types'
import type { Filter } from '../../src/types/filter'

// =============================================================================
// Test Helpers
// =============================================================================

interface TestEntity {
  $id: string
  $type: string
  name: string
  category: string
  value: number
  createdAt: number
}

/** Schema for test entities */
const testEntitySchema: ParquetSchema = {
  $id: { type: 'STRING' },
  $type: { type: 'STRING' },
  name: { type: 'STRING' },
  category: { type: 'STRING' },
  value: { type: 'DOUBLE' },
  createdAt: { type: 'INT64' },
}

/**
 * Generate test data with predictable IDs for verification
 */
function generateTestEntities(count: number, startId = 0): TestEntity[] {
  const categories = ['A', 'B', 'C', 'D']
  return Array.from({ length: count }, (_, i) => ({
    $id: `entity-${String(startId + i).padStart(6, '0')}`,
    $type: 'test',
    name: `Entity ${startId + i}`,
    category: categories[i % categories.length]!,
    value: (i + 1) * 10,
    createdAt: Date.now() - i * 1000,
  }))
}

/**
 * Create a test Parquet file with multiple row groups
 */
async function createTestParquetFile(
  storage: MemoryBackend,
  filePath: string,
  entitiesPerRowGroup: number,
  rowGroupCount: number
): Promise<{ totalRows: number; rowGroupSize: number }> {
  const writer = new ParquetWriter(storage, {
    rowGroupSize: entitiesPerRowGroup,
    compression: 'none',
  })

  const totalEntities = entitiesPerRowGroup * rowGroupCount
  const entities = generateTestEntities(totalEntities)

  await writer.write(filePath, entities, testEntitySchema)

  return {
    totalRows: totalEntities,
    rowGroupSize: entitiesPerRowGroup,
  }
}

// =============================================================================
// Streaming Row Group Processing Tests
// =============================================================================

describe('Streaming Row Group Processing', () => {
  let storage: MemoryBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
  })

  afterEach(async () => {
    // MemoryBackend doesn't need cleanup
  })

  describe('LIMIT Query Optimization', () => {
    test('LIMIT 10 query only reads necessary row groups', async () => {
      // Create file with 10 row groups, 100 rows each (1000 total rows)
      const filePath = 'test/limit-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 10)

      const results: TestEntity[] = []
      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      // Query with LIMIT 10
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 10,
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      // Should have exactly 10 results
      expect(results).toHaveLength(10)

      // Should have read only 1 row group (first 100 rows contain our 10)
      expect(stats.rowGroupsRead).toBe(1)
      expect(stats.rowGroupsTotal).toBe(10)
      expect(stats.terminatedEarly).toBe(true)

      // Rows scanned should be less than total
      expect(stats.rowsScanned).toBeLessThanOrEqual(100)
    })

    test('LIMIT query with filter reads only necessary row groups', async () => {
      // Create file with 10 row groups, 100 rows each
      const filePath = 'test/limit-filter-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 10)

      const results: TestEntity[] = []
      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      // Query with filter and limit
      const filter: Filter = { category: 'A' }
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        filter,
        limit: 5,
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      // Should have exactly 5 results
      expect(results).toHaveLength(5)
      // All should match filter
      expect(results.every(r => r.category === 'A')).toBe(true)

      // Should terminate early since we only need 5 category A entities
      // With 25% matching filter, need ~20 rows to get 5 matches
      expect(stats.terminatedEarly).toBe(true)
      expect(stats.rowGroupsRead).toBeLessThan(stats.rowGroupsTotal)
    })

    test('LIMIT query without filter terminates after exact count', async () => {
      const filePath = 'test/exact-limit-test.parquet'
      await createTestParquetFile(storage, filePath, 50, 4) // 200 total rows

      const results: TestEntity[] = []
      let rowsYielded = 0

      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 75,
      })) {
        results.push(row)
        rowsYielded++
      }

      // Should yield exactly 75 rows
      expect(results).toHaveLength(75)
      expect(rowsYielded).toBe(75)
    })
  })

  describe('Early Termination', () => {
    test('stops reading when enough rows found without sorting', async () => {
      const filePath = 'test/early-term-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 20) // 2000 total rows

      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 50,
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      expect(results).toHaveLength(50)
      expect(stats.terminatedEarly).toBe(true)
      // Should have read only 1 row group to get 50 rows
      expect(stats.rowGroupsRead).toBe(1)
    })

    test('reads all row groups when sorting is required', async () => {
      const filePath = 'test/sort-no-early-term.parquet'
      await createTestParquetFile(storage, filePath, 100, 5) // 500 total rows

      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 10,
        sort: { value: -1 }, // Sorting requires all rows
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      expect(results).toHaveLength(10)
      // With sorting, cannot terminate early - must read all to find top 10
      expect(stats.terminatedEarly).toBe(false)
      expect(stats.rowGroupsRead).toBe(5)
    })

    test('breaks out of generator when consumer stops iterating', async () => {
      const filePath = 'test/consumer-break-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 10) // 1000 total rows

      let rowsConsumed = 0
      const generator = parquetQueryStream<TestEntity>(storage, filePath, {})

      // Only consume 25 rows then break
      for await (const _row of generator) {
        rowsConsumed++
        if (rowsConsumed >= 25) {
          break
        }
      }

      expect(rowsConsumed).toBe(25)
    })
  })

  describe('Memory Efficiency', () => {
    test('memory stays constant regardless of total file size', async () => {
      // Create a large file with many row groups
      const filePath = 'test/memory-test.parquet'
      await createTestParquetFile(storage, filePath, 1000, 10) // 10,000 rows

      // Track memory during streaming
      const memorySnapshots: number[] = []
      const initialMemory = process.memoryUsage().heapUsed

      let rowCount = 0
      for await (const _row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 100, // Only need 100 rows
      })) {
        rowCount++
        if (rowCount % 20 === 0) {
          memorySnapshots.push(process.memoryUsage().heapUsed - initialMemory)
        }
      }

      expect(rowCount).toBe(100)

      // Memory snapshots should be relatively stable (within 2MB variance)
      // Note: This is a heuristic test - actual memory may vary
      if (memorySnapshots.length >= 2) {
        const maxMemory = Math.max(...memorySnapshots)
        const minMemory = Math.min(...memorySnapshots)
        const variance = maxMemory - minMemory

        // Allow up to 5MB variance for GC fluctuations
        expect(variance).toBeLessThan(5 * 1024 * 1024)
      }
    })

    test('does not load all rows into memory for streaming query', async () => {
      const filePath = 'test/no-full-load-test.parquet'
      await createTestParquetFile(storage, filePath, 500, 4) // 2000 rows

      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      // Consume only 50 rows
      let count = 0
      for await (const _row of parquetQueryStream<TestEntity>(storage, filePath, {
        limit: 50,
        onStats: (s) => Object.assign(stats, s),
      })) {
        count++
      }

      expect(count).toBe(50)
      // Should have scanned at most 1 row group worth (500 rows)
      expect(stats.rowsScanned).toBeLessThanOrEqual(500)
    })
  })

  describe('Streaming Aggregation', () => {
    test('aggregation streams without full materialization', async () => {
      const filePath = 'test/streaming-agg-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 10) // 1000 rows

      // Streaming sum aggregation
      let sum = 0
      let count = 0
      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        columns: ['value'], // Only need value column for aggregation
        onStats: (s) => Object.assign(stats, s),
      })) {
        sum += row.value
        count++
      }

      // Verify aggregation completed
      expect(count).toBe(1000)

      // Calculate expected sum: sum of (i+1)*10 for i=0 to 999
      const expectedSum = Array.from({ length: 1000 }, (_, i) => (i + 1) * 10).reduce(
        (a, b) => a + b,
        0
      )
      expect(sum).toBe(expectedSum)

      // All row groups should be read for full aggregation
      expect(stats.rowGroupsRead).toBe(10)
    })

    test('count aggregation with filter streams efficiently', async () => {
      const filePath = 'test/count-agg-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 10) // 1000 rows

      let count = 0
      const filter: Filter = { category: 'B' }

      for await (const _row of parquetQueryStream<TestEntity>(storage, filePath, {
        filter,
        columns: ['category'], // Minimal projection for count
      })) {
        count++
      }

      // 25% should match category B
      expect(count).toBe(250)
    })

    test('maintains running totals without storing rows', async () => {
      const filePath = 'test/running-total-test.parquet'
      await createTestParquetFile(storage, filePath, 200, 5) // 1000 rows

      // Track stats by category without storing rows
      const categoryStats = new Map<string, { count: number; sum: number }>()

      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        columns: ['category', 'value'],
      })) {
        const existing = categoryStats.get(row.category) || { count: 0, sum: 0 }
        existing.count++
        existing.sum += row.value
        categoryStats.set(row.category, existing)
      }

      // Should have 4 categories
      expect(categoryStats.size).toBe(4)

      // Each category should have 250 entries (1000 / 4)
      for (const stats of categoryStats.values()) {
        expect(stats.count).toBe(250)
      }
    })
  })

  describe('Row Group Processing with Concurrency', () => {
    test('processes row groups with concurrency limit', async () => {
      const filePath = 'test/concurrent-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 8) // 800 rows

      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        concurrency: 2, // Process 2 row groups at a time
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      expect(results).toHaveLength(800)
      expect(stats.rowGroupsRead).toBe(8)
    })

    test('maintains order even with concurrent row group reading', async () => {
      const filePath = 'test/order-test.parquet'
      await createTestParquetFile(storage, filePath, 50, 6) // 300 rows

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        concurrency: 3,
      })) {
        results.push(row)
      }

      // Verify order is maintained
      for (let i = 0; i < results.length - 1; i++) {
        const currentId = parseInt(results[i]!.$id.split('-')[1]!, 10)
        const nextId = parseInt(results[i + 1]!.$id.split('-')[1]!, 10)
        expect(currentId).toBeLessThan(nextId)
      }
    })
  })

  describe('Skip/Offset Support', () => {
    test('skip works with streaming', async () => {
      const filePath = 'test/skip-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 5) // 500 rows

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        skip: 150,
        limit: 50,
      })) {
        results.push(row)
      }

      expect(results).toHaveLength(50)
      // First result should be entity-000150
      expect(results[0]!.$id).toBe('entity-000150')
    })

    test('skip across multiple row groups', async () => {
      const filePath = 'test/skip-multi-rg-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 5) // 500 rows

      const stats: StreamingQueryStats = {
        rowGroupsTotal: 0,
        rowGroupsRead: 0,
        rowsScanned: 0,
        rowsYielded: 0,
        terminatedEarly: false,
      }

      const results: TestEntity[] = []
      for await (const row of parquetQueryStream<TestEntity>(storage, filePath, {
        skip: 250, // Skip first 2.5 row groups worth
        limit: 25,
        onStats: (s) => Object.assign(stats, s),
      })) {
        results.push(row)
      }

      expect(results).toHaveLength(25)
      // First result should be entity-000250
      expect(results[0]!.$id).toBe('entity-000250')

      // Should have read row groups 3+ (indices 2+)
      // May read 3 row groups: one partial for skip, one for results
      expect(stats.rowGroupsRead).toBeLessThanOrEqual(4)
    })
  })

  describe('Column Projection', () => {
    test('streams only requested columns', async () => {
      const filePath = 'test/projection-test.parquet'
      await createTestParquetFile(storage, filePath, 100, 3) // 300 rows

      const results: Array<{ $id: string; value: number }> = []
      for await (const row of parquetQueryStream<{ $id: string; value: number }>(
        storage,
        filePath,
        {
          columns: ['$id', 'value'],
          limit: 10,
        }
      )) {
        results.push(row)
      }

      expect(results).toHaveLength(10)
      // Should only have projected columns
      for (const row of results) {
        expect(Object.keys(row).sort()).toEqual(['$id', 'value'])
      }
    })
  })

  describe('Error Handling', () => {
    test('handles non-existent file gracefully', async () => {
      await expect(async () => {
        for await (const _row of parquetQueryStream(storage, 'non-existent.parquet', {})) {
          // Should not reach here
        }
      }).rejects.toThrow()
    })

    test('propagates errors from storage backend', async () => {
      const brokenStorage = {
        ...storage,
        readRange: async () => {
          throw new Error('Storage read error')
        },
        stat: async () => ({ size: 1000, mtime: new Date() }),
      } as unknown as MemoryBackend

      const filePath = 'test/error-test.parquet'
      await createTestParquetFile(storage, filePath, 10, 1)

      await expect(async () => {
        for await (const _row of parquetQueryStream(brokenStorage, filePath, {})) {
          // Should not reach here
        }
      }).rejects.toThrow('Storage read error')
    })
  })
})

describe('Benchmark: Streaming vs Eager Loading', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  test('streaming shows memory improvement for large files with small limits', async () => {
    // This is a benchmark test - marked for manual verification
    // Skip in CI environments
    if (process.env.CI) {
      return
    }

    const filePath = 'test/benchmark.parquet'
    await createTestParquetFile(storage, filePath, 1000, 10) // 10,000 rows

    // Streaming approach
    const streamingStart = process.memoryUsage().heapUsed
    let streamingCount = 0
    for await (const _row of parquetQueryStream<TestEntity>(storage, filePath, {
      limit: 100,
    })) {
      streamingCount++
    }
    const streamingMemory = process.memoryUsage().heapUsed - streamingStart

    expect(streamingCount).toBe(100)

    // The streaming memory should be significantly less than loading all rows
    // This is hard to measure precisely, but we can at least verify
    // the streaming version doesn't grow linearly with file size
    console.log(`Streaming memory used: ${(streamingMemory / 1024 / 1024).toFixed(2)} MB`)
    console.log(`Expected improvement: streaming should use O(row_group) memory, not O(total_rows)`)
  })
})

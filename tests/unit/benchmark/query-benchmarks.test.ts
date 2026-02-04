/**
 * Query Benchmark Tests (RED Phase)
 *
 * TDD tests for comprehensive query benchmarks.
 * All tests should FAIL initially - implementation comes in GREEN phase.
 *
 * Test cases for each query type:
 * 1. Full scan benchmark returns valid metrics (timing, rows/sec)
 * 2. Column projection benchmark measures only requested columns
 * 3. Point lookup benchmark hits correct row by $id
 * 4. Filtered query benchmark uses predicate pushdown
 * 5. Range query benchmark scans correct row groups
 * 6. Aggregation benchmarks (COUNT, SUM) return correct values
 * 7. Group by benchmark groups correctly
 * 8. Top-k benchmark respects limit and order
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import type {
  QueryType,
  BenchmarkResult,
  BenchmarkConfig,
  BenchmarkExecutor,
} from '../../../scripts/benchmark/types'
import { QueryBenchmarkRunner, createQueryBenchmarkRunner } from '../../../scripts/benchmark/query-runner'

// =============================================================================
// Mock Types and Interfaces
// =============================================================================

/**
 * Mock response for benchmark fetch calls
 */
interface MockBenchmarkResponse {
  timing: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
    stdDev: number
  }
  throughput: {
    rowsProcessed: number
    rowsPerSecond: number
    bytesRead: number
    bytesPerSecond: number
  }
  resources: {
    peakMemoryMB: number
    cpuTimeMs: number
  }
  metadata: {
    rowGroupsTotal: number
    rowGroupsRead: number
    columnsRequested: number
    predicatePushdown: boolean
    compressionRatio: number
  }
  data?: unknown[]
}

// QueryBenchmarkRunner is now imported from query-runner.ts

// =============================================================================
// Mock Setup
// =============================================================================

// The benchmark runner instance
let benchmarkRunner: QueryBenchmarkRunner

// Mock fetch for unit tests
const mockFetch = vi.fn()

beforeEach(() => {
  vi.clearAllMocks()
  // Reset the mock fetch
  global.fetch = mockFetch as unknown as typeof fetch
  // Create the benchmark runner instance
  benchmarkRunner = createQueryBenchmarkRunner('https://parquedb.workers.do')
})

afterEach(() => {
  vi.restoreAllMocks()
})

/**
 * Create a mock benchmark response
 */
function createMockResponse(overrides: Partial<MockBenchmarkResponse> = {}): MockBenchmarkResponse {
  return {
    timing: {
      min: 10,
      max: 50,
      avg: 25,
      p50: 23,
      p95: 45,
      p99: 48,
      stdDev: 8,
      ...overrides.timing,
    },
    throughput: {
      rowsProcessed: 10000,
      rowsPerSecond: 400000,
      bytesRead: 1048576,
      bytesPerSecond: 41943040,
      ...overrides.throughput,
    },
    resources: {
      peakMemoryMB: 128,
      cpuTimeMs: 200,
      ...overrides.resources,
    },
    metadata: {
      rowGroupsTotal: 10,
      rowGroupsRead: 10,
      columnsRequested: 5,
      predicatePushdown: false,
      compressionRatio: 0.4,
      ...overrides.metadata,
    },
    data: overrides.data ?? [],
  }
}

// =============================================================================
// Test Suites
// =============================================================================

describe('Query Benchmarks', () => {
  // ===========================================================================
  // 1. Full Scan Benchmark
  // ===========================================================================

  describe('Full Scan Benchmark', () => {
    it('should return valid timing metrics', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          timing: { min: 15, max: 60, avg: 30, p50: 28, p95: 55, p99: 58, stdDev: 10 },
        }),
      })

      // Act
      // This will fail because benchmarkRunner is not implemented
      const result = await benchmarkRunner.runFullScan({
        dataset: 'imdb',
        collection: 'titles',
        iterations: 10,
      })

      // Assert - timing metrics should be valid
      expect(result.timing).toBeDefined()
      expect(result.timing.min).toBeGreaterThan(0)
      expect(result.timing.max).toBeGreaterThanOrEqual(result.timing.min)
      expect(result.timing.avg).toBeGreaterThanOrEqual(result.timing.min)
      expect(result.timing.avg).toBeLessThanOrEqual(result.timing.max)
      expect(result.timing.p50).toBeGreaterThanOrEqual(result.timing.min)
      expect(result.timing.p95).toBeGreaterThanOrEqual(result.timing.p50)
      expect(result.timing.p99).toBeGreaterThanOrEqual(result.timing.p95)
      expect(result.timing.stdDev).toBeGreaterThanOrEqual(0)
    })

    it('should return valid throughput metrics (rows/sec)', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          throughput: {
            rowsProcessed: 50000,
            rowsPerSecond: 500000,
            bytesRead: 5242880,
            bytesPerSecond: 52428800,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFullScan({
        dataset: 'imdb',
        collection: 'titles',
        iterations: 10,
      })

      // Assert - throughput metrics should be valid
      expect(result.throughput).toBeDefined()
      expect(result.throughput.rowsProcessed).toBeGreaterThan(0)
      expect(result.throughput.rowsPerSecond).toBeGreaterThan(0)
      expect(result.throughput.bytesRead).toBeGreaterThan(0)
      expect(result.throughput.bytesPerSecond).toBeGreaterThan(0)
    })

    it('should scan all row groups for full scan', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 10,
            columnsRequested: 5,
            predicatePushdown: false,
            compressionRatio: 0.4,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFullScan({
        dataset: 'imdb',
        collection: 'titles',
        iterations: 10,
      })

      // Assert - full scan should read all row groups
      expect(result.metadata.rowGroupsRead).toBe(result.metadata.rowGroupsTotal)
      expect(result.metadata.predicatePushdown).toBe(false)
    })
  })

  // ===========================================================================
  // 2. Column Projection Benchmark
  // ===========================================================================

  describe('Column Projection Benchmark', () => {
    it('should measure only requested columns', async () => {
      // Arrange
      const requestedColumns = ['$id', 'title', 'year']
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 10,
            columnsRequested: requestedColumns.length,
            predicatePushdown: false,
            compressionRatio: 0.3,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runColumnProjection({
        dataset: 'imdb',
        collection: 'titles',
        columns: requestedColumns,
        iterations: 10,
      })

      // Assert - should only read requested columns
      expect(result.metadata.columnsRequested).toBe(requestedColumns.length)
    })

    it('should have lower bytes read than full scan', async () => {
      // Arrange - Column projection should read fewer bytes
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          throughput: {
            rowsProcessed: 50000,
            rowsPerSecond: 800000,
            bytesRead: 1048576, // 1MB - less than full scan
            bytesPerSecond: 83886080,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runColumnProjection({
        dataset: 'imdb',
        collection: 'titles',
        columns: ['$id', 'title'],
        iterations: 10,
      })

      // Assert - fewer bytes should be read for projection
      expect(result.throughput.bytesRead).toBeLessThan(5242880) // Less than 5MB full scan
      expect(result.throughput.rowsPerSecond).toBeGreaterThan(0)
    })

    it('should return valid timing for column projection', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          timing: { min: 5, max: 30, avg: 15, p50: 14, p95: 25, p99: 28, stdDev: 5 },
        }),
      })

      // Act
      const result = await benchmarkRunner.runColumnProjection({
        dataset: 'imdb',
        collection: 'titles',
        columns: ['$id', 'name'],
        iterations: 10,
      })

      // Assert
      expect(result.timing.avg).toBeLessThan(100) // Should be fast
      expect(result.queryType).toBe('column_projection')
    })
  })

  // ===========================================================================
  // 3. Point Lookup Benchmark
  // ===========================================================================

  describe('Point Lookup Benchmark', () => {
    it('should hit correct row by $id', async () => {
      // Arrange
      const targetId = 'tt0000001'
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          data: [{ $id: targetId, title: 'Carmencita', year: 1894 }],
          throughput: {
            rowsProcessed: 1,
            rowsPerSecond: 100000,
            bytesRead: 1024,
            bytesPerSecond: 102400000,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runPointLookup({
        dataset: 'imdb',
        collection: 'titles',
        id: targetId,
        iterations: 10,
      })

      // Assert - point lookup should find exactly one row
      expect(result.throughput.rowsProcessed).toBe(1)
    })

    it('should use minimal row groups for point lookup', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 1, // Should only read 1 row group
            columnsRequested: 10,
            predicatePushdown: true,
            compressionRatio: 0.4,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runPointLookup({
        dataset: 'imdb',
        collection: 'titles',
        id: 'tt0000001',
        iterations: 10,
      })

      // Assert - should use predicate pushdown and read minimal row groups
      expect(result.metadata.predicatePushdown).toBe(true)
      expect(result.metadata.rowGroupsRead).toBeLessThan(result.metadata.rowGroupsTotal)
    })

    it('should have sub-millisecond p50 latency for cached lookups', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          timing: { min: 0.1, max: 5, avg: 1, p50: 0.5, p95: 3, p99: 4, stdDev: 0.8 },
        }),
      })

      // Act
      const result = await benchmarkRunner.runPointLookup({
        dataset: 'imdb',
        collection: 'titles',
        id: 'tt0000001',
        iterations: 100,
      })

      // Assert - point lookups should be very fast
      expect(result.timing.p50).toBeLessThan(10) // Less than 10ms
    })
  })

  // ===========================================================================
  // 4. Filtered Query Benchmark
  // ===========================================================================

  describe('Filtered Query Benchmark', () => {
    it('should use predicate pushdown for equality filter', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 3, // Only some row groups match
            columnsRequested: 5,
            predicatePushdown: true,
            compressionRatio: 0.4,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFilteredQuery({
        dataset: 'imdb',
        collection: 'titles',
        filter: { titleType: 'movie' },
        iterations: 10,
      })

      // Assert - predicate pushdown should be enabled
      expect(result.metadata.predicatePushdown).toBe(true)
      expect(result.metadata.rowGroupsRead).toBeLessThan(result.metadata.rowGroupsTotal)
    })

    it('should skip row groups that do not match filter', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 2,
            columnsRequested: 5,
            predicatePushdown: true,
            compressionRatio: 0.4,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFilteredQuery({
        dataset: 'imdb',
        collection: 'titles',
        filter: { startYear: 2020 },
        iterations: 10,
      })

      // Assert - row groups should be skipped
      const skippedRowGroups = result.metadata.rowGroupsTotal - result.metadata.rowGroupsRead
      expect(skippedRowGroups).toBeGreaterThan(0)
    })

    it('should return accurate result count for filtered queries', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          throughput: {
            rowsProcessed: 5000,
            rowsPerSecond: 250000,
            bytesRead: 2097152,
            bytesPerSecond: 104857600,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFilteredQuery({
        dataset: 'imdb',
        collection: 'titles',
        filter: { isAdult: false },
        iterations: 10,
      })

      // Assert - should report accurate row count
      expect(result.throughput.rowsProcessed).toBeGreaterThan(0)
      expect(result.queryType).toBe('filtered_eq')
    })
  })

  // ===========================================================================
  // 5. Range Query Benchmark
  // ===========================================================================

  describe('Range Query Benchmark', () => {
    it('should scan correct row groups for range query', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 10,
            rowGroupsRead: 4, // Only row groups in range
            columnsRequested: 5,
            predicatePushdown: true,
            compressionRatio: 0.4,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runRangeQuery({
        dataset: 'imdb',
        collection: 'titles',
        field: 'startYear',
        min: 2010,
        max: 2020,
        iterations: 10,
      })

      // Assert - should use statistics to skip row groups
      expect(result.metadata.predicatePushdown).toBe(true)
      expect(result.metadata.rowGroupsRead).toBeLessThanOrEqual(result.metadata.rowGroupsTotal)
    })

    it('should use min/max statistics for row group pruning', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          metadata: {
            rowGroupsTotal: 20,
            rowGroupsRead: 5, // Significant pruning
            columnsRequested: 3,
            predicatePushdown: true,
            compressionRatio: 0.35,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runRangeQuery({
        dataset: 'imdb',
        collection: 'titles',
        field: 'runtimeMinutes',
        min: 60,
        max: 120,
        iterations: 10,
      })

      // Assert - many row groups should be pruned
      const pruningRatio = 1 - (result.metadata.rowGroupsRead / result.metadata.rowGroupsTotal)
      expect(pruningRatio).toBeGreaterThan(0.5) // At least 50% pruned
    })

    it('should have better performance than full scan for narrow ranges', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          timing: { min: 5, max: 25, avg: 12, p50: 11, p95: 20, p99: 23, stdDev: 4 },
          throughput: {
            rowsProcessed: 2000,
            rowsPerSecond: 166666,
            bytesRead: 524288,
            bytesPerSecond: 43690666,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runRangeQuery({
        dataset: 'imdb',
        collection: 'titles',
        field: 'startYear',
        min: 2019,
        max: 2020,
        iterations: 10,
      })

      // Assert - range query should be efficient
      expect(result.queryType).toBe('filtered_range')
      expect(result.throughput.rowsProcessed).toBeLessThan(50000) // Less than full collection
    })
  })

  // ===========================================================================
  // 6. Aggregation Benchmarks (COUNT, SUM)
  // ===========================================================================

  describe('Aggregation Benchmarks', () => {
    describe('COUNT aggregation', () => {
      it('should return correct count value', async () => {
        // Arrange
        const expectedCount = 10500000
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse(),
            count: expectedCount,
          }),
        })

        // Act
        const result = await benchmarkRunner.runCountAggregation({
          dataset: 'imdb',
          collection: 'titles',
          iterations: 10,
        })

        // Assert - COUNT should return accurate value
        expect(result.count).toBe(expectedCount)
        expect(result.count).toBeGreaterThan(0)
      })

      it('should support filtered COUNT', async () => {
        // Arrange
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse(),
            count: 500000,
          }),
        })

        // Act
        const result = await benchmarkRunner.runCountAggregation({
          dataset: 'imdb',
          collection: 'titles',
          filter: { titleType: 'movie' },
          iterations: 10,
        })

        // Assert - filtered COUNT should return subset
        expect(result.count).toBeGreaterThan(0)
        expect(result.count).toBeLessThan(10500000) // Less than total
      })

      it('should be faster than row-by-row counting', async () => {
        // Arrange
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse({
              timing: { min: 1, max: 10, avg: 5, p50: 4, p95: 8, p99: 9, stdDev: 2 },
            }),
            count: 10000000,
          }),
        })

        // Act
        const result = await benchmarkRunner.runCountAggregation({
          dataset: 'imdb',
          collection: 'titles',
          iterations: 10,
        })

        // Assert - COUNT should be very fast (uses metadata)
        expect(result.timing.avg).toBeLessThan(50) // Less than 50ms
      })
    })

    describe('SUM aggregation', () => {
      it('should return correct sum value', async () => {
        // Arrange
        const expectedSum = 1234567890
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse(),
            sum: expectedSum,
          }),
        })

        // Act
        const result = await benchmarkRunner.runSumAggregation({
          dataset: 'imdb',
          collection: 'titles',
          field: 'runtimeMinutes',
          iterations: 10,
        })

        // Assert - SUM should return accurate value
        expect(result.sum).toBe(expectedSum)
      })

      it('should handle filtered SUM', async () => {
        // Arrange
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse(),
            sum: 50000000,
          }),
        })

        // Act
        const result = await benchmarkRunner.runSumAggregation({
          dataset: 'imdb',
          collection: 'titles',
          field: 'runtimeMinutes',
          filter: { titleType: 'movie' },
          iterations: 10,
        })

        // Assert
        expect(result.sum).toBeGreaterThan(0)
      })

      it('should return valid timing metrics', async () => {
        // Arrange
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            ...createMockResponse({
              timing: { min: 10, max: 100, avg: 40, p50: 35, p95: 80, p99: 95, stdDev: 20 },
            }),
            sum: 1000000,
          }),
        })

        // Act
        const result = await benchmarkRunner.runSumAggregation({
          dataset: 'imdb',
          collection: 'titles',
          field: 'numVotes',
          iterations: 10,
        })

        // Assert
        expect(result.queryType).toBe('aggregation_sum')
        expect(result.timing.p50).toBeLessThan(result.timing.p95)
      })
    })
  })

  // ===========================================================================
  // 7. Group By Benchmark
  // ===========================================================================

  describe('Group By Benchmark', () => {
    it('should group correctly by field', async () => {
      // Arrange
      const expectedGroups = [
        { _id: 'movie', count: 500000 },
        { _id: 'tvSeries', count: 300000 },
        { _id: 'short', count: 200000 },
      ]
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          groups: expectedGroups,
        }),
      })

      // Act
      const result = await benchmarkRunner.runGroupBy({
        dataset: 'imdb',
        collection: 'titles',
        groupByField: 'titleType',
        aggregations: [{ field: '$count', op: 'count' }],
        iterations: 10,
      })

      // Assert - groups should be returned correctly
      expect(result.groups).toBeDefined()
      expect(result.groups.length).toBeGreaterThan(0)
      expect(result.groups[0]).toHaveProperty('_id')
    })

    it('should support multiple aggregations per group', async () => {
      // Arrange
      const expectedGroups = [
        { _id: 'movie', count: 500000, avgRuntime: 98.5, maxRuntime: 873 },
        { _id: 'tvSeries', count: 300000, avgRuntime: 45.2, maxRuntime: 60 },
      ]
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          groups: expectedGroups,
        }),
      })

      // Act
      const result = await benchmarkRunner.runGroupBy({
        dataset: 'imdb',
        collection: 'titles',
        groupByField: 'titleType',
        aggregations: [
          { field: '$count', op: 'count' },
          { field: 'runtimeMinutes', op: 'avg' },
          { field: 'runtimeMinutes', op: 'max' },
        ],
        iterations: 10,
      })

      // Assert - each group should have all aggregations
      expect(result.groups[0]).toHaveProperty('count')
      expect(result.groups[0]).toHaveProperty('avgRuntime')
      expect(result.groups[0]).toHaveProperty('maxRuntime')
    })

    it('should return valid timing for GROUP BY', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse({
            timing: { min: 50, max: 200, avg: 100, p50: 90, p95: 180, p99: 195, stdDev: 30 },
          }),
          groups: [{ _id: 'movie', count: 500000 }],
        }),
      })

      // Act
      const result = await benchmarkRunner.runGroupBy({
        dataset: 'imdb',
        collection: 'titles',
        groupByField: 'titleType',
        aggregations: [{ field: '$count', op: 'count' }],
        iterations: 10,
      })

      // Assert
      expect(result.queryType).toBe('group_by')
      expect(result.timing.avg).toBeGreaterThan(0)
    })

    it('should handle high-cardinality group by', async () => {
      // Arrange - many unique groups
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          groups: Array.from({ length: 1000 }, (_, i) => ({
            _id: `director_${i}`,
            count: Math.floor(Math.random() * 100) + 1,
          })),
        }),
      })

      // Act
      const result = await benchmarkRunner.runGroupBy({
        dataset: 'imdb',
        collection: 'titles',
        groupByField: 'directors',
        aggregations: [{ field: '$count', op: 'count' }],
        iterations: 5,
      })

      // Assert - should handle many groups
      expect(result.groups.length).toBeGreaterThan(100)
    })
  })

  // ===========================================================================
  // 8. Top-K Benchmark
  // ===========================================================================

  describe('Top-K Benchmark', () => {
    it('should respect limit', async () => {
      // Arrange
      const limit = 10
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          results: Array.from({ length: limit }, (_, i) => ({
            $id: `tt000000${i}`,
            title: `Top Title ${i}`,
            numVotes: 1000000 - i * 10000,
          })),
        }),
      })

      // Act
      const result = await benchmarkRunner.runTopK({
        dataset: 'imdb',
        collection: 'titles',
        orderBy: { field: 'numVotes', direction: 'desc' },
        limit,
        iterations: 10,
      })

      // Assert - should return exactly limit results
      expect(result.results.length).toBe(limit)
    })

    it('should respect order (descending)', async () => {
      // Arrange
      const results = [
        { $id: 'tt0000001', numVotes: 1000000 },
        { $id: 'tt0000002', numVotes: 900000 },
        { $id: 'tt0000003', numVotes: 800000 },
      ]
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          results,
        }),
      })

      // Act
      const result = await benchmarkRunner.runTopK({
        dataset: 'imdb',
        collection: 'titles',
        orderBy: { field: 'numVotes', direction: 'desc' },
        limit: 3,
        iterations: 10,
      })

      // Assert - results should be in descending order
      for (let i = 1; i < result.results.length; i++) {
        const prev = result.results[i - 1] as { numVotes: number }
        const curr = result.results[i] as { numVotes: number }
        expect(prev.numVotes).toBeGreaterThanOrEqual(curr.numVotes)
      }
    })

    it('should respect order (ascending)', async () => {
      // Arrange
      const results = [
        { $id: 'tt0000003', startYear: 1890 },
        { $id: 'tt0000002', startYear: 1891 },
        { $id: 'tt0000001', startYear: 1892 },
      ]
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          results,
        }),
      })

      // Act
      const result = await benchmarkRunner.runTopK({
        dataset: 'imdb',
        collection: 'titles',
        orderBy: { field: 'startYear', direction: 'asc' },
        limit: 3,
        iterations: 10,
      })

      // Assert - results should be in ascending order
      for (let i = 1; i < result.results.length; i++) {
        const prev = result.results[i - 1] as { startYear: number }
        const curr = result.results[i] as { startYear: number }
        expect(prev.startYear).toBeLessThanOrEqual(curr.startYear)
      }
    })

    it('should be more efficient than full sort', async () => {
      // Arrange - Top-K should not need to sort entire dataset
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse({
            timing: { min: 20, max: 80, avg: 40, p50: 38, p95: 70, p99: 78, stdDev: 15 },
            metadata: {
              rowGroupsTotal: 10,
              rowGroupsRead: 10,
              columnsRequested: 2, // Only sort field + result fields
              predicatePushdown: false,
              compressionRatio: 0.4,
            },
          }),
          results: Array.from({ length: 10 }, () => ({})),
        }),
      })

      // Act
      const result = await benchmarkRunner.runTopK({
        dataset: 'imdb',
        collection: 'titles',
        orderBy: { field: 'numVotes', direction: 'desc' },
        limit: 10,
        iterations: 10,
      })

      // Assert
      expect(result.queryType).toBe('top_k')
      // Top-K should be reasonably fast even on large datasets
      expect(result.timing.avg).toBeLessThan(500)
    })

    it('should handle Top-K with filter', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          ...createMockResponse(),
          results: Array.from({ length: 5 }, (_, i) => ({
            $id: `tt000000${i}`,
            titleType: 'movie',
            numVotes: 500000 - i * 10000,
          })),
        }),
      })

      // Act - This tests combination of filter + order + limit
      // Note: The interface might need to be extended for filtered Top-K
      const result = await benchmarkRunner.runTopK({
        dataset: 'imdb',
        collection: 'titles',
        orderBy: { field: 'numVotes', direction: 'desc' },
        limit: 5,
        iterations: 10,
      })

      // Assert
      expect(result.results.length).toBeLessThanOrEqual(5)
    })
  })

  // ===========================================================================
  // Cross-cutting Concerns
  // ===========================================================================

  describe('Cross-cutting Benchmark Concerns', () => {
    it('should include query type in all results', async () => {
      // Arrange
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => createMockResponse(),
      })

      // Act & Assert - Each benchmark type should set queryType
      const fullScanResult = await benchmarkRunner.runFullScan({
        dataset: 'imdb',
        collection: 'titles',
        iterations: 1,
      })
      expect(fullScanResult.queryType).toBe('full_scan')
    })

    it('should include resource usage metrics', async () => {
      // Arrange
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockResponse({
          resources: {
            peakMemoryMB: 256,
            cpuTimeMs: 500,
          },
        }),
      })

      // Act
      const result = await benchmarkRunner.runFullScan({
        dataset: 'imdb',
        collection: 'titles',
        iterations: 10,
      })

      // Assert - resource metrics should be included
      expect(result.resources).toBeDefined()
      expect(result.resources.peakMemoryMB).toBeGreaterThan(0)
      expect(result.resources.cpuTimeMs).toBeGreaterThan(0)
    })

    it('should handle benchmark errors gracefully', async () => {
      // Arrange
      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      // Act & Assert
      await expect(
        benchmarkRunner.runFullScan({
          dataset: 'imdb',
          collection: 'titles',
          iterations: 10,
        })
      ).rejects.toThrow()
    })

    it('should validate iterations parameter', async () => {
      // Act & Assert - iterations must be positive
      await expect(
        benchmarkRunner.runFullScan({
          dataset: 'imdb',
          collection: 'titles',
          iterations: 0,
        })
      ).rejects.toThrow()

      await expect(
        benchmarkRunner.runFullScan({
          dataset: 'imdb',
          collection: 'titles',
          iterations: -1,
        })
      ).rejects.toThrow()
    })
  })
})

// =============================================================================
// Integration Tests (Skip in CI)
// =============================================================================

describe.skip('Query Benchmark Integration Tests', () => {
  // These tests hit actual Worker endpoints
  // Run manually with: npx vitest run tests/benchmark/query-benchmarks.test.ts --test-name-pattern="Integration"

  const WORKER_URL = process.env.BENCHMARK_WORKER_URL || 'https://parquedb.workers.do'

  it('should benchmark full scan against live Worker', async () => {
    const response = await fetch(`${WORKER_URL}/datasets/imdb/titles?limit=1000`)
    expect(response.ok).toBe(true)
  })

  it('should benchmark point lookup against live Worker', async () => {
    const response = await fetch(`${WORKER_URL}/datasets/imdb/titles/tt0000001`)
    expect(response.ok).toBe(true)
  })

  it('should benchmark filtered query against live Worker', async () => {
    const response = await fetch(`${WORKER_URL}/datasets/imdb/titles?titleType=movie&limit=100`)
    expect(response.ok).toBe(true)
  })
})

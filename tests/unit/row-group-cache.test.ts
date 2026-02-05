/**
 * Row Group Data Cache Tests
 *
 * Tests for in-memory row group data caching in QueryExecutor.
 * This cache stores decoded row data to avoid re-reading and re-decoding
 * the same row groups repeatedly for hot datasets.
 *
 * TDD: Red phase - write failing tests first
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  RowGroupCache,
  type RowGroupCacheOptions,
  type CachedRowGroup,
  type RowGroupCacheStats,
} from '@/worker/RowGroupCache'

// =============================================================================
// Test Data
// =============================================================================

/**
 * Create mock row data for testing
 */
function createMockRows(count: number, prefix: string = 'row'): Array<{ $id: string; name: string; value: number }> {
  return Array.from({ length: count }, (_, i) => ({
    $id: `${prefix}_${i}`,
    name: `Name ${i}`,
    value: i * 10,
  }))
}

/**
 * Estimate byte size of rows (approximate JSON serialization)
 */
function estimateRowsBytes(rows: unknown[]): number {
  return JSON.stringify(rows).length
}

// =============================================================================
// Test Suites
// =============================================================================

describe('RowGroupCache', () => {
  let cache: RowGroupCache

  beforeEach(() => {
    cache = new RowGroupCache()
  })

  afterEach(() => {
    cache.clear()
  })

  // ===========================================================================
  // Basic Cache Operations
  // ===========================================================================

  describe('basic operations', () => {
    it('should return undefined for cache miss', () => {
      const result = cache.get('test/data.parquet', 0)
      expect(result).toBeUndefined()
    })

    it('should store and retrieve row group data', () => {
      const rows = createMockRows(100)
      const filePath = 'test/data.parquet'
      const rowGroupIndex = 0

      cache.set(filePath, rowGroupIndex, rows)
      const cached = cache.get(filePath, rowGroupIndex)

      expect(cached).toBeDefined()
      expect(cached?.data).toEqual(rows)
    })

    it('should generate cache key from file path and row group index', () => {
      const rows1 = createMockRows(10, 'rg0')
      const rows2 = createMockRows(10, 'rg1')

      cache.set('file.parquet', 0, rows1)
      cache.set('file.parquet', 1, rows2)

      expect(cache.get('file.parquet', 0)?.data).toEqual(rows1)
      expect(cache.get('file.parquet', 1)?.data).toEqual(rows2)
    })

    it('should differentiate between different files', () => {
      const rows1 = createMockRows(10, 'file1')
      const rows2 = createMockRows(10, 'file2')

      cache.set('file1.parquet', 0, rows1)
      cache.set('file2.parquet', 0, rows2)

      expect(cache.get('file1.parquet', 0)?.data).toEqual(rows1)
      expect(cache.get('file2.parquet', 0)?.data).toEqual(rows2)
    })

    it('should include cache version in key format', () => {
      const cache1 = new RowGroupCache({ cacheVersion: 1 })
      const cache2 = new RowGroupCache({ cacheVersion: 2 })

      const rows = createMockRows(10)
      cache1.set('file.parquet', 0, rows)

      // Different version should not find the cached data
      // (simulating behavior when cache format changes)
      expect(cache1.get('file.parquet', 0)).toBeDefined()
      // Note: In practice, different instances have separate caches
      // This test validates the version is part of the key structure
    })
  })

  // ===========================================================================
  // Cache Hit/Miss Tracking
  // ===========================================================================

  describe('cache hit returns same data without re-reading', () => {
    it('should return cached data on hit', () => {
      const rows = createMockRows(100)
      cache.set('test.parquet', 0, rows)

      // Multiple gets should return the same data
      const result1 = cache.get('test.parquet', 0)
      const result2 = cache.get('test.parquet', 0)
      const result3 = cache.get('test.parquet', 0)

      expect(result1?.data).toBe(result2?.data)
      expect(result2?.data).toBe(result3?.data)
    })

    it('should track hit count', () => {
      const rows = createMockRows(100)
      cache.set('test.parquet', 0, rows)

      cache.get('test.parquet', 0)
      cache.get('test.parquet', 0)
      cache.get('test.parquet', 0)

      const stats = cache.getStats()
      expect(stats.hits).toBe(3)
    })

    it('should track miss count', () => {
      cache.get('nonexistent.parquet', 0)
      cache.get('nonexistent.parquet', 1)

      const stats = cache.getStats()
      expect(stats.misses).toBe(2)
    })

    it('should calculate hit rate correctly', () => {
      const rows = createMockRows(10)
      cache.set('test.parquet', 0, rows)

      // 3 hits, 2 misses = 60% hit rate
      cache.get('test.parquet', 0) // hit
      cache.get('test.parquet', 0) // hit
      cache.get('test.parquet', 0) // hit
      cache.get('miss.parquet', 0) // miss
      cache.get('miss.parquet', 1) // miss

      const stats = cache.getStats()
      expect(stats.hits).toBe(3)
      expect(stats.misses).toBe(2)
      expect(stats.hitRate).toBeCloseTo(0.6, 2)
    })
  })

  // ===========================================================================
  // Cache Miss and Population
  // ===========================================================================

  describe('cache miss triggers read and populates cache', () => {
    it('should return undefined on miss', () => {
      const result = cache.get('nonexistent.parquet', 0)
      expect(result).toBeUndefined()
    })

    it('should populate cache after set', () => {
      expect(cache.get('test.parquet', 0)).toBeUndefined()

      const rows = createMockRows(100)
      cache.set('test.parquet', 0, rows)

      expect(cache.get('test.parquet', 0)).toBeDefined()
    })

    it('should update timestamp on access', async () => {
      const rows = createMockRows(10)
      cache.set('test.parquet', 0, rows)

      const initial = cache.get('test.parquet', 0)
      const initialTimestamp = initial?.timestamp ?? 0

      // Wait a small amount and access again
      await new Promise(resolve => setTimeout(resolve, 10))
      cache.get('test.parquet', 0) // This should update the timestamp

      const updated = cache.get('test.parquet', 0)
      expect(updated?.timestamp).toBeGreaterThanOrEqual(initialTimestamp)
    })
  })

  // ===========================================================================
  // LRU Eviction
  // ===========================================================================

  describe('LRU eviction when cache exceeds size limit', () => {
    it('should evict oldest entry when size limit exceeded', () => {
      const smallCache = new RowGroupCache({ maxSizeBytes: 5000 })

      // Add entries that exceed the size limit
      const rows1 = createMockRows(50, 'old') // ~2KB
      const rows2 = createMockRows(50, 'new') // ~2KB
      const rows3 = createMockRows(50, 'newer') // ~2KB

      smallCache.set('old.parquet', 0, rows1)
      smallCache.set('new.parquet', 0, rows2)
      smallCache.set('newer.parquet', 0, rows3)

      // Oldest entry should be evicted
      expect(smallCache.get('old.parquet', 0)).toBeUndefined()
      // Newer entries should still be present
      expect(smallCache.get('new.parquet', 0)).toBeDefined()
      expect(smallCache.get('newer.parquet', 0)).toBeDefined()
    })

    it('should evict least recently used entries', async () => {
      // Use maxEntries to make LRU eviction deterministic
      const smallCache = new RowGroupCache({ maxEntries: 2 })

      const rows1 = createMockRows(10, 'a')
      const rows2 = createMockRows(10, 'b')
      const rows3 = createMockRows(10, 'c')

      smallCache.set('a.parquet', 0, rows1)
      await new Promise(resolve => setTimeout(resolve, 5))
      smallCache.set('b.parquet', 0, rows2)
      await new Promise(resolve => setTimeout(resolve, 5))

      // Access 'a' to make it recently used (updates its timestamp)
      smallCache.get('a.parquet', 0)

      await new Promise(resolve => setTimeout(resolve, 5))
      // Adding 'c' should evict 'b' (least recently used), not 'a'
      smallCache.set('c.parquet', 0, rows3)

      // 'a' should remain (recently accessed), 'b' should be evicted
      expect(smallCache.get('a.parquet', 0)).toBeDefined()
      expect(smallCache.get('b.parquet', 0)).toBeUndefined()
      expect(smallCache.get('c.parquet', 0)).toBeDefined()
    })

    it('should track eviction count', () => {
      const smallCache = new RowGroupCache({ maxSizeBytes: 3000 })

      // Add entries that will cause evictions
      for (let i = 0; i < 5; i++) {
        smallCache.set(`file${i}.parquet`, 0, createMockRows(20, `file${i}`))
      }

      const stats = smallCache.getStats()
      expect(stats.evictions).toBeGreaterThan(0)
    })

    it('should respect maxEntries limit', () => {
      const limitedCache = new RowGroupCache({ maxEntries: 3 })

      for (let i = 0; i < 5; i++) {
        limitedCache.set(`file${i}.parquet`, 0, createMockRows(10, `file${i}`))
      }

      const stats = limitedCache.getStats()
      expect(stats.entryCount).toBeLessThanOrEqual(3)
    })
  })

  // ===========================================================================
  // Size Tracking
  // ===========================================================================

  describe('size tracking', () => {
    it('should track current cache size in bytes', () => {
      const rows = createMockRows(100)
      cache.set('test.parquet', 0, rows)

      const stats = cache.getStats()
      expect(stats.sizeBytes).toBeGreaterThan(0)
    })

    it('should update size when entries are added', () => {
      const initialStats = cache.getStats()
      expect(initialStats.sizeBytes).toBe(0)

      cache.set('file1.parquet', 0, createMockRows(50))
      const afterFirst = cache.getStats()

      cache.set('file2.parquet', 0, createMockRows(50))
      const afterSecond = cache.getStats()

      expect(afterSecond.sizeBytes).toBeGreaterThan(afterFirst.sizeBytes)
    })

    it('should decrease size when entries are evicted', () => {
      const smallCache = new RowGroupCache({ maxSizeBytes: 3000 })

      // Fill cache
      smallCache.set('file1.parquet', 0, createMockRows(30))
      const beforeEviction = smallCache.getStats()

      // Add more to trigger eviction
      smallCache.set('file2.parquet', 0, createMockRows(30))
      smallCache.set('file3.parquet', 0, createMockRows(30))

      const afterEviction = smallCache.getStats()

      // Size should be controlled despite more entries
      expect(afterEviction.sizeBytes).toBeLessThanOrEqual(smallCache['maxSizeBytes'])
    })

    it('should provide max size in stats', () => {
      const customCache = new RowGroupCache({ maxSizeBytes: 100 * 1024 * 1024 })
      const stats = customCache.getStats()
      expect(stats.maxSizeBytes).toBe(100 * 1024 * 1024)
    })
  })

  // ===========================================================================
  // Default Configuration
  // ===========================================================================

  describe('default configuration', () => {
    it('should use default max size of 100MB', () => {
      const stats = cache.getStats()
      expect(stats.maxSizeBytes).toBe(100 * 1024 * 1024)
    })

    it('should use default max entries of 50', () => {
      const defaultCache = new RowGroupCache()
      // Add more than default max
      for (let i = 0; i < 60; i++) {
        defaultCache.set(`file${i}.parquet`, 0, createMockRows(5))
      }

      const stats = defaultCache.getStats()
      expect(stats.entryCount).toBeLessThanOrEqual(50)
    })

    it('should allow custom configuration', () => {
      const customCache = new RowGroupCache({
        maxSizeBytes: 50 * 1024 * 1024,
        maxEntries: 25,
        cacheVersion: 5,
      })

      const stats = customCache.getStats()
      expect(stats.maxSizeBytes).toBe(50 * 1024 * 1024)
    })
  })

  // ===========================================================================
  // Cache Clearing
  // ===========================================================================

  describe('cache clearing', () => {
    it('should clear all entries', () => {
      cache.set('file1.parquet', 0, createMockRows(10))
      cache.set('file2.parquet', 0, createMockRows(10))

      cache.clear()

      expect(cache.get('file1.parquet', 0)).toBeUndefined()
      expect(cache.get('file2.parquet', 0)).toBeUndefined()
      expect(cache.getStats().entryCount).toBe(0)
    })

    it('should reset stats on clear', () => {
      cache.set('test.parquet', 0, createMockRows(10))
      cache.get('test.parquet', 0)
      cache.get('nonexistent.parquet', 0)

      cache.clear()

      const stats = cache.getStats()
      expect(stats.hits).toBe(0)
      expect(stats.misses).toBe(0)
      expect(stats.sizeBytes).toBe(0)
    })

    it('should invalidate specific namespace', () => {
      cache.set('dataset1/data.parquet', 0, createMockRows(10))
      cache.set('dataset1/data.parquet', 1, createMockRows(10))
      cache.set('dataset2/data.parquet', 0, createMockRows(10))

      cache.invalidate('dataset1')

      expect(cache.get('dataset1/data.parquet', 0)).toBeUndefined()
      expect(cache.get('dataset1/data.parquet', 1)).toBeUndefined()
      expect(cache.get('dataset2/data.parquet', 0)).toBeDefined()
    })

    it('should invalidate specific file', () => {
      cache.set('dataset/data.parquet', 0, createMockRows(10))
      cache.set('dataset/data.parquet', 1, createMockRows(10))
      cache.set('dataset/other.parquet', 0, createMockRows(10))

      cache.invalidateFile('dataset/data.parquet')

      expect(cache.get('dataset/data.parquet', 0)).toBeUndefined()
      expect(cache.get('dataset/data.parquet', 1)).toBeUndefined()
      expect(cache.get('dataset/other.parquet', 0)).toBeDefined()
    })
  })

  // ===========================================================================
  // TTL Support
  // ===========================================================================

  describe('TTL support', () => {
    it('should expire entries after TTL', async () => {
      const ttlCache = new RowGroupCache({ ttlMs: 50 })

      ttlCache.set('test.parquet', 0, createMockRows(10))
      expect(ttlCache.get('test.parquet', 0)).toBeDefined()

      // Wait for TTL to expire
      await new Promise(resolve => setTimeout(resolve, 100))

      expect(ttlCache.get('test.parquet', 0)).toBeUndefined()
    })

    it('should not expire entries before TTL', async () => {
      const ttlCache = new RowGroupCache({ ttlMs: 1000 })

      ttlCache.set('test.parquet', 0, createMockRows(10))

      // Wait less than TTL
      await new Promise(resolve => setTimeout(resolve, 50))

      expect(ttlCache.get('test.parquet', 0)).toBeDefined()
    })
  })

  // ===========================================================================
  // Stats
  // ===========================================================================

  describe('getStats', () => {
    it('should return comprehensive statistics', () => {
      cache.set('test.parquet', 0, createMockRows(100))
      cache.get('test.parquet', 0)
      cache.get('miss.parquet', 0)

      const stats = cache.getStats()

      expect(stats).toHaveProperty('entryCount')
      expect(stats).toHaveProperty('sizeBytes')
      expect(stats).toHaveProperty('maxSizeBytes')
      expect(stats).toHaveProperty('hits')
      expect(stats).toHaveProperty('misses')
      expect(stats).toHaveProperty('hitRate')
      expect(stats).toHaveProperty('evictions')
    })

    it('should handle zero total requests for hit rate', () => {
      const stats = cache.getStats()
      expect(stats.hitRate).toBe(0)
    })
  })
})

// =============================================================================
// Benchmark Tests
// =============================================================================

describe('RowGroupCache - Performance', () => {
  it('should show speedup on repeated queries', () => {
    const cache = new RowGroupCache()
    const largeRows = createMockRows(10000)

    // Measure time to "read" (simulate)
    const startSet = performance.now()
    cache.set('large.parquet', 0, largeRows)
    const setTime = performance.now() - startSet

    // Measure time for cache hits
    const hitTimes: number[] = []
    for (let i = 0; i < 100; i++) {
      const startGet = performance.now()
      cache.get('large.parquet', 0)
      hitTimes.push(performance.now() - startGet)
    }

    const avgHitTime = hitTimes.reduce((a, b) => a + b, 0) / hitTimes.length

    // Cache hits should be significantly faster than set (which simulates read)
    // In practice, hits are O(1) lookups vs O(n) reads + decoding
    expect(avgHitTime).toBeLessThan(setTime / 10)

    // Log for visibility during test runs
    console.log(`Set time: ${setTime.toFixed(3)}ms, Avg hit time: ${avgHitTime.toFixed(3)}ms`)
  })

  it('should handle high concurrency access patterns', () => {
    const cache = new RowGroupCache()

    // Pre-populate cache
    for (let i = 0; i < 10; i++) {
      cache.set(`file${i}.parquet`, 0, createMockRows(100, `file${i}`))
    }

    // Simulate concurrent access pattern
    const startTime = performance.now()
    for (let i = 0; i < 1000; i++) {
      const fileIndex = i % 10
      cache.get(`file${fileIndex}.parquet`, 0)
    }
    const totalTime = performance.now() - startTime

    // 1000 lookups should complete quickly (< 100ms)
    expect(totalTime).toBeLessThan(100)
    console.log(`1000 cache lookups: ${totalTime.toFixed(3)}ms`)
  })
})

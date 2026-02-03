/**
 * TTL Cache Tests
 *
 * Tests for the TTLCache utility class which provides:
 * - Time-to-live (TTL) for cached entries
 * - Maximum entry limit with LRU eviction
 * - Manual invalidation
 * - Cache statistics
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { TTLCache } from '@/utils/ttl-cache'

describe('TTLCache', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ===========================================================================
  // Basic Operations
  // ===========================================================================

  describe('Basic Operations', () => {
    it('should store and retrieve values', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      expect(cache.get('key1')).toBe('value1')
      expect(cache.get('key2')).toBe('value2')
    })

    it('should return undefined for missing keys', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      expect(cache.get('missing')).toBeUndefined()
    })

    it('should overwrite existing values', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value1')
      cache.set('key', 'value2')

      expect(cache.get('key')).toBe('value2')
    })

    it('should check if key exists with has()', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value')

      expect(cache.has('key')).toBe(true)
      expect(cache.has('missing')).toBe(false)
    })

    it('should delete individual entries', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value')
      expect(cache.has('key')).toBe(true)

      cache.delete('key')
      expect(cache.has('key')).toBe(false)
    })

    it('should clear all entries', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      cache.clear()

      expect(cache.has('key1')).toBe(false)
      expect(cache.has('key2')).toBe(false)
      expect(cache.size).toBe(0)
    })

    it('should report correct size', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      expect(cache.size).toBe(0)

      cache.set('key1', 'value1')
      expect(cache.size).toBe(1)

      cache.set('key2', 'value2')
      expect(cache.size).toBe(2)

      cache.delete('key1')
      expect(cache.size).toBe(1)
    })
  })

  // ===========================================================================
  // TTL Expiration
  // ===========================================================================

  describe('TTL Expiration', () => {
    it('should return value before TTL expires', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key', 'value')

      // Advance time by less than TTL
      vi.advanceTimersByTime(4000)

      expect(cache.get('key')).toBe('value')
    })

    it('should return undefined after TTL expires', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key', 'value')

      // Advance time past TTL
      vi.advanceTimersByTime(6000)

      expect(cache.get('key')).toBeUndefined()
    })

    it('should report has() as false after TTL expires', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key', 'value')

      vi.advanceTimersByTime(6000)

      expect(cache.has('key')).toBe(false)
    })

    it('should lazily remove expired entries on access', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      expect(cache.size).toBe(2)

      vi.advanceTimersByTime(6000)

      // Access expired entries to trigger cleanup
      cache.get('key1') // Cleans up key1
      cache.get('key2') // Cleans up key2

      // Size should be 0 after both expired entries are accessed
      expect(cache.size).toBe(0)
    })

    it('should refresh TTL on update', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key', 'value1')

      // Advance time by 4 seconds
      vi.advanceTimersByTime(4000)

      // Update the value (should reset TTL)
      cache.set('key', 'value2')

      // Advance by another 4 seconds (8 total from first set, but only 4 from second)
      vi.advanceTimersByTime(4000)

      // Should still be valid because TTL was reset
      expect(cache.get('key')).toBe('value2')

      // Advance past the new TTL
      vi.advanceTimersByTime(2000)

      // Now it should be expired
      expect(cache.get('key')).toBeUndefined()
    })

    it('should handle zero TTL (immediate expiration)', () => {
      const cache = new TTLCache<string>({ ttlMs: 0 })

      cache.set('key', 'value')

      // Even with no time advance, should be expired
      vi.advanceTimersByTime(1)
      expect(cache.get('key')).toBeUndefined()
    })
  })

  // ===========================================================================
  // Max Entries and LRU Eviction
  // ===========================================================================

  describe('Max Entries and LRU Eviction', () => {
    it('should evict oldest entry when max entries exceeded', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 3 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')
      cache.set('key3', 'value3')

      expect(cache.size).toBe(3)

      // Add a fourth entry, should evict key1
      cache.set('key4', 'value4')

      expect(cache.size).toBe(3)
      expect(cache.has('key1')).toBe(false) // Evicted
      expect(cache.has('key2')).toBe(true)
      expect(cache.has('key3')).toBe(true)
      expect(cache.has('key4')).toBe(true)
    })

    it('should update LRU order on get()', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 3 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')
      cache.set('key3', 'value3')

      // Access key1, making it most recently used
      cache.get('key1')

      // Add a fourth entry, should evict key2 (now least recently used)
      cache.set('key4', 'value4')

      expect(cache.has('key1')).toBe(true) // Still here because we accessed it
      expect(cache.has('key2')).toBe(false) // Evicted
      expect(cache.has('key3')).toBe(true)
      expect(cache.has('key4')).toBe(true)
    })

    it('should update LRU order on set() for existing key', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 3 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')
      cache.set('key3', 'value3')

      // Update key1, making it most recently used
      cache.set('key1', 'updated')

      // Add a fourth entry, should evict key2 (now least recently used)
      cache.set('key4', 'value4')

      expect(cache.has('key1')).toBe(true)
      expect(cache.get('key1')).toBe('updated')
      expect(cache.has('key2')).toBe(false) // Evicted
    })

    it('should handle maxEntries of 1', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 1 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      expect(cache.size).toBe(1)
      expect(cache.has('key1')).toBe(false)
      expect(cache.has('key2')).toBe(true)
    })

    it('should not evict when no maxEntries limit', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 }) // No maxEntries

      for (let i = 0; i < 100; i++) {
        cache.set(`key${i}`, `value${i}`)
      }

      expect(cache.size).toBe(100)
    })
  })

  // ===========================================================================
  // Statistics
  // ===========================================================================

  describe('Statistics', () => {
    it('should track cache hits', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value')

      cache.get('key')
      cache.get('key')
      cache.get('key')

      const stats = cache.getStats()
      expect(stats.hits).toBe(3)
    })

    it('should track cache misses', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.get('missing1')
      cache.get('missing2')

      const stats = cache.getStats()
      expect(stats.misses).toBe(2)
    })

    it('should track expired entries as misses', () => {
      const cache = new TTLCache<string>({ ttlMs: 5000 })

      cache.set('key', 'value')

      // Access before expiration (hit)
      cache.get('key')

      vi.advanceTimersByTime(6000)

      // Access after expiration (miss)
      cache.get('key')

      const stats = cache.getStats()
      expect(stats.hits).toBe(1)
      expect(stats.misses).toBe(1)
    })

    it('should track evictions', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 2 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')
      cache.set('key3', 'value3') // Evicts key1
      cache.set('key4', 'value4') // Evicts key2

      const stats = cache.getStats()
      expect(stats.evictions).toBe(2)
    })

    it('should report current size and max entries', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000, maxEntries: 10 })

      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      const stats = cache.getStats()
      expect(stats.size).toBe(2)
      expect(stats.maxEntries).toBe(10)
    })

    it('should calculate hit rate', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value')

      cache.get('key') // Hit
      cache.get('key') // Hit
      cache.get('missing') // Miss

      const stats = cache.getStats()
      expect(stats.hitRate).toBeCloseTo(0.667, 2)
    })

    it('should reset stats on clear', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('key', 'value')
      cache.get('key')
      cache.get('missing')

      cache.clear()

      const stats = cache.getStats()
      expect(stats.hits).toBe(0)
      expect(stats.misses).toBe(0)
      expect(stats.evictions).toBe(0)
    })
  })

  // ===========================================================================
  // Invalidation
  // ===========================================================================

  describe('Invalidation', () => {
    it('should invalidate by prefix', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('posts/data.parquet', 'posts-data')
      cache.set('posts/rels.parquet', 'posts-rels')
      cache.set('users/data.parquet', 'users-data')

      cache.invalidateByPrefix('posts/')

      expect(cache.has('posts/data.parquet')).toBe(false)
      expect(cache.has('posts/rels.parquet')).toBe(false)
      expect(cache.has('users/data.parquet')).toBe(true)
    })

    it('should invalidate by pattern (regex)', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('data/posts/v1.parquet', 'posts-v1')
      cache.set('data/posts/v2.parquet', 'posts-v2')
      cache.set('data/users/v1.parquet', 'users-v1')

      cache.invalidateByPattern(/posts/)

      expect(cache.has('data/posts/v1.parquet')).toBe(false)
      expect(cache.has('data/posts/v2.parquet')).toBe(false)
      expect(cache.has('data/users/v1.parquet')).toBe(true)
    })

    it('should return count of invalidated entries', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('posts/1', 'a')
      cache.set('posts/2', 'b')
      cache.set('posts/3', 'c')
      cache.set('users/1', 'd')

      const count = cache.invalidateByPrefix('posts/')

      expect(count).toBe(3)
    })
  })

  // ===========================================================================
  // Type Safety
  // ===========================================================================

  describe('Type Safety', () => {
    it('should work with complex types', () => {
      interface CacheEntry {
        data: unknown[]
        metadata: { rowCount: number }
      }

      const cache = new TTLCache<CacheEntry>({ ttlMs: 60000 })

      const entry: CacheEntry = {
        data: [{ id: 1 }, { id: 2 }],
        metadata: { rowCount: 2 },
      }

      cache.set('key', entry)

      const retrieved = cache.get('key')
      expect(retrieved).toEqual(entry)
      expect(retrieved?.metadata.rowCount).toBe(2)
    })

    it('should work with arrays', () => {
      const cache = new TTLCache<unknown[]>({ ttlMs: 60000 })

      const data = [1, 2, 3, 4, 5]
      cache.set('key', data)

      const retrieved = cache.get('key')
      expect(retrieved).toEqual(data)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle empty string keys', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      cache.set('', 'empty-key-value')

      expect(cache.get('')).toBe('empty-key-value')
    })

    it('should handle very long keys', () => {
      const cache = new TTLCache<string>({ ttlMs: 60000 })

      const longKey = 'a'.repeat(10000)
      cache.set(longKey, 'value')

      expect(cache.get(longKey)).toBe('value')
    })

    it('should handle null and undefined values', () => {
      const cache = new TTLCache<string | null | undefined>({ ttlMs: 60000 })

      cache.set('null-key', null)
      cache.set('undefined-key', undefined)

      // These should still be considered "set" even though values are null/undefined
      expect(cache.has('null-key')).toBe(true)
      expect(cache.has('undefined-key')).toBe(true)
      expect(cache.get('null-key')).toBeNull()
      expect(cache.get('undefined-key')).toBeUndefined()
    })

    it('should handle concurrent access patterns', () => {
      const cache = new TTLCache<number>({ ttlMs: 60000, maxEntries: 100 })

      // Simulate concurrent access
      for (let i = 0; i < 1000; i++) {
        const key = `key${i % 50}`
        if (i % 2 === 0) {
          cache.set(key, i)
        } else {
          cache.get(key)
        }
      }

      // Cache should still be operational
      expect(cache.size).toBeLessThanOrEqual(100)
    })
  })
})

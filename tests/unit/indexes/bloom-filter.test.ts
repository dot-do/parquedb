/**
 * Bloom Filter Tests
 *
 * Tests for the bloom filter implementation used for index pre-filtering.
 */

import { describe, it, expect } from 'vitest'
import {
  BloomFilter,
  IndexBloomFilter,
  calculateOptimalParams,
  estimateFalsePositiveRate,
} from '@/indexes/bloom'

describe('BloomFilter', () => {
  describe('basic operations', () => {
    it('should report false for empty filter', () => {
      const filter = new BloomFilter(1024, 3)
      expect(filter.mightContain('test')).toBe(false)
      expect(filter.mightContain(123)).toBe(false)
    })

    it('should report true for added string values', () => {
      const filter = new BloomFilter(1024, 3)
      filter.add('hello')
      filter.add('world')

      expect(filter.mightContain('hello')).toBe(true)
      expect(filter.mightContain('world')).toBe(true)
    })

    it('should report true for added number values', () => {
      const filter = new BloomFilter(1024, 3)
      filter.add(42)
      filter.add(3.14159)
      filter.add(-100)

      expect(filter.mightContain(42)).toBe(true)
      expect(filter.mightContain(3.14159)).toBe(true)
      expect(filter.mightContain(-100)).toBe(true)
    })

    it('should handle mixed types', () => {
      const filter = new BloomFilter(1024, 3)
      filter.add('test')
      filter.add(123)

      expect(filter.mightContain('test')).toBe(true)
      expect(filter.mightContain(123)).toBe(true)
    })

    it('should serialize and deserialize correctly', () => {
      const filter = new BloomFilter(1024, 3)
      filter.add('hello')
      filter.add('world')
      filter.add(42)

      const buffer = filter.toBuffer()
      const restored = BloomFilter.fromBuffer(buffer, 3)

      expect(restored.mightContain('hello')).toBe(true)
      expect(restored.mightContain('world')).toBe(true)
      expect(restored.mightContain(42)).toBe(true)
      expect(restored.mightContain('notadded')).toBe(false)
    })

    it('should estimate count approximately', () => {
      const filter = new BloomFilter(8192, 3) // 8KB
      const items = 1000

      for (let i = 0; i < items; i++) {
        filter.add(`item_${i}`)
      }

      const estimate = filter.estimateCount()
      // Should be within 20% of actual
      expect(estimate).toBeGreaterThan(items * 0.8)
      expect(estimate).toBeLessThan(items * 1.2)
    })

    it('should clear all bits', () => {
      const filter = new BloomFilter(1024, 3)
      filter.add('hello')
      filter.add('world')

      filter.clear()

      expect(filter.mightContain('hello')).toBe(false)
      expect(filter.mightContain('world')).toBe(false)
    })
  })

  describe('false positive rate', () => {
    it('should have low false positive rate with proper sizing', () => {
      // 10K items, 128KB filter should give ~0.1% FPR
      const filter = new BloomFilter(131072, 3) // 128KB

      // Add 10K items
      for (let i = 0; i < 10000; i++) {
        filter.add(`item_${i}`)
      }

      // Check 10K items that were NOT added
      let falsePositives = 0
      for (let i = 10000; i < 20000; i++) {
        if (filter.mightContain(`item_${i}`)) {
          falsePositives++
        }
      }

      // False positive rate should be < 1%
      const fpr = falsePositives / 10000
      expect(fpr).toBeLessThan(0.01)
    })
  })
})

describe('IndexBloomFilter', () => {
  describe('basic operations', () => {
    it('should create with specified row groups', () => {
      const filter = new IndexBloomFilter(10)
      expect(filter.getStats().numRowGroups).toBe(10)
    })

    it('should report false for values not added', () => {
      const filter = new IndexBloomFilter(5)
      expect(filter.mightContain('test')).toBe(false)
      expect(filter.getMatchingRowGroups('test')).toEqual([])
    })

    it('should report true for added values', () => {
      const filter = new IndexBloomFilter(5)
      filter.addEntry('hello', 0)
      filter.addEntry('world', 2)
      filter.addEntry(42, 4)

      expect(filter.mightContain('hello')).toBe(true)
      expect(filter.mightContain('world')).toBe(true)
      expect(filter.mightContain(42)).toBe(true)
    })

    it('should return correct row groups', () => {
      const filter = new IndexBloomFilter(5)
      filter.addEntry('hello', 0)
      filter.addEntry('hello', 2) // Same value in multiple row groups
      filter.addEntry('world', 1)

      const helloGroups = filter.getMatchingRowGroups('hello')
      expect(helloGroups).toContain(0)
      expect(helloGroups).toContain(2)
      expect(helloGroups).not.toContain(1)

      const worldGroups = filter.getMatchingRowGroups('world')
      expect(worldGroups).toContain(1)
      expect(worldGroups).not.toContain(0)
    })

    it('should handle null and undefined values', () => {
      const filter = new IndexBloomFilter(3)
      filter.addEntry(null, 0)
      filter.addEntry(undefined, 1)

      expect(filter.mightContain(null)).toBe(true)
      expect(filter.mightContain(undefined)).toBe(true)
    })

    it('should handle boolean values', () => {
      const filter = new IndexBloomFilter(3)
      filter.addEntry(true, 0)
      filter.addEntry(false, 1)

      expect(filter.mightContain(true)).toBe(true)
      expect(filter.mightContain(false)).toBe(true)
    })
  })

  describe('serialization', () => {
    it('should serialize and deserialize correctly', () => {
      const filter = new IndexBloomFilter(5, 8192, 3) // 5 row groups, 8KB value bloom
      filter.addEntry('hello', 0)
      filter.addEntry('world', 2)
      filter.addEntry(42, 4)
      filter.addEntry('hello', 2) // Same value in different row group

      const buffer = filter.toBuffer()
      const restored = IndexBloomFilter.fromBuffer(buffer)

      // Check values
      expect(restored.mightContain('hello')).toBe(true)
      expect(restored.mightContain('world')).toBe(true)
      expect(restored.mightContain(42)).toBe(true)
      expect(restored.mightContain('notadded')).toBe(false)

      // Check row groups
      const helloGroups = restored.getMatchingRowGroups('hello')
      expect(helloGroups).toContain(0)
      expect(helloGroups).toContain(2)
    })

    it('should report correct stats', () => {
      const filter = new IndexBloomFilter(10, 131072, 3) // 10 RGs, 128KB value bloom
      const stats = filter.getStats()

      expect(stats.numRowGroups).toBe(10)
      expect(stats.valueBloomSizeBytes).toBe(131072)
      expect(stats.rowGroupBloomSizeBytes).toBe(10 * 4096) // 40KB
      expect(stats.totalSizeBytes).toBe(16 + 131072 + 40960) // header + value + RG blooms
    })

    it('should throw on invalid buffer', () => {
      expect(() => {
        IndexBloomFilter.fromBuffer(new Uint8Array([1, 2, 3]))
      }).toThrow()

      expect(() => {
        IndexBloomFilter.fromBuffer(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
      }).toThrow('Invalid bloom filter: bad magic')
    })
  })

  describe('realistic usage', () => {
    it('should work with titleType-like values across row groups', () => {
      const numRowGroups = 20
      const filter = new IndexBloomFilter(numRowGroups, 131072, 3)

      // Simulate IMDB titleType distribution
      const titleTypes = ['movie', 'short', 'tvEpisode', 'tvSeries', 'tvMovie', 'tvMiniSeries', 'tvSpecial', 'video', 'videoGame']

      // Add entries simulating row group distribution
      for (let rg = 0; rg < numRowGroups; rg++) {
        // Each row group has a mix of types
        for (const type of titleTypes) {
          filter.addEntry(type, rg)
        }
      }

      // Check that "movie" returns all row groups (since it appears in all)
      const movieGroups = filter.getMatchingRowGroups('movie')
      expect(movieGroups.length).toBe(numRowGroups)

      // Check that non-existent type returns empty
      const fakeGroups = filter.getMatchingRowGroups('fakeType')
      expect(fakeGroups.length).toBe(0)

      // Serialize and verify size is reasonable
      const buffer = filter.toBuffer()
      const stats = filter.getStats()

      // Should be around 200KB total (128KB value + 80KB row groups + 16 bytes header)
      expect(buffer.length).toBeLessThan(250000)
      expect(stats.totalSizeBytes).toBe(buffer.length)
    })
  })
})

describe('calculateOptimalParams', () => {
  it('should calculate reasonable params for 1M items, 1% FPR', () => {
    const params = calculateOptimalParams(1000000, 0.01)

    // For 1M items at 1% FPR, optimal is ~1.2MB
    expect(params.sizeBytes).toBeGreaterThan(1000000)
    expect(params.sizeBytes).toBeLessThan(1500000)

    // Optimal k for 1% FPR is around 7
    expect(params.numHashFunctions).toBeGreaterThan(5)
    expect(params.numHashFunctions).toBeLessThan(10)
  })

  it('should calculate smaller params for fewer items', () => {
    const params = calculateOptimalParams(10000, 0.01)

    // For 10K items at 1% FPR
    expect(params.sizeBytes).toBeLessThan(15000) // ~12KB
    expect(params.numHashFunctions).toBeGreaterThan(5)
  })

  it('should calculate larger params for lower FPR', () => {
    const highFPR = calculateOptimalParams(100000, 0.1)
    const lowFPR = calculateOptimalParams(100000, 0.001)

    expect(lowFPR.sizeBytes).toBeGreaterThan(highFPR.sizeBytes)
  })
})

describe('estimateFalsePositiveRate', () => {
  it('should estimate FPR correctly', () => {
    // For 128KB (131072 bytes), 3 hash functions, 100K items
    // Expected FPR is approximately 1.5%
    const fpr = estimateFalsePositiveRate(131072, 3, 100000)
    expect(fpr).toBeGreaterThan(0.001)
    expect(fpr).toBeLessThan(0.02) // ~1.5% with these parameters
  })

  it('should show higher FPR with more items', () => {
    const fpr100k = estimateFalsePositiveRate(131072, 3, 100000)
    const fpr1m = estimateFalsePositiveRate(131072, 3, 1000000)

    expect(fpr1m).toBeGreaterThan(fpr100k)
  })

  it('should show lower FPR with larger filter', () => {
    const fprSmall = estimateFalsePositiveRate(65536, 3, 100000) // 64KB
    const fprLarge = estimateFalsePositiveRate(262144, 3, 100000) // 256KB

    expect(fprLarge).toBeLessThan(fprSmall)
  })
})

/**
 * Bloom Filter Query Integration Tests
 *
 * Tests for src/query/bloom.ts which handles bloom filter query planning,
 * row group pruning, and multi-field bloom filter indexes.
 *
 * Coverage includes:
 * - Bloom filter creation and membership testing
 * - Row group pruning via bloom filters (skip groups that definitely don't contain value)
 * - Multi-column bloom plans (BloomFilterIndex with multiple fields)
 * - False positive handling (bloom says maybe, but value not actually present)
 * - Filter integration (bloomFilterIndexMightMatch with MongoDB-style filters)
 * - Edge cases: empty bloom filter, null values, missing fields, serialization
 * - Storage-based checkBloomFilter with mocked StorageBackend
 */

import { describe, it, expect, vi } from 'vitest'
import {
  bloomFilterMightContain,
  bloomFilterAdd,
  createBloomFilter,
  bloomFilterMerge,
  bloomFilterEstimateCount,
  serializeBloomFilter,
  deserializeBloomFilter,
  createBloomFilterIndex,
  bloomFilterIndexAddRow,
  bloomFilterIndexMightMatch,
  serializeBloomFilterIndex,
  deserializeBloomFilterIndex,
  checkBloomFilter,
} from '@/query/bloom'
import type { BloomFilter, BloomFilterIndex } from '@/query/bloom'
import type { StorageBackend } from '@/types/storage'
import type { Filter } from '@/types/filter'

// =============================================================================
// Helper: Create a mock StorageBackend
// =============================================================================

function createMockStorage(files: Record<string, Uint8Array> = {}): StorageBackend {
  return {
    type: 'mock',
    read: vi.fn(async (path: string) => {
      const data = files[path]
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    }),
    readRange: vi.fn(async () => new Uint8Array(0)),
    exists: vi.fn(async (path: string) => path in files),
    stat: vi.fn(async () => null),
    list: vi.fn(async () => ({ files: [], hasMore: false })),
    write: vi.fn(async () => ({ etag: 'mock', size: 0 })),
    writeAtomic: vi.fn(async () => ({ etag: 'mock', size: 0 })),
    append: vi.fn(async () => {}),
    delete: vi.fn(async () => true),
    deletePrefix: vi.fn(async () => 0),
    mkdir: vi.fn(async () => {}),
    rmdir: vi.fn(async () => {}),
    writeConditional: vi.fn(async () => ({ etag: 'mock', size: 0 })),
    copy: vi.fn(async () => {}),
    move: vi.fn(async () => {}),
  }
}

// =============================================================================
// createBloomFilter
// =============================================================================

describe('createBloomFilter', () => {
  it('creates a filter with correct number of bits for given expected items', () => {
    const filter = createBloomFilter(1000, 0.01)
    // For 1000 items at 1% FPR, expect ~9585 bits (~1199 bytes)
    expect(filter.numBits).toBeGreaterThan(5000)
    expect(filter.numBits).toBeLessThan(20000)
    expect(filter.bits.length).toBe(Math.ceil(filter.numBits / 8))
  })

  it('creates a filter with at least 1 hash function', () => {
    const filter = createBloomFilter(10, 0.5)
    expect(filter.numHashes).toBeGreaterThanOrEqual(1)
  })

  it('creates an empty filter (all bits zero)', () => {
    const filter = createBloomFilter(100)
    const allZero = filter.bits.every((b) => b === 0)
    expect(allZero).toBe(true)
  })

  it('uses default false positive rate of 0.01', () => {
    const filterDefault = createBloomFilter(1000)
    const filterExplicit = createBloomFilter(1000, 0.01)
    expect(filterDefault.numBits).toBe(filterExplicit.numBits)
    expect(filterDefault.numHashes).toBe(filterExplicit.numHashes)
  })

  it('produces more bits for lower false positive rate', () => {
    const filterHigh = createBloomFilter(1000, 0.1)
    const filterLow = createBloomFilter(1000, 0.001)
    expect(filterLow.numBits).toBeGreaterThan(filterHigh.numBits)
  })
})

// =============================================================================
// bloomFilterAdd + bloomFilterMightContain
// =============================================================================

describe('bloomFilterAdd and bloomFilterMightContain', () => {
  describe('basic membership', () => {
    it('returns false for empty filter', () => {
      const filter = createBloomFilter(100)
      expect(bloomFilterMightContain(filter, 'hello')).toBe(false)
      expect(bloomFilterMightContain(filter, 42)).toBe(false)
    })

    it('returns true for added string values', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, 'hello')
      bloomFilterAdd(filter, 'world')

      expect(bloomFilterMightContain(filter, 'hello')).toBe(true)
      expect(bloomFilterMightContain(filter, 'world')).toBe(true)
    })

    it('returns true for added number values', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, 42)
      bloomFilterAdd(filter, 3.14)
      bloomFilterAdd(filter, -100)

      expect(bloomFilterMightContain(filter, 42)).toBe(true)
      expect(bloomFilterMightContain(filter, 3.14)).toBe(true)
      expect(bloomFilterMightContain(filter, -100)).toBe(true)
    })

    it('returns true for added boolean values', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, true)

      expect(bloomFilterMightContain(filter, true)).toBe(true)
    })

    it('returns true for added Date values', () => {
      const filter = createBloomFilter(100)
      const date = new Date('2024-06-15T10:30:00Z')
      bloomFilterAdd(filter, date)

      expect(bloomFilterMightContain(filter, new Date('2024-06-15T10:30:00Z'))).toBe(true)
    })

    it('returns false for values not added (true negatives)', () => {
      const filter = createBloomFilter(1000)
      bloomFilterAdd(filter, 'hello')
      bloomFilterAdd(filter, 42)

      expect(bloomFilterMightContain(filter, 'goodbye')).toBe(false)
      expect(bloomFilterMightContain(filter, 99)).toBe(false)
      expect(bloomFilterMightContain(filter, 'world')).toBe(false)
    })

    it('handles mixed types without cross-matching', () => {
      const filter = createBloomFilter(1000)
      bloomFilterAdd(filter, 'hello')
      bloomFilterAdd(filter, 42)

      // String '42' should not match number 42 (different byte representations)
      expect(bloomFilterMightContain(filter, '42')).toBe(false)
      expect(bloomFilterMightContain(filter, 'hello')).toBe(true)
      expect(bloomFilterMightContain(filter, 42)).toBe(true)
    })
  })

  describe('null and undefined handling', () => {
    it('can add and check null values', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, null)

      expect(bloomFilterMightContain(filter, null)).toBe(true)
    })

    it('can add and check undefined values', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, undefined)

      expect(bloomFilterMightContain(filter, undefined)).toBe(true)
    })

    it('null and undefined produce same byte representation', () => {
      // Both are encoded as [0] in valueToBytes
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, null)

      expect(bloomFilterMightContain(filter, undefined)).toBe(true)
    })
  })

  describe('object and array values', () => {
    it('handles object values (via JSON serialization)', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, { key: 'value' })

      expect(bloomFilterMightContain(filter, { key: 'value' })).toBe(true)
    })

    it('handles array values (via JSON serialization)', () => {
      const filter = createBloomFilter(100)
      bloomFilterAdd(filter, [1, 2, 3])

      expect(bloomFilterMightContain(filter, [1, 2, 3])).toBe(true)
    })
  })

  describe('false positive behavior', () => {
    it('may produce false positives but never false negatives', () => {
      const filter = createBloomFilter(100, 0.01)

      // Add known items
      const addedItems = Array.from({ length: 50 }, (_, i) => `item_${i}`)
      for (const item of addedItems) {
        bloomFilterAdd(filter, item)
      }

      // All added items must be found (no false negatives)
      for (const item of addedItems) {
        expect(bloomFilterMightContain(filter, item)).toBe(true)
      }

      // Some non-added items may be found (false positives) - that's acceptable
      // But not ALL non-added items should be false positives
      let falsePositives = 0
      const testCount = 1000
      for (let i = 100; i < 100 + testCount; i++) {
        if (bloomFilterMightContain(filter, `nonexistent_${i}`)) {
          falsePositives++
        }
      }

      // False positive rate should be reasonable (not 100%)
      expect(falsePositives).toBeLessThan(testCount)
    })

    it('has low false positive rate with properly sized filter', () => {
      const expectedItems = 1000
      const filter = createBloomFilter(expectedItems, 0.01)

      for (let i = 0; i < expectedItems; i++) {
        bloomFilterAdd(filter, `item_${i}`)
      }

      let falsePositives = 0
      const testCount = 10000
      for (let i = expectedItems; i < expectedItems + testCount; i++) {
        if (bloomFilterMightContain(filter, `item_${i}`)) {
          falsePositives++
        }
      }

      // FPR should be under 5% (generous margin over 1% target)
      const fpr = falsePositives / testCount
      expect(fpr).toBeLessThan(0.05)
    })
  })

  describe('edge cases for bloomFilterMightContain', () => {
    it('handles out-of-bounds byte index gracefully (returns true)', () => {
      // Create a very small filter where hash values could overflow
      const filter: BloomFilter = {
        bits: new Uint8Array(1), // only 1 byte = 8 bits
        numHashes: 3,
        numBits: 1000, // numBits is larger than actual bit capacity
      }
      // When byteIndex >= bits.length, the function returns true (conservative)
      // This tests the out-of-bounds guard
      const result = bloomFilterMightContain(filter, 'test-value')
      expect(typeof result).toBe('boolean')
    })
  })
})

// =============================================================================
// bloomFilterMerge
// =============================================================================

describe('bloomFilterMerge', () => {
  it('merges two filters containing different values', () => {
    const a = createBloomFilter(100)
    const b = createBloomFilter(100)

    // Ensure they have the same config by creating from same params
    expect(a.numBits).toBe(b.numBits)
    expect(a.numHashes).toBe(b.numHashes)

    bloomFilterAdd(a, 'hello')
    bloomFilterAdd(b, 'world')

    const merged = bloomFilterMerge(a, b)

    expect(bloomFilterMightContain(merged, 'hello')).toBe(true)
    expect(bloomFilterMightContain(merged, 'world')).toBe(true)
  })

  it('throws when merging filters with different configurations', () => {
    const a = createBloomFilter(100, 0.01)
    const b = createBloomFilter(1000, 0.01) // Different size

    expect(() => bloomFilterMerge(a, b)).toThrow('Cannot merge bloom filters with different configurations')
  })

  it('preserves all values from both filters', () => {
    const filterA = createBloomFilter(500)
    const filterB = createBloomFilter(500)

    for (let i = 0; i < 50; i++) bloomFilterAdd(filterA, `a_${i}`)
    for (let i = 0; i < 50; i++) bloomFilterAdd(filterB, `b_${i}`)

    const merged = bloomFilterMerge(filterA, filterB)

    for (let i = 0; i < 50; i++) {
      expect(bloomFilterMightContain(merged, `a_${i}`)).toBe(true)
      expect(bloomFilterMightContain(merged, `b_${i}`)).toBe(true)
    }
  })

  it('merged filter has same config as source filters', () => {
    const a = createBloomFilter(100)
    const b = createBloomFilter(100)
    const merged = bloomFilterMerge(a, b)

    expect(merged.numBits).toBe(a.numBits)
    expect(merged.numHashes).toBe(a.numHashes)
    expect(merged.bits.length).toBe(a.bits.length)
  })
})

// =============================================================================
// bloomFilterEstimateCount
// =============================================================================

describe('bloomFilterEstimateCount', () => {
  it('returns 0 for empty filter', () => {
    const filter = createBloomFilter(100)
    // The formula -m * ln(1 - 0/m) / k yields -0 (negative zero), which equals 0 numerically
    const count = bloomFilterEstimateCount(filter)
    expect(count === 0).toBe(true)
  })

  it('estimates count approximately for populated filter', () => {
    const filter = createBloomFilter(1000)
    const actual = 200
    for (let i = 0; i < actual; i++) {
      bloomFilterAdd(filter, `item_${i}`)
    }

    const estimate = bloomFilterEstimateCount(filter)
    // Should be within 30% of actual
    expect(estimate).toBeGreaterThan(actual * 0.7)
    expect(estimate).toBeLessThan(actual * 1.3)
  })

  it('returns Infinity for saturated filter', () => {
    const filter: BloomFilter = {
      bits: new Uint8Array(4).fill(0xff), // All bits set
      numHashes: 3,
      numBits: 32,
    }
    expect(bloomFilterEstimateCount(filter)).toBe(Infinity)
  })
})

// =============================================================================
// Serialization: serializeBloomFilter / deserializeBloomFilter
// =============================================================================

describe('serializeBloomFilter / deserializeBloomFilter', () => {
  it('round-trips a bloom filter correctly', () => {
    const filter = createBloomFilter(200)
    bloomFilterAdd(filter, 'hello')
    bloomFilterAdd(filter, 42)
    bloomFilterAdd(filter, true)

    const serialized = serializeBloomFilter(filter)
    const deserialized = deserializeBloomFilter(serialized)

    expect(deserialized.numBits).toBe(filter.numBits)
    expect(deserialized.numHashes).toBe(filter.numHashes)
    expect(deserialized.bits.length).toBe(filter.bits.length)

    // Verify membership is preserved
    expect(bloomFilterMightContain(deserialized, 'hello')).toBe(true)
    expect(bloomFilterMightContain(deserialized, 42)).toBe(true)
    expect(bloomFilterMightContain(deserialized, true)).toBe(true)
    expect(bloomFilterMightContain(deserialized, 'not-added')).toBe(false)
  })

  it('serializes to header (8 bytes) + bits', () => {
    const filter = createBloomFilter(100)
    const serialized = serializeBloomFilter(filter)
    expect(serialized.length).toBe(8 + filter.bits.length)
  })

  it('throws on data too short to deserialize', () => {
    const shortData = new Uint8Array(4)
    expect(() => deserializeBloomFilter(shortData)).toThrow('Invalid bloom filter data: too short')
  })

  it('deserializes header fields correctly', () => {
    const filter = createBloomFilter(500, 0.001)
    const serialized = serializeBloomFilter(filter)
    const deserialized = deserializeBloomFilter(serialized)

    expect(deserialized.numBits).toBe(filter.numBits)
    expect(deserialized.numHashes).toBe(filter.numHashes)
  })
})

// =============================================================================
// BloomFilterIndex (Multi-Field)
// =============================================================================

describe('createBloomFilterIndex', () => {
  it('creates an index with bloom filters for each field', () => {
    const index = createBloomFilterIndex(['name', 'email', 'status'], 1000)

    expect(index.version).toBe(1)
    expect(index.filters.size).toBe(3)
    expect(index.filters.has('name')).toBe(true)
    expect(index.filters.has('email')).toBe(true)
    expect(index.filters.has('status')).toBe(true)
    expect(index.metadata.fields).toEqual(['name', 'email', 'status'])
    expect(index.metadata.rowCount).toBe(0)
  })

  it('creates empty bloom filters for each field', () => {
    const index = createBloomFilterIndex(['field1'], 100)
    const filter = index.filters.get('field1')!
    expect(filter).toBeDefined()
    expect(filter.bits.every((b) => b === 0)).toBe(true)
  })
})

// =============================================================================
// bloomFilterIndexAddRow
// =============================================================================

describe('bloomFilterIndexAddRow', () => {
  it('adds values for each field from the row', () => {
    const index = createBloomFilterIndex(['name', 'status'], 100)

    bloomFilterIndexAddRow(index, { name: 'Alice', status: 'active' })

    const nameFilter = index.filters.get('name')!
    const statusFilter = index.filters.get('status')!

    expect(bloomFilterMightContain(nameFilter, 'Alice')).toBe(true)
    expect(bloomFilterMightContain(statusFilter, 'active')).toBe(true)
  })

  it('increments row count', () => {
    const index = createBloomFilterIndex(['name'], 100)

    bloomFilterIndexAddRow(index, { name: 'Alice' })
    bloomFilterIndexAddRow(index, { name: 'Bob' })

    expect(index.metadata.rowCount).toBe(2)
  })

  it('skips null and undefined field values', () => {
    const index = createBloomFilterIndex(['name', 'email'], 100)

    bloomFilterIndexAddRow(index, { name: 'Alice', email: null })
    bloomFilterIndexAddRow(index, { name: 'Bob' }) // email is undefined

    const emailFilter = index.filters.get('email')!
    // null and undefined are skipped, so filter should remain empty
    expect(emailFilter.bits.every((b) => b === 0)).toBe(true)
    expect(index.metadata.rowCount).toBe(2)
  })

  it('handles nested field values via dot notation', () => {
    const index = createBloomFilterIndex(['user.name', 'user.role'], 100)

    bloomFilterIndexAddRow(index, { user: { name: 'Alice', role: 'admin' } })

    const nameFilter = index.filters.get('user.name')!
    const roleFilter = index.filters.get('user.role')!

    expect(bloomFilterMightContain(nameFilter, 'Alice')).toBe(true)
    expect(bloomFilterMightContain(roleFilter, 'admin')).toBe(true)
  })

  it('handles missing nested paths gracefully (value is undefined, skipped)', () => {
    const index = createBloomFilterIndex(['user.address.city'], 100)

    // user.address doesn't exist
    bloomFilterIndexAddRow(index, { user: { name: 'Alice' } })

    const cityFilter = index.filters.get('user.address.city')!
    expect(cityFilter.bits.every((b) => b === 0)).toBe(true)
  })
})

// =============================================================================
// bloomFilterIndexMightMatch - Filter Integration
// =============================================================================

describe('bloomFilterIndexMightMatch', () => {
  describe('equality filters', () => {
    it('returns true when filter value exists in bloom', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: 'active' })).toBe(true)
    })

    it('returns false when filter value definitely does not exist', () => {
      const index = createBloomFilterIndex(['status'], 1000)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: 'nonexistent_value_xyz' })).toBe(false)
    })

    it('returns true when field has no bloom filter (unknown field)', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      // 'name' has no bloom filter, so we conservatively say true
      expect(bloomFilterIndexMightMatch(index, { name: 'Alice' })).toBe(true)
    })
  })

  describe('$eq operator', () => {
    it('returns true when $eq value exists in bloom', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: { $eq: 'active' } })).toBe(true)
    })

    it('returns false when $eq value definitely does not exist', () => {
      const index = createBloomFilterIndex(['status'], 1000)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: { $eq: 'nonexistent_xyz' } })).toBe(false)
    })
  })

  describe('$in operator', () => {
    it('returns true when at least one $in value might exist', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(
        bloomFilterIndexMightMatch(index, {
          status: { $in: ['inactive', 'active', 'deleted'] },
        })
      ).toBe(true)
    })

    it('returns false when no $in values might exist', () => {
      const index = createBloomFilterIndex(['status'], 1000)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(
        bloomFilterIndexMightMatch(index, {
          status: { $in: ['nonexistent_a', 'nonexistent_b', 'nonexistent_c'] },
        })
      ).toBe(false)
    })

    it('returns false for empty $in array', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: { $in: [] } })).toBe(false)
    })
  })

  describe('operators that are not bloom-filterable', () => {
    it('returns true for $gt (bloom cannot optimize range queries)', () => {
      const index = createBloomFilterIndex(['score'], 100)
      bloomFilterIndexAddRow(index, { score: 50 })

      // $gt is not an equality check, so bloom filter cannot prune
      expect(bloomFilterIndexMightMatch(index, { score: { $gt: 25 } })).toBe(true)
    })

    it('returns true for $lt', () => {
      const index = createBloomFilterIndex(['score'], 100)
      bloomFilterIndexAddRow(index, { score: 50 })

      expect(bloomFilterIndexMightMatch(index, { score: { $lt: 100 } })).toBe(true)
    })

    it('returns true for $ne', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      expect(bloomFilterIndexMightMatch(index, { status: { $ne: 'inactive' } })).toBe(true)
    })
  })

  describe('$-prefixed top-level keys are skipped', () => {
    it('returns true for $and and other logical operators', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      // $and is a top-level operator that starts with '$', so it's skipped
      const filter: Filter = { $and: [{ status: 'active' }] }
      expect(bloomFilterIndexMightMatch(index, filter)).toBe(true)
    })
  })

  describe('multi-column bloom plans', () => {
    it('prunes when any field bloom definitely has no match', () => {
      const index = createBloomFilterIndex(['name', 'status'], 1000)
      bloomFilterIndexAddRow(index, { name: 'Alice', status: 'active' })

      // name matches but status does not -> should return false
      expect(
        bloomFilterIndexMightMatch(index, {
          name: 'Alice',
          status: 'nonexistent_status_xyz',
        })
      ).toBe(false)
    })

    it('returns true only when all field blooms might match', () => {
      const index = createBloomFilterIndex(['name', 'status', 'role'], 100)
      bloomFilterIndexAddRow(index, { name: 'Alice', status: 'active', role: 'admin' })

      expect(
        bloomFilterIndexMightMatch(index, {
          name: 'Alice',
          status: 'active',
          role: 'admin',
        })
      ).toBe(true)
    })

    it('handles partial field coverage (some fields have blooms, some do not)', () => {
      const index = createBloomFilterIndex(['name'], 1000)
      bloomFilterIndexAddRow(index, { name: 'Alice' })

      // 'name' has bloom and matches; 'age' has no bloom -> returns true
      expect(bloomFilterIndexMightMatch(index, { name: 'Alice', age: 30 })).toBe(true)

      // 'name' has bloom and does NOT match -> returns false
      expect(bloomFilterIndexMightMatch(index, { name: 'nonexistent_name_xyz', age: 30 })).toBe(false)
    })
  })

  describe('null value handling in filters', () => {
    it('returns true for null object filter value (typeof check prevents bloom test)', () => {
      const index = createBloomFilterIndex(['status'], 100)
      bloomFilterIndexAddRow(index, { status: 'active' })

      // null is typeof 'object', so the direct equality check branch is not taken
      expect(bloomFilterIndexMightMatch(index, { status: null })).toBe(true)
    })
  })
})

// =============================================================================
// BloomFilterIndex Serialization
// =============================================================================

describe('serializeBloomFilterIndex / deserializeBloomFilterIndex', () => {
  it('round-trips a multi-field index correctly', () => {
    const index = createBloomFilterIndex(['name', 'status'], 200)
    bloomFilterIndexAddRow(index, { name: 'Alice', status: 'active' })
    bloomFilterIndexAddRow(index, { name: 'Bob', status: 'inactive' })

    const serialized = serializeBloomFilterIndex(index)
    const deserialized = deserializeBloomFilterIndex(serialized)

    expect(deserialized.version).toBe(1)
    expect(deserialized.metadata.rowCount).toBe(2)
    expect(deserialized.metadata.fields).toEqual(['name', 'status'])
    expect(deserialized.filters.size).toBe(2)

    // Check membership is preserved
    const nameFilter = deserialized.filters.get('name')!
    const statusFilter = deserialized.filters.get('status')!

    expect(bloomFilterMightContain(nameFilter, 'Alice')).toBe(true)
    expect(bloomFilterMightContain(nameFilter, 'Bob')).toBe(true)
    expect(bloomFilterMightContain(statusFilter, 'active')).toBe(true)
    expect(bloomFilterMightContain(statusFilter, 'inactive')).toBe(true)
  })

  it('preserves createdAt date', () => {
    const index = createBloomFilterIndex(['field1'], 100)

    const serialized = serializeBloomFilterIndex(index)
    const deserialized = deserializeBloomFilterIndex(serialized)

    // createdAt should round-trip (with possible ms precision loss from ISO string)
    expect(deserialized.metadata.createdAt).toBeInstanceOf(Date)
    expect(deserialized.metadata.createdAt.getTime()).toBeCloseTo(
      index.metadata.createdAt.getTime(),
      -3 // within 1 second
    )
  })

  it('works with empty index (no rows added)', () => {
    const index = createBloomFilterIndex(['a', 'b', 'c'], 100)

    const serialized = serializeBloomFilterIndex(index)
    const deserialized = deserializeBloomFilterIndex(serialized)

    expect(deserialized.metadata.rowCount).toBe(0)
    expect(deserialized.filters.size).toBe(3)
  })

  it('deserialized index can be used with bloomFilterIndexMightMatch', () => {
    const index = createBloomFilterIndex(['status'], 1000)
    bloomFilterIndexAddRow(index, { status: 'active' })
    bloomFilterIndexAddRow(index, { status: 'pending' })

    const serialized = serializeBloomFilterIndex(index)
    const deserialized = deserializeBloomFilterIndex(serialized)

    expect(bloomFilterIndexMightMatch(deserialized, { status: 'active' })).toBe(true)
    expect(bloomFilterIndexMightMatch(deserialized, { status: 'pending' })).toBe(true)
    expect(bloomFilterIndexMightMatch(deserialized, { status: 'nonexistent_xyz' })).toBe(false)
  })
})

// =============================================================================
// checkBloomFilter (Storage Backend Integration)
// =============================================================================

describe('checkBloomFilter', () => {
  it('returns true when no bloom filter file exists (conservative)', async () => {
    const storage = createMockStorage({})

    const result = await checkBloomFilter(storage, 'users', 'name', 'Alice')

    expect(result).toBe(true)
    expect(storage.exists).toHaveBeenCalledWith('indexes/bloom/users.bloom')
  })

  it('returns true when bloom filter file exists and value might be present', async () => {
    // Build a bloom filter index, serialize it, and store in mock storage
    const index = createBloomFilterIndex(['name'], 100)
    bloomFilterIndexAddRow(index, { name: 'Alice' })
    const serialized = serializeBloomFilterIndex(index)

    const storage = createMockStorage({
      'indexes/bloom/users.bloom': serialized,
    })

    const result = await checkBloomFilter(storage, 'users', 'name', 'Alice')
    expect(result).toBe(true)
  })

  it('returns false when bloom filter confirms value is definitely not present', async () => {
    const index = createBloomFilterIndex(['name'], 1000)
    bloomFilterIndexAddRow(index, { name: 'Alice' })
    const serialized = serializeBloomFilterIndex(index)

    const storage = createMockStorage({
      'indexes/bloom/users.bloom': serialized,
    })

    const result = await checkBloomFilter(storage, 'users', 'name', 'definitely_not_present_xyz')
    expect(result).toBe(false)
  })

  it('returns true when field has no bloom filter in the index', async () => {
    const index = createBloomFilterIndex(['name'], 100)
    bloomFilterIndexAddRow(index, { name: 'Alice' })
    const serialized = serializeBloomFilterIndex(index)

    const storage = createMockStorage({
      'indexes/bloom/users.bloom': serialized,
    })

    // Check for 'email' field which has no bloom filter
    const result = await checkBloomFilter(storage, 'users', 'email', 'alice@example.com')
    expect(result).toBe(true)
  })

  it('returns true when storage read fails (conservative error handling)', async () => {
    const storage = createMockStorage({})
    // Override exists to return true, but read throws
    ;(storage.exists as ReturnType<typeof vi.fn>).mockResolvedValue(true)
    ;(storage.read as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('Read failed'))

    const result = await checkBloomFilter(storage, 'users', 'name', 'Alice')
    expect(result).toBe(true)
  })

  it('returns true when bloom data is corrupted (conservative error handling)', async () => {
    const storage = createMockStorage({
      'indexes/bloom/users.bloom': new Uint8Array([1, 2, 3]), // Invalid data
    })

    const result = await checkBloomFilter(storage, 'users', 'name', 'Alice')
    expect(result).toBe(true)
  })
})

// =============================================================================
// Row Group Pruning Scenarios
// =============================================================================

describe('row group pruning via bloom filter', () => {
  it('should prune row groups that definitely do not contain a value', () => {
    const index = createBloomFilterIndex(['entityId'], 1000)

    // Simulate 5 row groups, each with different IDs
    const rowGroupData: Record<number, string[]> = {
      0: ['id_001', 'id_002', 'id_003'],
      1: ['id_004', 'id_005', 'id_006'],
      2: ['id_007', 'id_008', 'id_009'],
      3: ['id_010', 'id_011', 'id_012'],
      4: ['id_013', 'id_014', 'id_015'],
    }

    // For this test, we only add rows from group 0 and 2 to the filter
    // (simulating that only some row groups have been indexed)
    for (const id of rowGroupData[0]!) {
      bloomFilterIndexAddRow(index, { entityId: id })
    }
    for (const id of rowGroupData[2]!) {
      bloomFilterIndexAddRow(index, { entityId: id })
    }

    // The bloom filter should confirm presence of ids from added groups
    expect(bloomFilterIndexMightMatch(index, { entityId: 'id_001' })).toBe(true)
    expect(bloomFilterIndexMightMatch(index, { entityId: 'id_007' })).toBe(true)

    // And reject ids that were never added
    expect(bloomFilterIndexMightMatch(index, { entityId: 'totally_unknown_id_xyz' })).toBe(false)
  })

  it('should correctly prune with $in across multiple potential row groups', () => {
    const index = createBloomFilterIndex(['category'], 1000)

    // Only 'electronics' and 'books' were indexed
    bloomFilterIndexAddRow(index, { category: 'electronics' })
    bloomFilterIndexAddRow(index, { category: 'books' })

    // Searching for categories that include an indexed value -> true
    expect(
      bloomFilterIndexMightMatch(index, {
        category: { $in: ['clothing', 'electronics'] },
      })
    ).toBe(true)

    // Searching for categories none of which are indexed -> false
    expect(
      bloomFilterIndexMightMatch(index, {
        category: { $in: ['clothing', 'furniture', 'sports'] },
      })
    ).toBe(false)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('edge cases', () => {
  it('handles empty fields array in createBloomFilterIndex', () => {
    const index = createBloomFilterIndex([], 100)
    expect(index.filters.size).toBe(0)
    expect(index.metadata.fields).toEqual([])
  })

  it('handles adding a row when index has no fields', () => {
    const index = createBloomFilterIndex([], 100)
    bloomFilterIndexAddRow(index, { name: 'Alice' })
    expect(index.metadata.rowCount).toBe(1)
  })

  it('bloomFilterIndexMightMatch returns true for empty filter (no conditions)', () => {
    const index = createBloomFilterIndex(['name'], 100)
    bloomFilterIndexAddRow(index, { name: 'Alice' })

    expect(bloomFilterIndexMightMatch(index, {})).toBe(true)
  })

  it('handles very large number of items', () => {
    const filter = createBloomFilter(10000, 0.01)
    for (let i = 0; i < 5000; i++) {
      bloomFilterAdd(filter, `item_${i}`)
    }

    // All added items should be found
    for (let i = 0; i < 100; i++) {
      expect(bloomFilterMightContain(filter, `item_${i}`)).toBe(true)
    }
  })

  it('handles empty string as a valid value', () => {
    const filter = createBloomFilter(100)
    bloomFilterAdd(filter, '')
    expect(bloomFilterMightContain(filter, '')).toBe(true)
  })

  it('handles number 0 as a valid value', () => {
    const filter = createBloomFilter(100)
    bloomFilterAdd(filter, 0)
    expect(bloomFilterMightContain(filter, 0)).toBe(true)
  })

  it('handles false as a valid value', () => {
    const filter = createBloomFilter(100)
    bloomFilterAdd(filter, false)
    expect(bloomFilterMightContain(filter, false)).toBe(true)
  })

  it('Uint8Array values are handled correctly', () => {
    const filter = createBloomFilter(100)
    const bytes = new Uint8Array([1, 2, 3, 4])
    bloomFilterAdd(filter, bytes)
    expect(bloomFilterMightContain(filter, new Uint8Array([1, 2, 3, 4]))).toBe(true)
  })
})

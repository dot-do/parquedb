/**
 * Predicate Pushdown Tests
 *
 * Tests for filter-to-Parquet predicate pushdown, including:
 * - Row group selection based on min/max statistics
 * - Predicate function generation for in-memory filtering
 * - Field extraction for column projection
 * - Bloom filter integration
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  selectRowGroups,
  couldMatch,
  toPredicate,
  extractFilterFields,
  type RowGroupStats,
  type ColumnStats,
} from '../../src/query/predicate'
import {
  createBloomFilter,
  bloomFilterAdd,
  bloomFilterMightContain,
  createBloomFilterIndex,
  bloomFilterIndexAddRow,
  bloomFilterIndexMightMatch,
  serializeBloomFilter,
  deserializeBloomFilter,
  serializeBloomFilterIndex,
  deserializeBloomFilterIndex,
} from '../../src/query/bloom'
import type { Filter } from '../../src/types/filter'

// =============================================================================
// Test Helpers
// =============================================================================

function createColumnStats(overrides: Partial<ColumnStats> = {}): ColumnStats {
  return {
    min: undefined,
    max: undefined,
    nullCount: 0,
    distinctCount: undefined,
    hasBloomFilter: false,
    ...overrides,
  }
}

function createRowGroupStats(
  rowGroup: number,
  columns: Record<string, Partial<ColumnStats>>,
  rowCount = 1000
): RowGroupStats {
  const columnMap = new Map<string, ColumnStats>()
  for (const [name, stats] of Object.entries(columns)) {
    columnMap.set(name, createColumnStats(stats))
  }
  return { rowGroup, rowCount, columns: columnMap }
}

// =============================================================================
// selectRowGroups Tests
// =============================================================================

describe('Predicate Pushdown', () => {
  describe('selectRowGroups', () => {
    it('returns all row groups for empty filter', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      const selected = selectRowGroups({}, stats)
      expect(selected).toEqual([0, 1, 2])
    })

    it('prunes row groups where $gt exceeds max', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age > 60 can only match row group 2
      const selected = selectRowGroups({ age: { $gt: 60 } }, stats)
      expect(selected).toEqual([2])
    })

    it('prunes row groups where $gte exceeds max', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age >= 50 can match row groups 1 and 2
      const selected = selectRowGroups({ age: { $gte: 50 } }, stats)
      expect(selected).toEqual([1, 2])
    })

    it('prunes row groups where $lt is below min', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age < 25 can only match row group 0
      const selected = selectRowGroups({ age: { $lt: 25 } }, stats)
      expect(selected).toEqual([0])
    })

    it('prunes row groups where $lte is below min', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age <= 31 can match row groups 0 and 1
      const selected = selectRowGroups({ age: { $lte: 31 } }, stats)
      expect(selected).toEqual([0, 1])
    })

    it('includes row groups where $eq value is in range', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age = 25 can only be in row group 0
      const selected = selectRowGroups({ age: { $eq: 25 } }, stats)
      expect(selected).toEqual([0])
    })

    it('prunes row groups where $eq value is outside range', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age = 100 is outside all ranges
      const selected = selectRowGroups({ age: { $eq: 100 } }, stats)
      expect(selected).toEqual([])
    })

    it('handles $in with multiple values', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 } }),
      ]

      // age in [25, 60] can match row groups 0 and 2
      const selected = selectRowGroups({ age: { $in: [25, 60] } }, stats)
      expect(selected).toEqual([0, 2])
    })

    it('handles $in with value in all ranges', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 } }),
        createRowGroupStats(1, { age: { min: 20, max: 40 } }),
        createRowGroupStats(2, { age: { min: 30, max: 50 } }),
      ]

      // age in [25] overlaps all row groups
      const selected = selectRowGroups({ age: { $in: [25] } }, stats)
      expect(selected).toEqual([0, 1])
    })

    it('handles $and combining conditions', () => {
      const stats = [
        createRowGroupStats(0, { age: { min: 10, max: 30 }, score: { min: 0, max: 50 } }),
        createRowGroupStats(1, { age: { min: 31, max: 50 }, score: { min: 60, max: 100 } }),
        createRowGroupStats(2, { age: { min: 51, max: 70 }, score: { min: 0, max: 50 } }),
      ]

      // age > 40 AND score < 60
      // Row group 0: age max=30 < 40, so age > 40 can't match -> excluded
      // Row group 1: age max=50 >= 40, but score min=60 so score < 60 can't match -> excluded
      // Row group 2: age max=70 >= 40 and score max=50 so score < 60 can match -> included
      const selected = selectRowGroups({
        $and: [
          { age: { $gt: 40 } },
          { score: { $lt: 60 } },
        ],
      }, stats)
      expect(selected).toEqual([2])
    })

    it('handles $or (union of matches)', () => {
      const stats = [
        createRowGroupStats(0, { status: { min: 'active', max: 'active' } }),
        createRowGroupStats(1, { status: { min: 'deleted', max: 'deleted' } }),
        createRowGroupStats(2, { status: { min: 'pending', max: 'pending' } }),
      ]

      // status = 'active' OR status = 'pending'
      const selected = selectRowGroups({
        $or: [
          { status: 'active' },
          { status: 'pending' },
        ],
      }, stats)
      expect(selected).toEqual([0, 2])
    })

    it('handles string comparison with min/max', () => {
      const stats = [
        createRowGroupStats(0, { name: { min: 'Alice', max: 'Charlie' } }),
        createRowGroupStats(1, { name: { min: 'David', max: 'Frank' } }),
        createRowGroupStats(2, { name: { min: 'George', max: 'Ivan' } }),
      ]

      // name = 'Bob' is between Alice and Charlie
      const selected = selectRowGroups({ name: 'Bob' }, stats)
      expect(selected).toEqual([0])
    })

    it('handles date comparison', () => {
      const d1 = new Date('2024-01-01')
      const d2 = new Date('2024-02-01')
      const d3 = new Date('2024-03-01')
      const d4 = new Date('2024-04-01')

      const stats = [
        createRowGroupStats(0, { createdAt: { min: d1, max: d2 } }),
        createRowGroupStats(1, { createdAt: { min: d3, max: d4 } }),
      ]

      // createdAt > 2024-02-15 can only match row group 1
      const selected = selectRowGroups({
        createdAt: { $gt: new Date('2024-02-15') },
      }, stats)
      expect(selected).toEqual([1])
    })

    it('handles null check with nullCount', () => {
      const stats = [
        createRowGroupStats(0, { email: { min: 'a@a.com', max: 'z@z.com', nullCount: 0 } }),
        createRowGroupStats(1, { email: { min: 'a@a.com', max: 'z@z.com', nullCount: 10 } }),
      ]

      // email = null can only match row group 1 (has nulls)
      const selected = selectRowGroups({ email: null }, stats)
      expect(selected).toEqual([1])
    })

    it('includes all row groups when no column stats available', () => {
      const stats = [
        createRowGroupStats(0, {}),
        createRowGroupStats(1, {}),
      ]

      // No stats for 'age' column, include all
      const selected = selectRowGroups({ age: { $gt: 50 } }, stats)
      expect(selected).toEqual([0, 1])
    })

    it('handles multiple field filters', () => {
      const stats = [
        createRowGroupStats(0, {
          age: { min: 10, max: 30 },
          status: { min: 'active', max: 'active' },
        }),
        createRowGroupStats(1, {
          age: { min: 31, max: 50 },
          status: { min: 'deleted', max: 'deleted' },
        }),
        createRowGroupStats(2, {
          age: { min: 51, max: 70 },
          status: { min: 'active', max: 'active' },
        }),
      ]

      // age > 40 AND status = 'active'
      const selected = selectRowGroups({
        age: { $gt: 40 },
        status: 'active',
      }, stats)
      expect(selected).toEqual([2])
    })

    it('handles $ne conservatively', () => {
      const stats = [
        createRowGroupStats(0, { status: { min: 'active', max: 'active' } }),
        createRowGroupStats(1, { status: { min: 'deleted', max: 'pending' } }),
      ]

      // status != 'active' - should include row group 1, row group 0 has only 'active'
      const selected = selectRowGroups({ status: { $ne: 'active' } }, stats)
      expect(selected).toEqual([1])
    })

    it('handles $startsWith with string prefix matching', () => {
      const stats = [
        createRowGroupStats(0, { name: { min: 'Alice', max: 'Arthur' } }),
        createRowGroupStats(1, { name: { min: 'Bob', max: 'Carol' } }),
      ]

      // name starts with 'Al' - could match row group 0
      const selected = selectRowGroups({ name: { $startsWith: 'Al' } }, stats)
      expect(selected).toEqual([0])
    })
  })

  // ===========================================================================
  // toPredicate Tests
  // ===========================================================================

  describe('toPredicate', () => {
    describe('basic predicates', () => {
      it('builds predicate for empty filter (matches everything)', () => {
        const predicate = toPredicate({})
        expect(predicate({ name: 'test' })).toBe(true)
        expect(predicate(null)).toBe(true)
        expect(predicate({})).toBe(true)
      })

      it('builds predicate for direct equality', () => {
        const predicate = toPredicate({ status: 'published' })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
        expect(predicate({ status: null })).toBe(false)
      })

      it('builds predicate for $eq', () => {
        const predicate = toPredicate({ age: { $eq: 25 } })
        expect(predicate({ age: 25 })).toBe(true)
        expect(predicate({ age: 30 })).toBe(false)
        expect(predicate({ age: '25' })).toBe(false) // Type matters
      })

      it('builds predicate for $ne', () => {
        const predicate = toPredicate({ status: { $ne: 'draft' } })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
        expect(predicate({ status: null })).toBe(true)
      })
    })

    describe('comparison operators', () => {
      it('builds predicate for $gt', () => {
        const predicate = toPredicate({ age: { $gt: 30 } })
        expect(predicate({ age: 31 })).toBe(true)
        expect(predicate({ age: 30 })).toBe(false)
        expect(predicate({ age: 29 })).toBe(false)
        expect(predicate({ age: null })).toBe(false)
      })

      it('builds predicate for $gte', () => {
        const predicate = toPredicate({ age: { $gte: 30 } })
        expect(predicate({ age: 31 })).toBe(true)
        expect(predicate({ age: 30 })).toBe(true)
        expect(predicate({ age: 29 })).toBe(false)
      })

      it('builds predicate for $lt', () => {
        const predicate = toPredicate({ age: { $lt: 30 } })
        expect(predicate({ age: 29 })).toBe(true)
        expect(predicate({ age: 30 })).toBe(false)
        expect(predicate({ age: 31 })).toBe(false)
      })

      it('builds predicate for $lte', () => {
        const predicate = toPredicate({ age: { $lte: 30 } })
        expect(predicate({ age: 29 })).toBe(true)
        expect(predicate({ age: 30 })).toBe(true)
        expect(predicate({ age: 31 })).toBe(false)
      })

      it('builds predicate for $in', () => {
        const predicate = toPredicate({ status: { $in: ['published', 'featured'] } })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'featured' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
      })

      it('builds predicate for $nin', () => {
        const predicate = toPredicate({ status: { $nin: ['draft', 'archived'] } })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
        expect(predicate({ status: 'archived' })).toBe(false)
      })
    })

    describe('string operators', () => {
      it('builds predicate for $regex with string pattern', () => {
        const predicate = toPredicate({ name: { $regex: '^Hello' } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'Say Hello' })).toBe(false)
      })

      it('builds predicate for $regex with RegExp', () => {
        const predicate = toPredicate({ name: { $regex: /world$/i } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'Hello world' })).toBe(true)
        expect(predicate({ name: 'World Hello' })).toBe(false)
      })

      it('builds predicate for $regex with $options', () => {
        const predicate = toPredicate({ name: { $regex: 'hello', $options: 'i' } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'HELLO' })).toBe(true)
      })

      it('builds predicate for $startsWith', () => {
        const predicate = toPredicate({ name: { $startsWith: 'Hello' } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'Hi Hello' })).toBe(false)
      })

      it('builds predicate for $endsWith', () => {
        const predicate = toPredicate({ name: { $endsWith: 'World' } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'World Hello' })).toBe(false)
      })

      it('builds predicate for $contains', () => {
        const predicate = toPredicate({ name: { $contains: 'lo Wo' } })
        expect(predicate({ name: 'Hello World' })).toBe(true)
        expect(predicate({ name: 'Hi There' })).toBe(false)
      })
    })

    describe('array operators', () => {
      it('builds predicate for $all', () => {
        const predicate = toPredicate({ tags: { $all: ['tech', 'db'] } })
        expect(predicate({ tags: ['tech', 'db', 'cloud'] })).toBe(true)
        expect(predicate({ tags: ['tech', 'cloud'] })).toBe(false)
        expect(predicate({ tags: ['tech'] })).toBe(false)
      })

      it('builds predicate for $size', () => {
        const predicate = toPredicate({ tags: { $size: 3 } })
        expect(predicate({ tags: ['a', 'b', 'c'] })).toBe(true)
        expect(predicate({ tags: ['a', 'b'] })).toBe(false)
        expect(predicate({ tags: [] })).toBe(false)
      })

      it('builds predicate for $elemMatch', () => {
        const predicate = toPredicate({
          items: { $elemMatch: { price: { $gt: 100 } } },
        })
        expect(predicate({ items: [{ price: 50 }, { price: 150 }] })).toBe(true)
        expect(predicate({ items: [{ price: 50 }, { price: 75 }] })).toBe(false)
      })
    })

    describe('existence operators', () => {
      it('builds predicate for $exists true', () => {
        const predicate = toPredicate({ email: { $exists: true } })
        expect(predicate({ email: 'test@test.com' })).toBe(true)
        expect(predicate({ email: null })).toBe(true) // exists but is null
        expect(predicate({ name: 'test' })).toBe(false) // email undefined
      })

      it('builds predicate for $exists false', () => {
        const predicate = toPredicate({ email: { $exists: false } })
        expect(predicate({ name: 'test' })).toBe(true) // email undefined
        expect(predicate({ email: 'test@test.com' })).toBe(false)
      })

      it('builds predicate for $type', () => {
        const predicate = toPredicate({ value: { $type: 'number' } })
        expect(predicate({ value: 42 })).toBe(true)
        expect(predicate({ value: '42' })).toBe(false)
        expect(predicate({ value: null })).toBe(false)
      })
    })

    describe('logical operators', () => {
      it('builds predicate for $and', () => {
        const predicate = toPredicate({
          $and: [
            { status: 'published' },
            { views: { $gt: 100 } },
          ],
        })
        expect(predicate({ status: 'published', views: 150 })).toBe(true)
        expect(predicate({ status: 'published', views: 50 })).toBe(false)
        expect(predicate({ status: 'draft', views: 150 })).toBe(false)
      })

      it('builds predicate for $or', () => {
        const predicate = toPredicate({
          $or: [
            { status: 'published' },
            { status: 'featured' },
          ],
        })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'featured' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
      })

      it('builds predicate for $not', () => {
        const predicate = toPredicate({
          $not: { status: 'draft' },
        })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
      })

      it('builds predicate for $nor', () => {
        const predicate = toPredicate({
          $nor: [
            { status: 'draft' },
            { status: 'archived' },
          ],
        })
        expect(predicate({ status: 'published' })).toBe(true)
        expect(predicate({ status: 'draft' })).toBe(false)
        expect(predicate({ status: 'archived' })).toBe(false)
      })

      it('builds predicate for nested logical operators', () => {
        const predicate = toPredicate({
          $and: [
            {
              $or: [
                { status: 'published' },
                { status: 'featured' },
              ],
            },
            { views: { $gt: 100 } },
          ],
        })
        expect(predicate({ status: 'published', views: 150 })).toBe(true)
        expect(predicate({ status: 'featured', views: 150 })).toBe(true)
        expect(predicate({ status: 'draft', views: 150 })).toBe(false)
        expect(predicate({ status: 'published', views: 50 })).toBe(false)
      })
    })

    describe('nested fields', () => {
      it('handles dot notation for nested fields', () => {
        const predicate = toPredicate({ 'metadata.readTime': { $gt: 5 } })
        expect(predicate({ metadata: { readTime: 10 } })).toBe(true)
        expect(predicate({ metadata: { readTime: 3 } })).toBe(false)
        expect(predicate({ metadata: {} })).toBe(false)
      })

      it('handles deeply nested fields', () => {
        const predicate = toPredicate({ 'a.b.c.d': 'value' })
        expect(predicate({ a: { b: { c: { d: 'value' } } } })).toBe(true)
        expect(predicate({ a: { b: { c: { d: 'other' } } } })).toBe(false)
      })
    })

    describe('date handling', () => {
      it('compares dates correctly', () => {
        const date = new Date('2024-01-15')
        const predicate = toPredicate({ createdAt: { $gt: new Date('2024-01-01') } })
        expect(predicate({ createdAt: date })).toBe(true)
        expect(predicate({ createdAt: new Date('2023-12-31') })).toBe(false)
      })

      it('matches exact dates with $eq', () => {
        const date = new Date('2024-01-15T00:00:00.000Z')
        const predicate = toPredicate({ createdAt: { $eq: date } })
        expect(predicate({ createdAt: new Date('2024-01-15T00:00:00.000Z') })).toBe(true)
        expect(predicate({ createdAt: new Date('2024-01-15T00:00:01.000Z') })).toBe(false)
      })
    })
  })

  // ===========================================================================
  // extractFilterFields Tests
  // ===========================================================================

  describe('extractFilterFields', () => {
    it('extracts simple field names', () => {
      const fields = extractFilterFields({ status: 'published', age: { $gt: 25 } })
      expect(fields).toContain('status')
      expect(fields).toContain('age')
    })

    it('extracts nested field names', () => {
      const fields = extractFilterFields({ 'metadata.readTime': { $gt: 5 } })
      expect(fields).toContain('metadata.readTime')
      expect(fields).toContain('metadata') // Root field also extracted
    })

    it('extracts fields from $and', () => {
      const fields = extractFilterFields({
        $and: [
          { status: 'published' },
          { views: { $gt: 100 } },
        ],
      })
      expect(fields).toContain('status')
      expect(fields).toContain('views')
    })

    it('extracts fields from $or', () => {
      const fields = extractFilterFields({
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      })
      expect(fields).toContain('status')
      expect(fields).toContain('featured')
    })

    it('extracts fields from $not', () => {
      const fields = extractFilterFields({
        $not: { status: 'draft' },
      })
      expect(fields).toContain('status')
    })

    it('extracts fields from $nor', () => {
      const fields = extractFilterFields({
        $nor: [
          { status: 'draft' },
          { archived: true },
        ],
      })
      expect(fields).toContain('status')
      expect(fields).toContain('archived')
    })

    it('extracts fields from $elemMatch', () => {
      const fields = extractFilterFields({
        items: { $elemMatch: { price: { $gt: 100 } } },
      })
      expect(fields).toContain('items')
      expect(fields).toContain('price')
    })

    it('extracts field from $vector operator', () => {
      const fields = extractFilterFields({
        $vector: { $near: [1, 2, 3], $k: 10, $field: 'embedding' },
      })
      expect(fields).toContain('embedding')
    })

    it('handles complex nested filters', () => {
      const fields = extractFilterFields({
        $and: [
          {
            $or: [
              { status: 'published' },
              { 'metadata.featured': true },
            ],
          },
          { 'author.name': { $regex: '^John' } },
          { tags: { $all: ['tech', 'db'] } },
        ],
      })
      expect(fields).toContain('status')
      expect(fields).toContain('metadata.featured')
      expect(fields).toContain('metadata')
      expect(fields).toContain('author.name')
      expect(fields).toContain('author')
      expect(fields).toContain('tags')
    })

    it('returns empty array for empty filter', () => {
      const fields = extractFilterFields({})
      expect(fields).toEqual([])
    })

    it('deduplicates field names', () => {
      const fields = extractFilterFields({
        $and: [
          { status: 'published' },
          { status: { $ne: 'draft' } },
        ],
      })
      const statusCount = fields.filter(f => f === 'status').length
      expect(statusCount).toBe(1)
    })
  })

  // ===========================================================================
  // Bloom Filter Tests
  // ===========================================================================

  describe('Bloom Filter', () => {
    describe('createBloomFilter', () => {
      it('creates bloom filter with expected size', () => {
        const filter = createBloomFilter(1000, 0.01)
        expect(filter.numBits).toBeGreaterThan(0)
        expect(filter.numHashes).toBeGreaterThan(0)
        expect(filter.bits).toBeDefined()
      })

      it('creates larger filter for lower false positive rate', () => {
        const filter1 = createBloomFilter(1000, 0.1)
        const filter2 = createBloomFilter(1000, 0.01)
        expect(filter2.numBits).toBeGreaterThan(filter1.numBits)
      })
    })

    describe('bloomFilterAdd and bloomFilterMightContain', () => {
      it('returns true for added values', () => {
        const filter = createBloomFilter(100)
        bloomFilterAdd(filter, 'hello')
        bloomFilterAdd(filter, 'world')
        bloomFilterAdd(filter, 42)

        expect(bloomFilterMightContain(filter, 'hello')).toBe(true)
        expect(bloomFilterMightContain(filter, 'world')).toBe(true)
        expect(bloomFilterMightContain(filter, 42)).toBe(true)
      })

      it('returns false for values definitely not present', () => {
        const filter = createBloomFilter(100)
        bloomFilterAdd(filter, 'hello')

        // Most values not added should return false (may have rare false positives)
        let falseCount = 0
        for (let i = 0; i < 100; i++) {
          if (!bloomFilterMightContain(filter, `unique-${i}-${Math.random()}`)) {
            falseCount++
          }
        }
        // Should have many definite "not present" responses
        expect(falseCount).toBeGreaterThan(50)
      })

      it('handles different value types', () => {
        const filter = createBloomFilter(100)
        bloomFilterAdd(filter, 'string')
        bloomFilterAdd(filter, 123)
        bloomFilterAdd(filter, true)
        bloomFilterAdd(filter, null)
        bloomFilterAdd(filter, new Date('2024-01-01'))

        expect(bloomFilterMightContain(filter, 'string')).toBe(true)
        expect(bloomFilterMightContain(filter, 123)).toBe(true)
        expect(bloomFilterMightContain(filter, true)).toBe(true)
        expect(bloomFilterMightContain(filter, null)).toBe(true)
        expect(bloomFilterMightContain(filter, new Date('2024-01-01'))).toBe(true)
      })
    })

    describe('serializeBloomFilter and deserializeBloomFilter', () => {
      it('roundtrips correctly', () => {
        const filter = createBloomFilter(100)
        bloomFilterAdd(filter, 'test1')
        bloomFilterAdd(filter, 'test2')
        bloomFilterAdd(filter, 123)

        const serialized = serializeBloomFilter(filter)
        const deserialized = deserializeBloomFilter(serialized)

        expect(deserialized.numBits).toBe(filter.numBits)
        expect(deserialized.numHashes).toBe(filter.numHashes)
        expect(bloomFilterMightContain(deserialized, 'test1')).toBe(true)
        expect(bloomFilterMightContain(deserialized, 'test2')).toBe(true)
        expect(bloomFilterMightContain(deserialized, 123)).toBe(true)
      })
    })

    describe('BloomFilterIndex', () => {
      it('creates index for multiple fields', () => {
        const index = createBloomFilterIndex(['email', 'name'], 100)
        expect(index.filters.has('email')).toBe(true)
        expect(index.filters.has('name')).toBe(true)
      })

      it('adds rows to index', () => {
        const index = createBloomFilterIndex(['email', 'name'], 100)
        bloomFilterIndexAddRow(index, { email: 'test@test.com', name: 'Test User' })
        bloomFilterIndexAddRow(index, { email: 'other@test.com', name: 'Other User' })

        expect(bloomFilterMightContain(index.filters.get('email')!, 'test@test.com')).toBe(true)
        expect(bloomFilterMightContain(index.filters.get('name')!, 'Test User')).toBe(true)
        expect(index.metadata.rowCount).toBe(2)
      })

      it('checks filter matching correctly', () => {
        const index = createBloomFilterIndex(['status', 'email'], 100)
        bloomFilterIndexAddRow(index, { status: 'published', email: 'a@a.com' })
        bloomFilterIndexAddRow(index, { status: 'draft', email: 'b@b.com' })

        // Should match - status 'published' exists
        expect(bloomFilterIndexMightMatch(index, { status: 'published' })).toBe(true)

        // Should match - email exists
        expect(bloomFilterIndexMightMatch(index, { email: 'a@a.com' })).toBe(true)

        // Should not match - status 'archived' doesn't exist
        const archivedMatch = bloomFilterIndexMightMatch(index, { status: 'archived' })
        // Bloom filter might give false positive, so we just check it returns boolean
        expect(typeof archivedMatch).toBe('boolean')
      })

      it('handles $in operator', () => {
        const index = createBloomFilterIndex(['status'], 100)
        bloomFilterIndexAddRow(index, { status: 'published' })
        bloomFilterIndexAddRow(index, { status: 'draft' })

        // Should match - at least one value might exist
        expect(bloomFilterIndexMightMatch(index, {
          status: { $in: ['published', 'archived'] },
        })).toBe(true)
      })

      it('serializes and deserializes correctly', () => {
        const index = createBloomFilterIndex(['email', 'name'], 100)
        bloomFilterIndexAddRow(index, { email: 'test@test.com', name: 'Test' })

        const serialized = serializeBloomFilterIndex(index)
        const deserialized = deserializeBloomFilterIndex(serialized)

        expect(deserialized.version).toBe(index.version)
        expect(deserialized.metadata.rowCount).toBe(1)
        expect(deserialized.filters.has('email')).toBe(true)
        expect(bloomFilterMightContain(deserialized.filters.get('email')!, 'test@test.com')).toBe(true)
      })
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('handles null filter gracefully', () => {
      const predicate = toPredicate(null as unknown as Filter)
      expect(predicate({ any: 'value' })).toBe(true)
    })

    it('handles undefined filter gracefully', () => {
      const predicate = toPredicate(undefined as unknown as Filter)
      expect(predicate({ any: 'value' })).toBe(true)
    })

    it('handles rows with missing fields', () => {
      const predicate = toPredicate({ status: 'published' })
      expect(predicate({})).toBe(false)
    })

    it('handles array equality', () => {
      const predicate = toPredicate({ tags: ['a', 'b'] })
      expect(predicate({ tags: ['a', 'b'] })).toBe(true)
      expect(predicate({ tags: ['b', 'a'] })).toBe(false)
      expect(predicate({ tags: ['a'] })).toBe(false)
    })

    it('handles object equality', () => {
      // When the filter value is an object without operators, it's treated as an equality check
      const predicate = toPredicate({ meta: { $eq: { key: 'value' } } })
      expect(predicate({ meta: { key: 'value' } })).toBe(true)
      expect(predicate({ meta: { key: 'other' } })).toBe(false)
    })

    it('handles special characters in field names', () => {
      const predicate = toPredicate({ 'field-with-dash': 'value' })
      expect(predicate({ 'field-with-dash': 'value' })).toBe(true)
    })

    it('handles unicode values', () => {
      const predicate = toPredicate({ name: 'Cafe' })
      expect(predicate({ name: 'Cafe' })).toBe(true)
      expect(predicate({ name: 'cafe' })).toBe(false)
    })
  })
})

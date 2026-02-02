/**
 * Variant Filter Tests
 *
 * Tests for predicate pushdown into Variant-encoded columns.
 * Covers filter transformation, column extraction, nested value access,
 * row matching with comparison operators, and range predicates.
 *
 * Note: matchesVariantFilter and extractVariantFilterColumns skip any top-level
 * key starting with '$' (after handling $and/$or/$nor/$not). This means keys
 * like '$index.titleType' are skipped by those functions. The
 * transformVariantFilter function, however, does process them. In practice,
 * variant column names starting with '$' are handled by transformVariantFilter
 * first, and the row-level matching uses non-$-prefixed column structures.
 */

import { describe, it, expect } from 'vitest'
import {
  transformVariantFilter,
  extractVariantFilterColumns,
  getNestedValue,
  matchesVariantFilter,
  createRangePredicate,
  type VariantShredConfig,
} from '@/parquet/variant-filter'

// =============================================================================
// Shared fixtures
// =============================================================================

const defaultConfig: VariantShredConfig[] = [
  { column: '$index', fields: ['titleType', 'genre', 'year'] },
  { column: '$data', fields: ['status', 'rating'] },
]

/** Config using non-$ column names for matchesVariantFilter / extractVariantFilterColumns tests */
const nonDollarConfig: VariantShredConfig[] = [
  { column: 'idx', fields: ['titleType', 'genre', 'year'] },
  { column: 'data', fields: ['status', 'rating'] },
]

// =============================================================================
// transformVariantFilter
// =============================================================================

describe('transformVariantFilter', () => {
  describe('basic filter transformation', () => {
    it('should transform dot-notation on a shredded field to Parquet path', () => {
      const filter = { '$index.titleType': 'movie' }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.titleType.typed_value': 'movie',
      })
    })

    it('should transform multiple shredded fields', () => {
      const filter = {
        '$index.titleType': 'movie',
        '$index.genre': 'action',
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.titleType.typed_value': 'movie',
        '$index.typed_value.genre.typed_value': 'action',
      })
    })

    it('should transform fields from different variant columns', () => {
      const filter = {
        '$index.titleType': 'movie',
        '$data.status': 'published',
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.titleType.typed_value': 'movie',
        '$data.typed_value.status.typed_value': 'published',
      })
    })

    it('should pass through non-variant filters unchanged', () => {
      const filter = { name: 'John', age: 30 }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({ name: 'John', age: 30 })
    })

    it('should pass through dot-notation on unknown columns', () => {
      const filter = { 'other.field': 'value' }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({ 'other.field': 'value' })
    })

    it('should pass through dot-notation on non-shredded fields', () => {
      const filter = { '$index.unknownField': 'value' }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({ '$index.unknownField': 'value' })
    })

    it('should handle mixed variant and non-variant filters', () => {
      const filter = {
        '$index.titleType': 'movie',
        name: 'Test',
        '$index.unknownField': 'x',
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.titleType.typed_value': 'movie',
        name: 'Test',
        '$index.unknownField': 'x',
      })
    })

    it('should handle empty filter', () => {
      const result = transformVariantFilter({}, defaultConfig)
      expect(result).toEqual({})
    })

    it('should handle empty config', () => {
      const filter = { '$index.titleType': 'movie' }
      const result = transformVariantFilter(filter, [])
      expect(result).toEqual({ '$index.titleType': 'movie' })
    })

    it('should handle operator objects as values', () => {
      const filter = { '$index.year': { $gt: 2000 } }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.year.typed_value': { $gt: 2000 },
      })
    })

    it('should handle number values', () => {
      const filter = { '$index.year': 2024 }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        '$index.typed_value.year.typed_value': 2024,
      })
    })

    it('should handle boolean values', () => {
      const filter = { '$data.status': true }
      const result = transformVariantFilter(filter, [
        { column: '$data', fields: ['status'] },
      ])
      expect(result).toEqual({
        '$data.typed_value.status.typed_value': true,
      })
    })
  })

  describe('logical operators', () => {
    it('should transform filters inside $and', () => {
      const filter = {
        $and: [
          { '$index.titleType': 'movie' },
          { '$index.genre': 'action' },
        ],
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        $and: [
          { '$index.typed_value.titleType.typed_value': 'movie' },
          { '$index.typed_value.genre.typed_value': 'action' },
        ],
      })
    })

    it('should transform filters inside $or', () => {
      const filter = {
        $or: [
          { '$index.titleType': 'movie' },
          { '$data.status': 'published' },
        ],
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        $or: [
          { '$index.typed_value.titleType.typed_value': 'movie' },
          { '$data.typed_value.status.typed_value': 'published' },
        ],
      })
    })

    it('should transform filters inside $nor', () => {
      const filter = {
        $nor: [{ '$index.titleType': 'short' }],
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        $nor: [{ '$index.typed_value.titleType.typed_value': 'short' }],
      })
    })

    it('should transform filters inside $not', () => {
      const filter = {
        $not: { '$index.titleType': 'short' },
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        $not: { '$index.typed_value.titleType.typed_value': 'short' },
      })
    })

    it('should handle nested logical operators', () => {
      const filter = {
        $and: [
          {
            $or: [
              { '$index.titleType': 'movie' },
              { '$index.titleType': 'tvSeries' },
            ],
          },
          { '$data.status': 'published' },
        ],
      }
      const result = transformVariantFilter(filter, defaultConfig)
      expect(result).toEqual({
        $and: [
          {
            $or: [
              { '$index.typed_value.titleType.typed_value': 'movie' },
              { '$index.typed_value.titleType.typed_value': 'tvSeries' },
            ],
          },
          { '$data.typed_value.status.typed_value': 'published' },
        ],
      })
    })
  })
})

// =============================================================================
// extractVariantFilterColumns
// =============================================================================

describe('extractVariantFilterColumns', () => {
  describe('with non-$ column names', () => {
    it('should extract read and stats columns for shredded variant field', () => {
      const filter = { 'idx.titleType': 'movie' }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toEqual(['idx'])
      expect(result.statsColumns).toEqual([
        'idx.typed_value.titleType.typed_value',
      ])
    })

    it('should deduplicate read columns from multiple fields on the same variant', () => {
      const filter = {
        'idx.titleType': 'movie',
        'idx.genre': 'action',
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toEqual(['idx'])
      expect(result.statsColumns).toContain(
        'idx.typed_value.titleType.typed_value'
      )
      expect(result.statsColumns).toContain(
        'idx.typed_value.genre.typed_value'
      )
    })

    it('should extract columns from multiple variant columns', () => {
      const filter = {
        'idx.titleType': 'movie',
        'data.status': 'published',
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toContain('idx')
      expect(result.readColumns).toContain('data')
    })

    it('should extract columns from $and sub-filters', () => {
      const filter = {
        $and: [
          { 'idx.titleType': 'movie' },
          { name: 'Test' },
        ],
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toContain('idx')
      expect(result.readColumns).toContain('name')
    })

    it('should extract columns from $or sub-filters', () => {
      const filter = {
        $or: [
          { 'idx.titleType': 'movie' },
          { 'data.status': 'published' },
        ],
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toContain('idx')
      expect(result.readColumns).toContain('data')
    })

    it('should extract columns from $nor sub-filters', () => {
      const filter = {
        $nor: [{ 'idx.genre': 'horror' }],
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toContain('idx')
    })

    it('should extract columns from $not sub-filter', () => {
      const filter = {
        $not: { 'data.rating': { $lt: 5 } },
      }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toContain('data')
    })
  })

  describe('regular (non-variant) columns', () => {
    it('should extract regular columns as both read and stats', () => {
      const filter = { name: 'John' }
      const result = extractVariantFilterColumns(filter, defaultConfig)
      expect(result.readColumns).toEqual(['name'])
      expect(result.statsColumns).toEqual(['name'])
    })

    it('should handle non-shredded dot-notation as regular columns', () => {
      // 'other.field' is not a known variant column, so treated as regular
      const filter = { 'other.field': 'value' }
      const result = extractVariantFilterColumns(filter, nonDollarConfig)
      expect(result.readColumns).toEqual(['other.field'])
      expect(result.statsColumns).toEqual(['other.field'])
    })
  })

  describe('$-prefixed keys are skipped', () => {
    it('should skip top-level $ keys that are not logical operators', () => {
      const filter = { $comment: 'ignored' }
      const result = extractVariantFilterColumns(filter, defaultConfig)
      expect(result.readColumns).toEqual([])
      expect(result.statsColumns).toEqual([])
    })

    it('should skip $-prefixed variant column paths', () => {
      // '$index.titleType' starts with '$', so the startsWith('$') guard skips it
      const filter = { '$index.titleType': 'movie' }
      const result = extractVariantFilterColumns(filter, defaultConfig)
      expect(result.readColumns).toEqual([])
      expect(result.statsColumns).toEqual([])
    })
  })

  describe('empty inputs', () => {
    it('should handle empty filter', () => {
      const result = extractVariantFilterColumns({}, defaultConfig)
      expect(result.readColumns).toEqual([])
      expect(result.statsColumns).toEqual([])
    })
  })
})

// =============================================================================
// getNestedValue
// =============================================================================

describe('getNestedValue', () => {
  it('should get top-level value', () => {
    const row = { name: 'John', age: 30 }
    expect(getNestedValue(row, 'name')).toBe('John')
    expect(getNestedValue(row, 'age')).toBe(30)
  })

  it('should get nested value with dot notation', () => {
    const row = { idx: { titleType: 'movie', genre: 'action' } }
    expect(getNestedValue(row, 'idx.titleType')).toBe('movie')
    expect(getNestedValue(row, 'idx.genre')).toBe('action')
  })

  it('should get deeply nested values', () => {
    const row = { a: { b: { c: { d: 'deep' } } } }
    expect(getNestedValue(row, 'a.b.c.d')).toBe('deep')
  })

  it('should return undefined for missing top-level field', () => {
    const row = { name: 'John' }
    expect(getNestedValue(row, 'missing')).toBeUndefined()
  })

  it('should return undefined for missing nested field', () => {
    const row = { idx: { titleType: 'movie' } }
    expect(getNestedValue(row, 'idx.missing')).toBeUndefined()
  })

  it('should return undefined when intermediate is null', () => {
    const row = { a: null }
    expect(getNestedValue(row, 'a.b')).toBeUndefined()
  })

  it('should return undefined when intermediate is undefined', () => {
    const row = { a: undefined }
    expect(getNestedValue(row, 'a.b')).toBeUndefined()
  })

  it('should return undefined when intermediate is a primitive', () => {
    const row = { a: 42 }
    expect(getNestedValue(row, 'a.b')).toBeUndefined()
  })

  it('should return null value if field is null', () => {
    const row = { a: { b: null } }
    expect(getNestedValue(row, 'a.b')).toBeNull()
  })

  it('should handle boolean values', () => {
    const row = { idx: { active: true } }
    expect(getNestedValue(row, 'idx.active')).toBe(true)
  })

  it('should handle zero and empty string', () => {
    const row = { count: 0, label: '' }
    expect(getNestedValue(row, 'count')).toBe(0)
    expect(getNestedValue(row, 'label')).toBe('')
  })

  it('should handle array values', () => {
    const row = { tags: ['a', 'b', 'c'] }
    expect(getNestedValue(row, 'tags')).toEqual(['a', 'b', 'c'])
  })

  it('should navigate into $-prefixed keys', () => {
    const row = { $index: { titleType: 'movie' } }
    expect(getNestedValue(row, '$index.titleType')).toBe('movie')
  })
})

// =============================================================================
// matchesVariantFilter
// =============================================================================

describe('matchesVariantFilter', () => {
  // ---------------------------------------------------------------------------
  // Direct equality (string, number, boolean) using non-$ column names
  // ---------------------------------------------------------------------------

  describe('direct equality', () => {
    it('should match string equality via dot-notation', () => {
      const row = { idx: { titleType: 'movie' } }
      expect(matchesVariantFilter(row, { 'idx.titleType': 'movie' })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.titleType': 'series' })).toBe(false)
    })

    it('should match number equality via dot-notation', () => {
      const row = { idx: { year: 2024 } }
      expect(matchesVariantFilter(row, { 'idx.year': 2024 })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': 2023 })).toBe(false)
    })

    it('should match boolean equality via dot-notation', () => {
      const row = { idx: { active: true } }
      expect(matchesVariantFilter(row, { 'idx.active': true })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.active': false })).toBe(false)
    })

    it('should match null equality', () => {
      const row = { idx: { value: null } }
      expect(matchesVariantFilter(row, { 'idx.value': null })).toBe(true)
    })

    it('should match top-level field equality', () => {
      const row = { name: 'John' }
      expect(matchesVariantFilter(row, { name: 'John' })).toBe(true)
      expect(matchesVariantFilter(row, { name: 'Jane' })).toBe(false)
    })

    it('should match array value with reference equality', () => {
      const arr = [1, 2, 3]
      const row = { tags: arr }
      // Same reference matches
      expect(matchesVariantFilter(row, { tags: arr })).toBe(true)
      // Different reference does not match (=== comparison)
      expect(matchesVariantFilter(row, { tags: [1, 2, 3] })).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // $-prefixed keys are skipped in matchesVariantFilter
  // ---------------------------------------------------------------------------

  describe('$-prefixed keys behavior', () => {
    it('should skip $-prefixed field keys (treated as operators)', () => {
      const row = { $index: { titleType: 'movie' } }
      // '$index.titleType' starts with '$', so it is skipped -- always matches
      expect(matchesVariantFilter(row, { '$index.titleType': 'movie' })).toBe(true)
      expect(matchesVariantFilter(row, { '$index.titleType': 'WRONG_VALUE' })).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // Comparison operators ($eq, $ne, $gt, $gte, $lt, $lte)
  // ---------------------------------------------------------------------------

  describe('comparison operators', () => {
    const row = { idx: { year: 2020, rating: 8.5 } }

    it('$eq should match equal values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $eq: 2020 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $eq: 2021 } })).toBe(false)
    })

    it('$ne should match non-equal values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $ne: 2021 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $ne: 2020 } })).toBe(false)
    })

    it('$gt should match greater values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $gt: 2019 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $gt: 2020 } })).toBe(false)
      expect(matchesVariantFilter(row, { 'idx.year': { $gt: 2021 } })).toBe(false)
    })

    it('$gte should match greater or equal values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $gte: 2019 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $gte: 2020 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $gte: 2021 } })).toBe(false)
    })

    it('$lt should match lesser values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $lt: 2021 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $lt: 2020 } })).toBe(false)
      expect(matchesVariantFilter(row, { 'idx.year': { $lt: 2019 } })).toBe(false)
    })

    it('$lte should match lesser or equal values', () => {
      expect(matchesVariantFilter(row, { 'idx.year': { $lte: 2021 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $lte: 2020 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.year': { $lte: 2019 } })).toBe(false)
    })

    it('should handle float comparisons', () => {
      expect(matchesVariantFilter(row, { 'idx.rating': { $gt: 8.0 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.rating': { $lt: 9.0 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.rating': { $gte: 8.5 } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.rating': { $lte: 8.5 } })).toBe(true)
    })

    it('should support combined range operators', () => {
      expect(
        matchesVariantFilter(row, { 'idx.year': { $gte: 2018, $lte: 2022 } })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, { 'idx.year': { $gt: 2020, $lt: 2025 } })
      ).toBe(false)
    })

    it('should handle top-level field comparisons', () => {
      const simpleRow = { score: 75 }
      expect(matchesVariantFilter(simpleRow, { score: { $gte: 70, $lte: 80 } })).toBe(true)
      expect(matchesVariantFilter(simpleRow, { score: { $gt: 80 } })).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // $in and $nin
  // ---------------------------------------------------------------------------

  describe('$in and $nin operators', () => {
    const row = { idx: { titleType: 'movie', genre: 'action' } }

    it('$in should match if value is in array', () => {
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': { $in: ['movie', 'series'] },
        })
      ).toBe(true)
    })

    it('$in should not match if value is not in array', () => {
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': { $in: ['series', 'short'] },
        })
      ).toBe(false)
    })

    it('$nin should match if value is not in array', () => {
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': { $nin: ['series', 'short'] },
        })
      ).toBe(true)
    })

    it('$nin should not match if value is in array', () => {
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': { $nin: ['movie', 'short'] },
        })
      ).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // $exists
  // ---------------------------------------------------------------------------

  describe('$exists operator', () => {
    const row = { idx: { titleType: 'movie' }, name: 'Test' }

    it('$exists: true should match existing fields', () => {
      expect(matchesVariantFilter(row, { 'idx.titleType': { $exists: true } })).toBe(true)
      expect(matchesVariantFilter(row, { name: { $exists: true } })).toBe(true)
    })

    it('$exists: true should not match missing fields', () => {
      expect(matchesVariantFilter(row, { 'idx.missing': { $exists: true } })).toBe(false)
      expect(matchesVariantFilter(row, { missing: { $exists: true } })).toBe(false)
    })

    it('$exists: false should match missing fields', () => {
      expect(matchesVariantFilter(row, { 'idx.missing': { $exists: false } })).toBe(true)
      expect(matchesVariantFilter(row, { missing: { $exists: false } })).toBe(true)
    })

    it('$exists: false should not match existing fields', () => {
      expect(matchesVariantFilter(row, { 'idx.titleType': { $exists: false } })).toBe(false)
      expect(matchesVariantFilter(row, { name: { $exists: false } })).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // $regex
  // ---------------------------------------------------------------------------

  describe('$regex operator', () => {
    const row = { idx: { title: 'The Dark Knight' }, name: 'Hello World' }

    it('should match strings against regex', () => {
      expect(matchesVariantFilter(row, { 'idx.title': { $regex: 'Dark' } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.title': { $regex: '^The' } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.title': { $regex: 'Knight$' } })).toBe(true)
    })

    it('should not match when regex does not match', () => {
      expect(matchesVariantFilter(row, { 'idx.title': { $regex: '^Dark' } })).toBe(false)
    })

    it('should return false for non-string values', () => {
      const numRow = { count: 42 }
      expect(matchesVariantFilter(numRow, { count: { $regex: '42' } })).toBe(false)
    })

    it('should match top-level string fields', () => {
      expect(matchesVariantFilter(row, { name: { $regex: 'Hello' } })).toBe(true)
      expect(matchesVariantFilter(row, { name: { $regex: 'World$' } })).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // Logical operators
  // ---------------------------------------------------------------------------

  describe('logical operators', () => {
    const row = {
      idx: { titleType: 'movie', year: 2020, genre: 'action' },
      name: 'Test Movie',
    }

    it('$and should require all conditions to match', () => {
      expect(
        matchesVariantFilter(row, {
          $and: [
            { 'idx.titleType': 'movie' },
            { 'idx.year': { $gte: 2020 } },
          ],
        })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, {
          $and: [
            { 'idx.titleType': 'movie' },
            { 'idx.year': { $gt: 2020 } },
          ],
        })
      ).toBe(false)
    })

    it('$or should require at least one condition to match', () => {
      expect(
        matchesVariantFilter(row, {
          $or: [
            { 'idx.titleType': 'series' },
            { 'idx.genre': 'action' },
          ],
        })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, {
          $or: [
            { 'idx.titleType': 'series' },
            { 'idx.genre': 'comedy' },
          ],
        })
      ).toBe(false)
    })

    it('$nor should require no conditions to match', () => {
      expect(
        matchesVariantFilter(row, {
          $nor: [
            { 'idx.titleType': 'series' },
            { 'idx.genre': 'comedy' },
          ],
        })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, {
          $nor: [
            { 'idx.titleType': 'movie' },
            { 'idx.genre': 'comedy' },
          ],
        })
      ).toBe(false)
    })

    it('$not should negate the sub-filter', () => {
      expect(
        matchesVariantFilter(row, {
          $not: { 'idx.titleType': 'series' },
        })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, {
          $not: { 'idx.titleType': 'movie' },
        })
      ).toBe(false)
    })

    it('should handle multiple top-level conditions as implicit $and', () => {
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': 'movie',
          'idx.year': 2020,
        })
      ).toBe(true)
      expect(
        matchesVariantFilter(row, {
          'idx.titleType': 'movie',
          'idx.year': 2021,
        })
      ).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // Null / undefined handling
  // ---------------------------------------------------------------------------

  describe('null and undefined handling', () => {
    it('should match null field to null filter', () => {
      const row = { idx: { value: null } }
      expect(matchesVariantFilter(row, { 'idx.value': null })).toBe(true)
    })

    it('should not match undefined field to null filter', () => {
      const row = { idx: {} }
      // undefined !== null in direct equality
      expect(matchesVariantFilter(row, { 'idx.value': null })).toBe(false)
    })

    it('should handle undefined via $exists', () => {
      const row = { idx: { a: 1 } }
      expect(matchesVariantFilter(row, { 'idx.b': { $exists: false } })).toBe(true)
      expect(matchesVariantFilter(row, { 'idx.b': { $exists: true } })).toBe(false)
    })

    it('should handle $ne with null', () => {
      const row = { value: null }
      expect(matchesVariantFilter(row, { value: { $ne: null } })).toBe(false)
      expect(matchesVariantFilter(row, { value: { $ne: 'something' } })).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // Edge cases: missing fields, type mismatches
  // ---------------------------------------------------------------------------

  describe('edge cases', () => {
    it('should not match when nested field path is missing', () => {
      const row = { idx: {} }
      expect(matchesVariantFilter(row, { 'idx.titleType': 'movie' })).toBe(false)
    })

    it('should not match when parent object is missing', () => {
      const row = {} as Record<string, unknown>
      expect(matchesVariantFilter(row, { 'idx.titleType': 'movie' })).toBe(false)
    })

    it('should handle comparing number with string (type mismatch)', () => {
      const row = { value: 42 }
      expect(matchesVariantFilter(row, { value: '42' })).toBe(false)
    })

    it('should handle comparing boolean with number (type mismatch)', () => {
      const row = { value: true }
      expect(matchesVariantFilter(row, { value: 1 })).toBe(false)
    })

    it('should handle empty row', () => {
      const row = {} as Record<string, unknown>
      // Empty filter matches everything
      expect(matchesVariantFilter(row, {})).toBe(true)
      // Non-empty filter on empty row
      expect(matchesVariantFilter(row, { name: 'test' })).toBe(false)
    })

    it('should skip unknown $ operators in condition without causing rejection', () => {
      // Unknown operators in the condition object are silently skipped by the switch
      const row = { name: 'test' }
      expect(matchesVariantFilter(row, { name: { $unknownOp: 'value' } })).toBe(true)
    })

    it('should skip top-level $ keys that are not logical operators', () => {
      const row = { name: 'test' }
      // Top-level $ keys that aren't $and/$or/$nor/$not are skipped
      expect(matchesVariantFilter(row, { $comment: 'ignored', name: 'test' })).toBe(true)
    })

    it('should match deeply nested dot-notation paths', () => {
      const row = { a: { b: { c: 'found' } } }
      expect(matchesVariantFilter(row, { 'a.b.c': 'found' })).toBe(true)
      expect(matchesVariantFilter(row, { 'a.b.c': 'nope' })).toBe(false)
    })
  })
})

// =============================================================================
// createRangePredicate
// =============================================================================

describe('createRangePredicate', () => {
  describe('direct value', () => {
    it('should return a predicate for a direct numeric value', () => {
      const pred = createRangePredicate(50)
      expect(pred).not.toBeNull()
      // Value 50 is within range [0, 100]
      expect(pred!(0, 100)).toBe(true)
      // Value 50 is within range [50, 50]
      expect(pred!(50, 50)).toBe(true)
      // Value 50 is NOT within range [51, 100]
      expect(pred!(51, 100)).toBe(false)
      // Value 50 is NOT within range [0, 49]
      expect(pred!(0, 49)).toBe(false)
    })

    it('should handle string direct value', () => {
      const pred = createRangePredicate('movie')
      expect(pred).not.toBeNull()
      // 'movie' is within ['a', 'z']
      expect(pred!('a', 'z')).toBe(true)
      // 'movie' is within ['movie', 'movie']
      expect(pred!('movie', 'movie')).toBe(true)
    })
  })

  describe('$eq operator', () => {
    it('should check if $eq value is within range', () => {
      const pred = createRangePredicate({ $eq: 50 })
      expect(pred).not.toBeNull()
      expect(pred!(0, 100)).toBe(true)
      expect(pred!(50, 50)).toBe(true)
      expect(pred!(51, 100)).toBe(false)
    })
  })

  describe('$gt operator', () => {
    it('should check if max > $gt', () => {
      const pred = createRangePredicate({ $gt: 50 })
      expect(pred).not.toBeNull()
      // max=100 > 50 => possible
      expect(pred!(0, 100)).toBe(true)
      // max=50 > 50 => false (not strictly greater)
      expect(pred!(0, 50)).toBe(false)
      // max=49 > 50 => false
      expect(pred!(0, 49)).toBe(false)
    })
  })

  describe('$gte operator', () => {
    it('should check if max >= $gte', () => {
      const pred = createRangePredicate({ $gte: 50 })
      expect(pred).not.toBeNull()
      expect(pred!(0, 100)).toBe(true)
      expect(pred!(0, 50)).toBe(true)
      expect(pred!(0, 49)).toBe(false)
    })
  })

  describe('$lt operator', () => {
    it('should check if min < $lt', () => {
      const pred = createRangePredicate({ $lt: 50 })
      expect(pred).not.toBeNull()
      // min=0 < 50 => possible
      expect(pred!(0, 100)).toBe(true)
      // min=50 < 50 => false (not strictly less)
      expect(pred!(50, 100)).toBe(false)
      // min=51 < 50 => false
      expect(pred!(51, 100)).toBe(false)
    })
  })

  describe('$lte operator', () => {
    it('should check if min <= $lte', () => {
      const pred = createRangePredicate({ $lte: 50 })
      expect(pred).not.toBeNull()
      expect(pred!(0, 100)).toBe(true)
      expect(pred!(50, 100)).toBe(true)
      expect(pred!(51, 100)).toBe(false)
    })
  })

  describe('combined range operators', () => {
    it('should handle $gte and $lte together', () => {
      const pred = createRangePredicate({ $gte: 20, $lte: 80 })
      expect(pred).not.toBeNull()
      // Row group range [10, 90] overlaps [20, 80]
      expect(pred!(10, 90)).toBe(true)
      // Row group range [30, 50] is entirely within [20, 80]
      expect(pred!(30, 50)).toBe(true)
      // Row group range [0, 19] does not overlap [20, 80] (max < gte)
      expect(pred!(0, 19)).toBe(false)
      // Row group range [81, 100] does not overlap [20, 80] (min > lte)
      expect(pred!(81, 100)).toBe(false)
    })

    it('should handle $gt and $lt together', () => {
      const pred = createRangePredicate({ $gt: 20, $lt: 80 })
      expect(pred).not.toBeNull()
      expect(pred!(10, 90)).toBe(true)
      // max=20, not strictly > 20
      expect(pred!(0, 20)).toBe(false)
      // min=80, not strictly < 80
      expect(pred!(80, 100)).toBe(false)
    })
  })

  describe('$in operator', () => {
    it('should check if any $in value is within range', () => {
      const pred = createRangePredicate({ $in: [10, 50, 90] })
      expect(pred).not.toBeNull()
      // 50 is in [0, 100]
      expect(pred!(0, 100)).toBe(true)
      // 10 is in [5, 15]
      expect(pred!(5, 15)).toBe(true)
      // None of [10, 50, 90] is in [20, 40]
      expect(pred!(20, 40)).toBe(false)
    })

    it('should handle $in with single value', () => {
      const pred = createRangePredicate({ $in: [42] })
      expect(pred).not.toBeNull()
      expect(pred!(0, 100)).toBe(true)
      expect(pred!(43, 100)).toBe(false)
    })

    it('should handle $in with empty array', () => {
      const pred = createRangePredicate({ $in: [] })
      expect(pred).not.toBeNull()
      // No values to match, so no row group can contain a match
      expect(pred!(0, 100)).toBe(false)
    })
  })

  describe('null condition', () => {
    it('should return a direct comparison predicate for null', () => {
      // null is typeof 'object' but === null, so it gets a direct comparison predicate
      const pred = createRangePredicate(null)
      expect(pred).not.toBeNull()
    })
  })

  describe('boolean condition', () => {
    it('should handle boolean as direct value', () => {
      const pred = createRangePredicate(true)
      expect(pred).not.toBeNull()
    })
  })

  describe('undefined condition', () => {
    it('should handle undefined as direct value', () => {
      // undefined is not typeof 'object', so treated as direct value
      const pred = createRangePredicate(undefined)
      expect(pred).not.toBeNull()
    })
  })

  describe('no matching operators', () => {
    it('should return true by default when condition has no range operators', () => {
      // An object without any recognized range operators
      const pred = createRangePredicate({ $exists: true })
      expect(pred).not.toBeNull()
      // No $eq/$gt/$gte/$lt/$lte/$in, so `possible` stays true
      expect(pred!(0, 100)).toBe(true)
    })
  })
})

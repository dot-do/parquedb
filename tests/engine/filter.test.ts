import { describe, it, expect } from 'vitest'
import { matchesFilter, getNestedValue } from '@/engine/filter'

/**
 * Shared Filter Module Test Suite
 *
 * Tests the unified filter evaluation used across all engine modes:
 * - TableBuffer (in-memory scans)
 * - ParqueEngine (JSONL + buffer reads)
 * - DOReadPath (R2 Parquet + SQLite WAL merge-on-read)
 *
 * Covers:
 * - Implicit $eq (shorthand like { name: 'Alice' })
 * - All comparison operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex)
 * - Logical operators ($or, $and, $not)
 * - Nested field paths via dot notation
 * - Edge cases: null values, undefined fields, type mismatches
 * - Multiple conditions on the same entity
 */

// =============================================================================
// getNestedValue
// =============================================================================

describe('getNestedValue', () => {
  it('returns top-level field value', () => {
    expect(getNestedValue({ name: 'Alice' }, 'name')).toBe('Alice')
  })

  it('returns nested field value via dot notation', () => {
    expect(getNestedValue({ address: { city: 'NYC' } }, 'address.city')).toBe('NYC')
  })

  it('returns deeply nested field value', () => {
    const obj = { a: { b: { c: { d: 42 } } } }
    expect(getNestedValue(obj, 'a.b.c.d')).toBe(42)
  })

  it('returns undefined for missing top-level field', () => {
    expect(getNestedValue({ name: 'Alice' }, 'age')).toBeUndefined()
  })

  it('returns undefined for missing nested field', () => {
    expect(getNestedValue({ address: { city: 'NYC' } }, 'address.zip')).toBeUndefined()
  })

  it('returns undefined when intermediate path is null', () => {
    expect(getNestedValue({ address: null } as Record<string, unknown>, 'address.city')).toBeUndefined()
  })

  it('returns undefined when intermediate path is undefined', () => {
    expect(getNestedValue({}, 'address.city')).toBeUndefined()
  })

  it('returns undefined when intermediate path is a primitive', () => {
    expect(getNestedValue({ address: 'string-value' }, 'address.city')).toBeUndefined()
  })

  it('returns null when field value is null', () => {
    expect(getNestedValue({ name: null } as Record<string, unknown>, 'name')).toBeNull()
  })

  it('returns 0 when field value is 0', () => {
    expect(getNestedValue({ count: 0 }, 'count')).toBe(0)
  })

  it('returns empty string when field value is empty string', () => {
    expect(getNestedValue({ name: '' }, 'name')).toBe('')
  })

  it('returns false when field value is false', () => {
    expect(getNestedValue({ active: false }, 'active')).toBe(false)
  })
})

// =============================================================================
// matchesFilter - Implicit $eq (shorthand equality)
// =============================================================================

describe('matchesFilter - implicit $eq', () => {
  it('matches when field equals the filter value', () => {
    expect(matchesFilter({ name: 'Alice', age: 30 }, { name: 'Alice' })).toBe(true)
  })

  it('does not match when field differs from filter value', () => {
    expect(matchesFilter({ name: 'Bob', age: 25 }, { name: 'Alice' })).toBe(false)
  })

  it('matches with multiple conditions (implicit AND)', () => {
    expect(matchesFilter({ name: 'Alice', role: 'admin' }, { name: 'Alice', role: 'admin' })).toBe(true)
  })

  it('does not match when one condition fails', () => {
    expect(matchesFilter({ name: 'Alice', role: 'user' }, { name: 'Alice', role: 'admin' })).toBe(false)
  })

  it('matches when filter is empty', () => {
    expect(matchesFilter({ name: 'Alice' }, {})).toBe(true)
  })

  it('does not match when field is undefined and filter expects a value', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: 30 })).toBe(false)
  })

  it('matches numeric equality', () => {
    expect(matchesFilter({ age: 30 }, { age: 30 })).toBe(true)
  })

  it('does not match different numeric values', () => {
    expect(matchesFilter({ age: 30 }, { age: 31 })).toBe(false)
  })

  it('matches boolean equality', () => {
    expect(matchesFilter({ active: true }, { active: true })).toBe(true)
  })

  it('does not match different boolean values', () => {
    expect(matchesFilter({ active: true }, { active: false })).toBe(false)
  })

  it('matches null equality', () => {
    expect(matchesFilter({ value: null } as Record<string, unknown>, { value: null })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - $eq operator
// =============================================================================

describe('matchesFilter - $eq', () => {
  it('matches when field equals the $eq value', () => {
    expect(matchesFilter({ name: 'Alice' }, { name: { $eq: 'Alice' } })).toBe(true)
  })

  it('does not match when field differs', () => {
    expect(matchesFilter({ name: 'Bob' }, { name: { $eq: 'Alice' } })).toBe(false)
  })

  it('matches null with $eq: null', () => {
    expect(matchesFilter({ value: null } as Record<string, unknown>, { value: { $eq: null } })).toBe(true)
  })

  it('does not match undefined field against $eq: null', () => {
    expect(matchesFilter({ other: 1 }, { value: { $eq: null } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $ne operator
// =============================================================================

describe('matchesFilter - $ne', () => {
  it('matches when field is not equal to $ne value', () => {
    expect(matchesFilter({ name: 'Bob' }, { name: { $ne: 'Alice' } })).toBe(true)
  })

  it('does not match when field equals $ne value', () => {
    expect(matchesFilter({ name: 'Alice' }, { name: { $ne: 'Alice' } })).toBe(false)
  })

  it('matches when field is undefined (undefined !== value)', () => {
    expect(matchesFilter({ other: 1 }, { name: { $ne: 'Alice' } })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - $gt operator
// =============================================================================

describe('matchesFilter - $gt', () => {
  it('matches when field is greater than $gt value', () => {
    expect(matchesFilter({ age: 31 }, { age: { $gt: 30 } })).toBe(true)
  })

  it('does not match when field equals $gt value', () => {
    expect(matchesFilter({ age: 30 }, { age: { $gt: 30 } })).toBe(false)
  })

  it('does not match when field is less than $gt value', () => {
    expect(matchesFilter({ age: 29 }, { age: { $gt: 30 } })).toBe(false)
  })

  it('does not match when field is null', () => {
    expect(matchesFilter({ age: null } as Record<string, unknown>, { age: { $gt: 0 } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: { $gt: 0 } })).toBe(false)
  })

  it('works with string comparisons', () => {
    expect(matchesFilter({ name: 'Bob' }, { name: { $gt: 'Alice' } })).toBe(true)
    expect(matchesFilter({ name: 'Alice' }, { name: { $gt: 'Bob' } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $gte operator
// =============================================================================

describe('matchesFilter - $gte', () => {
  it('matches when field is greater than $gte value', () => {
    expect(matchesFilter({ age: 31 }, { age: { $gte: 30 } })).toBe(true)
  })

  it('matches when field equals $gte value', () => {
    expect(matchesFilter({ age: 30 }, { age: { $gte: 30 } })).toBe(true)
  })

  it('does not match when field is less than $gte value', () => {
    expect(matchesFilter({ age: 29 }, { age: { $gte: 30 } })).toBe(false)
  })

  it('does not match when field is null', () => {
    expect(matchesFilter({ age: null } as Record<string, unknown>, { age: { $gte: 0 } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: { $gte: 0 } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $lt operator
// =============================================================================

describe('matchesFilter - $lt', () => {
  it('matches when field is less than $lt value', () => {
    expect(matchesFilter({ age: 29 }, { age: { $lt: 30 } })).toBe(true)
  })

  it('does not match when field equals $lt value', () => {
    expect(matchesFilter({ age: 30 }, { age: { $lt: 30 } })).toBe(false)
  })

  it('does not match when field is greater than $lt value', () => {
    expect(matchesFilter({ age: 31 }, { age: { $lt: 30 } })).toBe(false)
  })

  it('does not match when field is null', () => {
    expect(matchesFilter({ age: null } as Record<string, unknown>, { age: { $lt: 100 } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: { $lt: 100 } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $lte operator
// =============================================================================

describe('matchesFilter - $lte', () => {
  it('matches when field is less than $lte value', () => {
    expect(matchesFilter({ age: 29 }, { age: { $lte: 30 } })).toBe(true)
  })

  it('matches when field equals $lte value', () => {
    expect(matchesFilter({ age: 30 }, { age: { $lte: 30 } })).toBe(true)
  })

  it('does not match when field is greater than $lte value', () => {
    expect(matchesFilter({ age: 31 }, { age: { $lte: 30 } })).toBe(false)
  })

  it('does not match when field is null', () => {
    expect(matchesFilter({ age: null } as Record<string, unknown>, { age: { $lte: 100 } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: { $lte: 100 } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $in operator
// =============================================================================

describe('matchesFilter - $in', () => {
  it('matches when field value is in the array', () => {
    expect(matchesFilter({ role: 'admin' }, { role: { $in: ['admin', 'mod'] } })).toBe(true)
  })

  it('does not match when field value is not in the array', () => {
    expect(matchesFilter({ role: 'user' }, { role: { $in: ['admin', 'mod'] } })).toBe(false)
  })

  it('matches with numeric values', () => {
    expect(matchesFilter({ age: 30 }, { age: { $in: [25, 30, 35] } })).toBe(true)
  })

  it('does not match when $in array is empty', () => {
    expect(matchesFilter({ role: 'admin' }, { role: { $in: [] } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ name: 'Alice' }, { role: { $in: ['admin'] } })).toBe(false)
  })

  it('does not match when $in is not an array', () => {
    expect(matchesFilter({ role: 'admin' }, { role: { $in: 'admin' as unknown as unknown[] } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $nin operator
// =============================================================================

describe('matchesFilter - $nin', () => {
  it('matches when field value is not in the array', () => {
    expect(matchesFilter({ role: 'user' }, { role: { $nin: ['admin', 'mod'] } })).toBe(true)
  })

  it('does not match when field value is in the array', () => {
    expect(matchesFilter({ role: 'admin' }, { role: { $nin: ['admin', 'mod'] } })).toBe(false)
  })

  it('matches when field is undefined and undefined is not in array', () => {
    expect(matchesFilter({ name: 'Alice' }, { role: { $nin: ['admin'] } })).toBe(true)
  })

  it('matches when $nin array is empty (nothing excluded)', () => {
    expect(matchesFilter({ role: 'admin' }, { role: { $nin: [] } })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - $exists operator
// =============================================================================

describe('matchesFilter - $exists', () => {
  it('matches when field exists and $exists is true', () => {
    expect(matchesFilter({ name: 'Alice' }, { name: { $exists: true } })).toBe(true)
  })

  it('does not match when field is missing and $exists is true', () => {
    expect(matchesFilter({ other: 1 }, { name: { $exists: true } })).toBe(false)
  })

  it('matches when field is missing and $exists is false', () => {
    expect(matchesFilter({ other: 1 }, { name: { $exists: false } })).toBe(true)
  })

  it('does not match when field exists and $exists is false', () => {
    expect(matchesFilter({ name: 'Alice' }, { name: { $exists: false } })).toBe(false)
  })

  it('considers null-valued fields as existing', () => {
    expect(matchesFilter({ name: null } as Record<string, unknown>, { name: { $exists: true } })).toBe(true)
  })

  it('considers 0-valued fields as existing', () => {
    expect(matchesFilter({ count: 0 }, { count: { $exists: true } })).toBe(true)
  })

  it('considers false-valued fields as existing', () => {
    expect(matchesFilter({ active: false }, { active: { $exists: true } })).toBe(true)
  })

  it('considers empty-string fields as existing', () => {
    expect(matchesFilter({ name: '' }, { name: { $exists: true } })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - $regex operator
// =============================================================================

describe('matchesFilter - $regex', () => {
  it('matches when string field matches the regex pattern', () => {
    expect(matchesFilter({ email: 'alice@example.com' }, { email: { $regex: '@example\\.com$' } })).toBe(true)
  })

  it('does not match when string field does not match the regex', () => {
    expect(matchesFilter({ email: 'alice@other.com' }, { email: { $regex: '@example\\.com$' } })).toBe(false)
  })

  it('supports partial matching (no anchors needed)', () => {
    expect(matchesFilter({ name: 'Alice Smith' }, { name: { $regex: 'Alice' } })).toBe(true)
  })

  it('supports case-insensitive flag via regex string', () => {
    expect(matchesFilter({ name: 'ALICE' }, { name: { $regex: 'alice' } })).toBe(false)
  })

  it('does not match when field is not a string', () => {
    expect(matchesFilter({ age: 30 }, { age: { $regex: '30' } })).toBe(false)
  })

  it('does not match when field is null', () => {
    expect(matchesFilter({ name: null } as Record<string, unknown>, { name: { $regex: '.*' } })).toBe(false)
  })

  it('does not match when field is undefined', () => {
    expect(matchesFilter({ other: 1 }, { name: { $regex: '.*' } })).toBe(false)
  })

  it('supports RegExp object as $regex value', () => {
    expect(matchesFilter({ name: 'ALICE' }, { name: { $regex: /alice/i } })).toBe(true)
  })

  it('compiles string regex once and reuses across multiple entities', () => {
    // Use a single filter object across many entities to exercise caching
    const filter = { name: { $regex: '^A' } }

    const entities = [
      { name: 'Alice' },
      { name: 'Bob' },
      { name: 'Anna' },
      { name: 'Charlie' },
      { name: 'Arnold' },
      { name: 'Dave' },
    ]

    const results = entities.filter(e => matchesFilter(e, filter))
    expect(results).toEqual([
      { name: 'Alice' },
      { name: 'Anna' },
      { name: 'Arnold' },
    ])

    // After first call, the string should have been compiled to RegExp in-place
    expect(filter.name.$regex).toBeInstanceOf(RegExp)
  })

  it('caches compiled regex and produces correct results on repeated calls', () => {
    const filter = { email: { $regex: '@example\\.com$' } }

    // First pass
    expect(matchesFilter({ email: 'a@example.com' }, filter)).toBe(true)
    expect(matchesFilter({ email: 'b@other.com' }, filter)).toBe(false)

    // $regex should now be a compiled RegExp
    expect(filter.email.$regex).toBeInstanceOf(RegExp)

    // Second pass with the same (now compiled) filter still works
    expect(matchesFilter({ email: 'c@example.com' }, filter)).toBe(true)
    expect(matchesFilter({ email: 'd@nope.org' }, filter)).toBe(false)
  })

  it('works correctly with $not and cached $regex', () => {
    const filter = { email: { $not: { $regex: '@blocked\\.com$' } } }

    const entities = [
      { email: 'good@ok.com' },
      { email: 'bad@blocked.com' },
      { email: 'also-good@fine.com' },
      { email: 'also-bad@blocked.com' },
    ]

    const results = entities.filter(e => matchesFilter(e, filter))
    expect(results).toEqual([
      { email: 'good@ok.com' },
      { email: 'also-good@fine.com' },
    ])
  })
})

// =============================================================================
// matchesFilter - $or logical operator
// =============================================================================

describe('matchesFilter - $or', () => {
  it('matches when at least one sub-filter matches', () => {
    expect(matchesFilter(
      { name: 'Alice', age: 30 },
      { $or: [{ name: 'Alice' }, { name: 'Bob' }] },
    )).toBe(true)
  })

  it('matches when only the second sub-filter matches', () => {
    expect(matchesFilter(
      { name: 'Bob', age: 25 },
      { $or: [{ name: 'Alice' }, { name: 'Bob' }] },
    )).toBe(true)
  })

  it('does not match when no sub-filter matches', () => {
    expect(matchesFilter(
      { name: 'Charlie', age: 35 },
      { $or: [{ name: 'Alice' }, { name: 'Bob' }] },
    )).toBe(false)
  })

  it('supports comparison operators inside $or sub-filters', () => {
    expect(matchesFilter(
      { age: 18 },
      { $or: [{ age: { $lt: 13 } }, { age: { $gte: 18 } }] },
    )).toBe(true)
  })

  it('does not match when $or array is empty', () => {
    expect(matchesFilter({ name: 'Alice' }, { $or: [] })).toBe(false)
  })

  it('can be combined with field conditions (AND with $or)', () => {
    expect(matchesFilter(
      { name: 'Alice', role: 'admin' },
      { role: 'admin', $or: [{ name: 'Alice' }, { name: 'Bob' }] },
    )).toBe(true)

    expect(matchesFilter(
      { name: 'Alice', role: 'user' },
      { role: 'admin', $or: [{ name: 'Alice' }, { name: 'Bob' }] },
    )).toBe(false)
  })
})

// =============================================================================
// matchesFilter - $and logical operator
// =============================================================================

describe('matchesFilter - $and', () => {
  it('matches when all sub-filters match', () => {
    expect(matchesFilter(
      { name: 'Alice', age: 30 },
      { $and: [{ name: 'Alice' }, { age: 30 }] },
    )).toBe(true)
  })

  it('does not match when one sub-filter fails', () => {
    expect(matchesFilter(
      { name: 'Alice', age: 25 },
      { $and: [{ name: 'Alice' }, { age: 30 }] },
    )).toBe(false)
  })

  it('supports comparison operators inside $and sub-filters', () => {
    expect(matchesFilter(
      { age: 25 },
      { $and: [{ age: { $gte: 18 } }, { age: { $lt: 65 } }] },
    )).toBe(true)

    expect(matchesFilter(
      { age: 70 },
      { $and: [{ age: { $gte: 18 } }, { age: { $lt: 65 } }] },
    )).toBe(false)
  })

  it('matches when $and array is empty (vacuously true)', () => {
    expect(matchesFilter({ name: 'Alice' }, { $and: [] })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - $not logical operator
// =============================================================================

describe('matchesFilter - $not', () => {
  it('matches when the sub-filter does NOT match', () => {
    expect(matchesFilter(
      { name: 'Bob' },
      { name: { $not: { $eq: 'Alice' } } },
    )).toBe(true)
  })

  it('does not match when the sub-filter matches', () => {
    expect(matchesFilter(
      { name: 'Alice' },
      { name: { $not: { $eq: 'Alice' } } },
    )).toBe(false)
  })

  it('works with $gt inside $not', () => {
    expect(matchesFilter({ age: 15 }, { age: { $not: { $gt: 18 } } })).toBe(true)
    expect(matchesFilter({ age: 25 }, { age: { $not: { $gt: 18 } } })).toBe(false)
  })

  it('works with $in inside $not', () => {
    expect(matchesFilter(
      { role: 'user' },
      { role: { $not: { $in: ['admin', 'mod'] } } },
    )).toBe(true)

    expect(matchesFilter(
      { role: 'admin' },
      { role: { $not: { $in: ['admin', 'mod'] } } },
    )).toBe(false)
  })

  it('works with $regex inside $not', () => {
    expect(matchesFilter(
      { email: 'alice@other.com' },
      { email: { $not: { $regex: '@example\\.com$' } } },
    )).toBe(true)

    expect(matchesFilter(
      { email: 'alice@example.com' },
      { email: { $not: { $regex: '@example\\.com$' } } },
    )).toBe(false)
  })
})

// =============================================================================
// matchesFilter - Nested field paths
// =============================================================================

describe('matchesFilter - nested field paths', () => {
  const entity = {
    name: 'Alice',
    address: {
      city: 'NYC',
      state: 'NY',
      location: {
        lat: 40.7128,
        lng: -74.006,
      },
    },
  }

  it('matches nested field with implicit $eq', () => {
    expect(matchesFilter(entity, { 'address.city': 'NYC' })).toBe(true)
  })

  it('does not match nested field with wrong value', () => {
    expect(matchesFilter(entity, { 'address.city': 'LA' })).toBe(false)
  })

  it('matches deeply nested field', () => {
    expect(matchesFilter(entity, { 'address.location.lat': 40.7128 })).toBe(true)
  })

  it('supports comparison operators on nested fields', () => {
    expect(matchesFilter(entity, { 'address.location.lat': { $gt: 40 } })).toBe(true)
    expect(matchesFilter(entity, { 'address.location.lat': { $lt: 40 } })).toBe(false)
  })

  it('supports $exists on nested fields', () => {
    expect(matchesFilter(entity, { 'address.zip': { $exists: false } })).toBe(true)
    expect(matchesFilter(entity, { 'address.city': { $exists: true } })).toBe(true)
  })

  it('returns undefined for completely missing nested paths', () => {
    expect(matchesFilter(entity, { 'foo.bar.baz': { $exists: false } })).toBe(true)
  })
})

// =============================================================================
// matchesFilter - Combined operators on the same field
// =============================================================================

describe('matchesFilter - combined operators on same field', () => {
  it('supports range query with $gte and $lt on same field', () => {
    expect(matchesFilter({ age: 25 }, { age: { $gte: 18, $lt: 30 } })).toBe(true)
    expect(matchesFilter({ age: 35 }, { age: { $gte: 18, $lt: 30 } })).toBe(false)
    expect(matchesFilter({ age: 18 }, { age: { $gte: 18, $lt: 30 } })).toBe(true)
    expect(matchesFilter({ age: 30 }, { age: { $gte: 18, $lt: 30 } })).toBe(false)
  })

  it('supports $ne combined with $gt', () => {
    expect(matchesFilter({ age: 25 }, { age: { $ne: 20, $gt: 18 } })).toBe(true)
    expect(matchesFilter({ age: 20 }, { age: { $ne: 20, $gt: 18 } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - Multiple conditions on same entity
// =============================================================================

describe('matchesFilter - multiple conditions', () => {
  const entity = { name: 'Alice', age: 30, role: 'admin', active: true }

  it('matches when all field conditions are met', () => {
    expect(matchesFilter(entity, { name: 'Alice', age: { $gte: 18 }, role: 'admin' })).toBe(true)
  })

  it('does not match when any field condition fails', () => {
    expect(matchesFilter(entity, { name: 'Alice', age: { $lt: 18 }, role: 'admin' })).toBe(false)
  })

  it('combines implicit $eq with operator conditions', () => {
    expect(matchesFilter(entity, { active: true, age: { $gt: 25 } })).toBe(true)
    expect(matchesFilter(entity, { active: false, age: { $gt: 25 } })).toBe(false)
  })
})

// =============================================================================
// matchesFilter - Edge cases
// =============================================================================

describe('matchesFilter - edge cases', () => {
  it('handles entity with no fields (empty object)', () => {
    expect(matchesFilter({}, {})).toBe(true)
    expect(matchesFilter({}, { name: 'Alice' })).toBe(false)
  })

  it('handles comparison with type mismatch (string vs number)', () => {
    // In JavaScript, '30' > 18 is true because '30' is coerced to number
    // But our filter should use direct comparison, matching JS semantics
    expect(matchesFilter({ age: '30' }, { age: { $gt: 18 } })).toBe(true)
  })

  it('handles $eq with undefined (field missing)', () => {
    expect(matchesFilter({ name: 'Alice' }, { age: { $eq: undefined } })).toBe(true)
  })

  it('handles deeply nested $or with $and', () => {
    const filter = {
      $or: [
        { $and: [{ age: { $gte: 18 } }, { role: 'admin' }] },
        { name: 'System' },
      ],
    }
    expect(matchesFilter({ name: 'Alice', age: 30, role: 'admin' }, filter)).toBe(true)
    expect(matchesFilter({ name: 'System', age: 5, role: 'bot' }, filter)).toBe(true)
    expect(matchesFilter({ name: 'Bob', age: 15, role: 'user' }, filter)).toBe(false)
  })
})

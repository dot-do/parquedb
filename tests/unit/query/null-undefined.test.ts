/**
 * Null vs Undefined Handling Tests
 *
 * This test file documents and verifies the intended behavior for null and undefined
 * values throughout ParqueDB's filter and comparison system.
 *
 * ## Design Decisions
 *
 * 1. **Equality Operations**: null and undefined are treated as equivalent
 *    - This follows MongoDB's behavior where missing fields match null
 *    - Simplifies queries: `{ field: null }` matches both null and missing
 *
 * 2. **Comparison Operations**: null/undefined return false for all comparisons
 *    - You cannot meaningfully compare null with numbers/strings/dates
 *    - `{ field: { $gt: 0 } }` is false when field is null or missing
 *
 * 3. **Sorting**: null and undefined are treated as equivalent, sort first
 *    - Consistent ordering regardless of null vs missing
 *    - Predictable behavior in sorted queries
 *
 * 4. **$exists Operator**: Distinguishes between null and undefined
 *    - `$exists: true` matches if field is present, even if null
 *    - `$exists: false` matches only if field is missing (undefined)
 *    - This is the ONLY operator that distinguishes null from undefined
 */

import { describe, it, expect } from 'vitest'
import { matchesFilter, matchesCondition, deepEqual, compareValues, getValueType } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// deepEqual - Equality Comparison
// =============================================================================

describe('deepEqual: null vs undefined handling', () => {
  describe('null equals null', () => {
    it('null === null is true', () => {
      expect(deepEqual(null, null)).toBe(true)
    })
  })

  describe('undefined equals undefined', () => {
    it('undefined === undefined is true', () => {
      expect(deepEqual(undefined, undefined)).toBe(true)
    })
  })

  describe('null and undefined are equivalent for equality', () => {
    it('null === undefined is true (MongoDB behavior)', () => {
      expect(deepEqual(null, undefined)).toBe(true)
    })

    it('undefined === null is true (symmetric)', () => {
      expect(deepEqual(undefined, null)).toBe(true)
    })
  })

  describe('null/undefined are not equal to falsy values', () => {
    it('null !== 0', () => {
      expect(deepEqual(null, 0)).toBe(false)
    })

    it('null !== empty string', () => {
      expect(deepEqual(null, '')).toBe(false)
    })

    it('null !== false', () => {
      expect(deepEqual(null, false)).toBe(false)
    })

    it('null !== empty array', () => {
      expect(deepEqual(null, [])).toBe(false)
    })

    it('null !== empty object', () => {
      expect(deepEqual(null, {})).toBe(false)
    })

    it('undefined !== 0', () => {
      expect(deepEqual(undefined, 0)).toBe(false)
    })

    it('undefined !== empty string', () => {
      expect(deepEqual(undefined, '')).toBe(false)
    })

    it('undefined !== false', () => {
      expect(deepEqual(undefined, false)).toBe(false)
    })
  })

  describe('null/undefined in nested objects', () => {
    it('objects with null fields are equal', () => {
      expect(deepEqual({ a: null }, { a: null })).toBe(true)
    })

    it('objects with undefined fields are equal', () => {
      expect(deepEqual({ a: undefined }, { a: undefined })).toBe(true)
    })

    it('objects with null and undefined fields are equal', () => {
      expect(deepEqual({ a: null }, { a: undefined })).toBe(true)
    })
  })

  describe('null/undefined in arrays', () => {
    it('arrays with null elements are equal', () => {
      expect(deepEqual([null, 1, null], [null, 1, null])).toBe(true)
    })

    it('arrays with undefined elements are equal', () => {
      expect(deepEqual([undefined, 1, undefined], [undefined, 1, undefined])).toBe(true)
    })

    it('arrays with null and undefined elements are equal', () => {
      expect(deepEqual([null, 1], [undefined, 1])).toBe(true)
    })
  })
})

// =============================================================================
// compareValues - Sorting/Ordering
// =============================================================================

describe('compareValues: null vs undefined handling for sorting', () => {
  describe('null and undefined are equivalent for sorting', () => {
    it('null compared to null returns 0', () => {
      expect(compareValues(null, null)).toBe(0)
    })

    it('undefined compared to undefined returns 0', () => {
      expect(compareValues(undefined, undefined)).toBe(0)
    })

    it('null compared to undefined returns 0', () => {
      expect(compareValues(null, undefined)).toBe(0)
    })

    it('undefined compared to null returns 0', () => {
      expect(compareValues(undefined, null)).toBe(0)
    })
  })

  describe('null/undefined sort before all other values', () => {
    it('null sorts before numbers', () => {
      expect(compareValues(null, 0)).toBeLessThan(0)
      expect(compareValues(null, -1)).toBeLessThan(0)
      expect(compareValues(null, 1)).toBeLessThan(0)
    })

    it('null sorts before strings', () => {
      expect(compareValues(null, '')).toBeLessThan(0)
      expect(compareValues(null, 'a')).toBeLessThan(0)
    })

    it('null sorts before booleans', () => {
      expect(compareValues(null, false)).toBeLessThan(0)
      expect(compareValues(null, true)).toBeLessThan(0)
    })

    it('null sorts before dates', () => {
      expect(compareValues(null, new Date())).toBeLessThan(0)
    })

    it('undefined sorts before numbers', () => {
      expect(compareValues(undefined, 0)).toBeLessThan(0)
      expect(compareValues(undefined, -1)).toBeLessThan(0)
    })

    it('undefined sorts before strings', () => {
      expect(compareValues(undefined, '')).toBeLessThan(0)
    })
  })

  describe('values sort after null/undefined', () => {
    it('numbers sort after null', () => {
      expect(compareValues(0, null)).toBeGreaterThan(0)
      expect(compareValues(-1, null)).toBeGreaterThan(0)
      expect(compareValues(1, null)).toBeGreaterThan(0)
    })

    it('strings sort after null', () => {
      expect(compareValues('', null)).toBeGreaterThan(0)
      expect(compareValues('a', null)).toBeGreaterThan(0)
    })

    it('booleans sort after null', () => {
      expect(compareValues(false, null)).toBeGreaterThan(0)
      expect(compareValues(true, null)).toBeGreaterThan(0)
    })

    it('numbers sort after undefined', () => {
      expect(compareValues(0, undefined)).toBeGreaterThan(0)
    })
  })

  describe('sorting arrays with null/undefined', () => {
    it('comparing null vs non-null produces consistent order', () => {
      // When comparing null to a number, null always comes first
      expect(compareValues(null, 5)).toBeLessThan(0)
      expect(compareValues(undefined, 5)).toBeLessThan(0)
      expect(compareValues(5, null)).toBeGreaterThan(0)
      expect(compareValues(5, undefined)).toBeGreaterThan(0)
    })

    it('null and undefined compare as equal', () => {
      // Since they compare as equal, Array.sort may not maintain their relative order
      // This is expected JS behavior - sort is not guaranteed stable for equal elements
      expect(compareValues(null, undefined)).toBe(0)
      expect(compareValues(undefined, null)).toBe(0)
    })

    it('numbers sort correctly among themselves', () => {
      const numbers = [3, 1, 2]
      const sorted = [...numbers].sort(compareValues)
      expect(sorted).toEqual([1, 2, 3])
    })
  })
})

// =============================================================================
// getValueType - Type Detection
// =============================================================================

describe('getValueType: null vs undefined handling', () => {
  it('null has type "null"', () => {
    expect(getValueType(null)).toBe('null')
  })

  it('undefined has type "null"', () => {
    expect(getValueType(undefined)).toBe('null')
  })

  it('both null and undefined return the same type', () => {
    expect(getValueType(null)).toBe(getValueType(undefined))
  })
})

// =============================================================================
// Filter Operators - Equality ($eq, $ne)
// =============================================================================

describe('$eq operator: null vs undefined handling', () => {
  describe('matching null', () => {
    it('{ field: null } matches field with null value', () => {
      expect(matchesFilter({ value: null }, { value: null })).toBe(true)
    })

    it('{ field: null } matches missing field (undefined)', () => {
      expect(matchesFilter({}, { missing: null })).toBe(true)
    })

    it('{ field: null } matches field explicitly set to undefined', () => {
      expect(matchesFilter({ value: undefined }, { value: null })).toBe(true)
    })

    it('{ field: { $eq: null } } matches null value', () => {
      expect(matchesFilter({ value: null }, { value: { $eq: null } })).toBe(true)
    })

    it('{ field: { $eq: null } } matches missing field', () => {
      expect(matchesFilter({}, { missing: { $eq: null } })).toBe(true)
    })

    it('{ field: { $eq: null } } matches undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $eq: null } })).toBe(true)
    })
  })

  describe('matching undefined', () => {
    it('{ field: undefined } always matches (no condition)', () => {
      // When condition is undefined, matchesCondition returns true
      expect(matchesFilter({ value: 'anything' }, { value: undefined })).toBe(true)
      expect(matchesFilter({ value: null }, { value: undefined })).toBe(true)
      expect(matchesFilter({}, { missing: undefined })).toBe(true)
    })

    it('{ field: { $eq: undefined } } matches undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $eq: undefined } })).toBe(true)
    })

    it('{ field: { $eq: undefined } } matches null value (equivalent)', () => {
      expect(matchesFilter({ value: null }, { value: { $eq: undefined } })).toBe(true)
    })

    it('{ field: { $eq: undefined } } matches missing field', () => {
      expect(matchesFilter({}, { missing: { $eq: undefined } })).toBe(true)
    })
  })

  describe('not matching non-null values', () => {
    it('{ field: null } does not match string', () => {
      expect(matchesFilter({ value: 'test' }, { value: null })).toBe(false)
    })

    it('{ field: null } does not match number 0', () => {
      expect(matchesFilter({ value: 0 }, { value: null })).toBe(false)
    })

    it('{ field: null } does not match empty string', () => {
      expect(matchesFilter({ value: '' }, { value: null })).toBe(false)
    })

    it('{ field: null } does not match false', () => {
      expect(matchesFilter({ value: false }, { value: null })).toBe(false)
    })

    it('{ field: null } does not match empty array', () => {
      expect(matchesFilter({ value: [] }, { value: null })).toBe(false)
    })

    it('{ field: null } does not match empty object', () => {
      expect(matchesFilter({ value: {} }, { value: null })).toBe(false)
    })
  })
})

describe('$ne operator: null vs undefined handling', () => {
  describe('not matching null', () => {
    it('{ field: { $ne: null } } matches non-null values', () => {
      expect(matchesFilter({ value: 'test' }, { value: { $ne: null } })).toBe(true)
      expect(matchesFilter({ value: 0 }, { value: { $ne: null } })).toBe(true)
      expect(matchesFilter({ value: false }, { value: { $ne: null } })).toBe(true)
      expect(matchesFilter({ value: '' }, { value: { $ne: null } })).toBe(true)
    })

    it('{ field: { $ne: null } } does not match null value', () => {
      expect(matchesFilter({ value: null }, { value: { $ne: null } })).toBe(false)
    })

    it('{ field: { $ne: null } } does not match undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $ne: null } })).toBe(false)
    })

    it('{ field: { $ne: null } } does not match missing field', () => {
      expect(matchesFilter({}, { missing: { $ne: null } })).toBe(false)
    })
  })
})

// =============================================================================
// Filter Operators - Set Membership ($in, $nin)
// =============================================================================

describe('$in operator: null vs undefined handling', () => {
  it('{ field: { $in: [null] } } matches null value', () => {
    expect(matchesFilter({ value: null }, { value: { $in: [null] } })).toBe(true)
  })

  it('{ field: { $in: [null] } } matches undefined value', () => {
    expect(matchesFilter({ value: undefined }, { value: { $in: [null] } })).toBe(true)
  })

  it('{ field: { $in: [null] } } matches missing field', () => {
    expect(matchesFilter({}, { missing: { $in: [null] } })).toBe(true)
  })

  it('{ field: { $in: [null, "other"] } } matches null among other values', () => {
    expect(matchesFilter({ value: null }, { value: { $in: [null, 'other'] } })).toBe(true)
    expect(matchesFilter({ value: 'other' }, { value: { $in: [null, 'other'] } })).toBe(true)
    expect(matchesFilter({ value: 'test' }, { value: { $in: [null, 'other'] } })).toBe(false)
  })

  it('{ field: { $in: [] } } does not match anything (empty array)', () => {
    expect(matchesFilter({ value: null }, { value: { $in: [] } })).toBe(false)
    expect(matchesFilter({}, { missing: { $in: [] } })).toBe(false)
  })
})

describe('$nin operator: null vs undefined handling', () => {
  it('{ field: { $nin: [null] } } does not match null value', () => {
    expect(matchesFilter({ value: null }, { value: { $nin: [null] } })).toBe(false)
  })

  it('{ field: { $nin: [null] } } does not match undefined value', () => {
    expect(matchesFilter({ value: undefined }, { value: { $nin: [null] } })).toBe(false)
  })

  it('{ field: { $nin: [null] } } does not match missing field', () => {
    expect(matchesFilter({}, { missing: { $nin: [null] } })).toBe(false)
  })

  it('{ field: { $nin: ["other"] } } matches null (null not in exclusion list)', () => {
    expect(matchesFilter({ value: null }, { value: { $nin: ['other'] } })).toBe(true)
  })

  it('{ field: { $nin: ["other"] } } matches missing field', () => {
    expect(matchesFilter({}, { missing: { $nin: ['other'] } })).toBe(true)
  })

  it('{ field: { $nin: [] } } matches everything (empty exclusion)', () => {
    expect(matchesFilter({ value: null }, { value: { $nin: [] } })).toBe(true)
    expect(matchesFilter({ value: 'test' }, { value: { $nin: [] } })).toBe(true)
    expect(matchesFilter({}, { missing: { $nin: [] } })).toBe(true)
  })
})

// =============================================================================
// Filter Operators - Comparison ($gt, $gte, $lt, $lte)
// =============================================================================

describe('comparison operators: null vs undefined handling', () => {
  describe('$gt returns false for null/undefined', () => {
    it('{ field: { $gt: 0 } } returns false for null value', () => {
      expect(matchesFilter({ value: null }, { value: { $gt: 0 } })).toBe(false)
    })

    it('{ field: { $gt: 0 } } returns false for undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $gt: 0 } })).toBe(false)
    })

    it('{ field: { $gt: 0 } } returns false for missing field', () => {
      expect(matchesFilter({}, { missing: { $gt: 0 } })).toBe(false)
    })

    it('{ field: { $gt: -Infinity } } returns false for null', () => {
      // Even -Infinity doesn't compare to null
      expect(matchesFilter({ value: null }, { value: { $gt: -Infinity } })).toBe(false)
    })
  })

  describe('$gte returns false for null/undefined', () => {
    it('{ field: { $gte: 0 } } returns false for null value', () => {
      expect(matchesFilter({ value: null }, { value: { $gte: 0 } })).toBe(false)
    })

    it('{ field: { $gte: 0 } } returns false for undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $gte: 0 } })).toBe(false)
    })

    it('{ field: { $gte: 0 } } returns false for missing field', () => {
      expect(matchesFilter({}, { missing: { $gte: 0 } })).toBe(false)
    })
  })

  describe('$lt returns false for null/undefined', () => {
    it('{ field: { $lt: 100 } } returns false for null value', () => {
      expect(matchesFilter({ value: null }, { value: { $lt: 100 } })).toBe(false)
    })

    it('{ field: { $lt: 100 } } returns false for undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $lt: 100 } })).toBe(false)
    })

    it('{ field: { $lt: 100 } } returns false for missing field', () => {
      expect(matchesFilter({}, { missing: { $lt: 100 } })).toBe(false)
    })

    it('{ field: { $lt: Infinity } } returns false for null', () => {
      // Even Infinity doesn't compare to null
      expect(matchesFilter({ value: null }, { value: { $lt: Infinity } })).toBe(false)
    })
  })

  describe('$lte returns false for null/undefined', () => {
    it('{ field: { $lte: 100 } } returns false for null value', () => {
      expect(matchesFilter({ value: null }, { value: { $lte: 100 } })).toBe(false)
    })

    it('{ field: { $lte: 100 } } returns false for undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $lte: 100 } })).toBe(false)
    })

    it('{ field: { $lte: 100 } } returns false for missing field', () => {
      expect(matchesFilter({}, { missing: { $lte: 100 } })).toBe(false)
    })
  })
})

// =============================================================================
// Filter Operators - Existence ($exists)
// =============================================================================

describe('$exists operator: distinguishes null from undefined', () => {
  describe('$exists: true matches present fields', () => {
    it('matches field with non-null value', () => {
      expect(matchesFilter({ value: 'test' }, { value: { $exists: true } })).toBe(true)
    })

    it('matches field with null value (field exists, just null)', () => {
      expect(matchesFilter({ value: null }, { value: { $exists: true } })).toBe(true)
    })

    it('matches field with zero', () => {
      expect(matchesFilter({ value: 0 }, { value: { $exists: true } })).toBe(true)
    })

    it('matches field with empty string', () => {
      expect(matchesFilter({ value: '' }, { value: { $exists: true } })).toBe(true)
    })

    it('matches field with false', () => {
      expect(matchesFilter({ value: false }, { value: { $exists: true } })).toBe(true)
    })

    it('does NOT match missing field (undefined)', () => {
      expect(matchesFilter({}, { missing: { $exists: true } })).toBe(false)
    })

    it('does NOT match field explicitly set to undefined', () => {
      expect(matchesFilter({ value: undefined }, { value: { $exists: true } })).toBe(false)
    })
  })

  describe('$exists: false matches missing fields only', () => {
    it('matches missing field', () => {
      expect(matchesFilter({}, { missing: { $exists: false } })).toBe(true)
    })

    it('matches field explicitly set to undefined', () => {
      expect(matchesFilter({ value: undefined }, { value: { $exists: false } })).toBe(true)
    })

    it('does NOT match field with null value', () => {
      expect(matchesFilter({ value: null }, { value: { $exists: false } })).toBe(false)
    })

    it('does NOT match field with any other value', () => {
      expect(matchesFilter({ value: 'test' }, { value: { $exists: false } })).toBe(false)
      expect(matchesFilter({ value: 0 }, { value: { $exists: false } })).toBe(false)
      expect(matchesFilter({ value: false }, { value: { $exists: false } })).toBe(false)
    })
  })

  describe('$exists is the ONLY operator that distinguishes null from undefined', () => {
    const docWithNull = { value: null }
    const docWithUndefined = { value: undefined }
    const docMissing = {}

    it('$eq treats them the same', () => {
      expect(matchesFilter(docWithNull, { value: { $eq: null } })).toBe(true)
      expect(matchesFilter(docWithUndefined, { value: { $eq: null } })).toBe(true)
      expect(matchesFilter(docMissing, { value: { $eq: null } })).toBe(true)
    })

    it('$exists distinguishes them', () => {
      expect(matchesFilter(docWithNull, { value: { $exists: true } })).toBe(true)
      expect(matchesFilter(docWithUndefined, { value: { $exists: true } })).toBe(false)
      expect(matchesFilter(docMissing, { value: { $exists: true } })).toBe(false)
    })
  })
})

// =============================================================================
// Filter Operators - Type ($type)
// =============================================================================

describe('$type operator: null vs undefined handling', () => {
  it('{ field: { $type: "null" } } matches null value', () => {
    expect(matchesFilter({ value: null }, { value: { $type: 'null' } })).toBe(true)
  })

  it('{ field: { $type: "null" } } matches undefined value', () => {
    expect(matchesFilter({ value: undefined }, { value: { $type: 'null' } })).toBe(true)
  })

  it('{ field: { $type: "null" } } matches missing field', () => {
    expect(matchesFilter({}, { missing: { $type: 'null' } })).toBe(true)
  })

  it('{ field: { $type: "null" } } does not match falsy values', () => {
    expect(matchesFilter({ value: 0 }, { value: { $type: 'null' } })).toBe(false)
    expect(matchesFilter({ value: '' }, { value: { $type: 'null' } })).toBe(false)
    expect(matchesFilter({ value: false }, { value: { $type: 'null' } })).toBe(false)
  })
})

// =============================================================================
// matchesCondition Direct Tests
// =============================================================================

describe('matchesCondition: null vs undefined handling', () => {
  describe('when condition is null', () => {
    it('matches null value', () => {
      expect(matchesCondition(null, null)).toBe(true)
    })

    it('matches undefined value', () => {
      expect(matchesCondition(undefined, null)).toBe(true)
    })

    it('does not match other values', () => {
      expect(matchesCondition('test', null)).toBe(false)
      expect(matchesCondition(0, null)).toBe(false)
      expect(matchesCondition(false, null)).toBe(false)
      expect(matchesCondition('', null)).toBe(false)
    })
  })

  describe('when condition is undefined', () => {
    it('always returns true (no condition)', () => {
      expect(matchesCondition(null, undefined)).toBe(true)
      expect(matchesCondition(undefined, undefined)).toBe(true)
      expect(matchesCondition('anything', undefined)).toBe(true)
      expect(matchesCondition(123, undefined)).toBe(true)
    })
  })
})

// =============================================================================
// Real-World Scenarios
// =============================================================================

describe('real-world scenarios: null vs undefined', () => {
  describe('finding documents with optional fields', () => {
    const users = [
      { id: 1, name: 'Alice', email: 'alice@example.com', phone: null },
      { id: 2, name: 'Bob', email: 'bob@example.com' }, // phone missing
      { id: 3, name: 'Charlie', email: 'charlie@example.com', phone: '+1234567890' },
    ]

    it('find users with no phone (null or missing)', () => {
      const filter: Filter = { phone: null }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Bob'])
    })

    it('find users with phone (exists and not null)', () => {
      const filter: Filter = { phone: { $exists: true, $ne: null } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Charlie'])
    })

    it('find users where phone field exists (even if null)', () => {
      const filter: Filter = { phone: { $exists: true } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })
  })

  describe('handling optional numeric fields', () => {
    const products = [
      { sku: 'A', price: 100, discount: null },
      { sku: 'B', price: 200 }, // discount missing
      { sku: 'C', price: 150, discount: 10 },
      { sku: 'D', price: 300, discount: 0 }, // discount is zero (valid)
    ]

    it('find products with no discount (null or missing)', () => {
      const filter: Filter = { discount: null }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['A', 'B'])
    })

    it('find products with any discount (including zero)', () => {
      const filter: Filter = { discount: { $exists: true, $ne: null } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['C', 'D'])
    })

    it('find products with positive discount', () => {
      const filter: Filter = { discount: { $gt: 0 } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['C'])
    })

    it('comparison operators return false for null/missing', () => {
      // Products A and B don't match $gt: 0 because null/undefined don't compare
      const filter: Filter = { discount: { $gte: 0 } }
      const results = products.filter(p => matchesFilter(p, filter))
      // Only C and D match (have actual numbers)
      expect(results.map(p => p.sku)).toEqual(['C', 'D'])
    })
  })

  describe('sorting with null/undefined values', () => {
    const items = [
      { name: 'A', priority: 3 },
      { name: 'B', priority: null },
      { name: 'C', priority: 1 },
      { name: 'D' }, // priority missing
      { name: 'E', priority: 2 },
    ]

    it('nulls sort before all other values', () => {
      const sorted = [...items].sort((a, b) =>
        compareValues(a.priority, b.priority)
      )
      // Nulls/undefined first (B, D), then sorted by priority (C, E, A)
      expect(sorted.map(i => i.name)).toEqual(['B', 'D', 'C', 'E', 'A'])
    })
  })

  describe('logical operators with null/undefined', () => {
    const docs = [
      { id: 1, a: null, b: 'value' },
      { id: 2, a: 'value', b: null },
      { id: 3 }, // both missing
      { id: 4, a: 'value', b: 'value' },
    ]

    it('$and with null field checks', () => {
      const filter: Filter = { $and: [{ a: null }, { b: 'value' }] }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([1])
    })

    it('$or with null field checks', () => {
      const filter: Filter = { $or: [{ a: null }, { b: null }] }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([1, 2, 3])
    })

    it('$not with null condition', () => {
      // Find docs where a is NOT null (meaning a has a value)
      const filter: Filter = { $not: { a: null } }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([2, 4])
    })

    it('$nor with null conditions', () => {
      // Find docs where neither a nor b is null
      const filter: Filter = { $nor: [{ a: null }, { b: null }] }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([4])
    })
  })

  describe('nested objects with null/undefined', () => {
    const docs = [
      { id: 1, nested: { value: 'test' } },
      { id: 2, nested: { value: null } },
      { id: 3, nested: null },
      { id: 4 }, // nested is missing
      { id: 5, nested: { value: undefined } },
      { id: 6, nested: {} }, // value is missing in nested
    ]

    it('matches nested null value', () => {
      const filter: Filter = { 'nested.value': null }
      const results = docs.filter(d => matchesFilter(d, filter))
      // Doc 2 (value: null), 3 (nested is null -> nested.value is undefined),
      // 4 (nested missing), 5 (value: undefined), 6 (value missing in nested)
      expect(results.map(d => d.id)).toEqual([2, 3, 4, 5, 6])
    })

    it('matches nested non-null value', () => {
      const filter: Filter = { 'nested.value': 'test' }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([1])
    })

    it('$exists on nested field distinguishes null from missing', () => {
      // nested.value exists (true): where nested.value is present, even if null
      const existsFilter: Filter = { 'nested.value': { $exists: true } }
      const existsResults = docs.filter(d => matchesFilter(d, existsFilter))
      expect(existsResults.map(d => d.id)).toEqual([1, 2])

      // nested.value doesn't exist: where nested.value is undefined
      const notExistsFilter: Filter = { 'nested.value': { $exists: false } }
      const notExistsResults = docs.filter(d => matchesFilter(d, notExistsFilter))
      expect(notExistsResults.map(d => d.id)).toEqual([3, 4, 5, 6])
    })
  })

  describe('array elements with null/undefined', () => {
    const docs = [
      { id: 1, tags: ['a', 'b'] },
      { id: 2, tags: [null, 'a'] },
      { id: 3, tags: ['a', undefined] },
      { id: 4, tags: null },
      { id: 5 }, // tags missing
      { id: 6, tags: [] },
    ]

    it('$all with null element', () => {
      const filter: Filter = { tags: { $all: [null] } }
      const results = docs.filter(d => matchesFilter(d, filter))
      // Doc 2 and 3: arrays contain null/undefined
      expect(results.map(d => d.id)).toEqual([2, 3])
    })

    it('$size works with arrays containing null', () => {
      const filter: Filter = { tags: { $size: 2 } }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([1, 2, 3])
    })

    it('$size fails on null/missing array', () => {
      // null and missing are not arrays, so $size doesn't match
      const filter: Filter = { tags: { $size: 0 } }
      const results = docs.filter(d => matchesFilter(d, filter))
      expect(results.map(d => d.id)).toEqual([6])
    })
  })
})

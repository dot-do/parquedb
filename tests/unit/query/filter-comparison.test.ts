/**
 * Comprehensive Comparison Operator Tests for Filter Evaluation
 *
 * This test file provides exhaustive coverage of comparison operators:
 * - $eq (equals)
 * - $ne (not equals)
 * - $gt (greater than)
 * - $gte (greater than or equal)
 * - $lt (less than)
 * - $lte (less than or equal)
 * - $in (in array)
 * - $nin (not in array)
 *
 * Coverage includes:
 * - Basic type comparisons (string, number, boolean, null)
 * - Edge cases (undefined values, type coercion, empty arrays)
 * - Nested field access with dot notation
 * - Array field matching
 *
 * TDD RED Phase - Some tests may fail to verify test coverage gaps
 */

import { describe, it, expect } from 'vitest'
import { matchesFilter, matchesCondition } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// $eq Operator - Equality Comparison
// =============================================================================

describe('$eq operator', () => {
  describe('basic type comparisons', () => {
    describe('string comparisons', () => {
      it('matches equal strings', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $eq: 'Alice' } })).toBe(true)
      })

      it('does not match different strings', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $eq: 'Bob' } })).toBe(false)
      })

      it('is case sensitive', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $eq: 'alice' } })).toBe(false)
        expect(matchesFilter({ name: 'HELLO' }, { name: { $eq: 'hello' } })).toBe(false)
      })

      it('matches empty string', () => {
        expect(matchesFilter({ name: '' }, { name: { $eq: '' } })).toBe(true)
        expect(matchesFilter({ name: 'test' }, { name: { $eq: '' } })).toBe(false)
      })

      it('handles whitespace strings', () => {
        expect(matchesFilter({ name: '  ' }, { name: { $eq: '  ' } })).toBe(true)
        expect(matchesFilter({ name: ' ' }, { name: { $eq: '  ' } })).toBe(false)
      })

      it('handles unicode strings', () => {
        expect(matchesFilter({ emoji: '\u{1F600}' }, { emoji: { $eq: '\u{1F600}' } })).toBe(true)
        expect(matchesFilter({ text: '\u00E9' }, { text: { $eq: '\u00E9' } })).toBe(true) // e with accent
      })

      it('handles special characters', () => {
        expect(matchesFilter({ path: '/path/to/file' }, { path: { $eq: '/path/to/file' } })).toBe(true)
        expect(matchesFilter({ regex: '.*[a-z]+' }, { regex: { $eq: '.*[a-z]+' } })).toBe(true)
      })
    })

    describe('number comparisons', () => {
      it('matches equal integers', () => {
        expect(matchesFilter({ count: 42 }, { count: { $eq: 42 } })).toBe(true)
        expect(matchesFilter({ count: 42 }, { count: { $eq: 43 } })).toBe(false)
      })

      it('matches equal floating point numbers', () => {
        expect(matchesFilter({ price: 19.99 }, { price: { $eq: 19.99 } })).toBe(true)
        expect(matchesFilter({ price: 19.99 }, { price: { $eq: 19.98 } })).toBe(false)
      })

      it('matches zero', () => {
        expect(matchesFilter({ count: 0 }, { count: { $eq: 0 } })).toBe(true)
        expect(matchesFilter({ count: 0 }, { count: { $eq: 1 } })).toBe(false)
      })

      it('matches negative numbers', () => {
        expect(matchesFilter({ temp: -10 }, { temp: { $eq: -10 } })).toBe(true)
        expect(matchesFilter({ temp: -10 }, { temp: { $eq: 10 } })).toBe(false)
      })

      it('handles Infinity', () => {
        expect(matchesFilter({ value: Infinity }, { value: { $eq: Infinity } })).toBe(true)
        expect(matchesFilter({ value: -Infinity }, { value: { $eq: -Infinity } })).toBe(true)
        expect(matchesFilter({ value: Infinity }, { value: { $eq: -Infinity } })).toBe(false)
      })

      it('handles NaN (should not equal itself)', () => {
        // NaN !== NaN in JavaScript
        expect(matchesFilter({ value: NaN }, { value: { $eq: NaN } })).toBe(false)
      })

      it('does not coerce string to number', () => {
        expect(matchesFilter({ count: 42 }, { count: { $eq: '42' } })).toBe(false)
        expect(matchesFilter({ count: '42' }, { count: { $eq: 42 } })).toBe(false)
      })
    })

    describe('boolean comparisons', () => {
      it('matches true', () => {
        expect(matchesFilter({ active: true }, { active: { $eq: true } })).toBe(true)
        expect(matchesFilter({ active: true }, { active: { $eq: false } })).toBe(false)
      })

      it('matches false', () => {
        expect(matchesFilter({ active: false }, { active: { $eq: false } })).toBe(true)
        expect(matchesFilter({ active: false }, { active: { $eq: true } })).toBe(false)
      })

      it('does not coerce truthy/falsy values', () => {
        expect(matchesFilter({ value: 1 }, { value: { $eq: true } })).toBe(false)
        expect(matchesFilter({ value: 0 }, { value: { $eq: false } })).toBe(false)
        expect(matchesFilter({ value: 'true' }, { value: { $eq: true } })).toBe(false)
        expect(matchesFilter({ value: '' }, { value: { $eq: false } })).toBe(false)
      })
    })

    describe('null comparisons', () => {
      it('matches null', () => {
        expect(matchesFilter({ value: null }, { value: { $eq: null } })).toBe(true)
      })

      it('does not match non-null values', () => {
        expect(matchesFilter({ value: 'test' }, { value: { $eq: null } })).toBe(false)
        expect(matchesFilter({ value: 0 }, { value: { $eq: null } })).toBe(false)
        expect(matchesFilter({ value: false }, { value: { $eq: null } })).toBe(false)
      })

      it('treats undefined as equal to null (MongoDB behavior)', () => {
        expect(matchesFilter({ value: undefined }, { value: { $eq: null } })).toBe(true)
        expect(matchesFilter({}, { missing: { $eq: null } })).toBe(true)
      })
    })

    describe('Date comparisons', () => {
      it('matches equal dates', () => {
        const date = new Date('2024-06-15T10:30:00Z')
        expect(matchesFilter({ created: date }, { created: { $eq: new Date('2024-06-15T10:30:00Z') } })).toBe(true)
      })

      it('does not match different dates', () => {
        const date = new Date('2024-06-15T10:30:00Z')
        expect(matchesFilter({ created: date }, { created: { $eq: new Date('2024-06-16T10:30:00Z') } })).toBe(false)
      })

      it('matches same date different timezone representation', () => {
        const date1 = new Date('2024-06-15T10:30:00Z')
        const date2 = new Date('2024-06-15T05:30:00-05:00') // Same moment
        expect(matchesFilter({ created: date1 }, { created: { $eq: date2 } })).toBe(true)
      })
    })

    describe('array comparisons', () => {
      it('matches equal arrays (order matters)', () => {
        expect(matchesFilter({ tags: ['a', 'b', 'c'] }, { tags: { $eq: ['a', 'b', 'c'] } })).toBe(true)
        expect(matchesFilter({ tags: ['a', 'b', 'c'] }, { tags: { $eq: ['c', 'b', 'a'] } })).toBe(false)
      })

      it('matches empty arrays', () => {
        expect(matchesFilter({ tags: [] }, { tags: { $eq: [] } })).toBe(true)
        expect(matchesFilter({ tags: ['a'] }, { tags: { $eq: [] } })).toBe(false)
      })

      it('matches nested arrays', () => {
        expect(matchesFilter({ matrix: [[1, 2], [3, 4]] }, { matrix: { $eq: [[1, 2], [3, 4]] } })).toBe(true)
        expect(matchesFilter({ matrix: [[1, 2], [3, 4]] }, { matrix: { $eq: [[1, 2], [4, 3]] } })).toBe(false)
      })
    })

    describe('object comparisons', () => {
      it('matches equal objects', () => {
        expect(matchesFilter(
          { meta: { key: 'value', count: 5 } },
          { meta: { $eq: { key: 'value', count: 5 } } }
        )).toBe(true)
      })

      it('does not match partial objects', () => {
        expect(matchesFilter(
          { meta: { key: 'value', count: 5 } },
          { meta: { $eq: { key: 'value' } } }
        )).toBe(false)
      })

      it('matches regardless of key order', () => {
        expect(matchesFilter(
          { meta: { a: 1, b: 2 } },
          { meta: { $eq: { b: 2, a: 1 } } }
        )).toBe(true)
      })

      it('matches empty objects', () => {
        expect(matchesFilter({ meta: {} }, { meta: { $eq: {} } })).toBe(true)
        expect(matchesFilter({ meta: { key: 'value' } }, { meta: { $eq: {} } })).toBe(false)
      })
    })
  })

  describe('edge cases', () => {
    it('handles missing field (returns false for non-null comparison)', () => {
      expect(matchesFilter({}, { missing: { $eq: 'value' } })).toBe(false)
      expect(matchesFilter({ other: 'test' }, { missing: { $eq: 'value' } })).toBe(false)
    })

    it('handles explicit undefined value', () => {
      expect(matchesFilter({ value: undefined }, { value: { $eq: undefined } })).toBe(true)
    })
  })
})

// =============================================================================
// $ne Operator - Not Equal Comparison
// =============================================================================

describe('$ne operator', () => {
  describe('basic type comparisons', () => {
    describe('string comparisons', () => {
      it('matches different strings', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $ne: 'Bob' } })).toBe(true)
      })

      it('does not match equal strings', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $ne: 'Alice' } })).toBe(false)
      })

      it('is case sensitive', () => {
        expect(matchesFilter({ name: 'Alice' }, { name: { $ne: 'alice' } })).toBe(true)
      })
    })

    describe('number comparisons', () => {
      it('matches different numbers', () => {
        expect(matchesFilter({ count: 42 }, { count: { $ne: 100 } })).toBe(true)
      })

      it('does not match equal numbers', () => {
        expect(matchesFilter({ count: 42 }, { count: { $ne: 42 } })).toBe(false)
      })
    })

    describe('boolean comparisons', () => {
      it('matches different booleans', () => {
        expect(matchesFilter({ active: true }, { active: { $ne: false } })).toBe(true)
      })

      it('does not match equal booleans', () => {
        expect(matchesFilter({ active: true }, { active: { $ne: true } })).toBe(false)
      })
    })

    describe('null comparisons', () => {
      it('matches non-null when comparing to null', () => {
        expect(matchesFilter({ value: 'test' }, { value: { $ne: null } })).toBe(true)
        expect(matchesFilter({ value: 0 }, { value: { $ne: null } })).toBe(true)
        expect(matchesFilter({ value: false }, { value: { $ne: null } })).toBe(true)
      })

      it('does not match null when comparing to null', () => {
        expect(matchesFilter({ value: null }, { value: { $ne: null } })).toBe(false)
      })

      it('treats undefined as equal to null', () => {
        expect(matchesFilter({ value: undefined }, { value: { $ne: null } })).toBe(false)
      })
    })
  })

  describe('edge cases', () => {
    it('matches when field is missing (undefined != value)', () => {
      expect(matchesFilter({}, { missing: { $ne: 'value' } })).toBe(true)
      expect(matchesFilter({ other: 'test' }, { missing: { $ne: 100 } })).toBe(true)
    })

    it('missing field does not equal null (undefined == null in MongoDB)', () => {
      expect(matchesFilter({}, { missing: { $ne: null } })).toBe(false)
    })

    it('handles NaN (NaN != NaN is true in JS)', () => {
      // This might be a tricky edge case
      expect(matchesFilter({ value: NaN }, { value: { $ne: NaN } })).toBe(true)
    })
  })
})

// =============================================================================
// $gt Operator - Greater Than Comparison
// =============================================================================

describe('$gt operator', () => {
  describe('basic type comparisons', () => {
    describe('number comparisons', () => {
      it('matches when value is greater', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gt: 50 } })).toBe(true)
      })

      it('does not match when value is equal', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gt: 100 } })).toBe(false)
      })

      it('does not match when value is less', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gt: 150 } })).toBe(false)
      })

      it('handles negative numbers', () => {
        expect(matchesFilter({ temp: -5 }, { temp: { $gt: -10 } })).toBe(true)
        expect(matchesFilter({ temp: -10 }, { temp: { $gt: -5 } })).toBe(false)
      })

      it('handles zero', () => {
        expect(matchesFilter({ count: 1 }, { count: { $gt: 0 } })).toBe(true)
        expect(matchesFilter({ count: 0 }, { count: { $gt: 0 } })).toBe(false)
        expect(matchesFilter({ count: -1 }, { count: { $gt: 0 } })).toBe(false)
      })

      it('handles floating point numbers', () => {
        expect(matchesFilter({ price: 19.99 }, { price: { $gt: 19.98 } })).toBe(true)
        expect(matchesFilter({ price: 19.99 }, { price: { $gt: 19.99 } })).toBe(false)
        expect(matchesFilter({ price: 19.99 }, { price: { $gt: 20.00 } })).toBe(false)
      })

      it('handles Infinity', () => {
        expect(matchesFilter({ value: Infinity }, { value: { $gt: 1000000 } })).toBe(true)
        expect(matchesFilter({ value: 1000000 }, { value: { $gt: Infinity } })).toBe(false)
        expect(matchesFilter({ value: -Infinity }, { value: { $gt: -1000000 } })).toBe(false)
      })
    })

    describe('string comparisons (lexicographic)', () => {
      it('compares strings lexicographically', () => {
        expect(matchesFilter({ name: 'charlie' }, { name: { $gt: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'bob' }, { name: { $gt: 'charlie' } })).toBe(false)
      })

      it('handles case sensitivity in string comparison', () => {
        // Capital letters come before lowercase in ASCII
        expect(matchesFilter({ name: 'alice' }, { name: { $gt: 'Alice' } })).toBe(true)
        expect(matchesFilter({ name: 'Alice' }, { name: { $gt: 'bob' } })).toBe(false)
      })

      it('compares empty string', () => {
        expect(matchesFilter({ name: 'a' }, { name: { $gt: '' } })).toBe(true)
        expect(matchesFilter({ name: '' }, { name: { $gt: '' } })).toBe(false)
      })
    })

    describe('Date comparisons', () => {
      it('matches when date is after', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $gt: new Date('2024-06-01') } }
        )).toBe(true)
      })

      it('does not match when date is equal', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $gt: new Date('2024-06-15') } }
        )).toBe(false)
      })

      it('does not match when date is before', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $gt: new Date('2024-06-30') } }
        )).toBe(false)
      })
    })

    describe('boolean comparisons', () => {
      it('true > false in comparison', () => {
        expect(matchesFilter({ active: true }, { active: { $gt: false } })).toBe(true)
        expect(matchesFilter({ active: false }, { active: { $gt: true } })).toBe(false)
      })

      it('equal booleans are not greater', () => {
        expect(matchesFilter({ active: true }, { active: { $gt: true } })).toBe(false)
        expect(matchesFilter({ active: false }, { active: { $gt: false } })).toBe(false)
      })
    })
  })

  describe('edge cases', () => {
    it('returns false for null value', () => {
      expect(matchesFilter({ score: null }, { score: { $gt: 0 } })).toBe(false)
    })

    it('returns false for undefined value', () => {
      expect(matchesFilter({ score: undefined }, { score: { $gt: 0 } })).toBe(false)
    })

    it('returns false for missing field', () => {
      expect(matchesFilter({}, { score: { $gt: 0 } })).toBe(false)
    })

    it('does not coerce types', () => {
      // String '100' should not be compared numerically to 50
      expect(matchesFilter({ score: '100' }, { score: { $gt: 50 } })).toBe(false)
    })

    it('handles NaN', () => {
      // NaN comparisons should always be false
      expect(matchesFilter({ value: NaN }, { value: { $gt: 0 } })).toBe(false)
      expect(matchesFilter({ value: 0 }, { value: { $gt: NaN } })).toBe(false)
    })
  })
})

// =============================================================================
// $gte Operator - Greater Than or Equal Comparison
// =============================================================================

describe('$gte operator', () => {
  describe('basic type comparisons', () => {
    describe('number comparisons', () => {
      it('matches when value is greater', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gte: 50 } })).toBe(true)
      })

      it('matches when value is equal', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gte: 100 } })).toBe(true)
      })

      it('does not match when value is less', () => {
        expect(matchesFilter({ score: 100 }, { score: { $gte: 150 } })).toBe(false)
      })

      it('handles edge case of exact equality', () => {
        expect(matchesFilter({ score: 0 }, { score: { $gte: 0 } })).toBe(true)
        expect(matchesFilter({ score: -1 }, { score: { $gte: -1 } })).toBe(true)
      })
    })

    describe('string comparisons', () => {
      it('matches when string is lexicographically greater or equal', () => {
        expect(matchesFilter({ name: 'charlie' }, { name: { $gte: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'bob' }, { name: { $gte: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'alice' }, { name: { $gte: 'bob' } })).toBe(false)
      })
    })

    describe('Date comparisons', () => {
      it('matches when date is equal or after', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $gte: new Date('2024-06-15') } }
        )).toBe(true)
        expect(matchesFilter(
          { created: new Date('2024-06-16') },
          { created: { $gte: new Date('2024-06-15') } }
        )).toBe(true)
      })
    })
  })

  describe('edge cases', () => {
    it('returns false for null value', () => {
      expect(matchesFilter({ score: null }, { score: { $gte: 0 } })).toBe(false)
    })

    it('returns false for undefined value', () => {
      expect(matchesFilter({}, { score: { $gte: 0 } })).toBe(false)
    })
  })
})

// =============================================================================
// $lt Operator - Less Than Comparison
// =============================================================================

describe('$lt operator', () => {
  describe('basic type comparisons', () => {
    describe('number comparisons', () => {
      it('matches when value is less', () => {
        expect(matchesFilter({ score: 50 }, { score: { $lt: 100 } })).toBe(true)
      })

      it('does not match when value is equal', () => {
        expect(matchesFilter({ score: 100 }, { score: { $lt: 100 } })).toBe(false)
      })

      it('does not match when value is greater', () => {
        expect(matchesFilter({ score: 150 }, { score: { $lt: 100 } })).toBe(false)
      })

      it('handles negative numbers', () => {
        expect(matchesFilter({ temp: -10 }, { temp: { $lt: -5 } })).toBe(true)
        expect(matchesFilter({ temp: -5 }, { temp: { $lt: -10 } })).toBe(false)
      })

      it('handles zero', () => {
        expect(matchesFilter({ count: -1 }, { count: { $lt: 0 } })).toBe(true)
        expect(matchesFilter({ count: 0 }, { count: { $lt: 0 } })).toBe(false)
        expect(matchesFilter({ count: 1 }, { count: { $lt: 0 } })).toBe(false)
      })

      it('handles floating point numbers', () => {
        expect(matchesFilter({ price: 19.98 }, { price: { $lt: 19.99 } })).toBe(true)
        expect(matchesFilter({ price: 19.99 }, { price: { $lt: 19.99 } })).toBe(false)
      })
    })

    describe('string comparisons', () => {
      it('compares strings lexicographically', () => {
        expect(matchesFilter({ name: 'alice' }, { name: { $lt: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'charlie' }, { name: { $lt: 'bob' } })).toBe(false)
      })
    })

    describe('Date comparisons', () => {
      it('matches when date is before', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-01') },
          { created: { $lt: new Date('2024-06-15') } }
        )).toBe(true)
      })

      it('does not match when date is equal or after', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $lt: new Date('2024-06-15') } }
        )).toBe(false)
      })
    })

    describe('boolean comparisons', () => {
      it('false < true in comparison', () => {
        expect(matchesFilter({ active: false }, { active: { $lt: true } })).toBe(true)
        expect(matchesFilter({ active: true }, { active: { $lt: false } })).toBe(false)
      })
    })
  })

  describe('edge cases', () => {
    it('returns false for null value', () => {
      expect(matchesFilter({ score: null }, { score: { $lt: 100 } })).toBe(false)
    })

    it('returns false for undefined value', () => {
      expect(matchesFilter({}, { score: { $lt: 100 } })).toBe(false)
    })

    it('does not coerce types', () => {
      expect(matchesFilter({ score: '50' }, { score: { $lt: 100 } })).toBe(false)
    })
  })
})

// =============================================================================
// $lte Operator - Less Than or Equal Comparison
// =============================================================================

describe('$lte operator', () => {
  describe('basic type comparisons', () => {
    describe('number comparisons', () => {
      it('matches when value is less', () => {
        expect(matchesFilter({ score: 50 }, { score: { $lte: 100 } })).toBe(true)
      })

      it('matches when value is equal', () => {
        expect(matchesFilter({ score: 100 }, { score: { $lte: 100 } })).toBe(true)
      })

      it('does not match when value is greater', () => {
        expect(matchesFilter({ score: 150 }, { score: { $lte: 100 } })).toBe(false)
      })

      it('handles edge case of exact equality', () => {
        expect(matchesFilter({ score: 0 }, { score: { $lte: 0 } })).toBe(true)
        expect(matchesFilter({ score: -1 }, { score: { $lte: -1 } })).toBe(true)
      })
    })

    describe('string comparisons', () => {
      it('matches when string is lexicographically less or equal', () => {
        expect(matchesFilter({ name: 'alice' }, { name: { $lte: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'bob' }, { name: { $lte: 'bob' } })).toBe(true)
        expect(matchesFilter({ name: 'charlie' }, { name: { $lte: 'bob' } })).toBe(false)
      })
    })

    describe('Date comparisons', () => {
      it('matches when date is equal or before', () => {
        expect(matchesFilter(
          { created: new Date('2024-06-15') },
          { created: { $lte: new Date('2024-06-15') } }
        )).toBe(true)
        expect(matchesFilter(
          { created: new Date('2024-06-14') },
          { created: { $lte: new Date('2024-06-15') } }
        )).toBe(true)
      })
    })
  })

  describe('edge cases', () => {
    it('returns false for null value', () => {
      expect(matchesFilter({ score: null }, { score: { $lte: 100 } })).toBe(false)
    })

    it('returns false for undefined value', () => {
      expect(matchesFilter({}, { score: { $lte: 100 } })).toBe(false)
    })
  })
})

// =============================================================================
// $in Operator - In Array Comparison
// =============================================================================

describe('$in operator', () => {
  describe('basic type comparisons', () => {
    describe('string comparisons', () => {
      it('matches when value is in array', () => {
        expect(matchesFilter(
          { status: 'published' },
          { status: { $in: ['draft', 'published', 'archived'] } }
        )).toBe(true)
      })

      it('does not match when value is not in array', () => {
        expect(matchesFilter(
          { status: 'pending' },
          { status: { $in: ['draft', 'published', 'archived'] } }
        )).toBe(false)
      })

      it('is case sensitive', () => {
        expect(matchesFilter(
          { status: 'Published' },
          { status: { $in: ['published', 'draft'] } }
        )).toBe(false)
      })
    })

    describe('number comparisons', () => {
      it('matches when number is in array', () => {
        expect(matchesFilter({ code: 42 }, { code: { $in: [1, 2, 42, 100] } })).toBe(true)
      })

      it('does not match when number is not in array', () => {
        expect(matchesFilter({ code: 42 }, { code: { $in: [1, 2, 3] } })).toBe(false)
      })

      it('handles floating point numbers', () => {
        expect(matchesFilter({ price: 19.99 }, { price: { $in: [19.99, 29.99] } })).toBe(true)
      })

      it('handles negative numbers', () => {
        expect(matchesFilter({ temp: -10 }, { temp: { $in: [-20, -10, 0] } })).toBe(true)
      })
    })

    describe('boolean comparisons', () => {
      it('matches when boolean is in array', () => {
        expect(matchesFilter({ active: true }, { active: { $in: [true] } })).toBe(true)
        expect(matchesFilter({ active: false }, { active: { $in: [false] } })).toBe(true)
      })

      it('does not coerce truthy/falsy', () => {
        expect(matchesFilter({ value: 1 }, { value: { $in: [true] } })).toBe(false)
        expect(matchesFilter({ value: 0 }, { value: { $in: [false] } })).toBe(false)
      })
    })

    describe('null comparisons', () => {
      it('matches null when null is in array', () => {
        expect(matchesFilter({ value: null }, { value: { $in: [null, 'other'] } })).toBe(true)
      })

      it('treats undefined as matching null in $in array', () => {
        expect(matchesFilter({}, { missing: { $in: [null, 'other'] } })).toBe(true)
        expect(matchesFilter({ value: undefined }, { value: { $in: [null] } })).toBe(true)
      })
    })

    describe('Date comparisons', () => {
      it('matches when date is in array', () => {
        const date = new Date('2024-06-15')
        expect(matchesFilter(
          { created: date },
          { created: { $in: [new Date('2024-06-01'), new Date('2024-06-15')] } }
        )).toBe(true)
      })
    })

    describe('object comparisons', () => {
      it('matches when object is in array (deep equality)', () => {
        expect(matchesFilter(
          { meta: { key: 'value' } },
          { meta: { $in: [{ key: 'value' }, { key: 'other' }] } }
        )).toBe(true)
      })

      it('does not match partial objects', () => {
        expect(matchesFilter(
          { meta: { key: 'value', extra: true } },
          { meta: { $in: [{ key: 'value' }] } }
        )).toBe(false)
      })
    })

    describe('array comparisons', () => {
      it('matches when array is in array (exact match)', () => {
        expect(matchesFilter(
          { tags: ['a', 'b'] },
          { tags: { $in: [['a', 'b'], ['c', 'd']] } }
        )).toBe(true)
      })

      it('array order matters', () => {
        expect(matchesFilter(
          { tags: ['b', 'a'] },
          { tags: { $in: [['a', 'b']] } }
        )).toBe(false)
      })
    })
  })

  describe('edge cases', () => {
    it('handles empty $in array (nothing matches)', () => {
      expect(matchesFilter({ status: 'published' }, { status: { $in: [] } })).toBe(false)
      expect(matchesFilter({ value: null }, { value: { $in: [] } })).toBe(false)
    })

    it('handles single element $in array', () => {
      expect(matchesFilter({ status: 'published' }, { status: { $in: ['published'] } })).toBe(true)
      expect(matchesFilter({ status: 'draft' }, { status: { $in: ['published'] } })).toBe(false)
    })

    it('handles mixed type $in array', () => {
      expect(matchesFilter({ value: 42 }, { value: { $in: ['42', 42, true] } })).toBe(true)
      expect(matchesFilter({ value: '42' }, { value: { $in: ['42', 42, true] } })).toBe(true)
      expect(matchesFilter({ value: true }, { value: { $in: ['42', 42, true] } })).toBe(true)
    })

    it('returns false when $in is not an array', () => {
      // This should gracefully handle invalid input
      expect(matchesFilter({ value: 'test' }, { value: { $in: 'test' as any } })).toBe(false)
    })

    it('handles undefined field', () => {
      expect(matchesFilter({}, { missing: { $in: ['value'] } })).toBe(false)
    })
  })
})

// =============================================================================
// $nin Operator - Not In Array Comparison
// =============================================================================

describe('$nin operator', () => {
  describe('basic type comparisons', () => {
    describe('string comparisons', () => {
      it('matches when value is not in array', () => {
        expect(matchesFilter(
          { status: 'pending' },
          { status: { $nin: ['draft', 'published'] } }
        )).toBe(true)
      })

      it('does not match when value is in array', () => {
        expect(matchesFilter(
          { status: 'published' },
          { status: { $nin: ['draft', 'published'] } }
        )).toBe(false)
      })
    })

    describe('number comparisons', () => {
      it('matches when number is not in array', () => {
        expect(matchesFilter({ code: 42 }, { code: { $nin: [1, 2, 3] } })).toBe(true)
      })

      it('does not match when number is in array', () => {
        expect(matchesFilter({ code: 42 }, { code: { $nin: [1, 42, 100] } })).toBe(false)
      })
    })

    describe('null comparisons', () => {
      it('does not match null when null is in array', () => {
        expect(matchesFilter({ value: null }, { value: { $nin: [null, 'other'] } })).toBe(false)
      })

      it('matches null when null is not in array', () => {
        expect(matchesFilter({ value: null }, { value: { $nin: ['other'] } })).toBe(true)
      })

      it('treats undefined as null for $nin', () => {
        expect(matchesFilter({}, { missing: { $nin: [null] } })).toBe(false)
        expect(matchesFilter({}, { missing: { $nin: ['value'] } })).toBe(true)
      })
    })
  })

  describe('edge cases', () => {
    it('handles empty $nin array (everything matches)', () => {
      expect(matchesFilter({ status: 'anything' }, { status: { $nin: [] } })).toBe(true)
      expect(matchesFilter({ value: null }, { value: { $nin: [] } })).toBe(true)
    })

    it('handles single element $nin array', () => {
      expect(matchesFilter({ status: 'draft' }, { status: { $nin: ['published'] } })).toBe(true)
      expect(matchesFilter({ status: 'published' }, { status: { $nin: ['published'] } })).toBe(false)
    })

    it('returns false when $nin is not an array', () => {
      // This should gracefully handle invalid input
      expect(matchesFilter({ value: 'test' }, { value: { $nin: 'test' as any } })).toBe(false)
    })

    it('handles undefined field', () => {
      // undefined is treated as null, so if null is not in $nin array, it should match
      expect(matchesFilter({}, { missing: { $nin: ['value', 'other'] } })).toBe(true)
    })
  })
})

// =============================================================================
// Nested Field Access with Dot Notation
// =============================================================================

describe('nested field access with dot notation', () => {
  describe('$eq with nested fields', () => {
    it('matches nested field with $eq', () => {
      const doc = { user: { name: 'Alice', profile: { age: 30 } } }
      expect(matchesFilter(doc, { 'user.name': { $eq: 'Alice' } })).toBe(true)
      expect(matchesFilter(doc, { 'user.profile.age': { $eq: 30 } })).toBe(true)
    })

    it('returns false for non-matching nested field', () => {
      const doc = { user: { name: 'Alice' } }
      expect(matchesFilter(doc, { 'user.name': { $eq: 'Bob' } })).toBe(false)
    })
  })

  describe('$ne with nested fields', () => {
    it('matches when nested field is not equal', () => {
      const doc = { user: { role: 'admin' } }
      expect(matchesFilter(doc, { 'user.role': { $ne: 'user' } })).toBe(true)
    })
  })

  describe('$gt/$gte/$lt/$lte with nested fields', () => {
    it('compares nested numeric fields', () => {
      const doc = { product: { price: 50, stock: 100 } }
      expect(matchesFilter(doc, { 'product.price': { $gt: 25 } })).toBe(true)
      expect(matchesFilter(doc, { 'product.price': { $gte: 50 } })).toBe(true)
      expect(matchesFilter(doc, { 'product.stock': { $lt: 200 } })).toBe(true)
      expect(matchesFilter(doc, { 'product.stock': { $lte: 100 } })).toBe(true)
    })

    it('compares nested date fields', () => {
      const doc = { event: { date: new Date('2024-06-15') } }
      expect(matchesFilter(doc, { 'event.date': { $gt: new Date('2024-06-01') } })).toBe(true)
    })
  })

  describe('$in/$nin with nested fields', () => {
    it('matches nested field with $in', () => {
      const doc = { user: { status: 'active' } }
      expect(matchesFilter(doc, { 'user.status': { $in: ['active', 'pending'] } })).toBe(true)
    })

    it('matches nested field with $nin', () => {
      const doc = { user: { role: 'viewer' } }
      expect(matchesFilter(doc, { 'user.role': { $nin: ['admin', 'moderator'] } })).toBe(true)
    })
  })

  describe('deeply nested paths', () => {
    it('handles 4+ levels of nesting', () => {
      const doc = {
        level1: {
          level2: {
            level3: {
              level4: {
                value: 42
              }
            }
          }
        }
      }
      expect(matchesFilter(doc, { 'level1.level2.level3.level4.value': { $eq: 42 } })).toBe(true)
      expect(matchesFilter(doc, { 'level1.level2.level3.level4.value': { $gt: 40 } })).toBe(true)
    })
  })

  describe('missing nested paths', () => {
    it('returns undefined for missing intermediate path', () => {
      const doc = { user: { name: 'Alice' } }
      expect(matchesFilter(doc, { 'user.profile.age': { $gt: 0 } })).toBe(false)
      expect(matchesFilter(doc, { 'user.profile.age': { $eq: null } })).toBe(true)
    })

    it('handles null in nested path', () => {
      const doc = { user: null }
      expect(matchesFilter(doc, { 'user.name': { $gt: '' } })).toBe(false)
      expect(matchesFilter(doc, { 'user.name': { $eq: null } })).toBe(true)
    })
  })

  describe('array index access', () => {
    it('accesses array elements by index', () => {
      const doc = { items: [{ name: 'first' }, { name: 'second' }] }
      expect(matchesFilter(doc, { 'items.0.name': { $eq: 'first' } })).toBe(true)
      expect(matchesFilter(doc, { 'items.1.name': { $eq: 'second' } })).toBe(true)
    })

    it('returns undefined for out of bounds index', () => {
      const doc = { items: [{ name: 'first' }] }
      expect(matchesFilter(doc, { 'items.5.name': { $eq: null } })).toBe(true)
      expect(matchesFilter(doc, { 'items.5.name': { $gt: '' } })).toBe(false)
    })
  })
})

// =============================================================================
// Array Field Matching
// =============================================================================

describe('array field matching', () => {
  describe('$eq with array fields', () => {
    it('matches exact array', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: { $eq: ['a', 'b', 'c'] } })).toBe(true)
    })

    it('does not match when order differs', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: { $eq: ['c', 'b', 'a'] } })).toBe(false)
    })

    it('does not match when length differs', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: { $eq: ['a', 'b'] } })).toBe(false)
    })
  })

  describe('$ne with array fields', () => {
    it('matches when array is different', () => {
      const doc = { tags: ['a', 'b'] }
      expect(matchesFilter(doc, { tags: { $ne: ['x', 'y'] } })).toBe(true)
    })

    it('does not match when array is same', () => {
      const doc = { tags: ['a', 'b'] }
      expect(matchesFilter(doc, { tags: { $ne: ['a', 'b'] } })).toBe(false)
    })
  })

  describe('$in with array fields (element matching)', () => {
    // In MongoDB, if the field is an array, $in checks if any element matches
    // This might be implementation-specific behavior
    it('matches if field array value is in the $in array', () => {
      const doc = { tags: ['javascript', 'typescript'] }
      // This checks if the array ['javascript', 'typescript'] is in the $in array
      expect(matchesFilter(doc, { tags: { $in: [['javascript', 'typescript'], ['python']] } })).toBe(true)
    })
  })

  describe('comparison operators on arrays', () => {
    it('compares arrays lexicographically with $gt', () => {
      // Arrays are compared element by element
      const doc = { values: [2, 3, 4] }
      expect(matchesFilter(doc, { values: { $gt: [1, 2, 3] } })).toBe(true)
      expect(matchesFilter(doc, { values: { $gt: [2, 3, 5] } })).toBe(false)
    })
  })
})

// =============================================================================
// Combined Comparison Operators
// =============================================================================

describe('combined comparison operators', () => {
  describe('range queries', () => {
    it('combines $gt and $lt for exclusive range', () => {
      expect(matchesFilter({ score: 50 }, { score: { $gt: 25, $lt: 75 } })).toBe(true)
      expect(matchesFilter({ score: 25 }, { score: { $gt: 25, $lt: 75 } })).toBe(false)
      expect(matchesFilter({ score: 75 }, { score: { $gt: 25, $lt: 75 } })).toBe(false)
    })

    it('combines $gte and $lte for inclusive range', () => {
      expect(matchesFilter({ score: 50 }, { score: { $gte: 25, $lte: 75 } })).toBe(true)
      expect(matchesFilter({ score: 25 }, { score: { $gte: 25, $lte: 75 } })).toBe(true)
      expect(matchesFilter({ score: 75 }, { score: { $gte: 25, $lte: 75 } })).toBe(true)
      expect(matchesFilter({ score: 24 }, { score: { $gte: 25, $lte: 75 } })).toBe(false)
      expect(matchesFilter({ score: 76 }, { score: { $gte: 25, $lte: 75 } })).toBe(false)
    })

    it('handles date ranges', () => {
      const date = new Date('2024-06-15')
      expect(matchesFilter(
        { created: date },
        { created: { $gte: new Date('2024-06-01'), $lte: new Date('2024-06-30') } }
      )).toBe(true)
    })
  })

  describe('combining $ne with comparison operators', () => {
    it('combines $ne and $gt', () => {
      expect(matchesFilter({ score: 100 }, { score: { $ne: 50, $gt: 75 } })).toBe(true)
      expect(matchesFilter({ score: 50 }, { score: { $ne: 50, $gt: 25 } })).toBe(false)
    })

    it('combines $ne with $in', () => {
      // $ne and $in together
      expect(matchesFilter(
        { status: 'active' },
        { status: { $in: ['active', 'pending'], $ne: 'pending' } }
      )).toBe(true)
      expect(matchesFilter(
        { status: 'pending' },
        { status: { $in: ['active', 'pending'], $ne: 'pending' } }
      )).toBe(false)
    })
  })
})

// =============================================================================
// matchesCondition Direct Tests
// =============================================================================

describe('matchesCondition', () => {
  describe('comparison operators', () => {
    it('evaluates $eq directly', () => {
      expect(matchesCondition(42, { $eq: 42 })).toBe(true)
      expect(matchesCondition(42, { $eq: 43 })).toBe(false)
    })

    it('evaluates $ne directly', () => {
      expect(matchesCondition(42, { $ne: 43 })).toBe(true)
      expect(matchesCondition(42, { $ne: 42 })).toBe(false)
    })

    it('evaluates $gt directly', () => {
      expect(matchesCondition(100, { $gt: 50 })).toBe(true)
      expect(matchesCondition(50, { $gt: 100 })).toBe(false)
    })

    it('evaluates $gte directly', () => {
      expect(matchesCondition(100, { $gte: 100 })).toBe(true)
      expect(matchesCondition(99, { $gte: 100 })).toBe(false)
    })

    it('evaluates $lt directly', () => {
      expect(matchesCondition(50, { $lt: 100 })).toBe(true)
      expect(matchesCondition(100, { $lt: 50 })).toBe(false)
    })

    it('evaluates $lte directly', () => {
      expect(matchesCondition(100, { $lte: 100 })).toBe(true)
      expect(matchesCondition(101, { $lte: 100 })).toBe(false)
    })

    it('evaluates $in directly', () => {
      expect(matchesCondition('active', { $in: ['active', 'pending'] })).toBe(true)
      expect(matchesCondition('deleted', { $in: ['active', 'pending'] })).toBe(false)
    })

    it('evaluates $nin directly', () => {
      expect(matchesCondition('deleted', { $nin: ['active', 'pending'] })).toBe(true)
      expect(matchesCondition('active', { $nin: ['active', 'pending'] })).toBe(false)
    })
  })

  describe('useful for array filtering', () => {
    it('filters array using $lt condition', () => {
      const numbers = [10, 25, 50, 75, 100]
      const filtered = numbers.filter(n => !matchesCondition(n, { $lt: 30 }))
      expect(filtered).toEqual([50, 75, 100])
    })

    it('filters array using $in condition', () => {
      const statuses = ['active', 'pending', 'deleted', 'active', 'archived']
      const filtered = statuses.filter(s => matchesCondition(s, { $in: ['active', 'pending'] }))
      expect(filtered).toEqual(['active', 'pending', 'active'])
    })

    it('filters array using range condition', () => {
      const scores = [10, 30, 50, 70, 90]
      const filtered = scores.filter(s => matchesCondition(s, { $gte: 30, $lte: 70 }))
      expect(filtered).toEqual([30, 50, 70])
    })
  })
})

// =============================================================================
// Type Coercion Edge Cases
// =============================================================================

describe('type coercion edge cases', () => {
  describe('no implicit type coercion', () => {
    it('string "42" does not equal number 42', () => {
      expect(matchesFilter({ value: '42' }, { value: { $eq: 42 } })).toBe(false)
      expect(matchesFilter({ value: 42 }, { value: { $eq: '42' } })).toBe(false)
    })

    it('string "true" does not equal boolean true', () => {
      expect(matchesFilter({ value: 'true' }, { value: { $eq: true } })).toBe(false)
      expect(matchesFilter({ value: true }, { value: { $eq: 'true' } })).toBe(false)
    })

    it('number 1 does not equal boolean true', () => {
      expect(matchesFilter({ value: 1 }, { value: { $eq: true } })).toBe(false)
      expect(matchesFilter({ value: 0 }, { value: { $eq: false } })).toBe(false)
    })

    it('empty string does not equal null', () => {
      expect(matchesFilter({ value: '' }, { value: { $eq: null } })).toBe(false)
    })

    it('zero does not equal null', () => {
      expect(matchesFilter({ value: 0 }, { value: { $eq: null } })).toBe(false)
    })

    it('empty array does not equal null', () => {
      expect(matchesFilter({ value: [] }, { value: { $eq: null } })).toBe(false)
    })

    it('empty object does not equal null', () => {
      expect(matchesFilter({ value: {} }, { value: { $eq: null } })).toBe(false)
    })
  })

  describe('special numeric values', () => {
    it('handles Number.MAX_VALUE', () => {
      expect(matchesFilter(
        { value: Number.MAX_VALUE },
        { value: { $eq: Number.MAX_VALUE } }
      )).toBe(true)
      // Note: Number.MAX_VALUE - 1 === Number.MAX_VALUE in JavaScript
      // due to floating-point precision, so we test against a meaningful smaller value
      expect(matchesFilter(
        { value: Number.MAX_VALUE },
        { value: { $gt: Number.MAX_VALUE / 2 } }
      )).toBe(true)
    })

    it('handles Number.MIN_VALUE', () => {
      expect(matchesFilter(
        { value: Number.MIN_VALUE },
        { value: { $eq: Number.MIN_VALUE } }
      )).toBe(true)
      expect(matchesFilter(
        { value: Number.MIN_VALUE },
        { value: { $gt: 0 } }
      )).toBe(true)
    })

    it('handles Number.EPSILON', () => {
      expect(matchesFilter(
        { value: 0.1 + 0.2 },
        { value: { $eq: 0.3 } }
      )).toBe(false) // Floating point precision issue
    })
  })
})

// =============================================================================
// Real-World Scenarios
// =============================================================================

describe('real-world scenarios', () => {
  describe('user role filtering', () => {
    const users = [
      { id: 1, name: 'Alice', role: 'admin', permissions: { canDelete: true } },
      { id: 2, name: 'Bob', role: 'user', permissions: { canDelete: false } },
      { id: 3, name: 'Charlie', role: 'moderator', permissions: { canDelete: true } },
      { id: 4, name: 'Diana', role: 'user', permissions: { canDelete: false } },
    ]

    it('finds non-admin users', () => {
      const filter: Filter = { role: { $ne: 'admin' } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Bob', 'Charlie', 'Diana'])
    })

    it('finds users with specific roles', () => {
      const filter: Filter = { role: { $in: ['admin', 'moderator'] } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })

    it('finds users without specific roles', () => {
      const filter: Filter = { role: { $nin: ['admin', 'moderator'] } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Bob', 'Diana'])
    })

    it('filters by nested permission', () => {
      const filter: Filter = { 'permissions.canDelete': { $eq: true } }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })
  })

  describe('product inventory filtering', () => {
    const products = [
      { sku: 'LAPTOP-001', price: 999, stock: 50, category: 'electronics' },
      { sku: 'MOUSE-002', price: 29, stock: 200, category: 'electronics' },
      { sku: 'DESK-003', price: 299, stock: 0, category: 'furniture' },
      { sku: 'CHAIR-004', price: 149, stock: 25, category: 'furniture' },
      { sku: 'MONITOR-005', price: 399, stock: 75, category: 'electronics' },
    ]

    it('finds products in price range', () => {
      const filter: Filter = { price: { $gte: 100, $lte: 500 } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['DESK-003', 'CHAIR-004', 'MONITOR-005'])
    })

    it('finds in-stock products', () => {
      const filter: Filter = { stock: { $gt: 0 } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['LAPTOP-001', 'MOUSE-002', 'CHAIR-004', 'MONITOR-005'])
    })

    it('finds out-of-stock products', () => {
      const filter: Filter = { stock: { $eq: 0 } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['DESK-003'])
    })

    it('finds low-stock products', () => {
      const filter: Filter = { stock: { $gt: 0, $lte: 50 } }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.sku)).toEqual(['LAPTOP-001', 'CHAIR-004'])
    })
  })

  describe('date-based event filtering', () => {
    const events = [
      { id: 1, name: 'Conference', date: new Date('2024-03-15'), status: 'completed' },
      { id: 2, name: 'Workshop', date: new Date('2024-06-20'), status: 'upcoming' },
      { id: 3, name: 'Meetup', date: new Date('2024-06-15'), status: 'upcoming' },
      { id: 4, name: 'Webinar', date: new Date('2024-09-01'), status: 'upcoming' },
    ]

    it('finds events after a date', () => {
      const filter: Filter = { date: { $gt: new Date('2024-06-01') } }
      const results = events.filter(e => matchesFilter(e, filter))
      expect(results.map(e => e.name)).toEqual(['Workshop', 'Meetup', 'Webinar'])
    })

    it('finds events in date range', () => {
      const filter: Filter = {
        date: {
          $gte: new Date('2024-06-01'),
          $lte: new Date('2024-06-30')
        }
      }
      const results = events.filter(e => matchesFilter(e, filter))
      expect(results.map(e => e.name)).toEqual(['Workshop', 'Meetup'])
    })

    it('combines date and status filter', () => {
      const filter: Filter = {
        date: { $gt: new Date('2024-06-01') },
        status: { $eq: 'upcoming' }
      }
      const results = events.filter(e => matchesFilter(e, filter))
      expect(results.map(e => e.name)).toEqual(['Workshop', 'Meetup', 'Webinar'])
    })
  })
})

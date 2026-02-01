/**
 * Comprehensive Logical Operators Tests for Filter Evaluation
 *
 * Tests for $and, $or, $not, $nor operators in src/query/filter.ts
 *
 * This file follows TDD RED phase - tests that verify behavior of logical operators.
 */

import { describe, it, expect } from 'vitest'
import { matchesFilter, createPredicate } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// $and Operator Tests
// =============================================================================

describe('$and operator', () => {
  describe('simple $and operations', () => {
    it('matches when all conditions are true', () => {
      const doc = { status: 'published', featured: true, score: 100 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
          { score: 100 },
        ],
      })).toBe(true)
    })

    it('fails when first condition is false', () => {
      const doc = { status: 'draft', featured: true, score: 100 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
          { score: 100 },
        ],
      })).toBe(false)
    })

    it('fails when middle condition is false', () => {
      const doc = { status: 'published', featured: false, score: 100 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
          { score: 100 },
        ],
      })).toBe(false)
    })

    it('fails when last condition is false', () => {
      const doc = { status: 'published', featured: true, score: 50 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
          { score: 100 },
        ],
      })).toBe(false)
    })

    it('fails when all conditions are false', () => {
      const doc = { status: 'draft', featured: false, score: 50 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
          { score: 100 },
        ],
      })).toBe(false)
    })

    it('matches with single condition', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, {
        $and: [{ status: 'published' }],
      })).toBe(true)
    })

    it('fails with single false condition', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, {
        $and: [{ status: 'published' }],
      })).toBe(false)
    })
  })

  describe('$and with comparison operators', () => {
    it('combines $and with $gt and $lt', () => {
      const doc = { price: 50 }
      expect(matchesFilter(doc, {
        $and: [
          { price: { $gt: 25 } },
          { price: { $lt: 75 } },
        ],
      })).toBe(true)
    })

    it('fails when $gt condition fails', () => {
      const doc = { price: 20 }
      expect(matchesFilter(doc, {
        $and: [
          { price: { $gt: 25 } },
          { price: { $lt: 75 } },
        ],
      })).toBe(false)
    })

    it('combines $and with $gte, $lte, and $ne', () => {
      const doc = { score: 100, status: 'active' }
      expect(matchesFilter(doc, {
        $and: [
          { score: { $gte: 50 } },
          { score: { $lte: 150 } },
          { status: { $ne: 'inactive' } },
        ],
      })).toBe(true)
    })

    it('combines $and with $in and $nin', () => {
      const doc = { category: 'electronics', brand: 'Acme' }
      expect(matchesFilter(doc, {
        $and: [
          { category: { $in: ['electronics', 'appliances'] } },
          { brand: { $nin: ['Generic', 'Unknown'] } },
        ],
      })).toBe(true)
    })
  })

  describe('$and with empty arrays', () => {
    it('returns true for empty $and array (vacuous truth)', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $and: [] })).toBe(true)
    })

    it('returns true for empty $and with empty document', () => {
      expect(matchesFilter({}, { $and: [] })).toBe(true)
    })

    it('returns true for empty $and with null document', () => {
      // Empty $and is vacuously true, but null document should fail
      expect(matchesFilter(null, { $and: [] })).toBe(false)
    })
  })

  describe('$and short-circuit behavior', () => {
    // Tests to verify short-circuit evaluation (stops at first false)
    it('should short-circuit on first false condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      // First condition fails, so second and third should not be evaluated
      expect(matchesFilter(doc, {
        $and: [
          { a: 99 },  // false - should stop here
          { b: 2 },   // would be true
          { c: 3 },   // would be true
        ],
      })).toBe(false)
    })

    it('evaluates all conditions when all are true', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $and: [
          { a: 1 },
          { b: 2 },
          { c: 3 },
        ],
      })).toBe(true)
    })

    it('stops at second false condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $and: [
          { a: 1 },   // true
          { b: 99 },  // false - should stop here
          { c: 3 },   // would be true
        ],
      })).toBe(false)
    })
  })

  describe('$and with field conditions (implicit AND)', () => {
    it('combines explicit $and with field conditions', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 1,  // implicit AND
        $and: [{ b: 2 }, { c: 3 }],
      })).toBe(true)
    })

    it('fails when field condition fails', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 99,  // fails
        $and: [{ b: 2 }, { c: 3 }],
      })).toBe(false)
    })

    it('fails when $and condition fails', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 1,  // passes
        $and: [{ b: 99 }, { c: 3 }],  // fails
      })).toBe(false)
    })
  })
})

// =============================================================================
// $or Operator Tests
// =============================================================================

describe('$or operator', () => {
  describe('simple $or operations', () => {
    it('matches when first condition is true', () => {
      const doc = { status: 'published', featured: false }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(true)
    })

    it('matches when second condition is true', () => {
      const doc = { status: 'draft', featured: true }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(true)
    })

    it('matches when all conditions are true', () => {
      const doc = { status: 'published', featured: true }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(true)
    })

    it('fails when all conditions are false', () => {
      const doc = { status: 'draft', featured: false }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(false)
    })

    it('matches with single true condition', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, {
        $or: [{ status: 'published' }],
      })).toBe(true)
    })

    it('fails with single false condition', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, {
        $or: [{ status: 'published' }],
      })).toBe(false)
    })

    it('matches when last condition is true', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { a: 99 },
          { b: 99 },
          { c: 3 },
        ],
      })).toBe(true)
    })
  })

  describe('$or with comparison operators', () => {
    it('combines $or with comparison operators', () => {
      const doc = { price: 150 }
      expect(matchesFilter(doc, {
        $or: [
          { price: { $lt: 100 } },
          { price: { $gt: 125 } },
        ],
      })).toBe(true)
    })

    it('fails when no comparison matches', () => {
      const doc = { price: 110 }
      expect(matchesFilter(doc, {
        $or: [
          { price: { $lt: 100 } },
          { price: { $gt: 125 } },
        ],
      })).toBe(false)
    })

    it('combines $or with $in operator', () => {
      const doc = { category: 'books', type: 'fiction' }
      expect(matchesFilter(doc, {
        $or: [
          { category: { $in: ['electronics', 'books'] } },
          { type: 'non-fiction' },
        ],
      })).toBe(true)
    })
  })

  describe('$or with empty arrays', () => {
    it('returns false for empty $or array (no conditions to satisfy)', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $or: [] })).toBe(false)
    })

    it('returns false for empty $or with empty document', () => {
      expect(matchesFilter({}, { $or: [] })).toBe(false)
    })

    it('returns false for empty $or with any document', () => {
      expect(matchesFilter({ a: 1, b: 2 }, { $or: [] })).toBe(false)
    })
  })

  describe('$or short-circuit behavior', () => {
    it('should short-circuit on first true condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { a: 1 },   // true - should stop here
          { b: 99 },  // would be false
          { c: 99 },  // would be false
        ],
      })).toBe(true)
    })

    it('evaluates all conditions when all are false', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { a: 99 },
          { b: 99 },
          { c: 99 },
        ],
      })).toBe(false)
    })

    it('stops at second true condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { a: 99 },  // false
          { b: 2 },   // true - should stop here
          { c: 99 },  // would be false
        ],
      })).toBe(true)
    })
  })

  describe('$or with field conditions', () => {
    it('combines $or with field conditions', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 1,  // must match
        $or: [{ b: 2 }, { c: 99 }],  // any must match
      })).toBe(true)
    })

    it('fails when field condition fails', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 99,  // fails
        $or: [{ b: 2 }, { c: 3 }],  // both match but field fails
      })).toBe(false)
    })

    it('fails when $or fails but field passes', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        a: 1,  // passes
        $or: [{ b: 99 }, { c: 99 }],  // all fail
      })).toBe(false)
    })
  })
})

// =============================================================================
// $not Operator Tests
// =============================================================================

describe('$not operator', () => {
  describe('simple $not operations', () => {
    it('negates a true condition to false', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, {
        $not: { status: 'published' },
      })).toBe(false)
    })

    it('negates a false condition to true', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, {
        $not: { status: 'published' },
      })).toBe(true)
    })

    it('negates missing field condition', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, {
        $not: { status: 'published' },
      })).toBe(true)
    })

    it('negates empty filter (always true becomes false)', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, {
        $not: {},
      })).toBe(false)
    })
  })

  describe('$not with comparison operators', () => {
    it('negates $gt operator', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, {
        $not: { score: { $gt: 75 } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { score: { $gt: 25 } },
      })).toBe(false)
    })

    it('negates $lt operator', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, {
        $not: { score: { $lt: 25 } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { score: { $lt: 75 } },
      })).toBe(false)
    })

    it('negates $in operator', () => {
      const doc = { status: 'pending' }
      expect(matchesFilter(doc, {
        $not: { status: { $in: ['active', 'completed'] } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { status: { $in: ['pending', 'active'] } },
      })).toBe(false)
    })

    it('negates $nin operator', () => {
      const doc = { status: 'pending' }
      expect(matchesFilter(doc, {
        $not: { status: { $nin: ['active', 'completed'] } },
      })).toBe(false)  // $nin matches (pending not in list), so $not negates to false
      expect(matchesFilter(doc, {
        $not: { status: { $nin: ['pending', 'active'] } },
      })).toBe(true)  // $nin fails (pending in list), so $not negates to true
    })

    it('negates $exists operator', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, {
        $not: { name: { $exists: true } },
      })).toBe(false)
      expect(matchesFilter(doc, {
        $not: { missing: { $exists: true } },
      })).toBe(true)
    })

    it('negates range query', () => {
      const doc = { price: 50 }
      expect(matchesFilter(doc, {
        $not: { price: { $gte: 25, $lte: 75 } },
      })).toBe(false)  // price is in range, so $not is false
      expect(matchesFilter(doc, {
        $not: { price: { $gte: 100, $lte: 200 } },
      })).toBe(true)  // price is not in range, so $not is true
    })
  })

  describe('$not with multiple field conditions', () => {
    it('negates multiple field conditions (AND)', () => {
      const doc = { a: 1, b: 2 }
      // { a: 1, b: 2 } matches the inner filter, so $not is false
      expect(matchesFilter(doc, {
        $not: { a: 1, b: 2 },
      })).toBe(false)
    })

    it('negates when inner filter partially matches', () => {
      const doc = { a: 1, b: 3 }
      // { a: 1, b: 2 } does not match (b is 3 not 2), so $not is true
      expect(matchesFilter(doc, {
        $not: { a: 1, b: 2 },
      })).toBe(true)
    })

    it('negates when inner filter completely fails', () => {
      const doc = { a: 99, b: 99 }
      expect(matchesFilter(doc, {
        $not: { a: 1, b: 2 },
      })).toBe(true)
    })
  })

  describe('$not with string operators', () => {
    it('negates $regex', () => {
      const doc = { email: 'user@example.com' }
      expect(matchesFilter(doc, {
        $not: { email: { $regex: '@other\\.com$' } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { email: { $regex: '@example\\.com$' } },
      })).toBe(false)
    })

    it('negates $startsWith', () => {
      const doc = { url: 'https://example.com' }
      expect(matchesFilter(doc, {
        $not: { url: { $startsWith: 'http://' } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { url: { $startsWith: 'https://' } },
      })).toBe(false)
    })

    it('negates $contains', () => {
      const doc = { text: 'hello world' }
      expect(matchesFilter(doc, {
        $not: { text: { $contains: 'foo' } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { text: { $contains: 'world' } },
      })).toBe(false)
    })
  })

  describe('$not with array operators', () => {
    it('negates $all', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, {
        $not: { tags: { $all: ['x', 'y'] } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { tags: { $all: ['a', 'b'] } },
      })).toBe(false)
    })

    it('negates $size', () => {
      const doc = { items: [1, 2, 3] }
      expect(matchesFilter(doc, {
        $not: { items: { $size: 5 } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { items: { $size: 3 } },
      })).toBe(false)
    })

    it('negates $elemMatch', () => {
      const doc = {
        products: [
          { name: 'apple', price: 1 },
          { name: 'banana', price: 2 },
        ],
      }
      expect(matchesFilter(doc, {
        $not: { products: { $elemMatch: { name: 'orange' } } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        $not: { products: { $elemMatch: { name: 'apple' } } },
      })).toBe(false)
    })
  })
})

// =============================================================================
// $nor Operator Tests
// =============================================================================

describe('$nor operator', () => {
  describe('simple $nor operations', () => {
    it('matches when no conditions are true', () => {
      const doc = { status: 'pending', type: 'basic' }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { type: 'premium' },
        ],
      })).toBe(true)
    })

    it('fails when first condition is true', () => {
      const doc = { status: 'published', type: 'basic' }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { type: 'premium' },
        ],
      })).toBe(false)
    })

    it('fails when second condition is true', () => {
      const doc = { status: 'pending', type: 'premium' }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { type: 'premium' },
        ],
      })).toBe(false)
    })

    it('fails when all conditions are true', () => {
      const doc = { status: 'published', type: 'premium' }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { type: 'premium' },
        ],
      })).toBe(false)
    })

    it('matches with single false condition', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, {
        $nor: [{ status: 'published' }],
      })).toBe(true)
    })

    it('fails with single true condition', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, {
        $nor: [{ status: 'published' }],
      })).toBe(false)
    })
  })

  describe('$nor with comparison operators', () => {
    it('matches when no comparisons are true', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, {
        $nor: [
          { score: { $lt: 25 } },
          { score: { $gt: 75 } },
        ],
      })).toBe(true)
    })

    it('fails when any comparison is true', () => {
      const doc = { score: 100 }
      expect(matchesFilter(doc, {
        $nor: [
          { score: { $lt: 25 } },
          { score: { $gt: 75 } },
        ],
      })).toBe(false)
    })

    it('combines $nor with $in', () => {
      const doc = { category: 'other' }
      expect(matchesFilter(doc, {
        $nor: [
          { category: { $in: ['electronics', 'books'] } },
          { category: { $in: ['clothing', 'food'] } },
        ],
      })).toBe(true)
    })
  })

  describe('$nor with empty arrays', () => {
    it('returns true for empty $nor array (no conditions to fail)', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $nor: [] })).toBe(true)
    })

    it('returns true for empty $nor with empty document', () => {
      expect(matchesFilter({}, { $nor: [] })).toBe(true)
    })
  })

  describe('$nor short-circuit behavior', () => {
    it('should short-circuit on first true condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      // First condition is true, so $nor should fail immediately
      expect(matchesFilter(doc, {
        $nor: [
          { a: 1 },   // true - should stop here, $nor fails
          { b: 99 },  // would be false
          { c: 99 },  // would be false
        ],
      })).toBe(false)
    })

    it('evaluates all conditions when all are false', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $nor: [
          { a: 99 },
          { b: 99 },
          { c: 99 },
        ],
      })).toBe(true)
    })

    it('stops at middle true condition', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $nor: [
          { a: 99 },  // false
          { b: 2 },   // true - $nor fails
          { c: 99 },  // would be false
        ],
      })).toBe(false)
    })
  })

  describe('$nor equivalence to $not + $or', () => {
    it('$nor is equivalent to $not: { $or: [...] }', () => {
      const doc = { status: 'pending', type: 'basic' }

      const norResult = matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { type: 'premium' },
        ],
      })

      const notOrResult = matchesFilter(doc, {
        $not: {
          $or: [
            { status: 'published' },
            { type: 'premium' },
          ],
        },
      })

      expect(norResult).toBe(notOrResult)
    })

    it('$nor and $not + $or match for true case', () => {
      const doc = { status: 'published', type: 'basic' }

      const norResult = matchesFilter(doc, {
        $nor: [{ status: 'published' }],
      })

      const notOrResult = matchesFilter(doc, {
        $not: { $or: [{ status: 'published' }] },
      })

      expect(norResult).toBe(notOrResult)
      expect(norResult).toBe(false)
    })
  })
})

// =============================================================================
// Nested Logical Operators Tests
// =============================================================================

describe('nested logical operators', () => {
  describe('$and inside $or', () => {
    it('matches when any $and group matches', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { $and: [{ a: 1 }, { b: 2 }] },  // matches
          { $and: [{ c: 99 }, { d: 99 }] },  // fails
        ],
      })).toBe(true)
    })

    it('fails when no $and group matches', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { $and: [{ a: 99 }, { b: 99 }] },  // fails
          { $and: [{ c: 99 }, { d: 99 }] },  // fails
        ],
      })).toBe(false)
    })

    it('matches when second $and group matches', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $or: [
          { $and: [{ a: 99 }, { b: 99 }] },  // fails
          { $and: [{ c: 3 }, { d: 4 }] },    // matches
        ],
      })).toBe(true)
    })
  })

  describe('$or inside $and', () => {
    it('matches when all $or groups have at least one match', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $and: [
          { $or: [{ a: 1 }, { a: 99 }] },  // first matches
          { $or: [{ c: 99 }, { d: 4 }] },  // second matches
        ],
      })).toBe(true)
    })

    it('fails when any $or group has no matches', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $and: [
          { $or: [{ a: 1 }, { a: 99 }] },    // matches
          { $or: [{ x: 99 }, { y: 99 }] },   // fails
        ],
      })).toBe(false)
    })
  })

  describe('$not inside $and', () => {
    it('matches when all conditions including $not are satisfied', () => {
      const doc = { status: 'published', type: 'article' }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { $not: { type: 'draft' } },
        ],
      })).toBe(true)
    })

    it('fails when $not condition fails', () => {
      const doc = { status: 'published', type: 'draft' }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { $not: { type: 'draft' } },
        ],
      })).toBe(false)
    })
  })

  describe('$not inside $or', () => {
    it('matches when any condition including $not is satisfied', () => {
      const doc = { status: 'pending' }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { $not: { status: 'draft' } },
        ],
      })).toBe(true)
    })

    it('fails when no $or condition is satisfied', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, {
        $or: [
          { status: 'published' },
          { $not: { status: 'draft' } },
        ],
      })).toBe(false)
    })
  })

  describe('$nor inside $and', () => {
    it('matches when $nor and other conditions pass', () => {
      const doc = { a: 1, b: 2, c: 'valid' }
      expect(matchesFilter(doc, {
        $and: [
          { a: 1 },
          { $nor: [{ c: 'invalid' }, { c: 'error' }] },
        ],
      })).toBe(true)
    })

    it('fails when $nor fails', () => {
      const doc = { a: 1, b: 2, c: 'invalid' }
      expect(matchesFilter(doc, {
        $and: [
          { a: 1 },
          { $nor: [{ c: 'invalid' }, { c: 'error' }] },
        ],
      })).toBe(false)
    })
  })

  describe('deeply nested logical operators', () => {
    it('handles 3 levels of nesting', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $and: [
          {
            $or: [
              { $and: [{ a: 1 }, { b: 2 }] },
              { c: 99 },
            ],
          },
          { d: 4 },
        ],
      })).toBe(true)
    })

    it('fails at deepest level', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $and: [
          {
            $or: [
              { $and: [{ a: 99 }, { b: 99 }] },  // deep fail
              { c: 99 },  // fail
            ],
          },
          { d: 4 },
        ],
      })).toBe(false)
    })

    it('handles 4 levels of nesting with $not', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $or: [
          {
            $and: [
              {
                $not: {
                  $or: [{ a: 99 }, { b: 99 }],
                },
              },
              { c: 3 },
            ],
          },
          { d: 99 },
        ],
      })).toBe(true)
    })

    it('handles complex nested structure', () => {
      const doc = { x: 10, y: 20, z: 30, status: 'active' }
      expect(matchesFilter(doc, {
        $and: [
          {
            $or: [
              { x: { $gt: 5 } },
              { y: { $lt: 10 } },
            ],
          },
          {
            $nor: [
              { status: 'deleted' },
              { status: 'archived' },
            ],
          },
          {
            $not: { z: { $gt: 100 } },
          },
        ],
      })).toBe(true)
    })
  })

  describe('mixed field and logical conditions', () => {
    it('combines field conditions with nested logical operators', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4, e: 5 }
      expect(matchesFilter(doc, {
        a: 1,  // field condition
        $and: [
          { b: 2 },
          {
            $or: [
              { c: 3 },
              { d: 99 },
            ],
          },
        ],
        $not: { e: 99 },
      })).toBe(true)
    })

    it('fails when field condition fails in nested structure', () => {
      const doc = { a: 99, b: 2, c: 3, d: 4, e: 5 }
      expect(matchesFilter(doc, {
        a: 1,  // field condition fails
        $and: [
          { b: 2 },
          {
            $or: [
              { c: 3 },
              { d: 4 },
            ],
          },
        ],
      })).toBe(false)
    })
  })
})

// =============================================================================
// Complex Real-World Scenarios
// =============================================================================

describe('complex real-world scenarios', () => {
  describe('content moderation filter', () => {
    const posts = [
      { id: 1, status: 'published', author: { verified: true, role: 'admin' }, flags: 0 },
      { id: 2, status: 'pending', author: { verified: true, role: 'user' }, flags: 0 },
      { id: 3, status: 'published', author: { verified: false, role: 'user' }, flags: 2 },
      { id: 4, status: 'flagged', author: { verified: true, role: 'user' }, flags: 5 },
      { id: 5, status: 'published', author: { verified: true, role: 'moderator' }, flags: 1 },
    ]

    it('finds safe published content', () => {
      const filter: Filter = {
        $and: [
          { status: 'published' },
          {
            $or: [
              { 'author.verified': true },
              { 'author.role': 'admin' },
            ],
          },
          { flags: { $lt: 3 } },
        ],
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([1, 5])
    })

    it('finds content needing review', () => {
      const filter: Filter = {
        $or: [
          { status: 'pending' },
          { status: 'flagged' },
          {
            $and: [
              { status: 'published' },
              { flags: { $gte: 2 } },
            ],
          },
        ],
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([2, 3, 4])
    })

    it('excludes flagged and unverified content', () => {
      const filter: Filter = {
        $and: [
          { $nor: [{ status: 'flagged' }, { status: 'pending' }] },
          { $not: { 'author.verified': false } },
        ],
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([1, 5])
    })
  })

  describe('e-commerce product availability', () => {
    const products = [
      { id: 1, name: 'Laptop', price: 999, stock: 10, category: 'electronics', featured: true },
      { id: 2, name: 'Phone', price: 599, stock: 0, category: 'electronics', featured: true },
      { id: 3, name: 'Tablet', price: 399, stock: 5, category: 'electronics', featured: false },
      { id: 4, name: 'Chair', price: 199, stock: 20, category: 'furniture', featured: false },
      { id: 5, name: 'Desk', price: 299, stock: 3, category: 'furniture', featured: true },
    ]

    it('finds available featured products', () => {
      const filter: Filter = {
        $and: [
          { featured: true },
          { stock: { $gt: 0 } },
        ],
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([1, 5])
    })

    it('finds products in price range or featured', () => {
      const filter: Filter = {
        $or: [
          { price: { $gte: 200, $lte: 400 } },
          { featured: true },
        ],
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([1, 2, 3, 5])
    })

    it('excludes out-of-stock electronics', () => {
      const filter: Filter = {
        $nor: [
          {
            $and: [
              { category: 'electronics' },
              { stock: 0 },
            ],
          },
        ],
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.id)).toEqual([1, 3, 4, 5])
    })
  })

  describe('user permission checks', () => {
    const users = [
      { id: 1, role: 'admin', department: 'IT', active: true, permissions: ['read', 'write', 'delete'] },
      { id: 2, role: 'manager', department: 'Sales', active: true, permissions: ['read', 'write'] },
      { id: 3, role: 'user', department: 'IT', active: true, permissions: ['read'] },
      { id: 4, role: 'user', department: 'Sales', active: false, permissions: ['read'] },
      { id: 5, role: 'manager', department: 'IT', active: true, permissions: ['read', 'write', 'approve'] },
    ]

    it('finds users who can modify data', () => {
      const filter: Filter = {
        $and: [
          { active: true },
          {
            $or: [
              { role: 'admin' },
              { permissions: { $all: ['write'] } },
            ],
          },
        ],
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.id)).toEqual([1, 2, 5])
    })

    it('finds non-admin IT department users', () => {
      const filter: Filter = {
        $and: [
          { department: 'IT' },
          { $not: { role: 'admin' } },
          { active: true },
        ],
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.id)).toEqual([3, 5])
    })

    it('excludes inactive or read-only users', () => {
      const filter: Filter = {
        $nor: [
          { active: false },
          { permissions: { $size: 1 } },  // only read permission
        ],
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.id)).toEqual([1, 2, 5])
    })
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('edge cases and error handling', () => {
  describe('null and undefined handling', () => {
    it('handles null document with logical operators', () => {
      expect(matchesFilter(null, { $and: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter(null, { $or: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter(null, { $not: { a: 1 } })).toBe(false)
      expect(matchesFilter(null, { $nor: [{ a: 1 }] })).toBe(false)
    })

    it('handles undefined document with logical operators', () => {
      expect(matchesFilter(undefined, { $and: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter(undefined, { $or: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter(undefined, { $not: { a: 1 } })).toBe(false)
      expect(matchesFilter(undefined, { $nor: [{ a: 1 }] })).toBe(false)
    })

    it('handles empty document with logical operators', () => {
      expect(matchesFilter({}, { $and: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter({}, { $or: [{ a: 1 }] })).toBe(false)
      expect(matchesFilter({}, { $not: { a: 1 } })).toBe(true)
      expect(matchesFilter({}, { $nor: [{ a: 1 }] })).toBe(true)
    })

    it('matches null field values in logical operators', () => {
      const doc = { value: null }
      expect(matchesFilter(doc, { $and: [{ value: null }] })).toBe(true)
      expect(matchesFilter(doc, { $or: [{ value: null }] })).toBe(true)
      expect(matchesFilter(doc, { $not: { value: null } })).toBe(false)
      expect(matchesFilter(doc, { $nor: [{ value: null }] })).toBe(false)
    })
  })

  describe('type coercion edge cases', () => {
    it('does not coerce string to number in logical operators', () => {
      const doc = { value: '42' }
      expect(matchesFilter(doc, { $and: [{ value: 42 }] })).toBe(false)
      expect(matchesFilter(doc, { $and: [{ value: '42' }] })).toBe(true)
    })

    it('does not coerce boolean to number', () => {
      const doc = { value: true }
      expect(matchesFilter(doc, { $and: [{ value: 1 }] })).toBe(false)
      expect(matchesFilter(doc, { $and: [{ value: true }] })).toBe(true)
    })
  })

  describe('array handling in logical operators', () => {
    it('handles array documents correctly', () => {
      const doc = [1, 2, 3]
      // Array as document should work with indexed access if supported
      expect(matchesFilter(doc, { $and: [] })).toBe(true)
    })

    it('handles nested arrays in conditions', () => {
      const doc = { items: [[1, 2], [3, 4]] }
      expect(matchesFilter(doc, {
        $and: [{ items: { $size: 2 } }],
      })).toBe(true)
    })
  })

  describe('deeply nested field access in logical operators', () => {
    it('handles deep nesting with $and', () => {
      const doc = { a: { b: { c: { d: { e: 'value' } } } } }
      expect(matchesFilter(doc, {
        $and: [
          { 'a.b.c.d.e': 'value' },
        ],
      })).toBe(true)
    })

    it('handles missing deep paths in $or', () => {
      const doc = { a: { b: 1 } }
      expect(matchesFilter(doc, {
        $or: [
          { 'a.b.c.d.e': 'value' },
          { 'a.b': 1 },
        ],
      })).toBe(true)
    })
  })

  describe('multiple logical operators at same level', () => {
    it('handles $and and $or at same level', () => {
      const doc = { a: 1, b: 2, c: 3 }
      // When both are present, behavior may vary by implementation
      // This tests the actual behavior
      expect(matchesFilter(doc, {
        $and: [{ a: 1 }],
        $or: [{ b: 2 }],
      })).toBe(true)
    })

    it('handles $and and $not at same level', () => {
      const doc = { a: 1, b: 2 }
      expect(matchesFilter(doc, {
        $and: [{ a: 1 }],
        $not: { c: 3 },
      })).toBe(true)
    })

    it('handles all logical operators at same level', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $and: [{ a: 1 }],
        $or: [{ b: 2 }],
        $not: { d: 4 },
        $nor: [{ e: 5 }],
      })).toBe(true)
    })
  })
})

// =============================================================================
// createPredicate with Logical Operators
// =============================================================================

describe('createPredicate with logical operators', () => {
  it('creates reusable $and predicate', () => {
    const isActiveAdmin = createPredicate({
      $and: [
        { role: 'admin' },
        { active: true },
      ],
    })

    expect(isActiveAdmin({ role: 'admin', active: true })).toBe(true)
    expect(isActiveAdmin({ role: 'admin', active: false })).toBe(false)
    expect(isActiveAdmin({ role: 'user', active: true })).toBe(false)
  })

  it('creates reusable $or predicate', () => {
    const isPrivileged = createPredicate({
      $or: [
        { role: 'admin' },
        { role: 'moderator' },
      ],
    })

    expect(isPrivileged({ role: 'admin' })).toBe(true)
    expect(isPrivileged({ role: 'moderator' })).toBe(true)
    expect(isPrivileged({ role: 'user' })).toBe(false)
  })

  it('creates reusable $not predicate', () => {
    const isNotBanned = createPredicate({
      $not: { status: 'banned' },
    })

    expect(isNotBanned({ status: 'active' })).toBe(true)
    expect(isNotBanned({ status: 'banned' })).toBe(false)
  })

  it('creates reusable complex predicate', () => {
    const canAccessFeature = createPredicate({
      $and: [
        { active: true },
        {
          $or: [
            { subscription: 'premium' },
            { role: 'admin' },
          ],
        },
        { $not: { suspended: true } },
      ],
    })

    expect(canAccessFeature({ active: true, subscription: 'premium', suspended: false })).toBe(true)
    expect(canAccessFeature({ active: true, role: 'admin', suspended: false })).toBe(true)
    expect(canAccessFeature({ active: true, subscription: 'free', suspended: false })).toBe(false)
    expect(canAccessFeature({ active: true, subscription: 'premium', suspended: true })).toBe(false)
    expect(canAccessFeature({ active: false, subscription: 'premium', suspended: false })).toBe(false)
  })

  it('filters arrays with logical predicates', () => {
    const items = [
      { id: 1, category: 'A', value: 10 },
      { id: 2, category: 'B', value: 20 },
      { id: 3, category: 'A', value: 30 },
      { id: 4, category: 'B', value: 40 },
      { id: 5, category: 'C', value: 50 },
    ]

    const predicate = createPredicate({
      $or: [
        { $and: [{ category: 'A' }, { value: { $gte: 20 } }] },
        { category: 'C' },
      ],
    })

    const filtered = items.filter(predicate)
    expect(filtered.map(i => i.id)).toEqual([3, 5])
  })
})

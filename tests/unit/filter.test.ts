/**
 * Filter Evaluation Engine Tests
 *
 * Comprehensive tests for the MongoDB-style filter evaluation engine.
 * Tests cover all operator types: comparison, logical, string, array, and existence.
 */

import { describe, it, expect } from 'vitest'
import {
  matchesFilter,
  createPredicate,
  matchesCondition,
  deepEqual,
  compareValues,
  getValueType,
} from '../../src/query/filter'
import type { Filter } from '../../src/types/filter'

// =============================================================================
// Basic Matching
// =============================================================================

describe('matchesFilter', () => {
  describe('empty filter', () => {
    it('returns true for empty filter', () => {
      expect(matchesFilter({ name: 'test' }, {})).toBe(true)
    })

    it('returns true for undefined filter', () => {
      expect(matchesFilter({ name: 'test' }, undefined as unknown as Filter)).toBe(true)
    })

    it('returns true for null filter', () => {
      expect(matchesFilter({ name: 'test' }, null as unknown as Filter)).toBe(true)
    })
  })

  describe('direct equality', () => {
    it('matches string fields', () => {
      const doc = { status: 'published', title: 'Hello' }
      expect(matchesFilter(doc, { status: 'published' })).toBe(true)
      expect(matchesFilter(doc, { status: 'draft' })).toBe(false)
    })

    it('matches number fields', () => {
      const doc = { score: 100, count: 5 }
      expect(matchesFilter(doc, { score: 100 })).toBe(true)
      expect(matchesFilter(doc, { score: 50 })).toBe(false)
    })

    it('matches boolean fields', () => {
      const doc = { featured: true, archived: false }
      expect(matchesFilter(doc, { featured: true })).toBe(true)
      expect(matchesFilter(doc, { featured: false })).toBe(false)
      expect(matchesFilter(doc, { archived: false })).toBe(true)
    })

    it('matches null fields', () => {
      const doc = { value: null, other: 'test' }
      expect(matchesFilter(doc, { value: null })).toBe(true)
      expect(matchesFilter(doc, { other: null })).toBe(false)
    })

    it('matches multiple fields', () => {
      const doc = { status: 'published', featured: true, score: 100 }
      expect(matchesFilter(doc, { status: 'published', featured: true })).toBe(true)
      expect(matchesFilter(doc, { status: 'published', featured: false })).toBe(false)
    })

    it('matches array fields with direct equality', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: ['a', 'b', 'c'] })).toBe(true)
      expect(matchesFilter(doc, { tags: ['a', 'b'] })).toBe(false)
      expect(matchesFilter(doc, { tags: ['c', 'b', 'a'] })).toBe(false) // order matters
    })

    it('matches object fields with direct equality', () => {
      const doc = { meta: { key: 'value', count: 5 } }
      expect(matchesFilter(doc, { meta: { key: 'value', count: 5 } })).toBe(true)
      expect(matchesFilter(doc, { meta: { key: 'value' } })).toBe(false)
    })

    it('matches Date fields', () => {
      const date = new Date('2024-01-15T10:00:00Z')
      const doc = { createdAt: date }
      expect(matchesFilter(doc, { createdAt: date })).toBe(true)
      expect(matchesFilter(doc, { createdAt: new Date('2024-01-15T10:00:00Z') })).toBe(true)
      expect(matchesFilter(doc, { createdAt: new Date('2024-01-16T10:00:00Z') })).toBe(false)
    })
  })

  describe('nested field access (dot notation)', () => {
    it('accesses nested fields', () => {
      const doc = { user: { name: 'John', profile: { age: 30 } } }
      expect(matchesFilter(doc, { 'user.name': 'John' })).toBe(true)
      expect(matchesFilter(doc, { 'user.profile.age': 30 })).toBe(true)
      expect(matchesFilter(doc, { 'user.name': 'Jane' })).toBe(false)
    })

    it('returns undefined for missing nested paths', () => {
      const doc = { user: { name: 'John' } }
      expect(matchesFilter(doc, { 'user.profile.age': undefined })).toBe(true)
      expect(matchesFilter(doc, { 'user.profile.age': 30 })).toBe(false)
    })

    it('handles null in nested path', () => {
      const doc = { user: null }
      expect(matchesFilter(doc, { 'user.name': undefined })).toBe(true)
      expect(matchesFilter(doc, { 'user.name': 'John' })).toBe(false)
    })
  })
})

// =============================================================================
// Comparison Operators
// =============================================================================

describe('comparison operators', () => {
  describe('$eq', () => {
    it('matches equal values', () => {
      const doc = { score: 100, name: 'test' }
      expect(matchesFilter(doc, { score: { $eq: 100 } })).toBe(true)
      expect(matchesFilter(doc, { name: { $eq: 'test' } })).toBe(true)
      expect(matchesFilter(doc, { score: { $eq: 50 } })).toBe(false)
    })

    it('matches null with $eq', () => {
      const doc = { value: null }
      expect(matchesFilter(doc, { value: { $eq: null } })).toBe(true)
    })
  })

  describe('$ne', () => {
    it('matches non-equal values', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, { status: { $ne: 'draft' } })).toBe(true)
      expect(matchesFilter(doc, { status: { $ne: 'published' } })).toBe(false)
    })

    it('matches when field is missing (undefined != value)', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { missing: { $ne: 'value' } })).toBe(true)
    })
  })

  describe('$gt', () => {
    it('matches greater than for numbers', () => {
      const doc = { score: 100 }
      expect(matchesFilter(doc, { score: { $gt: 50 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $gt: 100 } })).toBe(false)
      expect(matchesFilter(doc, { score: { $gt: 150 } })).toBe(false)
    })

    it('matches greater than for strings', () => {
      const doc = { name: 'charlie' }
      expect(matchesFilter(doc, { name: { $gt: 'bob' } })).toBe(true)
      expect(matchesFilter(doc, { name: { $gt: 'charlie' } })).toBe(false)
      expect(matchesFilter(doc, { name: { $gt: 'david' } })).toBe(false)
    })

    it('matches greater than for dates', () => {
      const doc = { date: new Date('2024-06-15') }
      expect(matchesFilter(doc, { date: { $gt: new Date('2024-01-01') } })).toBe(true)
      expect(matchesFilter(doc, { date: { $gt: new Date('2024-06-15') } })).toBe(false)
      expect(matchesFilter(doc, { date: { $gt: new Date('2024-12-31') } })).toBe(false)
    })

    it('returns false for null/undefined values', () => {
      expect(matchesFilter({ score: null }, { score: { $gt: 0 } })).toBe(false)
      expect(matchesFilter({ score: undefined }, { score: { $gt: 0 } })).toBe(false)
      expect(matchesFilter({}, { score: { $gt: 0 } })).toBe(false)
    })
  })

  describe('$gte', () => {
    it('matches greater than or equal', () => {
      const doc = { score: 100 }
      expect(matchesFilter(doc, { score: { $gte: 50 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $gte: 100 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $gte: 150 } })).toBe(false)
    })
  })

  describe('$lt', () => {
    it('matches less than for numbers', () => {
      const doc = { score: 100 }
      expect(matchesFilter(doc, { score: { $lt: 150 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $lt: 100 } })).toBe(false)
      expect(matchesFilter(doc, { score: { $lt: 50 } })).toBe(false)
    })

    it('returns false for null/undefined values', () => {
      expect(matchesFilter({ score: null }, { score: { $lt: 100 } })).toBe(false)
      expect(matchesFilter({}, { score: { $lt: 100 } })).toBe(false)
    })
  })

  describe('$lte', () => {
    it('matches less than or equal', () => {
      const doc = { score: 100 }
      expect(matchesFilter(doc, { score: { $lte: 150 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $lte: 100 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $lte: 50 } })).toBe(false)
    })
  })

  describe('$in', () => {
    it('matches if value is in array', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, { status: { $in: ['published', 'featured'] } })).toBe(true)
      expect(matchesFilter(doc, { status: { $in: ['draft', 'pending'] } })).toBe(false)
    })

    it('matches numbers in array', () => {
      const doc = { code: 42 }
      expect(matchesFilter(doc, { code: { $in: [1, 2, 42, 100] } })).toBe(true)
      expect(matchesFilter(doc, { code: { $in: [1, 2, 3] } })).toBe(false)
    })

    it('matches null in array', () => {
      const doc = { value: null }
      expect(matchesFilter(doc, { value: { $in: [null, 'other'] } })).toBe(true)
    })
  })

  describe('$nin', () => {
    it('matches if value is not in array', () => {
      const doc = { status: 'published' }
      expect(matchesFilter(doc, { status: { $nin: ['draft', 'pending'] } })).toBe(true)
      expect(matchesFilter(doc, { status: { $nin: ['published', 'featured'] } })).toBe(false)
    })
  })

  describe('combined comparison operators', () => {
    it('combines $gt and $lt (range query)', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { score: { $gt: 25, $lt: 75 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $gt: 0, $lt: 25 } })).toBe(false)
      expect(matchesFilter(doc, { score: { $gt: 75, $lt: 100 } })).toBe(false)
    })

    it('combines $gte and $lte (inclusive range)', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { score: { $gte: 50, $lte: 50 } })).toBe(true)
      expect(matchesFilter(doc, { score: { $gte: 25, $lte: 75 } })).toBe(true)
    })
  })
})

// =============================================================================
// Logical Operators
// =============================================================================

describe('logical operators', () => {
  describe('$and', () => {
    it('matches when all conditions are true', () => {
      const doc = { status: 'published', featured: true, score: 100 }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(true)
    })

    it('fails when any condition is false', () => {
      const doc = { status: 'published', featured: false }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(false)
    })

    it('handles empty $and array', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $and: [] })).toBe(true)
    })

    it('handles nested $and', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $and: [
          { a: 1 },
          { $and: [{ b: 2 }, { c: 3 }] },
        ],
      })).toBe(true)
    })
  })

  describe('$or', () => {
    it('matches when any condition is true', () => {
      const doc = { status: 'draft', featured: true }
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

    it('handles empty $or array', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $or: [] })).toBe(false)
    })

    it('handles nested $or', () => {
      const doc = { a: 1, b: 2, c: 3 }
      expect(matchesFilter(doc, {
        $or: [
          { a: 99 },
          { $or: [{ b: 99 }, { c: 3 }] },
        ],
      })).toBe(true)
    })
  })

  describe('$not', () => {
    it('negates a condition', () => {
      const doc = { status: 'draft' }
      expect(matchesFilter(doc, { $not: { status: 'published' } })).toBe(true)
      expect(matchesFilter(doc, { $not: { status: 'draft' } })).toBe(false)
    })

    it('negates complex conditions', () => {
      const doc = { score: 50 }
      expect(matchesFilter(doc, { $not: { score: { $gt: 75 } } })).toBe(true)
      expect(matchesFilter(doc, { $not: { score: { $lt: 75 } } })).toBe(false)
    })
  })

  describe('$nor', () => {
    it('matches when no conditions are true', () => {
      const doc = { status: 'draft', featured: false }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(true)
    })

    it('fails when any condition is true', () => {
      const doc = { status: 'draft', featured: true }
      expect(matchesFilter(doc, {
        $nor: [
          { status: 'published' },
          { featured: true },
        ],
      })).toBe(false)
    })

    it('handles empty $nor array', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { $nor: [] })).toBe(true)
    })
  })

  describe('combined logical operators', () => {
    it('combines $and with $or', () => {
      const doc = { status: 'published', type: 'article', featured: true }
      expect(matchesFilter(doc, {
        $and: [
          { status: 'published' },
          {
            $or: [
              { type: 'article' },
              { featured: true },
            ],
          },
        ],
      })).toBe(true)
    })

    it('combines multiple logical operators', () => {
      const doc = { a: 1, b: 2, c: 3, d: 4 }
      expect(matchesFilter(doc, {
        $and: [
          { $or: [{ a: 1 }, { b: 99 }] },
          { $nor: [{ c: 99 }, { d: 99 }] },
          { $not: { e: 5 } },
        ],
      })).toBe(true)
    })
  })
})

// =============================================================================
// String Operators
// =============================================================================

describe('string operators', () => {
  describe('$regex', () => {
    it('matches regex pattern', () => {
      const doc = { email: 'user@example.com' }
      expect(matchesFilter(doc, { email: { $regex: '@example\\.com$' } })).toBe(true)
      expect(matchesFilter(doc, { email: { $regex: '@other\\.com$' } })).toBe(false)
    })

    it('supports regex flags via $options', () => {
      const doc = { name: 'HELLO WORLD' }
      expect(matchesFilter(doc, { name: { $regex: 'hello', $options: 'i' } })).toBe(true)
      expect(matchesFilter(doc, { name: { $regex: 'hello' } })).toBe(false)
    })

    it('supports RegExp objects', () => {
      const doc = { name: 'Test123' }
      expect(matchesFilter(doc, { name: { $regex: /test/i } })).toBe(true)
      expect(matchesFilter(doc, { name: { $regex: /^test$/i } })).toBe(false)
    })

    it('returns false for non-string values', () => {
      const doc = { value: 123 }
      expect(matchesFilter(doc, { value: { $regex: '123' } })).toBe(false)
    })
  })

  describe('$startsWith', () => {
    it('matches string prefix', () => {
      const doc = { url: 'https://example.com/path' }
      expect(matchesFilter(doc, { url: { $startsWith: 'https://' } })).toBe(true)
      expect(matchesFilter(doc, { url: { $startsWith: 'http://' } })).toBe(false)
    })

    it('returns false for non-string values', () => {
      const doc = { value: 12345 }
      expect(matchesFilter(doc, { value: { $startsWith: '123' } })).toBe(false)
    })
  })

  describe('$endsWith', () => {
    it('matches string suffix', () => {
      const doc = { filename: 'document.pdf' }
      expect(matchesFilter(doc, { filename: { $endsWith: '.pdf' } })).toBe(true)
      expect(matchesFilter(doc, { filename: { $endsWith: '.doc' } })).toBe(false)
    })

    it('returns false for non-string values', () => {
      const doc = { value: 12345 }
      expect(matchesFilter(doc, { value: { $endsWith: '45' } })).toBe(false)
    })
  })

  describe('$contains', () => {
    it('matches substring', () => {
      const doc = { description: 'The quick brown fox jumps' }
      expect(matchesFilter(doc, { description: { $contains: 'brown fox' } })).toBe(true)
      expect(matchesFilter(doc, { description: { $contains: 'lazy dog' } })).toBe(false)
    })

    it('returns false for non-string values', () => {
      const doc = { value: 12345 }
      expect(matchesFilter(doc, { value: { $contains: '234' } })).toBe(false)
    })
  })
})

// =============================================================================
// Array Operators
// =============================================================================

describe('array operators', () => {
  describe('$all', () => {
    it('matches when array contains all specified values', () => {
      const doc = { tags: ['a', 'b', 'c', 'd'] }
      expect(matchesFilter(doc, { tags: { $all: ['a', 'b'] } })).toBe(true)
      expect(matchesFilter(doc, { tags: { $all: ['a', 'x'] } })).toBe(false)
    })

    it('matches regardless of order', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: { $all: ['c', 'a'] } })).toBe(true)
    })

    it('returns false for non-array values', () => {
      const doc = { tags: 'not-an-array' }
      expect(matchesFilter(doc, { tags: { $all: ['a'] } })).toBe(false)
    })

    it('handles empty $all array', () => {
      const doc = { tags: ['a', 'b'] }
      expect(matchesFilter(doc, { tags: { $all: [] } })).toBe(true)
    })
  })

  describe('$elemMatch', () => {
    it('matches when any element matches the filter', () => {
      const doc = {
        items: [
          { name: 'apple', price: 1 },
          { name: 'banana', price: 2 },
          { name: 'orange', price: 3 },
        ],
      }
      expect(matchesFilter(doc, {
        items: { $elemMatch: { name: 'banana', price: 2 } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        items: { $elemMatch: { name: 'grape' } },
      })).toBe(false)
    })

    it('supports operators in $elemMatch', () => {
      const doc = {
        scores: [
          { subject: 'math', score: 85 },
          { subject: 'english', score: 92 },
        ],
      }
      expect(matchesFilter(doc, {
        scores: { $elemMatch: { score: { $gte: 90 } } },
      })).toBe(true)
      expect(matchesFilter(doc, {
        scores: { $elemMatch: { score: { $gte: 95 } } },
      })).toBe(false)
    })

    it('returns false for non-array values', () => {
      const doc = { items: { name: 'test' } }
      expect(matchesFilter(doc, {
        items: { $elemMatch: { name: 'test' } },
      })).toBe(false)
    })

    it('matches primitive array elements', () => {
      const doc = { numbers: [1, 2, 3, 4, 5] }
      // Note: $elemMatch with primitives matches the whole element
      expect(matchesFilter(doc, {
        numbers: { $elemMatch: { $gt: 3 } },
      })).toBe(true)
    })
  })

  describe('$size', () => {
    it('matches array of exact size', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      expect(matchesFilter(doc, { tags: { $size: 3 } })).toBe(true)
      expect(matchesFilter(doc, { tags: { $size: 2 } })).toBe(false)
      expect(matchesFilter(doc, { tags: { $size: 4 } })).toBe(false)
    })

    it('matches empty array', () => {
      const doc = { tags: [] }
      expect(matchesFilter(doc, { tags: { $size: 0 } })).toBe(true)
      expect(matchesFilter(doc, { tags: { $size: 1 } })).toBe(false)
    })

    it('returns false for non-array values', () => {
      const doc = { tags: 'not-an-array' }
      expect(matchesFilter(doc, { tags: { $size: 1 } })).toBe(false)
    })
  })
})

// =============================================================================
// Existence Operators
// =============================================================================

describe('existence operators', () => {
  describe('$exists', () => {
    it('matches when field exists', () => {
      const doc = { name: 'test', value: null }
      expect(matchesFilter(doc, { name: { $exists: true } })).toBe(true)
      expect(matchesFilter(doc, { value: { $exists: true } })).toBe(true) // null is a value
      expect(matchesFilter(doc, { missing: { $exists: true } })).toBe(false)
    })

    it('matches when field does not exist', () => {
      const doc = { name: 'test' }
      expect(matchesFilter(doc, { missing: { $exists: false } })).toBe(true)
      expect(matchesFilter(doc, { name: { $exists: false } })).toBe(false)
    })
  })

  describe('$type', () => {
    it('matches string type', () => {
      const doc = { name: 'test', count: 5 }
      expect(matchesFilter(doc, { name: { $type: 'string' } })).toBe(true)
      expect(matchesFilter(doc, { count: { $type: 'string' } })).toBe(false)
    })

    it('matches number type', () => {
      const doc = { score: 100, name: 'test' }
      expect(matchesFilter(doc, { score: { $type: 'number' } })).toBe(true)
      expect(matchesFilter(doc, { name: { $type: 'number' } })).toBe(false)
    })

    it('matches boolean type', () => {
      const doc = { active: true, count: 1 }
      expect(matchesFilter(doc, { active: { $type: 'boolean' } })).toBe(true)
      expect(matchesFilter(doc, { count: { $type: 'boolean' } })).toBe(false)
    })

    it('matches null type', () => {
      const doc = { value: null, other: 'test' }
      expect(matchesFilter(doc, { value: { $type: 'null' } })).toBe(true)
      expect(matchesFilter(doc, { missing: { $type: 'null' } })).toBe(true) // undefined is null type
      expect(matchesFilter(doc, { other: { $type: 'null' } })).toBe(false)
    })

    it('matches array type', () => {
      const doc = { tags: ['a', 'b'], name: 'test' }
      expect(matchesFilter(doc, { tags: { $type: 'array' } })).toBe(true)
      expect(matchesFilter(doc, { name: { $type: 'array' } })).toBe(false)
    })

    it('matches object type', () => {
      const doc = { meta: { key: 'value' }, tags: ['a'] }
      expect(matchesFilter(doc, { meta: { $type: 'object' } })).toBe(true)
      expect(matchesFilter(doc, { tags: { $type: 'object' } })).toBe(false)
    })

    it('matches date type', () => {
      const doc = { createdAt: new Date(), name: 'test' }
      expect(matchesFilter(doc, { createdAt: { $type: 'date' } })).toBe(true)
      expect(matchesFilter(doc, { name: { $type: 'date' } })).toBe(false)
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('edge cases', () => {
  it('handles undefined document', () => {
    expect(matchesFilter(undefined, { name: 'test' })).toBe(false)
  })

  it('handles null document', () => {
    expect(matchesFilter(null, { name: 'test' })).toBe(false)
  })

  it('handles primitive document', () => {
    expect(matchesFilter('string', { name: 'test' })).toBe(false)
    expect(matchesFilter(123, { name: 'test' })).toBe(false)
  })

  it('ignores special operators ($text, $vector, $geo)', () => {
    const doc = { name: 'test' }
    // These should be handled elsewhere, filter ignores them
    expect(matchesFilter(doc, { $text: { $search: 'test' } } as Filter)).toBe(true)
    expect(matchesFilter(doc, { $vector: { $near: [1, 2, 3], $k: 10, $field: 'embedding' } } as Filter)).toBe(true)
    expect(matchesFilter(doc, { $geo: { $near: { lat: 0, lng: 0 } } } as Filter)).toBe(true)
  })

  it('handles deeply nested documents', () => {
    const doc = {
      level1: {
        level2: {
          level3: {
            level4: {
              value: 'deep',
            },
          },
        },
      },
    }
    expect(matchesFilter(doc, { 'level1.level2.level3.level4.value': 'deep' })).toBe(true)
  })

  it('handles mixed filters (field + logical operators)', () => {
    const doc = { a: 1, b: 2, c: 3 }
    expect(matchesFilter(doc, {
      a: 1,
      $or: [{ b: 2 }, { b: 99 }],
      $not: { d: 4 },
    })).toBe(true)
  })
})

// =============================================================================
// createPredicate
// =============================================================================

describe('createPredicate', () => {
  it('creates a reusable predicate function', () => {
    const isPublished = createPredicate({ status: 'published' })

    expect(isPublished({ status: 'published', name: 'Post 1' })).toBe(true)
    expect(isPublished({ status: 'published', name: 'Post 2' })).toBe(true)
    expect(isPublished({ status: 'draft', name: 'Post 3' })).toBe(false)
  })

  it('works with array filter', () => {
    const docs = [
      { id: 1, score: 50 },
      { id: 2, score: 75 },
      { id: 3, score: 100 },
      { id: 4, score: 125 },
    ]

    const highScores = createPredicate<typeof docs[0]>({ score: { $gte: 100 } })
    const filtered = docs.filter(highScores)

    expect(filtered).toHaveLength(2)
    expect(filtered.map(d => d.id)).toEqual([3, 4])
  })

  it('creates typed predicate', () => {
    interface User {
      name: string
      age: number
      active: boolean
    }

    const isActiveAdult = createPredicate<User>({
      age: { $gte: 18 },
      active: true,
    })

    const user1: User = { name: 'John', age: 25, active: true }
    const user2: User = { name: 'Jane', age: 16, active: true }
    const user3: User = { name: 'Bob', age: 30, active: false }

    expect(isActiveAdult(user1)).toBe(true)
    expect(isActiveAdult(user2)).toBe(false)
    expect(isActiveAdult(user3)).toBe(false)
  })
})

// =============================================================================
// matchesCondition
// =============================================================================

describe('matchesCondition', () => {
  it('matches direct values', () => {
    expect(matchesCondition('test', 'test')).toBe(true)
    expect(matchesCondition('test', 'other')).toBe(false)
    expect(matchesCondition(100, 100)).toBe(true)
    expect(matchesCondition(100, 50)).toBe(false)
  })

  it('matches with operators', () => {
    expect(matchesCondition(100, { $gt: 50 })).toBe(true)
    expect(matchesCondition(100, { $lt: 50 })).toBe(false)
    expect(matchesCondition('test', { $in: ['test', 'other'] })).toBe(true)
  })

  it('is useful for $pull operations', () => {
    const array = [10, 25, 50, 75, 100]
    const condition = { $lt: 30 }
    const filtered = array.filter(v => !matchesCondition(v, condition))
    expect(filtered).toEqual([50, 75, 100])
  })
})

// =============================================================================
// Helper Functions
// =============================================================================

describe('deepEqual', () => {
  it('compares primitives', () => {
    expect(deepEqual(1, 1)).toBe(true)
    expect(deepEqual(1, 2)).toBe(false)
    expect(deepEqual('a', 'a')).toBe(true)
    expect(deepEqual('a', 'b')).toBe(false)
    expect(deepEqual(true, true)).toBe(true)
    expect(deepEqual(true, false)).toBe(false)
  })

  it('compares null and undefined', () => {
    expect(deepEqual(null, null)).toBe(true)
    expect(deepEqual(undefined, undefined)).toBe(true)
    // Note: Implementation treats null and undefined as equivalent (like MongoDB)
    expect(deepEqual(null, undefined)).toBe(true)
    expect(deepEqual(null, 'null')).toBe(false)
  })

  it('compares dates', () => {
    const date1 = new Date('2024-01-15')
    const date2 = new Date('2024-01-15')
    const date3 = new Date('2024-01-16')

    expect(deepEqual(date1, date2)).toBe(true)
    expect(deepEqual(date1, date3)).toBe(false)
  })

  it('compares arrays', () => {
    expect(deepEqual([1, 2, 3], [1, 2, 3])).toBe(true)
    expect(deepEqual([1, 2, 3], [1, 2])).toBe(false)
    expect(deepEqual([1, 2, 3], [3, 2, 1])).toBe(false)
    expect(deepEqual([[1, 2], [3, 4]], [[1, 2], [3, 4]])).toBe(true)
  })

  it('compares objects', () => {
    expect(deepEqual({ a: 1, b: 2 }, { a: 1, b: 2 })).toBe(true)
    expect(deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 })).toBe(true)
    expect(deepEqual({ a: 1 }, { a: 1, b: 2 })).toBe(false)
    expect(deepEqual({ a: { b: 1 } }, { a: { b: 1 } })).toBe(true)
  })
})

describe('compareValues', () => {
  it('compares numbers', () => {
    expect(compareValues(1, 2)).toBeLessThan(0)
    expect(compareValues(2, 1)).toBeGreaterThan(0)
    expect(compareValues(1, 1)).toBe(0)
  })

  it('compares strings', () => {
    expect(compareValues('a', 'b')).toBeLessThan(0)
    expect(compareValues('b', 'a')).toBeGreaterThan(0)
    expect(compareValues('a', 'a')).toBe(0)
  })

  it('compares dates', () => {
    const earlier = new Date('2024-01-01')
    const later = new Date('2024-12-31')
    expect(compareValues(earlier, later)).toBeLessThan(0)
    expect(compareValues(later, earlier)).toBeGreaterThan(0)
  })

  it('compares booleans', () => {
    expect(compareValues(false, true)).toBeLessThan(0)
    expect(compareValues(true, false)).toBeGreaterThan(0)
  })

  it('handles null/undefined', () => {
    expect(compareValues(null, 1)).toBeLessThan(0)
    expect(compareValues(1, null)).toBeGreaterThan(0)
    expect(compareValues(null, null)).toBe(0)
    expect(compareValues(undefined, undefined)).toBe(0)
  })
})

describe('getValueType', () => {
  it('identifies null', () => {
    expect(getValueType(null)).toBe('null')
    expect(getValueType(undefined)).toBe('null')
  })

  it('identifies primitives', () => {
    expect(getValueType(true)).toBe('boolean')
    expect(getValueType(false)).toBe('boolean')
    expect(getValueType(42)).toBe('number')
    expect(getValueType(3.14)).toBe('number')
    expect(getValueType('hello')).toBe('string')
  })

  it('identifies arrays', () => {
    expect(getValueType([])).toBe('array')
    expect(getValueType([1, 2, 3])).toBe('array')
  })

  it('identifies objects', () => {
    expect(getValueType({})).toBe('object')
    expect(getValueType({ key: 'value' })).toBe('object')
  })

  it('identifies dates', () => {
    expect(getValueType(new Date())).toBe('date')
  })
})

// =============================================================================
// Real-world Scenarios
// =============================================================================

describe('real-world scenarios', () => {
  describe('e-commerce product filtering', () => {
    const products = [
      { id: 1, name: 'Laptop', price: 999, category: 'electronics', inStock: true, tags: ['computer', 'work'] },
      { id: 2, name: 'Headphones', price: 199, category: 'electronics', inStock: true, tags: ['audio', 'music'] },
      { id: 3, name: 'Desk', price: 299, category: 'furniture', inStock: false, tags: ['office', 'work'] },
      { id: 4, name: 'Chair', price: 149, category: 'furniture', inStock: true, tags: ['office', 'comfort'] },
      { id: 5, name: 'Monitor', price: 399, category: 'electronics', inStock: true, tags: ['computer', 'work'] },
    ]

    it('filters by price range and category', () => {
      const filter: Filter = {
        category: 'electronics',
        price: { $gte: 100, $lte: 500 },
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.name)).toEqual(['Headphones', 'Monitor'])
    })

    it('filters in-stock items with specific tags', () => {
      const filter: Filter = {
        inStock: true,
        tags: { $all: ['work'] },
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.name)).toEqual(['Laptop', 'Monitor'])
    })

    it('filters using $or for multiple categories', () => {
      const filter: Filter = {
        $or: [
          { category: 'electronics', price: { $lt: 300 } },
          { category: 'furniture', inStock: true },
        ],
      }
      const results = products.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.name)).toEqual(['Headphones', 'Chair'])
    })
  })

  describe('user management', () => {
    const users = [
      { id: 1, name: 'Alice', email: 'alice@example.com', role: 'admin', lastLogin: new Date('2024-06-01'), settings: { theme: 'dark', notifications: true } },
      { id: 2, name: 'Bob', email: 'bob@company.org', role: 'user', lastLogin: new Date('2024-05-15'), settings: { theme: 'light', notifications: false } },
      { id: 3, name: 'Charlie', email: 'charlie@example.com', role: 'user', lastLogin: new Date('2024-06-10'), settings: { theme: 'dark', notifications: true } },
      { id: 4, name: 'Diana', email: 'diana@company.org', role: 'moderator', lastLogin: null, settings: { theme: 'auto', notifications: true } },
    ]

    it('filters by email domain using regex', () => {
      const filter: Filter = {
        email: { $regex: '@example\\.com$' },
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })

    it('filters by nested settings', () => {
      const filter: Filter = {
        'settings.theme': 'dark',
        'settings.notifications': true,
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })

    it('filters excluding certain roles', () => {
      const filter: Filter = {
        role: { $nin: ['admin', 'moderator'] },
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Bob', 'Charlie'])
    })

    it('filters users who have logged in', () => {
      const filter: Filter = {
        lastLogin: { $type: 'date' },
      }
      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.name)).toEqual(['Alice', 'Bob', 'Charlie'])
    })
  })

  describe('content management', () => {
    const posts = [
      { id: 1, title: 'Getting Started with TypeScript', status: 'published', views: 1500, tags: ['typescript', 'tutorial'], author: { name: 'Alice', verified: true } },
      { id: 2, title: 'Advanced React Patterns', status: 'draft', views: 0, tags: ['react', 'advanced'], author: { name: 'Bob', verified: false } },
      { id: 3, title: 'Node.js Best Practices', status: 'published', views: 3200, tags: ['nodejs', 'backend'], author: { name: 'Charlie', verified: true } },
      { id: 4, title: 'CSS Grid Layout', status: 'published', views: 800, tags: ['css', 'tutorial'], author: { name: 'Diana', verified: true } },
    ]

    it('filters popular published posts by verified authors', () => {
      const filter: Filter = {
        status: 'published',
        views: { $gte: 1000 },
        'author.verified': true,
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.title)).toEqual([
        'Getting Started with TypeScript',
        'Node.js Best Practices',
      ])
    })

    it('filters tutorial posts', () => {
      const filter: Filter = {
        tags: { $all: ['tutorial'] },
        status: 'published',
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.title)).toEqual([
        'Getting Started with TypeScript',
        'CSS Grid Layout',
      ])
    })

    it('filters posts matching title pattern', () => {
      const filter: Filter = {
        title: { $regex: '^(Getting|Advanced)', $options: 'i' },
      }
      const results = posts.filter(p => matchesFilter(p, filter))
      expect(results.map(p => p.title)).toEqual([
        'Getting Started with TypeScript',
        'Advanced React Patterns',
      ])
    })
  })
})

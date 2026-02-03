/**
 * Filter Type Guards Tests
 *
 * Tests for individual operator type guards in the filter module.
 */

import { describe, it, expect } from 'vitest'
import {
  // Category type guards
  isComparisonOperator,
  isStringOperator,
  isArrayOperator,
  isExistenceOperator,
  isFieldOperator,
  hasLogicalOperators,
  hasSpecialOperators,
  // Comparison operator type guards
  is$Eq,
  is$Ne,
  is$Gt,
  is$Gte,
  is$Lt,
  is$Lte,
  is$In,
  is$Nin,
  // String operator type guards
  is$Regex,
  is$StartsWith,
  is$EndsWith,
  is$Contains,
  // Array operator type guards
  is$All,
  is$ElemMatch,
  is$Size,
  // Existence operator type guards
  is$Exists,
  is$Type,
  // Logical operator type guards
  is$And,
  is$Or,
  is$Not,
  is$Nor,
  // Special operator type guards
  is$Text,
  is$Vector,
  is$Geo,
  // Types
  type Filter,
} from '../../../src/types/filter'

// =============================================================================
// Category Type Guards (existing tests)
// =============================================================================

describe('category type guards', () => {
  describe('isComparisonOperator', () => {
    it('returns true for comparison operators', () => {
      expect(isComparisonOperator({ $eq: 5 })).toBe(true)
      expect(isComparisonOperator({ $ne: 'test' })).toBe(true)
      expect(isComparisonOperator({ $gt: 10 })).toBe(true)
      expect(isComparisonOperator({ $gte: 10 })).toBe(true)
      expect(isComparisonOperator({ $lt: 10 })).toBe(true)
      expect(isComparisonOperator({ $lte: 10 })).toBe(true)
      expect(isComparisonOperator({ $in: [1, 2, 3] })).toBe(true)
      expect(isComparisonOperator({ $nin: [1, 2, 3] })).toBe(true)
    })

    it('returns false for non-comparison operators', () => {
      expect(isComparisonOperator({ $regex: 'test' })).toBe(false)
      expect(isComparisonOperator({ $all: [1, 2] })).toBe(false)
      expect(isComparisonOperator({ $exists: true })).toBe(false)
    })

    it('returns false for non-objects', () => {
      expect(isComparisonOperator(null)).toBe(false)
      expect(isComparisonOperator(undefined)).toBe(false)
      expect(isComparisonOperator('string')).toBe(false)
      expect(isComparisonOperator(123)).toBe(false)
    })

    it('returns false for multiple operators', () => {
      expect(isComparisonOperator({ $gt: 5, $lt: 10 })).toBe(false)
    })
  })

  describe('isStringOperator', () => {
    it('returns true for string operators', () => {
      expect(isStringOperator({ $regex: 'pattern' })).toBe(true)
      expect(isStringOperator({ $regex: 'pattern', $options: 'i' })).toBe(true)
      expect(isStringOperator({ $startsWith: 'prefix' })).toBe(true)
      expect(isStringOperator({ $endsWith: 'suffix' })).toBe(true)
      expect(isStringOperator({ $contains: 'substring' })).toBe(true)
    })

    it('returns false for non-string operators', () => {
      expect(isStringOperator({ $eq: 'test' })).toBe(false)
      expect(isStringOperator({ $in: ['a', 'b'] })).toBe(false)
    })
  })

  describe('isArrayOperator', () => {
    it('returns true for array operators', () => {
      expect(isArrayOperator({ $all: [1, 2, 3] })).toBe(true)
      expect(isArrayOperator({ $elemMatch: { name: 'test' } })).toBe(true)
      expect(isArrayOperator({ $size: 5 })).toBe(true)
    })

    it('returns false for non-array operators', () => {
      expect(isArrayOperator({ $in: [1, 2, 3] })).toBe(false)
      expect(isArrayOperator({ $eq: [1, 2, 3] })).toBe(false)
    })
  })

  describe('isExistenceOperator', () => {
    it('returns true for existence operators', () => {
      expect(isExistenceOperator({ $exists: true })).toBe(true)
      expect(isExistenceOperator({ $exists: false })).toBe(true)
      expect(isExistenceOperator({ $type: 'string' })).toBe(true)
      expect(isExistenceOperator({ $type: 'number' })).toBe(true)
    })

    it('returns false for non-existence operators', () => {
      expect(isExistenceOperator({ $eq: true })).toBe(false)
      expect(isExistenceOperator({ $in: ['string'] })).toBe(false)
    })
  })

  describe('isFieldOperator', () => {
    it('returns true for any field operator', () => {
      expect(isFieldOperator({ $eq: 5 })).toBe(true)
      expect(isFieldOperator({ $regex: 'test' })).toBe(true)
      expect(isFieldOperator({ $all: [1, 2] })).toBe(true)
      expect(isFieldOperator({ $exists: true })).toBe(true)
    })

    it('returns false for direct values', () => {
      expect(isFieldOperator('test')).toBe(false)
      expect(isFieldOperator(123)).toBe(false)
      expect(isFieldOperator({ name: 'test' })).toBe(false)
    })
  })

  describe('hasLogicalOperators', () => {
    it('returns true when filter has logical operators', () => {
      expect(hasLogicalOperators({ $and: [{ a: 1 }] })).toBe(true)
      expect(hasLogicalOperators({ $or: [{ a: 1 }] })).toBe(true)
      expect(hasLogicalOperators({ $not: { a: 1 } })).toBe(true)
      expect(hasLogicalOperators({ $nor: [{ a: 1 }] })).toBe(true)
      expect(hasLogicalOperators({ name: 'test', $and: [{ a: 1 }] })).toBe(true)
    })

    it('returns false when no logical operators', () => {
      expect(hasLogicalOperators({ name: 'test' })).toBe(false)
      expect(hasLogicalOperators({ score: { $gt: 10 } })).toBe(false)
    })
  })

  describe('hasSpecialOperators', () => {
    it('returns true when filter has special operators', () => {
      expect(hasSpecialOperators({ $text: { $search: 'query' } } as Filter)).toBe(true)
      expect(hasSpecialOperators({ $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 } } as Filter)).toBe(true)
      expect(hasSpecialOperators({ $geo: { $near: { lat: 0, lng: 0 } } } as Filter)).toBe(true)
    })

    it('returns false when no special operators', () => {
      expect(hasSpecialOperators({ name: 'test' })).toBe(false)
      expect(hasSpecialOperators({ $and: [{ a: 1 }] })).toBe(false)
    })
  })
})

// =============================================================================
// Comparison Operator Type Guards
// =============================================================================

describe('comparison operator type guards', () => {
  describe('is$Eq', () => {
    it('returns true for valid $eq operators', () => {
      expect(is$Eq({ $eq: 5 })).toBe(true)
      expect(is$Eq({ $eq: 'test' })).toBe(true)
      expect(is$Eq({ $eq: null })).toBe(true)
      expect(is$Eq({ $eq: { nested: 'value' } })).toBe(true)
      expect(is$Eq({ $eq: [1, 2, 3] })).toBe(true)
    })

    it('returns false for non-$eq operators', () => {
      expect(is$Eq({ $ne: 5 })).toBe(false)
      expect(is$Eq({ $gt: 5 })).toBe(false)
    })

    it('returns false for multiple operators', () => {
      expect(is$Eq({ $eq: 5, $ne: 10 })).toBe(false)
    })

    it('returns false for non-objects', () => {
      expect(is$Eq(null)).toBe(false)
      expect(is$Eq(undefined)).toBe(false)
      expect(is$Eq(5)).toBe(false)
      expect(is$Eq('$eq')).toBe(false)
    })

    it('narrows type correctly', () => {
      const value: unknown = { $eq: 42 }
      if (is$Eq<number>(value)) {
        // TypeScript should know value.$eq exists
        expect(value.$eq).toBe(42)
      }
    })
  })

  describe('is$Ne', () => {
    it('returns true for valid $ne operators', () => {
      expect(is$Ne({ $ne: 5 })).toBe(true)
      expect(is$Ne({ $ne: 'test' })).toBe(true)
      expect(is$Ne({ $ne: null })).toBe(true)
    })

    it('returns false for non-$ne operators', () => {
      expect(is$Ne({ $eq: 5 })).toBe(false)
    })

    it('returns false for multiple operators', () => {
      expect(is$Ne({ $ne: 5, $eq: 10 })).toBe(false)
    })
  })

  describe('is$Gt', () => {
    it('returns true for valid $gt operators', () => {
      expect(is$Gt({ $gt: 5 })).toBe(true)
      expect(is$Gt({ $gt: new Date() })).toBe(true)
      expect(is$Gt({ $gt: 'abc' })).toBe(true)
    })

    it('returns false for non-$gt operators', () => {
      expect(is$Gt({ $gte: 5 })).toBe(false)
      expect(is$Gt({ $lt: 5 })).toBe(false)
    })
  })

  describe('is$Gte', () => {
    it('returns true for valid $gte operators', () => {
      expect(is$Gte({ $gte: 5 })).toBe(true)
      expect(is$Gte({ $gte: 0 })).toBe(true)
    })

    it('returns false for non-$gte operators', () => {
      expect(is$Gte({ $gt: 5 })).toBe(false)
    })
  })

  describe('is$Lt', () => {
    it('returns true for valid $lt operators', () => {
      expect(is$Lt({ $lt: 5 })).toBe(true)
      expect(is$Lt({ $lt: new Date() })).toBe(true)
    })

    it('returns false for non-$lt operators', () => {
      expect(is$Lt({ $lte: 5 })).toBe(false)
    })
  })

  describe('is$Lte', () => {
    it('returns true for valid $lte operators', () => {
      expect(is$Lte({ $lte: 5 })).toBe(true)
      expect(is$Lte({ $lte: 0 })).toBe(true)
    })

    it('returns false for non-$lte operators', () => {
      expect(is$Lte({ $lt: 5 })).toBe(false)
    })
  })

  describe('is$In', () => {
    it('returns true for valid $in operators', () => {
      expect(is$In({ $in: [1, 2, 3] })).toBe(true)
      expect(is$In({ $in: ['a', 'b', 'c'] })).toBe(true)
      expect(is$In({ $in: [] })).toBe(true)
    })

    it('returns false for non-array $in values', () => {
      expect(is$In({ $in: 'not-array' })).toBe(false)
      expect(is$In({ $in: 123 })).toBe(false)
    })

    it('returns false for non-$in operators', () => {
      expect(is$In({ $nin: [1, 2, 3] })).toBe(false)
    })

    it('narrows type correctly', () => {
      const value: unknown = { $in: [1, 2, 3] }
      if (is$In<number>(value)) {
        expect(value.$in).toEqual([1, 2, 3])
      }
    })
  })

  describe('is$Nin', () => {
    it('returns true for valid $nin operators', () => {
      expect(is$Nin({ $nin: [1, 2, 3] })).toBe(true)
      expect(is$Nin({ $nin: ['a', 'b'] })).toBe(true)
      expect(is$Nin({ $nin: [] })).toBe(true)
    })

    it('returns false for non-array $nin values', () => {
      expect(is$Nin({ $nin: 'not-array' })).toBe(false)
    })

    it('returns false for non-$nin operators', () => {
      expect(is$Nin({ $in: [1, 2, 3] })).toBe(false)
    })
  })
})

// =============================================================================
// String Operator Type Guards
// =============================================================================

describe('string operator type guards', () => {
  describe('is$Regex', () => {
    it('returns true for valid $regex operators', () => {
      expect(is$Regex({ $regex: 'pattern' })).toBe(true)
      expect(is$Regex({ $regex: /pattern/ })).toBe(true)
      expect(is$Regex({ $regex: 'pattern', $options: 'i' })).toBe(true)
      expect(is$Regex({ $regex: 'pattern', $options: 'gi' })).toBe(true)
    })

    it('returns false for non-$regex operators', () => {
      expect(is$Regex({ $contains: 'pattern' })).toBe(false)
      expect(is$Regex({ $eq: 'pattern' })).toBe(false)
    })

    it('returns false for $regex with extra keys', () => {
      expect(is$Regex({ $regex: 'pattern', $other: 'value' })).toBe(false)
    })

    it('returns false for non-objects', () => {
      expect(is$Regex('pattern')).toBe(false)
      expect(is$Regex(null)).toBe(false)
    })
  })

  describe('is$StartsWith', () => {
    it('returns true for valid $startsWith operators', () => {
      expect(is$StartsWith({ $startsWith: 'prefix' })).toBe(true)
      expect(is$StartsWith({ $startsWith: '' })).toBe(true)
    })

    it('returns false for non-$startsWith operators', () => {
      expect(is$StartsWith({ $endsWith: 'suffix' })).toBe(false)
      expect(is$StartsWith({ $contains: 'text' })).toBe(false)
    })

    it('returns false for multiple keys', () => {
      expect(is$StartsWith({ $startsWith: 'a', $endsWith: 'b' })).toBe(false)
    })
  })

  describe('is$EndsWith', () => {
    it('returns true for valid $endsWith operators', () => {
      expect(is$EndsWith({ $endsWith: 'suffix' })).toBe(true)
      expect(is$EndsWith({ $endsWith: '' })).toBe(true)
    })

    it('returns false for non-$endsWith operators', () => {
      expect(is$EndsWith({ $startsWith: 'prefix' })).toBe(false)
    })
  })

  describe('is$Contains', () => {
    it('returns true for valid $contains operators', () => {
      expect(is$Contains({ $contains: 'substring' })).toBe(true)
      expect(is$Contains({ $contains: '' })).toBe(true)
    })

    it('returns false for non-$contains operators', () => {
      expect(is$Contains({ $regex: 'pattern' })).toBe(false)
      expect(is$Contains({ $in: ['a', 'b'] })).toBe(false)
    })
  })
})

// =============================================================================
// Array Operator Type Guards
// =============================================================================

describe('array operator type guards', () => {
  describe('is$All', () => {
    it('returns true for valid $all operators', () => {
      expect(is$All({ $all: [1, 2, 3] })).toBe(true)
      expect(is$All({ $all: ['a', 'b'] })).toBe(true)
      expect(is$All({ $all: [] })).toBe(true)
    })

    it('returns false for non-array $all values', () => {
      expect(is$All({ $all: 'not-array' })).toBe(false)
      expect(is$All({ $all: 123 })).toBe(false)
    })

    it('returns false for non-$all operators', () => {
      expect(is$All({ $in: [1, 2, 3] })).toBe(false)
    })
  })

  describe('is$ElemMatch', () => {
    it('returns true for valid $elemMatch operators', () => {
      expect(is$ElemMatch({ $elemMatch: { name: 'test' } })).toBe(true)
      expect(is$ElemMatch({ $elemMatch: { price: { $gt: 10 } } })).toBe(true)
      expect(is$ElemMatch({ $elemMatch: {} })).toBe(true)
    })

    it('returns false for non-object $elemMatch values', () => {
      expect(is$ElemMatch({ $elemMatch: 'not-object' })).toBe(false)
      expect(is$ElemMatch({ $elemMatch: 123 })).toBe(false)
      expect(is$ElemMatch({ $elemMatch: null })).toBe(false)
    })

    it('returns false for non-$elemMatch operators', () => {
      expect(is$ElemMatch({ $all: [{ name: 'test' }] })).toBe(false)
    })
  })

  describe('is$Size', () => {
    it('returns true for valid $size operators', () => {
      expect(is$Size({ $size: 5 })).toBe(true)
      expect(is$Size({ $size: 0 })).toBe(true)
      expect(is$Size({ $size: 100 })).toBe(true)
    })

    it('returns false for non-number $size values', () => {
      expect(is$Size({ $size: '5' })).toBe(false)
      expect(is$Size({ $size: null })).toBe(false)
    })

    it('returns false for non-$size operators', () => {
      expect(is$Size({ $all: [1, 2, 3] })).toBe(false)
    })
  })
})

// =============================================================================
// Existence Operator Type Guards
// =============================================================================

describe('existence operator type guards', () => {
  describe('is$Exists', () => {
    it('returns true for valid $exists operators', () => {
      expect(is$Exists({ $exists: true })).toBe(true)
      expect(is$Exists({ $exists: false })).toBe(true)
    })

    it('returns false for non-boolean $exists values', () => {
      expect(is$Exists({ $exists: 1 })).toBe(false)
      expect(is$Exists({ $exists: 'true' })).toBe(false)
      expect(is$Exists({ $exists: null })).toBe(false)
    })

    it('returns false for non-$exists operators', () => {
      expect(is$Exists({ $type: 'boolean' })).toBe(false)
    })
  })

  describe('is$Type', () => {
    it('returns true for valid $type operators', () => {
      expect(is$Type({ $type: 'null' })).toBe(true)
      expect(is$Type({ $type: 'boolean' })).toBe(true)
      expect(is$Type({ $type: 'number' })).toBe(true)
      expect(is$Type({ $type: 'string' })).toBe(true)
      expect(is$Type({ $type: 'array' })).toBe(true)
      expect(is$Type({ $type: 'object' })).toBe(true)
      expect(is$Type({ $type: 'date' })).toBe(true)
    })

    it('returns false for invalid type values', () => {
      expect(is$Type({ $type: 'invalid' })).toBe(false)
      expect(is$Type({ $type: 'function' })).toBe(false)
      expect(is$Type({ $type: 'undefined' })).toBe(false)
    })

    it('returns false for non-string $type values', () => {
      expect(is$Type({ $type: 123 })).toBe(false)
      expect(is$Type({ $type: null })).toBe(false)
    })

    it('returns false for non-$type operators', () => {
      expect(is$Type({ $exists: true })).toBe(false)
    })
  })
})

// =============================================================================
// Logical Operator Type Guards
// =============================================================================

describe('logical operator type guards', () => {
  describe('is$And', () => {
    it('returns true for valid $and operators', () => {
      expect(is$And({ $and: [{ a: 1 }, { b: 2 }] })).toBe(true)
      expect(is$And({ $and: [] })).toBe(true)
      expect(is$And({ $and: [{ nested: { $gt: 5 } }] })).toBe(true)
    })

    it('returns false for non-array $and values', () => {
      expect(is$And({ $and: { a: 1 } })).toBe(false)
      expect(is$And({ $and: 'not-array' })).toBe(false)
    })

    it('returns false for non-$and operators', () => {
      expect(is$And({ $or: [{ a: 1 }] })).toBe(false)
    })

    it('returns false for multiple keys', () => {
      expect(is$And({ $and: [{ a: 1 }], $or: [{ b: 2 }] })).toBe(false)
    })
  })

  describe('is$Or', () => {
    it('returns true for valid $or operators', () => {
      expect(is$Or({ $or: [{ a: 1 }, { b: 2 }] })).toBe(true)
      expect(is$Or({ $or: [] })).toBe(true)
    })

    it('returns false for non-array $or values', () => {
      expect(is$Or({ $or: { a: 1 } })).toBe(false)
    })

    it('returns false for non-$or operators', () => {
      expect(is$Or({ $and: [{ a: 1 }] })).toBe(false)
    })
  })

  describe('is$Not', () => {
    it('returns true for valid $not operators', () => {
      expect(is$Not({ $not: { a: 1 } })).toBe(true)
      expect(is$Not({ $not: { score: { $gt: 100 } } })).toBe(true)
      expect(is$Not({ $not: {} })).toBe(true)
    })

    it('returns false for non-object $not values', () => {
      expect(is$Not({ $not: 'not-object' })).toBe(false)
      expect(is$Not({ $not: 123 })).toBe(false)
      expect(is$Not({ $not: null })).toBe(false)
    })

    it('returns false for non-$not operators', () => {
      expect(is$Not({ $and: [{ a: 1 }] })).toBe(false)
    })
  })

  describe('is$Nor', () => {
    it('returns true for valid $nor operators', () => {
      expect(is$Nor({ $nor: [{ a: 1 }, { b: 2 }] })).toBe(true)
      expect(is$Nor({ $nor: [] })).toBe(true)
    })

    it('returns false for non-array $nor values', () => {
      expect(is$Nor({ $nor: { a: 1 } })).toBe(false)
    })

    it('returns false for non-$nor operators', () => {
      expect(is$Nor({ $or: [{ a: 1 }] })).toBe(false)
    })
  })
})

// =============================================================================
// Special Operator Type Guards
// =============================================================================

describe('special operator type guards', () => {
  describe('is$Text', () => {
    it('returns true for valid $text operators', () => {
      expect(is$Text({ $text: { $search: 'query' } })).toBe(true)
      expect(is$Text({ $text: { $search: 'query', $language: 'en' } })).toBe(true)
      expect(is$Text({ $text: { $search: 'query', $caseSensitive: true } })).toBe(true)
    })

    it('returns false for invalid $text structures', () => {
      expect(is$Text({ $text: 'not-object' })).toBe(false)
      expect(is$Text({ $text: { query: 'missing $search' } })).toBe(false)
      expect(is$Text({ $text: null })).toBe(false)
    })

    it('returns false for non-$text operators', () => {
      expect(is$Text({ $vector: { query: [1], field: 'f', topK: 1 } })).toBe(false)
    })
  })

  describe('is$Vector', () => {
    it('returns true for valid $vector operators', () => {
      expect(is$Vector({ $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 } })).toBe(true)
      expect(is$Vector({ $vector: { query: 'search text', field: 'vec', topK: 5 } })).toBe(true)
      expect(is$Vector({ $vector: { $near: [1, 2], $k: 10, $field: 'vec' } })).toBe(true)
    })

    it('returns false for invalid $vector structures', () => {
      expect(is$Vector({ $vector: 'not-object' })).toBe(false)
      expect(is$Vector({ $vector: null })).toBe(false)
    })

    it('returns false for non-$vector operators', () => {
      expect(is$Vector({ $text: { $search: 'query' } })).toBe(false)
    })
  })

  describe('is$Geo', () => {
    it('returns true for valid $geo operators', () => {
      expect(is$Geo({ $geo: { $near: { lat: 0, lng: 0 } } })).toBe(true)
      expect(is$Geo({ $geo: { $near: { lat: 40.7128, lng: -74.0060 }, $maxDistance: 1000 } })).toBe(true)
    })

    it('returns false for invalid $geo structures', () => {
      expect(is$Geo({ $geo: 'not-object' })).toBe(false)
      expect(is$Geo({ $geo: null })).toBe(false)
      expect(is$Geo({ $geo: { location: { lat: 0, lng: 0 } } })).toBe(false) // missing $near
    })

    it('returns false for non-$geo operators', () => {
      expect(is$Geo({ $vector: { query: [1], field: 'f', topK: 1 } })).toBe(false)
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('edge cases', () => {
  it('handles empty objects', () => {
    expect(is$Eq({})).toBe(false)
    expect(is$In({})).toBe(false)
    expect(is$And({})).toBe(false)
    expect(isComparisonOperator({})).toBe(false)
  })

  it('handles arrays', () => {
    expect(is$Eq([{ $eq: 1 }])).toBe(false)
    expect(is$In([1, 2, 3])).toBe(false)
    expect(isComparisonOperator([{ $eq: 1 }])).toBe(false)
  })

  it('handles Date objects', () => {
    expect(is$Eq(new Date())).toBe(false)
    expect(isComparisonOperator(new Date())).toBe(false)
  })

  it('handles nested operators as values', () => {
    // These should be valid - the value can be any type
    expect(is$Eq({ $eq: { $nested: 'value' } })).toBe(true)
    expect(is$In({ $in: [{ complex: 'object' }] })).toBe(true)
  })

  it('all type guards return false for undefined', () => {
    expect(is$Eq(undefined)).toBe(false)
    expect(is$Ne(undefined)).toBe(false)
    expect(is$Gt(undefined)).toBe(false)
    expect(is$In(undefined)).toBe(false)
    expect(is$Regex(undefined)).toBe(false)
    expect(is$All(undefined)).toBe(false)
    expect(is$Exists(undefined)).toBe(false)
    expect(is$And(undefined)).toBe(false)
    expect(is$Text(undefined)).toBe(false)
    expect(is$Vector(undefined)).toBe(false)
    expect(is$Geo(undefined)).toBe(false)
  })
})

// =============================================================================
// Type Narrowing Examples
// =============================================================================

describe('type narrowing', () => {
  it('allows safe property access after type guard', () => {
    const value: unknown = { $in: [1, 2, 3] }

    if (is$In<number>(value)) {
      // TypeScript should allow this
      const sum = value.$in.reduce((a, b) => a + b, 0)
      expect(sum).toBe(6)
    } else {
      throw new Error('Type guard should have returned true')
    }
  })

  it('allows switching on operator type', () => {
    function describeOperator(op: unknown): string {
      if (is$Eq(op)) return `equals ${op.$eq}`
      if (is$Gt(op)) return `greater than ${op.$gt}`
      if (is$In(op)) return `in [${op.$in.join(', ')}]`
      if (is$Regex(op)) return `matches ${op.$regex}`
      return 'unknown operator'
    }

    expect(describeOperator({ $eq: 5 })).toBe('equals 5')
    expect(describeOperator({ $gt: 10 })).toBe('greater than 10')
    expect(describeOperator({ $in: [1, 2, 3] })).toBe('in [1, 2, 3]')
    expect(describeOperator({ $regex: '^test' })).toBe('matches ^test')
    expect(describeOperator({ foo: 'bar' })).toBe('unknown operator')
  })
})

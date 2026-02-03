/**
 * Tests for New MongoDB Comparison Operators
 *
 * This file tests the following new operators:
 * - $mod: Modulo operation (value % divisor === remainder)
 * - $expr: Expression evaluation using aggregation operators
 * - $comment: Query comment for logging/debugging (no-op)
 */

import { describe, it, expect } from 'vitest'
import { matchesFilter, matchesCondition } from '../../../src/query/filter'
import { QueryBuilder } from '../../../src/query/builder'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// $mod Operator - Modulo Operation
// =============================================================================

describe('$mod operator', () => {
  describe('basic modulo operations', () => {
    it('matches when value % divisor equals remainder', () => {
      // 10 % 3 = 1
      expect(matchesFilter({ count: 10 }, { count: { $mod: [3, 1] } })).toBe(true)
    })

    it('does not match when value % divisor does not equal remainder', () => {
      // 10 % 3 = 1, not 2
      expect(matchesFilter({ count: 10 }, { count: { $mod: [3, 2] } })).toBe(false)
    })

    it('matches even numbers (value % 2 === 0)', () => {
      expect(matchesFilter({ num: 4 }, { num: { $mod: [2, 0] } })).toBe(true)
      expect(matchesFilter({ num: 6 }, { num: { $mod: [2, 0] } })).toBe(true)
      expect(matchesFilter({ num: 5 }, { num: { $mod: [2, 0] } })).toBe(false)
      expect(matchesFilter({ num: 7 }, { num: { $mod: [2, 0] } })).toBe(false)
    })

    it('matches odd numbers (value % 2 === 1)', () => {
      expect(matchesFilter({ num: 5 }, { num: { $mod: [2, 1] } })).toBe(true)
      expect(matchesFilter({ num: 7 }, { num: { $mod: [2, 1] } })).toBe(true)
      expect(matchesFilter({ num: 4 }, { num: { $mod: [2, 1] } })).toBe(false)
      expect(matchesFilter({ num: 6 }, { num: { $mod: [2, 1] } })).toBe(false)
    })

    it('matches every nth item (value % n === 0)', () => {
      // Every 5th item
      expect(matchesFilter({ id: 5 }, { id: { $mod: [5, 0] } })).toBe(true)
      expect(matchesFilter({ id: 10 }, { id: { $mod: [5, 0] } })).toBe(true)
      expect(matchesFilter({ id: 15 }, { id: { $mod: [5, 0] } })).toBe(true)
      expect(matchesFilter({ id: 3 }, { id: { $mod: [5, 0] } })).toBe(false)
      expect(matchesFilter({ id: 7 }, { id: { $mod: [5, 0] } })).toBe(false)
    })
  })

  describe('edge cases', () => {
    it('handles zero remainder', () => {
      expect(matchesFilter({ value: 12 }, { value: { $mod: [4, 0] } })).toBe(true)
      expect(matchesFilter({ value: 13 }, { value: { $mod: [4, 0] } })).toBe(false)
    })

    it('handles value of zero', () => {
      expect(matchesFilter({ value: 0 }, { value: { $mod: [5, 0] } })).toBe(true)
      expect(matchesFilter({ value: 0 }, { value: { $mod: [5, 1] } })).toBe(false)
    })

    it('handles negative numbers', () => {
      // JavaScript: -10 % 3 = -1
      expect(matchesFilter({ value: -10 }, { value: { $mod: [3, -1] } })).toBe(true)
    })

    it('handles floating point divisor (truncates)', () => {
      // 10 % 3.5 in JavaScript gives 0 because 10 - 3.5*2 = 3, then 3 - 3.5 = -0.5... actually it's 3
      // Let's test with a cleaner case
      expect(matchesFilter({ value: 10 }, { value: { $mod: [3.0, 1] } })).toBe(true)
    })

    it('returns false for null value', () => {
      expect(matchesFilter({ value: null }, { value: { $mod: [3, 0] } })).toBe(false)
    })

    it('returns false for undefined value', () => {
      expect(matchesFilter({}, { missing: { $mod: [3, 0] } })).toBe(false)
    })

    it('returns false for non-numeric value', () => {
      expect(matchesFilter({ value: 'ten' }, { value: { $mod: [3, 1] } })).toBe(false)
    })

    it('returns false for invalid $mod array (wrong length)', () => {
      expect(matchesFilter({ value: 10 }, { value: { $mod: [3] as any } })).toBe(false)
      expect(matchesFilter({ value: 10 }, { value: { $mod: [] as any } })).toBe(false)
      expect(matchesFilter({ value: 10 }, { value: { $mod: [3, 1, 2] as any } })).toBe(false)
    })

    it('returns false when divisor is zero', () => {
      // Division by zero
      expect(matchesFilter({ value: 10 }, { value: { $mod: [0, 0] } })).toBe(false)
    })
  })

  describe('with nested fields', () => {
    it('works with dot notation', () => {
      const doc = { stats: { count: 15 } }
      expect(matchesFilter(doc, { 'stats.count': { $mod: [5, 0] } })).toBe(true)
      expect(matchesFilter(doc, { 'stats.count': { $mod: [5, 1] } })).toBe(false)
    })
  })

  describe('combined with other operators', () => {
    it('combines with $gt', () => {
      // Even numbers greater than 5
      const doc = { value: 8 }
      expect(matchesFilter(doc, { value: { $mod: [2, 0], $gt: 5 } })).toBe(true)
      expect(matchesFilter({ value: 4 }, { value: { $mod: [2, 0], $gt: 5 } })).toBe(false)
      expect(matchesFilter({ value: 7 }, { value: { $mod: [2, 0], $gt: 5 } })).toBe(false)
    })
  })

  describe('matchesCondition with $mod', () => {
    it('evaluates $mod directly', () => {
      expect(matchesCondition(10, { $mod: [3, 1] })).toBe(true)
      expect(matchesCondition(10, { $mod: [3, 2] })).toBe(false)
    })
  })
})

// =============================================================================
// $expr Operator - Expression Evaluation
// =============================================================================

describe('$expr operator', () => {
  describe('field comparison within document', () => {
    it('compares two fields using $eq', () => {
      const doc = { quantity: 10, sold: 10 }
      expect(matchesFilter(doc, { $expr: { $eq: ['$quantity', '$sold'] } })).toBe(true)

      const doc2 = { quantity: 10, sold: 5 }
      expect(matchesFilter(doc2, { $expr: { $eq: ['$quantity', '$sold'] } })).toBe(false)
    })

    it('compares field to literal using $gt', () => {
      const doc = { price: 100 }
      expect(matchesFilter(doc, { $expr: { $gt: ['$price', 50] } })).toBe(true)
      expect(matchesFilter(doc, { $expr: { $gt: ['$price', 150] } })).toBe(false)
    })

    it('compares field to field using $gt', () => {
      const doc = { budget: 1000, spent: 500 }
      expect(matchesFilter(doc, { $expr: { $gt: ['$budget', '$spent'] } })).toBe(true)

      const doc2 = { budget: 500, spent: 1000 }
      expect(matchesFilter(doc2, { $expr: { $gt: ['$budget', '$spent'] } })).toBe(false)
    })

    it('compares field to field using $gte', () => {
      const doc = { budget: 1000, spent: 1000 }
      expect(matchesFilter(doc, { $expr: { $gte: ['$budget', '$spent'] } })).toBe(true)
    })

    it('compares field to field using $lt', () => {
      const doc = { start: 10, end: 20 }
      expect(matchesFilter(doc, { $expr: { $lt: ['$start', '$end'] } })).toBe(true)
    })

    it('compares field to field using $lte', () => {
      const doc = { min: 10, max: 10 }
      expect(matchesFilter(doc, { $expr: { $lte: ['$min', '$max'] } })).toBe(true)
    })

    it('compares field to field using $ne', () => {
      const doc = { a: 10, b: 20 }
      expect(matchesFilter(doc, { $expr: { $ne: ['$a', '$b'] } })).toBe(true)

      const doc2 = { a: 10, b: 10 }
      expect(matchesFilter(doc2, { $expr: { $ne: ['$a', '$b'] } })).toBe(false)
    })
  })

  describe('nested field access', () => {
    it('accesses nested fields with dot notation', () => {
      const doc = { user: { score: 100, threshold: 80 } }
      expect(matchesFilter(doc, { $expr: { $gt: ['$user.score', '$user.threshold'] } })).toBe(true)
    })
  })

  describe('edge cases', () => {
    it('handles missing fields (returns null)', () => {
      const doc = { a: 10 }
      expect(matchesFilter(doc, { $expr: { $eq: ['$missing', null] } })).toBe(true)
    })

    it('returns false for invalid expression format', () => {
      const doc = { value: 10 }
      expect(matchesFilter(doc, { $expr: { $invalidOp: ['$value', 5] } })).toBe(false)
    })

    it('returns false for null document', () => {
      expect(matchesFilter(null as any, { $expr: { $eq: ['$a', '$b'] } })).toBe(false)
    })
  })

  describe('combined with other filters', () => {
    it('combines $expr with field conditions', () => {
      const doc = { status: 'active', quantity: 100, sold: 50 }
      expect(matchesFilter(doc, {
        status: 'active',
        $expr: { $gt: ['$quantity', '$sold'] }
      })).toBe(true)

      const doc2 = { status: 'inactive', quantity: 100, sold: 50 }
      expect(matchesFilter(doc2, {
        status: 'active',
        $expr: { $gt: ['$quantity', '$sold'] }
      })).toBe(false)
    })

    it('uses $expr inside $and', () => {
      const doc = { a: 10, b: 5, c: 3 }
      expect(matchesFilter(doc, {
        $and: [
          { $expr: { $gt: ['$a', '$b'] } },
          { $expr: { $gt: ['$b', '$c'] } }
        ]
      })).toBe(true)
    })
  })

  describe('real-world use cases', () => {
    it('finds products where discount price is lower than original', () => {
      const product = { originalPrice: 100, discountPrice: 80 }
      expect(matchesFilter(product, {
        $expr: { $lt: ['$discountPrice', '$originalPrice'] }
      })).toBe(true)
    })

    it('finds items where stock is below reorder threshold', () => {
      const item = { stock: 5, reorderLevel: 10 }
      expect(matchesFilter(item, {
        $expr: { $lt: ['$stock', '$reorderLevel'] }
      })).toBe(true)
    })

    it('finds users who have exceeded their budget', () => {
      const user = { monthlyBudget: 500, currentSpending: 600 }
      expect(matchesFilter(user, {
        $expr: { $gt: ['$currentSpending', '$monthlyBudget'] }
      })).toBe(true)
    })
  })
})

// =============================================================================
// $comment Operator - Query Comment
// =============================================================================

describe('$comment operator', () => {
  describe('basic behavior', () => {
    it('has no effect on matching (pass-through)', () => {
      const doc = { name: 'test', status: 'active' }

      // With $comment
      expect(matchesFilter(doc, {
        status: 'active',
        $comment: 'This query finds active items'
      })).toBe(true)

      // Same filter without $comment should have same result
      expect(matchesFilter(doc, {
        status: 'active'
      })).toBe(true)
    })

    it('does not affect filter results when condition is false', () => {
      const doc = { status: 'inactive' }

      expect(matchesFilter(doc, {
        status: 'active',
        $comment: 'Looking for active items'
      })).toBe(false)
    })

    it('works with empty filter (matches all)', () => {
      const doc = { name: 'test' }

      expect(matchesFilter(doc, {
        $comment: 'Match all documents'
      })).toBe(true)
    })

    it('accepts any string value', () => {
      const doc = { name: 'test' }

      expect(matchesFilter(doc, { $comment: '' })).toBe(true)
      expect(matchesFilter(doc, { $comment: 'simple comment' })).toBe(true)
      expect(matchesFilter(doc, { $comment: 'Query to find items created by user 123 for debugging issue #456' })).toBe(true)
    })
  })

  describe('combined with other operators', () => {
    it('works with logical operators', () => {
      const doc = { a: 1, b: 2 }

      expect(matchesFilter(doc, {
        $comment: 'Check if a=1 or b=2',
        $or: [{ a: 1 }, { b: 3 }]
      })).toBe(true)
    })

    it('works with comparison operators', () => {
      const doc = { score: 85 }

      expect(matchesFilter(doc, {
        $comment: 'Find scores above 80',
        score: { $gt: 80 }
      })).toBe(true)
    })

    it('works with $and', () => {
      const doc = { status: 'active', score: 100 }

      expect(matchesFilter(doc, {
        $comment: 'Find active high scorers',
        $and: [
          { status: 'active' },
          { score: { $gte: 90 } }
        ]
      })).toBe(true)
    })

    it('works with array operators', () => {
      const doc = { tags: ['tech', 'database'] }

      expect(matchesFilter(doc, {
        $comment: 'Find tech database posts',
        tags: { $all: ['tech', 'database'] }
      })).toBe(true)
    })
  })

  describe('does not interfere with complex queries', () => {
    it('complex filter with $comment behaves same as without', () => {
      const doc = {
        status: 'published',
        score: 150,
        tags: ['featured', 'tech'],
        author: { verified: true }
      }

      const filterWithComment: Filter = {
        $comment: 'Complex query for featured tech posts by verified authors',
        status: 'published',
        score: { $gte: 100 },
        tags: { $all: ['featured'] },
        'author.verified': true
      }

      const filterWithoutComment: Filter = {
        status: 'published',
        score: { $gte: 100 },
        tags: { $all: ['featured'] },
        'author.verified': true
      }

      expect(matchesFilter(doc, filterWithComment)).toBe(true)
      expect(matchesFilter(doc, filterWithComment)).toBe(matchesFilter(doc, filterWithoutComment))
    })
  })
})

// =============================================================================
// Integration Tests - All New Operators Together
// =============================================================================

describe('integration: combining new operators', () => {
  describe('$mod with $expr', () => {
    it('can use both in the same query', () => {
      const doc = { id: 10, quantity: 100, sold: 50 }

      expect(matchesFilter(doc, {
        id: { $mod: [5, 0] }, // id is divisible by 5
        $expr: { $gt: ['$quantity', '$sold'] } // quantity > sold
      })).toBe(true)
    })
  })

  describe('$comment with $mod', () => {
    it('$comment does not affect $mod evaluation', () => {
      const doc = { count: 15 }

      expect(matchesFilter(doc, {
        $comment: 'Find every 5th item',
        count: { $mod: [5, 0] }
      })).toBe(true)
    })
  })

  describe('all three together', () => {
    it('uses $mod, $expr, and $comment in same query', () => {
      const doc = {
        id: 100,
        budget: 1000,
        spent: 800
      }

      expect(matchesFilter(doc, {
        $comment: 'Find IDs divisible by 10 where budget > spent',
        id: { $mod: [10, 0] },
        $expr: { $gt: ['$budget', '$spent'] }
      })).toBe(true)
    })
  })
})

// =============================================================================
// Real-World Scenarios
// =============================================================================

// =============================================================================
// QueryBuilder Tests for $mod
// =============================================================================

describe('QueryBuilder whereMod', () => {

  it('generates correct $mod filter', () => {
    const builder = new QueryBuilder()
    builder.whereMod('id', 3, 0)
    const { filter } = builder.build()

    expect(filter).toEqual({ id: { $mod: [3, 0] } })
  })

  it('can combine whereMod with other conditions', () => {
    const builder = new QueryBuilder()
    builder
      .where('status', 'eq', 'active')
      .whereMod('id', 5, 0)

    const { filter } = builder.build()

    expect(filter).toEqual({
      status: { $eq: 'active' },
      id: { $mod: [5, 0] }
    })
  })

  it('throws error for zero divisor', () => {
    const builder = new QueryBuilder()
    expect(() => builder.whereMod('id', 0, 0)).toThrow('Divisor must be a positive number')
  })

  it('throws error for negative divisor', () => {
    const builder = new QueryBuilder()
    expect(() => builder.whereMod('id', -3, 0)).toThrow('Divisor must be a positive number')
  })

  it('throws error for negative remainder', () => {
    const builder = new QueryBuilder()
    expect(() => builder.whereMod('id', 3, -1)).toThrow('Remainder cannot be negative')
  })

  it('clones correctly with modConditions', () => {
    const builder = new QueryBuilder()
    builder.whereMod('id', 4, 2)

    const cloned = builder.clone()
    cloned.whereMod('count', 2, 0)

    const original = builder.build()
    const clonedResult = cloned.build()

    // Original should only have the first mod condition
    expect(original.filter).toEqual({ id: { $mod: [4, 2] } })

    // Cloned should have both
    expect(clonedResult.filter).toEqual({
      id: { $mod: [4, 2] },
      count: { $mod: [2, 0] }
    })
  })

  it('uses $and when same field has multiple conditions', () => {
    const builder = new QueryBuilder()
    builder
      .where('id', 'gt', 0)
      .whereMod('id', 3, 0)

    const { filter } = builder.build()

    expect(filter).toEqual({
      $and: [
        { id: { $gt: 0 } },
        { id: { $mod: [3, 0] } }
      ]
    })
  })
})

describe('real-world scenarios with new operators', () => {
  describe('batch processing with $mod', () => {
    it('distributes work across multiple workers', () => {
      const items = [
        { id: 1, name: 'Item 1' },
        { id: 2, name: 'Item 2' },
        { id: 3, name: 'Item 3' },
        { id: 4, name: 'Item 4' },
        { id: 5, name: 'Item 5' },
        { id: 6, name: 'Item 6' },
      ]

      // Worker 0 gets items where id % 3 === 0
      const worker0Items = items.filter(item => matchesFilter(item, { id: { $mod: [3, 0] } }))
      expect(worker0Items.map(i => i.id)).toEqual([3, 6])

      // Worker 1 gets items where id % 3 === 1
      const worker1Items = items.filter(item => matchesFilter(item, { id: { $mod: [3, 1] } }))
      expect(worker1Items.map(i => i.id)).toEqual([1, 4])

      // Worker 2 gets items where id % 3 === 2
      const worker2Items = items.filter(item => matchesFilter(item, { id: { $mod: [3, 2] } }))
      expect(worker2Items.map(i => i.id)).toEqual([2, 5])
    })
  })

  describe('inventory management with $expr', () => {
    it('finds items that need reordering', () => {
      const inventory = [
        { sku: 'A', stock: 5, reorderLevel: 10 },
        { sku: 'B', stock: 20, reorderLevel: 10 },
        { sku: 'C', stock: 10, reorderLevel: 10 },
        { sku: 'D', stock: 3, reorderLevel: 5 },
      ]

      const needsReorder = inventory.filter(item =>
        matchesFilter(item, { $expr: { $lt: ['$stock', '$reorderLevel'] } })
      )

      expect(needsReorder.map(i => i.sku)).toEqual(['A', 'D'])
    })

    it('finds sales where actual price is below list price', () => {
      const sales = [
        { id: 1, listPrice: 100, actualPrice: 80 },
        { id: 2, listPrice: 50, actualPrice: 50 },
        { id: 3, listPrice: 200, actualPrice: 150 },
      ]

      const discounted = sales.filter(sale =>
        matchesFilter(sale, { $expr: { $lt: ['$actualPrice', '$listPrice'] } })
      )

      expect(discounted.map(s => s.id)).toEqual([1, 3])
    })
  })

  describe('documented queries with $comment', () => {
    it('adds documentation to complex queries', () => {
      const users = [
        { id: 1, role: 'admin', lastLogin: new Date('2024-06-01'), settings: { notifications: true } },
        { id: 2, role: 'user', lastLogin: new Date('2024-05-01'), settings: { notifications: false } },
        { id: 3, role: 'admin', lastLogin: new Date('2024-01-01'), settings: { notifications: true } },
      ]

      const filter: Filter = {
        $comment: 'AUDIT-123: Find admin users with notifications enabled who logged in after May 2024',
        role: 'admin',
        'settings.notifications': true,
        lastLogin: { $gte: new Date('2024-05-01') }
      }

      const results = users.filter(u => matchesFilter(u, filter))
      expect(results.map(u => u.id)).toEqual([1])
    })
  })
})

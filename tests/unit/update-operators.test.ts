/**
 * Update Operators Engine Unit Tests
 *
 * Comprehensive tests for MongoDB-style update operators implementation.
 * Tests cover all operators: $set, $unset, $inc, $mul, $min, $max,
 * $push, $pull, $pullAll, $addToSet, $pop, $rename, $currentDate, $bit
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  applyUpdate,
  getField,
  setField,
  unsetField,
  validateUpdate,
} from '../../src/query/update'
import type { UpdateInput } from '../../src/types/update'

// =============================================================================
// Helper Functions Tests
// =============================================================================

describe('Field Access Helpers', () => {
  describe('getField', () => {
    it('gets top-level field', () => {
      const obj = { name: 'John', age: 30 }
      expect(getField(obj, 'name')).toBe('John')
      expect(getField(obj, 'age')).toBe(30)
    })

    it('gets nested field with dot notation', () => {
      const obj = {
        user: {
          profile: {
            name: 'John',
            settings: {
              theme: 'dark',
            },
          },
        },
      }
      expect(getField(obj, 'user.profile.name')).toBe('John')
      expect(getField(obj, 'user.profile.settings.theme')).toBe('dark')
    })

    it('gets array element by index', () => {
      const obj = { items: ['a', 'b', 'c'] }
      expect(getField(obj, 'items.0')).toBe('a')
      expect(getField(obj, 'items.1')).toBe('b')
      expect(getField(obj, 'items.2')).toBe('c')
    })

    it('gets nested field in array element', () => {
      const obj = {
        users: [
          { name: 'John', age: 30 },
          { name: 'Jane', age: 25 },
        ],
      }
      expect(getField(obj, 'users.0.name')).toBe('John')
      expect(getField(obj, 'users.1.age')).toBe(25)
    })

    it('returns undefined for non-existent field', () => {
      const obj = { name: 'John' }
      expect(getField(obj, 'age')).toBeUndefined()
      expect(getField(obj, 'a.b.c')).toBeUndefined()
    })

    it('returns undefined for null/undefined object', () => {
      expect(getField(null, 'name')).toBeUndefined()
      expect(getField(undefined, 'name')).toBeUndefined()
    })

    it('returns undefined when navigating through primitive', () => {
      const obj = { name: 'John' }
      expect(getField(obj, 'name.length')).toBeUndefined()
    })
  })

  describe('setField', () => {
    it('sets top-level field', () => {
      const obj = { name: 'John' }
      const result = setField(obj, 'age', 30)
      expect(result).toEqual({ name: 'John', age: 30 })
      // Original unchanged
      expect(obj).toEqual({ name: 'John' })
    })

    it('updates existing top-level field', () => {
      const obj = { name: 'John', age: 30 }
      const result = setField(obj, 'name', 'Jane')
      expect(result).toEqual({ name: 'Jane', age: 30 })
    })

    it('sets nested field with dot notation', () => {
      const obj = {
        user: {
          name: 'John',
        },
      }
      const result = setField(obj, 'user.age', 30)
      expect(result).toEqual({
        user: { name: 'John', age: 30 },
      })
    })

    it('creates nested structure if not exists', () => {
      const obj = { name: 'John' }
      const result = setField(obj, 'address.city', 'NYC')
      expect(result).toEqual({
        name: 'John',
        address: { city: 'NYC' },
      })
    })

    it('creates deeply nested structure', () => {
      const obj = {}
      const result = setField(obj, 'a.b.c.d', 'value')
      expect(result).toEqual({
        a: { b: { c: { d: 'value' } } },
      })
    })

    it('sets array element by index', () => {
      const obj = { items: ['a', 'b', 'c'] }
      const result = setField(obj, 'items.1', 'x')
      expect(result.items).toEqual(['a', 'x', 'c'])
    })

    it('preserves immutability', () => {
      const obj = { nested: { value: 1 } }
      const result = setField(obj, 'nested.value', 2)
      expect(obj.nested.value).toBe(1)
      expect(result.nested.value).toBe(2)
    })
  })

  describe('unsetField', () => {
    it('removes top-level field', () => {
      const obj = { name: 'John', age: 30 }
      const result = unsetField(obj, 'age')
      expect(result).toEqual({ name: 'John' })
      expect('age' in result).toBe(false)
    })

    it('removes nested field', () => {
      const obj = {
        user: {
          name: 'John',
          age: 30,
        },
      }
      const result = unsetField(obj, 'user.age')
      expect(result).toEqual({ user: { name: 'John' } })
    })

    it('handles non-existent field gracefully', () => {
      const obj = { name: 'John' }
      const result = unsetField(obj, 'nonexistent')
      expect(result).toEqual({ name: 'John' })
    })

    it('handles non-existent nested path gracefully', () => {
      const obj = { name: 'John' }
      const result = unsetField(obj, 'a.b.c')
      expect(result).toEqual({ name: 'John' })
    })

    it('preserves immutability', () => {
      const obj = { name: 'John', age: 30 }
      const result = unsetField(obj, 'age')
      expect(obj).toEqual({ name: 'John', age: 30 })
      expect(result).toEqual({ name: 'John' })
    })
  })
})

// =============================================================================
// $set Operator Tests
// =============================================================================

describe('$set Operator', () => {
  it('sets single field', () => {
    const doc = { name: 'John', age: 30 }
    const result = applyUpdate(doc, { $set: { name: 'Jane' } })
    expect(result).toEqual({ name: 'Jane', age: 30 })
  })

  it('sets multiple fields', () => {
    const doc = { name: 'John', age: 30 }
    const result = applyUpdate(doc, { $set: { name: 'Jane', age: 25 } })
    expect(result).toEqual({ name: 'Jane', age: 25 })
  })

  it('adds new field', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $set: { age: 30 } })
    expect(result).toEqual({ name: 'John', age: 30 })
  })

  it('sets nested field with dot notation', () => {
    const doc = { user: { name: 'John' } }
    const result = applyUpdate(doc, { $set: { 'user.name': 'Jane' } })
    expect(result.user.name).toBe('Jane')
  })

  it('creates nested structure when setting nested field', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $set: { 'address.city': 'NYC' } })
    expect(result).toEqual({
      name: 'John',
      address: { city: 'NYC' },
    })
  })

  it('sets deeply nested field', () => {
    const doc = {}
    const result = applyUpdate(doc, { $set: { 'a.b.c.d': 'value' } })
    expect(result).toEqual({ a: { b: { c: { d: 'value' } } } })
  })

  it('sets array field', () => {
    const doc = { tags: ['old'] }
    const result = applyUpdate(doc, { $set: { tags: ['new', 'tags'] } })
    expect(result.tags).toEqual(['new', 'tags'])
  })

  it('sets object field', () => {
    const doc = { data: null }
    const result = applyUpdate(doc, { $set: { data: { key: 'value' } } })
    expect(result.data).toEqual({ key: 'value' })
  })

  it('sets null value', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $set: { name: null } })
    expect(result.name).toBeNull()
  })

  it('preserves other fields', () => {
    const doc = { a: 1, b: 2, c: 3 }
    const result = applyUpdate(doc, { $set: { b: 20 } })
    expect(result).toEqual({ a: 1, b: 20, c: 3 })
  })
})

// =============================================================================
// $unset Operator Tests
// =============================================================================

describe('$unset Operator', () => {
  it('removes single field', () => {
    const doc = { name: 'John', age: 30 }
    const result = applyUpdate(doc, { $unset: { age: '' } })
    expect(result).toEqual({ name: 'John' })
    expect('age' in result).toBe(false)
  })

  it('removes multiple fields', () => {
    const doc = { name: 'John', age: 30, city: 'NYC' }
    const result = applyUpdate(doc, { $unset: { age: '', city: 1 } })
    expect(result).toEqual({ name: 'John' })
  })

  it('removes nested field', () => {
    const doc = { user: { name: 'John', age: 30 } }
    const result = applyUpdate(doc, { $unset: { 'user.age': '' } })
    expect(result).toEqual({ user: { name: 'John' } })
  })

  it('handles non-existent field gracefully', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $unset: { nonexistent: '' } })
    expect(result).toEqual({ name: 'John' })
  })

  it('accepts various truthy values', () => {
    const doc = { a: 1, b: 2, c: 3 }
    const result = applyUpdate(doc, { $unset: { a: 1, b: true as unknown as '', c: '' } })
    expect(result).toEqual({})
  })
})

// =============================================================================
// $inc Operator Tests
// =============================================================================

describe('$inc Operator', () => {
  it('increments field by positive value', () => {
    const doc = { count: 5 }
    const result = applyUpdate(doc, { $inc: { count: 1 } })
    expect(result.count).toBe(6)
  })

  it('increments field by negative value (decrements)', () => {
    const doc = { count: 5 }
    const result = applyUpdate(doc, { $inc: { count: -2 } })
    expect(result.count).toBe(3)
  })

  it('increments multiple fields', () => {
    const doc = { a: 1, b: 2 }
    const result = applyUpdate(doc, { $inc: { a: 10, b: 20 } })
    expect(result).toEqual({ a: 11, b: 22 })
  })

  it('creates field if not exists (defaults to 0)', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $inc: { count: 5 } })
    expect(result.count).toBe(5)
  })

  it('increments nested field', () => {
    const doc = { stats: { views: 100 } }
    const result = applyUpdate(doc, { $inc: { 'stats.views': 1 } })
    expect(result.stats.views).toBe(101)
  })

  it('handles floating point numbers', () => {
    const doc = { value: 1.5 }
    const result = applyUpdate(doc, { $inc: { value: 0.5 } })
    expect(result.value).toBe(2.0)
  })

  it('preserves other fields', () => {
    const doc = { count: 1, name: 'John' }
    const result = applyUpdate(doc, { $inc: { count: 1 } })
    expect(result.name).toBe('John')
  })
})

// =============================================================================
// $mul Operator Tests
// =============================================================================

describe('$mul Operator', () => {
  it('multiplies field by value', () => {
    const doc = { price: 10 }
    const result = applyUpdate(doc, { $mul: { price: 2 } })
    expect(result.price).toBe(20)
  })

  it('multiplies by fractional value', () => {
    const doc = { price: 100 }
    const result = applyUpdate(doc, { $mul: { price: 0.5 } })
    expect(result.price).toBe(50)
  })

  it('multiplies multiple fields', () => {
    const doc = { a: 2, b: 3 }
    const result = applyUpdate(doc, { $mul: { a: 3, b: 4 } })
    expect(result).toEqual({ a: 6, b: 12 })
  })

  it('creates field if not exists (defaults to 0)', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $mul: { value: 5 } })
    expect(result.value).toBe(0) // 0 * 5 = 0
  })

  it('multiplies nested field', () => {
    const doc = { stats: { score: 10 } }
    const result = applyUpdate(doc, { $mul: { 'stats.score': 2 } })
    expect(result.stats.score).toBe(20)
  })

  it('handles negative multiplier', () => {
    const doc = { value: 5 }
    const result = applyUpdate(doc, { $mul: { value: -2 } })
    expect(result.value).toBe(-10)
  })
})

// =============================================================================
// $min Operator Tests
// =============================================================================

describe('$min Operator', () => {
  it('sets field to minimum when new value is smaller', () => {
    const doc = { score: 100 }
    const result = applyUpdate(doc, { $min: { score: 50 } })
    expect(result.score).toBe(50)
  })

  it('keeps existing value when it is smaller', () => {
    const doc = { score: 50 }
    const result = applyUpdate(doc, { $min: { score: 100 } })
    expect(result.score).toBe(50)
  })

  it('sets field when it does not exist', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $min: { score: 50 } })
    expect(result.score).toBe(50)
  })

  it('compares strings', () => {
    const doc = { name: 'banana' }
    const result = applyUpdate(doc, { $min: { name: 'apple' } })
    expect(result.name).toBe('apple')
  })

  it('compares dates', () => {
    const earlier = new Date('2020-01-01')
    const later = new Date('2021-01-01')
    const doc = { date: later }
    const result = applyUpdate(doc, { $min: { date: earlier } })
    expect(result.date).toEqual(earlier)
  })

  it('handles nested field', () => {
    const doc = { stats: { low: 100 } }
    const result = applyUpdate(doc, { $min: { 'stats.low': 50 } })
    expect(result.stats.low).toBe(50)
  })
})

// =============================================================================
// $max Operator Tests
// =============================================================================

describe('$max Operator', () => {
  it('sets field to maximum when new value is larger', () => {
    const doc = { score: 50 }
    const result = applyUpdate(doc, { $max: { score: 100 } })
    expect(result.score).toBe(100)
  })

  it('keeps existing value when it is larger', () => {
    const doc = { score: 100 }
    const result = applyUpdate(doc, { $max: { score: 50 } })
    expect(result.score).toBe(100)
  })

  it('sets field when it does not exist', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $max: { score: 50 } })
    expect(result.score).toBe(50)
  })

  it('compares strings', () => {
    const doc = { name: 'apple' }
    const result = applyUpdate(doc, { $max: { name: 'banana' } })
    expect(result.name).toBe('banana')
  })

  it('compares dates', () => {
    const earlier = new Date('2020-01-01')
    const later = new Date('2021-01-01')
    const doc = { date: earlier }
    const result = applyUpdate(doc, { $max: { date: later } })
    expect(result.date).toEqual(later)
  })

  it('handles nested field', () => {
    const doc = { stats: { high: 50 } }
    const result = applyUpdate(doc, { $max: { 'stats.high': 100 } })
    expect(result.stats.high).toBe(100)
  })
})

// =============================================================================
// $push Operator Tests
// =============================================================================

describe('$push Operator', () => {
  it('pushes single value to array', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $push: { tags: 'c' } })
    expect(result.tags).toEqual(['a', 'b', 'c'])
  })

  it('creates array if field does not exist', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $push: { tags: 'first' } })
    expect(result.tags).toEqual(['first'])
  })

  it('pushes object to array', () => {
    const doc = { items: [{ id: 1 }] }
    const result = applyUpdate(doc, { $push: { items: { id: 2 } } })
    expect(result.items).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('pushes null to array', () => {
    const doc = { values: [1, 2] }
    const result = applyUpdate(doc, { $push: { values: null } })
    expect(result.values).toEqual([1, 2, null])
  })

  describe('with $each modifier', () => {
    it('pushes multiple values', () => {
      const doc = { tags: ['a'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['b', 'c', 'd'] } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c', 'd'])
    })

    it('pushes empty array (no change)', () => {
      const doc = { tags: ['a'] }
      const result = applyUpdate(doc, { $push: { tags: { $each: [] } } })
      expect(result.tags).toEqual(['a'])
    })
  })

  describe('with $position modifier', () => {
    it('inserts at beginning', () => {
      const doc = { tags: ['b', 'c'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['a'], $position: 0 } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c'])
    })

    it('inserts at middle', () => {
      const doc = { tags: ['a', 'c'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['b'], $position: 1 } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c'])
    })

    it('inserts multiple at position', () => {
      const doc = { tags: ['a', 'd'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['b', 'c'], $position: 1 } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c', 'd'])
    })
  })

  describe('with $slice modifier', () => {
    it('keeps first N elements', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['c', 'd', 'e'], $slice: 3 } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c'])
    })

    it('keeps last N elements with negative slice', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['c', 'd', 'e'], $slice: -3 } },
      })
      expect(result.tags).toEqual(['c', 'd', 'e'])
    })

    it('empties array with $slice: 0', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyUpdate(doc, {
        $push: { tags: { $each: ['c'], $slice: 0 } },
      })
      expect(result.tags).toEqual([])
    })
  })

  describe('with $sort modifier', () => {
    it('sorts ascending', () => {
      const doc = { scores: [3, 1] }
      const result = applyUpdate(doc, {
        $push: { scores: { $each: [4, 2], $sort: 1 } },
      })
      expect(result.scores).toEqual([1, 2, 3, 4])
    })

    it('sorts descending', () => {
      const doc = { scores: [1, 3] }
      const result = applyUpdate(doc, {
        $push: { scores: { $each: [4, 2], $sort: -1 } },
      })
      expect(result.scores).toEqual([4, 3, 2, 1])
    })

    it('sorts by field', () => {
      const doc = {
        items: [{ name: 'b', score: 2 }],
      }
      const result = applyUpdate(doc, {
        $push: {
          items: {
            $each: [{ name: 'a', score: 1 }, { name: 'c', score: 3 }],
            $sort: { score: 1 },
          },
        },
      })
      expect(result.items).toEqual([
        { name: 'a', score: 1 },
        { name: 'b', score: 2 },
        { name: 'c', score: 3 },
      ])
    })

    it('sorts by field descending', () => {
      const doc = {
        items: [{ name: 'b', score: 2 }],
      }
      const result = applyUpdate(doc, {
        $push: {
          items: {
            $each: [{ name: 'a', score: 1 }],
            $sort: { score: -1 },
          },
        },
      })
      expect(result.items).toEqual([
        { name: 'b', score: 2 },
        { name: 'a', score: 1 },
      ])
    })
  })

  describe('combined modifiers', () => {
    it('combines $each, $sort, and $slice', () => {
      const doc = { scores: [50] }
      const result = applyUpdate(doc, {
        $push: {
          scores: {
            $each: [90, 70, 30, 10],
            $sort: -1,
            $slice: 3,
          },
        },
      })
      expect(result.scores).toEqual([90, 70, 50])
    })
  })

  it('handles nested array field', () => {
    const doc = { user: { tags: ['a'] } }
    const result = applyUpdate(doc, { $push: { 'user.tags': 'b' } })
    expect(result.user.tags).toEqual(['a', 'b'])
  })
})

// =============================================================================
// $pull Operator Tests
// =============================================================================

describe('$pull Operator', () => {
  it('removes matching value from array', () => {
    const doc = { tags: ['a', 'b', 'c'] }
    const result = applyUpdate(doc, { $pull: { tags: 'b' } })
    expect(result.tags).toEqual(['a', 'c'])
  })

  it('removes all occurrences of value', () => {
    const doc = { values: [1, 2, 1, 3, 1] }
    const result = applyUpdate(doc, { $pull: { values: 1 } })
    expect(result.values).toEqual([2, 3])
  })

  it('handles no matches gracefully', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $pull: { tags: 'c' } })
    expect(result.tags).toEqual(['a', 'b'])
  })

  it('handles non-existent field', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $pull: { tags: 'a' } })
    // MongoDB behavior: $pull on non-existent field is a no-op
    expect(result.tags).toBeUndefined()
  })

  it('pulls with comparison operator', () => {
    const doc = { scores: [10, 50, 80, 100] }
    const result = applyUpdate(doc, {
      $pull: { scores: { $gte: 50 } },
    })
    expect(result.scores).toEqual([10])
  })

  it('pulls with $in operator', () => {
    const doc = { tags: ['a', 'b', 'c', 'd'] }
    const result = applyUpdate(doc, {
      $pull: { tags: { $in: ['b', 'd'] } },
    })
    expect(result.tags).toEqual(['a', 'c'])
  })

  it('pulls objects matching filter', () => {
    const doc = {
      items: [
        { name: 'a', value: 1 },
        { name: 'b', value: 2 },
        { name: 'c', value: 3 },
      ],
    }
    const result = applyUpdate(doc, {
      $pull: { items: { value: { $gte: 2 } } },
    })
    expect(result.items).toEqual([{ name: 'a', value: 1 }])
  })

  it('pulls by exact object match', () => {
    const doc = {
      items: [
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
      ],
    }
    const result = applyUpdate(doc, {
      $pull: { items: { id: 1, name: 'a' } },
    })
    expect(result.items).toEqual([{ id: 2, name: 'b' }])
  })

  it('handles nested array field', () => {
    const doc = { user: { tags: ['a', 'b', 'c'] } }
    const result = applyUpdate(doc, { $pull: { 'user.tags': 'b' } })
    expect(result.user.tags).toEqual(['a', 'c'])
  })
})

// =============================================================================
// $pullAll Operator Tests
// =============================================================================

describe('$pullAll Operator', () => {
  it('removes all specified values', () => {
    const doc = { tags: ['a', 'b', 'c', 'd'] }
    const result = applyUpdate(doc, { $pullAll: { tags: ['b', 'd'] } })
    expect(result.tags).toEqual(['a', 'c'])
  })

  it('removes all occurrences of each value', () => {
    const doc = { values: [1, 2, 1, 3, 2, 1] }
    const result = applyUpdate(doc, { $pullAll: { values: [1, 2] } })
    expect(result.values).toEqual([3])
  })

  it('handles no matches', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $pullAll: { tags: ['x', 'y'] } })
    expect(result.tags).toEqual(['a', 'b'])
  })

  it('handles empty values array', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $pullAll: { tags: [] } })
    expect(result.tags).toEqual(['a', 'b'])
  })

  it('handles non-existent field', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $pullAll: { tags: ['a'] } })
    expect(result.tags).toEqual([])
  })
})

// =============================================================================
// $addToSet Operator Tests
// =============================================================================

describe('$addToSet Operator', () => {
  it('adds value if not present', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $addToSet: { tags: 'c' } })
    expect(result.tags).toEqual(['a', 'b', 'c'])
  })

  it('does not add duplicate value', () => {
    const doc = { tags: ['a', 'b'] }
    const result = applyUpdate(doc, { $addToSet: { tags: 'a' } })
    expect(result.tags).toEqual(['a', 'b'])
  })

  it('creates array if field does not exist', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $addToSet: { tags: 'first' } })
    expect(result.tags).toEqual(['first'])
  })

  it('adds object to set', () => {
    const doc = { items: [{ id: 1 }] }
    const result = applyUpdate(doc, { $addToSet: { items: { id: 2 } } })
    expect(result.items).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('does not add duplicate object', () => {
    const doc = { items: [{ id: 1 }] }
    const result = applyUpdate(doc, { $addToSet: { items: { id: 1 } } })
    expect(result.items).toEqual([{ id: 1 }])
  })

  describe('with $each modifier', () => {
    it('adds multiple unique values', () => {
      const doc = { tags: ['a'] }
      const result = applyUpdate(doc, {
        $addToSet: { tags: { $each: ['b', 'c', 'a'] } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c'])
    })

    it('filters out all duplicates', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyUpdate(doc, {
        $addToSet: { tags: { $each: ['a', 'b', 'c'] } },
      })
      expect(result.tags).toEqual(['a', 'b', 'c'])
    })
  })

  it('handles nested field', () => {
    const doc = { user: { tags: ['a'] } }
    const result = applyUpdate(doc, { $addToSet: { 'user.tags': 'b' } })
    expect(result.user.tags).toEqual(['a', 'b'])
  })
})

// =============================================================================
// $pop Operator Tests
// =============================================================================

describe('$pop Operator', () => {
  it('removes last element with $pop: 1', () => {
    const doc = { items: ['a', 'b', 'c'] }
    const result = applyUpdate(doc, { $pop: { items: 1 } })
    expect(result.items).toEqual(['a', 'b'])
  })

  it('removes first element with $pop: -1', () => {
    const doc = { items: ['a', 'b', 'c'] }
    const result = applyUpdate(doc, { $pop: { items: -1 } })
    expect(result.items).toEqual(['b', 'c'])
  })

  it('handles empty array', () => {
    const doc = { items: [] }
    const result = applyUpdate(doc, { $pop: { items: 1 } })
    expect(result.items).toEqual([])
  })

  it('handles single element array', () => {
    const doc = { items: ['only'] }
    const result1 = applyUpdate(doc, { $pop: { items: 1 } })
    expect(result1.items).toEqual([])

    const result2 = applyUpdate(doc, { $pop: { items: -1 } })
    expect(result2.items).toEqual([])
  })

  it('creates empty array if field does not exist', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $pop: { items: 1 } })
    expect(result.items).toEqual([])
  })

  it('handles nested field', () => {
    const doc = { user: { items: ['a', 'b', 'c'] } }
    const result = applyUpdate(doc, { $pop: { 'user.items': 1 } })
    expect(result.user.items).toEqual(['a', 'b'])
  })
})

// =============================================================================
// $rename Operator Tests
// =============================================================================

describe('$rename Operator', () => {
  it('renames field', () => {
    const doc = { oldName: 'value' }
    const result = applyUpdate(doc, { $rename: { oldName: 'newName' } })
    expect(result.newName).toBe('value')
    expect('oldName' in result).toBe(false)
  })

  it('renames multiple fields', () => {
    const doc = { a: 1, b: 2 }
    const result = applyUpdate(doc, { $rename: { a: 'x', b: 'y' } })
    expect(result).toEqual({ x: 1, y: 2 })
  })

  it('handles non-existent field (no-op)', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $rename: { nonexistent: 'newName' } })
    expect(result).toEqual({ name: 'John' })
  })

  it('renames nested field', () => {
    const doc = { user: { firstName: 'John' } }
    const result = applyUpdate(doc, {
      $rename: { 'user.firstName': 'user.name' },
    })
    expect(result.user.name).toBe('John')
    expect('firstName' in result.user).toBe(false)
  })

  it('can move field to different path', () => {
    const doc = { source: 'value', target: {} }
    const result = applyUpdate(doc, { $rename: { source: 'target.value' } })
    expect(result.target).toEqual({ value: 'value' })
    expect('source' in result).toBe(false)
  })
})

// =============================================================================
// $currentDate Operator Tests
// =============================================================================

describe('$currentDate Operator', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-15T12:00:00.000Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('sets field to current date with true', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $currentDate: { updatedAt: true } })
    expect(result.updatedAt).toBeInstanceOf(Date)
    expect((result.updatedAt as Date).toISOString()).toBe(
      '2024-01-15T12:00:00.000Z'
    )
  })

  it('sets field to Date with $type: date', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, {
      $currentDate: { updatedAt: { $type: 'date' } },
    })
    expect(result.updatedAt).toBeInstanceOf(Date)
  })

  it('sets field to timestamp with $type: timestamp', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, {
      $currentDate: { updatedAt: { $type: 'timestamp' } },
    })
    expect(typeof result.updatedAt).toBe('number')
    expect(result.updatedAt).toBe(Date.now())
  })

  it('sets multiple date fields', () => {
    const doc = {}
    const result = applyUpdate(doc, {
      $currentDate: { createdAt: true, modifiedAt: true },
    })
    expect(result.createdAt).toBeInstanceOf(Date)
    expect(result.modifiedAt).toBeInstanceOf(Date)
  })

  it('sets nested date field', () => {
    const doc = { audit: {} }
    const result = applyUpdate(doc, {
      $currentDate: { 'audit.timestamp': true },
    })
    expect(result.audit.timestamp).toBeInstanceOf(Date)
  })
})

// =============================================================================
// $setOnInsert Operator Tests
// =============================================================================

describe('$setOnInsert Operator', () => {
  it('sets fields when isInsert is true', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(
      doc,
      { $setOnInsert: { createdAt: 'now', version: 1 } },
      { isInsert: true }
    )
    expect(result.createdAt).toBe('now')
    expect(result.version).toBe(1)
  })

  it('does not set fields when isInsert is false', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(
      doc,
      { $setOnInsert: { createdAt: 'now', version: 1 } },
      { isInsert: false }
    )
    expect(result.createdAt).toBeUndefined()
    expect(result.version).toBeUndefined()
  })

  it('does not set fields when options not provided', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, {
      $setOnInsert: { createdAt: 'now' },
    })
    expect(result.createdAt).toBeUndefined()
  })

  it('combines with other operators on insert', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(
      doc,
      {
        $set: { name: 'Jane' },
        $setOnInsert: { version: 1 },
      },
      { isInsert: true }
    )
    expect(result.name).toBe('Jane')
    expect(result.version).toBe(1)
  })
})

// =============================================================================
// $bit Operator Tests
// =============================================================================

describe('$bit Operator', () => {
  it('performs bitwise AND', () => {
    const doc = { flags: 0b1111 } // 15
    const result = applyUpdate(doc, { $bit: { flags: { and: 0b1010 } } }) // AND with 10
    expect(result.flags).toBe(0b1010) // 10
  })

  it('performs bitwise OR', () => {
    const doc = { flags: 0b0101 } // 5
    const result = applyUpdate(doc, { $bit: { flags: { or: 0b1010 } } }) // OR with 10
    expect(result.flags).toBe(0b1111) // 15
  })

  it('performs bitwise XOR', () => {
    const doc = { flags: 0b1111 } // 15
    const result = applyUpdate(doc, { $bit: { flags: { xor: 0b1010 } } }) // XOR with 10
    expect(result.flags).toBe(0b0101) // 5
  })

  it('performs multiple bit operations in sequence', () => {
    const doc = { flags: 0b1100 } // 12
    const result = applyUpdate(doc, {
      $bit: { flags: { and: 0b1010, or: 0b0001 } },
    })
    // First AND: 1100 & 1010 = 1000 (8)
    // Then OR: 1000 | 0001 = 1001 (9)
    expect(result.flags).toBe(0b1001) // 9
  })

  it('creates field with default 0 if not exists', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $bit: { flags: { or: 0b0101 } } })
    expect(result.flags).toBe(0b0101) // 5
  })

  it('handles nested field', () => {
    const doc = { settings: { permissions: 0b1111 } }
    const result = applyUpdate(doc, {
      $bit: { 'settings.permissions': { and: 0b1100 } },
    })
    expect(result.settings.permissions).toBe(0b1100)
  })
})

// =============================================================================
// Combined Operators Tests
// =============================================================================

describe('Combined Operators', () => {
  it('applies multiple different operators', () => {
    const doc = {
      name: 'John',
      count: 5,
      tags: ['a'],
      toRemove: 'bye',
    }
    const result = applyUpdate(doc, {
      $set: { name: 'Jane' },
      $inc: { count: 1 },
      $push: { tags: 'b' },
      $unset: { toRemove: '' },
    })
    expect(result).toEqual({
      name: 'Jane',
      count: 6,
      tags: ['a', 'b'],
    })
  })

  it('applies operators in correct order', () => {
    // This tests that $set happens before $inc on different fields
    const doc = { a: 1, b: 2 }
    const result = applyUpdate(doc, {
      $set: { a: 10 },
      $inc: { b: 5 },
    })
    expect(result).toEqual({ a: 10, b: 7 })
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('validateUpdate', () => {
  it('passes for valid single operator', () => {
    expect(() => validateUpdate({ $set: { name: 'John' } })).not.toThrow()
  })

  it('passes for multiple operators on different fields', () => {
    expect(() =>
      validateUpdate({
        $set: { name: 'John' },
        $inc: { count: 1 },
      })
    ).not.toThrow()
  })

  it('throws for same field in multiple operators', () => {
    expect(() =>
      validateUpdate({
        $set: { count: 10 },
        $inc: { count: 1 },
      })
    ).toThrow(/Conflicting operators.*count/)
  })

  it('throws for $rename conflict with old field name', () => {
    expect(() =>
      validateUpdate({
        $set: { oldName: 'value' },
        $rename: { oldName: 'newName' },
      })
    ).toThrow(/Conflicting operators.*oldName/)
  })

  it('throws for $rename conflict with new field name', () => {
    expect(() =>
      validateUpdate({
        $set: { newName: 'value' },
        $rename: { oldName: 'newName' },
      })
    ).toThrow(/Conflicting operators.*newName/)
  })
})

// =============================================================================
// Edge Cases and Immutability Tests
// =============================================================================

describe('Edge Cases', () => {
  it('handles empty update object', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, {})
    expect(result).toEqual({ name: 'John' })
  })

  it('preserves original document (immutability)', () => {
    const doc = { name: 'John', nested: { value: 1 } }
    const result = applyUpdate(doc, { $set: { name: 'Jane', 'nested.value': 2 } })

    // Original unchanged
    expect(doc.name).toBe('John')
    expect(doc.nested.value).toBe(1)

    // Result has changes
    expect(result.name).toBe('Jane')
    expect(result.nested.value).toBe(2)
  })

  it('handles undefined values in $set', () => {
    const doc = { name: 'John' }
    const result = applyUpdate(doc, { $set: { value: undefined } })
    expect(result.value).toBeUndefined()
  })

  it('handles special characters in field names', () => {
    const doc = { 'field-with-dash': 1 }
    const result = applyUpdate(doc, { $inc: { 'field-with-dash': 1 } })
    expect(result['field-with-dash']).toBe(2)
  })

  it('handles numeric string keys in arrays', () => {
    const doc = { items: ['a', 'b', 'c'] }
    const result = applyUpdate(doc, { $set: { 'items.1': 'x' } })
    expect(result.items).toEqual(['a', 'x', 'c'])
  })

  it('handles deeply nested updates', () => {
    const doc = { a: { b: { c: { d: { e: 1 } } } } }
    const result = applyUpdate(doc, { $inc: { 'a.b.c.d.e': 1 } })
    expect(result.a.b.c.d.e).toBe(2)
  })
})

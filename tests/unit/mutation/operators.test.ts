/**
 * Mutation Operators Tests
 *
 * Tests for the MongoDB-style update operators implementation.
 */

import { describe, it, expect } from 'vitest'
import {
  applyOperators,
  getField,
  setField,
  unsetField,
  validateUpdateOperators,
  isUnsafePath,
  validatePath,
} from '../../../src/mutation/operators'

// =============================================================================
// Field Access Tests
// =============================================================================

describe('Field Access Helpers', () => {
  describe('getField', () => {
    it('gets top-level fields', () => {
      const obj = { name: 'John', age: 30 }
      expect(getField(obj, 'name')).toBe('John')
      expect(getField(obj, 'age')).toBe(30)
    })

    it('gets nested fields with dot notation', () => {
      const obj = { user: { profile: { name: 'John' } } }
      expect(getField(obj, 'user.profile.name')).toBe('John')
    })

    it('gets array elements', () => {
      const obj = { items: ['a', 'b', 'c'] }
      expect(getField(obj, 'items.0')).toBe('a')
      expect(getField(obj, 'items.2')).toBe('c')
    })

    it('returns undefined for non-existent paths', () => {
      const obj = { name: 'John' }
      expect(getField(obj, 'age')).toBeUndefined()
      expect(getField(obj, 'user.profile.name')).toBeUndefined()
    })

    it('handles null and undefined objects', () => {
      expect(getField(null, 'name')).toBeUndefined()
      expect(getField(undefined, 'name')).toBeUndefined()
    })
  })

  describe('setField', () => {
    it('sets top-level fields', () => {
      const obj = { name: 'John' }
      const result = setField(obj, 'age', 30)
      expect(result).toEqual({ name: 'John', age: 30 })
      // Original should be unchanged (immutable)
      expect(obj).toEqual({ name: 'John' })
    })

    it('sets nested fields with dot notation', () => {
      const obj = { user: { name: 'John' } }
      const result = setField(obj, 'user.profile.email', 'john@example.com')
      expect(result.user.profile.email).toBe('john@example.com')
    })

    it('creates intermediate objects', () => {
      const obj = {}
      const result = setField(obj, 'a.b.c', 'deep')
      expect(result).toEqual({ a: { b: { c: 'deep' } } })
    })

    it('sets array elements', () => {
      const obj = { items: ['a', 'b', 'c'] }
      const result = setField(obj, 'items.1', 'x')
      expect(result.items).toEqual(['a', 'x', 'c'])
    })

    it('creates arrays for numeric paths', () => {
      const obj = {}
      const result = setField(obj, 'items.0', 'first')
      expect(result.items).toEqual(['first'])
    })
  })

  describe('unsetField', () => {
    it('removes top-level fields', () => {
      const obj = { name: 'John', age: 30 }
      const result = unsetField(obj, 'age')
      expect(result).toEqual({ name: 'John' })
      // Original should be unchanged
      expect(obj).toEqual({ name: 'John', age: 30 })
    })

    it('removes nested fields', () => {
      const obj = { user: { name: 'John', email: 'john@example.com' } }
      const result = unsetField(obj, 'user.email')
      expect(result.user).toEqual({ name: 'John' })
    })

    it('handles non-existent paths gracefully', () => {
      const obj = { name: 'John' }
      const result = unsetField(obj, 'age')
      expect(result).toEqual({ name: 'John' })
    })
  })
})

// =============================================================================
// Operator Tests
// =============================================================================

describe('applyOperators', () => {
  describe('$set operator', () => {
    it('sets scalar fields', () => {
      const doc = { name: 'John', age: 30 }
      const result = applyOperators(doc, { $set: { name: 'Jane', status: 'active' } })
      expect(result.document).toEqual({ name: 'Jane', age: 30, status: 'active' })
      expect(result.modifiedFields).toContain('name')
      expect(result.modifiedFields).toContain('status')
    })

    it('sets nested fields with dot notation', () => {
      const doc = { user: { name: 'John' } }
      const result = applyOperators(doc, { $set: { 'user.age': 30 } })
      expect(result.document.user.age).toBe(30)
    })
  })

  describe('$unset operator', () => {
    it('removes fields', () => {
      const doc = { name: 'John', age: 30, status: 'active' }
      const result = applyOperators(doc, { $unset: { status: '' } })
      expect(result.document).toEqual({ name: 'John', age: 30 })
    })
  })

  describe('$inc operator', () => {
    it('increments numeric fields', () => {
      const doc = { count: 5 }
      const result = applyOperators(doc, { $inc: { count: 3 } })
      expect(result.document.count).toBe(8)
    })

    it('creates field if not exists', () => {
      const doc = {}
      const result = applyOperators(doc, { $inc: { count: 1 } })
      expect(result.document.count).toBe(1)
    })

    it('handles negative increments', () => {
      const doc = { score: 100 }
      const result = applyOperators(doc, { $inc: { score: -25 } })
      expect(result.document.score).toBe(75)
    })

    it('throws on non-numeric fields', () => {
      const doc = { name: 'John' }
      expect(() => applyOperators(doc, { $inc: { name: 1 } as any })).toThrow()
    })
  })

  describe('$mul operator', () => {
    it('multiplies numeric fields', () => {
      const doc = { price: 10 }
      const result = applyOperators(doc, { $mul: { price: 1.5 } })
      expect(result.document.price).toBe(15)
    })

    it('multiplies by zero', () => {
      const doc = { value: 100 }
      const result = applyOperators(doc, { $mul: { value: 0 } })
      expect(result.document.value).toBe(0)
    })
  })

  describe('$min operator', () => {
    it('sets to minimum value', () => {
      const doc = { score: 50 }
      const result = applyOperators(doc, { $min: { score: 30 } })
      expect(result.document.score).toBe(30)
    })

    it('keeps current if already minimum', () => {
      const doc = { score: 10 }
      const result = applyOperators(doc, { $min: { score: 30 } })
      expect(result.document.score).toBe(10)
    })

    it('creates field if not exists', () => {
      const doc = {}
      const result = applyOperators(doc, { $min: { score: 30 } })
      expect(result.document.score).toBe(30)
    })
  })

  describe('$max operator', () => {
    it('sets to maximum value', () => {
      const doc = { score: 50 }
      const result = applyOperators(doc, { $max: { score: 100 } })
      expect(result.document.score).toBe(100)
    })

    it('keeps current if already maximum', () => {
      const doc = { score: 100 }
      const result = applyOperators(doc, { $max: { score: 50 } })
      expect(result.document.score).toBe(100)
    })
  })

  describe('$push operator', () => {
    it('appends to array', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyOperators(doc, { $push: { tags: 'c' } })
      expect(result.document.tags).toEqual(['a', 'b', 'c'])
    })

    it('creates array if not exists', () => {
      const doc = {}
      const result = applyOperators(doc, { $push: { tags: 'first' } })
      expect(result.document.tags).toEqual(['first'])
    })

    it('supports $each modifier', () => {
      const doc = { tags: ['a'] }
      const result = applyOperators(doc, { $push: { tags: { $each: ['b', 'c'] } } })
      expect(result.document.tags).toEqual(['a', 'b', 'c'])
    })

    it('supports $position modifier', () => {
      const doc = { items: ['first', 'last'] }
      const result = applyOperators(doc, { $push: { items: { $each: ['middle'], $position: 1 } } })
      expect(result.document.items).toEqual(['first', 'middle', 'last'])
    })

    it('supports $slice modifier (positive)', () => {
      const doc = { items: [1, 2] }
      const result = applyOperators(doc, { $push: { items: { $each: [3, 4, 5], $slice: 3 } } })
      expect(result.document.items).toEqual([1, 2, 3])
    })

    it('supports $slice modifier (negative)', () => {
      const doc = { items: [1, 2] }
      const result = applyOperators(doc, { $push: { items: { $each: [3, 4, 5], $slice: -3 } } })
      expect(result.document.items).toEqual([3, 4, 5])
    })

    it('supports $sort modifier', () => {
      const doc = { scores: [5, 2, 8] }
      const result = applyOperators(doc, { $push: { scores: { $each: [1, 9], $sort: 1 } } })
      expect(result.document.scores).toEqual([1, 2, 5, 8, 9])
    })
  })

  describe('$pull operator', () => {
    it('removes matching elements', () => {
      const doc = { tags: ['a', 'b', 'c', 'b'] }
      const result = applyOperators(doc, { $pull: { tags: 'b' } })
      expect(result.document.tags).toEqual(['a', 'c'])
    })

    it('supports comparison operators', () => {
      const doc = { scores: [10, 25, 50, 75, 100] }
      const result = applyOperators(doc, { $pull: { scores: { $lt: 30 } } })
      expect(result.document.scores).toEqual([50, 75, 100])
    })

    it('supports object matching', () => {
      const doc = { items: [{ id: 1, active: true }, { id: 2, active: false }] }
      const result = applyOperators(doc, { $pull: { items: { active: false } } })
      expect(result.document.items).toEqual([{ id: 1, active: true }])
    })
  })

  describe('$pullAll operator', () => {
    it('removes all matching values', () => {
      const doc = { tags: ['a', 'b', 'c', 'a', 'b'] }
      const result = applyOperators(doc, { $pullAll: { tags: ['a', 'b'] } })
      expect(result.document.tags).toEqual(['c'])
    })
  })

  describe('$addToSet operator', () => {
    it('adds unique value', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyOperators(doc, { $addToSet: { tags: 'c' } })
      expect(result.document.tags).toEqual(['a', 'b', 'c'])
    })

    it('does not add duplicate', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyOperators(doc, { $addToSet: { tags: 'a' } })
      expect(result.document.tags).toEqual(['a', 'b'])
    })

    it('supports $each modifier', () => {
      const doc = { tags: ['a'] }
      const result = applyOperators(doc, { $addToSet: { tags: { $each: ['a', 'b', 'c'] } } })
      expect(result.document.tags).toEqual(['a', 'b', 'c'])
    })
  })

  describe('$pop operator', () => {
    it('removes last element with 1', () => {
      const doc = { items: ['a', 'b', 'c'] }
      const result = applyOperators(doc, { $pop: { items: 1 } })
      expect(result.document.items).toEqual(['a', 'b'])
    })

    it('removes first element with -1', () => {
      const doc = { items: ['a', 'b', 'c'] }
      const result = applyOperators(doc, { $pop: { items: -1 } })
      expect(result.document.items).toEqual(['b', 'c'])
    })
  })

  describe('$rename operator', () => {
    it('renames fields', () => {
      const doc = { oldName: 'value' }
      const result = applyOperators(doc, { $rename: { oldName: 'newName' } })
      expect(result.document).toEqual({ newName: 'value' })
    })

    it('handles non-existent fields', () => {
      const doc = { name: 'John' }
      const result = applyOperators(doc, { $rename: { missing: 'newName' } })
      expect(result.document).toEqual({ name: 'John' })
    })
  })

  describe('$currentDate operator', () => {
    it('sets field to current date', () => {
      const doc = {}
      const timestamp = new Date('2024-01-01')
      const result = applyOperators(doc, { $currentDate: { updatedAt: true } }, { timestamp })
      expect(result.document.updatedAt).toEqual(timestamp)
    })

    it('supports timestamp type', () => {
      const doc = {}
      const timestamp = new Date('2024-01-01')
      const result = applyOperators(doc, { $currentDate: { ts: { $type: 'timestamp' } } }, { timestamp })
      expect(result.document.ts).toBe(timestamp.getTime())
    })
  })

  describe('$setOnInsert operator', () => {
    it('sets fields only on insert', () => {
      const doc = {}
      const result = applyOperators(doc, { $setOnInsert: { defaultValue: 100 } }, { isInsert: true })
      expect(result.document.defaultValue).toBe(100)
    })

    it('does not set fields on update', () => {
      const doc = {}
      const result = applyOperators(doc, { $setOnInsert: { defaultValue: 100 } }, { isInsert: false })
      expect(result.document.defaultValue).toBeUndefined()
    })
  })

  describe('$bit operator', () => {
    it('applies AND operation', () => {
      const doc = { flags: 0b1111 }
      const result = applyOperators(doc, { $bit: { flags: { and: 0b1010 } } })
      expect(result.document.flags).toBe(0b1010)
    })

    it('applies OR operation', () => {
      const doc = { flags: 0b1010 }
      const result = applyOperators(doc, { $bit: { flags: { or: 0b0101 } } })
      expect(result.document.flags).toBe(0b1111)
    })

    it('applies XOR operation', () => {
      const doc = { flags: 0b1111 }
      const result = applyOperators(doc, { $bit: { flags: { xor: 0b1010 } } })
      expect(result.document.flags).toBe(0b0101)
    })
  })

  describe('$link and $unlink operators', () => {
    it('extracts link operations', () => {
      const doc = {}
      const result = applyOperators(doc, { $link: { author: 'users/123' } } as any)
      expect(result.relationshipOps).toContainEqual({
        type: 'link',
        predicate: 'author',
        targets: ['users/123'],
      })
    })

    it('extracts unlink operations', () => {
      const doc = {}
      const result = applyOperators(doc, { $unlink: { author: 'users/123' } } as any)
      expect(result.relationshipOps).toContainEqual({
        type: 'unlink',
        predicate: 'author',
        targets: ['users/123'],
      })
    })

    it('handles $all unlink', () => {
      const doc = {}
      const result = applyOperators(doc, { $unlink: { author: '$all' } } as any)
      expect(result.relationshipOps).toContainEqual({
        type: 'unlink',
        predicate: 'author',
        targets: [],
      })
    })

    it('handles array targets', () => {
      const doc = {}
      const result = applyOperators(doc, { $link: { categories: ['cat/1', 'cat/2'] } } as any)
      expect(result.relationshipOps).toContainEqual({
        type: 'link',
        predicate: 'categories',
        targets: ['cat/1', 'cat/2'],
      })
    })
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('validateUpdateOperators', () => {
  it('accepts valid operators', () => {
    expect(() => validateUpdateOperators({ $set: { name: 'John' } })).not.toThrow()
    expect(() => validateUpdateOperators({ $inc: { count: 1 } })).not.toThrow()
    expect(() => validateUpdateOperators({ $push: { tags: 'new' } })).not.toThrow()
  })

  it('rejects invalid operators', () => {
    expect(() => validateUpdateOperators({ $invalid: { name: 'John' } } as any)).toThrow()
    expect(() => validateUpdateOperators({ $foo: {} } as any)).toThrow()
  })

  it('detects conflicting operators on same field', () => {
    expect(() =>
      validateUpdateOperators({
        $set: { count: 5 },
        $inc: { count: 1 },
      })
    ).toThrow(/Conflicting operators/)
  })
})

// =============================================================================
// Prototype Pollution Protection Tests
// =============================================================================

describe('Prototype Pollution Protection', () => {
  describe('isUnsafePath', () => {
    it('detects __proto__ as unsafe', () => {
      expect(isUnsafePath('__proto__')).toBe(true)
      expect(isUnsafePath('data.__proto__')).toBe(true)
      expect(isUnsafePath('__proto__.polluted')).toBe(true)
      expect(isUnsafePath('nested.__proto__.value')).toBe(true)
    })

    it('detects constructor as unsafe', () => {
      expect(isUnsafePath('constructor')).toBe(true)
      expect(isUnsafePath('data.constructor')).toBe(true)
      expect(isUnsafePath('constructor.prototype')).toBe(true)
    })

    it('detects prototype as unsafe', () => {
      expect(isUnsafePath('prototype')).toBe(true)
      expect(isUnsafePath('data.prototype')).toBe(true)
      expect(isUnsafePath('prototype.polluted')).toBe(true)
    })

    it('allows safe paths', () => {
      expect(isUnsafePath('name')).toBe(false)
      expect(isUnsafePath('user.profile.name')).toBe(false)
      expect(isUnsafePath('items.0.value')).toBe(false)
      expect(isUnsafePath('data_proto')).toBe(false)
      expect(isUnsafePath('myConstructor')).toBe(false)
      expect(isUnsafePath('prototypeVersion')).toBe(false)
    })
  })

  describe('validatePath', () => {
    it('throws on unsafe paths', () => {
      expect(() => validatePath('__proto__')).toThrow(/prototype pollution/)
      expect(() => validatePath('constructor')).toThrow(/prototype pollution/)
      expect(() => validatePath('prototype')).toThrow(/prototype pollution/)
      expect(() => validatePath('data.__proto__.polluted')).toThrow(/prototype pollution/)
    })

    it('does not throw on safe paths', () => {
      expect(() => validatePath('name')).not.toThrow()
      expect(() => validatePath('user.profile.email')).not.toThrow()
    })
  })

  describe('setField rejects prototype pollution', () => {
    it('throws when setting __proto__', () => {
      const obj = { name: 'test' }
      expect(() => setField(obj, '__proto__', { polluted: true })).toThrow(/prototype pollution/)
    })

    it('throws when setting nested __proto__', () => {
      const obj = { data: {} }
      expect(() => setField(obj, 'data.__proto__.polluted', true)).toThrow(/prototype pollution/)
    })

    it('throws when setting constructor', () => {
      const obj = { name: 'test' }
      expect(() => setField(obj, 'constructor', {})).toThrow(/prototype pollution/)
    })

    it('throws when setting constructor.prototype', () => {
      const obj = { name: 'test' }
      expect(() => setField(obj, 'constructor.prototype.polluted', true)).toThrow(/prototype pollution/)
    })

    it('throws when setting prototype', () => {
      const obj = { name: 'test' }
      expect(() => setField(obj, 'prototype', {})).toThrow(/prototype pollution/)
    })
  })

  describe('getField rejects prototype pollution', () => {
    it('throws when getting __proto__', () => {
      const obj = { name: 'test' }
      expect(() => getField(obj, '__proto__')).toThrow(/prototype pollution/)
    })

    it('throws when getting nested __proto__', () => {
      const obj = { data: { value: 1 } }
      expect(() => getField(obj, 'data.__proto__')).toThrow(/prototype pollution/)
    })
  })

  describe('unsetField rejects prototype pollution', () => {
    it('throws when unsetting __proto__', () => {
      const obj = { name: 'test' }
      expect(() => unsetField(obj, '__proto__')).toThrow(/prototype pollution/)
    })

    it('throws when unsetting nested __proto__', () => {
      const obj = { data: { value: 1 } }
      expect(() => unsetField(obj, 'data.__proto__')).toThrow(/prototype pollution/)
    })
  })

  describe('applyOperators rejects prototype pollution', () => {
    it('throws when $set uses __proto__ path', () => {
      const doc = { name: 'test' }
      expect(() => applyOperators(doc, { $set: { '__proto__.polluted': true } }))
        .toThrow(/prototype pollution/)
    })

    it('throws when $set uses constructor path', () => {
      const doc = { name: 'test' }
      expect(() => applyOperators(doc, { $set: { 'constructor.prototype.polluted': true } }))
        .toThrow(/prototype pollution/)
    })

    it('throws when $unset uses __proto__ path', () => {
      const doc = { name: 'test' }
      // Use JSON.parse to create an object where __proto__ is a real property
      // (object literals handle __proto__ specially)
      const update = JSON.parse('{"$unset": {"__proto__": ""}}')
      expect(() => applyOperators(doc, update))
        .toThrow(/prototype pollution/)
    })

    it('throws when $inc uses __proto__ path', () => {
      const doc = { count: 1 }
      expect(() => applyOperators(doc, { $inc: { '__proto__.count': 1 } }))
        .toThrow(/prototype pollution/)
    })

    it('throws when $rename source uses __proto__ path', () => {
      const doc = { name: 'test' }
      // Use JSON.parse to create an object where __proto__ is a real property
      const update = JSON.parse('{"$rename": {"__proto__": "newName"}}')
      expect(() => applyOperators(doc, update))
        .toThrow(/prototype pollution/)
    })

    it('throws when $rename target uses __proto__ path', () => {
      const doc = { name: 'test' }
      expect(() => applyOperators(doc, { $rename: { name: '__proto__' } }))
        .toThrow(/prototype pollution/)
    })

    it('does not pollute Object.prototype', () => {
      // This test verifies that even if our protection fails somehow,
      // the test itself checks that Object.prototype was not modified
      const beforeKeys = Object.keys(Object.prototype)

      try {
        applyOperators({}, { $set: { '__proto__.polluted': true } })
      } catch {
        // Expected to throw
      }

      const afterKeys = Object.keys(Object.prototype)
      expect(afterKeys).toEqual(beforeKeys)
      expect((Object.prototype as any).polluted).toBeUndefined()
    })
  })
})

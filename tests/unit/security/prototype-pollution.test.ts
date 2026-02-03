/**
 * Prototype Pollution Protection Tests
 *
 * Tests for protection against prototype pollution attacks in:
 * - Collection.ts (filters and updates)
 * - path-safety utilities
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Collection, clearGlobalStorage } from '../../../src/Collection'
import {
  UNSAFE_PATH_SEGMENTS,
  DANGEROUS_KEYS,
  isUnsafePath,
  isDangerousKey,
  validatePath,
  validateKey,
  validateObjectKeys,
  validateObjectKeysDeep,
  sanitizeObject,
} from '../../../src/utils/path-safety'

// =============================================================================
// Path Safety Utility Tests
// =============================================================================

describe('Path Safety Utilities', () => {
  describe('UNSAFE_PATH_SEGMENTS', () => {
    it('contains __proto__', () => {
      expect(UNSAFE_PATH_SEGMENTS.has('__proto__')).toBe(true)
    })

    it('contains constructor', () => {
      expect(UNSAFE_PATH_SEGMENTS.has('constructor')).toBe(true)
    })

    it('contains prototype', () => {
      expect(UNSAFE_PATH_SEGMENTS.has('prototype')).toBe(true)
    })
  })

  describe('DANGEROUS_KEYS', () => {
    it('contains __proto__', () => {
      expect(DANGEROUS_KEYS.has('__proto__')).toBe(true)
    })

    it('contains constructor', () => {
      expect(DANGEROUS_KEYS.has('constructor')).toBe(true)
    })

    it('contains prototype', () => {
      expect(DANGEROUS_KEYS.has('prototype')).toBe(true)
    })
  })

  describe('isUnsafePath', () => {
    it('detects __proto__ at start of path', () => {
      expect(isUnsafePath('__proto__')).toBe(true)
      expect(isUnsafePath('__proto__.polluted')).toBe(true)
    })

    it('detects __proto__ in middle of path', () => {
      expect(isUnsafePath('data.__proto__')).toBe(true)
      expect(isUnsafePath('data.__proto__.polluted')).toBe(true)
    })

    it('detects constructor in path', () => {
      expect(isUnsafePath('constructor')).toBe(true)
      expect(isUnsafePath('data.constructor')).toBe(true)
      expect(isUnsafePath('constructor.prototype')).toBe(true)
    })

    it('detects prototype in path', () => {
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

  describe('isDangerousKey', () => {
    it('identifies __proto__ as dangerous', () => {
      expect(isDangerousKey('__proto__')).toBe(true)
    })

    it('identifies constructor as dangerous', () => {
      expect(isDangerousKey('constructor')).toBe(true)
    })

    it('identifies prototype as dangerous', () => {
      expect(isDangerousKey('prototype')).toBe(true)
    })

    it('identifies safe keys as safe', () => {
      expect(isDangerousKey('name')).toBe(false)
      expect(isDangerousKey('value')).toBe(false)
      expect(isDangerousKey('_proto')).toBe(false)
      expect(isDangerousKey('myConstructor')).toBe(false)
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
      expect(() => validatePath('items.0.value')).not.toThrow()
    })
  })

  describe('validateKey', () => {
    it('throws on dangerous keys', () => {
      expect(() => validateKey('__proto__')).toThrow(/Invalid key/)
      expect(() => validateKey('constructor')).toThrow(/Invalid key/)
      expect(() => validateKey('prototype')).toThrow(/Invalid key/)
    })

    it('does not throw on safe keys', () => {
      expect(() => validateKey('name')).not.toThrow()
      expect(() => validateKey('value')).not.toThrow()
    })
  })

  describe('validateObjectKeys', () => {
    it('throws on objects with __proto__ key', () => {
      // Use JSON.parse to create an object where __proto__ is an own property
      const obj = JSON.parse('{"__proto__": {"polluted": true}}')
      expect(() => validateObjectKeys(obj)).toThrow(/Invalid key/)
    })

    it('throws on objects with constructor key', () => {
      const obj = { constructor: {} }
      expect(() => validateObjectKeys(obj)).toThrow(/Invalid key/)
    })

    it('throws on objects with prototype key', () => {
      const obj = { prototype: {} }
      expect(() => validateObjectKeys(obj)).toThrow(/Invalid key/)
    })

    it('does not throw on safe objects', () => {
      expect(() => validateObjectKeys({ name: 'John', age: 30 })).not.toThrow()
      expect(() => validateObjectKeys({ nested: { value: 1 } })).not.toThrow()
    })

    it('handles null and undefined', () => {
      expect(() => validateObjectKeys(null)).not.toThrow()
      expect(() => validateObjectKeys(undefined)).not.toThrow()
    })
  })

  describe('validateObjectKeysDeep', () => {
    it('throws on nested objects with __proto__', () => {
      const obj = {
        user: JSON.parse('{"__proto__": {"polluted": true}}'),
      }
      expect(() => validateObjectKeysDeep(obj)).toThrow(/Invalid key/)
    })

    it('throws on arrays containing objects with dangerous keys', () => {
      const obj = {
        items: [{ name: 'safe' }, JSON.parse('{"__proto__": {}}')],
      }
      expect(() => validateObjectKeysDeep(obj)).toThrow(/Invalid key/)
    })

    it('handles deeply nested dangerous keys', () => {
      const obj = {
        level1: {
          level2: {
            level3: JSON.parse('{"constructor": {}}'),
          },
        },
      }
      expect(() => validateObjectKeysDeep(obj)).toThrow(/Invalid key/)
    })

    it('respects max depth', () => {
      const deepObj = {
        level1: {
          level2: {
            level3: {
              level4: JSON.parse('{"__proto__": {}}'),
            },
          },
        },
      }
      // With maxDepth of 3, should not reach level 4
      expect(() => validateObjectKeysDeep(deepObj, 3)).not.toThrow()
    })

    it('does not throw on safe deep objects', () => {
      const safeObj = {
        user: {
          profile: {
            name: 'John',
            settings: {
              theme: 'dark',
            },
          },
        },
      }
      expect(() => validateObjectKeysDeep(safeObj)).not.toThrow()
    })
  })

  describe('sanitizeObject', () => {
    it('removes __proto__ key', () => {
      const obj = JSON.parse('{"name": "John", "__proto__": {"polluted": true}}')
      const result = sanitizeObject(obj)
      expect(result).toEqual({ name: 'John' })
      // Use Object.hasOwn to check for own property, not prototype chain
      expect(Object.hasOwn(result, '__proto__')).toBe(false)
    })

    it('removes constructor key', () => {
      const obj = { name: 'John', constructor: {} }
      const result = sanitizeObject(obj)
      expect(result).toEqual({ name: 'John' })
      expect(Object.hasOwn(result, 'constructor')).toBe(false)
    })

    it('removes prototype key', () => {
      const obj = { name: 'John', prototype: {} }
      const result = sanitizeObject(obj)
      expect(result).toEqual({ name: 'John' })
      expect(Object.hasOwn(result, 'prototype')).toBe(false)
    })

    it('preserves safe keys', () => {
      const obj = { name: 'John', age: 30, email: 'john@example.com' }
      const result = sanitizeObject(obj)
      expect(result).toEqual(obj)
    })

    it('handles null and undefined', () => {
      expect(sanitizeObject(null)).toEqual({})
      expect(sanitizeObject(undefined)).toEqual({})
    })
  })
})

// =============================================================================
// Collection Prototype Pollution Tests
// =============================================================================

describe('Collection Prototype Pollution Protection', () => {
  beforeEach(() => {
    clearGlobalStorage()
  })

  afterEach(() => {
    clearGlobalStorage()
  })

  describe('Filter validation', () => {
    it('rejects filters with __proto__ key', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      // Use JSON.parse to create filter with __proto__ as own property
      const filter = JSON.parse('{"__proto__": {"polluted": true}}')
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('rejects filters with constructor key', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const filter = { constructor: { polluted: true } }
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('rejects filters with prototype key', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const filter = { prototype: { polluted: true } }
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('rejects nested filters with dangerous keys in operator values', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      // Filter with dangerous key inside operator object
      const filter = {
        status: JSON.parse('{"__proto__": "active"}'),
      }
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('rejects $and with dangerous keys', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const filter = {
        $and: [{ name: 'test' }, JSON.parse('{"__proto__": {"polluted": true}}')],
      }
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('rejects $or with dangerous keys', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const filter = {
        $or: [{ name: 'test' }, { constructor: { polluted: true } }],
      }
      await expect(collection.find(filter)).rejects.toThrow(/Invalid key/)
    })

    it('allows safe filters', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1', status: 'active' })

      const result = await collection.find({ status: 'active' })
      expect(result).toHaveLength(1)
    })
  })

  describe('Update validation', () => {
    it('rejects $set with __proto__ path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      await expect(
        collection.update('test1', { $set: { '__proto__.polluted': true } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $set with constructor path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      await expect(
        collection.update('test1', { $set: { 'constructor.prototype.polluted': true } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $set with prototype path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      await expect(
        collection.update('test1', { $set: { 'prototype.polluted': true } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $unset with dangerous path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      // Use JSON.parse to create update with __proto__ as own property
      const update = JSON.parse('{"$unset": {"__proto__": ""}}')
      await expect(collection.update('test1', update)).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $inc with dangerous path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1', count: 1 })

      await expect(
        collection.update('test1', { $inc: { '__proto__.count': 1 } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $push with dangerous path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1', items: [] })

      await expect(
        collection.update('test1', { $push: { '__proto__.items': 'value' } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $rename with dangerous source path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1', value: 1 })

      // Use JSON.parse to create update with __proto__ as own property
      const update = JSON.parse('{"$rename": {"__proto__": "newName"}}')
      await expect(collection.update('test1', update)).rejects.toThrow(/prototype pollution/)
    })

    it('rejects $rename with dangerous target path', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1', value: 1 })

      await expect(
        collection.update('test1', { $rename: { value: '__proto__' } })
      ).rejects.toThrow(/prototype pollution/)
    })

    it('allows safe updates', async () => {
      const collection = new Collection('test')
      const entity = await collection.create({ $type: 'Test', name: 'test1', status: 'pending' })

      // Use the entity's local ID for update
      const localId = entity.$id.split('/')[1]
      await collection.update(localId!, { $set: { status: 'active' } })
      const result = await collection.get(localId!)
      expect(result.status).toBe('active')
    })
  })

  describe('Create validation', () => {
    it('rejects create with __proto__ in data', async () => {
      const collection = new Collection('test')

      const data = JSON.parse('{"$type": "Test", "name": "test1", "__proto__": {"polluted": true}}')
      await expect(collection.create(data)).rejects.toThrow(/Invalid key/)
    })

    it('rejects create with nested __proto__', async () => {
      const collection = new Collection('test')

      const data = {
        $type: 'Test',
        name: 'test1',
        nested: JSON.parse('{"__proto__": {"polluted": true}}'),
      }
      await expect(collection.create(data)).rejects.toThrow(/Invalid key/)
    })

    it('allows safe creates', async () => {
      const collection = new Collection('test')

      const entity = await collection.create({ $type: 'Test', name: 'test1', status: 'active' })
      expect(entity.name).toBe('test1')
      expect(entity.status).toBe('active')
    })
  })

  describe('Global prototype not polluted', () => {
    it('does not pollute Object.prototype via filter', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const beforeKeys = Object.keys(Object.prototype)

      try {
        const filter = JSON.parse('{"__proto__": {"polluted": true}}')
        await collection.find(filter)
      } catch {
        // Expected to throw
      }

      const afterKeys = Object.keys(Object.prototype)
      expect(afterKeys).toEqual(beforeKeys)
      expect((Object.prototype as Record<string, unknown>).polluted).toBeUndefined()
    })

    it('does not pollute Object.prototype via update', async () => {
      const collection = new Collection('test')
      await collection.create({ $type: 'Test', name: 'test1' })

      const beforeKeys = Object.keys(Object.prototype)

      try {
        await collection.update('test1', { $set: { '__proto__.polluted': true } })
      } catch {
        // Expected to throw
      }

      const afterKeys = Object.keys(Object.prototype)
      expect(afterKeys).toEqual(beforeKeys)
      expect((Object.prototype as Record<string, unknown>).polluted).toBeUndefined()
    })

    it('does not pollute Object.prototype via create', async () => {
      const collection = new Collection('test')

      const beforeKeys = Object.keys(Object.prototype)

      try {
        const data = JSON.parse('{"$type": "Test", "name": "test1", "__proto__": {"polluted": true}}')
        await collection.create(data)
      } catch {
        // Expected to throw
      }

      const afterKeys = Object.keys(Object.prototype)
      expect(afterKeys).toEqual(beforeKeys)
      expect((Object.prototype as Record<string, unknown>).polluted).toBeUndefined()
    })
  })
})

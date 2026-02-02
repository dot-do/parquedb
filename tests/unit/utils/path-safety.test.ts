/**
 * Path Safety Tests
 *
 * Tests for prototype pollution protection in dot-notation path operations.
 * These tests verify that dangerous path segments (__proto__, constructor, prototype)
 * are detected and rejected across all modules that handle nested paths.
 */

import { describe, it, expect } from 'vitest'
import {
  UNSAFE_PATH_SEGMENTS,
  isUnsafePath,
  validatePath,
} from '@/utils/path-safety'

describe('Path Safety Utility', () => {
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

    it('does not contain safe names', () => {
      expect(UNSAFE_PATH_SEGMENTS.has('name')).toBe(false)
      expect(UNSAFE_PATH_SEGMENTS.has('value')).toBe(false)
      expect(UNSAFE_PATH_SEGMENTS.has('proto')).toBe(false)
    })
  })

  describe('isUnsafePath', () => {
    it('returns false for safe simple paths', () => {
      expect(isUnsafePath('name')).toBe(false)
      expect(isUnsafePath('a')).toBe(false)
      expect(isUnsafePath('field')).toBe(false)
    })

    it('returns false for safe nested paths', () => {
      expect(isUnsafePath('a.b.c')).toBe(false)
      expect(isUnsafePath('user.name')).toBe(false)
      expect(isUnsafePath('data.nested.value')).toBe(false)
    })

    it('detects __proto__ as first segment', () => {
      expect(isUnsafePath('__proto__')).toBe(true)
      expect(isUnsafePath('__proto__.polluted')).toBe(true)
    })

    it('detects __proto__ as middle segment', () => {
      expect(isUnsafePath('a.__proto__.b')).toBe(true)
    })

    it('detects __proto__ as last segment', () => {
      expect(isUnsafePath('a.b.__proto__')).toBe(true)
    })

    it('detects constructor at any position', () => {
      expect(isUnsafePath('constructor')).toBe(true)
      expect(isUnsafePath('constructor.prototype')).toBe(true)
      expect(isUnsafePath('a.constructor')).toBe(true)
      expect(isUnsafePath('a.constructor.b')).toBe(true)
    })

    it('detects prototype at any position', () => {
      expect(isUnsafePath('prototype')).toBe(true)
      expect(isUnsafePath('prototype.polluted')).toBe(true)
      expect(isUnsafePath('a.prototype')).toBe(true)
      expect(isUnsafePath('a.prototype.b')).toBe(true)
    })

    it('does not false-positive on partial matches', () => {
      // These contain the word but not as a full segment
      expect(isUnsafePath('my__proto__field')).toBe(false)
      expect(isUnsafePath('constructorName')).toBe(false)
      expect(isUnsafePath('prototypeOf')).toBe(false)
    })
  })

  describe('validatePath', () => {
    it('does not throw for safe paths', () => {
      expect(() => validatePath('name')).not.toThrow()
      expect(() => validatePath('a.b.c')).not.toThrow()
      expect(() => validatePath('user.address.city')).not.toThrow()
    })

    it('throws for __proto__ paths', () => {
      expect(() => validatePath('__proto__')).toThrow('Unsafe path detected')
      expect(() => validatePath('__proto__.polluted')).toThrow('prototype pollution')
      expect(() => validatePath('a.__proto__.b')).toThrow('Unsafe path detected')
    })

    it('throws for constructor paths', () => {
      expect(() => validatePath('constructor')).toThrow('Unsafe path detected')
      expect(() => validatePath('a.constructor.prototype')).toThrow('prototype pollution')
    })

    it('throws for prototype paths', () => {
      expect(() => validatePath('prototype')).toThrow('Unsafe path detected')
      expect(() => validatePath('a.prototype.b')).toThrow('prototype pollution')
    })

    it('includes the offending path in the error message', () => {
      expect(() => validatePath('__proto__.bad')).toThrow('"__proto__.bad"')
    })
  })
})

describe('Path Safety Integration - query/update.ts', () => {
  it('getField rejects prototype pollution paths', async () => {
    const { getField } = await import('@/query/update')
    const obj = { a: { b: 1 } }

    expect(() => getField(obj, '__proto__')).toThrow('Unsafe path detected')
    expect(() => getField(obj, 'a.__proto__')).toThrow('Unsafe path detected')
    expect(() => getField(obj, 'constructor.prototype')).toThrow('Unsafe path detected')
  })

  it('getField works for safe paths', async () => {
    const { getField } = await import('@/query/update')
    const obj = { a: { b: 1 } }

    expect(getField(obj, 'a.b')).toBe(1)
    expect(getField(obj, 'a')).toEqual({ b: 1 })
  })

  it('setField rejects prototype pollution paths', async () => {
    const { setField } = await import('@/query/update')
    const obj = { a: 1 }

    expect(() => setField(obj, '__proto__.polluted', true)).toThrow('Unsafe path detected')
    expect(() => setField(obj, 'constructor.prototype.polluted', true)).toThrow('Unsafe path detected')
    expect(() => setField(obj, 'a.prototype', true)).toThrow('Unsafe path detected')
  })

  it('setField works for safe paths', async () => {
    const { setField } = await import('@/query/update')
    const obj = { a: { b: 1 } }

    const result = setField(obj, 'a.c', 2)
    expect(result).toEqual({ a: { b: 1, c: 2 } })
  })

  it('unsetField rejects prototype pollution paths', async () => {
    const { unsetField } = await import('@/query/update')
    const obj = { a: { b: 1 } }

    expect(() => unsetField(obj, '__proto__')).toThrow('Unsafe path detected')
    expect(() => unsetField(obj, 'a.constructor')).toThrow('Unsafe path detected')
    expect(() => unsetField(obj, 'a.prototype.b')).toThrow('Unsafe path detected')
  })

  it('unsetField works for safe paths', async () => {
    const { unsetField } = await import('@/query/update')
    const obj = { a: { b: 1, c: 2 } }

    const result = unsetField(obj, 'a.b')
    expect(result).toEqual({ a: { c: 2 } })
  })
})

describe('Path Safety Integration - Collection.ts', () => {
  it('update with $set rejects prototype pollution in nested paths', async () => {
    const { Collection, clearGlobalStorage } = await import('@/Collection')

    clearGlobalStorage()
    const col = new Collection('test')
    const entity = await col.create({ $type: 'Test', name: 'test-entity', value: 1 } as any)
    const localId = entity.$id.replace('test/', '')

    // Nested path with __proto__ should throw
    await expect(
      col.update(localId, { $set: { '__proto__.polluted': true } } as any)
    ).rejects.toThrow('Unsafe path detected')

    // Nested path with constructor should throw
    await expect(
      col.update(localId, { $set: { 'constructor.prototype.polluted': true } } as any)
    ).rejects.toThrow('Unsafe path detected')

    clearGlobalStorage()
  })

  it('update with $unset rejects prototype pollution in nested paths', async () => {
    const { Collection, clearGlobalStorage } = await import('@/Collection')

    clearGlobalStorage()
    const col = new Collection('test')
    const entity = await col.create({ $type: 'Test', name: 'test-entity', value: 1 } as any)
    const localId = entity.$id.replace('test/', '')

    // Nested path with __proto__ should throw
    await expect(
      col.update(localId, { $unset: { '__proto__.polluted': '' } } as any)
    ).rejects.toThrow('Unsafe path detected')

    clearGlobalStorage()
  })

  it('update with safe nested $set paths works correctly', async () => {
    const { Collection, clearGlobalStorage } = await import('@/Collection')

    clearGlobalStorage()
    const col = new Collection('test')
    const entity = await col.create({ $type: 'Test', name: 'test-entity', data: { x: 1 } } as any)
    const localId = entity.$id.replace('test/', '')

    const result = await col.update(localId, { $set: { 'data.y': 2 } } as any)
    expect(result.modifiedCount).toBe(1)

    const updated = await col.get(localId)
    expect((updated as any).data.y).toBe(2)

    clearGlobalStorage()
  })
})

describe('Path Safety Integration - mutation/operators.ts', () => {
  it('still exports isUnsafePath and validatePath for backwards compatibility', async () => {
    const { isUnsafePath, validatePath } = await import('@/mutation/operators')

    expect(isUnsafePath('__proto__')).toBe(true)
    expect(isUnsafePath('safe.path')).toBe(false)
    expect(() => validatePath('__proto__')).toThrow('Unsafe path detected')
    expect(() => validatePath('safe.path')).not.toThrow()
  })
})

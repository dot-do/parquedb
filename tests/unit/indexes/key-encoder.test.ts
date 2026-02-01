/**
 * Tests for Key Encoder
 */

import { describe, it, expect } from 'vitest'
import {
  encodeKey,
  decodeKey,
  compareKeys,
  hashKey,
  keyToHex,
  hexToKey,
  encodeCompositeKey,
  decodeCompositeKey,
} from '@/indexes/secondary/key-encoder'

describe('Key Encoder', () => {
  describe('encodeKey/decodeKey roundtrip', () => {
    it('handles null', () => {
      const encoded = encodeKey(null)
      expect(decodeKey(encoded)).toBe(null)
    })

    it('handles undefined', () => {
      const encoded = encodeKey(undefined)
      expect(decodeKey(encoded)).toBe(null) // undefined becomes null
    })

    it('handles boolean false', () => {
      const encoded = encodeKey(false)
      expect(decodeKey(encoded)).toBe(false)
    })

    it('handles boolean true', () => {
      const encoded = encodeKey(true)
      expect(decodeKey(encoded)).toBe(true)
    })

    it('handles positive integers', () => {
      const values = [0, 1, 42, 100, 1000000]
      for (const v of values) {
        expect(decodeKey(encodeKey(v))).toBe(v)
      }
    })

    it('handles negative integers', () => {
      const values = [-1, -42, -100, -1000000]
      for (const v of values) {
        expect(decodeKey(encodeKey(v))).toBe(v)
      }
    })

    it('handles floating point numbers', () => {
      const values = [0.5, 3.14159, -2.718, 1e10, 1e-10]
      for (const v of values) {
        expect(decodeKey(encodeKey(v))).toBeCloseTo(v)
      }
    })

    it('handles special numbers', () => {
      expect(decodeKey(encodeKey(Infinity))).toBe(Infinity)
      expect(decodeKey(encodeKey(-Infinity))).toBe(-Infinity)
      expect(Number.isNaN(decodeKey(encodeKey(NaN)) as number)).toBe(true)
    })

    it('handles empty string', () => {
      expect(decodeKey(encodeKey(''))).toBe('')
    })

    it('handles ASCII strings', () => {
      const values = ['hello', 'world', 'foo bar', 'test-123']
      for (const v of values) {
        expect(decodeKey(encodeKey(v))).toBe(v)
      }
    })

    it('handles Unicode strings', () => {
      const values = ['日本語', 'emoji 😀', 'café', 'über']
      for (const v of values) {
        expect(decodeKey(encodeKey(v))).toBe(v)
      }
    })

    it('handles dates', () => {
      const dates = [
        new Date('2024-01-15T10:30:00Z'),
        new Date('1970-01-01T00:00:00Z'),
        new Date('1969-07-20T20:17:00Z'), // Before Unix epoch
        new Date('2099-12-31T23:59:59Z'),
      ]
      for (const d of dates) {
        expect((decodeKey(encodeKey(d)) as Date).getTime()).toBe(d.getTime())
      }
    })

    it('handles binary data', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5, 0, 255])
      const decoded = decodeKey(encodeKey(data)) as Uint8Array
      expect(decoded).toEqual(data)
    })

    it('handles arrays', () => {
      const arrays = [
        [],
        [1, 2, 3],
        ['a', 'b', 'c'],
        [1, 'two', true, null],
      ]
      for (const arr of arrays) {
        expect(decodeKey(encodeKey(arr))).toEqual(arr)
      }
    })

    it('handles objects', () => {
      const objects = [
        {},
        { a: 1 },
        { name: 'test', value: 42 },
        { nested: { deep: true } },
      ]
      for (const obj of objects) {
        expect(decodeKey(encodeKey(obj))).toEqual(obj)
      }
    })
  })

  describe('compareKeys - sort ordering', () => {
    it('orders null before all other types', () => {
      const nullKey = encodeKey(null)
      const boolKey = encodeKey(false)
      const numKey = encodeKey(0)
      const strKey = encodeKey('')

      expect(compareKeys(nullKey, boolKey)).toBeLessThan(0)
      expect(compareKeys(nullKey, numKey)).toBeLessThan(0)
      expect(compareKeys(nullKey, strKey)).toBeLessThan(0)
    })

    it('orders booleans: false < true', () => {
      const falseKey = encodeKey(false)
      const trueKey = encodeKey(true)

      expect(compareKeys(falseKey, trueKey)).toBeLessThan(0)
      expect(compareKeys(trueKey, falseKey)).toBeGreaterThan(0)
      expect(compareKeys(falseKey, falseKey)).toBe(0)
    })

    it('orders negative numbers correctly', () => {
      const neg10 = encodeKey(-10)
      const neg5 = encodeKey(-5)
      const neg1 = encodeKey(-1)

      expect(compareKeys(neg10, neg5)).toBeLessThan(0)
      expect(compareKeys(neg5, neg1)).toBeLessThan(0)
    })

    it('orders negative before positive numbers', () => {
      const neg = encodeKey(-1)
      const zero = encodeKey(0)
      const pos = encodeKey(1)

      expect(compareKeys(neg, zero)).toBeLessThan(0)
      expect(compareKeys(zero, pos)).toBeLessThan(0)
    })

    it('orders positive numbers correctly', () => {
      const nums = [0, 1, 2, 10, 100, 1000]
      for (let i = 0; i < nums.length - 1; i++) {
        const key1 = encodeKey(nums[i])
        const key2 = encodeKey(nums[i + 1])
        expect(compareKeys(key1, key2)).toBeLessThan(0)
      }
    })

    it('orders strings lexicographically', () => {
      const strings = ['', 'a', 'aa', 'ab', 'b', 'ba']
      for (let i = 0; i < strings.length - 1; i++) {
        const key1 = encodeKey(strings[i])
        const key2 = encodeKey(strings[i + 1])
        expect(compareKeys(key1, key2)).toBeLessThan(0)
      }
    })

    it('orders dates chronologically', () => {
      const d1 = encodeKey(new Date('2020-01-01'))
      const d2 = encodeKey(new Date('2021-01-01'))
      const d3 = encodeKey(new Date('2022-01-01'))

      expect(compareKeys(d1, d2)).toBeLessThan(0)
      expect(compareKeys(d2, d3)).toBeLessThan(0)
    })

    it('orders numbers before strings', () => {
      const numKey = encodeKey(999)
      const strKey = encodeKey('0')

      expect(compareKeys(numKey, strKey)).toBeLessThan(0)
    })
  })

  describe('hashKey', () => {
    it('produces consistent hashes', () => {
      const key = encodeKey('test')
      const hash1 = hashKey(key)
      const hash2 = hashKey(key)
      expect(hash1).toBe(hash2)
    })

    it('produces different hashes for different values', () => {
      const hash1 = hashKey(encodeKey('foo'))
      const hash2 = hashKey(encodeKey('bar'))
      expect(hash1).not.toBe(hash2)
    })

    it('returns 32-bit unsigned integers', () => {
      const hashes = ['a', 'b', 'test', 'hello world'].map(s =>
        hashKey(encodeKey(s))
      )
      for (const h of hashes) {
        expect(h).toBeGreaterThanOrEqual(0)
        expect(h).toBeLessThanOrEqual(0xffffffff)
      }
    })
  })

  describe('keyToHex/hexToKey', () => {
    it('roundtrips correctly', () => {
      const original = encodeKey('hello')
      const hex = keyToHex(original)
      const decoded = hexToKey(hex)
      expect(decoded).toEqual(original)
    })

    it('produces lowercase hex', () => {
      const hex = keyToHex(encodeKey(255))
      expect(hex).toMatch(/^[0-9a-f]+$/)
    })
  })

  describe('encodeCompositeKey/decodeCompositeKey', () => {
    it('handles single value', () => {
      const encoded = encodeCompositeKey(['test'])
      const decoded = decodeCompositeKey(encoded)
      expect(decoded).toEqual(['test'])
    })

    it('handles multiple values', () => {
      const values = ['namespace', 42, true, new Date('2024-01-01')]
      const encoded = encodeCompositeKey(values)
      const decoded = decodeCompositeKey(encoded)

      expect(decoded[0]).toBe('namespace')
      expect(decoded[1]).toBe(42)
      expect(decoded[2]).toBe(true)
      expect((decoded[3] as Date).getTime()).toBe(new Date('2024-01-01').getTime())
    })

    it('handles mixed types', () => {
      const values = [null, 0, '', false]
      const encoded = encodeCompositeKey(values)
      const decoded = decodeCompositeKey(encoded)
      expect(decoded).toEqual(values)
    })

    it('maintains sort order for composite keys with same-length components', () => {
      const key1 = encodeCompositeKey(['users', 1])
      const key2 = encodeCompositeKey(['users', 2])
      const key3 = encodeCompositeKey(['users', 10])

      expect(compareKeys(key1, key2)).toBeLessThan(0)
      expect(compareKeys(key2, key3)).toBeLessThan(0)
    })

    it('sorts composite keys with same first component by second component', () => {
      const key1 = encodeCompositeKey(['ns', 100])
      const key2 = encodeCompositeKey(['ns', 200])
      const key3 = encodeCompositeKey(['ns', 300])

      expect(compareKeys(key1, key2)).toBeLessThan(0)
      expect(compareKeys(key2, key3)).toBeLessThan(0)
    })
  })
})

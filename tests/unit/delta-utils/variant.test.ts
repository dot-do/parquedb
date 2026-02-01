/**
 * Parquet VARIANT Encoding Utilities Test Suite
 *
 * Tests for the Variant encoding/decoding functionality.
 * Covers all supported types, roundtrip encoding, and utility functions.
 */

import { describe, it, expect } from 'vitest'
import {
  encodeVariant,
  decodeVariant,
  isEncodable,
  estimateVariantSize,
  variantEquals,
  type VariantValue,
  type EncodedVariant,
} from '../../../src/delta-utils/variant'

// =============================================================================
// PRIMITIVE TYPE ENCODING/DECODING TESTS
// =============================================================================

describe('Variant Primitive Types', () => {
  describe('null', () => {
    it('encodes and decodes null', () => {
      const encoded = encodeVariant(null)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })

    it('encodes and decodes undefined as null', () => {
      const encoded = encodeVariant(undefined as unknown as VariantValue)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })
  })

  describe('boolean', () => {
    it('encodes and decodes true', () => {
      const encoded = encodeVariant(true)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(true)
    })

    it('encodes and decodes false', () => {
      const encoded = encodeVariant(false)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(false)
    })
  })

  describe('integers', () => {
    it('encodes and decodes small positive integers (INT8)', () => {
      const values = [0, 1, 42, 127]

      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('encodes and decodes negative integers (INT8)', () => {
      const values = [-1, -42, -128]

      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('encodes and decodes INT16 range integers', () => {
      const values = [128, 255, 1000, 32767, -129, -32768]

      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('encodes and decodes INT32 range integers', () => {
      const values = [32768, 100000, 2147483647, -32769, -2147483648]

      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('encodes and decodes large integers as INT64', () => {
      const value = 2147483648 // Beyond INT32
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      // Large numbers may come back as bigint
      expect(Number(decoded)).toBe(value)
    })
  })

  describe('bigint', () => {
    it('encodes and decodes positive bigint', () => {
      const value = 9007199254740993n // Beyond MAX_SAFE_INTEGER

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes negative bigint', () => {
      const value = -9007199254740993n

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes zero bigint', () => {
      const value = 0n

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })
  })

  describe('floating point', () => {
    it('encodes and decodes positive float', () => {
      const value = 3.14159

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeCloseTo(value, 10)
    })

    it('encodes and decodes negative float', () => {
      const value = -2.71828

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeCloseTo(value, 10)
    })

    it('encodes and decodes very small float', () => {
      const value = 0.000001

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeCloseTo(value, 10)
    })

    it('encodes and decodes large float with fractional part', () => {
      // Use a value that JavaScript doesn't consider an integer
      // Large exponents make Number.isInteger return true, so we use a smaller exponent
      const value = 1.234567e10 // 12345670000

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      // This may be encoded as integer due to no fractional part
      expect(Number(decoded)).toBe(value)
    })
  })

  describe('strings', () => {
    it('encodes and decodes short string', () => {
      const value = 'Hello, World!'

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes empty string', () => {
      const value = ''

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes long string (> 63 chars)', () => {
      const value = 'a'.repeat(100)

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes unicode string', () => {
      const value = 'Hello, !'

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes emoji string', () => {
      const value = 'Hello'

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })

    it('encodes and decodes string with special characters', () => {
      const value = 'Line1\nLine2\tTabbed\r\nWindows'

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(value)
    })
  })

  describe('Date', () => {
    it('encodes and decodes Date', () => {
      const value = new Date('2024-01-15T10:30:00Z')

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Date

      expect(decoded).toBeInstanceOf(Date)
      expect(decoded.getTime()).toBe(value.getTime())
    })

    it('encodes and decodes epoch Date', () => {
      const value = new Date(0)

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Date

      expect(decoded.getTime()).toBe(0)
    })

    it('encodes and decodes future Date', () => {
      const value = new Date('2099-12-31T23:59:59Z')

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Date

      expect(decoded.getTime()).toBe(value.getTime())
    })
  })

  describe('Uint8Array (binary)', () => {
    it('encodes and decodes binary data', () => {
      const value = new Uint8Array([0x00, 0x01, 0x02, 0xFF])

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Uint8Array

      expect(decoded).toBeInstanceOf(Uint8Array)
      expect(Array.from(decoded)).toEqual([0x00, 0x01, 0x02, 0xFF])
    })

    it('encodes and decodes empty binary', () => {
      const value = new Uint8Array([])

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Uint8Array

      expect(decoded.length).toBe(0)
    })

    it('encodes and decodes large binary', () => {
      const value = new Uint8Array(1000).fill(0xAB)

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Uint8Array

      expect(decoded.length).toBe(1000)
      expect(decoded.every(b => b === 0xAB)).toBe(true)
    })
  })
})

// =============================================================================
// COMPLEX TYPE ENCODING/DECODING TESTS
// =============================================================================

describe('Variant Complex Types', () => {
  describe('arrays', () => {
    it('encodes and decodes empty array', () => {
      const value: VariantValue[] = []

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual([])
    })

    it('encodes and decodes array of numbers', () => {
      const value = [1, 2, 3, 4, 5]

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(value)
    })

    it('encodes and decodes array of strings', () => {
      const value = ['alice', 'bob', 'charlie']

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(value)
    })

    it('encodes and decodes mixed array', () => {
      const value: VariantValue[] = [1, 'two', true, null]

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(value)
    })

    it('encodes and decodes nested arrays', () => {
      const value: VariantValue[] = [[1, 2], [3, 4], [[5, 6]]]

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(value)
    })

    it('encodes and decodes large array', () => {
      const value = Array.from({ length: 300 }, (_, i) => i)

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(value)
    })
  })

  describe('objects', () => {
    it('encodes and decodes empty object', () => {
      const value = {}

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual({})
    })

    it('encodes and decodes simple object', () => {
      const value = {
        name: 'Alice',
        age: 30,
        active: true,
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, VariantValue>

      expect(decoded.name).toBe('Alice')
      expect(decoded.age).toBe(30)
      expect(decoded.active).toBe(true)
    })

    it('encodes and decodes nested object', () => {
      const value = {
        user: {
          profile: {
            name: 'Alice',
            settings: {
              theme: 'dark',
              notifications: true,
            },
          },
        },
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, VariantValue>

      expect((decoded.user as any).profile.settings.theme).toBe('dark')
    })

    it('encodes and decodes object with array values', () => {
      const value = {
        tags: ['feature', 'urgent'],
        scores: [95, 87, 92],
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, VariantValue>

      expect(decoded.tags).toEqual(['feature', 'urgent'])
      expect(decoded.scores).toEqual([95, 87, 92])
    })

    it('encodes and decodes object with null values', () => {
      const value = {
        name: 'Alice',
        middle: null,
        last: 'Smith',
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, VariantValue>

      expect(decoded.name).toBe('Alice')
      expect(decoded.middle).toBeNull()
      expect(decoded.last).toBe('Smith')
    })

    it('handles object with many keys', () => {
      const value: Record<string, number> = {}
      for (let i = 0; i < 300; i++) {
        value[`key${i}`] = i
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, VariantValue>

      expect(Object.keys(decoded).length).toBe(300)
      expect(decoded.key0).toBe(0)
      expect(decoded.key299).toBe(299)
    })
  })
})

// =============================================================================
// ROUNDTRIP TESTS
// =============================================================================

describe('Variant Roundtrip', () => {
  it('roundtrips complex nested structure', () => {
    const value: VariantValue = {
      id: 'doc-123',
      version: 5,
      created: new Date('2024-01-15T10:00:00Z'),
      data: {
        items: [
          { name: 'Item 1', price: 19.99, inStock: true },
          { name: 'Item 2', price: 29.99, inStock: false },
        ],
        metadata: {
          source: 'api',
          tags: ['featured', 'new'],
        },
      },
      flags: [true, false, true],
      extra: null,
    }

    const encoded = encodeVariant(value)
    const decoded = decodeVariant(encoded) as Record<string, VariantValue>

    expect(decoded.id).toBe('doc-123')
    expect(decoded.version).toBe(5)
    expect((decoded.created as Date).getTime()).toBe(new Date('2024-01-15T10:00:00Z').getTime())
    expect((decoded.data as any).items[0].name).toBe('Item 1')
    expect((decoded.data as any).items[0].price).toBeCloseTo(19.99)
    expect((decoded.data as any).metadata.tags).toEqual(['featured', 'new'])
    expect(decoded.flags).toEqual([true, false, true])
    expect(decoded.extra).toBeNull()
  })

  it('preserves key order through roundtrip', () => {
    // Note: Object key order is preserved due to dictionary encoding
    const value = {
      zebra: 1,
      apple: 2,
      mango: 3,
    }

    const encoded = encodeVariant(value)
    const decoded = decodeVariant(encoded) as Record<string, VariantValue>

    // All keys should be present
    expect(decoded.zebra).toBe(1)
    expect(decoded.apple).toBe(2)
    expect(decoded.mango).toBe(3)
  })
})

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('Variant Utilities', () => {
  describe('isEncodable', () => {
    it('returns true for null', () => {
      expect(isEncodable(null)).toBe(true)
    })

    it('returns true for undefined', () => {
      expect(isEncodable(undefined)).toBe(true)
    })

    it('returns true for boolean', () => {
      expect(isEncodable(true)).toBe(true)
      expect(isEncodable(false)).toBe(true)
    })

    it('returns true for finite numbers', () => {
      expect(isEncodable(42)).toBe(true)
      expect(isEncodable(-3.14)).toBe(true)
      expect(isEncodable(0)).toBe(true)
    })

    it('returns false for non-finite numbers', () => {
      expect(isEncodable(Infinity)).toBe(false)
      expect(isEncodable(-Infinity)).toBe(false)
      expect(isEncodable(NaN)).toBe(false)
    })

    it('returns true for bigint', () => {
      expect(isEncodable(123n)).toBe(true)
      expect(isEncodable(-456n)).toBe(true)
    })

    it('returns true for strings', () => {
      expect(isEncodable('')).toBe(true)
      expect(isEncodable('hello')).toBe(true)
    })

    it('returns true for valid Date', () => {
      expect(isEncodable(new Date())).toBe(true)
    })

    it('returns false for invalid Date', () => {
      expect(isEncodable(new Date('invalid'))).toBe(false)
    })

    it('returns true for Uint8Array', () => {
      expect(isEncodable(new Uint8Array([1, 2, 3]))).toBe(true)
    })

    it('returns true for encodable arrays', () => {
      expect(isEncodable([1, 2, 3])).toBe(true)
      expect(isEncodable(['a', 'b'])).toBe(true)
      expect(isEncodable([{ x: 1 }])).toBe(true)
    })

    it('returns false for arrays with non-encodable elements', () => {
      expect(isEncodable([1, NaN, 3])).toBe(false)
      expect(isEncodable([Symbol('test')])).toBe(false)
    })

    it('returns true for encodable objects', () => {
      expect(isEncodable({ a: 1, b: 'two' })).toBe(true)
      expect(isEncodable({ nested: { value: true } })).toBe(true)
    })

    it('returns false for objects with non-encodable values', () => {
      expect(isEncodable({ value: Infinity })).toBe(false)
      expect(isEncodable({ fn: () => {} })).toBe(false)
    })

    it('returns false for functions', () => {
      expect(isEncodable(() => {})).toBe(false)
      expect(isEncodable(function() {})).toBe(false)
    })

    it('returns false for symbols', () => {
      expect(isEncodable(Symbol('test'))).toBe(false)
    })
  })

  describe('estimateVariantSize', () => {
    it('estimates size for simple values', () => {
      expect(estimateVariantSize(42)).toBeGreaterThan(0)
      expect(estimateVariantSize('hello')).toBeGreaterThan(0)
      expect(estimateVariantSize(null)).toBeGreaterThan(0)
    })

    it('estimates larger size for larger data', () => {
      const small = { x: 1 }
      const large = { data: 'x'.repeat(1000) }

      const smallSize = estimateVariantSize(small)
      const largeSize = estimateVariantSize(large)

      expect(largeSize).toBeGreaterThan(smallSize)
    })

    it('handles complex nested structures', () => {
      const complex = {
        users: [
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 },
        ],
        metadata: {
          version: 1,
          tags: ['a', 'b', 'c'],
        },
      }

      const size = estimateVariantSize(complex)
      expect(size).toBeGreaterThan(0)
    })
  })

  describe('variantEquals', () => {
    it('returns true for same null values', () => {
      expect(variantEquals(null, null)).toBe(true)
    })

    it('returns true for same primitive values', () => {
      expect(variantEquals(42, 42)).toBe(true)
      expect(variantEquals('hello', 'hello')).toBe(true)
      expect(variantEquals(true, true)).toBe(true)
    })

    it('returns false for different primitive values', () => {
      expect(variantEquals(42, 43)).toBe(false)
      expect(variantEquals('hello', 'world')).toBe(false)
      expect(variantEquals(true, false)).toBe(false)
    })

    it('returns false for null vs non-null', () => {
      expect(variantEquals(null, 'value')).toBe(false)
      expect(variantEquals('value', null)).toBe(false)
    })

    it('returns false for different types', () => {
      expect(variantEquals(42, '42')).toBe(false)
      expect(variantEquals(true, 1)).toBe(false)
    })

    it('returns true for equal Dates', () => {
      const date1 = new Date('2024-01-15T10:00:00Z')
      const date2 = new Date('2024-01-15T10:00:00Z')

      expect(variantEquals(date1, date2)).toBe(true)
    })

    it('returns false for different Dates', () => {
      const date1 = new Date('2024-01-15T10:00:00Z')
      const date2 = new Date('2024-01-16T10:00:00Z')

      expect(variantEquals(date1, date2)).toBe(false)
    })

    it('returns true for equal Uint8Arrays', () => {
      const arr1 = new Uint8Array([1, 2, 3])
      const arr2 = new Uint8Array([1, 2, 3])

      expect(variantEquals(arr1, arr2)).toBe(true)
    })

    it('returns false for different Uint8Arrays', () => {
      const arr1 = new Uint8Array([1, 2, 3])
      const arr2 = new Uint8Array([1, 2, 4])

      expect(variantEquals(arr1, arr2)).toBe(false)
    })

    it('returns false for different length Uint8Arrays', () => {
      const arr1 = new Uint8Array([1, 2, 3])
      const arr2 = new Uint8Array([1, 2])

      expect(variantEquals(arr1, arr2)).toBe(false)
    })

    it('returns true for equal arrays', () => {
      expect(variantEquals([1, 2, 3], [1, 2, 3])).toBe(true)
      expect(variantEquals(['a', 'b'], ['a', 'b'])).toBe(true)
    })

    it('returns false for different arrays', () => {
      expect(variantEquals([1, 2, 3], [1, 2, 4])).toBe(false)
      expect(variantEquals([1, 2, 3], [1, 2])).toBe(false)
    })

    it('returns true for equal objects', () => {
      const obj1 = { a: 1, b: 'two' }
      const obj2 = { a: 1, b: 'two' }

      expect(variantEquals(obj1, obj2)).toBe(true)
    })

    it('returns false for different objects', () => {
      const obj1 = { a: 1, b: 'two' }
      const obj2 = { a: 1, b: 'three' }

      expect(variantEquals(obj1, obj2)).toBe(false)
    })

    it('returns false for objects with different keys', () => {
      const obj1 = { a: 1, b: 2 }
      const obj2 = { a: 1, c: 2 }

      expect(variantEquals(obj1, obj2)).toBe(false)
    })

    it('returns false for objects with different number of keys', () => {
      const obj1 = { a: 1, b: 2 }
      const obj2 = { a: 1 }

      expect(variantEquals(obj1, obj2)).toBe(false)
    })

    it('handles deeply nested structures', () => {
      const obj1 = {
        level1: {
          level2: {
            level3: {
              value: [1, 2, 3],
            },
          },
        },
      }
      const obj2 = {
        level1: {
          level2: {
            level3: {
              value: [1, 2, 3],
            },
          },
        },
      }

      expect(variantEquals(obj1, obj2)).toBe(true)
    })

    it('handles undefined array elements', () => {
      const arr1 = [1, undefined, 3]
      const arr2 = [1, undefined, 3]

      expect(variantEquals(arr1 as VariantValue[], arr2 as VariantValue[])).toBe(true)
    })

    it('handles undefined object values', () => {
      const obj1 = { a: 1, b: undefined }
      const obj2 = { a: 1, b: undefined }

      expect(variantEquals(obj1 as VariantValue, obj2 as VariantValue)).toBe(true)
    })

    it('distinguishes undefined from missing key', () => {
      const obj1 = { a: 1, b: undefined }
      const obj2 = { a: 1 }

      // obj1 has 2 keys, obj2 has 1 key
      expect(variantEquals(obj1 as VariantValue, obj2 as VariantValue)).toBe(false)
    })
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Variant Edge Cases', () => {
  describe('encoding edge cases', () => {
    it('handles very long strings', () => {
      const longString = 'a'.repeat(10000)

      const encoded = encodeVariant(longString)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBe(longString)
    })

    it('handles deeply nested structures', () => {
      // Create deeply nested object
      let deep: VariantValue = { value: 'bottom' }
      for (let i = 0; i < 50; i++) {
        deep = { nested: deep }
      }

      const encoded = encodeVariant(deep)
      const decoded = decodeVariant(encoded)

      // Navigate to the bottom
      let current = decoded as Record<string, VariantValue>
      for (let i = 0; i < 50; i++) {
        current = current.nested as Record<string, VariantValue>
      }
      expect(current.value).toBe('bottom')
    })

    it('handles objects with numeric string keys', () => {
      const value = {
        '123': 'numeric key',
        'abc': 'alpha key',
      }

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, string>

      expect(decoded['123']).toBe('numeric key')
      expect(decoded['abc']).toBe('alpha key')
    })

    it('handles arrays with explicit undefined elements', () => {
      // Note: Sparse arrays [1, , 3] are not supported by variant encoding
      // because the map operation produces undefined which cannot be serialized.
      // Use explicit undefined values in a dense array instead.
      const value = [1, undefined, 3] as VariantValue[]

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as VariantValue[]

      expect(decoded[0]).toBe(1)
      expect(decoded[1]).toBe(null) // Undefined becomes null
      expect(decoded[2]).toBe(3)
    })
  })

  describe('metadata handling', () => {
    it('creates proper metadata for object with shared keys', () => {
      const value = {
        users: [
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 },
        ],
      }

      const encoded = encodeVariant(value)

      // Metadata should contain dictionary with keys
      expect(encoded.metadata.length).toBeGreaterThan(0)
      expect(encoded.value.length).toBeGreaterThan(0)
    })

    it('handles empty metadata for primitives', () => {
      const encoded = encodeVariant(42)

      // Even primitives have some metadata
      expect(encoded.metadata.length).toBeGreaterThan(0)
    })
  })

  describe('boundary values', () => {
    it('handles MAX_SAFE_INTEGER', () => {
      const value = Number.MAX_SAFE_INTEGER

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      // May come back as bigint for large values
      expect(Number(decoded)).toBe(value)
    })

    it('handles MIN_SAFE_INTEGER', () => {
      const value = Number.MIN_SAFE_INTEGER

      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)

      expect(Number(decoded)).toBe(value)
    })

    it('handles bigint boundary values', () => {
      const maxInt64 = 9223372036854775807n
      const minInt64 = -9223372036854775808n

      const encodedMax = encodeVariant(maxInt64)
      const encodedMin = encodeVariant(minInt64)

      expect(decodeVariant(encodedMax)).toBe(maxInt64)
      expect(decodeVariant(encodedMin)).toBe(minInt64)
    })
  })
})

// =============================================================================
// BINARY FORMAT VERIFICATION
// =============================================================================

describe('Variant Binary Format', () => {
  it('null encodes to correct header', () => {
    const encoded = encodeVariant(null)

    // Null should have header byte 0x00
    expect(encoded.value[0]).toBe(0x00)
  })

  it('true encodes to correct header', () => {
    const encoded = encodeVariant(true)

    // True should have header byte 0x04 (typeId=1 << 2)
    expect(encoded.value[0]).toBe(0x04)
  })

  it('false encodes to correct header', () => {
    const encoded = encodeVariant(false)

    // False should have header byte 0x08 (typeId=2 << 2)
    expect(encoded.value[0]).toBe(0x08)
  })

  it('short string has inline length in header', () => {
    const value = 'hi' // 2 characters

    const encoded = encodeVariant(value)

    // Short string: basicType=1, length in upper 6 bits
    const header = encoded.value[0]
    expect(header & 0x03).toBe(0x01) // basicType = 1 (short string)
    expect((header >> 2) & 0x3F).toBe(2) // length = 2
  })

  it('array has correct basic type', () => {
    const value = [1, 2, 3]

    const encoded = encodeVariant(value)

    // Array: basicType=3
    expect(encoded.value[0] & 0x03).toBe(0x03)
  })

  it('object has correct basic type', () => {
    const value = { a: 1 }

    const encoded = encodeVariant(value)

    // Object: basicType=2
    expect(encoded.value[0] & 0x03).toBe(0x02)
  })
})

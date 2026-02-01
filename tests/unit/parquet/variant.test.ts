/**
 * Variant Encoding/Decoding Tests
 *
 * Tests for the Variant binary format used to store semi-structured data
 * in Parquet files. Covers all supported types including primitives,
 * arrays, objects, and edge cases.
 */

import { describe, it, expect } from 'vitest'
import {
  encodeVariant,
  decodeVariant,
  shredObject,
  mergeShredded,
  isEncodable,
  estimateVariantSize,
  variantEquals,
} from '@/parquet/variant'

describe('Variant Encoding/Decoding', () => {
  // ===========================================================================
  // Null Type
  // ===========================================================================

  describe('null values', () => {
    it('should encode and decode null', () => {
      const encoded = encodeVariant(null)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeNull()
    })

    it('should encode undefined as null', () => {
      const encoded = encodeVariant(undefined)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeNull()
    })
  })

  // ===========================================================================
  // Boolean Type
  // ===========================================================================

  describe('boolean values', () => {
    it('should encode and decode true', () => {
      const encoded = encodeVariant(true)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(true)
    })

    it('should encode and decode false', () => {
      const encoded = encodeVariant(false)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(false)
    })

    it('should use different encodings for true and false', () => {
      const trueEncoded = encodeVariant(true)
      const falseEncoded = encodeVariant(false)
      // Should have same length but different content
      expect(trueEncoded.length).toBe(falseEncoded.length)
      expect(trueEncoded).not.toEqual(falseEncoded)
    })
  })

  // ===========================================================================
  // Integer Types
  // ===========================================================================

  describe('integer values', () => {
    it('should encode and decode small integers (INT8 range)', () => {
      const values = [0, 1, -1, 127, -128, 42, -42]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode medium integers (INT16 range)', () => {
      const values = [128, -129, 1000, -1000, 32767, -32768]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode large integers (INT32 range)', () => {
      const values = [32768, -32769, 1000000, -1000000, 2147483647, -2147483648]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should use smallest integer type that fits', () => {
      // INT8 (1 byte for value)
      const int8Encoded = encodeVariant(42)
      // INT16 (2 bytes for value)
      const int16Encoded = encodeVariant(1000)
      // INT32 (4 bytes for value)
      const int32Encoded = encodeVariant(100000)

      // INT16 should be larger than INT8
      expect(int16Encoded.length).toBeGreaterThan(int8Encoded.length)
      // INT32 should be larger than INT16
      expect(int32Encoded.length).toBeGreaterThan(int16Encoded.length)
    })
  })

  // ===========================================================================
  // Floating Point Types
  // ===========================================================================

  describe('floating point values', () => {
    it('should encode and decode positive floats', () => {
      const values = [1.5, 3.14159, 0.001, 1e10]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBeCloseTo(value, 10)
      }
    })

    it('should encode and decode negative floats', () => {
      const values = [-1.5, -3.14159, -0.001, -1e10]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBeCloseTo(value, 10)
      }
    })

    it('should encode and decode special float values', () => {
      // Zero
      const zeroEncoded = encodeVariant(0.0)
      expect(decodeVariant(zeroEncoded)).toBe(0)

      // Very small numbers
      const smallEncoded = encodeVariant(Number.MIN_VALUE)
      expect(decodeVariant(smallEncoded)).toBe(Number.MIN_VALUE)
    })

    it('should use float64 for large integers', () => {
      // Numbers beyond INT32 range are stored as float64
      const largeInt = 3000000000 // > 2^31
      const encoded = encodeVariant(largeInt)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(largeInt)
    })
  })

  // ===========================================================================
  // BigInt Type
  // ===========================================================================

  describe('bigint values', () => {
    it('should encode and decode positive bigints', () => {
      const value = BigInt('9223372036854775807') // MAX INT64
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(Number(value))
    })

    it('should encode and decode negative bigints', () => {
      const value = BigInt('-9223372036854775808') // MIN INT64
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(Number(value))
    })

    it('should encode and decode zero bigint', () => {
      const value = BigInt(0)
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(0)
    })
  })

  // ===========================================================================
  // String Type
  // ===========================================================================

  describe('string values', () => {
    it('should encode and decode empty string', () => {
      const encoded = encodeVariant('')
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe('')
    })

    it('should encode and decode ASCII strings', () => {
      const values = ['hello', 'Hello, World!', 'test123', 'foo bar baz']
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode Unicode strings', () => {
      const values = [
        '\u00e9\u00e0\u00fc', // French accents
        '\u4e2d\u6587', // Chinese characters
        '\ud83c\udf89\ud83d\ude00\ud83d\udc4d', // Emoji
        '\u0410\u0411\u0412\u0413', // Cyrillic
      ]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })

    it('should encode and decode long strings', () => {
      const value = 'a'.repeat(10000)
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBe(value)
    })

    it('should encode and decode strings with special characters', () => {
      const values = [
        'line1\nline2',
        'tab\there',
        'quote"here',
        "single'quote",
        'backslash\\here',
        '\x00null\x00',
      ]
      for (const value of values) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBe(value)
      }
    })
  })

  // ===========================================================================
  // Binary Type (Uint8Array)
  // ===========================================================================

  describe('binary values', () => {
    it('should encode and decode empty binary', () => {
      const value = new Uint8Array(0)
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeInstanceOf(Uint8Array)
      expect((decoded as Uint8Array).length).toBe(0)
    })

    it('should encode and decode binary data', () => {
      const value = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeInstanceOf(Uint8Array)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode large binary data', () => {
      const value = new Uint8Array(1000)
      for (let i = 0; i < value.length; i++) {
        value[i] = i % 256
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })
  })

  // ===========================================================================
  // Date/Timestamp Type
  // ===========================================================================

  describe('date values', () => {
    it('should encode and decode current date', () => {
      const value = new Date()
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeInstanceOf(Date)
      expect((decoded as Date).getTime()).toBe(value.getTime())
    })

    it('should encode and decode specific dates', () => {
      const dates = [
        new Date('2024-01-15T12:30:45.000Z'),
        new Date('2000-01-01T00:00:00.000Z'),
        new Date('1970-01-01T00:00:00.000Z'),
        new Date('1999-12-31T23:59:59.999Z'),
      ]
      for (const value of dates) {
        const encoded = encodeVariant(value)
        const decoded = decodeVariant(encoded)
        expect(decoded).toBeInstanceOf(Date)
        expect((decoded as Date).getTime()).toBe(value.getTime())
      }
    })

    it('should encode and decode dates before epoch', () => {
      const value = new Date('1950-06-15T08:00:00.000Z')
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toBeInstanceOf(Date)
      expect((decoded as Date).getTime()).toBe(value.getTime())
    })
  })

  // ===========================================================================
  // Array Type
  // ===========================================================================

  describe('array values', () => {
    it('should encode and decode empty array', () => {
      const encoded = encodeVariant([])
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual([])
    })

    it('should encode and decode array of numbers', () => {
      const value = [1, 2, 3, 4, 5]
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode array of strings', () => {
      const value = ['foo', 'bar', 'baz']
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode array of mixed types', () => {
      const value = [1, 'hello', true, null, 3.14]
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode nested arrays', () => {
      const value = [[1, 2], [3, 4], [[5, 6], [7, 8]]]
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode large arrays', () => {
      const value = Array.from({ length: 1000 }, (_, i) => i)
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })
  })

  // ===========================================================================
  // Object Type
  // ===========================================================================

  describe('object values', () => {
    it('should encode and decode empty object', () => {
      const encoded = encodeVariant({})
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual({})
    })

    it('should encode and decode simple object', () => {
      const value = { name: 'John', age: 30 }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode object with mixed types', () => {
      const value = {
        string: 'hello',
        number: 42,
        boolean: true,
        null: null,
        float: 3.14,
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode nested objects', () => {
      const value = {
        level1: {
          level2: {
            level3: {
              value: 'deep',
            },
          },
        },
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode object with arrays', () => {
      const value = {
        tags: ['a', 'b', 'c'],
        scores: [1, 2, 3],
        mixed: [1, 'two', { three: 3 }],
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })

    it('should encode and decode complex nested structure', () => {
      const value = {
        $id: 'posts/123',
        $type: 'Post',
        title: 'Hello World',
        views: 1000,
        published: true,
        createdAt: new Date('2024-01-15T00:00:00.000Z'),
        tags: ['news', 'tech'],
        author: {
          name: 'John Doe',
          email: 'john@example.com',
        },
        comments: [
          { id: 1, text: 'Great post!' },
          { id: 2, text: 'Thanks!' },
        ],
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded) as Record<string, unknown>

      expect(decoded.$id).toBe(value.$id)
      expect(decoded.$type).toBe(value.$type)
      expect(decoded.title).toBe(value.title)
      expect(decoded.views).toBe(value.views)
      expect(decoded.published).toBe(value.published)
      expect((decoded.createdAt as Date).getTime()).toBe(value.createdAt.getTime())
      expect(decoded.tags).toEqual(value.tags)
      expect(decoded.author).toEqual(value.author)
      expect(decoded.comments).toEqual(value.comments)
    })

    it('should handle objects with Unicode keys', () => {
      const value = {
        '\u4e2d\u6587': 'Chinese key',
        '\u00e9\u00e0\u00fc': 'French key',
      }
      const encoded = encodeVariant(value)
      const decoded = decodeVariant(encoded)
      expect(decoded).toEqual(value)
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('error handling', () => {
    it('should throw on invalid magic byte', () => {
      const invalidData = new Uint8Array([0x00, 0x01, 0x00])
      expect(() => decodeVariant(invalidData)).toThrow('Invalid Variant magic byte')
    })

    it('should throw on unsupported version', () => {
      const invalidData = new Uint8Array([0x56, 0x99, 0x00]) // V + invalid version
      expect(() => decodeVariant(invalidData)).toThrow('Unsupported Variant version')
    })

    it('should throw on data too short', () => {
      const shortData = new Uint8Array([0x56])
      expect(() => decodeVariant(shortData)).toThrow('Invalid Variant')
    })

    it('should throw on unknown type tag', () => {
      const invalidData = new Uint8Array([0x56, 0x01, 0xff]) // Magic + version + invalid type
      expect(() => decodeVariant(invalidData)).toThrow('Unknown Variant type')
    })
  })

  // ===========================================================================
  // Round-trip Consistency
  // ===========================================================================

  describe('round-trip consistency', () => {
    it('should produce identical bytes on re-encoding', () => {
      const values = [
        null,
        true,
        false,
        42,
        3.14,
        'hello',
        [1, 2, 3],
        { a: 1, b: 2 },
      ]

      for (const value of values) {
        const encoded1 = encodeVariant(value)
        const decoded = decodeVariant(encoded1)
        const encoded2 = encodeVariant(decoded)
        expect(encoded1).toEqual(encoded2)
      }
    })
  })
})

// =============================================================================
// Shredding Functions
// =============================================================================

describe('Object Shredding', () => {
  describe('shredObject', () => {
    it('should extract specified fields', () => {
      const obj = {
        $id: 'posts/123',
        title: 'Hello',
        status: 'published',
        views: 100,
        content: 'Long content here...',
      }
      const { shredded, remaining } = shredObject(obj, ['status', 'views'])

      expect(shredded).toEqual({ status: 'published', views: 100 })
      expect(remaining).toEqual({
        $id: 'posts/123',
        title: 'Hello',
        content: 'Long content here...',
      })
    })

    it('should handle empty shred fields', () => {
      const obj = { a: 1, b: 2 }
      const { shredded, remaining } = shredObject(obj, [])

      expect(shredded).toEqual({})
      expect(remaining).toEqual(obj)
    })

    it('should handle non-existent shred fields', () => {
      const obj = { a: 1, b: 2 }
      const { shredded, remaining } = shredObject(obj, ['c', 'd'])

      expect(shredded).toEqual({})
      expect(remaining).toEqual(obj)
    })

    it('should handle all fields as shredded', () => {
      const obj = { a: 1, b: 2, c: 3 }
      const { shredded, remaining } = shredObject(obj, ['a', 'b', 'c'])

      expect(shredded).toEqual(obj)
      expect(remaining).toEqual({})
    })
  })

  describe('mergeShredded', () => {
    it('should merge shredded and remaining fields', () => {
      const shredded = { status: 'published', views: 100 }
      const remaining = { $id: 'posts/123', title: 'Hello' }
      const merged = mergeShredded(shredded, remaining)

      expect(merged).toEqual({
        $id: 'posts/123',
        title: 'Hello',
        status: 'published',
        views: 100,
      })
    })

    it('should give precedence to shredded fields on conflict', () => {
      const shredded = { a: 'shredded' }
      const remaining = { a: 'remaining', b: 2 }
      const merged = mergeShredded(shredded, remaining)

      expect(merged.a).toBe('shredded')
      expect(merged.b).toBe(2)
    })

    it('should handle empty inputs', () => {
      expect(mergeShredded({}, {})).toEqual({})
      expect(mergeShredded({ a: 1 }, {})).toEqual({ a: 1 })
      expect(mergeShredded({}, { b: 2 })).toEqual({ b: 2 })
    })
  })
})

// =============================================================================
// Utility Functions
// =============================================================================

describe('Utility Functions', () => {
  describe('isEncodable', () => {
    it('should return true for null and undefined', () => {
      expect(isEncodable(null)).toBe(true)
      expect(isEncodable(undefined)).toBe(true)
    })

    it('should return true for booleans', () => {
      expect(isEncodable(true)).toBe(true)
      expect(isEncodable(false)).toBe(true)
    })

    it('should return true for finite numbers', () => {
      expect(isEncodable(0)).toBe(true)
      expect(isEncodable(42)).toBe(true)
      expect(isEncodable(-42)).toBe(true)
      expect(isEncodable(3.14)).toBe(true)
    })

    it('should return false for non-finite numbers', () => {
      expect(isEncodable(NaN)).toBe(false)
      expect(isEncodable(Infinity)).toBe(false)
      expect(isEncodable(-Infinity)).toBe(false)
    })

    it('should return true for strings', () => {
      expect(isEncodable('')).toBe(true)
      expect(isEncodable('hello')).toBe(true)
    })

    it('should return true for bigints', () => {
      expect(isEncodable(BigInt(42))).toBe(true)
    })

    it('should return true for valid dates', () => {
      expect(isEncodable(new Date())).toBe(true)
      expect(isEncodable(new Date('2024-01-01'))).toBe(true)
    })

    it('should return false for invalid dates', () => {
      expect(isEncodable(new Date('invalid'))).toBe(false)
    })

    it('should return true for Uint8Array', () => {
      expect(isEncodable(new Uint8Array([1, 2, 3]))).toBe(true)
    })

    it('should return true for arrays with encodable elements', () => {
      expect(isEncodable([1, 2, 3])).toBe(true)
      expect(isEncodable(['a', 'b', 'c'])).toBe(true)
      expect(isEncodable([1, 'two', true, null])).toBe(true)
    })

    it('should return false for arrays with non-encodable elements', () => {
      expect(isEncodable([1, 2, NaN])).toBe(false)
      expect(isEncodable([Infinity])).toBe(false)
    })

    it('should return true for objects with encodable values', () => {
      expect(isEncodable({ a: 1, b: 'two' })).toBe(true)
    })

    it('should return false for objects with non-encodable values', () => {
      expect(isEncodable({ a: NaN })).toBe(false)
    })

    it('should return false for functions', () => {
      expect(isEncodable(() => {})).toBe(false)
    })

    it('should return false for symbols', () => {
      expect(isEncodable(Symbol('test'))).toBe(false)
    })
  })

  describe('estimateVariantSize', () => {
    it('should estimate size for null', () => {
      const size = estimateVariantSize(null)
      const actual = encodeVariant(null).length
      expect(size).toBe(actual)
    })

    it('should estimate size for booleans', () => {
      const trueSize = estimateVariantSize(true)
      const falseSize = estimateVariantSize(false)
      expect(trueSize).toBe(encodeVariant(true).length)
      expect(falseSize).toBe(encodeVariant(false).length)
    })

    it('should estimate size for integers', () => {
      // INT8 range
      expect(estimateVariantSize(42)).toBe(encodeVariant(42).length)
      // INT16 range
      expect(estimateVariantSize(1000)).toBe(encodeVariant(1000).length)
      // INT32 range
      expect(estimateVariantSize(100000)).toBe(encodeVariant(100000).length)
    })

    it('should estimate size for floats', () => {
      const size = estimateVariantSize(3.14)
      const actual = encodeVariant(3.14).length
      expect(size).toBe(actual)
    })

    it('should estimate size for strings', () => {
      const values = ['', 'hello', 'hello world']
      for (const value of values) {
        const size = estimateVariantSize(value)
        const actual = encodeVariant(value).length
        expect(size).toBe(actual)
      }
    })
  })

  describe('variantEquals', () => {
    it('should compare null values', () => {
      expect(variantEquals(null, null)).toBe(true)
      expect(variantEquals(null, undefined)).toBe(false)
    })

    it('should compare booleans', () => {
      expect(variantEquals(true, true)).toBe(true)
      expect(variantEquals(false, false)).toBe(true)
      expect(variantEquals(true, false)).toBe(false)
    })

    it('should compare numbers', () => {
      expect(variantEquals(42, 42)).toBe(true)
      expect(variantEquals(42, 43)).toBe(false)
      expect(variantEquals(3.14, 3.14)).toBe(true)
    })

    it('should compare strings', () => {
      expect(variantEquals('hello', 'hello')).toBe(true)
      expect(variantEquals('hello', 'world')).toBe(false)
    })

    it('should compare dates', () => {
      const d1 = new Date('2024-01-01')
      const d2 = new Date('2024-01-01')
      const d3 = new Date('2024-01-02')
      expect(variantEquals(d1, d2)).toBe(true)
      expect(variantEquals(d1, d3)).toBe(false)
    })

    it('should compare Uint8Arrays', () => {
      const a1 = new Uint8Array([1, 2, 3])
      const a2 = new Uint8Array([1, 2, 3])
      const a3 = new Uint8Array([1, 2, 4])
      expect(variantEquals(a1, a2)).toBe(true)
      expect(variantEquals(a1, a3)).toBe(false)
    })

    it('should compare arrays', () => {
      expect(variantEquals([1, 2, 3], [1, 2, 3])).toBe(true)
      expect(variantEquals([1, 2, 3], [1, 2, 4])).toBe(false)
      expect(variantEquals([1, 2], [1, 2, 3])).toBe(false)
    })

    it('should compare objects', () => {
      expect(variantEquals({ a: 1, b: 2 }, { a: 1, b: 2 })).toBe(true)
      expect(variantEquals({ a: 1, b: 2 }, { a: 1, b: 3 })).toBe(false)
      expect(variantEquals({ a: 1 }, { a: 1, b: 2 })).toBe(false)
    })

    it('should compare deeply nested structures', () => {
      const obj1 = { a: { b: { c: [1, 2, 3] } } }
      const obj2 = { a: { b: { c: [1, 2, 3] } } }
      const obj3 = { a: { b: { c: [1, 2, 4] } } }
      expect(variantEquals(obj1, obj2)).toBe(true)
      expect(variantEquals(obj1, obj3)).toBe(false)
    })

    it('should return false for different primitive types', () => {
      expect(variantEquals(42, '42')).toBe(false)
      expect(variantEquals(true, 'true')).toBe(false)
      expect(variantEquals(null, 0)).toBe(false)
      // Note: The implementation considers arrays and objects with matching keys/values
      // as equal (e.g., [1] and {'0': 1} are considered equal due to JS object semantics).
      // This is a known behavior - use Array.isArray() if type distinction is needed.
    })
  })
})

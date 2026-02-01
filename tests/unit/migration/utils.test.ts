/**
 * Tests for migration utility functions
 */

import { describe, it, expect } from 'vitest'
import {
  getNestedValue,
  inferType,
  parseCsvLine,
  generateName,
} from '../../../src/migration/utils'

describe('getNestedValue', () => {
  it('gets top-level values', () => {
    const obj = { name: 'Alice', age: 30 }
    expect(getNestedValue(obj, 'name')).toBe('Alice')
    expect(getNestedValue(obj, 'age')).toBe(30)
  })

  it('gets nested values', () => {
    const obj = {
      user: {
        profile: {
          name: 'Alice',
          address: {
            city: 'Seattle',
          },
        },
      },
    }
    expect(getNestedValue(obj, 'user.profile.name')).toBe('Alice')
    expect(getNestedValue(obj, 'user.profile.address.city')).toBe('Seattle')
  })

  it('returns undefined for missing paths', () => {
    const obj = { name: 'Alice' }
    expect(getNestedValue(obj, 'age')).toBeUndefined()
    expect(getNestedValue(obj, 'user.profile')).toBeUndefined()
  })

  it('handles null in path', () => {
    const obj = { user: null }
    expect(getNestedValue(obj, 'user.name')).toBeUndefined()
  })

  it('handles non-object values in path', () => {
    const obj = { user: 'string' }
    expect(getNestedValue(obj, 'user.name')).toBeUndefined()
  })
})

describe('inferType', () => {
  describe('null values', () => {
    it('returns null for empty string', () => {
      expect(inferType('')).toBe(null)
      expect(inferType('   ')).toBe(null)
    })

    it('returns null for "null" and "undefined"', () => {
      expect(inferType('null')).toBe(null)
      expect(inferType('NULL')).toBe(null)
      expect(inferType('undefined')).toBe(null)
    })
  })

  describe('boolean values', () => {
    it('infers true', () => {
      expect(inferType('true')).toBe(true)
      expect(inferType('TRUE')).toBe(true)
      expect(inferType('True')).toBe(true)
    })

    it('infers false', () => {
      expect(inferType('false')).toBe(false)
      expect(inferType('FALSE')).toBe(false)
      expect(inferType('False')).toBe(false)
    })
  })

  describe('number values', () => {
    it('infers integers', () => {
      expect(inferType('42')).toBe(42)
      expect(inferType('0')).toBe(0)
      expect(inferType('-123')).toBe(-123)
    })

    it('infers floats', () => {
      expect(inferType('3.14')).toBe(3.14)
      expect(inferType('-0.5')).toBe(-0.5)
      expect(inferType('.25')).toBe(0.25)
    })

    it('preserves very large numbers', () => {
      const bigNum = '9007199254740992' // Beyond safe integer
      const result = inferType(bigNum)
      expect(typeof result).toBe('number')
    })
  })

  describe('date values', () => {
    it('infers ISO dates', () => {
      expect(inferType('2024-01-15')).toBeInstanceOf(Date)
      expect(inferType('2024-01-15T10:30:00Z')).toBeInstanceOf(Date)
      expect(inferType('2024-01-15T10:30:00.000Z')).toBeInstanceOf(Date)
    })

    it('does not infer invalid dates', () => {
      expect(inferType('2024-13-45')).toBe('2024-13-45') // Invalid date stays as string
    })
  })

  describe('JSON values', () => {
    it('infers JSON arrays', () => {
      expect(inferType('["a","b","c"]')).toEqual(['a', 'b', 'c'])
      expect(inferType('[1,2,3]')).toEqual([1, 2, 3])
    })

    it('infers JSON objects', () => {
      expect(inferType('{"key":"value"}')).toEqual({ key: 'value' })
    })

    it('returns string for invalid JSON-like values', () => {
      expect(inferType('[invalid]')).toBe('[invalid]')
      expect(inferType('{invalid}')).toBe('{invalid}')
    })
  })

  describe('string values', () => {
    it('returns trimmed strings', () => {
      expect(inferType('hello')).toBe('hello')
      expect(inferType('  hello  ')).toBe('hello')
    })

    it('preserves strings that look like but are not numbers', () => {
      expect(inferType('123abc')).toBe('123abc')
      expect(inferType('12.34.56')).toBe('12.34.56')
    })
  })
})

describe('parseCsvLine', () => {
  describe('simple fields', () => {
    it('parses comma-separated values', () => {
      expect(parseCsvLine('a,b,c')).toEqual(['a', 'b', 'c'])
    })

    it('handles empty fields', () => {
      expect(parseCsvLine('a,,c')).toEqual(['a', '', 'c'])
      expect(parseCsvLine(',b,')).toEqual(['', 'b', ''])
    })

    it('handles single field', () => {
      expect(parseCsvLine('single')).toEqual(['single'])
    })

    it('handles empty line', () => {
      expect(parseCsvLine('')).toEqual([''])
    })
  })

  describe('custom delimiters', () => {
    it('uses semicolon delimiter', () => {
      expect(parseCsvLine('a;b;c', ';')).toEqual(['a', 'b', 'c'])
    })

    it('uses tab delimiter', () => {
      expect(parseCsvLine('a\tb\tc', '\t')).toEqual(['a', 'b', 'c'])
    })

    it('uses pipe delimiter', () => {
      expect(parseCsvLine('a|b|c', '|')).toEqual(['a', 'b', 'c'])
    })
  })

  describe('quoted fields', () => {
    it('handles simple quoted fields', () => {
      expect(parseCsvLine('"a","b","c"')).toEqual(['a', 'b', 'c'])
    })

    it('handles quoted fields with commas', () => {
      expect(parseCsvLine('"a,b","c,d"')).toEqual(['a,b', 'c,d'])
    })

    it('handles mixed quoted and unquoted', () => {
      expect(parseCsvLine('a,"b,c",d')).toEqual(['a', 'b,c', 'd'])
    })

    it('handles empty quoted fields', () => {
      expect(parseCsvLine('a,"",c')).toEqual(['a', '', 'c'])
    })
  })

  describe('escaped quotes', () => {
    it('handles doubled quotes', () => {
      expect(parseCsvLine('"say ""hello"""')).toEqual(['say "hello"'])
    })

    it('handles multiple escaped quotes', () => {
      expect(parseCsvLine('"a""b""c"')).toEqual(['a"b"c'])
    })
  })

  describe('edge cases', () => {
    it('handles quotes in the middle of unquoted field', () => {
      // This is technically invalid CSV, but we handle it
      expect(parseCsvLine('a"b"c,d')).toEqual(['abc', 'd'])
    })

    it('handles unclosed quotes', () => {
      // Treats the rest of the line as the quoted field
      expect(parseCsvLine('"unclosed,field')).toEqual(['unclosed,field'])
    })

    it('handles newlines in quoted fields', () => {
      expect(parseCsvLine('"line1\nline2"')).toEqual(['line1\nline2'])
    })
  })
})

describe('generateName', () => {
  it('uses name field if present', () => {
    expect(generateName({ name: 'Alice', other: 'value' }, 'User')).toBe('Alice')
  })

  it('uses title field if no name', () => {
    expect(generateName({ title: 'My Post' }, 'Post')).toBe('My Post')
  })

  it('uses label field if no name or title', () => {
    expect(generateName({ label: 'Option 1' }, 'Option')).toBe('Option 1')
  })

  it('uses displayName field', () => {
    expect(generateName({ displayName: 'John Doe' }, 'User')).toBe('John Doe')
  })

  it('uses username field', () => {
    expect(generateName({ username: 'johnd' }, 'User')).toBe('johnd')
  })

  it('uses email field', () => {
    expect(generateName({ email: 'john@example.com' }, 'User')).toBe('john@example.com')
  })

  it('uses slug field', () => {
    expect(generateName({ slug: 'my-article' }, 'Article')).toBe('my-article')
  })

  it('falls back to _id field', () => {
    expect(generateName({ _id: '12345' }, 'Doc')).toBe('12345')
  })

  it('handles MongoDB ObjectId format', () => {
    expect(generateName({ _id: { $oid: '507f1f77bcf86cd799439011' } }, 'Doc'))
      .toBe('507f1f77bcf86cd799439011')
  })

  it('uses id field', () => {
    expect(generateName({ id: 'item-123' }, 'Item')).toBe('item-123')
  })

  it('generates name from type when no fields found', () => {
    const name = generateName({ value: 100 }, 'Item')
    expect(name).toMatch(/^Item-\d+$/)
  })

  it('prefers name fields over id fields', () => {
    expect(generateName({ name: 'Named', _id: 'id123' }, 'Thing')).toBe('Named')
  })
})

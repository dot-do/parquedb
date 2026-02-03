/**
 * Tests for nullish type guards and helpers
 *
 * These tests verify the behavior of the nullish checking utilities
 * in src/utils/type-utils.ts. The guards provide consistent patterns
 * for null/undefined checks across the codebase.
 */

import { describe, it, expect } from 'vitest'
import {
  isNullish,
  isNotNullish,
  isObject,
  isArray,
  getNestedValue,
  coalesce,
  coalesceDefault,
} from '../../../src/utils/type-utils'

// =============================================================================
// isNullish
// =============================================================================

describe('isNullish', () => {
  describe('returns true for null and undefined', () => {
    it('returns true for null', () => {
      expect(isNullish(null)).toBe(true)
    })

    it('returns true for undefined', () => {
      expect(isNullish(undefined)).toBe(true)
    })
  })

  describe('returns false for falsy non-nullish values', () => {
    it('returns false for 0', () => {
      expect(isNullish(0)).toBe(false)
    })

    it('returns false for empty string', () => {
      expect(isNullish('')).toBe(false)
    })

    it('returns false for false', () => {
      expect(isNullish(false)).toBe(false)
    })

    it('returns false for NaN', () => {
      expect(isNullish(NaN)).toBe(false)
    })
  })

  describe('returns false for truthy values', () => {
    it('returns false for non-zero numbers', () => {
      expect(isNullish(1)).toBe(false)
      expect(isNullish(-1)).toBe(false)
    })

    it('returns false for non-empty strings', () => {
      expect(isNullish('hello')).toBe(false)
    })

    it('returns false for true', () => {
      expect(isNullish(true)).toBe(false)
    })

    it('returns false for objects', () => {
      expect(isNullish({})).toBe(false)
      expect(isNullish({ a: 1 })).toBe(false)
    })

    it('returns false for arrays', () => {
      expect(isNullish([])).toBe(false)
      expect(isNullish([1, 2, 3])).toBe(false)
    })
  })
})

// =============================================================================
// isNotNullish
// =============================================================================

describe('isNotNullish', () => {
  describe('returns false for null and undefined', () => {
    it('returns false for null', () => {
      expect(isNotNullish(null)).toBe(false)
    })

    it('returns false for undefined', () => {
      expect(isNotNullish(undefined)).toBe(false)
    })
  })

  describe('returns true for falsy non-nullish values', () => {
    it('returns true for 0', () => {
      expect(isNotNullish(0)).toBe(true)
    })

    it('returns true for empty string', () => {
      expect(isNotNullish('')).toBe(true)
    })

    it('returns true for false', () => {
      expect(isNotNullish(false)).toBe(true)
    })
  })

  describe('provides proper type narrowing', () => {
    it('narrows union types', () => {
      const value: string | null | undefined = 'hello'
      if (isNotNullish(value)) {
        // TypeScript should now know value is string
        expect(value.toUpperCase()).toBe('HELLO')
      }
    })

    it('handles number | null | undefined', () => {
      const value: number | null | undefined = 0
      if (isNotNullish(value)) {
        // TypeScript should now know value is number
        expect(value.toFixed(2)).toBe('0.00')
      }
    })
  })
})

// =============================================================================
// isObject
// =============================================================================

describe('isObject', () => {
  describe('returns true for plain objects', () => {
    it('returns true for empty object', () => {
      expect(isObject({})).toBe(true)
    })

    it('returns true for object with properties', () => {
      expect(isObject({ a: 1, b: 2 })).toBe(true)
    })

    it('returns true for object created with Object.create', () => {
      expect(isObject(Object.create(null))).toBe(true)
    })
  })

  describe('returns false for non-objects', () => {
    it('returns false for null', () => {
      expect(isObject(null)).toBe(false)
    })

    it('returns false for undefined', () => {
      expect(isObject(undefined)).toBe(false)
    })

    it('returns false for arrays', () => {
      expect(isObject([])).toBe(false)
      expect(isObject([1, 2, 3])).toBe(false)
    })

    it('returns false for primitives', () => {
      expect(isObject(42)).toBe(false)
      expect(isObject('string')).toBe(false)
      expect(isObject(true)).toBe(false)
    })

    it('returns false for functions', () => {
      expect(isObject(() => {})).toBe(false)
    })
  })
})

// =============================================================================
// isArray
// =============================================================================

describe('isArray', () => {
  describe('returns true for arrays', () => {
    it('returns true for empty array', () => {
      expect(isArray([])).toBe(true)
    })

    it('returns true for array with elements', () => {
      expect(isArray([1, 2, 3])).toBe(true)
    })

    it('returns true for mixed-type arrays', () => {
      expect(isArray([1, 'two', { three: 3 }])).toBe(true)
    })
  })

  describe('returns false for non-arrays', () => {
    it('returns false for null', () => {
      expect(isArray(null)).toBe(false)
    })

    it('returns false for undefined', () => {
      expect(isArray(undefined)).toBe(false)
    })

    it('returns false for objects', () => {
      expect(isArray({})).toBe(false)
      expect(isArray({ length: 3 })).toBe(false)
    })

    it('returns false for strings', () => {
      expect(isArray('hello')).toBe(false)
    })
  })
})

// =============================================================================
// getNestedValue
// =============================================================================

describe('getNestedValue', () => {
  describe('returns values at path', () => {
    it('returns top-level property', () => {
      expect(getNestedValue({ name: 'test' }, 'name')).toBe('test')
    })

    it('returns nested property', () => {
      expect(getNestedValue({ user: { name: 'test' } }, 'user.name')).toBe('test')
    })

    it('returns deeply nested property', () => {
      const obj = { a: { b: { c: { d: 'value' } } } }
      expect(getNestedValue(obj, 'a.b.c.d')).toBe('value')
    })
  })

  describe('returns undefined for invalid paths', () => {
    it('returns undefined for missing top-level property', () => {
      expect(getNestedValue({ name: 'test' }, 'missing')).toBeUndefined()
    })

    it('returns undefined for missing nested property', () => {
      expect(getNestedValue({ user: { name: 'test' } }, 'user.missing')).toBeUndefined()
    })

    it('returns undefined when intermediate value is null', () => {
      expect(getNestedValue({ user: null }, 'user.name')).toBeUndefined()
    })

    it('returns undefined when intermediate value is undefined', () => {
      expect(getNestedValue({ user: undefined }, 'user.name')).toBeUndefined()
    })

    it('returns undefined when intermediate value is primitive', () => {
      expect(getNestedValue({ user: 'string' }, 'user.name')).toBeUndefined()
    })
  })

  describe('handles edge cases', () => {
    it('returns undefined for null object', () => {
      expect(getNestedValue(null, 'path')).toBeUndefined()
    })

    it('returns undefined for undefined object', () => {
      expect(getNestedValue(undefined, 'path')).toBeUndefined()
    })

    it('handles array access through dot notation', () => {
      const obj = { items: { '0': 'first' } }
      expect(getNestedValue(obj, 'items.0')).toBe('first')
    })
  })
})

// =============================================================================
// coalesce
// =============================================================================

describe('coalesce', () => {
  describe('returns first non-nullish value', () => {
    it('returns first value if not nullish', () => {
      expect(coalesce('first', 'second')).toBe('first')
    })

    it('skips null values', () => {
      expect(coalesce(null, 'second')).toBe('second')
    })

    it('skips undefined values', () => {
      expect(coalesce(undefined, 'second')).toBe('second')
    })

    it('skips multiple nullish values', () => {
      expect(coalesce(null, undefined, null, 'fourth')).toBe('fourth')
    })
  })

  describe('preserves falsy non-nullish values', () => {
    it('returns 0 instead of fallback', () => {
      expect(coalesce(0, 100)).toBe(0)
    })

    it('returns empty string instead of fallback', () => {
      expect(coalesce('', 'fallback')).toBe('')
    })

    it('returns false instead of fallback', () => {
      expect(coalesce(false, true)).toBe(false)
    })
  })

  describe('returns undefined when all values are nullish', () => {
    it('returns undefined for all null', () => {
      expect(coalesce(null, null, null)).toBeUndefined()
    })

    it('returns undefined for all undefined', () => {
      expect(coalesce(undefined, undefined)).toBeUndefined()
    })

    it('returns undefined for mixed nullish', () => {
      expect(coalesce(null, undefined, null)).toBeUndefined()
    })
  })
})

// =============================================================================
// coalesceDefault
// =============================================================================

describe('coalesceDefault', () => {
  describe('returns first non-nullish value', () => {
    it('returns first value if not nullish', () => {
      expect(coalesceDefault('default', 'first', 'second')).toBe('first')
    })

    it('skips nullish values', () => {
      expect(coalesceDefault('default', null, undefined, 'found')).toBe('found')
    })
  })

  describe('returns default when all values are nullish', () => {
    it('returns default for all null', () => {
      expect(coalesceDefault('default', null, null)).toBe('default')
    })

    it('returns default for all undefined', () => {
      expect(coalesceDefault('default', undefined)).toBe('default')
    })

    it('returns default for no additional values', () => {
      expect(coalesceDefault('default')).toBe('default')
    })
  })

  describe('preserves falsy non-nullish values', () => {
    it('returns 0 instead of default', () => {
      expect(coalesceDefault(100, 0)).toBe(0)
    })

    it('returns empty string instead of default', () => {
      expect(coalesceDefault('default', '')).toBe('')
    })

    it('returns false instead of default', () => {
      expect(coalesceDefault(true, false)).toBe(false)
    })
  })
})

// =============================================================================
// Type narrowing integration tests
// =============================================================================

describe('type narrowing integration', () => {
  it('isNullish correctly narrows in if statements', () => {
    function processValue(value: string | null | undefined): string {
      if (isNullish(value)) {
        return 'default'
      }
      return value.toUpperCase()
    }

    expect(processValue('hello')).toBe('HELLO')
    expect(processValue(null)).toBe('default')
    expect(processValue(undefined)).toBe('default')
  })

  it('isNotNullish correctly narrows in filter', () => {
    const values: (string | null | undefined)[] = ['a', null, 'b', undefined, 'c']
    const filtered = values.filter(isNotNullish)

    // TypeScript should know filtered is string[]
    expect(filtered).toEqual(['a', 'b', 'c'])
    expect(filtered.map(s => s.toUpperCase())).toEqual(['A', 'B', 'C'])
  })

  it('isObject correctly narrows for property access', () => {
    function getName(value: unknown): string {
      if (isObject(value) && typeof value.name === 'string') {
        return value.name
      }
      return 'unknown'
    }

    expect(getName({ name: 'test' })).toBe('test')
    expect(getName(null)).toBe('unknown')
    expect(getName('string')).toBe('unknown')
  })
})

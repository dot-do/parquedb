/**
 * Tests for JSON validation utilities
 */

import { describe, it, expect } from 'vitest'
import {
  isRecord,
  isArray,
  isString,
  isNumber,
  isBoolean,
  JsonParseError,
  JsonValidationError,
  safeJsonParse,
  parseJsonRecord,
  parseJsonArray,
  parseRecordOrThrow,
  parseArrayOrThrow,
  validateSchema,
  parseWithSchema,
  parseStoredData,
  parseStoredArray,
  tryParseJson,
  parseWithGuard,
} from '../../../src/utils/json-validation'

// =============================================================================
// Type Guards
// =============================================================================

describe('isRecord', () => {
  it('returns true for plain objects', () => {
    expect(isRecord({})).toBe(true)
    expect(isRecord({ key: 'value' })).toBe(true)
    expect(isRecord({ nested: { deep: true } })).toBe(true)
  })

  it('returns false for null', () => {
    expect(isRecord(null)).toBe(false)
  })

  it('returns false for arrays', () => {
    expect(isRecord([])).toBe(false)
    expect(isRecord([1, 2, 3])).toBe(false)
  })

  it('returns false for primitives', () => {
    expect(isRecord('string')).toBe(false)
    expect(isRecord(123)).toBe(false)
    expect(isRecord(true)).toBe(false)
    expect(isRecord(undefined)).toBe(false)
  })
})

describe('isArray', () => {
  it('returns true for arrays', () => {
    expect(isArray([])).toBe(true)
    expect(isArray([1, 2, 3])).toBe(true)
    expect(isArray([{ nested: true }])).toBe(true)
  })

  it('returns false for objects', () => {
    expect(isArray({})).toBe(false)
    expect(isArray({ 0: 'a', 1: 'b' })).toBe(false)
  })

  it('returns false for primitives', () => {
    expect(isArray('string')).toBe(false)
    expect(isArray(null)).toBe(false)
  })
})

describe('isString', () => {
  it('returns true for strings', () => {
    expect(isString('')).toBe(true)
    expect(isString('hello')).toBe(true)
  })

  it('returns false for non-strings', () => {
    expect(isString(123)).toBe(false)
    expect(isString(null)).toBe(false)
    expect(isString(undefined)).toBe(false)
  })
})

describe('isNumber', () => {
  it('returns true for finite numbers', () => {
    expect(isNumber(0)).toBe(true)
    expect(isNumber(42)).toBe(true)
    expect(isNumber(-3.14)).toBe(true)
  })

  it('returns false for Infinity and NaN', () => {
    expect(isNumber(Infinity)).toBe(false)
    expect(isNumber(-Infinity)).toBe(false)
    expect(isNumber(NaN)).toBe(false)
  })

  it('returns false for non-numbers', () => {
    expect(isNumber('42')).toBe(false)
    expect(isNumber(null)).toBe(false)
  })
})

describe('isBoolean', () => {
  it('returns true for booleans', () => {
    expect(isBoolean(true)).toBe(true)
    expect(isBoolean(false)).toBe(true)
  })

  it('returns false for truthy/falsy values', () => {
    expect(isBoolean(1)).toBe(false)
    expect(isBoolean(0)).toBe(false)
    expect(isBoolean('')).toBe(false)
    expect(isBoolean(null)).toBe(false)
  })
})

// =============================================================================
// Safe Parsing Functions
// =============================================================================

describe('safeJsonParse', () => {
  it('returns Ok for valid JSON', () => {
    const result = safeJsonParse('{"key": "value"}')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toEqual({ key: 'value' })
    }
  })

  it('parses primitive values', () => {
    expect(safeJsonParse('42').ok && safeJsonParse('42').value).toBe(42)
    expect(safeJsonParse('"hello"').ok && safeJsonParse('"hello"').value).toBe('hello')
    expect(safeJsonParse('true').ok && safeJsonParse('true').value).toBe(true)
    expect(safeJsonParse('null').ok && safeJsonParse('null').value).toBe(null)
  })

  it('returns Err for invalid JSON', () => {
    const result = safeJsonParse('invalid json')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonParseError)
      expect(result.error.message).toContain('Failed to parse JSON')
    }
  })

  it('truncates long input in error messages', () => {
    const longInput = 'a'.repeat(200)
    const result = safeJsonParse(longInput)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.input.length).toBeLessThanOrEqual(103) // 100 + '...'
    }
  })
})

describe('parseJsonRecord', () => {
  it('returns Ok for valid object JSON', () => {
    const result = parseJsonRecord('{"name": "test", "count": 42}')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toEqual({ name: 'test', count: 42 })
    }
  })

  it('returns Err for array JSON', () => {
    const result = parseJsonRecord('[1, 2, 3]')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonValidationError)
      expect(result.error.message).toContain('Expected object')
    }
  })

  it('returns Err for primitive JSON', () => {
    const result = parseJsonRecord('"string"')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonValidationError)
    }
  })

  it('returns Err for invalid JSON', () => {
    const result = parseJsonRecord('not json')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonParseError)
    }
  })
})

describe('parseJsonArray', () => {
  it('returns Ok for valid array JSON', () => {
    const result = parseJsonArray('[1, "two", true]')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toEqual([1, 'two', true])
    }
  })

  it('returns Err for object JSON', () => {
    const result = parseJsonArray('{"key": "value"}')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonValidationError)
      expect(result.error.message).toContain('Expected array')
    }
  })

  it('returns Err for invalid JSON', () => {
    const result = parseJsonArray('not json')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonParseError)
    }
  })
})

// =============================================================================
// Throwing Variants
// =============================================================================

describe('parseRecordOrThrow', () => {
  it('returns record for valid object JSON', () => {
    const result = parseRecordOrThrow('{"key": "value"}')
    expect(result).toEqual({ key: 'value' })
  })

  it('throws for invalid JSON', () => {
    expect(() => parseRecordOrThrow('not json')).toThrow()
  })

  it('throws for non-object JSON', () => {
    expect(() => parseRecordOrThrow('[1, 2, 3]')).toThrow('Expected object')
  })

  it('includes context in error message', () => {
    expect(() => parseRecordOrThrow('invalid', 'Loading config')).toThrow('Loading config:')
  })
})

describe('parseArrayOrThrow', () => {
  it('returns array for valid array JSON', () => {
    const result = parseArrayOrThrow('[1, 2, 3]')
    expect(result).toEqual([1, 2, 3])
  })

  it('throws for non-array JSON', () => {
    expect(() => parseArrayOrThrow('{"key": "value"}')).toThrow('Expected array')
  })

  it('includes context in error message', () => {
    expect(() => parseArrayOrThrow('{}', 'Loading items')).toThrow('Loading items:')
  })
})

// =============================================================================
// Schema Validation
// =============================================================================

describe('validateSchema', () => {
  it('validates string type', () => {
    expect(validateSchema('hello', { type: 'string' })).toBe(true)
    expect(validateSchema(123, { type: 'string' })).toBe(false)
  })

  it('validates number type', () => {
    expect(validateSchema(42, { type: 'number' })).toBe(true)
    expect(validateSchema('42', { type: 'number' })).toBe(false)
    expect(validateSchema(NaN, { type: 'number' })).toBe(false)
  })

  it('validates boolean type', () => {
    expect(validateSchema(true, { type: 'boolean' })).toBe(true)
    expect(validateSchema(false, { type: 'boolean' })).toBe(true)
    expect(validateSchema(1, { type: 'boolean' })).toBe(false)
  })

  it('validates array type', () => {
    expect(validateSchema([1, 2, 3], { type: 'array' })).toBe(true)
    expect(validateSchema({}, { type: 'array' })).toBe(false)
  })

  it('validates array with item type', () => {
    expect(validateSchema([1, 2, 3], { type: 'array', items: { type: 'number' } })).toBe(true)
    expect(validateSchema(['a', 'b'], { type: 'array', items: { type: 'string' } })).toBe(true)
    expect(validateSchema([1, 'two'], { type: 'array', items: { type: 'number' } })).toBe(false)
  })

  it('validates object type', () => {
    expect(validateSchema({ key: 'value' }, { type: 'object' })).toBe(true)
    expect(validateSchema([], { type: 'object' })).toBe(false)
  })

  it('validates object with properties', () => {
    const schema = {
      type: 'object' as const,
      properties: {
        name: { type: 'string' as const, required: true },
        age: { type: 'number' as const },
      },
    }
    expect(validateSchema({ name: 'John', age: 30 }, schema)).toBe(true)
    expect(validateSchema({ name: 'John' }, schema)).toBe(true)
    expect(validateSchema({ age: 30 }, schema)).toBe(false) // missing required name
    expect(validateSchema({ name: 123 }, schema)).toBe(false) // wrong type
  })
})

describe('parseWithSchema', () => {
  it('returns Ok for valid JSON matching schema', () => {
    const result = parseWithSchema<{ name: string }>(
      '{"name": "test"}',
      { type: 'object', properties: { name: { type: 'string', required: true } } }
    )
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.name).toBe('test')
    }
  })

  it('returns Err for JSON not matching schema', () => {
    const result = parseWithSchema(
      '{"name": 123}',
      { type: 'object', properties: { name: { type: 'string' } } }
    )
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonValidationError)
    }
  })
})

// =============================================================================
// Convenience Functions
// =============================================================================

describe('parseStoredData', () => {
  it('returns parsed object for valid JSON', () => {
    const result = parseStoredData('{"key": "value"}')
    expect(result).toEqual({ key: 'value' })
  })

  it('returns empty object for invalid JSON', () => {
    const result = parseStoredData('invalid')
    expect(result).toEqual({})
  })

  it('returns empty object for non-object JSON', () => {
    const result = parseStoredData('[1, 2, 3]')
    expect(result).toEqual({})
  })

  it('calls error callback on failure', () => {
    const errors: Error[] = []
    parseStoredData('invalid', (err) => errors.push(err))
    expect(errors.length).toBe(1)
    expect(errors[0]).toBeInstanceOf(JsonParseError)
  })
})

describe('parseStoredArray', () => {
  it('returns parsed array for valid JSON', () => {
    const result = parseStoredArray('[1, 2, 3]')
    expect(result).toEqual([1, 2, 3])
  })

  it('returns empty array for invalid JSON', () => {
    const result = parseStoredArray('invalid')
    expect(result).toEqual([])
  })

  it('returns empty array for non-array JSON', () => {
    const result = parseStoredArray('{"key": "value"}')
    expect(result).toEqual([])
  })

  it('calls error callback on failure', () => {
    const errors: Error[] = []
    parseStoredArray('{}', (err) => errors.push(err))
    expect(errors.length).toBe(1)
    expect(errors[0]).toBeInstanceOf(JsonValidationError)
  })
})

describe('tryParseJson', () => {
  it('returns parsed value for valid JSON', () => {
    expect(tryParseJson('{"key": "value"}')).toEqual({ key: 'value' })
    expect(tryParseJson('[1, 2, 3]')).toEqual([1, 2, 3])
    expect(tryParseJson('42')).toBe(42)
  })

  it('returns undefined for invalid JSON', () => {
    expect(tryParseJson('invalid')).toBeUndefined()
  })
})

describe('parseWithGuard', () => {
  interface Config {
    name: string
    count: number
  }

  const isConfig = (v: unknown): v is Config =>
    isRecord(v) && isString(v.name) && isNumber(v.count)

  it('returns Ok for value passing type guard', () => {
    const result = parseWithGuard('{"name": "test", "count": 42}', isConfig, 'Config')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.name).toBe('test')
      expect(result.value.count).toBe(42)
    }
  })

  it('returns Err for value failing type guard', () => {
    const result = parseWithGuard('{"name": "test"}', isConfig, 'Config')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonValidationError)
      expect(result.error.message).toContain('Config')
    }
  })

  it('returns Err for invalid JSON', () => {
    const result = parseWithGuard('invalid', isConfig, 'Config')
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(JsonParseError)
    }
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('edge cases', () => {
  it('handles empty object JSON', () => {
    const result = parseJsonRecord('{}')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toEqual({})
    }
  })

  it('handles empty array JSON', () => {
    const result = parseJsonArray('[]')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toEqual([])
    }
  })

  it('handles nested structures', () => {
    const json = '{"nested": {"deep": {"value": [1, 2, 3]}}}'
    const result = parseJsonRecord(json)
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.nested).toEqual({ deep: { value: [1, 2, 3] } })
    }
  })

  it('handles unicode characters', () => {
    const result = parseJsonRecord('{"message": "Hello \\"World\\", from JSON"}')
    expect(result.ok).toBe(true)
  })

  it('handles null values in objects', () => {
    const result = parseJsonRecord('{"value": null}')
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.value).toBeNull()
    }
  })
})

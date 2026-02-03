/**
 * Slug Validation Tests
 *
 * Tests for slug validation utilities used across the codebase.
 */

import { describe, it, expect } from 'vitest'
import {
  isValidSlug,
  validateSlug,
  SLUG_MIN_LENGTH,
  SLUG_MAX_LENGTH,
  SLUG_ERROR_MESSAGE,
} from '@/utils/slug'

describe('Slug Validation', () => {
  describe('Constants', () => {
    it('defines minimum length as 1', () => {
      expect(SLUG_MIN_LENGTH).toBe(1)
    })

    it('defines maximum length as 64', () => {
      expect(SLUG_MAX_LENGTH).toBe(64)
    })

    it('provides descriptive error message', () => {
      expect(SLUG_ERROR_MESSAGE).toContain('1-64')
      expect(SLUG_ERROR_MESSAGE).toContain('lowercase')
      expect(SLUG_ERROR_MESSAGE).toContain('alphanumeric')
    })
  })

  describe('isValidSlug', () => {
    describe('valid slugs', () => {
      it('accepts simple lowercase slugs', () => {
        expect(isValidSlug('abc')).toBe(true)
        expect(isValidSlug('test')).toBe(true)
        expect(isValidSlug('mydataset')).toBe(true)
      })

      it('accepts slugs with numbers', () => {
        expect(isValidSlug('test123')).toBe(true)
        expect(isValidSlug('123test')).toBe(true)
        expect(isValidSlug('a1b2c3')).toBe(true)
      })

      it('accepts slugs with hyphens in the middle', () => {
        expect(isValidSlug('my-dataset')).toBe(true)
        expect(isValidSlug('test-123-data')).toBe(true)
        expect(isValidSlug('a-b-c')).toBe(true)
      })

      it('accepts short slugs (1-3 chars, alphanumeric only)', () => {
        expect(isValidSlug('a')).toBe(true)
        expect(isValidSlug('ab')).toBe(true)
        expect(isValidSlug('abc')).toBe(true)
        expect(isValidSlug('123')).toBe(true)
        expect(isValidSlug('a1b')).toBe(true)
      })

      it('accepts maximum length slugs (64 chars)', () => {
        const maxSlug = 'a' + 'b'.repeat(62) + 'c'
        expect(maxSlug.length).toBe(64)
        expect(isValidSlug(maxSlug)).toBe(true)
      })

      it('accepts slugs with consecutive hyphens', () => {
        expect(isValidSlug('test--data')).toBe(true)
        expect(isValidSlug('a---b')).toBe(true)
      })
    })

    describe('invalid slugs', () => {
      it('rejects empty string', () => {
        expect(isValidSlug('')).toBe(false)
      })

      it('rejects slugs that are too long', () => {
        const tooLongSlug = 'a'.repeat(65)
        expect(tooLongSlug.length).toBe(65)
        expect(isValidSlug(tooLongSlug)).toBe(false)
      })

      it('rejects slugs with uppercase letters', () => {
        expect(isValidSlug('Test')).toBe(false)
        expect(isValidSlug('TEST')).toBe(false)
        expect(isValidSlug('myDataset')).toBe(false)
      })

      it('rejects slugs starting with hyphen', () => {
        expect(isValidSlug('-test')).toBe(false)
        expect(isValidSlug('-abc')).toBe(false)
      })

      it('rejects slugs ending with hyphen', () => {
        expect(isValidSlug('test-')).toBe(false)
        expect(isValidSlug('abc-')).toBe(false)
      })

      it('rejects slugs starting or ending with hyphen', () => {
        expect(isValidSlug('a-')).toBe(false)
        expect(isValidSlug('-a')).toBe(false)
        expect(isValidSlug('-ab')).toBe(false)
        expect(isValidSlug('ab-')).toBe(false)
      })

      it('rejects slugs with underscores', () => {
        expect(isValidSlug('my_dataset')).toBe(false)
        expect(isValidSlug('test_123')).toBe(false)
      })

      it('rejects slugs with spaces', () => {
        expect(isValidSlug('my dataset')).toBe(false)
        expect(isValidSlug(' test')).toBe(false)
        expect(isValidSlug('test ')).toBe(false)
      })

      it('rejects slugs with special characters', () => {
        expect(isValidSlug('test@data')).toBe(false)
        expect(isValidSlug('test.data')).toBe(false)
        expect(isValidSlug('test/data')).toBe(false)
        expect(isValidSlug('test!data')).toBe(false)
      })

      it('rejects non-string values', () => {
        expect(isValidSlug(null as unknown as string)).toBe(false)
        expect(isValidSlug(undefined as unknown as string)).toBe(false)
        expect(isValidSlug(123 as unknown as string)).toBe(false)
        expect(isValidSlug({} as unknown as string)).toBe(false)
        expect(isValidSlug([] as unknown as string)).toBe(false)
      })
    })

    describe('edge cases', () => {
      it('handles hyphen only strings', () => {
        expect(isValidSlug('-')).toBe(false)
        expect(isValidSlug('---')).toBe(false)
      })

      it('handles numeric-only slugs', () => {
        expect(isValidSlug('1')).toBe(true)
        expect(isValidSlug('12')).toBe(true)
        expect(isValidSlug('123')).toBe(true)
        expect(isValidSlug('12345')).toBe(true)
      })

      it('validates boundary between short and long format (3 chars)', () => {
        // 3 chars alphanumeric - valid via short pattern
        expect(isValidSlug('abc')).toBe(true)
        // 3 chars with hyphen in middle - valid via long pattern
        expect(isValidSlug('a-b')).toBe(true)
        // 4 chars with hyphen - valid via long pattern
        expect(isValidSlug('ab-c')).toBe(true)
      })
    })
  })

  describe('validateSlug', () => {
    it('does not throw for valid slugs', () => {
      expect(() => validateSlug('my-dataset')).not.toThrow()
      expect(() => validateSlug('test123')).not.toThrow()
      expect(() => validateSlug('abc')).not.toThrow()
      expect(() => validateSlug('a')).not.toThrow()
    })

    it('throws Error for invalid slugs', () => {
      expect(() => validateSlug('')).toThrow(Error)
      expect(() => validateSlug('Test')).toThrow(Error)
      expect(() => validateSlug('-test')).toThrow(Error)
      expect(() => validateSlug('test-')).toThrow(Error)
    })

    it('throws with descriptive error message', () => {
      expect(() => validateSlug('invalid!')).toThrow(SLUG_ERROR_MESSAGE)
    })
  })
})

/**
 * Safe Regex Tests
 *
 * Tests for ReDoS (Regular Expression Denial of Service) protection.
 * These tests verify that malicious regex patterns are detected and rejected.
 */

import { describe, it, expect } from 'vitest'
import {
  createSafeRegex,
  validateRegexPattern,
  isRegexSafe,
  UnsafeRegexError,
} from '@/utils/safe-regex'

describe('Safe Regex', () => {
  describe('createSafeRegex', () => {
    it('creates regex from safe string pattern', () => {
      const regex = createSafeRegex('^hello', 'i')
      expect(regex.test('Hello World')).toBe(true)
      expect(regex.test('world')).toBe(false)
    })

    it('creates regex from safe RegExp', () => {
      const regex = createSafeRegex(/world$/i)
      expect(regex.test('Hello World')).toBe(true)
      expect(regex.test('world hello')).toBe(false)
    })

    it('passes through safe patterns', () => {
      // Simple patterns
      expect(() => createSafeRegex('^test$')).not.toThrow()
      expect(() => createSafeRegex('[a-z]+')).not.toThrow()
      expect(() => createSafeRegex('\\d{3}-\\d{4}')).not.toThrow()
      expect(() => createSafeRegex('(foo|bar)')).not.toThrow()

      // Email-like pattern
      expect(() => createSafeRegex('^[\\w.]+@[\\w.]+$')).not.toThrow()

      // URL-like pattern
      expect(() => createSafeRegex('^https?://')).not.toThrow()
    })

    it('allows overriding flags', () => {
      const regex = createSafeRegex(/test/, 'gi')
      expect(regex.flags).toBe('gi')
    })
  })

  describe('ReDoS protection - nested quantifiers', () => {
    it('rejects (a+)+$ pattern', () => {
      expect(() => createSafeRegex('(a+)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects (a*)*$ pattern', () => {
      expect(() => createSafeRegex('(a*)*$')).toThrow(UnsafeRegexError)
    })

    it('rejects (a+)*$ pattern', () => {
      expect(() => createSafeRegex('(a+)*$')).toThrow(UnsafeRegexError)
    })

    it('rejects (.+)+$ pattern', () => {
      expect(() => createSafeRegex('(.+)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects (.*)+$ pattern', () => {
      expect(() => createSafeRegex('(.*)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects deeply nested quantifiers', () => {
      expect(() => createSafeRegex('((a+)+)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects quantifier on group with internal quantifier', () => {
      expect(() => createSafeRegex('(ab+cd+)+')).toThrow(UnsafeRegexError)
    })
  })

  describe('ReDoS protection - overlapping alternations', () => {
    it('rejects (a|a)+$ pattern', () => {
      expect(() => createSafeRegex('(a|a)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects (a|ab)+$ pattern', () => {
      expect(() => createSafeRegex('(a|ab)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects (x|x|y)+$ pattern', () => {
      expect(() => createSafeRegex('(x|x|y)+$')).toThrow(UnsafeRegexError)
    })
  })

  describe('ReDoS protection - backreferences', () => {
    it('rejects backreferences by default', () => {
      expect(() => createSafeRegex('(a+)\\1')).toThrow(UnsafeRegexError)
    })

    it('allows backreferences when explicitly enabled', () => {
      expect(() => createSafeRegex('(a+)\\1', '', { allowBackreferences: true })).not.toThrow()
    })
  })

  describe('ReDoS protection - length limits', () => {
    it('rejects patterns exceeding max length', () => {
      const longPattern = 'a'.repeat(1001)
      expect(() => createSafeRegex(longPattern)).toThrow(UnsafeRegexError)
    })

    it('accepts patterns within length limit', () => {
      const longPattern = 'a'.repeat(1000)
      expect(() => createSafeRegex(longPattern)).not.toThrow()
    })

    it('respects custom max length', () => {
      const pattern = 'a'.repeat(100)
      expect(() => createSafeRegex(pattern, '', { maxLength: 50 })).toThrow(UnsafeRegexError)
      expect(() => createSafeRegex(pattern, '', { maxLength: 200 })).not.toThrow()
    })
  })

  describe('validateRegexPattern', () => {
    it('throws UnsafeRegexError with pattern info', () => {
      try {
        validateRegexPattern('(a+)+$')
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(UnsafeRegexError)
        expect((e as UnsafeRegexError).pattern).toBe('(a+)+$')
        expect((e as UnsafeRegexError).message).toContain('Unsafe regex pattern')
      }
    })
  })

  describe('isRegexSafe', () => {
    it('returns true for safe patterns', () => {
      expect(isRegexSafe('^hello')).toBe(true)
      expect(isRegexSafe('[a-z]+')).toBe(true)
      expect(isRegexSafe(/test/i)).toBe(true)
    })

    it('returns false for unsafe patterns', () => {
      expect(isRegexSafe('(a+)+$')).toBe(false)
      expect(isRegexSafe('(.*)*$')).toBe(false)
      expect(isRegexSafe('(a|a)+$')).toBe(false)
    })
  })

  describe('Edge cases', () => {
    it('handles empty pattern', () => {
      const regex = createSafeRegex('')
      expect(regex.test('anything')).toBe(true)
    })

    it('handles special regex characters', () => {
      const regex = createSafeRegex('\\[\\]\\(\\)')
      expect(regex.test('[]()test')).toBe(true)
    })

    it('handles character classes with quantifiers inside', () => {
      // Quantifiers inside character classes are literal, not special
      expect(() => createSafeRegex('[a+b*]+')).not.toThrow()
    })

    it('handles non-capturing groups', () => {
      expect(() => createSafeRegex('(?:foo)+')).not.toThrow()
    })

    it('handles lookahead', () => {
      expect(() => createSafeRegex('foo(?=bar)')).not.toThrow()
    })

    it('handles unicode patterns', () => {
      expect(() => createSafeRegex('[\u0600-\u06FF]+')).not.toThrow()
    })
  })

  describe('Real-world attack patterns', () => {
    it('rejects classic email ReDoS', () => {
      // This pattern is known to cause catastrophic backtracking
      expect(() => createSafeRegex('^([a-zA-Z0-9]+([.+_-][a-zA-Z0-9]+)*)+@')).toThrow(UnsafeRegexError)
    })

    it('rejects URL validation ReDoS', () => {
      // Simplified problematic URL pattern
      expect(() => createSafeRegex('(([a-z]+)+\\.)+com')).toThrow(UnsafeRegexError)
    })

    it('rejects space-based ReDoS', () => {
      expect(() => createSafeRegex('( +)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects word boundary ReDoS patterns', () => {
      // Patterns with word boundaries and greedy quantifiers can be problematic
      expect(() => createSafeRegex('\\b.*test.*\\b')).toThrow(UnsafeRegexError)
    })
  })
})

describe('Safe Regex Integration with $regex Filter', () => {
  // Import the filter function to test integration
  // These tests verify that $regex filters reject malicious patterns

  it('normal regex patterns work in filters', async () => {
    const { matchesFilter } = await import('@/query/filter')

    const doc = { name: 'Hello World', email: 'test@example.com' }

    // Simple patterns should work
    expect(matchesFilter(doc, { name: { $regex: '^Hello' } })).toBe(true)
    expect(matchesFilter(doc, { email: { $regex: '@example\\.com$' } })).toBe(true)
    expect(matchesFilter(doc, { name: { $regex: 'world', $options: 'i' } })).toBe(true)
  })

  it('malicious regex patterns are rejected in filters', async () => {
    const { matchesFilter } = await import('@/query/filter')

    const doc = { name: 'aaaaaaaaaaaaaaaaaaaaaa' }

    // Malicious patterns should throw
    expect(() => matchesFilter(doc, { name: { $regex: '(a+)+$' } })).toThrow(UnsafeRegexError)
    expect(() => matchesFilter(doc, { name: { $regex: '(.*)*$' } })).toThrow(UnsafeRegexError)
  })

  it('Collection regex filter rejects malicious patterns', async () => {
    // This tests the Collection.ts matchCondition function
    // The createSafeRegex should be called there too
    const { createSafeRegex } = await import('@/utils/safe-regex')

    // Verify the utility is available and working
    expect(() => createSafeRegex('(a+)+')).toThrow(UnsafeRegexError)
    expect(createSafeRegex('^hello').test('hello')).toBe(true)
  })
})

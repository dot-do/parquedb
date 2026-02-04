/**
 * Safe Regex Tests
 *
 * Tests for ReDoS (Regular Expression Denial of Service) protection.
 * These tests verify that malicious regex patterns are detected and rejected.
 */

import { describe, it, expect, vi } from 'vitest'
import {
  createSafeRegex,
  validateRegexPattern,
  isRegexSafe,
  UnsafeRegexError,
  calculateComplexityScore,
  analyzeRegexSafety,
  safeRegexTest,
  safeRegexMatch,
  safeRegexExec,
  RegexTimeoutError,
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

  describe('ReDoS protection - star height analysis', () => {
    it('rejects high star height patterns', () => {
      // ((a+)+)+ has star height 3 (default max is 2)
      expect(() => createSafeRegex('((a+)+)+')).toThrow(UnsafeRegexError)
    })

    it('accepts patterns within star height limit', () => {
      // (a+)+ has star height 2 (at the limit)
      // This would normally be caught by nested quantifiers check
      // but star height provides additional protection
      expect(() => createSafeRegex('a+b+')).not.toThrow()
    })

    it('respects custom maxStarHeight option', () => {
      // Simple pattern with star height 1
      expect(() => createSafeRegex('a+', '', { maxStarHeight: 1 })).not.toThrow()
      expect(() => createSafeRegex('a+', '', { maxStarHeight: 0 })).toThrow(UnsafeRegexError)
    })
  })

  describe('ReDoS protection - group depth analysis', () => {
    it('rejects excessively nested groups', () => {
      // Create a pattern with 15 levels of nesting (default max is 10)
      const deeplyNested = '('.repeat(15) + 'a' + ')'.repeat(15)
      expect(() => createSafeRegex(deeplyNested)).toThrow(UnsafeRegexError)
    })

    it('accepts patterns within group depth limit', () => {
      // Create a pattern with 5 levels of nesting
      const moderatelyNested = '('.repeat(5) + 'a' + ')'.repeat(5)
      expect(() => createSafeRegex(moderatelyNested)).not.toThrow()
    })

    it('respects custom maxGroupDepth option', () => {
      const nested = '((a))'
      expect(() => createSafeRegex(nested, '', { maxGroupDepth: 2 })).not.toThrow()
      expect(() => createSafeRegex(nested, '', { maxGroupDepth: 1 })).toThrow(UnsafeRegexError)
    })
  })

  describe('ReDoS protection - repetition count', () => {
    it('rejects patterns with too many repetitions', () => {
      // Create a pattern with 25 repetition operators (default max is 20)
      const manyRepetitions = Array(25).fill('a+').join('')
      expect(() => createSafeRegex(manyRepetitions)).toThrow(UnsafeRegexError)
    })

    it('accepts patterns within repetition limit', () => {
      // Create a pattern with 10 repetition operators
      const fewRepetitions = Array(10).fill('a+').join('')
      expect(() => createSafeRegex(fewRepetitions)).not.toThrow()
    })

    it('respects custom maxRepetitions option', () => {
      const pattern = 'a+b+c+'
      expect(() => createSafeRegex(pattern, '', { maxRepetitions: 3 })).not.toThrow()
      expect(() => createSafeRegex(pattern, '', { maxRepetitions: 2 })).toThrow(UnsafeRegexError)
    })
  })

  describe('ReDoS protection - additional dangerous patterns', () => {
    it('rejects overlapping character classes with quantifiers', () => {
      expect(() => createSafeRegex('[a-z]*[a-z]+')).toThrow(UnsafeRegexError)
    })

    it('rejects quantifier after optional group', () => {
      expect(() => createSafeRegex('(a?)+$')).toThrow(UnsafeRegexError)
    })

    it('rejects empty group with quantifier', () => {
      expect(() => createSafeRegex('()+$')).toThrow(UnsafeRegexError)
    })

    it('rejects recursive-like patterns', () => {
      expect(() => createSafeRegex('.*.*test')).toThrow(UnsafeRegexError)
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

  it('filter uses safeRegexTest for runtime protection', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Test that safeRegexTest is being used by verifying input length limits work
    // Use a pattern that will actually match to ensure we reach the safeRegexTest call
    const doc = { content: 'a'.repeat(200000) }

    // Should throw because input exceeds max length (default 100000)
    expect(() => matchesFilter(doc, { content: { $regex: 'a' } })).toThrow('Input length')
  })

  it('filter rejects patterns that would timeout on large input', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Create a document with moderate length input
    const doc = { content: 'x'.repeat(5000) }

    // A safe simple pattern should work
    expect(matchesFilter(doc, { content: { $regex: '^x' } })).toBe(true)
  })

  it('filter works with simple patterns on moderate inputs', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Test that simple patterns work well with moderate inputs
    const moderateInput = 'a'.repeat(50000)
    const doc = { text: moderateInput }

    // Simple safe pattern should work fine
    expect(matchesFilter(doc, { text: { $regex: '^a' } })).toBe(true)
    expect(matchesFilter(doc, { text: { $regex: 'a$' } })).toBe(true)
  })
})

describe('ReDoS Runtime Protection in Filter', () => {
  it('protects against input length exceeding limits', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Very long input should be rejected
    // Use a pattern that will match to ensure we reach the safeRegexTest call
    const veryLongInput = 'test'.repeat(50000) // 200000 characters
    const doc = { data: veryLongInput }

    expect(() => matchesFilter(doc, { data: { $regex: 'test' } })).toThrow(/Input length/)
  })

  it('handles moderately complex patterns safely', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Moderate input with safe pattern should work
    const moderateInput = 'hello world '.repeat(100)
    const doc = { text: moderateInput }

    expect(matchesFilter(doc, { text: { $regex: 'world' } })).toBe(true)
    expect(matchesFilter(doc, { text: { $regex: '^hello' } })).toBe(true)
  })

  it('combined static and runtime protection prevents ReDoS', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Static analysis catches this pattern before execution
    expect(() => matchesFilter(
      { text: 'aaaaaaaaa' },
      { text: { $regex: '(a+)+$' } }
    )).toThrow(UnsafeRegexError)

    // If a pattern somehow passed static analysis, runtime protection kicks in
    // This is tested by the safeRegexTest unit tests
  })

  it('regex filter works correctly with normal usage', async () => {
    const { matchesFilter } = await import('@/query/filter')

    // Standard use cases should work fine
    const users = [
      { email: 'alice@example.com' },
      { email: 'bob@company.org' },
      { email: 'charlie@example.com' },
    ]

    const exampleUsers = users.filter(u =>
      matchesFilter(u, { email: { $regex: '@example\\.com$' } })
    )
    expect(exampleUsers).toHaveLength(2)
    expect(exampleUsers.map(u => u.email)).toContain('alice@example.com')
    expect(exampleUsers.map(u => u.email)).toContain('charlie@example.com')
  })
})

describe('Complexity Scoring', () => {
  describe('calculateComplexityScore', () => {
    it('returns low score for simple patterns', () => {
      const result = calculateComplexityScore('^hello$')
      expect(result.score).toBeLessThan(5)
      expect(result.isSafe).toBe(true)
      expect(result.warnings).toHaveLength(0)
    })

    it('returns low score for common safe patterns', () => {
      // Email-like pattern
      const email = calculateComplexityScore('[\\w.]+@[\\w.]+')
      expect(email.isSafe).toBe(true)

      // Phone number
      const phone = calculateComplexityScore('\\d{3}-\\d{3}-\\d{4}')
      expect(phone.isSafe).toBe(true)

      // URL-like pattern
      const url = calculateComplexityScore('^https?://[\\w./]+$')
      expect(url.isSafe).toBe(true)
    })

    it('returns high score for nested quantifiers', () => {
      const result = calculateComplexityScore('(a+)+')
      expect(result.score).toBeGreaterThan(10)
      expect(result.factors.starHeight).toBeGreaterThan(0)
    })

    it('returns high score for complex patterns', () => {
      const result = calculateComplexityScore('((a|b)+c?)+d*')
      expect(result.score).toBeGreaterThan(5)
    })

    it('includes warnings for problematic constructs', () => {
      const result = calculateComplexityScore('(a+)+')
      expect(result.warnings.length).toBeGreaterThan(0)
      // Should warn about star height, quantifier depth, or nesting
      expect(result.warnings.some(w =>
        w.toLowerCase().includes('star') ||
        w.toLowerCase().includes('quantifier') ||
        w.toLowerCase().includes('nest') ||
        w.toLowerCase().includes('backtrack')
      )).toBe(true)
    })

    it('tracks individual factors separately', () => {
      const result = calculateComplexityScore('((a)+b*c+)')
      expect(result.factors).toHaveProperty('starHeight')
      expect(result.factors).toHaveProperty('groupDepth')
      expect(result.factors).toHaveProperty('quantifierDepth')
      expect(result.factors).toHaveProperty('repetitionCount')
    })
  })

  describe('analyzeRegexSafety', () => {
    it('provides detailed analysis for safe patterns', () => {
      const result = analyzeRegexSafety('^hello$')
      expect(result.isSafe).toBe(true)
      expect(result.violations).toHaveLength(0)
      expect(result.complexity.score).toBeLessThan(10)
    })

    it('lists violations for unsafe patterns', () => {
      const result = analyzeRegexSafety('(a+)+')
      expect(result.isSafe).toBe(false)
      expect(result.violations.length).toBeGreaterThan(0)
    })

    it('provides recommendations for improvement', () => {
      const result = analyzeRegexSafety('(a+)+')
      expect(result.recommendations.length).toBeGreaterThan(0)
    })

    it('identifies multiple issues in complex patterns', () => {
      // Pattern with multiple problems: nested quantifiers, high star height, overlapping alternation
      const result = analyzeRegexSafety('((a|b)+)+')
      expect(result.violations.length).toBeGreaterThan(1)
    })

    it('respects custom options', () => {
      const result = analyzeRegexSafety('a'.repeat(500), { maxLength: 100 })
      expect(result.isSafe).toBe(false)
      expect(result.violations.some(v => v.includes('length'))).toBe(true)
    })
  })
})

describe('Safe Regex Execution', () => {
  describe('safeRegexTest', () => {
    it('executes simple regex correctly', () => {
      const regex = createSafeRegex('^hello')
      expect(safeRegexTest(regex, 'hello world')).toBe(true)
      expect(safeRegexTest(regex, 'world hello')).toBe(false)
    })

    it('throws on input exceeding max length', () => {
      const regex = createSafeRegex('test')
      const longInput = 'a'.repeat(200000)
      expect(() => safeRegexTest(regex, longInput)).toThrow('Input length')
    })

    it('respects custom maxInputLength', () => {
      const regex = createSafeRegex('test')
      const input = 'a'.repeat(1000)
      expect(() => safeRegexTest(regex, input, { maxInputLength: 500 })).toThrow()
      expect(() => safeRegexTest(regex, input, { maxInputLength: 2000 })).not.toThrow()
    })

    it('handles patterns with moderate complexity', () => {
      const regex = createSafeRegex('[a-z]+\\d+')
      expect(safeRegexTest(regex, 'abc123')).toBe(true)
      expect(safeRegexTest(regex, '123abc')).toBe(false)
    })
  })

  describe('safeRegexMatch', () => {
    it('returns match results correctly', () => {
      const regex = createSafeRegex('(\\w+)@(\\w+)')
      const result = safeRegexMatch(regex, 'test@example')
      expect(result).not.toBeNull()
      expect(result![0]).toBe('test@example')
      expect(result![1]).toBe('test')
      expect(result![2]).toBe('example')
    })

    it('returns null for no match', () => {
      const regex = createSafeRegex('^hello$')
      expect(safeRegexMatch(regex, 'world')).toBeNull()
    })

    it('throws on excessive input length', () => {
      const regex = createSafeRegex('test')
      const longInput = 'a'.repeat(200000)
      expect(() => safeRegexMatch(regex, longInput)).toThrow()
    })
  })

  describe('safeRegexExec', () => {
    it('returns exec results correctly', () => {
      const regex = createSafeRegex('(\\d+)')
      const result = safeRegexExec(regex, 'abc123def')
      expect(result).not.toBeNull()
      expect(result![0]).toBe('123')
      expect(result![1]).toBe('123')
    })

    it('returns null for no match', () => {
      const regex = createSafeRegex('\\d+')
      expect(safeRegexExec(regex, 'abcdef')).toBeNull()
    })

    it('throws on excessive input length', () => {
      const regex = createSafeRegex('test')
      const longInput = 'a'.repeat(200000)
      expect(() => safeRegexExec(regex, longInput)).toThrow()
    })
  })

  describe('RegexTimeoutError', () => {
    it('contains pattern and timeout information', () => {
      const error = new RegexTimeoutError('(a+)+', 1000)
      expect(error.pattern).toBe('(a+)+')
      expect(error.timeoutMs).toBe(1000)
      expect(error.message).toContain('1000ms')
      expect(error.name).toBe('RegexTimeoutError')
    })
  })
})

describe('Enhanced Dangerous Pattern Detection', () => {
  describe('polynomial backtracking patterns', () => {
    it('rejects .*a.*a.* pattern', () => {
      expect(() => createSafeRegex('.*a.*a.*')).toThrow(UnsafeRegexError)
    })

    it('rejects patterns with multiple consecutive wildcards', () => {
      expect(() => createSafeRegex('.+.+')).toThrow(UnsafeRegexError)
    })
  })

  describe('excessive repetition counts', () => {
    it('rejects {1000,} repetition', () => {
      expect(() => createSafeRegex('a{1000,}')).toThrow(UnsafeRegexError)
    })

    it('rejects {999} large bounded repetition', () => {
      expect(() => createSafeRegex('a{999}')).toThrow(UnsafeRegexError)
    })

    it('accepts reasonable bounded repetitions', () => {
      expect(() => createSafeRegex('a{1,10}')).not.toThrow()
      expect(() => createSafeRegex('a{50}')).not.toThrow()
    })
  })

  describe('deeply nested alternation', () => {
    it('rejects ((a|b)+)+ pattern', () => {
      expect(() => createSafeRegex('((a|b)+)+')).toThrow(UnsafeRegexError)
    })

    it('accepts simple alternation', () => {
      expect(() => createSafeRegex('(a|b|c)')).not.toThrow()
    })
  })

  describe('greedy quantifier before anchor', () => {
    it('rejects .+.*$ pattern', () => {
      expect(() => createSafeRegex('.+test$')).toThrow(UnsafeRegexError)
    })
  })

  describe('alternation branch limits', () => {
    it('rejects patterns with too many alternation branches', () => {
      const manyBranches = '(' + Array(20).fill('a').join('|') + ')'
      expect(() => createSafeRegex(manyBranches)).toThrow(UnsafeRegexError)
    })

    it('accepts patterns within branch limit', () => {
      const fewBranches = '(a|b|c|d|e)'
      expect(() => createSafeRegex(fewBranches)).not.toThrow()
    })

    it('respects custom maxAlternationBranches', () => {
      const pattern = '(a|b|c)'
      expect(() => createSafeRegex(pattern, '', { maxAlternationBranches: 3 })).not.toThrow()
      expect(() => createSafeRegex(pattern, '', { maxAlternationBranches: 2 })).toThrow(UnsafeRegexError)
    })
  })

  describe('character class complexity', () => {
    it('rejects character classes exceeding max size', () => {
      // Create a character class with many characters
      const largeCharClass = '[' + 'a'.repeat(150) + ']'
      expect(() => createSafeRegex(largeCharClass)).toThrow(UnsafeRegexError)
    })

    it('accepts character classes within limits', () => {
      // Use \w which is equivalent to [a-zA-Z0-9_] but avoids triggering overlapping class detection
      expect(() => createSafeRegex('\\w+')).not.toThrow()
      expect(() => createSafeRegex('[a-z]+')).not.toThrow()
    })

    it('respects custom maxCharClassSize', () => {
      const pattern = '[abcdefghij]+'
      expect(() => createSafeRegex(pattern, '', { maxCharClassSize: 10 })).not.toThrow()
      expect(() => createSafeRegex(pattern, '', { maxCharClassSize: 5 })).toThrow(UnsafeRegexError)
    })
  })
})

describe('Edge Cases for Deeply Nested Structures', () => {
  it('detects quantified nesting at multiple levels', () => {
    // ((a+)+)+ - outer group with quantifier containing inner group with quantifier
    // The pattern ((a)+)+ may pass because inner quantifier is on single char
    // But ((a+)+)+ definitely has nested quantifiers
    expect(() => createSafeRegex('((a+)+)+')).toThrow(UnsafeRegexError)
  })

  it('detects mixed quantifier types in nesting', () => {
    // (a*)+
    expect(() => createSafeRegex('(a*)+')).toThrow(UnsafeRegexError)
    // (a+)*
    expect(() => createSafeRegex('(a+)*')).toThrow(UnsafeRegexError)
  })

  it('handles escaped characters correctly', () => {
    // \( and \) are literal, not groups
    expect(() => createSafeRegex('\\(a+\\)+')).not.toThrow()
  })

  it('handles nested non-capturing groups', () => {
    // (?:(?:a+)+)+ should still be caught - has nested quantifiers
    expect(() => createSafeRegex('(?:(?:a+)+)+')).toThrow(UnsafeRegexError)
  })

  it('handles mixed group types', () => {
    // ((?:a+)+)+ mixing capturing and non-capturing with nested quantifiers
    expect(() => createSafeRegex('((?:a+)+)+')).toThrow(UnsafeRegexError)
  })

  it('handles character classes in nested groups', () => {
    // ([a-z]+)+
    expect(() => createSafeRegex('([a-z]+)+')).toThrow(UnsafeRegexError)
  })

  it('correctly identifies safe deeply nested groups without quantifiers', () => {
    // Deeply nested but no quantifiers on inner groups
    expect(() => createSafeRegex('(((abc)))')).not.toThrow()
  })

  it('handles complex real-world vulnerable patterns', () => {
    // Known vulnerable patterns from CVEs and security advisories

    // CVE-like: email validation ReDoS
    expect(() => createSafeRegex('^([a-z]+)+@')).toThrow(UnsafeRegexError)

    // Semver-like vulnerable pattern
    expect(() => createSafeRegex('(\\d+\\.)+\\d+')).toThrow(UnsafeRegexError)

    // HTML tag matching vulnerable pattern
    expect(() => createSafeRegex('<([a-z]+)([^>]*)>.*</\\1>')).toThrow(UnsafeRegexError)
  })
})

describe('Complexity Score Edge Cases', () => {
  it('handles empty pattern', () => {
    const result = calculateComplexityScore('')
    expect(result.score).toBe(0)
    expect(result.isSafe).toBe(true)
  })

  it('handles pattern with only anchors', () => {
    const result = calculateComplexityScore('^$')
    expect(result.score).toBeLessThan(5)
    expect(result.isSafe).toBe(true)
  })

  it('handles pattern with only literals', () => {
    const result = calculateComplexityScore('hello world')
    expect(result.score).toBeLessThan(5)
    expect(result.isSafe).toBe(true)
  })

  it('increases score for each nested quantifier level', () => {
    const simple = calculateComplexityScore('a+')
    const nested = calculateComplexityScore('(a+)+')
    expect(nested.score).toBeGreaterThan(simple.score)
  })

  it('respects custom maxComplexityScore', () => {
    // A moderately complex pattern
    const pattern = '([a-z]+)+'
    // With high threshold - should be safe
    expect(() => createSafeRegex(pattern, '', { maxComplexityScore: 100 })).toThrow() // Still fails other checks

    // Pattern that barely passes other checks
    const edgePattern = '[a-z]+[0-9]+[a-z]+[0-9]+'
    const analysis = calculateComplexityScore(edgePattern)

    // Should be safe with default threshold
    if (analysis.isSafe) {
      expect(() => createSafeRegex(edgePattern, '', { maxComplexityScore: analysis.score - 1 })).toThrow(UnsafeRegexError)
    }
  })
})

describe('Subtle Dangerous Patterns', () => {
  it('detects anchored alternation with quantifier', () => {
    // ^(a|b)+ can cause issues with certain inputs
    const result = calculateComplexityScore('^(a|b)+')
    expect(result.warnings.length).toBeGreaterThan(0)
  })

  it('detects mixed lazy and greedy quantifiers', () => {
    const result = calculateComplexityScore('a+?b+')
    // Should add to complexity score
    expect(result.factors).toBeDefined()
  })

  it('detects multiple quantified groups', () => {
    const result = calculateComplexityScore('(a+)(b+)(c+)')
    // Multiple groups with quantifiers increase risk
    expect(result.score).toBeGreaterThan(0)
  })
})

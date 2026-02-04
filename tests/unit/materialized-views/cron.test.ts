/**
 * Cron Expression Validation Tests
 *
 * Comprehensive tests for cron expression validation with detailed error messages.
 */

import { describe, it, expect } from 'vitest'
import {
  validateCronExpression,
  isValidCronExpression,
  type CronValidationResult,
} from '@/materialized-views/scheduler'

// =============================================================================
// isValidCronExpression Tests
// =============================================================================

describe('isValidCronExpression', () => {
  describe('valid expressions', () => {
    it('accepts all wildcards', () => {
      expect(isValidCronExpression('* * * * *')).toBe(true)
    })

    it('accepts specific values for each field', () => {
      expect(isValidCronExpression('0 * * * *')).toBe(true)
      expect(isValidCronExpression('0 0 * * *')).toBe(true)
      expect(isValidCronExpression('30 4 1 * *')).toBe(true)
      expect(isValidCronExpression('0 12 15 6 1')).toBe(true)
    })

    it('accepts step expressions', () => {
      expect(isValidCronExpression('*/15 * * * *')).toBe(true)
      expect(isValidCronExpression('0 */2 * * *')).toBe(true)
      expect(isValidCronExpression('*/5 */4 */2 * *')).toBe(true)
      expect(isValidCronExpression('0-30/5 * * * *')).toBe(true)
    })

    it('accepts list expressions', () => {
      expect(isValidCronExpression('0,15,30,45 * * * *')).toBe(true)
      expect(isValidCronExpression('0 0 1,15 * *')).toBe(true)
      expect(isValidCronExpression('0 9,12,18 * * *')).toBe(true)
    })

    it('accepts range expressions', () => {
      expect(isValidCronExpression('0 9-17 * * *')).toBe(true)
      expect(isValidCronExpression('0 0 * * 1-5')).toBe(true)
      expect(isValidCronExpression('0-5 9-17 * * 1-5')).toBe(true)
    })

    it('accepts combined expressions', () => {
      expect(isValidCronExpression('0,30 9-17 * * 1-5')).toBe(true)
      expect(isValidCronExpression('*/15 9-17 1,15 * 1-5')).toBe(true)
    })

    it('accepts boundary values', () => {
      // Minute: 0-59
      expect(isValidCronExpression('0 * * * *')).toBe(true)
      expect(isValidCronExpression('59 * * * *')).toBe(true)
      // Hour: 0-23
      expect(isValidCronExpression('0 0 * * *')).toBe(true)
      expect(isValidCronExpression('0 23 * * *')).toBe(true)
      // Day of month: 1-31
      expect(isValidCronExpression('0 0 1 * *')).toBe(true)
      expect(isValidCronExpression('0 0 31 * *')).toBe(true)
      // Month: 1-12
      expect(isValidCronExpression('0 0 * 1 *')).toBe(true)
      expect(isValidCronExpression('0 0 * 12 *')).toBe(true)
      // Day of week: 0-6
      expect(isValidCronExpression('0 0 * * 0')).toBe(true)
      expect(isValidCronExpression('0 0 * * 6')).toBe(true)
    })

    it('accepts common scheduling patterns', () => {
      // Every minute
      expect(isValidCronExpression('* * * * *')).toBe(true)
      // Every hour
      expect(isValidCronExpression('0 * * * *')).toBe(true)
      // Daily at midnight
      expect(isValidCronExpression('0 0 * * *')).toBe(true)
      // Weekly on Sunday at midnight
      expect(isValidCronExpression('0 0 * * 0')).toBe(true)
      // Monthly on the 1st at midnight
      expect(isValidCronExpression('0 0 1 * *')).toBe(true)
      // Every 15 minutes
      expect(isValidCronExpression('*/15 * * * *')).toBe(true)
      // Weekdays at 9am
      expect(isValidCronExpression('0 9 * * 1-5')).toBe(true)
      // Every 5 minutes during business hours on weekdays
      expect(isValidCronExpression('*/5 9-17 * * 1-5')).toBe(true)
    })
  })

  describe('invalid expressions - wrong number of parts', () => {
    it('rejects empty string', () => {
      expect(isValidCronExpression('')).toBe(false)
    })

    it('rejects expressions with too few parts', () => {
      expect(isValidCronExpression('*')).toBe(false)
      expect(isValidCronExpression('* *')).toBe(false)
      expect(isValidCronExpression('* * *')).toBe(false)
      expect(isValidCronExpression('* * * *')).toBe(false)
    })

    it('rejects expressions with too many parts', () => {
      expect(isValidCronExpression('* * * * * *')).toBe(false)
      expect(isValidCronExpression('* * * * * * *')).toBe(false)
    })
  })

  describe('invalid expressions - out of range values', () => {
    it('rejects out-of-range minute values', () => {
      expect(isValidCronExpression('60 * * * *')).toBe(false)
      expect(isValidCronExpression('-1 * * * *')).toBe(false)
      expect(isValidCronExpression('100 * * * *')).toBe(false)
    })

    it('rejects out-of-range hour values', () => {
      expect(isValidCronExpression('0 24 * * *')).toBe(false)
      expect(isValidCronExpression('0 -1 * * *')).toBe(false)
      expect(isValidCronExpression('0 25 * * *')).toBe(false)
    })

    it('rejects out-of-range day of month values', () => {
      expect(isValidCronExpression('0 0 0 * *')).toBe(false)
      expect(isValidCronExpression('0 0 32 * *')).toBe(false)
      expect(isValidCronExpression('0 0 -1 * *')).toBe(false)
    })

    it('rejects out-of-range month values', () => {
      expect(isValidCronExpression('0 0 * 0 *')).toBe(false)
      expect(isValidCronExpression('0 0 * 13 *')).toBe(false)
      expect(isValidCronExpression('0 0 * -1 *')).toBe(false)
    })

    it('rejects out-of-range day of week values', () => {
      expect(isValidCronExpression('0 0 * * 7')).toBe(false)
      expect(isValidCronExpression('0 0 * * -1')).toBe(false)
      expect(isValidCronExpression('0 0 * * 8')).toBe(false)
    })
  })

  describe('invalid expressions - malformed syntax', () => {
    it('rejects invalid range expressions', () => {
      expect(isValidCronExpression('5-3 * * * *')).toBe(false) // start > end
      expect(isValidCronExpression('0-60 * * * *')).toBe(false) // end out of range
      expect(isValidCronExpression('-5-10 * * * *')).toBe(false) // start out of range
      expect(isValidCronExpression('10-100 * * * *')).toBe(false) // both out of range
    })

    it('rejects invalid step expressions', () => {
      expect(isValidCronExpression('*/0 * * * *')).toBe(false) // zero step
      expect(isValidCronExpression('*/-1 * * * *')).toBe(false) // negative step
      expect(isValidCronExpression('*/abc * * * *')).toBe(false) // non-numeric step
      expect(isValidCronExpression('*/ * * * *')).toBe(false) // missing step
    })

    it('rejects non-integer values', () => {
      expect(isValidCronExpression('1.5 * * * *')).toBe(false)
      expect(isValidCronExpression('abc * * * *')).toBe(false)
      expect(isValidCronExpression('0 12.5 * * *')).toBe(false)
    })

    it('rejects invalid list values', () => {
      expect(isValidCronExpression('0,60 * * * *')).toBe(false)
      expect(isValidCronExpression('0,abc * * * *')).toBe(false)
    })
  })

  describe('invalid expressions - type errors', () => {
    it('rejects null', () => {
      expect(isValidCronExpression(null as any)).toBe(false)
    })

    it('rejects undefined', () => {
      expect(isValidCronExpression(undefined as any)).toBe(false)
    })

    it('rejects non-string values', () => {
      expect(isValidCronExpression(123 as any)).toBe(false)
      expect(isValidCronExpression({} as any)).toBe(false)
      expect(isValidCronExpression([] as any)).toBe(false)
    })
  })
})

// =============================================================================
// validateCronExpression Tests
// =============================================================================

describe('validateCronExpression', () => {
  describe('valid expressions', () => {
    it('returns valid result for correct expressions', () => {
      const result = validateCronExpression('0 * * * *')
      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
      expect(result.field).toBeUndefined()
    })

    it('returns valid for various valid patterns', () => {
      expect(validateCronExpression('* * * * *').valid).toBe(true)
      expect(validateCronExpression('*/15 * * * *').valid).toBe(true)
      expect(validateCronExpression('0,30 9-17 * * 1-5').valid).toBe(true)
    })
  })

  describe('error messages - wrong number of parts', () => {
    it('provides detailed error for too few parts', () => {
      const result = validateCronExpression('* * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('5 fields')
      expect(result.error).toContain('got 3')
    })

    it('provides detailed error for too many parts', () => {
      const result = validateCronExpression('* * * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('5 fields')
      expect(result.error).toContain('got 6')
    })

    it('provides detailed error for empty string', () => {
      const result = validateCronExpression('')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('non-empty string')
    })

    it('provides detailed error for whitespace-only string', () => {
      const result = validateCronExpression('   ')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('cannot be empty')
    })
  })

  describe('error messages - out of range values', () => {
    it('provides detailed error for out-of-range minute', () => {
      const result = validateCronExpression('60 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain('60')
      expect(result.error).toContain('out of range')
      expect(result.error).toContain('0-59')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for out-of-range hour', () => {
      const result = validateCronExpression('0 24 * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('hour')
      expect(result.error).toContain('24')
      expect(result.error).toContain('0-23')
      expect(result.field).toBe('hour')
    })

    it('provides detailed error for out-of-range day of month', () => {
      const result = validateCronExpression('0 0 32 * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('day of month')
      expect(result.error).toContain('32')
      expect(result.error).toContain('1-31')
      expect(result.field).toBe('day of month')
    })

    it('provides detailed error for zero day of month', () => {
      const result = validateCronExpression('0 0 0 * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('day of month')
      expect(result.error).toContain('0')
      expect(result.error).toContain('1-31')
      expect(result.field).toBe('day of month')
    })

    it('provides detailed error for out-of-range month', () => {
      const result = validateCronExpression('0 0 * 13 *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('month')
      expect(result.error).toContain('13')
      expect(result.error).toContain('1-12')
      expect(result.field).toBe('month')
    })

    it('provides detailed error for zero month', () => {
      const result = validateCronExpression('0 0 * 0 *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('month')
      expect(result.error).toContain('0')
      expect(result.error).toContain('1-12')
      expect(result.field).toBe('month')
    })

    it('provides detailed error for out-of-range day of week', () => {
      const result = validateCronExpression('0 0 * * 7')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('day of week')
      expect(result.error).toContain('7')
      expect(result.error).toContain('0-6')
      expect(result.field).toBe('day of week')
    })
  })

  describe('error messages - invalid range syntax', () => {
    it('provides detailed error for inverted range', () => {
      const result = validateCronExpression('5-3 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('start (5)')
      expect(result.error).toContain('greater than end (3)')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for range with out-of-bound start', () => {
      const result = validateCronExpression('-5-10 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for range with out-of-bound end', () => {
      const result = validateCronExpression('0-60 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('range end')
      expect(result.error).toContain('60')
      expect(result.error).toContain('out of range')
      expect(result.field).toBe('minute')
    })
  })

  describe('error messages - invalid step syntax', () => {
    it('provides detailed error for zero step', () => {
      const result = validateCronExpression('*/0 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('step')
      expect(result.error).toContain("'0'")
      expect(result.error).toContain('positive integer')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for negative step', () => {
      const result = validateCronExpression('*/-1 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('step')
      expect(result.error).toContain("'-1'")
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for non-numeric step', () => {
      const result = validateCronExpression('*/abc * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('step')
      expect(result.error).toContain("'abc'")
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for missing step', () => {
      const result = validateCronExpression('*/ * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('missing step')
      expect(result.field).toBe('minute')
    })
  })

  describe('error messages - invalid value syntax', () => {
    it('provides detailed error for non-integer value', () => {
      const result = validateCronExpression('1.5 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain("'1.5'")
      expect(result.error).toContain('not a valid integer')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for alphabetic value', () => {
      const result = validateCronExpression('abc * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain("'abc'")
      expect(result.error).toContain('not a valid integer')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for invalid value in list', () => {
      const result = validateCronExpression('0,60 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain('60')
      expect(result.field).toBe('minute')
    })
  })

  describe('error messages - type errors', () => {
    it('provides detailed error for null', () => {
      const result = validateCronExpression(null as any)
      expect(result.valid).toBe(false)
      expect(result.error).toContain('non-empty string')
    })

    it('provides detailed error for undefined', () => {
      const result = validateCronExpression(undefined as any)
      expect(result.valid).toBe(false)
      expect(result.error).toContain('non-empty string')
    })
  })

  describe('field identification', () => {
    it('identifies minute field errors', () => {
      expect(validateCronExpression('60 * * * *').field).toBe('minute')
    })

    it('identifies hour field errors', () => {
      expect(validateCronExpression('0 24 * * *').field).toBe('hour')
    })

    it('identifies day of month field errors', () => {
      expect(validateCronExpression('0 0 32 * *').field).toBe('day of month')
    })

    it('identifies month field errors', () => {
      expect(validateCronExpression('0 0 * 13 *').field).toBe('month')
    })

    it('identifies day of week field errors', () => {
      expect(validateCronExpression('0 0 * * 7').field).toBe('day of week')
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  it('handles extra whitespace between fields', () => {
    expect(isValidCronExpression('0  *  *  *  *')).toBe(true)
    expect(isValidCronExpression('0   *   *   *   *')).toBe(true)
  })

  it('handles leading/trailing whitespace', () => {
    expect(isValidCronExpression('  0 * * * *  ')).toBe(true)
    expect(isValidCronExpression('\t0 * * * *\t')).toBe(true)
  })

  it('handles complex valid expressions', () => {
    // First and fifteenth of every month at midnight
    expect(isValidCronExpression('0 0 1,15 * *')).toBe(true)
    // Every 5 minutes from 9am to 5pm on weekdays
    expect(isValidCronExpression('*/5 9-17 * * 1-5')).toBe(true)
    // At 9:30am on the first Monday of every month
    // Note: This doesn't truly express "first Monday" but is valid syntax
    expect(isValidCronExpression('30 9 1-7 * 1')).toBe(true)
  })

  it('correctly validates boundary value ranges', () => {
    // Full minute range
    expect(isValidCronExpression('0-59 * * * *')).toBe(true)
    // Full hour range
    expect(isValidCronExpression('0 0-23 * * *')).toBe(true)
    // Full day of month range
    expect(isValidCronExpression('0 0 1-31 * *')).toBe(true)
    // Full month range
    expect(isValidCronExpression('0 0 * 1-12 *')).toBe(true)
    // Full day of week range
    expect(isValidCronExpression('0 0 * * 0-6')).toBe(true)
  })
})

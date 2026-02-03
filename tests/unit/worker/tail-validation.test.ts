/**
 * Tail Handler Input Validation Tests
 *
 * Comprehensive tests for runtime validation of tail handler input.
 * Tests cover:
 * - Valid TraceItem validation
 * - Invalid input handling (non-array, malformed objects)
 * - Required field validation
 * - Type validation for all fields
 * - Graceful degradation for partial valid data
 * - Configuration options
 */

import { describe, it, expect, vi } from 'vitest'
import {
  validateTraceItem,
  validateTraceItems,
  isValidTraceItem,
  createTailValidationError,
  type ValidatedTraceItem,
  type TailValidationConfig,
} from '@/worker/tail-validation'
import { ValidationError } from '@/errors'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a valid TraceItem for testing
 */
function createValidTraceItem(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
  return {
    scriptName: 'test-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    event: {
      request: {
        url: 'https://example.com/api/test',
        method: 'GET',
        headers: { 'user-agent': 'test' },
        cf: { colo: 'DFW', country: 'US' },
      },
      response: { status: 200 },
    },
    logs: [
      { timestamp: Date.now(), level: 'info', message: 'Test log' },
    ],
    exceptions: [],
    diagnosticsChannelEvents: [],
    ...overrides,
  }
}

// =============================================================================
// validateTraceItem Tests
// =============================================================================

describe('validateTraceItem', () => {
  describe('valid input', () => {
    it('validates a complete valid TraceItem', () => {
      const item = createValidTraceItem()
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
      expect(result.item).not.toBeNull()
      expect(result.item!.scriptName).toBe('test-worker')
      expect(result.item!.outcome).toBe('ok')
    })

    it('validates TraceItem with null scriptName', () => {
      const item = createValidTraceItem({ scriptName: null })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.scriptName).toBeNull()
    })

    it('validates TraceItem with null eventTimestamp', () => {
      const item = createValidTraceItem({ eventTimestamp: null })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.eventTimestamp).toBeNull()
    })

    it('validates TraceItem with null event', () => {
      const item = createValidTraceItem({ event: null })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.event).toBeNull()
    })

    it('validates TraceItem with empty logs array', () => {
      const item = createValidTraceItem({ logs: [] })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(0)
    })

    it('validates TraceItem with multiple logs', () => {
      const item = createValidTraceItem({
        logs: [
          { timestamp: 1000, level: 'debug', message: 'Debug' },
          { timestamp: 2000, level: 'info', message: 'Info' },
          { timestamp: 3000, level: 'warn', message: 'Warning' },
          { timestamp: 4000, level: 'error', message: 'Error' },
        ],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(4)
    })

    it('validates TraceItem with exceptions', () => {
      const item = createValidTraceItem({
        exceptions: [
          { name: 'TypeError', message: 'null is not an object', timestamp: Date.now() },
        ],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(1)
      expect(result.item!.exceptions[0].name).toBe('TypeError')
    })

    it('validates TraceItem with scheduled event', () => {
      const item = createValidTraceItem({
        event: {
          scheduledTime: 1704067200000,
          cron: '0 * * * *',
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.event!.scheduledTime).toBe(1704067200000)
      expect(result.item!.event!.cron).toBe('0 * * * *')
    })

    it('validates TraceItem with queue event', () => {
      const item = createValidTraceItem({
        event: {
          queue: 'my-queue',
          batchSize: 10,
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.event!.queue).toBe('my-queue')
      expect(result.item!.event!.batchSize).toBe(10)
    })
  })

  describe('invalid input - root level', () => {
    it('rejects null input', () => {
      const result = validateTraceItem(null)

      expect(result.valid).toBe(false)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]).toBeInstanceOf(ValidationError)
      expect(result.errors[0].message).toContain('must be an object')
    })

    it('rejects undefined input', () => {
      const result = validateTraceItem(undefined)

      expect(result.valid).toBe(false)
      expect(result.errors).toHaveLength(1)
    })

    it('rejects string input', () => {
      const result = validateTraceItem('not an object')

      expect(result.valid).toBe(false)
      expect(result.errors[0].message).toContain('must be an object')
    })

    it('rejects number input', () => {
      const result = validateTraceItem(123)

      expect(result.valid).toBe(false)
    })

    it('rejects array input', () => {
      const result = validateTraceItem([])

      expect(result.valid).toBe(false)
      expect(result.errors[0].message).toContain('must be an object')
    })
  })

  describe('invalid input - missing required fields', () => {
    it('rejects missing outcome', () => {
      const item = createValidTraceItem()
      delete item.outcome
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('outcome'))).toBe(true)
    })

    it('rejects non-string outcome', () => {
      const item = createValidTraceItem({ outcome: 123 })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('outcome'))).toBe(true)
    })

    it('rejects missing logs array', () => {
      const item = createValidTraceItem()
      delete item.logs
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('logs'))).toBe(true)
    })

    it('rejects non-array logs', () => {
      const item = createValidTraceItem({ logs: 'not an array' })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('logs'))).toBe(true)
    })

    it('rejects missing exceptions array', () => {
      const item = createValidTraceItem()
      delete item.exceptions
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('exceptions'))).toBe(true)
    })

    it('rejects non-array exceptions', () => {
      const item = createValidTraceItem({ exceptions: {} })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('exceptions'))).toBe(true)
    })
  })

  describe('invalid input - optional field types', () => {
    it('rejects non-string scriptName (not null)', () => {
      const item = createValidTraceItem({ scriptName: 123 })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('scriptName'))).toBe(true)
    })

    it('rejects non-number eventTimestamp (not null)', () => {
      const item = createValidTraceItem({ eventTimestamp: 'not a number' })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('eventTimestamp'))).toBe(true)
    })

    it('rejects array event (not object)', () => {
      const item = createValidTraceItem({ event: ['not', 'an', 'object'] })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('event'))).toBe(true)
    })
  })

  describe('invalid input - log entries', () => {
    it('rejects log without timestamp', () => {
      const item = createValidTraceItem({
        logs: [{ level: 'info', message: 'test' }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true) // Still valid, but log is excluded
      expect(result.item!.logs).toHaveLength(0) // Invalid log excluded
      expect(result.errors).toHaveLength(1) // Error recorded
    })

    it('rejects log with non-number timestamp', () => {
      const item = createValidTraceItem({
        logs: [{ timestamp: 'not a number', level: 'info', message: 'test' }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(0)
    })

    it('rejects log without level', () => {
      const item = createValidTraceItem({
        logs: [{ timestamp: 1000, message: 'test' }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(0)
    })

    it('rejects log with non-string level', () => {
      const item = createValidTraceItem({
        logs: [{ timestamp: 1000, level: 123, message: 'test' }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(0)
    })

    it('rejects non-object log entries', () => {
      const item = createValidTraceItem({
        logs: ['not an object'],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(0)
    })

    it('keeps valid logs when some are invalid', () => {
      const item = createValidTraceItem({
        logs: [
          { timestamp: 1000, level: 'info', message: 'valid' },
          { level: 'error', message: 'missing timestamp' }, // invalid
          { timestamp: 2000, level: 'warn', message: 'also valid' },
        ],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(2)
      expect(result.errors).toHaveLength(1) // One invalid log
    })
  })

  describe('invalid input - exception entries', () => {
    it('rejects exception without name', () => {
      const item = createValidTraceItem({
        exceptions: [{ message: 'test', timestamp: 1000 }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(0)
    })

    it('rejects exception without message', () => {
      const item = createValidTraceItem({
        exceptions: [{ name: 'Error', timestamp: 1000 }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(0)
    })

    it('rejects exception without timestamp', () => {
      const item = createValidTraceItem({
        exceptions: [{ name: 'Error', message: 'test' }],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(0)
    })

    it('rejects non-object exception entries', () => {
      const item = createValidTraceItem({
        exceptions: [null, 'string', 123],
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(0)
    })
  })

  describe('invalid input - request validation', () => {
    it('rejects request without url', () => {
      const item = createValidTraceItem({
        event: {
          request: { method: 'GET', headers: {} },
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('url'))).toBe(true)
    })

    it('rejects request without method', () => {
      const item = createValidTraceItem({
        event: {
          request: { url: 'https://example.com', headers: {} },
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('method'))).toBe(true)
    })

    it('rejects request with non-object headers', () => {
      const item = createValidTraceItem({
        event: {
          request: { url: 'https://example.com', method: 'GET', headers: 'not an object' },
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.message.includes('headers'))).toBe(true)
    })

    it('accepts request with missing headers (defaults to empty)', () => {
      const item = createValidTraceItem({
        event: {
          request: { url: 'https://example.com', method: 'GET' },
        },
      })
      const result = validateTraceItem(item)

      expect(result.valid).toBe(true)
      expect(result.item!.event!.request!.headers).toEqual({})
    })
  })

  describe('configuration options', () => {
    it('throws on first error when throwOnError is true', () => {
      const item = createValidTraceItem({ outcome: 123 })

      expect(() => validateTraceItem(item, { throwOnError: true })).toThrow(ValidationError)
    })

    it('limits logs processed with maxLogsPerItem', () => {
      const item = createValidTraceItem({
        logs: Array.from({ length: 100 }, (_, i) => ({
          timestamp: i,
          level: 'info',
          message: `Log ${i}`,
        })),
      })

      const result = validateTraceItem(item, { maxLogsPerItem: 10 })

      expect(result.valid).toBe(true)
      expect(result.item!.logs).toHaveLength(10)
    })

    it('limits exceptions processed with maxExceptionsPerItem', () => {
      const item = createValidTraceItem({
        exceptions: Array.from({ length: 50 }, (_, i) => ({
          name: 'Error',
          message: `Exception ${i}`,
          timestamp: i,
        })),
      })

      const result = validateTraceItem(item, { maxExceptionsPerItem: 5 })

      expect(result.valid).toBe(true)
      expect(result.item!.exceptions).toHaveLength(5)
    })
  })
})

// =============================================================================
// validateTraceItems Tests
// =============================================================================

describe('validateTraceItems', () => {
  describe('valid input', () => {
    it('validates an array of valid TraceItems', () => {
      const items = [
        createValidTraceItem({ scriptName: 'worker-1' }),
        createValidTraceItem({ scriptName: 'worker-2' }),
        createValidTraceItem({ scriptName: 'worker-3' }),
      ]

      const result = validateTraceItems(items)

      expect(result.validItems).toHaveLength(3)
      expect(result.invalidItems).toHaveLength(0)
      expect(result.totalCount).toBe(3)
      expect(result.validCount).toBe(3)
      expect(result.invalidCount).toBe(0)
    })

    it('validates empty array', () => {
      const result = validateTraceItems([])

      expect(result.validItems).toHaveLength(0)
      expect(result.invalidItems).toHaveLength(0)
      expect(result.totalCount).toBe(0)
    })
  })

  describe('invalid root input', () => {
    it('rejects non-array input', () => {
      const result = validateTraceItems('not an array')

      expect(result.validItems).toHaveLength(0)
      expect(result.invalidItems).toHaveLength(1)
      expect(result.invalidItems[0].index).toBe(-1)
    })

    it('throws on non-array input when throwOnError is true', () => {
      expect(() => validateTraceItems('not an array', { throwOnError: true })).toThrow(ValidationError)
    })

    it('rejects null input', () => {
      const result = validateTraceItems(null)

      expect(result.validItems).toHaveLength(0)
      expect(result.invalidItems).toHaveLength(1)
    })

    it('rejects undefined input', () => {
      const result = validateTraceItems(undefined)

      expect(result.validItems).toHaveLength(0)
      expect(result.invalidItems).toHaveLength(1)
    })

    it('rejects object input', () => {
      const result = validateTraceItems({ not: 'an array' })

      expect(result.validItems).toHaveLength(0)
      expect(result.invalidItems).toHaveLength(1)
    })
  })

  describe('mixed valid/invalid items', () => {
    it('separates valid and invalid items', () => {
      const items = [
        createValidTraceItem({ scriptName: 'valid-1' }),
        { invalid: 'item' }, // Missing required fields
        createValidTraceItem({ scriptName: 'valid-2' }),
        null, // Not an object
        createValidTraceItem({ scriptName: 'valid-3' }),
      ]

      const result = validateTraceItems(items)

      expect(result.validItems).toHaveLength(3)
      expect(result.invalidItems).toHaveLength(2)
      expect(result.validCount).toBe(3)
      expect(result.invalidCount).toBe(2)
      expect(result.totalCount).toBe(5)
    })

    it('records correct indices for invalid items', () => {
      const items = [
        createValidTraceItem(), // index 0 - valid
        'invalid', // index 1 - invalid
        createValidTraceItem(), // index 2 - valid
        123, // index 3 - invalid
      ]

      const result = validateTraceItems(items)

      expect(result.invalidItems[0].index).toBe(1)
      expect(result.invalidItems[1].index).toBe(3)
    })

    it('includes validation errors for each invalid item', () => {
      const items = [
        { outcome: 123 }, // Invalid outcome type
        { logs: 'not array' }, // Invalid logs type
      ]

      const result = validateTraceItems(items)

      expect(result.invalidItems[0].errors.length).toBeGreaterThan(0)
      expect(result.invalidItems[1].errors.length).toBeGreaterThan(0)
    })
  })

  describe('configuration options', () => {
    it('limits items processed with maxItems', () => {
      const items = Array.from({ length: 100 }, () => createValidTraceItem())

      const result = validateTraceItems(items, { maxItems: 10 })

      expect(result.validItems).toHaveLength(10)
      expect(result.totalCount).toBe(100)
    })

    it('throws on first invalid item when throwOnError is true', () => {
      const items = [
        createValidTraceItem(),
        { invalid: 'item' },
        createValidTraceItem(),
      ]

      expect(() => validateTraceItems(items, { throwOnError: true })).toThrow(ValidationError)
    })
  })
})

// =============================================================================
// isValidTraceItem Tests
// =============================================================================

describe('isValidTraceItem', () => {
  it('returns true for valid TraceItem', () => {
    const item = createValidTraceItem()
    expect(isValidTraceItem(item)).toBe(true)
  })

  it('returns false for null', () => {
    expect(isValidTraceItem(null)).toBe(false)
  })

  it('returns false for undefined', () => {
    expect(isValidTraceItem(undefined)).toBe(false)
  })

  it('returns false for invalid object', () => {
    expect(isValidTraceItem({ invalid: 'object' })).toBe(false)
  })

  it('returns false for array', () => {
    expect(isValidTraceItem([])).toBe(false)
  })

  it('acts as type guard', () => {
    const item: unknown = createValidTraceItem()

    if (isValidTraceItem(item)) {
      // TypeScript should now know item is ValidatedTraceItem
      expect(item.scriptName).toBe('test-worker')
      expect(item.outcome).toBe('ok')
    } else {
      throw new Error('Should be valid')
    }
  })
})

// =============================================================================
// createTailValidationError Tests
// =============================================================================

describe('createTailValidationError', () => {
  it('creates ValidationError with tail operation context', () => {
    const error = createTailValidationError('Test error message', { field: 'testField' })

    expect(error).toBeInstanceOf(ValidationError)
    expect(error.message).toBe('Test error message')
    expect(error.context.operation).toBe('tail')
    expect(error.context.field).toBe('testField')
  })

  it('creates ValidationError without extra context', () => {
    const error = createTailValidationError('Simple error')

    expect(error).toBeInstanceOf(ValidationError)
    expect(error.message).toBe('Simple error')
    expect(error.context.operation).toBe('tail')
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  it('handles log message with any type', () => {
    const item = createValidTraceItem({
      logs: [
        { timestamp: 1000, level: 'info', message: 'string message' },
        { timestamp: 2000, level: 'info', message: { nested: 'object' } },
        { timestamp: 3000, level: 'info', message: [1, 2, 3] },
        { timestamp: 4000, level: 'info', message: null },
        { timestamp: 5000, level: 'info', message: undefined },
      ],
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.logs).toHaveLength(5)
    expect(result.item!.logs[0].message).toBe('string message')
    expect(result.item!.logs[1].message).toEqual({ nested: 'object' })
    expect(result.item!.logs[2].message).toEqual([1, 2, 3])
    expect(result.item!.logs[3].message).toBeNull()
    expect(result.item!.logs[4].message).toBeUndefined()
  })

  it('handles diagnosticsChannelEvents of any type', () => {
    const item = createValidTraceItem({
      diagnosticsChannelEvents: [
        { type: 'custom', data: 'test' },
        'string event',
        123,
      ],
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.diagnosticsChannelEvents).toHaveLength(3)
  })

  it('handles missing diagnosticsChannelEvents gracefully', () => {
    const item = createValidTraceItem()
    delete item.diagnosticsChannelEvents

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.diagnosticsChannelEvents).toEqual([])
  })

  it('handles very large number of items', () => {
    const items = Array.from({ length: 10000 }, () => createValidTraceItem())

    const result = validateTraceItems(items, { maxItems: 10000 })

    expect(result.validCount).toBe(10000)
    expect(result.invalidCount).toBe(0)
  })

  it('handles deeply nested event data', () => {
    const item = createValidTraceItem({
      event: {
        request: {
          url: 'https://example.com',
          method: 'POST',
          headers: {},
          cf: {
            colo: 'DFW',
            country: 'US',
            city: 'Dallas',
            nested: {
              deeply: {
                value: 'test',
              },
            },
          },
        },
      },
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.event!.request!.cf).toHaveProperty('nested')
  })

  it('handles special characters in strings', () => {
    const item = createValidTraceItem({
      scriptName: 'worker-with-special-chars-!@#$%',
      logs: [
        { timestamp: 1000, level: 'info', message: 'Message with unicode: \u{1F600}' },
      ],
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.scriptName).toBe('worker-with-special-chars-!@#$%')
  })

  it('handles empty string values', () => {
    const item = createValidTraceItem({
      scriptName: '',
      outcome: '',
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.scriptName).toBe('')
    expect(result.item!.outcome).toBe('')
  })

  it('handles numeric timestamp edge cases', () => {
    const item = createValidTraceItem({
      eventTimestamp: 0, // Valid (epoch)
      logs: [
        { timestamp: 0, level: 'info', message: 'At epoch' },
        { timestamp: Number.MAX_SAFE_INTEGER, level: 'info', message: 'Max safe integer' },
      ],
    })

    const result = validateTraceItem(item)

    expect(result.valid).toBe(true)
    expect(result.item!.eventTimestamp).toBe(0)
    expect(result.item!.logs).toHaveLength(2)
  })
})

// =============================================================================
// Integration with Tail Handler Tests
// =============================================================================

describe('Integration with createTailHandler', () => {
  it('validates input before processing in tail handler pattern', async () => {
    const validationCallback = vi.fn()

    // Simulate what createTailHandler does internally
    const mockHandler = async (events: unknown) => {
      const result = validateTraceItems(events)

      if (result.invalidCount > 0) {
        validationCallback(result)
      }

      // Process only valid items
      for (const item of result.validItems) {
        // Simulated processing
        expect(item.outcome).toBeDefined()
      }
    }

    // Mix of valid and invalid items
    await mockHandler([
      createValidTraceItem(),
      'invalid',
      createValidTraceItem(),
    ])

    expect(validationCallback).toHaveBeenCalledTimes(1)
    expect(validationCallback).toHaveBeenCalledWith(
      expect.objectContaining({
        validCount: 2,
        invalidCount: 1,
      })
    )
  })

  it('handles completely invalid input gracefully', async () => {
    const mockHandler = async (events: unknown) => {
      const result = validateTraceItems(events)
      return result
    }

    // Not an array
    const result = await mockHandler('not an array')

    expect(result.validItems).toHaveLength(0)
    expect(result.invalidCount).toBe(1)
  })
})

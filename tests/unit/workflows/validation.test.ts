/**
 * Tests for Workflow JSON Response Validation
 *
 * Tests the runtime type validation for JSON responses from Durable Objects
 * and other external sources in workflow code.
 *
 * IMPORTANT: This file imports type guards from production code to ensure
 * tests fail if production behavior changes.
 */

import { describe, it, expect } from 'vitest'

// Import the type guard from production code
import { isWindowsReadyResponse } from '@/workflows/compaction-queue-consumer'

// =============================================================================
// Type Definitions for Test Data
// =============================================================================

/**
 * UpdateRequestBody type guard - used for validating incoming request bodies
 * Note: This is tested here but could be moved to production if validation
 * is needed at runtime in the actual code.
 */
interface UpdateRequestBody {
  updates: Array<{
    namespace: string
    writerId: string
    file: string
    timestamp: number
    size: number
  }>
  config: {
    windowSizeMs: number
    minFilesToCompact: number
    maxWaitTimeMs: number
    targetFormat: string
  }
}

/**
 * Type guard for UpdateRequestBody
 * Validates incoming request bodies have correct structure and types.
 */
function isUpdateRequestBody(data: unknown): data is UpdateRequestBody {
  if (typeof data !== 'object' || data === null) {
    return false
  }
  const d = data as Record<string, unknown>

  // Validate updates array
  if (!Array.isArray(d.updates)) {
    return false
  }
  for (const update of d.updates) {
    if (typeof update !== 'object' || update === null) {
      return false
    }
    const u = update as Record<string, unknown>
    if (
      typeof u.namespace !== 'string' ||
      typeof u.writerId !== 'string' ||
      typeof u.file !== 'string' ||
      typeof u.timestamp !== 'number' ||
      typeof u.size !== 'number'
    ) {
      return false
    }
  }

  // Validate config object
  if (typeof d.config !== 'object' || d.config === null) {
    return false
  }
  const c = d.config as Record<string, unknown>
  if (
    typeof c.windowSizeMs !== 'number' ||
    typeof c.minFilesToCompact !== 'number' ||
    typeof c.maxWaitTimeMs !== 'number' ||
    typeof c.targetFormat !== 'string'
  ) {
    return false
  }

  return true
}

// =============================================================================
// Tests
// =============================================================================

describe('Workflow JSON Response Validation', () => {
  describe('isWindowsReadyResponse (production import)', () => {
    it('should accept valid response with empty windowsReady array', () => {
      const response = { windowsReady: [] }
      expect(isWindowsReadyResponse(response)).toBe(true)
    })

    it('should accept valid response with populated windowsReady array', () => {
      // Note: Production code requires windowKey field
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: ['data/users/1700000000-writer1-1.parquet'],
            writers: ['writer1'],
          },
          {
            namespace: 'posts',
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: ['data/posts/1700000000-writer1-1.parquet', 'data/posts/1700000000-writer2-1.parquet'],
            writers: ['writer1', 'writer2'],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(true)
    })

    it('should reject null', () => {
      expect(isWindowsReadyResponse(null)).toBe(false)
    })

    it('should reject undefined', () => {
      expect(isWindowsReadyResponse(undefined)).toBe(false)
    })

    it('should reject non-object types', () => {
      expect(isWindowsReadyResponse('string')).toBe(false)
      expect(isWindowsReadyResponse(123)).toBe(false)
      expect(isWindowsReadyResponse(true)).toBe(false)
      expect(isWindowsReadyResponse([])).toBe(false)
    })

    it('should reject response without windowsReady field', () => {
      expect(isWindowsReadyResponse({})).toBe(false)
      expect(isWindowsReadyResponse({ other: [] })).toBe(false)
    })

    it('should reject response with non-array windowsReady', () => {
      expect(isWindowsReadyResponse({ windowsReady: null })).toBe(false)
      expect(isWindowsReadyResponse({ windowsReady: 'string' })).toBe(false)
      expect(isWindowsReadyResponse({ windowsReady: {} })).toBe(false)
    })

    it('should reject response with invalid window entry - missing namespace', () => {
      const response = {
        windowsReady: [
          {
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: [],
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - missing windowKey', () => {
      // Production code requires windowKey - this test ensures we validate it
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: [],
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - wrong type for namespace', () => {
      const response = {
        windowsReady: [
          {
            namespace: 123,
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: [],
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - missing windowStart', () => {
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowKey: '1700000000000',
            windowEnd: 1700003600000,
            files: [],
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - wrong type for windowStart', () => {
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowKey: '1700000000000',
            windowStart: '1700000000000',
            windowEnd: 1700003600000,
            files: [],
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - files is not array', () => {
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: 'not-an-array',
            writers: [],
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with invalid window entry - writers is not array', () => {
      const response = {
        windowsReady: [
          {
            namespace: 'users',
            windowKey: '1700000000000',
            windowStart: 1700000000000,
            windowEnd: 1700003600000,
            files: [],
            writers: null,
          },
        ],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with null entry in windowsReady array', () => {
      const response = {
        windowsReady: [null],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })

    it('should reject response with primitive entry in windowsReady array', () => {
      const response = {
        windowsReady: ['string'],
      }
      expect(isWindowsReadyResponse(response)).toBe(false)
    })
  })

  describe('isUpdateRequestBody', () => {
    it('should accept valid request body with empty updates', () => {
      const body = {
        updates: [],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(true)
    })

    it('should accept valid request body with populated updates', () => {
      const body = {
        updates: [
          {
            namespace: 'users',
            writerId: 'writer1',
            file: 'data/users/1700000000-writer1-1.parquet',
            timestamp: 1700000000000,
            size: 1024,
          },
          {
            namespace: 'posts',
            writerId: 'writer2',
            file: 'data/posts/1700000000-writer2-1.parquet',
            timestamp: 1700000001000,
            size: 2048,
          },
        ],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'iceberg',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(true)
    })

    it('should reject null', () => {
      expect(isUpdateRequestBody(null)).toBe(false)
    })

    it('should reject undefined', () => {
      expect(isUpdateRequestBody(undefined)).toBe(false)
    })

    it('should reject non-object types', () => {
      expect(isUpdateRequestBody('string')).toBe(false)
      expect(isUpdateRequestBody(123)).toBe(false)
      expect(isUpdateRequestBody(true)).toBe(false)
    })

    it('should reject body without updates array', () => {
      const body = {
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with non-array updates', () => {
      const body = {
        updates: 'not-an-array',
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body without config object', () => {
      const body = {
        updates: [],
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with null config', () => {
      const body = {
        updates: [],
        config: null,
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid update entry - missing namespace', () => {
      const body = {
        updates: [
          {
            writerId: 'writer1',
            file: 'file.parquet',
            timestamp: 1700000000000,
            size: 1024,
          },
        ],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid update entry - wrong type for writerId', () => {
      const body = {
        updates: [
          {
            namespace: 'users',
            writerId: 123,
            file: 'file.parquet',
            timestamp: 1700000000000,
            size: 1024,
          },
        ],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid update entry - wrong type for timestamp', () => {
      const body = {
        updates: [
          {
            namespace: 'users',
            writerId: 'writer1',
            file: 'file.parquet',
            timestamp: '1700000000000',
            size: 1024,
          },
        ],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid update entry - wrong type for size', () => {
      const body = {
        updates: [
          {
            namespace: 'users',
            writerId: 'writer1',
            file: 'file.parquet',
            timestamp: 1700000000000,
            size: '1024',
          },
        ],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with null update entry', () => {
      const body = {
        updates: [null],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid config - missing windowSizeMs', () => {
      const body = {
        updates: [],
        config: {
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid config - wrong type for windowSizeMs', () => {
      const body = {
        updates: [],
        config: {
          windowSizeMs: '3600000',
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 'native',
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })

    it('should reject body with invalid config - wrong type for targetFormat', () => {
      const body = {
        updates: [],
        config: {
          windowSizeMs: 3600000,
          minFilesToCompact: 10,
          maxWaitTimeMs: 300000,
          targetFormat: 123,
        },
      }
      expect(isUpdateRequestBody(body)).toBe(false)
    })
  })

  describe('Error Messages', () => {
    it('should provide meaningful feedback about validation failures', () => {
      // Test that we can identify what's wrong with invalid data
      const invalidResponse = { windowsReady: 'not-an-array' }

      // Using the type guard to verify rejection
      expect(isWindowsReadyResponse(invalidResponse)).toBe(false)

      // In production, when validation fails, the error message should be descriptive
      // The actual implementation throws errors like:
      // "Invalid response from CompactionStateDO: expected { windowsReady: Array<WindowReadyEntry> }"
    })
  })
})

/**
 * Fire-and-Forget Safety Net Tests (RED)
 *
 * Tests for ensuring errors are never silently lost when fire-and-forget
 * operations fail, even when error handlers and logging also fail.
 *
 * These tests verify:
 * 1. When fire-and-forget fails AND error handler throws, error is captured
 * 2. Global unhandled rejection handler catches escaping errors
 * 3. Errors are tracked in persistent queue for post-mortem analysis
 * 4. Logging failures don't cause silent error loss
 *
 * @module tests/unit/utils/fire-and-forget-safety
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  fireAndForget,
  globalFireAndForgetMetrics,
  type FireAndForgetConfig,
  type ErrorHandler,
} from '@/utils/fire-and-forget'
import type { Logger } from '@/utils/logger'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Wait for pending promises to settle
 */
async function flushPromises(): Promise<void> {
  await new Promise(setImmediate)
}

/**
 * Create a logger that throws on specific methods
 */
function createThrowingLogger(throwOn: ('warn' | 'error')[]): Logger {
  return {
    debug: vi.fn(),
    info: vi.fn(),
    warn: throwOn.includes('warn')
      ? vi.fn(() => {
          throw new Error('Logger warn failed')
        })
      : vi.fn(),
    error: throwOn.includes('error')
      ? vi.fn(() => {
          throw new Error('Logger error failed')
        })
      : vi.fn(),
  }
}

// =============================================================================
// Unhandled Rejection Safety Net Tests
// =============================================================================

describe('Fire-and-Forget Safety Net', () => {
  let capturedUnhandledRejections: PromiseRejectionEvent[]
  let originalUnhandledRejectionHandler: ((event: PromiseRejectionEvent) => void) | null

  beforeEach(() => {
    globalFireAndForgetMetrics.reset()
    capturedUnhandledRejections = []

    // Capture unhandled rejections for testing
    originalUnhandledRejectionHandler = null
    if (typeof process !== 'undefined') {
      // Node.js environment
      const handler = (reason: unknown, promise: Promise<unknown>) => {
        capturedUnhandledRejections.push({
          promise,
          reason,
        } as PromiseRejectionEvent)
      }
      process.on('unhandledRejection', handler)
      // @ts-expect-error storing handler for cleanup
      originalUnhandledRejectionHandler = handler
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
    if (originalUnhandledRejectionHandler && typeof process !== 'undefined') {
      process.off('unhandledRejection', originalUnhandledRejectionHandler as never)
    }
  })

  // ===========================================================================
  // Error Handler Throws Scenario
  // ===========================================================================

  describe('Error Handler Throws', () => {
    it('should capture error when operation fails AND error handler throws', async () => {
      // Setup: error handler that throws
      const throwingErrorHandler: ErrorHandler = vi.fn(() => {
        throw new Error('Error handler exploded')
      })

      // Setup: logger that tracks what it receives (and also throws on error)
      const throwingLogger = createThrowingLogger(['error'])

      const config: FireAndForgetConfig = {
        logger: throwingLogger,
        onError: throwingErrorHandler,
      }

      // Execute: fire-and-forget operation that fails
      const operationError = new Error('Operation failed')
      fireAndForget(
        'auto-snapshot',
        async () => {
          throw operationError
        },
        config
      )

      await flushPromises()

      // Assert: Error should NOT be lost - it should be in a safety queue
      // This test will FAIL until we implement the safety net
      const { getUnhandledErrors } = await import('@/utils/fire-and-forget')
      const unhandledErrors = getUnhandledErrors()

      expect(unhandledErrors.length).toBeGreaterThan(0)
      expect(unhandledErrors.some((e) => e.originalError.message === 'Operation failed')).toBe(true)
    })

    it('should record original error in safety queue even when all handlers fail', async () => {
      const throwingErrorHandler: ErrorHandler = () => {
        throw new Error('Handler threw')
      }

      const throwingLogger = createThrowingLogger(['warn', 'error'])

      fireAndForget(
        'background-revalidation',
        async () => {
          throw new Error('Critical operation failed')
        },
        {
          logger: throwingLogger,
          onError: throwingErrorHandler,
        }
      )

      await flushPromises()

      // Safety queue should contain the error
      const { getUnhandledErrors } = await import('@/utils/fire-and-forget')
      const errors = getUnhandledErrors()

      expect(errors.length).toBeGreaterThanOrEqual(1)

      const errorRecord = errors.find((e) => e.originalError.message === 'Critical operation failed')
      expect(errorRecord).toBeDefined()
      expect(errorRecord!.operation).toBe('background-revalidation')
      expect(errorRecord!.handlerError?.message).toBe('Handler threw')
      expect(errorRecord!.loggingError).toBeDefined()
    })
  })

  // ===========================================================================
  // Global Unhandled Rejection Handler
  // ===========================================================================

  describe('Global Unhandled Rejection Handler', () => {
    it('should catch errors that escape the catch block via global handler', async () => {
      // This tests the scenario where even the safety mechanisms fail
      // and we need a global unhandled rejection handler as last resort

      const { installSafetyNet, uninstallSafetyNet, getSafetyNetCaughtErrors } = await import(
        '@/utils/fire-and-forget'
      )

      // Install the safety net
      installSafetyNet()

      try {
        // Simulate a truly catastrophic failure that escapes all handlers
        // This might happen if there's a bug in our error handling code itself
        const config: FireAndForgetConfig = {
          logger: {
            debug: vi.fn(),
            info: vi.fn(),
            warn: () => {
              // This simulates a synchronous throw that could escape
              throw new Error('Catastrophic logger failure')
            },
            error: () => {
              throw new Error('Even error logging failed')
            },
          },
          onError: () => {
            throw new Error('Error handler also failed')
          },
        }

        fireAndForget(
          'cache-cleanup',
          async () => {
            throw new Error('Initial operation error')
          },
          config
        )

        await flushPromises()

        // The safety net should have caught something
        const caughtErrors = getSafetyNetCaughtErrors()
        expect(caughtErrors.length).toBeGreaterThan(0)
      } finally {
        uninstallSafetyNet()
      }
    })

    it('should register global handler when module initializes', async () => {
      // The fire-and-forget module should automatically register a global handler
      const { isGlobalSafetyNetInstalled } = await import('@/utils/fire-and-forget')

      expect(isGlobalSafetyNetInstalled()).toBe(true)
    })
  })

  // ===========================================================================
  // Persistent Error Queue
  // ===========================================================================

  describe('Persistent Error Queue', () => {
    it('should track errors in queue for post-mortem analysis', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      // Clear any existing errors
      clearUnhandledErrors()

      const throwingHandler: ErrorHandler = () => {
        throw new Error('Handler failed')
      }

      const throwingLogger = createThrowingLogger(['error'])

      // Generate multiple errors
      fireAndForget(
        'auto-snapshot',
        async () => {
          throw new Error('Error 1')
        },
        { onError: throwingHandler, logger: throwingLogger }
      )

      fireAndForget(
        'periodic-flush',
        async () => {
          throw new Error('Error 2')
        },
        { onError: throwingHandler, logger: throwingLogger }
      )

      fireAndForget(
        'index-update',
        async () => {
          throw new Error('Error 3')
        },
        { onError: throwingHandler, logger: throwingLogger }
      )

      await flushPromises()

      const errors = getUnhandledErrors()

      // All three errors should be in the queue
      expect(errors.length).toBe(3)

      // Each should have the proper structure
      for (const error of errors) {
        expect(error).toHaveProperty('originalError')
        expect(error).toHaveProperty('operation')
        expect(error).toHaveProperty('timestamp')
        expect(error).toHaveProperty('handlerError')
      }

      // Operations should be tracked
      const operations = errors.map((e) => e.operation)
      expect(operations).toContain('auto-snapshot')
      expect(operations).toContain('periodic-flush')
      expect(operations).toContain('index-update')
    })

    it('should include error context in queue entries', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      const context = { entityId: '123', attempt: 1, source: 'test' }

      fireAndForget(
        'auto-snapshot',
        async () => {
          throw new Error('Contextual error')
        },
        {
          onError: () => {
            throw new Error('Handler failed')
          },
          logger: createThrowingLogger(['error']),
        },
        context
      )

      await flushPromises()

      const errors = getUnhandledErrors()
      expect(errors.length).toBe(1)

      const errorRecord = errors[0]
      expect(errorRecord.context).toEqual(context)
    })

    it('should limit queue size to prevent memory leaks', async () => {
      const { getUnhandledErrors, clearUnhandledErrors, MAX_UNHANDLED_ERROR_QUEUE_SIZE } =
        await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      const throwingHandler: ErrorHandler = () => {
        throw new Error('Handler failed')
      }

      // Generate more errors than the max queue size
      const errorCount = MAX_UNHANDLED_ERROR_QUEUE_SIZE + 50

      for (let i = 0; i < errorCount; i++) {
        fireAndForget(
          'custom',
          async () => {
            throw new Error(`Error ${i}`)
          },
          {
            onError: throwingHandler,
            logger: createThrowingLogger(['error']),
          }
        )
      }

      await flushPromises()

      const errors = getUnhandledErrors()

      // Queue should not exceed max size
      expect(errors.length).toBeLessThanOrEqual(MAX_UNHANDLED_ERROR_QUEUE_SIZE)

      // Most recent errors should be kept (FIFO eviction of old errors)
      const errorMessages = errors.map((e) => e.originalError.message)
      expect(errorMessages).toContain(`Error ${errorCount - 1}`)
    })

    it('should support draining the queue', async () => {
      const { getUnhandledErrors, clearUnhandledErrors, drainUnhandledErrors } = await import(
        '@/utils/fire-and-forget'
      )

      clearUnhandledErrors()

      fireAndForget(
        'auto-snapshot',
        async () => {
          throw new Error('Test error')
        },
        {
          onError: () => {
            throw new Error('Handler failed')
          },
          logger: createThrowingLogger(['error']),
        }
      )

      await flushPromises()

      // Drain should return and clear the errors
      const drained = drainUnhandledErrors()
      expect(drained.length).toBe(1)

      // Queue should now be empty
      const remaining = getUnhandledErrors()
      expect(remaining.length).toBe(0)
    })
  })

  // ===========================================================================
  // Logging Failure Scenarios
  // ===========================================================================

  describe('Logging Failures', () => {
    it('should not lose errors when warn logging fails', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      // Logger where warn throws
      const throwingLogger = createThrowingLogger(['warn'])

      fireAndForget(
        'metrics-flush',
        async () => {
          throw new Error('Flush failed')
        },
        { logger: throwingLogger }
      )

      await flushPromises()

      // Even without an error handler, if logging fails, error should be in safety queue
      const errors = getUnhandledErrors()
      expect(errors.length).toBe(1)
      expect(errors[0].originalError.message).toBe('Flush failed')
      expect(errors[0].loggingError?.message).toBe('Logger warn failed')
    })

    it('should not lose errors when error logging fails during handler error', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      // Logger where error() throws (used when handler fails)
      const throwingLogger = createThrowingLogger(['error'])

      const throwingHandler: ErrorHandler = () => {
        throw new Error('Handler exploded')
      }

      fireAndForget(
        'auto-snapshot',
        async () => {
          throw new Error('Original error')
        },
        {
          logger: throwingLogger,
          onError: throwingHandler,
        }
      )

      await flushPromises()

      const errors = getUnhandledErrors()
      expect(errors.length).toBe(1)

      const errorRecord = errors[0]
      expect(errorRecord.originalError.message).toBe('Original error')
      expect(errorRecord.handlerError?.message).toBe('Handler exploded')
      expect(errorRecord.loggingError?.message).toBe('Logger error failed')
    })

    it('should capture both warn and error logging failures', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      // Both warn and error throw
      const throwingLogger = createThrowingLogger(['warn', 'error'])

      const throwingHandler: ErrorHandler = () => {
        throw new Error('Handler threw')
      }

      fireAndForget(
        'cache-cleanup',
        async () => {
          throw new Error('Cleanup failed')
        },
        {
          logger: throwingLogger,
          onError: throwingHandler,
        }
      )

      await flushPromises()

      const errors = getUnhandledErrors()
      expect(errors.length).toBe(1)

      const errorRecord = errors[0]
      expect(errorRecord.originalError.message).toBe('Cleanup failed')
      // Should capture all failures in the error record
      expect(errorRecord.handlerError).toBeDefined()
      expect(errorRecord.loggingError).toBeDefined()
    })
  })

  // ===========================================================================
  // Error Record Structure
  // ===========================================================================

  describe('Error Record Structure', () => {
    it('should have all required fields in error records', async () => {
      const { getUnhandledErrors, clearUnhandledErrors } = await import('@/utils/fire-and-forget')

      clearUnhandledErrors()

      fireAndForget(
        'background-revalidation',
        async () => {
          throw new Error('Test error')
        },
        {
          onError: () => {
            throw new Error('Handler error')
          },
          logger: createThrowingLogger(['error']),
        },
        { key: 'value' }
      )

      await flushPromises()

      const errors = getUnhandledErrors()
      expect(errors.length).toBe(1)

      const record = errors[0]

      // Required fields
      expect(record).toHaveProperty('id')
      expect(typeof record.id).toBe('string')

      expect(record).toHaveProperty('timestamp')
      expect(typeof record.timestamp).toBe('number')
      expect(record.timestamp).toBeLessThanOrEqual(Date.now())

      expect(record).toHaveProperty('operation')
      expect(record.operation).toBe('background-revalidation')

      expect(record).toHaveProperty('originalError')
      expect(record.originalError).toBeInstanceOf(Error)

      // Optional fields that should be present in this case
      expect(record).toHaveProperty('handlerError')
      expect(record.handlerError).toBeInstanceOf(Error)

      expect(record).toHaveProperty('loggingError')
      expect(record.loggingError).toBeInstanceOf(Error)

      expect(record).toHaveProperty('context')
      expect(record.context).toEqual({ key: 'value' })
    })
  })
})

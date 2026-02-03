/**
 * CircuitBreaker Tests
 *
 * Tests for the circuit breaker pattern implementation.
 * Verifies state transitions, failure counting, and recovery behavior.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  CircuitBreaker,
  CircuitState,
  CircuitOpenError,
  createStorageCircuitBreaker,
  createFastFailCircuitBreaker,
  createConservativeCircuitBreaker,
} from '../../../src/storage/CircuitBreaker'

// =============================================================================
// Helpers
// =============================================================================

function createFailingOperation(error: Error = new Error('Operation failed')): () => Promise<never> {
  return async () => {
    throw error
  }
}

function createSuccessOperation<T>(result: T): () => Promise<T> {
  return async () => result
}

/**
 * Advance fake timers by specified milliseconds and flush pending promises
 */
async function advanceTime(ms: number): Promise<void> {
  vi.advanceTimersByTime(ms)
  // Flush any pending promise resolutions
  await vi.runOnlyPendingTimersAsync()
}

// =============================================================================
// Test Suite
// =============================================================================

describe('CircuitBreaker', () => {
  let breaker: CircuitBreaker

  beforeEach(() => {
    vi.useFakeTimers()
    breaker = new CircuitBreaker({
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 100,
      failureWindowMs: 1000,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ===========================================================================
  // Initial State
  // ===========================================================================

  describe('initial state', () => {
    it('should start in CLOSED state', () => {
      expect(breaker.getState()).toBe(CircuitState.CLOSED)
    })

    it('should start with zero metrics', () => {
      const metrics = breaker.getMetrics()
      expect(metrics.failureCount).toBe(0)
      expect(metrics.successCount).toBe(0)
      expect(metrics.totalRequests).toBe(0)
      expect(metrics.totalFailures).toBe(0)
      expect(metrics.totalSuccesses).toBe(0)
    })

    it('should allow requests initially', () => {
      expect(breaker.isAllowingRequests()).toBe(true)
    })
  })

  // ===========================================================================
  // CLOSED State Behavior
  // ===========================================================================

  describe('CLOSED state', () => {
    it('should execute operations successfully', async () => {
      const result = await breaker.execute(createSuccessOperation('success'))
      expect(result).toBe('success')
    })

    it('should track successful operations', async () => {
      await breaker.execute(createSuccessOperation('a'))
      await breaker.execute(createSuccessOperation('b'))

      const metrics = breaker.getMetrics()
      expect(metrics.totalRequests).toBe(2)
      expect(metrics.totalSuccesses).toBe(2)
      expect(metrics.totalFailures).toBe(0)
    })

    it('should track failed operations', async () => {
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      const metrics = breaker.getMetrics()
      expect(metrics.totalRequests).toBe(1)
      expect(metrics.totalFailures).toBe(1)
      expect(metrics.failureCount).toBe(1)
    })

    it('should propagate errors from operations', async () => {
      const error = new Error('Custom error')
      await expect(breaker.execute(createFailingOperation(error))).rejects.toThrow('Custom error')
    })

    it('should reset failure count on success', async () => {
      // Fail twice (below threshold)
      for (let i = 0; i < 2; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }
      expect(breaker.getMetrics().failureCount).toBe(2)

      // Succeed once
      await breaker.execute(createSuccessOperation('ok'))

      // Failure count should be reset
      expect(breaker.getMetrics().failureCount).toBe(0)
    })
  })

  // ===========================================================================
  // State Transition: CLOSED -> OPEN
  // ===========================================================================

  describe('CLOSED -> OPEN transition', () => {
    it('should open circuit after failure threshold is reached', async () => {
      // Trigger 3 failures (threshold)
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should call onStateChange callback when opening', async () => {
      const onStateChange = vi.fn()
      const trackedBreaker = new CircuitBreaker({
        failureThreshold: 2,
        resetTimeoutMs: 100,
        onStateChange,
        name: 'test-breaker',
      })

      for (let i = 0; i < 2; i++) {
        try {
          await trackedBreaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      expect(onStateChange).toHaveBeenCalledWith(
        CircuitState.CLOSED,
        CircuitState.OPEN,
        'test-breaker'
      )
    })

    it('should track lastFailureTime when opening', async () => {
      const before = Date.now()
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }
      const after = Date.now()

      const metrics = breaker.getMetrics()
      expect(metrics.lastFailureTime).toBeGreaterThanOrEqual(before)
      expect(metrics.lastFailureTime).toBeLessThanOrEqual(after)
    })
  })

  // ===========================================================================
  // OPEN State Behavior
  // ===========================================================================

  describe('OPEN state', () => {
    beforeEach(async () => {
      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should not allow requests', () => {
      expect(breaker.isAllowingRequests()).toBe(false)
    })

    it('should throw CircuitOpenError immediately', async () => {
      await expect(breaker.execute(createSuccessOperation('ok'))).rejects.toThrow(CircuitOpenError)
    })

    it('should include circuit name in error', async () => {
      const namedBreaker = new CircuitBreaker({
        failureThreshold: 1,
        resetTimeoutMs: 100,
        name: 'my-service',
      })

      try {
        await namedBreaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      try {
        await namedBreaker.execute(createSuccessOperation('ok'))
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CircuitOpenError)
        expect((error as CircuitOpenError).circuitName).toBe('my-service')
      }
    })

    it('should include remaining timeout in error', async () => {
      try {
        await breaker.execute(createSuccessOperation('ok'))
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CircuitOpenError)
        expect((error as CircuitOpenError).remainingMs).toBeGreaterThan(0)
        expect((error as CircuitOpenError).remainingMs).toBeLessThanOrEqual(100)
      }
    })

    it('should still count rejected requests in metrics', async () => {
      const before = breaker.getMetrics().totalRequests

      try {
        await breaker.execute(createSuccessOperation('ok'))
      } catch {
        // Expected
      }

      expect(breaker.getMetrics().totalRequests).toBe(before + 1)
    })
  })

  // ===========================================================================
  // State Transition: OPEN -> HALF_OPEN
  // ===========================================================================

  describe('OPEN -> HALF_OPEN transition', () => {
    beforeEach(async () => {
      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }
    })

    it('should transition to HALF_OPEN after timeout', async () => {
      expect(breaker.getState()).toBe(CircuitState.OPEN)

      // Wait for reset timeout using fake timers
      await advanceTime(150)

      // getState() should trigger the transition
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN)
    })

    it('should allow requests after timeout', async () => {
      await advanceTime(150)
      expect(breaker.isAllowingRequests()).toBe(true)
    })

    it('should call onStateChange when transitioning to HALF_OPEN', async () => {
      const onStateChange = vi.fn()
      const trackedBreaker = new CircuitBreaker({
        failureThreshold: 1,
        resetTimeoutMs: 50,
        onStateChange,
      })

      try {
        await trackedBreaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      // Wait for timeout using fake timers
      await advanceTime(60)
      trackedBreaker.getState()

      expect(onStateChange).toHaveBeenCalledWith(CircuitState.OPEN, CircuitState.HALF_OPEN, undefined)
    })
  })

  // ===========================================================================
  // HALF_OPEN State Behavior
  // ===========================================================================

  describe('HALF_OPEN state', () => {
    beforeEach(async () => {
      // Open the circuit, then wait for HALF_OPEN
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }
      await advanceTime(150)
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN)
    })

    it('should allow test requests', () => {
      expect(breaker.isAllowingRequests()).toBe(true)
    })

    it('should execute operations', async () => {
      const result = await breaker.execute(createSuccessOperation('recovered'))
      expect(result).toBe('recovered')
    })

    it('should close circuit after enough successes', async () => {
      // Need 2 successes (successThreshold)
      await breaker.execute(createSuccessOperation('a'))
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN)

      await breaker.execute(createSuccessOperation('b'))
      expect(breaker.getState()).toBe(CircuitState.CLOSED)
    })

    it('should open circuit immediately on failure', async () => {
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })
  })

  // ===========================================================================
  // Failure Window
  // ===========================================================================

  describe('failure window', () => {
    it('should only count failures within the window', async () => {
      const windowBreaker = new CircuitBreaker({
        failureThreshold: 3,
        resetTimeoutMs: 100,
        failureWindowMs: 50, // 50ms window
      })

      // First failure
      try {
        await windowBreaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      // Wait for failure to age out using fake timers
      await advanceTime(60)

      // Two more failures
      for (let i = 0; i < 2; i++) {
        try {
          await windowBreaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      // Should still be closed (only 2 failures in window)
      expect(windowBreaker.getState()).toBe(CircuitState.CLOSED)
      expect(windowBreaker.getMetrics().failureCount).toBe(2)
    })
  })

  // ===========================================================================
  // isFailure Filter
  // ===========================================================================

  describe('isFailure filter', () => {
    it('should not count NotFoundError as failure by default', async () => {
      const notFoundError = new Error('File not found')
      notFoundError.name = 'NotFoundError'

      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(createFailingOperation(notFoundError))
        } catch {
          // Expected
        }
      }

      // Should still be closed
      expect(breaker.getState()).toBe(CircuitState.CLOSED)
      expect(breaker.getMetrics().failureCount).toBe(0)
    })

    it('should use custom isFailure function', async () => {
      const customBreaker = new CircuitBreaker({
        failureThreshold: 2,
        resetTimeoutMs: 100,
        isFailure: (error) => error.message.includes('critical'),
      })

      // Non-critical errors should not count
      for (let i = 0; i < 3; i++) {
        try {
          await customBreaker.execute(createFailingOperation(new Error('minor issue')))
        } catch {
          // Expected
        }
      }
      expect(customBreaker.getState()).toBe(CircuitState.CLOSED)

      // Critical errors should count
      for (let i = 0; i < 2; i++) {
        try {
          await customBreaker.execute(createFailingOperation(new Error('critical failure')))
        } catch {
          // Expected
        }
      }
      expect(customBreaker.getState()).toBe(CircuitState.OPEN)
    })
  })

  // ===========================================================================
  // Manual Controls
  // ===========================================================================

  describe('manual controls', () => {
    describe('reset()', () => {
      it('should reset to CLOSED state', async () => {
        // Open the circuit
        for (let i = 0; i < 3; i++) {
          try {
            await breaker.execute(createFailingOperation())
          } catch {
            // Expected
          }
        }
        expect(breaker.getState()).toBe(CircuitState.OPEN)

        breaker.reset()

        expect(breaker.getState()).toBe(CircuitState.CLOSED)
        expect(breaker.getMetrics().failureCount).toBe(0)
        expect(breaker.isAllowingRequests()).toBe(true)
      })
    })

    describe('trip()', () => {
      it('should manually open the circuit', () => {
        expect(breaker.getState()).toBe(CircuitState.CLOSED)

        breaker.trip()

        expect(breaker.getState()).toBe(CircuitState.OPEN)
        expect(breaker.isAllowingRequests()).toBe(false)
      })
    })
  })

  // ===========================================================================
  // Metrics
  // ===========================================================================

  describe('getMetrics()', () => {
    it('should return complete metrics', async () => {
      // Perform some operations
      await breaker.execute(createSuccessOperation('ok'))
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      const metrics = breaker.getMetrics()

      expect(metrics).toEqual(expect.objectContaining({
        state: CircuitState.CLOSED,
        totalRequests: 2,
        totalSuccesses: 1,
        totalFailures: 1,
        failureCount: 1,
        successCount: 0,
      }))
      expect(metrics.lastStateChangeTime).toBeDefined()
    })
  })

  // ===========================================================================
  // Non-Error Thrown Values
  // ===========================================================================

  describe('non-Error thrown values', () => {
    it('should handle string thrown values', async () => {
      const stringThrower = async () => {
        throw 'string error'
      }

      await expect(breaker.execute(stringThrower)).rejects.toBe('string error')
    })

    it('should handle object thrown values', async () => {
      const objThrower = async () => {
        throw { code: 500 }
      }

      await expect(breaker.execute(objThrower)).rejects.toEqual({ code: 500 })
    })
  })
})

// =============================================================================
// Factory Functions
// =============================================================================

describe('Factory Functions', () => {
  describe('createStorageCircuitBreaker()', () => {
    it('should create breaker with default settings', () => {
      const breaker = createStorageCircuitBreaker('test')
      const metrics = breaker.getMetrics()

      expect(metrics.state).toBe(CircuitState.CLOSED)
    })

    it('should not count NotFoundError as failure', async () => {
      const breaker = createStorageCircuitBreaker('test')
      const notFoundError = new Error('not found')
      notFoundError.name = 'NotFoundError'

      for (let i = 0; i < 10; i++) {
        try {
          await breaker.execute(createFailingOperation(notFoundError))
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.CLOSED)
    })
  })

  describe('createFastFailCircuitBreaker()', () => {
    it('should open faster with lower threshold', async () => {
      const breaker = createFastFailCircuitBreaker('test')

      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })
  })

  describe('createConservativeCircuitBreaker()', () => {
    it('should require more failures to open', async () => {
      const breaker = createConservativeCircuitBreaker('test')

      for (let i = 0; i < 9; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      // Should still be closed (threshold is 10)
      expect(breaker.getState()).toBe(CircuitState.CLOSED)

      // One more failure should open it
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })
  })
})

// =============================================================================
// Timeout and Edge Case Tests
// =============================================================================

/**
 * Helper to create a delayed promise for testing async behavior with fake timers
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe('CircuitBreaker - Timeout Scenarios', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('operation timeouts', () => {
    it('should count timeout errors as failures', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      const timeoutError = new Error('Operation timed out')
      timeoutError.name = 'TimeoutError'

      for (let i = 0; i < 2; i++) {
        try {
          await breaker.execute(createFailingOperation(timeoutError))
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should handle AbortError from AbortController', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      const abortError = new DOMException('The operation was aborted', 'AbortError')

      for (let i = 0; i < 2; i++) {
        try {
          await breaker.execute(async () => {
            throw abortError
          })
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })
  })

  describe('concurrent requests', () => {
    it('should handle multiple concurrent requests in CLOSED state', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 5,
      })

      const results = await Promise.all([
        breaker.execute(createSuccessOperation('a')),
        breaker.execute(createSuccessOperation('b')),
        breaker.execute(createSuccessOperation('c')),
      ])

      expect(results).toEqual(['a', 'b', 'c'])
      expect(breaker.getMetrics().totalRequests).toBe(3)
    })

    it('should handle concurrent failures without race conditions', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 3,
        resetTimeoutMs: 100,
      })

      // Start 3 concurrent failing requests
      const promises = Array.from({ length: 3 }, () =>
        breaker.execute(createFailingOperation()).catch(() => 'failed')
      )

      await Promise.all(promises)

      // Circuit should be open after 3 failures
      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should reject new requests immediately when circuit opens mid-batch', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
        resetTimeoutMs: 100,
      })

      // First two requests will fail and open the circuit
      const slowFail = async () => {
        await vi.advanceTimersByTimeAsync(10)
        throw new Error('Slow failure')
      }

      const results = await Promise.allSettled([
        breaker.execute(slowFail),
        breaker.execute(slowFail),
        // This one might get rejected if circuit opens
        breaker.execute(slowFail),
      ])

      // At least 2 should be rejected, possibly all 3
      const rejected = results.filter(r => r.status === 'rejected')
      expect(rejected.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('edge cases', () => {
    it('should handle rapid state changes', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 1,
        successThreshold: 1,
        resetTimeoutMs: 10,
      })

      // Open
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN)

      // Wait for half-open using fake timers
      await vi.advanceTimersByTimeAsync(15)
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN)

      // Succeed to close
      await breaker.execute(createSuccessOperation('ok'))
      expect(breaker.getState()).toBe(CircuitState.CLOSED)

      // Open again
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should handle very long operations', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 3,
        resetTimeoutMs: 50,
      })

      const longOperation = async () => {
        await vi.advanceTimersByTimeAsync(100)
        return 'completed'
      }

      const result = await breaker.execute(longOperation)
      expect(result).toBe('completed')
      expect(breaker.getState()).toBe(CircuitState.CLOSED)
    })

    it('should maintain metrics across state transitions', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
        successThreshold: 1,
        resetTimeoutMs: 20,
      })

      // Accumulate some successes
      await breaker.execute(createSuccessOperation('a'))
      await breaker.execute(createSuccessOperation('b'))

      // Fail to open circuit
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }
      try {
        await breaker.execute(createFailingOperation())
      } catch {
        // Expected
      }

      // Wait and recover using fake timers
      await vi.advanceTimersByTimeAsync(30)
      await breaker.execute(createSuccessOperation('c'))

      const metrics = breaker.getMetrics()
      expect(metrics.totalRequests).toBe(5)
      expect(metrics.totalSuccesses).toBe(3)
      expect(metrics.totalFailures).toBe(2)
    })

    it('should handle undefined error name gracefully', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
      })

      const errorWithNoName = new Error('test')
      // @ts-expect-error - testing undefined name
      errorWithNoName.name = undefined

      for (let i = 0; i < 2; i++) {
        try {
          await breaker.execute(async () => {
            throw errorWithNoName
          })
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })

    it('should handle null values in error message', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 2,
      })

      for (let i = 0; i < 2; i++) {
        try {
          await breaker.execute(async () => {
            throw null
          })
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN)
    })
  })

  describe('metrics accuracy', () => {
    it('should accurately track metrics under load', async () => {
      const breaker = new CircuitBreaker({
        failureThreshold: 100, // High threshold to prevent opening
      })

      const successCount = 50
      const failCount = 30

      // Run successes
      for (let i = 0; i < successCount; i++) {
        await breaker.execute(createSuccessOperation('ok'))
      }

      // Run failures
      for (let i = 0; i < failCount; i++) {
        try {
          await breaker.execute(createFailingOperation())
        } catch {
          // Expected
        }
      }

      const metrics = breaker.getMetrics()
      expect(metrics.totalRequests).toBe(successCount + failCount)
      expect(metrics.totalSuccesses).toBe(successCount)
      expect(metrics.totalFailures).toBe(failCount)
    })
  })
})

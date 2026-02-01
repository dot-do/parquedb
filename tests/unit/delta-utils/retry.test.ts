/**
 * Retry with Exponential Backoff Test Suite
 *
 * Tests for the retry utility with exponential backoff and jitter.
 * Covers success paths, error handling, and configuration options.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  withRetry,
  isRetryableError,
  AbortError,
  DEFAULT_RETRY_CONFIG,
  type RetryConfig,
  type RetryMetrics,
  type RetryInfo,
} from '../../../src/delta-utils/retry'

// =============================================================================
// ERROR UTILITIES TESTS
// =============================================================================

describe('isRetryableError', () => {
  it('returns false for null', () => {
    expect(isRetryableError(null)).toBe(false)
  })

  it('returns false for undefined', () => {
    expect(isRetryableError(undefined)).toBe(false)
  })

  it('returns false for non-Error objects', () => {
    expect(isRetryableError('error string')).toBe(false)
    expect(isRetryableError(42)).toBe(false)
    expect(isRetryableError({ message: 'error' })).toBe(false)
  })

  it('returns false for generic Error', () => {
    expect(isRetryableError(new Error('generic error'))).toBe(false)
  })

  it('returns false for TypeError', () => {
    expect(isRetryableError(new TypeError('type error'))).toBe(false)
  })

  it('returns false for SyntaxError', () => {
    expect(isRetryableError(new SyntaxError('syntax error'))).toBe(false)
  })

  it('returns true for ConcurrencyError by name', () => {
    const error = new Error('conflict')
    error.name = 'ConcurrencyError'
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns true for VersionMismatchError by name', () => {
    const error = new Error('version mismatch')
    error.name = 'VersionMismatchError'
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns true for error with retryable property', () => {
    const error = new Error('retryable error') as Error & { retryable: boolean }
    error.retryable = true
    expect(isRetryableError(error)).toBe(true)
  })

  it('returns false for error with retryable=false', () => {
    const error = new Error('non-retryable') as Error & { retryable: boolean }
    error.retryable = false
    expect(isRetryableError(error)).toBe(false)
  })
})

describe('AbortError', () => {
  it('has correct name', () => {
    const error = new AbortError()
    expect(error.name).toBe('AbortError')
  })

  it('has default message', () => {
    const error = new AbortError()
    expect(error.message).toBe('Operation was aborted')
  })

  it('accepts custom message', () => {
    const error = new AbortError('Custom abort message')
    expect(error.message).toBe('Custom abort message')
  })

  it('is instance of Error', () => {
    const error = new AbortError()
    expect(error).toBeInstanceOf(Error)
  })
})

// =============================================================================
// DEFAULT CONFIG TESTS
// =============================================================================

describe('DEFAULT_RETRY_CONFIG', () => {
  it('has expected default values', () => {
    expect(DEFAULT_RETRY_CONFIG.maxRetries).toBe(3)
    expect(DEFAULT_RETRY_CONFIG.baseDelay).toBe(100)
    expect(DEFAULT_RETRY_CONFIG.maxDelay).toBe(10000)
    expect(DEFAULT_RETRY_CONFIG.multiplier).toBe(2)
    expect(DEFAULT_RETRY_CONFIG.jitter).toBe(true)
    expect(DEFAULT_RETRY_CONFIG.jitterFactor).toBe(0.5)
  })
})

// =============================================================================
// withRetry TESTS
// =============================================================================

describe('withRetry', () => {
  // Mock delay function for instant tests
  const instantDelay = async (_ms: number): Promise<void> => {}

  describe('successful operations', () => {
    it('returns result on first success', async () => {
      const fn = vi.fn().mockResolvedValue('success')

      const result = await withRetry(fn, { _delayFn: instantDelay })

      expect(result).toBe('success')
      expect(fn).toHaveBeenCalledTimes(1)
    })

    it('handles synchronous functions', async () => {
      const fn = vi.fn().mockReturnValue('sync result')

      const result = await withRetry(fn, { _delayFn: instantDelay })

      expect(result).toBe('sync result')
    })

    it('calls onSuccess callback on success', async () => {
      const onSuccess = vi.fn()
      const fn = vi.fn().mockResolvedValue('success')

      await withRetry(fn, {
        _delayFn: instantDelay,
        onSuccess,
      })

      expect(onSuccess).toHaveBeenCalledTimes(1)
      expect(onSuccess).toHaveBeenCalledWith({
        result: 'success',
        attempts: 1,
      })
    })
  })

  describe('retryable errors', () => {
    it('retries on retryable error', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success after retry')

      const result = await withRetry(fn, { _delayFn: instantDelay })

      expect(result).toBe('success after retry')
      expect(fn).toHaveBeenCalledTimes(2)
    })

    it('retries multiple times before success', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      const result = await withRetry(fn, {
        _delayFn: instantDelay,
        maxRetries: 5,
      })

      expect(result).toBe('success')
      expect(fn).toHaveBeenCalledTimes(4)
    })

    it('calls onRetry callback before each retry', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const onRetry = vi.fn()
      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: instantDelay,
        onRetry,
      })

      expect(onRetry).toHaveBeenCalledTimes(2)
      expect(onRetry.mock.calls[0][0].attempt).toBe(1)
      expect(onRetry.mock.calls[1][0].attempt).toBe(2)
    })

    it('aborts if onRetry returns false', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const onRetry = vi.fn().mockReturnValue(false)
      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('should not reach')

      await expect(withRetry(fn, {
        _delayFn: instantDelay,
        onRetry,
      })).rejects.toThrow('conflict')

      expect(fn).toHaveBeenCalledTimes(1)
      expect(onRetry).toHaveBeenCalledTimes(1)
    })
  })

  describe('non-retryable errors', () => {
    it('does not retry on non-retryable error', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('permanent error'))

      await expect(withRetry(fn, { _delayFn: instantDelay })).rejects.toThrow('permanent error')

      expect(fn).toHaveBeenCalledTimes(1)
    })

    it('does not retry on TypeError', async () => {
      const fn = vi.fn().mockRejectedValue(new TypeError('type error'))

      await expect(withRetry(fn, { _delayFn: instantDelay })).rejects.toThrow('type error')

      expect(fn).toHaveBeenCalledTimes(1)
    })
  })

  describe('max retries exhausted', () => {
    it('throws after max retries', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn().mockRejectedValue(concurrencyError)

      await expect(withRetry(fn, {
        _delayFn: instantDelay,
        maxRetries: 3,
      })).rejects.toThrow('conflict')

      // 1 initial attempt + 3 retries = 4 total
      expect(fn).toHaveBeenCalledTimes(4)
    })

    it('calls onFailure when retries exhausted', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const onFailure = vi.fn()
      const fn = vi.fn().mockRejectedValue(concurrencyError)

      await expect(withRetry(fn, {
        _delayFn: instantDelay,
        maxRetries: 2,
        onFailure,
      })).rejects.toThrow()

      expect(onFailure).toHaveBeenCalledTimes(1)
      expect(onFailure).toHaveBeenCalledWith({
        error: concurrencyError,
        attempts: 3,
      })
    })
  })

  describe('custom isRetryable', () => {
    it('uses custom isRetryable function', async () => {
      const customError = new Error('custom retryable')

      const isRetryable = vi.fn().mockReturnValue(true)
      const fn = vi.fn()
        .mockRejectedValueOnce(customError)
        .mockResolvedValueOnce('success')

      const result = await withRetry(fn, {
        _delayFn: instantDelay,
        isRetryable,
      })

      expect(result).toBe('success')
      expect(isRetryable).toHaveBeenCalledWith(customError)
    })

    it('does not retry if custom isRetryable returns false', async () => {
      const error = new Error('non-retryable')
      error.name = 'ConcurrencyError' // Would normally be retryable

      const isRetryable = vi.fn().mockReturnValue(false)
      const fn = vi.fn().mockRejectedValue(error)

      await expect(withRetry(fn, {
        _delayFn: instantDelay,
        isRetryable,
      })).rejects.toThrow()

      expect(fn).toHaveBeenCalledTimes(1)
    })
  })

  describe('returnMetrics option', () => {
    it('returns metrics on success', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      const { result, metrics } = await withRetry(fn, {
        _delayFn: instantDelay,
        returnMetrics: true,
      })

      expect(result).toBe('success')
      expect(metrics.attempts).toBe(2)
      expect(metrics.retries).toBe(1)
      expect(metrics.succeeded).toBe(true)
      expect(metrics.errors.length).toBe(1)
      expect(metrics.delays.length).toBe(1)
      expect(metrics.elapsedMs).toBeGreaterThanOrEqual(0)
    })

    it('attaches metrics to error on failure', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn().mockRejectedValue(concurrencyError)

      try {
        await withRetry(fn, {
          _delayFn: instantDelay,
          maxRetries: 2,
          returnMetrics: true,
        })
        expect.fail('Should have thrown')
      } catch (e) {
        const error = e as Error & { metrics: RetryMetrics }
        expect(error.metrics).toBeDefined()
        expect(error.metrics.succeeded).toBe(false)
        expect(error.metrics.attempts).toBe(3)
      }
    })
  })

  describe('abort signal', () => {
    it('throws AbortError if signal is already aborted', async () => {
      const controller = new AbortController()
      controller.abort()

      const fn = vi.fn().mockResolvedValue('success')

      await expect(withRetry(fn, {
        signal: controller.signal,
      })).rejects.toThrow(AbortError)

      expect(fn).not.toHaveBeenCalled()
    })

    it('throws AbortError if signal aborts during delay', async () => {
      const controller = new AbortController()
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delayFn = vi.fn().mockImplementation(async () => {
        controller.abort()
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await expect(withRetry(fn, {
        _delayFn: delayFn,
        signal: controller.signal,
      })).rejects.toThrow(AbortError)
    })
  })

  describe('delay calculation', () => {
    it('respects baseDelay', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const delayFn = vi.fn().mockImplementation(async (ms: number) => {
        delays.push(ms)
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: delayFn,
        baseDelay: 500,
        jitter: false,
      })

      expect(delays.length).toBe(1)
      expect(delays[0]).toBe(500) // First retry delay = baseDelay
    })

    it('applies exponential backoff', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const delayFn = vi.fn().mockImplementation(async (ms: number) => {
        delays.push(ms)
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: delayFn,
        baseDelay: 100,
        multiplier: 2,
        jitter: false,
      })

      expect(delays).toEqual([100, 200, 400])
    })

    it('caps delay at maxDelay', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const delayFn = vi.fn().mockImplementation(async (ms: number) => {
        delays.push(ms)
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: delayFn,
        baseDelay: 100,
        maxDelay: 300,
        multiplier: 2,
        maxRetries: 5,
        jitter: false,
      })

      // Should cap at 300
      expect(delays).toEqual([100, 200, 300, 300, 300])
    })

    it('applies jitter when enabled', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const delayFn = vi.fn().mockImplementation(async (ms: number) => {
        delays.push(ms)
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: delayFn,
        baseDelay: 100,
        jitter: true,
        jitterFactor: 0.5,
      })

      // With jitter, delays should be within jitter range
      // Base delay is 100, jitter factor 0.5 means +/- 50
      // So delay should be between 50 and 150
      expect(delays[0]).toBeGreaterThanOrEqual(0)
      expect(delays[0]).toBeLessThanOrEqual(150)
    })
  })

  describe('error handling', () => {
    it('wraps non-Error thrown values', async () => {
      const fn = vi.fn().mockRejectedValue('string error')

      await expect(withRetry(fn, {
        _delayFn: instantDelay,
      })).rejects.toThrow('string error')
    })

    it('propagates delay function errors', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delayFn = vi.fn().mockRejectedValue(new Error('delay failed'))
      const fn = vi.fn().mockRejectedValue(concurrencyError)

      await expect(withRetry(fn, {
        _delayFn: delayFn,
      })).rejects.toThrow('delay failed')
    })
  })

  describe('onRetry callback info', () => {
    it('provides correct info to onRetry', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const retryInfos: RetryInfo[] = []
      const onRetry = vi.fn().mockImplementation((info: RetryInfo) => {
        retryInfos.push(info)
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: instantDelay,
        baseDelay: 100,
        jitter: false,
        onRetry,
      })

      expect(retryInfos.length).toBe(2)

      expect(retryInfos[0].attempt).toBe(1)
      expect(retryInfos[0].error).toBe(concurrencyError)
      expect(retryInfos[0].delay).toBe(100)

      expect(retryInfos[1].attempt).toBe(2)
      expect(retryInfos[1].error).toBe(concurrencyError)
      expect(retryInfos[1].delay).toBe(200) // exponential backoff
    })
  })
})

// =============================================================================
// INTEGRATION-LIKE TESTS
// =============================================================================

describe('withRetry Integration Scenarios', () => {
  const instantDelay = async (_ms: number): Promise<void> => {}

  it('handles database write with optimistic concurrency', async () => {
    let attempts = 0
    const concurrencyError = new Error('version conflict')
    concurrencyError.name = 'ConcurrencyError'

    const writeToDatabase = async () => {
      attempts++
      if (attempts < 3) {
        throw concurrencyError
      }
      return { id: 'doc-123', version: attempts }
    }

    const result = await withRetry(writeToDatabase, {
      _delayFn: instantDelay,
    })

    expect(result).toEqual({ id: 'doc-123', version: 3 })
    expect(attempts).toBe(3)
  })

  it('fails fast on permission errors', async () => {
    const permissionError = new Error('Permission denied')

    const deleteFile = async () => {
      throw permissionError
    }

    await expect(withRetry(deleteFile, {
      _delayFn: instantDelay,
    })).rejects.toThrow('Permission denied')
  })

  it('collects metrics for monitoring', async () => {
    const concurrencyError = new Error('conflict')
    concurrencyError.name = 'ConcurrencyError'

    let attemptCount = 0
    const operation = async () => {
      attemptCount++
      if (attemptCount < 3) {
        throw concurrencyError
      }
      return 'completed'
    }

    const { result, metrics } = await withRetry(operation, {
      _delayFn: instantDelay,
      returnMetrics: true,
    })

    expect(result).toBe('completed')
    expect(metrics.succeeded).toBe(true)
    expect(metrics.attempts).toBe(3)
    expect(metrics.retries).toBe(2)
    expect(metrics.errors.length).toBe(2)
  })

  it('supports cancellation via AbortController', async () => {
    const controller = new AbortController()
    const concurrencyError = new Error('conflict')
    concurrencyError.name = 'ConcurrencyError'

    let operationAttempts = 0
    const longRunningOperation = async () => {
      operationAttempts++
      throw concurrencyError
    }

    // Abort after first failure
    const delayFn = async () => {
      controller.abort()
    }

    await expect(withRetry(longRunningOperation, {
      _delayFn: delayFn,
      signal: controller.signal,
      maxRetries: 10,
    })).rejects.toThrow(AbortError)

    expect(operationAttempts).toBe(1)
  })

  it('handles mixed success and failure scenarios', async () => {
    const versionError = new Error('version mismatch')
    versionError.name = 'VersionMismatchError'

    const networkError = new Error('network timeout') as Error & { retryable: boolean }
    networkError.retryable = true

    let attempt = 0
    const mixedOperation = async () => {
      attempt++
      if (attempt === 1) throw versionError
      if (attempt === 2) throw networkError
      if (attempt === 3) throw new Error('permanent failure')
      return 'should not reach'
    }

    await expect(withRetry(mixedOperation, {
      _delayFn: instantDelay,
    })).rejects.toThrow('permanent failure')

    expect(attempt).toBe(3)
  })
})

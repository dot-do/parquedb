/**
 * Tests for the safeCallback utility
 *
 * Issue: parquedb-amcq.3
 *
 * Verifies that the safeCallback utility:
 * 1. Handles synchronous callbacks correctly
 * 2. Handles asynchronous callbacks correctly
 * 3. Catches and logs errors without propagating them
 * 4. Calls onSuccess and onError handlers appropriately
 * 5. Formats context information for logging
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  safeCallback,
  createSafeCallback,
  safeCallbackIfDefined,
  type SafeCallbackResult,
} from '../../../src/utils/safe-callback'
import { setLogger, noopLogger, type Logger } from '../../../src/utils/logger'

describe('safeCallback utility', () => {
  let logMessages: { level: string; message: string; args: unknown[] }[]
  let testLogger: Logger

  beforeEach(() => {
    logMessages = []
    testLogger = {
      debug(message: string, ...args: unknown[]): void {
        logMessages.push({ level: 'debug', message, args })
      },
      info(message: string, ...args: unknown[]): void {
        logMessages.push({ level: 'info', message, args })
      },
      warn(message: string, ...args: unknown[]): void {
        logMessages.push({ level: 'warn', message, args })
      },
      error(message: string, error?: unknown, ...args: unknown[]): void {
        logMessages.push({ level: 'error', message, args: [error, ...args] })
      },
    }
    setLogger(testLogger)
  })

  afterEach(() => {
    setLogger(noopLogger)
  })

  describe('synchronous callbacks', () => {
    it('should return sync success for synchronous callback that completes', () => {
      const callback = vi.fn()
      const result = safeCallback(callback, {})

      expect(result.type).toBe('sync')
      expect((result as { type: 'sync'; success: boolean }).success).toBe(true)
      expect(callback).toHaveBeenCalledTimes(1)
    })

    it('should pass arguments to the callback', () => {
      const callback = vi.fn()
      safeCallback(callback, {}, 'arg1', 42, { key: 'value' })

      expect(callback).toHaveBeenCalledWith('arg1', 42, { key: 'value' })
    })

    it('should catch synchronous errors and return failure', () => {
      const error = new Error('Sync error')
      const callback = vi.fn(() => {
        throw error
      })

      const result = safeCallback(callback, {})

      expect(result.type).toBe('sync')
      expect((result as { type: 'sync'; success: boolean }).success).toBe(false)
      expect((result as { type: 'sync'; success: false; error: Error }).error).toBe(error)
    })

    it('should log errors with context', () => {
      const callback = vi.fn(() => {
        throw new Error('Test error message')
      })

      safeCallback(callback, {
        logPrefix: '[TestPrefix]',
        context: { operation: 'testOp', id: 123 },
      })

      const warningLogs = logMessages.filter(l => l.level === 'warn')
      expect(warningLogs.length).toBeGreaterThan(0)
      expect(warningLogs[0]!.message).toContain('[TestPrefix]')
      expect(warningLogs[0]!.message).toContain('Test error message')
      expect(warningLogs[0]!.message).toContain('operation=testOp')
      expect(warningLogs[0]!.message).toContain('id=123')
    })

    it('should call onSuccess handler on success', () => {
      const onSuccess = vi.fn()
      const callback = vi.fn()

      safeCallback(callback, { onSuccess })

      expect(onSuccess).toHaveBeenCalledTimes(1)
    })

    it('should call onError handler on error', () => {
      const onError = vi.fn()
      const error = new Error('Test error')
      const callback = vi.fn(() => {
        throw error
      })
      const context = { foo: 'bar' }

      safeCallback(callback, { onError, context })

      expect(onError).toHaveBeenCalledTimes(1)
      expect(onError).toHaveBeenCalledWith(error, context)
    })

    it('should not propagate errors from error handler', () => {
      const onError = vi.fn(() => {
        throw new Error('Handler error')
      })
      const callback = vi.fn(() => {
        throw new Error('Original error')
      })

      // Should not throw
      const result = safeCallback(callback, { onError, context: {} })

      expect(result.type).toBe('sync')
      expect((result as { type: 'sync'; success: boolean }).success).toBe(false)
    })

    it('should rethrow sync errors when rethrowSync is true', () => {
      const error = new Error('Rethrow me')
      const callback = vi.fn(() => {
        throw error
      })

      expect(() => {
        safeCallback(callback, { rethrowSync: true })
      }).toThrow(error)
    })
  })

  describe('asynchronous callbacks', () => {
    it('should return async type for Promise-returning callback', async () => {
      const callback = vi.fn(async () => {})
      const result = safeCallback(callback, {})

      expect(result.type).toBe('async')
      expect((result as { type: 'async'; promise: Promise<void> }).promise).toBeInstanceOf(Promise)

      // Wait for promise to settle
      await (result as { type: 'async'; promise: Promise<void> }).promise
    })

    it('should call onSuccess after async callback resolves', async () => {
      const onSuccess = vi.fn()
      const callback = vi.fn(async () => {
        await new Promise(resolve => setTimeout(resolve, 10))
      })

      const result = safeCallback(callback, { onSuccess })
      expect(onSuccess).not.toHaveBeenCalled()

      await (result as { type: 'async'; promise: Promise<void> }).promise
      expect(onSuccess).toHaveBeenCalledTimes(1)
    })

    it('should catch async errors and call onError', async () => {
      const onError = vi.fn()
      const error = new Error('Async error')
      const callback = vi.fn(async () => {
        throw error
      })
      const context = { async: true }

      const result = safeCallback(callback, { onError, context })

      // Wait for promise to settle
      await (result as { type: 'async'; promise: Promise<void> }).promise

      expect(onError).toHaveBeenCalledTimes(1)
      expect(onError).toHaveBeenCalledWith(error, context)
    })

    it('should log async errors', async () => {
      const callback = vi.fn(async () => {
        throw new Error('Async failure')
      })

      const result = safeCallback(callback, {
        logPrefix: '[AsyncTest]',
        context: { phase: 'async' },
      })

      await (result as { type: 'async'; promise: Promise<void> }).promise

      const warningLogs = logMessages.filter(l => l.level === 'warn')
      expect(warningLogs.length).toBeGreaterThan(0)
      expect(warningLogs[0]!.message).toContain('[AsyncTest]')
      expect(warningLogs[0]!.message).toContain('Async failure')
    })

    it('should not cause unhandled promise rejection', async () => {
      const unhandledRejections: unknown[] = []
      const handler = (reason: unknown) => {
        unhandledRejections.push(reason)
      }
      process.on('unhandledRejection', handler)

      try {
        const callback = vi.fn(async () => {
          throw new Error('Should be caught')
        })

        safeCallback(callback, {})

        // Wait for async error handling
        await new Promise(resolve => setTimeout(resolve, 50))

        expect(unhandledRejections).toHaveLength(0)
      } finally {
        process.off('unhandledRejection', handler)
      }
    })
  })

  describe('createSafeCallback', () => {
    it('should create a wrapped callback function', () => {
      const original = vi.fn()
      const safe = createSafeCallback(original, {})

      const result = safe('arg1', 'arg2')

      expect(original).toHaveBeenCalledWith('arg1', 'arg2')
      expect(result.type).toBe('sync')
    })

    it('should preserve options across calls', () => {
      const onSuccess = vi.fn()
      const original = vi.fn()
      const safe = createSafeCallback(original, { onSuccess })

      safe()
      safe()
      safe()

      expect(onSuccess).toHaveBeenCalledTimes(3)
    })
  })

  describe('safeCallbackIfDefined', () => {
    it('should return null for undefined callback', () => {
      const result = safeCallbackIfDefined(undefined, {})
      expect(result).toBeNull()
    })

    it('should return null for null callback', () => {
      const result = safeCallbackIfDefined(null, {})
      expect(result).toBeNull()
    })

    it('should invoke defined callback', () => {
      const callback = vi.fn()
      const result = safeCallbackIfDefined(callback, {}, 'arg')

      expect(callback).toHaveBeenCalledWith('arg')
      expect(result).not.toBeNull()
      expect(result!.type).toBe('sync')
    })
  })

  describe('context formatting', () => {
    it('should format object context as key=value pairs', () => {
      const callback = vi.fn(() => {
        throw new Error('Test')
      })

      safeCallback(callback, {
        context: { a: 'string', b: 123, c: true },
      })

      const log = logMessages.find(l => l.level === 'warn')!
      expect(log.message).toContain('a=string')
      expect(log.message).toContain('b=123')
      expect(log.message).toContain('c=true')
    })

    it('should handle string context', () => {
      const callback = vi.fn(() => {
        throw new Error('Test')
      })

      safeCallback(callback, {
        context: 'simple-context',
      })

      const log = logMessages.find(l => l.level === 'warn')!
      expect(log.message).toContain('simple-context')
    })

    it('should limit context entries to 5', () => {
      const callback = vi.fn(() => {
        throw new Error('Test')
      })

      safeCallback(callback, {
        context: { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7 },
      })

      const log = logMessages.find(l => l.level === 'warn')!
      // Should show first 5 and indicate there are more
      expect(log.message).toContain('...')
    })
  })
})

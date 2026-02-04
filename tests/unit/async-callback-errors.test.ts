/**
 * Tests for async event callback error handling
 *
 * Issue: parquedb-amcq.1
 *
 * Verifies that async callback rejections in the event system are:
 * 1. Caught and logged (not silently swallowed)
 * 2. Isolated from the caller (errors don't propagate)
 * 3. Circuit breaker protected (frequently failing callbacks are disabled)
 * 4. Include callback context (which event, which callback)
 *
 * These tests verify the RED phase - they should FAIL until the feature is implemented.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { setLogger, noopLogger, type Logger } from '../../src/utils/logger'
import type { Event } from '../../src/types'

describe('Async Event Callback Error Handling', () => {
  let db: ParqueDB
  let storage: MemoryBackend
  let logMessages: { level: string; message: string; args: unknown[] }[]
  let testLogger: Logger

  beforeEach(() => {
    storage = new MemoryBackend()

    // Create a logger that captures all log messages for inspection
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

    db = new ParqueDB({ storage })
  })

  afterEach(() => {
    setLogger(noopLogger)
    // Clean up to prevent hanging on pending flush promises
    db.dispose()
  })

  describe('async rejection is caught and logged', () => {
    it('should log errors when async event callback rejects', async () => {
      // Set up a callback that always rejects
      const callbackError = new Error('Async callback failed')
      db.setEventCallback(async (_event: Event) => {
        throw callbackError
      })

      // This should NOT throw - the error should be caught internally
      await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      // Allow async operations to settle (fire-and-forget pattern)
      await new Promise(resolve => setTimeout(resolve, 10))

      // The error should have been logged with context
      // Current implementation logs: "[ParqueDB] MV event callback error:"
      // We want it to include the actual error message, not just a generic message
      const errorLogs = logMessages.filter(log =>
        log.level === 'warn' && log.message.includes('callback')
      )

      // Test expectation: error should be logged with context
      expect(errorLogs.length).toBeGreaterThan(0)

      // FAILING EXPECTATION: The log message should contain the actual error message
      // Current implementation only logs the error object, but doesn't include
      // event context (op, target, etc.) in the message itself
      const logMessageWithContext = errorLogs.find(log =>
        log.message.includes('Async callback failed') &&
        log.message.includes('CREATE') // Should include event type
      )
      expect(logMessageWithContext).toBeDefined()
    })

    it('should include error message in log output', async () => {
      const specificMessage = 'Very specific error: XYZ123'
      db.setEventCallback(async () => {
        throw new Error(specificMessage)
      })

      await db.create('posts', { $type: 'Post', name: 'Test', title: 'Test' })
      await new Promise(resolve => setTimeout(resolve, 10))

      // Should find the specific error message in logs
      const hasSpecificError = logMessages.some(log =>
        JSON.stringify(log).includes(specificMessage)
      )
      expect(hasSpecificError).toBe(true)
    })
  })

  describe('callback errors do not propagate to caller', () => {
    it('should not throw when async callback rejects', async () => {
      db.setEventCallback(async () => {
        throw new Error('This should not propagate')
      })

      // This should complete successfully despite the callback failure
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Test',
      })

      expect(entity).toBeDefined()
      expect(entity.$id).toBeDefined()
    })

    it('should not throw when sync callback throws in Promise.resolve wrapper', async () => {
      // Callback that throws synchronously but looks async
      db.setEventCallback((_event: Event) => {
        throw new Error('Sync error in async context')
      })

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Test',
      })

      expect(entity).toBeDefined()
    })

    it('should continue processing after callback error', async () => {
      let callCount = 0
      db.setEventCallback(async () => {
        callCount++
        throw new Error('Always fails')
      })

      // Create multiple entities - all should succeed despite callback failures
      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'T1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'T2' })
      await db.create('posts', { $type: 'Post', name: 'Post 3', title: 'T3' })

      await new Promise(resolve => setTimeout(resolve, 20))

      // Callback should have been called for each entity
      expect(callCount).toBe(3)
    })
  })

  describe('circuit breaker for frequently failing callbacks', () => {
    it('should disable callback after threshold failures', async () => {
      let callCount = 0
      const failingCallback = vi.fn(async () => {
        callCount++
        throw new Error('Always fails')
      })

      db.setEventCallback(failingCallback)

      // Create many entities to trigger circuit breaker
      for (let i = 0; i < 20; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `T${i}` })
      }

      await new Promise(resolve => setTimeout(resolve, 50))

      // FAILING EXPECTATION: After circuit breaker triggers, callback should stop
      // Current implementation has no circuit breaker - all 20 calls are made
      expect(callCount).toBeLessThan(20)

      // Should have logged a circuit breaker message
      const circuitBreakerLogs = logMessages.filter(log =>
        log.message.includes('circuit') ||
        log.message.includes('disabled') ||
        log.message.includes('too many failures')
      )
      expect(circuitBreakerLogs.length).toBeGreaterThan(0)
    })

    it('should log when circuit breaker trips', async () => {
      db.setEventCallback(async () => {
        throw new Error('Persistent failure')
      })

      // Create enough entities to trip the circuit breaker
      for (let i = 0; i < 15; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `T${i}` })
      }

      await new Promise(resolve => setTimeout(resolve, 50))

      // FAILING EXPECTATION: Should have a log indicating the circuit breaker tripped
      const circuitBreakerLog = logMessages.find(log =>
        log.level === 'warn' &&
        (log.message.includes('circuit breaker') ||
         log.message.includes('callback disabled') ||
         log.message.includes('too many failures'))
      )
      expect(circuitBreakerLog).toBeDefined()
    })
  })

  describe('error includes callback context', () => {
    it('should include event type in error log', async () => {
      db.setEventCallback(async () => {
        throw new Error('Callback failed')
      })

      await db.create('posts', { $type: 'Post', name: 'Test', title: 'Test' })
      await new Promise(resolve => setTimeout(resolve, 10))

      // FAILING EXPECTATION: The log should include information about the CREATE event
      // Current implementation just logs "[ParqueDB] MV event callback error:" + error
      const hasEventContext = logMessages.some(log =>
        log.message.includes('CREATE')
      )
      expect(hasEventContext).toBe(true)
    })

    it('should include target namespace in error log', async () => {
      db.setEventCallback(async () => {
        throw new Error('Callback failed')
      })

      await db.create('orders', { $type: 'Order', name: 'Order 1', total: 100 })
      await new Promise(resolve => setTimeout(resolve, 10))

      // FAILING EXPECTATION: The log should include information about the orders namespace
      const hasNamespaceContext = logMessages.some(log =>
        log.message.includes('orders')
      )
      expect(hasNamespaceContext).toBe(true)
    })

    it('should include entity ID in error log for entity events', async () => {
      let capturedEvent: Event | null = null
      db.setEventCallback(async (event: Event) => {
        capturedEvent = event
        throw new Error('Callback failed')
      })

      const entity = await db.create('posts', { $type: 'Post', name: 'Test', title: 'Test' })
      await new Promise(resolve => setTimeout(resolve, 10))

      // FAILING EXPECTATION: The log should include the entity ID or target
      const entityId = entity.$id
      const hasEntityIdContext = logMessages.some(log =>
        log.message.includes(String(entityId)) ||
        (capturedEvent && log.message.includes(capturedEvent.target))
      )
      expect(hasEntityIdContext).toBe(true)
    })
  })

  describe('unhandled rejection protection', () => {
    it('should not cause unhandled promise rejection', async () => {
      const unhandledRejections: unknown[] = []

      // Note: In Node.js/Vitest environment, we use process event
      const nodeHandler = (reason: unknown) => {
        unhandledRejections.push(reason)
      }
      process.on('unhandledRejection', nodeHandler)

      try {
        db.setEventCallback(async () => {
          throw new Error('This should be caught')
        })

        await db.create('posts', { $type: 'Post', name: 'Test', title: 'Test' })

        // Give time for any unhandled rejections to be reported
        await new Promise(resolve => setTimeout(resolve, 50))

        // No unhandled rejections should have occurred
        // Current implementation DOES handle this correctly with .catch()
        expect(unhandledRejections).toHaveLength(0)
      } finally {
        process.off('unhandledRejection', nodeHandler)
      }
    })
  })
})

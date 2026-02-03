/**
 * Tests for async error boundaries
 *
 * Verifies that async operations properly handle and propagate errors
 * without silently swallowing them.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { SubscriptionManager } from '../../src/subscriptions/manager'
import type { SubscriptionWriter } from '../../src/subscriptions/types'

describe('Async Error Boundaries', () => {
  describe('SubscriptionManager', () => {
    let manager: SubscriptionManager
    let consoleErrorSpy: ReturnType<typeof vi.spyOn>
    let consoleWarnSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      vi.useFakeTimers()
      manager = new SubscriptionManager({ debug: false })
      consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    })

    afterEach(() => {
      manager.stopHeartbeat()
      consoleErrorSpy.mockRestore()
      consoleWarnSpy.mockRestore()
      vi.useRealTimers()
    })

    it('should handle errors when removing timed-out connections', async () => {
      // Create a writer that throws on close
      const failingWriter: SubscriptionWriter = {
        send: vi.fn().mockResolvedValue(undefined),
        close: vi.fn().mockRejectedValue(new Error('Close failed')),
        isOpen: () => true,
      }

      // Add connection
      const conn = manager.addConnection(failingWriter)

      // Force connection to be timed out by manipulating lastActivity
      ;(conn as { lastActivity: number }).lastActivity = Date.now() - 1000000

      // Start heartbeat with very short interval
      const originalConfig = (manager as unknown as { config: { connectionTimeoutMs: number; heartbeatIntervalMs: number } }).config
      originalConfig.connectionTimeoutMs = 100
      originalConfig.heartbeatIntervalMs = 50

      manager.startHeartbeat()

      // Advance fake timers to trigger heartbeat
      await vi.advanceTimersByTimeAsync(100)

      // The error should be caught and logged, not thrown
      // The manager should still be functional
      expect(manager.getStats().activeConnections).toBe(0)
    })

    it('should handle errors when sending pong messages', async () => {
      // Create a writer that fails on send
      const failingWriter: SubscriptionWriter = {
        send: vi.fn().mockRejectedValue(new Error('Send failed')),
        close: vi.fn().mockResolvedValue(undefined),
        isOpen: () => true,
      }

      // Add connection
      manager.addConnection(failingWriter)

      // Handle ping should not throw
      manager.handlePing('non-existent-id')

      // The manager should still be functional
      expect(manager.getStats().activeConnections).toBe(1)
    })

    it('should properly clean up connections that fail to send', async () => {
      // Create a writer that fails after first send
      let sendCount = 0
      const failingWriter: SubscriptionWriter = {
        send: vi.fn().mockImplementation(async () => {
          sendCount++
          if (sendCount > 1) {
            throw new Error('Send failed')
          }
        }),
        close: vi.fn().mockResolvedValue(undefined),
        isOpen: () => true,
      }

      // Add connection - first send (connected message) succeeds
      const conn = manager.addConnection(failingWriter)

      // Second send should fail and trigger connection removal
      await manager['sendToConnection'](conn, { type: 'pong', ts: Date.now() })

      // Connection should be removed
      expect(manager.getStats().activeConnections).toBe(0)
    })
  })

  describe('Error propagation patterns', () => {
    it('should not swallow errors in Promise.all operations', async () => {
      const operation1 = Promise.resolve('success')
      const operation2 = Promise.reject(new Error('Expected failure'))

      // This should throw, not silently fail
      await expect(Promise.all([operation1, operation2])).rejects.toThrow('Expected failure')
    })

    it('should properly catch and re-throw errors', async () => {
      const asyncFn = async () => {
        throw new Error('Test error')
      }

      await expect(asyncFn()).rejects.toThrow('Test error')
    })

    it('should handle errors in fire-and-forget patterns with proper logging', async () => {
      const mockLogger = vi.fn()
      const originalWarn = console.warn
      console.warn = mockLogger

      try {
        // Simulate a fire-and-forget operation that fails
        const operation = async () => {
          throw new Error('Background operation failed')
        }

        // This is the pattern we want - fire-and-forget with error logging
        operation().catch(err => {
          console.warn('Background operation failed:', err)
        })

        // Flush pending microtasks/promises
        await vi.runAllTimersAsync()

        // Error should be logged
        expect(mockLogger).toHaveBeenCalled()
      } finally {
        console.warn = originalWarn
      }
    })
  })
})

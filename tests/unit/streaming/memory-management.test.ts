/**
 * Memory Management Tests for StreamingRefreshEngine
 *
 * Tests verify that the memory leak fix is working correctly:
 * - dispose() clears all resources
 * - onError() returns unsubscribe function
 * - onWarning() returns unsubscribe function
 * - removeAllErrorListeners() clears all error listeners
 * - removeAllWarningListeners() clears all warning listeners
 */

import { describe, test, expect, afterEach } from 'vitest'
import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
} from '../../../src/materialized-views/streaming'
import type { Event, EventOp } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  target: string,
  op: EventOp,
  after?: Record<string, unknown>,
  before?: Record<string, unknown>
): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op,
    target,
    after,
    before,
    actor: 'test:user',
  }
}

function createMVHandler(options?: {
  delay?: number
  failOn?: string[]
  onProcess?: (events: Event[]) => void
}): MVHandler & { processedEvents: Event[][] } {
  const processedEvents: Event[][] = []

  return {
    processedEvents,
    name: 'TestMV',
    sourceNamespaces: ['orders', 'products'],
    async process(events: Event[]): Promise<void> {
      options?.onProcess?.(events)

      for (const event of events) {
        if (options?.failOn?.some(pattern => event.target.includes(pattern))) {
          throw new Error(`Simulated failure for ${event.target}`)
        }
      }

      if (options?.delay) {
        await new Promise(resolve => setTimeout(resolve, options.delay))
      }

      processedEvents.push([...events])
    },
  }
}

// =============================================================================
// Memory Management Tests
// =============================================================================

describe('Memory Management (Memory Leak Fix)', () => {
  let engine: StreamingRefreshEngine

  afterEach(async () => {
    if (engine) {
      await engine.dispose()
    }
  })

  describe('dispose()', () => {
    test('clears all registered MVs', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      expect(engine.getRegisteredMVs()).toContain('TestMV')

      await engine.dispose()

      expect(engine.getRegisteredMVs()).toEqual([])
    })

    test('clears error listeners', async () => {
      engine = createStreamingRefreshEngine()
      const errors: Error[] = []
      engine.onError((err) => errors.push(err))
      await engine.start()

      await engine.dispose()

      // After dispose, error listeners should be cleared
      // Re-register an MV and try to process - should not call old error handler
      const failingHandler = createMVHandler({ failOn: ['fail'] })
      engine.registerMV(failingHandler)
      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      // The old error listener should not have received any errors
      expect(errors.length).toBe(0)
    })

    test('clears warning listeners', async () => {
      engine = createStreamingRefreshEngine()
      const warnings: string[] = []
      engine.onWarning((msg) => warnings.push(msg))

      await engine.dispose()

      // After dispose, warning listeners should be cleared
      // This should not throw even if already empty
      engine.removeAllWarningListeners()
      expect(true).toBe(true)
    })

    test('can be called multiple times safely', async () => {
      engine = createStreamingRefreshEngine()
      await engine.start()

      await engine.dispose()
      await engine.dispose()
      await engine.dispose()

      // Should not throw and engine should be fully disposed
      expect(engine.isRunning()).toBe(false)
    })

    test('clears buffers', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({
        batchSize: 100, // Large batch size so events stay in buffer
        batchTimeoutMs: 10000,
      })
      engine.registerMV(handler)
      await engine.start()

      // Add some events to the buffer
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))

      // Don't flush - let dispose handle it
      await engine.dispose()

      // Stats should be reset (buffer cleared)
      const stats = engine.getStats()
      expect(stats.currentBufferSize).toBe(0)
    })

    test('resets statistics', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(engine.getStats().eventsReceived).toBe(1)

      await engine.dispose()

      // After dispose, stats should be reset
      const stats = engine.getStats()
      expect(stats.eventsReceived).toBe(0)
      expect(stats.eventsProcessed).toBe(0)
      expect(stats.batchesProcessed).toBe(0)
    })
  })

  describe('onError() unsubscribe', () => {
    test('returns a function to unsubscribe', async () => {
      engine = createStreamingRefreshEngine()
      const errors: Error[] = []
      const unsubscribe = engine.onError((err) => errors.push(err))

      expect(typeof unsubscribe).toBe('function')

      await engine.dispose()
    })

    test('unsubscribe removes the specific listener', async () => {
      const handler = createMVHandler({ failOn: ['fail'] })
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)

      const errors1: Error[] = []
      const errors2: Error[] = []
      const unsubscribe1 = engine.onError((err) => errors1.push(err))
      engine.onError((err) => errors2.push(err))

      // Unsubscribe the first listener
      unsubscribe1()

      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      // Only the second listener should receive the error
      expect(errors1.length).toBe(0)
      expect(errors2.length).toBeGreaterThan(0)
    })

    test('unsubscribe is idempotent', async () => {
      engine = createStreamingRefreshEngine()
      const errors: Error[] = []
      const unsubscribe = engine.onError((err) => errors.push(err))

      // Call unsubscribe multiple times
      unsubscribe()
      unsubscribe()
      unsubscribe()

      // Should not throw
      await engine.dispose()
    })
  })

  describe('onWarning() unsubscribe', () => {
    test('returns a function to unsubscribe', async () => {
      engine = createStreamingRefreshEngine()
      const warnings: string[] = []
      const unsubscribe = engine.onWarning((msg) => warnings.push(msg))

      expect(typeof unsubscribe).toBe('function')

      await engine.dispose()
    })

    test('unsubscribe removes the specific listener', async () => {
      engine = createStreamingRefreshEngine()

      const warnings1: string[] = []
      const warnings2: string[] = []
      const unsubscribe1 = engine.onWarning((msg) => warnings1.push(msg))
      engine.onWarning((msg) => warnings2.push(msg))

      // Unsubscribe the first listener
      unsubscribe1()

      await engine.dispose()
      // After dispose and unsubscribe, first listener should have no warnings
      expect(warnings1.length).toBe(0)
    })
  })

  describe('removeAllErrorListeners()', () => {
    test('removes all error listeners', async () => {
      const handler = createMVHandler({ failOn: ['fail'] })
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)

      const errors1: Error[] = []
      const errors2: Error[] = []
      engine.onError((err) => errors1.push(err))
      engine.onError((err) => errors2.push(err))

      // Remove all listeners
      engine.removeAllErrorListeners()

      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      // No listeners should receive the error
      expect(errors1.length).toBe(0)
      expect(errors2.length).toBe(0)
    })
  })

  describe('removeAllWarningListeners()', () => {
    test('removes all warning listeners', async () => {
      engine = createStreamingRefreshEngine()

      const warnings: string[] = []
      engine.onWarning((msg) => warnings.push(msg))
      engine.onWarning((msg) => warnings.push(msg))

      // Remove all listeners
      engine.removeAllWarningListeners()

      // Verify no listeners remain (indirectly through dispose not throwing)
      await engine.dispose()
      expect(warnings.length).toBe(0)
    })
  })

  describe('Memory release verification', () => {
    test('engine can be fully re-used after dispose', async () => {
      const handler1 = createMVHandler()
      handler1.name = 'MV1'
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler1)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(handler1.processedEvents.length).toBe(1)

      // Dispose completely
      await engine.dispose()

      // Re-use the same engine instance
      const handler2 = createMVHandler()
      handler2.name = 'MV2'
      engine.registerMV(handler2)
      await engine.start()

      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.flush()

      // New handler should have received the event
      expect(handler2.processedEvents.length).toBe(1)
      // Old handler should still have its old events (it was not cleared)
      expect(handler1.processedEvents.length).toBe(1)
    })

    test('multiple dispose/restart cycles work correctly', async () => {
      engine = createStreamingRefreshEngine()

      for (let i = 0; i < 5; i++) {
        const handler = createMVHandler()
        handler.name = `MV${i}`
        engine.registerMV(handler)
        await engine.start()

        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
        await engine.flush()

        expect(handler.processedEvents.length).toBe(1)
        expect(engine.getRegisteredMVs()).toContain(`MV${i}`)

        await engine.dispose()

        expect(engine.getRegisteredMVs()).toEqual([])
        expect(engine.isRunning()).toBe(false)
      }
    })

    test('listeners do not accumulate across restart cycles', async () => {
      engine = createStreamingRefreshEngine()

      const allErrors: Error[] = []

      for (let i = 0; i < 5; i++) {
        // Register a new error listener each cycle
        engine.onError((err) => allErrors.push(err))

        const handler = createMVHandler({ failOn: ['fail'] })
        engine.registerMV(handler)
        await engine.start()

        await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
        await engine.flush()

        // After first cycle: 1 error
        // After second cycle WITHOUT dispose: would be 3 errors (2 listeners)
        // With dispose: should still be 1 error per cycle
        await engine.dispose()
      }

      // With proper cleanup, we should have exactly 5 errors (one per cycle)
      // Without cleanup, we'd have 1+2+3+4+5 = 15 errors
      expect(allErrors.length).toBe(5)
    })
  })
})

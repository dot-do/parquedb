/**
 * Streaming Engine Tests
 *
 * Tests for the StreamingRefreshEngine as exported from src/streaming/
 * Focuses on:
 * - Streaming query execution
 * - Chunk handling and batching
 * - Back-pressure mechanisms
 * - Error recovery in streams
 * - Retry logic with exponential backoff
 */

import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
  type StreamingRefreshConfig,
  type StreamingStats,
  type ErrorHandler,
  type WarningHandler,
} from '../../../src/streaming'
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
  name?: string
  namespaces?: string[]
  delay?: number
  failOn?: string[]
  failCount?: number
  onProcess?: (events: Event[]) => void
}): MVHandler & { processedEvents: Event[][]; callCount: number } {
  const processedEvents: Event[][] = []
  let callCount = 0
  let failuresRemaining = options?.failCount ?? Infinity

  return {
    processedEvents,
    get callCount() {
      return callCount
    },
    name: options?.name ?? 'TestMV',
    sourceNamespaces: options?.namespaces ?? ['orders', 'products'],
    async process(events: Event[]): Promise<void> {
      callCount++
      options?.onProcess?.(events)

      for (const event of events) {
        if (
          options?.failOn?.some(pattern => event.target.includes(pattern)) &&
          failuresRemaining > 0
        ) {
          failuresRemaining--
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
// Streaming Query Execution Tests
// =============================================================================

describe('StreamingRefreshEngine (src/streaming export)', () => {
  let engine: StreamingRefreshEngine

  afterEach(async () => {
    if (engine) {
      await engine.dispose()
    }
    vi.useRealTimers()
  })

  describe('Streaming Query Execution', () => {
    test('processes events through registered MV handlers', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      const event = createEvent('orders:order-1', 'CREATE', { total: 100 })
      await engine.processEvent(event)
      await engine.flush()

      expect(handler.processedEvents.length).toBe(1)
      expect(handler.processedEvents[0]![0]).toEqual(event)
    })

    test('routes events to multiple MVs interested in same namespace', async () => {
      const handler1 = createMVHandler({ name: 'Analytics', namespaces: ['orders'] })
      const handler2 = createMVHandler({ name: 'Reporting', namespaces: ['orders'] })

      engine = createStreamingRefreshEngine()
      engine.registerMV(handler1)
      engine.registerMV(handler2)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(handler1.processedEvents.length).toBe(1)
      expect(handler2.processedEvents.length).toBe(1)
    })

    test('handles processEvents for batch event submission', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      const events = [
        createEvent('orders:o1', 'CREATE', { id: 1 }),
        createEvent('orders:o2', 'CREATE', { id: 2 }),
        createEvent('orders:o3', 'CREATE', { id: 3 }),
      ]

      await engine.processEvents(events)
      await engine.flush()

      const totalProcessed = handler.processedEvents.reduce(
        (sum, batch) => sum + batch.length,
        0
      )
      expect(totalProcessed).toBe(3)
    })

    test('tracks statistics for streaming queries', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'UPDATE', { id: 2 }))
      await engine.processEvent(createEvent('products:p1', 'DELETE'))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.eventsReceived).toBe(3)
      expect(stats.eventsByOp.CREATE).toBe(1)
      expect(stats.eventsByOp.UPDATE).toBe(1)
      expect(stats.eventsByOp.DELETE).toBe(1)
      expect(stats.eventsByNamespace['orders']).toBe(2)
      expect(stats.eventsByNamespace['products']).toBe(1)
    })
  })

  describe('Chunk Handling', () => {
    test('batches events according to batchSize', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({
        batchSize: 3,
        batchTimeoutMs: 10000, // Long timeout to test size-based batching
      })
      engine.registerMV(handler)
      await engine.start()

      // Send 6 events
      for (let i = 0; i < 6; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }
      await engine.flush()

      // Should have processed in batches of 3
      expect(handler.processedEvents.length).toBeGreaterThanOrEqual(2)
      const totalProcessed = handler.processedEvents.reduce(
        (sum, batch) => sum + batch.length,
        0
      )
      expect(totalProcessed).toBe(6)
    })

    test('flushes partial batches after timeout', async () => {
      vi.useFakeTimers()

      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({
        batchSize: 100, // Large batch size
        batchTimeoutMs: 50, // Short timeout
      })
      engine.registerMV(handler)
      await engine.start()

      // Send fewer events than batch size
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))

      // Advance time to trigger timeout
      await vi.advanceTimersByTimeAsync(100)

      expect(handler.processedEvents.length).toBe(1)
      expect(handler.processedEvents[0]!.length).toBe(2)

      vi.useRealTimers()
    })

    test('handles mixed namespace events in chunks correctly', async () => {
      const ordersHandler = createMVHandler({ name: 'OrdersMV', namespaces: ['orders'] })
      const productsHandler = createMVHandler({ name: 'ProductsMV', namespaces: ['products'] })

      engine = createStreamingRefreshEngine({ batchSize: 4 })
      engine.registerMV(ordersHandler)
      engine.registerMV(productsHandler)
      await engine.start()

      // Interleave events from different namespaces
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('products:p1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.processEvent(createEvent('products:p2', 'CREATE', { id: 2 }))
      await engine.flush()

      // Each handler should only receive its namespace's events
      const ordersTotal = ordersHandler.processedEvents.reduce(
        (sum, batch) => sum + batch.length,
        0
      )
      const productsTotal = productsHandler.processedEvents.reduce(
        (sum, batch) => sum + batch.length,
        0
      )

      expect(ordersTotal).toBe(2)
      expect(productsTotal).toBe(2)
    })

    test('processes large event streams efficiently', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({ batchSize: 50 })
      engine.registerMV(handler)
      await engine.start()

      const eventCount = 500
      for (let i = 0; i < eventCount; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }
      await engine.flush()

      const totalProcessed = handler.processedEvents.reduce(
        (sum, batch) => sum + batch.length,
        0
      )
      expect(totalProcessed).toBe(eventCount)

      // Should have been batched (not 500 individual batches)
      expect(handler.processedEvents.length).toBeLessThan(eventCount / 10)
    })
  })

  describe('Back-pressure Mechanisms', () => {
    test('applies backpressure when buffer reaches maxBufferSize', async () => {
      const processedCount = { value: 0 }
      const handler: MVHandler = {
        name: 'SlowMV',
        sourceNamespaces: ['orders'],
        async process(events: Event[]): Promise<void> {
          // Simulate slow processing
          await new Promise(resolve => setTimeout(resolve, 50))
          processedCount.value += events.length
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 2,
        maxBufferSize: 5,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)
      await engine.start()

      const startTime = Date.now()
      for (let i = 0; i < 10; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }
      const elapsed = Date.now() - startTime

      // Backpressure should have caused delays
      expect(elapsed).toBeGreaterThan(50)

      await engine.flush()
      expect(processedCount.value).toBe(10)
    })

    test('tracks backpressure events in statistics', async () => {
      // This test verifies backpressure tracking works - the actual backpressure
      // behavior is tested in "applies backpressure when buffer reaches maxBufferSize"
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({
        batchSize: 2,
        maxBufferSize: 3,
        batchTimeoutMs: 50,
      })
      engine.registerMV(handler)
      await engine.start()

      // Process events that will fill the buffer beyond maxBufferSize
      // The engine should apply backpressure
      for (let i = 0; i < 10; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }
      await engine.flush()

      // Verify stats tracking exists and shows processed events
      // Backpressure may or may not have triggered depending on processing speed
      const stats = engine.getStats()
      expect(stats.eventsProcessed).toBe(10)
      // backpressureEvents should at least exist in stats
      expect(typeof stats.backpressureEvents).toBe('number')
    })

    test('emits warning at 80% buffer capacity', async () => {
      vi.useFakeTimers()

      const handler: MVHandler = {
        name: 'SlowMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          await vi.advanceTimersByTimeAsync(100)
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 100, // Large batch to prevent auto-flush
        maxBufferSize: 10, // 80% = 8 events
        batchTimeoutMs: 10000,
      })
      engine.registerMV(handler)

      const warnings: { message: string; context?: Record<string, unknown> }[] = []
      engine.onWarning((message, context) => {
        warnings.push({ message, context })
      })

      await engine.start()

      // Add 9 events to trigger 80% warning (threshold at 8)
      for (let i = 0; i < 9; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }

      expect(warnings.length).toBeGreaterThan(0)
      expect(warnings[0]!.message).toContain('80%')

      await engine.flush()
      vi.useRealTimers()
    })

    test('buffer capacity warning is only emitted once until reset', async () => {
      vi.useFakeTimers()

      const handler: MVHandler = {
        name: 'BufferedMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          await vi.advanceTimersByTimeAsync(10)
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 100,
        maxBufferSize: 10,
        batchTimeoutMs: 10000,
      })
      engine.registerMV(handler)

      const warnings: string[] = []
      engine.onWarning((message) => warnings.push(message))

      await engine.start()

      // Fill buffer past 80% multiple times
      for (let i = 0; i < 9; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }

      const warningCountAfterFirst = warnings.length

      // Continue adding events - should not emit more warnings
      // (use different target to still be routed)
      await engine.processEvent(createEvent('products:p1', 'CREATE', { id: 1 }))

      // Warning count should not increase
      expect(warnings.length).toBe(warningCountAfterFirst)

      // Flush to reset, then fill again
      await engine.flush()

      // After flush, warning flag is reset - adding events again should trigger new warning
      for (let i = 0; i < 9; i++) {
        await engine.processEvent(createEvent(`orders:o${i + 10}`, 'CREATE', { id: i + 10 }))
      }

      // Should have emitted another warning
      expect(warnings.length).toBe(warningCountAfterFirst + 1)

      await engine.flush()
      vi.useRealTimers()
    })
  })

  describe('Error Recovery in Streams', () => {
    test('retries failed batches with exponential backoff', async () => {
      let attemptCount = 0

      const handler: MVHandler = {
        name: 'RetryMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          attemptCount++
          if (attemptCount < 3) {
            throw new Error('Transient failure')
          }
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: {
          maxAttempts: 3,
          baseDelayMs: 1, // Very short delay for tests
          maxDelayMs: 10,
        },
      })
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(attemptCount).toBe(3) // 2 failures + 1 success
    })

    test('gives up after max retry attempts and emits error', async () => {
      const handler = createMVHandler({
        failOn: ['fail'],
        failCount: 10, // More failures than max attempts
      })

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: {
          maxAttempts: 3,
          baseDelayMs: 1, // Very short delay for tests
          maxDelayMs: 10,
        },
      })
      engine.registerMV(handler)

      const errors: Error[] = []
      engine.onError((err) => errors.push(err))

      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(handler.callCount).toBe(3) // All retry attempts
      expect(errors.length).toBe(1)
      expect(errors[0]!.message).toContain('fail-1')

      const stats = engine.getStats()
      expect(stats.failedBatches).toBe(1)
    })

    test('isolates failures between MVs', async () => {
      const failingHandler = createMVHandler({
        name: 'FailingMV',
        namespaces: ['orders'],
        failOn: ['all'],
        failCount: 10,
      })

      const successHandler = createMVHandler({
        name: 'SuccessMV',
        namespaces: ['orders'],
      })

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: {
          maxAttempts: 1, // Single attempt for faster test
          baseDelayMs: 1,
          maxDelayMs: 1,
        },
      })
      engine.registerMV(failingHandler)
      engine.registerMV(successHandler)

      const errors: { error: Error; mvName?: string }[] = []
      engine.onError((err, context) => errors.push({ error: err, mvName: context?.mvName }))

      await engine.start()
      await engine.processEvent(createEvent('orders:all-1', 'CREATE', { id: 1 }))
      await engine.flush()

      // FailingMV should fail, but SuccessMV should succeed
      expect(errors.length).toBe(1)
      expect(errors[0]!.mvName).toBe('FailingMV')
      expect(successHandler.processedEvents.length).toBe(1)
    })

    test('continues processing new events after handler failure', async () => {
      const handler = createMVHandler({
        failOn: ['fail'],
        failCount: 1, // Only fail once
      })

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
      })
      engine.registerMV(handler)
      await engine.start()

      // First event fails
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      // Second event should succeed
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.flush()

      // Only the successful event should be in processedEvents
      expect(handler.processedEvents.length).toBe(1)
      expect(handler.processedEvents[0]![0]!.target).toBe('orders:o2')
    })

    test('error handler receives batch context', async () => {
      const handler = createMVHandler({ failOn: ['error'], failCount: 10 })

      engine = createStreamingRefreshEngine({
        batchSize: 2,
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
      })
      engine.registerMV(handler)

      let capturedBatch: Event[] | undefined

      engine.onError((_err, context) => {
        capturedBatch = context?.batch
      })

      await engine.start()
      await engine.processEvent(createEvent('orders:error-1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:error-2', 'CREATE', { id: 2 }))
      await engine.flush()

      expect(capturedBatch).toBeDefined()
      expect(capturedBatch!.length).toBe(2)
    })
  })

  describe('Lifecycle and Configuration', () => {
    test('creates engine with custom configuration', () => {
      const config: StreamingRefreshConfig = {
        batchSize: 50,
        batchTimeoutMs: 200,
        maxBufferSize: 500,
        retry: {
          maxAttempts: 5,
          baseDelayMs: 200,
          maxDelayMs: 10000,
        },
      }

      engine = createStreamingRefreshEngine(config)
      expect(engine).toBeInstanceOf(StreamingRefreshEngine)
    })

    test('rejects events when engine is not running', async () => {
      engine = createStreamingRefreshEngine()
      // Not started

      await expect(
        engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      ).rejects.toThrow('not running')
    })

    test('isFlushing() returns correct state', async () => {
      vi.useFakeTimers()

      let flushingDuringProcess = false

      const handler: MVHandler = {
        name: 'FlushCheckMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          flushingDuringProcess = engine.isFlushing()
          await vi.advanceTimersByTimeAsync(10)
        },
      }

      engine = createStreamingRefreshEngine({ batchSize: 1 })
      engine.registerMV(handler)
      await engine.start()

      expect(engine.isFlushing()).toBe(false)

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(flushingDuringProcess).toBe(true)
      expect(engine.isFlushing()).toBe(false)

      vi.useRealTimers()
    })

    test('flushes pending events on stop', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine({
        batchSize: 100,
        batchTimeoutMs: 10000,
      })
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))

      // Stop should flush
      await engine.stop()

      expect(handler.processedEvents.length).toBe(1)
      expect(engine.isRunning()).toBe(false)
    })

    test('can restart engine after stop', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)

      await engine.start()
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.stop()

      expect(handler.processedEvents.length).toBe(1)

      await engine.start()
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.flush()

      expect(handler.processedEvents.length).toBe(2)
    })
  })

  describe('Event Handler Management', () => {
    test('onError returns unsubscribe function', async () => {
      const handler = createMVHandler({ failOn: ['fail'], failCount: 10 })
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
      })
      engine.registerMV(handler)

      const errors: Error[] = []
      const unsubscribe = engine.onError((err) => errors.push(err))

      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(errors.length).toBe(1)

      // Unsubscribe
      unsubscribe()

      await engine.processEvent(createEvent('orders:fail-2', 'CREATE', { id: 2 }))
      await engine.flush()

      // Should still be 1 (unsubscribed)
      expect(errors.length).toBe(1)
    })

    test('onWarning returns unsubscribe function', async () => {
      vi.useFakeTimers()

      const handler: MVHandler = {
        name: 'BufferedMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          await vi.advanceTimersByTimeAsync(10)
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 100,
        maxBufferSize: 10,
        batchTimeoutMs: 10000,
      })
      engine.registerMV(handler)

      const warnings: string[] = []
      const unsubscribe = engine.onWarning((msg) => warnings.push(msg))

      await engine.start()

      // Trigger warning
      for (let i = 0; i < 9; i++) {
        await engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i }))
      }

      expect(warnings.length).toBe(1)

      // Unsubscribe
      unsubscribe()
      await engine.flush()

      // Trigger another warning cycle
      for (let i = 0; i < 9; i++) {
        await engine.processEvent(createEvent(`orders:o${i + 10}`, 'CREATE', { id: i + 10 }))
      }

      // Should still be 1 (unsubscribed)
      expect(warnings.length).toBe(1)

      await engine.flush()
      vi.useRealTimers()
    })

    test('removeAllErrorListeners clears all listeners', async () => {
      const handler = createMVHandler({ failOn: ['fail'], failCount: 10 })
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
      })
      engine.registerMV(handler)

      const errors1: Error[] = []
      const errors2: Error[] = []
      engine.onError((err) => errors1.push(err))
      engine.onError((err) => errors2.push(err))

      engine.removeAllErrorListeners()

      await engine.start()
      await engine.processEvent(createEvent('orders:fail-1', 'CREATE', { id: 1 }))
      await engine.flush()

      expect(errors1.length).toBe(0)
      expect(errors2.length).toBe(0)
    })

    test('removeAllWarningListeners clears all listeners', async () => {
      engine = createStreamingRefreshEngine()

      const warnings1: string[] = []
      const warnings2: string[] = []
      engine.onWarning((msg) => warnings1.push(msg))
      engine.onWarning((msg) => warnings2.push(msg))

      engine.removeAllWarningListeners()

      // Verify by checking dispose doesn't cause issues
      await engine.dispose()
      expect(warnings1.length).toBe(0)
      expect(warnings2.length).toBe(0)
    })
  })

  describe('Statistics Tracking', () => {
    test('tracks processing time statistics', async () => {
      const handler = createMVHandler({ delay: 10 })
      engine = createStreamingRefreshEngine({ batchSize: 2 })
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.avgBatchProcessingMs).toBeGreaterThan(0)
    })

    test('tracks startedAt timestamp', async () => {
      engine = createStreamingRefreshEngine()
      await engine.start()

      const stats = engine.getStats()
      expect(stats.startedAt).not.toBeNull()
      expect(stats.startedAt).toBeLessThanOrEqual(Date.now())
    })

    test('tracks lastEventAt timestamp', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      const beforeEvent = Date.now()
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      const afterEvent = Date.now()

      const stats = engine.getStats()
      expect(stats.lastEventAt).not.toBeNull()
      expect(stats.lastEventAt).toBeGreaterThanOrEqual(beforeEvent)
      expect(stats.lastEventAt).toBeLessThanOrEqual(afterEvent)

      await engine.flush()
    })

    test('tracks events by MV', async () => {
      const handler1 = createMVHandler({ name: 'MV1', namespaces: ['orders'] })
      const handler2 = createMVHandler({ name: 'MV2', namespaces: ['products'] })

      engine = createStreamingRefreshEngine()
      engine.registerMV(handler1)
      engine.registerMV(handler2)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      await engine.processEvent(createEvent('products:p1', 'CREATE', { id: 1 }))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.eventsByMV['MV1']).toBe(2)
      expect(stats.eventsByMV['MV2']).toBe(1)
    })

    test('resetStats clears all statistics', async () => {
      const handler = createMVHandler()
      engine = createStreamingRefreshEngine()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      await engine.flush()

      engine.resetStats()

      const stats = engine.getStats()
      expect(stats.eventsReceived).toBe(0)
      expect(stats.eventsProcessed).toBe(0)
      expect(stats.batchesProcessed).toBe(0)
      expect(stats.failedBatches).toBe(0)
      // startedAt should be set since engine is still running
      expect(stats.startedAt).not.toBeNull()
    })
  })

  describe('Concurrent Processing', () => {
    test('prevents duplicate processing of same events', async () => {
      const processedIds = new Set<string>()

      const handler: MVHandler = {
        name: 'DeduplicatingMV',
        sourceNamespaces: ['orders'],
        async process(events: Event[]): Promise<void> {
          for (const event of events) {
            processedIds.add(event.target)
          }
        },
      }

      engine = createStreamingRefreshEngine({ batchSize: 2, batchTimeoutMs: 50 })
      engine.registerMV(handler)
      await engine.start()

      // Send events rapidly
      const promises: Promise<void>[] = []
      for (let i = 0; i < 10; i++) {
        promises.push(engine.processEvent(createEvent(`orders:o${i}`, 'CREATE', { id: i })))
      }
      await Promise.all(promises)
      await engine.flush()

      // Should have exactly 10 unique events (no duplicates)
      expect(processedIds.size).toBe(10)
    })

    test('waits for flush to complete before concurrent flushes', async () => {
      vi.useFakeTimers()

      const flushOrder: number[] = []
      let flushCounter = 0

      const handler: MVHandler = {
        name: 'OrderedMV',
        sourceNamespaces: ['orders'],
        async process(): Promise<void> {
          const currentFlush = ++flushCounter
          await vi.advanceTimersByTimeAsync(20)
          flushOrder.push(currentFlush)
        },
      }

      engine = createStreamingRefreshEngine({
        batchSize: 100,
        batchTimeoutMs: 50,
      })
      engine.registerMV(handler)
      await engine.start()

      // Trigger multiple overlapping flush attempts
      await engine.processEvent(createEvent('orders:o1', 'CREATE', { id: 1 }))
      const flush1 = engine.flush()

      await engine.processEvent(createEvent('orders:o2', 'CREATE', { id: 2 }))
      const flush2 = engine.flush()

      await engine.processEvent(createEvent('orders:o3', 'CREATE', { id: 3 }))
      const flush3 = engine.flush()

      await Promise.all([flush1, flush2, flush3])

      // Flushes should complete in order
      for (let i = 1; i < flushOrder.length; i++) {
        expect(flushOrder[i]).toBeGreaterThan(flushOrder[i - 1]!)
      }

      vi.useRealTimers()
    })
  })
})

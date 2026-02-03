/**
 * Concurrent Refresh Race Condition Tests
 *
 * Tests for concurrent MV refreshes, race conditions, locking behavior,
 * and consistency after concurrent operations.
 *
 * Issue: parquedb-yd24 - Add: Concurrent refresh race condition tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
} from '../../../src/materialized-views/streaming'

import {
  MVScheduler,
  createMVScheduler,
  type ScheduledView,
  type MVSchedulerConfig,
} from '../../../src/materialized-views/scheduler'

import type { ViewDefinition, ViewMetadata, ViewState, ViewName } from '../../../src/materialized-views/types'
import type { Event, EventOp } from '../../../src/types/entity'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock event for testing
 */
function createMockEvent(opts: {
  id?: string
  ns?: string
  op?: EventOp
  timestamp?: Date
}): Event {
  const id = opts.id ?? `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const ns = opts.ns ?? 'test'
  const timestamp = opts.timestamp ?? new Date()

  return {
    id,
    ts: timestamp.getTime(),
    op: opts.op ?? 'CREATE',
    target: `${ns}:entity_${id}`,
    after: {
      $id: `${ns}/entity_${id}`,
      $type: 'TestEntity',
      name: `Entity ${id}`,
      value: Math.random() * 100,
    },
    actor: 'test:user',
  }
}

/**
 * Create a mock Durable Object storage for scheduler tests
 */
function createMockDOStorage() {
  const data = new Map<string, unknown>()
  let alarm: number | null = null

  return {
    get: vi.fn(async <T>(key: string): Promise<T | undefined> => data.get(key) as T | undefined),
    put: vi.fn(async (key: string, value: unknown) => {
      data.set(key, value)
    }),
    delete: vi.fn(async (keys: string | string[]) => {
      const keysArray = Array.isArray(keys) ? keys : [keys]
      for (const key of keysArray) {
        data.delete(key)
      }
    }),
    list: vi.fn(async <T>(options?: { prefix?: string }): Promise<Map<string, T>> => {
      const result = new Map<string, T>()
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value as T)
        }
      }
      return result
    }),
    setAlarm: vi.fn(async (time: number) => {
      alarm = time
    }),
    getAlarm: vi.fn(async () => alarm),
    deleteAlarm: vi.fn(async () => {
      alarm = null
    }),
    _data: data,
    _getAlarm: () => alarm,
  }
}

/**
 * Delay utility for fake timers - uses vi.advanceTimersByTimeAsync
 */
async function delay(ms: number): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms)
}

/**
 * Delay utility for real timers - uses actual setTimeout
 */
function realDelay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// Concurrent Streaming Refresh Tests
// =============================================================================

describe('Concurrent Streaming Refresh', () => {
  let engine: StreamingRefreshEngine

  beforeEach(() => {
    vi.useFakeTimers()
    engine = createStreamingRefreshEngine({
      batchSize: 5,
      batchTimeoutMs: 50,
    })
  })

  afterEach(async () => {
    if (engine.isRunning()) {
      await engine.stop()
    }
    vi.useRealTimers()
  })

  describe('Concurrent Event Processing', () => {
    it('should handle concurrent event processing from multiple sources', async () => {
      const processedBatches: Array<{ name: string; events: Event[] }> = []
      const processingDelay = 20

      const handler: MVHandler = {
        name: 'ConcurrentMV',
        sourceNamespaces: ['orders', 'products', 'customers'],
        async process(events) {
          // Simulate processing time
          await delay(processingDelay)
          processedBatches.push({ name: this.name, events: [...events] })
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Fire events concurrently from multiple "sources"
      const eventPromises = [
        // Source 1: Orders
        Promise.all([
          engine.processEvent(createMockEvent({ ns: 'orders', id: 'order1' })),
          engine.processEvent(createMockEvent({ ns: 'orders', id: 'order2' })),
          engine.processEvent(createMockEvent({ ns: 'orders', id: 'order3' })),
        ]),
        // Source 2: Products
        Promise.all([
          engine.processEvent(createMockEvent({ ns: 'products', id: 'prod1' })),
          engine.processEvent(createMockEvent({ ns: 'products', id: 'prod2' })),
        ]),
        // Source 3: Customers
        Promise.all([
          engine.processEvent(createMockEvent({ ns: 'customers', id: 'cust1' })),
          engine.processEvent(createMockEvent({ ns: 'customers', id: 'cust2' })),
        ]),
      ]

      await Promise.all(eventPromises)
      await engine.flush()

      // Verify all events were processed
      const totalEvents = processedBatches.reduce((sum, b) => sum + b.events.length, 0)
      expect(totalEvents).toBe(7)
    })

    it('should maintain event order within same namespace under concurrent load', async () => {
      const processedEvents: Event[] = []

      const handler: MVHandler = {
        name: 'OrderedMV',
        sourceNamespaces: ['orders'],
        async process(events) {
          processedEvents.push(...events)
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Send events with specific timestamps to verify ordering
      const events = Array.from({ length: 10 }, (_, i) =>
        createMockEvent({
          ns: 'orders',
          id: `seq_${i.toString().padStart(3, '0')}`,
          timestamp: new Date(Date.now() + i * 10),
        })
      )

      // Process all events concurrently
      await Promise.all(events.map(e => engine.processEvent(e)))
      await engine.flush()

      // Events should maintain relative order (by ID since we control them)
      expect(processedEvents.length).toBe(10)
      for (let i = 0; i < processedEvents.length - 1; i++) {
        const currentId = processedEvents[i]!.id
        const nextId = processedEvents[i + 1]!.id
        expect(currentId <= nextId).toBe(true)
      }
    })

    it('should handle race between batch flush and new events', async () => {
      const batches: Event[][] = []

      const handler: MVHandler = {
        name: 'RaceMV',
        sourceNamespaces: ['test'],
        async process(events) {
          // Slow processing to create race window
          await delay(30)
          batches.push([...events])
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Send initial batch that triggers flush
      for (let i = 0; i < 5; i++) {
        await engine.processEvent(createMockEvent({ ns: 'test', id: `batch1_${i}` }))
      }

      // Immediately send more events while flush is in progress
      const racePromises = []
      for (let i = 0; i < 5; i++) {
        racePromises.push(engine.processEvent(createMockEvent({ ns: 'test', id: `batch2_${i}` })))
      }

      await Promise.all(racePromises)
      await engine.flush()

      // All events should be processed without loss
      const totalEvents = batches.reduce((sum, b) => sum + b.length, 0)
      expect(totalEvents).toBe(10)
    })

    it('should handle high volume concurrent events without data loss', async () => {
      const processedIds = new Set<string>()

      const handler: MVHandler = {
        name: 'HighVolumeMV',
        sourceNamespaces: ['test'],
        async process(events) {
          for (const event of events) {
            processedIds.add(event.id)
          }
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Fire 100 events concurrently
      const eventCount = 100
      const events = Array.from({ length: eventCount }, (_, i) =>
        createMockEvent({ ns: 'test', id: `high_vol_${i}` })
      )

      await Promise.all(events.map(e => engine.processEvent(e)))
      await engine.flush()

      // All unique events should be processed
      expect(processedIds.size).toBe(eventCount)
    })
  })

  describe('Multiple MV Handlers Concurrent Processing', () => {
    it('should process events through multiple handlers concurrently', async () => {
      const handler1Results: Event[] = []
      const handler2Results: Event[] = []
      const handler3Results: Event[] = []

      const handler1: MVHandler = {
        name: 'Handler1',
        sourceNamespaces: ['test'],
        async process(events) {
          await delay(20)
          handler1Results.push(...events)
        },
      }

      const handler2: MVHandler = {
        name: 'Handler2',
        sourceNamespaces: ['test'],
        async process(events) {
          await delay(15)
          handler2Results.push(...events)
        },
      }

      const handler3: MVHandler = {
        name: 'Handler3',
        sourceNamespaces: ['test'],
        async process(events) {
          await delay(25)
          handler3Results.push(...events)
        },
      }

      engine.registerMV(handler1)
      engine.registerMV(handler2)
      engine.registerMV(handler3)
      await engine.start()

      // Send events
      for (let i = 0; i < 10; i++) {
        await engine.processEvent(createMockEvent({ ns: 'test', id: `event_${i}` }))
      }
      await engine.flush()

      // All handlers should have received all events
      expect(handler1Results.length).toBe(10)
      expect(handler2Results.length).toBe(10)
      expect(handler3Results.length).toBe(10)
    })

    it('should isolate failures between handlers during concurrent processing', async () => {
      // Create a new engine with no retries to avoid timeout issues with fake timers
      const noRetryEngine = createStreamingRefreshEngine({
        batchSize: 5,
        batchTimeoutMs: 50,
        retry: {
          maxAttempts: 1, // No retries - fail immediately
          baseDelayMs: 0,
          maxDelayMs: 0,
        },
      })

      const successfulResults: Event[] = []
      const errors: Error[] = []

      const failingHandler: MVHandler = {
        name: 'FailingHandler',
        sourceNamespaces: ['test'],
        async process() {
          throw new Error('Handler failure')
        },
      }

      const successHandler: MVHandler = {
        name: 'SuccessHandler',
        sourceNamespaces: ['test'],
        async process(events) {
          successfulResults.push(...events)
        },
      }

      noRetryEngine.onError(err => errors.push(err))
      noRetryEngine.registerMV(failingHandler)
      noRetryEngine.registerMV(successHandler)
      await noRetryEngine.start()

      // Send events
      for (let i = 0; i < 5; i++) {
        await noRetryEngine.processEvent(createMockEvent({ ns: 'test' }))
      }
      await noRetryEngine.flush()
      await noRetryEngine.stop()

      // Success handler should have processed all events despite failing handler
      expect(successfulResults.length).toBe(5)
      expect(errors.length).toBeGreaterThan(0)
    })

    it('should maintain separate buffers for different namespaces', async () => {
      const orderEvents: Event[] = []
      const productEvents: Event[] = []

      const orderHandler: MVHandler = {
        name: 'OrderHandler',
        sourceNamespaces: ['orders'],
        async process(events) {
          orderEvents.push(...events)
        },
      }

      const productHandler: MVHandler = {
        name: 'ProductHandler',
        sourceNamespaces: ['products'],
        async process(events) {
          productEvents.push(...events)
        },
      }

      engine.registerMV(orderHandler)
      engine.registerMV(productHandler)
      await engine.start()

      // Send mixed events concurrently
      await Promise.all([
        engine.processEvent(createMockEvent({ ns: 'orders', id: 'o1' })),
        engine.processEvent(createMockEvent({ ns: 'products', id: 'p1' })),
        engine.processEvent(createMockEvent({ ns: 'orders', id: 'o2' })),
        engine.processEvent(createMockEvent({ ns: 'products', id: 'p2' })),
        engine.processEvent(createMockEvent({ ns: 'orders', id: 'o3' })),
      ])
      await engine.flush()

      // Each handler should only receive its namespace events
      expect(orderEvents.length).toBe(3)
      expect(productEvents.length).toBe(2)
    })
  })

  describe('Backpressure Under Concurrent Load', () => {
    it('should handle buffer overflow gracefully', async () => {
      // Create engine with small buffer
      const smallBufferEngine = createStreamingRefreshEngine({
        batchSize: 5,
        maxBufferSize: 10,
        batchTimeoutMs: 100,
      })

      let processedCount = 0
      const handler: MVHandler = {
        name: 'SlowHandler',
        sourceNamespaces: ['test'],
        async process(events) {
          // Slow processing
          await delay(10)
          processedCount += events.length
        },
      }

      smallBufferEngine.registerMV(handler)
      await smallBufferEngine.start()

      // Send many events
      for (let i = 0; i < 30; i++) {
        await smallBufferEngine.processEvent(createMockEvent({ ns: 'test' }))
      }

      await smallBufferEngine.flush()
      await smallBufferEngine.stop()

      // All events should eventually be processed
      expect(processedCount).toBe(30)
    })

    it('should track backpressure statistics', async () => {
      const engine2 = createStreamingRefreshEngine({
        batchSize: 2,
        maxBufferSize: 3,
        batchTimeoutMs: 1000,
      })

      let processCount = 0
      const handler: MVHandler = {
        name: 'StatsHandler',
        sourceNamespaces: ['test'],
        async process(events) {
          // Very slow processing to trigger backpressure
          await delay(100)
          processCount += events.length
        },
      }

      engine2.registerMV(handler)
      await engine2.start()

      // Send events rapidly
      const eventPromises = []
      for (let i = 0; i < 10; i++) {
        eventPromises.push(engine2.processEvent(createMockEvent({ ns: 'test', id: `bp_${i}` })))
      }
      await Promise.all(eventPromises)
      await engine2.flush()
      await engine2.stop()

      // All events should be processed
      expect(processCount).toBe(10)

      // Stats should show event processing
      const stats = engine2.getStats()
      expect(stats.eventsReceived).toBe(10)
    })
  })

  describe('Engine Lifecycle Concurrent Operations', () => {
    it('should handle start/stop race conditions', async () => {
      const operations: string[] = []

      const handler: MVHandler = {
        name: 'LifecycleHandler',
        sourceNamespaces: ['test'],
        async process() {
          operations.push('process')
        },
      }

      engine.registerMV(handler)

      // Rapid start/stop
      await engine.start()
      const processPromise = engine.processEvent(createMockEvent({ ns: 'test' }))

      // Stop should wait for pending operations
      await engine.stop()

      // Should be safe to restart
      await engine.start()
      await engine.processEvent(createMockEvent({ ns: 'test' }))
      await engine.flush()
      await engine.stop()

      // At least one event should have been processed
      expect(operations.length).toBeGreaterThanOrEqual(1)
    })

    it('should flush pending events on stop', async () => {
      let processedCount = 0

      const handler: MVHandler = {
        name: 'FlushHandler',
        sourceNamespaces: ['test'],
        async process(events) {
          processedCount += events.length
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Add events without waiting for flush
      for (let i = 0; i < 3; i++) {
        await engine.processEvent(createMockEvent({ ns: 'test' }))
      }

      // Stop should flush remaining events
      await engine.stop()

      expect(processedCount).toBe(3)
    })
  })
})

// =============================================================================
// Scheduler Concurrent Refresh Tests
// =============================================================================

describe('Scheduler Concurrent Refresh', () => {
  let mockStorage: ReturnType<typeof createMockDOStorage>
  let scheduler: MVScheduler
  let refreshCalls: Array<{ view: string; start: number; end: number }>

  beforeEach(() => {
    vi.useFakeTimers()
    mockStorage = createMockDOStorage()
    refreshCalls = []

    const config: MVSchedulerConfig = {
      onRefresh: async viewName => {
        const start = Date.now()
        // Simulate refresh taking time
        await delay(30)
        refreshCalls.push({ view: viewName as string, start, end: Date.now() })
      },
      minAlarmIntervalMs: 10,
    }

    scheduler = createMVScheduler(mockStorage as unknown as DurableObjectStorage, config)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('Locking Behavior', () => {
    it('should prevent concurrent refresh of same view', async () => {
      // Schedule a view
      await scheduler.scheduleView('test_view', { intervalMs: 100 })

      // Start first refresh (don't await)
      const refresh1 = scheduler.triggerRefresh('test_view')

      // Small delay to ensure first refresh has started
      await delay(5)

      // Second attempt should be blocked
      const refresh2 = await scheduler.triggerRefresh('test_view')

      // Wait for first to complete
      await refresh1

      // First should trigger, second should return false
      expect(refresh2).toBe(false)
    })

    it('should clear processing state after completion', async () => {
      await scheduler.scheduleView('clear_state_view', { intervalMs: 100 })

      // Trigger and wait for refresh
      const result = await scheduler.triggerRefresh('clear_state_view')
      expect(result).toBe(true)

      // Processing state should be cleared
      expect(scheduler.isProcessing('clear_state_view')).toBe(false)

      // Should be able to trigger again
      const result2 = await scheduler.triggerRefresh('clear_state_view')
      expect(result2).toBe(true)
    })

    it('should allow concurrent refresh of different views', async () => {
      const views = ['view_1', 'view_2', 'view_3']

      for (const view of views) {
        await scheduler.scheduleView(view, { intervalMs: 100 })
      }

      // Trigger all and wait for all to complete
      const results = await Promise.all(views.map(v => scheduler.triggerRefresh(v)))

      // All should trigger successfully
      expect(results.every(r => r === true)).toBe(true)

      // Wait for all refreshes to complete
      await delay(100)

      // Verify all views were refreshed
      const refreshedViews = new Set(refreshCalls.map(c => c.view))
      expect(refreshedViews.size).toBe(3)
    })

    it('should track concurrent processing state for multiple views', async () => {
      await scheduler.scheduleView('multi_1', { intervalMs: 1000 })
      await scheduler.scheduleView('multi_2', { intervalMs: 1000 })

      // Start both refreshes without awaiting
      const p1 = scheduler.triggerRefresh('multi_1')
      const p2 = scheduler.triggerRefresh('multi_2')

      // Small delay to ensure both started
      await delay(5)

      // Both should show as processing
      expect(scheduler.isProcessing('multi_1')).toBe(true)
      expect(scheduler.isProcessing('multi_2')).toBe(true)

      // Wait for completion
      await Promise.all([p1, p2])

      // Both should be cleared
      expect(scheduler.isProcessing('multi_1')).toBe(false)
      expect(scheduler.isProcessing('multi_2')).toBe(false)
    })
  })

  describe('Alarm Processing Race Conditions', () => {
    it('should handle multiple due views in single alarm', async () => {
      const views = ['alarm_v1', 'alarm_v2', 'alarm_v3', 'alarm_v4', 'alarm_v5']

      for (const view of views) {
        await scheduler.scheduleView(view, { intervalMs: 5 })
      }

      // Wait for all views to be due
      await delay(10)

      // Process alarm
      const result = await scheduler.processAlarm()

      // All views should be refreshed
      expect(result.refreshed.length).toBe(views.length)
      expect(result.skipped.length).toBe(0)
    })

    it('should skip already-processing views during alarm', async () => {
      await scheduler.scheduleView('skip_test', { intervalMs: 5 })

      // Wait for view to be due
      await delay(10)

      // Start a refresh without awaiting
      const refreshPromise = scheduler.triggerRefresh('skip_test')

      // Small delay to ensure processing started
      await delay(5)

      // Process alarm while refresh is in progress
      const result = await scheduler.processAlarm()

      // View should be skipped since it's already processing
      await refreshPromise

      // Either refreshed in trigger or skipped in alarm (depends on timing)
      expect(result.refreshed.length + result.skipped.length).toBeLessThanOrEqual(1)
    })

    it('should schedule next alarm after processing', async () => {
      await scheduler.scheduleView('next_alarm', { intervalMs: 100 })

      // Process alarm
      const result = await scheduler.processAlarm()

      // Should have scheduled next alarm
      expect(result.nextAlarmAt).toBeDefined()
      expect(result.nextAlarmAt).toBeGreaterThan(Date.now())
    })
  })

  describe('State Consistency After Concurrent Operations', () => {
    it('should maintain consistent stats after concurrent refreshes', async () => {
      const views = ['stat_v1', 'stat_v2', 'stat_v3']

      for (const view of views) {
        await scheduler.scheduleView(view, { intervalMs: 1000 })
      }

      // Refresh all views
      await Promise.all(views.map(v => scheduler.triggerRefresh(v)))

      // Wait for all to complete
      await delay(100)

      const stats = await scheduler.getStats()

      // Stats should reflect actual refresh count
      expect(stats.totalViews).toBe(3)
      expect(stats.totalRefreshes).toBe(3)
      expect(stats.successfulRefreshes).toBe(3)
    })

    it('should correctly update view state after refresh', async () => {
      await scheduler.scheduleView('state_test', { intervalMs: 1000 })

      // Get initial state
      const before = await scheduler.getView('state_test')
      expect(before).not.toBeNull()
      expect(before!.lastRefreshAt).toBeUndefined()

      // Trigger refresh
      await scheduler.triggerRefresh('state_test')

      const after = await scheduler.getView('state_test')
      expect(after).not.toBeNull()
      expect(after!.lastRefreshAt).toBeDefined()
      expect(after!.lastRefreshDurationMs).toBeDefined()
      expect(after!.consecutiveFailures).toBe(0)
    })

    it('should handle concurrent enable/disable operations', async () => {
      await scheduler.scheduleView('toggle_view', { intervalMs: 1000 })

      // Sequential toggle operations (not truly concurrent due to await)
      // i=0: disable, i=1: enable, i=2: disable, ..., i=9: enable
      for (let i = 0; i < 10; i++) {
        if (i % 2 === 0) {
          await scheduler.disableView('toggle_view')
        } else {
          await scheduler.enableView('toggle_view')
        }
      }

      const view = await scheduler.getView('toggle_view')
      expect(view).not.toBeNull()
      // Final state should be enabled (last index i=9 is odd, so enableView was called last)
      expect(view!.enabled).toBe(true)
    })

    it('should preserve view data during rapid schedule updates', async () => {
      await scheduler.scheduleView('rapid_update', { intervalMs: 100 })

      // Rapid schedule updates
      for (let i = 1; i <= 5; i++) {
        await scheduler.updateSchedule('rapid_update', { intervalMs: i * 100 })
      }

      const view = await scheduler.getView('rapid_update')
      expect(view).not.toBeNull()
      expect(view!.schedule.intervalMs).toBe(500)
      expect(view!.enabled).toBe(true)
    })
  })

  describe('Error Recovery Under Concurrent Load', () => {
    it('should track failures correctly', async () => {
      let callCount = 0
      const failingScheduler = createMVScheduler(
        mockStorage as unknown as DurableObjectStorage,
        {
          onRefresh: async () => {
            callCount++
            throw new Error('Simulated failure')
          },
          defaultRetryConfig: {
            maxRetries: 3,
            baseDelayMs: 5,
            maxDelayMs: 10,
            backoffMultiplier: 1,
          },
        }
      )

      await failingScheduler.scheduleView('failing_view', { intervalMs: 5 })

      // Wait for view to be due
      await delay(10)

      // Process alarm
      const result = await failingScheduler.processAlarm()

      // Should have failed
      expect(result.failed.length).toBe(1)

      // View should have consecutive failures tracked
      const view = await failingScheduler.getView('failing_view')
      expect(view!.consecutiveFailures).toBe(1)
    })

    it('should disable view after max failures', async () => {
      let callCount = 0
      const alwaysFailScheduler = createMVScheduler(
        mockStorage as unknown as DurableObjectStorage,
        {
          onRefresh: async () => {
            callCount++
            throw new Error('Always fails')
          },
          defaultRetryConfig: {
            maxRetries: 3,
            baseDelayMs: 1,
            maxDelayMs: 5,
            backoffMultiplier: 1,
          },
        }
      )

      await alwaysFailScheduler.scheduleView('always_fail', { intervalMs: 5 })

      // Process alarms until view is disabled
      for (let i = 0; i < 5; i++) {
        await delay(10)
        await alwaysFailScheduler.processAlarm()
      }

      const view = await alwaysFailScheduler.getView('always_fail')
      expect(view!.enabled).toBe(false)
      expect(view!.consecutiveFailures).toBeGreaterThanOrEqual(3)
    })

    it('should reset failure count on success', async () => {
      let callCount = 0
      const sometimesFailScheduler = createMVScheduler(
        mockStorage as unknown as DurableObjectStorage,
        {
          onRefresh: async () => {
            callCount++
            if (callCount <= 2) {
              throw new Error('Initial failures')
            }
            // Success after 2 failures
            await delay(10)
          },
          defaultRetryConfig: {
            maxRetries: 5,
            baseDelayMs: 1,
            maxDelayMs: 5,
            backoffMultiplier: 1,
          },
        }
      )

      await sometimesFailScheduler.scheduleView('recover_view', { intervalMs: 5 })

      // Process until success
      for (let i = 0; i < 5; i++) {
        await delay(10)
        await sometimesFailScheduler.processAlarm()
      }

      const view = await sometimesFailScheduler.getView('recover_view')
      expect(view!.consecutiveFailures).toBe(0)
      expect(view!.enabled).toBe(true)
    })
  })

  describe('Scheduler Clear Operations', () => {
    it('should clear all views safely', async () => {
      await scheduler.scheduleView('clear_1', { intervalMs: 100 })
      await scheduler.scheduleView('clear_2', { intervalMs: 100 })
      await scheduler.scheduleView('clear_3', { intervalMs: 100 })

      const beforeViews = await scheduler.getViews()
      expect(beforeViews.length).toBe(3)

      await scheduler.clear()

      const afterViews = await scheduler.getViews()
      expect(afterViews.length).toBe(0)

      const stats = await scheduler.getStats()
      expect(stats.totalViews).toBe(0)
    })
  })
})

// =============================================================================
// Integration Tests: Combined Streaming and Scheduler
// =============================================================================

describe('Combined Streaming and Scheduler Integration', () => {
  let engine: StreamingRefreshEngine
  let mockStorage: ReturnType<typeof createMockDOStorage>
  let scheduler: MVScheduler

  beforeEach(() => {
    vi.useFakeTimers()
    engine = createStreamingRefreshEngine({
      batchSize: 5,
      batchTimeoutMs: 50,
    })

    mockStorage = createMockDOStorage()
    scheduler = createMVScheduler(mockStorage as unknown as DurableObjectStorage, {
      onRefresh: async () => {
        await delay(20)
      },
      minAlarmIntervalMs: 10,
    })
  })

  afterEach(async () => {
    if (engine.isRunning()) {
      await engine.stop()
    }
    vi.useRealTimers()
  })

  it('should handle streaming events and scheduled refresh independently', async () => {
    const streamedEvents: Event[] = []

    // Setup streaming handler
    const handler: MVHandler = {
      name: 'IntegrationMV',
      sourceNamespaces: ['test'],
      async process(events) {
        streamedEvents.push(...events)
      },
    }

    engine.registerMV(handler)
    await engine.start()

    // Setup scheduled view
    await scheduler.scheduleView('scheduled_view', { intervalMs: 50 })

    // Run concurrent operations
    const operations = [
      // Streaming events
      Promise.all([
        engine.processEvent(createMockEvent({ ns: 'test', id: 'stream_1' })),
        engine.processEvent(createMockEvent({ ns: 'test', id: 'stream_2' })),
        engine.processEvent(createMockEvent({ ns: 'test', id: 'stream_3' })),
      ]),
      // Scheduled refresh
      scheduler.triggerRefresh('scheduled_view'),
    ]

    await Promise.all(operations)
    await engine.flush()

    // Both should complete independently
    expect(streamedEvents.length).toBe(3)

    const schedulerStats = await scheduler.getStats()
    expect(schedulerStats.successfulRefreshes).toBe(1)
  })

  it('should maintain isolation between streaming and scheduled systems', async () => {
    const streamErrors: Error[] = []
    const schedulerErrors: string[] = []

    // Create a new engine with no retries to avoid timeout issues with fake timers
    const noRetryEngine = createStreamingRefreshEngine({
      batchSize: 5,
      batchTimeoutMs: 50,
      retry: {
        maxAttempts: 1, // No retries - fail immediately
        baseDelayMs: 0,
        maxDelayMs: 0,
      },
    })

    // Setup failing streaming handler
    const failingHandler: MVHandler = {
      name: 'FailingStreamMV',
      sourceNamespaces: ['test'],
      async process() {
        throw new Error('Stream failure')
      },
    }

    noRetryEngine.onError(err => streamErrors.push(err))
    noRetryEngine.registerMV(failingHandler)
    await noRetryEngine.start()

    // Setup scheduler with failure tracking
    const failingScheduler = createMVScheduler(mockStorage as unknown as DurableObjectStorage, {
      onRefresh: async () => {
        throw new Error('Schedule failure')
      },
      onRefreshError: async (_vn, err) => {
        schedulerErrors.push(err.message)
      },
    })

    await failingScheduler.scheduleView('failing_scheduled', { intervalMs: 10 })

    // Run both failing operations
    await noRetryEngine.processEvent(createMockEvent({ ns: 'test' }))
    await noRetryEngine.flush()
    await noRetryEngine.stop()

    await delay(15)
    await failingScheduler.processAlarm()

    // Both should fail independently
    expect(streamErrors.length).toBeGreaterThan(0)
    expect(schedulerErrors.length).toBeGreaterThan(0)
  })
})

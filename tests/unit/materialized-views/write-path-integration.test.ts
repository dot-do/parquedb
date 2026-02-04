/**
 * Tests for Write Path Integration for Materialized Views
 *
 * Tests the integration between ParqueDB mutations and the MV streaming engine.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  MVEventEmitter,
  MVIntegrationBridge,
  MVEventSourceAdapter,
  createMVEventEmitter,
  createMVIntegrationBridge,
  createMVEventSourceAdapter,
  createMutationHook,
  createMVIntegration,
  type MVEventSubscriber,
} from '@/materialized-views/write-path-integration'
import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
} from '@/materialized-views/streaming'
import type { Event, EventOp } from '@/types/entity'
import type { Entity, EntityId } from '@/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEvent(
  op: EventOp,
  target: string,
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

function createTestEntity(id: string, data: Record<string, unknown> = {}): Entity {
  return {
    $id: id,
    $type: 'TestEntity',
    ...data,
    createdAt: new Date(),
    updatedAt: new Date(),
    version: 1,
  } as Entity
}

// =============================================================================
// MVEventEmitter Tests
// =============================================================================

describe('MVEventEmitter', () => {
  let emitter: MVEventEmitter

  beforeEach(() => {
    vi.useFakeTimers()
    emitter = createMVEventEmitter()
  })

  afterEach(() => {
    emitter.dispose()
    vi.useRealTimers()
  })

  describe('subscribe', () => {
    it('registers a subscriber', () => {
      const subscriber: MVEventSubscriber = vi.fn()
      emitter.subscribe(subscriber)

      expect(emitter.getSubscriberCount()).toBe(1)
      expect(emitter.hasSubscribers()).toBe(true)
    })

    it('returns an unsubscribe function', () => {
      const subscriber: MVEventSubscriber = vi.fn()
      const unsubscribe = emitter.subscribe(subscriber)

      expect(emitter.getSubscriberCount()).toBe(1)

      unsubscribe()

      expect(emitter.getSubscriberCount()).toBe(0)
      expect(emitter.hasSubscribers()).toBe(false)
    })

    it('supports multiple subscribers', () => {
      const subscriber1: MVEventSubscriber = vi.fn()
      const subscriber2: MVEventSubscriber = vi.fn()

      emitter.subscribe(subscriber1)
      emitter.subscribe(subscriber2)

      expect(emitter.getSubscriberCount()).toBe(2)
    })
  })

  describe('emit', () => {
    it('emits events to all subscribers', async () => {
      const subscriber1 = vi.fn()
      const subscriber2 = vi.fn()

      emitter.subscribe(subscriber1)
      emitter.subscribe(subscriber2)

      const event = createTestEvent('CREATE', 'orders:order-1', { total: 100 })
      await emitter.emit(event)
      await emitter.flush()

      expect(subscriber1).toHaveBeenCalledWith(event)
      expect(subscriber2).toHaveBeenCalledWith(event)
    })

    it('handles subscriber errors gracefully', async () => {
      const errorHandler = vi.fn()
      emitter = createMVEventEmitter({ onError: errorHandler })

      const failingSubscriber: MVEventSubscriber = () => {
        throw new Error('Subscriber error')
      }
      const workingSubscriber = vi.fn()

      emitter.subscribe(failingSubscriber)
      emitter.subscribe(workingSubscriber)

      const event = createTestEvent('CREATE', 'orders:order-1')
      await emitter.emit(event)
      await emitter.flush()

      // Error handler should be called
      expect(errorHandler).toHaveBeenCalled()

      // Working subscriber should still be called
      expect(workingSubscriber).toHaveBeenCalledWith(event)
    })

    it('tracks subscriber errors in stats', async () => {
      emitter = createMVEventEmitter({ onError: () => {} })

      emitter.subscribe(() => {
        throw new Error('Test error')
      })

      await emitter.emit(createTestEvent('CREATE', 'orders:order-1'))
      await emitter.flush()

      const stats = emitter.getStats()
      expect(stats.subscriberErrors).toBe(1)
    })
  })

  describe('emitEntityEvent', () => {
    it('creates and emits entity events', async () => {
      const subscriber = vi.fn()
      emitter.subscribe(subscriber)

      const before = createTestEntity('orders/order-1', { total: 100 })
      const after = createTestEntity('orders/order-1', { total: 150 })

      await emitter.emitEntityEvent(
        'UPDATE',
        'orders',
        'order-1',
        before,
        after,
        'users/user-1' as EntityId
      )
      await emitter.flush()

      expect(subscriber).toHaveBeenCalled()
      const emittedEvent = subscriber.mock.calls[0][0]
      expect(emittedEvent.op).toBe('UPDATE')
      expect(emittedEvent.target).toBe('orders:order-1')
      expect(emittedEvent.before).toEqual(before)
      expect(emittedEvent.after).toEqual(after)
    })

    it('emits CREATE events', async () => {
      const subscriber = vi.fn()
      emitter.subscribe(subscriber)

      const entity = createTestEntity('orders/order-1', { total: 100 })

      await emitter.emitEntityEvent('CREATE', 'orders', 'order-1', null, entity)
      await emitter.flush()

      const emittedEvent = subscriber.mock.calls[0][0]
      expect(emittedEvent.op).toBe('CREATE')
      expect(emittedEvent.before).toBeFalsy() // null or undefined for create events
      expect(emittedEvent.after).toEqual(entity)
    })

    it('emits DELETE events', async () => {
      const subscriber = vi.fn()
      emitter.subscribe(subscriber)

      const entity = createTestEntity('orders/order-1', { total: 100 })

      await emitter.emitEntityEvent('DELETE', 'orders', 'order-1', entity, null)
      await emitter.flush()

      const emittedEvent = subscriber.mock.calls[0][0]
      expect(emittedEvent.op).toBe('DELETE')
      expect(emittedEvent.before).toEqual(entity)
      expect(emittedEvent.after).toBeFalsy() // null or undefined for delete events
    })
  })

  describe('emitRelationshipEvent', () => {
    it('emits REL_CREATE events', async () => {
      const subscriber = vi.fn()
      emitter.subscribe(subscriber)

      await emitter.emitRelationshipEvent(
        'REL_CREATE',
        'orders',
        'order-1',
        'items',
        'products',
        'product-1'
      )
      await emitter.flush()

      const emittedEvent = subscriber.mock.calls[0][0]
      expect(emittedEvent.op).toBe('REL_CREATE')
      expect(emittedEvent.target).toBe('orders:order-1:items:products:product-1')
      expect(emittedEvent.after?.predicate).toBe('items')
    })

    it('emits REL_DELETE events', async () => {
      const subscriber = vi.fn()
      emitter.subscribe(subscriber)

      await emitter.emitRelationshipEvent(
        'REL_DELETE',
        'orders',
        'order-1',
        'items',
        'products',
        'product-1'
      )
      await emitter.flush()

      const emittedEvent = subscriber.mock.calls[0][0]
      expect(emittedEvent.op).toBe('REL_DELETE')
    })
  })

  describe('statistics', () => {
    it('tracks total events emitted', async () => {
      emitter.subscribe(() => {})

      await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
      await emitter.emit(createTestEvent('UPDATE', 'orders:o2'))
      await emitter.flush()

      const stats = emitter.getStats()
      expect(stats.totalEmitted).toBe(2)
    })

    it('tracks events by operation type', async () => {
      emitter.subscribe(() => {})

      await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
      await emitter.emit(createTestEvent('CREATE', 'orders:o2'))
      await emitter.emit(createTestEvent('UPDATE', 'orders:o1'))
      await emitter.emit(createTestEvent('DELETE', 'orders:o2'))
      await emitter.flush()

      const stats = emitter.getStats()
      expect(stats.eventsByOp.CREATE).toBe(2)
      expect(stats.eventsByOp.UPDATE).toBe(1)
      expect(stats.eventsByOp.DELETE).toBe(1)
    })

    it('tracks events by namespace', async () => {
      emitter.subscribe(() => {})

      await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
      await emitter.emit(createTestEvent('CREATE', 'orders:o2'))
      await emitter.emit(createTestEvent('CREATE', 'products:p1'))
      await emitter.flush()

      const stats = emitter.getStats()
      expect(stats.eventsByNamespace['orders']).toBe(2)
      expect(stats.eventsByNamespace['products']).toBe(1)
    })

    it('can reset statistics', async () => {
      emitter.subscribe(() => {})

      await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
      await emitter.flush()

      expect(emitter.getStats().totalEmitted).toBe(1)

      emitter.resetStats()

      expect(emitter.getStats().totalEmitted).toBe(0)
    })
  })

  describe('synchronous mode', () => {
    it('processes events synchronously when configured', async () => {
      emitter = createMVEventEmitter({ synchronous: true })
      const callOrder: number[] = []

      emitter.subscribe(async () => {
        callOrder.push(1)
        await vi.advanceTimersByTimeAsync(10)
        callOrder.push(2)
      })

      await emitter.emit(createTestEvent('CREATE', 'orders:o1'))

      // In synchronous mode, both pushes should happen before emit returns
      expect(callOrder).toEqual([1, 2])
    })
  })

  describe('backpressure', () => {
    it('applies backpressure when queue is full', async () => {
      // Use real timers to avoid deadlock with concurrent async operations
      vi.useRealTimers()
      // Create a fresh emitter with real timers and a small queue
      emitter = createMVEventEmitter({ maxQueueSize: 5 })

      const processedEvents: Event[] = []
      emitter.subscribe((event) => {
        processedEvents.push(event)
      })

      // Emit events sequentially - backpressure kicks in when queue exceeds maxQueueSize
      for (let i = 0; i < 10; i++) {
        await emitter.emit(createTestEvent('CREATE', `orders:o${i}`))
      }

      await emitter.flush()

      // All events should be processed
      expect(processedEvents.length).toBe(10)

      // Stats should reflect all events
      const stats = emitter.getStats()
      expect(stats.totalEmitted).toBe(10)
      // Restore fake timers for afterEach
      vi.useFakeTimers()
    })

    it('reports backpressure state in stats', async () => {
      vi.useRealTimers()
      emitter = createMVEventEmitter({ maxQueueSize: 2 })

      // Initially no backpressure
      const stats = emitter.getStats()
      expect(stats.backpressureActive).toBe(false)

      vi.useFakeTimers()
    })
  })

  describe('dispose', () => {
    it('clears all subscribers and state', () => {
      emitter.subscribe(() => {})
      emitter.subscribe(() => {})

      expect(emitter.getSubscriberCount()).toBe(2)

      emitter.dispose()

      expect(emitter.getSubscriberCount()).toBe(0)
      expect(emitter.hasSubscribers()).toBe(false)
    })
  })
})

// =============================================================================
// MVIntegrationBridge Tests
// =============================================================================

describe('MVIntegrationBridge', () => {
  let emitter: MVEventEmitter
  let engine: StreamingRefreshEngine
  let bridge: MVIntegrationBridge

  beforeEach(async () => {
    emitter = createMVEventEmitter()
    engine = createStreamingRefreshEngine({ batchSize: 1 })
    bridge = createMVIntegrationBridge(emitter, engine)
  })

  afterEach(async () => {
    bridge.disconnect()
    await engine.stop()
    emitter.dispose()
  })

  it('connects emitter to engine', async () => {
    const handler: MVHandler = {
      name: 'TestMV',
      sourceNamespaces: ['orders'],
      process: vi.fn(),
    }

    engine.registerMV(handler)
    await engine.start()

    bridge.connect()

    expect(bridge.isConnected()).toBe(true)

    await emitter.emit(createTestEvent('CREATE', 'orders:o1', { total: 100 }))
    await emitter.flush()
    await engine.flush()

    expect(handler.process).toHaveBeenCalled()
  })

  it('disconnects emitter from engine', async () => {
    const handler: MVHandler = {
      name: 'TestMV',
      sourceNamespaces: ['orders'],
      process: vi.fn(),
    }

    engine.registerMV(handler)
    await engine.start()

    bridge.connect()
    bridge.disconnect()

    expect(bridge.isConnected()).toBe(false)

    await emitter.emit(createTestEvent('CREATE', 'orders:o1', { total: 100 }))
    await emitter.flush()
    await engine.flush()

    // Handler should not be called after disconnect
    expect(handler.process).not.toHaveBeenCalled()
  })

  it('provides access to emitter and engine', () => {
    expect(bridge.getEmitter()).toBe(emitter)
    expect(bridge.getEngine()).toBe(engine)
  })
})

// =============================================================================
// MVEventSourceAdapter Tests
// =============================================================================

describe('MVEventSourceAdapter', () => {
  let emitter: MVEventEmitter
  let adapter: MVEventSourceAdapter

  beforeEach(() => {
    emitter = createMVEventEmitter()
    adapter = createMVEventSourceAdapter(emitter)
    adapter.start()
  })

  afterEach(() => {
    adapter.stop()
    emitter.dispose()
  })

  it('captures events from emitter', async () => {
    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.emit(createTestEvent('CREATE', 'orders:o2'))
    await emitter.flush()

    expect(adapter.getEventCount()).toBe(2)
  })

  it('provides getEventsInRange', async () => {
    const now = Date.now()
    const event1 = { ...createTestEvent('CREATE', 'orders:o1'), ts: now - 1000 }
    const event2 = { ...createTestEvent('CREATE', 'orders:o2'), ts: now }
    const event3 = { ...createTestEvent('CREATE', 'orders:o3'), ts: now + 1000 }

    await emitter.emit(event1)
    await emitter.emit(event2)
    await emitter.emit(event3)
    await emitter.flush()

    const events = await adapter.getEventsInRange(now - 500, now + 500)
    expect(events.length).toBe(1)
    expect(events[0].target).toBe('orders:o2')
  })

  it('provides getEventsForTarget', async () => {
    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.emit(createTestEvent('CREATE', 'orders:o2'))
    await emitter.emit(createTestEvent('CREATE', 'products:p1'))
    await emitter.flush()

    const orderEvents = await adapter.getEventsForTarget('orders:o1')
    expect(orderEvents.length).toBe(2) // Both orders:o1 and orders:o2 start with 'orders:'
  })

  it('respects maxEvents limit', async () => {
    adapter.stop()
    adapter = createMVEventSourceAdapter(emitter, { maxEvents: 5 })
    adapter.start()

    for (let i = 0; i < 10; i++) {
      await emitter.emit(createTestEvent('CREATE', `orders:o${i}`))
    }
    await emitter.flush()

    expect(adapter.getEventCount()).toBe(5)
  })

  it('clears captured events', async () => {
    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.flush()

    expect(adapter.getEventCount()).toBe(1)

    adapter.clear()

    expect(adapter.getEventCount()).toBe(0)
  })

  it('stops capturing after stop()', async () => {
    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.flush()

    expect(adapter.getEventCount()).toBe(1)

    adapter.stop()

    await emitter.emit(createTestEvent('CREATE', 'orders:o2'))
    await emitter.flush()

    expect(adapter.getEventCount()).toBe(1)
  })
})

// =============================================================================
// createMutationHook Tests
// =============================================================================

describe('createMutationHook', () => {
  it('creates a hook that emits events to emitter', async () => {
    const emitter = createMVEventEmitter()
    const subscriber = vi.fn()
    emitter.subscribe(subscriber)

    const hook = createMutationHook(emitter)

    const before = createTestEntity('orders/order-1', { total: 100 })
    const after = createTestEntity('orders/order-1', { total: 150 })

    await hook('UPDATE', 'orders:order-1', before, after, 'users/user-1' as EntityId)
    await emitter.flush()

    expect(subscriber).toHaveBeenCalled()
    const emittedEvent = subscriber.mock.calls[0][0]
    expect(emittedEvent.op).toBe('UPDATE')
    expect(emittedEvent.target).toBe('orders:order-1')

    emitter.dispose()
  })
})

// =============================================================================
// createMVIntegration Tests
// =============================================================================

describe('createMVIntegration', () => {
  it('creates all integration components', () => {
    const integration = createMVIntegration()

    expect(integration.emitter).toBeInstanceOf(MVEventEmitter)
    expect(integration.engine).toBeInstanceOf(StreamingRefreshEngine)
    expect(integration.bridge).toBeInstanceOf(MVIntegrationBridge)
    expect(typeof integration.mutationHook).toBe('function')
    expect(integration.eventSourceAdapter).toBeInstanceOf(MVEventSourceAdapter)

    integration.emitter.dispose()
  })

  it('works end-to-end', async () => {
    const integration = createMVIntegration()
    const { emitter, engine, bridge, mutationHook } = integration

    const processedEvents: Event[] = []
    const handler: MVHandler = {
      name: 'TestMV',
      sourceNamespaces: ['orders'],
      async process(events) {
        processedEvents.push(...events)
      },
    }

    engine.registerMV(handler)
    await engine.start()
    bridge.connect()

    // Use the mutation hook to emit an event
    const entity = createTestEntity('orders/order-1', { total: 100 })
    await mutationHook('CREATE', 'orders:order-1', null, entity)
    await emitter.flush()
    await engine.flush()

    expect(processedEvents.length).toBe(1)
    expect(processedEvents[0].op).toBe('CREATE')
    expect(processedEvents[0].target).toBe('orders:order-1')

    bridge.disconnect()
    await engine.stop()
    emitter.dispose()
  })

  it('supports custom options', () => {
    const integration = createMVIntegration({
      emitterOptions: { synchronous: true },
      engineOptions: { batchSize: 50 },
    })

    expect(integration.emitter).toBeInstanceOf(MVEventEmitter)
    expect(integration.engine).toBeInstanceOf(StreamingRefreshEngine)

    integration.emitter.dispose()
  })
})

// =============================================================================
// Integration with IncrementalRefresher Tests
// =============================================================================

describe('Integration with IncrementalRefresher', () => {
  it('MVEventSourceAdapter provides EventSource interface', async () => {
    const emitter = createMVEventEmitter()
    const adapter = createMVEventSourceAdapter(emitter)
    adapter.start()

    // Emit some events
    const now = Date.now()
    await emitter.emit({ ...createTestEvent('CREATE', 'users:user1'), ts: now - 2000 })
    await emitter.emit({ ...createTestEvent('UPDATE', 'users:user1'), ts: now - 1000 })
    await emitter.emit({ ...createTestEvent('CREATE', 'users:user2'), ts: now })
    await emitter.flush()

    // Query using EventSource interface
    const eventsInRange = await adapter.getEventsInRange(now - 1500, now + 500)
    expect(eventsInRange.length).toBe(2) // UPDATE and second CREATE

    const eventsForTarget = await adapter.getEventsForTarget('users:user1', now - 3000)
    expect(eventsForTarget.length).toBe(3) // All user events

    adapter.stop()
    emitter.dispose()
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling', () => {
  it('emitter handles subscriber async errors', async () => {
    const errors: Error[] = []
    const emitter = createMVEventEmitter({
      onError: (err) => errors.push(err),
    })

    emitter.subscribe(async () => {
      throw new Error('Async error')
    })

    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.flush()

    expect(errors.length).toBe(1)
    expect(errors[0].message).toBe('Async error')

    emitter.dispose()
  })

  it('bridge handles engine errors gracefully', async () => {
    const emitter = createMVEventEmitter()
    const engine = createStreamingRefreshEngine()

    const handler: MVHandler = {
      name: 'FailingMV',
      sourceNamespaces: ['orders'],
      async process() {
        throw new Error('Handler error')
      },
    }

    engine.registerMV(handler)
    await engine.start()

    const bridge = createMVIntegrationBridge(emitter, engine)
    bridge.connect()

    // Should not throw
    await emitter.emit(createTestEvent('CREATE', 'orders:o1'))
    await emitter.flush()
    await engine.flush()

    bridge.disconnect()
    await engine.stop()
    emitter.dispose()
  })
})

// =============================================================================
// attachMVIntegration Tests
// =============================================================================

describe('attachMVIntegration', () => {
  it('attaches MV integration to a mock db instance', async () => {
    const { attachMVIntegration } = await import('@/materialized-views/write-path-integration')

    let eventCallback: ((event: Event) => void | Promise<void>) | null = null
    const mockDb = {
      setEventCallback: (cb: ((event: Event) => void | Promise<void>) | null) => {
        eventCallback = cb
      },
    }

    const processedEvents: Event[] = []
    const { integration, start, stop, detach } = attachMVIntegration(mockDb)

    integration.engine.registerMV({
      name: 'TestMV',
      sourceNamespaces: ['orders'],
      async process(events) {
        processedEvents.push(...events)
      },
    })

    await start()

    // Verify callback was set
    expect(eventCallback).not.toBeNull()

    // Simulate an event from ParqueDB
    const event = createTestEvent('CREATE', 'orders:order-1', { total: 100 })
    await eventCallback!(event)
    await integration.emitter.flush()
    await integration.engine.flush()

    // Verify event was processed
    expect(processedEvents.length).toBe(1)
    expect(processedEvents[0].op).toBe('CREATE')
    expect(processedEvents[0].target).toBe('orders:order-1')

    await stop()
    detach()

    // Verify callback was cleared
    expect(eventCallback).toBeNull()
  })

  it('detaches cleanly', async () => {
    const { attachMVIntegration } = await import('@/materialized-views/write-path-integration')

    let eventCallback: ((event: Event) => void | Promise<void>) | null = null
    const mockDb = {
      setEventCallback: (cb: ((event: Event) => void | Promise<void>) | null) => {
        eventCallback = cb
      },
    }

    const { detach } = attachMVIntegration(mockDb)

    expect(eventCallback).not.toBeNull()

    detach()

    expect(eventCallback).toBeNull()
  })
})

// ParqueDB integration tests are in parquedb-mv-integration.test.ts
// (separated to avoid fake timer conflicts)

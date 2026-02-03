/**
 * Tests for Stream Adapters
 *
 * Tests the EventEmitter, AsyncIterator, and Polling adapters for
 * streaming observability data.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  createStreamAdapter,
  EventEmitterAdapter,
  AsyncIteratorAdapter,
  PollingAdapter,
  StreamEventEmitter,
  DEFAULT_STREAM_ADAPTER_CONFIG,
  DEFAULT_POLLING_ADAPTER_CONFIG,
  type ObservabilityStreamEvent,
  type QueryStreamEvent,
  type MutationStreamEvent,
  type StorageStreamEvent,
} from '../../../src/observability/stream-adapter'
import {
  HookRegistry,
  createQueryContext,
  createMutationContext,
  createStorageContext,
  type QueryResult,
  type MutationResult,
  type StorageResult,
} from '../../../src/observability'

// =============================================================================
// EventEmitter Adapter Tests
// =============================================================================

describe('EventEmitterAdapter', () => {
  let adapter: EventEmitterAdapter
  let registry: HookRegistry

  beforeEach(() => {
    adapter = new EventEmitterAdapter()
    registry = new HookRegistry()
    registry.registerHook(adapter.getHook())
  })

  afterEach(() => {
    adapter.close()
    registry.clearHooks()
  })

  describe('basic functionality', () => {
    it('should emit events for query operations', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('event', (event) => events.push(event))

      const context = createQueryContext('find', 'posts', { status: 'published' })
      await registry.dispatchQueryStart(context)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('query')
      expect(events[0].type).toBe('start')
    })

    it('should emit events for query end', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('event', (event) => events.push(event))

      const context = createQueryContext('find', 'posts')
      const result: QueryResult = { rowCount: 10, durationMs: 50 }
      await registry.dispatchQueryEnd(context, result)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('query')
      expect(events[0].type).toBe('end')
      expect((events[0] as QueryStreamEvent).result).toEqual(result)
    })

    it('should emit events for query errors', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('event', (event) => events.push(event))

      const context = createQueryContext('find', 'posts')
      const error = new Error('Query failed')
      await registry.dispatchQueryError(context, error)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('query')
      expect(events[0].type).toBe('error')
      expect((events[0] as QueryStreamEvent).error).toBe(error)
    })

    it('should emit events for mutation operations', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('mutation', (event) => events.push(event))

      const context = createMutationContext('create', 'posts')
      await registry.dispatchMutationStart(context)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('mutation')
      expect(events[0].type).toBe('start')
    })

    it('should emit events for storage operations', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('storage', (event) => events.push(event))

      const context = createStorageContext('read', 'data/posts.parquet')
      const result: StorageResult = { bytesTransferred: 1024, durationMs: 15 }
      await registry.dispatchStorageRead(context, result)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('storage')
      expect((events[0] as StorageStreamEvent).result).toEqual(result)
    })
  })

  describe('category-specific events', () => {
    it('should emit to query listener for query events', async () => {
      const queryEvents: QueryStreamEvent[] = []
      const allEvents: ObservabilityStreamEvent[] = []

      adapter.getEmitter().on('query', (event) => queryEvents.push(event))
      adapter.getEmitter().on('event', (event) => allEvents.push(event))

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(queryEvents).toHaveLength(1)
      expect(allEvents).toHaveLength(1)
    })

    it('should emit to mutation listener for mutation events', async () => {
      const mutationEvents: MutationStreamEvent[] = []

      adapter.getEmitter().on('mutation', (event) => mutationEvents.push(event))

      const context = createMutationContext('update', 'posts', 'posts/123')
      await registry.dispatchMutationStart(context)

      expect(mutationEvents).toHaveLength(1)
      expect(mutationEvents[0].context.operationType).toBe('update')
    })
  })

  describe('batch events', () => {
    it('should emit batch events when buffer reaches batch size', async () => {
      const smallBatchAdapter = new EventEmitterAdapter({ batchSize: 3 })
      const smallBatchRegistry = new HookRegistry()
      smallBatchRegistry.registerHook(smallBatchAdapter.getHook())

      const batches: ObservabilityStreamEvent[][] = []
      smallBatchAdapter.getEmitter().on('batch', (batch) => batches.push(batch))

      // Emit 3 events to trigger batch
      const context = createQueryContext('find', 'posts')
      await smallBatchRegistry.dispatchQueryStart(context)
      await smallBatchRegistry.dispatchQueryStart(context)
      await smallBatchRegistry.dispatchQueryStart(context)

      expect(batches).toHaveLength(1)
      expect(batches[0]).toHaveLength(3)

      smallBatchAdapter.close()
    })

    it('should flush remaining events on close', async () => {
      const batches: ObservabilityStreamEvent[][] = []
      adapter.getEmitter().on('batch', (batch) => batches.push(batch))

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      adapter.close()

      expect(batches).toHaveLength(1)
    })
  })

  describe('filtering', () => {
    it('should filter by category', async () => {
      const filteredAdapter = new EventEmitterAdapter({
        categories: ['query'],
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const events: ObservabilityStreamEvent[] = []
      filteredAdapter.getEmitter().on('event', (event) => events.push(event))

      const queryContext = createQueryContext('find', 'posts')
      const mutationContext = createMutationContext('create', 'posts')

      await filteredRegistry.dispatchQueryStart(queryContext)
      await filteredRegistry.dispatchMutationStart(mutationContext)

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('query')

      filteredAdapter.close()
    })

    it('should filter by type', async () => {
      const filteredAdapter = new EventEmitterAdapter({
        types: ['end', 'error'],
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const events: ObservabilityStreamEvent[] = []
      filteredAdapter.getEmitter().on('event', (event) => events.push(event))

      const context = createQueryContext('find', 'posts')
      await filteredRegistry.dispatchQueryStart(context)
      await filteredRegistry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('end')

      filteredAdapter.close()
    })

    it('should filter with custom filter function', async () => {
      const filteredAdapter = new EventEmitterAdapter({
        filter: (event) => event.category === 'query' && event.type === 'end',
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const events: ObservabilityStreamEvent[] = []
      filteredAdapter.getEmitter().on('event', (event) => events.push(event))

      const context = createQueryContext('find', 'posts')
      await filteredRegistry.dispatchQueryStart(context)
      await filteredRegistry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('end')

      filteredAdapter.close()
    })
  })

  describe('buffer overflow', () => {
    it('should emit overflow event when buffer is full', async () => {
      const smallBufferAdapter = new EventEmitterAdapter({
        maxBufferSize: 2,
        batchSize: 100, // Large batch size to prevent auto-flush
      })
      const smallBufferRegistry = new HookRegistry()
      smallBufferRegistry.registerHook(smallBufferAdapter.getHook())

      const overflows: Array<{ dropped: number; bufferSize: number }> = []
      smallBufferAdapter.getEmitter().on('overflow', (info) => overflows.push(info))

      const context = createQueryContext('find', 'posts')
      await smallBufferRegistry.dispatchQueryStart(context)
      await smallBufferRegistry.dispatchQueryStart(context)
      await smallBufferRegistry.dispatchQueryStart(context)

      expect(overflows).toHaveLength(1)
      expect(overflows[0].dropped).toBe(1)

      smallBufferAdapter.close()
    })
  })

  describe('state management', () => {
    it('should track events processed', async () => {
      expect(adapter.getEventsProcessed()).toBe(0)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      expect(adapter.getEventsProcessed()).toBe(2)
    })

    it('should report active state correctly', () => {
      expect(adapter.isActive()).toBe(true)
      adapter.close()
      expect(adapter.isActive()).toBe(false)
    })

    it('should not emit events after close', async () => {
      const events: ObservabilityStreamEvent[] = []
      adapter.getEmitter().on('event', (event) => events.push(event))

      adapter.close()

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(events).toHaveLength(0)
    })
  })
})

// =============================================================================
// AsyncIterator Adapter Tests
// =============================================================================

describe('AsyncIteratorAdapter', () => {
  let adapter: AsyncIteratorAdapter
  let registry: HookRegistry

  beforeEach(() => {
    adapter = new AsyncIteratorAdapter()
    registry = new HookRegistry()
    registry.registerHook(adapter.getHook())
  })

  afterEach(() => {
    adapter.close()
    registry.clearHooks()
  })

  describe('basic functionality', () => {
    it('should provide async iterable', () => {
      const iterator = adapter.getIterator()
      expect(iterator[Symbol.asyncIterator]).toBeDefined()
    })

    it('should yield events from async iteration', async () => {
      const iterator = adapter.getIterator()

      // Emit event first
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      // Then iterate
      const asyncIterator = iterator[Symbol.asyncIterator]()
      const result = await asyncIterator.next()

      expect(result.done).toBe(false)
      expect(result.value.category).toBe('query')
      expect(result.value.type).toBe('start')
    })

    it('should return done when closed', async () => {
      const iterator = adapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      adapter.close()

      const result = await asyncIterator.next()
      expect(result.done).toBe(true)
    })

    it('should handle multiple events in sequence', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      const iterator = adapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      const event1 = await asyncIterator.next()
      const event2 = await asyncIterator.next()

      expect(event1.done).toBe(false)
      expect(event1.value.type).toBe('start')
      expect(event2.done).toBe(false)
      expect(event2.value.type).toBe('end')
    })
  })

  describe('blocking behavior', () => {
    it('should wait for events when buffer is empty', async () => {
      const iterator = adapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      // Start waiting for next event
      const nextPromise = asyncIterator.next()

      // Emit event after a delay
      setTimeout(async () => {
        const context = createQueryContext('find', 'posts')
        await registry.dispatchQueryStart(context)
      }, 10)

      const result = await nextPromise
      expect(result.done).toBe(false)
      expect(result.value.category).toBe('query')
    })
  })

  describe('filtering', () => {
    it('should filter by category', async () => {
      const filteredAdapter = new AsyncIteratorAdapter({
        categories: ['mutation'],
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const queryContext = createQueryContext('find', 'posts')
      const mutationContext = createMutationContext('create', 'posts')

      await filteredRegistry.dispatchQueryStart(queryContext)
      await filteredRegistry.dispatchMutationStart(mutationContext)

      const iterator = filteredAdapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      const result = await asyncIterator.next()
      expect(result.value.category).toBe('mutation')

      filteredAdapter.close()
    })
  })

  describe('buffer management', () => {
    it('should drop oldest events on overflow', async () => {
      const smallBufferAdapter = new AsyncIteratorAdapter({
        maxBufferSize: 2,
      })
      const smallBufferRegistry = new HookRegistry()
      smallBufferRegistry.registerHook(smallBufferAdapter.getHook())

      // Emit 3 events, first should be dropped
      const context1 = createQueryContext('find', 'posts')
      const context2 = createQueryContext('findOne', 'posts')
      const context3 = createQueryContext('count', 'posts')

      await smallBufferRegistry.dispatchQueryStart(context1)
      await smallBufferRegistry.dispatchQueryStart(context2)
      await smallBufferRegistry.dispatchQueryStart(context3)

      // Buffer should have last 2 events
      expect(smallBufferAdapter.getBufferSize()).toBe(2)

      const iterator = smallBufferAdapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      const result1 = await asyncIterator.next()
      expect((result1.value as QueryStreamEvent).context.operationType).toBe('findOne')

      smallBufferAdapter.close()
    })
  })

  describe('for-await-of usage', () => {
    it('should work with for-await-of loop', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      const events: ObservabilityStreamEvent[] = []
      const iterator = adapter.getIterator()

      // Use promise to collect events
      const collectPromise = (async () => {
        for await (const event of iterator) {
          events.push(event)
          if (events.length >= 2) break
        }
      })()

      await collectPromise

      expect(events).toHaveLength(2)
      expect(events[0].type).toBe('start')
      expect(events[1].type).toBe('end')
    })
  })

  describe('return method', () => {
    it('should close adapter when iterator return is called', async () => {
      const iterator = adapter.getIterator()
      const asyncIterator = iterator[Symbol.asyncIterator]()

      expect(adapter.isActive()).toBe(true)

      await asyncIterator.return?.()

      expect(adapter.isActive()).toBe(false)
    })
  })
})

// =============================================================================
// Polling Adapter Tests
// =============================================================================

describe('PollingAdapter', () => {
  let adapter: PollingAdapter
  let registry: HookRegistry

  beforeEach(() => {
    adapter = new PollingAdapter()
    registry = new HookRegistry()
    registry.registerHook(adapter.getHook())
  })

  afterEach(() => {
    adapter.close()
    registry.clearHooks()
  })

  describe('poll()', () => {
    it('should return buffered events', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })

      const events = adapter.poll()

      expect(events).toHaveLength(2)
      expect(events[0].type).toBe('start')
      expect(events[1].type).toBe('end')
    })

    it('should clear buffer after poll', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      const events1 = adapter.poll()
      const events2 = adapter.poll()

      expect(events1).toHaveLength(1)
      expect(events2).toHaveLength(0)
    })

    it('should return empty array when no events', () => {
      const events = adapter.poll()
      expect(events).toHaveLength(0)
    })
  })

  describe('peek()', () => {
    it('should return buffered events without clearing', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      const events1 = adapter.peek()
      const events2 = adapter.peek()

      expect(events1).toHaveLength(1)
      expect(events2).toHaveLength(1)
    })

    it('should return a copy of the buffer', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      const events = adapter.peek()
      events.push({} as ObservabilityStreamEvent)

      expect(adapter.getBufferSize()).toBe(1)
    })
  })

  describe('take()', () => {
    it('should return specified number of events', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })
      await registry.dispatchQueryStart(context)

      const events = adapter.take(2)

      expect(events).toHaveLength(2)
      expect(adapter.getBufferSize()).toBe(1)
    })

    it('should return all events if count exceeds buffer', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      const events = adapter.take(10)

      expect(events).toHaveLength(1)
      expect(adapter.getBufferSize()).toBe(0)
    })
  })

  describe('filtering', () => {
    it('should filter by category', async () => {
      const filteredAdapter = new PollingAdapter({
        categories: ['mutation'],
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const queryContext = createQueryContext('find', 'posts')
      const mutationContext = createMutationContext('create', 'posts')

      await filteredRegistry.dispatchQueryStart(queryContext)
      await filteredRegistry.dispatchMutationStart(mutationContext)

      const events = filteredAdapter.poll()

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('mutation')

      filteredAdapter.close()
    })

    it('should filter by type', async () => {
      const filteredAdapter = new PollingAdapter({
        types: ['error'],
      })
      const filteredRegistry = new HookRegistry()
      filteredRegistry.registerHook(filteredAdapter.getHook())

      const context = createQueryContext('find', 'posts')
      await filteredRegistry.dispatchQueryStart(context)
      await filteredRegistry.dispatchQueryError(context, new Error('Failed'))

      const events = filteredAdapter.poll()

      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('error')

      filteredAdapter.close()
    })
  })

  describe('buffer overflow', () => {
    it('should drop oldest events on overflow', async () => {
      const smallBufferAdapter = new PollingAdapter({
        maxBufferSize: 2,
      })
      const smallBufferRegistry = new HookRegistry()
      smallBufferRegistry.registerHook(smallBufferAdapter.getHook())

      const context1 = createQueryContext('find', 'posts')
      const context2 = createQueryContext('findOne', 'posts')
      const context3 = createQueryContext('count', 'posts')

      await smallBufferRegistry.dispatchQueryStart(context1)
      await smallBufferRegistry.dispatchQueryStart(context2)
      await smallBufferRegistry.dispatchQueryStart(context3)

      const events = smallBufferAdapter.poll()

      expect(events).toHaveLength(2)
      expect((events[0] as QueryStreamEvent).context.operationType).toBe('findOne')
      expect((events[1] as QueryStreamEvent).context.operationType).toBe('count')

      smallBufferAdapter.close()
    })
  })

  describe('state management', () => {
    it('should track events processed', async () => {
      expect(adapter.getEventsProcessed()).toBe(0)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(adapter.getEventsProcessed()).toBe(1)
    })

    it('should report buffer size correctly', async () => {
      expect(adapter.getBufferSize()).toBe(0)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(adapter.getBufferSize()).toBe(1)
    })

    it('should clear buffer on close', async () => {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      adapter.close()

      expect(adapter.getBufferSize()).toBe(0)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createStreamAdapter', () => {
  describe('emitter type', () => {
    it('should create emitter adapter', () => {
      const result = createStreamAdapter({ type: 'emitter' })

      expect(result.type).toBe('emitter')
      expect(result.adapter).toBeInstanceOf(EventEmitterAdapter)
      expect(result.emitter).toBeInstanceOf(StreamEventEmitter)
      expect(result.hook).toBeDefined()
      expect(typeof result.close).toBe('function')

      result.close()
    })

    it('should pass config to emitter adapter', () => {
      const result = createStreamAdapter({
        type: 'emitter',
        maxBufferSize: 500,
        categories: ['query'],
      })

      // Verify config is applied by checking that only queries are emitted
      const registry = new HookRegistry()
      registry.registerHook(result.hook)

      const events: ObservabilityStreamEvent[] = []
      result.emitter.on('event', (event) => events.push(event))

      registry.dispatchQueryStart(createQueryContext('find', 'posts'))
      registry.dispatchMutationStart(createMutationContext('create', 'posts'))

      expect(events).toHaveLength(1)
      expect(events[0].category).toBe('query')

      result.close()
    })
  })

  describe('iterator type', () => {
    it('should create iterator adapter', () => {
      const result = createStreamAdapter({ type: 'iterator' })

      expect(result.type).toBe('iterator')
      expect(result.adapter).toBeInstanceOf(AsyncIteratorAdapter)
      expect(result.iterator[Symbol.asyncIterator]).toBeDefined()
      expect(result.hook).toBeDefined()
      expect(typeof result.close).toBe('function')

      result.close()
    })

    it('should pass config to iterator adapter', async () => {
      const result = createStreamAdapter({
        type: 'iterator',
        categories: ['mutation'],
      })

      const registry = new HookRegistry()
      registry.registerHook(result.hook)

      await registry.dispatchQueryStart(createQueryContext('find', 'posts'))
      await registry.dispatchMutationStart(createMutationContext('create', 'posts'))

      const asyncIterator = result.iterator[Symbol.asyncIterator]()
      const event = await asyncIterator.next()

      expect(event.value.category).toBe('mutation')

      result.close()
    })
  })

  describe('polling type', () => {
    it('should create polling adapter', () => {
      const result = createStreamAdapter({ type: 'polling' })

      expect(result.type).toBe('polling')
      expect(result.adapter).toBeInstanceOf(PollingAdapter)
      expect(typeof result.poll).toBe('function')
      expect(typeof result.peek).toBe('function')
      expect(typeof result.take).toBe('function')
      expect(result.hook).toBeDefined()
      expect(typeof result.close).toBe('function')

      result.close()
    })

    it('should provide working poll/peek/take functions', async () => {
      const result = createStreamAdapter({ type: 'polling' })

      const registry = new HookRegistry()
      registry.registerHook(result.hook)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
      await registry.dispatchQueryEnd(context, { rowCount: 10, durationMs: 50 })
      await registry.dispatchQueryStart(context)

      // Test peek
      const peeked = result.peek()
      expect(peeked).toHaveLength(3)

      // Test take
      const taken = result.take(1)
      expect(taken).toHaveLength(1)

      // Test poll
      const polled = result.poll()
      expect(polled).toHaveLength(2)

      result.close()
    })
  })

  describe('error handling', () => {
    it('should throw for unknown type', () => {
      expect(() => {
        createStreamAdapter({ type: 'unknown' as 'emitter' })
      }).toThrow()
    })
  })
})

// =============================================================================
// Default Configuration Tests
// =============================================================================

describe('Default Configurations', () => {
  it('should have correct default stream adapter config', () => {
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.maxBufferSize).toBe(10000)
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.includeFullContext).toBe(true)
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.categories).toEqual(['query', 'mutation', 'storage'])
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.types).toEqual(['start', 'end', 'error'])
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.batchSize).toBe(100)
    expect(DEFAULT_STREAM_ADAPTER_CONFIG.batchTimeoutMs).toBe(1000)
  })

  it('should have correct default polling adapter config', () => {
    expect(DEFAULT_POLLING_ADAPTER_CONFIG.pollingIntervalMs).toBe(100)
    // Should include all stream adapter defaults
    expect(DEFAULT_POLLING_ADAPTER_CONFIG.maxBufferSize).toBe(10000)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration', () => {
  it('should work with global hook registry', async () => {
    const { globalHookRegistry } = await import('../../../src/observability')

    const result = createStreamAdapter({ type: 'polling' })
    const unregister = globalHookRegistry.registerHook(result.hook)

    try {
      const context = createQueryContext('find', 'posts')
      await globalHookRegistry.dispatchQueryStart(context)

      const events = result.poll()
      expect(events).toHaveLength(1)
    } finally {
      unregister()
      result.close()
    }
  })

  it('should work with observed storage backend', async () => {
    const { MemoryBackend, withObservability } = await import('../../../src/storage')
    const { globalHookRegistry } = await import('../../../src/observability')

    const memory = new MemoryBackend()
    const observed = withObservability(memory)

    const result = createStreamAdapter({
      type: 'polling',
      categories: ['storage'],
    })
    const unregister = globalHookRegistry.registerHook(result.hook)

    try {
      await observed.write('test.txt', new Uint8Array([1, 2, 3]))

      const events = result.poll()
      expect(events.length).toBeGreaterThan(0)
      expect(events[0].category).toBe('storage')
    } finally {
      unregister()
      result.close()
      globalHookRegistry.clearHooks()
    }
  })

  it('should handle high-volume event streams', async () => {
    const result = createStreamAdapter({
      type: 'polling',
      maxBufferSize: 1000,
    })

    const registry = new HookRegistry()
    registry.registerHook(result.hook)

    // Emit many events
    for (let i = 0; i < 500; i++) {
      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)
    }

    const events = result.poll()
    expect(events).toHaveLength(500)
    expect(result.adapter.getEventsProcessed()).toBe(500)

    result.close()
  })
})

// =============================================================================
// StreamEventEmitter Tests
// =============================================================================

describe('StreamEventEmitter', () => {
  it('should emit typed events', () => {
    const emitter = new StreamEventEmitter()
    const events: ObservabilityStreamEvent[] = []

    emitter.on('event', (event) => events.push(event))

    const event: QueryStreamEvent = {
      id: 'test',
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: createQueryContext('find', 'posts'),
    }

    emitter.emit('event', event)

    expect(events).toHaveLength(1)
    expect(events[0]).toBe(event)
  })

  it('should support once listener', () => {
    const emitter = new StreamEventEmitter()
    const events: ObservabilityStreamEvent[] = []

    emitter.once('event', (event) => events.push(event))

    const event: QueryStreamEvent = {
      id: 'test',
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: createQueryContext('find', 'posts'),
    }

    emitter.emit('event', event)
    emitter.emit('event', event)

    expect(events).toHaveLength(1)
  })

  it('should support off to remove listeners', () => {
    const emitter = new StreamEventEmitter()
    const events: ObservabilityStreamEvent[] = []

    const listener = (event: ObservabilityStreamEvent) => events.push(event)
    emitter.on('event', listener)

    const event: QueryStreamEvent = {
      id: 'test',
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: createQueryContext('find', 'posts'),
    }

    emitter.emit('event', event)
    emitter.off('event', listener)
    emitter.emit('event', event)

    expect(events).toHaveLength(1)
  })
})

/**
 * Subscriptions Module Re-export Test Suite
 *
 * Verifies that all expected exports are accessible from the subscriptions index module.
 */

import { describe, it, expect } from 'vitest'
import {
  // Types (re-exported from types.ts)
  DEFAULT_SUBSCRIPTION_CONFIG,
  // Manager
  SubscriptionManager,
  createSubscriptionManager,
  // Transport implementations
  WebSocketWriter,
  SSEWriter,
  createSSEResponse,
  NodeSSEWriter,
  MockWriter,
  // Event sources
  InMemoryEventSource,
  createInMemoryEventSource,
  EventWriterSource,
  createEventWriterSource,
  PollingEventSource,
  createPollingEventSource,
  // WebSocket handler
  handleWebSocketUpgrade,
} from '@/subscriptions'

// =============================================================================
// Re-export Verification Tests
// =============================================================================

describe('subscriptions module exports', () => {
  describe('types', () => {
    it('exports DEFAULT_SUBSCRIPTION_CONFIG', () => {
      expect(DEFAULT_SUBSCRIPTION_CONFIG).toBeDefined()
      expect(DEFAULT_SUBSCRIPTION_CONFIG.maxSubscriptionsPerConnection).toBeTypeOf('number')
      expect(DEFAULT_SUBSCRIPTION_CONFIG.connectionTimeoutMs).toBeTypeOf('number')
      expect(DEFAULT_SUBSCRIPTION_CONFIG.heartbeatIntervalMs).toBeTypeOf('number')
      expect(DEFAULT_SUBSCRIPTION_CONFIG.maxPendingEvents).toBeTypeOf('number')
      expect(DEFAULT_SUBSCRIPTION_CONFIG.debug).toBeTypeOf('boolean')
    })
  })

  describe('manager', () => {
    it('exports SubscriptionManager class', () => {
      expect(SubscriptionManager).toBeDefined()
      expect(typeof SubscriptionManager).toBe('function')

      const manager = new SubscriptionManager()
      expect(manager).toBeInstanceOf(SubscriptionManager)
    })

    it('exports createSubscriptionManager factory', () => {
      expect(createSubscriptionManager).toBeTypeOf('function')

      const manager = createSubscriptionManager()
      expect(manager).toBeInstanceOf(SubscriptionManager)
    })
  })

  describe('transports', () => {
    it('exports WebSocketWriter class', () => {
      expect(WebSocketWriter).toBeDefined()
      expect(typeof WebSocketWriter).toBe('function')
    })

    it('exports SSEWriter class', () => {
      expect(SSEWriter).toBeDefined()
      expect(typeof SSEWriter).toBe('function')
    })

    it('exports createSSEResponse factory', () => {
      expect(createSSEResponse).toBeTypeOf('function')
    })

    it('exports NodeSSEWriter class', () => {
      expect(NodeSSEWriter).toBeDefined()
      expect(typeof NodeSSEWriter).toBe('function')
    })

    it('exports MockWriter class', () => {
      expect(MockWriter).toBeDefined()
      expect(typeof MockWriter).toBe('function')

      const writer = new MockWriter()
      expect(writer).toBeInstanceOf(MockWriter)
    })

    it('exports handleWebSocketUpgrade function', () => {
      expect(handleWebSocketUpgrade).toBeTypeOf('function')
    })
  })

  describe('event sources', () => {
    it('exports InMemoryEventSource class', () => {
      expect(InMemoryEventSource).toBeDefined()
      expect(typeof InMemoryEventSource).toBe('function')

      const source = new InMemoryEventSource()
      expect(source).toBeInstanceOf(InMemoryEventSource)
    })

    it('exports createInMemoryEventSource factory', () => {
      expect(createInMemoryEventSource).toBeTypeOf('function')

      const source = createInMemoryEventSource()
      expect(source).toBeInstanceOf(InMemoryEventSource)
    })

    it('exports EventWriterSource class', () => {
      expect(EventWriterSource).toBeDefined()
      expect(typeof EventWriterSource).toBe('function')

      const source = new EventWriterSource()
      expect(source).toBeInstanceOf(EventWriterSource)
    })

    it('exports createEventWriterSource factory', () => {
      expect(createEventWriterSource).toBeTypeOf('function')

      const source = createEventWriterSource()
      expect(source).toBeInstanceOf(EventWriterSource)
    })

    it('exports PollingEventSource class', () => {
      expect(PollingEventSource).toBeDefined()
      expect(typeof PollingEventSource).toBe('function')
    })

    it('exports createPollingEventSource factory', () => {
      expect(createPollingEventSource).toBeTypeOf('function')
    })
  })
})

// =============================================================================
// Integration: End-to-end subscription flow via module exports
// =============================================================================

describe('subscriptions end-to-end flow via module exports', () => {
  it('creates a full subscription pipeline', async () => {
    // Create components using module exports
    const manager = createSubscriptionManager()
    const source = createInMemoryEventSource()

    await manager.setEventSource(source)
    await manager.start()

    // Add connection with mock writer
    const writer = new MockWriter()
    const connection = manager.addConnection(writer)

    // Should receive connected message
    expect(writer.getMessagesOfType('connected')).toHaveLength(1)

    // Subscribe to a namespace
    const subId = manager.subscribe(connection.id, {
      ns: 'posts',
      ops: ['CREATE', 'UPDATE'],
      filter: { published: true },
    })

    expect(subId).not.toBeNull()
    expect(writer.getMessagesOfType('subscribed')).toHaveLength(1)

    writer.clear()

    // Emit matching event
    source.emit({
      id: 'evt_e2e_1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'posts:post1',
      after: { title: 'Test', published: true },
    })

    const changes = writer.getMessagesOfType('change')
    expect(changes).toHaveLength(1)
    expect(changes[0].data.ns).toBe('posts')
    expect(changes[0].data.entityId).toBe('post1')

    // Emit non-matching event (wrong op)
    source.emit({
      id: 'evt_e2e_2',
      ts: Date.now(),
      op: 'DELETE',
      target: 'posts:post2',
      before: { title: 'Deleted', published: true },
    })

    // Should still only have 1 change (DELETE not subscribed)
    expect(writer.getMessagesOfType('change')).toHaveLength(1)

    // Emit non-matching event (filter mismatch)
    source.emit({
      id: 'evt_e2e_3',
      ts: Date.now(),
      op: 'CREATE',
      target: 'posts:post3',
      after: { title: 'Draft', published: false },
    })

    // Should still only have 1 change
    expect(writer.getMessagesOfType('change')).toHaveLength(1)

    // Unsubscribe
    const removed = manager.unsubscribe(connection.id, subId!)
    expect(removed).toBe(true)

    // Verify stats
    const stats = manager.getStats()
    expect(stats.eventsProcessed).toBe(3)
    expect(stats.eventsDelivered).toBe(1)

    // Clean up
    await manager.stop()
  })
})

/**
 * SubscriptionManager Test Suite
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  SubscriptionManager,
  createSubscriptionManager,
  InMemoryEventSource,
  MockWriter,
} from '@/subscriptions'
import type { Event } from '@/types'

// =============================================================================
// Helper Functions
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'posts:post1',
    after: { title: 'Test Post', status: 'published' },
    actor: 'users/admin',
    ...overrides,
  }
}

// =============================================================================
// SubscriptionManager Tests
// =============================================================================

describe('SubscriptionManager', () => {
  let manager: SubscriptionManager
  let eventSource: InMemoryEventSource

  beforeEach(async () => {
    manager = createSubscriptionManager({ debug: false })
    eventSource = new InMemoryEventSource()
    await manager.setEventSource(eventSource)
    await manager.start()
  })

  afterEach(async () => {
    await manager.stop()
  })

  describe('connection management', () => {
    it('adds a connection', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      expect(connection.id).toMatch(/^conn_/)
      expect(connection.subscriptions.size).toBe(0)

      // Should receive connected message
      expect(writer.messages).toHaveLength(1)
      expect(writer.messages[0].type).toBe('connected')
    })

    it('removes a connection', async () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      await manager.removeConnection(connection.id)

      expect(manager.getConnection(connection.id)).toBeUndefined()
      expect(writer.isOpen()).toBe(false)
    })

    it('tracks multiple connections', () => {
      const writer1 = new MockWriter()
      const writer2 = new MockWriter()

      const conn1 = manager.addConnection(writer1)
      const conn2 = manager.addConnection(writer2)

      expect(conn1.id).not.toBe(conn2.id)
      expect(manager.getConnection(conn1.id)).toBeDefined()
      expect(manager.getConnection(conn2.id)).toBeDefined()
    })

    it('stores connection metadata', () => {
      const writer = new MockWriter()
      const metadata = { userId: 'user1', role: 'admin' }
      const connection = manager.addConnection(writer, metadata)

      expect(connection.metadata).toEqual(metadata)
    })
  })

  describe('subscription management', () => {
    it('creates a subscription', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)
      writer.clear() // Clear connected message

      const subId = manager.subscribe(connection.id, {
        ns: 'posts',
        filter: { status: 'published' },
      })

      expect(subId).toMatch(/^sub_/)

      // Should receive subscribed message
      expect(writer.messages).toHaveLength(1)
      expect(writer.messages[0]).toEqual({
        type: 'subscribed',
        subscriptionId: subId,
        ns: 'posts',
      })
    })

    it('removes a subscription', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const subId = manager.subscribe(connection.id, { ns: 'posts' })
      writer.clear()

      const removed = manager.unsubscribe(connection.id, subId!)

      expect(removed).toBe(true)
      expect(writer.messages).toHaveLength(1)
      expect(writer.messages[0]).toEqual({
        type: 'unsubscribed',
        subscriptionId: subId,
      })
    })

    it('fails to subscribe to non-existent connection', () => {
      const subId = manager.subscribe('non-existent', { ns: 'posts' })
      expect(subId).toBeNull()
    })

    it('fails to unsubscribe from non-existent subscription', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const removed = manager.unsubscribe(connection.id, 'non-existent')
      expect(removed).toBe(false)
    })

    it('enforces max subscriptions per connection', () => {
      const manager = createSubscriptionManager({
        maxSubscriptionsPerConnection: 2,
      })

      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const sub1 = manager.subscribe(connection.id, { ns: 'posts' })
      const sub2 = manager.subscribe(connection.id, { ns: 'users' })
      const sub3 = manager.subscribe(connection.id, { ns: 'comments' })

      expect(sub1).not.toBeNull()
      expect(sub2).not.toBeNull()
      expect(sub3).toBeNull() // Should fail

      // Should receive error message
      const errorMessages = writer.getMessagesOfType('error')
      expect(errorMessages).toHaveLength(1)
      expect(errorMessages[0].code).toBe('MAX_SUBSCRIPTIONS')
    })
  })

  describe('event delivery', () => {
    it('delivers events matching subscription', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
      })
      writer.clear()

      // Emit an event
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        after: { title: 'New Post' },
      }))

      // Should receive change event
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(1)
      expect(changeMessages[0].data.ns).toBe('posts')
      expect(changeMessages[0].data.entityId).toBe('post1')
      expect(changeMessages[0].data.after).toEqual({ title: 'New Post' })
    })

    it('does not deliver events for other namespaces', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
      })
      writer.clear()

      // Emit an event for different namespace
      eventSource.emit(createTestEvent({
        target: 'users:user1',
        after: { name: 'Test User' },
      }))

      // Should not receive any change events
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(0)
    })

    it('filters events by filter', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        filter: { status: 'published' },
      })
      writer.clear()

      // Emit event that matches filter
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        after: { title: 'Published Post', status: 'published' },
      }))

      // Emit event that doesn't match filter
      eventSource.emit(createTestEvent({
        target: 'posts:post2',
        after: { title: 'Draft Post', status: 'draft' },
      }))

      // Should only receive one change event
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(1)
      expect(changeMessages[0].data.entityId).toBe('post1')
    })

    it('filters events by operation', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        ops: ['CREATE'],
      })
      writer.clear()

      // Emit CREATE event
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        op: 'CREATE',
      }))

      // Emit UPDATE event
      eventSource.emit(createTestEvent({
        target: 'posts:post2',
        op: 'UPDATE',
        before: { title: 'Old' },
        after: { title: 'New' },
      }))

      // Should only receive CREATE event
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(1)
      expect(changeMessages[0].data.op).toBe('CREATE')
    })

    it('delivers to multiple subscribers', () => {
      const writer1 = new MockWriter()
      const writer2 = new MockWriter()

      const conn1 = manager.addConnection(writer1)
      const conn2 = manager.addConnection(writer2)

      manager.subscribe(conn1.id, { ns: 'posts' })
      manager.subscribe(conn2.id, { ns: 'posts' })

      writer1.clear()
      writer2.clear()

      // Emit an event
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
      }))

      // Both should receive
      expect(writer1.getMessagesOfType('change')).toHaveLength(1)
      expect(writer2.getMessagesOfType('change')).toHaveLength(1)
    })

    it('respects includeState option', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        includeState: false,
      })
      writer.clear()

      // Emit an event
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        before: { title: 'Before' },
        after: { title: 'After' },
      }))

      // Should receive event without state
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(1)
      expect(changeMessages[0].data.before).toBeUndefined()
      expect(changeMessages[0].data.after).toBeUndefined()
    })
  })

  describe('statistics', () => {
    it('tracks connection count', () => {
      const writer1 = new MockWriter()
      const writer2 = new MockWriter()

      manager.addConnection(writer1)
      expect(manager.getStats().activeConnections).toBe(1)

      manager.addConnection(writer2)
      expect(manager.getStats().activeConnections).toBe(2)
    })

    it('tracks subscription count', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      expect(manager.getStats().totalSubscriptions).toBe(1)

      manager.subscribe(connection.id, { ns: 'users' })
      expect(manager.getStats().totalSubscriptions).toBe(2)
    })

    it('tracks events processed and delivered', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })

      eventSource.emit(createTestEvent({ target: 'posts:post1' }))
      eventSource.emit(createTestEvent({ target: 'users:user1' })) // Different ns

      const stats = manager.getStats()
      expect(stats.eventsProcessed).toBe(2)
      expect(stats.eventsDelivered).toBe(1)
      expect(stats.eventsFiltered).toBe(1)
    })

    it('tracks subscriptions by namespace', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      manager.subscribe(connection.id, { ns: 'posts' })
      manager.subscribe(connection.id, { ns: 'users' })

      const stats = manager.getStats()
      expect(stats.subscriptionsByNs.posts).toBe(2)
      expect(stats.subscriptionsByNs.users).toBe(1)
    })
  })

  describe('heartbeat', () => {
    it('responds to ping', async () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)
      writer.clear()

      manager.handlePing(connection.id)

      // Should receive pong
      expect(writer.messages).toHaveLength(1)
      expect(writer.messages[0].type).toBe('pong')
      expect((writer.messages[0] as any).ts).toBeTypeOf('number')
    })

    it('removes connection after timeout', async () => {
      vi.useFakeTimers()
      const manager = createSubscriptionManager({
        connectionTimeoutMs: 100,
        heartbeatIntervalMs: 50,
      })

      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.startHeartbeat()

      // Advance time past timeout
      await vi.advanceTimersByTimeAsync(200)

      // Connection should be removed
      expect(manager.getConnection(connection.id)).toBeUndefined()

      manager.stopHeartbeat()
      vi.useRealTimers()
    })
  })

  describe('reconnection', () => {
    it('resumes connection with subscriptions', async () => {
      const writer1 = new MockWriter()
      const conn1 = manager.addConnection(writer1)

      const subId = manager.subscribe(conn1.id, {
        ns: 'posts',
        filter: { status: 'published' },
      })

      // Simulate reconnection
      const writer2 = new MockWriter()
      const result = await manager.resumeConnection(writer2, {
        connectionId: conn1.id,
        lastEventIds: { [subId!]: 'evt_123' },
        subscriptions: [
          {
            id: subId!,
            ns: 'posts',
            filter: { status: 'published' },
          },
        ],
      })

      expect(result.success).toBe(true)
      expect(result.resumedSubscriptions).toHaveLength(1)
      expect(result.failedSubscriptions).toHaveLength(0)

      // New connection should be active
      expect(manager.getConnection(result.connectionId)).toBeDefined()
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createSubscriptionManager', () => {
  it('creates a manager with default config', () => {
    const manager = createSubscriptionManager()
    expect(manager).toBeInstanceOf(SubscriptionManager)
  })

  it('creates a manager with custom config', () => {
    const manager = createSubscriptionManager({
      maxSubscriptionsPerConnection: 5,
      connectionTimeoutMs: 60000,
    })

    const writer = new MockWriter()
    const connection = manager.addConnection(writer)

    // Create 5 subscriptions (should work)
    for (let i = 0; i < 5; i++) {
      const subId = manager.subscribe(connection.id, { ns: `ns${i}` })
      expect(subId).not.toBeNull()
    }

    // 6th should fail
    const subId = manager.subscribe(connection.id, { ns: 'ns5' })
    expect(subId).toBeNull()
  })
})

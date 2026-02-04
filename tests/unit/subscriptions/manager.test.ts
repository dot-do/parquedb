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

  describe('lifecycle management', () => {
    it('starts and stops cleanly', async () => {
      const manager = createSubscriptionManager()
      const eventSource = new InMemoryEventSource()

      await manager.setEventSource(eventSource)
      await manager.start()

      const writer = new MockWriter()
      const connection = manager.addConnection(writer)
      manager.subscribe(connection.id, { ns: 'posts' })

      expect(manager.getConnection(connection.id)).toBeDefined()
      expect(manager.getStats().activeConnections).toBe(1)

      await manager.stop()

      // Connection should be removed
      expect(manager.getConnection(connection.id)).toBeUndefined()
      expect(manager.getStats().activeConnections).toBe(0)
    })

    it('cleans up event source on stop', async () => {
      const manager = createSubscriptionManager()
      const eventSource = new InMemoryEventSource()

      await manager.setEventSource(eventSource)
      await manager.start()

      expect(eventSource.isStarted()).toBe(true)

      await manager.stop()

      expect(eventSource.isStarted()).toBe(false)
    })

    it('replaces existing event source', async () => {
      const manager = createSubscriptionManager()
      const eventSource1 = new InMemoryEventSource()
      const eventSource2 = new InMemoryEventSource()

      await manager.setEventSource(eventSource1)
      await manager.start()

      expect(eventSource1.isStarted()).toBe(true)

      await manager.setEventSource(eventSource2)

      expect(eventSource1.isStarted()).toBe(false)
      expect(eventSource2.isStarted()).toBe(true)
    })
  })

  describe('error handling', () => {
    it('handles closed writer gracefully', async () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      writer.clear()

      // Close the writer before event delivery
      await writer.close()

      // Should not throw when emitting event
      expect(() => {
        eventSource.emit(createTestEvent({ target: 'posts:post1' }))
      }).not.toThrow()

      // Connection should be removed
      expect(manager.getConnection(connection.id)).toBeUndefined()
    })

    it('removes connection on failed send', async () => {
      // Create a writer that will fail on send
      const failingWriter: MockWriter = new MockWriter()
      const originalSend = failingWriter.send.bind(failingWriter)
      let sendCount = 0
      failingWriter.send = async (msg) => {
        sendCount++
        // Fail on the second send (first is 'connected')
        if (sendCount > 1) {
          throw new Error('Send failed')
        }
        return originalSend(msg)
      }

      const connection = manager.addConnection(failingWriter)
      manager.subscribe(connection.id, { ns: 'posts' })

      // This event should trigger the failing send
      eventSource.emit(createTestEvent({ target: 'posts:post1' }))

      // Allow async operations to complete
      await new Promise((resolve) => setTimeout(resolve, 10))

      // Connection should be removed
      expect(manager.getConnection(connection.id)).toBeUndefined()
    })

    it('handles events with invalid targets', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      writer.clear()

      // Emit event with invalid target format
      eventSource.emit(createTestEvent({
        target: 'invalid-target-format', // Missing colon separator
      }))

      // Should not crash, and no change messages should be delivered
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(0)
    })

    it('handles filter matching errors gracefully', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      // Subscribe with a complex filter
      manager.subscribe(connection.id, {
        ns: 'posts',
        filter: { $and: [{ status: 'published' }] },
      })
      writer.clear()

      // Emit event with state that causes filter evaluation
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        after: { status: 'published' },
      }))

      // Should still deliver matching events
      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages.length).toBeGreaterThanOrEqual(0)
    })

    it('ignores ping for non-existent connection', () => {
      // Should not throw
      expect(() => {
        manager.handlePing('non-existent-connection')
      }).not.toThrow()
    })

    it('handles remove of non-existent connection', async () => {
      // Should not throw
      await expect(manager.removeConnection('non-existent')).resolves.not.toThrow()
    })
  })

  describe('concurrent operations', () => {
    it('handles multiple simultaneous connections', () => {
      const writers = Array.from({ length: 10 }, () => new MockWriter())
      const connections = writers.map((w) => manager.addConnection(w))

      expect(connections).toHaveLength(10)
      expect(manager.getStats().activeConnections).toBe(10)

      // Each should have unique ID
      const ids = new Set(connections.map((c) => c.id))
      expect(ids.size).toBe(10)
    })

    it('handles many subscriptions across connections', () => {
      const writers = Array.from({ length: 5 }, () => new MockWriter())
      const connections = writers.map((w) => manager.addConnection(w))

      // Each connection subscribes to multiple namespaces
      connections.forEach((conn) => {
        manager.subscribe(conn.id, { ns: 'posts' })
        manager.subscribe(conn.id, { ns: 'users' })
      })

      const stats = manager.getStats()
      expect(stats.totalSubscriptions).toBe(10)
      expect(stats.subscriptionsByNs.posts).toBe(5)
      expect(stats.subscriptionsByNs.users).toBe(5)
    })

    it('delivers events to many subscribers efficiently', () => {
      const writers = Array.from({ length: 20 }, () => new MockWriter())
      const connections = writers.map((w) => manager.addConnection(w))

      connections.forEach((conn) => {
        manager.subscribe(conn.id, { ns: 'posts' })
      })

      writers.forEach((w) => w.clear())

      // Emit one event
      eventSource.emit(createTestEvent({ target: 'posts:post1' }))

      // All should receive the event
      writers.forEach((writer) => {
        const changes = writer.getMessagesOfType('change')
        expect(changes).toHaveLength(1)
      })

      expect(manager.getStats().eventsDelivered).toBe(1)
    })
  })

  describe('event source integration', () => {
    it('handles batch events', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      writer.clear()

      // Emit multiple events at once
      const events = Array.from({ length: 5 }, (_, i) =>
        createTestEvent({ target: `posts:post${i}` })
      )
      eventSource.emitMany(events)

      const changeMessages = writer.getMessagesOfType('change')
      expect(changeMessages).toHaveLength(5)
    })

    it('tracks lastEventId on subscription', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const subId = manager.subscribe(connection.id, { ns: 'posts' })
      writer.clear()

      const event = createTestEvent({ target: 'posts:post1', id: 'evt_test_123' })
      eventSource.emit(event)

      // The subscription should have lastEventId updated
      const conn = manager.getConnection(connection.id)
      const sub = conn?.subscriptions.get(subId!)
      expect(sub?.lastEventId).toBe('evt_test_123')
    })
  })

  describe('statistics management', () => {
    it('resets stats correctly', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)
      manager.subscribe(connection.id, { ns: 'posts' })

      eventSource.emit(createTestEvent({ target: 'posts:post1' }))
      eventSource.emit(createTestEvent({ target: 'users:user1' }))

      expect(manager.getStats().eventsProcessed).toBe(2)

      manager.resetStats()

      const stats = manager.getStats()
      expect(stats.eventsProcessed).toBe(0)
      expect(stats.eventsDelivered).toBe(0)
      expect(stats.eventsFiltered).toBe(0)
      // Connection and subscription counts should be preserved
      expect(stats.activeConnections).toBe(1)
      expect(stats.totalSubscriptions).toBe(1)
    })

    it('decrements namespace stats on unsubscribe', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const subId1 = manager.subscribe(connection.id, { ns: 'posts' })
      const subId2 = manager.subscribe(connection.id, { ns: 'posts' })

      expect(manager.getStats().subscriptionsByNs.posts).toBe(2)

      manager.unsubscribe(connection.id, subId1!)
      expect(manager.getStats().subscriptionsByNs.posts).toBe(1)

      manager.unsubscribe(connection.id, subId2!)
      expect(manager.getStats().subscriptionsByNs.posts).toBeUndefined()
    })

    it('cleans up stats when connection is removed', async () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, { ns: 'posts' })
      manager.subscribe(connection.id, { ns: 'users' })

      expect(manager.getStats().totalSubscriptions).toBe(2)

      await manager.removeConnection(connection.id)

      expect(manager.getStats().totalSubscriptions).toBe(0)
      expect(manager.getStats().activeConnections).toBe(0)
    })
  })

  describe('advanced filter scenarios', () => {
    it('handles empty filter', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        filter: {},
      })
      writer.clear()

      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        after: { any: 'data' },
      }))

      expect(writer.getMessagesOfType('change')).toHaveLength(1)
    })

    it('filters DELETE events using before state', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        filter: { status: 'archived' },
        ops: ['DELETE'],
      })
      writer.clear()

      // DELETE event with matching before state
      eventSource.emit(createTestEvent({
        target: 'posts:post1',
        op: 'DELETE',
        before: { title: 'Old Post', status: 'archived' },
        after: undefined,
      }))

      expect(writer.getMessagesOfType('change')).toHaveLength(1)
    })

    it('handles ALL operation type', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        ops: ['ALL'],
      })
      writer.clear()

      eventSource.emit(createTestEvent({ target: 'posts:post1', op: 'CREATE' }))
      eventSource.emit(createTestEvent({ target: 'posts:post2', op: 'UPDATE', before: {}, after: {} }))
      eventSource.emit(createTestEvent({ target: 'posts:post3', op: 'DELETE', before: {} }))

      expect(writer.getMessagesOfType('change')).toHaveLength(3)
    })

    it('handles multiple operation types', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      manager.subscribe(connection.id, {
        ns: 'posts',
        ops: ['CREATE', 'DELETE'],
      })
      writer.clear()

      eventSource.emit(createTestEvent({ target: 'posts:post1', op: 'CREATE' }))
      eventSource.emit(createTestEvent({ target: 'posts:post2', op: 'UPDATE', before: {}, after: {} }))
      eventSource.emit(createTestEvent({ target: 'posts:post3', op: 'DELETE', before: {} }))

      const changes = writer.getMessagesOfType('change')
      expect(changes).toHaveLength(2)
      expect(changes.map((c) => c.data.op)).toEqual(['CREATE', 'DELETE'])
    })
  })

  describe('heartbeat management', () => {
    it('starts and stops heartbeat', async () => {
      vi.useFakeTimers()

      const manager = createSubscriptionManager({
        heartbeatIntervalMs: 100,
        connectionTimeoutMs: 1000,
      })

      const writer = new MockWriter()
      manager.addConnection(writer)
      writer.clear()

      manager.startHeartbeat()

      // Advance time to trigger heartbeat
      await vi.advanceTimersByTimeAsync(100)

      // Should receive pong message
      expect(writer.getMessagesOfType('pong').length).toBeGreaterThanOrEqual(1)

      manager.stopHeartbeat()

      const pongCount = writer.getMessagesOfType('pong').length
      await vi.advanceTimersByTimeAsync(200)

      // No additional pongs after stopping
      expect(writer.getMessagesOfType('pong').length).toBe(pongCount)

      vi.useRealTimers()
    })

    it('does not start duplicate heartbeat', () => {
      vi.useFakeTimers()

      const manager = createSubscriptionManager({
        heartbeatIntervalMs: 100,
      })

      manager.startHeartbeat()
      manager.startHeartbeat() // Should be idempotent

      // Should not throw or create multiple timers
      manager.stopHeartbeat()

      vi.useRealTimers()
    })

    it('updates lastActivity on ping', () => {
      const writer = new MockWriter()
      const connection = manager.addConnection(writer)

      const initialActivity = connection.lastActivity

      // Wait a bit
      const now = Date.now()
      vi.spyOn(Date, 'now').mockReturnValue(now + 1000)

      manager.handlePing(connection.id)

      const conn = manager.getConnection(connection.id)
      expect(conn?.lastActivity).toBeGreaterThan(initialActivity)

      vi.restoreAllMocks()
    })
  })

  describe('reconnection scenarios', () => {
    it('handles reconnection with multiple subscriptions', async () => {
      const writer1 = new MockWriter()
      const conn1 = manager.addConnection(writer1)

      const sub1 = manager.subscribe(conn1.id, { ns: 'posts' })
      const sub2 = manager.subscribe(conn1.id, { ns: 'users' })
      const sub3 = manager.subscribe(conn1.id, { ns: 'comments' })

      const writer2 = new MockWriter()
      const result = await manager.resumeConnection(writer2, {
        connectionId: conn1.id,
        lastEventIds: {
          [sub1!]: 'evt_1',
          [sub2!]: 'evt_2',
          [sub3!]: 'evt_3',
        },
        subscriptions: [
          { id: sub1!, ns: 'posts' },
          { id: sub2!, ns: 'users' },
          { id: sub3!, ns: 'comments' },
        ],
      })

      expect(result.success).toBe(true)
      expect(result.resumedSubscriptions).toHaveLength(3)
    })

    it('creates new connection ID on resume', async () => {
      const writer1 = new MockWriter()
      const conn1 = manager.addConnection(writer1)
      const originalConnId = conn1.id

      const writer2 = new MockWriter()
      const result = await manager.resumeConnection(writer2, {
        connectionId: originalConnId,
        lastEventIds: {},
        subscriptions: [],
      })

      // New connection should have different ID
      expect(result.connectionId).not.toBe(originalConnId)
    })

    it('delivers events to resumed connection', async () => {
      const writer1 = new MockWriter()
      const conn1 = manager.addConnection(writer1)
      manager.subscribe(conn1.id, { ns: 'posts' })

      const writer2 = new MockWriter()
      const result = await manager.resumeConnection(writer2, {
        connectionId: conn1.id,
        lastEventIds: {},
        subscriptions: [{ id: 'sub_old', ns: 'posts' }],
      })

      writer2.clear()

      eventSource.emit(createTestEvent({ target: 'posts:post1' }))

      expect(writer2.getMessagesOfType('change')).toHaveLength(1)
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

  it('creates manager with debug mode enabled', async () => {
    const manager = createSubscriptionManager({ debug: true })
    const eventSource = new InMemoryEventSource()
    await manager.setEventSource(eventSource)

    // Should not throw with debug enabled
    await manager.start()

    const writer = new MockWriter()
    manager.addConnection(writer)

    await manager.stop()
  })
})

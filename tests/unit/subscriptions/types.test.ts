/**
 * Subscription Types Test Suite
 *
 * Tests for type exports and default configuration values.
 */

import { describe, it, expect } from 'vitest'
import {
  DEFAULT_SUBSCRIPTION_CONFIG,
} from '@/subscriptions'
import type {
  SubscriptionOp,
  ChangeEvent,
  SubscriptionControlMessage,
  SubscriptionMessage,
  SubscriptionOptions,
  Subscription,
  Connection,
  SubscriptionManagerConfig,
  SubscriptionWriter,
  SubscriptionEventSource,
  ReconnectionState,
  ResumeResult,
  SubscriptionStats,
} from '@/subscriptions'

// =============================================================================
// Default Configuration Tests
// =============================================================================

describe('DEFAULT_SUBSCRIPTION_CONFIG', () => {
  it('has maxSubscriptionsPerConnection set', () => {
    expect(DEFAULT_SUBSCRIPTION_CONFIG.maxSubscriptionsPerConnection).toBe(10)
  })

  it('has connectionTimeoutMs set', () => {
    expect(DEFAULT_SUBSCRIPTION_CONFIG.connectionTimeoutMs).toBe(30000)
  })

  it('has heartbeatIntervalMs set', () => {
    expect(DEFAULT_SUBSCRIPTION_CONFIG.heartbeatIntervalMs).toBe(15000)
  })

  it('has maxPendingEvents set', () => {
    expect(DEFAULT_SUBSCRIPTION_CONFIG.maxPendingEvents).toBe(1000)
  })

  it('has debug disabled by default', () => {
    expect(DEFAULT_SUBSCRIPTION_CONFIG.debug).toBe(false)
  })

  it('is a complete Required<SubscriptionManagerConfig>', () => {
    // All fields should be defined (Required type)
    const keys = Object.keys(DEFAULT_SUBSCRIPTION_CONFIG)
    expect(keys).toContain('maxSubscriptionsPerConnection')
    expect(keys).toContain('connectionTimeoutMs')
    expect(keys).toContain('heartbeatIntervalMs')
    expect(keys).toContain('maxPendingEvents')
    expect(keys).toContain('debug')
    expect(keys.length).toBe(5)
  })
})

// =============================================================================
// Type Shape Verification Tests
// =============================================================================

describe('type shapes', () => {
  describe('SubscriptionOp', () => {
    it('allows valid operation types', () => {
      const ops: SubscriptionOp[] = ['CREATE', 'UPDATE', 'DELETE', 'ALL']
      expect(ops).toHaveLength(4)
    })
  })

  describe('ChangeEvent', () => {
    it('has correct shape', () => {
      const event: ChangeEvent = {
        id: 'evt_123',
        ts: Date.now(),
        op: 'CREATE',
        ns: 'posts',
        entityId: 'post1',
        fullId: 'posts/post1' as any,
      }

      expect(event.id).toBeDefined()
      expect(event.ts).toBeTypeOf('number')
      expect(event.op).toBeDefined()
      expect(event.ns).toBeDefined()
      expect(event.entityId).toBeDefined()
      expect(event.fullId).toBeDefined()
    })

    it('allows optional fields', () => {
      const event: ChangeEvent = {
        id: 'evt_123',
        ts: Date.now(),
        op: 'UPDATE',
        ns: 'posts',
        entityId: 'post1',
        fullId: 'posts/post1' as any,
        before: { title: 'Old' },
        after: { title: 'New' },
        actor: 'users/admin',
        metadata: { source: 'api' },
      }

      expect(event.before).toEqual({ title: 'Old' })
      expect(event.after).toEqual({ title: 'New' })
      expect(event.actor).toBe('users/admin')
      expect(event.metadata).toEqual({ source: 'api' })
    })
  })

  describe('SubscriptionControlMessage', () => {
    it('allows all control message types', () => {
      const types: SubscriptionControlMessage['type'][] = [
        'subscribe',
        'unsubscribe',
        'ping',
        'pong',
        'error',
        'ack',
      ]
      expect(types).toHaveLength(6)
    })
  })

  describe('SubscriptionMessage', () => {
    it('discriminates message types correctly', () => {
      const messages: SubscriptionMessage[] = [
        { type: 'change', data: { id: 'evt1', ts: 123, op: 'CREATE', ns: 'posts', entityId: 'p1', fullId: 'posts/p1' as any } },
        { type: 'subscribed', subscriptionId: 'sub1', ns: 'posts' },
        { type: 'unsubscribed', subscriptionId: 'sub1' },
        { type: 'error', error: 'Something went wrong' },
        { type: 'error', error: 'With code', code: 'ERR_CODE' },
        { type: 'pong', ts: 12345 },
        { type: 'connected', connectionId: 'conn1' },
      ]

      expect(messages).toHaveLength(7)
    })
  })

  describe('SubscriptionOptions', () => {
    it('requires ns field', () => {
      const opts: SubscriptionOptions = { ns: 'posts' }
      expect(opts.ns).toBe('posts')
    })

    it('allows optional fields', () => {
      const opts: SubscriptionOptions = {
        ns: 'posts',
        filter: { status: 'published' },
        ops: ['CREATE', 'UPDATE'],
        includeState: true,
        resumeAfter: 'evt_123',
        maxEventsPerSecond: 100,
      }

      expect(opts.filter).toEqual({ status: 'published' })
      expect(opts.ops).toEqual(['CREATE', 'UPDATE'])
      expect(opts.includeState).toBe(true)
      expect(opts.resumeAfter).toBe('evt_123')
      expect(opts.maxEventsPerSecond).toBe(100)
    })
  })

  describe('Subscription', () => {
    it('has all required fields', () => {
      const sub: Subscription = {
        id: 'sub_123',
        connectionId: 'conn_123',
        ns: 'posts',
        filter: {},
        ops: ['ALL'],
        includeState: true,
        createdAt: Date.now(),
      }

      expect(sub.id).toBeDefined()
      expect(sub.connectionId).toBeDefined()
      expect(sub.ns).toBeDefined()
      expect(sub.filter).toBeDefined()
      expect(sub.ops).toBeDefined()
      expect(sub.includeState).toBeDefined()
      expect(sub.createdAt).toBeTypeOf('number')
    })
  })

  describe('Connection', () => {
    it('has all required fields', () => {
      const mockWriter: SubscriptionWriter = {
        send: async () => {},
        close: async () => {},
        isOpen: () => true,
      }

      const conn: Connection = {
        id: 'conn_123',
        subscriptions: new Map(),
        lastActivity: Date.now(),
        writer: mockWriter,
      }

      expect(conn.id).toBeDefined()
      expect(conn.subscriptions).toBeInstanceOf(Map)
      expect(conn.lastActivity).toBeTypeOf('number')
      expect(conn.writer).toBeDefined()
    })
  })

  describe('ReconnectionState', () => {
    it('has all required fields', () => {
      const state: ReconnectionState = {
        connectionId: 'conn_123',
        lastEventIds: { sub1: 'evt_100', sub2: 'evt_200' },
        subscriptions: [
          { id: 'sub1', ns: 'posts' },
          { id: 'sub2', ns: 'users', filter: { active: true }, ops: ['UPDATE'] },
        ],
      }

      expect(state.connectionId).toBeDefined()
      expect(state.lastEventIds).toHaveProperty('sub1')
      expect(state.subscriptions).toHaveLength(2)
    })
  })

  describe('ResumeResult', () => {
    it('has all required fields for successful resume', () => {
      const result: ResumeResult = {
        success: true,
        connectionId: 'conn_new',
        resumedSubscriptions: ['sub1', 'sub2'],
        failedSubscriptions: [],
      }

      expect(result.success).toBe(true)
      expect(result.connectionId).toBeDefined()
      expect(result.resumedSubscriptions).toHaveLength(2)
      expect(result.failedSubscriptions).toHaveLength(0)
    })

    it('supports failed subscriptions', () => {
      const result: ResumeResult = {
        success: false,
        connectionId: 'conn_new',
        resumedSubscriptions: ['sub1'],
        failedSubscriptions: [
          { id: 'sub2', reason: 'Namespace not found' },
        ],
        missedEvents: [
          { id: 'evt1', ts: 123, op: 'CREATE', ns: 'posts', entityId: 'p1', fullId: 'posts/p1' as any },
        ],
      }

      expect(result.success).toBe(false)
      expect(result.failedSubscriptions).toHaveLength(1)
      expect(result.missedEvents).toHaveLength(1)
    })
  })

  describe('SubscriptionStats', () => {
    it('has all required fields', () => {
      const stats: SubscriptionStats = {
        activeConnections: 5,
        totalSubscriptions: 15,
        eventsProcessed: 1000,
        eventsDelivered: 800,
        eventsFiltered: 200,
        queueDepth: 10,
        subscriptionsByNs: {
          posts: 8,
          users: 5,
          comments: 2,
        },
      }

      expect(stats.activeConnections).toBe(5)
      expect(stats.totalSubscriptions).toBe(15)
      expect(stats.eventsProcessed).toBe(1000)
      expect(stats.eventsDelivered).toBe(800)
      expect(stats.eventsFiltered).toBe(200)
      expect(stats.queueDepth).toBe(10)
      expect(stats.subscriptionsByNs.posts).toBe(8)
    })
  })
})

// =============================================================================
// SubscriptionWriter Interface Tests
// =============================================================================

describe('SubscriptionWriter interface', () => {
  it('can be implemented', async () => {
    const messages: SubscriptionMessage[] = []
    let open = true

    const writer: SubscriptionWriter = {
      send: async (msg) => {
        messages.push(msg)
      },
      close: async () => {
        open = false
      },
      isOpen: () => open,
    }

    expect(writer.isOpen()).toBe(true)

    await writer.send({ type: 'connected', connectionId: 'conn1' })
    expect(messages).toHaveLength(1)

    await writer.close()
    expect(writer.isOpen()).toBe(false)
  })
})

// =============================================================================
// SubscriptionEventSource Interface Tests
// =============================================================================

describe('SubscriptionEventSource interface', () => {
  it('can be implemented', async () => {
    let started = false
    const handlers: Array<(event: any) => void> = []

    const source: SubscriptionEventSource = {
      onEvent: (handler) => {
        handlers.push(handler)
        return () => {
          const idx = handlers.indexOf(handler)
          if (idx >= 0) handlers.splice(idx, 1)
        }
      },
      start: async () => {
        started = true
      },
      stop: async () => {
        started = false
      },
    }

    // Register handler
    const received: any[] = []
    const unsubscribe = source.onEvent((evt) => received.push(evt))

    // Start source
    await source.start()
    expect(started).toBe(true)

    // Emit event (simulated)
    handlers.forEach((h) => h({ id: 'evt1' }))
    expect(received).toHaveLength(1)

    // Unsubscribe
    unsubscribe()
    handlers.forEach((h) => h({ id: 'evt2' }))
    expect(received).toHaveLength(1) // Still 1

    // Stop source
    await source.stop()
    expect(started).toBe(false)
  })
})

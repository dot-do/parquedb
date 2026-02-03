/**
 * Backpressure Handling Tests
 *
 * Tests for event buffer backpressure in EventWalManager and RelationshipWalManager.
 *
 * Backpressure prevents unbounded memory growth under sustained write load by:
 * - Pausing new writes when buffer exceeds configurable thresholds
 * - Tracking pending flush promises
 * - Providing feedback to callers via Promises
 * - Releasing backpressure when buffer drops below release threshold
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { EventWalManager } from '../../../src/worker/do/event-wal'
import { RelationshipWalManager } from '../../../src/worker/do/relationship-wal'
import {
  DEFAULT_BACKPRESSURE_CONFIG,
  BackpressureTimeoutError,
  type BackpressureConfig,
  type BackpressureState,
} from '../../../src/worker/do/types'
import type { Event } from '../../../src/types'

// =============================================================================
// Mock SQLite Storage
// =============================================================================

interface SqlOperation {
  type: 'insert' | 'update' | 'delete' | 'select'
  table: string
  params: unknown[]
}

class MockSqlStorage {
  private tables: Map<string, unknown[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()
  operations: SqlOperation[] = []

  constructor() {
    // Initialize tables
    this.tables.set('events_wal', [])
    this.tables.set('rels_wal', [])
  }

  exec<T = unknown>(query: string, ...params: unknown[]): Iterable<T> {
    const trimmedQuery = query.trim().toLowerCase()

    // CREATE TABLE
    if (trimmedQuery.startsWith('create table')) {
      return [] as T[]
    }

    // CREATE INDEX
    if (trimmedQuery.startsWith('create index')) {
      return [] as T[]
    }

    // INSERT
    if (trimmedQuery.startsWith('insert into')) {
      const match = query.match(/insert into (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        const id = (this.autoIncrement.get(tableName) || 0) + 1
        this.autoIncrement.set(tableName, id)

        this.operations.push({ type: 'insert', table: tableName, params })

        let row: Record<string, unknown>
        if (tableName === 'events_wal' || tableName === 'rels_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
            created_at: params[4] || new Date().toISOString(),
          }
        } else {
          row = { id, ...Object.fromEntries(params.map((p, i) => [`param${i}`, p])) }
        }

        rows.push(row)
        this.tables.set(tableName, rows)
      }
      return [] as T[]
    }

    // SELECT COUNT(*)
    if (trimmedQuery.includes('count(*)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        return [{ count: rows.length }] as T[]
      }
    }

    // SELECT SUM
    if (trimmedQuery.includes('sum(')) {
      return [{ total: 0 }] as T[]
    }

    // SELECT MAX(last_seq)
    if (trimmedQuery.includes('max(last_seq)')) {
      return [] as T[]
    }

    // SELECT events
    if (trimmedQuery.includes('select') && trimmedQuery.includes('events')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        if (params.length > 0) {
          const ns = params[0] as string
          return rows.filter((r: any) => r.ns === ns) as T[]
        }
        return rows as T[]
      }
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        if (params.length === 2) {
          const ns = params[0] as string
          const upToSeq = params[1] as number
          const remaining = rows.filter(
            (r: any) => r.ns !== ns || r.last_seq > upToSeq
          )
          this.tables.set(tableName, remaining)
        }
      }
      return [] as T[]
    }

    return [] as T[]
  }

  getTable(name: string): unknown[] {
    return this.tables.get(name) || []
  }

  clear() {
    this.tables.clear()
    this.autoIncrement.clear()
    this.operations = []
    this.tables.set('events_wal', [])
    this.tables.set('rels_wal', [])
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEvent(id: string, ns: string, entityId: string): Event {
  return {
    id,
    ts: Date.now(),
    op: 'CREATE',
    target: `${ns}:${entityId}`,
    after: { $type: 'Test', name: 'Test Entity' },
    actor: 'test',
  }
}

function createLargeEvent(id: string, ns: string, entityId: string, sizeBytes: number): Event {
  // Create an event with approximately the specified size
  const padding = 'x'.repeat(Math.max(0, sizeBytes - 100))
  return {
    id,
    ts: Date.now(),
    op: 'CREATE',
    target: `${ns}:${entityId}`,
    after: { $type: 'Test', name: 'Test Entity', data: padding },
    actor: 'test',
  }
}

// =============================================================================
// EventWalManager Backpressure Tests
// =============================================================================

describe('EventWalManager Backpressure', () => {
  let sql: MockSqlStorage
  let counters: Map<string, number>

  beforeEach(() => {
    sql = new MockSqlStorage()
    counters = new Map()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('Default Configuration', () => {
    it('should use default backpressure configuration', () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters)
      const config = manager.getBackpressureConfig()

      expect(config.maxBufferSizeBytes).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBufferSizeBytes)
      expect(config.maxBufferEventCount).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBufferEventCount)
      expect(config.maxPendingFlushes).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxPendingFlushes)
      expect(config.releaseThreshold).toBe(DEFAULT_BACKPRESSURE_CONFIG.releaseThreshold)
      expect(config.timeoutMs).toBe(DEFAULT_BACKPRESSURE_CONFIG.timeoutMs)
    })

    it('should allow custom backpressure configuration', () => {
      const customConfig: Partial<BackpressureConfig> = {
        maxBufferSizeBytes: 512 * 1024,
        maxBufferEventCount: 500,
        maxPendingFlushes: 5,
        releaseThreshold: 0.7,
        timeoutMs: 60000,
      }

      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, customConfig)
      const config = manager.getBackpressureConfig()

      expect(config.maxBufferSizeBytes).toBe(512 * 1024)
      expect(config.maxBufferEventCount).toBe(500)
      expect(config.maxPendingFlushes).toBe(5)
      expect(config.releaseThreshold).toBe(0.7)
      expect(config.timeoutMs).toBe(60000)
    })

    it('should allow updating backpressure configuration', () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters)

      manager.setBackpressureConfig({ maxBufferEventCount: 200 })
      const config = manager.getBackpressureConfig()

      expect(config.maxBufferEventCount).toBe(200)
      // Other values should remain default
      expect(config.maxBufferSizeBytes).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBufferSizeBytes)
    })
  })

  describe('Backpressure State Monitoring', () => {
    it('should report initial state as inactive', () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters)
      const state = manager.getBackpressureState()

      expect(state.active).toBe(false)
      expect(state.currentBufferSizeBytes).toBe(0)
      expect(state.currentEventCount).toBe(0)
      expect(state.pendingFlushCount).toBe(0)
      expect(state.backpressureEvents).toBe(0)
      expect(state.totalWaitTimeMs).toBe(0)
      expect(state.lastBackpressureAt).toBeNull()
    })

    it('should track buffer size and event count', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 1000, // High threshold to prevent auto-flush
      })

      const event = createTestEvent('evt1', 'posts', 'p1')
      await manager.appendEvent(event)

      const state = manager.getBackpressureState()
      expect(state.currentEventCount).toBe(1)
      expect(state.currentBufferSizeBytes).toBeGreaterThan(0)
    })
  })

  describe('Event Count Threshold', () => {
    it('should apply backpressure when event count exceeds threshold', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 5,
        maxBufferSizeBytes: 10 * 1024 * 1024, // High size limit
        timeoutMs: 100,
      })

      // Add events up to threshold using namespace-based buffering
      for (let i = 0; i < 5; i++) {
        await manager.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:p${i}`,
          after: { $type: 'Post', name: `Post ${i}` },
          actor: 'test',
        })
      }

      const state = manager.getBackpressureState()
      expect(state.active).toBe(true)
      expect(state.backpressureEvents).toBe(1)
    })

    it('should release backpressure after flush', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 5,
        maxBufferSizeBytes: 10 * 1024 * 1024,
        releaseThreshold: 0.8,
        timeoutMs: 100,
      })

      // Add events to trigger backpressure using namespace-based buffering
      for (let i = 0; i < 5; i++) {
        await manager.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:p${i}`,
          after: { $type: 'Post', name: `Post ${i}` },
          actor: 'test',
        })
      }

      expect(manager.getBackpressureState().active).toBe(true)

      // Flush to release backpressure
      await manager.flushNsEventBatch('posts')

      // After flush, namespace buffer should be empty
      const stateAfter = manager.getBackpressureState()
      expect(stateAfter.currentEventCount).toBe(0)
      expect(stateAfter.active).toBe(false)
    })
  })

  describe('Buffer Size Threshold', () => {
    it('should apply backpressure when buffer size exceeds threshold', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferSizeBytes: 1000, // Small threshold for testing
        maxBufferEventCount: 10000, // High count limit
        timeoutMs: 100,
      })

      // Add a large event that exceeds the size threshold
      const largeEvent = createLargeEvent('evt1', 'posts', 'p1', 1500)
      await manager.appendEvent(largeEvent)

      const state = manager.getBackpressureState()
      expect(state.active).toBe(true)
      expect(state.currentBufferSizeBytes).toBeGreaterThan(1000)
    })
  })

  describe('Timeout Handling', () => {
    it('should throw BackpressureTimeoutError when timeout is exceeded', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 2,
        timeoutMs: 50,
      })

      // Add events to trigger backpressure
      for (let i = 0; i < 2; i++) {
        await manager.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      }

      expect(manager.getBackpressureState().active).toBe(true)

      // Try to add another event which should wait for backpressure
      const appendPromise = manager.appendEvent(createTestEvent('evt2', 'posts', 'p2'))

      // Advance timer past timeout
      vi.advanceTimersByTime(100)

      await expect(appendPromise).rejects.toThrow(BackpressureTimeoutError)
    })

    it('should not timeout when timeout is set to Infinity', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 2,
        timeoutMs: Infinity,
      })

      // Add events to trigger backpressure
      for (let i = 0; i < 2; i++) {
        await manager.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      }

      expect(manager.getBackpressureState().active).toBe(true)

      // Start append (will wait for backpressure)
      const appendPromise = manager.appendEvent(createTestEvent('evt2', 'posts', 'p2'))
      let resolved = false
      appendPromise.then(() => {
        resolved = true
      })

      // Advance timer significantly
      vi.advanceTimersByTime(1000000)

      // Should still be waiting (not resolved, not rejected)
      expect(resolved).toBe(false)

      // Force release to complete the test
      manager.forceReleaseBackpressure()
      await appendPromise
      expect(resolved).toBe(true)
    })
  })

  describe('Statistics Tracking', () => {
    it('should track backpressure events', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 3,
        timeoutMs: 100,
      })

      // Trigger backpressure multiple times
      for (let i = 0; i < 3; i++) {
        await manager.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      }

      const state = manager.getBackpressureState()
      expect(state.backpressureEvents).toBe(1)
      expect(state.lastBackpressureAt).not.toBeNull()
    })

    it('should reset backpressure statistics', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 3,
        timeoutMs: 100,
      })

      for (let i = 0; i < 3; i++) {
        await manager.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      }

      expect(manager.getBackpressureState().backpressureEvents).toBe(1)

      manager.resetBackpressureStats()

      const state = manager.getBackpressureState()
      expect(state.backpressureEvents).toBe(0)
      expect(state.totalWaitTimeMs).toBe(0)
      expect(state.lastBackpressureAt).toBeNull()
    })
  })

  describe('Force Release', () => {
    it('should force release backpressure', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 3,
        timeoutMs: 100,
      })

      for (let i = 0; i < 3; i++) {
        await manager.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      }

      expect(manager.getBackpressureState().active).toBe(true)

      manager.forceReleaseBackpressure()

      expect(manager.getBackpressureState().active).toBe(false)
    })
  })

  describe('Namespace Event Buffers', () => {
    it('should apply backpressure across multiple namespaces', async () => {
      const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 5,
        timeoutMs: 100,
      })

      // Add events to different namespaces
      for (let i = 0; i < 3; i++) {
        await manager.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:p${i}`,
          after: { $type: 'Post' },
          actor: 'test',
        })
      }

      for (let i = 0; i < 2; i++) {
        await manager.appendEventWithSeq('users', {
          ts: Date.now(),
          op: 'CREATE',
          target: `users:u${i}`,
          after: { $type: 'User' },
          actor: 'test',
        })
      }

      const state = manager.getBackpressureState()
      expect(state.currentEventCount).toBe(5)
      expect(state.active).toBe(true)
    })
  })
})

// =============================================================================
// RelationshipWalManager Backpressure Tests
// =============================================================================

describe('RelationshipWalManager Backpressure', () => {
  let sql: MockSqlStorage
  let counters: Map<string, number>

  beforeEach(() => {
    sql = new MockSqlStorage()
    counters = new Map()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('Default Configuration', () => {
    it('should use default backpressure configuration', () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters)
      const config = manager.getBackpressureConfig()

      expect(config.maxBufferSizeBytes).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBufferSizeBytes)
      expect(config.maxBufferEventCount).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxBufferEventCount)
      expect(config.maxPendingFlushes).toBe(DEFAULT_BACKPRESSURE_CONFIG.maxPendingFlushes)
    })

    it('should allow custom backpressure configuration', () => {
      const customConfig: Partial<BackpressureConfig> = {
        maxBufferEventCount: 250,
        releaseThreshold: 0.6,
      }

      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, customConfig)
      const config = manager.getBackpressureConfig()

      expect(config.maxBufferEventCount).toBe(250)
      expect(config.releaseThreshold).toBe(0.6)
    })
  })

  describe('Backpressure State Monitoring', () => {
    it('should report initial state as inactive', () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters)
      const state = manager.getBackpressureState()

      expect(state.active).toBe(false)
      expect(state.currentBufferSizeBytes).toBe(0)
      expect(state.currentEventCount).toBe(0)
    })

    it('should track buffer state after adding events', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 1000,
      })

      await manager.appendRelEvent('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'rel:posts:p1->author->users:u1',
        after: { predicate: 'author' },
        actor: 'test',
      })

      const state = manager.getBackpressureState()
      expect(state.currentEventCount).toBe(1)
      expect(state.currentBufferSizeBytes).toBeGreaterThan(0)
    })
  })

  describe('Event Count Threshold', () => {
    it('should apply backpressure when event count exceeds threshold', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 3,
        maxBufferSizeBytes: 10 * 1024 * 1024,
        timeoutMs: 100,
      })

      for (let i = 0; i < 3; i++) {
        await manager.appendRelEvent('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `rel:posts:p${i}->author->users:u${i}`,
          after: { predicate: 'author' },
          actor: 'test',
        })
      }

      const state = manager.getBackpressureState()
      expect(state.active).toBe(true)
      expect(state.backpressureEvents).toBe(1)
    })

    it('should release backpressure after flush', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 3,
        releaseThreshold: 0.8,
        timeoutMs: 100,
      })

      for (let i = 0; i < 3; i++) {
        await manager.appendRelEvent('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `rel:posts:p${i}->author->users:u${i}`,
          after: { predicate: 'author' },
          actor: 'test',
        })
      }

      expect(manager.getBackpressureState().active).toBe(true)

      await manager.flushRelEventBatch('posts')

      const stateAfter = manager.getBackpressureState()
      expect(stateAfter.currentEventCount).toBe(0)
      expect(stateAfter.active).toBe(false)
    })
  })

  describe('Timeout Handling', () => {
    it('should throw BackpressureTimeoutError when timeout is exceeded', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 2,
        timeoutMs: 50,
      })

      for (let i = 0; i < 2; i++) {
        await manager.appendRelEvent('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `rel:posts:p${i}->author->users:u${i}`,
          after: { predicate: 'author' },
          actor: 'test',
        })
      }

      expect(manager.getBackpressureState().active).toBe(true)

      const appendPromise = manager.appendRelEvent('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'rel:posts:p2->author->users:u2',
        after: { predicate: 'author' },
        actor: 'test',
      })

      vi.advanceTimersByTime(100)

      await expect(appendPromise).rejects.toThrow(BackpressureTimeoutError)
    })
  })

  describe('Force Release', () => {
    it('should force release backpressure', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 2,
        timeoutMs: 100,
      })

      for (let i = 0; i < 2; i++) {
        await manager.appendRelEvent('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `rel:posts:p${i}->author->users:u${i}`,
          after: { predicate: 'author' },
          actor: 'test',
        })
      }

      expect(manager.getBackpressureState().active).toBe(true)

      manager.forceReleaseBackpressure()

      expect(manager.getBackpressureState().active).toBe(false)
    })
  })

  describe('Transaction Rollback', () => {
    it('should check backpressure after restoring buffers', async () => {
      const manager = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
        maxBufferEventCount: 5,
        timeoutMs: 100,
      })

      // Create a buffer map that exceeds threshold
      const largeBuffers = new Map()
      largeBuffers.set('posts', {
        events: Array(6).fill({ id: 'evt', ts: Date.now(), op: 'CREATE', target: 'test', actor: 'test' }),
        firstSeq: 1,
        lastSeq: 7,
        sizeBytes: 1000,
      })

      manager.setRelEventBuffers(largeBuffers)

      const state = manager.getBackpressureState()
      expect(state.active).toBe(true)
      expect(state.currentEventCount).toBe(6)
    })
  })
})

// =============================================================================
// BackpressureTimeoutError Tests
// =============================================================================

describe('BackpressureTimeoutError', () => {
  it('should include state information in error', () => {
    const state: BackpressureState = {
      active: true,
      currentBufferSizeBytes: 512000,
      currentEventCount: 500,
      pendingFlushCount: 2,
      backpressureEvents: 3,
      totalWaitTimeMs: 1500,
      lastBackpressureAt: Date.now(),
    }

    const error = new BackpressureTimeoutError(30000, state)

    expect(error.name).toBe('BackpressureTimeoutError')
    expect(error.state).toBe(state)
    expect(error.message).toContain('30000ms')
    expect(error.message).toContain('512000')
    expect(error.message).toContain('500')
    expect(error.message).toContain('2')
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Backpressure Integration', () => {
  let sql: MockSqlStorage
  let counters: Map<string, number>

  beforeEach(() => {
    sql = new MockSqlStorage()
    counters = new Map()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should handle concurrent event and relationship writes with backpressure', async () => {
    const eventWal = new EventWalManager(sql as unknown as SqlStorage, counters, {
      maxBufferEventCount: 10,
      timeoutMs: 100,
    })

    const relWal = new RelationshipWalManager(sql as unknown as SqlStorage, counters, {
      maxBufferEventCount: 10,
      timeoutMs: 100,
    })

    // Add events to both managers
    for (let i = 0; i < 5; i++) {
      await eventWal.appendEvent(createTestEvent(`evt${i}`, 'posts', `p${i}`))
      await relWal.appendRelEvent('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: `rel:posts:p${i}->author->users:u${i}`,
        after: { predicate: 'author' },
        actor: 'test',
      })
    }

    const eventState = eventWal.getBackpressureState()
    const relState = relWal.getBackpressureState()

    expect(eventState.currentEventCount).toBe(5)
    expect(relState.currentEventCount).toBe(5)

    // Neither should have triggered backpressure yet
    expect(eventState.active).toBe(false)
    expect(relState.active).toBe(false)
  })

  it('should recover after flushing under heavy load', async () => {
    vi.useRealTimers() // Use real timers for this test

    const manager = new EventWalManager(sql as unknown as SqlStorage, counters, {
      maxBufferEventCount: 10,
      releaseThreshold: 0.5,
      timeoutMs: 5000,
    })

    // Add events to trigger backpressure using namespace-based buffering
    for (let i = 0; i < 10; i++) {
      await manager.appendEventWithSeq('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: `posts:p${i}`,
        after: { $type: 'Post', name: `Post ${i}` },
        actor: 'test',
      })
    }

    expect(manager.getBackpressureState().active).toBe(true)

    // Flush to release backpressure
    await manager.flushNsEventBatch('posts')

    // Should be able to add more events now
    for (let i = 10; i < 15; i++) {
      await manager.appendEventWithSeq('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: `posts:p${i}`,
        after: { $type: 'Post', name: `Post ${i}` },
        actor: 'test',
      })
    }

    const state = manager.getBackpressureState()
    expect(state.currentEventCount).toBe(5)
    expect(state.active).toBe(false)
  })
})

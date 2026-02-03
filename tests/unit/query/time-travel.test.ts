/**
 * Time-Travel Query Tests
 *
 * Comprehensive tests for time-travel/snapshot queries, focusing on:
 * - Querying data at specific timestamps
 * - Event replay functionality
 * - Boundary conditions and edge cases
 * - Empty histories
 * - Complex temporal scenarios
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventReplayer,
  InMemoryEventSource,
  InMemorySnapshotStorage,
  replayEvents,
  replayEventsWithSnapshots,
  replayEventsBatch,
  getEntityHistory,
  createEventReplayer,
  createInMemoryEventSource,
  createInMemorySnapshotStorage,
} from '../../../src/events/replay'
import type { Event, Variant } from '../../../src/types/entity'
import { entityTarget } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create an event with reasonable defaults for testing
 */
function createEvent(
  target: string,
  op: 'CREATE' | 'UPDATE' | 'DELETE',
  ts: number,
  options: {
    before?: Variant
    after?: Variant
    id?: string
    actor?: string
    metadata?: Variant
  } = {}
): Event {
  return {
    id: options.id ?? `evt-${ts}-${Math.random().toString(36).slice(2, 8)}`,
    ts,
    op,
    target,
    before: options.before,
    after: options.after,
    actor: options.actor,
    metadata: options.metadata,
  }
}

/**
 * Generate a sequence of events for an entity from state transitions
 */
function createEntityLifecycle(
  ns: string,
  id: string,
  transitions: Array<{ op: 'CREATE' | 'UPDATE' | 'DELETE'; state?: Variant; ts: number }>
): Event[] {
  const target = entityTarget(ns, id)
  const events: Event[] = []
  let previousState: Variant | null = null

  for (const { op, state, ts } of transitions) {
    const event = createEvent(target, op, ts, {
      id: `evt-${id}-${ts}`,
      before: previousState ?? undefined,
      after: op === 'DELETE' ? undefined : state,
    })
    events.push(event)
    previousState = op === 'DELETE' ? null : (state ?? null)
  }

  return events
}

// =============================================================================
// Timestamp Boundary Tests
// =============================================================================

describe('Time-Travel: Timestamp Boundaries', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  describe('exact timestamp matches', () => {
    it('returns state at exact CREATE timestamp', async () => {
      const target = 'posts:p1'
      eventSource.addEvent(createEvent(target, 'CREATE', 1000, {
        after: { title: 'Created' },
      }))

      const result = await replayer.replayEntity(target, { at: 1000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Created' })
    })

    it('returns state at exact UPDATE timestamp', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, {
          before: { title: 'V1' },
          after: { title: 'V2' },
        }),
      ])

      const result = await replayer.replayEntity(target, { at: 2000 })

      expect(result.state).toEqual({ title: 'V2' })
    })

    it('returns null at exact DELETE timestamp', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'To Delete' } }),
        createEvent(target, 'DELETE', 2000, { before: { title: 'To Delete' } }),
      ])

      const result = await replayer.replayEntity(target, { at: 2000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })
  })

  describe('between-event timestamps', () => {
    it('returns previous state between CREATE and UPDATE', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 3000, {
          before: { title: 'V1' },
          after: { title: 'V2' },
        }),
      ])

      const result = await replayer.replayEntity(target, { at: 2000 })

      expect(result.state).toEqual({ title: 'V1' })
      expect(result.eventsReplayed).toBe(1)
    })

    it('returns last state between UPDATE and DELETE', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'Original' } }),
        createEvent(target, 'UPDATE', 2000, {
          before: { title: 'Original' },
          after: { title: 'Modified' },
        }),
        createEvent(target, 'DELETE', 4000, { before: { title: 'Modified' } }),
      ])

      const result = await replayer.replayEntity(target, { at: 3000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Modified' })
    })

    it('returns null between DELETE and re-CREATE', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'DELETE', 2000, { before: { title: 'V1' } }),
        createEvent(target, 'CREATE', 4000, { after: { title: 'V2' } }),
      ])

      const result = await replayer.replayEntity(target, { at: 3000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })
  })

  describe('pre-creation and post-deletion timestamps', () => {
    it('returns null at timestamp before entity creation', async () => {
      const target = 'posts:p1'
      eventSource.addEvent(createEvent(target, 'CREATE', 5000, {
        after: { title: 'Future Entity' },
      }))

      const result = await replayer.replayEntity(target, { at: 1000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(0)
    })

    it('returns null at timestamp far after deletion', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'Deleted' } }),
        createEvent(target, 'DELETE', 2000, { before: { title: 'Deleted' } }),
      ])

      const result = await replayer.replayEntity(target, { at: 1000000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })

    it('returns latest state at timestamp far in future', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, {
          before: { title: 'V1' },
          after: { title: 'Final' },
        }),
      ])

      const result = await replayer.replayEntity(target, { at: Number.MAX_SAFE_INTEGER })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Final' })
    })
  })

  describe('timestamp edge values', () => {
    it('handles timestamp of 0', async () => {
      const target = 'posts:p1'
      eventSource.addEvent(createEvent(target, 'CREATE', 1, {
        after: { title: 'Early' },
      }))

      const result = await replayer.replayEntity(target, { at: 0 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })

    it('handles timestamp of 1', async () => {
      const target = 'posts:p1'
      eventSource.addEvent(createEvent(target, 'CREATE', 1, {
        after: { title: 'Early' },
      }))

      const result = await replayer.replayEntity(target, { at: 1 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Early' })
    })

    it('handles very large timestamps', async () => {
      const target = 'posts:p1'
      const largeTs = Date.now() + 10 * 365 * 24 * 60 * 60 * 1000 // 10 years from now
      eventSource.addEvent(createEvent(target, 'CREATE', largeTs, {
        after: { title: 'Future' },
      }))

      const beforeResult = await replayer.replayEntity(target, { at: largeTs - 1 })
      expect(beforeResult.existed).toBe(false)

      const atResult = await replayer.replayEntity(target, { at: largeTs })
      expect(atResult.existed).toBe(true)
      expect(atResult.state).toEqual({ title: 'Future' })
    })
  })
})

// =============================================================================
// Empty History Tests
// =============================================================================

describe('Time-Travel: Empty Histories', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('returns not existed for completely empty event source', async () => {
    const result = await replayer.replayEntity('posts:p1', { at: Date.now() })

    expect(result.existed).toBe(false)
    expect(result.state).toBeNull()
    expect(result.eventsReplayed).toBe(0)
  })

  it('returns not existed for non-existent entity in populated source', async () => {
    eventSource.addEvent(createEvent('posts:other', 'CREATE', 1000, {
      after: { title: 'Other' },
    }))

    const result = await replayer.replayEntity('posts:nonexistent', { at: 2000 })

    expect(result.existed).toBe(false)
    expect(result.state).toBeNull()
    expect(result.eventsReplayed).toBe(0)
  })

  it('returns empty history for entity with no events', async () => {
    const history = await replayer.getStateHistory('posts:nonexistent')

    expect(history).toHaveLength(0)
  })

  it('handles batch replay with empty results', async () => {
    const results = await replayer.replayEntities(
      ['posts:a', 'posts:b', 'posts:c'],
      { at: 1000 }
    )

    expect(results.size).toBe(3)
    for (const result of results.values()) {
      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    }
  })

  it('returns empty for entity created after query timestamp', async () => {
    eventSource.addEvent(createEvent('posts:p1', 'CREATE', 5000, {
      after: { title: 'Future' },
    }))

    const result = await replayer.replayEntity('posts:p1', { at: 1000 })

    expect(result.existed).toBe(false)
    expect(result.state).toBeNull()
    expect(result.eventsReplayed).toBe(0)
  })
})

// =============================================================================
// Complex Lifecycle Tests
// =============================================================================

describe('Time-Travel: Complex Entity Lifecycles', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  describe('create-update-delete-recreate cycles', () => {
    it('handles full entity lifecycle', async () => {
      const events = createEntityLifecycle('posts', 'p1', [
        { op: 'CREATE', state: { title: 'V1', version: 1 }, ts: 1000 },
        { op: 'UPDATE', state: { title: 'V2', version: 2 }, ts: 2000 },
        { op: 'UPDATE', state: { title: 'V3', version: 3 }, ts: 3000 },
        { op: 'DELETE', ts: 4000 },
        { op: 'CREATE', state: { title: 'V4-New', version: 1 }, ts: 5000 },
        { op: 'UPDATE', state: { title: 'V5-New', version: 2 }, ts: 6000 },
      ])
      eventSource.addEvents(events)

      // At each stage
      expect((await replayer.replayEntity('posts:p1', { at: 1500 })).state)
        .toEqual({ title: 'V1', version: 1 })
      expect((await replayer.replayEntity('posts:p1', { at: 2500 })).state)
        .toEqual({ title: 'V2', version: 2 })
      expect((await replayer.replayEntity('posts:p1', { at: 3500 })).state)
        .toEqual({ title: 'V3', version: 3 })
      expect((await replayer.replayEntity('posts:p1', { at: 4500 })).existed)
        .toBe(false)
      expect((await replayer.replayEntity('posts:p1', { at: 5500 })).state)
        .toEqual({ title: 'V4-New', version: 1 })
      expect((await replayer.replayEntity('posts:p1', { at: 6500 })).state)
        .toEqual({ title: 'V5-New', version: 2 })
    })

    it('handles multiple delete-recreate cycles', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { cycle: 1 } }),
        createEvent(target, 'DELETE', 2000, { before: { cycle: 1 } }),
        createEvent(target, 'CREATE', 3000, { after: { cycle: 2 } }),
        createEvent(target, 'DELETE', 4000, { before: { cycle: 2 } }),
        createEvent(target, 'CREATE', 5000, { after: { cycle: 3 } }),
      ])

      expect((await replayer.replayEntity(target, { at: 1500 })).state)
        .toEqual({ cycle: 1 })
      expect((await replayer.replayEntity(target, { at: 2500 })).existed)
        .toBe(false)
      expect((await replayer.replayEntity(target, { at: 3500 })).state)
        .toEqual({ cycle: 2 })
      expect((await replayer.replayEntity(target, { at: 4500 })).existed)
        .toBe(false)
      expect((await replayer.replayEntity(target, { at: 5500 })).state)
        .toEqual({ cycle: 3 })
    })
  })

  describe('high-frequency updates', () => {
    it('handles rapid successive updates', async () => {
      const target = 'posts:p1'
      const events: Event[] = []
      const baseTime = 1000

      // Create entity
      events.push(createEvent(target, 'CREATE', baseTime, {
        id: 'e0',
        after: { counter: 0 },
      }))

      // 100 updates within 100ms
      for (let i = 1; i <= 100; i++) {
        events.push(createEvent(target, 'UPDATE', baseTime + i, {
          id: `e${i}`,
          before: { counter: i - 1 },
          after: { counter: i },
        }))
      }
      eventSource.addEvents(events)

      // Check at specific points
      expect((await replayer.replayEntity(target, { at: 1050 })).state)
        .toEqual({ counter: 50 })
      expect((await replayer.replayEntity(target, { at: 1100 })).state)
        .toEqual({ counter: 100 })
    })

    it('handles updates at same millisecond (ordered by ID)', async () => {
      const target = 'posts:p1'
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, {
          id: 'aaa',
          after: { value: 'first' },
        }),
        createEvent(target, 'UPDATE', 1000, {
          id: 'bbb',
          before: { value: 'first' },
          after: { value: 'second' },
        }),
        createEvent(target, 'UPDATE', 1000, {
          id: 'ccc',
          before: { value: 'second' },
          after: { value: 'third' },
        }),
      ])

      const result = await replayer.replayEntity(target, { at: 1000 })

      // Should apply in ID order: aaa, bbb, ccc
      expect(result.state).toEqual({ value: 'third' })
      expect(result.eventsReplayed).toBe(3)
    })
  })
})

// =============================================================================
// Snapshot Query Tests
// =============================================================================

describe('Time-Travel: Snapshot Queries', () => {
  let eventSource: InMemoryEventSource
  let snapshotStorage: InMemorySnapshotStorage
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    snapshotStorage = createInMemorySnapshotStorage()
    replayer = new EventReplayer(eventSource, { snapshotStorage })
  })

  describe('snapshot selection', () => {
    it('uses snapshot at target timestamp', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { v: 1 } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { v: 1 }, after: { v: 2 } }),
        createEvent(target, 'UPDATE', 3000, { id: 'e3', before: { v: 2 }, after: { v: 3 } }),
      ])

      await snapshotStorage.saveSnapshot({
        target,
        ts: 2000,
        state: { v: 2 },
        eventCount: 2,
        lastEventId: 'e2',
      })

      const result = await replayer.replayWithSnapshot(target, { at: 2000 })

      expect(result.usedSnapshot).toBe(true)
      expect(result.snapshotTs).toBe(2000)
      expect(result.state).toEqual({ v: 2 })
    })

    it('uses older snapshot when querying between snapshots', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { v: 1 } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { v: 1 }, after: { v: 2 } }),
        createEvent(target, 'UPDATE', 4000, { id: 'e4', before: { v: 2 }, after: { v: 4 } }),
      ])

      await snapshotStorage.saveSnapshot({
        target,
        ts: 1000,
        state: { v: 1 },
        eventCount: 1,
        lastEventId: 'e1',
      })

      await snapshotStorage.saveSnapshot({
        target,
        ts: 4000,
        state: { v: 4 },
        eventCount: 3,
        lastEventId: 'e4',
      })

      // Query at ts=3000, should use snapshot at ts=1000
      const result = await replayer.replayWithSnapshot(target, { at: 3000 })

      expect(result.usedSnapshot).toBe(true)
      expect(result.snapshotTs).toBe(1000)
      expect(result.state).toEqual({ v: 2 })
    })

    it('falls back to full replay when no suitable snapshot', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { v: 1 } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { v: 1 }, after: { v: 2 } }),
      ])

      // Snapshot is after query time
      await snapshotStorage.saveSnapshot({
        target,
        ts: 5000,
        state: { v: 5 },
        eventCount: 5,
        lastEventId: 'e5',
      })

      const result = await replayer.replayWithSnapshot(target, { at: 1500 })

      expect(result.usedSnapshot).toBe(false)
      expect(result.state).toEqual({ v: 1 })
    })
  })

  describe('snapshot creation during query', () => {
    it('creates snapshot when threshold exceeded', async () => {
      const target = 'posts:p1'

      // Create many events
      const events: Event[] = [
        createEvent(target, 'CREATE', 1000, { id: 'e0', after: { counter: 0 } }),
      ]
      for (let i = 1; i <= 50; i++) {
        events.push(createEvent(target, 'UPDATE', 1000 + i * 100, {
          id: `e${i}`,
          before: { counter: i - 1 },
          after: { counter: i },
        }))
      }
      eventSource.addEvents(events)

      await replayer.replayWithSnapshot(target, {
        at: 10000,
        createSnapshot: true,
        snapshotThreshold: 25, // Lower threshold
      })

      const snapshots = await snapshotStorage.getSnapshots(target)
      expect(snapshots.length).toBe(1)
    })

    it('does not create snapshot when below threshold', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { v: 1 } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { v: 1 }, after: { v: 2 } }),
      ])

      await replayer.replayWithSnapshot(target, {
        at: 3000,
        createSnapshot: true,
        snapshotThreshold: 100,
      })

      const snapshots = await snapshotStorage.getSnapshots(target)
      expect(snapshots.length).toBe(0)
    })

    it('does not create snapshot when disabled', async () => {
      const target = 'posts:p1'

      const events: Event[] = [
        createEvent(target, 'CREATE', 1000, { id: 'e0', after: { counter: 0 } }),
      ]
      for (let i = 1; i <= 200; i++) {
        events.push(createEvent(target, 'UPDATE', 1000 + i, {
          id: `e${i}`,
          before: { counter: i - 1 },
          after: { counter: i },
        }))
      }
      eventSource.addEvents(events)

      await replayer.replayWithSnapshot(target, {
        at: 2000,
        createSnapshot: false,
      })

      const snapshots = await snapshotStorage.getSnapshots(target)
      expect(snapshots.length).toBe(0)
    })
  })
})

// =============================================================================
// Backward Replay Tests
// =============================================================================

describe('Time-Travel: Backward Replay', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('replays backward from current state to earlier time', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
      createEvent(target, 'UPDATE', 2000, {
        before: { title: 'V1' },
        after: { title: 'V2' },
      }),
      createEvent(target, 'UPDATE', 3000, {
        before: { title: 'V2' },
        after: { title: 'V3' },
      }),
    ])

    const result = await replayer.replayEntity(target, {
      at: 1500,
      currentState: { title: 'V3' },
      currentTs: 3000,
    })

    expect(result.state).toEqual({ title: 'V1' })
    expect(result.eventsReplayed).toBe(2) // Undid 2 events
  })

  it('handles backward replay to before creation', async () => {
    const target = 'posts:p1'
    eventSource.addEvent(createEvent(target, 'CREATE', 2000, {
      after: { title: 'Created' },
    }))

    const result = await replayer.replayEntity(target, {
      at: 1000,
      currentState: { title: 'Created' },
      currentTs: 2000,
    })

    // When target timestamp is before any events, the implementation
    // returns early (no events to replay) rather than using backward replay
    expect(result.existed).toBe(false)
    expect(result.state).toBeNull()
    expect(result.eventsReplayed).toBe(0)
  })

  it('restores state after DELETE in backward replay', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { title: 'Existed' } }),
      createEvent(target, 'DELETE', 2000, { before: { title: 'Existed' } }),
    ])

    const result = await replayer.replayEntity(target, {
      at: 1500,
      currentState: null,
      currentTs: 2000,
    })

    expect(result.existed).toBe(true)
    expect(result.state).toEqual({ title: 'Existed' })
  })

  it('handles multiple undo operations', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { counter: 1 } }),
      createEvent(target, 'UPDATE', 2000, {
        before: { counter: 1 },
        after: { counter: 2 },
      }),
      createEvent(target, 'UPDATE', 3000, {
        before: { counter: 2 },
        after: { counter: 3 },
      }),
      createEvent(target, 'UPDATE', 4000, {
        before: { counter: 3 },
        after: { counter: 4 },
      }),
      createEvent(target, 'UPDATE', 5000, {
        before: { counter: 4 },
        after: { counter: 5 },
      }),
    ])

    const result = await replayer.replayEntity(target, {
      at: 2500,
      currentState: { counter: 5 },
      currentTs: 5000,
    })

    expect(result.state).toEqual({ counter: 2 })
    expect(result.eventsReplayed).toBe(3) // Undid events at 5000, 4000, 3000
  })
})

// =============================================================================
// State History Tests
// =============================================================================

describe('Time-Travel: State History', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('returns complete state history in order', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { v: 1 } }),
      createEvent(target, 'UPDATE', 2000, { before: { v: 1 }, after: { v: 2 } }),
      createEvent(target, 'UPDATE', 3000, { before: { v: 2 }, after: { v: 3 } }),
      createEvent(target, 'DELETE', 4000, { before: { v: 3 } }),
    ])

    const history = await replayer.getStateHistory(target)

    expect(history).toHaveLength(4)
    expect(history[0]).toEqual({ ts: 1000, state: { v: 1 }, op: 'CREATE' })
    expect(history[1]).toEqual({ ts: 2000, state: { v: 2 }, op: 'UPDATE' })
    expect(history[2]).toEqual({ ts: 3000, state: { v: 3 }, op: 'UPDATE' })
    expect(history[3]).toEqual({ ts: 4000, state: null, op: 'DELETE' })
  })

  it('filters history by time range', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { v: 1 } }),
      createEvent(target, 'UPDATE', 2000, { before: { v: 1 }, after: { v: 2 } }),
      createEvent(target, 'UPDATE', 3000, { before: { v: 2 }, after: { v: 3 } }),
      createEvent(target, 'UPDATE', 4000, { before: { v: 3 }, after: { v: 4 } }),
      createEvent(target, 'UPDATE', 5000, { before: { v: 4 }, after: { v: 5 } }),
    ])

    const history = await replayer.getStateHistory(target, {
      minTs: 2000,
      maxTs: 4000,
    })

    expect(history).toHaveLength(3)
    expect(history.map(h => h.ts)).toEqual([2000, 3000, 4000])
  })

  it('handles history with delete and recreate', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: { cycle: 1 } }),
      createEvent(target, 'DELETE', 2000, { before: { cycle: 1 } }),
      createEvent(target, 'CREATE', 3000, { after: { cycle: 2 } }),
    ])

    const history = await replayer.getStateHistory(target)

    expect(history).toHaveLength(3)
    expect(history[0].state).toEqual({ cycle: 1 })
    expect(history[1].state).toBeNull()
    expect(history[2].state).toEqual({ cycle: 2 })
  })
})

// =============================================================================
// Batch Query Tests
// =============================================================================

describe('Time-Travel: Batch Queries', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('queries multiple entities at same timestamp', async () => {
    eventSource.addEvents([
      createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Post 1' } }),
      createEvent('posts:p2', 'CREATE', 1500, { after: { title: 'Post 2' } }),
      createEvent('users:u1', 'CREATE', 2000, { after: { name: 'User 1' } }),
    ])

    const results = await replayer.replayEntities(
      ['posts:p1', 'posts:p2', 'users:u1'],
      { at: 2500 }
    )

    expect(results.size).toBe(3)
    expect(results.get('posts:p1')?.state).toEqual({ title: 'Post 1' })
    expect(results.get('posts:p2')?.state).toEqual({ title: 'Post 2' })
    expect(results.get('users:u1')?.state).toEqual({ name: 'User 1' })
  })

  it('handles mixed existence in batch query', async () => {
    eventSource.addEvents([
      createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Exists' } }),
      createEvent('posts:p2', 'CREATE', 3000, { after: { title: 'Future' } }),
    ])

    const results = await replayer.replayEntities(
      ['posts:p1', 'posts:p2', 'posts:p3'],
      { at: 2000 }
    )

    expect(results.get('posts:p1')?.existed).toBe(true)
    expect(results.get('posts:p2')?.existed).toBe(false)
    expect(results.get('posts:p3')?.existed).toBe(false)
  })

  it('handles deleted entities in batch query', async () => {
    eventSource.addEvents([
      createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'P1' } }),
      createEvent('posts:p2', 'CREATE', 1000, { after: { title: 'P2' } }),
      createEvent('posts:p1', 'DELETE', 2000, { before: { title: 'P1' } }),
    ])

    const results = await replayer.replayEntities(
      ['posts:p1', 'posts:p2'],
      { at: 3000 }
    )

    expect(results.get('posts:p1')?.existed).toBe(false)
    expect(results.get('posts:p2')?.existed).toBe(true)
  })
})

// =============================================================================
// Data Type Handling Tests
// =============================================================================

describe('Time-Travel: Data Type Handling', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('preserves complex nested objects', async () => {
    const target = 'posts:p1'
    const complexData = {
      title: 'Complex',
      metadata: {
        tags: ['tech', 'database', 'parquet'],
        author: {
          name: 'John',
          email: 'john@example.com',
        },
        stats: {
          views: 1000,
          likes: 50,
          shares: 10,
        },
      },
      content: {
        blocks: [
          { type: 'heading', text: 'Introduction' },
          { type: 'paragraph', text: 'Lorem ipsum...' },
        ],
      },
    }

    eventSource.addEvent(createEvent(target, 'CREATE', 1000, { after: complexData }))

    const result = await replayer.replayEntity(target, { at: 2000 })

    expect(result.state).toEqual(complexData)
  })

  it('handles arrays correctly', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, {
        after: { items: [1, 2, 3] },
      }),
      createEvent(target, 'UPDATE', 2000, {
        before: { items: [1, 2, 3] },
        after: { items: [1, 2, 3, 4, 5] },
      }),
    ])

    const result = await replayer.replayEntity(target, { at: 3000 })

    expect(result.state).toEqual({ items: [1, 2, 3, 4, 5] })
  })

  it('handles null values', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, {
        after: { value: null, nested: { inner: null } },
      }),
    ])

    const result = await replayer.replayEntity(target, { at: 2000 })

    expect(result.state).toEqual({ value: null, nested: { inner: null } })
  })

  it('handles empty object state', async () => {
    const target = 'posts:p1'
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: {} }),
      createEvent(target, 'UPDATE', 2000, { before: {}, after: { added: 'field' } }),
    ])

    const atCreate = await replayer.replayEntity(target, { at: 1500 })
    expect(atCreate.state).toEqual({})

    const atUpdate = await replayer.replayEntity(target, { at: 2500 })
    expect(atUpdate.state).toEqual({ added: 'field' })
  })

  it('handles boolean and numeric values', async () => {
    const target = 'posts:p1'
    eventSource.addEvent(createEvent(target, 'CREATE', 1000, {
      after: {
        active: true,
        archived: false,
        count: 0,
        score: -1.5,
        bigNumber: Number.MAX_SAFE_INTEGER,
      },
    }))

    const result = await replayer.replayEntity(target, { at: 2000 })

    expect(result.state).toEqual({
      active: true,
      archived: false,
      count: 0,
      score: -1.5,
      bigNumber: Number.MAX_SAFE_INTEGER,
    })
  })
})

// =============================================================================
// Convenience Function Tests
// =============================================================================

describe('Time-Travel: Convenience Functions', () => {
  let eventSource: InMemoryEventSource

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
  })

  describe('replayEvents', () => {
    it('reconstructs state from namespace and id', async () => {
      eventSource.addEvent(createEvent('posts:p1', 'CREATE', 1000, {
        after: { title: 'Hello' },
      }))

      const result = await replayEvents(eventSource, 'posts', 'p1')

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Hello' })
    })

    it('defaults to current time when at not specified', async () => {
      const futureTs = Date.now() + 10000
      eventSource.addEvent(createEvent('posts:p1', 'CREATE', futureTs, {
        after: { title: 'Future' },
      }))

      const result = await replayEvents(eventSource, 'posts', 'p1')

      // Should not see future entity
      expect(result.existed).toBe(false)
    })
  })

  describe('replayEventsWithSnapshots', () => {
    it('uses snapshots for efficient replay', async () => {
      const snapshotStorage = createInMemorySnapshotStorage()

      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { id: 'e1', after: { v: 1 } }),
        createEvent('posts:p1', 'UPDATE', 2000, { id: 'e2', before: { v: 1 }, after: { v: 2 } }),
      ])

      await snapshotStorage.saveSnapshot({
        target: 'posts:p1',
        ts: 1000,
        state: { v: 1 },
        eventCount: 1,
        lastEventId: 'e1',
      })

      const result = await replayEventsWithSnapshots(
        eventSource,
        snapshotStorage,
        'posts',
        'p1'
      )

      expect(result.usedSnapshot).toBe(true)
      expect(result.eventsFromSnapshot).toBe(1)
    })
  })

  describe('replayEventsBatch', () => {
    it('replays multiple entities efficiently', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Post 1' } }),
        createEvent('posts:p2', 'CREATE', 1000, { after: { title: 'Post 2' } }),
        createEvent('users:u1', 'CREATE', 1000, { after: { name: 'User 1' } }),
      ])

      const results = await replayEventsBatch(eventSource, [
        { namespace: 'posts', entityId: 'p1' },
        { namespace: 'posts', entityId: 'p2' },
        { namespace: 'users', entityId: 'u1' },
      ])

      expect(results.size).toBe(3)
      expect(results.get('posts:p1')?.state).toEqual({ title: 'Post 1' })
      expect(results.get('posts:p2')?.state).toEqual({ title: 'Post 2' })
      expect(results.get('users:u1')?.state).toEqual({ name: 'User 1' })
    })
  })

  describe('getEntityHistory', () => {
    it('returns full history', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { v: 1 } }),
        createEvent('posts:p1', 'UPDATE', 2000, { before: { v: 1 }, after: { v: 2 } }),
      ])

      const history = await getEntityHistory(eventSource, 'posts', 'p1')

      expect(history).toHaveLength(2)
      expect(history[0].op).toBe('CREATE')
      expect(history[1].op).toBe('UPDATE')
    })
  })
})

// =============================================================================
// Performance Tests
// =============================================================================

describe('Time-Travel: Performance', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('handles 10,000 events efficiently', async () => {
    const target = 'posts:p1'
    const events: Event[] = []

    events.push(createEvent(target, 'CREATE', 1000, {
      id: 'e00000',
      after: { counter: 0 },
    }))

    for (let i = 1; i < 10000; i++) {
      events.push(createEvent(target, 'UPDATE', 1000 + i, {
        id: `e${i.toString().padStart(5, '0')}`,
        before: { counter: i - 1 },
        after: { counter: i },
      }))
    }
    eventSource.addEvents(events)

    const start = performance.now()
    const result = await replayer.replayEntity(target, { at: 20000 })
    const elapsed = performance.now() - start

    expect(result.state).toEqual({ counter: 9999 })
    expect(result.eventsReplayed).toBe(10000)
    // Should complete in reasonable time
    expect(elapsed).toBeLessThan(2000)
  })

  it('handles 1,000 entities efficiently', async () => {
    const targets: string[] = []
    for (let i = 0; i < 1000; i++) {
      const target = `posts:p${i}`
      targets.push(target)
      eventSource.addEvent(createEvent(target, 'CREATE', 1000, {
        after: { id: i },
      }))
    }

    const start = performance.now()
    const results = await replayer.replayEntities(targets, { at: 2000 })
    const elapsed = performance.now() - start

    expect(results.size).toBe(1000)
    expect(elapsed).toBeLessThan(2000)
  })
})

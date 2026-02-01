/**
 * Event Replay Test Suite
 *
 * Tests for event sourcing functionality:
 * - Event replay and state reconstruction
 * - Snapshot-based replay
 * - Event versioning and schema evolution
 * - Edge cases (deletions, relationships, concurrent updates)
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
  createVersionedEvent,
  getEventVersion,
  createFieldRenameUpgrader,
  createEventReplayer,
  createInMemoryEventSource,
  createInMemorySnapshotStorage,
  type EventSource,
  type Snapshot,
  type SnapshotStorage,
  type ReplayOptions,
  type ExtendedReplayResult,
} from '../../../src/events/replay'
import type { Event, Variant } from '../../../src/types/entity'
import { entityTarget } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

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

function createEntityEvents(
  ns: string,
  id: string,
  states: Array<{ op: 'CREATE' | 'UPDATE' | 'DELETE'; state?: Variant; ts: number }>
): Event[] {
  const target = entityTarget(ns, id)
  const events: Event[] = []
  let previousState: Variant | null = null

  for (const { op, state, ts } of states) {
    const event = createEvent(target, op, ts, {
      before: previousState ?? undefined,
      after: op === 'DELETE' ? undefined : state,
    })
    events.push(event)
    previousState = op === 'DELETE' ? null : (state ?? null)
  }

  return events
}

// =============================================================================
// Basic Replay Tests
// =============================================================================

describe('EventReplayer', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  describe('replayEntity', () => {
    it('reconstructs state from CREATE event', async () => {
      const target = 'posts:p1'
      eventSource.addEvent(createEvent(target, 'CREATE', 1000, {
        after: { title: 'Hello', content: 'World' },
      }))

      const result = await replayer.replayEntity(target, { at: 2000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Hello', content: 'World' })
      expect(result.eventsReplayed).toBe(1)
    })

    it('reconstructs state from multiple UPDATE events', async () => {
      const target = 'posts:p1'
      const initial = { title: 'V1', views: 0 }
      const updated1 = { title: 'V2', views: 0 }
      const updated2 = { title: 'V2', views: 10 }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: initial }),
        createEvent(target, 'UPDATE', 2000, { before: initial, after: updated1 }),
        createEvent(target, 'UPDATE', 3000, { before: updated1, after: updated2 }),
      ])

      const result = await replayer.replayEntity(target, { at: 4000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'V2', views: 10 })
      expect(result.eventsReplayed).toBe(3)
    })

    it('returns null for deleted entities', async () => {
      const target = 'posts:p1'
      const state = { title: 'To Delete' }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: state }),
        createEvent(target, 'DELETE', 2000, { before: state }),
      ])

      const result = await replayer.replayEntity(target, { at: 3000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(2)
    })

    it('returns state before entity was deleted when querying at earlier time', async () => {
      const target = 'posts:p1'
      const state = { title: 'Exists' }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: state }),
        createEvent(target, 'DELETE', 3000, { before: state }),
      ])

      const result = await replayer.replayEntity(target, { at: 2000 })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Exists' })
      expect(result.eventsReplayed).toBe(1)
    })

    it('returns null when entity did not exist yet', async () => {
      const target = 'posts:p1'

      eventSource.addEvent(createEvent(target, 'CREATE', 5000, {
        after: { title: 'Future' },
      }))

      const result = await replayer.replayEntity(target, { at: 1000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(0)
    })

    it('returns null for entity with no events', async () => {
      const result = await replayer.replayEntity('posts:nonexistent', { at: 1000 })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
      expect(result.eventsReplayed).toBe(0)
    })

    it('handles re-creation after delete', async () => {
      const target = 'posts:p1'
      const state1 = { title: 'V1' }
      const state2 = { title: 'V2 - Recreated' }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: state1 }),
        createEvent(target, 'DELETE', 2000, { before: state1 }),
        createEvent(target, 'CREATE', 3000, { after: state2 }),
      ])

      // After first delete
      const afterDelete = await replayer.replayEntity(target, { at: 2500 })
      expect(afterDelete.existed).toBe(false)

      // After recreation
      const afterRecreate = await replayer.replayEntity(target, { at: 4000 })
      expect(afterRecreate.existed).toBe(true)
      expect(afterRecreate.state).toEqual({ title: 'V2 - Recreated' })
    })
  })

  describe('backward replay', () => {
    it('replays backward from current state', async () => {
      const target = 'posts:p1'
      const state1 = { title: 'V1' }
      const state2 = { title: 'V2' }
      const state3 = { title: 'V3' }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: state1 }),
        createEvent(target, 'UPDATE', 2000, { before: state1, after: state2 }),
        createEvent(target, 'UPDATE', 3000, { before: state2, after: state3 }),
      ])

      // Replay to time 1500, starting from current state at 3000
      const result = await replayer.replayEntity(target, {
        at: 1500,
        currentState: state3,
        currentTs: 3000,
      })

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'V1' })
      expect(result.eventsReplayed).toBe(2) // Undid 2 events
    })

    it('handles backward replay to before entity creation', async () => {
      const target = 'posts:p1'
      const state = { title: 'Created' }

      eventSource.addEvents([
        createEvent(target, 'CREATE', 2000, { after: state }),
      ])

      const result = await replayer.replayEntity(target, {
        at: 1000,
        currentState: state,
        currentTs: 2000,
      })

      expect(result.existed).toBe(false)
      expect(result.state).toBeNull()
    })
  })

  describe('replayEntities (batch)', () => {
    it('replays multiple entities', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Post 1' } }),
        createEvent('posts:p2', 'CREATE', 1500, { after: { title: 'Post 2' } }),
        createEvent('users:u1', 'CREATE', 2000, { after: { name: 'User 1' } }),
      ])

      const results = await replayer.replayEntities(
        ['posts:p1', 'posts:p2', 'users:u1'],
        { at: 3000 }
      )

      expect(results.size).toBe(3)
      expect(results.get('posts:p1')?.state).toEqual({ title: 'Post 1' })
      expect(results.get('posts:p2')?.state).toEqual({ title: 'Post 2' })
      expect(results.get('users:u1')?.state).toEqual({ name: 'User 1' })
    })

    it('handles mixed existence states', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Exists' } }),
        createEvent('posts:p2', 'CREATE', 5000, { after: { title: 'Future' } }),
      ])

      const results = await replayer.replayEntities(
        ['posts:p1', 'posts:p2', 'posts:nonexistent'],
        { at: 3000 }
      )

      expect(results.get('posts:p1')?.existed).toBe(true)
      expect(results.get('posts:p2')?.existed).toBe(false)
      expect(results.get('posts:nonexistent')?.existed).toBe(false)
    })
  })

  describe('getStateHistory', () => {
    it('returns state after each event', async () => {
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

      const history = await replayer.getStateHistory(target)

      expect(history.length).toBe(3)
      expect(history[0]).toEqual({ ts: 1000, state: { title: 'V1' }, op: 'CREATE' })
      expect(history[1]).toEqual({ ts: 2000, state: { title: 'V2' }, op: 'UPDATE' })
      expect(history[2]).toEqual({ ts: 3000, state: { title: 'V3' }, op: 'UPDATE' })
    })

    it('filters by time range', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, { before: { title: 'V1' }, after: { title: 'V2' } }),
        createEvent(target, 'UPDATE', 3000, { before: { title: 'V2' }, after: { title: 'V3' } }),
        createEvent(target, 'UPDATE', 4000, { before: { title: 'V3' }, after: { title: 'V4' } }),
      ])

      const history = await replayer.getStateHistory(target, { minTs: 1500, maxTs: 3500 })

      expect(history.length).toBe(2)
      expect(history[0].ts).toBe(2000)
      expect(history[1].ts).toBe(3000)
    })
  })
})

// =============================================================================
// Snapshot-Based Replay Tests
// =============================================================================

describe('Snapshot-Based Replay', () => {
  let eventSource: InMemoryEventSource
  let snapshotStorage: InMemorySnapshotStorage
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    snapshotStorage = createInMemorySnapshotStorage()
    replayer = new EventReplayer(eventSource, { snapshotStorage })
  })

  describe('replayWithSnapshot', () => {
    it('uses existing snapshot when available', async () => {
      const target = 'posts:p1'

      // Add events
      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { title: 'V1' }, after: { title: 'V2' } }),
        createEvent(target, 'UPDATE', 3000, { id: 'e3', before: { title: 'V2' }, after: { title: 'V3' } }),
        createEvent(target, 'UPDATE', 4000, { id: 'e4', before: { title: 'V3' }, after: { title: 'V4' } }),
      ])

      // Add snapshot at event e2
      await snapshotStorage.saveSnapshot({
        target,
        ts: 2000,
        state: { title: 'V2' },
        eventCount: 2,
        lastEventId: 'e2',
      })

      const result = await replayer.replayWithSnapshot(target, { at: 5000 })

      expect(result.usedSnapshot).toBe(true)
      expect(result.snapshotTs).toBe(2000)
      expect(result.eventsFromSnapshot).toBe(2) // Only e3 and e4
      expect(result.state).toEqual({ title: 'V4' })
    })

    it('creates snapshot when threshold is exceeded', async () => {
      const target = 'posts:p1'

      // Add many events
      const events: Event[] = []
      for (let i = 1; i <= 150; i++) {
        events.push(createEvent(target, i === 1 ? 'CREATE' : 'UPDATE', i * 1000, {
          id: `e${i}`,
          before: i > 1 ? { title: `V${i - 1}` } : undefined,
          after: { title: `V${i}` },
        }))
      }
      eventSource.addEvents(events)

      // Replay with lower threshold
      const result = await replayer.replayWithSnapshot(target, {
        at: 200000,
        createSnapshot: true,
        snapshotThreshold: 100,
      })

      expect(result.eventsReplayed).toBe(150)

      // Verify snapshot was created
      const snapshots = await snapshotStorage.getSnapshots(target)
      expect(snapshots.length).toBe(1)
    })

    it('does not create snapshot when below threshold', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      await replayer.replayWithSnapshot(target, {
        at: 3000,
        createSnapshot: true,
        snapshotThreshold: 100, // Higher than event count
      })

      const snapshots = await snapshotStorage.getSnapshots(target)
      expect(snapshots.length).toBe(0)
    })

    it('falls back to full replay when no snapshot exists', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, { before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      const result = await replayer.replayWithSnapshot(target, { at: 3000 })

      expect(result.usedSnapshot).toBe(false)
      expect(result.eventsReplayed).toBe(2)
      expect(result.state).toEqual({ title: 'V2' })
    })
  })

  describe('createSnapshot', () => {
    it('creates snapshot from current state', async () => {
      const target = 'posts:p1'

      eventSource.addEvents([
        createEvent(target, 'CREATE', 1000, { id: 'e1', after: { title: 'V1' } }),
        createEvent(target, 'UPDATE', 2000, { id: 'e2', before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      const snapshot = await replayer.createSnapshot(target)

      expect(snapshot).not.toBeNull()
      expect(snapshot!.target).toBe(target)
      expect(snapshot!.state).toEqual({ title: 'V2' })
      expect(snapshot!.eventCount).toBe(2)
      expect(snapshot!.lastEventId).toBe('e2')
    })

    it('returns null for entity with no events', async () => {
      const snapshot = await replayer.createSnapshot('posts:nonexistent')
      expect(snapshot).toBeNull()
    })
  })

  describe('compactSnapshots', () => {
    it('removes old snapshots keeping only most recent', async () => {
      const target = 'posts:p1'

      // Add multiple snapshots
      await snapshotStorage.saveSnapshot({ target, ts: 1000, state: { v: 1 }, eventCount: 1, lastEventId: 'e1' })
      await snapshotStorage.saveSnapshot({ target, ts: 2000, state: { v: 2 }, eventCount: 2, lastEventId: 'e2' })
      await snapshotStorage.saveSnapshot({ target, ts: 3000, state: { v: 3 }, eventCount: 3, lastEventId: 'e3' })
      await snapshotStorage.saveSnapshot({ target, ts: 4000, state: { v: 4 }, eventCount: 4, lastEventId: 'e4' })

      const deleted = await replayer.compactSnapshots(target, 2)

      expect(deleted).toBe(2)

      const remaining = await snapshotStorage.getSnapshots(target)
      expect(remaining.length).toBe(2)
      expect(remaining.map(s => s.ts).sort((a, b) => b - a)).toEqual([4000, 3000])
    })
  })
})

// =============================================================================
// InMemorySnapshotStorage Tests
// =============================================================================

describe('InMemorySnapshotStorage', () => {
  let storage: InMemorySnapshotStorage

  beforeEach(() => {
    storage = createInMemorySnapshotStorage()
  })

  it('returns null when no snapshots exist', async () => {
    const result = await storage.getSnapshot('posts:p1')
    expect(result).toBeNull()
  })

  it('returns latest snapshot when no timestamp specified', async () => {
    await storage.saveSnapshot({ target: 'posts:p1', ts: 1000, state: { v: 1 }, eventCount: 1, lastEventId: 'e1' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 3000, state: { v: 3 }, eventCount: 3, lastEventId: 'e3' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 2000, state: { v: 2 }, eventCount: 2, lastEventId: 'e2' })

    const result = await storage.getSnapshot('posts:p1')
    expect(result?.ts).toBe(3000)
  })

  it('returns latest snapshot before specified timestamp', async () => {
    await storage.saveSnapshot({ target: 'posts:p1', ts: 1000, state: { v: 1 }, eventCount: 1, lastEventId: 'e1' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 2000, state: { v: 2 }, eventCount: 2, lastEventId: 'e2' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 3000, state: { v: 3 }, eventCount: 3, lastEventId: 'e3' })

    const result = await storage.getSnapshot('posts:p1', 2500)
    expect(result?.ts).toBe(2000)
  })

  it('deletes snapshots before timestamp', async () => {
    await storage.saveSnapshot({ target: 'posts:p1', ts: 1000, state: { v: 1 }, eventCount: 1, lastEventId: 'e1' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 2000, state: { v: 2 }, eventCount: 2, lastEventId: 'e2' })
    await storage.saveSnapshot({ target: 'posts:p1', ts: 3000, state: { v: 3 }, eventCount: 3, lastEventId: 'e3' })

    const deleted = await storage.deleteSnapshotsBefore('posts:p1', 2500)

    expect(deleted).toBe(2)
    const remaining = await storage.getSnapshots('posts:p1')
    expect(remaining.length).toBe(1)
    expect(remaining[0].ts).toBe(3000)
  })
})

// =============================================================================
// Convenience Function Tests
// =============================================================================

describe('Convenience Functions', () => {
  let eventSource: InMemoryEventSource

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
  })

  describe('replayEvents', () => {
    it('reconstructs entity state from namespace and id', async () => {
      eventSource.addEvent(createEvent('posts:p1', 'CREATE', 1000, {
        after: { title: 'Hello' },
      }))

      const result = await replayEvents(eventSource, 'posts', 'p1')

      expect(result.existed).toBe(true)
      expect(result.state).toEqual({ title: 'Hello' })
    })

    it('supports time-travel with at option', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent('posts:p1', 'UPDATE', 3000, { before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      const result = await replayEvents(eventSource, 'posts', 'p1', { at: 2000 })

      expect(result.state).toEqual({ title: 'V1' })
    })
  })

  describe('replayEventsWithSnapshots', () => {
    it('uses snapshot storage for efficient replay', async () => {
      const snapshotStorage = createInMemorySnapshotStorage()

      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { id: 'e1', after: { title: 'V1' } }),
        createEvent('posts:p1', 'UPDATE', 2000, { id: 'e2', before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      await snapshotStorage.saveSnapshot({
        target: 'posts:p1',
        ts: 1000,
        state: { title: 'V1' },
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
      expect(result.eventsFromSnapshot).toBe(1) // Only e2
    })
  })

  describe('replayEventsBatch', () => {
    it('replays multiple entities efficiently', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'Post 1' } }),
        createEvent('posts:p2', 'CREATE', 1000, { after: { title: 'Post 2' } }),
      ])

      const results = await replayEventsBatch(eventSource, [
        { namespace: 'posts', entityId: 'p1' },
        { namespace: 'posts', entityId: 'p2' },
      ])

      expect(results.size).toBe(2)
      expect(results.get('posts:p1')?.state).toEqual({ title: 'Post 1' })
      expect(results.get('posts:p2')?.state).toEqual({ title: 'Post 2' })
    })
  })

  describe('getEntityHistory', () => {
    it('returns full state history', async () => {
      eventSource.addEvents([
        createEvent('posts:p1', 'CREATE', 1000, { after: { title: 'V1' } }),
        createEvent('posts:p1', 'UPDATE', 2000, { before: { title: 'V1' }, after: { title: 'V2' } }),
      ])

      const history = await getEntityHistory(eventSource, 'posts', 'p1')

      expect(history.length).toBe(2)
      expect(history[0].op).toBe('CREATE')
      expect(history[1].op).toBe('UPDATE')
    })
  })
})

// =============================================================================
// Event Versioning Tests
// =============================================================================

describe('Event Versioning', () => {
  describe('createVersionedEvent', () => {
    it('adds schema version to event metadata', () => {
      const event = createEvent('posts:p1', 'CREATE', 1000, {
        after: { title: 'Hello' },
      })

      const versioned = createVersionedEvent(event, 2)

      expect(getEventVersion(versioned)).toBe(2)
    })

    it('preserves existing metadata', () => {
      const event = createEvent('posts:p1', 'CREATE', 1000, {
        after: { title: 'Hello' },
        metadata: { correlationId: 'abc123' },
      })

      const versioned = createVersionedEvent(event, 2)

      expect((versioned.metadata as any).correlationId).toBe('abc123')
      expect(getEventVersion(versioned)).toBe(2)
    })
  })

  describe('createFieldRenameUpgrader', () => {
    it('renames fields in before and after states', () => {
      const upgrader = createFieldRenameUpgrader({
        viewCount: 'views',
        userName: 'name',
      })

      const event = createEvent('posts:p1', 'UPDATE', 1000, {
        before: { title: 'Old', viewCount: 10, userName: 'John' },
        after: { title: 'New', viewCount: 20, userName: 'Jane' },
      })

      const upgraded = upgrader(event, 1, 2)

      expect(upgraded.before).toEqual({ title: 'Old', views: 10, name: 'John' })
      expect(upgraded.after).toEqual({ title: 'New', views: 20, name: 'Jane' })
      expect(getEventVersion(upgraded)).toBe(2)
      expect((upgraded.metadata as any).upgradedFrom).toBe(1)
    })
  })

  describe('EventReplayer with versioning', () => {
    it('upgrades events during replay', async () => {
      const eventSource = createInMemoryEventSource()
      const upgrader = createFieldRenameUpgrader({ oldField: 'newField' })

      const replayer = new EventReplayer(eventSource, {
        versioning: { currentVersion: 2, upgrader },
      })

      // Add old version event
      eventSource.addEvent(createEvent('posts:p1', 'CREATE', 1000, {
        after: { title: 'Hello', oldField: 'value' },
        metadata: { schemaVersion: 1 },
      }))

      const result = await replayer.replayEntity('posts:p1', { at: 2000 })

      expect(result.state).toEqual({ title: 'Hello', newField: 'value' })
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let eventSource: InMemoryEventSource
  let replayer: EventReplayer

  beforeEach(() => {
    eventSource = createInMemoryEventSource()
    replayer = createEventReplayer(eventSource)
  })

  it('handles events with same timestamp', async () => {
    const target = 'posts:p1'

    // Events with same timestamp, different IDs
    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { id: 'aaa', after: { title: 'V1' } }),
      createEvent(target, 'UPDATE', 1000, { id: 'bbb', before: { title: 'V1' }, after: { title: 'V2' } }),
      createEvent(target, 'UPDATE', 1000, { id: 'ccc', before: { title: 'V2' }, after: { title: 'V3' } }),
    ])

    const result = await replayer.replayEntity(target, { at: 2000 })

    // Should process in ID order
    expect(result.state).toEqual({ title: 'V3' })
    expect(result.eventsReplayed).toBe(3)
  })

  it('handles complex nested data', async () => {
    const target = 'posts:p1'
    const complexState = {
      title: 'Complex',
      metadata: {
        tags: ['a', 'b', 'c'],
        nested: {
          deep: {
            value: 42,
          },
        },
      },
      counts: [1, 2, 3],
    }

    eventSource.addEvent(createEvent(target, 'CREATE', 1000, {
      after: complexState,
    }))

    const result = await replayer.replayEntity(target, { at: 2000 })

    expect(result.state).toEqual(complexState)
  })

  it('handles empty state in events', async () => {
    const target = 'posts:p1'

    eventSource.addEvents([
      createEvent(target, 'CREATE', 1000, { after: {} }),
      createEvent(target, 'UPDATE', 2000, { before: {}, after: { title: 'Added' } }),
    ])

    const result = await replayer.replayEntity(target, { at: 3000 })

    expect(result.state).toEqual({ title: 'Added' })
  })

  it('handles many events efficiently', async () => {
    const target = 'posts:p1'
    const events: Event[] = []

    // Create 10000 events
    for (let i = 1; i <= 10000; i++) {
      events.push(createEvent(
        target,
        i === 1 ? 'CREATE' : 'UPDATE',
        i * 100,
        {
          id: `e${i.toString().padStart(5, '0')}`,
          before: i > 1 ? { counter: i - 1 } : undefined,
          after: { counter: i },
        }
      ))
    }
    eventSource.addEvents(events)

    const start = performance.now()
    const result = await replayer.replayEntity(target, { at: 1000001 })
    const elapsed = performance.now() - start

    expect(result.state).toEqual({ counter: 10000 })
    expect(result.eventsReplayed).toBe(10000)
    // Should complete in reasonable time (less than 1 second)
    expect(elapsed).toBeLessThan(1000)
  })
})

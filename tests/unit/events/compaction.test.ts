/**
 * Event Compaction Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventCompactor,
  StateCollector,
  InMemoryStateWriter,
  createEventCompactor,
  createStateCollector,
  createInMemoryStateWriter,
} from '../../../src/events/compaction'
import { ManifestManager } from '../../../src/events/manifest'
import type { SegmentStorage } from '../../../src/events/segment'
import type { Event } from '../../../src/types'
import type { EventBatch, EventSegment } from '../../../src/events/types'

// =============================================================================
// Mock Storage Implementation
// =============================================================================

class MockStorage implements SegmentStorage {
  private files: Map<string, Uint8Array> = new Map()

  async put(key: string, data: Uint8Array | ArrayBuffer): Promise<void> {
    const uint8 = data instanceof Uint8Array ? data : new Uint8Array(data)
    this.files.set(key, uint8)
  }

  async get(key: string): Promise<Uint8Array | null> {
    return this.files.get(key) ?? null
  }

  async head(key: string): Promise<boolean> {
    return this.files.has(key)
  }

  async delete(key: string): Promise<void> {
    this.files.delete(key)
  }

  async list(prefix: string): Promise<string[]> {
    return [...this.files.keys()].filter(k => k.startsWith(prefix))
  }

  getFileCount(): number {
    return this.files.size
  }

  clear(): void {
    this.files.clear()
  }

  // Helper to write a segment directly
  writeSegmentData(path: string, events: Event[]): void {
    const lines = events.map(e => JSON.stringify(e))
    const json = lines.join('\n')
    this.files.set(path, new TextEncoder().encode(json))
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'users:user1',
    after: { name: 'Test User' },
    ...overrides,
  }
}

function createTestSegment(overrides: Partial<EventSegment> = {}): EventSegment {
  return {
    seq: 1,
    path: 'test-app/events/seg-0001.parquet',
    minTs: 1000,
    maxTs: 2000,
    count: 10,
    sizeBytes: 1024,
    createdAt: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// StateCollector Tests
// =============================================================================

describe('StateCollector', () => {
  let collector: StateCollector

  beforeEach(() => {
    collector = new StateCollector()
  })

  describe('entity events', () => {
    it('collects CREATE event', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Alice' },
      }))

      const states = collector.getEntityStates()
      expect(states).toHaveLength(1)
      expect(states[0]).toMatchObject({
        target: 'users:user1',
        ns: 'users',
        id: 'user1',
        state: { name: 'Alice' },
        exists: true,
      })
    })

    it('collects UPDATE event', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'UPDATE',
        before: { name: 'Alice' },
        after: { name: 'Alice Updated' },
      }))

      const states = collector.getEntityStates()
      expect(states[0].state).toEqual({ name: 'Alice Updated' })
      expect(states[0].exists).toBe(true)
    })

    it('collects DELETE event', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'DELETE',
        before: { name: 'Alice' },
      }))

      const states = collector.getEntityStates()
      expect(states[0].state).toBeNull()
      expect(states[0].exists).toBe(false)
    })

    it('uses latest event for same target', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'V1' },
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'V2' },
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1500, // Older than V2
        op: 'UPDATE',
        after: { name: 'V1.5' },
      }))

      const states = collector.getEntityStates()
      expect(states).toHaveLength(1)
      expect(states[0].state).toEqual({ name: 'V2' })
      expect(states[0].lastEventTs).toBe(2000)
    })

    it('tracks multiple entities', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User 1' },
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user2',
        ts: 1500,
        op: 'CREATE',
        after: { name: 'User 2' },
      }))

      collector.processEvent(createTestEvent({
        target: 'posts:post1',
        ts: 2000,
        op: 'CREATE',
        after: { title: 'Post 1' },
      }))

      const states = collector.getEntityStates()
      expect(states).toHaveLength(3)
    })
  })

  describe('relationship events', () => {
    it('collects relationship CREATE', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1000,
        op: 'CREATE',
        after: { since: '2024-01-01' },
      }))

      const states = collector.getRelationshipStates()
      expect(states).toHaveLength(1)
      expect(states[0]).toMatchObject({
        target: 'users:user1:follows:users:user2',
        from: 'users:user1',
        predicate: 'follows',
        to: 'users:user2',
        data: { since: '2024-01-01' },
        exists: true,
      })
    })

    it('collects relationship DELETE', () => {
      collector.processEvent(createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1000,
        op: 'DELETE',
        before: { since: '2024-01-01' },
      }))

      const states = collector.getRelationshipStates()
      expect(states[0].exists).toBe(false)
      expect(states[0].data).toBeNull()
    })
  })

  describe('batch processing', () => {
    it('processes batch of events', () => {
      const batch: EventBatch = {
        events: [
          createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'A' } }),
          createTestEvent({ target: 'users:user2', ts: 2000, op: 'CREATE', after: { name: 'B' } }),
          createTestEvent({ target: 'users:user1', ts: 3000, op: 'UPDATE', after: { name: 'A2' } }),
        ],
        minTs: 1000,
        maxTs: 3000,
        count: 3,
      }

      collector.processBatch(batch)

      const states = collector.getEntityStates()
      expect(states).toHaveLength(2)

      const user1 = states.find(s => s.id === 'user1')
      expect(user1?.state).toEqual({ name: 'A2' })
    })
  })

  describe('filtering', () => {
    beforeEach(() => {
      // Mix of existing and deleted entities
      collector.processEvent(createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Exists' },
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user2',
        ts: 2000,
        op: 'DELETE',
        before: { name: 'Deleted' },
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1500,
        op: 'CREATE',
        after: {},
      }))

      collector.processEvent(createTestEvent({
        target: 'users:user2:follows:users:user1',
        ts: 2500,
        op: 'DELETE',
      }))
    })

    it('getExistingEntities excludes deleted', () => {
      const existing = collector.getExistingEntities()
      expect(existing).toHaveLength(1)
      expect(existing[0].id).toBe('user1')
    })

    it('getExistingRelationships excludes deleted', () => {
      const existing = collector.getExistingRelationships()
      expect(existing).toHaveLength(1)
      expect(existing[0].from).toBe('users:user1')
    })

    it('getStats returns accurate counts', () => {
      const stats = collector.getStats()
      expect(stats.entityCount).toBe(2)
      expect(stats.existingEntities).toBe(1)
      expect(stats.relationshipCount).toBe(2)
      expect(stats.existingRelationships).toBe(1)
    })
  })

  describe('clear', () => {
    it('clears all state', () => {
      collector.processEvent(createTestEvent({ target: 'users:user1' }))
      collector.processEvent(createTestEvent({ target: 'users:user1:follows:users:user2' }))

      collector.clear()

      expect(collector.getEntityStates()).toHaveLength(0)
      expect(collector.getRelationshipStates()).toHaveLength(0)
    })
  })
})

// =============================================================================
// EventCompactor Tests
// =============================================================================

describe('EventCompactor', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    storage = new MockStorage()
    manifest = new ManifestManager(storage, { dataset: 'test-app' })
    await manifest.load()

    compactor = new EventCompactor({
      dataset: 'test-app',
      storage,
      manifest,
    })

    stateWriter = new InMemoryStateWriter()
  })

  describe('compact', () => {
    it('returns empty result when no segments', async () => {
      const result = await compactor.compact({
        throughTimestamp: Date.now(),
        stateWriter,
      })

      expect(result.eventsProcessed).toBe(0)
      expect(result.entityCount).toBe(0)
      expect(result.segmentsCompacted).toHaveLength(0)
    })

    it('compacts events from segments', async () => {
      // Create segment with events
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
        createTestEvent({ target: 'users:user2', ts: 1500, op: 'CREATE', after: { name: 'User 2' } }),
        createTestEvent({ target: 'users:user1', ts: 2000, op: 'UPDATE', after: { name: 'User 1 Updated' } }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 2000,
        count: 3,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      const result = await compactor.compact({
        throughTimestamp: 3000,
        stateWriter,
      })

      expect(result.eventsProcessed).toBe(3)
      expect(result.entityCount).toBe(2)
      expect(stateWriter.entities).toHaveLength(2)

      const user1 = stateWriter.entities.find(e => e.id === 'user1')
      expect(user1?.state).toEqual({ name: 'User 1 Updated' })
    })

    it('filters events by throughTimestamp', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'V1' } }),
        createTestEvent({ target: 'users:user1', ts: 2000, op: 'UPDATE', after: { name: 'V2' } }),
        createTestEvent({ target: 'users:user1', ts: 3000, op: 'UPDATE', after: { name: 'V3' } }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 3000,
        count: 3,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      // Only compact through ts=2000
      const result = await compactor.compact({
        throughTimestamp: 2000,
        stateWriter,
      })

      expect(result.eventsProcessed).toBe(2) // Only first two events
      expect(stateWriter.entities[0].state).toEqual({ name: 'V2' })
    })

    it('handles relationships', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
        createTestEvent({ target: 'users:user2', ts: 1000, op: 'CREATE', after: { name: 'User 2' } }),
        createTestEvent({
          target: 'users:user1:follows:users:user2',
          ts: 2000,
          op: 'CREATE',
          after: { since: '2024-01-01' },
        }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 2000,
        count: 3,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      const result = await compactor.compact({
        throughTimestamp: 3000,
        stateWriter,
      })

      expect(result.entityCount).toBe(2)
      expect(result.relationshipCount).toBe(1)
      expect(stateWriter.relationships).toHaveLength(1)
      expect(stateWriter.relationships[0].predicate).toBe('follows')
    })

    it('creates snapshot when requested', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 1000,
        count: 1,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      const result = await compactor.compact({
        throughTimestamp: 2000,
        stateWriter,
        createSnapshot: true,
      })

      expect(result.snapshotPath).toBe('snapshots/2000')
      expect(stateWriter.snapshots.has(2000)).toBe(true)
    })

    it('updates manifest compactedThrough', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 1000,
        count: 1,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      await compactor.compact({
        throughTimestamp: 5000,
        stateWriter,
      })

      const compactedThrough = await manifest.getCompactedThrough()
      expect(compactedThrough).toBe(5000)
    })

    it('deletes segments when requested', async () => {
      const events1 = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
      ]
      const events2 = [
        createTestEvent({ target: 'users:user2', ts: 3000, op: 'CREATE', after: { name: 'User 2' } }),
      ]

      const segment1 = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 1000,
        count: 1,
      })
      const segment2 = createTestSegment({
        seq: 2,
        path: 'test-app/events/seg-0002.parquet',
        minTs: 3000,
        maxTs: 3000,
        count: 1,
      })

      storage.writeSegmentData(segment1.path, events1)
      storage.writeSegmentData(segment2.path, events2)
      await manifest.addSegment(segment1)
      await manifest.addSegment(segment2)

      // Compact through 2000 - should delete segment1 but not segment2
      await compactor.compact({
        throughTimestamp: 2000,
        stateWriter,
        deleteSegments: true,
      })

      expect(await storage.head(segment1.path)).toBe(false)
      expect(await storage.head(segment2.path)).toBe(true)

      const segments = await manifest.getSegments()
      expect(segments).toHaveLength(1)
      expect(segments[0].seq).toBe(2)
    })

    it('handles multiple segments', async () => {
      const events1 = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'V1' } }),
      ]
      const events2 = [
        createTestEvent({ target: 'users:user1', ts: 2000, op: 'UPDATE', after: { name: 'V2' } }),
      ]
      const events3 = [
        createTestEvent({ target: 'users:user1', ts: 3000, op: 'UPDATE', after: { name: 'V3' } }),
      ]

      const segment1 = createTestSegment({ seq: 1, path: 'test-app/events/seg-0001.parquet', minTs: 1000, maxTs: 1000, count: 1 })
      const segment2 = createTestSegment({ seq: 2, path: 'test-app/events/seg-0002.parquet', minTs: 2000, maxTs: 2000, count: 1 })
      const segment3 = createTestSegment({ seq: 3, path: 'test-app/events/seg-0003.parquet', minTs: 3000, maxTs: 3000, count: 1 })

      storage.writeSegmentData(segment1.path, events1)
      storage.writeSegmentData(segment2.path, events2)
      storage.writeSegmentData(segment3.path, events3)

      await manifest.addSegment(segment1)
      await manifest.addSegment(segment2)
      await manifest.addSegment(segment3)

      const result = await compactor.compact({
        throughTimestamp: 5000,
        stateWriter,
      })

      expect(result.eventsProcessed).toBe(3)
      expect(result.segmentsCompacted).toHaveLength(3)
      expect(stateWriter.entities[0].state).toEqual({ name: 'V3' })
    })

    it('excludes deleted entities from final state', async () => {
      const events = [
        createTestEvent({ target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
        createTestEvent({ target: 'users:user2', ts: 1500, op: 'CREATE', after: { name: 'User 2' } }),
        createTestEvent({ target: 'users:user1', ts: 2000, op: 'DELETE', before: { name: 'User 1' } }),
      ]

      const segment = createTestSegment({
        seq: 1,
        path: 'test-app/events/seg-0001.parquet',
        minTs: 1000,
        maxTs: 2000,
        count: 3,
      })

      storage.writeSegmentData(segment.path, events)
      await manifest.addSegment(segment)

      const result = await compactor.compact({
        throughTimestamp: 3000,
        stateWriter,
      })

      // Only user2 should exist
      expect(result.entityCount).toBe(1)
      expect(stateWriter.entities).toHaveLength(1)
      expect(stateWriter.entities[0].id).toBe('user2')
    })
  })

  describe('needsCompaction', () => {
    it('returns false for empty manifest', async () => {
      const result = await compactor.needsCompaction()
      expect(result.needed).toBe(false)
    })

    it('returns true when event count exceeds threshold', async () => {
      // Create compactor with low threshold
      compactor = new EventCompactor({
        dataset: 'test-app',
        storage,
        manifest,
        config: { minEvents: 10 },
      })

      // Add segments with more than 10 events
      await manifest.addSegment(createTestSegment({ count: 15 }))

      const result = await compactor.needsCompaction()
      expect(result.needed).toBe(true)
      expect(result.reason).toContain('exceeds threshold')
    })
  })
})

// =============================================================================
// InMemoryStateWriter Tests
// =============================================================================

describe('InMemoryStateWriter', () => {
  let writer: InMemoryStateWriter

  beforeEach(() => {
    writer = new InMemoryStateWriter()
  })

  it('stores entities', async () => {
    const entities = [
      { target: 'users:user1', ns: 'users', id: 'user1', state: { name: 'Test' }, lastEventTs: 1000, exists: true },
    ]

    await writer.writeEntities(entities)
    expect(writer.entities).toEqual(entities)
  })

  it('stores relationships', async () => {
    const relationships = [
      { target: 'u1:f:u2', from: 'u1', predicate: 'f', to: 'u2', data: {}, lastEventTs: 1000, exists: true },
    ]

    await writer.writeRelationships(relationships)
    expect(writer.relationships).toEqual(relationships)
  })

  it('creates snapshots', async () => {
    const path = await writer.createSnapshot(1000, [], [])
    expect(path).toBe('snapshots/1000')
    expect(writer.snapshots.has(1000)).toBe(true)
  })

  it('clears all data', () => {
    writer.entities = [{ target: 't', ns: 'n', id: 'i', state: {}, lastEventTs: 0, exists: true }]
    writer.clear()
    expect(writer.entities).toHaveLength(0)
    expect(writer.relationships).toHaveLength(0)
    expect(writer.snapshots.size).toBe(0)
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory functions', () => {
  it('createEventCompactor creates a compactor', () => {
    const storage = new MockStorage()
    const manifest = new ManifestManager(storage, { dataset: 'test' })
    const compactor = createEventCompactor({ dataset: 'test', storage, manifest })
    expect(compactor).toBeInstanceOf(EventCompactor)
  })

  it('createStateCollector creates a collector', () => {
    const collector = createStateCollector()
    expect(collector).toBeInstanceOf(StateCollector)
  })

  it('createInMemoryStateWriter creates a writer', () => {
    const writer = createInMemoryStateWriter()
    expect(writer).toBeInstanceOf(InMemoryStateWriter)
  })
})

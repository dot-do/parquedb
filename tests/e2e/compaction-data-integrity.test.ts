/**
 * Data Integrity Tests for Compaction
 *
 * Tests to verify data is not corrupted or lost during compaction:
 * 1. Row count preservation - total records before/after match
 * 2. Sort order maintenance - events remain in timestamp order
 * 3. Data consistency - field values are preserved exactly
 * 4. Entity deduplication - latest state wins for same entity
 * 5. Relationship preservation - all relationships survive compaction
 *
 * Issue: parquedb-c5r6
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventCompactor,
  StateCollector,
  InMemoryStateWriter,
  createEventCompactor,
  createStateCollector,
  createInMemoryStateWriter,
} from '@/events/compaction'
import { ManifestManager } from '@/events/manifest'
import type { SegmentStorage } from '@/events/segment'
import type { Event, Variant } from '@/types'
import type { EventBatch, EventSegment } from '@/events/types'

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

  writeSegmentData(path: string, events: Event[]): void {
    const lines = events.map(e => JSON.stringify(e))
    const json = lines.join('\n')
    this.files.set(path, new TextEncoder().encode(json))
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

let eventIdCounter = 0

function createTestEvent(overrides: Partial<Event> = {}): Event {
  eventIdCounter++
  return {
    id: `evt_${eventIdCounter}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
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

/**
 * Generate a deterministic dataset for testing
 */
function generateTestDataset(entityCount: number, updatesPerEntity: number = 1): Event[] {
  const events: Event[] = []
  const baseTs = 1000000

  for (let i = 0; i < entityCount; i++) {
    const entityId = `entity_${i.toString().padStart(5, '0')}`
    const ns = i % 3 === 0 ? 'users' : i % 3 === 1 ? 'posts' : 'comments'

    // Create event
    events.push(createTestEvent({
      target: `${ns}:${entityId}`,
      ts: baseTs + i * 100,
      op: 'CREATE',
      after: {
        id: entityId,
        name: `Entity ${i}`,
        index: i,
        createdAt: baseTs + i * 100,
        tags: [`tag${i % 5}`, `tag${(i + 1) % 5}`],
        metadata: {
          nested: {
            value: i * 10,
            flag: i % 2 === 0,
          },
        },
      },
    }))

    // Update events
    for (let j = 0; j < updatesPerEntity; j++) {
      events.push(createTestEvent({
        target: `${ns}:${entityId}`,
        ts: baseTs + i * 100 + (j + 1) * 10,
        op: 'UPDATE',
        before: {
          id: entityId,
          name: `Entity ${i}`,
          updateCount: j,
        },
        after: {
          id: entityId,
          name: `Entity ${i} (updated ${j + 1})`,
          updateCount: j + 1,
          lastUpdated: baseTs + i * 100 + (j + 1) * 10,
        },
      }))
    }
  }

  return events
}

/**
 * Generate relationship events with unique from/to combinations
 */
function generateRelationshipEvents(count: number): Event[] {
  const events: Event[] = []
  const baseTs = 2000000

  // Create unique relationships by using unique from/to pairs
  for (let i = 0; i < count; i++) {
    // Use a larger space of users to ensure unique relationships
    const fromId = `user_${Math.floor(i / 20).toString().padStart(3, '0')}`
    const toId = `user_${((i % 20) + 100).toString().padStart(3, '0')}`

    events.push(createTestEvent({
      target: `users:${fromId}:follows:users:${toId}`,
      ts: baseTs + i * 50,
      op: 'CREATE',
      after: {
        since: `2024-01-${(i % 28 + 1).toString().padStart(2, '0')}`,
        strength: (i % 10) / 10,
      },
    }))
  }

  return events
}

// =============================================================================
// Row Count Preservation Tests
// =============================================================================

describe('Row Count Preservation', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should preserve exact row count for single segment with unique entities', async () => {
    const entityCount = 100
    const events = generateTestDataset(entityCount, 0) // No updates, just creates

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: events[0]?.ts ?? 0,
      maxTs: events[events.length - 1]?.ts ?? 0,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    // All entities should be preserved
    expect(result.eventsProcessed).toBe(entityCount)
    expect(result.entityCount).toBe(entityCount)
    expect(stateWriter.entities).toHaveLength(entityCount)
  })

  it('should correctly deduplicate entities with multiple updates', async () => {
    const entityCount = 50
    const updatesPerEntity = 3
    const events = generateTestDataset(entityCount, updatesPerEntity)

    const totalEvents = entityCount * (1 + updatesPerEntity) // creates + updates

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: events[0]?.ts ?? 0,
      maxTs: events[events.length - 1]?.ts ?? 0,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    // All events should be processed
    expect(result.eventsProcessed).toBe(totalEvents)

    // But only unique entities should remain (deduplication)
    expect(result.entityCount).toBe(entityCount)
    expect(stateWriter.entities).toHaveLength(entityCount)

    // Verify each entity has the latest state (highest updateCount)
    for (const entity of stateWriter.entities) {
      expect(entity.state?.updateCount).toBe(updatesPerEntity)
    }
  })

  it('should preserve row count across multiple segments', async () => {
    const eventsPerSegment = 30
    const segmentCount = 5
    const totalExpectedEntities = eventsPerSegment * segmentCount

    for (let s = 0; s < segmentCount; s++) {
      const segmentEvents: Event[] = []

      for (let i = 0; i < eventsPerSegment; i++) {
        const entityIndex = s * eventsPerSegment + i
        segmentEvents.push(createTestEvent({
          target: `users:entity_${entityIndex.toString().padStart(5, '0')}`,
          ts: 1000000 + entityIndex * 100,
          op: 'CREATE',
          after: {
            index: entityIndex,
            segment: s,
          },
        }))
      }

      const segment = createTestSegment({
        seq: s + 1,
        path: `test-app/events/seg-${(s + 1).toString().padStart(4, '0')}.parquet`,
        minTs: segmentEvents[0]?.ts ?? 0,
        maxTs: segmentEvents[segmentEvents.length - 1]?.ts ?? 0,
        count: segmentEvents.length,
      })

      storage.writeSegmentData(segment.path, segmentEvents)
      await manifest.addSegment(segment)
    }

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(totalExpectedEntities)
    expect(result.entityCount).toBe(totalExpectedEntities)
    expect(stateWriter.entities).toHaveLength(totalExpectedEntities)
  })

  it('should correctly count entities after deletions', async () => {
    const events: Event[] = [
      // Create 10 entities
      ...Array.from({ length: 10 }, (_, i) =>
        createTestEvent({
          target: `users:user_${i}`,
          ts: 1000 + i,
          op: 'CREATE',
          after: { name: `User ${i}` },
        })
      ),
      // Delete 3 entities
      createTestEvent({
        target: 'users:user_2',
        ts: 2000,
        op: 'DELETE',
        before: { name: 'User 2' },
      }),
      createTestEvent({
        target: 'users:user_5',
        ts: 2001,
        op: 'DELETE',
        before: { name: 'User 5' },
      }),
      createTestEvent({
        target: 'users:user_8',
        ts: 2002,
        op: 'DELETE',
        before: { name: 'User 8' },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 2002,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    // 10 creates + 3 deletes = 13 events processed
    expect(result.eventsProcessed).toBe(13)

    // 10 - 3 = 7 entities remaining
    expect(result.entityCount).toBe(7)
    expect(stateWriter.entities).toHaveLength(7)

    // Verify deleted entities are not in the result
    const entityIds = stateWriter.entities.map(e => e.id)
    expect(entityIds).not.toContain('user_2')
    expect(entityIds).not.toContain('user_5')
    expect(entityIds).not.toContain('user_8')
  })

  it('should handle entity recreation after deletion', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User 1 - v1' },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 2000,
        op: 'DELETE',
        before: { name: 'User 1 - v1' },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 3000,
        op: 'CREATE',
        after: { name: 'User 1 - v2' },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 3000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(3)
    expect(result.entityCount).toBe(1)
    expect(stateWriter.entities).toHaveLength(1)
    expect(stateWriter.entities[0]?.state?.name).toBe('User 1 - v2')
  })
})

// =============================================================================
// Sort Order Maintenance Tests
// =============================================================================

describe('Sort Order Maintenance', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should apply events in timestamp order', async () => {
    // Create events out of timestamp order
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_1',
        ts: 3000,
        op: 'UPDATE',
        after: { name: 'Third (latest)', version: 3 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'First', version: 1 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Second', version: 2 },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 3000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.entityCount).toBe(1)

    // Should have the latest state (timestamp 3000)
    expect(stateWriter.entities[0]?.state?.name).toBe('Third (latest)')
    expect(stateWriter.entities[0]?.state?.version).toBe(3)
    expect(stateWriter.entities[0]?.lastEventTs).toBe(3000)
  })

  it('should maintain timestamp ordering across multiple segments', async () => {
    // Segment 1: timestamps 1000-1999
    const segment1Events: Event[] = [
      createTestEvent({
        target: 'users:user_1',
        ts: 1500,
        op: 'UPDATE',
        after: { name: 'Segment 1 update', version: 2 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Segment 1 create', version: 1 },
      }),
    ]

    // Segment 2: timestamps 2000-2999 (later)
    const segment2Events: Event[] = [
      createTestEvent({
        target: 'users:user_1',
        ts: 2500,
        op: 'UPDATE',
        after: { name: 'Segment 2 update 2', version: 4 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Segment 2 update 1', version: 3 },
      }),
    ]

    const seg1 = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1500,
      count: segment1Events.length,
    })

    const seg2 = createTestSegment({
      seq: 2,
      path: 'test-app/events/seg-0002.parquet',
      minTs: 2000,
      maxTs: 2500,
      count: segment2Events.length,
    })

    storage.writeSegmentData(seg1.path, segment1Events)
    storage.writeSegmentData(seg2.path, segment2Events)
    await manifest.addSegment(seg1)
    await manifest.addSegment(seg2)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(4)
    expect(result.entityCount).toBe(1)

    // Should have the latest state from segment 2
    expect(stateWriter.entities[0]?.state?.name).toBe('Segment 2 update 2')
    expect(stateWriter.entities[0]?.state?.version).toBe(4)
    expect(stateWriter.entities[0]?.lastEventTs).toBe(2500)
  })

  it('should handle concurrent events with same timestamp', async () => {
    // Events with identical timestamps - last one processed should win
    const events: Event[] = [
      createTestEvent({
        id: 'evt_aaa',
        target: 'users:user_1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'First event', order: 1 },
      }),
      createTestEvent({
        id: 'evt_bbb',
        target: 'users:user_1',
        ts: 1000,
        op: 'UPDATE',
        after: { name: 'Second event', order: 2 },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.entityCount).toBe(1)
    // Both events have same timestamp, but the second should be processed last
    // due to file ordering
    expect(stateWriter.entities[0]?.state?.order).toBe(2)
  })

  it('should filter events by throughTimestamp correctly', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'V1', version: 1 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'V2', version: 2 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 3000,
        op: 'UPDATE',
        after: { name: 'V3', version: 3 },
      }),
      createTestEvent({
        target: 'users:user_1',
        ts: 4000,
        op: 'UPDATE',
        after: { name: 'V4', version: 4 },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 4000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    // Only compact through timestamp 2500
    const result = await compactor.compact({
      throughTimestamp: 2500,
      stateWriter,
    })

    // Only first 2 events should be processed
    expect(result.eventsProcessed).toBe(2)
    expect(result.entityCount).toBe(1)
    expect(stateWriter.entities[0]?.state?.version).toBe(2)
    expect(stateWriter.entities[0]?.lastEventTs).toBe(2000)
  })
})

// =============================================================================
// Data Consistency Tests
// =============================================================================

describe('Data Consistency', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should preserve all data types correctly', async () => {
    const complexData: Variant = {
      string: 'hello world',
      number: 42,
      float: 3.14159,
      boolean: true,
      null: null,
      array: [1, 'two', true, null, { nested: 'value' }],
      object: {
        nested: {
          deeply: {
            value: 'found',
          },
        },
      },
      emptyArray: [],
      emptyObject: {},
      unicode: 'Hello World',
      specialChars: '!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~',
      largeNumber: 9007199254740991, // Max safe integer
      negativeNumber: -42,
      zero: 0,
    }

    const events: Event[] = [
      createTestEvent({
        target: 'users:user_complex',
        ts: 1000,
        op: 'CREATE',
        after: complexData,
      }),
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
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.entityCount).toBe(1)

    const state = stateWriter.entities[0]?.state
    expect(state).toEqual(complexData)

    // Verify specific types
    expect(typeof state?.string).toBe('string')
    expect(typeof state?.number).toBe('number')
    expect(typeof state?.float).toBe('number')
    expect(typeof state?.boolean).toBe('boolean')
    expect(state?.null).toBeNull()
    expect(Array.isArray(state?.array)).toBe(true)
    expect(typeof state?.object).toBe('object')
  })

  it('should preserve entity identity (ns and id)', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_abc123',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User ABC' },
      }),
      createTestEvent({
        target: 'posts:post_xyz789',
        ts: 1001,
        op: 'CREATE',
        after: { title: 'Post XYZ' },
      }),
      createTestEvent({
        target: 'comments:comment_def456',
        ts: 1002,
        op: 'CREATE',
        after: { content: 'Comment DEF' },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1002,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(stateWriter.entities).toHaveLength(3)

    const user = stateWriter.entities.find(e => e.ns === 'users')
    expect(user?.id).toBe('user_abc123')
    expect(user?.target).toBe('users:user_abc123')

    const post = stateWriter.entities.find(e => e.ns === 'posts')
    expect(post?.id).toBe('post_xyz789')
    expect(post?.target).toBe('posts:post_xyz789')

    const comment = stateWriter.entities.find(e => e.ns === 'comments')
    expect(comment?.id).toBe('comment_def456')
    expect(comment?.target).toBe('comments:comment_def456')
  })

  it('should preserve large text content', async () => {
    const largeText = 'A'.repeat(100000) // 100KB of text

    const events: Event[] = [
      createTestEvent({
        target: 'documents:doc_large',
        ts: 1000,
        op: 'CREATE',
        after: {
          content: largeText,
          length: largeText.length,
        },
      }),
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
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(stateWriter.entities).toHaveLength(1)
    expect(stateWriter.entities[0]?.state?.content).toBe(largeText)
    expect(stateWriter.entities[0]?.state?.content?.length).toBe(100000)
  })

  it('should handle before/after state transitions correctly', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_state',
        ts: 1000,
        op: 'CREATE',
        after: { a: 1, b: 2, c: 3 },
      }),
      createTestEvent({
        target: 'users:user_state',
        ts: 2000,
        op: 'UPDATE',
        before: { a: 1, b: 2, c: 3 },
        after: { a: 10, b: 2, c: 3, d: 4 }, // a changed, d added
      }),
      createTestEvent({
        target: 'users:user_state',
        ts: 3000,
        op: 'UPDATE',
        before: { a: 10, b: 2, c: 3, d: 4 },
        after: { a: 10, b: 20, d: 4 }, // c removed, b changed
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 3000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(stateWriter.entities).toHaveLength(1)

    const finalState = stateWriter.entities[0]?.state
    expect(finalState).toEqual({ a: 10, b: 20, d: 4 })
    expect(finalState?.c).toBeUndefined()
  })
})

// =============================================================================
// Relationship Preservation Tests
// =============================================================================

describe('Relationship Preservation', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should preserve all relationship data', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1000,
        op: 'CREATE',
        after: {
          since: '2024-01-01',
          strength: 0.8,
          metadata: { source: 'import' },
        },
      }),
      createTestEvent({
        target: 'users:user1:likes:posts:post1',
        ts: 1001,
        op: 'CREATE',
        after: {
          at: '2024-01-02T12:00:00Z',
          reaction: 'heart',
        },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1001,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.relationshipCount).toBe(2)
    expect(stateWriter.relationships).toHaveLength(2)

    const followRel = stateWriter.relationships.find(r => r.predicate === 'follows')
    expect(followRel?.from).toBe('users:user1')
    expect(followRel?.to).toBe('users:user2')
    expect(followRel?.data?.since).toBe('2024-01-01')
    expect(followRel?.data?.strength).toBe(0.8)

    const likeRel = stateWriter.relationships.find(r => r.predicate === 'likes')
    expect(likeRel?.from).toBe('users:user1')
    expect(likeRel?.to).toBe('posts:post1')
    expect(likeRel?.data?.reaction).toBe('heart')
  })

  it('should handle relationship updates correctly', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1000,
        op: 'CREATE',
        after: { strength: 0.5 },
      }),
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 2000,
        op: 'UPDATE',
        before: { strength: 0.5 },
        after: { strength: 0.9, note: 'upgraded' },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 2000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.relationshipCount).toBe(1)
    expect(stateWriter.relationships).toHaveLength(1)
    expect(stateWriter.relationships[0]?.data?.strength).toBe(0.9)
    expect(stateWriter.relationships[0]?.data?.note).toBe('upgraded')
    expect(stateWriter.relationships[0]?.lastEventTs).toBe(2000)
  })

  it('should correctly handle relationship deletion', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1000,
        op: 'CREATE',
        after: { strength: 0.5 },
      }),
      createTestEvent({
        target: 'users:user1:follows:users:user3',
        ts: 1001,
        op: 'CREATE',
        after: { strength: 0.7 },
      }),
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 2000,
        op: 'DELETE',
        before: { strength: 0.5 },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 2000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    // Only 1 relationship should remain
    expect(result.relationshipCount).toBe(1)
    expect(stateWriter.relationships).toHaveLength(1)
    expect(stateWriter.relationships[0]?.to).toBe('users:user3')
  })

  it('should preserve high volume of relationships', async () => {
    const relationshipCount = 200
    const events = generateRelationshipEvents(relationshipCount)

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: events[0]?.ts ?? 0,
      maxTs: events[events.length - 1]?.ts ?? 0,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(relationshipCount)
    expect(result.relationshipCount).toBe(relationshipCount)
    expect(stateWriter.relationships).toHaveLength(relationshipCount)

    // Verify each relationship has correct structure
    for (const rel of stateWriter.relationships) {
      expect(rel.from).toMatch(/^users:user_\d{3}$/)
      expect(rel.to).toMatch(/^users:user_\d{3}$/)
      expect(rel.predicate).toBe('follows')
      expect(rel.exists).toBe(true)
      expect(rel.data?.since).toBeDefined()
      expect(rel.data?.strength).toBeGreaterThanOrEqual(0)
      expect(rel.data?.strength).toBeLessThanOrEqual(1)
    }
  })
})

// =============================================================================
// Snapshot Integrity Tests
// =============================================================================

describe('Snapshot Integrity', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should create snapshot with complete state', async () => {
    const entityCount = 25
    const relationshipCount = 15

    const entityEvents = generateTestDataset(entityCount, 1)
    const relEvents = generateRelationshipEvents(relationshipCount)
    const events = [...entityEvents, ...relEvents]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: events[0]?.ts ?? 0,
      maxTs: events[events.length - 1]?.ts ?? 0,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const snapshotTs = Date.now()
    const result = await compactor.compact({
      throughTimestamp: snapshotTs,
      stateWriter,
      createSnapshot: true,
    })

    expect(result.snapshotPath).toBe(`snapshots/${snapshotTs}`)
    expect(stateWriter.snapshots.has(snapshotTs)).toBe(true)

    const snapshot = stateWriter.snapshots.get(snapshotTs)
    expect(snapshot?.entities).toHaveLength(entityCount)
    expect(snapshot?.relationships).toHaveLength(relationshipCount)

    // Snapshot should match writer state
    expect(snapshot?.entities).toEqual(stateWriter.entities)
    expect(snapshot?.relationships).toEqual(stateWriter.relationships)
  })

  it('should maintain snapshot consistency with main state', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user_snap',
        ts: 1000,
        op: 'CREATE',
        after: { version: 1 },
      }),
      createTestEvent({
        target: 'users:user_snap',
        ts: 2000,
        op: 'UPDATE',
        after: { version: 2 },
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 2000,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const snapshotTs = 5000
    await compactor.compact({
      throughTimestamp: snapshotTs,
      stateWriter,
      createSnapshot: true,
    })

    const snapshot = stateWriter.snapshots.get(snapshotTs)

    // Verify snapshot matches current state exactly
    expect(snapshot?.entities).toHaveLength(1)
    expect(snapshot?.entities[0]?.state?.version).toBe(2)
    expect(snapshot?.entities[0]?.lastEventTs).toBe(2000)
    expect(snapshot?.entities[0]).toEqual(stateWriter.entities[0])
  })
})

// =============================================================================
// Edge Case Tests
// =============================================================================

describe('Edge Cases', () => {
  let storage: MockStorage
  let manifest: ManifestManager
  let compactor: EventCompactor
  let stateWriter: InMemoryStateWriter

  beforeEach(async () => {
    eventIdCounter = 0
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

  it('should handle empty segment gracefully', async () => {
    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1000,
      count: 0,
    })

    storage.writeSegmentData(segment.path, [])
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(0)
    expect(result.entityCount).toBe(0)
    expect(result.relationshipCount).toBe(0)
  })

  it('should handle entity with empty state object', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:empty_user',
        ts: 1000,
        op: 'CREATE',
        after: {},
      }),
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
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.entityCount).toBe(1)
    expect(stateWriter.entities[0]?.state).toEqual({})
  })

  it('should handle very long entity IDs', async () => {
    const longId = 'a'.repeat(500)

    const events: Event[] = [
      createTestEvent({
        target: `users:${longId}`,
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Long ID entity' },
      }),
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
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.entityCount).toBe(1)
    expect(stateWriter.entities[0]?.id).toBe(longId)
    expect(stateWriter.entities[0]?.id?.length).toBe(500)
  })

  it('should handle mixed entities and relationships in same segment', async () => {
    const events: Event[] = [
      createTestEvent({
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User 1' },
      }),
      createTestEvent({
        target: 'users:user1:follows:users:user2',
        ts: 1001,
        op: 'CREATE',
        after: {},
      }),
      createTestEvent({
        target: 'users:user2',
        ts: 1002,
        op: 'CREATE',
        after: { name: 'User 2' },
      }),
      createTestEvent({
        target: 'users:user2:follows:users:user1',
        ts: 1003,
        op: 'CREATE',
        after: {},
      }),
    ]

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: 1000,
      maxTs: 1003,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(4)
    expect(result.entityCount).toBe(2)
    expect(result.relationshipCount).toBe(2)
  })

  it('should handle compaction with no segments', async () => {
    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(0)
    expect(result.entityCount).toBe(0)
    expect(result.relationshipCount).toBe(0)
    expect(result.segmentsCompacted).toHaveLength(0)
  })

  it('should handle rapid create-delete-create cycles', async () => {
    const cycles = 10
    const events: Event[] = []

    for (let i = 0; i < cycles; i++) {
      const baseTs = i * 100

      events.push(createTestEvent({
        target: 'users:cycle_user',
        ts: baseTs + 1,
        op: 'CREATE',
        after: { cycle: i, phase: 'created' },
      }))

      events.push(createTestEvent({
        target: 'users:cycle_user',
        ts: baseTs + 2,
        op: 'DELETE',
        before: { cycle: i, phase: 'created' },
      }))

      events.push(createTestEvent({
        target: 'users:cycle_user',
        ts: baseTs + 3,
        op: 'CREATE',
        after: { cycle: i, phase: 'recreated' },
      }))
    }

    const segment = createTestSegment({
      seq: 1,
      path: 'test-app/events/seg-0001.parquet',
      minTs: events[0]?.ts ?? 0,
      maxTs: events[events.length - 1]?.ts ?? 0,
      count: events.length,
    })

    storage.writeSegmentData(segment.path, events)
    await manifest.addSegment(segment)

    const result = await compactor.compact({
      throughTimestamp: Date.now(),
      stateWriter,
    })

    expect(result.eventsProcessed).toBe(cycles * 3)
    expect(result.entityCount).toBe(1) // Only one unique entity

    // Should have the state from the last cycle's recreate
    expect(stateWriter.entities[0]?.state?.cycle).toBe(cycles - 1)
    expect(stateWriter.entities[0]?.state?.phase).toBe('recreated')
  })
})

// =============================================================================
// StateCollector Unit Tests for Data Integrity
// =============================================================================

describe('StateCollector Data Integrity', () => {
  let collector: StateCollector

  beforeEach(() => {
    eventIdCounter = 0
    collector = createStateCollector()
  })

  it('should maintain accurate stats throughout collection', () => {
    // Add some entities
    for (let i = 0; i < 10; i++) {
      collector.processEvent(createTestEvent({
        target: `users:user_${i}`,
        ts: 1000 + i,
        op: 'CREATE',
        after: { index: i },
      }))
    }

    let stats = collector.getStats()
    expect(stats.entityCount).toBe(10)
    expect(stats.existingEntities).toBe(10)

    // Delete some
    for (let i = 0; i < 3; i++) {
      collector.processEvent(createTestEvent({
        target: `users:user_${i}`,
        ts: 2000 + i,
        op: 'DELETE',
      }))
    }

    stats = collector.getStats()
    expect(stats.entityCount).toBe(10) // Total tracked
    expect(stats.existingEntities).toBe(7) // Still existing

    // Add relationships
    collector.processEvent(createTestEvent({
      target: 'users:user_5:follows:users:user_6',
      ts: 3000,
      op: 'CREATE',
      after: {},
    }))

    stats = collector.getStats()
    expect(stats.relationshipCount).toBe(1)
    expect(stats.existingRelationships).toBe(1)
  })

  it('should correctly differentiate existing vs deleted entities', () => {
    collector.processEvent(createTestEvent({
      target: 'users:user_alive',
      ts: 1000,
      op: 'CREATE',
      after: { status: 'alive' },
    }))

    collector.processEvent(createTestEvent({
      target: 'users:user_deleted',
      ts: 1001,
      op: 'CREATE',
      after: { status: 'alive' },
    }))

    collector.processEvent(createTestEvent({
      target: 'users:user_deleted',
      ts: 2000,
      op: 'DELETE',
    }))

    const allEntities = collector.getEntityStates()
    const existingEntities = collector.getExistingEntities()

    expect(allEntities).toHaveLength(2)
    expect(existingEntities).toHaveLength(1)
    expect(existingEntities[0]?.id).toBe('user_alive')
    expect(existingEntities[0]?.exists).toBe(true)

    const deletedEntity = allEntities.find(e => e.id === 'user_deleted')
    expect(deletedEntity?.exists).toBe(false)
  })

  it('should clear state completely', () => {
    // Add some data
    for (let i = 0; i < 5; i++) {
      collector.processEvent(createTestEvent({
        target: `users:user_${i}`,
        ts: 1000 + i,
        op: 'CREATE',
        after: { index: i },
      }))

      collector.processEvent(createTestEvent({
        target: `users:user_${i}:follows:users:user_${(i + 1) % 5}`,
        ts: 2000 + i,
        op: 'CREATE',
        after: {},
      }))
    }

    let stats = collector.getStats()
    expect(stats.entityCount).toBe(5)
    expect(stats.relationshipCount).toBe(5)

    collector.clear()

    stats = collector.getStats()
    expect(stats.entityCount).toBe(0)
    expect(stats.existingEntities).toBe(0)
    expect(stats.relationshipCount).toBe(0)
    expect(stats.existingRelationships).toBe(0)
    expect(collector.getEntityStates()).toHaveLength(0)
    expect(collector.getRelationshipStates()).toHaveLength(0)
  })
})

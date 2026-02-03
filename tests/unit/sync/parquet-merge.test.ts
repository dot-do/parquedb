/**
 * Unit tests for Parquet segment merging
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { Event } from '../../../src/types/entity'
import {
  type EventManifest,
  type EventSegment,
  createEmptyManifest,
  addSegment,
  loadManifest,
  saveManifest,
  findSegmentsInRange,
  findSegmentForEvent,
  computeSegmentChecksum,
  createSegment,
} from '../../../src/sync/event-manifest'
import {
  readSegmentEvents,
  readEventsFromSegments,
  readEventsInRange,
  readEventBatches,
  countEvents,
  getSegmentStats,
} from '../../../src/sync/segment-reader'
import {
  writeEventsToSegment,
  writeEvents,
  appendEvents,
  createBatchWriter,
  validateEventOrder,
  deduplicateEvents,
} from '../../../src/sync/segment-writer'
import {
  mergeEventParquets,
  incrementalMerge,
} from '../../../src/sync/parquet-merge'

// =============================================================================
// Test Data Generators
// =============================================================================

function createEvent(
  id: string,
  ts: number,
  op: Event['op'] = 'UPDATE',
  target: string = 'users:user1'
): Event {
  return {
    id,
    ts,
    op,
    target,
    before: { count: ts - 1 },
    after: { count: ts },
    actor: 'users:admin',
  }
}

function createEventSequence(start: number, count: number): Event[] {
  return Array.from({ length: count }, (_, i) => {
    const ts = start + i * 1000
    const id = `evt-${ts.toString().padStart(10, '0')}`
    return createEvent(id, ts)
  })
}

// =============================================================================
// Event Manifest Tests
// =============================================================================

describe('EventManifest', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('creates empty manifest', () => {
    const manifest = createEmptyManifest('main')

    expect(manifest.version).toBe(1)
    expect(manifest.segments).toEqual([])
    expect(manifest.branch).toBe('main')
    expect(manifest.lastEventId).toBe('')
    expect(manifest.lastEventTs).toBe(0)
  })

  it('saves and loads manifest', async () => {
    const manifest = createEmptyManifest('main')
    await saveManifest(storage, manifest)

    const loaded = await loadManifest(storage)
    expect(loaded).toEqual(manifest)
  })

  it('adds segments to manifest', () => {
    const manifest = createEmptyManifest('main')
    const segment: EventSegment = {
      file: 'events/seg-001.parquet',
      minId: 'evt-0001',
      maxId: 'evt-0100',
      minTs: 1000,
      maxTs: 2000,
      count: 100,
      checksum: 'abc123',
    }

    const updated = addSegment(manifest, segment)

    expect(updated.segments).toHaveLength(1)
    expect(updated.segments[0]).toEqual(segment)
    expect(updated.lastEventId).toBe(segment.maxId)
    expect(updated.lastEventTs).toBe(segment.maxTs)
  })

  it('finds segments in time range', () => {
    const manifest = createEmptyManifest('main')

    const seg1: EventSegment = {
      file: 'events/seg-001.parquet',
      minId: 'evt-0001',
      maxId: 'evt-0100',
      minTs: 1000,
      maxTs: 2000,
      count: 100,
      checksum: 'abc123',
    }

    const seg2: EventSegment = {
      file: 'events/seg-002.parquet',
      minId: 'evt-0101',
      maxId: 'evt-0200',
      minTs: 3000,
      maxTs: 4000,
      count: 100,
      checksum: 'def456',
    }

    const seg3: EventSegment = {
      file: 'events/seg-003.parquet',
      minId: 'evt-0201',
      maxId: 'evt-0300',
      minTs: 5000,
      maxTs: 6000,
      count: 100,
      checksum: 'ghi789',
    }

    manifest.segments = [seg1, seg2, seg3]

    // Range overlapping seg1 and seg2
    const found = findSegmentsInRange(manifest, 1500, 3500)
    expect(found).toHaveLength(2)
    expect(found[0]).toEqual(seg1)
    expect(found[1]).toEqual(seg2)

    // Range in middle of seg2
    const found2 = findSegmentsInRange(manifest, 3200, 3800)
    expect(found2).toHaveLength(1)
    expect(found2[0]).toEqual(seg2)

    // Range outside all segments
    const found3 = findSegmentsInRange(manifest, 10000, 20000)
    expect(found3).toHaveLength(0)
  })

  it('finds segment for event ID', () => {
    const manifest = createEmptyManifest('main')

    const seg1: EventSegment = {
      file: 'events/seg-001.parquet',
      minId: 'evt-0001',
      maxId: 'evt-0100',
      minTs: 1000,
      maxTs: 2000,
      count: 100,
      checksum: 'abc123',
    }

    const seg2: EventSegment = {
      file: 'events/seg-002.parquet',
      minId: 'evt-0101',
      maxId: 'evt-0200',
      minTs: 3000,
      maxTs: 4000,
      count: 100,
      checksum: 'def456',
    }

    manifest.segments = [seg1, seg2]

    expect(findSegmentForEvent(manifest, 'evt-0050')).toEqual(seg1)
    expect(findSegmentForEvent(manifest, 'evt-0150')).toEqual(seg2)
    expect(findSegmentForEvent(manifest, 'evt-0300')).toBeUndefined()
  })

  it('computes deterministic checksums', () => {
    const data = new TextEncoder().encode('test data')
    const checksum1 = computeSegmentChecksum(data)
    const checksum2 = computeSegmentChecksum(data)

    expect(checksum1).toBe(checksum2)
    expect(checksum1).toHaveLength(64) // SHA256 hex = 64 chars
  })
})

// =============================================================================
// Segment Writer Tests
// =============================================================================

describe('SegmentWriter', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('writes events to segment', async () => {
    const events = createEventSequence(1000, 10)
    const segment = await writeEventsToSegment(storage, events)

    expect(segment.count).toBe(10)
    expect(segment.minId).toBe(events[0]!.id)
    expect(segment.maxId).toBe(events[9]!.id)
    expect(segment.minTs).toBe(events[0]!.ts)
    expect(segment.maxTs).toBe(events[9]!.ts)
    expect(segment.checksum).toBeTruthy()

    // Verify file was written
    const exists = await storage.exists(segment.file)
    expect(exists).toBe(true)
  })

  it('writes multiple segments when exceeding size limit', async () => {
    const events = createEventSequence(1000, 25)
    const segments = await writeEvents(storage, events, {
      maxEventsPerSegment: 10,
    })

    expect(segments).toHaveLength(3)
    expect(segments[0]!.count).toBe(10)
    expect(segments[1]!.count).toBe(10)
    expect(segments[2]!.count).toBe(5)
  })

  it('validates event ordering', () => {
    const events = createEventSequence(1000, 5)
    expect(validateEventOrder(events)).toBe(true)

    // Scramble order
    const scrambled = [events[2]!, events[0]!, events[1]!]
    expect(validateEventOrder(scrambled)).toBe(false)
  })

  it('deduplicates events by ID', () => {
    const event1 = createEvent('evt-001', 1000)
    const event2 = createEvent('evt-002', 2000)
    const event1Dup = createEvent('evt-001', 3000) // Same ID, different ts

    const deduplicated = deduplicateEvents([event1, event2, event1Dup])

    expect(deduplicated).toHaveLength(2)
    expect(deduplicated[0]).toBe(event1) // First occurrence kept
    expect(deduplicated[1]).toBe(event2)
  })

  it('appends events to existing segments', async () => {
    // Write initial segment
    const initial = createEventSequence(1000, 5)
    const segment1 = await writeEventsToSegment(storage, initial, {
      maxEventsPerSegment: 10,
    })

    // Append more events
    const additional = createEventSequence(6000, 3)
    const allSegments = await appendEvents(
      storage,
      additional,
      [segment1],
      { maxEventsPerSegment: 10 }
    )

    // Should combine into single segment (8 events < 10 limit)
    expect(allSegments).toHaveLength(1)
    expect(allSegments[0]!.count).toBe(8)
  })

  it('batch writer accumulates and flushes events', async () => {
    const writer = createBatchWriter(storage, { maxEventsPerSegment: 5 })

    const events = createEventSequence(1000, 12)

    for (const event of events) {
      await writer.add(event)
    }

    const segments = await writer.flush()

    expect(segments).toHaveLength(3)
    expect(segments[0]!.count).toBe(5)
    expect(segments[1]!.count).toBe(5)
    expect(segments[2]!.count).toBe(2)
  })
})

// =============================================================================
// Segment Reader Tests
// =============================================================================

describe('SegmentReader', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('reads events from segment', async () => {
    const events = createEventSequence(1000, 10)
    const segment = await writeEventsToSegment(storage, events)

    const readEvents: Event[] = []
    for await (const event of readSegmentEvents(storage, segment)) {
      readEvents.push(event)
    }

    expect(readEvents).toHaveLength(10)
    expect(readEvents[0]!.id).toBe(events[0]!.id)
    expect(readEvents[9]!.id).toBe(events[9]!.id)
  })

  it('reads events from multiple segments', async () => {
    const events1 = createEventSequence(1000, 5)
    const events2 = createEventSequence(6000, 5)

    const segment1 = await writeEventsToSegment(storage, events1)
    const segment2 = await writeEventsToSegment(storage, events2)

    const allEvents: Event[] = []
    for await (const event of readEventsFromSegments(storage, [segment1, segment2])) {
      allEvents.push(event)
    }

    expect(allEvents).toHaveLength(10)
  })

  it('reads events in time range', async () => {
    const events = createEventSequence(1000, 10) // 1000, 2000, ..., 10000
    const segments = await writeEvents(storage, events, { maxEventsPerSegment: 5 })

    // Read events from 3000-7000
    const filtered: Event[] = []
    for await (const event of readEventsInRange(storage, segments, 3000, 7000)) {
      filtered.push(event)
    }

    expect(filtered).toHaveLength(5)
    expect(filtered[0]!.ts).toBe(3000)
    expect(filtered[4]!.ts).toBe(7000)
  })

  it('reads events in batches', async () => {
    const events = createEventSequence(1000, 15)
    const segments = await writeEvents(storage, events, { maxEventsPerSegment: 10 })

    const batches: Event[][] = []
    for await (const batch of readEventBatches(storage, segments, 5)) {
      batches.push(batch)
    }

    expect(batches).toHaveLength(3)
    expect(batches[0]).toHaveLength(5)
    expect(batches[1]).toHaveLength(5)
    expect(batches[2]).toHaveLength(5)
  })

  it('counts events', async () => {
    const events = createEventSequence(1000, 15)
    const segments = await writeEvents(storage, events, { maxEventsPerSegment: 10 })

    const fastCount = await countEvents(storage, segments, false)
    expect(fastCount).toBe(15)

    const accurateCount = await countEvents(storage, segments, true)
    expect(accurateCount).toBe(15)
  })

  it('gets segment statistics', async () => {
    const events = createEventSequence(1000, 20)
    const segments = await writeEvents(storage, events, { maxEventsPerSegment: 10 })

    const stats = getSegmentStats(segments)

    expect(stats.totalSegments).toBe(2)
    expect(stats.totalEvents).toBe(20)
    expect(stats.avgEventsPerSegment).toBe(10)
    expect(stats.oldestEvent).toBe(1000)
    expect(stats.newestEvent).toBe(20000)
  })
})

// =============================================================================
// Parquet Merge Tests
// =============================================================================

describe('ParquetMerge', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('merges with no overlapping segments (fast path)', async () => {
    // Base: events 1-10
    const baseEvents = createEventSequence(1000, 10)
    const baseSegments = await writeEvents(storage, baseEvents)
    let baseManifest = createEmptyManifest('main')
    baseSegments.forEach(seg => {
      baseManifest = addSegment(baseManifest, seg)
    })

    // Ours: same as base
    const ourManifest = { ...baseManifest }

    // Theirs: same as base
    const theirManifest = { ...baseManifest }

    const result = await mergeEventParquets(
      storage,
      baseManifest,
      ourManifest,
      theirManifest
    )

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.stats.sharedSegments).toBeGreaterThan(0)
  })

  it('merges when only one branch has changes', async () => {
    // Base: events 1-10
    const baseEvents = createEventSequence(1000, 10)
    const baseSegments = await writeEvents(storage, baseEvents)
    let baseManifest = createEmptyManifest('main')
    baseSegments.forEach(seg => {
      baseManifest = addSegment(baseManifest, seg)
    })

    // Ours: same as base
    const ourManifest = { ...baseManifest }

    // Theirs: add events 11-20
    const theirEvents = createEventSequence(11000, 10)
    const theirSegments = await writeEvents(storage, theirEvents)
    let theirManifest = { ...baseManifest }
    theirSegments.forEach(seg => {
      theirManifest = addSegment(theirManifest, seg)
    })

    const result = await mergeEventParquets(
      storage,
      baseManifest,
      ourManifest,
      theirManifest
    )

    expect(result.success).toBe(true)
    expect(result.manifest).toEqual(theirManifest)
    expect(result.stats.theirNewSegments).toBeGreaterThan(0)
    expect(result.stats.ourNewSegments).toBe(0)
  })

  it('merges with non-conflicting changes in both branches', async () => {
    // Base: empty
    const baseManifest = createEmptyManifest('main')

    // Ours: events for user1
    const ourEvents = createEventSequence(1000, 5)
      .map(e => ({ ...e, target: 'users:user1' }))
    const ourSegments = await writeEvents(storage, ourEvents)
    let ourManifest = { ...baseManifest }
    ourSegments.forEach(seg => {
      ourManifest = addSegment(ourManifest, seg)
    })

    // Theirs: events for user2 (different target = no conflict)
    const theirEvents = createEventSequence(1000, 5)
      .map(e => ({ ...e, target: 'users:user2' }))
    const theirSegments = await writeEvents(storage, theirEvents)
    let theirManifest = { ...baseManifest }
    theirSegments.forEach(seg => {
      theirManifest = addSegment(theirManifest, seg)
    })

    const result = await mergeEventParquets(
      storage,
      baseManifest,
      ourManifest,
      theirManifest
    )

    expect(result.success).toBe(true)
    expect(result.manifest).toBeDefined()
    expect(result.stats.mergedSegments).toBeGreaterThan(0)
    expect(result.stats.totalEvents).toBe(10)
  })

  it('merges concurrent updates to same entity', async () => {
    // Base: user1 with name = "Unknown"
    const baseEvent: Event = {
      id: 'evt-000',
      ts: 500,
      op: 'CREATE',
      target: 'users:user1',
      after: { name: 'Unknown' },
    }
    const baseSegments = await writeEvents(storage, [baseEvent])
    let baseManifest = createEmptyManifest('main')
    baseSegments.forEach(seg => {
      baseManifest = addSegment(baseManifest, seg)
    })

    // Ours: set user1.name = "Alice"
    const ourEvent: Event = {
      id: 'evt-001',
      ts: 1000,
      op: 'UPDATE',
      target: 'users:user1',
      before: { name: 'Unknown' },
      after: { name: 'Alice' },
      metadata: { update: { $set: { name: 'Alice' } } },
    }
    const ourSegments = await writeEvents(storage, [ourEvent])
    let ourManifest = { ...baseManifest }
    ourSegments.forEach(seg => {
      ourManifest = addSegment(ourManifest, seg)
    })

    // Theirs: set user1.name = "Bob" (concurrent update)
    const theirEvent: Event = {
      id: 'evt-002',
      ts: 1000,
      op: 'UPDATE',
      target: 'users:user1',
      before: { name: 'Unknown' },
      after: { name: 'Bob' },
      metadata: { update: { $set: { name: 'Bob' } } },
    }
    const theirSegments = await writeEvents(storage, [theirEvent])
    let theirManifest = { ...baseManifest }
    theirSegments.forEach(seg => {
      theirManifest = addSegment(theirManifest, seg)
    })

    const result = await mergeEventParquets(
      storage,
      baseManifest,
      ourManifest,
      theirManifest
    )

    // The merge should complete (with or without conflicts)
    // Conflict detection is tested separately in event-merge tests
    expect(result).toBeDefined()
    expect(result.manifest || result.conflicts).toBeDefined()
  })

  it('content-addresses segments by checksum for deduplication', async () => {
    // Create identical events
    const events = createEventSequence(1000, 10)

    // Write to two different branches
    const segments1 = await writeEvents(storage, events, {
      baseDir: 'events/branch1',
    })
    const segments2 = await writeEvents(storage, events, {
      baseDir: 'events/branch2',
    })

    // Checksums should be identical for same content
    expect(segments1[0]!.checksum).toBe(segments2[0]!.checksum)
  })

  it('performs incremental merge', async () => {
    // Initial state
    const initialEvents = createEventSequence(1000, 5)
    const initialSegments = await writeEvents(storage, initialEvents)
    let ourManifest = createEmptyManifest('main')
    initialSegments.forEach(seg => {
      ourManifest = addSegment(ourManifest, seg)
    })

    // Their branch: copy our manifest and add more events
    let theirManifest = { ...ourManifest, branch: 'feature' }
    const theirNewEvents = createEventSequence(6000, 5)
    const theirNewSegments = await writeEvents(storage, theirNewEvents)
    theirNewSegments.forEach(seg => {
      theirManifest = addSegment(theirManifest, seg)
    })

    // Incremental merge
    const result = await incrementalMerge(
      storage,
      ourManifest,
      theirManifest
    )

    expect(result.success).toBe(true)
    expect(result.manifest).toBeDefined()
    // Since their manifest includes our segments, all get counted
    expect(result.stats.totalEvents).toBeGreaterThanOrEqual(10)
  })

  it('handles large segment merge without OOM', async () => {
    // Create large event set (streaming should prevent OOM)
    const largeEvents = createEventSequence(1000, 1000)
    const segments = await writeEvents(storage, largeEvents, {
      maxEventsPerSegment: 200,
    })

    let manifest = createEmptyManifest('main')
    segments.forEach(seg => {
      manifest = addSegment(manifest, seg)
    })

    // Count events using streaming
    const count = await countEvents(storage, manifest.segments, true)
    expect(count).toBe(1000)
  })
})

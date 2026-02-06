/**
 * Event Log Retention & Partitioning Test Suite
 *
 * Tests for time-based partitioning of the event log so that compaction
 * only rewrites the current/active partition instead of the entire history.
 *
 * Partition key format: events/YYYY-MM.compacted
 * Active (current) partition: events.compacted (unchanged, backward-compatible)
 *
 * Beads: zou5.12
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, readdir, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  compactEventsPartitioned,
  readAllPartitions,
  applyRetention,
  getPartitionKey,
} from '@/engine/compactor-events'
import type { EventStorageAdapter, PartitionedCompactOptions } from '@/engine/compactor-events'
import type { EventLine, SchemaLine } from '@/engine/types'

// =============================================================================
// Types
// =============================================================================

type AnyEvent = EventLine | SchemaLine

// =============================================================================
// JSON Storage Adapter (test-only)
// =============================================================================

async function readEvents(path: string): Promise<AnyEvent[]> {
  try {
    return JSON.parse(await readFile(path, 'utf-8'))
  } catch {
    return []
  }
}

async function writeEvents(path: string, data: AnyEvent[]): Promise<void> {
  await writeFile(path, JSON.stringify(data))
}

const jsonAdapter: EventStorageAdapter = {
  readEvents,
  writeEvents,
}

// =============================================================================
// Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'event-partitioning-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to write JSONL lines to a file */
async function writeJsonl(filePath: string, lines: unknown[]): Promise<void> {
  const content = lines.map(line => JSON.stringify(line)).join('\n')
  await writeFile(filePath, content, 'utf-8')
}

/** Helper to create an EventLine */
function makeEvent(overrides: Partial<EventLine> & { id: string; ts: number }): EventLine {
  return {
    op: 'c',
    ns: 'users',
    eid: 'entity-1',
    ...overrides,
  }
}

/** Helper to create a SchemaLine */
function makeSchema(overrides: Partial<SchemaLine> & { id: string; ts: number }): SchemaLine {
  return {
    op: 's',
    ns: 'users',
    schema: { name: 'string', email: 'string' },
    ...overrides,
  }
}

// =============================================================================
// Tests: getPartitionKey
// =============================================================================

describe('getPartitionKey', () => {
  it('returns YYYY-MM format for a given timestamp', () => {
    // 2025-01-15T00:00:00.000Z
    const ts = new Date('2025-01-15T00:00:00.000Z').getTime()
    expect(getPartitionKey(ts)).toBe('2025-01')
  })

  it('returns correct month for end-of-month timestamps', () => {
    // 2025-12-31T23:59:59.999Z
    const ts = new Date('2025-12-31T23:59:59.999Z').getTime()
    expect(getPartitionKey(ts)).toBe('2025-12')
  })

  it('handles epoch zero', () => {
    expect(getPartitionKey(0)).toBe('1970-01')
  })
})

// =============================================================================
// Tests: compactEventsPartitioned
// =============================================================================

describe('compactEventsPartitioned', () => {
  // ===========================================================================
  // Basic partitioning
  // ===========================================================================

  describe('basic partitioning', () => {
    it('partitions events by month into separate files', async () => {
      const events: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: new Date('2025-01-15').getTime(), eid: 'u1' }),
        makeEvent({ id: 'e2', ts: new Date('2025-01-20').getTime(), eid: 'u2' }),
        makeEvent({ id: 'e3', ts: new Date('2025-02-10').getTime(), eid: 'u3' }),
        makeEvent({ id: 'e4', ts: new Date('2025-03-05').getTime(), eid: 'u4' }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      const result = await compactEventsPartitioned(tempDir, jsonAdapter)

      expect(result).not.toBeNull()
      expect(result!.totalEvents).toBe(4)
      expect(result!.partitions).toContain('2025-01')
      expect(result!.partitions).toContain('2025-02')
      expect(result!.partitions).toContain('2025-03')

      // Check that partition files were created in events/ subdirectory
      const janEvents = await readEvents(join(tempDir, 'events', '2025-01.compacted'))
      expect(janEvents).toHaveLength(2)

      const febEvents = await readEvents(join(tempDir, 'events', '2025-02.compacted'))
      expect(febEvents).toHaveLength(1)

      const marEvents = await readEvents(join(tempDir, 'events', '2025-03.compacted'))
      expect(marEvents).toHaveLength(1)
    })

    it('only rewrites partitions that received new events', async () => {
      // Pre-populate January partition
      await mkdir(join(tempDir, 'events'), { recursive: true })
      const janExisting: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: new Date('2025-01-01').getTime(), eid: 'u1' }),
      ]
      await writeEvents(join(tempDir, 'events', '2025-01.compacted'), janExisting)

      // New events only in February
      const newEvents: AnyEvent[] = [
        makeEvent({ id: 'e2', ts: new Date('2025-02-15').getTime(), eid: 'u2' }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      const result = await compactEventsPartitioned(tempDir, jsonAdapter)

      expect(result).not.toBeNull()
      // Only the February partition should have been touched
      expect(result!.partitions).toEqual(['2025-02'])

      // January partition should be unchanged (1 event)
      const janEvents = await readEvents(join(tempDir, 'events', '2025-01.compacted'))
      expect(janEvents).toHaveLength(1)

      // February partition created with 1 event
      const febEvents = await readEvents(join(tempDir, 'events', '2025-02.compacted'))
      expect(febEvents).toHaveLength(1)
    })

    it('appends to an existing partition when new events fall in the same month', async () => {
      // Pre-populate February partition with 2 events
      await mkdir(join(tempDir, 'events'), { recursive: true })
      const febExisting: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: new Date('2025-02-01').getTime(), eid: 'u1' }),
        makeEvent({ id: 'e2', ts: new Date('2025-02-05').getTime(), eid: 'u2' }),
      ]
      await writeEvents(join(tempDir, 'events', '2025-02.compacted'), febExisting)

      // New events also in February
      const newEvents: AnyEvent[] = [
        makeEvent({ id: 'e3', ts: new Date('2025-02-20').getTime(), eid: 'u3' }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      const result = await compactEventsPartitioned(tempDir, jsonAdapter)

      expect(result).not.toBeNull()
      expect(result!.totalEvents).toBe(3)

      // February partition should now have 3 events, sorted by ts
      const febEvents = await readEvents(join(tempDir, 'events', '2025-02.compacted'))
      expect(febEvents).toHaveLength(3)
      const timestamps = febEvents.map(e => e.ts)
      expect(timestamps[0]).toBeLessThanOrEqual(timestamps[1])
      expect(timestamps[1]).toBeLessThanOrEqual(timestamps[2])
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('returns null when no events.jsonl exists', async () => {
      const result = await compactEventsPartitioned(tempDir, jsonAdapter)
      expect(result).toBeNull()
    })

    it('returns null when events.jsonl is empty', async () => {
      await writeFile(join(tempDir, 'events.jsonl'), '', 'utf-8')
      // rotate will see the file but it will be empty after rotation
      const result = await compactEventsPartitioned(tempDir, jsonAdapter)
      expect(result).toBeNull()
    })

    it('preserves all event fields through partitioning', async () => {
      const events: AnyEvent[] = [
        makeEvent({
          id: 'e1',
          ts: new Date('2025-03-10').getTime(),
          op: 'u',
          ns: 'users',
          eid: 'user-42',
          before: { name: 'Old Name' },
          after: { name: 'New Name' },
          actor: 'admin-1',
        }),
        makeSchema({
          id: 's1',
          ts: new Date('2025-03-10').getTime(),
          ns: 'posts',
          schema: { title: 'string' },
          migration: { added: ['tags'] },
        }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEventsPartitioned(tempDir, jsonAdapter)

      const partEvents = await readEvents(join(tempDir, 'events', '2025-03.compacted'))
      expect(partEvents).toHaveLength(2)

      const eventLine = partEvents[0] as EventLine
      expect(eventLine.before).toEqual({ name: 'Old Name' })
      expect(eventLine.after).toEqual({ name: 'New Name' })
      expect(eventLine.actor).toBe('admin-1')

      const schemaLine = partEvents[1] as SchemaLine
      expect(schemaLine.schema).toEqual({ title: 'string' })
      expect(schemaLine.migration).toEqual({ added: ['tags'] })
    })

    it('cleans up .compacting file after partitioned compaction', async () => {
      const events: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: new Date('2025-01-10').getTime(), eid: 'u1' }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEventsPartitioned(tempDir, jsonAdapter)

      const files = await readdir(tempDir)
      expect(files).not.toContain('events.jsonl.compacting')
    })
  })

  // ===========================================================================
  // Schema events
  // ===========================================================================

  describe('schema events', () => {
    it('schema events are partitioned by their timestamp like data events', async () => {
      const events: AnyEvent[] = [
        makeSchema({ id: 's1', ts: new Date('2025-01-01').getTime(), ns: 'users', schema: { name: 'string' } }),
        makeEvent({ id: 'e1', ts: new Date('2025-02-01').getTime(), eid: 'u1' }),
        makeSchema({ id: 's2', ts: new Date('2025-02-01').getTime(), ns: 'users', schema: { name: 'string', email: 'string' } }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEventsPartitioned(tempDir, jsonAdapter)

      const janEvents = await readEvents(join(tempDir, 'events', '2025-01.compacted'))
      expect(janEvents).toHaveLength(1)
      expect(janEvents[0].op).toBe('s')

      const febEvents = await readEvents(join(tempDir, 'events', '2025-02.compacted'))
      expect(febEvents).toHaveLength(2)
    })
  })
})

// =============================================================================
// Tests: readAllPartitions
// =============================================================================

describe('readAllPartitions', () => {
  it('reads events from all partition files and returns them sorted by ts', async () => {
    await mkdir(join(tempDir, 'events'), { recursive: true })

    await writeEvents(join(tempDir, 'events', '2025-01.compacted'), [
      makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
      makeEvent({ id: 'e2', ts: 200, eid: 'u2' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-02.compacted'), [
      makeEvent({ id: 'e3', ts: 300, eid: 'u3' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-03.compacted'), [
      makeEvent({ id: 'e4', ts: 400, eid: 'u4' }),
    ])

    const all = await readAllPartitions(tempDir, jsonAdapter)

    expect(all).toHaveLength(4)
    const timestamps = all.map(e => e.ts)
    expect(timestamps).toEqual([100, 200, 300, 400])
  })

  it('returns empty array when no partitions exist', async () => {
    const all = await readAllPartitions(tempDir, jsonAdapter)
    expect(all).toHaveLength(0)
  })

  it('also reads legacy events.compacted if it exists', async () => {
    // Legacy un-partitioned file
    await writeEvents(join(tempDir, 'events.compacted'), [
      makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
    ])

    // Plus a partition file
    await mkdir(join(tempDir, 'events'), { recursive: true })
    await writeEvents(join(tempDir, 'events', '2025-02.compacted'), [
      makeEvent({ id: 'e2', ts: 200, eid: 'u2' }),
    ])

    const all = await readAllPartitions(tempDir, jsonAdapter)

    expect(all).toHaveLength(2)
    expect(all[0].id).toBe('e1')
    expect(all[1].id).toBe('e2')
  })
})

// =============================================================================
// Tests: applyRetention
// =============================================================================

describe('applyRetention', () => {
  it('removes partitions older than maxPartitions', async () => {
    await mkdir(join(tempDir, 'events'), { recursive: true })

    // Create 5 monthly partitions
    await writeEvents(join(tempDir, 'events', '2025-01.compacted'), [
      makeEvent({ id: 'e1', ts: new Date('2025-01-01').getTime(), eid: 'u1' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-02.compacted'), [
      makeEvent({ id: 'e2', ts: new Date('2025-02-01').getTime(), eid: 'u2' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-03.compacted'), [
      makeEvent({ id: 'e3', ts: new Date('2025-03-01').getTime(), eid: 'u3' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-04.compacted'), [
      makeEvent({ id: 'e4', ts: new Date('2025-04-01').getTime(), eid: 'u4' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-05.compacted'), [
      makeEvent({ id: 'e5', ts: new Date('2025-05-01').getTime(), eid: 'u5' }),
    ])

    // Keep only the 3 most recent partitions
    const removed = await applyRetention(tempDir, { maxPartitions: 3 })

    expect(removed).toHaveLength(2)
    expect(removed).toContain('2025-01')
    expect(removed).toContain('2025-02')

    // Verify the files were actually deleted
    const files = await readdir(join(tempDir, 'events'))
    expect(files).not.toContain('2025-01.compacted')
    expect(files).not.toContain('2025-02.compacted')
    expect(files).toContain('2025-03.compacted')
    expect(files).toContain('2025-04.compacted')
    expect(files).toContain('2025-05.compacted')
  })

  it('does nothing when fewer partitions exist than maxPartitions', async () => {
    await mkdir(join(tempDir, 'events'), { recursive: true })

    await writeEvents(join(tempDir, 'events', '2025-01.compacted'), [
      makeEvent({ id: 'e1', ts: new Date('2025-01-01').getTime(), eid: 'u1' }),
    ])
    await writeEvents(join(tempDir, 'events', '2025-02.compacted'), [
      makeEvent({ id: 'e2', ts: new Date('2025-02-01').getTime(), eid: 'u2' }),
    ])

    const removed = await applyRetention(tempDir, { maxPartitions: 5 })

    expect(removed).toHaveLength(0)

    const files = await readdir(join(tempDir, 'events'))
    expect(files).toContain('2025-01.compacted')
    expect(files).toContain('2025-02.compacted')
  })

  it('does nothing when events directory does not exist', async () => {
    const removed = await applyRetention(tempDir, { maxPartitions: 3 })
    expect(removed).toHaveLength(0)
  })

  it('removes partitions older than maxAge', async () => {
    await mkdir(join(tempDir, 'events'), { recursive: true })

    const now = Date.now()
    const threeMonthsAgo = now - 90 * 24 * 60 * 60 * 1000

    // Create a partition from 6 months ago
    const oldDate = new Date(now - 180 * 24 * 60 * 60 * 1000)
    const oldKey = `${oldDate.getUTCFullYear()}-${String(oldDate.getUTCMonth() + 1).padStart(2, '0')}`
    await writeEvents(join(tempDir, 'events', `${oldKey}.compacted`), [
      makeEvent({ id: 'e1', ts: oldDate.getTime(), eid: 'u1' }),
    ])

    // Create a recent partition
    const recentDate = new Date(now - 15 * 24 * 60 * 60 * 1000)
    const recentKey = `${recentDate.getUTCFullYear()}-${String(recentDate.getUTCMonth() + 1).padStart(2, '0')}`
    await writeEvents(join(tempDir, 'events', `${recentKey}.compacted`), [
      makeEvent({ id: 'e2', ts: recentDate.getTime(), eid: 'u2' }),
    ])

    // maxAge: 90 days in ms
    const removed = await applyRetention(tempDir, { maxAge: 90 * 24 * 60 * 60 * 1000 })

    expect(removed).toContain(oldKey)
    expect(removed).not.toContain(recentKey)
  })
})

// =============================================================================
// Tests: Backward compatibility
// =============================================================================

describe('backward compatibility', () => {
  it('compactEventsPartitioned migrates legacy events.compacted into partitions', async () => {
    // Legacy un-partitioned compacted file with events from different months
    const legacyEvents: AnyEvent[] = [
      makeEvent({ id: 'e1', ts: new Date('2025-01-10').getTime(), eid: 'u1' }),
      makeEvent({ id: 'e2', ts: new Date('2025-02-10').getTime(), eid: 'u2' }),
    ]
    await writeEvents(join(tempDir, 'events.compacted'), legacyEvents)

    // New events in March
    const newEvents: AnyEvent[] = [
      makeEvent({ id: 'e3', ts: new Date('2025-03-10').getTime(), eid: 'u3' }),
    ]
    await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

    const result = await compactEventsPartitioned(tempDir, jsonAdapter)

    expect(result).not.toBeNull()
    expect(result!.totalEvents).toBe(3)

    // All events should be in their respective partitions
    const janEvents = await readEvents(join(tempDir, 'events', '2025-01.compacted'))
    expect(janEvents).toHaveLength(1)
    expect(janEvents[0].id).toBe('e1')

    const febEvents = await readEvents(join(tempDir, 'events', '2025-02.compacted'))
    expect(febEvents).toHaveLength(1)
    expect(febEvents[0].id).toBe('e2')

    const marEvents = await readEvents(join(tempDir, 'events', '2025-03.compacted'))
    expect(marEvents).toHaveLength(1)
    expect(marEvents[0].id).toBe('e3')
  })
})

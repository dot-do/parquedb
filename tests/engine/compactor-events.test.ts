/**
 * Event Compactor Test Suite
 *
 * Tests for append-only event log compaction in the MergeTree engine.
 * Events are never deduplicated -- compaction simply converts JSONL
 * to the compacted format, appending new events to existing ones.
 *
 * The storage adapter is injected so tests can use JSON files instead
 * of Parquet, while production uses Parquet.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, access, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { compactEvents, shouldCompact } from '@/engine/compactor-events'
import type { EventStorageAdapter } from '@/engine/compactor-events'
import type { EventLine, SchemaLine } from '@/engine/types'

// =============================================================================
// Types
// =============================================================================

type AnyEvent = EventLine | SchemaLine

// =============================================================================
// JSON Storage Adapter (test-only)
// =============================================================================

/**
 * Read events from a JSON file. Returns [] if file doesn't exist.
 */
async function readEvents(path: string): Promise<AnyEvent[]> {
  try {
    return JSON.parse(await readFile(path, 'utf-8'))
  } catch {
    return []
  }
}

/**
 * Write events to a JSON file.
 */
async function writeEvents(path: string, data: AnyEvent[]): Promise<void> {
  await writeFile(path, JSON.stringify(data))
}

/**
 * JSON-based storage adapter for tests.
 */
const jsonAdapter: EventStorageAdapter = {
  readEvents,
  writeEvents,
}

// =============================================================================
// Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'compactor-events-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to check if a file exists */
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}

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
// Tests
// =============================================================================

describe('compactEvents', () => {
  // ===========================================================================
  // First compaction (no existing file)
  // ===========================================================================
  describe('first compaction', () => {
    it('1. 5 events in JSONL, no existing file -> output has 5 events', async () => {
      const events: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1', after: { name: 'Alice' } }),
        makeEvent({ id: 'e2', ts: 200, eid: 'u2', after: { name: 'Bob' } }),
        makeEvent({ id: 'e3', ts: 300, eid: 'u3', after: { name: 'Charlie' } }),
        makeEvent({ id: 'e4', ts: 400, eid: 'u4', after: { name: 'Diana' } }),
        makeEvent({ id: 'e5', ts: 500, eid: 'u5', after: { name: 'Eve' } }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      const count = await compactEvents(tempDir, jsonAdapter)

      expect(count).toBe(5)
      const output = await readEvents(join(tempDir, 'events.compacted'))
      expect(output).toHaveLength(5)
    })

    it('2. output is sorted by ts', async () => {
      const events: AnyEvent[] = [
        makeEvent({ id: 'e3', ts: 300, eid: 'u3' }),
        makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
        makeEvent({ id: 'e5', ts: 500, eid: 'u5' }),
        makeEvent({ id: 'e2', ts: 200, eid: 'u2' }),
        makeEvent({ id: 'e4', ts: 400, eid: 'u4' }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEvents(tempDir, jsonAdapter)

      const output = await readEvents(join(tempDir, 'events.compacted'))
      const timestamps = output.map(e => e.ts)
      expect(timestamps).toEqual([100, 200, 300, 400, 500])
    })

    it('3. cleanup: .compacting file removed after compaction', async () => {
      const events: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEvents(tempDir, jsonAdapter)

      const compactingExists = await fileExists(join(tempDir, 'events.jsonl.compacting'))
      expect(compactingExists).toBe(false)
    })
  })

  // ===========================================================================
  // Append to existing
  // ===========================================================================
  describe('append to existing', () => {
    it('4. 3 existing events + 2 new events -> 5 events total (all preserved, no dedup)', async () => {
      // Write existing compacted events
      const existing: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1', after: { name: 'Alice' } }),
        makeEvent({ id: 'e2', ts: 200, eid: 'u2', after: { name: 'Bob' } }),
        makeEvent({ id: 'e3', ts: 300, eid: 'u3', after: { name: 'Charlie' } }),
      ]
      await writeEvents(join(tempDir, 'events.compacted'), existing)

      // Write new JSONL events
      const newEvents: AnyEvent[] = [
        makeEvent({ id: 'e4', ts: 400, eid: 'u4', after: { name: 'Diana' } }),
        makeEvent({ id: 'e5', ts: 500, eid: 'u5', after: { name: 'Eve' } }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      const count = await compactEvents(tempDir, jsonAdapter)

      expect(count).toBe(5)
      const output = await readEvents(join(tempDir, 'events.compacted'))
      expect(output).toHaveLength(5)
    })

    it('5. events from both sources are interleaved by ts in output', async () => {
      // Existing events at ts 100, 300, 500
      const existing: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
        makeEvent({ id: 'e3', ts: 300, eid: 'u3' }),
        makeEvent({ id: 'e5', ts: 500, eid: 'u5' }),
      ]
      await writeEvents(join(tempDir, 'events.compacted'), existing)

      // New events at ts 200, 400
      const newEvents: AnyEvent[] = [
        makeEvent({ id: 'e2', ts: 200, eid: 'u2' }),
        makeEvent({ id: 'e4', ts: 400, eid: 'u4' }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      await compactEvents(tempDir, jsonAdapter)

      const output = await readEvents(join(tempDir, 'events.compacted'))
      const timestamps = output.map(e => e.ts)
      expect(timestamps).toEqual([100, 200, 300, 400, 500])

      const ids = output.map(e => e.id)
      expect(ids).toEqual(['e1', 'e2', 'e3', 'e4', 'e5'])
    })
  })

  // ===========================================================================
  // Schema events
  // ===========================================================================
  describe('schema events', () => {
    it('6. mix of EventLine and SchemaLine -> all preserved in output', async () => {
      const events: AnyEvent[] = [
        makeSchema({ id: 's1', ts: 100, ns: 'users', schema: { name: 'string' } }),
        makeEvent({ id: 'e1', ts: 200, eid: 'u1', after: { name: 'Alice' } }),
        makeSchema({ id: 's2', ts: 300, ns: 'users', schema: { name: 'string', email: 'string' }, migration: { added: ['email'] } }),
        makeEvent({ id: 'e2', ts: 400, eid: 'u2', after: { name: 'Bob', email: 'bob@b.co' } }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      const count = await compactEvents(tempDir, jsonAdapter)

      expect(count).toBe(4)
      const output = await readEvents(join(tempDir, 'events.compacted'))
      expect(output).toHaveLength(4)

      // Verify schema events preserved
      const schemaEvents = output.filter(e => e.op === 's')
      expect(schemaEvents).toHaveLength(2)

      // Verify event lines preserved
      const dataEvents = output.filter(e => e.op !== 's')
      expect(dataEvents).toHaveLength(2)
    })

    it('7. schema events (op=s) are NOT deduplicated (each is a unique snapshot)', async () => {
      // Two schema events for the same namespace at different times
      const existing: AnyEvent[] = [
        makeSchema({ id: 's1', ts: 100, ns: 'users', schema: { name: 'string' } }),
      ]
      await writeEvents(join(tempDir, 'events.compacted'), existing)

      const newEvents: AnyEvent[] = [
        makeSchema({ id: 's2', ts: 200, ns: 'users', schema: { name: 'string', email: 'string' }, migration: { added: ['email'] } }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      await compactEvents(tempDir, jsonAdapter)

      const output = await readEvents(join(tempDir, 'events.compacted'))
      // Both schema events are preserved
      expect(output).toHaveLength(2)
      const schemaEvents = output.filter(e => e.op === 's') as SchemaLine[]
      expect(schemaEvents).toHaveLength(2)
      expect(schemaEvents[0].id).toBe('s1')
      expect(schemaEvents[1].id).toBe('s2')
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================
  describe('edge cases', () => {
    it('8. empty JSONL -> returns null (nothing to compact)', async () => {
      // No events.jsonl file at all
      const count = await compactEvents(tempDir, jsonAdapter)

      expect(count).toBeNull()
    })

    it('9. duplicate event IDs -> both preserved (events are immutable, no dedup)', async () => {
      const existing: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1', after: { name: 'Alice' } }),
      ]
      await writeEvents(join(tempDir, 'events.compacted'), existing)

      // New event with same ID but different content
      const newEvents: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 200, eid: 'u1', after: { name: 'Alice Updated' } }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      await compactEvents(tempDir, jsonAdapter)

      const output = await readEvents(join(tempDir, 'events.compacted'))
      // Both events with id 'e1' are preserved
      expect(output).toHaveLength(2)
      expect(output[0].id).toBe('e1')
      expect(output[1].id).toBe('e1')
      expect(output[0].ts).toBe(100)
      expect(output[1].ts).toBe(200)
    })

    it('10. large: 1000 existing + 500 new -> all 1500 in output', async () => {
      const existing: AnyEvent[] = []
      for (let i = 0; i < 1000; i++) {
        existing.push(makeEvent({ id: `e-${String(i).padStart(5, '0')}`, ts: i * 10, eid: `u${i}` }))
      }
      await writeEvents(join(tempDir, 'events.compacted'), existing)

      const newEvents: AnyEvent[] = []
      for (let i = 0; i < 500; i++) {
        newEvents.push(makeEvent({ id: `n-${String(i).padStart(5, '0')}`, ts: 10000 + i * 10, eid: `u${1000 + i}` }))
      }
      await writeJsonl(join(tempDir, 'events.jsonl'), newEvents)

      const count = await compactEvents(tempDir, jsonAdapter)

      expect(count).toBe(1500)
      const output = await readEvents(join(tempDir, 'events.compacted'))
      expect(output).toHaveLength(1500)

      // Verify sorted by ts
      for (let i = 1; i < output.length; i++) {
        expect(output[i].ts).toBeGreaterThanOrEqual(output[i - 1].ts)
      }
    })

    it('11. events preserve all fields (before, after, actor, etc.)', async () => {
      const events: AnyEvent[] = [
        makeEvent({
          id: 'e1',
          ts: 100,
          op: 'u',
          ns: 'users',
          eid: 'user-42',
          before: { name: 'Old Name', email: 'old@example.com' },
          after: { name: 'New Name', email: 'new@example.com' },
          actor: 'admin-1',
        }),
        makeSchema({
          id: 's1',
          ts: 200,
          ns: 'posts',
          schema: { title: 'string', body: 'string', tags: 'string[]' },
          migration: { added: ['tags'], default: { tags: [] } },
        }),
      ]

      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      await compactEvents(tempDir, jsonAdapter)

      const output = await readEvents(join(tempDir, 'events.compacted'))
      expect(output).toHaveLength(2)

      // Verify EventLine fields
      const eventLine = output[0] as EventLine
      expect(eventLine.id).toBe('e1')
      expect(eventLine.ts).toBe(100)
      expect(eventLine.op).toBe('u')
      expect(eventLine.ns).toBe('users')
      expect(eventLine.eid).toBe('user-42')
      expect(eventLine.before).toEqual({ name: 'Old Name', email: 'old@example.com' })
      expect(eventLine.after).toEqual({ name: 'New Name', email: 'new@example.com' })
      expect(eventLine.actor).toBe('admin-1')

      // Verify SchemaLine fields
      const schemaLine = output[1] as SchemaLine
      expect(schemaLine.id).toBe('s1')
      expect(schemaLine.ts).toBe(200)
      expect(schemaLine.op).toBe('s')
      expect(schemaLine.ns).toBe('posts')
      expect(schemaLine.schema).toEqual({ title: 'string', body: 'string', tags: 'string[]' })
      expect(schemaLine.migration).toEqual({ added: ['tags'], default: { tags: [] } })
    })
  })

  // ===========================================================================
  // shouldCompact
  // ===========================================================================
  describe('shouldCompact', () => {
    it('12. returns true when JSONL exceeds threshold', async () => {
      // Create a JSONL file with enough lines to exceed the default threshold
      const events: AnyEvent[] = []
      for (let i = 0; i < 200; i++) {
        events.push(makeEvent({
          id: `e${i}`,
          ts: i * 10,
          eid: `u${i}`,
          after: { name: `User ${i}`, data: 'x'.repeat(100) },
        }))
      }
      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      const result = await shouldCompact(tempDir)

      expect(result).toBe(true)
    })

    it('12b. returns false when JSONL is below threshold', async () => {
      // Create a small JSONL file
      const events: AnyEvent[] = [
        makeEvent({ id: 'e1', ts: 100, eid: 'u1' }),
      ]
      await writeJsonl(join(tempDir, 'events.jsonl'), events)

      const result = await shouldCompact(tempDir, { lineThreshold: 100 })

      expect(result).toBe(false)
    })

    it('12c. returns false when JSONL does not exist', async () => {
      const result = await shouldCompact(tempDir)

      expect(result).toBe(false)
    })
  })
})

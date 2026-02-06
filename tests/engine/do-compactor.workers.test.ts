/**
 * DOCompactor Workers Tests (Wave 7.2)
 *
 * Tests DO compaction from SQLite WAL -> R2 Parquet using REAL Cloudflare
 * DO SQLite + R2 bindings via vitest-pool-workers. No mocking.
 *
 * Run with: pnpm vitest run --project 'engine:workers' tests/engine/do-compactor.workers.test.ts
 */

import { env } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'
import { parseDataField } from '../../src/engine/parquet-data-utils'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface TestEnv {
  MERGETREE: DurableObjectNamespace
  BUCKET: R2Bucket
}

interface MergeTreeStub {
  // WAL methods
  append(table: string, line: Record<string, unknown>): Promise<void>
  appendBatch(table: string, lines: Record<string, unknown>[]): Promise<void>
  getBatches(table: string): Promise<Array<{ id: number; ts: number; batch: string; row_count: number }>>
  getUnflushedCount(): Promise<number>
  getUnflushedCountForTable(table: string): Promise<number>
  getUnflushedTables(): Promise<string[]>
  replayUnflushed(table: string): Promise<Record<string, unknown>[]>
  markFlushed(batchIds: number[]): Promise<void>
  cleanup(): Promise<void>

  // DOCompactor methods
  compact(table: string): Promise<{ count: number; flushed: number } | null>
  compactRels(): Promise<{ count: number; flushed: number } | null>
  compactEvents(): Promise<{ count: number; flushed: number } | null>
  compactAll(): Promise<{ tables: Record<string, number>; rels: number | null; events: number | null }>
  shouldCompact(threshold?: number): Promise<boolean>
}

const testEnv = env as unknown as TestEnv

function getStub(name?: string): MergeTreeStub {
  const id = testEnv.MERGETREE.idFromName(name ?? `compact-${Date.now()}-${Math.random()}`)
  return testEnv.MERGETREE.get(id) as unknown as MergeTreeStub
}

/**
 * Helper to read Parquet from R2 and decode rows.
 */
async function readR2Parquet(key: string): Promise<Record<string, unknown>[]> {
  const obj = await testEnv.BUCKET.get(key)
  if (!obj) return []

  const buffer = await obj.arrayBuffer()
  if (buffer.byteLength === 0) return []

  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) =>
      buffer.slice(start, end ?? buffer.byteLength),
  }
  return (await parquetReadObjects({ file: asyncBuffer })) as Record<string, unknown>[]
}

/**
 * Helper to clean up R2 keys after tests.
 */
async function cleanR2(...keys: string[]): Promise<void> {
  for (const key of keys) {
    try {
      await testEnv.BUCKET.delete(key)
    } catch {
      // ignore
    }
  }
}

/** Coerce BigInt/number to number */
function toNumber(value: unknown): number {
  if (typeof value === 'number') return value
  if (typeof value === 'bigint') return Number(value)
  return 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DOCompactor (real DO SQLite + R2)', () => {

  // =========================================================================
  // compactTable — data table compaction
  // =========================================================================

  describe('compactTable', () => {

    // 1. No unflushed data returns null
    it('returns null when no unflushed data exists', async () => {
      const stub = getStub()
      const result = await stub.compact('users')
      expect(result).toBeNull()
    })

    // 2. Flushes WAL data to R2 Parquet
    it('flushes WAL data to R2 Parquet', async () => {
      const table = `users_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Key = `data/${table}.parquet`
      const stub = getStub()

      try {
        // Write data to WAL
        await stub.append(table, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.append(table, { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

        // Compact
        const result = await stub.compact(table)

        expect(result).not.toBeNull()
        expect(result!.count).toBe(2)
        expect(result!.flushed).toBe(2)

        // Verify R2 has the Parquet file
        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(2)

        // Rows are sorted by $id
        expect(rows[0].$id).toBe('u1')
        expect(rows[1].$id).toBe('u2')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // 3. Merges with existing R2 Parquet data
    it('merges with existing R2 Parquet data', async () => {
      const table = `users_merge_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Key = `data/${table}.parquet`
      const stub = getStub()

      try {
        // First compaction: write initial data
        await stub.append(table, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.append(table, { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

        const result1 = await stub.compact(table)
        expect(result1!.count).toBe(2)

        // Second compaction: add new data that should merge with existing
        await stub.append(table, { $id: 'u3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie' })

        const result2 = await stub.compact(table)
        expect(result2!.count).toBe(3) // All three entities
        expect(result2!.flushed).toBe(1)

        // Verify R2 has all 3 entities
        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(3)
        expect(rows[0].$id).toBe('u1')
        expect(rows[1].$id).toBe('u2')
        expect(rows[2].$id).toBe('u3')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // 4. Handles updates (higher $v overwrites)
    it('handles updates where higher $v overwrites existing', async () => {
      const table = `users_update_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Key = `data/${table}.parquet`
      const stub = getStub()

      try {
        // First compaction: create entity
        await stub.append(table, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.compact(table)

        // Second compaction: update entity with higher version
        await stub.append(table, { $id: 'u1', $op: 'u', $v: 2, $ts: 1050, name: 'Alice Smith' })
        const result = await stub.compact(table)

        expect(result!.count).toBe(1)

        // Verify the updated data
        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(1)
        expect(rows[0].$id).toBe('u1')
        expect(rows[0].$op).toBe('u')
        expect(toNumber(rows[0].$v)).toBe(2)

        // Verify $data contains the updated name (VARIANT, decoded via parseDataField)
        const data = parseDataField(rows[0].$data)
        expect(data.name).toBe('Alice Smith')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // 5. Handles deletes (removes from output)
    it('handles deletes by removing entity from output', async () => {
      const table = `users_delete_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Key = `data/${table}.parquet`
      const stub = getStub()

      try {
        // First compaction: create two entities
        await stub.append(table, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.append(table, { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
        await stub.compact(table)

        // Second compaction: delete u1
        await stub.append(table, { $id: 'u1', $op: 'd', $v: 2, $ts: 1050 })
        const result = await stub.compact(table)

        expect(result!.count).toBe(1) // Only Bob remains

        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(1)
        expect(rows[0].$id).toBe('u2')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // 6. Marks batches as flushed after write
    it('marks batches as flushed after write', async () => {
      const table = `users_flush_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Key = `data/${table}.parquet`
      const stub = getStub()

      try {
        await stub.append(table, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.append(table, { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

        // Before compact: 2 unflushed
        expect(await stub.getUnflushedCountForTable(table)).toBe(2)

        await stub.compact(table)

        // After compact: 0 unflushed
        expect(await stub.getUnflushedCountForTable(table)).toBe(0)

        // Batches should be marked flushed (getBatches returns only unflushed)
        const batches = await stub.getBatches(table)
        expect(batches).toHaveLength(0)
      } finally {
        await cleanR2(r2Key)
      }
    })
  })

  // =========================================================================
  // compactRels — relationship compaction
  // =========================================================================

  describe('compactRels', () => {

    // 7. Flushes WAL rels to R2 Parquet
    it('flushes WAL rels to R2 Parquet', async () => {
      const r2Key = 'rels/rels.parquet'
      const stub = getStub()

      try {
        await stub.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' })
        await stub.append('rels', { $op: 'l', $ts: 1001, f: 'u1', p: 'author', r: 'posts', t: 'p2' })

        const result = await stub.compactRels()

        expect(result).not.toBeNull()
        expect(result!.count).toBe(2)
        expect(result!.flushed).toBe(2)

        // Verify R2 has the Parquet file
        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(2)
        expect(rows[0].f).toBe('u1')
        expect(rows[0].p).toBe('author')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // 8. Merges links and filters unlinks
    it('merges links and filters unlinks', async () => {
      const r2Key = 'rels/rels.parquet'
      const stub = getStub()

      try {
        // First compaction: create two links
        await stub.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' })
        await stub.append('rels', { $op: 'l', $ts: 1001, f: 'u1', p: 'author', r: 'posts', t: 'p2' })
        await stub.compactRels()

        // Second compaction: unlink one relationship
        await stub.append('rels', { $op: 'u', $ts: 1050, f: 'u1', p: 'author', r: 'posts', t: 'p1' })
        const result = await stub.compactRels()

        expect(result!.count).toBe(1) // Only p2 link remains

        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(1)
        expect(rows[0].t).toBe('p2')
        expect(rows[0].$op).toBe('l')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // compactRels returns null when no unflushed rels
    it('returns null when no unflushed rels exist', async () => {
      const stub = getStub()
      const result = await stub.compactRels()
      expect(result).toBeNull()
    })
  })

  // =========================================================================
  // compactEvents — CDC events (append-only)
  // =========================================================================

  describe('compactEvents', () => {

    // 9. Appends new events to existing
    it('appends new events to existing R2 Parquet', async () => {
      const r2Key = 'events/events.parquet'
      const stub = getStub()

      try {
        // First compaction
        await stub.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } })
        await stub.append('events', { id: 'e2', ts: 1001, op: 'c', ns: 'users', eid: 'u2', after: { name: 'Bob' } })
        const result1 = await stub.compactEvents()
        expect(result1!.count).toBe(2)

        // Second compaction: new events should be appended
        await stub.append('events', { id: 'e3', ts: 1002, op: 'u', ns: 'users', eid: 'u1', before: { name: 'Alice' }, after: { name: 'Alice Smith' } })
        const result2 = await stub.compactEvents()
        expect(result2!.count).toBe(3) // All three events

        const rows = await readR2Parquet(r2Key)
        expect(rows).toHaveLength(3)

        // Should be sorted by ts
        expect(rows[0].id).toBe('e1')
        expect(rows[1].id).toBe('e2')
        expect(rows[2].id).toBe('e3')
      } finally {
        await cleanR2(r2Key)
      }
    })

    // compactEvents returns null when no unflushed events
    it('returns null when no unflushed events exist', async () => {
      const stub = getStub()
      const result = await stub.compactEvents()
      expect(result).toBeNull()
    })
  })

  // =========================================================================
  // compactAll — compact everything
  // =========================================================================

  describe('compactAll', () => {

    // 10. Compacts all unflushed tables
    it('compacts all unflushed tables, rels, and events', async () => {
      const table1 = `users_all_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const table2 = `posts_all_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
      const r2Keys = [
        `data/${table1}.parquet`,
        `data/${table2}.parquet`,
        'rels/rels.parquet',
        'events/events.parquet',
      ]
      const stub = getStub()

      try {
        // Add data to multiple tables
        await stub.append(table1, { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
        await stub.append(table2, { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })

        // Add rels
        await stub.append('rels', { $op: 'l', $ts: 1002, f: 'u1', p: 'author', r: 'posts', t: 'p1' })

        // Add events
        await stub.append('events', { id: 'e1', ts: 1003, op: 'c', ns: 'users', eid: 'u1' })

        // Compact everything
        const result = await stub.compactAll()

        // Data tables
        expect(result.tables[table1]).toBe(1)
        expect(result.tables[table2]).toBe(1)

        // Rels
        expect(result.rels).toBe(1)

        // Events
        expect(result.events).toBe(1)

        // All WAL data should be flushed
        expect(await stub.getUnflushedCount()).toBe(0)

        // Verify R2 files exist
        const dataRows1 = await readR2Parquet(r2Keys[0])
        expect(dataRows1).toHaveLength(1)

        const dataRows2 = await readR2Parquet(r2Keys[1])
        expect(dataRows2).toHaveLength(1)

        const relRows = await readR2Parquet(r2Keys[2])
        expect(relRows).toHaveLength(1)

        const eventRows = await readR2Parquet(r2Keys[3])
        expect(eventRows).toHaveLength(1)
      } finally {
        await cleanR2(...r2Keys)
      }
    })
  })

  // =========================================================================
  // shouldCompact — threshold check
  // =========================================================================

  describe('shouldCompact', () => {

    // 11. Returns true when threshold exceeded
    it('returns true when unflushed count exceeds threshold', async () => {
      const stub = getStub()

      // Add enough data to exceed threshold
      const lines = []
      for (let i = 0; i < 10; i++) {
        lines.push({ $id: `u${i}`, $op: 'c', $v: 1, $ts: 1000 + i })
      }
      await stub.appendBatch('users', lines)

      // Threshold of 5 should return true (we have 10)
      expect(await stub.shouldCompact(5)).toBe(true)

      // Threshold of 10 should return true (we have exactly 10)
      expect(await stub.shouldCompact(10)).toBe(true)
    })

    // 12. Returns false below threshold
    it('returns false when below threshold', async () => {
      const stub = getStub()

      // Add just 3 rows
      await stub.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000 })
      await stub.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001 })
      await stub.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 1002 })

      // Threshold of 5 should return false (we have 3)
      expect(await stub.shouldCompact(5)).toBe(false)

      // Default threshold (100) should also return false
      expect(await stub.shouldCompact()).toBe(false)
    })

    // shouldCompact returns false with no data
    it('returns false when no data exists', async () => {
      const stub = getStub()
      expect(await stub.shouldCompact(1)).toBe(false)
      expect(await stub.shouldCompact()).toBe(false)
    })
  })
})

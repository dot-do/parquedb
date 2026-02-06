/**
 * DO Compactor for MergeTree Engine (Wave 7.2)
 *
 * Flushes SQLite WAL batches to R2 Parquet files during compaction.
 * Runs inside a Durable Object on alarm to compact buffered mutations
 * into columnar Parquet storage on R2.
 *
 * Compaction flow:
 *   1. Read unflushed batches from SQLite WAL
 *   2. Replay unflushed lines
 *   3. Read existing Parquet from R2 (if any)
 *   4. Merge using ReplacingMergeTree semantics (data) or key-based dedup (rels)
 *   5. Encode merged result to Parquet
 *   6. Write Parquet to R2
 *   7. Mark WAL batches as flushed
 *
 * Key paths in R2:
 *   - data/{table}.parquet   — entity data per table
 *   - rels/rels.parquet      — all relationships
 *   - events/events.parquet  — append-only CDC events
 */

import { SqliteWal } from './sqlite-wal'
import { mergeResults } from './merge'
import {
  encodeDataToParquet,
  encodeRelsToParquet,
  encodeEventsToParquet,
} from './parquet-encoders'
import type { DataLine, RelLine, EventLine } from './types'
import { mergeRelationships } from './merge-rels'
import { mergeEvents } from './merge-events'
import type { AnyEventLine } from './merge-events'
import { toNumber } from './utils'
import { parseDataField } from './parquet-data-utils'

// =============================================================================
// Helpers
// =============================================================================

/**
 * Read a Parquet file from R2 and decode rows using hyparquet.
 * Returns an empty array if the key does not exist.
 */
async function readParquetFromR2<T extends Record<string, unknown>>(bucket: R2Bucket, key: string): Promise<T[]> {
  const obj = await bucket.get(key)
  if (!obj) return []

  const buffer = await obj.arrayBuffer()
  if (buffer.byteLength === 0) return []

  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) =>
      buffer.slice(start, end ?? buffer.byteLength),
  }

  return (await parquetReadObjects({ file: asyncBuffer })) as T[]
}

/**
 * Decode raw Parquet data rows into DataLine objects.
 * Handles the $data column which is either:
 * - A JS object (new JSON converted type, auto-decoded by hyparquet)
 * - A JSON string (legacy UTF8 format, parsed via fallback)
 */
function decodeDataRows(
  rows: Array<{ $id: string; $op: string; $v: unknown; $ts: unknown; $data?: unknown }>,
): DataLine[] {
  return rows.map((row) => {
    const dataFields = parseDataField(row.$data)
    return {
      ...dataFields,
      $id: row.$id,
      $op: row.$op as DataLine['$op'],
      $v: toNumber(row.$v),
      $ts: toNumber(row.$ts),
    }
  })
}

/**
 * Decode raw Parquet rel rows into RelLine objects.
 */
function decodeRelRows(
  rows: Array<{ $op: string; $ts: unknown; f: string; p: string; r: string; t: string }>,
): RelLine[] {
  return rows.map((row) => ({
    $op: row.$op as RelLine['$op'],
    $ts: toNumber(row.$ts),
    f: row.f,
    p: row.p,
    r: row.r,
    t: row.t,
  }))
}

/**
 * Decode raw Parquet event rows into EventLine records.
 */
function decodeEventRows(
  rows: Array<{
    id: string
    ts: unknown
    op: string
    ns: string
    eid: string
    before?: string
    after?: string
    actor?: string
  }>,
): EventLine[] {
  return rows.map((row) => {
    const event: EventLine = {
      id: row.id,
      ts: toNumber(row.ts),
      op: row.op as EventLine['op'],
      ns: row.ns,
      eid: row.eid,
    }

    if (row.before && row.before !== '') {
      try {
        event.before = JSON.parse(row.before) as Record<string, unknown>
      } catch {
        console.warn(`[do-compactor] Skipping corrupted before JSON for event ${row.id}: ${row.before.slice(0, 100)}`)
      }
    }
    if (row.after && row.after !== '') {
      try {
        event.after = JSON.parse(row.after) as Record<string, unknown>
      } catch {
        console.warn(`[do-compactor] Skipping corrupted after JSON for event ${row.id}: ${row.after.slice(0, 100)}`)
      }
    }
    if (row.actor && row.actor !== '') {
      event.actor = row.actor
    }

    return event
  })
}

// =============================================================================
// DOCompactor
// =============================================================================

export class DOCompactor {
  private wal: SqliteWal
  private bucket: R2Bucket

  constructor(wal: SqliteWal, bucket: R2Bucket) {
    this.wal = wal
    this.bucket = bucket
  }

  // ---------------------------------------------------------------------------
  // compactTable — data tables
  // ---------------------------------------------------------------------------

  /**
   * Compact a single data table: read WAL -> merge with existing R2 Parquet -> write new Parquet -> mark flushed.
   *
   * @param table - The table/namespace name (e.g. "users", "posts")
   * @returns { count, flushed } or null if nothing to compact
   */
  async compactTable(table: string): Promise<{ count: number; flushed: number } | null> {
    // 1. Get unflushed batches from WAL
    const batches = this.wal.getBatches(table)
    if (batches.length === 0) return null

    // 2. Replay unflushed lines
    const walLines = this.wal.replayUnflushed<DataLine>(table)

    // 3. Read existing Parquet from R2
    const r2Key = `data/${table}.parquet`
    const existingRaw = await readParquetFromR2<{
      $id: string
      $op: string
      $v: unknown
      $ts: unknown
      $data?: unknown
    }>(this.bucket, r2Key)
    const existing = decodeDataRows(existingRaw)

    // 4. Merge using ReplacingMergeTree semantics
    const merged = mergeResults(existing, walLines)

    // 5. Encode to Parquet
    const buffer = await encodeDataToParquet(merged)

    // 6. Write to R2
    await this.bucket.put(r2Key, buffer)

    // 7. Mark flushed
    const batchIds = batches.map((b) => b.id)
    this.wal.markFlushed(batchIds)

    return { count: merged.length, flushed: batchIds.length }
  }

  // ---------------------------------------------------------------------------
  // compactRels — relationships
  // ---------------------------------------------------------------------------

  /**
   * Compact relationships: merge WAL rels with existing R2 Parquet, filtering unlinks.
   *
   * @returns { count, flushed } or null if nothing to compact
   */
  async compactRels(): Promise<{ count: number; flushed: number } | null> {
    // 1. Get unflushed batches from WAL
    const batches = this.wal.getBatches('rels')
    if (batches.length === 0) return null

    // 2. Replay unflushed lines
    const walLines = this.wal.replayUnflushed<RelLine>('rels')

    // 3. Read existing Parquet from R2
    const r2Key = 'rels/rels.parquet'
    const existingRaw = await readParquetFromR2<{
      $op: string
      $ts: unknown
      f: string
      p: string
      r: string
      t: string
    }>(this.bucket, r2Key)
    const existing = decodeRelRows(existingRaw)

    // 4–5. Merge using shared logic: dedup by f:p:t, $ts wins, filter unlinks, sort
    const live = mergeRelationships(existing, walLines)

    // 6. Encode to Parquet
    const buffer = await encodeRelsToParquet(live)

    // 7. Write to R2
    await this.bucket.put(r2Key, buffer)

    // 8. Mark flushed
    const batchIds = batches.map((b) => b.id)
    this.wal.markFlushed(batchIds)

    return { count: live.length, flushed: batchIds.length }
  }

  // ---------------------------------------------------------------------------
  // compactEvents — CDC events (append-only)
  // ---------------------------------------------------------------------------

  /**
   * Compact events: append WAL events to existing R2 Parquet (no merge, pure concatenation).
   *
   * @returns { count, flushed } or null if nothing to compact
   */
  async compactEvents(): Promise<{ count: number; flushed: number } | null> {
    // 1. Get unflushed batches from WAL
    const batches = this.wal.getBatches('events')
    if (batches.length === 0) return null

    // 2. Replay unflushed event lines
    const walLines = this.wal.replayUnflushed<AnyEventLine>('events')

    // 3. Read existing Parquet from R2
    const r2Key = 'events/events.parquet'
    const existingRaw = await readParquetFromR2<{
      id: string
      ts: unknown
      op: string
      ns: string
      eid: string
      before?: string
      after?: string
      actor?: string
    }>(this.bucket, r2Key)
    const existing = decodeEventRows(existingRaw)

    // 4–5. Merge using shared logic: concatenate and sort by ts
    const all = mergeEvents(existing, walLines)

    // 6. Encode to Parquet
    const buffer = await encodeEventsToParquet(all)

    // 7. Write to R2
    await this.bucket.put(r2Key, buffer)

    // 8. Mark flushed
    const batchIds = batches.map((b) => b.id)
    this.wal.markFlushed(batchIds)

    return { count: all.length, flushed: batchIds.length }
  }

  // ---------------------------------------------------------------------------
  // compactAll — compact every unflushed table + rels + events
  // ---------------------------------------------------------------------------

  /**
   * Compact all unflushed tables, relationships, and events.
   *
   * @returns Summary of compacted counts per table
   */
  async compactAll(): Promise<{
    tables: Map<string, number>
    rels: number | null
    events: number | null
  }> {
    const tables = new Map<string, number>()

    // Get all unflushed data tables (excluding 'rels' and 'events' which are special)
    const unflushedTables = this.wal.getUnflushedTables()
    const dataTables = unflushedTables.filter((t) => t !== 'rels' && t !== 'events')

    for (const table of dataTables) {
      const result = await this.compactTable(table)
      if (result) {
        tables.set(table, result.count)
      }
    }

    const relsResult = await this.compactRels()
    const eventsResult = await this.compactEvents()

    return {
      tables,
      rels: relsResult?.count ?? null,
      events: eventsResult?.count ?? null,
    }
  }

  // ---------------------------------------------------------------------------
  // shouldCompact — threshold check
  // ---------------------------------------------------------------------------

  /**
   * Check if compaction is needed based on unflushed row count.
   *
   * @param threshold - Minimum unflushed row count to trigger compaction (default: 100)
   * @returns true if the unflushed count exceeds the threshold
   */
  shouldCompact(threshold: number = 100): boolean {
    return this.wal.getUnflushedCount() >= threshold
  }
}

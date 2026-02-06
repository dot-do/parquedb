/**
 * DOReadPath -- Three-way merge-on-read for the MergeTree engine.
 *
 * The MergeTree engine has a three-level read path:
 *   1. R2 Parquet (base layer, compacted data)
 *   2. SQLite WAL (unflushed mutations)
 *   3. (Optional) In-memory buffer (handled at a higher level)
 *
 * This class implements the merge-on-read logic for the DO (Durable Object)
 * context where reads combine R2 Parquet files with unflushed SQLite WAL entries.
 *
 * For entities (DataLine), it uses ReplacingMergeTree semantics: highest $v wins,
 * tombstones ($op='d') are excluded from results.
 *
 * For relationships (RelLine), it merges by f:p:t composite key with latest $ts
 * winning, and filters out unlinks ($op='u').
 *
 * For events, it's append-only: concatenate and sort by ts.
 */

import { SqliteWal } from './sqlite-wal'
import { mergeResults } from './merge'
import type { DataLine, RelLine } from './types'
import { mergeRelationships } from './merge-rels'
import { mergeEvents } from './merge-events'
import type { AnyEventLine } from './merge-events'
import { matchesFilter } from './filter'
import {
  readParquetFromR2,
  decodeDataRows,
  decodeRelRows,
  decodeEventRows,
} from './r2-parquet-utils'

// =============================================================================
// DOReadPath
// =============================================================================

export class DOReadPath {
  constructor(
    private wal: SqliteWal,
    private bucket: R2Bucket,
  ) {}

  /**
   * Find entities in a table with three-way merge:
   * 1. Read from R2 Parquet (base layer)
   * 2. Merge with unflushed SQLite WAL entries
   * 3. Apply optional filter
   * Returns merged, de-duplicated results.
   */
  async find(table: string, filter?: Record<string, unknown>): Promise<DataLine[]> {
    // 1. Read R2 Parquet
    const r2Rows = await readParquetFromR2(this.bucket, `data/${table}.parquet`)
    const r2Lines = decodeDataRows(r2Rows)

    // 2. Read WAL
    const walLines = this.wal.replayUnflushed<DataLine>(table)

    // 3. Merge (WAL overrides R2 since it's newer, highest $v wins)
    const merged = mergeResults(r2Lines, walLines)

    // 4. Apply filter if provided
    if (filter && Object.keys(filter).length > 0) {
      return merged.filter((entity) => matchesFilter(entity, filter))
    }

    return merged
  }

  /**
   * Fast path: get single entity by ID.
   * Checks WAL first (most recent), then R2 Parquet.
   * Returns the entity with highest $v, or null if deleted/not found.
   */
  async getById(table: string, id: string): Promise<DataLine | null> {
    // 1. Check WAL first
    const walLines = this.wal
      .replayUnflushed<DataLine>(table)
      .filter((l) => l.$id === id)

    // 2. Read R2 for the base entity
    const r2Rows = await readParquetFromR2(this.bucket, `data/${table}.parquet`)
    const r2Lines = decodeDataRows(r2Rows).filter((l) => l.$id === id)

    // 3. Merge and pick highest $v
    const allLines = [...r2Lines, ...walLines]
    if (allLines.length === 0) return null

    // Pick the entry with highest $v (walLines come second, so on tie they win)
    let best = allLines[0]
    for (let i = 1; i < allLines.length; i++) {
      if (allLines[i].$v >= best.$v) {
        best = allLines[i]
      }
    }

    // 4. If the winning entry is a delete, return null
    if (best.$op === 'd') return null

    return best
  }

  /**
   * Find relationships with WAL + R2 merge.
   * Merges by f:p:t key, latest $ts wins, filters out unlinks.
   */
  async findRels(fromId?: string): Promise<RelLine[]> {
    // 1. Read R2 rels
    const r2Rows = await readParquetFromR2(this.bucket, 'rels/rels.parquet')
    const r2Lines = decodeRelRows(r2Rows)

    // 2. Read WAL rels
    const walLines = this.wal.replayUnflushed<RelLine>('rels')

    // 3–4. Merge using shared logic: dedup by f:p:t, $ts wins, filter unlinks, sort
    const results = mergeRelationships(r2Lines, walLines)

    // 5. If fromId, filter by f === fromId
    if (fromId) {
      return results.filter((rel) => rel.f === fromId)
    }

    return results
  }

  /**
   * Find events (append-only, no merge needed -- just concatenate).
   */
  async findEvents(): Promise<AnyEventLine[]> {
    // 1. Read R2 events and decode through decodeEventRows
    const r2Rows = await readParquetFromR2(this.bucket, 'events/events.parquet')
    const r2Events = decodeEventRows(r2Rows)

    // 2. Read WAL events
    const walLines = this.wal.replayUnflushed<AnyEventLine>('events')

    // 3. Merge using shared logic: concatenate and sort by ts
    return mergeEvents(r2Events, walLines)
  }
}

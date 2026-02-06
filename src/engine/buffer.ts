/**
 * TableBuffer -- In-memory buffer for the MergeTree engine.
 *
 * Holds recent mutations in a Map keyed by entity $id before they are
 * compacted into Parquet files. The buffer is rebuilt from JSONL replay
 * on startup.
 *
 * Key invariants:
 * - Deletes are stored as **tombstones** ($op: 'd'), not removed from the Map.
 *   Tombstones are essential for suppressing stale Parquet entities during
 *   merge-on-read.
 * - set() always overwrites the previous entry for the same $id, whether it
 *   was a live entity or a tombstone.
 * - scan() only returns live entities (excludes tombstones), with optional
 *   MongoDB-style filter matching.
 */

import type { DataLine } from '@/engine/types'
import type { ComparisonFilter } from '@/engine/filter'
import { matchesFilter } from '@/engine/filter'

// Re-export matchesFilter so existing imports from buffer.ts continue to work
export { matchesFilter }

// Re-export ComparisonFilter so consumers can import from buffer.ts
export type { ComparisonFilter }

// =============================================================================
// Filter types for scan()
// =============================================================================

/** Value for a field condition: a literal for implicit $eq, or a ComparisonFilter object */
export type FieldCondition = ComparisonFilter | string | number | boolean | null | undefined

/**
 * A MongoDB-style scan filter.
 *
 * Keys are either field paths (supporting dot notation for nested fields) or
 * logical operators (`$or`, `$and`).
 *
 * Field values can be:
 * - Literals (implicit `$eq`): `{ name: 'Alice' }`
 * - ComparisonFilter objects: `{ age: { $gt: 18, $lt: 65 } }`
 * - Dot-notation for nested fields: `{ 'address.city': 'NYC' }`
 *
 * Logical operators:
 * - `$or`: Array of sub-filters, at least one must match
 * - `$and`: Array of sub-filters, all must match
 *
 * All top-level conditions are ANDed together.
 */
export interface ScanFilter {
  /** Logical OR: at least one sub-filter must match */
  $or?: ScanFilter[]
  /** Logical AND: all sub-filters must match */
  $and?: ScanFilter[]
  /** Field path conditions (implicit $eq or ComparisonFilter) */
  [field: string]: FieldCondition | ScanFilter[] | undefined
}

// =============================================================================
// TableBuffer
// =============================================================================

export class TableBuffer {
  private readonly store: Map<string, DataLine> = new Map()

  /**
   * Store or overwrite an entity in the buffer.
   * If a tombstone exists for the same $id, it will be replaced.
   */
  set(entity: DataLine): void {
    this.store.set(entity.$id, entity)
  }

  /**
   * Retrieve an entity (or tombstone) by $id.
   * Returns undefined if the $id is not in the buffer at all.
   */
  get(id: string): DataLine | undefined {
    return this.store.get(id)
  }

  /**
   * Check whether an entry exists in the buffer (live or tombstone).
   */
  has(id: string): boolean {
    return this.store.has(id)
  }

  /**
   * Check whether the entry for the given $id is a tombstone.
   * Returns false if the entry is missing or is a live entity.
   */
  isTombstone(id: string): boolean {
    const entry = this.store.get(id)
    return entry !== undefined && entry.$op === 'd'
  }

  /**
   * Delete an entity by replacing it with a tombstone.
   * If the entity doesn't exist, a tombstone is still created so that it
   * can suppress any matching entity from the Parquet layer during merge-on-read.
   */
  delete(id: string, version: number, ts: number): void {
    this.store.set(id, { $id: id, $op: 'd', $v: version, $ts: ts })
  }

  /**
   * Scan live entities (excluding tombstones), optionally filtered.
   *
   * Filter supports:
   * - Simple field equality: `{ name: 'Alice' }`
   * - Dot-notation for nested fields: `{ 'address.city': 'NYC' }`
   * - Comparison operators: `{ age: { $gt: 18 } }`
   * - $in: `{ role: { $in: ['admin', 'mod'] } }`
   * - $exists: `{ email: { $exists: true } }`
   *
   * All filter keys are ANDed together.
   */
  scan(filter?: ScanFilter): DataLine[] {
    const results: DataLine[] = []
    for (const entity of this.store.values()) {
      // Skip tombstones
      if (entity.$op === 'd') continue

      // Apply filter if provided
      if (filter && !matchesFilter(entity, filter)) continue

      results.push(entity)
    }
    return results
  }

  /**
   * Count live entities matching an optional filter, without collecting them
   * into an array.
   *
   * This is more efficient than `scan(filter).length` because it avoids
   * allocating the results array entirely — only a counter is maintained.
   */
  count(filter?: ScanFilter): number {
    let count = 0
    for (const entity of this.store.values()) {
      // Skip tombstones
      if (entity.$op === 'd') continue

      // Apply filter if provided
      if (filter && !matchesFilter(entity, filter)) continue

      count++
    }
    return count
  }

  /**
   * Remove all entries from the buffer.
   */
  clear(): void {
    this.store.clear()
  }

  /**
   * Total number of entries in the buffer (live + tombstones).
   */
  get size(): number {
    return this.store.size
  }

  /**
   * Number of live (non-tombstone) entities in the buffer.
   */
  get liveSize(): number {
    let count = 0
    for (const entity of this.store.values()) {
      if (entity.$op !== 'd') count++
    }
    return count
  }

  /**
   * Iterate all entries including tombstones.
   */
  entries(): IterableIterator<[string, DataLine]> {
    return this.store.entries()
  }
}

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

// =============================================================================
// Filter types for scan()
// =============================================================================

/** Comparison operators supported in scan filters */
interface ComparisonFilter {
  $eq?: unknown
  $ne?: unknown
  $gt?: number | string
  $gte?: number | string
  $lt?: number | string
  $lte?: number | string
  $in?: unknown[]
  $exists?: boolean
}

/** A scan filter: keys are field paths, values are literals or comparison objects */
export type ScanFilter = Record<string, unknown>

// =============================================================================
// Internal helpers
// =============================================================================

/**
 * Resolve a dot-notation path on an object.
 * e.g. getNestedValue({ address: { city: 'NYC' } }, 'address.city') => 'NYC'
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

/**
 * Check whether a value is a comparison filter object (has operator keys).
 */
function isComparisonFilter(value: unknown): value is ComparisonFilter {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false
  }
  const keys = Object.keys(value as object)
  return keys.length > 0 && keys.every(k => k.startsWith('$'))
}

/**
 * Evaluate a single field condition against an entity value.
 */
function matchFieldCondition(entityValue: unknown, condition: unknown): boolean {
  // If the condition is a comparison filter object, evaluate each operator
  if (isComparisonFilter(condition)) {
    return matchComparisonFilter(entityValue, condition)
  }
  // Otherwise it's a direct equality check
  return entityValue === condition
}

/**
 * Evaluate a comparison filter (e.g. { $gt: 18, $lt: 65 }) against a value.
 * All operators in the filter must match (implicit AND).
 */
function matchComparisonFilter(value: unknown, filter: ComparisonFilter): boolean {
  if ('$eq' in filter && value !== filter.$eq) return false
  if ('$ne' in filter && value === filter.$ne) return false

  if ('$gt' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) > (filter.$gt as number))) return false
  }
  if ('$gte' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) >= (filter.$gte as number))) return false
  }
  if ('$lt' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) < (filter.$lt as number))) return false
  }
  if ('$lte' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) <= (filter.$lte as number))) return false
  }

  if ('$in' in filter) {
    if (!Array.isArray(filter.$in)) return false
    if (!filter.$in.includes(value)) return false
  }

  if ('$exists' in filter) {
    const exists = value !== undefined
    if (filter.$exists && !exists) return false
    if (!filter.$exists && exists) return false
  }

  return true
}

/**
 * Check whether an entity matches all conditions in a scan filter.
 */
export function matchesFilter(entity: DataLine, filter: ScanFilter): boolean {
  for (const [path, condition] of Object.entries(filter)) {
    const value = getNestedValue(entity as unknown as Record<string, unknown>, path)
    if (!matchFieldCondition(value, condition)) {
      return false
    }
  }
  return true
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

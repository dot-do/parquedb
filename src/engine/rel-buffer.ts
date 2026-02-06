/**
 * RelationshipBuffer -- In-memory buffer for relationships in the MergeTree engine.
 *
 * Holds recent relationship mutations in a Map keyed by composite 'f:p:t' before
 * they are compacted into Parquet files. The buffer is rebuilt from JSONL replay
 * on startup.
 *
 * Key invariants:
 * - Unlinks are stored as **tombstones** ($op: 'u'), not removed from the Map.
 *   Tombstones are essential for suppressing stale Parquet relationships during
 *   merge-on-read.
 * - Forward/reverse indexes only include LIVE relationships (not tombstones).
 * - link() adds to both indexes, stores with $op='l'.
 * - unlink() removes from both indexes, stores tombstone with $op='u'.
 * - getAll() returns only live (non-tombstone) entries.
 * - entries() includes tombstones (needed for compaction).
 * - size counts only live entries.
 */

import type { RelLine, RelOp } from '@/engine/types'

// =============================================================================
// Input types
// =============================================================================

export interface LinkInput {
  /** From entity $id */
  f: string
  /** Predicate (forward relationship name) */
  p: string
  /** Reverse relationship name */
  r: string
  /** To entity $id */
  t: string
  /** Epoch milliseconds timestamp */
  $ts: number
}

export interface UnlinkInput {
  /** From entity $id */
  f: string
  /** Predicate (forward relationship name) */
  p: string
  /** To entity $id */
  t: string
  /** Epoch milliseconds timestamp */
  $ts: number
}

// =============================================================================
// RelationshipBuffer
// =============================================================================

export class RelationshipBuffer {
  /** Primary store keyed by composite 'f:p:t', holds all entries including tombstones */
  private readonly store: Map<string, RelLine> = new Map()

  /** Forward index: fromId -> predicate -> Set<toId> (live relationships only) */
  private readonly forwardIndex: Map<string, Map<string, Set<string>>> = new Map()

  /** Reverse index: toId -> reverseName -> Set<fromId> (live relationships only) */
  private readonly reverseIndex: Map<string, Map<string, Set<string>>> = new Map()

  /**
   * Create a link between two entities.
   *
   * If a link with the same f:p:t already exists (live or tombstoned), it is
   * overwritten with the new timestamp and $op='l'. The forward and reverse
   * indexes are updated to include the relationship.
   */
  link(input: LinkInput): void {
    const key = this.compositeKey(input.f, input.p, input.t)

    const line: RelLine = {
      $op: 'l',
      $ts: input.$ts,
      f: input.f,
      p: input.p,
      r: input.r,
      t: input.t,
    }

    this.store.set(key, line)

    // Update forward index: fromId -> predicate -> Set<toId>
    this.addToForwardIndex(input.f, input.p, input.t)

    // Update reverse index: toId -> reverseName -> Set<fromId>
    this.addToReverseIndex(input.t, input.r, input.f)
  }

  /**
   * Remove a link between two entities.
   *
   * The link is replaced with a tombstone ($op='u') in the store, and removed
   * from both the forward and reverse indexes. If the link doesn't exist, the
   * tombstone is still stored (for merge-on-read suppression) but no index
   * modification is needed.
   *
   * Note: unlink needs the reverse name to clean up the reverse index. It looks
   * up the existing entry in the store to find it. For non-existent relationships,
   * the tombstone is stored without a reverse name since there's nothing to clean
   * up in the reverse index.
   */
  unlink(input: UnlinkInput): void {
    const key = this.compositeKey(input.f, input.p, input.t)

    // Look up existing entry to get the reverse name for index cleanup
    const existing = this.store.get(key)
    const reverseName = existing?.r ?? ''

    const line: RelLine = {
      $op: 'u',
      $ts: input.$ts,
      f: input.f,
      p: input.p,
      r: reverseName,
      t: input.t,
    }

    this.store.set(key, line)

    // Remove from forward index
    this.removeFromForwardIndex(input.f, input.p, input.t)

    // Remove from reverse index (only if we know the reverse name)
    if (reverseName) {
      this.removeFromReverseIndex(input.t, reverseName, input.f)
    }
  }

  /**
   * Get all target entity IDs linked from a given entity via a specific predicate.
   * Returns only live (non-tombstoned) relationships.
   */
  getForward(fromId: string, predicate: string): string[] {
    const predicateMap = this.forwardIndex.get(fromId)
    if (!predicateMap) return []

    const targets = predicateMap.get(predicate)
    if (!targets) return []

    return [...targets]
  }

  /**
   * Get all source entity IDs that link to a given entity via a specific reverse name.
   * Returns only live (non-tombstoned) relationships.
   */
  getReverse(toId: string, reverseName: string): string[] {
    const reverseMap = this.reverseIndex.get(toId)
    if (!reverseMap) return []

    const sources = reverseMap.get(reverseName)
    if (!sources) return []

    return [...sources]
  }

  /**
   * Check whether a live link exists between two entities for a given predicate.
   * Returns false for tombstoned or non-existent relationships.
   */
  hasLink(f: string, p: string, t: string): boolean {
    const key = this.compositeKey(f, p, t)
    const entry = this.store.get(key)
    return entry !== undefined && entry.$op === 'l'
  }

  /**
   * Check whether a relationship has been explicitly unlinked (tombstoned).
   * Returns true only for entries with $op='u'.
   * Returns false for live relationships or non-existent entries.
   */
  isUnlinked(f: string, p: string, t: string): boolean {
    const key = this.compositeKey(f, p, t)
    const entry = this.store.get(key)
    return entry !== undefined && entry.$op === 'u'
  }

  /**
   * Get all live (non-tombstoned) relationships as RelLine[].
   */
  getAll(): RelLine[] {
    const results: RelLine[] = []
    for (const line of this.store.values()) {
      if (line.$op === 'l') {
        results.push(line)
      }
    }
    return results
  }

  /**
   * Remove all entries from the buffer and clear all indexes.
   */
  clear(): void {
    this.store.clear()
    this.forwardIndex.clear()
    this.reverseIndex.clear()
  }

  /**
   * Count of live (non-tombstoned) relationships in the buffer.
   */
  get size(): number {
    let count = 0
    for (const line of this.store.values()) {
      if (line.$op === 'l') count++
    }
    return count
  }

  /**
   * Iterate all entries including tombstones.
   * Needed for compaction — tombstones must be written to the compacted Parquet
   * file to suppress stale entries from older Parquet files.
   */
  entries(): IterableIterator<[string, RelLine]> {
    return this.store.entries()
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

  private compositeKey(f: string, p: string, t: string): string {
    return `${f}:${p}:${t}`
  }

  private addToForwardIndex(fromId: string, predicate: string, toId: string): void {
    let predicateMap = this.forwardIndex.get(fromId)
    if (!predicateMap) {
      predicateMap = new Map()
      this.forwardIndex.set(fromId, predicateMap)
    }

    let targets = predicateMap.get(predicate)
    if (!targets) {
      targets = new Set()
      predicateMap.set(predicate, targets)
    }

    targets.add(toId)
  }

  private removeFromForwardIndex(fromId: string, predicate: string, toId: string): void {
    const predicateMap = this.forwardIndex.get(fromId)
    if (!predicateMap) return

    const targets = predicateMap.get(predicate)
    if (!targets) return

    targets.delete(toId)

    // Clean up empty sets/maps
    if (targets.size === 0) {
      predicateMap.delete(predicate)
    }
    if (predicateMap.size === 0) {
      this.forwardIndex.delete(fromId)
    }
  }

  private addToReverseIndex(toId: string, reverseName: string, fromId: string): void {
    let reverseMap = this.reverseIndex.get(toId)
    if (!reverseMap) {
      reverseMap = new Map()
      this.reverseIndex.set(toId, reverseMap)
    }

    let sources = reverseMap.get(reverseName)
    if (!sources) {
      sources = new Set()
      reverseMap.set(reverseName, sources)
    }

    sources.add(fromId)
  }

  private removeFromReverseIndex(toId: string, reverseName: string, fromId: string): void {
    const reverseMap = this.reverseIndex.get(toId)
    if (!reverseMap) return

    const sources = reverseMap.get(reverseName)
    if (!sources) return

    sources.delete(fromId)

    // Clean up empty sets/maps
    if (sources.size === 0) {
      reverseMap.delete(reverseName)
    }
    if (reverseMap.size === 0) {
      this.reverseIndex.delete(toId)
    }
  }
}

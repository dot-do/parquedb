/**
 * Shared Relationship Merge Logic for MergeTree Engine
 *
 * Provides the canonical implementation for merging relationship arrays
 * using f:p:t composite key deduplication with $ts-based conflict resolution.
 *
 * Previously this logic was independently implemented in:
 * - compactor-rels.ts (compactRelationships)
 * - storage-adapters.ts (hybridCompactRels)
 * - do-compactor.ts (DOCompactor.compactRels)
 * - do-read-path.ts (DOReadPath.findRels)
 *
 * Each had slight inconsistencies (some used iteration order, some used $ts).
 * This module standardizes on $ts-based conflict resolution: for duplicate
 * f:p:t keys, the entry with the higher $ts wins. On ties, overlay wins.
 */

import type { RelLine } from './types'

// =============================================================================
// Composite Key
// =============================================================================

/**
 * Build the dedup key for a relationship: `f:p:t`
 * (from entity + predicate + to entity).
 */
export function relKey(rel: RelLine): string {
  return `${rel.f}:${rel.p}:${rel.t}`
}

// =============================================================================
// Sort Comparator
// =============================================================================

/**
 * Compare two RelLines by (f, p, t) for deterministic output ordering.
 */
export function relComparator(a: RelLine, b: RelLine): number {
  if (a.f !== b.f) return a.f < b.f ? -1 : 1
  if (a.p !== b.p) return a.p < b.p ? -1 : 1
  if (a.t !== b.t) return a.t < b.t ? -1 : 1
  return 0
}

// =============================================================================
// mergeRelationships
// =============================================================================

/**
 * Merge relationship arrays using ReplacingMergeTree semantics.
 *
 * For duplicate f:p:t keys, the entry with the higher $ts wins.
 * On equal $ts, the overlay entry wins (assumed more recent).
 * Tombstones ($op === 'u' for unlink) are filtered from the result.
 * Output is sorted by (f, p, t) for deterministic ordering.
 *
 * @param base - Existing relationships (e.g. from Parquet / R2)
 * @param overlay - New mutations (e.g. from JSONL / WAL)
 * @returns Merged, deduplicated, tombstone-free relationships sorted by (f, p, t)
 */
export function mergeRelationships(base: RelLine[], overlay: RelLine[]): RelLine[] {
  const merged = new Map<string, RelLine>()

  // 1. Seed with base entries
  for (const rel of base) {
    merged.set(relKey(rel), rel)
  }

  // 2. Overlay: higher $ts wins, ties go to overlay (>= check)
  for (const rel of overlay) {
    const key = relKey(rel)
    const existing = merged.get(key)
    if (!existing || rel.$ts >= existing.$ts) {
      merged.set(key, rel)
    }
  }

  // 3. Filter tombstones ($op === 'u') and sort by (f, p, t)
  return Array.from(merged.values())
    .filter(r => r.$op !== 'u')
    .sort(relComparator)
}

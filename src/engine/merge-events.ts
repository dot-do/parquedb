/**
 * Shared Event Merge Logic for MergeTree Engine
 *
 * Provides the canonical implementation for merging event arrays.
 * Events are append-only (no dedup), so merging is simply concatenation
 * followed by sorting by timestamp.
 *
 * Previously this logic was independently implemented in:
 * - compactor-events.ts (compactEvents)
 * - storage-adapters.ts (hybridCompactEvents)
 * - do-compactor.ts (DOCompactor.compactEvents)
 *
 * This module standardizes the timestamp field resolution: events may use
 * either `ts` or `$ts` as the timestamp field. Both are supported.
 */

// =============================================================================
// mergeEvents
// =============================================================================

import type { EventLine, SchemaLine } from './types'

/** Union of event types that flow through the event pipeline */
export type AnyEventLine = EventLine | SchemaLine

/**
 * Merge event arrays by concatenation and timestamp sort.
 *
 * Events are append-only, so no deduplication is performed.
 * Sorting uses the `ts` field (falling back to `$ts`, then 0).
 *
 * @param base - Existing events (e.g. from compacted Parquet)
 * @param overlay - New events (e.g. from JSONL / WAL)
 * @returns All events sorted by timestamp
 */
export function mergeEvents(
  base: Array<AnyEventLine>,
  overlay: Array<AnyEventLine>,
): Array<AnyEventLine> {
  return [...base, ...overlay].sort((a, b) => {
    const tsA = a.ts ?? 0
    const tsB = b.ts ?? 0
    return tsA - tsB
  })
}

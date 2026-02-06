/**
 * Event Compactor for the MergeTree Engine
 *
 * The simplest compactor -- events are append-only and never deduplicated.
 * Compaction converts JSONL to the compacted format (Parquet in production,
 * JSON in tests via the storage adapter), appending new events to existing ones.
 *
 * Algorithm:
 *   1. Rotate events.jsonl -> events.jsonl.compacting
 *   2. Read existing compacted events via storage adapter
 *   3. Read rotated JSONL via jsonl-reader
 *   4. Concatenate: [...existing, ...new]
 *   5. Sort by ts
 *   6. Write to tmp file, atomic rename, cleanup
 *   7. Return total count (or null if nothing to compact)
 *
 * No dedup -- this is a pure append-only log.
 */

import { rename } from 'node:fs/promises'
import { join } from 'node:path'
import { rotate, cleanup } from './rotation'
import { replay } from './jsonl-reader'
import { lineCount } from './jsonl-reader'
import { mergeEvents } from './merge-events'
import type { AnyEventLine } from './merge-events'

// =============================================================================
// Types
// =============================================================================

/**
 * Storage adapter interface for reading/writing compacted event files.
 * In production this would use Parquet; in tests it uses JSON.
 */
export interface EventStorageAdapter {
  /** Read all events from a compacted file. Returns [] if file doesn't exist. */
  readEvents(path: string): Promise<AnyEventLine[]>
  /** Write all events to a compacted file (overwrite). */
  writeEvents(path: string, data: AnyEventLine[]): Promise<void>
}

/** Options for shouldCompact threshold check. */
export interface CompactOptions {
  /** Minimum number of lines in the JSONL file to trigger compaction. Default: 100 */
  lineThreshold?: number
}

// =============================================================================
// Constants
// =============================================================================

const JSONL_FILENAME = 'events.jsonl'
const COMPACTED_FILENAME = 'events.compacted'
const TMP_SUFFIX = '.tmp'
const DEFAULT_LINE_THRESHOLD = 100

// =============================================================================
// Public API
// =============================================================================

/**
 * Compact the events JSONL file into the compacted format.
 *
 * @param dataDir - Directory containing events.jsonl and events.compacted
 * @param storage - Storage adapter for reading/writing compacted files
 * @returns Total number of events in the compacted file, or null if nothing to compact
 */
export async function compactEvents(
  dataDir: string,
  storage: EventStorageAdapter,
): Promise<number | null> {
  const jsonlPath = join(dataDir, JSONL_FILENAME)
  const compactedPath = join(dataDir, COMPACTED_FILENAME)
  const tmpPath = compactedPath + TMP_SUFFIX

  // 1. Rotate events.jsonl -> events.jsonl.compacting
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    // Nothing to rotate -- either no file or compaction already in progress
    return null
  }

  try {
    // 2. Read existing compacted events
    const existing = await storage.readEvents(compactedPath)

    // 3. Read rotated JSONL
    const newEvents = await replay<AnyEventLine>(compactingPath)

    // If rotated JSONL was empty, skip compaction
    if (newEvents.length === 0 && existing.length === 0) {
      await cleanup(compactingPath)
      return null
    }

    // 4–5. Merge using shared logic: concatenate and sort by ts
    const all = mergeEvents(existing, newEvents)

    // 6. Write to tmp file, then atomic rename
    await storage.writeEvents(tmpPath, all)
    await rename(tmpPath, compactedPath)

    // 7. Cleanup the .compacting file
    await cleanup(compactingPath)

    return all.length
  } catch (error) {
    // On error, still try to clean up, but re-throw
    // Note: we do NOT cleanup the .compacting file on error so recovery can retry
    throw error
  }
}

/**
 * Check if the events JSONL file should be compacted based on line count.
 *
 * @param dataDir - Directory containing events.jsonl
 * @param options - Optional threshold configuration
 * @returns true if the JSONL file exceeds the line threshold
 */
export async function shouldCompact(
  dataDir: string,
  options?: CompactOptions,
): Promise<boolean> {
  const threshold = options?.lineThreshold ?? DEFAULT_LINE_THRESHOLD
  const jsonlPath = join(dataDir, JSONL_FILENAME)
  const count = await lineCount(jsonlPath)
  return count >= threshold
}

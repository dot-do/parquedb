/**
 * Event Compactor for the MergeTree Engine
 *
 * The simplest compactor -- events are append-only and never deduplicated.
 * Compaction converts JSONL to the compacted format (Parquet in production,
 * JSON in tests via the storage adapter), appending new events to existing ones.
 *
 * Algorithm (non-partitioned):
 *   1. Rotate events.jsonl -> events.jsonl.compacting
 *   2. Read existing compacted events via storage adapter
 *   3. Read rotated JSONL via jsonl-reader
 *   4. Concatenate: [...existing, ...new]
 *   5. Sort by ts
 *   6. Write to tmp file, atomic rename, cleanup
 *   7. Return total count (or null if nothing to compact)
 *
 * Algorithm (partitioned):
 *   1. Rotate events.jsonl -> events.jsonl.compacting
 *   2. Read rotated JSONL via jsonl-reader
 *   3. If legacy events.compacted exists, read & migrate into partitions
 *   4. Group new events by partition key (YYYY-MM)
 *   5. For each affected partition: read existing, merge, write back
 *   6. Cleanup the .compacting file
 *   7. Return total count and list of affected partitions
 *
 * No dedup -- this is a pure append-only log.
 */

import { rename, mkdir, readdir, unlink, access } from 'node:fs/promises'
import { join } from 'node:path'
import { rotate, cleanup } from './rotation'
import { replay } from './jsonl-reader'
import { lineCount } from './jsonl-reader'
import { mergeEvents } from './merge-events'
import type { AnyEventLine } from './merge-events'
import type { EventStorageAdapter } from './storage-adapters'

// Re-export so existing consumers that import from './compactor-events' still work
export type { EventStorageAdapter } from './storage-adapters'

// =============================================================================
// Types
// =============================================================================

/** Options for shouldCompact threshold check. */
export interface CompactOptions {
  /** Minimum number of lines in the JSONL file to trigger compaction. Default: 100 */
  lineThreshold?: number
}

/** Result of a partitioned compaction. */
export interface PartitionedCompactResult {
  /** Total number of events across all affected partitions */
  totalEvents: number
  /** List of partition keys (YYYY-MM) that were written */
  partitions: string[]
}

/** Options for partitioned compaction. */
export interface PartitionedCompactOptions {
  /** Optional retention policy: max number of partitions to keep */
  maxPartitions?: number
  /** Optional retention policy: max age in milliseconds */
  maxAge?: number
}

/** Retention policy options for applyRetention. */
export interface RetentionOptions {
  /** Keep at most this many partitions (oldest are removed first) */
  maxPartitions?: number
  /** Remove partitions older than this many milliseconds */
  maxAge?: number
}

// =============================================================================
// Constants
// =============================================================================

const JSONL_FILENAME = 'events.jsonl'
const COMPACTED_FILENAME = 'events.compacted'
const EVENTS_DIR = 'events'
const COMPACTED_EXT = '.compacted'
const TMP_SUFFIX = '.tmp'
const DEFAULT_LINE_THRESHOLD = 100

// =============================================================================
// Helpers
// =============================================================================

/**
 * Atomically rename a file using the adapter's rename if available,
 * otherwise fall back to fs.rename (local disk).
 */
async function atomicRename(
  storage: EventStorageAdapter,
  fromPath: string,
  toPath: string,
): Promise<void> {
  if ('rename' in storage && typeof (storage as any).rename === 'function') {
    await (storage as any).rename(fromPath, toPath)
  } else {
    await rename(fromPath, toPath)
  }
}

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
    await atomicRename(storage, tmpPath, compactedPath)

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

// =============================================================================
// Partitioned Event Compaction
// =============================================================================

/**
 * Get the partition key (YYYY-MM) for a given epoch millisecond timestamp.
 */
export function getPartitionKey(ts: number): string {
  const d = new Date(ts)
  const year = d.getUTCFullYear()
  const month = String(d.getUTCMonth() + 1).padStart(2, '0')
  return `${year}-${month}`
}

/**
 * Check if a file exists at the given path.
 */
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}

/**
 * Get the timestamp for an event, resolving both `ts` and `$ts` fields.
 */
function getEventTs(event: AnyEventLine): number {
  return event.ts ?? 0
}

/**
 * Group events by their partition key (YYYY-MM).
 */
function groupByPartition(events: AnyEventLine[]): Map<string, AnyEventLine[]> {
  const groups = new Map<string, AnyEventLine[]>()
  for (const event of events) {
    const key = getPartitionKey(getEventTs(event))
    let group = groups.get(key)
    if (!group) {
      group = []
      groups.set(key, group)
    }
    group.push(event)
  }
  return groups
}

/**
 * Get the path for a partition file.
 */
function partitionPath(dataDir: string, key: string): string {
  return join(dataDir, EVENTS_DIR, key + COMPACTED_EXT)
}

/**
 * Compact the events JSONL file into time-based partitions.
 *
 * Events are partitioned by month (YYYY-MM). Only partitions that receive
 * new events are rewritten. Old partitions are preserved untouched.
 *
 * If a legacy events.compacted file exists, its events are migrated into
 * the appropriate partitions during compaction.
 *
 * @param dataDir - Directory containing events.jsonl
 * @param storage - Storage adapter for reading/writing compacted files
 * @param options - Optional partitioned compaction options
 * @returns Result with total event count and affected partitions, or null if nothing to compact
 */
export async function compactEventsPartitioned(
  dataDir: string,
  storage: EventStorageAdapter,
  options?: PartitionedCompactOptions,
): Promise<PartitionedCompactResult | null> {
  const jsonlPath = join(dataDir, JSONL_FILENAME)
  const eventsDir = join(dataDir, EVENTS_DIR)
  const legacyPath = join(dataDir, COMPACTED_FILENAME)

  // 1. Rotate events.jsonl -> events.jsonl.compacting
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    return null
  }

  try {
    // 2. Read rotated JSONL
    const newEvents = await replay<AnyEventLine>(compactingPath)

    // 3. Check for legacy events.compacted and migrate if it exists
    let legacyEvents: AnyEventLine[] = []
    const hasLegacy = await fileExists(legacyPath)
    if (hasLegacy) {
      legacyEvents = await storage.readEvents(legacyPath)
    }

    // Combine legacy + new events
    const allNewEvents = [...legacyEvents, ...newEvents]

    // If nothing to compact, bail out
    if (allNewEvents.length === 0) {
      await cleanup(compactingPath)
      return null
    }

    // 4. Ensure events/ directory exists
    await mkdir(eventsDir, { recursive: true })

    // 5. Group all new events by partition key
    const grouped = groupByPartition(allNewEvents)

    let totalEvents = 0
    const affectedPartitions: string[] = []

    // 6. For each affected partition: read existing, merge, write back
    for (const [key, partitionNewEvents] of grouped) {
      const pPath = partitionPath(dataDir, key)
      const tmpPath = pPath + TMP_SUFFIX

      // Read existing events in this partition
      const existing = await storage.readEvents(pPath)

      // Merge: concatenate and sort by ts
      const merged = mergeEvents(existing, partitionNewEvents)

      // Write to tmp, then atomic rename
      await storage.writeEvents(tmpPath, merged)
      await atomicRename(storage, tmpPath, pPath)

      totalEvents += merged.length
      affectedPartitions.push(key)
    }

    // 7. If legacy file was migrated, remove it
    if (hasLegacy && legacyEvents.length > 0) {
      try {
        await unlink(legacyPath)
      } catch {
        // Ignore if already gone
      }
    }

    // 8. Cleanup the .compacting file
    await cleanup(compactingPath)

    // 9. Apply retention if configured
    if (options?.maxPartitions || options?.maxAge) {
      await applyRetention(dataDir, options)
    }

    return {
      totalEvents,
      partitions: affectedPartitions.sort(),
    }
  } catch (error) {
    throw error
  }
}

/**
 * Read all events from all partitions (and legacy file) sorted by timestamp.
 *
 * @param dataDir - Directory containing events/ partition files
 * @param storage - Storage adapter for reading compacted files
 * @returns All events from all partitions, sorted by ts
 */
export async function readAllPartitions(
  dataDir: string,
  storage: EventStorageAdapter,
): Promise<AnyEventLine[]> {
  const eventsDir = join(dataDir, EVENTS_DIR)
  const legacyPath = join(dataDir, COMPACTED_FILENAME)
  const allEvents: AnyEventLine[] = []

  // Read legacy events.compacted if it exists
  const legacyEvents = await storage.readEvents(legacyPath)
  allEvents.push(...legacyEvents)

  // Read partition files from events/ directory
  try {
    const files = await readdir(eventsDir)
    const partitionFiles = files
      .filter(f => f.endsWith(COMPACTED_EXT))
      .sort() // Sort by partition key (YYYY-MM lexicographic order)

    for (const file of partitionFiles) {
      const events = await storage.readEvents(join(eventsDir, file))
      allEvents.push(...events)
    }
  } catch {
    // events/ directory doesn't exist -- no partitions
  }

  // Sort all events by timestamp
  return allEvents.sort((a, b) => getEventTs(a) - getEventTs(b))
}

/**
 * Apply a retention policy to event partitions.
 *
 * Removes partition files that exceed the configured limits.
 *
 * @param dataDir - Directory containing events/ partition files
 * @param options - Retention policy options
 * @returns List of partition keys that were removed
 */
export async function applyRetention(
  dataDir: string,
  options: RetentionOptions,
): Promise<string[]> {
  const eventsDir = join(dataDir, EVENTS_DIR)
  const removed: string[] = []

  // List existing partitions
  let files: string[]
  try {
    files = await readdir(eventsDir)
  } catch {
    // events/ directory doesn't exist
    return removed
  }

  const partitionKeys = files
    .filter(f => f.endsWith(COMPACTED_EXT))
    .map(f => f.replace(COMPACTED_EXT, ''))
    .sort() // Lexicographic sort = chronological for YYYY-MM

  if (partitionKeys.length === 0) return removed

  // Apply maxPartitions: keep only the N most recent
  if (options.maxPartitions !== undefined && partitionKeys.length > options.maxPartitions) {
    const toRemove = partitionKeys.slice(0, partitionKeys.length - options.maxPartitions)
    for (const key of toRemove) {
      try {
        await unlink(join(eventsDir, key + COMPACTED_EXT))
        removed.push(key)
      } catch {
        // Ignore removal errors
      }
    }
  }

  // Apply maxAge: remove partitions whose key represents a month older than the cutoff
  if (options.maxAge !== undefined) {
    const cutoff = Date.now() - options.maxAge
    const cutoffKey = getPartitionKey(cutoff)

    for (const key of partitionKeys) {
      // If partition key is before the cutoff month, remove it
      if (key < cutoffKey && !removed.includes(key)) {
        try {
          await unlink(join(eventsDir, key + COMPACTED_EXT))
          removed.push(key)
        } catch {
          // Ignore removal errors
        }
      }
    }
  }

  return removed
}

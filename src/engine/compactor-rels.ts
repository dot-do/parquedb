/**
 * Relationship Compactor for MergeTree Engine
 *
 * Merges relationship mutations from rels.jsonl into rels.parquet using
 * ReplacingMergeTree semantics. The dedup key is the composite `f:p:t`
 * (from entity + predicate + to entity).
 *
 * Algorithm:
 * 1. Rotate rels.jsonl -> rels.jsonl.compacting (atomic swap)
 * 2. Read existing rels.parquet via storage adapter
 * 3. Read rotated JSONL via jsonl-reader replay
 * 4. Build Map keyed by `f:p:t` from existing data
 * 5. Overlay JSONL entries: links replace/add, unlinks tombstone
 * 6. Filter out tombstones ($op='u')
 * 7. Sort by (f, p, t) for deterministic output
 * 8. Write to tmp file, atomic rename, cleanup .compacting
 * 9. Return count of live relationships (or null if nothing to compact)
 *
 * Design decisions:
 * - Storage adapter pattern allows testing with JSON files instead of Parquet
 * - Unlinks for non-existent relationships are silently ignored (no error)
 * - Output contains only live links ($op='l'), tombstones are consumed
 * - Sorting by (f, p, t) enables efficient range scans on the output file
 */

import { rename, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { rotate, cleanup } from './rotation'
import { replay, lineCount } from './jsonl-reader'
import type { RelLine } from './types'
import { mergeRelationships } from './merge-rels'
import type { RelStorageAdapter } from './storage-adapters'

// Re-export so existing consumers that import from './compactor-rels' still work
export type { RelStorageAdapter } from './storage-adapters'

// =============================================================================
// Compaction Thresholds
// =============================================================================

export interface CompactThresholds {
  /** Compact when JSONL exceeds this many lines */
  lineThreshold: number
  /** Compact when JSONL exceeds this many bytes (optional) */
  byteThreshold?: number
}

const DEFAULT_THRESHOLDS: CompactThresholds = {
  lineThreshold: 1000,
  byteThreshold: 10 * 1024 * 1024, // 10 MB
}



// =============================================================================
// Helpers
// =============================================================================

/**
 * Atomically rename a file using the adapter's rename if available,
 * otherwise fall back to fs.rename (local disk).
 */
async function atomicRename(
  storage: RelStorageAdapter,
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
// compactRelationships
// =============================================================================

/**
 * Compact relationship mutations from rels.jsonl into rels.parquet.
 *
 * @param dataDir - Directory containing rels.jsonl and rels.parquet
 * @param storage - Storage adapter for reading/writing the parquet file
 * @returns Count of live relationships in the output, or null if nothing to compact
 */
export async function compactRelationships(
  dataDir: string,
  storage: RelStorageAdapter,
): Promise<number | null> {
  const jsonlPath = join(dataDir, 'rels.jsonl')
  const parquetPath = join(dataDir, 'rels.parquet')
  const tmpPath = join(dataDir, 'rels.parquet.tmp')

  // 1. Rotate rels.jsonl -> rels.jsonl.compacting
  const compactingPath = await rotate(jsonlPath)
  if (compactingPath === null) {
    // Nothing to compact (file missing or compaction already in progress)
    return null
  }

  try {
    // 2. Read existing rels.parquet
    const existing = await storage.readRels(parquetPath)

    // 3. Read rotated JSONL
    const mutations = await replay<RelLine>(compactingPath)

    // If the rotated file had no lines, clean up and return null
    if (mutations.length === 0) {
      await cleanup(compactingPath)
      return null
    }

    // 4–7. Merge using shared logic: dedup by f:p:t, $ts wins, filter tombstones, sort
    const live = mergeRelationships(existing, mutations)

    // 8. Write to tmp, atomic rename
    await storage.writeRels(tmpPath, live)
    await atomicRename(storage, tmpPath, parquetPath)

    // 9. Cleanup .compacting file
    await cleanup(compactingPath)

    return live.length
  } catch (error) {
    // On failure, leave the .compacting file for recovery
    throw error
  }
}

// =============================================================================
// shouldCompact
// =============================================================================

/**
 * Check whether the rels.jsonl file should be compacted based on size thresholds.
 *
 * @param jsonlPath - Full path to the rels.jsonl file
 * @param thresholds - Line and byte thresholds for triggering compaction
 * @returns true if compaction should be triggered
 */
export async function shouldCompact(
  jsonlPath: string,
  thresholds: CompactThresholds = DEFAULT_THRESHOLDS,
): Promise<boolean> {
  let fileInfo
  try {
    fileInfo = await stat(jsonlPath)
  } catch {
    // File doesn't exist — nothing to compact
    return false
  }

  // Check byte threshold first (cheaper than reading lines)
  if (thresholds.byteThreshold !== undefined && fileInfo.size >= thresholds.byteThreshold) {
    return true
  }

  // Check line threshold
  const lines = await lineCount(jsonlPath)
  return lines >= thresholds.lineThreshold
}

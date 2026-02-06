/**
 * Data Table Compactor for ParqueDB's MergeTree Engine
 *
 * Compaction merges the JSONL buffer into a Parquet (or JSON) data file:
 *
 * 1. Rotate the JSONL file (table.jsonl -> table.jsonl.compacting)
 * 2. Read existing table.parquet (if any)
 * 3. Read table.jsonl.compacting
 * 4. Merge: deduplicate by $id, latest $v wins, tombstones remove entities
 * 5. Write new table.parquet.tmp sorted by $id
 * 6. Atomic swap: rename .tmp -> .parquet
 * 7. Delete .jsonl.compacting
 *
 * The storage format is abstracted behind a StorageAdapter interface so
 * tests can use simple JSON files while production uses Parquet.
 */

import { rename, stat } from 'node:fs/promises'
import { rotate, cleanup } from './rotation'
import { replay } from './jsonl-reader'
import { lineCount } from './jsonl-reader'
import { mergeResults } from './merge'
import type { DataLine } from './types'

// =============================================================================
// Types
// =============================================================================

export interface CompactOptions {
  /** Compact when JSONL file exceeds this many bytes */
  maxBytes?: number
  /** Compact when JSONL file exceeds this many lines */
  maxLines?: number
}

export interface StorageAdapter {
  /** Read entities from a data file (Parquet or JSON) */
  readData(path: string): Promise<DataLine[]>
  /** Write entities to a data file (Parquet or JSON) */
  writeData(path: string, data: DataLine[]): Promise<void>
}

// =============================================================================
// shouldCompact
// =============================================================================

/**
 * Check if a table's JSONL file exceeds compaction thresholds.
 *
 * Returns true if ANY of the specified thresholds are exceeded.
 * Returns false if the file does not exist or no thresholds are specified.
 */
export async function shouldCompact(
  jsonlPath: string,
  options: CompactOptions,
): Promise<boolean> {
  const { maxBytes, maxLines } = options

  // If no thresholds specified, never compact
  if (maxBytes === undefined && maxLines === undefined) {
    return false
  }

  // Check byte threshold
  if (maxBytes !== undefined) {
    try {
      const fileStat = await stat(jsonlPath)
      if (fileStat.size > maxBytes) {
        return true
      }
    } catch {
      // File does not exist or is inaccessible
      return false
    }
  }

  // Check line count threshold
  if (maxLines !== undefined) {
    const count = await lineCount(jsonlPath)
    if (count > maxLines) {
      return true
    }
  }

  return false
}

// =============================================================================
// compactDataTable
// =============================================================================

/**
 * Compact a data table: merge JSONL buffer into the data file.
 *
 * @param dataDir - Directory containing the table files
 * @param table - Table name (e.g. 'users')
 * @param storage - StorageAdapter for reading/writing data files
 * @returns The number of entities in the compacted output, or null if skipped
 */
export async function compactDataTable(
  dataDir: string,
  table: string,
  storage: StorageAdapter,
): Promise<number | null> {
  const jsonlPath = `${dataDir}/${table}.jsonl`
  const dataPath = `${dataDir}/${table}.parquet`

  // Step 1: Rotate the JSONL file
  const compactingPath = await rotate(jsonlPath)

  // If rotation returned null, there is nothing to compact
  if (compactingPath === null) {
    return null
  }

  // Step 2: Read existing data file (may be empty if first compaction)
  const existing = await storage.readData(dataPath)

  // Step 3: Read the rotated JSONL file
  const jsonlData = await replay<DataLine>(compactingPath)

  // Step 4: Merge using ReplacingMergeTree semantics
  const merged = mergeResults(existing, jsonlData)

  // Step 5: Write to a temporary file
  const tmpPath = dataPath + '.tmp'
  await storage.writeData(tmpPath, merged)

  // Step 6: Atomic rename: .tmp -> .parquet
  await rename(tmpPath, dataPath)

  // Step 7: Cleanup the compacting file
  await cleanup(compactingPath)

  // Step 8: Return the count of entities in the compacted output
  return merged.length
}

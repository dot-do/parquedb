import type { DataLine } from './types'

/**
 * Merge buffer results with Parquet results using ReplacingMergeTree semantics.
 *
 * For each $id, the entry with the highest $v wins.
 * Ties go to the buffer (assumed more recent).
 * Tombstones ($op='d') are excluded from final results.
 *
 * @param parquetResults - Entities from compacted Parquet file
 * @param bufferResults - Entities from in-memory buffer (including tombstones)
 * @returns Merged, deduplicated entities sorted by $id
 */
export function mergeResults(
  parquetResults: DataLine[],
  bufferResults: DataLine[]
): DataLine[] {
  // 1. Build a Map<$id, DataLine> from parquetResults
  const merged = new Map<string, DataLine>()
  for (const entry of parquetResults) {
    merged.set(entry.$id, entry)
  }

  // 2. Overlay bufferResults: for each buffer entry, if its $v >= existing $v
  //    (or no existing entry), replace. This ensures buffer wins on ties.
  for (const entry of bufferResults) {
    const existing = merged.get(entry.$id)
    if (!existing || entry.$v >= existing.$v) {
      merged.set(entry.$id, entry)
    }
  }

  // 3. Filter out tombstones ($op === 'd')
  // 4. Sort by $id for deterministic output
  const results: DataLine[] = []
  for (const entry of merged.values()) {
    if (entry.$op !== 'd') {
      results.push(entry)
    }
  }

  results.sort((a, b) => (a.$id < b.$id ? -1 : a.$id > b.$id ? 1 : 0))

  return results
}

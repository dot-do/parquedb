/**
 * JSONL File Rotation for MergeTree Compaction
 *
 * Provides atomic file rotation so new writes go to a fresh JSONL file
 * while compaction processes the old one. The sequence:
 *   1. Rename table.jsonl -> table.jsonl.compacting
 *   2. Create fresh empty table.jsonl (new writes go here immediately)
 *   3. Compaction reads from table.jsonl.compacting
 *   4. After successful compaction, delete table.jsonl.compacting
 */

import { rename, writeFile, unlink, access } from 'node:fs/promises'

const COMPACTING_SUFFIX = '.compacting'

/**
 * Get the .compacting path for a base JSONL path.
 */
export function getCompactingPath(basePath: string): string {
  return basePath + COMPACTING_SUFFIX
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
 * Atomically rotate a JSONL file for compaction.
 *
 * Renames file.jsonl -> file.jsonl.compacting, then creates a fresh empty file.jsonl.
 * Returns the path to the .compacting file, or null if rotation was skipped.
 *
 * Rotation is skipped when:
 * - The base file does not exist (nothing to rotate)
 * - A .compacting file already exists (compaction already in progress)
 */
export async function rotate(basePath: string): Promise<string | null> {
  const compactingPath = getCompactingPath(basePath)

  // If base file doesn't exist, nothing to rotate
  if (!(await fileExists(basePath))) {
    return null
  }

  // If .compacting already exists, a compaction is in progress — skip
  if (await fileExists(compactingPath)) {
    return null
  }

  // Rename base -> compacting
  await rename(basePath, compactingPath)

  // Create fresh empty base file for new writes
  await writeFile(basePath, '', 'utf-8')

  return compactingPath
}

/**
 * Clean up the .compacting file after successful compaction.
 * No-op if the file does not exist.
 */
export async function cleanup(compactingPath: string): Promise<void> {
  try {
    await unlink(compactingPath)
  } catch (err: unknown) {
    // If file doesn't exist, that's fine — treat as no-op
    if (err instanceof Error && 'code' in err && (err as NodeJS.ErrnoException).code === 'ENOENT') {
      return
    }
    throw err
  }
}

/**
 * Check if a .compacting file exists (indicating an interrupted compaction).
 */
export async function needsRecovery(basePath: string): Promise<boolean> {
  return fileExists(getCompactingPath(basePath))
}

/**
 * JSONL Reader for MergeTree Engine
 *
 * Stateless functions to replay lines from .jsonl files.
 * Used on startup to rebuild the in-memory buffer, and during
 * compaction to read the rotated file.
 *
 * Design decisions:
 * - Uses readFile (not streaming) since files should be small (compacted regularly)
 * - Missing files return empty results (no errors) — expected on first startup
 * - Corrupted lines are skipped with a console.warn — partial recovery preferred
 * - All functions are exported as standalone (not a class) — stateless operations
 */

import { readFile } from 'node:fs/promises'

/**
 * Split file content into non-empty lines.
 */
function splitLines(content: string): string[] {
  return content.split('\n').filter(line => line.trim() !== '')
}

/**
 * Safely parse a JSON line. Returns the parsed object on success,
 * or undefined on failure (with a console.warn).
 */
function parseLine<T>(line: string, lineNumber: number, path: string): T | undefined {
  try {
    return JSON.parse(line) as T
  } catch {
    console.warn(`[jsonl-reader] Skipping corrupted line ${lineNumber} in ${path}: ${line.slice(0, 100)}`)
    return undefined
  }
}

/**
 * Read file content as string, returning empty string for missing files (ENOENT).
 */
async function readFileContent(path: string): Promise<string> {
  try {
    return await readFile(path, 'utf-8')
  } catch (error: unknown) {
    if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
      return ''
    }
    throw error
  }
}

/**
 * Read all lines from a JSONL file and return as parsed objects.
 */
export async function replay<T = Record<string, unknown>>(path: string): Promise<T[]> {
  const content = await readFileContent(path)
  if (!content) return []

  const lines = splitLines(content)
  const results: T[] = []

  for (let i = 0; i < lines.length; i++) {
    const parsed = parseLine<T>(lines[i], i + 1, path)
    if (parsed !== undefined) {
      results.push(parsed)
    }
  }

  return results
}

/**
 * Read lines from JSONL file, calling callback for each one.
 * More memory-efficient than replay() for large files.
 *
 * @returns The number of successfully parsed lines.
 */
export async function replayInto<T = Record<string, unknown>>(
  path: string,
  callback: (line: T) => void,
): Promise<number> {
  const content = await readFileContent(path)
  if (!content) return 0

  const lines = splitLines(content)
  let count = 0

  for (let i = 0; i < lines.length; i++) {
    const parsed = parseLine<T>(lines[i], i + 1, path)
    if (parsed !== undefined) {
      callback(parsed)
      count++
    }
  }

  return count
}

/**
 * Read lines from JSONL file filtered by timestamp range.
 * Checks both `$ts` (DataLine) and `ts` (EventLine) fields.
 *
 * Filtering logic:
 * - If the line has a `$ts` field, include it only if `fromTs <= $ts <= toTs`
 * - Else if the line has a `ts` field, include it only if `fromTs <= ts <= toTs`
 * - If neither field exists, include the line (no filtering applied)
 *
 * @param path - Path to the JSONL file
 * @param fromTs - Inclusive lower bound of the timestamp range
 * @param toTs - Inclusive upper bound of the timestamp range
 * @returns Filtered array of parsed objects
 */
export async function replayRange<T = Record<string, unknown>>(
  path: string,
  fromTs: number,
  toTs: number,
): Promise<T[]> {
  const all = await replay<T>(path)

  return all.filter((item) => {
    const record = item as Record<string, unknown>
    const dataTs = record['$ts']
    const eventTs = record['ts']

    if (typeof dataTs === 'number') {
      return dataTs >= fromTs && dataTs <= toTs
    }
    if (typeof eventTs === 'number') {
      return eventTs >= fromTs && eventTs <= toTs
    }

    // No timestamp field — include by default
    return true
  })
}

/**
 * Count lines in a JSONL file without full JSON parsing.
 * Returns 0 for missing or empty files.
 */
export async function lineCount(path: string): Promise<number> {
  const content = await readFileContent(path)
  if (!content) return 0

  return splitLines(content).length
}

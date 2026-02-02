/**
 * Export Command
 *
 * Export data from the ParqueDB database to a file.
 *
 * Supported formats:
 *   - JSON: Array of objects
 *   - NDJSON: Newline-delimited JSON
 *   - CSV: Comma-separated values
 *
 * Usage:
 *   parquedb export <namespace> <file>
 */

import { join, extname } from 'node:path'
import { promises as fs } from 'node:fs'
import { createWriteStream } from 'node:fs'
import type { ParsedArgs } from '../index'
import { print, printError, printSuccess } from '../index'
import { ParqueDB } from '../../ParqueDB'
import { FsBackend } from '../../storage/FsBackend'
import type { Filter, FindOptions, Entity } from '../../types'

// =============================================================================
// Constants
// =============================================================================

import { MAX_BATCH_SIZE } from '../../constants'

const CONFIG_FILENAME = 'parquedb.json'
const BATCH_SIZE = MAX_BATCH_SIZE // Fetch in batches for large exports

// =============================================================================
// Export Command
// =============================================================================

/**
 * Export data to a file
 */
export async function exportCommand(parsed: ParsedArgs): Promise<number> {
  // Validate arguments
  if (parsed.args.length < 2) {
    printError('Missing arguments')
    print('Usage: parquedb export <namespace> <file>')
    return 1
  }

  const namespace = parsed.args[0]!
  const filePath = parsed.args[1]!
  const directory = parsed.options.directory

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    // Intentionally ignored: fs.access throws when config doesn't exist, meaning DB is not initialized
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  // Determine format from extension or option
  let format = parsed.options.format
  const ext = extname(filePath).toLowerCase()
  if (ext === '.ndjson' || ext === '.jsonl') {
    format = 'ndjson'
  } else if (ext === '.csv') {
    format = 'csv'
  }

  try {
    // Create storage backend and database
    const storage = new FsBackend(directory)
    const db = new ParqueDB({ storage })

    // Build find options
    const options: FindOptions = {
      limit: BATCH_SIZE,
    }

    // For CSV, we need all data to determine columns
    // For JSON/NDJSON, we can stream
    let exported = 0

    switch (format) {
      case 'json': {
        const data = await fetchAllData(db, namespace, options, parsed.options.quiet)
        await writeJsonFile(filePath, data, parsed.options.pretty)
        exported = data.length
        break
      }

      case 'ndjson': {
        exported = await exportNdjson(db, namespace, filePath, options, parsed.options.quiet)
        break
      }

      case 'csv': {
        const data = await fetchAllData(db, namespace, options, parsed.options.quiet)
        await writeCsvFile(filePath, data)
        exported = data.length
        break
      }
    }

    // Print summary
    if (!parsed.options.quiet) {
      printSuccess(`Exported ${exported} entities to ${filePath}`)
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Export failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Data Fetching
// =============================================================================

/**
 * Fetch all data from a namespace (with pagination)
 */
async function fetchAllData(
  db: ParqueDB,
  namespace: string,
  options: FindOptions,
  quiet: boolean
): Promise<Entity[]> {
  const allData: Entity[] = []
  let cursor: string | undefined
  let batch = 0

  do {
    const result = await db.find(namespace, undefined, {
      ...options,
      cursor,
    })

    allData.push(...result.items)
    cursor = result.nextCursor
    batch++

    // Progress indicator
    if (!quiet && batch > 1) {
      process.stdout.write(`\rFetched ${allData.length} entities...`)
    }
  } while (cursor)

  // Clear progress line
  if (!quiet && batch > 1) {
    process.stdout.write('\r' + ' '.repeat(40) + '\r')
  }

  return allData
}

// =============================================================================
// File Writers
// =============================================================================

/**
 * Write data to a JSON file
 */
async function writeJsonFile(
  filePath: string,
  data: Entity[],
  pretty: boolean
): Promise<void> {
  const content = pretty
    ? JSON.stringify(data, dateReplacer, 2)
    : JSON.stringify(data, dateReplacer)
  await fs.writeFile(filePath, content + '\n')
}

/**
 * Export to NDJSON file (streaming)
 */
async function exportNdjson(
  db: ParqueDB,
  namespace: string,
  filePath: string,
  options: FindOptions,
  quiet: boolean
): Promise<number> {
  const writeStream = createWriteStream(filePath)
  let cursor: string | undefined
  let exported = 0
  let batch = 0

  try {
    do {
      const result = await db.find(namespace, undefined, {
        ...options,
        cursor,
      })

      for (const item of result.items) {
        writeStream.write(JSON.stringify(item, dateReplacer) + '\n')
        exported++
      }

      cursor = result.nextCursor
      batch++

      // Progress indicator
      if (!quiet && batch > 1) {
        process.stdout.write(`\rExported ${exported} entities...`)
      }
    } while (cursor)

    // Clear progress line
    if (!quiet && batch > 1) {
      process.stdout.write('\r' + ' '.repeat(40) + '\r')
    }
  } finally {
    writeStream.end()
    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', resolve)
      writeStream.on('error', reject)
    })
  }

  return exported
}

/**
 * Write data to a CSV file
 */
async function writeCsvFile(filePath: string, data: Entity[]): Promise<void> {
  if (data.length === 0) {
    await fs.writeFile(filePath, '')
    return
  }

  // Collect all unique keys from all objects
  const keys = new Set<string>()
  for (const item of data) {
    for (const key of Object.keys(item)) {
      keys.add(key)
    }
  }
  const headers = Array.from(keys)

  // Build CSV content
  const lines: string[] = []

  // Header row
  lines.push(headers.map(escapeCsvValue).join(','))

  // Data rows
  for (const item of data) {
    const row = headers.map(key => {
      const value = (item as Record<string, unknown>)[key]
      return escapeCsvValue(formatCsvValue(value))
    })
    lines.push(row.join(','))
  }

  await fs.writeFile(filePath, lines.join('\n') + '\n')
}

/**
 * JSON replacer that converts Date objects to ISO strings
 */
function dateReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Date) {
    return value.toISOString()
  }
  return value
}

/**
 * Format a value for CSV output
 */
function formatCsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

/**
 * Escape a value for CSV (quote if contains comma, quote, or newline)
 */
function escapeCsvValue(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return '"' + value.replace(/"/g, '""') + '"'
  }
  return value
}

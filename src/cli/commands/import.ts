/**
 * Import Command
 *
 * Import data from a file into the ParqueDB database.
 *
 * Supported formats:
 *   - JSON: Array of objects
 *   - NDJSON: Newline-delimited JSON
 *   - CSV: Comma-separated values (with header row)
 *
 * Usage:
 *   parquedb import <namespace> <file>
 */

import { join, extname } from 'node:path'
import { promises as fs } from 'node:fs'
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import type { ParsedArgs } from '../index'
import { print, printError, printSuccess } from '../index'
import { ParqueDB } from '../../ParqueDB'
import { FsBackend } from '../../storage/FsBackend'
import type { CreateInput } from '../../types'

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'
const BATCH_SIZE = 1000 // Process in batches for large files

// =============================================================================
// Import Command
// =============================================================================

/**
 * Import data from a file
 */
export async function importCommand(parsed: ParsedArgs): Promise<number> {
  // Validate arguments
  if (parsed.args.length < 2) {
    printError('Missing arguments')
    print('Usage: parquedb import <namespace> <file>')
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
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  // Check if file exists
  try {
    await fs.access(filePath)
  } catch {
    printError(`File not found: ${filePath}`)
    return 1
  }

  // Determine format from extension or option
  let format = parsed.options.format
  if (format === 'json') {
    // Check if it might be NDJSON based on extension
    const ext = extname(filePath).toLowerCase()
    if (ext === '.ndjson' || ext === '.jsonl') {
      format = 'ndjson'
    } else if (ext === '.csv') {
      format = 'csv'
    }
  }

  try {
    // Create storage backend and database
    const storage = new FsBackend(directory)
    const db = new ParqueDB({ storage })

    let imported = 0
    let errors = 0

    // Read and parse file based on format
    switch (format) {
      case 'json': {
        const data = await readJsonFile(filePath)
        const result = await importBatch(db, namespace, data)
        imported = result.imported
        errors = result.errors
        break
      }

      case 'ndjson': {
        const result = await importNdjson(db, namespace, filePath, parsed.options.quiet)
        imported = result.imported
        errors = result.errors
        break
      }

      case 'csv': {
        const data = await readCsvFile(filePath)
        const result = await importBatch(db, namespace, data)
        imported = result.imported
        errors = result.errors
        break
      }
    }

    // Print summary
    if (!parsed.options.quiet) {
      printSuccess(`Imported ${imported} entities into ${namespace}`)
      if (errors > 0) {
        print(`  (${errors} errors)`)
      }
    }

    return errors > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Import failed: ${message}`)
    return 1
  }
}

// =============================================================================
// File Readers
// =============================================================================

/**
 * Read a JSON file (array of objects)
 */
async function readJsonFile(filePath: string): Promise<Record<string, unknown>[]> {
  const content = await fs.readFile(filePath, 'utf-8')
  const data = JSON.parse(content) as unknown

  if (!Array.isArray(data)) {
    throw new Error('JSON file must contain an array of objects')
  }

  return data as Record<string, unknown>[]
}

/**
 * Read a CSV file
 */
async function readCsvFile(filePath: string): Promise<Record<string, unknown>[]> {
  const content = await fs.readFile(filePath, 'utf-8')
  const lines = content.split('\n').filter(line => line.trim())

  if (lines.length === 0) {
    return []
  }

  // Parse header row
  const headers = parseCsvLine(lines[0]!)

  // Parse data rows
  const data: Record<string, unknown>[] = []
  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]!)
    const obj: Record<string, unknown> = {}
    for (let j = 0; j < headers.length; j++) {
      const header = headers[j]
      const value = values[j]
      if (header !== undefined) {
        obj[header] = parseValue(value ?? '')
      }
    }
    data.push(obj)
  }

  return data
}

/**
 * Parse a CSV line into values
 */
function parseCsvLine(line: string): string[] {
  const values: string[] = []
  let current = ''
  let inQuotes = false

  for (let i = 0; i < line.length; i++) {
    const char = line[i]!

    if (inQuotes) {
      if (char === '"') {
        if (line[i + 1] === '"') {
          // Escaped quote
          current += '"'
          i++
        } else {
          // End of quoted field
          inQuotes = false
        }
      } else {
        current += char
      }
    } else {
      if (char === '"') {
        inQuotes = true
      } else if (char === ',') {
        values.push(current)
        current = ''
      } else {
        current += char
      }
    }
  }

  values.push(current)
  return values
}

/**
 * Parse a string value into the appropriate type
 */
function parseValue(value: string): unknown {
  // Empty string
  if (value === '') {
    return null
  }

  // Boolean
  if (value.toLowerCase() === 'true') return true
  if (value.toLowerCase() === 'false') return false

  // Number
  if (/^-?\d+(\.\d+)?$/.test(value)) {
    const num = parseFloat(value)
    if (!isNaN(num)) return num
  }

  // ISO date
  if (/^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?)?$/.test(value)) {
    const date = new Date(value)
    if (!isNaN(date.getTime())) return date
  }

  // JSON object or array
  if ((value.startsWith('{') && value.endsWith('}')) ||
      (value.startsWith('[') && value.endsWith(']'))) {
    try {
      return JSON.parse(value)
    } catch {
      // Not valid JSON, return as string
    }
  }

  return value
}

// =============================================================================
// Import Functions
// =============================================================================

/**
 * Import a batch of entities
 */
async function importBatch(
  db: ParqueDB,
  namespace: string,
  data: Record<string, unknown>[]
): Promise<{ imported: number; errors: number }> {
  let imported = 0
  let errors = 0

  for (const item of data) {
    try {
      await db.create(namespace, item as CreateInput)
      imported++
    } catch (error) {
      errors++
      // Log error but continue with other items
      console.error(`Error importing item: ${error instanceof Error ? error.message : error}`)
    }
  }

  return { imported, errors }
}

/**
 * Import from NDJSON file (streaming)
 */
async function importNdjson(
  db: ParqueDB,
  namespace: string,
  filePath: string,
  quiet: boolean
): Promise<{ imported: number; errors: number }> {
  let imported = 0
  let errors = 0
  let batch: Record<string, unknown>[] = []

  const fileStream = createReadStream(filePath)
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  for await (const line of rl) {
    if (!line.trim()) continue

    try {
      const item = JSON.parse(line) as Record<string, unknown>
      batch.push(item)

      // Process batch when it reaches BATCH_SIZE
      if (batch.length >= BATCH_SIZE) {
        const result = await importBatch(db, namespace, batch)
        imported += result.imported
        errors += result.errors
        batch = []

        // Progress indicator
        if (!quiet) {
          process.stdout.write(`\rImported ${imported} entities...`)
        }
      }
    } catch {
      errors++
    }
  }

  // Process remaining items
  if (batch.length > 0) {
    const result = await importBatch(db, namespace, batch)
    imported += result.imported
    errors += result.errors
  }

  // Clear progress line
  if (!quiet) {
    process.stdout.write('\r' + ' '.repeat(40) + '\r')
  }

  return { imported, errors }
}

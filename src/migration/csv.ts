/**
 * CSV import utilities for ParqueDB
 *
 * Supports importing from CSV files with:
 * - Header inference
 * - Type inference
 * - Custom delimiters
 * - Streaming large files
 * - Async iterators for memory-efficient processing
 */

import type { ParqueDB } from '../ParqueDB'
import type { CreateInput, EntityId } from '../types/entity'
import type { CsvImportOptions, MigrationResult, MigrationError, StreamingDocument, CsvStreamingOptions } from './types'
import { fileExists, parseCsvLine, inferType } from './utils'
import { MAX_BATCH_SIZE } from '../constants'
import { extractPositionFromSyntaxError } from './errors'

/**
 * Result of converting a value with potential JSON error
 */
interface ConversionResult {
  value: unknown
  error?: { column: string; message: string } | undefined
}

/**
 * Default CSV import options
 */
const DEFAULT_OPTIONS: Required<Omit<CsvImportOptions, 'onProgress' | 'transform' | 'entityType' | 'columnTypes' | 'nameColumn' | 'headers'>> = {
  batchSize: MAX_BATCH_SIZE,
  skipValidation: false,
  actor: 'system/migration',
  delimiter: ',',
  skipEmptyLines: true,
  inferTypes: true,
  streaming: false,
}

/**
 * Import documents from a CSV file
 *
 * @param db - ParqueDB instance
 * @param ns - Namespace to import into
 * @param path - Path to CSV file
 * @param options - Import options
 * @returns Migration result
 *
 * @example
 * // Import with default options (auto-detect headers)
 * const result = await importFromCsv(db, 'products', './products.csv')
 *
 * @example
 * // Import with custom options
 * const result = await importFromCsv(db, 'transactions', './data.csv', {
 *   delimiter: ';',
 *   columnTypes: {
 *     amount: 'number',
 *     date: 'date',
 *     active: 'boolean',
 *   },
 *   nameColumn: 'transaction_id',
 *   transform: (doc) => ({
 *     ...doc,
 *     $type: 'Transaction',
 *   }),
 * })
 */
export async function importFromCsv(
  db: ParqueDB,
  ns: string,
  path: string,
  options?: CsvImportOptions
): Promise<MigrationResult> {
  const startTime = Date.now()
  const opts = { ...DEFAULT_OPTIONS, ...options, headers: options?.headers ?? true }
  const errors: MigrationError[] = []
  let imported = 0
  let skipped = 0
  let failed = 0
  let lineNumber = 0

  // Check if file exists
  if (!await fileExists(path)) {
    throw new Error(`File not found: ${path}`)
  }

  // Get collection
  const collection = db.collection(ns)

  // Determine headers
  let headers: string[] = []
  let isFirstDataLine = true

  // Process lines using async iteration
  const batch: CreateInput<Record<string, unknown>>[] = []

  for await (const line of readLines(path)) {
    lineNumber++

    // Skip empty lines
    if (opts.skipEmptyLines && !line.trim()) {
      continue
    }

    // Parse CSV line
    const fields = parseCsvLine(line, opts.delimiter)

    // Handle headers
    if (isFirstDataLine) {
      isFirstDataLine = false

      if (opts.headers === true) {
        // Use first row as headers
        headers = fields.map(h => normalizeHeader(h))
        continue
      } else if (Array.isArray(opts.headers)) {
        // Use provided headers
        headers = opts.headers
      } else {
        // No headers - generate column names
        headers = fields.map((_, i) => `column${i + 1}`)
      }
    }

    // Skip if we're still reading headers
    if (headers.length === 0) {
      continue
    }

    // Convert row to document
    let doc: Record<string, unknown> = {}
    let jsonColumnError: { column: string; message: string } | undefined

    for (let i = 0; i < fields.length; i++) {
      const header = headers[i] || `column${i + 1}`
      let value: unknown = fields[i]

      // Apply type conversion
      if (opts.columnTypes && header in opts.columnTypes) {
        const result = convertToType(fields[i] || '', opts.columnTypes[header]!, header)
        value = result.value
        if (result.error && !jsonColumnError) {
          jsonColumnError = result.error
        }
      } else if (opts.inferTypes) {
        value = inferType(fields[i] || '')
      }

      doc[header] = value
    }

    // Check for JSON column parse errors
    if (jsonColumnError) {
      errors.push({
        index: lineNumber,
        message: `${jsonColumnError.message} at line ${lineNumber}`,
        document: doc,
      })
      failed++
      continue
    }

    // Apply transform
    if (opts.transform) {
      try {
        doc = opts.transform(doc) as Record<string, unknown>
      } catch (err) {
        errors.push({
          index: lineNumber,
          message: `Transform failed at line ${lineNumber}: ${(err as Error).message}`,
          document: doc,
        })
        failed++
        continue
      }
    }

    // Skip null/undefined documents
    if (doc == null) {
      skipped++
      continue
    }

    // Prepare document for import
    const createInput = prepareDocument(doc, ns, opts.entityType, opts.nameColumn, headers)
    batch.push(createInput)

    // Process batch when full
    if (batch.length >= (opts.batchSize ?? MAX_BATCH_SIZE)) {
      const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, lineNumber - batch.length)
      imported += batchResult.imported
      failed += batchResult.failed
      batch.length = 0

      // Report progress
      if (opts.onProgress) {
        opts.onProgress(imported + skipped + failed)
      }
    }
  }

  // Process remaining batch
  if (batch.length > 0) {
    const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, lineNumber - batch.length)
    imported += batchResult.imported
    failed += batchResult.failed

    // Report final progress
    if (opts.onProgress) {
      opts.onProgress(imported + skipped + failed)
    }
  }

  return {
    imported,
    skipped,
    failed,
    errors,
    duration: Date.now() - startTime,
  }
}

/**
 * Normalize a CSV header to a valid field name
 */
function normalizeHeader(header: string): string {
  return header
    .trim()
    // Remove surrounding quotes
    .replace(/^["']|["']$/g, '')
    // Replace spaces and special chars with underscores
    .replace(/[^a-zA-Z0-9_]/g, '_')
    // Remove leading/trailing underscores
    .replace(/^_+|_+$/g, '')
    // Convert to camelCase (optional)
    .replace(/_([a-z])/g, (_, letter) => letter.toUpperCase())
    // Ensure it starts with a letter
    .replace(/^(\d)/, '_$1')
    || 'column'
}

/**
 * Convert a string value to a specific type
 * For JSON type, returns error info instead of silently returning null
 */
function convertToType(
  value: string,
  type: 'string' | 'number' | 'boolean' | 'date' | 'json',
  column: string
): ConversionResult {
  const trimmed = value.trim()

  if (trimmed === '' || trimmed.toLowerCase() === 'null') {
    return { value: null }
  }

  switch (type) {
    case 'string':
      return { value: trimmed }

    case 'number':
      const num = parseFloat(trimmed)
      return { value: isNaN(num) ? null : num }

    case 'boolean':
      const lower = trimmed.toLowerCase()
      if (lower === 'true' || lower === '1' || lower === 'yes') return { value: true }
      if (lower === 'false' || lower === '0' || lower === 'no') return { value: false }
      return { value: null }

    case 'date':
      const date = new Date(trimmed)
      return { value: isNaN(date.getTime()) ? null : date }

    case 'json':
      try {
        return { value: JSON.parse(trimmed) }
      } catch (err) {
        // Return error context for JSON column parse failures
        if (err instanceof SyntaxError) {
          const position = extractPositionFromSyntaxError(err)
          let message = `JSON parse error in column '${column}'`
          if (position !== undefined) {
            message += ` (position ${position})`
          }
          message += `: ${err.message}`
          return { value: null, error: { column, message } }
        }
        return { value: null, error: { column, message: `JSON parse error in column '${column}': ${(err as Error).message}` } }
      }

    default:
      return { value: trimmed }
  }
}

/**
 * Prepare a document for import by adding required fields
 */
function prepareDocument(
  doc: Record<string, unknown>,
  ns: string,
  entityType?: string,
  nameColumn?: string,
  headers?: string[]
): CreateInput<Record<string, unknown>> {
  // Determine $type
  const $type = doc.$type as string
    || entityType
    || capitalizeNamespace(ns)

  // Determine name
  let name: string

  if (nameColumn && doc[nameColumn]) {
    name = String(doc[nameColumn])
  } else if (doc.name) {
    name = String(doc.name)
  } else if (doc.title) {
    name = String(doc.title)
  } else if (headers && headers.length > 0 && doc[headers[0]!]) {
    // Use first column as name
    name = String(doc[headers[0]!])
  } else {
    // Generate a name
    name = `${$type}-${Date.now()}`
  }

  return {
    ...doc,
    $type,
    name,
  }
}

/**
 * Process a batch of documents
 */
async function processBatch(
  collection: ReturnType<ParqueDB['collection']>,
  batch: CreateInput<Record<string, unknown>>[],
  opts: { skipValidation: boolean; actor: string },
  errors: MigrationError[],
  startIndex: number
): Promise<{ imported: number; failed: number }> {
  let imported = 0
  let failed = 0

  for (let i = 0; i < batch.length; i++) {
    const doc = batch[i]!
    try {
      await collection.create(doc, {
        skipValidation: opts.skipValidation,
        actor: opts.actor as EntityId,
      })
      imported++
    } catch (err) {
      errors.push({
        index: startIndex + i,
        message: (err as Error).message,
        document: doc,
      })
      failed++
    }
  }

  return { imported, failed }
}

/**
 * Capitalize a namespace for use as entity type
 */
function capitalizeNamespace(ns: string): string {
  return ns
    .split(/[-_]/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join('')
    .replace(/s$/, '')
}

/**
 * Read lines from a file using async iteration
 */
async function* readLines(path: string): AsyncGenerator<string> {
  const fs = await import('fs')
  const readline = await import('readline')

  const fileStream = fs.createReadStream(path, { encoding: 'utf-8' })
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  for await (const line of rl) {
    yield line
  }
}

/**
 * Stream documents from a CSV file
 *
 * Returns an async iterator that yields documents one at a time,
 * enabling memory-efficient processing of large CSV files.
 *
 * @param path - Path to CSV file
 * @param options - CSV streaming options
 * @returns Async iterator yielding StreamingDocument objects
 *
 * @example
 * // Process CSV rows one at a time
 * for await (const { document, lineNumber, error } of streamFromCsv('./data.csv')) {
 *   if (error) {
 *     console.error(`Error at line ${lineNumber}: ${error}`)
 *     continue
 *   }
 *   await processRow(document)
 * }
 *
 * @example
 * // With custom options
 * const stream = streamFromCsv('./data.csv', {
 *   delimiter: ';',
 *   headers: ['id', 'name', 'value'],
 *   columnTypes: { id: 'number', value: 'number' },
 *   transform: (doc) => ({ ...doc, processed: true }),
 * })
 * for await (const { document } of stream) {
 *   console.log(document)
 * }
 */
export async function* streamFromCsv(
  path: string,
  options?: CsvStreamingOptions
): AsyncGenerator<StreamingDocument> {
  const opts = {
    skipErrors: false,
    delimiter: ',',
    skipEmptyLines: true,
    inferTypes: true,
    headers: true as boolean | string[],
    ...options,
  }

  // Check if file exists
  if (!await fileExists(path)) {
    throw new Error(`File not found: ${path}`)
  }

  // Determine headers
  let headers: string[] = []
  let isFirstDataLine = true
  // Track file line number (1-indexed, including header)
  let fileLineNumber = 0

  for await (const line of readLines(path)) {
    fileLineNumber++

    // Skip empty lines
    if (opts.skipEmptyLines && !line.trim()) {
      continue
    }

    // Parse CSV line
    const fields = parseCsvLine(line, opts.delimiter)

    // Handle headers
    if (isFirstDataLine) {
      isFirstDataLine = false

      if (opts.headers === true) {
        // Use first row as headers
        headers = fields.map(h => normalizeHeader(h))
        continue
      } else if (Array.isArray(opts.headers)) {
        // Use provided headers
        headers = opts.headers
      } else {
        // No headers - generate column names
        headers = fields.map((_, i) => `column${i + 1}`)
      }
    }

    // Skip if we're still reading headers
    if (headers.length === 0) {
      continue
    }

    // Current line number for reporting (file line number)
    const lineNumber = fileLineNumber

    // Convert row to document
    let doc: Record<string, unknown> = {}
    let jsonColumnError: { column: string; message: string } | undefined

    for (let i = 0; i < fields.length; i++) {
      const header = headers[i] || `column${i + 1}`
      let value: unknown = fields[i]

      // Apply type conversion
      if (opts.columnTypes && header in opts.columnTypes) {
        const result = convertToType(fields[i] || '', opts.columnTypes[header]!, header)
        value = result.value
        if (result.error && !jsonColumnError) {
          jsonColumnError = result.error
        }
      } else if (opts.inferTypes) {
        value = inferType(fields[i] || '')
      }

      doc[header] = value
    }

    // Check for JSON column parse errors
    if (jsonColumnError) {
      if (opts.skipErrors) {
        continue
      }
      yield {
        document: null,
        lineNumber,
        error: jsonColumnError.message,
      }
      continue
    }

    // Apply transform
    if (opts.transform) {
      try {
        doc = opts.transform(doc) as Record<string, unknown>
      } catch (err) {
        if (opts.skipErrors) {
          continue
        }
        yield {
          document: null,
          lineNumber,
          error: `Transform failed: ${(err as Error).message}`,
        }
        continue
      }
    }

    // Skip null/undefined documents
    if (doc == null) {
      continue
    }

    yield {
      document: doc,
      lineNumber,
    }
  }
}

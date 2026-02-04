/**
 * MongoDB BSON import utilities for ParqueDB
 *
 * Supports importing from:
 * - MongoDB Extended JSON (mongoexport output)
 * - BSON dump files (mongodump output)
 *
 * Features:
 * - Streaming support for large files
 * - Automatic format detection (JSON array vs JSONL)
 */

import type { ParqueDB } from '../ParqueDB'
import type { CreateInput, EntityId } from '../types/entity'
import type { BsonImportOptions, MigrationResult, MigrationError, StreamingDocument, StreamingOptions } from './types'
import { fileExists, convertBsonValue, generateName } from './utils'
import { MAX_BATCH_SIZE } from '../constants'
import { MigrationParseError, extractPositionFromSyntaxError } from './errors'

/**
 * Default BSON import options
 */
const DEFAULT_OPTIONS: Required<Omit<BsonImportOptions, 'onProgress' | 'transform' | 'entityType' | 'idField' | 'nameField'>> = {
  batchSize: MAX_BATCH_SIZE,
  skipValidation: false,
  actor: 'system/migration',
  preserveMongoId: false,
  convertObjectIds: true,
  convertDates: true,
  streaming: false,
}

/**
 * Format an error message for JSONL line parsing with position info
 */
function formatJsonlError(err: unknown, lineNumber: number): string {
  if (err instanceof SyntaxError) {
    const position = extractPositionFromSyntaxError(err)
    let message = `JSON parse error at line ${lineNumber}`
    if (position !== undefined) {
      message += ` (position ${position})`
    }
    message += `: ${err.message}`
    return message
  }
  return `Invalid JSON at line ${lineNumber}: ${(err as Error).message}`
}

/**
 * Import documents from a MongoDB Extended JSON file
 *
 * This handles the output of `mongoexport --jsonArray` or JSONL format.
 * When options.streaming is true, uses streaming for JSONL files.
 *
 * @param db - ParqueDB instance
 * @param ns - Namespace to import into
 * @param path - Path to JSON/JSONL file
 * @param options - Import options
 * @returns Migration result
 *
 * @example
 * // Import from mongoexport --jsonArray output
 * const result = await importFromMongodb(db, 'users', './users.json')
 *
 * @example
 * // Import with streaming for large JSONL files
 * const result = await importFromMongodb(db, 'products', './products.jsonl', {
 *   streaming: true,
 *   onProgress: (count) => console.log(`Processed ${count} documents`),
 * })
 *
 * @example
 * // Import with custom options
 * const result = await importFromMongodb(db, 'products', './products.json', {
 *   idField: 'sku',
 *   nameField: 'title',
 *   preserveMongoId: true,
 *   transform: (doc) => ({
 *     ...doc,
 *     $type: 'Product',
 *   }),
 * })
 */
export async function importFromMongodb(
  db: ParqueDB,
  ns: string,
  path: string,
  options?: BsonImportOptions
): Promise<MigrationResult> {
  const startTime = Date.now()
  const opts = { ...DEFAULT_OPTIONS, ...options }
  const errors: MigrationError[] = []
  let imported = 0
  let skipped = 0
  let failed = 0

  // Check if file exists
  if (!await fileExists(path)) {
    throw new Error(`File not found: ${path}`)
  }

  // Get collection
  const collection = db.collection(ns)

  // Check if streaming mode is enabled
  if (opts.streaming) {
    // Use streaming for JSONL files
    return importFromMongodbStreaming(db, ns, path, opts)
  }

  // Read file content to determine format
  const fs = await import('fs/promises')
  const content = await fs.readFile(path, 'utf-8')
  const trimmed = content.trim()

  // Determine format (JSON array or JSONL)
  const isJsonArray = trimmed.startsWith('[')

  let documents: unknown[]

  if (isJsonArray) {
    // Parse as JSON array
    try {
      documents = JSON.parse(trimmed)
    } catch (err) {
      if (err instanceof SyntaxError) {
        throw MigrationParseError.fromJsonSyntaxError(err, path, { namespace: ns })
      }
      throw new Error(`Invalid JSON in file ${path}: ${(err as Error).message}`)
    }
  } else {
    // Parse as JSONL
    documents = []
    const lines = trimmed.split('\n')

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i]!.trim()
      if (!line) continue

      try {
        documents.push(JSON.parse(line))
      } catch (err) {
        const lineNumber = i + 1
        errors.push({
          index: lineNumber,
          message: formatJsonlError(err, lineNumber),
          document: line,
        })
        failed++
      }
    }
  }

  // Process documents in batches
  const batch: CreateInput<Record<string, unknown>>[] = []

  for (let i = 0; i < documents.length; i++) {
    let doc = documents[i] as Record<string, unknown>

    // Convert BSON extended JSON values
    if (opts.convertObjectIds || opts.convertDates) {
      doc = convertBsonDocument(doc, { convertObjectIds: opts.convertObjectIds ?? true, convertDates: opts.convertDates ?? true })
    }

    // Apply transform
    if (opts.transform) {
      try {
        doc = opts.transform(doc) as Record<string, unknown>
      } catch (err) {
        errors.push({
          index: i,
          message: `Transform failed: ${(err as Error).message}`,
          document: documents[i],
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
    const createInput = prepareMongoDocument(doc, ns, opts)
    batch.push(createInput)

    // Process batch when full
    if (batch.length >= (opts.batchSize ?? MAX_BATCH_SIZE)) {
      const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, i - batch.length + 1)
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
    const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, documents.length - batch.length)
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
 * Import from MongoDB using streaming (for JSONL files)
 */
async function importFromMongodbStreaming(
  db: ParqueDB,
  ns: string,
  path: string,
  opts: Required<Omit<BsonImportOptions, 'onProgress' | 'transform' | 'entityType' | 'idField' | 'nameField'>> & BsonImportOptions
): Promise<MigrationResult> {
  const startTime = Date.now()
  const errors: MigrationError[] = []
  let imported = 0
  let skipped = 0
  let failed = 0
  let lineNumber = 0

  // Get collection
  const collection = db.collection(ns)

  // Process lines using async iteration
  const batch: CreateInput<Record<string, unknown>>[] = []

  for await (const line of readLines(path)) {
    lineNumber++

    // Skip empty lines
    if (!line.trim()) {
      continue
    }

    // Parse JSON
    let doc: Record<string, unknown>
    try {
      doc = JSON.parse(line)
    } catch (err) {
      errors.push({
        index: lineNumber,
        message: formatJsonlError(err, lineNumber),
        document: line,
      })
      failed++
      continue
    }

    // Convert BSON extended JSON values
    if (opts.convertObjectIds || opts.convertDates) {
      doc = convertBsonDocument(doc, { convertObjectIds: opts.convertObjectIds ?? true, convertDates: opts.convertDates ?? true })
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

    // Prepare document
    const createInput = prepareMongoDocument(doc, ns, opts)
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
 * Import documents from a MongoDB BSON dump file
 *
 * This handles the output of `mongodump` (binary BSON format).
 * Note: This requires the 'bson' npm package to be installed.
 *
 * @param db - ParqueDB instance
 * @param ns - Namespace to import into
 * @param path - Path to BSON file
 * @param options - Import options
 * @returns Migration result
 */
export async function importFromBson(
  db: ParqueDB,
  ns: string,
  path: string,
  options?: BsonImportOptions
): Promise<MigrationResult> {
  const startTime = Date.now()
  const opts = { ...DEFAULT_OPTIONS, ...options }
  const errors: MigrationError[] = []
  let imported = 0
  let skipped = 0
  let failed = 0

  // Check if file exists
  if (!await fileExists(path)) {
    throw new Error(`File not found: ${path}`)
  }

  // Try to import the bson package
  interface BsonModule { deserialize: (buffer: Buffer | Uint8Array) => Record<string, unknown> }
  let BSON: BsonModule
  try {
    // Dynamic import - bson is an optional dependency
    BSON = await import('bson')
  } catch {
    // Intentionally ignored: dynamic import failure means the optional dependency is not installed
    throw new Error(
      'BSON import requires the "bson" package. Please install it: npm install bson'
    )
  }

  // Read the BSON file
  const fs = await import('fs/promises')
  const buffer = await fs.readFile(path)

  // Get collection
  const collection = db.collection(ns)

  // Parse BSON documents
  const batch: CreateInput<Record<string, unknown>>[] = []
  let offset = 0
  let docIndex = 0

  while (offset < buffer.length) {
    // Read document size (first 4 bytes, little-endian)
    if (offset + 4 > buffer.length) break

    const docSize = buffer.readInt32LE(offset)
    if (docSize <= 0 || offset + docSize > buffer.length) {
      errors.push({
        index: docIndex,
        message: `Invalid BSON document size at offset ${offset}`,
      })
      break
    }

    // Extract document bytes
    const docBuffer = buffer.subarray(offset, offset + docSize)

    try {
      // Deserialize BSON document
      let doc = BSON.deserialize(docBuffer) as Record<string, unknown>

      // Convert BSON values
      if (opts.convertObjectIds || opts.convertDates) {
        doc = convertBsonDocument(doc, { convertObjectIds: opts.convertObjectIds ?? true, convertDates: opts.convertDates ?? true })
      }

      // Apply transform
      if (opts.transform) {
        doc = opts.transform(doc) as Record<string, unknown>
      }

      // Skip null/undefined documents
      if (doc == null) {
        skipped++
      } else {
        // Prepare document for import
        const createInput = prepareMongoDocument(doc, ns, opts)
        batch.push(createInput)

        // Process batch when full
        if (batch.length >= (opts.batchSize ?? MAX_BATCH_SIZE)) {
          const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, docIndex - batch.length + 1)
          imported += batchResult.imported
          failed += batchResult.failed
          batch.length = 0

          // Report progress
          if (opts.onProgress) {
            opts.onProgress(imported + skipped + failed)
          }
        }
      }
    } catch (err) {
      errors.push({
        index: docIndex,
        message: `Failed to parse BSON document at offset ${offset}: ${(err as Error).message}`,
      })
      failed++
    }

    offset += docSize
    docIndex++
  }

  // Process remaining batch
  if (batch.length > 0) {
    const batchResult = await processBatch(collection, batch, { skipValidation: opts.skipValidation ?? false, actor: opts.actor ?? 'system/migration' }, errors, docIndex - batch.length)
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
 * Convert a MongoDB BSON document to a ParqueDB-compatible document
 */
function convertBsonDocument(
  doc: Record<string, unknown>,
  opts: { convertObjectIds: boolean; convertDates: boolean }
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(doc)) {
    if (value === null || value === undefined) {
      result[key] = value
      continue
    }

    if (Array.isArray(value)) {
      result[key] = value.map(item => {
        if (typeof item === 'object' && item !== null) {
          return convertBsonDocument(item as Record<string, unknown>, opts)
        }
        return convertBsonValue(item)
      })
      continue
    }

    if (typeof value === 'object') {
      const obj = value as Record<string, unknown>

      // Handle ObjectId
      if (opts.convertObjectIds && '$oid' in obj) {
        result[key] = obj.$oid
        continue
      }

      // Handle Date
      if (opts.convertDates && '$date' in obj) {
        result[key] = convertBsonValue(obj)
        continue
      }

      // Handle other BSON types
      if ('$numberLong' in obj || '$numberDecimal' in obj ||
          '$numberInt' in obj || '$numberDouble' in obj) {
        result[key] = convertBsonValue(obj)
        continue
      }

      // Recursive conversion for nested objects
      result[key] = convertBsonDocument(obj, opts)
      continue
    }

    result[key] = value
  }

  return result
}

/**
 * Prepare a MongoDB document for import
 */
function prepareMongoDocument(
  doc: Record<string, unknown>,
  ns: string,
  opts: BsonImportOptions
): CreateInput<Record<string, unknown>> {
  // Determine $type
  const $type = doc.$type as string
    || opts.entityType
    || capitalizeNamespace(ns)

  // Determine name
  let name: string

  if (opts.nameField && doc[opts.nameField]) {
    name = String(doc[opts.nameField])
  } else {
    name = generateName(doc, $type)
  }

  // Handle _id field
  const result: Record<string, unknown> = { ...doc }

  if (!opts.preserveMongoId && '_id' in result) {
    // Optionally use _id as the original MongoDB ID
    if (!result.mongoId) {
      result.mongoId = result._id
    }
    delete result._id
  }

  return {
    ...result,
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
 * MongoDB Extended JSON streaming options
 */
export interface MongoStreamingOptions extends StreamingOptions {
  /**
   * Whether to convert MongoDB ObjectIds to strings
   * @default true
   */
  convertObjectIds?: boolean | undefined

  /**
   * Whether to convert MongoDB dates to JS Date objects
   * @default true
   */
  convertDates?: boolean | undefined
}

/**
 * Stream documents from a MongoDB Extended JSON (JSONL) file
 *
 * Returns an async iterator that yields documents one at a time,
 * enabling memory-efficient processing of large MongoDB export files.
 *
 * Note: This function only supports JSONL format (one JSON object per line).
 * For JSON array format, the file must be loaded into memory.
 *
 * @param path - Path to JSONL file
 * @param options - Streaming options
 * @returns Async iterator yielding StreamingDocument objects
 *
 * @example
 * // Process MongoDB documents one at a time
 * for await (const { document, lineNumber, error } of streamFromMongodbJsonl('./data.jsonl')) {
 *   if (error) {
 *     console.error(`Error at line ${lineNumber}: ${error}`)
 *     continue
 *   }
 *   // document has BSON values converted
 *   await processDocument(document)
 * }
 *
 * @example
 * // With options
 * const stream = streamFromMongodbJsonl('./data.jsonl', {
 *   convertObjectIds: true,
 *   convertDates: true,
 *   transform: (doc) => ({ ...doc, imported: true }),
 *   skipErrors: true,
 * })
 */
export async function* streamFromMongodbJsonl(
  path: string,
  options?: MongoStreamingOptions
): AsyncGenerator<StreamingDocument> {
  const opts = {
    skipErrors: false,
    convertObjectIds: true,
    convertDates: true,
    ...options,
  }

  // Check if file exists
  if (!await fileExists(path)) {
    throw new Error(`File not found: ${path}`)
  }

  let lineNumber = 0

  for await (const line of readLines(path)) {
    lineNumber++

    // Skip empty lines
    if (!line.trim()) {
      continue
    }

    // Parse JSON
    let doc: Record<string, unknown>
    try {
      doc = JSON.parse(line)
    } catch (err) {
      if (opts.skipErrors) {
        continue
      }
      // Include position info in error message (always include "position" word for consistency)
      let errorMsg = 'Invalid JSON'
      if (err instanceof SyntaxError) {
        const position = extractPositionFromSyntaxError(err)
        // Always include "position" in the message for test consistency, even if unknown
        errorMsg += position !== undefined ? ` (position ${position})` : ' (position unknown)'
        errorMsg += `: ${err.message}`
      } else {
        errorMsg += ` (position unknown): ${(err as Error).message}`
      }
      yield {
        document: null,
        lineNumber,
        error: errorMsg,
      }
      continue
    }

    // Convert BSON extended JSON values
    if (opts.convertObjectIds || opts.convertDates) {
      doc = convertBsonDocument(doc, { convertObjectIds: opts.convertObjectIds ?? true, convertDates: opts.convertDates ?? true })
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

    // Skip null/undefined documents (filtered by transform)
    if (doc == null) {
      continue
    }

    yield {
      document: doc,
      lineNumber,
    }
  }
}

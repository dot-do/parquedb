/**
 * JSON/JSONL import utilities for ParqueDB
 *
 * Supports importing from:
 * - JSON array files
 * - JSON objects with nested arrays
 * - JSONL (JSON Lines) files
 */

import type { ParqueDB } from '../ParqueDB'
import type { CreateInput, EntityId } from '../types/entity'
import type { JsonImportOptions, MigrationResult, MigrationError } from './types'
import { getNestedValue, fileExists } from './utils'

/**
 * Default migration options
 */
const DEFAULT_OPTIONS: Required<Omit<JsonImportOptions, 'onProgress' | 'transform' | 'entityType' | 'arrayPath'>> = {
  batchSize: 1000,
  skipValidation: false,
  actor: 'system/migration',
}

/**
 * Import documents from a JSON file
 *
 * Supports JSON array files or objects with nested arrays.
 *
 * @param db - ParqueDB instance
 * @param ns - Namespace to import into
 * @param path - Path to JSON file
 * @param options - Import options
 * @returns Migration result
 *
 * @example
 * // Import from JSON array file
 * const result = await importFromJson(db, 'users', './users.json')
 *
 * @example
 * // Import with transform
 * const result = await importFromJson(db, 'products', './data.json', {
 *   arrayPath: 'data.products',
 *   transform: (doc) => ({
 *     ...doc,
 *     $type: 'Product',
 *     name: doc.title,
 *   }),
 *   onProgress: (count) => console.log(`Imported ${count} documents`),
 * })
 */
export async function importFromJson(
  db: ParqueDB,
  ns: string,
  path: string,
  options?: JsonImportOptions
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

  // Read and parse JSON file
  const content = await readFile(path)
  let data: unknown

  try {
    data = JSON.parse(content)
  } catch (err) {
    throw new Error(`Invalid JSON in file ${path}: ${(err as Error).message}`)
  }

  // Extract array from data
  let documents: unknown[]

  if (Array.isArray(data)) {
    documents = data
  } else if (typeof data === 'object' && data !== null) {
    if (opts.arrayPath) {
      const nested = getNestedValue(data as Record<string, unknown>, opts.arrayPath)
      if (!Array.isArray(nested)) {
        throw new Error(`Path '${opts.arrayPath}' does not contain an array`)
      }
      documents = nested
    } else {
      // Try to find an array in the object
      const arrays = Object.values(data).filter(Array.isArray)
      if (arrays.length === 1) {
        documents = arrays[0] as unknown[]
      } else if (arrays.length > 1) {
        throw new Error('Multiple arrays found in JSON object. Please specify arrayPath option.')
      } else {
        throw new Error('No array found in JSON object. Please specify arrayPath option.')
      }
    }
  } else {
    throw new Error('JSON root must be an array or object')
  }

  // Get collection
  const collection = db.collection(ns)

  // Process documents in batches
  const batch: CreateInput<Record<string, unknown>>[] = []

  for (let i = 0; i < documents.length; i++) {
    let doc = documents[i]

    // Apply transform
    if (opts.transform) {
      try {
        doc = opts.transform(doc)
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

    // Skip null/undefined documents (filtered by transform)
    if (doc == null) {
      skipped++
      continue
    }

    // Prepare document for import
    const createInput = prepareDocument(doc, ns, opts.entityType)
    batch.push(createInput)

    // Process batch when full
    if (batch.length >= opts.batchSize) {
      const batchResult = await processBatch(collection, batch, opts, errors, i - batch.length + 1)
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
    const batchResult = await processBatch(collection, batch, opts, errors, documents.length - batch.length)
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
 * Import documents from a JSONL (JSON Lines) file
 *
 * JSONL files contain one JSON object per line, making them suitable
 * for streaming large datasets.
 *
 * @param db - ParqueDB instance
 * @param ns - Namespace to import into
 * @param path - Path to JSONL file
 * @param options - Import options
 * @returns Migration result
 *
 * @example
 * const result = await importFromJsonl(db, 'logs', './events.jsonl', {
 *   batchSize: 5000,
 *   onProgress: (count) => console.log(`Processed ${count} lines`),
 * })
 */
export async function importFromJsonl(
  db: ParqueDB,
  ns: string,
  path: string,
  options?: JsonImportOptions
): Promise<MigrationResult> {
  const startTime = Date.now()
  const opts = { ...DEFAULT_OPTIONS, ...options }
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

  // Process lines using async iteration
  const batch: CreateInput<Record<string, unknown>>[] = []

  for await (const line of readLines(path)) {
    lineNumber++

    // Skip empty lines
    if (!line.trim()) {
      continue
    }

    // Parse JSON
    let doc: unknown
    try {
      doc = JSON.parse(line)
    } catch (err) {
      errors.push({
        index: lineNumber,
        message: `Invalid JSON at line ${lineNumber}: ${(err as Error).message}`,
        document: line,
      })
      failed++
      continue
    }

    // Apply transform
    if (opts.transform) {
      try {
        doc = opts.transform(doc)
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
    const createInput = prepareDocument(doc, ns, opts.entityType)
    batch.push(createInput)

    // Process batch when full
    if (batch.length >= opts.batchSize) {
      const batchResult = await processBatch(collection, batch, opts, errors, lineNumber - batch.length)
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
    const batchResult = await processBatch(collection, batch, opts, errors, lineNumber - batch.length)
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
 * Prepare a document for import by adding required fields
 */
function prepareDocument(
  doc: unknown,
  ns: string,
  entityType?: string
): CreateInput<Record<string, unknown>> {
  if (typeof doc !== 'object' || doc === null) {
    // Wrap primitives in an object
    return {
      $type: entityType || capitalizeNamespace(ns),
      name: String(doc),
      value: doc,
    }
  }

  const obj = doc as Record<string, unknown>

  // Determine $type
  const $type = obj.$type as string
    || entityType
    || capitalizeNamespace(ns)

  // Determine name
  const name = obj.name as string
    || obj.title as string
    || obj.label as string
    || obj._id as string
    || obj.id as string
    || `${$type}-${Date.now()}`

  return {
    ...obj,
    $type,
    name: String(name),
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
 * e.g., 'users' -> 'User', 'blog-posts' -> 'BlogPost'
 */
function capitalizeNamespace(ns: string): string {
  return ns
    .split(/[-_]/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join('')
    .replace(/s$/, '') // Remove trailing 's' for singular form
}

/**
 * Read file contents
 */
async function readFile(path: string): Promise<string> {
  // Use dynamic import for fs to support both Node.js and other environments
  const fs = await import('fs/promises')
  return fs.readFile(path, 'utf-8')
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

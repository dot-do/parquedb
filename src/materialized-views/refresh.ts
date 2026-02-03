/**
 * Materialized View Refresh Logic for ParqueDB
 *
 * Implements full refresh for materialized views, which:
 * 1. Reads all source data
 * 2. Applies the view query (filter/project/sort or pipeline)
 * 3. Atomically replaces the MV data
 *
 * Handles large datasets efficiently through batching.
 */

import type { StorageBackend } from '../types/storage'
import type { Filter } from '../types/filter'
import type { AggregationStage } from '../aggregation/types'
import { executeAggregation } from '../aggregation/executor'
import { matchesFilter } from '../query/filter'
import { ParquetReader } from '../parquet/reader'
import { ParquetWriter } from '../parquet/writer'
import type { ParquetSchema } from '../parquet/types'
import type {
  ViewDefinition,
  ViewQuery,
  ViewMetadata,
  ViewState,
} from './types'
import { isPipelineQuery } from './types'

// =============================================================================
// Constants
// =============================================================================

/**
 * Default batch size for processing large datasets
 */
export const DEFAULT_BATCH_SIZE = 10000

/**
 * Default row group size for output Parquet files
 */
export const DEFAULT_ROW_GROUP_SIZE = 5000

// =============================================================================
// Refresh Options
// =============================================================================

/**
 * Options for full refresh operation
 */
export interface FullRefreshOptions {
  /**
   * Maximum rows to process per batch (for memory efficiency)
   * @default 10000
   */
  batchSize?: number

  /**
   * Row group size for output Parquet file
   * @default 5000
   */
  rowGroupSize?: number

  /**
   * Progress callback for long-running refreshes
   */
  onProgress?: (progress: RefreshProgress) => void

  /**
   * Abort signal for cancellation
   */
  signal?: AbortSignal

  /**
   * Schema for the output Parquet file (inferred from first batch if not provided)
   */
  outputSchema?: ParquetSchema
}

/**
 * Progress information during refresh
 */
export interface RefreshProgress {
  /** Current phase */
  phase: 'reading' | 'processing' | 'writing'

  /** Rows processed so far */
  rowsProcessed: number

  /** Total rows (if known) */
  totalRows?: number

  /** Percentage complete (0-100) */
  percent: number

  /** Current batch number */
  batch?: number

  /** Total batches (if known) */
  totalBatches?: number
}

/**
 * Result of a full refresh operation
 */
export interface FullRefreshResult {
  /** Whether the refresh was successful */
  success: boolean

  /** Type of refresh performed */
  refreshType: 'full'

  /** Number of rows in the refreshed MV */
  rowCount: number

  /** Size of the output file in bytes */
  sizeBytes: number

  /** Duration of the refresh in milliseconds */
  durationMs: number

  /** Number of source rows read */
  sourceRowsRead: number

  /** Error message if refresh failed */
  error?: string
}

// =============================================================================
// Storage Paths
// =============================================================================

/**
 * Get the path for a view's data file
 */
export function getViewDataPath(viewName: string): string {
  return `_views/${viewName}/data.parquet`
}

/**
 * Get the path for a view's temporary data file (for atomic writes)
 */
export function getViewTempDataPath(viewName: string): string {
  return `_views/${viewName}/data.tmp.parquet`
}

/**
 * Get the path for a view's metadata file
 */
export function getViewMetadataPath(viewName: string): string {
  return `_views/${viewName}/metadata.json`
}

/**
 * Get the path for a source collection's data file
 */
export function getSourceDataPath(source: string): string {
  return `data/${source}/data.parquet`
}

// =============================================================================
// Full Refresh Implementation
// =============================================================================

/**
 * Perform a full refresh of a materialized view
 *
 * This function:
 * 1. Reads all source data
 * 2. Applies the view query
 * 3. Atomically replaces the MV data
 *
 * @param storage - Storage backend for reading/writing data
 * @param definition - The view definition
 * @param options - Refresh options
 * @returns Result of the refresh operation
 *
 * @example
 * ```typescript
 * const result = await fullRefresh(storage, viewDefinition, {
 *   batchSize: 5000,
 *   onProgress: (p) => console.log(`${p.percent}% complete`)
 * })
 *
 * if (result.success) {
 *   console.log(`Refreshed ${result.rowCount} rows in ${result.durationMs}ms`)
 * }
 * ```
 */
export async function fullRefresh(
  storage: StorageBackend,
  definition: ViewDefinition,
  options: FullRefreshOptions = {}
): Promise<FullRefreshResult> {
  const startTime = Date.now()
  const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
  const rowGroupSize = options.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE

  try {
    // Check for cancellation
    if (options.signal?.aborted) {
      return createErrorResult('Refresh cancelled', startTime)
    }

    // Report progress: starting
    options.onProgress?.({
      phase: 'reading',
      rowsProcessed: 0,
      percent: 0,
    })

    // Step 1: Read source data
    const sourcePath = getSourceDataPath(definition.source)
    const sourceExists = await storage.exists(sourcePath)

    if (!sourceExists) {
      // Source doesn't exist - create empty view
      return await createEmptyView(storage, definition, options, startTime)
    }

    const reader = new ParquetReader({ storage })
    const sourceData = await reader.read<Record<string, unknown>>(sourcePath)
    const sourceRowsRead = sourceData.length

    // Check for cancellation
    if (options.signal?.aborted) {
      return createErrorResult('Refresh cancelled', startTime)
    }

    // Report progress: reading complete
    options.onProgress?.({
      phase: 'processing',
      rowsProcessed: 0,
      totalRows: sourceRowsRead,
      percent: 10,
    })

    // Step 2: Apply the view query
    let processedData: Record<string, unknown>[]

    if (isPipelineQuery(definition.query)) {
      // Use aggregation pipeline
      processedData = executeAggregation<Record<string, unknown>>(
        sourceData,
        definition.query.pipeline!
      )
    } else {
      // Apply filter/project/sort
      processedData = applySimpleQuery(sourceData, definition.query)
    }

    // Check for cancellation
    if (options.signal?.aborted) {
      return createErrorResult('Refresh cancelled', startTime)
    }

    // Report progress: processing complete
    options.onProgress?.({
      phase: 'writing',
      rowsProcessed: processedData.length,
      totalRows: processedData.length,
      percent: 70,
    })

    // Step 3: Write to temporary file
    const tempPath = getViewTempDataPath(definition.name as string)
    const finalPath = getViewDataPath(definition.name as string)

    // Infer schema from data if not provided
    const schema = options.outputSchema ?? inferSchema(processedData)

    const writer = new ParquetWriter(storage, {
      rowGroupSize,
      compression: 'lz4',
    })

    // Write in batches for large datasets
    if (processedData.length > batchSize) {
      await writeBatched(writer, tempPath, processedData, schema, batchSize, options)
    } else {
      await writer.write(tempPath, processedData, schema)
    }

    // Check for cancellation before finalizing
    if (options.signal?.aborted) {
      // Clean up temp file
      await storage.delete(tempPath).catch(() => {})
      return createErrorResult('Refresh cancelled', startTime)
    }

    // Step 4: Atomically replace the MV data
    await atomicReplace(storage, tempPath, finalPath)

    // Get file size
    const stat = await storage.stat(finalPath)
    const sizeBytes = stat?.size ?? 0

    // Report progress: complete
    options.onProgress?.({
      phase: 'writing',
      rowsProcessed: processedData.length,
      totalRows: processedData.length,
      percent: 100,
    })

    return {
      success: true,
      refreshType: 'full',
      rowCount: processedData.length,
      sizeBytes,
      durationMs: Date.now() - startTime,
      sourceRowsRead,
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return createErrorResult(message, startTime)
  }
}

/**
 * Apply a simple query (filter/project/sort) to data
 */
function applySimpleQuery(
  data: Record<string, unknown>[],
  query: ViewQuery
): Record<string, unknown>[] {
  let result = data

  // Apply filter
  if (query.filter && Object.keys(query.filter).length > 0) {
    result = result.filter((row) => matchesFilter(row, query.filter!))
  }

  // Apply projection
  if (query.project) {
    result = applyProjection(result, query.project)
  }

  // Apply sort
  if (query.sort) {
    result = applySort(result, query.sort)
  }

  return result
}

/**
 * Apply projection to data
 */
function applyProjection(
  data: Record<string, unknown>[],
  projection: Record<string, 0 | 1 | boolean>
): Record<string, unknown>[] {
  const fields = Object.keys(projection)
  const isInclusion = fields.some((f) => projection[f] === 1 || projection[f] === true)

  return data.map((row) => {
    const result: Record<string, unknown> = {}

    if (isInclusion) {
      // Include only specified fields
      for (const field of fields) {
        if (projection[field] === 1 || projection[field] === true) {
          result[field] = row[field]
        }
      }
    } else {
      // Exclude specified fields
      for (const [key, value] of Object.entries(row)) {
        if (!(key in projection) || projection[key] !== 0) {
          result[key] = value
        }
      }
    }

    return result
  })
}

/**
 * Apply sort to data
 */
function applySort(
  data: Record<string, unknown>[],
  sort: Record<string, 1 | -1>
): Record<string, unknown>[] {
  const sortEntries = Object.entries(sort)

  return [...data].sort((a, b) => {
    for (const [field, direction] of sortEntries) {
      const aVal = a[field]
      const bVal = b[field]

      // Handle null/undefined
      if (aVal == null && bVal == null) continue
      if (aVal == null) return direction
      if (bVal == null) return -direction

      // Compare values
      if (aVal < bVal) return -direction
      if (aVal > bVal) return direction
    }
    return 0
  })
}

/**
 * Write data in batches
 */
async function writeBatched(
  writer: ParquetWriter,
  path: string,
  data: Record<string, unknown>[],
  schema: ParquetSchema,
  batchSize: number,
  options: FullRefreshOptions
): Promise<void> {
  const totalBatches = Math.ceil(data.length / batchSize)

  // For now, collect all data and write at once
  // In a more sophisticated implementation, we could write multiple row groups
  // and merge them

  for (let i = 0; i < totalBatches; i++) {
    // Check for cancellation
    if (options.signal?.aborted) {
      throw new Error('Refresh cancelled')
    }

    const start = i * batchSize
    const end = Math.min(start + batchSize, data.length)
    const rowsProcessed = end

    // Report progress
    const percent = 70 + Math.floor((30 * rowsProcessed) / data.length)
    options.onProgress?.({
      phase: 'writing',
      rowsProcessed,
      totalRows: data.length,
      percent,
      batch: i + 1,
      totalBatches,
    })
  }

  // Write all data at once (proper batched writing would need file appending)
  await writer.write(path, data, schema)
}

/**
 * Atomically replace a file
 */
async function atomicReplace(
  storage: StorageBackend,
  tempPath: string,
  finalPath: string
): Promise<void> {
  // Delete existing file if it exists
  if (await storage.exists(finalPath)) {
    await storage.delete(finalPath)
  }

  // Move temp to final
  await storage.move(tempPath, finalPath)
}

/**
 * Create an empty view when source doesn't exist
 */
async function createEmptyView(
  storage: StorageBackend,
  definition: ViewDefinition,
  options: FullRefreshOptions,
  startTime: number
): Promise<FullRefreshResult> {
  const finalPath = getViewDataPath(definition.name as string)

  // Create directory if needed
  await storage.mkdir(`_views/${definition.name}`)

  // Write empty file with basic schema
  const writer = new ParquetWriter(storage)
  const schema: ParquetSchema = options.outputSchema ?? {
    $id: { type: 'UTF8', optional: false },
  }

  await writer.write(finalPath, [], schema)

  return {
    success: true,
    refreshType: 'full',
    rowCount: 0,
    sizeBytes: 0,
    durationMs: Date.now() - startTime,
    sourceRowsRead: 0,
  }
}

/**
 * Create an error result
 */
function createErrorResult(error: string, startTime: number): FullRefreshResult {
  return {
    success: false,
    refreshType: 'full',
    rowCount: 0,
    sizeBytes: 0,
    durationMs: Date.now() - startTime,
    sourceRowsRead: 0,
    error,
  }
}

/**
 * Infer a Parquet schema from data
 */
function inferSchema(data: Record<string, unknown>[]): ParquetSchema {
  if (data.length === 0) {
    return { $id: { type: 'UTF8', optional: false } }
  }

  const schema: ParquetSchema = {}
  const sample = data[0]!

  for (const [key, value] of Object.entries(sample)) {
    schema[key] = {
      type: inferParquetType(value),
      optional: true,
    }
  }

  // Ensure common fields are non-optional
  if (schema.$id) {
    schema.$id.optional = false
  }
  if (schema._id) {
    schema._id.optional = false
  }

  return schema
}

/**
 * Infer Parquet type from a JavaScript value
 */
function inferParquetType(value: unknown): ParquetSchema[string]['type'] {
  if (value === null || value === undefined) {
    return 'UTF8' // Default to string for null
  }

  if (typeof value === 'string') {
    return 'UTF8'
  }

  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'INT64' : 'DOUBLE'
  }

  if (typeof value === 'boolean') {
    return 'BOOLEAN'
  }

  if (value instanceof Date) {
    return 'TIMESTAMP_MILLIS'
  }

  if (Array.isArray(value) || typeof value === 'object') {
    return 'UTF8' // Store complex types as JSON strings
  }

  return 'UTF8'
}

// =============================================================================
// View Metadata Management
// =============================================================================

/**
 * Update view metadata after refresh
 */
export async function updateViewMetadata(
  storage: StorageBackend,
  viewName: string,
  refreshResult: FullRefreshResult
): Promise<ViewMetadata> {
  const metadataPath = getViewMetadataPath(viewName)
  let metadata: ViewMetadata

  // Try to read existing metadata
  try {
    if (await storage.exists(metadataPath)) {
      const data = await storage.read(metadataPath)
      metadata = JSON.parse(new TextDecoder().decode(data)) as ViewMetadata
    } else {
      // Create initial metadata (will need a definition passed in for full implementation)
      throw new Error('Metadata file not found')
    }
  } catch {
    // Return a stub - in production, we'd create proper initial metadata
    throw new Error(`View metadata not found: ${viewName}`)
  }

  // Update metadata
  const now = new Date()
  const newState: ViewState = refreshResult.success ? 'ready' : 'error'

  metadata.state = newState
  metadata.lastRefreshedAt = now
  metadata.lastRefreshDurationMs = refreshResult.durationMs
  metadata.documentCount = refreshResult.rowCount
  metadata.sizeBytes = refreshResult.sizeBytes
  metadata.version += 1

  if (!refreshResult.success) {
    metadata.error = refreshResult.error
  } else {
    delete metadata.error
  }

  // Write updated metadata
  const encoded = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  await storage.write(metadataPath, encoded)

  return metadata
}

// =============================================================================
// Refresh Coordinator
// =============================================================================

/**
 * Coordinate a full refresh with metadata updates
 *
 * This is the high-level function that should be called to refresh a view.
 * It handles:
 * - Setting the view state to 'building'
 * - Performing the refresh
 * - Updating the view state to 'ready' or 'error'
 *
 * @param storage - Storage backend
 * @param viewName - Name of the view to refresh
 * @param options - Refresh options
 * @returns Result of the refresh
 */
export async function refreshView(
  storage: StorageBackend,
  viewName: string,
  options: FullRefreshOptions = {}
): Promise<FullRefreshResult> {
  const metadataPath = getViewMetadataPath(viewName)

  // Load view definition from metadata
  let metadata: ViewMetadata
  try {
    if (!(await storage.exists(metadataPath))) {
      return createErrorResult(`View not found: ${viewName}`, Date.now())
    }

    const data = await storage.read(metadataPath)
    metadata = JSON.parse(new TextDecoder().decode(data)) as ViewMetadata
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return createErrorResult(`Failed to load view metadata: ${message}`, Date.now())
  }

  // Set state to building
  metadata.state = 'building'
  const encoded = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  await storage.write(metadataPath, encoded)

  // Perform the refresh
  const result = await fullRefresh(storage, metadata.definition, options)

  // Update metadata with result
  await updateViewMetadata(storage, viewName, result)

  return result
}

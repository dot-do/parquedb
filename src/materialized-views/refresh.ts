/**
 * Materialized View Refresh Logic for ParqueDB
 *
 * Implements full refresh for materialized views, which:
 * 1. Reads all source data (with chunked/streaming support for large datasets)
 * 2. Applies the view query (filter/project/sort or pipeline)
 * 3. Atomically replaces the MV data
 *
 * Handles large datasets efficiently through:
 * - Chunked reading: Processes data in chunks to avoid memory exhaustion
 * - Partial aggregation: For aggregation pipelines, maintains partial state across chunks
 * - Batched writing: Writes output in configurable batch sizes
 * - Progress tracking: Reports progress during long-running refreshes
 */

import type { StorageBackend } from '../types/storage'
import type { AggregationStage, GroupSpec } from '../aggregation/types'
import {
  isGroupStage,
  isMatchStage,
  isSortStage,
  isLimitStage,
  isSkipStage,
  isProjectStage,
  isSumAccumulator,
  isAvgAccumulator,
  isMinAccumulator,
  isMaxAccumulator,
  isCountAccumulator,
  isFirstAccumulator,
  isLastAccumulator,
  isPushAccumulator,
  isAddToSetAccumulator,
  isFieldRef,
} from '../aggregation/types'
import { executeAggregation } from '../aggregation/executor'
import { matchesFilter } from '../query/filter'
import { getNestedValue } from '../utils'
import { ParquetReader } from '../parquet/reader'
import { ParquetWriter } from '../parquet/writer'
import type { ParquetSchema, ParquetMetadata } from '../parquet/types'
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

/**
 * Default threshold for automatic chunked reading (100k rows)
 */
export const DEFAULT_CHUNKED_READING_THRESHOLD = 100000

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
  batchSize?: number | undefined

  /**
   * Row group size for output Parquet file
   * @default 5000
   */
  rowGroupSize?: number | undefined

  /**
   * Progress callback for long-running refreshes
   */
  onProgress?: ((progress: RefreshProgress) => void) | undefined

  /**
   * Abort signal for cancellation
   */
  signal?: AbortSignal | undefined

  /**
   * Schema for the output Parquet file (inferred from first batch if not provided)
   */
  outputSchema?: ParquetSchema | undefined

  /**
   * Enable chunked reading for large datasets.
   * When true, reads source data in chunks instead of all at once.
   * This is more memory efficient for large datasets.
   * @default false (for backwards compatibility)
   */
  chunkedReading?: boolean | undefined

  /**
   * Threshold in rows above which chunked reading is automatically enabled.
   * If the source file has more rows than this threshold, chunked reading
   * will be used automatically (unless explicitly disabled).
   * Set to 0 to always use chunked reading when available.
   * @default 100000 (100k rows)
   */
  chunkedReadingThreshold?: number | undefined
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
  totalRows?: number | undefined

  /** Percentage complete (0-100) */
  percent: number

  /** Current batch number */
  batch?: number | undefined

  /** Total batches (if known) */
  totalBatches?: number | undefined
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
  error?: string | undefined
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
 * 1. Reads all source data (with optional chunked reading for large datasets)
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
 * // Basic usage
 * const result = await fullRefresh(storage, viewDefinition, {
 *   batchSize: 5000,
 *   onProgress: (p) => console.log(`${p.percent}% complete`)
 * })
 *
 * // With chunked reading for large datasets
 * const result = await fullRefresh(storage, viewDefinition, {
 *   chunkedReading: true,
 *   batchSize: 5000,
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
  const chunkedReadingThreshold = options.chunkedReadingThreshold ?? DEFAULT_CHUNKED_READING_THRESHOLD

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

    // Check if we should use chunked reading
    const reader = new ParquetReader({ storage })
    let useChunkedReading = options.chunkedReading ?? false

    // Auto-detect if chunked reading should be enabled based on file size
    if (!useChunkedReading && options.chunkedReading !== false) {
      try {
        const metadata = await reader.readMetadata(sourcePath)
        if (metadata.numRows > chunkedReadingThreshold) {
          useChunkedReading = true
        }
      } catch {
        // If we can't read metadata, fall back to non-chunked reading
      }
    }

    // Check if chunked aggregation is possible for pipeline queries
    if (isPipelineQuery(definition.query) && useChunkedReading) {
      const pipeline = definition.query.pipeline as AggregationStage[]
      if (canChunkAggregation(pipeline)) {
        return await fullRefreshChunkedAggregation(storage, definition, options, startTime, reader, sourcePath)
      }
      // Fall back to non-chunked for non-chunkable pipelines
      useChunkedReading = false
    }

    // Use chunked reading for simple queries (filter/project/sort)
    if (useChunkedReading && !isPipelineQuery(definition.query)) {
      return await fullRefreshChunked(storage, definition, options, startTime, reader, sourcePath)
    }

    // Non-chunked path (original implementation)
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

// =============================================================================
// Chunked Reading Implementation
// =============================================================================

/**
 * Perform a full refresh using chunked reading for large datasets.
 *
 * This function processes source data in chunks using offset/limit,
 * avoiding loading the entire dataset into memory. It's suitable for
 * simple queries (filter/project/sort) but not for aggregation pipelines.
 *
 * @internal
 */
async function fullRefreshChunked(
  storage: StorageBackend,
  definition: ViewDefinition,
  options: FullRefreshOptions,
  startTime: number,
  reader: ParquetReader,
  sourcePath: string
): Promise<FullRefreshResult> {
  const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
  const rowGroupSize = options.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE

  // Get metadata to know total rows
  let metadata: ParquetMetadata
  try {
    metadata = await reader.readMetadata(sourcePath)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return createErrorResult(`Failed to read source metadata: ${message}`, startTime)
  }

  const totalRows = metadata.numRows
  const totalBatches = Math.ceil(totalRows / batchSize)

  // Report progress: starting chunked read
  options.onProgress?.({
    phase: 'reading',
    rowsProcessed: 0,
    totalRows,
    percent: 0,
    batch: 0,
    totalBatches,
  })

  // Prepare output paths
  const tempPath = getViewTempDataPath(definition.name as string)
  const finalPath = getViewDataPath(definition.name as string)

  // We'll collect all processed data (filter/project applied) and write at end
  // For sort, we need all data before sorting
  const allProcessedData: Record<string, unknown>[] = []
  let sourceRowsRead = 0
  let schema: ParquetSchema | undefined = options.outputSchema

  const writer = new ParquetWriter(storage, {
    rowGroupSize,
    compression: 'lz4',
  })

  // Process in batches using offset/limit
  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    // Check for cancellation
    if (options.signal?.aborted) {
      return createErrorResult('Refresh cancelled', startTime)
    }

    const offset = batchIndex * batchSize
    const limit = Math.min(batchSize, totalRows - offset)

    // Read chunk using offset/limit
    const chunkData = await reader.read<Record<string, unknown>>(sourcePath, {
      offset,
      limit,
    })
    sourceRowsRead += chunkData.length

    // Report progress: reading
    const readPercent = Math.floor(((batchIndex + 1) / totalBatches) * 50)
    options.onProgress?.({
      phase: 'reading',
      rowsProcessed: sourceRowsRead,
      totalRows,
      percent: readPercent,
      batch: batchIndex + 1,
      totalBatches,
    })

    // Apply filter and project (no sort yet - sort requires all data)
    const processed = applySimpleQueryChunked(chunkData, definition.query)
    allProcessedData.push(...processed)

    // Infer schema from first batch if not provided
    if (!schema && processed.length > 0) {
      schema = inferSchema(processed)
    }
  }

  // Check for cancellation before writing
  if (options.signal?.aborted) {
    return createErrorResult('Refresh cancelled', startTime)
  }

  // Apply sort if needed (requires all data)
  let finalData = allProcessedData
  if (definition.query.sort && Object.keys(definition.query.sort).length > 0) {
    finalData = applySort(allProcessedData, definition.query.sort)
  }

  // Report progress: writing
  options.onProgress?.({
    phase: 'writing',
    rowsProcessed: finalData.length,
    totalRows: finalData.length,
    percent: 70,
  })

  // Write all processed data
  if (finalData.length > 0) {
    if (!schema) {
      schema = inferSchema(finalData)
    }
    await writer.write(tempPath, finalData, schema)
  } else if (schema) {
    // Write empty file with schema
    await writer.write(tempPath, [], schema)
  } else {
    // No data and no schema - create empty view
    return await createEmptyView(storage, definition, options, startTime)
  }

  // Check for cancellation before finalizing
  if (options.signal?.aborted) {
    await storage.delete(tempPath).catch(() => {})
    return createErrorResult('Refresh cancelled', startTime)
  }

  // Atomically replace the MV data
  await atomicReplace(storage, tempPath, finalPath)

  // Get final file size
  const stat = await storage.stat(finalPath)
  const sizeBytes = stat?.size ?? 0

  // Report progress: complete
  options.onProgress?.({
    phase: 'writing',
    rowsProcessed: finalData.length,
    totalRows: finalData.length,
    percent: 100,
  })

  return {
    success: true,
    refreshType: 'full',
    rowCount: finalData.length,
    sizeBytes,
    durationMs: Date.now() - startTime,
    sourceRowsRead,
  }
}

/**
 * Apply simple query operations (filter/project) to a chunk of data.
 * Note: Sort is not applied here in chunked mode - it's done at the end.
 *
 * @internal
 */
function applySimpleQueryChunked(
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

  // Sort is intentionally omitted - it's applied after all chunks are collected
  return result
}

// =============================================================================
// Chunked Aggregation Implementation
// =============================================================================

/**
 * Partial aggregation state for a single group
 * Tracks intermediate values needed to finalize aggregations
 */
interface PartialGroupState {
  sums: Record<string, number>
  counts: Record<string, number>
  avgs: Record<string, { sum: number; count: number }>
  mins: Record<string, number | null>
  maxs: Record<string, number | null>
  firsts: Record<string, { value: unknown; set: boolean }>
  lasts: Record<string, unknown>
  pushes: Record<string, unknown[]>
  sets: Record<string, Set<string>>
}

/**
 * Check if an aggregation pipeline can be processed in chunks.
 *
 * A pipeline can be chunked if it follows a pattern like:
 * - Optional $match at the start (can be applied to each chunk)
 * - A single $group stage (can use partial aggregation)
 * - Optional $sort, $limit, $skip, $project at the end
 */
export function canChunkAggregation(pipeline: AggregationStage[]): boolean {
  if (!pipeline || pipeline.length === 0) {
    return false
  }

  let seenGroup = false

  for (const stage of pipeline) {
    if (isMatchStage(stage)) {
      if (seenGroup) return false
    } else if (isGroupStage(stage)) {
      if (seenGroup) return false
      seenGroup = true
      if (!areAccumulatorsChunkable(stage.$group)) return false
    } else if (isSortStage(stage) || isLimitStage(stage) || isSkipStage(stage) || isProjectStage(stage)) {
      // Allowed after $group
    } else {
      return false
    }
  }

  return seenGroup
}

function areAccumulatorsChunkable(groupSpec: GroupSpec): boolean {
  for (const [field, spec] of Object.entries(groupSpec)) {
    if (field === '_id') continue
    if (
      isSumAccumulator(spec) ||
      isAvgAccumulator(spec) ||
      isMinAccumulator(spec) ||
      isMaxAccumulator(spec) ||
      isCountAccumulator(spec) ||
      isFirstAccumulator(spec) ||
      isLastAccumulator(spec) ||
      isPushAccumulator(spec) ||
      isAddToSetAccumulator(spec)
    ) {
      continue
    }
    return false
  }
  return true
}

function createEmptyPartialState(): PartialGroupState {
  return {
    sums: {},
    counts: {},
    avgs: {},
    mins: {},
    maxs: {},
    firsts: {},
    lasts: {},
    pushes: {},
    sets: {},
  }
}

function updatePartialState(
  state: PartialGroupState,
  items: Record<string, unknown>[],
  groupSpec: GroupSpec
): void {
  for (const [field, spec] of Object.entries(groupSpec)) {
    if (field === '_id') continue

    if (isSumAccumulator(spec)) {
      const sumValue = spec.$sum
      let chunkSum = 0
      if (sumValue === 1) {
        chunkSum = items.length
      } else if (typeof sumValue === 'number') {
        chunkSum = items.length * sumValue
      } else if (isFieldRef(sumValue)) {
        const fieldPath = sumValue.slice(1)
        chunkSum = items.reduce((sum, item) => {
          const val = getNestedValue(item, fieldPath)
          return sum + (typeof val === 'number' ? val : 0)
        }, 0)
      }
      state.sums[field] = (state.sums[field] ?? 0) + chunkSum
    } else if (isCountAccumulator(spec)) {
      state.counts[field] = (state.counts[field] ?? 0) + items.length
    } else if (isAvgAccumulator(spec) && isFieldRef(spec.$avg)) {
      const fieldPath = spec.$avg.slice(1)
      const chunkSum = items.reduce((sum, item) => {
        const val = getNestedValue(item, fieldPath)
        return sum + (typeof val === 'number' ? val : 0)
      }, 0)
      if (!state.avgs[field]) state.avgs[field] = { sum: 0, count: 0 }
      state.avgs[field].sum += chunkSum
      state.avgs[field].count += items.length
    } else if (isMinAccumulator(spec) && isFieldRef(spec.$min)) {
      const fieldPath = spec.$min.slice(1)
      const chunkMin = items.reduce((min: number | null, item) => {
        const val = getNestedValue(item, fieldPath)
        return typeof val === 'number' && (min === null || val < min) ? val : min
      }, null)
      if (chunkMin !== null) {
        const currentMin = state.mins[field]
        if (currentMin === null || currentMin === undefined || chunkMin < currentMin) {
          state.mins[field] = chunkMin
        }
      }
    } else if (isMaxAccumulator(spec) && isFieldRef(spec.$max)) {
      const fieldPath = spec.$max.slice(1)
      const chunkMax = items.reduce((max: number | null, item) => {
        const val = getNestedValue(item, fieldPath)
        return typeof val === 'number' && (max === null || val > max) ? val : max
      }, null)
      if (chunkMax !== null) {
        const currentMax = state.maxs[field]
        if (currentMax === null || currentMax === undefined || chunkMax > currentMax) {
          state.maxs[field] = chunkMax
        }
      }
    } else if (isFirstAccumulator(spec) && isFieldRef(spec.$first)) {
      if (!state.firsts[field] || !state.firsts[field].set) {
        if (items.length > 0) {
          const fieldPath = spec.$first.slice(1)
          state.firsts[field] = { value: getNestedValue(items[0]!, fieldPath), set: true }
        }
      }
    } else if (isLastAccumulator(spec) && isFieldRef(spec.$last)) {
      if (items.length > 0) {
        const fieldPath = spec.$last.slice(1)
        state.lasts[field] = getNestedValue(items[items.length - 1]!, fieldPath)
      }
    } else if (isPushAccumulator(spec) && isFieldRef(spec.$push)) {
      const fieldPath = spec.$push.slice(1)
      if (!state.pushes[field]) state.pushes[field] = []
      for (const item of items) {
        state.pushes[field].push(getNestedValue(item, fieldPath))
      }
    } else if (isAddToSetAccumulator(spec) && isFieldRef(spec.$addToSet)) {
      const fieldPath = spec.$addToSet.slice(1)
      if (!state.sets[field]) state.sets[field] = new Set()
      for (const item of items) {
        state.sets[field].add(JSON.stringify(getNestedValue(item, fieldPath)))
      }
    }
  }
}

function finalizePartialState(
  groupKey: unknown,
  state: PartialGroupState,
  groupSpec: GroupSpec
): Record<string, unknown> {
  const result: Record<string, unknown> = { _id: groupKey }

  for (const [field, spec] of Object.entries(groupSpec)) {
    if (field === '_id') continue

    if (isSumAccumulator(spec)) {
      result[field] = state.sums[field] ?? 0
    } else if (isCountAccumulator(spec)) {
      result[field] = state.counts[field] ?? 0
    } else if (isAvgAccumulator(spec)) {
      const avgState = state.avgs[field]
      result[field] = avgState && avgState.count > 0 ? avgState.sum / avgState.count : 0
    } else if (isMinAccumulator(spec)) {
      result[field] = state.mins[field] ?? null
    } else if (isMaxAccumulator(spec)) {
      result[field] = state.maxs[field] ?? null
    } else if (isFirstAccumulator(spec)) {
      result[field] = state.firsts[field]?.value ?? null
    } else if (isLastAccumulator(spec)) {
      result[field] = state.lasts[field] ?? null
    } else if (isPushAccumulator(spec)) {
      result[field] = state.pushes[field] ?? []
    } else if (isAddToSetAccumulator(spec)) {
      const set = state.sets[field]
      result[field] = set ? Array.from(set).map(s => JSON.parse(s)) : []
    }
  }

  return result
}

function extractGroupKey(doc: Record<string, unknown>, idSpec: unknown): unknown {
  if (idSpec === null) return null
  if (isFieldRef(idSpec)) return getNestedValue(doc, idSpec.slice(1))
  if (typeof idSpec === 'object' && idSpec !== null) {
    const compoundKey: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(idSpec as Record<string, unknown>)) {
      compoundKey[key] = isFieldRef(value) ? getNestedValue(doc, value.slice(1)) : value
    }
    return compoundKey
  }
  return idSpec
}

/**
 * Perform a full refresh using chunked aggregation for pipelines.
 * @internal
 */
async function fullRefreshChunkedAggregation(
  storage: StorageBackend,
  definition: ViewDefinition,
  options: FullRefreshOptions,
  startTime: number,
  reader: ParquetReader,
  sourcePath: string
): Promise<FullRefreshResult> {
  const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
  const rowGroupSize = options.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE
  const pipeline = definition.query.pipeline as AggregationStage[]

  let metadata: ParquetMetadata
  try {
    metadata = await reader.readMetadata(sourcePath)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return createErrorResult(`Failed to read source metadata: ${message}`, startTime)
  }

  const totalRows = metadata.numRows
  const totalBatches = Math.ceil(totalRows / batchSize)

  options.onProgress?.({
    phase: 'reading',
    rowsProcessed: 0,
    totalRows,
    percent: 0,
    batch: 0,
    totalBatches,
  })

  // Extract pipeline stages
  let matchFilter: Record<string, unknown> | undefined
  let groupSpec: GroupSpec | undefined
  const postGroupStages: AggregationStage[] = []

  for (const stage of pipeline) {
    if (isMatchStage(stage)) matchFilter = stage.$match
    else if (isGroupStage(stage)) groupSpec = stage.$group
    else postGroupStages.push(stage)
  }

  if (!groupSpec) {
    return createErrorResult('No $group stage found in pipeline', startTime)
  }

  const partialStates = new Map<string, { key: unknown; state: PartialGroupState }>()
  let sourceRowsRead = 0

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    if (options.signal?.aborted) {
      return createErrorResult('Refresh cancelled', startTime)
    }

    const offset = batchIndex * batchSize
    const limit = Math.min(batchSize, totalRows - offset)

    let chunkData = await reader.read<Record<string, unknown>>(sourcePath, { offset, limit })
    sourceRowsRead += chunkData.length

    options.onProgress?.({
      phase: 'reading',
      rowsProcessed: sourceRowsRead,
      totalRows,
      percent: Math.floor(((batchIndex + 1) / totalBatches) * 50),
      batch: batchIndex + 1,
      totalBatches,
    })

    if (matchFilter) {
      chunkData = chunkData.filter(doc => matchesFilter(doc, matchFilter))
    }

    const chunkGroups = new Map<string, Record<string, unknown>[]>()
    for (const doc of chunkData) {
      const groupKey = extractGroupKey(doc, groupSpec._id)
      const keyStr = JSON.stringify(groupKey)
      if (!chunkGroups.has(keyStr)) chunkGroups.set(keyStr, [])
      chunkGroups.get(keyStr)!.push(doc)
    }

    for (const [keyStr, items] of chunkGroups) {
      if (!partialStates.has(keyStr)) {
        partialStates.set(keyStr, { key: JSON.parse(keyStr), state: createEmptyPartialState() })
      }
      updatePartialState(partialStates.get(keyStr)!.state, items, groupSpec)
    }

    options.onProgress?.({
      phase: 'processing',
      rowsProcessed: sourceRowsRead,
      totalRows,
      percent: 50 + Math.floor(((batchIndex + 1) / totalBatches) * 25),
      batch: batchIndex + 1,
      totalBatches,
    })
  }

  let results: Record<string, unknown>[] = []
  for (const { key, state } of partialStates.values()) {
    results.push(finalizePartialState(key, state, groupSpec))
  }

  if (postGroupStages.length > 0) {
    results = executeAggregation(results, postGroupStages)
  }

  const tempPath = getViewTempDataPath(definition.name as string)
  const finalPath = getViewDataPath(definition.name as string)

  if (options.signal?.aborted) {
    return createErrorResult('Refresh cancelled', startTime)
  }

  options.onProgress?.({ phase: 'writing', rowsProcessed: results.length, totalRows: results.length, percent: 80 })

  const schema = options.outputSchema ?? inferSchema(results)
  const writer = new ParquetWriter(storage, { rowGroupSize, compression: 'lz4' })

  await writer.write(tempPath, results, schema)
  await atomicReplace(storage, tempPath, finalPath)

  const stat = await storage.stat(finalPath)
  const sizeBytes = stat?.size ?? 0

  options.onProgress?.({ phase: 'writing', rowsProcessed: results.length, totalRows: results.length, percent: 100 })

  return {
    success: true,
    refreshType: 'full',
    rowCount: results.length,
    sizeBytes,
    durationMs: Date.now() - startTime,
    sourceRowsRead,
  }
}

// =============================================================================
// Query Helpers
// =============================================================================

/**
 * Apply a simple query (filter/project/sort) to data
 */
function applySimpleQuery(
  data: Record<string, unknown>[],
  query: ViewQuery
): Record<string, unknown>[] {
  let result = data

  if (query.filter && Object.keys(query.filter).length > 0) {
    result = result.filter((row) => matchesFilter(row, query.filter!))
  }

  if (query.project) {
    result = applyProjection(result, query.project)
  }

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
      for (const field of fields) {
        if (projection[field] === 1 || projection[field] === true) {
          result[field] = row[field]
        }
      }
    } else {
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

      if (aVal == null && bVal == null) continue
      if (aVal == null) return direction
      if (bVal == null) return -direction

      if (aVal < bVal) return -direction
      if (aVal > bVal) return direction
    }
    return 0
  })
}

// =============================================================================
// Write Helpers
// =============================================================================

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

  for (let i = 0; i < totalBatches; i++) {
    if (options.signal?.aborted) {
      throw new Error('Refresh cancelled')
    }

    const start = i * batchSize
    const end = Math.min(start + batchSize, data.length)
    const rowsProcessed = end

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
  if (await storage.exists(finalPath)) {
    await storage.delete(finalPath)
  }
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

  await storage.mkdir(`_views/${definition.name}`)

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

  if (schema.$id) schema.$id.optional = false
  if (schema._id) schema._id.optional = false

  return schema
}

/**
 * Infer Parquet type from a JavaScript value
 */
function inferParquetType(value: unknown): ParquetSchema[string]['type'] {
  if (value === null || value === undefined) return 'UTF8'
  if (typeof value === 'string') return 'UTF8'
  if (typeof value === 'number') return Number.isInteger(value) ? 'INT64' : 'DOUBLE'
  if (typeof value === 'boolean') return 'BOOLEAN'
  if (value instanceof Date) return 'TIMESTAMP_MILLIS'
  if (Array.isArray(value) || typeof value === 'object') return 'UTF8'
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

  try {
    if (await storage.exists(metadataPath)) {
      const data = await storage.read(metadataPath)
      metadata = JSON.parse(new TextDecoder().decode(data)) as ViewMetadata
    } else {
      throw new Error('Metadata file not found')
    }
  } catch {
    throw new Error(`View metadata not found: ${viewName}`)
  }

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

  const encoded = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  await storage.write(metadataPath, encoded)

  return metadata
}

// =============================================================================
// Refresh Coordinator
// =============================================================================

/**
 * Coordinate a full refresh with metadata updates
 */
export async function refreshView(
  storage: StorageBackend,
  viewName: string,
  options: FullRefreshOptions = {}
): Promise<FullRefreshResult> {
  const metadataPath = getViewMetadataPath(viewName)

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

  metadata.state = 'building'
  const encoded = new TextEncoder().encode(JSON.stringify(metadata, null, 2))
  await storage.write(metadataPath, encoded)

  const result = await fullRefresh(storage, metadata.definition, options)

  await updateViewMetadata(storage, viewName, result)

  return result
}

// =============================================================================
// Crash Recovery
// =============================================================================

/**
 * Result of view crash recovery
 */
export interface ViewRecoveryResult {
  /** Whether recovery was performed */
  recovered: boolean
  /** Action taken during recovery */
  action: 'none' | 'restored_backup' | 'deleted_backup' | 'deleted_temp'
  /** View name (for batch recovery) */
  viewName?: string | undefined
  /** Error message if recovery failed */
  error?: string | undefined
}

/**
 * Recover a view from a crash during atomic replace
 *
 * This function handles the following scenarios:
 * 1. Both backup and final exist - clean up orphaned backup
 * 2. Only backup exists (final deleted but move failed) - restore backup
 * 3. Only temp exists - clean up orphaned temp file
 */
export async function recoverViewFromCrash(
  storage: StorageBackend,
  viewName: string
): Promise<ViewRecoveryResult> {
  const finalPath = getViewDataPath(viewName)
  const backupPath = `${finalPath}.backup`
  const tempPath = getViewTempDataPath(viewName)

  const [finalExists, backupExists, tempExists] = await Promise.all([
    storage.exists(finalPath),
    storage.exists(backupPath),
    storage.exists(tempPath),
  ])

  // No orphaned files - nothing to recover
  if (!backupExists && !tempExists) {
    return { recovered: false, action: 'none' }
  }

  // Case 1: Final and backup both exist - clean up backup
  if (finalExists && backupExists) {
    await storage.delete(backupPath)
    if (tempExists) {
      await storage.delete(tempPath)
    }
    return { recovered: true, action: 'deleted_backup' }
  }

  // Case 2: Only backup exists (crash after move-to-backup but before restore)
  if (backupExists && !finalExists) {
    // Read backup and write to final
    const backupData = await storage.read(backupPath)
    await storage.write(finalPath, backupData)
    await storage.delete(backupPath)
    if (tempExists) {
      await storage.delete(tempPath)
    }
    return { recovered: true, action: 'restored_backup' }
  }

  // Case 3: Only temp exists - clean it up
  if (tempExists && !backupExists) {
    await storage.delete(tempPath)
    return { recovered: true, action: 'deleted_temp' }
  }

  return { recovered: false, action: 'none' }
}

/**
 * Recover all views from crashes
 *
 * Scans the _views directory and attempts recovery on each view
 * Returns array of recovery results (only views that needed recovery)
 */
export async function recoverAllViewsFromCrash(
  storage: StorageBackend
): Promise<ViewRecoveryResult[]> {
  const results: ViewRecoveryResult[] = []

  try {
    const listing = await storage.list('_views/')

    // Extract unique view names from the listing
    const viewNames = new Set<string>()
    for (const filePath of listing.files) {
      // Path format: _views/{viewName}/...
      const match = filePath.match(/^_views\/([^/]+)\//)
      if (match) {
        viewNames.add(match[1])
      }
    }

    // Attempt recovery for each view
    for (const viewName of viewNames) {
      try {
        const result = await recoverViewFromCrash(storage, viewName)
        // Only include views that actually had recovery performed
        if (result.recovered) {
          results.push({ ...result, viewName })
        }
      } catch (error) {
        results.push({
          recovered: false,
          action: 'none',
          viewName,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }
  } catch {
    // If we can't list views, return empty results
  }

  return results
}

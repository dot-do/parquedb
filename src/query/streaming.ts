/**
 * Streaming Row Group Processing for ParqueDB
 *
 * Provides lazy/streaming query execution that:
 * - Only reads row groups as needed
 * - Supports early termination for LIMIT queries
 * - Maintains O(row_group) memory instead of O(total_rows)
 * - Enables streaming aggregations without full materialization
 *
 * @example
 * ```typescript
 * // Stream through results efficiently
 * for await (const row of parquetQueryStream(storage, path, { limit: 10 })) {
 *   console.log(row)
 * }
 *
 * // Streaming aggregation
 * let sum = 0
 * for await (const row of parquetQueryStream(storage, path, { columns: ['value'] })) {
 *   sum += row.value
 * }
 * ```
 */

import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import type { FileMetaData } from 'hyparquet'
import type { StorageBackend } from '../types/storage'
import type { Filter } from '../types/filter'
import type { SortSpec } from '../types/options'
import { initializeAsyncBuffer } from '../parquet/reader'
import { compressors } from '../parquet/compression'
import { toPredicate, extractRowGroupStats, selectRowGroups } from './predicate'
import { DEFAULT_CONCURRENCY } from '../constants'
import { isNullish } from '../utils/comparison'

// =============================================================================
// Types
// =============================================================================

/**
 * Statistics for streaming query execution
 */
export interface StreamingQueryStats {
  /** Total row groups in the file */
  rowGroupsTotal: number
  /** Row groups actually read */
  rowGroupsRead: number
  /** Rows scanned from storage */
  rowsScanned: number
  /** Rows yielded to consumer */
  rowsYielded: number
  /** Whether query terminated early (before reading all row groups) */
  terminatedEarly: boolean
}

/**
 * Options for streaming Parquet queries
 */
export interface StreamingQueryOptions<T = unknown> {
  /** MongoDB-style filter to apply */
  filter?: Filter | undefined
  /** Columns to read (undefined = all columns) */
  columns?: string[] | undefined
  /** Sort specification - note: sorting requires full materialization */
  sort?: SortSpec | undefined
  /** Maximum rows to return */
  limit?: number | undefined
  /** Rows to skip before returning */
  skip?: number | undefined
  /** Number of row groups to read concurrently */
  concurrency?: number | undefined
  /** Include soft-deleted entities */
  includeDeleted?: boolean | undefined
  /** Callback to receive execution statistics */
  onStats?: ((stats: StreamingQueryStats) => void) | undefined
  /** Optional pre-loaded metadata */
  metadata?: FileMetaData | undefined
}

// =============================================================================
// Streaming Query Implementation
// =============================================================================

/**
 * Stream rows from a Parquet file with lazy row group loading.
 *
 * This is the main entry point for memory-efficient query execution.
 * Row groups are loaded on-demand as the consumer iterates, enabling:
 *
 * 1. **Early termination**: LIMIT queries stop after finding enough rows
 * 2. **Constant memory**: Only one row group worth of data in memory at a time
 * 3. **Streaming aggregations**: Process rows without storing all in memory
 *
 * @param storage - Storage backend for reading the file
 * @param path - Path to the Parquet file
 * @param options - Query options (filter, limit, columns, etc.)
 * @yields Individual rows matching the query
 *
 * @example
 * ```typescript
 * // Basic streaming with limit
 * for await (const row of parquetQueryStream(storage, 'data.parquet', { limit: 10 })) {
 *   console.log(row.$id)
 * }
 *
 * // Streaming aggregation
 * let count = 0
 * for await (const row of parquetQueryStream(storage, 'data.parquet', { filter: { status: 'active' } })) {
 *   count++
 * }
 * ```
 */
export async function* parquetQueryStream<T = Record<string, unknown>>(
  storage: StorageBackend,
  path: string,
  options: StreamingQueryOptions<T> = {}
): AsyncGenerator<T, void, undefined> {
  const {
    filter = {},
    columns,
    sort,
    limit,
    skip = 0,
    concurrency = DEFAULT_CONCURRENCY,
    includeDeleted = false,
    onStats,
  } = options

  // Initialize statistics
  const stats: StreamingQueryStats = {
    rowGroupsTotal: 0,
    rowGroupsRead: 0,
    rowsScanned: 0,
    rowsYielded: 0,
    terminatedEarly: false,
  }

  // Load metadata
  const asyncBuffer = await initializeAsyncBuffer(storage, path)
  const metadata = options.metadata ?? (await parquetMetadataAsync(asyncBuffer))

  stats.rowGroupsTotal = metadata.row_groups?.length ?? 0

  // If sorting is required, we cannot stream - must load all and sort
  if (sort && Object.keys(sort).length > 0) {
    yield* streamWithSort(asyncBuffer, metadata, options, stats)
    return
  }

  // Get row group stats for predicate pushdown
  const rowGroupStats = extractRowGroupStats({
    rowGroups: (metadata.row_groups ?? []).map((rg, i) => ({
      numRows: Number(rg.num_rows ?? 0),
      totalByteSize: Number(rg.total_byte_size ?? 0),
      ordinal: i,
      columns: (rg.columns ?? []).map((col) => {
        const metaData = col.meta_data as {
          path_in_schema?: string[]
          statistics?: {
            min_value?: unknown
            max_value?: unknown
            null_count?: number | bigint
          }
        } | undefined
        return {
          pathInSchema: metaData?.path_in_schema ?? [],
          statistics: metaData?.statistics
            ? {
                min: metaData.statistics.min_value,
                max: metaData.statistics.max_value,
                nullCount:
                  metaData.statistics.null_count !== undefined
                    ? Number(metaData.statistics.null_count)
                    : undefined,
              }
            : undefined,
          totalCompressedSize: 0,
          totalUncompressedSize: 0,
          numValues: 0,
          encodings: [],
          codec: 'UNCOMPRESSED' as const,
        }
      }),
    })),
    numRows: Number(metadata.num_rows ?? 0),
    version: metadata.version ?? 1,
    schema: metadata.schema ?? [],
  })

  // Select row groups that might contain matching data
  const selectedRowGroups = selectRowGroups(filter, rowGroupStats)

  // Create filter predicate
  const predicate = Object.keys(filter).length > 0 ? toPredicate(filter) : () => true

  // Calculate target count for early termination
  const targetCount = limit !== undefined ? skip + limit : Infinity

  let rowsSkipped = 0
  let rowsYielded = 0

  // Determine effective concurrency: use 1 when we have a small limit to minimize over-reading
  // This ensures we don't read 4 row groups when we only need 10 rows from the first one
  const hasSmallLimit = limit !== undefined && limit <= 100
  const effectiveConcurrency = hasSmallLimit ? 1 : concurrency

  // Process row groups sequentially or in small batches
  for (let i = 0; i < selectedRowGroups.length; i += effectiveConcurrency) {
    // Check for early termination before starting batch
    if (rowsYielded >= (limit ?? Infinity)) {
      stats.terminatedEarly = true
      break
    }

    const batch = selectedRowGroups.slice(i, i + effectiveConcurrency)

    // Read batch of row groups (possibly in parallel)
    const batchResults = await Promise.all(
      batch.map(async (rgIndex) => {
        const rowGroup = metadata.row_groups?.[rgIndex]
        if (!rowGroup) return []

        const groupStart = calculateGroupStart(metadata, rgIndex)
        const groupRows = Number(rowGroup.num_rows ?? 0)

        const rows = await parquetReadObjects({
          file: asyncBuffer,
          metadata,
          columns,
          rowStart: groupStart,
          rowEnd: groupStart + groupRows,
          compressors,
        })

        stats.rowGroupsRead++
        stats.rowsScanned += rows.length

        return rows as T[]
      })
    )

    // Yield rows from each row group in order
    for (const rows of batchResults) {
      for (const row of rows) {
        // Apply soft-delete filter
        if (!includeDeleted) {
          const r = row as Record<string, unknown>
          if (!isNullish(r.deletedAt)) {
            continue
          }
        }

        // Apply filter predicate
        if (!predicate(row)) {
          continue
        }

        // Handle skip
        if (rowsSkipped < skip) {
          rowsSkipped++
          continue
        }

        // Check limit
        if (rowsYielded >= (limit ?? Infinity)) {
          stats.terminatedEarly = true
          break
        }

        yield row
        rowsYielded++
        stats.rowsYielded++

        // Check for early termination after yield
        if (rowsYielded >= (limit ?? Infinity)) {
          stats.terminatedEarly = true
          break
        }
      }

      // Check for early termination between row groups
      if (rowsYielded >= (limit ?? Infinity)) {
        stats.terminatedEarly = true
        break
      }
    }
  }

  // Report final statistics
  if (onStats) {
    onStats(stats)
  }
}

/**
 * Stream with sorting - requires full materialization
 * This path is used when sort is specified
 */
async function* streamWithSort<T>(
  asyncBuffer: Awaited<ReturnType<typeof initializeAsyncBuffer>>,
  metadata: FileMetaData,
  options: StreamingQueryOptions<T>,
  stats: StreamingQueryStats
): AsyncGenerator<T, void, undefined> {
  const { filter = {}, columns, sort, limit, skip = 0, includeDeleted = false, onStats } = options

  // Must read all data for sorting
  const allRows: T[] = []

  const predicate = Object.keys(filter).length > 0 ? toPredicate(filter) : () => true

  // Read all row groups
  for (let rgIndex = 0; rgIndex < (metadata.row_groups?.length ?? 0); rgIndex++) {
    const rowGroup = metadata.row_groups?.[rgIndex]
    if (!rowGroup) continue

    const groupStart = calculateGroupStart(metadata, rgIndex)
    const groupRows = Number(rowGroup.num_rows ?? 0)

    const rows = await parquetReadObjects({
      file: asyncBuffer,
      metadata,
      columns,
      rowStart: groupStart,
      rowEnd: groupStart + groupRows,
      compressors,
    })

    stats.rowGroupsRead++
    stats.rowsScanned += rows.length

    // Filter and collect rows
    for (const row of rows as T[]) {
      // Apply soft-delete filter
      if (!includeDeleted) {
        const r = row as Record<string, unknown>
        if (!isNullish(r.deletedAt)) {
          continue
        }
      }

      if (predicate(row)) {
        allRows.push(row)
      }
    }
  }

  // Sort all rows
  if (sort) {
    sortRows(allRows, sort)
  }

  // Apply skip and limit, yield results
  let yielded = 0
  for (let i = skip; i < allRows.length; i++) {
    if (limit !== undefined && yielded >= limit) {
      break
    }

    yield allRows[i]!
    yielded++
    stats.rowsYielded++
  }

  // Sorting means we couldn't terminate early
  stats.terminatedEarly = false

  if (onStats) {
    onStats(stats)
  }
}

/**
 * Calculate the starting row index for a row group
 */
function calculateGroupStart(metadata: FileMetaData, rgIndex: number): number {
  let start = 0
  for (let i = 0; i < rgIndex; i++) {
    start += Number(metadata.row_groups?.[i]?.num_rows ?? 0)
  }
  return start
}

/**
 * Sort rows by sort specification
 */
function sortRows<T>(rows: T[], sort: SortSpec): void {
  const sortEntries = Object.entries(sort)
  if (sortEntries.length === 0) return

  rows.sort((a, b) => {
    for (const [field, direction] of sortEntries) {
      const aVal = getFieldValue(a, field)
      const bVal = getFieldValue(b, field)

      const cmp = compareValues(aVal, bVal)
      if (cmp !== 0) {
        const dir = direction === -1 || direction === 'desc' ? -1 : 1
        return cmp * dir
      }
    }
    return 0
  })
}

/**
 * Get field value with dot notation support
 */
function getFieldValue(obj: unknown, path: string): unknown {
  if (isNullish(obj)) return undefined

  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (isNullish(current)) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Compare two values for sorting
 */
function compareValues(a: unknown, b: unknown): number {
  if (isNullish(a)) return isNullish(b) ? 0 : -1
  if (isNullish(b)) return 1

  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b)
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
  if (typeof a === 'boolean' && typeof b === 'boolean') return (a ? 1 : 0) - (b ? 1 : 0)

  return String(a).localeCompare(String(b))
}

// =============================================================================
// Utility Exports
// =============================================================================

/**
 * Count rows matching a filter using streaming (memory-efficient)
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @param filter - Optional filter
 * @returns Count of matching rows
 */
export async function streamingCount(
  storage: StorageBackend,
  path: string,
  filter?: Filter
): Promise<number> {
  let count = 0
  for await (const _row of parquetQueryStream(storage, path, {
    filter,
    columns: ['$id'], // Minimal projection
  })) {
    count++
  }
  return count
}

/**
 * Check if any rows exist matching a filter (streaming)
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @param filter - Optional filter
 * @returns True if at least one row matches
 */
export async function streamingExists(
  storage: StorageBackend,
  path: string,
  filter?: Filter
): Promise<boolean> {
  for await (const _row of parquetQueryStream(storage, path, {
    filter,
    columns: ['$id'],
    limit: 1,
  })) {
    return true
  }
  return false
}

/**
 * Sum a numeric field using streaming
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @param field - Field to sum
 * @param filter - Optional filter
 * @returns Sum of the field values
 */
export async function streamingSum(
  storage: StorageBackend,
  path: string,
  field: string,
  filter?: Filter
): Promise<number> {
  let sum = 0
  for await (const row of parquetQueryStream<Record<string, unknown>>(storage, path, {
    filter,
    columns: [field],
  })) {
    const value = row[field]
    if (typeof value === 'number') {
      sum += value
    }
  }
  return sum
}

/**
 * Calculate average of a numeric field using streaming
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @param field - Field to average
 * @param filter - Optional filter
 * @returns Average of the field values, or null if no rows
 */
export async function streamingAvg(
  storage: StorageBackend,
  path: string,
  field: string,
  filter?: Filter
): Promise<number | null> {
  let sum = 0
  let count = 0
  for await (const row of parquetQueryStream<Record<string, unknown>>(storage, path, {
    filter,
    columns: [field],
  })) {
    const value = row[field]
    if (typeof value === 'number') {
      sum += value
      count++
    }
  }
  return count > 0 ? sum / count : null
}

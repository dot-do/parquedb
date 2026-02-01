/**
 * Parquet Reader using hyparquet
 *
 * Provides async Parquet file reading with:
 * - Metadata extraction
 * - Column projection
 * - Row group filtering
 * - Predicate pushdown
 * - Streaming iteration
 *
 * Works across Node.js, browsers, and Cloudflare Workers.
 */

import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from './compression'
import type { StorageBackend } from '../types/storage'
import type {
  ParquetReaderOptions,
  ReadOptions,
  ParquetMetadata,
  RowGroupMetadata,
  AsyncBuffer,
  RowFilter,
} from './types'

// =============================================================================
// AsyncBuffer Adapter
// =============================================================================

/**
 * Create an AsyncBuffer compatible with hyparquet from a StorageBackend
 *
 * This adapter allows hyparquet to read byte ranges from any storage backend
 * (filesystem, R2, S3, memory) using a consistent interface.
 *
 * @param storage - Storage backend for reading bytes
 * @param path - Path to the Parquet file
 * @returns AsyncBuffer compatible with hyparquet
 */
export function createAsyncBuffer(
  storage: StorageBackend,
  path: string
): AsyncBuffer {
  // Cache the file size to avoid repeated stat calls
  let cachedByteLength: number | undefined

  return {
    get byteLength(): number {
      // hyparquet expects byteLength to be synchronous
      // We'll initialize it before use with initializeAsyncBuffer
      if (cachedByteLength === undefined) {
        throw new Error(
          'AsyncBuffer not initialized. Call initializeAsyncBuffer first.'
        )
      }
      return cachedByteLength
    },

    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? cachedByteLength ?? 0
      const data = await storage.readRange(path, start, effectiveEnd)
      // Convert Uint8Array to ArrayBuffer
      // Create a new ArrayBuffer to avoid SharedArrayBuffer issues
      const buffer = new ArrayBuffer(data.byteLength)
      new Uint8Array(buffer).set(data)
      return buffer
    },
  }
}

/**
 * Initialize an AsyncBuffer by fetching the file size
 *
 * @param storage - Storage backend
 * @param path - Path to the Parquet file
 * @returns Initialized AsyncBuffer
 */
export async function initializeAsyncBuffer(
  storage: StorageBackend,
  path: string
): Promise<AsyncBuffer> {
  const stat = await storage.stat(path)
  if (!stat) {
    throw new Error(`File not found: ${path}`)
  }

  const byteLength = stat.size

  return {
    byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? byteLength
      const data = await storage.readRange(path, start, effectiveEnd)
      // Create a new ArrayBuffer to avoid SharedArrayBuffer issues
      const buffer = new ArrayBuffer(data.byteLength)
      new Uint8Array(buffer).set(data)
      return buffer
    },
  }
}

// =============================================================================
// ParquetReader Class
// =============================================================================

/**
 * Parquet file reader using hyparquet
 *
 * Provides efficient reading of Parquet files with support for:
 * - Column projection (read only needed columns)
 * - Row group filtering (skip irrelevant row groups)
 * - Predicate pushdown (filter using column statistics)
 * - Streaming iteration (memory-efficient row-by-row access)
 *
 * @example
 * ```typescript
 * const reader = new ParquetReader({ storage: myStorage })
 *
 * // Read all data
 * const data = await reader.read<Post>('data/posts/data.parquet')
 *
 * // Read with options
 * const filtered = await reader.read<Post>('data/posts/data.parquet', {
 *   columns: ['$id', 'title', 'status'],
 *   limit: 100
 * })
 *
 * // Stream rows
 * for await (const row of reader.stream<Post>('data/posts/data.parquet')) {
 *   console.log(row)
 * }
 * ```
 */
export class ParquetReader {
  private storage: StorageBackend
  private defaultColumns?: string[]
  private defaultRowGroups?: number[]

  /**
   * Create a new ParquetReader
   *
   * @param options - Reader configuration
   */
  constructor(options: ParquetReaderOptions) {
    this.storage = options.storage
    this.defaultColumns = options.columns
    this.defaultRowGroups = options.rowGroups
  }

  /**
   * Read Parquet file metadata
   *
   * Returns schema information, row group metadata, and file statistics
   * without reading the actual data.
   *
   * @param path - Path to the Parquet file
   * @returns Parquet file metadata
   */
  async readMetadata(path: string): Promise<ParquetMetadata> {
    const asyncBuffer = await initializeAsyncBuffer(this.storage, path)
    const metadata = await parquetMetadataAsync(asyncBuffer)
    return this.convertMetadata(metadata)
  }

  /**
   * Read Parquet data
   *
   * @param path - Path to the Parquet file
   * @param options - Read options
   * @returns Array of rows
   */
  async read<T = Record<string, unknown>>(
    path: string,
    options: ReadOptions = {}
  ): Promise<T[]> {
    const asyncBuffer = await initializeAsyncBuffer(this.storage, path)
    const columns = options.columns ?? this.defaultColumns
    const rowGroups = options.rowGroups ?? this.defaultRowGroups

    // Build read options for hyparquet
    // Include compressors for LZ4, GZIP, ZSTD, and Brotli support
    const readOptions: any = {
      file: asyncBuffer,
      compressors,
    }

    if (columns && columns.length > 0) {
      readOptions.columns = columns
    }

    if (rowGroups && rowGroups.length > 0) {
      readOptions.rowGroups = rowGroups
    }

    // Read data using parquetReadObjects which returns row objects directly
    const rows = await parquetReadObjects(readOptions) as T[]

    // Apply post-read filtering if needed
    let filteredRows = rows
    if (options.filter) {
      filteredRows = this.applyFilter(rows, options.filter)
    }

    // Apply offset and limit
    if (options.offset !== undefined || options.limit !== undefined) {
      const start = options.offset ?? 0
      const end = options.limit !== undefined ? start + options.limit : undefined
      filteredRows = filteredRows.slice(start, end)
    }

    return filteredRows
  }

  /**
   * Stream Parquet rows
   *
   * Yields rows one at a time for memory-efficient processing.
   * Note: hyparquet reads row groups at a time, so this provides
   * row-by-row iteration over those chunks.
   *
   * @param path - Path to the Parquet file
   * @param options - Read options
   * @yields Individual rows
   */
  async *stream<T = Record<string, unknown>>(
    path: string,
    options: ReadOptions = {}
  ): AsyncGenerator<T, void, unknown> {
    const asyncBuffer = await initializeAsyncBuffer(this.storage, path)
    const metadata = await parquetMetadataAsync(asyncBuffer)
    const columns = options.columns ?? this.defaultColumns
    const rowGroups = options.rowGroups ?? this.defaultRowGroups

    // Determine which row groups to read
    const groupsToRead = rowGroups ?? Array.from(
      { length: metadata.row_groups?.length ?? 0 },
      (_, i) => i
    )

    let rowCount = 0
    const offset = options.offset ?? 0
    const limit = options.limit

    for (const groupIndex of groupsToRead) {
      // Check if we've reached the limit
      if (limit !== undefined && rowCount >= limit) {
        break
      }

      // Read single row group
      // Include compressors for LZ4, GZIP, ZSTD, and Brotli support
      const readOptions: any = {
        file: asyncBuffer,
        rowGroups: [groupIndex],
        compressors,
      }

      if (columns && columns.length > 0) {
        readOptions.columns = columns
      }

      const rows = await parquetReadObjects(readOptions) as T[]

      for (const row of rows) {
        // Skip rows until we reach offset
        if (rowCount < offset) {
          rowCount++
          continue
        }

        // Check limit
        if (limit !== undefined && rowCount - offset >= limit) {
          return
        }

        // Apply filter
        if (options.filter && !this.matchesFilter(row, options.filter)) {
          continue
        }

        yield row
        rowCount++
      }
    }
  }

  /**
   * Read specific row groups only
   *
   * Useful when you know which row groups contain relevant data
   * (e.g., from statistics-based filtering).
   *
   * @param path - Path to the Parquet file
   * @param groups - Row group indices to read
   * @returns Array of rows from specified row groups
   */
  async readRowGroups<T = Record<string, unknown>>(
    path: string,
    groups: number[]
  ): Promise<T[]> {
    return this.read<T>(path, { rowGroups: groups })
  }

  /**
   * Check if row groups can be skipped based on filter and statistics
   *
   * @param metadata - File metadata
   * @param filter - Row filter
   * @returns Indices of row groups that may contain matching rows
   */
  getRelevantRowGroups(
    metadata: ParquetMetadata,
    filter: RowFilter
  ): number[] {
    const relevantGroups: number[] = []

    for (let i = 0; i < metadata.rowGroups.length; i++) {
      const rowGroup = metadata.rowGroups[i]

      // Find the column statistics
      const columnMeta = rowGroup.columns.find(
        (col) => col.pathInSchema.join('.') === filter.column ||
                 col.pathInSchema[col.pathInSchema.length - 1] === filter.column
      )

      if (!columnMeta?.statistics) {
        // No statistics, must include this row group
        relevantGroups.push(i)
        continue
      }

      const stats = columnMeta.statistics
      const { min, max } = stats

      // Check if row group can be skipped based on statistics
      if (this.canSkipRowGroup(filter, min, max)) {
        continue
      }

      relevantGroups.push(i)
    }

    return relevantGroups
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Convert hyparquet metadata to our format
   */
  private convertMetadata(metadata: any): ParquetMetadata {
    return {
      version: metadata.version ?? 1,
      schema: metadata.schema ?? [],
      numRows: metadata.num_rows ?? 0,
      rowGroups: (metadata.row_groups ?? []).map((rg: any, index: number) => ({
        numRows: rg.num_rows ?? 0,
        totalByteSize: rg.total_byte_size ?? 0,
        columns: (rg.columns ?? []).map((col: any) => ({
          pathInSchema: col.meta_data?.path_in_schema ?? [],
          totalCompressedSize: col.meta_data?.total_compressed_size ?? 0,
          totalUncompressedSize: col.meta_data?.total_uncompressed_size ?? 0,
          numValues: col.meta_data?.num_values ?? 0,
          encodings: col.meta_data?.encodings ?? [],
          codec: col.meta_data?.codec ?? 'UNCOMPRESSED',
          statistics: col.meta_data?.statistics
            ? {
                min: col.meta_data.statistics.min_value,
                max: col.meta_data.statistics.max_value,
                nullCount: col.meta_data.statistics.null_count,
                distinctCount: col.meta_data.statistics.distinct_count,
              }
            : undefined,
        })),
        ordinal: index,
      })),
      keyValueMetadata: metadata.key_value_metadata?.map((kv: any) => ({
        key: kv.key,
        value: kv.value,
      })),
      createdBy: metadata.created_by,
    }
  }

  /**
   * Convert column data to row objects
   */
  private convertToRows<T>(
    result: any,
    columns?: string[]
  ): T[] {
    // hyparquet returns data in column format
    // We need to convert to row format

    if (!result || typeof result !== 'object') {
      return []
    }

    // Get all column names
    const columnNames = columns ?? Object.keys(result)

    if (columnNames.length === 0) {
      return []
    }

    // Get the number of rows from the first column
    const firstColumn = result[columnNames[0]]
    if (!firstColumn || !Array.isArray(firstColumn)) {
      return []
    }

    const numRows = firstColumn.length
    const rows: T[] = []

    for (let i = 0; i < numRows; i++) {
      const row: Record<string, unknown> = {}
      for (const colName of columnNames) {
        if (colName in result) {
          row[colName] = result[colName][i]
        }
      }
      rows.push(row as T)
    }

    return rows
  }

  /**
   * Apply filter to rows
   */
  private applyFilter<T>(rows: T[], filter: RowFilter): T[] {
    return rows.filter((row) => this.matchesFilter(row, filter))
  }

  /**
   * Check if a row matches a filter
   */
  private matchesFilter<T>(row: T, filter: RowFilter): boolean {
    const value = (row as Record<string, unknown>)[filter.column]

    switch (filter.op) {
      case 'eq':
        return value === filter.value
      case 'ne':
        return value !== filter.value
      case 'gt':
        return value !== null && value !== undefined && (value as number | string | Date) > (filter.value as number | string | Date)
      case 'gte':
        return value !== null && value !== undefined && (value as number | string | Date) >= (filter.value as number | string | Date)
      case 'lt':
        return value !== null && value !== undefined && (value as number | string | Date) < (filter.value as number | string | Date)
      case 'lte':
        return value !== null && value !== undefined && (value as number | string | Date) <= (filter.value as number | string | Date)
      case 'in':
        return Array.isArray(filter.value) && filter.value.includes(value)
      case 'isNull':
        return value === null || value === undefined
      case 'isNotNull':
        return value !== null && value !== undefined
      default:
        return true
    }
  }

  /**
   * Check if a row group can be skipped based on statistics
   */
  private canSkipRowGroup(
    filter: RowFilter,
    min: unknown,
    max: unknown
  ): boolean {
    if (min === undefined || max === undefined || min === null || max === null) {
      return false // Can't skip without statistics
    }

    const filterVal = filter.value as number | string

    switch (filter.op) {
      case 'eq':
        // Skip if value is outside min/max range
        return filterVal < min || filterVal > max
      case 'ne':
        // Can't reliably skip for not-equal
        return false
      case 'gt':
        // Skip if max is not greater than filter value
        return (max as number | string) <= filterVal
      case 'gte':
        // Skip if max is less than filter value
        return (max as number | string) < filterVal
      case 'lt':
        // Skip if min is not less than filter value
        return (min as number | string) >= filterVal
      case 'lte':
        // Skip if min is greater than filter value
        return (min as number | string) > filterVal
      case 'in':
        // Can't reliably skip for in
        return false
      case 'isNull':
        // Would need null count statistics
        return false
      case 'isNotNull':
        // Would need null count statistics
        return false
      default:
        return false
    }
  }
}

// =============================================================================
// Standalone Functions
// =============================================================================

/**
 * Read Parquet file metadata
 *
 * Convenience function for one-off metadata reads.
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @returns Parquet metadata
 */
export async function readParquetMetadata(
  storage: StorageBackend,
  path: string
): Promise<ParquetMetadata> {
  const reader = new ParquetReader({ storage })
  return reader.readMetadata(path)
}

/**
 * Read Parquet file
 *
 * Convenience function for one-off reads.
 *
 * @param storage - Storage backend
 * @param path - Path to Parquet file
 * @param options - Read options
 * @returns Array of rows
 */
export async function readParquet<T = Record<string, unknown>>(
  storage: StorageBackend,
  path: string,
  options?: ReadOptions
): Promise<T[]> {
  const reader = new ParquetReader({ storage })
  return reader.read<T>(path, options)
}

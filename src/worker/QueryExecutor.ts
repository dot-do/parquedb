/**
 * QueryExecutor - Query execution on R2 for ParqueDB
 *
 * Executes queries against Parquet files stored in R2:
 * - Predicate pushdown for efficient filtering
 * - Bloom filter integration for fast negative lookups
 * - Parallel row group processing
 * - Result post-processing (sort, limit, project)
 */

import type { Filter, FieldFilter, FieldOperator } from '../types/filter'
import type {
  FindOptions,
  GetOptions,
  SortSpec,
  Projection,
} from '../types/options'
import type { EntityRecord, PaginatedResult } from '../types/entity'
import type { StorageBackend, FileStat, ListResult } from '../types/storage'
import { ReadPath, NotFoundError } from './ReadPath'
import { ParquetReader } from '../parquet/reader'

// =============================================================================
// Types
// =============================================================================

/**
 * Parquet file metadata
 */
export interface ParquetMetadata {
  /** Number of rows in the file */
  numRows: number
  /** Row groups in the file */
  rowGroups: RowGroupMetadata[]
  /** Column metadata */
  columns: ColumnMetadata[]
  /** File schema */
  schema: ParquetSchema
  /** File-level key-value metadata */
  keyValueMetadata?: Record<string, string>
}

/**
 * Row group metadata for predicate pushdown
 */
export interface RowGroupMetadata {
  /** Row group index */
  index: number
  /** Number of rows in this group */
  numRows: number
  /** Byte offset in file */
  offset: number
  /** Compressed size in bytes */
  compressedSize: number
  /** Uncompressed size in bytes */
  totalSize: number
  /** Column statistics for predicate pushdown */
  columnStats: Record<string, ColumnStats>
}

/**
 * Column statistics for predicate pushdown
 */
export interface ColumnStats {
  /** Column name */
  name: string
  /** Minimum value (if available) */
  min?: unknown
  /** Maximum value (if available) */
  max?: unknown
  /** Number of null values */
  nullCount: number
  /** Number of distinct values (if available) */
  distinctCount?: number
  /** Whether statistics are valid */
  hasStats: boolean
}

/**
 * Column metadata
 */
export interface ColumnMetadata {
  /** Column name */
  name: string
  /** Parquet physical type */
  physicalType: string
  /** Logical type */
  logicalType?: string
  /** Encoding */
  encoding: string
  /** Compression codec */
  compression: string
}

/**
 * Parquet schema (simplified)
 */
export interface ParquetSchema {
  /** Schema fields */
  fields: SchemaField[]
}

/**
 * Schema field
 */
export interface SchemaField {
  /** Field name */
  name: string
  /** Field type */
  type: string
  /** Whether field is nullable */
  nullable: boolean
  /** Nested fields (for struct types) */
  fields?: SchemaField[]
}

/**
 * Bloom filter for fast negative lookups
 */
export interface BloomFilter {
  /** Check if value might be in the set */
  mightContain(value: string): boolean
  /** Estimated false positive rate */
  falsePositiveRate: number
  /** Number of bits in filter */
  numBits: number
  /** Number of hash functions */
  numHashFunctions: number
}

/**
 * Query execution result
 */
export interface FindResult<T> {
  /** Matching documents */
  items: T[]
  /** Total count (if calculated) */
  total?: number
  /** Cursor for next page */
  nextCursor?: string
  /** Whether there are more results */
  hasMore: boolean
  /** Query execution stats */
  stats: QueryStats
}

/**
 * Query execution statistics
 */
export interface QueryStats {
  /** Number of row groups scanned */
  rowGroupsScanned: number
  /** Number of row groups skipped by predicate pushdown */
  rowGroupsSkipped: number
  /** Number of rows scanned */
  rowsScanned: number
  /** Number of rows returned */
  rowsReturned: number
  /** Bytes read from storage */
  bytesRead: number
  /** Execution time in milliseconds */
  executionTimeMs: number
  /** Whether bloom filter was used */
  usedBloomFilter: boolean
  /** Whether cache was hit */
  cacheHit: boolean
}

/**
 * Query plan for explain
 */
export interface QueryPlan {
  /** Filter after optimization */
  optimizedFilter: Filter
  /** Row groups selected for scanning */
  selectedRowGroups: number[]
  /** Columns needed for query */
  requiredColumns: string[]
  /** Whether bloom filter will be used */
  useBloomFilter: boolean
  /** Estimated rows to scan */
  estimatedRows: number
  /** Estimated bytes to read */
  estimatedBytes: number
}

// =============================================================================
// QueryExecutor Implementation
// =============================================================================

/**
 * Query executor for ParqueDB
 *
 * Executes queries against Parquet files with:
 * - Predicate pushdown using row group statistics
 * - Bloom filter integration for fast ID lookups
 * - Parallel row group processing
 * - Projection pushdown
 * - Post-processing (filter, sort, limit)
 *
 * @example
 * ```typescript
 * const executor = new QueryExecutor(readPath)
 *
 * // Find with filter
 * const posts = await executor.find<Post>('posts', { status: 'published' }, { limit: 10 })
 *
 * // Get single entity
 * const post = await executor.get<Post>('posts', 'abc123')
 *
 * // Explain query plan
 * const plan = await executor.explain('posts', { score: { $gte: 100 } })
 * ```
 */
/**
 * Simple R2 storage adapter for ParquetReader
 * Implements only the methods needed for Parquet reading
 */
class R2StorageAdapter implements Partial<StorageBackend> {
  readonly type = 'r2-adapter'

  constructor(private bucket: R2Bucket) {}

  async read(path: string): Promise<Uint8Array> {
    const obj = await this.bucket.get(path)
    if (!obj) throw new Error(`Object not found: ${path}`)
    return new Uint8Array(await obj.arrayBuffer())
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const obj = await this.bucket.get(path, {
      range: { offset: start, length: end - start },
    })
    if (!obj) throw new Error(`Object not found: ${path}`)
    return new Uint8Array(await obj.arrayBuffer())
  }

  async exists(path: string): Promise<boolean> {
    const head = await this.bucket.head(path)
    return head !== null
  }

  async stat(path: string): Promise<FileStat | null> {
    const head = await this.bucket.head(path)
    if (!head) return null
    return {
      path,
      size: head.size,
      mtime: head.uploaded,
      isDirectory: false,
    }
  }

  async list(prefix: string): Promise<ListResult> {
    const result = await this.bucket.list({ prefix, limit: 1000 })
    return {
      files: result.objects.map(obj => obj.key),
      hasMore: result.truncated,
      stats: result.objects.map(obj => ({
        path: obj.key,
        size: obj.size,
        mtime: obj.uploaded,
        isDirectory: false,
      })),
    }
  }

  // Write operations not needed for reading
  async write(): Promise<never> { throw new Error('Not implemented') }
  async writeAtomic(): Promise<never> { throw new Error('Not implemented') }
  async append(): Promise<never> { throw new Error('Not implemented') }
  async delete(): Promise<never> { throw new Error('Not implemented') }
  async deletePrefix(): Promise<never> { throw new Error('Not implemented') }
  async mkdir(): Promise<never> { throw new Error('Not implemented') }
  async rmdir(): Promise<never> { throw new Error('Not implemented') }
  async copy(): Promise<never> { throw new Error('Not implemented') }
  async move(): Promise<never> { throw new Error('Not implemented') }
}

export class QueryExecutor {
  /** Cache of loaded metadata per namespace */
  private metadataCache = new Map<string, ParquetMetadata>()

  /** Cache of loaded bloom filters per namespace */
  private bloomCache = new Map<string, BloomFilter>()

  /** R2 storage adapter for ParquetReader */
  private storageAdapter: R2StorageAdapter | null = null

  /** ParquetReader instance */
  private parquetReader: ParquetReader | null = null

  constructor(private readPath: ReadPath, private bucket?: R2Bucket) {
    if (bucket) {
      this.storageAdapter = new R2StorageAdapter(bucket)
      this.parquetReader = new ParquetReader({ storage: this.storageAdapter as unknown as StorageBackend })
    }
  }

  // ===========================================================================
  // Find Operations
  // ===========================================================================

  /**
   * Execute find query against Parquet files
   *
   * Uses ParquetReader for actual data reading when available.
   * Falls back to legacy metadata-based approach otherwise.
   *
   * @param ns - Namespace to query
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @returns Query results with stats
   */
  async find<T = EntityRecord>(
    ns: string,
    filter: Filter,
    options: FindOptions<T> = {}
  ): Promise<FindResult<T>> {
    const startTime = performance.now()
    const stats: QueryStats = {
      rowGroupsScanned: 0,
      rowGroupsSkipped: 0,
      rowsScanned: 0,
      rowsReturned: 0,
      bytesRead: 0,
      executionTimeMs: 0,
      usedBloomFilter: false,
      cacheHit: false,
    }

    try {
      const path = `data/${ns}/data.parquet`

      // Use ParquetReader when available (real implementation)
      if (this.parquetReader && this.storageAdapter) {
        // Read metadata for stats
        const metadata = await this.parquetReader.readMetadata(path)
        stats.rowGroupsScanned = metadata.rowGroups.length

        // Read all data using ParquetReader
        let results = await this.parquetReader.read<T>(path)
        stats.rowsScanned = results.length

        // Apply MongoDB-style filter
        results = this.applyFilter(results, filter)

        // Post-process: sort, skip, limit, project
        const processed = this.postProcess(results, options)

        stats.rowsReturned = processed.items.length
        stats.executionTimeMs = performance.now() - startTime

        return {
          items: processed.items,
          total: processed.total,
          nextCursor: processed.nextCursor,
          hasMore: processed.hasMore,
          stats,
        }
      }

      // Legacy path: use metadata-based approach (placeholder implementation)
      // 1. Load Parquet metadata (cached)
      const metadata = await this.loadMetadata(ns)

      // 2. Determine which row groups to read based on filter
      const selectedRowGroups = this.selectRowGroups(metadata, filter)
      stats.rowGroupsSkipped = metadata.rowGroups.length - selectedRowGroups.length
      stats.rowGroupsScanned = selectedRowGroups.length

      // 3. Read only necessary row groups (parallel)
      const rowGroupData = await Promise.all(
        selectedRowGroups.map((rg) => this.readRowGroup(ns, rg, metadata))
      )

      // Track bytes read
      stats.bytesRead = rowGroupData.reduce((sum, rg) => sum + (rg?.bytesRead ?? 0), 0)

      // 4. Parse and combine results
      let results: T[] = rowGroupData
        .filter((rg) => rg !== null)
        .flatMap((rg) => rg!.records as T[])

      stats.rowsScanned = results.length

      // 5. Apply remaining filters (for complex filters not pushed down)
      results = this.applyFilter(results, filter)

      // 6. Post-process: sort, skip, limit, project
      const processed = this.postProcess(results, options)

      stats.rowsReturned = processed.items.length
      stats.executionTimeMs = performance.now() - startTime

      return {
        items: processed.items,
        total: processed.total,
        nextCursor: processed.nextCursor,
        hasMore: processed.hasMore,
        stats,
      }
    } catch (error) {
      if (error instanceof NotFoundError) {
        // No data file exists - return empty result
        stats.executionTimeMs = performance.now() - startTime
        return {
          items: [],
          hasMore: false,
          stats,
        }
      }
      throw error
    }
  }

  /**
   * Get single entity by ID
   *
   * Uses bloom filter for fast negative lookups:
   * 1. Check bloom filter (very fast)
   * 2. If might contain, read from Parquet
   *
   * @param ns - Namespace
   * @param id - Entity ID
   * @param options - Get options
   * @returns Entity or null if not found
   */
  async get<T = EntityRecord>(
    ns: string,
    id: string,
    _options: GetOptions = {}
  ): Promise<T | null> {
    // Check bloom filter first (very fast)
    const bloom = await this.loadBloomFilter(ns)
    if (bloom && !bloom.mightContain(id)) {
      // Definitely not in the set
      return null
    }

    // Read from Parquet
    const result = await this.find<T>(ns, { id: { $eq: id } }, { limit: 1 })

    if (result.stats.usedBloomFilter !== undefined) {
      result.stats.usedBloomFilter = bloom !== null
    }

    return result.items[0] ?? null
  }

  /**
   * Count documents matching filter
   *
   * @param ns - Namespace
   * @param filter - Filter to apply
   * @returns Count of matching documents
   */
  async count(ns: string, filter: Filter = {}): Promise<number> {
    // For simple filters, we might be able to use row group stats
    const metadata = await this.loadMetadata(ns)

    // If no filter, return total row count
    if (Object.keys(filter).length === 0) {
      return metadata.numRows
    }

    // Otherwise, need to scan
    const result = await this.find(ns, filter, { limit: Infinity })
    return result.items.length
  }

  /**
   * Check if any document matches filter
   *
   * @param ns - Namespace
   * @param filter - Filter to apply
   * @returns true if at least one match exists
   */
  async exists(ns: string, filter: Filter): Promise<boolean> {
    const result = await this.find(ns, filter, { limit: 1 })
    return result.items.length > 0
  }

  // ===========================================================================
  // Query Planning
  // ===========================================================================

  /**
   * Explain query plan without executing
   *
   * @param ns - Namespace
   * @param filter - Filter to analyze
   * @param options - Query options
   * @returns Query plan
   */
  async explain(
    ns: string,
    filter: Filter,
    options: FindOptions = {}
  ): Promise<QueryPlan> {
    const metadata = await this.loadMetadata(ns)
    const selectedRowGroups = this.selectRowGroups(metadata, filter)

    // Determine required columns from filter and projection
    const requiredColumns = this.getRequiredColumns(filter, options.project)

    // Estimate rows and bytes
    const estimatedRows = selectedRowGroups.reduce(
      (sum, rg) => sum + rg.numRows,
      0
    )
    const estimatedBytes = selectedRowGroups.reduce(
      (sum, rg) => sum + rg.compressedSize,
      0
    )

    // Check if bloom filter can be used
    const useBloomFilter = this.canUseBloomFilter(filter)

    return {
      optimizedFilter: filter,
      selectedRowGroups: selectedRowGroups.map((rg) => rg.index),
      requiredColumns,
      useBloomFilter,
      estimatedRows,
      estimatedBytes,
    }
  }

  // ===========================================================================
  // Metadata Loading
  // ===========================================================================

  /**
   * Load Parquet metadata for a namespace
   *
   * Metadata is cached in memory for repeated queries.
   *
   * @param ns - Namespace
   * @returns Parquet metadata
   */
  async loadMetadata(ns: string): Promise<ParquetMetadata> {
    // Check cache first
    const cached = this.metadataCache.get(ns)
    if (cached) {
      return cached
    }

    // Read from R2
    const path = `data/${ns}/data.parquet`

    // Read footer to get metadata length
    const footer = await this.readPath.readParquetFooter(path)
    const metadataLength = this.parseMetadataLength(footer)

    // Read and parse metadata
    const metadataBytes = await this.readPath.readParquetMetadata(path, metadataLength)
    const metadata = this.parseMetadata(metadataBytes)

    // Cache for future queries
    this.metadataCache.set(ns, metadata)

    return metadata
  }

  /**
   * Load bloom filter for a namespace
   *
   * @param ns - Namespace
   * @returns Bloom filter or null if not available
   */
  async loadBloomFilter(ns: string): Promise<BloomFilter | null> {
    // Check cache first
    const cached = this.bloomCache.get(ns)
    if (cached) {
      return cached
    }

    try {
      const path = `indexes/bloom/${ns}.bloom`
      const data = await this.readPath.readParquet(path, { type: 'bloom' })
      const bloom = this.parseBloomFilter(data)

      // Cache for future queries
      this.bloomCache.set(ns, bloom)

      return bloom
    } catch (error) {
      if (error instanceof NotFoundError) {
        return null
      }
      throw error
    }
  }

  /**
   * Invalidate cached metadata for a namespace
   *
   * Called after writes to ensure fresh data.
   *
   * @param ns - Namespace to invalidate
   */
  invalidateCache(ns: string): void {
    this.metadataCache.delete(ns)
    this.bloomCache.delete(ns)
  }

  // ===========================================================================
  // Row Group Selection (Predicate Pushdown)
  // ===========================================================================

  /**
   * Select row groups that might contain matching rows
   *
   * Uses column statistics for predicate pushdown.
   *
   * @param metadata - Parquet metadata
   * @param filter - Query filter
   * @returns Row groups that need to be scanned
   */
  private selectRowGroups(
    metadata: ParquetMetadata,
    filter: Filter
  ): RowGroupMetadata[] {
    return metadata.rowGroups.filter((rg) => this.rowGroupMightMatch(rg, filter))
  }

  /**
   * Check if a row group might contain matching rows
   *
   * @param rowGroup - Row group metadata
   * @param filter - Query filter
   * @returns true if row group might contain matches
   */
  private rowGroupMightMatch(rowGroup: RowGroupMetadata, filter: Filter): boolean {
    // Check each field filter against column statistics
    for (const [field, condition] of Object.entries(filter)) {
      // Skip logical operators
      if (field.startsWith('$')) continue

      const stats = rowGroup.columnStats[field]
      if (!stats || !stats.hasStats) continue

      // Check if condition can be satisfied by this row group
      if (!this.conditionMightMatch(stats, condition)) {
        return false
      }
    }

    return true
  }

  /**
   * Check if a condition might match given column statistics
   *
   * @param stats - Column statistics
   * @param condition - Filter condition
   * @returns true if condition might match
   */
  private conditionMightMatch(stats: ColumnStats, condition: FieldFilter): boolean {
    // Handle direct equality
    if (typeof condition !== 'object' || condition === null) {
      // Equality: value must be within [min, max]
      if (stats.min !== undefined && stats.min !== null &&
          stats.max !== undefined && stats.max !== null) {
        const minNum = stats.min as number
        const maxNum = stats.max as number
        const condNum = condition as number
        return condNum >= minNum && condNum <= maxNum
      }
      return true
    }

    const operator = condition as FieldOperator

    // Handle comparison operators
    if ('$eq' in operator) {
      if (stats.min !== undefined && stats.min !== null &&
          stats.max !== undefined && stats.max !== null) {
        const eqVal = operator.$eq as number
        const minNum = stats.min as number
        const maxNum = stats.max as number
        return eqVal >= minNum && eqVal <= maxNum
      }
    }

    if ('$gt' in operator) {
      // Value must be > condition, so max must be > condition
      if (stats.max !== undefined && stats.max !== null) {
        const gtVal = operator.$gt as number
        const maxNum = stats.max as number
        return maxNum > gtVal
      }
    }

    if ('$gte' in operator) {
      if (stats.max !== undefined && stats.max !== null) {
        const gteVal = operator.$gte as number
        const maxNum = stats.max as number
        return maxNum >= gteVal
      }
    }

    if ('$lt' in operator) {
      // Value must be < condition, so min must be < condition
      if (stats.min !== undefined && stats.min !== null) {
        const ltVal = operator.$lt as number
        const minNum = stats.min as number
        return minNum < ltVal
      }
    }

    if ('$lte' in operator) {
      if (stats.min !== undefined && stats.min !== null) {
        const lteVal = operator.$lte as number
        const minNum = stats.min as number
        return minNum <= lteVal
      }
    }

    if ('$in' in operator) {
      // At least one value must be in range
      const values = operator.$in as number[]
      if (stats.min !== undefined && stats.min !== null &&
          stats.max !== undefined && stats.max !== null) {
        const minNum = stats.min as number
        const maxNum = stats.max as number
        return values.some((v) => v >= minNum && v <= maxNum)
      }
    }

    // For other operators, assume might match
    return true
  }

  // ===========================================================================
  // Row Group Reading
  // ===========================================================================

  /**
   * Read and parse a row group
   *
   * @param ns - Namespace
   * @param rowGroup - Row group metadata
   * @param metadata - File metadata
   * @returns Parsed records and bytes read
   */
  private async readRowGroup(
    ns: string,
    rowGroup: RowGroupMetadata,
    metadata: ParquetMetadata
  ): Promise<{ records: EntityRecord[]; bytesRead: number } | null> {
    const path = `data/${ns}/data.parquet`

    // Read row group bytes
    const data = await this.readPath.readRange(
      path,
      rowGroup.offset,
      rowGroup.offset + rowGroup.compressedSize
    )

    // Parse row group (placeholder - actual implementation would use Parquet parser)
    const records = this.parseRowGroup(data, rowGroup, metadata)

    return {
      records,
      bytesRead: data.byteLength,
    }
  }

  // ===========================================================================
  // Filtering
  // ===========================================================================

  /**
   * Apply filter to records (for complex filters not pushed down)
   *
   * @param records - Records to filter
   * @param filter - Filter to apply
   * @returns Filtered records
   */
  private applyFilter<T>(records: T[], filter: Filter): T[] {
    if (Object.keys(filter).length === 0) {
      return records
    }

    return records.filter((record) => this.matchesFilter(record, filter))
  }

  /**
   * Check if a record matches a filter
   *
   * @param record - Record to check
   * @param filter - Filter to match
   * @returns true if record matches
   */
  private matchesFilter(record: unknown, filter: Filter): boolean {
    const rec = record as Record<string, unknown>

    // Handle logical operators
    if (filter.$and) {
      return filter.$and.every((f) => this.matchesFilter(record, f))
    }

    if (filter.$or) {
      return filter.$or.some((f) => this.matchesFilter(record, f))
    }

    if (filter.$not) {
      return !this.matchesFilter(record, filter.$not)
    }

    if (filter.$nor) {
      return !filter.$nor.some((f) => this.matchesFilter(record, f))
    }

    // Handle field filters
    for (const [field, condition] of Object.entries(filter)) {
      if (field.startsWith('$')) continue

      const value = rec[field]
      if (!this.matchesCondition(value, condition)) {
        return false
      }
    }

    return true
  }

  /**
   * Check if a value matches a condition
   *
   * @param value - Value to check
   * @param condition - Condition to match
   * @returns true if value matches
   */
  private matchesCondition(value: unknown, condition: FieldFilter): boolean {
    // Handle direct equality
    if (typeof condition !== 'object' || condition === null) {
      return value === condition
    }

    const operator = condition as FieldOperator

    // Handle comparison operators
    if ('$eq' in operator) return value === operator.$eq
    if ('$ne' in operator) return value !== operator.$ne
    if ('$gt' in operator) return (value as number) > (operator.$gt as number)
    if ('$gte' in operator) return (value as number) >= (operator.$gte as number)
    if ('$lt' in operator) return (value as number) < (operator.$lt as number)
    if ('$lte' in operator) return (value as number) <= (operator.$lte as number)
    if ('$in' in operator) return (operator.$in as unknown[]).includes(value)
    if ('$nin' in operator) return !(operator.$nin as unknown[]).includes(value)

    // Handle string operators
    if ('$regex' in operator) {
      const regex = new RegExp(operator.$regex as string, operator.$options)
      return regex.test(value as string)
    }
    if ('$startsWith' in operator) {
      return (value as string).startsWith(operator.$startsWith)
    }
    if ('$endsWith' in operator) {
      return (value as string).endsWith(operator.$endsWith)
    }
    if ('$contains' in operator) {
      return (value as string).includes(operator.$contains)
    }

    // Handle array operators
    if ('$all' in operator) {
      const arr = value as unknown[]
      return (operator.$all as unknown[]).every((v) => arr.includes(v))
    }
    if ('$size' in operator) {
      return (value as unknown[]).length === operator.$size
    }

    // Handle existence operators
    if ('$exists' in operator) {
      return operator.$exists ? value !== undefined : value === undefined
    }

    return true
  }

  // ===========================================================================
  // Post-Processing
  // ===========================================================================

  /**
   * Post-process results (sort, skip, limit, project)
   *
   * @param results - Results to process
   * @param options - Query options
   * @returns Processed results
   */
  private postProcess<T>(
    results: T[],
    options: FindOptions<T>
  ): PaginatedResult<T> {
    let processed = [...results]

    // Sort
    if (options.sort) {
      processed = this.applySort(processed, options.sort)
    }

    // Calculate total before pagination
    const total = processed.length

    // Skip
    if (options.skip && options.skip > 0) {
      processed = processed.slice(options.skip)
    }

    // Limit
    let hasMore = false
    if (options.limit && options.limit < processed.length) {
      hasMore = true
      processed = processed.slice(0, options.limit)
    }

    // Project
    if (options.project) {
      processed = this.applyProjection(processed, options.project)
    }

    // Generate cursor for next page
    let nextCursor: string | undefined
    if (hasMore && processed.length > 0) {
      const lastItem = processed[processed.length - 1] as Record<string, unknown>
      nextCursor = this.generateCursor(lastItem, options.sort)
    }

    return {
      items: processed,
      total,
      nextCursor,
      hasMore,
    }
  }

  /**
   * Apply sort to results
   */
  private applySort<T>(results: T[], sort: SortSpec): T[] {
    return results.sort((a, b) => {
      for (const [field, direction] of Object.entries(sort)) {
        const aVal = (a as Record<string, unknown>)[field] as number | string | null | undefined
        const bVal = (b as Record<string, unknown>)[field] as number | string | null | undefined

        if (aVal === bVal) continue

        // Handle null/undefined
        if (aVal == null && bVal == null) continue
        if (aVal == null) return 1
        if (bVal == null) return -1

        const cmp = aVal < bVal ? -1 : 1
        const dir = direction === 'asc' || direction === 1 ? 1 : -1

        return cmp * dir
      }
      return 0
    })
  }

  /**
   * Apply projection to results
   */
  private applyProjection<T>(results: T[], projection: Projection): T[] {
    const includeFields = Object.entries(projection)
      .filter(([, v]) => v === 1 || v === true)
      .map(([k]) => k)

    const excludeFields = Object.entries(projection)
      .filter(([, v]) => v === 0 || v === false)
      .map(([k]) => k)

    return results.map((item) => {
      const rec = item as Record<string, unknown>

      if (includeFields.length > 0) {
        // Include mode: only keep specified fields
        const projected: Record<string, unknown> = {}
        for (const field of includeFields) {
          if (field in rec) {
            projected[field] = rec[field]
          }
        }
        return projected as T
      } else if (excludeFields.length > 0) {
        // Exclude mode: remove specified fields
        const projected = { ...rec }
        for (const field of excludeFields) {
          delete projected[field]
        }
        return projected as T
      }

      return item
    })
  }

  /**
   * Generate cursor for pagination
   */
  private generateCursor(item: Record<string, unknown>, sort?: SortSpec): string {
    const cursorData: Record<string, unknown> = {
      id: item.id,
    }

    // Include sort fields in cursor
    if (sort) {
      for (const field of Object.keys(sort)) {
        cursorData[field] = item[field]
      }
    }

    return Buffer.from(JSON.stringify(cursorData)).toString('base64')
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Get required columns from filter and projection
   */
  private getRequiredColumns(filter: Filter, projection?: Projection): string[] {
    const columns = new Set<string>()

    // Add columns from filter
    for (const field of Object.keys(filter)) {
      if (!field.startsWith('$')) {
        columns.add(field)
      }
    }

    // Add columns from projection
    if (projection) {
      for (const [field, value] of Object.entries(projection)) {
        if (value === 1 || value === true) {
          columns.add(field)
        }
      }
    }

    return Array.from(columns)
  }

  /**
   * Check if bloom filter can be used for this filter
   */
  private canUseBloomFilter(filter: Filter): boolean {
    // Bloom filter can be used for equality on id field
    return (
      'id' in filter &&
      (typeof filter.id === 'string' ||
        (typeof filter.id === 'object' && filter.id !== null && '$eq' in filter.id))
    )
  }

  // ===========================================================================
  // Parsing Methods (Placeholders)
  // ===========================================================================

  /**
   * Parse metadata length from Parquet footer
   * Footer format: [4 bytes metadata length][4 bytes magic number "PAR1"]
   */
  private parseMetadataLength(footer: Uint8Array): number {
    // Read metadata length as little-endian 32-bit int
    const view = new DataView(footer.buffer, footer.byteOffset, footer.byteLength)
    return view.getInt32(0, true)
  }

  /**
   * Parse Parquet metadata from bytes
   * Placeholder - actual implementation would use Thrift deserialization
   */
  private parseMetadata(_bytes: Uint8Array): ParquetMetadata {
    // Placeholder implementation
    return {
      numRows: 0,
      rowGroups: [],
      columns: [],
      schema: { fields: [] },
    }
  }

  /**
   * Parse row group from bytes
   * Placeholder - actual implementation would use Parquet decoder
   */
  private parseRowGroup(
    _data: Uint8Array,
    _rowGroup: RowGroupMetadata,
    _metadata: ParquetMetadata
  ): EntityRecord[] {
    // Placeholder implementation
    return []
  }

  /**
   * Parse bloom filter from bytes
   * Placeholder - actual implementation would deserialize bloom filter
   */
  private parseBloomFilter(_data: Uint8Array): BloomFilter {
    // Placeholder implementation
    return {
      mightContain: () => true,
      falsePositiveRate: 0.01,
      numBits: 0,
      numHashFunctions: 0,
    }
  }
}

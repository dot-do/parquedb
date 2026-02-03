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
// Predicate pushdown from hyparquet fork
import { parquetQuery } from 'hyparquet'
// Patched LZ4 decompressor (fixes match length extension + signed integer overflow bugs)
import { compressors } from '../parquet/compressors'
// Index cache for secondary index lookups
import { IndexCache, createR2IndexStorageAdapter } from './IndexCache'
// Centralized constants
import { MAX_CACHE_SIZE as _MAX_CACHE_SIZE, DEFAULT_CACHE_TTL } from '../constants'
// Logger
import { logger } from '../utils/logger'
import { stringToBase64 } from '../utils/base64'
import { createSafeRegex } from '../utils/safe-regex'
import { tryParseJson, isRecord } from '../utils/json-validation'

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
  /** Secondary index that would be used (if any) */
  secondaryIndex?: {
    name: string
    type: 'fts'
    field: string
  } | null
  /** Number of index catalog entries for this dataset */
  indexCatalogEntries?: number
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
 * CDN-backed R2 storage adapter for ParquetReader
 *
 * Uses CDN bucket (separate R2 bucket) for reads, which enables:
 * - Read/write separation (CDN bucket is read-only copy)
 * - Edge caching via CDN custom domain
 * - Reduced load on primary bucket
 *
 * Files in CDN bucket are stored under parquedb/ prefix.
 */
class CdnR2StorageAdapter implements Partial<StorageBackend> {
  readonly type = 'r2-cdn-adapter'

  // Stats for debugging
  public cdnHits = 0
  public primaryHits = 0
  public edgeHits = 0
  public totalReads = 0
  public cacheHits = 0

  // Whole-file cache: path -> Uint8Array (for small files)
  private fileCache = new Map<string, Uint8Array>()

  // Files being loaded (for deduplication)
  private loadingFiles = new Map<string, Promise<Uint8Array>>()

  // Max file size for whole-file caching - reserved for future size-based caching decisions
  public static readonly _MAX_CACHE_SIZE = _MAX_CACHE_SIZE

  constructor(
    private cdnBucket: R2Bucket,      // CDN bucket (cdn) for reads
    private primaryBucket: R2Bucket,  // Primary bucket (parquedb) as fallback
    private cdnPrefix: string = 'parquedb',  // Prefix in CDN bucket
    private r2DevUrl?: string  // r2.dev URL for edge caching (e.g. 'https://pub-xxx.r2.dev/parquedb')
  ) {}

  async read(path: string): Promise<Uint8Array> {
    // Check if file is already cached
    const cached = this.fileCache.get(path)
    if (cached) {
      this.cacheHits++
      return cached
    }

    // Check if file is being loaded
    const loading = this.loadingFiles.get(path)
    if (loading) {
      return loading
    }

    // Load and cache
    const loadPromise = this.loadWholeFile(path)
    this.loadingFiles.set(path, loadPromise)

    try {
      const data = await loadPromise
      this.fileCache.set(path, data)
      return data
    } finally {
      this.loadingFiles.delete(path)
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Check if file is already cached - serve range from cache (no network!)
    const cached = this.fileCache.get(path)
    if (cached) {
      this.cacheHits++
      return cached.slice(start, end)
    }

    // Check if file is being loaded by another request
    const loading = this.loadingFiles.get(path)
    if (loading) {
      const data = await loading
      return data.slice(start, end)
    }

    // Load whole file in ONE request, cache it, return the range
    // This converts N range requests into 1 whole-file request
    const loadPromise = this.loadWholeFile(path)
    this.loadingFiles.set(path, loadPromise)

    try {
      const data = await loadPromise
      this.fileCache.set(path, data)
      return data.slice(start, end)
    } finally {
      this.loadingFiles.delete(path)
    }
  }

  /**
   * Load entire file in one request
   */
  private async loadWholeFile(path: string): Promise<Uint8Array> {
    this.totalReads++

    // Use edge cache via cdn.workers.do for better global performance
    if (this.r2DevUrl) {
      // Version string for cache invalidation
      const url = `${this.r2DevUrl}/${path}?v=single-snappy`
      const response = await fetch(url, {
        cf: {
          cacheTtl: DEFAULT_CACHE_TTL,  // 1 hour edge cache
          cacheEverything: true,
        },
      })
      if (response.ok) {
        this.edgeHits++
        return new Uint8Array(await response.arrayBuffer())
      }
    }

    // Try CDN bucket with prefix (whole file)
    const cdnPath = `${this.cdnPrefix}/${path}`
    const cdnObj = await this.cdnBucket.get(cdnPath)
    if (cdnObj) {
      this.cdnHits++
      return new Uint8Array(await cdnObj.arrayBuffer())
    }

    // Fall back to primary bucket
    this.primaryHits++
    const obj = await this.primaryBucket.get(path)
    if (!obj) throw new Error(`Object not found: ${path}`)
    return new Uint8Array(await obj.arrayBuffer())
  }

  async exists(path: string): Promise<boolean> {
    // Check CDN bucket first
    const cdnPath = `${this.cdnPrefix}/${path}`
    const cdnHead = await this.cdnBucket.head(cdnPath)
    if (cdnHead !== null) return true

    // Fall back to primary
    const head = await this.primaryBucket.head(path)
    return head !== null
  }

  async stat(path: string): Promise<FileStat | null> {
    // Check CDN bucket first
    const cdnPath = `${this.cdnPrefix}/${path}`
    const cdnHead = await this.cdnBucket.head(cdnPath)
    if (cdnHead) {
      return {
        path,
        size: cdnHead.size,
        mtime: cdnHead.uploaded,
        isDirectory: false,
      }
    }

    // Fall back to primary
    const head = await this.primaryBucket.head(path)
    if (!head) return null
    return {
      path,
      size: head.size,
      mtime: head.uploaded,
      isDirectory: false,
    }
  }

  async list(prefix: string): Promise<ListResult> {
    // List from CDN bucket with prefix
    const cdnPrefix = `${this.cdnPrefix}/${prefix}`
    const result = await this.cdnBucket.list({ prefix: cdnPrefix, limit: 1000 })

    // Remove CDN prefix from paths
    const prefixLen = this.cdnPrefix.length + 1
    return {
      files: result.objects.map(obj => obj.key.slice(prefixLen)),
      hasMore: result.truncated,
      stats: result.objects.map(obj => ({
        path: obj.key.slice(prefixLen),
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

  /** Cache of parsed parquet data (for small files) */
  private dataCache = new Map<string, unknown[]>()

  /** Cache of file sizes for whole-file read decisions - reserved for future optimization */
  public _fileSizeCache = new Map<string, number>()

  /** Maximum file size for whole-file caching - reserved for future size limits */
  public static readonly _MAX_CACHE_SIZE_LIMIT = _MAX_CACHE_SIZE

  /** R2 storage adapter for ParquetReader */
  private storageAdapter: CdnR2StorageAdapter | null = null

  /** ParquetReader instance */
  private parquetReader: ParquetReader | null = null

  /** Index cache for secondary index lookups */
  private indexCache: IndexCache | null = null

  /**
   * Create a QueryExecutor
   *
   * @param readPath - ReadPath for legacy metadata reads
   * @param bucket - Primary R2 bucket (parquedb) for writes and fallback reads
   * @param cdnBucket - Optional CDN R2 bucket (cdn) for optimized reads
   * @param r2DevUrl - Optional r2.dev URL for edge caching (e.g. 'https://pub-xxx.r2.dev/parquedb')
   */
  constructor(private readPath: ReadPath, public _bucket?: R2Bucket, public _cdnBucket?: R2Bucket, public _r2DevUrl?: string) {
    const bucket = _bucket
    const cdnBucket = _cdnBucket
    const r2DevUrl = _r2DevUrl
    if (bucket) {
      // Use CDN adapter if CDN bucket is available, otherwise direct primary bucket access
      if (cdnBucket) {
        this.storageAdapter = new CdnR2StorageAdapter(cdnBucket, bucket, 'parquedb', r2DevUrl)
      } else {
        // Create adapter that uses primary bucket only
        this.storageAdapter = new CdnR2StorageAdapter(bucket, bucket, '', r2DevUrl)
      }
      this.parquetReader = new ParquetReader({ storage: this.storageAdapter as unknown as StorageBackend })

      // Initialize index cache for secondary index lookups
      this.indexCache = new IndexCache(createR2IndexStorageAdapter(bucket as unknown as { get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>; head(key: string): Promise<{ size: number } | null> }))
    }
  }

  /**
   * Get storage stats for debugging
   */
  getStorageStats(): { cdnHits: number; primaryHits: number; edgeHits: number; cacheHits: number; totalReads: number; usingCdn: boolean; usingEdge: boolean } {
    if (!this.storageAdapter) {
      return { cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false }
    }
    return {
      cdnHits: this.storageAdapter.cdnHits,
      primaryHits: this.storageAdapter.primaryHits,
      edgeHits: this.storageAdapter.edgeHits,
      cacheHits: this.storageAdapter.cacheHits,
      totalReads: this.storageAdapter.totalReads,
      usingCdn: !!this._cdnBucket,
      usingEdge: !!this._r2DevUrl,
    }
  }

  /**
   * Clear in-memory caches (for benchmarking cold queries)
   */
  clearCache(): void {
    this.dataCache.clear()
    this.metadataCache.clear()
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
      // Path format: either "{dataset}" or "{dataset}/{collection}"
      // For "{dataset}" we read from {dataset}/data.parquet
      // For "{dataset}/{collection}" we read from {dataset}/{collection}.parquet
      const _datasetId = ns.includes('/') ? ns.split('/')[0] : ns
      void _datasetId // Reserved for future use in multi-dataset queries
      const path = ns.includes('/') ? `${ns}.parquet` : `${ns}/data.parquet`

      // Use ParquetReader when available (real implementation)
      if (this.parquetReader && this.storageAdapter) {
        // Check in-memory cache first (avoids ALL I/O for repeated queries)
        const cached = this.dataCache.get(path) as T[] | undefined
        if (cached) {
          stats.cacheHit = true
          let results = this.applyFilter([...cached], filter)
          const processed = this.postProcess(results, options)
          stats.rowsScanned = cached.length
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

        // Check for applicable secondary indexes (hash, fts)
        // NOTE: SST indexes removed - range queries now use native parquet predicate pushdown
        // Pass full ns path (e.g., 'onet-full/occupations') for index catalog lookup
        if (this.indexCache) {
          const indexResult = await this.executeWithIndex<T>(ns, path, filter, options, stats, startTime)
          if (indexResult) {
            return indexResult
          }
        }

        // Extract pushdown filters ($id and $index_* columns with parquet statistics)
        // These enable row-group skipping based on min/max column statistics
        const pushdownFilter = this.extractPushdownFilter(filter)

        type DataRow = { $id: string; data: T | string; [key: string]: unknown }
        let rows: DataRow[]

        if (pushdownFilter && this.storageAdapter) {
          // Use parquetQuery with predicate pushdown
          // hyparquet uses min/max statistics to skip row groups that can't match
          const asyncBuffer = await this.createAsyncBuffer(path)
          try {
            rows = await parquetQuery({
              file: asyncBuffer,
              filter: pushdownFilter,
              columns: ['$id', 'data'],
              compressors,
            }) as DataRow[]
            stats.rowsScanned = rows.length
            // Log pushdown filter for debugging
            logger.debug(`Pushdown filter applied: ${JSON.stringify(pushdownFilter)}`)
          } catch (error: unknown) {
            // Fall back to full read if parquetQuery fails (e.g. column not found)
            logger.debug('parquetQuery with pushdown failed, falling back to full read', error)
            rows = await this.parquetReader.read<DataRow>(path)
            stats.rowsScanned = rows.length
          }
        } else {
          // No pushable filter - read all data
          rows = await this.parquetReader.read<DataRow>(path)
          stats.rowsScanned = rows.length
        }

        // Unpack data column - parse JSON if string, use directly if object
        let results = rows.map(row => {
          if (typeof row.data === 'string') {
            const parsed = tryParseJson<T>(row.data)
            return parsed ?? (row as unknown as T)
          }
          return (row.data ?? row) as T
        })

        // Merge pending files (DO WAL Phase 2 - Bulk Bypass)
        // Pending files contain bulk writes that bypassed SQLite buffering
        const datasetId = ns.includes('/') ? ns.split('/')[0] : ns
        const pendingRows = await this.readPendingFiles<T>(datasetId)
        if (pendingRows.length > 0) {
          results = [...results, ...pendingRows]
          stats.rowsScanned += pendingRows.length
        }

        // Cache the unpacked data for subsequent requests (only for full reads without pending)
        if (!pushdownFilter && pendingRows.length === 0) {
          this.dataCache.set(path, results as unknown[])
        }

        // Apply remaining MongoDB-style filters (for nested fields in data)
        const remainingFilter = this.removeIdFilter(filter)
        if (Object.keys(remainingFilter).length > 0) {
          results = this.applyFilter(results, remainingFilter)
        }

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
    } catch (error: unknown) {
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

  // ===========================================================================
  // Index-Based Query Execution
  // ===========================================================================

  /**
   * Try to execute query using a secondary index
   *
   * @param datasetId - Dataset ID (e.g., 'imdb-1m')
   * @param dataPath - Path to data.parquet file
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @param stats - Query stats to update
   * @param startTime - Query start time
   * @returns Query result if index was used, null if no applicable index
   */
  private async executeWithIndex<T>(
    datasetId: string,
    dataPath: string,
    filter: Filter,
    options: FindOptions<T>,
    stats: QueryStats,
    startTime: number
  ): Promise<FindResult<T> | null> {
    if (!this.indexCache || !this.parquetReader || !this.storageAdapter) {
      return null
    }

    try {
      // Select the best index for this query
      const selected = await this.indexCache.selectIndex(datasetId, filter as Record<string, unknown>)
      if (!selected) {
        return null
      }

      // Track index usage in stats
      const extendedStats = stats as QueryStats & {
        indexUsed?: string
        indexType?: string
        indexLookupMs?: number
        rowGroupsTotal?: number
        rowGroupsRead?: number
      }
      extendedStats.indexUsed = selected.entry.name
      extendedStats.indexType = selected.type

      const indexLookupStart = performance.now()
      let candidateDocIds: string[] = []
      let targetRowGroups: number[] = []

      // Execute index lookup based on type
      // NOTE: Hash and SST indexes removed - equality and range queries now use native parquet predicate pushdown on $index_* columns
      if (selected.type === 'fts') {
        const textCondition = selected.condition as { $search: string; $language?: string }
        const ftsResults = await this.indexCache.executeFTSSearch(
          datasetId,
          selected.entry,
          textCondition,
          { limit: options.limit }
        )
        candidateDocIds = ftsResults.map(r => r.docId)
        // FTS doesn't return row groups, will need to scan all
        targetRowGroups = []
      }

      extendedStats.indexLookupMs = performance.now() - indexLookupStart

      // If no matches from index, return empty result
      if (candidateDocIds.length === 0) {
        stats.rowsScanned = 0
        stats.rowsReturned = 0
        stats.executionTimeMs = performance.now() - startTime
        return {
          items: [],
          hasMore: false,
          stats,
        }
      }

      // Read parquet metadata to check selectivity before proceeding
      const metadata = await this.parquetReader.readMetadata(dataPath)
      const totalRows = metadata.rowGroups.reduce((sum, rg) => sum + Number(rg.numRows), 0)

      // Track selectivity metrics in extended stats
      const extendedStatsWithSelectivity = extendedStats as QueryStats & {
        indexUsed?: string
        indexType?: string
        indexLookupMs?: number
        rowGroupsTotal?: number
        rowGroupsRead?: number
        indexSelectivity?: number
        candidateCount?: number
      }
      extendedStatsWithSelectivity.indexSelectivity = candidateDocIds.length / totalRows
      extendedStatsWithSelectivity.candidateCount = candidateDocIds.length

      // Log row-group skip ratio
      logger.debug(`Index ${selected.entry.name}: ${targetRowGroups.length}/${metadata.rowGroups.length} row groups, ${candidateDocIds.length} candidates`)

      // Check if index lookup is beneficial (reduces scan by at least 50%)
      // If not, fall back to full scan which avoids the overhead of building
      // candidate sets and filtering rows
      if (candidateDocIds.length > totalRows * 0.5) {
        // Low selectivity: index matched >50% of rows
        // Fall back to full scan which is more efficient
        logger.debug(`Index ${selected.entry.name} matched ${candidateDocIds.length}/${totalRows} rows (${Math.round(candidateDocIds.length/totalRows*100)}%), falling back to scan`)
        return null
      }

      // Build candidate set for O(1) lookup
      const candidateSet = new Set(candidateDocIds)

      type DataRow = { $id: string; data: T | string }
      let rows: DataRow[]

      // ROW GROUP SKIPPING: Only read the row groups that contain matching documents
      if (targetRowGroups.length > 0 && this.storageAdapter) {
        // Use already-loaded metadata for row group boundaries
        extendedStatsWithSelectivity.rowGroupsTotal = metadata.rowGroups.length
        extendedStatsWithSelectivity.rowGroupsRead = targetRowGroups.length
        stats.rowGroupsSkipped = metadata.rowGroups.length - targetRowGroups.length

        // Calculate row ranges for target row groups
        // Row groups are sequential, so we need to calculate cumulative row offsets
        const rowGroupOffsets: number[] = []
        let cumulativeRows = 0
        for (const rg of metadata.rowGroups) {
          rowGroupOffsets.push(cumulativeRows)
          // Convert BigInt to number (parquet metadata uses BigInt for row counts)
          cumulativeRows += Number(rg.numRows)
        }

        // Sort target row groups and merge adjacent ones into ranges
        const sortedGroups = [...targetRowGroups].sort((a, b) => a - b)
        const rowRanges: Array<{ rowStart: number; rowEnd: number }> = []

        for (const rgIndex of sortedGroups) {
          if (rgIndex < 0 || rgIndex >= metadata.rowGroups.length) continue
          const rg = metadata.rowGroups[rgIndex]
          const rowStart = rowGroupOffsets[rgIndex]
          if (rowStart === undefined || rg === undefined) continue
          const rowEnd = rowStart + Number(rg.numRows)

          // Try to merge with previous range if adjacent
          const lastRange = rowRanges[rowRanges.length - 1]
          if (lastRange && lastRange.rowEnd === rowStart) {
            lastRange.rowEnd = rowEnd
          } else {
            rowRanges.push({ rowStart, rowEnd })
          }
        }

        // Read only the target row groups using row ranges
        // For multiple ranges, read each separately and combine
        rows = []
        let totalRowsScanned = 0

        for (const range of rowRanges) {
          const asyncBuffer = await this.createAsyncBuffer(dataPath)
          const rangeRows = await parquetQuery({
            file: asyncBuffer,
            columns: ['$id', 'data'],
            rowStart: range.rowStart,
            rowEnd: range.rowEnd,
            compressors,
          }) as DataRow[]
          // Use loop instead of spread to avoid stack overflow with large arrays
          for (const row of rangeRows) {
            rows.push(row)
          }
          totalRowsScanned += rangeRows.length
        }

        stats.rowsScanned = totalRowsScanned
        stats.rowGroupsScanned = targetRowGroups.length
      } else {
        // No row group hints, fall back to full scan
        rows = await this.parquetReader.read<DataRow>(dataPath)
        stats.rowsScanned = rows.length
      }

      // Filter rows to candidates and unpack data
      let results = rows
        .filter(row => {
          // Check both $id and extracted ID from data
          if (candidateSet.has(row.$id)) return true
          // Also check data.$id for nested structure
          const data = typeof row.data === 'string' ? tryParseJson(row.data) : row.data
          const dataId = (data && isRecord(data)) ? (data as Record<string, unknown>).$id as string : undefined
          if (dataId && candidateSet.has(dataId)) return true
          return false
        })
        .map(row => {
          if (typeof row.data === 'string') {
            const parsed = tryParseJson<T>(row.data)
            return parsed ?? (row as unknown as T)
          }
          return (row.data ?? row) as T
        })

      // Apply any remaining filter conditions (excluding the indexed field)
      const remainingFilter = this.removeIndexedField(filter, selected.entry.field)
      if (Object.keys(remainingFilter).length > 0) {
        results = this.applyFilter(results, remainingFilter)
      }

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
    } catch (error: unknown) {
      // Index execution failed - log with context and fall back to full scan
      // This is an expected failure mode (e.g., corrupted index, network issues)
      // so we log at warn level and allow the query to proceed via full scan
      const cause = error instanceof Error ? error : new Error(String(error))
      logger.warn(`Index execution failed for ${datasetId}, falling back to full scan: ${cause.message}`, {
        datasetId,
        filter,
        error: cause,
      })

      // Record that we attempted index usage but had to fall back
      const extendedStats = stats as QueryStats & {
        indexFallback?: boolean
        indexError?: string
      }
      extendedStats.indexFallback = true
      extendedStats.indexError = cause.message

      return null
    }
  }

  /**
   * Remove the indexed field from filter (already satisfied by index lookup)
   */
  private removeIndexedField(filter: Filter, field: string): Filter {
    const result: Filter = {}

    for (const [key, value] of Object.entries(filter)) {
      if (key === field) continue
      if (key === '$text') continue // FTS already applied
      (result as Record<string, unknown>)[key] = value
    }

    return result
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

    // Read from Parquet - try multiple ID field patterns
    // Support: $id (ParqueDB), id (legacy), code (O*NET occupations), elementId (O*NET skills/abilities/knowledge)
    const fullId = id.includes('/') ? id : `${ns.split('/').pop()}/${id}`
    const result = await this.find<T>(ns, {
      $or: [
        { $id: { $eq: fullId } },
        { $id: { $eq: id } },
        { code: { $eq: id } },
        { id: { $eq: id } },
        { elementId: { $eq: id } },
      ]
    }, { limit: 1 })

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

  /**
   * Get relationships from rels.parquet
   *
   * Reads edges from the unified rels.parquet file, filtered by from_id and predicate.
   * Much faster than scanning all nodes to find reverse relationships.
   *
   * @param dataset - Dataset prefix (e.g., 'onet-graph')
   * @param fromId - Source entity ID (e.g., '2.C.2.b')
   * @param predicate - Relationship predicate (e.g., 'requiredBy')
   * @returns Array of relationship edges
   */
  async getRelationships(
    dataset: string,
    fromId: string,
    predicate?: string
  ): Promise<Array<{
    to_ns: string
    to_id: string
    to_name: string
    to_type: string
    predicate: string
    importance: number | null
    level: number | null
  }>> {
    if (!this.parquetReader || !this.storageAdapter) {
      return []
    }

    // Optimized format: from_id (string) + data (JSON)
    type RelRow = {
      from_id: string
      data: {
        to: string      // Target $id
        ns: string      // Target namespace
        name: string    // Target name
        pred: string    // Predicate
        rev: string     // Reverse predicate
        importance?: number
        level?: number
      }
    }

    try {
      // Single rels.parquet file (no sharding - doesn't scale for large datasets)
      const path = `${dataset}/rels.parquet`

      // Check in-memory cache first (cache stores parsed data)
      let allRels = this.dataCache.get(path) as RelRow[] | undefined
      if (!allRels) {
        // Read raw rows and parse JSON data column
        type RawRelRow = { from_id: string; data: string | RelRow['data'] }
        const rawRels = await this.parquetReader.read<RawRelRow>(path)

        // Parse JSON data column if needed
        allRels = rawRels.map(row => {
          if (typeof row.data === 'string') {
            const parsed = tryParseJson<RelRow['data']>(row.data)
            return {
              from_id: row.from_id,
              data: parsed ?? { to: '', ns: '', name: '', pred: '', rev: '' },
            }
          }
          return row as RelRow
        })

        this.dataCache.set(path, allRels as unknown[])
      }

      // Filter by from_id and optionally predicate, then map to expected format
      return allRels!
        .filter(rel =>
          rel.from_id === fromId &&
          (!predicate || rel.data.pred === predicate)
        )
        .map(rel => ({
          to_ns: rel.data.ns,
          to_id: rel.data.to.split('/').pop() || rel.data.to,
          to_name: rel.data.name,
          to_type: rel.data.ns.charAt(0).toUpperCase() + rel.data.ns.slice(1, -1), // occupations -> Occupation
          predicate: rel.data.pred,
          importance: rel.data.importance ?? null,
          level: rel.data.level ?? null,
        }))
    } catch (error: unknown) {
      // Relationship data may not exist or may be malformed - return empty gracefully
      logger.debug('Failed to load relationships', error)
      return []
    }
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

    // Check for secondary index selection
    let secondaryIndex: QueryPlan['secondaryIndex'] = null
    let indexCatalogEntries: number | undefined

    if (this.indexCache) {
      try {
        // Extract dataset ID from ns (e.g., 'imdb-1m/movies' -> 'imdb-1m')
        const datasetId = ns.includes('/') ? (ns.split('/')[0] ?? ns) : ns
        const selected = await this.indexCache.selectIndex(datasetId, filter as Record<string, unknown>)

        if (selected) {
          secondaryIndex = {
            name: selected.entry.name,
            type: selected.type,
            field: selected.entry.field,
          }
        }

        // Get catalog entry count
        const catalog = await this.indexCache.loadCatalog(datasetId)
        indexCatalogEntries = catalog.length
      } catch (error: unknown) {
        // Index lookup failed, log and continue without index info
        logger.debug('Failed to check secondary indexes for explain', error)
      }
    }

    return {
      optimizedFilter: filter,
      selectedRowGroups: selectedRowGroups.map((rg) => rg.index),
      requiredColumns,
      useBloomFilter,
      estimatedRows,
      estimatedBytes,
      secondaryIndex,
      indexCatalogEntries,
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

    // Read from R2 - supports both {dataset} and {dataset}/{collection} paths
    const _datasetId = ns.includes('/') ? ns.split('/')[0] : ns
    void _datasetId // Reserved for future use
    const path = ns.includes('/') ? `${ns}.parquet` : `${ns}/data.parquet`

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
    } catch (error: unknown) {
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
    // Supports both {dataset} and {dataset}/{collection} paths
    const _datasetId = ns.includes('/') ? ns.split('/')[0] : ns
    void _datasetId // Reserved for future use
    const path = ns.includes('/') ? `${ns}.parquet` : `${ns}/data.parquet`

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
    // Only skip logical operators ($and, $or, $not, $nor)
    // Allow field names that start with $ like $id, $type
    const logicalOperators = new Set(['$and', '$or', '$not', '$nor'])
    for (const [field, condition] of Object.entries(filter)) {
      if (logicalOperators.has(field)) continue

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
      const regex = createSafeRegex(operator.$regex as string, operator.$options)
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

    // Base64 encode the cursor (Worker-safe)
    return stringToBase64(JSON.stringify(cursorData))
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

  /**
   * Extract filters that can be pushed down to Parquet
   * Includes $id and $index_* column filters which have min/max statistics
   * Returns the filter portion that can be pushed down to Parquet
   */
  private extractPushdownFilter(filter: Filter): Filter | null {
    const pushdownFilter: Filter = {}

    // Extract direct column filters that can use statistics
    for (const [key, value] of Object.entries(filter)) {
      // Push down $id and $index_* columns (these have parquet statistics)
      if (key === '$id' || key.startsWith('$index_')) {
        pushdownFilter[key] = value
      }
    }

    // Handle $and - extract pushable conditions
    if (filter.$and && Array.isArray(filter.$and)) {
      const pushableConditions: Filter[] = []
      for (const cond of filter.$and) {
        const extracted = this.extractPushdownFilter(cond as Filter)
        if (extracted && Object.keys(extracted).length > 0) {
          pushableConditions.push(extracted)
        }
      }
      if (pushableConditions.length > 0) {
        // If we have existing pushdown filters and AND conditions, combine them
        if (Object.keys(pushdownFilter).length > 0) {
          return { $and: [pushdownFilter, ...pushableConditions] }
        }
        if (pushableConditions.length === 1 && pushableConditions[0]) {
          return pushableConditions[0]
        }
        return { $and: pushableConditions }
      }
    }

    // Return null if no pushable filters found
    if (Object.keys(pushdownFilter).length === 0) {
      return null
    }

    return pushdownFilter
  }

  /**
   * Extract $id filter for predicate pushdown (legacy method for compatibility)
   * Returns the filter portion that can be pushed down to Parquet
   * @internal Reserved for compatibility with older code
   */
  public _extractIdFilter(filter: Filter): Filter | null {
    return this.extractPushdownFilter(filter)
  }

  /**
   * Check if a key is a pushdown column ($id or $index_*)
   */
  private isPushdownColumn(key: string): boolean {
    return key === '$id' || key.startsWith('$index_')
  }

  /**
   * Remove pushed-down filters from original filter
   * Returns remaining filters to apply after predicate pushdown
   */
  private removeIdFilter(filter: Filter): Filter {
    const result: Filter = {}

    for (const [key, value] of Object.entries(filter)) {
      // Skip pushdown columns - they're handled by parquet statistics
      if (this.isPushdownColumn(key)) continue

      if (key === '$or' && Array.isArray(value)) {
        // For $or, keep conditions that aren't purely pushdown
        const nonPushdownConditions = value.filter(f => {
          const keys = Object.keys(f)
          return keys.some(k => !this.isPushdownColumn(k))
        })
        if (nonPushdownConditions.length > 0) {
          result.$or = nonPushdownConditions
        }
      } else if (key === '$and' && Array.isArray(value)) {
        // For $and, filter out conditions that were pushed down
        const remainingConditions: Filter[] = []
        for (const cond of value) {
          const remaining = this.removeIdFilter(cond as Filter)
          if (Object.keys(remaining).length > 0) {
            remainingConditions.push(remaining)
          }
        }
        if (remainingConditions.length > 0) {
          result.$and = remainingConditions
        }
      } else {
        (result as Record<string, unknown>)[key] = value
      }
    }

    return result
  }

  /**
   * Create AsyncBuffer from storage adapter for parquetQuery
   */
  private async createAsyncBuffer(path: string): Promise<{ byteLength: number; slice: (start: number, end?: number) => Promise<ArrayBuffer> }> {
    if (!this.storageAdapter) {
      throw new Error('Storage adapter not available')
    }

    // Get file size first
    const stat = await this.storageAdapter.stat(path)
    if (!stat) {
      throw new Error(`File not found: ${path}`)
    }

    const storage = this.storageAdapter
    return {
      byteLength: stat.size,
      async slice(start: number, end?: number): Promise<ArrayBuffer> {
        const data = await storage.readRange(path, start, end ?? stat.size)
        // Copy to new ArrayBuffer to avoid SharedArrayBuffer issues
        const copy = new ArrayBuffer(data.byteLength)
        new Uint8Array(copy).set(data)
        return copy
      }
    }
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

  // ===========================================================================
  // Pending Files (DO WAL Phase 2 - Bulk Bypass)
  // ===========================================================================

  /**
   * Read pending Parquet files for a namespace
   *
   * Pending files are created by bulk writes that bypass SQLite buffering.
   * They are stored in data/{ns}/pending/*.parquet and need to be merged
   * with committed data during reads.
   *
   * @param ns - Namespace to read pending files for
   * @returns Array of rows from pending files
   */
  async readPendingFiles<T>(ns: string): Promise<T[]> {
    if (!this.storageAdapter || !this._bucket) {
      return []
    }

    // List pending files
    const pendingPrefix = ns.includes('/') ? `${ns}/pending/` : `data/${ns}/pending/`

    try {
      const listResult = await this._bucket.list({ prefix: pendingPrefix, limit: 100 })
      if (!listResult.objects || listResult.objects.length === 0) {
        return []
      }

      const allRows: T[] = []

      // Read each pending file
      for (const obj of listResult.objects) {
        const path = obj.key

        try {
          if (path.endsWith('.parquet') && this.parquetReader) {
            // Read Parquet file
            type DataRow = { $id: string; data: T | string }
            const rows = await this.parquetReader.read<DataRow>(path)

            // Unpack data column
            for (const row of rows) {
              if (typeof row.data === 'string') {
                const parsed = tryParseJson<T>(row.data)
                if (parsed) {
                  allRows.push(parsed)
                }
              } else if (row.data) {
                allRows.push(row.data)
              }
            }
          } else if (path.endsWith('.json')) {
            // Fallback: Read JSON file (for when hyparquet-writer is not available)
            const objData = await this._bucket.get(path)
            if (objData) {
              const text = await objData.text()
              const rows = tryParseJson<Array<{ $id: string; data: string }>>(text)
              if (rows) {
                for (const row of rows) {
                  const parsed = tryParseJson<T>(row.data)
                  if (parsed) {
                    allRows.push(parsed)
                  }
                }
              }
            }
          }
        } catch (error: unknown) {
          // Skip files that fail to read - they may be corrupted or in-progress
          logger.debug(`Failed to read pending file ${path}`, error)
        }
      }

      return allRows
    } catch (error: unknown) {
      // Listing may fail if prefix doesn't exist - that's fine
      logger.debug(`Failed to list pending files for ${ns}`, error)
      return []
    }
  }

  /**
   * Check if namespace has pending files
   *
   * @param ns - Namespace to check
   * @returns true if pending files exist
   */
  async hasPendingFiles(ns: string): Promise<boolean> {
    if (!this._bucket) {
      return false
    }

    const pendingPrefix = ns.includes('/') ? `${ns}/pending/` : `data/${ns}/pending/`

    try {
      const listResult = await this._bucket.list({ prefix: pendingPrefix, limit: 1 })
      return (listResult.objects?.length ?? 0) > 0
    } catch (error: unknown) {
      return false
    }
  }
}

/**
 * Query Executor for ParqueDB
 *
 * Orchestrates query execution with predicate pushdown, column projection,
 * and parallel row group reading for efficient Parquet queries.
 */

import type { Filter, FindOptions, SortSpec, Projection } from '../types'
import type { StorageBackend } from '../types/storage'
import {
  selectRowGroups,
  toPredicate,
  extractFilterFields,
  extractRowGroupStats,
  type RowGroupStats,
  type ParquetMetadata,
} from './predicate'
// checkBloomFilter reserved for future bloom filter optimization
export { checkBloomFilter as _checkBloomFilter } from './bloom'
import type { IndexManager, SelectedIndex } from '../indexes/manager'
import type { MVRouter } from './optimizer'
// Re-export index types for consumers
export type { IndexLookupResult, FTSSearchResult } from '../indexes/types'
import { logger } from '../utils/logger'
import { stringToBase64 } from '../utils/base64'
import { isNullish } from '../utils/comparison'
import {
  globalHookRegistry,
  createQueryContext,
} from '../observability'
import { getGlobalTelemetry } from '../observability/telemetry'
import { DEFAULT_CONCURRENCY } from '../constants'

// =============================================================================
// Query Result Types
// =============================================================================

/**
 * Query execution result
 */
export interface QueryResult<T> {
  /** Matching rows */
  rows: T[]
  /** Total count (if available/requested) */
  totalCount?: number | undefined
  /** Cursor for pagination */
  nextCursor?: string | undefined
  /** Whether more results exist */
  hasMore: boolean
  /** Query execution statistics */
  stats: QueryStats
}

/**
 * Query execution statistics for monitoring and optimization
 */
export interface QueryStats {
  /** Total row groups in the file */
  totalRowGroups: number
  /** Row groups scanned after predicate pushdown */
  scannedRowGroups: number
  /** Row groups skipped by predicate pushdown */
  skippedRowGroups: number
  /** Total rows scanned */
  rowsScanned: number
  /** Rows that matched the filter */
  rowsMatched: number
  /** Execution time in milliseconds */
  executionTimeMs: number
  /** Columns read from Parquet */
  columnsRead: string[]
  /** Whether bloom filter was used */
  usedBloomFilter: boolean
  /** Index used (if any) */
  indexUsed?: string | undefined
  /** Index type used */
  indexType?: 'fts' | 'vector' | 'geo' | undefined
}

/**
 * Query plan for explain mode
 */
export interface QueryPlan {
  /** Filter analysis */
  filter: {
    original: Filter
    fields: string[]
    hasLogicalOps: boolean
    hasSpecialOps: boolean
  }
  /** Predicate pushdown analysis */
  predicatePushdown: {
    enabled: boolean
    rowGroupsTotal: number
    rowGroupsSelected: number
    estimatedSavings: string
  }
  /** Column projection */
  projection: {
    columns: string[]
    isFullScan: boolean
  }
  /** Sort plan */
  sort: {
    fields: string[]
    canUseSortedData: boolean
  }
  /** Index usage plan */
  index?: {
    /** Index name */
    name: string
    /** Index type */
    type: 'fts' | 'vector' | 'geo'
    /** Field being queried */
    field?: string | undefined
    /** Whether index will be used */
    willUse: boolean
  } | undefined
  /** Vector search plan (for $vector queries) */
  vectorSearch?: {
    /** Vector index name */
    indexName: string
    /** Field being searched */
    field: string
    /** Number of results requested */
    topK: number
    /** efSearch parameter */
    efSearch: number
    /** Minimum similarity score */
    minScore?: number | undefined
    /** Whether this is a hybrid search */
    isHybrid: boolean
    /** Hybrid search strategy (if applicable) */
    hybridStrategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
    /** Strategy reasoning */
    strategyReason?: string | undefined
    /** Estimated filter selectivity for hybrid search */
    filterSelectivity?: number | undefined
  } | undefined
}

// =============================================================================
// Parquet Reader Interface
// =============================================================================

/**
 * Interface for reading Parquet files
 * Actual implementation would use parquet-wasm or similar
 */
export interface ParquetReader {
  /**
   * Read file metadata (schema, row groups, statistics)
   */
  readMetadata(path: string): Promise<ParquetMetadata>

  /**
   * Read specific row groups from a Parquet file
   */
  readRowGroups<T>(
    path: string,
    rowGroups: number[],
    columns?: string[]
  ): Promise<T[]>

  /**
   * Read all rows from a Parquet file
   */
  readAll<T>(path: string, columns?: string[]): Promise<T[]>

  /**
   * Get bloom filter for a column
   */
  getBloomFilter(
    path: string,
    rowGroup: number,
    column: string
  ): Promise<BloomFilterReader | null>
}

/**
 * Bloom filter reader interface
 */
export interface BloomFilterReader {
  /**
   * Check if value might exist
   * Returns false = definitely not present
   * Returns true = might be present (false positive possible)
   */
  mightContain(value: unknown): boolean
}

// =============================================================================
// Query Executor
// =============================================================================

/**
 * Executes queries against Parquet files with predicate pushdown
 */
export class QueryExecutor {
  private indexManager?: IndexManager
  private mvRouter?: MVRouter

  constructor(
    private reader: ParquetReader,
    _storage: StorageBackend,
    indexManager?: IndexManager,
    mvRouter?: MVRouter
  ) {
    void _storage // Reserved for future direct storage access
    this.indexManager = indexManager
    this.mvRouter = mvRouter
  }

  /**
   * Set the index manager for index-aware query execution
   */
  setIndexManager(indexManager: IndexManager): void {
    this.indexManager = indexManager
  }

  /**
   * Set the MV router for materialized view-aware query execution
   */
  setMVRouter(mvRouter: MVRouter): void {
    this.mvRouter = mvRouter
  }

  /**
   * Execute a query with filter and options
   *
   * @param ns - Namespace (collection name)
   * @param filter - MongoDB-style filter
   * @param options - Find options (sort, limit, skip, project, etc.)
   * @returns Query result with matching rows
   */
  async execute<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T> = {}
  ): Promise<QueryResult<T>> {
    const startTime = Date.now()
    const hookContext = createQueryContext('find', ns, filter, options as FindOptions<unknown>)
    const telemetry = getGlobalTelemetry()
    const span = telemetry.startSpan('query.execute', {
      'db.namespace': ns,
      'db.operation': 'find',
      'db.filter_fields': Object.keys(filter).join(','),
    })

    // Dispatch query start hook
    await globalHookRegistry.dispatchQueryStart(hookContext)

    try {
      let result: QueryResult<T>

      // Check for applicable materialized views first
      if (this.mvRouter) {
        const mvResult = await this.executeWithMV<T>(ns, filter, options, startTime)
        if (mvResult) {
          result = mvResult
          // Dispatch query end hook
          await globalHookRegistry.dispatchQueryEnd(hookContext, {
            rowCount: result.rows.length,
            durationMs: result.stats.executionTimeMs,
            indexUsed: result.stats.indexUsed,
            rowGroupsScanned: result.stats.scannedRowGroups,
            rowGroupsSkipped: result.stats.skippedRowGroups,
          })
          return result
        }
      }

      // Check for applicable indexes
      if (this.indexManager) {
        const indexPlan = await this.indexManager.selectIndex(ns, filter)
        if (indexPlan) {
          const indexResult = await this.executeWithIndex<T>(ns, filter, options, indexPlan, startTime)
          if (indexResult) {
            result = indexResult
            // Dispatch query end hook
            await globalHookRegistry.dispatchQueryEnd(hookContext, {
              rowCount: result.rows.length,
              durationMs: result.stats.executionTimeMs,
              indexUsed: result.stats.indexUsed,
              rowGroupsScanned: result.stats.scannedRowGroups,
              rowGroupsSkipped: result.stats.skippedRowGroups,
            })
            return result
          }
          // Fall through to full scan if index execution fails
        }
      }

      // Standard execution path with predicate pushdown
      result = await this.executeFullScan<T>(ns, filter, options, startTime)

      // Dispatch query end hook
      await globalHookRegistry.dispatchQueryEnd(hookContext, {
        rowCount: result.rows.length,
        durationMs: result.stats.executionTimeMs,
        indexUsed: result.stats.indexUsed,
        rowGroupsScanned: result.stats.scannedRowGroups,
        rowGroupsSkipped: result.stats.skippedRowGroups,
        cached: false,
      })

      // End telemetry span
      telemetry.endSpan(span.spanId, 'ok', {
        'db.rows_matched': result.rows.length,
        'db.rows_scanned': result.stats.rowsScanned,
        'db.row_groups_scanned': result.stats.scannedRowGroups,
        'db.row_groups_skipped': result.stats.skippedRowGroups,
        'db.execution_time_ms': result.stats.executionTimeMs,
      })

      // Structured log for query completion
      telemetry.emitLog('info', 'query_completed', 'query', {
        rowsMatched: result.rows.length,
        rowsScanned: result.stats.rowsScanned,
        rowGroupsScanned: result.stats.scannedRowGroups,
        rowGroupsSkipped: result.stats.skippedRowGroups,
        indexUsed: result.stats.indexUsed,
      }, {
        namespace: ns,
        operation: 'find',
        traceId: span.traceId,
        spanId: span.spanId,
        durationMs: result.stats.executionTimeMs,
      })

      return result
    } catch (error) {
      // End telemetry span with error
      telemetry.endSpan(span.spanId, 'error', {
        'error.type': error instanceof Error ? error.name : 'Unknown',
        'error.message': error instanceof Error ? error.message : String(error),
      })

      telemetry.emitLog('error', 'query_failed', 'query', {}, {
        namespace: ns,
        operation: 'find',
        traceId: span.traceId,
        spanId: span.spanId,
        error: error instanceof Error ? error : new Error(String(error)),
      })

      // Dispatch query error hook
      await globalHookRegistry.dispatchQueryError(
        hookContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  /**
   * Execute query using a materialized view
   */
  private async executeWithMV<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T>,
    startTime: number
  ): Promise<QueryResult<T> | null> {
    if (!this.mvRouter) {
      return null
    }

    try {
      // Route query to find applicable MV
      const routingResult = await this.mvRouter.route(ns, filter, options)

      if (!routingResult.canUseMV || !routingResult.mvName) {
        return null
      }

      // Read from the MV instead of the source collection
      const mvPath = `data/${routingResult.mvName}/data.parquet`
      const metadata = await this.reader.readMetadata(mvPath)
      const stats = extractRowGroupStats(metadata)

      // Apply post-filter if needed, otherwise empty filter
      const mvFilter = routingResult.postFilter ?? {}
      const selectedRowGroups = selectRowGroups(mvFilter, stats)

      // Determine columns to read
      const columns = this.selectColumns(mvFilter, options)

      // Read row groups
      const rowBatches = await this.readRowGroupsParallel<T>(
        mvPath,
        selectedRowGroups,
        columns
      )

      // Flatten row batches
      const allRows = rowBatches.flat()

      // Apply post-filter if needed
      let filtered = allRows
      if (routingResult.needsPostFilter && routingResult.postFilter) {
        const predicate = toPredicate(routingResult.postFilter)
        filtered = allRows.filter(row => predicate(row))
      }

      // Apply sort, limit, skip
      const result = this.postProcess(filtered, options)

      // Build statistics
      const executionStats: QueryStats = {
        totalRowGroups: stats.length,
        scannedRowGroups: selectedRowGroups.length,
        skippedRowGroups: stats.length - selectedRowGroups.length,
        rowsScanned: allRows.length,
        rowsMatched: filtered.length,
        executionTimeMs: Date.now() - startTime,
        columnsRead: columns,
        usedBloomFilter: false,
        indexUsed: `mv:${routingResult.mvName}`,
      }

      return {
        rows: result.rows,
        totalCount: result.totalCount,
        nextCursor: result.nextCursor,
        hasMore: result.hasMore,
        stats: executionStats,
      }
    } catch (error) {
      // MV execution failed - log and fall back to regular execution
      logger.debug('MV-based query execution failed, falling back to full scan', error)
      return null
    }
  }

  /**
   * Execute query using a secondary index
   */
  private async executeWithIndex<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T>,
    indexPlan: SelectedIndex,
    startTime: number
  ): Promise<QueryResult<T> | null> {
    try {
      let candidateDocIds: string[] = []
      let usedFTS = false

      // Execute index lookup based on type
      // NOTE: Hash and SST indexes removed - equality and range queries now use native parquet predicate pushdown
      switch (indexPlan.type) {
        case 'fts': {
          const searchQuery = (filter.$text as { $search: string })?.$search
          if (searchQuery) {
            const results = await this.indexManager!.ftsSearch(ns, searchQuery, {
              limit: options.limit,
            })
            candidateDocIds = results.map(r => r.docId)
            usedFTS = true
          }
          break
        }

        case 'vector': {
          // Handle vector similarity search with hybrid filtering support
          const vectorCondition = indexPlan.condition as {
            // New format
            query?: number[] | string | undefined
            field?: string | undefined
            topK?: number | undefined
            minScore?: number | undefined
            strategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
            efSearch?: number | undefined
            // Legacy format
            $near?: number[] | undefined
            $k?: number | undefined
            $field?: string | undefined
            $minScore?: number | undefined
          }

          // Support both new and legacy format
          const queryVector = vectorCondition.query ?? vectorCondition.$near
          const topK = vectorCondition.topK ?? vectorCondition.$k
          const minScore = vectorCondition.minScore ?? vectorCondition.$minScore
          const strategy = vectorCondition.strategy ?? 'auto'
          const efSearch = vectorCondition.efSearch

          if (queryVector && typeof queryVector !== 'string' && topK) {
            // Check if there are metadata filters (fields other than $vector)
            const metadataFilter = this.extractMetadataFilter(filter)
            const hasMetadataFilter = Object.keys(metadataFilter).length > 0

            if (hasMetadataFilter) {
              // Hybrid search: combine vector similarity with metadata filtering
              const hybridResult = await this.executeHybridVectorSearch<T>(
                ns,
                indexPlan.index.name,
                queryVector,
                topK,
                metadataFilter,
                { minScore, strategy, efSearch },
                startTime
              )
              if (hybridResult) {
                return hybridResult
              }
            } else {
              // Pure vector search (no metadata filtering)
              const vectorResult = await this.indexManager!.vectorSearch(
                ns,
                indexPlan.index.name,
                queryVector,
                topK,
                { minScore, efSearch }
              )

              // Vector search returns results ordered by similarity
              candidateDocIds = vectorResult.docIds
            }
          }
          break
        }

        case 'geo': {
          // Handle geo proximity search
          const geoCondition = indexPlan.condition as {
            // $geo operator format
            $near?: { lat: number; lng: number } | [number, number] | undefined
            $maxDistance?: number | undefined
            $minDistance?: number | undefined
            limit?: number | undefined
            // Field-level $near format (MongoDB style)
            lat?: number | undefined
            lng?: number | undefined
          }

          // Extract center point
          let centerLat: number | undefined
          let centerLng: number | undefined

          if (geoCondition.$near) {
            if (Array.isArray(geoCondition.$near)) {
              // [lng, lat] format (GeoJSON style)
              centerLng = geoCondition.$near[0]
              centerLat = geoCondition.$near[1]
            } else {
              // { lat, lng } format
              centerLat = geoCondition.$near.lat
              centerLng = geoCondition.$near.lng
            }
          }

          if (typeof centerLat === 'number' && typeof centerLng === 'number') {
            const geoResult = await this.indexManager!.geoSearch(
              ns,
              indexPlan.index.name,
              centerLat,
              centerLng,
              {
                maxDistance: geoCondition.$maxDistance,
                minDistance: geoCondition.$minDistance,
                limit: geoCondition.limit ?? options.limit,
              }
            )

            // Return results with distance info
            const geoDocIds = geoResult.docIds
            if (geoDocIds.length === 0) {
              return {
                rows: [],
                hasMore: false,
                stats: {
                  totalRowGroups: 0,
                  scannedRowGroups: 0,
                  skippedRowGroups: 0,
                  rowsScanned: geoResult.entriesScanned,
                  rowsMatched: 0,
                  executionTimeMs: Date.now() - startTime,
                  columnsRead: [],
                  usedBloomFilter: false,
                  indexUsed: indexPlan.index.name,
                  indexType: 'geo',
                },
              }
            }

            // Load matching documents
            const dataPath = `data/${ns}/data.parquet`
            const metadata = await this.reader.readMetadata(dataPath)
            const columns = this.selectColumns(filter, options)
            const allRows = await this.reader.readAll<T>(dataPath, columns.length > 0 ? columns : undefined)

            // Filter to matching documents and add distance
            const candidateSet = new Set(geoDocIds)
            const distanceMap = new Map<string, number>()
            geoDocIds.forEach((id, i) => {
              distanceMap.set(id, geoResult.distances[i] ?? 0)
            })

            let filtered = allRows.filter(row => {
              const id = (row as Record<string, unknown>).$id as string
              return candidateSet.has(id)
            })

            // Sort by distance (already ordered from index, but maintain order)
            filtered.sort((a, b) => {
              const idA = (a as Record<string, unknown>).$id as string
              const idB = (b as Record<string, unknown>).$id as string
              return (distanceMap.get(idA) ?? 0) - (distanceMap.get(idB) ?? 0)
            })

            // Add distance to each result
            filtered = filtered.map(row => ({
              ...row as object,
              $distance: distanceMap.get((row as Record<string, unknown>).$id as string),
            })) as T[]

            // Apply limit
            if (options.limit && filtered.length > options.limit) {
              filtered = filtered.slice(0, options.limit)
            }

            return {
              rows: filtered,
              hasMore: false,
              stats: {
                totalRowGroups: metadata.rowGroups?.length ?? 0,
                scannedRowGroups: 1,
                skippedRowGroups: 0,
                rowsScanned: allRows.length,
                rowsMatched: filtered.length,
                executionTimeMs: Date.now() - startTime,
                columnsRead: columns,
                usedBloomFilter: false,
                indexUsed: indexPlan.index.name,
                indexType: 'geo',
              },
            }
          }
          break
        }
      }

      if (candidateDocIds.length === 0) {
        // Index found no matches - return empty result
        return {
          rows: [],
          hasMore: false,
          stats: {
            totalRowGroups: 0,
            scannedRowGroups: 0,
            skippedRowGroups: 0,
            rowsScanned: 0,
            rowsMatched: 0,
            executionTimeMs: Date.now() - startTime,
            columnsRead: [],
            usedBloomFilter: false,
          },
        }
      }

      // Load metadata for reading specific documents
      const dataPath = `data/${ns}/data.parquet`
      const metadata = await this.reader.readMetadata(dataPath)
      const columns = this.selectColumns(filter, options)

      // Read all rows and filter by candidate IDs
      // Performance note: Row group filtering based on index hints could optimize this further
      // by using candidate docIds to determine which row groups contain relevant data
      const allRows = await this.reader.readAll<T>(dataPath, columns.length > 0 ? columns : undefined)

      // Filter to candidate documents
      const candidateSet = new Set(candidateDocIds)
      let filtered = allRows.filter(row => {
        const id = (row as Record<string, unknown>).$id as string
        return candidateSet.has(id)
      })

      // Apply remaining filter conditions (excluding the indexed field)
      if (!usedFTS) {
        const predicate = toPredicate(filter)
        filtered = filtered.filter(row => predicate(row))
      }

      // Apply post-processing (sort, limit, skip)
      const result = this.postProcess(filtered, options)

      return {
        rows: result.rows,
        totalCount: result.totalCount,
        nextCursor: result.nextCursor,
        hasMore: result.hasMore,
        stats: {
          totalRowGroups: metadata.rowGroups?.length ?? 0,
          scannedRowGroups: 1, // Simplified - we read all for now
          skippedRowGroups: 0,
          rowsScanned: allRows.length,
          rowsMatched: filtered.length,
          executionTimeMs: Date.now() - startTime,
          columnsRead: columns,
          usedBloomFilter: false,
        },
      }
    } catch (error: unknown) {
      // Index execution failed - fall back to full scan
      logger.debug('Index-based query execution failed, falling back to full scan', error)
      return null
    }
  }

  /**
   * Execute hybrid vector search combining vector similarity with metadata filtering.
   *
   * Supports two strategies:
   * - pre-filter: Apply metadata filters first, then vector search on filtered candidates
   * - post-filter: Perform vector search first, then filter results by metadata
   */
  private async executeHybridVectorSearch<T>(
    ns: string,
    indexName: string,
    queryVector: number[],
    topK: number,
    metadataFilter: Filter,
    options: {
      minScore?: number | undefined
      strategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
      efSearch?: number | undefined
    },
    startTime: number
  ): Promise<QueryResult<T> | null> {
    const strategy = options.strategy ?? 'auto'

    // Load data for filtering
    const dataPath = `data/${ns}/data.parquet`
    const metadata = await this.reader.readMetadata(dataPath)

    // Read all rows for filtering
    const allRows = await this.reader.readAll<T>(dataPath)
    const predicate = toPredicate(metadataFilter)

    // Determine strategy
    let actualStrategy = strategy
    if (strategy === 'auto') {
      // Estimate filter selectivity by sampling or use heuristics
      const totalRows = allRows.length
      const matchingRows = allRows.filter(row => predicate(row)).length
      const selectivity = totalRows > 0 ? matchingRows / totalRows : 1

      // Use pre-filter if filtering removes more than 50% of documents
      actualStrategy = selectivity < 0.5 ? 'pre-filter' : 'post-filter'
    }

    let results: T[]
    let vectorScores: Map<string, number> = new Map()

    if (actualStrategy === 'pre-filter') {
      // Pre-filter: First filter by metadata, then vector search on candidates
      const filteredRows = allRows.filter(row => predicate(row))
      const candidateIds = new Set(
        filteredRows.map(row => (row as Record<string, unknown>).$id as string)
      )

      // Perform hybrid search with candidate restriction
      const hybridResult = await this.indexManager!.hybridSearch(
        ns,
        indexName,
        queryVector,
        topK,
        {
          strategy: 'pre-filter',
          candidateIds,
          minScore: options.minScore,
          efSearch: options.efSearch,
        }
      )

      // Map results back to rows
      const resultIdSet = new Set(hybridResult.docIds)
      results = filteredRows.filter(row => {
        const id = (row as Record<string, unknown>).$id as string
        return resultIdSet.has(id)
      })

      // Store scores for sorting
      hybridResult.docIds.forEach((docId, i) => {
        vectorScores.set(docId, hybridResult.scores?.[i] ?? 0)
      })

      // Sort by vector similarity score
      results.sort((a, b) => {
        const scoreA = vectorScores.get((a as Record<string, unknown>).$id as string) ?? 0
        const scoreB = vectorScores.get((b as Record<string, unknown>).$id as string) ?? 0
        return scoreB - scoreA // Descending by score
      })
    } else {
      // Post-filter: Perform vector search with over-fetching, then filter
      const overFetchMultiplier = 3
      const hybridResult = await this.indexManager!.hybridSearch(
        ns,
        indexName,
        queryVector,
        topK * overFetchMultiplier,
        {
          strategy: 'post-filter',
          minScore: options.minScore,
          efSearch: options.efSearch,
          overFetchMultiplier,
        }
      )

      // Store scores
      hybridResult.docIds.forEach((docId, i) => {
        vectorScores.set(docId, hybridResult.scores?.[i] ?? 0)
      })

      // Filter vector results by metadata
      const resultIdSet = new Set(hybridResult.docIds)
      const candidateRows = allRows.filter(row => {
        const id = (row as Record<string, unknown>).$id as string
        return resultIdSet.has(id)
      })

      // Apply metadata filter
      results = candidateRows.filter(row => predicate(row))

      // Sort by vector similarity score
      results.sort((a, b) => {
        const scoreA = vectorScores.get((a as Record<string, unknown>).$id as string) ?? 0
        const scoreB = vectorScores.get((b as Record<string, unknown>).$id as string) ?? 0
        return scoreB - scoreA
      })

      // Limit to topK after filtering
      results = results.slice(0, topK)
    }

    return {
      rows: results,
      hasMore: false,
      stats: {
        totalRowGroups: metadata.rowGroups?.length ?? 0,
        scannedRowGroups: 1,
        skippedRowGroups: 0,
        rowsScanned: allRows.length,
        rowsMatched: results.length,
        executionTimeMs: Date.now() - startTime,
        columnsRead: [],
        usedBloomFilter: false,
        indexUsed: indexName,
        indexType: 'vector',
      },
    }
  }

  /**
   * Extract metadata filter from a filter that includes $vector.
   * Returns a filter with $vector removed.
   */
  private extractMetadataFilter(filter: Filter): Filter {
    const result: Filter = {}

    for (const [key, value] of Object.entries(filter)) {
      // Skip $vector and other special operators we don't want in metadata filter
      if (key === '$vector') continue

      result[key] = value
    }

    return result
  }

  /**
   * Execute query with full scan and predicate pushdown
   */
  private async executeFullScan<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T>,
    startTime: number
  ): Promise<QueryResult<T>> {
    // 1. Load metadata
    const dataPath = `data/${ns}/data.parquet`
    const metadata = await this.reader.readMetadata(dataPath)

    // 2. Get row group stats
    const stats = extractRowGroupStats(metadata)

    // 3. Select row groups using predicate pushdown
    const selectedRowGroups = selectRowGroups(filter, stats)

    // 4. Determine columns to read
    const columns = this.selectColumns(filter, options)

    // 5. Check bloom filters for equality checks (if available)
    const bloomFilteredGroups = await this.applyBloomFilters(
      dataPath,
      filter,
      selectedRowGroups,
      stats
    )

    // 6. Read selected row groups in parallel
    const rowBatches = await this.readRowGroupsParallel<T>(
      dataPath,
      bloomFilteredGroups,
      columns
    )

    // 7. Flatten row batches
    const allRows = rowBatches.flat()

    // 8. Apply filter (post-predicate pushdown)
    const predicate = toPredicate(filter)
    const filtered = allRows.filter(row => predicate(row))

    // 9. Apply sort, limit, skip
    const result = this.postProcess(filtered, options)

    // 10. Build statistics
    const executionStats: QueryStats = {
      totalRowGroups: stats.length,
      scannedRowGroups: bloomFilteredGroups.length,
      skippedRowGroups: stats.length - bloomFilteredGroups.length,
      rowsScanned: allRows.length,
      rowsMatched: filtered.length,
      executionTimeMs: Date.now() - startTime,
      columnsRead: columns,
      usedBloomFilter: bloomFilteredGroups.length < selectedRowGroups.length,
    }

    return {
      rows: result.rows,
      totalCount: result.totalCount,
      nextCursor: result.nextCursor,
      hasMore: result.hasMore,
      stats: executionStats,
    }
  }

  /**
   * Extract range query operators from a condition
   * @internal Reserved for future range query optimization
   */
  public _extractRangeQuery(condition: unknown): {
    $gt?: unknown | undefined
    $gte?: unknown | undefined
    $lt?: unknown | undefined
    $lte?: unknown | undefined
  } | null {
    if (typeof condition !== 'object' || condition === null) {
      return null
    }

    const obj = condition as Record<string, unknown>
    const range: { $gt?: unknown | undefined; $gte?: unknown | undefined; $lt?: unknown | undefined; $lte?: unknown | undefined } = {}

    if ('$gt' in obj) range.$gt = obj.$gt
    if ('$gte' in obj) range.$gte = obj.$gte
    if ('$lt' in obj) range.$lt = obj.$lt
    if ('$lte' in obj) range.$lte = obj.$lte

    if (Object.keys(range).length === 0) {
      return null
    }

    return range
  }

  /**
   * Explain query plan without executing
   *
   * @param ns - Namespace (collection name)
   * @param filter - MongoDB-style filter
   * @param options - Find options
   * @returns Query plan
   */
  async explain<T>(
    ns: string,
    filter: Filter,
    options: FindOptions<T> = {}
  ): Promise<QueryPlan> {
    const startTime = Date.now()
    const hookContext = createQueryContext('explain', ns, filter, options as FindOptions<unknown>)

    // Dispatch query start hook
    await globalHookRegistry.dispatchQueryStart(hookContext)

    try {
      // Load metadata
      const dataPath = `data/${ns}/data.parquet`
      const metadata = await this.reader.readMetadata(dataPath)

    // Get row group stats
    const stats = extractRowGroupStats(metadata)

    // Select row groups using predicate pushdown
    const selectedRowGroups = selectRowGroups(filter, stats)

    // Extract filter fields
    const filterFields = extractFilterFields(filter)

    // Determine columns to read
    const columns = this.selectColumns(filter, options)

    // Check for logical operators
    const hasLogicalOps = !!(filter.$and || filter.$or || filter.$not || filter.$nor)

    // Check for special operators
    const hasSpecialOps = !!(filter.$text || filter.$vector || filter.$geo)

    // Check if data is sorted
    const sortFields = options.sort ? Object.keys(options.sort) : []
    const canUseSortedData = this.canUseSortedData(metadata, sortFields)

    // Calculate estimated savings
    const rowGroupSavings = stats.length - selectedRowGroups.length
    const savingsPercent = stats.length > 0
      ? ((rowGroupSavings / stats.length) * 100).toFixed(1)
      : '0.0'

      const plan: QueryPlan = {
        filter: {
          original: filter,
          fields: filterFields,
          hasLogicalOps,
          hasSpecialOps,
        },
        predicatePushdown: {
          enabled: true,
          rowGroupsTotal: stats.length,
          rowGroupsSelected: selectedRowGroups.length,
          estimatedSavings: `${savingsPercent}% of row groups skipped`,
        },
        projection: {
          columns,
          isFullScan: columns.length === 0,
        },
        sort: {
          fields: sortFields,
          canUseSortedData,
        },
      }

      // Add vector search plan if applicable
      if (filter.$vector) {
        const vq = filter.$vector as {
          query?: number[] | string | undefined
          field?: string | undefined
          topK?: number | undefined
          minScore?: number | undefined
          efSearch?: number | undefined
          strategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
          $near?: number[] | undefined
          $k?: number | undefined
          $field?: string | undefined
          $minScore?: number | undefined
        }

        const field = vq.field ?? vq.$field ?? 'embedding'
        const topK = vq.topK ?? vq.$k ?? 10
        const efSearch = vq.efSearch ?? Math.max(topK * 2, 50)

        // Check if this is a hybrid search
        const metadataFilterKeys = Object.keys(filter).filter(
          k => k !== '$vector' && k !== '$and' && k !== '$or' && k !== '$not'
        )
        const isHybrid = metadataFilterKeys.length > 0

        // Estimate filter selectivity for hybrid search
        let filterSelectivity: number | undefined
        let strategyReason: string | undefined

        if (isHybrid) {
          // Simple heuristic: estimate based on number of filter conditions
          filterSelectivity = Math.max(0.1, 1 - (metadataFilterKeys.length * 0.2))
          const strategy = vq.strategy ?? 'auto'

          if (strategy === 'auto') {
            const usePreFilter = filterSelectivity < 0.3
            strategyReason = usePreFilter
              ? `Auto-selected pre-filter: filter selectivity ${(filterSelectivity * 100).toFixed(0)}% is low enough for efficient candidate filtering`
              : `Auto-selected post-filter: filter selectivity ${(filterSelectivity * 100).toFixed(0)}% is too high for pre-filtering to be efficient`
          } else {
            strategyReason = `Strategy explicitly set to ${strategy}`
          }
        }

        // Get index name from index manager if available
        let indexName = `idx_${field}`
        if (this.indexManager) {
          const selectedIndex = await this.indexManager.selectIndex(ns, filter)
          if (selectedIndex?.index) {
            indexName = selectedIndex.index.name
          }
        }

        plan.vectorSearch = {
          indexName,
          field,
          topK,
          efSearch,
          minScore: vq.minScore ?? vq.$minScore,
          isHybrid,
          hybridStrategy: isHybrid ? (vq.strategy ?? 'auto') : undefined,
          strategyReason,
          filterSelectivity,
        }
      }

      // Dispatch query end hook
      await globalHookRegistry.dispatchQueryEnd(hookContext, {
        rowCount: 0,
        durationMs: Date.now() - startTime,
      })

      return plan
    } catch (error) {
      // Dispatch query error hook
      await globalHookRegistry.dispatchQueryError(
        hookContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  /**
   * Execute aggregation query
   *
   * @param ns - Namespace (collection name)
   * @param pipeline - Aggregation pipeline stages
   * @param options - Aggregation options
   */
  async aggregate<T, R>(
    ns: string,
    pipeline: AggregationStage[],
    options: AggregationOptions = {}
  ): Promise<R[]> {
    const startTime = Date.now()

    // Extract $match stage if present (for predicate pushdown)
    const matchStage = pipeline.find(
      (stage): stage is { $match: Filter } => '$match' in stage
    )
    const filter = matchStage?.$match ?? {}

    const hookContext = createQueryContext('aggregate', ns, filter)
    // Store pipeline in metadata
    hookContext.metadata = { pipeline }

    // Dispatch query start hook
    await globalHookRegistry.dispatchQueryStart(hookContext)

    try {
      // Execute base query
      const result = await this.execute<T>(ns, filter, {
        includeDeleted: options.includeDeleted,
        asOf: options.asOf,
      })

      // Apply remaining pipeline stages
      let data: unknown[] = result.rows

      for (const stage of pipeline) {
        if ('$match' in stage) {
          // Already applied via predicate pushdown
          continue
        }
        data = this.applyAggregationStage(data, stage)
      }

      // Dispatch query end hook
      await globalHookRegistry.dispatchQueryEnd(hookContext, {
        rowCount: data.length,
        durationMs: Date.now() - startTime,
      })

      return data as R[]
    } catch (error) {
      // Dispatch query error hook
      await globalHookRegistry.dispatchQueryError(
        hookContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  /**
   * Select columns to read based on filter and options
   */
  private selectColumns<T>(filter: Filter, options: FindOptions<T>): string[] {
    // Extract fields from filter
    const filterFields = extractFilterFields(filter)

    // Extract fields from projection
    const projectFields = options.project
      ? Object.keys(options.project).filter(k => {
          const val = (options.project as Projection)[k]
          return val === 1 || val === true
        })
      : []

    // Extract fields from sort
    const sortFields = options.sort ? Object.keys(options.sort) : []

    // Combine all fields (use Set to deduplicate)
    const allFields = new Set([
      ...filterFields,
      ...projectFields,
      ...sortFields,
      // Always include core fields
      '$id',
      '$type',
      'name',
      'createdAt',
      'updatedAt',
      'deletedAt',
      'version',
    ])

    // If projection excludes fields (value = 0), we need all columns
    if (options.project) {
      const hasExclusions = Object.values(options.project).some(v => v === 0 || v === false)
      if (hasExclusions) {
        return [] // Empty array means read all columns
      }
    }

    return Array.from(allFields)
  }

  /**
   * Apply bloom filters to further filter row groups
   */
  private async applyBloomFilters(
    path: string,
    filter: Filter,
    rowGroups: number[],
    stats: RowGroupStats[]
  ): Promise<number[]> {
    // Extract equality conditions that could use bloom filters
    const equalityConditions = this.extractEqualityConditions(filter)

    if (equalityConditions.length === 0) {
      return rowGroups
    }

    // For each row group, check bloom filters
    const filteredGroups: number[] = []

    for (const rgIndex of rowGroups) {
      const rgStats = stats.find(s => s.rowGroup === rgIndex)
      if (!rgStats) {
        filteredGroups.push(rgIndex)
        continue
      }

      let mightContain = true

      for (const { field, value } of equalityConditions) {
        const colStats = rgStats.columns.get(field)

        // If column has bloom filter, check it
        if (colStats?.hasBloomFilter) {
          const bloomFilter = await this.reader.getBloomFilter(path, rgIndex, field)
          if (bloomFilter && !bloomFilter.mightContain(value)) {
            // Bloom filter says value is definitely not present
            mightContain = false
            break
          }
        }
      }

      if (mightContain) {
        filteredGroups.push(rgIndex)
      }
    }

    return filteredGroups
  }

  /**
   * Extract equality conditions from filter
   */
  private extractEqualityConditions(filter: Filter): Array<{ field: string; value: unknown }> {
    const conditions: Array<{ field: string; value: unknown }> = []

    for (const [field, value] of Object.entries(filter)) {
      if (field.startsWith('$')) continue

      if (value === null || typeof value !== 'object') {
        // Direct equality
        conditions.push({ field, value })
      } else if ('$eq' in (value as Record<string, unknown>)) {
        conditions.push({ field, value: (value as { $eq: unknown }).$eq })
      }
    }

    return conditions
  }

  /**
   * Read row groups in parallel with concurrency limit
   */
  private async readRowGroupsParallel<T>(
    path: string,
    rowGroups: number[],
    columns: string[]
  ): Promise<T[][]> {
    // Limit concurrency to avoid overwhelming storage
    const CONCURRENCY = DEFAULT_CONCURRENCY

    const batches: number[][] = []
    for (let i = 0; i < rowGroups.length; i += CONCURRENCY) {
      batches.push(rowGroups.slice(i, i + CONCURRENCY))
    }

    const results: T[][] = []

    for (const batch of batches) {
      const batchResults = await Promise.all(
        batch.map(rg =>
          this.reader.readRowGroups<T>(path, [rg], columns.length > 0 ? columns : undefined)
        )
      )
      results.push(...batchResults)
    }

    return results
  }

  /**
   * Apply post-processing (sort, limit, skip)
   */
  private postProcess<T>(
    rows: T[],
    options: FindOptions<T>
  ): { rows: T[]; totalCount?: number | undefined; nextCursor?: string | undefined; hasMore: boolean } {
    let result = rows

    // Apply soft-delete filter unless includeDeleted is true
    if (!options.includeDeleted) {
      result = result.filter(row => {
        const r = row as Record<string, unknown>
        return isNullish(r.deletedAt)
      })
    }

    // Apply sort
    if (options.sort) {
      result = this.sortRows(result, options.sort)
    }

    // Calculate total before pagination
    const totalCount = result.length

    // Apply skip
    if (options.skip && options.skip > 0) {
      result = result.slice(options.skip)
    }

    // Apply limit
    let hasMore = false
    if (options.limit && options.limit > 0) {
      if (result.length > options.limit) {
        hasMore = true
        result = result.slice(0, options.limit)
      }
    }

    // Generate cursor for pagination
    let nextCursor: string | undefined
    if (hasMore && result.length > 0) {
      const lastRow = result[result.length - 1] as Record<string, unknown>
      nextCursor = this.generateCursor(lastRow, options.sort)
    }

    // Apply projection
    if (options.project) {
      result = this.applyProjection(result, options.project)
    }

    return { rows: result, totalCount, nextCursor, hasMore }
  }

  /**
   * Sort rows by sort specification
   */
  private sortRows<T>(rows: T[], sort: SortSpec): T[] {
    const sortEntries = Object.entries(sort)
    if (sortEntries.length === 0) return rows

    return [...rows].sort((a, b) => {
      for (const [field, direction] of sortEntries) {
        const aVal = this.getFieldValue(a, field)
        const bVal = this.getFieldValue(b, field)

        const cmp = this.compareValues(aVal, bVal)
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
  private getFieldValue(obj: unknown, path: string): unknown {
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
  private compareValues(a: unknown, b: unknown): number {
    // Nulls sort first
    if (isNullish(a)) {
      return isNullish(b) ? 0 : -1
    }
    if (isNullish(b)) return 1

    // Same type comparisons
    if (typeof a === 'number' && typeof b === 'number') return a - b
    if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b)
    if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
    if (typeof a === 'boolean' && typeof b === 'boolean') return (a ? 1 : 0) - (b ? 1 : 0)

    // Fallback to string comparison
    return String(a).localeCompare(String(b))
  }

  /**
   * Generate cursor for pagination
   */
  private generateCursor(row: Record<string, unknown>, sort?: SortSpec): string {
    // Include sort fields and $id in cursor
    const cursorData: Record<string, unknown> = {
      $id: row.$id,
    }

    if (sort) {
      for (const field of Object.keys(sort)) {
        cursorData[field] = this.getFieldValue(row, field)
      }
    }

    // Base64 encode the cursor (Worker-safe)
    return stringToBase64(JSON.stringify(cursorData))
  }

  /**
   * Apply projection to rows
   */
  private applyProjection<T>(rows: T[], projection: Projection): T[] {
    const includes = new Set<string>()
    const excludes = new Set<string>()

    for (const [field, value] of Object.entries(projection)) {
      if (value === 1 || value === true) {
        includes.add(field)
      } else {
        excludes.add(field)
      }
    }

    // Always include core fields
    const coreFields = ['$id', '$type', 'name']
    for (const field of coreFields) {
      includes.add(field)
      excludes.delete(field)
    }

    return rows.map(row => {
      const obj = row as Record<string, unknown>
      const result: Record<string, unknown> = {}

      if (includes.size > coreFields.length) {
        // Include mode: only specified fields
        const includeArr = Array.from(includes)
        for (const field of includeArr) {
          if (field in obj) {
            result[field] = obj[field]
          }
        }
      } else if (excludes.size > 0) {
        // Exclude mode: all except specified fields
        for (const [key, value] of Object.entries(obj)) {
          if (!excludes.has(key)) {
            result[key] = value
          }
        }
      } else {
        // No projection: return all
        return row
      }

      return result as T
    })
  }

  /**
   * Check if data is already sorted by the requested fields
   */
  private canUseSortedData(metadata: ParquetMetadata, sortFields: string[]): boolean {
    // Check if the Parquet file has sorting columns metadata
    // This is a simplified check - real implementation would check
    // the actual sorting columns in the metadata
    if (sortFields.length === 0) return true

    // Check key-value metadata for sorting info
    const sortingCols = metadata.keyValueMetadata?.find(
      kv => kv.key === 'parquet.sorting_columns'
    )

    if (sortingCols) {
      try {
        const sortedBy = JSON.parse(sortingCols.value) as string[]
        return sortFields.every((f, i) => sortedBy[i] === f)
      } catch {
        // Intentionally ignored: invalid JSON in sorting_columns metadata means data is not pre-sorted
        return false
      }
    }

    return false
  }

  /**
   * Apply a single aggregation stage
   */
  private applyAggregationStage(data: unknown[], stage: AggregationStage): unknown[] {
    if ('$sort' in stage) {
      return this.sortRows(data, stage.$sort)
    }

    if ('$limit' in stage) {
      return data.slice(0, stage.$limit)
    }

    if ('$skip' in stage) {
      return data.slice(stage.$skip)
    }

    if ('$project' in stage) {
      return this.applyProjection(data, stage.$project as Projection)
    }

    if ('$group' in stage) {
      return this.applyGroup(data, stage.$group)
    }

    if ('$unwind' in stage) {
      return this.applyUnwind(data, stage.$unwind)
    }

    if ('$count' in stage) {
      return [{ [stage.$count]: data.length }]
    }

    if ('$addFields' in stage || '$set' in stage) {
      const fields = '$addFields' in stage ? stage.$addFields : stage.$set
      return data.map(row => ({
        ...(row as object),
        ...this.evaluateAddFields(row, fields),
      }))
    }

    if ('$unset' in stage) {
      const fieldsToRemove = Array.isArray(stage.$unset) ? stage.$unset : [stage.$unset]
      return data.map(row => {
        const result = { ...(row as object) }
        for (const field of fieldsToRemove) {
          delete (result as Record<string, unknown>)[field]
        }
        return result
      })
    }

    // $lookup would require access to other collections
    // For now, return data unchanged
    if ('$lookup' in stage) {
      logger.warn('$lookup requires cross-collection access, skipping')
      return data
    }

    return data
  }

  /**
   * Apply $group aggregation stage
   */
  private applyGroup(
    data: unknown[],
    group: { _id: unknown; [key: string]: unknown }
  ): unknown[] {
    const groups = new Map<string, { _id: unknown; rows: unknown[] }>()

    for (const row of data) {
      const groupKey = this.evaluateExpression(row, group._id)
      const keyStr = JSON.stringify(groupKey)

      if (!groups.has(keyStr)) {
        groups.set(keyStr, { _id: groupKey, rows: [] })
      }
      groups.get(keyStr)!.rows.push(row)
    }

    const results: unknown[] = []
    const groupValues = Array.from(groups.values())

    for (const { _id, rows } of groupValues) {
      const result: Record<string, unknown> = { _id }

      for (const [field, expr] of Object.entries(group)) {
        if (field === '_id') continue

        if (typeof expr === 'object' && expr !== null) {
          const exprObj = expr as Record<string, unknown>
          if ('$sum' in exprObj) {
            result[field] = this.sumField(rows, exprObj.$sum)
          } else if ('$avg' in exprObj) {
            result[field] = this.avgField(rows, exprObj.$avg)
          } else if ('$min' in exprObj) {
            result[field] = this.minField(rows, exprObj.$min)
          } else if ('$max' in exprObj) {
            result[field] = this.maxField(rows, exprObj.$max)
          } else if ('$first' in exprObj) {
            result[field] = rows.length > 0 ? this.evaluateExpression(rows[0], exprObj.$first) : null
          } else if ('$last' in exprObj) {
            result[field] = rows.length > 0 ? this.evaluateExpression(rows[rows.length - 1], exprObj.$last) : null
          } else if ('$push' in exprObj) {
            result[field] = rows.map(r => this.evaluateExpression(r, exprObj.$push))
          }
        }
      }

      results.push(result)
    }

    return results
  }

  /**
   * Apply $unwind aggregation stage
   */
  private applyUnwind(
    data: unknown[],
    unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean | undefined }
  ): unknown[] {
    const path = typeof unwind === 'string' ? unwind : unwind.path
    const preserveNullAndEmpty =
      typeof unwind === 'object' ? unwind.preserveNullAndEmptyArrays ?? false : false

    // Remove leading $ from path
    const fieldPath = path.startsWith('$') ? path.slice(1) : path

    const results: unknown[] = []

    for (const row of data) {
      const arrayValue = this.getFieldValue(row, fieldPath)

      if (!Array.isArray(arrayValue)) {
        if (preserveNullAndEmpty) {
          results.push({ ...(row as object), [fieldPath]: null })
        }
        continue
      }

      if (arrayValue.length === 0) {
        if (preserveNullAndEmpty) {
          results.push({ ...(row as object), [fieldPath]: null })
        }
        continue
      }

      for (const item of arrayValue) {
        results.push({ ...(row as object), [fieldPath]: item })
      }
    }

    return results
  }

  /**
   * Evaluate an expression (e.g., '$fieldName' or literal)
   */
  private evaluateExpression(row: unknown, expr: unknown): unknown {
    if (typeof expr === 'string' && expr.startsWith('$')) {
      const path = expr.slice(1)
      return this.getFieldValue(row, path)
    }

    if (typeof expr === 'object' && expr !== null) {
      // Handle expression operators
      const exprObj = expr as Record<string, unknown>

      if ('$gt' in exprObj) {
        const [left, right] = exprObj.$gt as unknown[]
        const leftVal = this.evaluateExpression(row, left)
        const rightVal = this.evaluateExpression(row, right)
        return this.compareValues(leftVal, rightVal) > 0
      }

      // Add more expression operators as needed
    }

    return expr
  }

  /**
   * Evaluate $addFields expressions
   */
  private evaluateAddFields(
    row: unknown,
    fields: Record<string, unknown>
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {}

    for (const [field, expr] of Object.entries(fields)) {
      result[field] = this.evaluateExpression(row, expr)
    }

    return result
  }

  /**
   * Sum values for $sum accumulator
   */
  private sumField(rows: unknown[], expr: unknown): number {
    if (expr === 1) {
      // Count
      return rows.length
    }

    let sum = 0
    for (const row of rows) {
      const val = this.evaluateExpression(row, expr)
      sum += typeof val === 'number' ? val : 0
    }
    return sum
  }

  /**
   * Average values for $avg accumulator
   */
  private avgField(rows: unknown[], expr: unknown): number | null {
    if (rows.length === 0) return null

    const sum = this.sumField(rows, expr)
    return sum / rows.length
  }

  /**
   * Min value for $min accumulator
   */
  private minField(rows: unknown[], expr: unknown): unknown {
    if (rows.length === 0) return null

    let min: unknown = undefined

    for (const row of rows) {
      const val = this.evaluateExpression(row, expr)
      if (isNullish(val)) continue
      if (min === undefined || this.compareValues(val, min) < 0) {
        min = val
      }
    }

    return min ?? null
  }

  /**
   * Max value for $max accumulator
   */
  private maxField(rows: unknown[], expr: unknown): unknown {
    if (rows.length === 0) return null

    let max: unknown = undefined

    for (const row of rows) {
      const val = this.evaluateExpression(row, expr)
      if (isNullish(val)) continue
      if (max === undefined || this.compareValues(val, max) > 0) {
        max = val
      }
    }

    return max ?? null
  }
}

// =============================================================================
// Aggregation Types
// =============================================================================

/** Aggregation pipeline stage */
export type AggregationStage =
  | { $match: Filter }
  | { $group: { _id: unknown; [key: string]: unknown } }
  | { $sort: Record<string, 1 | -1> }
  | { $limit: number }
  | { $skip: number }
  | { $project: Record<string, 0 | 1 | boolean | unknown> }
  | { $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean | undefined } }
  | { $lookup: { from: string; localField: string; foreignField: string; as: string } }
  | { $count: string }
  | { $addFields: Record<string, unknown> }
  | { $set: Record<string, unknown> }
  | { $unset: string | string[] }

/** Aggregation options */
export interface AggregationOptions {
  /** Maximum time in milliseconds */
  maxTimeMs?: number | undefined
  /** Allow disk use for large aggregations */
  allowDiskUse?: boolean | undefined
  /** Hint for index */
  hint?: string | { [field: string]: 1 | -1 } | undefined
  /** Include soft-deleted entities */
  includeDeleted?: boolean | undefined
  /** Time-travel */
  asOf?: Date | undefined
  /** Explain without executing */
  explain?: boolean | undefined
}

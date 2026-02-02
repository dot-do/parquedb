/**
 * Native Pushdown Benchmark Runner for ParqueDB
 *
 * Benchmarks for native Parquet predicate pushdown performance
 * using $index_* columns with row group statistics across
 * IMDB, O*NET, and UNSPSC datasets.
 */

import type { Filter } from '../types/filter'
import type { BenchmarkQuery } from './benchmark-queries'
import { ALL_QUERIES, QUERY_STATS } from './benchmark-queries'
import { QueryExecutor, type FindResult, type QueryStats } from './QueryExecutor'
import { ReadPath } from './ReadPath'
import { DEFAULT_CACHE_CONFIG } from './CacheStrategy'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

// Use the Cloudflare R2Bucket type directly
type R2Bucket = {
  get(key: string, options?: { range?: { offset: number; length: number } }): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<unknown>
  delete(key: string): Promise<void>
  head(key: string): Promise<{ size: number } | null>
  list(options?: { prefix?: string; limit?: number }): Promise<{ objects: { key: string; size: number; uploaded: Date }[]; truncated: boolean }>
}

/**
 * Individual query benchmark result
 *
 * Measures native Parquet predicate pushdown performance on $index_* columns.
 * Row group skipping is achieved via min/max statistics in Parquet metadata.
 */
export interface QueryBenchmarkResult {
  /** Query definition */
  query: BenchmarkQuery
  /** Query execution metrics */
  execution: {
    /** Latency percentiles */
    latencyMs: { p50: number; p95: number; avg: number }
    /** Raw latency values for debugging */
    rawLatencies?: number[]
    /** Rows scanned after pushdown */
    rowsScanned: number
    /** Rows returned after filtering */
    rowsReturned: number
    /** Total row groups in the file */
    rowGroupsTotal?: number
    /** Row groups read (after statistics filtering) */
    rowGroupsRead?: number
    /** Row groups skipped via statistics pushdown */
    rowGroupsSkipped?: number
    /** Success flag */
    success: boolean
    /** Error message if failed */
    error?: string
    /** Number of iterations that completed */
    iterations?: number
  }
  /** Pushdown effectiveness metrics */
  pushdown: {
    /** Whether filter uses $index_* columns (eligible for pushdown) */
    usesPushdownColumns: boolean
    /** Columns used for pushdown */
    pushdownColumns: string[]
    /** Selectivity: ratio of rows returned to rows scanned (0-1, lower = more selective) */
    selectivity: number
    /** Row group skip ratio (0-1, higher = better pushdown) */
    rowGroupSkipRatio: number
  }
}

/**
 * Dataset benchmark summary
 */
export interface DatasetBenchmarkSummary {
  /** Dataset ID */
  dataset: string
  /** Number of queries run */
  queryCount: number
  /** Average latency across queries */
  avgLatencyMs: number
  /** Best latency achieved */
  bestLatency: { queryId: string; latencyMs: number }
  /** Worst latency */
  worstLatency: { queryId: string; latencyMs: number }
  /** Queries that successfully used pushdown */
  pushdownUsedCount: number
  /** Average row group skip ratio */
  avgRowGroupSkipRatio: number
  /** Average selectivity */
  avgSelectivity: number
}

/**
 * Full benchmark result
 */
export interface PushdownBenchmarkResult {
  /** Benchmark metadata */
  metadata: {
    timestamp: string
    iterations: number
    datasets: string[]
    totalQueries: number
    durationMs: number
    /** Datasets skipped due to size limits */
    skippedDatasets?: string[]
  }
  /** Per-query results */
  queries: QueryBenchmarkResult[]
  /** Per-dataset summaries */
  datasetSummaries: DatasetBenchmarkSummary[]
  /** Overall summary */
  summary: {
    /** Average latency across all queries */
    avgLatencyMs: number
    /** Median latency */
    medianLatencyMs: number
    /** Best performing query */
    bestLatency: { queryId: string; latencyMs: number }
    /** Percentage of queries that used pushdown */
    pushdownUsageRate: number
    /** Average row group skip ratio */
    avgRowGroupSkipRatio: number
    /** By query category */
    byCategory: {
      equality: { count: number; avgLatencyMs: number; avgSkipRatio: number }
      range: { count: number; avgLatencyMs: number; avgSkipRatio: number }
      compound: { count: number; avgLatencyMs: number; avgSkipRatio: number }
      fts: { count: number; avgLatencyMs: number; avgSkipRatio: number }
    }
    /** By selectivity level */
    bySelectivity: {
      high: { count: number; avgLatencyMs: number }
      medium: { count: number; avgLatencyMs: number }
      low: { count: number; avgLatencyMs: number }
    }
  }
}

// Legacy alias for backwards compatibility
export type IndexedBenchmarkResult = PushdownBenchmarkResult

/**
 * Benchmark configuration
 */
export interface BenchmarkConfig {
  /** Number of iterations per query */
  iterations: number
  /** Warmup iterations (not counted) */
  warmupIterations: number
  /** Maximum queries per dataset (for quick runs) */
  maxQueriesPerDataset?: number
  /** Specific datasets to benchmark */
  datasets?: Array<'imdb' | 'imdb-1m' | 'onet-full' | 'unspsc-full'>
  /** Query categories to include */
  categories?: BenchmarkQuery['category'][]
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Run native pushdown benchmarks
 *
 * Measures query performance using native Parquet predicate pushdown
 * on $index_* columns. Row groups are skipped based on min/max statistics.
 *
 * @param bucket - R2 bucket containing datasets
 * @param cache - Cache API for metadata caching
 * @param config - Benchmark configuration
 * @returns Full benchmark results
 */
export async function runPushdownBenchmark(
  bucket: R2Bucket,
  cache: Cache,
  config: BenchmarkConfig = {
    iterations: 5,
    warmupIterations: 1,
  }
): Promise<PushdownBenchmarkResult> {
  const startTime = performance.now()

  // Initialize query executor - cast bucket to global R2Bucket for compatibility
  const readPath = new ReadPath(bucket as unknown as globalThis.R2Bucket, cache, DEFAULT_CACHE_CONFIG)
  const queryExecutor = new QueryExecutor(
    readPath,
    bucket as unknown as globalThis.R2Bucket,
    undefined,
    undefined
  )

  // Select queries to run
  let queries = [...ALL_QUERIES]

  // Filter by dataset
  if (config.datasets) {
    queries = queries.filter(q => config.datasets!.includes(q.dataset))
  }

  // Skip large datasets that exceed Worker memory limits
  // The imdb-1m dataset (47MB parquet + 224MB FTS index) causes OOM
  const LARGE_DATASETS = ['imdb-1m']
  const skippedDatasetNames = [...new Set(queries.filter(q => LARGE_DATASETS.includes(q.dataset)).map(q => q.dataset))]
  if (skippedDatasetNames.length > 0) {
    logger.warn(`Skipping queries from large datasets (${skippedDatasetNames.join(', ')}) - exceeds Worker memory limits`)
    queries = queries.filter(q => !LARGE_DATASETS.includes(q.dataset))
  }

  // Filter by category
  if (config.categories) {
    queries = queries.filter(q => config.categories!.includes(q.category))
  }

  // Limit queries per dataset
  if (config.maxQueriesPerDataset) {
    const limitedQueries: BenchmarkQuery[] = []
    const datasets = new Set(queries.map(q => q.dataset))
    for (const dataset of datasets) {
      const datasetQueries = queries.filter(q => q.dataset === dataset)
      limitedQueries.push(...datasetQueries.slice(0, config.maxQueriesPerDataset))
    }
    queries = limitedQueries
  }

  // Run benchmarks
  const results: QueryBenchmarkResult[] = []

  for (const query of queries) {
    const result = await benchmarkQuery(queryExecutor, query, config)
    results.push(result)
  }

  // Calculate summaries
  const datasetSummaries = calculateDatasetSummaries(results)
  const summary = calculateOverallSummary(results)

  const durationMs = performance.now() - startTime

  return {
    metadata: {
      timestamp: new Date().toISOString(),
      iterations: config.iterations,
      datasets: [...new Set(queries.map(q => q.dataset))],
      totalQueries: queries.length,
      durationMs: Math.round(durationMs),
      skippedDatasets: skippedDatasetNames.length > 0 ? skippedDatasetNames : undefined,
    },
    queries: results,
    datasetSummaries,
    summary,
  }
}

// Legacy alias for backwards compatibility
export const runIndexedBenchmark = runPushdownBenchmark

/**
 * R2 prefix for benchmark data files
 */
const BENCHMARK_DATA_PREFIX = 'benchmark-data'

/**
 * Get the R2 namespace path for a query (with benchmark-data prefix)
 */
function getBenchmarkNamespace(query: BenchmarkQuery): string {
  return `${BENCHMARK_DATA_PREFIX}/${query.dataset}/${query.collection}`
}

/**
 * Extract $index_* columns from a filter for pushdown analysis
 */
function extractPushdownColumns(filter: Filter): string[] {
  const columns: string[] = []

  for (const [key, value] of Object.entries(filter)) {
    if (key === '$id' || key.startsWith('$index_')) {
      columns.push(key)
    }
    // Handle $and
    if (key === '$and' && Array.isArray(value)) {
      for (const cond of value) {
        columns.push(...extractPushdownColumns(cond as Filter))
      }
    }
  }

  return [...new Set(columns)]
}

/**
 * Benchmark a single query using native pushdown
 */
async function benchmarkQuery(
  executor: QueryExecutor,
  query: BenchmarkQuery,
  config: BenchmarkConfig
): Promise<QueryBenchmarkResult> {
  const latencies: number[] = []
  let queryResult: FindResult<unknown> | null = null
  let queryError: string | undefined

  // Namespace with benchmark-data prefix for R2 paths
  const ns = getBenchmarkNamespace(query)

  // Analyze filter for pushdown columns
  const pushdownColumns = extractPushdownColumns(query.filter)
  const usesPushdownColumns = pushdownColumns.length > 0

  // Clear cache before benchmarking to ensure we measure cold query performance
  executor.clearCache()

  // Warmup iterations
  for (let i = 0; i < config.warmupIterations; i++) {
    try {
      executor.clearCache() // Clear between iterations for consistent warmup
      await executor.find(ns, query.filter, { limit: 100 })
    } catch (error: unknown) {
      // Warmup errors are expected and non-critical
      logger.debug('Benchmark warmup iteration error', error)
    }
  }

  // Clear cache before measured iterations
  executor.clearCache()

  // Execute query (uses native pushdown on $index_* columns)
  for (let i = 0; i < config.iterations; i++) {
    try {
      executor.clearCache() // Clear between iterations for cold measurements
      const start = performance.now()
      queryResult = await executor.find(ns, query.filter, { limit: 100 })
      const elapsed = performance.now() - start
      latencies.push(elapsed)
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      queryError = `${errorMessage} (ns=${ns}, filter=${JSON.stringify(query.filter)})`
      logger.warn('Benchmark query error', queryError)
    }
  }

  // Calculate percentiles
  const percentiles = calculatePercentiles(latencies)

  // Extract stats
  const stats = queryResult?.stats ?? {
    rowsScanned: 0,
    rowsReturned: 0,
    rowGroupsSkipped: 0,
    rowGroupsScanned: 0,
  }

  // Calculate pushdown metrics
  const rowGroupsTotal = (stats.rowGroupsSkipped ?? 0) + (stats.rowGroupsScanned ?? 0)
  const rowGroupSkipRatio = rowGroupsTotal > 0 ? (stats.rowGroupsSkipped ?? 0) / rowGroupsTotal : 0
  const selectivity = stats.rowsScanned > 0 ? stats.rowsReturned / stats.rowsScanned : 0

  return {
    query,
    execution: {
      latencyMs: percentiles,
      rawLatencies: latencies,
      rowsScanned: stats.rowsScanned,
      rowsReturned: stats.rowsReturned,
      rowGroupsTotal: rowGroupsTotal > 0 ? rowGroupsTotal : undefined,
      rowGroupsRead: stats.rowGroupsScanned,
      rowGroupsSkipped: stats.rowGroupsSkipped,
      success: !queryError,
      error: queryError,
      iterations: latencies.length,
    },
    pushdown: {
      usesPushdownColumns,
      pushdownColumns,
      selectivity: Math.round(selectivity * 1000) / 1000,
      rowGroupSkipRatio: Math.round(rowGroupSkipRatio * 1000) / 1000,
    },
  }
}

/**
 * Calculate latency percentiles
 */
function calculatePercentiles(latencies: number[]): { p50: number; p95: number; avg: number } {
  if (latencies.length === 0) {
    return { p50: 0, p95: 0, avg: 0 }
  }

  const sorted = [...latencies].sort((a, b) => a - b)
  const p50Index = Math.floor(sorted.length * 0.5)
  const p95Index = Math.floor(sorted.length * 0.95)
  const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length

  return {
    p50: Math.round(sorted[p50Index] ?? 0),
    p95: Math.round(sorted[p95Index] ?? 0),
    avg: Math.round(avg),
  }
}

/**
 * Calculate per-dataset summaries
 */
function calculateDatasetSummaries(results: QueryBenchmarkResult[]): DatasetBenchmarkSummary[] {
  const datasets = [...new Set(results.map(r => r.query.dataset))]

  return datasets.map(dataset => {
    const datasetResults = results.filter(r => r.query.dataset === dataset)

    // Find best/worst latency
    const best = datasetResults.reduce((a, b) =>
      a.execution.latencyMs.p50 < b.execution.latencyMs.p50 ? a : b
    )
    const worst = datasetResults.reduce((a, b) =>
      a.execution.latencyMs.p50 > b.execution.latencyMs.p50 ? a : b
    )

    // Calculate averages
    const avgLatency = datasetResults.reduce((sum, r) => sum + r.execution.latencyMs.avg, 0) / datasetResults.length
    const avgSkipRatio = datasetResults.reduce((sum, r) => sum + r.pushdown.rowGroupSkipRatio, 0) / datasetResults.length
    const avgSelectivity = datasetResults.reduce((sum, r) => sum + r.pushdown.selectivity, 0) / datasetResults.length

    return {
      dataset,
      queryCount: datasetResults.length,
      avgLatencyMs: Math.round(avgLatency),
      bestLatency: { queryId: best.query.id, latencyMs: best.execution.latencyMs.p50 },
      worstLatency: { queryId: worst.query.id, latencyMs: worst.execution.latencyMs.p50 },
      pushdownUsedCount: datasetResults.filter(r => r.pushdown.usesPushdownColumns).length,
      avgRowGroupSkipRatio: Math.round(avgSkipRatio * 1000) / 1000,
      avgSelectivity: Math.round(avgSelectivity * 1000) / 1000,
    }
  })
}

/**
 * Calculate overall summary
 */
function calculateOverallSummary(results: QueryBenchmarkResult[]): PushdownBenchmarkResult['summary'] {
  // Handle empty results
  if (results.length === 0) {
    return {
      avgLatencyMs: 0,
      medianLatencyMs: 0,
      bestLatency: { queryId: 'none', latencyMs: 0 },
      pushdownUsageRate: 0,
      avgRowGroupSkipRatio: 0,
      byCategory: {
        equality: { count: 0, avgLatencyMs: 0, avgSkipRatio: 0 },
        range: { count: 0, avgLatencyMs: 0, avgSkipRatio: 0 },
        compound: { count: 0, avgLatencyMs: 0, avgSkipRatio: 0 },
        fts: { count: 0, avgLatencyMs: 0, avgSkipRatio: 0 },
      },
      bySelectivity: {
        high: { count: 0, avgLatencyMs: 0 },
        medium: { count: 0, avgLatencyMs: 0 },
        low: { count: 0, avgLatencyMs: 0 },
      },
    }
  }

  // Calculate latency stats
  const latencies = results.map(r => r.execution.latencyMs.p50).sort((a, b) => a - b)
  const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length
  const medianLatency = latencies[Math.floor(latencies.length / 2)] ?? 0

  const best = results.reduce((a, b) =>
    a.execution.latencyMs.p50 < b.execution.latencyMs.p50 ? a : b
  )

  // Calculate pushdown stats
  const pushdownUsageRate = Math.round(
    results.filter(r => r.pushdown.usesPushdownColumns).length / results.length * 100
  )
  const avgSkipRatio = results.reduce((sum, r) => sum + r.pushdown.rowGroupSkipRatio, 0) / results.length

  // By category
  const byCategory = {
    equality: calculateCategoryStats(results.filter(r => r.query.category === 'equality')),
    range: calculateCategoryStats(results.filter(r => r.query.category === 'range')),
    compound: calculateCategoryStats(results.filter(r => r.query.category === 'compound')),
    fts: calculateCategoryStats(results.filter(r => r.query.category === 'fts')),
  }

  // By selectivity
  const bySelectivity = {
    high: calculateSelectivityStats(results.filter(r => r.query.selectivity === 'high')),
    medium: calculateSelectivityStats(results.filter(r => r.query.selectivity === 'medium')),
    low: calculateSelectivityStats(results.filter(r => r.query.selectivity === 'low')),
  }

  return {
    avgLatencyMs: Math.round(avgLatency),
    medianLatencyMs: Math.round(medianLatency),
    bestLatency: { queryId: best.query.id, latencyMs: best.execution.latencyMs.p50 },
    pushdownUsageRate,
    avgRowGroupSkipRatio: Math.round(avgSkipRatio * 1000) / 1000,
    byCategory,
    bySelectivity,
  }
}

/**
 * Calculate stats for a category of results
 */
function calculateCategoryStats(results: QueryBenchmarkResult[]): { count: number; avgLatencyMs: number; avgSkipRatio: number } {
  if (results.length === 0) {
    return { count: 0, avgLatencyMs: 0, avgSkipRatio: 0 }
  }

  const avgLatency = results.reduce((sum, r) => sum + r.execution.latencyMs.avg, 0) / results.length
  const avgSkipRatio = results.reduce((sum, r) => sum + r.pushdown.rowGroupSkipRatio, 0) / results.length

  return {
    count: results.length,
    avgLatencyMs: Math.round(avgLatency),
    avgSkipRatio: Math.round(avgSkipRatio * 1000) / 1000,
  }
}

/**
 * Calculate stats for a selectivity group
 */
function calculateSelectivityStats(results: QueryBenchmarkResult[]): { count: number; avgLatencyMs: number } {
  if (results.length === 0) {
    return { count: 0, avgLatencyMs: 0 }
  }

  const avgLatency = results.reduce((sum, r) => sum + r.execution.latencyMs.avg, 0) / results.length

  return {
    count: results.length,
    avgLatencyMs: Math.round(avgLatency),
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

/**
 * Handle /benchmark-pushdown HTTP request
 *
 * Measures native Parquet predicate pushdown performance on $index_* columns.
 */
export async function handlePushdownBenchmarkRequest(
  request: Request,
  bucket: R2Bucket
): Promise<Response> {
  const url = new URL(request.url)
  const startTime = performance.now()

  // Parse config from query params
  const iterations = parseInt(url.searchParams.get('iterations') || '3')
  const warmup = parseInt(url.searchParams.get('warmup') || '1')
  const maxQueries = url.searchParams.get('maxQueries')
    ? parseInt(url.searchParams.get('maxQueries')!)
    : undefined

  const datasetsParam = url.searchParams.get('datasets')
  const datasets = datasetsParam
    ? datasetsParam.split(',') as Array<'imdb' | 'imdb-1m' | 'onet-full' | 'unspsc-full'>
    : undefined

  const categoriesParam = url.searchParams.get('categories')
  const categories = categoriesParam
    ? categoriesParam.split(',') as BenchmarkQuery['category'][]
    : undefined

  try {
    // Open cache
    const cache = await caches.open('parquedb-benchmark')

    const result = await runPushdownBenchmark(bucket, cache, {
      iterations,
      warmupIterations: warmup,
      maxQueriesPerDataset: maxQueries,
      datasets,
      categories,
    })

    const totalTime = Math.round(performance.now() - startTime)

    return Response.json({
      benchmark: 'Native Parquet Pushdown Performance',
      description: 'Measures query performance using native Parquet predicate pushdown on $index_* columns',
      totalTimeMs: totalTime,
      queryStats: QUERY_STATS,
      ...result,
      interpretation: {
        avgLatency: `Average query latency: ${result.summary.avgLatencyMs}ms`,
        pushdownUsage: `${result.summary.pushdownUsageRate}% of queries use pushdown columns`,
        rowGroupSkipping: `Average ${Math.round(result.summary.avgRowGroupSkipRatio * 100)}% of row groups skipped via statistics`,
        bestCase: `Best: ${result.summary.bestLatency.queryId} (${result.summary.bestLatency.latencyMs}ms)`,
        recommendation: result.summary.avgRowGroupSkipRatio > 0.5
          ? 'Native pushdown is effectively skipping row groups'
          : result.summary.avgRowGroupSkipRatio > 0.2
          ? 'Moderate row group skipping - consider data organization'
          : 'Low row group skipping - data may benefit from sorting by filter columns',
      },
    }, {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Server-Timing': `total;dur=${totalTime}`,
      },
    })
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    const stack = error instanceof Error ? error.stack : undefined
    return Response.json({
      error: true,
      message,
      stack,
    }, { status: 500 })
  }
}

// Legacy alias for backwards compatibility
export const handleIndexedBenchmarkRequest = handlePushdownBenchmarkRequest

/**
 * Indexed Benchmark Runner for ParqueDB
 *
 * Comprehensive benchmarks comparing indexed vs scan query performance
 * across IMDB, O*NET, and UNSPSC datasets.
 */

import type { Filter } from '../types/filter'
import type { BenchmarkQuery } from './benchmark-queries'
import { ALL_QUERIES, getQueriesForDataset, QUERY_STATS } from './benchmark-queries'
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
 */
export interface QueryBenchmarkResult {
  /** Query definition */
  query: BenchmarkQuery
  /** Indexed execution metrics */
  indexed: {
    /** Latency percentiles */
    latencyMs: { p50: number; p95: number; avg: number }
    /** Raw latency values for debugging */
    rawLatencies?: number[]
    /** Index lookup time */
    indexLookupMs?: number
    /** Rows scanned */
    rowsScanned: number
    /** Rows returned */
    rowsReturned: number
    /** Index type actually used */
    indexUsed?: string
    /** Success flag */
    success: boolean
    /** Error message if failed */
    error?: string
    /** Number of iterations that completed */
    iterations?: number
  }
  /** Scan execution metrics (baseline) */
  scan: {
    /** Latency percentiles */
    latencyMs: { p50: number; p95: number; avg: number }
    /** Rows scanned */
    rowsScanned: number
    /** Rows returned */
    rowsReturned: number
    /** Success flag */
    success: boolean
    /** Error message if failed */
    error?: string
  }
  /** Performance comparison */
  speedup: number
  /** Whether index provided speedup */
  indexBeneficial: boolean
}

/**
 * Dataset benchmark summary
 */
export interface DatasetBenchmarkSummary {
  /** Dataset ID */
  dataset: string
  /** Number of queries run */
  queryCount: number
  /** Average speedup across queries */
  avgSpeedup: number
  /** Best speedup achieved */
  bestSpeedup: { queryId: string; speedup: number }
  /** Worst speedup (or slowdown) */
  worstSpeedup: { queryId: string; speedup: number }
  /** Queries where index helped */
  indexBeneficialCount: number
  /** Average indexed latency */
  avgIndexedLatencyMs: number
  /** Average scan latency */
  avgScanLatencyMs: number
}

/**
 * Full benchmark result
 */
export interface IndexedBenchmarkResult {
  /** Benchmark metadata */
  metadata: {
    timestamp: string
    iterations: number
    datasets: string[]
    totalQueries: number
    durationMs: number
  }
  /** Per-query results */
  queries: QueryBenchmarkResult[]
  /** Per-dataset summaries */
  datasetSummaries: DatasetBenchmarkSummary[]
  /** Overall summary */
  summary: {
    avgSpeedup: number
    medianSpeedup: number
    bestOverall: { queryId: string; speedup: number }
    indexBeneficialRate: number
    byIndexType: {
      hash: { count: number; avgSpeedup: number }
      sst: { count: number; avgSpeedup: number }
      fts: { count: number; avgSpeedup: number }
    }
    byCategory: {
      equality: { count: number; avgSpeedup: number }
      range: { count: number; avgSpeedup: number }
      compound: { count: number; avgSpeedup: number }
      fts: { count: number; avgSpeedup: number }
    }
  }
}

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
  /** Include scan baselines */
  includeScanBaselines: boolean
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Run indexed benchmarks
 *
 * @param bucket - R2 bucket containing datasets and indexes
 * @param cache - Cache API for metadata caching
 * @param config - Benchmark configuration
 * @returns Full benchmark results
 */
export async function runIndexedBenchmark(
  bucket: R2Bucket,
  cache: Cache,
  config: BenchmarkConfig = {
    iterations: 5,
    warmupIterations: 1,
    includeScanBaselines: true,
  }
): Promise<IndexedBenchmarkResult> {
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

  // Filter by category
  if (config.categories) {
    queries = queries.filter(q => config.categories!.includes(q.category))
  }

  // Skip scan baselines if not requested
  if (!config.includeScanBaselines) {
    queries = queries.filter(q => q.category !== 'scan')
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
    },
    queries: results,
    datasetSummaries,
    summary,
  }
}

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
 * Benchmark a single query
 */
async function benchmarkQuery(
  executor: QueryExecutor,
  query: BenchmarkQuery,
  config: BenchmarkConfig
): Promise<QueryBenchmarkResult> {
  const indexedLatencies: number[] = []
  const scanLatencies: number[] = []
  let indexedResult: FindResult<unknown> | null = null
  let scanResult: FindResult<unknown> | null = null
  let indexedError: string | undefined
  let scanError: string | undefined

  // Namespace with benchmark-data prefix for R2 paths
  const ns = getBenchmarkNamespace(query)

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

  // Indexed execution (uses secondary index if available)
  for (let i = 0; i < config.iterations; i++) {
    try {
      executor.clearCache() // Clear between iterations for cold measurements
      const start = performance.now()
      indexedResult = await executor.find(ns, query.filter, { limit: 100 })
      const elapsed = performance.now() - start
      indexedLatencies.push(elapsed)
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      indexedError = `${errorMessage} (ns=${ns}, filter=${JSON.stringify(query.filter)})`
      logger.warn('Benchmark query error', indexedError)
    }
  }

  // Scan baseline (force scan by using non-indexed field names)
  // Only run for non-scan queries (scan queries are already baselines)
  if (query.category !== 'scan' && query.scanFilter) {
    for (let i = 0; i < config.iterations; i++) {
      try {
        const start = performance.now()
        scanResult = await executor.find(ns, query.scanFilter, { limit: 100 })
        scanLatencies.push(performance.now() - start)
      } catch (error: unknown) {
        scanError = error instanceof Error ? error.message : String(error)
      }
    }
  } else if (query.category === 'scan') {
    // For scan baseline queries, use the same filter
    scanLatencies.push(...indexedLatencies)
    scanResult = indexedResult
  }

  // Calculate percentiles
  const indexedPercentiles = calculatePercentiles(indexedLatencies)
  const scanPercentiles = calculatePercentiles(scanLatencies.length > 0 ? scanLatencies : indexedLatencies)

  // Calculate speedup
  const speedup = scanPercentiles.p50 > 0 ? scanPercentiles.p50 / indexedPercentiles.p50 : 1

  // Extract index metadata from stats
  const indexStats = indexedResult?.stats as QueryStats & { indexUsed?: string; indexLookupMs?: number }

  return {
    query,
    indexed: {
      latencyMs: indexedPercentiles,
      rawLatencies: indexedLatencies,
      indexLookupMs: indexStats?.indexLookupMs,
      rowsScanned: indexedResult?.stats?.rowsScanned ?? 0,
      rowsReturned: indexedResult?.stats?.rowsReturned ?? 0,
      indexUsed: indexStats?.indexUsed,
      success: !indexedError,
      error: indexedError,
      iterations: indexedLatencies.length,
    },
    scan: {
      latencyMs: scanPercentiles,
      rowsScanned: scanResult?.stats?.rowsScanned ?? 0,
      rowsReturned: scanResult?.stats?.rowsReturned ?? 0,
      success: !scanError,
      error: scanError,
    },
    speedup: Math.round(speedup * 10) / 10,
    indexBeneficial: speedup > 1.1, // Consider beneficial if >10% faster
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
    const speedups = datasetResults.map(r => r.speedup)
    const avgSpeedup = speedups.reduce((a, b) => a + b, 0) / speedups.length

    const best = datasetResults.reduce((a, b) => a.speedup > b.speedup ? a : b)
    const worst = datasetResults.reduce((a, b) => a.speedup < b.speedup ? a : b)

    const avgIndexedLatency = datasetResults.reduce((sum, r) => sum + r.indexed.latencyMs.avg, 0) / datasetResults.length
    const avgScanLatency = datasetResults.reduce((sum, r) => sum + r.scan.latencyMs.avg, 0) / datasetResults.length

    return {
      dataset,
      queryCount: datasetResults.length,
      avgSpeedup: Math.round(avgSpeedup * 10) / 10,
      bestSpeedup: { queryId: best.query.id, speedup: best.speedup },
      worstSpeedup: { queryId: worst.query.id, speedup: worst.speedup },
      indexBeneficialCount: datasetResults.filter(r => r.indexBeneficial).length,
      avgIndexedLatencyMs: Math.round(avgIndexedLatency),
      avgScanLatencyMs: Math.round(avgScanLatency),
    }
  })
}

/**
 * Calculate overall summary
 */
function calculateOverallSummary(results: QueryBenchmarkResult[]): IndexedBenchmarkResult['summary'] {
  const speedups = results.map(r => r.speedup).sort((a, b) => a - b)
  const avgSpeedup = speedups.reduce((a, b) => a + b, 0) / speedups.length
  const medianSpeedup = speedups[Math.floor(speedups.length / 2)] ?? 0

  const best = results.reduce((a, b) => a.speedup > b.speedup ? a : b)

  // By index type
  const byIndexType = {
    hash: calculateGroupStats(results.filter(r => r.query.expectedIndex === 'hash')),
    sst: calculateGroupStats(results.filter(r => r.query.expectedIndex === 'sst')),
    fts: calculateGroupStats(results.filter(r => r.query.expectedIndex === 'fts')),
  }

  // By category
  const byCategory = {
    equality: calculateGroupStats(results.filter(r => r.query.category === 'equality')),
    range: calculateGroupStats(results.filter(r => r.query.category === 'range')),
    compound: calculateGroupStats(results.filter(r => r.query.category === 'compound')),
    fts: calculateGroupStats(results.filter(r => r.query.category === 'fts')),
  }

  return {
    avgSpeedup: Math.round(avgSpeedup * 10) / 10,
    medianSpeedup: Math.round(medianSpeedup * 10) / 10,
    bestOverall: { queryId: best.query.id, speedup: best.speedup },
    indexBeneficialRate: Math.round(results.filter(r => r.indexBeneficial).length / results.length * 100),
    byIndexType,
    byCategory,
  }
}

/**
 * Calculate stats for a group of results
 */
function calculateGroupStats(results: QueryBenchmarkResult[]): { count: number; avgSpeedup: number } {
  if (results.length === 0) {
    return { count: 0, avgSpeedup: 0 }
  }

  const avgSpeedup = results.reduce((sum, r) => sum + r.speedup, 0) / results.length
  return {
    count: results.length,
    avgSpeedup: Math.round(avgSpeedup * 10) / 10,
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

/**
 * Handle /benchmark-indexed HTTP request
 */
export async function handleIndexedBenchmarkRequest(
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

  const includeScans = url.searchParams.get('includeScans') !== 'false'

  try {
    // Open cache
    const cache = await caches.open('parquedb-benchmark')

    const result = await runIndexedBenchmark(bucket, cache, {
      iterations,
      warmupIterations: warmup,
      maxQueriesPerDataset: maxQueries,
      datasets,
      categories,
      includeScanBaselines: includeScans,
    })

    const totalTime = Math.round(performance.now() - startTime)

    return Response.json({
      benchmark: 'Secondary Index Performance',
      description: 'Real-world R2 benchmarks comparing indexed vs scan queries',
      totalTimeMs: totalTime,
      queryStats: QUERY_STATS,
      ...result,
      interpretation: {
        avgSpeedup: `Indexed queries are ${result.summary.avgSpeedup}x faster on average`,
        indexBeneficial: `${result.summary.indexBeneficialRate}% of queries benefit from indexes`,
        bestCase: `Best: ${result.summary.bestOverall.queryId} (${result.summary.bestOverall.speedup}x speedup)`,
        recommendation: result.summary.avgSpeedup > 5
          ? 'Secondary indexes provide significant performance benefits'
          : result.summary.avgSpeedup > 2
          ? 'Secondary indexes provide moderate performance benefits'
          : 'Consider reviewing index coverage and query patterns',
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

/**
 * Type definitions for E2E Benchmark Suite
 *
 * Types for benchmarking ParqueDB on deployed Cloudflare Workers.
 */

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * E2E Benchmark configuration
 */
export interface E2EBenchmarkConfig {
  /** Worker URL to benchmark */
  url: string
  /** Datasets to test */
  datasets: string[]
  /** Number of iterations per test */
  iterations: number
  /** Warmup iterations (not counted) */
  warmup: number
  /** Max queries per dataset */
  maxQueries: number
  /** Concurrent request count */
  concurrency: number
  /** Output format */
  output: 'table' | 'json' | 'markdown'
  /** Verbose logging */
  verbose: boolean
  /** Include cold start tests */
  includeColdStart: boolean
  /** Include cache tests */
  includeCacheTests: boolean
  /** Timeout per request in ms */
  timeout: number
}

/**
 * Default configuration
 */
export const DEFAULT_E2E_CONFIG: E2EBenchmarkConfig = {
  url: 'https://api.parquedb.com',
  datasets: ['imdb', 'onet-full', 'unspsc-full'],
  iterations: 5,
  warmup: 2,
  maxQueries: 10,
  concurrency: 1,
  output: 'table',
  verbose: false,
  includeColdStart: true,
  includeCacheTests: true,
  timeout: 30000,
}

// =============================================================================
// Measurement Types
// =============================================================================

/**
 * Latency percentiles
 */
export interface LatencyStats {
  min: number
  max: number
  mean: number
  median: number
  p50: number
  p75: number
  p90: number
  p95: number
  p99: number
  stdDev: number
}

/**
 * Single request result
 */
export interface RequestResult {
  success: boolean
  status?: number
  latencyMs: number
  error?: string
  data?: unknown
  headers?: Record<string, string>
  serverTiming?: string
  cacheStatus?: 'HIT' | 'MISS' | 'STALE' | 'BYPASS' | 'DYNAMIC' | 'UNKNOWN'
  cfColo?: string
}

/**
 * Benchmark test result
 */
export interface BenchmarkTestResult {
  /** Test name/identifier */
  name: string
  /** Test description */
  description: string
  /** Category of the test */
  category: BenchmarkCategory
  /** Number of successful iterations */
  successCount: number
  /** Number of failed iterations */
  failureCount: number
  /** Total iterations attempted */
  totalIterations: number
  /** Latency statistics */
  latency: LatencyStats
  /** Throughput (ops/sec) */
  throughput: number
  /** Raw latencies for analysis */
  rawLatencies: number[]
  /** Cache hit rate (if applicable) */
  cacheHitRate?: number
  /** Additional metadata */
  metadata?: Record<string, unknown>
  /** Timestamp */
  timestamp: string
}

/**
 * Benchmark categories
 */
export type BenchmarkCategory =
  | 'cold-start'
  | 'warm-request'
  | 'cache-hit'
  | 'cache-miss'
  | 'query-equality'
  | 'query-range'
  | 'query-compound'
  | 'query-fts'
  | 'concurrency'
  | 'r2-io'
  | 'index-lookup'
  | 'full-scan'

// =============================================================================
// Cold Start Types
// =============================================================================

/**
 * Cold start measurement result
 */
export interface ColdStartResult {
  /** First request latency (cold) */
  coldLatencyMs: number
  /** Subsequent request latency (warm) */
  warmLatencyMs: number
  /** Cold start overhead */
  overheadMs: number
  /** Overhead percentage */
  overheadPercent: number
  /** CF colo for the request */
  colo?: string
  /** Number of warm measurements */
  warmSamples: number
}

// =============================================================================
// Cache Performance Types
// =============================================================================

/**
 * Cache performance result
 */
export interface CachePerformanceResult {
  /** Cache miss latency (first request) */
  missLatency: LatencyStats
  /** Cache hit latency (subsequent requests) */
  hitLatency: LatencyStats
  /** Speedup from caching */
  speedup: number
  /** Hit rate after warmup */
  hitRate: number
  /** Total requests made */
  totalRequests: number
}

// =============================================================================
// Dataset Benchmark Types
// =============================================================================

/**
 * Dataset benchmark result
 */
export interface DatasetBenchmarkResult {
  /** Dataset name */
  dataset: string
  /** Queries executed */
  queries: QueryBenchmarkResult[]
  /** Total execution time */
  totalTimeMs: number
  /** Summary statistics */
  summary: {
    queryCount: number
    avgLatencyMs: number
    medianLatencyMs: number
    bestQuery: { name: string; latencyMs: number }
    worstQuery: { name: string; latencyMs: number }
    successRate: number
  }
}

/**
 * Single query benchmark result
 */
export interface QueryBenchmarkResult {
  /** Query identifier */
  id: string
  /** Query description */
  description: string
  /** Query category */
  category: BenchmarkCategory
  /** Latency statistics */
  latency: LatencyStats
  /** Rows returned */
  rowsReturned: number
  /** Success flag */
  success: boolean
  /** Error if failed */
  error?: string
}

// =============================================================================
// Index Performance Types
// =============================================================================

/**
 * Index vs scan comparison result
 */
export interface IndexComparisonResult {
  /** Query identifier */
  queryId: string
  /** Indexed query performance */
  indexed: {
    latency: LatencyStats
    success: boolean
    error?: string
    rowGroupsSkipped?: number
    rowGroupsScanned?: number
  }
  /** Full scan performance */
  scan: {
    latency: LatencyStats
    success: boolean
    error?: string
  }
  /** Speedup factor */
  speedup: number
  /** Bytes saved */
  bytesSaved?: number
}

// =============================================================================
// Concurrency Types
// =============================================================================

/**
 * Concurrency test result
 */
export interface ConcurrencyResult {
  /** Concurrency level */
  concurrency: number
  /** Successful requests */
  successCount: number
  /** Failed requests */
  failureCount: number
  /** Total execution time */
  totalTimeMs: number
  /** Throughput (requests/second) */
  throughput: number
  /** Per-request latency */
  latency: LatencyStats
  /** Error rate */
  errorRate: number
}

// =============================================================================
// Full Benchmark Suite Results
// =============================================================================

/**
 * Complete benchmark suite result
 */
export interface E2EBenchmarkSuiteResult {
  /** Configuration used */
  config: E2EBenchmarkConfig
  /** Benchmark metadata */
  metadata: {
    startedAt: string
    completedAt: string
    durationMs: number
    workerUrl: string
    cfColo?: string
    runnerVersion: string
  }
  /** Health check result */
  health: {
    success: boolean
    latencyMs: number
    status?: string
    error?: string
  }
  /** Cold start measurements */
  coldStart?: ColdStartResult
  /** Cache performance */
  cachePerformance?: CachePerformanceResult
  /** Dataset benchmarks */
  datasets: DatasetBenchmarkResult[]
  /** Index comparisons */
  indexComparisons?: IndexComparisonResult[]
  /** Concurrency tests */
  concurrency?: ConcurrencyResult[]
  /** R2 I/O benchmarks */
  r2io?: BenchmarkTestResult
  /** Overall summary */
  summary: {
    totalTests: number
    passedTests: number
    failedTests: number
    avgLatencyMs: number
    p95LatencyMs: number
    overallThroughput: number
    cacheSpeedup?: number
    indexSpeedup?: number
  }
  /** Regression analysis (if baseline provided) */
  regression?: RegressionAnalysis
}

// =============================================================================
// Regression Detection Types
// =============================================================================

/**
 * Regression analysis result
 */
export interface RegressionAnalysis {
  /** Baseline version */
  baselineVersion: string
  /** Current version */
  currentVersion: string
  /** Individual metric comparisons */
  metrics: RegressionMetric[]
  /** Overall regression detected */
  hasRegression: boolean
  /** Severity level */
  severity: 'none' | 'minor' | 'moderate' | 'severe'
  /** Summary message */
  message: string
}

/**
 * Single regression metric
 */
export interface RegressionMetric {
  /** Metric name */
  name: string
  /** Baseline value */
  baseline: number
  /** Current value */
  current: number
  /** Change (current - baseline) */
  change: number
  /** Change percentage */
  changePercent: number
  /** Is regression (performance degraded) */
  isRegression: boolean
  /** Threshold used */
  threshold: number
}

/**
 * Regression thresholds (percentage increase that triggers regression)
 */
export interface RegressionThresholds {
  latencyP50: number
  latencyP95: number
  latencyP99: number
  coldStartOverhead: number
  cacheHitRate: number
  throughput: number
}

/**
 * Default regression thresholds
 */
export const DEFAULT_REGRESSION_THRESHOLDS: RegressionThresholds = {
  latencyP50: 20, // 20% increase in P50 latency
  latencyP95: 25, // 25% increase in P95 latency
  latencyP99: 30, // 30% increase in P99 latency
  coldStartOverhead: 50, // 50% increase in cold start overhead
  cacheHitRate: -10, // 10% decrease in cache hit rate
  throughput: -15, // 15% decrease in throughput
}

// =============================================================================
// Storage Types
// =============================================================================

/**
 * Stored benchmark result for historical comparison
 */
export interface StoredBenchmarkResult {
  /** Unique run ID */
  runId: string
  /** Git commit SHA */
  commitSha?: string
  /** Git branch */
  branch?: string
  /** Git tag (if any) */
  tag?: string
  /** Environment (prod, staging, etc) */
  environment: string
  /** Full benchmark results */
  results: E2EBenchmarkSuiteResult
  /** Timestamp */
  timestamp: string
}

// =============================================================================
// Output Types
// =============================================================================

/**
 * Console output format
 */
export interface ConsoleOutput {
  table: () => void
  json: () => string
  markdown: () => string
}

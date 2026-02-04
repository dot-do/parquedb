/**
 * E2E Benchmark Types
 *
 * Shared types for E2E benchmark results, regression analysis, and stored results.
 * These types are used by both the benchmark runner and the observability module.
 */

// =============================================================================
// Benchmark Result Types
// =============================================================================

/** Latency statistics */
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

/** Cold start measurement result */
export interface ColdStartResult {
  coldLatencyMs: number
  warmLatencyMs: number
  overheadMs: number
  overheadPercent: number
  colo?: string | undefined
  warmSamples: number
}

/** Cache performance result */
export interface CachePerformanceResult {
  missLatency: LatencyStats
  hitLatency: LatencyStats
  speedup: number
  hitRate: number
  totalRequests: number
}

/** Concurrency test result */
export interface ConcurrencyResult {
  concurrency: number
  successCount: number
  failureCount: number
  totalTimeMs: number
  throughput: number
  latency: LatencyStats
  errorRate: number
}

/** Benchmark summary */
export interface BenchmarkSummary {
  totalTests: number
  passedTests: number
  failedTests: number
  avgLatencyMs: number
  p95LatencyMs: number
  overallThroughput: number
  cacheSpeedup?: number | undefined
  indexSpeedup?: number | undefined
}

/** E2E Benchmark suite result */
export interface E2EBenchmarkSuiteResult {
  config: Record<string, unknown>
  metadata: {
    startedAt: string
    completedAt: string
    durationMs: number
    workerUrl: string
    cfColo?: string | undefined
    runnerVersion: string
  }
  health: {
    success: boolean
    latencyMs: number
    status?: string | undefined
    error?: string | undefined
  }
  coldStart?: ColdStartResult | undefined
  cachePerformance?: CachePerformanceResult | undefined
  datasets: unknown[]
  concurrency?: ConcurrencyResult[] | undefined
  summary: BenchmarkSummary
  regression?: RegressionAnalysis | undefined
}

// =============================================================================
// Regression Analysis Types
// =============================================================================

/** Regression metric comparison */
export interface RegressionMetric {
  name: string
  baseline: number
  current: number
  change: number
  changePercent: number
  isRegression: boolean
  threshold: number
}

/** Regression analysis result */
export interface RegressionAnalysis {
  baselineVersion: string
  currentVersion: string
  metrics: RegressionMetric[]
  hasRegression: boolean
  severity: 'none' | 'minor' | 'moderate' | 'severe'
  message: string
}

/** Regression thresholds */
export interface RegressionThresholds {
  latencyP50: number
  latencyP95: number
  latencyP99: number
  coldStartOverhead: number
  cacheHitRate: number
  throughput: number
}

/** Default regression thresholds */
export const DEFAULT_REGRESSION_THRESHOLDS: RegressionThresholds = {
  latencyP50: 20,
  latencyP95: 25,
  latencyP99: 30,
  coldStartOverhead: 50,
  cacheHitRate: -10,
  throughput: -15,
}

// =============================================================================
// Storage Types
// =============================================================================

/** Stored benchmark result */
export interface StoredBenchmarkResult {
  runId: string
  commitSha?: string | undefined
  branch?: string | undefined
  tag?: string | undefined
  environment: string
  results: E2EBenchmarkSuiteResult
  timestamp: string
}

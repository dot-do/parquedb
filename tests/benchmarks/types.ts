/**
 * Type definitions for Backend Benchmark Suite
 *
 * Shared types for benchmarking across all storage backends.
 */

// =============================================================================
// Configuration Types
// =============================================================================

export type BackendType = 'memory' | 'local' | 'r2' | 'iceberg' | 'delta'

export interface BackendBenchmarkConfig {
  /** Backends to benchmark */
  backends: BackendType[]
  /** Dataset sizes to test */
  sizes: number[]
  /** Number of iterations per test */
  iterations: number
  /** Warmup iterations (not counted) */
  warmup: number
  /** Row group size for Parquet files */
  rowGroupSize: number
  /** Output format */
  output: 'table' | 'json' | 'markdown'
  /** Verbose logging */
  verbose: boolean
  /** R2 bucket (for R2 tests) */
  r2Bucket?: string | undefined
  /** R2 endpoint (for R2 tests) */
  r2Endpoint?: string | undefined
}

export const DEFAULT_BACKEND_CONFIG: BackendBenchmarkConfig = {
  backends: ['memory', 'iceberg', 'delta'],
  sizes: [1000, 10000],
  iterations: 10,
  warmup: 2,
  rowGroupSize: 1000,
  output: 'table',
  verbose: false,
}

// =============================================================================
// Statistics Types
// =============================================================================

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

export interface BenchmarkStats extends LatencyStats {
  name: string
  backend: BackendType
  datasetSize: number
  iterations: number
  samples: number[]
  opsPerSecond: number
}

// =============================================================================
// Operation Types
// =============================================================================

export type OperationType =
  | 'bulk-write'
  | 'point-read'
  | 'filtered-query'
  | 'range-query'
  | 'compound-query'
  | 'full-scan'
  | 'update'
  | 'delete'
  | 'time-travel-query'
  | 'snapshot-create'
  | 'compaction'

export interface OperationResult {
  operation: OperationType
  backend: BackendType
  datasetSize: number
  stats: BenchmarkStats
  bytesRead?: number | undefined
  bytesWritten?: number | undefined
  rowsAffected?: number | undefined
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// Suite Results
// =============================================================================

export interface BackendBenchmarkSuiteResult {
  config: BackendBenchmarkConfig
  startedAt: string
  completedAt: string
  durationMs: number
  results: OperationResult[]
  summary: BackendSummary
}

export interface BackendSummary {
  backendComparison: Record<BackendType, BackendPerformance>
  bestBackend: {
    write: BackendType
    read: BackendType
    query: BackendType
    overall: BackendType
  }
  recommendations: string[]
}

export interface BackendPerformance {
  avgWriteMs: number
  avgReadMs: number
  avgQueryMs: number
  avgUpdateMs: number
  p95WriteMs: number
  p95ReadMs: number
  p95QueryMs: number
  throughputOpsPerSec: number
}

// =============================================================================
// Comparison Types
// =============================================================================

export interface BackendComparison {
  baseline: BackendType
  comparison: BackendType
  metrics: ComparisonMetric[]
  overallSpeedup: number
  recommendation: string
}

export interface ComparisonMetric {
  operation: OperationType
  baselineP50: number
  comparisonP50: number
  speedup: number
  percentChange: number
}

// =============================================================================
// Time Travel Benchmark Types
// =============================================================================

export interface TimeTravelBenchmarkResult {
  backend: BackendType
  snapshotCount: number
  snapshotCreateLatency: LatencyStats
  snapshotQueryLatency: LatencyStats
  historyQueryLatency: LatencyStats
  storageOverhead: {
    totalBytes: number
    snapshotBytes: number
    overheadPercent: number
  }
}

// =============================================================================
// Format-Specific Types
// =============================================================================

export interface IcebergBenchmarkMetrics {
  manifestReads: number
  manifestListReads: number
  dataFileReads: number
  rowGroupsScanned: number
  rowGroupsSkipped: number
  predicatePushdownEfficiency: number
}

export interface DeltaBenchmarkMetrics {
  commitLogReads: number
  checkpointReads: number
  dataFileReads: number
  filesScanned: number
  filesSkipped: number
}

// =============================================================================
// R2 Specific Types
// =============================================================================

export interface R2BenchmarkConfig extends BackendBenchmarkConfig {
  r2Bucket: string
  r2Endpoint: string
  r2AccessKeyId?: string | undefined
  r2SecretAccessKey?: string | undefined
}

export interface R2BenchmarkMetrics {
  bytesRead: number
  bytesWritten: number
  requestCount: number
  cacheHits: number
  cacheMisses: number
  avgRequestLatencyMs: number
}

// =============================================================================
// Utility Functions
// =============================================================================

export function calculateLatencyStats(samples: number[]): LatencyStats {
  if (samples.length === 0) {
    return {
      min: 0, max: 0, mean: 0, median: 0,
      p50: 0, p75: 0, p90: 0, p95: 0, p99: 0, stdDev: 0,
    }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = samples.reduce((a, b) => a + b, 0)
  const mean = sum / n

  const variance = samples.reduce((acc, s) => acc + Math.pow(s - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))]!
  }

  return {
    min: sorted[0]!,
    max: sorted[n - 1]!,
    mean,
    median: percentile(50),
    p50: percentile(50),
    p75: percentile(75),
    p90: percentile(90),
    p95: percentile(95),
    p99: percentile(99),
    stdDev,
  }
}

export function calculateBenchmarkStats(
  name: string,
  backend: BackendType,
  datasetSize: number,
  samples: number[]
): BenchmarkStats {
  const latencyStats = calculateLatencyStats(samples)

  return {
    ...latencyStats,
    name,
    backend,
    datasetSize,
    iterations: samples.length,
    samples,
    opsPerSecond: latencyStats.mean > 0 ? 1000 / latencyStats.mean : 0,
  }
}

/**
 * E2E Benchmark Types
 *
 * Type definitions for benchmark configuration, results, and reporting.
 */

// =============================================================================
// Configuration Types
// =============================================================================

export interface BenchmarkConfig {
  /** Base URL of the deployed worker */
  baseUrl: string
  /** Number of iterations for cold start tests */
  coldStartIterations: number
  /** Delay between cold start measurements (ms) to allow isolate teardown */
  coldStartDelay: number
  /** Number of iterations for warm request tests */
  warmIterations: number
  /** Number of warmup requests before measurement */
  warmupRequests: number
  /** Enable verbose logging */
  verbose: boolean
  /** Output file path for JSON results */
  outputPath?: string
}

export const defaultConfig: BenchmarkConfig = {
  baseUrl: 'https://parque.do',
  coldStartIterations: 5,
  coldStartDelay: 65000, // 65 seconds to ensure isolate teardown
  warmIterations: 20,
  warmupRequests: 3,
  verbose: false,
  outputPath: undefined,
}

// =============================================================================
// Latency Statistics
// =============================================================================

export interface LatencyStats {
  p50: number
  p95: number
  p99: number
  avg: number
  min: number
  max: number
  stdDev: number
}

// =============================================================================
// Cold Start Results
// =============================================================================

export interface ColdStartMeasurement {
  totalMs: number
  serverMs: number
  metadata: {
    colo: string
    timestamp: string
    isolateId?: string
  }
}

export interface ColdStartResults {
  measurements: ColdStartMeasurement[]
  stats: LatencyStats
  serverStats: LatencyStats
}

// =============================================================================
// Warm Request Results
// =============================================================================

export interface WarmRequestMeasurement {
  totalMs: number
  serverMs: number
  cacheHit: boolean
  statusCode: number
}

export interface WarmRequestResults {
  endpoint: string
  measurements: WarmRequestMeasurement[]
  stats: LatencyStats
  serverStats: LatencyStats
  cacheHitRate: number
}

// =============================================================================
// Cache Performance Results
// =============================================================================

export interface CachePerformanceResults {
  missLatency: LatencyStats
  hitLatency: LatencyStats
  improvementPercent: number
}

// =============================================================================
// Full Benchmark Report
// =============================================================================

export interface BenchmarkReport {
  meta: {
    startTime: string
    endTime: string
    durationMs: number
    baseUrl: string
    config: Omit<BenchmarkConfig, 'baseUrl'>
  }
  coldStart: ColdStartResults
  warmRequests: {
    health: WarmRequestResults
    crud: {
      create: WarmRequestResults
      read: WarmRequestResults
    }
  }
  cachePerformance?: CachePerformanceResults
  errors: BenchmarkError[]
}

export interface BenchmarkError {
  phase: string
  endpoint: string
  error: string
  timestamp: string
}

// =============================================================================
// Server Response Types (from worker endpoints)
// =============================================================================

export interface ServerHealthResponse {
  status: 'ok' | 'degraded' | 'error'
  timestamp: string
  checks: {
    r2: { status: 'ok' | 'error'; latencyMs: number }
    cache: { status: 'ok' | 'error'; latencyMs: number }
  }
  metadata: {
    colo: string
    region?: string
  }
}

export interface ServerColdStartResponse {
  coldStartMs: number
  workerInitMs: number
  cacheInitMs: number
  firstQueryMs: number
  metadata: {
    colo: string
    timestamp: string
    isolateId?: string
  }
}

export interface ServerCrudResponse {
  operation: string
  iterations: number
  batchSize?: number
  latencyMs: LatencyStats
  throughput: {
    opsPerSec: number
    totalTimeMs: number
  }
}

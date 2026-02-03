/**
 * E2E Benchmark Suite
 *
 * Public API for running E2E benchmarks programmatically.
 */

export { runBenchmarks, parseArgs } from './runner'
export { calculateStats, formatStats, formatLatency } from './stats'
export type {
  BenchmarkConfig,
  BenchmarkReport,
  BenchmarkError,
  LatencyStats,
  ColdStartResults,
  ColdStartMeasurement,
  WarmRequestResults,
  WarmRequestMeasurement,
  CachePerformanceResults,
} from './types'
export { defaultConfig } from './types'

/**
 * Benchmark Observability Module
 *
 * Provides infrastructure for storing, querying, and analyzing benchmark results over time.
 *
 * @module observability/benchmarks
 */

export {
  // Types
  type TrendGranularity,
  type TrendDirection,
  type TrendStoreConfig,
  type TrendQueryOptions,
  type TrendDataPoint,
  type TrendSeries,
  type TrendSummary,
  type SaveResult,

  // Interface
  type TrendStore,

  // Classes
  R2TrendStore,
  FileTrendStore,
  HttpTrendStore,
  MemoryTrendStore,

  // Factory functions
  createTrendStore,
  createStoredResult,
  generateRunId,

  // Analysis helpers
  calculateTrendDirection,
  calculateChangePercent,
  calculateStats,
  buildTrendSeries,
  buildTrendSummary,

  // Path helpers
  getResultPath,
  getDatePrefix,
  getEnvironmentPrefix,
  parseResultPath,
} from './trend-store'

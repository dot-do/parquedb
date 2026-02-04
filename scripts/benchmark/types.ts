/**
 * Benchmark Framework Types
 *
 * Defines the full matrix of benchmark permutations:
 * - Query types × Storage modes × Backends × Formats × Scales × Cache states
 */

// =============================================================================
// Dimensions
// =============================================================================

export type QueryType =
  | 'full_scan'           // Read all rows, all columns
  | 'column_projection'   // Read all rows, 2-3 columns
  | 'point_lookup'        // Get single row by $id
  | 'filtered_eq'         // Filter with = predicate
  | 'filtered_range'      // Filter with > < predicates
  | 'aggregation_count'   // COUNT(*)
  | 'aggregation_sum'     // SUM(column)
  | 'aggregation_avg'     // AVG(column)
  | 'group_by'            // GROUP BY + COUNT
  | 'top_k'               // ORDER BY + LIMIT
  | 'row_reconstruction'  // Parse data column to full object

export type StorageMode =
  | 'columnar-only'   // Native columns, no row store
  | 'columnar-row'    // Native columns + data JSON blob
  | 'row-only'        // Just $id, $type, name, data
  | 'row-index'       // data + $index_* shredded columns

export type Backend =
  | 'memory'          // In-memory ArrayBuffer
  | 'fs'              // Local filesystem
  | 'r2-direct'       // R2 via REST API
  | 'r2-worker'       // R2 via Worker binding
  | 's3'              // AWS S3

export type TableFormat =
  | 'parquet'         // Raw Parquet files
  | 'iceberg'         // Apache Iceberg
  | 'delta'           // Delta Lake

export type DataScale =
  | '1k'
  | '10k'
  | '100k'
  | '1m'
  | '10m'

export type CacheState =
  | 'cold'            // No caching, fresh read
  | 'warm'            // After warmup reads

// =============================================================================
// Benchmark Configuration
// =============================================================================

export interface BenchmarkConfig {
  /** Unique identifier for this benchmark run */
  runId: string

  /** Timestamp when benchmark started */
  startedAt: Date

  /** Git commit hash */
  gitCommit: string

  /** Git branch */
  gitBranch: string

  /** Environment (local, ci, production) */
  environment: 'local' | 'ci' | 'production'

  /** Number of iterations per test */
  iterations: number

  /** Warmup iterations (not counted) */
  warmupIterations: number

  /** Dimensions to test */
  dimensions: {
    queryTypes: QueryType[]
    storageModes: StorageMode[]
    backends: Backend[]
    tableFormats: TableFormat[]
    dataScales: DataScale[]
    cacheStates: CacheState[]
  }

  /** Datasets to benchmark */
  datasets: string[]
}

// =============================================================================
// Benchmark Results
// =============================================================================

export interface BenchmarkResult {
  /** Unique result ID */
  id: string

  /** Reference to parent config */
  runId: string

  /** Dimensions for this result */
  queryType: QueryType
  storageMode: StorageMode
  backend: Backend
  tableFormat: TableFormat
  dataScale: DataScale
  cacheState: CacheState

  /** Dataset and collection */
  dataset: string
  collection: string

  /** Timing metrics (milliseconds) */
  timing: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
    stdDev: number
  }

  /** Throughput metrics */
  throughput: {
    rowsProcessed: number
    rowsPerSecond: number
    bytesRead: number
    bytesPerSecond: number
  }

  /** Resource usage */
  resources: {
    peakMemoryMB: number
    cpuTimeMs: number
  }

  /** Additional metadata */
  metadata: {
    rowGroupsTotal: number
    rowGroupsRead: number
    columnsRequested: number
    predicatePushdown: boolean
    compressionRatio: number
  }

  /** Error if benchmark failed */
  error?: string
}

// =============================================================================
// Regression Detection
// =============================================================================

export interface RegressionThresholds {
  /** Max allowed increase in p50 latency (percentage) */
  p50LatencyIncrease: number

  /** Max allowed increase in p95 latency (percentage) */
  p95LatencyIncrease: number

  /** Max allowed decrease in throughput (percentage) */
  throughputDecrease: number

  /** Minimum number of runs to compare */
  minRunsForComparison: number
}

export const DEFAULT_THRESHOLDS: RegressionThresholds = {
  p50LatencyIncrease: 20,
  p95LatencyIncrease: 30,
  throughputDecrease: 15,
  minRunsForComparison: 3,
}

export interface RegressionReport {
  /** Baseline run ID */
  baselineRunId: string

  /** Current run ID */
  currentRunId: string

  /** Detected regressions */
  regressions: Array<{
    resultId: string
    dimension: string
    metric: string
    baselineValue: number
    currentValue: number
    changePercent: number
    threshold: number
  }>

  /** Improvements (opposite of regressions) */
  improvements: Array<{
    resultId: string
    dimension: string
    metric: string
    baselineValue: number
    currentValue: number
    changePercent: number
  }>

  /** Overall status */
  status: 'pass' | 'fail'
}

// =============================================================================
// Storage
// =============================================================================

export interface BenchmarkStore {
  /** Save a benchmark run config */
  saveConfig(config: BenchmarkConfig): Promise<void>

  /** Save benchmark results */
  saveResults(results: BenchmarkResult[]): Promise<void>

  /** Get config by run ID */
  getConfig(runId: string): Promise<BenchmarkConfig | null>

  /** Get results for a run */
  getResults(runId: string): Promise<BenchmarkResult[]>

  /** Get baseline for comparison */
  getBaseline(environment: string): Promise<{ config: BenchmarkConfig; results: BenchmarkResult[] } | null>

  /** Set new baseline */
  setBaseline(runId: string): Promise<void>

  /** List recent runs */
  listRuns(limit: number): Promise<BenchmarkConfig[]>

  /** Query results across runs for trend analysis */
  queryTrends(filter: {
    queryType?: QueryType
    storageMode?: StorageMode
    backend?: Backend
    dataScale?: DataScale
    dataset?: string
    startDate?: Date
    endDate?: Date
  }): Promise<BenchmarkResult[]>
}

// =============================================================================
// Executor Interface
// =============================================================================

export interface BenchmarkExecutor {
  /** Execute a single benchmark permutation */
  execute(params: {
    queryType: QueryType
    storageMode: StorageMode
    backend: Backend
    tableFormat: TableFormat
    dataScale: DataScale
    cacheState: CacheState
    dataset: string
    collection: string
    iterations: number
  }): Promise<BenchmarkResult>

  /** Check if this executor supports the given backend */
  supportsBackend(backend: Backend): boolean

  /** Check if this executor supports the given format */
  supportsFormat(format: TableFormat): boolean
}

// =============================================================================
// Reporter Interface
// =============================================================================

export interface BenchmarkReporter {
  /** Generate summary report */
  summary(config: BenchmarkConfig, results: BenchmarkResult[]): string

  /** Generate regression report */
  regression(report: RegressionReport): string

  /** Generate trend chart data */
  trends(results: BenchmarkResult[]): object

  /** Export to CSV */
  csv(results: BenchmarkResult[]): string

  /** Export to JSON */
  json(config: BenchmarkConfig, results: BenchmarkResult[]): string
}

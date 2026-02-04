/**
 * Query Benchmark Runner
 *
 * Implements comprehensive query benchmarks by hitting Worker endpoints.
 * Measures performance for all query types:
 * - Full scan
 * - Column projection
 * - Point lookup
 * - Filtered query (equality)
 * - Range query
 * - Aggregations (COUNT, SUM)
 * - Group by
 * - Top-K
 */

import type {
  QueryType,
  BenchmarkResult,
  StorageMode,
  Backend,
  TableFormat,
  DataScale,
  CacheState,
} from './types'

// =============================================================================
// Configuration
// =============================================================================

export interface QueryBenchmarkConfig {
  /** Base URL for Worker endpoint */
  baseUrl: string

  /** Default storage mode */
  storageMode?: StorageMode

  /** Default backend */
  backend?: Backend

  /** Default table format */
  tableFormat?: TableFormat

  /** Default data scale */
  dataScale?: DataScale

  /** Default cache state */
  cacheState?: CacheState
}

// =============================================================================
// Types
// =============================================================================

interface FullScanOptions {
  dataset: string
  collection: string
  iterations: number
}

interface ColumnProjectionOptions {
  dataset: string
  collection: string
  columns: string[]
  iterations: number
}

interface PointLookupOptions {
  dataset: string
  collection: string
  id: string
  iterations: number
}

interface FilteredQueryOptions {
  dataset: string
  collection: string
  filter: Record<string, unknown>
  iterations: number
}

interface RangeQueryOptions {
  dataset: string
  collection: string
  field: string
  min: number | string
  max: number | string
  iterations: number
}

interface CountAggregationOptions {
  dataset: string
  collection: string
  filter?: Record<string, unknown>
  iterations: number
}

interface SumAggregationOptions {
  dataset: string
  collection: string
  field: string
  filter?: Record<string, unknown>
  iterations: number
}

interface GroupByOptions {
  dataset: string
  collection: string
  groupByField: string
  aggregations: Array<{ field: string; op: 'count' | 'sum' | 'avg' | 'min' | 'max' }>
  iterations: number
}

interface TopKOptions {
  dataset: string
  collection: string
  orderBy: { field: string; direction: 'asc' | 'desc' }
  limit: number
  iterations: number
}

interface BenchmarkResponse {
  timing: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
    stdDev: number
  }
  throughput: {
    rowsProcessed: number
    rowsPerSecond: number
    bytesRead: number
    bytesPerSecond: number
  }
  resources: {
    peakMemoryMB: number
    cpuTimeMs: number
  }
  metadata: {
    rowGroupsTotal: number
    rowGroupsRead: number
    columnsRequested: number
    predicatePushdown: boolean
    compressionRatio: number
  }
  data?: unknown[]
  count?: number
  sum?: number
  groups?: Array<{ _id: unknown; [key: string]: unknown }>
  results?: unknown[]
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Generate a unique result ID
 */
function generateResultId(): string {
  return `result_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`
}

/**
 * Create base benchmark result with defaults
 */
function createBaseBenchmarkResult(
  queryType: QueryType,
  dataset: string,
  collection: string,
  config: QueryBenchmarkConfig
): Omit<BenchmarkResult, 'timing' | 'throughput' | 'resources' | 'metadata'> {
  return {
    id: generateResultId(),
    runId: `run_${Date.now()}`,
    queryType,
    storageMode: config.storageMode ?? 'columnar-row',
    backend: config.backend ?? 'r2-worker',
    tableFormat: config.tableFormat ?? 'parquet',
    dataScale: config.dataScale ?? '10m',
    cacheState: config.cacheState ?? 'warm',
    dataset,
    collection,
  }
}

/**
 * Build URL with query parameters
 */
function buildUrl(
  baseUrl: string,
  dataset: string,
  collection: string,
  params?: Record<string, string | number | boolean | undefined>
): string {
  let url = `${baseUrl}/datasets/${dataset}/${collection}`

  if (params) {
    const searchParams = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) {
        searchParams.append(key, String(value))
      }
    }
    const queryString = searchParams.toString()
    if (queryString) {
      url += `?${queryString}`
    }
  }

  return url
}

/**
 * Build URL for point lookup
 */
function buildPointLookupUrl(baseUrl: string, dataset: string, collection: string, id: string): string {
  return `${baseUrl}/datasets/${dataset}/${collection}/${id}`
}

// =============================================================================
// Query Benchmark Runner Implementation
// =============================================================================

export class QueryBenchmarkRunner {
  private config: QueryBenchmarkConfig

  constructor(config: QueryBenchmarkConfig) {
    this.config = config
  }

  /**
   * Validate iterations parameter
   */
  private validateIterations(iterations: number): void {
    if (iterations <= 0) {
      throw new Error('Iterations must be a positive number')
    }
  }

  /**
   * Execute fetch and parse response
   */
  private async executeQuery(url: string): Promise<BenchmarkResponse> {
    const response = await fetch(url, {
      headers: {
        Accept: 'application/json',
        'User-Agent': 'ParqueDB-Benchmark/1.0',
      },
    })

    if (!response.ok) {
      throw new Error(`Benchmark request failed: ${response.status} ${response.statusText}`)
    }

    return (await response.json()) as BenchmarkResponse
  }

  /**
   * Run a full scan benchmark
   */
  async runFullScan(options: FullScanOptions): Promise<BenchmarkResult> {
    this.validateIterations(options.iterations)

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, {
      limit: 10000,
    })

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('full_scan', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
    }
  }

  /**
   * Run a column projection benchmark
   */
  async runColumnProjection(options: ColumnProjectionOptions): Promise<BenchmarkResult> {
    this.validateIterations(options.iterations)

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, {
      select: options.columns.join(','),
    })

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('column_projection', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
    }
  }

  /**
   * Run a point lookup benchmark
   */
  async runPointLookup(options: PointLookupOptions): Promise<BenchmarkResult> {
    this.validateIterations(options.iterations)

    const url = buildPointLookupUrl(this.config.baseUrl, options.dataset, options.collection, options.id)

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('point_lookup', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
    }
  }

  /**
   * Run a filtered query benchmark (equality filter)
   */
  async runFilteredQuery(options: FilteredQueryOptions): Promise<BenchmarkResult> {
    this.validateIterations(options.iterations)

    // Convert filter to query params
    const params: Record<string, string | number | boolean> = {}
    for (const [key, value] of Object.entries(options.filter)) {
      params[key] = value as string | number | boolean
    }

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, params)

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('filtered_eq', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
    }
  }

  /**
   * Run a range query benchmark
   */
  async runRangeQuery(options: RangeQueryOptions): Promise<BenchmarkResult> {
    this.validateIterations(options.iterations)

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, {
      [`${options.field}.gte`]: options.min,
      [`${options.field}.lte`]: options.max,
    })

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('filtered_range', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
    }
  }

  /**
   * Run a COUNT aggregation benchmark
   */
  async runCountAggregation(options: CountAggregationOptions): Promise<BenchmarkResult & { count: number }> {
    this.validateIterations(options.iterations)

    const params: Record<string, string | number | boolean> = {
      aggregate: 'count',
    }

    // Add filter params if provided
    if (options.filter) {
      for (const [key, value] of Object.entries(options.filter)) {
        params[key] = value as string | number | boolean
      }
    }

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, params)

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('aggregation_count', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
      count: response.count ?? 0,
    }
  }

  /**
   * Run a SUM aggregation benchmark
   */
  async runSumAggregation(options: SumAggregationOptions): Promise<BenchmarkResult & { sum: number }> {
    this.validateIterations(options.iterations)

    const params: Record<string, string | number | boolean> = {
      aggregate: 'sum',
      field: options.field,
    }

    // Add filter params if provided
    if (options.filter) {
      for (const [key, value] of Object.entries(options.filter)) {
        params[key] = value as string | number | boolean
      }
    }

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, params)

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('aggregation_sum', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
      sum: response.sum ?? 0,
    }
  }

  /**
   * Run a GROUP BY benchmark
   */
  async runGroupBy(
    options: GroupByOptions
  ): Promise<BenchmarkResult & { groups: Array<{ _id: unknown; [key: string]: unknown }> }> {
    this.validateIterations(options.iterations)

    // Build aggregation string
    const aggStr = options.aggregations.map((a) => `${a.op}:${a.field}`).join(',')

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, {
      groupBy: options.groupByField,
      aggregate: aggStr,
    })

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('group_by', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
      groups: response.groups ?? [],
    }
  }

  /**
   * Run a TOP-K benchmark
   */
  async runTopK(options: TopKOptions): Promise<BenchmarkResult & { results: unknown[] }> {
    this.validateIterations(options.iterations)

    const url = buildUrl(this.config.baseUrl, options.dataset, options.collection, {
      orderBy: options.orderBy.field,
      order: options.orderBy.direction,
      limit: options.limit,
    })

    const response = await this.executeQuery(url)

    return {
      ...createBaseBenchmarkResult('top_k', options.dataset, options.collection, this.config),
      timing: response.timing,
      throughput: response.throughput,
      resources: response.resources,
      metadata: response.metadata,
      results: response.results ?? response.data ?? [],
    }
  }
}

/**
 * Create a benchmark runner with default configuration
 */
export function createQueryBenchmarkRunner(
  baseUrl: string = 'https://parquedb.workers.do'
): QueryBenchmarkRunner {
  return new QueryBenchmarkRunner({ baseUrl })
}

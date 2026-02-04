/**
 * Benchmark Trend Store
 *
 * Infrastructure for storing and querying benchmark results over time.
 * Supports R2 storage for production and file-based storage for local testing.
 *
 * Storage layout in R2 bucket:
 *   benchmarks/results/{env}/{year}/{month}/{day}/run-{timestamp}-{runId}.json
 *
 * @module observability/benchmarks/trend-store
 */

import type {
  StoredBenchmarkResult,
  E2EBenchmarkSuiteResult,
} from '../e2e/types'

// =============================================================================
// Types
// =============================================================================

/** Time granularity for trend aggregation */
export type TrendGranularity = 'hourly' | 'daily' | 'weekly' | 'monthly'

/** Trend direction */
export type TrendDirection = 'improving' | 'degrading' | 'stable'

/** Trend store configuration */
export interface TrendStoreConfig {
  /** R2 bucket binding (for Workers) */
  bucket?: R2Bucket | undefined
  /** Base URL for R2 operations via HTTP (for Node.js) */
  r2BaseUrl?: string | undefined
  /** R2 API token for authenticated access */
  r2ApiToken?: string | undefined
  /** Wrangler account ID */
  accountId?: string | undefined
  /** R2 bucket name */
  bucketName?: string | undefined
  /** Base path for file-based storage */
  basePath?: string | undefined
}

/** Query options for retrieving historical data */
export interface TrendQueryOptions {
  /** Environment to query */
  environment: string
  /** Start date (inclusive) */
  startDate?: Date | undefined
  /** End date (inclusive) */
  endDate?: Date | undefined
  /** Number of most recent runs to retrieve */
  limit?: number | undefined
  /** Filter by git branch */
  branch?: string | undefined
  /** Filter by git commit SHA (prefix match) */
  commitSha?: string | undefined
  /** Filter by tag */
  tag?: string | undefined
}

/** A single data point in a trend series */
export interface TrendDataPoint {
  timestamp: string
  runId: string
  commitSha?: string | undefined
  branch?: string | undefined
  value: number
}

/** Trend series for a specific metric */
export interface TrendSeries {
  metricName: string
  dataPoints: TrendDataPoint[]
  direction: TrendDirection
  changePercent: number
  stats: {
    min: number
    max: number
    mean: number
    stdDev: number
    current: number
    previous: number
  }
}

/** Aggregated trend summary */
export interface TrendSummary {
  environment: string
  period: {
    start: string
    end: string
    granularity: TrendGranularity
    dataPointCount: number
  }
  metrics: {
    latencyP50: TrendSeries
    latencyP95: TrendSeries
    latencyP99: TrendSeries
    throughput: TrendSeries
    coldStartOverhead: TrendSeries | null
    cacheSpeedup: TrendSeries | null
  }
  overallDirection: TrendDirection
  regressionRisk: 'low' | 'medium' | 'high'
}

/** Result of a save operation */
export interface SaveResult {
  success: boolean
  path: string
  error?: string | undefined
}

// =============================================================================
// Trend Store Interface
// =============================================================================

export interface TrendStore {
  /** Save a benchmark result */
  saveResult(result: StoredBenchmarkResult): Promise<SaveResult>

  /** Get a specific result by run ID */
  getResult(environment: string, runId: string): Promise<StoredBenchmarkResult | null>

  /** Query historical results */
  queryResults(options: TrendQueryOptions): Promise<StoredBenchmarkResult[]>

  /** Get results for the last N days */
  getRecentResults(environment: string, days: number, limit?: number): Promise<StoredBenchmarkResult[]>

  /** List available environments */
  listEnvironments(): Promise<string[]>

  /** List available run IDs for an environment */
  listRunIds(environment: string, options?: { startDate?: Date; endDate?: Date; limit?: number }): Promise<string[]>

  /** Delete a result */
  deleteResult(environment: string, runId: string): Promise<boolean>

  /** Delete results older than a certain date */
  deleteOldResults(environment: string, olderThan: Date): Promise<number>
}

// =============================================================================
// Trend Analysis Helpers
// =============================================================================

/**
 * Calculate trend direction based on data points
 */
export function calculateTrendDirection(values: number[], threshold: number = 5): TrendDirection {
  if (values.length < 2) return 'stable'

  // Use linear regression to determine trend
  const n = values.length
  const indices = values.map((_, i) => i)

  const sumX = indices.reduce((a, b) => a + b, 0)
  const sumY = values.reduce((a, b) => a + b, 0)
  const sumXY = indices.reduce((sum, x, i) => sum + x * values[i]!, 0)
  const sumX2 = indices.reduce((sum, x) => sum + x * x, 0)

  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX)

  // Normalize slope as percentage of mean
  const mean = sumY / n
  const normalizedSlope = mean !== 0 ? (slope / mean) * 100 : 0

  if (Math.abs(normalizedSlope) < threshold) return 'stable'
  // For latency metrics, negative slope is improving (lower is better)
  // Adjust based on metric type when calling this function
  return normalizedSlope < 0 ? 'improving' : 'degrading'
}

/**
 * Calculate percentage change between first and last values
 */
export function calculateChangePercent(values: number[]): number {
  if (values.length < 2) return 0
  const first = values[0]!
  const last = values[values.length - 1]!
  if (first === 0) return 0
  return ((last - first) / first) * 100
}

/**
 * Calculate statistics for a series of values
 */
export function calculateStats(values: number[]): { min: number; max: number; mean: number; stdDev: number } {
  if (values.length === 0) {
    return { min: 0, max: 0, mean: 0, stdDev: 0 }
  }

  const min = Math.min(...values)
  const max = Math.max(...values)
  const mean = values.reduce((a, b) => a + b, 0) / values.length

  const squaredDiffs = values.map(v => (v - mean) ** 2)
  const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / values.length
  const stdDev = Math.sqrt(avgSquaredDiff)

  return { min, max, mean, stdDev }
}

/**
 * Build a trend series from stored results
 */
export function buildTrendSeries(
  results: StoredBenchmarkResult[],
  metricExtractor: (result: E2EBenchmarkSuiteResult) => number | undefined,
  metricName: string,
  invertDirection: boolean = false
): TrendSeries | null {
  const dataPoints: TrendDataPoint[] = []

  for (const result of results) {
    const value = metricExtractor(result.results)
    if (value !== undefined && !isNaN(value)) {
      dataPoints.push({
        timestamp: result.timestamp,
        runId: result.runId,
        commitSha: result.commitSha,
        branch: result.branch,
        value,
      })
    }
  }

  if (dataPoints.length === 0) return null

  // Sort by timestamp ascending
  dataPoints.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())

  const values = dataPoints.map(dp => dp.value)
  const stats = calculateStats(values)
  const changePercent = calculateChangePercent(values)

  let direction = calculateTrendDirection(values)
  // For throughput, higher is better, so invert direction
  if (invertDirection) {
    if (direction === 'improving') direction = 'degrading'
    else if (direction === 'degrading') direction = 'improving'
  }

  return {
    metricName,
    dataPoints,
    direction,
    changePercent,
    stats: {
      ...stats,
      current: values[values.length - 1]!,
      previous: values.length > 1 ? values[values.length - 2]! : values[0]!,
    },
  }
}

/**
 * Build a complete trend summary from stored results
 */
export function buildTrendSummary(
  results: StoredBenchmarkResult[],
  environment: string,
  granularity: TrendGranularity
): TrendSummary | null {
  if (results.length === 0) return null

  // Sort by timestamp
  const sorted = [...results].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  )

  // Build trend series for each metric
  const latencyP50 = buildTrendSeries(
    sorted,
    r => r.summary.avgLatencyMs,
    'latencyP50'
  )
  const latencyP95 = buildTrendSeries(
    sorted,
    r => r.summary.p95LatencyMs,
    'latencyP95'
  )
  const latencyP99 = buildTrendSeries(
    sorted,
    r => {
      // Extract P99 from summary or datasets
      if (r.datasets && r.datasets.length > 0) {
        const allLatencies = r.datasets
          .filter((d): d is { queries?: { latency?: { p99?: number } }[] } =>
            d !== null && typeof d === 'object')
          .flatMap(d => d.queries?.map(q => q.latency?.p99) ?? [])
          .filter((v): v is number => v !== undefined)
        if (allLatencies.length > 0) {
          return allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length
        }
      }
      return r.summary.p95LatencyMs * 1.2 // Estimate P99 from P95
    },
    'latencyP99'
  )
  const throughput = buildTrendSeries(
    sorted,
    r => r.summary.overallThroughput,
    'throughput',
    true // Invert: higher throughput is better
  )
  const coldStartOverhead = buildTrendSeries(
    sorted,
    r => r.coldStart?.overheadPercent,
    'coldStartOverhead'
  )
  const cacheSpeedup = buildTrendSeries(
    sorted,
    r => r.cachePerformance?.speedup ?? r.summary.cacheSpeedup,
    'cacheSpeedup',
    true // Invert: higher speedup is better
  )

  if (!latencyP50 || !latencyP95 || !latencyP99 || !throughput) {
    return null
  }

  // Determine overall direction
  const directions = [
    latencyP50.direction,
    latencyP95.direction,
    latencyP99.direction,
    throughput.direction,
    coldStartOverhead?.direction,
    cacheSpeedup?.direction,
  ].filter((d): d is TrendDirection => d !== undefined)

  const improvingCount = directions.filter(d => d === 'improving').length
  const degradingCount = directions.filter(d => d === 'degrading').length

  let overallDirection: TrendDirection = 'stable'
  if (improvingCount > degradingCount && improvingCount > directions.length / 3) {
    overallDirection = 'improving'
  } else if (degradingCount > improvingCount && degradingCount > directions.length / 3) {
    overallDirection = 'degrading'
  }

  // Determine regression risk
  let regressionRisk: 'low' | 'medium' | 'high' = 'low'
  if (degradingCount >= 3 || latencyP95.direction === 'degrading' && latencyP99.direction === 'degrading') {
    regressionRisk = 'high'
  } else if (degradingCount >= 2 || latencyP50.direction === 'degrading') {
    regressionRisk = 'medium'
  }

  return {
    environment,
    period: {
      start: sorted[0]!.timestamp,
      end: sorted[sorted.length - 1]!.timestamp,
      granularity,
      dataPointCount: sorted.length,
    },
    metrics: {
      latencyP50,
      latencyP95,
      latencyP99,
      throughput,
      coldStartOverhead,
      cacheSpeedup,
    },
    overallDirection,
    regressionRisk,
  }
}

// =============================================================================
// Path Helpers
// =============================================================================

/**
 * Generate the storage path for a benchmark result
 */
export function getResultPath(result: StoredBenchmarkResult): string {
  const date = new Date(result.timestamp)
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  const timestamp = date.toISOString().replace(/[:.]/g, '-')
  return `benchmarks/results/${result.environment}/${year}/${month}/${day}/run-${timestamp}-${result.runId}.json`
}

/**
 * Get the prefix for listing results in a date range
 */
export function getDatePrefix(environment: string, date: Date): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  return `benchmarks/results/${environment}/${year}/${month}/${day}/`
}

/**
 * Get the prefix for an environment
 */
export function getEnvironmentPrefix(environment: string): string {
  return `benchmarks/results/${environment}/`
}

/**
 * Parse run ID and timestamp from a result path
 */
export function parseResultPath(path: string): { environment: string; runId: string; date: Date } | null {
  const match = path.match(/benchmarks\/results\/([^/]+)\/(\d{4})\/(\d{2})\/(\d{2})\/run-[^-]+-([^.]+)\.json/)
  if (!match) return null

  const [, environment, year, month, day, runId] = match
  return {
    environment: environment!,
    runId: runId!,
    date: new Date(Date.UTC(parseInt(year!, 10), parseInt(month!, 10) - 1, parseInt(day!, 10))),
  }
}

// =============================================================================
// R2 Trend Store (for Workers)
// =============================================================================

/**
 * Trend store using Cloudflare R2 bucket binding
 */
export class R2TrendStore implements TrendStore {
  private bucket: R2Bucket

  constructor(bucket: R2Bucket) {
    this.bucket = bucket
  }

  async saveResult(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = getResultPath(result)
    try {
      await this.bucket.put(path, JSON.stringify(result, null, 2), {
        httpMetadata: { contentType: 'application/json' },
        customMetadata: {
          runId: result.runId,
          environment: result.environment,
          commitSha: result.commitSha ?? '',
          branch: result.branch ?? '',
          tag: result.tag ?? '',
        },
      })
      return { success: true, path }
    } catch (error) {
      return {
        success: false,
        path,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async getResult(environment: string, runId: string): Promise<StoredBenchmarkResult | null> {
    // We need to search for the result since we don't know the exact date
    const results = await this.queryResults({ environment, limit: 1000 })
    return results.find(r => r.runId === runId) ?? null
  }

  async queryResults(options: TrendQueryOptions): Promise<StoredBenchmarkResult[]> {
    const { environment, startDate, endDate, limit = 100, branch, commitSha, tag } = options
    const results: StoredBenchmarkResult[] = []

    // Determine date range
    const end = endDate ?? new Date()
    const start = startDate ?? new Date(end.getTime() - 30 * 24 * 60 * 60 * 1000) // Default 30 days

    // Iterate through dates
    const currentDate = new Date(end)
    while (currentDate >= start && results.length < limit) {
      const prefix = getDatePrefix(environment, currentDate)
      const listed = await this.bucket.list({ prefix, limit: limit - results.length })

      for (const object of listed.objects) {
        try {
          const obj = await this.bucket.get(object.key)
          if (obj) {
            const text = await obj.text()
            const result = JSON.parse(text) as StoredBenchmarkResult

            // Apply filters
            if (branch && result.branch !== branch) continue
            if (commitSha && !result.commitSha?.startsWith(commitSha)) continue
            if (tag && result.tag !== tag) continue

            results.push(result)
          }
        } catch {
          // Skip invalid objects
        }
      }

      // Move to previous day
      currentDate.setUTCDate(currentDate.getUTCDate() - 1)
    }

    // Sort by timestamp descending
    return results.sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    )
  }

  async getRecentResults(environment: string, days: number, limit: number = 100): Promise<StoredBenchmarkResult[]> {
    const endDate = new Date()
    const startDate = new Date(endDate.getTime() - days * 24 * 60 * 60 * 1000)
    return this.queryResults({ environment, startDate, endDate, limit })
  }

  async listEnvironments(): Promise<string[]> {
    const prefix = 'benchmarks/results/'
    const environments = new Set<string>()

    const listed = await this.bucket.list({ prefix, delimiter: '/' })
    for (const prefix of listed.delimitedPrefixes) {
      const env = prefix.replace('benchmarks/results/', '').replace('/', '')
      if (env) environments.add(env)
    }

    return Array.from(environments)
  }

  async listRunIds(
    environment: string,
    options: { startDate?: Date; endDate?: Date; limit?: number } = {}
  ): Promise<string[]> {
    const results = await this.queryResults({
      environment,
      startDate: options.startDate,
      endDate: options.endDate,
      limit: options.limit ?? 1000,
    })
    return results.map(r => r.runId)
  }

  async deleteResult(environment: string, runId: string): Promise<boolean> {
    const result = await this.getResult(environment, runId)
    if (!result) return false

    const path = getResultPath(result)
    try {
      await this.bucket.delete(path)
      return true
    } catch {
      return false
    }
  }

  async deleteOldResults(environment: string, olderThan: Date): Promise<number> {
    const results = await this.queryResults({
      environment,
      endDate: olderThan,
      limit: 10000,
    })

    let deleted = 0
    for (const result of results) {
      const resultDate = new Date(result.timestamp)
      if (resultDate < olderThan) {
        if (await this.deleteResult(environment, result.runId)) {
          deleted++
        }
      }
    }

    return deleted
  }
}

// =============================================================================
// File-based Trend Store (for local testing / Node.js)
// =============================================================================

/**
 * Trend store using local filesystem
 * Used for local development and testing
 */
export class FileTrendStore implements TrendStore {
  private basePath: string

  constructor(basePath: string) {
    this.basePath = basePath
  }

  async saveResult(result: StoredBenchmarkResult): Promise<SaveResult> {
    const relativePath = getResultPath(result)
    const fullPath = `${this.basePath}/${relativePath}`
    try {
      const fs = await import('fs/promises')
      const dir = fullPath.substring(0, fullPath.lastIndexOf('/'))
      await fs.mkdir(dir, { recursive: true })
      await fs.writeFile(fullPath, JSON.stringify(result, null, 2))
      return { success: true, path: fullPath }
    } catch (error) {
      return {
        success: false,
        path: fullPath,
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  async getResult(environment: string, runId: string): Promise<StoredBenchmarkResult | null> {
    const results = await this.queryResults({ environment, limit: 1000 })
    return results.find(r => r.runId === runId) ?? null
  }

  async queryResults(options: TrendQueryOptions): Promise<StoredBenchmarkResult[]> {
    const { environment, startDate, endDate, limit = 100, branch, commitSha, tag } = options
    const results: StoredBenchmarkResult[] = []

    try {
      const fs = await import('fs/promises')
      const path = await import('path')

      const end = endDate ?? new Date()
      const start = startDate ?? new Date(end.getTime() - 30 * 24 * 60 * 60 * 1000)

      const currentDate = new Date(end)
      while (currentDate >= start && results.length < limit) {
        const prefix = getDatePrefix(environment, currentDate)
        const dirPath = `${this.basePath}/${prefix}`

        try {
          const files = await fs.readdir(dirPath)
          for (const file of files) {
            if (!file.endsWith('.json')) continue
            try {
              const text = await fs.readFile(path.join(dirPath, file), 'utf-8')
              const result = JSON.parse(text) as StoredBenchmarkResult

              // Apply filters
              if (branch && result.branch !== branch) continue
              if (commitSha && !result.commitSha?.startsWith(commitSha)) continue
              if (tag && result.tag !== tag) continue

              results.push(result)
              if (results.length >= limit) break
            } catch {
              // Skip invalid files
            }
          }
        } catch {
          // Directory doesn't exist
        }

        currentDate.setUTCDate(currentDate.getUTCDate() - 1)
      }
    } catch {
      // fs module not available
    }

    return results.sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    )
  }

  async getRecentResults(environment: string, days: number, limit: number = 100): Promise<StoredBenchmarkResult[]> {
    const endDate = new Date()
    const startDate = new Date(endDate.getTime() - days * 24 * 60 * 60 * 1000)
    return this.queryResults({ environment, startDate, endDate, limit })
  }

  async listEnvironments(): Promise<string[]> {
    try {
      const fs = await import('fs/promises')
      const basePath = `${this.basePath}/benchmarks/results`
      const entries = await fs.readdir(basePath, { withFileTypes: true })
      return entries
        .filter(e => e.isDirectory())
        .map(e => e.name)
    } catch {
      return []
    }
  }

  async listRunIds(
    environment: string,
    options: { startDate?: Date; endDate?: Date; limit?: number } = {}
  ): Promise<string[]> {
    const results = await this.queryResults({
      environment,
      startDate: options.startDate,
      endDate: options.endDate,
      limit: options.limit ?? 1000,
    })
    return results.map(r => r.runId)
  }

  async deleteResult(environment: string, runId: string): Promise<boolean> {
    const result = await this.getResult(environment, runId)
    if (!result) return false

    const relativePath = getResultPath(result)
    const fullPath = `${this.basePath}/${relativePath}`
    try {
      const fs = await import('fs/promises')
      await fs.unlink(fullPath)
      return true
    } catch {
      return false
    }
  }

  async deleteOldResults(environment: string, olderThan: Date): Promise<number> {
    const results = await this.queryResults({
      environment,
      endDate: olderThan,
      limit: 10000,
    })

    let deleted = 0
    for (const result of results) {
      const resultDate = new Date(result.timestamp)
      if (resultDate < olderThan) {
        if (await this.deleteResult(environment, result.runId)) {
          deleted++
        }
      }
    }

    return deleted
  }
}

// =============================================================================
// HTTP Trend Store (for fetching from R2 via HTTP)
// =============================================================================

/**
 * Trend store that fetches results via HTTP
 * Read-only store for use from CI/CD environments without R2 bindings
 */
export class HttpTrendStore implements TrendStore {
  private baseUrl: string
  private headers: Record<string, string>

  constructor(baseUrl: string, headers: Record<string, string> = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.headers = headers
  }

  async saveResult(_result: StoredBenchmarkResult): Promise<SaveResult> {
    return {
      success: false,
      path: '',
      error: 'HttpTrendStore is read-only. Use wrangler r2 or R2TrendStore to save results.',
    }
  }

  async getResult(environment: string, runId: string): Promise<StoredBenchmarkResult | null> {
    // Without knowing the exact date, we need to use the index endpoint if available
    try {
      const url = `${this.baseUrl}/benchmarks/results/${environment}/index.json`
      const response = await fetch(url, { headers: this.headers })
      if (!response.ok) return null

      const index = (await response.json()) as { results: Array<{ runId: string; path: string }> }
      const entry = index.results.find(r => r.runId === runId)
      if (!entry) return null

      const resultResponse = await fetch(`${this.baseUrl}/${entry.path}`, { headers: this.headers })
      if (!resultResponse.ok) return null

      return (await resultResponse.json()) as StoredBenchmarkResult
    } catch {
      return null
    }
  }

  async queryResults(_options: TrendQueryOptions): Promise<StoredBenchmarkResult[]> {
    // HTTP store has limited query capability - requires server-side support
    return []
  }

  async getRecentResults(environment: string, days: number, limit: number = 100): Promise<StoredBenchmarkResult[]> {
    try {
      const url = `${this.baseUrl}/benchmarks/results/${environment}/recent.json?days=${days}&limit=${limit}`
      const response = await fetch(url, { headers: this.headers })
      if (!response.ok) return []

      return (await response.json()) as StoredBenchmarkResult[]
    } catch {
      return []
    }
  }

  async listEnvironments(): Promise<string[]> {
    try {
      const url = `${this.baseUrl}/benchmarks/results/environments.json`
      const response = await fetch(url, { headers: this.headers })
      if (!response.ok) return []

      const data = (await response.json()) as { environments: string[] }
      return data.environments
    } catch {
      return []
    }
  }

  async listRunIds(_environment: string, _options?: { startDate?: Date; endDate?: Date; limit?: number }): Promise<string[]> {
    return []
  }

  async deleteResult(_environment: string, _runId: string): Promise<boolean> {
    return false
  }

  async deleteOldResults(_environment: string, _olderThan: Date): Promise<number> {
    return 0
  }
}

// =============================================================================
// In-Memory Trend Store (for testing)
// =============================================================================

/**
 * In-memory trend store for unit testing
 */
export class MemoryTrendStore implements TrendStore {
  private results: Map<string, StoredBenchmarkResult> = new Map()

  async saveResult(result: StoredBenchmarkResult): Promise<SaveResult> {
    const path = getResultPath(result)
    this.results.set(path, result)
    return { success: true, path }
  }

  async getResult(environment: string, runId: string): Promise<StoredBenchmarkResult | null> {
    for (const result of this.results.values()) {
      if (result.environment === environment && result.runId === runId) {
        return result
      }
    }
    return null
  }

  async queryResults(options: TrendQueryOptions): Promise<StoredBenchmarkResult[]> {
    const { environment, startDate, endDate, limit = 100, branch, commitSha, tag } = options

    const results = Array.from(this.results.values())
      .filter(r => {
        if (r.environment !== environment) return false
        if (startDate && new Date(r.timestamp) < startDate) return false
        if (endDate && new Date(r.timestamp) > endDate) return false
        if (branch && r.branch !== branch) return false
        if (commitSha && !r.commitSha?.startsWith(commitSha)) return false
        if (tag && r.tag !== tag) return false
        return true
      })
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
      .slice(0, limit)

    return results
  }

  async getRecentResults(environment: string, days: number, limit: number = 100): Promise<StoredBenchmarkResult[]> {
    const endDate = new Date()
    const startDate = new Date(endDate.getTime() - days * 24 * 60 * 60 * 1000)
    return this.queryResults({ environment, startDate, endDate, limit })
  }

  async listEnvironments(): Promise<string[]> {
    const environments = new Set<string>()
    for (const result of this.results.values()) {
      environments.add(result.environment)
    }
    return Array.from(environments)
  }

  async listRunIds(
    environment: string,
    options: { startDate?: Date; endDate?: Date; limit?: number } = {}
  ): Promise<string[]> {
    const results = await this.queryResults({
      environment,
      startDate: options.startDate,
      endDate: options.endDate,
      limit: options.limit ?? 1000,
    })
    return results.map(r => r.runId)
  }

  async deleteResult(environment: string, runId: string): Promise<boolean> {
    for (const [path, result] of this.results.entries()) {
      if (result.environment === environment && result.runId === runId) {
        this.results.delete(path)
        return true
      }
    }
    return false
  }

  async deleteOldResults(environment: string, olderThan: Date): Promise<number> {
    let deleted = 0
    for (const [path, result] of this.results.entries()) {
      if (result.environment === environment && new Date(result.timestamp) < olderThan) {
        this.results.delete(path)
        deleted++
      }
    }
    return deleted
  }

  /** Clear all stored results (for testing) */
  clear(): void {
    this.results.clear()
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a trend store from configuration
 */
export function createTrendStore(config: TrendStoreConfig): TrendStore {
  if (config.bucket) {
    return new R2TrendStore(config.bucket)
  }

  if (config.r2BaseUrl) {
    const headers: Record<string, string> = {}
    if (config.r2ApiToken) {
      headers['Authorization'] = `Bearer ${config.r2ApiToken}`
    }
    return new HttpTrendStore(config.r2BaseUrl, headers)
  }

  if (config.basePath) {
    return new FileTrendStore(config.basePath)
  }

  // Default to file-based store for local testing
  return new FileTrendStore('./benchmark-results')
}

/**
 * Create a stored benchmark result with metadata
 */
export function createStoredResult(
  suiteResult: E2EBenchmarkSuiteResult,
  metadata: {
    runId: string
    environment: string
    commitSha?: string | undefined
    branch?: string | undefined
    tag?: string | undefined
  }
): StoredBenchmarkResult {
  return {
    runId: metadata.runId,
    commitSha: metadata.commitSha,
    branch: metadata.branch,
    tag: metadata.tag,
    environment: metadata.environment,
    results: suiteResult,
    timestamp: new Date().toISOString(),
  }
}

/**
 * Generate a unique run ID
 */
export function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `${timestamp}-${random}`
}

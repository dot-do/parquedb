/**
 * Utility functions for E2E Benchmark Suite
 *
 * Helpers for making requests, calculating statistics, and formatting output.
 */

import type {
  E2EBenchmarkConfig,
  RequestResult,
  LatencyStats,
  BenchmarkTestResult,
  BenchmarkCategory,
  RegressionAnalysis,
  RegressionThresholds,
  DEFAULT_REGRESSION_THRESHOLDS,
  E2EBenchmarkSuiteResult,
} from './types'

// =============================================================================
// HTTP Request Utilities
// =============================================================================

/**
 * Make a timed HTTP request
 */
export async function timedFetch(
  url: string,
  options: RequestInit = {},
  timeout: number = 30000
): Promise<RequestResult> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  const start = performance.now()

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    })
    const latencyMs = performance.now() - start

    clearTimeout(timeoutId)

    // Extract headers
    const headers: Record<string, string> = {}
    response.headers.forEach((value, key) => {
      headers[key.toLowerCase()] = value
    })

    // Determine cache status
    const cacheStatus = parseCacheStatus(headers)
    const cfColo = headers['cf-ray']?.split('-')[1] || headers['x-colo']

    // Parse response body
    let data: unknown
    const contentType = response.headers.get('content-type') || ''
    if (contentType.includes('application/json')) {
      try {
        data = await response.json()
      } catch {
        data = await response.text()
      }
    } else {
      data = await response.text()
    }

    return {
      success: response.ok,
      status: response.status,
      latencyMs,
      data,
      headers,
      serverTiming: headers['server-timing'],
      cacheStatus,
      cfColo,
    }
  } catch (error) {
    clearTimeout(timeoutId)
    const latencyMs = performance.now() - start

    const errorMessage =
      error instanceof Error
        ? error.name === 'AbortError'
          ? `Request timeout after ${timeout}ms`
          : error.message
        : String(error)

    return {
      success: false,
      latencyMs,
      error: errorMessage,
    }
  }
}

/**
 * Parse cache status from headers
 */
function parseCacheStatus(
  headers: Record<string, string>
): RequestResult['cacheStatus'] {
  // Check CF-Cache-Status
  const cfCache = headers['cf-cache-status']?.toUpperCase()
  if (cfCache) {
    if (cfCache === 'HIT') return 'HIT'
    if (cfCache === 'MISS') return 'MISS'
    if (cfCache === 'STALE') return 'STALE'
    if (cfCache === 'BYPASS') return 'BYPASS'
    if (cfCache === 'DYNAMIC') return 'DYNAMIC'
  }

  // Check X-Cache
  const xCache = headers['x-cache']?.toUpperCase()
  if (xCache?.includes('HIT')) return 'HIT'
  if (xCache?.includes('MISS')) return 'MISS'

  return 'UNKNOWN'
}

/**
 * Make multiple requests and collect results
 */
export async function runBenchmarkIterations(
  url: string,
  iterations: number,
  warmup: number,
  options: RequestInit = {},
  timeout: number = 30000
): Promise<{ results: RequestResult[]; warmupResults: RequestResult[] }> {
  const warmupResults: RequestResult[] = []
  const results: RequestResult[] = []

  // Warmup iterations
  for (let i = 0; i < warmup; i++) {
    const result = await timedFetch(url, options, timeout)
    warmupResults.push(result)
  }

  // Measured iterations
  for (let i = 0; i < iterations; i++) {
    const result = await timedFetch(url, options, timeout)
    results.push(result)
  }

  return { results, warmupResults }
}

/**
 * Run concurrent requests
 */
export async function runConcurrentRequests(
  url: string,
  concurrency: number,
  options: RequestInit = {},
  timeout: number = 30000
): Promise<RequestResult[]> {
  const promises = Array.from({ length: concurrency }, () =>
    timedFetch(url, options, timeout)
  )
  return Promise.all(promises)
}

// =============================================================================
// Statistics Utilities
// =============================================================================

/**
 * Calculate latency statistics from an array of latencies
 */
export function calculateLatencyStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return {
      min: 0,
      max: 0,
      mean: 0,
      median: 0,
      p50: 0,
      p75: 0,
      p90: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
    }
  }

  const sorted = [...latencies].sort((a, b) => a - b)
  const n = sorted.length
  const mean = sorted.reduce((a, b) => a + b, 0) / n

  // Calculate standard deviation
  const variance = sorted.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  // Percentile helper
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

/**
 * Calculate throughput from latencies
 */
export function calculateThroughput(
  totalTimeMs: number,
  requestCount: number
): number {
  if (totalTimeMs === 0) return 0
  return (requestCount / totalTimeMs) * 1000 // requests per second
}

/**
 * Build a BenchmarkTestResult from request results
 */
export function buildTestResult(
  name: string,
  description: string,
  category: BenchmarkCategory,
  results: RequestResult[]
): BenchmarkTestResult {
  const successResults = results.filter((r) => r.success)
  const failureCount = results.length - successResults.length
  const latencies = successResults.map((r) => r.latencyMs)
  const latencyStats = calculateLatencyStats(latencies)

  // Calculate cache hit rate
  const cacheResults = results.filter((r) => r.cacheStatus !== 'UNKNOWN')
  const cacheHits = cacheResults.filter((r) => r.cacheStatus === 'HIT').length
  const cacheHitRate =
    cacheResults.length > 0 ? cacheHits / cacheResults.length : undefined

  const totalTime = latencies.reduce((a, b) => a + b, 0)

  return {
    name,
    description,
    category,
    successCount: successResults.length,
    failureCount,
    totalIterations: results.length,
    latency: latencyStats,
    throughput: calculateThroughput(totalTime, successResults.length),
    rawLatencies: latencies,
    cacheHitRate,
    timestamp: new Date().toISOString(),
  }
}

// =============================================================================
// Formatting Utilities
// =============================================================================

/**
 * Format milliseconds with appropriate precision
 */
export function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`
  if (ms < 10) return `${ms.toFixed(2)}ms`
  if (ms < 100) return `${ms.toFixed(1)}ms`
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

/**
 * Format percentage
 */
export function formatPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`
}

/**
 * Format bytes
 */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex++
  }

  return `${value.toFixed(2)} ${units[unitIndex]}`
}

/**
 * Format speedup factor
 */
export function formatSpeedup(speedup: number): string {
  if (speedup >= 1) {
    return `${speedup.toFixed(1)}x faster`
  }
  return `${(1 / speedup).toFixed(1)}x slower`
}

/**
 * Print a formatted table row
 */
export function tableRow(columns: string[], widths: number[]): string {
  return columns
    .map((col, i) => {
      const width = widths[i] || 10
      return col.length > width ? col.slice(0, width - 2) + '..' : col.padEnd(width)
    })
    .join(' ')
}

/**
 * Print a separator line
 */
export function tableSeparator(widths: number[]): string {
  return widths.map((w) => '-'.repeat(w)).join(' ')
}

// =============================================================================
// Console Output Utilities
// =============================================================================

/**
 * Print benchmark result as table
 */
export function printResultTable(result: BenchmarkTestResult): void {
  const widths = [35, 10, 10, 10, 10, 12]
  const headers = ['Test', 'Mean', 'P50', 'P95', 'P99', 'Throughput']

  console.log(tableSeparator(widths))
  console.log(tableRow(headers, widths))
  console.log(tableSeparator(widths))

  console.log(
    tableRow(
      [
        result.name,
        formatMs(result.latency.mean),
        formatMs(result.latency.p50),
        formatMs(result.latency.p95),
        formatMs(result.latency.p99),
        `${result.throughput.toFixed(1)}/s`,
      ],
      widths
    )
  )
}

/**
 * Print suite summary
 */
export function printSuiteSummary(result: E2EBenchmarkSuiteResult): void {
  console.log('\n' + '='.repeat(70))
  console.log('E2E BENCHMARK SUITE SUMMARY')
  console.log('='.repeat(70))

  console.log(`\nWorker URL: ${result.metadata.workerUrl}`)
  console.log(`Duration: ${formatMs(result.metadata.durationMs)}`)
  console.log(`CF Colo: ${result.metadata.cfColo || 'N/A'}`)

  console.log('\n--- Health Check ---')
  console.log(
    `  Status: ${result.health.success ? 'OK' : 'FAILED'}` +
      ` (${formatMs(result.health.latencyMs)})`
  )

  if (result.coldStart) {
    console.log('\n--- Cold Start ---')
    console.log(`  Cold: ${formatMs(result.coldStart.coldLatencyMs)}`)
    console.log(`  Warm: ${formatMs(result.coldStart.warmLatencyMs)}`)
    console.log(
      `  Overhead: ${formatMs(result.coldStart.overheadMs)} (${result.coldStart.overheadPercent.toFixed(0)}%)`
    )
  }

  if (result.cachePerformance) {
    console.log('\n--- Cache Performance ---')
    console.log(`  Miss: ${formatMs(result.cachePerformance.missLatency.p50)}`)
    console.log(`  Hit:  ${formatMs(result.cachePerformance.hitLatency.p50)}`)
    console.log(`  Speedup: ${formatSpeedup(result.cachePerformance.speedup)}`)
    console.log(`  Hit Rate: ${formatPercent(result.cachePerformance.hitRate)}`)
  }

  console.log('\n--- Overall ---')
  console.log(`  Total Tests: ${result.summary.totalTests}`)
  console.log(`  Passed: ${result.summary.passedTests}`)
  console.log(`  Failed: ${result.summary.failedTests}`)
  console.log(`  Avg Latency: ${formatMs(result.summary.avgLatencyMs)}`)
  console.log(`  P95 Latency: ${formatMs(result.summary.p95LatencyMs)}`)

  if (result.regression) {
    console.log('\n--- Regression Analysis ---')
    console.log(
      `  Status: ${result.regression.hasRegression ? 'REGRESSION DETECTED' : 'No regression'}`
    )
    console.log(`  Severity: ${result.regression.severity}`)
    console.log(`  ${result.regression.message}`)
  }

  console.log('\n' + '='.repeat(70))
}

// =============================================================================
// Regression Detection
// =============================================================================

/**
 * Detect performance regressions
 */
export function detectRegressions(
  current: E2EBenchmarkSuiteResult,
  baseline: E2EBenchmarkSuiteResult,
  thresholds: RegressionThresholds = {
    latencyP50: 20,
    latencyP95: 25,
    latencyP99: 30,
    coldStartOverhead: 50,
    cacheHitRate: -10,
    throughput: -15,
  }
): RegressionAnalysis {
  const metrics: RegressionAnalysis['metrics'] = []

  // Compare latency metrics
  if (baseline.summary.avgLatencyMs && current.summary.avgLatencyMs) {
    const change =
      ((current.summary.avgLatencyMs - baseline.summary.avgLatencyMs) /
        baseline.summary.avgLatencyMs) *
      100
    metrics.push({
      name: 'Average Latency',
      baseline: baseline.summary.avgLatencyMs,
      current: current.summary.avgLatencyMs,
      change: current.summary.avgLatencyMs - baseline.summary.avgLatencyMs,
      changePercent: change,
      isRegression: change > thresholds.latencyP50,
      threshold: thresholds.latencyP50,
    })
  }

  if (baseline.summary.p95LatencyMs && current.summary.p95LatencyMs) {
    const change =
      ((current.summary.p95LatencyMs - baseline.summary.p95LatencyMs) /
        baseline.summary.p95LatencyMs) *
      100
    metrics.push({
      name: 'P95 Latency',
      baseline: baseline.summary.p95LatencyMs,
      current: current.summary.p95LatencyMs,
      change: current.summary.p95LatencyMs - baseline.summary.p95LatencyMs,
      changePercent: change,
      isRegression: change > thresholds.latencyP95,
      threshold: thresholds.latencyP95,
    })
  }

  // Compare cold start
  if (baseline.coldStart && current.coldStart) {
    const change =
      ((current.coldStart.overheadMs - baseline.coldStart.overheadMs) /
        baseline.coldStart.overheadMs) *
      100
    metrics.push({
      name: 'Cold Start Overhead',
      baseline: baseline.coldStart.overheadMs,
      current: current.coldStart.overheadMs,
      change: current.coldStart.overheadMs - baseline.coldStart.overheadMs,
      changePercent: change,
      isRegression: change > thresholds.coldStartOverhead,
      threshold: thresholds.coldStartOverhead,
    })
  }

  // Compare throughput
  if (baseline.summary.overallThroughput && current.summary.overallThroughput) {
    const change =
      ((current.summary.overallThroughput - baseline.summary.overallThroughput) /
        baseline.summary.overallThroughput) *
      100
    metrics.push({
      name: 'Throughput',
      baseline: baseline.summary.overallThroughput,
      current: current.summary.overallThroughput,
      change:
        current.summary.overallThroughput - baseline.summary.overallThroughput,
      changePercent: change,
      isRegression: change < thresholds.throughput, // Negative threshold = decrease is bad
      threshold: thresholds.throughput,
    })
  }

  // Determine overall regression status
  const regressionCount = metrics.filter((m) => m.isRegression).length
  const hasRegression = regressionCount > 0

  let severity: RegressionAnalysis['severity'] = 'none'
  if (regressionCount >= 3) severity = 'severe'
  else if (regressionCount >= 2) severity = 'moderate'
  else if (regressionCount >= 1) severity = 'minor'

  const message = hasRegression
    ? `${regressionCount} metric(s) show regression: ${metrics
        .filter((m) => m.isRegression)
        .map((m) => m.name)
        .join(', ')}`
    : 'All metrics within acceptable thresholds'

  return {
    baselineVersion: 'baseline',
    currentVersion: 'current',
    metrics,
    hasRegression,
    severity,
    message,
  }
}

// =============================================================================
// Result Storage Utilities
// =============================================================================

/**
 * Generate a unique run ID
 */
export function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `run-${timestamp}-${random}`
}

/**
 * Sleep for specified milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Retry a function with exponential backoff
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelayMs: number = 1000
): Promise<T> {
  let lastError: Error | undefined

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn()
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error))

      if (attempt < maxRetries) {
        const delay = baseDelayMs * Math.pow(2, attempt)
        await sleep(delay)
      }
    }
  }

  throw lastError
}

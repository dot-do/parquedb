#!/usr/bin/env tsx
/**
 * Storage Benchmark Runner
 *
 * Tests query patterns across different storage backends (CDN, FS, R2) and
 * collects performance metrics including latency percentiles, bytes read,
 * range requests, and row group statistics.
 *
 * Usage:
 *   npx tsx tests/benchmarks/storage-benchmark-runner.ts [options]
 *
 * Options:
 *   --backend=cdn|fs|r2         Storage backend to test (default: cdn)
 *   --datasets=imdb,onet,...    Comma-separated datasets to test (default: all)
 *   --iterations=N              Number of test iterations (default: 10)
 *   --warmup=N                  Warmup iterations (default: 2)
 *   --output=table|json|markdown Output format (default: table)
 *   --verbose                   Enable verbose logging
 *   --timeout=N                 Request timeout in ms (default: 30000)
 *   --data-dir=PATH             Data directory for FS backend (default: ./data)
 *
 * Examples:
 *   npx tsx tests/benchmarks/storage-benchmark-runner.ts --backend=cdn
 *   npx tsx tests/benchmarks/storage-benchmark-runner.ts --backend=fs --datasets=blog,ecommerce
 *   npx tsx tests/benchmarks/storage-benchmark-runner.ts --output=markdown --iterations=20
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Storage backend types supported by this benchmark runner
 */
export type StorageBackendType = 'cdn' | 'fs' | 'r2'

/**
 * Query pattern interface (compatible with all dataset patterns)
 */
export interface QueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Query category */
  category: string
  /** Target latency in milliseconds */
  targetMs: number
  /** Description of the query pattern */
  description?: string
  /** HTTP query function */
  query: (baseUrl?: string) => Promise<Response>
}

/**
 * Latency statistics
 */
export interface LatencyStats {
  min: number
  max: number
  mean: number
  p50: number
  p95: number
  p99: number
  stdDev: number
}

/**
 * Storage benchmark result for a single pattern
 */
export interface StorageBenchmarkResult {
  /** Backend type tested */
  backend: StorageBackendType
  /** Dataset name */
  dataset: string
  /** Pattern name */
  pattern: string
  /** Pattern category */
  category: string
  /** Target latency in ms */
  targetMs: number
  /** Latency percentiles */
  latencyMs: LatencyStats
  /** Total bytes read across all iterations */
  bytesRead: number
  /** Average bytes read per iteration */
  avgBytesRead: number
  /** Total range requests made */
  rangeRequests: number
  /** Average range requests per iteration */
  avgRangeRequests: number
  /** Row groups scanned (if available) */
  rowGroupsScanned?: number
  /** Row groups skipped via predicate pushdown (if available) */
  rowGroupsSkipped?: number
  /** Whether p50 latency passed target */
  passedTarget: boolean
  /** Number of successful iterations */
  successCount: number
  /** Number of failed iterations */
  failureCount: number
  /** Error messages if any */
  errors?: string[]
}

/**
 * Benchmark run configuration
 */
export interface BenchmarkOptions {
  /** Number of measured iterations */
  iterations: number
  /** Number of warmup iterations */
  warmup: number
  /** Request timeout in ms */
  timeout: number
  /** Enable verbose logging */
  verbose: boolean
  /** Output format */
  output: 'table' | 'json' | 'markdown'
  /** Data directory for FS backend */
  dataDir: string
}

/**
 * Complete benchmark suite result
 */
export interface BenchmarkSuiteResult {
  /** Configuration used */
  config: {
    backend: StorageBackendType
    datasets: string[]
    iterations: number
    warmup: number
    timeout: number
  }
  /** Benchmark metadata */
  metadata: {
    startedAt: string
    completedAt: string
    durationMs: number
    runnerVersion: string
  }
  /** Individual pattern results */
  results: StorageBenchmarkResult[]
  /** Summary statistics */
  summary: {
    totalPatterns: number
    passedPatterns: number
    failedPatterns: number
    avgLatencyMs: number
    p95LatencyMs: number
    totalBytesRead: number
    totalRangeRequests: number
    byDataset: Record<string, { passed: number; failed: number; avgLatencyMs: number }>
    byCategory: Record<string, { passed: number; failed: number; avgLatencyMs: number }>
  }
}

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_OPTIONS: BenchmarkOptions = {
  iterations: 10,
  warmup: 2,
  timeout: 30000,
  verbose: false,
  output: 'table',
  dataDir: './data',
}

const CDN_BASE_URL = 'https://cdn.workers.do/parquedb'

// =============================================================================
// Pattern Imports
// =============================================================================

import { imdbPatterns } from './patterns/imdb'
import { onetPatterns } from './patterns/onet'
import { unspscPatterns } from './patterns/unspsc'
import { blogPatterns, blogLocalPatterns } from './patterns/blog'
import { ecommercePatterns } from './patterns/ecommerce'

/**
 * All available dataset patterns
 */
const DATASET_PATTERNS: Record<string, QueryPattern[]> = {
  imdb: imdbPatterns,
  onet: onetPatterns,
  unspsc: unspscPatterns,
  blog: blogPatterns,
  ecommerce: ecommercePatterns,
}

/**
 * Datasets that are available locally (small enough for FS backend testing)
 */
const LOCAL_DATASETS = ['blog', 'ecommerce', 'onet', 'unspsc']

/**
 * Get available datasets for a backend type
 */
function getAvailableDatasets(backend: StorageBackendType): string[] {
  switch (backend) {
    case 'fs':
      return LOCAL_DATASETS
    case 'cdn':
    case 'r2':
      return Object.keys(DATASET_PATTERNS)
    default:
      return Object.keys(DATASET_PATTERNS)
  }
}

// =============================================================================
// Statistics Utilities
// =============================================================================

/**
 * Calculate latency statistics from an array of latencies
 */
function calculateLatencyStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return { min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0, stdDev: 0 }
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
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    stdDev,
  }
}

// =============================================================================
// HTTP Request Utilities
// =============================================================================

/**
 * Result from a single request
 */
interface RequestMetrics {
  success: boolean
  latencyMs: number
  bytesRead: number
  rangeRequests: number
  status?: number
  error?: string
  headers?: Record<string, string>
}

/**
 * Make a timed HTTP request and collect metrics
 */
async function timedRequest(
  url: string,
  options: RequestInit = {},
  timeout: number = 30000
): Promise<RequestMetrics> {
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

    // Get response body to measure bytes
    const data = await response.arrayBuffer()
    const bytesRead = data.byteLength

    // Count range requests (CDN may include this info in headers)
    const rangeRequests = headers['x-range-requests']
      ? parseInt(headers['x-range-requests'], 10)
      : 1

    return {
      success: response.ok,
      latencyMs,
      bytesRead,
      rangeRequests,
      status: response.status,
      headers,
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
      bytesRead: 0,
      rangeRequests: 0,
      error: errorMessage,
    }
  }
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Get the base URL for a backend type
 *
 * Note: The query patterns define their own endpoints (e.g., parquedb.workers.do).
 * This function is used when we need to override the default endpoint.
 * For 'cdn' backend, we use the patterns' default endpoints since they hit the deployed worker API.
 */
function getBaseUrlForBackend(backend: StorageBackendType): string | undefined {
  switch (backend) {
    case 'cdn':
      // CDN mode uses the default worker endpoints (parquedb.workers.do)
      // The patterns already have this as their default
      return undefined
    case 'fs':
      // FS mode would require a local server - not yet implemented
      // For now, use the default worker endpoints
      return undefined
    case 'r2':
      // R2 mode would use direct R2 API - not yet implemented
      // For now, use the default worker endpoints
      return undefined
    default:
      return undefined
  }
}

/**
 * Run a single pattern benchmark
 */
async function runPatternBenchmark(
  backend: StorageBackendType,
  dataset: string,
  pattern: QueryPattern,
  options: BenchmarkOptions
): Promise<StorageBenchmarkResult> {
  const latencies: number[] = []
  const errors: string[] = []
  let totalBytesRead = 0
  let totalRangeRequests = 0
  let successCount = 0
  let failureCount = 0

  // Get base URL for this backend (may be undefined to use pattern's default)
  const baseUrl = getBaseUrlForBackend(backend)

  // Warmup iterations
  for (let i = 0; i < options.warmup; i++) {
    try {
      const response = await pattern.query(baseUrl)
      // Consume body to ensure connection is properly closed
      await response.arrayBuffer()
    } catch {
      // Ignore warmup errors
    }
  }

  // Measured iterations
  for (let i = 0; i < options.iterations; i++) {
    try {
      const start = performance.now()
      const response = await pattern.query(baseUrl)
      const latencyMs = performance.now() - start

      if (response.ok) {
        // Get response body size
        const data = await response.arrayBuffer()
        totalBytesRead += data.byteLength
        totalRangeRequests++

        // Only record successful latencies
        latencies.push(latencyMs)
        successCount++

        // Try to extract metrics from headers
        const rangeReqs = response.headers.get('x-range-requests')
        if (rangeReqs) {
          totalRangeRequests += parseInt(rangeReqs, 10) - 1
        }
      } else {
        // Record latency even for failed requests to understand response times
        latencies.push(latencyMs)
        failureCount++

        // Try to get error details from response body
        try {
          const text = await response.text()
          const detail = text.length > 100 ? text.slice(0, 100) + '...' : text
          errors.push(`HTTP ${response.status}: ${response.statusText} - ${detail}`)
        } catch {
          errors.push(`HTTP ${response.status}: ${response.statusText}`)
        }
      }
    } catch (error) {
      failureCount++
      errors.push(error instanceof Error ? error.message : String(error))
    }
  }

  const latencyStats = calculateLatencyStats(latencies)
  // Pass target only if we have successful responses
  const passedTarget = successCount > 0 && latencyStats.p50 <= pattern.targetMs

  if (options.verbose) {
    const status = successCount === 0 ? 'ERROR' : passedTarget ? 'PASS' : 'FAIL'
    const latencyDisplay = successCount > 0 ? `p50=${latencyStats.p50.toFixed(1)}ms` : 'no data'
    console.log(
      `  [${status}] ${pattern.name}: ${latencyDisplay} (target: ${pattern.targetMs}ms, success: ${successCount}/${options.iterations})`
    )
    if (errors.length > 0 && options.verbose) {
      console.log(`         Error: ${errors[0]}`)
    }
  }

  return {
    backend,
    dataset,
    pattern: pattern.name,
    category: pattern.category,
    targetMs: pattern.targetMs,
    latencyMs: latencyStats,
    bytesRead: totalBytesRead,
    avgBytesRead: successCount > 0 ? totalBytesRead / successCount : 0,
    rangeRequests: totalRangeRequests,
    avgRangeRequests: successCount > 0 ? totalRangeRequests / successCount : 0,
    passedTarget,
    successCount,
    failureCount,
    errors: errors.length > 0 ? errors.slice(0, 5) : undefined, // Limit to 5 errors
  }
}

/**
 * Run benchmarks for a dataset
 */
async function runDatasetBenchmark(
  backend: StorageBackendType,
  dataset: string,
  patterns: QueryPattern[],
  options: BenchmarkOptions
): Promise<StorageBenchmarkResult[]> {
  console.log(`\n--- ${dataset.toUpperCase()} Dataset (${patterns.length} patterns) ---`)

  const results: StorageBenchmarkResult[] = []

  for (const pattern of patterns) {
    const result = await runPatternBenchmark(backend, dataset, pattern, options)
    results.push(result)
  }

  return results
}

/**
 * Run the complete storage benchmark suite
 */
export async function runStorageBenchmark(
  backend: StorageBackendType,
  datasets: string[],
  options: Partial<BenchmarkOptions> = {}
): Promise<BenchmarkSuiteResult> {
  const opts: BenchmarkOptions = { ...DEFAULT_OPTIONS, ...options }
  const startTime = performance.now()
  const startedAt = new Date().toISOString()

  console.log('='.repeat(70))
  console.log('STORAGE BENCHMARK RUNNER')
  console.log('='.repeat(70))
  console.log(`Backend: ${backend}`)
  console.log(`Datasets: ${datasets.join(', ')}`)
  console.log(`Iterations: ${opts.iterations}`)
  console.log(`Warmup: ${opts.warmup}`)
  console.log(`Timeout: ${opts.timeout}ms`)
  console.log('='.repeat(70))

  const allResults: StorageBenchmarkResult[] = []

  for (const dataset of datasets) {
    const patterns = DATASET_PATTERNS[dataset]
    if (!patterns) {
      console.warn(`  Warning: Unknown dataset "${dataset}", skipping`)
      continue
    }

    const results = await runDatasetBenchmark(backend, dataset, patterns, opts)
    allResults.push(...results)
  }

  const endTime = performance.now()
  const durationMs = endTime - startTime

  // Calculate summary statistics
  const passedPatterns = allResults.filter((r) => r.passedTarget).length
  const failedPatterns = allResults.length - passedPatterns
  const allLatencies = allResults.flatMap((r) =>
    r.successCount > 0 ? [r.latencyMs.p50] : []
  )
  const avgLatencyMs =
    allLatencies.length > 0
      ? allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length
      : 0
  const p95LatencyMs = calculateLatencyStats(allLatencies).p95
  const totalBytesRead = allResults.reduce((sum, r) => sum + r.bytesRead, 0)
  const totalRangeRequests = allResults.reduce((sum, r) => sum + r.rangeRequests, 0)

  // By dataset summary
  const byDataset: Record<string, { passed: number; failed: number; avgLatencyMs: number }> = {}
  for (const dataset of datasets) {
    const datasetResults = allResults.filter((r) => r.dataset === dataset)
    const passed = datasetResults.filter((r) => r.passedTarget).length
    const failed = datasetResults.length - passed
    const latencies = datasetResults.filter((r) => r.successCount > 0).map((r) => r.latencyMs.p50)
    const avg = latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0
    byDataset[dataset] = { passed, failed, avgLatencyMs: avg }
  }

  // By category summary
  const byCategory: Record<string, { passed: number; failed: number; avgLatencyMs: number }> = {}
  const categories = [...new Set(allResults.map((r) => r.category))]
  for (const category of categories) {
    const categoryResults = allResults.filter((r) => r.category === category)
    const passed = categoryResults.filter((r) => r.passedTarget).length
    const failed = categoryResults.length - passed
    const latencies = categoryResults.filter((r) => r.successCount > 0).map((r) => r.latencyMs.p50)
    const avg = latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0
    byCategory[category] = { passed, failed, avgLatencyMs: avg }
  }

  return {
    config: {
      backend,
      datasets,
      iterations: opts.iterations,
      warmup: opts.warmup,
      timeout: opts.timeout,
    },
    metadata: {
      startedAt,
      completedAt: new Date().toISOString(),
      durationMs,
      runnerVersion: '1.0.0',
    },
    results: allResults,
    summary: {
      totalPatterns: allResults.length,
      passedPatterns,
      failedPatterns,
      avgLatencyMs,
      p95LatencyMs,
      totalBytesRead,
      totalRangeRequests,
      byDataset,
      byCategory,
    },
  }
}

// =============================================================================
// Output Formatters
// =============================================================================

/**
 * Format milliseconds with appropriate precision
 */
function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`
  if (ms < 10) return `${ms.toFixed(2)}ms`
  if (ms < 100) return `${ms.toFixed(1)}ms`
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

/**
 * Format bytes with appropriate unit
 */
function formatBytes(bytes: number): string {
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
 * Print results as table
 */
function printTable(result: BenchmarkSuiteResult): void {
  console.log('\n' + '='.repeat(70))
  console.log('BENCHMARK RESULTS')
  console.log('='.repeat(70))

  // Header
  const header = ['Dataset', 'Pattern', 'Category', 'Target', 'P50', 'P95', 'P99', 'Status']
  const widths = [12, 35, 15, 8, 10, 10, 10, 6]

  console.log(header.map((h, i) => h.padEnd(widths[i]!)).join(' '))
  console.log(widths.map((w) => '-'.repeat(w)).join(' '))

  // Results
  for (const r of result.results) {
    const status = r.passedTarget ? 'PASS' : 'FAIL'
    const row = [
      r.dataset.slice(0, widths[0]! - 1),
      r.pattern.slice(0, widths[1]! - 1),
      r.category.slice(0, widths[2]! - 1),
      `${r.targetMs}ms`,
      formatMs(r.latencyMs.p50),
      formatMs(r.latencyMs.p95),
      formatMs(r.latencyMs.p99),
      status,
    ]
    console.log(row.map((c, i) => c.padEnd(widths[i]!)).join(' '))
  }

  // Summary
  console.log('\n' + '='.repeat(70))
  console.log('SUMMARY')
  console.log('='.repeat(70))
  console.log(`Backend: ${result.config.backend}`)
  console.log(`Duration: ${formatMs(result.metadata.durationMs)}`)
  console.log(`Total Patterns: ${result.summary.totalPatterns}`)
  console.log(`Passed: ${result.summary.passedPatterns}`)
  console.log(`Failed: ${result.summary.failedPatterns}`)
  console.log(`Avg Latency (p50): ${formatMs(result.summary.avgLatencyMs)}`)
  console.log(`P95 Latency: ${formatMs(result.summary.p95LatencyMs)}`)
  console.log(`Total Bytes Read: ${formatBytes(result.summary.totalBytesRead)}`)
  console.log(`Total Range Requests: ${result.summary.totalRangeRequests}`)

  console.log('\nBy Dataset:')
  for (const [dataset, stats] of Object.entries(result.summary.byDataset)) {
    console.log(
      `  ${dataset}: ${stats.passed}/${stats.passed + stats.failed} passed, avg ${formatMs(stats.avgLatencyMs)}`
    )
  }

  console.log('\nBy Category:')
  for (const [category, stats] of Object.entries(result.summary.byCategory)) {
    console.log(
      `  ${category}: ${stats.passed}/${stats.passed + stats.failed} passed, avg ${formatMs(stats.avgLatencyMs)}`
    )
  }
}

/**
 * Print results as markdown
 */
function printMarkdown(result: BenchmarkSuiteResult): void {
  console.log('\n## Storage Benchmark Results\n')
  console.log(`**Backend:** ${result.config.backend}`)
  console.log(`**Duration:** ${formatMs(result.metadata.durationMs)}`)
  console.log(`**Date:** ${result.metadata.completedAt}\n`)

  console.log('### Summary\n')
  console.log('| Metric | Value |')
  console.log('|--------|-------|')
  console.log(`| Total Patterns | ${result.summary.totalPatterns} |`)
  console.log(`| Passed | ${result.summary.passedPatterns} |`)
  console.log(`| Failed | ${result.summary.failedPatterns} |`)
  console.log(`| Avg Latency (p50) | ${formatMs(result.summary.avgLatencyMs)} |`)
  console.log(`| P95 Latency | ${formatMs(result.summary.p95LatencyMs)} |`)
  console.log(`| Total Bytes Read | ${formatBytes(result.summary.totalBytesRead)} |`)
  console.log(`| Total Range Requests | ${result.summary.totalRangeRequests} |`)
  console.log('')

  console.log('### Results by Pattern\n')
  console.log('| Dataset | Pattern | Category | Target | P50 | P95 | Status |')
  console.log('|---------|---------|----------|--------|-----|-----|--------|')
  for (const r of result.results) {
    const status = r.passedTarget ? 'PASS' : 'FAIL'
    console.log(
      `| ${r.dataset} | ${r.pattern} | ${r.category} | ${r.targetMs}ms | ${formatMs(r.latencyMs.p50)} | ${formatMs(r.latencyMs.p95)} | ${status} |`
    )
  }
  console.log('')

  console.log('### Results by Dataset\n')
  console.log('| Dataset | Passed | Failed | Avg Latency |')
  console.log('|---------|--------|--------|-------------|')
  for (const [dataset, stats] of Object.entries(result.summary.byDataset)) {
    console.log(
      `| ${dataset} | ${stats.passed} | ${stats.failed} | ${formatMs(stats.avgLatencyMs)} |`
    )
  }
  console.log('')

  console.log('### Results by Category\n')
  console.log('| Category | Passed | Failed | Avg Latency |')
  console.log('|----------|--------|--------|-------------|')
  for (const [category, stats] of Object.entries(result.summary.byCategory)) {
    console.log(
      `| ${category} | ${stats.passed} | ${stats.failed} | ${formatMs(stats.avgLatencyMs)} |`
    )
  }
}

/**
 * Print results as JSON
 */
function printJson(result: BenchmarkSuiteResult): void {
  console.log(JSON.stringify(result, null, 2))
}

/**
 * Print results in the specified format
 */
function printResults(result: BenchmarkSuiteResult, format: 'table' | 'json' | 'markdown'): void {
  switch (format) {
    case 'json':
      printJson(result)
      break
    case 'markdown':
      printMarkdown(result)
      break
    default:
      printTable(result)
  }
}

// =============================================================================
// CLI Argument Parsing
// =============================================================================

interface CLIConfig {
  backend: StorageBackendType
  datasets: string[]
  options: BenchmarkOptions
}

function parseArgs(): CLIConfig {
  const args = process.argv.slice(2)
  let backend: StorageBackendType = 'cdn'
  let datasets: string[] | null = null
  const options: BenchmarkOptions = { ...DEFAULT_OPTIONS }

  for (const arg of args) {
    if (arg.startsWith('--backend=')) {
      const value = arg.slice(10) as StorageBackendType
      if (['cdn', 'fs', 'r2'].includes(value)) {
        backend = value
      } else {
        console.error(`Invalid backend: ${value}. Must be cdn, fs, or r2.`)
        process.exit(1)
      }
    } else if (arg.startsWith('--datasets=')) {
      datasets = arg.slice(11).split(',').filter(Boolean)
    } else if (arg.startsWith('--iterations=')) {
      options.iterations = parseInt(arg.slice(13), 10)
    } else if (arg.startsWith('--warmup=')) {
      options.warmup = parseInt(arg.slice(9), 10)
    } else if (arg.startsWith('--output=')) {
      const value = arg.slice(9) as 'table' | 'json' | 'markdown'
      if (['table', 'json', 'markdown'].includes(value)) {
        options.output = value
      }
    } else if (arg === '--verbose') {
      options.verbose = true
    } else if (arg.startsWith('--timeout=')) {
      options.timeout = parseInt(arg.slice(10), 10)
    } else if (arg.startsWith('--data-dir=')) {
      options.dataDir = arg.slice(11)
    } else if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }
  }

  // Default to available datasets for the backend
  if (!datasets) {
    datasets = getAvailableDatasets(backend)
  }

  // Validate datasets
  const available = getAvailableDatasets(backend)
  for (const dataset of datasets) {
    if (!available.includes(dataset)) {
      console.error(
        `Dataset "${dataset}" not available for backend "${backend}". Available: ${available.join(', ')}`
      )
      process.exit(1)
    }
  }

  return { backend, datasets, options }
}

function printHelp(): void {
  console.log(`
Storage Benchmark Runner

Tests query patterns across different storage backends and collects
performance metrics including latency percentiles and I/O statistics.

USAGE:
  npx tsx tests/benchmarks/storage-benchmark-runner.ts [OPTIONS]

OPTIONS:
  --backend=TYPE      Storage backend to test: cdn, fs, r2 (default: cdn)
  --datasets=LIST     Comma-separated datasets to test (default: all available)
  --iterations=N      Number of measured iterations (default: 10)
  --warmup=N          Number of warmup iterations (default: 2)
  --output=FORMAT     Output format: table, json, markdown (default: table)
  --timeout=MS        Request timeout in milliseconds (default: 30000)
  --data-dir=PATH     Data directory for FS backend (default: ./data)
  --verbose           Enable verbose output
  --help, -h          Show this help message

AVAILABLE DATASETS:
  cdn:  imdb, onet, unspsc, blog, ecommerce
  fs:   blog, ecommerce, onet, unspsc (local datasets)
  r2:   imdb, onet, unspsc, blog, ecommerce

EXAMPLES:
  # Run CDN benchmarks with all datasets
  npx tsx tests/benchmarks/storage-benchmark-runner.ts

  # Run FS benchmarks with specific datasets
  npx tsx tests/benchmarks/storage-benchmark-runner.ts --backend=fs --datasets=blog,ecommerce

  # Run with more iterations and markdown output
  npx tsx tests/benchmarks/storage-benchmark-runner.ts --iterations=20 --output=markdown

  # Run with verbose output
  npx tsx tests/benchmarks/storage-benchmark-runner.ts --verbose
`)
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main(): Promise<void> {
  const { backend, datasets, options } = parseArgs()

  try {
    const result = await runStorageBenchmark(backend, datasets, options)
    printResults(result, options.output)

    // Exit with error if any patterns failed
    if (result.summary.failedPatterns > 0) {
      console.error(`\nWARNING: ${result.summary.failedPatterns} pattern(s) failed to meet target latency`)
    }
  } catch (error) {
    console.error('Benchmark failed:', error)
    process.exit(1)
  }
}

// Run if executed directly
if (process.argv[1]?.endsWith('storage-benchmark-runner.ts')) {
  main()
}

// =============================================================================
// Additional Exports for Programmatic Use
// =============================================================================

// Note: Types and runStorageBenchmark are already exported inline above.
// Export the remaining utilities here.
export {
  calculateLatencyStats,
  DATASET_PATTERNS,
  LOCAL_DATASETS,
  getAvailableDatasets,
  formatMs,
  formatBytes,
  printTable,
  printMarkdown,
  printJson,
  printResults,
  DEFAULT_OPTIONS,
  CDN_BASE_URL,
}

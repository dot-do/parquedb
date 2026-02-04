/**
 * Runtime Detection and Unified Benchmark Interface
 *
 * Provides runtime detection and a unified interface for running benchmarks
 * across Browser, Worker, and Node.js environments.
 *
 * @module runtimes
 */

import {
  type RuntimeName,
  type BackendName,
  type DatasetName,
  detectRuntime,
  getRuntimeConfig,
  getValidCombinations,
  getValidDatasets,
  getValidBackends,
  isValidCombination,
} from '../runtime-configs'

import {
  type BenchmarkSuiteResult,
  type StorageBenchmarkResult,
  type BenchmarkOptions,
  type StorageBackendType,
  runStorageBenchmark,
  calculateLatencyStats,
  formatMs,
  formatBytes,
} from '../storage-benchmark-runner'

// =============================================================================
// Types
// =============================================================================

/**
 * Unified benchmark result across all runtimes
 */
export interface UnifiedBenchmarkResult {
  /** Runtime where benchmark was executed */
  runtime: RuntimeName
  /** User agent or runtime version info */
  userAgent: string
  /** Benchmark suite results */
  suiteResult: BenchmarkSuiteResult
  /** Runtime-specific metadata */
  runtimeMetadata: RuntimeMetadata
}

/**
 * Runtime-specific metadata collected during benchmark
 */
export interface RuntimeMetadata {
  /** Runtime name */
  runtime: RuntimeName
  /** Timing precision available (ms resolution) */
  timingPrecision: 'high-resolution' | 'standard'
  /** Whether fetch supports streaming */
  supportsStreaming: boolean
  /** Memory usage if available */
  memoryUsage?: {
    heapUsed: number
    heapTotal: number
    external?: number
  }
  /** Environment info */
  environment: Record<string, string | undefined>
}

/**
 * Options for running benchmarks in the current runtime
 */
export interface RuntimeBenchmarkOptions extends Partial<BenchmarkOptions> {
  /** Specific backends to test (defaults to all valid for runtime) */
  backends?: BackendName[]
  /** Specific datasets to test (defaults to all valid for runtime) */
  datasets?: DatasetName[]
  /** Whether to include warmup in timing (default: false) */
  includeWarmupTiming?: boolean
  /** Callback for progress updates */
  onProgress?: (progress: BenchmarkProgress) => void
}

/**
 * Progress update during benchmark execution
 */
export interface BenchmarkProgress {
  /** Current phase */
  phase: 'warmup' | 'measurement' | 'complete'
  /** Current backend being tested */
  backend: BackendName
  /** Current dataset being tested */
  dataset: DatasetName
  /** Current pattern name */
  pattern?: string
  /** Iteration number */
  iteration: number
  /** Total iterations */
  totalIterations: number
  /** Elapsed time in ms */
  elapsedMs: number
}

// =============================================================================
// Runtime Detection (re-export with enhancements)
// =============================================================================

export { detectRuntime }

/**
 * Enhanced runtime detection with additional checks
 *
 * @returns Detected runtime name with confidence level
 */
export function detectRuntimeWithDetails(): {
  runtime: RuntimeName
  confidence: 'high' | 'medium' | 'low'
  indicators: string[]
} {
  const indicators: string[] = []

  // Check for Cloudflare Workers
  if (typeof globalThis.caches !== 'undefined') {
    indicators.push('globalThis.caches exists')
  }
  if (typeof (globalThis as Record<string, unknown>).HTMLRewriter !== 'undefined') {
    indicators.push('HTMLRewriter exists')
  }
  if (typeof (globalThis as Record<string, unknown>).R2Bucket !== 'undefined') {
    indicators.push('R2Bucket exists')
  }

  // Check for browser
  if (typeof window !== 'undefined') {
    indicators.push('window exists')
  }
  if (typeof document !== 'undefined') {
    indicators.push('document exists')
  }
  if (typeof navigator !== 'undefined') {
    indicators.push('navigator exists')
  }

  // Check for Node.js
  if (typeof process !== 'undefined' && process.versions?.node) {
    indicators.push(`Node.js ${process.versions.node}`)
  }

  const runtime = detectRuntime()
  let confidence: 'high' | 'medium' | 'low' = 'high'

  // Determine confidence based on number of indicators
  if (runtime === 'worker' && indicators.length < 2) {
    confidence = 'medium'
  }
  if (runtime === 'browser' && indicators.length < 2) {
    confidence = 'medium'
  }
  if (runtime === 'node' && !indicators.some((i) => i.startsWith('Node.js'))) {
    confidence = 'low'
  }

  return { runtime, confidence, indicators }
}

// =============================================================================
// Runtime Metadata Collection
// =============================================================================

/**
 * Collect metadata about the current runtime environment
 */
export function collectRuntimeMetadata(): RuntimeMetadata {
  const runtime = detectRuntime()

  const metadata: RuntimeMetadata = {
    runtime,
    timingPrecision: hasHighResolutionTiming() ? 'high-resolution' : 'standard',
    supportsStreaming: supportsStreamingFetch(),
    environment: {},
  }

  // Collect memory usage in Node.js
  if (runtime === 'node' && typeof process !== 'undefined' && process.memoryUsage) {
    const mem = process.memoryUsage()
    metadata.memoryUsage = {
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
      external: mem.external,
    }
  }

  // Collect environment info
  if (runtime === 'node' && typeof process !== 'undefined') {
    metadata.environment = {
      NODE_ENV: process.env.NODE_ENV,
      NODE_VERSION: process.versions?.node,
      V8_VERSION: process.versions?.v8,
      PLATFORM: process.platform,
      ARCH: process.arch,
    }
  } else if (runtime === 'browser' && typeof navigator !== 'undefined') {
    metadata.environment = {
      USER_AGENT: navigator.userAgent,
      PLATFORM: navigator.platform,
      LANGUAGE: navigator.language,
    }
  } else if (runtime === 'worker') {
    metadata.environment = {
      RUNTIME: 'cloudflare-workers',
      // CF runtime info would be available via request.cf
    }
  }

  return metadata
}

/**
 * Check if high-resolution timing is available
 */
function hasHighResolutionTiming(): boolean {
  if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
    // Check resolution by comparing two calls
    const t1 = performance.now()
    const t2 = performance.now()
    // High resolution if we can detect sub-millisecond differences
    return t2 - t1 < 0.1
  }
  return false
}

/**
 * Check if fetch supports streaming responses
 */
function supportsStreamingFetch(): boolean {
  if (typeof Response !== 'undefined') {
    try {
      const response = new Response('test')
      return typeof response.body?.getReader === 'function'
    } catch {
      return false
    }
  }
  return false
}

// =============================================================================
// Unified Benchmark Runner
// =============================================================================

/**
 * Run benchmarks for the current runtime with automatic configuration
 *
 * Automatically detects the runtime environment and runs only valid
 * backend/dataset combinations.
 *
 * @param options - Benchmark options
 * @returns Promise resolving to benchmark results
 *
 * @example
 * ```typescript
 * const results = await runBenchmarksForCurrentRuntime({
 *   iterations: 10,
 *   warmup: 2,
 *   verbose: true,
 * })
 * console.log(results.suiteResult.summary)
 * ```
 */
export async function runBenchmarksForCurrentRuntime(
  options: RuntimeBenchmarkOptions = {}
): Promise<UnifiedBenchmarkResult[]> {
  const runtime = detectRuntime()
  const config = getRuntimeConfig(runtime)

  if (!config) {
    throw new Error(`No configuration found for runtime: ${runtime}`)
  }

  const results: UnifiedBenchmarkResult[] = []
  const runtimeMetadata = collectRuntimeMetadata()
  const userAgent = getUserAgent()

  // Determine which backends and datasets to test
  const backends = options.backends ?? config.supportedBackends
  const requestedDatasets = options.datasets ?? config.supportedDatasets

  for (const backend of backends) {
    // Validate backend for this runtime
    if (!config.supportedBackends.includes(backend)) {
      console.warn(
        `Skipping backend "${backend}" - not supported in ${runtime} runtime`
      )
      continue
    }

    // Get valid datasets for this backend
    const validDatasets = getValidDatasets(runtime, backend)
    const datasets = requestedDatasets.filter((d) => validDatasets.includes(d))

    if (datasets.length === 0) {
      console.warn(
        `Skipping backend "${backend}" - no valid datasets for ${runtime} runtime`
      )
      continue
    }

    // Map BackendName to StorageBackendType
    const storageBackend = mapBackendToStorageType(backend)
    if (!storageBackend) {
      console.warn(
        `Skipping backend "${backend}" - not yet implemented in storage runner`
      )
      continue
    }

    // Run benchmark for this backend
    const benchmarkOptions: Partial<BenchmarkOptions> = {
      iterations: options.iterations ?? 10,
      warmup: options.warmup ?? 2,
      timeout: options.timeout ?? 30000,
      verbose: options.verbose ?? false,
      output: options.output ?? 'table',
      dataDir: options.dataDir ?? './data',
    }

    const suiteResult = await runStorageBenchmark(
      storageBackend,
      datasets,
      benchmarkOptions
    )

    results.push({
      runtime,
      userAgent,
      suiteResult,
      runtimeMetadata,
    })
  }

  return results
}

/**
 * Map BackendName to StorageBackendType for the storage runner
 */
function mapBackendToStorageType(backend: BackendName): StorageBackendType | null {
  switch (backend) {
    case 'cdn':
      return 'cdn'
    case 'fs':
      return 'fs'
    case 'r2':
      return 'r2'
    case 'iceberg':
    case 'delta':
      // These are not yet implemented in the storage runner
      return null
    default:
      return null
  }
}

/**
 * Get user agent string for the current runtime
 */
function getUserAgent(): string {
  const runtime = detectRuntime()

  if (runtime === 'browser' && typeof navigator !== 'undefined') {
    return navigator.userAgent
  }

  if (runtime === 'node' && typeof process !== 'undefined') {
    return `Node.js/${process.versions?.node} (${process.platform}; ${process.arch})`
  }

  if (runtime === 'worker') {
    return 'Cloudflare-Workers/1.0'
  }

  return 'Unknown/1.0'
}

// =============================================================================
// Result Aggregation
// =============================================================================

/**
 * Aggregate results from multiple runtimes
 */
export function aggregateResults(
  results: UnifiedBenchmarkResult[]
): AggregatedBenchmarkResult {
  const byRuntime: Record<RuntimeName, BenchmarkSuiteResult[]> = {
    browser: [],
    worker: [],
    node: [],
  }

  for (const result of results) {
    byRuntime[result.runtime].push(result.suiteResult)
  }

  const comparison: RuntimeComparison[] = []

  // Compare each runtime pair
  const runtimes = Object.keys(byRuntime).filter(
    (r) => byRuntime[r as RuntimeName].length > 0
  ) as RuntimeName[]

  for (let i = 0; i < runtimes.length; i++) {
    for (let j = i + 1; j < runtimes.length; j++) {
      const r1 = runtimes[i]!
      const r2 = runtimes[j]!

      const r1Results = byRuntime[r1]
      const r2Results = byRuntime[r2]

      if (r1Results.length > 0 && r2Results.length > 0) {
        comparison.push(compareRuntimes(r1, r1Results, r2, r2Results))
      }
    }
  }

  return {
    totalResults: results.length,
    byRuntime,
    comparison,
    fastest: findFastestRuntime(byRuntime),
    slowest: findSlowestRuntime(byRuntime),
  }
}

/**
 * Aggregated benchmark results across runtimes
 */
export interface AggregatedBenchmarkResult {
  totalResults: number
  byRuntime: Record<RuntimeName, BenchmarkSuiteResult[]>
  comparison: RuntimeComparison[]
  fastest: RuntimeName | null
  slowest: RuntimeName | null
}

/**
 * Comparison between two runtimes
 */
export interface RuntimeComparison {
  runtime1: RuntimeName
  runtime2: RuntimeName
  avgLatencyDiff: number
  p95LatencyDiff: number
  faster: RuntimeName
  speedupFactor: number
}

/**
 * Compare two runtimes' benchmark results
 */
function compareRuntimes(
  r1: RuntimeName,
  r1Results: BenchmarkSuiteResult[],
  r2: RuntimeName,
  r2Results: BenchmarkSuiteResult[]
): RuntimeComparison {
  const r1AvgLatency =
    r1Results.reduce((sum, r) => sum + r.summary.avgLatencyMs, 0) / r1Results.length
  const r2AvgLatency =
    r2Results.reduce((sum, r) => sum + r.summary.avgLatencyMs, 0) / r2Results.length

  const r1P95Latency =
    r1Results.reduce((sum, r) => sum + r.summary.p95LatencyMs, 0) / r1Results.length
  const r2P95Latency =
    r2Results.reduce((sum, r) => sum + r.summary.p95LatencyMs, 0) / r2Results.length

  const faster = r1AvgLatency <= r2AvgLatency ? r1 : r2
  const speedupFactor =
    faster === r1 ? r2AvgLatency / r1AvgLatency : r1AvgLatency / r2AvgLatency

  return {
    runtime1: r1,
    runtime2: r2,
    avgLatencyDiff: Math.abs(r1AvgLatency - r2AvgLatency),
    p95LatencyDiff: Math.abs(r1P95Latency - r2P95Latency),
    faster,
    speedupFactor,
  }
}

/**
 * Find the fastest runtime by average latency
 */
function findFastestRuntime(
  byRuntime: Record<RuntimeName, BenchmarkSuiteResult[]>
): RuntimeName | null {
  let fastest: RuntimeName | null = null
  let fastestAvg = Infinity

  for (const [runtime, results] of Object.entries(byRuntime) as [
    RuntimeName,
    BenchmarkSuiteResult[],
  ][]) {
    if (results.length === 0) continue

    const avg =
      results.reduce((sum, r) => sum + r.summary.avgLatencyMs, 0) / results.length
    if (avg < fastestAvg) {
      fastestAvg = avg
      fastest = runtime
    }
  }

  return fastest
}

/**
 * Find the slowest runtime by average latency
 */
function findSlowestRuntime(
  byRuntime: Record<RuntimeName, BenchmarkSuiteResult[]>
): RuntimeName | null {
  let slowest: RuntimeName | null = null
  let slowestAvg = -Infinity

  for (const [runtime, results] of Object.entries(byRuntime) as [
    RuntimeName,
    BenchmarkSuiteResult[],
  ][]) {
    if (results.length === 0) continue

    const avg =
      results.reduce((sum, r) => sum + r.summary.avgLatencyMs, 0) / results.length
    if (avg > slowestAvg) {
      slowestAvg = avg
      slowest = runtime
    }
  }

  return slowest
}

// =============================================================================
// Standard Output Formatters
// =============================================================================

/**
 * Format unified results as JSON
 */
export function formatResultsAsJson(results: UnifiedBenchmarkResult[]): string {
  return JSON.stringify(results, null, 2)
}

/**
 * Format unified results for console output
 */
export function formatResultsForConsole(results: UnifiedBenchmarkResult[]): string {
  const lines: string[] = []

  for (const result of results) {
    lines.push('='.repeat(70))
    lines.push(`RUNTIME: ${result.runtime.toUpperCase()}`)
    lines.push(`User Agent: ${result.userAgent}`)
    lines.push(`Timing Precision: ${result.runtimeMetadata.timingPrecision}`)
    lines.push('='.repeat(70))

    const summary = result.suiteResult.summary
    lines.push(`Total Patterns: ${summary.totalPatterns}`)
    lines.push(`Passed: ${summary.passedPatterns}`)
    lines.push(`Failed: ${summary.failedPatterns}`)
    lines.push(`Avg Latency (p50): ${formatMs(summary.avgLatencyMs)}`)
    lines.push(`P95 Latency: ${formatMs(summary.p95LatencyMs)}`)
    lines.push(`Total Bytes Read: ${formatBytes(summary.totalBytesRead)}`)
    lines.push('')
  }

  return lines.join('\n')
}

// =============================================================================
// Re-exports
// =============================================================================

export {
  type RuntimeName,
  type BackendName,
  type DatasetName,
  type BenchmarkSuiteResult,
  type StorageBenchmarkResult,
  type BenchmarkOptions,
  getRuntimeConfig,
  getValidCombinations,
  getValidDatasets,
  getValidBackends,
  isValidCombination,
  formatMs,
  formatBytes,
}

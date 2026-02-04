/**
 * Cloudflare Worker Benchmark Harness
 *
 * Designed to run ParqueDB benchmarks in Cloudflare Workers environment.
 * Supports CDN, R2, Iceberg, and Delta backends.
 *
 * Deployment:
 *   This file is designed to be deployed as a Cloudflare Worker.
 *   Configure wrangler.toml with appropriate R2 bindings.
 *
 * Usage (HTTP API):
 *   GET /benchmark?backend=cdn&datasets=imdb,onet&iterations=10
 *   GET /benchmark?backend=r2&datasets=blog&warmup=2&verbose=true
 *   GET /results?format=json
 *   GET /health
 *
 * Query Parameters:
 *   backend     - Storage backend: cdn, r2, iceberg, delta (default: cdn)
 *   datasets    - Comma-separated datasets (default: all valid for backend)
 *   iterations  - Number of test iterations (default: 10)
 *   warmup      - Warmup iterations (default: 2)
 *   verbose     - Enable verbose logging (default: false)
 *   format      - Output format: json, table, markdown (default: json)
 *
 * @module runtimes/worker-harness
 */

/// <reference types="@cloudflare/workers-types" />

import {
  type BackendName,
  type DatasetName,
  runtimeConfigs,
  getValidDatasets,
  isValidCombination,
} from '../runtime-configs'

// Import patterns directly since we're in Workers environment
import { imdbPatterns } from '../patterns/imdb'
import { onetPatterns } from '../patterns/onet'
import { unspscPatterns } from '../patterns/unspsc'
import { blogPatterns } from '../patterns/blog'
import { ecommercePatterns } from '../patterns/ecommerce'

// =============================================================================
// Cloudflare Workers Types (inline for portability)
// =============================================================================

// These types are provided for standalone use without @cloudflare/workers-types
interface R2BucketBinding {
  head(key: string): Promise<R2Object | null>
  get(key: string): Promise<R2ObjectBody | null>
  put(key: string, value: ArrayBuffer | string): Promise<R2Object>
  delete(key: string): Promise<void>
  list(options?: { prefix?: string; limit?: number }): Promise<R2Objects>
}

interface R2Object {
  key: string
  size: number
  etag: string
}

interface R2ObjectBody extends R2Object {
  body: ReadableStream
  arrayBuffer(): Promise<ArrayBuffer>
  text(): Promise<string>
}

interface R2Objects {
  objects: R2Object[]
  truncated: boolean
}

interface KVNamespaceBinding {
  get(key: string): Promise<string | null>
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>
  delete(key: string): Promise<void>
  list(options?: { prefix?: string; limit?: number }): Promise<{ keys: { name: string }[] }>
}

interface CfProperties {
  colo?: string
  country?: string
  region?: string
  city?: string
  timezone?: string
}

// =============================================================================
// Types
// =============================================================================

/**
 * Cloudflare Worker environment bindings
 */
export interface Env {
  /** R2 bucket binding for direct R2 access */
  PARQUEDB_BUCKET?: R2BucketBinding
  /** KV namespace for caching benchmark results */
  BENCHMARK_RESULTS?: KVNamespaceBinding
  /** Environment name */
  ENVIRONMENT?: string
  /** Base URL override */
  BASE_URL?: string
}

/**
 * Worker benchmark configuration
 */
interface WorkerBenchmarkConfig {
  backend: BackendName
  datasets: DatasetName[]
  iterations: number
  warmup: number
  verbose: boolean
  format: 'json' | 'table' | 'markdown'
}

/**
 * Latency statistics
 */
interface LatencyStats {
  min: number
  max: number
  mean: number
  p50: number
  p95: number
  p99: number
  stdDev: number
}

/**
 * Pattern benchmark result
 */
interface PatternResult {
  pattern: string
  category: string
  targetMs: number
  latencyMs: LatencyStats
  passedTarget: boolean
  successCount: number
  failureCount: number
  errors?: string[]
}

/**
 * Dataset benchmark result
 */
interface DatasetResult {
  dataset: string
  patterns: PatternResult[]
  summary: {
    totalPatterns: number
    passedPatterns: number
    failedPatterns: number
    avgLatencyMs: number
    p95LatencyMs: number
  }
}

/**
 * Complete benchmark result
 */
interface WorkerBenchmarkResult {
  config: WorkerBenchmarkConfig
  runtime: 'worker'
  runtimeInfo: {
    region?: string
    colo?: string
    country?: string
  }
  startedAt: string
  completedAt: string
  durationMs: number
  results: DatasetResult[]
  summary: {
    totalPatterns: number
    passedPatterns: number
    failedPatterns: number
    avgLatencyMs: number
    p95LatencyMs: number
    totalBytesRead: number
  }
}

// =============================================================================
// Pattern Collections
// =============================================================================

const DATASET_PATTERNS: Record<DatasetName, Array<{
  name: string
  category: string
  targetMs: number
  query: (baseUrl?: string) => Promise<Response>
}>> = {
  imdb: imdbPatterns,
  onet: onetPatterns,
  unspsc: unspscPatterns,
  blog: blogPatterns,
  ecommerce: ecommercePatterns,
}

// =============================================================================
// Statistics Utilities
// =============================================================================

/**
 * Calculate latency statistics from samples
 */
function calculateLatencyStats(samples: number[]): LatencyStats {
  if (samples.length === 0) {
    return { min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0, stdDev: 0 }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = sorted.length
  const mean = sorted.reduce((a, b) => a + b, 0) / n

  const variance = sorted.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

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
// Benchmark Execution
// =============================================================================

/**
 * Run a single pattern benchmark
 */
async function runPatternBenchmark(
  pattern: { name: string; category: string; targetMs: number; query: (baseUrl?: string) => Promise<Response> },
  config: WorkerBenchmarkConfig,
  baseUrl?: string
): Promise<PatternResult> {
  const latencies: number[] = []
  const errors: string[] = []
  let successCount = 0
  let failureCount = 0

  // Warmup iterations
  for (let i = 0; i < config.warmup; i++) {
    try {
      const response = await pattern.query(baseUrl)
      await response.arrayBuffer()
    } catch {
      // Ignore warmup errors
    }
  }

  // Measured iterations
  for (let i = 0; i < config.iterations; i++) {
    try {
      const start = performance.now()
      const response = await pattern.query(baseUrl)
      const latencyMs = performance.now() - start

      if (response.ok) {
        await response.arrayBuffer()
        latencies.push(latencyMs)
        successCount++
      } else {
        latencies.push(latencyMs)
        failureCount++
        try {
          const text = await response.text()
          errors.push(`HTTP ${response.status}: ${text.slice(0, 100)}`)
        } catch {
          errors.push(`HTTP ${response.status}`)
        }
      }
    } catch (error) {
      failureCount++
      errors.push(error instanceof Error ? error.message : String(error))
    }
  }

  const latencyStats = calculateLatencyStats(latencies)
  const passedTarget = successCount > 0 && latencyStats.p50 <= pattern.targetMs

  return {
    pattern: pattern.name,
    category: pattern.category,
    targetMs: pattern.targetMs,
    latencyMs: latencyStats,
    passedTarget,
    successCount,
    failureCount,
    errors: errors.length > 0 ? errors.slice(0, 3) : undefined,
  }
}

/**
 * Run benchmarks for a dataset
 */
async function runDatasetBenchmark(
  dataset: DatasetName,
  config: WorkerBenchmarkConfig,
  baseUrl?: string
): Promise<DatasetResult> {
  const patterns = DATASET_PATTERNS[dataset] || []
  const patternResults: PatternResult[] = []

  for (const pattern of patterns) {
    const result = await runPatternBenchmark(pattern, config, baseUrl)
    patternResults.push(result)
  }

  // Calculate summary
  const passedPatterns = patternResults.filter((r) => r.passedTarget).length
  const allLatencies = patternResults
    .filter((r) => r.successCount > 0)
    .map((r) => r.latencyMs.p50)

  const avgLatencyMs = allLatencies.length > 0
    ? allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length
    : 0

  const p95LatencyMs = calculateLatencyStats(allLatencies).p95

  return {
    dataset,
    patterns: patternResults,
    summary: {
      totalPatterns: patternResults.length,
      passedPatterns,
      failedPatterns: patternResults.length - passedPatterns,
      avgLatencyMs,
      p95LatencyMs,
    },
  }
}

/**
 * Run the complete benchmark suite
 */
async function runBenchmarkSuite(
  config: WorkerBenchmarkConfig,
  env: Env,
  request: Request
): Promise<WorkerBenchmarkResult> {
  const startedAt = new Date().toISOString()
  const startTime = performance.now()

  // Get runtime info from Cloudflare headers
  const cf = (request as Request & { cf?: CfProperties }).cf
  const runtimeInfo = {
    region: cf?.region,
    colo: cf?.colo,
    country: cf?.country,
  }

  // Determine base URL
  const baseUrl = env.BASE_URL || new URL(request.url).origin

  const datasetResults: DatasetResult[] = []

  for (const dataset of config.datasets) {
    // Validate combination
    if (!isValidCombination('worker', config.backend, dataset)) {
      console.warn(`Skipping invalid combination: worker/${config.backend}/${dataset}`)
      continue
    }

    const result = await runDatasetBenchmark(dataset, config, baseUrl)
    datasetResults.push(result)
  }

  const completedAt = new Date().toISOString()
  const durationMs = performance.now() - startTime

  // Calculate overall summary
  const totalPatterns = datasetResults.reduce((sum, r) => sum + r.summary.totalPatterns, 0)
  const passedPatterns = datasetResults.reduce((sum, r) => sum + r.summary.passedPatterns, 0)
  const failedPatterns = totalPatterns - passedPatterns

  const allLatencies = datasetResults
    .flatMap((r) => r.patterns.filter((p) => p.successCount > 0).map((p) => p.latencyMs.p50))

  const avgLatencyMs = allLatencies.length > 0
    ? allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length
    : 0

  return {
    config,
    runtime: 'worker',
    runtimeInfo,
    startedAt,
    completedAt,
    durationMs,
    results: datasetResults,
    summary: {
      totalPatterns,
      passedPatterns,
      failedPatterns,
      avgLatencyMs,
      p95LatencyMs: calculateLatencyStats(allLatencies).p95,
      totalBytesRead: 0, // Would need to track this during requests
    },
  }
}

// =============================================================================
// Request Handling
// =============================================================================

/**
 * Parse benchmark configuration from request
 */
function parseConfig(url: URL): WorkerBenchmarkConfig {
  const workerConfig = runtimeConfigs.find((c) => c.name === 'worker')!

  const backend = (url.searchParams.get('backend') || 'cdn') as BackendName
  const datasetsParam = url.searchParams.get('datasets')
  const datasets = datasetsParam
    ? (datasetsParam.split(',').filter(Boolean) as DatasetName[])
    : workerConfig.supportedDatasets

  return {
    backend,
    datasets,
    iterations: parseInt(url.searchParams.get('iterations') || '10', 10),
    warmup: parseInt(url.searchParams.get('warmup') || '2', 10),
    verbose: url.searchParams.get('verbose') === 'true',
    format: (url.searchParams.get('format') || 'json') as 'json' | 'table' | 'markdown',
  }
}

/**
 * Format result for response
 */
function formatResult(result: WorkerBenchmarkResult, format: 'json' | 'table' | 'markdown'): string {
  if (format === 'json') {
    return JSON.stringify(result, null, 2)
  }

  if (format === 'markdown') {
    return formatAsMarkdown(result)
  }

  return formatAsTable(result)
}

/**
 * Format result as markdown
 */
function formatAsMarkdown(result: WorkerBenchmarkResult): string {
  const lines: string[] = []

  lines.push('# Worker Benchmark Results\n')
  lines.push(`**Runtime:** ${result.runtime}`)
  lines.push(`**Backend:** ${result.config.backend}`)
  lines.push(`**Region:** ${result.runtimeInfo.colo || 'unknown'}`)
  lines.push(`**Duration:** ${result.durationMs.toFixed(1)}ms`)
  lines.push(`**Date:** ${result.completedAt}\n`)

  lines.push('## Summary\n')
  lines.push('| Metric | Value |')
  lines.push('|--------|-------|')
  lines.push(`| Total Patterns | ${result.summary.totalPatterns} |`)
  lines.push(`| Passed | ${result.summary.passedPatterns} |`)
  lines.push(`| Failed | ${result.summary.failedPatterns} |`)
  lines.push(`| Avg Latency | ${result.summary.avgLatencyMs.toFixed(1)}ms |`)
  lines.push(`| P95 Latency | ${result.summary.p95LatencyMs.toFixed(1)}ms |`)
  lines.push('')

  for (const datasetResult of result.results) {
    lines.push(`## ${datasetResult.dataset.toUpperCase()}\n`)
    lines.push('| Pattern | Category | Target | P50 | P95 | Status |')
    lines.push('|---------|----------|--------|-----|-----|--------|')

    for (const pattern of datasetResult.patterns) {
      const status = pattern.passedTarget ? 'PASS' : 'FAIL'
      lines.push(
        `| ${pattern.pattern} | ${pattern.category} | ${pattern.targetMs}ms | ${pattern.latencyMs.p50.toFixed(1)}ms | ${pattern.latencyMs.p95.toFixed(1)}ms | ${status} |`
      )
    }
    lines.push('')
  }

  return lines.join('\n')
}

/**
 * Format result as plain text table
 */
function formatAsTable(result: WorkerBenchmarkResult): string {
  const lines: string[] = []
  const sep = '='.repeat(80)

  lines.push(sep)
  lines.push('WORKER BENCHMARK RESULTS')
  lines.push(sep)
  lines.push(`Runtime: ${result.runtime}`)
  lines.push(`Backend: ${result.config.backend}`)
  lines.push(`Region: ${result.runtimeInfo.colo || 'unknown'}`)
  lines.push(`Duration: ${result.durationMs.toFixed(1)}ms`)
  lines.push(sep)
  lines.push('')

  lines.push('SUMMARY:')
  lines.push(`  Total Patterns: ${result.summary.totalPatterns}`)
  lines.push(`  Passed: ${result.summary.passedPatterns}`)
  lines.push(`  Failed: ${result.summary.failedPatterns}`)
  lines.push(`  Avg Latency: ${result.summary.avgLatencyMs.toFixed(1)}ms`)
  lines.push(`  P95 Latency: ${result.summary.p95LatencyMs.toFixed(1)}ms`)
  lines.push('')

  for (const datasetResult of result.results) {
    lines.push(`--- ${datasetResult.dataset.toUpperCase()} ---`)
    for (const pattern of datasetResult.patterns) {
      const status = pattern.passedTarget ? 'PASS' : 'FAIL'
      lines.push(
        `  [${status}] ${pattern.pattern}: p50=${pattern.latencyMs.p50.toFixed(1)}ms (target: ${pattern.targetMs}ms)`
      )
    }
    lines.push('')
  }

  return lines.join('\n')
}

/**
 * Handle health check request
 */
function handleHealth(env: Env): Response {
  const workerConfig = runtimeConfigs.find((c) => c.name === 'worker')!

  return new Response(
    JSON.stringify({
      status: 'healthy',
      runtime: 'worker',
      supportedBackends: workerConfig.supportedBackends,
      supportedDatasets: workerConfig.supportedDatasets,
      r2Available: !!env.PARQUEDB_BUCKET,
      kvAvailable: !!env.BENCHMARK_RESULTS,
    }),
    {
      headers: { 'Content-Type': 'application/json' },
    }
  )
}

/**
 * Handle stored results request
 */
async function handleResults(env: Env, url: URL): Promise<Response> {
  if (!env.BENCHMARK_RESULTS) {
    return new Response(
      JSON.stringify({ error: 'Results storage not configured' }),
      { status: 503, headers: { 'Content-Type': 'application/json' } }
    )
  }

  const format = url.searchParams.get('format') || 'json'
  const limit = parseInt(url.searchParams.get('limit') || '10', 10)

  // List recent results
  const list = await env.BENCHMARK_RESULTS.list({ limit })
  const results = await Promise.all(
    list.keys.map(async (key) => {
      const value = await env.BENCHMARK_RESULTS!.get(key.name)
      return value ? JSON.parse(value) : null
    })
  )

  const validResults = results.filter(Boolean)

  if (format === 'json') {
    return new Response(JSON.stringify(validResults, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Summary format
  const summary = validResults.map((r: WorkerBenchmarkResult) => ({
    timestamp: r.completedAt,
    backend: r.config.backend,
    datasets: r.config.datasets,
    passed: r.summary.passedPatterns,
    failed: r.summary.failedPatterns,
    avgLatencyMs: r.summary.avgLatencyMs.toFixed(1),
  }))

  return new Response(JSON.stringify(summary, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  })
}

// =============================================================================
// Worker Export
// =============================================================================

export default {
  /**
   * Main fetch handler for the Worker
   */
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    }

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders })
    }

    try {
      // Route handling
      if (path === '/health' || path === '/') {
        return handleHealth(env)
      }

      if (path === '/results') {
        const response = await handleResults(env, url)
        const responseHeaders: Record<string, string> = {}
        response.headers.forEach((value, key) => {
          responseHeaders[key] = value
        })
        return new Response(response.body, {
          status: response.status,
          headers: { ...responseHeaders, ...corsHeaders },
        })
      }

      if (path === '/benchmark') {
        const config = parseConfig(url)
        const workerConfig = runtimeConfigs.find((c) => c.name === 'worker')!

        // Validate backend
        if (!workerConfig.supportedBackends.includes(config.backend)) {
          return new Response(
            JSON.stringify({
              error: `Invalid backend: ${config.backend}`,
              valid: workerConfig.supportedBackends,
            }),
            { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
          )
        }

        // Validate datasets
        const validDatasets = getValidDatasets('worker', config.backend)
        const invalidDatasets = config.datasets.filter((d) => !validDatasets.includes(d))
        if (invalidDatasets.length > 0) {
          return new Response(
            JSON.stringify({
              error: `Invalid datasets for ${config.backend}: ${invalidDatasets.join(', ')}`,
              valid: validDatasets,
            }),
            { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
          )
        }

        // Run benchmark
        const result = await runBenchmarkSuite(config, env, request)

        // Store result if KV is available
        if (env.BENCHMARK_RESULTS) {
          const key = `benchmark-${Date.now()}`
          await env.BENCHMARK_RESULTS.put(key, JSON.stringify(result), {
            expirationTtl: 86400 * 7, // 7 days
          })
        }

        // Format and return response
        const body = formatResult(result, config.format)
        const contentType = config.format === 'json' ? 'application/json' : 'text/plain'

        return new Response(body, {
          headers: { 'Content-Type': contentType, ...corsHeaders },
        })
      }

      // 404 for unknown paths
      return new Response(
        JSON.stringify({
          error: 'Not found',
          endpoints: ['/health', '/benchmark', '/results'],
        }),
        { status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      )
    } catch (error) {
      console.error('Benchmark error:', error)
      return new Response(
        JSON.stringify({
          error: error instanceof Error ? error.message : 'Unknown error',
        }),
        { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      )
    }
  },
}

// =============================================================================
// Exports for programmatic use
// =============================================================================

export {
  runBenchmarkSuite,
  runDatasetBenchmark,
  runPatternBenchmark,
  parseConfig,
  formatResult,
  calculateLatencyStats,
}

export type {
  WorkerBenchmarkConfig,
  WorkerBenchmarkResult,
  DatasetResult,
  PatternResult,
  LatencyStats,
}

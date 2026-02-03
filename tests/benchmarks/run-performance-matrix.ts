#!/usr/bin/env npx tsx
/**
 * External Performance Matrix Runner for Deployed Workers
 *
 * This script runs cold/warm/cached benchmarks against actually deployed
 * Cloudflare Workers to get accurate cold start measurements.
 *
 * Usage:
 *   pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://your-worker.workers.dev
 *   pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://your-worker.workers.dev --iterations 50
 *   pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://your-worker.workers.dev --output results.json
 *
 * The worker must expose benchmark endpoints at:
 *   GET /benchmark/cold/:operation
 *   GET /benchmark/warm/:operation
 *   GET /benchmark/cached/:operation
 *
 * Operations:
 *   - get-by-id
 *   - filter-indexed
 *   - filter-scan
 *   - relationship-traverse
 *   - aggregation
 */

import { parseArgs } from 'node:util'

// =============================================================================
// Types
// =============================================================================

type TemperatureState = 'cold' | 'warm' | 'cached'
type OperationCategory =
  | 'get-by-id'
  | 'filter-indexed'
  | 'filter-scan'
  | 'relationship-traverse'
  | 'aggregation'

interface BenchmarkStats {
  name: string
  iterations: number
  totalTime: number
  mean: number
  median: number
  min: number
  max: number
  stdDev: number
  p95: number
  p99: number
  opsPerSecond: number
}

interface PerformanceResult {
  operation: OperationCategory
  temperature: TemperatureState
  stats: BenchmarkStats
}

interface PerformanceMatrix {
  timestamp: string
  workerUrl: string
  results: PerformanceResult[]
  summary: {
    coldVsWarmRatio: Record<OperationCategory, number>
    cachedVsWarmSpeedup: Record<OperationCategory, number>
    overallColdPenalty: number
    overallCacheSpeedup: number
  }
  rawTimings: Record<string, number[]>
}

// =============================================================================
// Argument Parsing
// =============================================================================

const { values: args } = parseArgs({
  options: {
    url: { type: 'string', short: 'u' },
    iterations: { type: 'string', short: 'i', default: '20' },
    warmupIterations: { type: 'string', short: 'w', default: '5' },
    coldDelay: { type: 'string', short: 'd', default: '5000' },
    output: { type: 'string', short: 'o' },
    help: { type: 'boolean', short: 'h' },
    verbose: { type: 'boolean', short: 'v' },
  },
})

if (args.help || !args.url) {
  console.log(`
Performance Matrix Benchmark Runner

Runs cold/warm/cached benchmarks against deployed Cloudflare Workers.

Usage:
  pnpm tsx tests/benchmarks/run-performance-matrix.ts --url <worker-url> [options]

Options:
  -u, --url <url>         Worker URL (required)
  -i, --iterations <n>    Number of benchmark iterations (default: 20)
  -w, --warmupIterations  Number of warmup iterations (default: 5)
  -d, --coldDelay <ms>    Delay before cold start tests (default: 5000)
  -o, --output <file>     Output results to JSON file
  -v, --verbose           Verbose output
  -h, --help              Show this help

Examples:
  # Basic benchmark
  pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://api.parque.db

  # Save results to file
  pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://api.parque.db -o results.json

  # More iterations for accuracy
  pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://api.parque.db -i 50

Required Worker Endpoints:
  The worker must expose these benchmark endpoints:

  GET /_benchmark/setup           - Initialize test data (called once)
  GET /_benchmark/:temp/:op       - Run a single benchmark operation
  GET /_benchmark/invalidate      - Invalidate caches (for cold tests)

  Where:
    :temp = cold | warm | cached
    :op   = get-by-id | filter-indexed | filter-scan | relationship-traverse | aggregation

  Response format:
  {
    "success": true,
    "operation": "get-by-id",
    "durationMs": 5.234,
    "serverTiming": { ... }
  }
`)
  process.exit(args.help ? 0 : 1)
}

const WORKER_URL = args.url!.replace(/\/$/, '')
const ITERATIONS = parseInt(args.iterations!, 10)
const WARMUP_ITERATIONS = parseInt(args.warmupIterations!, 10)
const COLD_DELAY = parseInt(args.coldDelay!, 10)
const VERBOSE = args.verbose ?? false

// =============================================================================
// Utility Functions
// =============================================================================

function log(message: string): void {
  if (VERBOSE) {
    console.log(`[${new Date().toISOString()}] ${message}`)
  }
}

function calculateStats(name: string, durations: number[]): BenchmarkStats {
  if (durations.length === 0) {
    return {
      name,
      iterations: 0,
      totalTime: 0,
      mean: 0,
      median: 0,
      min: 0,
      max: 0,
      stdDev: 0,
      p95: 0,
      p99: 0,
      opsPerSecond: 0,
    }
  }

  const sorted = [...durations].sort((a, b) => a - b)
  const n = durations.length
  const totalTime = durations.reduce((a, b) => a + b, 0)
  const mean = totalTime / n

  const variance = durations.reduce((sum, d) => sum + Math.pow(d - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  const percentile = (p: number) => {
    const index = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(index, n - 1))]!
  }

  return {
    name,
    iterations: n,
    totalTime,
    mean,
    median: percentile(50),
    min: sorted[0]!,
    max: sorted[n - 1]!,
    stdDev,
    p95: percentile(95),
    p99: percentile(99),
    opsPerSecond: (n / totalTime) * 1000,
  }
}

function formatStats(stats: BenchmarkStats): string {
  return [
    `  ${stats.name}:`,
    `    Iterations: ${stats.iterations}`,
    `    Mean:       ${stats.mean.toFixed(3)}ms`,
    `    Median:     ${stats.median.toFixed(3)}ms`,
    `    Min:        ${stats.min.toFixed(3)}ms`,
    `    Max:        ${stats.max.toFixed(3)}ms`,
    `    P95:        ${stats.p95.toFixed(3)}ms`,
    `    P99:        ${stats.p99.toFixed(3)}ms`,
    `    Ops/sec:    ${stats.opsPerSecond.toFixed(1)}`,
  ].join('\n')
}

function formatDuration(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}us`
  if (ms < 1000) return `${ms.toFixed(2)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

// =============================================================================
// HTTP Client
// =============================================================================

interface BenchmarkResponse {
  success: boolean
  operation: string
  durationMs: number
  error?: string | undefined
  serverTiming?: Record<string, number> | undefined
}

async function fetchBenchmark(
  temperature: TemperatureState,
  operation: OperationCategory
): Promise<BenchmarkResponse> {
  const url = `${WORKER_URL}/_benchmark/${temperature}/${operation}`
  const start = performance.now()

  try {
    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'Cache-Control': 'no-cache',
      },
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json() as BenchmarkResponse

    // If server didn't provide timing, use client-side measurement
    if (!data.durationMs) {
      data.durationMs = performance.now() - start
    }

    return data
  } catch (error) {
    return {
      success: false,
      operation,
      durationMs: performance.now() - start,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

async function invalidateCache(): Promise<void> {
  try {
    await fetch(`${WORKER_URL}/_benchmark/invalidate`, {
      method: 'POST',
      headers: { 'Cache-Control': 'no-cache' },
    })
  } catch {
    // Ignore invalidation errors
  }
}

async function setupTestData(): Promise<boolean> {
  try {
    const response = await fetch(`${WORKER_URL}/_benchmark/setup`, {
      method: 'POST',
      headers: { 'Accept': 'application/json' },
    })
    const data = await response.json() as { success: boolean }
    return data.success ?? response.ok
  } catch (error) {
    console.error('Failed to setup test data:', error)
    return false
  }
}

// =============================================================================
// Benchmark Execution
// =============================================================================

async function runColdBenchmark(
  operation: OperationCategory,
  iterations: number
): Promise<number[]> {
  const timings: number[] = []

  for (let i = 0; i < iterations; i++) {
    log(`Cold benchmark ${operation} iteration ${i + 1}/${iterations}`)

    // Wait for worker to go idle
    await new Promise(resolve => setTimeout(resolve, COLD_DELAY))

    // Invalidate caches
    await invalidateCache()

    // Run benchmark
    const result = await fetchBenchmark('cold', operation)

    if (result.success) {
      timings.push(result.durationMs)
    } else {
      console.warn(`  Warning: Cold ${operation} failed: ${result.error}`)
    }
  }

  return timings
}

async function runWarmBenchmark(
  operation: OperationCategory,
  warmupIterations: number,
  iterations: number
): Promise<number[]> {
  // Warmup
  log(`Warming up ${operation}...`)
  for (let i = 0; i < warmupIterations; i++) {
    await fetchBenchmark('warm', operation)
  }

  // Benchmark
  const timings: number[] = []
  for (let i = 0; i < iterations; i++) {
    log(`Warm benchmark ${operation} iteration ${i + 1}/${iterations}`)
    const result = await fetchBenchmark('warm', operation)

    if (result.success) {
      timings.push(result.durationMs)
    } else {
      console.warn(`  Warning: Warm ${operation} failed: ${result.error}`)
    }
  }

  return timings
}

async function runCachedBenchmark(
  operation: OperationCategory,
  warmupIterations: number,
  iterations: number
): Promise<number[]> {
  // Prime the cache
  log(`Priming cache for ${operation}...`)
  for (let i = 0; i < warmupIterations; i++) {
    await fetchBenchmark('cached', operation)
  }

  // Benchmark cached reads
  const timings: number[] = []
  for (let i = 0; i < iterations; i++) {
    log(`Cached benchmark ${operation} iteration ${i + 1}/${iterations}`)
    const result = await fetchBenchmark('cached', operation)

    if (result.success) {
      timings.push(result.durationMs)
    } else {
      console.warn(`  Warning: Cached ${operation} failed: ${result.error}`)
    }
  }

  return timings
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  console.log('')
  console.log('='.repeat(80))
  console.log('     COLD/WARM/CACHED PERFORMANCE MATRIX BENCHMARK')
  console.log('='.repeat(80))
  console.log(`Worker URL:         ${WORKER_URL}`)
  console.log(`Iterations:         ${ITERATIONS}`)
  console.log(`Warmup Iterations:  ${WARMUP_ITERATIONS}`)
  console.log(`Cold Delay:         ${COLD_DELAY}ms`)
  console.log('='.repeat(80))
  console.log('')

  // Setup test data
  console.log('Setting up test data...')
  const setupSuccess = await setupTestData()
  if (!setupSuccess) {
    console.log('Warning: Test data setup may have failed. Continuing anyway.')
  }
  console.log('Setup complete.')
  console.log('')

  const operations: OperationCategory[] = [
    'get-by-id',
    'filter-indexed',
    'filter-scan',
    'relationship-traverse',
    'aggregation',
  ]

  const results: PerformanceResult[] = []
  const rawTimings: Record<string, number[]> = {}

  // Run benchmarks for each operation
  for (const operation of operations) {
    console.log(`\n${'─'.repeat(60)}`)
    console.log(`  ${operation.toUpperCase()}`)
    console.log('─'.repeat(60))

    // Cold benchmark
    console.log('\n  [COLD] First request after idle...')
    const coldTimings = await runColdBenchmark(operation, Math.ceil(ITERATIONS / 2))
    rawTimings[`cold:${operation}`] = coldTimings

    if (coldTimings.length > 0) {
      const coldStats = calculateStats(`COLD ${operation}`, coldTimings)
      results.push({ operation, temperature: 'cold', stats: coldStats })
      console.log(formatStats(coldStats))
    }

    // Warm benchmark
    console.log('\n  [WARM] Subsequent requests...')
    const warmTimings = await runWarmBenchmark(operation, WARMUP_ITERATIONS, ITERATIONS)
    rawTimings[`warm:${operation}`] = warmTimings

    if (warmTimings.length > 0) {
      const warmStats = calculateStats(`WARM ${operation}`, warmTimings)
      results.push({ operation, temperature: 'warm', stats: warmStats })
      console.log(formatStats(warmStats))
    }

    // Cached benchmark
    console.log('\n  [CACHED] Cache API hits...')
    const cachedTimings = await runCachedBenchmark(operation, WARMUP_ITERATIONS, ITERATIONS)
    rawTimings[`cached:${operation}`] = cachedTimings

    if (cachedTimings.length > 0) {
      const cachedStats = calculateStats(`CACHED ${operation}`, cachedTimings)
      results.push({ operation, temperature: 'cached', stats: cachedStats })
      console.log(formatStats(cachedStats))
    }
  }

  // Calculate summary
  const coldVsWarmRatio: Record<OperationCategory, number> = {} as any
  const cachedVsWarmSpeedup: Record<OperationCategory, number> = {} as any
  let totalColdPenalty = 0
  let totalCacheSpeedup = 0
  let opCount = 0

  for (const operation of operations) {
    const cold = results.find(r => r.operation === operation && r.temperature === 'cold')
    const warm = results.find(r => r.operation === operation && r.temperature === 'warm')
    const cached = results.find(r => r.operation === operation && r.temperature === 'cached')

    if (cold && warm) {
      coldVsWarmRatio[operation] = cold.stats.median / warm.stats.median
      totalColdPenalty += coldVsWarmRatio[operation]
      opCount++
    }

    if (warm && cached) {
      cachedVsWarmSpeedup[operation] = warm.stats.median / cached.stats.median
      totalCacheSpeedup += cachedVsWarmSpeedup[operation]
    }
  }

  const overallColdPenalty = opCount > 0 ? totalColdPenalty / opCount : 0
  const overallCacheSpeedup = opCount > 0 ? totalCacheSpeedup / opCount : 0

  // Build result object
  const matrix: PerformanceMatrix = {
    timestamp: new Date().toISOString(),
    workerUrl: WORKER_URL,
    results,
    summary: {
      coldVsWarmRatio,
      cachedVsWarmSpeedup,
      overallColdPenalty,
      overallCacheSpeedup,
    },
    rawTimings,
  }

  // Print summary
  console.log('\n')
  console.log('='.repeat(80))
  console.log('                           SUMMARY')
  console.log('='.repeat(80))
  console.log('')

  console.log('Cold vs Warm Ratio (lower is better):')
  console.log('─'.repeat(60))
  for (const [op, ratio] of Object.entries(coldVsWarmRatio)) {
    const status = ratio < 3 ? 'GOOD' : ratio < 5 ? 'WARN' : 'SLOW'
    console.log(`  ${op.padEnd(25)} ${ratio.toFixed(2)}x  [${status}]`)
  }
  console.log(`  ${'OVERALL'.padEnd(25)} ${overallColdPenalty.toFixed(2)}x`)

  console.log('')
  console.log('Cache Speedup (higher is better):')
  console.log('─'.repeat(60))
  for (const [op, speedup] of Object.entries(cachedVsWarmSpeedup)) {
    const status = speedup >= 5 ? 'GOOD' : speedup >= 2 ? 'OK' : 'LOW'
    console.log(`  ${op.padEnd(25)} ${speedup.toFixed(2)}x  [${status}]`)
  }
  console.log(`  ${'OVERALL'.padEnd(25)} ${overallCacheSpeedup.toFixed(2)}x`)

  console.log('')
  console.log('Performance Matrix:')
  console.log('─'.repeat(80))
  console.log(`${'Operation'.padEnd(25)} ${'Cold'.padStart(10)} ${'Warm'.padStart(10)} ${'Cached'.padStart(10)} ${'Cold/Warm'.padStart(12)} ${'Cache'.padStart(8)}`)
  console.log('─'.repeat(80))

  for (const operation of operations) {
    const cold = results.find(r => r.operation === operation && r.temperature === 'cold')
    const warm = results.find(r => r.operation === operation && r.temperature === 'warm')
    const cached = results.find(r => r.operation === operation && r.temperature === 'cached')

    const coldMs = cold ? formatDuration(cold.stats.median) : '-'
    const warmMs = warm ? formatDuration(warm.stats.median) : '-'
    const cachedMs = cached ? formatDuration(cached.stats.median) : '-'
    const ratio = coldVsWarmRatio[operation]?.toFixed(2) || '-'
    const speedup = cachedVsWarmSpeedup[operation]?.toFixed(2) || '-'

    console.log(
      `${operation.padEnd(25)} ${coldMs.padStart(10)} ${warmMs.padStart(10)} ${cachedMs.padStart(10)} ${(ratio + 'x').padStart(12)} ${(speedup + 'x').padStart(8)}`
    )
  }

  console.log('─'.repeat(80))
  console.log('')

  // Performance targets check
  console.log('Performance Target Check:')
  console.log('─'.repeat(60))

  const coldTarget = overallColdPenalty < 3
  const cacheTarget = overallCacheSpeedup > 5

  console.log(`  Cold start penalty < 3x:  ${coldTarget ? 'PASS' : 'FAIL'} (${overallColdPenalty.toFixed(2)}x)`)
  console.log(`  Cache speedup > 5x:       ${cacheTarget ? 'PASS' : 'FAIL'} (${overallCacheSpeedup.toFixed(2)}x)`)

  console.log('')
  console.log('='.repeat(80))

  // Save to file if requested
  if (args.output) {
    const fs = await import('node:fs/promises')
    await fs.writeFile(args.output, JSON.stringify(matrix, null, 2))
    console.log(`\nResults saved to: ${args.output}`)
  }

  // Exit with appropriate code
  process.exit(coldTarget && cacheTarget ? 0 : 1)
}

main().catch(error => {
  console.error('Benchmark failed:', error)
  process.exit(1)
})

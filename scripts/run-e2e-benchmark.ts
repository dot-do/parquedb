#!/usr/bin/env bun
/**
 * E2E Benchmark Runner Script
 *
 * Orchestrates end-to-end performance benchmarks against deployed ParqueDB workers.
 * Calls the benchmark endpoints and collects results for analysis.
 *
 * Usage:
 *   bun scripts/run-e2e-benchmark.ts [options]
 *
 * Options:
 *   --url=<url>             Worker URL (default: https://parquedb.workers.do)
 *   --benchmarks=<list>     Comma-separated benchmark types (default: all)
 *                           Valid: cold-start, crud, query, backend
 *   --backend=<type>        Backend to test: iceberg, delta, native-parquet (default: all)
 *   --iterations=<n>        Iterations per test (default: 5)
 *   --warmup=<n>            Warmup iterations (default: 2)
 *   --output=<format>       Output format: table, json, markdown (default: table)
 *   --verbose               Show detailed results
 *   --compare-baseline=<f>  Compare against baseline file
 *   --threshold=<n>         Regression threshold percentage (default: 20)
 *   --concurrency=<n>       Concurrent requests for throughput tests (default: 1)
 *   --help                  Show this help
 *
 * Examples:
 *   bun scripts/run-e2e-benchmark.ts
 *   bun scripts/run-e2e-benchmark.ts --benchmarks=cold-start,crud
 *   bun scripts/run-e2e-benchmark.ts --url=http://localhost:8787 --verbose
 *   bun scripts/run-e2e-benchmark.ts --backend=iceberg --output=json > results.json
 */

// =============================================================================
// Types
// =============================================================================

interface Config {
  url: string
  benchmarks: string[]
  backend: string[]
  iterations: number
  warmup: number
  output: 'table' | 'json' | 'markdown'
  verbose: boolean
  compareBaseline?: string
  threshold: number
  concurrency: number
}

interface LatencyStats {
  p50: number
  p95: number
  p99: number
  avg: number
  min: number
  max: number
}

interface ColdStartResult {
  coldStartMs: number
  workerInitMs: number
  cacheInitMs: number
  firstQueryMs: number
  metadata: {
    colo: string
    timestamp: string
  }
}

interface CrudResult {
  operation: string
  iterations: number
  batchSize?: number
  latencyMs: LatencyStats
  throughput: {
    opsPerSec: number
    totalTimeMs: number
  }
}

interface QueryResult {
  queryType: string
  dataset: string
  iterations: number
  latencyMs: LatencyStats
  queryStats: {
    rowsScanned: number
    rowsReturned: number
    rowGroupsSkipped: number
    rowGroupsScanned: number
  }
}

interface BackendResult {
  backend: string
  operations: {
    write?: { latencyMs: LatencyStats; bytesWritten: number; rowsWritten: number }
    read?: { latencyMs: LatencyStats; bytesRead: number; rowsRead: number }
    'time-travel'?: { latencyMs: LatencyStats; snapshotsAvailable: number }
  }
}

interface BenchmarkResults {
  metadata: {
    url: string
    timestamp: string
    duration: number
    config: Config
  }
  coldStart?: ColdStartResult
  crud?: CrudResult[]
  query?: QueryResult[]
  backend?: BackendResult[]
  summary: {
    avgLatencyMs: number
    p95LatencyMs: number
    totalOpsPerSec: number
  }
}

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_CONFIG: Config = {
  url: 'https://parquedb.workers.do',
  benchmarks: ['cold-start', 'crud', 'query', 'backend'],
  backend: ['iceberg', 'delta', 'native-parquet'],
  iterations: 5,
  warmup: 2,
  output: 'table',
  verbose: false,
  threshold: 20,
  concurrency: 1,
}

function parseArgs(): Config {
  const args = process.argv.slice(2)
  const config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
E2E Benchmark Runner - ParqueDB Worker Performance Testing

Usage: bun scripts/run-e2e-benchmark.ts [options]

Options:
  --url=<url>             Worker URL (default: ${DEFAULT_CONFIG.url})
  --benchmarks=<list>     Comma-separated: cold-start, crud, query, backend
  --backend=<type>        Backend to test: iceberg, delta, native-parquet
  --iterations=<n>        Iterations per test (default: ${DEFAULT_CONFIG.iterations})
  --warmup=<n>            Warmup iterations (default: ${DEFAULT_CONFIG.warmup})
  --output=<format>       Output: table, json, markdown (default: ${DEFAULT_CONFIG.output})
  --verbose               Show detailed results
  --compare-baseline=<f>  Compare against baseline JSON file
  --threshold=<n>         Regression threshold % (default: ${DEFAULT_CONFIG.threshold})
  --concurrency=<n>       Concurrent requests (default: ${DEFAULT_CONFIG.concurrency})
  --help                  Show this help

Benchmark Types:
  cold-start    Measure worker cold start latency
  crud          Benchmark CRUD operations (create, read, update, delete, batch-*)
  query         Benchmark query patterns (simple, range, compound filters)
  backend       Backend-specific benchmarks (Iceberg, Delta, Native Parquet)

Examples:
  # Run all benchmarks against production
  bun scripts/run-e2e-benchmark.ts --url https://parquedb.workers.do

  # Run specific benchmarks
  bun scripts/run-e2e-benchmark.ts --benchmarks cold-start,crud

  # Compare backends
  bun scripts/run-e2e-benchmark.ts --backend iceberg --output json > iceberg.json
  bun scripts/run-e2e-benchmark.ts --backend delta --output json > delta.json

  # Detect regressions
  bun scripts/run-e2e-benchmark.ts --compare-baseline baseline.json --threshold 20
`)
      process.exit(0)
    }

    const [key, value] = arg.split('=')
    switch (key) {
      case '--url':
        config.url = value!
        break
      case '--benchmarks':
        config.benchmarks = value!.split(',')
        break
      case '--backend':
        config.backend = value!.split(',')
        break
      case '--iterations':
        config.iterations = parseInt(value!, 10)
        break
      case '--warmup':
        config.warmup = parseInt(value!, 10)
        break
      case '--output':
        config.output = value as 'table' | 'json' | 'markdown'
        break
      case '--verbose':
        config.verbose = true
        break
      case '--compare-baseline':
        config.compareBaseline = value
        break
      case '--threshold':
        config.threshold = parseInt(value!, 10)
        break
      case '--concurrency':
        config.concurrency = parseInt(value!, 10)
        break
    }
  }

  return config
}

// =============================================================================
// HTTP Client
// =============================================================================

interface FetchResult<T> {
  success: boolean
  status?: number
  error?: string
  latencyMs: number
  data?: T
  serverTiming?: string | null
}

async function timedFetch<T>(url: string): Promise<FetchResult<T>> {
  const start = performance.now()
  try {
    const response = await fetch(url)
    const latencyMs = performance.now() - start
    const data = await response.json() as T
    return {
      success: response.ok,
      status: response.status,
      latencyMs,
      data,
      serverTiming: response.headers.get('Server-Timing'),
    }
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      latencyMs: performance.now() - start,
    }
  }
}

// =============================================================================
// Benchmark Runners
// =============================================================================

async function runColdStartBenchmark(config: Config): Promise<ColdStartResult | undefined> {
  console.log('\n--- Cold Start Benchmark ---')

  const url = `${config.url}/benchmark/e2e/cold-start`
  const result = await timedFetch<ColdStartResult>(url)

  if (result.success && result.data) {
    const data = result.data
    console.log(`  Cold start: ${data.coldStartMs}ms`)
    console.log(`  Worker init: ${data.workerInitMs}ms`)
    console.log(`  Cache init: ${data.cacheInitMs}ms`)
    console.log(`  First query: ${data.firstQueryMs}ms`)
    console.log(`  Colo: ${data.metadata.colo}`)
    return data
  } else {
    console.log(`  Error: ${result.error || 'Unknown error'}`)
    return undefined
  }
}

async function runCrudBenchmarks(config: Config): Promise<CrudResult[]> {
  console.log('\n--- CRUD Benchmarks ---')

  const operations = ['create', 'read', 'update', 'delete', 'batch-create', 'batch-read']
  const results: CrudResult[] = []

  for (const op of operations) {
    const url = `${config.url}/benchmark/e2e/crud/${op}?iterations=${config.iterations}&warmup=${config.warmup}&batchSize=100`
    const result = await timedFetch<CrudResult>(url)

    if (result.success && result.data) {
      const data = result.data
      console.log(`  ${op}: p50=${data.latencyMs.p50}ms, p95=${data.latencyMs.p95}ms, throughput=${data.throughput.opsPerSec} ops/s`)
      results.push(data)
    } else {
      console.log(`  ${op}: Error - ${result.error || 'Unknown error'}`)
    }
  }

  return results
}

async function runQueryBenchmarks(config: Config): Promise<QueryResult[]> {
  console.log('\n--- Query Benchmarks ---')

  const queryTypes = ['simple-filter', 'range-filter', 'compound-filter']
  const results: QueryResult[] = []

  for (const queryType of queryTypes) {
    const url = `${config.url}/benchmark/e2e/query/${queryType}?dataset=imdb&iterations=${config.iterations}&limit=100`
    const result = await timedFetch<QueryResult>(url)

    if (result.success && result.data) {
      const data = result.data
      console.log(`  ${queryType}: p50=${data.latencyMs.p50}ms, p95=${data.latencyMs.p95}ms, rows=${data.queryStats.rowsReturned}`)

      if (config.verbose) {
        console.log(`    Scanned: ${data.queryStats.rowsScanned}, Skipped: ${data.queryStats.rowGroupsSkipped} row groups`)
      }

      results.push(data)
    } else {
      console.log(`  ${queryType}: Error - ${result.error || 'Unknown error'}`)
    }
  }

  return results
}

async function runBackendBenchmarks(config: Config): Promise<BackendResult[]> {
  console.log('\n--- Backend Benchmarks ---')

  const results: BackendResult[] = []

  for (const backend of config.backend) {
    console.log(`\n  Backend: ${backend}`)
    const url = `${config.url}/benchmark/e2e/backend/${backend}?iterations=${config.iterations}&dataSize=1000`
    const result = await timedFetch<BackendResult>(url)

    if (result.success && result.data) {
      const data = result.data

      if (data.operations.write) {
        console.log(`    Write: p50=${data.operations.write.latencyMs.p50}ms, ${data.operations.write.bytesWritten} bytes`)
      }
      if (data.operations.read) {
        console.log(`    Read: p50=${data.operations.read.latencyMs.p50}ms, ${data.operations.read.bytesRead} bytes`)
      }
      if (data.operations['time-travel']) {
        console.log(`    Time-travel: p50=${data.operations['time-travel'].latencyMs.p50}ms`)
      }

      results.push(data)
    } else {
      console.log(`    Error: ${result.error || 'Unknown error'}`)
    }
  }

  return results
}

async function runHealthCheck(config: Config): Promise<boolean> {
  console.log('--- Health Check ---')

  const url = `${config.url}/benchmark/e2e/health`
  const result = await timedFetch<{ status: string; checks: Record<string, { status: string }> }>(url)

  if (result.success && result.data) {
    console.log(`  Status: ${result.data.status}`)
    console.log(`  Latency: ${Math.round(result.latencyMs)}ms`)
    return result.data.status === 'ok'
  } else {
    console.log(`  Error: ${result.error || 'Unknown error'}`)
    return false
  }
}

// =============================================================================
// Output Formatters
// =============================================================================

function outputTable(results: BenchmarkResults): void {
  console.log('\n====================================')
  console.log('E2E Benchmark Results')
  console.log('====================================')
  console.log(`URL: ${results.metadata.url}`)
  console.log(`Timestamp: ${results.metadata.timestamp}`)
  console.log(`Duration: ${Math.round(results.metadata.duration / 1000)}s`)
  console.log('')

  console.log('Benchmark           P50     P95     Throughput')
  console.log('-----------         ---     ---     ----------')

  if (results.coldStart) {
    console.log(`cold-start          ${results.coldStart.coldStartMs}ms    -       -`)
  }

  if (results.crud) {
    for (const op of results.crud) {
      const name = `crud/${op.operation}`.padEnd(18)
      const p50 = `${op.latencyMs.p50}ms`.padEnd(8)
      const p95 = `${op.latencyMs.p95}ms`.padEnd(8)
      const throughput = `${op.throughput.opsPerSec} ops/s`
      console.log(`${name}${p50}${p95}${throughput}`)
    }
  }

  if (results.query) {
    for (const q of results.query) {
      const name = `query/${q.queryType}`.substring(0, 18).padEnd(18)
      const p50 = `${q.latencyMs.p50}ms`.padEnd(8)
      const p95 = `${q.latencyMs.p95}ms`.padEnd(8)
      console.log(`${name}${p50}${p95}-`)
    }
  }

  console.log('')
  console.log('Summary:')
  console.log(`  Avg Latency: ${results.summary.avgLatencyMs}ms`)
  console.log(`  P95 Latency: ${results.summary.p95LatencyMs}ms`)
  console.log(`  Total Throughput: ${results.summary.totalOpsPerSec} ops/s`)
}

function outputMarkdown(results: BenchmarkResults): void {
  console.log('## E2E Benchmark Results')
  console.log('')
  console.log('| Benchmark | P50 | P95 | Throughput |')
  console.log('|-----------|-----|-----|------------|')

  if (results.coldStart) {
    console.log(`| cold-start | ${results.coldStart.coldStartMs}ms | - | - |`)
  }

  if (results.crud) {
    for (const op of results.crud) {
      console.log(`| crud/${op.operation} | ${op.latencyMs.p50}ms | ${op.latencyMs.p95}ms | ${op.throughput.opsPerSec} ops/s |`)
    }
  }

  if (results.query) {
    for (const q of results.query) {
      console.log(`| query/${q.queryType} | ${q.latencyMs.p50}ms | ${q.latencyMs.p95}ms | - |`)
    }
  }

  if (results.backend) {
    for (const b of results.backend) {
      if (b.operations.write) {
        console.log(`| ${b.backend}/write | ${b.operations.write.latencyMs.p50}ms | ${b.operations.write.latencyMs.p95}ms | - |`)
      }
      if (b.operations.read) {
        console.log(`| ${b.backend}/read | ${b.operations.read.latencyMs.p50}ms | ${b.operations.read.latencyMs.p95}ms | - |`)
      }
    }
  }

  console.log('')
  console.log('### Summary')
  console.log(`- Average Latency: ${results.summary.avgLatencyMs}ms`)
  console.log(`- P95 Latency: ${results.summary.p95LatencyMs}ms`)
  console.log(`- Total Throughput: ${results.summary.totalOpsPerSec} ops/s`)
}

function calculateSummary(results: Partial<BenchmarkResults>): BenchmarkResults['summary'] {
  const latencies: number[] = []
  const p95s: number[] = []
  let totalOps = 0
  let totalTime = 0

  if (results.coldStart) {
    latencies.push(results.coldStart.coldStartMs)
    p95s.push(results.coldStart.coldStartMs)
  }

  if (results.crud) {
    for (const op of results.crud) {
      latencies.push(op.latencyMs.avg)
      p95s.push(op.latencyMs.p95)
      totalOps += op.iterations
      totalTime += op.throughput.totalTimeMs
    }
  }

  if (results.query) {
    for (const q of results.query) {
      latencies.push(q.latencyMs.avg)
      p95s.push(q.latencyMs.p95)
    }
  }

  const avgLatency = latencies.length > 0
    ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length)
    : 0

  const p95Latency = p95s.length > 0
    ? Math.round(Math.max(...p95s))
    : 0

  const totalOpsPerSec = totalTime > 0
    ? Math.round((totalOps / (totalTime / 1000)) * 10) / 10
    : 0

  return {
    avgLatencyMs: avgLatency,
    p95LatencyMs: p95Latency,
    totalOpsPerSec,
  }
}

// =============================================================================
// Baseline Comparison
// =============================================================================

async function compareBaseline(results: BenchmarkResults, baselineFile: string, threshold: number): Promise<boolean> {
  console.log(`\n--- Comparing against baseline: ${baselineFile} ---`)

  try {
    const fs = await import('fs/promises')
    const content = await fs.readFile(baselineFile, 'utf-8')
    const baseline = JSON.parse(content) as BenchmarkResults

    let hasRegression = false
    const regressions: string[] = []

    // Compare cold start
    if (results.coldStart && baseline.coldStart) {
      const diff = ((results.coldStart.coldStartMs - baseline.coldStart.coldStartMs) / baseline.coldStart.coldStartMs) * 100
      if (diff > threshold) {
        regressions.push(`cold-start: ${diff.toFixed(1)}% slower (${baseline.coldStart.coldStartMs}ms -> ${results.coldStart.coldStartMs}ms)`)
        hasRegression = true
      }
    }

    // Compare CRUD operations
    if (results.crud && baseline.crud) {
      for (const op of results.crud) {
        const baselineOp = baseline.crud.find(b => b.operation === op.operation)
        if (baselineOp) {
          const diff = ((op.latencyMs.p50 - baselineOp.latencyMs.p50) / baselineOp.latencyMs.p50) * 100
          if (diff > threshold) {
            regressions.push(`crud/${op.operation}: ${diff.toFixed(1)}% slower (${baselineOp.latencyMs.p50}ms -> ${op.latencyMs.p50}ms)`)
            hasRegression = true
          }
        }
      }
    }

    // Compare queries
    if (results.query && baseline.query) {
      for (const q of results.query) {
        const baselineQ = baseline.query.find(b => b.queryType === q.queryType)
        if (baselineQ) {
          const diff = ((q.latencyMs.p50 - baselineQ.latencyMs.p50) / baselineQ.latencyMs.p50) * 100
          if (diff > threshold) {
            regressions.push(`query/${q.queryType}: ${diff.toFixed(1)}% slower (${baselineQ.latencyMs.p50}ms -> ${q.latencyMs.p50}ms)`)
            hasRegression = true
          }
        }
      }
    }

    if (hasRegression) {
      console.log('\nPerformance regressions detected:')
      for (const r of regressions) {
        console.log(`  - ${r}`)
      }
      return false
    } else {
      console.log('\nNo performance regressions detected.')
      return true
    }
  } catch (error) {
    console.error(`Failed to read baseline file: ${(error as Error).message}`)
    return false
  }
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()
  const startTime = performance.now()

  console.log('====================================')
  console.log('ParqueDB E2E Benchmark Runner')
  console.log('====================================')
  console.log(`Worker URL: ${config.url}`)
  console.log(`Benchmarks: ${config.benchmarks.join(', ')}`)
  console.log(`Iterations: ${config.iterations}`)
  console.log(`Warmup: ${config.warmup}`)
  console.log(`Started at: ${new Date().toISOString()}`)

  // Health check first
  const healthy = await runHealthCheck(config)
  if (!healthy) {
    console.error('\nHealth check failed. Worker may be down or unreachable.')
    process.exit(1)
  }

  const results: Partial<BenchmarkResults> = {}

  // Run benchmarks based on config
  if (config.benchmarks.includes('cold-start')) {
    results.coldStart = await runColdStartBenchmark(config)
  }

  if (config.benchmarks.includes('crud')) {
    results.crud = await runCrudBenchmarks(config)
  }

  if (config.benchmarks.includes('query')) {
    results.query = await runQueryBenchmarks(config)
  }

  if (config.benchmarks.includes('backend')) {
    results.backend = await runBackendBenchmarks(config)
  }

  const duration = performance.now() - startTime

  const fullResults: BenchmarkResults = {
    metadata: {
      url: config.url,
      timestamp: new Date().toISOString(),
      duration,
      config,
    },
    ...results,
    summary: calculateSummary(results),
  }

  // Output results
  switch (config.output) {
    case 'json':
      console.log(JSON.stringify(fullResults, null, 2))
      break
    case 'markdown':
      outputMarkdown(fullResults)
      break
    case 'table':
    default:
      outputTable(fullResults)
      break
  }

  // Compare against baseline if specified
  if (config.compareBaseline) {
    const passed = await compareBaseline(fullResults, config.compareBaseline, config.threshold)
    if (!passed) {
      process.exit(1)
    }
  }

  console.log(`\nCompleted at: ${new Date().toISOString()}`)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})

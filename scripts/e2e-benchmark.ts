#!/usr/bin/env bun
/**
 * E2E Benchmark Script - Node.js client to deployed ParqueDB Worker
 *
 * Tests real-world latency, throughput, and index speedups by calling
 * the deployed Worker endpoints with actual datasets.
 *
 * Usage:
 *   bun scripts/e2e-benchmark.ts [options]
 *
 * Options:
 *   --url=<url>         Worker URL (default: https://api.parquedb.com)
 *   --datasets=<list>   Comma-separated datasets (default: imdb,onet-full,unspsc-full)
 *   --iterations=<n>    Iterations per query (default: 3)
 *   --warmup=<n>        Warmup iterations (default: 1)
 *   --maxQueries=<n>    Max queries per dataset (default: 5)
 *   --concurrency=<n>   Concurrent requests (default: 1)
 *   --output=<format>   Output format: table, json (default: table)
 *   --verbose           Show detailed query results
 *   --help              Show this help
 *
 * Examples:
 *   bun scripts/e2e-benchmark.ts
 *   bun scripts/e2e-benchmark.ts --datasets=imdb --iterations=5
 *   bun scripts/e2e-benchmark.ts --url=http://localhost:8787 --verbose
 */

// =============================================================================
// Types
// =============================================================================

interface Config {
  url: string
  datasets: string[]
  iterations: number
  warmup: number
  maxQueries: number
  concurrency: number
  output: string
  verbose: boolean
}

interface FetchResult {
  success: boolean
  status?: number
  error?: string
  latency: number
  data?: unknown
  serverTiming?: string | null
}

interface PercentileStats {
  min: number
  max: number
  p50: number
  p95: number
  p99: number
  avg: number
}

interface BenchmarkResult {
  description: string
  url: string
  iterations: number
  warmup: number
  successCount: number
  latencyMs: PercentileStats
  lastResult?: FetchResult
}

interface ConcurrencyResult {
  concurrency: number
  successCount: number
  totalTimeMs: number
  throughput: number
  latencyMs: PercentileStats
}

interface AllResults {
  config: Config
  startedAt: string
  completedAt?: string
  health: FetchResult | null
  benchmark: BenchmarkResult | null
  benchmarkDatasets: BenchmarkResult[] | null
  benchmarkIndexed: BenchmarkResult | null
  concurrency: ConcurrencyResult | null
}

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_CONFIG: Config = {
  url: 'https://api.parquedb.com',
  datasets: ['imdb', 'onet-full', 'unspsc-full'],
  iterations: 3,
  warmup: 1,
  maxQueries: 5,
  concurrency: 1,
  output: 'table',
  verbose: false,
}

// Parse CLI arguments
function parseArgs(): Config {
  const args = process.argv.slice(2)
  const config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
E2E Benchmark - ParqueDB Worker Performance Testing

Usage: bun scripts/e2e-benchmark.ts [options]

Options:
  --url=<url>         Worker URL (default: ${DEFAULT_CONFIG.url})
  --datasets=<list>   Comma-separated datasets: imdb, imdb-1m, onet-full, unspsc-full
  --iterations=<n>    Iterations per query (default: ${DEFAULT_CONFIG.iterations})
  --warmup=<n>        Warmup iterations (default: ${DEFAULT_CONFIG.warmup})
  --maxQueries=<n>    Max queries per dataset (default: ${DEFAULT_CONFIG.maxQueries})
  --concurrency=<n>   Concurrent requests (default: ${DEFAULT_CONFIG.concurrency})
  --output=<format>   Output format: table, json (default: ${DEFAULT_CONFIG.output})
  --verbose           Show detailed query results
  --help              Show this help

Endpoints tested:
  /benchmark          - R2 I/O performance (v1 vs v3 shredding)
  /benchmark-datasets - Real dataset queries
  /benchmark-indexed  - Secondary index performance
`)
      process.exit(0)
    }

    const [key, value] = arg.split('=')
    switch (key) {
      case '--url':
        config.url = value
        break
      case '--datasets':
        config.datasets = value.split(',')
        break
      case '--iterations':
        config.iterations = parseInt(value, 10)
        break
      case '--warmup':
        config.warmup = parseInt(value, 10)
        break
      case '--maxQueries':
        config.maxQueries = parseInt(value, 10)
        break
      case '--concurrency':
        config.concurrency = parseInt(value, 10)
        break
      case '--output':
        config.output = value
        break
      case '--verbose':
        config.verbose = true
        break
    }
  }

  return config
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Run a single HTTP request and measure latency
 */
async function timedFetch(url: string, _options: RequestInit = {}): Promise<FetchResult> {
  const start = performance.now()
  try {
    const response = await fetch(url)
    const latency = performance.now() - start
    const data = await response.json()
    return {
      success: response.ok,
      status: response.status,
      latency,
      data,
      serverTiming: response.headers.get('Server-Timing'),
    }
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      latency: performance.now() - start,
    }
  }
}

/**
 * Calculate percentiles from an array of numbers
 */
function percentiles(values: number[]): PercentileStats {
  if (values.length === 0) return { min: 0, max: 0, p50: 0, p95: 0, p99: 0, avg: 0 }
  const sorted = [...values].sort((a, b) => a - b)
  const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length
  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    p50: sorted[Math.floor(sorted.length * 0.5)],
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1],
    p99: sorted[Math.floor(sorted.length * 0.99)] ?? sorted[sorted.length - 1],
    avg,
  }
}

/**
 * Run benchmark endpoint multiple times and collect metrics
 */
async function benchmarkEndpoint(url: string, iterations: number, warmup: number, description: string): Promise<BenchmarkResult> {
  const latencies: number[] = []
  const results: FetchResult[] = []

  // Warmup iterations (not counted)
  for (let i = 0; i < warmup; i++) {
    await timedFetch(url)
  }

  // Measured iterations
  for (let i = 0; i < iterations; i++) {
    const result = await timedFetch(url)
    if (result.success) {
      latencies.push(result.latency)
      results.push(result)
    }
  }

  return {
    description,
    url,
    iterations,
    warmup,
    successCount: latencies.length,
    latencyMs: percentiles(latencies),
    lastResult: results[results.length - 1],
  }
}

// =============================================================================
// Benchmark Tests
// =============================================================================

/**
 * Test /benchmark endpoint - R2 I/O performance
 */
async function testBenchmark(config: Config): Promise<BenchmarkResult> {
  console.log('\n--- /benchmark - R2 I/O Performance ---')

  // Use smaller sizes and fewer iterations for E2E to avoid timeouts
  const url = `${config.url}/benchmark?sizes=10000&iterations=2&rowGroupSize=5000`

  const result = await benchmarkEndpoint(
    url,
    config.iterations,
    config.warmup,
    'V1 vs V3 Parquet shredding comparison'
  )

  if (result.lastResult?.data) {
    const data = result.lastResult.data as Record<string, unknown>
    const summary = data.summary as Record<string, unknown> | undefined
    const datasets = data.datasets as Array<{ size: number; queries: Array<{ name: string; speedup: string; bytesReduction: string }> }> | undefined

    console.log(`  Total time: ${data.totalTimeMs}ms`)
    console.log(`  Average speedup: ${summary?.avgSpeedup}x`)

    const bestSpeedup = summary?.bestSpeedup as Record<string, unknown> | undefined
    console.log(`  Best speedup: ${bestSpeedup?.speedup}x (${bestSpeedup?.query})`)
    console.log(`  Bytes reduction: ${summary?.avgBytesReduction}%`)

    if (config.verbose && datasets) {
      console.log('\n  Detailed Results:')
      for (const ds of datasets) {
        console.log(`    Dataset ${ds.size.toLocaleString()} rows:`)
        for (const q of ds.queries) {
          console.log(`      ${q.name}: ${q.speedup}x speedup, ${q.bytesReduction} bytes saved`)
        }
      }
    }
  }

  return result
}

/**
 * Test /benchmark-datasets endpoint - Real dataset queries
 */
async function testBenchmarkDatasets(config: Config): Promise<BenchmarkResult[]> {
  console.log('\n--- /benchmark-datasets - Real Dataset Queries ---')

  const results: BenchmarkResult[] = []

  for (const dataset of config.datasets) {
    console.log(`\n  Dataset: ${dataset}`)

    const url = `${config.url}/benchmark-datasets?dataset=${dataset}&iterations=${config.iterations}&maxQueries=${config.maxQueries}&warmup=${config.warmup}`

    const result = await benchmarkEndpoint(
      url,
      1, // Only one outer iteration since the endpoint runs multiple internal iterations
      0,
      `Dataset benchmark for ${dataset}`
    )

    if (result.lastResult?.data) {
      const data = result.lastResult.data as Record<string, unknown>
      const summary = data.summary as Record<string, unknown> | undefined
      const datasets = data.datasets as Array<{ files: Array<{ name: string; size: string }>; queries: Array<{ name: string; latency: { median: string; p95: string } }> }> | undefined

      console.log(`    Total time: ${data.totalTimeMs}ms`)
      console.log(`    Queries executed: ${summary?.queriesExecuted}`)
      console.log(`    Avg latency: ${summary?.avgLatency}`)
      console.log(`    Total bytes read: ${summary?.totalBytesRead}`)

      if (config.verbose && datasets) {
        for (const ds of datasets) {
          console.log(`\n    Files:`)
          for (const f of ds.files) {
            console.log(`      ${f.name}: ${f.size}`)
          }
          console.log(`    Queries:`)
          for (const q of ds.queries) {
            console.log(`      ${q.name}: ${q.latency.median} (p95: ${q.latency.p95})`)
          }
        }
      }
    } else if (result.lastResult?.error) {
      const data = result.lastResult.data as Record<string, unknown> | undefined
      console.log(`    Error: ${data?.message || 'Unknown error'}`)
    }

    results.push(result)
  }

  return results
}

/**
 * Test /benchmark-indexed endpoint - Secondary index performance
 */
async function testBenchmarkIndexed(config: Config): Promise<BenchmarkResult> {
  console.log('\n--- /benchmark-indexed - Secondary Index Performance ---')

  const url = `${config.url}/benchmark-indexed?iterations=${config.iterations}&warmup=${config.warmup}&maxQueries=${config.maxQueries}&datasets=${config.datasets.join(',')}&includeScans=true`

  const result = await benchmarkEndpoint(
    url,
    1, // Only one outer iteration since the endpoint runs multiple internal iterations
    0,
    'Index vs scan performance comparison'
  )

  if (result.lastResult?.data) {
    const data = result.lastResult.data as Record<string, unknown>
    const metadata = data.metadata as Record<string, unknown> | undefined
    const summary = data.summary as Record<string, unknown> | undefined
    const queries = data.queries as Array<{
      query: { id: string }
      indexed: { success: boolean; error?: string; latencyMs: { p50: number } }
      scan: { latencyMs: { p50: number } }
      speedup: string
    }> | undefined
    const datasetSummaries = data.datasetSummaries as Array<{
      dataset: string
      queryCount: number
      avgSpeedup: string
      bestSpeedup: { queryId: string; speedup: string }
      avgIndexedLatencyMs: string
    }> | undefined

    console.log(`  Total time: ${data.totalTimeMs}ms`)
    console.log(`  Total queries: ${metadata?.totalQueries}`)
    console.log(`  Avg speedup: ${summary?.avgSpeedup}x`)
    console.log(`  Median speedup: ${summary?.medianSpeedup}x`)

    const bestOverall = summary?.bestOverall as Record<string, unknown> | undefined
    console.log(`  Best overall: ${bestOverall?.queryId} (${bestOverall?.speedup}x)`)
    console.log(`  Index beneficial rate: ${summary?.indexBeneficialRate}%`)

    console.log('\n  By Index Type:')
    const byIndexType = summary?.byIndexType as Record<string, { count: number; avgSpeedup: string }> | undefined
    if (byIndexType) {
      for (const [type, stats] of Object.entries(byIndexType)) {
        console.log(`    ${type}: ${stats.count} queries, ${stats.avgSpeedup}x avg speedup`)
      }
    }

    console.log('\n  By Query Category:')
    const byCategory = summary?.byCategory as Record<string, { count: number; avgSpeedup: string }> | undefined
    if (byCategory) {
      for (const [cat, stats] of Object.entries(byCategory)) {
        console.log(`    ${cat}: ${stats.count} queries, ${stats.avgSpeedup}x avg speedup`)
      }
    }

    if (config.verbose && queries) {
      console.log('\n  Query Details:')
      for (const q of queries) {
        const status = q.indexed.success ? 'OK' : 'FAIL'
        console.log(`    [${status}] ${q.query.id}: ${q.speedup}x (indexed: ${q.indexed.latencyMs.p50}ms, scan: ${q.scan.latencyMs.p50}ms)`)
        if (!q.indexed.success && q.indexed.error) {
          console.log(`         Error: ${q.indexed.error.substring(0, 100)}...`)
        }
      }
    }

    // Show dataset summaries
    if (datasetSummaries) {
      console.log('\n  Dataset Summaries:')
      for (const ds of datasetSummaries) {
        console.log(`    ${ds.dataset}:`)
        console.log(`      Queries: ${ds.queryCount}, Avg speedup: ${ds.avgSpeedup}x`)
        console.log(`      Best: ${ds.bestSpeedup.queryId} (${ds.bestSpeedup.speedup}x)`)
        console.log(`      Avg indexed latency: ${ds.avgIndexedLatencyMs}ms`)
      }
    }
  } else if (result.lastResult?.error) {
    const data = result.lastResult.data as Record<string, unknown> | undefined
    console.log(`  Error: ${data?.message || 'Unknown error'}`)
  }

  return result
}

/**
 * Test concurrent requests
 */
async function testConcurrency(config: Config): Promise<ConcurrencyResult | null> {
  if (config.concurrency <= 1) return null

  console.log(`\n--- Concurrency Test (${config.concurrency} parallel requests) ---`)

  const url = `${config.url}/benchmark-datasets?dataset=${config.datasets[0]}&iterations=1&maxQueries=2`

  const start = performance.now()
  const promises = Array(config.concurrency).fill(null).map(() => timedFetch(url))
  const results = await Promise.all(promises)
  const totalTime = performance.now() - start

  const successCount = results.filter(r => r.success).length
  const latencies = results.filter(r => r.success).map(r => r.latency)
  const stats = percentiles(latencies)

  console.log(`  Requests: ${config.concurrency}`)
  console.log(`  Successful: ${successCount}`)
  console.log(`  Total time: ${Math.round(totalTime)}ms`)
  console.log(`  Throughput: ${(successCount / (totalTime / 1000)).toFixed(2)} req/s`)
  console.log(`  Latency - min: ${Math.round(stats.min)}ms, p50: ${Math.round(stats.p50)}ms, p95: ${Math.round(stats.p95)}ms, max: ${Math.round(stats.max)}ms`)

  return {
    concurrency: config.concurrency,
    successCount,
    totalTimeMs: totalTime,
    throughput: successCount / (totalTime / 1000),
    latencyMs: stats,
  }
}

/**
 * Test health endpoint (sanity check)
 */
async function testHealth(config: Config): Promise<FetchResult | null> {
  console.log('--- Health Check ---')

  const result = await timedFetch(`${config.url}/health`)

  if (result.success) {
    const data = result.data as Record<string, unknown>
    const api = data?.api as Record<string, unknown> | undefined
    const user = data?.user as Record<string, unknown> | undefined
    console.log(`  Status: ${api?.status || 'unknown'}`)
    console.log(`  Latency: ${Math.round(result.latency)}ms`)
    console.log(`  Colo: ${user?.colo || 'unknown'}`)
  } else {
    console.log(`  Failed: ${result.error || result.status}`)
    return null
  }

  return result
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()

  console.log('====================================')
  console.log('ParqueDB E2E Benchmark')
  console.log('====================================')
  console.log(`Worker URL: ${config.url}`)
  console.log(`Datasets: ${config.datasets.join(', ')}`)
  console.log(`Iterations: ${config.iterations}`)
  console.log(`Warmup: ${config.warmup}`)
  console.log(`Max queries: ${config.maxQueries}`)
  console.log(`Concurrency: ${config.concurrency}`)
  console.log(`Started at: ${new Date().toISOString()}`)

  const allResults: AllResults = {
    config,
    startedAt: new Date().toISOString(),
    health: null,
    benchmark: null,
    benchmarkDatasets: null,
    benchmarkIndexed: null,
    concurrency: null,
  }

  try {
    // Health check first
    allResults.health = await testHealth(config)
    if (!allResults.health) {
      console.error('\nHealth check failed. Worker may be down or unreachable.')
      process.exit(1)
    }

    // Run benchmark tests
    allResults.benchmark = await testBenchmark(config)
    allResults.benchmarkDatasets = await testBenchmarkDatasets(config)
    allResults.benchmarkIndexed = await testBenchmarkIndexed(config)

    // Concurrency test
    allResults.concurrency = await testConcurrency(config)

  } catch (error) {
    console.error('\nBenchmark failed:', (error as Error).message)
    process.exit(1)
  }

  allResults.completedAt = new Date().toISOString()

  // Output summary
  console.log('\n====================================')
  console.log('Summary')
  console.log('====================================')

  if (allResults.benchmark?.lastResult?.data) {
    const data = allResults.benchmark.lastResult.data as Record<string, unknown>
    const s = data.summary as Record<string, unknown> | undefined
    if (s) {
      const bestSpeedup = s.bestSpeedup as Record<string, unknown> | undefined
      console.log(`\nR2 I/O (V3 vs V1):`)
      console.log(`  Avg speedup: ${s.avgSpeedup}x`)
      console.log(`  Best: ${bestSpeedup?.speedup}x`)
    }
  }

  if (allResults.benchmarkIndexed?.lastResult?.data) {
    const data = allResults.benchmarkIndexed.lastResult.data as Record<string, unknown>
    const s = data.summary as Record<string, unknown> | undefined
    if (s) {
      console.log(`\nSecondary Indexes:`)
      console.log(`  Avg speedup: ${s.avgSpeedup}x`)
      console.log(`  Median speedup: ${s.medianSpeedup}x`)
      console.log(`  Index beneficial: ${s.indexBeneficialRate}%`)
    }
  }

  if (allResults.concurrency) {
    console.log(`\nConcurrency (${allResults.concurrency.concurrency} parallel):`)
    console.log(`  Throughput: ${allResults.concurrency.throughput.toFixed(2)} req/s`)
    console.log(`  P50 latency: ${Math.round(allResults.concurrency.latencyMs.p50)}ms`)
  }

  // JSON output if requested
  if (config.output === 'json') {
    console.log('\n--- JSON Output ---')
    console.log(JSON.stringify(allResults, null, 2))
  }

  console.log(`\nCompleted at: ${allResults.completedAt}`)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})

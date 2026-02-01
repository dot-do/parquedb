#!/usr/bin/env node
/**
 * E2E Benchmark Script - Node.js client to deployed ParqueDB Worker
 *
 * Tests real-world latency, throughput, and index speedups by calling
 * the deployed Worker endpoints with actual datasets.
 *
 * Usage:
 *   node scripts/e2e-benchmark.mjs [options]
 *
 * Options:
 *   --url=<url>         Worker URL (default: https://parquedb.workers.do)
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
 *   node scripts/e2e-benchmark.mjs
 *   node scripts/e2e-benchmark.mjs --datasets=imdb --iterations=5
 *   node scripts/e2e-benchmark.mjs --url=http://localhost:8787 --verbose
 */

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_CONFIG = {
  url: 'https://parquedb.workers.do',
  datasets: ['imdb', 'onet-full', 'unspsc-full'],
  iterations: 3,
  warmup: 1,
  maxQueries: 5,
  concurrency: 1,
  output: 'table',
  verbose: false,
}

// Parse CLI arguments
function parseArgs() {
  const args = process.argv.slice(2)
  const config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
E2E Benchmark - ParqueDB Worker Performance Testing

Usage: node scripts/e2e-benchmark.mjs [options]

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
async function timedFetch(url, options = {}) {
  const start = performance.now()
  try {
    const response = await fetch(url, options)
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
      error: error.message,
      latency: performance.now() - start,
    }
  }
}

/**
 * Calculate percentiles from an array of numbers
 */
function percentiles(values) {
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
async function benchmarkEndpoint(url, iterations, warmup, description) {
  const latencies = []
  const results = []

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
async function testBenchmark(config) {
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
    const data = result.lastResult.data
    console.log(`  Total time: ${data.totalTimeMs}ms`)
    console.log(`  Average speedup: ${data.summary?.avgSpeedup}x`)
    console.log(`  Best speedup: ${data.summary?.bestSpeedup?.speedup}x (${data.summary?.bestSpeedup?.query})`)
    console.log(`  Bytes reduction: ${data.summary?.avgBytesReduction}%`)

    if (config.verbose && data.datasets) {
      console.log('\n  Detailed Results:')
      for (const ds of data.datasets) {
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
async function testBenchmarkDatasets(config) {
  console.log('\n--- /benchmark-datasets - Real Dataset Queries ---')

  const results = []

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
      const data = result.lastResult.data
      console.log(`    Total time: ${data.totalTimeMs}ms`)
      console.log(`    Queries executed: ${data.summary?.queriesExecuted}`)
      console.log(`    Avg latency: ${data.summary?.avgLatency}`)
      console.log(`    Total bytes read: ${data.summary?.totalBytesRead}`)

      if (config.verbose && data.datasets) {
        for (const ds of data.datasets) {
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
      console.log(`    Error: ${result.lastResult.data?.message || 'Unknown error'}`)
    }

    results.push(result)
  }

  return results
}

/**
 * Test /benchmark-indexed endpoint - Secondary index performance
 */
async function testBenchmarkIndexed(config) {
  console.log('\n--- /benchmark-indexed - Secondary Index Performance ---')

  const url = `${config.url}/benchmark-indexed?iterations=${config.iterations}&warmup=${config.warmup}&maxQueries=${config.maxQueries}&datasets=${config.datasets.join(',')}&includeScans=true`

  const result = await benchmarkEndpoint(
    url,
    1, // Only one outer iteration since the endpoint runs multiple internal iterations
    0,
    'Index vs scan performance comparison'
  )

  if (result.lastResult?.data) {
    const data = result.lastResult.data
    console.log(`  Total time: ${data.totalTimeMs}ms`)
    console.log(`  Total queries: ${data.metadata?.totalQueries}`)
    console.log(`  Avg speedup: ${data.summary?.avgSpeedup}x`)
    console.log(`  Median speedup: ${data.summary?.medianSpeedup}x`)
    console.log(`  Best overall: ${data.summary?.bestOverall?.queryId} (${data.summary?.bestOverall?.speedup}x)`)
    console.log(`  Index beneficial rate: ${data.summary?.indexBeneficialRate}%`)

    console.log('\n  By Index Type:')
    if (data.summary?.byIndexType) {
      for (const [type, stats] of Object.entries(data.summary.byIndexType)) {
        console.log(`    ${type}: ${stats.count} queries, ${stats.avgSpeedup}x avg speedup`)
      }
    }

    console.log('\n  By Query Category:')
    if (data.summary?.byCategory) {
      for (const [cat, stats] of Object.entries(data.summary.byCategory)) {
        console.log(`    ${cat}: ${stats.count} queries, ${stats.avgSpeedup}x avg speedup`)
      }
    }

    if (config.verbose && data.queries) {
      console.log('\n  Query Details:')
      for (const q of data.queries) {
        const status = q.indexed.success ? 'OK' : 'FAIL'
        console.log(`    [${status}] ${q.query.id}: ${q.speedup}x (indexed: ${q.indexed.latencyMs.p50}ms, scan: ${q.scan.latencyMs.p50}ms)`)
        if (!q.indexed.success && q.indexed.error) {
          console.log(`         Error: ${q.indexed.error.substring(0, 100)}...`)
        }
      }
    }

    // Show dataset summaries
    if (data.datasetSummaries) {
      console.log('\n  Dataset Summaries:')
      for (const ds of data.datasetSummaries) {
        console.log(`    ${ds.dataset}:`)
        console.log(`      Queries: ${ds.queryCount}, Avg speedup: ${ds.avgSpeedup}x`)
        console.log(`      Best: ${ds.bestSpeedup.queryId} (${ds.bestSpeedup.speedup}x)`)
        console.log(`      Avg indexed latency: ${ds.avgIndexedLatencyMs}ms`)
      }
    }
  } else if (result.lastResult?.error) {
    console.log(`  Error: ${result.lastResult.data?.message || 'Unknown error'}`)
  }

  return result
}

/**
 * Test concurrent requests
 */
async function testConcurrency(config) {
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
async function testHealth(config) {
  console.log('--- Health Check ---')

  const result = await timedFetch(`${config.url}/health`)

  if (result.success) {
    console.log(`  Status: ${result.data?.api?.status || 'unknown'}`)
    console.log(`  Latency: ${Math.round(result.latency)}ms`)
    console.log(`  Colo: ${result.data?.user?.colo || 'unknown'}`)
  } else {
    console.log(`  Failed: ${result.error || result.status}`)
    return null
  }

  return result
}

// =============================================================================
// Main
// =============================================================================

async function main() {
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

  const allResults = {
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
    console.error('\nBenchmark failed:', error.message)
    process.exit(1)
  }

  allResults.completedAt = new Date().toISOString()

  // Output summary
  console.log('\n====================================')
  console.log('Summary')
  console.log('====================================')

  if (allResults.benchmark?.lastResult?.data?.summary) {
    const s = allResults.benchmark.lastResult.data.summary
    console.log(`\nR2 I/O (V3 vs V1):`)
    console.log(`  Avg speedup: ${s.avgSpeedup}x`)
    console.log(`  Best: ${s.bestSpeedup?.speedup}x`)
  }

  if (allResults.benchmarkIndexed?.lastResult?.data?.summary) {
    const s = allResults.benchmarkIndexed.lastResult.data.summary
    console.log(`\nSecondary Indexes:`)
    console.log(`  Avg speedup: ${s.avgSpeedup}x`)
    console.log(`  Median speedup: ${s.medianSpeedup}x`)
    console.log(`  Index beneficial: ${s.indexBeneficialRate}%`)
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

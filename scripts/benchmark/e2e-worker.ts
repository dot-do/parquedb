#!/usr/bin/env npx tsx
/**
 * E2E Worker Benchmark
 *
 * Measures actual production performance by hitting deployed Worker endpoints.
 * This is what matters - not local benchmarks.
 *
 * Usage:
 *   npx tsx scripts/benchmark/e2e-worker.ts --url=https://parquedb.workers.do
 *   npx tsx scripts/benchmark/e2e-worker.ts --url=https://imdb.parquedb.com
 */

import { formatNumber } from '../lib/storage-modes'

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_URL = 'https://parquedb.workers.do'
const DEFAULT_ITERATIONS = 10
const WARMUP_ITERATIONS = 2

interface QuerySpec {
  name: string
  description: string
  path: string
  expectedRows?: number
  category: 'list' | 'entity' | 'filter' | 'aggregation' | 'search'
}

// Queries to benchmark against IMDB dataset
const IMDB_QUERIES: QuerySpec[] = [
  // List queries
  {
    name: 'list_titles_default',
    description: 'List titles with default pagination',
    path: '/datasets/imdb/titles',
    category: 'list',
  },
  {
    name: 'list_titles_limit_100',
    description: 'List titles with limit=100',
    path: '/datasets/imdb/titles?limit=100',
    expectedRows: 100,
    category: 'list',
  },
  {
    name: 'list_titles_limit_1000',
    description: 'List titles with limit=1000',
    path: '/datasets/imdb/titles?limit=1000',
    expectedRows: 1000,
    category: 'list',
  },

  // Entity lookups
  {
    name: 'entity_lookup_first',
    description: 'Get first title entity',
    path: '/datasets/imdb/titles/tt0000001',
    expectedRows: 1,
    category: 'entity',
  },
  {
    name: 'entity_lookup_middle',
    description: 'Get middle title entity',
    path: '/datasets/imdb/titles/tt0050000',
    expectedRows: 1,
    category: 'entity',
  },
  {
    name: 'entity_lookup_recent',
    description: 'Get recent title entity',
    path: '/datasets/imdb/titles/tt0099999',
    expectedRows: 1,
    category: 'entity',
  },

  // Filtered queries
  {
    name: 'filter_titleType_movie',
    description: 'Filter by titleType=movie',
    path: '/datasets/imdb/titles?titleType=movie&limit=100',
    category: 'filter',
  },
  {
    name: 'filter_titleType_tvSeries',
    description: 'Filter by titleType=tvSeries',
    path: '/datasets/imdb/titles?titleType=tvSeries&limit=100',
    category: 'filter',
  },
  {
    name: 'filter_startYear_2020',
    description: 'Filter by startYear=2020',
    path: '/datasets/imdb/titles?startYear=2020&limit=100',
    category: 'filter',
  },
  {
    name: 'filter_isAdult_false',
    description: 'Filter by isAdult=false',
    path: '/datasets/imdb/titles?isAdult=false&limit=100',
    category: 'filter',
  },

  // Search queries
  {
    name: 'search_name',
    description: 'Search by name pattern',
    path: '/datasets/imdb/titles?name=Star&limit=100',
    category: 'search',
  },
]

// Queries for O*NET dataset
const ONET_QUERIES: QuerySpec[] = [
  {
    name: 'list_occupations',
    description: 'List all occupations',
    path: '/datasets/onet/occupations',
    category: 'list',
  },
  {
    name: 'entity_occupation',
    description: 'Get single occupation',
    path: '/datasets/onet/occupations/15-1252.00',
    category: 'entity',
  },
  {
    name: 'list_skills',
    description: 'List all skills',
    path: '/datasets/onet/skills',
    category: 'list',
  },
]

// =============================================================================
// Types
// =============================================================================

interface BenchmarkResult {
  query: string
  category: string
  iterations: number
  timing: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
  }
  throughput: {
    requestsPerSecond: number
  }
  response: {
    statusCode: number
    contentLength: number
    rowCount?: number
  }
  serverTiming?: {
    total?: number
    query?: number
    cache?: string
  }
  errors: number
}

// =============================================================================
// Utilities
// =============================================================================

function percentile(arr: number[], p: number): number {
  const sorted = [...arr].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)] ?? 0
}

async function fetchWithTiming(url: string): Promise<{
  status: number
  contentLength: number
  body: any
  latencyMs: number
  serverTiming?: Record<string, string | number>
}> {
  const start = performance.now()
  const res = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'User-Agent': 'ParqueDB-Benchmark/1.0',
    },
  })
  const latencyMs = performance.now() - start

  const body = await res.json()
  const contentLength = parseInt(res.headers.get('content-length') || '0') || JSON.stringify(body).length

  // Parse Server-Timing header if present
  let serverTiming: Record<string, string | number> | undefined
  const serverTimingHeader = res.headers.get('server-timing')
  if (serverTimingHeader) {
    serverTiming = {}
    for (const part of serverTimingHeader.split(',')) {
      const [name, ...rest] = part.trim().split(';')
      const durMatch = rest.join(';').match(/dur=(\d+(?:\.\d+)?)/)
      if (durMatch) {
        serverTiming[name!.trim()] = parseFloat(durMatch[1]!)
      }
    }
  }

  return { status: res.status, contentLength, body, latencyMs, serverTiming }
}

// =============================================================================
// Benchmark Runner
// =============================================================================

async function runBenchmark(
  baseUrl: string,
  query: QuerySpec,
  iterations: number,
  warmup: number
): Promise<BenchmarkResult> {
  const url = `${baseUrl}${query.path}`
  const latencies: number[] = []
  let lastResponse: Awaited<ReturnType<typeof fetchWithTiming>> | null = null
  let errors = 0

  // Warmup
  for (let i = 0; i < warmup; i++) {
    try {
      await fetchWithTiming(url)
    } catch {
      // Ignore warmup errors
    }
  }

  // Actual benchmark
  for (let i = 0; i < iterations; i++) {
    try {
      const result = await fetchWithTiming(url)
      latencies.push(result.latencyMs)
      lastResponse = result
    } catch (e) {
      errors++
    }
  }

  if (latencies.length === 0) {
    return {
      query: query.name,
      category: query.category,
      iterations,
      timing: { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 },
      throughput: { requestsPerSecond: 0 },
      response: { statusCode: 0, contentLength: 0 },
      errors,
    }
  }

  const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length

  // Extract row count from response
  let rowCount: number | undefined
  if (lastResponse?.body?.data) {
    if (Array.isArray(lastResponse.body.data)) {
      rowCount = lastResponse.body.data.length
    } else {
      rowCount = 1
    }
  }
  if (lastResponse?.body?.api?.total !== undefined) {
    rowCount = lastResponse.body.api.total
  }

  return {
    query: query.name,
    category: query.category,
    iterations,
    timing: {
      min: Math.round(Math.min(...latencies) * 100) / 100,
      max: Math.round(Math.max(...latencies) * 100) / 100,
      avg: Math.round(avg * 100) / 100,
      p50: Math.round(percentile(latencies, 50) * 100) / 100,
      p95: Math.round(percentile(latencies, 95) * 100) / 100,
      p99: Math.round(percentile(latencies, 99) * 100) / 100,
    },
    throughput: {
      requestsPerSecond: Math.round((1000 / avg) * 100) / 100,
    },
    response: {
      statusCode: lastResponse?.status ?? 0,
      contentLength: lastResponse?.contentLength ?? 0,
      rowCount,
    },
    serverTiming: lastResponse?.serverTiming ? {
      total: lastResponse.serverTiming['total'] as number,
      query: lastResponse.serverTiming['query'] as number,
      cache: lastResponse.serverTiming['cache'] as string,
    } : undefined,
    errors,
  }
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)
  const urlArg = args.find(a => a.startsWith('--url='))
  const iterArg = args.find(a => a.startsWith('--iterations='))
  const datasetArg = args.find(a => a.startsWith('--dataset='))
  const jsonOutput = args.includes('--json')

  const baseUrl = urlArg ? urlArg.split('=')[1]! : DEFAULT_URL
  const iterations = iterArg ? parseInt(iterArg.split('=')[1]!) : DEFAULT_ITERATIONS
  const dataset = datasetArg ? datasetArg.split('=')[1]! : 'imdb'

  const queries = dataset === 'onet' ? ONET_QUERIES : IMDB_QUERIES

  if (!jsonOutput) {
    console.log('=== E2E Worker Benchmark ===')
    console.log(`URL: ${baseUrl}`)
    console.log(`Dataset: ${dataset}`)
    console.log(`Iterations: ${iterations} (+ ${WARMUP_ITERATIONS} warmup)`)
    console.log('')
  }

  const results: BenchmarkResult[] = []

  for (const query of queries) {
    if (!jsonOutput) {
      process.stdout.write(`  ${query.name}... `)
    }

    const result = await runBenchmark(baseUrl, query, iterations, WARMUP_ITERATIONS)
    results.push(result)

    if (!jsonOutput) {
      if (result.errors > 0) {
        console.log(`ERROR (${result.errors}/${iterations} failed)`)
      } else {
        console.log(`p50=${result.timing.p50}ms, p95=${result.timing.p95}ms, ${result.throughput.requestsPerSecond} req/s`)
      }
    }
  }

  if (jsonOutput) {
    console.log(JSON.stringify({
      url: baseUrl,
      dataset,
      iterations,
      timestamp: new Date().toISOString(),
      results,
    }, null, 2))
    return
  }

  // Print summary table
  console.log('\n' + '='.repeat(100))
  console.log('SUMMARY')
  console.log('='.repeat(100))
  console.log('Query                          | Category    | p50 (ms) | p95 (ms) | p99 (ms) | req/s    | Status')
  console.log('-------------------------------|-------------|----------|----------|----------|----------|--------')

  for (const r of results) {
    const name = r.query.padEnd(30)
    const cat = r.category.padEnd(11)
    const p50 = r.timing.p50.toFixed(0).padStart(8)
    const p95 = r.timing.p95.toFixed(0).padStart(8)
    const p99 = r.timing.p99.toFixed(0).padStart(8)
    const rps = r.throughput.requestsPerSecond.toFixed(1).padStart(8)
    const status = r.errors > 0 ? `ERR(${r.errors})` : r.response.statusCode.toString()
    console.log(`${name} | ${cat} | ${p50} | ${p95} | ${p99} | ${rps} | ${status}`)
  }

  // Category summaries
  console.log('\n' + '='.repeat(60))
  console.log('BY CATEGORY')
  console.log('='.repeat(60))

  const categories = [...new Set(results.map(r => r.category))]
  for (const cat of categories) {
    const catResults = results.filter(r => r.category === cat && r.errors === 0)
    if (catResults.length === 0) continue

    const avgP50 = catResults.reduce((s, r) => s + r.timing.p50, 0) / catResults.length
    const avgP95 = catResults.reduce((s, r) => s + r.timing.p95, 0) / catResults.length
    const avgRps = catResults.reduce((s, r) => s + r.throughput.requestsPerSecond, 0) / catResults.length

    console.log(`${cat.padEnd(12)}: p50=${avgP50.toFixed(0)}ms, p95=${avgP95.toFixed(0)}ms, ${avgRps.toFixed(1)} req/s avg`)
  }

  // Check for regressions against targets
  console.log('\n' + '='.repeat(60))
  console.log('PERFORMANCE TARGETS')
  console.log('='.repeat(60))

  const targets: Record<string, { p50: number; p95: number }> = {
    entity: { p50: 50, p95: 100 },
    list: { p50: 100, p95: 200 },
    filter: { p50: 150, p95: 300 },
    search: { p50: 200, p95: 400 },
  }

  let hasRegression = false
  for (const cat of categories) {
    const target = targets[cat]
    if (!target) continue

    const catResults = results.filter(r => r.category === cat && r.errors === 0)
    const avgP50 = catResults.reduce((s, r) => s + r.timing.p50, 0) / catResults.length
    const avgP95 = catResults.reduce((s, r) => s + r.timing.p95, 0) / catResults.length

    const p50Status = avgP50 <= target.p50 ? '✓' : '✗'
    const p95Status = avgP95 <= target.p95 ? '✓' : '✗'

    if (avgP50 > target.p50 || avgP95 > target.p95) hasRegression = true

    console.log(`${cat.padEnd(12)}: p50=${avgP50.toFixed(0)}ms ${p50Status} (target: ${target.p50}ms), p95=${avgP95.toFixed(0)}ms ${p95Status} (target: ${target.p95}ms)`)
  }

  if (hasRegression) {
    console.log('\n⚠️  PERFORMANCE REGRESSION DETECTED')
    process.exit(1)
  } else {
    console.log('\n✓ All performance targets met')
  }
}

main().catch(console.error)

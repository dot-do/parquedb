#!/usr/bin/env npx tsx
/**
 * Benchmark script for search workers
 *
 * Tests CPU time, wall time, and throughput across different:
 * - Query types (simple, fuzzy, complex)
 * - Dataset sizes
 * - Feature combinations
 */

const BASE_URL = 'https://cdn.workers.do'
const VERSIONS = ['v6', 'v7', 'v8', 'v9']
const DATASETS = ['imdb', 'onet', 'unspsc']

interface BenchmarkResult {
  version: string
  dataset: string
  queryType: string
  query: string
  wallMs: number
  cpuMs?: number
  total: number
  status: 'ok' | 'error'
  error?: string
}

const QUERIES: Record<string, { type: string; params: string }[]> = {
  imdb: [
    { type: 'simple', params: 'q=matrix' },
    { type: 'fuzzy', params: 'q=matrx~' },
    { type: 'filter', params: 'type=movie&year_gte=2000' },
    { type: 'facets', params: 'q=love&facets=genres,titleType' },
    { type: 'stats', params: 'type=movie&stats=startYear,runtimeMinutes' },
    { type: 'complex', params: 'q=love&type=movie&facets=genres&stats=startYear&sort=startYear:desc' },
    { type: 'wildcard', params: 'q=mat*' },
    { type: 'negation', params: 'q=matrix -reloaded' },
  ],
  onet: [
    { type: 'simple', params: 'q=engineer' },
    { type: 'fuzzy', params: 'q=enginear~' },
    { type: 'prefix', params: 'q=soft' },
  ],
  unspsc: [
    { type: 'simple', params: 'q=computer' },
    { type: 'filter', params: 'segment=43' },
  ],
}

async function runBenchmark(
  version: string,
  dataset: string,
  queryType: string,
  params: string
): Promise<BenchmarkResult> {
  const url = `${BASE_URL}/search-${version}/${dataset}?${params}&timing=true`
  const start = performance.now()

  try {
    const response = await fetch(url)
    const wallMs = performance.now() - start
    const data = await response.json() as {
      total?: number
      timing?: { cpuMs?: number; wallMs?: number }
      error?: string
    }

    if (data.error) {
      return {
        version, dataset, queryType, query: params,
        wallMs, total: 0, status: 'error', error: data.error
      }
    }

    return {
      version, dataset, queryType, query: params,
      wallMs: Math.round(wallMs),
      cpuMs: data.timing?.cpuMs,
      total: data.total || 0,
      status: 'ok'
    }
  } catch (e) {
    return {
      version, dataset, queryType, query: params,
      wallMs: performance.now() - start,
      total: 0, status: 'error', error: String(e)
    }
  }
}

async function runAllBenchmarks(): Promise<void> {
  console.log('=== ParqueDB Search Worker Benchmarks ===\n')
  console.log(`Date: ${new Date().toISOString()}`)
  console.log(`Versions: ${VERSIONS.join(', ')}`)
  console.log(`Datasets: ${DATASETS.join(', ')}\n`)

  const results: BenchmarkResult[] = []

  for (const version of VERSIONS) {
    console.log(`\n--- Testing ${version} ---`)

    for (const dataset of DATASETS) {
      const queries = QUERIES[dataset] || []

      for (const { type, params } of queries) {
        // Run 3 times and take median
        const runs: BenchmarkResult[] = []
        for (let i = 0; i < 3; i++) {
          const result = await runBenchmark(version, dataset, type, params)
          runs.push(result)
          await new Promise(r => setTimeout(r, 100)) // Rate limit
        }

        // Take median by wall time
        runs.sort((a, b) => a.wallMs - b.wallMs)
        const median = runs[1]!
        results.push(median)

        const cpuStr = median.cpuMs !== undefined ? `${median.cpuMs}ms` : 'N/A'
        const statusIcon = median.status === 'ok' ? '✓' : '✗'

        console.log(
          `  ${statusIcon} ${dataset}/${type}: ` +
          `wall=${median.wallMs}ms, cpu=${cpuStr}, results=${median.total}`
        )
      }
    }
  }

  // Summary table
  console.log('\n\n=== Summary ===\n')
  console.log('Version | Dataset | Query Type | Wall (ms) | CPU (ms) | Results')
  console.log('--------|---------|------------|-----------|----------|--------')

  for (const r of results) {
    if (r.status === 'ok') {
      console.log(
        `${r.version.padEnd(7)} | ${r.dataset.padEnd(7)} | ${r.queryType.padEnd(10)} | ` +
        `${String(r.wallMs).padStart(9)} | ${String(r.cpuMs ?? 'N/A').padStart(8)} | ${r.total}`
      )
    }
  }

  // Aggregate stats
  console.log('\n\n=== Aggregate Stats ===\n')

  for (const version of VERSIONS) {
    const vResults = results.filter(r => r.version === version && r.status === 'ok')
    if (!vResults.length) continue

    const wallTimes = vResults.map(r => r.wallMs)
    const avgWall = Math.round(wallTimes.reduce((a, b) => a + b, 0) / wallTimes.length)
    const maxWall = Math.max(...wallTimes)
    const minWall = Math.min(...wallTimes)

    const cpuTimes = vResults.filter(r => r.cpuMs !== undefined).map(r => r.cpuMs!)
    const avgCpu = cpuTimes.length
      ? Math.round(cpuTimes.reduce((a, b) => a + b, 0) / cpuTimes.length * 100) / 100
      : 'N/A'

    console.log(`${version}:`)
    console.log(`  Wall time: avg=${avgWall}ms, min=${minWall}ms, max=${maxWall}ms`)
    console.log(`  CPU time:  avg=${avgCpu}ms`)
    console.log()
  }
}

// CPU stress test
async function cpuStressTest(): Promise<void> {
  console.log('\n=== CPU Stress Test ===\n')
  console.log('Testing v9 with increasingly complex queries...\n')

  const stressQueries = [
    { name: '1 term', params: 'q=matrix' },
    { name: '3 terms', params: 'q=matrix+reloaded+revolution' },
    { name: '5 terms', params: 'q=matrix+reloaded+revolution+neo+morpheus' },
    { name: 'fuzzy', params: 'q=matrx~' },
    { name: 'wildcard', params: 'q=mat*' },
    { name: 'facets x2', params: 'q=love&facets=genres,titleType' },
    { name: 'facets x3', params: 'q=love&facets=genres,titleType,startYear' },
    { name: 'stats', params: 'q=love&stats=startYear,runtimeMinutes' },
    { name: 'full combo', params: 'q=love~&facets=genres,titleType&stats=startYear&sort=startYear:desc' },
  ]

  console.log('Query | Wall (ms) | CPU (ms) | Budget Used | Exceeded')
  console.log('------|-----------|----------|-------------|----------')

  for (const { name, params } of stressQueries) {
    const url = `${BASE_URL}/search-v9/imdb?${params}&timing=true`
    const response = await fetch(url)
    const data = await response.json() as {
      timing?: { wallMs?: number; cpuMs?: number }
      cpuBudget?: { used: number; limit: number; exceeded: boolean }
    }

    const wall = data.timing?.wallMs ?? 'N/A'
    const cpu = data.timing?.cpuMs ?? 'N/A'
    const used = data.cpuBudget?.used ?? 'N/A'
    const exceeded = data.cpuBudget?.exceeded ? 'YES' : 'no'

    console.log(
      `${name.padEnd(12)} | ${String(wall).padStart(9)} | ${String(cpu).padStart(8)} | ` +
      `${String(used).padStart(11)} | ${exceeded}`
    )

    await new Promise(r => setTimeout(r, 200))
  }
}

// Main
async function main() {
  const args = process.argv.slice(2)

  if (args.includes('--stress')) {
    await cpuStressTest()
  } else if (args.includes('--quick')) {
    // Quick test - just v9 on imdb
    console.log('Quick benchmark: v9 on imdb\n')
    for (const { type, params } of QUERIES.imdb!) {
      const result = await runBenchmark('v9', 'imdb', type, params)
      console.log(`${type}: wall=${result.wallMs}ms, cpu=${result.cpuMs}ms, results=${result.total}`)
    }
  } else {
    await runAllBenchmarks()
    await cpuStressTest()
  }
}

main().catch(console.error)

#!/usr/bin/env tsx
/**
 * E2E Benchmark Runner for Deployed Workers
 *
 * Runs comprehensive benchmarks against deployed Cloudflare Workers and
 * generates detailed reports with latency percentiles and performance validation.
 *
 * Usage:
 *   npx tsx tests/e2e/benchmarks/runner.ts [options]
 *
 * Options:
 *   --url=<url>          Worker URL to benchmark (default: https://api.parquedb.com)
 *   --iterations=<n>     Number of iterations per test (default: 10)
 *   --warmup=<n>         Warmup iterations (default: 2)
 *   --output=<format>    Output format: table, json, markdown (default: table)
 *   --verbose            Enable verbose logging
 *   --datasets=<list>    Comma-separated datasets to test (default: imdb,onet-full)
 *   --skip-cold-start    Skip cold start tests
 *   --skip-cache         Skip cache tests
 *   --concurrency=<n>    Max concurrent requests to test (default: 25)
 *
 * Examples:
 *   npx tsx tests/e2e/benchmarks/runner.ts --url=https://staging.parquedb.com
 *   npx tsx tests/e2e/benchmarks/runner.ts --iterations=20 --output=markdown
 *   WORKER_URL=https://my-worker.workers.dev npx tsx tests/e2e/benchmarks/runner.ts
 */

import {
  timedFetch,
  runBenchmarkIterations,
  runConcurrentRequests,
  calculateLatencyStats,
  buildTestResult,
  formatMs,
  formatPercent,
  formatSpeedup,
  printResultTable,
  printSuiteSummary,
  detectRegressions,
  generateRunId,
  sleep,
  withRetry,
} from './utils'
import type {
  E2EBenchmarkConfig,
  LatencyStats,
  RequestResult,
  BenchmarkTestResult,
  BenchmarkCategory,
  ColdStartResult,
  CachePerformanceResult,
  ConcurrencyResult,
  DatasetBenchmarkResult,
  E2EBenchmarkSuiteResult,
} from './types'
import { DEFAULT_E2E_CONFIG } from './types'

// =============================================================================
// CLI Argument Parsing
// =============================================================================

function parseArgs(): E2EBenchmarkConfig {
  const args = process.argv.slice(2)
  const config: E2EBenchmarkConfig = { ...DEFAULT_E2E_CONFIG }

  for (const arg of args) {
    if (arg.startsWith('--url=')) {
      config.url = arg.slice(6)
    } else if (arg.startsWith('--iterations=')) {
      config.iterations = parseInt(arg.slice(13), 10)
    } else if (arg.startsWith('--warmup=')) {
      config.warmup = parseInt(arg.slice(9), 10)
    } else if (arg.startsWith('--output=')) {
      config.output = arg.slice(9) as 'table' | 'json' | 'markdown'
    } else if (arg === '--verbose') {
      config.verbose = true
    } else if (arg.startsWith('--datasets=')) {
      config.datasets = arg.slice(11).split(',')
    } else if (arg === '--skip-cold-start') {
      config.includeColdStart = false
    } else if (arg === '--skip-cache') {
      config.includeCacheTests = false
    } else if (arg.startsWith('--concurrency=')) {
      config.concurrency = parseInt(arg.slice(14), 10)
    } else if (arg.startsWith('--timeout=')) {
      config.timeout = parseInt(arg.slice(10), 10)
    }
  }

  // Check environment variable
  if (process.env.WORKER_URL && !args.some(a => a.startsWith('--url='))) {
    config.url = process.env.WORKER_URL
  }

  return config
}

// =============================================================================
// Performance Targets (from CLAUDE.md)
// =============================================================================

const PERFORMANCE_TARGETS = {
  getById: { p50: 5, p99: 20, name: 'Get by ID' },
  findIndexed: { p50: 20, p99: 100, name: 'Find (indexed)' },
  findScan: { p50: 100, p99: 500, name: 'Find (scan)' },
  create: { p50: 10, p99: 50, name: 'Create' },
  update: { p50: 15, p99: 75, name: 'Update' },
  relationshipTraverse: { p50: 50, p99: 200, name: 'Relationship traverse' },
} as const

// =============================================================================
// Test Data
// =============================================================================

interface TestPost {
  $type: string
  name: string
  title: string
  content: string
  status: string
  views: number
  tags: string[]
}

function generatePost(index: number): TestPost {
  return {
    $type: 'Post',
    name: `Benchmark Post ${index}`,
    title: `Benchmark Test Post ${index}: ${Date.now()}`,
    content: `Benchmark content for post ${index}. Lorem ipsum dolor sit amet.`,
    status: index % 3 === 0 ? 'published' : 'draft',
    views: Math.floor(Math.random() * 10000),
    tags: ['benchmark', 'test', `tag-${index % 5}`],
  }
}

// =============================================================================
// Benchmark Runner Class
// =============================================================================

class E2EBenchmarkRunner {
  private config: E2EBenchmarkConfig
  private results: BenchmarkTestResult[] = []
  private createdIds: string[] = []
  private startTime: number = 0

  constructor(config: E2EBenchmarkConfig) {
    this.config = config
  }

  /**
   * Make a request to the worker
   */
  private async request(
    path: string,
    options: RequestInit = {}
  ): Promise<RequestResult> {
    const url = `${this.config.url}${path}`
    return timedFetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    }, this.config.timeout)
  }

  /**
   * Create an entity
   */
  private async createEntity(namespace: string, data: Record<string, unknown>): Promise<RequestResult> {
    return this.request(`/api/${namespace}`, {
      method: 'POST',
      body: JSON.stringify(data),
    })
  }

  /**
   * Get an entity
   */
  private async getEntity(namespace: string, id: string): Promise<RequestResult> {
    return this.request(`/api/${namespace}/${id}`)
  }

  /**
   * Update an entity
   */
  private async updateEntity(
    namespace: string,
    id: string,
    update: Record<string, unknown>
  ): Promise<RequestResult> {
    return this.request(`/api/${namespace}/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(update),
    })
  }

  /**
   * Delete an entity
   */
  private async deleteEntity(namespace: string, id: string): Promise<RequestResult> {
    return this.request(`/api/${namespace}/${id}`, {
      method: 'DELETE',
    })
  }

  /**
   * Find entities
   */
  private async findEntities(
    namespace: string,
    filter?: Record<string, unknown>,
    options?: { limit?: number }
  ): Promise<RequestResult> {
    const params = new URLSearchParams()
    if (filter) {
      params.set('filter', JSON.stringify(filter))
    }
    if (options?.limit) {
      params.set('limit', String(options.limit))
    }
    const query = params.toString()
    return this.request(`/api/${namespace}${query ? `?${query}` : ''}`)
  }

  /**
   * Run a benchmark test
   */
  private async runBenchmark(
    name: string,
    description: string,
    category: BenchmarkCategory,
    fn: () => Promise<RequestResult>
  ): Promise<BenchmarkTestResult> {
    const results: RequestResult[] = []

    // Warmup
    for (let i = 0; i < this.config.warmup; i++) {
      await fn()
    }

    // Measured iterations
    for (let i = 0; i < this.config.iterations; i++) {
      results.push(await fn())
    }

    const testResult = buildTestResult(name, description, category, results)
    this.results.push(testResult)

    if (this.config.verbose) {
      console.log(`  ${name}: ${formatMs(testResult.latency.p50)} (p50), ${formatMs(testResult.latency.p99)} (p99)`)
    }

    return testResult
  }

  /**
   * Run cold start benchmark
   */
  private async runColdStartBenchmark(): Promise<ColdStartResult> {
    console.log('\n--- Cold Start Benchmark ---')

    // Wait for potential isolate eviction
    await sleep(5000)

    // First request (cold)
    const coldResult = await this.request('/health')
    const coldLatencyMs = coldResult.latencyMs

    // Warm requests
    const warmLatencies: number[] = []
    for (let i = 0; i < 5; i++) {
      const result = await this.request('/health')
      warmLatencies.push(result.latencyMs)
    }

    const warmLatencyMs = warmLatencies.reduce((a, b) => a + b, 0) / warmLatencies.length
    const overheadMs = coldLatencyMs - warmLatencyMs
    const overheadPercent = (overheadMs / warmLatencyMs) * 100

    const result: ColdStartResult = {
      coldLatencyMs,
      warmLatencyMs,
      overheadMs,
      overheadPercent,
      colo: coldResult.cfColo,
      warmSamples: warmLatencies.length,
    }

    console.log(`  Cold: ${formatMs(coldLatencyMs)}`)
    console.log(`  Warm: ${formatMs(warmLatencyMs)}`)
    console.log(`  Overhead: ${formatMs(overheadMs)} (${overheadPercent.toFixed(0)}%)`)

    return result
  }

  /**
   * Run cache performance benchmark
   */
  private async runCacheBenchmark(): Promise<CachePerformanceResult> {
    console.log('\n--- Cache Performance Benchmark ---')

    const cachePath = `/api/posts?limit=10&_cache_test=${Date.now()}`

    // First request (cache miss)
    const missResults: RequestResult[] = []
    for (let i = 0; i < this.config.iterations; i++) {
      const uniquePath = `/api/posts?limit=10&_cache_bust=${Date.now()}-${i}`
      missResults.push(await this.request(uniquePath))
    }
    const missLatency = calculateLatencyStats(missResults.map(r => r.latencyMs))

    // Repeated requests (cache hits)
    const hitPath = `/api/posts?status=published&limit=10`
    const hitResults: RequestResult[] = []
    for (let i = 0; i < this.config.iterations * 2; i++) {
      hitResults.push(await this.request(hitPath))
    }
    const hitLatency = calculateLatencyStats(hitResults.map(r => r.latencyMs))

    // Calculate cache hit rate
    const cacheHits = hitResults.filter(r => r.cacheStatus === 'HIT').length
    const hitRate = hitResults.length > 0 ? cacheHits / hitResults.length : 0

    const speedup = hitLatency.p50 > 0 ? missLatency.p50 / hitLatency.p50 : 1

    const result: CachePerformanceResult = {
      missLatency,
      hitLatency,
      speedup,
      hitRate,
      totalRequests: missResults.length + hitResults.length,
    }

    console.log(`  Miss (p50): ${formatMs(missLatency.p50)}`)
    console.log(`  Hit (p50): ${formatMs(hitLatency.p50)}`)
    console.log(`  Speedup: ${formatSpeedup(speedup)}`)
    console.log(`  Hit Rate: ${formatPercent(hitRate)}`)

    return result
  }

  /**
   * Run concurrency benchmarks
   */
  private async runConcurrencyBenchmarks(): Promise<ConcurrencyResult[]> {
    console.log('\n--- Concurrency Benchmarks ---')

    const levels = [1, 5, 10, 25, 50]
    const results: ConcurrencyResult[] = []

    for (const concurrency of levels) {
      if (concurrency > this.config.concurrency) continue

      const start = performance.now()
      const responses = await runConcurrentRequests(
        `${this.config.url}/api/posts?limit=10`,
        concurrency,
        {},
        this.config.timeout
      )
      const totalTimeMs = performance.now() - start

      const successCount = responses.filter(r => r.success).length
      const failureCount = responses.length - successCount
      const latencies = responses.filter(r => r.success).map(r => r.latencyMs)

      const result: ConcurrencyResult = {
        concurrency,
        successCount,
        failureCount,
        totalTimeMs,
        throughput: (successCount / totalTimeMs) * 1000,
        latency: calculateLatencyStats(latencies),
        errorRate: failureCount / responses.length,
      }

      results.push(result)

      console.log(`  ${concurrency} concurrent: ${formatMs(result.latency.p50)} p50, ${result.throughput.toFixed(1)} req/s, ${formatPercent(result.errorRate)} errors`)
    }

    return results
  }

  /**
   * Run CRUD benchmarks
   */
  private async runCrudBenchmarks(): Promise<void> {
    console.log('\n--- CRUD Benchmarks ---')

    // Create
    await this.runBenchmark(
      'create',
      'Create single entity',
      'warm-request',
      async () => {
        const result = await this.createEntity('posts', generatePost(Date.now()))
        if (result.success && result.data) {
          const post = result.data as { $id?: string }
          if (post.$id) {
            this.createdIds.push(post.$id)
          }
        }
        return result
      }
    )

    // Get by ID
    await this.runBenchmark(
      'getById',
      'Get entity by ID',
      'warm-request',
      async () => {
        if (this.createdIds.length > 0) {
          const id = this.createdIds[Math.floor(Math.random() * this.createdIds.length)]
          return this.getEntity('posts', id.replace('posts/', ''))
        }
        return { success: false, latencyMs: 0, error: 'No entities available' }
      }
    )

    // Update
    await this.runBenchmark(
      'update',
      'Update entity with $set',
      'warm-request',
      async () => {
        if (this.createdIds.length > 0) {
          const id = this.createdIds[Math.floor(Math.random() * this.createdIds.length)]
          return this.updateEntity('posts', id.replace('posts/', ''), {
            $set: { views: Math.floor(Math.random() * 10000) },
          })
        }
        return { success: false, latencyMs: 0, error: 'No entities available' }
      }
    )

    // Delete
    await this.runBenchmark(
      'delete',
      'Delete single entity',
      'warm-request',
      async () => {
        const createResult = await this.createEntity('posts', generatePost(Date.now()))
        if (createResult.success && createResult.data) {
          const post = createResult.data as { $id?: string }
          if (post.$id) {
            return this.deleteEntity('posts', post.$id.replace('posts/', ''))
          }
        }
        return createResult
      }
    )
  }

  /**
   * Run query benchmarks
   */
  private async runQueryBenchmarks(): Promise<void> {
    console.log('\n--- Query Benchmarks ---')

    // Find all
    await this.runBenchmark(
      'findAll',
      'Find all entities (limit 100)',
      'full-scan',
      () => this.findEntities('posts', {}, { limit: 100 })
    )

    // Find indexed
    await this.runBenchmark(
      'findIndexed',
      'Find with equality filter',
      'index-lookup',
      () => this.findEntities('posts', { status: 'published' })
    )

    // Find range
    await this.runBenchmark(
      'findRange',
      'Find with range filter',
      'query-range',
      () => this.findEntities('posts', { views: { $gt: 1000, $lt: 5000 } })
    )

    // Find compound
    await this.runBenchmark(
      'findCompound',
      'Find with $and filter',
      'query-compound',
      () => this.findEntities('posts', {
        $and: [
          { status: 'published' },
          { views: { $gt: 100 } },
        ],
      })
    )
  }

  /**
   * Run relationship benchmarks
   */
  private async runRelationshipBenchmarks(): Promise<void> {
    console.log('\n--- Relationship Benchmarks ---')

    await this.runBenchmark(
      'populate',
      'Find with populate',
      'warm-request',
      () => this.request('/api/posts?populate=author&limit=10')
    )
  }

  /**
   * Validate results against performance targets
   */
  private validatePerformanceTargets(): void {
    console.log('\n--- Performance Target Validation ---')

    const validations = [
      { key: 'getById', result: this.results.find(r => r.name === 'getById') },
      { key: 'findIndexed', result: this.results.find(r => r.name === 'findIndexed') },
      { key: 'findAll', result: this.results.find(r => r.name === 'findAll'), targetKey: 'findScan' },
      { key: 'create', result: this.results.find(r => r.name === 'create') },
      { key: 'update', result: this.results.find(r => r.name === 'update') },
      { key: 'populate', result: this.results.find(r => r.name === 'populate'), targetKey: 'relationshipTraverse' },
    ]

    const targetKeys = Object.keys(PERFORMANCE_TARGETS) as (keyof typeof PERFORMANCE_TARGETS)[]

    for (const { key, result, targetKey } of validations) {
      const tKey = (targetKey || key) as keyof typeof PERFORMANCE_TARGETS
      const target = PERFORMANCE_TARGETS[tKey]
      if (!target || !result) continue

      const p50Pass = result.latency.p50 <= target.p50
      const p99Pass = result.latency.p99 <= target.p99
      const status = p50Pass && p99Pass ? 'PASS' : 'FAIL'

      console.log(`  ${target.name}:`)
      console.log(`    p50: ${formatMs(result.latency.p50)} (target: <${target.p50}ms) [${p50Pass ? 'OK' : 'FAIL'}]`)
      console.log(`    p99: ${formatMs(result.latency.p99)} (target: <${target.p99}ms) [${p99Pass ? 'OK' : 'FAIL'}]`)
    }
  }

  /**
   * Clean up test data
   */
  private async cleanup(): Promise<void> {
    console.log('\n--- Cleanup ---')
    let deleted = 0
    for (const id of this.createdIds) {
      try {
        await this.deleteEntity('posts', id.replace('posts/', ''))
        deleted++
      } catch {
        // Ignore cleanup errors
      }
    }
    console.log(`  Deleted ${deleted}/${this.createdIds.length} test entities`)
    this.createdIds = []
  }

  /**
   * Build the final suite result
   */
  private buildSuiteResult(
    coldStart?: ColdStartResult,
    cachePerformance?: CachePerformanceResult,
    concurrency?: ConcurrencyResult[]
  ): E2EBenchmarkSuiteResult {
    const endTime = performance.now()
    const durationMs = endTime - this.startTime

    // Calculate summary stats
    const allLatencies = this.results.flatMap(r => r.rawLatencies)
    const overallStats = calculateLatencyStats(allLatencies)
    const totalTests = this.results.length
    const passedTests = this.results.filter(r => r.failureCount === 0).length

    return {
      config: this.config,
      metadata: {
        startedAt: new Date(this.startTime).toISOString(),
        completedAt: new Date().toISOString(),
        durationMs,
        workerUrl: this.config.url,
        runnerVersion: '1.0.0',
      },
      health: {
        success: true,
        latencyMs: 0,
      },
      coldStart,
      cachePerformance,
      datasets: [],
      concurrency,
      summary: {
        totalTests,
        passedTests,
        failedTests: totalTests - passedTests,
        avgLatencyMs: overallStats.mean,
        p95LatencyMs: overallStats.p95,
        overallThroughput: this.results.reduce((sum, r) => sum + r.throughput, 0) / totalTests,
        cacheSpeedup: cachePerformance?.speedup,
      },
    }
  }

  /**
   * Run the full benchmark suite
   */
  async run(): Promise<E2EBenchmarkSuiteResult> {
    this.startTime = performance.now()

    console.log('=' .repeat(70))
    console.log('E2E BENCHMARK SUITE - DEPLOYED WORKERS')
    console.log('='.repeat(70))
    console.log(`Worker URL: ${this.config.url}`)
    console.log(`Iterations: ${this.config.iterations}`)
    console.log(`Warmup: ${this.config.warmup}`)
    console.log(`Output: ${this.config.output}`)
    console.log('='.repeat(70))

    // Health check
    console.log('\n--- Health Check ---')
    const healthResult = await withRetry(() => this.request('/health'), 3, 1000)
    if (!healthResult.success) {
      throw new Error(`Worker not available at ${this.config.url}: ${healthResult.error}`)
    }
    console.log(`  Status: OK (${formatMs(healthResult.latencyMs)})`)

    // Seed test data
    console.log('\n--- Seeding Test Data ---')
    for (let i = 0; i < 20; i++) {
      const result = await this.createEntity('posts', generatePost(i))
      if (result.success && result.data) {
        const post = result.data as { $id?: string }
        if (post.$id) {
          this.createdIds.push(post.$id)
        }
      }
    }
    console.log(`  Created ${this.createdIds.length} test entities`)

    // Run benchmarks
    let coldStart: ColdStartResult | undefined
    let cachePerformance: CachePerformanceResult | undefined
    let concurrency: ConcurrencyResult[] | undefined

    if (this.config.includeColdStart) {
      coldStart = await this.runColdStartBenchmark()
    }

    await this.runCrudBenchmarks()
    await this.runQueryBenchmarks()
    await this.runRelationshipBenchmarks()

    if (this.config.includeCacheTests) {
      cachePerformance = await this.runCacheBenchmark()
    }

    concurrency = await this.runConcurrencyBenchmarks()

    // Validate against targets
    this.validatePerformanceTargets()

    // Cleanup
    await this.cleanup()

    // Build and return results
    const suiteResult = this.buildSuiteResult(coldStart, cachePerformance, concurrency)

    // Output results
    if (this.config.output === 'json') {
      console.log('\n' + JSON.stringify(suiteResult, null, 2))
    } else if (this.config.output === 'markdown') {
      this.printMarkdown(suiteResult)
    } else {
      printSuiteSummary(suiteResult)
    }

    return suiteResult
  }

  /**
   * Print results as markdown
   */
  private printMarkdown(result: E2EBenchmarkSuiteResult): void {
    console.log('\n## E2E Benchmark Results\n')
    console.log(`**Worker URL:** ${result.metadata.workerUrl}`)
    console.log(`**Duration:** ${formatMs(result.metadata.durationMs)}`)
    console.log(`**Date:** ${result.metadata.completedAt}\n`)

    console.log('### Summary\n')
    console.log(`| Metric | Value |`)
    console.log(`|--------|-------|`)
    console.log(`| Total Tests | ${result.summary.totalTests} |`)
    console.log(`| Passed | ${result.summary.passedTests} |`)
    console.log(`| Failed | ${result.summary.failedTests} |`)
    console.log(`| Avg Latency | ${formatMs(result.summary.avgLatencyMs)} |`)
    console.log(`| P95 Latency | ${formatMs(result.summary.p95LatencyMs)} |`)
    console.log('')

    if (result.coldStart) {
      console.log('### Cold Start\n')
      console.log(`| Metric | Value |`)
      console.log(`|--------|-------|`)
      console.log(`| Cold | ${formatMs(result.coldStart.coldLatencyMs)} |`)
      console.log(`| Warm | ${formatMs(result.coldStart.warmLatencyMs)} |`)
      console.log(`| Overhead | ${formatMs(result.coldStart.overheadMs)} (${result.coldStart.overheadPercent.toFixed(0)}%) |`)
      console.log('')
    }

    console.log('### Latency by Operation\n')
    console.log(`| Operation | Mean | P50 | P95 | P99 |`)
    console.log(`|-----------|------|-----|-----|-----|`)
    for (const r of this.results) {
      console.log(`| ${r.name} | ${formatMs(r.latency.mean)} | ${formatMs(r.latency.p50)} | ${formatMs(r.latency.p95)} | ${formatMs(r.latency.p99)} |`)
    }
    console.log('')

    if (result.concurrency && result.concurrency.length > 0) {
      console.log('### Concurrency\n')
      console.log(`| Concurrent | P50 | Throughput | Error Rate |`)
      console.log(`|------------|-----|------------|------------|`)
      for (const c of result.concurrency) {
        console.log(`| ${c.concurrency} | ${formatMs(c.latency.p50)} | ${c.throughput.toFixed(1)}/s | ${formatPercent(c.errorRate)} |`)
      }
    }
  }
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()
  const runner = new E2EBenchmarkRunner(config)

  try {
    const results = await runner.run()

    // Exit with error code if any tests failed
    if (results.summary.failedTests > 0) {
      process.exit(1)
    }
  } catch (error) {
    console.error('Benchmark failed:', error)
    process.exit(1)
  }
}

// Run if executed directly
if (process.argv[1]?.endsWith('runner.ts')) {
  main()
}

export { E2EBenchmarkRunner }

/**
 * E2E Benchmark Runner
 *
 * Runs comprehensive benchmarks against a deployed ParqueDB worker.
 * Measures cold start latency, warm request latency, and cache performance.
 */

import { calculateStats, formatStats, formatLatency } from './stats'
import type {
  BenchmarkConfig,
  BenchmarkReport,
  BenchmarkError,
  ColdStartResults,
  ColdStartMeasurement,
  WarmRequestResults,
  WarmRequestMeasurement,
  CachePerformanceResults,
  ServerColdStartResponse,
  ServerHealthResponse,
  ServerCrudResponse,
} from './types'
import { defaultConfig } from './types'

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Extract Server-Timing header value
 */
function getServerTiming(response: Response): number {
  const timing = response.headers.get('Server-Timing')
  if (!timing) return 0

  // Parse "total;dur=123" format
  const match = timing.match(/dur=(\d+(?:\.\d+)?)/)
  return match ? parseFloat(match[1]) : 0
}

/**
 * Check if response indicates a cache hit
 */
function isCacheHit(response: Response): boolean {
  const cacheStatus = response.headers.get('CF-Cache-Status')
  return cacheStatus === 'HIT'
}

/**
 * Log message if verbose mode is enabled
 */
function log(config: BenchmarkConfig, ...args: unknown[]): void {
  if (config.verbose) {
    console.log(...args)
  }
}

// =============================================================================
// Cold Start Benchmarks
// =============================================================================

/**
 * Measure cold start latency
 *
 * To measure cold starts, we need to wait long enough for the isolate to be
 * evicted (typically 30-60 seconds of inactivity on Cloudflare Workers).
 * We use a unique cache buster to avoid CF edge caching.
 */
async function measureColdStart(
  config: BenchmarkConfig,
  errors: BenchmarkError[]
): Promise<ColdStartResults> {
  console.log('\n=== Cold Start Benchmark ===')
  console.log(`Running ${config.coldStartIterations} iterations with ${config.coldStartDelay / 1000}s delay between each`)

  const measurements: ColdStartMeasurement[] = []

  for (let i = 0; i < config.coldStartIterations; i++) {
    if (i > 0) {
      console.log(`Waiting ${config.coldStartDelay / 1000}s for isolate teardown...`)
      await sleep(config.coldStartDelay)
    }

    console.log(`Cold start measurement ${i + 1}/${config.coldStartIterations}`)

    // Use cache buster and no-store to ensure fresh request
    const url = `${config.baseUrl}/benchmark/e2e/cold-start?_cb=${Date.now()}-${Math.random()}`

    try {
      const start = performance.now()
      const response = await fetch(url, {
        headers: {
          'Cache-Control': 'no-store',
        },
      })
      const totalMs = Math.round(performance.now() - start)

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`)
      }

      const data = (await response.json()) as ServerColdStartResponse
      const serverMs = getServerTiming(response)

      measurements.push({
        totalMs,
        serverMs: serverMs || data.coldStartMs,
        metadata: data.metadata,
      })

      log(config, `  Total: ${totalMs}ms, Server: ${data.coldStartMs}ms, Colo: ${data.metadata.colo}`)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      console.error(`  Error: ${message}`)
      errors.push({
        phase: 'cold-start',
        endpoint: url,
        error: message,
        timestamp: new Date().toISOString(),
      })
    }
  }

  const stats = calculateStats(measurements.map(m => m.totalMs))
  const serverStats = calculateStats(measurements.map(m => m.serverMs))

  console.log('\nCold Start Results:')
  console.log(formatStats(stats, 'Total (client measured)'))
  console.log(formatStats(serverStats, 'Server (Server-Timing)'))

  return { measurements, stats, serverStats }
}

// =============================================================================
// Warm Request Benchmarks
// =============================================================================

/**
 * Measure warm request latency for a specific endpoint
 */
async function measureWarmRequests(
  config: BenchmarkConfig,
  endpoint: string,
  label: string,
  errors: BenchmarkError[]
): Promise<WarmRequestResults> {
  console.log(`\n--- ${label} ---`)

  const url = `${config.baseUrl}${endpoint}`
  const measurements: WarmRequestMeasurement[] = []

  // Warmup requests
  console.log(`Warmup: ${config.warmupRequests} requests`)
  for (let i = 0; i < config.warmupRequests; i++) {
    try {
      await fetch(url)
    } catch {
      // Ignore warmup errors
    }
  }

  // Measured requests
  console.log(`Measuring: ${config.warmIterations} requests`)
  for (let i = 0; i < config.warmIterations; i++) {
    try {
      const start = performance.now()
      const response = await fetch(url)
      const totalMs = Math.round((performance.now() - start) * 100) / 100

      measurements.push({
        totalMs,
        serverMs: getServerTiming(response),
        cacheHit: isCacheHit(response),
        statusCode: response.status,
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      errors.push({
        phase: 'warm-requests',
        endpoint: url,
        error: message,
        timestamp: new Date().toISOString(),
      })
    }
  }

  const stats = calculateStats(measurements.map(m => m.totalMs))
  const serverStats = calculateStats(measurements.map(m => m.serverMs).filter(m => m > 0))
  const cacheHits = measurements.filter(m => m.cacheHit).length
  const cacheHitRate = measurements.length > 0 ? cacheHits / measurements.length : 0

  console.log(`  p50: ${formatLatency(stats.p50)}, p95: ${formatLatency(stats.p95)}, avg: ${formatLatency(stats.avg)}`)
  console.log(`  Cache hit rate: ${Math.round(cacheHitRate * 100)}%`)

  return {
    endpoint,
    measurements,
    stats,
    serverStats,
    cacheHitRate,
  }
}

/**
 * Run all warm request benchmarks
 */
async function runWarmBenchmarks(
  config: BenchmarkConfig,
  errors: BenchmarkError[]
): Promise<{
  health: WarmRequestResults
  crud: {
    create: WarmRequestResults
    read: WarmRequestResults
  }
}> {
  console.log('\n=== Warm Request Benchmarks ===')

  const health = await measureWarmRequests(
    config,
    '/benchmark/e2e/health',
    'Health Check',
    errors
  )

  const create = await measureWarmRequests(
    config,
    '/benchmark/e2e/crud/create?iterations=1&warmup=0',
    'CRUD Create',
    errors
  )

  const read = await measureWarmRequests(
    config,
    '/benchmark/e2e/crud/read?iterations=1&warmup=0',
    'CRUD Read',
    errors
  )

  return { health, crud: { create, read } }
}

// =============================================================================
// Cache Performance Benchmarks
// =============================================================================

/**
 * Measure cache hit vs miss performance
 */
async function measureCachePerformance(
  config: BenchmarkConfig,
  errors: BenchmarkError[]
): Promise<CachePerformanceResults | undefined> {
  console.log('\n=== Cache Performance ===')

  // Cache miss measurements (use unique cache buster each time)
  const missLatencies: number[] = []
  console.log('Measuring cache misses...')

  for (let i = 0; i < 10; i++) {
    const url = `${config.baseUrl}/benchmark/e2e/health?_cb=${Date.now()}-${i}`
    try {
      const start = performance.now()
      await fetch(url, {
        headers: { 'Cache-Control': 'no-store' },
      })
      missLatencies.push(Math.round((performance.now() - start) * 100) / 100)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      errors.push({
        phase: 'cache-performance',
        endpoint: url,
        error: message,
        timestamp: new Date().toISOString(),
      })
    }
  }

  // Cache hit measurements (use same URL repeatedly)
  const hitLatencies: number[] = []
  console.log('Measuring cache hits...')

  const cacheableUrl = `${config.baseUrl}/health`
  // Seed the cache
  await fetch(cacheableUrl).catch(() => {})

  for (let i = 0; i < 10; i++) {
    try {
      const start = performance.now()
      const response = await fetch(cacheableUrl)
      const latency = Math.round((performance.now() - start) * 100) / 100

      // Only count actual cache hits
      if (isCacheHit(response)) {
        hitLatencies.push(latency)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      errors.push({
        phase: 'cache-performance',
        endpoint: cacheableUrl,
        error: message,
        timestamp: new Date().toISOString(),
      })
    }
  }

  if (hitLatencies.length === 0) {
    console.log('  No cache hits detected (edge caching may be disabled)')
    return undefined
  }

  const missStats = calculateStats(missLatencies)
  const hitStats = calculateStats(hitLatencies)
  const improvement = missStats.avg > 0 ? ((missStats.avg - hitStats.avg) / missStats.avg) * 100 : 0

  console.log(`  Cache miss avg: ${formatLatency(missStats.avg)}`)
  console.log(`  Cache hit avg: ${formatLatency(hitStats.avg)}`)
  console.log(`  Improvement: ${Math.round(improvement)}%`)

  return {
    missLatency: missStats,
    hitLatency: hitStats,
    improvementPercent: Math.round(improvement * 100) / 100,
  }
}

// =============================================================================
// Main Runner
// =============================================================================

/**
 * Run the complete E2E benchmark suite
 */
export async function runBenchmarks(
  userConfig: Partial<BenchmarkConfig> = {}
): Promise<BenchmarkReport> {
  const config: BenchmarkConfig = { ...defaultConfig, ...userConfig }
  const errors: BenchmarkError[] = []
  const startTime = new Date()

  console.log('========================================')
  console.log('ParqueDB E2E Benchmark Suite')
  console.log('========================================')
  console.log(`Target: ${config.baseUrl}`)
  console.log(`Started: ${startTime.toISOString()}`)

  // Verify connectivity
  console.log('\nVerifying connectivity...')
  try {
    const response = await fetch(`${config.baseUrl}/benchmark/e2e/health`)
    if (!response.ok) {
      throw new Error(`Health check failed: HTTP ${response.status}`)
    }
    const health = (await response.json()) as ServerHealthResponse
    console.log(`  Connected to ${health.metadata.colo} - R2: ${health.checks.r2.status}, Cache: ${health.checks.cache.status}`)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    console.error(`  Failed to connect: ${message}`)
    throw new Error(`Cannot connect to ${config.baseUrl}: ${message}`)
  }

  // Run benchmarks
  const coldStart = await measureColdStart(config, errors)
  const warmRequests = await runWarmBenchmarks(config, errors)
  const cachePerformance = await measureCachePerformance(config, errors)

  const endTime = new Date()
  const durationMs = endTime.getTime() - startTime.getTime()

  // Build report
  const report: BenchmarkReport = {
    meta: {
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      durationMs,
      baseUrl: config.baseUrl,
      config: {
        coldStartIterations: config.coldStartIterations,
        coldStartDelay: config.coldStartDelay,
        warmIterations: config.warmIterations,
        warmupRequests: config.warmupRequests,
        verbose: config.verbose,
        outputPath: config.outputPath,
      },
    },
    coldStart,
    warmRequests,
    cachePerformance,
    errors,
  }

  // Print summary
  console.log('\n========================================')
  console.log('Summary')
  console.log('========================================')
  console.log(`Duration: ${Math.round(durationMs / 1000)}s`)
  console.log(`Cold start p50: ${formatLatency(coldStart.stats.p50)}`)
  console.log(`Warm health p50: ${formatLatency(warmRequests.health.stats.p50)}`)
  console.log(`Warm CRUD read p50: ${formatLatency(warmRequests.crud.read.stats.p50)}`)
  if (cachePerformance) {
    console.log(`Cache improvement: ${cachePerformance.improvementPercent}%`)
  }
  console.log(`Errors: ${errors.length}`)

  return report
}

/**
 * Parse command line arguments
 */
export function parseArgs(args: string[]): Partial<BenchmarkConfig> {
  const config: Partial<BenchmarkConfig> = {}

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    const next = args[i + 1]

    switch (arg) {
      case '--url':
      case '-u':
        if (next) {
          config.baseUrl = next
          i++
        }
        break
      case '--cold-iterations':
      case '-c':
        if (next) {
          config.coldStartIterations = parseInt(next, 10)
          i++
        }
        break
      case '--cold-delay':
        if (next) {
          config.coldStartDelay = parseInt(next, 10) * 1000 // Convert to ms
          i++
        }
        break
      case '--warm-iterations':
      case '-w':
        if (next) {
          config.warmIterations = parseInt(next, 10)
          i++
        }
        break
      case '--output':
      case '-o':
        if (next) {
          config.outputPath = next
          i++
        }
        break
      case '--verbose':
      case '-v':
        config.verbose = true
        break
    }
  }

  return config
}

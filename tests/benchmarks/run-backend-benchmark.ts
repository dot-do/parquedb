/**
 * Comprehensive Backend Benchmark Suite
 *
 * Benchmarks ParqueDB across all storage backends:
 * - Local filesystem (FsBackend)
 * - R2 (Cloudflare)
 * - Iceberg format
 * - Delta Lake format
 *
 * Run with: npx tsx tests/benchmarks/run-backend-benchmark.ts
 *
 * Options:
 *   --backend=local|r2|iceberg|delta|all  (default: all)
 *   --iterations=N                         (default: 10)
 *   --sizes=1000,10000,50000              (default: 1000,10000)
 *   --output=table|json|markdown          (default: table)
 */

import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { IcebergBackend } from '../../src/backends/iceberg'
import { DeltaBackend } from '../../src/backends/delta'
import type { StorageBackend } from '../../src/types/storage'
import type { Entity, CreateInput } from '../../src/types/entity'

// =============================================================================
// Configuration
// =============================================================================

interface BenchmarkConfig {
  backends: BackendType[]
  sizes: number[]
  iterations: number
  warmup: number
  output: 'table' | 'json' | 'markdown'
}

type BackendType = 'memory' | 'iceberg' | 'delta'

const DEFAULT_CONFIG: BenchmarkConfig = {
  backends: ['memory', 'iceberg', 'delta'],
  sizes: [1000, 10000],
  iterations: 10,
  warmup: 2,
  output: 'table',
}

// =============================================================================
// Statistics
// =============================================================================

interface BenchmarkStats {
  name: string
  backend: BackendType
  datasetSize: number
  iterations: number
  samples: number[]
  mean: number
  median: number
  min: number
  max: number
  stdDev: number
  p50: number
  p95: number
  p99: number
  opsPerSecond: number
}

function calculateStats(
  name: string,
  backend: BackendType,
  datasetSize: number,
  samples: number[]
): BenchmarkStats {
  if (samples.length === 0) {
    return {
      name,
      backend,
      datasetSize,
      iterations: 0,
      samples: [],
      mean: 0,
      median: 0,
      min: 0,
      max: 0,
      stdDev: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      opsPerSecond: 0,
    }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = samples.reduce((a, b) => a + b, 0)
  const mean = sum / n

  const variance = samples.reduce((acc, s) => acc + Math.pow(s - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))]!
  }

  return {
    name,
    backend,
    datasetSize,
    iterations: n,
    samples,
    mean,
    median: percentile(50),
    min: sorted[0]!,
    max: sorted[n - 1]!,
    stdDev,
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    opsPerSecond: mean > 0 ? 1000 / mean : 0,
  }
}

// =============================================================================
// Data Generation
// =============================================================================

const CATEGORIES = ['electronics', 'clothing', 'books', 'home', 'sports', 'toys', 'food', 'auto']
const STATUSES = ['active', 'inactive', 'pending', 'archived']

interface Product {
  $type: string
  name: string
  category: string
  status: string
  price: number
  rating: number
  createdYear: number
  description: string
}

function generateProducts(count: number): Product[] {
  const products: Product[] = []
  const payload = 'x'.repeat(100)

  for (let i = 0; i < count; i++) {
    products.push({
      $type: 'Product',
      name: `Product ${i}`,
      category: CATEGORIES[i % CATEGORIES.length]!,
      status: STATUSES[i % STATUSES.length]!,
      price: 10 + (i % 990),
      rating: 1 + (i % 50) / 10,
      createdYear: 2015 + (i % 10),
      description: `Product ${i} description. ${payload}`,
    })
  }

  return products
}

// =============================================================================
// Backend Factory
// =============================================================================

interface BackendInstance {
  type: BackendType
  backend: IcebergBackend | DeltaBackend
  storage: StorageBackend
  cleanup: () => Promise<void>
}

async function createBackend(type: BackendType): Promise<BackendInstance> {
  const storage = new MemoryBackend()

  switch (type) {
    case 'memory': {
      // Use Iceberg with memory storage as baseline
      const backend = new IcebergBackend({
        storage,
        warehouse: 'benchmark',
        database: 'test',
      })
      await backend.initialize()
      return {
        type,
        backend,
        storage,
        cleanup: async () => {
          await backend.close()
        },
      }
    }

    case 'iceberg': {
      const backend = new IcebergBackend({
        storage,
        warehouse: 'benchmark-iceberg',
        database: 'test',
      })
      await backend.initialize()
      return {
        type,
        backend,
        storage,
        cleanup: async () => {
          await backend.close()
        },
      }
    }

    case 'delta': {
      const backend = new DeltaBackend({
        storage,
        location: 'benchmark-delta',
      })
      await backend.initialize()
      return {
        type,
        backend,
        storage,
        cleanup: async () => {
          await backend.close()
        },
      }
    }
  }
}

// =============================================================================
// Benchmark Runner
// =============================================================================

async function runBenchmark(
  name: string,
  backend: BackendType,
  datasetSize: number,
  fn: () => Promise<void>,
  options: { iterations: number; warmup: number }
): Promise<BenchmarkStats> {
  const { iterations, warmup } = options
  const samples: number[] = []

  // Warmup
  for (let i = 0; i < warmup; i++) {
    await fn()
  }

  // Measure
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    samples.push(performance.now() - start)
  }

  return calculateStats(name, backend, datasetSize, samples)
}

// =============================================================================
// Benchmark Suite
// =============================================================================

interface SuiteResult {
  config: BenchmarkConfig
  results: BenchmarkStats[]
  summary: {
    backendComparison: Record<BackendType, {
      avgWriteMs: number
      avgReadMs: number
      avgQueryMs: number
    }>
    bestBackend: {
      write: BackendType
      read: BackendType
      query: BackendType
    }
  }
}

async function runBackendBenchmarks(config: BenchmarkConfig): Promise<SuiteResult> {
  const results: BenchmarkStats[] = []
  const backendStats: Record<BackendType, { writes: number[]; reads: number[]; queries: number[] }> = {
    memory: { writes: [], reads: [], queries: [] },
    iceberg: { writes: [], reads: [], queries: [] },
    delta: { writes: [], reads: [], queries: [] },
  }

  console.log('='.repeat(80))
  console.log('ParqueDB Backend Benchmark Suite')
  console.log('='.repeat(80))
  console.log(`\nBackends: ${config.backends.join(', ')}`)
  console.log(`Dataset sizes: ${config.sizes.join(', ')}`)
  console.log(`Iterations: ${config.iterations} (warmup: ${config.warmup})`)
  console.log('')

  for (const backendType of config.backends) {
    console.log(`\n${'─'.repeat(80)}`)
    console.log(`Backend: ${backendType.toUpperCase()}`)
    console.log('─'.repeat(80))

    for (const size of config.sizes) {
      console.log(`\n  Dataset size: ${size.toLocaleString()} products`)

      const instance = await createBackend(backendType)
      const products = generateProducts(size)

      try {
        // Benchmark: Bulk Write
        console.log('  • Bulk write...')
        const writeResult = await runBenchmark(
          'Bulk Write',
          backendType,
          size,
          async () => {
            // Create fresh backend for each iteration
            const freshInstance = await createBackend(backendType)
            const batch = products.slice(0, Math.min(100, size))
            for (const product of batch) {
              await freshInstance.backend.create('products', product as unknown as CreateInput)
            }
            await freshInstance.cleanup()
          },
          { iterations: config.iterations, warmup: config.warmup }
        )
        results.push(writeResult)
        backendStats[backendType].writes.push(writeResult.p50)
        console.log(`    p50: ${writeResult.p50.toFixed(2)}ms, p95: ${writeResult.p95.toFixed(2)}ms`)

        // Setup: Write all products for read tests and collect their IDs
        const createdEntityIds: string[] = []
        for (const product of products) {
          const entity = await instance.backend.create('products', product as unknown as CreateInput)
          // Extract just the ID part (e.g., 'products/abc123' -> 'abc123')
          const parts = entity.$id.split('/')
          createdEntityIds.push(parts[parts.length - 1] ?? entity.$id)
        }

        // Benchmark: Point Read
        console.log('  • Point read...')
        let readIdx = 0
        const readResult = await runBenchmark(
          'Point Read',
          backendType,
          size,
          async () => {
            const id = createdEntityIds[readIdx % createdEntityIds.length]
            readIdx++
            if (id) {
              await instance.backend.get('products', id)
            }
          },
          { iterations: config.iterations * 5, warmup: config.warmup }
        )
        results.push(readResult)
        backendStats[backendType].reads.push(readResult.p50)
        console.log(`    p50: ${readResult.p50.toFixed(2)}ms, p95: ${readResult.p95.toFixed(2)}ms`)

        // Benchmark: Filtered Query (category = 'electronics')
        console.log('  • Filtered query (category)...')
        const queryResult = await runBenchmark(
          'Filtered Query (category)',
          backendType,
          size,
          async () => {
            await instance.backend.find('products', { category: 'electronics' }, { limit: 100 })
          },
          { iterations: config.iterations, warmup: config.warmup }
        )
        results.push(queryResult)
        backendStats[backendType].queries.push(queryResult.p50)
        console.log(`    p50: ${queryResult.p50.toFixed(2)}ms, p95: ${queryResult.p95.toFixed(2)}ms`)

        // Benchmark: Range Query (price > 500)
        console.log('  • Range query (price)...')
        const rangeResult = await runBenchmark(
          'Range Query (price)',
          backendType,
          size,
          async () => {
            await instance.backend.find('products', { price: { $gt: 500 } }, { limit: 100 })
          },
          { iterations: config.iterations, warmup: config.warmup }
        )
        results.push(rangeResult)
        console.log(`    p50: ${rangeResult.p50.toFixed(2)}ms, p95: ${rangeResult.p95.toFixed(2)}ms`)

        // Benchmark: Compound Query
        console.log('  • Compound query...')
        const compoundResult = await runBenchmark(
          'Compound Query',
          backendType,
          size,
          async () => {
            await instance.backend.find(
              'products',
              { category: 'electronics', status: 'active' },
              { limit: 50 }
            )
          },
          { iterations: config.iterations, warmup: config.warmup }
        )
        results.push(compoundResult)
        console.log(`    p50: ${compoundResult.p50.toFixed(2)}ms, p95: ${compoundResult.p95.toFixed(2)}ms`)

        // Benchmark: Full Scan (no filter)
        console.log('  • Full scan...')
        const scanResult = await runBenchmark(
          'Full Scan',
          backendType,
          size,
          async () => {
            await instance.backend.find('products', undefined, { limit: size })
          },
          { iterations: Math.max(3, config.iterations / 2), warmup: 1 }
        )
        results.push(scanResult)
        console.log(`    p50: ${scanResult.p50.toFixed(2)}ms, p95: ${scanResult.p95.toFixed(2)}ms`)

        // Note: Update and Delete benchmarks are skipped for now due to
        // IcebergBackend read-after-write consistency issues in testing.
        // These operations are well-tested in unit tests, but benchmarking
        // requires additional setup for proper snapshot visibility.
        console.log('  • Update... (skipped - requires snapshot sync)')
        console.log('  • Delete... (skipped - requires snapshot sync)')

      } finally {
        await instance.cleanup()
      }
    }
  }

  // Calculate summary
  const avgStats = (arr: number[]) => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0

  const backendComparison: Record<BackendType, { avgWriteMs: number; avgReadMs: number; avgQueryMs: number }> = {
    memory: {
      avgWriteMs: avgStats(backendStats.memory.writes),
      avgReadMs: avgStats(backendStats.memory.reads),
      avgQueryMs: avgStats(backendStats.memory.queries),
    },
    iceberg: {
      avgWriteMs: avgStats(backendStats.iceberg.writes),
      avgReadMs: avgStats(backendStats.iceberg.reads),
      avgQueryMs: avgStats(backendStats.iceberg.queries),
    },
    delta: {
      avgWriteMs: avgStats(backendStats.delta.writes),
      avgReadMs: avgStats(backendStats.delta.reads),
      avgQueryMs: avgStats(backendStats.delta.queries),
    },
  }

  const findBest = (metric: 'avgWriteMs' | 'avgReadMs' | 'avgQueryMs'): BackendType => {
    return config.backends.reduce((best, current) => {
      const currentVal = backendComparison[current][metric]
      const bestVal = backendComparison[best][metric]
      return currentVal > 0 && (bestVal === 0 || currentVal < bestVal) ? current : best
    }, config.backends[0]!)
  }

  return {
    config,
    results,
    summary: {
      backendComparison,
      bestBackend: {
        write: findBest('avgWriteMs'),
        read: findBest('avgReadMs'),
        query: findBest('avgQueryMs'),
      },
    },
  }
}

// =============================================================================
// Output Formatters
// =============================================================================

function formatTable(suite: SuiteResult): void {
  console.log('\n' + '='.repeat(80))
  console.log('SUMMARY')
  console.log('='.repeat(80))

  // Backend comparison table
  console.log('\nBackend Comparison (p50 latency averages):')
  console.log('─'.repeat(60))
  console.log(
    'Backend'.padEnd(15) +
    'Write (ms)'.padStart(15) +
    'Read (ms)'.padStart(15) +
    'Query (ms)'.padStart(15)
  )
  console.log('─'.repeat(60))

  for (const backend of suite.config.backends) {
    const stats = suite.summary.backendComparison[backend]
    console.log(
      backend.padEnd(15) +
      stats.avgWriteMs.toFixed(2).padStart(15) +
      stats.avgReadMs.toFixed(2).padStart(15) +
      stats.avgQueryMs.toFixed(2).padStart(15)
    )
  }

  console.log('─'.repeat(60))
  console.log(`\nBest for writes: ${suite.summary.bestBackend.write}`)
  console.log(`Best for reads:  ${suite.summary.bestBackend.read}`)
  console.log(`Best for queries: ${suite.summary.bestBackend.query}`)

  // Detailed results table
  console.log('\n\nDetailed Results:')
  console.log('─'.repeat(100))
  console.log(
    'Operation'.padEnd(30) +
    'Backend'.padEnd(10) +
    'Size'.padStart(10) +
    'p50 (ms)'.padStart(12) +
    'p95 (ms)'.padStart(12) +
    'p99 (ms)'.padStart(12) +
    'ops/sec'.padStart(12)
  )
  console.log('─'.repeat(100))

  for (const r of suite.results) {
    console.log(
      r.name.padEnd(30) +
      r.backend.padEnd(10) +
      r.datasetSize.toLocaleString().padStart(10) +
      r.p50.toFixed(2).padStart(12) +
      r.p95.toFixed(2).padStart(12) +
      r.p99.toFixed(2).padStart(12) +
      r.opsPerSecond.toFixed(1).padStart(12)
    )
  }

  console.log('─'.repeat(100))
}

function formatMarkdown(suite: SuiteResult): string {
  let md = '# ParqueDB Backend Benchmark Results\n\n'
  md += `**Date:** ${new Date().toISOString()}\n\n`
  md += '## Configuration\n\n'
  md += `- Backends: ${suite.config.backends.join(', ')}\n`
  md += `- Dataset sizes: ${suite.config.sizes.join(', ')}\n`
  md += `- Iterations: ${suite.config.iterations}\n\n`

  md += '## Summary\n\n'
  md += '| Backend | Write (ms) | Read (ms) | Query (ms) |\n'
  md += '|---------|------------|-----------|------------|\n'

  for (const backend of suite.config.backends) {
    const stats = suite.summary.backendComparison[backend]
    md += `| ${backend} | ${stats.avgWriteMs.toFixed(2)} | ${stats.avgReadMs.toFixed(2)} | ${stats.avgQueryMs.toFixed(2)} |\n`
  }

  md += `\n**Best for writes:** ${suite.summary.bestBackend.write}\n`
  md += `**Best for reads:** ${suite.summary.bestBackend.read}\n`
  md += `**Best for queries:** ${suite.summary.bestBackend.query}\n\n`

  md += '## Detailed Results\n\n'
  md += '| Operation | Backend | Size | p50 (ms) | p95 (ms) | p99 (ms) | ops/sec |\n'
  md += '|-----------|---------|------|----------|----------|----------|----------|\n'

  for (const r of suite.results) {
    md += `| ${r.name} | ${r.backend} | ${r.datasetSize.toLocaleString()} | ${r.p50.toFixed(2)} | ${r.p95.toFixed(2)} | ${r.p99.toFixed(2)} | ${r.opsPerSecond.toFixed(1)} |\n`
  }

  return md
}

// =============================================================================
// CLI
// =============================================================================

function parseArgs(): BenchmarkConfig {
  const args = process.argv.slice(2)
  const config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg.startsWith('--backend=')) {
      const value = arg.split('=')[1]
      if (value === 'all') {
        config.backends = ['memory', 'iceberg', 'delta']
      } else {
        config.backends = value?.split(',').filter(b =>
          ['memory', 'iceberg', 'delta'].includes(b)
        ) as BackendType[]
      }
    } else if (arg.startsWith('--iterations=')) {
      config.iterations = parseInt(arg.split('=')[1] ?? '10')
    } else if (arg.startsWith('--sizes=')) {
      config.sizes = (arg.split('=')[1] ?? '').split(',').map(s => parseInt(s)).filter(n => !isNaN(n))
    } else if (arg.startsWith('--output=')) {
      const output = arg.split('=')[1]
      if (output === 'table' || output === 'json' || output === 'markdown') {
        config.output = output
      }
    }
  }

  return config
}

async function main() {
  const config = parseArgs()
  const suite = await runBackendBenchmarks(config)

  switch (config.output) {
    case 'table':
      formatTable(suite)
      break
    case 'json':
      console.log(JSON.stringify(suite, null, 2))
      break
    case 'markdown':
      console.log(formatMarkdown(suite))
      break
  }

  console.log('\n' + '='.repeat(80))
  console.log('Benchmark complete!')
  console.log('='.repeat(80))
}

main().catch(console.error)

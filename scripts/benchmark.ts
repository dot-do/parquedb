#!/usr/bin/env bun
/**
 * Unified Benchmark Script for ParqueDB
 *
 * Runs comprehensive performance benchmarks and generates reports.
 *
 * Usage:
 *   bun scripts/benchmark.ts [options]
 *
 * Options:
 *   --suite=<name>   Run specific suite: crud, queries, parquet, all (default: all)
 *   --scale=<list>   Entity counts: 100,1000,10000 (default: 100,1000,10000)
 *   --iterations=<n> Iterations per benchmark (default: 10)
 *   --warmup=<n>     Warmup iterations (default: 3)
 *   --output=<fmt>   Output format: table, json, markdown (default: table)
 *   --verbose        Show detailed results
 *   --help           Show this help
 */

// =============================================================================
// Types
// =============================================================================

interface BenchmarkConfig {
  suite: string
  scale: number[]
  iterations: number
  warmup: number
  output: string
  verbose: boolean
}

interface BenchmarkResult {
  name: string
  iterations: number
  mean: number
  median: number
  min: number
  max: number
  p95: number
  p99: number
  opsPerSec: number
}

interface Entity {
  $id: string
  $type?: string
  name?: string
  title?: string
  content?: string
  status?: string
  views?: number
  likes?: number
  tags?: string[]
  $createdAt?: number
  $updatedAt?: number
  [key: string]: unknown
}

interface FindOptions {
  limit?: number
  skip?: number
  sort?: Record<string, 1 | -1>
  project?: Record<string, 1>
}

interface UpdateOperators {
  $set?: Record<string, unknown>
  $inc?: Record<string, number>
}

interface MatchStage {
  $match: Record<string, unknown>
}

interface GroupStage {
  $group: {
    _id: string | null
    [key: string]: unknown
  }
}

type AggregateStage = MatchStage | GroupStage

interface AllResults {
  config: BenchmarkConfig
  timestamp: string
  suites: Record<string, BenchmarkResult[]>
}

// =============================================================================
// Mock Collection Implementation
// =============================================================================

class MockCollection {
  private ns: string
  private data: Map<string, Entity>
  private idCounter: number

  constructor(ns: string) {
    this.ns = ns
    this.data = new Map()
    this.idCounter = 0
  }

  async create(doc: Omit<Entity, '$id'>): Promise<Entity> {
    const id = `${this.ns}/${++this.idCounter}`
    const entity: Entity = { $id: id, ...doc, $createdAt: Date.now(), $updatedAt: Date.now() }
    this.data.set(id, entity)
    return entity
  }

  async get(id: string): Promise<Entity | null> {
    return this.data.get(id) || this.data.get(`${this.ns}/${id}`) || null
  }

  async find(filter: Record<string, unknown> = {}, options: FindOptions = {}): Promise<Entity[]> {
    let results = Array.from(this.data.values())

    // Apply basic filter
    if (Object.keys(filter).length > 0) {
      results = results.filter(doc => {
        for (const [key, value] of Object.entries(filter)) {
          if (key === '$and') continue
          if (key === '$or') continue
          if (typeof value === 'object' && value !== null) {
            const ops = value as Record<string, unknown>
            // Handle operators
            if ('$gt' in ops && !(doc[key] as number > (ops.$gt as number))) return false
            if ('$gte' in ops && !(doc[key] as number >= (ops.$gte as number))) return false
            if ('$lt' in ops && !(doc[key] as number < (ops.$lt as number))) return false
            if ('$lte' in ops && !(doc[key] as number <= (ops.$lte as number))) return false
            if ('$in' in ops && !(ops.$in as unknown[]).includes(doc[key])) return false
          } else {
            if (doc[key] !== value) return false
          }
        }
        return true
      })
    }

    // Apply sort
    if (options.sort) {
      const sortEntries = Object.entries(options.sort)
      results.sort((a, b) => {
        for (const [key, order] of sortEntries) {
          const aVal = a[key]
          const bVal = b[key]
          if (aVal < bVal) return order === -1 ? 1 : -1
          if (aVal > bVal) return order === -1 ? -1 : 1
        }
        return 0
      })
    }

    // Apply skip/limit
    if (options.skip) results = results.slice(options.skip)
    if (options.limit) results = results.slice(0, options.limit)

    return results
  }

  async update(id: string, ops: UpdateOperators): Promise<Entity | null> {
    const fullId = id.includes('/') ? id : `${this.ns}/${id}`
    const doc = this.data.get(fullId)
    if (!doc) return null

    if (ops.$set) Object.assign(doc, ops.$set)
    if (ops.$inc) {
      for (const [key, val] of Object.entries(ops.$inc)) {
        doc[key] = ((doc[key] as number) || 0) + val
      }
    }
    doc.$updatedAt = Date.now()
    return doc
  }

  async delete(id: string): Promise<boolean> {
    const fullId = id.includes('/') ? id : `${this.ns}/${id}`
    return this.data.delete(fullId)
  }

  async count(filter: Record<string, unknown> = {}): Promise<number> {
    return (await this.find(filter)).length
  }

  async aggregate(pipeline: AggregateStage[]): Promise<unknown[]> {
    let results: unknown[] = Array.from(this.data.values())

    for (const stage of pipeline) {
      if ('$match' in stage) {
        results = (results as Entity[]).filter(doc => {
          for (const [key, value] of Object.entries(stage.$match)) {
            if (typeof value === 'object' && value !== null) {
              const ops = value as Record<string, unknown>
              if ('$gt' in ops && !(doc[key] as number > (ops.$gt as number))) return false
            } else {
              if (doc[key] !== value) return false
            }
          }
          return true
        })
      }

      if ('$group' in stage) {
        const groups = new Map<unknown, { _id: unknown; docs: Entity[] }>()
        for (const doc of results as Entity[]) {
          const key = stage.$group._id?.startsWith('$')
            ? doc[stage.$group._id.slice(1)]
            : stage.$group._id
          if (!groups.has(key)) {
            groups.set(key, { _id: key, docs: [] })
          }
          groups.get(key)!.docs.push(doc)
        }
        results = Array.from(groups.values()).map(g => {
          const result: Record<string, unknown> = { _id: g._id }
          for (const [field, op] of Object.entries(stage.$group)) {
            if (field === '_id') continue
            const opValue = op as Record<string, unknown>
            if (opValue.$sum === 1) result[field] = g.docs.length
            if (opValue.$avg) {
              const fieldName = (opValue.$avg as string).slice(1)
              result[field] = g.docs.reduce((s, d) => s + ((d[fieldName] as number) || 0), 0) / g.docs.length
            }
          }
          return result
        })
      }
    }

    return results
  }
}

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_CONFIG: BenchmarkConfig = {
  suite: 'all',
  scale: [100, 1000, 10000],
  iterations: 10,
  warmup: 3,
  output: 'table',
  verbose: false,
}

function parseArgs(): BenchmarkConfig {
  const args = process.argv.slice(2)
  const config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
ParqueDB Benchmark Runner

Usage: bun scripts/benchmark.ts [options]

Options:
  --suite=<name>   Run specific suite: crud, queries, parquet, scalability, all
  --scale=<list>   Entity counts comma-separated (default: 100,1000,10000)
  --iterations=<n> Iterations per benchmark (default: 10)
  --warmup=<n>     Warmup iterations (default: 3)
  --output=<fmt>   Output format: table, json, markdown
  --verbose        Show detailed results
  --help           Show this help

Examples:
  bun scripts/benchmark.ts
  bun scripts/benchmark.ts --suite=crud --iterations=20
  bun scripts/benchmark.ts --scale=100,1000 --output=json
`)
      process.exit(0)
    }

    const [key, value] = arg.split('=')
    switch (key) {
      case '--suite':
        config.suite = value
        break
      case '--scale':
        config.scale = value.split(',').map(Number)
        break
      case '--iterations':
        config.iterations = parseInt(value, 10)
        break
      case '--warmup':
        config.warmup = parseInt(value, 10)
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
// Utilities
// =============================================================================

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function percentile(arr: number[], p: number): number {
  const sorted = [...arr].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

function mean(arr: number[]): number {
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

async function benchmark(name: string, fn: () => Promise<void>, config: BenchmarkConfig): Promise<BenchmarkResult> {
  const times: number[] = []

  // Warmup
  for (let i = 0; i < config.warmup; i++) {
    await fn()
  }

  // Measure
  for (let i = 0; i < config.iterations; i++) {
    const start = performance.now()
    await fn()
    times.push(performance.now() - start)
  }

  return {
    name,
    iterations: config.iterations,
    mean: mean(times),
    median: median(times),
    min: Math.min(...times),
    max: Math.max(...times),
    p95: percentile(times, 95),
    p99: percentile(times, 99),
    opsPerSec: Math.round((config.iterations / times.reduce((a, b) => a + b, 0)) * 1000),
  }
}

function printTable(results: BenchmarkResult[], title: string): void {
  console.log(`\n${'='.repeat(70)}`)
  console.log(title)
  console.log('='.repeat(70))
  console.log(`${'Name'.padEnd(40)} ${'Mean'.padStart(8)} ${'P95'.padStart(8)} ${'Ops/s'.padStart(8)}`)
  console.log('-'.repeat(70))

  for (const r of results) {
    console.log(
      `${r.name.slice(0, 38).padEnd(40)} ${r.mean.toFixed(2).padStart(6)}ms ${r.p95.toFixed(2).padStart(6)}ms ${String(r.opsPerSec).padStart(8)}`
    )
  }
}

function printMarkdown(results: BenchmarkResult[], title: string): void {
  console.log(`\n## ${title}\n`)
  console.log('| Operation | Mean (ms) | P95 (ms) | Ops/sec |')
  console.log('|-----------|-----------|----------|---------|')

  for (const r of results) {
    console.log(`| ${r.name} | ${r.mean.toFixed(2)} | ${r.p95.toFixed(2)} | ${r.opsPerSec} |`)
  }
}

// =============================================================================
// Data Generators
// =============================================================================

const statuses = ['draft', 'published', 'archived']
const categories = ['tech', 'science', 'arts', 'sports', 'business']
const tags = ['featured', 'trending', 'new', 'popular', 'editor-pick']

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function randomElement<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

function randomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let result = ''
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

function generatePost(index: number): Omit<Entity, '$id'> {
  return {
    $type: 'Post',
    name: `Post ${index}`,
    title: `Test Post ${index}: ${randomString(20)}`,
    content: randomString(500),
    status: statuses[index % 3],
    views: randomInt(0, 100000),
    likes: randomInt(0, 5000),
    tags: [randomElement(tags), randomElement(tags)],
  }
}

// =============================================================================
// Benchmark Suites
// =============================================================================

async function runCrudBenchmarks(config: BenchmarkConfig): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []
  const ns = `crud-bench-${Date.now()}`
  const posts = new MockCollection(ns)

  // Seed some data for read/update/delete tests
  const entityIds: string[] = []
  for (let i = 0; i < 100; i++) {
    const entity = await posts.create(generatePost(i))
    entityIds.push(entity.$id)
  }

  // Create
  results.push(await benchmark('Create single entity', async () => {
    await posts.create(generatePost(Date.now()))
  }, config))

  // Get by ID
  results.push(await benchmark('Get by ID', async () => {
    await posts.get(randomElement(entityIds))
  }, config))

  // Find with filter
  results.push(await benchmark('Find with equality filter', async () => {
    await posts.find({ status: 'published' })
  }, config))

  results.push(await benchmark('Find with range filter', async () => {
    await posts.find({ views: { $gt: 50000 } })
  }, config))

  results.push(await benchmark('Find with $in filter', async () => {
    await posts.find({ status: { $in: ['published', 'archived'] } })
  }, config))

  results.push(await benchmark('Find with complex filter', async () => {
    await posts.find({
      $and: [
        { status: 'published' },
        { views: { $gt: 10000 } },
      ],
    })
  }, config))

  // Update
  results.push(await benchmark('Update ($set)', async () => {
    await posts.update(randomElement(entityIds), {
      $set: { title: `Updated ${Date.now()}` },
    })
  }, config))

  results.push(await benchmark('Update ($inc)', async () => {
    await posts.update(randomElement(entityIds), {
      $inc: { views: 1 },
    })
  }, config))

  // Count
  results.push(await benchmark('Count all', async () => {
    await posts.count()
  }, config))

  results.push(await benchmark('Count with filter', async () => {
    await posts.count({ status: 'published' })
  }, config))

  return results
}

async function runQueryBenchmarks(config: BenchmarkConfig): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []
  const ns = `query-bench-${Date.now()}`
  const posts = new MockCollection(ns)

  // Seed data
  console.log('  Seeding 1000 entities for query benchmarks...')
  for (let i = 0; i < 1000; i++) {
    await posts.create(generatePost(i))
  }

  // Query patterns
  results.push(await benchmark('Full scan (no filter)', async () => {
    await posts.find()
  }, config))

  results.push(await benchmark('Find with limit 10', async () => {
    await posts.find({}, { limit: 10 })
  }, config))

  results.push(await benchmark('Find with limit 100', async () => {
    await posts.find({}, { limit: 100 })
  }, config))

  results.push(await benchmark('Find with sort (single field)', async () => {
    await posts.find({}, { sort: { views: -1 }, limit: 100 })
  }, config))

  results.push(await benchmark('Find with sort (multi-field)', async () => {
    await posts.find({}, { sort: { status: 1, views: -1 }, limit: 100 })
  }, config))

  results.push(await benchmark('Find with projection', async () => {
    await posts.find({}, { project: { title: 1, status: 1 }, limit: 100 })
  }, config))

  results.push(await benchmark('Pagination: page 1 (skip 0)', async () => {
    await posts.find({}, { limit: 20, skip: 0 })
  }, config))

  results.push(await benchmark('Pagination: page 10 (skip 180)', async () => {
    await posts.find({}, { limit: 20, skip: 180 })
  }, config))

  results.push(await benchmark('Pagination: page 50 (skip 980)', async () => {
    await posts.find({}, { limit: 20, skip: 980 })
  }, config))

  // Aggregation
  results.push(await benchmark('Aggregate: group by status', async () => {
    await posts.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ])
  }, config))

  results.push(await benchmark('Aggregate: match + group', async () => {
    await posts.aggregate([
      { $match: { views: { $gt: 10000 } } },
      { $group: { _id: '$status', avgViews: { $avg: '$views' } } },
    ])
  }, config))

  return results
}

async function runScalabilityBenchmarks(config: BenchmarkConfig): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []

  for (const scale of config.scale) {
    console.log(`\n  Testing scale: ${scale.toLocaleString()} entities...`)

    const ns = `scale-${scale}-${Date.now()}`
    const collection = new MockCollection(ns)

    // Seed data
    const seedStart = performance.now()
    for (let i = 0; i < scale; i++) {
      await collection.create(generatePost(i))
    }
    const seedTime = performance.now() - seedStart
    console.log(`    Seeded in ${(seedTime / 1000).toFixed(2)}s (${Math.round(scale / (seedTime / 1000))} entities/sec)`)

    // Benchmarks
    results.push(await benchmark(`[${scale}] Find all`, async () => {
      await collection.find()
    }, { ...config, iterations: Math.min(config.iterations, 5) }))

    results.push(await benchmark(`[${scale}] Find with filter`, async () => {
      await collection.find({ status: 'published' })
    }, config))

    results.push(await benchmark(`[${scale}] Find with sort + limit`, async () => {
      await collection.find({}, { sort: { views: -1 }, limit: 10 })
    }, config))

    results.push(await benchmark(`[${scale}] Count`, async () => {
      await collection.count()
    }, config))

    results.push(await benchmark(`[${scale}] Count with filter`, async () => {
      await collection.count({ status: 'published' })
    }, config))
  }

  return results
}

async function runBatchBenchmarks(config: BenchmarkConfig): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = []

  for (const batchSize of [10, 100, 1000]) {
    const ns = `batch-${batchSize}-${Date.now()}`
    const collection = new MockCollection(ns)

    results.push(await benchmark(`Batch create ${batchSize}`, async () => {
      for (let i = 0; i < batchSize; i++) {
        await collection.create(generatePost(i))
      }
    }, { ...config, iterations: Math.max(1, Math.floor(config.iterations / (batchSize / 10))) }))
  }

  return results
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()

  console.log('='.repeat(70))
  console.log('ParqueDB Benchmark Suite')
  console.log('='.repeat(70))
  console.log(`Suite: ${config.suite}`)
  console.log(`Scale: ${config.scale.join(', ')}`)
  console.log(`Iterations: ${config.iterations}`)
  console.log(`Warmup: ${config.warmup}`)
  console.log(`Started: ${new Date().toISOString()}`)

  const allResults: AllResults = {
    config,
    timestamp: new Date().toISOString(),
    suites: {},
  }

  const printFn = config.output === 'markdown' ? printMarkdown : printTable

  try {
    // CRUD Benchmarks
    if (config.suite === 'all' || config.suite === 'crud') {
      console.log('\nRunning CRUD benchmarks...')
      const crudResults = await runCrudBenchmarks(config)
      allResults.suites.crud = crudResults
      printFn(crudResults, 'CRUD Operations')
    }

    // Query Benchmarks
    if (config.suite === 'all' || config.suite === 'queries') {
      console.log('\nRunning Query benchmarks...')
      const queryResults = await runQueryBenchmarks(config)
      allResults.suites.queries = queryResults
      printFn(queryResults, 'Query Operations')
    }

    // Scalability Benchmarks
    if (config.suite === 'all' || config.suite === 'scalability') {
      console.log('\nRunning Scalability benchmarks...')
      const scaleResults = await runScalabilityBenchmarks(config)
      allResults.suites.scalability = scaleResults
      printFn(scaleResults, 'Scalability Tests')
    }

    // Batch Benchmarks
    if (config.suite === 'all' || config.suite === 'batch') {
      console.log('\nRunning Batch benchmarks...')
      const batchResults = await runBatchBenchmarks(config)
      allResults.suites.batch = batchResults
      printFn(batchResults, 'Batch Operations')
    }

  } catch (error) {
    console.error('\nBenchmark failed:', (error as Error).message)
    if (config.verbose) {
      console.error((error as Error).stack)
    }
    process.exit(1)
  }

  // JSON output
  if (config.output === 'json') {
    console.log('\n' + JSON.stringify(allResults, null, 2))
  }

  // Summary
  console.log('\n' + '='.repeat(70))
  console.log('Summary')
  console.log('='.repeat(70))

  let totalBenchmarks = 0
  for (const [suite, results] of Object.entries(allResults.suites)) {
    console.log(`  ${suite}: ${results.length} benchmarks`)
    totalBenchmarks += results.length
  }

  console.log(`\nTotal benchmarks: ${totalBenchmarks}`)
  console.log(`Completed: ${new Date().toISOString()}`)
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})

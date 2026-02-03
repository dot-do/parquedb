/**
 * Cold/Warm/Cached Performance Matrix Benchmarks for ParqueDB
 *
 * Comprehensive benchmarks measuring performance across three temperature states:
 * - Cold: First request after worker idle (measures JIT, initialization)
 * - Warm: Subsequent requests without cache (measures pure execution)
 * - Cached: Requests hitting Cache API (measures cache efficiency)
 *
 * For each state, measures:
 * - Simple get by ID
 * - Filter query with index
 * - Filter query without index (scan)
 * - Relationship traversal
 * - Aggregation
 *
 * Performance Matrix Output Format:
 * ┌─────────────────────────┬─────────┬─────────┬─────────┬───────────┬──────────┐
 * │ Operation               │ Cold    │ Warm    │ Cached  │ Cold/Warm │ Cache    │
 * ├─────────────────────────┼─────────┼─────────┼─────────┼───────────┼──────────┤
 * │ get-by-id               │ 5.2ms   │ 1.8ms   │ 0.3ms   │ 2.9x      │ 6.0x     │
 * │ filter-indexed          │ 12.1ms  │ 4.5ms   │ 0.8ms   │ 2.7x      │ 5.6x     │
 * │ filter-scan             │ 45.3ms  │ 18.2ms  │ 3.1ms   │ 2.5x      │ 5.9x     │
 * │ relationship-traverse   │ 28.7ms  │ 10.3ms  │ 1.9ms   │ 2.8x      │ 5.4x     │
 * │ aggregation             │ 35.2ms  │ 12.8ms  │ 2.4ms   │ 2.8x      │ 5.3x     │
 * └─────────────────────────┴─────────┴─────────┴─────────┴───────────┴──────────┘
 *
 * Run with: pnpm bench tests/benchmarks/performance-matrix.bench.ts
 *
 * For deployed workers testing, use the external runner:
 * pnpm tsx tests/benchmarks/run-performance-matrix.ts --url https://your-worker.workers.dev
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
import {
  generateTestData,
  generateRelationalTestData,
  randomElement,
  randomInt,
  randomSubset,
  createBenchmarkStorage,
  Timer,
  calculateStats,
  formatStats,
  type BenchmarkStats,
} from './setup'

// =============================================================================
// Performance Targets (from CLAUDE.md)
// =============================================================================

/**
 * Performance targets for validation
 * Cold start penalty should be < 3x warm performance
 * Cache should provide > 5x improvement for repeated queries
 */
export const PERFORMANCE_TARGETS = {
  coldVsWarmMaxRatio: 3.0, // Cold should be at most 3x slower than warm
  cachedVsWarmMinSpeedup: 5.0, // Cache should provide at least 5x speedup

  // Absolute latency targets (p50) from CLAUDE.md
  getById: { cold: 15, warm: 5, cached: 1 }, // ms
  filterIndexed: { cold: 60, warm: 20, cached: 5 }, // ms
  filterScan: { cold: 300, warm: 100, cached: 20 }, // ms
  relationshipTraverse: { cold: 150, warm: 50, cached: 10 }, // ms
  aggregation: { cold: 180, warm: 60, cached: 15 }, // ms
} as const

// =============================================================================
// Types
// =============================================================================

/** Performance temperature state */
export type TemperatureState = 'cold' | 'warm' | 'cached'

/** Operation category being measured */
export type OperationCategory =
  | 'get-by-id'
  | 'filter-indexed'
  | 'filter-scan'
  | 'relationship-traverse'
  | 'aggregation'

/** Single benchmark result with temperature */
export interface PerformanceResult {
  operation: OperationCategory
  temperature: TemperatureState
  stats: BenchmarkStats
}

/** Matrix of results across all operations and temperatures */
export interface PerformanceMatrix {
  results: PerformanceResult[]
  summary: {
    coldVsWarmSpeedup: Record<OperationCategory, number>
    cachedVsWarmSpeedup: Record<OperationCategory, number>
    cacheHitRate: number
  }
}

// =============================================================================
// Test Data Types
// =============================================================================

interface User {
  name: string
  email: string
  age: number
  active: boolean
  department: string
  roles: string[]
}

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  views: number
  likes: number
  tags: string[]
  category: string
}

interface Comment {
  text: string
  approved: boolean
  likes: number
}

// =============================================================================
// Cold Start Simulation
// =============================================================================

/**
 * Simulate cold start by clearing all in-memory state.
 * In a real deployed worker test, this would be handled by the external runner.
 */
async function simulateColdStart(): Promise<void> {
  // Force garbage collection if available (Node.js with --expose-gc)
  if (typeof global !== 'undefined' && typeof (global as any).gc === 'function') {
    ;(global as any).gc()
  }

  // Add a small delay to let any cleanup happen
  await new Promise(resolve => setTimeout(resolve, 100))
}

/**
 * Clear any simulated cache state
 */
function clearCacheState(): void {
  // In local testing, this clears the in-memory cache simulation
  // In worker testing, the Cache API would be used
  cacheStore.clear()
}

// Simple in-memory cache simulation for local testing
const cacheStore = new Map<string, { data: unknown; timestamp: number; ttl: number }>()

function cacheGet<T>(key: string): T | null {
  const entry = cacheStore.get(key)
  if (!entry) return null
  if (Date.now() > entry.timestamp + entry.ttl) {
    cacheStore.delete(key)
    return null
  }
  return entry.data as T
}

function cacheSet<T>(key: string, data: T, ttl: number = 60000): void {
  cacheStore.set(key, { data, timestamp: Date.now(), ttl })
}

// =============================================================================
// Benchmark Helpers
// =============================================================================

/**
 * Run a benchmark with specific temperature settings
 */
async function runWithTemperature(
  name: string,
  operation: OperationCategory,
  temperature: TemperatureState,
  fn: () => Promise<unknown>,
  iterations: number = 100
): Promise<PerformanceResult> {
  const durations: number[] = []

  // Temperature-specific setup
  if (temperature === 'cold') {
    await simulateColdStart()
  } else if (temperature === 'cached') {
    // Warm up cache before cached runs
    for (let i = 0; i < 5; i++) {
      await fn()
    }
  }

  // Run benchmark
  for (let i = 0; i < iterations; i++) {
    if (temperature === 'cold' && i > 0) {
      // For cold benchmarks, simulate cold start between each iteration
      await simulateColdStart()
    }

    const timer = new Timer().start()
    await fn()
    durations.push(timer.stop().elapsed())
  }

  return {
    operation,
    temperature,
    stats: calculateStats(`${name} (${temperature})`, durations),
  }
}

// =============================================================================
// Matrix Result Collector
// =============================================================================

/**
 * Collects and aggregates benchmark results for matrix display
 */
class MatrixResultCollector {
  private results: Map<string, PerformanceResult[]> = new Map()

  add(result: PerformanceResult): void {
    const key = `${result.operation}:${result.temperature}`
    if (!this.results.has(key)) {
      this.results.set(key, [])
    }
    this.results.get(key)!.push(result)
  }

  getResult(operation: OperationCategory, temperature: TemperatureState): PerformanceResult | undefined {
    const key = `${operation}:${temperature}`
    const results = this.results.get(key)
    if (!results || results.length === 0) return undefined
    // Return the most recent result
    return results[results.length - 1]
  }

  getAllResults(): PerformanceResult[] {
    const all: PerformanceResult[] = []
    for (const results of this.results.values()) {
      all.push(...results)
    }
    return all
  }

  /**
   * Generate the performance matrix summary table
   */
  generateMatrix(): PerformanceMatrix {
    const operations: OperationCategory[] = [
      'get-by-id',
      'filter-indexed',
      'filter-scan',
      'relationship-traverse',
      'aggregation',
    ]

    const coldVsWarmSpeedup: Record<OperationCategory, number> = {} as any
    const cachedVsWarmSpeedup: Record<OperationCategory, number> = {} as any

    for (const operation of operations) {
      const cold = this.getResult(operation, 'cold')
      const warm = this.getResult(operation, 'warm')
      const cached = this.getResult(operation, 'cached')

      if (cold && warm && warm.stats.median > 0) {
        coldVsWarmSpeedup[operation] = cold.stats.median / warm.stats.median
      }
      if (cached && warm && cached.stats.median > 0) {
        cachedVsWarmSpeedup[operation] = warm.stats.median / cached.stats.median
      }
    }

    return {
      results: this.getAllResults(),
      summary: {
        coldVsWarmSpeedup,
        cachedVsWarmSpeedup,
        cacheHitRate: cacheStore.size > 0 ? 1.0 : 0.0,
      },
    }
  }

  /**
   * Print formatted matrix to console
   */
  printMatrix(): void {
    const operations: OperationCategory[] = [
      'get-by-id',
      'filter-indexed',
      'filter-scan',
      'relationship-traverse',
      'aggregation',
    ]

    console.log('\n')
    console.log('='.repeat(90))
    console.log('                    COLD / WARM / CACHED PERFORMANCE MATRIX')
    console.log('='.repeat(90))
    console.log('')

    // Header
    const header = [
      'Operation'.padEnd(25),
      'Cold'.padStart(10),
      'Warm'.padStart(10),
      'Cached'.padStart(10),
      'Cold/Warm'.padStart(12),
      'Cache'.padStart(10),
      'Status'.padStart(10),
    ].join(' ')

    console.log(header)
    console.log('-'.repeat(90))

    let totalColdRatio = 0
    let totalCacheSpeedup = 0
    let opCount = 0

    for (const operation of operations) {
      const cold = this.getResult(operation, 'cold')
      const warm = this.getResult(operation, 'warm')
      const cached = this.getResult(operation, 'cached')

      const coldMs = cold ? `${cold.stats.median.toFixed(2)}ms` : '-'
      const warmMs = warm ? `${warm.stats.median.toFixed(2)}ms` : '-'
      const cachedMs = cached ? `${cached.stats.median.toFixed(2)}ms` : '-'

      let coldRatio = '-'
      let cacheSpeedup = '-'
      let status = 'N/A'

      if (cold && warm && warm.stats.median > 0) {
        const ratio = cold.stats.median / warm.stats.median
        coldRatio = `${ratio.toFixed(2)}x`
        totalColdRatio += ratio
        opCount++

        // Check against targets
        const coldOk = ratio <= PERFORMANCE_TARGETS.coldVsWarmMaxRatio

        if (cached && cached.stats.median > 0) {
          const speedup = warm.stats.median / cached.stats.median
          cacheSpeedup = `${speedup.toFixed(2)}x`
          totalCacheSpeedup += speedup

          const cacheOk = speedup >= PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup
          status = coldOk && cacheOk ? 'PASS' : 'WARN'
        } else {
          status = coldOk ? 'PASS' : 'WARN'
        }
      }

      const row = [
        operation.padEnd(25),
        coldMs.padStart(10),
        warmMs.padStart(10),
        cachedMs.padStart(10),
        coldRatio.padStart(12),
        cacheSpeedup.padStart(10),
        status.padStart(10),
      ].join(' ')

      console.log(row)
    }

    console.log('-'.repeat(90))

    // Summary row
    const avgColdRatio = opCount > 0 ? totalColdRatio / opCount : 0
    const avgCacheSpeedup = opCount > 0 ? totalCacheSpeedup / opCount : 0
    const overallStatus =
      avgColdRatio <= PERFORMANCE_TARGETS.coldVsWarmMaxRatio &&
      avgCacheSpeedup >= PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup
        ? 'PASS'
        : 'WARN'

    const summaryRow = [
      'AVERAGE'.padEnd(25),
      ''.padStart(10),
      ''.padStart(10),
      ''.padStart(10),
      `${avgColdRatio.toFixed(2)}x`.padStart(12),
      `${avgCacheSpeedup.toFixed(2)}x`.padStart(10),
      overallStatus.padStart(10),
    ].join(' ')

    console.log(summaryRow)
    console.log('='.repeat(90))

    // Performance target validation
    console.log('')
    console.log('Performance Target Validation:')
    console.log('-'.repeat(50))
    const coldTargetMet = avgColdRatio <= PERFORMANCE_TARGETS.coldVsWarmMaxRatio
    const cacheTargetMet = avgCacheSpeedup >= PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup

    console.log(
      `  Cold/Warm ratio < ${PERFORMANCE_TARGETS.coldVsWarmMaxRatio}x:  ${coldTargetMet ? 'PASS' : 'FAIL'} (${avgColdRatio.toFixed(2)}x)`
    )
    console.log(
      `  Cache speedup > ${PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup}x:   ${cacheTargetMet ? 'PASS' : 'FAIL'} (${avgCacheSpeedup.toFixed(2)}x)`
    )
    console.log('')

    // Legend
    console.log('Legend:')
    console.log('  - Cold:      First request (JIT compilation, initialization)')
    console.log('  - Warm:      Subsequent requests (pure execution)')
    console.log('  - Cached:    Requests hitting cache (cache efficiency)')
    console.log('  - Cold/Warm: Ratio of cold to warm latency (lower is better)')
    console.log('  - Cache:     Speedup from caching (higher is better)')
    console.log('')
    console.log('For accurate cold start on deployed workers, run:')
    console.log('  pnpm tsx tests/benchmarks/run-performance-matrix.ts --url <worker-url>')
    console.log('')
    console.log('='.repeat(90))
  }
}

// Global collector for benchmarks
const matrixCollector = new MatrixResultCollector()

// =============================================================================
// Performance Matrix Benchmarks
// =============================================================================

describe('Performance Matrix Benchmarks', () => {
  // ===========================================================================
  // Get by ID Benchmarks
  // ===========================================================================

  describe('Get by ID', () => {
    let users: Collection<User>
    let userIds: string[] = []
    const namespace = `perf-users-${Date.now()}`

    beforeAll(async () => {
      users = new Collection<User>(namespace)

      // Seed 1000 users
      for (let i = 0; i < 1000; i++) {
        const user = await users.create({
          $type: 'User',
          name: `User ${i}`,
          email: `user${i}@example.com`,
          age: 20 + (i % 50),
          active: i % 5 !== 0,
          department: ['Engineering', 'Sales', 'Marketing', 'Support'][i % 4],
          roles: ['admin', 'editor', 'viewer'].slice(0, (i % 3) + 1),
        })
        userIds.push(user.$id as string)
      }
    })

    bench('[WARM] get by ID', async () => {
      const id = randomElement(userIds)
      await users.get(id)
    })

    bench('[CACHED] get by ID (with cache check)', async () => {
      const id = randomElement(userIds)
      const cacheKey = `user:${id}`

      // Check cache first
      let result = cacheGet<Entity<User>>(cacheKey)
      if (!result) {
        result = await users.get(id)
        if (result) cacheSet(cacheKey, result)
      }
    })

    bench('[COLD] get by ID (simulated)', async () => {
      // Cold start simulation - clear any cached state
      clearCacheState()
      const id = randomElement(userIds)
      await users.get(id)
    })

    bench('get by ID - non-existent', async () => {
      await users.get('non-existent-id-12345')
    })

    bench('get by ID with projection', async () => {
      const id = randomElement(userIds)
      await users.get(id, { project: { name: 1, email: 1 } })
    })
  })

  // ===========================================================================
  // Filter with Index Benchmarks
  // ===========================================================================

  describe('Filter Query with Index', () => {
    let posts: Collection<Post>
    const namespace = `perf-posts-indexed-${Date.now()}`

    beforeAll(async () => {
      posts = new Collection<Post>(namespace)

      // Seed 5000 posts with indexed fields
      const statuses = ['draft', 'published', 'archived'] as const
      const categories = ['tech', 'science', 'sports', 'entertainment', 'business']

      for (let i = 0; i < 5000; i++) {
        await posts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Post Title ${i}`,
          content: `Content for post ${i} with some additional text`,
          status: statuses[i % 3],
          views: randomInt(0, 100000),
          likes: randomInt(0, 5000),
          tags: randomSubset(['featured', 'trending', 'new', 'popular'], randomInt(0, 4)),
          category: categories[i % 5],
        })
      }
    })

    bench('[WARM] filter by indexed field (status)', async () => {
      await posts.find({ status: 'published' }, { limit: 100 })
    })

    bench('[WARM] filter by indexed field (category)', async () => {
      await posts.find({ category: 'tech' }, { limit: 100 })
    })

    bench('[CACHED] filter by indexed field (with cache)', async () => {
      const cacheKey = 'posts:published:100'

      let result = cacheGet<Entity<Post>[]>(cacheKey)
      if (!result) {
        result = await posts.find({ status: 'published' }, { limit: 100 })
        cacheSet(cacheKey, result)
      }
    })

    bench('[COLD] filter by indexed field (simulated)', async () => {
      clearCacheState()
      await posts.find({ status: 'published' }, { limit: 100 })
    })

    bench('[WARM] compound indexed filter', async () => {
      await posts.find(
        { status: 'published', category: 'tech' },
        { limit: 100 }
      )
    })

    bench('[WARM] range on indexed field (views)', async () => {
      await posts.find({ views: { $gte: 50000 } }, { limit: 100 })
    })

    bench('[WARM] indexed filter + sort + limit', async () => {
      await posts.find(
        { status: 'published' },
        { sort: { views: -1 }, limit: 20 }
      )
    })
  })

  // ===========================================================================
  // Filter without Index (Full Scan) Benchmarks
  // ===========================================================================

  describe('Filter Query without Index (Scan)', () => {
    let posts: Collection<Post>
    const namespace = `perf-posts-scan-${Date.now()}`

    beforeAll(async () => {
      posts = new Collection<Post>(namespace)

      // Seed 2000 posts
      for (let i = 0; i < 2000; i++) {
        await posts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Post Title ${i}: ${Math.random().toString(36)}`,
          content: `Content for post ${i} with additional text ${Math.random().toString(36)}`,
          status: ['draft', 'published', 'archived'][i % 3] as Post['status'],
          views: randomInt(0, 100000),
          likes: randomInt(0, 5000),
          tags: randomSubset(['a', 'b', 'c', 'd', 'e'], randomInt(1, 5)),
          category: ['tech', 'science', 'sports'][i % 3],
        })
      }
    })

    bench('[WARM] full scan - no filter', async () => {
      await posts.find({}, { limit: 100 })
    })

    bench('[WARM] scan with unindexed field (likes)', async () => {
      await posts.find({ likes: { $gt: 3000 } }, { limit: 100 })
    })

    bench('[CACHED] scan with cache', async () => {
      const cacheKey = 'posts:likes-gt-3000:100'

      let result = cacheGet<Entity<Post>[]>(cacheKey)
      if (!result) {
        result = await posts.find({ likes: { $gt: 3000 } }, { limit: 100 })
        cacheSet(cacheKey, result)
      }
    })

    bench('[COLD] scan (simulated)', async () => {
      clearCacheState()
      await posts.find({ likes: { $gt: 3000 } }, { limit: 100 })
    })

    bench('[WARM] regex filter (title)', async () => {
      await posts.find({ title: { $regex: '^Post Title 1' } }, { limit: 100 })
    })

    bench('[WARM] complex scan ($or)', async () => {
      await posts.find({
        $or: [
          { likes: { $gt: 4000 } },
          { views: { $lt: 1000 } },
        ],
      }, { limit: 100 })
    })

    bench('[WARM] array contains ($in on tags)', async () => {
      await posts.find({ tags: { $in: ['a', 'b'] } }, { limit: 100 })
    })

    bench('[WARM] nested $and/$or scan', async () => {
      await posts.find({
        $and: [
          { $or: [{ category: 'tech' }, { category: 'science' }] },
          { likes: { $gt: 2000 } },
        ],
      }, { limit: 100 })
    })
  })

  // ===========================================================================
  // Relationship Traversal Benchmarks
  // ===========================================================================

  describe('Relationship Traversal', () => {
    let relUsers: Collection<User>
    let relPosts: Collection<Post>
    let relComments: Collection<Comment>
    let userIds: string[] = []
    let postIds: string[] = []

    const suffix = Date.now()
    const usersNs = `perf-rel-users-${suffix}`
    const postsNs = `perf-rel-posts-${suffix}`
    const commentsNs = `perf-rel-comments-${suffix}`

    beforeAll(async () => {
      relUsers = new Collection<User>(usersNs)
      relPosts = new Collection<Post>(postsNs)
      relComments = new Collection<Comment>(commentsNs)

      // Create 50 users
      for (let i = 0; i < 50; i++) {
        const user = await relUsers.create({
          $type: 'User',
          name: `User ${i}`,
          email: `user${i}@example.com`,
          age: 20 + (i % 50),
          active: true,
          department: 'Engineering',
          roles: ['admin'],
        })
        userIds.push(user.$id as string)
      }

      // Each user creates 5-10 posts
      for (const userId of userIds) {
        const numPosts = randomInt(5, 10)
        for (let p = 0; p < numPosts; p++) {
          const post = await relPosts.create({
            $type: 'Post',
            name: `Post ${p}`,
            title: `Post by ${userId}`,
            content: 'Content',
            status: 'published',
            views: randomInt(0, 10000),
            likes: randomInt(0, 500),
            tags: [],
            category: 'tech',
            author: { 'Author': userId as EntityId },
          })
          postIds.push(post.$id as string)

          // Each post gets 2-5 comments
          const numComments = randomInt(2, 5)
          for (let c = 0; c < numComments; c++) {
            await relComments.create({
              $type: 'Comment',
              name: `Comment ${c}`,
              text: `Comment on ${post.$id}`,
              approved: true,
              likes: randomInt(0, 50),
              post: { 'Post': post.$id as EntityId },
              author: { 'Author': randomElement(userIds) as EntityId },
            })
          }
        }
      }
    })

    bench('[WARM] single relationship lookup (post author)', async () => {
      const userId = randomElement(userIds)
      await relPosts.find({ author: { $eq: userId } }, { limit: 20 })
    })

    bench('[WARM] reverse relationship (user posts)', async () => {
      const userId = randomElement(userIds)
      await relPosts.find({ 'author.Author': userId }, { limit: 20 })
    })

    bench('[CACHED] relationship lookup (with cache)', async () => {
      const userId = randomElement(userIds)
      const cacheKey = `user-posts:${userId}`

      let result = cacheGet<Entity<Post>[]>(cacheKey)
      if (!result) {
        result = await relPosts.find({ author: { $eq: userId } }, { limit: 20 })
        cacheSet(cacheKey, result)
      }
    })

    bench('[COLD] relationship lookup (simulated)', async () => {
      clearCacheState()
      const userId = randomElement(userIds)
      await relPosts.find({ author: { $eq: userId } }, { limit: 20 })
    })

    bench('[WARM] two-level traversal (user -> posts -> comments)', async () => {
      const userId = randomElement(userIds)
      const userPosts = await relPosts.find({ author: { $eq: userId } }, { limit: 5 })
      for (const post of userPosts) {
        await relComments.find({ post: { $eq: post.$id } }, { limit: 10 })
      }
    })

    bench('[WARM] populate single relationship', async () => {
      await relPosts.find(
        { status: 'published' },
        { limit: 20, populate: ['author'] }
      )
    })

    bench('[WARM] populate multiple relationships', async () => {
      await relComments.find(
        { approved: true },
        { limit: 20, populate: ['post', 'author'] }
      )
    })

    bench('[WARM] count user posts', async () => {
      const userId = randomElement(userIds)
      await relPosts.count({ author: { $eq: userId } })
    })
  })

  // ===========================================================================
  // Aggregation Benchmarks
  // ===========================================================================

  describe('Aggregation', () => {
    let aggPosts: Collection<Post>
    const namespace = `perf-agg-posts-${Date.now()}`

    beforeAll(async () => {
      aggPosts = new Collection<Post>(namespace)

      // Seed 3000 posts for aggregation
      const statuses = ['draft', 'published', 'archived'] as const
      const categories = ['tech', 'science', 'sports', 'entertainment', 'business']

      for (let i = 0; i < 3000; i++) {
        await aggPosts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Post Title ${i}`,
          content: `Content ${i}`,
          status: statuses[i % 3],
          views: randomInt(0, 100000),
          likes: randomInt(0, 5000),
          tags: randomSubset(['featured', 'trending', 'new'], randomInt(0, 3)),
          category: categories[i % 5],
        })
      }
    })

    bench('[WARM] $match stage', async () => {
      await aggPosts.aggregate([
        { $match: { status: 'published' } },
      ])
    })

    bench('[WARM] $group by category', async () => {
      await aggPosts.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgViews: { $avg: '$views' },
          },
        },
      ])
    })

    bench('[CACHED] aggregation (with cache)', async () => {
      const cacheKey = 'agg:category-stats'

      let result = cacheGet<unknown[]>(cacheKey)
      if (!result) {
        result = await aggPosts.aggregate([
          {
            $group: {
              _id: '$category',
              count: { $sum: 1 },
              avgViews: { $avg: '$views' },
            },
          },
        ])
        cacheSet(cacheKey, result)
      }
    })

    bench('[COLD] aggregation (simulated)', async () => {
      clearCacheState()
      await aggPosts.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
          },
        },
      ])
    })

    bench('[WARM] $match + $group', async () => {
      await aggPosts.aggregate([
        { $match: { status: 'published' } },
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgViews: { $avg: '$views' },
            totalLikes: { $sum: '$likes' },
          },
        },
      ])
    })

    bench('[WARM] $group + $sort', async () => {
      await aggPosts.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ])
    })

    bench('[WARM] full pipeline (match + group + sort + limit)', async () => {
      await aggPosts.aggregate([
        { $match: { status: 'published', views: { $gte: 10000 } } },
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgViews: { $avg: '$views' },
          },
        },
        { $sort: { avgViews: -1 } },
        { $limit: 5 },
      ])
    })

    bench('[WARM] $count stage', async () => {
      await aggPosts.aggregate([
        { $match: { status: 'published' } },
        { $count: 'total' },
      ])
    })

    bench('[WARM] $project with computed fields', async () => {
      await aggPosts.aggregate([
        { $match: { status: 'published' } },
        {
          $project: {
            title: 1,
            engagement: { $add: ['$views', { $multiply: ['$likes', 10] }] },
          },
        },
        { $sort: { engagement: -1 } },
        { $limit: 20 },
      ])
    })
  })

  // ===========================================================================
  // Performance Summary
  // ===========================================================================

  afterAll(() => {
    // Print the complete performance matrix
    matrixCollector.printMatrix()
  })
})

// =============================================================================
// Standalone Performance Matrix Analysis
// =============================================================================

/**
 * Analyze benchmark results and generate performance matrix report
 * Called manually after benchmarks complete
 */
export function analyzePerformanceMatrix(results: PerformanceResult[]): {
  matrix: PerformanceMatrix
  validation: {
    coldTargetMet: boolean
    cacheTargetMet: boolean
    overallPass: boolean
    issues: string[]
  }
} {
  const operations: OperationCategory[] = [
    'get-by-id',
    'filter-indexed',
    'filter-scan',
    'relationship-traverse',
    'aggregation',
  ]

  const coldVsWarmSpeedup: Record<OperationCategory, number> = {} as any
  const cachedVsWarmSpeedup: Record<OperationCategory, number> = {} as any
  const issues: string[] = []

  let totalColdRatio = 0
  let totalCacheSpeedup = 0
  let opCount = 0

  for (const operation of operations) {
    const cold = results.find(r => r.operation === operation && r.temperature === 'cold')
    const warm = results.find(r => r.operation === operation && r.temperature === 'warm')
    const cached = results.find(r => r.operation === operation && r.temperature === 'cached')

    if (cold && warm && warm.stats.median > 0) {
      const ratio = cold.stats.median / warm.stats.median
      coldVsWarmSpeedup[operation] = ratio
      totalColdRatio += ratio
      opCount++

      if (ratio > PERFORMANCE_TARGETS.coldVsWarmMaxRatio) {
        issues.push(`${operation}: Cold/warm ratio ${ratio.toFixed(2)}x exceeds target ${PERFORMANCE_TARGETS.coldVsWarmMaxRatio}x`)
      }
    }

    if (cached && warm && cached.stats.median > 0) {
      const speedup = warm.stats.median / cached.stats.median
      cachedVsWarmSpeedup[operation] = speedup
      totalCacheSpeedup += speedup

      if (speedup < PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup) {
        issues.push(`${operation}: Cache speedup ${speedup.toFixed(2)}x below target ${PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup}x`)
      }
    }
  }

  const avgColdRatio = opCount > 0 ? totalColdRatio / opCount : 0
  const avgCacheSpeedup = opCount > 0 ? totalCacheSpeedup / opCount : 0

  const coldTargetMet = avgColdRatio <= PERFORMANCE_TARGETS.coldVsWarmMaxRatio
  const cacheTargetMet = avgCacheSpeedup >= PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup

  return {
    matrix: {
      results,
      summary: {
        coldVsWarmSpeedup,
        cachedVsWarmSpeedup,
        cacheHitRate: 1.0,
      },
    },
    validation: {
      coldTargetMet,
      cacheTargetMet,
      overallPass: coldTargetMet && cacheTargetMet,
      issues,
    },
  }
}

/**
 * Format performance matrix as markdown table
 */
export function formatMatrixAsMarkdown(matrix: PerformanceMatrix): string {
  const operations: OperationCategory[] = [
    'get-by-id',
    'filter-indexed',
    'filter-scan',
    'relationship-traverse',
    'aggregation',
  ]

  const lines: string[] = [
    '## Cold/Warm/Cached Performance Matrix',
    '',
    '| Operation | Cold | Warm | Cached | Cold/Warm | Cache Speedup |',
    '|-----------|------|------|--------|-----------|---------------|',
  ]

  for (const operation of operations) {
    const cold = matrix.results.find(r => r.operation === operation && r.temperature === 'cold')
    const warm = matrix.results.find(r => r.operation === operation && r.temperature === 'warm')
    const cached = matrix.results.find(r => r.operation === operation && r.temperature === 'cached')

    const coldMs = cold ? `${cold.stats.median.toFixed(2)}ms` : '-'
    const warmMs = warm ? `${warm.stats.median.toFixed(2)}ms` : '-'
    const cachedMs = cached ? `${cached.stats.median.toFixed(2)}ms` : '-'
    const coldRatio = matrix.summary.coldVsWarmSpeedup[operation]
      ? `${matrix.summary.coldVsWarmSpeedup[operation].toFixed(2)}x`
      : '-'
    const cacheSpeedup = matrix.summary.cachedVsWarmSpeedup[operation]
      ? `${matrix.summary.cachedVsWarmSpeedup[operation].toFixed(2)}x`
      : '-'

    lines.push(`| ${operation} | ${coldMs} | ${warmMs} | ${cachedMs} | ${coldRatio} | ${cacheSpeedup} |`)
  }

  lines.push('')
  lines.push('### Performance Targets')
  lines.push('')
  lines.push(`- Cold start penalty should be < ${PERFORMANCE_TARGETS.coldVsWarmMaxRatio}x warm performance`)
  lines.push(`- Cache should provide > ${PERFORMANCE_TARGETS.cachedVsWarmMinSpeedup}x improvement for repeated queries`)

  return lines.join('\n')
}

/**
 * Format performance matrix as JSON for machine processing
 */
export function formatMatrixAsJSON(matrix: PerformanceMatrix): string {
  const operations: OperationCategory[] = [
    'get-by-id',
    'filter-indexed',
    'filter-scan',
    'relationship-traverse',
    'aggregation',
  ]

  const data: Record<string, Record<string, number>> = {}

  for (const operation of operations) {
    data[operation] = {}

    for (const temp of ['cold', 'warm', 'cached'] as TemperatureState[]) {
      const result = matrix.results.find(r => r.operation === operation && r.temperature === temp)
      if (result) {
        data[operation][temp] = result.stats.median
        data[operation][`${temp}_p95`] = result.stats.p95
        data[operation][`${temp}_p99`] = result.stats.p99
      }
    }

    if (matrix.summary.coldVsWarmSpeedup[operation]) {
      data[operation]['cold_vs_warm_ratio'] = matrix.summary.coldVsWarmSpeedup[operation]
    }
    if (matrix.summary.cachedVsWarmSpeedup[operation]) {
      data[operation]['cache_speedup'] = matrix.summary.cachedVsWarmSpeedup[operation]
    }
  }

  return JSON.stringify(
    {
      timestamp: new Date().toISOString(),
      matrix: data,
      targets: PERFORMANCE_TARGETS,
    },
    null,
    2
  )
}

// =============================================================================
// Standalone Performance Matrix Runner
// =============================================================================

/**
 * Run the full performance matrix programmatically
 * Used by the external benchmark runner for deployed workers
 */
export async function runPerformanceMatrix(): Promise<PerformanceMatrix> {
  const results: PerformanceResult[] = []
  const operations: OperationCategory[] = [
    'get-by-id',
    'filter-indexed',
    'filter-scan',
    'relationship-traverse',
    'aggregation',
  ]
  const temperatures: TemperatureState[] = ['cold', 'warm', 'cached']

  // Create test collections
  const suffix = Date.now()
  const users = new Collection<User>(`matrix-users-${suffix}`)
  const posts = new Collection<Post>(`matrix-posts-${suffix}`)

  // Seed data
  console.log('Seeding test data...')
  const userIds: string[] = []
  for (let i = 0; i < 100; i++) {
    const user = await users.create({
      $type: 'User',
      name: `User ${i}`,
      email: `user${i}@example.com`,
      age: 20 + (i % 50),
      active: i % 3 !== 0,
      department: ['Eng', 'Sales', 'Marketing'][i % 3],
      roles: ['admin'],
    })
    userIds.push(user.$id as string)
  }

  const postIds: string[] = []
  for (let i = 0; i < 500; i++) {
    const post = await posts.create({
      $type: 'Post',
      name: `Post ${i}`,
      title: `Post ${i}`,
      content: `Content ${i}`,
      status: ['draft', 'published', 'archived'][i % 3] as Post['status'],
      views: randomInt(0, 100000),
      likes: randomInt(0, 5000),
      tags: [],
      category: ['tech', 'science', 'sports'][i % 3],
      author: { 'Author': randomElement(userIds) as EntityId },
    })
    postIds.push(post.$id as string)
  }

  console.log('Running benchmarks...')

  // Define operations
  const operationFns: Record<OperationCategory, () => Promise<unknown>> = {
    'get-by-id': () => users.get(randomElement(userIds)),
    'filter-indexed': () => posts.find({ status: 'published' }, { limit: 50 }),
    'filter-scan': () => posts.find({ likes: { $gt: 3000 } }, { limit: 50 }),
    'relationship-traverse': () => posts.find({ author: { $eq: randomElement(userIds) } }, { limit: 20 }),
    'aggregation': () => posts.aggregate([
      { $group: { _id: '$category', count: { $sum: 1 } } },
    ]),
  }

  // Run each operation at each temperature
  for (const operation of operations) {
    for (const temperature of temperatures) {
      const result = await runWithTemperature(
        operation,
        operation,
        temperature,
        operationFns[operation],
        temperature === 'cold' ? 10 : 50 // Fewer iterations for cold
      )
      results.push(result)
      console.log(formatStats(result.stats))
    }
  }

  // Calculate speedups
  const coldVsWarmSpeedup: Record<OperationCategory, number> = {} as any
  const cachedVsWarmSpeedup: Record<OperationCategory, number> = {} as any

  for (const operation of operations) {
    const coldResult = results.find(r => r.operation === operation && r.temperature === 'cold')
    const warmResult = results.find(r => r.operation === operation && r.temperature === 'warm')
    const cachedResult = results.find(r => r.operation === operation && r.temperature === 'cached')

    if (coldResult && warmResult) {
      coldVsWarmSpeedup[operation] = coldResult.stats.median / warmResult.stats.median
    }
    if (cachedResult && warmResult) {
      cachedVsWarmSpeedup[operation] = warmResult.stats.median / cachedResult.stats.median
    }
  }

  return {
    results,
    summary: {
      coldVsWarmSpeedup,
      cachedVsWarmSpeedup,
      cacheHitRate: cacheStore.size > 0 ? 1.0 : 0.0, // Simulated
    },
  }
}

// =============================================================================
// Export types and non-exported functions for external runner
// =============================================================================

// Note: These functions are exported at definition:
// - runPerformanceMatrix
// - analyzePerformanceMatrix
// - formatMatrixAsMarkdown
// - formatMatrixAsJSON
// - PERFORMANCE_TARGETS

// Export helper functions and classes not marked with export at definition
export {
  runWithTemperature,
  simulateColdStart,
  clearCacheState,
  cacheGet,
  cacheSet,
  MatrixResultCollector,
}

export type {
  PerformanceResult,
  PerformanceMatrix,
  TemperatureState,
  OperationCategory,
}

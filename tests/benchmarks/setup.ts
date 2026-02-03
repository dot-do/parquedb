/**
 * Benchmark Utilities for ParqueDB
 *
 * Provides utilities for running benchmarks including:
 * - benchmark() function for running and reporting performance
 * - generateTestData() for synthetic entity generation
 * - loadTestData() for loading real Parquet data from disk
 * - Timer utilities for precise measurements
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import type {
  Entity,
  EntityId,
  CreateInput,
  Schema,
  StorageBackend,
} from '../../src/types'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParquetReader } from '../../src/parquet/reader'

// =============================================================================
// Timer Utilities
// =============================================================================

/**
 * High-resolution timer for performance measurements
 */
export class Timer {
  private startTime: number = 0
  private endTime: number = 0
  private marks: Map<string, number> = new Map()

  /**
   * Start the timer
   */
  start(): this {
    this.startTime = performance.now()
    return this
  }

  /**
   * Stop the timer
   */
  stop(): this {
    this.endTime = performance.now()
    return this
  }

  /**
   * Get elapsed time in milliseconds
   */
  elapsed(): number {
    const end = this.endTime || performance.now()
    return end - this.startTime
  }

  /**
   * Get elapsed time in microseconds
   */
  elapsedMicros(): number {
    return this.elapsed() * 1000
  }

  /**
   * Mark a point in time
   */
  mark(name: string): this {
    this.marks.set(name, performance.now())
    return this
  }

  /**
   * Get time since a mark
   */
  sinceMark(name: string): number {
    const markTime = this.marks.get(name)
    if (markTime === undefined) {
      throw new Error(`Mark "${name}" not found`)
    }
    return performance.now() - markTime
  }

  /**
   * Reset the timer
   */
  reset(): this {
    this.startTime = 0
    this.endTime = 0
    this.marks.clear()
    return this
  }
}

/**
 * Create a timer and immediately start it
 */
export function startTimer(): Timer {
  return new Timer().start()
}

/**
 * Measure execution time of an async function
 */
export async function measure<T>(fn: () => Promise<T>): Promise<{ result: T; duration: number }> {
  const timer = startTimer()
  const result = await fn()
  return { result, duration: timer.stop().elapsed() }
}

/**
 * Measure execution time of a sync function
 */
export function measureSync<T>(fn: () => T): { result: T; duration: number } {
  const timer = startTimer()
  const result = fn()
  return { result, duration: timer.stop().elapsed() }
}

// =============================================================================
// Benchmark Statistics
// =============================================================================

/**
 * Statistics from a benchmark run
 */
export interface BenchmarkStats {
  /** Name of the benchmark */
  name: string
  /** Total number of iterations */
  iterations: number
  /** Total time in milliseconds */
  totalTime: number
  /** Average time per iteration in milliseconds */
  mean: number
  /** Median time per iteration in milliseconds */
  median: number
  /** Minimum time per iteration in milliseconds */
  min: number
  /** Maximum time per iteration in milliseconds */
  max: number
  /** Standard deviation */
  stdDev: number
  /** 95th percentile */
  p95: number
  /** 99th percentile */
  p99: number
  /** Operations per second */
  opsPerSecond: number
}

/**
 * Calculate statistics from an array of durations
 */
export function calculateStats(name: string, durations: number[]): BenchmarkStats {
  const n = durations.length

  // Handle empty array case
  if (n === 0) {
    return {
      name,
      iterations: 0,
      totalTime: 0,
      mean: 0,
      median: 0,
      min: 0,
      max: 0,
      stdDev: 0,
      p95: 0,
      p99: 0,
      opsPerSecond: 0,
    }
  }

  const sorted = [...durations].sort((a, b) => a - b)
  const totalTime = durations.reduce((a, b) => a + b, 0)
  const mean = totalTime / n

  // Calculate standard deviation
  const variance = durations.reduce((sum, d) => sum + Math.pow(d - mean, 2), 0) / n
  const stdDev = Math.sqrt(variance)

  // Percentiles
  const percentile = (p: number) => {
    const index = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(index, n - 1))]
  }

  return {
    name,
    iterations: n,
    totalTime,
    mean,
    median: percentile(50),
    min: sorted[0],
    max: sorted[n - 1],
    stdDev,
    p95: percentile(95),
    p99: percentile(99),
    opsPerSecond: (n / totalTime) * 1000,
  }
}

/**
 * Format benchmark stats for display
 */
export function formatStats(stats: BenchmarkStats): string {
  const lines = [
    `Benchmark: ${stats.name}`,
    `  Iterations:   ${stats.iterations}`,
    `  Total Time:   ${stats.totalTime.toFixed(2)}ms`,
    `  Mean:         ${stats.mean.toFixed(3)}ms`,
    `  Median:       ${stats.median.toFixed(3)}ms`,
    `  Min:          ${stats.min.toFixed(3)}ms`,
    `  Max:          ${stats.max.toFixed(3)}ms`,
    `  Std Dev:      ${stats.stdDev.toFixed(3)}ms`,
    `  P95:          ${stats.p95.toFixed(3)}ms`,
    `  P99:          ${stats.p99.toFixed(3)}ms`,
    `  Ops/sec:      ${stats.opsPerSecond.toFixed(2)}`,
  ]
  return lines.join('\n')
}

// =============================================================================
// Benchmark Runner
// =============================================================================

/**
 * Options for running a benchmark
 */
export interface BenchmarkOptions {
  /** Number of iterations (default: 100) */
  iterations?: number
  /** Number of warmup iterations (default: 10) */
  warmupIterations?: number
  /** Setup function called once before all iterations */
  setup?: () => Promise<void> | void
  /** Teardown function called once after all iterations */
  teardown?: () => Promise<void> | void
  /** Function called before each iteration */
  beforeEach?: () => Promise<void> | void
  /** Function called after each iteration */
  afterEach?: () => Promise<void> | void
}

/**
 * Run a benchmark and return statistics
 *
 * @param name - Name of the benchmark
 * @param fn - Function to benchmark (can be async)
 * @param options - Benchmark options
 * @returns Benchmark statistics
 *
 * @example
 * ```typescript
 * const stats = await benchmark('create entity', async () => {
 *   await db.create('posts', { $type: 'Post', name: 'Test', title: 'Test' })
 * }, { iterations: 1000 })
 * console.log(formatStats(stats))
 * ```
 */
export async function benchmark(
  name: string,
  fn: () => Promise<void> | void,
  options: BenchmarkOptions = {}
): Promise<BenchmarkStats> {
  const {
    iterations = 100,
    warmupIterations = 10,
    setup,
    teardown,
    beforeEach,
    afterEach,
  } = options

  // Run setup
  if (setup) {
    await setup()
  }

  // Warmup
  for (let i = 0; i < warmupIterations; i++) {
    if (beforeEach) await beforeEach()
    await fn()
    if (afterEach) await afterEach()
  }

  // Run benchmark
  const durations: number[] = []

  for (let i = 0; i < iterations; i++) {
    if (beforeEach) await beforeEach()

    const timer = startTimer()
    await fn()
    durations.push(timer.stop().elapsed())

    if (afterEach) await afterEach()
  }

  // Run teardown
  if (teardown) {
    await teardown()
  }

  return calculateStats(name, durations)
}

/**
 * Run multiple benchmarks and compare results
 */
export async function benchmarkSuite(
  suiteName: string,
  benchmarks: Array<{ name: string; fn: () => Promise<void> | void; options?: BenchmarkOptions }>
): Promise<BenchmarkStats[]> {
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Suite: ${suiteName}`)
  console.log('='.repeat(60))

  const results: BenchmarkStats[] = []

  for (const { name, fn, options } of benchmarks) {
    const stats = await benchmark(name, fn, options)
    results.push(stats)
    console.log(`\n${formatStats(stats)}`)
  }

  // Print comparison table
  console.log(`\n${'─'.repeat(60)}`)
  console.log('Comparison:')
  console.log('─'.repeat(60))
  console.log(`${'Name'.padEnd(30)} ${'Mean'.padStart(10)} ${'Ops/sec'.padStart(12)}`)
  console.log('─'.repeat(60))

  for (const stats of results) {
    const name = stats.name.length > 28 ? stats.name.slice(0, 28) + '..' : stats.name
    console.log(
      `${name.padEnd(30)} ${stats.mean.toFixed(3).padStart(10)}ms ${stats.opsPerSecond.toFixed(0).padStart(12)}`
    )
  }

  return results
}

// =============================================================================
// Test Data Generation
// =============================================================================

/** Counter for generating unique IDs */
let idCounter = 0

/**
 * Generate a unique test ID
 */
export function generateId(prefix = 'test'): string {
  idCounter++
  return `${prefix}-${Date.now().toString(36)}-${idCounter}`
}

/**
 * Reset the ID counter
 */
export function resetIdCounter(): void {
  idCounter = 0
}

/**
 * Generate random string of specified length
 */
export function randomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let result = ''
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

/**
 * Generate random integer in range [min, max]
 */
export function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

/**
 * Pick random element from array
 */
export function randomElement<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

/**
 * Pick random subset of array
 */
export function randomSubset<T>(arr: T[], count: number): T[] {
  const shuffled = [...arr].sort(() => Math.random() - 0.5)
  return shuffled.slice(0, Math.min(count, arr.length))
}

/**
 * Generate a random date within a range
 */
export function randomDate(start: Date = new Date(2020, 0, 1), end: Date = new Date()): Date {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

/**
 * Entity types for test data generation
 */
export type TestEntityType = 'User' | 'Post' | 'Comment' | 'Category' | 'Tag' | 'Product' | 'Order'

/**
 * Generated test entity shape
 */
export interface TestEntity extends Record<string, unknown> {
  $type: string
  name: string
  [key: string]: unknown
}

/**
 * Generate test data for a specific entity type
 */
export function generateEntity(type: TestEntityType, index?: number): TestEntity {
  const idx = index ?? idCounter++
  const statuses = ['draft', 'published', 'archived']
  const categories = ['tech', 'science', 'arts', 'sports', 'business']
  const tags = ['featured', 'trending', 'new', 'popular', 'editor-pick']

  switch (type) {
    case 'User':
      return {
        $type: 'User',
        name: `User ${idx}`,
        email: `user${idx}@example.com`,
        username: `user_${idx}`,
        age: randomInt(18, 80),
        active: Math.random() > 0.2,
        bio: randomString(100),
        followerCount: randomInt(0, 10000),
        createdAt: randomDate(),
        roles: randomSubset(['admin', 'editor', 'author', 'viewer'], randomInt(1, 3)),
        settings: {
          theme: randomElement(['light', 'dark', 'auto']),
          notifications: Math.random() > 0.3,
          language: randomElement(['en', 'es', 'fr', 'de', 'ja']),
        },
      }

    case 'Post':
      return {
        $type: 'Post',
        name: `Post ${idx}`,
        title: `Test Post Title ${idx}: ${randomString(20)}`,
        content: randomString(500),
        excerpt: randomString(100),
        status: randomElement(statuses),
        views: randomInt(0, 100000),
        likes: randomInt(0, 5000),
        readTime: randomInt(1, 30),
        publishedAt: Math.random() > 0.3 ? randomDate() : undefined,
        tags: randomSubset(tags, randomInt(0, 5)),
        metadata: {
          wordCount: randomInt(100, 5000),
          imageCount: randomInt(0, 10),
          featured: Math.random() > 0.8,
        },
      }

    case 'Comment':
      return {
        $type: 'Comment',
        name: `Comment ${idx}`,
        text: randomString(200),
        approved: Math.random() > 0.1,
        likes: randomInt(0, 100),
        createdAt: randomDate(),
        edited: Math.random() > 0.8,
        sentiment: randomElement(['positive', 'neutral', 'negative']),
      }

    case 'Category':
      return {
        $type: 'Category',
        name: `Category ${idx}`,
        slug: `category-${idx}`,
        description: randomString(150),
        postCount: randomInt(0, 1000),
        parentId: Math.random() > 0.7 ? `categories/cat-${randomInt(1, idx)}` : undefined,
        featured: Math.random() > 0.9,
        color: `#${Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0')}`,
      }

    case 'Tag':
      return {
        $type: 'Tag',
        name: `Tag ${idx}`,
        slug: `tag-${idx}`,
        usageCount: randomInt(0, 5000),
        trending: Math.random() > 0.9,
      }

    case 'Product':
      return {
        $type: 'Product',
        name: `Product ${idx}`,
        sku: `SKU-${idx.toString().padStart(6, '0')}`,
        price: randomInt(100, 100000) / 100,
        currency: 'USD',
        stock: randomInt(0, 1000),
        active: Math.random() > 0.1,
        category: randomElement(categories),
        rating: randomInt(1, 50) / 10,
        reviewCount: randomInt(0, 500),
        attributes: {
          weight: randomInt(1, 1000),
          dimensions: {
            width: randomInt(1, 100),
            height: randomInt(1, 100),
            depth: randomInt(1, 100),
          },
        },
      }

    case 'Order':
      return {
        $type: 'Order',
        name: `Order ${idx}`,
        orderNumber: `ORD-${Date.now()}-${idx}`,
        status: randomElement(['pending', 'processing', 'shipped', 'delivered', 'cancelled']),
        total: randomInt(1000, 50000) / 100,
        currency: 'USD',
        itemCount: randomInt(1, 20),
        createdAt: randomDate(),
        shippedAt: Math.random() > 0.5 ? randomDate() : undefined,
        shippingAddress: {
          street: `${randomInt(1, 999)} ${randomString(10)} St`,
          city: randomElement(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
          state: randomElement(['NY', 'CA', 'IL', 'TX', 'AZ']),
          zip: randomInt(10000, 99999).toString(),
        },
      }

    default:
      return {
        $type: type,
        name: `Entity ${idx}`,
        value: randomString(50),
        number: randomInt(0, 1000),
        active: Math.random() > 0.5,
      }
  }
}

/**
 * Generate an array of test entities
 *
 * @param count - Number of entities to generate
 * @param type - Entity type (default: mixed)
 * @returns Array of test entities
 *
 * @example
 * ```typescript
 * const users = generateTestData(1000, 'User')
 * const posts = generateTestData(5000, 'Post')
 * const mixed = generateTestData(10000) // Random types
 * ```
 */
export function generateTestData(count: number, type?: TestEntityType): TestEntity[] {
  const types: TestEntityType[] = ['User', 'Post', 'Comment', 'Category', 'Tag', 'Product', 'Order']
  const entities: TestEntity[] = []

  for (let i = 0; i < count; i++) {
    const entityType = type ?? randomElement(types)
    entities.push(generateEntity(entityType, i))
  }

  return entities
}

/**
 * Generate test data with relationships
 */
export interface TestDataWithRelationships {
  users: TestEntity[]
  posts: TestEntity[]
  comments: TestEntity[]
  categories: TestEntity[]
  relationships: Array<{ from: string; predicate: string; to: string }>
}

/**
 * Generate a complete test dataset with relationships
 */
export function generateRelationalTestData(options: {
  userCount?: number
  postsPerUser?: number
  commentsPerPost?: number
  categoryCount?: number
} = {}): TestDataWithRelationships {
  const {
    userCount = 10,
    postsPerUser = 5,
    commentsPerPost = 3,
    categoryCount = 5,
  } = options

  const users: TestEntity[] = []
  const posts: TestEntity[] = []
  const comments: TestEntity[] = []
  const categories: TestEntity[] = []
  const relationships: Array<{ from: string; predicate: string; to: string }> = []

  // Generate categories
  for (let i = 0; i < categoryCount; i++) {
    const category = generateEntity('Category', i)
    category.$id = `categories/cat-${i}` as EntityId
    categories.push(category)
  }

  // Generate users
  for (let u = 0; u < userCount; u++) {
    const user = generateEntity('User', u)
    user.$id = `users/user-${u}` as EntityId
    users.push(user)

    // Generate posts for each user
    for (let p = 0; p < postsPerUser; p++) {
      const postIdx = u * postsPerUser + p
      const post = generateEntity('Post', postIdx)
      post.$id = `posts/post-${postIdx}` as EntityId
      posts.push(post)

      // Link post to author
      relationships.push({
        from: post.$id as string,
        predicate: 'author',
        to: user.$id as string,
      })

      // Link post to random categories
      const postCategories = randomSubset(categories, randomInt(1, 3))
      for (const cat of postCategories) {
        relationships.push({
          from: post.$id as string,
          predicate: 'categories',
          to: cat.$id as string,
        })
      }

      // Generate comments for each post
      for (let c = 0; c < commentsPerPost; c++) {
        const commentIdx = postIdx * commentsPerPost + c
        const comment = generateEntity('Comment', commentIdx)
        comment.$id = `comments/comment-${commentIdx}` as EntityId
        comments.push(comment)

        // Link comment to post
        relationships.push({
          from: comment.$id as string,
          predicate: 'post',
          to: post.$id as string,
        })

        // Link comment to random user
        const commentAuthor = randomElement(users)
        relationships.push({
          from: comment.$id as string,
          predicate: 'author',
          to: commentAuthor.$id as string,
        })
      }
    }
  }

  return { users, posts, comments, categories, relationships }
}

// =============================================================================
// Memory Usage Utilities
// =============================================================================

/**
 * Get current memory usage (Node.js only)
 */
export function getMemoryUsage(): { heapUsed: number; heapTotal: number; external: number } | null {
  if (typeof process !== 'undefined' && process.memoryUsage) {
    const mem = process.memoryUsage()
    return {
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
      external: mem.external,
    }
  }
  return null
}

/**
 * Format bytes to human-readable string
 */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex++
  }

  return `${value.toFixed(2)} ${units[unitIndex]}`
}

// =============================================================================
// Test Schema
// =============================================================================

/**
 * Standard schema for benchmarks
 */
export const benchmarkSchema: Schema = {
  User: {
    $type: 'schema:Person',
    $ns: 'users',
    name: 'string!',
    email: { type: 'email!', index: 'unique' },
    username: 'string!',
    age: 'int?',
    active: { type: 'boolean', default: true },
    bio: 'text?',
    followerCount: 'int?',
    roles: 'string[]',
    settings: 'json?',
    posts: '<- Post.author[]',
    comments: '<- Comment.author[]',
  },
  Post: {
    $type: 'schema:BlogPosting',
    $ns: 'posts',
    $shred: ['status', 'publishedAt'],
    name: 'string!',
    title: 'string!',
    content: 'markdown!',
    excerpt: 'text?',
    status: { type: 'string', default: 'draft', index: true },
    views: { type: 'int', default: 0, index: true },
    likes: 'int?',
    readTime: 'int?',
    publishedAt: 'datetime?',
    tags: 'string[]',
    metadata: 'json?',
    author: '-> User.posts',
    categories: '-> Category.posts[]',
    comments: '<- Comment.post[]',
  },
  Comment: {
    $type: 'schema:Comment',
    $ns: 'comments',
    name: 'string!',
    text: 'string!',
    approved: { type: 'boolean', default: false },
    likes: 'int?',
    sentiment: 'string?',
    post: '-> Post.comments',
    author: '-> User.comments',
  },
  Category: {
    $type: 'schema:Category',
    $ns: 'categories',
    name: 'string!',
    slug: { type: 'string!', index: 'unique' },
    description: 'text?',
    postCount: 'int?',
    parentId: 'string?',
    featured: 'boolean?',
    color: 'string?',
    posts: '<- Post.categories[]',
  },
  Tag: {
    $type: 'schema:Tag',
    $ns: 'tags',
    name: 'string!',
    slug: { type: 'string!', index: 'unique' },
    usageCount: 'int?',
    trending: 'boolean?',
  },
  Product: {
    $type: 'schema:Product',
    $ns: 'products',
    name: 'string!',
    sku: { type: 'string!', index: 'unique' },
    price: 'float!',
    currency: 'string!',
    stock: 'int!',
    active: { type: 'boolean', default: true, index: true },
    category: { type: 'string', index: true },
    rating: 'float?',
    reviewCount: 'int?',
    attributes: 'json?',
  },
  Order: {
    $type: 'schema:Order',
    $ns: 'orders',
    name: 'string!',
    orderNumber: { type: 'string!', index: 'unique' },
    status: { type: 'string!', index: true },
    total: 'float!',
    currency: 'string!',
    itemCount: 'int!',
    shippedAt: 'datetime?',
    shippingAddress: 'json?',
  },
}

// =============================================================================
// Mock Storage for Benchmarks
// =============================================================================

import { MemoryBackend } from '../../src/storage/MemoryBackend'

/**
 * Create a fresh MemoryBackend for benchmarks
 */
export function createBenchmarkStorage(): MemoryBackend {
  return new MemoryBackend()
}

// =============================================================================
// Real Data Loading for Benchmarks
// =============================================================================

/**
 * Supported dataset names
 */
export type DatasetName = 'onet' | 'imdb' | 'wiktionary' | 'unspsc' | 'wikidata' | 'commoncrawl'

/**
 * Data statistics for a loaded dataset
 */
export interface DataStats {
  /** Dataset name */
  dataset: DatasetName
  /** Path to the data directory */
  path: string
  /** Number of Parquet files found */
  fileCount: number
  /** Total size in bytes */
  totalSize: number
  /** Human-readable size */
  totalSizeFormatted: string
  /** Map of collection name to file info */
  collections: Map<string, { path: string; size: number }>
  /** Whether the dataset is available */
  available: boolean
}

/**
 * Get the data directory path for a dataset
 */
export function getDataPath(dataset: DatasetName): string {
  return join(process.cwd(), 'data', dataset)
}

/**
 * Check if a dataset is available on disk
 */
export async function datasetExists(dataset: DatasetName): Promise<boolean> {
  const dataPath = getDataPath(dataset)
  try {
    const stat = await fs.stat(dataPath)
    return stat.isDirectory()
  } catch {
    return false
  }
}

/**
 * Get statistics about a dataset's files
 */
export async function getDataStats(dataset: DatasetName): Promise<DataStats> {
  const dataPath = getDataPath(dataset)
  const stats: DataStats = {
    dataset,
    path: dataPath,
    fileCount: 0,
    totalSize: 0,
    totalSizeFormatted: '0 B',
    collections: new Map(),
    available: false,
  }

  try {
    const entries = await fs.readdir(dataPath, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = join(dataPath, entry.name)

      if (entry.isFile() && (entry.name.endsWith('.parquet') || entry.name.endsWith('.json'))) {
        const fileStat = await fs.stat(fullPath)
        stats.fileCount++
        stats.totalSize += fileStat.size

        // Extract collection name from filename (remove extension)
        const collectionName = entry.name.replace(/\.(parquet|json)$/, '')
        stats.collections.set(collectionName, {
          path: fullPath,
          size: fileStat.size,
        })
      } else if (entry.isDirectory()) {
        // Check for parquet files in subdirectories
        const subEntries = await fs.readdir(fullPath, { withFileTypes: true })
        for (const subEntry of subEntries) {
          if (subEntry.isFile() && (subEntry.name.endsWith('.parquet') || subEntry.name.endsWith('.json'))) {
            const subPath = join(fullPath, subEntry.name)
            const fileStat = await fs.stat(subPath)
            stats.fileCount++
            stats.totalSize += fileStat.size

            const collectionName = `${entry.name}/${subEntry.name.replace(/\.(parquet|json)$/, '')}`
            stats.collections.set(collectionName, {
              path: subPath,
              size: fileStat.size,
            })
          }
        }
      }
    }

    stats.available = stats.fileCount > 0
    stats.totalSizeFormatted = formatBytes(stats.totalSize)
  } catch {
    // Directory doesn't exist or can't be read
    stats.available = false
  }

  return stats
}

/**
 * Result of loading test data
 */
export interface LoadedTestData<T = Record<string, unknown>> {
  /** The storage backend pointing to the data */
  storage: FsBackend
  /** Parquet reader for querying data */
  reader: ParquetReader
  /** Statistics about the loaded data */
  stats: DataStats
  /** Helper to read a specific collection */
  readCollection: <R = T>(name: string, options?: { limit?: number; columns?: string[] }) => Promise<R[]>
  /** List available collections */
  listCollections: () => string[]
}

/**
 * Load test data from disk for benchmarking
 *
 * This function checks if the dataset exists at ./data/{dataset}/ and loads it
 * using FsBackend for real Parquet file reading.
 *
 * @param dataset - Name of the dataset to load (e.g., 'onet', 'imdb')
 * @throws Error if the dataset doesn't exist with instructions to load it
 *
 * @example
 * ```typescript
 * const data = await loadTestData('onet')
 * console.log(`Loaded ${data.stats.fileCount} files (${data.stats.totalSizeFormatted})`)
 *
 * // Read a specific collection
 * const occupations = await data.readCollection('occupations', { limit: 100 })
 *
 * // List available collections
 * const collections = data.listCollections()
 * ```
 */
export async function loadTestData<T = Record<string, unknown>>(
  dataset: DatasetName
): Promise<LoadedTestData<T>> {
  const dataPath = getDataPath(dataset)
  const exists = await datasetExists(dataset)

  if (!exists) {
    const loadCommands: Record<DatasetName, string> = {
      onet: 'npm run load:onet',
      imdb: 'npm run load:imdb',
      wiktionary: 'npm run load:wiktionary',
      unspsc: 'npm run load:unspsc',
      wikidata: 'npm run load:wikidata',
      commoncrawl: 'npm run load:commoncrawl',
    }

    const cmd = loadCommands[dataset] || `npx tsx examples/${dataset}/load.ts`

    throw new Error(
      `Dataset "${dataset}" not found at ${dataPath}\n\n` +
      `Run \`${cmd}\` first to load test data.\n\n` +
      `This benchmark requires real data files - no network calls are made during benchmark runs.`
    )
  }

  const stats = await getDataStats(dataset)
  const storage = new FsBackend(dataPath)
  const reader = new ParquetReader({ storage })

  // Helper to find the actual file path for a collection
  const findCollectionPath = (name: string): string | null => {
    // Check direct match
    const collection = stats.collections.get(name)
    if (collection) {
      return collection.path.replace(dataPath + '/', '')
    }

    // Check with parquet extension
    const parquetPath = stats.collections.get(`${name}`)
    if (parquetPath) {
      return parquetPath.path.replace(dataPath + '/', '')
    }

    // Check in parquet subdirectory
    for (const [collName, info] of stats.collections) {
      if (collName.includes(name) || collName.endsWith(`/${name}`)) {
        return info.path.replace(dataPath + '/', '')
      }
    }

    return null
  }

  return {
    storage,
    reader,
    stats,

    readCollection: async <R = T>(
      name: string,
      options?: { limit?: number; columns?: string[] }
    ): Promise<R[]> => {
      const relativePath = findCollectionPath(name)
      if (!relativePath) {
        throw new Error(
          `Collection "${name}" not found in dataset "${dataset}". ` +
          `Available: ${Array.from(stats.collections.keys()).join(', ')}`
        )
      }

      // Check if it's a JSON file (JSONL format)
      if (relativePath.endsWith('.json')) {
        const content = await storage.read(relativePath)
        const text = new TextDecoder().decode(content)
        const lines = text.trim().split('\n')
        const data = lines.map(line => JSON.parse(line) as R)
        return options?.limit ? data.slice(0, options.limit) : data
      }

      // Read Parquet file
      return reader.read<R>(relativePath, options)
    },

    listCollections: (): string[] => {
      return Array.from(stats.collections.keys())
    },
  }
}

/**
 * Check data availability and print status
 *
 * Call this at the start of benchmark suites to provide helpful messages
 * about missing data.
 *
 * @param datasets - List of datasets to check
 * @returns Object with availability status for each dataset
 */
export async function checkDataAvailability(
  datasets: DatasetName[]
): Promise<Record<DatasetName, DataStats>> {
  const results: Record<string, DataStats> = {}

  console.log('\n' + '='.repeat(60))
  console.log('Checking Data Availability')
  console.log('='.repeat(60))

  for (const dataset of datasets) {
    const stats = await getDataStats(dataset)
    results[dataset] = stats

    if (stats.available) {
      console.log(`  [OK] ${dataset}: ${stats.fileCount} files (${stats.totalSizeFormatted})`)
    } else {
      console.log(`  [--] ${dataset}: Not loaded`)
    }
  }

  console.log('='.repeat(60) + '\n')

  return results as Record<DatasetName, DataStats>
}

/**
 * Skip benchmark if data is not available
 *
 * Use this in beforeAll/beforeEach to conditionally skip benchmarks
 * when test data hasn't been loaded.
 *
 * @param dataset - Dataset name to check
 * @param skipFn - Vitest skip function (e.g., describe.skip or test.skip)
 */
export async function skipIfNoData(
  dataset: DatasetName,
  skipFn: (message: string) => void
): Promise<boolean> {
  const exists = await datasetExists(dataset)
  if (!exists) {
    skipFn(`Dataset "${dataset}" not loaded. Run load script first.`)
    return true
  }
  return false
}

// =============================================================================
// Exports
// =============================================================================

export type { Entity, EntityId, CreateInput, Schema, StorageBackend }

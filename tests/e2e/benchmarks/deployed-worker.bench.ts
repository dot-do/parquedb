/**
 * E2E Benchmark Suite for Deployed Cloudflare Workers
 *
 * This benchmark suite tests ParqueDB performance against actual deployed workers.
 * It measures latency (p50, p99) and throughput for:
 * - CRUD operations (create, read, update, delete)
 * - Query operations (filters, sorting, pagination)
 * - Relationship operations (link, populate)
 * - Cold start performance
 * - Cache performance
 *
 * Performance Targets (from CLAUDE.md):
 * | Operation           | Target (p50) | Target (p99) |
 * |---------------------|--------------|--------------|
 * | Get by ID           | 5ms          | 20ms         |
 * | Find (indexed)      | 20ms         | 100ms        |
 * | Find (scan)         | 100ms        | 500ms        |
 * | Create              | 10ms         | 50ms         |
 * | Update              | 15ms         | 75ms         |
 * | Relationship traverse| 50ms        | 200ms        |
 *
 * Usage:
 *   WORKER_URL=https://api.parquedb.com pnpm test:e2e:bench
 *
 * Or with default URL:
 *   pnpm test:e2e:bench
 */

import { describe, bench, beforeAll, beforeEach, afterAll, expect } from 'vitest'
import {
  timedFetch,
  runBenchmarkIterations,
  runConcurrentRequests,
  calculateLatencyStats,
  buildTestResult,
  formatMs,
  formatPercent,
  formatSpeedup,
  sleep,
  withRetry,
} from './utils'
import type {
  E2EBenchmarkConfig,
  LatencyStats,
  RequestResult,
  BenchmarkTestResult,
  ColdStartResult,
  CachePerformanceResult,
  ConcurrencyResult,
} from './types'
import { DEFAULT_E2E_CONFIG } from './types'

// =============================================================================
// Configuration
// =============================================================================

/**
 * Get worker URL from environment or use default
 */
function getWorkerUrl(): string {
  return process.env.WORKER_URL || process.env.PARQUEDB_URL || DEFAULT_E2E_CONFIG.url
}

/**
 * Performance targets from CLAUDE.md
 */
const PERFORMANCE_TARGETS = {
  getById: { p50: 5, p99: 20 },
  findIndexed: { p50: 20, p99: 100 },
  findScan: { p50: 100, p99: 500 },
  create: { p50: 10, p99: 50 },
  update: { p50: 15, p99: 75 },
  relationshipTraverse: { p50: 50, p99: 200 },
} as const

/**
 * Default test configuration
 */
const TEST_CONFIG = {
  iterations: 10,
  warmup: 2,
  timeout: 30000,
  concurrencyLevels: [1, 5, 10, 25],
}

// =============================================================================
// Test Data
// =============================================================================

interface TestPost {
  $type: string
  name: string
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  views: number
  tags: string[]
}

interface TestUser {
  $type: string
  name: string
  email: string
  role: 'admin' | 'user' | 'guest'
}

/**
 * Generate a test post
 */
function generatePost(index: number): TestPost {
  return {
    $type: 'Post',
    name: `Benchmark Post ${index}`,
    title: `Benchmark Test Post ${index}: ${Date.now()}`,
    content: `This is benchmark content for post ${index}. Lorem ipsum dolor sit amet, consectetur adipiscing elit.`,
    status: index % 3 === 0 ? 'published' : index % 3 === 1 ? 'draft' : 'archived',
    views: Math.floor(Math.random() * 10000),
    tags: ['benchmark', 'test', `tag-${index % 5}`],
  }
}

/**
 * Generate a test user
 */
function generateUser(index: number): TestUser {
  return {
    $type: 'User',
    name: `Benchmark User ${index}`,
    email: `benchmark-user-${index}-${Date.now()}@example.com`,
    role: index % 3 === 0 ? 'admin' : index % 3 === 1 ? 'user' : 'guest',
  }
}

// =============================================================================
// API Helper Functions
// =============================================================================

/**
 * Make a request to the deployed worker
 */
async function workerRequest(
  path: string,
  options: RequestInit = {},
  timeout: number = TEST_CONFIG.timeout
): Promise<RequestResult> {
  const url = `${getWorkerUrl()}${path}`
  return timedFetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  }, timeout)
}

/**
 * Create an entity via the API
 */
async function createEntity(namespace: string, data: Record<string, unknown>): Promise<RequestResult> {
  return workerRequest(`/api/${namespace}`, {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

/**
 * Get an entity by ID
 */
async function getEntity(namespace: string, id: string): Promise<RequestResult> {
  return workerRequest(`/api/${namespace}/${id}`)
}

/**
 * Update an entity
 */
async function updateEntity(namespace: string, id: string, update: Record<string, unknown>): Promise<RequestResult> {
  return workerRequest(`/api/${namespace}/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(update),
  })
}

/**
 * Delete an entity
 */
async function deleteEntity(namespace: string, id: string): Promise<RequestResult> {
  return workerRequest(`/api/${namespace}/${id}`, {
    method: 'DELETE',
  })
}

/**
 * Find entities with optional filter
 */
async function findEntities(
  namespace: string,
  filter?: Record<string, unknown>,
  options?: { limit?: number; sort?: Record<string, number> }
): Promise<RequestResult> {
  const params = new URLSearchParams()
  if (filter) {
    params.set('filter', JSON.stringify(filter))
  }
  if (options?.limit) {
    params.set('limit', String(options.limit))
  }
  if (options?.sort) {
    params.set('sort', JSON.stringify(options.sort))
  }
  const query = params.toString()
  return workerRequest(`/api/${namespace}${query ? `?${query}` : ''}`)
}

// =============================================================================
// Benchmark Suite
// =============================================================================

describe('E2E Deployed Worker Benchmarks', () => {
  const workerUrl = getWorkerUrl()
  let createdPostIds: string[] = []
  let createdUserIds: string[] = []

  beforeAll(async () => {
    console.log(`\n${'='.repeat(70)}`)
    console.log('E2E BENCHMARK SUITE - DEPLOYED WORKERS')
    console.log('='.repeat(70))
    console.log(`Worker URL: ${workerUrl}`)
    console.log(`Iterations: ${TEST_CONFIG.iterations}`)
    console.log(`Warmup: ${TEST_CONFIG.warmup}`)
    console.log('='.repeat(70) + '\n')

    // Health check
    const health = await workerRequest('/health')
    if (!health.success) {
      console.error(`Worker health check failed: ${health.error}`)
      throw new Error(`Worker not available at ${workerUrl}: ${health.error}`)
    }
    console.log(`Health check passed (${formatMs(health.latencyMs)})`)

    // Seed some test data for read operations
    console.log('\nSeeding test data...')
    for (let i = 0; i < 20; i++) {
      const postResult = await createEntity('posts', generatePost(i))
      if (postResult.success && postResult.data) {
        const post = postResult.data as { $id?: string }
        if (post.$id) {
          createdPostIds.push(post.$id)
        }
      }

      const userResult = await createEntity('users', generateUser(i))
      if (userResult.success && userResult.data) {
        const user = userResult.data as { $id?: string }
        if (user.$id) {
          createdUserIds.push(user.$id)
        }
      }
    }
    console.log(`Seeded ${createdPostIds.length} posts and ${createdUserIds.length} users\n`)
  })

  afterAll(async () => {
    // Cleanup test data
    console.log('\nCleaning up test data...')
    for (const id of createdPostIds) {
      await deleteEntity('posts', id.replace('posts/', ''))
    }
    for (const id of createdUserIds) {
      await deleteEntity('users', id.replace('users/', ''))
    }
    console.log('Cleanup complete\n')
  })

  // ===========================================================================
  // Cold Start Benchmarks
  // ===========================================================================

  describe('Cold Start', () => {
    bench('cold start - health endpoint', async () => {
      // Wait long enough for isolate to potentially be evicted
      await sleep(100)
      await workerRequest('/health')
    }, { iterations: 5, warmup: 0 })

    bench('cold start - API endpoint', async () => {
      await sleep(100)
      await workerRequest('/api/posts?limit=1')
    }, { iterations: 5, warmup: 0 })
  })

  // ===========================================================================
  // CRUD Operation Benchmarks
  // ===========================================================================

  describe('CRUD Operations', () => {
    let tempPostId: string | null = null

    describe('Create Operations', () => {
      bench('create single entity', async () => {
        const result = await createEntity('posts', generatePost(Date.now()))
        if (result.success && result.data) {
          const post = result.data as { $id?: string }
          if (post.$id) {
            // Track for cleanup
            createdPostIds.push(post.$id)
          }
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('create entity with relationships', async () => {
        // Create a post with author relationship
        const post = {
          ...generatePost(Date.now()),
          author: createdUserIds[0] ? { 'Author': createdUserIds[0] } : undefined,
        }
        const result = await createEntity('posts', post)
        if (result.success && result.data) {
          const created = result.data as { $id?: string }
          if (created.$id) {
            createdPostIds.push(created.$id)
          }
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })

    describe('Read Operations', () => {
      bench('get by ID', async () => {
        if (createdPostIds.length > 0) {
          const id = createdPostIds[Math.floor(Math.random() * createdPostIds.length)]
          await getEntity('posts', id.replace('posts/', ''))
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('get with projection', async () => {
        if (createdPostIds.length > 0) {
          const id = createdPostIds[Math.floor(Math.random() * createdPostIds.length)]
          await workerRequest(`/api/posts/${id.replace('posts/', '')}?fields=title,status,views`)
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('get non-existent ID', async () => {
        await getEntity('posts', 'non-existent-id-12345')
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })

    describe('Update Operations', () => {
      beforeEach(async () => {
        // Create a fresh entity for update tests
        const result = await createEntity('posts', generatePost(Date.now()))
        if (result.success && result.data) {
          const post = result.data as { $id?: string }
          tempPostId = post.$id || null
          if (tempPostId) {
            createdPostIds.push(tempPostId)
          }
        }
      })

      bench('update single field ($set)', async () => {
        if (tempPostId) {
          await updateEntity('posts', tempPostId.replace('posts/', ''), {
            $set: { title: `Updated Title ${Date.now()}` },
          })
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('update multiple fields ($set)', async () => {
        if (tempPostId) {
          await updateEntity('posts', tempPostId.replace('posts/', ''), {
            $set: {
              title: `Updated Title ${Date.now()}`,
              content: 'Updated content',
              status: 'published',
            },
          })
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('update with $inc operator', async () => {
        if (tempPostId) {
          await updateEntity('posts', tempPostId.replace('posts/', ''), {
            $inc: { views: 1 },
          })
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('update with $push operator', async () => {
        if (tempPostId) {
          await updateEntity('posts', tempPostId.replace('posts/', ''), {
            $push: { tags: 'new-tag' },
          })
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('update with combined operators', async () => {
        if (tempPostId) {
          await updateEntity('posts', tempPostId.replace('posts/', ''), {
            $set: { status: 'published' },
            $inc: { views: 10 },
            $push: { tags: 'featured' },
          })
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })

    describe('Delete Operations', () => {
      bench('delete single entity', async () => {
        // Create then delete
        const result = await createEntity('posts', generatePost(Date.now()))
        if (result.success && result.data) {
          const post = result.data as { $id?: string }
          if (post.$id) {
            await deleteEntity('posts', post.$id.replace('posts/', ''))
          }
        }
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })
  })

  // ===========================================================================
  // Query Benchmarks
  // ===========================================================================

  describe('Query Operations', () => {
    describe('Find Operations', () => {
      bench('find all (no filter)', async () => {
        await findEntities('posts', undefined, { limit: 100 })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with simple equality filter', async () => {
        await findEntities('posts', { status: 'published' })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with comparison filter ($gt)', async () => {
        await findEntities('posts', { views: { $gt: 5000 } })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with range filter ($gte, $lt)', async () => {
        await findEntities('posts', { views: { $gte: 1000, $lt: 5000 } })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with $in operator', async () => {
        await findEntities('posts', { status: { $in: ['published', 'archived'] } })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with $and filter', async () => {
        await findEntities('posts', {
          $and: [
            { status: 'published' },
            { views: { $gt: 1000 } },
          ],
        })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with $or filter', async () => {
        await findEntities('posts', {
          $or: [
            { status: 'draft' },
            { views: { $gt: 5000 } },
          ],
        })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with complex nested filter', async () => {
        await findEntities('posts', {
          $and: [
            {
              $or: [
                { status: 'published' },
                { status: 'archived' },
              ],
            },
            { views: { $gte: 100 } },
          ],
        })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })

    describe('Pagination', () => {
      bench('find with limit (10)', async () => {
        await findEntities('posts', {}, { limit: 10 })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with limit (100)', async () => {
        await findEntities('posts', {}, { limit: 100 })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with sort (single field)', async () => {
        await findEntities('posts', {}, { sort: { views: -1 }, limit: 20 })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

      bench('find with filter + sort + limit', async () => {
        await findEntities('posts', { status: 'published' }, { sort: { views: -1 }, limit: 10 })
      }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
    })
  })

  // ===========================================================================
  // Relationship Benchmarks
  // ===========================================================================

  describe('Relationship Operations', () => {
    let linkedPostId: string | null = null

    beforeAll(async () => {
      // Create a post with author relationship for traversal tests
      if (createdUserIds.length > 0) {
        const post = {
          ...generatePost(Date.now()),
          author: { 'Author': createdUserIds[0] },
        }
        const result = await createEntity('posts', post)
        if (result.success && result.data) {
          const created = result.data as { $id?: string }
          linkedPostId = created.$id || null
          if (linkedPostId) {
            createdPostIds.push(linkedPostId)
          }
        }
      }
    })

    bench('link single entity (post -> author)', async () => {
      if (createdPostIds.length > 0 && createdUserIds.length > 0) {
        const postId = createdPostIds[Math.floor(Math.random() * createdPostIds.length)]
        const userId = createdUserIds[Math.floor(Math.random() * createdUserIds.length)]
        await updateEntity('posts', postId.replace('posts/', ''), {
          $link: { author: userId },
        })
      }
    }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

    bench('unlink entity', async () => {
      if (createdPostIds.length > 0 && createdUserIds.length > 0) {
        const postId = createdPostIds[Math.floor(Math.random() * createdPostIds.length)]
        const userId = createdUserIds[Math.floor(Math.random() * createdUserIds.length)]
        await updateEntity('posts', postId.replace('posts/', ''), {
          $unlink: { author: userId },
        })
      }
    }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

    bench('find with populate (author)', async () => {
      await workerRequest('/api/posts?populate=author&limit=10')
    }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })

    bench('find posts by author (reverse lookup)', async () => {
      if (createdUserIds.length > 0) {
        const userId = createdUserIds[0]
        await findEntities('posts', { author: { $eq: userId } })
      }
    }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
  })

  // ===========================================================================
  // Concurrency Benchmarks
  // ===========================================================================

  describe('Concurrency', () => {
    for (const concurrency of TEST_CONFIG.concurrencyLevels) {
      bench(`${concurrency} concurrent read requests`, async () => {
        const requests = Array.from({ length: concurrency }, () =>
          findEntities('posts', {}, { limit: 10 })
        )
        await Promise.all(requests)
      }, { iterations: 5, warmup: 1 })

      bench(`${concurrency} concurrent write requests`, async () => {
        const requests = Array.from({ length: concurrency }, (_, i) =>
          createEntity('posts', generatePost(Date.now() + i))
        )
        const results = await Promise.all(requests)

        // Track created IDs for cleanup
        for (const result of results) {
          if (result.success && result.data) {
            const post = result.data as { $id?: string }
            if (post.$id) {
              createdPostIds.push(post.$id)
            }
          }
        }
      }, { iterations: 3, warmup: 1 })

      bench(`${concurrency} concurrent mixed requests`, async () => {
        const requests = Array.from({ length: concurrency }, (_, i) => {
          if (i % 2 === 0) {
            return findEntities('posts', {}, { limit: 10 })
          } else {
            return createEntity('posts', generatePost(Date.now() + i))
          }
        })
        const results = await Promise.all(requests)

        // Track created IDs for cleanup
        for (const result of results) {
          if (result.success && result.data) {
            const post = result.data as { $id?: string }
            if (post.$id && !createdPostIds.includes(post.$id)) {
              createdPostIds.push(post.$id)
            }
          }
        }
      }, { iterations: 3, warmup: 1 })
    }
  })

  // ===========================================================================
  // Cache Performance Benchmarks
  // ===========================================================================

  describe('Cache Performance', () => {
    bench('first request (cache miss)', async () => {
      // Use a unique query to ensure cache miss
      const uniqueParam = Date.now()
      await workerRequest(`/api/posts?_cache_bust=${uniqueParam}&limit=10`)
    }, { iterations: TEST_CONFIG.iterations, warmup: 0 })

    bench('repeated request (potential cache hit)', async () => {
      // Same request multiple times to test cache
      await findEntities('posts', { status: 'published' }, { limit: 10 })
    }, { iterations: TEST_CONFIG.iterations, warmup: TEST_CONFIG.warmup })
  })

  // ===========================================================================
  // Performance Target Validation
  // ===========================================================================

  describe('Performance Target Validation', () => {
    bench(`get by ID (target: p50<${PERFORMANCE_TARGETS.getById.p50}ms, p99<${PERFORMANCE_TARGETS.getById.p99}ms)`, async () => {
      if (createdPostIds.length > 0) {
        const id = createdPostIds[0]
        await getEntity('posts', id.replace('posts/', ''))
      }
    }, { iterations: 20, warmup: 5 })

    bench(`find indexed (target: p50<${PERFORMANCE_TARGETS.findIndexed.p50}ms, p99<${PERFORMANCE_TARGETS.findIndexed.p99}ms)`, async () => {
      await findEntities('posts', { status: 'published' }, { limit: 20 })
    }, { iterations: 20, warmup: 5 })

    bench(`find scan (target: p50<${PERFORMANCE_TARGETS.findScan.p50}ms, p99<${PERFORMANCE_TARGETS.findScan.p99}ms)`, async () => {
      await findEntities('posts', { views: { $gt: 0 } }, { limit: 100 })
    }, { iterations: 20, warmup: 5 })

    bench(`create (target: p50<${PERFORMANCE_TARGETS.create.p50}ms, p99<${PERFORMANCE_TARGETS.create.p99}ms)`, async () => {
      const result = await createEntity('posts', generatePost(Date.now()))
      if (result.success && result.data) {
        const post = result.data as { $id?: string }
        if (post.$id) {
          createdPostIds.push(post.$id)
        }
      }
    }, { iterations: 20, warmup: 5 })

    bench(`update (target: p50<${PERFORMANCE_TARGETS.update.p50}ms, p99<${PERFORMANCE_TARGETS.update.p99}ms)`, async () => {
      if (createdPostIds.length > 0) {
        const id = createdPostIds[0]
        await updateEntity('posts', id.replace('posts/', ''), {
          $inc: { views: 1 },
        })
      }
    }, { iterations: 20, warmup: 5 })

    bench(`relationship traverse (target: p50<${PERFORMANCE_TARGETS.relationshipTraverse.p50}ms, p99<${PERFORMANCE_TARGETS.relationshipTraverse.p99}ms)`, async () => {
      await workerRequest('/api/posts?populate=author&limit=10')
    }, { iterations: 20, warmup: 5 })
  })
})

/**
 * Cloudflare Workers Benchmarks
 *
 * Benchmarks Worker to Durable Object and R2 latencies.
 * Uses test-based benchmarking since vitest bench is not fully supported in vitest-pool-workers.
 *
 * Run with: npx vitest run tests/benchmarks/workers.workers.bench.ts --project 'e2e:bench'
 *
 * Tests:
 * - Worker to DO RPC latency (stub RPC call)
 * - Worker to R2 read latency
 * - Worker to R2 write latency
 * - DO SQLite query latency
 * - Cold start vs warm performance
 * - Sequential vs concurrent requests
 * - Small vs large payloads
 */

import { env } from 'cloudflare:test'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'

// Type for the env bindings from wrangler.jsonc
interface TestEnv {
  BUCKET: R2Bucket
  PARQUEDB: DurableObjectNamespace
  ENVIRONMENT: string
}

// Cast env to our typed environment
const testEnv = env as TestEnv

// =============================================================================
// Benchmarking Utilities
// =============================================================================

interface BenchmarkResult {
  name: string
  iterations: number
  totalMs: number
  meanMs: number
  minMs: number
  maxMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  opsPerSecond: number
}

/**
 * Run a micro-benchmark and collect statistics
 */
async function runBenchmark(
  name: string,
  fn: () => Promise<void>,
  options: { iterations?: number; warmup?: number } = {}
): Promise<BenchmarkResult> {
  const { iterations = 50, warmup = 5 } = options

  // Warmup runs
  for (let i = 0; i < warmup; i++) {
    await fn()
  }

  // Timed runs
  const timings: number[] = []
  const startTotal = performance.now()

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    const end = performance.now()
    timings.push(end - start)
  }

  const endTotal = performance.now()
  const totalMs = endTotal - startTotal

  // Calculate statistics
  const sorted = [...timings].sort((a, b) => a - b)
  const meanMs = timings.reduce((a, b) => a + b, 0) / iterations
  const minMs = sorted[0]
  const maxMs = sorted[sorted.length - 1]
  const p50Ms = sorted[Math.floor(iterations * 0.5)]
  const p95Ms = sorted[Math.floor(iterations * 0.95)]
  const p99Ms = sorted[Math.floor(iterations * 0.99)]
  const opsPerSecond = (iterations / totalMs) * 1000

  return {
    name,
    iterations,
    totalMs,
    meanMs,
    minMs,
    maxMs,
    p50Ms,
    p95Ms,
    p99Ms,
    opsPerSecond,
  }
}

/**
 * Format benchmark result for console output
 */
function formatResult(result: BenchmarkResult): string {
  return [
    `${result.name}:`,
    `  iterations: ${result.iterations}`,
    `  mean: ${result.meanMs.toFixed(3)}ms`,
    `  min: ${result.minMs.toFixed(3)}ms`,
    `  max: ${result.maxMs.toFixed(3)}ms`,
    `  p50: ${result.p50Ms.toFixed(3)}ms`,
    `  p95: ${result.p95Ms.toFixed(3)}ms`,
    `  p99: ${result.p99Ms.toFixed(3)}ms`,
    `  ops/sec: ${result.opsPerSecond.toFixed(1)}`,
  ].join('\n')
}

// Collect all results for final summary
const allResults: BenchmarkResult[] = []

// =============================================================================
// Test Data
// =============================================================================

/** Small payload (~100 bytes) */
const SMALL_PAYLOAD = JSON.stringify({
  $type: 'Post',
  name: 'Test Post',
  title: 'Benchmark Test',
  status: 'draft',
})

/** Medium payload (~1KB) */
const MEDIUM_PAYLOAD = JSON.stringify({
  $type: 'Post',
  name: 'Medium Test Post',
  title: 'Benchmark Test with Medium Payload',
  content: 'x'.repeat(800),
  status: 'published',
  tags: ['benchmark', 'test', 'performance'],
  metadata: {
    views: 1000,
    likes: 50,
    shares: 10,
  },
})

/** Large payload (~10KB) */
const LARGE_PAYLOAD = JSON.stringify({
  $type: 'Post',
  name: 'Large Test Post',
  title: 'Benchmark Test with Large Payload',
  content: 'x'.repeat(9000),
  status: 'published',
  tags: Array.from({ length: 20 }, (_, i) => `tag-${i}`),
  metadata: {
    views: 100000,
    likes: 5000,
    shares: 1000,
    comments: Array.from({ length: 50 }, (_, i) => ({
      id: i,
      text: `Comment ${i} with some additional text`,
      author: `user-${i}`,
    })),
  },
})

// Binary test data
const SMALL_BINARY = new Uint8Array(100).fill(42)
const MEDIUM_BINARY = new Uint8Array(1024).fill(42)
const LARGE_BINARY = new Uint8Array(10 * 1024).fill(42)

// =============================================================================
// R2 Bucket Benchmarks
// =============================================================================

describe('R2 Bucket Benchmarks', () => {
  const testPrefix = `bench-${Date.now()}`
  let objectCounter = 0

  afterAll(async () => {
    // Cleanup all benchmark objects
    try {
      const listed = await testEnv.BUCKET.list({ prefix: testPrefix })
      for (const obj of listed.objects) {
        await testEnv.BUCKET.delete(obj.key)
      }
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('R2 Write Latency', () => {
    it('benchmarks R2 writes', async () => {
      const results: BenchmarkResult[] = []

      // Small payload write
      results.push(await runBenchmark('R2 write - small (100B)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/small-${objectCounter}.json`, SMALL_PAYLOAD)
      }))

      // Medium payload write
      results.push(await runBenchmark('R2 write - medium (1KB)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/medium-${objectCounter}.json`, MEDIUM_PAYLOAD)
      }))

      // Large payload write
      results.push(await runBenchmark('R2 write - large (10KB)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/large-${objectCounter}.json`, LARGE_PAYLOAD)
      }))

      // Binary writes
      results.push(await runBenchmark('R2 write - binary small (100B)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/bin-small-${objectCounter}`, SMALL_BINARY)
      }))

      results.push(await runBenchmark('R2 write - binary medium (1KB)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/bin-medium-${objectCounter}`, MEDIUM_BINARY)
      }))

      results.push(await runBenchmark('R2 write - binary large (10KB)', async () => {
        objectCounter++
        await testEnv.BUCKET.put(`${testPrefix}/bin-large-${objectCounter}`, LARGE_BINARY)
      }))

      // Log results
      console.log('\n=== R2 Write Latency Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      // Verify benchmarks ran
      expect(results.length).toBe(6)
      // Note: workerd timing resolution is ~1ms, some ops complete faster
      for (const r of results) {
        expect(r.meanMs).toBeGreaterThanOrEqual(0)
      }
    })
  })

  describe('R2 Read Latency', () => {
    const readKeys = {
      small: '',
      medium: '',
      large: '',
      smallBin: '',
      mediumBin: '',
      largeBin: '',
    }

    beforeAll(async () => {
      // Pre-create objects for reading
      readKeys.small = `${testPrefix}/read-small.json`
      readKeys.medium = `${testPrefix}/read-medium.json`
      readKeys.large = `${testPrefix}/read-large.json`
      readKeys.smallBin = `${testPrefix}/read-small.bin`
      readKeys.mediumBin = `${testPrefix}/read-medium.bin`
      readKeys.largeBin = `${testPrefix}/read-large.bin`

      await Promise.all([
        testEnv.BUCKET.put(readKeys.small, SMALL_PAYLOAD),
        testEnv.BUCKET.put(readKeys.medium, MEDIUM_PAYLOAD),
        testEnv.BUCKET.put(readKeys.large, LARGE_PAYLOAD),
        testEnv.BUCKET.put(readKeys.smallBin, SMALL_BINARY),
        testEnv.BUCKET.put(readKeys.mediumBin, MEDIUM_BINARY),
        testEnv.BUCKET.put(readKeys.largeBin, LARGE_BINARY),
      ])
    })

    it('benchmarks R2 reads', async () => {
      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('R2 read - small (100B)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.small)
        if (obj) await obj.text()
      }))

      results.push(await runBenchmark('R2 read - medium (1KB)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.medium)
        if (obj) await obj.text()
      }))

      results.push(await runBenchmark('R2 read - large (10KB)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.large)
        if (obj) await obj.text()
      }))

      results.push(await runBenchmark('R2 read - binary small (100B)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.smallBin)
        if (obj) await obj.arrayBuffer()
      }))

      results.push(await runBenchmark('R2 read - binary medium (1KB)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.mediumBin)
        if (obj) await obj.arrayBuffer()
      }))

      results.push(await runBenchmark('R2 read - binary large (10KB)', async () => {
        const obj = await testEnv.BUCKET.get(readKeys.largeBin)
        if (obj) await obj.arrayBuffer()
      }))

      results.push(await runBenchmark('R2 head - check existence', async () => {
        await testEnv.BUCKET.head(readKeys.small)
      }))

      results.push(await runBenchmark('R2 read - non-existent key', async () => {
        await testEnv.BUCKET.get(`${testPrefix}/nonexistent-key`)
      }))

      // Log results
      console.log('\n=== R2 Read Latency Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(8)
    })
  })

  describe('R2 List Latency', () => {
    beforeAll(async () => {
      // Create 100 objects for list testing
      const promises = []
      for (let i = 0; i < 100; i++) {
        promises.push(
          testEnv.BUCKET.put(`${testPrefix}/list-test/item-${String(i).padStart(3, '0')}.json`, `{"id":${i}}`)
        )
      }
      await Promise.all(promises)
    })

    it('benchmarks R2 list operations', async () => {
      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('R2 list - 10 items', async () => {
        await testEnv.BUCKET.list({ prefix: `${testPrefix}/list-test/`, limit: 10 })
      }))

      results.push(await runBenchmark('R2 list - 100 items', async () => {
        await testEnv.BUCKET.list({ prefix: `${testPrefix}/list-test/`, limit: 100 })
      }))

      results.push(await runBenchmark('R2 list - with delimiter', async () => {
        await testEnv.BUCKET.list({ prefix: `${testPrefix}/`, delimiter: '/' })
      }))

      console.log('\n=== R2 List Latency Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(3)
    })
  })

  describe('R2 Concurrent Operations', () => {
    it('benchmarks R2 concurrent operations', async () => {
      const results: BenchmarkResult[] = []
      const concurrentPrefix = `${testPrefix}/concurrent`

      results.push(await runBenchmark('R2 concurrent writes - 5 parallel', async () => {
        const writes = Array.from({ length: 5 }, (_, i) =>
          testEnv.BUCKET.put(`${concurrentPrefix}/parallel-${Date.now()}-${i}.json`, SMALL_PAYLOAD)
        )
        await Promise.all(writes)
      }, { iterations: 20 }))

      results.push(await runBenchmark('R2 concurrent writes - 10 parallel', async () => {
        const writes = Array.from({ length: 10 }, (_, i) =>
          testEnv.BUCKET.put(`${concurrentPrefix}/parallel10-${Date.now()}-${i}.json`, SMALL_PAYLOAD)
        )
        await Promise.all(writes)
      }, { iterations: 20 }))

      // Prep for concurrent reads
      const readKeys = Array.from({ length: 10 }, (_, i) => `${concurrentPrefix}/read-${i}.json`)
      await Promise.all(readKeys.map(key => testEnv.BUCKET.put(key, SMALL_PAYLOAD)))

      results.push(await runBenchmark('R2 concurrent reads - 5 parallel', async () => {
        const reads = readKeys.slice(0, 5).map(key => testEnv.BUCKET.get(key).then(obj => obj?.text()))
        await Promise.all(reads)
      }, { iterations: 20 }))

      results.push(await runBenchmark('R2 concurrent reads - 10 parallel', async () => {
        const reads = readKeys.map(key => testEnv.BUCKET.get(key).then(obj => obj?.text()))
        await Promise.all(reads)
      }, { iterations: 20 }))

      console.log('\n=== R2 Concurrent Operations Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(4)
    })
  })
})

// =============================================================================
// Durable Object Benchmarks
// =============================================================================

describe('Durable Object Benchmarks', () => {
  describe('DO RPC Latency - Cold Start', () => {
    it('benchmarks DO cold start', async () => {
      const results: BenchmarkResult[] = []
      let stubCounter = 0

      results.push(await runBenchmark('DO cold start - first RPC (create)', async () => {
        stubCounter++
        const id = testEnv.PARQUEDB.idFromName(`cold-${Date.now()}-${stubCounter}`)
        const stub = testEnv.PARQUEDB.get(id) as unknown as {
          create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        }
        await stub.create('posts', {
          $type: 'Post',
          name: 'Cold Start Test',
          title: 'Testing Cold Start',
        }, {})
      }, { iterations: 20, warmup: 0 }))

      console.log('\n=== DO Cold Start Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(1)
    })
  })

  describe('DO RPC Latency - Warm', () => {
    it('benchmarks DO warm operations', async () => {
      // Create fresh stub and data within the test to avoid isolatedStorage issues
      const id = testEnv.PARQUEDB.idFromName(`warm-test-${Date.now()}`)
      const warmStub = testEnv.PARQUEDB.get(id) as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        get(ns: string, id: string): Promise<unknown>
        update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
        delete(ns: string, id: string, options?: unknown): Promise<boolean>
      }
      const createdIds: string[] = []

      // Warm up with initial data
      for (let i = 0; i < 10; i++) {
        const entity = await warmStub.create('posts', {
          $type: 'Post',
          name: `Warmup Post ${i}`,
          title: `Warmup Title ${i}`,
          content: 'Warmup content',
        }, {}) as { $id: string }
        createdIds.push(entity.$id)
      }

      const results: BenchmarkResult[] = []
      let readCounter = 0

      results.push(await runBenchmark('DO warm - create entity', async () => {
        const entity = await warmStub.create('posts', {
          $type: 'Post',
          name: `Bench Post ${Date.now()}`,
          title: 'Benchmark Title',
          content: 'Benchmark content',
        }, {}) as { $id: string }
        createdIds.push(entity.$id)
      }))

      results.push(await runBenchmark('DO warm - get by ID', async () => {
        readCounter++
        const entityId = createdIds[readCounter % createdIds.length]
        const idPart = entityId.split('/')[1]
        await warmStub.get('posts', idPart)
      }))

      results.push(await runBenchmark('DO warm - update ($set)', async () => {
        readCounter++
        const entityId = createdIds[readCounter % createdIds.length]
        const idPart = entityId.split('/')[1]
        await warmStub.update('posts', idPart, {
          $set: { title: `Updated Title ${Date.now()}` },
        }, {})
      }))

      results.push(await runBenchmark('DO warm - update ($inc)', async () => {
        readCounter++
        const entityId = createdIds[readCounter % createdIds.length]
        const idPart = entityId.split('/')[1]
        // First set viewCount to a number
        await warmStub.update('posts', idPart, { $set: { viewCount: 0 } }, {})
        await warmStub.update('posts', idPart, { $inc: { viewCount: 1 } }, {})
      }, { iterations: 25 }))

      console.log('\n=== DO Warm Operations Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(4)
    })
  })

  describe('DO SQLite Query Latency', () => {
    it('benchmarks DO SQLite queries', async () => {
      // Create fresh stub and data within the test to avoid isolatedStorage issues
      const id = testEnv.PARQUEDB.idFromName(`sqlite-bench-${Date.now()}`)
      const sqliteStub = testEnv.PARQUEDB.get(id) as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        get(ns: string, id: string): Promise<unknown>
        getRelationships(ns: string, id: string, predicate?: string, direction?: string): Promise<unknown[]>
        link(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
      }
      const postIds: string[] = []

      // Create an author
      const author = await sqliteStub.create('users', {
        $type: 'User',
        name: 'Benchmark Author',
        email: 'bench@example.com',
      }, {}) as { $id: string }
      const authorId = author.$id

      // Create 20 posts with relationships (reduced for speed)
      for (let i = 0; i < 20; i++) {
        const post = await sqliteStub.create('posts', {
          $type: 'Post',
          name: `SQLite Bench Post ${i}`,
          title: `SQLite Benchmark Title ${i}`,
          content: `Content for post ${i}`,
          status: i % 3 === 0 ? 'published' : 'draft',
          views: Math.floor(Math.random() * 10000),
        }, {}) as { $id: string }
        postIds.push(post.$id)

        // Link post to author
        await sqliteStub.link(post.$id, 'author', authorId, {})
      }

      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('DO SQLite - get by ID', async () => {
        const postId = postIds[Math.floor(Math.random() * postIds.length)]
        const idPart = postId.split('/')[1]
        await sqliteStub.get('posts', idPart)
      }))

      results.push(await runBenchmark('DO SQLite - get relationships (outbound)', async () => {
        const postId = postIds[Math.floor(Math.random() * postIds.length)]
        const idPart = postId.split('/')[1]
        await sqliteStub.getRelationships('posts', idPart, 'author', 'outbound')
      }))

      results.push(await runBenchmark('DO SQLite - get relationships (inbound)', async () => {
        const idPart = authorId.split('/')[1]
        await sqliteStub.getRelationships('users', idPart, undefined, 'inbound')
      }))

      console.log('\n=== DO SQLite Query Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(3)
    })
  })

  describe('DO Payload Sizes', () => {
    let payloadStub: {
      create(ns: string, data: unknown, options?: unknown): Promise<unknown>
    }

    beforeAll(() => {
      const id = testEnv.PARQUEDB.idFromName(`payload-bench-${Date.now()}`)
      payloadStub = testEnv.PARQUEDB.get(id) as unknown as typeof payloadStub
    })

    it('benchmarks DO with different payload sizes', async () => {
      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('DO create - small (~100B)', async () => {
        await payloadStub.create('posts', {
          $type: 'Post',
          name: 'Small',
          title: 'Small Payload',
        }, {})
      }))

      results.push(await runBenchmark('DO create - medium (~1KB)', async () => {
        await payloadStub.create('posts', {
          $type: 'Post',
          name: 'Medium',
          title: 'Medium Payload',
          content: 'x'.repeat(800),
          tags: ['a', 'b', 'c', 'd', 'e'],
        }, {})
      }))

      results.push(await runBenchmark('DO create - large (~10KB)', async () => {
        await payloadStub.create('posts', {
          $type: 'Post',
          name: 'Large',
          title: 'Large Payload',
          content: 'x'.repeat(9000),
          tags: Array.from({ length: 20 }, (_, i) => `tag-${i}`),
          metadata: {
            nested: {
              deeply: {
                nested: 'value'.repeat(100),
              },
            },
          },
        }, {})
      }))

      console.log('\n=== DO Payload Size Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(3)
    })
  })

  describe('DO Sequential vs Batched', () => {
    it('benchmarks sequential vs batched operations', async () => {
      // Create fresh stub and data within the test to avoid isolatedStorage issues
      const id = testEnv.PARQUEDB.idFromName(`batch-bench-${Date.now()}`)
      const batchStub = testEnv.PARQUEDB.get(id) as unknown as {
        create(ns: string, data: unknown, options?: unknown): Promise<unknown>
        get(ns: string, id: string): Promise<unknown>
      }
      const batchIds: string[] = []

      // Create initial data
      for (let i = 0; i < 10; i++) {
        const entity = await batchStub.create('posts', {
          $type: 'Post',
          name: `Batch Post ${i}`,
          title: `Batch Title ${i}`,
        }, {}) as { $id: string }
        batchIds.push(entity.$id)
      }

      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('DO sequential creates - 5', async () => {
        for (let i = 0; i < 5; i++) {
          await batchStub.create('posts', {
            $type: 'Post',
            name: `Sequential ${Date.now()}-${i}`,
            title: 'Sequential Create',
          }, {})
        }
      }, { iterations: 10 }))

      results.push(await runBenchmark('DO batched creates - 5 (Promise.all)', async () => {
        const creates = Array.from({ length: 5 }, (_, i) =>
          batchStub.create('posts', {
            $type: 'Post',
            name: `Batched ${Date.now()}-${i}`,
            title: 'Batched Create',
          }, {})
        )
        await Promise.all(creates)
      }, { iterations: 10 }))

      results.push(await runBenchmark('DO sequential reads - 5', async () => {
        for (let i = 0; i < 5; i++) {
          const entityId = batchIds[i % batchIds.length]
          const idPart = entityId.split('/')[1]
          await batchStub.get('posts', idPart)
        }
      }, { iterations: 20 }))

      results.push(await runBenchmark('DO batched reads - 5 (Promise.all)', async () => {
        const reads = Array.from({ length: 5 }, (_, i) => {
          const entityId = batchIds[i % batchIds.length]
          const idPart = entityId.split('/')[1]
          return batchStub.get('posts', idPart)
        })
        await Promise.all(reads)
      }, { iterations: 20 }))

      console.log('\n=== DO Sequential vs Batched Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(4)
    })
  })
})

// =============================================================================
// End-to-End Benchmarks
// =============================================================================

describe('End-to-End Benchmarks', () => {
  describe('Full CRUD Cycle', () => {
    let flowStub: {
      create(ns: string, data: unknown, options?: unknown): Promise<{ $id: string }>
      get(ns: string, id: string): Promise<unknown>
      update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
      delete(ns: string, id: string, options?: unknown): Promise<boolean>
    }

    beforeAll(() => {
      const id = testEnv.PARQUEDB.idFromName(`flow-bench-${Date.now()}`)
      flowStub = testEnv.PARQUEDB.get(id) as unknown as typeof flowStub
    })

    it('benchmarks full CRUD cycle', async () => {
      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('Full CRUD cycle (create->read->update->delete)', async () => {
        // Create
        const entity = await flowStub.create('posts', {
          $type: 'Post',
          name: 'CRUD Test',
          title: 'Full CRUD Cycle',
          content: 'Testing complete cycle',
        }, {})

        const id = entity.$id.split('/')[1]

        // Read
        await flowStub.get('posts', id)

        // Update
        await flowStub.update('posts', id, {
          $set: { title: 'Updated CRUD Cycle' },
        }, {})

        // Delete
        await flowStub.delete('posts', id, {})
      }, { iterations: 20 }))

      console.log('\n=== Full CRUD Cycle Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(1)
    })
  })

  describe('DO + R2 Combined', () => {
    let combinedStub: {
      create(ns: string, data: unknown, options?: unknown): Promise<{ $id: string }>
    }
    const r2Prefix = `bench-combined-${Date.now()}`

    beforeAll(() => {
      const id = testEnv.PARQUEDB.idFromName(`combined-bench-${Date.now()}`)
      combinedStub = testEnv.PARQUEDB.get(id) as unknown as typeof combinedStub
    })

    afterAll(async () => {
      try {
        const listed = await testEnv.BUCKET.list({ prefix: r2Prefix })
        for (const obj of listed.objects) {
          await testEnv.BUCKET.delete(obj.key)
        }
      } catch {
        // Ignore cleanup errors
      }
    })

    it('benchmarks DO + R2 combined operations', async () => {
      const results: BenchmarkResult[] = []

      results.push(await runBenchmark('DO create + R2 write (sequential)', async () => {
        // Create entity in DO
        const entity = await combinedStub.create('posts', {
          $type: 'Post',
          name: 'Combined Test',
          title: 'DO + R2 Test',
        }, {})

        // Write to R2
        await testEnv.BUCKET.put(`${r2Prefix}/${entity.$id.replace('/', '-')}.json`, JSON.stringify(entity))
      }, { iterations: 20 }))

      results.push(await runBenchmark('DO create + R2 write (parallel)', async () => {
        const ts = Date.now()
        const tempKey = `${r2Prefix}/temp-${ts}.json`

        // Start both operations in parallel
        const [entity] = await Promise.all([
          combinedStub.create('posts', {
            $type: 'Post',
            name: 'Parallel Combined Test',
            title: 'Parallel DO + R2 Test',
          }, {}),
          testEnv.BUCKET.put(tempKey, '{"pending":true}'),
        ])

        // Update R2 with actual data
        await testEnv.BUCKET.put(`${r2Prefix}/${entity.$id.replace('/', '-')}.json`, JSON.stringify(entity))
      }, { iterations: 20 }))

      console.log('\n=== DO + R2 Combined Results ===')
      for (const r of results) {
        console.log(formatResult(r))
        allResults.push(r)
      }

      expect(results.length).toBe(2)
    })
  })
})

// =============================================================================
// Final Summary
// =============================================================================

describe('Benchmark Summary', () => {
  it('prints final summary', () => {
    console.log('\n')
    console.log('='.repeat(80))
    console.log('                    CLOUDFLARE WORKERS BENCHMARK SUMMARY')
    console.log('='.repeat(80))
    console.log('')

    // Group by category
    const r2Results = allResults.filter(r => r.name.startsWith('R2'))
    const doResults = allResults.filter(r => r.name.startsWith('DO'))
    const e2eResults = allResults.filter(r => !r.name.startsWith('R2') && !r.name.startsWith('DO'))

    if (r2Results.length > 0) {
      console.log('R2 BUCKET OPERATIONS:')
      console.log('-'.repeat(80))
      console.log(`${'Operation'.padEnd(45)} ${'Mean'.padStart(10)} ${'P95'.padStart(10)} ${'Ops/sec'.padStart(12)}`)
      console.log('-'.repeat(80))
      for (const r of r2Results) {
        const name = r.name.length > 43 ? r.name.slice(0, 43) + '..' : r.name
        console.log(`${name.padEnd(45)} ${(r.meanMs.toFixed(2) + 'ms').padStart(10)} ${(r.p95Ms.toFixed(2) + 'ms').padStart(10)} ${r.opsPerSecond.toFixed(0).padStart(12)}`)
      }
      console.log('')
    }

    if (doResults.length > 0) {
      console.log('DURABLE OBJECT OPERATIONS:')
      console.log('-'.repeat(80))
      console.log(`${'Operation'.padEnd(45)} ${'Mean'.padStart(10)} ${'P95'.padStart(10)} ${'Ops/sec'.padStart(12)}`)
      console.log('-'.repeat(80))
      for (const r of doResults) {
        const name = r.name.length > 43 ? r.name.slice(0, 43) + '..' : r.name
        console.log(`${name.padEnd(45)} ${(r.meanMs.toFixed(2) + 'ms').padStart(10)} ${(r.p95Ms.toFixed(2) + 'ms').padStart(10)} ${r.opsPerSecond.toFixed(0).padStart(12)}`)
      }
      console.log('')
    }

    if (e2eResults.length > 0) {
      console.log('END-TO-END OPERATIONS:')
      console.log('-'.repeat(80))
      console.log(`${'Operation'.padEnd(45)} ${'Mean'.padStart(10)} ${'P95'.padStart(10)} ${'Ops/sec'.padStart(12)}`)
      console.log('-'.repeat(80))
      for (const r of e2eResults) {
        const name = r.name.length > 43 ? r.name.slice(0, 43) + '..' : r.name
        console.log(`${name.padEnd(45)} ${(r.meanMs.toFixed(2) + 'ms').padStart(10)} ${(r.p95Ms.toFixed(2) + 'ms').padStart(10)} ${r.opsPerSecond.toFixed(0).padStart(12)}`)
      }
      console.log('')
    }

    console.log('='.repeat(80))
    console.log(`Total benchmarks: ${allResults.length}`)
    console.log('='.repeat(80))

    expect(allResults.length).toBeGreaterThan(0)
  })
})

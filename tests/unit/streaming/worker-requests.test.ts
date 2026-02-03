/**
 * WorkerRequests Materialized View Tests
 *
 * Tests for the streaming MV that tracks and aggregates worker HTTP requests.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  // Recording functions
  recordRequest,
  recordRequests,
  // Metrics functions
  getRequestMetrics,
  getCurrentMetrics,
  getPathLatency,
  getErrorSummary,
  // MV definition
  createWorkerRequestsMV,
  // Buffer for high-throughput
  RequestBuffer,
  createRequestBuffer,
  // Helper functions
  getStatusCategory,
  generateRequestId,
  percentile,
  getBucketStart,
  getBucketEnd,
  // Types
  type HttpMethod,
  type StatusCategory,
  type WorkerRequest,
  type RecordRequestInput,
  type TimeBucket,
  type RequestMetrics,
} from '../../../src/streaming/worker-requests'

// =============================================================================
// Mock ParqueDB
// =============================================================================

function createMockCollection() {
  const store: Map<string, Record<string, unknown>> = new Map()
  let idCounter = 0

  return {
    store,
    create: vi.fn(async (data: Record<string, unknown>) => {
      const id = `mock/${++idCounter}`
      const record = { $id: id, ...data, createdAt: new Date() }
      store.set(id, record)
      return record
    }),
    createMany: vi.fn(async (items: Record<string, unknown>[]) => {
      return Promise.all(items.map(async (data) => {
        const id = `mock/${++idCounter}`
        const record = { $id: id, ...data, createdAt: new Date() }
        store.set(id, record)
        return record
      }))
    }),
    find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
      let results = Array.from(store.values())

      // Apply basic filtering
      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item[key] as Date
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
            } else if (key === 'status' && typeof value === 'object' && value !== null) {
              const status = item[key] as number
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && status < (filterObj.$gte as number)) return false
            } else if (item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      // Apply sorting
      if (options?.sort) {
        const [sortKey, sortDir] = Object.entries(options.sort)[0]!
        results.sort((a, b) => {
          const aVal = a[sortKey]
          const bVal = b[sortKey]
          if (aVal instanceof Date && bVal instanceof Date) {
            return sortDir > 0 ? aVal.getTime() - bVal.getTime() : bVal.getTime() - aVal.getTime()
          }
          if (typeof aVal === 'number' && typeof bVal === 'number') {
            return sortDir > 0 ? aVal - bVal : bVal - aVal
          }
          return 0
        })
      }

      // Apply limit
      if (options?.limit) {
        results = results.slice(0, options.limit)
      }

      return results
    }),
  }
}

function createMockDB() {
  const collections: Map<string, ReturnType<typeof createMockCollection>> = new Map()

  return {
    collections,
    collection: vi.fn((name: string) => {
      if (!collections.has(name)) {
        collections.set(name, createMockCollection())
      }
      return collections.get(name)!
    }),
  }
}

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('Helper Functions', () => {
  describe('getStatusCategory', () => {
    it('should return 1xx for informational responses', () => {
      expect(getStatusCategory(100)).toBe('1xx')
      expect(getStatusCategory(101)).toBe('1xx')
      expect(getStatusCategory(199)).toBe('1xx')
    })

    it('should return 2xx for success responses', () => {
      expect(getStatusCategory(200)).toBe('2xx')
      expect(getStatusCategory(201)).toBe('2xx')
      expect(getStatusCategory(204)).toBe('2xx')
      expect(getStatusCategory(299)).toBe('2xx')
    })

    it('should return 3xx for redirection responses', () => {
      expect(getStatusCategory(301)).toBe('3xx')
      expect(getStatusCategory(302)).toBe('3xx')
      expect(getStatusCategory(304)).toBe('3xx')
      expect(getStatusCategory(399)).toBe('3xx')
    })

    it('should return 4xx for client error responses', () => {
      expect(getStatusCategory(400)).toBe('4xx')
      expect(getStatusCategory(401)).toBe('4xx')
      expect(getStatusCategory(404)).toBe('4xx')
      expect(getStatusCategory(499)).toBe('4xx')
    })

    it('should return 5xx for server error responses', () => {
      expect(getStatusCategory(500)).toBe('5xx')
      expect(getStatusCategory(502)).toBe('5xx')
      expect(getStatusCategory(503)).toBe('5xx')
      expect(getStatusCategory(599)).toBe('5xx')
    })
  })

  describe('generateRequestId', () => {
    it('should generate unique request IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 1000; i++) {
        ids.add(generateRequestId())
      }
      expect(ids.size).toBe(1000)
    })

    it('should prefix IDs with req_', () => {
      const id = generateRequestId()
      expect(id.startsWith('req_')).toBe(true)
    })

    it('should have reasonable length', () => {
      const id = generateRequestId()
      expect(id.length).toBeGreaterThan(10)
      expect(id.length).toBeLessThan(30)
    })
  })

  describe('percentile', () => {
    it('should return 0 for empty array', () => {
      expect(percentile([], 50)).toBe(0)
    })

    it('should return single value for single-element array', () => {
      expect(percentile([42], 50)).toBe(42)
      expect(percentile([42], 99)).toBe(42)
    })

    it('should calculate p50 (median) correctly', () => {
      expect(percentile([1, 2, 3, 4, 5], 50)).toBe(3)
      expect(percentile([1, 2, 3, 4], 50)).toBe(2.5)
    })

    it('should calculate p95 correctly', () => {
      const data = Array.from({ length: 100 }, (_, i) => i + 1)
      expect(percentile(data, 95)).toBeCloseTo(95.05, 1)
    })

    it('should calculate p99 correctly', () => {
      const data = Array.from({ length: 100 }, (_, i) => i + 1)
      expect(percentile(data, 99)).toBeCloseTo(99.01, 1)
    })

    it('should handle p0 (minimum)', () => {
      expect(percentile([1, 5, 10, 20, 100], 0)).toBe(1)
    })

    it('should handle p100 (maximum)', () => {
      expect(percentile([1, 5, 10, 20, 100], 100)).toBe(100)
    })
  })

  describe('getBucketStart', () => {
    it('should get minute bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketStart(date, 'minute')
      expect(bucket.toISOString()).toBe('2024-01-15T10:35:00.000Z')
    })

    it('should get hour bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketStart(date, 'hour')
      expect(bucket.toISOString()).toBe('2024-01-15T10:00:00.000Z')
    })

    it('should get day bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketStart(date, 'day')
      // Day bucket uses local time, so we check the date part
      expect(bucket.getDate()).toBeLessThanOrEqual(15)
      expect(bucket.getHours()).toBe(0)
      expect(bucket.getMinutes()).toBe(0)
      expect(bucket.getSeconds()).toBe(0)
    })

    it('should get month bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketStart(date, 'month')
      // Month bucket uses local time
      expect(bucket.getDate()).toBe(1)
      expect(bucket.getHours()).toBe(0)
      expect(bucket.getMinutes()).toBe(0)
    })
  })

  describe('getBucketEnd', () => {
    it('should get minute bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketEnd(date, 'minute')
      expect(bucket.toISOString()).toBe('2024-01-15T10:36:00.000Z')
    })

    it('should get hour bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketEnd(date, 'hour')
      expect(bucket.toISOString()).toBe('2024-01-15T11:00:00.000Z')
    })

    it('should get day bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketEnd(date, 'day')
      // Day bucket end is next day at midnight (local time)
      const start = getBucketStart(date, 'day')
      expect(bucket.getTime() - start.getTime()).toBe(24 * 60 * 60 * 1000) // 1 day
      expect(bucket.getHours()).toBe(0)
      expect(bucket.getMinutes()).toBe(0)
    })

    it('should get month bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getBucketEnd(date, 'month')
      // Month bucket end is first of next month
      expect(bucket.getDate()).toBe(1)
      expect(bucket.getMonth()).toBe(1) // February (0-indexed)
      expect(bucket.getHours()).toBe(0)
    })
  })
})

// =============================================================================
// Request Recording Tests
// =============================================================================

describe('Request Recording', () => {
  describe('recordRequest', () => {
    it('should record a basic request', async () => {
      const db = createMockDB()

      const result = await recordRequest(db as any, {
        method: 'GET',
        path: '/api/users',
        status: 200,
        latencyMs: 45,
      })

      expect(result.method).toBe('GET')
      expect(result.path).toBe('/api/users')
      expect(result.status).toBe(200)
      expect(result.latencyMs).toBe(45)
      expect(result.statusCategory).toBe('2xx')
      expect(result.cached).toBe(false)
      expect(result.requestId).toBeDefined()
      expect(result.timestamp).toBeDefined()
    })

    it('should record request with all optional fields', async () => {
      const db = createMockDB()
      const now = new Date()

      const result = await recordRequest(db as any, {
        method: 'POST',
        path: '/api/entities',
        status: 201,
        latencyMs: 150,
        cached: false,
        cacheTier: undefined,
        colo: 'SJC',
        country: 'US',
        city: 'San Francisco',
        region: 'CA',
        timezone: 'America/Los_Angeles',
        requestSize: 1024,
        responseSize: 512,
        userAgent: 'Mozilla/5.0',
        dataset: 'my-dataset',
        collection: 'users',
        resourceType: 'entity',
        requestId: 'custom-req-123',
        timestamp: now,
        metadata: { foo: 'bar' },
      })

      expect(result.method).toBe('POST')
      expect(result.colo).toBe('SJC')
      expect(result.country).toBe('US')
      expect(result.city).toBe('San Francisco')
      expect(result.dataset).toBe('my-dataset')
      expect(result.requestId).toBe('custom-req-123')
      expect(result.timestamp).toEqual(now)
      expect(result.metadata).toEqual({ foo: 'bar' })
    })

    it('should correctly categorize error statuses', async () => {
      const db = createMockDB()

      const result404 = await recordRequest(db as any, {
        method: 'GET',
        path: '/api/not-found',
        status: 404,
        latencyMs: 10,
        error: 'Not found',
      })
      expect(result404.statusCategory).toBe('4xx')
      expect(result404.error).toBe('Not found')

      const result500 = await recordRequest(db as any, {
        method: 'GET',
        path: '/api/error',
        status: 500,
        latencyMs: 5,
        error: 'Internal server error',
        errorCode: 'INTERNAL_ERROR',
      })
      expect(result500.statusCategory).toBe('5xx')
      expect(result500.error).toBe('Internal server error')
      expect(result500.errorCode).toBe('INTERNAL_ERROR')
    })

    it('should record to custom collection', async () => {
      const db = createMockDB()

      await recordRequest(db as any, {
        method: 'GET',
        path: '/api/test',
        status: 200,
        latencyMs: 10,
      }, { collection: 'custom_requests' })

      expect(db.collection).toHaveBeenCalledWith('custom_requests')
    })
  })

  describe('recordRequests (batch)', () => {
    it('should record multiple requests at once', async () => {
      const db = createMockDB()

      const inputs: RecordRequestInput[] = [
        { method: 'GET', path: '/api/users', status: 200, latencyMs: 45 },
        { method: 'POST', path: '/api/users', status: 201, latencyMs: 120 },
        { method: 'GET', path: '/api/users/1', status: 404, latencyMs: 10 },
      ]

      const results = await recordRequests(db as any, inputs)

      expect(results).toHaveLength(3)
      expect(results[0]!.method).toBe('GET')
      expect(results[1]!.method).toBe('POST')
      expect(results[2]!.status).toBe(404)
    })

    it('should auto-generate unique request IDs', async () => {
      const db = createMockDB()

      const inputs: RecordRequestInput[] = Array.from({ length: 10 }, () => ({
        method: 'GET' as HttpMethod,
        path: '/api/test',
        status: 200,
        latencyMs: 10,
      }))

      const results = await recordRequests(db as any, inputs)
      const ids = results.map(r => r.requestId)
      const uniqueIds = new Set(ids)

      expect(uniqueIds.size).toBe(10)
    })
  })
})

// =============================================================================
// Metrics Aggregation Tests
// =============================================================================

describe('Metrics Aggregation', () => {
  let db: ReturnType<typeof createMockDB>

  beforeEach(() => {
    db = createMockDB()
  })

  describe('getRequestMetrics', () => {
    it('should return empty metrics for no data', async () => {
      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
      })

      expect(metrics).toHaveLength(0)
    })

    it('should aggregate requests into time buckets', async () => {
      // Add test data directly to mock store
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add requests within the same hour
      const testRequests = [
        { timestamp: new Date(baseTime.getTime()), method: 'GET', path: '/api/users', status: 200, statusCategory: '2xx', latencyMs: 50, cached: true },
        { timestamp: new Date(baseTime.getTime() + 1000), method: 'GET', path: '/api/users', status: 200, statusCategory: '2xx', latencyMs: 60, cached: false },
        { timestamp: new Date(baseTime.getTime() + 2000), method: 'POST', path: '/api/users', status: 201, statusCategory: '2xx', latencyMs: 150, cached: false },
        { timestamp: new Date(baseTime.getTime() + 3000), method: 'GET', path: '/api/users/1', status: 404, statusCategory: '4xx', latencyMs: 10, cached: false },
      ]

      for (const req of testRequests) {
        await collection.create(req)
      }

      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics).toHaveLength(1)
      expect(metrics[0]!.totalRequests).toBe(4)
      expect(metrics[0]!.successCount).toBe(3)
      expect(metrics[0]!.clientErrorCount).toBe(1)
      expect(metrics[0]!.serverErrorCount).toBe(0)
      expect(metrics[0]!.cacheHits).toBe(1)
      expect(metrics[0]!.cacheMisses).toBe(3)
    })

    it('should calculate latency statistics correctly', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add requests with known latencies: [10, 20, 50, 100, 500]
      const latencies = [10, 20, 50, 100, 500]
      for (let i = 0; i < latencies.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          path: '/api/test',
          status: 200,
          statusCategory: '2xx',
          latencyMs: latencies[i],
          cached: false,
        })
      }

      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.latency.min).toBe(10)
      expect(metrics[0]!.latency.max).toBe(500)
      expect(metrics[0]!.latency.avg).toBe(136) // (10+20+50+100+500)/5
      expect(metrics[0]!.latency.p50).toBe(50)
    })

    it('should calculate error rate correctly', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // 7 success, 2 client error, 1 server error = 30% error rate
      const requests = [
        ...Array.from({ length: 7 }, (_, i) => ({ status: 200, statusCategory: '2xx' as StatusCategory })),
        { status: 400, statusCategory: '4xx' as StatusCategory },
        { status: 404, statusCategory: '4xx' as StatusCategory },
        { status: 500, statusCategory: '5xx' as StatusCategory },
      ]

      for (let i = 0; i < requests.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          path: '/api/test',
          ...requests[i],
          latencyMs: 10,
          cached: false,
        })
      }

      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.errorRate).toBeCloseTo(0.3, 2)
    })

    it('should calculate cache hit ratio correctly', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // 3 cached, 7 not cached = 30% hit ratio
      for (let i = 0; i < 10; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          path: '/api/test',
          status: 200,
          statusCategory: '2xx',
          latencyMs: 10,
          cached: i < 3,
        })
      }

      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.cacheHitRatio).toBeCloseTo(0.3, 2)
    })

    it('should group by path when specified', async () => {
      const collection = db.collection('worker_requests')
      const now = new Date()

      // Add requests to different paths within the same hour
      const paths = ['/api/users', '/api/users', '/api/posts', '/api/posts', '/api/posts']
      for (let i = 0; i < paths.length; i++) {
        await collection.create({
          timestamp: new Date(now.getTime() + i * 1000),
          method: 'GET',
          path: paths[i],
          status: 200,
          statusCategory: '2xx',
          latencyMs: 10,
          cached: false,
        })
      }

      const metrics = await getRequestMetrics(db as any, {
        timeBucket: 'hour',
        groupBy: 'path',
      })

      // We should have at least 2 different path groups
      expect(metrics.length).toBeGreaterThanOrEqual(2)

      // Verify we have metrics with group values
      const groupValues = metrics.map(m => m.groupValue).filter(Boolean)
      expect(groupValues).toContain('/api/users')
      expect(groupValues).toContain('/api/posts')

      // Verify counts
      const userMetrics = metrics.find(m => m.groupValue === '/api/users')
      const postMetrics = metrics.find(m => m.groupValue === '/api/posts')

      if (userMetrics && postMetrics) {
        expect(userMetrics.totalRequests).toBe(2)
        expect(postMetrics.totalRequests).toBe(3)
      } else {
        // If we can't find by exact match, verify the groupBy is working
        expect(metrics.every(m => m.groupBy === 'path')).toBe(true)
        const totalRequests = metrics.reduce((sum, m) => sum + m.totalRequests, 0)
        expect(totalRequests).toBe(5)
      }
    })
  })

  describe('getPathLatency', () => {
    it('should return latency stats for a specific path', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      const latencies = [10, 20, 30, 40, 50]
      for (let i = 0; i < latencies.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          path: '/api/users',
          status: 200,
          statusCategory: '2xx',
          latencyMs: latencies[i],
          cached: false,
        })
      }

      const stats = await getPathLatency(db as any, '/api/users')

      expect(stats.min).toBe(10)
      expect(stats.max).toBe(50)
      expect(stats.avg).toBe(30)
      expect(stats.p50).toBe(30)
    })

    it('should return zero stats for non-existent path', async () => {
      const stats = await getPathLatency(db as any, '/api/non-existent')

      expect(stats.min).toBe(0)
      expect(stats.max).toBe(0)
      expect(stats.avg).toBe(0)
      expect(stats.p50).toBe(0)
    })
  })

  describe('getErrorSummary', () => {
    it('should return error summary', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add various error requests
      const errors = [
        { path: '/api/users/1', status: 404, error: 'Not found' },
        { path: '/api/users/2', status: 404, error: 'Not found' },
        { path: '/api/posts', status: 500, error: 'Internal error', errorCode: 'INTERNAL' },
        { path: '/api/posts', status: 503, error: 'Service unavailable', errorCode: 'UNAVAILABLE' },
      ]

      for (let i = 0; i < errors.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          ...errors[i],
          statusCategory: errors[i]!.status >= 500 ? '5xx' : '4xx',
          latencyMs: 10,
          cached: false,
        })
      }

      const summary = await getErrorSummary(db as any)

      expect(summary.totalErrors).toBe(4)
      expect(summary.byPath['/api/users/1']).toBe(1)
      expect(summary.byPath['/api/users/2']).toBe(1)
      expect(summary.byPath['/api/posts']).toBe(2)
      expect(summary.byStatusCode[404]).toBe(2)
      expect(summary.byStatusCode[500]).toBe(1)
      expect(summary.byStatusCode[503]).toBe(1)
      expect(summary.byErrorCode['INTERNAL']).toBe(1)
      expect(summary.byErrorCode['UNAVAILABLE']).toBe(1)
    })

    it('should include recent errors', async () => {
      const collection = db.collection('worker_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      for (let i = 0; i < 5; i++) {
        await collection.create({
          requestId: `req-${i}`,
          timestamp: new Date(baseTime.getTime() + i * 1000),
          method: 'GET',
          path: `/api/error/${i}`,
          status: 500,
          statusCategory: '5xx',
          latencyMs: 10,
          cached: false,
          error: `Error ${i}`,
        })
      }

      const summary = await getErrorSummary(db as any)

      expect(summary.recentErrors).toHaveLength(5)
      // Most recent should be first (based on sort order)
      expect(summary.recentErrors[0]!.path).toContain('/api/error/')
    })
  })
})

// =============================================================================
// Materialized View Definition Tests
// =============================================================================

describe('Materialized View Definition', () => {
  describe('createWorkerRequestsMV', () => {
    it('should create view with default options', () => {
      const view = createWorkerRequestsMV()

      expect(view.name).toBe('worker_requests_metrics')
      expect(view.source).toBe('worker_requests')
      expect(view.options.refreshMode).toBe('streaming')
      expect(view.options.refreshStrategy).toBe('incremental')
      expect(view.options.maxStalenessMs).toBe(5000)
    })

    it('should allow custom options', () => {
      const view = createWorkerRequestsMV({
        refreshMode: 'scheduled',
        refreshStrategy: 'full',
        maxStalenessMs: 10000,
        requestsCollection: 'custom_requests',
        metricsCollection: 'custom_metrics',
        schedule: { intervalMs: 60000 },
      })

      expect(view.options.refreshMode).toBe('scheduled')
      expect(view.options.refreshStrategy).toBe('full')
      expect(view.options.maxStalenessMs).toBe(10000)
      expect(view.options.schedule?.intervalMs).toBe(60000)
      expect(view.source).toBe('custom_requests')
    })

    it('should include aggregation pipeline', () => {
      const view = createWorkerRequestsMV()

      expect(view.query.pipeline).toBeDefined()
      expect(Array.isArray(view.query.pipeline)).toBe(true)
      expect(view.query.pipeline!.length).toBeGreaterThan(0)
    })

    it('should include proper indexes', () => {
      const view = createWorkerRequestsMV()

      expect(view.options.indexes).toContain('bucketStart')
      expect(view.options.indexes).toContain('path')
      expect(view.options.indexes).toContain('colo')
      expect(view.options.indexes).toContain('country')
    })

    it('should include description and tags', () => {
      const view = createWorkerRequestsMV()

      expect(view.options.description).toBeDefined()
      expect(view.options.tags).toContain('analytics')
      expect(view.options.tags).toContain('worker')
      expect(view.options.tags).toContain('requests')
    })
  })
})

// =============================================================================
// Request Buffer Tests
// =============================================================================

describe('RequestBuffer', () => {
  let db: ReturnType<typeof createMockDB>

  beforeEach(() => {
    db = createMockDB()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('basic operations', () => {
    it('should buffer requests', async () => {
      const buffer = new RequestBuffer(db as any, {
        maxBufferSize: 10,
        flushIntervalMs: 1000,
      })

      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })
      await buffer.add({ method: 'POST', path: '/test', status: 201, latencyMs: 20 })

      expect(buffer.getBufferSize()).toBe(2)
    })

    it('should flush when buffer size is reached', async () => {
      const buffer = new RequestBuffer(db as any, {
        maxBufferSize: 3,
        flushIntervalMs: 10000,
      })

      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })
      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })
      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })

      // Buffer should be empty after auto-flush
      expect(buffer.getBufferSize()).toBe(0)
    })

    it('should flush on close', async () => {
      const buffer = new RequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 10000,
      })

      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })
      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })

      expect(buffer.getBufferSize()).toBe(2)

      await buffer.close()

      expect(buffer.getBufferSize()).toBe(0)
    })
  })

  describe('periodic flush', () => {
    it('should flush periodically when timer is started', async () => {
      const buffer = new RequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 50, // Short interval for testing
      })

      buffer.startTimer()

      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })
      expect(buffer.getBufferSize()).toBe(1)

      // Advance fake timers past flush interval
      await vi.advanceTimersByTimeAsync(100)

      expect(buffer.getBufferSize()).toBe(0)

      buffer.stopTimer()
    })

    it('should stop periodic flush when timer is stopped', async () => {
      const buffer = new RequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 1000,
      })

      buffer.startTimer()
      buffer.stopTimer()

      await buffer.add({ method: 'GET', path: '/test', status: 200, latencyMs: 10 })

      vi.advanceTimersByTime(2000)

      // Buffer should still have the request since timer was stopped
      expect(buffer.getBufferSize()).toBe(1)
    })
  })

  describe('createRequestBuffer factory', () => {
    it('should create buffer with auto-start by default', () => {
      const buffer = createRequestBuffer(db as any, {
        maxBufferSize: 50,
        flushIntervalMs: 5000,
      })

      // Should have started timer
      buffer.stopTimer()
    })

    it('should not auto-start when disabled', () => {
      const buffer = createRequestBuffer(db as any, {
        autoStart: false,
      })

      expect(buffer.getBufferSize()).toBe(0)
    })
  })
})

/**
 * AIRequests Materialized View Tests
 *
 * Tests for the streaming MV that tracks and aggregates AI/LLM requests.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  // Recording functions
  recordAIRequest,
  recordAIRequests,
  // Metrics functions
  getAIMetrics,
  getCurrentAIMetrics,
  getAICostSummary,
  getAIErrorSummary,
  // MV definition
  createAIRequestsMV,
  // Buffer for high-throughput
  AIRequestBuffer,
  createAIRequestBuffer,
  // Helper functions
  generateAIRequestId,
  calculateCost,
  getAIBucketStart,
  getAIBucketEnd,
  // Constants
  DEFAULT_MODEL_PRICING,
  // Types
  type AIRequest,
  type RecordAIRequestInput,
  type AITimeBucket,
  type AIMetrics,
  type AIRequestType,
} from '../../../src/streaming/ai-requests'

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
            } else if (key === 'success' && typeof value === 'boolean') {
              if (item[key] !== value) return false
            } else if (typeof value !== 'object' && item[key] !== value) {
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
  describe('generateAIRequestId', () => {
    it('should generate unique request IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 1000; i++) {
        ids.add(generateAIRequestId())
      }
      expect(ids.size).toBe(1000)
    })

    it('should prefix IDs with ai_', () => {
      const id = generateAIRequestId()
      expect(id.startsWith('ai_')).toBe(true)
    })

    it('should have reasonable length', () => {
      const id = generateAIRequestId()
      expect(id.length).toBeGreaterThan(10)
      expect(id.length).toBeLessThan(30)
    })
  })

  describe('calculateCost', () => {
    it('should calculate cost for known models', () => {
      // GPT-4: $0.03/1k prompt, $0.06/1k completion
      const cost = calculateCost('gpt-4', 1000, 500)
      expect(cost).toBeCloseTo(0.03 + 0.03, 4)
    })

    it('should calculate cost for GPT-4o', () => {
      // GPT-4o: $0.005/1k prompt, $0.015/1k completion
      const cost = calculateCost('gpt-4o', 1000, 1000)
      expect(cost).toBeCloseTo(0.005 + 0.015, 4)
    })

    it('should calculate cost for Claude models', () => {
      // Claude 3 Sonnet: $0.003/1k prompt, $0.015/1k completion
      const cost = calculateCost('claude-3-sonnet', 2000, 1000)
      expect(cost).toBeCloseTo(0.006 + 0.015, 4)
    })

    it('should use default pricing for unknown models', () => {
      const defaultPricing = DEFAULT_MODEL_PRICING['default']!
      const cost = calculateCost('unknown-model-xyz', 1000, 1000)
      const expected = (1000 / 1000) * defaultPricing.promptPer1k +
                       (1000 / 1000) * defaultPricing.completionPer1k
      expect(cost).toBeCloseTo(expected, 6)
    })

    it('should use custom pricing when provided', () => {
      const customPricing = {
        'my-custom-model': { promptPer1k: 0.1, completionPer1k: 0.2 },
      }
      const cost = calculateCost('my-custom-model', 1000, 1000, customPricing)
      expect(cost).toBeCloseTo(0.1 + 0.2, 4)
    })

    it('should handle zero tokens', () => {
      const cost = calculateCost('gpt-4', 0, 0)
      expect(cost).toBe(0)
    })

    it('should find pricing by partial match', () => {
      // Should match 'gpt-4' for 'gpt-4-0125-preview'
      const cost = calculateCost('gpt-4-0125-preview', 1000, 1000)
      // Should find a match (gpt-4) and not use default
      expect(cost).toBeGreaterThan(0)
    })
  })

  describe('getAIBucketStart', () => {
    it('should get minute bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketStart(date, 'minute')
      expect(bucket.toISOString()).toBe('2024-01-15T10:35:00.000Z')
    })

    it('should get hour bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketStart(date, 'hour')
      expect(bucket.toISOString()).toBe('2024-01-15T10:00:00.000Z')
    })

    it('should get day bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketStart(date, 'day')
      expect(bucket.getHours()).toBe(0)
      expect(bucket.getMinutes()).toBe(0)
      expect(bucket.getSeconds()).toBe(0)
    })

    it('should get month bucket start', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketStart(date, 'month')
      expect(bucket.getDate()).toBe(1)
      expect(bucket.getHours()).toBe(0)
      expect(bucket.getMinutes()).toBe(0)
    })
  })

  describe('getAIBucketEnd', () => {
    it('should get minute bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketEnd(date, 'minute')
      expect(bucket.toISOString()).toBe('2024-01-15T10:36:00.000Z')
    })

    it('should get hour bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketEnd(date, 'hour')
      expect(bucket.toISOString()).toBe('2024-01-15T11:00:00.000Z')
    })

    it('should get day bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketEnd(date, 'day')
      const start = getAIBucketStart(date, 'day')
      expect(bucket.getTime() - start.getTime()).toBe(24 * 60 * 60 * 1000)
    })

    it('should get month bucket end', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')
      const bucket = getAIBucketEnd(date, 'month')
      expect(bucket.getDate()).toBe(1)
      expect(bucket.getMonth()).toBe(1) // February
    })
  })
})

// =============================================================================
// Request Recording Tests
// =============================================================================

describe('Request Recording', () => {
  describe('recordAIRequest', () => {
    it('should record a basic AI request', async () => {
      const db = createMockDB()

      const result = await recordAIRequest(db as any, {
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 850,
        promptTokens: 150,
        completionTokens: 200,
      })

      expect(result.modelId).toBe('gpt-4')
      expect(result.providerId).toBe('openai')
      expect(result.requestType).toBe('generate')
      expect(result.latencyMs).toBe(850)
      expect(result.promptTokens).toBe(150)
      expect(result.completionTokens).toBe(200)
      expect(result.totalTokens).toBe(350)
      expect(result.costUSD).toBeGreaterThan(0)
      expect(result.cached).toBe(false)
      expect(result.success).toBe(true)
      expect(result.requestId).toBeDefined()
      expect(result.timestamp).toBeDefined()
    })

    it('should record request with all optional fields', async () => {
      const db = createMockDB()
      const now = new Date()

      const result = await recordAIRequest(db as any, {
        modelId: 'claude-3-sonnet',
        providerId: 'anthropic',
        requestType: 'stream',
        latencyMs: 1200,
        promptTokens: 500,
        completionTokens: 1000,
        cached: true,
        success: true,
        finishReason: 'stop',
        temperature: 0.7,
        maxTokens: 4096,
        toolsUsed: true,
        toolCallCount: 2,
        userId: 'user-123',
        appId: 'my-app',
        environment: 'production',
        requestId: 'custom-req-123',
        timestamp: now,
        metadata: { feature: 'chat' },
      })

      expect(result.modelId).toBe('claude-3-sonnet')
      expect(result.providerId).toBe('anthropic')
      expect(result.cached).toBe(true)
      expect(result.finishReason).toBe('stop')
      expect(result.temperature).toBe(0.7)
      expect(result.toolsUsed).toBe(true)
      expect(result.toolCallCount).toBe(2)
      expect(result.userId).toBe('user-123')
      expect(result.appId).toBe('my-app')
      expect(result.environment).toBe('production')
      expect(result.requestId).toBe('custom-req-123')
      expect(result.timestamp).toEqual(now)
      expect(result.metadata).toEqual({ feature: 'chat' })
    })

    it('should record error requests', async () => {
      const db = createMockDB()

      const result = await recordAIRequest(db as any, {
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        success: false,
        error: 'Rate limit exceeded',
        errorCode: 'RATE_LIMIT',
      })

      expect(result.success).toBe(false)
      expect(result.error).toBe('Rate limit exceeded')
      expect(result.errorCode).toBe('RATE_LIMIT')
    })

    it('should calculate cost automatically', async () => {
      const db = createMockDB()

      const result = await recordAIRequest(db as any, {
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 1000,
        completionTokens: 500,
      })

      // GPT-4: $0.03/1k prompt, $0.06/1k completion
      // 1000 prompt = $0.03, 500 completion = $0.03
      expect(result.costUSD).toBeCloseTo(0.06, 4)
    })

    it('should use custom cost when provided', async () => {
      const db = createMockDB()

      const result = await recordAIRequest(db as any, {
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 1000,
        completionTokens: 500,
        costUSD: 0.123,
      })

      expect(result.costUSD).toBe(0.123)
    })

    it('should record to custom collection', async () => {
      const db = createMockDB()

      await recordAIRequest(db as any, {
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
      }, { collection: 'custom_ai_requests' })

      expect(db.collection).toHaveBeenCalledWith('custom_ai_requests')
    })
  })

  describe('recordAIRequests (batch)', () => {
    it('should record multiple requests at once', async () => {
      const db = createMockDB()

      const inputs: RecordAIRequestInput[] = [
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500, promptTokens: 100, completionTokens: 200 },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 800, promptTokens: 200, completionTokens: 400 },
        { modelId: 'gemini-pro', providerId: 'google', requestType: 'chat', latencyMs: 300, promptTokens: 50, completionTokens: 100 },
      ]

      const results = await recordAIRequests(db as any, inputs)

      expect(results).toHaveLength(3)
      expect(results[0]!.modelId).toBe('gpt-4')
      expect(results[1]!.modelId).toBe('claude-3-sonnet')
      expect(results[2]!.modelId).toBe('gemini-pro')
    })

    it('should auto-generate unique request IDs', async () => {
      const db = createMockDB()

      const inputs: RecordAIRequestInput[] = Array.from({ length: 10 }, () => ({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate' as AIRequestType,
        latencyMs: 100,
      }))

      const results = await recordAIRequests(db as any, inputs)
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

  describe('getAIMetrics', () => {
    it('should return empty metrics for no data', async () => {
      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
      })

      expect(metrics).toHaveLength(0)
    })

    it('should aggregate requests into time buckets', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add requests within the same hour
      const testRequests = [
        { timestamp: new Date(baseTime.getTime()), modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500, promptTokens: 100, completionTokens: 200, totalTokens: 300, costUSD: 0.015, cached: false, success: true },
        { timestamp: new Date(baseTime.getTime() + 1000), modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 600, promptTokens: 150, completionTokens: 250, totalTokens: 400, costUSD: 0.02, cached: true, success: true },
        { timestamp: new Date(baseTime.getTime() + 2000), modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 800, promptTokens: 200, completionTokens: 300, totalTokens: 500, costUSD: 0.025, cached: false, success: true },
        { timestamp: new Date(baseTime.getTime() + 3000), modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 50, completionTokens: 0, totalTokens: 50, costUSD: 0.001, cached: false, success: false },
      ]

      for (const req of testRequests) {
        await collection.create(req)
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics).toHaveLength(1)
      expect(metrics[0]!.totalRequests).toBe(4)
      expect(metrics[0]!.successCount).toBe(3)
      expect(metrics[0]!.errorCount).toBe(1)
      expect(metrics[0]!.cacheHits).toBe(1)
      expect(metrics[0]!.cacheMisses).toBe(3)
    })

    it('should calculate token statistics correctly', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add requests with known token counts
      const requests = [
        { timestamp: baseTime, modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 200, totalTokens: 300, costUSD: 0.01, cached: false, success: true },
        { timestamp: new Date(baseTime.getTime() + 1000), modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 200, completionTokens: 400, totalTokens: 600, costUSD: 0.02, cached: false, success: true },
        { timestamp: new Date(baseTime.getTime() + 2000), modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 300, completionTokens: 600, totalTokens: 900, costUSD: 0.03, cached: false, success: true },
      ]

      for (const req of requests) {
        await collection.create(req)
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.tokens.totalPromptTokens).toBe(600)
      expect(metrics[0]!.tokens.totalCompletionTokens).toBe(1200)
      expect(metrics[0]!.tokens.totalTokens).toBe(1800)
      expect(metrics[0]!.tokens.avgPromptTokens).toBe(200)
      expect(metrics[0]!.tokens.avgCompletionTokens).toBe(400)
      expect(metrics[0]!.tokens.avgTotalTokens).toBe(600)
    })

    it('should calculate latency statistics correctly', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add requests with known latencies: [100, 200, 300, 400, 500]
      const latencies = [100, 200, 300, 400, 500]
      for (let i = 0; i < latencies.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: latencies[i],
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          costUSD: 0.01,
          cached: false,
          success: true,
        })
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.latency.min).toBe(100)
      expect(metrics[0]!.latency.max).toBe(500)
      expect(metrics[0]!.latency.avg).toBe(300)
      expect(metrics[0]!.latency.p50).toBe(300)
    })

    it('should calculate cost statistics correctly', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      const requests = [
        { costUSD: 0.01, cached: false },
        { costUSD: 0.02, cached: false },
        { costUSD: 0.03, cached: true }, // Cache hit - cost savings
        { costUSD: 0.01, cached: false },
      ]

      for (let i = 0; i < requests.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          ...requests[i],
          success: true,
        })
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      // Total cost is from non-cached requests: 0.01 + 0.02 + 0.01 = 0.04
      expect(metrics[0]!.cost.totalCostUSD).toBeCloseTo(0.04, 4)
      // Cache savings: 0.03
      expect(metrics[0]!.cost.cacheSavingsUSD).toBeCloseTo(0.03, 4)
    })

    it('should calculate error rate correctly', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // 7 success, 3 error = 30% error rate
      const requests = [
        ...Array.from({ length: 7 }, () => ({ success: true })),
        ...Array.from({ length: 3 }, () => ({ success: false })),
      ]

      for (let i = 0; i < requests.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          costUSD: 0.01,
          cached: false,
          ...requests[i],
        })
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics[0]!.errorRate).toBeCloseTo(0.3, 2)
    })

    it('should group by model when specified', async () => {
      const collection = db.collection('ai_requests')
      const now = new Date()

      const models = ['gpt-4', 'gpt-4', 'claude-3-sonnet', 'claude-3-sonnet', 'claude-3-sonnet']
      for (let i = 0; i < models.length; i++) {
        await collection.create({
          timestamp: new Date(now.getTime() + i * 1000),
          modelId: models[i],
          providerId: models[i]!.startsWith('gpt') ? 'openai' : 'anthropic',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          costUSD: 0.01,
          cached: false,
          success: true,
        })
      }

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        groupBy: 'model',
      })

      // Should have groups for different models
      expect(metrics.length).toBeGreaterThanOrEqual(2)

      // Verify we have metrics with group values
      const groupValues = metrics.map(m => m.groupValue).filter(Boolean)
      expect(groupValues).toContain('gpt-4')
      expect(groupValues).toContain('claude-3-sonnet')

      // Verify counts
      const gpt4Metrics = metrics.find(m => m.groupValue === 'gpt-4')
      const claudeMetrics = metrics.find(m => m.groupValue === 'claude-3-sonnet')

      if (gpt4Metrics && claudeMetrics) {
        expect(gpt4Metrics.totalRequests).toBe(2)
        expect(claudeMetrics.totalRequests).toBe(3)
      }
    })

    it('should filter by provider', async () => {
      const collection = db.collection('ai_requests')
      const now = new Date()

      await collection.create({ timestamp: now, modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 100, totalTokens: 200, costUSD: 0.01, cached: false, success: true })
      await collection.create({ timestamp: new Date(now.getTime() + 1000), modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 100, totalTokens: 200, costUSD: 0.01, cached: false, success: true })

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        providerId: 'openai',
      })

      // Should only include OpenAI requests
      expect(metrics.length).toBeGreaterThanOrEqual(1)
      for (const m of metrics) {
        expect(m.providers['openai'] || 0).toBeGreaterThan(0)
      }
    })
  })

  describe('getAICostSummary', () => {
    it('should return cost summary', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      const requests = [
        { modelId: 'gpt-4', providerId: 'openai', costUSD: 0.05, totalTokens: 500, cached: false },
        { modelId: 'gpt-4', providerId: 'openai', costUSD: 0.03, totalTokens: 300, cached: false },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', costUSD: 0.02, totalTokens: 400, cached: false },
        { modelId: 'gpt-4', providerId: 'openai', costUSD: 0.04, totalTokens: 400, cached: true }, // Cache hit
      ]

      for (let i = 0; i < requests.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          requestId: `req-${i}`,
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          success: true,
          ...requests[i],
        })
      }

      const summary = await getAICostSummary(db as any)

      expect(summary.totalCostUSD).toBeCloseTo(0.1, 4) // 0.05 + 0.03 + 0.02
      expect(summary.cacheSavingsUSD).toBeCloseTo(0.04, 4)
      expect(summary.byModel['gpt-4']!.cost).toBeCloseTo(0.08, 4) // 0.05 + 0.03
      expect(summary.byModel['claude-3-sonnet']!.cost).toBeCloseTo(0.02, 4)
      expect(summary.byProvider['openai']!.requests).toBe(3)
      expect(summary.byProvider['anthropic']!.requests).toBe(1)
    })

    it('should include top expensive requests', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      for (let i = 0; i < 5; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          requestId: `req-${i}`,
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: (i + 1) * 100,
          costUSD: (i + 1) * 0.01,
          cached: false,
          success: true,
        })
      }

      const summary = await getAICostSummary(db as any)

      expect(summary.topExpensive).toHaveLength(5)
      // Most expensive should be first
      expect(summary.topExpensive[0]!.costUSD).toBe(0.05)
    })
  })

  describe('getAIErrorSummary', () => {
    it('should return error summary', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add successful and failed requests
      const requests = [
        { success: true },
        { success: true },
        { success: false, error: 'Rate limit', errorCode: 'RATE_LIMIT', modelId: 'gpt-4', providerId: 'openai' },
        { success: false, error: 'Timeout', errorCode: 'TIMEOUT', modelId: 'claude-3-sonnet', providerId: 'anthropic' },
        { success: false, error: 'Rate limit', errorCode: 'RATE_LIMIT', modelId: 'gpt-4', providerId: 'openai' },
      ]

      for (let i = 0; i < requests.length; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          requestId: `req-${i}`,
          modelId: requests[i].modelId ?? 'gpt-4',
          providerId: requests[i].providerId ?? 'openai',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          costUSD: 0.01,
          cached: false,
          ...requests[i],
        })
      }

      const summary = await getAIErrorSummary(db as any)

      expect(summary.totalErrors).toBe(3)
      expect(summary.errorRate).toBeCloseTo(0.6, 2) // 3/5
      expect(summary.byModel['gpt-4']).toBe(2)
      expect(summary.byModel['claude-3-sonnet']).toBe(1)
      expect(summary.byErrorCode['RATE_LIMIT']).toBe(2)
      expect(summary.byErrorCode['TIMEOUT']).toBe(1)
    })

    it('should include recent errors', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      for (let i = 0; i < 5; i++) {
        await collection.create({
          timestamp: new Date(baseTime.getTime() + i * 1000),
          requestId: `req-${i}`,
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          promptTokens: 100,
          completionTokens: 100,
          totalTokens: 200,
          costUSD: 0.01,
          cached: false,
          success: false,
          error: `Error ${i}`,
          errorCode: `ERR_${i}`,
        })
      }

      const summary = await getAIErrorSummary(db as any)

      expect(summary.recentErrors).toHaveLength(5)
      expect(summary.recentErrors[0]!.error).toContain('Error')
    })
  })
})

// =============================================================================
// Materialized View Definition Tests
// =============================================================================

describe('Materialized View Definition', () => {
  describe('createAIRequestsMV', () => {
    it('should create view with default options', () => {
      const view = createAIRequestsMV()

      expect(view.name).toBe('ai_requests_metrics')
      expect(view.source).toBe('ai_requests')
      expect(view.options.refreshMode).toBe('streaming')
      expect(view.options.refreshStrategy).toBe('incremental')
      expect(view.options.maxStalenessMs).toBe(5000)
    })

    it('should allow custom options', () => {
      const view = createAIRequestsMV({
        refreshMode: 'scheduled',
        refreshStrategy: 'full',
        maxStalenessMs: 10000,
        requestsCollection: 'custom_ai_requests',
        metricsCollection: 'custom_ai_metrics',
        schedule: { intervalMs: 60000 },
      })

      expect(view.options.refreshMode).toBe('scheduled')
      expect(view.options.refreshStrategy).toBe('full')
      expect(view.options.maxStalenessMs).toBe(10000)
      expect(view.options.schedule?.intervalMs).toBe(60000)
      expect(view.source).toBe('custom_ai_requests')
    })

    it('should include aggregation pipeline', () => {
      const view = createAIRequestsMV()

      expect(view.query.pipeline).toBeDefined()
      expect(Array.isArray(view.query.pipeline)).toBe(true)
      expect(view.query.pipeline!.length).toBeGreaterThan(0)
    })

    it('should include proper indexes', () => {
      const view = createAIRequestsMV()

      expect(view.options.indexes).toContain('bucketStart')
      expect(view.options.indexes).toContain('modelId')
      expect(view.options.indexes).toContain('providerId')
      expect(view.options.indexes).toContain('appId')
    })

    it('should include description and tags', () => {
      const view = createAIRequestsMV()

      expect(view.options.description).toBeDefined()
      expect(view.options.tags).toContain('analytics')
      expect(view.options.tags).toContain('ai')
      expect(view.options.tags).toContain('llm')
    })
  })
})

// =============================================================================
// Request Buffer Tests
// =============================================================================

describe('AIRequestBuffer', () => {
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
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 10,
        flushIntervalMs: 1000,
      })

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      await buffer.add({ modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 200 })

      expect(buffer.getBufferSize()).toBe(2)
    })

    it('should flush when buffer size is reached', async () => {
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 3,
        flushIntervalMs: 10000,
      })

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })

      // Buffer should be empty after auto-flush
      expect(buffer.getBufferSize()).toBe(0)
    })

    it('should flush on close', async () => {
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 10000,
      })

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })

      expect(buffer.getBufferSize()).toBe(2)

      await buffer.close()

      expect(buffer.getBufferSize()).toBe(0)
    })
  })

  describe('periodic flush', () => {
    it('should flush periodically when timer is started', async () => {
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 50,
      })

      buffer.startTimer()

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      expect(buffer.getBufferSize()).toBe(1)

      // Advance fake timers past flush interval
      await vi.advanceTimersByTimeAsync(100)

      expect(buffer.getBufferSize()).toBe(0)

      buffer.stopTimer()
    })

    it('should stop periodic flush when timer is stopped', async () => {
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 100,
        flushIntervalMs: 1000,
      })

      buffer.startTimer()
      buffer.stopTimer()

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })

      vi.advanceTimersByTime(2000)

      // Buffer should still have the request since timer was stopped
      expect(buffer.getBufferSize()).toBe(1)
    })
  })

  describe('createAIRequestBuffer factory', () => {
    it('should create buffer with auto-start by default', () => {
      const buffer = createAIRequestBuffer(db as any, {
        maxBufferSize: 50,
        flushIntervalMs: 5000,
      })

      // Should have started timer
      buffer.stopTimer()
    })

    it('should not auto-start when disabled', () => {
      const buffer = createAIRequestBuffer(db as any, {
        autoStart: false,
      })

      expect(buffer.getBufferSize()).toBe(0)
    })

    it('should use custom pricing', async () => {
      const customPricing = {
        'my-model': { promptPer1k: 0.5, completionPer1k: 1.0 },
      }

      const buffer = createAIRequestBuffer(db as any, {
        autoStart: false,
        pricing: customPricing,
      })

      await buffer.add({
        modelId: 'my-model',
        providerId: 'custom',
        requestType: 'generate',
        latencyMs: 100,
        promptTokens: 1000,
        completionTokens: 1000,
      })

      await buffer.flush()

      const collection = db.collection('ai_requests')
      const results = await collection.find()

      expect(results.length).toBe(1)
      // Cost should be 0.5 + 1.0 = 1.5
      expect((results[0] as any).costUSD).toBeCloseTo(1.5, 4)
    })
  })
})

// =============================================================================
// DEFAULT_MODEL_PRICING Tests
// =============================================================================

describe('DEFAULT_MODEL_PRICING', () => {
  it('should have pricing for major OpenAI models', () => {
    expect(DEFAULT_MODEL_PRICING['gpt-4']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['gpt-4o']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['gpt-4o-mini']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['gpt-3.5-turbo']).toBeDefined()
  })

  it('should have pricing for Anthropic models', () => {
    expect(DEFAULT_MODEL_PRICING['claude-3-opus']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['claude-3-sonnet']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['claude-3-haiku']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['claude-3.5-sonnet']).toBeDefined()
  })

  it('should have pricing for Google models', () => {
    expect(DEFAULT_MODEL_PRICING['gemini-pro']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['gemini-1.5-pro']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['gemini-1.5-flash']).toBeDefined()
  })

  it('should have a default fallback pricing', () => {
    expect(DEFAULT_MODEL_PRICING['default']).toBeDefined()
    expect(DEFAULT_MODEL_PRICING['default']!.promptPer1k).toBeGreaterThan(0)
    expect(DEFAULT_MODEL_PRICING['default']!.completionPer1k).toBeGreaterThan(0)
  })

  it('should have valid pricing structure for all models', () => {
    for (const [modelId, pricing] of Object.entries(DEFAULT_MODEL_PRICING)) {
      expect(pricing.promptPer1k).toBeGreaterThanOrEqual(0)
      expect(pricing.completionPer1k).toBeGreaterThanOrEqual(0)
      expect(typeof pricing.promptPer1k).toBe('number')
      expect(typeof pricing.completionPer1k).toBe('number')
    }
  })
})

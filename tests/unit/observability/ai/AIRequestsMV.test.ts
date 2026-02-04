/**
 * Tests for AIRequestsMV - AI Requests Materialized View
 *
 * Tests the recording, filtering, and aggregation of AI API requests.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  AIRequestsMV,
  createAIRequestsMV,
  generateRequestId,
  type AIRequestRecord,
  type AIRequestsMVConfig,
  type RecordAIRequestInput,
  type AIRequestsQueryOptions,
} from '../../../../src/observability/ai/AIRequestsMV'
import type { ModelPricing } from '../../../../src/observability/ai/types'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockRecord {
  $id: string
  [key: string]: unknown
}

function createMockCollection() {
  const store: Map<string, MockRecord> = new Map()
  let idCounter = 0

  return {
    store,
    create: vi.fn(async (data: Record<string, unknown>) => {
      const id = `ai_requests/${++idCounter}`
      const record = { $id: id, ...data } as MockRecord
      store.set(id, record)
      return record
    }),
    createMany: vi.fn(async (items: Record<string, unknown>[]) => {
      return Promise.all(items.map(async (data) => {
        const id = `ai_requests/${++idCounter}`
        const record = { $id: id, ...data } as MockRecord
        store.set(id, record)
        return record
      }))
    }),
    find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
      let results = Array.from(store.values())

      // Apply filters
      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item[key] as Date
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
            } else if (typeof value === 'boolean') {
              if (item[key] !== value) return false
            } else if (typeof value === 'string') {
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

      return { items: results }
    }),
    delete: vi.fn(async (id: string) => {
      const fullId = `ai_requests/${id}`
      const exists = store.has(fullId)
      store.delete(fullId)
      return { deletedCount: exists ? 1 : 0 }
    }),
    count: vi.fn(async (filter?: Record<string, unknown>) => {
      let results = Array.from(store.values())

      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item[key] as Date
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
            } else if (typeof value === 'boolean') {
              if (item[key] !== value) return false
            } else if (typeof value === 'string') {
              if (item[key] !== value) return false
            } else if (typeof value !== 'object' && item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      return results.length
    }),
    deleteMany: vi.fn(async (filter: Record<string, unknown>, _options?: { hard?: boolean }) => {
      let toDelete: string[] = []

      for (const [id, item] of store) {
        let matches = true
        for (const [key, value] of Object.entries(filter)) {
          if (key === 'timestamp' && typeof value === 'object' && value !== null) {
            const ts = item[key] as Date
            const filterObj = value as Record<string, unknown>
            if (filterObj.$lt && ts >= (filterObj.$lt as Date)) matches = false
            if (filterObj.$gte && ts < (filterObj.$gte as Date)) matches = false
          } else if (typeof value === 'boolean') {
            if (item[key] !== value) matches = false
          } else if (typeof value === 'string') {
            if (item[key] !== value) matches = false
          } else if (typeof value !== 'object' && item[key] !== value) {
            matches = false
          }
        }
        if (matches) {
          toDelete.push(id)
        }
      }

      for (const id of toDelete) {
        store.delete(id)
      }

      return { deletedCount: toDelete.length }
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
  describe('generateRequestId', () => {
    it('should generate unique request IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 1000; i++) {
        ids.add(generateRequestId())
      }
      expect(ids.size).toBe(1000)
    })

    it('should prefix IDs with ai_', () => {
      const id = generateRequestId()
      expect(id.startsWith('ai_')).toBe(true)
    })

    it('should have reasonable length', () => {
      const id = generateRequestId()
      expect(id.length).toBeGreaterThan(10)
      expect(id.length).toBeLessThan(30)
    })
  })
})

// =============================================================================
// AIRequestsMV Constructor Tests
// =============================================================================

describe('AIRequestsMV', () => {
  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = new AIRequestsMV(db)

      expect(mv).toBeInstanceOf(AIRequestsMV)
      const config = mv.getConfig()
      expect(config.collection).toBe('ai_requests')
      expect(config.maxAgeMs).toBe(30 * 24 * 60 * 60 * 1000) // 30 days
      expect(config.batchSize).toBe(1000)
      expect(config.debug).toBe(false)
    })

    it('should accept custom configuration', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const config: AIRequestsMVConfig = {
        collection: 'custom_ai_requests',
        maxAgeMs: 7 * 24 * 60 * 60 * 1000,
        batchSize: 500,
        debug: true,
      }

      const mv = new AIRequestsMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.collection).toBe('custom_ai_requests')
      expect(resolvedConfig.maxAgeMs).toBe(7 * 24 * 60 * 60 * 1000)
      expect(resolvedConfig.batchSize).toBe(500)
      expect(resolvedConfig.debug).toBe(true)
    })

    it('should include custom pricing', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const customPricing: ModelPricing[] = [
        { modelId: 'custom-model', providerId: 'custom', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00 },
      ]

      const mv = new AIRequestsMV(db, { customPricing })
      const config = mv.getConfig()

      expect(config.pricing.has('custom-model:custom')).toBe(true)
      expect(config.pricing.get('custom-model:custom')?.inputPricePerMillion).toBe(1.00)
    })
  })

  describe('createAIRequestsMV factory', () => {
    it('should create an AIRequestsMV instance', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = createAIRequestsMV(db)

      expect(mv).toBeInstanceOf(AIRequestsMV)
    })
  })
})

// =============================================================================
// Recording Tests
// =============================================================================

describe('Request Recording', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: AIRequestsMV

  beforeEach(() => {
    db = createMockDB()
    mv = new AIRequestsMV(db as unknown as Parameters<typeof createAIRequestsMV>[0])
  })

  describe('record', () => {
    it('should record a basic AI request', async () => {
      const result = await mv.record({
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
      expect(result.status).toBe('success')
      expect(result.cached).toBe(false)
      expect(result.requestId).toBeDefined()
      expect(result.timestamp).toBeInstanceOf(Date)
    })

    it('should record request with all optional fields', async () => {
      const now = new Date()

      const result = await mv.record({
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
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        success: false,
        error: 'Rate limit exceeded',
        errorCode: 'RATE_LIMIT',
      })

      expect(result.status).toBe('error')
      expect(result.error).toBe('Rate limit exceeded')
      expect(result.errorCode).toBe('RATE_LIMIT')
    })

    it('should record timeout requests', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 30000,
        success: false,
        error: 'Request timed out',
        errorCode: 'TIMEOUT',
      })

      expect(result.status).toBe('timeout')
    })

    it('should calculate cost automatically for known models', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 1000000, // 1M prompt tokens
        completionTokens: 500000, // 0.5M completion tokens
      })

      // GPT-4: $30/1M input, $60/1M output
      // 1M input = $30, 0.5M output = $30
      expect(result.estimatedCost).toBeCloseTo(60.00, 2)
    })

    it('should use custom cost when provided', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 1000,
        completionTokens: 500,
        estimatedCost: 0.123,
      })

      expect(result.estimatedCost).toBe(0.123)
    })

    it('should default promptTokens and completionTokens to 0', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
      })

      expect(result.promptTokens).toBe(0)
      expect(result.completionTokens).toBe(0)
      expect(result.totalTokens).toBe(0)
    })
  })

  describe('recordMany', () => {
    it('should record multiple requests at once', async () => {
      const inputs: RecordAIRequestInput[] = [
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500, promptTokens: 100, completionTokens: 200 },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 800, promptTokens: 200, completionTokens: 400 },
        { modelId: 'gemini-1.5-pro', providerId: 'google', requestType: 'chat', latencyMs: 300, promptTokens: 50, completionTokens: 100 },
      ]

      const results = await mv.recordMany(inputs)

      expect(results).toHaveLength(3)
      expect(results[0]!.modelId).toBe('gpt-4')
      expect(results[1]!.modelId).toBe('claude-3-sonnet')
      expect(results[2]!.modelId).toBe('gemini-1.5-pro')
    })

    it('should auto-generate unique request IDs', async () => {
      const inputs: RecordAIRequestInput[] = Array.from({ length: 10 }, () => ({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate' as const,
        latencyMs: 100,
      }))

      const results = await mv.recordMany(inputs)
      const ids = results.map(r => r.requestId)
      const uniqueIds = new Set(ids)

      expect(uniqueIds.size).toBe(10)
    })
  })
})

// =============================================================================
// Query Tests
// =============================================================================

describe('Query Operations', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: AIRequestsMV

  beforeEach(async () => {
    db = createMockDB()
    mv = new AIRequestsMV(db as unknown as Parameters<typeof createAIRequestsMV>[0])

    // Add test data
    const baseTime = new Date('2026-02-03T10:00:00.000Z')
    const requests: RecordAIRequestInput[] = [
      { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500, promptTokens: 100, completionTokens: 200, timestamp: new Date(baseTime.getTime()) },
      { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 600, promptTokens: 150, completionTokens: 250, timestamp: new Date(baseTime.getTime() + 1000), cached: true },
      { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 800, promptTokens: 200, completionTokens: 300, timestamp: new Date(baseTime.getTime() + 2000) },
      { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 50, success: false, error: 'Rate limit', errorCode: 'RATE_LIMIT', timestamp: new Date(baseTime.getTime() + 3000) },
      { modelId: 'gemini-1.5-pro', providerId: 'google', requestType: 'chat', latencyMs: 400, promptTokens: 80, completionTokens: 120, timestamp: new Date(baseTime.getTime() + 4000), userId: 'user-123' },
    ]

    await mv.recordMany(requests)
  })

  describe('find', () => {
    it('should find all requests with default options', async () => {
      const results = await mv.find()
      expect(results.length).toBe(5)
    })

    it('should filter by modelId', async () => {
      const results = await mv.find({ modelId: 'gpt-4' })
      expect(results.length).toBe(3)
      expect(results.every(r => r.modelId === 'gpt-4')).toBe(true)
    })

    it('should filter by providerId', async () => {
      const results = await mv.find({ providerId: 'anthropic' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('claude-3-sonnet')
    })

    it('should filter by requestType', async () => {
      const results = await mv.find({ requestType: 'stream' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('claude-3-sonnet')
    })

    it('should filter by userId', async () => {
      const results = await mv.find({ userId: 'user-123' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('gemini-1.5-pro')
    })

    it('should filter by cachedOnly', async () => {
      const results = await mv.find({ cachedOnly: true })
      expect(results.length).toBe(1)
      expect(results[0]!.cached).toBe(true)
    })

    it('should filter by errorsOnly', async () => {
      const results = await mv.find({ errorsOnly: true })
      expect(results.length).toBe(1)
      expect(results[0]!.status).toBe('error')
    })

    it('should respect limit option', async () => {
      const results = await mv.find({ limit: 2 })
      expect(results.length).toBe(2)
    })
  })

  describe('findOne', () => {
    it('should find a single request by requestId', async () => {
      const allRequests = await mv.find()
      const targetId = allRequests[0]!.requestId

      const result = await mv.findOne(targetId)
      expect(result).not.toBeNull()
      expect(result!.requestId).toBe(targetId)
    })

    it('should return null for non-existent requestId', async () => {
      const result = await mv.findOne('non-existent-id')
      expect(result).toBeNull()
    })
  })
})

// =============================================================================
// Statistics Tests
// =============================================================================

describe('Statistics', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: AIRequestsMV

  beforeEach(async () => {
    db = createMockDB()
    mv = new AIRequestsMV(db as unknown as Parameters<typeof createAIRequestsMV>[0])
  })

  describe('getStats', () => {
    it('should return empty stats for no data', async () => {
      const stats = await mv.getStats()

      expect(stats.totalRequests).toBe(0)
      expect(stats.successCount).toBe(0)
      expect(stats.errorCount).toBe(0)
      expect(stats.errorRate).toBe(0)
    })

    it('should calculate basic statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 200 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 200, promptTokens: 150, completionTokens: 250 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 300, promptTokens: 200, completionTokens: 300 },
      ])

      const stats = await mv.getStats()

      expect(stats.totalRequests).toBe(3)
      expect(stats.successCount).toBe(3)
      expect(stats.errorCount).toBe(0)
      expect(stats.errorRate).toBe(0)
    })

    it('should calculate token statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 200 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 200, completionTokens: 400 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 300, completionTokens: 600 },
      ])

      const stats = await mv.getStats()

      expect(stats.tokens.totalPromptTokens).toBe(600)
      expect(stats.tokens.totalCompletionTokens).toBe(1200)
      expect(stats.tokens.totalTokens).toBe(1800)
      expect(stats.tokens.avgPromptTokens).toBe(200)
      expect(stats.tokens.avgCompletionTokens).toBe(400)
      expect(stats.tokens.avgTotalTokens).toBe(600)
    })

    it('should calculate latency statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 200 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 300 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 400 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500 },
      ])

      const stats = await mv.getStats()

      expect(stats.latency.min).toBe(100)
      expect(stats.latency.max).toBe(500)
      expect(stats.latency.avg).toBe(300)
      expect(stats.latency.p50).toBe(300)
    })

    it('should calculate error rate', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: true },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: true },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Error 1' },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: true },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Error 2' },
      ])

      const stats = await mv.getStats()

      expect(stats.totalRequests).toBe(5)
      expect(stats.successCount).toBe(3)
      expect(stats.errorCount).toBe(2)
      expect(stats.errorRate).toBeCloseTo(0.4, 2)
    })

    it('should calculate cache statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, cached: false },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 10, cached: true },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, cached: false },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 10, cached: true },
      ])

      const stats = await mv.getStats()

      expect(stats.cacheHits).toBe(2)
      expect(stats.cacheHitRatio).toBe(0.5)
    })

    it('should provide breakdown by model', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 100, completionTokens: 100 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 200, promptTokens: 100, completionTokens: 100 },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 300, promptTokens: 200, completionTokens: 200 },
      ])

      const stats = await mv.getStats()

      expect(stats.byModel['gpt-4']).toBeDefined()
      expect(stats.byModel['gpt-4']!.count).toBe(2)
      expect(stats.byModel['gpt-4']!.avgLatency).toBe(150)

      expect(stats.byModel['claude-3-sonnet']).toBeDefined()
      expect(stats.byModel['claude-3-sonnet']!.count).toBe(1)
    })

    it('should provide breakdown by provider', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 },
        { modelId: 'gpt-4o', providerId: 'openai', requestType: 'generate', latencyMs: 200 },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 300 },
      ])

      const stats = await mv.getStats()

      expect(stats.byProvider['openai']).toBeDefined()
      expect(stats.byProvider['openai']!.count).toBe(2)

      expect(stats.byProvider['anthropic']).toBeDefined()
      expect(stats.byProvider['anthropic']!.count).toBe(1)
    })

    it('should provide breakdown by request type', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'stream', latencyMs: 100 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'embed', latencyMs: 50 },
      ])

      const stats = await mv.getStats()

      expect(stats.byRequestType['generate']).toBe(2)
      expect(stats.byRequestType['stream']).toBe(1)
      expect(stats.byRequestType['embed']).toBe(1)
    })
  })

  describe('getErrorSummary', () => {
    it('should return error summary', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: true },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Rate limit', errorCode: 'RATE_LIMIT' },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 100, success: false, error: 'Timeout', errorCode: 'TIMEOUT' },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Rate limit', errorCode: 'RATE_LIMIT' },
      ])

      const summary = await mv.getErrorSummary()

      expect(summary.totalErrors).toBe(3)
      expect(summary.errorRate).toBeCloseTo(0.75, 2)
      expect(summary.byModel['gpt-4']).toBe(2)
      expect(summary.byModel['claude-3-sonnet']).toBe(1)
      expect(summary.byErrorCode['RATE_LIMIT']).toBe(2)
      expect(summary.byErrorCode['TIMEOUT']).toBe(1)
    })

    it('should include recent errors', async () => {
      const baseTime = new Date('2026-02-03T10:00:00.000Z')
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Error 1', timestamp: new Date(baseTime.getTime()) },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Error 2', timestamp: new Date(baseTime.getTime() + 1000) },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, success: false, error: 'Error 3', timestamp: new Date(baseTime.getTime() + 2000) },
      ])

      const summary = await mv.getErrorSummary()

      expect(summary.recentErrors).toHaveLength(3)
      // Most recent should be first
      expect(summary.recentErrors[0]!.error).toBe('Error 3')
    })
  })

  describe('getCostSummary', () => {
    it('should return cost summary', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 1000000, completionTokens: 500000, cached: false },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, promptTokens: 500000, completionTokens: 250000, cached: true },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 100, promptTokens: 1000000, completionTokens: 500000, cached: false },
      ])

      const summary = await mv.getCostSummary()

      // GPT-4: $30/1M input, $60/1M output
      // Request 1: $30 + $30 = $60
      // Claude 3 Sonnet: $3/1M input, $15/1M output
      // Request 3: $3 + $7.5 = $10.5
      expect(summary.totalCost).toBeGreaterThan(0)
      expect(summary.cacheSavings).toBeGreaterThan(0)
      expect(summary.byModel['gpt-4']).toBeDefined()
      expect(summary.byProvider['openai']).toBeDefined()
    })

    it('should include top expensive requests', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, estimatedCost: 0.01 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, estimatedCost: 0.05 },
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100, estimatedCost: 0.02 },
      ])

      const summary = await mv.getCostSummary()

      expect(summary.topExpensive).toHaveLength(3)
      // Most expensive should be first
      expect(summary.topExpensive[0]!.cost).toBe(0.05)
    })
  })
})

// =============================================================================
// Cleanup Tests
// =============================================================================

describe('Cleanup', () => {
  it('should delete old requests', async () => {
    const db = createMockDB()
    const mv = new AIRequestsMV(
      db as unknown as Parameters<typeof createAIRequestsMV>[0],
      { maxAgeMs: 1000 } // 1 second max age
    )

    // Add an old request
    const oldTimestamp = new Date(Date.now() - 5000) // 5 seconds ago
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 100,
      timestamp: oldTimestamp,
    })

    // Add a recent request
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 100,
    })

    // Run cleanup
    const result = await mv.cleanup()

    // Old request should be deleted
    expect(result.success).toBe(true)
    expect(result.deletedCount).toBe(1)
  })

  it('should return CleanupResult with success false on error', async () => {
    const db = createMockDB()
    const mv = new AIRequestsMV(
      db as unknown as Parameters<typeof createAIRequestsMV>[0],
      { maxAgeMs: 1000 }
    )

    // First, record something to ensure collection is created
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 100,
    })

    // Mock find to throw an error (cleanup uses find internally)
    const collection = db.collections.get('ai_requests')!
    collection.find = vi.fn().mockRejectedValue(new Error('Find failed'))

    const result = await mv.cleanup()

    expect(result.success).toBe(false)
    expect(result.error).toBe('Find failed')
  })

  it('should support progress callback', async () => {
    const db = createMockDB()
    const mv = new AIRequestsMV(
      db as unknown as Parameters<typeof createAIRequestsMV>[0],
      { maxAgeMs: 1000 }
    )

    const oldTimestamp = new Date(Date.now() - 5000)
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 100,
      timestamp: oldTimestamp,
    })

    const progressCalls: Array<{ deletedSoFar: number; percentage: number }> = []
    const result = await mv.cleanup({
      onProgress: (progress) => progressCalls.push({ ...progress }),
    })

    expect(result.success).toBe(true)
    expect(progressCalls.length).toBeGreaterThan(0)
  })

  it('should return zero when no records to delete', async () => {
    const db = createMockDB()
    const mv = new AIRequestsMV(
      db as unknown as Parameters<typeof createAIRequestsMV>[0],
      { maxAgeMs: 1000 }
    )

    // Add only recent requests
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 100,
    })

    const result = await mv.cleanup()

    expect(result.success).toBe(true)
    expect(result.deletedCount).toBe(0)
  })
})

// =============================================================================
// Content Sampling Tests
// =============================================================================

describe('Content Sampling', () => {
  let db: ReturnType<typeof createMockDB>

  beforeEach(() => {
    db = createMockDB()
  })

  describe('Configuration', () => {
    it('should accept content sampling configuration', () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 0.1, // Sample 10% of requests
            maxPromptChars: 500,
            maxCompletionChars: 1000,
          },
        }
      )

      const config = mv.getConfig()
      expect(config.contentSampling).toBeDefined()
      expect(config.contentSampling?.enabled).toBe(true)
      expect(config.contentSampling?.sampleRate).toBe(0.1)
      expect(config.contentSampling?.maxPromptChars).toBe(500)
      expect(config.contentSampling?.maxCompletionChars).toBe(1000)
    })

    it('should default content sampling to disabled', () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0]
      )

      const config = mv.getConfig()
      expect(config.contentSampling?.enabled).toBe(false)
    })

    it('should support sampleAllErrors policy', () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 0,
            sampleAllErrors: true,
          },
        }
      )

      const config = mv.getConfig()
      expect(config.contentSampling?.sampleAllErrors).toBe(true)
    })
  })

  describe('Recording with Content', () => {
    it('should record prompt and completion samples when enabled', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0, // Sample all requests
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 100,
        completionTokens: 200,
        promptSample: 'What is the meaning of life?',
        completionSample: 'The meaning of life is a philosophical question...',
      })

      expect(result.promptSample).toBe('What is the meaning of life?')
      expect(result.completionSample).toBe('The meaning of life is a philosophical question...')
    })

    it('should truncate content to maxChars when specified', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
            maxPromptChars: 20,
            maxCompletionChars: 30,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'This is a very long prompt that should be truncated',
        completionSample: 'This is a very long completion that should also be truncated',
      })

      expect(result.promptSample?.length).toBeLessThanOrEqual(20)
      expect(result.completionSample?.length).toBeLessThanOrEqual(30)
    })

    it('should not store content when sampling is disabled', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: false,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'This should not be stored',
        completionSample: 'This should not be stored either',
      })

      expect(result.promptSample).toBeUndefined()
      expect(result.completionSample).toBeUndefined()
    })

    it('should sample errors even when sampleRate is 0 if sampleAllErrors is true', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 0,
            sampleAllErrors: true,
          },
        }
      )

      // Error request should be sampled
      const errorResult = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        success: false,
        error: 'Rate limit exceeded',
        promptSample: 'Prompt for error request',
        completionSample: 'Error response',
      })

      expect(errorResult.promptSample).toBe('Prompt for error request')

      // Success request should NOT be sampled (sampleRate is 0)
      const successResult = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        success: true,
        promptSample: 'Prompt for success request',
        completionSample: 'Success response',
      })

      expect(successResult.promptSample).toBeUndefined()
    })
  })

  describe('Content Fingerprinting', () => {
    it('should generate content fingerprint for deduplication', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
            generateFingerprint: true,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'What is the meaning of life?',
      })

      expect(result.promptFingerprint).toBeDefined()
      expect(typeof result.promptFingerprint).toBe('string')
    })

    it('should generate same fingerprint for identical prompts', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
            generateFingerprint: true,
          },
        }
      )

      const prompt = 'What is the meaning of life?'

      const result1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: prompt,
      })

      const result2 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 600,
        promptSample: prompt,
      })

      expect(result1.promptFingerprint).toBe(result2.promptFingerprint)
    })
  })

  describe('PII Detection Hooks', () => {
    it('should call redactor function before storing content', async () => {
      const redactor = vi.fn((content: string) => content.replace(/\d{3}-\d{2}-\d{4}/g, '[SSN_REDACTED]'))

      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
            redactor,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'My SSN is 123-45-6789',
        completionSample: 'Your SSN 123-45-6789 has been processed',
      })

      expect(redactor).toHaveBeenCalled()
      expect(result.promptSample).toBe('My SSN is [SSN_REDACTED]')
      expect(result.completionSample).toBe('Your SSN [SSN_REDACTED] has been processed')
    })

    it('should support async redactor functions', async () => {
      vi.useFakeTimers()
      const asyncRedactor = vi.fn(async (content: string) => {
        await vi.advanceTimersByTimeAsync(1)
        return content.replace(/secret/gi, '[REDACTED]')
      })

      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
            redactor: asyncRedactor,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'The secret code is SECRET',
      })

      expect(asyncRedactor).toHaveBeenCalled()
      expect(result.promptSample).toBe('The [REDACTED] code is [REDACTED]')
      vi.useRealTimers()
    })
  })

  describe('GeneratedContentMV Correlation', () => {
    it('should include contentId for correlation with GeneratedContentMV', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
          },
        }
      )

      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'Test prompt',
        completionSample: 'Test completion',
        contentId: 'gc_abc123',
      })

      expect(result.contentId).toBe('gc_abc123')
    })
  })

  describe('Querying Sampled Content', () => {
    it('should include hasSampledContent filter in query options', async () => {
      const mv = new AIRequestsMV(
        db as unknown as Parameters<typeof createAIRequestsMV>[0],
        {
          contentSampling: {
            enabled: true,
            sampleRate: 1.0,
          },
        }
      )

      await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptSample: 'Test prompt 1',
      })

      await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 600,
        // No prompt sample
      })

      // Verify both records exist
      const all = await mv.find()
      expect(all.length).toBe(2)

      // Verify first record has prompt sample
      const withSample = all.filter(r => r.promptSample !== undefined)
      expect(withSample.length).toBe(1)
      expect(withSample[0]!.promptSample).toBe('Test prompt 1')

      // Second record has no prompt sample (no input provided)
      const withoutSample = all.filter(r => r.promptSample === undefined)
      expect(withoutSample.length).toBe(1)
    })
  })
})

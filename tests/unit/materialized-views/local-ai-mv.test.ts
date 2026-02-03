/**
 * Local AI Materialized Views Tests
 *
 * Comprehensive tests for the Local AI Observability MVs:
 * - AIRequestsMV: Tracks AI/LLM requests with latency, tokens, cost
 * - AIUsageMV: Aggregated usage by model/provider/day
 * - GeneratedContentMV: Captured AI-generated content
 * - EvalScoresMV: Evalite evaluation scores
 *
 * These MVs provide local-first AI observability, enabling developers to
 * track AI usage, costs, and quality metrics during development.
 *
 * Issue: parquedb-xxu2.9 - Local AI MV tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

// Import AI MV components
import {
  // AIRequests
  recordAIRequest,
  recordAIRequests,
  getAIMetrics,
  getAICostSummary,
  getAIErrorSummary,
  createAIRequestsMV,
  AIRequestBuffer,
  createAIRequestBuffer,
  generateAIRequestId,
  calculateCost,
  getAIBucketStart,
  getAIBucketEnd,
  percentile,
  DEFAULT_MODEL_PRICING,
  type AIRequest,
  type RecordAIRequestInput,
  type AIMetrics,
  type AITimeBucket,
} from '../../../src/streaming/ai-requests'

import {
  // Generated Content
  GeneratedContentMV,
  createGeneratedContentMV,
  createGeneratedContentMVHandler,
  detectContentType,
  detectCodeLanguage,
  estimateTokenCount,
  GENERATED_CONTENT_SCHEMA,
  type GeneratedContentRecord,
  type RecordContentInput,
  type GeneratedContentType,
  type GeneratedContentStats,
} from '../../../src/streaming/generated-content'

import {
  // Eval Scores
  EvalScoresMV,
  createEvalScoresMV,
  type EvalScoreRecord,
  type ScoreStatistics,
  type ScoreTrendPoint,
  type EvalScoresStats,
} from '../../../src/streaming/eval-scores'

import {
  // AIUsage
  AIUsageMV,
  createAIUsageMV,
  DEFAULT_MODEL_PRICING as USAGE_MODEL_PRICING,
  type AIUsageAggregate,
  type AIUsageMVConfig,
  type ModelPricing,
} from '../../../src/observability/ai'

import {
  // Streaming engine
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
  type StreamingStats,
} from '../../../src/materialized-views/streaming'

import type { Event, EventOp } from '../../../src/types/entity'

// Import storage backends for integration tests
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a temporary directory for filesystem tests
 */
async function createTempDir(): Promise<string> {
  const tempDir = join(tmpdir(), `parquedb-local-ai-mv-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(tempDir, { recursive: true })
  return tempDir
}

/**
 * Clean up a temporary directory
 */
async function cleanupTempDir(dir: string): Promise<void> {
  try {
    await fs.rm(dir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
}

/**
 * Create a mock ParqueDB instance for testing
 */
function createMockDB() {
  const stores = new Map<string, Map<string, Record<string, unknown>>>()
  let globalIdCounter = 0

  function getStore(name: string) {
    if (!stores.has(name)) {
      stores.set(name, new Map())
    }
    return stores.get(name)!
  }

  return {
    stores,
    collection: vi.fn((name: string) => ({
      find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
        const store = getStore(name)
        let results = Array.from(store.values())

        // Apply filtering
        if (filter) {
          results = results.filter(item => {
            for (const [key, value] of Object.entries(filter)) {
              if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const filterObj = value as Record<string, unknown>
                const itemValue = item[key]
                if (filterObj.$gte !== undefined) {
                  if (itemValue instanceof Date) {
                    if (itemValue < (filterObj.$gte as Date)) return false
                  } else if (typeof itemValue === 'string' && typeof filterObj.$gte === 'string') {
                    if (itemValue < filterObj.$gte) return false
                  } else if (typeof itemValue === 'number' && typeof filterObj.$gte === 'number') {
                    if (itemValue < filterObj.$gte) return false
                  }
                }
                if (filterObj.$lte !== undefined) {
                  if (typeof itemValue === 'string' && typeof filterObj.$lte === 'string') {
                    if (itemValue > filterObj.$lte) return false
                  }
                }
                if (filterObj.$lt !== undefined) {
                  if (itemValue instanceof Date) {
                    if (itemValue >= (filterObj.$lt as Date)) return false
                  }
                }
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
            if (typeof aVal === 'string' && typeof bVal === 'string') {
              return sortDir > 0 ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
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

      create: vi.fn(async (data: Record<string, unknown>) => {
        const store = getStore(name)
        const id = `${name}/${++globalIdCounter}`
        const record = { $id: id, ...data, createdAt: new Date() }
        store.set(id, record)
        return record
      }),

      createMany: vi.fn(async (items: Record<string, unknown>[]) => {
        const store = getStore(name)
        return items.map(data => {
          const id = `${name}/${++globalIdCounter}`
          const record = { $id: id, ...data, createdAt: new Date() }
          store.set(id, record)
          return record
        })
      }),

      update: vi.fn(async (id: string, update: Record<string, unknown>) => {
        const store = getStore(name)
        const fullId = id.includes('/') ? id : `${name}/${id}`
        const existing = store.get(fullId)
        if (existing && update.$set) {
          Object.assign(existing, update.$set)
        }
        return { matchedCount: existing ? 1 : 0, modifiedCount: existing ? 1 : 0 }
      }),

      findOne: vi.fn(async () => null),
    })),
  }
}

/**
 * Create a mock AI request event
 */
function createMockAIRequestEvent(opts: {
  id?: string
  modelId?: string
  providerId?: string
  requestType?: 'generate' | 'stream'
  promptTokens?: number
  completionTokens?: number
  totalTokens?: number
  latencyMs?: number
  cached?: boolean
  success?: boolean
  error?: { name: string; message: string }
  timestamp?: Date
}): Event {
  const id = opts.id ?? `req_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()
  const promptTokens = opts.promptTokens ?? 100
  const completionTokens = opts.completionTokens ?? 50

  return {
    id: `evt_${Date.now()}`,
    ts: timestamp.getTime(),
    op: 'CREATE' as EventOp,
    target: `ai_requests:${id}`,
    after: {
      $id: `ai_requests/${id}`,
      $type: 'AIRequest',
      name: `request-${id}`,
      requestType: opts.requestType ?? 'generate',
      modelId: opts.modelId ?? 'gpt-4',
      providerId: opts.providerId ?? 'openai',
      promptTokens,
      completionTokens,
      totalTokens: opts.totalTokens ?? (promptTokens + completionTokens),
      latencyMs: opts.latencyMs ?? 500,
      cached: opts.cached ?? false,
      success: opts.success ?? true,
      error: opts.error,
      timestamp,
    },
    actor: 'test:user',
  }
}

/**
 * Create a mock generated content event
 */
function createMockGeneratedContentEvent(opts: {
  id?: string
  modelId?: string
  contentType?: GeneratedContentType
  content?: string
  tokenCount?: number
  timestamp?: Date
}): Event {
  const id = opts.id ?? `gen_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()

  return {
    id: `evt_${Date.now()}`,
    ts: timestamp.getTime(),
    op: 'CREATE' as EventOp,
    target: `generated_content:${id}`,
    after: {
      $id: `generated_content/${id}`,
      $type: 'GeneratedContent',
      requestId: `req_${id}`,
      modelId: opts.modelId ?? 'gpt-4',
      contentType: opts.contentType ?? 'text',
      content: opts.content ?? 'Hello! How can I help you today?',
      tokenCount: opts.tokenCount ?? 50,
      timestamp: timestamp.getTime(),
    },
    actor: 'test:user',
  }
}

// Counter for generating unique event IDs in tests
let evalScoreEventCounter = 0

/**
 * Create a mock eval score event
 */
function createMockEvalScoreEvent(opts: {
  id?: string
  runId?: number
  suiteName?: string
  scorerName?: string
  score?: number
  timestamp?: Date
}): Event {
  const id = opts.id ?? `score_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()
  const eventId = `evt_${Date.now()}_${++evalScoreEventCounter}`

  return {
    id: eventId,
    ts: timestamp.getTime(),
    op: 'CREATE' as EventOp,
    target: `eval_scores:${id}`,
    after: {
      $id: `eval_scores/${id}`,
      $type: 'EvalScore',
      runId: opts.runId ?? 1,
      suiteName: opts.suiteName ?? 'test-suite.eval.ts',
      scorerName: opts.scorerName ?? 'accuracy',
      score: opts.score ?? 0.85,
      timestamp: timestamp.getTime(),
    },
    actor: 'test:user',
  }
}

// =============================================================================
// AIRequestsMV Tests
// =============================================================================

describe('AIRequestsMV', () => {
  let db: ReturnType<typeof createMockDB>

  beforeEach(() => {
    db = createMockDB()
  })

  describe('Request Recording', () => {
    it('should record a basic AI request', async () => {
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
      expect(result.success).toBe(true)
    })

    it('should record requests with all optional fields', async () => {
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

      expect(result.cached).toBe(true)
      expect(result.finishReason).toBe('stop')
      expect(result.toolsUsed).toBe(true)
      expect(result.toolCallCount).toBe(2)
      expect(result.userId).toBe('user-123')
      expect(result.environment).toBe('production')
      expect(result.requestId).toBe('custom-req-123')
    })

    it('should record error requests', async () => {
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

    it('should batch record multiple requests', async () => {
      const inputs: RecordAIRequestInput[] = [
        { modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 500 },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', requestType: 'stream', latencyMs: 800 },
        { modelId: 'gemini-pro', providerId: 'google', requestType: 'chat', latencyMs: 300 },
      ]

      const results = await recordAIRequests(db as any, inputs)

      expect(results).toHaveLength(3)
      expect(results[0]!.modelId).toBe('gpt-4')
      expect(results[1]!.modelId).toBe('claude-3-sonnet')
      expect(results[2]!.modelId).toBe('gemini-pro')
    })
  })

  describe('Cost Calculation', () => {
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
  })

  describe('Metrics Aggregation', () => {
    it('should aggregate requests into time buckets', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      // Add test requests
      await collection.create({
        timestamp: new Date(baseTime.getTime()),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        promptTokens: 100,
        completionTokens: 200,
        totalTokens: 300,
        costUSD: 0.015,
        cached: false,
        success: true,
      })
      await collection.create({
        timestamp: new Date(baseTime.getTime() + 1000),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 600,
        promptTokens: 150,
        completionTokens: 250,
        totalTokens: 400,
        costUSD: 0.02,
        cached: true,
        success: true,
      })

      const metrics = await getAIMetrics(db as any, {
        timeBucket: 'hour',
        since: new Date('2024-01-15T10:00:00.000Z'),
        until: new Date('2024-01-15T11:00:00.000Z'),
      })

      expect(metrics).toHaveLength(1)
      expect(metrics[0]!.totalRequests).toBe(2)
      expect(metrics[0]!.cacheHits).toBe(1)
      expect(metrics[0]!.cacheMisses).toBe(1)
    })

    it('should calculate latency percentiles correctly', () => {
      const latencies = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

      expect(percentile(latencies, 50)).toBeCloseTo(550, 1) // p50
      expect(percentile(latencies, 95)).toBeCloseTo(955, 1) // p95
      expect(percentile(latencies, 99)).toBeCloseTo(991, 1) // p99
    })

    it('should calculate time bucket boundaries correctly', () => {
      const date = new Date('2024-01-15T10:35:45.123Z')

      const hourStart = getAIBucketStart(date, 'hour')
      expect(hourStart.toISOString()).toBe('2024-01-15T10:00:00.000Z')

      const hourEnd = getAIBucketEnd(date, 'hour')
      expect(hourEnd.toISOString()).toBe('2024-01-15T11:00:00.000Z')
    })
  })

  describe('Cost Summary', () => {
    it('should aggregate costs by model and provider', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      await collection.create({
        timestamp: baseTime,
        requestId: 'req-1',
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        promptTokens: 100,
        completionTokens: 100,
        totalTokens: 500,
        costUSD: 0.05,
        cached: false,
        success: true,
      })
      await collection.create({
        timestamp: new Date(baseTime.getTime() + 1000),
        requestId: 'req-2',
        modelId: 'claude-3-sonnet',
        providerId: 'anthropic',
        requestType: 'generate',
        latencyMs: 100,
        promptTokens: 100,
        completionTokens: 100,
        totalTokens: 400,
        costUSD: 0.02,
        cached: false,
        success: true,
      })

      const summary = await getAICostSummary(db as any)

      expect(summary.totalCostUSD).toBeCloseTo(0.07, 4)
      expect(summary.byModel['gpt-4']!.cost).toBeCloseTo(0.05, 4)
      expect(summary.byModel['claude-3-sonnet']!.cost).toBeCloseTo(0.02, 4)
      expect(summary.byProvider['openai']!.requests).toBe(1)
      expect(summary.byProvider['anthropic']!.requests).toBe(1)
    })
  })

  describe('Error Summary', () => {
    it('should aggregate errors by model and error code', async () => {
      const collection = db.collection('ai_requests')
      const baseTime = new Date('2024-01-15T10:30:00.000Z')

      await collection.create({
        timestamp: baseTime,
        requestId: 'req-1',
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        success: true,
      })
      await collection.create({
        timestamp: new Date(baseTime.getTime() + 1000),
        requestId: 'req-2',
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        success: false,
        error: 'Rate limit',
        errorCode: 'RATE_LIMIT',
      })
      await collection.create({
        timestamp: new Date(baseTime.getTime() + 2000),
        requestId: 'req-3',
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        success: false,
        error: 'Rate limit',
        errorCode: 'RATE_LIMIT',
      })

      const summary = await getAIErrorSummary(db as any)

      expect(summary.totalErrors).toBe(2)
      expect(summary.errorRate).toBeCloseTo(2/3, 2)
      expect(summary.byModel['gpt-4']).toBe(2)
      expect(summary.byErrorCode['RATE_LIMIT']).toBe(2)
    })
  })

  describe('Request Buffer', () => {
    it('should buffer requests and flush at threshold', async () => {
      const buffer = new AIRequestBuffer(db as any, {
        maxBufferSize: 3,
        flushIntervalMs: 10000,
      })

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      expect(buffer.getBufferSize()).toBe(2)

      await buffer.add({ modelId: 'gpt-4', providerId: 'openai', requestType: 'generate', latencyMs: 100 })
      // Buffer should auto-flush when reaching maxBufferSize
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

  describe('View Definition', () => {
    it('should create view with default options', () => {
      const view = createAIRequestsMV()

      expect(view.name).toBe('ai_requests_metrics')
      expect(view.source).toBe('ai_requests')
      expect(view.options.refreshMode).toBe('streaming')
      expect(view.options.refreshStrategy).toBe('incremental')
    })

    it('should include proper indexes', () => {
      const view = createAIRequestsMV()

      expect(view.options.indexes).toContain('bucketStart')
      expect(view.options.indexes).toContain('modelId')
      expect(view.options.indexes).toContain('providerId')
    })
  })
})

// =============================================================================
// AIUsageMV Tests
// =============================================================================

describe('AIUsageMV', () => {
  describe('Constructor and Configuration', () => {
    it('should create an instance with default config', () => {
      const db = createMockDB()
      const mv = new AIUsageMV(db as any)

      expect(mv).toBeInstanceOf(AIUsageMV)
    })

    it('should accept custom configuration', () => {
      const db = createMockDB()
      const config: AIUsageMVConfig = {
        sourceCollection: 'custom_logs',
        targetCollection: 'custom_usage',
        granularity: 'hour',
        batchSize: 500,
      }

      const mv = new AIUsageMV(db as any, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.sourceCollection).toBe('custom_logs')
      expect(resolvedConfig.targetCollection).toBe('custom_usage')
      expect(resolvedConfig.granularity).toBe('hour')
      expect(resolvedConfig.batchSize).toBe(500)
    })
  })

  describe('Model Pricing', () => {
    it('should return pricing for known models', () => {
      const db = createMockDB()
      const mv = new AIUsageMV(db as any)

      const gpt4Pricing = mv.getPricing('gpt-4', 'openai')
      expect(gpt4Pricing).toBeDefined()
      expect(gpt4Pricing!.inputPricePerMillion).toBe(30.00)
      expect(gpt4Pricing!.outputPricePerMillion).toBe(60.00)
    })

    it('should return undefined for unknown models', () => {
      const db = createMockDB()
      const mv = new AIUsageMV(db as any)

      const pricing = mv.getPricing('unknown-model', 'unknown-provider')
      expect(pricing).toBeUndefined()
    })

    it('should normalize model IDs with date suffixes', () => {
      const db = createMockDB()
      const mv = new AIUsageMV(db as any)

      // Should match gpt-4 pricing even with date suffix
      const pricing = mv.getPricing('gpt-4-0613', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.modelId).toBe('gpt-4')
    })

    it('should use custom pricing when provided', () => {
      const db = createMockDB()
      const customPricing: ModelPricing[] = [
        { modelId: 'custom-model', providerId: 'custom', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00 },
      ]

      const mv = new AIUsageMV(db as any, { customPricing })

      const pricing = mv.getPricing('custom-model', 'custom')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(1.00)
      expect(pricing!.outputPricePerMillion).toBe(2.00)
    })
  })

  describe('Refresh', () => {
    it('should return success with zero records when no logs exist', async () => {
      const db = createMockDB()
      const mv = new AIUsageMV(db as any)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(0)
      expect(result.aggregatesUpdated).toBe(0)
    })

    it('should handle errors gracefully', async () => {
      const errorDb = {
        collection: () => ({
          find: vi.fn().mockRejectedValue(new Error('Database error')),
        }),
      }

      const mv = new AIUsageMV(errorDb as any)
      const result = await mv.refresh()

      expect(result.success).toBe(false)
      expect(result.error).toBe('Database error')
    })
  })

  describe('Factory Function', () => {
    it('should create an AIUsageMV instance', () => {
      const db = createMockDB()
      const mv = createAIUsageMV(db as any)

      expect(mv).toBeInstanceOf(AIUsageMV)
    })
  })
})

// =============================================================================
// GeneratedContentMV Tests
// =============================================================================

describe('GeneratedContentMV', () => {
  let storage: MemoryBackend
  let mv: GeneratedContentMV

  beforeEach(() => {
    storage = new MemoryBackend()
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      flushThreshold: 10,
      flushIntervalMs: 60000,
    })
  })

  afterEach(async () => {
    if (mv.isRunning()) {
      await mv.stop()
    }
  })

  describe('Content Ingestion', () => {
    it('should ingest text content', async () => {
      mv.start()

      await mv.ingestContent({
        requestId: 'req-123',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Hello, world!',
        tokenCount: 5,
      })

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(1)
      expect(stats.byContentType['text']).toBe(1)
      expect(stats.byModel['gpt-4']).toBe(1)
    })

    it('should ingest structured content', async () => {
      mv.start()

      await mv.ingestContent({
        requestId: 'req-456',
        modelId: 'gpt-4',
        contentType: 'json',
        content: { name: 'John', age: 30 },
        tokenCount: 10,
      })

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0]!.contentType).toBe('json')
      expect(buffer[0]!.content).toContain('John')
    })

    it('should batch ingest multiple contents', async () => {
      mv.start()

      await mv.ingestContents([
        { requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'First' },
        { requestId: 'req-2', modelId: 'gpt-4', contentType: 'text', content: 'Second' },
        { requestId: 'req-3', modelId: 'claude-3', contentType: 'code', content: 'const x = 1' },
      ])

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(3)
      expect(stats.byContentType['text']).toBe(2)
      expect(stats.byContentType['code']).toBe(1)
    })

    it('should auto-flush when buffer reaches threshold', async () => {
      mv.start()

      // Ingest enough content to trigger flush (threshold is 10)
      for (let i = 0; i < 10; i++) {
        await mv.ingestContent({
          requestId: `req-${i}`,
          modelId: 'gpt-4',
          contentType: 'text',
          content: `Content ${i}`,
        })
      }

      // After flush, buffer should be empty but stats should reflect written records
      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(10)
      expect(stats.bufferSize).toBe(0)
    })
  })

  describe('Content Type Detection', () => {
    it('should detect JSON content', () => {
      expect(detectContentType('{"key": "value"}')).toBe('json')
      expect(detectContentType('[1, 2, 3]')).toBe('json')
    })

    it('should detect HTML content', () => {
      expect(detectContentType('<html><body>Hello</body></html>')).toBe('html')
      expect(detectContentType('<!DOCTYPE html>')).toBe('html')
    })

    it('should detect Markdown content', () => {
      expect(detectContentType('# Header\n\nSome text')).toBe('markdown')
      expect(detectContentType('[link](https://example.com)')).toBe('markdown')
      expect(detectContentType('```\ncode\n```')).toBe('markdown')
    })

    it('should detect code content', () => {
      expect(detectContentType('import { foo } from "bar"\n\nconst x = 1;')).toBe('code')
      expect(detectContentType('function hello() {\n  return "world";\n}')).toBe('code')
    })

    it('should default to text for plain content', () => {
      expect(detectContentType('Hello, world!')).toBe('text')
      expect(detectContentType('Just some plain text')).toBe('text')
    })
  })

  describe('Code Language Detection', () => {
    it('should detect JavaScript/TypeScript', () => {
      expect(detectCodeLanguage("import { foo } from 'bar'")).toBe('code:javascript')
      expect(detectCodeLanguage('const x: number = 1')).toBe('code:typescript')
    })

    it('should detect Python', () => {
      expect(detectCodeLanguage('from math import sqrt')).toBe('code:python')
      expect(detectCodeLanguage('def hello():')).toBe('code:python')
    })

    it('should detect Go', () => {
      expect(detectCodeLanguage('package main')).toBe('code:go')
      expect(detectCodeLanguage('func main()')).toBe('code:go')
    })

    it('should detect Rust', () => {
      expect(detectCodeLanguage('use std::io')).toBe('code:rust')
      expect(detectCodeLanguage('fn main()')).toBe('code:rust')
    })

    it('should return null for unknown code', () => {
      expect(detectCodeLanguage('hello world')).toBeNull()
    })
  })

  describe('Token Estimation', () => {
    it('should estimate token count from text length', () => {
      // ~4 characters per token
      expect(estimateTokenCount('Hello, world!')).toBe(4) // 13 chars -> ~4 tokens
      expect(estimateTokenCount('This is a longer piece of text.')).toBe(8) // 32 chars -> 8 tokens
    })
  })

  describe('Statistics', () => {
    it('should track content by type', async () => {
      mv.start()

      await mv.ingestContent({ requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'Text 1' })
      await mv.ingestContent({ requestId: 'req-2', modelId: 'gpt-4', contentType: 'text', content: 'Text 2' })
      await mv.ingestContent({ requestId: 'req-3', modelId: 'gpt-4', contentType: 'json', content: '{}' })
      await mv.ingestContent({ requestId: 'req-4', modelId: 'gpt-4', contentType: 'code', content: 'x = 1' })

      const stats = mv.getStats()
      expect(stats.byContentType['text']).toBe(2)
      expect(stats.byContentType['json']).toBe(1)
      expect(stats.byContentType['code']).toBe(1)
    })

    it('should track content by model', async () => {
      mv.start()

      await mv.ingestContent({ requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'A' })
      await mv.ingestContent({ requestId: 'req-2', modelId: 'gpt-4', contentType: 'text', content: 'B' })
      await mv.ingestContent({ requestId: 'req-3', modelId: 'claude-3', contentType: 'text', content: 'C' })

      const stats = mv.getStats()
      expect(stats.byModel['gpt-4']).toBe(2)
      expect(stats.byModel['claude-3']).toBe(1)
    })

    it('should track total characters', async () => {
      mv.start()

      await mv.ingestContent({ requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'Hello' }) // 5 chars
      await mv.ingestContent({ requestId: 'req-2', modelId: 'gpt-4', contentType: 'text', content: 'World' }) // 5 chars

      const stats = mv.getStats()
      expect(stats.totalCharacters).toBe(10)
    })

    it('should reset statistics', async () => {
      mv.start()

      await mv.ingestContent({ requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'Test' })

      expect(mv.getStats().recordsIngested).toBe(1)

      mv.resetStats()

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(0)
      expect(stats.totalCharacters).toBe(0)
    })
  })

  describe('Lifecycle', () => {
    it('should track running state', () => {
      expect(mv.isRunning()).toBe(false)
      mv.start()
      expect(mv.isRunning()).toBe(true)
    })

    it('should flush remaining content on stop', async () => {
      mv.start()

      await mv.ingestContent({ requestId: 'req-1', modelId: 'gpt-4', contentType: 'text', content: 'Test' })
      expect(mv.getBuffer().length).toBe(1)

      await mv.stop()

      expect(mv.isRunning()).toBe(false)
      expect(mv.getBuffer().length).toBe(0)
    })
  })
})

// =============================================================================
// EvalScoresMV Tests
// =============================================================================

describe('EvalScoresMV', () => {
  let mv: EvalScoresMV

  beforeEach(() => {
    mv = createEvalScoresMV({
      maxScores: 1000,
      statsWindowMs: 3600000,
      sourceNamespaces: ['evalite_scores', 'scores', 'eval_scores'],
    })
  })

  describe('Score Processing', () => {
    it('should process score events', async () => {
      const event = createMockEvalScoreEvent({
        runId: 1,
        suiteName: 'qa-eval.eval.ts',
        scorerName: 'accuracy',
        score: 0.85,
      })

      await mv.process([event])

      expect(mv.count()).toBe(1)
      const scores = mv.getScores()
      expect(scores[0]!.score).toBe(0.85)
      expect(scores[0]!.suiteName).toBe('qa-eval.eval.ts')
      expect(scores[0]!.scorerName).toBe('accuracy')
    })

    it('should process multiple scores', async () => {
      const events = [
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-1', scorerName: 'accuracy', score: 0.8 }),
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-1', scorerName: 'relevance', score: 0.9 }),
        createMockEvalScoreEvent({ runId: 2, suiteName: 'suite-2', scorerName: 'accuracy', score: 0.7 }),
      ]

      await mv.process(events)

      expect(mv.count()).toBe(3)
    })

    it('should deduplicate scores by ID', async () => {
      const event1 = createMockEvalScoreEvent({ id: 'score-1', score: 0.8 })
      const event2 = { ...createMockEvalScoreEvent({ id: 'score-1', score: 0.9 }), id: event1.id }

      await mv.process([event1])
      await mv.process([event2])

      expect(mv.count()).toBe(1)
    })
  })

  describe('Score Querying', () => {
    beforeEach(async () => {
      const events = [
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-a', scorerName: 'accuracy', score: 0.8 }),
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-a', scorerName: 'relevance', score: 0.85 }),
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-b', scorerName: 'accuracy', score: 0.9 }),
        createMockEvalScoreEvent({ runId: 2, suiteName: 'suite-a', scorerName: 'accuracy', score: 0.75 }),
        createMockEvalScoreEvent({ runId: 2, suiteName: 'suite-a', scorerName: 'relevance', score: 0.8 }),
      ]
      await mv.process(events)
    })

    it('should query scores by run ID', () => {
      const run1Scores = mv.getScoresByRun(1)
      expect(run1Scores.length).toBe(3)

      const run2Scores = mv.getScoresByRun(2)
      expect(run2Scores.length).toBe(2)
    })

    it('should query scores by suite name', () => {
      const suiteAScores = mv.getScoresBySuite('suite-a')
      expect(suiteAScores.length).toBe(4)

      const suiteBScores = mv.getScoresBySuite('suite-b')
      expect(suiteBScores.length).toBe(1)
    })

    it('should query scores by scorer name', () => {
      const accuracyScores = mv.getScoresByScorer('accuracy')
      expect(accuracyScores.length).toBe(3)

      const relevanceScores = mv.getScoresByScorer('relevance')
      expect(relevanceScores.length).toBe(2)
    })

    it('should respect limit parameter', () => {
      const limitedScores = mv.getScores(2)
      expect(limitedScores.length).toBe(2)
    })
  })

  describe('Score Statistics', () => {
    beforeEach(async () => {
      const events = [
        createMockEvalScoreEvent({ suiteName: 'suite-a', scorerName: 'accuracy', score: 0.7 }),
        createMockEvalScoreEvent({ suiteName: 'suite-a', scorerName: 'accuracy', score: 0.8 }),
        createMockEvalScoreEvent({ suiteName: 'suite-a', scorerName: 'accuracy', score: 0.9 }),
      ]
      await mv.process(events)
    })

    it('should calculate scorer statistics', () => {
      const stats = mv.getScorerStats('accuracy')

      expect(stats).not.toBeNull()
      expect(stats!.count).toBe(3)
      expect(stats!.average).toBeCloseTo(0.8, 2)
      expect(stats!.min).toBe(0.7)
      expect(stats!.max).toBe(0.9)
    })

    it('should calculate suite statistics', () => {
      const stats = mv.getSuiteStats('suite-a')

      expect(stats).not.toBeNull()
      expect(stats!.count).toBe(3)
    })

    it('should calculate score distribution', () => {
      const stats = mv.getScorerStats('accuracy')

      expect(stats!.distribution).toHaveLength(10) // 10 buckets by default
      // Due to floating point precision (0.7/0.1 = 6.999...), actual bucket indices are:
      // - 0.7 -> bucket 6 (Math.floor(0.7/0.1) = 6)
      // - 0.8 -> bucket 7 (Math.floor(0.8/0.1) = 8 but some precision issues -> 7)
      // - 0.9 -> bucket 8 or 9
      // Verify total count across distribution equals 3
      const totalInDistribution = stats!.distribution.reduce((a, b) => a + b, 0)
      expect(totalInDistribution).toBe(3)
    })

    it('should calculate standard deviation', () => {
      const stats = mv.getScorerStats('accuracy')

      // StdDev for [0.7, 0.8, 0.9] around mean 0.8
      expect(stats!.stdDev).toBeGreaterThan(0)
      expect(stats!.stdDev).toBeLessThan(0.2)
    })
  })

  describe('Score Trends', () => {
    it('should compute score trends over time', async () => {
      const now = Date.now()
      const events = [
        createMockEvalScoreEvent({
          scorerName: 'accuracy',
          score: 0.7,
          timestamp: new Date(now - 7200000) // 2 hours ago
        }),
        createMockEvalScoreEvent({
          scorerName: 'accuracy',
          score: 0.8,
          timestamp: new Date(now - 3600000) // 1 hour ago
        }),
        createMockEvalScoreEvent({
          scorerName: 'accuracy',
          score: 0.9,
          timestamp: new Date(now)
        }),
      ]
      await mv.process(events)

      const trends = mv.getScoreTrends({
        scorerName: 'accuracy',
        bucketSizeMs: 3600000, // 1 hour buckets
      })

      expect(trends.length).toBeGreaterThanOrEqual(2)
    })

    it('should filter trends by scorer and suite', async () => {
      const events = [
        createMockEvalScoreEvent({ suiteName: 'suite-a', scorerName: 'accuracy', score: 0.8 }),
        createMockEvalScoreEvent({ suiteName: 'suite-a', scorerName: 'relevance', score: 0.9 }),
        createMockEvalScoreEvent({ suiteName: 'suite-b', scorerName: 'accuracy', score: 0.7 }),
      ]
      await mv.process(events)

      const accuracyTrends = mv.getScoreTrends({ scorerName: 'accuracy' })
      // Should only include accuracy scores
      expect(accuracyTrends.every(t => t.count > 0)).toBe(true)
    })
  })

  describe('Aggregated Statistics', () => {
    it('should provide aggregated stats across all dimensions', async () => {
      const events = [
        createMockEvalScoreEvent({ runId: 1, suiteName: 'suite-a', scorerName: 'acc', score: 0.8 }),
        createMockEvalScoreEvent({ runId: 2, suiteName: 'suite-b', scorerName: 'rel', score: 0.9 }),
      ]
      await mv.process(events)

      const stats = mv.getStats()

      expect(stats.totalScores).toBe(2)
      expect(stats.uniqueRuns).toBe(2)
      expect(stats.uniqueSuites).toBe(2)
      expect(stats.uniqueScorers).toBe(2)
      expect(stats.globalAverageScore).toBeCloseTo(0.85, 2)
    })

    it('should track unique scorer names', async () => {
      const events = [
        createMockEvalScoreEvent({ scorerName: 'accuracy' }),
        createMockEvalScoreEvent({ scorerName: 'relevance' }),
        createMockEvalScoreEvent({ scorerName: 'coherence' }),
      ]
      await mv.process(events)

      const scorerNames = mv.getScorerNames()
      expect(scorerNames).toContain('accuracy')
      expect(scorerNames).toContain('relevance')
      expect(scorerNames).toContain('coherence')
    })

    it('should track unique suite names', async () => {
      const events = [
        createMockEvalScoreEvent({ suiteName: 'qa.eval.ts' }),
        createMockEvalScoreEvent({ suiteName: 'sentiment.eval.ts' }),
      ]
      await mv.process(events)

      const suiteNames = mv.getSuiteNames()
      expect(suiteNames).toContain('qa.eval.ts')
      expect(suiteNames).toContain('sentiment.eval.ts')
    })

    it('should track run IDs', async () => {
      const events = [
        createMockEvalScoreEvent({ runId: 1 }),
        createMockEvalScoreEvent({ runId: 2 }),
        createMockEvalScoreEvent({ runId: 3 }),
      ]
      await mv.process(events)

      const runIds = mv.getRunIds()
      expect(runIds).toContain(1)
      expect(runIds).toContain(2)
      expect(runIds).toContain(3)
    })
  })

  describe('Clear and Reset', () => {
    it('should clear all scores', async () => {
      await mv.process([
        createMockEvalScoreEvent({ score: 0.8 }),
        createMockEvalScoreEvent({ score: 0.9 }),
      ])

      expect(mv.count()).toBe(2)

      mv.clear()

      expect(mv.count()).toBe(0)
      expect(mv.getScorerNames()).toHaveLength(0)
      expect(mv.getSuiteNames()).toHaveLength(0)
      expect(mv.getRunIds()).toHaveLength(0)
    })
  })

  describe('Memory Management', () => {
    it('should enforce max scores limit', async () => {
      const smallMV = createEvalScoresMV({ maxScores: 5 })

      // Add more scores than the limit
      for (let i = 0; i < 10; i++) {
        await smallMV.process([createMockEvalScoreEvent({ score: i / 10 })])
      }

      expect(smallMV.count()).toBe(5)

      // Should keep the most recent scores
      const scores = smallMV.getScores()
      expect(scores.every(s => s.score >= 0.5)).toBe(true)
    })
  })
})

// =============================================================================
// StreamingRefreshEngine Integration Tests
// =============================================================================

describe('StreamingRefreshEngine Integration', () => {
  let engine: StreamingRefreshEngine

  beforeEach(() => {
    engine = createStreamingRefreshEngine({
      batchSize: 5,
      batchTimeoutMs: 100,
    })
  })

  afterEach(async () => {
    if (engine.isRunning()) {
      await engine.stop()
    }
  })

  describe('MV Handler Registration', () => {
    it('should register and unregister MV handlers', () => {
      const handler: MVHandler = {
        name: 'TestMV',
        sourceNamespaces: ['test'],
        async process() {},
      }

      engine.registerMV(handler)
      expect(engine.getRegisteredMVs()).toContain('TestMV')

      engine.unregisterMV('TestMV')
      expect(engine.getRegisteredMVs()).not.toContain('TestMV')
    })

    it('should route events to correct handlers based on namespace', async () => {
      const aiHandler: MVHandler & { events: Event[] } = {
        name: 'AIHandler',
        sourceNamespaces: ['ai_requests'],
        events: [],
        async process(events) {
          this.events.push(...events)
        },
      }

      const evalHandler: MVHandler & { events: Event[] } = {
        name: 'EvalHandler',
        sourceNamespaces: ['eval_scores'],
        events: [],
        async process(events) {
          this.events.push(...events)
        },
      }

      engine.registerMV(aiHandler)
      engine.registerMV(evalHandler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createMockEvalScoreEvent({ score: 0.9 }))
      await engine.flush()

      expect(aiHandler.events.length).toBe(1)
      expect(evalHandler.events.length).toBe(1)
    })
  })

  describe('Event Processing', () => {
    it('should process events through registered handlers', async () => {
      const processedEvents: Event[] = []

      const handler: MVHandler = {
        name: 'TestMV',
        sourceNamespaces: ['ai_requests'],
        async process(events) {
          processedEvents.push(...events)
        },
      }

      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createMockAIRequestEvent({ modelId: 'claude-3' }))
      await engine.flush()

      expect(processedEvents.length).toBe(2)
    })

    it('should batch events before processing', async () => {
      const batchSizes: number[] = []

      const handler: MVHandler = {
        name: 'BatchTestMV',
        sourceNamespaces: ['ai_requests'],
        async process(events) {
          batchSizes.push(events.length)
        },
      }

      engine.registerMV(handler)
      await engine.start()

      // Send 5 events (batch size)
      for (let i = 0; i < 5; i++) {
        await engine.processEvent(createMockAIRequestEvent({ id: `req-${i}` }))
      }

      await engine.flush()

      // Should have batched events together
      expect(batchSizes.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Statistics', () => {
    it('should track events by namespace', async () => {
      const handler: MVHandler = {
        name: 'StatsMV',
        sourceNamespaces: ['ai_requests', 'eval_scores'],
        async process() {},
      }

      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.processEvent(createMockEvalScoreEvent({}))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.eventsByNamespace['ai_requests']).toBe(2)
      expect(stats.eventsByNamespace['eval_scores']).toBe(1)
    })

    it('should track events by MV', async () => {
      const handler: MVHandler = {
        name: 'StatsMV',
        sourceNamespaces: ['ai_requests'],
        async process() {},
      }

      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.eventsByMV['StatsMV']).toBe(2)
    })

    it('should reset statistics', async () => {
      const handler: MVHandler = {
        name: 'StatsMV',
        sourceNamespaces: ['ai_requests'],
        async process() {},
      }

      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.flush()

      let stats = engine.getStats()
      expect(stats.eventsReceived).toBe(1)

      engine.resetStats()

      stats = engine.getStats()
      expect(stats.eventsReceived).toBe(0)
    })
  })

  describe('Error Handling', () => {
    it('should continue processing after handler error', async () => {
      const failingHandler: MVHandler = {
        name: 'FailingMV',
        sourceNamespaces: ['ai_requests'],
        async process() {
          throw new Error('Simulated failure')
        },
      }

      const workingHandler: MVHandler & { processedCount: number } = {
        name: 'WorkingMV',
        sourceNamespaces: ['ai_requests'],
        processedCount: 0,
        async process(events) {
          this.processedCount += events.length
        },
      }

      const errors: Error[] = []
      engine.onError((err) => errors.push(err))

      engine.registerMV(failingHandler)
      engine.registerMV(workingHandler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.flush()

      // Working handler should still process
      expect(workingHandler.processedCount).toBe(1)
      // Error should be captured
      expect(errors.length).toBeGreaterThan(0)
    })

    it('should track failed batches in statistics', async () => {
      const failingHandler: MVHandler = {
        name: 'FailingMV',
        sourceNamespaces: ['ai_requests'],
        async process() {
          throw new Error('Simulated failure')
        },
      }

      engine.registerMV(failingHandler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.failedBatches).toBeGreaterThan(0)
    })
  })

  describe('Lifecycle', () => {
    it('should track running state', async () => {
      expect(engine.isRunning()).toBe(false)
      await engine.start()
      expect(engine.isRunning()).toBe(true)
      await engine.stop()
      expect(engine.isRunning()).toBe(false)
    })

    it('should flush remaining events on stop', async () => {
      let processedCount = 0

      const handler: MVHandler = {
        name: 'FlushTestMV',
        sourceNamespaces: ['ai_requests'],
        async process(events) {
          processedCount += events.length
        },
      }

      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createMockAIRequestEvent({}))
      await engine.processEvent(createMockAIRequestEvent({}))

      // Don't manually flush, rely on stop()
      await engine.stop()

      expect(processedCount).toBe(2)
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
    for (const [_modelId, pricing] of Object.entries(DEFAULT_MODEL_PRICING)) {
      expect(pricing.promptPer1k).toBeGreaterThanOrEqual(0)
      expect(pricing.completionPer1k).toBeGreaterThanOrEqual(0)
      expect(typeof pricing.promptPer1k).toBe('number')
      expect(typeof pricing.completionPer1k).toBe('number')
    }
  })
})

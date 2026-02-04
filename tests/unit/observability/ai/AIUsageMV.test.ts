/**
 * Tests for AIUsageMV - AI Usage Materialized View
 *
 * Tests the aggregation of AI API usage data, cost estimation,
 * and query capabilities of the AIUsageMV class.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  AIUsageMV,
  createAIUsageMV,
  DEFAULT_MODEL_PRICING,
  type AIUsageAggregate,
  type AIUsageMVConfig,
  type ModelPricing,
} from '../../../../src/observability/ai'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockLog {
  $id: string
  timestamp: Date
  modelId: string
  providerId: string
  requestType: 'generate' | 'stream'
  usage?: {
    promptTokens?: number | undefined
    completionTokens?: number | undefined
    totalTokens?: number | undefined
  }
  latencyMs: number
  cached: boolean
  error?: { name: string; message: string } | undefined
}

interface MockAggregate {
  $id: string
  dateKey: string
  modelId: string
  providerId: string
  [key: string]: unknown
}

function createMockParqueDB(initialLogs: MockLog[] = [], initialAggregates: MockAggregate[] = []) {
  const logs = [...initialLogs]
  const aggregates = [...initialAggregates]

  return {
    collection: (name: string) => ({
      find: vi.fn().mockImplementation(async (filter: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
        if (name === 'ai_logs') {
          let results = [...logs]

          // Apply timestamp filter
          if (filter.timestamp) {
            const tsFilter = filter.timestamp as { $gte?: Date }
            if (tsFilter.$gte) {
              results = results.filter(log => new Date(log.timestamp) >= tsFilter.$gte!)
            }
          }

          // Apply sort
          if (options?.sort?.timestamp) {
            results.sort((a, b) => {
              const diff = new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
              return options.sort!.timestamp === 1 ? diff : -diff
            })
          }

          // Apply limit
          if (options?.limit) {
            results = results.slice(0, options.limit)
          }

          return { items: results }
        }

        if (name === 'ai_usage') {
          let results = [...aggregates]

          // Apply filters
          if (filter.dateKey) {
            if (typeof filter.dateKey === 'string') {
              results = results.filter(agg => agg.dateKey === filter.dateKey)
            } else if (typeof filter.dateKey === 'object') {
              const dk = filter.dateKey as { $gte?: string; $lte?: string }
              if (dk.$gte) {
                results = results.filter(agg => agg.dateKey >= dk.$gte!)
              }
              if (dk.$lte) {
                results = results.filter(agg => agg.dateKey <= dk.$lte!)
              }
            }
          }

          if (filter.modelId) {
            results = results.filter(agg => agg.modelId === filter.modelId)
          }

          if (filter.providerId) {
            results = results.filter(agg => agg.providerId === filter.providerId)
          }

          // Apply sort
          if (options?.sort) {
            const sortKey = Object.keys(options.sort)[0]!
            const sortOrder = options.sort[sortKey]
            results.sort((a, b) => {
              const aVal = a[sortKey]
              const bVal = b[sortKey]
              if (typeof aVal === 'string' && typeof bVal === 'string') {
                return sortOrder === 1 ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
              }
              return sortOrder === 1 ? Number(aVal) - Number(bVal) : Number(bVal) - Number(aVal)
            })
          }

          // Apply limit
          if (options?.limit) {
            results = results.slice(0, options.limit)
          }

          return { items: results }
        }

        return { items: [] }
      }),

      create: vi.fn().mockImplementation(async (data: Record<string, unknown>) => {
        const id = `${name}/${Date.now()}`
        const created = { ...data, $id: id }
        if (name === 'ai_usage') {
          aggregates.push(created as MockAggregate)
        }
        return created
      }),

      update: vi.fn().mockImplementation(async (id: string, update: Record<string, unknown>) => {
        if (name === 'ai_usage') {
          const idx = aggregates.findIndex(a => a.$id.endsWith(`/${id}`))
          if (idx >= 0 && update.$set) {
            Object.assign(aggregates[idx], update.$set)
          }
        }
        return { matchedCount: 1, modifiedCount: 1 }
      }),

      findOne: vi.fn().mockImplementation(async () => null),
    }),

    // Expose internal state for testing
    __logs: logs,
    __aggregates: aggregates,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('AIUsageMV', () => {
  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      expect(mv).toBeInstanceOf(AIUsageMV)
    })

    it('should accept custom configuration', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const config: AIUsageMVConfig = {
        sourceCollection: 'custom_logs',
        targetCollection: 'custom_usage',
        granularity: 'hour',
        batchSize: 500,
      }

      const mv = new AIUsageMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.sourceCollection).toBe('custom_logs')
      expect(resolvedConfig.targetCollection).toBe('custom_usage')
      expect(resolvedConfig.granularity).toBe('hour')
      expect(resolvedConfig.batchSize).toBe(500)
    })
  })

  describe('createAIUsageMV factory', () => {
    it('should create an AIUsageMV instance', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = createAIUsageMV(db)

      expect(mv).toBeInstanceOf(AIUsageMV)
    })
  })

  describe('getPricing', () => {
    it('should return pricing for known models', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const gpt4Pricing = mv.getPricing('gpt-4', 'openai')
      expect(gpt4Pricing).toBeDefined()
      expect(gpt4Pricing!.inputPricePerMillion).toBe(30.00)
      expect(gpt4Pricing!.outputPricePerMillion).toBe(60.00)

      const claudePricing = mv.getPricing('claude-3-opus', 'anthropic')
      expect(claudePricing).toBeDefined()
      expect(claudePricing!.inputPricePerMillion).toBe(15.00)
    })

    it('should return undefined for unknown models', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const pricing = mv.getPricing('unknown-model', 'unknown-provider')
      expect(pricing).toBeUndefined()
    })

    it('should normalize model IDs with date suffixes', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      // Should match gpt-4 pricing even with date suffix
      const pricing = mv.getPricing('gpt-4-0613', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.modelId).toBe('gpt-4')
    })

    it('should use custom pricing when provided', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const customPricing: ModelPricing[] = [
        { modelId: 'custom-model', providerId: 'custom', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00 },
      ]

      const mv = new AIUsageMV(db, { customPricing })

      const pricing = mv.getPricing('custom-model', 'custom')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(1.00)
      expect(pricing!.outputPricePerMillion).toBe(2.00)
    })

    it('should override default pricing with custom pricing', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const customPricing: ModelPricing[] = [
        { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 100.00, outputPricePerMillion: 200.00 },
      ]

      const mv = new AIUsageMV(db, { customPricing })

      const pricing = mv.getPricing('gpt-4', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(100.00)
    })
  })

  describe('refresh', () => {
    it('should return success with zero records when no logs exist', async () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(0)
      expect(result.aggregatesUpdated).toBe(0)
    })

    it('should process logs and create aggregates', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50, totalTokens: 150 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 200, completionTokens: 100, totalTokens: 300 },
          latencyMs: 750,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(2)
      expect(result.aggregatesUpdated).toBe(1)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)

      // Check aggregates were created
      expect(mockDb.__aggregates.length).toBe(1)
      const agg = mockDb.__aggregates[0]
      expect(agg.modelId).toBe('gpt-4')
      expect(agg.providerId).toBe('openai')
      expect(agg.requestCount).toBe(2)
      expect(agg.totalPromptTokens).toBe(300)
      expect(agg.totalCompletionTokens).toBe(150)
    })

    it('should handle errors gracefully', async () => {
      const db = {
        collection: () => ({
          find: vi.fn().mockRejectedValue(new Error('Database error')),
        }),
      } as unknown as Parameters<typeof createAIUsageMV>[0]

      const mv = new AIUsageMV(db)
      const result = await mv.refresh()

      expect(result.success).toBe(false)
      expect(result.error).toBe('Database error')
    })

    it('should track error counts from failed requests', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          cached: false,
          error: { name: 'APIError', message: 'Rate limited' },
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.successCount).toBe(1)
      expect(agg.errorCount).toBe(1)
    })

    it('should track cached response counts', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 5,
          cached: true,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.cachedCount).toBe(1)
    })

    it('should track request types (generate vs stream)', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'stream',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 1000,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.generateCount).toBe(1)
      expect(agg.streamCount).toBe(1)
    })

    it('should calculate latency statistics', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/3',
          timestamp: new Date('2026-02-03T12:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 300,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.totalLatencyMs).toBe(900)
      expect(agg.avgLatencyMs).toBe(300)
      expect(agg.minLatencyMs).toBe(100)
      expect(agg.maxLatencyMs).toBe(500)
    })

    it('should calculate percentile latencies (p50, p90, p95, p99)', async () => {
      // Generate 100 requests with known latency distribution
      // Latencies: 1, 2, 3, ..., 100 ms
      const logs: MockLog[] = []
      for (let i = 1; i <= 100; i++) {
        logs.push({
          $id: `ai_logs/${i}`,
          timestamp: new Date(`2026-02-03T10:${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}Z`),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: i,
          cached: false,
        })
      }

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]

      // For 100 values from 1-100:
      // p50 should be around 50 (median)
      // p90 should be around 90
      // p95 should be around 95
      // p99 should be around 99
      expect(agg.p50LatencyMs).toBeDefined()
      expect(agg.p90LatencyMs).toBeDefined()
      expect(agg.p95LatencyMs).toBeDefined()
      expect(agg.p99LatencyMs).toBeDefined()

      // Allow some tolerance for rounding
      expect(agg.p50LatencyMs).toBeGreaterThanOrEqual(49)
      expect(agg.p50LatencyMs).toBeLessThanOrEqual(51)
      expect(agg.p90LatencyMs).toBeGreaterThanOrEqual(89)
      expect(agg.p90LatencyMs).toBeLessThanOrEqual(91)
      expect(agg.p95LatencyMs).toBeGreaterThanOrEqual(94)
      expect(agg.p95LatencyMs).toBeLessThanOrEqual(96)
      expect(agg.p99LatencyMs).toBeGreaterThanOrEqual(98)
      expect(agg.p99LatencyMs).toBeLessThanOrEqual(100)
    })

    it('should handle percentiles with small sample sizes', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 100,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T10:01:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 200,
          cached: false,
        },
        {
          $id: 'ai_logs/3',
          timestamp: new Date('2026-02-03T10:02:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          latencyMs: 300,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]

      // With 3 samples, percentiles should still be calculated
      expect(agg.p50LatencyMs).toBeDefined()
      expect(agg.p90LatencyMs).toBeDefined()
      expect(agg.p95LatencyMs).toBeDefined()
      expect(agg.p99LatencyMs).toBeDefined()

      // For [100, 200, 300]: p50 should be 200, p99 should be 300
      expect(agg.p50LatencyMs).toBe(200)
      expect(agg.p99LatencyMs).toBe(300)
    })

    it('should calculate cost estimates', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 1000000, completionTokens: 500000 }, // 1M input, 0.5M output
          latencyMs: 500,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      // GPT-4: $30/1M input, $60/1M output
      expect(agg.estimatedInputCost).toBe(30.00) // 1M * $30/1M
      expect(agg.estimatedOutputCost).toBe(30.00) // 0.5M * $60/1M
      expect(agg.estimatedTotalCost).toBe(60.00)
    })

    it('should create separate aggregates for different models', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T11:00:00Z'),
          modelId: 'claude-3-opus',
          providerId: 'anthropic',
          requestType: 'generate',
          usage: { promptTokens: 200, completionTokens: 100 },
          latencyMs: 750,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      expect(mockDb.__aggregates.length).toBe(2)

      const gpt4Agg = mockDb.__aggregates.find(a => a.modelId === 'gpt-4')
      const claudeAgg = mockDb.__aggregates.find(a => a.modelId === 'claude-3-opus')

      expect(gpt4Agg).toBeDefined()
      expect(claudeAgg).toBeDefined()
      expect(gpt4Agg!.requestCount).toBe(1)
      expect(claudeAgg!.requestCount).toBe(1)
    })

    it('should create separate aggregates for different dates', async () => {
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-01T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-02T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 200, completionTokens: 100 },
          latencyMs: 750,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      expect(mockDb.__aggregates.length).toBe(2)

      const feb1Agg = mockDb.__aggregates.find(a => a.dateKey === '2026-02-01')
      const feb2Agg = mockDb.__aggregates.find(a => a.dateKey === '2026-02-02')

      expect(feb1Agg).toBeDefined()
      expect(feb2Agg).toBeDefined()
    })
  })

  describe('getUsage', () => {
    it('should query aggregates with default options', async () => {
      const aggregates: MockAggregate[] = [
        { $id: 'ai_usage/1', dateKey: '2026-02-03', modelId: 'gpt-4', providerId: 'openai', requestCount: 10 },
        { $id: 'ai_usage/2', dateKey: '2026-02-03', modelId: 'claude-3-opus', providerId: 'anthropic', requestCount: 5 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const usage = await mv.getUsage()

      expect(usage.length).toBe(2)
    })

    it('should filter by modelId', async () => {
      const aggregates: MockAggregate[] = [
        { $id: 'ai_usage/1', dateKey: '2026-02-03', modelId: 'gpt-4', providerId: 'openai', requestCount: 10 },
        { $id: 'ai_usage/2', dateKey: '2026-02-03', modelId: 'claude-3-opus', providerId: 'anthropic', requestCount: 5 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const usage = await mv.getUsage({ modelId: 'gpt-4' })

      expect(usage.length).toBe(1)
      expect(usage[0].modelId).toBe('gpt-4')
    })

    it('should filter by providerId', async () => {
      const aggregates: MockAggregate[] = [
        { $id: 'ai_usage/1', dateKey: '2026-02-03', modelId: 'gpt-4', providerId: 'openai', requestCount: 10 },
        { $id: 'ai_usage/2', dateKey: '2026-02-03', modelId: 'claude-3-opus', providerId: 'anthropic', requestCount: 5 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const usage = await mv.getUsage({ providerId: 'anthropic' })

      expect(usage.length).toBe(1)
      expect(usage[0].providerId).toBe('anthropic')
    })

    it('should filter by date range', async () => {
      const aggregates: MockAggregate[] = [
        { $id: 'ai_usage/1', dateKey: '2026-02-01', modelId: 'gpt-4', providerId: 'openai', requestCount: 10 },
        { $id: 'ai_usage/2', dateKey: '2026-02-02', modelId: 'gpt-4', providerId: 'openai', requestCount: 15 },
        { $id: 'ai_usage/3', dateKey: '2026-02-03', modelId: 'gpt-4', providerId: 'openai', requestCount: 20 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const usage = await mv.getUsage({
        from: new Date('2026-02-02'),
        to: new Date('2026-02-02'),
      })

      expect(usage.length).toBe(1)
      expect(usage[0].dateKey).toBe('2026-02-02')
    })

    it('should respect limit option', async () => {
      const aggregates: MockAggregate[] = [
        { $id: 'ai_usage/1', dateKey: '2026-02-01', modelId: 'gpt-4', providerId: 'openai', requestCount: 10 },
        { $id: 'ai_usage/2', dateKey: '2026-02-02', modelId: 'gpt-4', providerId: 'openai', requestCount: 15 },
        { $id: 'ai_usage/3', dateKey: '2026-02-03', modelId: 'gpt-4', providerId: 'openai', requestCount: 20 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const usage = await mv.getUsage({ limit: 2 })

      expect(usage.length).toBe(2)
    })
  })

  describe('getSummary', () => {
    it('should return a summary with zero values when no aggregates exist', async () => {
      const mockDb = createMockParqueDB([], [])
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const summary = await mv.getSummary()

      expect(summary.totalRequests).toBe(0)
      expect(summary.totalTokens).toBe(0)
      expect(summary.estimatedTotalCost).toBe(0)
    })

    it('should aggregate statistics across all records', async () => {
      const aggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-01',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 10,
          successCount: 9,
          errorCount: 1,
          totalTokens: 1000,
          totalPromptTokens: 600,
          totalCompletionTokens: 400,
          totalLatencyMs: 5000,
          estimatedTotalCost: 0.50,
        },
        {
          $id: 'ai_usage/2',
          dateKey: '2026-02-02',
          modelId: 'claude-3-opus',
          providerId: 'anthropic',
          requestCount: 5,
          successCount: 5,
          errorCount: 0,
          totalTokens: 500,
          totalPromptTokens: 300,
          totalCompletionTokens: 200,
          totalLatencyMs: 2500,
          estimatedTotalCost: 0.25,
        },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const summary = await mv.getSummary()

      expect(summary.totalRequests).toBe(15)
      expect(summary.totalSuccessful).toBe(14)
      expect(summary.totalErrors).toBe(1)
      expect(summary.totalTokens).toBe(1500)
      expect(summary.totalPromptTokens).toBe(900)
      expect(summary.totalCompletionTokens).toBe(600)
      expect(summary.estimatedTotalCost).toBe(0.75)
      expect(summary.avgLatencyMs).toBe(500) // 7500 / 15
      expect(summary.errorRate).toBeCloseTo(1 / 15, 5)
    })

    it('should provide breakdown by model', async () => {
      const aggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-01',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 10,
          successCount: 10,
          errorCount: 0,
          totalTokens: 1000,
          totalPromptTokens: 600,
          totalCompletionTokens: 400,
          totalLatencyMs: 5000,
          estimatedTotalCost: 0.50,
        },
        {
          $id: 'ai_usage/2',
          dateKey: '2026-02-01',
          modelId: 'claude-3-opus',
          providerId: 'anthropic',
          requestCount: 5,
          successCount: 5,
          errorCount: 0,
          totalTokens: 500,
          totalPromptTokens: 300,
          totalCompletionTokens: 200,
          totalLatencyMs: 2500,
          estimatedTotalCost: 0.25,
        },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const summary = await mv.getSummary()

      expect(summary.byModel['gpt-4']).toBeDefined()
      expect(summary.byModel['gpt-4'].requestCount).toBe(10)
      expect(summary.byModel['gpt-4'].totalTokens).toBe(1000)
      expect(summary.byModel['gpt-4'].estimatedCost).toBe(0.50)

      expect(summary.byModel['claude-3-opus']).toBeDefined()
      expect(summary.byModel['claude-3-opus'].requestCount).toBe(5)
    })

    it('should provide breakdown by provider', async () => {
      const aggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-01',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 10,
          successCount: 10,
          errorCount: 0,
          totalTokens: 1000,
          totalPromptTokens: 600,
          totalCompletionTokens: 400,
          totalLatencyMs: 5000,
          estimatedTotalCost: 0.50,
        },
        {
          $id: 'ai_usage/2',
          dateKey: '2026-02-01',
          modelId: 'gpt-4o',
          providerId: 'openai',
          requestCount: 20,
          successCount: 20,
          errorCount: 0,
          totalTokens: 2000,
          totalPromptTokens: 1200,
          totalCompletionTokens: 800,
          totalLatencyMs: 4000,
          estimatedTotalCost: 0.10,
        },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const summary = await mv.getSummary()

      expect(summary.byProvider['openai']).toBeDefined()
      expect(summary.byProvider['openai'].requestCount).toBe(30)
      expect(summary.byProvider['openai'].totalTokens).toBe(3000)
      expect(summary.byProvider['openai'].estimatedCost).toBe(0.60)
    })
  })

  describe('incremental refresh', () => {
    it('should preserve existing aggregate values when processing new logs', async () => {
      // Initial aggregate with existing data (simulating data loaded from DB)
      const existingAggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-03',
          modelId: 'gpt-4',
          providerId: 'openai',
          granularity: 'day',
          requestCount: 5,
          successCount: 5,
          errorCount: 0,
          cachedCount: 1,
          generateCount: 3,
          streamCount: 2,
          totalPromptTokens: 500,
          totalCompletionTokens: 250,
          totalTokens: 750,
          avgTokensPerRequest: 150,
          totalLatencyMs: 2500,
          avgLatencyMs: 500,
          minLatencyMs: 100,
          maxLatencyMs: 1000,
          estimatedInputCost: 0.015,
          estimatedOutputCost: 0.015,
          estimatedTotalCost: 0.030,
          createdAt: new Date('2026-02-03T08:00:00Z'),
          updatedAt: new Date('2026-02-03T10:00:00Z'),
          version: 2,
        },
      ]

      // New log to add to existing aggregate
      const logs: MockLog[] = [
        {
          $id: 'ai_logs/new1',
          timestamp: new Date('2026-02-03T12:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50, totalTokens: 150 },
          latencyMs: 300,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs, existingAggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(1)
      expect(result.aggregatesUpdated).toBe(1)

      // Verify the aggregate was updated correctly with merged values
      const agg = mockDb.__aggregates[0]
      expect(agg.requestCount).toBe(6) // 5 existing + 1 new
      expect(agg.successCount).toBe(6) // 5 existing + 1 new
      expect(agg.generateCount).toBe(4) // 3 existing + 1 new
      expect(agg.totalPromptTokens).toBe(600) // 500 existing + 100 new
      expect(agg.totalCompletionTokens).toBe(300) // 250 existing + 50 new
      expect(agg.totalTokens).toBe(900) // 750 existing + 150 new
      expect(agg.totalLatencyMs).toBe(2800) // 2500 existing + 300 new
      expect(agg.minLatencyMs).toBe(100) // min(100, 300) = 100
      expect(agg.maxLatencyMs).toBe(1000) // max(1000, 300) = 1000
      expect(agg.version).toBe(3) // 2 existing + 1 increment
    })

    it('should handle minLatencyMs stored as null (from JSON serialization of Infinity)', async () => {
      // Simulate what happens when Infinity is serialized to JSON and back
      const existingAggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-03',
          modelId: 'gpt-4',
          providerId: 'openai',
          granularity: 'day',
          requestCount: 0,
          successCount: 0,
          errorCount: 0,
          cachedCount: 0,
          generateCount: 0,
          streamCount: 0,
          totalPromptTokens: 0,
          totalCompletionTokens: 0,
          totalTokens: 0,
          avgTokensPerRequest: 0,
          totalLatencyMs: 0,
          avgLatencyMs: 0,
          minLatencyMs: null, // This is what Infinity becomes after JSON serialization
          maxLatencyMs: 0,
          estimatedInputCost: 0,
          estimatedOutputCost: 0,
          estimatedTotalCost: 0,
          createdAt: new Date('2026-02-03T08:00:00Z'),
          updatedAt: new Date('2026-02-03T08:00:00Z'),
          version: 1,
        },
      ]

      const logs: MockLog[] = [
        {
          $id: 'ai_logs/1',
          timestamp: new Date('2026-02-03T10:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 100, completionTokens: 50 },
          latencyMs: 500,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs, existingAggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)

      const agg = mockDb.__aggregates[0]
      expect(agg.minLatencyMs).toBe(500) // Should be 500, not null or NaN
    })

    it('should preserve cost estimates when merging aggregates', async () => {
      const existingAggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-03',
          modelId: 'gpt-4',
          providerId: 'openai',
          granularity: 'day',
          requestCount: 1,
          successCount: 1,
          errorCount: 0,
          cachedCount: 0,
          generateCount: 1,
          streamCount: 0,
          totalPromptTokens: 1000000, // 1M tokens
          totalCompletionTokens: 500000, // 0.5M tokens
          totalTokens: 1500000,
          avgTokensPerRequest: 1500000,
          totalLatencyMs: 1000,
          avgLatencyMs: 1000,
          minLatencyMs: 1000,
          maxLatencyMs: 1000,
          estimatedInputCost: 30.0, // $30 for 1M input tokens
          estimatedOutputCost: 30.0, // $30 for 0.5M output tokens ($60/M)
          estimatedTotalCost: 60.0,
          createdAt: new Date('2026-02-03T08:00:00Z'),
          updatedAt: new Date('2026-02-03T08:00:00Z'),
          version: 1,
        },
      ]

      const logs: MockLog[] = [
        {
          $id: 'ai_logs/2',
          timestamp: new Date('2026-02-03T12:00:00Z'),
          modelId: 'gpt-4',
          providerId: 'openai',
          requestType: 'generate',
          usage: { promptTokens: 1000000, completionTokens: 500000 }, // Another 1M input, 0.5M output
          latencyMs: 2000,
          cached: false,
        },
      ]

      const mockDb = createMockParqueDB(logs, existingAggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.requestCount).toBe(2)
      expect(agg.totalPromptTokens).toBe(2000000) // 1M + 1M
      expect(agg.totalCompletionTokens).toBe(1000000) // 0.5M + 0.5M
      expect(agg.estimatedInputCost).toBe(60.0) // $30 + $30
      expect(agg.estimatedOutputCost).toBe(60.0) // $30 + $30
      expect(agg.estimatedTotalCost).toBe(120.0) // $60 + $60
    })
  })

  describe('getDailyCosts', () => {
    it('should return daily cost breakdown', async () => {
      const aggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-01',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 10,
          granularity: 'day',
          totalTokens: 1000,
          estimatedInputCost: 0.30,
          estimatedOutputCost: 0.20,
          estimatedTotalCost: 0.50,
        },
        {
          $id: 'ai_usage/2',
          dateKey: '2026-02-02',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 15,
          granularity: 'day',
          totalTokens: 1500,
          estimatedInputCost: 0.45,
          estimatedOutputCost: 0.30,
          estimatedTotalCost: 0.75,
        },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const costs = await mv.getDailyCosts()

      expect(costs.length).toBe(2)
      expect(costs[0].dateKey).toBe('2026-02-01')
      expect(costs[0].totalCost).toBe(0.50)
      expect(costs[1].dateKey).toBe('2026-02-02')
      expect(costs[1].totalCost).toBe(0.75)
    })

    it('should aggregate multiple models on the same day', async () => {
      const aggregates: MockAggregate[] = [
        {
          $id: 'ai_usage/1',
          dateKey: '2026-02-01',
          modelId: 'gpt-4',
          providerId: 'openai',
          requestCount: 10,
          granularity: 'day',
          totalTokens: 1000,
          estimatedInputCost: 0.30,
          estimatedOutputCost: 0.20,
          estimatedTotalCost: 0.50,
        },
        {
          $id: 'ai_usage/2',
          dateKey: '2026-02-01',
          modelId: 'claude-3-opus',
          providerId: 'anthropic',
          requestCount: 5,
          granularity: 'day',
          totalTokens: 500,
          estimatedInputCost: 0.15,
          estimatedOutputCost: 0.10,
          estimatedTotalCost: 0.25,
        },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db)

      const costs = await mv.getDailyCosts()

      expect(costs.length).toBe(1)
      expect(costs[0].dateKey).toBe('2026-02-01')
      expect(costs[0].totalCost).toBe(0.75)
      expect(costs[0].inputCost).toBeCloseTo(0.45, 10)
      expect(costs[0].outputCost).toBeCloseTo(0.30, 10)
      expect(costs[0].totalTokens).toBe(1500)
      expect(costs[0].requestCount).toBe(15)
    })
  })
})

// =============================================================================
// Type Tests
// =============================================================================

describe('DEFAULT_MODEL_PRICING', () => {
  it('should contain OpenAI models', () => {
    const openaiModels = DEFAULT_MODEL_PRICING.filter(p => p.providerId === 'openai')
    expect(openaiModels.length).toBeGreaterThan(0)
    expect(openaiModels.some(p => p.modelId === 'gpt-4')).toBe(true)
    expect(openaiModels.some(p => p.modelId === 'gpt-4o')).toBe(true)
  })

  it('should contain Anthropic models', () => {
    const anthropicModels = DEFAULT_MODEL_PRICING.filter(p => p.providerId === 'anthropic')
    expect(anthropicModels.length).toBeGreaterThan(0)
    expect(anthropicModels.some(p => p.modelId === 'claude-3-opus')).toBe(true)
    expect(anthropicModels.some(p => p.modelId === 'claude-3-sonnet')).toBe(true)
  })

  it('should contain Google models', () => {
    const googleModels = DEFAULT_MODEL_PRICING.filter(p => p.providerId === 'google')
    expect(googleModels.length).toBeGreaterThan(0)
    expect(googleModels.some(p => p.modelId.includes('gemini'))).toBe(true)
  })

  it('should have valid pricing values', () => {
    for (const pricing of DEFAULT_MODEL_PRICING) {
      expect(pricing.inputPricePerMillion).toBeGreaterThanOrEqual(0)
      expect(pricing.outputPricePerMillion).toBeGreaterThanOrEqual(0)
    }
  })
})

// =============================================================================
// Date Key Generation Tests
// =============================================================================

describe('date key generation', () => {
  it('should group by day correctly', async () => {
    const logs: MockLog[] = [
      {
        $id: 'ai_logs/1',
        timestamp: new Date('2026-02-03T00:00:00Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
      {
        $id: 'ai_logs/2',
        timestamp: new Date('2026-02-03T23:59:59Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
    ]

    const mockDb = createMockParqueDB(logs)
    const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
    const mv = new AIUsageMV(db, { granularity: 'day' })

    await mv.refresh()

    // Both should be in the same day aggregate
    expect(mockDb.__aggregates.length).toBe(1)
    expect(mockDb.__aggregates[0].dateKey).toBe('2026-02-03')
    expect(mockDb.__aggregates[0].requestCount).toBe(2)
  })

  it('should group by hour correctly', async () => {
    const logs: MockLog[] = [
      {
        $id: 'ai_logs/1',
        timestamp: new Date('2026-02-03T10:00:00Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
      {
        $id: 'ai_logs/2',
        timestamp: new Date('2026-02-03T10:59:59Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
      {
        $id: 'ai_logs/3',
        timestamp: new Date('2026-02-03T11:00:00Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
    ]

    const mockDb = createMockParqueDB(logs)
    const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
    const mv = new AIUsageMV(db, { granularity: 'hour' })

    await mv.refresh()

    // Should be 2 aggregates: 10:xx and 11:xx
    expect(mockDb.__aggregates.length).toBe(2)
    expect(mockDb.__aggregates.some(a => a.dateKey === '2026-02-03T10')).toBe(true)
    expect(mockDb.__aggregates.some(a => a.dateKey === '2026-02-03T11')).toBe(true)
  })

  it('should group by month correctly', async () => {
    const logs: MockLog[] = [
      {
        $id: 'ai_logs/1',
        timestamp: new Date('2026-02-01T10:00:00Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
      {
        $id: 'ai_logs/2',
        timestamp: new Date('2026-02-28T23:59:59Z'),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
      },
    ]

    const mockDb = createMockParqueDB(logs)
    const db = mockDb as unknown as Parameters<typeof createAIUsageMV>[0]
    const mv = new AIUsageMV(db, { granularity: 'month' })

    await mv.refresh()

    // Both should be in the same month aggregate
    expect(mockDb.__aggregates.length).toBe(1)
    expect(mockDb.__aggregates[0].dateKey).toBe('2026-02')
    expect(mockDb.__aggregates[0].requestCount).toBe(2)
  })
})

/**
 * AI SDK Middleware - Materialized Views Integration Tests
 *
 * Tests for the integration between AI SDK middleware and ParqueDB's
 * materialized views system for local AI observability.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  AIObservabilityMVIntegration,
  createAIObservabilityMVs,
  queryBuiltinView,
  type AIObservabilityConfig,
  type AIAnalyticsView,
  type ModelUsageData,
  type HourlyRequestData,
  type ErrorRateData,
  type CacheHitRateData,
  type TokenUsageData,
  type LatencyPercentileData,
} from '../../../src/integrations/ai-sdk'
import type { LogEntry } from '../../../src/integrations/ai-sdk'

// =============================================================================
// Test Helpers
// =============================================================================

function createLogEntry(overrides: Partial<LogEntry> = {}): LogEntry {
  return {
    $id: `log_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    $type: 'AILog',
    name: `log-test-${Date.now()}`,
    timestamp: new Date(),
    modelId: 'gpt-4',
    providerId: 'openai',
    requestType: 'generate',
    latencyMs: 100,
    cached: false,
    ...overrides,
  }
}

function createLogEntries(count: number, overrides: Partial<LogEntry> = {}): LogEntry[] {
  return Array.from({ length: count }, (_, i) =>
    createLogEntry({
      ...overrides,
      $id: `log_${Date.now()}_${i}`,
      timestamp: new Date(Date.now() - i * 1000),
    })
  )
}

// =============================================================================
// Core Functionality Tests
// =============================================================================

describe('AIObservabilityMVIntegration', () => {
  let integration: AIObservabilityMVIntegration

  afterEach(async () => {
    if (integration) {
      await integration.stop()
    }
  })

  describe('Initialization', () => {
    it('should create integration with default config', () => {
      integration = createAIObservabilityMVs()

      expect(integration).toBeInstanceOf(AIObservabilityMVIntegration)
      expect(integration.isRunning()).toBe(false)
    })

    it('should create integration with custom config', () => {
      integration = createAIObservabilityMVs({
        basePath: './custom-path',
        batchSize: 100,
        batchTimeoutMs: 2000,
        enableBuiltinViews: true,
        retentionMs: 24 * 60 * 60 * 1000,
      })

      expect(integration).toBeInstanceOf(AIObservabilityMVIntegration)
    })

    it('should initialize with builtin views when enabled', () => {
      integration = createAIObservabilityMVs({
        enableBuiltinViews: true,
      })

      const viewNames = integration.getViewNames()
      expect(viewNames).toContain('model_usage')
      expect(viewNames).toContain('hourly_requests')
      expect(viewNames).toContain('error_rates')
      expect(viewNames).toContain('latency_percentiles')
      expect(viewNames).toContain('cache_hit_rates')
      expect(viewNames).toContain('token_usage')
    })

    it('should not include builtin views when disabled', () => {
      integration = createAIObservabilityMVs({
        enableBuiltinViews: false,
      })

      const viewNames = integration.getViewNames()
      expect(viewNames).not.toContain('model_usage')
      expect(viewNames).toHaveLength(0)
    })
  })

  describe('Lifecycle', () => {
    it('should start and stop cleanly', async () => {
      integration = createAIObservabilityMVs()

      expect(integration.isRunning()).toBe(false)

      await integration.start()
      expect(integration.isRunning()).toBe(true)

      await integration.stop()
      expect(integration.isRunning()).toBe(false)
    })

    it('should handle multiple start/stop cycles', async () => {
      integration = createAIObservabilityMVs()

      await integration.start()
      await integration.stop()
      await integration.start()
      await integration.stop()

      expect(integration.isRunning()).toBe(false)
    })
  })

  describe('Log Entry Processing', () => {
    beforeEach(async () => {
      integration = createAIObservabilityMVs()
      await integration.start()
    })

    it('should process a single log entry', async () => {
      const entry = createLogEntry()

      await integration.processLogEntry(entry)
      await integration.stop()

      const state = integration.getState()
      expect(state.entriesProcessed).toBe(1)
    })

    it('should process multiple log entries', async () => {
      const entries = createLogEntries(10)

      for (const entry of entries) {
        await integration.processLogEntry(entry)
      }
      await integration.stop()

      const state = integration.getState()
      expect(state.entriesProcessed).toBe(10)
    })

    it('should update builtin views on log entry', async () => {
      const entry = createLogEntry({
        modelId: 'gpt-4',
        usage: { promptTokens: 10, completionTokens: 5, totalTokens: 15 },
      })

      await integration.processLogEntry(entry)
      await integration.stop()

      const modelUsage = await integration.query<Map<string, ModelUsageData>>('model_usage')
      expect(modelUsage).toBeDefined()
      expect(modelUsage!.has('gpt-4')).toBe(true)

      const gpt4Data = modelUsage!.get('gpt-4')
      expect(gpt4Data?.requestCount).toBe(1)
      expect(gpt4Data?.totalTokens).toBe(15)
    })

    it('should query raw logs', async () => {
      const entries = createLogEntries(5)

      for (const entry of entries) {
        await integration.processLogEntry(entry)
      }
      await integration.stop()

      const logs = await integration.queryLogs()
      expect(logs).toHaveLength(5)
    })

    it('should filter logs by modelId', async () => {
      await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
      await integration.processLogEntry(createLogEntry({ modelId: 'claude-3' }))
      await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
      await integration.stop()

      const logs = await integration.queryLogs({ modelId: 'gpt-4' })
      expect(logs).toHaveLength(2)
      expect(logs.every(l => l.modelId === 'gpt-4')).toBe(true)
    })

    it('should filter logs by time range', async () => {
      const now = Date.now()
      const entries = [
        createLogEntry({ timestamp: new Date(now - 3600000) }), // 1 hour ago
        createLogEntry({ timestamp: new Date(now - 1800000) }), // 30 min ago
        createLogEntry({ timestamp: new Date(now) }), // now
      ]

      for (const entry of entries) {
        await integration.processLogEntry(entry)
      }
      await integration.stop()

      const logs = await integration.queryLogs({
        since: new Date(now - 2700000), // 45 min ago
      })
      expect(logs).toHaveLength(2)
    })

    it('should respect limit in log queries', async () => {
      const entries = createLogEntries(10)

      for (const entry of entries) {
        await integration.processLogEntry(entry)
      }
      await integration.stop()

      const logs = await integration.queryLogs({ limit: 3 })
      expect(logs).toHaveLength(3)
    })
  })

  describe('Builtin Views', () => {
    beforeEach(async () => {
      integration = createAIObservabilityMVs({ enableBuiltinViews: true })
      await integration.start()
    })

    describe('model_usage', () => {
      it('should track request counts by model', async () => {
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
        await integration.processLogEntry(createLogEntry({ modelId: 'claude-3' }))
        await integration.stop()

        const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')

        expect(usage!.get('gpt-4')?.requestCount).toBe(2)
        expect(usage!.get('claude-3')?.requestCount).toBe(1)
      })

      it('should track error counts by model', async () => {
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          error: { name: 'APIError', message: 'Rate limited' },
        }))
        await integration.stop()

        const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')

        expect(usage!.get('gpt-4')?.requestCount).toBe(2)
        expect(usage!.get('gpt-4')?.errorCount).toBe(1)
      })

      it('should track token usage by model', async () => {
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          usage: { promptTokens: 100, completionTokens: 50, totalTokens: 150 },
        }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          usage: { promptTokens: 200, completionTokens: 100, totalTokens: 300 },
        }))
        await integration.stop()

        const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')

        expect(usage!.get('gpt-4')?.totalTokens).toBe(450)
      })

      it('should calculate average latency', async () => {
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4', latencyMs: 100 }))
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4', latencyMs: 200 }))
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4', latencyMs: 300 }))
        await integration.stop()

        const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')

        expect(usage!.get('gpt-4')?.avgLatencyMs).toBe(200)
      })
    })

    describe('hourly_requests', () => {
      it('should aggregate requests by hour', async () => {
        const now = new Date()
        const hourStart = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours())

        await integration.processLogEntry(createLogEntry({ timestamp: now, requestType: 'generate' }))
        await integration.processLogEntry(createLogEntry({ timestamp: now, requestType: 'stream' }))
        await integration.processLogEntry(createLogEntry({ timestamp: now, cached: true }))
        await integration.stop()

        const hourly = await integration.query<Map<string, HourlyRequestData>>('hourly_requests')
        const hourData = hourly!.get(hourStart.toISOString())

        expect(hourData).toBeDefined()
        expect(hourData!.requestCount).toBe(3)
        expect(hourData!.generateCount).toBe(2) // 2 generate (default + explicit)
        expect(hourData!.streamCount).toBe(1)
        expect(hourData!.cachedCount).toBe(1)
      })

      it('should track errors in hourly data', async () => {
        await integration.processLogEntry(createLogEntry({}))
        await integration.processLogEntry(createLogEntry({
          error: { name: 'Error', message: 'Failed' },
        }))
        await integration.stop()

        const hourly = await integration.query<Map<string, HourlyRequestData>>('hourly_requests')
        const values = Array.from(hourly!.values())

        expect(values[0]?.errorCount).toBe(1)
      })
    })

    describe('error_rates', () => {
      it('should calculate error rate by model', async () => {
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          error: { name: 'APIError', message: 'Failed' },
        }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          error: { name: 'TimeoutError', message: 'Timeout' },
        }))
        await integration.stop()

        const errorRates = await integration.query<Map<string, ErrorRateData>>('error_rates')
        const gpt4Errors = errorRates!.get('gpt-4')

        expect(gpt4Errors?.totalRequests).toBe(4)
        expect(gpt4Errors?.errorCount).toBe(2)
        expect(gpt4Errors?.errorRate).toBe(0.5)
        expect(gpt4Errors?.errorsByType['APIError']).toBe(1)
        expect(gpt4Errors?.errorsByType['TimeoutError']).toBe(1)
      })
    })

    describe('latency_percentiles', () => {
      it('should calculate latency percentiles', async () => {
        // Add 100 entries with known latencies
        for (let i = 1; i <= 100; i++) {
          await integration.processLogEntry(createLogEntry({
            modelId: 'gpt-4',
            latencyMs: i * 10, // 10, 20, 30, ... 1000
          }))
        }
        await integration.stop()

        const percentiles = await integration.query<Map<string, LatencyPercentileData>>('latency_percentiles')
        const gpt4 = percentiles!.get('gpt-4')

        expect(gpt4).toBeDefined()
        // Percentiles use floor-based indexing, so values are approximate
        expect(gpt4!.p50).toBeGreaterThanOrEqual(490)
        expect(gpt4!.p50).toBeLessThanOrEqual(520)
        expect(gpt4!.p90).toBeGreaterThanOrEqual(890)
        expect(gpt4!.p90).toBeLessThanOrEqual(910)
        expect(gpt4!.p95).toBeGreaterThanOrEqual(940)
        expect(gpt4!.p95).toBeLessThanOrEqual(960)
        expect(gpt4!.p99).toBeGreaterThanOrEqual(980)
        expect(gpt4!.p99).toBeLessThanOrEqual(1000)
      })
    })

    describe('cache_hit_rates', () => {
      it('should calculate overall cache hit rate', async () => {
        await integration.processLogEntry(createLogEntry({ cached: true }))
        await integration.processLogEntry(createLogEntry({ cached: true }))
        await integration.processLogEntry(createLogEntry({ cached: false }))
        await integration.processLogEntry(createLogEntry({ cached: false }))
        await integration.stop()

        const cacheRates = await integration.query<CacheHitRateData>('cache_hit_rates')

        expect(cacheRates?.totalRequests).toBe(4)
        expect(cacheRates?.cachedRequests).toBe(2)
        expect(cacheRates?.hitRate).toBe(0.5)
      })

      it('should calculate cache hit rate by model', async () => {
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4', cached: true }))
        await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4', cached: false }))
        await integration.processLogEntry(createLogEntry({ modelId: 'claude-3', cached: true }))
        await integration.processLogEntry(createLogEntry({ modelId: 'claude-3', cached: true }))
        await integration.stop()

        const cacheRates = await integration.query<CacheHitRateData>('cache_hit_rates')

        expect(cacheRates?.byModel['gpt-4']?.rate).toBe(0.5)
        expect(cacheRates?.byModel['claude-3']?.rate).toBe(1)
      })
    })

    describe('token_usage', () => {
      it('should aggregate token usage', async () => {
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          usage: { promptTokens: 100, completionTokens: 50, totalTokens: 150 },
        }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'gpt-4',
          usage: { promptTokens: 200, completionTokens: 100, totalTokens: 300 },
        }))
        await integration.processLogEntry(createLogEntry({
          modelId: 'claude-3',
          usage: { promptTokens: 150, completionTokens: 75, totalTokens: 225 },
        }))
        await integration.stop()

        const tokenUsage = await integration.query<TokenUsageData>('token_usage')

        expect(tokenUsage?.totalPromptTokens).toBe(450)
        expect(tokenUsage?.totalCompletionTokens).toBe(225)
        expect(tokenUsage?.totalTokens).toBe(675)
        expect(tokenUsage?.byModel['gpt-4']?.totalTokens).toBe(450)
        expect(tokenUsage?.byModel['claude-3']?.totalTokens).toBe(225)
      })
    })
  })

  describe('Custom Views', () => {
    beforeEach(async () => {
      integration = createAIObservabilityMVs({ enableBuiltinViews: false })
      await integration.start()
    })

    it('should register a custom view', () => {
      const customView: AIAnalyticsView<number> = {
        name: 'total_requests',
        description: 'Total number of AI requests',
        aggregate: (entries, existing = 0) => existing + entries.length,
      }

      integration.registerView(customView)

      const viewNames = integration.getViewNames()
      expect(viewNames).toContain('total_requests')
    })

    it('should process entries through custom view', async () => {
      let aggregateCalled = 0
      const customView: AIAnalyticsView<number> = {
        name: 'counter',
        aggregate: (entries, existing = 0) => {
          aggregateCalled++
          return existing + entries.length
        },
      }

      integration.registerView(customView)

      await integration.processLogEntry(createLogEntry())
      await integration.processLogEntry(createLogEntry())
      await integration.stop()

      const count = await integration.query<number>('counter')
      expect(count).toBe(2)
    })

    it('should unregister a custom view', async () => {
      const customView: AIAnalyticsView<number> = {
        name: 'temp_view',
        aggregate: () => 42,
      }

      integration.registerView(customView)
      expect(integration.getViewNames()).toContain('temp_view')

      integration.unregisterView('temp_view')
      expect(integration.getViewNames()).not.toContain('temp_view')
    })

    it('should support complex aggregations', async () => {
      interface RequestSummary {
        byProvider: Record<string, number>
        byType: Record<string, number>
      }

      const summaryView: AIAnalyticsView<RequestSummary> = {
        name: 'request_summary',
        aggregate: (entries, existing = { byProvider: {}, byType: {} }) => {
          for (const entry of entries) {
            const provider = entry.providerId || 'unknown'
            existing.byProvider[provider] = (existing.byProvider[provider] || 0) + 1

            const type = entry.requestType
            existing.byType[type] = (existing.byType[type] || 0) + 1
          }
          return existing
        },
      }

      integration.registerView(summaryView)

      await integration.processLogEntry(createLogEntry({ providerId: 'openai', requestType: 'generate' }))
      await integration.processLogEntry(createLogEntry({ providerId: 'openai', requestType: 'stream' }))
      await integration.processLogEntry(createLogEntry({ providerId: 'anthropic', requestType: 'generate' }))
      await integration.stop()

      const summary = await integration.query<RequestSummary>('request_summary')

      expect(summary?.byProvider['openai']).toBe(2)
      expect(summary?.byProvider['anthropic']).toBe(1)
      expect(summary?.byType['generate']).toBe(2)
      expect(summary?.byType['stream']).toBe(1)
    })
  })

  describe('State Management', () => {
    it('should return integration state', async () => {
      integration = createAIObservabilityMVs({ enableBuiltinViews: true })
      await integration.start()

      await integration.processLogEntry(createLogEntry())
      await integration.processLogEntry(createLogEntry())
      await integration.stop()

      const state = integration.getState()

      expect(state.isRunning).toBe(false)
      expect(state.entriesProcessed).toBe(2)
      expect(state.viewCount).toBe(6) // 6 builtin views
      expect(state.viewNames).toContain('model_usage')
    })

    it('should clear all data', async () => {
      integration = createAIObservabilityMVs()
      await integration.start()

      await integration.processLogEntry(createLogEntry())
      await integration.processLogEntry(createLogEntry())
      await integration.stop()

      expect(integration.getState().entriesProcessed).toBe(2)

      integration.clear()

      expect(integration.getState().entriesProcessed).toBe(0)
      const logs = await integration.queryLogs()
      expect(logs).toHaveLength(0)
    })

    it('should refresh all views', async () => {
      integration = createAIObservabilityMVs()
      await integration.start()

      await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
      await integration.processLogEntry(createLogEntry({ modelId: 'gpt-4' }))
      await integration.stop()

      // Refresh forces recalculation from raw logs
      await integration.refresh()

      const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')
      expect(usage!.get('gpt-4')?.requestCount).toBe(2)
    })
  })

  describe('Retention', () => {
    it('should remove old logs based on retention policy', async () => {
      integration = createAIObservabilityMVs({
        retentionMs: 60000, // 1 minute retention
      })
      await integration.start()

      // Add old and new entries
      await integration.processLogEntry(createLogEntry({
        timestamp: new Date(Date.now() - 120000), // 2 minutes ago
      }))
      await integration.processLogEntry(createLogEntry({
        timestamp: new Date(), // now
      }))
      await integration.stop()

      const removed = integration.applyRetention()

      expect(removed).toBe(1)

      const logs = await integration.queryLogs()
      expect(logs).toHaveLength(1)
    })
  })

  describe('Type-safe Query Helper', () => {
    it('should use queryBuiltinView for type-safe queries', async () => {
      integration = createAIObservabilityMVs()
      await integration.start()

      await integration.processLogEntry(createLogEntry({
        modelId: 'gpt-4',
        usage: { totalTokens: 100 },
      }))
      await integration.stop()

      const tokenUsage = await queryBuiltinView(integration, 'token_usage')

      expect(tokenUsage?.totalTokens).toBe(100)
    })
  })
})

describe('Integration with Middleware', () => {
  it('should work as onLog callback', async () => {
    const integration = createAIObservabilityMVs()
    await integration.start()

    // Simulate what the middleware would do
    const mockEntry: LogEntry = {
      $id: 'test-1',
      $type: 'AILog',
      name: 'log-test',
      timestamp: new Date(),
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      latencyMs: 150,
      cached: false,
      finishReason: 'stop',
      usage: { promptTokens: 50, completionTokens: 25, totalTokens: 75 },
    }

    // This is how it would be called from middleware
    await integration.processLogEntry(mockEntry)
    await integration.stop()

    const usage = await integration.query<Map<string, ModelUsageData>>('model_usage')
    expect(usage!.get('gpt-4')?.totalTokens).toBe(75)
    expect(usage!.get('gpt-4')?.avgLatencyMs).toBe(150)
  })
})

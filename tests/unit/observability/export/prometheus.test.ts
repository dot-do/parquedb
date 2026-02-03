/**
 * Prometheus Export Tests
 *
 * Tests for Prometheus format export of observability data.
 */

import { describe, it, expect } from 'vitest'
import {
  exportAIUsageToPrometheus,
  exportAIStatsToPrometheus,
  exportCompactionToPrometheus,
  combinePrometheusExports,
  AI_PROMETHEUS_METRICS,
} from '../../../../src/observability/export/prometheus'
import type { AIUsageAggregate } from '../../../../src/observability/ai/types'
import type { AIRequestsStats } from '../../../../src/observability/ai/AIRequestsMV'
import type { CompactionMetrics } from '../../../../src/observability/compaction/types'

describe('Prometheus Export', () => {
  describe('AI_PROMETHEUS_METRICS', () => {
    it('should define all expected metrics', () => {
      const metricNames = AI_PROMETHEUS_METRICS.map(m => m.name)

      expect(metricNames).toContain('parquedb_ai_requests_total')
      expect(metricNames).toContain('parquedb_ai_requests_cached_total')
      expect(metricNames).toContain('parquedb_ai_tokens_total')
      expect(metricNames).toContain('parquedb_ai_cost_dollars_total')
      expect(metricNames).toContain('parquedb_ai_request_duration_milliseconds')
      expect(metricNames).toContain('parquedb_ai_error_rate')
      expect(metricNames).toContain('parquedb_ai_cache_hit_ratio')
    })

    it('should have correct metric types', () => {
      const requestsMetric = AI_PROMETHEUS_METRICS.find(m => m.name === 'parquedb_ai_requests_total')
      expect(requestsMetric?.type).toBe('counter')

      const errorRateMetric = AI_PROMETHEUS_METRICS.find(m => m.name === 'parquedb_ai_error_rate')
      expect(errorRateMetric?.type).toBe('gauge')
    })
  })

  describe('exportAIUsageToPrometheus', () => {
    const createMockAggregate = (overrides: Partial<AIUsageAggregate> = {}): AIUsageAggregate => ({
      $id: 'test-1',
      $type: 'AIUsage',
      name: 'gpt-4/openai (2026-02-03)',
      modelId: 'gpt-4',
      providerId: 'openai',
      dateKey: '2026-02-03',
      granularity: 'day',
      requestCount: 100,
      successCount: 95,
      errorCount: 5,
      cachedCount: 10,
      generateCount: 80,
      streamCount: 20,
      totalPromptTokens: 10000,
      totalCompletionTokens: 5000,
      totalTokens: 15000,
      avgTokensPerRequest: 150,
      totalLatencyMs: 50000,
      avgLatencyMs: 500,
      minLatencyMs: 100,
      maxLatencyMs: 2000,
      p50LatencyMs: 450,
      p90LatencyMs: 800,
      p95LatencyMs: 1000,
      p99LatencyMs: 1500,
      estimatedInputCost: 0.30,
      estimatedOutputCost: 0.30,
      estimatedTotalCost: 0.60,
      createdAt: new Date('2026-02-03T00:00:00Z'),
      updatedAt: new Date('2026-02-03T12:00:00Z'),
      version: 1,
      ...overrides,
    })

    it('should export aggregates in Prometheus format', () => {
      const aggregates = [createMockAggregate()]
      const output = exportAIUsageToPrometheus(aggregates)

      // Check HELP and TYPE lines
      expect(output).toContain('# HELP parquedb_ai_requests_total')
      expect(output).toContain('# TYPE parquedb_ai_requests_total counter')

      // Check metric values with labels
      expect(output).toContain('parquedb_ai_requests_total{model="gpt-4",provider="openai",status="success"} 95')
      expect(output).toContain('parquedb_ai_requests_total{model="gpt-4",provider="openai",status="error"} 5')
    })

    it('should export token metrics', () => {
      const aggregates = [createMockAggregate()]
      const output = exportAIUsageToPrometheus(aggregates)

      expect(output).toContain('# HELP parquedb_ai_tokens_total')
      expect(output).toContain('parquedb_ai_tokens_total{model="gpt-4",provider="openai",type="prompt"} 10000')
      expect(output).toContain('parquedb_ai_tokens_total{model="gpt-4",provider="openai",type="completion"} 5000')
    })

    it('should export cost metrics', () => {
      const aggregates = [createMockAggregate()]
      const output = exportAIUsageToPrometheus(aggregates)

      expect(output).toContain('# HELP parquedb_ai_cost_dollars_total')
      expect(output).toContain('parquedb_ai_cost_dollars_total{model="gpt-4",provider="openai"} 0.6')
    })

    it('should export latency percentiles', () => {
      const aggregates = [createMockAggregate()]
      const output = exportAIUsageToPrometheus(aggregates)

      expect(output).toContain('parquedb_ai_request_duration_milliseconds{model="gpt-4",provider="openai",quantile="0.5"} 450')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{model="gpt-4",provider="openai",quantile="0.9"} 800')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{model="gpt-4",provider="openai",quantile="0.95"} 1000')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{model="gpt-4",provider="openai",quantile="0.99"} 1500')
    })

    it('should export error rate', () => {
      const aggregates = [createMockAggregate()]
      const output = exportAIUsageToPrometheus(aggregates)

      expect(output).toContain('# HELP parquedb_ai_error_rate')
      expect(output).toContain('parquedb_ai_error_rate{model="gpt-4",provider="openai"} 0.05')
    })

    it('should aggregate multiple aggregates for same model/provider', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-02-01', requestCount: 50, successCount: 48, errorCount: 2 }),
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 50, successCount: 47, errorCount: 3 }),
      ]
      const output = exportAIUsageToPrometheus(aggregates)

      // Should sum to 100 total
      expect(output).toContain('parquedb_ai_requests_total{model="gpt-4",provider="openai",status="success"} 95')
      expect(output).toContain('parquedb_ai_requests_total{model="gpt-4",provider="openai",status="error"} 5')
    })

    it('should handle multiple models/providers', () => {
      const aggregates = [
        createMockAggregate({ modelId: 'gpt-4', providerId: 'openai' }),
        createMockAggregate({ modelId: 'claude-3-opus', providerId: 'anthropic', requestCount: 50 }),
      ]
      const output = exportAIUsageToPrometheus(aggregates)

      expect(output).toContain('model="gpt-4",provider="openai"')
      expect(output).toContain('model="claude-3-opus",provider="anthropic"')
    })

    it('should include timestamp when provided', () => {
      const aggregates = [createMockAggregate()]
      const timestamp = 1706918400000
      const output = exportAIUsageToPrometheus(aggregates, { timestamp })

      expect(output).toContain(`${timestamp}`)
    })
  })

  describe('exportAIStatsToPrometheus', () => {
    const mockStats: AIRequestsStats = {
      totalRequests: 1000,
      successCount: 950,
      errorCount: 50,
      errorRate: 0.05,
      cacheHits: 100,
      cacheHitRatio: 0.1,
      tokens: {
        totalPromptTokens: 100000,
        totalCompletionTokens: 50000,
        totalTokens: 150000,
        avgPromptTokens: 100,
        avgCompletionTokens: 50,
        avgTotalTokens: 150,
      },
      cost: {
        totalCost: 10.00,
        avgCost: 0.01,
        cacheSavings: 1.00,
      },
      latency: {
        min: 50,
        max: 5000,
        avg: 500,
        p50: 400,
        p95: 1500,
        p99: 3000,
      },
      byModel: {
        'gpt-4': { count: 500, cost: 5.00, avgLatency: 600 },
        'gpt-3.5-turbo': { count: 500, cost: 5.00, avgLatency: 400 },
      },
      byProvider: {
        openai: { count: 1000, cost: 10.00, avgLatency: 500 },
      },
      byRequestType: {
        generate: 800,
        stream: 200,
      },
      timeRange: {
        from: new Date('2026-02-01'),
        to: new Date('2026-02-03'),
      },
    }

    it('should export stats in Prometheus format', () => {
      const output = exportAIStatsToPrometheus(mockStats)

      expect(output).toContain('parquedb_ai_requests_total{status="total"} 1000')
      expect(output).toContain('parquedb_ai_requests_total{status="success"} 950')
      expect(output).toContain('parquedb_ai_requests_total{status="error"} 50')
    })

    it('should export per-model stats', () => {
      const output = exportAIStatsToPrometheus(mockStats)

      expect(output).toContain('parquedb_ai_requests_total{model="gpt-4",status="all"} 500')
      expect(output).toContain('parquedb_ai_cost_dollars_total{model="gpt-4"} 5')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{model="gpt-4",quantile="avg"} 600')
    })

    it('should export latency percentiles', () => {
      const output = exportAIStatsToPrometheus(mockStats)

      expect(output).toContain('parquedb_ai_request_duration_milliseconds{quantile="0.5"} 400')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{quantile="0.95"} 1500')
      expect(output).toContain('parquedb_ai_request_duration_milliseconds{quantile="0.99"} 3000')
    })

    it('should export error rate and cache hit ratio', () => {
      const output = exportAIStatsToPrometheus(mockStats)

      // The output format may vary - check for the metric name and value
      expect(output).toContain('parquedb_ai_error_rate')
      expect(output).toContain('0.05')
      expect(output).toContain('parquedb_ai_cache_hit_ratio')
      expect(output).toContain('0.1')
    })
  })

  describe('exportCompactionToPrometheus', () => {
    const createMockCompactionMetrics = (namespace: string): CompactionMetrics => ({
      namespace,
      timestamp: Date.now(),
      windows_pending: 5,
      windows_processing: 2,
      windows_dispatched: 1,
      files_pending: 50,
      oldest_window_age_ms: 3600000,
      known_writers: 3,
      active_writers: 2,
      bytes_pending: 1024000,
      windows_stuck: 0,
    })

    it('should export compaction metrics in Prometheus format', () => {
      const metrics = new Map([
        ['posts', createMockCompactionMetrics('posts')],
        ['users', createMockCompactionMetrics('users')],
      ])
      const output = exportCompactionToPrometheus(metrics)

      expect(output).toContain('# HELP parquedb_compaction_windows_pending')
      expect(output).toContain('# TYPE parquedb_compaction_windows_pending gauge')
      expect(output).toContain('parquedb_compaction_windows_pending{namespace="posts"} 5')
      expect(output).toContain('parquedb_compaction_windows_pending{namespace="users"} 5')
    })

    it('should export all compaction metric types', () => {
      const metrics = new Map([['posts', createMockCompactionMetrics('posts')]])
      const output = exportCompactionToPrometheus(metrics)

      expect(output).toContain('parquedb_compaction_windows_processing{namespace="posts"} 2')
      expect(output).toContain('parquedb_compaction_windows_dispatched{namespace="posts"} 1')
      expect(output).toContain('parquedb_compaction_files_pending{namespace="posts"} 50')
      expect(output).toContain('parquedb_compaction_bytes_pending{namespace="posts"} 1024000')
      expect(output).toContain('parquedb_compaction_oldest_window_age_seconds{namespace="posts"} 3600')
      expect(output).toContain('parquedb_compaction_known_writers{namespace="posts"} 3')
      expect(output).toContain('parquedb_compaction_active_writers{namespace="posts"} 2')
      expect(output).toContain('parquedb_compaction_windows_stuck{namespace="posts"} 0')
    })

    it('should filter by namespaces when specified', () => {
      const metrics = new Map([
        ['posts', createMockCompactionMetrics('posts')],
        ['users', createMockCompactionMetrics('users')],
      ])
      const output = exportCompactionToPrometheus(metrics, { namespaces: ['posts'] })

      expect(output).toContain('namespace="posts"')
      expect(output).not.toContain('namespace="users"')
    })

    it('should accept array of metrics', () => {
      const metrics = [
        createMockCompactionMetrics('posts'),
        createMockCompactionMetrics('users'),
      ]
      const output = exportCompactionToPrometheus(metrics)

      expect(output).toContain('namespace="posts"')
      expect(output).toContain('namespace="users"')
    })
  })

  describe('combinePrometheusExports', () => {
    it('should combine multiple exports', () => {
      const export1 = '# HELP metric1\nmetric1 1'
      const export2 = '# HELP metric2\nmetric2 2'
      const combined = combinePrometheusExports(export1, export2)

      expect(combined).toContain('metric1 1')
      expect(combined).toContain('metric2 2')
      expect(combined).toContain('\n\n') // Double newline separator
    })

    it('should filter out empty exports', () => {
      const export1 = '# HELP metric1\nmetric1 1'
      const combined = combinePrometheusExports(export1, '', '# HELP metric2\nmetric2 2')

      expect(combined).not.toContain('\n\n\n') // No triple newlines
    })
  })
})

/**
 * Grafana Export Tests
 *
 * Tests for Grafana-compatible data source endpoints.
 */

import { describe, it, expect } from 'vitest'
import {
  AI_GRAFANA_METRICS,
  COMPACTION_GRAFANA_METRICS,
  parseGrafanaTimeRange,
  handleGrafanaSearch,
  handleAIUsageQuery,
  handleCompactionQuery,
  handleAnnotationsQuery,
  handleTagKeys,
  handleTagValues,
  handleVariableQuery,
} from '../../../../src/observability/export/grafana'
import type { AIUsageAggregate } from '../../../../src/observability/ai/types'
import type { CompactionMetrics } from '../../../../src/observability/compaction/types'
import type { GrafanaQueryRequest } from '../../../../src/observability/export/types'

describe('Grafana Export', () => {
  describe('Metric Constants', () => {
    it('should define AI metrics', () => {
      expect(AI_GRAFANA_METRICS).toContain('ai.requests.total')
      expect(AI_GRAFANA_METRICS).toContain('ai.tokens.total')
      expect(AI_GRAFANA_METRICS).toContain('ai.cost.total')
      expect(AI_GRAFANA_METRICS).toContain('ai.latency.avg')
      expect(AI_GRAFANA_METRICS).toContain('ai.error_rate')
    })

    it('should define compaction metrics', () => {
      expect(COMPACTION_GRAFANA_METRICS).toContain('compaction.windows.pending')
      expect(COMPACTION_GRAFANA_METRICS).toContain('compaction.files.pending')
      expect(COMPACTION_GRAFANA_METRICS).toContain('compaction.bytes.pending')
    })
  })

  describe('parseGrafanaTimeRange', () => {
    it('should parse ISO date strings', () => {
      const range = parseGrafanaTimeRange({
        from: '2026-02-01T00:00:00Z',
        to: '2026-02-03T00:00:00Z',
      })

      expect(range.from.toISOString()).toBe('2026-02-01T00:00:00.000Z')
      expect(range.to.toISOString()).toBe('2026-02-03T00:00:00.000Z')
    })
  })

  describe('handleGrafanaSearch', () => {
    it('should return all available metrics', () => {
      const metrics = handleGrafanaSearch()

      expect(metrics).toContain('ai.requests.total')
      expect(metrics).toContain('compaction.windows.pending')
      expect(metrics.length).toBe(AI_GRAFANA_METRICS.length + COMPACTION_GRAFANA_METRICS.length)
    })
  })

  describe('handleAIUsageQuery', () => {
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
      estimatedInputCost: 0.30,
      estimatedOutputCost: 0.30,
      estimatedTotalCost: 0.60,
      createdAt: new Date('2026-02-03T00:00:00Z'),
      updatedAt: new Date('2026-02-03T12:00:00Z'),
      version: 1,
      ...overrides,
    })

    const createMockRequest = (target: string, format?: 'time_series' | 'table'): GrafanaQueryRequest => ({
      targets: [{ refId: 'A', target, format }],
      range: {
        from: '2026-02-01T00:00:00Z',
        to: '2026-02-05T00:00:00Z',
        raw: { from: '2026-02-01T00:00:00Z', to: '2026-02-05T00:00:00Z' },
      },
      intervalMs: 60000,
      maxDataPoints: 100,
    })

    it('should return time series response by default', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-02-01', requestCount: 50 }),
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 75 }),
        createMockAggregate({ dateKey: '2026-02-03', requestCount: 100 }),
      ]
      const request = createMockRequest('ai.requests.total')
      const responses = handleAIUsageQuery(request, aggregates)

      expect(responses.length).toBe(1)
      expect(responses[0]).toHaveProperty('target')
      expect(responses[0]).toHaveProperty('datapoints')

      const response = responses[0] as { target: string; datapoints: Array<[number, number]> }
      expect(response.datapoints.length).toBe(3)
      // Datapoints are [value, timestamp]
      expect(response.datapoints[0][0]).toBe(50)
      expect(response.datapoints[2][0]).toBe(100)
    })

    it('should return table response when requested', () => {
      const aggregates = [createMockAggregate()]
      const request = createMockRequest('ai.requests.total', 'table')
      const responses = handleAIUsageQuery(request, aggregates)

      expect(responses.length).toBe(1)
      expect(responses[0]).toHaveProperty('columns')
      expect(responses[0]).toHaveProperty('rows')
      expect(responses[0]).toHaveProperty('type', 'table')
    })

    it('should filter by model when specified in target', () => {
      const aggregates = [
        createMockAggregate({ modelId: 'gpt-4', requestCount: 50 }),
        createMockAggregate({ modelId: 'claude-3-opus', requestCount: 100 }),
      ]
      const request = createMockRequest('ai.requests.total{model=gpt-4}')
      const responses = handleAIUsageQuery(request, aggregates)

      expect(responses.length).toBe(1)
      const response = responses[0] as { datapoints: Array<[number, number]> }
      expect(response.datapoints.length).toBe(1)
      expect(response.datapoints[0][0]).toBe(50)
    })

    it('should filter by time range', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-01-15', requestCount: 25 }), // Before range
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 50 }), // In range
        createMockAggregate({ dateKey: '2026-02-10', requestCount: 75 }), // After range
      ]
      const request = createMockRequest('ai.requests.total')
      const responses = handleAIUsageQuery(request, aggregates)

      const response = responses[0] as { datapoints: Array<[number, number]> }
      expect(response.datapoints.length).toBe(1)
      expect(response.datapoints[0][0]).toBe(50)
    })

    it('should handle different metrics', () => {
      const aggregates = [createMockAggregate({
        totalTokens: 15000,
        estimatedTotalCost: 0.60,
        avgLatencyMs: 500,
      })]

      const tokensRequest = createMockRequest('ai.tokens.total')
      const tokensResponse = handleAIUsageQuery(tokensRequest, aggregates)
      expect((tokensResponse[0] as { datapoints: Array<[number, number]> }).datapoints[0][0]).toBe(15000)

      const costRequest = createMockRequest('ai.cost.total')
      const costResponse = handleAIUsageQuery(costRequest, aggregates)
      expect((costResponse[0] as { datapoints: Array<[number, number]> }).datapoints[0][0]).toBe(0.60)

      const latencyRequest = createMockRequest('ai.latency.avg')
      const latencyResponse = handleAIUsageQuery(latencyRequest, aggregates)
      expect((latencyResponse[0] as { datapoints: Array<[number, number]> }).datapoints[0][0]).toBe(500)
    })
  })

  describe('handleCompactionQuery', () => {
    const createMockCompactionMetrics = (namespace: string, pending: number): CompactionMetrics => ({
      namespace,
      timestamp: Date.now(),
      windows_pending: pending,
      windows_processing: 2,
      windows_dispatched: 1,
      files_pending: 50,
      oldest_window_age_ms: 3600000,
      known_writers: 3,
      active_writers: 2,
      bytes_pending: 1024000,
      windows_stuck: 0,
    })

    it('should return compaction metrics', () => {
      const latestMetrics = new Map([
        ['posts', createMockCompactionMetrics('posts', 5)],
        ['users', createMockCompactionMetrics('users', 3)],
      ])
      const request: GrafanaQueryRequest = {
        targets: [{ refId: 'A', target: 'compaction.windows.pending' }],
        range: {
          from: '2026-02-01T00:00:00Z',
          to: '2026-02-03T00:00:00Z',
          raw: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
        },
        intervalMs: 60000,
        maxDataPoints: 100,
      }

      const responses = handleCompactionQuery(request, latestMetrics)
      expect(responses.length).toBe(1)

      const response = responses[0] as { datapoints: Array<[number, number]> }
      // Should have data points for both namespaces
      expect(response.datapoints.length).toBeGreaterThan(0)
    })

    it('should filter by namespace', () => {
      const latestMetrics = new Map([
        ['posts', createMockCompactionMetrics('posts', 5)],
        ['users', createMockCompactionMetrics('users', 3)],
      ])
      const request: GrafanaQueryRequest = {
        targets: [{ refId: 'A', target: 'compaction.windows.pending{namespace=posts}' }],
        range: {
          from: '2026-02-01T00:00:00Z',
          to: '2026-02-03T00:00:00Z',
          raw: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
        },
        intervalMs: 60000,
        maxDataPoints: 100,
      }

      const responses = handleCompactionQuery(request, latestMetrics)
      expect(responses.length).toBe(1)

      // Should only have one datapoint (posts, not users)
      const response = responses[0] as { target: string; datapoints: Array<[number, number]> }
      expect(response.datapoints.length).toBe(1)
      expect(response.datapoints[0][0]).toBe(5) // posts has 5 windows pending
    })
  })

  describe('handleAnnotationsQuery', () => {
    it('should return annotations for alerts in time range', () => {
      const alerts = [
        { timestamp: new Date('2026-02-02T10:00:00Z'), severity: 'critical', title: 'High error rate', message: 'Error rate above 10%', namespace: 'posts' },
        { timestamp: new Date('2026-02-02T14:00:00Z'), severity: 'warning', title: 'Slow responses', message: 'P95 latency above 2s' },
        { timestamp: new Date('2026-01-15T10:00:00Z'), severity: 'critical', title: 'Old alert', message: 'Outside range' },
      ]
      const request = {
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
        annotation: { name: 'Alerts', datasource: 'test', enable: true },
      }

      const annotations = handleAnnotationsQuery(request, alerts)
      expect(annotations.length).toBe(2) // Only alerts in range
      expect(annotations[0].title).toBe('High error rate')
      expect(annotations[0].tags).toContain('critical')
      expect(annotations[0].tags).toContain('posts')
    })

    it('should filter by query', () => {
      const alerts = [
        { timestamp: new Date('2026-02-02T10:00:00Z'), severity: 'critical', title: 'Critical alert' },
        { timestamp: new Date('2026-02-02T11:00:00Z'), severity: 'warning', title: 'Warning alert' },
      ]
      const request = {
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
        annotation: { name: 'Alerts', datasource: 'test', enable: true, query: 'critical' },
      }

      const annotations = handleAnnotationsQuery(request, alerts)
      expect(annotations.length).toBe(1)
      expect(annotations[0].title).toBe('Critical alert')
    })

    it('should return empty array when no alerts', () => {
      const request = {
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
        annotation: { name: 'Alerts', datasource: 'test', enable: true },
      }

      const annotations = handleAnnotationsQuery(request, undefined)
      expect(annotations).toEqual([])
    })
  })

  describe('handleTagKeys', () => {
    it('should return available tag keys', () => {
      const keys = handleTagKeys()

      expect(keys).toContainEqual({ type: 'string', text: 'model' })
      expect(keys).toContainEqual({ type: 'string', text: 'provider' })
      expect(keys).toContainEqual({ type: 'string', text: 'namespace' })
      expect(keys).toContainEqual({ type: 'string', text: 'status' })
    })
  })

  describe('handleTagValues', () => {
    const aggregates: AIUsageAggregate[] = [
      {
        $id: '1', $type: 'AIUsage', name: '', modelId: 'gpt-4', providerId: 'openai',
        dateKey: '2026-02-03', granularity: 'day', requestCount: 100, successCount: 95,
        errorCount: 5, cachedCount: 10, generateCount: 80, streamCount: 20,
        totalPromptTokens: 10000, totalCompletionTokens: 5000, totalTokens: 15000,
        avgTokensPerRequest: 150, totalLatencyMs: 50000, avgLatencyMs: 500,
        minLatencyMs: 100, maxLatencyMs: 2000, estimatedInputCost: 0.3,
        estimatedOutputCost: 0.3, estimatedTotalCost: 0.6,
        createdAt: new Date(), updatedAt: new Date(), version: 1,
      },
      {
        $id: '2', $type: 'AIUsage', name: '', modelId: 'claude-3-opus', providerId: 'anthropic',
        dateKey: '2026-02-03', granularity: 'day', requestCount: 50, successCount: 48,
        errorCount: 2, cachedCount: 5, generateCount: 40, streamCount: 10,
        totalPromptTokens: 5000, totalCompletionTokens: 2500, totalTokens: 7500,
        avgTokensPerRequest: 150, totalLatencyMs: 25000, avgLatencyMs: 500,
        minLatencyMs: 100, maxLatencyMs: 1500, estimatedInputCost: 0.15,
        estimatedOutputCost: 0.15, estimatedTotalCost: 0.3,
        createdAt: new Date(), updatedAt: new Date(), version: 1,
      },
    ]

    const compactionMetrics = new Map([
      ['posts', { namespace: 'posts', timestamp: Date.now(), windows_pending: 5, windows_processing: 2, windows_dispatched: 1, files_pending: 50, oldest_window_age_ms: 3600000, known_writers: 3, active_writers: 2, bytes_pending: 1024000, windows_stuck: 0 }],
      ['users', { namespace: 'users', timestamp: Date.now(), windows_pending: 3, windows_processing: 1, windows_dispatched: 0, files_pending: 30, oldest_window_age_ms: 1800000, known_writers: 2, active_writers: 1, bytes_pending: 512000, windows_stuck: 0 }],
    ])

    it('should return model values', () => {
      const values = handleTagValues('model', aggregates, compactionMetrics)

      expect(values).toContainEqual({ text: 'gpt-4' })
      expect(values).toContainEqual({ text: 'claude-3-opus' })
    })

    it('should return provider values', () => {
      const values = handleTagValues('provider', aggregates, compactionMetrics)

      expect(values).toContainEqual({ text: 'openai' })
      expect(values).toContainEqual({ text: 'anthropic' })
    })

    it('should return namespace values', () => {
      const values = handleTagValues('namespace', aggregates, compactionMetrics)

      expect(values).toContainEqual({ text: 'posts' })
      expect(values).toContainEqual({ text: 'users' })
    })

    it('should return status values', () => {
      const values = handleTagValues('status', aggregates, compactionMetrics)

      expect(values).toContainEqual({ text: 'success' })
      expect(values).toContainEqual({ text: 'error' })
      expect(values).toContainEqual({ text: 'cached' })
    })
  })

  describe('handleVariableQuery', () => {
    const aggregates: AIUsageAggregate[] = [
      {
        $id: '1', $type: 'AIUsage', name: '', modelId: 'gpt-4', providerId: 'openai',
        dateKey: '2026-02-03', granularity: 'day', requestCount: 100, successCount: 95,
        errorCount: 5, cachedCount: 10, generateCount: 80, streamCount: 20,
        totalPromptTokens: 10000, totalCompletionTokens: 5000, totalTokens: 15000,
        avgTokensPerRequest: 150, totalLatencyMs: 50000, avgLatencyMs: 500,
        minLatencyMs: 100, maxLatencyMs: 2000, estimatedInputCost: 0.3,
        estimatedOutputCost: 0.3, estimatedTotalCost: 0.6,
        createdAt: new Date(), updatedAt: new Date(), version: 1,
      },
    ]

    const compactionMetrics = new Map([
      ['posts', { namespace: 'posts', timestamp: Date.now(), windows_pending: 5, windows_processing: 2, windows_dispatched: 1, files_pending: 50, oldest_window_age_ms: 3600000, known_writers: 3, active_writers: 2, bytes_pending: 1024000, windows_stuck: 0 }],
    ])

    it('should return models for models target', () => {
      const request = {
        payload: { target: 'models' },
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
      }
      const values = handleVariableQuery(request, aggregates, compactionMetrics)

      expect(values).toContainEqual({ __text: 'gpt-4', __value: 'gpt-4' })
    })

    it('should return providers for providers target', () => {
      const request = {
        payload: { target: 'providers' },
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
      }
      const values = handleVariableQuery(request, aggregates, compactionMetrics)

      expect(values).toContainEqual({ __text: 'openai', __value: 'openai' })
    })

    it('should return namespaces for namespaces target', () => {
      const request = {
        payload: { target: 'namespaces' },
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
      }
      const values = handleVariableQuery(request, aggregates, compactionMetrics)

      expect(values).toContainEqual({ __text: 'posts', __value: 'posts' })
    })

    it('should return metrics for metrics target', () => {
      const request = {
        payload: { target: 'metrics' },
        range: { from: '2026-02-01T00:00:00Z', to: '2026-02-03T00:00:00Z' },
      }
      const values = handleVariableQuery(request, aggregates, compactionMetrics)

      expect(values.length).toBe(AI_GRAFANA_METRICS.length + COMPACTION_GRAFANA_METRICS.length)
    })
  })
})

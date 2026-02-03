/**
 * JSON/CSV Export Tests
 *
 * Tests for JSON and CSV export of observability data.
 */

import { describe, it, expect } from 'vitest'
import {
  exportAIUsageToJSON,
  exportCompactionToJSON,
  exportAIRequestsToJSON,
  exportAIUsageToCSV,
  exportAIRequestsToCSV,
  exportCompactionToCSV,
} from '../../../../src/observability/export/json-csv'
import type { AIUsageAggregate } from '../../../../src/observability/ai/types'
import type { AIRequestRecord } from '../../../../src/observability/ai/AIRequestsMV'
import type { CompactionMetrics } from '../../../../src/observability/compaction/types'

describe('JSON Export', () => {
  describe('exportAIUsageToJSON', () => {
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

    it('should export aggregates to JSON format', () => {
      const aggregates = [createMockAggregate()]
      const json = exportAIUsageToJSON(aggregates)

      expect(json.version).toBeDefined()
      expect(json.timestamp).toBeDefined()
      expect(json.exportedAt).toBeDefined()
      expect(json.namespaces['gpt-4/openai']).toBeDefined()
    })

    it('should include latest metrics', () => {
      const aggregates = [createMockAggregate()]
      const json = exportAIUsageToJSON(aggregates)

      const ns = json.namespaces['gpt-4/openai']
      expect(ns?.latest?.requestCount).toBe(100)
      expect(ns?.latest?.successCount).toBe(95)
      expect(ns?.latest?.errorCount).toBe(5)
      expect(ns?.latest?.totalTokens).toBe(15000)
      expect(ns?.latest?.estimatedTotalCost).toBe(0.60)
    })

    it('should include time series data', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-02-01', requestCount: 50 }),
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 75 }),
        createMockAggregate({ dateKey: '2026-02-03', requestCount: 100 }),
      ]
      const json = exportAIUsageToJSON(aggregates)

      const ns = json.namespaces['gpt-4/openai']
      expect(ns?.timeSeries?.requestCount?.dataPoints.length).toBe(3)
    })

    it('should filter by time range', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-01-15', requestCount: 25 }),
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 50 }),
        createMockAggregate({ dateKey: '2026-02-10', requestCount: 75 }),
      ]
      const json = exportAIUsageToJSON(aggregates, undefined, {
        timeRange: {
          from: new Date('2026-02-01'),
          to: new Date('2026-02-05'),
        },
      })

      const ns = json.namespaces['gpt-4/openai']
      expect(ns?.timeSeries?.requestCount?.dataPoints.length).toBe(1)
    })

    it('should filter by namespaces', () => {
      const aggregates = [
        createMockAggregate({ modelId: 'gpt-4', providerId: 'openai' }),
        createMockAggregate({ modelId: 'claude-3-opus', providerId: 'anthropic' }),
      ]
      const json = exportAIUsageToJSON(aggregates, undefined, {
        namespaces: ['gpt-4/openai'],
      })

      expect(json.namespaces['gpt-4/openai']).toBeDefined()
      expect(json.namespaces['claude-3-opus/anthropic']).toBeUndefined()
    })

    it('should include metadata when requested', () => {
      const aggregates = [createMockAggregate()]
      const json = exportAIUsageToJSON(aggregates, undefined, { includeMetadata: true })

      const ns = json.namespaces['gpt-4/openai']
      expect(ns?.metadata?.modelId).toBe('gpt-4')
      expect(ns?.metadata?.providerId).toBe('openai')
      expect(ns?.metadata?.granularity).toBe('day')
    })

    it('should respect maxDataPoints', () => {
      const aggregates = Array.from({ length: 100 }, (_, i) =>
        createMockAggregate({ dateKey: `2026-02-${String(i % 28 + 1).padStart(2, '0')}` })
      )
      const json = exportAIUsageToJSON(aggregates, undefined, { maxDataPoints: 10 })

      const ns = json.namespaces['gpt-4/openai']
      expect(ns?.timeSeries?.requestCount?.dataPoints.length).toBeLessThanOrEqual(10)
    })
  })

  describe('exportCompactionToJSON', () => {
    const createMockMetrics = (namespace: string): CompactionMetrics => ({
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

    it('should export compaction metrics to JSON', () => {
      const metrics = new Map([
        ['posts', createMockMetrics('posts')],
        ['users', createMockMetrics('users')],
      ])
      const json = exportCompactionToJSON(metrics)

      expect(json.version).toBeDefined()
      expect(json.namespaces['posts']).toBeDefined()
      expect(json.namespaces['users']).toBeDefined()
    })

    it('should include latest metrics', () => {
      const metrics = new Map([['posts', createMockMetrics('posts')]])
      const json = exportCompactionToJSON(metrics)

      const ns = json.namespaces['posts']
      expect(ns?.latest?.windows_pending).toBe(5)
      expect(ns?.latest?.files_pending).toBe(50)
      expect(ns?.latest?.bytes_pending).toBe(1024000)
    })

    it('should filter by namespaces', () => {
      const metrics = new Map([
        ['posts', createMockMetrics('posts')],
        ['users', createMockMetrics('users')],
      ])
      const json = exportCompactionToJSON(metrics, undefined, { namespaces: ['posts'] })

      expect(json.namespaces['posts']).toBeDefined()
      expect(json.namespaces['users']).toBeUndefined()
    })
  })

  describe('exportAIRequestsToJSON', () => {
    const createMockRequest = (overrides: Partial<AIRequestRecord> = {}): AIRequestRecord => ({
      $id: 'ai_requests/req-1',
      $type: 'AIRequest',
      name: 'req-1',
      requestId: 'req-1',
      timestamp: new Date('2026-02-03T10:00:00Z'),
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      status: 'success',
      latencyMs: 500,
      promptTokens: 100,
      completionTokens: 50,
      totalTokens: 150,
      estimatedCost: 0.01,
      cached: false,
      createdAt: new Date('2026-02-03T10:00:00Z'),
      ...overrides,
    })

    it('should export requests to JSON', () => {
      const requests = [createMockRequest()]
      const json = exportAIRequestsToJSON(requests)

      expect(json.version).toBeDefined()
      expect(json.count).toBe(1)
      expect(json.requests.length).toBe(1)
      expect(json.requests[0].requestId).toBe('req-1')
    })

    it('should filter by time range', () => {
      const requests = [
        createMockRequest({ requestId: 'req-1', timestamp: new Date('2026-01-15T10:00:00Z') }),
        createMockRequest({ requestId: 'req-2', timestamp: new Date('2026-02-02T10:00:00Z') }),
        createMockRequest({ requestId: 'req-3', timestamp: new Date('2026-02-10T10:00:00Z') }),
      ]
      const json = exportAIRequestsToJSON(requests, {
        timeRange: {
          from: new Date('2026-02-01'),
          to: new Date('2026-02-05'),
        },
      })

      expect(json.count).toBe(1)
      expect(json.requests[0].requestId).toBe('req-2')
    })

    it('should respect maxDataPoints', () => {
      const requests = Array.from({ length: 100 }, (_, i) =>
        createMockRequest({ requestId: `req-${i}` })
      )
      const json = exportAIRequestsToJSON(requests, { maxDataPoints: 10 })

      expect(json.count).toBe(10)
    })
  })
})

describe('CSV Export', () => {
  describe('exportAIUsageToCSV', () => {
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

    it('should export aggregates to CSV with header', () => {
      const aggregates = [createMockAggregate()]
      const csv = exportAIUsageToCSV(aggregates)

      const lines = csv.split('\n')
      expect(lines.length).toBe(2) // Header + 1 data row
      expect(lines[0]).toContain('dateKey')
      expect(lines[0]).toContain('modelId')
      expect(lines[0]).toContain('requestCount')
    })

    it('should include data values', () => {
      const aggregates = [createMockAggregate()]
      const csv = exportAIUsageToCSV(aggregates)

      const lines = csv.split('\n')
      expect(lines[1]).toContain('2026-02-03')
      expect(lines[1]).toContain('gpt-4')
      expect(lines[1]).toContain('openai')
      expect(lines[1]).toContain('100') // requestCount
    })

    it('should respect includeHeader option', () => {
      const aggregates = [createMockAggregate()]
      const csv = exportAIUsageToCSV(aggregates, { includeHeader: false })

      const lines = csv.split('\n')
      expect(lines.length).toBe(1)
      expect(lines[0]).not.toContain('dateKey')
    })

    it('should use custom delimiter', () => {
      const aggregates = [createMockAggregate()]
      const csv = exportAIUsageToCSV(aggregates, { delimiter: '\t' })

      expect(csv).toContain('\t')
      expect(csv).not.toContain(',')
    })

    it('should escape values with delimiter', () => {
      const aggregates = [createMockAggregate({ modelId: 'gpt-4,turbo' })]
      const csv = exportAIUsageToCSV(aggregates)

      // Should be quoted
      expect(csv).toContain('"gpt-4,turbo"')
    })

    it('should filter by time range', () => {
      const aggregates = [
        createMockAggregate({ dateKey: '2026-01-15', requestCount: 25 }),
        createMockAggregate({ dateKey: '2026-02-02', requestCount: 50 }),
        createMockAggregate({ dateKey: '2026-02-10', requestCount: 75 }),
      ]
      const csv = exportAIUsageToCSV(aggregates, {
        timeRange: {
          from: new Date('2026-02-01'),
          to: new Date('2026-02-05'),
        },
      })

      const lines = csv.split('\n')
      expect(lines.length).toBe(2) // Header + 1 data row
      expect(lines[1]).toContain('2026-02-02')
    })
  })

  describe('exportAIRequestsToCSV', () => {
    const createMockRequest = (overrides: Partial<AIRequestRecord> = {}): AIRequestRecord => ({
      $id: 'ai_requests/req-1',
      $type: 'AIRequest',
      name: 'req-1',
      requestId: 'req-1',
      timestamp: new Date('2026-02-03T10:00:00Z'),
      modelId: 'gpt-4',
      providerId: 'openai',
      requestType: 'generate',
      status: 'success',
      latencyMs: 500,
      promptTokens: 100,
      completionTokens: 50,
      totalTokens: 150,
      estimatedCost: 0.01,
      cached: false,
      createdAt: new Date('2026-02-03T10:00:00Z'),
      ...overrides,
    })

    it('should export requests to CSV', () => {
      const requests = [createMockRequest()]
      const csv = exportAIRequestsToCSV(requests)

      const lines = csv.split('\n')
      expect(lines.length).toBe(2)
      expect(lines[0]).toContain('requestId')
      expect(lines[0]).toContain('modelId')
      expect(lines[1]).toContain('req-1')
    })

    it('should include all fields', () => {
      const requests = [createMockRequest({
        error: 'Test error',
        errorCode: 'ERR001',
        userId: 'user-1',
      })]
      const csv = exportAIRequestsToCSV(requests)

      expect(csv).toContain('Test error')
      expect(csv).toContain('ERR001')
      expect(csv).toContain('user-1')
    })
  })

  describe('exportCompactionToCSV', () => {
    const createMockMetrics = (namespace: string): CompactionMetrics => ({
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

    it('should export compaction metrics to CSV', () => {
      const metrics = new Map([
        ['posts', createMockMetrics('posts')],
        ['users', createMockMetrics('users')],
      ])
      const csv = exportCompactionToCSV(metrics)

      const lines = csv.split('\n')
      expect(lines.length).toBe(3) // Header + 2 data rows
      expect(lines[0]).toContain('namespace')
      expect(csv).toContain('posts')
      expect(csv).toContain('users')
    })

    it('should filter by namespaces', () => {
      const metrics = new Map([
        ['posts', createMockMetrics('posts')],
        ['users', createMockMetrics('users')],
      ])
      const csv = exportCompactionToCSV(metrics, { namespaces: ['posts'] })

      expect(csv).toContain('posts')
      expect(csv).not.toContain('users')
    })

    it('should accept array of metrics', () => {
      const metrics = [
        createMockMetrics('posts'),
        createMockMetrics('users'),
      ]
      const csv = exportCompactionToCSV(metrics)

      expect(csv).toContain('posts')
      expect(csv).toContain('users')
    })
  })
})

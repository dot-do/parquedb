/**
 * Streaming Export Tests
 *
 * Tests for SSE and WebSocket streaming of observability data.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  formatSSEEvent,
  createSSEMetricEvent,
  createSSEAlertEvent,
  createSSEHeartbeat,
  createSSEError,
  compactionMetricsToSSE,
  aiUsageToSSE,
  parseWSMessage,
  formatWSMessage,
  createWSMetricMessage,
  createWSAlertMessage,
  createWSAckMessage,
  handleWSMessage,
  cleanupWSConnection,
  type WSConnectionState,
} from '../../../../src/observability/export/streaming'
import type { CompactionMetrics } from '../../../../src/observability/compaction/types'
import type { AIUsageAggregate } from '../../../../src/observability/ai/types'

describe('SSE Export', () => {
  describe('formatSSEEvent', () => {
    it('should format metric event', () => {
      const event = createSSEMetricEvent('posts', { windows_pending: 5 })
      const formatted = formatSSEEvent(event)

      expect(formatted).toContain('event: metric')
      expect(formatted).toContain('data: ')
      expect(formatted).toContain('"type":"metric"')
      expect(formatted).toContain('"namespace":"posts"')
      expect(formatted).toContain('"windows_pending":5')
      expect(formatted.endsWith('\n\n')).toBe(true)
    })

    it('should format alert event', () => {
      const event = createSSEAlertEvent('critical', 'High Error Rate', 'Error rate above 10%', 'posts')
      const formatted = formatSSEEvent(event)

      expect(formatted).toContain('event: alert')
      expect(formatted).toContain('"severity":"critical"')
      expect(formatted).toContain('"title":"High Error Rate"')
      expect(formatted).toContain('"namespace":"posts"')
    })

    it('should format heartbeat event', () => {
      const event = createSSEHeartbeat()
      const formatted = formatSSEEvent(event)

      expect(formatted).toContain('event: heartbeat')
      expect(formatted).toContain('"type":"heartbeat"')
      expect(formatted).toContain('"timestamp":')
    })

    it('should format error event', () => {
      const event = createSSEError('Connection failed', 'CONN_ERROR')
      const formatted = formatSSEEvent(event)

      expect(formatted).toContain('event: error')
      expect(formatted).toContain('"error":"Connection failed"')
      expect(formatted).toContain('"code":"CONN_ERROR"')
    })
  })

  describe('createSSEMetricEvent', () => {
    it('should create metric event with timestamp', () => {
      const event = createSSEMetricEvent('posts', { value: 42 })

      expect(event.type).toBe('metric')
      expect(event.namespace).toBe('posts')
      expect(event.metrics).toEqual({ value: 42 })
      expect(event.timestamp).toBeDefined()
      expect(typeof event.timestamp).toBe('number')
    })
  })

  describe('createSSEAlertEvent', () => {
    it('should create alert event with all fields', () => {
      const event = createSSEAlertEvent(
        'warning',
        'Slow Responses',
        'P95 latency above 2 seconds',
        'api',
        { threshold: 2000 }
      )

      expect(event.type).toBe('alert')
      expect(event.severity).toBe('warning')
      expect(event.title).toBe('Slow Responses')
      expect(event.message).toBe('P95 latency above 2 seconds')
      expect(event.namespace).toBe('api')
      expect(event.metadata).toEqual({ threshold: 2000 })
    })

    it('should create alert event without optional fields', () => {
      const event = createSSEAlertEvent('info', 'Test Alert', 'Test message')

      expect(event.type).toBe('alert')
      expect(event.namespace).toBeUndefined()
      expect(event.metadata).toBeUndefined()
    })
  })

  describe('compactionMetricsToSSE', () => {
    it('should convert compaction metrics to SSE event', () => {
      const metrics: CompactionMetrics = {
        namespace: 'posts',
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
      }

      const event = compactionMetricsToSSE(metrics)

      expect(event.type).toBe('metric')
      expect(event.namespace).toBe('posts')
      expect(event.metrics).toEqual({
        windows_pending: 5,
        windows_processing: 2,
        windows_dispatched: 1,
        windows_stuck: 0,
        files_pending: 50,
        bytes_pending: 1024000,
        oldest_window_age_ms: 3600000,
        known_writers: 3,
        active_writers: 2,
      })
    })
  })

  describe('aiUsageToSSE', () => {
    it('should convert AI usage aggregate to SSE event', () => {
      const aggregate: AIUsageAggregate = {
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
      }

      const event = aiUsageToSSE(aggregate)

      expect(event.type).toBe('metric')
      expect(event.namespace).toBe('gpt-4/openai')
      expect(event.metrics.requests_total).toBe(100)
      expect(event.metrics.requests_success).toBe(95)
      expect(event.metrics.requests_error).toBe(5)
      expect(event.metrics.tokens_total).toBe(15000)
      expect(event.metrics.cost_total).toBe(0.60)
      expect(event.metrics.latency_avg).toBe(500)
      expect(event.metrics.error_rate).toBe(0.05)
    })
  })
})

describe('WebSocket Export', () => {
  describe('parseWSMessage', () => {
    it('should parse valid subscribe message', () => {
      const data = JSON.stringify({
        type: 'subscribe',
        id: 'sub-1',
        namespaces: ['posts', 'users'],
        interval: 5000,
      })

      const message = parseWSMessage(data)
      expect(message).toBeDefined()
      expect(message?.type).toBe('subscribe')
      expect((message as { id: string }).id).toBe('sub-1')
    })

    it('should parse valid unsubscribe message', () => {
      const data = JSON.stringify({ type: 'unsubscribe', id: 'sub-1' })

      const message = parseWSMessage(data)
      expect(message).toBeDefined()
      expect(message?.type).toBe('unsubscribe')
    })

    it('should return null for invalid JSON', () => {
      const message = parseWSMessage('invalid json')
      expect(message).toBeNull()
    })
  })

  describe('formatWSMessage', () => {
    it('should format message as JSON', () => {
      const message = createWSMetricMessage('sub-1', 'posts', { value: 42 })
      const formatted = formatWSMessage(message)

      expect(formatted).toContain('"type":"metric"')
      expect(formatted).toContain('"subscriptionId":"sub-1"')
      expect(formatted).toContain('"namespace":"posts"')
    })
  })

  describe('createWSMetricMessage', () => {
    it('should create metric message with all fields', () => {
      const message = createWSMetricMessage('sub-1', 'posts', { windows_pending: 5 })

      expect(message.type).toBe('metric')
      expect(message.subscriptionId).toBe('sub-1')
      expect(message.namespace).toBe('posts')
      expect(message.metrics).toEqual({ windows_pending: 5 })
      expect(message.timestamp).toBeDefined()
    })
  })

  describe('createWSAlertMessage', () => {
    it('should create alert message with all fields', () => {
      const message = createWSAlertMessage('critical', 'High Error Rate', 'Error rate above 10%', 'api')

      expect(message.type).toBe('alert')
      expect(message.severity).toBe('critical')
      expect(message.title).toBe('High Error Rate')
      expect(message.message).toBe('Error rate above 10%')
      expect(message.namespace).toBe('api')
    })
  })

  describe('createWSAckMessage', () => {
    it('should create ack message for subscription', () => {
      const message = createWSAckMessage('sub-1', 'subscribed')

      expect(message.type).toBe('ack')
      expect(message.id).toBe('sub-1')
      expect(message.status).toBe('subscribed')
    })

    it('should create ack message with error', () => {
      const message = createWSAckMessage('sub-1', 'error', 'Already subscribed')

      expect(message.type).toBe('ack')
      expect(message.status).toBe('error')
      expect(message.message).toBe('Already subscribed')
    })
  })

  describe('handleWSMessage', () => {
    let state: WSConnectionState
    let sentMessages: string[]

    beforeEach(() => {
      vi.useFakeTimers()
      sentMessages = []
      state = {
        subscriptions: new Map(),
        onSend: (msg) => sentMessages.push(msg),
      }
    })

    afterEach(() => {
      // Clean up any timers
      cleanupWSConnection(state)
      vi.useRealTimers()
    })

    it('should handle subscribe message', async () => {
      const getMetrics = vi.fn().mockResolvedValue([
        { namespace: 'posts', metrics: { value: 42 } },
      ])

      const data = JSON.stringify({
        type: 'subscribe',
        id: 'sub-1',
        namespaces: ['posts'],
        interval: 100,
      })

      handleWSMessage(data, state, getMetrics)

      // Should have subscription
      expect(state.subscriptions.has('sub-1')).toBe(true)

      // Should send ack
      expect(sentMessages.length).toBe(1)
      const ack = JSON.parse(sentMessages[0])
      expect(ack.type).toBe('ack')
      expect(ack.status).toBe('subscribed')

      // Advance time for first metric push
      await vi.advanceTimersByTimeAsync(150)
      expect(getMetrics).toHaveBeenCalled()
    })

    it('should handle unsubscribe message', () => {
      // First subscribe
      const getMetrics = vi.fn().mockResolvedValue([])
      const subData = JSON.stringify({
        type: 'subscribe',
        id: 'sub-1',
        interval: 1000,
      })
      handleWSMessage(subData, state, getMetrics)

      expect(state.subscriptions.has('sub-1')).toBe(true)
      sentMessages = []

      // Then unsubscribe
      const unsubData = JSON.stringify({
        type: 'unsubscribe',
        id: 'sub-1',
      })
      handleWSMessage(unsubData, state, getMetrics)

      expect(state.subscriptions.has('sub-1')).toBe(false)
      expect(sentMessages.length).toBe(1)
      const ack = JSON.parse(sentMessages[0])
      expect(ack.status).toBe('unsubscribed')
    })

    it('should handle duplicate subscription', () => {
      const getMetrics = vi.fn().mockResolvedValue([])
      const data = JSON.stringify({
        type: 'subscribe',
        id: 'sub-1',
        interval: 1000,
      })

      handleWSMessage(data, state, getMetrics)
      sentMessages = []
      handleWSMessage(data, state, getMetrics)

      const ack = JSON.parse(sentMessages[0])
      expect(ack.status).toBe('error')
      expect(ack.message).toBe('Already subscribed')
    })

    it('should handle invalid message', () => {
      handleWSMessage('invalid json', state, vi.fn())

      expect(sentMessages.length).toBe(1)
      const error = JSON.parse(sentMessages[0])
      expect(error.type).toBe('error')
      expect(error.code).toBe('PARSE_ERROR')
    })

    it('should handle unknown message type', () => {
      const data = JSON.stringify({ type: 'unknown' })
      handleWSMessage(data, state, vi.fn())

      expect(sentMessages.length).toBe(1)
      const error = JSON.parse(sentMessages[0])
      expect(error.type).toBe('error')
      expect(error.code).toBe('UNKNOWN_TYPE')
    })
  })

  describe('cleanupWSConnection', () => {
    it('should clear all subscriptions and timers', () => {
      // Use fake timers to avoid leaking real interval timers
      vi.useFakeTimers()

      const state: WSConnectionState = {
        subscriptions: new Map(),
        onSend: vi.fn(),
      }

      // Add a subscription with timer
      const timer = setInterval(() => {}, 1000)
      state.subscriptions.set('sub-1', {
        id: 'sub-1',
        namespaces: [],
        metrics: [],
        interval: 1000,
        timer,
      })

      cleanupWSConnection(state)

      expect(state.subscriptions.size).toBe(0)

      vi.useRealTimers()
    })
  })
})

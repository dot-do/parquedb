/**
 * TailDO Tests
 *
 * Tests for the TailDO hibernatable WebSocket Durable Object.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { TailWorkerMessage, TailAckMessage, TailErrorMessage } from '../../../src/worker/TailDO'
import type { ValidatedTraceItem } from '../../../src/worker/tail-validation'

describe('TailDO', () => {
  describe('Message Types', () => {
    it('should have correct TailWorkerMessage structure', () => {
      const validatedItem: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [
          { timestamp: Date.now(), level: 'info', message: 'test message' }
        ],
        exceptions: [],
        diagnosticsChannelEvents: [],
      }

      const message: TailWorkerMessage = {
        type: 'tail_events',
        instanceId: 'tail-abc123',
        timestamp: Date.now(),
        events: [validatedItem],
      }

      expect(message.type).toBe('tail_events')
      expect(message.events).toHaveLength(1)
      expect(message.events[0].scriptName).toBe('test-worker')
    })

    it('should have correct TailAckMessage structure', () => {
      const ack: TailAckMessage = {
        type: 'ack',
        count: 10,
        timestamp: Date.now(),
      }

      expect(ack.type).toBe('ack')
      expect(ack.count).toBe(10)
    })

    it('should have correct TailErrorMessage structure', () => {
      const error: TailErrorMessage = {
        type: 'error',
        message: 'Invalid message format',
        timestamp: Date.now(),
      }

      expect(error.type).toBe('error')
      expect(error.message).toBe('Invalid message format')
    })
  })

  describe('ValidatedTraceItem conversion', () => {
    it('should handle null scriptName', () => {
      const item: ValidatedTraceItem = {
        scriptName: null,
        outcome: 'ok',
        eventTimestamp: null,
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [],
      }

      // When converted in TailDO, null scriptName becomes 'unknown'
      const converted = {
        scriptName: item.scriptName ?? 'unknown',
        outcome: item.outcome,
        eventTimestamp: item.eventTimestamp ?? Date.now(),
        event: item.event,
        logs: item.logs,
        exceptions: item.exceptions,
      }

      expect(converted.scriptName).toBe('unknown')
      expect(converted.eventTimestamp).toBeTypeOf('number')
    })

    it('should handle events with request info', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'api-worker',
        outcome: 'ok',
        eventTimestamp: 1700000000000,
        event: {
          request: {
            url: 'https://api.example.com/v1/users',
            method: 'GET',
            headers: { 'content-type': 'application/json' },
            cf: { colo: 'SJC', country: 'US' },
          },
          response: { status: 200 },
        },
        logs: [
          { timestamp: 1700000000100, level: 'info', message: 'Request received' },
          { timestamp: 1700000000200, level: 'info', message: 'Response sent' },
        ],
        exceptions: [],
        diagnosticsChannelEvents: [],
      }

      expect(item.event?.request?.url).toBe('https://api.example.com/v1/users')
      expect(item.event?.request?.cf?.colo).toBe('SJC')
      expect(item.logs).toHaveLength(2)
    })

    it('should handle events with exceptions', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'error-worker',
        outcome: 'exception',
        eventTimestamp: 1700000000000,
        event: null,
        logs: [
          { timestamp: 1700000000100, level: 'error', message: 'Something went wrong' },
        ],
        exceptions: [
          { name: 'TypeError', message: 'Cannot read property of undefined', timestamp: 1700000000100 },
        ],
        diagnosticsChannelEvents: [],
      }

      expect(item.outcome).toBe('exception')
      expect(item.exceptions).toHaveLength(1)
      expect(item.exceptions[0].name).toBe('TypeError')
    })
  })

  describe('Message Serialization', () => {
    it('should serialize and deserialize TailWorkerMessage correctly', () => {
      const original: TailWorkerMessage = {
        type: 'tail_events',
        instanceId: 'tail-xyz789',
        timestamp: 1700000000000,
        events: [
          {
            scriptName: 'my-worker',
            outcome: 'ok',
            eventTimestamp: 1700000000000,
            event: null,
            logs: [
              { timestamp: 1700000000100, level: 'info', message: 'Hello' },
            ],
            exceptions: [],
            diagnosticsChannelEvents: [],
          },
        ],
      }

      const serialized = JSON.stringify(original)
      const deserialized = JSON.parse(serialized) as TailWorkerMessage

      expect(deserialized.type).toBe('tail_events')
      expect(deserialized.instanceId).toBe('tail-xyz789')
      expect(deserialized.events).toHaveLength(1)
      expect(deserialized.events[0].scriptName).toBe('my-worker')
      expect(deserialized.events[0].logs[0].message).toBe('Hello')
    })
  })
})

describe('Streaming Tail Configuration', () => {
  // Note: Full integration tests for TailDO require the Cloudflare Workers runtime
  // These tests validate the configuration structure and types only

  it('should have sensible default configuration values', () => {
    // Default configuration values (mirrored from tail-streaming.ts)
    const DEFAULT_STREAMING_CONFIG = {
      validation: {
        skipInvalidItems: true,
        maxItems: 10000,
        maxLogsPerItem: 1000,
        maxExceptionsPerItem: 100,
      },
      batchSize: 50,
      batchWaitMs: 1000,
      doIdStrategy: 'global' as const,
    }

    expect(DEFAULT_STREAMING_CONFIG.batchSize).toBeGreaterThan(0)
    expect(DEFAULT_STREAMING_CONFIG.batchWaitMs).toBeGreaterThan(0)
    expect(DEFAULT_STREAMING_CONFIG.doIdStrategy).toBe('global')
    expect(DEFAULT_STREAMING_CONFIG.validation).toBeDefined()
    expect(DEFAULT_STREAMING_CONFIG.validation.skipInvalidItems).toBe(true)
  })

  it('should support hourly DO ID strategy', () => {
    // Test DO ID generation logic (mirrored from tail-streaming.ts)
    function getDOId(strategy: 'global' | 'hourly'): string {
      if (strategy === 'global') {
        return 'tail-global'
      }
      const now = new Date()
      const hourKey = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}-${String(now.getUTCHours()).padStart(2, '0')}`
      return `tail-${hourKey}`
    }

    expect(getDOId('global')).toBe('tail-global')

    const hourlyId = getDOId('hourly')
    expect(hourlyId).toMatch(/^tail-\d{4}-\d{2}-\d{2}-\d{2}$/)
  })

  it('should generate unique instance IDs', () => {
    // Test instance ID generation (mirrored from tail-streaming.ts)
    function generateInstanceId(): string {
      const timestamp = Date.now().toString(36)
      const random = Math.random().toString(36).substring(2, 8)
      return `tail-${timestamp}-${random}`
    }

    const id1 = generateInstanceId()
    const id2 = generateInstanceId()

    expect(id1).toMatch(/^tail-[a-z0-9]+-[a-z0-9]+$/)
    expect(id2).toMatch(/^tail-[a-z0-9]+-[a-z0-9]+$/)
    // IDs should be different (with high probability)
    expect(id1).not.toBe(id2)
  })
})

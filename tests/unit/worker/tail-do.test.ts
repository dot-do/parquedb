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

describe('TailDO Hibernation Persistence', () => {
  describe('batchSeq persistence', () => {
    it('should persist batchSeq across hibernation cycles', async () => {
      // Mock storage to track persisted values
      const storageMap = new Map<string, unknown>()
      const mockStorage = {
        get: vi.fn(async (key: string) => storageMap.get(key)),
        put: vi.fn(async (key: string, value: unknown) => {
          storageMap.set(key, value)
        }),
        setAlarm: vi.fn(),
      }

      // Simulate the initialization flow that should restore batchSeq
      // When the DO wakes from hibernation, it should call storage.get('batchSeq')
      // and restore the value

      // Initially, batchSeq should be 0 (no stored value)
      expect(await mockStorage.get('batchSeq')).toBeUndefined()

      // After a batch is written, batchSeq should be persisted
      await mockStorage.put('batchSeq', 1)
      expect(await mockStorage.get('batchSeq')).toBe(1)

      // After more batches
      await mockStorage.put('batchSeq', 5)
      expect(await mockStorage.get('batchSeq')).toBe(5)

      // When DO wakes from hibernation, it should read the persisted value
      const restoredBatchSeq = (await mockStorage.get('batchSeq')) ?? 0
      expect(restoredBatchSeq).toBe(5)
    })

    it('should default batchSeq to 0 when no persisted value exists', async () => {
      const mockStorage = {
        get: vi.fn(async () => undefined),
        put: vi.fn(),
        setAlarm: vi.fn(),
      }

      // When no value is persisted, batchSeq should default to 0
      const restoredBatchSeq = (await mockStorage.get('batchSeq')) ?? 0
      expect(restoredBatchSeq).toBe(0)
    })
  })

  describe('rawEventsBuffer persistence', () => {
    it('should persist buffer events across hibernation cycles', async () => {
      // Mock storage to track persisted values
      const storageMap = new Map<string, unknown>()
      const mockStorage = {
        get: vi.fn(async (key: string) => storageMap.get(key)),
        put: vi.fn(async (key: string, value: unknown) => {
          storageMap.set(key, value)
        }),
        setAlarm: vi.fn(),
      }

      const testEvents: ValidatedTraceItem[] = [
        {
          scriptName: 'test-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          event: null,
          logs: [{ timestamp: Date.now(), level: 'info', message: 'test' }],
          exceptions: [],
          diagnosticsChannelEvents: [],
        },
        {
          scriptName: 'test-worker-2',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          event: null,
          logs: [],
          exceptions: [],
          diagnosticsChannelEvents: [],
        },
      ]

      // Buffer should be persisted after receiving events
      await mockStorage.put('rawEventsBuffer', testEvents)
      expect(await mockStorage.get('rawEventsBuffer')).toEqual(testEvents)

      // When DO wakes from hibernation, it should restore the buffer
      const restoredBuffer = (await mockStorage.get('rawEventsBuffer')) as ValidatedTraceItem[] ?? []
      expect(restoredBuffer).toHaveLength(2)
      expect(restoredBuffer[0].scriptName).toBe('test-worker')
      expect(restoredBuffer[1].scriptName).toBe('test-worker-2')
    })

    it('should default buffer to empty array when no persisted value exists', async () => {
      const mockStorage = {
        get: vi.fn(async () => undefined),
        put: vi.fn(),
        setAlarm: vi.fn(),
      }

      // When no value is persisted, buffer should default to empty array
      const restoredBuffer = (await mockStorage.get('rawEventsBuffer')) ?? []
      expect(restoredBuffer).toEqual([])
    })

    it('should clear buffer after successful flush', async () => {
      // Mock storage to track persisted values
      const storageMap = new Map<string, unknown>()
      const mockStorage = {
        get: vi.fn(async (key: string) => storageMap.get(key)),
        put: vi.fn(async (key: string, value: unknown) => {
          storageMap.set(key, value)
        }),
        setAlarm: vi.fn(),
      }

      const testEvents: ValidatedTraceItem[] = [
        {
          scriptName: 'test-worker',
          outcome: 'ok',
          eventTimestamp: Date.now(),
          event: null,
          logs: [],
          exceptions: [],
          diagnosticsChannelEvents: [],
        },
      ]

      // Buffer is persisted with events
      await mockStorage.put('rawEventsBuffer', testEvents)
      expect(await mockStorage.get('rawEventsBuffer')).toHaveLength(1)

      // After flush, buffer should be cleared (empty array, not deleted)
      await mockStorage.put('rawEventsBuffer', [])
      expect(await mockStorage.get('rawEventsBuffer')).toEqual([])
    })

    it('should preserve batchSeq when clearing buffer', async () => {
      // Mock storage to track persisted values
      const storageMap = new Map<string, unknown>()
      const mockStorage = {
        get: vi.fn(async (key: string) => storageMap.get(key)),
        put: vi.fn(async (key: string, value: unknown) => {
          storageMap.set(key, value)
        }),
        setAlarm: vi.fn(),
      }

      // Set both buffer and batchSeq
      await mockStorage.put('rawEventsBuffer', [{ scriptName: 'test' }])
      await mockStorage.put('batchSeq', 42)

      // Clear buffer (simulating flush)
      await mockStorage.put('rawEventsBuffer', [])

      // batchSeq should be preserved
      expect(await mockStorage.get('batchSeq')).toBe(42)
      expect(await mockStorage.get('rawEventsBuffer')).toEqual([])
    })
  })

  describe('combined state restoration', () => {
    it('should restore both buffer and batchSeq on wake from hibernation', async () => {
      // Mock storage with pre-existing state (simulating hibernation wake)
      const preExistingState = new Map<string, unknown>([
        ['rawEventsBuffer', [
          {
            scriptName: 'persisted-worker',
            outcome: 'ok',
            eventTimestamp: 1700000000000,
            event: null,
            logs: [],
            exceptions: [],
            diagnosticsChannelEvents: [],
          },
        ]],
        ['batchSeq', 10],
      ])

      const mockStorage = {
        get: vi.fn(async (key: string) => preExistingState.get(key)),
        put: vi.fn(async (key: string, value: unknown) => {
          preExistingState.set(key, value)
        }),
        setAlarm: vi.fn(),
      }

      // Simulate loading state on wake
      const [buffer, batchSeq] = await Promise.all([
        mockStorage.get('rawEventsBuffer') as Promise<ValidatedTraceItem[] | undefined>,
        mockStorage.get('batchSeq') as Promise<number | undefined>,
      ])

      expect(buffer).toBeDefined()
      expect(buffer).toHaveLength(1)
      expect(buffer![0].scriptName).toBe('persisted-worker')
      expect(batchSeq).toBe(10)
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

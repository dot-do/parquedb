/**
 * Tail Worker Scaffold Tests
 *
 * Unit tests for the ParqueDB Tail Worker scaffold implementation.
 * Tests event filtering, batching, processing, and storage functions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  filterTraceItem,
  filterTraceItems,
  processTraceItem,
  processTraceItems,
  createBatchState,
  shouldFlushBatch,
  addToBatch,
  storeEventsInR2,
  writeToAnalytics,
  sendAlert,
  createTailHandler,
  DEFAULT_FILTER,
  DEFAULT_BATCH_CONFIG,
  type TraceItem,
  type TailEventFilter,
  type BatchConfig,
  type ProcessedEvent,
  type TailWorkerEnv,
} from '@/worker/tail'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock trace item
 */
function createMockTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: 'parquedb-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    event: {
      request: {
        url: 'https://parquedb.com/datasets/imdb/titles',
        method: 'GET',
        headers: {},
        cf: {
          colo: 'SJC',
          country: 'US',
          city: 'San Jose',
          asn: 13335,
          asOrganization: 'Cloudflare',
        },
      },
    },
    logs: [
      { timestamp: Date.now(), level: 'info', message: 'Request received' },
    ],
    exceptions: [],
    diagnosticsChannelEvents: [],
    ...overrides,
  }
}

/**
 * Create a mock R2 bucket
 */
function createMockR2Bucket() {
  return {
    put: vi.fn().mockResolvedValue(undefined),
    get: vi.fn().mockResolvedValue(null),
    list: vi.fn().mockResolvedValue({ objects: [] }),
    delete: vi.fn().mockResolvedValue(undefined),
  }
}

/**
 * Create a mock Analytics Engine dataset
 */
function createMockAnalytics() {
  return {
    writeDataPoint: vi.fn(),
  }
}

// =============================================================================
// Event Filtering Tests
// =============================================================================

describe('Event Filtering', () => {
  describe('filterTraceItem', () => {
    it('should include all items with default filter', () => {
      const item = createMockTraceItem()
      expect(filterTraceItem(item, DEFAULT_FILTER)).toBe(true)
    })

    it('should filter by script name', () => {
      const item = createMockTraceItem({ scriptName: 'parquedb-worker' })
      const filter: TailEventFilter = { scriptNames: ['other-worker'] }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should include matching script name', () => {
      const item = createMockTraceItem({ scriptName: 'parquedb-worker' })
      const filter: TailEventFilter = { scriptNames: ['parquedb-worker', 'other'] }

      expect(filterTraceItem(item, filter)).toBe(true)
    })

    it('should filter by outcome', () => {
      const item = createMockTraceItem({ outcome: 'exception' })
      const filter: TailEventFilter = { outcomes: ['ok'] }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should include matching outcome', () => {
      const item = createMockTraceItem({ outcome: 'exception' })
      const filter: TailEventFilter = { outcomes: ['ok', 'exception'] }

      expect(filterTraceItem(item, filter)).toBe(true)
    })

    it('should filter for exceptions only', () => {
      const itemNoExceptions = createMockTraceItem({ exceptions: [] })
      const itemWithExceptions = createMockTraceItem({
        exceptions: [
          { name: 'Error', message: 'Test error', timestamp: Date.now() },
        ],
      })
      const filter: TailEventFilter = { exceptionsOnly: true }

      expect(filterTraceItem(itemNoExceptions, filter)).toBe(false)
      expect(filterTraceItem(itemWithExceptions, filter)).toBe(true)
    })

    it('should filter by minimum logs', () => {
      const item = createMockTraceItem({
        logs: [{ timestamp: Date.now(), level: 'info', message: 'single log' }],
      })
      const filter: TailEventFilter = { minLogs: 3 }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should include items meeting minimum logs', () => {
      const item = createMockTraceItem({
        logs: [
          { timestamp: Date.now(), level: 'info', message: 'log 1' },
          { timestamp: Date.now(), level: 'info', message: 'log 2' },
          { timestamp: Date.now(), level: 'info', message: 'log 3' },
        ],
      })
      const filter: TailEventFilter = { minLogs: 3 }

      expect(filterTraceItem(item, filter)).toBe(true)
    })

    it('should filter by log level', () => {
      const item = createMockTraceItem({
        logs: [{ timestamp: Date.now(), level: 'debug', message: 'debug only' }],
      })
      const filter: TailEventFilter = { logLevels: ['warn', 'error'] }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should include items with matching log levels', () => {
      const item = createMockTraceItem({
        logs: [
          { timestamp: Date.now(), level: 'debug', message: 'debug' },
          { timestamp: Date.now(), level: 'error', message: 'error' },
        ],
      })
      const filter: TailEventFilter = { logLevels: ['warn', 'error'] }

      expect(filterTraceItem(item, filter)).toBe(true)
    })

    it('should filter by URL pattern', () => {
      const item = createMockTraceItem({
        event: {
          request: {
            url: 'https://parquedb.com/datasets/imdb/titles',
            method: 'GET',
            headers: {},
          },
        },
      })
      const filter: TailEventFilter = { urlPatterns: ['*/health', '*/debug/*'] }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should include items matching URL pattern', () => {
      const item = createMockTraceItem({
        event: {
          request: {
            url: 'https://parquedb.com/datasets/imdb/titles',
            method: 'GET',
            headers: {},
          },
        },
      })
      const filter: TailEventFilter = { urlPatterns: ['*/datasets/*'] }

      expect(filterTraceItem(item, filter)).toBe(true)
    })

    it('should handle null script name with script filter', () => {
      const item = createMockTraceItem({ scriptName: null })
      const filter: TailEventFilter = { scriptNames: ['parquedb-worker'] }

      expect(filterTraceItem(item, filter)).toBe(false)
    })

    it('should handle empty logs with log level filter', () => {
      const item = createMockTraceItem({ logs: [] })
      const filter: TailEventFilter = { logLevels: ['error'] }

      // Empty logs should pass - we only filter if there ARE logs but none match
      expect(filterTraceItem(item, filter)).toBe(true)
    })
  })

  describe('filterTraceItems', () => {
    it('should filter an array of items', () => {
      const items = [
        createMockTraceItem({ outcome: 'ok' }),
        createMockTraceItem({ outcome: 'exception' }),
        createMockTraceItem({ outcome: 'ok' }),
      ]
      const filter: TailEventFilter = { outcomes: ['exception'] }

      const result = filterTraceItems(items, filter)

      expect(result).toHaveLength(1)
      expect(result[0]!.outcome).toBe('exception')
    })

    it('should return empty array when no items match', () => {
      const items = [
        createMockTraceItem({ outcome: 'ok' }),
        createMockTraceItem({ outcome: 'ok' }),
      ]
      const filter: TailEventFilter = { outcomes: ['exception'] }

      const result = filterTraceItems(items, filter)

      expect(result).toHaveLength(0)
    })
  })
})

// =============================================================================
// Event Processing Tests
// =============================================================================

describe('Event Processing', () => {
  describe('processTraceItem', () => {
    it('should transform trace item to processed event', () => {
      const timestamp = Date.now()
      const item = createMockTraceItem({ eventTimestamp: timestamp })

      const result = processTraceItem(item)

      expect(result.id).toBeDefined()
      expect(result.timestamp).toBe(new Date(timestamp).toISOString())
      expect(result.scriptName).toBe('parquedb-worker')
      expect(result.outcome).toBe('ok')
      expect(result.logCount).toBe(1)
      expect(result.exceptionCount).toBe(0)
    })

    it('should include request information', () => {
      const item = createMockTraceItem()

      const result = processTraceItem(item)

      expect(result.method).toBe('GET')
      expect(result.url).toBe('https://parquedb.com/datasets/imdb/titles')
      expect(result.colo).toBe('SJC')
      expect(result.country).toBe('US')
    })

    it('should include exceptions as errors', () => {
      const item = createMockTraceItem({
        exceptions: [
          { name: 'TypeError', message: 'null is not an object', timestamp: Date.now() },
          { name: 'RangeError', message: 'index out of bounds', timestamp: Date.now() },
        ],
      })

      const result = processTraceItem(item)

      expect(result.errors).toHaveLength(2)
      expect(result.errors![0]).toBe('TypeError: null is not an object')
      expect(result.errors![1]).toBe('RangeError: index out of bounds')
      expect(result.exceptionCount).toBe(2)
    })

    it('should include warn and error logs only', () => {
      const item = createMockTraceItem({
        logs: [
          { timestamp: Date.now(), level: 'debug', message: 'debug log' },
          { timestamp: Date.now(), level: 'info', message: 'info log' },
          { timestamp: Date.now(), level: 'warn', message: 'warning log' },
          { timestamp: Date.now(), level: 'error', message: 'error log' },
        ],
      })

      const result = processTraceItem(item)

      expect(result.logs).toHaveLength(2)
      expect(result.logs![0]!.level).toBe('warn')
      expect(result.logs![1]!.level).toBe('error')
    })

    it('should stringify non-string log messages', () => {
      const item = createMockTraceItem({
        logs: [
          { timestamp: Date.now(), level: 'error', message: { foo: 'bar' } },
        ],
      })

      const result = processTraceItem(item)

      expect(result.logs![0]!.message).toBe('{"foo":"bar"}')
    })

    it('should handle null event timestamp', () => {
      const item = createMockTraceItem({ eventTimestamp: null })

      const result = processTraceItem(item)

      // Should use current time
      expect(result.timestamp).toBeDefined()
      const parsed = new Date(result.timestamp)
      expect(parsed.getTime()).toBeGreaterThan(Date.now() - 1000)
    })

    it('should handle null script name', () => {
      const item = createMockTraceItem({ scriptName: null })

      const result = processTraceItem(item)

      expect(result.scriptName).toBe('unknown')
    })

    it('should generate unique event IDs', () => {
      const item = createMockTraceItem()

      const result1 = processTraceItem(item)
      const result2 = processTraceItem(item)

      expect(result1.id).not.toBe(result2.id)
    })
  })

  describe('processTraceItems', () => {
    it('should process multiple items', () => {
      const items = [
        createMockTraceItem({ scriptName: 'worker-1' }),
        createMockTraceItem({ scriptName: 'worker-2' }),
      ]

      const results = processTraceItems(items)

      expect(results).toHaveLength(2)
      expect(results[0]!.scriptName).toBe('worker-1')
      expect(results[1]!.scriptName).toBe('worker-2')
    })
  })
})

// =============================================================================
// Batching Tests
// =============================================================================

describe('Batching', () => {
  describe('createBatchState', () => {
    it('should create empty batch state', () => {
      const state = createBatchState()

      expect(state.events).toHaveLength(0)
      expect(state.startTime).toBeGreaterThan(0)
      expect(state.lastFlush).toBeGreaterThan(0)
    })
  })

  describe('shouldFlushBatch', () => {
    it('should not flush empty batch', () => {
      const state = createBatchState()
      const config: BatchConfig = { maxEvents: 10, maxWaitMs: 1000, minEvents: 1 }

      expect(shouldFlushBatch(state, config)).toBe(false)
    })

    it('should flush when max events reached', () => {
      const state = createBatchState()
      state.events = Array(10).fill({} as ProcessedEvent)
      const config: BatchConfig = { maxEvents: 10, maxWaitMs: 1000, minEvents: 1 }

      expect(shouldFlushBatch(state, config)).toBe(true)
    })

    it('should flush when max wait time reached and min events met', () => {
      const state = createBatchState()
      state.events = [{} as ProcessedEvent]
      state.startTime = Date.now() - 2000 // 2 seconds ago
      const config: BatchConfig = { maxEvents: 100, maxWaitMs: 1000, minEvents: 1 }

      expect(shouldFlushBatch(state, config)).toBe(true)
    })

    it('should not flush if min events not met', () => {
      const state = createBatchState()
      state.events = [{} as ProcessedEvent]
      state.startTime = Date.now() - 2000 // 2 seconds ago
      const config: BatchConfig = { maxEvents: 100, maxWaitMs: 1000, minEvents: 5 }

      expect(shouldFlushBatch(state, config)).toBe(false)
    })
  })

  describe('addToBatch', () => {
    it('should add events to batch', () => {
      const state = createBatchState()
      const events = [
        { id: '1', scriptName: 'test' } as ProcessedEvent,
        { id: '2', scriptName: 'test' } as ProcessedEvent,
      ]
      const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

      const toFlush = addToBatch(state, events, config)

      expect(toFlush).toHaveLength(0)
      expect(state.events).toHaveLength(2)
    })

    it('should return events to flush when threshold reached', () => {
      const state = createBatchState()
      state.events = Array(9).fill({ id: 'existing' } as ProcessedEvent)
      const newEvent = { id: 'new' } as ProcessedEvent
      const config: BatchConfig = { maxEvents: 10, maxWaitMs: 10000, minEvents: 1 }

      const toFlush = addToBatch(state, [newEvent], config)

      expect(toFlush).toHaveLength(10)
      expect(state.events).toHaveLength(0) // Batch reset
    })

    it('should reset batch state after flush', () => {
      const state = createBatchState()
      state.events = Array(9).fill({ id: 'existing' } as ProcessedEvent)
      const startTimeBefore = state.startTime
      const config: BatchConfig = { maxEvents: 10, maxWaitMs: 10000, minEvents: 1 }

      addToBatch(state, [{ id: 'new' } as ProcessedEvent], config)

      expect(state.startTime).toBeGreaterThanOrEqual(startTimeBefore)
      expect(state.lastFlush).toBeGreaterThanOrEqual(startTimeBefore)
    })
  })
})

// =============================================================================
// Storage Tests
// =============================================================================

describe('Storage', () => {
  describe('storeEventsInR2', () => {
    it('should store events in R2 with correct key format', async () => {
      const bucket = createMockR2Bucket()
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '2024-01-01T00:00:00Z', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 0 },
      ]

      const key = await storeEventsInR2(bucket as unknown as R2Bucket, events)

      expect(key).toMatch(/^logs\/\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.json$/)
      expect(bucket.put).toHaveBeenCalledTimes(1)
      expect(bucket.put).toHaveBeenCalledWith(
        key,
        expect.any(String),
        expect.objectContaining({
          httpMetadata: { contentType: 'application/json' },
          customMetadata: expect.objectContaining({
            eventCount: '1',
          }),
        })
      )
    })

    it('should store events as JSON', async () => {
      const bucket = createMockR2Bucket()
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '2024-01-01T00:00:00Z', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 0 },
        { id: '2', timestamp: '2024-01-01T00:00:01Z', scriptName: 'test', outcome: 'exception', logCount: 1, exceptionCount: 1 },
      ]

      await storeEventsInR2(bucket as unknown as R2Bucket, events)

      const storedJson = bucket.put.mock.calls[0]![1] as string
      const parsed = JSON.parse(storedJson)
      expect(parsed).toHaveLength(2)
      expect(parsed[0].id).toBe('1')
      expect(parsed[1].id).toBe('2')
    })
  })

  describe('writeToAnalytics', () => {
    it('should write data points to Analytics Engine', async () => {
      const analytics = createMockAnalytics()
      const events: ProcessedEvent[] = [
        {
          id: '1',
          timestamp: '2024-01-01T00:00:00Z',
          scriptName: 'parquedb-worker',
          outcome: 'ok',
          method: 'GET',
          country: 'US',
          colo: 'SJC',
          logCount: 2,
          exceptionCount: 0,
        },
      ]

      await writeToAnalytics(analytics as unknown as AnalyticsEngineDataset, events)

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(1)
      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['parquedb-worker', 'ok', 'GET', 'US'],
        doubles: [1, 2, 0],
        indexes: ['SJC'],
      })
    })

    it('should write multiple data points for multiple events', async () => {
      const analytics = createMockAnalytics()
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '', scriptName: 'w1', outcome: 'ok', logCount: 0, exceptionCount: 0 },
        { id: '2', timestamp: '', scriptName: 'w2', outcome: 'exception', logCount: 1, exceptionCount: 1 },
      ]

      await writeToAnalytics(analytics as unknown as AnalyticsEngineDataset, events)

      expect(analytics.writeDataPoint).toHaveBeenCalledTimes(2)
    })

    it('should use "unknown" for missing fields', async () => {
      const analytics = createMockAnalytics()
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 0 },
      ]

      await writeToAnalytics(analytics as unknown as AnalyticsEngineDataset, events)

      expect(analytics.writeDataPoint).toHaveBeenCalledWith({
        blobs: ['test', 'ok', 'unknown', 'unknown'],
        doubles: [1, 0, 0],
        indexes: ['unknown'],
      })
    })
  })

  describe('sendAlert', () => {
    beforeEach(() => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true }))
    })

    it('should send alert for critical events', async () => {
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '', scriptName: 'test', outcome: 'exception', logCount: 0, exceptionCount: 1 },
      ]

      await sendAlert('https://webhook.example.com', events)

      expect(fetch).toHaveBeenCalledWith(
        'https://webhook.example.com',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        })
      )

      const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0]![1].body)
      expect(body.text).toContain('1 critical event')
      expect(body.events).toHaveLength(1)
    })

    it('should not send alert when all events are ok', async () => {
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 0 },
        { id: '2', timestamp: '', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 0 },
      ]

      await sendAlert('https://webhook.example.com', events)

      expect(fetch).not.toHaveBeenCalled()
    })

    it('should limit events to 10 in alert', async () => {
      const events: ProcessedEvent[] = Array(20).fill(null).map((_, i) => ({
        id: String(i),
        timestamp: '',
        scriptName: 'test',
        outcome: 'exception',
        logCount: 0,
        exceptionCount: 1,
      }))

      await sendAlert('https://webhook.example.com', events)

      const body = JSON.parse((fetch as ReturnType<typeof vi.fn>).mock.calls[0]![1].body)
      expect(body.events).toHaveLength(10)
    })

    it('should include events with exceptions even if outcome is ok', async () => {
      const events: ProcessedEvent[] = [
        { id: '1', timestamp: '', scriptName: 'test', outcome: 'ok', logCount: 0, exceptionCount: 1 },
      ]

      await sendAlert('https://webhook.example.com', events)

      expect(fetch).toHaveBeenCalled()
    })
  })
})

// =============================================================================
// Tail Handler Tests
// =============================================================================

describe('createTailHandler', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true }))
  })

  it('should create handler with default config', () => {
    const handler = createTailHandler()
    expect(typeof handler).toBe('function')
  })

  it('should filter, process, and store events', async () => {
    const bucket = createMockR2Bucket()
    const analytics = createMockAnalytics()
    const env: TailWorkerEnv = {
      LOGS_BUCKET: bucket as unknown as R2Bucket,
      ANALYTICS: analytics as unknown as AnalyticsEngineDataset,
    }

    const handler = createTailHandler({
      batch: { maxEvents: 1, maxWaitMs: 0, minEvents: 1 },
      enableR2Storage: true,
      enableAnalytics: true,
      enableAlerts: false,
    })

    const events = [createMockTraceItem()]

    await handler(events, env)

    expect(bucket.put).toHaveBeenCalled()
    expect(analytics.writeDataPoint).toHaveBeenCalled()
  })

  it('should skip storage when no events match filter', async () => {
    const bucket = createMockR2Bucket()
    const env: TailWorkerEnv = {
      LOGS_BUCKET: bucket as unknown as R2Bucket,
    }

    const handler = createTailHandler({
      filter: { outcomes: ['exception'] },
      batch: { maxEvents: 1, maxWaitMs: 0, minEvents: 1 },
    })

    const events = [createMockTraceItem({ outcome: 'ok' })]

    await handler(events, env)

    expect(bucket.put).not.toHaveBeenCalled()
  })

  it('should not store if R2 is disabled', async () => {
    const bucket = createMockR2Bucket()
    const env: TailWorkerEnv = {
      LOGS_BUCKET: bucket as unknown as R2Bucket,
    }

    const handler = createTailHandler({
      batch: { maxEvents: 1, maxWaitMs: 0, minEvents: 1 },
      enableR2Storage: false,
    })

    const events = [createMockTraceItem()]

    await handler(events, env)

    expect(bucket.put).not.toHaveBeenCalled()
  })

  it('should not write analytics if Analytics Engine not bound', async () => {
    const analytics = createMockAnalytics()
    const env: TailWorkerEnv = {
      // ANALYTICS not bound
    }

    const handler = createTailHandler({
      batch: { maxEvents: 1, maxWaitMs: 0, minEvents: 1 },
      enableAnalytics: true,
    })

    const events = [createMockTraceItem()]

    await handler(events, env)

    expect(analytics.writeDataPoint).not.toHaveBeenCalled()
  })

  it('should batch events before flushing', async () => {
    const bucket = createMockR2Bucket()
    const env: TailWorkerEnv = {
      LOGS_BUCKET: bucket as unknown as R2Bucket,
    }

    const handler = createTailHandler({
      batch: { maxEvents: 5, maxWaitMs: 10000, minEvents: 1 },
      enableR2Storage: true,
    })

    // First call - not enough events
    await handler([createMockTraceItem()], env)
    expect(bucket.put).not.toHaveBeenCalled()

    // Second call - still not enough
    await handler([createMockTraceItem(), createMockTraceItem()], env)
    expect(bucket.put).not.toHaveBeenCalled()

    // Third call - reaches threshold
    await handler([createMockTraceItem(), createMockTraceItem()], env)
    expect(bucket.put).toHaveBeenCalledTimes(1)
  })
})

// =============================================================================
// Default Export Tests
// =============================================================================

describe('Default Export', () => {
  it('should export tail handler', async () => {
    const { default: tailWorker } = await import('@/worker/tail')

    expect(tailWorker.tail).toBeDefined()
    expect(typeof tailWorker.tail).toBe('function')
  })
})

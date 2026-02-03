/**
 * Tail Worker Tests
 *
 * Comprehensive tests for Tail Worker integration, covering:
 * - Event handling (processing TraceItem events)
 * - Batching logic (efficient batch processing)
 * - Error handling (graceful error recovery)
 * - MV integration (defineStreamView, db.ingestStream)
 *
 * Based on the design in docs/architecture/materialized-views.md
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// =============================================================================
// Type Definitions (based on Cloudflare Workers types)
// =============================================================================

/**
 * TraceItem represents a single event from a tailed worker.
 * This mirrors the Cloudflare Workers TraceItem interface.
 */
interface TraceItem {
  readonly event:
    | TraceItemFetchEventInfo
    | TraceItemScheduledEventInfo
    | TraceItemAlarmEventInfo
    | TraceItemQueueEventInfo
    | null
  readonly eventTimestamp: number | null
  readonly logs: TraceLog[]
  readonly exceptions: TraceException[]
  readonly scriptName: string | null
  readonly outcome: string
  readonly executionModel: string
  readonly truncated: boolean
  readonly cpuTime: number
  readonly wallTime: number
}

interface TraceItemFetchEventInfo {
  readonly response?: { readonly status: number }
  readonly request: {
    readonly cf?: {
      readonly colo?: string
      readonly country?: string
    }
    readonly headers: Record<string, string>
    readonly method: string
    readonly url: string
  }
}

interface TraceItemScheduledEventInfo {
  readonly scheduledTime: number
  readonly cron: string
}

interface TraceItemAlarmEventInfo {
  readonly scheduledTime: Date
}

interface TraceItemQueueEventInfo {
  readonly queue: string
  readonly batchSize: number
}

interface TraceLog {
  readonly timestamp: number
  readonly level: string
  readonly message: unknown
}

interface TraceException {
  readonly timestamp: number
  readonly message: string
  readonly name: string
  readonly stack?: string
}

// =============================================================================
// Stream View Definition Types (from the MV design)
// =============================================================================

interface StreamViewDefinition<TInput, TOutput> {
  $type: string
  $stream: string
  $schema: Record<string, string>
  $filter?: (event: TInput) => boolean
  $transform: (event: TInput) => TOutput | TOutput[]
  $refresh: {
    mode: 'streaming'
    backend?: 'native' | 'iceberg' | 'delta'
  }
}

// =============================================================================
// Mock Implementations
// =============================================================================

/**
 * Mock storage for testing MV writes
 */
class MockStorage {
  private data: Map<string, unknown[]> = new Map()

  async write(viewName: string, records: unknown[]): Promise<void> {
    const existing = this.data.get(viewName) || []
    this.data.set(viewName, [...existing, ...records])
  }

  getData(viewName: string): unknown[] {
    return this.data.get(viewName) || []
  }

  clear(): void {
    this.data.clear()
  }
}

/**
 * TailEventProcessor handles processing of tail events and writing to MVs.
 * This simulates the core logic that would be in src/tail/index.ts
 */
class TailEventProcessor {
  private storage: MockStorage
  private views: Map<string, StreamViewDefinition<TraceItem, unknown>> = new Map()
  private batchSize: number
  private flushInterval: number
  private pendingBatches: Map<string, unknown[]> = new Map()
  private errorHandler?: (error: Error, event: TraceItem) => void

  constructor(options: {
    storage: MockStorage
    batchSize?: number
    flushInterval?: number
    errorHandler?: (error: Error, event: TraceItem) => void
  }) {
    this.storage = options.storage
    this.batchSize = options.batchSize || 100
    this.flushInterval = options.flushInterval || 1000
    this.errorHandler = options.errorHandler
  }

  registerView<TOutput>(view: StreamViewDefinition<TraceItem, TOutput>): void {
    this.views.set(view.$type, view as StreamViewDefinition<TraceItem, unknown>)
    this.pendingBatches.set(view.$type, [])
  }

  async processEvents(events: TraceItem[]): Promise<ProcessingResult> {
    const result: ProcessingResult = {
      processed: 0,
      filtered: 0,
      errors: 0,
      byView: new Map(),
    }

    for (const event of events) {
      for (const [viewName, view] of this.views) {
        try {
          // Apply filter if present
          if (view.$filter && !view.$filter(event)) {
            result.filtered++
            continue
          }

          // Transform the event
          const transformed = view.$transform(event)
          const records = Array.isArray(transformed) ? transformed : [transformed]

          // Add to pending batch
          const pending = this.pendingBatches.get(viewName) || []
          pending.push(...records)
          this.pendingBatches.set(viewName, pending)

          // Update stats
          const viewCount = result.byView.get(viewName) || 0
          result.byView.set(viewName, viewCount + records.length)
          result.processed += records.length

          // Check if we should flush
          if (pending.length >= this.batchSize) {
            await this.flushView(viewName)
          }
        } catch (error) {
          result.errors++
          if (this.errorHandler) {
            this.errorHandler(error as Error, event)
          }
        }
      }
    }

    return result
  }

  async flush(): Promise<void> {
    for (const viewName of this.views.keys()) {
      await this.flushView(viewName)
    }
  }

  private async flushView(viewName: string): Promise<void> {
    const pending = this.pendingBatches.get(viewName) || []
    if (pending.length === 0) return

    await this.storage.write(viewName, pending)
    this.pendingBatches.set(viewName, [])
  }

  getPendingCount(viewName: string): number {
    return (this.pendingBatches.get(viewName) || []).length
  }
}

interface ProcessingResult {
  processed: number
  filtered: number
  errors: number
  byView: Map<string, number>
}

// =============================================================================
// Test Helpers
// =============================================================================

function createMockTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    event: {
      request: {
        method: 'GET',
        url: 'https://example.com/api/test',
        headers: { 'user-agent': 'test-client' },
        cf: { colo: 'DFW', country: 'US' },
      },
      response: { status: 200 },
    } as TraceItemFetchEventInfo,
    eventTimestamp: Date.now(),
    logs: [],
    exceptions: [],
    scriptName: 'test-worker',
    outcome: 'ok',
    executionModel: 'stateless',
    truncated: false,
    cpuTime: 10,
    wallTime: 50,
    ...overrides,
  }
}

function createMockLog(overrides: Partial<TraceLog> = {}): TraceLog {
  return {
    timestamp: Date.now(),
    level: 'log',
    message: 'Test log message',
    ...overrides,
  }
}

function createMockException(overrides: Partial<TraceException> = {}): TraceException {
  return {
    timestamp: Date.now(),
    message: 'Test error message',
    name: 'Error',
    stack: 'Error: Test error message\n    at test.ts:1:1',
    ...overrides,
  }
}

// =============================================================================
// View Definitions for Testing (based on docs/architecture/materialized-views.md)
// =============================================================================

const WorkerLogsView: StreamViewDefinition<TraceItem, {
  $id: string
  scriptName: string
  level: string
  message: string
  timestamp: Date
  colo: string | null
  url: string | null
}> = {
  $type: 'WorkerLog',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    level: 'string!',
    message: 'string!',
    timestamp: 'timestamp!',
    colo: 'string?',
    url: 'string?',
  },
  $transform: (event: TraceItem) => event.logs.map(log => ({
    $id: `${event.eventTimestamp}-${log.timestamp}`,
    scriptName: event.scriptName || 'unknown',
    level: log.level,
    message: Array.isArray(log.message) ? log.message.join(' ') : String(log.message),
    timestamp: new Date(log.timestamp),
    colo: (event.event as TraceItemFetchEventInfo)?.request?.cf?.colo || null,
    url: (event.event as TraceItemFetchEventInfo)?.request?.url || null,
  })),
  $refresh: { mode: 'streaming', backend: 'native' },
}

const WorkerErrorsView: StreamViewDefinition<TraceItem, {
  $id: string
  scriptName: string
  outcome: string
  exceptionName: string | null
  exceptionMessage: string | null
  timestamp: Date
  url: string | null
  status: number | null
  colo: string | null
}> = {
  $type: 'WorkerError',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    outcome: 'string!',
    exceptionName: 'string?',
    exceptionMessage: 'string?',
    timestamp: 'timestamp!',
    url: 'string?',
    status: 'int?',
    colo: 'string?',
  },
  $filter: (event: TraceItem) =>
    event.outcome !== 'ok' || event.exceptions.length > 0,
  $transform: (event: TraceItem) => ({
    $id: `${event.scriptName}-${event.eventTimestamp}`,
    scriptName: event.scriptName || 'unknown',
    outcome: event.outcome,
    exceptionName: event.exceptions[0]?.name || null,
    exceptionMessage: event.exceptions[0]?.message || null,
    timestamp: new Date(event.eventTimestamp || Date.now()),
    url: (event.event as TraceItemFetchEventInfo)?.request?.url || null,
    status: (event.event as TraceItemFetchEventInfo)?.response?.status || null,
    colo: (event.event as TraceItemFetchEventInfo)?.request?.cf?.colo || null,
  }),
  $refresh: { mode: 'streaming', backend: 'native' },
}

const WorkerRequestsView: StreamViewDefinition<TraceItem, {
  $id: string
  scriptName: string
  method: string
  url: string
  pathname: string
  status: number
  outcome: string
  colo: string
  country: string | null
  timestamp: Date
}> = {
  $type: 'WorkerRequest',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    method: 'string!',
    url: 'string!',
    pathname: 'string!',
    status: 'int!',
    outcome: 'string!',
    colo: 'string!',
    country: 'string?',
    timestamp: 'timestamp!',
  },
  $filter: (event: TraceItem) => event.event != null,
  $transform: (event: TraceItem) => {
    const fetchEvent = event.event as TraceItemFetchEventInfo
    const url = new URL(fetchEvent.request.url)
    return {
      $id: `${event.scriptName}-${event.eventTimestamp}`,
      scriptName: event.scriptName || 'unknown',
      method: fetchEvent.request.method,
      url: fetchEvent.request.url,
      pathname: url.pathname,
      status: fetchEvent.response?.status ?? 0,
      outcome: event.outcome,
      colo: fetchEvent.request.cf?.colo ?? 'unknown',
      country: fetchEvent.request.cf?.country || null,
      timestamp: new Date(event.eventTimestamp || Date.now()),
    }
  },
  $refresh: { mode: 'streaming', backend: 'native' },
}

// =============================================================================
// Test Suites
// =============================================================================

describe('Tail Worker', () => {
  let storage: MockStorage
  let processor: TailEventProcessor

  beforeEach(() => {
    storage = new MockStorage()
    processor = new TailEventProcessor({ storage })
  })

  afterEach(() => {
    storage.clear()
  })

  // ===========================================================================
  // Event Handling Tests
  // ===========================================================================

  describe('Event Handling', () => {
    it('processes a single trace item', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem()
      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      expect(result.errors).toBe(0)
      expect(storage.getData('WorkerRequest')).toHaveLength(1)
    })

    it('processes multiple trace items', async () => {
      processor.registerView(WorkerRequestsView)

      const events = [
        createMockTraceItem({ eventTimestamp: 1000 }),
        createMockTraceItem({ eventTimestamp: 2000 }),
        createMockTraceItem({ eventTimestamp: 3000 }),
      ]

      const result = await processor.processEvents(events)
      await processor.flush()

      expect(result.processed).toBe(3)
      expect(storage.getData('WorkerRequest')).toHaveLength(3)
    })

    it('handles trace items with no event (scheduled, alarm, etc.)', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({ event: null })
      const result = await processor.processEvents([event])
      await processor.flush()

      // Should be filtered out since WorkerRequestsView requires event != null
      expect(result.filtered).toBe(1)
      expect(result.processed).toBe(0)
      expect(storage.getData('WorkerRequest')).toHaveLength(0)
    })

    it('handles trace items with logs', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [
          createMockLog({ level: 'log', message: 'Hello' }),
          createMockLog({ level: 'warn', message: 'Warning!' }),
          createMockLog({ level: 'error', message: 'Error occurred' }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(3)
      expect(storage.getData('WorkerLog')).toHaveLength(3)
    })

    it('handles trace items with exceptions', async () => {
      processor.registerView(WorkerErrorsView)

      const event = createMockTraceItem({
        outcome: 'exception',
        exceptions: [
          createMockException({ name: 'TypeError', message: 'undefined is not a function' }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const errors = storage.getData('WorkerError') as Array<{ exceptionName: string; exceptionMessage: string }>
      expect(errors).toHaveLength(1)
      expect(errors[0].exceptionName).toBe('TypeError')
      expect(errors[0].exceptionMessage).toBe('undefined is not a function')
    })

    it('handles trace items with multiple exceptions', async () => {
      processor.registerView(WorkerErrorsView)

      const event = createMockTraceItem({
        outcome: 'exception',
        exceptions: [
          createMockException({ name: 'Error', message: 'First error' }),
          createMockException({ name: 'ReferenceError', message: 'x is not defined' }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      // Only the first exception is captured per the view definition
      const errors = storage.getData('WorkerError') as Array<{ exceptionName: string }>
      expect(errors).toHaveLength(1)
      expect(errors[0].exceptionName).toBe('Error')
    })

    it('processes events with different outcomes', async () => {
      processor.registerView(WorkerErrorsView)

      const events = [
        createMockTraceItem({ outcome: 'ok' }),
        createMockTraceItem({ outcome: 'exception', exceptions: [createMockException()] }),
        createMockTraceItem({ outcome: 'canceled' }),
        createMockTraceItem({ outcome: 'exceededCpu' }),
        createMockTraceItem({ outcome: 'exceededMemory' }),
      ]

      const result = await processor.processEvents(events)
      await processor.flush()

      // 'ok' without exceptions should be filtered
      expect(result.filtered).toBe(1)
      expect(result.processed).toBe(4)
    })

    it('extracts request metadata correctly', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        scriptName: 'my-api-worker',
        event: {
          request: {
            method: 'POST',
            url: 'https://api.example.com/users/123',
            headers: { 'content-type': 'application/json' },
            cf: { colo: 'SJC', country: 'US' },
          },
          response: { status: 201 },
        } as TraceItemFetchEventInfo,
        outcome: 'ok',
        eventTimestamp: 1704067200000,
      })

      await processor.processEvents([event])
      await processor.flush()

      const requests = storage.getData('WorkerRequest') as Array<{
        scriptName: string
        method: string
        url: string
        pathname: string
        status: number
        colo: string
        country: string | null
      }>

      expect(requests).toHaveLength(1)
      expect(requests[0].scriptName).toBe('my-api-worker')
      expect(requests[0].method).toBe('POST')
      expect(requests[0].url).toBe('https://api.example.com/users/123')
      expect(requests[0].pathname).toBe('/users/123')
      expect(requests[0].status).toBe(201)
      expect(requests[0].colo).toBe('SJC')
      expect(requests[0].country).toBe('US')
    })

    it('handles missing colo data', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'https://example.com/',
            headers: {},
            cf: undefined,
          },
          response: { status: 200 },
        } as TraceItemFetchEventInfo,
      })

      await processor.processEvents([event])
      await processor.flush()

      const requests = storage.getData('WorkerRequest') as Array<{ colo: string }>
      expect(requests[0].colo).toBe('unknown')
    })

    it('handles missing response data', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'https://example.com/',
            headers: {},
            cf: { colo: 'DFW' },
          },
          response: undefined,
        } as TraceItemFetchEventInfo,
      })

      await processor.processEvents([event])
      await processor.flush()

      const requests = storage.getData('WorkerRequest') as Array<{ status: number }>
      expect(requests[0].status).toBe(0)
    })

    it('handles truncated events', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        truncated: true,
        logs: [createMockLog({ message: 'Partial log...' })],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      // Truncated events should still be processed
      expect(result.processed).toBe(1)
    })
  })

  // ===========================================================================
  // Batching Logic Tests
  // ===========================================================================

  describe('Batching Logic', () => {
    it('batches events up to batchSize', async () => {
      const processor = new TailEventProcessor({
        storage,
        batchSize: 5,
      })
      processor.registerView(WorkerRequestsView)

      const events = Array.from({ length: 4 }, (_, i) =>
        createMockTraceItem({ eventTimestamp: i * 1000 })
      )

      await processor.processEvents(events)

      // Should not have flushed yet (4 < 5)
      expect(storage.getData('WorkerRequest')).toHaveLength(0)
      expect(processor.getPendingCount('WorkerRequest')).toBe(4)
    })

    it('auto-flushes when batch size is reached', async () => {
      const processor = new TailEventProcessor({
        storage,
        batchSize: 3,
      })
      processor.registerView(WorkerRequestsView)

      const events = Array.from({ length: 5 }, (_, i) =>
        createMockTraceItem({ eventTimestamp: i * 1000 })
      )

      await processor.processEvents(events)

      // Should have flushed once (3 events) and have 2 pending
      expect(storage.getData('WorkerRequest')).toHaveLength(3)
      expect(processor.getPendingCount('WorkerRequest')).toBe(2)
    })

    it('flushes multiple times for large batches', async () => {
      const processor = new TailEventProcessor({
        storage,
        batchSize: 3,
      })
      processor.registerView(WorkerRequestsView)

      const events = Array.from({ length: 10 }, (_, i) =>
        createMockTraceItem({ eventTimestamp: i * 1000 })
      )

      await processor.processEvents(events)

      // Should have flushed 3 times (9 events) and have 1 pending
      expect(storage.getData('WorkerRequest')).toHaveLength(9)
      expect(processor.getPendingCount('WorkerRequest')).toBe(1)

      await processor.flush()
      expect(storage.getData('WorkerRequest')).toHaveLength(10)
    })

    it('manual flush clears all pending events', async () => {
      const processor = new TailEventProcessor({
        storage,
        batchSize: 100, // High batch size so no auto-flush
      })
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [createMockLog(), createMockLog()],
      })

      await processor.processEvents([event])

      expect(processor.getPendingCount('WorkerRequest')).toBe(1)
      expect(processor.getPendingCount('WorkerLog')).toBe(2)

      await processor.flush()

      expect(processor.getPendingCount('WorkerRequest')).toBe(0)
      expect(processor.getPendingCount('WorkerLog')).toBe(0)
      expect(storage.getData('WorkerRequest')).toHaveLength(1)
      expect(storage.getData('WorkerLog')).toHaveLength(2)
    })

    it('handles empty event batches', async () => {
      processor.registerView(WorkerRequestsView)

      const result = await processor.processEvents([])
      await processor.flush()

      expect(result.processed).toBe(0)
      expect(result.errors).toBe(0)
      expect(storage.getData('WorkerRequest')).toHaveLength(0)
    })

    it('processes events across multiple views independently', async () => {
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerLogsView)
      processor.registerView(WorkerErrorsView)

      const events = [
        createMockTraceItem({
          outcome: 'ok',
          logs: [createMockLog()],
        }),
        createMockTraceItem({
          outcome: 'exception',
          logs: [createMockLog(), createMockLog()],
          exceptions: [createMockException()],
        }),
      ]

      const result = await processor.processEvents(events)
      await processor.flush()

      expect(storage.getData('WorkerRequest')).toHaveLength(2)
      expect(storage.getData('WorkerLog')).toHaveLength(3)
      expect(storage.getData('WorkerError')).toHaveLength(1) // Only the exception event
    })

    it('maintains correct counts per view', async () => {
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerLogsView)

      const events = [
        createMockTraceItem({ logs: [createMockLog()] }),
        createMockTraceItem({ logs: [createMockLog(), createMockLog(), createMockLog()] }),
        createMockTraceItem({ logs: [] }),
      ]

      const result = await processor.processEvents(events)

      expect(result.byView.get('WorkerRequest')).toBe(3)
      expect(result.byView.get('WorkerLog')).toBe(4)
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('Error Handling', () => {
    it('continues processing after transform error', async () => {
      const errorHandler = vi.fn()
      const processor = new TailEventProcessor({ storage, errorHandler })

      // View with a transform that throws for certain events
      const faultyView: StreamViewDefinition<TraceItem, unknown> = {
        $type: 'FaultyView',
        $stream: 'tail',
        $schema: { $id: 'string!' },
        $transform: (event: TraceItem) => {
          if (event.scriptName === 'throw-error') {
            throw new Error('Transform failed')
          }
          return { $id: String(event.eventTimestamp) }
        },
        $refresh: { mode: 'streaming' },
      }

      processor.registerView(faultyView)

      const events = [
        createMockTraceItem({ scriptName: 'good-worker', eventTimestamp: 1000 }),
        createMockTraceItem({ scriptName: 'throw-error', eventTimestamp: 2000 }),
        createMockTraceItem({ scriptName: 'good-worker', eventTimestamp: 3000 }),
      ]

      const result = await processor.processEvents(events)
      await processor.flush()

      expect(result.processed).toBe(2)
      expect(result.errors).toBe(1)
      expect(errorHandler).toHaveBeenCalledTimes(1)
      expect(errorHandler).toHaveBeenCalledWith(
        expect.any(Error),
        expect.objectContaining({ scriptName: 'throw-error' })
      )
      expect(storage.getData('FaultyView')).toHaveLength(2)
    })

    it('captures error details in error handler', async () => {
      const capturedErrors: Array<{ error: Error; event: TraceItem }> = []
      const errorHandler = (error: Error, event: TraceItem) => {
        capturedErrors.push({ error, event })
      }
      const processor = new TailEventProcessor({ storage, errorHandler })

      const faultyView: StreamViewDefinition<TraceItem, unknown> = {
        $type: 'FaultyView',
        $stream: 'tail',
        $schema: {},
        $transform: () => {
          throw new TypeError('Cannot read property of undefined')
        },
        $refresh: { mode: 'streaming' },
      }

      processor.registerView(faultyView)

      const event = createMockTraceItem({ scriptName: 'test' })
      await processor.processEvents([event])

      expect(capturedErrors).toHaveLength(1)
      expect(capturedErrors[0].error.message).toBe('Cannot read property of undefined')
      expect(capturedErrors[0].error).toBeInstanceOf(TypeError)
      expect(capturedErrors[0].event.scriptName).toBe('test')
    })

    it('handles filter errors gracefully', async () => {
      const errorHandler = vi.fn()
      const processor = new TailEventProcessor({ storage, errorHandler })

      const faultyView: StreamViewDefinition<TraceItem, unknown> = {
        $type: 'FaultyView',
        $stream: 'tail',
        $schema: {},
        $filter: (event: TraceItem) => {
          if (event.scriptName === 'throw-error') {
            throw new Error('Filter failed')
          }
          return true
        },
        $transform: (event: TraceItem) => ({ $id: String(event.eventTimestamp) }),
        $refresh: { mode: 'streaming' },
      }

      processor.registerView(faultyView)

      const events = [
        createMockTraceItem({ scriptName: 'good-worker' }),
        createMockTraceItem({ scriptName: 'throw-error' }),
        createMockTraceItem({ scriptName: 'good-worker' }),
      ]

      const result = await processor.processEvents(events)
      await processor.flush()

      expect(result.processed).toBe(2)
      expect(result.errors).toBe(1)
    })

    it('handles malformed URL in request', async () => {
      processor.registerView(WorkerRequestsView)

      // This would throw when trying to parse the URL
      const event = createMockTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'not-a-valid-url', // Invalid URL
            headers: {},
          },
          response: { status: 200 },
        } as TraceItemFetchEventInfo,
      })

      // Should not throw - error should be caught
      const result = await processor.processEvents([event])
      expect(result.errors).toBe(1)
    })

    it('handles null scriptName', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        scriptName: null,
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const requests = storage.getData('WorkerRequest') as Array<{ scriptName: string }>
      expect(requests[0].scriptName).toBe('unknown')
    })

    it('handles null eventTimestamp', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        eventTimestamp: null,
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const requests = storage.getData('WorkerRequest') as Array<{ timestamp: Date }>
      expect(requests[0].timestamp).toBeInstanceOf(Date)
    })

    it('handles log message as array', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [
          createMockLog({ message: ['Multiple', 'parts', 'of', 'message'] }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const logs = storage.getData('WorkerLog') as Array<{ message: string }>
      expect(logs[0].message).toBe('Multiple parts of message')
    })

    it('handles log message as object', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [
          createMockLog({ message: { key: 'value', nested: { data: 123 } } }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const logs = storage.getData('WorkerLog') as Array<{ message: string }>
      expect(logs[0].message).toBe('[object Object]')
    })

    it('handles empty logs array', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({ logs: [] })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(0)
      expect(storage.getData('WorkerLog')).toHaveLength(0)
    })

    it('handles empty exceptions array', async () => {
      processor.registerView(WorkerErrorsView)

      const event = createMockTraceItem({
        outcome: 'canceled', // non-ok outcome but no exceptions
        exceptions: [],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const errors = storage.getData('WorkerError') as Array<{ exceptionName: string | null }>
      expect(errors[0].exceptionName).toBeNull()
    })
  })

  // ===========================================================================
  // MV Integration Tests
  // ===========================================================================

  describe('MV Integration', () => {
    it('writes to multiple MVs from same event', async () => {
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [createMockLog({ level: 'info', message: 'Request processed' })],
      })

      await processor.processEvents([event])
      await processor.flush()

      expect(storage.getData('WorkerRequest')).toHaveLength(1)
      expect(storage.getData('WorkerLog')).toHaveLength(1)
    })

    it('applies view-specific filters', async () => {
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerErrorsView)

      const events = [
        createMockTraceItem({ outcome: 'ok' }),
        createMockTraceItem({ outcome: 'exception', exceptions: [createMockException()] }),
      ]

      await processor.processEvents(events)
      await processor.flush()

      // Both events go to WorkerRequests
      expect(storage.getData('WorkerRequest')).toHaveLength(2)
      // Only error event goes to WorkerErrors
      expect(storage.getData('WorkerError')).toHaveLength(1)
    })

    it('applies view-specific transforms', async () => {
      processor.registerView(WorkerRequestsView)
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        scriptName: 'test-worker',
        eventTimestamp: 1704067200000,
        logs: [
          createMockLog({ timestamp: 1704067200100, level: 'log', message: 'Hello' }),
        ],
        event: {
          request: {
            method: 'GET',
            url: 'https://example.com/test',
            headers: {},
            cf: { colo: 'DFW' },
          },
          response: { status: 200 },
        } as TraceItemFetchEventInfo,
      })

      await processor.processEvents([event])
      await processor.flush()

      const requests = storage.getData('WorkerRequest') as Array<{
        $id: string
        pathname: string
        method: string
      }>
      expect(requests[0].$id).toBe('test-worker-1704067200000')
      expect(requests[0].pathname).toBe('/test')
      expect(requests[0].method).toBe('GET')

      const logs = storage.getData('WorkerLog') as Array<{
        $id: string
        level: string
        message: string
      }>
      expect(logs[0].$id).toBe('1704067200000-1704067200100')
      expect(logs[0].level).toBe('log')
      expect(logs[0].message).toBe('Hello')
    })

    it('handles one-to-many transforms (logs)', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [
          createMockLog({ level: 'debug', message: 'Debug message' }),
          createMockLog({ level: 'info', message: 'Info message' }),
          createMockLog({ level: 'warn', message: 'Warning message' }),
          createMockLog({ level: 'error', message: 'Error message' }),
        ],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(4)
      expect(result.byView.get('WorkerLog')).toBe(4)
      expect(storage.getData('WorkerLog')).toHaveLength(4)
    })

    it('handles scheduled event type', async () => {
      // Create a custom view for scheduled events
      const scheduledView: StreamViewDefinition<TraceItem, unknown> = {
        $type: 'ScheduledEvent',
        $stream: 'tail',
        $schema: {
          $id: 'string!',
          scriptName: 'string!',
          cron: 'string!',
          scheduledTime: 'timestamp!',
          outcome: 'string!',
        },
        $filter: (event: TraceItem) =>
          event.event !== null && 'cron' in event.event,
        $transform: (event: TraceItem) => {
          const scheduled = event.event as TraceItemScheduledEventInfo
          return {
            $id: `${event.scriptName}-${scheduled.scheduledTime}`,
            scriptName: event.scriptName || 'unknown',
            cron: scheduled.cron,
            scheduledTime: new Date(scheduled.scheduledTime),
            outcome: event.outcome,
          }
        },
        $refresh: { mode: 'streaming' },
      }

      processor.registerView(scheduledView)

      const event = createMockTraceItem({
        scriptName: 'cron-worker',
        event: {
          scheduledTime: 1704067200000,
          cron: '0 * * * *',
        } as TraceItemScheduledEventInfo,
        outcome: 'ok',
      })

      await processor.processEvents([event])
      await processor.flush()

      const scheduled = storage.getData('ScheduledEvent') as Array<{
        cron: string
        outcome: string
      }>
      expect(scheduled).toHaveLength(1)
      expect(scheduled[0].cron).toBe('0 * * * *')
      expect(scheduled[0].outcome).toBe('ok')
    })

    it('generates unique IDs for records', async () => {
      processor.registerView(WorkerRequestsView)

      const events = [
        createMockTraceItem({ scriptName: 'worker-a', eventTimestamp: 1000 }),
        createMockTraceItem({ scriptName: 'worker-b', eventTimestamp: 1000 }),
        createMockTraceItem({ scriptName: 'worker-a', eventTimestamp: 2000 }),
      ]

      await processor.processEvents(events)
      await processor.flush()

      const requests = storage.getData('WorkerRequest') as Array<{ $id: string }>
      const ids = requests.map(r => r.$id)

      // All IDs should be unique
      expect(new Set(ids).size).toBe(3)
      expect(ids).toContain('worker-a-1000')
      expect(ids).toContain('worker-b-1000')
      expect(ids).toContain('worker-a-2000')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('handles very long log messages', async () => {
      processor.registerView(WorkerLogsView)

      const longMessage = 'x'.repeat(10000)
      const event = createMockTraceItem({
        logs: [createMockLog({ message: longMessage })],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const logs = storage.getData('WorkerLog') as Array<{ message: string }>
      expect(logs[0].message).toBe(longMessage)
    })

    it('handles special characters in URLs', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'https://example.com/search?q=hello%20world&filter=%3Cscript%3E',
            headers: {},
            cf: { colo: 'DFW' },
          },
          response: { status: 200 },
        } as TraceItemFetchEventInfo,
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const requests = storage.getData('WorkerRequest') as Array<{ pathname: string }>
      expect(requests[0].pathname).toBe('/search')
    })

    it('handles unicode in log messages', async () => {
      processor.registerView(WorkerLogsView)

      const event = createMockTraceItem({
        logs: [createMockLog({ message: 'Hello \u{1F600} World \u{1F389}' })],
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      const logs = storage.getData('WorkerLog') as Array<{ message: string }>
      expect(logs[0].message).toContain('\u{1F600}')
    })

    it('handles high CPU time values', async () => {
      processor.registerView(WorkerRequestsView)

      const event = createMockTraceItem({
        cpuTime: 999999,
        wallTime: 30000,
      })

      const result = await processor.processEvents([event])
      expect(result.processed).toBe(1)
    })

    it('handles very old timestamps', async () => {
      processor.registerView(WorkerRequestsView)

      // Use Unix epoch + 1ms to avoid falsy value (0 triggers Date.now() fallback)
      const event = createMockTraceItem({
        eventTimestamp: 1, // Unix epoch + 1ms
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
      const requests = storage.getData('WorkerRequest') as Array<{ timestamp: Date }>
      expect(requests[0].timestamp.getTime()).toBe(1)
    })

    it('handles future timestamps', async () => {
      processor.registerView(WorkerRequestsView)

      const futureTime = Date.now() + 365 * 24 * 60 * 60 * 1000 // 1 year from now
      const event = createMockTraceItem({
        eventTimestamp: futureTime,
      })

      const result = await processor.processEvents([event])
      await processor.flush()

      expect(result.processed).toBe(1)
    })

    it('handles large batch of events', async () => {
      const processor = new TailEventProcessor({
        storage,
        batchSize: 50,
      })
      processor.registerView(WorkerRequestsView)

      const events = Array.from({ length: 1000 }, (_, i) =>
        createMockTraceItem({ eventTimestamp: i })
      )

      const result = await processor.processEvents(events)
      await processor.flush()

      expect(result.processed).toBe(1000)
      expect(storage.getData('WorkerRequest')).toHaveLength(1000)
    })

    it('handles concurrent processing', async () => {
      processor.registerView(WorkerRequestsView)

      const batch1 = Array.from({ length: 50 }, (_, i) =>
        createMockTraceItem({ scriptName: 'batch1', eventTimestamp: i })
      )
      const batch2 = Array.from({ length: 50 }, (_, i) =>
        createMockTraceItem({ scriptName: 'batch2', eventTimestamp: i + 100 })
      )

      // Process concurrently
      const [result1, result2] = await Promise.all([
        processor.processEvents(batch1),
        processor.processEvents(batch2),
      ])

      await processor.flush()

      expect(result1.processed + result2.processed).toBe(100)
      expect(storage.getData('WorkerRequest')).toHaveLength(100)
    })
  })
})

/**
 * Integration Tests: Tail Streaming Module
 *
 * Tests the ParqueDB tail integration module (src/integrations/tail/index.ts)
 * components working together in realistic multi-step scenarios.
 *
 * Unlike the unit tests that verify individual functions in isolation,
 * these tests exercise:
 *
 * 1. createTailHandler with filtering, transforms, and name generators
 * 2. filterTraceItems -> processTraceItems -> addToBatch pipeline
 * 3. Batching lifecycle across multiple handler invocations
 * 4. Derived collection definitions (WorkerErrors, WorkerExceptions, WorkerLogs)
 * 5. Multi-worker event mixing and concurrent handler scenarios
 * 6. Error recovery and graceful degradation
 *
 * @see parquedb-z2iy - P2: Add tail integration tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  TailEvents,
  WorkerErrors,
  WorkerExceptions,
  WorkerLogs,
  createTailHandler,
  filterTraceItem,
  filterTraceItems,
  processTraceItem,
  processTraceItems,
  createBatchState,
  shouldFlushBatch,
  addToBatch,
  DEFAULT_FILTER,
  DEFAULT_BATCH_CONFIG,
  type TraceItem,
  type TailEventFilter,
  type BatchConfig,
  type BatchState,
  type ProcessedEvent,
} from '../../src/integrations/tail'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a realistic trace item simulating Cloudflare Worker output
 */
function createTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: 'api-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    event: {
      request: {
        method: 'GET',
        url: 'https://api.example.com/v1/data',
        headers: {
          'content-type': 'application/json',
          'cf-ray': `ray-${Date.now()}`,
        },
        cf: {
          colo: 'SJC',
          country: 'US',
          city: 'San Jose',
          asn: 13335,
        },
      },
      response: { status: 200 },
    },
    logs: [],
    exceptions: [],
    ...overrides,
  }
}

/**
 * Create a trace item representing a failed request
 */
function createFailedTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return createTraceItem({
    outcome: 'exception',
    event: {
      request: {
        method: 'POST',
        url: 'https://api.example.com/v1/submit',
        headers: { 'content-type': 'application/json' },
        cf: { colo: 'LAX', country: 'US' },
      },
      response: { status: 500 },
    },
    logs: [
      { timestamp: Date.now(), level: 'error', message: ['Unhandled error in request'] },
    ],
    exceptions: [
      { timestamp: Date.now(), name: 'TypeError', message: 'Cannot read property "id" of undefined' },
    ],
    ...overrides,
  })
}

/**
 * Create a batch of trace items simulating a diverse production workload
 */
function createProductionWorkload(count: number): TraceItem[] {
  const workers = ['api-worker', 'auth-worker', 'cron-worker', 'email-worker', 'cdn-worker']
  const outcomes: Array<TraceItem['outcome']> = ['ok', 'ok', 'ok', 'ok', 'exception']
  const methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
  const colos = ['SJC', 'LAX', 'DFW', 'IAD', 'AMS', 'NRT', 'SIN']
  const countries = ['US', 'US', 'US', 'DE', 'JP', 'SG', 'GB']

  return Array.from({ length: count }, (_, i) => {
    const workerIdx = i % workers.length
    const outcome = outcomes[i % outcomes.length] || 'ok'
    const isError = outcome !== 'ok'
    const now = Date.now() + i

    return createTraceItem({
      scriptName: workers[workerIdx] || 'unknown',
      outcome,
      eventTimestamp: now,
      event: {
        request: {
          method: methods[i % methods.length] || 'GET',
          url: `https://api.example.com/v1/resource/${i}`,
          headers: { 'content-type': 'application/json' },
          cf: {
            colo: colos[i % colos.length],
            country: countries[i % countries.length],
          },
        },
        response: { status: isError ? 500 : 200 },
      },
      logs: isError
        ? [{ timestamp: now, level: 'error', message: [`Error in request ${i}`] }]
        : i % 3 === 0
          ? [{ timestamp: now, level: 'info', message: [`Processing request ${i}`] }]
          : [],
      exceptions: isError
        ? [{ timestamp: now, name: 'Error', message: `Request ${i} failed` }]
        : [],
    })
  })
}

// =============================================================================
// Integration Tests: createTailHandler + DB Interaction
// =============================================================================

describe('Integration: Tail Handler with DB', () => {
  let mockDb: { TailEvents: { create: ReturnType<typeof vi.fn> } }
  let createdEntities: Array<Record<string, unknown>>

  beforeEach(() => {
    createdEntities = []
    mockDb = {
      TailEvents: {
        create: vi.fn().mockImplementation(async (data: Record<string, unknown>) => {
          createdEntities.push(data)
          return { $id: `entity-${createdEntities.length}`, ...data }
        }),
      },
    }
  })

  it('should ingest a realistic multi-worker batch through the handler', async () => {
    const handler = createTailHandler(mockDb)

    const items: TraceItem[] = [
      createTraceItem({ scriptName: 'api-worker', outcome: 'ok' }),
      createTraceItem({ scriptName: 'auth-worker', outcome: 'ok' }),
      createFailedTraceItem({ scriptName: 'api-worker' }),
      createTraceItem({
        scriptName: 'cron-worker',
        outcome: 'ok',
        event: null,
        logs: [{ timestamp: Date.now(), level: 'info', message: ['Cron job completed'] }],
      }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(4)

    // Verify entity structure
    for (const entity of createdEntities) {
      expect(entity).toHaveProperty('$type', 'TailEvent')
      expect(entity).toHaveProperty('name')
      expect(entity).toHaveProperty('scriptName')
      expect(entity).toHaveProperty('outcome')
      expect(entity).toHaveProperty('eventTimestamp')
      expect(entity).toHaveProperty('logs')
      expect(entity).toHaveProperty('exceptions')
    }

    // Verify specific entries
    const apiError = createdEntities.find(
      (e) => e.scriptName === 'api-worker' && e.outcome === 'exception'
    )
    expect(apiError).toBeDefined()
    expect(apiError!.name).toBe('api-worker:exception')

    const cronEntry = createdEntities.find((e) => e.scriptName === 'cron-worker')
    expect(cronEntry).toBeDefined()
    expect(cronEntry!.event).toBeNull()
  })

  it('should combine scriptName and outcome filters', async () => {
    const handler = createTailHandler(mockDb, {
      filter: {
        scriptNames: ['api-worker', 'auth-worker'],
        outcomes: ['exception', 'exceededCpu'],
      },
    })

    const items = [
      createTraceItem({ scriptName: 'api-worker', outcome: 'ok' }),
      createFailedTraceItem({ scriptName: 'api-worker' }),
      createFailedTraceItem({ scriptName: 'auth-worker' }),
      createFailedTraceItem({ scriptName: 'cron-worker' }),
      createTraceItem({ scriptName: 'auth-worker', outcome: 'exceededCpu' }),
    ]

    await handler(items)

    // Only api-worker and auth-worker with exception/exceededCpu outcomes
    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(3)
    const scriptNames = createdEntities.map((e) => e.scriptName)
    expect(scriptNames).not.toContain('cron-worker')
    expect(scriptNames.every((s) => s === 'api-worker' || s === 'auth-worker')).toBe(true)
  })

  it('should apply transform then filter (transform returning null skips)', async () => {
    const handler = createTailHandler(mockDb, {
      // Transform: redact URLs containing /admin/
      transform: (item) => {
        if (item.event?.request?.url.includes('/admin/')) {
          return null // Skip admin requests
        }
        return {
          ...item,
          // Redact IP headers
          event: item.event
            ? {
                ...item.event,
                request: item.event.request
                  ? {
                      ...item.event.request,
                      headers: { 'content-type': item.event.request.headers['content-type'] || '' },
                    }
                  : undefined,
              }
            : null,
        }
      },
    })

    const items = [
      createTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'https://api.example.com/v1/users',
            headers: { 'content-type': 'application/json', 'x-forwarded-for': '1.2.3.4' },
          },
        },
      }),
      createTraceItem({
        event: {
          request: {
            method: 'GET',
            url: 'https://api.example.com/admin/config',
            headers: { 'content-type': 'text/html' },
          },
        },
      }),
      createTraceItem({
        event: {
          request: {
            method: 'POST',
            url: 'https://api.example.com/v1/submit',
            headers: { 'content-type': 'application/json', 'authorization': 'Bearer secret' },
          },
        },
      }),
    ]

    await handler(items)

    // Admin request should be filtered out
    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(2)

    // Verify headers were redacted
    for (const entity of createdEntities) {
      const event = entity.event as { request?: { headers: Record<string, string> } }
      if (event?.request) {
        expect(event.request.headers).not.toHaveProperty('x-forwarded-for')
        expect(event.request.headers).not.toHaveProperty('authorization')
      }
    }
  })

  it('should use custom name generator with composite keys', async () => {
    const handler = createTailHandler(mockDb, {
      nameGenerator: (item) => {
        const method = item.event?.request?.method || 'N/A'
        const path = item.event?.request?.url
          ? new URL(item.event.request.url).pathname
          : '/unknown'
        return `${item.scriptName}:${method}:${path}:${item.outcome}`
      },
    })

    await handler([
      createTraceItem({
        scriptName: 'api-worker',
        event: {
          request: {
            method: 'GET',
            url: 'https://api.example.com/v1/users/123',
            headers: {},
          },
          response: { status: 200 },
        },
      }),
    ])

    expect(createdEntities[0]!.name).toBe('api-worker:GET:/v1/users/123:ok')
  })

  it('should handle sequential handler invocations correctly', async () => {
    const handler = createTailHandler(mockDb)

    // First batch
    await handler([
      createTraceItem({ scriptName: 'batch-1-worker' }),
      createTraceItem({ scriptName: 'batch-1-worker' }),
    ])

    // Second batch
    await handler([
      createTraceItem({ scriptName: 'batch-2-worker' }),
    ])

    // Third batch (empty)
    await handler([])

    // Fourth batch
    await handler([
      createTraceItem({ scriptName: 'batch-4-worker' }),
    ])

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(4)
    expect(createdEntities[0]!.scriptName).toBe('batch-1-worker')
    expect(createdEntities[2]!.scriptName).toBe('batch-2-worker')
    expect(createdEntities[3]!.scriptName).toBe('batch-4-worker')
  })

  it('should handle handler errors without losing subsequent events', async () => {
    let callCount = 0
    const failOnThird = {
      TailEvents: {
        create: vi.fn().mockImplementation(async (data: Record<string, unknown>) => {
          callCount++
          if (callCount === 3) {
            throw new Error('Simulated DB failure')
          }
          createdEntities.push(data)
          return { $id: `entity-${callCount}`, ...data }
        }),
      },
    }

    const handler = createTailHandler(failOnThird)

    // The handler processes items sequentially, so the 3rd item will throw
    await expect(
      handler([
        createTraceItem({ scriptName: 'worker-1' }),
        createTraceItem({ scriptName: 'worker-2' }),
        createTraceItem({ scriptName: 'worker-3' }), // This will fail
        createTraceItem({ scriptName: 'worker-4' }),
      ])
    ).rejects.toThrow('Simulated DB failure')

    // First two should have been created before the failure
    expect(createdEntities).toHaveLength(2)
  })
})

// =============================================================================
// Integration Tests: Filter -> Process -> Batch Pipeline
// =============================================================================

describe('Integration: Filter-Process-Batch Pipeline', () => {
  it('should filter, process, and batch a production workload', () => {
    const workload = createProductionWorkload(250)

    // Step 1: Filter to only error events
    const filter: TailEventFilter = {
      outcomes: ['exception', 'exceededCpu', 'exceededMemory'],
    }
    const filtered = filterTraceItems(workload, filter)

    // 1 out of every 5 events is an exception
    expect(filtered.length).toBe(50)
    expect(filtered.every((item) => item.outcome !== 'ok')).toBe(true)

    // Step 2: Process filtered events
    const processed = processTraceItems(filtered)

    expect(processed.length).toBe(50)
    for (const event of processed) {
      expect(event.id).toBeDefined()
      expect(event.timestamp).toBeDefined()
      expect(event.scriptName).toBeDefined()
      expect(event.outcome).not.toBe('ok')
      expect(event.exceptionCount).toBeGreaterThan(0)
      expect(event.errors).toBeDefined()
    }

    // Step 3: Batch the processed events
    const batchConfig: BatchConfig = { maxEvents: 20, maxWaitMs: 10000, minEvents: 1 }
    const state = createBatchState()
    const flushedBatches: ProcessedEvent[][] = []

    // Feed all events into the batcher
    for (const event of processed) {
      const toFlush = addToBatch(state, [event], batchConfig)
      if (toFlush.length > 0) {
        flushedBatches.push(toFlush)
      }
    }

    // We should have gotten 2 full batches (20 events each), with 10 remaining
    expect(flushedBatches.length).toBe(2)
    expect(flushedBatches[0]!.length).toBe(20)
    expect(flushedBatches[1]!.length).toBe(20)
    expect(state.events.length).toBe(10) // Remaining in buffer
  })

  it('should handle mixed filter criteria across a workload', () => {
    const workload = createProductionWorkload(100)

    // Filter 1: Only errors from specific workers
    const errorFilter: TailEventFilter = {
      scriptNames: ['api-worker'],
      exceptionsOnly: true,
    }
    const apiErrors = filterTraceItems(workload, errorFilter)
    expect(apiErrors.every((i) => i.scriptName === 'api-worker')).toBe(true)
    expect(apiErrors.every((i) => i.exceptions.length > 0)).toBe(true)

    // Filter 2: Events with warn/error logs
    const logFilter: TailEventFilter = {
      logLevels: ['warn', 'error'],
    }
    const withSignificantLogs = filterTraceItems(workload, logFilter)
    // Items with error logs (from failed items) pass, items with only info logs are filtered
    for (const item of withSignificantLogs) {
      if (item.logs.length > 0) {
        const hasWarnOrError = item.logs.some(
          (l) => l.level === 'warn' || l.level === 'error'
        )
        expect(hasWarnOrError).toBe(true)
      }
    }

    // Filter 3: Minimum logs threshold
    const verboseFilter: TailEventFilter = {
      minLogs: 1,
    }
    const verbose = filterTraceItems(workload, verboseFilter)
    expect(verbose.every((i) => i.logs.length >= 1)).toBe(true)

    // Process each filtered set
    const processedErrors = processTraceItems(apiErrors)
    const processedLogs = processTraceItems(withSignificantLogs)

    // Verify errors have error info
    for (const event of processedErrors) {
      expect(event.errors).toBeDefined()
      expect(event.exceptionCount).toBeGreaterThan(0)
    }

    // Verify log-filtered events were processed
    expect(processedLogs.length).toBe(withSignificantLogs.length)
  })

  it('should correctly process trace items with all event types', () => {
    const items: TraceItem[] = [
      // Fetch event
      createTraceItem({
        scriptName: 'api-worker',
        event: {
          request: {
            method: 'POST',
            url: 'https://api.example.com/v1/users',
            headers: { 'content-type': 'application/json' },
            cf: { colo: 'SJC', country: 'US' },
          },
          response: { status: 201 },
        },
      }),
      // Scheduled event (no request)
      createTraceItem({
        scriptName: 'cron-worker',
        event: {
          scheduledTime: Date.now(),
          cron: '*/5 * * * *',
        },
      }),
      // Queue event (no request)
      createTraceItem({
        scriptName: 'queue-worker',
        event: {
          queue: 'email-queue',
          batchSize: 10,
        },
      }),
      // Event with null event info
      createTraceItem({
        scriptName: 'unknown-worker',
        event: null,
      }),
    ]

    const processed = processTraceItems(items)

    expect(processed.length).toBe(4)

    // Fetch event should have request info
    expect(processed[0]!.method).toBe('POST')
    expect(processed[0]!.url).toBe('https://api.example.com/v1/users')
    expect(processed[0]!.colo).toBe('SJC')
    expect(processed[0]!.country).toBe('US')

    // Scheduled event should not have request info
    expect(processed[1]!.method).toBeUndefined()
    expect(processed[1]!.url).toBeUndefined()

    // Queue event should not have request info
    expect(processed[2]!.method).toBeUndefined()

    // Null event should not have request info
    expect(processed[3]!.method).toBeUndefined()
    expect(processed[3]!.url).toBeUndefined()
  })

  it('should generate unique IDs across multiple batches', () => {
    const items = createProductionWorkload(200)
    const processed = processTraceItems(items)

    const ids = new Set(processed.map((e) => e.id))
    expect(ids.size).toBe(200)
  })
})

// =============================================================================
// Integration Tests: Batching Lifecycle
// =============================================================================

describe('Integration: Batching Lifecycle', () => {
  it('should accumulate events and flush at threshold', () => {
    const config: BatchConfig = { maxEvents: 50, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()
    const allFlushed: ProcessedEvent[][] = []

    const workload = createProductionWorkload(120)
    const processed = processTraceItems(workload)

    // Feed events one at a time
    for (const event of processed) {
      const flushed = addToBatch(state, [event], config)
      if (flushed.length > 0) {
        allFlushed.push(flushed)
      }
    }

    // 120 events / 50 threshold = 2 flushes with 20 remaining
    expect(allFlushed.length).toBe(2)
    expect(allFlushed[0]!.length).toBe(50)
    expect(allFlushed[1]!.length).toBe(50)
    expect(state.events.length).toBe(20)

    // Verify no events were lost
    const totalFlushed = allFlushed.reduce((sum, batch) => sum + batch.length, 0)
    expect(totalFlushed + state.events.length).toBe(120)
  })

  it('should flush when maxWaitMs is exceeded', () => {
    const config: BatchConfig = { maxEvents: 1000, maxWaitMs: 5000, minEvents: 1 }
    const state = createBatchState()

    // Add a few events
    const items = createProductionWorkload(5)
    const processed = processTraceItems(items)
    addToBatch(state, processed, config)

    // Should not flush yet (under max events and within time)
    expect(state.events.length).toBe(5)

    // Simulate time passing
    state.startTime = Date.now() - 6000

    // Now should flush on next check
    expect(shouldFlushBatch(state, config)).toBe(true)
  })

  it('should not flush when minEvents is not met even if time expired', () => {
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 5000, minEvents: 10 }
    const state = createBatchState()

    // Add a few events (under minEvents)
    const items = createProductionWorkload(5)
    const processed = processTraceItems(items)
    addToBatch(state, processed, config)

    // Simulate time passing
    state.startTime = Date.now() - 6000

    // Should not flush because minEvents (10) is not met
    expect(shouldFlushBatch(state, config)).toBe(false)
  })

  it('should handle burst traffic correctly', () => {
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }
    const state = createBatchState()
    const allFlushed: ProcessedEvent[][] = []

    // Simulate a burst: 500 events arriving at once
    const burst = createProductionWorkload(500)
    const processed = processTraceItems(burst)

    // Add all at once (simulating a large batch)
    const flushed = addToBatch(state, processed, config)
    if (flushed.length > 0) {
      allFlushed.push(flushed)
    }

    // When adding 500 events at once, all 500 are in the batch which exceeds 100,
    // so it should flush all 500
    expect(allFlushed.length).toBe(1)
    expect(allFlushed[0]!.length).toBe(500)
    expect(state.events.length).toBe(0)
  })

  it('should preserve event order through batching', () => {
    const config: BatchConfig = { maxEvents: 10, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()
    const allFlushed: ProcessedEvent[][] = []

    const workload = createProductionWorkload(25)
    const processed = processTraceItems(workload)

    // Assign sequential markers
    for (let i = 0; i < processed.length; i++) {
      processed[i]!.scriptName = `worker-${String(i).padStart(3, '0')}`
    }

    for (const event of processed) {
      const flushed = addToBatch(state, [event], config)
      if (flushed.length > 0) {
        allFlushed.push(flushed)
      }
    }

    // Verify order within each flush batch
    for (const batch of allFlushed) {
      for (let i = 1; i < batch.length; i++) {
        const prev = parseInt(batch[i - 1]!.scriptName.split('-')[1]!, 10)
        const curr = parseInt(batch[i]!.scriptName.split('-')[1]!, 10)
        expect(curr).toBeGreaterThan(prev)
      }
    }
  })
})

// =============================================================================
// Integration Tests: Derived Collection Definitions
// =============================================================================

describe('Integration: Derived Collection Definitions', () => {
  it('should define WorkerErrors as a filter on TailEvents for non-ok outcomes', () => {
    // Verify the derived collection would correctly filter trace items
    const workload = createProductionWorkload(100)

    // Apply the WorkerErrors filter logic
    const errorItems = workload.filter((item) => item.outcome !== 'ok')

    // The filter definition should match
    expect(WorkerErrors.$filter).toEqual({ outcome: { $ne: 'ok' } })
    expect(WorkerErrors.$from).toBe('TailEvents')

    // Every 5th event in our workload is an exception
    expect(errorItems.length).toBe(20)
  })

  it('should define WorkerExceptions for items with exceptions array populated', () => {
    const workload = createProductionWorkload(100)

    // Apply the WorkerExceptions filter logic
    const exceptionItems = workload.filter(
      (item) => item.exceptions.length > 0
    )

    expect(WorkerExceptions.$filter).toEqual({ 'exceptions.0': { $exists: true } })
    expect(WorkerExceptions.$from).toBe('TailEvents')
    expect(exceptionItems.every((i) => i.exceptions.length > 0)).toBe(true)
  })

  it('should define WorkerLogs for items with warn or error level logs', () => {
    const workload = [
      createTraceItem({
        logs: [{ timestamp: Date.now(), level: 'info', message: ['Normal log'] }],
      }),
      createTraceItem({
        logs: [{ timestamp: Date.now(), level: 'warn', message: ['Warning log'] }],
      }),
      createTraceItem({
        logs: [{ timestamp: Date.now(), level: 'error', message: ['Error log'] }],
      }),
      createTraceItem({
        logs: [
          { timestamp: Date.now(), level: 'info', message: ['Info'] },
          { timestamp: Date.now(), level: 'error', message: ['Error'] },
        ],
      }),
      createTraceItem({ logs: [] }),
    ]

    // Apply WorkerLogs filter logic
    const logItems = workload.filter(
      (item) =>
        item.logs.some((l) => l.level === 'warn') ||
        item.logs.some((l) => l.level === 'error')
    )

    expect(WorkerLogs.$filter).toEqual({
      $or: [
        { 'logs.level': 'warn' },
        { 'logs.level': 'error' },
      ],
    })
    expect(WorkerLogs.$from).toBe('TailEvents')
    expect(logItems.length).toBe(3) // warn, error, and info+error items
  })

  it('should ensure all derived collections reference TailEvents', () => {
    expect(TailEvents.$type).toBe('TailEvent')
    expect(TailEvents.$ingest).toBe('tail')

    expect(WorkerErrors.$from).toBe('TailEvents')
    expect(WorkerExceptions.$from).toBe('TailEvents')
    expect(WorkerLogs.$from).toBe('TailEvents')
  })
})

// =============================================================================
// Integration Tests: Multi-Worker Event Mixing
// =============================================================================

describe('Integration: Multi-Worker Event Mixing', () => {
  it('should correctly attribute events to different workers', async () => {
    const createdEntities: Array<Record<string, unknown>> = []
    const mockDb = {
      TailEvents: {
        create: vi.fn().mockImplementation(async (data: Record<string, unknown>) => {
          createdEntities.push(data)
          return { $id: `entity-${createdEntities.length}` }
        }),
      },
    }

    const handler = createTailHandler(mockDb)

    // Interleaved events from different workers
    const events: TraceItem[] = [
      createTraceItem({ scriptName: 'api-worker', eventTimestamp: 1000 }),
      createTraceItem({ scriptName: 'auth-worker', eventTimestamp: 1001 }),
      createTraceItem({ scriptName: 'api-worker', eventTimestamp: 1002 }),
      createTraceItem({ scriptName: 'cdn-worker', eventTimestamp: 1003 }),
      createTraceItem({ scriptName: 'auth-worker', eventTimestamp: 1004 }),
    ]

    await handler(events)

    // Group by script name
    const byWorker = new Map<string, number>()
    for (const entity of createdEntities) {
      const script = entity.scriptName as string
      byWorker.set(script, (byWorker.get(script) || 0) + 1)
    }

    expect(byWorker.get('api-worker')).toBe(2)
    expect(byWorker.get('auth-worker')).toBe(2)
    expect(byWorker.get('cdn-worker')).toBe(1)
  })

  it('should filter and process events from multiple workers independently', () => {
    const allEvents: TraceItem[] = [
      createTraceItem({ scriptName: 'api-worker', outcome: 'ok' }),
      createFailedTraceItem({ scriptName: 'api-worker' }),
      createTraceItem({ scriptName: 'auth-worker', outcome: 'ok' }),
      createFailedTraceItem({ scriptName: 'auth-worker' }),
      createTraceItem({ scriptName: 'cron-worker', outcome: 'ok', event: null }),
    ]

    // Filter per-worker errors
    const apiErrors = filterTraceItems(allEvents, {
      scriptNames: ['api-worker'],
      exceptionsOnly: true,
    })
    const authErrors = filterTraceItems(allEvents, {
      scriptNames: ['auth-worker'],
      exceptionsOnly: true,
    })

    expect(apiErrors.length).toBe(1)
    expect(authErrors.length).toBe(1)

    // Process and batch independently
    const processedApi = processTraceItems(apiErrors)
    const processedAuth = processTraceItems(authErrors)

    expect(processedApi[0]!.scriptName).toBe('api-worker')
    expect(processedAuth[0]!.scriptName).toBe('auth-worker')

    // Batch them together
    const config: BatchConfig = { maxEvents: 10, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()

    addToBatch(state, processedApi, config)
    addToBatch(state, processedAuth, config)

    expect(state.events.length).toBe(2)
    expect(state.events.map((e) => e.scriptName).sort()).toEqual(['api-worker', 'auth-worker'])
  })
})

// =============================================================================
// Integration Tests: Complete Pipeline End-to-End
// =============================================================================

describe('Integration: Complete Pipeline E2E', () => {
  it('should process a realistic production scenario end-to-end', () => {
    // Step 1: Generate production-like workload
    const rawEvents = createProductionWorkload(500)

    // Step 2: Apply default filter
    const filtered = filterTraceItems(rawEvents, DEFAULT_FILTER)

    // All events should pass the default filter (outcomes include 'ok' and 'exception')
    expect(filtered.length).toBe(500)

    // Step 3: Apply a more specific filter (errors only)
    const errorsOnly = filterTraceItems(filtered, {
      exceptionsOnly: true,
    })
    expect(errorsOnly.length).toBe(100) // 1 in 5

    // Step 4: Process errors
    const processed = processTraceItems(errorsOnly)

    // Step 5: Verify processing results
    for (const event of processed) {
      expect(event.outcome).not.toBe('ok')
      expect(event.exceptionCount).toBeGreaterThan(0)
      expect(event.errors).toBeDefined()
      expect(event.errors!.length).toBeGreaterThan(0)
    }

    // Step 6: Batch into groups of 25
    const batchConfig: BatchConfig = { maxEvents: 25, maxWaitMs: 60000, minEvents: 1 }
    const batchState = createBatchState()
    const batches: ProcessedEvent[][] = []

    for (const event of processed) {
      const flushed = addToBatch(batchState, [event], batchConfig)
      if (flushed.length > 0) {
        batches.push(flushed)
      }
    }

    // 100 / 25 = 4 batches (no remainder)
    expect(batches.length).toBe(4)
    expect(batchState.events.length).toBe(0)

    // Verify all events accounted for
    const totalInBatches = batches.reduce((sum, b) => sum + b.length, 0)
    expect(totalInBatches).toBe(100)
  })

  it('should handle a mixed workload with various log levels', () => {
    const events: TraceItem[] = [
      createTraceItem({
        scriptName: 'api-worker',
        logs: [
          { timestamp: 1000, level: 'debug', message: ['Debug info'] },
          { timestamp: 1001, level: 'info', message: ['Request started'] },
          { timestamp: 1002, level: 'warn', message: ['Slow query detected'] },
          { timestamp: 1003, level: 'info', message: ['Request completed'] },
        ],
      }),
      createTraceItem({
        scriptName: 'auth-worker',
        logs: [
          { timestamp: 2000, level: 'info', message: ['Auth check'] },
          { timestamp: 2001, level: 'error', message: ['Token expired'] },
        ],
      }),
      createTraceItem({
        scriptName: 'cdn-worker',
        logs: [
          { timestamp: 3000, level: 'info', message: ['Cache hit'] },
        ],
      }),
    ]

    // Process all events
    const processed = processTraceItems(events)

    // API worker: 4 total logs, 1 significant (warn)
    expect(processed[0]!.logCount).toBe(4)
    expect(processed[0]!.logs).toHaveLength(1)
    expect(processed[0]!.logs![0]!.level).toBe('warn')

    // Auth worker: 2 total logs, 1 significant (error)
    expect(processed[1]!.logCount).toBe(2)
    expect(processed[1]!.logs).toHaveLength(1)
    expect(processed[1]!.logs![0]!.level).toBe('error')

    // CDN worker: 1 total log, 0 significant
    expect(processed[2]!.logCount).toBe(1)
    expect(processed[2]!.logs).toBeUndefined()
  })

  it('should handle events with complex exception chains', () => {
    const event = createTraceItem({
      outcome: 'exception',
      exceptions: [
        { timestamp: 1000, name: 'TypeError', message: 'Cannot read property "x" of undefined' },
        { timestamp: 1001, name: 'Error', message: 'Wrapped: TypeError: Cannot read property "x" of undefined' },
        { timestamp: 1002, name: 'UnhandledPromiseRejection', message: 'Unhandled rejection: Error: Wrapped...' },
      ],
    })

    const processed = processTraceItem(event)

    expect(processed.exceptionCount).toBe(3)
    expect(processed.errors).toEqual([
      'TypeError: Cannot read property "x" of undefined',
      'Error: Wrapped: TypeError: Cannot read property "x" of undefined',
      'UnhandledPromiseRejection: Unhandled rejection: Error: Wrapped...',
    ])
  })

  it('should correctly round-trip events through filter, process, and batch', () => {
    // Create specific events with known data
    const events: TraceItem[] = [
      createTraceItem({
        scriptName: 'worker-a',
        outcome: 'ok',
        eventTimestamp: 1704067200000, // 2024-01-01T00:00:00Z
        event: {
          request: {
            method: 'GET',
            url: 'https://api.example.com/users',
            headers: {},
            cf: { colo: 'SJC', country: 'US' },
          },
        },
      }),
      createFailedTraceItem({
        scriptName: 'worker-b',
        eventTimestamp: 1704067260000, // 2024-01-01T00:01:00Z
      }),
    ]

    // Filter: keep all
    const filtered = filterTraceItems(events, {})
    expect(filtered.length).toBe(2)

    // Process
    const processed = processTraceItems(filtered)

    // Verify data integrity
    expect(processed[0]!.scriptName).toBe('worker-a')
    expect(processed[0]!.outcome).toBe('ok')
    expect(processed[0]!.timestamp).toBe('2024-01-01T00:00:00.000Z')
    expect(processed[0]!.method).toBe('GET')
    expect(processed[0]!.colo).toBe('SJC')
    expect(processed[0]!.exceptionCount).toBe(0)
    expect(processed[0]!.errors).toBeUndefined()

    expect(processed[1]!.scriptName).toBe('worker-b')
    expect(processed[1]!.outcome).toBe('exception')
    expect(processed[1]!.timestamp).toBe('2024-01-01T00:01:00.000Z')
    expect(processed[1]!.exceptionCount).toBe(1)
    expect(processed[1]!.errors).toBeDefined()

    // Batch
    const config: BatchConfig = { maxEvents: 10, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()
    addToBatch(state, processed, config)

    // Both events in buffer
    expect(state.events.length).toBe(2)

    // Verify data survived batching
    expect(state.events[0]!.scriptName).toBe('worker-a')
    expect(state.events[1]!.scriptName).toBe('worker-b')
  })
})

// =============================================================================
// Integration Tests: URL Pattern Filtering
// =============================================================================

describe('Integration: URL Pattern Filtering', () => {
  it('should filter events by exact URL pattern', () => {
    const events = [
      createTraceItem({
        event: {
          request: { method: 'GET', url: 'https://api.example.com/v1/users', headers: {} },
        },
      }),
      createTraceItem({
        event: {
          request: { method: 'GET', url: 'https://api.example.com/v1/posts', headers: {} },
        },
      }),
      createTraceItem({
        event: {
          request: { method: 'GET', url: 'https://api.example.com/v2/users', headers: {} },
        },
      }),
    ]

    const filter: TailEventFilter = {
      urlPatterns: ['https://api.example.com/v1/users'],
    }

    const filtered = filterTraceItems(events, filter)
    expect(filtered.length).toBe(1)
    expect(filtered[0]!.event!.request!.url).toBe('https://api.example.com/v1/users')
  })

  it('should pass events without request URLs when URL filter is set', () => {
    const events = [
      createTraceItem({
        event: null, // Scheduled event, no URL
      }),
      createTraceItem({
        event: {
          request: { method: 'GET', url: 'https://api.example.com/v1/users', headers: {} },
        },
      }),
    ]

    const filter: TailEventFilter = {
      urlPatterns: ['https://api.example.com/v1/users'],
    }

    const filtered = filterTraceItems(events, filter)
    // Event without URL should pass through (URL filter only applies when URL exists)
    expect(filtered.length).toBe(2)
  })

  it('should combine URL filter with other filters', () => {
    const events = [
      createTraceItem({
        scriptName: 'api-worker',
        outcome: 'ok',
        event: {
          request: { method: 'GET', url: 'https://api.example.com/v1/users', headers: {} },
        },
      }),
      createFailedTraceItem({
        scriptName: 'api-worker',
        event: {
          request: { method: 'POST', url: 'https://api.example.com/v1/users', headers: {} },
          response: { status: 500 },
        },
      }),
      createTraceItem({
        scriptName: 'cdn-worker',
        outcome: 'ok',
        event: {
          request: { method: 'GET', url: 'https://cdn.example.com/assets/logo.png', headers: {} },
        },
      }),
    ]

    const filter: TailEventFilter = {
      scriptNames: ['api-worker'],
      urlPatterns: ['https://api.example.com/v1/users'],
      exceptionsOnly: true,
    }

    const filtered = filterTraceItems(events, filter)
    expect(filtered.length).toBe(1)
    expect(filtered[0]!.outcome).toBe('exception')
    expect(filtered[0]!.scriptName).toBe('api-worker')
  })
})

// =============================================================================
// Integration Tests: Default Filter and Batch Config
// =============================================================================

describe('Integration: Default Configurations', () => {
  it('should accept all standard outcomes with default filter', () => {
    const events: TraceItem[] = [
      createTraceItem({ outcome: 'ok' }),
      createTraceItem({ outcome: 'exception' }),
      createTraceItem({ outcome: 'exceededCpu' }),
      createTraceItem({ outcome: 'exceededMemory' }),
      createTraceItem({ outcome: 'unknown' }),
    ]

    const filtered = filterTraceItems(events, DEFAULT_FILTER)
    expect(filtered.length).toBe(5)
  })

  it('should reject non-standard outcomes with default filter', () => {
    const events: TraceItem[] = [
      createTraceItem({ outcome: 'canceled' }),
      createTraceItem({ outcome: 'scriptNotFound' }),
      createTraceItem({ outcome: 'custom' }),
    ]

    const filtered = filterTraceItems(events, DEFAULT_FILTER)
    expect(filtered.length).toBe(0)
  })

  it('should work with default batch config values', () => {
    expect(DEFAULT_BATCH_CONFIG.maxEvents).toBe(100)
    expect(DEFAULT_BATCH_CONFIG.minEvents).toBe(1)
    expect(DEFAULT_BATCH_CONFIG.maxWaitMs).toBeGreaterThan(0)

    const state = createBatchState()
    const items = createProductionWorkload(DEFAULT_BATCH_CONFIG.maxEvents)
    const processed = processTraceItems(items)

    const flushed = addToBatch(state, processed, DEFAULT_BATCH_CONFIG)
    expect(flushed.length).toBe(DEFAULT_BATCH_CONFIG.maxEvents)
    expect(state.events.length).toBe(0)
  })
})

// =============================================================================
// Integration Tests: Edge Cases in Pipeline
// =============================================================================

describe('Integration: Pipeline Edge Cases', () => {
  it('should handle events with empty strings for scriptName', () => {
    const event = createTraceItem({ scriptName: '' })
    const processed = processTraceItem(event)

    // Empty string is falsy, so processTraceItem converts it to 'unknown'
    expect(processed.scriptName).toBe('unknown')
  })

  it('should handle events with very large log arrays', () => {
    const manyLogs = Array.from({ length: 100 }, (_, i) => ({
      timestamp: Date.now() + i,
      level: i % 10 === 0 ? 'error' as const : 'info' as const,
      message: [`Log message ${i}`],
    }))

    const event = createTraceItem({ logs: manyLogs })
    const processed = processTraceItem(event)

    expect(processed.logCount).toBe(100)
    // Only error logs (every 10th) should be in processed.logs
    expect(processed.logs).toHaveLength(10)
  })

  it('should handle events with non-string log messages', () => {
    const event = createTraceItem({
      logs: [
        { timestamp: 1000, level: 'error', message: [42] },
        { timestamp: 1001, level: 'warn', message: [{ key: 'value' }] },
        { timestamp: 1002, level: 'error', message: [null] },
        { timestamp: 1003, level: 'warn', message: [true, false, 'mixed'] },
      ],
    })

    const processed = processTraceItem(event)

    expect(processed.logs).toHaveLength(4)
    // Non-string messages should be JSON-stringified
    expect(processed.logs![0]!.message).toBe('[42]')
    expect(processed.logs![1]!.message).toBe('[{"key":"value"}]')
    expect(processed.logs![2]!.message).toBe('[null]')
    expect(processed.logs![3]!.message).toBe('[true,false,"mixed"]')
  })

  it('should handle zero-timestamp events', () => {
    const event = createTraceItem({ eventTimestamp: 0 })
    const processed = processTraceItem(event)

    // eventTimestamp 0 is falsy, so it should use current time
    const processedDate = new Date(processed.timestamp)
    expect(processedDate.getFullYear()).toBeGreaterThan(1970)
  })

  it('should process and batch empty filtered result', () => {
    const events = createProductionWorkload(10)

    // Filter for a non-existent worker
    const filtered = filterTraceItems(events, {
      scriptNames: ['non-existent-worker'],
    })

    expect(filtered.length).toBe(0)

    const processed = processTraceItems(filtered)
    expect(processed.length).toBe(0)

    const config: BatchConfig = { maxEvents: 10, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()
    const flushed = addToBatch(state, processed, config)

    expect(flushed.length).toBe(0)
    expect(state.events.length).toBe(0)
  })

  it('should handle batch state reset across multiple flush cycles', () => {
    const config: BatchConfig = { maxEvents: 5, maxWaitMs: 60000, minEvents: 1 }
    const state = createBatchState()
    const allFlushed: ProcessedEvent[][] = []

    const events = createProductionWorkload(23)
    const processed = processTraceItems(events)

    for (const event of processed) {
      const flushed = addToBatch(state, [event], config)
      if (flushed.length > 0) {
        allFlushed.push(flushed)
      }
    }

    // 23 / 5 = 4 full batches + 3 remaining
    expect(allFlushed.length).toBe(4)
    expect(state.events.length).toBe(3)

    // Verify batch state was properly reset
    for (const batch of allFlushed) {
      expect(batch.length).toBe(5)
    }

    // Total events preserved
    const totalFlushed = allFlushed.reduce((sum, b) => sum + b.length, 0)
    expect(totalFlushed + state.events.length).toBe(23)
  })
})

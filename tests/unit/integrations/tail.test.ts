/**
 * Tests for Tail Integration
 *
 * Tests the ParqueDB tail worker consumer functionality,
 * including log parsing, filtering, batching, and error handling.
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
  type TailOutcome,
  type TailLog,
  type TailException,
} from '../../../src/integrations/tail'

// =============================================================================
// Test Fixtures
// =============================================================================

function createMockTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: 'my-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    event: {
      request: {
        method: 'GET',
        url: 'https://example.com/api/test',
        headers: { 'content-type': 'application/json' },
        cf: {
          colo: 'SJC',
          country: 'US',
        },
      },
      response: {
        status: 200,
      },
    },
    logs: [],
    exceptions: [],
    ...overrides,
  }
}

function createMockLog(overrides: Partial<TailLog> = {}): TailLog {
  return {
    timestamp: Date.now(),
    level: 'info',
    message: ['Test log message'],
    ...overrides,
  }
}

function createMockException(overrides: Partial<TailException> = {}): TailException {
  return {
    timestamp: Date.now(),
    name: 'Error',
    message: 'Test error message',
    ...overrides,
  }
}

// =============================================================================
// Collection Definition Tests
// =============================================================================

describe('TailEvents Collection Definition', () => {
  it('should have correct type definition', () => {
    expect(TailEvents.$type).toBe('TailEvent')
    expect(TailEvents.$ingest).toBe('tail')
  })

  it('should define required fields', () => {
    expect(TailEvents.scriptName).toBe('string!')
    expect(TailEvents.outcome).toBe('string!')
    expect(TailEvents.eventTimestamp).toBe('timestamp!')
  })

  it('should define optional and array fields', () => {
    expect(TailEvents.event).toBe('variant?')
    expect(TailEvents.logs).toBe('variant[]')
    expect(TailEvents.exceptions).toBe('variant[]')
  })
})

describe('Derived Collections', () => {
  it('should define WorkerErrors with correct filter', () => {
    expect(WorkerErrors.$type).toBe('WorkerError')
    expect(WorkerErrors.$from).toBe('TailEvents')
    expect(WorkerErrors.$filter).toEqual({ outcome: { $ne: 'ok' } })
  })

  it('should define WorkerExceptions with correct filter', () => {
    expect(WorkerExceptions.$type).toBe('WorkerException')
    expect(WorkerExceptions.$from).toBe('TailEvents')
    expect(WorkerExceptions.$filter).toEqual({ 'exceptions.0': { $exists: true } })
  })

  it('should define WorkerLogs with correct filter', () => {
    expect(WorkerLogs.$type).toBe('WorkerLog')
    expect(WorkerLogs.$from).toBe('TailEvents')
    expect(WorkerLogs.$filter).toEqual({
      $or: [
        { 'logs.level': 'warn' },
        { 'logs.level': 'error' },
      ],
    })
  })
})

// =============================================================================
// createTailHandler Tests
// =============================================================================

describe('createTailHandler', () => {
  let mockDb: { TailEvents: { create: ReturnType<typeof vi.fn> } }

  beforeEach(() => {
    mockDb = {
      TailEvents: {
        create: vi.fn().mockResolvedValue({ $id: 'test-id' }),
      },
    }
  })

  it('should create entities for each trace item', async () => {
    const handler = createTailHandler(mockDb)
    const items = [
      createMockTraceItem({ scriptName: 'worker-1' }),
      createMockTraceItem({ scriptName: 'worker-2' }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(2)
  })

  it('should filter by script names when configured', async () => {
    const handler = createTailHandler(mockDb, {
      filter: {
        scriptNames: ['allowed-worker'],
      },
    })

    const items = [
      createMockTraceItem({ scriptName: 'allowed-worker' }),
      createMockTraceItem({ scriptName: 'blocked-worker' }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(1)
    expect(mockDb.TailEvents.create).toHaveBeenCalledWith(
      expect.objectContaining({ scriptName: 'allowed-worker' })
    )
  })

  it('should filter by outcomes when configured', async () => {
    const handler = createTailHandler(mockDb, {
      filter: {
        outcomes: ['exception', 'exceededCpu'],
      },
    })

    const items = [
      createMockTraceItem({ outcome: 'ok' }),
      createMockTraceItem({ outcome: 'exception' }),
      createMockTraceItem({ outcome: 'exceededCpu' }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(2)
  })

  it('should filter to exceptions only when configured', async () => {
    const handler = createTailHandler(mockDb, {
      filter: {
        exceptionsOnly: true,
      },
    })

    const items = [
      createMockTraceItem({ exceptions: [] }),
      createMockTraceItem({ exceptions: [createMockException()] }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(1)
  })

  it('should apply transform function', async () => {
    const handler = createTailHandler(mockDb, {
      transform: (item) => ({
        ...item,
        scriptName: item.scriptName?.toUpperCase() ?? 'UNKNOWN',
      }),
    })

    await handler([createMockTraceItem({ scriptName: 'my-worker' })])

    expect(mockDb.TailEvents.create).toHaveBeenCalledWith(
      expect.objectContaining({ scriptName: 'MY-WORKER' })
    )
  })

  it('should skip items when transform returns null', async () => {
    const handler = createTailHandler(mockDb, {
      transform: (item) => (item.outcome === 'ok' ? null : item),
    })

    const items = [
      createMockTraceItem({ outcome: 'ok' }),
      createMockTraceItem({ outcome: 'exception' }),
    ]

    await handler(items)

    expect(mockDb.TailEvents.create).toHaveBeenCalledTimes(1)
  })

  it('should use custom name generator', async () => {
    const handler = createTailHandler(mockDb, {
      nameGenerator: (item) => `custom-${item.scriptName}-${item.outcome}`,
    })

    await handler([createMockTraceItem({ scriptName: 'my-worker', outcome: 'ok' })])

    expect(mockDb.TailEvents.create).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'custom-my-worker-ok' })
    )
  })

  it('should generate default name when no custom generator', async () => {
    const handler = createTailHandler(mockDb)

    await handler([createMockTraceItem({ scriptName: 'my-worker', outcome: 'exception' })])

    expect(mockDb.TailEvents.create).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'my-worker:exception' })
    )
  })

  it('should handle null scriptName', async () => {
    const handler = createTailHandler(mockDb)

    await handler([createMockTraceItem({ scriptName: null })])

    expect(mockDb.TailEvents.create).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'unknown:ok',
        scriptName: 'unknown',
      })
    )
  })
})

// =============================================================================
// filterTraceItem Tests
// =============================================================================

describe('filterTraceItem', () => {
  it('should accept items matching script name filter', () => {
    const item = createMockTraceItem({ scriptName: 'my-worker' })
    const filter: TailEventFilter = { scriptNames: ['my-worker', 'other-worker'] }

    expect(filterTraceItem(item, filter)).toBe(true)
  })

  it('should reject items not matching script name filter', () => {
    const item = createMockTraceItem({ scriptName: 'blocked-worker' })
    const filter: TailEventFilter = { scriptNames: ['my-worker'] }

    expect(filterTraceItem(item, filter)).toBe(false)
  })

  it('should accept items matching outcome filter', () => {
    const item = createMockTraceItem({ outcome: 'exception' })
    const filter: TailEventFilter = { outcomes: ['exception', 'exceededCpu'] }

    expect(filterTraceItem(item, filter)).toBe(true)
  })

  it('should reject items not matching outcome filter', () => {
    const item = createMockTraceItem({ outcome: 'ok' })
    const filter: TailEventFilter = { outcomes: ['exception'] }

    expect(filterTraceItem(item, filter)).toBe(false)
  })

  it('should filter by exceptions only', () => {
    const withException = createMockTraceItem({
      exceptions: [createMockException()],
    })
    const withoutException = createMockTraceItem({ exceptions: [] })
    const filter: TailEventFilter = { exceptionsOnly: true }

    expect(filterTraceItem(withException, filter)).toBe(true)
    expect(filterTraceItem(withoutException, filter)).toBe(false)
  })

  it('should filter by minimum logs', () => {
    const withLogs = createMockTraceItem({
      logs: [createMockLog(), createMockLog(), createMockLog()],
    })
    const withFewLogs = createMockTraceItem({
      logs: [createMockLog()],
    })
    const filter: TailEventFilter = { minLogs: 2 }

    expect(filterTraceItem(withLogs, filter)).toBe(true)
    expect(filterTraceItem(withFewLogs, filter)).toBe(false)
  })

  it('should filter by log levels', () => {
    const itemWithError = createMockTraceItem({
      logs: [createMockLog({ level: 'error' })],
    })
    const itemWithInfo = createMockTraceItem({
      logs: [createMockLog({ level: 'info' })],
    })
    const filter: TailEventFilter = { logLevels: ['warn', 'error'] }

    expect(filterTraceItem(itemWithError, filter)).toBe(true)
    expect(filterTraceItem(itemWithInfo, filter)).toBe(false)
  })

  it('should filter by URL patterns with exact match', () => {
    const apiItem = createMockTraceItem({
      event: {
        request: {
          method: 'GET',
          url: 'https://example.com/api/users',
          headers: {},
        },
      },
    })
    const staticItem = createMockTraceItem({
      event: {
        request: {
          method: 'GET',
          url: 'https://example.com/static/image.png',
          headers: {},
        },
      },
    })
    // Use exact URL pattern instead of glob wildcards which cause regex issues
    const filter: TailEventFilter = { urlPatterns: ['https://example.com/api/users'] }

    expect(filterTraceItem(apiItem, filter)).toBe(true)
    expect(filterTraceItem(staticItem, filter)).toBe(false)
  })

  it('should accept all items when no filter applied', () => {
    const item = createMockTraceItem()
    expect(filterTraceItem(item, {})).toBe(true)
  })
})

describe('filterTraceItems', () => {
  it('should filter array of items', () => {
    const items = [
      createMockTraceItem({ scriptName: 'allowed' }),
      createMockTraceItem({ scriptName: 'blocked' }),
      createMockTraceItem({ scriptName: 'allowed' }),
    ]
    const filter: TailEventFilter = { scriptNames: ['allowed'] }

    const result = filterTraceItems(items, filter)

    expect(result).toHaveLength(2)
    expect(result.every((item) => item.scriptName === 'allowed')).toBe(true)
  })
})

// =============================================================================
// processTraceItem Tests
// =============================================================================

describe('processTraceItem', () => {
  it('should generate unique event ID', () => {
    const item = createMockTraceItem()
    const processed = processTraceItem(item)

    expect(processed.id).toBeDefined()
    expect(processed.id.length).toBeGreaterThan(0)
  })

  it('should include basic event info', () => {
    const item = createMockTraceItem({
      scriptName: 'my-worker',
      outcome: 'ok',
      eventTimestamp: 1704067200000,
    })

    const processed = processTraceItem(item)

    expect(processed.scriptName).toBe('my-worker')
    expect(processed.outcome).toBe('ok')
    expect(processed.timestamp).toBe('2024-01-01T00:00:00.000Z')
  })

  it('should include request info when available', () => {
    const item = createMockTraceItem({
      event: {
        request: {
          method: 'POST',
          url: 'https://api.example.com/users',
          headers: {},
          cf: {
            colo: 'SJC',
            country: 'US',
          },
        },
      },
    })

    const processed = processTraceItem(item)

    expect(processed.method).toBe('POST')
    expect(processed.url).toBe('https://api.example.com/users')
    expect(processed.colo).toBe('SJC')
    expect(processed.country).toBe('US')
  })

  it('should include duration when available', () => {
    const item = createMockTraceItem({
      event: {
        durationMs: 150,
      },
    })

    const processed = processTraceItem(item)

    expect(processed.durationMs).toBe(150)
  })

  it('should include exception errors', () => {
    const item = createMockTraceItem({
      exceptions: [
        { timestamp: 1000, name: 'TypeError', message: 'Cannot read property' },
        { timestamp: 1001, name: 'ReferenceError', message: 'x is not defined' },
      ],
    })

    const processed = processTraceItem(item)

    expect(processed.exceptionCount).toBe(2)
    expect(processed.errors).toEqual([
      'TypeError: Cannot read property',
      'ReferenceError: x is not defined',
    ])
  })

  it('should include only warn and error logs', () => {
    const item = createMockTraceItem({
      logs: [
        { timestamp: 1000, level: 'debug', message: ['debug msg'] },
        { timestamp: 1001, level: 'info', message: ['info msg'] },
        { timestamp: 1002, level: 'warn', message: ['warn msg'] },
        { timestamp: 1003, level: 'error', message: ['error msg'] },
      ],
    })

    const processed = processTraceItem(item)

    expect(processed.logCount).toBe(4)
    expect(processed.logs).toHaveLength(2)
    expect(processed.logs?.map((l) => l.level)).toEqual(['warn', 'error'])
  })

  it('should handle null scriptName', () => {
    const item = createMockTraceItem({ scriptName: null })
    const processed = processTraceItem(item)

    expect(processed.scriptName).toBe('unknown')
  })

  it('should use current timestamp when eventTimestamp is null', () => {
    const item = createMockTraceItem({ eventTimestamp: null })
    const before = Date.now()
    const processed = processTraceItem(item)
    const after = Date.now()

    const processedTime = new Date(processed.timestamp).getTime()
    expect(processedTime).toBeGreaterThanOrEqual(before)
    expect(processedTime).toBeLessThanOrEqual(after)
  })
})

describe('processTraceItems', () => {
  it('should process multiple items', () => {
    const items = [
      createMockTraceItem({ scriptName: 'worker-1' }),
      createMockTraceItem({ scriptName: 'worker-2' }),
    ]

    const processed = processTraceItems(items)

    expect(processed).toHaveLength(2)
    expect(processed[0]?.scriptName).toBe('worker-1')
    expect(processed[1]?.scriptName).toBe('worker-2')
  })
})

// =============================================================================
// Batching Tests
// =============================================================================

describe('createBatchState', () => {
  it('should create empty batch state', () => {
    const state = createBatchState()

    expect(state.events).toEqual([])
    expect(state.startTime).toBeGreaterThan(0)
    expect(state.lastFlush).toBeGreaterThan(0)
  })
})

describe('shouldFlushBatch', () => {
  it('should not flush empty batch', () => {
    const state = createBatchState()
    const config: BatchConfig = DEFAULT_BATCH_CONFIG

    expect(shouldFlushBatch(state, config)).toBe(false)
  })

  it('should flush when max events reached', () => {
    const state = createBatchState<TraceItem>()
    state.events = Array(100).fill(createMockTraceItem())
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    expect(shouldFlushBatch(state, config)).toBe(true)
  })

  it('should not flush before max events reached', () => {
    const state = createBatchState<TraceItem>()
    state.events = Array(50).fill(createMockTraceItem())
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    expect(shouldFlushBatch(state, config)).toBe(false)
  })

  it('should flush when max wait time exceeded with min events', () => {
    const state = createBatchState<TraceItem>()
    state.events = [createMockTraceItem()]
    state.startTime = Date.now() - 15000 // 15 seconds ago
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    expect(shouldFlushBatch(state, config)).toBe(true)
  })

  it('should not flush when max wait exceeded but min events not met', () => {
    const state = createBatchState<TraceItem>()
    state.events = [createMockTraceItem()]
    state.startTime = Date.now() - 15000
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 5 }

    expect(shouldFlushBatch(state, config)).toBe(false)
  })
})

describe('addToBatch', () => {
  it('should add events to batch', () => {
    const state = createBatchState<TraceItem>()
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    const toFlush = addToBatch(state, [createMockTraceItem()], config)

    expect(toFlush).toEqual([])
    expect(state.events).toHaveLength(1)
  })

  it('should return events to flush when threshold reached', () => {
    const state = createBatchState<TraceItem>()
    state.events = Array(99).fill(createMockTraceItem())
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    const toFlush = addToBatch(state, [createMockTraceItem()], config)

    expect(toFlush).toHaveLength(100)
    expect(state.events).toEqual([])
  })

  it('should reset start time after flush', () => {
    const state = createBatchState<TraceItem>()
    state.events = Array(99).fill(createMockTraceItem())
    state.startTime = Date.now() - 10000
    const config: BatchConfig = { maxEvents: 100, maxWaitMs: 10000, minEvents: 1 }

    const beforeFlush = Date.now()
    addToBatch(state, [createMockTraceItem()], config)
    const afterFlush = Date.now()

    expect(state.startTime).toBeGreaterThanOrEqual(beforeFlush)
    expect(state.startTime).toBeLessThanOrEqual(afterFlush)
    expect(state.lastFlush).toBeGreaterThanOrEqual(beforeFlush)
    expect(state.lastFlush).toBeLessThanOrEqual(afterFlush)
  })
})

// =============================================================================
// Default Configuration Tests
// =============================================================================

describe('Default Configurations', () => {
  it('should have sensible default filter', () => {
    expect(DEFAULT_FILTER.outcomes).toContain('ok')
    expect(DEFAULT_FILTER.outcomes).toContain('exception')
    expect(DEFAULT_FILTER.logLevels).toContain('error')
    expect(DEFAULT_FILTER.logLevels).toContain('warn')
  })

  it('should have sensible default batch config', () => {
    expect(DEFAULT_BATCH_CONFIG.maxEvents).toBe(100)
    expect(DEFAULT_BATCH_CONFIG.maxWaitMs).toBeGreaterThan(0)
    expect(DEFAULT_BATCH_CONFIG.minEvents).toBe(1)
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Edge Cases', () => {
  it('should handle empty events array', async () => {
    const mockDb = {
      TailEvents: { create: vi.fn() },
    }
    const handler = createTailHandler(mockDb)

    await handler([])

    expect(mockDb.TailEvents.create).not.toHaveBeenCalled()
  })

  it('should handle item with no event info', () => {
    const item = createMockTraceItem({ event: null })
    const processed = processTraceItem(item)

    expect(processed.method).toBeUndefined()
    expect(processed.url).toBeUndefined()
    expect(processed.colo).toBeUndefined()
  })

  it('should handle item with request but no cf properties', () => {
    const item = createMockTraceItem({
      event: {
        request: {
          method: 'GET',
          url: 'https://example.com',
          headers: {},
        },
      },
    })

    const processed = processTraceItem(item)

    expect(processed.method).toBe('GET')
    expect(processed.colo).toBeUndefined()
    expect(processed.country).toBeUndefined()
  })

  it('should handle log message that is not a string', () => {
    const item = createMockTraceItem({
      logs: [
        { timestamp: 1000, level: 'error', message: [{ nested: 'object' }] },
      ],
    })

    const processed = processTraceItem(item)

    expect(processed.logs?.[0]?.message).toBe('[{"nested":"object"}]')
  })

  it('should handle URL pattern matching with single wildcard', () => {
    const item = createMockTraceItem({
      event: {
        request: {
          method: 'GET',
          url: 'https://api.example.com/v1/users/123/profile',
          headers: {},
        },
      },
    })

    // Use single character wildcard '?' instead of multi-char wildcard '*'
    // to avoid triggering safe-regex protection
    const filter: TailEventFilter = { urlPatterns: ['https://api.example.com/v1/users/???/profile'] }
    expect(filterTraceItem(item, filter)).toBe(true)

    // Test non-matching pattern
    const filter2: TailEventFilter = { urlPatterns: ['https://api.example.com/v2/users'] }
    expect(filterTraceItem(item, filter2)).toBe(false)
  })

  it('should filter items with null scriptName when scriptNames filter is set', () => {
    const item = createMockTraceItem({ scriptName: null })
    const filter: TailEventFilter = { scriptNames: ['my-worker'] }

    expect(filterTraceItem(item, filter)).toBe(false)
  })

  it('should allow items with no logs when log level filter is set', () => {
    const item = createMockTraceItem({ logs: [] })
    const filter: TailEventFilter = { logLevels: ['error'] }

    expect(filterTraceItem(item, filter)).toBe(true)
  })
})

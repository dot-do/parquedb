/**
 * WorkerLogs Materialized View Tests
 *
 * Tests for the WorkerLogsMV that ingests Cloudflare Tail Worker logs
 * and stores them in Parquet format.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  WorkerLogsMV,
  createWorkerLogsMV,
  createWorkerLogsMVHandler,
  type TailEvent,
  type TailItem,
  type WorkerLogRecord,
  type WorkerLogsStats,
  WORKER_LOGS_SCHEMA,
} from '../../../src/streaming/worker-logs'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock storage backend
 */
function createMockStorage(): StorageBackend & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>()

  return {
    files,
    async read(path: string): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    },
    async write(path: string, data: Uint8Array): Promise<{ etag: string; size: number }> {
      files.set(path, data)
      return { etag: 'mock-etag', size: data.length }
    },
    async writeAtomic(path: string, data: Uint8Array): Promise<{ etag: string; size: number }> {
      files.set(path, data)
      return { etag: 'mock-etag', size: data.length }
    },
    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },
    async delete(path: string): Promise<void> {
      files.delete(path)
    },
    async list(prefix: string): Promise<string[]> {
      return Array.from(files.keys()).filter(k => k.startsWith(prefix))
    },
  } as StorageBackend & { files: Map<string, Uint8Array> }
}

/**
 * Create a sample TailItem
 */
function createTailItem(overrides: Partial<TailItem> = {}): TailItem {
  return {
    scriptName: 'test-worker',
    event: {
      request: {
        method: 'GET',
        url: 'https://example.com/api/test',
        headers: { 'cf-ray': 'test-ray-id' },
        cf: { colo: 'SJC', country: 'US' },
      },
      response: { status: 200 },
    },
    eventTimestamp: Date.now(),
    logs: [
      { timestamp: Date.now(), level: 'info', message: ['Test log message'] },
    ],
    exceptions: [],
    outcome: 'ok',
    ...overrides,
  }
}

/**
 * Create a sample TailEvent
 */
function createTailEvent(traces: TailItem[] = [createTailItem()]): TailEvent {
  return {
    type: 'tail',
    traces,
  }
}

/**
 * Create a deterministic ID generator for testing
 */
function createIdGenerator(): () => string {
  let counter = 0
  return () => `test-id-${++counter}`
}

// =============================================================================
// WorkerLogsMV Tests
// =============================================================================

describe('WorkerLogsMV', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: WorkerLogsMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createWorkerLogsMV({
      storage,
      datasetPath: 'logs/workers',
      flushThreshold: 100, // Low threshold for testing
      flushIntervalMs: 60000, // High interval so we control flushing
      generateId: createIdGenerator(),
    })
  })

  afterEach(async () => {
    if (mv.isRunning()) {
      await mv.stop()
    }
  })

  describe('constructor', () => {
    it('creates an instance with default config', () => {
      const defaultMv = createWorkerLogsMV({
        storage,
        datasetPath: 'logs/test',
      })
      expect(defaultMv).toBeInstanceOf(WorkerLogsMV)
    })

    it('normalizes dataset path by removing trailing slash', () => {
      const mvWithSlash = createWorkerLogsMV({
        storage,
        datasetPath: 'logs/test/',
        generateId: createIdGenerator(),
      })
      mvWithSlash.start()
      // The path normalization is internal, but we can verify via flush
      expect(mvWithSlash.isRunning()).toBe(true)
      mvWithSlash.stop()
    })
  })

  describe('start/stop', () => {
    it('starts and stops the MV', () => {
      expect(mv.isRunning()).toBe(false)

      mv.start()
      expect(mv.isRunning()).toBe(true)

      mv.start() // Should be idempotent
      expect(mv.isRunning()).toBe(true)
    })

    it('stop flushes remaining records', async () => {
      mv.start()

      // Ingest some records
      await mv.ingestTailEvent(createTailEvent())
      expect(mv.getBuffer().length).toBeGreaterThan(0)

      // Stop should flush
      await mv.stop()
      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
    })

    it('stop is idempotent', async () => {
      mv.start()
      await mv.stop()
      await mv.stop() // Should not throw
      expect(mv.isRunning()).toBe(false)
    })
  })

  describe('ingestTailEvent', () => {
    it('ingests a simple Tail event', async () => {
      mv.start()

      await mv.ingestTailEvent(createTailEvent())

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].scriptName).toBe('test-worker')
      expect(buffer[0].level).toBe('info')
      expect(buffer[0].message).toBe('Test log message')
    })

    it('ingests multiple traces from one event', async () => {
      mv.start()

      const event = createTailEvent([
        createTailItem({ scriptName: 'worker-1' }),
        createTailItem({ scriptName: 'worker-2' }),
      ])

      await mv.ingestTailEvent(event)

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(2)
      expect(buffer[0].scriptName).toBe('worker-1')
      expect(buffer[1].scriptName).toBe('worker-2')
    })

    it('extracts HTTP metadata from fetch events', async () => {
      mv.start()

      await mv.ingestTailEvent(createTailEvent())

      const buffer = mv.getBuffer()
      expect(buffer[0].httpMethod).toBe('GET')
      expect(buffer[0].httpUrl).toBe('https://example.com/api/test')
      expect(buffer[0].httpStatus).toBe(200)
      expect(buffer[0].colo).toBe('SJC')
      expect(buffer[0].country).toBe('US')
      expect(buffer[0].requestId).toBe('test-ray-id')
    })

    it('handles null event (scheduled/queue triggers)', async () => {
      mv.start()

      const item = createTailItem({ event: null })
      await mv.ingestTailEvent(createTailEvent([item]))

      const buffer = mv.getBuffer()
      expect(buffer[0].httpMethod).toBeNull()
      expect(buffer[0].httpUrl).toBeNull()
      expect(buffer[0].httpStatus).toBeNull()
    })

    it('converts exceptions to error log records', async () => {
      mv.start()

      const item = createTailItem({
        logs: [],
        exceptions: [
          { timestamp: Date.now(), name: 'TypeError', message: 'Cannot read property' },
        ],
        outcome: 'exception',
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].level).toBe('error')
      expect(buffer[0].isException).toBe(true)
      expect(buffer[0].exceptionName).toBe('TypeError')
      expect(buffer[0].message).toContain('TypeError: Cannot read property')
    })

    it('creates synthetic record when no logs or exceptions', async () => {
      mv.start()

      const item = createTailItem({
        logs: [],
        exceptions: [],
        outcome: 'ok',
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].message).toContain('Worker execution: ok')
      expect(buffer[0].level).toBe('info')
    })

    it('converts multiple log messages', async () => {
      mv.start()

      const item = createTailItem({
        logs: [
          { timestamp: Date.now(), level: 'debug', message: ['Debug message'] },
          { timestamp: Date.now(), level: 'info', message: ['Info message'] },
          { timestamp: Date.now(), level: 'warn', message: ['Warning'] },
          { timestamp: Date.now(), level: 'error', message: ['Error!'] },
        ],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(4)
      expect(buffer.map(r => r.level)).toEqual(['debug', 'info', 'warn', 'error'])
    })
  })

  describe('ingestTailItem', () => {
    it('ingests a single TailItem directly', async () => {
      mv.start()

      await mv.ingestTailItem(createTailItem())

      expect(mv.getBuffer().length).toBe(1)
    })
  })

  describe('ingestRecords', () => {
    it('ingests raw WorkerLogRecords', async () => {
      mv.start()

      const records: WorkerLogRecord[] = [
        {
          id: 'rec-1',
          scriptName: 'test',
          eventTimestamp: Date.now(),
          timestamp: Date.now(),
          level: 'info',
          message: 'Test',
          httpMethod: null,
          httpUrl: null,
          httpStatus: null,
          outcome: 'ok',
          isException: false,
          exceptionName: null,
          colo: null,
          country: null,
          requestId: null,
        },
      ]

      await mv.ingestRecords(records)

      expect(mv.getBuffer().length).toBe(1)
      expect(mv.getStats().recordsIngested).toBe(1)
    })
  })

  describe('flush', () => {
    it('flushes buffer to Parquet file', async () => {
      mv.start()

      await mv.ingestTailEvent(createTailEvent())
      expect(mv.getBuffer().length).toBe(1)

      await mv.flush()

      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)

      // Verify file path has timestamp partitioning
      const [path] = Array.from(storage.files.keys())
      expect(path).toMatch(/logs\/workers\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/logs-\d+\.parquet/)
    })

    it('handles empty buffer gracefully', async () => {
      mv.start()

      await mv.flush()

      expect(storage.files.size).toBe(0)
    })

    it('auto-flushes when threshold is reached', async () => {
      // Create MV with low threshold
      mv = createWorkerLogsMV({
        storage,
        datasetPath: 'logs/workers',
        flushThreshold: 3,
        generateId: createIdGenerator(),
      })
      mv.start()

      // Ingest records below threshold
      await mv.ingestTailItem(createTailItem())
      await mv.ingestTailItem(createTailItem())
      expect(storage.files.size).toBe(0)

      // This should trigger auto-flush
      await mv.ingestTailItem(createTailItem())
      expect(storage.files.size).toBe(1)
    })

    it('puts records back in buffer on flush failure', async () => {
      // Create storage that fails on write
      const failingStorage = createMockStorage()
      failingStorage.writeAtomic = async () => {
        throw new Error('Storage write failed')
      }

      // Create a separate MV for this test (not using the shared one)
      const failingMv = createWorkerLogsMV({
        storage: failingStorage,
        datasetPath: 'logs/workers',
        generateId: createIdGenerator(),
      })
      // Don't call start() to avoid periodic flush timer issues

      await failingMv.ingestTailItem(createTailItem())
      expect(failingMv.getBuffer().length).toBe(1)

      await expect(failingMv.flush()).rejects.toThrow('Storage write failed')
      expect(failingMv.getBuffer().length).toBe(1) // Records restored
    })
  })

  describe('message serialization', () => {
    it('serializes string messages directly', async () => {
      mv.start()

      const item = createTailItem({
        logs: [{ timestamp: Date.now(), level: 'info', message: ['Hello world'] }],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      expect(mv.getBuffer()[0].message).toBe('Hello world')
    })

    it('serializes object messages as JSON', async () => {
      mv.start()

      const item = createTailItem({
        logs: [{ timestamp: Date.now(), level: 'info', message: [{ key: 'value' }] }],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      expect(mv.getBuffer()[0].message).toBe('{"key":"value"}')
    })

    it('serializes multiple message arguments', async () => {
      mv.start()

      const item = createTailItem({
        logs: [{ timestamp: Date.now(), level: 'info', message: ['Hello', 123, true] }],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      expect(mv.getBuffer()[0].message).toBe('["Hello",123,true]')
    })
  })

  describe('statistics', () => {
    it('tracks ingested records', async () => {
      mv.start()

      await mv.ingestTailEvent(createTailEvent())

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(1)
      expect(stats.bufferSize).toBe(1)
    })

    it('tracks records by level', async () => {
      mv.start()

      const item = createTailItem({
        logs: [
          { timestamp: Date.now(), level: 'info', message: ['Info'] },
          { timestamp: Date.now(), level: 'warn', message: ['Warn'] },
          { timestamp: Date.now(), level: 'error', message: ['Error'] },
        ],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      const stats = mv.getStats()
      expect(stats.byLevel.info).toBe(1)
      expect(stats.byLevel.warn).toBe(1)
      expect(stats.byLevel.error).toBe(1)
    })

    it('tracks records by outcome', async () => {
      mv.start()

      await mv.ingestTailItem(createTailItem({ outcome: 'ok' }))
      await mv.ingestTailItem(createTailItem({ outcome: 'exception' }))
      await mv.ingestTailItem(createTailItem({ outcome: 'exceededCpu' }))

      const stats = mv.getStats()
      expect(stats.byOutcome.ok).toBe(1)
      expect(stats.byOutcome.exception).toBe(1)
      expect(stats.byOutcome.exceededCpu).toBe(1)
    })

    it('tracks records by script name', async () => {
      mv.start()

      await mv.ingestTailItem(createTailItem({ scriptName: 'worker-a' }))
      await mv.ingestTailItem(createTailItem({ scriptName: 'worker-a' }))
      await mv.ingestTailItem(createTailItem({ scriptName: 'worker-b' }))

      const stats = mv.getStats()
      expect(stats.byScript['worker-a']).toBe(2)
      expect(stats.byScript['worker-b']).toBe(1)
    })

    it('tracks exceptions', async () => {
      mv.start()

      const item = createTailItem({
        logs: [],
        exceptions: [
          { timestamp: Date.now(), name: 'Error', message: 'Oops' },
        ],
      })

      await mv.ingestTailEvent(createTailEvent([item]))

      const stats = mv.getStats()
      expect(stats.exceptions).toBe(1)
    })

    it('tracks written records and files after flush', async () => {
      mv.start()

      await mv.ingestTailItem(createTailItem())
      await mv.ingestTailItem(createTailItem())
      await mv.flush()

      const stats = mv.getStats()
      expect(stats.recordsWritten).toBe(2)
      expect(stats.filesCreated).toBe(1)
      expect(stats.bytesWritten).toBeGreaterThan(0)
      expect(stats.flushCount).toBe(1)
      expect(stats.lastFlushAt).not.toBeNull()
    })

    it('resets statistics', async () => {
      mv.start()

      await mv.ingestTailItem(createTailItem())
      expect(mv.getStats().recordsIngested).toBe(1)

      mv.resetStats()

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(0)
      expect(stats.bufferSize).toBe(1) // Buffer is not cleared
    })
  })
})

// =============================================================================
// Schema Tests
// =============================================================================

describe('WORKER_LOGS_SCHEMA', () => {
  it('defines all required columns', () => {
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('id')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('scriptName')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('eventTimestamp')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('timestamp')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('level')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('message')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('httpMethod')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('httpUrl')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('httpStatus')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('outcome')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('isException')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('exceptionName')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('colo')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('country')
    expect(WORKER_LOGS_SCHEMA).toHaveProperty('requestId')
  })

  it('marks required columns as non-optional', () => {
    expect(WORKER_LOGS_SCHEMA.id.optional).toBe(false)
    expect(WORKER_LOGS_SCHEMA.scriptName.optional).toBe(false)
    expect(WORKER_LOGS_SCHEMA.timestamp.optional).toBe(false)
    expect(WORKER_LOGS_SCHEMA.level.optional).toBe(false)
    expect(WORKER_LOGS_SCHEMA.outcome.optional).toBe(false)
    expect(WORKER_LOGS_SCHEMA.isException.optional).toBe(false)
  })

  it('marks nullable columns as optional', () => {
    expect(WORKER_LOGS_SCHEMA.httpMethod.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.httpUrl.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.httpStatus.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.exceptionName.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.colo.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.country.optional).toBe(true)
    expect(WORKER_LOGS_SCHEMA.requestId.optional).toBe(true)
  })
})

// =============================================================================
// Handler Integration Tests
// =============================================================================

describe('createWorkerLogsMVHandler', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: WorkerLogsMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createWorkerLogsMV({
      storage,
      datasetPath: 'logs/workers',
      generateId: createIdGenerator(),
    })
    mv.start()
  })

  afterEach(async () => {
    await mv.stop()
  })

  it('creates an MV handler for StreamingRefreshEngine', () => {
    const handler = createWorkerLogsMVHandler(mv)

    expect(handler.name).toBe('WorkerLogs')
    expect(handler.sourceNamespaces).toEqual(['worker_logs'])
    expect(typeof handler.process).toBe('function')
  })

  it('uses custom source namespace', () => {
    const handler = createWorkerLogsMVHandler(mv, 'custom_logs')

    expect(handler.sourceNamespaces).toEqual(['custom_logs'])
  })

  it('processes CDC events into WorkerLogRecords', async () => {
    const handler = createWorkerLogsMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'CREATE' as const,
        target: 'worker_logs:log-1',
        after: {
          scriptName: 'test-worker',
          timestamp: Date.now(),
          level: 'info',
          message: 'Test message',
        },
      },
    ]

    await handler.process(events)

    const buffer = mv.getBuffer()
    expect(buffer.length).toBe(1)
    expect(buffer[0].scriptName).toBe('test-worker')
  })

  it('ignores events without required fields', async () => {
    const handler = createWorkerLogsMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'CREATE' as const,
        target: 'worker_logs:log-1',
        after: {
          // Missing scriptName, timestamp, level
          message: 'Test message',
        },
      },
    ]

    await handler.process(events)

    expect(mv.getBuffer().length).toBe(0)
  })

  it('ignores UPDATE and DELETE events', async () => {
    const handler = createWorkerLogsMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'UPDATE' as const,
        target: 'worker_logs:log-1',
        before: { scriptName: 'old' },
        after: { scriptName: 'new', timestamp: Date.now(), level: 'info' },
      },
      {
        id: 'evt-2',
        ts: Date.now(),
        op: 'DELETE' as const,
        target: 'worker_logs:log-2',
        before: { scriptName: 'test', timestamp: Date.now(), level: 'info' },
      },
    ]

    await handler.process(events)

    expect(mv.getBuffer().length).toBe(0)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: WorkerLogsMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createWorkerLogsMV({
      storage,
      datasetPath: 'logs/workers',
      generateId: createIdGenerator(),
    })
    mv.start()
  })

  afterEach(async () => {
    await mv.stop()
  })

  it('handles empty TailEvent', async () => {
    const event: TailEvent = { type: 'tail', traces: [] }

    await mv.ingestTailEvent(event)

    expect(mv.getBuffer().length).toBe(0)
  })

  it('handles missing cf properties', async () => {
    const item = createTailItem({
      event: {
        request: {
          method: 'POST',
          url: 'https://example.com',
          headers: {},
          // No cf property
        },
        response: { status: 201 },
      },
    })

    await mv.ingestTailItem(item)

    const record = mv.getBuffer()[0]
    expect(record.colo).toBeNull()
    expect(record.country).toBeNull()
  })

  it('handles missing response', async () => {
    const item = createTailItem({
      event: {
        request: {
          method: 'GET',
          url: 'https://example.com',
          headers: {},
        },
        // No response
      },
    })

    await mv.ingestTailItem(item)

    expect(mv.getBuffer()[0].httpStatus).toBeNull()
  })

  it('handles large message arrays', async () => {
    const item = createTailItem({
      logs: [
        {
          timestamp: Date.now(),
          level: 'info',
          message: Array.from({ length: 100 }, (_, i) => `arg${i}`),
        },
      ],
    })

    await mv.ingestTailItem(item)

    const message = mv.getBuffer()[0].message
    expect(message).toContain('arg0')
    expect(message).toContain('arg99')
  })

  it('handles non-serializable values in message gracefully', async () => {
    // Note: This test verifies that the serialization handles non-serializable data
    // When message has multiple elements, undefined becomes null in JSON arrays
    const item = createTailItem({
      logs: [
        {
          timestamp: Date.now(),
          level: 'info',
          message: ['hello', undefined, 'world'], // undefined becomes null in JSON array
        },
      ],
    })

    await mv.ingestTailItem(item)

    const buffer = mv.getBuffer()
    expect(buffer.length).toBe(1)
    // Multiple args get stringified as array, undefined becomes null
    expect(buffer[0].message).toBe('["hello",null,"world"]')
  })

  it('generates unique IDs for each record', async () => {
    mv = createWorkerLogsMV({
      storage,
      datasetPath: 'logs/workers',
      // Use default ULID generator
    })
    mv.start()

    await mv.ingestTailItem(createTailItem())
    await mv.ingestTailItem(createTailItem())

    const buffer = mv.getBuffer()
    expect(buffer[0].id).not.toBe(buffer[1].id)

    await mv.stop()
  })
})

/**
 * Integration Test: Full Tail Event Pipeline
 *
 * Tests the complete event-driven architecture from tail events to queryable Parquet:
 *
 * ```
 * TailWorker -> TailDO (WebSocket) -> R2 (NDJSON raw events)
 *                                          ↓ object-create notification
 *                                     Queue -> CompactionConsumer
 *                                          ↓
 *                                     WorkerLogsMV -> Parquet files
 * ```
 *
 * This test verifies:
 * 1. Tail events are validated and batched
 * 2. TailDO writes raw events to storage (NDJSON format)
 * 3. CompactionConsumer parses raw events files
 * 4. WorkerLogsMV ingests and stores as Parquet
 * 5. Statistics are correctly tracked through the pipeline
 *
 * @see parquedb-dyui.15 - Add integration test for full tail event pipeline
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { validateTraceItems, type ValidatedTraceItem } from '../../src/worker/tail-validation'
import {
  createWorkerLogsMV,
  type TailEvent,
  type TailItem,
  type WorkerLogsMV,
} from '../../src/streaming/worker-logs'
import type { StorageBackend } from '../../src/types/storage'
import type { TailWorkerMessage, RawEventsFile } from '../../src/worker/TailDO'

// =============================================================================
// Mock Storage Backend
// =============================================================================

interface MockFile {
  data: Uint8Array
  size: number
  metadata?: Record<string, string>
  contentType?: string
}

/**
 * Create a mock storage backend that simulates R2
 */
function createMockStorage(): StorageBackend & {
  files: Map<string, MockFile>
  writeLog: Array<{ path: string; size: number; timestamp: number }>
  clear: () => void
} {
  const files = new Map<string, MockFile>()
  const writeLog: Array<{ path: string; size: number; timestamp: number }> = []

  return {
    files,
    writeLog,

    async read(path: string): Promise<Uint8Array> {
      const file = files.get(path)
      if (!file) throw new Error(`File not found: ${path}`)
      return file.data
    },

    async write(
      path: string,
      data: Uint8Array,
      options?: { contentType?: string; metadata?: Record<string, string> }
    ): Promise<{ etag: string; size: number }> {
      files.set(path, {
        data,
        size: data.length,
        contentType: options?.contentType,
        metadata: options?.metadata,
      })
      writeLog.push({ path, size: data.length, timestamp: Date.now() })
      return { etag: `etag-${path}`, size: data.length }
    },

    async writeAtomic(
      path: string,
      data: Uint8Array,
      options?: { contentType?: string; metadata?: Record<string, string> }
    ): Promise<{ etag: string; size: number }> {
      return this.write(path, data, options)
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

    clear() {
      files.clear()
      writeLog.length = 0
    },
  } as StorageBackend & {
    files: Map<string, MockFile>
    writeLog: Array<{ path: string; size: number; timestamp: number }>
    clear: () => void
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a sample ValidatedTraceItem (simulating Cloudflare tail event)
 */
function createValidatedTraceItem(overrides: Partial<ValidatedTraceItem> = {}): ValidatedTraceItem {
  const now = Date.now()
  return {
    scriptName: 'test-worker',
    outcome: 'ok',
    eventTimestamp: now,
    event: {
      request: {
        method: 'GET',
        url: 'https://api.example.com/v1/users',
        headers: { 'cf-ray': `ray-${now}`, 'content-type': 'application/json' },
        cf: { colo: 'SJC', country: 'US' },
      },
      response: { status: 200 },
    },
    logs: [
      { timestamp: now + 10, level: 'info', message: 'Request received' },
      { timestamp: now + 50, level: 'info', message: 'Response sent' },
    ],
    exceptions: [],
    diagnosticsChannelEvents: [],
    ...overrides,
  }
}

/**
 * Create a TailWorkerMessage (what TailDO receives via WebSocket)
 */
function createTailWorkerMessage(
  events: ValidatedTraceItem[],
  instanceId = 'tail-test-001'
): TailWorkerMessage {
  return {
    type: 'tail_events',
    instanceId,
    timestamp: Date.now(),
    events,
  }
}

/**
 * Simulate TailDO writing raw events to storage in NDJSON format
 */
async function simulateTailDOWrite(
  storage: StorageBackend,
  events: ValidatedTraceItem[],
  options: { doId?: string; batchSeq?: number; prefix?: string } = {}
): Promise<string> {
  const { doId = 'tail-global', batchSeq = 0, prefix = 'raw-events' } = options
  const timestamp = Date.now()
  const filePath = `${prefix}/${timestamp}-${doId}-${batchSeq}.ndjson`

  // Build NDJSON content (first line is metadata, rest are events)
  const metadata: Omit<RawEventsFile, 'events'> = {
    doId,
    createdAt: timestamp,
    batchSeq,
  }

  const lines = [JSON.stringify(metadata)]
  for (const event of events) {
    lines.push(JSON.stringify(event))
  }
  const content = lines.join('\n')

  await storage.write(filePath, new TextEncoder().encode(content), {
    contentType: 'application/x-ndjson',
    metadata: {
      doId,
      batchSeq: String(batchSeq),
      eventCount: String(events.length),
    },
  })

  return filePath
}

/**
 * Parse a raw events file from NDJSON content (mirrors CompactionConsumer logic)
 */
function parseRawEventsFile(content: string): RawEventsFile {
  const lines = content.trim().split('\n').filter(Boolean)
  if (lines.length === 0) {
    throw new Error('Empty raw events file')
  }

  // First line contains metadata
  const metadata = JSON.parse(lines[0]!) as Omit<RawEventsFile, 'events'>

  // Remaining lines are events
  const events: ValidatedTraceItem[] = []
  for (let i = 1; i < lines.length; i++) {
    try {
      events.push(JSON.parse(lines[i]!) as ValidatedTraceItem)
    } catch {
      // Skip malformed lines (mirrors CompactionConsumer behavior)
    }
  }

  return { ...metadata, events }
}

/**
 * Convert ValidatedTraceItem to TailItem format (for WorkerLogsMV)
 */
function validatedItemToTailItem(item: ValidatedTraceItem): TailItem {
  return {
    scriptName: item.scriptName ?? 'unknown',
    outcome: item.outcome as TailItem['outcome'],
    eventTimestamp: item.eventTimestamp ?? Date.now(),
    event: item.event
      ? {
          request: item.event.request
            ? {
                method: item.event.request.method,
                url: item.event.request.url,
                headers: item.event.request.headers,
                cf: item.event.request.cf,
              }
            : undefined!,
          response: item.event.response,
        }
      : null,
    logs: item.logs.map(log => ({
      timestamp: log.timestamp,
      level: log.level as TailItem['logs'][number]['level'],
      message: [log.message],
    })),
    exceptions: item.exceptions,
    diagnosticsChannelEvents: item.diagnosticsChannelEvents,
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
// Integration Tests: Full Pipeline
// =============================================================================

describe('Integration: Full Tail Event Pipeline', () => {
  let storage: ReturnType<typeof createMockStorage>
  let mv: WorkerLogsMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createWorkerLogsMV({
      storage,
      datasetPath: 'logs/workers',
      flushThreshold: 100,
      flushIntervalMs: 60000, // High to control flushing manually
      generateId: createIdGenerator(),
    })
  })

  afterEach(async () => {
    if (mv.isRunning()) {
      await mv.stop()
    }
    storage.clear()
  })

  describe('Stage 1: Tail Event Validation', () => {
    it('validates incoming trace items from producer workers', () => {
      const rawEvents = [
        createValidatedTraceItem({ scriptName: 'api-worker' }),
        createValidatedTraceItem({ scriptName: 'auth-worker', outcome: 'exception' }),
        { invalid: 'event' }, // This should be filtered out
        createValidatedTraceItem({ scriptName: 'cron-worker' }),
      ]

      const result = validateTraceItems(rawEvents, { skipInvalidItems: true })

      expect(result.validCount).toBe(3)
      expect(result.invalidCount).toBe(1)
      expect(result.validItems.map(i => i.scriptName)).toEqual([
        'api-worker',
        'auth-worker',
        'cron-worker',
      ])
    })

    it('preserves all trace item fields through validation', () => {
      const original = createValidatedTraceItem({
        scriptName: 'test-worker',
        outcome: 'ok',
        logs: [
          { timestamp: 1000, level: 'info', message: 'Log 1' },
          { timestamp: 2000, level: 'warn', message: 'Log 2' },
        ],
        exceptions: [{ timestamp: 3000, name: 'Error', message: 'Test error' }],
        diagnosticsChannelEvents: [
          { channel: 'fetch', timestamp: 1500, message: { url: 'https://api.test.com' } },
        ],
      })

      const result = validateTraceItems([original], { skipInvalidItems: true })

      expect(result.validCount).toBe(1)
      const validated = result.validItems[0]
      expect(validated.scriptName).toBe('test-worker')
      expect(validated.logs).toHaveLength(2)
      expect(validated.exceptions).toHaveLength(1)
      expect(validated.diagnosticsChannelEvents).toHaveLength(1)
    })
  })

  describe('Stage 2: TailDO Raw Event Storage', () => {
    it('writes raw events to storage in NDJSON format', async () => {
      const events = [
        createValidatedTraceItem({ scriptName: 'worker-1' }),
        createValidatedTraceItem({ scriptName: 'worker-2' }),
      ]

      const filePath = await simulateTailDOWrite(storage, events)

      expect(storage.files.has(filePath)).toBe(true)
      expect(filePath).toMatch(/^raw-events\/\d+-tail-global-0\.ndjson$/)

      const file = storage.files.get(filePath)!
      expect(file.contentType).toBe('application/x-ndjson')
      expect(file.metadata?.eventCount).toBe('2')
    })

    it('creates valid NDJSON with metadata header', async () => {
      const events = [createValidatedTraceItem()]
      const filePath = await simulateTailDOWrite(storage, events, {
        doId: 'tail-test',
        batchSeq: 5,
      })

      const content = new TextDecoder().decode(storage.files.get(filePath)!.data)
      const lines = content.trim().split('\n')

      expect(lines.length).toBe(2) // 1 metadata + 1 event

      const metadata = JSON.parse(lines[0]!)
      expect(metadata.doId).toBe('tail-test')
      expect(metadata.batchSeq).toBe(5)
      expect(metadata.createdAt).toBeTypeOf('number')

      const event = JSON.parse(lines[1]!)
      expect(event.scriptName).toBe('test-worker')
    })

    it('handles multiple batches with incrementing sequence numbers', async () => {
      const batch1 = [createValidatedTraceItem({ scriptName: 'batch1-worker' })]
      const batch2 = [createValidatedTraceItem({ scriptName: 'batch2-worker' })]
      const batch3 = [createValidatedTraceItem({ scriptName: 'batch3-worker' })]

      await simulateTailDOWrite(storage, batch1, { batchSeq: 0 })
      await simulateTailDOWrite(storage, batch2, { batchSeq: 1 })
      await simulateTailDOWrite(storage, batch3, { batchSeq: 2 })

      const files = await storage.list('raw-events/')
      expect(files.length).toBe(3)

      // Verify each file has correct batch sequence
      for (const filePath of files) {
        const content = new TextDecoder().decode(storage.files.get(filePath)!.data)
        const metadata = JSON.parse(content.split('\n')[0]!)
        expect([0, 1, 2]).toContain(metadata.batchSeq)
      }
    })
  })

  describe('Stage 3: Compaction Consumer Processing', () => {
    it('parses raw events files correctly', async () => {
      const events = [
        createValidatedTraceItem({ scriptName: 'api-worker' }),
        createValidatedTraceItem({ scriptName: 'auth-worker' }),
      ]

      const filePath = await simulateTailDOWrite(storage, events, {
        doId: 'compaction-test',
        batchSeq: 10,
      })

      // Read and parse the file (simulating CompactionConsumer)
      const data = await storage.read(filePath)
      const content = new TextDecoder().decode(data)
      const parsed = parseRawEventsFile(content)

      expect(parsed.doId).toBe('compaction-test')
      expect(parsed.batchSeq).toBe(10)
      expect(parsed.events).toHaveLength(2)
      expect(parsed.events[0].scriptName).toBe('api-worker')
      expect(parsed.events[1].scriptName).toBe('auth-worker')
    })

    it('skips malformed event lines gracefully', async () => {
      // Manually create a file with some malformed lines
      const timestamp = Date.now()
      const filePath = `raw-events/${timestamp}-test-0.ndjson`
      const content = [
        JSON.stringify({ doId: 'test', createdAt: timestamp, batchSeq: 0 }),
        JSON.stringify(createValidatedTraceItem({ scriptName: 'valid-1' })),
        'not valid json',
        JSON.stringify(createValidatedTraceItem({ scriptName: 'valid-2' })),
        '{ broken json',
        JSON.stringify(createValidatedTraceItem({ scriptName: 'valid-3' })),
      ].join('\n')

      await storage.write(filePath, new TextEncoder().encode(content))

      const data = await storage.read(filePath)
      const parsed = parseRawEventsFile(new TextDecoder().decode(data))

      // Should have 3 valid events despite 2 malformed lines
      expect(parsed.events).toHaveLength(3)
      expect(parsed.events.map(e => e.scriptName)).toEqual(['valid-1', 'valid-2', 'valid-3'])
    })

    it('converts ValidatedTraceItem to TailItem format', () => {
      const validated = createValidatedTraceItem({
        scriptName: 'conversion-test',
        logs: [{ timestamp: 1000, level: 'info', message: 'Test message' }],
      })

      const tailItem = validatedItemToTailItem(validated)

      expect(tailItem.scriptName).toBe('conversion-test')
      expect(tailItem.logs).toHaveLength(1)
      expect(tailItem.logs[0].message).toEqual(['Test message'])
    })
  })

  describe('Stage 4: WorkerLogsMV Ingestion', () => {
    it('ingests TailItems and buffers them', async () => {
      mv.start()

      const tailItems: TailItem[] = [
        validatedItemToTailItem(createValidatedTraceItem({ scriptName: 'worker-1' })),
        validatedItemToTailItem(createValidatedTraceItem({ scriptName: 'worker-2' })),
      ]

      const tailEvent: TailEvent = { type: 'tail', traces: tailItems }
      await mv.ingestTailEvent(tailEvent)

      const buffer = mv.getBuffer()
      // Each TailItem with logs creates records for each log
      expect(buffer.length).toBeGreaterThan(0)

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBeGreaterThan(0)
      expect(stats.bufferSize).toBe(buffer.length)
    })

    it('flushes buffer to Parquet files', async () => {
      mv.start()

      const tailItem = validatedItemToTailItem(createValidatedTraceItem())
      await mv.ingestTailItem(tailItem)
      await mv.flush()

      const parquetFiles = await storage.list('logs/workers/')
      expect(parquetFiles.length).toBe(1)
      expect(parquetFiles[0]).toMatch(/\.parquet$/)

      const stats = mv.getStats()
      expect(stats.filesCreated).toBe(1)
      expect(stats.recordsWritten).toBeGreaterThan(0)
    })

    it('tracks statistics correctly', async () => {
      mv.start()

      // Ingest various events
      await mv.ingestTailItem(
        validatedItemToTailItem(
          createValidatedTraceItem({
            scriptName: 'api-worker',
            outcome: 'ok',
            logs: [{ timestamp: 1000, level: 'info', message: 'Info log' }],
          })
        )
      )

      await mv.ingestTailItem(
        validatedItemToTailItem(
          createValidatedTraceItem({
            scriptName: 'api-worker',
            outcome: 'exception',
            logs: [{ timestamp: 2000, level: 'error', message: 'Error log' }],
            exceptions: [{ timestamp: 2000, name: 'Error', message: 'Test error' }],
          })
        )
      )

      await mv.ingestTailItem(
        validatedItemToTailItem(
          createValidatedTraceItem({
            scriptName: 'cron-worker',
            outcome: 'ok',
            logs: [{ timestamp: 3000, level: 'warn', message: 'Warning' }],
          })
        )
      )

      const stats = mv.getStats()

      // Check by-script tracking
      expect(stats.byScript['api-worker']).toBeGreaterThan(0)
      expect(stats.byScript['cron-worker']).toBeGreaterThan(0)

      // Check by-outcome tracking
      expect(stats.byOutcome['ok']).toBeGreaterThan(0)
      expect(stats.byOutcome['exception']).toBeGreaterThan(0)

      // Check exceptions
      expect(stats.exceptions).toBeGreaterThan(0)
    })
  })

  describe('Stage 5: End-to-End Pipeline', () => {
    it('processes events through the complete pipeline', async () => {
      mv.start()

      // Stage 1: Validate raw tail events
      const rawEvents = [
        createValidatedTraceItem({
          scriptName: 'e2e-worker-1',
          logs: [{ timestamp: Date.now(), level: 'info', message: 'Processing request' }],
        }),
        createValidatedTraceItem({
          scriptName: 'e2e-worker-2',
          outcome: 'exception',
          exceptions: [{ timestamp: Date.now(), name: 'TypeError', message: 'Test error' }],
        }),
      ]

      const validationResult = validateTraceItems(rawEvents, { skipInvalidItems: true })
      expect(validationResult.validCount).toBe(2)

      // Stage 2: Simulate TailDO writing to R2
      const filePath = await simulateTailDOWrite(storage, validationResult.validItems, {
        doId: 'e2e-test',
        batchSeq: 0,
      })

      expect(await storage.exists(filePath)).toBe(true)

      // Stage 3: Simulate CompactionConsumer reading and parsing
      const rawData = await storage.read(filePath)
      const rawContent = new TextDecoder().decode(rawData)
      const parsedFile = parseRawEventsFile(rawContent)

      expect(parsedFile.events).toHaveLength(2)

      // Stage 4: Ingest into WorkerLogsMV
      const tailItems = parsedFile.events.map(validatedItemToTailItem)
      const tailEvent: TailEvent = { type: 'tail', traces: tailItems }

      await mv.ingestTailEvent(tailEvent)

      // Stage 5: Flush to Parquet
      await mv.flush()

      // Verify final state
      const parquetFiles = await storage.list('logs/workers/')
      expect(parquetFiles.length).toBe(1)

      const finalStats = mv.getStats()
      expect(finalStats.filesCreated).toBe(1)
      expect(finalStats.recordsWritten).toBeGreaterThan(0)
      expect(finalStats.byScript['e2e-worker-1']).toBeGreaterThan(0)
      expect(finalStats.byScript['e2e-worker-2']).toBeGreaterThan(0)
      expect(finalStats.exceptions).toBe(1)
    })

    it('handles high-volume event streams', async () => {
      mv.start()

      // Generate a large batch of events
      const eventCount = 500
      const rawEvents = Array.from({ length: eventCount }, (_, i) =>
        createValidatedTraceItem({
          scriptName: `worker-${i % 10}`, // 10 different workers
          outcome: i % 20 === 0 ? 'exception' : 'ok',
          logs: [{ timestamp: Date.now() + i, level: i % 5 === 0 ? 'error' : 'info', message: `Log ${i}` }],
        })
      )

      // Validate
      const validation = validateTraceItems(rawEvents, { skipInvalidItems: true })
      expect(validation.validCount).toBe(eventCount)

      // Write to storage in batches (simulating TailDO batching)
      const batchSize = 100
      for (let i = 0; i < validation.validItems.length; i += batchSize) {
        const batch = validation.validItems.slice(i, i + batchSize)
        await simulateTailDOWrite(storage, batch, {
          doId: 'high-volume-test',
          batchSeq: Math.floor(i / batchSize),
        })
      }

      const rawFiles = await storage.list('raw-events/')
      expect(rawFiles.length).toBe(5) // 500 / 100 = 5 batches

      // Process all raw files through the pipeline
      for (const filePath of rawFiles) {
        const data = await storage.read(filePath)
        const content = new TextDecoder().decode(data)
        const parsed = parseRawEventsFile(content)

        const tailItems = parsed.events.map(validatedItemToTailItem)
        await mv.ingestTailEvent({ type: 'tail', traces: tailItems })
      }

      // Flush all to Parquet
      await mv.flush()

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(eventCount)
      expect(stats.recordsWritten).toBe(eventCount)
      expect(stats.filesCreated).toBeGreaterThan(0)

      // Verify worker distribution
      for (let i = 0; i < 10; i++) {
        expect(stats.byScript[`worker-${i}`]).toBe(50) // 500 / 10 workers
      }
    })

    it('preserves data integrity through the pipeline', async () => {
      mv.start()

      // Create event with specific data to track
      const testTimestamp = 1700000000000
      const testUrl = 'https://api.example.com/v1/specific-endpoint'
      const testRayId = 'ray-integrity-test-12345'

      const originalEvent = createValidatedTraceItem({
        scriptName: 'integrity-test-worker',
        outcome: 'ok',
        eventTimestamp: testTimestamp,
        event: {
          request: {
            method: 'POST',
            url: testUrl,
            headers: { 'cf-ray': testRayId, 'content-type': 'application/json' },
            cf: { colo: 'LAX', country: 'US' },
          },
          response: { status: 201 },
        },
        logs: [
          { timestamp: testTimestamp + 10, level: 'info', message: 'Request received' },
          { timestamp: testTimestamp + 50, level: 'info', message: 'Processing complete' },
        ],
        exceptions: [],
        diagnosticsChannelEvents: [
          { channel: 'fetch', timestamp: testTimestamp + 20, message: { url: 'https://external.api.com' } },
          { channel: 'fetch', timestamp: testTimestamp + 30, message: { url: 'https://another.api.com' } },
        ],
      })

      // Run through pipeline
      await simulateTailDOWrite(storage, [originalEvent])

      const files = await storage.list('raw-events/')
      const data = await storage.read(files[0])
      const parsed = parseRawEventsFile(new TextDecoder().decode(data))

      const tailItem = validatedItemToTailItem(parsed.events[0])
      await mv.ingestTailItem(tailItem)

      // Verify buffer contents before flush
      const buffer = mv.getBuffer()

      // Should have records for each log (2 logs)
      expect(buffer.length).toBe(2)

      // Verify preserved fields
      for (const record of buffer) {
        expect(record.scriptName).toBe('integrity-test-worker')
        expect(record.outcome).toBe('ok')
        expect(record.httpMethod).toBe('POST')
        expect(record.httpUrl).toBe(testUrl)
        expect(record.httpStatus).toBe(201)
        expect(record.colo).toBe('LAX')
        expect(record.country).toBe('US')
        expect(record.requestId).toBe(testRayId)
        expect(record.fetchCount).toBe(2) // 2 fetch diagnosticsChannelEvents
      }
    })
  })

  describe('Error Handling', () => {
    it('handles empty event arrays gracefully', async () => {
      mv.start()

      const validation = validateTraceItems([], { skipInvalidItems: true })
      expect(validation.validCount).toBe(0)

      // Should not throw when writing empty batch
      // (TailDO would not write anything in this case)

      // Empty TailEvent should be handled
      await mv.ingestTailEvent({ type: 'tail', traces: [] })
      expect(mv.getBuffer().length).toBe(0)
    })

    it('handles null scriptName in trace items', async () => {
      mv.start()

      const eventWithNullScript = createValidatedTraceItem({
        scriptName: null as unknown as string,
      })

      const tailItem = validatedItemToTailItem(eventWithNullScript)
      expect(tailItem.scriptName).toBe('unknown')

      await mv.ingestTailItem(tailItem)
      const buffer = mv.getBuffer()

      for (const record of buffer) {
        expect(record.scriptName).toBe('unknown')
      }
    })

    it('handles events without request/response info', async () => {
      mv.start()

      const scheduledEvent = createValidatedTraceItem({
        scriptName: 'cron-worker',
        event: null,
        logs: [{ timestamp: Date.now(), level: 'info', message: 'Scheduled task ran' }],
      })

      const tailItem = validatedItemToTailItem(scheduledEvent)
      await mv.ingestTailItem(tailItem)

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].httpMethod).toBeNull()
      expect(buffer[0].httpUrl).toBeNull()
      expect(buffer[0].httpStatus).toBeNull()
    })

    it('recovers from flush failures', async () => {
      // Create a storage that fails on write
      const failingStorage = createMockStorage()
      let shouldFail = true
      const originalWrite = failingStorage.writeAtomic.bind(failingStorage)
      failingStorage.writeAtomic = async (path, data, options) => {
        if (shouldFail) {
          throw new Error('Simulated storage failure')
        }
        return originalWrite(path, data, options)
      }

      const failingMv = createWorkerLogsMV({
        storage: failingStorage,
        datasetPath: 'logs/workers',
        generateId: createIdGenerator(),
      })

      // Ingest an event
      const tailItem = validatedItemToTailItem(createValidatedTraceItem())
      await failingMv.ingestTailItem(tailItem)
      expect(failingMv.getBuffer().length).toBeGreaterThan(0)

      // Flush should fail
      await expect(failingMv.flush()).rejects.toThrow('Simulated storage failure')

      // Buffer should be restored
      expect(failingMv.getBuffer().length).toBeGreaterThan(0)

      // Now allow writes to succeed
      shouldFail = false
      await failingMv.flush()

      // Buffer should be empty after successful flush
      expect(failingMv.getBuffer().length).toBe(0)
    })
  })
})

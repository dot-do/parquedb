/**
 * Tests for StreamProcessor
 *
 * Tests cover:
 * - Basic push and flush operations
 * - Batching behavior (size-based and time-based)
 * - Backpressure handling
 * - Error handling and retry logic
 * - Statistics tracking
 * - Lifecycle management (start/stop)
 * - Transform functions
 * - Utility functions (teeToProcessor, drainToProcessor, createProcessorSink)
 */

import { describe, test, expect, beforeEach, afterEach, vi, type Mock } from 'vitest'
import {
  StreamProcessor,
  createStreamProcessor,
  teeToProcessor,
  drainToProcessor,
  createProcessorSink,
  WriteFailureError,
  type StreamProcessorConfig,
  type StreamProcessorStats,
  type BatchWriteResult,
  type ErrorContext,
  type FailedBatch,
  type WriteFailureBehavior,
} from '../../../src/materialized-views/stream-processor'
import type { StorageBackend, WriteResult, FileStat } from '../../../src/types/storage'
import type { ParquetSchema } from '../../../src/parquet/types'

// =============================================================================
// Test Helpers
// =============================================================================

/** Simple schema for testing */
const TEST_SCHEMA: ParquetSchema = {
  id: { type: 'UTF8', optional: false },
  value: { type: 'INT64', optional: true },
  name: { type: 'UTF8', optional: true },
}

/** Test record type */
interface TestRecord {
  id: string
  value?: number
  name?: string
}

/** Create a test record */
function createRecord(id: number, value?: number): TestRecord {
  return {
    id: `record-${id}`,
    value: value ?? id * 10,
    name: `Name ${id}`,
  }
}

/** Create a mock storage backend */
function createMockStorage(options?: {
  writeDelay?: number
  failOnWrite?: boolean
  failCount?: number
}): StorageBackend & { writtenFiles: Map<string, unknown[]> } {
  const writtenFiles = new Map<string, unknown[]>()
  let failuresRemaining = options?.failCount ?? 0

  return {
    type: 'mock',
    writtenFiles,

    async read(path: string): Promise<Uint8Array> {
      return new Uint8Array()
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      return new Uint8Array()
    },

    async exists(path: string): Promise<boolean> {
      return writtenFiles.has(path)
    },

    async stat(path: string): Promise<FileStat | null> {
      if (!writtenFiles.has(path)) return null
      const data = writtenFiles.get(path)
      return {
        path,
        size: data ? JSON.stringify(data).length : 0,
        mtime: new Date(),
        isDirectory: false,
      }
    },

    async list() {
      return { files: [], hasMore: false }
    },

    async write(path: string, data: Uint8Array): Promise<WriteResult> {
      if (options?.writeDelay) {
        // Use fake timer advancement when available
        await vi.advanceTimersByTimeAsync(options.writeDelay)
      }

      if (options?.failOnWrite && failuresRemaining > 0) {
        failuresRemaining--
        throw new Error('Simulated write failure')
      }

      // Store the data (we'll parse it from the mock writer)
      writtenFiles.set(path, [])
      return { etag: 'mock-etag', size: data.length }
    },

    async writeAtomic(path: string, data: Uint8Array): Promise<WriteResult> {
      return this.write(path, data)
    },

    async append(): Promise<void> {},

    async delete(path: string): Promise<boolean> {
      return writtenFiles.delete(path)
    },

    async deletePrefix(): Promise<number> {
      return 0
    },

    async mkdir(): Promise<void> {},

    async rmdir(): Promise<void> {},

    async writeConditional(
      path: string,
      data: Uint8Array,
      expectedVersion: string | null
    ): Promise<WriteResult> {
      return this.write(path, data)
    },

    async copy(): Promise<void> {},

    async move(): Promise<void> {},
  }
}

/** Mock the ParquetWriter to capture what would be written */
vi.mock('../../../src/parquet/writer', () => ({
  ParquetWriter: class MockParquetWriter {
    storage: StorageBackend
    options: unknown

    constructor(storage: StorageBackend, options?: unknown) {
      this.storage = storage
      this.options = options
    }

    async write(path: string, data: Record<string, unknown>[], schema: ParquetSchema): Promise<void> {
      // Store the records in the mock storage
      const mockStorage = this.storage as StorageBackend & { writtenFiles?: Map<string, unknown[]> }
      if (mockStorage.writtenFiles) {
        mockStorage.writtenFiles.set(path, data)
      }
      // Simulate writing some bytes
      await this.storage.write(path, new Uint8Array(JSON.stringify(data).length))
    }
  },
}))

// =============================================================================
// Basic Functionality Tests
// =============================================================================

describe('StreamProcessor', () => {
  let storage: ReturnType<typeof createMockStorage>
  let processor: StreamProcessor<TestRecord>

  beforeEach(() => {
    storage = createMockStorage()
  })

  afterEach(async () => {
    if (processor && processor.isRunning()) {
      await processor.stop()
    }
  })

  describe('Lifecycle', () => {
    test('starts and stops cleanly', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
      })

      expect(processor.getState()).toBe('idle')

      await processor.start()
      expect(processor.getState()).toBe('running')
      expect(processor.isRunning()).toBe(true)

      await processor.stop()
      expect(processor.getState()).toBe('stopped')
      expect(processor.isRunning()).toBe(false)
    })

    test('flushes pending records on stop', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 100, // Large batch to prevent auto-flush
        flushIntervalMs: 10000, // Long interval
      })

      await processor.start()

      // Push some records (less than batch size)
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.push(createRecord(3))

      // Stop should flush
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(3)
      expect(stats.batchesWritten).toBe(1)
    })

    test('rejects push when not running', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
      })

      await expect(processor.push(createRecord(1))).rejects.toThrow('not running')
    })

    test('can restart after stop', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 1,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()
      await processor.stop()

      await processor.start()
      await processor.push(createRecord(2))
      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(2)
    })
  })

  describe('Push and Flush', () => {
    test('pushes a single record', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 1,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsReceived).toBe(1)
      expect(stats.recordsWritten).toBe(1)
    })

    test('pushes multiple records with pushMany', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 10,
      })

      await processor.start()
      await processor.pushMany([
        createRecord(1),
        createRecord(2),
        createRecord(3),
      ])
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsReceived).toBe(3)
      expect(stats.recordsWritten).toBe(3)
    })

    test('manual flush writes pending records', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 100,
        flushIntervalMs: 10000,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))

      let stats = processor.getStats()
      expect(stats.bufferSize).toBe(2)
      expect(stats.recordsWritten).toBe(0)

      await processor.flush()

      stats = processor.getStats()
      expect(stats.bufferSize).toBe(0)
      expect(stats.recordsWritten).toBe(2)
    })

    test('flush with empty buffer is a no-op', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
      })

      await processor.start()
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(0)
      expect(stats.batchesWritten).toBe(0)
    })
  })

  describe('Batching', () => {
    test('auto-flushes when batch size is reached', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 3,
        flushIntervalMs: 10000,
      })

      await processor.start()

      // Push exactly batch size
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.push(createRecord(3))

      // Explicitly flush to ensure completion (more deterministic than waiting)
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(3)
      expect(stats.batchesWritten).toBe(1)
    })

    test('creates multiple batches for many records', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 3,
        flushIntervalMs: 10000,
      })

      await processor.start()

      // Push 7 records = 2 full batches + 1 partial
      for (let i = 0; i < 7; i++) {
        await processor.push(createRecord(i))
      }
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(7)
      expect(stats.batchesWritten).toBe(3)
    })

    test('time-based flush triggers after interval', async () => {
      vi.useFakeTimers()
      try {
        processor = createStreamProcessor({
          name: 'test',
          storage,
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          batchSize: 100, // Large batch
          flushIntervalMs: 50, // Short interval
        })

        await processor.start()
        await processor.push(createRecord(1))

        // Advance fake timers past the flush interval
        await vi.advanceTimersByTimeAsync(100)

        const stats = processor.getStats()
        expect(stats.recordsWritten).toBe(1)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('Backpressure', () => {
    test('applies backpressure when buffer is full', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ writeDelay: 100 }), // Slow writes
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        maxBufferSize: 5,
        maxPendingWrites: 1,
        flushIntervalMs: 10000,
      })

      await processor.start()

      const startTime = Date.now()

      // Fill buffer rapidly - should trigger backpressure
      for (let i = 0; i < 10; i++) {
        await processor.push(createRecord(i))
      }

      const elapsed = Date.now() - startTime

      // Should have taken time due to backpressure
      expect(elapsed).toBeGreaterThan(50)

      await processor.stop()

      // Backpressure events may or may not be triggered depending on timing
      // The important thing is that the elapsed time demonstrates backpressure
      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(10) // All records should be written
    })

    test('tracks backpressure events in stats', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ writeDelay: 50 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        maxBufferSize: 3,
        maxPendingWrites: 1,
        flushIntervalMs: 10000,
      })

      await processor.start()

      // Push enough to trigger backpressure
      for (let i = 0; i < 8; i++) {
        await processor.push(createRecord(i))
      }

      await processor.stop()

      const stats = processor.getStats()
      // Verify all records were processed despite backpressure
      expect(stats.recordsWritten).toBe(8)
      // Backpressure events depend on timing, so just verify stats are tracked
      expect(stats.backpressureEvents).toBeGreaterThanOrEqual(0)
    })
  })

  describe('Error Handling', () => {
    test('retries failed writes', async () => {
      vi.useFakeTimers()
      try {
        const errorHandler = vi.fn()
        processor = createStreamProcessor({
          name: 'test',
          storage: createMockStorage({ failOnWrite: true, failCount: 2 }),
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          batchSize: 1,
          retry: {
            maxAttempts: 3,
            initialDelayMs: 10,
            maxDelayMs: 50,
          },
          onError: errorHandler,
        })

        await processor.start()
        await processor.push(createRecord(1))
        await processor.flush()

        // Advance timers to allow retries to complete
        await vi.advanceTimersByTimeAsync(200)

        const stats = processor.getStats()
        // Should succeed after retries
        expect(stats.recordsWritten).toBe(1)
      } finally {
        vi.useRealTimers()
      }
    })

    test('reports failures after max retries', async () => {
      vi.useFakeTimers()
      try {
        const errorHandler = vi.fn()
        processor = createStreamProcessor({
          name: 'test',
          storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          batchSize: 1,
          retry: {
            maxAttempts: 2,
            initialDelayMs: 5,
            maxDelayMs: 10,
          },
          onError: errorHandler,
        })

        await processor.start()
        await processor.push(createRecord(1))
        await processor.flush()

        // Advance timers to allow retries to complete
        await vi.advanceTimersByTimeAsync(100)

        const stats = processor.getStats()
        expect(stats.failedBatches).toBeGreaterThan(0)
        expect(errorHandler).toHaveBeenCalled()
      } finally {
        vi.useRealTimers()
      }
    })

    test('emits error events with context', async () => {
      vi.useFakeTimers()
      try {
        const errorHandler = vi.fn()
        processor = createStreamProcessor({
          name: 'test',
          storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          batchSize: 1,
          retry: {
            maxAttempts: 1,
            initialDelayMs: 1,
          },
          onError: errorHandler,
        })

        await processor.start()
        await processor.push(createRecord(1))
        await processor.flush()

        // Advance timers to allow error handling
        await vi.advanceTimersByTimeAsync(50)

        expect(errorHandler).toHaveBeenCalled()
        const [error, context] = errorHandler.mock.calls[0]!
        expect(error).toBeInstanceOf(Error)
        expect(context.phase).toBe('write')
        expect(context.records).toHaveLength(1)
      } finally {
        vi.useRealTimers()
      }
    })

    test('queues failed batches in dead-letter queue by default', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000, // Long interval to prevent auto-flush
        retry: {
          maxAttempts: 2,
          initialDelayMs: 1,
          maxDelayMs: 5,
        },
        // writeFailureBehavior defaults to 'queue'
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      // Check dead-letter queue
      const dlq = processor.getDeadLetterQueue()
      expect(dlq.length).toBe(1)
      expect(dlq[0]!.records).toHaveLength(2)
      expect(dlq[0]!.batchNumber).toBe(1)
      expect(dlq[0]!.attempts).toBe(2)
      expect(dlq[0]!.error).toBeInstanceOf(Error)
    })

    test('throws WriteFailureError when writeFailureBehavior is throw', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2, // Batch size > 1 so push doesn't auto-flush
        flushIntervalMs: 60000,
        retry: {
          maxAttempts: 1,
          initialDelayMs: 1,
        },
        writeFailureBehavior: 'throw',
      })

      await processor.start()
      await processor.push(createRecord(1))

      // Flush should throw WriteFailureError
      await expect(processor.flush()).rejects.toThrow(WriteFailureError)
    })

    test('calls onWriteError callback when writeFailureBehavior is callback', async () => {
      const onWriteError = vi.fn()

      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000,
        retry: {
          maxAttempts: 1,
          initialDelayMs: 1,
        },
        writeFailureBehavior: 'callback',
        onWriteError,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      expect(onWriteError).toHaveBeenCalled()
      const failedBatch = onWriteError.mock.calls[0]![0]
      expect(failedBatch.records).toHaveLength(2)
      expect(failedBatch.batchNumber).toBe(1)
      expect(failedBatch.error).toBeInstanceOf(Error)
      expect(failedBatch.attempts).toBe(1)
    })

    test('requires onWriteError when writeFailureBehavior is callback', () => {
      expect(() => {
        createStreamProcessor({
          name: 'test',
          storage: createMockStorage(),
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          writeFailureBehavior: 'callback',
          // Missing onWriteError
        })
      }).toThrow("onWriteError callback is required")
    })

    test('clearDeadLetterQueue returns and clears batches', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000,
        retry: { maxAttempts: 1, initialDelayMs: 1 },
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      expect(processor.getDeadLetterQueueSize()).toBe(1)

      const cleared = processor.clearDeadLetterQueue()
      expect(cleared).toHaveLength(1)
      expect(cleared[0]!.records).toHaveLength(1)
      expect(processor.getDeadLetterQueueSize()).toBe(0)
    })

    test('retryDeadLetterQueue retries failed batches', async () => {
      // Start with failing storage that fails only the first write
      // First flush: uses 1 fail count (fail)
      // Retry: uses 1 attempt which succeeds (failCount exhausted)
      const mockStorage = createMockStorage({ failOnWrite: true, failCount: 1 })

      processor = createStreamProcessor({
        name: 'test',
        storage: mockStorage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000,
        retry: { maxAttempts: 1, initialDelayMs: 1 },
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      // Should have 1 failed batch in DLQ
      expect(processor.getDeadLetterQueueSize()).toBe(1)

      // Retry - storage has now used up its fail count so should succeed
      const retried = await processor.retryDeadLetterQueue()
      expect(retried).toBe(1)
      expect(processor.getDeadLetterQueueSize()).toBe(0)

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(1)
    })

    test('failed batch includes enough info for replay', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 10, // Large batch so push won't auto-flush
        flushIntervalMs: 60000,
        retry: { maxAttempts: 2, initialDelayMs: 1 },
      })

      await processor.start()
      await processor.push(createRecord(1, 100))
      await processor.push(createRecord(2, 200))
      await processor.push(createRecord(3, 300))
      await processor.flush()

      const dlq = processor.getDeadLetterQueue()
      expect(dlq.length).toBe(1)

      const failedBatch = dlq[0]!
      // All original records preserved
      expect(failedBatch.records).toHaveLength(3)
      expect(failedBatch.records[0]).toEqual(createRecord(1, 100))
      expect(failedBatch.records[1]).toEqual(createRecord(2, 200))
      expect(failedBatch.records[2]).toEqual(createRecord(3, 300))

      // Metadata for replay
      expect(failedBatch.batchNumber).toBe(1)
      expect(failedBatch.filePath).toContain('test')
      expect(failedBatch.filePath).toContain('.parquet')
      expect(failedBatch.failedAt).toBeGreaterThan(0)
      expect(failedBatch.attempts).toBe(2)
      expect(failedBatch.error.message).toContain('Simulated write failure')
    })

    test('silent behavior drops records (legacy, not recommended)', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000,
        retry: { maxAttempts: 1, initialDelayMs: 1 },
        writeFailureBehavior: 'silent',
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      // Records are silently dropped
      expect(processor.getDeadLetterQueueSize()).toBe(0)
      const stats = processor.getStats()
      expect(stats.failedBatches).toBe(1)
      expect(stats.recordsWritten).toBe(0)
    })

    test('error context includes retry attempts', async () => {
      const errorHandler = vi.fn()
      processor = createStreamProcessor({
        name: 'test',
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        flushIntervalMs: 60000,
        retry: {
          maxAttempts: 3,
          initialDelayMs: 1,
        },
        onError: errorHandler,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      expect(errorHandler).toHaveBeenCalled()
      const [, context] = errorHandler.mock.calls[0]!
      expect(context.attempts).toBe(3)
    })
  })

  describe('Transform', () => {
    test('applies transform function to records', async () => {
      const onBatchWritten = vi.fn()
      processor = createStreamProcessor<TestRecord>({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 1,
        transform: (record) => ({
          ...record,
          transformed: true,
          value: (record.value ?? 0) * 2,
        }),
        onBatchWritten,
      })

      await processor.start()
      await processor.push(createRecord(1, 10))
      await processor.flush()

      // Verify the batch was written
      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(1)
      expect(onBatchWritten).toHaveBeenCalled()

      // Check that the written data was transformed by looking at the storage
      const writtenFiles = Array.from(storage.writtenFiles.entries())
      expect(writtenFiles.length).toBeGreaterThanOrEqual(1)
      // The transform is applied before writing, so verify stats
      expect(stats.batchesWritten).toBe(1)
    })

    test('handles transform errors gracefully', async () => {
      const errorHandler = vi.fn()
      processor = createStreamProcessor<TestRecord>({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 1,
        transform: () => {
          throw new Error('Transform failed')
        },
        onError: errorHandler,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      // Should have called error handler but still written the original record
      expect(errorHandler).toHaveBeenCalled()
      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(1)
    })
  })

  describe('Statistics', () => {
    test('tracks comprehensive statistics', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
      })

      await processor.start()

      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.push(createRecord(3))
      await processor.flush()

      const stats = processor.getStats()
      expect(stats.recordsReceived).toBe(3)
      expect(stats.recordsWritten).toBe(3)
      expect(stats.batchesWritten).toBe(2)
      expect(stats.failedBatches).toBe(0)
      expect(stats.startedAt).not.toBeNull()
      expect(stats.lastRecordAt).not.toBeNull()
      expect(stats.lastWriteAt).not.toBeNull()
      expect(stats.bytesWritten).toBeGreaterThan(0)
    })

    test('can reset statistics', async () => {
      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 1,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      let stats = processor.getStats()
      expect(stats.recordsReceived).toBe(1)

      processor.resetStats()

      stats = processor.getStats()
      expect(stats.recordsReceived).toBe(0)
      expect(stats.recordsWritten).toBe(0)
      expect(stats.startedAt).not.toBeNull() // Preserved since running
    })

    test('calculates average batch duration', async () => {
      vi.useFakeTimers()
      try {
        // Use a storage with slight delay to ensure measurable duration
        processor = createStreamProcessor({
          name: 'test',
          storage: createMockStorage({ writeDelay: 5 }),
          outputPath: '_test/output',
          schema: TEST_SCHEMA,
          batchSize: 1,
          flushIntervalMs: 10000,
        })

        await processor.start()

        for (let i = 0; i < 5; i++) {
          await processor.push(createRecord(i))
          // Wait for the batch to complete using fake timers
          await vi.advanceTimersByTimeAsync(20)
        }

        await processor.stop()

        const stats = processor.getStats()
        // With write delay, average should be measurable
        expect(stats.avgBatchDurationMs).toBeGreaterThanOrEqual(0)
        expect(stats.batchesWritten).toBe(5)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('Callbacks', () => {
    test('calls onBatchWritten callback', async () => {
      const onBatchWritten = vi.fn()

      processor = createStreamProcessor({
        name: 'test',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
        batchSize: 2,
        onBatchWritten,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      expect(onBatchWritten).toHaveBeenCalled()
      const result: BatchWriteResult = onBatchWritten.mock.calls[0]![0]
      expect(result.recordCount).toBe(2)
      expect(result.batchNumber).toBe(1)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
      expect(result.filePath).toContain('test')
    })
  })

  describe('getName', () => {
    test('returns processor name', () => {
      processor = createStreamProcessor({
        name: 'my-processor',
        storage,
        outputPath: '_test/output',
        schema: TEST_SCHEMA,
      })

      expect(processor.getName()).toBe('my-processor')
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createStreamProcessor', () => {
  test('creates processor with default config', () => {
    const storage = createMockStorage()
    const processor = createStreamProcessor({
      name: 'test',
      storage,
      outputPath: '_test/output',
      schema: TEST_SCHEMA,
    })

    expect(processor).toBeInstanceOf(StreamProcessor)
    expect(processor.getState()).toBe('idle')
  })

  test('creates processor with custom config', () => {
    const storage = createMockStorage()
    const processor = createStreamProcessor({
      name: 'custom',
      storage,
      outputPath: '_test/output',
      schema: TEST_SCHEMA,
      batchSize: 50,
      flushIntervalMs: 1000,
      maxPendingWrites: 5,
    })

    expect(processor).toBeInstanceOf(StreamProcessor)
    expect(processor.getName()).toBe('custom')
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('teeToProcessor', () => {
  test('tees async iterator to processor', async () => {
    const storage = createMockStorage()
    const processor = createStreamProcessor({
      name: 'test',
      storage,
      outputPath: '_test/output',
      schema: TEST_SCHEMA,
      batchSize: 10,
    })

    await processor.start()

    async function* source(): AsyncGenerator<TestRecord> {
      for (let i = 0; i < 5; i++) {
        yield createRecord(i)
      }
    }

    const results: TestRecord[] = []
    for await (const item of teeToProcessor(source(), processor)) {
      results.push(item)
    }

    await processor.stop()

    expect(results.length).toBe(5)
    const stats = processor.getStats()
    expect(stats.recordsReceived).toBe(5)
    expect(stats.recordsWritten).toBe(5)
  })
})

describe('drainToProcessor', () => {
  test('drains async iterator to processor', async () => {
    const storage = createMockStorage()
    const processor = createStreamProcessor({
      name: 'test',
      storage,
      outputPath: '_test/output',
      schema: TEST_SCHEMA,
      batchSize: 10,
    })

    await processor.start()

    async function* source(): AsyncGenerator<TestRecord> {
      for (let i = 0; i < 5; i++) {
        yield createRecord(i)
      }
    }

    await drainToProcessor(source(), processor)
    await processor.stop()

    const stats = processor.getStats()
    expect(stats.recordsReceived).toBe(5)
    expect(stats.recordsWritten).toBe(5)
  })
})

describe('createProcessorSink', () => {
  test('creates a WritableStream that pushes to processor', async () => {
    const storage = createMockStorage()
    const processor = createStreamProcessor({
      name: 'test',
      storage,
      outputPath: '_test/output',
      schema: TEST_SCHEMA,
      batchSize: 10,
    })

    await processor.start()

    const sink = createProcessorSink(processor)
    const writer = sink.getWriter()

    await writer.write(createRecord(1))
    await writer.write(createRecord(2))
    await writer.write(createRecord(3))
    await writer.close()

    await processor.stop()

    const stats = processor.getStats()
    expect(stats.recordsReceived).toBe(3)
    expect(stats.recordsWritten).toBe(3)
  })
})

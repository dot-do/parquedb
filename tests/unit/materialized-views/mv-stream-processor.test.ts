/**
 * Tests for MVStreamProcessor
 *
 * Tests cover:
 * - Integration with MV infrastructure (metadata updates)
 * - Filter and projection application
 * - State management (pending -> building -> ready/error)
 * - Incremental updates tracking
 * - Error handling with MV state updates
 * - Statistics tracking with MV-specific fields
 */

import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  MVStreamProcessor,
  createMVStreamProcessor,
  type MVStreamProcessorConfig,
  type MVStreamProcessorStats,
  type MVBatchWriteResult,
  type MVErrorContext,
} from '../../../src/materialized-views/stream-processor'
import type { StorageBackend, WriteResult, FileStat } from '../../../src/types/storage'
import type { ParquetSchema } from '../../../src/parquet/types'
import type { MVStorageManager } from '../../../src/materialized-views/storage'
import type { ViewMetadata, ViewState, ViewDefinition} from '../../../src/materialized-views/types'

// =============================================================================
// Test Helpers
// =============================================================================

/** Simple schema for testing */
const TEST_SCHEMA: ParquetSchema = {
  id: { type: 'UTF8', optional: false },
  value: { type: 'INT64', optional: true },
  status: { type: 'UTF8', optional: true },
  model: { type: 'UTF8', optional: true },
}

/** Test record type */
interface TestRecord {
  id: string
  value?: number | undefined
  status?: string | undefined
  model?: string | undefined
}

/** Create a test record */
function createRecord(id: number, options?: { value?: number; status?: string; model?: string }): TestRecord {
  return {
    id: `record-${id}`,
    value: options?.value ?? id * 10,
    status: options?.status ?? 'active',
    model: options?.model ?? 'gpt-4',
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

/** Create a mock MV storage manager */
function createMockMVStorage(options?: {
  initialState?: ViewState
  throwOnGetMetadata?: boolean
  throwOnSaveMetadata?: boolean
}): MVStorageManager & {
  stateUpdates: ViewState[]
  metadataSaves: ViewMetadata[]
} {
  const stateUpdates: ViewState[] = []
  const metadataSaves: ViewMetadata[] = []
  let currentState: ViewState = options?.initialState ?? 'pending'
  let version = 1

  const mockDefinition: ViewDefinition = {
    name: 'test-view',
    source: 'test-source',
    query: {},
    options: { refreshMode: 'streaming' },
  }

  const mockMetadata: ViewMetadata = {
    definition: mockDefinition,
    state: currentState,
    createdAt: new Date(),
    version,
  }

  return {
    stateUpdates,
    metadataSaves,

    async getViewMetadata(name: string): Promise<ViewMetadata> {
      if (options?.throwOnGetMetadata) {
        throw new Error('Simulated getViewMetadata failure')
      }
      return { ...mockMetadata, state: currentState, version }
    },

    async saveViewMetadata(name: string, metadata: ViewMetadata): Promise<WriteResult> {
      if (options?.throwOnSaveMetadata) {
        throw new Error('Simulated saveViewMetadata failure')
      }
      metadataSaves.push({ ...metadata })
      version = metadata.version
      return { etag: 'mock-etag', size: 100 }
    },

    async updateViewState(name: string, state: ViewState): Promise<void> {
      stateUpdates.push(state)
      currentState = state
    },
  } as unknown as MVStorageManager & {
    stateUpdates: ViewState[]
    metadataSaves: ViewMetadata[]
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
      const mockStorage = this.storage as StorageBackend & { writtenFiles?: Map<string, unknown[]> }
      if (mockStorage.writtenFiles) {
        mockStorage.writtenFiles.set(path, data)
      }
      await this.storage.write(path, new Uint8Array(JSON.stringify(data).length))
    }
  },
}))

// =============================================================================
// MVStreamProcessor Tests
// =============================================================================

describe('MVStreamProcessor', () => {
  let storage: ReturnType<typeof createMockStorage>
  let mvStorage: ReturnType<typeof createMockMVStorage>
  let processor: MVStreamProcessor<TestRecord>

  beforeEach(() => {
    vi.useFakeTimers()
    storage = createMockStorage()
    mvStorage = createMockMVStorage()
  })

  afterEach(async () => {
    if (processor && processor.isRunning()) {
      await processor.stop()
    }
    vi.useRealTimers()
  })

  describe('Lifecycle', () => {
    test('starts and updates state to building', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
      })

      await processor.start()

      expect(processor.isRunning()).toBe(true)
      expect(mvStorage.stateUpdates).toContain('building')
    })

    test('stops and updates state to ready on success', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 10,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()
      await processor.stop()

      expect(processor.isRunning()).toBe(false)
      expect(mvStorage.stateUpdates).toContain('ready')
    })

    test('stops and updates state to error on failures', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        schema: TEST_SCHEMA,
        batchSize: 1,
        retry: { maxAttempts: 1, initialDelayMs: 1 },
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      // Wait for error handling
      await vi.advanceTimersByTimeAsync(50)

      await processor.stop()

      expect(mvStorage.stateUpdates).toContain('error')
    })
  })

  describe('Filtering', () => {
    test('filters records based on configured filter', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        filter: { status: 'active' },
        batchSize: 10,
      })

      await processor.start()

      // Push active records
      await processor.push(createRecord(1, { status: 'active' }))
      await processor.push(createRecord(2, { status: 'inactive' }))
      await processor.push(createRecord(3, { status: 'active' }))

      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsFilteredOut).toBe(1)
      expect(stats.recordsWritten).toBe(2)
    })

    test('filters with complex filter conditions', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        filter: {
          status: 'active',
          value: { $gt: 15 }
        },
        batchSize: 10,
      })

      await processor.start()

      await processor.push(createRecord(1, { status: 'active', value: 10 })) // filtered (value too low)
      await processor.push(createRecord(2, { status: 'active', value: 20 })) // passes
      await processor.push(createRecord(3, { status: 'inactive', value: 20 })) // filtered (wrong status)
      await processor.push(createRecord(4, { status: 'active', value: 30 })) // passes

      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsFilteredOut).toBe(2)
      expect(stats.recordsWritten).toBe(2)
    })

    test('passes all records when no filter configured', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 10,
      })

      await processor.start()

      await processor.push(createRecord(1, { status: 'active' }))
      await processor.push(createRecord(2, { status: 'inactive' }))
      await processor.push(createRecord(3, { status: 'pending' }))

      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsFilteredOut).toBe(0)
      expect(stats.recordsWritten).toBe(3)
    })
  })

  describe('Projection', () => {
    test('applies projection to records', async () => {
      const onBatchWritten = vi.fn()

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        project: {
          id: 1,
          model: 1,
        },
        batchSize: 1,
        onBatchWritten,
      })

      await processor.start()
      await processor.push(createRecord(1, { model: 'gpt-4' }))
      await processor.flush()
      await processor.stop()

      expect(onBatchWritten).toHaveBeenCalled()
    })

    test('applies field mapping in projection', async () => {
      const onBatchWritten = vi.fn()

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        project: {
          entityId: '$id',
          modelName: 'model',
        },
        batchSize: 1,
        onBatchWritten,
      })

      await processor.start()
      await processor.push(createRecord(1, { model: 'gpt-4' }))
      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()
      expect(stats.recordsWritten).toBe(1)
    })
  })

  describe('Statistics', () => {
    test('tracks MV-specific statistics', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        filter: { status: 'active' },
        batchSize: 2,
      })

      await processor.start()

      // Push records with some filtered out
      await processor.push(createRecord(1, { status: 'active' }))
      await processor.push(createRecord(2, { status: 'inactive' }))
      await processor.push(createRecord(3, { status: 'active' }))
      await processor.push(createRecord(4, { status: 'active' }))

      await processor.flush()
      await processor.stop()

      const stats = processor.getStats()

      expect(stats.viewName).toBe('test-view')
      expect(stats.recordsFilteredOut).toBe(1)
      expect(stats.totalMVRecords).toBe(3)
      expect(stats.mvState).toBe('ready')
    })

    test('resets statistics', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 1,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      let stats = processor.getStats()
      expect(stats.recordsWritten).toBe(1)

      processor.resetStats()

      stats = processor.getStats()
      expect(stats.recordsWritten).toBe(0)
      expect(stats.recordsFilteredOut).toBe(0)
    })
  })

  describe('Metadata Updates', () => {
    test('updates metadata after each batch when configured', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 2,
        updateMetadataOnBatch: true,
      })

      await processor.start()

      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      // Wait for async metadata update
      await vi.advanceTimersByTimeAsync(50)

      expect(mvStorage.metadataSaves.length).toBeGreaterThan(0)
      const lastSave = mvStorage.metadataSaves[mvStorage.metadataSaves.length - 1]!
      expect(lastSave.documentCount).toBe(2)
    })

    test('does not update metadata when disabled', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 2,
        updateMetadataOnBatch: false,
      })

      await processor.start()

      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      // Wait a bit
      await vi.advanceTimersByTimeAsync(50)

      // No metadata saves during batches
      expect(mvStorage.metadataSaves.length).toBe(0)
    })

    test('handles metadata update failures gracefully', async () => {
      const failingMVStorage = createMockMVStorage({ throwOnSaveMetadata: true })

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage: failingMVStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 1,
        updateMetadataOnBatch: true,
      })

      await processor.start()

      // Should not throw despite metadata failures
      await expect(processor.push(createRecord(1))).resolves.not.toThrow()
      await expect(processor.flush()).resolves.not.toThrow()
    })
  })

  describe('Callbacks', () => {
    test('calls onBatchWritten with MV-specific result', async () => {
      const onBatchWritten = vi.fn()

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        filter: { status: 'active' },
        batchSize: 2,
        onBatchWritten,
      })

      await processor.start()

      await processor.push(createRecord(1, { status: 'active' }))
      await processor.push(createRecord(2, { status: 'inactive' }))
      await processor.push(createRecord(3, { status: 'active' }))
      await processor.flush()

      expect(onBatchWritten).toHaveBeenCalled()
      const result: MVBatchWriteResult = onBatchWritten.mock.calls[0]![0]
      expect(result.viewName).toBe('test-view')
      expect(result.recordCount).toBe(2)
    })

    test('calls onStateChange when state changes', async () => {
      const onStateChange = vi.fn()

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        onStateChange,
      })

      await processor.start()
      await processor.stop()

      expect(onStateChange).toHaveBeenCalledWith('building', 'pending')
      expect(onStateChange).toHaveBeenCalledWith('ready', 'building')
    })

    test('calls onError with MV context', async () => {
      const onError = vi.fn()

      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage: createMockStorage({ failOnWrite: true, failCount: 10 }),
        schema: TEST_SCHEMA,
        batchSize: 1,
        retry: { maxAttempts: 1, initialDelayMs: 1 },
        onError,
      })

      await processor.start()
      await processor.push(createRecord(1))
      await processor.flush()

      // Wait for error handling
      await vi.advanceTimersByTimeAsync(100)

      expect(onError).toHaveBeenCalled()
      const context: MVErrorContext<TestRecord> = onError.mock.calls[0]![1]
      expect(context.viewName).toBe('test-view')
    })
  })

  describe('Getters', () => {
    test('getViewName returns view name', () => {
      processor = createMVStreamProcessor({
        viewName: 'my-test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
      })

      expect(processor.getViewName()).toBe('my-test-view')
    })

    test('getMVState returns current state', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
      })

      expect(processor.getMVState()).toBe('pending')

      await processor.start()
      expect(processor.getMVState()).toBe('building')

      await processor.stop()
      expect(processor.getMVState()).toBe('ready')
    })

    test('getTotalRecords returns cumulative count', async () => {
      processor = createMVStreamProcessor({
        viewName: 'test-view',
        mvStorage,
        storage,
        schema: TEST_SCHEMA,
        batchSize: 2,
      })

      await processor.start()

      expect(processor.getTotalRecords()).toBe(0)

      await processor.push(createRecord(1))
      await processor.push(createRecord(2))
      await processor.flush()

      // Wait for batch to complete
      await vi.advanceTimersByTimeAsync(50)

      expect(processor.getTotalRecords()).toBe(2)

      await processor.push(createRecord(3))
      await processor.push(createRecord(4))
      await processor.flush()

      // Wait for batch to complete
      await vi.advanceTimersByTimeAsync(50)

      expect(processor.getTotalRecords()).toBe(4)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createMVStreamProcessor', () => {
  let storage: ReturnType<typeof createMockStorage>
  let mvStorage: ReturnType<typeof createMockMVStorage>

  beforeEach(() => {
    storage = createMockStorage()
    mvStorage = createMockMVStorage()
  })

  test('creates processor with config', () => {
    const processor = createMVStreamProcessor({
      viewName: 'test-view',
      mvStorage,
      storage,
      schema: TEST_SCHEMA,
    })

    expect(processor).toBeInstanceOf(MVStreamProcessor)
    expect(processor.getViewName()).toBe('test-view')
  })

  test('creates processor with filter and projection', () => {
    const processor = createMVStreamProcessor({
      viewName: 'test-view',
      mvStorage,
      storage,
      schema: TEST_SCHEMA,
      filter: { status: 'active' },
      project: { id: 1, status: 1 },
    })

    expect(processor).toBeInstanceOf(MVStreamProcessor)
  })
})

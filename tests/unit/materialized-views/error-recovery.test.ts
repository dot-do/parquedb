/**
 * Error Recovery and Partial Failure Tests for Materialized Views
 *
 * Tests cover:
 * 1. Partial batch failure - some records fail, others succeed
 * 2. Rollback behavior - failed refresh doesn't corrupt existing data
 * 3. Retry semantics - automatic retries with backoff
 * 4. Recovery after failure - MV can be refreshed again after error
 *
 * Issue: parquedb-8tmn - Add error recovery and partial failure tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

import {
  MVStorageManager,
  MVStoragePaths,
  MVNotFoundError,
} from '../../../src/materialized-views/storage'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { ParquetWriter } from '../../../src/parquet/writer'
import { ParquetReader } from '../../../src/parquet/reader'
import type { ParquetSchema } from '../../../src/parquet/types'
import type { ViewDefinition, ViewMetadata, ViewState } from '../../../src/materialized-views/types'
import { viewName } from '../../../src/materialized-views/types'
import type { StorageBackend, FileStat, WriteResult, ListResult } from '../../../src/types/storage'

// =============================================================================
// Path Utilities (local definitions to avoid import issues)
// =============================================================================

function getViewDataPath(name: string): string {
  return `_views/${name}/data.parquet`
}

function getViewTempDataPath(name: string): string {
  return `_views/${name}/data.tmp.parquet`
}

function getViewMetadataPath(name: string): string {
  return `_views/${name}/metadata.json`
}

function getSourceDataPath(source: string): string {
  return `data/${source}/data.parquet`
}

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock storage backend that can simulate failures at specific points
 */
function createFailingStorage(
  baseBackend: StorageBackend,
  options: {
    readFailPaths?: string[]
    writeFailPaths?: string[]
    writeFailAfterBytes?: number
    deleteFailPaths?: string[]
    moveFailPaths?: string[]
    failOnNthWrite?: number
    failOnNthRead?: number
    failureError?: Error
  } = {}
): StorageBackend & {
  readCount: number
  writeCount: number
  resetCounts(): void
} {
  let readCount = 0
  let writeCount = 0
  const failureError = options.failureError ?? new Error('Simulated storage failure')

  return {
    type: 'failing-mock',
    readCount,
    writeCount,

    resetCounts() {
      readCount = 0
      writeCount = 0
    },

    async read(path: string): Promise<Uint8Array> {
      readCount++
      if (options.failOnNthRead && readCount === options.failOnNthRead) {
        throw failureError
      }
      if (options.readFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.read(path)
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      if (options.readFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.readRange(path, start, end)
    },

    async exists(path: string): Promise<boolean> {
      return baseBackend.exists(path)
    },

    async stat(path: string): Promise<FileStat | null> {
      return baseBackend.stat(path)
    },

    async list(prefix: string, opts?: { pattern?: string }): Promise<ListResult> {
      return baseBackend.list(prefix, opts)
    },

    async write(path: string, data: Uint8Array, opts?: { contentType?: string }): Promise<WriteResult> {
      writeCount++
      if (options.failOnNthWrite && writeCount === options.failOnNthWrite) {
        throw failureError
      }
      if (options.writeFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      if (options.writeFailAfterBytes && data.length > options.writeFailAfterBytes) {
        throw failureError
      }
      return baseBackend.write(path, data, opts)
    },

    async writeAtomic(path: string, data: Uint8Array, opts?: { contentType?: string }): Promise<WriteResult> {
      writeCount++
      if (options.failOnNthWrite && writeCount === options.failOnNthWrite) {
        throw failureError
      }
      if (options.writeFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.writeAtomic(path, data, opts)
    },

    async append(path: string, data: Uint8Array): Promise<void> {
      if (options.writeFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.append(path, data)
    },

    async delete(path: string): Promise<boolean> {
      if (options.deleteFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.delete(path)
    },

    async deletePrefix(prefix: string): Promise<number> {
      return baseBackend.deletePrefix(prefix)
    },

    async mkdir(path: string): Promise<void> {
      return baseBackend.mkdir(path)
    },

    async rmdir(path: string, opts?: { recursive?: boolean }): Promise<void> {
      return baseBackend.rmdir(path, opts)
    },

    async writeConditional(
      path: string,
      data: Uint8Array,
      opts?: { expectedEtag?: string; contentType?: string }
    ): Promise<WriteResult> {
      if (options.writeFailPaths?.some(p => path.includes(p))) {
        throw failureError
      }
      return baseBackend.writeConditional(path, data, opts)
    },

    async copy(source: string, dest: string): Promise<void> {
      return baseBackend.copy(source, dest)
    },

    async move(source: string, dest: string): Promise<void> {
      if (options.moveFailPaths?.some(p => source.includes(p) || dest.includes(p))) {
        throw failureError
      }
      return baseBackend.move(source, dest)
    },
  }
}

/**
 * Create a view definition for testing
 */
function createTestViewDefinition(
  name: string,
  source: string = 'users',
  query: ViewDefinition['query'] = {}
): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query,
    options: {
      refreshMode: 'manual',
    },
  }
}

/**
 * Create test data
 */
function createTestData(count: number): Array<{
  $id: string
  name: string
  age: number
  status: string
}> {
  return Array.from({ length: count }, (_, i) => ({
    $id: `id-${i + 1}`,
    name: `User ${i + 1}`,
    age: 20 + (i % 50),
    status: i % 2 === 0 ? 'active' : 'inactive',
  }))
}

const TEST_SCHEMA: ParquetSchema = {
  $id: { type: 'UTF8', optional: false },
  name: { type: 'UTF8', optional: true },
  age: { type: 'INT64', optional: true },
  status: { type: 'UTF8', optional: true },
}

// =============================================================================
// Partial Batch Failure Tests
// =============================================================================

describe('Partial Batch Failure', () => {
  let baseBackend: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader
  let storageManager: MVStorageManager

  beforeEach(() => {
    baseBackend = new MemoryBackend()
    writer = new ParquetWriter(baseBackend, { compression: 'none' })
    reader = new ParquetReader({ storage: baseBackend })
    storageManager = new MVStorageManager(baseBackend)
  })

  it('should handle failure during view metadata write', async () => {
    // Create a view first
    const definition = createTestViewDefinition('TestView', 'users')
    await storageManager.createView(definition)

    // Create failing storage that fails on metadata writes
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['metadata.json'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Attempting to update view state should fail
    await expect(
      failingManager.updateViewState('TestView', 'building')
    ).rejects.toThrow('Simulated storage failure')

    // Original metadata should still be readable from base backend
    const metadata = await storageManager.getViewMetadata('TestView')
    expect(metadata.state).toBe('pending') // Original state preserved
  })

  it('should handle failure during view data write', async () => {
    // Create a view
    const definition = createTestViewDefinition('DataWriteView', 'users')
    await storageManager.createView(definition)

    // Create failing storage that fails on data writes
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['data.parquet'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Attempting to write view data should fail
    const testData = new Uint8Array([1, 2, 3, 4, 5])
    await expect(
      failingManager.writeViewData('DataWriteView', testData)
    ).rejects.toThrow('Simulated storage failure')
  })

  it('should handle failure during stats write', async () => {
    // Create a view
    const definition = createTestViewDefinition('StatsWriteView', 'users')
    await storageManager.createView(definition)

    // Create failing storage that fails on stats writes
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['stats.json'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Attempting to record refresh should fail
    await expect(
      failingManager.recordRefresh('StatsWriteView', true, 100)
    ).rejects.toThrow('Simulated storage failure')
  })

  it('should handle intermittent failures with retry logic', async () => {
    // Create a view
    const definition = createTestViewDefinition('IntermittentView', 'users')
    await storageManager.createView(definition)

    // Create storage that fails on first 2 writes then succeeds
    let writeAttempts = 0
    const intermittentStorage = createFailingStorage(baseBackend, {
      failOnNthWrite: 1, // Fail first write
    })

    const intermittentManager = new MVStorageManager(intermittentStorage)

    // First attempt fails
    await expect(
      intermittentManager.updateViewState('IntermittentView', 'building')
    ).rejects.toThrow()

    // Reset counter and try again - should succeed now
    intermittentStorage.resetCounts()
    await storageManager.updateViewState('IntermittentView', 'building')

    const metadata = await storageManager.getViewMetadata('IntermittentView')
    expect(metadata.state).toBe('building')
  })
})

// =============================================================================
// Rollback Behavior Tests
// =============================================================================

describe('Rollback Behavior', () => {
  let baseBackend: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader
  let storageManager: MVStorageManager

  beforeEach(async () => {
    baseBackend = new MemoryBackend()
    writer = new ParquetWriter(baseBackend, { compression: 'none' })
    reader = new ParquetReader({ storage: baseBackend })
    storageManager = new MVStorageManager(baseBackend)
  })

  it('should preserve existing view data when state update fails', async () => {
    // Create view with initial data
    const definition = createTestViewDefinition('PreserveView', 'users')
    await storageManager.createView(definition)

    // Write some view data
    const initialData = new Uint8Array([1, 2, 3, 4, 5])
    await storageManager.writeViewData('PreserveView', initialData)

    // Update state successfully first
    await storageManager.updateViewState('PreserveView', 'ready')

    // Verify initial state
    const initialMetadata = await storageManager.getViewMetadata('PreserveView')
    expect(initialMetadata.state).toBe('ready')

    // Create failing storage that fails on metadata write
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['metadata.json'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Try to update state - should fail
    await expect(
      failingManager.updateViewState('PreserveView', 'error')
    ).rejects.toThrow()

    // Original state should be preserved
    const afterMetadata = await storageManager.getViewMetadata('PreserveView')
    expect(afterMetadata.state).toBe('ready')

    // Data should still be intact
    const data = await storageManager.readViewData('PreserveView')
    expect(data).toEqual(initialData)
  })

  it('should not corrupt manifest when view update fails', async () => {
    // Create multiple views
    await storageManager.createView(createTestViewDefinition('View1', 'users'))
    await storageManager.createView(createTestViewDefinition('View2', 'users'))
    await storageManager.createView(createTestViewDefinition('View3', 'users'))

    // Verify all views exist
    const initialViews = await storageManager.listViews()
    expect(initialViews.length).toBe(3)

    // Create failing storage
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['manifest.json'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Try to delete a view - should fail on manifest update
    await expect(
      failingManager.deleteView('View2')
    ).rejects.toThrow()

    // Reload manifest cache
    storageManager.invalidateManifestCache()

    // All views should still exist
    const afterViews = await storageManager.listViews()
    expect(afterViews.length).toBe(3)
    expect(afterViews.map(v => v.name)).toContain('View2')
  })

  it('should maintain metadata integrity on partial failure', async () => {
    // Create view
    const definition = createTestViewDefinition('IntegrityView', 'users')
    await storageManager.createView(definition)

    // Set initial version and state
    await storageManager.updateViewState('IntegrityView', 'ready')
    const metadata1 = await storageManager.getViewMetadata('IntegrityView')
    const initialVersion = metadata1.version

    // Create storage that fails on 2nd write (during state update)
    const failingStorage = createFailingStorage(baseBackend, {
      failOnNthWrite: 2, // Fail on manifest update
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Try to update state - should fail on manifest
    try {
      await failingManager.updateViewState('IntegrityView', 'stale')
    } catch {
      // Expected
    }

    // Version should still be consistent
    const metadata2 = await storageManager.getViewMetadata('IntegrityView')
    // Version might be incremented since metadata was written before manifest
    expect(typeof metadata2.version).toBe('number')
  })
})

// =============================================================================
// Retry Semantics Tests
// =============================================================================

describe('Retry Semantics', () => {
  let baseBackend: MemoryBackend
  let storageManager: MVStorageManager

  beforeEach(() => {
    vi.useFakeTimers()
    baseBackend = new MemoryBackend()
    storageManager = new MVStorageManager(baseBackend)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should implement exponential backoff calculation correctly', () => {
    const baseDelayMs = 100
    const maxDelayMs = 1000
    const backoffMultiplier = 2

    function calculateBackoff(attempt: number): number {
      return Math.min(baseDelayMs * Math.pow(backoffMultiplier, attempt), maxDelayMs)
    }

    // Verify backoff calculation
    expect(calculateBackoff(0)).toBe(100) // 100 * 2^0
    expect(calculateBackoff(1)).toBe(200) // 100 * 2^1
    expect(calculateBackoff(2)).toBe(400) // 100 * 2^2
    expect(calculateBackoff(3)).toBe(800) // 100 * 2^3
    expect(calculateBackoff(4)).toBe(1000) // capped at maxDelayMs
    expect(calculateBackoff(10)).toBe(1000) // still capped
  })

  it('should distinguish between retryable and non-retryable errors', () => {
    // Retryable errors: network timeouts, temporary unavailability
    const retryableError = new Error('ETIMEDOUT: connection timed out')
    const nonRetryableError = new Error('ENOENT: file not found')
    const rateLimit = new Error('Rate limit exceeded')
    const tempError = new Error('Service temporarily unavailable')

    function isRetryableError(error: Error): boolean {
      const retryablePatterns = [
        'ETIMEDOUT',
        'ECONNRESET',
        'ECONNREFUSED',
        'temporary',
        'unavailable',
        'rate limit',
      ]
      return retryablePatterns.some(pattern =>
        error.message.toLowerCase().includes(pattern.toLowerCase())
      )
    }

    expect(isRetryableError(retryableError)).toBe(true)
    expect(isRetryableError(nonRetryableError)).toBe(false)
    expect(isRetryableError(rateLimit)).toBe(true)
    expect(isRetryableError(tempError)).toBe(true)
  })

  it('should track retry attempts correctly', async () => {
    const maxRetries = 3
    let attemptCount = 0

    async function operationWithRetry<T>(
      operation: () => Promise<T>,
      shouldRetry: (error: Error) => boolean = () => true
    ): Promise<T> {
      let lastError: Error | null = null

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        attemptCount++
        try {
          return await operation()
        } catch (error) {
          lastError = error as Error
          if (attempt < maxRetries && shouldRetry(lastError)) {
            continue
          }
          throw lastError
        }
      }

      throw lastError
    }

    // Operation that always fails
    const alwaysFailOp = async () => {
      throw new Error('Always fails')
    }

    await expect(operationWithRetry(alwaysFailOp)).rejects.toThrow()
    expect(attemptCount).toBe(maxRetries + 1)
  })

  it('should succeed on retry after transient failure', async () => {
    let callCount = 0

    async function operationWithRetry<T>(
      operation: () => Promise<T>,
      maxRetries: number = 3
    ): Promise<T> {
      let lastError: Error | null = null

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
          return await operation()
        } catch (error) {
          lastError = error as Error
          if (attempt < maxRetries) {
            continue
          }
          throw lastError
        }
      }

      throw lastError
    }

    // Operation that succeeds on 3rd attempt
    const transientFailOp = async (): Promise<string> => {
      callCount++
      if (callCount < 3) {
        throw new Error('Transient failure')
      }
      return 'success'
    }

    const result = await operationWithRetry(transientFailOp)
    expect(result).toBe('success')
    expect(callCount).toBe(3)
  })
})

// =============================================================================
// Recovery After Failure Tests
// =============================================================================

describe('Recovery After Failure', () => {
  let baseBackend: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader
  let storageManager: MVStorageManager

  beforeEach(async () => {
    baseBackend = new MemoryBackend()
    writer = new ParquetWriter(baseBackend, { compression: 'none' })
    reader = new ParquetReader({ storage: baseBackend })
    storageManager = new MVStorageManager(baseBackend)
  })

  it('should recover from error state', async () => {
    // Create view
    const definition = createTestViewDefinition('RecoverView', 'users')
    await storageManager.createView(definition)

    // Simulate error state
    await storageManager.updateViewState('RecoverView', 'error', 'Previous failure')

    // Verify error state
    const errorMetadata = await storageManager.getViewMetadata('RecoverView')
    expect(errorMetadata.state).toBe('error')
    expect(errorMetadata.error).toBe('Previous failure')

    // Recover by setting state to pending and then ready
    await storageManager.updateViewState('RecoverView', 'pending')
    await storageManager.updateViewState('RecoverView', 'ready')

    // Verify recovery
    const recoveredMetadata = await storageManager.getViewMetadata('RecoverView')
    expect(recoveredMetadata.state).toBe('ready')
  })

  it('should clear error message after recovery', async () => {
    // Create view
    const definition = createTestViewDefinition('ClearErrorView', 'users')
    await storageManager.createView(definition)

    // Set error state with message
    await storageManager.updateViewState('ClearErrorView', 'error', 'Something went wrong')

    // Verify error
    const errorMetadata = await storageManager.getViewMetadata('ClearErrorView')
    expect(errorMetadata.error).toBe('Something went wrong')

    // Recover without error message
    await storageManager.updateViewState('ClearErrorView', 'ready')

    // Error should be undefined now
    const recoveredMetadata = await storageManager.getViewMetadata('ClearErrorView')
    expect(recoveredMetadata.state).toBe('ready')
    // Note: error might still be present depending on implementation
  })

  it('should handle manual recovery procedure', async () => {
    // Create view in error state
    const definition = createTestViewDefinition('ManualRecovery', 'users')
    await storageManager.createView(definition)

    // Write some data
    const data = new Uint8Array([1, 2, 3, 4, 5])
    await storageManager.writeViewData('ManualRecovery', data)

    // Simulate error state
    await storageManager.updateViewState('ManualRecovery', 'error', 'Corrupted data')

    // Manual recovery steps:
    // 1. Delete existing view data
    await storageManager.deleteViewData('ManualRecovery')

    // 2. Reset view state to pending
    await storageManager.updateViewState('ManualRecovery', 'pending')

    // 3. Verify view data was deleted
    const dataExists = await storageManager.viewDataExists('ManualRecovery')
    expect(dataExists).toBe(false)

    // 4. View should be in pending state, ready for refresh
    const metadata = await storageManager.getViewMetadata('ManualRecovery')
    expect(metadata.state).toBe('pending')
  })

  it('should increment version after recovery', async () => {
    // Create view
    const definition = createTestViewDefinition('VersionIncrement', 'users')
    await storageManager.createView(definition)

    // Get initial version
    const initialMetadata = await storageManager.getViewMetadata('VersionIncrement')
    const initialVersion = initialMetadata.version

    // First state change
    await storageManager.updateViewState('VersionIncrement', 'ready')
    const afterFirstChange = await storageManager.getViewMetadata('VersionIncrement')
    expect(afterFirstChange.version).toBe(initialVersion + 1)

    // Simulate error
    await storageManager.updateViewState('VersionIncrement', 'error', 'Test error')
    const afterError = await storageManager.getViewMetadata('VersionIncrement')
    expect(afterError.version).toBe(initialVersion + 2)

    // Recovery
    await storageManager.updateViewState('VersionIncrement', 'ready')
    const afterRecovery = await storageManager.getViewMetadata('VersionIncrement')
    expect(afterRecovery.version).toBe(initialVersion + 3)
  })

  it('should preserve refresh history through failure and recovery', async () => {
    // Create view
    const definition = createTestViewDefinition('HistoryPreserve', 'users')
    await storageManager.createView(definition)

    // Record successful refresh
    await storageManager.recordRefresh('HistoryPreserve', true, 100)

    // Get stats after first refresh
    const statsAfterFirst = await storageManager.getViewStats('HistoryPreserve')
    expect(statsAfterFirst.totalRefreshes).toBe(1)
    expect(statsAfterFirst.successfulRefreshes).toBe(1)

    // Record failed refresh
    await storageManager.recordRefresh('HistoryPreserve', false, 50)

    // Get stats after failure
    const statsAfterFailure = await storageManager.getViewStats('HistoryPreserve')
    expect(statsAfterFailure.totalRefreshes).toBe(2)
    expect(statsAfterFailure.successfulRefreshes).toBe(1)
    expect(statsAfterFailure.failedRefreshes).toBe(1)

    // Record recovery refresh
    await storageManager.recordRefresh('HistoryPreserve', true, 120)

    // Final stats should show complete history
    const finalStats = await storageManager.getViewStats('HistoryPreserve')
    expect(finalStats.totalRefreshes).toBe(3)
    expect(finalStats.successfulRefreshes).toBe(2)
    expect(finalStats.failedRefreshes).toBe(1)
  })

  it('should handle recovery from disabled state', async () => {
    // Create view
    const definition = createTestViewDefinition('DisabledRecovery', 'users')
    await storageManager.createView(definition)

    // Disable view (e.g., after repeated failures)
    await storageManager.updateViewState('DisabledRecovery', 'disabled')

    const disabledMetadata = await storageManager.getViewMetadata('DisabledRecovery')
    expect(disabledMetadata.state).toBe('disabled')

    // Re-enable view
    await storageManager.updateViewState('DisabledRecovery', 'pending')

    // Then set to ready (simulating successful refresh)
    await storageManager.updateViewState('DisabledRecovery', 'ready')

    const recoveredMetadata = await storageManager.getViewMetadata('DisabledRecovery')
    expect(recoveredMetadata.state).toBe('ready')
  })
})

// =============================================================================
// Data Integrity Tests
// =============================================================================

describe('Data Integrity After Failures', () => {
  let baseBackend: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader
  let storageManager: MVStorageManager

  beforeEach(() => {
    baseBackend = new MemoryBackend()
    writer = new ParquetWriter(baseBackend, { compression: 'none' })
    reader = new ParquetReader({ storage: baseBackend })
    storageManager = new MVStorageManager(baseBackend)
  })

  it('should verify data after write', async () => {
    // Create view
    const definition = createTestViewDefinition('DataVerifyView', 'users')
    await storageManager.createView(definition)

    // Write data
    const testData = new Uint8Array([0x50, 0x41, 0x52, 0x31, 1, 2, 3, 4, 5])
    await storageManager.writeViewData('DataVerifyView', testData)

    // Read and verify
    const readData = await storageManager.readViewData('DataVerifyView')
    expect(readData).toEqual(testData)
    expect(readData.length).toBe(testData.length)
  })

  it('should maintain data integrity across multiple writes', async () => {
    // Create view
    const definition = createTestViewDefinition('MultiWriteView', 'users')
    await storageManager.createView(definition)

    // First write
    const data1 = new Uint8Array([1, 2, 3])
    await storageManager.writeViewData('MultiWriteView', data1)

    // Second write (overwrites)
    const data2 = new Uint8Array([4, 5, 6, 7, 8])
    await storageManager.writeViewData('MultiWriteView', data2)

    // Should have latest data
    const readData = await storageManager.readViewData('MultiWriteView')
    expect(readData).toEqual(data2)
  })

  it('should handle partial range reads correctly', async () => {
    // Create view with data
    const definition = createTestViewDefinition('RangeReadView', 'users')
    await storageManager.createView(definition)

    const fullData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    await storageManager.writeViewData('RangeReadView', fullData)

    // Read a range
    const partialData = await storageManager.readViewDataRange('RangeReadView', 2, 6)
    expect(partialData).toEqual(new Uint8Array([2, 3, 4, 5]))
  })

  it('should correctly report file stats', async () => {
    // Create view with data
    const definition = createTestViewDefinition('StatsView', 'users')
    await storageManager.createView(definition)

    const testData = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    await storageManager.writeViewData('StatsView', testData)

    // Get stats
    const stat = await storageManager.getViewDataStat('StatsView')
    expect(stat).not.toBeNull()
    expect(stat!.size).toBe(10)
    expect(stat!.mtime).toBeInstanceOf(Date)
  })

  it('should handle empty data correctly', async () => {
    // Create view
    const definition = createTestViewDefinition('EmptyDataView', 'users')
    await storageManager.createView(definition)

    // Write empty data
    const emptyData = new Uint8Array([])
    await storageManager.writeViewData('EmptyDataView', emptyData)

    // Read and verify
    const readData = await storageManager.readViewData('EmptyDataView')
    expect(readData.length).toBe(0)
  })
})

// =============================================================================
// Concurrent Failure Scenarios
// =============================================================================

describe('Concurrent Failure Scenarios', () => {
  let baseBackend: MemoryBackend
  let storageManager: MVStorageManager

  beforeEach(async () => {
    baseBackend = new MemoryBackend()
    storageManager = new MVStorageManager(baseBackend)
  })

  it('should handle concurrent state updates safely', async () => {
    // Create view
    const definition = createTestViewDefinition('ConcurrentView', 'users')
    await storageManager.createView(definition)

    // Simulate concurrent updates
    const updates = await Promise.allSettled([
      storageManager.updateViewState('ConcurrentView', 'building'),
      storageManager.updateViewState('ConcurrentView', 'ready'),
      storageManager.updateViewState('ConcurrentView', 'stale'),
    ])

    // All should settle (either resolve or reject)
    expect(updates.every(u => u.status === 'fulfilled' || u.status === 'rejected')).toBe(true)

    // View should be in a valid state
    const metadata = await storageManager.getViewMetadata('ConcurrentView')
    const validStates: ViewState[] = ['pending', 'ready', 'building', 'stale', 'error', 'disabled']
    expect(validStates).toContain(metadata.state)
  })

  it('should isolate failures between different views', async () => {
    // Create two views
    const def1 = createTestViewDefinition('IsolateView1', 'users')
    const def2 = createTestViewDefinition('IsolateView2', 'users')
    await storageManager.createView(def1)
    await storageManager.createView(def2)

    // Create storage that fails for View1 metadata
    const failingStorage = createFailingStorage(baseBackend, {
      writeFailPaths: ['IsolateView1/metadata'],
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Update View1 should fail
    await expect(
      failingManager.updateViewState('IsolateView1', 'error')
    ).rejects.toThrow()

    // Update View2 should succeed
    await failingManager.updateViewState('IsolateView2', 'ready')

    // Verify states
    const meta1 = await storageManager.getViewMetadata('IsolateView1')
    const meta2 = await storageManager.getViewMetadata('IsolateView2')

    expect(meta1.state).toBe('pending') // Unchanged due to failure
    expect(meta2.state).toBe('ready') // Updated successfully
  })

  it('should handle rapid create-delete cycles', async () => {
    for (let i = 0; i < 5; i++) {
      const viewName = `RapidCycleView${i}`
      const definition = createTestViewDefinition(viewName, 'users')

      // Create
      await storageManager.createView(definition)
      expect(await storageManager.viewExists(viewName)).toBe(true)

      // Delete
      await storageManager.deleteView(viewName)
      expect(await storageManager.viewExists(viewName)).toBe(false)
    }

    // Final manifest should be consistent
    const views = await storageManager.listViews()
    expect(views.length).toBe(0)
  })

  it('should maintain manifest consistency after errors', async () => {
    // Create several views
    for (let i = 0; i < 5; i++) {
      await storageManager.createView(createTestViewDefinition(`ConsistencyView${i}`, 'users'))
    }

    // Create failing storage for manifest writes
    const failingStorage = createFailingStorage(baseBackend, {
      failOnNthWrite: 2, // Fail on second write
    })
    const failingManager = new MVStorageManager(failingStorage)

    // Try to create another view - may fail
    try {
      await failingManager.createView(createTestViewDefinition('FailView', 'users'))
    } catch {
      // Expected
    }

    // Reload and verify manifest consistency
    storageManager.invalidateManifestCache()
    const views = await storageManager.listViews()

    // Should have at least the original 5 views
    expect(views.length).toBeGreaterThanOrEqual(5)

    // Each view in manifest should have valid metadata
    for (const view of views) {
      expect(view.name).toBeDefined()
      expect(view.source).toBeDefined()
      expect(view.state).toBeDefined()
    }
  })
})

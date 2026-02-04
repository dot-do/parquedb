/**
 * Tests for StreamPersistence
 *
 * Tests cover:
 * - WAL operations (log, commit, fail, rotation)
 * - DLQ persistence (persist, update, clear)
 * - Checkpointing (save, load)
 * - Recovery protocol
 * - Cleanup operations
 */

import { describe, test, expect, beforeEach } from 'vitest'
import {
  StreamPersistence,
  createStreamPersistence,
  type StreamPersistenceConfig,
  type WALEntry,
  type StreamCheckpoint,
  type RecoveryResult,
  type PersistedDLQEntry,
} from '../../../src/materialized-views/stream-processor'
import type { FailedBatch } from '../../../src/materialized-views/stream-processor'
import type { StorageBackend, WriteResult, FileStat } from '../../../src/types/storage'

// =============================================================================
// Test Helpers
// =============================================================================

interface TestRecord {
  id: string
  value: number
}

/** Create a test record */
function createRecord(id: number): TestRecord {
  return { id: `record-${id}`, value: id * 10 }
}

/** Create a failed batch */
function createFailedBatch(batchNumber: number, records: TestRecord[]): FailedBatch<TestRecord> {
  return {
    records,
    batchNumber,
    filePath: `_test/output/test.${batchNumber}.parquet`,
    error: new Error(`Test failure for batch ${batchNumber}`),
    failedAt: Date.now(),
    attempts: 3,
  }
}

/** Create an in-memory storage backend for testing */
function createMockStorage(): StorageBackend & {
  files: Map<string, Uint8Array>
  getFileContent: (path: string) => string | null
} {
  const files = new Map<string, Uint8Array>()

  const storage: StorageBackend & {
    files: Map<string, Uint8Array>
    getFileContent: (path: string) => string | null
  } = {
    type: 'mock',
    files,

    getFileContent(path: string): string | null {
      const data = files.get(path)
      return data ? new TextDecoder().decode(data) : null
    },

    async read(path: string): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data.slice(start, end)
    },

    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },

    async stat(path: string): Promise<FileStat | null> {
      const data = files.get(path)
      if (!data) return null
      return {
        path,
        size: data.length,
        mtime: new Date(),
        isDirectory: false,
      }
    },

    async list(prefix: string) {
      const matchingFiles = Array.from(files.keys()).filter((key) => key.startsWith(prefix))
      return { files: matchingFiles, hasMore: false }
    },

    async write(path: string, data: Uint8Array): Promise<WriteResult> {
      files.set(path, data)
      return { etag: 'mock-etag', size: data.length }
    },

    async writeAtomic(path: string, data: Uint8Array): Promise<WriteResult> {
      return this.write(path, data)
    },

    async append(path: string, data: Uint8Array): Promise<void> {
      const existing = files.get(path) || new Uint8Array(0)
      const combined = new Uint8Array(existing.length + data.length)
      combined.set(existing)
      combined.set(data, existing.length)
      files.set(path, combined)
    },

    async delete(path: string): Promise<boolean> {
      return files.delete(path)
    },

    async deletePrefix(prefix: string): Promise<number> {
      let count = 0
      for (const key of files.keys()) {
        if (key.startsWith(prefix)) {
          files.delete(key)
          count++
        }
      }
      return count
    },

    async mkdir(): Promise<void> {},
    async rmdir(): Promise<void> {},

    async writeConditional(
      path: string,
      data: Uint8Array,
      _expectedVersion: string | null
    ): Promise<WriteResult> {
      return this.write(path, data)
    },

    async copy(source: string, dest: string): Promise<void> {
      const data = files.get(source)
      if (!data) throw new Error(`Source not found: ${source}`)
      files.set(dest, data)
    },

    async move(source: string, dest: string): Promise<void> {
      const data = files.get(source)
      if (!data) throw new Error(`Source not found: ${source}`)
      files.set(dest, data)
      files.delete(source)
    },
  }

  return storage
}

// =============================================================================
// Initialization & Recovery Tests
// =============================================================================

describe('StreamPersistence', () => {
  let storage: ReturnType<typeof createMockStorage>
  let persistence: StreamPersistence<TestRecord>

  beforeEach(() => {
    storage = createMockStorage()
  })

  describe('Initialization', () => {
    test('creates persistence instance', () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      expect(persistence).toBeInstanceOf(StreamPersistence)
    })

    test('recover returns clean result when no prior state', async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      const result = await persistence.recover()

      expect(result.cleanRecovery).toBe(true)
      expect(result.pendingRecords).toHaveLength(0)
      expect(result.failedBatches).toHaveLength(0)
      expect(result.checkpoint).toBeNull()
      expect(result.walEntriesRecovered).toBe(0)
      expect(result.dlqEntriesRecovered).toBe(0)
    })
  })

  describe('WAL Operations', () => {
    beforeEach(async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })
      await persistence.recover()
    })

    test('logs records to WAL', async () => {
      const records = [createRecord(1), createRecord(2)]

      const entry = await persistence.logWAL(records, 1, '_test/output/batch-1.parquet')

      expect(entry.id).toBeDefined()
      expect(entry.batchNumber).toBe(1)
      expect(entry.records).toEqual(records)
      expect(entry.status).toBe('pending')
      expect(entry.targetPath).toBe('_test/output/batch-1.parquet')

      // Verify WAL file was created
      const walContent = storage.getFileContent('_test/output/wal/pending.jsonl')
      expect(walContent).toBeTruthy()
      expect(walContent).toContain('"status":"pending"')
    })

    test('commits WAL entry', async () => {
      const records = [createRecord(1)]
      const entry = await persistence.logWAL(records, 1, '_test/output/batch-1.parquet')

      await persistence.commitWAL(entry.id)

      const walContent = storage.getFileContent('_test/output/wal/pending.jsonl')
      expect(walContent).toContain('"type":"commit"')
      expect(walContent).toContain(entry.id)
    })

    test('marks WAL entry as failed', async () => {
      const records = [createRecord(1)]
      const entry = await persistence.logWAL(records, 1, '_test/output/batch-1.parquet')

      await persistence.failWAL(entry.id, new Error('Test failure'))

      const walContent = storage.getFileContent('_test/output/wal/pending.jsonl')
      expect(walContent).toContain('"type":"fail"')
      expect(walContent).toContain('Test failure')
    })

    test('throws if not initialized', async () => {
      const uninitPersistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      await expect(
        uninitPersistence.logWAL([createRecord(1)], 1, 'path')
      ).rejects.toThrow('not initialized')
    })
  })

  describe('DLQ Persistence', () => {
    beforeEach(async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })
      await persistence.recover()
    })

    test('persists failed batch to DLQ', async () => {
      const failedBatch = createFailedBatch(1, [createRecord(1), createRecord(2)])

      await persistence.persistDLQ(failedBatch)

      // Verify DLQ file was created
      const files = await storage.list('_test/output/dlq/')
      expect(files.files.length).toBe(1)
      expect(files.files[0]).toContain('batch-1-')
    })

    test('lists DLQ entries', async () => {
      await persistence.persistDLQ(createFailedBatch(1, [createRecord(1)]))
      await persistence.persistDLQ(createFailedBatch(2, [createRecord(2)]))

      const entries = await persistence.listDLQ()

      expect(entries).toHaveLength(2)
      expect(entries.map((e) => e.batch.batchNumber).sort()).toEqual([1, 2])
    })

    test('updates DLQ entry on retry success', async () => {
      const failedBatch = createFailedBatch(1, [createRecord(1)])
      await persistence.persistDLQ(failedBatch)

      await persistence.updateDLQEntry(failedBatch, true)

      // Entry should be removed
      const entries = await persistence.listDLQ()
      expect(entries).toHaveLength(0)
    })

    test('updates DLQ entry on retry failure', async () => {
      const failedBatch = createFailedBatch(1, [createRecord(1)])
      await persistence.persistDLQ(failedBatch)

      await persistence.updateDLQEntry(failedBatch, false)

      const entries = await persistence.listDLQ()
      expect(entries).toHaveLength(1)
      expect(entries[0]!.replayAttempts).toBe(1)
    })

    test('clears all DLQ entries', async () => {
      await persistence.persistDLQ(createFailedBatch(1, [createRecord(1)]))
      await persistence.persistDLQ(createFailedBatch(2, [createRecord(2)]))

      const cleared = await persistence.clearDLQ()

      expect(cleared).toBe(2)
      const entries = await persistence.listDLQ()
      expect(entries).toHaveLength(0)
    })
  })

  describe('Checkpointing', () => {
    beforeEach(async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })
      await persistence.recover()
    })

    test('saves checkpoint', async () => {
      const checkpoint: StreamCheckpoint = {
        sequence: 'seq-123',
        timestamp: Date.now(),
        lastBatchNumber: 5,
        recordsProcessed: 100,
        metadata: { custom: 'value' },
      }

      await persistence.saveCheckpoint(checkpoint)

      // Verify checkpoint file was created
      const exists = await storage.exists('_test/output/checkpoint.json')
      expect(exists).toBe(true)
    })

    test('loads checkpoint', async () => {
      const checkpoint: StreamCheckpoint = {
        sequence: 'seq-456',
        timestamp: Date.now(),
        lastBatchNumber: 10,
        recordsProcessed: 200,
      }
      await persistence.saveCheckpoint(checkpoint)

      const loaded = await persistence.loadCheckpoint()

      expect(loaded).toBeTruthy()
      expect(loaded!.sequence).toBe('seq-456')
      expect(loaded!.lastBatchNumber).toBe(10)
      expect(loaded!.recordsProcessed).toBe(200)
    })

    test('returns null when no checkpoint exists', async () => {
      const loaded = await persistence.loadCheckpoint()
      expect(loaded).toBeNull()
    })

    test('clears checkpoint', async () => {
      await persistence.saveCheckpoint({
        timestamp: Date.now(),
        lastBatchNumber: 1,
        recordsProcessed: 10,
      })

      await persistence.clearCheckpoint()

      const loaded = await persistence.loadCheckpoint()
      expect(loaded).toBeNull()
    })
  })

  describe('Recovery Protocol', () => {
    test('recovers pending WAL entries', async () => {
      // Pre-create a WAL file with pending entries
      const walEntry: WALEntry<TestRecord> = {
        id: 'wal-1',
        batchNumber: 1,
        records: [createRecord(1), createRecord(2)],
        timestamp: Date.now(),
        targetPath: 'test.parquet',
        status: 'pending',
      }
      await storage.write(
        '_test/output/wal/pending.jsonl',
        new TextEncoder().encode(JSON.stringify(walEntry) + '\n')
      )

      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      const result = await persistence.recover()

      expect(result.cleanRecovery).toBe(false)
      expect(result.walEntriesRecovered).toBe(1)
      expect(result.pendingRecords).toHaveLength(2)
      expect(result.pendingRecords[0]).toEqual(createRecord(1))
    })

    test('recovers DLQ entries', async () => {
      // Pre-create a DLQ file
      const dlqEntry: PersistedDLQEntry<TestRecord> = {
        batch: createFailedBatch(1, [createRecord(1)]),
        persistedAt: Date.now(),
        replayAttempts: 0,
      }
      await storage.write(
        '_test/output/dlq/batch-1-12345.json',
        new TextEncoder().encode(JSON.stringify(dlqEntry))
      )

      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      const result = await persistence.recover()

      expect(result.cleanRecovery).toBe(false)
      expect(result.dlqEntriesRecovered).toBe(1)
      expect(result.failedBatches).toHaveLength(1)
      expect(result.failedBatches[0]!.batchNumber).toBe(1)
    })

    test('recovers checkpoint', async () => {
      // Pre-create a checkpoint file
      const checkpoint: StreamCheckpoint = {
        sequence: 'seq-100',
        timestamp: Date.now(),
        lastBatchNumber: 50,
        recordsProcessed: 1000,
      }
      await storage.write(
        '_test/output/checkpoint.json',
        new TextEncoder().encode(JSON.stringify(checkpoint))
      )

      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })

      const result = await persistence.recover()

      // Checkpoint alone doesn't make it non-clean
      expect(result.cleanRecovery).toBe(true)
      expect(result.checkpoint).toBeTruthy()
      expect(result.checkpoint!.sequence).toBe('seq-100')
      expect(result.checkpoint!.lastBatchNumber).toBe(50)
    })
  })

  describe('Statistics', () => {
    test('returns persistence stats', async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })
      await persistence.recover()

      // Add some data
      await persistence.logWAL([createRecord(1)], 1, 'test.parquet')
      await persistence.persistDLQ(createFailedBatch(1, [createRecord(1)]))
      await persistence.saveCheckpoint({ timestamp: Date.now(), lastBatchNumber: 1, recordsProcessed: 1 })

      const stats = await persistence.getStats()

      expect(stats.walFiles).toBeGreaterThanOrEqual(1)
      expect(stats.dlqFiles).toBe(1)
      expect(stats.hasCheckpoint).toBe(true)
      expect(stats.currentWalSize).toBeGreaterThan(0)
    })
  })

  describe('Cleanup', () => {
    test('cleans up old WAL archives', async () => {
      persistence = createStreamPersistence({
        name: 'test',
        storage,
        basePath: '_test/output',
      })
      await persistence.recover()

      // Create some old archive files
      const oldTimestamp = Date.now() - 48 * 60 * 60 * 1000 // 48 hours ago
      await storage.write(
        `_test/output/wal/archive-${oldTimestamp}.jsonl`,
        new TextEncoder().encode('old data')
      )

      // Create a recent archive
      const recentTimestamp = Date.now() - 1000 // 1 second ago
      await storage.write(
        `_test/output/wal/archive-${recentTimestamp}.jsonl`,
        new TextEncoder().encode('recent data')
      )

      const cleaned = await persistence.cleanupWALArchives(24 * 60 * 60 * 1000) // 24 hours

      expect(cleaned).toBe(1) // Only old file should be cleaned
    })
  })
})

// =============================================================================
// Integration with StreamProcessor Tests
// =============================================================================

describe('StreamPersistence Integration', () => {
  test('full recovery workflow', async () => {
    const storage = createMockStorage()

    // Simulate a crash: create WAL and DLQ entries
    const walEntry: WALEntry<TestRecord> = {
      id: 'crash-wal-1',
      batchNumber: 5,
      records: [createRecord(10), createRecord(11), createRecord(12)],
      timestamp: Date.now() - 5000,
      targetPath: '_test/output/batch-5.parquet',
      status: 'pending',
    }
    await storage.write(
      '_test/output/wal/pending.jsonl',
      new TextEncoder().encode(JSON.stringify(walEntry) + '\n')
    )

    const dlqEntry: PersistedDLQEntry<TestRecord> = {
      batch: createFailedBatch(3, [createRecord(5), createRecord(6)]),
      persistedAt: Date.now() - 10000,
      replayAttempts: 2,
    }
    await storage.write(
      '_test/output/dlq/batch-3-crash.json',
      new TextEncoder().encode(JSON.stringify(dlqEntry))
    )

    const checkpoint: StreamCheckpoint = {
      sequence: 4,
      timestamp: Date.now() - 15000,
      lastBatchNumber: 4,
      recordsProcessed: 40,
    }
    await storage.write(
      '_test/output/checkpoint.json',
      new TextEncoder().encode(JSON.stringify(checkpoint))
    )

    // Create new persistence and recover
    const persistence = createStreamPersistence<TestRecord>({
      name: 'recovery-test',
      storage,
      basePath: '_test/output',
    })

    const recovery = await persistence.recover()

    // Verify recovery results
    expect(recovery.cleanRecovery).toBe(false)
    expect(recovery.walEntriesRecovered).toBe(1)
    expect(recovery.dlqEntriesRecovered).toBe(1)
    expect(recovery.pendingRecords).toHaveLength(3)
    expect(recovery.failedBatches).toHaveLength(1)
    expect(recovery.checkpoint!.sequence).toBe(4)
    expect(recovery.checkpoint!.lastBatchNumber).toBe(4)
  })
})

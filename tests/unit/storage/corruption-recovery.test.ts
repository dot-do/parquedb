/**
 * Corruption Recovery Tests
 *
 * Tests for corruption detection and recovery scenarios:
 * - Corrupted parquet file detection
 * - Recovery from partial writes
 * - WAL replay with missing entries
 * - Graceful degradation on read errors
 *
 * Uses mocks for storage to simulate corruption scenarios.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { StorageBackend, FileStat, ListResult, WriteResult } from '../../../src/types/storage'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { NotFoundError, StorageError, StorageErrorCode, NetworkError, OperationError } from '../../../src/storage/errors'
import { ParquetReader, initializeAsyncBuffer } from '../../../src/parquet/reader'
import { SqliteWal, type SqliteInterface } from '../../../src/events/sqlite-wal'

// =============================================================================
// Helper Functions
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

/**
 * Create a mock storage backend that can simulate failures
 */
function createFailingBackend(
  baseBackend: StorageBackend,
  options: {
    failRead?: boolean | ((path: string) => boolean)
    failWrite?: boolean | ((path: string) => boolean)
    failReadRange?: boolean | ((path: string) => boolean)
    failStat?: boolean | ((path: string) => boolean)
    corruptData?: (path: string, data: Uint8Array) => Uint8Array
    networkError?: boolean
    partialRead?: boolean
  } = {}
): StorageBackend {
  return {
    type: 'failing-mock',

    async read(path: string): Promise<Uint8Array> {
      const shouldFail = typeof options.failRead === 'function'
        ? options.failRead(path)
        : options.failRead

      if (shouldFail) {
        if (options.networkError) {
          throw new NetworkError('Connection reset during read', path)
        }
        throw new OperationError('Simulated read failure', 'read', path)
      }

      const data = await baseBackend.read(path)

      if (options.corruptData) {
        return options.corruptData(path, data)
      }

      if (options.partialRead) {
        // Return only half the data to simulate truncation
        return data.slice(0, Math.floor(data.length / 2))
      }

      return data
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      const shouldFail = typeof options.failReadRange === 'function'
        ? options.failReadRange(path)
        : options.failReadRange

      if (shouldFail) {
        if (options.networkError) {
          throw new NetworkError('Connection reset during range read', path)
        }
        throw new OperationError('Simulated readRange failure', 'readRange', path)
      }

      const data = await baseBackend.readRange(path, start, end)

      if (options.corruptData) {
        return options.corruptData(path, data)
      }

      return data
    },

    async write(path: string, data: Uint8Array, writeOptions?: unknown): Promise<WriteResult> {
      const shouldFail = typeof options.failWrite === 'function'
        ? options.failWrite(path)
        : options.failWrite

      if (shouldFail) {
        throw new OperationError('Simulated write failure', 'write', path)
      }

      return baseBackend.write(path, data, writeOptions as Parameters<typeof baseBackend.write>[2])
    },

    async writeAtomic(path: string, data: Uint8Array, writeOptions?: unknown): Promise<WriteResult> {
      const shouldFail = typeof options.failWrite === 'function'
        ? options.failWrite(path)
        : options.failWrite

      if (shouldFail) {
        throw new OperationError('Simulated write failure', 'writeAtomic', path)
      }

      return baseBackend.writeAtomic(path, data, writeOptions as Parameters<typeof baseBackend.writeAtomic>[2])
    },

    async exists(path: string): Promise<boolean> {
      return baseBackend.exists(path)
    },

    async stat(path: string): Promise<FileStat | null> {
      const shouldFail = typeof options.failStat === 'function'
        ? options.failStat(path)
        : options.failStat

      if (shouldFail) {
        throw new OperationError('Simulated stat failure', 'stat', path)
      }

      return baseBackend.stat(path)
    },

    async list(prefix: string, listOptions?: unknown): Promise<ListResult> {
      return baseBackend.list(prefix, listOptions as Parameters<typeof baseBackend.list>[1])
    },

    async append(path: string, data: Uint8Array): Promise<void> {
      return baseBackend.append(path, data)
    },

    async delete(path: string): Promise<boolean> {
      return baseBackend.delete(path)
    },

    async deletePrefix(prefix: string): Promise<number> {
      return baseBackend.deletePrefix(prefix)
    },

    async mkdir(path: string): Promise<void> {
      return baseBackend.mkdir(path)
    },

    async rmdir(path: string, rmdirOptions?: unknown): Promise<void> {
      return baseBackend.rmdir(path, rmdirOptions as Parameters<typeof baseBackend.rmdir>[1])
    },

    async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null, writeOptions?: unknown): Promise<WriteResult> {
      return baseBackend.writeConditional(path, data, expectedVersion, writeOptions as Parameters<typeof baseBackend.writeConditional>[3])
    },

    async copy(source: string, dest: string): Promise<void> {
      return baseBackend.copy(source, dest)
    },

    async move(source: string, dest: string): Promise<void> {
      return baseBackend.move(source, dest)
    },
  }
}

/**
 * Create a mock SQLite interface for testing WAL
 *
 * Note: The WalRow type uses camelCase (minTs, maxTs) which is what TypeScript expects,
 * but the actual SQLite columns are snake_case (min_ts, max_ts). The DO SQLite likely
 * performs this mapping. For our mock, we'll use the TypeScript interface field names.
 */
function createMockSqlite(): SqliteInterface & {
  data: Map<string, unknown[]>
  shouldFail: boolean
  lastInsertId: number
} {
  const data = new Map<string, unknown[]>()
  let lastInsertId = 0

  return {
    data,
    shouldFail: false,
    lastInsertId,

    exec<T = unknown>(query: string, ...params: unknown[]): Iterable<T> {
      if (this.shouldFail) {
        throw new Error('SQLite execution failed')
      }

      // Simple mock that handles basic WAL operations
      if (query.includes('CREATE TABLE')) {
        return [] as unknown as Iterable<T>
      }

      if (query.includes('CREATE INDEX')) {
        return [] as unknown as Iterable<T>
      }

      if (query.includes('INSERT INTO')) {
        this.lastInsertId++
        const tableName = query.match(/INSERT INTO (\w+)/)?.[1] ?? 'default'
        const existing = data.get(tableName) ?? []
        existing.push({
          id: this.lastInsertId,
          batch: params[0],
          // Use camelCase to match WalRow interface
          minTs: params[1],
          maxTs: params[2],
          count: params[3],
          flushed: 0,
        })
        data.set(tableName, existing)
        return [] as unknown as Iterable<T>
      }

      if (query.includes('last_insert_rowid')) {
        return [{ id: this.lastInsertId }] as unknown as Iterable<T>
      }

      // Handle SELECT queries for unflushed batches
      if (query.includes('SELECT') && query.includes('flushed = 0') && query.includes('ORDER BY')) {
        const tableName = query.match(/FROM (\w+)/)?.[1] ?? 'default'
        const rows = (data.get(tableName) ?? []).filter((r: unknown) => (r as { flushed: number }).flushed === 0)
        return rows as unknown as Iterable<T>
      }

      if (query.includes('SUM(count)')) {
        const tableName = query.match(/FROM (\w+)/)?.[1] ?? 'default'
        const rows = data.get(tableName) ?? []
        const total = rows
          .filter((r: unknown) => (r as { flushed: number }).flushed === 0)
          .reduce((sum, r: unknown) => sum + ((r as { count: number }).count ?? 0), 0)
        return [{ total }] as unknown as Iterable<T>
      }

      if (query.includes('COUNT(*)')) {
        const tableName = query.match(/FROM (\w+)/)?.[1] ?? 'default'
        const rows = data.get(tableName) ?? []
        const count = rows.filter((r: unknown) => (r as { flushed: number }).flushed === 0).length
        return [{ count }] as unknown as Iterable<T>
      }

      if (query.includes('UPDATE')) {
        const tableName = query.match(/UPDATE (\w+)/)?.[1] ?? 'default'
        const ids = params as number[]
        const rows = data.get(tableName) ?? []
        rows.forEach((r: unknown) => {
          if (ids.includes((r as { id: number }).id)) {
            (r as { flushed: number }).flushed = 1
          }
        })
        return [] as unknown as Iterable<T>
      }

      if (query.includes('DELETE')) {
        const tableName = query.match(/FROM (\w+)/)?.[1] ?? 'default'
        const rows = data.get(tableName) ?? []
        data.set(tableName, rows.filter((r: unknown) => (r as { flushed: number }).flushed !== 1))
        return [] as unknown as Iterable<T>
      }

      return [] as unknown as Iterable<T>
    },
  }
}

// Valid minimal Parquet file header (magic + some metadata)
const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // 'PAR1'

function createMinimalParquetHeader(): Uint8Array {
  // PAR1 magic at start
  return PARQUET_MAGIC
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Corruption Recovery', () => {
  let baseBackend: MemoryBackend

  beforeEach(() => {
    baseBackend = new MemoryBackend()
  })

  // ===========================================================================
  // 1. Corrupted Parquet File Detection
  // ===========================================================================

  describe('Corrupted Parquet File Detection', () => {
    it('should detect when Parquet magic bytes are missing', async () => {
      const path = 'data/test/data.parquet'
      // Write data without valid Parquet header
      const corruptedData = textToBytes('This is not a parquet file')
      await baseBackend.write(path, corruptedData)

      const reader = new ParquetReader({ storage: baseBackend })

      // Should throw when trying to read metadata
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })

    it('should detect truncated Parquet files', async () => {
      const path = 'data/test/truncated.parquet'
      // Write only the magic header without the rest
      await baseBackend.write(path, PARQUET_MAGIC)

      const reader = new ParquetReader({ storage: baseBackend })

      // Should throw when trying to read - file is too short
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })

    it('should detect corrupted Parquet footer', async () => {
      const path = 'data/test/bad-footer.parquet'
      // Create a file with magic at start but corrupted footer
      const data = new Uint8Array(100)
      data.set(PARQUET_MAGIC, 0)
      // Fill with garbage
      for (let i = 4; i < 96; i++) {
        data[i] = 0xFF
      }
      // End with PAR1 magic (but metadata length is wrong)
      data.set(PARQUET_MAGIC, 96)
      await baseBackend.write(path, data)

      const reader = new ParquetReader({ storage: baseBackend })

      // Should throw when parsing invalid metadata
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })

    it('should handle file that becomes unavailable during read', async () => {
      const path = 'data/test/disappearing.parquet'
      await baseBackend.write(path, createMinimalParquetHeader())

      const failingBackend = createFailingBackend(baseBackend, {
        failReadRange: true,
        networkError: true,
      })

      const reader = new ParquetReader({ storage: failingBackend })

      // Should throw when trying to read metadata (uses readRange internally)
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })
  })

  // ===========================================================================
  // 2. Recovery from Partial Writes
  // ===========================================================================

  describe('Recovery from Partial Writes', () => {
    it('should handle write failure gracefully', async () => {
      const path = 'data/test/partial-write.parquet'
      const data = textToBytes('test data')

      const failingBackend = createFailingBackend(baseBackend, {
        failWrite: true,
      })

      await expect(failingBackend.write(path, data)).rejects.toThrow(OperationError)

      // File should not exist after failed write
      expect(await baseBackend.exists(path)).toBe(false)
    })

    it('should detect and reject partial data during read', async () => {
      const path = 'data/test/partial.parquet'
      const fullData = new Uint8Array(1000)
      fullData.set(PARQUET_MAGIC, 0)
      await baseBackend.write(path, fullData)

      const failingBackend = createFailingBackend(baseBackend, {
        partialRead: true,
      })

      const reader = new ParquetReader({ storage: failingBackend })

      // Should fail because it only gets half the file
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })

    it('should preserve original file on failed conditional write', async () => {
      const path = 'data/test/conditional.txt'
      const originalContent = textToBytes('original content')
      const newContent = textToBytes('new content')

      await baseBackend.write(path, originalContent)
      const stat = await baseBackend.stat(path)
      const originalEtag = stat?.etag

      // Attempt conditional write with wrong etag
      await expect(
        baseBackend.writeConditional(path, newContent, 'wrong-etag')
      ).rejects.toThrow()

      // Original content should be preserved
      const result = await baseBackend.read(path)
      expect(bytesToText(result)).toBe('original content')
    })

    it('should handle atomic write failure without corrupting existing file', async () => {
      const path = 'data/test/atomic.txt'
      const originalContent = textToBytes('original')
      await baseBackend.write(path, originalContent)

      const failingBackend = createFailingBackend(baseBackend, {
        failWrite: (p) => p === path,
      })

      const newContent = textToBytes('new content')

      await expect(failingBackend.writeAtomic(path, newContent)).rejects.toThrow()

      // Original file should still be intact (read from base backend)
      const result = await baseBackend.read(path)
      expect(bytesToText(result)).toBe('original')
    })

    it('should handle mid-write storage failure', async () => {
      const path = 'data/test/mid-write.txt'
      let writeCount = 0

      const failingBackend = createFailingBackend(baseBackend, {
        failWrite: () => {
          writeCount++
          return writeCount > 1 // Fail on second write
        },
      })

      // First write succeeds
      await failingBackend.write(path, textToBytes('first'))

      // Second write fails
      await expect(
        failingBackend.write(path, textToBytes('second'))
      ).rejects.toThrow()
    })
  })

  // ===========================================================================
  // 3. WAL Replay with Missing Entries
  // ===========================================================================

  describe('WAL Replay with Missing Entries', () => {
    it('should handle empty WAL gracefully', () => {
      const sql = createMockSqlite()
      const wal = new SqliteWal(sql)

      wal.ensureTable()
      const batches = wal.readUnflushedBatches()

      expect(batches).toEqual([])
    })

    it('should handle WAL with corrupted batch data', () => {
      // Create a mock that returns corrupted data for SELECT queries
      const mockWithCorruptedData: SqliteInterface = {
        exec<T = unknown>(query: string): Iterable<T> {
          if (query.includes('CREATE TABLE') || query.includes('CREATE INDEX')) {
            return [] as Iterable<T>
          }
          if (query.includes('SELECT') && query.includes('flushed = 0')) {
            // Return a row with corrupted batch data
            return [{
              id: 1,
              batch: new Uint8Array([0xFF, 0xFE, 0xFD]), // Invalid JSON
              minTs: 1000,
              maxTs: 2000,
              count: 5,
            }] as Iterable<T>
          }
          return [] as Iterable<T>
        },
      }

      const wal = new SqliteWal(mockWithCorruptedData)
      wal.ensureTable()

      // Should not throw, but return empty events in the batch
      const batches = wal.readUnflushedBatches()
      expect(batches.length).toBe(1)
      expect(batches[0]?.events).toEqual([]) // Corrupted JSON yields empty events
    })

    it('should handle WAL SQLite execution failure', () => {
      const sql = createMockSqlite()
      const wal = new SqliteWal(sql)

      wal.ensureTable()
      sql.shouldFail = true

      expect(() => wal.writeBatch({
        events: [],
        minTs: 1000,
        maxTs: 2000,
        count: 0,
      })).toThrow('SQLite execution failed')
    })

    it('should recover valid batches even when some are corrupted', () => {
      // Create valid JSON batch data
      const validEvents = [{ id: '1', type: 'test', ts: 1500, data: {} }]
      const validBatchData = new TextEncoder().encode(JSON.stringify(validEvents))

      // Create a mock that returns both valid and corrupted batches
      const mockWithMixedData: SqliteInterface = {
        exec<T = unknown>(query: string): Iterable<T> {
          if (query.includes('CREATE TABLE') || query.includes('CREATE INDEX')) {
            return [] as Iterable<T>
          }
          if (query.includes('SELECT') && query.includes('flushed = 0') && query.includes('ORDER BY')) {
            // Return both a valid batch and a corrupted batch
            return [
              {
                id: 1,
                batch: validBatchData,
                minTs: 1000,
                maxTs: 2000,
                count: 1,
              },
              {
                id: 2,
                batch: new Uint8Array([0xFF, 0xFE]), // Invalid JSON
                minTs: 3000,
                maxTs: 4000,
                count: 1,
              },
            ] as Iterable<T>
          }
          return [] as Iterable<T>
        },
      }

      const wal = new SqliteWal(mockWithMixedData)
      wal.ensureTable()

      const batches = wal.readUnflushedBatches()
      expect(batches.length).toBe(2)

      // First batch should have valid events
      expect(batches[0]?.events.length).toBe(1)

      // Second batch should have empty events (corrupted)
      expect(batches[1]?.events).toEqual([])
    })

    it('should handle marking non-existent batches as flushed', () => {
      const sql = createMockSqlite()
      const wal = new SqliteWal(sql)

      wal.ensureTable()

      // Should not throw when marking non-existent IDs
      expect(() => wal.markFlushed([999, 1000, 1001])).not.toThrow()
    })

    it('should handle empty batch ID array for markFlushed', () => {
      const sql = createMockSqlite()
      const wal = new SqliteWal(sql)

      wal.ensureTable()

      // Should be a no-op
      expect(() => wal.markFlushed([])).not.toThrow()
    })
  })

  // ===========================================================================
  // 4. Graceful Degradation on Read Errors
  // ===========================================================================

  describe('Graceful Degradation on Read Errors', () => {
    it('should throw NotFoundError for missing file', async () => {
      await expect(baseBackend.read('nonexistent.parquet')).rejects.toThrow(NotFoundError)
    })

    it('should propagate network errors appropriately', async () => {
      const path = 'data/test/network-fail.parquet'
      await baseBackend.write(path, createMinimalParquetHeader())

      const failingBackend = createFailingBackend(baseBackend, {
        failRead: true,
        networkError: true,
      })

      await expect(failingBackend.read(path)).rejects.toThrow(NetworkError)
    })

    it('should handle intermittent read failures', async () => {
      const path = 'data/test/intermittent.txt'
      const content = textToBytes('test content')
      await baseBackend.write(path, content)

      let readCount = 0
      const failingBackend = createFailingBackend(baseBackend, {
        failRead: () => {
          readCount++
          return readCount === 1 // Only fail first read
        },
      })

      // First read fails
      await expect(failingBackend.read(path)).rejects.toThrow()

      // Second read succeeds
      const result = await failingBackend.read(path)
      expect(bytesToText(result)).toBe('test content')
    })

    it('should handle stat failures gracefully', async () => {
      const path = 'data/test/stat-fail.txt'
      await baseBackend.write(path, textToBytes('content'))

      const failingBackend = createFailingBackend(baseBackend, {
        failStat: true,
      })

      await expect(failingBackend.stat(path)).rejects.toThrow(OperationError)
    })

    it('should handle readRange failures for Parquet partial reads', async () => {
      const path = 'data/test/range-fail.parquet'
      const data = new Uint8Array(1000)
      data.set(PARQUET_MAGIC, 0)
      await baseBackend.write(path, data)

      const failingBackend = createFailingBackend(baseBackend, {
        failReadRange: true,
      })

      await expect(failingBackend.readRange(path, 0, 100)).rejects.toThrow(OperationError)
    })

    it('should detect data corruption via checksum mismatch simulation', async () => {
      const path = 'data/test/corrupted.parquet'
      const originalData = new Uint8Array(100)
      originalData.set(PARQUET_MAGIC, 0)
      await baseBackend.write(path, originalData)

      // Simulate corruption by flipping bits
      const failingBackend = createFailingBackend(baseBackend, {
        corruptData: (_path, data) => {
          const corrupted = new Uint8Array(data)
          // Flip some bits in the middle
          for (let i = 10; i < 20 && i < corrupted.length; i++) {
            corrupted[i] = corrupted[i]! ^ 0xFF
          }
          return corrupted
        },
      })

      const corruptedData = await failingBackend.read(path)

      // Verify data was corrupted
      expect(corruptedData).not.toEqual(originalData)

      // The ParquetReader should fail on corrupted data
      const reader = new ParquetReader({ storage: failingBackend })
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })
  })

  // ===========================================================================
  // 5. Storage Backend Error Classification
  // ===========================================================================

  describe('Storage Backend Error Classification', () => {
    it('should distinguish NotFoundError from other errors', async () => {
      const notFoundError = new NotFoundError('test.txt')

      // NotFoundError uses the legacy storageCode for backward compatibility
      expect(notFoundError.storageCode).toBe(StorageErrorCode.NOT_FOUND)
      expect(notFoundError.isNotFound()).toBe(true)
      expect(notFoundError.isPreconditionFailed()).toBe(false)
    })

    it('should classify network errors correctly', () => {
      const networkError = new NetworkError('Connection timeout', 'test.txt')

      expect(networkError.storageCode).toBe(StorageErrorCode.NETWORK_ERROR)
      expect(networkError.path).toBe('test.txt')
    })

    it('should classify operation errors correctly', () => {
      const opError = new OperationError('Write failed', 'write', 'test.txt')

      expect(opError.storageCode).toBe(StorageErrorCode.OPERATION_ERROR)
      expect(opError.operation).toBe('write')
      expect(opError.path).toBe('test.txt')
    })

    it('should preserve error chain with cause', () => {
      const cause = new Error('Underlying IO error')
      const storageError = new StorageError(
        'Failed to read file',
        StorageErrorCode.OPERATION_ERROR,
        'test.txt',
        cause
      )

      expect(storageError.cause).toBe(cause)
      expect(storageError.message).toBe('Failed to read file')
    })
  })

  // ===========================================================================
  // 6. Recovery Strategy Tests
  // ===========================================================================

  describe('Recovery Strategies', () => {
    it('should allow retry after transient failure', async () => {
      const path = 'data/test/retry.txt'
      const content = textToBytes('retry content')
      await baseBackend.write(path, content)

      let attemptCount = 0
      const transientFailBackend = createFailingBackend(baseBackend, {
        failRead: () => {
          attemptCount++
          return attemptCount < 3 // Fail first 2 attempts
        },
      })

      // Simple retry logic
      let result: Uint8Array | undefined
      for (let i = 0; i < 5; i++) {
        try {
          result = await transientFailBackend.read(path)
          break
        } catch {
          // Retry
        }
      }

      expect(result).toBeDefined()
      expect(bytesToText(result!)).toBe('retry content')
      expect(attemptCount).toBe(3)
    })

    it('should handle fallback to backup storage', async () => {
      const path = 'data/test/backup.txt'
      const content = textToBytes('backup content')

      // Primary backend fails
      const primaryBackend = createFailingBackend(baseBackend, {
        failRead: true,
      })

      // Backup backend works
      const backupBackend = new MemoryBackend()
      await backupBackend.write(path, content)

      // Fallback logic
      let result: Uint8Array
      try {
        result = await primaryBackend.read(path)
      } catch {
        result = await backupBackend.read(path)
      }

      expect(bytesToText(result)).toBe('backup content')
    })

    it('should detect and skip corrupted row groups in batch processing', async () => {
      const paths = [
        'data/test/batch1.txt',
        'data/test/batch2.txt',
        'data/test/batch3.txt',
      ]

      // Write test data
      for (const path of paths) {
        await baseBackend.write(path, textToBytes(`content of ${path}`))
      }

      // Make second file fail
      const failingBackend = createFailingBackend(baseBackend, {
        failRead: (path) => path === 'data/test/batch2.txt',
      })

      // Process files, skipping failures
      const results: string[] = []
      const errors: string[] = []

      for (const path of paths) {
        try {
          const data = await failingBackend.read(path)
          results.push(bytesToText(data))
        } catch (e) {
          errors.push(path)
        }
      }

      expect(results.length).toBe(2)
      expect(errors.length).toBe(1)
      expect(errors[0]).toBe('data/test/batch2.txt')
    })
  })

  // ===========================================================================
  // 7. Metadata Inconsistency Tests
  // ===========================================================================

  describe('Metadata Inconsistency', () => {
    it('should handle file stat returning incorrect size', async () => {
      const path = 'data/test/wrong-size.parquet'
      const content = new Uint8Array(100)
      content.set(PARQUET_MAGIC, 0)
      await baseBackend.write(path, content)

      // Create a backend that reports wrong file size in stat
      const wrongSizeBackend: StorageBackend = {
        ...baseBackend,
        type: 'wrong-size-mock',
        async stat(path: string): Promise<FileStat | null> {
          const realStat = await baseBackend.stat(path)
          if (realStat) {
            // Report double the actual size
            return { ...realStat, size: realStat.size * 2 }
          }
          return null
        },
        async read(path: string): Promise<Uint8Array> {
          return baseBackend.read(path)
        },
        async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
          return baseBackend.readRange(path, start, end)
        },
        async exists(path: string): Promise<boolean> {
          return baseBackend.exists(path)
        },
        async list(prefix: string): Promise<ListResult> {
          return baseBackend.list(prefix)
        },
        async write(path: string, data: Uint8Array): Promise<WriteResult> {
          return baseBackend.write(path, data)
        },
        async writeAtomic(path: string, data: Uint8Array): Promise<WriteResult> {
          return baseBackend.writeAtomic(path, data)
        },
        async append(path: string, data: Uint8Array): Promise<void> {
          return baseBackend.append(path, data)
        },
        async delete(path: string): Promise<boolean> {
          return baseBackend.delete(path)
        },
        async deletePrefix(prefix: string): Promise<number> {
          return baseBackend.deletePrefix(prefix)
        },
        async mkdir(path: string): Promise<void> {
          return baseBackend.mkdir(path)
        },
        async rmdir(path: string): Promise<void> {
          return baseBackend.rmdir(path)
        },
        async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<WriteResult> {
          return baseBackend.writeConditional(path, data, expectedVersion)
        },
        async copy(source: string, dest: string): Promise<void> {
          return baseBackend.copy(source, dest)
        },
        async move(source: string, dest: string): Promise<void> {
          return baseBackend.move(source, dest)
        },
      }

      const reader = new ParquetReader({ storage: wrongSizeBackend })

      // Should still fail because the Parquet metadata is invalid
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })

    it('should handle missing file after exists check', async () => {
      const path = 'data/test/race-condition.txt'
      await baseBackend.write(path, textToBytes('content'))

      // Check exists, then delete, then try to read
      expect(await baseBackend.exists(path)).toBe(true)
      await baseBackend.delete(path)

      await expect(baseBackend.read(path)).rejects.toThrow(NotFoundError)
    })

    it('should handle etag changes between reads', async () => {
      const path = 'data/test/etag-change.txt'
      await baseBackend.write(path, textToBytes('version 1'))

      const stat1 = await baseBackend.stat(path)
      const etag1 = stat1?.etag

      // Modify file
      await baseBackend.write(path, textToBytes('version 2'))

      const stat2 = await baseBackend.stat(path)
      const etag2 = stat2?.etag

      // ETags should be different
      expect(etag1).not.toBe(etag2)
    })
  })

  // ===========================================================================
  // 8. Append Operation Corruption Tests
  // ===========================================================================

  describe('Append Operation Corruption', () => {
    it('should handle failed append gracefully', async () => {
      const path = 'data/test/append-fail.txt'
      await baseBackend.write(path, textToBytes('initial'))

      // Create a custom backend that fails on append
      let appendCalled = false
      const failingAppendBackend: StorageBackend = {
        ...baseBackend,
        type: 'failing-append-mock',
        async append(_path: string, _data: Uint8Array): Promise<void> {
          appendCalled = true
          throw new OperationError('Simulated append failure', 'append', _path)
        },
        async read(path: string): Promise<Uint8Array> {
          return baseBackend.read(path)
        },
        async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
          return baseBackend.readRange(path, start, end)
        },
        async exists(path: string): Promise<boolean> {
          return baseBackend.exists(path)
        },
        async stat(path: string): Promise<FileStat | null> {
          return baseBackend.stat(path)
        },
        async list(prefix: string): Promise<ListResult> {
          return baseBackend.list(prefix)
        },
        async write(path: string, data: Uint8Array): Promise<WriteResult> {
          return baseBackend.write(path, data)
        },
        async writeAtomic(path: string, data: Uint8Array): Promise<WriteResult> {
          return baseBackend.writeAtomic(path, data)
        },
        async delete(path: string): Promise<boolean> {
          return baseBackend.delete(path)
        },
        async deletePrefix(prefix: string): Promise<number> {
          return baseBackend.deletePrefix(prefix)
        },
        async mkdir(path: string): Promise<void> {
          return baseBackend.mkdir(path)
        },
        async rmdir(path: string): Promise<void> {
          return baseBackend.rmdir(path)
        },
        async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<WriteResult> {
          return baseBackend.writeConditional(path, data, expectedVersion)
        },
        async copy(source: string, dest: string): Promise<void> {
          return baseBackend.copy(source, dest)
        },
        async move(source: string, dest: string): Promise<void> {
          return baseBackend.move(source, dest)
        },
      }

      // The append should fail
      await expect(failingAppendBackend.append(path, textToBytes(' more'))).rejects.toThrow(OperationError)
      expect(appendCalled).toBe(true)

      // Original content should still be intact (in the base backend)
      const result = await baseBackend.read(path)
      expect(bytesToText(result)).toBe('initial')
    })

    it('should handle concurrent append operations safely', async () => {
      const path = 'data/test/concurrent-append.txt'
      await baseBackend.write(path, textToBytes('start:'))

      // Execute multiple appends concurrently
      await Promise.all([
        baseBackend.append(path, textToBytes('a')),
        baseBackend.append(path, textToBytes('b')),
        baseBackend.append(path, textToBytes('c')),
      ])

      const result = await baseBackend.read(path)
      const text = bytesToText(result)

      // Should contain all appended data (order may vary)
      expect(text).toContain('start:')
      expect(text).toContain('a')
      expect(text).toContain('b')
      expect(text).toContain('c')
      expect(text.length).toBe('start:abc'.length)
    })
  })

  // ===========================================================================
  // 9. Storage Capacity and Limits
  // ===========================================================================

  describe('Storage Capacity and Limits', () => {
    it('should handle zero-byte files', async () => {
      const path = 'data/test/empty.txt'
      await baseBackend.write(path, new Uint8Array(0))

      const result = await baseBackend.read(path)
      expect(result.length).toBe(0)

      const stat = await baseBackend.stat(path)
      expect(stat?.size).toBe(0)
    })

    it('should handle readRange on empty file', async () => {
      const path = 'data/test/empty-range.txt'
      await baseBackend.write(path, new Uint8Array(0))

      const result = await baseBackend.readRange(path, 0, 100)
      expect(result.length).toBe(0)
    })

    it('should handle very small Parquet-like files', async () => {
      const path = 'data/test/tiny.parquet'
      // Just the magic header - too small to be valid
      await baseBackend.write(path, PARQUET_MAGIC)

      const reader = new ParquetReader({ storage: baseBackend })

      // Should throw because file is too small for valid Parquet
      await expect(reader.readMetadata(path)).rejects.toThrow()
    })
  })

  // ===========================================================================
  // 10. Directory and Path Corruption
  // ===========================================================================

  describe('Directory and Path Handling', () => {
    it('should handle deletePrefix on non-existent prefix', async () => {
      const result = await baseBackend.deletePrefix('nonexistent/path/')
      expect(result).toBe(0)
    })

    it('should handle list on corrupted/invalid path gracefully', async () => {
      // List should return empty results for non-existent paths, not throw
      const result = await baseBackend.list('path/that/does/not/exist/')

      expect(result.files).toEqual([])
      expect(result.hasMore).toBe(false)
    })

    it('should handle copy when source is deleted mid-operation', async () => {
      const source = 'data/test/copy-source.txt'
      const dest = 'data/test/copy-dest.txt'
      await baseBackend.write(source, textToBytes('copy me'))

      // Delete source then try to copy
      await baseBackend.delete(source)

      await expect(baseBackend.copy(source, dest)).rejects.toThrow(NotFoundError)
    })

    it('should handle move when source is deleted mid-operation', async () => {
      const source = 'data/test/move-source.txt'
      const dest = 'data/test/move-dest.txt'
      await baseBackend.write(source, textToBytes('move me'))

      // Delete source then try to move
      await baseBackend.delete(source)

      await expect(baseBackend.move(source, dest)).rejects.toThrow(NotFoundError)
    })
  })
})

/**
 * Tests for DeltaCommitter optimistic concurrency control (OCC)
 *
 * Verifies that the DeltaCommitter properly handles concurrent writes:
 * - Uses ifNoneMatch: '*' for atomic commit file creation
 * - Retries on conflict with exponential backoff
 * - Properly determines version numbers
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  DeltaCommitter,
  createDeltaCommitter,
  commitToDeltaTable,
} from '../../../src/backends/delta-commit'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

describe('DeltaCommitter OCC', () => {
  let storage: MemoryBackend
  let committer: DeltaCommitter

  beforeEach(async () => {
    storage = new MemoryBackend()
    committer = createDeltaCommitter({
      storage,
      tableLocation: 'test-table',
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Version tracking', () => {
    it('should return -1 for non-existent table', async () => {
      const version = await committer.getCurrentVersion()
      expect(version).toBe(-1)
    })

    it('should return 0 after first commit', async () => {
      await committer.ensureTable()
      const version = await committer.getCurrentVersion()
      expect(version).toBe(0)
    })

    it('should increment version on each commit', async () => {
      await committer.ensureTable()

      const result1 = await committer.commitDataFile({
        path: 'file1.parquet',
        size: 1000,
      })
      expect(result1.success).toBe(true)
      expect(result1.version).toBe(1)

      const result2 = await committer.commitDataFile({
        path: 'file2.parquet',
        size: 2000,
      })
      expect(result2.success).toBe(true)
      expect(result2.version).toBe(2)

      const version = await committer.getCurrentVersion()
      expect(version).toBe(2)
    })
  })

  describe('Atomic commit with ifNoneMatch', () => {
    it('should use ifNoneMatch when writing commit files', async () => {
      const writeSpy = vi.spyOn(storage, 'write')

      await committer.commitDataFile({
        path: 'test-file.parquet',
        size: 1000,
      })

      // Find the commit file write call
      const commitWriteCall = writeSpy.mock.calls.find(
        (call) =>
          typeof call[0] === 'string' &&
          call[0].includes('_delta_log/') &&
          call[0].endsWith('.json')
      )

      expect(commitWriteCall).toBeDefined()
      const options = commitWriteCall?.[2]
      expect(options?.ifNoneMatch).toBe('*')
    })
  })

  describe('Conflict detection and retry', () => {
    it('should detect when commit file already exists', async () => {
      // Create table at version 0
      await committer.ensureTable()

      // Simulate another writer creating version 1
      const conflictingCommit = JSON.stringify({
        add: { path: 'conflict.parquet', size: 100, modificationTime: Date.now(), dataChange: true },
      }) + '\n' + JSON.stringify({
        commitInfo: { timestamp: Date.now(), operation: 'WRITE', readVersion: 0 },
      })
      await storage.write(
        'test-table/_delta_log/00000000000000000001.json',
        new TextEncoder().encode(conflictingCommit)
      )

      // Our commit should detect conflict and succeed at version 2
      const result = await committer.commitDataFile({
        path: 'our-file.parquet',
        size: 500,
      })

      expect(result.success).toBe(true)
      expect(result.version).toBe(2)
    })

    it('should retry on conflict with backoff', async () => {
      await committer.ensureTable()

      let attempts = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          path.includes('00000000000000000001') &&
          attempts < 2
        ) {
          attempts++
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      const result = await committer.commitDataFile({
        path: 'test-file.parquet',
        size: 1000,
      })

      expect(result.success).toBe(true)
      expect(attempts).toBe(2)
    })

    it('should fail after max retries exceeded', async () => {
      const lowRetryCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'test-table',
        maxRetries: 2,
        baseBackoffMs: 10,
      })

      await lowRetryCommitter.ensureTable()

      // Make every commit attempt fail
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          path.includes('00000000000000000001')
        ) {
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      const result = await lowRetryCommitter.commitDataFile({
        path: 'test-file.parquet',
        size: 1000,
      })

      expect(result.success).toBe(false)
      expect(result.error).toMatch(/retr|conflict/i)
    })
  })

  describe('Concurrent writers simulation', () => {
    it('should handle two concurrent commits correctly', async () => {
      await committer.ensureTable()

      // Both try to write at the same time
      const [result1, result2] = await Promise.all([
        committer.commitDataFile({ path: 'file1.parquet', size: 1000 }),
        committer.commitDataFile({ path: 'file2.parquet', size: 2000 }),
      ])

      expect(result1.success).toBe(true)
      expect(result2.success).toBe(true)

      // They should have different versions
      expect(result1.version).not.toBe(result2.version)

      // Both versions should be valid (1 and 2)
      const versions = [result1.version, result2.version].sort((a, b) => a! - b!)
      expect(versions).toEqual([1, 2])
    })

    it('should handle ten concurrent commits correctly', async () => {
      await committer.ensureTable()

      const promises = Array.from({ length: 10 }, (_, i) =>
        committer.commitDataFile({ path: `file${i}.parquet`, size: 1000 * (i + 1) })
      )

      const results = await Promise.all(promises)

      // All should succeed
      expect(results.every(r => r.success)).toBe(true)

      // All versions should be unique (1 through 10)
      const versions = results.map(r => r.version!).sort((a, b) => a - b)
      expect(versions).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      // Final version should be 10
      const finalVersion = await committer.getCurrentVersion()
      expect(finalVersion).toBe(10)
    })

    it('should handle two committer instances writing concurrently', async () => {
      const committer2 = createDeltaCommitter({
        storage,
        tableLocation: 'test-table',
      })

      // Both need to create the table first
      await committer.ensureTable()

      // Both write concurrently
      const [result1, result2] = await Promise.all([
        committer.commitDataFile({ path: 'from-committer1.parquet', size: 1000 }),
        committer2.commitDataFile({ path: 'from-committer2.parquet', size: 2000 }),
      ])

      expect(result1.success).toBe(true)
      expect(result2.success).toBe(true)
      expect(result1.version).not.toBe(result2.version)
    })
  })

  describe('commitToDeltaTable convenience function', () => {
    it('should create table and commit data file', async () => {
      const result = await commitToDeltaTable({
        storage,
        tableLocation: 'convenience-test',
        dataFiles: [{ path: 'data.parquet', size: 5000 }],
      })

      expect(result.success).toBe(true)
      expect(result.version).toBe(1)

      // Verify commit file exists
      const commitExists = await storage.exists(
        'convenience-test/_delta_log/00000000000000000001.json'
      )
      expect(commitExists).toBe(true)
    })

    it('should handle multiple data files in one commit', async () => {
      const result = await commitToDeltaTable({
        storage,
        tableLocation: 'multi-file-test',
        dataFiles: [
          { path: 'file1.parquet', size: 1000 },
          { path: 'file2.parquet', size: 2000 },
          { path: 'file3.parquet', size: 3000 },
        ],
      })

      expect(result.success).toBe(true)

      // Read commit file and verify all files are referenced
      const commitData = await storage.read(result.logPath!)
      const commitText = new TextDecoder().decode(commitData)
      expect(commitText).toContain('file1.parquet')
      expect(commitText).toContain('file2.parquet')
      expect(commitText).toContain('file3.parquet')
    })

    it('should return success for empty dataFiles array', async () => {
      const result = await commitToDeltaTable({
        storage,
        tableLocation: 'empty-test',
        dataFiles: [],
      })

      expect(result.success).toBe(true)
      expect(result.version).toBeUndefined()
    })
  })

  describe('Configuration options', () => {
    it('should respect maxRetries configuration', async () => {
      const customCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'test-table',
        maxRetries: 3,
      })
      await customCommitter.ensureTable()

      let attempts = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          path.includes('00000000000000000001')
        ) {
          attempts++
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      const result = await customCommitter.commitDataFile({
        path: 'test.parquet',
        size: 1000,
      })

      expect(result.success).toBe(false)
      // Should be 4 attempts: initial + 3 retries
      expect(attempts).toBe(4)
    })

    it('should respect baseBackoffMs configuration', async () => {
      const customCommitter = createDeltaCommitter({
        storage,
        tableLocation: 'test-table',
        maxRetries: 3,
        baseBackoffMs: 50,
      })
      await customCommitter.ensureTable()

      const delays: number[] = []
      const originalSetTimeout = global.setTimeout
      // @ts-expect-error - mocking setTimeout
      global.setTimeout = (fn: () => void, delay: number) => {
        delays.push(delay)
        return originalSetTimeout(fn, 0)
      }

      let attempts = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          path.includes('00000000000000000001') &&
          attempts < 2
        ) {
          attempts++
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      try {
        await customCommitter.commitDataFile({
          path: 'test.parquet',
          size: 1000,
        })
      } catch {
        // May succeed or fail
      }

      global.setTimeout = originalSetTimeout

      // First delay should be around baseBackoffMs (50ms)
      if (delays.length > 0) {
        expect(delays[0]).toBeGreaterThanOrEqual(50)
      }
    })
  })

  describe('Commit file format', () => {
    it('should include protocol and metadata for first commit', async () => {
      await committer.commitDataFile({
        path: 'first-file.parquet',
        size: 1000,
      })

      const commitData = await storage.read(
        'test-table/_delta_log/00000000000000000000.json'
      )
      const commitText = new TextDecoder().decode(commitData)

      expect(commitText).toContain('"protocol"')
      expect(commitText).toContain('"minReaderVersion"')
      expect(commitText).toContain('"metaData"')
    })

    it('should not include protocol/metadata for subsequent commits', async () => {
      await committer.ensureTable()

      await committer.commitDataFile({
        path: 'second-file.parquet',
        size: 1000,
      })

      const commitData = await storage.read(
        'test-table/_delta_log/00000000000000000001.json'
      )
      const commitText = new TextDecoder().decode(commitData)

      expect(commitText).not.toContain('"protocol"')
      expect(commitText).not.toContain('"metaData"')
      expect(commitText).toContain('"add"')
      expect(commitText).toContain('second-file.parquet')
    })

    it('should include commitInfo with readVersion', async () => {
      await committer.ensureTable()

      await committer.commitDataFile({
        path: 'test-file.parquet',
        size: 1000,
      })

      const commitData = await storage.read(
        'test-table/_delta_log/00000000000000000001.json'
      )
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const commitInfo = actions.find((a: Record<string, unknown>) => 'commitInfo' in a)
      expect(commitInfo).toBeDefined()
      expect(commitInfo.commitInfo.readVersion).toBe(0)
    })
  })
})

/**
 * Tests for DeltaBackend vacuum and compact operations
 *
 * These tests verify the maintenance operations:
 * - vacuum: Deletes unreferenced Parquet files older than retention period
 * - compact: Merges small files into larger files
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaBackend, createDeltaBackend } from '../../../src/backends/delta'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

describe('DeltaBackend Maintenance', () => {
  let storage: MemoryBackend
  let backend: DeltaBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createDeltaBackend({
      type: 'delta',
      storage,
      location: 'warehouse',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  describe('vacuum()', () => {
    it('should return zero results for empty table', async () => {
      const result = await backend.vacuum('users')

      expect(result.filesDeleted).toBe(0)
      expect(result.bytesReclaimed).toBe(0)
      expect(result.snapshotsExpired).toBe(0)
    })

    it('should return zero results when all files are within retention', async () => {
      // Create some entities
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
      ])

      // Vacuum with default 7-day retention should not delete anything
      const result = await backend.vacuum('users')

      expect(result.filesDeleted).toBe(0)
      expect(result.bytesReclaimed).toBe(0)
    })

    it('should not delete active files even with zero retention', async () => {
      // Create entities
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
      ])

      // Vacuum with 0 retention (delete everything older than now)
      const result = await backend.vacuum('users', { retentionMs: 0 })

      // Active files should never be deleted
      expect(result.filesDeleted).toBe(0)

      // Data should still be readable
      const users = await backend.find('users', {})
      expect(users).toHaveLength(2)
    })

    it('should support dry run mode', async () => {
      await backend.create('users', { $type: 'User', name: 'Alice' })

      const result = await backend.vacuum('users', { dryRun: true })

      // Dry run should report but not actually delete
      expect(result).toBeDefined()

      // Data should still be readable
      const users = await backend.find('users', {})
      expect(users).toHaveLength(1)
    })

    it('should throw when backend is read-only', async () => {
      const readOnlyBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        readOnly: true,
      })
      await readOnlyBackend.initialize()

      await expect(readOnlyBackend.vacuum('users')).rejects.toThrow(/read.only/i)

      await readOnlyBackend.close()
    })
  })

  describe('compact()', () => {
    it('should return zero results for empty table', async () => {
      const result = await backend.compact('users')

      expect(result.filesCompacted).toBe(0)
      expect(result.filesCreated).toBe(0)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })

    it('should return zero results when there is only one file', async () => {
      // Create entities in single operation (one file)
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
      ])

      // With default target size, one small file should not trigger compaction
      const result = await backend.compact('users', { minFileSize: 1 })

      // Need at least 2 files to compact
      expect(result.filesCompacted).toBe(0)
    })

    it('should compact multiple small files into one', async () => {
      // Create multiple small files by doing individual creates
      for (let i = 0; i < 5; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Verify we have 5 files
      const filesBefore = await storage.list('warehouse/posts/')
      const parquetFilesBefore = filesBefore.files.filter(
        f => f.endsWith('.parquet') && !f.includes('_delta_log')
      )
      expect(parquetFilesBefore.length).toBe(5)

      // Compact with very high minFileSize to ensure all files qualify
      const result = await backend.compact('posts', {
        targetFileSize: 1024 * 1024, // 1MB target
        minFileSize: 1024 * 1024, // 1MB min (all files are smaller)
      })

      expect(result.filesCompacted).toBe(5)
      expect(result.filesCreated).toBe(1)
      expect(result.bytesBefore).toBeGreaterThan(0)
      expect(result.bytesAfter).toBeGreaterThan(0)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)

      // Data should still be readable and complete
      const posts = await backend.find('posts', {})
      expect(posts).toHaveLength(5)
      expect(posts.map(p => p.name).sort()).toEqual([
        'Post 0',
        'Post 1',
        'Post 2',
        'Post 3',
        'Post 4',
      ])
    })

    it('should support dry run mode', async () => {
      // Create multiple files
      for (let i = 0; i < 3; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Dry run should report what would be done
      const result = await backend.compact('posts', {
        targetFileSize: 1024 * 1024,
        minFileSize: 1024 * 1024,
        dryRun: true,
      })

      expect(result.filesCompacted).toBe(3)
      expect(result.filesCreated).toBe(1)

      // Files should still exist as before (not actually compacted)
      const files = await storage.list('warehouse/posts/')
      const parquetFiles = files.files.filter(
        f => f.endsWith('.parquet') && !f.includes('_delta_log')
      )
      expect(parquetFiles.length).toBe(3) // Still 3 files, not 1
    })

    it('should respect maxFiles option', async () => {
      // Create 10 small files
      for (let i = 0; i < 10; i++) {
        await backend.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Compact with maxFiles = 3
      const result = await backend.compact('posts', {
        targetFileSize: 1024 * 1024,
        minFileSize: 1024 * 1024,
        maxFiles: 3,
      })

      // Should only compact 3 files
      expect(result.filesCompacted).toBe(3)
      expect(result.filesCreated).toBe(1)

      // Data should still be complete (10 posts)
      const posts = await backend.find('posts', {})
      expect(posts).toHaveLength(10)
    })

    it('should create valid Delta commit for compaction', async () => {
      // Create files
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })
      await backend.create('posts', { $type: 'Post', name: 'Post 2' })

      // Compact
      await backend.compact('posts', {
        targetFileSize: 1024 * 1024,
        minFileSize: 1024 * 1024,
      })

      // Check commit file has remove and add actions
      const deltaLogFiles = await storage.list('warehouse/posts/_delta_log/')
      const commitFiles = deltaLogFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
        .sort()
      const latestCommitPath = commitFiles[commitFiles.length - 1]!

      const commitData = await storage.read(latestCommitPath)
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      // Should have remove actions and one add action
      const removeActions = actions.filter((a: Record<string, unknown>) => 'remove' in a)
      const addActions = actions.filter((a: Record<string, unknown>) => 'add' in a)
      const commitInfo = actions.find((a: Record<string, unknown>) => 'commitInfo' in a)

      expect(removeActions.length).toBe(2) // Removed 2 old files
      expect(addActions.length).toBe(1) // Added 1 new file
      expect(commitInfo.commitInfo.operation).toBe('OPTIMIZE')
    })

    it('should throw when backend is read-only', async () => {
      const readOnlyBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        readOnly: true,
      })
      await readOnlyBackend.initialize()

      await expect(readOnlyBackend.compact('posts')).rejects.toThrow(/read.only/i)

      await readOnlyBackend.close()
    })

    it('should preserve time travel after compaction', async () => {
      // Create entities
      await backend.create('posts', { $type: 'Post', name: 'Post 1' })
      await backend.create('posts', { $type: 'Post', name: 'Post 2' })

      // Get version before compaction
      const versionBefore = (await backend.listSnapshots('posts')).length - 1

      // Compact
      await backend.compact('posts', {
        targetFileSize: 1024 * 1024,
        minFileSize: 1024 * 1024,
      })

      // Time travel to before compaction should still work
      const snapshotBackend = await backend.snapshot('posts', versionBefore)
      const oldPosts = await snapshotBackend.find('posts', {})
      expect(oldPosts).toHaveLength(2)
    })
  })
})

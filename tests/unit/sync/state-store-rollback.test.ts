/**
 * State Store Rollback Tests
 *
 * Tests for critical data safety during state reconstruction rollback.
 * Verifies that:
 * - Rollback failures are properly reported
 * - Backups are preserved when rollback fails
 * - Database can be manually recovered after failed rollback
 *
 * Issue: parquedb-o2q8 - Critical data loss risk in state reconstruction rollback
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  storeObject,
  reconstructState,
} from '../../../src/sync/state-store'
import { createCommit, type DatabaseCommit } from '../../../src/sync/commit'

/**
 * A storage backend wrapper that can be configured to fail on specific operations.
 * Used to test rollback behavior when storage operations fail.
 */
class FailingStorageBackend implements MemoryBackend {
  readonly type = 'memory' as const
  private backend: MemoryBackend
  private failOnCopyTo: Map<string, number> = new Map()
  private failOnCopyFrom: Map<string, number> = new Map()
  private failOnDelete: Map<string, number> = new Map()
  private copyOperations: Array<{ from: string; to: string }> = []

  constructor(backend: MemoryBackend) {
    this.backend = backend
  }

  /**
   * Configure copy to fail when copying to a specific destination path
   */
  failCopyTo(destPath: string, times = 1): void {
    this.failOnCopyTo.set(destPath, times)
  }

  /**
   * Configure copy to fail when copying from a specific source path (e.g., backup files)
   */
  failCopyFrom(sourcePath: string, times = 1): void {
    this.failOnCopyFrom.set(sourcePath, times)
  }

  /**
   * Configure copy to fail when copying from any path matching a pattern
   */
  failCopyFromPattern(pattern: string, times = 1): void {
    this.failOnCopyFrom.set(`pattern:${pattern}`, times)
  }

  /**
   * Configure delete to fail for a specific path
   */
  failDeleteOf(path: string, times = 1): void {
    this.failOnDelete.set(path, times)
  }

  /**
   * Get recorded copy operations for debugging
   */
  getCopyOperations(): Array<{ from: string; to: string }> {
    return [...this.copyOperations]
  }

  private shouldFail(map: Map<string, number>, key: string): boolean {
    const remaining = map.get(key) ?? 0
    if (remaining > 0) {
      map.set(key, remaining - 1)
      return true
    }
    return false
  }

  private shouldFailPattern(map: Map<string, number>, value: string): boolean {
    for (const [key, remaining] of map.entries()) {
      if (key.startsWith('pattern:') && remaining > 0) {
        const pattern = key.slice('pattern:'.length)
        if (value.includes(pattern)) {
          map.set(key, remaining - 1)
          return true
        }
      }
    }
    return false
  }

  // Delegate all methods to backend, with failure injection

  async read(path: string): Promise<Uint8Array> {
    return this.backend.read(path)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    return this.backend.readRange(path, start, end)
  }

  async exists(path: string): Promise<boolean> {
    return this.backend.exists(path)
  }

  async stat(path: string) {
    return this.backend.stat(path)
  }

  async list(prefix: string, options?: Parameters<MemoryBackend['list']>[1]) {
    return this.backend.list(prefix, options)
  }

  async write(path: string, data: Uint8Array, options?: Parameters<MemoryBackend['write']>[2]) {
    return this.backend.write(path, data, options)
  }

  async writeAtomic(path: string, data: Uint8Array, options?: Parameters<MemoryBackend['writeAtomic']>[2]) {
    return this.backend.writeAtomic(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.backend.append(path, data)
  }

  async delete(path: string): Promise<boolean> {
    if (this.shouldFail(this.failOnDelete, path)) {
      throw new Error(`Simulated delete failure for ${path}`)
    }
    return this.backend.delete(path)
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.backend.deletePrefix(prefix)
  }

  async mkdir(path: string): Promise<void> {
    return this.backend.mkdir(path)
  }

  async rmdir(path: string, options?: Parameters<MemoryBackend['rmdir']>[1]): Promise<void> {
    return this.backend.rmdir(path, options)
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: Parameters<MemoryBackend['writeConditional']>[3]
  ) {
    return this.backend.writeConditional(path, data, expectedVersion, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    this.copyOperations.push({ from: source, to: dest })

    // Check if we should fail on destination
    if (this.shouldFail(this.failOnCopyTo, dest)) {
      throw new Error(`Simulated copy failure to ${dest}`)
    }

    // Check if we should fail on source
    if (this.shouldFail(this.failOnCopyFrom, source)) {
      throw new Error(`Simulated copy failure from ${source}`)
    }

    // Check if we should fail on source pattern
    if (this.shouldFailPattern(this.failOnCopyFrom, source)) {
      throw new Error(`Simulated copy failure from ${source}`)
    }

    return this.backend.copy(source, dest)
  }

  async move(source: string, dest: string): Promise<void> {
    return this.backend.move(source, dest)
  }

  /**
   * Access to underlying backend for verification
   */
  getBackend(): MemoryBackend {
    return this.backend
  }
}

describe('state-store-rollback', () => {
  let backend: MemoryBackend
  let storage: FailingStorageBackend

  beforeEach(() => {
    backend = new MemoryBackend()
    storage = new FailingStorageBackend(backend)
  })

  /**
   * Helper to create a commit with data stored in the object store
   */
  async function createCommitWithData(
    collections: Record<string, string>,
    parents: string[] = []
  ): Promise<DatabaseCommit> {
    const collectionState: Record<string, { dataHash: string; schemaHash: string; rowCount: number }> = {}

    for (const [ns, content] of Object.entries(collections)) {
      const data = new TextEncoder().encode(content)
      const hash = await storeObject(storage, data)
      collectionState[ns] = {
        dataHash: hash,
        schemaHash: '',
        rowCount: 1,
      }
    }

    const emptyHash = await storeObject(storage, new TextEncoder().encode('{}'))

    return createCommit(
      {
        collections: collectionState,
        relationships: {
          forwardHash: emptyHash,
          reverseHash: emptyHash,
        },
        eventLogPosition: {
          segmentId: 'initial',
          offset: 0,
        },
      },
      {
        message: 'Test commit',
        author: 'test',
        parents,
      }
    )
  }

  describe('rollback failure handling', () => {
    it('should preserve backups when rollback fails partway through', async () => {
      // Setup: Create existing data files
      await storage.write('data/posts/data.parquet', new TextEncoder().encode('original posts'))
      await storage.write('data/users/data.parquet', new TextEncoder().encode('original users'))

      // Create a commit that will fail (references non-existent object)
      // First store a valid object for 'posts' so backup is created before failure
      const postsData = new TextEncoder().encode('new posts data')
      const postsHash = await storeObject(storage, postsData)

      const badCommit = await createCommit(
        {
          collections: {
            posts: {
              dataHash: postsHash,  // Valid - will be processed first
              schemaHash: '',
              rowCount: 1,
            },
            users: {
              dataHash: 'nonexistent_object_hash_that_will_fail',  // Invalid - will trigger rollback
              schemaHash: '',
              rowCount: 1,
            },
          },
          relationships: {
            forwardHash: await storeObject(storage, new TextEncoder().encode('{}')),
            reverseHash: await storeObject(storage, new TextEncoder().encode('{}')),
          },
          eventLogPosition: {
            segmentId: 'initial',
            offset: 0,
          },
        },
        {
          message: 'Partially bad commit',
          author: 'test',
          parents: [],
        }
      )

      // Configure storage to fail during rollback when copying FROM backup files
      // During rollback, it will copy from 'data/users/data.parquet.backup-XXX' to 'data/users/data.parquet'
      // We fail the copy TO 'data/users/data.parquet' (which happens during rollback)
      // But this copy happens AFTER the first file is already restored
      //
      // Actually, we need to let the first copy during rollback succeed (to restore posts),
      // then fail the second copy during rollback (to restore users)
      // Since rollback copies TO original paths, we need to fail the second copy to an original path
      storage.failCopyTo('data/users/data.parquet', 1)

      // Attempt reconstruction - should fail during users restoration
      let error: Error | undefined
      try {
        await reconstructState(storage, badCommit)
      } catch (e) {
        error = e as Error
      }

      // Verify it failed with CRITICAL error (rollback failed)
      expect(error).toBeDefined()
      expect(error?.message).toContain('CRITICAL')
      expect(error?.message).toContain('rollback was incomplete')
      expect(error?.message).toContain('data/users/data.parquet')

      // CRITICAL: Verify backup files are preserved for manual recovery
      // Find the backup files
      const allFiles = await backend.list('data/', { delimiter: undefined })
      const backupFiles = allFiles.files.filter(f => f.includes('.backup-'))

      // The backup for 'users' should still exist because rollback failed for that file
      const usersBackup = backupFiles.find(f => f.includes('users'))
      expect(usersBackup).toBeDefined()

      // The backup should contain the original data
      if (usersBackup) {
        const backupData = await backend.read(usersBackup)
        expect(new TextDecoder().decode(backupData)).toBe('original users')
      }

      // IMPORTANT: The backup for 'posts' should ALSO still exist
      // This is the bug we're fixing - currently, after successfully restoring 'posts',
      // its backup is deleted BEFORE attempting to restore 'users'
      // If 'users' restore fails, 'posts' backup is gone
      const postsBackup = backupFiles.find(f => f.includes('posts'))
      expect(postsBackup).toBeDefined()  // This will fail with the current bug

      if (postsBackup) {
        const backupData = await backend.read(postsBackup)
        expect(new TextDecoder().decode(backupData)).toBe('original posts')
      }
    })

    it('should report which files were successfully restored and which failed', async () => {
      // Setup multiple data files
      await storage.write('data/a/data.parquet', new TextEncoder().encode('file a'))
      await storage.write('data/b/data.parquet', new TextEncoder().encode('file b'))
      await storage.write('data/c/data.parquet', new TextEncoder().encode('file c'))

      // Store valid hashes for a and b, invalid for c
      const hashA = await storeObject(storage, new TextEncoder().encode('new a'))
      const hashB = await storeObject(storage, new TextEncoder().encode('new b'))

      // Create commit where 'c' will fail (triggers rollback)
      const badCommit = await createCommit(
        {
          collections: {
            a: { dataHash: hashA, schemaHash: '', rowCount: 1 },
            b: { dataHash: hashB, schemaHash: '', rowCount: 1 },
            c: { dataHash: 'bad_hash_c', schemaHash: '', rowCount: 1 },  // This triggers rollback
          },
          relationships: {
            forwardHash: await storeObject(storage, new TextEncoder().encode('{}')),
            reverseHash: await storeObject(storage, new TextEncoder().encode('{}')),
          },
          eventLogPosition: { segmentId: 'initial', offset: 0 },
        },
        { message: 'Bad', author: 'test', parents: [] }
      )

      // Fail rollback for file b only (during the copy from backup to original)
      storage.failCopyTo('data/b/data.parquet', 1)

      let error: Error | undefined
      try {
        await reconstructState(storage, badCommit)
      } catch (e) {
        error = e as Error
      }

      // Should show which restoration failed
      expect(error?.message).toContain('CRITICAL')
      expect(error?.message).toContain('data/b/data.parquet')

      // Should mention the backup suffix for manual recovery
      expect(error?.message).toMatch(/backup.*suffix.*\.backup-\d+/i)
    })

    it('should not delete backup files until ALL rollback operations succeed', async () => {
      // This is the key test - the bug was that we deleted backups as we restored,
      // so if later restorations failed, earlier backups were already gone

      await storage.write('data/first/data.parquet', new TextEncoder().encode('first original'))
      await storage.write('data/second/data.parquet', new TextEncoder().encode('second original'))
      await storage.write('data/third/data.parquet', new TextEncoder().encode('third original'))

      // Store valid hashes for first and second, invalid for third
      const firstHash = await storeObject(storage, new TextEncoder().encode('new first'))
      const secondHash = await storeObject(storage, new TextEncoder().encode('new second'))

      const badCommit = await createCommit(
        {
          collections: {
            first: { dataHash: firstHash, schemaHash: '', rowCount: 1 },
            second: { dataHash: secondHash, schemaHash: '', rowCount: 1 },
            third: { dataHash: 'bad_third_hash', schemaHash: '', rowCount: 1 },  // Triggers rollback
          },
          relationships: {
            forwardHash: await storeObject(storage, new TextEncoder().encode('{}')),
            reverseHash: await storeObject(storage, new TextEncoder().encode('{}')),
          },
          eventLogPosition: { segmentId: 'initial', offset: 0 },
        },
        { message: 'Bad', author: 'test', parents: [] }
      )

      // Fail rollback for the second file - at this point, first file's backup
      // should NOT have been deleted yet
      storage.failCopyTo('data/second/data.parquet', 1)

      let error: Error | undefined
      try {
        await reconstructState(storage, badCommit)
      } catch (e) {
        error = e as Error
      }

      expect(error).toBeDefined()
      expect(error?.message).toContain('CRITICAL')

      // BOTH backup files for first and second should still exist for manual recovery
      // (third was never modified because we failed before getting to it)
      const allFiles = await backend.list('data/', { delimiter: undefined })
      const backupFiles = allFiles.files.filter(f => f.includes('.backup-'))

      // Should have backup for BOTH first and second - not just the one that failed
      const firstBackup = backupFiles.find(f => f.includes('first'))
      const secondBackup = backupFiles.find(f => f.includes('second'))

      // THIS IS THE BUG: With the current code, firstBackup is deleted after
      // successfully restoring 'first', but BEFORE attempting to restore 'second'
      // If 'second' restore fails, 'first' backup is gone
      expect(firstBackup).toBeDefined()
      expect(secondBackup).toBeDefined()

      // Verify backups contain original data
      if (firstBackup) {
        const data = await backend.read(firstBackup)
        expect(new TextDecoder().decode(data)).toBe('first original')
      }
      if (secondBackup) {
        const data = await backend.read(secondBackup)
        expect(new TextDecoder().decode(data)).toBe('second original')
      }
    })

    it('should clean up backups only after successful rollback', async () => {
      // Setup
      await storage.write('data/test/data.parquet', new TextEncoder().encode('original'))

      const badCommit = await createCommit(
        {
          collections: {
            test: { dataHash: 'nonexistent', schemaHash: '', rowCount: 1 },
          },
          relationships: {
            forwardHash: await storeObject(storage, new TextEncoder().encode('{}')),
            reverseHash: await storeObject(storage, new TextEncoder().encode('{}')),
          },
          eventLogPosition: { segmentId: 'initial', offset: 0 },
        },
        { message: 'Bad', author: 'test', parents: [] }
      )

      // Don't configure any failures - rollback should succeed
      let error: Error | undefined
      try {
        await reconstructState(storage, badCommit)
      } catch (e) {
        error = e as Error
      }

      // Should have failed but with successful rollback
      expect(error).toBeDefined()
      expect(error?.message).toContain('rolled back to previous state')
      expect(error?.message).not.toContain('CRITICAL')

      // After successful rollback, backups should be cleaned up
      const allFiles = await backend.list('data/', { delimiter: undefined })
      const backupFiles = allFiles.files.filter(f => f.includes('.backup-'))

      expect(backupFiles).toHaveLength(0)

      // Original data should be restored
      const restored = await backend.read('data/test/data.parquet')
      expect(new TextDecoder().decode(restored)).toBe('original')
    })
  })
})

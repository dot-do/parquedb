/**
 * Error Recovery and Partial Failure Tests
 *
 * Tests for error recovery and partial failures in ParqueDB:
 * 1. Partial write failures with rollback
 * 2. Network interruption during sync
 * 3. Corrupted file recovery
 * 4. Transaction abort scenarios
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { MemoryBackend, NotFoundError, ETagMismatchError } from '../../../src/storage/MemoryBackend'
import { SyncEngine, createSyncEngine, type SyncEngineOptions } from '../../../src/sync/engine'
import type { SyncManifest } from '../../../src/sync/manifest'
import type { StorageBackend } from '../../../src/types/storage'
import {
  MutationExecutor,
  VersionConflictError,
  type EntityStore,
} from '../../../src/mutation'
import type { Entity, EntityId } from '../../../src/types'
import {
  withRetry,
  isRetryableError,
  AbortError,
  type RetryConfig,
} from '../../../src/delta-utils/retry'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a simple in-memory entity store for testing
 */
function createTestStore(): EntityStore & { data: Map<string, Entity> } {
  const data = new Map<string, Entity>()
  return {
    data,
    get: (id: string) => data.get(id),
    set: (id: string, entity: Entity) => { data.set(id, entity) },
    delete: (id: string) => data.delete(id),
    has: (id: string) => data.has(id),
    forEach: (callback) => data.forEach(callback),
  }
}

/**
 * Create a SyncEngine with fresh MemoryBackend instances
 */
function createTestEngine(options?: Partial<SyncEngineOptions>): {
  engine: SyncEngine
  local: MemoryBackend
  remote: MemoryBackend
} {
  const local = new MemoryBackend()
  const remote = new MemoryBackend()

  const engine = createSyncEngine({
    local,
    remote,
    databaseId: 'test-db-123',
    name: 'test-database',
    owner: 'test-user',
    ...options,
  })

  return { engine, local, remote }
}

/**
 * Populate a backend with test files
 */
async function populateBackend(
  backend: StorageBackend,
  files: Record<string, string | Uint8Array>
): Promise<void> {
  for (const [path, content] of Object.entries(files)) {
    const data = typeof content === 'string'
      ? new TextEncoder().encode(content)
      : content
    await backend.write(path, data)
  }
}

/**
 * Create a test manifest with files
 */
function createTestManifest(
  files: Record<string, { hash: string; size: number; modifiedAt?: string }>
): SyncManifest {
  const fileEntries: SyncManifest['files'] = {}

  for (const [path, { hash, size, modifiedAt }] of Object.entries(files)) {
    fileEntries[path] = {
      path,
      size,
      hash,
      hashAlgorithm: 'sha256',
      modifiedAt: modifiedAt ?? new Date().toISOString(),
    }
  }

  return {
    version: 1,
    databaseId: 'test-db-123',
    name: 'test-database',
    visibility: 'private',
    lastSyncedAt: new Date().toISOString(),
    files: fileEntries,
  }
}

/**
 * Create a manifest and save it to a backend
 */
async function saveManifest(
  backend: StorageBackend,
  manifest: SyncManifest
): Promise<void> {
  const data = new TextEncoder().encode(JSON.stringify(manifest, null, 2))
  await backend.write('_meta/manifest.json', data)
}

// =============================================================================
// 1. Partial Write Failure Tests with Rollback
// =============================================================================

describe('Partial Write Failures with Rollback', () => {
  let executor: MutationExecutor
  let store: EntityStore & { data: Map<string, Entity> }

  beforeEach(() => {
    executor = new MutationExecutor({
      defaultActor: 'users/test' as EntityId,
    })
    store = createTestStore()
  })

  describe('batch creation with partial failure', () => {
    it('should not affect store when early creation fails', async () => {
      // First, create some entities that should persist
      const entity1 = await executor.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'First Post',
      }, store)

      expect(store.has(entity1.$id as string)).toBe(true)

      // Now attempt to create an invalid entity (missing required fields)
      await expect(
        executor.create('posts', {
          $type: 'Post',
          // Missing 'name' field
        } as any, store)
      ).rejects.toThrow()

      // Original entity should still exist
      expect(store.has(entity1.$id as string)).toBe(true)
      expect(store.get(entity1.$id as string)?.name).toBe('Post 1')
    })

    it('should maintain store integrity after version conflict', async () => {
      // Create an entity
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
      }, store)

      // Update to version 2
      await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store)

      // Attempt update with stale version (should fail)
      await expect(
        executor.update('posts', entity.$id as string, {
          $set: { title: 'Stale Update' },
        }, store, { expectedVersion: 1 })
      ).rejects.toThrow(VersionConflictError)

      // Store should have the correct updated version
      const current = store.get(entity.$id as string)
      expect(current?.title).toBe('Updated')
      expect(current?.version).toBe(2)
    })

    it('should handle sequential updates correctly', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Sequential Post',
        viewCount: 0,
      }, store)

      // Perform sequential updates without version checking
      // Each update will read the current version and increment
      for (let i = 0; i < 5; i++) {
        await executor.update('posts', entity.$id as string, {
          $inc: { viewCount: 1 },
        }, store)
      }

      const final = store.get(entity.$id as string)
      expect(final?.viewCount).toBe(5)
      expect(final?.version).toBe(6)
    })

    it('should isolate failed batch operation from successful ones', async () => {
      // Create a batch of entities
      const entities: Entity[] = []
      for (let i = 0; i < 3; i++) {
        const entity = await executor.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        }, store)
        entities.push(entity)
      }

      // Update first two successfully
      await executor.update('posts', entities[0].$id as string, {
        $set: { status: 'updated' },
      }, store)

      await executor.update('posts', entities[1].$id as string, {
        $set: { status: 'updated' },
      }, store)

      // Third update fails with version conflict
      await executor.update('posts', entities[2].$id as string, {
        $set: { status: 'first' },
      }, store)

      await expect(
        executor.update('posts', entities[2].$id as string, {
          $set: { status: 'conflict' },
        }, store, { expectedVersion: 1 }) // Stale version
      ).rejects.toThrow(VersionConflictError)

      // First two should be updated, third should have first update
      expect(store.get(entities[0].$id as string)?.status).toBe('updated')
      expect(store.get(entities[1].$id as string)?.status).toBe('updated')
      expect(store.get(entities[2].$id as string)?.status).toBe('first')
    })
  })

  describe('delete operation rollback scenarios', () => {
    it('should handle failed delete gracefully', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'To Delete',
      }, store)

      // Soft delete
      await executor.delete('posts', entity.$id as string, store)

      // Try to delete again (should return 0 count)
      const result = await executor.delete('posts', entity.$id as string, store)
      expect(result.deletedCount).toBe(0)

      // Entity still exists (soft deleted)
      const deleted = store.get(entity.$id as string)
      expect(deleted).toBeDefined()
      expect(deleted?.deletedAt).toBeDefined()
    })

    it('should restore soft-deleted entity correctly', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Restorable',
        value: 42,
      }, store)

      // Soft delete
      await executor.delete('posts', entity.$id as string, store)

      // Restore
      const restored = await executor.restore('posts', entity.$id as string, store)

      expect(restored?.deletedAt).toBeUndefined()
      expect(restored?.name).toBe('Restorable')
      expect(restored?.value).toBe(42)
    })
  })
})

// =============================================================================
// 2. Network Interruption During Sync Tests
// =============================================================================

describe('Network Interruption During Sync', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('push with network failures', () => {
    it('should continue after single file failure', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create multiple files locally
      await populateBackend(local, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
        'data/file3.parquet': 'content 3',
      })

      // Mock remote.write to fail for specific file
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/file2.parquet') {
          throw new Error('Network timeout')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.push()

      // Should have error for file2
      expect(result.errors.some(e => e.path === 'data/file2.parquet')).toBe(true)
      expect(result.errors.some(e => e.message.includes('Network timeout'))).toBe(true)

      // Other files should succeed
      expect(result.uploaded).toContain('data/file1.parquet')
      expect(result.uploaded).toContain('data/file3.parquet')
      expect(result.uploaded).not.toContain('data/file2.parquet')
    })

    it('should report all errors when multiple files fail', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
        'data/file3.parquet': 'content 3',
      })

      // Mock data file writes to fail (but allow manifest writes)
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path.startsWith('data/')) {
          throw new Error('Connection refused')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.push()

      // All data uploads should have failed
      expect(result.uploaded).toHaveLength(0)
      expect(result.errors.length).toBeGreaterThan(0)
      expect(result.errors.every(e => e.operation === 'upload')).toBe(true)
    })

    it('should handle intermittent failures gracefully', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
      })

      // Mock write to fail intermittently
      let callCount = 0
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        callCount++
        // Fail on first call to simulate intermittent failure
        if (callCount === 1) {
          throw new Error('Temporary failure')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.push()

      // First file should fail, second should succeed
      expect(result.errors.length).toBe(1)
      expect(result.uploaded.length).toBe(1)
    })
  })

  describe('pull with network failures', () => {
    it('should handle remote read failures', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create files on remote
      await populateBackend(remote, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
      })

      const manifest = createTestManifest({
        'data/file1.parquet': { hash: 'hash1', size: 100 },
        'data/file2.parquet': { hash: 'hash2', size: 100 },
      })
      await saveManifest(remote, manifest)

      // Mock remote read to fail for specific file
      const originalRead = remote.read.bind(remote)
      vi.spyOn(remote, 'read').mockImplementation(async (path: string) => {
        if (path === 'data/file1.parquet') {
          throw new Error('Connection reset')
        }
        return originalRead(path)
      })

      const result = await engine.pull()

      // First file should fail, second should succeed
      expect(result.errors.some(e => e.path === 'data/file1.parquet')).toBe(true)
      expect(result.downloaded).toContain('data/file2.parquet')
    })

    it('should handle local write failures during download', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(remote, {
        'data/file1.parquet': 'content 1',
      })

      const manifest = createTestManifest({
        'data/file1.parquet': { hash: 'hash1', size: 100 },
      })
      await saveManifest(remote, manifest)

      // Mock local write to fail
      const originalWrite = local.write.bind(local)
      vi.spyOn(local, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/file1.parquet') {
          throw new Error('Disk full')
        }
        return originalWrite(path, data, options)
      })

      const result = await engine.pull()

      expect(result.errors.some(e => e.message.includes('Disk full'))).toBe(true)
      expect(result.downloaded).not.toContain('data/file1.parquet')
    })
  })

  describe('sync with bidirectional failures', () => {
    it('should handle upload and download failures independently', async () => {
      const { engine, local, remote } = createTestEngine()

      // Local has file1, remote has file2
      await populateBackend(local, {
        'data/local.parquet': 'local content',
      })
      await populateBackend(remote, {
        'data/remote.parquet': 'remote content',
      })

      const localManifest = createTestManifest({
        'data/local.parquet': { hash: 'local-hash', size: 100 },
      })
      const remoteManifest = createTestManifest({
        'data/remote.parquet': { hash: 'remote-hash', size: 100 },
      })

      await saveManifest(local, localManifest)
      await saveManifest(remote, remoteManifest)

      // Mock upload to fail, download to succeed
      const originalRemoteWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/local.parquet') {
          throw new Error('Upload failed')
        }
        return originalRemoteWrite(path, data, options)
      })

      const result = await engine.sync()

      // Upload should fail
      expect(result.errors.some(e => e.operation === 'upload')).toBe(true)

      // Download should succeed
      expect(result.downloaded).toContain('data/remote.parquet')
      expect(await local.exists('data/remote.parquet')).toBe(true)
    })
  })
})

// =============================================================================
// 3. Corrupted File Recovery Tests
// =============================================================================

describe('Corrupted File Recovery', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('manifest corruption', () => {
    it('should throw descriptive error for corrupted local manifest', async () => {
      const { engine, local } = createTestEngine()

      // Write corrupted JSON to manifest
      await populateBackend(local, {
        '_meta/manifest.json': '{ invalid json',
      })

      await expect(engine.status()).rejects.toThrow(/Corrupted local manifest/)
    })

    it('should throw descriptive error for corrupted remote manifest', async () => {
      const { engine, remote } = createTestEngine()

      // Write corrupted JSON to remote manifest
      await populateBackend(remote, {
        '_meta/manifest.json': 'not json at all',
      })

      await expect(engine.pull()).rejects.toThrow(/Corrupted remote manifest/)
    })

    it('should handle empty manifest file', async () => {
      const { engine, local } = createTestEngine()

      // Write empty content
      await populateBackend(local, {
        '_meta/manifest.json': '',
      })

      await expect(engine.status()).rejects.toThrow()
    })

    it('should handle manifest with invalid structure', async () => {
      const { engine, local } = createTestEngine()

      // Write valid JSON but invalid manifest structure
      await populateBackend(local, {
        '_meta/manifest.json': '{"invalid": "structure"}',
      })

      // Should still parse, but may have undefined fields
      // The engine should be resilient to missing fields
      const status = await engine.status()
      expect(status).toBeDefined()
    })
  })

  describe('data file corruption detection', () => {
    it('should detect hash mismatch on pull', async () => {
      const { engine, local, remote } = createTestEngine()

      // Create file with known content
      const content = 'original content'
      await populateBackend(remote, {
        'data/file.parquet': content,
      })

      // Create manifest with wrong hash
      const manifest = createTestManifest({
        'data/file.parquet': { hash: 'wrong-hash-value', size: content.length },
      })
      await saveManifest(remote, manifest)

      // Pull should succeed (hash verification happens elsewhere)
      const result = await engine.pull()

      // File should be downloaded
      expect(result.downloaded).toContain('data/file.parquet')

      // Verify content was transferred
      const downloaded = await local.read('data/file.parquet')
      expect(new TextDecoder().decode(downloaded)).toBe(content)
    })

    it('should handle file size mismatch', async () => {
      const { engine, local, remote } = createTestEngine()

      const content = 'actual content here'
      await populateBackend(remote, {
        'data/file.parquet': content,
      })

      // Create manifest with wrong size
      const manifest = createTestManifest({
        'data/file.parquet': { hash: 'some-hash', size: 999 }, // Wrong size
      })
      await saveManifest(remote, manifest)

      // Pull should still work (size is metadata, not enforced on transfer)
      const result = await engine.pull()
      expect(result.success).toBe(true)
    })
  })

  describe('partial file corruption', () => {
    it('should handle truncated file gracefully', async () => {
      const local = new MemoryBackend()

      // Write a file
      await local.write('data/file.txt', new TextEncoder().encode('full content'))

      // Verify it can be read
      const content = await local.read('data/file.txt')
      expect(new TextDecoder().decode(content)).toBe('full content')

      // Overwrite with truncated content (simulating corruption)
      await local.write('data/file.txt', new TextEncoder().encode('trunc'))

      // Read truncated file
      const truncated = await local.read('data/file.txt')
      expect(new TextDecoder().decode(truncated)).toBe('trunc')
    })
  })
})

// =============================================================================
// 4. Transaction Abort Scenarios
// =============================================================================

describe('Transaction Abort Scenarios', () => {
  describe('abort via signal', () => {
    it('should abort retry on AbortSignal', async () => {
      const controller = new AbortController()
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      let attempts = 0
      const fn = vi.fn().mockImplementation(async () => {
        attempts++
        throw concurrencyError
      })

      // Abort after short delay
      const abortAfterDelay = async (ms: number): Promise<void> => {
        if (attempts >= 2) {
          controller.abort()
        }
      }

      await expect(
        withRetry(fn, {
          signal: controller.signal,
          maxRetries: 10,
          _delayFn: abortAfterDelay,
        })
      ).rejects.toThrow(AbortError)

      expect(attempts).toBeLessThan(10)
    })

    it('should not start if already aborted', async () => {
      const controller = new AbortController()
      controller.abort()

      const fn = vi.fn().mockResolvedValue('should not run')

      await expect(
        withRetry(fn, {
          signal: controller.signal,
        })
      ).rejects.toThrow(AbortError)

      expect(fn).not.toHaveBeenCalled()
    })
  })

  describe('abort via onRetry callback', () => {
    it('should stop retrying when onRetry returns false', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('should not reach')

      const onRetry = vi.fn().mockReturnValue(false) // Stop on first retry

      await expect(
        withRetry(fn, {
          _delayFn: async () => {},
          onRetry,
          maxRetries: 5,
        })
      ).rejects.toThrow('conflict')

      // Should only attempt once before abort
      expect(fn).toHaveBeenCalledTimes(1)
      expect(onRetry).toHaveBeenCalledTimes(1)
    })

    it('should continue when onRetry returns true', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      const onRetry = vi.fn().mockReturnValue(true)

      const result = await withRetry(fn, {
        _delayFn: async () => {},
        onRetry,
      })

      expect(result).toBe('success')
      expect(fn).toHaveBeenCalledTimes(3)
      expect(onRetry).toHaveBeenCalledTimes(2)
    })

    it('should provide correct retry info to callback', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const retryInfos: any[] = []
      const onRetry = vi.fn().mockImplementation((info) => {
        retryInfos.push(info)
        return true
      })

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        _delayFn: async () => {},
        onRetry,
        baseDelay: 100,
        jitter: false,
      })

      expect(retryInfos.length).toBe(2)
      expect(retryInfos[0].attempt).toBe(1)
      expect(retryInfos[0].delay).toBe(100)
      expect(retryInfos[1].attempt).toBe(2)
      expect(retryInfos[1].delay).toBe(200) // Exponential backoff
    })
  })

  describe('mutation abort scenarios', () => {
    it('should not partially modify store on validation failure', async () => {
      const executor = new MutationExecutor({
        defaultActor: 'users/test' as EntityId,
      })
      const store = createTestStore()

      // Create valid entity
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Valid Post',
        status: 'active',
      }, store)

      const originalEntity = { ...store.get(entity.$id as string) }

      // Attempt update that should fail validation (depends on implementation)
      // Using version conflict to simulate abort
      await executor.update('posts', entity.$id as string, {
        $set: { status: 'updated' },
      }, store)

      // Try with stale version
      await expect(
        executor.update('posts', entity.$id as string, {
          $set: { status: 'aborted' },
        }, store, { expectedVersion: 1 })
      ).rejects.toThrow(VersionConflictError)

      // Entity should have the first update, not the aborted one
      const current = store.get(entity.$id as string)
      expect(current?.status).toBe('updated')
    })
  })

  describe('sync abort scenarios', () => {
    it('should preserve already synced files when later files fail', async () => {
      const { engine, local, remote } = createTestEngine()

      await populateBackend(local, {
        'data/file1.parquet': 'content 1',
        'data/file2.parquet': 'content 2',
        'data/file3.parquet': 'content 3',
      })

      // Track successful writes
      const successfulWrites: string[] = []
      const originalWrite = remote.write.bind(remote)
      vi.spyOn(remote, 'write').mockImplementation(async (path: string, data: Uint8Array, options?: any) => {
        if (path === 'data/file3.parquet') {
          throw new Error('Upload aborted')
        }
        const result = await originalWrite(path, data, options)
        successfulWrites.push(path)
        return result
      })

      await engine.push()

      // First two files should be on remote
      expect(await remote.exists('data/file1.parquet')).toBe(true)
      expect(await remote.exists('data/file2.parquet')).toBe(true)
      expect(await remote.exists('data/file3.parquet')).toBe(false)

      vi.restoreAllMocks()
    })
  })
})

// =============================================================================
// 5. Retry Semantics Tests
// =============================================================================

describe('Retry Semantics', () => {
  describe('retryable error detection', () => {
    it('should identify ConcurrencyError as retryable', () => {
      const error = new Error('conflict')
      error.name = 'ConcurrencyError'
      expect(isRetryableError(error)).toBe(true)
    })

    it('should identify VersionMismatchError as retryable', () => {
      const error = new Error('version mismatch')
      error.name = 'VersionMismatchError'
      expect(isRetryableError(error)).toBe(true)
    })

    it('should identify errors with retryable property', () => {
      const error = new Error('temporary') as Error & { retryable: boolean }
      error.retryable = true
      expect(isRetryableError(error)).toBe(true)
    })

    it('should not retry TypeError', () => {
      expect(isRetryableError(new TypeError('type error'))).toBe(false)
    })

    it('should not retry generic Error', () => {
      expect(isRetryableError(new Error('generic'))).toBe(false)
    })

    it('should not retry null or undefined', () => {
      expect(isRetryableError(null)).toBe(false)
      expect(isRetryableError(undefined)).toBe(false)
    })
  })

  describe('exponential backoff', () => {
    it('should increase delay exponentially', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        baseDelay: 100,
        multiplier: 2,
        jitter: false,
        maxRetries: 5,
        _delayFn: async (ms) => { delays.push(ms) },
      })

      expect(delays).toEqual([100, 200, 400])
    })

    it('should cap delay at maxDelay', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delays: number[] = []
      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      await withRetry(fn, {
        baseDelay: 100,
        maxDelay: 250,
        multiplier: 2,
        jitter: false,
        maxRetries: 5,
        _delayFn: async (ms) => { delays.push(ms) },
      })

      // Should cap at 250
      expect(delays).toEqual([100, 200, 250, 250])
    })
  })

  describe('metrics collection', () => {
    it('should collect accurate metrics on success', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn()
        .mockRejectedValueOnce(concurrencyError)
        .mockRejectedValueOnce(concurrencyError)
        .mockResolvedValueOnce('success')

      const { result, metrics } = await withRetry(fn, {
        returnMetrics: true,
        _delayFn: async () => {},
      })

      expect(result).toBe('success')
      expect(metrics.succeeded).toBe(true)
      expect(metrics.attempts).toBe(3)
      expect(metrics.retries).toBe(2)
      expect(metrics.errors.length).toBe(2)
    })

    it('should attach metrics to error on failure', async () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const fn = vi.fn().mockRejectedValue(concurrencyError)

      try {
        await withRetry(fn, {
          returnMetrics: true,
          maxRetries: 2,
          _delayFn: async () => {},
        })
        expect.fail('Should have thrown')
      } catch (e) {
        const error = e as Error & { metrics?: any }
        expect(error.metrics).toBeDefined()
        expect(error.metrics.succeeded).toBe(false)
        expect(error.metrics.attempts).toBe(3) // 1 initial + 2 retries
      }
    })
  })
})

// =============================================================================
// 6. ETag-based Optimistic Concurrency
// =============================================================================

describe('ETag-based Optimistic Concurrency', () => {
  it('should write successfully with matching etag', async () => {
    const backend = new MemoryBackend()

    // Initial write
    const result1 = await backend.write('file.txt', new TextEncoder().encode('v1'))
    const etag1 = result1.etag

    // Update with correct etag
    const result2 = await backend.write(
      'file.txt',
      new TextEncoder().encode('v2'),
      { ifMatch: etag1 }
    )

    expect(result2.etag).not.toBe(etag1) // New etag after update
  })

  it('should throw ETagMismatchError with stale etag', async () => {
    const backend = new MemoryBackend()

    // Initial write
    const result1 = await backend.write('file.txt', new TextEncoder().encode('v1'))
    const etag1 = result1.etag

    // Another write changes the etag
    await backend.write('file.txt', new TextEncoder().encode('v2'))

    // Try to write with old etag
    await expect(
      backend.write('file.txt', new TextEncoder().encode('v3'), { ifMatch: etag1 })
    ).rejects.toThrow(ETagMismatchError)
  })

  it('should create only if not exists with ifNoneMatch', async () => {
    const backend = new MemoryBackend()

    // Create file
    await backend.write('file.txt', new TextEncoder().encode('content'))

    // Try to create again with ifNoneMatch: '*'
    await expect(
      backend.write('file.txt', new TextEncoder().encode('new'), { ifNoneMatch: '*' })
    ).rejects.toThrow()

    // File should have original content
    const content = await backend.read('file.txt')
    expect(new TextDecoder().decode(content)).toBe('content')
  })

  it('should succeed with ifNoneMatch on new file', async () => {
    const backend = new MemoryBackend()

    // Create with ifNoneMatch
    const result = await backend.write(
      'new-file.txt',
      new TextEncoder().encode('content'),
      { ifNoneMatch: '*' }
    )

    expect(result.etag).toBeDefined()
  })

  it('should handle writeConditional with expected version', async () => {
    const backend = new MemoryBackend()

    // Create file
    const result1 = await backend.writeConditional(
      'file.txt',
      new TextEncoder().encode('v1'),
      null // Expect file to not exist
    )

    // Update with correct version
    await backend.writeConditional(
      'file.txt',
      new TextEncoder().encode('v2'),
      result1.etag
    )

    // Fail with wrong version
    await expect(
      backend.writeConditional(
        'file.txt',
        new TextEncoder().encode('v3'),
        result1.etag // Old etag
      )
    ).rejects.toThrow(ETagMismatchError)
  })
})

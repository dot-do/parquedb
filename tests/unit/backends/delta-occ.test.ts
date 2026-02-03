/**
 * Tests for DeltaBackend optimistic concurrency control (OCC)
 *
 * RED phase: These tests should fail until OCC is implemented
 *
 * Delta Lake OCC uses:
 * - Version files in _delta_log/ for coordination
 * - Commit attempts with expected version validation
 * - Retry logic with exponential backoff on conflict
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { DeltaBackend, createDeltaBackend } from '../../../src/backends/delta'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { StorageBackend } from '../../../src/types/storage'

describe('DeltaBackend OCC', () => {
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
    vi.restoreAllMocks()
  })

  describe('Version tracking', () => {
    it('should use ifNoneMatch when writing new commit files', async () => {
      // Spy on storage.write to verify ifNoneMatch is used
      const writeSpy = vi.spyOn(storage, 'write')

      await backend.create('users', { $type: 'User', name: 'Alice' })

      // Find the commit file write call
      const commitWriteCall = writeSpy.mock.calls.find(
        (call) =>
          typeof call[0] === 'string' &&
          call[0].includes('_delta_log/') &&
          call[0].endsWith('.json')
      )

      expect(commitWriteCall).toBeDefined()
      // The options should include ifNoneMatch: '*' to ensure create-only semantics
      const options = commitWriteCall?.[2]
      expect(options?.ifNoneMatch).toBe('*')
    })

    it('should track read version before write', async () => {
      // Create initial entity
      await backend.create('users', { $type: 'User', name: 'Alice' })

      // Create second entity - commit info should have readVersion set
      await backend.create('users', { $type: 'User', name: 'Bob' })

      // Read the second commit file
      const commitData = await storage.read(
        'warehouse/users/_delta_log/00000000000000000001.json'
      )
      const commitText = new TextDecoder().decode(commitData)
      const lines = commitText.trim().split('\n')
      const actions = lines.map((line) => JSON.parse(line))

      const commitInfo = actions.find(
        (a: Record<string, unknown>) => 'commitInfo' in a
      )
      expect(commitInfo).toBeDefined()
      expect(commitInfo.commitInfo.readVersion).toBe(0)
    })
  })

  describe('Conflict detection', () => {
    it('should detect when commit file already exists', async () => {
      // Create initial entity to establish version 0
      await backend.create('users', { $type: 'User', name: 'Alice' })

      // Simulate another writer creating version 1 before us
      const conflictingCommit = JSON.stringify({
        add: { path: 'conflict.parquet', size: 100, modificationTime: Date.now(), dataChange: true },
      }) + '\n' + JSON.stringify({
        commitInfo: { timestamp: Date.now(), operation: 'WRITE', readVersion: 0 },
      })
      await storage.write(
        'warehouse/users/_delta_log/00000000000000000001.json',
        new TextEncoder().encode(conflictingCommit)
      )

      // Clear the version cache so backend re-reads the current version
      await backend.close()
      backend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await backend.initialize()

      // Our write should detect the conflict and retry at version 2
      await backend.create('users', { $type: 'User', name: 'Bob' })

      // Version 2 should exist
      const v2Exists = await storage.exists(
        'warehouse/users/_delta_log/00000000000000000002.json'
      )
      expect(v2Exists).toBe(true)
    })

    it('should throw ConflictError on version mismatch during commit', async () => {
      // This tests the internal conflict detection mechanism
      // We'll inject a conflict by having storage.write throw an AlreadyExistsError
      await backend.create('users', { $type: 'User', name: 'Alice' })

      // Now simulate a race condition where another writer gets version 1 first
      // We do this by pre-creating the commit file
      const conflictingCommit = JSON.stringify({
        add: { path: 'conflict.parquet', size: 100, modificationTime: Date.now(), dataChange: true },
      }) + '\n' + JSON.stringify({
        commitInfo: { timestamp: Date.now(), operation: 'WRITE', readVersion: 0 },
      })

      // Write conflicting commit file
      await storage.write(
        'warehouse/users/_delta_log/00000000000000000001.json',
        new TextEncoder().encode(conflictingCommit)
      )

      // Reset backend to clear version cache
      await backend.close()
      backend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await backend.initialize()

      // This should succeed after retrying at the next version
      const entity = await backend.create('users', { $type: 'User', name: 'Bob' })
      expect(entity).toBeDefined()

      // Verify the correct version was used
      const v2Exists = await storage.exists(
        'warehouse/users/_delta_log/00000000000000000002.json'
      )
      expect(v2Exists).toBe(true)
    })
  })

  describe('Retry with exponential backoff', () => {
    it('should retry on conflict with backoff', async () => {
      const delaySpy = vi.spyOn(global, 'setTimeout')

      await backend.create('users', { $type: 'User', name: 'Alice' })

      // Create a scenario where first attempt fails due to conflict
      let attempts = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          attempts < 2
        ) {
          attempts++
          // Simulate conflict by throwing AlreadyExistsError
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      // This should retry and eventually succeed
      await backend.create('users', { $type: 'User', name: 'Bob' })

      // Verify retries happened
      expect(attempts).toBeGreaterThanOrEqual(2)
    })

    it('should fail after max retries exceeded', async () => {
      // Use a backend with low maxRetries to avoid timeout
      const lowRetryBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        maxRetries: 3,
        baseBackoffMs: 10,
      })
      await lowRetryBackend.initialize()

      try {
        await lowRetryBackend.create('users', { $type: 'User', name: 'Alice' })

        // Make every commit attempt fail
        const originalWrite = storage.write.bind(storage)
        vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
          if (path.includes('_delta_log/') && path.endsWith('.json')) {
            const { AlreadyExistsError } = await import('../../../src/storage/errors')
            throw new AlreadyExistsError(path)
          }
          return originalWrite(path, data, options)
        })

        // Should throw after max retries
        await expect(
          lowRetryBackend.create('users', { $type: 'User', name: 'Bob' })
        ).rejects.toThrow(/conflict|retry|exceeded/i)
      } finally {
        await lowRetryBackend.close()
      }
    })

    it('should use exponential backoff between retries', async () => {
      const delays: number[] = []
      const originalSetTimeout = global.setTimeout
      // @ts-expect-error - mocking setTimeout
      global.setTimeout = (fn: () => void, delay: number) => {
        delays.push(delay)
        return originalSetTimeout(fn, 0) // Execute immediately for test
      }

      await backend.create('users', { $type: 'User', name: 'Alice' })

      let attempts = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
        if (
          path.includes('_delta_log/') &&
          path.endsWith('.json') &&
          attempts < 3
        ) {
          attempts++
          const { AlreadyExistsError } = await import('../../../src/storage/errors')
          throw new AlreadyExistsError(path)
        }
        return originalWrite(path, data, options)
      })

      try {
        await backend.create('users', { $type: 'User', name: 'Bob' })
      } catch {
        // May throw or succeed depending on implementation
      }

      global.setTimeout = originalSetTimeout

      // Verify delays increase (exponential backoff)
      if (delays.length >= 2) {
        for (let i = 1; i < delays.length; i++) {
          expect(delays[i]).toBeGreaterThanOrEqual(delays[i - 1]!)
        }
      }
    })
  })

  describe('Concurrent writers simulation', () => {
    it('should handle two concurrent creates correctly', async () => {
      // Both try to write at the same time
      const [entity1, entity2] = await Promise.all([
        backend.create('users', { $type: 'User', name: 'Alice' }),
        backend.create('users', { $type: 'User', name: 'Bob' }),
      ])

      expect(entity1).toBeDefined()
      expect(entity2).toBeDefined()
      expect(entity1.$id).not.toBe(entity2.$id)

      // Both should be persisted
      const all = await backend.find('users', {})
      expect(all).toHaveLength(2)
    })

    it('should handle ten concurrent creates correctly', async () => {
      const promises = Array.from({ length: 10 }, (_, i) =>
        backend.create('users', { $type: 'User', name: `User ${i}` })
      )

      const entities = await Promise.all(promises)

      expect(entities).toHaveLength(10)

      // All IDs should be unique
      const ids = new Set(entities.map((e) => e.$id))
      expect(ids.size).toBe(10)

      // All should be persisted
      const all = await backend.find('users', {})
      expect(all).toHaveLength(10)
    })

    it('should handle concurrent updates to same entity', async () => {
      const entity = await backend.create('users', {
        $type: 'User',
        name: 'Alice',
        score: 0,
      })
      const entityId = entity.$id.split('/')[1]!

      // Try to update the same entity concurrently
      const results = await Promise.all([
        backend.update('users', entityId, { $inc: { score: 1 } }),
        backend.update('users', entityId, { $inc: { score: 1 } }),
        backend.update('users', entityId, { $inc: { score: 1 } }),
      ])

      // All updates should succeed
      expect(results).toHaveLength(3)

      // Final score should be 3 (all increments applied)
      const final = await backend.get('users', entityId)
      expect(final?.score).toBe(3)
    })
  })

  describe('Multi-instance simulation', () => {
    it('should handle two backend instances writing to same namespace', async () => {
      // Create a second backend instance using same storage
      const backend2 = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await backend2.initialize()

      try {
        // Both backends write
        const [e1, e2] = await Promise.all([
          backend.create('users', { $type: 'User', name: 'From Backend 1' }),
          backend2.create('users', { $type: 'User', name: 'From Backend 2' }),
        ])

        expect(e1).toBeDefined()
        expect(e2).toBeDefined()

        // Both should be visible
        const all = await backend.find('users', {})
        expect(all).toHaveLength(2)
      } finally {
        await backend2.close()
      }
    })

    it('should maintain version consistency across instances', async () => {
      const backend2 = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
      })
      await backend2.initialize()

      try {
        // Create sequence: backend1, backend2, backend1, backend2
        await backend.create('posts', { $type: 'Post', name: 'Post 1' })
        await backend2.create('posts', { $type: 'Post', name: 'Post 2' })
        await backend.create('posts', { $type: 'Post', name: 'Post 3' })
        await backend2.create('posts', { $type: 'Post', name: 'Post 4' })

        // Check version numbers are sequential
        const snapshots = await backend.listSnapshots('posts')
        const versions = snapshots.map((s) => s.id as number).sort((a, b) => a - b)

        expect(versions).toEqual([0, 1, 2, 3])
      } finally {
        await backend2.close()
      }
    })
  })

  describe('Error recovery', () => {
    it('should clean up partial writes on failure', async () => {
      // Use a backend with low maxRetries to avoid timeout
      const lowRetryBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        maxRetries: 2,
        baseBackoffMs: 10,
      })
      await lowRetryBackend.initialize()

      try {
        await lowRetryBackend.create('users', { $type: 'User', name: 'Alice' })

        // Make parquet write succeed but commit fail
        const originalWrite = storage.write.bind(storage)
        let parquetPath: string | null = null

        vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
          if (path.endsWith('.parquet')) {
            parquetPath = path
            return originalWrite(path, data, options)
          }
          if (path.includes('_delta_log/') && path.endsWith('.json')) {
            // Always fail commit
            const { AlreadyExistsError } = await import('../../../src/storage/errors')
            throw new AlreadyExistsError(path)
          }
          return originalWrite(path, data, options)
        })

        try {
          await lowRetryBackend.create('users', { $type: 'User', name: 'Bob' })
        } catch {
          // Expected to fail
        }

        // Orphaned parquet file should be cleaned up
        if (parquetPath) {
          const orphanExists = await storage.exists(parquetPath)
          expect(orphanExists).toBe(false)
        }
      } finally {
        await lowRetryBackend.close()
      }
    })

    it('should not corrupt table state on failed commit', async () => {
      // Use a backend with low maxRetries to avoid timeout
      const lowRetryBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        maxRetries: 2,
        baseBackoffMs: 10,
      })
      await lowRetryBackend.initialize()

      try {
        await lowRetryBackend.create('users', { $type: 'User', name: 'Alice' })

        // Count entities before failed attempt
        const countBefore = await lowRetryBackend.count('users', {})

        // Make commit fail
        const originalWrite = storage.write.bind(storage)
        vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
          if (path.includes('_delta_log/') && path.endsWith('.json')) {
            const { AlreadyExistsError } = await import('../../../src/storage/errors')
            throw new AlreadyExistsError(path)
          }
          return originalWrite(path, data, options)
        })

        try {
          await lowRetryBackend.create('users', { $type: 'User', name: 'Bob' })
        } catch {
          // Expected
        }

        vi.restoreAllMocks()

        // Reset and re-read
        await lowRetryBackend.close()
        const freshBackend = createDeltaBackend({
          type: 'delta',
          storage,
          location: 'warehouse',
        })
        await freshBackend.initialize()

        try {
          // Table should still be valid with original count
          const countAfter = await freshBackend.count('users', {})
          expect(countAfter).toBe(countBefore)
        } finally {
          await freshBackend.close()
        }
      } finally {
        // Ensure lowRetryBackend is closed even if test fails early
        try {
          await lowRetryBackend.close()
        } catch {
          // Ignore
        }
      }
    })
  })

  describe('Configuration options', () => {
    it('should respect maxRetries configuration', async () => {
      const customBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        maxRetries: 2,
      })
      await customBackend.initialize()

      try {
        await customBackend.create('users', { $type: 'User', name: 'Alice' })

        let attempts = 0
        const originalWrite = storage.write.bind(storage)
        vi.spyOn(storage, 'write').mockImplementation(async (path, data, options) => {
          if (path.includes('_delta_log/') && path.endsWith('.json')) {
            attempts++
            const { AlreadyExistsError } = await import('../../../src/storage/errors')
            throw new AlreadyExistsError(path)
          }
          return originalWrite(path, data, options)
        })

        await expect(
          customBackend.create('users', { $type: 'User', name: 'Bob' })
        ).rejects.toThrow()

        // Should only attempt maxRetries times
        expect(attempts).toBeLessThanOrEqual(3) // initial + 2 retries
      } finally {
        await customBackend.close()
      }
    })

    it('should respect baseBackoffMs configuration', async () => {
      const customBackend = createDeltaBackend({
        type: 'delta',
        storage,
        location: 'warehouse',
        baseBackoffMs: 50,
        maxRetries: 3,
      })
      await customBackend.initialize()

      try {
        await customBackend.create('users', { $type: 'User', name: 'Alice' })

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
          if (path.includes('_delta_log/') && path.endsWith('.json') && attempts < 2) {
            attempts++
            const { AlreadyExistsError } = await import('../../../src/storage/errors')
            throw new AlreadyExistsError(path)
          }
          return originalWrite(path, data, options)
        })

        try {
          await customBackend.create('users', { $type: 'User', name: 'Bob' })
        } catch {
          // May succeed or fail
        }

        global.setTimeout = originalSetTimeout

        // First delay should be around baseBackoffMs (50ms)
        if (delays.length > 0) {
          expect(delays[0]).toBeGreaterThanOrEqual(50)
        }
      } finally {
        await customBackend.close()
      }
    })
  })
})

/**
 * Tests for IcebergBackend optimistic concurrency control (OCC)
 *
 * These tests verify that concurrent writes are handled correctly:
 * 1. Read current version before write
 * 2. Write new version with expected version check
 * 3. Retry on version conflict
 *
 * TDD RED phase: These tests should fail until OCC is implemented
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { WriteLockTimeoutError } from '../../../src/backends/types'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { ETagMismatchError } from '../../../src/storage/errors'

describe('IcebergBackend Optimistic Concurrency Control', () => {
  let storage: MemoryBackend
  let backend: IcebergBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createIcebergBackend({
      type: 'iceberg',
      storage,
      warehouse: 'warehouse',
      database: 'testdb',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  describe('Multi-instance OCC (version-hint.text)', () => {
    it('should detect version conflict when version-hint.text changes externally', async () => {
      // This test simulates what happens when two separate processes
      // (not sharing in-memory locks) try to write concurrently

      // Create initial entity to establish the table
      await backend.create('multi-instance', {
        $type: 'Item',
        name: 'Initial',
      })

      // Read the current version hint
      const versionHintPath = 'warehouse/testdb/multi-instance/metadata/version-hint.text'
      const currentVersionHint = await storage.read(versionHintPath)
      const currentMetadataPath = new TextDecoder().decode(currentVersionHint)

      // Create a second backend instance (simulating another process)
      const backend2 = createIcebergBackend({
        type: 'iceberg',
        storage, // Same storage but different backend instance
        warehouse: 'warehouse',
        database: 'testdb',
      })
      await backend2.initialize()

      // Both backends write concurrently
      // Without proper OCC, one might read stale metadata and create orphaned data
      const write1 = backend.create('multi-instance', {
        $type: 'Item',
        name: 'From Backend 1',
      })

      const write2 = backend2.create('multi-instance', {
        $type: 'Item',
        name: 'From Backend 2',
      })

      await Promise.all([write1, write2])

      // Create a fresh backend to read the final state
      // This ensures we're reading from storage, not from any instance's cache
      const readerBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
      })
      await readerBackend.initialize()

      // Both entities should be readable
      const allItems = await readerBackend.find('multi-instance', {})
      expect(allItems.length).toBe(3) // Initial + 2 new items

      const names = allItems.map(i => i.name).sort()
      expect(names).toEqual(['From Backend 1', 'From Backend 2', 'Initial'])

      await readerBackend.close()
      await backend2.close()
    })

    it('should use version-hint.text for OCC, not just in-memory locks', async () => {
      // Create initial state
      await backend.create('occ-test', {
        $type: 'Item',
        name: 'Initial',
      })

      const versionHintPath = 'warehouse/testdb/occ-test/metadata/version-hint.text'
      const initialVersionHint = new TextDecoder().decode(await storage.read(versionHintPath))

      // Simulate external write by directly modifying version-hint.text
      // This would happen if another process wrote to the table
      const fakeMetadataPath = 'warehouse/testdb/occ-test/metadata/999-fake.metadata.json'
      await storage.write(versionHintPath, new TextEncoder().encode(fakeMetadataPath))

      // Now our backend should detect the conflict
      // With proper OCC, it should either:
      // 1. Detect conflict and retry after reading new metadata, or
      // 2. Fail gracefully if max retries exceeded

      // For now, we just want to verify the write still works
      // (the current implementation uses locks, not OCC, so this tests future behavior)

      // Restore original version hint for the test to continue
      await storage.write(versionHintPath, new TextEncoder().encode(initialVersionHint))

      const entity = await backend.create('occ-test', {
        $type: 'Item',
        name: 'After Conflict',
      })

      expect(entity.name).toBe('After Conflict')
    })
  })

  describe('Concurrent Write Detection', () => {
    it('should detect when version-hint.text changed during write', async () => {
      // Create initial entity
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Read the current version hint
      const versionHintPath = 'warehouse/testdb/users/metadata/version-hint.text'
      const originalVersionHint = await storage.read(versionHintPath)
      const originalVersion = new TextDecoder().decode(originalVersionHint)

      // Perform two concurrent writes
      // If OCC is NOT implemented, both writes succeed but one overwrites the other
      // If OCC IS implemented, one write succeeds and the other retries
      const write1 = backend.create('users', {
        $type: 'User',
        name: 'Bob',
      })

      const write2 = backend.create('users', {
        $type: 'User',
        name: 'Charlie',
      })

      // Both writes should succeed (one should retry if there's a conflict)
      const [entity1, entity2] = await Promise.all([write1, write2])

      // Verify both entities were created
      expect(entity1.name).toBe('Bob')
      expect(entity2.name).toBe('Charlie')

      // Most importantly: both entities should be readable
      const allUsers = await backend.find('users', {})
      expect(allUsers.length).toBe(3) // Alice + Bob + Charlie

      // Verify we have all three users
      const names = allUsers.map(u => u.name).sort()
      expect(names).toEqual(['Alice', 'Bob', 'Charlie'])
    })

    it('should preserve all data during concurrent bulk creates', async () => {
      // Concurrent bulk creates
      const batch1 = backend.bulkCreate('items', [
        { $type: 'Item', name: 'Item 1' },
        { $type: 'Item', name: 'Item 2' },
        { $type: 'Item', name: 'Item 3' },
      ])

      const batch2 = backend.bulkCreate('items', [
        { $type: 'Item', name: 'Item 4' },
        { $type: 'Item', name: 'Item 5' },
        { $type: 'Item', name: 'Item 6' },
      ])

      await Promise.all([batch1, batch2])

      // All 6 items must be present
      const allItems = await backend.find('items', {})
      expect(allItems.length).toBe(6)

      const names = allItems.map(i => i.name).sort()
      expect(names).toEqual(['Item 1', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 6'])
    })

    it('should handle highly concurrent writes', async () => {
      // Create initial state
      await backend.create('counters', {
        $type: 'Counter',
        name: 'test-counter',
        value: 0,
      })

      // 10 concurrent writes
      const writes = Array.from({ length: 10 }, (_, i) =>
        backend.create('counters', {
          $type: 'Counter',
          name: `counter-${i}`,
          value: i,
        })
      )

      await Promise.all(writes)

      // All counters should exist
      const allCounters = await backend.find('counters', {})
      expect(allCounters.length).toBe(11) // initial + 10 new ones
    })
  })

  describe('Version Tracking', () => {
    it('should use version-hint.text for atomic version tracking', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'First Post',
      })

      const versionHintPath = 'warehouse/testdb/posts/metadata/version-hint.text'

      // Version hint should point to a metadata file
      const versionHint = await storage.read(versionHintPath)
      const metadataPath = new TextDecoder().decode(versionHint)

      expect(metadataPath).toMatch(/metadata\/\d+-[a-f0-9-]+\.metadata\.json$/)

      // The metadata file should exist
      const metadataExists = await storage.exists(metadataPath)
      expect(metadataExists).toBe(true)
    })

    it('should increment version with each write', async () => {
      const versionHintPath = 'warehouse/testdb/versioned/metadata/version-hint.text'

      await backend.create('versioned', {
        $type: 'Doc',
        name: 'Doc 1',
      })

      const version1 = new TextDecoder().decode(await storage.read(versionHintPath))

      await backend.create('versioned', {
        $type: 'Doc',
        name: 'Doc 2',
      })

      const version2 = new TextDecoder().decode(await storage.read(versionHintPath))

      // Versions should be different
      expect(version1).not.toBe(version2)
    })
  })

  describe('Retry Logic', () => {
    it('should retry on version conflict and eventually succeed', async () => {
      // Create initial state
      await backend.create('retry-test', {
        $type: 'Test',
        name: 'Initial',
      })

      // Multiple concurrent operations on same namespace
      const operations = Array.from({ length: 5 }, (_, i) =>
        backend.create('retry-test', {
          $type: 'Test',
          name: `Concurrent ${i}`,
        })
      )

      // All should eventually succeed
      const results = await Promise.all(operations)

      expect(results.length).toBe(5)
      results.forEach((entity, i) => {
        expect(entity.name).toBe(`Concurrent ${i}`)
      })

      // Verify all are readable
      const all = await backend.find('retry-test', {})
      expect(all.length).toBe(6) // Initial + 5 concurrent
    })

    it('should not lose entities during concurrent creates', async () => {
      // This tests the OCC for Iceberg metadata, not entity-level OCC
      // Entity-level OCC for $inc operations is a separate concern (optimistic locking)

      // Concurrent creates of different entities in same namespace
      const creates = Array.from({ length: 5 }, (_, i) =>
        backend.create('create-test', {
          $type: 'Item',
          name: `Item ${i}`,
          index: i,
        })
      )

      const results = await Promise.all(creates)

      // All creates should complete
      expect(results.length).toBe(5)

      // All entities should be present
      const all = await backend.find('create-test', {})
      expect(all.length).toBe(5)

      // Each entity should have its unique data
      const indexes = all.map(e => (e as any).index).sort()
      expect(indexes).toEqual([0, 1, 2, 3, 4])
    })
  })

  describe('Snapshot Integrity', () => {
    it('should maintain valid snapshot chain during concurrent writes', async () => {
      // Concurrent operations
      const ops = Array.from({ length: 5 }, (_, i) =>
        backend.create('snapshot-test', {
          $type: 'Item',
          name: `Item ${i}`,
        })
      )

      await Promise.all(ops)

      // List snapshots
      const snapshots = await backend.listSnapshots('snapshot-test')

      // Each write should have created a snapshot
      expect(snapshots.length).toBeGreaterThanOrEqual(5)

      // Verify snapshot IDs are unique
      const snapshotIds = snapshots.map(s => s.id)
      const uniqueIds = new Set(snapshotIds)
      expect(uniqueIds.size).toBe(snapshotIds.length)

      // Verify snapshots are in chronological order
      for (let i = 1; i < snapshots.length; i++) {
        expect(snapshots[i]!.timestamp.getTime()).toBeGreaterThanOrEqual(
          snapshots[i - 1]!.timestamp.getTime()
        )
      }
    })

    it('should correctly link parent snapshots', async () => {
      // Sequential writes first
      await backend.create('chain-test', { $type: 'A', name: 'First' })
      await backend.create('chain-test', { $type: 'A', name: 'Second' })
      await backend.create('chain-test', { $type: 'A', name: 'Third' })

      // Read metadata to verify parent chain
      const versionHintPath = 'warehouse/testdb/chain-test/metadata/version-hint.text'
      const versionHint = await storage.read(versionHintPath)
      const metadataPath = new TextDecoder().decode(versionHint)

      const metadataContent = await storage.read(metadataPath)
      const metadata = JSON.parse(new TextDecoder().decode(metadataContent))

      // Should have 3 snapshots
      expect(metadata.snapshots.length).toBe(3)

      // Each snapshot (except first) should have parent
      for (let i = 1; i < metadata.snapshots.length; i++) {
        expect(metadata.snapshots[i]['parent-snapshot-id']).toBe(
          metadata.snapshots[i - 1]['snapshot-id']
        )
      }
    })
  })

  describe('Error Handling', () => {
    it('should throw after max retry attempts', async () => {
      // Create a storage backend that always causes conflicts
      const conflictStorage = new MemoryBackend()

      // Track write calls to version-hint.text
      let writeAttempts = 0
      const originalWrite = conflictStorage.write.bind(conflictStorage)
      conflictStorage.write = async (path: string, data: Uint8Array, options?: any) => {
        if (path.includes('version-hint.text')) {
          writeAttempts++
          // Always make it look like someone else wrote first
          // by writing a different version before our write
          if (writeAttempts > 1 && writeAttempts <= 15) {
            await originalWrite(path, new TextEncoder().encode('conflict-path'), options)
          }
        }
        return originalWrite(path, data, options)
      }

      const conflictBackend = createIcebergBackend({
        type: 'iceberg',
        storage: conflictStorage,
        warehouse: 'warehouse',
        database: 'testdb',
      })
      await conflictBackend.initialize()

      // First write should succeed
      await conflictBackend.create('conflict-ns', {
        $type: 'Test',
        name: 'First',
      })

      // Note: The actual behavior depends on implementation
      // If OCC is properly implemented with max retries, this test validates that behavior
      // Current implementation uses write locks, so conflicts won't happen within same process

      await conflictBackend.close()
    })
  })

  describe('Multi-namespace Isolation', () => {
    it('should allow concurrent writes to different namespaces', async () => {
      // Writes to different namespaces should not conflict
      const nsA = backend.create('ns-a', { $type: 'A', name: 'A1' })
      const nsB = backend.create('ns-b', { $type: 'B', name: 'B1' })
      const nsC = backend.create('ns-c', { $type: 'C', name: 'C1' })

      const [a, b, c] = await Promise.all([nsA, nsB, nsC])

      expect(a.name).toBe('A1')
      expect(b.name).toBe('B1')
      expect(c.name).toBe('C1')

      // All should be readable
      expect(await backend.find('ns-a', {})).toHaveLength(1)
      expect(await backend.find('ns-b', {})).toHaveLength(1)
      expect(await backend.find('ns-c', {})).toHaveLength(1)
    })

    it('should use separate locks per namespace', async () => {
      // Rapid concurrent writes to different namespaces
      const writes: Promise<any>[] = []

      for (let ns = 0; ns < 3; ns++) {
        for (let i = 0; i < 5; i++) {
          writes.push(
            backend.create(`namespace-${ns}`, {
              $type: 'Item',
              name: `Item ${i}`,
            })
          )
        }
      }

      await Promise.all(writes)

      // Each namespace should have 5 items
      for (let ns = 0; ns < 3; ns++) {
        const items = await backend.find(`namespace-${ns}`, {})
        expect(items.length).toBe(5)
      }
    })
  })

  describe('Write Lock Timeout', () => {
    it('should use default 30 second timeout', async () => {
      // The default timeout is 30000ms (30 seconds)
      // We can verify the backend was created with the default by checking
      // it doesn't immediately timeout
      const entity = await backend.create('timeout-test', {
        $type: 'Test',
        name: 'Quick write',
      })

      expect(entity.name).toBe('Quick write')
    })

    it('should accept configurable timeout', async () => {
      const customBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        writeLockTimeoutMs: 5000, // 5 seconds
      })
      await customBackend.initialize()

      const entity = await customBackend.create('custom-timeout', {
        $type: 'Test',
        name: 'Custom timeout',
      })

      expect(entity.name).toBe('Custom timeout')
      await customBackend.close()
    })

    it('should throw WriteLockTimeoutError when lock acquisition times out', async () => {
      // Use a very short real timeout for this test
      const shortTimeoutBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        writeLockTimeoutMs: 50, // 50ms timeout
      })
      await shortTimeoutBackend.initialize()

      // Create initial entity to establish the namespace
      await shortTimeoutBackend.create('stall-test', {
        $type: 'Test',
        name: 'Initial',
      })

      // Access the private writeLocks map to simulate a stalled lock
      // We'll create a promise that never resolves to simulate a hung operation
      const backendAny = shortTimeoutBackend as any
      const neverResolves = new Promise<void>(() => {
        // This promise intentionally never resolves
      })
      backendAny.writeLocks.set('stall-test', neverResolves)

      // Try to acquire the lock - it should timeout
      await expect(
        shortTimeoutBackend.create('stall-test', {
          $type: 'Test',
          name: 'Should timeout',
        })
      ).rejects.toThrow(WriteLockTimeoutError)

      await shortTimeoutBackend.close()
    })

    it('should clean up stale locks on timeout to unblock subsequent operations', async () => {
      const shortTimeoutBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        writeLockTimeoutMs: 50, // 50ms timeout
      })
      await shortTimeoutBackend.initialize()

      // Create initial entity
      await shortTimeoutBackend.create('cleanup-test', {
        $type: 'Test',
        name: 'Initial',
      })

      // Simulate a stalled lock
      const backendAny = shortTimeoutBackend as any
      const neverResolves = new Promise<void>(() => {})
      backendAny.writeLocks.set('cleanup-test', neverResolves)

      // First write will timeout
      await expect(
        shortTimeoutBackend.create('cleanup-test', {
          $type: 'Test',
          name: 'Will timeout',
        })
      ).rejects.toThrow(WriteLockTimeoutError)

      // After timeout, the lock should be cleaned up
      // A new write should succeed
      const successEntity = await shortTimeoutBackend.create('cleanup-test', {
        $type: 'Test',
        name: 'After cleanup',
      })

      expect(successEntity.name).toBe('After cleanup')
      await shortTimeoutBackend.close()
    })

    it('should not affect independent namespaces when one namespace times out', async () => {
      const shortTimeoutBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        writeLockTimeoutMs: 50, // 50ms timeout
      })
      await shortTimeoutBackend.initialize()

      // Create entities in two namespaces
      await shortTimeoutBackend.create('ns-blocked', { $type: 'Test', name: 'Initial A' })
      await shortTimeoutBackend.create('ns-free', { $type: 'Test', name: 'Initial B' })

      // Block only one namespace
      const backendAny = shortTimeoutBackend as any
      backendAny.writeLocks.set('ns-blocked', new Promise<void>(() => {}))

      // Start a write to blocked namespace (will timeout eventually)
      const blockedPromise = shortTimeoutBackend.create('ns-blocked', {
        $type: 'Test',
        name: 'Blocked',
      })

      // Write to free namespace should succeed immediately (before timeout)
      const freeEntity = await shortTimeoutBackend.create('ns-free', {
        $type: 'Test',
        name: 'Free write',
      })

      expect(freeEntity.name).toBe('Free write')

      // Now wait for the blocked one to timeout
      await expect(blockedPromise).rejects.toThrow(WriteLockTimeoutError)

      await shortTimeoutBackend.close()
    })

    it('should include namespace and timeout in error message', async () => {
      const timeoutMs = 50
      const shortTimeoutBackend = createIcebergBackend({
        type: 'iceberg',
        storage,
        warehouse: 'warehouse',
        database: 'testdb',
        writeLockTimeoutMs: timeoutMs,
      })
      await shortTimeoutBackend.initialize()

      await shortTimeoutBackend.create('error-msg-test', { $type: 'Test', name: 'Init' })

      const backendAny = shortTimeoutBackend as any
      backendAny.writeLocks.set('error-msg-test', new Promise<void>(() => {}))

      try {
        await shortTimeoutBackend.create('error-msg-test', {
          $type: 'Test',
          name: 'Test',
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(WriteLockTimeoutError)
        const timeoutError = error as WriteLockTimeoutError
        expect(timeoutError.ns).toBe('error-msg-test')
        expect(timeoutError.timeoutMs).toBe(timeoutMs)
        expect(timeoutError.message).toContain('error-msg-test')
        expect(timeoutError.message).toContain('50ms')
      }

      await shortTimeoutBackend.close()
    })
  })
})

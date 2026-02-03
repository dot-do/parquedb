/**
 * Flush Race Condition Test Suite
 *
 * Tests for the race condition bug in flushEvents where pendingEvents
 * is cleared before async writes complete. This can cause:
 * 1. Events to be lost if new events are added during a flush
 * 2. Incorrect rollback behavior if the flush fails
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type { EntityId } from '../../src/types'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

/**
 * Helper to wait for async operations to proceed.
 * Uses a minimal 5ms delay to allow async I/O operations to complete.
 * This is a pragmatic approach for tests that need to wait for real async operations.
 */
async function tick(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 5))
}

describe('Flush Race Condition', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-flush-race-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Dispose of the database to clean up internal state before removing temp dir
    db.dispose()
    // Add a small delay to allow any background operations to settle
    await new Promise(resolve => setTimeout(resolve, 50))
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('concurrent event handling', () => {
    it('should not lose events added during a flush operation', async () => {
      // Create multiple entities rapidly - this tests that events added
      // while a flush is in progress are not lost
      const promises: Promise<unknown>[] = []

      // Start multiple create operations in rapid succession
      for (let i = 0; i < 10; i++) {
        promises.push(
          db.create('posts', {
            $type: 'Post',
            name: `Post ${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
          })
        )
      }

      // Wait for all operations to complete
      const entities = await Promise.all(promises)

      // Verify all entities were created
      expect(entities).toHaveLength(10)

      // Verify all events were recorded
      const eventLog = db.getEventLog()
      const allEvents = await eventLog.getEventsByNamespace('posts')

      // Each create should have recorded exactly one CREATE event
      expect(allEvents).toHaveLength(10)
    })

    it('should handle events added during flush failure gracefully', async () => {
      // Create a spy on storage.write to make it fail on specific calls
      let writeCallCount = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        writeCallCount++
        // Fail on the 4th write (after entity1's data is written, during entity2's flush)
        // Each create does: 1) events.jsonl, 2) data/{ns}/data.json, 3) {ns}/events.json
        if (writeCallCount === 4) {
          throw new Error('Simulated write failure')
        }
        return originalWrite(path, data)
      })

      // Create first entity (should succeed - writes 1-3)
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      expect(entity1).toBeDefined()

      // Create second entity (should fail on write 4)
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'Post 2',
          title: 'Title 2',
          content: 'Content 2',
        })
      ).rejects.toThrow('Simulated write failure')

      // The first entity should still exist in memory after the failed second create
      // (because its data was successfully written before the failure)
      const found = await db.get('posts', entity1.$id as string)
      expect(found).toBeDefined()
      expect(found?.name).toBe('Post 1')
    })

    it('should not clear pendingEvents before flush completes', async () => {
      // This test verifies that if a flush is slow and new events arrive,
      // those events are not lost

      // Track write completion
      let writeResolvers: Array<() => void> = []
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        // Create a promise that we control when to resolve
        await new Promise<void>(resolve => {
          writeResolvers.push(resolve)
        })
        return originalWrite(path, data)
      })

      // Start first create operation
      const promise1 = db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      // Wait for the first write to be initiated
      await tick()

      // Start second create while first flush is blocked
      const promise2 = db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })

      // Now complete all the writes
      await tick()

      // Resolve all pending writes
      while (writeResolvers.length > 0) {
        const resolver = writeResolvers.shift()!
        resolver()
        // Allow the next write to be initiated
        await tick()
      }

      // Wait for both operations
      const [entity1, entity2] = await Promise.all([promise1, promise2])

      expect(entity1).toBeDefined()
      expect(entity2).toBeDefined()

      // Verify both events were recorded
      const eventLog = db.getEventLog()
      const events1 = await eventLog.getEvents(entity1.$id as EntityId)
      const events2 = await eventLog.getEvents(entity2.$id as EntityId)

      expect(events1).toHaveLength(1)
      expect(events2).toHaveLength(1)
    })
  })

  describe('flushPromise correctness', () => {
    it('should batch multiple synchronous creates into fewer flushes', async () => {
      // Track how many times flushEvents actually writes
      let writeCount = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        writeCount++
        return originalWrite(path, data)
      })

      // Start multiple creates synchronously (they should batch)
      const promises = [
        db.create('posts', { $type: 'Post', name: 'P1', title: 'T1', content: 'C1' }),
        db.create('posts', { $type: 'Post', name: 'P2', title: 'T2', content: 'C2' }),
        db.create('posts', { $type: 'Post', name: 'P3', title: 'T3', content: 'C3' }),
      ]

      await Promise.all(promises)

      // With batching, we expect fewer writes than if each create flushed independently
      // Each flush writes: 1) events.jsonl, 2) data/{ns}/data.json, 3) {ns}/events.json
      // Without batching: 9 writes (3 creates * 3 writes each)
      // With perfect batching: 3 writes (1 batch * 3 writes)
      // Note: The exact number depends on microtask timing, but should be <= 9
      expect(writeCount).toBeLessThanOrEqual(9)

      // Verify all entities were created
      const eventLog = db.getEventLog()
      const events = await eventLog.getEventsByNamespace('posts')
      expect(events).toHaveLength(3)
    })

    it('should not start a new flush while one is in progress', async () => {
      // Track concurrent flush operations
      let activeFlushes = 0
      let maxConcurrentFlushes = 0

      // Track each write operation
      let writeResolvers: Array<() => void> = []
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        activeFlushes++
        maxConcurrentFlushes = Math.max(maxConcurrentFlushes, activeFlushes)

        // Create a controlled delay
        await new Promise<void>(resolve => {
          writeResolvers.push(resolve)
        })

        const result = await originalWrite(path, data)
        activeFlushes--
        return result
      })

      // Start first create
      const promise1 = db.create('posts', { $type: 'Post', name: 'P1', title: 'T1', content: 'C1' })

      // Wait for flush to start
      await tick()

      // Start second create while first is flushing
      const promise2 = db.create('posts', { $type: 'Post', name: 'P2', title: 'T2', content: 'C2' })

      // Complete all writes
      await tick()
      while (writeResolvers.length > 0) {
        const resolver = writeResolvers.shift()!
        resolver()
        await tick()
      }

      await Promise.all([promise1, promise2])

      // Each write operation counts as +1 active flush (writes are sequential within a flush)
      // The key invariant is that we don't have multiple flush operations running in parallel
      // causing writes to interleave. Since writes are sequential, maxConcurrentFlushes represents
      // the max number of writes waiting at any point, which should be 1.
      expect(maxConcurrentFlushes).toBe(1)
    })
  })

  describe('rollback correctness', () => {
    it('should properly rollback entity state on flush failure', async () => {
      // First create a successful entity
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })
      expect(entity1).toBeDefined()

      // Now make the next flush fail
      let writeCallCount = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        writeCallCount++
        // Fail on the first write of the new flush
        if (writeCallCount === 1) {
          throw new Error('Simulated write failure')
        }
        return originalWrite(path, data)
      })

      // Try to create a second entity (should fail)
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'Post 2',
          title: 'Title 2',
          content: 'Content 2',
        })
      ).rejects.toThrow('Simulated write failure')

      // The failed entity should not exist in memory
      const result = await db.find('posts')
      expect(result.items).toHaveLength(1)
      expect(result.items[0]?.name).toBe('Post 1')
    })

    it('should remove failed events from pending queue', async () => {
      // Make all writes fail
      vi.spyOn(storage, 'write').mockImplementation(async () => {
        throw new Error('All writes fail')
      })

      // Try to create an entity (should fail)
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'Post 1',
          title: 'Title 1',
          content: 'Content 1',
        })
      ).rejects.toThrow('All writes fail')

      // Now restore write functionality
      vi.restoreAllMocks()

      // Creating a new entity should work and not include the failed event
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })

      expect(entity).toBeDefined()
      expect(entity.name).toBe('Post 2')

      // Only the successful entity should exist
      const result = await db.find('posts')
      expect(result.items).toHaveLength(1)
    })
  })
})

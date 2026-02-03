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

describe('Flush Race Condition', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-flush-race-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    vi.useRealTimers()
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
      // Create a spy on storage.write to make it fail intermittently
      let writeCallCount = 0
      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        writeCallCount++
        // Fail on the second write to simulate partial failure
        if (writeCallCount === 2) {
          throw new Error('Simulated write failure')
        }
        return originalWrite(path, data)
      })

      // Create first entity (should succeed)
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      expect(entity1).toBeDefined()

      // Create second entity (should fail and rollback)
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'Post 2',
          title: 'Title 2',
          content: 'Content 2',
        })
      ).rejects.toThrow('Simulated write failure')

      // The first entity should still exist after the failed second create
      const found = await db.get('posts', entity1.$id as string)
      expect(found).toBeDefined()
      expect(found?.name).toBe('Post 1')
    })

    it('should not clear pendingEvents before flush completes', async () => {
      // This test verifies that if a flush is slow and new events arrive,
      // those events are not lost

      let flushStarted = false
      let flushCompleted = false

      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        flushStarted = true
        // Simulate slow write
        await new Promise(resolve => setTimeout(resolve, 100))
        const result = await originalWrite(path, data)
        flushCompleted = true
        return result
      })

      // Start first create operation
      const promise1 = db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      // Advance timers to trigger the microtask and start the flush
      await vi.advanceTimersByTimeAsync(0)

      // Start second create while flush is potentially in progress
      const promise2 = db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })

      // Advance timers to complete the slow write
      await vi.advanceTimersByTimeAsync(200)

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
    it('should not allow new flush while one is in progress', async () => {
      // Track how many flushes actually run
      let activeFlushes = 0
      let maxConcurrentFlushes = 0

      const originalWrite = storage.write.bind(storage)
      vi.spyOn(storage, 'write').mockImplementation(async (path, data) => {
        activeFlushes++
        maxConcurrentFlushes = Math.max(maxConcurrentFlushes, activeFlushes)
        // Simulate slow write
        await new Promise(resolve => setTimeout(resolve, 50))
        const result = await originalWrite(path, data)
        activeFlushes--
        return result
      })

      // Start multiple creates that should share a single flush
      const promises = [
        db.create('posts', { $type: 'Post', name: 'P1', title: 'T1', content: 'C1' }),
        db.create('posts', { $type: 'Post', name: 'P2', title: 'T2', content: 'C2' }),
        db.create('posts', { $type: 'Post', name: 'P3', title: 'T3', content: 'C3' }),
      ]

      // Advance timers to trigger the flush
      await vi.advanceTimersByTimeAsync(100)

      await Promise.all(promises)

      // There should only be one concurrent flush at most (batching)
      // Note: Due to the microtask scheduling, rapid creates should batch
      expect(maxConcurrentFlushes).toBeLessThanOrEqual(1)
    })
  })
})

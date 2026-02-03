/**
 * ParqueDB DO WAL Phase 1 - Batched Event Storage Tests
 *
 * Tests the events_wal table with:
 * - Namespace-based sequence counters (Sqids)
 * - Event batching to reduce SQLite row costs
 * - Counter persistence across DO restarts
 *
 * These tests run in the Cloudflare Workers environment with real bindings:
 * - Durable Objects (PARQUEDB) with SQLite storage
 *
 * Run with: npm run test:e2e:workers
 */

import { env } from 'cloudflare:test'
import { describe, it, expect, beforeEach } from 'vitest'
import type { TestEnv, ParqueDBDOTestStub } from './types'
import { asDOTestStub } from './types'

// Cast env to our typed environment
const testEnv = env as TestEnv

/**
 * Helper to ensure all promises are resolved before checking sync state
 * This helps avoid isolated storage issues in vitest-pool-workers
 */
async function withDO<T>(fn: () => Promise<T>): Promise<T> {
  const result = await fn()
  // Small delay to ensure all async operations complete
  await new Promise(resolve => setTimeout(resolve, 10))
  return result
}

describe('DO WAL Phase 1 - Batched Event Storage', () => {
  describe('Sqids Short ID Generation', () => {
    it('generates short IDs using Sqids from namespace counter', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-sqids-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity - should use Sqids for ID
      const entity = await stub.create('posts', {
        $type: 'Post',
        name: 'First Post',
      }, {}) as Record<string, unknown>

      expect(entity).toBeDefined()
      expect(entity.$id).toBeDefined()

      // ID format should be posts/{sqid} where sqid is short
      const entityId = entity.$id as string
      const [ns, sqid] = entityId.split('/')
      expect(ns).toBe('posts')
      expect(sqid).toBeDefined()
      // Sqids produces short IDs like 'Uk', '86u', 'RHEA'
      expect(sqid.length).toBeLessThanOrEqual(10)
    })

    it('increments counter for sequential creates', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-counter-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create multiple entities
      const entities: Array<Record<string, unknown>> = []
      for (let i = 0; i < 5; i++) {
        const entity = await stub.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
        }, {}) as Record<string, unknown>
        entities.push(entity)
      }

      // Each entity should have a unique ID
      const ids = entities.map(e => e.$id)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).toBe(5)

      // Verify all IDs are unique strings
      for (const entityId of ids) {
        expect(typeof entityId).toBe('string')
        expect(entityId).toContain('/')
      }
    })
  })

  describe('Event Batching with events_wal', () => {
    it('buffers events before flushing to events_wal', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-buffer-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Append events to buffer
      const eventIds: string[] = []
      for (let i = 0; i < 5; i++) {
        const eventId = await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
        eventIds.push(eventId)
      }

      // All event IDs should be unique
      expect(new Set(eventIds).size).toBe(5)

      // Before flush, WAL batch count should be 0 (events still in buffer)
      const batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(0)
    })

    it('writes batched events as single row on manual flush', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-flush-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Append events
      for (let i = 0; i < 10; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
      }

      // Manually flush
      await stub.flushNsEventBatch('posts')

      // Should have one batch in events_wal
      const batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(1)

      // Event count should still be tracked
      const eventCount = await stub.getUnflushedWalEventCount('posts')
      expect(eventCount).toBe(10)
    })

    it('stores sequence range in batch (first_seq, last_seq)', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-seq-range-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Append 25 events
      for (let i = 0; i < 25; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
      }

      // Flush and read events back
      await stub.flushNsEventBatch('posts')

      const events = await stub.readUnflushedWalEvents('posts')
      expect(events.length).toBe(25)

      // First event should have first Sqids ID
      // Note: Sqids IDs are short strings, not numbers
      expect(events[0].id).toBeDefined()
      expect(typeof events[0].id).toBe('string')
    })
  })

  describe('Counter Persistence', () => {
    it('maintains counter across multiple batches', async () => {
      const doName = `wal-persist-${Date.now()}`
      const id = testEnv.PARQUEDB.idFromName(doName)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // First batch: 10 events
      for (let i = 0; i < 10; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
      }
      await stub.flushNsEventBatch('posts')

      // Second batch: 10 more events
      for (let i = 10; i < 20; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
      }
      await stub.flushNsEventBatch('posts')

      // Should have 2 batches
      const batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(2)

      // All events should be readable
      const events = await stub.readUnflushedWalEvents('posts')
      expect(events.length).toBe(20)

      // Event IDs should all be unique
      const eventIds = events.map(e => e.id)
      expect(new Set(eventIds).size).toBe(20)
    })

    it('continues counter sequence after flush', async () => {
      const doName = `wal-continue-${Date.now()}`
      const id = testEnv.PARQUEDB.idFromName(doName)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create and flush first batch
      const firstBatchIds: string[] = []
      for (let i = 0; i < 5; i++) {
        const eventId = await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
        firstBatchIds.push(eventId)
      }
      await stub.flushNsEventBatch('posts')

      // Create second batch - IDs should continue sequence
      const secondBatchIds: string[] = []
      for (let i = 5; i < 10; i++) {
        const eventId = await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
        secondBatchIds.push(eventId)
      }

      // IDs should all be unique across batches
      const allIds = [...firstBatchIds, ...secondBatchIds]
      expect(new Set(allIds).size).toBe(10)

      // No ID from second batch should be in first batch
      for (const id of secondBatchIds) {
        expect(firstBatchIds).not.toContain(id)
      }
    })
  })

  describe('Multi-Namespace Support', () => {
    it('maintains separate counters per namespace', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-multi-ns-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Events for posts namespace
      const postsEventIds: string[] = []
      for (let i = 0; i < 5; i++) {
        const eventId = await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          actor: 'test/user',
        })
        postsEventIds.push(eventId)
      }

      // Events for users namespace
      const usersEventIds: string[] = []
      for (let i = 0; i < 3; i++) {
        const eventId = await stub.appendEventWithSeq('users', {
          ts: Date.now(),
          op: 'CREATE',
          target: `users:${i}`,
          actor: 'test/user',
        })
        usersEventIds.push(eventId)
      }

      // Each namespace should have unique IDs
      expect(new Set(postsEventIds).size).toBe(5)
      expect(new Set(usersEventIds).size).toBe(3)

      // Flush and check counts
      await stub.flushNsEventBatch('posts')
      await stub.flushNsEventBatch('users')

      const postsCount = await stub.getUnflushedWalEventCount('posts')
      const usersCount = await stub.getUnflushedWalEventCount('users')

      expect(postsCount).toBe(5)
      expect(usersCount).toBe(3)
    })

    it('flushes all namespaces with flushAllNsEventBatches', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-flush-all-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Buffer events for multiple namespaces
      await stub.appendEventWithSeq('posts', { ts: Date.now(), op: 'CREATE', target: 'posts:1', actor: 'test' })
      await stub.appendEventWithSeq('posts', { ts: Date.now(), op: 'CREATE', target: 'posts:2', actor: 'test' })
      await stub.appendEventWithSeq('users', { ts: Date.now(), op: 'CREATE', target: 'users:1', actor: 'test' })
      await stub.appendEventWithSeq('orders', { ts: Date.now(), op: 'CREATE', target: 'orders:1', actor: 'test' })

      // Flush all at once
      await stub.flushAllNsEventBatches()

      // Total count should be correct across all namespaces
      const totalCount = await stub.getTotalUnflushedWalEventCount()
      expect(totalCount).toBe(4)

      // Should have 3 batches (one per namespace)
      const batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(3)
    })
  })

  describe('Event Batch Cleanup', () => {
    it('can delete WAL batches up to a sequence', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-cleanup-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create 3 batches
      for (let batch = 0; batch < 3; batch++) {
        for (let i = 0; i < 10; i++) {
          await stub.appendEventWithSeq('posts', {
            ts: Date.now(),
            op: 'CREATE',
            target: `posts:${batch * 10 + i}`,
            actor: 'test/user',
          })
        }
        await stub.flushNsEventBatch('posts')
      }

      // Should have 3 batches, 30 events
      let batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(3)

      // Delete first 2 batches (sequences 1-20)
      await stub.deleteWalBatches('posts', 20)

      // Should have 1 batch remaining
      batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(1)

      // Should have 10 events remaining
      const eventCount = await stub.getUnflushedWalEventCount('posts')
      expect(eventCount).toBe(10)
    })
  })

  describe('Cost Optimization', () => {
    it('stores 100 events in a single SQLite row', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-cost-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create 100 events (default batch threshold)
      for (let i = 0; i < 100; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:${i}`,
          after: { name: `Post ${i}` },
          actor: 'test/user',
        })
      }

      // At threshold, should auto-flush
      // Note: The threshold triggers flush, so we should have 1 batch
      const batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(1)

      // 100 events in 1 row = 100x cost reduction vs per-event storage
      const events = await stub.readUnflushedWalEvents('posts')
      expect(events.length).toBe(100)
    })
  })

  describe('Entity Creation with Sqids', () => {
    it('creates entities with short Sqids-based IDs', async () => {
      const id = testEnv.PARQUEDB.idFromName(`wal-entity-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create first entity
      const entity1 = await stub.create('posts', {
        $type: 'Post',
        name: 'First',
      }, {}) as Record<string, unknown>

      // Create second entity
      const entity2 = await stub.create('posts', {
        $type: 'Post',
        name: 'Second',
      }, {}) as Record<string, unknown>

      // IDs should be short and different
      const id1 = (entity1.$id as string).split('/')[1]
      const id2 = (entity2.$id as string).split('/')[1]

      expect(id1.length).toBeLessThanOrEqual(10)
      expect(id2.length).toBeLessThanOrEqual(10)
      expect(id1).not.toBe(id2)

      // Can retrieve by ID
      const retrieved = await stub.get('posts', id1)
      expect(retrieved).not.toBeNull()
      expect((retrieved as Record<string, unknown>).name).toBe('First')
    })
  })
})

/**
 * ParqueDB DO SQLite Persistence E2E Tests
 *
 * Comprehensive tests for Durable Object SQLite persistence covering:
 * 1. Entity persistence across DO restarts
 * 2. Event log persistence and replay
 * 3. Relationship persistence
 * 4. Transaction durability
 * 5. Concurrent access patterns
 * 6. Recovery from failures
 *
 * These tests run in the Cloudflare Workers environment with real bindings:
 * - Durable Objects (PARQUEDB) with SQLite storage
 * - R2 (BUCKET) for Parquet file storage
 *
 * Run with: npm run test:e2e:workers
 */

import { env } from 'cloudflare:test'
import { describe, it, expect, beforeEach } from 'vitest'
import type { TestEnv, ParqueDBDOTestStub } from '../types'
import { asDOTestStub } from '../types'

// Cast env to our typed environment
const testEnv = env as TestEnv

/**
 * Helper to create a fresh DO stub with a unique name
 */
function createDOStub(prefix: string): { id: DurableObjectId; stub: ParqueDBDOTestStub; name: string } {
  const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const id = testEnv.PARQUEDB.idFromName(name)
  const stub = asDOTestStub(testEnv.PARQUEDB.get(id))
  return { id, stub, name }
}

/**
 * Helper to get a DO stub by name (simulates restart by getting fresh reference)
 */
function getDOStubByName(name: string): ParqueDBDOTestStub {
  const id = testEnv.PARQUEDB.idFromName(name)
  return asDOTestStub(testEnv.PARQUEDB.get(id))
}

// =============================================================================
// 1. Entity Persistence Across DO Restarts
// =============================================================================

describe('DO SQLite Persistence - Entity Persistence', () => {
  describe('Entity Persistence Across DO Restarts', () => {
    it('persists entity data in SQLite across DO stub recreations', async () => {
      const { stub, name } = createDOStub('persist-entity')

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Persisted Post',
        content: 'This content should persist',
        views: 100,
      }, {}) as Record<string, unknown>

      expect(created.$id).toBeDefined()
      const entityId = (created.$id as string).split('/')[1]!

      // Get a "fresh" stub (simulates DO restart - new stub reference)
      const freshStub = getDOStubByName(name)

      // Entity should still be retrievable
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>

      expect(retrieved).not.toBeNull()
      expect(retrieved.name).toBe('Persisted Post')
      expect(retrieved.content).toBe('This content should persist')
      expect(retrieved.views).toBe(100)
      expect(retrieved.version).toBe(1)
    })

    it('persists entity updates across DO restarts', async () => {
      const { stub, name } = createDOStub('persist-update')

      // Create and update an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Original Name',
        counter: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      await stub.update('posts', entityId, {
        $set: { name: 'Updated Name' },
        $inc: { counter: 5 },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Updates should be visible
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>

      expect(retrieved.name).toBe('Updated Name')
      expect(retrieved.counter).toBe(5)
      expect(retrieved.version).toBe(2)
    })

    it('persists deleted entity state across DO restarts', async () => {
      const { stub, name } = createDOStub('persist-delete')

      // Create and delete an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'To Be Deleted',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!
      await stub.delete('posts', entityId, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Deleted entity should not be found (without includeDeleted)
      const result = await freshStub.get('posts', entityId)
      expect(result).toBeNull()

      // But should be found with includeDeleted
      const deleted = await freshStub.get('posts', entityId, true) as Record<string, unknown>
      expect(deleted).not.toBeNull()
      expect(deleted.deletedAt).toBeDefined()
    })

    it('persists multiple entities in same namespace', async () => {
      const { stub, name } = createDOStub('persist-multi')

      // Create multiple entities
      const entities: Array<Record<string, unknown>> = []
      for (let i = 0; i < 5; i++) {
        const entity = await stub.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          index: i,
        }, {}) as Record<string, unknown>
        entities.push(entity)
      }

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be retrievable
      for (let i = 0; i < entities.length; i++) {
        const entityId = (entities[i]!.$id as string).split('/')[1]!
        const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
        expect(retrieved).not.toBeNull()
        expect(retrieved.name).toBe(`Post ${i}`)
        expect(retrieved.index).toBe(i)
      }
    })

    it('persists entities across multiple namespaces', async () => {
      const { stub, name } = createDOStub('persist-namespaces')

      // Create entities in different namespaces
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, {}) as Record<string, unknown>

      const user = await stub.create('users', {
        $type: 'User',
        name: 'Test User',
      }, {}) as Record<string, unknown>

      const comment = await stub.create('comments', {
        $type: 'Comment',
        name: 'Test Comment',
      }, {}) as Record<string, unknown>

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be retrievable from their namespaces
      const postId = (post.$id as string).split('/')[1]!
      const userId = (user.$id as string).split('/')[1]!
      const commentId = (comment.$id as string).split('/')[1]!

      const retrievedPost = await freshStub.get('posts', postId)
      const retrievedUser = await freshStub.get('users', userId)
      const retrievedComment = await freshStub.get('comments', commentId)

      expect(retrievedPost).not.toBeNull()
      expect(retrievedUser).not.toBeNull()
      expect(retrievedComment).not.toBeNull()
    })
  })

  describe('Sequence Counter Persistence', () => {
    it('maintains sequence counters across DO restarts', async () => {
      const { stub, name } = createDOStub('persist-counter')

      // Create some entities to advance the counter
      const first = await stub.create('posts', {
        $type: 'Post',
        name: 'First Post',
      }, {}) as Record<string, unknown>

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Create another entity - ID should continue from where we left off
      const second = await freshStub.create('posts', {
        $type: 'Post',
        name: 'Second Post',
      }, {}) as Record<string, unknown>

      // IDs should be unique and sequential
      expect(first.$id).not.toBe(second.$id)

      // The sequence should have continued
      const firstId = (first.$id as string).split('/')[1]!
      const secondId = (second.$id as string).split('/')[1]!
      expect(firstId).not.toBe(secondId)
    })

    it('maintains separate counters per namespace across restarts', async () => {
      const { stub, name } = createDOStub('persist-multi-counter')

      // Create entities in different namespaces
      await stub.create('posts', { $type: 'Post', name: 'Post 1' }, {})
      await stub.create('posts', { $type: 'Post', name: 'Post 2' }, {})
      await stub.create('users', { $type: 'User', name: 'User 1' }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Create more entities - counters should continue correctly
      const post3 = await freshStub.create('posts', { $type: 'Post', name: 'Post 3' }, {}) as Record<string, unknown>
      const user2 = await freshStub.create('users', { $type: 'User', name: 'User 2' }, {}) as Record<string, unknown>

      // IDs should be unique
      expect(post3.$id).toBeDefined()
      expect(user2.$id).toBeDefined()
      expect(post3.$id).toContain('posts/')
      expect(user2.$id).toContain('users/')
    })
  })
})

// =============================================================================
// 2. Event Log Persistence and Replay
// =============================================================================

describe('DO SQLite Persistence - Event Log', () => {
  describe('Event Log Persistence', () => {
    it('persists event log entries in events_wal', async () => {
      const { stub, name } = createDOStub('persist-events')

      // Create some entities (generates CREATE events)
      for (let i = 0; i < 3; i++) {
        await stub.create('posts', {
          $type: 'Post',
          name: `Event Post ${i}`,
        }, {})
      }

      // Flush events to WAL
      await stub.flushNsEventBatch('posts')

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Events should be readable from WAL
      const events = await freshStub.readUnflushedWalEvents('posts')
      expect(events.length).toBe(3)

      // All events should be CREATE operations
      for (const event of events) {
        expect(event.op).toBe('CREATE')
      }
    })

    it('replays events correctly to reconstruct entity state', async () => {
      const { stub, name } = createDOStub('persist-replay')

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Replay Test',
        counter: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Perform multiple updates (generates UPDATE events)
      await stub.update('posts', entityId, { $inc: { counter: 1 } }, {})
      await stub.update('posts', entityId, { $inc: { counter: 2 } }, {})
      await stub.update('posts', entityId, { $set: { name: 'Updated Name' } }, {})

      // Flush all events
      await stub.flushNsEventBatch('posts')

      // Get fresh stub (clears entity cache)
      const freshStub = getDOStubByName(name)

      // Entity state should be correctly reconstructed from events
      const reconstructed = await freshStub.getEntityFromEvents('posts', entityId) as Record<string, unknown>

      expect(reconstructed).not.toBeNull()
      expect(reconstructed.name).toBe('Updated Name')
      expect(reconstructed.counter).toBe(3)
      expect(reconstructed.version).toBe(4) // 1 create + 3 updates
    })

    it('maintains event order across batches', async () => {
      const { stub, name } = createDOStub('persist-order')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Order Test',
        values: [],
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Multiple updates with flushes between
      for (let i = 0; i < 3; i++) {
        await stub.update('posts', entityId, {
          $push: { values: i },
        }, {})
        await stub.flushNsEventBatch('posts')
      }

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Reconstructed state should have values in order
      const reconstructed = await freshStub.getEntityFromEvents('posts', entityId) as Record<string, unknown>

      expect(reconstructed).not.toBeNull()
      expect((reconstructed.values as number[])).toEqual([0, 1, 2])
    })

    it('preserves event metadata (timestamps, actors)', async () => {
      const { stub, name } = createDOStub('persist-metadata')

      // Append event with specific actor
      const eventId = await stub.appendEventWithSeq('posts', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:test-meta',
        after: { $type: 'Post', name: 'Metadata Test' },
        actor: 'users/test-actor',
      })

      await stub.flushNsEventBatch('posts')

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Read events back
      const events = await freshStub.readUnflushedWalEvents('posts')
      const event = events.find(e => e.id === eventId)

      expect(event).toBeDefined()
      expect(event!.actor).toBe('users/test-actor')
      expect(event!.ts).toBeGreaterThan(0)
    })
  })

  describe('Event Batch Persistence', () => {
    it('persists batched events as single row', async () => {
      const { stub, name } = createDOStub('persist-batch')

      // Append multiple events
      for (let i = 0; i < 10; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:batch-${i}`,
          after: { name: `Batch ${i}` },
          actor: 'test/user',
        })
      }

      // Flush as single batch
      await stub.flushNsEventBatch('posts')

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Should have 1 batch with 10 events
      const batchCount = await freshStub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(1)

      const eventCount = await freshStub.getUnflushedWalEventCount('posts')
      expect(eventCount).toBe(10)
    })

    it('persists multiple batches independently', async () => {
      const { stub, name } = createDOStub('persist-multi-batch')

      // First batch
      for (let i = 0; i < 5; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:first-${i}`,
          actor: 'test/user',
        })
      }
      await stub.flushNsEventBatch('posts')

      // Second batch
      for (let i = 0; i < 5; i++) {
        await stub.appendEventWithSeq('posts', {
          ts: Date.now(),
          op: 'CREATE',
          target: `posts:second-${i}`,
          actor: 'test/user',
        })
      }
      await stub.flushNsEventBatch('posts')

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Should have 2 batches with 10 total events
      const batchCount = await freshStub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(2)

      const eventCount = await freshStub.getUnflushedWalEventCount('posts')
      expect(eventCount).toBe(10)
    })
  })
})

// =============================================================================
// 3. Relationship Persistence
// =============================================================================

describe('DO SQLite Persistence - Relationships', () => {
  describe('Relationship Persistence', () => {
    it('persists relationships across DO restarts', async () => {
      const { stub, name } = createDOStub('persist-rel')

      // Create entities
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Linked Post',
      }, {}) as Record<string, unknown>

      const user = await stub.create('users', {
        $type: 'User',
        name: 'Post Author',
      }, {}) as Record<string, unknown>

      const postId = post.$id as string
      const postShortId = postId.split('/')[1]!
      const userId = user.$id as string

      // Create relationship
      await stub.update('posts', postShortId, {
        $link: { author: userId },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Relationship should be persisted
      // getRelationships(ns, id, predicate?, direction?)
      const relationships = await freshStub.getRelationships('posts', postShortId)

      expect(relationships.length).toBeGreaterThan(0)
      const authorRel = relationships.find(r => r.predicate === 'author')
      expect(authorRel).toBeDefined()
      // DORelationship has toNs/toId instead of 'to'
      expect(`${authorRel!.toNs}/${authorRel!.toId}`).toBe(userId)
    })

    it('persists multiple relationships for same entity', async () => {
      const { stub, name } = createDOStub('persist-multi-rel')

      // Create entities
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Multi-Rel Post',
      }, {}) as Record<string, unknown>

      const author = await stub.create('users', {
        $type: 'User',
        name: 'Author',
      }, {}) as Record<string, unknown>

      const reviewer = await stub.create('users', {
        $type: 'User',
        name: 'Reviewer',
      }, {}) as Record<string, unknown>

      const postId = post.$id as string
      const postShortId = postId.split('/')[1]!

      // Create multiple relationships
      await stub.update('posts', postShortId, {
        $link: {
          author: author.$id as string,
          reviewer: reviewer.$id as string,
        },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All relationships should be persisted
      // getRelationships(ns, id, predicate?, direction?)
      const relationships = await freshStub.getRelationships('posts', postShortId)

      expect(relationships.length).toBe(2)
      expect(relationships.some(r => r.predicate === 'author')).toBe(true)
      expect(relationships.some(r => r.predicate === 'reviewer')).toBe(true)
    })

    it('persists relationship deletions (unlink)', async () => {
      const { stub, name } = createDOStub('persist-unlink')

      // Create entities and link them
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Unlink Test',
      }, {}) as Record<string, unknown>

      const user = await stub.create('users', {
        $type: 'User',
        name: 'User to Unlink',
      }, {}) as Record<string, unknown>

      const postId = post.$id as string
      const postShortId = postId.split('/')[1]!
      const userId = user.$id as string

      // Link then unlink
      await stub.update('posts', postShortId, {
        $link: { author: userId },
      }, {})

      await stub.update('posts', postShortId, {
        $unlink: { author: userId },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Relationship should be deleted (or marked as deleted)
      // getRelationships(ns, id, predicate?, direction?) - returns only non-deleted by default
      const relationships = await freshStub.getRelationships('posts', postShortId)

      // getRelationships returns only non-deleted relationships, so author should not be present
      const authorRel = relationships.find(r => r.predicate === 'author')
      expect(authorRel).toBeUndefined()
    })

    it('persists relationship metadata (matchMode, similarity)', async () => {
      const { stub, name } = createDOStub('persist-rel-meta')

      // Create entities
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Metadata Rel Post',
      }, {}) as Record<string, unknown>

      const user = await stub.create('users', {
        $type: 'User',
        name: 'Fuzzy Match User',
      }, {}) as Record<string, unknown>

      const postId = post.$id as string
      const postShortId = postId.split('/')[1]!
      const userId = user.$id as string

      // Create relationship with metadata using link method directly
      await stub.link(postId, 'author', userId, {
        matchMode: 'fuzzy',
        similarity: 0.85,
        data: { confidence: 'high' },
      })

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Relationship metadata should be persisted
      // getRelationships(ns, id, predicate?, direction?)
      const relationships = await freshStub.getRelationships('posts', postShortId)

      const authorRel = relationships.find(r => r.predicate === 'author')
      expect(authorRel).toBeDefined()
      expect(authorRel!.matchMode).toBe('fuzzy')
      expect(authorRel!.similarity).toBeCloseTo(0.85)
    })
  })
})

// =============================================================================
// 4. Transaction Durability
// =============================================================================

describe('DO SQLite Persistence - Transaction Durability', () => {
  describe('Atomic Write Operations', () => {
    it('ensures create operation is atomic', async () => {
      const { stub, name } = createDOStub('atomic-create')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Atomic Create',
        field1: 'value1',
        field2: 'value2',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Entity should be fully persisted with all fields
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>

      expect(retrieved).not.toBeNull()
      expect(retrieved.name).toBe('Atomic Create')
      expect(retrieved.field1).toBe('value1')
      expect(retrieved.field2).toBe('value2')
    })

    it('ensures update operation is atomic', async () => {
      const { stub, name } = createDOStub('atomic-update')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Original',
        counter: 0,
        status: 'draft',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Complex update with multiple operators
      await stub.update('posts', entityId, {
        $set: { name: 'Updated', status: 'published' },
        $inc: { counter: 10 },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All updates should be applied atomically
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>

      expect(retrieved.name).toBe('Updated')
      expect(retrieved.status).toBe('published')
      expect(retrieved.counter).toBe(10)
    })

    it('ensures bulk create is atomic (all or nothing)', async () => {
      const { stub, name } = createDOStub('atomic-bulk')

      // Bulk create (5+ triggers R2 bypass)
      const items = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Bulk Post ${i}`,
        index: i,
      }))

      const entities = await stub.createMany('posts', items, {}) as Array<Record<string, unknown>>
      expect(entities.length).toBe(10)

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be retrievable
      for (const entity of entities) {
        const entityId = (entity.$id as string).split('/')[1]!
        const retrieved = await freshStub.get('posts', entityId)
        expect(retrieved).not.toBeNull()
      }
    })
  })

  describe('Version-Based Concurrency', () => {
    it('persists version increments correctly', async () => {
      const { stub, name } = createDOStub('version-persist')

      // Create entity (version 1)
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Version Test',
      }, {}) as Record<string, unknown>

      expect(created.version).toBe(1)
      const entityId = (created.$id as string).split('/')[1]!

      // Multiple updates
      await stub.update('posts', entityId, { $set: { name: 'V2' } }, {})
      await stub.update('posts', entityId, { $set: { name: 'V3' } }, {})
      await stub.update('posts', entityId, { $set: { name: 'V4' } }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Version should be preserved
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(retrieved.version).toBe(4)
    })

    // Note: Error validation tests are skipped because DO RPC errors
    // cause isolated storage issues with vitest-pool-workers.
    // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
    it.skip('enforces optimistic concurrency on update', async () => {
      const { stub, name } = createDOStub('optimistic-concurrency')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Concurrency Test',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Update with correct version
      await stub.update('posts', entityId, {
        $set: { name: 'Updated' },
      }, { expectedVersion: 1 })

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Try update with wrong version - should fail
      // Use try-catch pattern for DO error handling to avoid isolated storage issues
      let errorMessage = ''
      try {
        await freshStub.update('posts', entityId, {
          $set: { name: 'Should Fail' },
        }, { expectedVersion: 1 }) // Version is now 2
      } catch (err) {
        errorMessage = (err as Error).message
      }
      expect(errorMessage).toMatch(/version/i)
    })
  })
})

// =============================================================================
// 5. Concurrent Access Patterns
// =============================================================================

describe('DO SQLite Persistence - Concurrent Access', () => {
  describe('Sequential Operations', () => {
    it('handles rapid sequential creates correctly', async () => {
      const { stub, name } = createDOStub('rapid-create')

      // Rapidly create many entities
      const entities: Array<Record<string, unknown>> = []
      for (let i = 0; i < 20; i++) {
        const entity = await stub.create('posts', {
          $type: 'Post',
          name: `Rapid Post ${i}`,
        }, {}) as Record<string, unknown>
        entities.push(entity)
      }

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be persisted
      for (const entity of entities) {
        const entityId = (entity.$id as string).split('/')[1]!
        const retrieved = await freshStub.get('posts', entityId)
        expect(retrieved).not.toBeNull()
      }

      // All IDs should be unique
      const ids = entities.map(e => e.$id)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).toBe(20)
    })

    it('handles rapid sequential updates to same entity', async () => {
      const { stub, name } = createDOStub('rapid-update')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Rapid Update',
        counter: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Rapidly update the same entity
      for (let i = 0; i < 10; i++) {
        await stub.update('posts', entityId, {
          $inc: { counter: 1 },
        }, {})
      }

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Final state should reflect all updates
      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(retrieved.counter).toBe(10)
      expect(retrieved.version).toBe(11) // 1 create + 10 updates
    })
  })

  describe('Parallel Operations (within same stub)', () => {
    it('handles parallel creates in same namespace', async () => {
      const { stub, name } = createDOStub('parallel-create')

      // Create entities in parallel
      const promises = Array.from({ length: 5 }, (_, i) =>
        stub.create('posts', {
          $type: 'Post',
          name: `Parallel Post ${i}`,
        }, {})
      )

      const entities = await Promise.all(promises) as Array<Record<string, unknown>>

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be persisted with unique IDs
      const ids = new Set<string>()
      for (const entity of entities) {
        expect(ids.has(entity.$id as string)).toBe(false)
        ids.add(entity.$id as string)

        const entityId = (entity.$id as string).split('/')[1]!
        const retrieved = await freshStub.get('posts', entityId)
        expect(retrieved).not.toBeNull()
      }
    })

    it('handles parallel creates across namespaces', async () => {
      const { stub, name } = createDOStub('parallel-ns')

      // Create entities in different namespaces in parallel
      const [post, user, comment] = await Promise.all([
        stub.create('posts', { $type: 'Post', name: 'Parallel Post' }, {}),
        stub.create('users', { $type: 'User', name: 'Parallel User' }, {}),
        stub.create('comments', { $type: 'Comment', name: 'Parallel Comment' }, {}),
      ]) as Array<Record<string, unknown>>

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // All entities should be persisted
      const postId = (post.$id as string).split('/')[1]!
      const userId = (user.$id as string).split('/')[1]!
      const commentId = (comment.$id as string).split('/')[1]!

      expect(await freshStub.get('posts', postId)).not.toBeNull()
      expect(await freshStub.get('users', userId)).not.toBeNull()
      expect(await freshStub.get('comments', commentId)).not.toBeNull()
    })
  })
})

// =============================================================================
// 6. Recovery from Failures
// =============================================================================

describe('DO SQLite Persistence - Recovery', () => {
  describe('State Recovery', () => {
    it('recovers entity state from events after cache clear', async () => {
      const { stub, name } = createDOStub('recover-cache')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Cache Recovery',
        data: { nested: { value: 42 } },
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Update it
      await stub.update('posts', entityId, {
        $set: { name: 'Updated Cache' },
      }, {})

      // Flush events
      await stub.flushNsEventBatch('posts')

      // Clear entity cache (simulates memory pressure)
      stub.clearEntityCache()

      // Entity should be recoverable from events
      const recovered = await stub.get('posts', entityId) as Record<string, unknown>
      expect(recovered).not.toBeNull()
      expect(recovered.name).toBe('Updated Cache')
      expect((recovered.data as Record<string, unknown>).nested).toEqual({ value: 42 })
    })

    it('recovers from partial flush (buffered events)', async () => {
      const { stub, name } = createDOStub('recover-partial')

      // Create entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Partial Flush',
        counter: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Some updates that get flushed
      await stub.update('posts', entityId, { $inc: { counter: 1 } }, {})
      await stub.flushNsEventBatch('posts')

      // More updates still in buffer
      await stub.update('posts', entityId, { $inc: { counter: 2 } }, {})
      await stub.update('posts', entityId, { $inc: { counter: 3 } }, {})
      // Don't flush these

      // Get fresh stub (buffer is lost, but entity state should be reconstructable)
      const freshStub = getDOStubByName(name)

      // Note: Events in memory buffer may be lost on "restart"
      // But flushed events should still be there
      const recovered = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(recovered).not.toBeNull()
      // At minimum, flushed state should be recoverable (counter = 1)
      expect(recovered.counter).toBeGreaterThanOrEqual(1)
    })

    it('recovers relationships from SQLite', async () => {
      const { stub, name } = createDOStub('recover-rels')

      // Create entities with relationships
      const post = await stub.create('posts', {
        $type: 'Post',
        name: 'Post with Rels',
      }, {}) as Record<string, unknown>

      const user = await stub.create('users', {
        $type: 'User',
        name: 'Related User',
      }, {}) as Record<string, unknown>

      const postId = post.$id as string
      const postShortId = postId.split('/')[1]!
      const userId = user.$id as string

      await stub.update('posts', postShortId, {
        $link: { author: userId },
      }, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Relationships stored in SQLite should survive
      // getRelationships(ns, id, predicate?, direction?)
      const relationships = await freshStub.getRelationships('posts', postShortId)
      expect(relationships.length).toBeGreaterThan(0)
    })
  })

  describe('WAL Cleanup and Recovery', () => {
    it('can delete processed WAL batches', async () => {
      const { stub, name } = createDOStub('wal-cleanup')

      // Create 3 batches of events
      for (let batch = 0; batch < 3; batch++) {
        for (let i = 0; i < 5; i++) {
          await stub.appendEventWithSeq('posts', {
            ts: Date.now(),
            op: 'CREATE',
            target: `posts:batch${batch}-${i}`,
            actor: 'test/user',
          })
        }
        await stub.flushNsEventBatch('posts')
      }

      // Should have 3 batches, 15 events
      let batchCount = await stub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(3)

      // Delete first 2 batches (events 1-10)
      await stub.deleteWalBatches('posts', 10)

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Should have 1 batch remaining
      batchCount = await freshStub.getUnflushedWalBatchCount()
      expect(batchCount).toBe(1)
    })

    it('persists pending row groups for bulk operations', async () => {
      const { stub, name } = createDOStub('pending-rg')

      // Bulk create (triggers pending row group)
      const items = Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Pending RG Post ${i}`,
      }))

      await stub.createMany('posts', items, {})

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      // Pending row groups should be persisted
      const pending = await freshStub.getPendingRowGroups('posts')
      expect(pending.length).toBe(1)
      expect(pending[0]!.rowCount).toBe(10)
    })
  })
})

// =============================================================================
// 7. Edge Cases and Data Integrity
// =============================================================================

describe('DO SQLite Persistence - Edge Cases', () => {
  describe('Data Integrity', () => {
    it('preserves special characters in entity data', async () => {
      const { stub, name } = createDOStub('special-chars')

      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Special "Quotes" & <Tags>',
        content: "Line1\nLine2\tTabbed",
        emoji: '🎉 Party! 🎊',
        unicode: 'Japanese: 日本語, Chinese: 中文',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(retrieved.name).toBe('Special "Quotes" & <Tags>')
      expect(retrieved.content).toBe("Line1\nLine2\tTabbed")
      expect(retrieved.emoji).toBe('🎉 Party! 🎊')
      expect(retrieved.unicode).toBe('Japanese: 日本語, Chinese: 中文')
    })

    it('preserves nested objects and arrays', async () => {
      const { stub, name } = createDOStub('nested-data')

      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Nested Data',
        metadata: {
          level1: {
            level2: {
              level3: 'deep value',
            },
          },
          array: [1, 2, { nested: true }],
        },
        tags: ['tag1', 'tag2', 'tag3'],
        numbers: [1, 2.5, -3, 0],
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      const metadata = retrieved.metadata as Record<string, unknown>

      expect((metadata.level1 as Record<string, unknown>).level2).toEqual({ level3: 'deep value' })
      expect((metadata.array as unknown[])[2]).toEqual({ nested: true })
      expect(retrieved.tags).toEqual(['tag1', 'tag2', 'tag3'])
      expect(retrieved.numbers).toEqual([1, 2.5, -3, 0])
    })

    it('preserves null and undefined values appropriately', async () => {
      const { stub, name } = createDOStub('null-values')

      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Null Test',
        nullField: null,
        emptyString: '',
        zero: 0,
        falseValue: false,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(retrieved.nullField).toBeNull()
      expect(retrieved.emptyString).toBe('')
      expect(retrieved.zero).toBe(0)
      expect(retrieved.falseValue).toBe(false)
    })
  })

  describe('Large Data Handling', () => {
    it('persists entities with large text content', async () => {
      const { stub, name } = createDOStub('large-text')

      // Create entity with large content (100KB)
      const largeContent = 'x'.repeat(100 * 1024)

      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Large Content Post',
        content: largeContent,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      expect(retrieved.content).toBe(largeContent)
    })

    it('persists entities with many fields', async () => {
      const { stub, name } = createDOStub('many-fields')

      // Create entity with 100 fields
      const fields: Record<string, unknown> = {
        $type: 'Post',
        name: 'Many Fields Post',
      }
      for (let i = 0; i < 100; i++) {
        fields[`field_${i}`] = `value_${i}`
      }

      const created = await stub.create('posts', fields as any, {}) as Record<string, unknown>
      const entityId = (created.$id as string).split('/')[1]!

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      for (let i = 0; i < 100; i++) {
        expect(retrieved[`field_${i}`]).toBe(`value_${i}`)
      }
    })
  })

  describe('Timestamp Handling', () => {
    it('preserves createdAt/updatedAt timestamps', async () => {
      const { stub, name } = createDOStub('timestamps')

      const beforeCreate = Date.now()
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Timestamp Test',
      }, {}) as Record<string, unknown>
      const afterCreate = Date.now()

      const entityId = (created.$id as string).split('/')[1]!

      // Small delay before update
      await new Promise(resolve => setTimeout(resolve, 10))

      const beforeUpdate = Date.now()
      await stub.update('posts', entityId, {
        $set: { name: 'Updated Timestamp' },
      }, {})
      const afterUpdate = Date.now()

      // Get fresh stub
      const freshStub = getDOStubByName(name)

      const retrieved = await freshStub.get('posts', entityId) as Record<string, unknown>
      const createdAt = new Date(retrieved.createdAt as string).getTime()
      const updatedAt = new Date(retrieved.updatedAt as string).getTime()

      // createdAt should be between beforeCreate and afterCreate
      expect(createdAt).toBeGreaterThanOrEqual(beforeCreate)
      expect(createdAt).toBeLessThanOrEqual(afterCreate)

      // updatedAt should be between beforeUpdate and afterUpdate
      expect(updatedAt).toBeGreaterThanOrEqual(beforeUpdate)
      expect(updatedAt).toBeLessThanOrEqual(afterUpdate)

      // updatedAt should be after createdAt
      expect(updatedAt).toBeGreaterThanOrEqual(createdAt)
    })
  })
})

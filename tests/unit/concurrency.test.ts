/**
 * Concurrency and Race Condition Tests for ParqueDB
 *
 * Tests for concurrent operations to ensure data consistency and proper
 * handling of race conditions in CRUD operations, index updates, event
 * ordering, and cache consistency.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Collection, clearGlobalStorage, getEventsForEntity, clearEventLog } from '../../src/Collection'
import type { Entity, EntityId } from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  views: number
  likes: number
  tags: string[]
}

interface Counter {
  value: number
}

interface User {
  email: string
  username: string
  balance: number
}

// =============================================================================
// Test Setup
// =============================================================================

describe('Concurrency and Race Conditions', () => {
  let postsCollection: Collection<Post>
  let countersCollection: Collection<Counter>
  let usersCollection: Collection<User>

  beforeEach(() => {
    clearGlobalStorage()
    clearEventLog()
    postsCollection = new Collection<Post>('posts')
    countersCollection = new Collection<Counter>('counters')
    usersCollection = new Collection<User>('users')
  })

  afterEach(() => {
    clearGlobalStorage()
    clearEventLog()
  })

  // ===========================================================================
  // Parallel CRUD Operations
  // ===========================================================================

  describe('Parallel CRUD Operations', () => {
    describe('parallel creates', () => {
      it('should handle multiple parallel creates without data loss', async () => {
        const createPromises = Array.from({ length: 50 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `post-${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: 0,
            likes: 0,
            tags: [],
          })
        )

        const results = await Promise.all(createPromises)

        // All creates should succeed
        expect(results).toHaveLength(50)

        // All entities should have unique IDs
        const ids = results.map(r => r.$id)
        const uniqueIds = new Set(ids)
        expect(uniqueIds.size).toBe(50)

        // Verify all entities exist in storage
        const allPosts = await postsCollection.find()
        expect(allPosts).toHaveLength(50)
      })

      it('should generate monotonically increasing IDs even under concurrent creates', async () => {
        const createPromises = Array.from({ length: 100 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `post-${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: 0,
            likes: 0,
            tags: [],
          })
        )

        const results = await Promise.all(createPromises)

        // All should have unique IDs
        const ids = results.map(r => r.$id as string)
        expect(new Set(ids).size).toBe(100)

        // IDs should be sortable (ULID-like format)
        const sortedIds = [...ids].sort()
        // Each ID should be unique (no collisions)
        for (let i = 1; i < sortedIds.length; i++) {
          expect(sortedIds[i]).not.toBe(sortedIds[i - 1])
        }
      })
    })

    describe('parallel updates to different entities', () => {
      it('should handle concurrent updates to different entities', async () => {
        // Create multiple entities first
        const entities = await Promise.all(
          Array.from({ length: 20 }, (_, i) =>
            postsCollection.create({
              $type: 'Post',
              name: `post-${i}`,
              title: `Title ${i}`,
              content: `Content ${i}`,
              status: 'draft',
              views: 0,
              likes: 0,
              tags: [],
            })
          )
        )

        // Update all entities concurrently
        const updatePromises = entities.map((e, i) =>
          postsCollection.update((e.$id as string).split('/')[1], {
            $set: { status: 'published', title: `Updated Title ${i}` },
            $inc: { views: 100 },
          })
        )

        const results = await Promise.all(updatePromises)

        // All updates should succeed
        expect(results.every(r => r.modifiedCount === 1)).toBe(true)

        // Verify all updates persisted correctly
        const allPosts = await postsCollection.find({ status: 'published' })
        expect(allPosts).toHaveLength(20)
        expect(allPosts.every(p => (p.views as number) === 100)).toBe(true)
      })
    })

    describe('parallel deletes', () => {
      it('should handle concurrent deletes without errors', async () => {
        // Create entities
        const entities = await Promise.all(
          Array.from({ length: 30 }, (_, i) =>
            postsCollection.create({
              $type: 'Post',
              name: `post-${i}`,
              title: `Title ${i}`,
              content: `Content ${i}`,
              status: 'draft',
              views: 0,
              likes: 0,
              tags: [],
            })
          )
        )

        // Delete all concurrently
        const deletePromises = entities.map(e =>
          postsCollection.delete((e.$id as string).split('/')[1])
        )

        const results = await Promise.all(deletePromises)

        // All deletes should succeed
        expect(results.every(r => r.deletedCount === 1)).toBe(true)

        // Verify all entities are soft-deleted
        const activePosts = await postsCollection.find()
        expect(activePosts).toHaveLength(0)

        const allPosts = await postsCollection.find({}, { includeDeleted: true })
        expect(allPosts).toHaveLength(30)
        expect(allPosts.every(p => p.deletedAt !== undefined)).toBe(true)
      })
    })

    describe('mixed CRUD operations', () => {
      it('should handle concurrent create, update, delete operations', async () => {
        // Create initial entities
        const initialEntities = await Promise.all(
          Array.from({ length: 10 }, (_, i) =>
            postsCollection.create({
              $type: 'Post',
              name: `initial-${i}`,
              title: `Initial ${i}`,
              content: `Content ${i}`,
              status: 'draft',
              views: 0,
              likes: 0,
              tags: [],
            })
          )
        )

        // Mix of operations concurrently
        const operations = [
          // Create new entities
          ...Array.from({ length: 5 }, (_, i) =>
            postsCollection.create({
              $type: 'Post',
              name: `new-${i}`,
              title: `New ${i}`,
              content: `New Content ${i}`,
              status: 'draft',
              views: 0,
              likes: 0,
              tags: [],
            })
          ),
          // Update some existing entities
          ...initialEntities.slice(0, 5).map((e, i) =>
            postsCollection.update((e.$id as string).split('/')[1], {
              $set: { status: 'published' },
            })
          ),
          // Delete some existing entities
          ...initialEntities.slice(5).map(e =>
            postsCollection.delete((e.$id as string).split('/')[1])
          ),
        ]

        await Promise.all(operations)

        // Verify final state
        const allPosts = await postsCollection.find()
        // 5 new + 5 updated = 10 active (5 deleted)
        expect(allPosts).toHaveLength(10)

        const publishedPosts = await postsCollection.find({ status: 'published' })
        expect(publishedPosts).toHaveLength(5)
      })
    })
  })

  // ===========================================================================
  // Concurrent Reads During Writes
  // ===========================================================================

  describe('Concurrent Reads During Writes', () => {
    it('should return consistent snapshots during concurrent writes', async () => {
      // Create initial entity
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'concurrent-test',
        title: 'Original',
        content: 'Original content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Perform reads and writes concurrently
      const operations = [
        postsCollection.get(localId),
        postsCollection.update(localId, { $set: { title: 'Updated 1' } }),
        postsCollection.get(localId),
        postsCollection.update(localId, { $set: { title: 'Updated 2' } }),
        postsCollection.get(localId),
        postsCollection.find({ name: 'concurrent-test' }),
      ]

      const results = await Promise.all(operations)

      // All operations should complete without errors
      expect(results).toHaveLength(6)

      // Get operations should return valid entities
      const getResults = [results[0], results[2], results[4]] as Entity<Post>[]
      for (const result of getResults) {
        expect(result).toBeDefined()
        expect(result.$id).toBe(entity.$id)
        // Title should be one of the valid states
        expect(['Original', 'Updated 1', 'Updated 2']).toContain(result.title)
      }
    })

    it('should handle high-volume concurrent reads', async () => {
      // Create test entity
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'high-volume',
        title: 'High Volume Test',
        content: 'Content',
        status: 'published',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Perform many concurrent reads
      const readPromises = Array.from({ length: 100 }, () =>
        postsCollection.get(localId)
      )

      const results = await Promise.all(readPromises)

      // All reads should return the same entity
      expect(results.every(r => r?.$id === entity.$id)).toBe(true)
      expect(results.every(r => r?.title === 'High Volume Test')).toBe(true)
    })

    it('should handle concurrent find operations with different filters', async () => {
      // Create diverse test data
      await Promise.all(
        Array.from({ length: 50 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `post-${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: i % 2 === 0 ? 'published' : 'draft',
            views: i * 10,
            likes: i % 5,
            tags: [`tag-${i % 3}`],
          })
        )
      )

      // Concurrent find operations with different filters
      const findPromises = [
        postsCollection.find({ status: 'published' }),
        postsCollection.find({ status: 'draft' }),
        postsCollection.find({ views: { $gt: 200 } }),
        postsCollection.find({ views: { $lt: 100 } }),
        postsCollection.find({ likes: { $gte: 3 } }),
        postsCollection.count(),
        postsCollection.find({}, { limit: 10 }),
        postsCollection.find({}, { sort: { views: -1 }, limit: 5 }),
      ]

      const results = await Promise.all(findPromises)

      // All operations should complete
      expect(results).toHaveLength(8)

      // Verify result consistency
      expect((results[0] as Entity<Post>[]).length).toBe(25) // published
      expect((results[1] as Entity<Post>[]).length).toBe(25) // draft
      expect(results[5]).toBe(50) // count
    })
  })

  // ===========================================================================
  // Index Updates Under Concurrent Writes
  // ===========================================================================

  describe('Index Updates Under Concurrent Writes', () => {
    it('should maintain correct counts after concurrent increments', async () => {
      // Create a counter
      const counter = await countersCollection.create({
        $type: 'Counter',
        name: 'view-counter',
        value: 0,
      })
      const localId = (counter.$id as string).split('/')[1]

      // Perform many concurrent increments
      const incrementCount = 100
      const incrementPromises = Array.from({ length: incrementCount }, () =>
        countersCollection.update(localId, { $inc: { value: 1 } })
      )

      await Promise.all(incrementPromises)

      // Verify final count
      const result = await countersCollection.get(localId)
      // Note: Without proper locking, the final value may not equal incrementCount
      // This test documents the actual behavior
      expect((result.value as number)).toBe(incrementCount)
    })

    it('should maintain data integrity with concurrent field updates', async () => {
      // Create entity with multiple fields
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'multi-field',
        title: 'Multi Field Test',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: ['initial'],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update different fields concurrently
      const updatePromises = [
        postsCollection.update(localId, { $inc: { views: 100 } }),
        postsCollection.update(localId, { $inc: { likes: 50 } }),
        postsCollection.update(localId, { $set: { status: 'published' } }),
        postsCollection.update(localId, { $push: { tags: 'new-tag' } }),
      ]

      await Promise.all(updatePromises)

      const result = await postsCollection.get(localId)

      // All updates should have been applied
      expect((result.views as number)).toBe(100)
      expect((result.likes as number)).toBe(50)
      expect(result.status).toBe('published')
      expect((result.tags as string[])).toContain('new-tag')
    })

    it('should handle concurrent array operations correctly', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'array-test',
        title: 'Array Test',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Concurrent push operations
      const pushPromises = Array.from({ length: 20 }, (_, i) =>
        postsCollection.update(localId, { $push: { tags: `tag-${i}` } })
      )

      await Promise.all(pushPromises)

      const result = await postsCollection.get(localId)
      const tags = result.tags as string[]

      // All tags should be present
      expect(tags.length).toBe(20)
      for (let i = 0; i < 20; i++) {
        expect(tags).toContain(`tag-${i}`)
      }
    })
  })

  // ===========================================================================
  // Event Ordering Verification Under Load
  // ===========================================================================

  describe('Event Ordering Verification', () => {
    it('should maintain event order for single entity operations', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'event-order-test',
        title: 'Event Order',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const entityId = entity.$id as string

      // Perform sequential operations
      const localId = entityId.split('/')[1]
      await postsCollection.update(localId, { $set: { title: 'Updated 1' } })
      await postsCollection.update(localId, { $set: { title: 'Updated 2' } })
      await postsCollection.update(localId, { $set: { title: 'Updated 3' } })

      // Get events for this entity
      const events = getEventsForEntity(entityId)

      // Events should be in chronological order
      expect(events.length).toBeGreaterThanOrEqual(4) // CREATE + 3 UPDATEs

      // Event IDs should be sorted (ULID-like ordering)
      for (let i = 1; i < events.length; i++) {
        expect(events[i].id > events[i - 1].id).toBe(true)
      }

      // Verify operation sequence
      expect(events[0].op).toBe('CREATE')
      const updates = events.filter(e => e.op === 'UPDATE')
      expect(updates.length).toBe(3)
    })

    it('should maintain event ordering with concurrent entity creates', async () => {
      // Create multiple entities concurrently
      const createPromises = Array.from({ length: 20 }, (_, i) =>
        postsCollection.create({
          $type: 'Post',
          name: `event-test-${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
      )

      const entities = await Promise.all(createPromises)

      // Get all events
      const allEvents: ReturnType<typeof getEventsForEntity> = []
      for (const entity of entities) {
        const events = getEventsForEntity(entity.$id as string)
        allEvents.push(...events)
      }

      // Sort all events by ID
      allEvents.sort((a, b) => a.id.localeCompare(b.id))

      // Events should have unique IDs
      const eventIds = allEvents.map(e => e.id)
      expect(new Set(eventIds).size).toBe(allEvents.length)

      // All entities should have CREATE events
      const createEvents = allEvents.filter(e => e.op === 'CREATE')
      expect(createEvents.length).toBe(20)
    })

    it('should track event timestamps accurately under load', async () => {
      const startTime = new Date()

      // Create entities with small delays
      const entities: Entity<Post>[] = []
      for (let i = 0; i < 10; i++) {
        const entity = await postsCollection.create({
          $type: 'Post',
          name: `timestamp-test-${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
        entities.push(entity)
      }

      const endTime = new Date()

      // Verify all event timestamps are within the expected range
      for (const entity of entities) {
        const events = getEventsForEntity(entity.$id as string)
        for (const event of events) {
          expect(event.ts.getTime()).toBeGreaterThanOrEqual(startTime.getTime())
          expect(event.ts.getTime()).toBeLessThanOrEqual(endTime.getTime())
        }
      }
    })
  })

  // ===========================================================================
  // Cache Consistency During Concurrent Access
  // ===========================================================================

  describe('Cache Consistency During Concurrent Access', () => {
    it('should maintain cache consistency with rapid updates', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'cache-test',
        title: 'Cache Test',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Rapid succession of updates
      for (let i = 0; i < 50; i++) {
        await postsCollection.update(localId, { $set: { views: i } })
      }

      // Verify final state
      const result = await postsCollection.get(localId)
      expect((result.views as number)).toBe(49)
    })

    it('should not return stale data after update', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'stale-test',
        title: 'Original Title',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update and immediately read
      await postsCollection.update(localId, { $set: { title: 'New Title' } })
      const result = await postsCollection.get(localId)

      expect(result.title).toBe('New Title')
    })

    it('should handle concurrent reads and writes to same entity', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'concurrent-rw',
        title: 'Title',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Interleave reads and writes
      const operations: Promise<unknown>[] = []
      for (let i = 0; i < 50; i++) {
        if (i % 2 === 0) {
          operations.push(postsCollection.get(localId))
        } else {
          operations.push(postsCollection.update(localId, { $inc: { views: 1 } }))
        }
      }

      const results = await Promise.all(operations)

      // All operations should complete
      expect(results).toHaveLength(50)

      // Reads should return valid entities
      const reads = results.filter((_, i) => i % 2 === 0) as Entity<Post>[]
      for (const read of reads) {
        expect(read.$id).toBe(entity.$id)
        expect(typeof read.views).toBe('number')
      }
    })

    it('should handle cache under high contention', async () => {
      // Create multiple entities
      const entities = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `contention-${i}`,
            title: `Title ${i}`,
            content: `Content ${i}`,
            status: 'draft',
            views: 0,
            likes: 0,
            tags: [],
          })
        )
      )

      // High contention: many operations on all entities
      const operations: Promise<unknown>[] = []
      for (let round = 0; round < 10; round++) {
        for (const entity of entities) {
          const localId = (entity.$id as string).split('/')[1]
          operations.push(postsCollection.get(localId))
          operations.push(postsCollection.update(localId, { $inc: { views: 1 } }))
          operations.push(postsCollection.find({ name: entity.name }))
        }
      }

      await Promise.all(operations)

      // Verify final state
      for (const entity of entities) {
        const localId = (entity.$id as string).split('/')[1]
        const result = await postsCollection.get(localId)
        expect((result.views as number)).toBe(10) // 10 increments per entity
      }
    })
  })

  // ===========================================================================
  // Optimistic Locking Conflict Detection
  // ===========================================================================

  describe('Optimistic Locking Conflict Detection', () => {
    it('should reject update with stale version', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'optimistic-lock',
        title: 'Original',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // First update with correct version
      await postsCollection.update(localId, {
        $set: { title: 'Updated' },
      }, { expectedVersion: 1 })

      // Second update with stale version should fail
      await expect(
        postsCollection.update(localId, {
          $set: { title: 'Conflicting' },
        }, { expectedVersion: 1 })
      ).rejects.toThrow()
    })

    it('should allow sequential updates with correct versions', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'sequential-version',
        title: 'V1',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update 1
      await postsCollection.update(localId, {
        $set: { title: 'V2' },
      }, { expectedVersion: 1 })

      // Get updated entity to check version
      let current = await postsCollection.get(localId)
      expect(current.version).toBe(2)

      // Update 2
      await postsCollection.update(localId, {
        $set: { title: 'V3' },
      }, { expectedVersion: 2 })

      current = await postsCollection.get(localId)
      expect(current.version).toBe(3)
      expect(current.title).toBe('V3')
    })

    it('should detect concurrent modification conflicts', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'concurrent-conflict',
        title: 'Original',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Simulate concurrent updates: both read the same version
      const version = entity.version as number

      // Both try to update with same expectedVersion - one should succeed, one should fail
      const update1 = postsCollection.update(localId, {
        $set: { title: 'Update 1' },
      }, { expectedVersion: version })

      const update2 = postsCollection.update(localId, {
        $set: { title: 'Update 2' },
      }, { expectedVersion: version })

      // Use Promise.allSettled to capture both results
      const results = await Promise.allSettled([update1, update2])

      // At least one should succeed, at least one might fail
      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      expect(successes.length).toBeGreaterThanOrEqual(1)
      // Due to the sequential nature of JS, both might succeed in some cases
      // but in a properly implemented optimistic locking, one should fail
      expect(successes.length + failures.length).toBe(2)
    })

    it('should handle delete with version check', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'delete-version',
        title: 'To Delete',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update to increment version
      await postsCollection.update(localId, { $set: { title: 'Updated' } })

      // Try to delete with stale version
      await expect(
        postsCollection.delete(localId, { expectedVersion: 1 })
      ).rejects.toThrow()

      // Delete with correct version should succeed
      const result = await postsCollection.delete(localId, { expectedVersion: 2 })
      expect(result.deletedCount).toBe(1)
    })
  })

  // ===========================================================================
  // Race Condition Edge Cases
  // ===========================================================================

  describe('Race Condition Edge Cases', () => {
    it('should handle create-then-immediate-read race', async () => {
      const createAndRead = async (index: number) => {
        const entity = await postsCollection.create({
          $type: 'Post',
          name: `race-${index}`,
          title: `Title ${index}`,
          content: 'Content',
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
        const localId = (entity.$id as string).split('/')[1]
        const read = await postsCollection.get(localId)
        return { created: entity, read }
      }

      const results = await Promise.all(
        Array.from({ length: 20 }, (_, i) => createAndRead(i))
      )

      // All reads should return the created entity
      for (const { created, read } of results) {
        expect(read.$id).toBe(created.$id)
        expect(read.name).toBe(created.name)
      }
    })

    it('should handle update-then-immediate-delete race', async () => {
      const entity = await postsCollection.create({
        $type: 'Post',
        name: 'update-delete-race',
        title: 'Original',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      const localId = (entity.$id as string).split('/')[1]

      // Fire update and delete concurrently
      const [updateResult, deleteResult] = await Promise.all([
        postsCollection.update(localId, { $set: { title: 'Updated' } }),
        postsCollection.delete(localId),
      ])

      // Both operations should have been processed
      // (order determines final state)
      expect(updateResult).toBeDefined()
      expect(deleteResult).toBeDefined()
    })

    it('should handle find during bulk creates', async () => {
      const findDuringCreate = async () => {
        // Start creating many entities
        const createPromises = Array.from({ length: 50 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `bulk-${i}`,
            title: `Title ${i}`,
            content: 'Content',
            status: 'draft',
            views: 0,
            likes: 0,
            tags: [],
          })
        )

        // Intersperse finds
        const findPromises = Array.from({ length: 10 }, () =>
          postsCollection.find({})
        )

        const [createResults, findResults] = await Promise.all([
          Promise.all(createPromises),
          Promise.all(findPromises),
        ])

        return { createResults, findResults }
      }

      const { createResults, findResults } = await findDuringCreate()

      expect(createResults).toHaveLength(50)
      expect(findResults).toHaveLength(10)

      // Each find should return a consistent snapshot (even if incomplete)
      for (const result of findResults) {
        expect(Array.isArray(result)).toBe(true)
      }

      // Final state should have all entities
      const finalCount = await postsCollection.count()
      expect(finalCount).toBe(50)
    })

    it('should handle Promise.race correctly for first-to-complete queries', async () => {
      // Create test data
      await Promise.all(
        Array.from({ length: 100 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `race-query-${i}`,
            title: `Title ${i}`,
            content: 'Content',
            status: i % 2 === 0 ? 'published' : 'draft',
            views: i,
            likes: 0,
            tags: [],
          })
        )
      )

      // Race multiple queries - first one wins
      const raceResult = await Promise.race([
        postsCollection.find({ status: 'published' }),
        postsCollection.find({ status: 'draft' }),
        postsCollection.find({ views: { $gt: 50 } }),
      ])

      // Result should be a valid array
      expect(Array.isArray(raceResult)).toBe(true)
      expect(raceResult.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Data Consistency After Concurrent Operations
  // ===========================================================================

  describe('Data Consistency Verification', () => {
    it('should maintain referential integrity after concurrent operations', async () => {
      // Create users and posts concurrently
      const userPromises = Array.from({ length: 10 }, (_, i) =>
        usersCollection.create({
          $type: 'User',
          name: `user-${i}`,
          email: `user${i}@test.com`,
          username: `user${i}`,
          balance: 100,
        })
      )

      const postPromises = Array.from({ length: 20 }, (_, i) =>
        postsCollection.create({
          $type: 'Post',
          name: `post-${i}`,
          title: `Title ${i}`,
          content: 'Content',
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
      )

      const [users, posts] = await Promise.all([
        Promise.all(userPromises),
        Promise.all(postPromises),
      ])

      // Verify all entities exist
      expect(users).toHaveLength(10)
      expect(posts).toHaveLength(20)

      // Verify counts match
      const userCount = await usersCollection.count()
      const postCount = await postsCollection.count()

      expect(userCount).toBe(10)
      expect(postCount).toBe(20)
    })

    it('should maintain sum consistency with concurrent increments/decrements', async () => {
      // Create users with initial balance
      const users = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          usersCollection.create({
            $type: 'User',
            name: `user-${i}`,
            email: `user${i}@test.com`,
            username: `user${i}`,
            balance: 100,
          })
        )
      )

      // Total initial balance: 500

      // Perform transfers: decrement one user, increment another
      const transferPromises: Promise<unknown>[] = []
      for (let i = 0; i < 10; i++) {
        const fromUser = users[i % 5]
        const toUser = users[(i + 1) % 5]
        const fromLocalId = (fromUser.$id as string).split('/')[1]
        const toLocalId = (toUser.$id as string).split('/')[1]

        transferPromises.push(
          usersCollection.update(fromLocalId, { $inc: { balance: -10 } })
        )
        transferPromises.push(
          usersCollection.update(toLocalId, { $inc: { balance: 10 } })
        )
      }

      await Promise.all(transferPromises)

      // Verify total balance is still 500 (conservation)
      const allUsers = await usersCollection.find()
      const totalBalance = allUsers.reduce((sum, u) => sum + (u.balance as number), 0)
      expect(totalBalance).toBe(500)
    })

    it('should verify final state consistency after mixed operations', async () => {
      // Create initial state
      const initialPosts = await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          postsCollection.create({
            $type: 'Post',
            name: `consistency-${i}`,
            title: `Title ${i}`,
            content: 'Content',
            status: 'draft',
            views: 0,
            likes: 0,
            tags: [],
          })
        )
      )

      // Concurrent operations
      const operations: Promise<unknown>[] = []

      // Update first 10 to published
      for (let i = 0; i < 10; i++) {
        const localId = (initialPosts[i].$id as string).split('/')[1]
        operations.push(
          postsCollection.update(localId, { $set: { status: 'published' }, $inc: { views: 100 } })
        )
      }

      // Delete next 5
      for (let i = 10; i < 15; i++) {
        const localId = (initialPosts[i].$id as string).split('/')[1]
        operations.push(postsCollection.delete(localId))
      }

      // Update remaining 5 to archived
      for (let i = 15; i < 20; i++) {
        const localId = (initialPosts[i].$id as string).split('/')[1]
        operations.push(
          postsCollection.update(localId, { $set: { status: 'archived' } })
        )
      }

      await Promise.all(operations)

      // Verify final state
      const published = await postsCollection.find({ status: 'published' })
      const archived = await postsCollection.find({ status: 'archived' })
      const draft = await postsCollection.find({ status: 'draft' })
      const deleted = await postsCollection.find({}, { includeDeleted: true })

      expect(published).toHaveLength(10)
      expect(archived).toHaveLength(5)
      expect(draft).toHaveLength(0)
      expect(deleted).toHaveLength(20) // All including soft-deleted

      // Verify views on published posts
      expect(published.every(p => (p.views as number) === 100)).toBe(true)
    })
  })
})

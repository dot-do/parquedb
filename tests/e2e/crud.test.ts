/**
 * E2E CRUD Operations Tests via RPC
 *
 * Tests the full flow: RPC -> Worker -> Durable Object -> R2
 * Validates that all CRUD operations work correctly through service bindings.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  getTestClient,
  cleanupTestData,
  waitForConsistency,
  generateTestId,
  assertAuditFields,
  type ParqueDBClient,
  type Post,
  type User,
} from './setup'

describe('CRUD Operations via RPC', () => {
  let client: ParqueDBClient

  beforeEach(async () => {
    await cleanupTestData()
    client = getTestClient()
  })

  afterEach(async () => {
    await cleanupTestData()
  })

  // ===========================================================================
  // Create Operations
  // ===========================================================================

  describe('Create', () => {
    it('creates entity through DO and returns with $id', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'My First Post',
        title: 'Test Post',
        content: 'Hello World',
      })

      // Verify ID format: namespace/id
      expect(post.$id).toMatch(/^posts\//)
      expect(post.$type).toBe('Post')
      expect(post.name).toBe('My First Post')
      expect(post.title).toBe('Test Post')
      expect(post.content).toBe('Hello World')
    })

    it('creates entity with audit fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Audited Post',
        title: 'Audited',
      })

      // Verify audit fields are present
      expect(post.createdAt).toBeInstanceOf(Date)
      expect(post.updatedAt).toBeInstanceOf(Date)
      expect(post.version).toBe(1)

      // createdAt and updatedAt should be the same for new entities
      expect(post.createdAt.getTime()).toBe(post.updatedAt.getTime())
    })

    it('creates entity with all data fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Full Post',
        title: 'Complete Post',
        content: 'Full content here',
        status: 'draft',
        tags: ['tech', 'tutorial'],
        viewCount: 0,
      })

      expect(post.title).toBe('Complete Post')
      expect(post.content).toBe('Full content here')
      expect(post.status).toBe('draft')
      expect(post.tags).toEqual(['tech', 'tutorial'])
      expect(post.viewCount).toBe(0)
    })

    it('creates multiple entities with unique IDs', async () => {
      const post1 = await client.Posts.create({
        $type: 'Post',
        name: 'Post 1',
        title: 'First',
      })

      const post2 = await client.Posts.create({
        $type: 'Post',
        name: 'Post 2',
        title: 'Second',
      })

      expect(post1.$id).not.toBe(post2.$id)
      expect(post1.title).toBe('First')
      expect(post2.title).toBe('Second')
    })

    it('creates entities in different collections', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'A Post',
        title: 'Post Title',
      })

      const user = await client.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      expect(post.$id).toMatch(/^posts\//)
      expect(user.$id).toMatch(/^users\//)
      expect(post.$type).toBe('Post')
      expect(user.$type).toBe('User')
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('Read', () => {
    it('reads entity from R2 by ID', async () => {
      const created = await client.Posts.create({
        $type: 'Post',
        name: 'Readable Post',
        title: 'Test',
        content: 'Content',
      })

      // Allow time for R2 write to complete
      await waitForConsistency()

      const fetched = await client.Posts.get(created.$id)

      expect(fetched.$id).toBe(created.$id)
      expect(fetched.title).toBe('Test')
      expect(fetched.content).toBe('Content')
    })

    it('reads entity with all fields intact', async () => {
      const created = await client.Posts.create({
        $type: 'Post',
        name: 'Full Entity',
        title: 'Complete',
        content: 'Full content',
        status: 'published',
        tags: ['a', 'b', 'c'],
        viewCount: 42,
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(created.$id)

      // All data should match
      expect(fetched).toEqual(created)
    })

    it('returns null for non-existent entity', async () => {
      const result = await client.Posts.findOne({ $id: 'posts/non-existent-id' })

      expect(result).toBeNull()
    })

    it('finds entities by filter', async () => {
      await client.Posts.create({
        $type: 'Post',
        name: 'Draft Post',
        title: 'Draft',
        status: 'draft',
      })

      await client.Posts.create({
        $type: 'Post',
        name: 'Published Post',
        title: 'Published',
        status: 'published',
      })

      await waitForConsistency()

      const published = await client.Posts.find({ status: 'published' })

      expect(published.length).toBe(1)
      expect(published[0].title).toBe('Published')
      expect(published[0].status).toBe('published')
    })

    it('finds entities with complex filters', async () => {
      await client.Posts.create({
        $type: 'Post',
        name: 'Post A',
        title: 'A',
        viewCount: 10,
      })

      await client.Posts.create({
        $type: 'Post',
        name: 'Post B',
        title: 'B',
        viewCount: 50,
      })

      await client.Posts.create({
        $type: 'Post',
        name: 'Post C',
        title: 'C',
        viewCount: 100,
      })

      await waitForConsistency()

      const highViews = await client.Posts.find({ viewCount: { $gte: 50 } })

      expect(highViews.length).toBe(2)
      expect(highViews.map((p) => p.title).sort()).toEqual(['B', 'C'])
    })

    it('finds with limit and sort', async () => {
      for (let i = 0; i < 5; i++) {
        await client.Posts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          viewCount: i * 10,
        })
      }

      await waitForConsistency()

      const result = await client.Posts.find(
        {},
        {
          sort: { viewCount: -1 },
          limit: 3,
        }
      )

      expect(result.length).toBe(3)
      expect(result[0].viewCount).toBe(40) // Highest first
      expect(result[1].viewCount).toBe(30)
      expect(result[2].viewCount).toBe(20)
    })

    it('counts entities', async () => {
      await client.Posts.create({ $type: 'Post', name: 'Post 1', title: 'P1' })
      await client.Posts.create({ $type: 'Post', name: 'Post 2', title: 'P2' })
      await client.Posts.create({ $type: 'Post', name: 'Post 3', title: 'P3' })

      await waitForConsistency()

      const count = await client.Posts.count()

      expect(count).toBe(3)
    })

    it('counts entities with filter', async () => {
      await client.Posts.create({ $type: 'Post', name: 'Draft 1', title: 'D1', status: 'draft' })
      await client.Posts.create({ $type: 'Post', name: 'Draft 2', title: 'D2', status: 'draft' })
      await client.Posts.create({ $type: 'Post', name: 'Published', title: 'P1', status: 'published' })

      await waitForConsistency()

      const draftCount = await client.Posts.count({ status: 'draft' })

      expect(draftCount).toBe(2)
    })

    it('checks entity existence', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Existing',
        title: 'Exists',
      })

      await waitForConsistency()

      const exists = await client.Posts.exists(post.$id)
      const notExists = await client.Posts.exists('posts/fake-id')

      expect(exists).toBe(true)
      expect(notExists).toBe(false)
    })
  })

  // ===========================================================================
  // Update Operations
  // ===========================================================================

  describe('Update', () => {
    it('updates entity through DO with $set', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Original',
        title: 'Original Title',
      })

      const updated = await client.Posts.update(post.$id, {
        $set: { title: 'Updated Title' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Updated Title')
      expect(updated!.name).toBe('Original') // Unchanged field
    })

    it('updates entity and increments version', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Versioned',
        title: 'V1',
      })

      expect(post.version).toBe(1)

      const updated = await client.Posts.update(post.$id, {
        $set: { title: 'V2' },
      })

      expect(updated!.version).toBe(2)
    })

    it('updates multiple fields at once', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Multi-field',
        title: 'Original',
        status: 'draft',
        viewCount: 0,
      })

      const updated = await client.Posts.update(post.$id, {
        $set: {
          title: 'Updated',
          status: 'published',
          viewCount: 100,
        },
      })

      expect(updated!.title).toBe('Updated')
      expect(updated!.status).toBe('published')
      expect(updated!.viewCount).toBe(100)
    })

    it('updates with $inc for numeric fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Counter',
        title: 'Counter Post',
        viewCount: 10,
      })

      const updated = await client.Posts.update(post.$id, {
        $inc: { viewCount: 5 },
      })

      expect(updated!.viewCount).toBe(15)
    })

    it('updates with $push for array fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Tagged',
        title: 'Tagged Post',
        tags: ['original'],
      })

      const updated = await client.Posts.update(post.$id, {
        $push: { tags: 'new-tag' },
      })

      expect(updated!.tags).toEqual(['original', 'new-tag'])
    })

    it('updates with $pull for array fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Remove Tag',
        title: 'Pull Test',
        tags: ['keep', 'remove', 'also-keep'],
      })

      const updated = await client.Posts.update(post.$id, {
        $pull: { tags: 'remove' },
      })

      expect(updated!.tags).toEqual(['keep', 'also-keep'])
    })

    it('updates with $unset to remove fields', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Unset Test',
        title: 'Remove Field',
        content: 'This will be removed',
      })

      const updated = await client.Posts.update(post.$id, {
        $unset: { content: true },
      })

      expect(updated!.content).toBeUndefined()
    })

    it('updates updatedAt timestamp', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Timestamp',
        title: 'Timestamp Test',
      })

      const originalUpdatedAt = post.updatedAt

      vi.useFakeTimers()
      try {
        // Advance time deterministically
        vi.advanceTimersByTime(10)

        const updated = await client.Posts.update(post.$id, {
          $set: { title: 'Changed' },
        })

        expect(updated!.updatedAt.getTime()).toBeGreaterThan(originalUpdatedAt.getTime())
      } finally {
        vi.useRealTimers()
      }
    })

    it('returns null when updating non-existent entity', async () => {
      const result = await client.Posts.update('posts/non-existent', {
        $set: { title: 'Does not exist' },
      })

      expect(result).toBeNull()
    })

    it('supports optimistic concurrency with expectedVersion', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Concurrent',
        title: 'Concurrency Test',
      })

      // First update should succeed
      const updated1 = await client.Posts.update(
        post.$id,
        { $set: { title: 'Update 1' } },
        { expectedVersion: 1 }
      )

      expect(updated1!.version).toBe(2)

      // Second update with wrong version should fail
      await expect(
        client.Posts.update(post.$id, { $set: { title: 'Update 2' } }, { expectedVersion: 1 })
      ).rejects.toThrow()
    })
  })

  // ===========================================================================
  // Delete Operations
  // ===========================================================================

  describe('Delete', () => {
    it('soft deletes entity by default', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Deletable',
        title: 'To Delete',
      })

      const result = await client.Posts.delete(post.$id)

      expect(result.deletedCount).toBe(1)

      // Soft deleted entity should not be found by default
      const found = await client.Posts.findOne({ $id: post.$id })
      expect(found).toBeNull()

      // But should be found with includeDeleted
      const foundDeleted = await client.Posts.findOne(
        { $id: post.$id },
        { includeDeleted: true }
      )
      expect(foundDeleted).not.toBeNull()
      expect(foundDeleted!.deletedAt).toBeInstanceOf(Date)
    })

    it('hard deletes entity when specified', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Hard Delete',
        title: 'Permanent Delete',
      })

      const result = await client.Posts.delete(post.$id, { hard: true })

      expect(result.deletedCount).toBe(1)

      // Hard deleted entity should not exist at all
      const exists = await client.Posts.exists(post.$id)
      expect(exists).toBe(false)

      // Should not be found even with includeDeleted
      const found = await client.Posts.findOne(
        { $id: post.$id },
        { includeDeleted: true }
      )
      expect(found).toBeNull()
    })

    it('returns 0 when deleting non-existent entity', async () => {
      const result = await client.Posts.delete('posts/non-existent')

      expect(result.deletedCount).toBe(0)
    })

    it('deletes multiple entities with filter', async () => {
      await client.Posts.create({ $type: 'Post', name: 'Draft 1', title: 'D1', status: 'draft' })
      await client.Posts.create({ $type: 'Post', name: 'Draft 2', title: 'D2', status: 'draft' })
      await client.Posts.create({ $type: 'Post', name: 'Published', title: 'P1', status: 'published' })

      await waitForConsistency()

      const result = await client.Posts.deleteMany({ status: 'draft' })

      expect(result.deletedCount).toBe(2)

      // Only published should remain
      const remaining = await client.Posts.find()
      expect(remaining.length).toBe(1)
      expect(remaining[0].status).toBe('published')
    })
  })

  // ===========================================================================
  // Edge Cases and Error Handling
  // ===========================================================================

  describe('Edge Cases', () => {
    it('handles special characters in content', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Special Chars',
        title: 'Title with "quotes" and \'apostrophes\'',
        content: 'Content with <html> & special chars: \u00e9\u00e0\u00fc \ud83d\ude00',
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)

      expect(fetched.title).toBe('Title with "quotes" and \'apostrophes\'')
      expect(fetched.content).toBe('Content with <html> & special chars: \u00e9\u00e0\u00fc \ud83d\ude00')
    })

    it('handles large content', async () => {
      const largeContent = 'x'.repeat(10000)

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Large Post',
        title: 'Large Content',
        content: largeContent,
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)

      expect(fetched.content).toBe(largeContent)
      expect(fetched.content!.length).toBe(10000)
    })

    it('handles null and undefined values', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Nullable',
        title: 'Null Test',
        content: undefined,
        status: undefined,
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)

      expect(fetched.content).toBeUndefined()
      expect(fetched.status).toBeUndefined()
    })

    it('handles rapid create-read-update-delete', async () => {
      // Create
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Rapid',
        title: 'Rapid Test',
      })

      // Read immediately
      const read1 = await client.Posts.get(post.$id)
      expect(read1.title).toBe('Rapid Test')

      // Update immediately
      const updated = await client.Posts.update(post.$id, {
        $set: { title: 'Updated Rapid' },
      })
      expect(updated!.title).toBe('Updated Rapid')

      // Read again
      const read2 = await client.Posts.get(post.$id)
      expect(read2.title).toBe('Updated Rapid')

      // Delete
      const deleted = await client.Posts.delete(post.$id, { hard: true })
      expect(deleted.deletedCount).toBe(1)

      // Should be gone
      const exists = await client.Posts.exists(post.$id)
      expect(exists).toBe(false)
    })
  })
})

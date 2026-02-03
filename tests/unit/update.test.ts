/**
 * Update Operation Tests
 *
 * Tests for MongoDB-style update operators.
 * Tests cover $set, $unset, $inc, $push, $pull, optimistic concurrency,
 * and returnDocument options.
 *
 * Uses real MemoryBackend for storage instead of mocks.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type {
  EntityId,
  UpdateInput,
  UpdateOptions,
  Entity,
} from '../../src/types'
import {
  createEntityId,
  createTestEntity,
  createAuditFields,
} from '../factories'

// =============================================================================
// Test Suite
// =============================================================================

describe('Update Operations', () => {
  let db: ParqueDB
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    db = new ParqueDB({ storage })
  })

  // ===========================================================================
  // $set operator
  // ===========================================================================

  describe('$set operator', () => {
    it('sets scalar fields', async () => {
      // Create an entity first
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Original content',
        status: 'draft',
      })

      // Update with $set
      const updated = await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title', status: 'published' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Updated Title')
      expect(updated!.status).toBe('published')
      // Other fields should remain unchanged
      expect(updated!.content).toBe('Original content')
    })

    it('sets nested fields with dot notation', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        metadata: {
          readTime: 5,
          wordCount: 1000,
          author: {
            name: 'John',
            verified: false,
          },
        },
      })

      const updated = await db.update('posts', entity.$id as string, {
        $set: {
          'metadata.readTime': 10,
          'metadata.author.verified': true,
        },
      })

      expect(updated).not.toBeNull()
      expect((updated!.metadata as any).readTime).toBe(10)
      expect((updated!.metadata as any).wordCount).toBe(1000) // unchanged
      expect((updated!.metadata as any).author.verified).toBe(true)
      expect((updated!.metadata as any).author.name).toBe('John') // unchanged
    })

    it('sets array fields', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['old', 'tags'],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $set: { tags: ['new', 'updated', 'tags'] },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['new', 'updated', 'tags'])
    })

    it('creates fields that do not exist', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $set: { newField: 'new value', nested: { deep: 'value' } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.newField).toBe('new value')
      expect((updated!.nested as any).deep).toBe('value')
    })
  })

  // ===========================================================================
  // $unset operator
  // ===========================================================================

  describe('$unset operator', () => {
    it('removes fields', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        description: 'A description to remove',
        metadata: { key: 'value' },
      })

      const updated = await db.update('posts', entity.$id as string, {
        $unset: { description: '', metadata: 1 },
      })

      expect(updated).not.toBeNull()
      expect(updated!.description).toBeUndefined()
      expect(updated!.metadata).toBeUndefined()
      // Other fields should remain
      expect(updated!.title).toBe('Title')
    })

    it('handles non-existent fields gracefully', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Should not throw when trying to unset fields that don't exist
      const updated = await db.update('posts', entity.$id as string, {
        $unset: { nonExistentField: '', anotherMissing: true },
      })

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Title') // original data preserved
    })

    it('supports multiple unset value formats', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        field1: 'value1',
        field2: 'value2',
        field3: 'value3',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $unset: {
          field1: '',     // empty string
          field2: 1,      // number 1
          field3: true,   // boolean true
        },
      })

      expect(updated).not.toBeNull()
      expect(updated!.field1).toBeUndefined()
      expect(updated!.field2).toBeUndefined()
      expect(updated!.field3).toBeUndefined()
    })
  })

  // ===========================================================================
  // $inc operator
  // ===========================================================================

  describe('$inc operator', () => {
    it('increments numeric fields', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        viewCount: 10,
        likeCount: 5,
      })

      const updated = await db.update('posts', entity.$id as string, {
        $inc: { viewCount: 1, likeCount: 3 },
      })

      expect(updated).not.toBeNull()
      expect(updated!.viewCount).toBe(11)
      expect(updated!.likeCount).toBe(8)
    })

    it('throws on non-numeric fields', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        status: 'draft',
      })

      await expect(
        db.update('posts', entity.$id as string, {
          $inc: { status: 1 } as any, // status is a string, should throw
        })
      ).rejects.toThrow()
    })

    it('handles negative increments', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        score: 100,
      })

      const updated = await db.update('posts', entity.$id as string, {
        $inc: { score: -25 },
      })

      expect(updated).not.toBeNull()
      expect(updated!.score).toBe(75)
    })

    it('creates field if it does not exist', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $inc: { viewCount: 1 },
      })

      expect(updated).not.toBeNull()
      expect(updated!.viewCount).toBe(1)
    })

    it('handles float increments', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        rating: 4.0,
      })

      const updated = await db.update('posts', entity.$id as string, {
        $inc: { rating: 0.5 },
      })

      expect(updated).not.toBeNull()
      expect(updated!.rating).toBe(4.5)
    })
  })

  // ===========================================================================
  // $push operator
  // ===========================================================================

  describe('$push operator', () => {
    it('appends to array', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['existing', 'tags'],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $push: { tags: 'new-tag' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['existing', 'tags', 'new-tag'])
    })

    it('creates array if not exists', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $push: { tags: 'first-tag' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['first-tag'])
    })

    it('supports $each modifier', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['existing'],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $push: { tags: { $each: ['tag1', 'tag2', 'tag3'] } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['existing', 'tag1', 'tag2', 'tag3'])
    })

    it('supports $position modifier', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['first', 'last'],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $push: { tags: { $each: ['middle'], $position: 1 } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['first', 'middle', 'last'])
    })

    it('supports $slice modifier', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        recentViews: [1, 2, 3],
      })

      // Push and keep only last 3 elements
      const updated = await db.update('posts', entity.$id as string, {
        $push: { recentViews: { $each: [4, 5], $slice: -3 } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.recentViews).toEqual([3, 4, 5])
    })

    it('supports $sort modifier', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        scores: [5, 2, 8],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $push: { scores: { $each: [1, 9], $sort: 1 } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.scores).toEqual([1, 2, 5, 8, 9])
    })
  })

  // ===========================================================================
  // $pull operator
  // ===========================================================================

  describe('$pull operator', () => {
    it('removes matching elements', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        tags: ['keep', 'remove', 'also-keep', 'remove'],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $pull: { tags: 'remove' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.tags).toEqual(['keep', 'also-keep'])
    })

    it('supports filter conditions', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        scores: [10, 25, 50, 75, 100],
      })

      // Remove all scores less than 30
      const updated = await db.update('posts', entity.$id as string, {
        $pull: { scores: { $lt: 30 } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.scores).toEqual([50, 75, 100])
    })

    it('handles non-existent array gracefully', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $pull: { nonExistentArray: 'value' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.nonExistentArray).toBeUndefined()
    })

    it('removes objects matching filter', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        comments: [
          { id: 1, text: 'Good', spam: false },
          { id: 2, text: 'Buy now!', spam: true },
          { id: 3, text: 'Nice post', spam: false },
        ],
      })

      const updated = await db.update('posts', entity.$id as string, {
        $pull: { comments: { spam: true } },
      })

      expect(updated).not.toBeNull()
      expect(updated!.comments).toHaveLength(2)
      expect((updated!.comments as any[]).every(c => !c.spam)).toBe(true)
    })
  })

  // ===========================================================================
  // Optimistic concurrency
  // ===========================================================================

  describe('optimistic concurrency', () => {
    it('succeeds when version matches', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      expect(entity.version).toBe(1)

      const updated = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'Updated Title' } },
        { expectedVersion: 1 }
      )

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Updated Title')
      expect(updated!.version).toBe(2)
    })

    it('throws VersionConflictError when version mismatch', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Simulate another process updating the entity
      await db.update('posts', entity.$id as string, {
        $set: { title: 'Changed by someone else' },
      })

      // Try to update with stale version
      await expect(
        db.update(
          'posts',
          entity.$id as string,
          { $set: { title: 'My update' } },
          { expectedVersion: 1 } // This is now stale
        )
      ).rejects.toThrow()

      // When implemented, should throw VersionConflictError
      try {
        await db.update(
          'posts',
          entity.$id as string,
          { $set: { title: 'My update' } },
          { expectedVersion: 1 }
        )
      } catch (error: any) {
        expect(error.name).toBe('VersionConflictError')
        expect(error.expectedVersion).toBe(1)
        expect(error.actualVersion).toBe(2)
      }
    })

    it('increments version on each update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      expect(entity.version).toBe(1)

      const updated1 = await db.update('posts', entity.$id as string, {
        $set: { title: 'Update 1' },
      })
      expect(updated1!.version).toBe(2)

      const updated2 = await db.update('posts', entity.$id as string, {
        $set: { title: 'Update 2' },
      })
      expect(updated2!.version).toBe(3)

      const updated3 = await db.update('posts', entity.$id as string, {
        $set: { title: 'Update 3' },
      })
      expect(updated3!.version).toBe(4)
    })
  })

  // ===========================================================================
  // returnDocument option
  // ===========================================================================

  describe('returnDocument option', () => {
    it('returns document before update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Content',
        viewCount: 10,
      })

      const before = await db.update(
        'posts',
        entity.$id as string,
        {
          $set: { title: 'New Title' },
          $inc: { viewCount: 5 },
        },
        { returnDocument: 'before' }
      )

      expect(before).not.toBeNull()
      expect(before!.title).toBe('Original Title')
      expect(before!.viewCount).toBe(10)
      expect(before!.version).toBe(1)

      // Verify the update actually happened
      const current = await db.get('posts', entity.$id as string)
      expect(current!.title).toBe('New Title')
      expect(current!.viewCount).toBe(15)
    })

    it('returns document after update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Content',
        viewCount: 10,
      })

      const after = await db.update(
        'posts',
        entity.$id as string,
        {
          $set: { title: 'New Title' },
          $inc: { viewCount: 5 },
        },
        { returnDocument: 'after' }
      )

      expect(after).not.toBeNull()
      expect(after!.title).toBe('New Title')
      expect(after!.viewCount).toBe(15)
      expect(after!.version).toBe(2)
    })

    it('defaults to returning document after update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Content',
      })

      // No returnDocument option specified
      const result = await db.update('posts', entity.$id as string, {
        $set: { title: 'New Title' },
      })

      expect(result).not.toBeNull()
      expect(result!.title).toBe('New Title')
      expect(result!.version).toBe(2)
    })
  })

  // ===========================================================================
  // Combined operators
  // ===========================================================================

  describe('combined operators', () => {
    it('applies multiple operators in single update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        status: 'draft',
        viewCount: 0,
        tags: ['initial'],
        toRemove: 'this field',
      })

      const updated = await db.update('posts', entity.$id as string, {
        $set: { status: 'published', publishedAt: new Date() },
        $inc: { viewCount: 1 },
        $push: { tags: 'featured' },
        $unset: { toRemove: '' },
      })

      expect(updated).not.toBeNull()
      expect(updated!.status).toBe('published')
      expect(updated!.publishedAt).toBeInstanceOf(Date)
      expect(updated!.viewCount).toBe(1)
      expect(updated!.tags).toEqual(['initial', 'featured'])
      expect(updated!.toRemove).toBeUndefined()
    })
  })

  // ===========================================================================
  // Actor tracking
  // ===========================================================================

  describe('actor tracking', () => {
    it('sets updatedBy from actor option', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const updated = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'Updated' } },
        { actor: 'users/editor' as EntityId }
      )

      expect(updated).not.toBeNull()
      expect(updated!.updatedBy).toBe('users/editor')
    })

    it('updates updatedAt timestamp', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const originalUpdatedAt = entity.updatedAt

      vi.useFakeTimers()
      try {
        // Advance time deterministically
        vi.advanceTimersByTime(10)

        const updated = await db.update('posts', entity.$id as string, {
          $set: { title: 'Updated' },
        })

        expect(updated).not.toBeNull()
        expect(updated!.updatedAt.getTime()).toBeGreaterThan(
          originalUpdatedAt.getTime()
        )
      } finally {
        vi.useRealTimers()
      }
    })
  })
})

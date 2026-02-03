/**
 * Delete Operation Tests
 *
 * Tests for delete operations including soft delete (default),
 * hard delete, and deleteMany using real storage.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type {
  EntityId,
  Schema,
} from '../../src/types'

// =============================================================================
// Test Schema with Relationships
// =============================================================================

function createTestSchema(): Schema {
  return {
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      status: { type: 'string', default: 'draft' },
      author: '-> User.posts',
      categories: '-> Category.posts[]',
    },
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      posts: '<- Post.author[]',
    },
    Category: {
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      posts: '<- Post.categories[]',
    },
    Comment: {
      $ns: 'comments',
      text: 'string!',
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Delete Operations', () => {
  let db: ParqueDB
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-delete-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // Soft delete (default)
  // ===========================================================================

  describe('soft delete (default)', () => {
    it('sets deletedAt timestamp', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const before = new Date()
      const result = await db.delete('posts', entity.$id as string)
      const after = new Date()

      expect(result.deletedCount).toBe(1)

      // Retrieve with includeDeleted to check deletedAt
      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })

      expect(deleted).not.toBeNull()
      expect(deleted!.deletedAt).toBeInstanceOf(Date)
      expect(deleted!.deletedAt!.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(deleted!.deletedAt!.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('sets deletedBy actor', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const actorId = 'users/admin' as EntityId
      await db.delete('posts', entity.$id as string, { actor: actorId })

      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })

      expect(deleted).not.toBeNull()
      expect(deleted!.deletedBy).toBe(actorId)
    })

    it('excludes from normal queries', async () => {
      // Create multiple posts
      const post1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
        status: 'published',
      })

      const post2 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
        status: 'published',
      })

      const post3 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 3',
        title: 'Title 3',
        content: 'Content 3',
        status: 'published',
      })

      // Delete one post
      await db.delete('posts', post2.$id as string)

      // Normal find should exclude deleted
      const results = await db.find('posts', { status: 'published' })
      expect(results.items).toHaveLength(2)
      expect(results.items.map(p => p.$id)).not.toContain(post2.$id)

      // Normal get should return null
      const getResult = await db.get('posts', post2.$id as string)
      expect(getResult).toBeNull()
    })

    it('includes with includeDeleted option', async () => {
      const post1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
        status: 'published',
      })

      const post2 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
        status: 'published',
      })

      // Delete one post
      await db.delete('posts', post2.$id as string)

      // Find with includeDeleted should include all
      const results = await db.find('posts', { status: 'published' }, {
        includeDeleted: true,
      })
      expect(results.items).toHaveLength(2)

      // Get with includeDeleted should return the entity
      const deleted = await db.get('posts', post2.$id as string, {
        includeDeleted: true,
      })
      expect(deleted).not.toBeNull()
      expect(deleted!.$id).toBe(post2.$id)
      expect(deleted!.deletedAt).toBeInstanceOf(Date)
    })

    it('preserves all original data', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Important Title',
        content: 'Valuable content that should be preserved',
        status: 'published',
        metadata: { key: 'value' },
      })

      await db.delete('posts', entity.$id as string)

      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })

      expect(deleted).not.toBeNull()
      expect(deleted!.title).toBe('Important Title')
      expect(deleted!.content).toBe('Valuable content that should be preserved')
      expect(deleted!.status).toBe('published')
      expect((deleted!.metadata as any).key).toBe('value')
    })

    it('increments version on soft delete', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      expect(entity.version).toBe(1)

      await db.delete('posts', entity.$id as string)

      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })

      expect(deleted!.version).toBe(2)
    })

    it('returns deletedCount 0 for non-existent entity', async () => {
      const result = await db.delete('posts', 'posts/nonexistent')
      expect(result.deletedCount).toBe(0)
    })

    it('returns deletedCount 0 for already deleted entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // First delete
      const result1 = await db.delete('posts', entity.$id as string)
      expect(result1.deletedCount).toBe(1)

      // Second delete - already deleted
      const result2 = await db.delete('posts', entity.$id as string)
      expect(result2.deletedCount).toBe(0)
    })
  })

  // ===========================================================================
  // Hard delete
  // ===========================================================================

  describe('hard delete', () => {
    it('permanently removes entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const result = await db.delete('posts', entity.$id as string, {
        hard: true,
      })

      expect(result.deletedCount).toBe(1)

      // Entity should not exist even with includeDeleted
      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })
      expect(deleted).toBeNull()
    })

    it('removes all relationships', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      // Create related entities
      const user = await db.create('users', {
        $type: 'User',
        name: 'Test User',
        email: 'test@example.com',
      })

      const category = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
        author: { 'Test User': user.$id as EntityId },
        categories: { 'Tech': category.$id as EntityId },
      })

      // Hard delete the post
      await db.delete('posts', post.$id as string, { hard: true })

      // Relationships should be removed
      // User's posts should not include the deleted post
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
      })
      expect(userWithPosts!.posts).toBeDefined()
      const posts = userWithPosts!.posts as any
      expect(posts.$count).toBe(0)

      // Category's posts should not include the deleted post
      const categoryWithPosts = await db.get('categories', category.$id as string, {
        hydrate: ['posts'],
      })
      expect(categoryWithPosts!.posts).toBeDefined()
      const categoryPosts = categoryWithPosts!.posts as any
      expect(categoryPosts.$count).toBe(0)
    })

    it('records event in log', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await db.delete('posts', entity.$id as string, {
        hard: true,
        actor: 'users/admin' as EntityId,
      })

      // Check event log
      const events = await db.getHistory('posts', entity.$id as string)

      // Should have CREATE and DELETE events
      expect(events.items).toHaveLength(2)

      const deleteEvent = events.items.find(e => e.op === 'DELETE')
      expect(deleteEvent).toBeDefined()
      expect(deleteEvent!.actor).toBe('users/admin')
      expect(deleteEvent!.before).toBeDefined() // Should capture state before delete
      expect(deleteEvent!.after).toBeNull() // No state after hard delete
    })

    it('supports expectedVersion for optimistic concurrency', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Delete with correct version
      const result = await db.delete('posts', entity.$id as string, {
        hard: true,
        expectedVersion: 1,
      })
      expect(result.deletedCount).toBe(1)
    })

    it('fails with version mismatch', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Update to change version
      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      })

      // Delete with stale version should fail
      await expect(
        db.delete('posts', entity.$id as string, {
          hard: true,
          expectedVersion: 1, // Now version is 2
        })
      ).rejects.toThrow()
    })
  })

  // ===========================================================================
  // deleteMany
  // ===========================================================================

  describe('deleteMany', () => {
    it('deletes matching entities', async () => {
      // Create multiple posts
      await db.create('posts', {
        $type: 'Post',
        name: 'Draft 1',
        title: 'Draft 1',
        content: 'Content',
        status: 'draft',
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Draft 2',
        title: 'Draft 2',
        content: 'Content',
        status: 'draft',
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Published',
        title: 'Published',
        content: 'Content',
        status: 'published',
      })

      // Delete all drafts
      const result = await db.deleteMany('posts', { status: 'draft' })

      expect(result.deletedCount).toBe(2)

      // Only published should remain in normal query
      const remaining = await db.find('posts', {})
      expect(remaining.items).toHaveLength(1)
      expect(remaining.items[0].status).toBe('published')
    })

    it('returns count of deleted', async () => {
      // Create posts
      for (let i = 0; i < 5; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
          viewCount: i * 10,
        })
      }

      // Delete posts with viewCount >= 20
      const result = await db.deleteMany('posts', {
        viewCount: { $gte: 20 },
      })

      expect(result.deletedCount).toBe(3) // Posts 2, 3, 4
    })

    it('returns 0 when no entities match', async () => {
      await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
        status: 'published',
      })

      const result = await db.deleteMany('posts', { status: 'archived' })
      expect(result.deletedCount).toBe(0)
    })

    it('supports hard delete option', async () => {
      await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content',
        status: 'archived',
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content',
        status: 'archived',
      })

      const result = await db.deleteMany('posts', { status: 'archived' }, {
        hard: true,
      })

      expect(result.deletedCount).toBe(2)

      // Should not exist even with includeDeleted
      const remaining = await db.find('posts', { status: 'archived' }, {
        includeDeleted: true,
      })
      expect(remaining.items).toHaveLength(0)
    })

    it('supports actor option', async () => {
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
        status: 'draft',
      })

      await db.deleteMany('posts', { status: 'draft' }, {
        actor: 'users/admin' as EntityId,
      })

      const deleted = await db.get('posts', post.$id as string, {
        includeDeleted: true,
      })

      expect(deleted!.deletedBy).toBe('users/admin')
    })

    it('deletes all when filter is empty', async () => {
      for (let i = 0; i < 3; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
        })
      }

      const result = await db.deleteMany('posts', {})
      expect(result.deletedCount).toBe(3)
    })

    it('supports complex filters', async () => {
      await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content',
        status: 'draft',
        viewCount: 50,
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content',
        status: 'published',
        viewCount: 100,
      })

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 3',
        title: 'Title 3',
        content: 'Content',
        status: 'draft',
        viewCount: 5,
      })

      // Delete drafts with low views
      const result = await db.deleteMany('posts', {
        $and: [
          { status: 'draft' },
          { viewCount: { $lt: 20 } },
        ],
      })

      expect(result.deletedCount).toBe(1)
    })
  })

  // ===========================================================================
  // Restore (undelete)
  // ===========================================================================

  describe('restore (undelete)', () => {
    it('restores soft-deleted entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Soft delete
      await db.delete('posts', entity.$id as string)

      // Verify deleted
      const deleted = await db.get('posts', entity.$id as string)
      expect(deleted).toBeNull()

      // Restore
      const restored = await db.restore('posts', entity.$id as string)
      expect(restored).not.toBeNull()
      expect(restored!.deletedAt).toBeUndefined()
      expect(restored!.deletedBy).toBeUndefined()

      // Should be visible in normal queries
      const found = await db.get('posts', entity.$id as string)
      expect(found).not.toBeNull()
    })

    it('increments version on restore', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      expect(entity.version).toBe(1)

      await db.delete('posts', entity.$id as string)

      const deleted = await db.get('posts', entity.$id as string, {
        includeDeleted: true,
      })
      expect(deleted!.version).toBe(2)

      const restored = await db.restore('posts', entity.$id as string)
      expect(restored!.version).toBe(3)
    })

    it('cannot restore hard-deleted entity', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Hard delete
      await db.delete('posts', entity.$id as string, { hard: true })

      // Try to restore
      const restored = await db.restore('posts', entity.$id as string)
      expect(restored).toBeNull()
    })

    it('sets restoredBy actor', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await db.delete('posts', entity.$id as string)

      const restored = await db.restore('posts', entity.$id as string, {
        actor: 'users/admin' as EntityId,
      })

      expect(restored!.updatedBy).toBe('users/admin')
    })
  })

  // ===========================================================================
  // Proxy-based access
  // ===========================================================================

  describe('proxy-based access', () => {
    it('works with collection.delete()', async () => {
      const posts = (db as any).Posts

      const entity = await posts.create({
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const result = await posts.delete(entity.$id)
      expect(result.deletedCount).toBe(1)
    })

    it('works with collection.deleteMany()', async () => {
      const posts = (db as any).Posts

      await posts.create({
        $type: 'Post',
        name: 'Draft 1',
        title: 'Title',
        content: 'Content',
        status: 'draft',
      })

      await posts.create({
        $type: 'Post',
        name: 'Draft 2',
        title: 'Title',
        content: 'Content',
        status: 'draft',
      })

      const result = await posts.deleteMany({ status: 'draft' })
      expect(result.deletedCount).toBe(2)
    })
  })
})

/**
 * CRUD Workflow E2E Tests
 *
 * Full database workflows testing complete user scenarios.
 * These tests use real ParqueDB with MemoryBackend for fast, isolated testing.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest'
import { ParqueDB, MemoryBackend } from '../../src'
import {
  createUserInput,
  createPostInput,
  generateTestId,
} from '../factories'
import type { Entity, EntityId, Filter } from '../../src/types'

describe('CRUD Workflow E2E', () => {
  let db: ParqueDB
  let backend: MemoryBackend

  beforeAll(async () => {
    backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  afterAll(async () => {
    // Dispose of ParqueDB instance
    if (db && typeof (db as unknown as { dispose?: () => void }).dispose === 'function') {
      (db as unknown as { dispose: () => void }).dispose()
    }
  })

  beforeEach(async () => {
    // Create a fresh instance for each test
    backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  describe('User Management Workflow', () => {
    it('should complete a full user lifecycle', async () => {
      // 1. Create a new user
      const userInput = createUserInput({
        name: 'Alice Johnson',
        email: 'alice@example.com',
        bio: 'Software engineer',
      })

      const user = await db.create('users', userInput)

      expect(user).toBeValidEntity()
      expect(user.$type).toBe('User')
      expect(user.name).toBe('Alice Johnson')
      expect(user.email).toBe('alice@example.com')
      expect(user.version).toBe(1)

      // 2. Read the user back
      const fetchedUser = await db.get('users', user.$id)
      expect(fetchedUser).not.toBeNull()
      expect(fetchedUser!.$id).toBe(user.$id)

      // 3. Update the user
      const updatedUser = await db.update('users', user.$id, {
        $set: { bio: 'Senior software engineer' },
      })

      expect(updatedUser).not.toBeNull()
      expect(updatedUser!.bio).toBe('Senior software engineer')
      expect(updatedUser!.version).toBe(2)

      // 4. Soft delete the user
      const deleteResult = await db.delete('users', user.$id)
      expect(deleteResult.deletedCount).toBe(1)

      // 5. User should not appear in regular queries
      const usersResult = await db.find('users')
      expect(usersResult.items.length).toBe(0)

      // 6. User data still exists (soft deleted) - can be fetched with includeDeleted
      const deletedUser = await db.get('users', user.$id, { includeDeleted: true })
      expect(deletedUser).not.toBeNull()
      expect(deletedUser!.deletedAt).toBeInstanceOf(Date)
    })

    it('should handle multiple users', async () => {
      // Create multiple users
      const users = await Promise.all([
        db.create('users', createUserInput({ name: 'User 1' })),
        db.create('users', createUserInput({ name: 'User 2' })),
        db.create('users', createUserInput({ name: 'User 3' })),
      ])

      expect(users).toHaveLength(3)

      // Find all users
      const allUsersResult = await db.find('users')
      expect(allUsersResult.items).toHaveLength(3)

      // Count users (via find)
      expect(allUsersResult.total).toBe(3)
    })
  })

  describe('Blog Post Workflow', () => {
    let author: Entity

    beforeEach(async () => {
      // Create an author for posts
      author = await db.create('users', createUserInput({
        name: 'Blog Author',
        email: 'author@blog.com',
      }))
    })

    it('should complete a full post lifecycle', async () => {
      // 1. Create a draft post
      const post = await db.create('posts', createPostInput({
        title: 'Getting Started with ParqueDB',
        content: '# Introduction\n\nParqueDB is...',
        status: 'draft',
        author: { [author.name]: author.$id },
      }))

      expect(post.status).toBe('draft')
      expect(post.publishedAt).toBeUndefined()

      // 2. Update to published
      const now = new Date()
      const publishedPost = await db.update('posts', post.$id, {
        $set: {
          status: 'published',
          publishedAt: now,
        },
      })

      expect(publishedPost!.status).toBe('published')
      expect(publishedPost!.publishedAt).toEqual(now)
      expect(publishedPost!.version).toBe(2)

      // 3. Edit the published post
      const editedPost = await db.update('posts', post.$id, {
        $set: { content: '# Introduction\n\nParqueDB is a database...' },
      })

      expect(editedPost!.version).toBe(3)

      // 4. Archive the post
      const archivedPost = await db.update('posts', post.$id, {
        $set: { status: 'archived' },
      })

      expect(archivedPost!.status).toBe('archived')
    })

    it('should filter posts by status', async () => {
      // Create posts with different statuses
      await db.create('posts', createPostInput({ status: 'draft' }))
      await db.create('posts', createPostInput({ status: 'draft' }))
      await db.create('posts', createPostInput({ status: 'published' }))
      await db.create('posts', createPostInput({ status: 'archived' }))

      // Find draft posts
      const draftsResult = await db.find('posts', { status: 'draft' })
      expect(draftsResult.items).toHaveLength(2)

      // Find published posts
      const publishedResult = await db.find('posts', { status: 'published' })
      expect(publishedResult.items).toHaveLength(1)

      // Find archived posts
      const archivedResult = await db.find('posts', { status: 'archived' })
      expect(archivedResult.items).toHaveLength(1)
    })

    it('should handle post with categories', async () => {
      // Create categories
      const techCategory = await db.create('categories', {
        $type: 'Category',
        name: 'Technology',
        slug: 'tech',
      })

      const dbCategory = await db.create('categories', {
        $type: 'Category',
        name: 'Databases',
        slug: 'databases',
      })

      // Create post with categories
      const post = await db.create('posts', createPostInput({
        title: 'ParqueDB Deep Dive',
        categories: {
          [techCategory.name]: techCategory.$id,
          [dbCategory.name]: dbCategory.$id,
        },
      }))

      expect(post.categories).toEqual({
        [techCategory.name]: techCategory.$id,
        [dbCategory.name]: dbCategory.$id,
      })
    })
  })

  describe('Concurrent Operations', () => {
    it('should handle concurrent creates', async () => {
      const createPromises = Array.from({ length: 20 }, (_, i) =>
        db.create('items', {
          $type: 'Item',
          name: `Item ${i}`,
          index: i,
        })
      )

      const items = await Promise.all(createPromises)

      expect(items).toHaveLength(20)
      expect(new Set(items.map((i) => i.$id)).size).toBe(20) // All unique IDs
    })

    it('should handle sequential reads and writes correctly', async () => {
      // Create initial data
      const item = await db.create('items', {
        $type: 'Item',
        name: 'Shared Item',
        count: 0,
      })

      // Sequential operations (concurrent r/w on real filesystems can race)
      const result1 = await db.get('items', item.$id)
      expect(result1).not.toBeNull()
      expect(result1!.count).toBe(0)

      const result2 = await db.update('items', item.$id, { $set: { count: 1 } })
      expect(result2).not.toBeNull()
      expect(result2!.count).toBe(1)

      const result3 = await db.get('items', item.$id)
      expect(result3).not.toBeNull()
      expect(result3!.count).toBe(1)

      const result4 = await db.update('items', item.$id, { $set: { count: 2 } })
      expect(result4).not.toBeNull()
      expect(result4!.count).toBe(2)

      const findResult = await db.find('items')
      expect(findResult.items.length).toBeGreaterThan(0)
    })
  })

  describe('Error Handling', () => {
    it('should handle updates to non-existent entities', async () => {
      const result = await db.update('items', 'items/nonexistent', { $set: { name: 'Test' } })
      expect(result).toBeNull()
    })

    it('should handle deletes of non-existent entities', async () => {
      const result = await db.delete('items', 'items/nonexistent')
      expect(result.deletedCount).toBe(0)
    })

    it('should handle gets of non-existent entities', async () => {
      const result = await db.get('items', 'items/nonexistent')
      expect(result).toBeNull()
    })
  })

  describe('Query Scenarios', () => {
    beforeEach(async () => {
      // Seed test data
      await db.create('products', { $type: 'Product', name: 'Product A', price: 10, category: 'electronics' })
      await db.create('products', { $type: 'Product', name: 'Product B', price: 25, category: 'electronics' })
      await db.create('products', { $type: 'Product', name: 'Product C', price: 50, category: 'clothing' })
      await db.create('products', { $type: 'Product', name: 'Product D', price: 100, category: 'clothing' })
    })

    it('should filter by category', async () => {
      const electronicsResult = await db.find('products', { category: 'electronics' })
      expect(electronicsResult.items).toHaveLength(2)

      const clothingResult = await db.find('products', { category: 'clothing' })
      expect(clothingResult.items).toHaveLength(2)
    })

    it('should filter by type', async () => {
      const productsResult = await db.find('products', { $type: 'Product' })
      expect(productsResult.items).toHaveLength(4)
    })

    it('should find all without filter', async () => {
      const allResult = await db.find('products')
      expect(allResult.items).toHaveLength(4)
    })
  })
})

describe('Data Integrity E2E', () => {
  let db: ParqueDB
  let backend: MemoryBackend

  beforeAll(async () => {
    backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  afterAll(async () => {
    // Dispose of ParqueDB instance
    if (db && typeof (db as unknown as { dispose?: () => void }).dispose === 'function') {
      (db as unknown as { dispose: () => void }).dispose()
    }
  })

  beforeEach(async () => {
    // Create a fresh instance for each test
    backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  it('should maintain version consistency', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Versioned Item',
    })

    expect(entity.version).toBe(1)

    // Multiple updates
    let current = entity
    for (let i = 2; i <= 10; i++) {
      current = (await db.update('items', current.$id, { $set: { name: `Update ${i}` } }))!
      expect(current.version).toBe(i)
    }

    // Final check
    const final = await db.get('items', entity.$id)
    expect(final!.version).toBe(10)
  })

  it('should track timestamps correctly', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Timestamped Item',
    })

    const createdAt = entity.createdAt

    vi.useFakeTimers()
    try {
      // Advance time deterministically
      vi.advanceTimersByTime(10)

      const updated = await db.update('items', entity.$id, { $set: { name: 'Updated Name' } })

      expect(updated!.createdAt.getTime()).toBe(createdAt.getTime())
      expect(updated!.updatedAt.getTime()).toBeGreaterThan(createdAt.getTime())
    } finally {
      vi.useRealTimers()
    }
  })

  it('should preserve entity ID through updates', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Original',
    })

    const originalId = entity.$id

    const updated = await db.update('items', entity.$id, { $set: { name: 'Updated' } })

    expect(updated!.$id).toBe(originalId)
  })

  it('should persist data correctly in memory backend', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Persisted Item',
      data: { nested: { value: 42 } },
    })

    // Read directly from the database to verify persistence
    const fetchedEntity = await db.get('items', entity.$id)

    expect(fetchedEntity).not.toBeNull()
    expect(fetchedEntity!.name).toBe('Persisted Item')
    expect(fetchedEntity!.data).toEqual({ nested: { value: 42 } })
  })
})

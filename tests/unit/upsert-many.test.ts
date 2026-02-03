/**
 * UpsertMany Operation Tests
 *
 * RED phase tests for bulk upsert operations.
 * Tests cover bulk insert when records don't exist, bulk update when records exist,
 * mixed insert/update, conflict handling options, and return value verification.
 *
 * Uses real FsBackend storage with temporary directories (NO MOCKS).
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
  Filter,
  UpdateInput,
} from '../../src/types'

// =============================================================================
// Test Suite
// =============================================================================

describe('Collection.upsertMany()', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-upsertmany-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // Bulk Insert (records don't exist)
  // ===========================================================================

  describe('bulk insert when records do not exist', () => {
    it('inserts multiple new records', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        {
          filter: { slug: 'post-1' },
          update: {
            $set: { title: 'First Post', content: 'Content 1' },
            $setOnInsert: { viewCount: 0 },
          },
        },
        {
          filter: { slug: 'post-2' },
          update: {
            $set: { title: 'Second Post', content: 'Content 2' },
            $setOnInsert: { viewCount: 0 },
          },
        },
        {
          filter: { slug: 'post-3' },
          update: {
            $set: { title: 'Third Post', content: 'Content 3' },
            $setOnInsert: { viewCount: 0 },
          },
        },
      ])

      expect(result).toBeDefined()
      expect(result.insertedCount).toBe(3)
      expect(result.modifiedCount).toBe(0)
      expect(result.upsertedCount).toBe(3)

      // Verify all records were created
      const found = await posts.find({ slug: { $in: ['post-1', 'post-2', 'post-3'] } })
      expect(found.items).toHaveLength(3)
    })

    it('applies $setOnInsert only on insert', async () => {
      const posts = (db as any).Posts

      await posts.upsertMany([
        {
          filter: { slug: 'new-post' },
          update: {
            $set: { status: 'draft' },
            $setOnInsert: {
              title: 'Default Title',
              viewCount: 0,
              createdVia: 'upsertMany',
            },
          },
        },
      ])

      const post = await posts.findOne({ slug: 'new-post' })
      expect(post).not.toBeNull()
      expect(post.status).toBe('draft')
      expect(post.title).toBe('Default Title')
      expect(post.viewCount).toBe(0)
      expect(post.createdVia).toBe('upsertMany')
    })

    it('generates unique IDs for each inserted record', async () => {
      const users = (db as any).Users

      const result = await users.upsertMany([
        { filter: { email: 'user1@test.com' }, update: { $set: { name: 'User 1' } } },
        { filter: { email: 'user2@test.com' }, update: { $set: { name: 'User 2' } } },
      ])

      expect(result.upsertedIds).toBeDefined()
      expect(result.upsertedIds).toHaveLength(2)
      expect(result.upsertedIds[0]).not.toBe(result.upsertedIds[1])
    })

    it('includes filter fields in the created document', async () => {
      const products = (db as any).Products

      await products.upsertMany([
        {
          filter: { sku: 'SKU-001', category: 'electronics' },
          update: { $set: { name: 'Widget', price: 9.99 } },
        },
      ])

      const product = await products.findOne({ sku: 'SKU-001' })
      expect(product).not.toBeNull()
      expect(product.sku).toBe('SKU-001')
      expect(product.category).toBe('electronics')
      expect(product.name).toBe('Widget')
    })
  })

  // ===========================================================================
  // Bulk Update (records exist)
  // ===========================================================================

  describe('bulk update when records exist', () => {
    beforeEach(async () => {
      // Create existing records
      const posts = (db as any).Posts
      await posts.create({ $type: 'Post', name: 'Post 1', slug: 'post-1', title: 'Original 1', viewCount: 10 })
      await posts.create({ $type: 'Post', name: 'Post 2', slug: 'post-2', title: 'Original 2', viewCount: 20 })
      await posts.create({ $type: 'Post', name: 'Post 3', slug: 'post-3', title: 'Original 3', viewCount: 30 })
    })

    it('updates multiple existing records', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated 1' } } },
        { filter: { slug: 'post-2' }, update: { $set: { title: 'Updated 2' } } },
        { filter: { slug: 'post-3' }, update: { $set: { title: 'Updated 3' } } },
      ])

      expect(result.insertedCount).toBe(0)
      expect(result.modifiedCount).toBe(3)
      expect(result.upsertedCount).toBe(0)

      // Verify updates
      const post1 = await posts.findOne({ slug: 'post-1' })
      const post2 = await posts.findOne({ slug: 'post-2' })
      const post3 = await posts.findOne({ slug: 'post-3' })

      expect(post1.title).toBe('Updated 1')
      expect(post2.title).toBe('Updated 2')
      expect(post3.title).toBe('Updated 3')
    })

    it('ignores $setOnInsert when updating existing records', async () => {
      const posts = (db as any).Posts

      await posts.upsertMany([
        {
          filter: { slug: 'post-1' },
          update: {
            $set: { status: 'published' },
            $setOnInsert: { title: 'Should Not Override', viewCount: 0 },
          },
        },
      ])

      const post = await posts.findOne({ slug: 'post-1' })
      expect(post.status).toBe('published')
      expect(post.title).toBe('Original 1') // NOT overwritten
      expect(post.viewCount).toBe(10) // NOT overwritten
    })

    it('applies $inc operator correctly', async () => {
      const posts = (db as any).Posts

      await posts.upsertMany([
        { filter: { slug: 'post-1' }, update: { $inc: { viewCount: 5 } } },
        { filter: { slug: 'post-2' }, update: { $inc: { viewCount: 10 } } },
      ])

      const post1 = await posts.findOne({ slug: 'post-1' })
      const post2 = await posts.findOne({ slug: 'post-2' })

      expect(post1.viewCount).toBe(15) // 10 + 5
      expect(post2.viewCount).toBe(30) // 20 + 10
    })

    it('increments version on update', async () => {
      const posts = (db as any).Posts

      const original = await posts.findOne({ slug: 'post-1' })
      expect(original.version).toBe(1)

      await posts.upsertMany([
        { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated' } } },
      ])

      const updated = await posts.findOne({ slug: 'post-1' })
      expect(updated.version).toBe(2)
    })
  })

  // ===========================================================================
  // Mixed Insert/Update
  // ===========================================================================

  describe('mixed insert and update operations', () => {
    beforeEach(async () => {
      // Create some existing records
      const posts = (db as any).Posts
      await posts.create({ $type: 'Post', name: 'Existing 1', slug: 'existing-1', title: 'Existing Title 1' })
      await posts.create({ $type: 'Post', name: 'Existing 2', slug: 'existing-2', title: 'Existing Title 2' })
    })

    it('handles mix of inserts and updates correctly', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        // Update existing
        { filter: { slug: 'existing-1' }, update: { $set: { title: 'Updated Title 1' } } },
        // Insert new
        { filter: { slug: 'new-1' }, update: { $set: { title: 'New Title 1' } } },
        // Update existing
        { filter: { slug: 'existing-2' }, update: { $set: { title: 'Updated Title 2' } } },
        // Insert new
        { filter: { slug: 'new-2' }, update: { $set: { title: 'New Title 2' } } },
      ])

      expect(result.insertedCount).toBe(2)
      expect(result.modifiedCount).toBe(2)
      expect(result.upsertedCount).toBe(2)

      // Verify updates
      const existing1 = await posts.findOne({ slug: 'existing-1' })
      const existing2 = await posts.findOne({ slug: 'existing-2' })
      expect(existing1.title).toBe('Updated Title 1')
      expect(existing2.title).toBe('Updated Title 2')

      // Verify inserts
      const new1 = await posts.findOne({ slug: 'new-1' })
      const new2 = await posts.findOne({ slug: 'new-2' })
      expect(new1.title).toBe('New Title 1')
      expect(new2.title).toBe('New Title 2')
    })

    it('returns correct counts in result', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        { filter: { slug: 'existing-1' }, update: { $set: { x: 1 } } }, // update
        { filter: { slug: 'new-1' }, update: { $set: { x: 1 } } }, // insert
        { filter: { slug: 'existing-2' }, update: { $set: { x: 1 } } }, // update
        { filter: { slug: 'new-2' }, update: { $set: { x: 1 } } }, // insert
        { filter: { slug: 'new-3' }, update: { $set: { x: 1 } } }, // insert
      ])

      expect(result.insertedCount).toBe(3)
      expect(result.modifiedCount).toBe(2)
      expect(result.matchedCount).toBe(2)
      expect(result.upsertedCount).toBe(3)
    })

    it('tracks upserted IDs separately', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        { filter: { slug: 'existing-1' }, update: { $set: { x: 1 } } }, // update
        { filter: { slug: 'new-1' }, update: { $set: { x: 1 } } }, // insert
        { filter: { slug: 'new-2' }, update: { $set: { x: 1 } } }, // insert
      ])

      expect(result.upsertedIds).toHaveLength(2)
      // upsertedIds should only contain IDs for newly created documents
      expect(result.upsertedIds.every((id: string) => id.startsWith('posts/'))).toBe(true)
    })
  })

  // ===========================================================================
  // Conflict Handling Options
  // ===========================================================================

  describe('conflict handling options', () => {
    it('ordered: true stops on first error', async () => {
      const posts = (db as any).Posts

      // Create a record
      await posts.create({ $type: 'Post', name: 'Post', slug: 'post-1', title: 'Title' })

      // This should fail due to ordered: true and stop at the invalid operation
      const result = await posts.upsertMany(
        [
          { filter: { slug: 'new-1' }, update: { $set: { title: 'New 1' } } },
          // Invalid: trying to update with expectedVersion that doesn't match
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'new-2' }, update: { $set: { title: 'New 2' } } }, // Should not run
        ],
        { ordered: true }
      )

      expect(result.insertedCount).toBe(1) // Only first succeeded
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].index).toBe(1)

      // Third operation should not have run
      const new2 = await posts.findOne({ slug: 'new-2' })
      expect(new2).toBeNull()
    })

    it('ordered: false continues on error', async () => {
      const posts = (db as any).Posts

      // Create a record
      await posts.create({ $type: 'Post', name: 'Post', slug: 'post-1', title: 'Title' })

      // Should continue past errors
      const result = await posts.upsertMany(
        [
          { filter: { slug: 'new-1' }, update: { $set: { title: 'New 1' } } },
          // Invalid operation
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'new-2' }, update: { $set: { title: 'New 2' } } }, // Should still run
        ],
        { ordered: false }
      )

      expect(result.insertedCount).toBe(2) // First and third succeeded
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].index).toBe(1)

      // Third operation should have run
      const new2 = await posts.findOne({ slug: 'new-2' })
      expect(new2).not.toBeNull()
    })

    it('supports per-operation options', async () => {
      const posts = (db as any).Posts

      // Create records
      const post1 = await posts.create({ $type: 'Post', name: 'Post 1', slug: 'post-1', title: 'Title 1' })
      const post2 = await posts.create({ $type: 'Post', name: 'Post 2', slug: 'post-2', title: 'Title 2' })

      const result = await posts.upsertMany([
        {
          filter: { slug: 'post-1' },
          update: { $set: { title: 'Updated 1' } },
          options: { expectedVersion: post1.version },
        },
        {
          filter: { slug: 'post-2' },
          update: { $set: { title: 'Updated 2' } },
          options: { expectedVersion: post2.version },
        },
      ])

      expect(result.modifiedCount).toBe(2)
      expect(result.errors).toHaveLength(0)
    })

    it('respects actor option for audit fields', async () => {
      const posts = (db as any).Posts

      await posts.upsertMany(
        [
          { filter: { slug: 'new-1' }, update: { $set: { title: 'New' } } },
        ],
        { actor: 'users/admin' as EntityId }
      )

      const post = await posts.findOne({ slug: 'new-1' })
      expect(post.createdBy).toBe('users/admin')
      expect(post.updatedBy).toBe('users/admin')
    })
  })

  // ===========================================================================
  // Return Value Verification
  // ===========================================================================

  describe('return values', () => {
    it('returns comprehensive result object', async () => {
      const posts = (db as any).Posts

      await posts.create({ $type: 'Post', name: 'Existing', slug: 'existing', title: 'Title' })

      const result = await posts.upsertMany([
        { filter: { slug: 'existing' }, update: { $set: { title: 'Updated' } } },
        { filter: { slug: 'new-1' }, update: { $set: { title: 'New 1' } } },
        { filter: { slug: 'new-2' }, update: { $set: { title: 'New 2' } } },
      ])

      // Verify result structure
      expect(result).toMatchObject({
        ok: true,
        insertedCount: 2,
        modifiedCount: 1,
        matchedCount: 1,
        upsertedCount: 2,
      })

      expect(result.upsertedIds).toBeDefined()
      expect(result.upsertedIds).toHaveLength(2)
      expect(result.errors).toEqual([])
    })

    it('returns ok: false when there are errors', async () => {
      const posts = (db as any).Posts

      await posts.create({ $type: 'Post', name: 'Post', slug: 'post-1', title: 'Title' })

      const result = await posts.upsertMany([
        { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated' } }, options: { expectedVersion: 999 } },
      ])

      expect(result.ok).toBe(false)
      expect(result.errors).toHaveLength(1)
    })

    it('includes error details in result', async () => {
      const posts = (db as any).Posts

      await posts.create({ $type: 'Post', name: 'Post', slug: 'post-1', title: 'Title' })

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'new' }, update: { $set: { title: 'OK' } } },
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Fail' } }, options: { expectedVersion: 999 } },
        ],
        { ordered: false }
      )

      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]).toMatchObject({
        index: 1,
        filter: { slug: 'post-1' },
      })
      expect(result.errors[0].error).toBeDefined()
      expect(result.errors[0].error.message).toContain('Version')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty array input', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([])

      expect(result.ok).toBe(true)
      expect(result.insertedCount).toBe(0)
      expect(result.modifiedCount).toBe(0)
      expect(result.upsertedCount).toBe(0)
      expect(result.errors).toEqual([])
    })

    it('handles single item array', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([
        { filter: { slug: 'single' }, update: { $set: { title: 'Single Post' } } },
      ])

      expect(result.upsertedCount).toBe(1)

      const post = await posts.findOne({ slug: 'single' })
      expect(post.title).toBe('Single Post')
    })

    it('handles large batch operations', async () => {
      const posts = (db as any).Posts

      const items = Array.from({ length: 100 }, (_, i) => ({
        filter: { slug: `post-${i}` },
        update: { $set: { title: `Post ${i}`, index: i } },
      }))

      const result = await posts.upsertMany(items)

      expect(result.insertedCount).toBe(100)
      expect(result.upsertedCount).toBe(100)

      // Verify a few random entries
      const post0 = await posts.findOne({ slug: 'post-0' })
      const post50 = await posts.findOne({ slug: 'post-50' })
      const post99 = await posts.findOne({ slug: 'post-99' })

      expect(post0.index).toBe(0)
      expect(post50.index).toBe(50)
      expect(post99.index).toBe(99)
    })

    it('handles complex filters', async () => {
      const posts = (db as any).Posts

      await posts.create({
        $type: 'Post',
        name: 'Target',
        author: 'users/john' as EntityId,
        category: 'tech',
        status: 'draft',
      })

      const result = await posts.upsertMany([
        {
          filter: {
            $and: [
              { author: 'users/john' as EntityId },
              { category: 'tech' },
              { status: 'draft' },
            ],
          },
          update: { $set: { status: 'published' } },
        },
      ])

      expect(result.modifiedCount).toBe(1)

      const post = await posts.findOne({ author: 'users/john' as EntityId })
      expect(post.status).toBe('published')
    })

    it('handles $link operator in updates', async () => {
      const posts = (db as any).Posts
      const users = (db as any).Users

      const user = await users.create({ $type: 'User', name: 'John' })

      await posts.upsertMany([
        {
          filter: { slug: 'new-post' },
          update: {
            $set: { title: 'New Post' },
            $link: { author: user.$id as EntityId },
          },
        },
      ])

      const post = await posts.findOne({ slug: 'new-post' })
      expect(post).not.toBeNull()
      expect(post.title).toBe('New Post')
      // Verify the relationship was established by checking the author field
      expect(post.author).toBeDefined()
      // The author field should contain a reference to the user
      expect(Object.values(post.author as Record<string, string>)).toContain(user.$id)
    })

    it('handles updates with multiple operators', async () => {
      const posts = (db as any).Posts

      await posts.create({
        $type: 'Post',
        name: 'Post',
        slug: 'post-1',
        title: 'Original',
        viewCount: 10,
        tags: ['old'],
      })

      await posts.upsertMany([
        {
          filter: { slug: 'post-1' },
          update: {
            $set: { title: 'Updated' },
            $inc: { viewCount: 5 },
            $push: { tags: 'new' },
            $currentDate: { lastModified: true },
          },
        },
      ])

      const post = await posts.findOne({ slug: 'post-1' })
      expect(post.title).toBe('Updated')
      expect(post.viewCount).toBe(15)
      expect(post.tags).toContain('new')
      expect(post.lastModified).toBeInstanceOf(Date)
    })
  })

  // ===========================================================================
  // ParqueDB level access
  // ===========================================================================

  describe('db-level upsertMany', () => {
    it('works via db.upsertMany with namespace', async () => {
      const result = await db.upsertMany('posts', [
        { filter: { slug: 'post-1' }, update: { $set: { title: 'Post 1' } } },
        { filter: { slug: 'post-2' }, update: { $set: { title: 'Post 2' } } },
      ])

      expect(result.upsertedCount).toBe(2)

      const posts = (db as any).Posts
      const post1 = await posts.findOne({ slug: 'post-1' })
      expect(post1.title).toBe('Post 1')
    })
  })
})

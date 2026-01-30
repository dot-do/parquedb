/**
 * Upsert Operation Tests
 *
 * Tests for upsert (update or insert) operations using real storage.
 * Tests cover create-if-not-exists, update-if-exists, $setOnInsert,
 * and filter-based upsert.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type { EntityId } from '../../src/types'

// =============================================================================
// Test Suite
// =============================================================================

describe('Upsert Operations', () => {
  let db: ParqueDB
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-upsert-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    await rm(tempDir, { recursive: true, force: true })
  })

  // ===========================================================================
  // Basic Upsert
  // ===========================================================================

  describe('basic upsert', () => {
    it('creates if not exists', async () => {
      // Try to update a non-existent entity with upsert: true
      const result = await db.update(
        'posts',
        'posts/new-post-123',
        {
          $set: {
            title: 'New Post',
            content: 'Content created via upsert',
            status: 'draft',
          },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.$id).toBe('posts/new-post-123')
      expect(result!.title).toBe('New Post')
      expect(result!.content).toBe('Content created via upsert')
      expect(result!.status).toBe('draft')
      expect(result!.version).toBe(1)
      expect(result!.createdAt).toBeInstanceOf(Date)
    })

    it('updates if exists', async () => {
      // First create an entity
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Existing Post',
        title: 'Original Title',
        content: 'Original content',
        status: 'draft',
      })

      // Now upsert - should update, not create
      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $set: {
            title: 'Updated Title',
            status: 'published',
          },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.$id).toBe(created.$id)
      expect(result!.title).toBe('Updated Title')
      expect(result!.status).toBe('published')
      // Original fields should be preserved
      expect(result!.content).toBe('Original content')
      expect(result!.version).toBe(2)
    })

    it('without upsert returns null for non-existent entity', async () => {
      const result = await db.update(
        'posts',
        'posts/nonexistent',
        {
          $set: { title: 'Will not be created' },
        },
        {
          upsert: false, // Explicit false
        }
      )

      expect(result).toBeNull()
    })

    it('returns upserted document', async () => {
      const result = await db.update(
        'posts',
        'posts/upserted-123',
        {
          $set: { title: 'Upserted', content: 'Created via upsert' },
        },
        {
          upsert: true,
          returnDocument: 'after',
        }
      )

      expect(result).not.toBeNull()
      expect(result!.$id).toBe('posts/upserted-123')
    })
  })

  // ===========================================================================
  // $setOnInsert
  // ===========================================================================

  describe('$setOnInsert operator', () => {
    it('applies $setOnInsert only on create', async () => {
      const result = await db.update(
        'posts',
        'posts/new-post',
        {
          $set: { status: 'active' },
          $setOnInsert: {
            title: 'Default Title',
            initialViews: 0,
            createdVia: 'upsert',
          },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.status).toBe('active')
      expect(result!.title).toBe('Default Title')
      expect(result!.initialViews).toBe(0)
      expect(result!.createdVia).toBe('upsert')
    })

    it('ignores $setOnInsert when updating', async () => {
      // First create an entity
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Existing',
        title: 'Original Title',
        content: 'Content',
        initialViews: 100,
      })

      // Upsert with $setOnInsert - should NOT apply $setOnInsert values
      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $set: { status: 'active' },
          $setOnInsert: {
            title: 'Should Not Override',
            initialViews: 0,
          },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.status).toBe('active')
      expect(result!.title).toBe('Original Title') // NOT overwritten
      expect(result!.initialViews).toBe(100) // NOT overwritten
    })

    it('combines $set and $setOnInsert correctly on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/combined-123',
        {
          $set: {
            status: 'draft',
            lastUpdated: new Date(),
          },
          $setOnInsert: {
            title: 'Initial Title',
            viewCount: 0,
          },
          $inc: { version: 1 },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      // $set values applied
      expect(result!.status).toBe('draft')
      expect(result!.lastUpdated).toBeInstanceOf(Date)
      // $setOnInsert values applied (because it's an insert)
      expect(result!.title).toBe('Initial Title')
      expect(result!.viewCount).toBe(0)
    })

    it('$setOnInsert can set $type and name on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/typed-123',
        {
          $set: { content: 'Some content' },
          $setOnInsert: {
            $type: 'Post',
            name: 'Auto-created Post',
          },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.$type).toBe('Post')
      expect(result!.name).toBe('Auto-created Post')
      expect(result!.content).toBe('Some content')
    })
  })

  // ===========================================================================
  // Filter-based upsert
  // ===========================================================================

  describe('filter-based upsert', () => {
    it('handles filter-based upsert', async () => {
      // First, verify no matching post exists
      const before = await db.find('posts', {
        slug: 'unique-slug',
        author: 'users/john',
      })
      expect(before.items).toHaveLength(0)

      // Upsert with filter - should create since no match
      const result = await db.upsert(
        'posts',
        { slug: 'unique-slug', author: 'users/john' as EntityId },
        {
          $set: { title: 'Created via filter upsert', status: 'draft' },
          $setOnInsert: { viewCount: 0 },
        }
      )

      expect(result).not.toBeNull()
      expect(result!.slug).toBe('unique-slug')
      expect(result!.author).toBe('users/john')
      expect(result!.title).toBe('Created via filter upsert')
      expect(result!.viewCount).toBe(0)
    })

    it('updates matching entity with filter-based upsert', async () => {
      // Create an entity first
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Slugged Post',
        slug: 'my-unique-slug',
        author: 'users/john' as EntityId,
        title: 'Original',
        viewCount: 50,
      })

      // Upsert with filter - should update existing
      const result = await db.upsert(
        'posts',
        { slug: 'my-unique-slug', author: 'users/john' as EntityId },
        {
          $set: { title: 'Updated via filter' },
          $inc: { viewCount: 1 },
          $setOnInsert: { shouldNotApply: true },
        }
      )

      expect(result).not.toBeNull()
      expect(result!.$id).toBe(created.$id)
      expect(result!.title).toBe('Updated via filter')
      expect(result!.viewCount).toBe(51)
      expect(result!.shouldNotApply).toBeUndefined() // $setOnInsert ignored
    })

    it('uses filter fields in created document', async () => {
      const result = await db.upsert(
        'posts',
        {
          externalId: 'ext-123',
          source: 'api',
        },
        {
          $set: { title: 'From API' },
        }
      )

      expect(result).not.toBeNull()
      // Filter fields should be in the created document
      expect(result!.externalId).toBe('ext-123')
      expect(result!.source).toBe('api')
      expect(result!.title).toBe('From API')
    })
  })

  // ===========================================================================
  // Upsert with operators
  // ===========================================================================

  describe('upsert with operators', () => {
    it('applies $inc on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/counter-123',
        {
          $inc: { count: 1 },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.count).toBe(1)
    })

    it('applies $push on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/array-123',
        {
          $push: { tags: 'first-tag' },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.tags).toEqual(['first-tag'])
    })

    it('applies $addToSet on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/set-123',
        {
          $addToSet: { categories: 'tech' },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.categories).toEqual(['tech'])
    })

    it('applies $currentDate on insert', async () => {
      const before = new Date()

      const result = await db.update(
        'posts',
        'posts/dated-123',
        {
          $set: { title: 'Dated Post' },
          $currentDate: { publishedAt: true },
        },
        {
          upsert: true,
        }
      )

      const after = new Date()

      expect(result).not.toBeNull()
      expect(result!.publishedAt).toBeInstanceOf(Date)
      expect((result!.publishedAt as Date).getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect((result!.publishedAt as Date).getTime()).toBeLessThanOrEqual(after.getTime())
    })
  })

  // ===========================================================================
  // Upsert with relationships
  // ===========================================================================

  describe('upsert with relationships', () => {
    it('applies $link on insert', async () => {
      // Create target user entity first
      const user = await db.create('users', {
        $type: 'User',
        name: 'John',
      })

      const result = await db.update(
        'posts',
        'posts/linked-123',
        {
          $set: { title: 'Linked Post' },
          $link: { author: user.$id as EntityId },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.author).toBeDefined()
    })

    it('applies $link on update', async () => {
      // Create target category entity first
      const category = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
      })

      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $link: { categories: category.$id as EntityId },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.categories).toBeDefined()
    })
  })

  // ===========================================================================
  // Upsert options
  // ===========================================================================

  describe('upsert options', () => {
    it('respects actor option on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/actor-123',
        {
          $set: { title: 'Created by actor' },
        },
        {
          upsert: true,
          actor: 'users/creator' as EntityId,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.createdBy).toBe('users/creator')
      expect(result!.updatedBy).toBe('users/creator')
    })

    it('respects actor option on update', async () => {
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $set: { title: 'Updated by different actor' },
        },
        {
          upsert: true,
          actor: 'users/updater' as EntityId,
        }
      )

      expect(result).not.toBeNull()
      expect(result!.createdBy).not.toBe('users/updater') // Original creator
      expect(result!.updatedBy).toBe('users/updater')
    })

    it('respects returnDocument: before on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/return-before-123',
        {
          $set: { title: 'New Post' },
        },
        {
          upsert: true,
          returnDocument: 'before',
        }
      )

      // For insert, 'before' should return null (no previous document)
      expect(result).toBeNull()

      // Verify entity was created
      const created = await db.get('posts', 'posts/return-before-123')
      expect(created).not.toBeNull()
      expect(created!.title).toBe('New Post')
    })

    it('respects returnDocument: after on insert', async () => {
      const result = await db.update(
        'posts',
        'posts/return-after-123',
        {
          $set: { title: 'New Post' },
        },
        {
          upsert: true,
          returnDocument: 'after',
        }
      )

      expect(result).not.toBeNull()
      expect(result!.title).toBe('New Post')
    })
  })

  // ===========================================================================
  // Upsert result metadata
  // ===========================================================================

  describe('upsert result metadata', () => {
    it('indicates when entity was created', async () => {
      const result = await db.update(
        'posts',
        'posts/meta-new-123',
        {
          $set: { title: 'New Post' },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      // The version should be 1 for newly created
      expect(result!.version).toBe(1)
    })

    it('indicates when entity was updated', async () => {
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Existing',
        title: 'Original',
        content: 'Content',
      })

      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $set: { title: 'Updated' },
        },
        {
          upsert: true,
        }
      )

      expect(result).not.toBeNull()
      // Version incremented means it was an update
      expect(result!.version).toBe(2)
    })
  })

  // ===========================================================================
  // Proxy-based access
  // ===========================================================================

  describe('proxy-based access', () => {
    it('works with collection.update() and upsert option', async () => {
      const posts = (db as any).Posts

      const result = await posts.update(
        'posts/proxy-upsert-123',
        { $set: { title: 'Via Proxy' } },
        { upsert: true }
      )

      expect(result).not.toBeNull()
      expect(result.title).toBe('Via Proxy')
    })

    it('works with collection.upsert() method', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsert(
        { slug: 'proxy-slug' },
        {
          $set: { title: 'Via Proxy Upsert' },
          $setOnInsert: { viewCount: 0 },
        }
      )

      expect(result).not.toBeNull()
      expect(result.slug).toBe('proxy-slug')
      expect(result.title).toBe('Via Proxy Upsert')
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty update with upsert', async () => {
      const result = await db.update(
        'posts',
        'posts/empty-123',
        {},
        { upsert: true }
      )

      // Should create with just the ID
      expect(result).not.toBeNull()
      expect(result!.$id).toBe('posts/empty-123')
    })

    it('handles $unset on non-existent entity', async () => {
      const result = await db.update(
        'posts',
        'posts/unset-123',
        {
          $unset: { nonExistentField: '' },
        },
        { upsert: true }
      )

      expect(result).not.toBeNull()
      expect(result!.nonExistentField).toBeUndefined()
    })

    it('preserves existing fields not in update', async () => {
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
        customField: 'custom value',
        nested: { deep: 'value' },
      })

      const result = await db.update(
        'posts',
        created.$id as string,
        {
          $set: { title: 'Updated Title' },
        },
        { upsert: true }
      )

      expect(result).not.toBeNull()
      expect(result!.title).toBe('Updated Title')
      expect(result!.content).toBe('Content')
      expect(result!.customField).toBe('custom value')
      expect((result!.nested as any).deep).toBe('value')
    })

    it('handles concurrent upserts correctly', async () => {
      // Simulate concurrent upserts to the same entity
      const promises = [
        db.update(
          'posts',
          'posts/concurrent-123',
          { $inc: { count: 1 }, $set: { name: 'Post' } },
          { upsert: true }
        ),
        db.update(
          'posts',
          'posts/concurrent-123',
          { $inc: { count: 1 }, $set: { name: 'Post' } },
          { upsert: true }
        ),
      ]

      // One should succeed, one might get version conflict
      const results = await Promise.allSettled(promises)

      // At least one should succeed
      const successes = results.filter(r => r.status === 'fulfilled')
      expect(successes.length).toBeGreaterThanOrEqual(1)

      // Final count should reflect the updates
      const final = await db.get('posts', 'posts/concurrent-123')
      expect(final).not.toBeNull()
      expect(final!.count).toBeGreaterThanOrEqual(1)
    })
  })
})

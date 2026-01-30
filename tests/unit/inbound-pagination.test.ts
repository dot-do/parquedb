/**
 * Inbound Reference Pagination Tests
 *
 * Tests for paginating through inbound (reverse) relationships using real storage.
 * Tests cover $count, $next cursor, maxInbound limits, and filtering.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type {
  EntityId,
  Schema,
  RelSet,
} from '../../src/types'

// =============================================================================
// Test Schema
// =============================================================================

function createBlogSchema(): Schema {
  return {
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      posts: '<- Post.author[]',
      comments: '<- Comment.author[]',
    },
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      status: { type: 'string', default: 'draft' },
      author: '-> User.posts',
      comments: '<- Comment.post[]',
    },
    Category: {
      $type: 'schema:Category',
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      posts: '<- Post.categories[]',
    },
    Comment: {
      $type: 'schema:Comment',
      $ns: 'comments',
      text: 'string!',
      post: '-> Post.comments',
      author: '-> User.comments',
    },
    Tag: {
      $type: 'schema:Tag',
      $ns: 'tags',
      name: 'string!',
      // This could have thousands of posts
      posts: '<- Post.tags[]',
    },
  }
}

// =============================================================================
// Helper to create many posts
// =============================================================================

async function createManyPosts(
  db: ParqueDB,
  authorId: EntityId,
  count: number,
  categoryId?: EntityId
): Promise<EntityId[]> {
  const postIds: EntityId[] = []

  for (let i = 0; i < count; i++) {
    const post = await db.create('posts', {
      $type: 'Post',
      name: `Post ${i + 1}`,
      title: `Title ${i + 1}`,
      content: `Content for post ${i + 1}`,
      status: i % 2 === 0 ? 'published' : 'draft',
      author: { 'Author': authorId },
      ...(categoryId ? { categories: { 'Category': categoryId } } : {}),
    })
    postIds.push(post.$id as EntityId)
  }

  return postIds
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Inbound Reference Pagination', () => {
  let db: ParqueDB
  let tempDir: string
  let storage: FsBackend
  let schema: Schema

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-inbound-test-'))
    storage = new FsBackend(tempDir)
    schema = createBlogSchema()
    db = new ParqueDB({ storage, schema })
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    await rm(tempDir, { recursive: true, force: true })
  })

  // ===========================================================================
  // $count
  // ===========================================================================

  describe('$count of total references', () => {
    it('returns $count of total references', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Prolific Author',
        email: 'author@example.com',
      })

      // Create 25 posts for this user
      await createManyPosts(db, user.$id as EntityId, 25)

      // Get user with posts
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
      })

      expect(userWithPosts).not.toBeNull()
      expect(userWithPosts!.posts).toBeDefined()

      // The posts should be a RelSet with $count
      const posts = userWithPosts!.posts as RelSet
      expect(posts.$count).toBe(25)
    })

    it('returns accurate $count even with maxInbound limit', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Prolific Author',
        email: 'author@example.com',
      })

      // Create 100 posts
      await createManyPosts(db, user.$id as EntityId, 100)

      // Get with limited maxInbound
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 10,
      })

      const posts = userWithPosts!.posts as RelSet
      // Should return total count even though only 10 are inlined
      expect(posts.$count).toBe(100)
      // But only 10 entity links should be present
      const linkCount = Object.keys(posts).filter(k => !k.startsWith('$')).length
      expect(linkCount).toBe(10)
    })

    it('returns 0 for empty relationships', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'New Author',
        email: 'new@example.com',
      })

      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
      })

      const posts = userWithPosts!.posts as RelSet
      expect(posts.$count).toBe(0)
    })
  })

  // ===========================================================================
  // $next cursor
  // ===========================================================================

  describe('$next cursor for pagination', () => {
    it('returns $next cursor for pagination', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create more posts than maxInbound
      await createManyPosts(db, user.$id as EntityId, 50)

      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 10,
      })

      const posts = userWithPosts!.posts as RelSet
      expect(posts.$count).toBe(50)
      expect(posts.$next).toBeDefined()
      expect(typeof posts.$next).toBe('string')
    })

    it('returns no $next when all references fit', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create fewer posts than maxInbound
      await createManyPosts(db, user.$id as EntityId, 5)

      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 10,
      })

      const posts = userWithPosts!.posts as RelSet
      expect(posts.$count).toBe(5)
      expect(posts.$next).toBeUndefined()
    })

    it('uses cursor to fetch next page', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create 30 posts
      const allPostIds = await createManyPosts(db, user.$id as EntityId, 30)

      // Get first page
      const page1 = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 10,
      })

      const posts1 = page1!.posts as RelSet
      expect(posts1.$count).toBe(30)
      expect(posts1.$next).toBeDefined()

      const page1PostIds = Object.values(posts1).filter(
        (v): v is EntityId => typeof v === 'string' && v.includes('/')
      )
      expect(page1PostIds).toHaveLength(10)

      // Get second page using cursor
      const page2 = await db.getRelated('users', user.$id as string, 'posts', {
        cursor: posts1.$next,
        limit: 10,
      })

      expect(page2.items).toHaveLength(10)

      // Post IDs should not overlap
      const page2PostIds = page2.items.map(p => p.$id)
      const overlap = page1PostIds.filter(id => page2PostIds.includes(id))
      expect(overlap).toHaveLength(0)
    })
  })

  // ===========================================================================
  // maxInbound limit
  // ===========================================================================

  describe('maxInbound limit', () => {
    it('respects maxInbound limit', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 100)

      // Request with maxInbound of 5
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 5,
      })

      const posts = userWithPosts!.posts as RelSet
      const linkCount = Object.keys(posts).filter(k => !k.startsWith('$')).length
      expect(linkCount).toBe(5)
      expect(posts.$count).toBe(100)
      expect(posts.$next).toBeDefined()
    })

    it('uses default maxInbound when not specified', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 500)

      // Default maxInbound should be reasonable (e.g., 100)
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
      })

      const posts = userWithPosts!.posts as RelSet
      const linkCount = Object.keys(posts).filter(k => !k.startsWith('$')).length
      // Should be limited to default, not all 500
      expect(linkCount).toBeLessThanOrEqual(100)
      expect(posts.$count).toBe(500)
    })

    it('maxInbound of 0 returns only metadata', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 50)

      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 0,
      })

      const posts = userWithPosts!.posts as RelSet
      const linkCount = Object.keys(posts).filter(k => !k.startsWith('$')).length
      expect(linkCount).toBe(0)
      expect(posts.$count).toBe(50)
      expect(posts.$next).toBeDefined()
    })
  })

  // ===========================================================================
  // Pagination through large reference sets
  // ===========================================================================

  describe('paginates through large reference sets', () => {
    it('paginates through large reference sets', async () => {
      const category = await db.create('categories', {
        $type: 'Category',
        name: 'Popular Category',
        slug: 'popular',
      })

      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create 100 posts in this category
      await createManyPosts(db, user.$id as EntityId, 100, category.$id as EntityId)

      // Paginate through all posts
      let allPostIds: EntityId[] = []
      let cursor: string | undefined

      do {
        const result = await db.getRelated('categories', category.$id as string, 'posts', {
          cursor,
          limit: 20,
        })

        allPostIds = [...allPostIds, ...result.items.map(p => p.$id as EntityId)]
        cursor = result.nextCursor

      } while (cursor)

      // Should have collected all 100 posts
      expect(allPostIds).toHaveLength(100)

      // All IDs should be unique
      const uniqueIds = new Set(allPostIds)
      expect(uniqueIds.size).toBe(100)
    })

    it('maintains consistent ordering during pagination', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 30)

      // Get all posts in multiple pages
      const allPosts: EntityId[] = []
      let cursor: string | undefined

      do {
        const result = await db.getRelated('users', user.$id as string, 'posts', {
          cursor,
          limit: 10,
          sort: { createdAt: -1 }, // Newest first
        })

        allPosts.push(...result.items.map(p => p.$id as EntityId))
        cursor = result.nextCursor
      } while (cursor)

      // Verify ordering is consistent
      // Fetch all at once and compare
      const allAtOnce = await db.getRelated('users', user.$id as string, 'posts', {
        sort: { createdAt: -1 },
      })

      expect(allPosts).toEqual(allAtOnce.items.map(p => p.$id))
    })
  })

  // ===========================================================================
  // Filtering inbound references
  // ===========================================================================

  describe('filters inbound references', () => {
    it('filters inbound references', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create 20 posts - half published, half draft
      await createManyPosts(db, user.$id as EntityId, 20)

      // Get only published posts
      const publishedPosts = await db.getRelated('users', user.$id as string, 'posts', {
        filter: { status: 'published' },
      })

      // Half should be published (0, 2, 4, 6, 8, 10, 12, 14, 16, 18)
      expect(publishedPosts.items).toHaveLength(10)
      expect(publishedPosts.items.every(p => p.status === 'published')).toBe(true)
    })

    it('filters with complex conditions', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create posts with different titles
      for (let i = 0; i < 30; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: i < 10 ? `Featured: Title ${i}` : `Regular: Title ${i}`,
          content: `Content ${i}`,
          status: i % 2 === 0 ? 'published' : 'draft',
          viewCount: i * 10,
          author: { 'Author': user.$id as EntityId },
        })
      }

      // Filter: published AND viewCount >= 100 AND title starts with "Featured"
      const filtered = await db.getRelated('users', user.$id as string, 'posts', {
        filter: {
          $and: [
            { status: 'published' },
            { viewCount: { $gte: 100 } },
            { title: { $regex: '^Featured' } },
          ],
        },
      })

      // Verify all results match criteria
      filtered.items.forEach(post => {
        expect(post.status).toBe('published')
        expect(post.viewCount as number).toBeGreaterThanOrEqual(100)
        expect((post.title as string).startsWith('Featured')).toBe(true)
      })
    })

    it('returns filtered count', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 50)

      const publishedPosts = await db.getRelated('users', user.$id as string, 'posts', {
        filter: { status: 'published' },
        limit: 5,
      })

      // Total count should reflect filtered count, not all posts
      expect(publishedPosts.total).toBe(25) // Half of 50
      expect(publishedPosts.items).toHaveLength(5)
      expect(publishedPosts.hasMore).toBe(true)
    })
  })

  // ===========================================================================
  // Sorting inbound references
  // ===========================================================================

  describe('sorting inbound references', () => {
    it('sorts by field ascending', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create posts with different view counts
      for (let i = 0; i < 10; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
          viewCount: Math.random() * 1000,
          author: { 'Author': user.$id as EntityId },
        })
      }

      const sorted = await db.getRelated('users', user.$id as string, 'posts', {
        sort: { viewCount: 1 }, // Ascending
      })

      // Verify ascending order
      for (let i = 1; i < sorted.items.length; i++) {
        expect(sorted.items[i].viewCount as number).toBeGreaterThanOrEqual(
          sorted.items[i - 1].viewCount as number
        )
      }
    })

    it('sorts by field descending', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 10)

      const sorted = await db.getRelated('users', user.$id as string, 'posts', {
        sort: { createdAt: -1 }, // Descending - newest first
      })

      // Verify descending order
      for (let i = 1; i < sorted.items.length; i++) {
        expect(new Date(sorted.items[i].createdAt as Date).getTime()).toBeLessThanOrEqual(
          new Date(sorted.items[i - 1].createdAt as Date).getTime()
        )
      }
    })

    it('sorts by multiple fields', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 20)

      const sorted = await db.getRelated('users', user.$id as string, 'posts', {
        sort: { status: 1, createdAt: -1 }, // Status asc, then createdAt desc
      })

      expect(sorted.items.length).toBeGreaterThan(0)
      // Drafts should come first (alphabetically), then within same status, newest first
    })
  })

  // ===========================================================================
  // Hydration with pagination
  // ===========================================================================

  describe('hydration with pagination', () => {
    it('returns fully hydrated entities in page', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 10)

      const posts = await db.getRelated('users', user.$id as string, 'posts', {
        limit: 5,
      })

      // Each post should be fully hydrated
      posts.items.forEach(post => {
        expect(post.$id).toBeDefined()
        expect(post.$type).toBe('Post')
        expect(post.name).toBeDefined()
        expect(post.title).toBeDefined()
        expect(post.content).toBeDefined()
        expect(post.createdAt).toBeDefined()
        expect(post.version).toBeDefined()
      })
    })

    it('supports projection in getRelated', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 10)

      const posts = await db.getRelated('users', user.$id as string, 'posts', {
        project: { title: 1, status: 1 },
      })

      posts.items.forEach(post => {
        expect(post.$id).toBeDefined() // Always included
        expect(post.title).toBeDefined()
        expect(post.status).toBeDefined()
        // Content should not be included
        expect(post.content).toBeUndefined()
      })
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles cursor that becomes invalid', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const postIds = await createManyPosts(db, user.$id as EntityId, 20)

      // Get first page
      const page1 = await db.getRelated('users', user.$id as string, 'posts', {
        limit: 10,
      })

      // Delete some posts that would be in page 2
      await db.delete('posts', postIds[15])
      await db.delete('posts', postIds[16])

      // Using cursor should still work, just with fewer results
      const page2 = await db.getRelated('users', user.$id as string, 'posts', {
        cursor: page1.nextCursor,
        limit: 10,
      })

      expect(page2.items.length).toBeLessThanOrEqual(10)
    })

    it('handles very large maxInbound gracefully', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await createManyPosts(db, user.$id as EntityId, 10)

      // Request more than exists
      const userWithPosts = await db.get('users', user.$id as string, {
        hydrate: ['posts'],
        maxInbound: 1000000,
      })

      const posts = userWithPosts!.posts as RelSet
      expect(posts.$count).toBe(10)
      expect(posts.$next).toBeUndefined()
    })

    it('returns empty result for non-existent relationship', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'New User',
        email: 'new@example.com',
      })

      const posts = await db.getRelated('users', user.$id as string, 'posts')

      expect(posts.items).toHaveLength(0)
      expect(posts.total).toBe(0)
      expect(posts.hasMore).toBe(false)
    })

    it('excludes soft-deleted from inbound by default', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const postIds = await createManyPosts(db, user.$id as EntityId, 10)

      // Soft delete 3 posts
      await db.delete('posts', postIds[0])
      await db.delete('posts', postIds[5])
      await db.delete('posts', postIds[9])

      const posts = await db.getRelated('users', user.$id as string, 'posts')

      expect(posts.items).toHaveLength(7)
      expect(posts.total).toBe(7)
    })

    it('includes soft-deleted with option', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const postIds = await createManyPosts(db, user.$id as EntityId, 10)

      await db.delete('posts', postIds[0])
      await db.delete('posts', postIds[5])

      const posts = await db.getRelated('users', user.$id as string, 'posts', {
        includeDeleted: true,
      })

      expect(posts.total).toBe(10)
    })
  })
})

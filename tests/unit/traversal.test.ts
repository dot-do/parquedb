/**
 * Tests for relationship traversal in ParqueDB
 *
 * Tests the bidirectional relationship traversal API using real FsBackend storage.
 * - getRelated(namespace, id, predicate, options) - Get outbound related entities
 * - Inbound references via reverse relationships
 *
 * NO MOCKS - Uses real FsBackend with temp directories.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParqueDB } from '../../src/ParqueDB'
import type {
  Entity,
  EntityId,
  Schema,
} from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  publishedAt?: Date
}

interface User {
  email: string
  role: 'admin' | 'author' | 'reader'
}

interface Comment {
  text: string
  rating?: number
  createdAt: Date
}

interface Category {
  slug: string
}

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
      role: 'string!',
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
      publishedAt: 'datetime?',
      author: '-> User.posts',
      categories: '-> Category.posts[]',
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
      name: 'string!',
      text: 'string!',
      rating: 'int?',
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// Helper to extract local ID from full EntityId
function localId(entityId: EntityId): string {
  return entityId.split('/')[1]
}

// =============================================================================
// Test Setup
// =============================================================================

describe('Relationship Traversal', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB
  let aliceId: EntityId
  let bobId: EntityId
  let charlieId: EntityId
  let techCategoryId: EntityId
  let dbCategoryId: EntityId
  let post1Id: EntityId
  let post2Id: EntityId
  let post3Id: EntityId
  let comment1Id: EntityId
  let comment2Id: EntityId
  let comment3Id: EntityId

  beforeEach(async () => {
    // Create temp directory
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-traversal-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage, schema: createBlogSchema() })

    // Create users
    const alice = await db.create('users', {
      $type: 'User',
      name: 'Alice Johnson',
      email: 'alice@example.com',
      role: 'author',
    })
    aliceId = alice.$id

    const bob = await db.create('users', {
      $type: 'User',
      name: 'Bob Smith',
      email: 'bob@example.com',
      role: 'reader',
    })
    bobId = bob.$id

    const charlie = await db.create('users', {
      $type: 'User',
      name: 'Charlie Brown',
      email: 'charlie@example.com',
      role: 'admin',
    })
    charlieId = charlie.$id

    // Create categories
    const tech = await db.create('categories', {
      $type: 'Category',
      name: 'Tech',
      slug: 'tech',
    })
    techCategoryId = tech.$id

    const dbCat = await db.create('categories', {
      $type: 'Category',
      name: 'Database',
      slug: 'database',
    })
    dbCategoryId = dbCat.$id

    // Create posts
    const post1 = await db.create('posts', {
      $type: 'Post',
      name: 'First Post',
      title: 'Getting Started with ParqueDB',
      content: 'This is the first post...',
      status: 'published',
      publishedAt: new Date('2024-01-10'),
    })
    post1Id = post1.$id

    await db.update('posts', post1Id, {
      $link: {
        author: aliceId,
        categories: [techCategoryId, dbCategoryId],
      },
    })

    const post2 = await db.create('posts', {
      $type: 'Post',
      name: 'Second Post',
      title: 'Advanced Queries in ParqueDB',
      content: 'In this post, we explore...',
      status: 'published',
      publishedAt: new Date('2024-01-15'),
    })
    post2Id = post2.$id

    await db.update('posts', post2Id, {
      $link: {
        author: aliceId,
        categories: [techCategoryId],
      },
    })

    const post3 = await db.create('posts', {
      $type: 'Post',
      name: 'Draft Post',
      title: 'Work in Progress',
      content: 'Draft content...',
      status: 'draft',
    })
    post3Id = post3.$id

    await db.update('posts', post3Id, {
      $link: { author: aliceId },
    })

    // Create comments
    const c1 = await db.create('comments', {
      $type: 'Comment',
      name: 'Great article!',
      text: 'Really helpful, thanks!',
      rating: 5,
    })
    comment1Id = c1.$id

    await db.update('comments', comment1Id, {
      $link: { post: post1Id, author: bobId },
    })

    const c2 = await db.create('comments', {
      $type: 'Comment',
      name: 'Question',
      text: 'How does this work with large datasets?',
      rating: 4,
    })
    comment2Id = c2.$id

    await db.update('comments', comment2Id, {
      $link: { post: post1Id, author: charlieId },
    })

    const c3 = await db.create('comments', {
      $type: 'Comment',
      name: 'Nice follow-up',
      text: 'The advanced queries section was helpful.',
      rating: 5,
    })
    comment3Id = c3.$id

    await db.update('comments', comment3Id, {
      $link: { post: post2Id, author: bobId },
    })
  })

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true })
  })

  // =============================================================================
  // Test Suite: getRelated() - Forward Relationship Traversal
  // =============================================================================

  describe('getRelated() - Forward Relationship Traversal', () => {
    describe('Basic traversal', () => {
      it('should get entities via forward relationship predicate', async () => {
        // Get the author of a post
        const authors = await db.getRelated<User>('posts', localId(post1Id), 'author')

        expect(authors.items).toHaveLength(1)
        expect(authors.items[0]?.$id).toBe(aliceId)
        expect(authors.items[0]?.name).toBe('Alice Johnson')
      })

      it('should get multiple related entities', async () => {
        // Get categories of a post
        const categories = await db.getRelated<Category>('posts', localId(post1Id), 'categories')

        expect(categories.items).toHaveLength(2)
        const slugs = categories.items.map((c) => c.slug)
        expect(slugs).toContain('tech')
        expect(slugs).toContain('database')
      })

      it('should return empty result when no relationships exist', async () => {
        // Draft post has no categories
        const categories = await db.getRelated<Category>('posts', localId(post3Id), 'categories')

        expect(categories.items).toHaveLength(0)
        expect(categories.hasMore).toBe(false)
      })
    })

    describe('Pagination with limit and cursor', () => {
      it('should limit results when limit option is provided', async () => {
        // Alice has 3 posts, but we only want 2
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', { limit: 2 })

        expect(posts.items.length).toBeLessThanOrEqual(2)
        if (posts.total && posts.total > 2) {
          expect(posts.hasMore).toBe(true)
          expect(posts.nextCursor).toBeDefined()
        }
      })

      it('should return next page using cursor', async () => {
        // First page
        const page1 = await db.getRelated<Post>('users', localId(aliceId), 'posts', { limit: 2 })

        if (page1.hasMore && page1.nextCursor) {
          // Second page
          const page2 = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
            limit: 2,
            cursor: page1.nextCursor,
          })

          // All items should be unique
          const allIds = [...page1.items, ...page2.items].map((p) => p.$id)
          expect(new Set(allIds).size).toBe(allIds.length)
        }
      })

      it('should include total count when requested', async () => {
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', { limit: 1 })

        expect(posts.total).toBeDefined()
        expect(posts.total).toBeGreaterThanOrEqual(1)
      })
    })

    describe('Filtering related entities', () => {
      it('should filter related entities with simple equality', async () => {
        const published = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
          filter: { status: 'published' },
        })

        expect(published.items.length).toBe(2)
        published.items.forEach((post) => {
          expect(post.status).toBe('published')
        })
      })

      it('should filter with comparison operators', async () => {
        const highRatedComments = await db.getRelated<Comment>('posts', localId(post1Id), 'comments', {
          filter: { rating: { $gte: 5 } },
        })

        highRatedComments.items.forEach(comment => {
          expect(comment.rating).toBeGreaterThanOrEqual(5)
        })
      })

      it('should combine filter with pagination', async () => {
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
          filter: { status: 'published' },
          limit: 1,
        })

        expect(posts.items.length).toBeLessThanOrEqual(1)
        if (posts.items.length > 0) {
          expect(posts.items[0].status).toBe('published')
        }
      })
    })

    describe('Sorting related entities', () => {
      it('should sort by single field ascending', async () => {
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
          sort: { createdAt: 1 },
        })

        if (posts.items.length > 1) {
          const dates = posts.items.map((p) => p.createdAt.getTime())
          for (let i = 1; i < dates.length; i++) {
            expect(dates[i]).toBeGreaterThanOrEqual(dates[i - 1])
          }
        }
      })

      it('should sort by single field descending', async () => {
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
          sort: { createdAt: -1 },
        })

        if (posts.items.length > 1) {
          const dates = posts.items.map((p) => p.createdAt.getTime())
          for (let i = 1; i < dates.length; i++) {
            expect(dates[i]).toBeLessThanOrEqual(dates[i - 1])
          }
        }
      })

      it('should combine sort with filter and pagination', async () => {
        const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
          filter: { status: 'published' },
          sort: { publishedAt: -1 },
          limit: 1,
        })

        expect(posts.items.length).toBeLessThanOrEqual(1)
        if (posts.items.length > 0) {
          expect(posts.items[0].status).toBe('published')
        }
      })
    })
  })

  // =============================================================================
  // Test Suite: Inbound References (Reverse Relationship Traversal)
  // =============================================================================

  describe('Inbound References via get()', () => {
    describe('Basic retrieval', () => {
      it('should get entities that reference this one', async () => {
        // Get user with posts inbound
        const user = await db.get<User>('users', aliceId)
        expect(user).not.toBeNull()
        expect(user!.posts).toBeDefined()

        const posts = user!.posts as { [key: string]: EntityId }
        const postIds = Object.values(posts).filter(v => typeof v === 'string' && v.startsWith('posts/'))
        expect(postIds.length).toBeGreaterThan(0)
      })

      it('should return empty result when no references exist', async () => {
        // Charlie has no posts authored (only comments)
        const user = await db.get<User>('users', charlieId)
        const posts = user!.posts as { [key: string]: EntityId } | undefined

        if (posts) {
          const postIds = Object.values(posts).filter(v => typeof v === 'string' && v.startsWith('posts/'))
          expect(postIds.length).toBe(0)
        }
      })
    })

    describe('Pagination with $count and $next', () => {
      it('should limit results with maxInbound option', async () => {
        const user = await db.get<User>('users', aliceId, { maxInbound: 2 })
        const posts = user!.posts as { [key: string]: EntityId | number | string }

        const postIds = Object.entries(posts).filter(([k, v]) =>
          !k.startsWith('$') && typeof v === 'string' && v.startsWith('posts/')
        )

        expect(postIds.length).toBeLessThanOrEqual(2)
      })

      it('should include $count in RelSet response', async () => {
        const user = await db.get<User>('users', aliceId, { maxInbound: 1 })
        const posts = user!.posts as { $count?: number; [key: string]: unknown }

        expect(posts.$count).toBeDefined()
        expect(typeof posts.$count).toBe('number')
      })
    })
  })

  // =============================================================================
  // Test Suite: Multi-hop Traversal
  // =============================================================================

  describe('Multi-hop traversal', () => {
    it('should traverse through multiple relationship hops', async () => {
      // Get all comments on all posts by Alice
      // user -> posts -> comments
      const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts')

      const allComments: Entity<Comment>[] = []
      for (const post of posts.items) {
        const comments = await db.getRelated<Comment>('posts', localId(post.$id), 'comments')
        allComments.push(...comments.items)
      }

      expect(allComments.length).toBeGreaterThan(0)
    })

    it('should handle circular relationships safely', async () => {
      // user -> posts -> author (back to users)
      // Should not cause infinite loops
      const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts')

      for (const post of posts.items) {
        const authors = await db.getRelated<User>('posts', localId(post.$id), 'author')

        // Should return the author without infinite recursion
        expect(authors.items).toHaveLength(1)
        expect(authors.items[0]?.$id).toBe(aliceId)
      }
    })
  })

  // =============================================================================
  // Test Suite: Edge Cases
  // =============================================================================

  describe('Edge cases', () => {
    it('should handle entity with no relationships defined', async () => {
      // Categories might not have outbound relationships
      const result = await db.getRelated<Post>('categories', localId(techCategoryId), 'posts')
      expect(result.items).toBeDefined()
    })

    it('should handle concurrent traversal operations', async () => {
      // Run multiple traversals concurrently
      const [authors, categories] = await Promise.all([
        db.getRelated('posts', localId(post1Id), 'author'),
        db.getRelated('posts', localId(post1Id), 'categories'),
      ])

      expect(authors.items).toBeDefined()
      expect(categories.items).toBeDefined()
    })
  })

  // =============================================================================
  // Test Suite: Collection-based API
  // =============================================================================

  describe('Collection-based traversal API', () => {
    it('should support chained traversal from collection.get()', async () => {
      const post = await db.get<Post>('posts', post1Id)
      expect(post).not.toBeNull()

      // Should have author relationship
      expect(post!.author).toBeDefined()
    })

    it('should work across different collection types', async () => {
      // Traverse from Users collection
      const user = await db.get<User>('users', aliceId)
      expect(user!.posts).toBeDefined()

      // Traverse from Comments collection
      const comment = await db.get<Comment>('comments', comment1Id)
      expect(comment).not.toBeNull()
    })

    it('should maintain type safety through traversal', async () => {
      const authors = await db.getRelated<User>('posts', localId(post1Id), 'author')

      // TypeScript should know this is User[]
      if (authors.items.length > 0) {
        const authorEmails = authors.items.map((a) => a.email)
        expect(authorEmails).toContain('alice@example.com')
      }
    })
  })
})

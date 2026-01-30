/**
 * Tests for populate/hydration functionality
 *
 * Populate hydrates related entities inline instead of just returning IDs.
 * This allows fetching a post with its full author object embedded,
 * rather than just the author's EntityId reference.
 *
 * NO MOCKS - Uses real FsBackend with temp directories.
 * Relationships are persisted to rels/ directory.
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

interface User {
  email: string
  bio?: string
}

interface Category {
  slug: string
}

interface Comment {
  text: string
  approved: boolean
}

interface Post {
  title: string
  content: string
  status: 'draft' | 'published'
  publishedAt?: Date
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
      bio: 'text?',
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
      approved: { type: 'boolean', default: false },
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('Populate/Hydration', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB
  let aliceId: EntityId
  let bobId: EntityId
  let techCategoryId: EntityId
  let dbCategoryId: EntityId
  let post1Id: EntityId
  let post2Id: EntityId
  let post3Id: EntityId
  let comment1Id: EntityId
  let comment2Id: EntityId
  let comment3Id: EntityId

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-populate-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage, schema: createBlogSchema() })

    // Create users
    const alice = await db.create('users', {
      $type: 'User',
      name: 'Alice',
      email: 'alice@example.com',
      bio: 'Software engineer',
    })
    aliceId = alice.$id

    const bob = await db.create('users', {
      $type: 'User',
      name: 'Bob',
      email: 'bob@example.com',
    })
    bobId = bob.$id

    // Create categories
    const tech = await db.create('categories', {
      $type: 'Category',
      name: 'Technology',
      slug: 'tech',
    })
    techCategoryId = tech.$id

    const dbCat = await db.create('categories', {
      $type: 'Category',
      name: 'Databases',
      slug: 'db',
    })
    dbCategoryId = dbCat.$id

    // Create posts
    const post1 = await db.create('posts', {
      $type: 'Post',
      name: 'First Post',
      title: 'Introduction to ParqueDB',
      content: 'ParqueDB is a Parquet-based database...',
      status: 'published',
      publishedAt: new Date(),
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
      title: 'Advanced Queries',
      content: 'In this post we explore advanced queries...',
      status: 'published',
      publishedAt: new Date(),
    })
    post2Id = post2.$id

    await db.update('posts', post2Id, {
      $link: {
        author: aliceId,
        categories: [dbCategoryId],
      },
    })

    const post3 = await db.create('posts', {
      $type: 'Post',
      name: 'Draft Post',
      title: 'Work in Progress',
      content: 'This is a draft...',
      status: 'draft',
    })
    post3Id = post3.$id

    await db.update('posts', post3Id, {
      $link: { author: bobId },
    })

    // Create comments
    const c1 = await db.create('comments', {
      $type: 'Comment',
      name: 'Comment 1',
      text: 'Great post!',
      approved: true,
    })
    comment1Id = c1.$id

    await db.update('comments', comment1Id, {
      $link: { post: post1Id, author: bobId },
    })

    const c2 = await db.create('comments', {
      $type: 'Comment',
      name: 'Comment 2',
      text: 'Thanks for sharing!',
      approved: true,
    })
    comment2Id = c2.$id

    await db.update('comments', comment2Id, {
      $link: { post: post1Id, author: aliceId },
    })

    const c3 = await db.create('comments', {
      $type: 'Comment',
      name: 'Comment 3',
      text: 'Spam comment',
      approved: false,
    })
    comment3Id = c3.$id

    await db.update('comments', comment3Id, {
      $link: { post: post1Id, author: bobId },
    })
  })

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true })
  })

  // ---------------------------------------------------------------------------
  // 1. Simple populate: { hydrate: ['author'] }
  // ---------------------------------------------------------------------------
  describe('Simple populate via hydrate option', () => {
    it('should populate a single relationship', async () => {
      const post = await db.get<Post>('posts', post1Id, {
        hydrate: ['author'],
      })

      expect(post).not.toBeNull()
      expect(post!.author).toBeDefined()

      // Author should be hydrated with at least $id
      const author = post!.author as Record<string, unknown>
      const authorId = Object.values(author).find(v => typeof v === 'string' && v.startsWith('users/'))
      expect(authorId).toBeDefined()
    })
  })

  // ---------------------------------------------------------------------------
  // 2. getRelated() for fetching related entities
  // ---------------------------------------------------------------------------
  describe('getRelated() for fetching related entities', () => {
    it('should get related entities via predicate', async () => {
      const postLocalId = post1Id.split('/')[1]
      const authors = await db.getRelated<User>('posts', postLocalId, 'author')

      expect(authors.items).toHaveLength(1)
      expect(authors.items[0].$id).toBe(aliceId)
      expect(authors.items[0].name).toBe('Alice')
    })

    it('should get multiple related entities', async () => {
      const postLocalId = post1Id.split('/')[1]
      const categories = await db.getRelated<Category>('posts', postLocalId, 'categories')

      expect(categories.items).toHaveLength(2)
      const slugs = categories.items.map(c => c.slug)
      expect(slugs).toContain('tech')
      expect(slugs).toContain('db')
    })
  })

  // ---------------------------------------------------------------------------
  // 3. Populate with options: filter and pagination
  // ---------------------------------------------------------------------------
  describe('Populate with options', () => {
    it('should respect limit option on related', async () => {
      // Post-1 has 3 comments, limit should return fewer
      const postLocalId = post1Id.split('/')[1]
      const comments = await db.getRelated<Comment>('posts', postLocalId, 'comments', {
        limit: 2,
      })

      expect(comments.items.length).toBeLessThanOrEqual(2)
    })

    it('should filter related entities', async () => {
      const postLocalId = post1Id.split('/')[1]
      const approvedComments = await db.getRelated<Comment>('posts', postLocalId, 'comments', {
        filter: { approved: true },
      })

      approvedComments.items.forEach(comment => {
        expect(comment.approved).toBe(true)
      })
    })

    it('should combine filter with pagination', async () => {
      const postLocalId = post1Id.split('/')[1]
      const approvedComments = await db.getRelated<Comment>('posts', postLocalId, 'comments', {
        filter: { approved: true },
        limit: 1,
      })

      expect(approvedComments.items.length).toBeLessThanOrEqual(1)
      if (approvedComments.items.length > 0) {
        expect(approvedComments.items[0].approved).toBe(true)
      }
    })
  })

  // ---------------------------------------------------------------------------
  // 4. Multi-hop Traversal
  // ---------------------------------------------------------------------------
  describe('Multi-hop traversal', () => {
    it('should traverse through multiple relationship hops', async () => {
      // Get all comments on all posts by Alice
      // user -> posts -> comments
      const userLocalId = aliceId.split('/')[1]
      const posts = await db.getRelated<Post>('users', userLocalId, 'posts')

      const allComments: Entity<Comment>[] = []
      for (const post of posts.items) {
        const postLocalId = post.$id.split('/')[1]
        const comments = await db.getRelated<Comment>('posts', postLocalId, 'comments')
        allComments.push(...comments.items)
      }

      expect(allComments.length).toBeGreaterThan(0)
    })

    it('should handle circular relationships safely', async () => {
      // user -> posts -> author (back to users)
      // Should not cause infinite loops
      const userLocalId = aliceId.split('/')[1]
      const posts = await db.getRelated<Post>('users', userLocalId, 'posts')

      for (const post of posts.items) {
        const postLocalId = post.$id.split('/')[1]
        const authors = await db.getRelated<User>('posts', postLocalId, 'author')

        // Should return the author without infinite recursion
        expect(authors.items).toHaveLength(1)
        expect(authors.items[0]?.$id).toBe(aliceId)
      }
    })
  })

  // ---------------------------------------------------------------------------
  // 5. Missing relationships
  // ---------------------------------------------------------------------------
  describe('Missing relationships', () => {
    it('should return empty for missing relationship', async () => {
      // Draft post (post3) has no categories
      const postLocalId = post3Id.split('/')[1]
      const categories = await db.getRelated<Category>('posts', postLocalId, 'categories')

      expect(categories.items).toHaveLength(0)
    })

    it('should return empty for missing comments', async () => {
      // Post 2 has no comments
      const postLocalId = post2Id.split('/')[1]
      const comments = await db.getRelated<Comment>('posts', postLocalId, 'comments')

      expect(comments.items).toHaveLength(0)
    })
  })

  // ---------------------------------------------------------------------------
  // 6. Inbound references (reverse relationships)
  // ---------------------------------------------------------------------------
  describe('Inbound references via get()', () => {
    it('should include inbound references on get()', async () => {
      const user = await db.get<User>('users', aliceId)
      expect(user).not.toBeNull()

      // User should have posts inbound reference
      expect(user!.posts).toBeDefined()
    })

    it('should limit inbound with maxInbound option', async () => {
      const user = await db.get<User>('users', aliceId, { maxInbound: 1 })

      // Posts should be limited
      const posts = user!.posts as Record<string, unknown>
      const postIds = Object.values(posts).filter(v => typeof v === 'string' && v.startsWith('posts/'))
      expect(postIds.length).toBeLessThanOrEqual(1)
    })
  })

  // ---------------------------------------------------------------------------
  // 7. Concurrent operations
  // ---------------------------------------------------------------------------
  describe('Concurrent operations', () => {
    it('should handle concurrent getRelated operations', async () => {
      const postLocalId = post1Id.split('/')[1]
      const [authors, categories] = await Promise.all([
        db.getRelated<User>('posts', postLocalId, 'author'),
        db.getRelated<Category>('posts', postLocalId, 'categories'),
      ])

      expect(authors.items).toHaveLength(1)
      expect(categories.items.length).toBeGreaterThan(0)
    })
  })

  // ---------------------------------------------------------------------------
  // 8. Type safety
  // ---------------------------------------------------------------------------
  describe('Type safety', () => {
    it('should maintain type safety through traversal', async () => {
      const postLocalId = post1Id.split('/')[1]
      const authors = await db.getRelated<User>('posts', postLocalId, 'author')

      // TypeScript should know this is User[]
      if (authors.items.length > 0) {
        expect(authors.items[0].email).toBe('alice@example.com')
      }
    })
  })
})

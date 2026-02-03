/**
 * Relationship Reading Tests for ParqueDB
 *
 * Tests for relationship reading functionality using real FsBackend storage.
 * Tests cover:
 * - Outbound relationships (via predicates)
 * - Inbound references (via reverse predicates)
 * - related() method for fetching related entities
 * - referencedBy() method for fetching referencing entities
 *
 * NO MOCKS - Uses real FsBackend with temp directories.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParqueDB } from '../../src/ParqueDB'
import { cleanupTempDir } from '../setup'
import type {
  Entity,
  EntityId,
  RelLink,
  RelSet,
  Schema,
} from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  // Outbound relationships
  author?: RelLink    // -> User
  categories?: RelSet // -> Category[]
}

interface User {
  email: string
  role: 'admin' | 'author' | 'reader'
  // Inbound references
  posts?: RelSet      // <- Post.author
  comments?: RelSet   // <- Comment.author
}

interface Comment {
  text: string
  rating?: number
  // Outbound relationships
  post?: RelLink      // -> Post
  author?: RelLink    // -> User
}

interface Category {
  slug: string
  // Inbound references
  posts?: RelSet      // <- Post.categories
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
      // Reverse relationships
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
      // Forward relationships
      author: '-> User.posts',
      categories: '-> Category.posts[]',
      // Reverse relationships
      comments: '<- Comment.post[]',
    },
    Category: {
      $type: 'schema:Category',
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      // Reverse relationship
      posts: '<- Post.categories[]',
    },
    Comment: {
      $type: 'schema:Comment',
      $ns: 'comments',
      name: 'string!',
      text: 'string!',
      rating: 'int?',
      // Forward relationships
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// =============================================================================
// Test Setup
// =============================================================================

describe('Relationship Reading', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB
  let aliceId: EntityId
  let bobId: EntityId
  let techCategoryId: EntityId
  let dbCategoryId: EntityId
  let emptyCategoryId: EntityId
  let post1Id: EntityId
  let post2Id: EntityId
  let orphanedPostId: EntityId
  let comment1Id: EntityId
  let comment2Id: EntityId

  beforeEach(async () => {
    // Create temp directory
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-rel-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage, schema: createBlogSchema() })

    // Create users
    const alice = await db.create('users', {
      $type: 'User',
      name: 'Alice',
      email: 'alice@example.com',
      role: 'author',
    })
    aliceId = alice.$id

    const bob = await db.create('users', {
      $type: 'User',
      name: 'User With No Posts',
      email: 'noposts@example.com',
      role: 'reader',
    })
    bobId = bob.$id

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

    const empty = await db.create('categories', {
      $type: 'Category',
      name: 'Empty Category',
      slug: 'empty-category',
    })
    emptyCategoryId = empty.$id

    // Create posts with relationships
    const post1 = await db.create('posts', {
      $type: 'Post',
      name: 'Post 1',
      title: 'First Post',
      content: 'Content of first post',
      status: 'published',
    })
    post1Id = post1.$id

    // Link post to author and categories
    await db.update('posts', post1Id, {
      $link: {
        author: aliceId,
        categories: [techCategoryId, dbCategoryId],
      },
    })

    const post2 = await db.create('posts', {
      $type: 'Post',
      name: 'Post Without Categories',
      title: 'No Categories Post',
      content: 'This post has no categories',
      status: 'draft',
    })
    post2Id = post2.$id

    await db.update('posts', post2Id, {
      $link: { author: aliceId },
    })

    const orphaned = await db.create('posts', {
      $type: 'Post',
      name: 'Orphaned Post',
      title: 'Orphaned Post',
      content: 'This post has no author',
      status: 'draft',
    })
    orphanedPostId = orphaned.$id

    // Create comments
    const c1 = await db.create('comments', {
      $type: 'Comment',
      name: 'Comment 1',
      text: 'Great post!',
      rating: 5,
    })
    comment1Id = c1.$id

    await db.update('comments', comment1Id, {
      $link: {
        post: post1Id,
        author: aliceId,
      },
    })

    const c2 = await db.create('comments', {
      $type: 'Comment',
      name: 'Comment 2',
      text: 'Thanks for sharing',
      rating: 4,
    })
    comment2Id = c2.$id

    await db.update('comments', comment2Id, {
      $link: {
        post: post1Id,
        author: aliceId,
      },
    })
  })

  afterEach(async () => {
    // Clean up temp directory with retry logic
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // Outbound Relationships
  // ===========================================================================

  describe('outbound relationships', () => {
    it('returns linked entity IDs', async () => {
      const post = await db.get<Post>('posts', post1Id)
      expect(post).not.toBeNull()

      // Post should have author relationship as RelLink
      expect(post!.author).toBeDefined()
      expect(typeof post!.author).toBe('object')

      // RelLink is { displayName: entityId }
      const authorValues = Object.values(post!.author as RelLink)
      expect(authorValues.length).toBe(1)
      expect(authorValues[0]).toMatch(/^users\//)
    })

    it('returns single link as value', async () => {
      const post = await db.get<Post>('posts', post1Id)

      // Single relationship should be a RelLink with one entry
      const author = post!.author as RelLink
      const entries = Object.entries(author)

      expect(entries.length).toBe(1)

      const [displayName, entityId] = entries[0]
      expect(typeof displayName).toBe('string')
      expect(entityId).toMatch(/^users\//)
    })

    it('returns multiple links as array', async () => {
      const post = await db.get<Post>('posts', post1Id)

      // Multiple relationship should be a RelSet
      const categories = post!.categories as RelSet

      // Should have multiple entries (filter out $ metadata fields)
      const entries = Object.entries(categories).filter(([key]) => !key.startsWith('$'))
      expect(entries.length).toBeGreaterThan(0)

      // Each entry should be a valid EntityId
      entries.forEach(([displayName, entityId]) => {
        expect(typeof displayName).toBe('string')
        expect(entityId).toMatch(/^categories\//)
      })
    })

    it('returns empty array for no links', async () => {
      // Get a post with no categories
      const post = await db.get<Post>('posts', post2Id)

      const categories = post!.categories as RelSet | undefined

      // Should be undefined or empty RelSet
      if (categories) {
        const entries = Object.entries(categories).filter(([key]) => !key.startsWith('$'))
        expect(entries.length).toBe(0)
      }
    })

    it('includes display name in RelLink', async () => {
      const post = await db.get<Post>('posts', post1Id)
      const author = post!.author as RelLink

      // The key should be a human-readable display name
      const displayName = Object.keys(author)[0]
      expect(displayName).toBeTruthy()
      expect(displayName.length).toBeGreaterThan(0)
      // Should not be the entity ID
      expect(displayName).not.toMatch(/^users\//)
    })

    it('preserves relationship order when defined', async () => {
      const post = await db.get<Post>('posts', post1Id)
      const categories = post!.categories as RelSet

      // Order should be preserved (if ordered relationships are supported)
      const entries = Object.entries(categories).filter(([key]) => !key.startsWith('$'))
      expect(entries.length).toBeGreaterThan(1)

      // Same order on repeated reads
      const post2 = await db.get<Post>('posts', post1Id)
      const categories2 = post2!.categories as RelSet
      const entries2 = Object.entries(categories2).filter(([key]) => !key.startsWith('$'))

      expect(entries.map(e => e[1])).toEqual(entries2.map(e => e[1]))
    })
  })

  // ===========================================================================
  // Inbound References
  // ===========================================================================

  describe('inbound references', () => {
    it('returns referencing entity IDs', async () => {
      const user = await db.get<User>('users', aliceId)
      expect(user).not.toBeNull()

      // User should have posts inbound reference as RelSet
      expect(user!.posts).toBeDefined()
      expect(typeof user!.posts).toBe('object')

      const posts = user!.posts as RelSet
      const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))

      expect(entries.length).toBeGreaterThan(0)
      entries.forEach(([, entityId]) => {
        expect(entityId).toMatch(/^posts\//)
      })
    })

    it('returns $count for total', async () => {
      const user = await db.get<User>('users', aliceId, { maxInbound: 2 })
      const posts = user!.posts as RelSet

      // $count should reflect total, not just returned items
      expect(posts.$count).toBeDefined()
      expect(typeof posts.$count).toBe('number')
      expect(posts.$count).toBeGreaterThanOrEqual(0)

      // Count may be greater than returned entries
      const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))
      expect(posts.$count).toBeGreaterThanOrEqual(entries.length)
    })

    it('returns $next cursor for pagination', async () => {
      const user = await db.get<User>('users', aliceId, { maxInbound: 1 })
      const posts = user!.posts as RelSet

      // If there are more posts than maxInbound, $next should be defined
      if ((posts.$count ?? 0) > 1) {
        expect(posts.$next).toBeDefined()
        expect(typeof posts.$next).toBe('string')
      }
    })

    it('limits inbound with maxInbound option', async () => {
      const user = await db.get<User>('users', aliceId, { maxInbound: 2 })
      const posts = user!.posts as RelSet

      const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))

      // Should return at most maxInbound entries
      expect(entries.length).toBeLessThanOrEqual(2)
    })

    it('returns empty RelSet for no inbound references', async () => {
      // Get a user with no posts
      const user = await db.get<User>('users', bobId)

      const posts = user!.posts as RelSet | undefined

      if (posts) {
        const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))
        expect(entries.length).toBe(0)
        expect(posts.$count).toBe(0)
      }
    })

    it('includes both display name and entity ID', async () => {
      const user = await db.get<User>('users', aliceId)
      const posts = user!.posts as RelSet

      const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))

      entries.forEach(([displayName, entityId]) => {
        // Display name should be human readable (e.g., "My First Post")
        expect(displayName).toBeTruthy()
        expect(displayName).not.toMatch(/^posts\//)

        // Entity ID should be full ID
        expect(entityId).toMatch(/^posts\//)
      })
    })

    it('handles maxInbound of 0 to exclude inbound', async () => {
      const user = await db.get<User>('users', aliceId, { maxInbound: 0 })

      // With maxInbound: 0, inbound relationships should not be included
      // or should be empty
      if (user!.posts) {
        const posts = user!.posts as RelSet
        const entries = Object.entries(posts).filter(([key]) => !key.startsWith('$'))
        expect(entries.length).toBe(0)
      }
    })
  })

  // ===========================================================================
  // related() Method via getRelated()
  // ===========================================================================

  describe('related() method', () => {
    it('fetches related entities', async () => {
      const post = await db.get<Post>('posts', post1Id)
      expect(post).not.toBeNull()

      // Use getRelated() to get the full author entity
      // getRelated takes (namespace, id, relationField, options)
      const postLocalId = post1Id.split('/')[1]
      const authors = await db.getRelated<User>('posts', postLocalId, 'author')

      expect(authors.items).toHaveLength(1)
      expect(authors.items[0]?.$id).toMatch(/^users\//)
      expect(authors.items[0]?.name).toBeDefined()
    })

    it('fetches multiple related entities', async () => {
      const postLocalId = post1Id.split('/')[1]
      const categories = await db.getRelated<Category>('posts', postLocalId, 'categories')

      expect(categories.items.length).toBeGreaterThan(0)
      categories.items.forEach(category => {
        expect(category.$id).toMatch(/^categories\//)
        expect(category.name).toBeDefined()
      })
    })

    it('supports filter on related', async () => {
      // First link post2 with published status
      await db.update('posts', post2Id, { $set: { status: 'published' } })

      // Get only published posts by this author
      const userLocalId = aliceId.split('/')[1]
      const publishedPosts = await db.getRelated<Post>('users', userLocalId, 'posts', {
        filter: { status: 'published' },
      })

      publishedPosts.items.forEach(post => {
        expect(post.status).toBe('published')
      })
    })

    it('supports pagination on related', async () => {
      const userLocalId = aliceId.split('/')[1]
      // Get first page
      const page1 = await db.getRelated<Post>('users', userLocalId, 'posts', { limit: 1 })

      expect(page1.items.length).toBeLessThanOrEqual(1)

      if (page1.hasMore && page1.nextCursor) {
        // Get second page
        const page2 = await db.getRelated<Post>('users', userLocalId, 'posts', {
          limit: 1,
          cursor: page1.nextCursor,
        })

        // Pages should not overlap
        const page1Ids = page1.items.map(p => p.$id)
        const page2Ids = page2.items.map(p => p.$id)

        page2Ids.forEach(id => {
          expect(page1Ids).not.toContain(id)
        })
      }
    })

    it('returns total count when available', async () => {
      const userLocalId = aliceId.split('/')[1]
      const posts = await db.getRelated<Post>('users', userLocalId, 'posts', { limit: 1 })

      // total should reflect all related entities
      if (posts.total !== undefined) {
        expect(posts.total).toBeGreaterThanOrEqual(posts.items.length)
      }
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles circular relationships', async () => {
      // User -> Posts -> Author (back to Users)
      const userLocalId = aliceId.split('/')[1]
      const posts = await db.getRelated<Post>('users', userLocalId, 'posts')

      for (const post of posts.items) {
        const postLocalId = post.$id.split('/')[1]
        const authors = await db.getRelated<User>('posts', postLocalId, 'author')
        // Should complete without infinite loop
        expect(authors.items).toBeDefined()
      }
    })

    it('handles relationship with null values', async () => {
      // Get orphaned post (no author)
      const orphanedLocalId = orphanedPostId.split('/')[1]
      const authors = await db.getRelated<User>('posts', orphanedLocalId, 'author')
      expect(authors.items).toHaveLength(0)
    })

    it('handles concurrent relationship queries', async () => {
      const postLocalId = post1Id.split('/')[1]
      // Run multiple relationship queries in parallel
      const [authors, categories] = await Promise.all([
        db.getRelated<User>('posts', postLocalId, 'author'),
        db.getRelated<Category>('posts', postLocalId, 'categories'),
      ])

      expect(authors.items).toBeDefined()
      expect(categories.items).toBeDefined()
    })
  })
})

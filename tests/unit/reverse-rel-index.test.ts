/**
 * Reverse Relationship Index Tests for ParqueDB
 *
 * Tests the reverse relationship index that fixes the N+1 query pattern.
 * The index provides O(1) lookups for reverse relationships instead of
 * scanning all entities.
 *
 * Issue: parquedb-ep6z - Fix N+1 query pattern in relationship traversal
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParqueDB } from '../../src/ParqueDB'
import type { Entity, EntityId, Schema } from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Author {
  bio?: string
  // Reverse relationship
  posts?: Record<string, EntityId>
}

interface Post {
  title: string
  content: string
  // Forward relationship
  author?: Record<string, EntityId>
  tags?: Record<string, EntityId>
}

interface Tag {
  slug: string
  // Reverse relationship
  posts?: Record<string, EntityId>
}

// =============================================================================
// Test Schema
// =============================================================================

function createTestSchema(): Schema {
  return {
    Author: {
      $type: 'schema:Person',
      $ns: 'authors',
      name: 'string!',
      bio: 'string?',
      posts: '<- Post.author[]',
    },
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'string!',
      author: '-> Author.posts',
      tags: '-> Tag.posts[]',
    },
    Tag: {
      $type: 'schema:Tag',
      $ns: 'tags',
      name: 'string!',
      slug: 'string!',
      posts: '<- Post.tags[]',
    },
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('Reverse Relationship Index', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-revrel-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage, schema: createTestSchema() })
  })

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true })
  })

  describe('index updates on $link', () => {
    it('indexes new relationships', async () => {
      // Create author and post
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Alice',
        bio: 'Writer',
      })
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'First Post',
        content: 'Hello World',
      })

      // Link post to author
      await db.update('posts', post.$id, {
        $link: { author: author.$id },
      })

      // Verify reverse lookup works via getRelated
      const authorLocalId = author.$id.split('/')[1]
      const authorPosts = await db.getRelated<Post>('authors', authorLocalId, 'posts')

      expect(authorPosts.items).toHaveLength(1)
      expect(authorPosts.items[0]?.$id).toBe(post.$id)
    })

    it('indexes multiple relationships to same target', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Bob',
      })

      // Create multiple posts
      const post1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })
      const post2 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })
      const post3 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 3',
        title: 'Title 3',
        content: 'Content 3',
      })

      // Link all posts to same author
      await db.update('posts', post1.$id, { $link: { author: author.$id } })
      await db.update('posts', post2.$id, { $link: { author: author.$id } })
      await db.update('posts', post3.$id, { $link: { author: author.$id } })

      // Verify all posts found via reverse lookup
      const authorLocalId = author.$id.split('/')[1]
      const authorPosts = await db.getRelated<Post>('authors', authorLocalId, 'posts')

      expect(authorPosts.items).toHaveLength(3)
      expect(authorPosts.total).toBe(3)
    })

    it('indexes array relationships (tags)', async () => {
      const tag1 = await db.create('tags', {
        $type: 'Tag',
        name: 'JavaScript',
        slug: 'javascript',
      })
      const tag2 = await db.create('tags', {
        $type: 'Tag',
        name: 'TypeScript',
        slug: 'typescript',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'JS/TS Post',
        content: 'Content',
      })

      // Link post to multiple tags
      await db.update('posts', post.$id, {
        $link: { tags: [tag1.$id, tag2.$id] },
      })

      // Verify reverse lookup for each tag
      const tag1LocalId = tag1.$id.split('/')[1]
      const tag1Posts = await db.getRelated<Post>('tags', tag1LocalId, 'posts')
      expect(tag1Posts.items).toHaveLength(1)

      const tag2LocalId = tag2.$id.split('/')[1]
      const tag2Posts = await db.getRelated<Post>('tags', tag2LocalId, 'posts')
      expect(tag2Posts.items).toHaveLength(1)
    })
  })

  describe('index updates on $unlink', () => {
    it('removes relationships from index', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Carol',
      })
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      })

      // Link then unlink
      await db.update('posts', post.$id, { $link: { author: author.$id } })

      // Verify link
      let authorLocalId = author.$id.split('/')[1]
      let authorPosts = await db.getRelated<Post>('authors', authorLocalId, 'posts')
      expect(authorPosts.items).toHaveLength(1)

      // Unlink
      await db.update('posts', post.$id, { $unlink: { author: author.$id } })

      // Verify unlink
      authorPosts = await db.getRelated<Post>('authors', authorLocalId, 'posts')
      expect(authorPosts.items).toHaveLength(0)
    })

    it('handles $unlink: "$all"', async () => {
      const tag1 = await db.create('tags', {
        $type: 'Tag',
        name: 'Tag1',
        slug: 'tag1',
      })
      const tag2 = await db.create('tags', {
        $type: 'Tag',
        name: 'Tag2',
        slug: 'tag2',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      })

      // Link to both tags
      await db.update('posts', post.$id, {
        $link: { tags: [tag1.$id, tag2.$id] },
      })

      // Verify both tags link to post
      let tag1Posts = await db.getRelated<Post>('tags', tag1.$id.split('/')[1], 'posts')
      expect(tag1Posts.items).toHaveLength(1)

      // Unlink all
      await db.update('posts', post.$id, { $unlink: { tags: '$all' } })

      // Verify both tags no longer link to post
      tag1Posts = await db.getRelated<Post>('tags', tag1.$id.split('/')[1], 'posts')
      expect(tag1Posts.items).toHaveLength(0)

      const tag2Posts = await db.getRelated<Post>('tags', tag2.$id.split('/')[1], 'posts')
      expect(tag2Posts.items).toHaveLength(0)
    })
  })

  describe('index updates on delete', () => {
    it('removes relationships when entity is hard deleted', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Dave',
      })
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, { $link: { author: author.$id } })

      // Verify link
      let authorPosts = await db.getRelated<Post>('authors', author.$id.split('/')[1], 'posts')
      expect(authorPosts.items).toHaveLength(1)

      // Hard delete post
      await db.delete('posts', post.$id, { hard: true })

      // Verify index is updated - author should have no posts
      authorPosts = await db.getRelated<Post>('authors', author.$id.split('/')[1], 'posts')
      expect(authorPosts.items).toHaveLength(0)
    })
  })

  describe('index with hydrate', () => {
    it('uses index for hydrating reverse relationships', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Eve',
        bio: 'Developer',
      })

      // Create multiple posts
      for (let i = 0; i < 5; i++) {
        const post = await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
        })
        await db.update('posts', post.$id, { $link: { author: author.$id } })
      }

      // Get author with hydrated posts (uses reverse index)
      const hydratedAuthor = await db.get<Author>('authors', author.$id, {
        hydrate: ['posts'],
      })

      expect(hydratedAuthor).not.toBeNull()
      expect(hydratedAuthor!.posts).toBeDefined()

      // Should have $count metadata
      const posts = hydratedAuthor!.posts as { $count: number }
      expect(posts.$count).toBe(5)
    })
  })

  describe('batched loading performance', () => {
    it('retrieves many relationships efficiently via getRelated', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Performance Test Author',
      })

      // Create 100 posts linked to this author
      const numPosts = 100
      for (let i = 0; i < numPosts; i++) {
        const post = await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
        })
        await db.update('posts', post.$id, { $link: { author: author.$id } })
      }

      // Time the reverse lookup
      const start = performance.now()
      const authorPosts = await db.getRelated<Post>('authors', author.$id.split('/')[1], 'posts')
      const elapsed = performance.now() - start

      // All posts should be retrieved
      expect(authorPosts.items).toHaveLength(numPosts)
      expect(authorPosts.total).toBe(numPosts)

      // With the index, this should be fast (< 50ms for 100 items)
      // Without the index (N+1), it would scan all entities repeatedly
      expect(elapsed).toBeLessThan(100) // Allow some buffer for CI environments
    })

    it('retrieves related entities from multiple sources efficiently', async () => {
      // Create tags
      const tag = await db.create('tags', {
        $type: 'Tag',
        name: 'Test Tag',
        slug: 'test-tag',
      })

      // Create 50 posts, each linked to the tag
      const numPosts = 50
      for (let i = 0; i < numPosts; i++) {
        const post = await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: `Content ${i}`,
        })
        await db.update('posts', post.$id, { $link: { tags: tag.$id } })
      }

      // Time the reverse lookup
      const start = performance.now()
      const tagPosts = await db.getRelated<Post>('tags', tag.$id.split('/')[1], 'posts')
      const elapsed = performance.now() - start

      expect(tagPosts.items).toHaveLength(numPosts)

      // Should be fast due to index
      expect(elapsed).toBeLessThan(100)
    })
  })

  describe('singular relationship replacement', () => {
    it('updates index when singular relationship is replaced', async () => {
      const author1 = await db.create('authors', {
        $type: 'Author',
        name: 'Author One',
      })
      const author2 = await db.create('authors', {
        $type: 'Author',
        name: 'Author Two',
      })
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      })

      // Link to first author
      await db.update('posts', post.$id, { $link: { author: author1.$id } })

      // Verify first author has the post
      let author1Posts = await db.getRelated<Post>('authors', author1.$id.split('/')[1], 'posts')
      expect(author1Posts.items).toHaveLength(1)

      // Replace with second author (singular relationship should clear first)
      await db.update('posts', post.$id, { $link: { author: author2.$id } })

      // First author should no longer have the post
      author1Posts = await db.getRelated<Post>('authors', author1.$id.split('/')[1], 'posts')
      expect(author1Posts.items).toHaveLength(0)

      // Second author should have the post
      const author2Posts = await db.getRelated<Post>('authors', author2.$id.split('/')[1], 'posts')
      expect(author2Posts.items).toHaveLength(1)
    })
  })

  describe('initial relationship data on create', () => {
    it('indexes relationships provided at entity creation', async () => {
      const author = await db.create('authors', {
        $type: 'Author',
        name: 'Inline Author',
      })

      // Create post with author already set
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Inline Post',
        title: 'Title',
        content: 'Content',
        author: { 'Inline Author': author.$id as EntityId },
      })

      // Verify index was populated at creation time
      const authorPosts = await db.getRelated<Post>('authors', author.$id.split('/')[1], 'posts')
      expect(authorPosts.items).toHaveLength(1)
      expect(authorPosts.items[0]?.$id).toBe(post.$id)
    })
  })
})

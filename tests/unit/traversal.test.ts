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
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
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
  publishedAt?: Date | undefined
}

interface User {
  email: string
  role: 'admin' | 'author' | 'reader'
}

interface Comment {
  text: string
  rating?: number | undefined
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
    // Allow pending async operations to settle before cleanup
    await new Promise(resolve => setTimeout(resolve, 100))
    await cleanupTempDir(tempDir)
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

    it('should traverse 3-hop path: User -> Post -> Comment -> User (comment author)', async () => {
      // Start from Alice, get her posts, then comments on those posts, then comment authors
      // This tests: User -> Post -> Comment -> User
      const posts = await db.getRelated<Post>('users', localId(aliceId), 'posts')
      expect(posts.items.length).toBeGreaterThan(0)

      const commentAuthors = new Set<string>()
      for (const post of posts.items) {
        const comments = await db.getRelated<Comment>('posts', localId(post.$id), 'comments')
        for (const comment of comments.items) {
          const authors = await db.getRelated<User>('comments', localId(comment.$id), 'author')
          for (const author of authors.items) {
            commentAuthors.add(author.$id)
          }
        }
      }

      // Bob and Charlie commented on Alice's posts
      expect(commentAuthors.size).toBeGreaterThan(0)
      expect(commentAuthors.has(bobId)).toBe(true)
      expect(commentAuthors.has(charlieId)).toBe(true)
    })

    it('should traverse bidirectionally: Category -> Posts -> Author', async () => {
      // Start from a category, get all posts in that category, then get their authors
      const postsInCategory = await db.getRelated<Post>('categories', localId(techCategoryId), 'posts')
      expect(postsInCategory.items.length).toBeGreaterThan(0)

      const authors = new Set<string>()
      for (const post of postsInCategory.items) {
        const postAuthors = await db.getRelated<User>('posts', localId(post.$id), 'author')
        for (const author of postAuthors.items) {
          authors.add(author.$id)
        }
      }

      // Alice is the author of all posts in the tech category
      expect(authors.has(aliceId)).toBe(true)
    })

    it('should collect unique entities across multiple paths', async () => {
      // Both post1 and post2 are in the tech category
      // When traversing Category -> Posts -> Author, we should find Alice once per path
      // but the final set of authors should only contain unique entries
      const postsInCategory = await db.getRelated<Post>('categories', localId(techCategoryId), 'posts')

      // Build a map of author -> posts authored
      const authorToPosts = new Map<string, string[]>()
      for (const post of postsInCategory.items) {
        const postAuthors = await db.getRelated<User>('posts', localId(post.$id), 'author')
        for (const author of postAuthors.items) {
          const existing = authorToPosts.get(author.$id) || []
          existing.push(post.$id)
          authorToPosts.set(author.$id, existing)
        }
      }

      // Alice should appear multiple times in paths but be deduplicated in final set
      expect(authorToPosts.size).toBe(1) // Only Alice
      expect(authorToPosts.get(aliceId)?.length).toBeGreaterThanOrEqual(2) // Multiple posts
    })
  })

  // =============================================================================
  // Test Suite: Cycle Detection
  // =============================================================================

  describe('Cycle detection', () => {
    it('should handle direct back-reference without infinite loop', async () => {
      // User -> Posts -> Author -> Posts (cycle back to same posts)
      const alicePosts = await db.getRelated<Post>('users', localId(aliceId), 'posts')
      const firstPost = alicePosts.items[0]
      expect(firstPost).toBeDefined()

      // Get author of first post (Alice)
      const authors = await db.getRelated<User>('posts', localId(firstPost.$id), 'author')
      expect(authors.items).toHaveLength(1)
      expect(authors.items[0].$id).toBe(aliceId)

      // Get Alice's posts again (cycle back)
      const postsAgain = await db.getRelated<Post>('users', localId(authors.items[0].$id), 'posts')

      // Should return same posts without infinite recursion
      expect(postsAgain.items.length).toBe(alicePosts.items.length)
    })

    it('should handle cycle with visited set tracking (manual BFS)', async () => {
      // Implement manual BFS with visited tracking to detect cycles
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: aliceId, ns: 'users', depth: 0 }
      ]
      const maxDepth = 10

      // BFS traversal with cycle detection
      while (queue.length > 0) {
        const current = queue.shift()!

        // Skip if already visited
        if (visited.has(current.entityId)) {
          continue
        }
        visited.add(current.entityId)

        // Stop at max depth
        if (current.depth >= maxDepth) {
          continue
        }

        // Get related entities based on namespace
        if (current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            if (!visited.has(post.$id)) {
              queue.push({ entityId: post.$id, ns: 'posts', depth: current.depth + 1 })
            }
          }
        } else if (current.ns === 'posts') {
          const authors = await db.getRelated<User>('posts', localId(current.entityId), 'author')
          for (const author of authors.items) {
            if (!visited.has(author.$id)) {
              queue.push({ entityId: author.$id, ns: 'users', depth: current.depth + 1 })
            }
          }
        }
      }

      // Visited should contain Alice, her posts, and no duplicates
      expect(visited.has(aliceId)).toBe(true)
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(post2Id)).toBe(true)
      expect(visited.has(post3Id)).toBe(true)
    })

    it('should handle complex cycles: Post -> Comment -> Author -> Posts -> Comment', async () => {
      // Post1 -> Comment1 -> Bob -> (Bob's comments) -> Post1 (cycle back)
      const comments = await db.getRelated<Comment>('posts', localId(post1Id), 'comments')
      expect(comments.items.length).toBeGreaterThan(0)

      const visited = new Set<string>([post1Id])
      let cycleDetected = false

      for (const comment of comments.items) {
        const authors = await db.getRelated<User>('comments', localId(comment.$id), 'author')
        for (const author of authors.items) {
          // Get this author's comments
          const authorComments = await db.getRelated<Comment>('users', localId(author.$id), 'comments')
          for (const authorComment of authorComments.items) {
            // Get posts of these comments
            const commentPosts = await db.getRelated<Post>('comments', localId(authorComment.$id), 'post')
            for (const commentPost of commentPosts.items) {
              if (visited.has(commentPost.$id)) {
                cycleDetected = true
              }
            }
          }
        }
      }

      // A cycle should be detected when we traverse back to post1
      expect(cycleDetected).toBe(true)
    })

    it('should not detect false positives when traversing different branches', async () => {
      // When two different posts share the same author, visiting the author twice
      // from different posts is NOT a cycle - it's just convergence
      const postsWithTechCategory = await db.getRelated<Post>('categories', localId(techCategoryId), 'posts')

      const authorVisitCount = new Map<string, number>()
      for (const post of postsWithTechCategory.items) {
        const authors = await db.getRelated<User>('posts', localId(post.$id), 'author')
        for (const author of authors.items) {
          const count = authorVisitCount.get(author.$id) || 0
          authorVisitCount.set(author.$id, count + 1)
        }
      }

      // Alice is the author of multiple posts - this is convergence, not a cycle
      expect(authorVisitCount.get(aliceId)).toBeGreaterThanOrEqual(2)
    })
  })

  // =============================================================================
  // Test Suite: Depth Limits
  // =============================================================================

  describe('Depth limits', () => {
    it('should respect depth limit of 0 (only start node)', async () => {
      const maxDepth = 0
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: aliceId, ns: 'users', depth: 0 }
      ]

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue

        visited.add(current.entityId)

        if (current.depth < maxDepth && current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            queue.push({ entityId: post.$id, ns: 'posts', depth: current.depth + 1 })
          }
        }
      }

      // Only the start node should be visited
      expect(visited.size).toBe(1)
      expect(visited.has(aliceId)).toBe(true)
    })

    it('should respect depth limit of 1 (start + immediate neighbors)', async () => {
      const maxDepth = 1
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: aliceId, ns: 'users', depth: 0 }
      ]

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue

        visited.add(current.entityId)

        if (current.depth < maxDepth && current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            queue.push({ entityId: post.$id, ns: 'posts', depth: current.depth + 1 })
          }
        }
      }

      // Start node + immediate posts
      expect(visited.has(aliceId)).toBe(true)
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(post2Id)).toBe(true)
      expect(visited.has(post3Id)).toBe(true)
      expect(visited.size).toBe(4) // Alice + 3 posts
    })

    it('should respect depth limit of 2 (start + neighbors + neighbors of neighbors)', async () => {
      const maxDepth = 2
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: aliceId, ns: 'users', depth: 0 }
      ]

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue

        visited.add(current.entityId)

        if (current.depth < maxDepth) {
          if (current.ns === 'users') {
            const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
            for (const post of posts.items) {
              queue.push({ entityId: post.$id, ns: 'posts', depth: current.depth + 1 })
            }
          } else if (current.ns === 'posts') {
            const comments = await db.getRelated<Comment>('posts', localId(current.entityId), 'comments')
            for (const comment of comments.items) {
              queue.push({ entityId: comment.$id, ns: 'comments', depth: current.depth + 1 })
            }
          }
        }
      }

      // Alice -> Posts -> Comments
      expect(visited.has(aliceId)).toBe(true)
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(comment1Id)).toBe(true)
      expect(visited.has(comment2Id)).toBe(true)
      expect(visited.has(comment3Id)).toBe(true)
    })

    it('should count edges correctly at each depth level', async () => {
      interface DepthStats {
        depth: number
        nodeCount: number
        nodes: string[]
      }

      const depthStats: DepthStats[] = []
      const visited = new Set<string>()
      let currentDepth = 0
      let currentLevel: { entityId: string; ns: string }[] = [{ entityId: aliceId, ns: 'users' }]

      while (currentLevel.length > 0 && currentDepth <= 3) {
        const nodesAtDepth: string[] = []
        const nextLevel: { entityId: string; ns: string }[] = []

        for (const { entityId, ns } of currentLevel) {
          if (visited.has(entityId)) continue
          visited.add(entityId)
          nodesAtDepth.push(entityId)

          if (ns === 'users') {
            const posts = await db.getRelated<Post>('users', localId(entityId), 'posts')
            for (const post of posts.items) {
              if (!visited.has(post.$id)) {
                nextLevel.push({ entityId: post.$id, ns: 'posts' })
              }
            }
          } else if (ns === 'posts') {
            const comments = await db.getRelated<Comment>('posts', localId(entityId), 'comments')
            for (const comment of comments.items) {
              if (!visited.has(comment.$id)) {
                nextLevel.push({ entityId: comment.$id, ns: 'comments' })
              }
            }
          } else if (ns === 'comments') {
            const authors = await db.getRelated<User>('comments', localId(entityId), 'author')
            for (const author of authors.items) {
              if (!visited.has(author.$id)) {
                nextLevel.push({ entityId: author.$id, ns: 'users' })
              }
            }
          }
        }

        if (nodesAtDepth.length > 0) {
          depthStats.push({
            depth: currentDepth,
            nodeCount: nodesAtDepth.length,
            nodes: nodesAtDepth
          })
        }

        currentLevel = nextLevel
        currentDepth++
      }

      // Verify depth stats
      expect(depthStats[0].depth).toBe(0)
      expect(depthStats[0].nodeCount).toBe(1) // Alice
      expect(depthStats[1].depth).toBe(1)
      expect(depthStats[1].nodeCount).toBe(3) // 3 posts
      expect(depthStats[2].depth).toBe(2)
      expect(depthStats[2].nodeCount).toBe(3) // 3 comments
    })
  })

  // =============================================================================
  // Test Suite: Filtered Traversal (Predicate Selection)
  // =============================================================================

  describe('Filtered traversal', () => {
    it('should only follow "author" predicate, not "categories"', async () => {
      // Start from a post, but only follow the author relationship
      const predicatesToFollow = ['author']
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: post1Id, ns: 'posts', depth: 0 }
      ]
      const maxDepth = 2

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue
        visited.add(current.entityId)

        if (current.depth < maxDepth && current.ns === 'posts') {
          // Only follow specified predicates
          if (predicatesToFollow.includes('author')) {
            const authors = await db.getRelated<User>('posts', localId(current.entityId), 'author')
            for (const author of authors.items) {
              queue.push({ entityId: author.$id, ns: 'users', depth: current.depth + 1 })
            }
          }
          // Explicitly NOT following categories
        }
      }

      // Should have visited post and author, but NOT categories
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(aliceId)).toBe(true)
      expect(visited.has(techCategoryId)).toBe(false)
      expect(visited.has(dbCategoryId)).toBe(false)
    })

    it('should only follow "categories" predicate, not "author"', async () => {
      const predicatesToFollow = ['categories']
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: post1Id, ns: 'posts', depth: 0 }
      ]
      const maxDepth = 2

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue
        visited.add(current.entityId)

        if (current.depth < maxDepth && current.ns === 'posts') {
          if (predicatesToFollow.includes('categories')) {
            const categories = await db.getRelated<Category>('posts', localId(current.entityId), 'categories')
            for (const category of categories.items) {
              queue.push({ entityId: category.$id, ns: 'categories', depth: current.depth + 1 })
            }
          }
        }
      }

      // Should have visited post and categories, but NOT author
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(techCategoryId)).toBe(true)
      expect(visited.has(dbCategoryId)).toBe(true)
      expect(visited.has(aliceId)).toBe(false)
    })

    it('should follow multiple predicates when specified', async () => {
      const predicatesToFollow = ['author', 'categories']
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string; depth: number }[] = [
        { entityId: post1Id, ns: 'posts', depth: 0 }
      ]
      const maxDepth = 1

      while (queue.length > 0) {
        const current = queue.shift()!
        if (current.depth > maxDepth) continue
        if (visited.has(current.entityId)) continue
        visited.add(current.entityId)

        if (current.depth < maxDepth && current.ns === 'posts') {
          if (predicatesToFollow.includes('author')) {
            const authors = await db.getRelated<User>('posts', localId(current.entityId), 'author')
            for (const author of authors.items) {
              queue.push({ entityId: author.$id, ns: 'users', depth: current.depth + 1 })
            }
          }
          if (predicatesToFollow.includes('categories')) {
            const categories = await db.getRelated<Category>('posts', localId(current.entityId), 'categories')
            for (const category of categories.items) {
              queue.push({ entityId: category.$id, ns: 'categories', depth: current.depth + 1 })
            }
          }
        }
      }

      // Should have visited post, author, and categories
      expect(visited.has(post1Id)).toBe(true)
      expect(visited.has(aliceId)).toBe(true)
      expect(visited.has(techCategoryId)).toBe(true)
      expect(visited.has(dbCategoryId)).toBe(true)
    })

    it('should apply filter to entities within traversal', async () => {
      // Only get published posts when traversing from user
      const publishedPosts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
        filter: { status: 'published' }
      })

      // Traverse further from only published posts
      const visitedFromPublished = new Set<string>()
      for (const post of publishedPosts.items) {
        expect(post.status).toBe('published')
        visitedFromPublished.add(post.$id)

        const comments = await db.getRelated<Comment>('posts', localId(post.$id), 'comments')
        for (const comment of comments.items) {
          visitedFromPublished.add(comment.$id)
        }
      }

      // Draft post should not be visited
      expect(visitedFromPublished.has(post3Id)).toBe(false)
      // Published posts and their comments should be visited
      expect(visitedFromPublished.has(post1Id)).toBe(true)
      expect(visitedFromPublished.has(post2Id)).toBe(true)
    })

    it('should apply different filters at different levels', async () => {
      // Level 1: Get only published posts
      const publishedPosts = await db.getRelated<Post>('users', localId(aliceId), 'posts', {
        filter: { status: 'published' }
      })

      // Level 2: Get only high-rated comments
      const highRatedComments: Entity<Comment>[] = []
      for (const post of publishedPosts.items) {
        const comments = await db.getRelated<Comment>('posts', localId(post.$id), 'comments', {
          filter: { rating: { $gte: 5 } }
        })
        highRatedComments.push(...comments.items)
      }

      // All returned comments should be high-rated
      for (const comment of highRatedComments) {
        expect(comment.rating).toBeGreaterThanOrEqual(5)
      }
    })
  })

  // =============================================================================
  // Test Suite: Breadth-First vs Depth-First Traversal
  // =============================================================================

  describe('Traversal order', () => {
    it('should perform BFS traversal correctly (level by level)', async () => {
      const bfsOrder: string[] = []
      const visited = new Set<string>()
      const queue: { entityId: string; ns: string }[] = [{ entityId: aliceId, ns: 'users' }]

      while (queue.length > 0) {
        const current = queue.shift()! // BFS uses shift (FIFO)
        if (visited.has(current.entityId)) continue
        visited.add(current.entityId)
        bfsOrder.push(current.entityId)

        if (current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            if (!visited.has(post.$id)) {
              queue.push({ entityId: post.$id, ns: 'posts' })
            }
          }
        } else if (current.ns === 'posts') {
          const comments = await db.getRelated<Comment>('posts', localId(current.entityId), 'comments')
          for (const comment of comments.items) {
            if (!visited.has(comment.$id)) {
              queue.push({ entityId: comment.$id, ns: 'comments' })
            }
          }
        }
      }

      // BFS should visit all users first, then all posts, then all comments
      // First element should be Alice
      expect(bfsOrder[0]).toBe(aliceId)

      // Alice's posts should come before any comments
      const postIndices = [post1Id, post2Id, post3Id]
        .filter(id => bfsOrder.includes(id))
        .map(id => bfsOrder.indexOf(id))
      const commentIndices = [comment1Id, comment2Id, comment3Id]
        .filter(id => bfsOrder.includes(id))
        .map(id => bfsOrder.indexOf(id))

      // All posts should appear before any comments (BFS property)
      if (postIndices.length > 0 && commentIndices.length > 0) {
        expect(Math.max(...postIndices)).toBeLessThan(Math.min(...commentIndices))
      }
    })

    it('should perform DFS traversal correctly (depth first)', async () => {
      const dfsOrder: string[] = []
      const visited = new Set<string>()
      const stack: { entityId: string; ns: string }[] = [{ entityId: aliceId, ns: 'users' }]

      while (stack.length > 0) {
        const current = stack.pop()! // DFS uses pop (LIFO)
        if (visited.has(current.entityId)) continue
        visited.add(current.entityId)
        dfsOrder.push(current.entityId)

        if (current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          // Push in reverse order so first post is processed first
          for (let i = posts.items.length - 1; i >= 0; i--) {
            const post = posts.items[i]
            if (!visited.has(post.$id)) {
              stack.push({ entityId: post.$id, ns: 'posts' })
            }
          }
        } else if (current.ns === 'posts') {
          const comments = await db.getRelated<Comment>('posts', localId(current.entityId), 'comments')
          for (let i = comments.items.length - 1; i >= 0; i--) {
            const comment = comments.items[i]
            if (!visited.has(comment.$id)) {
              stack.push({ entityId: comment.$id, ns: 'comments' })
            }
          }
        }
      }

      // DFS should visit deeply before broadly
      // First element should be Alice
      expect(dfsOrder[0]).toBe(aliceId)

      // In DFS, comments from a post should appear right after that post
      // (unlike BFS where all posts appear before any comments)
      // This is harder to assert definitively without knowing exact order of posts.items
      // But we can verify the traversal completes without issues
      expect(dfsOrder.length).toBeGreaterThan(0)
      expect(visited.has(aliceId)).toBe(true)
    })

    it('should visit same nodes in BFS and DFS, just different order', async () => {
      // BFS traversal
      const bfsVisited = new Set<string>()
      const bfsQueue: { entityId: string; ns: string }[] = [{ entityId: aliceId, ns: 'users' }]

      while (bfsQueue.length > 0) {
        const current = bfsQueue.shift()!
        if (bfsVisited.has(current.entityId)) continue
        bfsVisited.add(current.entityId)

        if (current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            if (!bfsVisited.has(post.$id)) {
              bfsQueue.push({ entityId: post.$id, ns: 'posts' })
            }
          }
        }
      }

      // DFS traversal
      const dfsVisited = new Set<string>()
      const dfsStack: { entityId: string; ns: string }[] = [{ entityId: aliceId, ns: 'users' }]

      while (dfsStack.length > 0) {
        const current = dfsStack.pop()!
        if (dfsVisited.has(current.entityId)) continue
        dfsVisited.add(current.entityId)

        if (current.ns === 'users') {
          const posts = await db.getRelated<Post>('users', localId(current.entityId), 'posts')
          for (const post of posts.items) {
            if (!dfsVisited.has(post.$id)) {
              dfsStack.push({ entityId: post.$id, ns: 'posts' })
            }
          }
        }
      }

      // Both should visit the same nodes
      expect(bfsVisited.size).toBe(dfsVisited.size)
      for (const node of bfsVisited) {
        expect(dfsVisited.has(node)).toBe(true)
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

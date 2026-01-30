/**
 * Relationship Benchmarks for ParqueDB
 *
 * Measures performance of relationship operations:
 * - Link/unlink operations
 * - Hydration with 1-level populate
 * - Multi-level populate
 * - Inbound reference traversal
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Schema, UpdateInput } from '../../src/types'
import {
  generateTestData,
  generateRelationalTestData,
  benchmarkSchema,
  createBenchmarkStorage,
  randomElement,
  randomInt,
  randomSubset,
} from './setup'

// =============================================================================
// Test Types
// =============================================================================

interface User {
  name: string
  email: string
  bio?: string
}

interface Post {
  title: string
  content: string
  status: string
  views: number
}

interface Comment {
  text: string
  approved: boolean
}

interface Category {
  name: string
  slug: string
}

// =============================================================================
// Relationship Benchmarks
// =============================================================================

describe('Relationship Benchmarks', () => {
  // ===========================================================================
  // Link/Unlink Operations
  // ===========================================================================

  describe('Link Operations', () => {
    let db: ParqueDB
    let users: Collection<User>
    let posts: Collection<Post>
    let categories: Collection<Category>
    let userIds: string[] = []
    let postIds: string[] = []
    let categoryIds: string[] = []

    beforeAll(async () => {
      const storage = createBenchmarkStorage()
      db = new ParqueDB({ storage, schema: benchmarkSchema })

      // Create unique namespace suffix
      const suffix = Date.now()

      // Create users
      const usersNs = `users-${suffix}`
      users = new Collection<User>(usersNs)
      for (let i = 0; i < 100; i++) {
        const user = await users.create({
          $type: 'User',
          name: `User ${i}`,
          email: `user${i}@example.com`,
        })
        userIds.push(user.$id as string)
      }

      // Create posts
      const postsNs = `posts-${suffix}`
      posts = new Collection<Post>(postsNs)
      for (let i = 0; i < 500; i++) {
        const post = await posts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Post Title ${i}`,
          content: `Content for post ${i}`,
          status: 'draft',
          views: 0,
        })
        postIds.push(post.$id as string)
      }

      // Create categories
      const categoriesNs = `categories-${suffix}`
      categories = new Collection<Category>(categoriesNs)
      for (let i = 0; i < 20; i++) {
        const category = await categories.create({
          $type: 'Category',
          name: `Category ${i}`,
          slug: `category-${i}`,
        })
        categoryIds.push(category.$id as string)
      }
    })

    bench('link single entity (post -> author)', async () => {
      const postId = randomElement(postIds)
      const userId = randomElement(userIds)
      await posts.update(postId, {
        $link: { author: userId as EntityId },
      })
    })

    bench('link multiple entities (post -> categories)', async () => {
      const postId = randomElement(postIds)
      const cats = randomSubset(categoryIds, 3)
      await posts.update(postId, {
        $link: { categories: cats as EntityId[] },
      })
    })

    bench('link to array (add to existing)', async () => {
      const postId = randomElement(postIds)
      const newCat = randomElement(categoryIds)
      await posts.update(postId, {
        $addToSet: { categories: newCat },
      })
    })

    bench('unlink single entity', async () => {
      const postId = randomElement(postIds)
      const userId = randomElement(userIds)
      await posts.update(postId, {
        $unlink: { author: userId as EntityId },
      })
    })

    bench('unlink from array (single)', async () => {
      const postId = randomElement(postIds)
      const catToRemove = randomElement(categoryIds)
      await posts.update(postId, {
        $pull: { categories: catToRemove },
      })
    })

    bench('unlink all from array', async () => {
      const postId = randomElement(postIds)
      await posts.update(postId, {
        $set: { status: 'draft' },
      })
    })

    bench('replace link (change author)', async () => {
      const postId = randomElement(postIds)
      const newAuthor = randomElement(userIds)
      await posts.update(postId, {
        $link: { author: newAuthor as EntityId },
      })
    })

    bench('batch link - 10 posts to same author', async () => {
      const targetPosts = randomSubset(postIds, 10)
      const author = randomElement(userIds)
      for (const postId of targetPosts) {
        await posts.update(postId, {
          $link: { author: author as EntityId },
        })
      }
    })

    bench('batch link - 100 posts to same author', async () => {
      const targetPosts = randomSubset(postIds, 100)
      const author = randomElement(userIds)
      for (const postId of targetPosts) {
        await posts.update(postId, {
          $link: { author: author as EntityId },
        })
      }
    }, { iterations: 10 })
  })

  // ===========================================================================
  // Populate (Hydration) Operations
  // ===========================================================================

  describe('Populate Operations', () => {
    let relDb: ParqueDB
    let relUsers: Collection<User>
    let relPosts: Collection<Post>
    let relComments: Collection<Comment>
    let relCategories: Collection<Category>

    let seededUserIds: string[] = []
    let seededPostIds: string[] = []
    let seededCommentIds: string[] = []
    let seededCategoryIds: string[] = []

    beforeAll(async () => {
      const storage = createBenchmarkStorage()
      relDb = new ParqueDB({ storage, schema: benchmarkSchema })

      const suffix = Date.now()

      // Create and seed data with relationships
      const usersNs = `rel-users-${suffix}`
      const postsNs = `rel-posts-${suffix}`
      const commentsNs = `rel-comments-${suffix}`
      const categoriesNs = `rel-categories-${suffix}`

      relUsers = new Collection<User>(usersNs)
      relPosts = new Collection<Post>(postsNs)
      relComments = new Collection<Comment>(commentsNs)
      relCategories = new Collection<Category>(categoriesNs)

      // Create 50 users
      for (let i = 0; i < 50; i++) {
        const user = await relUsers.create({
          $type: 'User',
          name: `User ${i}`,
          email: `user${i}@example.com`,
          bio: `Bio for user ${i}`,
        })
        seededUserIds.push(user.$id as string)
      }

      // Create 10 categories
      for (let i = 0; i < 10; i++) {
        const category = await relCategories.create({
          $type: 'Category',
          name: `Category ${i}`,
          slug: `category-${i}`,
        })
        seededCategoryIds.push(category.$id as string)
      }

      // Create 200 posts with author and categories
      for (let i = 0; i < 200; i++) {
        const authorId = seededUserIds[i % 50]
        const postCategories = randomSubset(seededCategoryIds, randomInt(1, 3))

        const post = await relPosts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Post Title ${i}`,
          content: `Content for post ${i}`,
          status: i % 3 === 0 ? 'published' : 'draft',
          views: randomInt(0, 10000),
          // Store relationships
          author: { 'Author': authorId as EntityId },
          categories: postCategories.reduce((acc, catId, idx) => {
            acc[`Cat${idx}`] = catId as EntityId
            return acc
          }, {} as Record<string, EntityId>),
        })
        seededPostIds.push(post.$id as string)

        // Create 3-5 comments per post
        const numComments = randomInt(3, 5)
        for (let c = 0; c < numComments; c++) {
          const commentAuthor = randomElement(seededUserIds)
          const comment = await relComments.create({
            $type: 'Comment',
            name: `Comment ${i}-${c}`,
            text: `Comment text ${c} on post ${i}`,
            approved: Math.random() > 0.2,
            post: { 'Post': post.$id as EntityId },
            author: { 'Author': commentAuthor as EntityId },
          })
          seededCommentIds.push(comment.$id as string)
        }
      }
    })

    bench('find without populate', async () => {
      await relPosts.find({ status: 'published' }, { limit: 20 })
    })

    bench('find with single populate (author)', async () => {
      await relPosts.find(
        { status: 'published' },
        { limit: 20, populate: ['author'] }
      )
    })

    bench('find with array populate (categories)', async () => {
      await relPosts.find(
        { status: 'published' },
        { limit: 20, populate: ['categories'] }
      )
    })

    bench('find with multiple populates (author + categories)', async () => {
      await relPosts.find(
        { status: 'published' },
        { limit: 20, populate: ['author', 'categories'] }
      )
    })

    bench('get single with hydrate', async () => {
      const postId = randomElement(seededPostIds)
      await relPosts.get(postId, { hydrate: ['author', 'categories'] })
    })

    bench('find with nested populate (post.author)', async () => {
      await relComments.find(
        { approved: true },
        {
          limit: 20,
          populate: {
            post: {
              populate: ['author'],
            },
          },
        }
      )
    })

    bench('find with deep nested populate (comment.post.author + comment.author)', async () => {
      await relComments.find(
        { approved: true },
        {
          limit: 20,
          populate: {
            post: {
              populate: ['author', 'categories'],
            },
            author: true,
          },
        }
      )
    })

    bench('find with selective populate (with limit)', async () => {
      await relPosts.find(
        { status: 'published' },
        {
          limit: 20,
          populate: {
            author: {
              limit: 1,
            },
          },
        }
      )
    })

    bench('populate many results (100 posts)', async () => {
      await relPosts.find(
        {},
        { limit: 100, populate: ['author'] }
      )
    })

    bench('populate with filter on populated field', async () => {
      await relPosts.find(
        { status: 'published' },
        {
          limit: 20,
          populate: {
            author: {
              filter: { active: true },
            },
          },
        }
      )
    })
  })

  // ===========================================================================
  // Inbound Reference Traversal
  // ===========================================================================

  describe('Inbound Reference Traversal', () => {
    let travDb: ParqueDB
    let travUsers: Collection<User>
    let travPosts: Collection<Post>
    let travComments: Collection<Comment>

    let travUserIds: string[] = []
    let travPostIds: string[] = []

    beforeAll(async () => {
      const storage = createBenchmarkStorage()
      travDb = new ParqueDB({ storage, schema: benchmarkSchema })

      const suffix = Date.now()

      const usersNs = `trav-users-${suffix}`
      const postsNs = `trav-posts-${suffix}`
      const commentsNs = `trav-comments-${suffix}`

      travUsers = new Collection<User>(usersNs)
      travPosts = new Collection<Post>(postsNs)
      travComments = new Collection<Comment>(commentsNs)

      // Create 20 users, each with varying numbers of posts
      for (let u = 0; u < 20; u++) {
        const user = await travUsers.create({
          $type: 'User',
          name: `User ${u}`,
          email: `user${u}@example.com`,
        })
        travUserIds.push(user.$id as string)

        // Each user has 5-20 posts
        const numPosts = randomInt(5, 20)
        for (let p = 0; p < numPosts; p++) {
          const post = await travPosts.create({
            $type: 'Post',
            name: `Post ${u}-${p}`,
            title: `User ${u} Post ${p}`,
            content: `Content`,
            status: 'published',
            views: 0,
            author: { 'Author': user.$id as EntityId },
          })
          travPostIds.push(post.$id as string)

          // Each post has 2-10 comments
          const numComments = randomInt(2, 10)
          for (let c = 0; c < numComments; c++) {
            await travComments.create({
              $type: 'Comment',
              name: `Comment ${u}-${p}-${c}`,
              text: `Comment ${c}`,
              approved: true,
              post: { 'Post': post.$id as EntityId },
              author: { 'Author': randomElement(travUserIds) as EntityId },
            })
          }
        }
      }
    })

    bench('get user posts (reverse lookup)', async () => {
      const userId = randomElement(travUserIds)
      await travPosts.find({ author: { $eq: userId } })
    })

    bench('get user posts via inbound hydrate', async () => {
      const userId = randomElement(travUserIds)
      await travUsers.get(userId, { hydrate: ['posts'] })
    })

    bench('get post comments (reverse lookup)', async () => {
      const postId = randomElement(travPostIds)
      await travComments.find({ post: { $eq: postId } })
    })

    bench('count user posts', async () => {
      const userId = randomElement(travUserIds)
      await travPosts.count({ author: { $eq: userId } })
    })

    bench('get all comments by user (through multiple posts)', async () => {
      const userId = randomElement(travUserIds)
      await travComments.find({ author: { $eq: userId } })
    })

    bench('traverse: user -> posts -> comments', async () => {
      const userId = randomElement(travUserIds)
      // Get user's posts
      const userPosts = await travPosts.find({ author: { $eq: userId } })
      // Get comments for each post
      for (const post of userPosts.slice(0, 5)) {
        await travComments.find({ post: { $eq: post.$id } })
      }
    })

    bench('traverse with populate: user -> posts with comments', async () => {
      const userId = randomElement(travUserIds)
      await travPosts.find(
        { author: { $eq: userId } },
        { populate: ['comments'] }
      )
    })

    bench('bidirectional: find co-authors', async () => {
      // Find all users who commented on the same posts as a given user
      const userId = randomElement(travUserIds)
      // Get posts by user
      const userPosts = await travPosts.find({ author: { $eq: userId } })
      // Get comments on those posts
      const commentAuthors = new Set<string>()
      for (const post of userPosts.slice(0, 5)) {
        const comments = await travComments.find({ post: { $eq: post.$id } })
        for (const comment of comments) {
          const authorRef = comment.author as Record<string, EntityId>
          if (authorRef) {
            for (const authorId of Object.values(authorRef)) {
              commentAuthors.add(authorId)
            }
          }
        }
      }
    })

    bench('pagination: user posts with cursor', async () => {
      const userId = randomElement(travUserIds)
      // First page
      const page1 = await travPosts.find(
        { author: { $eq: userId } },
        { limit: 5, sort: { createdAt: -1 } }
      )
      // Second page using cursor
      if (page1.length > 0) {
        const lastPost = page1[page1.length - 1]
        await travPosts.find(
          {
            author: { $eq: userId },
            createdAt: { $lt: lastPost.createdAt },
          },
          { limit: 5, sort: { createdAt: -1 } }
        )
      }
    })
  })

  // ===========================================================================
  // Relationship Integrity
  // ===========================================================================

  describe('Relationship Integrity Operations', () => {
    let intDb: ParqueDB
    let intUsers: Collection<User>
    let intPosts: Collection<Post>
    let intCategories: Collection<Category>

    beforeEach(async () => {
      const storage = createBenchmarkStorage()
      intDb = new ParqueDB({ storage, schema: benchmarkSchema })

      const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`
      intUsers = new Collection<User>(`int-users-${suffix}`)
      intPosts = new Collection<Post>(`int-posts-${suffix}`)
      intCategories = new Collection<Category>(`int-categories-${suffix}`)
    })

    bench('create entity with relationship', async () => {
      const user = await intUsers.create({
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      await intPosts.create({
        $type: 'Post',
        name: 'Post with Author',
        title: 'Title',
        content: 'Content',
        status: 'draft',
        views: 0,
        author: { 'Author': user.$id as EntityId },
      })
    })

    bench('create entity with multiple relationships', async () => {
      const user = await intUsers.create({
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const cat1 = await intCategories.create({
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const cat2 = await intCategories.create({
        $type: 'Category',
        name: 'Science',
        slug: 'science',
      })

      await intPosts.create({
        $type: 'Post',
        name: 'Post with Multiple Relationships',
        title: 'Title',
        content: 'Content',
        status: 'draft',
        views: 0,
        author: { 'Author': user.$id as EntityId },
        categories: {
          'Tech': cat1.$id as EntityId,
          'Science': cat2.$id as EntityId,
        },
      })
    })

    bench('validate relationship on update', async () => {
      const user = await intUsers.create({
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await intPosts.create({
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
        status: 'draft',
        views: 0,
      })

      // Update with relationship
      await intPosts.update(post.$id as string, {
        $link: { author: user.$id as EntityId },
      })
    })

    bench('cascade check simulation', async () => {
      const user = await intUsers.create({
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      // Create 10 posts by this user
      for (let i = 0; i < 10; i++) {
        await intPosts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
          status: 'draft',
          views: 0,
          author: { 'Author': user.$id as EntityId },
        })
      }

      // Check for dependent posts before "deleting" user
      const dependentPosts = await intPosts.find({ author: { $eq: user.$id } })
      // In a real cascade, we'd delete or update these
    })
  })
})

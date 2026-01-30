/**
 * CRUD Operation Benchmarks for ParqueDB
 *
 * Measures performance of core CRUD operations:
 * - Single entity create
 * - Batch create (100, 1000, 10000 entities)
 * - Find with simple filter
 * - Find with complex filter ($and, $or, nested)
 * - Update single entity
 * - Update with $inc, $push operators
 * - Delete single vs batch
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Filter, UpdateInput } from '../../src/types'
import {
  generateTestData,
  generateEntity,
  benchmarkSchema,
  createBenchmarkStorage,
  randomInt,
  randomElement,
  resetIdCounter,
} from './setup'

// =============================================================================
// Test Setup
// =============================================================================

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  views: number
  likes: number
  tags: string[]
}

describe('CRUD Benchmarks', () => {
  let db: ParqueDB
  let posts: Collection<Post>
  let seededPosts: Entity<Post>[] = []
  let namespace: string

  // ===========================================================================
  // Create Operations
  // ===========================================================================

  describe('Create Operations', () => {
    beforeEach(() => {
      // Fresh namespace for each benchmark
      namespace = `posts-${Date.now()}-${Math.random().toString(36).slice(2)}`
      posts = new Collection<Post>(namespace)
    })

    bench('single entity create', async () => {
      await posts.create({
        $type: 'Post',
        name: `Post ${Date.now()}`,
        title: 'Benchmark Post',
        content: 'This is benchmark content for testing create performance.',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: ['benchmark', 'test'],
      })
    })

    bench('batch create 100 entities', async () => {
      const data = generateTestData(100, 'Post')
      for (const entity of data) {
        await posts.create(entity as unknown as Partial<Post> & { $type: string; name: string })
      }
    })

    bench('batch create 1000 entities', async () => {
      const data = generateTestData(1000, 'Post')
      for (const entity of data) {
        await posts.create(entity as unknown as Partial<Post> & { $type: string; name: string })
      }
    }, { iterations: 10 })
  })

  // ===========================================================================
  // Find Operations
  // ===========================================================================

  describe('Find Operations', () => {
    let searchPosts: Collection<Post>
    let searchNamespace: string
    let createdIds: string[] = []

    beforeAll(async () => {
      // Create a collection with seeded data for search benchmarks
      searchNamespace = `search-posts-${Date.now()}`
      searchPosts = new Collection<Post>(searchNamespace)

      // Seed 1000 posts with varied data
      const testData = generateTestData(1000, 'Post')
      for (const data of testData) {
        const entity = await searchPosts.create(
          data as unknown as Partial<Post> & { $type: string; name: string }
        )
        createdIds.push(entity.$id as string)
      }
    })

    bench('find all (no filter)', async () => {
      await searchPosts.find()
    })

    bench('find with simple equality filter', async () => {
      await searchPosts.find({ status: 'published' })
    })

    bench('find with comparison filter ($gt)', async () => {
      await searchPosts.find({ views: { $gt: 50000 } })
    })

    bench('find with comparison filter ($gte, $lt range)', async () => {
      await searchPosts.find({ views: { $gte: 10000, $lt: 50000 } })
    })

    bench('find with $in operator', async () => {
      await searchPosts.find({ status: { $in: ['published', 'archived'] } })
    })

    bench('find with $and filter', async () => {
      await searchPosts.find({
        $and: [
          { status: 'published' },
          { views: { $gt: 10000 } },
        ],
      })
    })

    bench('find with $or filter', async () => {
      await searchPosts.find({
        $or: [
          { status: 'draft' },
          { views: { $gt: 50000 } },
        ],
      })
    })

    bench('find with complex nested filter', async () => {
      await searchPosts.find({
        $and: [
          {
            $or: [
              { status: 'published' },
              { status: 'archived' },
            ],
          },
          { views: { $gte: 1000 } },
          {
            $or: [
              { likes: { $gt: 100 } },
              { tags: { $size: { $gt: 2 } } },
            ],
          },
        ],
      })
    })

    bench('find with $regex filter', async () => {
      await searchPosts.find({ title: { $regex: '^Test' } })
    })

    bench('find with $exists filter', async () => {
      await searchPosts.find({ publishedAt: { $exists: true } })
    })

    bench('find with limit (10)', async () => {
      await searchPosts.find({}, { limit: 10 })
    })

    bench('find with limit (100)', async () => {
      await searchPosts.find({}, { limit: 100 })
    })

    bench('find with sort (single field)', async () => {
      await searchPosts.find({}, { sort: { views: -1 } })
    })

    bench('find with sort (multiple fields)', async () => {
      await searchPosts.find({}, { sort: { status: 1, views: -1 } })
    })

    bench('find with filter + sort + limit', async () => {
      await searchPosts.find(
        { status: 'published' },
        { sort: { views: -1 }, limit: 10 }
      )
    })

    bench('find with projection', async () => {
      await searchPosts.find({}, { project: { title: 1, status: 1, views: 1 } })
    })
  })

  // ===========================================================================
  // Get Operations
  // ===========================================================================

  describe('Get Operations', () => {
    let getPosts: Collection<Post>
    let getNamespace: string
    let entityIds: string[] = []

    beforeAll(async () => {
      getNamespace = `get-posts-${Date.now()}`
      getPosts = new Collection<Post>(getNamespace)

      // Create 100 posts
      const testData = generateTestData(100, 'Post')
      for (const data of testData) {
        const entity = await getPosts.create(
          data as unknown as Partial<Post> & { $type: string; name: string }
        )
        entityIds.push(entity.$id as string)
      }
    })

    bench('get by ID', async () => {
      const id = randomElement(entityIds)
      await getPosts.get(id)
    })

    bench('get with projection', async () => {
      const id = randomElement(entityIds)
      await getPosts.get(id, { project: { title: 1, status: 1 } })
    })

    bench('get non-existent ID', async () => {
      await getPosts.get('non-existent-id-12345')
    })
  })

  // ===========================================================================
  // Update Operations
  // ===========================================================================

  describe('Update Operations', () => {
    let updatePosts: Collection<Post>
    let updateNamespace: string
    let updateIds: string[] = []

    beforeAll(async () => {
      updateNamespace = `update-posts-${Date.now()}`
      updatePosts = new Collection<Post>(updateNamespace)

      // Create 500 posts for update benchmarks
      const testData = generateTestData(500, 'Post')
      for (const data of testData) {
        const entity = await updatePosts.create(
          data as unknown as Partial<Post> & { $type: string; name: string }
        )
        updateIds.push(entity.$id as string)
      }
    })

    bench('update single field ($set)', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $set: { title: `Updated Title ${Date.now()}` },
      })
    })

    bench('update multiple fields ($set)', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $set: {
          title: `Updated Title ${Date.now()}`,
          content: 'Updated content',
          status: 'published',
        },
      })
    })

    bench('update with $inc operator', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $inc: { views: 1 },
      })
    })

    bench('update with multiple $inc', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $inc: { views: 1, likes: 1 },
      })
    })

    bench('update with $push operator', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $push: { tags: 'new-tag' },
      })
    })

    bench('update with $addToSet operator', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $addToSet: { tags: 'unique-tag' },
      })
    })

    bench('update with $pull operator', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $pull: { tags: 'benchmark' },
      })
    })

    bench('update with $unset operator', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $unset: { tags: '' },
      })
    })

    bench('update with $min/$max operators', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $max: { views: 100000 },
        $min: { likes: 0 },
      })
    })

    bench('update with combined operators', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $set: { status: 'published' },
        $inc: { views: 10 },
        $push: { tags: 'featured' },
      })
    })

    bench('update with nested field (simulated)', async () => {
      const id = randomElement(updateIds)
      await updatePosts.update(id, {
        $set: { tags: ['updated', 'nested'] },
      })
    })
  })

  // ===========================================================================
  // Delete Operations
  // ===========================================================================

  describe('Delete Operations', () => {
    let deleteCollection: Collection<Post>
    let deleteNamespace: string

    beforeEach(async () => {
      // Fresh collection for each delete benchmark
      deleteNamespace = `delete-posts-${Date.now()}-${Math.random().toString(36).slice(2)}`
      deleteCollection = new Collection<Post>(deleteNamespace)
    })

    bench('delete single entity', async () => {
      // Create then delete
      const entity = await deleteCollection.create({
        $type: 'Post',
        name: 'To Delete',
        title: 'Delete Me',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      await deleteCollection.delete(entity.$id as string)
    })

    bench('delete with options', async () => {
      const entity = await deleteCollection.create({
        $type: 'Post',
        name: 'To Delete With Options',
        title: 'Delete Me With Options',
        content: 'Content',
        status: 'draft',
        views: 0,
        likes: 0,
        tags: [],
      })
      await deleteCollection.delete(entity.$id as string)
    })

    bench('batch delete 10 entities', async () => {
      // Create 10 entities
      const ids: string[] = []
      for (let i = 0; i < 10; i++) {
        const entity = await deleteCollection.create({
          $type: 'Post',
          name: `Delete ${i}`,
          title: `Delete Me ${i}`,
          content: 'Content',
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
        ids.push(entity.$id as string)
      }

      // Delete all
      for (const id of ids) {
        await deleteCollection.delete(id)
      }
    })

    bench('batch delete 100 entities', async () => {
      // Create 100 entities
      const ids: string[] = []
      for (let i = 0; i < 100; i++) {
        const entity = await deleteCollection.create({
          $type: 'Post',
          name: `Delete ${i}`,
          title: `Delete Me ${i}`,
          content: 'Content',
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
        ids.push(entity.$id as string)
      }

      // Delete all
      for (const id of ids) {
        await deleteCollection.delete(id)
      }
    }, { iterations: 10 })

    bench('deleteMany with filter', async () => {
      // Create mixed status posts
      for (let i = 0; i < 50; i++) {
        await deleteCollection.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
          status: i % 2 === 0 ? 'draft' : 'published',
          views: 0,
          likes: 0,
          tags: [],
        })
      }

      // Delete only drafts
      await deleteCollection.deleteMany({ status: 'draft' })
    }, { iterations: 10 })
  })

  // ===========================================================================
  // Find and Update Operations (Alternative to Upsert)
  // ===========================================================================

  describe('Find and Update Operations', () => {
    let findUpdatePosts: Collection<Post>
    let findUpdateNamespace: string
    let findUpdateIds: string[] = []

    beforeAll(async () => {
      findUpdateNamespace = `find-update-posts-${Date.now()}`
      findUpdatePosts = new Collection<Post>(findUpdateNamespace)

      // Create test posts
      for (let i = 0; i < 100; i++) {
        const entity = await findUpdatePosts.create({
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
          status: i % 2 === 0 ? 'draft' : 'published',
          views: i * 10,
          likes: i,
          tags: ['test'],
        })
        findUpdateIds.push(entity.$id as string)
      }
    })

    bench('find then update (insert-like)', async () => {
      // Find if exists, if not create
      const existing = await findUpdatePosts.find({ title: `Unique Title ${Date.now()}` }, { limit: 1 })
      if (existing.length === 0) {
        await findUpdatePosts.create({
          $type: 'Post',
          name: 'New Post',
          title: `Unique Title ${Date.now()}`,
          content: 'New content',
          status: 'draft',
          views: 0,
          likes: 0,
          tags: [],
        })
      }
    })

    bench('find then update (update case)', async () => {
      // Find existing and update
      const existing = await findUpdatePosts.find({ status: 'draft' }, { limit: 1 })
      if (existing.length > 0) {
        await findUpdatePosts.update(existing[0].$id as string, {
          $set: { content: 'Updated content' },
          $inc: { views: 1 },
        })
      }
    })
  })

  // ===========================================================================
  // Count Operations
  // ===========================================================================

  describe('Count Operations', () => {
    let countPosts: Collection<Post>
    let countNamespace: string

    beforeAll(async () => {
      countNamespace = `count-posts-${Date.now()}`
      countPosts = new Collection<Post>(countNamespace)

      // Seed 1000 posts
      const testData = generateTestData(1000, 'Post')
      for (const data of testData) {
        await countPosts.create(
          data as unknown as Partial<Post> & { $type: string; name: string }
        )
      }
    })

    bench('count all', async () => {
      await countPosts.count()
    })

    bench('count with simple filter', async () => {
      await countPosts.count({ status: 'published' })
    })

    bench('count with complex filter', async () => {
      await countPosts.count({
        $and: [
          { status: 'published' },
          { views: { $gt: 10000 } },
        ],
      })
    })
  })
})

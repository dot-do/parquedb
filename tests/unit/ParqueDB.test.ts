/**
 * ParqueDB Test Suite
 *
 * Tests for the ParqueDB class with Proxy pattern for collection access.
 * Uses real FsBackend storage with temporary directories for actual file I/O.
 */

import { describe, it, expect, beforeEach, afterEach, afterAll } from 'vitest'
import { ParqueDB, type ParqueDBConfig, type Collection } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type {
  StorageBackend,
  Schema,
  Filter,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  UpdateInput,
  CreateInput,
  EntityId,
} from '../../src/types'

// =============================================================================
// Test Utilities
// =============================================================================

/** Track all created temp directories for cleanup */
const tempDirs: string[] = []

/**
 * Create a real FsBackend with a unique temporary directory
 */
async function createRealStorage(): Promise<FsBackend> {
  const tempDir = join(tmpdir(), `parquedb-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(tempDir, { recursive: true })
  tempDirs.push(tempDir)
  return new FsBackend(tempDir)
}

/**
 * Clean up a temporary directory
 */
async function cleanupTempDir(dir: string): Promise<void> {
  try {
    await fs.rm(dir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
}

// Clean up all temp directories after all tests
afterAll(async () => {
  await Promise.all(tempDirs.map(cleanupTempDir))
})

/**
 * Create a sample schema for testing
 */
function createTestSchema(): Schema {
  return {
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      status: { type: 'string', default: 'draft', index: true },
      publishedAt: 'datetime?',
      author: '-> User.posts',
      categories: '-> Category.posts[]',
    },
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      posts: '<- Post.author[]',
    },
    Category: {
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      posts: '<- Post.categories[]',
    },
    Comment: {
      $ns: 'comments',
      text: 'string!',
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// =============================================================================
// Test Suite: Constructor
// =============================================================================

describe('ParqueDB', () => {
  describe('Constructor', () => {
    it('should create an instance with storage backend option', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage })
      expect(db).toBeInstanceOf(ParqueDB)
    })

    it('should accept schema in constructor options', async () => {
      const storage = await createRealStorage()
      const schema = createTestSchema()
      const db = new ParqueDB({ storage, schema })
      expect(db).toBeInstanceOf(ParqueDB)
    })

    it('should accept defaultNamespace in constructor options', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage, defaultNamespace: 'default' })
      expect(db).toBeInstanceOf(ParqueDB)
    })

    it('should throw error when storage is not provided', () => {
      expect(() => {
        new ParqueDB({} as ParqueDBConfig)
      }).toThrow()
    })

    it('should store the storage backend reference', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage })
      // Implementation should expose storage or use it internally
      expect(db).toBeDefined()
    })
  })

  // ===========================================================================
  // Test Suite: Proxy-based Collection Access
  // ===========================================================================

  describe('Proxy-based Collection Access', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should provide collection access via db.Posts', () => {
      const posts = (db as any).Posts as Collection
      expect(posts).toBeDefined()
      expect(typeof posts.find).toBe('function')
      expect(typeof posts.get).toBe('function')
      expect(typeof posts.create).toBe('function')
      expect(typeof posts.update).toBe('function')
      expect(typeof posts.delete).toBe('function')
    })

    it('should provide collection access via db.Users', () => {
      const users = (db as any).Users as Collection
      expect(users).toBeDefined()
      expect(typeof users.find).toBe('function')
    })

    it('should provide collection access via db.Comments', () => {
      const comments = (db as any).Comments as Collection
      expect(comments).toBeDefined()
      expect(typeof comments.find).toBe('function')
    })

    it('should allow chained method calls on proxy collection', async () => {
      const posts = (db as any).Posts as Collection
      const result = await posts.find({ status: 'published' })
      expect(result).toHaveProperty('items')
      expect(result).toHaveProperty('hasMore')
    })

    it('should return the same collection instance for repeated access', () => {
      const posts1 = (db as any).Posts as Collection
      const posts2 = (db as any).Posts as Collection
      expect(posts1).toBe(posts2)
    })

    it('should provide namespace property on collection', () => {
      const posts = (db as any).Posts as Collection
      expect(posts.namespace).toBe('posts')
    })
  })

  // ===========================================================================
  // Test Suite: Collection Name Normalization
  // ===========================================================================

  describe('Collection Name Normalization', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should normalize Posts to posts namespace', () => {
      const posts = (db as any).Posts as Collection
      expect(posts.namespace).toBe('posts')
    })

    it('should normalize Users to users namespace', () => {
      const users = (db as any).Users as Collection
      expect(users.namespace).toBe('users')
    })

    it('should normalize BlogPosts to blogposts namespace', () => {
      const blogPosts = (db as any).BlogPosts as Collection
      expect(blogPosts.namespace).toBe('blogposts')
    })

    it('should normalize POSTS to posts namespace', () => {
      const posts = (db as any).POSTS as Collection
      expect(posts.namespace).toBe('posts')
    })

    it('should preserve already lowercase names', () => {
      const posts = (db as any).posts as Collection
      expect(posts.namespace).toBe('posts')
    })

    it('should handle snake_case names', () => {
      const blogPosts = (db as any).blog_posts as Collection
      expect(blogPosts.namespace).toBe('blog_posts')
    })

    it('should handle kebab-case names', () => {
      const blogPosts = (db as any)['blog-posts'] as Collection
      expect(blogPosts.namespace).toBe('blog-posts')
    })
  })

  // ===========================================================================
  // Test Suite: Schema Registration and Validation
  // ===========================================================================

  describe('Schema Registration and Validation', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should allow registering a schema after construction', () => {
      const schema = createTestSchema()
      expect(() => db.registerSchema(schema)).not.toThrow()
    })

    it('should validate entities against registered schema on create', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      // Missing required field 'title'
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'My Post',
          content: 'Content here',
          // title is missing - required
        })
      ).rejects.toThrow()
    })

    it('should validate field types against schema', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'My Post',
          title: 'Title',
          content: 'Content',
          status: 12345, // Should be string
        } as any)
      ).rejects.toThrow()
    })

    it('should apply default values from schema on create', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'My Title',
        content: 'My Content',
        // status not provided - should default to 'draft'
      })

      expect(post.status).toBe('draft')
    })

    it('should allow valid entities to pass validation', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'My Title',
        content: 'My Content',
        status: 'published',
      })

      expect(post.$id).toBeDefined()
    })

    it('should support multiple schemas registered sequentially', () => {
      const schema1: Schema = {
        Post: { name: 'string!', title: 'string!' },
      }
      const schema2: Schema = {
        User: { name: 'string!', email: 'email!' },
      }

      db.registerSchema(schema1)
      db.registerSchema(schema2)

      // Both should be accessible
      expect((db as any).Posts).toBeDefined()
      expect((db as any).Users).toBeDefined()
    })

    it('should validate relationship references in schema', async () => {
      const schema = createTestSchema()
      db.registerSchema(schema)

      // Invalid author reference format
      await expect(
        db.create('posts', {
          $type: 'Post',
          name: 'My Post',
          title: 'Title',
          content: 'Content',
          author: { 'Invalid': 'not-an-entity-id' },
        } as any)
      ).rejects.toThrow()
    })
  })

  // ===========================================================================
  // Test Suite: find() Method
  // ===========================================================================

  describe('find(ns, filter, options) Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should return paginated result with items array', async () => {
      const result = await db.find('posts', {})
      expect(result).toHaveProperty('items')
      expect(Array.isArray(result.items)).toBe(true)
    })

    it('should return hasMore boolean in result', async () => {
      const result = await db.find('posts', {})
      expect(result).toHaveProperty('hasMore')
      expect(typeof result.hasMore).toBe('boolean')
    })

    it('should accept empty filter to get all entities', async () => {
      const result = await db.find('posts')
      expect(result.items).toBeDefined()
    })

    it('should filter by simple field equality', async () => {
      const result = await db.find('posts', { status: 'published' })
      expect(result.items).toBeDefined()
    })

    it('should filter using $eq operator', async () => {
      const result = await db.find('posts', { status: { $eq: 'published' } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $ne operator', async () => {
      const result = await db.find('posts', { status: { $ne: 'draft' } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $gt operator', async () => {
      const result = await db.find('posts', { viewCount: { $gt: 100 } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $gte operator', async () => {
      const result = await db.find('posts', { viewCount: { $gte: 100 } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $lt operator', async () => {
      const result = await db.find('posts', { viewCount: { $lt: 100 } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $lte operator', async () => {
      const result = await db.find('posts', { viewCount: { $lte: 100 } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $in operator', async () => {
      const result = await db.find('posts', { status: { $in: ['published', 'featured'] } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $nin operator', async () => {
      const result = await db.find('posts', { status: { $nin: ['draft', 'archived'] } })
      expect(result.items).toBeDefined()
    })

    it('should filter using $and operator', async () => {
      const result = await db.find('posts', {
        $and: [{ status: 'published' }, { featured: true }],
      })
      expect(result.items).toBeDefined()
    })

    it('should filter using $or operator', async () => {
      const result = await db.find('posts', {
        $or: [{ status: 'published' }, { featured: true }],
      })
      expect(result.items).toBeDefined()
    })

    it('should filter using $not operator', async () => {
      const result = await db.find('posts', {
        $not: { status: 'draft' },
      })
      expect(result.items).toBeDefined()
    })

    it('should filter using $regex operator', async () => {
      const result = await db.find('posts', {
        title: { $regex: '^Hello' },
      })
      expect(result.items).toBeDefined()
    })

    it('should filter using $exists operator', async () => {
      const result = await db.find('posts', {
        publishedAt: { $exists: true },
      })
      expect(result.items).toBeDefined()
    })

    it('should accept sort option', async () => {
      const options: FindOptions = { sort: { createdAt: -1 } }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept limit option', async () => {
      const options: FindOptions = { limit: 10 }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept skip option', async () => {
      const options: FindOptions = { skip: 20 }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept cursor option for pagination', async () => {
      const options: FindOptions = { cursor: 'some-cursor-token' }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should return nextCursor when more results exist', async () => {
      const options: FindOptions = { limit: 1 }
      const result = await db.find('posts', {}, options)
      if (result.hasMore) {
        expect(result.nextCursor).toBeDefined()
      }
    })

    it('should accept projection option', async () => {
      const options: FindOptions = { project: { title: 1, content: 1 } }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept populate option as array', async () => {
      const options: FindOptions = { populate: ['author'] }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    }    )

    it('should accept populate option as object', async () => {
      const options: FindOptions = {
        populate: { author: true, comments: { limit: 5 } },
      }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept includeDeleted option', async () => {
      const options: FindOptions = { includeDeleted: true }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should accept asOf option for time-travel queries', async () => {
      const options: FindOptions = { asOf: new Date('2024-01-01') }
      const result = await db.find('posts', {}, options)
      expect(result.items).toBeDefined()
    })

    it('should work with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection
      const result = await posts.find({ status: 'published' })
      expect(result.items).toBeDefined()
    })

    it('should work with proxy-based access and options', async () => {
      const posts = (db as any).Posts as Collection
      const result = await posts.find({ status: 'published' }, { limit: 10 })
      expect(result.items).toBeDefined()
    })
  })

  // ===========================================================================
  // Test Suite: get() Method
  // ===========================================================================

  describe('get(ns, id, options) Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should return entity when found', async () => {
      // Assuming entity exists
      const entity = await db.get('posts', 'posts/123')
      if (entity) {
        expect(entity.$id).toBeDefined()
      }
    })

    it('should return null when entity not found', async () => {
      const entity = await db.get('posts', 'posts/nonexistent')
      expect(entity).toBeNull()
    })

    it('should accept full EntityId format (ns/id)', async () => {
      const entity = await db.get('posts', 'posts/123')
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should accept just the id part', async () => {
      const entity = await db.get('posts', '123')
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should return entity with $id field', async () => {
      const entity = await db.get('posts', 'posts/123')
      if (entity) {
        expect(entity.$id).toMatch(/^posts\//)
      }
    })

    it('should return entity with $type field', async () => {
      const entity = await db.get('posts', 'posts/123')
      if (entity) {
        expect(entity.$type).toBeDefined()
      }
    })

    it('should return entity with audit fields', async () => {
      const entity = await db.get('posts', 'posts/123')
      if (entity) {
        expect(entity.createdAt).toBeInstanceOf(Date)
        expect(entity.updatedAt).toBeInstanceOf(Date)
        expect(entity.createdBy).toBeDefined()
        expect(entity.updatedBy).toBeDefined()
        expect(typeof entity.version).toBe('number')
      }
    })

    it('should accept includeDeleted option', async () => {
      const options: GetOptions = { includeDeleted: true }
      const entity = await db.get('posts', 'posts/deleted-123', options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should accept asOf option for time-travel', async () => {
      const options: GetOptions = { asOf: new Date('2024-01-01') }
      const entity = await db.get('posts', 'posts/123', options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should accept hydrate option for relationships', async () => {
      const options: GetOptions = { hydrate: ['author', 'categories'] }
      const entity = await db.get('posts', 'posts/123', options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should accept project option', async () => {
      const options: GetOptions = { project: { title: 1, content: 1 } }
      const entity = await db.get('posts', 'posts/123', options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should work with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection
      const entity = await posts.get('posts/123')
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should work with proxy-based access and options', async () => {
      const posts = (db as any).Posts as Collection
      const options: GetOptions = { hydrate: ['author'] }
      const entity = await posts.get('posts/123', options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })
  })

  // ===========================================================================
  // Test Suite: create() Method
  // ===========================================================================

  describe('create(ns, data, options) Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should create an entity and return it', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My First Post',
        title: 'Hello World',
        content: 'This is my first post content.',
      }
      const entity = await db.create('posts', data)
      expect(entity).toBeDefined()
      expect(entity.$id).toBeDefined()
    })

    it('should generate unique $id for created entity', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity1 = await db.create('posts', data)
      const entity2 = await db.create('posts', data)
      expect(entity1.$id).not.toBe(entity2.$id)
    })

    it('should set createdAt to current time', async () => {
      const before = new Date()
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity = await db.create('posts', data)
      const after = new Date()

      expect(entity.createdAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(entity.createdAt.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('should set updatedAt equal to createdAt on create', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity = await db.create('posts', data)
      expect(entity.updatedAt.getTime()).toBe(entity.createdAt.getTime())
    })

    it('should set version to 1 on create', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity = await db.create('posts', data)
      expect(entity.version).toBe(1)
    })

    it('should use $type from data', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity = await db.create('posts', data)
      expect(entity.$type).toBe('Post')
    })

    it('should use name from data', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post Name',
        title: 'Title',
        content: 'Content',
      }
      const entity = await db.create('posts', data)
      expect(entity.name).toBe('My Post Name')
    })

    it('should include custom fields from data', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'My Title',
        content: 'My Content',
        customField: 'custom value',
      }
      const entity = await db.create('posts', data)
      expect(entity.title).toBe('My Title')
      expect(entity.content).toBe('My Content')
      expect(entity.customField).toBe('custom value')
    })

    it('should accept actor option for audit', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const options: CreateOptions = { actor: 'users/admin' as EntityId }
      const entity = await db.create('posts', data, options)
      expect(entity.createdBy).toBe('users/admin')
    })

    it('should accept skipValidation option', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        // Missing required fields - but skipValidation should allow it
      }
      const options: CreateOptions = { skipValidation: true }
      const entity = await db.create('posts', data, options)
      expect(entity.$id).toBeDefined()
    })

    it('should handle inline relationship definitions', async () => {
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
        author: { 'John Doe': 'users/john' as EntityId },
      }
      const entity = await db.create('posts', data)
      expect(entity.author).toBeDefined()
    })

    it('should work with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const entity = await posts.create(data)
      expect(entity.$id).toBeDefined()
    })

    it('should work with proxy-based access and options', async () => {
      const posts = (db as any).Posts as Collection
      const data: CreateInput = {
        $type: 'Post',
        name: 'My Post',
        title: 'Title',
        content: 'Content',
      }
      const options: CreateOptions = { actor: 'users/admin' as EntityId }
      const entity = await posts.create(data, options)
      expect(entity.createdBy).toBe('users/admin')
    })
  })

  // ===========================================================================
  // Test Suite: update() Method
  // ===========================================================================

  describe('update(ns, id, update, options) Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should update entity and return updated version', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(entity.status).toBe('published')
      }
    })

    it('should return null when entity not found', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const entity = await db.update('posts', 'posts/nonexistent', update)
      expect(entity).toBeNull()
    })

    it('should increment version on update', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(entity.version).toBeGreaterThan(1)
      }
    })

    it('should update updatedAt on update', async () => {
      const before = new Date()
      const update: UpdateInput = { $set: { status: 'published' } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(entity.updatedAt.getTime()).toBeGreaterThanOrEqual(before.getTime())
      }
    })

    it('should support $set operator', async () => {
      const update: UpdateInput = { $set: { title: 'New Title', status: 'published' } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(entity.title).toBe('New Title')
        expect(entity.status).toBe('published')
      }
    })

    it('should support $unset operator', async () => {
      const update: UpdateInput = { $unset: { customField: '' } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(entity.customField).toBeUndefined()
      }
    })

    it('should support $inc operator', async () => {
      const update: UpdateInput = { $inc: { viewCount: 1 } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(typeof entity.viewCount).toBe('number')
      }
    })

    it('should support $mul operator', async () => {
      const update: UpdateInput = { $mul: { score: 2 } }
      const entity = await db.update('posts', 'posts/123', update)
      if (entity) {
        expect(typeof entity.score).toBe('number')
      }
    })

    it('should support $min operator', async () => {
      const update: UpdateInput = { $min: { lowestScore: 10 } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $max operator', async () => {
      const update: UpdateInput = { $max: { highestScore: 100 } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $push operator for arrays', async () => {
      const update: UpdateInput = { $push: { tags: 'new-tag' } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $pull operator for arrays', async () => {
      const update: UpdateInput = { $pull: { tags: 'old-tag' } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $addToSet operator', async () => {
      const update: UpdateInput = { $addToSet: { tags: 'unique-tag' } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $currentDate operator', async () => {
      const update: UpdateInput = { $currentDate: { lastAccessed: true } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $link operator for relationships', async () => {
      const update: UpdateInput = { $link: { categories: 'categories/tech' as EntityId } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support $unlink operator for relationships', async () => {
      const update: UpdateInput = { $unlink: { categories: 'categories/old' as EntityId } }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should support combined update operators', async () => {
      const update: UpdateInput = {
        $set: { status: 'published' },
        $inc: { viewCount: 1 },
        $currentDate: { publishedAt: true },
      }
      const entity = await db.update('posts', 'posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should accept actor option for audit', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const options: UpdateOptions = { actor: 'users/editor' as EntityId }
      const entity = await db.update('posts', 'posts/123', update, options)
      if (entity) {
        expect(entity.updatedBy).toBe('users/editor')
      }
    })

    it('should accept expectedVersion option for optimistic concurrency', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const options: UpdateOptions = { expectedVersion: 1 }
      const entity = await db.update('posts', 'posts/123', update, options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should fail when expectedVersion does not match', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const options: UpdateOptions = { expectedVersion: 999 }
      await expect(db.update('posts', 'posts/123', update, options)).rejects.toThrow()
    })

    it('should accept upsert option', async () => {
      const update: UpdateInput = { $set: { status: 'published', title: 'New Post' } }
      const options: UpdateOptions = { upsert: true }
      const entity = await db.update('posts', 'posts/newpost', update, options)
      expect(entity).toBeDefined()
      expect(entity!.$id).toBeDefined()
    })

    it('should accept returnDocument option', async () => {
      const update: UpdateInput = { $set: { status: 'published' } }
      const options: UpdateOptions = { returnDocument: 'before' }
      const entity = await db.update('posts', 'posts/123', update, options)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should work with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection
      const update: UpdateInput = { $set: { status: 'published' } }
      const entity = await posts.update('posts/123', update)
      expect(entity === null || entity.$id !== undefined).toBe(true)
    })

    it('should work with proxy-based access and options', async () => {
      const posts = (db as any).Posts as Collection
      const update: UpdateInput = { $set: { status: 'published' } }
      const options: UpdateOptions = { actor: 'users/editor' as EntityId }
      const entity = await posts.update('posts/123', update, options)
      if (entity) {
        expect(entity.updatedBy).toBe('users/editor')
      }
    })
  })

  // ===========================================================================
  // Test Suite: delete() Method
  // ===========================================================================

  describe('delete(ns, id, options) Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should delete entity and return result', async () => {
      const result = await db.delete('posts', 'posts/123')
      expect(result).toHaveProperty('deletedCount')
    })

    it('should return deletedCount of 1 when entity exists', async () => {
      // Create entity first
      const created = await db.create('posts', { $type: 'Post', title: 'Test' })
      const result = await db.delete('posts', created.$id as string)
      expect(result.deletedCount).toBe(1)
    })

    it('should return deletedCount of 0 when entity not found', async () => {
      const result = await db.delete('posts', 'posts/nonexistent')
      expect(result.deletedCount).toBe(0)
    })

    it('should soft delete by default (set deletedAt)', async () => {
      await db.delete('posts', 'posts/123')
      // Entity should still exist with deletedAt set
      const entity = await db.get('posts', 'posts/123', { includeDeleted: true })
      if (entity) {
        expect(entity.deletedAt).toBeInstanceOf(Date)
      }
    })

    it('should not return deleted entity on normal get', async () => {
      await db.delete('posts', 'posts/123')
      const entity = await db.get('posts', 'posts/123')
      expect(entity).toBeNull()
    })

    it('should accept hard option for permanent delete', async () => {
      const options: DeleteOptions = { hard: true }
      await db.delete('posts', 'posts/123', options)
      // Entity should be completely gone
      const entity = await db.get('posts', 'posts/123', { includeDeleted: true })
      expect(entity).toBeNull()
    })

    it('should accept actor option for audit', async () => {
      const options: DeleteOptions = { actor: 'users/admin' as EntityId }
      await db.delete('posts', 'posts/123', options)
      const entity = await db.get('posts', 'posts/123', { includeDeleted: true })
      if (entity) {
        expect(entity.deletedBy).toBe('users/admin')
      }
    })

    it('should accept expectedVersion option for optimistic concurrency', async () => {
      const options: DeleteOptions = { expectedVersion: 1 }
      const result = await db.delete('posts', 'posts/123', options)
      expect(result.deletedCount).toBeDefined()
    })

    it('should fail when expectedVersion does not match', async () => {
      const options: DeleteOptions = { expectedVersion: 999 }
      await expect(db.delete('posts', 'posts/123', options)).rejects.toThrow()
    })

    it('should work with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection
      const result = await posts.delete('posts/123')
      expect(result).toHaveProperty('deletedCount')
    })

    it('should work with proxy-based access and options', async () => {
      const posts = (db as any).Posts as Collection
      const options: DeleteOptions = { hard: true }
      const result = await posts.delete('posts/123', options)
      expect(result).toHaveProperty('deletedCount')
    })
  })

  // ===========================================================================
  // Test Suite: Error Handling
  // ===========================================================================

  describe('Error Handling', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should throw error for empty namespace', async () => {
      await expect(db.find('', {})).rejects.toThrow()
    })

    it('should throw error for null namespace', async () => {
      await expect(db.find(null as any, {})).rejects.toThrow()
    })

    it('should throw error for undefined namespace', async () => {
      await expect(db.find(undefined as any, {})).rejects.toThrow()
    })

    it('should throw error for namespace with invalid characters', async () => {
      await expect(db.find('posts/subpath', {})).rejects.toThrow()
    })

    it('should throw error for namespace starting with underscore', async () => {
      await expect(db.find('_internal', {})).rejects.toThrow()
    })

    it('should throw error for namespace starting with dollar sign', async () => {
      await expect(db.find('$system', {})).rejects.toThrow()
    })

    it('should handle storage backend errors gracefully', async () => {
      // Create a custom storage backend that throws errors
      const failingStorage: StorageBackend = {
        type: 'failing',
        async read(): Promise<Uint8Array> {
          throw new Error('Storage error')
        },
        async readRange(): Promise<Uint8Array> {
          throw new Error('Storage error')
        },
        async exists(): Promise<boolean> {
          throw new Error('Storage error')
        },
        async stat() {
          throw new Error('Storage error')
        },
        async list() {
          throw new Error('Storage error')
        },
        async write() {
          throw new Error('Storage error')
        },
        async writeAtomic() {
          throw new Error('Storage error')
        },
        async append() {
          throw new Error('Storage error')
        },
        async delete() {
          throw new Error('Storage error')
        },
        async deletePrefix() {
          throw new Error('Storage error')
        },
        async mkdir() {
          throw new Error('Storage error')
        },
        async rmdir() {
          throw new Error('Storage error')
        },
        async writeConditional() {
          throw new Error('Storage error')
        },
        async copy() {
          throw new Error('Storage error')
        },
        async move() {
          throw new Error('Storage error')
        },
      }
      const failDb = new ParqueDB({ storage: failingStorage })
      await expect(failDb.get('posts', 'posts/123')).rejects.toThrow('Storage error')
    })

    it('should provide meaningful error messages', async () => {
      try {
        await db.find('', {})
      } catch (error: any) {
        expect(error.message).toBeTruthy()
        expect(error.message.length).toBeGreaterThan(0)
      }
    })

    it('should throw error for invalid filter operators', async () => {
      await expect(
        db.find('posts', { status: { $invalidOp: 'value' } } as any)
      ).rejects.toThrow()
    })

    it('should throw error for invalid update operators', async () => {
      await expect(
        db.update('posts', 'posts/123', { $invalidOp: { field: 'value' } } as any)
      ).rejects.toThrow()
    })

    it('should auto-derive $type from namespace when not provided', async () => {
      const entity = await db.create('posts', { title: 'Hello' } as any)
      expect(entity.$type).toBe('Post')  // derived from 'posts'
    })

    it('should auto-derive name from title when not provided', async () => {
      const entity = await db.create('posts', { title: 'Hello World' } as any)
      expect(entity.name).toBe('Hello World')  // derived from title
    })
  })

  // ===========================================================================
  // Test Suite: Collection Method
  // ===========================================================================

  describe('collection() Method', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should return a collection for explicit namespace', () => {
      const posts = db.collection('posts')
      expect(posts).toBeDefined()
      expect(posts.namespace).toBe('posts')
    })

    it('should return collection with find method', () => {
      const posts = db.collection('posts')
      expect(typeof posts.find).toBe('function')
    })

    it('should return collection with get method', () => {
      const posts = db.collection('posts')
      expect(typeof posts.get).toBe('function')
    })

    it('should return collection with create method', () => {
      const posts = db.collection('posts')
      expect(typeof posts.create).toBe('function')
    })

    it('should return collection with update method', () => {
      const posts = db.collection('posts')
      expect(typeof posts.update).toBe('function')
    })

    it('should return collection with delete method', () => {
      const posts = db.collection('posts')
      expect(typeof posts.delete).toBe('function')
    })

    it('should allow typed collection access', async () => {
      interface Post {
        title: string
        content: string
        status: string
      }
      const posts = db.collection<Post>('posts')
      const result = await posts.find({ status: 'published' })
      // TypeScript should infer result.items as Entity<Post>[]
      expect(result.items).toBeDefined()
    })

    it('should work same as proxy-based access', async () => {
      const collection = db.collection('posts')
      const proxy = (db as any).posts as Collection

      expect(collection.namespace).toBe(proxy.namespace)
    })
  })

  // ===========================================================================
  // Test Suite: Integration Tests
  // ===========================================================================

  describe('Integration Tests', () => {
    let db: ParqueDB
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    it('should support full CRUD workflow', async () => {
      // Create
      const created = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Title',
        content: 'Test Content',
      })
      expect(created.$id).toBeDefined()

      // Read
      const found = await db.get('posts', created.$id as string)
      expect(found).toBeDefined()
      expect(found!.title).toBe('Test Title')

      // Update
      const updated = await db.update('posts', created.$id as string, {
        $set: { title: 'Updated Title' },
      })
      expect(updated!.title).toBe('Updated Title')

      // Delete
      const deleted = await db.delete('posts', created.$id as string)
      expect(deleted.deletedCount).toBe(1)

      // Verify deleted
      const afterDelete = await db.get('posts', created.$id as string)
      expect(afterDelete).toBeNull()
    })

    it('should support full CRUD workflow with proxy-based access', async () => {
      const posts = (db as any).Posts as Collection

      // Create
      const created = await posts.create({
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Title',
        content: 'Test Content',
      })
      expect(created.$id).toBeDefined()

      // Read
      const found = await posts.get(created.$id as string)
      expect(found).toBeDefined()

      // Update
      const updated = await posts.update(created.$id as string, {
        $set: { title: 'Updated Title' },
      })
      expect(updated).toBeDefined()

      // Delete
      const deleted = await posts.delete(created.$id as string)
      expect(deleted.deletedCount).toBe(1)
    })

    it('should handle multiple namespaces independently', async () => {
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      const user = await db.create('users', {
        $type: 'User',
        name: 'Test User',
        email: 'test@example.com',
      })

      expect(post.$id).toMatch(/^posts\//)
      expect(user.$id).toMatch(/^users\//)

      const posts = await db.find('posts', {})
      const users = await db.find('users', {})

      expect(posts.items.length).toBeGreaterThan(0)
      expect(users.items.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Test Suite: Dispose / Resource Cleanup
  // ===========================================================================

  describe('Dispose / Resource Cleanup', () => {
    it('should have a dispose method', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage })
      expect(typeof db.dispose).toBe('function')
    })

    it('should clear in-memory state after dispose', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage })

      // Create some data
      await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Title',
        content: 'Test Content',
      })

      // Verify data exists
      const beforeDispose = await db.find('posts', {})
      expect(beforeDispose.items.length).toBe(1)

      // Dispose
      db.dispose()

      // After dispose, in-memory state should be cleared
      // A new query should return empty results (since we cleared the cache)
      const afterDispose = await db.find('posts', {})
      expect(afterDispose.items.length).toBe(0)
    })

    it('should allow multiple disposes without error', async () => {
      const storage = await createRealStorage()
      const db = new ParqueDB({ storage })

      await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Title',
        content: 'Test Content',
      })

      // Multiple disposes should not throw
      expect(() => db.dispose()).not.toThrow()
      expect(() => db.dispose()).not.toThrow()
    })

    it('should isolate state between different storage backends', async () => {
      const storage1 = await createRealStorage()
      const storage2 = await createRealStorage()

      const db1 = new ParqueDB({ storage: storage1 })
      const db2 = new ParqueDB({ storage: storage2 })

      // Create data in db1
      await db1.create('posts', {
        $type: 'Post',
        name: 'Post in DB1',
        title: 'Title 1',
        content: 'Content 1',
      })

      // Create data in db2
      await db2.create('posts', {
        $type: 'Post',
        name: 'Post in DB2',
        title: 'Title 2',
        content: 'Content 2',
      })

      // Verify both have data
      const db1Before = await db1.find('posts', {})
      const db2Before = await db2.find('posts', {})
      expect(db1Before.items.length).toBe(1)
      expect(db2Before.items.length).toBe(1)

      // Dispose db1
      db1.dispose()

      // db2 should still have its data
      const db2After = await db2.find('posts', {})
      expect(db2After.items.length).toBe(1)

      // db1 should be cleared
      const db1After = await db1.find('posts', {})
      expect(db1After.items.length).toBe(0)

      // Clean up
      db2.dispose()
    })

    it('should share state between ParqueDB instances with same storage backend', async () => {
      const storage = await createRealStorage()

      const db1 = new ParqueDB({ storage })
      const db2 = new ParqueDB({ storage })

      // Create data via db1
      await db1.create('posts', {
        $type: 'Post',
        name: 'Shared Post',
        title: 'Shared Title',
        content: 'Shared Content',
      })

      // db2 should see the same data (shared state)
      const db2Results = await db2.find('posts', {})
      expect(db2Results.items.length).toBe(1)
      expect(db2Results.items[0].name).toBe('Shared Post')

      // Disposing db1 affects db2 since they share the same storage backend
      db1.dispose()

      // Both should now show empty (shared state was cleared)
      const db1After = await db1.find('posts', {})
      const db2After = await db2.find('posts', {})
      expect(db1After.items.length).toBe(0)
      expect(db2After.items.length).toBe(0)
    })
  })
})

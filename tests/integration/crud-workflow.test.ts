/**
 * CRUD Workflow E2E Tests
 *
 * Full database workflows testing complete user scenarios.
 * These tests simulate real-world usage patterns with real storage.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { FsBackend } from '../../src/storage/FsBackend'
import {
  createTestEntity,
  createUserInput,
  createPostInput,
  createBlogSchema,
  generateTestId,
  createEntityId,
} from '../factories'
import { USERS, POSTS, CATEGORIES, BLOG_SCHEMA, FILTERS } from '../fixtures'
import type { Entity, EntityId, CreateInput, Filter } from '../../src/types'

/**
 * SimpleDB - A simplified database implementation for testing
 * Uses real FsBackend for persistence
 */
class SimpleDB {
  private storage = new Map<string, Entity>()
  private backend: FsBackend

  constructor(backend: FsBackend) {
    this.backend = backend
  }

  /**
   * Create an entity
   */
  async create(namespace: string, input: CreateInput): Promise<Entity> {
    const id = generateTestId()
    const entityId = createEntityId(namespace, id)
    const now = new Date()

    const entity: Entity = {
      $id: entityId,
      $type: input.$type,
      name: input.name,
      createdAt: now,
      createdBy: 'system/test' as EntityId,
      updatedAt: now,
      updatedBy: 'system/test' as EntityId,
      version: 1,
      ...input,
    }

    this.storage.set(entityId, entity)

    // Persist to storage
    const data = new TextEncoder().encode(JSON.stringify(entity))
    await this.backend.write(`data/${namespace}/${id}.json`, data)

    return entity
  }

  /**
   * Get an entity by ID
   */
  async get(entityId: EntityId): Promise<Entity | null> {
    // Try memory first
    const cached = this.storage.get(entityId)
    if (cached) return cached

    // Try to load from storage
    const [namespace, id] = entityId.split('/')
    const path = `data/${namespace}/${id}.json`

    if (await this.backend.exists(path)) {
      const data = await this.backend.read(path)
      const entity = JSON.parse(new TextDecoder().decode(data)) as Entity
      // Restore Date objects
      entity.createdAt = new Date(entity.createdAt)
      entity.updatedAt = new Date(entity.updatedAt)
      this.storage.set(entityId, entity)
      return entity
    }

    return null
  }

  /**
   * Find entities matching a filter
   */
  async find(namespace: string, filter?: Filter): Promise<Entity[]> {
    const results: Entity[] = []

    // Load all entities from storage for the namespace
    const listResult = await this.backend.list(`data/${namespace}`)

    for (const filePath of listResult.files) {
      if (!filePath.endsWith('.json')) continue

      const data = await this.backend.read(filePath)
      const entity = JSON.parse(new TextDecoder().decode(data)) as Entity
      entity.createdAt = new Date(entity.createdAt)
      entity.updatedAt = new Date(entity.updatedAt)

      if (entity.deletedAt) continue // Skip soft-deleted

      if (!filter || this.matchesFilter(entity, filter)) {
        results.push(entity)
        this.storage.set(entity.$id, entity)
      }
    }

    return results
  }

  /**
   * Update an entity
   */
  async update(
    entityId: EntityId,
    update: Partial<Record<string, unknown>>
  ): Promise<Entity | null> {
    const entity = await this.get(entityId)
    if (!entity) return null

    const updated: Entity = {
      ...entity,
      ...update,
      updatedAt: new Date(),
      version: entity.version + 1,
    }

    this.storage.set(entityId, updated)

    // Persist to storage
    const [namespace, id] = entityId.split('/')
    const data = new TextEncoder().encode(JSON.stringify(updated))
    await this.backend.write(`data/${namespace}/${id}.json`, data)

    return updated
  }

  /**
   * Delete an entity (soft delete)
   */
  async delete(entityId: EntityId, hard = false): Promise<boolean> {
    const entity = await this.get(entityId)
    if (!entity) return false

    const [namespace, id] = entityId.split('/')

    if (hard) {
      this.storage.delete(entityId)
      await this.backend.delete(`data/${namespace}/${id}.json`)
    } else {
      entity.deletedAt = new Date()
      entity.deletedBy = 'system/test' as EntityId
      this.storage.set(entityId, entity)

      const data = new TextEncoder().encode(JSON.stringify(entity))
      await this.backend.write(`data/${namespace}/${id}.json`, data)
    }

    return true
  }

  /**
   * Count entities matching filter
   */
  async count(namespace: string, filter?: Filter): Promise<number> {
    const entities = await this.find(namespace, filter)
    return entities.length
  }

  /**
   * Check if entity exists
   */
  async exists(entityId: EntityId): Promise<boolean> {
    const [namespace, id] = entityId.split('/')
    return this.backend.exists(`data/${namespace}/${id}.json`)
  }

  /**
   * Clear all data
   */
  async clear(): Promise<void> {
    this.storage.clear()
    await this.backend.deletePrefix('data/')
  }

  private matchesFilter(entity: Entity, filter: Filter): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (key.startsWith('$')) continue // Skip operators for now

      const entityValue = (entity as Record<string, unknown>)[key]

      if (typeof value === 'object' && value !== null) {
        // Handle operators
        for (const [op, opValue] of Object.entries(value)) {
          switch (op) {
            case '$eq':
              if (entityValue !== opValue) return false
              break
            case '$ne':
              if (entityValue === opValue) return false
              break
            case '$in':
              if (!Array.isArray(opValue) || !opValue.includes(entityValue)) return false
              break
            case '$exists':
              if (opValue && entityValue === undefined) return false
              if (!opValue && entityValue !== undefined) return false
              break
          }
        }
      } else if (entityValue !== value) {
        return false
      }
    }
    return true
  }
}

describe('CRUD Workflow E2E', () => {
  let db: SimpleDB
  let backend: FsBackend
  let testDir: string

  beforeAll(async () => {
    testDir = join(tmpdir(), `parquedb-crud-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
    db = new SimpleDB(backend)
  })

  afterAll(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  beforeEach(async () => {
    await db.clear()
    // Ensure data directories exist
    await backend.mkdir('data/users')
    await backend.mkdir('data/posts')
    await backend.mkdir('data/categories')
    await backend.mkdir('data/products')
    await backend.mkdir('data/items')
  })

  describe('User Management Workflow', () => {
    it('should complete a full user lifecycle', async () => {
      // 1. Create a new user
      const userInput = createUserInput({
        name: 'Alice Johnson',
        email: 'alice@example.com',
        bio: 'Software engineer',
      })

      const user = await db.create('users', userInput)

      expect(user).toBeValidEntity()
      expect(user.$type).toBe('User')
      expect(user.name).toBe('Alice Johnson')
      expect(user.email).toBe('alice@example.com')
      expect(user.version).toBe(1)

      // 2. Read the user back
      const fetchedUser = await db.get(user.$id)
      expect(fetchedUser).not.toBeNull()
      expect(fetchedUser!.$id).toBe(user.$id)

      // 3. Update the user
      const updatedUser = await db.update(user.$id, {
        bio: 'Senior software engineer',
      })

      expect(updatedUser).not.toBeNull()
      expect(updatedUser!.bio).toBe('Senior software engineer')
      expect(updatedUser!.version).toBe(2)

      // 4. Soft delete the user
      const deleted = await db.delete(user.$id)
      expect(deleted).toBe(true)

      // 5. User should not appear in regular queries
      const users = await db.find('users')
      expect(users.length).toBe(0)

      // 6. User data still exists (soft deleted)
      const exists = await db.exists(user.$id)
      expect(exists).toBe(true)
    })

    it('should handle multiple users', async () => {
      // Create multiple users
      const users = await Promise.all([
        db.create('users', createUserInput({ name: 'User 1' })),
        db.create('users', createUserInput({ name: 'User 2' })),
        db.create('users', createUserInput({ name: 'User 3' })),
      ])

      expect(users).toHaveLength(3)

      // Find all users
      const allUsers = await db.find('users')
      expect(allUsers).toHaveLength(3)

      // Count users
      const count = await db.count('users')
      expect(count).toBe(3)
    })
  })

  describe('Blog Post Workflow', () => {
    let author: Entity

    beforeEach(async () => {
      // Create an author for posts
      author = await db.create('users', createUserInput({
        name: 'Blog Author',
        email: 'author@blog.com',
      }))
    })

    it('should complete a full post lifecycle', async () => {
      // 1. Create a draft post
      const post = await db.create('posts', createPostInput({
        title: 'Getting Started with ParqueDB',
        content: '# Introduction\n\nParqueDB is...',
        status: 'draft',
        author: { [author.name]: author.$id },
      }))

      expect(post.status).toBe('draft')
      expect(post.publishedAt).toBeUndefined()

      // 2. Update to published
      const publishedPost = await db.update(post.$id, {
        status: 'published',
        publishedAt: new Date(),
      })

      expect(publishedPost!.status).toBe('published')
      expect(publishedPost!.publishedAt).toBeInstanceOf(Date)
      expect(publishedPost!.version).toBe(2)

      // 3. Edit the published post
      const editedPost = await db.update(post.$id, {
        content: '# Introduction\n\nParqueDB is a database...',
      })

      expect(editedPost!.version).toBe(3)

      // 4. Archive the post
      const archivedPost = await db.update(post.$id, {
        status: 'archived',
      })

      expect(archivedPost!.status).toBe('archived')
    })

    it('should filter posts by status', async () => {
      // Create posts with different statuses
      await db.create('posts', createPostInput({ status: 'draft' }))
      await db.create('posts', createPostInput({ status: 'draft' }))
      await db.create('posts', createPostInput({ status: 'published' }))
      await db.create('posts', createPostInput({ status: 'archived' }))

      // Find draft posts
      const drafts = await db.find('posts', { status: 'draft' })
      expect(drafts).toHaveLength(2)

      // Find published posts
      const published = await db.find('posts', { status: 'published' })
      expect(published).toHaveLength(1)

      // Find archived posts
      const archived = await db.find('posts', { status: 'archived' })
      expect(archived).toHaveLength(1)
    })

    it('should handle post with categories', async () => {
      // Create categories
      const techCategory = await db.create('categories', {
        $type: 'Category',
        name: 'Technology',
        slug: 'tech',
      })

      const dbCategory = await db.create('categories', {
        $type: 'Category',
        name: 'Databases',
        slug: 'databases',
      })

      // Create post with categories
      const post = await db.create('posts', createPostInput({
        title: 'ParqueDB Deep Dive',
        categories: {
          [techCategory.name]: techCategory.$id,
          [dbCategory.name]: dbCategory.$id,
        },
      }))

      expect(post.categories).toEqual({
        [techCategory.name]: techCategory.$id,
        [dbCategory.name]: dbCategory.$id,
      })
    })
  })

  describe('Concurrent Operations', () => {
    it('should handle concurrent creates', async () => {
      const createPromises = Array.from({ length: 20 }, (_, i) =>
        db.create('items', {
          $type: 'Item',
          name: `Item ${i}`,
          index: i,
        })
      )

      const items = await Promise.all(createPromises)

      expect(items).toHaveLength(20)
      expect(new Set(items.map((i) => i.$id)).size).toBe(20) // All unique IDs
    })

    it('should handle sequential reads and writes correctly', async () => {
      // Create initial data
      const item = await db.create('items', {
        $type: 'Item',
        name: 'Shared Item',
        count: 0,
      })

      // Sequential operations (concurrent r/w on real filesystems can race)
      const result1 = await db.get(item.$id)
      expect(result1).not.toBeNull()
      expect(result1!.count).toBe(0)

      const result2 = await db.update(item.$id, { count: 1 })
      expect(result2).not.toBeNull()
      expect(result2!.count).toBe(1)

      const result3 = await db.get(item.$id)
      expect(result3).not.toBeNull()
      expect(result3!.count).toBe(1)

      const result4 = await db.update(item.$id, { count: 2 })
      expect(result4).not.toBeNull()
      expect(result4!.count).toBe(2)

      const findResult = await db.find('items')
      expect(findResult.length).toBeGreaterThan(0)
    })
  })

  describe('Error Handling', () => {
    it('should handle updates to non-existent entities', async () => {
      const result = await db.update('items/nonexistent' as EntityId, { name: 'Test' })
      expect(result).toBeNull()
    })

    it('should handle deletes of non-existent entities', async () => {
      const result = await db.delete('items/nonexistent' as EntityId)
      expect(result).toBe(false)
    })

    it('should handle gets of non-existent entities', async () => {
      const result = await db.get('items/nonexistent' as EntityId)
      expect(result).toBeNull()
    })
  })

  describe('Query Scenarios', () => {
    beforeEach(async () => {
      // Seed test data
      await db.create('products', { $type: 'Product', name: 'Product A', price: 10, category: 'electronics' })
      await db.create('products', { $type: 'Product', name: 'Product B', price: 25, category: 'electronics' })
      await db.create('products', { $type: 'Product', name: 'Product C', price: 50, category: 'clothing' })
      await db.create('products', { $type: 'Product', name: 'Product D', price: 100, category: 'clothing' })
    })

    it('should filter by category', async () => {
      const electronics = await db.find('products', { category: 'electronics' })
      expect(electronics).toHaveLength(2)

      const clothing = await db.find('products', { category: 'clothing' })
      expect(clothing).toHaveLength(2)
    })

    it('should filter by type', async () => {
      const products = await db.find('products', { $type: 'Product' })
      expect(products).toHaveLength(4)
    })

    it('should find all without filter', async () => {
      const all = await db.find('products')
      expect(all).toHaveLength(4)
    })
  })
})

describe('Data Integrity E2E', () => {
  let db: SimpleDB
  let backend: FsBackend
  let testDir: string

  beforeAll(async () => {
    testDir = join(tmpdir(), `parquedb-integrity-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
    db = new SimpleDB(backend)
  })

  afterAll(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  beforeEach(async () => {
    await db.clear()
    await backend.mkdir('data/items')
  })

  it('should maintain version consistency', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Versioned Item',
    })

    expect(entity.version).toBe(1)

    // Multiple updates
    let current = entity
    for (let i = 2; i <= 10; i++) {
      current = (await db.update(current.$id, { name: `Update ${i}` }))!
      expect(current.version).toBe(i)
    }

    // Final check
    const final = await db.get(entity.$id)
    expect(final!.version).toBe(10)
  })

  it('should track timestamps correctly', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Timestamped Item',
    })

    const createdAt = entity.createdAt

    // Wait and update
    await new Promise((resolve) => setTimeout(resolve, 10))

    const updated = await db.update(entity.$id, { name: 'Updated Name' })

    expect(updated!.createdAt.getTime()).toBe(createdAt.getTime())
    expect(updated!.updatedAt.getTime()).toBeGreaterThan(createdAt.getTime())
  })

  it('should preserve entity ID through updates', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Original',
    })

    const originalId = entity.$id

    const updated = await db.update(entity.$id, { name: 'Updated' })

    expect(updated!.$id).toBe(originalId)
  })

  it('should persist data across storage operations', async () => {
    const entity = await db.create('items', {
      $type: 'Item',
      name: 'Persisted Item',
      data: { nested: { value: 42 } },
    })

    // Read directly from storage to verify persistence
    const [namespace, id] = entity.$id.split('/')
    const storedData = await backend.read(`data/${namespace}/${id}.json`)
    const parsed = JSON.parse(new TextDecoder().decode(storedData))

    expect(parsed.name).toBe('Persisted Item')
    expect(parsed.data.nested.value).toBe(42)
  })
})

/**
 * ParqueDBClient Unit Tests
 *
 * Tests for the main ParqueDBClient class:
 * - Client initialization and configuration
 * - Proxy-based collection access (db.Posts, db.Users, etc.)
 * - Collection caching
 * - Direct method access
 * - Actor option handling
 *
 * Uses mocked RPC service to test client-side behavior in isolation.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  ParqueDBClient,
  createParqueDBClient,
  type ParqueDBService,
  type Collection,
} from '../../../src/client/ParqueDBClient'
import type { Entity, PaginatedResult, DeleteResult } from '../../../src/types'

// =============================================================================
// Mock Service Factory
// =============================================================================

/**
 * Create a mock ParqueDB service for testing client behavior
 * Each method is a vi.fn() so we can verify calls and set return values
 */
function createMockService(): ParqueDBService & {
  find: ReturnType<typeof vi.fn>
  get: ReturnType<typeof vi.fn>
  create: ReturnType<typeof vi.fn>
  createMany: ReturnType<typeof vi.fn>
  update: ReturnType<typeof vi.fn>
  delete: ReturnType<typeof vi.fn>
  deleteMany: ReturnType<typeof vi.fn>
  count: ReturnType<typeof vi.fn>
  exists: ReturnType<typeof vi.fn>
  getRelationships: ReturnType<typeof vi.fn>
  link: ReturnType<typeof vi.fn>
  unlink: ReturnType<typeof vi.fn>
  flush: ReturnType<typeof vi.fn>
  getFlushStatus: ReturnType<typeof vi.fn>
} {
  return {
    find: vi.fn().mockResolvedValue({ items: [], hasMore: false }),
    get: vi.fn().mockResolvedValue(null),
    create: vi.fn().mockResolvedValue({
      $id: 'test/1',
      $type: 'Test',
      name: 'Test',
      version: 1,
      createdAt: new Date().toISOString(),
      createdBy: 'system',
    }),
    createMany: vi.fn().mockResolvedValue([]),
    update: vi.fn().mockResolvedValue({
      $id: 'test/1',
      $type: 'Test',
      name: 'Updated',
      version: 2,
    }),
    delete: vi.fn().mockResolvedValue({ deletedCount: 1 }),
    deleteMany: vi.fn().mockResolvedValue({ deletedCount: 0 }),
    count: vi.fn().mockResolvedValue(0),
    exists: vi.fn().mockResolvedValue(false),
    getRelationships: vi.fn().mockResolvedValue([]),
    link: vi.fn().mockResolvedValue(undefined),
    unlink: vi.fn().mockResolvedValue(undefined),
    flush: vi.fn().mockResolvedValue(undefined),
    getFlushStatus: vi.fn().mockResolvedValue({ unflushedCount: 0 }),
  } as unknown as ParqueDBService & {
    find: ReturnType<typeof vi.fn>
    get: ReturnType<typeof vi.fn>
    create: ReturnType<typeof vi.fn>
    createMany: ReturnType<typeof vi.fn>
    update: ReturnType<typeof vi.fn>
    delete: ReturnType<typeof vi.fn>
    deleteMany: ReturnType<typeof vi.fn>
    count: ReturnType<typeof vi.fn>
    exists: ReturnType<typeof vi.fn>
    getRelationships: ReturnType<typeof vi.fn>
    link: ReturnType<typeof vi.fn>
    unlink: ReturnType<typeof vi.fn>
    flush: ReturnType<typeof vi.fn>
    getFlushStatus: ReturnType<typeof vi.fn>
  }
}

// =============================================================================
// ParqueDBClient Constructor Tests
// =============================================================================

describe('ParqueDBClient', () => {
  let mockService: ReturnType<typeof createMockService>
  let client: ParqueDBClient

  beforeEach(() => {
    mockService = createMockService()
    client = new ParqueDBClient(mockService)
  })

  describe('constructor', () => {
    it('should create client with service stub', () => {
      expect(client).toBeInstanceOf(ParqueDBClient)
    })

    it('should accept options', () => {
      const clientWithOptions = new ParqueDBClient(mockService, {
        actor: 'users/admin',
        timeout: 5000,
        debug: true,
      })
      expect(clientWithOptions).toBeInstanceOf(ParqueDBClient)
    })

    it('should work without options', () => {
      const clientNoOptions = new ParqueDBClient(mockService)
      expect(clientNoOptions).toBeInstanceOf(ParqueDBClient)
    })
  })

  // ===========================================================================
  // Proxy-based Collection Access
  // ===========================================================================

  describe('Proxy-based Collection Access', () => {
    it('should provide access via db.Posts', () => {
      const posts = (client as any).Posts as Collection
      expect(posts).toBeDefined()
      expect(posts.namespace).toBe('posts')
    })

    it('should provide access via db.Users', () => {
      const users = (client as any).Users as Collection
      expect(users).toBeDefined()
      expect(users.namespace).toBe('users')
    })

    it('should provide access via db.Comments', () => {
      const comments = (client as any).Comments as Collection
      expect(comments).toBeDefined()
      expect(comments.namespace).toBe('comments')
    })

    it('should provide access via db.Categories', () => {
      const categories = (client as any).Categories as Collection
      expect(categories).toBeDefined()
      expect(categories.namespace).toBe('categories')
    })

    it('should provide access via db.Tags', () => {
      const tags = (client as any).Tags as Collection
      expect(tags).toBeDefined()
      expect(tags.namespace).toBe('tags')
    })

    it('should normalize PascalCase to camelCase namespace', () => {
      const blogPosts = (client as any).BlogPosts as Collection
      expect(blogPosts.namespace).toBe('blogPosts')
    })

    it('should handle single-letter namespace', () => {
      const x = (client as any).X as Collection
      expect(x.namespace).toBe('x')
    })

    it('should cache collection instances', () => {
      const posts1 = (client as any).Posts as Collection
      const posts2 = (client as any).Posts as Collection
      expect(posts1).toBe(posts2)
    })

    it('should cache different collections separately', () => {
      const posts = (client as any).Posts as Collection
      const users = (client as any).Users as Collection
      expect(posts).not.toBe(users)
    })

    it('should return actual methods when accessed', () => {
      expect(typeof client.collection).toBe('function')
      expect(typeof client.find).toBe('function')
      expect(typeof client.get).toBe('function')
      expect(typeof client.create).toBe('function')
      expect(typeof client.update).toBe('function')
      expect(typeof client.delete).toBe('function')
    })
  })

  // ===========================================================================
  // collection() Method
  // ===========================================================================

  describe('collection() Method', () => {
    it('should return collection by name', () => {
      const posts = client.collection('posts')
      expect(posts).toBeDefined()
      expect(posts.namespace).toBe('posts')
    })

    it('should support typed collections', () => {
      interface Post {
        title: string
        content: string
        status: 'draft' | 'published'
      }
      const posts = client.collection<Post>('posts')
      expect(posts.namespace).toBe('posts')
    })

    it('should cache collections', () => {
      const posts1 = client.collection('posts')
      const posts2 = client.collection('posts')
      expect(posts1).toBe(posts2)
    })

    it('should use lowercase namespace', () => {
      const collection = client.collection('myCollection')
      expect(collection.namespace).toBe('myCollection')
    })
  })

  // ===========================================================================
  // Direct Method Access
  // ===========================================================================

  describe('Direct Methods', () => {
    describe('find', () => {
      it('should call service find with namespace and filter', async () => {
        mockService.find.mockResolvedValue({
          items: [{ $id: 'posts/1', name: 'Test' }],
          hasMore: false,
        })

        const result = await client.find('posts', { status: 'published' })

        expect(mockService.find).toHaveBeenCalledWith('posts', { status: 'published' }, undefined)
        expect(result.items).toHaveLength(1)
      })

      it('should pass options to service find', async () => {
        await client.find('posts', {}, { limit: 10, skip: 5 })

        expect(mockService.find).toHaveBeenCalledWith('posts', {}, { limit: 10, skip: 5 })
      })
    })

    describe('get', () => {
      it('should call service get with namespace and id', async () => {
        mockService.get.mockResolvedValue({ $id: 'posts/1', name: 'Test' })

        const result = await client.get('posts', 'post-123')

        expect(mockService.get).toHaveBeenCalledWith('posts', 'post-123', undefined)
        expect(result).toEqual({ $id: 'posts/1', name: 'Test' })
      })

      it('should pass options to service get', async () => {
        await client.get('posts', 'post-123', { hydrate: ['author'] })

        expect(mockService.get).toHaveBeenCalledWith('posts', 'post-123', { hydrate: ['author'] })
      })

      it('should return null for non-existent entity', async () => {
        mockService.get.mockResolvedValue(null)

        const result = await client.get('posts', 'non-existent')

        expect(result).toBeNull()
      })
    })

    describe('create', () => {
      it('should call service create with namespace and data', async () => {
        const data = { $type: 'Post', name: 'New Post' }

        await client.create('posts', data)

        expect(mockService.create).toHaveBeenCalledWith('posts', data, { actor: undefined })
      })

      it('should merge actor from client options', async () => {
        const clientWithActor = new ParqueDBClient(mockService, { actor: 'users/admin' })

        await clientWithActor.create('posts', { $type: 'Post', name: 'Test' })

        expect(mockService.create).toHaveBeenCalledWith(
          'posts',
          { $type: 'Post', name: 'Test' },
          { actor: 'users/admin' }
        )
      })

      it('should allow overriding actor in call options', async () => {
        const clientWithActor = new ParqueDBClient(mockService, { actor: 'users/admin' })

        await clientWithActor.create(
          'posts',
          { $type: 'Post', name: 'Test' },
          { actor: 'users/editor' }
        )

        expect(mockService.create).toHaveBeenCalledWith(
          'posts',
          { $type: 'Post', name: 'Test' },
          { actor: 'users/editor' }
        )
      })
    })

    describe('update', () => {
      it('should call service update with namespace, id, and update', async () => {
        await client.update('posts', 'post-123', { $set: { title: 'Updated' } })

        expect(mockService.update).toHaveBeenCalledWith(
          'posts',
          'post-123',
          { $set: { title: 'Updated' } },
          { actor: undefined }
        )
      })

      it('should merge actor from client options', async () => {
        const clientWithActor = new ParqueDBClient(mockService, { actor: 'users/admin' })

        await clientWithActor.update('posts', 'post-123', { $set: { title: 'Updated' } })

        expect(mockService.update).toHaveBeenCalledWith(
          'posts',
          'post-123',
          { $set: { title: 'Updated' } },
          { actor: 'users/admin' }
        )
      })
    })

    describe('delete', () => {
      it('should call service delete with namespace and id', async () => {
        await client.delete('posts', 'post-123')

        expect(mockService.delete).toHaveBeenCalledWith(
          'posts',
          'post-123',
          { actor: undefined }
        )
      })

      it('should pass options to service delete', async () => {
        await client.delete('posts', 'post-123', { hard: true, actor: 'users/admin' })

        expect(mockService.delete).toHaveBeenCalledWith(
          'posts',
          'post-123',
          { hard: true, actor: 'users/admin' }
        )
      })
    })

    describe('link', () => {
      it('should call service link with from, predicate, and to', async () => {
        await client.link('posts/1', 'author', 'users/1')

        expect(mockService.link).toHaveBeenCalledWith('posts/1', 'author', 'users/1', { data: undefined })
      })

      it('should pass data to service link', async () => {
        await client.link('posts/1', 'author', 'users/1', { role: 'primary' })

        expect(mockService.link).toHaveBeenCalledWith('posts/1', 'author', 'users/1', { data: { role: 'primary' } })
      })
    })

    describe('unlink', () => {
      it('should call service unlink with from, predicate, and to', async () => {
        await client.unlink('posts/1', 'author', 'users/1')

        expect(mockService.unlink).toHaveBeenCalledWith('posts/1', 'author', 'users/1')
      })
    })
  })

  // ===========================================================================
  // Admin Operations
  // ===========================================================================

  describe('Admin Operations', () => {
    describe('flush', () => {
      it('should call service flush', async () => {
        await client.flush()

        expect(mockService.flush).toHaveBeenCalledWith(undefined)
      })

      it('should call service flush with namespace', async () => {
        await client.flush('posts')

        expect(mockService.flush).toHaveBeenCalledWith('posts')
      })
    })

    describe('getFlushStatus', () => {
      it('should call service getFlushStatus', async () => {
        mockService.getFlushStatus.mockResolvedValue({ unflushedCount: 5 })

        const result = await client.getFlushStatus()

        expect(mockService.getFlushStatus).toHaveBeenCalledWith(undefined)
        expect(result).toEqual({ unflushedCount: 5 })
      })

      it('should call service getFlushStatus with namespace', async () => {
        await client.getFlushStatus('posts')

        expect(mockService.getFlushStatus).toHaveBeenCalledWith('posts')
      })
    })
  })

  // ===========================================================================
  // Collection Method Proxying
  // ===========================================================================

  describe('Collection Method Proxying', () => {
    it('should proxy find through collection', async () => {
      mockService.find.mockResolvedValue({ items: [], hasMore: false })

      const posts = client.collection('posts')
      await posts.find({ status: 'published' })

      expect(mockService.find).toHaveBeenCalledWith('posts', { status: 'published' }, undefined)
    })

    it('should proxy findOne through collection', async () => {
      mockService.find.mockResolvedValue({
        items: [{ $id: 'posts/1', name: 'Test' }],
        hasMore: false,
      })

      const posts = client.collection('posts')
      const result = await posts.findOne({ status: 'published' })

      expect(mockService.find).toHaveBeenCalled()
      expect(result).toEqual({ $id: 'posts/1', name: 'Test' })
    })

    it('should return null from findOne when no results', async () => {
      mockService.find.mockResolvedValue({ items: [], hasMore: false })

      const posts = client.collection('posts')
      const result = await posts.findOne({ status: 'published' })

      expect(result).toBeNull()
    })

    it('should proxy get through collection', async () => {
      mockService.get.mockResolvedValue({ $id: 'posts/1', name: 'Test' })

      const posts = client.collection('posts')
      await posts.get('post-123')

      expect(mockService.get).toHaveBeenCalledWith('posts', 'post-123', undefined)
    })

    it('should proxy create through collection', async () => {
      const posts = client.collection('posts')
      await posts.create({ $type: 'Post', name: 'New Post' })

      expect(mockService.create).toHaveBeenCalledWith(
        'posts',
        { $type: 'Post', name: 'New Post' },
        { actor: undefined }
      )
    })

    it('should proxy createMany through collection', async () => {
      mockService.createMany.mockResolvedValue([
        { $id: 'posts/1', name: 'Post 1' },
        { $id: 'posts/2', name: 'Post 2' },
      ])

      const posts = client.collection('posts')
      const result = await posts.createMany([
        { $type: 'Post', name: 'Post 1' },
        { $type: 'Post', name: 'Post 2' },
      ])

      expect(mockService.createMany).toHaveBeenCalled()
      expect(result).toHaveLength(2)
    })

    it('should proxy update through collection', async () => {
      const posts = client.collection('posts')
      await posts.update('post-123', { $set: { title: 'Updated' } })

      expect(mockService.update).toHaveBeenCalledWith(
        'posts',
        'post-123',
        { $set: { title: 'Updated' } },
        { actor: undefined }
      )
    })

    it('should proxy delete through collection', async () => {
      const posts = client.collection('posts')
      await posts.delete('post-123')

      expect(mockService.delete).toHaveBeenCalledWith(
        'posts',
        'post-123',
        { actor: undefined }
      )
    })

    it('should proxy deleteMany through collection', async () => {
      mockService.deleteMany.mockResolvedValue({ deletedCount: 3 })

      const posts = client.collection('posts')
      const result = await posts.deleteMany({ status: 'archived' })

      expect(mockService.deleteMany).toHaveBeenCalledWith(
        'posts',
        { status: 'archived' },
        { actor: undefined }
      )
      expect(result).toEqual({ deletedCount: 3 })
    })

    it('should proxy count through collection', async () => {
      mockService.count.mockResolvedValue(42)

      const posts = client.collection('posts')
      const result = await posts.count({ status: 'published' })

      expect(mockService.count).toHaveBeenCalledWith('posts', { status: 'published' })
      expect(result).toBe(42)
    })

    it('should proxy exists through collection', async () => {
      mockService.exists.mockResolvedValue(true)

      const posts = client.collection('posts')
      const result = await posts.exists('post-123')

      expect(mockService.exists).toHaveBeenCalledWith('posts', 'post-123')
      expect(result).toBe(true)
    })

    it('should proxy getRelationships through collection', async () => {
      mockService.getRelationships.mockResolvedValue([
        { from: 'posts/1', predicate: 'author', to: 'users/1' },
      ])

      const posts = client.collection('posts')
      const result = await posts.getRelationships('post-123', 'author', 'outbound')

      expect(mockService.getRelationships).toHaveBeenCalledWith('posts', 'post-123', 'author', 'outbound')
      expect(result).toHaveLength(1)
    })

    it('should proxy link through collection', async () => {
      const posts = client.collection('posts')
      await posts.link('post-123', 'author', 'users/1')

      expect(mockService.link).toHaveBeenCalledWith('posts/post-123', 'author', 'users/1')
    })

    it('should proxy unlink through collection', async () => {
      const posts = client.collection('posts')
      await posts.unlink('post-123', 'author', 'users/1')

      expect(mockService.unlink).toHaveBeenCalledWith('posts/post-123', 'author', 'users/1')
    })
  })
})

// =============================================================================
// createParqueDBClient Factory Tests
// =============================================================================

describe('createParqueDBClient', () => {
  it('should create client from service', () => {
    const mockService = createMockService()
    const client = createParqueDBClient(mockService)
    expect(client).toBeInstanceOf(ParqueDBClient)
  })

  it('should accept options', () => {
    const mockService = createMockService()
    const client = createParqueDBClient(mockService, { actor: 'users/admin' })
    expect(client).toBeInstanceOf(ParqueDBClient)
  })

  it('should pass options to ParqueDBClient', async () => {
    const mockService = createMockService()
    const client = createParqueDBClient(mockService, { actor: 'users/admin' })

    await client.create('posts', { $type: 'Post', name: 'Test' })

    expect(mockService.create).toHaveBeenCalledWith(
      'posts',
      { $type: 'Post', name: 'Test' },
      { actor: 'users/admin' }
    )
  })
})

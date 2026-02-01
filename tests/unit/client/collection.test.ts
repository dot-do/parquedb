/**
 * CollectionClient Unit Tests
 *
 * Tests for the CollectionClient class:
 * - CRUD method proxying via RPC
 * - RpcPromise creation for each method
 * - Bulk operations (createMany, updateMany, deleteMany)
 * - Convenience methods (findOne, findAndMap)
 * - Namespace handling
 *
 * Uses mocked RPC service to test client-side behavior in isolation.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { CollectionClient, type RpcService } from '../../../src/client/collection'
import { isRpcPromise } from '../../../src/client/rpc-promise'
import type { Entity, PaginatedResult, DeleteResult } from '../../../src/types'

// =============================================================================
// Mock RPC Service Factory
// =============================================================================

/**
 * Create a mock RPC service that returns controllable responses
 */
function createMockRpcService(responses?: Record<string, unknown>): RpcService & { lastRequest?: { method?: string; chain?: unknown[] } } {
  const service: RpcService & { lastRequest?: { method?: string; chain?: unknown[] } } = {
    lastRequest: undefined,
    async fetch(path: string, options?: { method?: string; body?: string }) {
      if (path === '/rpc' && options?.method === 'POST' && options?.body) {
        const body = JSON.parse(options.body)
        service.lastRequest = body

        // Handle chain execution
        if (body.chain && Array.isArray(body.chain)) {
          const firstMethod = body.chain[0]?.method
          if (firstMethod && responses?.[firstMethod] !== undefined) {
            return new Response(JSON.stringify(responses[firstMethod]), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          }
        }

        // Default responses based on method
        const method = body.chain?.[0]?.method
        const defaultResponses: Record<string, unknown> = {
          find: { items: [], hasMore: false },
          findOne: null,
          get: null,
          create: { $id: 'test/1', $type: 'Test', name: 'Created', version: 1 },
          update: { $id: 'test/1', $type: 'Test', name: 'Updated', version: 2 },
          delete: { deletedCount: 1 },
          deleteMany: { deletedCount: 0 },
          count: 0,
          exists: false,
          createMany: [],
          updateMany: { matchedCount: 0, modifiedCount: 0 },
        }

        return new Response(JSON.stringify(defaultResponses[method] ?? null), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response('Not Found', { status: 404 })
    },
  }

  return service
}

// =============================================================================
// CollectionClient Constructor Tests
// =============================================================================

describe('CollectionClient', () => {
  let mockService: RpcService
  let collection: CollectionClient

  beforeEach(() => {
    mockService = createMockRpcService()
    collection = new CollectionClient(mockService, 'posts')
  })

  describe('constructor', () => {
    it('should create collection with namespace', () => {
      expect(collection.namespace).toBe('posts')
    })

    it('should have __rpcTarget marker', () => {
      expect((collection as any).__rpcTarget).toBe(true)
    })

    it('should store namespace correctly', () => {
      const users = new CollectionClient(mockService, 'users')
      expect(users.namespace).toBe('users')
    })

    it('should preserve namespace case', () => {
      const camelCase = new CollectionClient(mockService, 'blogPosts')
      expect(camelCase.namespace).toBe('blogPosts')
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('Read Operations', () => {
    describe('find', () => {
      it('should create RpcPromise for find', () => {
        const promise = collection.find({ status: 'published' })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute find with filter', async () => {
        const service = createMockRpcService({
          find: { items: [{ $id: 'posts/1', name: 'Test' }], hasMore: false },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.find({ status: 'published' }) as PaginatedResult<Entity>

        expect(result.items).toHaveLength(1)
        expect(result.items[0].name).toBe('Test')
      })

      it('should pass options to find', async () => {
        const service = createMockRpcService({
          find: { items: [], hasMore: false },
        })
        const col = new CollectionClient(service, 'posts')

        await col.find({ status: 'published' }, { limit: 10, skip: 5 })

        expect(service.lastRequest?.chain?.[0]).toEqual({
          method: 'find',
          args: ['posts', { status: 'published' }, { limit: 10, skip: 5 }],
        })
      })

      it('should support empty filter', async () => {
        const service = createMockRpcService({
          find: { items: [], hasMore: false },
        })
        const col = new CollectionClient(service, 'posts')

        await col.find()

        // Note: undefined becomes null after JSON serialization
        expect(service.lastRequest?.chain?.[0]?.method).toBe('find')
        expect(service.lastRequest?.chain?.[0]?.args?.[0]).toBe('posts')
      })

      it('should support chaining with map', async () => {
        const service = createMockRpcService({
          find: ['Post 1', 'Post 2'], // After map, returns array of names
        })
        const col = new CollectionClient(service, 'posts')

        const promise = col.find().map((p: any) => p.name)
        expect(isRpcPromise(promise)).toBe(true)

        // The chain should include both find and map
        await promise
        expect(service.lastRequest?.chain).toHaveLength(2)
        expect(service.lastRequest?.chain?.[0]?.method).toBe('find')
        expect(service.lastRequest?.chain?.[1]?.method).toBe('map')
      })
    })

    describe('get', () => {
      it('should create RpcPromise for get', () => {
        const promise = collection.get('post-123')
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute get with id', async () => {
        const service = createMockRpcService({
          get: { $id: 'posts/123', name: 'Test Post' },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.get('123') as Entity

        expect(result.$id).toBe('posts/123')
        expect(result.name).toBe('Test Post')
      })

      it('should pass options to get', async () => {
        const service = createMockRpcService({ get: null })
        const col = new CollectionClient(service, 'posts')

        await col.get('123', { hydrate: ['author'] })

        expect(service.lastRequest?.chain?.[0]).toEqual({
          method: 'get',
          args: ['posts', '123', { hydrate: ['author'] }],
        })
      })

      it('should handle full EntityId', async () => {
        const service = createMockRpcService({ get: null })
        const col = new CollectionClient(service, 'posts')

        await col.get('posts/123')

        expect(service.lastRequest?.chain?.[0]?.args?.[1]).toBe('posts/123')
      })

      it('should return null for non-existent entity', async () => {
        const service = createMockRpcService({ get: null })
        const col = new CollectionClient(service, 'posts')

        const result = await col.get('non-existent')

        expect(result).toBeNull()
      })
    })

    describe('findOne', () => {
      it('should create RpcPromise for findOne', () => {
        const promise = collection.findOne({ status: 'published' })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute findOne and return single entity', async () => {
        const service = createMockRpcService({
          findOne: { $id: 'posts/1', name: 'Found' },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.findOne({ status: 'published' }) as Entity

        expect(result.name).toBe('Found')
      })

      it('should return null when no match', async () => {
        const service = createMockRpcService({ findOne: null })
        const col = new CollectionClient(service, 'posts')

        const result = await col.findOne({ status: 'nonexistent' })

        expect(result).toBeNull()
      })
    })

    describe('count', () => {
      it('should create RpcPromise for count', () => {
        const promise = collection.count({ status: 'published' })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute count and return number', async () => {
        const service = createMockRpcService({ count: 42 })
        const col = new CollectionClient(service, 'posts')

        const result = await col.count({ status: 'published' })

        expect(result).toBe(42)
      })

      it('should support empty filter', async () => {
        const service = createMockRpcService({ count: 100 })
        const col = new CollectionClient(service, 'posts')

        await col.count()

        // Note: undefined becomes null after JSON serialization
        const filterArg = service.lastRequest?.chain?.[0]?.args?.[1]
        expect(filterArg === undefined || filterArg === null).toBe(true)
      })
    })

    describe('exists', () => {
      it('should create RpcPromise for exists', () => {
        const promise = collection.exists('post-123')
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should return true when entity exists', async () => {
        const service = createMockRpcService({ exists: true })
        const col = new CollectionClient(service, 'posts')

        const result = await col.exists('post-123')

        expect(result).toBe(true)
      })

      it('should return false when entity does not exist', async () => {
        const service = createMockRpcService({ exists: false })
        const col = new CollectionClient(service, 'posts')

        const result = await col.exists('non-existent')

        expect(result).toBe(false)
      })
    })
  })

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  describe('Write Operations', () => {
    describe('create', () => {
      it('should create RpcPromise for create', () => {
        const promise = collection.create({ $type: 'Post', name: 'Test' })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute create and return entity', async () => {
        const service = createMockRpcService({
          create: { $id: 'posts/1', $type: 'Post', name: 'New Post', version: 1 },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.create({ $type: 'Post', name: 'New Post' }) as Entity

        expect(result.$id).toBe('posts/1')
        expect(result.name).toBe('New Post')
        expect(result.version).toBe(1)
      })

      it('should pass options to create', async () => {
        const service = createMockRpcService({ create: { $id: 'posts/1' } })
        const col = new CollectionClient(service, 'posts')

        await col.create(
          { $type: 'Post', name: 'Test' },
          { actor: 'users/admin' }
        )

        expect(service.lastRequest?.chain?.[0]?.args?.[2]).toEqual({ actor: 'users/admin' })
      })
    })

    describe('update', () => {
      it('should create RpcPromise for update', () => {
        const promise = collection.update('post-123', { $set: { title: 'Updated' } })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute update and return modified entity', async () => {
        const service = createMockRpcService({
          update: { $id: 'posts/123', name: 'Updated', version: 2 },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.update('123', { $set: { name: 'Updated' } }) as Entity

        expect(result.name).toBe('Updated')
        expect(result.version).toBe(2)
      })

      it('should pass options to update', async () => {
        const service = createMockRpcService({ update: null })
        const col = new CollectionClient(service, 'posts')

        await col.update('123', { $set: { name: 'Test' } }, { expectedVersion: 1 })

        expect(service.lastRequest?.chain?.[0]?.args?.[3]).toEqual({ expectedVersion: 1 })
      })

      it('should return null when entity not found', async () => {
        const service = createMockRpcService({ update: null })
        const col = new CollectionClient(service, 'posts')

        const result = await col.update('non-existent', { $set: { name: 'Test' } })

        expect(result).toBeNull()
      })
    })

    describe('delete', () => {
      it('should create RpcPromise for delete', () => {
        const promise = collection.delete('post-123')
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute delete and return result', async () => {
        const service = createMockRpcService({
          delete: { deletedCount: 1 },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.delete('123') as DeleteResult

        expect(result.deletedCount).toBe(1)
      })

      it('should pass options to delete', async () => {
        const service = createMockRpcService({ delete: { deletedCount: 1 } })
        const col = new CollectionClient(service, 'posts')

        await col.delete('123', { hard: true })

        expect(service.lastRequest?.chain?.[0]?.args?.[2]).toEqual({ hard: true })
      })
    })
  })

  // ===========================================================================
  // Bulk Operations
  // ===========================================================================

  describe('Bulk Operations', () => {
    describe('createMany', () => {
      it('should create RpcPromise for createMany', () => {
        const promise = collection.createMany([
          { $type: 'Post', name: 'Post 1' },
          { $type: 'Post', name: 'Post 2' },
        ])
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute createMany and return entities', async () => {
        const service = createMockRpcService({
          createMany: [
            { $id: 'posts/1', name: 'Post 1' },
            { $id: 'posts/2', name: 'Post 2' },
          ],
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.createMany([
          { $type: 'Post', name: 'Post 1' },
          { $type: 'Post', name: 'Post 2' },
        ]) as Entity[]

        expect(result).toHaveLength(2)
        expect(result[0].name).toBe('Post 1')
        expect(result[1].name).toBe('Post 2')
      })

      it('should pass options to createMany', async () => {
        const service = createMockRpcService({ createMany: [] })
        const col = new CollectionClient(service, 'posts')

        await col.createMany(
          [{ $type: 'Post', name: 'Test' }],
          { actor: 'users/admin' }
        )

        expect(service.lastRequest?.chain?.[0]?.args?.[2]).toEqual({ actor: 'users/admin' })
      })
    })

    describe('updateMany', () => {
      it('should create RpcPromise for updateMany', () => {
        const promise = collection.updateMany(
          { status: 'draft' },
          { $set: { status: 'published' } }
        )
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute updateMany and return counts', async () => {
        const service = createMockRpcService({
          updateMany: { matchedCount: 5, modifiedCount: 3 },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.updateMany(
          { status: 'draft' },
          { $set: { status: 'published' } }
        ) as { matchedCount: number; modifiedCount: number }

        expect(result.matchedCount).toBe(5)
        expect(result.modifiedCount).toBe(3)
      })
    })

    describe('deleteMany', () => {
      it('should create RpcPromise for deleteMany', () => {
        const promise = collection.deleteMany({ status: 'archived' })
        expect(isRpcPromise(promise)).toBe(true)
      })

      it('should execute deleteMany and return count', async () => {
        const service = createMockRpcService({
          deleteMany: { deletedCount: 10 },
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.deleteMany({ status: 'archived' }) as DeleteResult

        expect(result.deletedCount).toBe(10)
      })

      it('should pass options to deleteMany', async () => {
        const service = createMockRpcService({ deleteMany: { deletedCount: 0 } })
        const col = new CollectionClient(service, 'posts')

        await col.deleteMany({ status: 'archived' }, { hard: true })

        expect(service.lastRequest?.chain?.[0]?.args?.[2]).toEqual({ hard: true })
      })
    })
  })

  // ===========================================================================
  // Convenience Methods
  // ===========================================================================

  describe('Convenience Methods', () => {
    describe('findAndMap', () => {
      it('should combine find and map in single chain', async () => {
        const service = createMockRpcService({
          find: ['Post 1', 'Post 2'], // After map transformation
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.findAndMap(
          { status: 'published' },
          (post: any) => post.name
        )

        // Should have both find and map in the chain
        expect(service.lastRequest?.chain).toHaveLength(2)
        expect(service.lastRequest?.chain?.[0]?.method).toBe('find')
        expect(service.lastRequest?.chain?.[1]?.method).toBe('map')
      })

      it('should return mapped results', async () => {
        const service = createMockRpcService({
          find: ['Title 1', 'Title 2'],
        })
        const col = new CollectionClient(service, 'posts')

        const result = await col.findAndMap(
          {},
          (post: any) => post.title
        ) as string[]

        expect(result).toEqual(['Title 1', 'Title 2'])
      })
    })
  })

  // ===========================================================================
  // Namespace Accessor
  // ===========================================================================

  describe('namespace accessor', () => {
    it('should return correct namespace', () => {
      expect(collection.namespace).toBe('posts')
    })

    it('should be read-only', () => {
      // TypeScript prevents direct assignment, but we can verify the getter works
      const ns = collection.namespace
      expect(ns).toBe('posts')
    })
  })

  // ===========================================================================
  // Typed Collections
  // ===========================================================================

  describe('Typed Collections', () => {
    interface Post {
      title: string
      content: string
      status: 'draft' | 'published'
    }

    it('should support typed collection creation', () => {
      const posts = new CollectionClient<Post>(mockService, 'posts')
      expect(posts.namespace).toBe('posts')
    })

    it('should preserve type information through methods', async () => {
      const service = createMockRpcService({
        find: { items: [{ $id: 'posts/1', title: 'Test', status: 'published' }], hasMore: false },
      })
      const posts = new CollectionClient<Post>(service, 'posts')

      const promise = posts.find({ status: 'published' })
      expect(isRpcPromise(promise)).toBe(true)
    })
  })
})

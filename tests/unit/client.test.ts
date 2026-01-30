/**
 * ParqueDB Client Tests
 *
 * Tests for the RPC client implementation using capnweb patterns.
 * Tests the ParqueDBClient, CollectionClient, RpcPromise, and ServiceBindingAdapter.
 *
 * Uses REAL HTTP/RPC communication - no mocks.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  ParqueDBClient,
  createParqueDBClient,
  type ParqueDBService,
  type Collection,
} from '../../src/client/ParqueDBClient'
import {
  createRpcPromise,
  isRpcPromise,
  batchRpc,
  resolvedRpcPromise,
  RpcError,
  deserializeFunction,
  type RpcPromiseChain,
} from '../../src/client/rpc-promise'
import { CollectionClient, type RpcService } from '../../src/client/collection'
import {
  ServiceBindingAdapter,
  createServiceAdapter,
  isServiceBinding,
} from '../../src/client/service-binding'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { Entity, PaginatedResult, DeleteResult } from '../../src/types'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a real ParqueDB instance with in-memory storage
 */
function createTestDB(): ParqueDB {
  const storage = new MemoryBackend()
  return new ParqueDB({ storage })
}

/**
 * Create a real RPC service that wraps a ParqueDB instance
 * This simulates the service binding pattern used in production
 */
function createRealService(db: ParqueDB): ParqueDBService {
  return {
    async find(ns: string, filter?: Record<string, unknown>, options?: Record<string, unknown>) {
      return db.find(ns, filter, options)
    },
    async get(ns: string, id: string, options?: Record<string, unknown>) {
      return db.get(ns, id, options)
    },
    async create(ns: string, data: Record<string, unknown>, options?: Record<string, unknown>) {
      return db.create(ns, data, options)
    },
    async createMany(ns: string, items: Record<string, unknown>[], options?: Record<string, unknown>) {
      const results: Entity[] = []
      for (const item of items) {
        const entity = await db.create(ns, item, options)
        results.push(entity)
      }
      return results
    },
    async update(ns: string, id: string, update: Record<string, unknown>, options?: Record<string, unknown>) {
      return db.update(ns, id, update, options)
    },
    async delete(ns: string, id: string, options?: Record<string, unknown>) {
      return db.delete(ns, id, options)
    },
    async deleteMany(ns: string, filter: Record<string, unknown>, options?: Record<string, unknown>) {
      return db.deleteMany(ns, filter, options)
    },
    async count(ns: string, filter?: Record<string, unknown>) {
      // Implement count using find - this matches how service workers implement it
      const result = await db.find(ns, filter)
      return result.items.length
    },
    async exists(ns: string, id: string) {
      // Implement exists using get - this matches how service workers implement it
      const fullId = id.includes('/') ? id : `${ns}/${id}`
      const entity = await db.get(ns, fullId)
      return entity !== null
    },
    async getRelationships(ns: string, id: string, predicate?: string, direction?: 'outbound' | 'inbound') {
      return db.getRelationships(`${ns}/${id}`, predicate, direction)
    },
    async link(fromId: string, predicate: string, toId: string, options?: Record<string, unknown>) {
      return db.link(fromId, predicate, toId, options)
    },
    async unlink(fromId: string, predicate: string, toId: string) {
      return db.unlink(fromId, predicate, toId)
    },
    async flush(_ns?: string) {
      // No-op for memory backend
    },
    async getFlushStatus(_ns?: string) {
      return { unflushedCount: 0 }
    },
  } as unknown as ParqueDBService
}

/**
 * Create a real HTTP-style RPC service that exercises the full request/response cycle
 */
function createHttpRpcService(db: ParqueDB): RpcService {
  return {
    async fetch(path: string, options?: { method?: string; body?: string; headers?: Record<string, string> }): Promise<Response> {
      if (path === '/rpc' && options?.method === 'POST' && options?.body) {
        const body = JSON.parse(options.body)

        // Handle RPC chain execution
        if (body.chain && Array.isArray(body.chain)) {
          let result: unknown = undefined

          for (const step of body.chain) {
            if (step.method === 'find') {
              const [ns, filter, findOptions] = step.args
              const findResult = await db.find(ns, filter, findOptions)
              result = findResult
            } else if (step.method === 'get') {
              const [ns, id, getOptions] = step.args
              result = await db.get(ns, id, getOptions)
            } else if (step.method === 'create') {
              const [ns, data, createOptions] = step.args
              result = await db.create(ns, data, createOptions)
            } else if (step.method === 'update') {
              const [ns, id, update, updateOptions] = step.args
              result = await db.update(ns, id, update, updateOptions)
            } else if (step.method === 'delete') {
              const [ns, id, deleteOptions] = step.args
              result = await db.delete(ns, id, deleteOptions)
            } else if (step.method === 'count') {
              const [ns, filter] = step.args
              // Implement count using find - this matches how service workers implement it
              const findResult = await db.find(ns, filter)
              result = findResult.items.length
            } else if (step.method === 'exists') {
              const [ns, id] = step.args
              // Implement exists using get - this matches how service workers implement it
              const fullId = id.includes('/') ? id : `${ns}/${id}`
              const entity = await db.get(ns, fullId)
              result = entity !== null
            } else if (step.method === 'map') {
              // Deserialize and apply the mapping function
              const [fnSerialized] = step.args
              const fn = deserializeFunction(fnSerialized)
              if (result && typeof result === 'object' && 'items' in result) {
                // Map over PaginatedResult items
                const paginatedResult = result as PaginatedResult<Entity>
                result = paginatedResult.items.map(fn)
              } else if (Array.isArray(result)) {
                result = result.map(fn)
              } else {
                result = fn(result)
              }
            } else if (step.method === 'createMany') {
              const [ns, items, options] = step.args
              const results: Entity[] = []
              for (const item of items as Record<string, unknown>[]) {
                const entity = await db.create(ns, item, options)
                results.push(entity)
              }
              result = results
            } else if (step.method === 'updateMany') {
              const [ns, filter, update, options] = step.args
              // Simple implementation: find all matching and update them
              const found = await db.find(ns, filter)
              let modifiedCount = 0
              for (const entity of found.items) {
                const id = entity.$id.split('/')[1]
                await db.update(ns, id, update, options)
                modifiedCount++
              }
              result = { matchedCount: found.items.length, modifiedCount }
            } else if (step.method === 'deleteMany') {
              const [ns, filter, options] = step.args
              result = await db.deleteMany(ns, filter, options)
            } else if (step.method === 'findOne') {
              const [ns, filter, options] = step.args
              const found = await db.find(ns, filter, { ...options, limit: 1 })
              result = found.items[0] || null
            }
          }

          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          })
        }

        // Handle simple method calls
        if (body.method && body.args) {
          const { method, args } = body
          let result: unknown

          switch (method) {
            case 'find':
              result = await db.find(args[0], args[1], args[2])
              break
            case 'get':
              result = await db.get(args[0], args[1], args[2])
              break
            case 'create':
              result = await db.create(args[0], args[1], args[2])
              break
            case 'update':
              result = await db.update(args[0], args[1], args[2], args[3])
              break
            case 'delete':
              result = await db.delete(args[0], args[1], args[2])
              break
            default:
              return new Response(`Unknown method: ${method}`, { status: 400 })
          }

          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          })
        }
      }

      if (path === '/health') {
        return new Response(JSON.stringify({ ok: true, version: '1.0.0' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response('Not Found', { status: 404 })
    },
  }
}

/**
 * Create a service binding that wraps a ParqueDB instance
 * This simulates Cloudflare's Service Binding pattern
 */
function createServiceBinding(db: ParqueDB) {
  const httpService = createHttpRpcService(db)

  return {
    fetch: async (request: Request) => {
      const url = new URL(request.url)
      const path = url.pathname
      const options: { method?: string; body?: string; headers?: Record<string, string> } = {
        method: request.method,
      }

      if (request.body) {
        options.body = await request.text()
      }

      return httpService.fetch(path, options)
    },
    // Direct RPC methods for service binding optimization
    find: (ns: string, filter?: Record<string, unknown>, options?: Record<string, unknown>) =>
      db.find(ns, filter, options),
    get: (ns: string, id: string, options?: Record<string, unknown>) =>
      db.get(ns, id, options),
    create: (ns: string, data: Record<string, unknown>, options?: Record<string, unknown>) =>
      db.create(ns, data, options),
    update: (ns: string, id: string, update: Record<string, unknown>, options?: Record<string, unknown>) =>
      db.update(ns, id, update, options),
    delete: (ns: string, id: string, options?: Record<string, unknown>) =>
      db.delete(ns, id, options),
  }
}

// =============================================================================
// ParqueDBClient Tests
// =============================================================================

describe('ParqueDBClient', () => {
  let db: ParqueDB
  let service: ParqueDBService
  let client: ParqueDBClient

  beforeEach(() => {
    db = createTestDB()
    service = createRealService(db)
    client = new ParqueDBClient(service)
  })

  describe('Constructor', () => {
    it('should create client with service stub', () => {
      expect(client).toBeInstanceOf(ParqueDBClient)
    })

    it('should accept options', () => {
      const clientWithOptions = new ParqueDBClient(service, {
        actor: 'users/admin',
        timeout: 5000,
      })
      expect(clientWithOptions).toBeInstanceOf(ParqueDBClient)
    })
  })

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

    it('should normalize PascalCase to lowercase', () => {
      const blogPosts = (client as any).BlogPosts as Collection
      expect(blogPosts.namespace).toBe('blogPosts')
    })

    it('should cache collection instances', () => {
      const posts1 = (client as any).Posts as Collection
      const posts2 = (client as any).Posts as Collection
      expect(posts1).toBe(posts2)
    })
  })

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
      }
      const posts = client.collection<Post>('posts')
      expect(posts.namespace).toBe('posts')
    })

    it('should cache collections', () => {
      const posts1 = client.collection('posts')
      const posts2 = client.collection('posts')
      expect(posts1).toBe(posts2)
    })
  })

  describe('Direct Methods with Real Data', () => {
    it('should call find on service and return real results', async () => {
      // Create some test data
      await db.create('posts', { $type: 'Post', name: 'Test Post', status: 'published' })

      const result = await client.find('posts', { status: 'published' })

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Test Post')
      expect(result.hasMore).toBe(false)
    })

    it('should call get on service and return real entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Get Test' })
      const id = created.$id.split('/')[1]

      const result = await client.get('posts', id)

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Get Test')
    })

    it('should call create on service and persist entity', async () => {
      const data = { $type: 'Post', name: 'Test Post' }

      const entity = await client.create('posts', data)

      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.$type).toBe('Post')
      expect(entity.name).toBe('Test Post')
      expect(entity.version).toBe(1)

      // Verify persisted
      const fetched = await db.get('posts', entity.$id)
      expect(fetched).not.toBeNull()
      expect(fetched!.name).toBe('Test Post')
    })

    it('should call update on service and modify entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Original' })
      const id = created.$id.split('/')[1]

      const updated = await client.update('posts', id, { $set: { name: 'Updated' } })

      expect(updated.name).toBe('Updated')
      expect(updated.version).toBe(2)

      // Verify persisted
      const fetched = await db.get('posts', created.$id)
      expect(fetched!.name).toBe('Updated')
    })

    it('should call delete on service and remove entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'To Delete' })
      const id = created.$id.split('/')[1]

      const result = await client.delete('posts', id)

      expect(result.deletedCount).toBe(1)

      // Verify soft deleted
      const fetched = await db.get('posts', created.$id)
      expect(fetched).toBeNull()
    })
  })

  describe('Collection Methods with Real Data', () => {
    it('should call find through collection', async () => {
      await db.create('posts', { $type: 'Post', name: 'Collection Find', status: 'published' })

      const posts = client.collection('posts')
      const result = await posts.find({ status: 'published' })

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Collection Find')
    })

    it('should call findOne through collection', async () => {
      await db.create('posts', { $type: 'Post', name: 'FindOne Test', status: 'published' })

      const posts = client.collection('posts')
      const result = await posts.findOne({ status: 'published' })

      expect(result).not.toBeNull()
      expect(result!.name).toBe('FindOne Test')
    })

    it('should call get through collection', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Get Through Collection' })
      const id = created.$id.split('/')[1]

      const posts = client.collection('posts')
      const result = await posts.get(id)

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Get Through Collection')
    })

    it('should call create through collection', async () => {
      const posts = client.collection('posts')
      const entity = await posts.create({ $type: 'Post', name: 'Created Through Collection' })

      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.name).toBe('Created Through Collection')
    })

    it('should call update through collection', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Update Through Collection' })
      const id = created.$id.split('/')[1]

      const posts = client.collection('posts')
      const updated = await posts.update(id, { $set: { name: 'Updated Name' } })

      expect(updated.name).toBe('Updated Name')
    })

    it('should call delete through collection', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Delete Through Collection' })
      const id = created.$id.split('/')[1]

      const posts = client.collection('posts')
      const result = await posts.delete(id)

      expect(result.deletedCount).toBe(1)
    })

    it('should call count through collection', async () => {
      await db.create('posts', { $type: 'Post', name: 'Count 1', status: 'published' })
      await db.create('posts', { $type: 'Post', name: 'Count 2', status: 'published' })
      await db.create('posts', { $type: 'Post', name: 'Count 3', status: 'draft' })

      const posts = client.collection('posts')
      const count = await posts.count({ status: 'published' })

      expect(count).toBe(2)
    })

    it('should call exists through collection', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Exists Test' })
      const id = created.$id.split('/')[1]

      const posts = client.collection('posts')

      expect(await posts.exists(id)).toBe(true)
      expect(await posts.exists('nonexistent')).toBe(false)
    })
  })

  describe('Actor Option', () => {
    it('should pass actor from options to create', async () => {
      const clientWithActor = new ParqueDBClient(service, { actor: 'users/admin' })
      const entity = await clientWithActor.create('posts', { $type: 'Post', name: 'Test' })

      expect(entity.createdBy).toBe('users/admin')
    })

    it('should allow overriding actor in call options', async () => {
      const clientWithActor = new ParqueDBClient(service, { actor: 'users/admin' })
      const entity = await clientWithActor.create(
        'posts',
        { $type: 'Post', name: 'Test' },
        { actor: 'users/editor' }
      )

      expect(entity.createdBy).toBe('users/editor')
    })
  })
})

// =============================================================================
// createParqueDBClient Tests
// =============================================================================

describe('createParqueDBClient', () => {
  it('should create client from service', () => {
    const db = createTestDB()
    const service = createRealService(db)
    const client = createParqueDBClient(service)
    expect(client).toBeInstanceOf(ParqueDBClient)
  })

  it('should accept options', () => {
    const db = createTestDB()
    const service = createRealService(db)
    const client = createParqueDBClient(service, { actor: 'users/admin' })
    expect(client).toBeInstanceOf(ParqueDBClient)
  })
})

// =============================================================================
// RpcPromise Tests
// =============================================================================

describe('RpcPromise', () => {
  let db: ParqueDB
  let httpService: RpcService

  beforeEach(() => {
    db = createTestDB()
    httpService = createHttpRpcService(db)
  })

  describe('createRpcPromise', () => {
    it('should create a thenable promise', () => {
      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
      expect(promise).toHaveProperty('then')
      expect(typeof promise.then).toBe('function')
    })

    it('should have __rpcPromise marker', () => {
      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
      expect((promise as any).__rpcPromise).toBe(true)
    })

    it('should have map method', () => {
      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
      expect(typeof (promise as any).map).toBe('function')
    })

    it('should execute chain when awaited', async () => {
      // Create test data
      await db.create('posts', { $type: 'Post', name: 'Test Post', title: 'Test Title' })

      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
      const result = await promise as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Test Post')
    })

    it('should collect chain with map before executing', async () => {
      // Create test data
      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })

      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
        .map((p: any) => p.name)

      const result = await promise as string[]

      expect(result).toContain('Post 1')
      expect(result).toContain('Post 2')
    })

    it('should handle multiple map calls', async () => {
      // Create test data with nested structure
      await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        author: { name: 'Author 1' },
      })
      await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        author: { name: 'Author 2' },
      })

      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
        .map((p: any) => p.author)
        .map((a: any) => a.name)

      const result = await promise as string[]

      expect(result).toContain('Author 1')
      expect(result).toContain('Author 2')
    })

    it('should throw RpcError on non-ok response', async () => {
      // Create a service that returns errors
      const errorService: RpcService = {
        async fetch() {
          return new Response('Internal Server Error', { status: 500 })
        },
      }

      const promise = createRpcPromise(errorService, 'find', ['posts', {}])

      await expect(promise).rejects.toThrow(RpcError)
    })
  })

  describe('isRpcPromise', () => {
    it('should return true for RpcPromise', () => {
      const promise = createRpcPromise(httpService, 'find', ['posts', {}])
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should return false for regular Promise', () => {
      const promise = Promise.resolve()
      expect(isRpcPromise(promise)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isRpcPromise(null)).toBe(false)
    })

    it('should return false for non-object', () => {
      expect(isRpcPromise('string')).toBe(false)
      expect(isRpcPromise(123)).toBe(false)
    })
  })

  describe('batchRpc', () => {
    it('should execute multiple promises in parallel', async () => {
      // Create test data
      await db.create('posts', { $type: 'Post', name: 'Post 1' })
      await db.create('users', { $type: 'User', name: 'User 1', email: 'user1@test.com' })

      const promise1 = createRpcPromise(httpService, 'find', ['posts', {}])
      const promise2 = createRpcPromise(httpService, 'find', ['users', {}])

      const [result1, result2] = await batchRpc(promise1, promise2)

      expect((result1 as PaginatedResult<Entity>).items).toHaveLength(1)
      expect((result2 as PaginatedResult<Entity>).items).toHaveLength(1)
      expect((result1 as PaginatedResult<Entity>).items[0].name).toBe('Post 1')
      expect((result2 as PaginatedResult<Entity>).items[0].name).toBe('User 1')
    })
  })

  describe('resolvedRpcPromise', () => {
    it('should create a resolved RpcPromise', async () => {
      const promise = resolvedRpcPromise([1, 2, 3])
      expect(isRpcPromise(promise)).toBe(true)
      expect(await promise).toEqual([1, 2, 3])
    })

    it('should support map for arrays', async () => {
      const promise = resolvedRpcPromise([1, 2, 3])
      const mapped = promise.map((n: number) => n * 2)
      expect(await mapped).toEqual([2, 4, 6])
    })

    it('should support map for single values', async () => {
      const promise = resolvedRpcPromise({ name: 'test' })
      const mapped = promise.map((obj: { name: string }) => obj.name)
      expect(await mapped).toBe('test')
    })
  })

  describe('RpcError', () => {
    it('should include status and chain', () => {
      const chain: RpcPromiseChain = [{ method: 'find', args: ['posts'] }]
      const error = new RpcError('Test error', 500, chain)

      expect(error.message).toBe('Test error')
      expect(error.status).toBe(500)
      expect(error.chain).toEqual(chain)
      expect(error.name).toBe('RpcError')
    })
  })

  describe('deserializeFunction', () => {
    it('should deserialize arrow function', () => {
      const serialized = JSON.stringify({ type: 'sync', body: '(x) => x.name' })
      const fn = deserializeFunction<{ name: string }, string>(serialized)
      expect(fn({ name: 'test' })).toBe('test')
    })

    it('should deserialize arrow function without parens', () => {
      const serialized = JSON.stringify({ type: 'sync', body: 'x => x.value' })
      const fn = deserializeFunction<{ value: number }, number>(serialized)
      expect(fn({ value: 42 })).toBe(42)
    })

    it('should deserialize arrow function with block body', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => { const y = x * 2; return y + 1; }',
      })
      const fn = deserializeFunction<number, number>(serialized)
      expect(fn(5)).toBe(11)
    })

    it('should deserialize regular function', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: 'function(x) { return x + 1; }',
      })
      const fn = deserializeFunction<number, number>(serialized)
      expect(fn(5)).toBe(6)
    })

    it('should throw for invalid function', () => {
      const serialized = JSON.stringify({ type: 'sync', body: 'not a function' })
      expect(() => deserializeFunction(serialized)).toThrow()
    })
  })
})

// =============================================================================
// CollectionClient Tests
// =============================================================================

describe('CollectionClient', () => {
  let db: ParqueDB
  let httpService: RpcService
  let collection: CollectionClient

  beforeEach(() => {
    db = createTestDB()
    httpService = createHttpRpcService(db)
    collection = new CollectionClient(httpService, 'posts')
  })

  describe('Constructor', () => {
    it('should create collection with namespace', () => {
      expect(collection.namespace).toBe('posts')
    })

    it('should have __rpcTarget marker', () => {
      expect((collection as any).__rpcTarget).toBe(true)
    })
  })

  describe('Read Operations', () => {
    it('should create RpcPromise for find', () => {
      const promise = collection.find({ status: 'published' })
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute find and return results', async () => {
      await db.create('posts', { $type: 'Post', name: 'Test', status: 'published' })

      const result = await collection.find({ status: 'published' }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Test')
    })

    it('should create RpcPromise for get', () => {
      const promise = collection.get('post-123')
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute get and return entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Get Test' })
      const id = created.$id.split('/')[1]

      const result = await collection.get(id) as Entity | null

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Get Test')
    })

    it('should create RpcPromise for count', () => {
      const promise = collection.count({ status: 'published' })
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute count and return number', async () => {
      await db.create('posts', { $type: 'Post', name: 'Count 1', status: 'published' })
      await db.create('posts', { $type: 'Post', name: 'Count 2', status: 'draft' })

      const result = await collection.count({ status: 'published' }) as number

      expect(result).toBe(1)
    })

    it('should create RpcPromise for exists', () => {
      const promise = collection.exists('post-123')
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute exists and return boolean', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Exists Test' })
      const id = created.$id.split('/')[1]

      expect(await collection.exists(id)).toBe(true)
      expect(await collection.exists('nonexistent')).toBe(false)
    })
  })

  describe('Write Operations', () => {
    it('should create RpcPromise for create', () => {
      const promise = collection.create({ $type: 'Post', name: 'Test' })
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute create and persist entity', async () => {
      const result = await collection.create({ $type: 'Post', name: 'Create Test' }) as Entity

      expect(result.$id).toMatch(/^posts\//)
      expect(result.name).toBe('Create Test')

      // Verify persisted
      const fetched = await db.get('posts', result.$id)
      expect(fetched).not.toBeNull()
    })

    it('should create RpcPromise for update', () => {
      const promise = collection.update('post-123', { $set: { title: 'Updated' } })
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute update and modify entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Original' })
      const id = created.$id.split('/')[1]

      const result = await collection.update(id, { $set: { name: 'Updated' } }) as Entity | null

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Updated')
    })

    it('should create RpcPromise for delete', () => {
      const promise = collection.delete('post-123')
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should execute delete and remove entity', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'To Delete' })
      const id = created.$id.split('/')[1]

      const result = await collection.delete(id) as DeleteResult

      expect(result.deletedCount).toBe(1)
    })
  })

  describe('findAndMap', () => {
    it('should combine find and map in single chain', async () => {
      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })

      const result = await collection.findAndMap(
        {},
        (post: any) => post.name
      ) as string[]

      expect(result).toContain('Post 1')
      expect(result).toContain('Post 2')
    })
  })

  describe('Bulk Operations', () => {
    it('should create RpcPromise for createMany', () => {
      const promise = collection.createMany([
        { $type: 'Post', name: 'Test 1' },
        { $type: 'Post', name: 'Test 2' },
      ])
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should create RpcPromise for updateMany', () => {
      const promise = collection.updateMany(
        { status: 'draft' },
        { $set: { status: 'published' } }
      )
      expect(isRpcPromise(promise)).toBe(true)
    })

    it('should create RpcPromise for deleteMany', () => {
      const promise = collection.deleteMany({ status: 'archived' })
      expect(isRpcPromise(promise)).toBe(true)
    })
  })
})

// =============================================================================
// ServiceBindingAdapter Tests
// =============================================================================

describe('ServiceBindingAdapter', () => {
  let db: ParqueDB
  let binding: ReturnType<typeof createServiceBinding>
  let adapter: ServiceBindingAdapter

  beforeEach(() => {
    db = createTestDB()
    binding = createServiceBinding(db)
    adapter = new ServiceBindingAdapter(binding as any)
  })

  describe('fetch', () => {
    it('should forward requests to binding', async () => {
      const response = await adapter.fetch('/health')

      expect(response.ok).toBe(true)
      const body = await response.json()
      expect(body).toEqual({ ok: true, version: '1.0.0' })
    })
  })

  describe('call', () => {
    it('should use direct RPC if available', async () => {
      const result = await adapter.call('find', ['posts', {}])

      expect(result).toHaveProperty('items')
      expect(result).toHaveProperty('hasMore')
    })

    it('should execute find with real data', async () => {
      await db.create('posts', { $type: 'Post', name: 'Direct RPC Test' })

      const result = await adapter.call('find', ['posts', {}]) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Direct RPC Test')
    })
  })

  describe('executeChain', () => {
    it('should fall back to HTTP for chain execution', async () => {
      await db.create('posts', { $type: 'Post', name: 'Chain Test' })

      const chain = [{ method: 'find', args: ['posts', {}] }]
      const result = await adapter.executeChain(chain) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })
  })

  describe('Collection Methods', () => {
    it('should call find with real data', async () => {
      await db.create('posts', { $type: 'Post', name: 'Find Test', status: 'published' })

      const result = await adapter.find<PaginatedResult<Entity>>('posts', { status: 'published' })

      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Find Test')
    })

    it('should call get with real data', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Get Test' })
      const id = created.$id.split('/')[1]

      const result = await adapter.get<Entity>('posts', id)

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Get Test')
    })

    it('should call create and persist', async () => {
      const result = await adapter.create<Entity>('posts', { $type: 'Post', name: 'Create Test' })

      expect(result.$id).toMatch(/^posts\//)
      expect(result.name).toBe('Create Test')

      // Verify persisted
      const fetched = await db.get('posts', result.$id)
      expect(fetched).not.toBeNull()
    })

    it('should call update and modify', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'Original' })
      const id = created.$id.split('/')[1]

      const result = await adapter.update<Entity>('posts', id, { $set: { name: 'Updated' } })

      expect(result).not.toBeNull()
      expect(result!.name).toBe('Updated')
    })

    it('should call delete and remove', async () => {
      const created = await db.create('posts', { $type: 'Post', name: 'To Delete' })
      const id = created.$id.split('/')[1]

      const result = await adapter.delete('posts', id)

      expect(result.deletedCount).toBe(1)
    })
  })

  describe('health', () => {
    it('should return ok: true for successful response', async () => {
      const result = await adapter.health()

      expect(result).toEqual({ ok: true, version: '1.0.0' })
    })

    it('should return ok: false for error response', async () => {
      // Create an adapter with a failing binding
      const failingBinding = {
        fetch: async () => new Response('Error', { status: 500 }),
      }
      const failingAdapter = new ServiceBindingAdapter(failingBinding as any)

      const result = await failingAdapter.health()

      expect(result).toEqual({ ok: false })
    })

    it('should return ok: false for network error', async () => {
      // Create an adapter with a binding that throws
      const throwingBinding = {
        fetch: async () => {
          throw new Error('Network error')
        },
      }
      const throwingAdapter = new ServiceBindingAdapter(throwingBinding as any)

      const result = await throwingAdapter.health()

      expect(result).toEqual({ ok: false })
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createServiceAdapter', () => {
  it('should create adapter from binding', () => {
    const db = createTestDB()
    const binding = createServiceBinding(db)
    const adapter = createServiceAdapter(binding as any)
    expect(adapter).toBeInstanceOf(ServiceBindingAdapter)
  })
})

describe('isServiceBinding', () => {
  it('should return true for object with fetch', () => {
    expect(isServiceBinding({ fetch: () => {} })).toBe(true)
  })

  it('should return false for object without fetch', () => {
    expect(isServiceBinding({})).toBe(false)
  })

  it('should return false for null', () => {
    expect(isServiceBinding(null)).toBe(false)
  })

  it('should return false for non-object', () => {
    expect(isServiceBinding('string')).toBe(false)
  })
})

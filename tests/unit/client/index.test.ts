/**
 * Client Module Index Tests
 *
 * Tests for the client module exports:
 * - Verifies all expected exports are available
 * - Tests re-exports work correctly
 * - Ensures type helpers are exported
 */

import { describe, it, expect } from 'vitest'

// Import from the main client index
import {
  // Main client exports
  ParqueDBClient,
  createParqueDBClient,

  // RPC Promise exports
  createRpcPromise,
  isRpcPromise,
  batchRpc,
  resolvedRpcPromise,
  RpcError,

  // Service binding exports
  ServiceBindingAdapter,
  createServiceAdapter,
  isServiceBinding,

  // Collection client export
  CollectionClient,
} from '../../../src/client'

// Import types to verify they're exported
import type {
  ParqueDBService,
  Collection,
  ParqueDBClientOptions,
  TypedCollection,
  EntityOf,
  CreateInputOf,
  RpcPromiseChain,
  Service,
  CollectionCreateInput,
} from '../../../src/client'

// =============================================================================
// Main Client Exports
// =============================================================================

describe('Client Module Exports', () => {
  describe('ParqueDBClient exports', () => {
    it('should export ParqueDBClient class', () => {
      expect(ParqueDBClient).toBeDefined()
      expect(typeof ParqueDBClient).toBe('function')
    })

    it('should export createParqueDBClient function', () => {
      expect(createParqueDBClient).toBeDefined()
      expect(typeof createParqueDBClient).toBe('function')
    })
  })

  describe('RpcPromise exports', () => {
    it('should export createRpcPromise function', () => {
      expect(createRpcPromise).toBeDefined()
      expect(typeof createRpcPromise).toBe('function')
    })

    it('should export isRpcPromise function', () => {
      expect(isRpcPromise).toBeDefined()
      expect(typeof isRpcPromise).toBe('function')
    })

    it('should export batchRpc function', () => {
      expect(batchRpc).toBeDefined()
      expect(typeof batchRpc).toBe('function')
    })

    it('should export resolvedRpcPromise function', () => {
      expect(resolvedRpcPromise).toBeDefined()
      expect(typeof resolvedRpcPromise).toBe('function')
    })

    it('should export RpcError class', () => {
      expect(RpcError).toBeDefined()
      expect(typeof RpcError).toBe('function')

      // Verify it can be instantiated
      const error = new RpcError('test', 500, [])
      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(RpcError)
    })
  })

  describe('ServiceBinding exports', () => {
    it('should export ServiceBindingAdapter class', () => {
      expect(ServiceBindingAdapter).toBeDefined()
      expect(typeof ServiceBindingAdapter).toBe('function')
    })

    it('should export createServiceAdapter function', () => {
      expect(createServiceAdapter).toBeDefined()
      expect(typeof createServiceAdapter).toBe('function')
    })

    it('should export isServiceBinding function', () => {
      expect(isServiceBinding).toBeDefined()
      expect(typeof isServiceBinding).toBe('function')
    })
  })

  describe('CollectionClient export', () => {
    it('should export CollectionClient class', () => {
      expect(CollectionClient).toBeDefined()
      expect(typeof CollectionClient).toBe('function')
    })
  })
})

// =============================================================================
// Type Export Tests
// =============================================================================

describe('Type Exports', () => {
  // These tests verify that types are properly exported and usable
  // The actual type checking happens at compile time

  it('should allow using ParqueDBClientOptions type', () => {
    const options: ParqueDBClientOptions = {
      actor: 'users/admin',
      timeout: 5000,
      debug: true,
    }
    expect(options.actor).toBe('users/admin')
  })

  it('should allow using RpcPromiseChain type', () => {
    const chain: RpcPromiseChain = [
      { method: 'find', args: ['posts', {}] },
      { method: 'map', args: ['(x) => x.name'] },
    ]
    expect(chain).toHaveLength(2)
  })

  it('should allow using CollectionCreateInput type', () => {
    const input: CollectionCreateInput = {
      $type: 'Post',
      name: 'Test Post',
      customField: 'value',
    }
    expect(input.$type).toBe('Post')
    expect(input.name).toBe('Test Post')
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Export Integration', () => {
  it('should create working ParqueDBClient', () => {
    // Create a minimal mock service
    const mockService = {
      find: async () => ({ items: [], hasMore: false }),
      get: async () => null,
      create: async () => ({ $id: 'test/1', name: 'Test' }),
      createMany: async () => [],
      update: async () => null,
      delete: async () => ({ deletedCount: 0 }),
      deleteMany: async () => ({ deletedCount: 0 }),
      count: async () => 0,
      exists: async () => false,
      getRelationships: async () => [],
      link: async () => {},
      unlink: async () => {},
      flush: async () => {},
      getFlushStatus: async () => ({ unflushedCount: 0 }),
    }

    const client = createParqueDBClient(mockService as any)
    expect(client).toBeInstanceOf(ParqueDBClient)
  })

  it('should create working ServiceBindingAdapter', () => {
    const mockBinding: Service = {
      fetch: async () => new Response('OK'),
    }

    const adapter = createServiceAdapter(mockBinding)
    expect(adapter).toBeInstanceOf(ServiceBindingAdapter)
  })

  it('should create working CollectionClient', () => {
    const mockRpcService = {
      fetch: async () => new Response(JSON.stringify(null), { status: 200 }),
    }

    const collection = new CollectionClient(mockRpcService, 'posts')
    expect(collection.namespace).toBe('posts')
  })

  it('should create working RpcPromise', async () => {
    const mockService = {
      fetch: async () => new Response(JSON.stringify({ result: 'test' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    }

    const promise = createRpcPromise(mockService, 'get', ['test', '1'])
    expect(isRpcPromise(promise)).toBe(true)

    const result = await promise
    expect(result).toEqual({ result: 'test' })
  })

  it('should work with resolvedRpcPromise', async () => {
    const promise = resolvedRpcPromise([1, 2, 3])
    expect(isRpcPromise(promise)).toBe(true)

    const mapped = promise.map((n: number) => n * 2)
    expect(await mapped).toEqual([2, 4, 6])
  })

  it('should verify service bindings', () => {
    expect(isServiceBinding({ fetch: () => {} })).toBe(true)
    expect(isServiceBinding({})).toBe(false)
    expect(isServiceBinding(null)).toBe(false)
  })

  it('should create RpcError instances', () => {
    const chain: RpcPromiseChain = [{ method: 'find', args: [] }]
    const error = new RpcError('Test error', 500, chain)

    expect(error.message).toBe('Test error')
    expect(error.status).toBe(500)
    expect(error.chain).toEqual(chain)
    expect(error.name).toBe('RpcError')
  })
})

// =============================================================================
// Type Helper Tests
// =============================================================================

describe('Type Helpers', () => {
  // These tests verify that type helpers work correctly
  // The type inference is checked at compile time

  it('should TypedCollection be assignable from Collection', () => {
    // This is a compile-time check - if it compiles, the types are correct
    interface Post {
      title: string
      content: string
    }

    const mockService = {
      find: async () => ({ items: [], hasMore: false }),
      get: async () => null,
      create: async () => ({ $id: 'posts/1', name: 'Test', title: 'Title', content: 'Content' }),
      createMany: async () => [],
      update: async () => null,
      delete: async () => ({ deletedCount: 0 }),
      deleteMany: async () => ({ deletedCount: 0 }),
      count: async () => 0,
      exists: async () => false,
      getRelationships: async () => [],
      link: async () => {},
      unlink: async () => {},
      flush: async () => {},
      getFlushStatus: async () => ({ unflushedCount: 0 }),
    }

    const client = new ParqueDBClient(mockService as any)
    const posts: TypedCollection<Post> = client.collection<Post>('posts')

    expect(posts.namespace).toBe('posts')
  })
})

/**
 * Tests for ParqueDB RPC Client with rpc.do batching
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  createParqueDBRPCClient,
  createBatchLoaderDB,
  type ParqueDBRPCClient,
  type BatchedRequest,
  type BatchedResponse,
} from '../../../src/integrations/rpc-do'

// =============================================================================
// Mock Server Setup
// =============================================================================

/**
 * Create a mock server that handles JSON-RPC requests
 */
function createMockServer() {
  const requests: Array<{ method: string; params: unknown[] }> = []
  const batchRequests: BatchedRequest[][] = []

  const handlers: Record<string, (...args: unknown[]) => unknown> = {
    'db.get': (type: string, id: string) => {
      if (id === 'not-found') return null
      return { $id: `${type}/${id}`, $type: type, name: `Entity ${id}` }
    },
    'db.find': (type: string, filter?: unknown, options?: unknown) => ({
      items: [
        { $id: `${type}/1`, $type: type, name: 'Entity 1' },
        { $id: `${type}/2`, $type: type, name: 'Entity 2' },
      ],
      total: 2,
      hasMore: false,
    }),
    'db.create': (type: string, data: unknown) => ({
      $id: `${type}/new-id`,
      $type: type,
      ...(data as Record<string, unknown>),
    }),
    'db.update': (type: string, id: string, update: unknown) => ({
      $id: `${type}/${id}`,
      $type: type,
      name: 'Updated Entity',
    }),
    'db.delete': (type: string, id: string) => ({
      deletedCount: 1,
    }),
    'db.count': (type: string, filter?: unknown) => 42,
    'db.exists': (type: string, id: string) => id !== 'not-found',
    'db.getRelated': (type: string, id: string, relation: string) => ({
      items: [
        { $id: `related/1`, $type: 'related', name: `Related to ${id}` },
      ],
      total: 1,
      hasMore: false,
    }),
    'db.batchGet': (type: string, ids: string[]) =>
      ids.map((id) =>
        id === 'not-found'
          ? null
          : { $id: `${type}/${id}`, $type: type, name: `Entity ${id}` }
      ),
    '__batch': (batch: BatchedRequest[]) => {
      batchRequests.push(batch)
      return batch.map((req) => {
        const handler = handlers[req.method]
        if (!handler) {
          return { id: req.id, error: { message: `Unknown method: ${req.method}` } }
        }
        try {
          const result = handler(...req.args)
          return { id: req.id, result }
        } catch (error) {
          return { id: req.id, error: { message: (error as Error).message } }
        }
      })
    },
  }

  return {
    requests,
    batchRequests,
    handlers,
    fetch: vi.fn(async (url: string, init?: RequestInit) => {
      const body = JSON.parse(init?.body as string)
      requests.push({ method: body.method, params: body.params })

      const handler = handlers[body.method]
      if (!handler) {
        return new Response(JSON.stringify({
          jsonrpc: '2.0',
          id: body.id,
          error: { message: `Unknown method: ${body.method}` },
        }), { status: 200 })
      }

      const result = handler(...(body.params || []))
      return new Response(JSON.stringify({
        jsonrpc: '2.0',
        id: body.id,
        result,
      }), { status: 200 })
    }),
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('createParqueDBRPCClient', () => {
  let mockServer: ReturnType<typeof createMockServer>
  let originalFetch: typeof globalThis.fetch

  beforeEach(() => {
    mockServer = createMockServer()
    originalFetch = globalThis.fetch
    globalThis.fetch = mockServer.fetch as unknown as typeof globalThis.fetch
  })

  afterEach(() => {
    globalThis.fetch = originalFetch
    vi.useRealTimers()
  })

  describe('basic client creation', () => {
    it('should create a client with required options', () => {
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })

      expect(client).toBeDefined()
      expect(client.collection).toBeDefined()
      expect(client.batchGetRelated).toBeDefined()
      expect(client.batchGet).toBeDefined()
      expect(client.close).toBeDefined()
    })

    it('should create a client with all options', () => {
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        auth: 'test-token',
        timeout: 5000,
        batchingOptions: {
          windowMs: 10,
          maxBatchSize: 50,
        },
        headers: {
          'X-Custom-Header': 'value',
        },
      })

      expect(client).toBeDefined()
    })
  })

  describe('collection operations', () => {
    let client: ParqueDBRPCClient

    beforeEach(() => {
      client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })
    })

    afterEach(() => {
      client.close()
    })

    it('should find entities', async () => {
      const posts = client.collection('posts')
      const result = await posts.find({ published: true })

      expect(result.items).toHaveLength(2)
      expect(result.items[0]?.$type).toBe('posts')
      // Note: undefined becomes null in JSON serialization
      expect(mockServer.requests).toContainEqual({
        method: 'db.find',
        params: ['posts', { published: true }, null],
      })
    })

    it('should find one entity', async () => {
      const posts = client.collection('posts')
      const result = await posts.findOne({ published: true })

      expect(result?.$type).toBe('posts')
    })

    it('should get entity by ID', async () => {
      const posts = client.collection('posts')
      const result = await posts.get('123')

      expect(result?.$id).toBe('posts/123')
      expect(mockServer.requests).toContainEqual({
        method: 'db.get',
        params: ['posts', '123'],
      })
    })

    it('should return null for non-existent entity', async () => {
      const posts = client.collection('posts')
      const result = await posts.get('not-found')

      expect(result).toBeNull()
    })

    it('should create entity', async () => {
      const posts = client.collection('posts')
      const result = await posts.create({
        $type: 'Post',
        name: 'New Post',
        title: 'Hello World',
      })

      expect(result.$id).toBe('posts/new-id')
      expect(mockServer.requests).toContainEqual({
        method: 'db.create',
        params: ['posts', { $type: 'Post', name: 'New Post', title: 'Hello World' }],
      })
    })

    it('should update entity', async () => {
      const posts = client.collection('posts')
      const result = await posts.update('123', { $set: { title: 'Updated' } })

      expect(result?.name).toBe('Updated Entity')
      expect(mockServer.requests).toContainEqual({
        method: 'db.update',
        params: ['posts', '123', { $set: { title: 'Updated' } }],
      })
    })

    it('should delete entity', async () => {
      const posts = client.collection('posts')
      const result = await posts.delete('123')

      expect(result.deletedCount).toBe(1)
      expect(mockServer.requests).toContainEqual({
        method: 'db.delete',
        params: ['posts', '123'],
      })
    })

    it('should count entities', async () => {
      const posts = client.collection('posts')
      const count = await posts.count({ published: true })

      expect(count).toBe(42)
    })

    it('should check entity existence', async () => {
      const posts = client.collection('posts')

      expect(await posts.exists('123')).toBe(true)
      expect(await posts.exists('not-found')).toBe(false)
    })

    it('should get related entities', async () => {
      const users = client.collection('users')
      const result = await users.getRelated('user-1', 'posts')

      expect(result.items).toHaveLength(1)
      expect(result.items[0]?.$type).toBe('related')
    })

    it('should cache collection instances', () => {
      const posts1 = client.collection('posts')
      const posts2 = client.collection('posts')

      expect(posts1).toBe(posts2)
    })
  })

  describe('batch operations', () => {
    let client: ParqueDBRPCClient

    beforeEach(() => {
      client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })
    })

    afterEach(() => {
      client.close()
    })

    it('should batch get entities by IDs', async () => {
      const result = await client.batchGet('users', ['id-1', 'id-2', 'id-3'])

      expect(result).toHaveLength(3)
      expect(result[0]?.$id).toBe('users/id-1')
      expect(result[1]?.$id).toBe('users/id-2')
      expect(result[2]?.$id).toBe('users/id-3')
    })

    it('should handle not-found in batch get', async () => {
      const result = await client.batchGet('users', ['id-1', 'not-found', 'id-3'])

      expect(result).toHaveLength(3)
      expect(result[0]?.$id).toBe('users/id-1')
      expect(result[1]).toBeNull()
      expect(result[2]?.$id).toBe('users/id-3')
    })

    it('should batch get related entities', async () => {
      const result = await client.batchGetRelated([
        { type: 'users', id: 'user-1', relation: 'posts' },
        { type: 'users', id: 'user-2', relation: 'posts' },
        { type: 'users', id: 'user-3', relation: 'comments' },
      ])

      expect(result).toHaveLength(3)
      expect(result[0]?.items).toHaveLength(1)
      expect(result[1]?.items).toHaveLength(1)
      expect(result[2]?.items).toHaveLength(1)
    })
  })

  describe('batching transport', () => {
    it('should batch concurrent requests', async () => {
      vi.useFakeTimers()

      const onBatch = vi.fn()
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        batchingOptions: {
          windowMs: 10,
          maxBatchSize: 50,
          onBatch,
        },
      })

      // Make concurrent requests
      const p1 = client.collection('users').get('1')
      const p2 = client.collection('users').get('2')
      const p3 = client.collection('posts').get('3')

      // Advance timers to trigger batch flush
      await vi.advanceTimersByTimeAsync(15)

      // Wait for all promises
      const [r1, r2, r3] = await Promise.all([p1, p2, p3])

      expect(r1?.$id).toBe('users/1')
      expect(r2?.$id).toBe('users/2')
      expect(r3?.$id).toBe('posts/3')

      // Verify batching occurred
      expect(onBatch).toHaveBeenCalled()
      expect(mockServer.batchRequests.length).toBeGreaterThan(0)

      client.close()
    })

    it('should flush batch when max size reached', async () => {
      vi.useFakeTimers()

      const onBatch = vi.fn()
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        batchingOptions: {
          windowMs: 1000, // Long window
          maxBatchSize: 3, // Small batch size
          onBatch,
        },
      })

      // Make concurrent requests equal to max batch size
      const p1 = client.collection('users').get('1')
      const p2 = client.collection('users').get('2')
      const p3 = client.collection('users').get('3')

      // Should flush immediately without waiting for timer
      await vi.advanceTimersByTimeAsync(1)

      const [r1, r2, r3] = await Promise.all([p1, p2, p3])

      expect(r1?.$id).toBe('users/1')
      expect(r2?.$id).toBe('users/2')
      expect(r3?.$id).toBe('users/3')

      // Verify batch was called
      expect(onBatch).toHaveBeenCalled()
      const batch = onBatch.mock.calls[0]?.[0] as BatchedRequest[]
      expect(batch).toHaveLength(3)

      client.close()
    })
  })

  describe('authentication', () => {
    it('should include auth token in requests', async () => {
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        auth: 'test-token',
      })

      await client.collection('posts').get('123')

      expect(mockServer.fetch).toHaveBeenCalledWith(
        'https://api.example.com/rpc',
        expect.objectContaining({
          headers: expect.objectContaining({
            'Authorization': 'Bearer test-token',
          }),
        })
      )

      client.close()
    })

    it('should support auth provider function', async () => {
      const authProvider = vi.fn().mockResolvedValue('dynamic-token')

      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        auth: authProvider,
      })

      await client.collection('posts').get('123')

      expect(authProvider).toHaveBeenCalled()
      expect(mockServer.fetch).toHaveBeenCalledWith(
        'https://api.example.com/rpc',
        expect.objectContaining({
          headers: expect.objectContaining({
            'Authorization': 'Bearer dynamic-token',
          }),
        })
      )

      client.close()
    })

    it('should handle null auth token', async () => {
      const authProvider = vi.fn().mockResolvedValue(null)

      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        auth: authProvider,
      })

      await client.collection('posts').get('123')

      expect(mockServer.fetch).toHaveBeenCalledWith(
        'https://api.example.com/rpc',
        expect.objectContaining({
          headers: expect.not.objectContaining({
            'Authorization': expect.any(String),
          }),
        })
      )

      client.close()
    })
  })

  describe('error handling', () => {
    it('should handle RPC errors', async () => {
      mockServer.handlers['db.get'] = () => {
        throw new Error('Database error')
      }

      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })

      await expect(client.collection('posts').get('123')).rejects.toThrow('Database error')

      client.close()
    })

    it('should handle HTTP errors', async () => {
      mockServer.fetch.mockResolvedValueOnce(
        new Response('Internal Server Error', { status: 500, statusText: 'Internal Server Error' })
      )

      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })

      await expect(client.collection('posts').get('123')).rejects.toThrow('RPC request failed: 500 Internal Server Error')

      client.close()
    })

    it('should handle network errors', async () => {
      mockServer.fetch.mockRejectedValueOnce(new Error('Network error'))

      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })

      await expect(client.collection('posts').get('123')).rejects.toThrow('Network error')

      client.close()
    })
  })

  describe('custom headers', () => {
    it('should include custom headers in requests', async () => {
      const client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
        headers: {
          'X-Custom-Header': 'custom-value',
          'X-Request-ID': 'req-123',
        },
      })

      await client.collection('posts').get('123')

      expect(mockServer.fetch).toHaveBeenCalledWith(
        'https://api.example.com/rpc',
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Custom-Header': 'custom-value',
            'X-Request-ID': 'req-123',
          }),
        })
      )

      client.close()
    })
  })

  describe('createBatchLoaderDB', () => {
    let client: ParqueDBRPCClient

    beforeEach(() => {
      client = createParqueDBRPCClient({
        url: 'https://api.example.com/rpc',
      })
    })

    afterEach(() => {
      client.close()
    })

    it('should create a BatchLoaderDB-compatible interface', () => {
      const loaderDB = createBatchLoaderDB(client)

      expect(loaderDB).toBeDefined()
      expect(loaderDB.getRelated).toBeDefined()
      expect(loaderDB.getByIds).toBeDefined()
    })

    it('should delegate getRelated to client collection', async () => {
      const loaderDB = createBatchLoaderDB(client)

      const result = await loaderDB.getRelated('users', 'user-1', 'posts')

      expect(result.items).toHaveLength(1)
      expect(mockServer.requests).toContainEqual({
        method: 'db.getRelated',
        params: ['users', 'user-1', 'posts'],
      })
    })

    it('should delegate getByIds to client batchGet', async () => {
      const loaderDB = createBatchLoaderDB(client)

      const results = await loaderDB.getByIds!('users', ['id-1', 'id-2', 'not-found'])

      // Not-found should be filtered out
      expect(results).toHaveLength(2)
      expect(results[0]?.$id).toBe('users/id-1')
      expect(results[1]?.$id).toBe('users/id-2')
    })
  })
})

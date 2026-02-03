/**
 * Mock Factories Unit Tests
 *
 * Tests to verify that all mock factories work correctly.
 */

import { describe, it, expect } from 'vitest'
import {
  createMockR2Bucket,
  createMockR2Object,
  createMockR2ObjectBody,
  createErrorR2Bucket,
} from '../../mocks/r2-bucket'
import {
  createMockKVNamespace,
  createPopulatedKVNamespace,
} from '../../mocks/kv-namespace'
import {
  createMockDurableObjectState,
  createMockDurableObjectStorage,
  createMockDurableObjectStub,
  createMockDurableObjectNamespace,
  createMockSqlStorage,
  createMockDurableObjectEnv,
} from '../../mocks/durable-object'
import {
  createMockStorageBackend,
  createErrorStorageBackend,
} from '../../mocks/storage'
import {
  createMockFetch,
  createJsonResponse,
  createTextResponse,
  createBinaryResponse,
  createErrorResponse,
  createRoutedMockFetch,
  createSequentialMockFetch,
  createFailingMockFetch,
} from '../../mocks/fetch'
import {
  createMockExecutionContext,
  createNoopExecutionContext,
  createTrackingExecutionContext,
} from '../../mocks/execution-context'
import {
  createMockWorker,
  createMockHandlerContext,
  createErrorWorker,
  createMockCaches,
} from '../../mocks/worker'

// =============================================================================
// R2Bucket Mock Tests
// =============================================================================

describe('R2Bucket Mock', () => {
  describe('spy-based mock (default)', () => {
    it('should create a mock with default values', () => {
      const bucket = createMockR2Bucket()
      expect(bucket.get).toBeDefined()
      expect(bucket.put).toBeDefined()
      expect(bucket.delete).toBeDefined()
      expect(bucket.list).toBeDefined()
    })

    it('should return null for get by default', async () => {
      const bucket = createMockR2Bucket()
      const result = await bucket.get('nonexistent')
      expect(result).toBeNull()
    })
  })

  describe('functional mock', () => {
    it('should store and retrieve data', async () => {
      const bucket = createMockR2Bucket({ functional: true })
      const data = new Uint8Array([1, 2, 3])

      await bucket.put('test.bin', data)
      const result = await bucket.get('test.bin')

      expect(result).not.toBeNull()
      const bytes = new Uint8Array(await result!.arrayBuffer())
      expect(Array.from(bytes)).toEqual([1, 2, 3])
    })

    it('should delete data', async () => {
      const bucket = createMockR2Bucket({ functional: true })
      await bucket.put('test.txt', 'hello')
      await bucket.delete('test.txt')
      const result = await bucket.get('test.txt')
      expect(result).toBeNull()
    })

    it('should list objects', async () => {
      const bucket = createMockR2Bucket({ functional: true })
      await bucket.put('a.txt', 'a')
      await bucket.put('b.txt', 'b')

      const result = await bucket.list()
      expect(result.objects).toHaveLength(2)
    })

    it('should filter by prefix', async () => {
      const bucket = createMockR2Bucket({ functional: true })
      await bucket.put('data/a.txt', 'a')
      await bucket.put('data/b.txt', 'b')
      await bucket.put('other/c.txt', 'c')

      const result = await bucket.list({ prefix: 'data/' })
      expect(result.objects).toHaveLength(2)
    })
  })

  describe('error mock', () => {
    it('should reject operations with notFound error', async () => {
      const bucket = createErrorR2Bucket('notFound')
      await expect(bucket.get('test')).rejects.toThrow()
    })
  })

  describe('helper functions', () => {
    it('createMockR2Object should create valid metadata', () => {
      const obj = createMockR2Object('test.txt', 100)
      expect(obj.key).toBe('test.txt')
      expect(obj.size).toBe(100)
      expect(obj.etag).toBeDefined()
    })

    it('createMockR2ObjectBody should include data methods', async () => {
      const data = new TextEncoder().encode('hello')
      const body = createMockR2ObjectBody('test.txt', data)

      expect(await body.text()).toBe('hello')
      expect(body.size).toBe(5)
    })
  })
})

// =============================================================================
// KVNamespace Mock Tests
// =============================================================================

describe('KVNamespace Mock', () => {
  describe('spy-based mock (default)', () => {
    it('should create a mock with default values', () => {
      const kv = createMockKVNamespace()
      expect(kv.get).toBeDefined()
      expect(kv.put).toBeDefined()
      expect(kv.delete).toBeDefined()
      expect(kv.list).toBeDefined()
    })

    it('should return null for get by default', async () => {
      const kv = createMockKVNamespace()
      const result = await kv.get('nonexistent')
      expect(result).toBeNull()
    })
  })

  describe('functional mock', () => {
    it('should store and retrieve text', async () => {
      const kv = createMockKVNamespace({ functional: true })
      await kv.put('key', 'value')
      const result = await kv.get('key')
      expect(result).toBe('value')
    })

    it('should return JSON when requested', async () => {
      const kv = createMockKVNamespace({ functional: true })
      await kv.put('key', JSON.stringify({ data: 'test' }))
      const result = await kv.get('key', 'json')
      expect(result).toEqual({ data: 'test' })
    })

    it('should handle expiration', async () => {
      const kv = createMockKVNamespace({ functional: true })
      await kv.put('key', 'value', { expirationTtl: -1 }) // Already expired
      kv._setExpired('key')
      const result = await kv.get('key')
      expect(result).toBeNull()
    })

    it('should list keys', async () => {
      const kv = createMockKVNamespace({ functional: true })
      await kv.put('a', '1')
      await kv.put('b', '2')

      const result = await kv.list()
      expect(result.keys).toHaveLength(2)
    })
  })

  describe('pre-populated mock', () => {
    it('should have initial data', async () => {
      const kv = createPopulatedKVNamespace({
        key1: 'value1',
        key2: { value: 'value2', metadata: { foo: 'bar' } },
      })

      expect(await kv.get('key1')).toBe('value1')
      expect(await kv.get('key2')).toBe('value2')
    })
  })
})

// =============================================================================
// DurableObject Mock Tests
// =============================================================================

describe('DurableObject Mocks', () => {
  describe('MockDurableObjectStorage', () => {
    it('should store and retrieve values', async () => {
      const storage = createMockDurableObjectStorage()
      await storage.put('key', { data: 'test' })
      const result = await storage.get('key')
      expect(result).toEqual({ data: 'test' })
    })

    it('should delete values', async () => {
      const storage = createMockDurableObjectStorage()
      await storage.put('key', 'value')
      const deleted = await storage.delete('key')
      expect(deleted).toBe(true)
      const result = await storage.get('key')
      expect(result).toBeUndefined()
    })

    it('should list values with prefix', async () => {
      const storage = createMockDurableObjectStorage()
      await storage.put('prefix:a', 1)
      await storage.put('prefix:b', 2)
      await storage.put('other:c', 3)

      const result = await storage.list({ prefix: 'prefix:' })
      expect(result.size).toBe(2)
    })
  })

  describe('MockSqlStorage', () => {
    it('should track queries', () => {
      const sql = createMockSqlStorage()
      sql.exec('SELECT * FROM entities WHERE id = ?', 'test-id')
      expect(sql._queries).toHaveLength(1)
      expect(sql._queries[0].query).toContain('SELECT')
    })

    it('should return configured results', () => {
      const sql = createMockSqlStorage()
      sql._setResult('entities', [{ id: '1', name: 'Test' }])

      const cursor = sql.exec('SELECT * FROM entities')
      expect(cursor.toArray()).toHaveLength(1)
    })
  })

  describe('MockDurableObjectState', () => {
    it('should have id and storage', () => {
      const state = createMockDurableObjectState()
      expect(state.id).toBeDefined()
      expect(state.storage).toBeDefined()
    })

    it('should track waitUntil calls', () => {
      const state = createMockDurableObjectState()
      state.waitUntil(Promise.resolve())
      expect(state.waitUntil).toHaveBeenCalled()
    })

    it('should execute blockConcurrencyWhile callback', async () => {
      const state = createMockDurableObjectState()
      const result = await state.blockConcurrencyWhile(async () => 'result')
      expect(result).toBe('result')
    })
  })

  describe('MockDurableObjectStub', () => {
    it('should handle fetch requests', async () => {
      const stub = createMockDurableObjectStub()
      const response = await stub.fetch('https://example.com/test')
      expect(response.status).toBe(200)
    })

    it('should use custom fetch handler', async () => {
      const stub = createMockDurableObjectStub({
        fetchHandler: async () => new Response('custom', { status: 201 }),
      })
      const response = await stub.fetch('https://example.com')
      expect(response.status).toBe(201)
    })
  })

  describe('MockDurableObjectNamespace', () => {
    it('should create unique IDs', () => {
      const ns = createMockDurableObjectNamespace()
      const id1 = ns.newUniqueId()
      const id2 = ns.newUniqueId()
      expect(id1.toString()).not.toBe(id2.toString())
    })

    it('should create ID from name', () => {
      const ns = createMockDurableObjectNamespace()
      const id = ns.idFromName('my-do')
      expect(id.name).toBe('my-do')
    })

    it('should return stubs', () => {
      const ns = createMockDurableObjectNamespace()
      const id = ns.newUniqueId()
      const stub = ns.get(id)
      expect(stub.id).toBe(id)
    })
  })

  describe('createMockDurableObjectEnv', () => {
    it('should create all DO mocks', () => {
      const env = createMockDurableObjectEnv()
      expect(env.state).toBeDefined()
      expect(env.storage).toBeDefined()
      expect(env.sql).toBeDefined()
      expect(env.namespace).toBeDefined()
    })
  })
})

// =============================================================================
// StorageBackend Mock Tests
// =============================================================================

describe('StorageBackend Mock', () => {
  describe('spy-based mock (default)', () => {
    it('should have all required methods', () => {
      const storage = createMockStorageBackend()
      expect(storage.read).toBeDefined()
      expect(storage.write).toBeDefined()
      expect(storage.delete).toBeDefined()
      expect(storage.exists).toBeDefined()
      expect(storage.list).toBeDefined()
    })

    it('should reject read by default', async () => {
      const storage = createMockStorageBackend()
      await expect(storage.read('test.txt')).rejects.toThrow()
    })
  })

  describe('functional mock', () => {
    it('should write and read files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      const data = new TextEncoder().encode('hello')

      await storage.write('test.txt', data)
      const result = await storage.read('test.txt')

      expect(new TextDecoder().decode(result)).toBe('hello')
    })

    it('should check file existence', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('exists.txt', new Uint8Array([1]))

      expect(await storage.exists('exists.txt')).toBe(true)
      expect(await storage.exists('nonexistent.txt')).toBe(false)
    })

    it('should delete files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('test.txt', new Uint8Array([1]))
      await storage.delete('test.txt')
      expect(await storage.exists('test.txt')).toBe(false)
    })

    it('should list files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('a.txt', new Uint8Array([1]))
      await storage.write('b.txt', new Uint8Array([2]))

      const result = await storage.list('')
      expect(result.files).toHaveLength(2)
    })

    it('should append to files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('log.txt', new TextEncoder().encode('line1\n'))
      await storage.append('log.txt', new TextEncoder().encode('line2\n'))

      const result = await storage.read('log.txt')
      expect(new TextDecoder().decode(result)).toBe('line1\nline2\n')
    })

    it('should copy files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('source.txt', new TextEncoder().encode('content'))
      await storage.copy('source.txt', 'dest.txt')

      expect(await storage.exists('source.txt')).toBe(true)
      expect(await storage.exists('dest.txt')).toBe(true)
    })

    it('should move files', async () => {
      const storage = createMockStorageBackend({ functional: true })
      await storage.write('source.txt', new TextEncoder().encode('content'))
      await storage.move('source.txt', 'dest.txt')

      expect(await storage.exists('source.txt')).toBe(false)
      expect(await storage.exists('dest.txt')).toBe(true)
    })
  })

  describe('error mock', () => {
    it('should reject all operations', async () => {
      const storage = createErrorStorageBackend('notFound')
      await expect(storage.read('test')).rejects.toThrow()
      await expect(storage.write('test', new Uint8Array())).rejects.toThrow()
    })
  })
})

// =============================================================================
// Fetch Mock Tests
// =============================================================================

describe('Fetch Mock', () => {
  describe('createMockFetch', () => {
    it('should return 404 by default', async () => {
      const mockFetch = createMockFetch()
      const response = await mockFetch('https://example.com')
      expect(response.status).toBe(404)
    })

    it('should allow mocking resolved value', async () => {
      const mockFetch = createMockFetch()
      mockFetch.mockResolvedValue(createJsonResponse({ success: true }))

      const response = await mockFetch('https://example.com')
      const json = await response.json()
      expect(json.success).toBe(true)
    })
  })

  describe('createJsonResponse', () => {
    it('should create JSON response with correct content type', async () => {
      const response = createJsonResponse({ data: 'test' })
      expect(response.headers.get('Content-Type')).toBe('application/json')

      const json = await response.json()
      expect(json.data).toBe('test')
    })

    it('should handle null body', async () => {
      const response = createJsonResponse(null)
      const json = await response.json()
      expect(json).toBeNull()
    })

    it('should handle arrays', async () => {
      const response = createJsonResponse([1, 2, 3])
      const json = await response.json()
      expect(json).toEqual([1, 2, 3])
    })
  })

  describe('createTextResponse', () => {
    it('should create text response', async () => {
      const response = createTextResponse('hello')
      expect(response.headers.get('Content-Type')).toBe('text/plain')
      expect(await response.text()).toBe('hello')
    })
  })

  describe('createBinaryResponse', () => {
    it('should create binary response', async () => {
      const data = new Uint8Array([1, 2, 3])
      const response = createBinaryResponse(data)
      expect(response.headers.get('Content-Type')).toBe('application/octet-stream')
    })
  })

  describe('createErrorResponse', () => {
    it('should create error response with correct status', () => {
      const response = createErrorResponse(404, 'Not found')
      expect(response.status).toBe(404)
      expect(response.ok).toBe(false)
    })
  })

  describe('createRoutedMockFetch', () => {
    it('should route to correct handler', async () => {
      const mockFetch = createRoutedMockFetch({
        routes: [
          {
            pattern: '/api/users',
            handler: () => createJsonResponse([{ id: 1 }]),
          },
        ],
      })

      const response = await mockFetch('https://example.com/api/users')
      const json = await response.json()
      expect(json).toHaveLength(1)
    })

    it('should support regex patterns', async () => {
      const mockFetch = createRoutedMockFetch({
        routes: [
          {
            pattern: /\/api\/users\/\d+/,
            handler: () => createJsonResponse({ id: 1 }),
          },
        ],
      })

      const response = await mockFetch('https://example.com/api/users/123')
      expect(response.ok).toBe(true)
    })

    it('should filter by method', async () => {
      const mockFetch = createRoutedMockFetch({
        routes: [
          {
            pattern: '/api/users',
            method: 'POST',
            handler: () => createJsonResponse({ created: true }, { status: 201 }),
          },
        ],
      })

      const getResponse = await mockFetch('https://example.com/api/users')
      expect(getResponse.status).toBe(404)

      const postResponse = await mockFetch('https://example.com/api/users', { method: 'POST' })
      expect(postResponse.status).toBe(201)
    })
  })

  describe('createSequentialMockFetch', () => {
    it('should return responses in order', async () => {
      const mockFetch = createSequentialMockFetch([
        createJsonResponse({ attempt: 1 }),
        createErrorResponse(503),
        createJsonResponse({ attempt: 3 }),
      ])

      const r1 = await mockFetch('https://example.com')
      expect((await r1.json()).attempt).toBe(1)

      const r2 = await mockFetch('https://example.com')
      expect(r2.status).toBe(503)

      const r3 = await mockFetch('https://example.com')
      expect((await r3.json()).attempt).toBe(3)
    })
  })

  describe('createFailingMockFetch', () => {
    it('should throw network error', async () => {
      const mockFetch = createFailingMockFetch('network')
      await expect(mockFetch('https://example.com')).rejects.toThrow('Failed to fetch')
    })

    it('should throw timeout error', async () => {
      const mockFetch = createFailingMockFetch('timeout')
      await expect(mockFetch('https://example.com')).rejects.toThrow()
    })
  })
})

// =============================================================================
// ExecutionContext Mock Tests
// =============================================================================

describe('ExecutionContext Mock', () => {
  describe('createMockExecutionContext', () => {
    it('should track waitUntil calls', () => {
      const ctx = createMockExecutionContext()
      ctx.waitUntil(Promise.resolve('done'))
      expect(ctx.waitUntil).toHaveBeenCalled()
      expect(ctx._pendingPromises).toHaveLength(1)
    })

    it('should wait for pending promises', async () => {
      const ctx = createMockExecutionContext()
      let resolved = false
      ctx.waitUntil(
        new Promise<void>((resolve) => {
          setTimeout(() => {
            resolved = true
            resolve()
          }, 10)
        })
      )

      await ctx._waitForPending()
      expect(resolved).toBe(true)
    })

    it('should track passThroughOnException', () => {
      const ctx = createMockExecutionContext()
      ctx.passThroughOnException()
      expect(ctx.passThroughOnException).toHaveBeenCalled()
    })
  })

  describe('createNoopExecutionContext', () => {
    it('should not throw', () => {
      const ctx = createNoopExecutionContext()
      expect(() => ctx.waitUntil(Promise.resolve())).not.toThrow()
      expect(() => ctx.passThroughOnException()).not.toThrow()
    })
  })

  describe('createTrackingExecutionContext', () => {
    it('should track pending count', () => {
      const ctx = createTrackingExecutionContext()
      ctx.waitUntil(Promise.resolve())
      ctx.waitUntil(Promise.resolve())
      expect(ctx.pendingCount).toBe(2)
    })

    it('should flush all promises', async () => {
      const ctx = createTrackingExecutionContext()
      let count = 0
      ctx.waitUntil(Promise.resolve().then(() => count++))
      ctx.waitUntil(Promise.resolve().then(() => count++))

      await ctx.flush()
      expect(count).toBe(2)
    })
  })
})

// =============================================================================
// Worker Mock Tests
// =============================================================================

describe('Worker Mock', () => {
  describe('createMockWorker (spy-based)', () => {
    it('should have all methods', () => {
      const worker = createMockWorker()
      expect(worker.get).toBeDefined()
      expect(worker.find).toBeDefined()
      expect(worker.create).toBeDefined()
      expect(worker.update).toBeDefined()
      expect(worker.delete).toBeDefined()
      expect(worker.getRelationships).toBeDefined()
      expect(worker.getStorageStats).toBeDefined()
    })

    it('should return null for get by default', async () => {
      const worker = createMockWorker()
      const result = await worker.get('users', '1')
      expect(result).toBeNull()
    })

    it('should return empty find result by default', async () => {
      const worker = createMockWorker()
      const result = await worker.find('users')
      expect(result.items).toHaveLength(0)
      expect(result.hasMore).toBe(false)
    })
  })

  describe('createMockWorker (functional)', () => {
    it('should store and retrieve entities', async () => {
      const worker = createMockWorker({ functional: true })
      await worker.create('users', { $id: 'users/1', name: 'Alice' })
      const entity = await worker.get('users', '1')
      expect(entity?.name).toBe('Alice')
    })

    it('should initialize with entities', async () => {
      const worker = createMockWorker({
        functional: true,
        entities: [{ $id: 'users/1', name: 'Alice' }],
      })
      const entity = await worker.get('users', '1')
      expect(entity?.name).toBe('Alice')
    })

    it('should find entities with filter', async () => {
      const worker = createMockWorker({
        functional: true,
        entities: [
          { $id: 'users/1', name: 'Alice', role: 'admin' },
          { $id: 'users/2', name: 'Bob', role: 'user' },
        ],
      })

      const result = await worker.find('users', { role: 'admin' })
      expect(result.items).toHaveLength(1)
      expect(result.items[0].name).toBe('Alice')
    })
  })

  describe('createErrorWorker', () => {
    it('should reject all operations', async () => {
      const worker = createErrorWorker('serverError')
      await expect(worker.get('users', '1')).rejects.toThrow()
      await expect(worker.find('users')).rejects.toThrow()
      await expect(worker.create('users', {})).rejects.toThrow()
    })
  })

  describe('createMockHandlerContext', () => {
    it('should create context from URL', () => {
      const ctx = createMockHandlerContext('https://api.example.com/v1/users')
      expect(ctx.path).toBe('/v1/users')
      expect(ctx.baseUrl).toBe('https://api.example.com')
    })

    it('should include CSRF headers for mutations', () => {
      const ctx = createMockHandlerContext('/api/users', { method: 'POST' })
      expect(ctx.request.headers.get('X-Requested-With')).toBe('XMLHttpRequest')
    })

    it('should include body for mutations', () => {
      const ctx = createMockHandlerContext('/api/users', {
        method: 'POST',
        body: { name: 'Alice' },
      })
      expect(ctx.request.headers.get('Content-Type')).toBe('application/json')
    })
  })

  describe('createMockCaches', () => {
    it('should create cache mock', async () => {
      const caches = createMockCaches()
      expect(caches.default).toBeDefined()
      expect(caches.open).toBeDefined()

      // Should return null for match by default
      const result = await caches.default.match('test')
      expect(result).toBeNull()
    })
  })
})

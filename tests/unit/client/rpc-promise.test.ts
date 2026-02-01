/**
 * RpcPromise Unit Tests
 *
 * Tests for the RpcPromise implementation:
 * - Promise chaining with map
 * - then/catch behavior
 * - Chain collection before execution
 * - Function serialization/deserialization
 * - Error handling (RpcError)
 * - Utility functions (isRpcPromise, batchRpc, resolvedRpcPromise)
 *
 * Uses mocked RPC service to test client-side behavior in isolation.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  createRpcPromise,
  isRpcPromise,
  batchRpc,
  resolvedRpcPromise,
  RpcError,
  deserializeFunction,
  registerMapper,
  getRegisteredMapper,
  clearMapperRegistry,
  type RpcPromiseChain,
} from '../../../src/client/rpc-promise'
import type { RpcService } from '../../../src/client/collection'

// =============================================================================
// Mock RPC Service Factory
// =============================================================================

/**
 * Create a mock RPC service for testing
 */
function createMockRpcService(handler?: (chain: RpcPromiseChain) => unknown): RpcService & { lastChain?: RpcPromiseChain } {
  const service: RpcService & { lastChain?: RpcPromiseChain } = {
    lastChain: undefined,
    async fetch(path: string, options?: { method?: string; body?: string }) {
      if (path === '/rpc' && options?.method === 'POST' && options?.body) {
        const body = JSON.parse(options.body)

        if (body.chain && Array.isArray(body.chain)) {
          service.lastChain = body.chain

          if (handler) {
            const result = handler(body.chain)
            return new Response(JSON.stringify(result), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          }

          // Default: return empty result
          return new Response(JSON.stringify({ items: [], hasMore: false }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          })
        }
      }

      return new Response('Not Found', { status: 404 })
    },
  }

  return service
}

// =============================================================================
// createRpcPromise Tests
// =============================================================================

describe('createRpcPromise', () => {
  describe('basic behavior', () => {
    it('should create a thenable promise', () => {
      const service = createMockRpcService()
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      expect(promise).toHaveProperty('then')
      expect(typeof promise.then).toBe('function')
    })

    it('should have __rpcPromise marker', () => {
      const service = createMockRpcService()
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      expect((promise as any).__rpcPromise).toBe(true)
    })

    it('should have map method', () => {
      const service = createMockRpcService()
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      expect(typeof (promise as any).map).toBe('function')
    })

    it('should have catch method', () => {
      const service = createMockRpcService()
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      expect(typeof promise.catch).toBe('function')
    })

    it('should have finally method', () => {
      const service = createMockRpcService()
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      expect(typeof promise.finally).toBe('function')
    })
  })

  describe('chain execution', () => {
    it('should execute chain when awaited', async () => {
      const service = createMockRpcService(() => ({ items: [{ name: 'Test' }], hasMore: false }))
      const promise = createRpcPromise(service, 'find', ['posts', {}])

      const result = await promise

      expect(service.lastChain).toEqual([
        { method: 'find', args: ['posts', {}] },
      ])
      expect(result).toEqual({ items: [{ name: 'Test' }], hasMore: false })
    })

    it('should collect chain with map before executing', async () => {
      const service = createMockRpcService(() => ['Post 1', 'Post 2'])
      const promise = createRpcPromise(service, 'find', ['posts', {}])
        .map((p: any) => p.name)

      await promise

      expect(service.lastChain).toHaveLength(2)
      expect(service.lastChain?.[0]?.method).toBe('find')
      expect(service.lastChain?.[1]?.method).toBe('map')
    })

    it('should handle multiple map calls', async () => {
      const service = createMockRpcService(() => ['Author 1', 'Author 2'])
      const promise = createRpcPromise(service, 'find', ['posts', {}])
        .map((p: any) => p.author)
        .map((a: any) => a.name)

      await promise

      expect(service.lastChain).toHaveLength(3)
      expect(service.lastChain?.[0]?.method).toBe('find')
      expect(service.lastChain?.[1]?.method).toBe('map')
      expect(service.lastChain?.[2]?.method).toBe('map')
    })

    it('should defer execution until awaited', async () => {
      let fetchCalled = false
      const service: RpcService = {
        async fetch() {
          fetchCalled = true
          return new Response(JSON.stringify(null), { status: 200 })
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', {}])

      // Chain should not execute immediately
      expect(fetchCalled).toBe(false)

      // Wait for microtask queue to process
      await new Promise(resolve => setTimeout(resolve, 0))

      // Now it should have executed
      expect(fetchCalled).toBe(true)
    })
  })

  describe('then/catch behavior', () => {
    it('should support then callback', async () => {
      const service = createMockRpcService(() => ({ value: 42 }))
      const promise = createRpcPromise(service, 'get', ['test', '1'])

      const result = await promise.then((data: any) => data.value)

      expect(result).toBe(42)
    })

    it('should support chained then callbacks', async () => {
      const service = createMockRpcService(() => ({ value: 10 }))
      const promise = createRpcPromise(service, 'get', ['test', '1'])

      const result = await promise
        .then((data: any) => data.value)
        .then((value: number) => value * 2)
        .then((value: number) => value + 5)

      expect(result).toBe(25)
    })

    it('should support catch callback', async () => {
      const service: RpcService = {
        async fetch() {
          return new Response('Server Error', { status: 500 })
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', {}])

      const error = await promise.catch((err: Error) => err)

      expect(error).toBeInstanceOf(RpcError)
    })

    it('should support finally callback', async () => {
      let finallyCalled = false
      const service = createMockRpcService(() => null)
      const promise = createRpcPromise(service, 'get', ['test', '1'])

      await promise.finally(() => {
        finallyCalled = true
      })

      expect(finallyCalled).toBe(true)
    })

    it('should call finally on error', async () => {
      let finallyCalled = false
      const service: RpcService = {
        async fetch() {
          return new Response('Error', { status: 500 })
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', {}])

      try {
        await promise.finally(() => {
          finallyCalled = true
        })
      } catch {
        // Expected error
      }

      expect(finallyCalled).toBe(true)
    })
  })

  describe('error handling', () => {
    it('should throw RpcError on non-ok response', async () => {
      const service: RpcService = {
        async fetch() {
          return new Response('Internal Server Error', { status: 500 })
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', {}])

      await expect(promise).rejects.toThrow(RpcError)
    })

    it('should include status code in RpcError', async () => {
      const service: RpcService = {
        async fetch() {
          return new Response('Not Found', { status: 404 })
        },
      }

      const promise = createRpcPromise(service, 'get', ['posts', '999'])

      try {
        await promise
        expect.fail('Should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(RpcError)
        expect((err as RpcError).status).toBe(404)
      }
    })

    it('should include chain in RpcError', async () => {
      const service: RpcService = {
        async fetch() {
          return new Response('Error', { status: 500 })
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', { status: 'published' }])
        .map((p: any) => p.name)

      try {
        await promise
        expect.fail('Should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(RpcError)
        const rpcErr = err as RpcError
        expect(rpcErr.chain).toHaveLength(2)
        expect(rpcErr.chain[0].method).toBe('find')
        expect(rpcErr.chain[1].method).toBe('map')
      }
    })

    it('should handle network errors', async () => {
      const service: RpcService = {
        async fetch() {
          throw new Error('Network failure')
        },
      }

      const promise = createRpcPromise(service, 'find', ['posts', {}])

      await expect(promise).rejects.toThrow('Network failure')
    })
  })
})

// =============================================================================
// map Method Tests
// =============================================================================

describe('RpcPromise.map', () => {
  it('should serialize arrow function as path mapper', async () => {
    const service = createMockRpcService(() => null)
    const promise = createRpcPromise(service, 'find', ['posts', {}])
      .map((x: any) => x.name)

    await promise

    const mapArg = service.lastChain?.[1]?.args?.[0]
    expect(typeof mapArg).toBe('string')

    const parsed = JSON.parse(mapArg as string)
    // Simple property access is converted to 'path' type
    expect(parsed.mapperType).toBe('path')
    expect(parsed.path).toBe('name')
  })

  it('should serialize async arrow function', async () => {
    const service = createMockRpcService(() => null)
    const promise = createRpcPromise(service, 'find', ['posts', {}])
      .map(async (x: any) => x.name)

    await promise

    const mapArg = service.lastChain?.[1]?.args?.[0]
    const parsed = JSON.parse(mapArg as string)
    // Simple property access is converted to 'path' type, with async flag
    expect(parsed.mapperType).toBe('path')
    expect(parsed.async).toBe(true)
  })

  it('should return same RpcPromise for chaining', () => {
    const service = createMockRpcService()
    const promise1 = createRpcPromise(service, 'find', ['posts', {}])
    const promise2 = promise1.map((x: any) => x.name)

    expect(isRpcPromise(promise2)).toBe(true)
  })

  it('should support typed mapping', async () => {
    interface Post {
      title: string
    }

    const service = createMockRpcService(() => ['Title 1', 'Title 2'])
    const promise = createRpcPromise<Post[]>(service, 'find', ['posts', {}])
      .map((post) => post.title)

    const result = await promise as string[]
    expect(result).toEqual(['Title 1', 'Title 2'])
  })
})

// =============================================================================
// isRpcPromise Tests
// =============================================================================

describe('isRpcPromise', () => {
  it('should return true for RpcPromise', () => {
    const service = createMockRpcService()
    const promise = createRpcPromise(service, 'find', ['posts', {}])
    expect(isRpcPromise(promise)).toBe(true)
  })

  it('should return true for RpcPromise after map', () => {
    const service = createMockRpcService()
    const promise = createRpcPromise(service, 'find', ['posts', {}])
      .map((x: any) => x.name)
    expect(isRpcPromise(promise)).toBe(true)
  })

  it('should return false for regular Promise', () => {
    const promise = Promise.resolve()
    expect(isRpcPromise(promise)).toBe(false)
  })

  it('should return false for null', () => {
    expect(isRpcPromise(null)).toBe(false)
  })

  it('should return false for undefined', () => {
    expect(isRpcPromise(undefined)).toBe(false)
  })

  it('should return false for non-object', () => {
    expect(isRpcPromise('string')).toBe(false)
    expect(isRpcPromise(123)).toBe(false)
    expect(isRpcPromise(true)).toBe(false)
  })

  it('should return false for object without marker', () => {
    expect(isRpcPromise({ then: () => {} })).toBe(false)
  })

  it('should return false for object with false marker', () => {
    expect(isRpcPromise({ __rpcPromise: false })).toBe(false)
  })

  it('should return true for resolvedRpcPromise', () => {
    const promise = resolvedRpcPromise([1, 2, 3])
    expect(isRpcPromise(promise)).toBe(true)
  })
})

// =============================================================================
// batchRpc Tests
// =============================================================================

describe('batchRpc', () => {
  it('should execute multiple promises in parallel', async () => {
    const service = createMockRpcService((chain) => {
      const method = chain[0]?.method
      if (method === 'find' && chain[0]?.args?.[0] === 'posts') {
        return { items: [{ name: 'Post 1' }], hasMore: false }
      }
      if (method === 'find' && chain[0]?.args?.[0] === 'users') {
        return { items: [{ name: 'User 1' }], hasMore: false }
      }
      return null
    })

    const promise1 = createRpcPromise(service, 'find', ['posts', {}])
    const promise2 = createRpcPromise(service, 'find', ['users', {}])

    const [result1, result2] = await batchRpc(promise1, promise2)

    expect((result1 as any).items[0].name).toBe('Post 1')
    expect((result2 as any).items[0].name).toBe('User 1')
  })

  it('should handle empty array', async () => {
    const results = await batchRpc()
    expect(results).toEqual([])
  })

  it('should handle single promise', async () => {
    const service = createMockRpcService(() => ({ value: 42 }))
    const promise = createRpcPromise(service, 'get', ['test', '1'])

    const [result] = await batchRpc(promise)

    expect((result as any).value).toBe(42)
  })

  it('should propagate errors', async () => {
    const service: RpcService = {
      async fetch() {
        return new Response('Error', { status: 500 })
      },
    }

    const promise1 = createRpcPromise(service, 'find', ['posts', {}])
    const promise2 = createRpcPromise(service, 'find', ['users', {}])

    await expect(batchRpc(promise1, promise2)).rejects.toThrow()
  })
})

// =============================================================================
// resolvedRpcPromise Tests
// =============================================================================

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
    const promise = resolvedRpcPromise({ name: 'test', value: 42 })
    const mapped = promise.map((obj: { name: string; value: number }) => obj.name)

    expect(await mapped).toBe('test')
  })

  it('should support chained map calls', async () => {
    const promise = resolvedRpcPromise([{ value: 1 }, { value: 2 }, { value: 3 }])
    const mapped = promise
      .map((obj: { value: number }) => obj.value)
      .map((n: number) => n * 10)

    expect(await mapped).toEqual([10, 20, 30])
  })

  it('should handle async map functions', async () => {
    const promise = resolvedRpcPromise([1, 2, 3])
    const mapped = promise.map(async (n: number) => {
      await new Promise(resolve => setTimeout(resolve, 1))
      return n * 2
    })

    expect(await mapped).toEqual([2, 4, 6])
  })

  it('should handle null value', async () => {
    const promise = resolvedRpcPromise(null)
    expect(await promise).toBeNull()
  })

  it('should handle undefined value', async () => {
    const promise = resolvedRpcPromise(undefined)
    expect(await promise).toBeUndefined()
  })

  it('should handle empty array', async () => {
    const promise = resolvedRpcPromise([])
    const mapped = promise.map((x: any) => x.name)
    expect(await mapped).toEqual([])
  })
})

// =============================================================================
// RpcError Tests
// =============================================================================

describe('RpcError', () => {
  it('should include message', () => {
    const chain: RpcPromiseChain = [{ method: 'find', args: ['posts'] }]
    const error = new RpcError('Test error', 500, chain)

    expect(error.message).toBe('Test error')
  })

  it('should include status', () => {
    const chain: RpcPromiseChain = [{ method: 'find', args: ['posts'] }]
    const error = new RpcError('Test error', 404, chain)

    expect(error.status).toBe(404)
  })

  it('should include chain', () => {
    const chain: RpcPromiseChain = [
      { method: 'find', args: ['posts', { status: 'published' }] },
      { method: 'map', args: ['(x) => x.name'] },
    ]
    const error = new RpcError('Test error', 500, chain)

    expect(error.chain).toEqual(chain)
  })

  it('should have name "RpcError"', () => {
    const chain: RpcPromiseChain = []
    const error = new RpcError('Test', 500, chain)

    expect(error.name).toBe('RpcError')
  })

  it('should be instanceof Error', () => {
    const chain: RpcPromiseChain = []
    const error = new RpcError('Test', 500, chain)

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(RpcError)
  })

  it('should have stack trace', () => {
    const chain: RpcPromiseChain = []
    const error = new RpcError('Test', 500, chain)

    expect(error.stack).toBeDefined()
  })
})

// =============================================================================
// deserializeFunction Tests
// =============================================================================

describe('deserializeFunction', () => {
  describe('arrow functions', () => {
    it('should deserialize arrow function with parens', () => {
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

    it('should deserialize arrow function with multiple params', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(a, b) => a + b',
      })
      const fn = deserializeFunction<[number, number], number>(serialized)

      // Note: deserializeFunction uses single param, so this tests the parsing
      // In practice, mapping functions take single items
      expect(typeof fn).toBe('function')
    })

    it('should deserialize async arrow function', () => {
      const serialized = JSON.stringify({
        type: 'async',
        body: 'async (x) => x.name',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(typeof fn).toBe('function')
    })
  })

  describe('regular functions', () => {
    it('should reject named functions for security (use anonymous instead)', () => {
      // Named functions look like function calls and are blocked for security
      // Use anonymous functions instead: function(x) { return x.name; }
      const serialized = JSON.stringify({
        type: 'sync',
        body: 'function getName(x) { return x.name; }',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function calls detected')
    })

    it('should deserialize anonymous function with simple body', () => {
      // Anonymous functions with simple bodies are allowed
      const serialized = JSON.stringify({
        type: 'sync',
        body: 'function(x) { return x.name; }',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(fn({ name: 'test' })).toBe('test')
    })

    it('should deserialize anonymous function', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: 'function(x) { return x + 1; }',
      })
      const fn = deserializeFunction<number, number>(serialized)

      expect(fn(5)).toBe(6)
    })

    it('should deserialize function with multiple statements', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: 'function(x) { const doubled = x * 2; const tripled = doubled + x; return tripled; }',
      })
      const fn = deserializeFunction<number, number>(serialized)

      expect(fn(5)).toBe(15)
    })
  })

  describe('error handling', () => {
    it('should throw for invalid function string', () => {
      const serialized = JSON.stringify({ type: 'sync', body: 'not a function' })

      expect(() => deserializeFunction(serialized)).toThrow('Unable to deserialize function')
    })

    it('should throw for malformed JSON', () => {
      expect(() => deserializeFunction('not json')).toThrow()
    })

    it('should throw for missing body', () => {
      const serialized = JSON.stringify({ type: 'sync' })

      expect(() => deserializeFunction(serialized)).toThrow()
    })
  })

  describe('complex expressions', () => {
    it('should handle property access chains', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.user.profile.name',
      })
      const fn = deserializeFunction<{ user: { profile: { name: string } } }, string>(serialized)

      expect(fn({ user: { profile: { name: 'John' } } })).toBe('John')
    })

    it('should handle array access', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.items[0]',
      })
      const fn = deserializeFunction<{ items: number[] }, number>(serialized)

      expect(fn({ items: [42, 43, 44] })).toBe(42)
    })

    it('should handle object literals', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => ({ id: x.$id, label: x.name })',
      })
      const fn = deserializeFunction<{ $id: string; name: string }, { id: string; label: string }>(serialized)

      expect(fn({ $id: 'posts/1', name: 'Test' })).toEqual({ id: 'posts/1', label: 'Test' })
    })

    it('should handle template literals', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => `${x.first} ${x.last}`',
      })
      const fn = deserializeFunction<{ first: string; last: string }, string>(serialized)

      expect(fn({ first: 'John', last: 'Doe' })).toBe('John Doe')
    })

    it('should handle ternary expressions', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.active ? "yes" : "no"',
      })
      const fn = deserializeFunction<{ active: boolean }, string>(serialized)

      expect(fn({ active: true })).toBe('yes')
      expect(fn({ active: false })).toBe('no')
    })
  })

  describe('path-based mappers (secure)', () => {
    it('should deserialize simple path mapper', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: 'name',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(fn({ name: 'test' })).toBe('test')
    })

    it('should deserialize nested path mapper', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: 'author.name',
      })
      const fn = deserializeFunction<{ author: { name: string } }, string>(serialized)

      expect(fn({ author: { name: 'John' } })).toBe('John')
    })

    it('should deserialize deeply nested path mapper', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: 'user.profile.settings.theme',
      })
      const fn = deserializeFunction<{ user: { profile: { settings: { theme: string } } } }, string>(serialized)

      expect(fn({ user: { profile: { settings: { theme: 'dark' } } } })).toBe('dark')
    })

    it('should handle empty path (return entire object)', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: '',
      })
      const fn = deserializeFunction<{ name: string }, { name: string }>(serialized)

      expect(fn({ name: 'test' })).toEqual({ name: 'test' })
    })

    it('should handle array index access', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: 'items[0]',
      })
      const fn = deserializeFunction<{ items: string[] }, string>(serialized)

      expect(fn({ items: ['first', 'second'] })).toBe('first')
    })

    it('should return undefined for missing paths', () => {
      const serialized = JSON.stringify({
        mapperType: 'path',
        path: 'missing.property',
      })
      const fn = deserializeFunction<{ name: string }, unknown>(serialized)

      expect(fn({ name: 'test' })).toBeUndefined()
    })
  })

  describe('registered mappers (secure)', () => {
    beforeEach(() => {
      // Clear registry before each test
      clearMapperRegistry()
    })

    it('should use registered mapper', () => {
      // Register a mapper
      registerMapper('extractName', (x: any) => x.name.toUpperCase())

      const serialized = JSON.stringify({
        mapperType: 'registered',
        name: 'extractName',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(fn({ name: 'test' })).toBe('TEST')
    })

    it('should throw for unregistered mapper', () => {
      const serialized = JSON.stringify({
        mapperType: 'registered',
        name: 'nonexistent',
      })

      expect(() => deserializeFunction(serialized)).toThrow("Mapper 'nonexistent' is not registered")
    })

    it('should throw when registering duplicate mapper name', () => {
      registerMapper('duplicate', (x: any) => x)

      expect(() => registerMapper('duplicate', (x: any) => x)).toThrow("Mapper 'duplicate' is already registered")
    })
  })

  describe('security validation', () => {
    it('should block eval', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => eval(x.code)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block new Function', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => new Function(x.code)()',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block fetch', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => fetch(x.url)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block process', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => process.env.SECRET',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block globalThis', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => globalThis.fetch(x)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block import', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => import(x.module)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block require', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => require(x.module)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block constructor access', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.constructor.constructor("return this")()',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block __proto__ access', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.__proto__.polluted = true',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should block setTimeout', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => setTimeout(() => {}, 0)',
      })

      expect(() => deserializeFunction(serialized)).toThrow('Unsafe function pattern detected')
    })

    it('should allow safe built-in methods like slice', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.name.slice(0, 3)',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(fn({ name: 'testing' })).toBe('tes')
    })

    it('should allow safe built-in methods like toLowerCase', () => {
      const serialized = JSON.stringify({
        type: 'sync',
        body: '(x) => x.name.toLowerCase()',
      })
      const fn = deserializeFunction<{ name: string }, string>(serialized)

      expect(fn({ name: 'TEST' })).toBe('test')
    })
  })
})

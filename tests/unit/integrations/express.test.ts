/**
 * Express Integration Tests
 *
 * Comprehensive tests for the ParqueDB Express middleware adapter.
 * Tests middleware creation, request handling, error mapping, lifecycle hooks,
 * and shared singleton behavior.
 *
 * Heavy transitive dependencies (ParqueDB, DB, FsBackend) are mocked to avoid
 * long module resolution times in the test environment.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

// ---------------------------------------------------------------------------
// Mocks – vi.mock calls are hoisted, so factories must be self-contained
// ---------------------------------------------------------------------------

vi.mock('../../../src/ParqueDB', () => {
  class ParqueDB {
    _config: Record<string, unknown>
    constructor(config: Record<string, unknown> = {}) {
      this._config = config
    }
    collection(ns: string) {
      return {
        create: async (data: Record<string, unknown>) => ({
          $id: `${ns}/mock-id`,
          $type: 'Mock',
          name: data.name ?? 'mock',
          ...data,
        }),
        find: async () => [],
        get: async () => null,
      }
    }
  }
  return { ParqueDB }
})

vi.mock('../../../src/storage', () => {
  class MemoryBackend {
    _data = new Map<string, ArrayBuffer>()
    name = 'memory'
  }
  class FsBackend {
    _basePath: string
    name = 'fs'
    constructor(basePath: string) {
      this._basePath = basePath
    }
  }
  return { MemoryBackend, FsBackend }
})

vi.mock('../../../src/db', () => {
  // Import the already-mocked ParqueDB to reuse in DB factory
  // At runtime the mock is already installed
  return {
    DB: (_schema: Record<string, unknown>, opts?: Record<string, unknown>) => {
      // Inline a lightweight mock – returns same shape as ParqueDB mock
      return {
        _config: opts ?? {},
        collection(ns: string) {
          return {
            create: async (data: Record<string, unknown>) => ({
              $id: `${ns}/mock-id`,
              $type: 'Mock',
              name: data.name ?? 'mock',
              ...data,
            }),
            find: async () => [],
            get: async () => null,
          }
        },
      }
    },
  }
})

// ---------------------------------------------------------------------------
// Imports – these now resolve against the mocked modules
// ---------------------------------------------------------------------------

import {
  createParqueDBMiddleware,
  createErrorMiddleware,
  getSharedDB,
  resetSharedDB,
  ParqueDB,
  type ParqueDBMiddlewareOptions,
} from '../../../src/integrations/express'
import { MemoryBackend } from '../../../src/storage'

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

function createMockRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    method: 'GET',
    url: '/',
    headers: {},
    params: {},
    query: {},
    body: {},
    ...overrides,
  }
}

function createMockResponse(): Record<string, unknown> & {
  statusCode: number
  _json: unknown
  _ended: boolean
} {
  const res: Record<string, unknown> & {
    statusCode: number
    _json: unknown
    _ended: boolean
  } = {
    statusCode: 200,
    _json: null,
    _ended: false,
    status: vi.fn((code: number) => {
      res.statusCode = code
      return {
        json: vi.fn((data: unknown) => {
          res._json = data
        }),
      }
    }),
    json: vi.fn((data: unknown) => {
      res._json = data
    }),
    end: vi.fn((..._args: unknown[]) => {
      res._ended = true
    }),
  }
  return res
}

function createMockNext(): ((error?: Error) => void) & { mock: { calls: unknown[][] } } {
  return vi.fn() as ((error?: Error) => void) & { mock: { calls: unknown[][] } }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Express Integration', () => {
  beforeEach(() => {
    resetSharedDB()
  })

  afterEach(() => {
    resetSharedDB()
  })

  // =========================================================================
  // createParqueDBMiddleware
  // =========================================================================

  describe('createParqueDBMiddleware', () => {
    describe('middleware creation', () => {
      it('should create middleware function', () => {
        const middleware = createParqueDBMiddleware({})

        expect(typeof middleware).toBe('function')
        expect(middleware.length).toBe(3) // (req, res, next)
      })

      it('should create middleware with default options', async () => {
        const middleware = createParqueDBMiddleware()
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.db).toBeDefined()
        expect(next).toHaveBeenCalledWith()
      })

      it('should create middleware with custom storage backend', async () => {
        const storage = new MemoryBackend()
        const middleware = createParqueDBMiddleware({ storage: storage as any })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.db).toBeInstanceOf(ParqueDB)
        expect(next).toHaveBeenCalledWith()
      })

      it('should create middleware with basePath option', async () => {
        const middleware = createParqueDBMiddleware({ basePath: './test-data' })

        expect(typeof middleware).toBe('function')
      })

      it('should create middleware with schema option', async () => {
        const schema = {
          types: {
            User: {
              fields: {
                name: { type: 'string' },
                email: { type: 'string' },
              },
            },
          },
        }
        const middleware = createParqueDBMiddleware({ schema })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.db).toBeDefined()
      })

      it('should create middleware with dbSchema option', async () => {
        const middleware = createParqueDBMiddleware({
          dbSchema: {
            users: { name: 'string!', email: 'string!' },
            posts: { title: 'string!', content: 'text' },
          },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.db).toBeDefined()
      })
    })

    describe('request attachment', () => {
      it('should attach db to request with default property name', async () => {
        const middleware = createParqueDBMiddleware({})
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.db).toBeDefined()
        expect(req.db).toBeInstanceOf(ParqueDB)
      })

      it('should attach db to request with custom property name', async () => {
        const middleware = createParqueDBMiddleware({ propertyName: 'parquedb' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(req.parquedb).toBeDefined()
        expect(req.db).toBeUndefined()
      })

      it('should call next() on successful attachment', async () => {
        const middleware = createParqueDBMiddleware({})
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(next).toHaveBeenCalledTimes(1)
        expect(next).toHaveBeenCalledWith()
      })

      it('should attach same db instance to multiple requests', async () => {
        const middleware = createParqueDBMiddleware({})
        const req1 = createMockRequest()
        const req2 = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req1, res, next)
        await middleware(req2, res, next)

        expect(req1.db).toBe(req2.db)
      })
    })

    describe('lifecycle hooks', () => {
      it('should call onInit hook during initialization', async () => {
        const onInit = vi.fn()
        const middleware = createParqueDBMiddleware({
          hooks: { onInit },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(onInit).toHaveBeenCalledTimes(1)
        expect(onInit).toHaveBeenCalledWith(expect.any(ParqueDB))
      })

      it('should call async onInit hook', async () => {
        const onInit = vi.fn().mockResolvedValue(undefined)
        const middleware = createParqueDBMiddleware({
          hooks: { onInit },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(onInit).toHaveBeenCalledTimes(1)
      })

      it('should call onInit only once for multiple requests', async () => {
        const onInit = vi.fn()
        const options: ParqueDBMiddlewareOptions = {
          hooks: { onInit },
        }
        const middleware = createParqueDBMiddleware(options)
        const next = createMockNext()

        await middleware(createMockRequest(), createMockResponse(), next)
        await middleware(createMockRequest(), createMockResponse(), next)
        await middleware(createMockRequest(), createMockResponse(), next)

        expect(onInit).toHaveBeenCalledTimes(1)
      })

      it('should call onRequest hook for each request', async () => {
        const onRequest = vi.fn()
        const middleware = createParqueDBMiddleware({
          hooks: { onRequest },
        })
        const req1 = createMockRequest()
        const req2 = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req1, res, next)
        await middleware(req2, res, next)

        expect(onRequest).toHaveBeenCalledTimes(2)
        expect(onRequest).toHaveBeenCalledWith(expect.any(ParqueDB), req1)
        expect(onRequest).toHaveBeenCalledWith(expect.any(ParqueDB), req2)
      })

      it('should call onResponse hook when response ends', async () => {
        const onResponse = vi.fn()
        const middleware = createParqueDBMiddleware({
          hooks: { onResponse },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        // Simulate response ending
        ;(res.end as () => void)()

        // Allow for async execution
        await new Promise(resolve => setTimeout(resolve, 10))

        expect(onResponse).toHaveBeenCalledWith(expect.any(ParqueDB), req, res)
      })

      it('should call onError hook when error occurs', async () => {
        const onError = vi.fn()
        const onRequest = vi.fn().mockRejectedValue(new Error('Request hook error'))
        const middleware = createParqueDBMiddleware({
          hooks: { onRequest, onError },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(onError).toHaveBeenCalledWith(expect.any(Error), expect.any(ParqueDB))
        expect(next).toHaveBeenCalledWith(expect.any(Error))
      })
    })

    describe('error handling', () => {
      it('should pass errors to next()', async () => {
        const middleware = createParqueDBMiddleware({
          hooks: {
            onRequest: vi.fn().mockRejectedValue(new Error('Test error')),
          },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        await middleware(req, res, next)

        expect(next).toHaveBeenCalledWith(expect.any(Error))
        expect(next.mock.calls[0]?.[0]).toBeInstanceOf(Error)
      })

      it('should swallow errors in onError hook', async () => {
        const middleware = createParqueDBMiddleware({
          hooks: {
            onRequest: vi.fn().mockRejectedValue(new Error('Request error')),
            onError: vi.fn().mockRejectedValue(new Error('Error hook error')),
          },
        })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        // Should not throw even if error hook fails
        await middleware(req, res, next)

        expect(next).toHaveBeenCalledWith(expect.any(Error))
      })
    })
  })

  // =========================================================================
  // createErrorMiddleware
  // =========================================================================

  describe('createErrorMiddleware', () => {
    describe('error mapping', () => {
      it('should map NOT_FOUND error to 404', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Entity not found'), { code: 'NOT_FOUND' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(404)
        expect(res._json).toEqual({
          error: {
            code: 'NOT_FOUND',
            message: 'Entity not found',
          },
        })
      })

      it('should map ENTITY_NOT_FOUND error to 404', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Entity not found'), { code: 'ENTITY_NOT_FOUND' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(404)
      })

      it('should map VALIDATION_ERROR to 400', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Invalid data'), { code: 'VALIDATION_ERROR' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(400)
        expect(res._json).toEqual({
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Invalid data',
          },
        })
      })

      it('should map CONFLICT error to 409', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Conflict detected'), { code: 'CONFLICT' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(409)
      })

      it('should map VERSION_CONFLICT error to 409', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Version mismatch'), { code: 'VERSION_CONFLICT' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(409)
      })

      it('should map UNAUTHORIZED error to 403', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Access denied'), { code: 'UNAUTHORIZED' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(403)
      })

      it('should map PERMISSION_DENIED error to 403', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Permission denied'), { code: 'PERMISSION_DENIED' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(403)
      })

      it('should use status from error if provided', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Custom error'), { code: 'CUSTOM', status: 418 })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(418)
      })

      it('should default to 500 for unknown error codes', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = Object.assign(new Error('Unknown error'), { code: 'UNKNOWN_CODE' })
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(res.statusCode).toBe(500)
      })

      it('should pass non-ParqueDB errors to next', () => {
        const errorMiddleware = createErrorMiddleware()
        const error = new Error('Generic error') // No code property
        const req = createMockRequest()
        const res = createMockResponse()
        const next = createMockNext()

        errorMiddleware(error, req, res, next)

        expect(next).toHaveBeenCalledWith(error)
      })
    })
  })

  // =========================================================================
  // shared DB singleton
  // =========================================================================

  describe('shared DB singleton', () => {
    it('should return null before middleware initialization', () => {
      const db = getSharedDB()

      expect(db).toBeNull()
    })

    it('should return shared DB after middleware initialization', async () => {
      const middleware = createParqueDBMiddleware({})
      const req = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware(req, res, next)

      const sharedDb = getSharedDB()
      expect(sharedDb).toBeInstanceOf(ParqueDB)
      expect(sharedDb).toBe(req.db)
    })

    it('should reset shared DB with resetSharedDB()', async () => {
      const middleware = createParqueDBMiddleware({})
      const req = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware(req, res, next)

      expect(getSharedDB()).not.toBeNull()

      resetSharedDB()

      expect(getSharedDB()).toBeNull()
    })

    it('should create new instance after reset', async () => {
      const middleware1 = createParqueDBMiddleware({})
      const req1 = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware1(req1, res, next)
      const firstDb = getSharedDB()

      resetSharedDB()

      const middleware2 = createParqueDBMiddleware({})
      const req2 = createMockRequest()

      await middleware2(req2, res, next)
      const secondDb = getSharedDB()

      expect(firstDb).not.toBe(secondDb)
    })
  })

  // =========================================================================
  // Integration scenarios
  // =========================================================================

  describe('integration scenarios', () => {
    it('should work with full request/response cycle', async () => {
      const storage = new MemoryBackend()
      const middleware = createParqueDBMiddleware({ storage: storage as any })
      const req = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware(req, res, next)

      // Verify db is accessible and functional
      const db = req.db as InstanceType<typeof ParqueDB>
      expect(db).toBeInstanceOf(ParqueDB)

      // Test basic operations via collection proxy
      const result = await db.collection('test').create({
        $type: 'Test',
        name: 'test-item',
        value: 42,
      })

      expect(result.$id).toBeDefined()
      expect(result.name).toBe('test-item')
    })

    it('should handle multiple middlewares with same options', async () => {
      const options: ParqueDBMiddlewareOptions = { storage: new MemoryBackend() as any }
      const middleware1 = createParqueDBMiddleware(options)
      const middleware2 = createParqueDBMiddleware(options)

      const req1 = createMockRequest()
      const req2 = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware1(req1, res, next)
      await middleware2(req2, res, next)

      // Same options object means same db instance
      expect(req1.db).toBe(req2.db)
    })

    it('should create different instances for different options', async () => {
      resetSharedDB()

      const middleware1 = createParqueDBMiddleware({ storage: new MemoryBackend() as any })
      const req1 = createMockRequest()
      await middleware1(req1, createMockResponse(), createMockNext())
      const db1 = req1.db

      resetSharedDB()

      const middleware2 = createParqueDBMiddleware({ storage: new MemoryBackend() as any })
      const req2 = createMockRequest()
      await middleware2(req2, createMockResponse(), createMockNext())
      const db2 = req2.db

      expect(db1).not.toBe(db2)
    })

    it('should support defaultNamespace option', async () => {
      const middleware = createParqueDBMiddleware({ defaultNamespace: 'myapp' })
      const req = createMockRequest()
      const res = createMockResponse()
      const next = createMockNext()

      await middleware(req, res, next)

      expect(req.db).toBeDefined()
    })

    it('should handle concurrent requests correctly', async () => {
      const middleware = createParqueDBMiddleware({})
      const requests = Array.from({ length: 5 }, (_, i) =>
        createMockRequest({ url: `/route-${i}` })
      )
      const res = createMockResponse()

      const results = await Promise.all(
        requests.map(async (req) => {
          const next = createMockNext()
          await middleware(req, res, next)
          return { db: req.db, nextCalled: next.mock.calls.length > 0 }
        })
      )

      // All requests get the same db instance
      const firstDb = results[0].db
      for (const r of results) {
        expect(r.db).toBe(firstDb)
        expect(r.nextCalled).toBe(true)
      }
    })
  })
})

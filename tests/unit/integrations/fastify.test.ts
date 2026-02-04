/**
 * Fastify Integration Tests
 *
 * Comprehensive tests for the ParqueDB Fastify plugin adapter.
 * Tests plugin registration, decorators, lifecycle hooks, error handling,
 * and cleanup behavior.
 *
 * Heavy transitive dependencies (ParqueDB, DB, FsBackend) are mocked to avoid
 * long module resolution times in the test environment.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

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
  return {
    DB: (_schema: Record<string, unknown>, opts?: Record<string, unknown>) => {
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
  parquedbPlugin,
  parquedbErrorHandler,
  createParqueDBHook,
  ParqueDB,
  type ParqueDBPluginOptions,
  type FastifyInstance,
} from '../../../src/integrations/fastify'
import { MemoryBackend } from '../../../src/storage'

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

function createMockFastify(): FastifyInstance & {
  _decorators: Map<string, unknown>
  _requestDecorators: Map<string, unknown>
  _hooks: Map<string, ((...args: unknown[]) => unknown)[]>
} {
  const decorators = new Map<string, unknown>()
  const requestDecorators = new Map<string, unknown>()
  const hooks = new Map<string, ((...args: unknown[]) => unknown)[]>()

  return {
    _decorators: decorators,
    _requestDecorators: requestDecorators,
    _hooks: hooks,

    decorate: vi.fn((name: string, value: unknown) => {
      decorators.set(name, value)
    }),

    decorateRequest: vi.fn((name: string, value: unknown) => {
      requestDecorators.set(name, value)
    }),

    hasDecorator: vi.fn((name: string) => {
      return decorators.has(name)
    }),

    hasRequestDecorator: vi.fn((name: string) => {
      return requestDecorators.has(name)
    }),

    addHook: vi.fn((name: string, handler: (...args: unknown[]) => unknown) => {
      if (!hooks.has(name)) {
        hooks.set(name, [])
      }
      hooks.get(name)!.push(handler)
    }),

    get db(): unknown {
      return decorators.get('db')
    },
  }
}

function createMockRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    method: 'GET',
    url: '/',
    headers: {},
    params: {},
    query: {},
    body: null,
    ...overrides,
  }
}

function createMockReply(): Record<string, unknown> & {
  statusCode: number
  _sent: unknown
} {
  const reply: Record<string, unknown> & {
    statusCode: number
    _sent: unknown
  } = {
    statusCode: 200,
    _sent: null,
    code: vi.fn((statusCode: number) => {
      reply.statusCode = statusCode
      return {
        send: vi.fn((data: unknown) => {
          reply._sent = data
        }),
      }
    }),
    send: vi.fn((data: unknown) => {
      reply._sent = data
    }),
  }
  return reply
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Fastify Integration', () => {
  // =========================================================================
  // parquedbPlugin
  // =========================================================================

  describe('parquedbPlugin', () => {
    describe('plugin registration', () => {
      it('should register plugin successfully', async () => {
        const fastify = createMockFastify()
        const options: ParqueDBPluginOptions = {}

        await parquedbPlugin(fastify, options)

        expect(fastify.hasDecorator('db')).toBe(true)
      })

      it('should decorate fastify instance with db', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {})

        expect(fastify.decorate).toHaveBeenCalledWith('db', expect.any(Object))
        expect(fastify._decorators.get('db')).toBeInstanceOf(ParqueDB)
      })

      it('should use custom decorator name', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { decoratorName: 'parquedb' })

        expect(fastify.decorate).toHaveBeenCalledWith('parquedb', expect.any(Object))
        expect(fastify._decorators.get('parquedb')).toBeInstanceOf(ParqueDB)
      })

      it('should throw if plugin already registered', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {})

        await expect(parquedbPlugin(fastify, {})).rejects.toThrow(
          "ParqueDB plugin already registered with decorator name 'db'"
        )
      })

      it('should throw with custom name if already registered', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { decoratorName: 'mydb' })

        await expect(parquedbPlugin(fastify, { decoratorName: 'mydb' })).rejects.toThrow(
          "ParqueDB plugin already registered with decorator name 'mydb'"
        )
      })
    })

    describe('request decorator', () => {
      it('should decorate request with db by default', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {})

        expect(fastify.decorateRequest).toHaveBeenCalledWith('db', expect.any(Object))
      })

      it('should skip request decoration when decorateRequest is false', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { decorateRequest: false })

        expect(fastify.decorateRequest).not.toHaveBeenCalled()
      })

      it('should use custom decorator name for request', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { decoratorName: 'parquedb' })

        expect(fastify.decorateRequest).toHaveBeenCalledWith('parquedb', expect.any(Object))
      })

      it('should not re-decorate request if already decorated', async () => {
        const fastify = createMockFastify()
        fastify._requestDecorators.set('db', {}) // Pre-existing decorator

        await parquedbPlugin(fastify, {})

        // Should still register the main decorator on the instance
        expect(fastify._decorators.get('db')).toBeInstanceOf(ParqueDB)
      })
    })

    describe('storage backends', () => {
      it('should use provided storage backend', async () => {
        const fastify = createMockFastify()
        const storage = new MemoryBackend()

        await parquedbPlugin(fastify, { storage: storage as any })

        expect(fastify._decorators.get('db')).toBeInstanceOf(ParqueDB)
      })

      it('should use MemoryBackend when no storage provided', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {})

        expect(fastify._decorators.get('db')).toBeInstanceOf(ParqueDB)
      })

      it('should accept basePath option', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { basePath: './test-data' })

        expect(fastify._decorators.has('db')).toBe(true)
      })
    })

    describe('schema options', () => {
      it('should accept schema option', async () => {
        const fastify = createMockFastify()
        const schema = {
          types: {
            User: {
              fields: {
                name: { type: 'string' },
              },
            },
          },
        }

        await parquedbPlugin(fastify, { schema })

        expect(fastify._decorators.get('db')).toBeInstanceOf(ParqueDB)
      })

      it('should accept dbSchema option', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {
          dbSchema: {
            users: { name: 'string!', email: 'string!' },
            posts: { title: 'string!', content: 'text' },
          },
        })

        expect(fastify._decorators.get('db')).toBeDefined()
      })

      it('should accept defaultNamespace option', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, { defaultNamespace: 'myapp' })

        expect(fastify._decorators.get('db')).toBeDefined()
      })
    })

    describe('lifecycle hooks', () => {
      it('should call onInit hook during registration', async () => {
        const fastify = createMockFastify()
        const onInit = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onInit },
        })

        expect(onInit).toHaveBeenCalledTimes(1)
        expect(onInit).toHaveBeenCalledWith(expect.any(ParqueDB))
      })

      it('should call async onInit hook', async () => {
        const fastify = createMockFastify()
        const onInit = vi.fn().mockResolvedValue(undefined)

        await parquedbPlugin(fastify, {
          hooks: { onInit },
        })

        expect(onInit).toHaveBeenCalledTimes(1)
      })

      it('should register onRequest hook when provided', async () => {
        const fastify = createMockFastify()
        const onRequest = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onRequest },
        })

        expect(fastify.addHook).toHaveBeenCalledWith('onRequest', expect.any(Function))
        expect(fastify._hooks.get('onRequest')?.length).toBe(1)
      })

      it('should call onRequest hook with db and request', async () => {
        const fastify = createMockFastify()
        const onRequest = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onRequest },
        })

        // Simulate hook execution
        const hookFn = fastify._hooks.get('onRequest')?.[0]
        const mockRequest = createMockRequest()

        await hookFn?.(mockRequest)

        expect(onRequest).toHaveBeenCalledWith(expect.any(ParqueDB), mockRequest)
      })

      it('should register onResponse hook when provided', async () => {
        const fastify = createMockFastify()
        const onResponse = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onResponse },
        })

        expect(fastify.addHook).toHaveBeenCalledWith('onResponse', expect.any(Function))
        expect(fastify._hooks.get('onResponse')?.length).toBe(1)
      })

      it('should call onResponse hook with db, request, and reply', async () => {
        const fastify = createMockFastify()
        const onResponse = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onResponse },
        })

        // Simulate hook execution
        const hookFn = fastify._hooks.get('onResponse')?.[0]
        const mockRequest = createMockRequest()
        const mockReply = createMockReply()

        await hookFn?.(mockRequest, mockReply)

        expect(onResponse).toHaveBeenCalledWith(expect.any(ParqueDB), mockRequest, mockReply)
      })

      it('should register onError hook when provided', async () => {
        const fastify = createMockFastify()
        const onError = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onError },
        })

        expect(fastify.addHook).toHaveBeenCalledWith('onError', expect.any(Function))
        expect(fastify._hooks.get('onError')?.length).toBe(1)
      })

      it('should call onError hook with error and db', async () => {
        const fastify = createMockFastify()
        const onError = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onError },
        })

        // Simulate hook execution (error is 3rd argument in Fastify onError)
        const hookFn = fastify._hooks.get('onError')?.[0]
        const testError = new Error('Test error')

        await hookFn?.(createMockRequest(), createMockReply(), testError)

        expect(onError).toHaveBeenCalledWith(testError, expect.any(ParqueDB))
      })

      it('should register onClose hook when provided', async () => {
        const fastify = createMockFastify()
        const onClose = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onClose },
        })

        expect(fastify.addHook).toHaveBeenCalledWith('onClose', expect.any(Function))
        expect(fastify._hooks.get('onClose')?.length).toBe(1)
      })

      it('should call onClose hook with db', async () => {
        const fastify = createMockFastify()
        const onClose = vi.fn()

        await parquedbPlugin(fastify, {
          hooks: { onClose },
        })

        // Simulate hook execution
        const hookFn = fastify._hooks.get('onClose')?.[0]
        await hookFn?.()

        expect(onClose).toHaveBeenCalledWith(expect.any(ParqueDB))
      })

      it('should not register hooks when none are provided', async () => {
        const fastify = createMockFastify()

        await parquedbPlugin(fastify, {})

        expect(fastify._hooks.size).toBe(0)
      })
    })

    describe('plugin metadata', () => {
      it('should have skip-override symbol set', () => {
        const skipOverride = (parquedbPlugin as unknown as { [key: symbol]: unknown })[
          Symbol.for('skip-override')
        ]
        expect(skipOverride).toBe(true)
      })

      it('should have fastify-plugin metadata', () => {
        const metadata = (parquedbPlugin as unknown as { [key: string]: unknown })['@@fastify-plugin']
        expect(metadata).toEqual({
          name: 'parquedb',
          fastify: '>=4.0.0',
        })
      })
    })
  })

  // =========================================================================
  // parquedbErrorHandler
  // =========================================================================

  describe('parquedbErrorHandler', () => {
    describe('error mapping', () => {
      it('should map NOT_FOUND error to 404', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Not found'), { code: 'NOT_FOUND' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(404)
        expect(reply._sent).toEqual({
          error: {
            code: 'NOT_FOUND',
            message: 'Not found',
          },
        })
      })

      it('should map ENTITY_NOT_FOUND error to 404', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Entity not found'), { code: 'ENTITY_NOT_FOUND' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(404)
      })

      it('should map VALIDATION_ERROR to 400', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Invalid input'), { code: 'VALIDATION_ERROR' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(400)
      })

      it('should map CONFLICT error to 409', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Conflict'), { code: 'CONFLICT' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(409)
      })

      it('should map VERSION_CONFLICT error to 409', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Version mismatch'), { code: 'VERSION_CONFLICT' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(409)
      })

      it('should map UNAUTHORIZED error to 403', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Unauthorized'), { code: 'UNAUTHORIZED' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(403)
      })

      it('should map PERMISSION_DENIED error to 403', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Permission denied'), { code: 'PERMISSION_DENIED' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(403)
      })

      it('should use status from error if provided', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Custom'), { code: 'CUSTOM', status: 418 })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(418)
      })

      it('should default to 500 for unknown error codes', async () => {
        const fastify = createMockFastify()

        await parquedbErrorHandler(fastify)

        const hookFn = fastify._hooks.get('onError')?.[0]
        const error = Object.assign(new Error('Unknown'), { code: 'UNKNOWN_ERROR' })
        const reply = createMockReply()

        await hookFn?.(createMockRequest(), reply, error)

        expect(reply.statusCode).toBe(500)
      })
    })

    describe('plugin metadata', () => {
      it('should have skip-override symbol set', () => {
        const skipOverride = (parquedbErrorHandler as unknown as { [key: symbol]: unknown })[
          Symbol.for('skip-override')
        ]
        expect(skipOverride).toBe(true)
      })

      it('should have fastify-plugin metadata', () => {
        const metadata = (parquedbErrorHandler as unknown as { [key: string]: unknown })['@@fastify-plugin']
        expect(metadata).toEqual({
          name: 'parquedb-error-handler',
          fastify: '>=4.0.0',
        })
      })
    })
  })

  // =========================================================================
  // createParqueDBHook
  // =========================================================================

  describe('createParqueDBHook', () => {
    it('should return db and onRequest hook', () => {
      const result = createParqueDBHook({})

      expect(result.db).toBeInstanceOf(ParqueDB)
      expect(typeof result.onRequest).toBe('function')
    })

    it('should use provided storage backend', () => {
      const storage = new MemoryBackend()
      const result = createParqueDBHook({ storage: storage as any })

      expect(result.db).toBeInstanceOf(ParqueDB)
    })

    it('should use MemoryBackend when no storage provided', () => {
      const result = createParqueDBHook({})

      expect(result.db).toBeInstanceOf(ParqueDB)
    })

    it('should attach db to request via onRequest', () => {
      const result = createParqueDBHook({})
      const request: Record<string, unknown> = {}

      result.onRequest(request)

      expect(request.db).toBe(result.db)
    })

    it('should use custom decorator name', () => {
      const result = createParqueDBHook({ decoratorName: 'parquedb' })
      const request: Record<string, unknown> = {}

      result.onRequest(request)

      expect(request.parquedb).toBe(result.db)
      expect(request.db).toBeUndefined()
    })

    it('should accept basePath option', () => {
      const result = createParqueDBHook({ basePath: './test-data' })

      expect(result.db).toBeDefined()
    })

    it('should accept schema option', () => {
      const result = createParqueDBHook({
        schema: {
          types: {
            User: { fields: { name: { type: 'string' } } },
          },
        },
      })

      expect(result.db).toBeDefined()
    })

    it('should accept dbSchema option', () => {
      const result = createParqueDBHook({
        dbSchema: {
          users: { name: 'string!', email: 'string!' },
        },
      })

      expect(result.db).toBeDefined()
    })

    it('should accept defaultNamespace option', () => {
      const result = createParqueDBHook({ defaultNamespace: 'myapp' })

      expect(result.db).toBeDefined()
    })
  })

  // =========================================================================
  // Integration scenarios
  // =========================================================================

  describe('integration scenarios', () => {
    it('should support full plugin lifecycle', async () => {
      const fastify = createMockFastify()
      const onInit = vi.fn()
      const onClose = vi.fn()

      await parquedbPlugin(fastify, {
        storage: new MemoryBackend() as any,
        hooks: { onInit, onClose },
      })

      // Verify initialization
      expect(onInit).toHaveBeenCalled()

      // Get db and perform operations
      const db = fastify._decorators.get('db') as InstanceType<typeof ParqueDB>
      const entity = await db.collection('test').create({
        $type: 'Test',
        name: 'test-item',
      })

      expect(entity.$id).toBeDefined()

      // Simulate close
      const closeHook = fastify._hooks.get('onClose')?.[0]
      await closeHook?.()

      expect(onClose).toHaveBeenCalled()
    })

    it('should work with both main plugin and error handler', async () => {
      const fastify = createMockFastify()

      await parquedbPlugin(fastify, { storage: new MemoryBackend() as any })
      await parquedbErrorHandler(fastify)

      // Both plugins registered
      expect(fastify._decorators.has('db')).toBe(true)
      expect(fastify._hooks.get('onError')?.length).toBe(1)
    })

    it('should handle request/response cycle with hooks', async () => {
      const fastify = createMockFastify()
      const requestLog: string[] = []

      await parquedbPlugin(fastify, {
        storage: new MemoryBackend() as any,
        hooks: {
          onInit: () => {
            requestLog.push('init')
          },
          onRequest: () => {
            requestLog.push('request')
          },
          onResponse: () => {
            requestLog.push('response')
          },
        },
      })

      // Simulate request
      const onRequestHook = fastify._hooks.get('onRequest')?.[0]
      await onRequestHook?.(createMockRequest())

      // Simulate response
      const onResponseHook = fastify._hooks.get('onResponse')?.[0]
      await onResponseHook?.(createMockRequest(), createMockReply())

      expect(requestLog).toEqual(['init', 'request', 'response'])
    })

    it('should handle error/close lifecycle correctly', async () => {
      const fastify = createMockFastify()
      const lifecycleLog: string[] = []

      await parquedbPlugin(fastify, {
        hooks: {
          onError: () => {
            lifecycleLog.push('error')
          },
          onClose: () => {
            lifecycleLog.push('close')
          },
        },
      })

      // Simulate error
      const onErrorHook = fastify._hooks.get('onError')?.[0]
      await onErrorHook?.(createMockRequest(), createMockReply(), new Error('test'))

      // Simulate close
      const onCloseHook = fastify._hooks.get('onClose')?.[0]
      await onCloseHook?.()

      expect(lifecycleLog).toEqual(['error', 'close'])
    })
  })
})

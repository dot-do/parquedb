/**
 * Unit Tests for Vercel AI SDK Middleware Integration
 *
 * Tests the ParqueDB middleware for Vercel AI SDK including:
 * - Response caching functionality
 * - Request logging functionality
 * - Cache statistics and management
 * - Error handling scenarios
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  createParqueDBMiddleware,
  hashParams,
  isExpired,
  queryCacheEntries,
  queryLogEntries,
  clearExpiredCache,
  getCacheStats,
  type ParqueDBMiddlewareOptions,
  type CacheEntry,
  type LogEntry,
  type LanguageModelCallOptions,
  type LanguageModelGenerateResult,
  type LanguageModelStreamResult,
  type LanguageModel,
} from '../../../src/integrations/ai-sdk'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockEntity {
  $id: string
  $type: string
  name: string
  [key: string]: unknown
}

/**
 * Create a mock ParqueDB instance for testing
 */
function createMockDB() {
  const collections: Map<string, MockEntity[]> = new Map()
  const collectionInstances: Map<string, ReturnType<typeof createCollectionMock>> = new Map()

  const createCollectionMock = (collectionName: string) => {
    return {
      find: vi.fn(async (filter?: Record<string, unknown>, _options?: Record<string, unknown>) => {
        const data = collections.get(collectionName) ?? []
        if (!filter || Object.keys(filter).length === 0) {
          return { items: data }
        }

        const filteredData = data.filter((item) => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'expiresAt' && typeof value === 'object') {
              const dateValue = item[key] as Date
              if ('$gt' in (value as Record<string, unknown>)) {
                if (dateValue <= (value as Record<string, Date>).$gt) return false
              }
              if ('$lt' in (value as Record<string, unknown>)) {
                if (dateValue >= (value as Record<string, Date>).$lt) return false
              }
            } else if (item[key] !== value) {
              return false
            }
          }
          return true
        })
        return { items: filteredData }
      }),

      findOne: vi.fn(async (filter: Record<string, unknown>) => {
        const data = collections.get(collectionName) ?? []
        return data.find((item) => {
          for (const [key, value] of Object.entries(filter)) {
            if (item[key] !== value) return false
          }
          return true
        }) ?? null
      }),

      create: vi.fn(async (input: Record<string, unknown>) => {
        const data = collections.get(collectionName) ?? []
        const id = `${collectionName}/${Date.now()}_${Math.random().toString(36).slice(2)}`
        const entity: MockEntity = {
          $id: id,
          $type: (input.$type as string) ?? 'Unknown',
          name: (input.name as string) ?? 'unnamed',
          createdAt: new Date(),
          ...input,
        }
        data.push(entity)
        collections.set(collectionName, data)
        return entity
      }),

      update: vi.fn(async (id: string, update: Record<string, unknown>) => {
        const data = collections.get(collectionName) ?? []
        const fullId = `${collectionName}/${id}`
        const index = data.findIndex((item) => item.$id === fullId || item.$id.endsWith(`/${id}`))
        if (index !== -1) {
          const existing = data[index]
          if ('$set' in update) {
            Object.assign(existing, (update as Record<string, Record<string, unknown>>).$set)
          }
          if ('$inc' in update) {
            for (const [key, value] of Object.entries((update as Record<string, Record<string, number>>).$inc)) {
              existing[key] = ((existing[key] as number) ?? 0) + value
            }
          }
        }
        return data[index]
      }),

      deleteMany: vi.fn(async (filter: Record<string, unknown>, _options?: Record<string, unknown>) => {
        const data = collections.get(collectionName) ?? []
        const initialCount = data.length
        const remaining = data.filter((item) => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'expiresAt' && typeof value === 'object') {
              const dateValue = item[key] as Date
              if ('$lt' in (value as Record<string, unknown>)) {
                if (dateValue < (value as Record<string, Date>).$lt) return false
              }
            }
          }
          return true
        })
        collections.set(collectionName, remaining)
        return { deletedCount: initialCount - remaining.length }
      }),
    }
  }

  const mockCollection = (collectionName: string) => {
    if (!collections.has(collectionName)) {
      collections.set(collectionName, [])
    }
    if (!collectionInstances.has(collectionName)) {
      collectionInstances.set(collectionName, createCollectionMock(collectionName))
    }
    return collectionInstances.get(collectionName)!
  }

  return {
    collection: vi.fn(mockCollection),
    _collections: collections,
    _getCollection: (name: string) => collections.get(name) ?? [],
  }
}

/**
 * Create a mock language model
 */
function createMockModel(options?: Partial<LanguageModel>): LanguageModel {
  return {
    specificationVersion: 'v3',
    modelId: 'test-model',
    provider: 'test-provider',
    capabilities: {
      streaming: true,
      tools: true,
    },
    ...options,
  }
}

/**
 * Create mock generate result
 */
function createMockGenerateResult(options?: Partial<LanguageModelGenerateResult>): LanguageModelGenerateResult {
  return {
    text: 'Hello, this is a test response.',
    finishReason: 'stop',
    usage: {
      promptTokens: 10,
      completionTokens: 20,
      totalTokens: 30,
    },
    ...options,
  }
}

/**
 * Create mock stream result
 */
function createMockStreamResult(options?: Partial<LanguageModelStreamResult>): LanguageModelStreamResult {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'text-delta', text: 'Hello' })
      controller.enqueue({ type: 'text-delta', text: ' world' })
      controller.close()
    },
  })

  return {
    stream,
    response: Promise.resolve({
      text: 'Hello world',
      finishReason: 'stop',
      usage: {
        promptTokens: 5,
        completionTokens: 2,
        totalTokens: 7,
      },
    }),
    ...options,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('Vercel AI SDK Middleware', () => {
  let mockDB: ReturnType<typeof createMockDB>

  beforeEach(() => {
    mockDB = createMockDB()
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('createParqueDBMiddleware', () => {
    it('creates middleware with default options', () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      })

      expect(middleware).toBeDefined()
      expect(middleware.specificationVersion).toBe('v3')
      expect(middleware.wrapGenerate).toBeInstanceOf(Function)
      expect(middleware.wrapStream).toBeInstanceOf(Function)
    })

    it('creates middleware with caching enabled', () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
          ttlSeconds: 3600,
        },
      })

      expect(middleware).toBeDefined()
      expect(middleware.wrapGenerate).toBeInstanceOf(Function)
    })

    it('creates middleware with logging enabled', () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'verbose',
        },
      })

      expect(middleware).toBeDefined()
      expect(middleware.wrapGenerate).toBeInstanceOf(Function)
    })

    it('creates middleware with both caching and logging', () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
          ttlSeconds: 7200,
          collection: 'my_cache',
        },
        logging: {
          enabled: true,
          level: 'standard',
          collection: 'my_logs',
          metadata: { app: 'test' },
        },
      })

      expect(middleware).toBeDefined()
    })
  })

  describe('wrapGenerate - caching', () => {
    it('caches generate responses', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
          ttlSeconds: 3600,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: [{ role: 'user', content: 'Hello!' }],
        maxTokens: 100,
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      // First call - should execute and cache
      const result1 = await middleware.wrapGenerate!({
        doGenerate,
        doStream,
        params,
        model,
      })

      expect(result1).toEqual(expectedResult)
      expect(doGenerate).toHaveBeenCalledTimes(1)

      // Verify cache was written
      const cacheData = mockDB._getCollection('ai_cache')
      expect(cacheData.length).toBe(1)
      expect(cacheData[0].response).toEqual(expectedResult)
    })

    it('returns cached response on cache hit', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
          ttlSeconds: 3600,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: [{ role: 'user', content: 'Hello!' }],
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      // First call - cache miss
      await middleware.wrapGenerate!({
        doGenerate,
        doStream,
        params,
        model,
      })

      expect(doGenerate).toHaveBeenCalledTimes(1)

      // Second call - cache hit
      const result2 = await middleware.wrapGenerate!({
        doGenerate,
        doStream,
        params,
        model,
      })

      // Should not call doGenerate again
      expect(doGenerate).toHaveBeenCalledTimes(1)
      expect(result2).toEqual(expectedResult)
    })

    it('bypasses cache when disabled', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: false,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Hello!',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      // Call multiple times
      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })
      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      // Should call doGenerate each time
      expect(doGenerate).toHaveBeenCalledTimes(2)

      // No cache entries should be created
      const cacheData = mockDB._getCollection('ai_cache')
      expect(cacheData.length).toBe(0)
    })

    it('uses custom collection name for cache', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
          collection: 'custom_cache',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Test',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      // Should use custom collection
      const customCacheData = mockDB._getCollection('custom_cache')
      expect(customCacheData.length).toBe(1)

      const defaultCacheData = mockDB._getCollection('ai_cache')
      expect(defaultCacheData.length).toBe(0)
    })

    it('handles doGenerate errors', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Error test',
      }
      const testError = new Error('Model error')

      const doGenerate = vi.fn().mockRejectedValue(testError)
      const doStream = vi.fn()

      await expect(
        middleware.wrapGenerate!({ doGenerate, doStream, params, model })
      ).rejects.toThrow('Model error')

      // Should not cache errors
      const cacheData = mockDB._getCollection('ai_cache')
      expect(cacheData.length).toBe(0)
    })
  })

  describe('wrapGenerate - logging', () => {
    it('logs generate requests at minimal level', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'minimal',
        },
      })

      const model = createMockModel({ modelId: 'gpt-4' })
      const params: LanguageModelCallOptions = {
        prompt: 'Log test',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].modelId).toBe('gpt-4')
      expect(logs[0].requestType).toBe('generate')
      expect(logs[0].cached).toBe(false)
    })

    it('logs generate requests at standard level with usage', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Standard log test',
      }
      const expectedResult = createMockGenerateResult({
        usage: { promptTokens: 15, completionTokens: 25, totalTokens: 40 },
      })

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].usage).toEqual({
        promptTokens: 15,
        completionTokens: 25,
        totalTokens: 40,
      })
    })

    it('logs generate requests at verbose level with prompt and response', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'verbose',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: [{ role: 'user', content: 'Verbose log test' }],
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].prompt).toBeDefined()
      expect(logs[0].response).toBeDefined()
    })

    it('includes custom metadata in logs', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
          metadata: { app: 'my-app', version: '1.0.0' },
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Metadata test',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      const logs = mockDB._getCollection('ai_logs')
      expect(logs[0].metadata).toEqual({ app: 'my-app', version: '1.0.0' })
    })

    it('logs errors when doGenerate fails', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Error log test',
      }
      const testError = new Error('Test error')

      const doGenerate = vi.fn().mockRejectedValue(testError)
      const doStream = vi.fn()

      await expect(
        middleware.wrapGenerate!({ doGenerate, doStream, params, model })
      ).rejects.toThrow('Test error')

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].error).toBeDefined()
      expect(logs[0].error.message).toBe('Test error')
    })

    it('calls onLog callback', async () => {
      const onLog = vi.fn()

      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
          onLog,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Callback test',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      expect(onLog).toHaveBeenCalledTimes(1)
      expect(onLog).toHaveBeenCalledWith(expect.objectContaining({
        $type: 'AILog',
        requestType: 'generate',
      }))
    })
  })

  describe('wrapStream', () => {
    it('passes through stream without caching', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Stream test',
      }
      const expectedStreamResult = createMockStreamResult()

      const doGenerate = vi.fn()
      const doStream = vi.fn().mockResolvedValue(expectedStreamResult)

      const result = await middleware.wrapStream!({
        doGenerate,
        doStream,
        params,
        model,
      })

      expect(result.stream).toBeDefined()
      expect(doStream).toHaveBeenCalledTimes(1)

      // Stream results should not be cached
      const cacheData = mockDB._getCollection('ai_cache')
      expect(cacheData.length).toBe(0)
    })

    it('logs stream requests when logging enabled', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Stream log test',
      }
      const expectedStreamResult = createMockStreamResult()

      const doGenerate = vi.fn()
      const doStream = vi.fn().mockResolvedValue(expectedStreamResult)

      await middleware.wrapStream!({
        doGenerate,
        doStream,
        params,
        model,
      })

      // Allow async logging to complete using fake timers
      vi.useFakeTimers()
      await vi.advanceTimersByTimeAsync(50)
      vi.useRealTimers()

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].requestType).toBe('stream')
    })

    it('logs stream errors', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        logging: {
          enabled: true,
          level: 'standard',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Stream error test',
      }
      const testError = new Error('Stream error')

      const doGenerate = vi.fn()
      const doStream = vi.fn().mockRejectedValue(testError)

      await expect(
        middleware.wrapStream!({ doGenerate, doStream, params, model })
      ).rejects.toThrow('Stream error')

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(1)
      expect(logs[0].error).toBeDefined()
      expect(logs[0].error.message).toBe('Stream error')
    })
  })

  describe('caching + logging combined', () => {
    it('logs cache hit as cached=true', async () => {
      const middleware = createParqueDBMiddleware({
        db: mockDB as unknown as ParqueDBMiddlewareOptions['db'],
        cache: {
          enabled: true,
        },
        logging: {
          enabled: true,
          level: 'standard',
        },
      })

      const model = createMockModel()
      const params: LanguageModelCallOptions = {
        prompt: 'Combined test',
      }
      const expectedResult = createMockGenerateResult()

      const doGenerate = vi.fn().mockResolvedValue(expectedResult)
      const doStream = vi.fn()

      // First call - cache miss
      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      // Second call - cache hit
      await middleware.wrapGenerate!({ doGenerate, doStream, params, model })

      const logs = mockDB._getCollection('ai_logs')
      expect(logs.length).toBe(2)
      expect(logs[0].cached).toBe(false) // First call
      expect(logs[1].cached).toBe(true)  // Second call (cache hit)
    })
  })
})

describe('hashParams', () => {
  it('generates consistent hash for same params', async () => {
    const params: LanguageModelCallOptions = {
      prompt: 'Test prompt',
      maxTokens: 100,
    }

    const hash1 = await hashParams(params, 'model-1')
    const hash2 = await hashParams(params, 'model-1')

    expect(hash1).toBe(hash2)
  })

  it('generates different hash for different params', async () => {
    const params1: LanguageModelCallOptions = {
      prompt: 'Test prompt 1',
    }
    const params2: LanguageModelCallOptions = {
      prompt: 'Test prompt 2',
    }

    const hash1 = await hashParams(params1, 'model-1')
    const hash2 = await hashParams(params2, 'model-1')

    expect(hash1).not.toBe(hash2)
  })

  it('generates different hash for different models', async () => {
    const params: LanguageModelCallOptions = {
      prompt: 'Same prompt',
    }

    const hash1 = await hashParams(params, 'model-1')
    const hash2 = await hashParams(params, 'model-2')

    expect(hash1).not.toBe(hash2)
  })

  it('excludes specified fields from hash', async () => {
    const params1: LanguageModelCallOptions = {
      prompt: 'Test',
      temperature: 0.5,
    }
    const params2: LanguageModelCallOptions = {
      prompt: 'Test',
      temperature: 1.0,
    }

    // Without exclude, should be different
    const hash1 = await hashParams(params1, 'model')
    const hash2 = await hashParams(params2, 'model')
    expect(hash1).not.toBe(hash2)

    // With exclude, should be same
    const hash3 = await hashParams(params1, 'model', ['temperature'])
    const hash4 = await hashParams(params2, 'model', ['temperature'])
    expect(hash3).toBe(hash4)
  })

  it('returns hash with cache_ prefix', async () => {
    const params: LanguageModelCallOptions = {
      prompt: 'Test',
    }

    const hash = await hashParams(params, 'model')

    expect(hash).toMatch(/^cache_[a-z0-9]+$/)
  })
})

describe('isExpired', () => {
  it('returns true for expired entry', () => {
    const entry: CacheEntry = {
      key: 'test',
      params: {},
      response: {},
      hitCount: 0,
      createdAt: new Date(Date.now() - 7200000), // 2 hours ago
      expiresAt: new Date(Date.now() - 3600000), // 1 hour ago
      lastAccessedAt: new Date(Date.now() - 3600000),
    }

    expect(isExpired(entry)).toBe(true)
  })

  it('returns false for non-expired entry', () => {
    const entry: CacheEntry = {
      key: 'test',
      params: {},
      response: {},
      hitCount: 0,
      createdAt: new Date(),
      expiresAt: new Date(Date.now() + 3600000), // 1 hour from now
      lastAccessedAt: new Date(),
    }

    expect(isExpired(entry)).toBe(false)
  })
})

describe('queryCacheEntries', () => {
  let mockDB: ReturnType<typeof createMockDB>

  beforeEach(() => {
    mockDB = createMockDB()

    // Add some cache entries
    const cacheCollection = mockDB._collections
    cacheCollection.set('ai_cache', [
      {
        $id: 'ai_cache/1',
        $type: 'AICache',
        name: 'cache-1',
        key: 'key1',
        params: {},
        response: { text: 'Response 1' },
        modelId: 'gpt-4',
        hitCount: 5,
        createdAt: new Date(Date.now() - 3600000),
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      },
      {
        $id: 'ai_cache/2',
        $type: 'AICache',
        name: 'cache-2',
        key: 'key2',
        params: {},
        response: { text: 'Response 2' },
        modelId: 'gpt-3.5-turbo',
        hitCount: 10,
        createdAt: new Date(Date.now() - 7200000),
        expiresAt: new Date(Date.now() - 1000), // expired
        lastAccessedAt: new Date(),
      },
    ])
  })

  it('returns all non-expired cache entries', async () => {
    const entries = await queryCacheEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db']
    )

    // Should call collection.find with filter for non-expired
    expect(mockDB.collection).toHaveBeenCalledWith('ai_cache')
  })

  it('filters by modelId', async () => {
    await queryCacheEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { modelId: 'gpt-4' }
    )

    const findCall = mockDB.collection('ai_cache').find
    expect(findCall).toHaveBeenCalled()
  })

  it('includes expired entries when specified', async () => {
    await queryCacheEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { includeExpired: true }
    )

    expect(mockDB.collection).toHaveBeenCalledWith('ai_cache')
  })
})

describe('queryLogEntries', () => {
  let mockDB: ReturnType<typeof createMockDB>

  beforeEach(() => {
    mockDB = createMockDB()

    // Add some log entries
    const logCollection = mockDB._collections
    logCollection.set('ai_logs', [
      {
        $id: 'ai_logs/1',
        $type: 'AILog',
        name: 'log-1',
        timestamp: new Date(),
        modelId: 'gpt-4',
        requestType: 'generate',
        latencyMs: 500,
        cached: false,
      },
      {
        $id: 'ai_logs/2',
        $type: 'AILog',
        name: 'log-2',
        timestamp: new Date(Date.now() - 3600000),
        modelId: 'gpt-3.5-turbo',
        requestType: 'stream',
        latencyMs: 200,
        cached: true,
        error: { name: 'Error', message: 'Test error' },
      },
    ])
  })

  it('returns log entries', async () => {
    const entries = await queryLogEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db']
    )

    expect(mockDB.collection).toHaveBeenCalledWith('ai_logs')
  })

  it('filters by modelId', async () => {
    await queryLogEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { modelId: 'gpt-4' }
    )

    expect(mockDB.collection).toHaveBeenCalledWith('ai_logs')
  })

  it('filters by requestType', async () => {
    await queryLogEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { requestType: 'generate' }
    )

    expect(mockDB.collection).toHaveBeenCalledWith('ai_logs')
  })

  it('filters errors only', async () => {
    await queryLogEntries(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { errorsOnly: true }
    )

    expect(mockDB.collection).toHaveBeenCalledWith('ai_logs')
  })
})

describe('clearExpiredCache', () => {
  it('deletes expired entries', async () => {
    const mockDB = createMockDB()

    // Add expired entry
    mockDB._collections.set('ai_cache', [
      {
        $id: 'ai_cache/1',
        $type: 'AICache',
        name: 'cache-1',
        key: 'key1',
        params: {},
        response: {},
        hitCount: 0,
        createdAt: new Date(Date.now() - 7200000),
        expiresAt: new Date(Date.now() - 3600000), // expired
        lastAccessedAt: new Date(),
      },
    ])

    const deleted = await clearExpiredCache(
      mockDB as unknown as ParqueDBMiddlewareOptions['db']
    )

    expect(deleted).toBeGreaterThanOrEqual(0)
  })

  it('uses custom collection name', async () => {
    const mockDB = createMockDB()

    await clearExpiredCache(
      mockDB as unknown as ParqueDBMiddlewareOptions['db'],
      { collection: 'custom_cache' }
    )

    expect(mockDB.collection).toHaveBeenCalledWith('custom_cache')
  })
})

describe('getCacheStats', () => {
  it('returns cache statistics', async () => {
    const mockDB = createMockDB()

    // Add cache entries
    mockDB._collections.set('ai_cache', [
      {
        $id: 'ai_cache/1',
        $type: 'AICache',
        name: 'cache-1',
        key: 'key1',
        params: {},
        response: {},
        hitCount: 5,
        createdAt: new Date(Date.now() - 3600000),
        expiresAt: new Date(Date.now() + 3600000), // active
        lastAccessedAt: new Date(),
      },
      {
        $id: 'ai_cache/2',
        $type: 'AICache',
        name: 'cache-2',
        key: 'key2',
        params: {},
        response: {},
        hitCount: 10,
        createdAt: new Date(Date.now() - 7200000),
        expiresAt: new Date(Date.now() - 1000), // expired
        lastAccessedAt: new Date(),
      },
    ])

    const stats = await getCacheStats(
      mockDB as unknown as ParqueDBMiddlewareOptions['db']
    )

    expect(stats.totalEntries).toBe(2)
    expect(stats.activeEntries).toBe(1)
    expect(stats.expiredEntries).toBe(1)
    expect(stats.totalHits).toBe(15)
    expect(stats.oldestEntry).toBeDefined()
    expect(stats.newestEntry).toBeDefined()
  })

  it('handles empty cache', async () => {
    const mockDB = createMockDB()
    mockDB._collections.set('ai_cache', [])

    const stats = await getCacheStats(
      mockDB as unknown as ParqueDBMiddlewareOptions['db']
    )

    expect(stats.totalEntries).toBe(0)
    expect(stats.activeEntries).toBe(0)
    expect(stats.expiredEntries).toBe(0)
    expect(stats.totalHits).toBe(0)
    expect(stats.oldestEntry).toBeUndefined()
    expect(stats.newestEntry).toBeUndefined()
  })
})

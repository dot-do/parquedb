/**
 * AI SDK Middleware Integration Tests
 *
 * Tests for the ParqueDB AI SDK middleware that provides caching and logging
 * for Vercel AI SDK language model operations.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { DB } from '../../../src/db'
import {
  createParqueDBMiddleware,
  hashParams,
  isExpired,
  queryCacheEntries,
  queryLogEntries,
  clearExpiredCache,
  getCacheStats,
} from '../../../src/integrations/ai-sdk'
import type {
  ParqueDBMiddlewareOptions,
  CacheEntry,
  LogEntry,
  LanguageModelGenerateResult,
  LanguageModelStreamResult,
  LanguageModel,
  LanguageModelCallOptions,
} from '../../../src/integrations/ai-sdk'

describe('AI SDK Middleware Integration', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    // Create a fresh database for each test
    db = DB({
      ai_cache: {
        key: 'string!',
        params: 'json',
        response: 'json',
        modelId: 'string',
        hitCount: 'int',
        expiresAt: 'datetime',
        lastAccessedAt: 'datetime',
      },
      ai_logs: {
        timestamp: 'datetime',
        modelId: 'string',
        providerId: 'string',
        requestType: 'string',
        prompt: 'json',
        response: 'json',
        responseText: 'text',
        usage: 'json',
        latencyMs: 'int',
        cached: 'boolean',
        finishReason: 'string',
        metadata: 'json',
        error: 'json',
      },
    })
  })

  describe('createParqueDBMiddleware', () => {
    it('should create middleware with default options', () => {
      const middleware = createParqueDBMiddleware({ db })

      expect(middleware).toBeDefined()
      expect(middleware.specificationVersion).toBe('v3')
      expect(typeof middleware.wrapGenerate).toBe('function')
      expect(typeof middleware.wrapStream).toBe('function')
    })

    it('should create middleware with cache enabled', () => {
      const middleware = createParqueDBMiddleware({
        db,
        cache: { enabled: true },
      })

      expect(middleware).toBeDefined()
      expect(middleware.wrapGenerate).toBeDefined()
    })

    it('should create middleware with logging enabled', () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true },
      })

      expect(middleware).toBeDefined()
      expect(middleware.wrapGenerate).toBeDefined()
    })

    it('should create middleware with both cache and logging', () => {
      const middleware = createParqueDBMiddleware({
        db,
        cache: { enabled: true, ttlSeconds: 7200 },
        logging: { enabled: true, level: 'verbose' },
      })

      expect(middleware).toBeDefined()
    })

    it('should create middleware with custom collection names', () => {
      const middleware = createParqueDBMiddleware({
        db,
        cache: { enabled: true, collection: 'my_cache' },
        logging: { enabled: true, collection: 'my_logs' },
      })

      expect(middleware).toBeDefined()
    })
  })

  describe('hashParams', () => {
    it('should generate consistent hash for same params', async () => {
      const params: LanguageModelCallOptions = {
        prompt: 'Hello, world!',
        temperature: 0.7,
        maxTokens: 100,
      }

      const hash1 = await hashParams(params, 'gpt-4')
      const hash2 = await hashParams(params, 'gpt-4')

      expect(hash1).toBe(hash2)
    })

    it('should generate different hash for different params', async () => {
      const params1: LanguageModelCallOptions = { prompt: 'Hello' }
      const params2: LanguageModelCallOptions = { prompt: 'Goodbye' }

      const hash1 = await hashParams(params1, 'gpt-4')
      const hash2 = await hashParams(params2, 'gpt-4')

      expect(hash1).not.toBe(hash2)
    })

    it('should generate different hash for different models', async () => {
      const params: LanguageModelCallOptions = { prompt: 'Hello' }

      const hash1 = await hashParams(params, 'gpt-4')
      const hash2 = await hashParams(params, 'gpt-3.5-turbo')

      expect(hash1).not.toBe(hash2)
    })

    it('should exclude specified fields from hash', async () => {
      const params1: LanguageModelCallOptions = { prompt: 'Hello', temperature: 0.7 }
      const params2: LanguageModelCallOptions = { prompt: 'Hello', temperature: 0.9 }

      const hash1 = await hashParams(params1, 'gpt-4', ['temperature'])
      const hash2 = await hashParams(params2, 'gpt-4', ['temperature'])

      expect(hash1).toBe(hash2)
    })

    it('should return string starting with cache_', async () => {
      const params: LanguageModelCallOptions = { prompt: 'Test' }
      const hash = await hashParams(params, 'gpt-4')

      expect(hash.startsWith('cache_')).toBe(true)
    })
  })

  describe('isExpired', () => {
    it('should return false for non-expired entry', () => {
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

    it('should return true for expired entry', () => {
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
  })

  describe('wrapGenerate', () => {
    it('should call doGenerate and return result', async () => {
      const middleware = createParqueDBMiddleware({ db })

      const mockResult: LanguageModelGenerateResult = {
        text: 'Hello from AI!',
        finishReason: 'stop',
        usage: { promptTokens: 10, completionTokens: 5, totalTokens: 15 },
      }

      const mockModel: LanguageModel = {
        specificationVersion: 'v3',
        modelId: 'gpt-4',
        provider: 'openai',
      }

      const result = await middleware.wrapGenerate!({
        doGenerate: async () => mockResult,
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Hello!' },
        model: mockModel,
      })

      expect(result).toEqual(mockResult)
    })

    it('should cache response when caching is enabled', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        cache: { enabled: true, ttlSeconds: 3600 },
      })

      const mockResult: LanguageModelGenerateResult = {
        text: 'Cached response',
        finishReason: 'stop',
      }

      const mockModel: LanguageModel = {
        modelId: 'gpt-4',
        provider: 'openai',
      }

      const doGenerate = vi.fn().mockResolvedValue(mockResult)

      // First call - should execute doGenerate
      await middleware.wrapGenerate!({
        doGenerate,
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Cache me!' },
        model: mockModel,
      })

      expect(doGenerate).toHaveBeenCalledTimes(1)

      // Verify cache entry was created
      const cacheEntries = await db.collection('ai_cache').find({})
      expect(cacheEntries.items.length).toBe(1)
    })

    it('should return cached response on cache hit', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        cache: { enabled: true, ttlSeconds: 3600 },
      })

      const mockResult: LanguageModelGenerateResult = {
        text: 'Cached response',
        finishReason: 'stop',
      }

      const mockModel: LanguageModel = {
        modelId: 'gpt-4',
        provider: 'openai',
      }

      const doGenerate = vi.fn().mockResolvedValue(mockResult)
      const params = { prompt: 'Cache hit test!' }

      // First call - cache miss
      await middleware.wrapGenerate!({
        doGenerate,
        doStream: async () => ({ stream: new ReadableStream() }),
        params,
        model: mockModel,
      })

      expect(doGenerate).toHaveBeenCalledTimes(1)

      // Second call - cache hit
      const result = await middleware.wrapGenerate!({
        doGenerate,
        doStream: async () => ({ stream: new ReadableStream() }),
        params,
        model: mockModel,
      })

      // Should still be 1 - second call used cache
      expect(doGenerate).toHaveBeenCalledTimes(1)
      expect(result.text).toBe('Cached response')
    })

    it('should log request when logging is enabled', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true, level: 'standard' },
      })

      const mockResult: LanguageModelGenerateResult = {
        text: 'Logged response',
        finishReason: 'stop',
        usage: { promptTokens: 10, completionTokens: 5, totalTokens: 15 },
      }

      await middleware.wrapGenerate!({
        doGenerate: async () => mockResult,
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Log this!' },
        model: { modelId: 'gpt-4', provider: 'openai' },
      })

      const logEntries = await db.collection('ai_logs').find({})
      expect(logEntries.items.length).toBe(1)

      const log = logEntries.items[0] as unknown as LogEntry
      expect(log.modelId).toBe('gpt-4')
      expect(log.providerId).toBe('openai')
      expect(log.requestType).toBe('generate')
      expect(log.cached).toBe(false)
      expect(log.latencyMs).toBeGreaterThanOrEqual(0)
    })

    it('should log errors when request fails', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true },
      })

      const mockError = new Error('API Error')

      await expect(
        middleware.wrapGenerate!({
          doGenerate: async () => {
            throw mockError
          },
          doStream: async () => ({ stream: new ReadableStream() }),
          params: { prompt: 'This will fail' },
          model: { modelId: 'gpt-4', provider: 'openai' },
        })
      ).rejects.toThrow('API Error')

      const logEntries = await db.collection('ai_logs').find({})
      expect(logEntries.items.length).toBe(1)

      const log = logEntries.items[0] as unknown as LogEntry
      expect(log.error).toBeDefined()
      expect(log.error?.message).toBe('API Error')
    })

    it('should call onLog callback when provided', async () => {
      const onLog = vi.fn()
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true, onLog },
      })

      await middleware.wrapGenerate!({
        doGenerate: async () => ({ text: 'Test', finishReason: 'stop' }),
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Callback test' },
        model: { modelId: 'gpt-4' },
      })

      expect(onLog).toHaveBeenCalledTimes(1)
    })
  })

  describe('wrapStream', () => {
    it('should call doStream and return result', async () => {
      const middleware = createParqueDBMiddleware({ db })

      const mockStream = new ReadableStream()
      const mockResult: LanguageModelStreamResult = {
        stream: mockStream,
      }

      const result = await middleware.wrapStream!({
        doGenerate: async () => ({ text: 'Test' }),
        doStream: async () => mockResult,
        params: { prompt: 'Stream test' },
        model: { modelId: 'gpt-4' },
      })

      expect(result.stream).toBe(mockStream)
    })

    it('should log stream request when logging is enabled', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true },
      })

      const mockStream = new ReadableStream()
      const mockResult: LanguageModelStreamResult = {
        stream: mockStream,
      }

      await middleware.wrapStream!({
        doGenerate: async () => ({ text: 'Test' }),
        doStream: async () => mockResult,
        params: { prompt: 'Stream log test' },
        model: { modelId: 'gpt-4', provider: 'openai' },
      })

      const logEntries = await db.collection('ai_logs').find({})
      expect(logEntries.items.length).toBe(1)

      const log = logEntries.items[0] as unknown as LogEntry
      expect(log.requestType).toBe('stream')
    })
  })

  describe('queryCacheEntries', () => {
    beforeEach(async () => {
      // Add some cache entries
      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-1',
        key: 'key1',
        params: { prompt: 'Test 1' },
        response: { text: 'Response 1' },
        modelId: 'gpt-4',
        hitCount: 5,
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-2',
        key: 'key2',
        params: { prompt: 'Test 2' },
        response: { text: 'Response 2' },
        modelId: 'gpt-3.5-turbo',
        hitCount: 10,
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-expired',
        key: 'key3',
        params: { prompt: 'Expired' },
        response: { text: 'Expired response' },
        modelId: 'gpt-4',
        hitCount: 1,
        expiresAt: new Date(Date.now() - 3600000), // Expired
        lastAccessedAt: new Date(Date.now() - 3600000),
      })
    })

    it('should return cache entries', async () => {
      const entries = await queryCacheEntries(db)
      expect(entries.length).toBeGreaterThan(0)
    })

    it('should filter by modelId', async () => {
      const entries = await queryCacheEntries(db, { modelId: 'gpt-4' })

      for (const entry of entries) {
        expect(entry.modelId).toBe('gpt-4')
      }
    })

    it('should exclude expired entries by default', async () => {
      const entries = await queryCacheEntries(db)

      for (const entry of entries) {
        expect(new Date(entry.expiresAt).getTime()).toBeGreaterThan(Date.now())
      }
    })

    it('should include expired entries when specified', async () => {
      const entries = await queryCacheEntries(db, { includeExpired: true })
      expect(entries.length).toBe(3)
    })

    it('should respect limit', async () => {
      const entries = await queryCacheEntries(db, { limit: 1, includeExpired: true })
      expect(entries.length).toBe(1)
    })
  })

  describe('queryLogEntries', () => {
    beforeEach(async () => {
      // Add some log entries
      await db.collection('ai_logs').create({
        $type: 'AILog',
        name: 'log-1',
        timestamp: new Date(),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 100,
        cached: false,
        finishReason: 'stop',
      })

      await db.collection('ai_logs').create({
        $type: 'AILog',
        name: 'log-2',
        timestamp: new Date(),
        modelId: 'gpt-3.5-turbo',
        providerId: 'openai',
        requestType: 'stream',
        latencyMs: 200,
        cached: true,
        finishReason: 'stop',
      })

      await db.collection('ai_logs').create({
        $type: 'AILog',
        name: 'log-error',
        timestamp: new Date(),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 50,
        cached: false,
        error: { name: 'Error', message: 'API failed' },
      })
    })

    it('should return log entries', async () => {
      const entries = await queryLogEntries(db)
      expect(entries.length).toBe(3)
    })

    it('should filter by modelId', async () => {
      const entries = await queryLogEntries(db, { modelId: 'gpt-4' })

      for (const entry of entries) {
        expect(entry.modelId).toBe('gpt-4')
      }
    })

    it('should filter by requestType', async () => {
      const entries = await queryLogEntries(db, { requestType: 'stream' })
      expect(entries.length).toBe(1)
      expect(entries[0]?.requestType).toBe('stream')
    })

    it('should filter for errors only', async () => {
      const entries = await queryLogEntries(db, { errorsOnly: true })
      expect(entries.length).toBe(1)
      expect(entries[0]?.error).toBeDefined()
    })

    it('should filter for cached only', async () => {
      const entries = await queryLogEntries(db, { cachedOnly: true })
      expect(entries.length).toBe(1)
      expect(entries[0]?.cached).toBe(true)
    })
  })

  describe('clearExpiredCache', () => {
    beforeEach(async () => {
      // Add expired and non-expired entries
      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-active',
        key: 'active',
        params: {},
        response: {},
        hitCount: 0,
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-expired-1',
        key: 'expired1',
        params: {},
        response: {},
        hitCount: 0,
        expiresAt: new Date(Date.now() - 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-expired-2',
        key: 'expired2',
        params: {},
        response: {},
        hitCount: 0,
        expiresAt: new Date(Date.now() - 7200000),
        lastAccessedAt: new Date(),
      })
    })

    it('should delete expired entries and return count', async () => {
      const deleted = await clearExpiredCache(db)
      expect(deleted).toBe(2)

      const remaining = await db.collection('ai_cache').find({})
      expect(remaining.items.length).toBe(1)
    })
  })

  describe('getCacheStats', () => {
    beforeEach(async () => {
      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-1',
        key: 'key1',
        params: {},
        response: {},
        hitCount: 10,
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-2',
        key: 'key2',
        params: {},
        response: {},
        hitCount: 5,
        expiresAt: new Date(Date.now() + 3600000),
        lastAccessedAt: new Date(),
      })

      await db.collection('ai_cache').create({
        $type: 'AICache',
        name: 'cache-expired',
        key: 'key3',
        params: {},
        response: {},
        hitCount: 3,
        expiresAt: new Date(Date.now() - 3600000),
        lastAccessedAt: new Date(),
      })
    })

    it('should return cache statistics', async () => {
      const stats = await getCacheStats(db)

      expect(stats.totalEntries).toBe(3)
      expect(stats.activeEntries).toBe(2)
      expect(stats.expiredEntries).toBe(1)
      expect(stats.totalHits).toBe(18) // 10 + 5 + 3
    })
  })

  describe('Logging Levels', () => {
    it('should include usage in standard level', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true, level: 'standard' },
      })

      await middleware.wrapGenerate!({
        doGenerate: async () => ({
          text: 'Test',
          finishReason: 'stop',
          usage: { promptTokens: 10, completionTokens: 5, totalTokens: 15 },
        }),
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Standard level test' },
        model: { modelId: 'gpt-4' },
      })

      const logs = await db.collection('ai_logs').find({})
      const log = logs.items[0] as unknown as LogEntry

      expect(log.usage).toBeDefined()
      expect(log.responseText).toBe('Test')
    })

    it('should include prompt and response in verbose level', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: { enabled: true, level: 'verbose' },
      })

      await middleware.wrapGenerate!({
        doGenerate: async () => ({
          text: 'Verbose response',
          finishReason: 'stop',
        }),
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Verbose prompt' },
        model: { modelId: 'gpt-4' },
      })

      const logs = await db.collection('ai_logs').find({})
      const log = logs.items[0] as unknown as LogEntry

      expect(log.prompt).toBeDefined()
      expect(log.response).toBeDefined()
    })

    it('should include custom metadata', async () => {
      const middleware = createParqueDBMiddleware({
        db,
        logging: {
          enabled: true,
          metadata: { app: 'test-app', version: '1.0.0' },
        },
      })

      await middleware.wrapGenerate!({
        doGenerate: async () => ({ text: 'Test' }),
        doStream: async () => ({ stream: new ReadableStream() }),
        params: { prompt: 'Metadata test' },
        model: { modelId: 'gpt-4' },
      })

      const logs = await db.collection('ai_logs').find({})
      const log = logs.items[0] as unknown as LogEntry

      expect(log.metadata).toEqual({ app: 'test-app', version: '1.0.0' })
    })
  })
})

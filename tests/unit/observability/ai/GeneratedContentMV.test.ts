/**
 * Tests for GeneratedContentMV - Generated Content Materialized View
 *
 * Tests the recording, filtering, versioning, and aggregation of generated content.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  GeneratedContentMV,
  createGeneratedContentMV,
  generateContentId,
  hashContent,
  type GeneratedContentRecord,
  type GeneratedContentMVConfig,
  type RecordContentInput,
  type ContentQueryOptions,
} from '../../../../src/observability/ai/GeneratedContentMV'
import type { ModelPricing } from '../../../../src/observability/ai/types'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockRecord {
  $id: string
  [key: string]: unknown
}

function createMockCollection() {
  const store: Map<string, MockRecord> = new Map()
  let idCounter = 0

  return {
    store,
    create: vi.fn(async (data: Record<string, unknown>) => {
      const id = `generated_content/${++idCounter}`
      const record = { $id: id, ...data } as MockRecord
      store.set(id, record)
      return record
    }),
    createMany: vi.fn(async (items: Record<string, unknown>[]) => {
      return Promise.all(items.map(async (data) => {
        const id = `generated_content/${++idCounter}`
        const record = { $id: id, ...data } as MockRecord
        store.set(id, record)
        return record
      }))
    }),
    find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
      let results = Array.from(store.values())

      // Apply filters
      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item[key] as Date
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
            } else if (typeof value === 'boolean') {
              if (item[key] !== value) return false
            } else if (typeof value === 'string') {
              if (item[key] !== value) return false
            } else if (typeof value !== 'object' && item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      // Apply sorting
      if (options?.sort) {
        const [sortKey, sortDir] = Object.entries(options.sort)[0]!
        results.sort((a, b) => {
          const aVal = a[sortKey]
          const bVal = b[sortKey]
          if (aVal instanceof Date && bVal instanceof Date) {
            return sortDir > 0 ? aVal.getTime() - bVal.getTime() : bVal.getTime() - aVal.getTime()
          }
          if (typeof aVal === 'number' && typeof bVal === 'number') {
            return sortDir > 0 ? aVal - bVal : bVal - aVal
          }
          return 0
        })
      }

      // Apply limit
      if (options?.limit) {
        results = results.slice(0, options.limit)
      }

      return results
    }),
    update: vi.fn(async (id: string, update: { $set: Record<string, unknown> }) => {
      const fullId = `generated_content/${id}`
      const record = store.get(fullId)
      if (record && update.$set) {
        for (const [key, value] of Object.entries(update.$set)) {
          record[key] = value
        }
      }
      return record
    }),
    delete: vi.fn(async (id: string) => {
      const fullId = `generated_content/${id}`
      const exists = store.has(fullId)
      store.delete(fullId)
      return { deletedCount: exists ? 1 : 0 }
    }),
    count: vi.fn(async (filter?: Record<string, unknown>) => {
      let results = Array.from(store.values())

      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item[key] as Date
              const filterObj = value as Record<string, unknown>
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
            } else if (typeof value === 'boolean') {
              if (item[key] !== value) return false
            } else if (typeof value === 'string') {
              if (item[key] !== value) return false
            } else if (typeof value !== 'object' && item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      return results.length
    }),
    deleteMany: vi.fn(async (filter: Record<string, unknown>, _options?: { hard?: boolean }) => {
      let toDelete: string[] = []

      for (const [id, item] of store) {
        let matches = true
        for (const [key, value] of Object.entries(filter)) {
          if (key === 'timestamp' && typeof value === 'object' && value !== null) {
            const ts = item[key] as Date
            const filterObj = value as Record<string, unknown>
            if (filterObj.$lt && ts >= (filterObj.$lt as Date)) matches = false
            if (filterObj.$gte && ts < (filterObj.$gte as Date)) matches = false
          } else if (typeof value === 'boolean') {
            if (item[key] !== value) matches = false
          } else if (typeof value === 'string') {
            if (item[key] !== value) matches = false
          } else if (typeof value !== 'object' && item[key] !== value) {
            matches = false
          }
        }
        if (matches) {
          toDelete.push(id)
        }
      }

      for (const id of toDelete) {
        store.delete(id)
      }

      return { deletedCount: toDelete.length }
    }),
  }
}

function createMockDB() {
  const collections: Map<string, ReturnType<typeof createMockCollection>> = new Map()

  return {
    collections,
    collection: vi.fn((name: string) => {
      if (!collections.has(name)) {
        collections.set(name, createMockCollection())
      }
      return collections.get(name)!
    }),
  }
}

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('Helper Functions', () => {
  describe('generateContentId', () => {
    it('should generate unique content IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 1000; i++) {
        ids.add(generateContentId())
      }
      expect(ids.size).toBe(1000)
    })

    it('should prefix IDs with gc_', () => {
      const id = generateContentId()
      expect(id.startsWith('gc_')).toBe(true)
    })

    it('should have reasonable length', () => {
      const id = generateContentId()
      expect(id.length).toBeGreaterThan(10)
      expect(id.length).toBeLessThan(30)
    })
  })

  describe('hashContent', () => {
    it('should produce same hash for same content', () => {
      const content = 'Hello, world!'
      const hash1 = hashContent(content)
      const hash2 = hashContent(content)
      expect(hash1).toBe(hash2)
    })

    it('should produce different hashes for different content', () => {
      const hash1 = hashContent('Hello, world!')
      const hash2 = hashContent('Goodbye, world!')
      expect(hash1).not.toBe(hash2)
    })

    it('should handle empty string', () => {
      const hash = hashContent('')
      expect(hash).toBeDefined()
      expect(typeof hash).toBe('string')
    })
  })
})

// =============================================================================
// GeneratedContentMV Constructor Tests
// =============================================================================

describe('GeneratedContentMV', () => {
  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = new GeneratedContentMV(db)

      expect(mv).toBeInstanceOf(GeneratedContentMV)
      const config = mv.getConfig()
      expect(config.collection).toBe('generated_content')
      expect(config.maxAgeMs).toBe(30 * 24 * 60 * 60 * 1000) // 30 days
      expect(config.batchSize).toBe(1000)
      expect(config.debug).toBe(false)
    })

    it('should accept custom configuration', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const config: GeneratedContentMVConfig = {
        collection: 'custom_content',
        maxAgeMs: 7 * 24 * 60 * 60 * 1000,
        batchSize: 500,
        debug: true,
      }

      const mv = new GeneratedContentMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.collection).toBe('custom_content')
      expect(resolvedConfig.maxAgeMs).toBe(7 * 24 * 60 * 60 * 1000)
      expect(resolvedConfig.batchSize).toBe(500)
      expect(resolvedConfig.debug).toBe(true)
    })

    it('should include custom pricing', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const customPricing: ModelPricing[] = [
        { modelId: 'custom-model', providerId: 'custom', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00 },
      ]

      const mv = new GeneratedContentMV(db, { customPricing })
      const config = mv.getConfig()

      expect(config.pricing.has('custom-model:custom')).toBe(true)
      expect(config.pricing.get('custom-model:custom')?.inputPricePerMillion).toBe(1.00)
    })
  })

  describe('createGeneratedContentMV factory', () => {
    it('should create a GeneratedContentMV instance', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = createGeneratedContentMV(db)

      expect(mv).toBeInstanceOf(GeneratedContentMV)
    })
  })
})

// =============================================================================
// Recording Tests
// =============================================================================

describe('Content Recording', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(() => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])
  })

  describe('record', () => {
    it('should record basic text content', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Hello, this is a generated response.',
        tokenCount: 10,
      })

      expect(result.modelId).toBe('gpt-4')
      expect(result.providerId).toBe('openai')
      expect(result.contentType).toBe('text')
      expect(result.content).toBe('Hello, this is a generated response.')
      expect(result.contentLength).toBe(36)
      expect(result.tokenCount).toBe(10)
      expect(result.version).toBe(1)
      expect(result.contentId).toBeDefined()
      expect(result.timestamp).toBeInstanceOf(Date)
    })

    it('should serialize object content to JSON', async () => {
      const content = { key: 'value', nested: { foo: 'bar' } }
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'json',
        content,
      })

      expect(result.content).toBe(JSON.stringify(content))
      expect(result.contentType).toBe('json')
    })

    it('should record content with all optional fields', async () => {
      const now = new Date()

      const result = await mv.record({
        modelId: 'claude-3-sonnet',
        providerId: 'anthropic',
        contentType: 'code',
        content: 'function hello() { return "world"; }',
        tokenCount: 50,
        promptTokenCount: 20,
        totalTokenCount: 70,
        finishReason: 'stop',
        latencyMs: 500,
        isStreaming: true,
        isCached: false,
        classification: 'safe',
        toolName: 'code_gen',
        toolCallId: 'call_123',
        language: 'code:javascript',
        sessionId: 'session_abc',
        userId: 'user_xyz',
        appId: 'my-app',
        environment: 'production',
        contentId: 'custom-content-123',
        timestamp: now,
        metadata: { feature: 'code-assist' },
      })

      expect(result.modelId).toBe('claude-3-sonnet')
      expect(result.providerId).toBe('anthropic')
      expect(result.tokenCount).toBe(50)
      expect(result.promptTokenCount).toBe(20)
      expect(result.totalTokenCount).toBe(70)
      expect(result.finishReason).toBe('stop')
      expect(result.isStreaming).toBe(true)
      expect(result.isCached).toBe(false)
      expect(result.classification).toBe('safe')
      expect(result.toolName).toBe('code_gen')
      expect(result.toolCallId).toBe('call_123')
      expect(result.language).toBe('code:javascript')
      expect(result.sessionId).toBe('session_abc')
      expect(result.userId).toBe('user_xyz')
      expect(result.appId).toBe('my-app')
      expect(result.environment).toBe('production')
      expect(result.contentId).toBe('custom-content-123')
      expect(result.timestamp).toEqual(now)
      expect(result.metadata).toEqual({ feature: 'code-assist' })
    })

    it('should use default values for optional fields', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Test',
      })

      expect(result.tokenCount).toBe(0)
      expect(result.promptTokenCount).toBe(0)
      expect(result.finishReason).toBe('unknown')
      expect(result.latencyMs).toBe(0)
      expect(result.isStreaming).toBe(false)
      expect(result.isCached).toBe(false)
      expect(result.classification).toBe('unclassified')
    })

    it('should calculate cost automatically for known models', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Test response',
        promptTokenCount: 1000000, // 1M prompt tokens
        tokenCount: 500000, // 0.5M completion tokens
      })

      // GPT-4: $30/1M input, $60/1M output
      expect(result.estimatedCost).toBeCloseTo(60.00, 2)
    })

    it('should use custom cost when provided', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Test',
        tokenCount: 1000,
        estimatedCost: 0.123,
      })

      expect(result.estimatedCost).toBe(0.123)
    })

    it('should generate content hash', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Test content',
      })

      expect(result.contentHash).toBeDefined()
      expect(typeof result.contentHash).toBe('string')
    })

    it('should set rootContentId to own contentId for first version', async () => {
      const result = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'First version',
      })

      expect(result.version).toBe(1)
      expect(result.rootContentId).toBe(result.contentId)
      expect(result.parentContentId).toBeUndefined()
    })
  })

  describe('recordMany', () => {
    it('should record multiple content at once', async () => {
      const inputs: RecordContentInput[] = [
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Content 1' },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', contentType: 'code', content: 'Content 2' },
        { modelId: 'gemini-1.5-pro', providerId: 'google', contentType: 'markdown', content: 'Content 3' },
      ]

      const results = await mv.recordMany(inputs)

      expect(results).toHaveLength(3)
      expect(results[0]!.modelId).toBe('gpt-4')
      expect(results[1]!.modelId).toBe('claude-3-sonnet')
      expect(results[2]!.modelId).toBe('gemini-1.5-pro')
    })

    it('should auto-generate unique content IDs', async () => {
      const inputs: RecordContentInput[] = Array.from({ length: 10 }, () => ({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text' as const,
        content: 'Test content',
      }))

      const results = await mv.recordMany(inputs)
      const ids = results.map(r => r.contentId)
      const uniqueIds = new Set(ids)

      expect(uniqueIds.size).toBe(10)
    })
  })
})

// =============================================================================
// Query Tests
// =============================================================================

describe('Query Operations', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(async () => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])

    // Add test data
    const baseTime = new Date('2026-02-03T10:00:00.000Z')
    const contents: RecordContentInput[] = [
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Text content 1', timestamp: new Date(baseTime.getTime()) },
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Text content 2', timestamp: new Date(baseTime.getTime() + 1000), isCached: true },
      { modelId: 'claude-3-sonnet', providerId: 'anthropic', contentType: 'code', content: 'function test() {}', timestamp: new Date(baseTime.getTime() + 2000), isStreaming: true },
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'json', content: '{"key":"value"}', timestamp: new Date(baseTime.getTime() + 3000), classification: 'sensitive' },
      { modelId: 'gemini-1.5-pro', providerId: 'google', contentType: 'markdown', content: '# Heading', timestamp: new Date(baseTime.getTime() + 4000), userId: 'user-123' },
    ]

    await mv.recordMany(contents)
  })

  describe('find', () => {
    it('should find all content with default options', async () => {
      const results = await mv.find()
      expect(results.length).toBe(5)
    })

    it('should filter by modelId', async () => {
      const results = await mv.find({ modelId: 'gpt-4' })
      expect(results.length).toBe(3)
      expect(results.every(r => r.modelId === 'gpt-4')).toBe(true)
    })

    it('should filter by providerId', async () => {
      const results = await mv.find({ providerId: 'anthropic' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('claude-3-sonnet')
    })

    it('should filter by contentType', async () => {
      const results = await mv.find({ contentType: 'code' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('claude-3-sonnet')
    })

    it('should filter by classification', async () => {
      const results = await mv.find({ classification: 'sensitive' })
      expect(results.length).toBe(1)
      expect(results[0]!.contentType).toBe('json')
    })

    it('should filter by userId', async () => {
      const results = await mv.find({ userId: 'user-123' })
      expect(results.length).toBe(1)
      expect(results[0]!.modelId).toBe('gemini-1.5-pro')
    })

    it('should filter by cachedOnly', async () => {
      const results = await mv.find({ cachedOnly: true })
      expect(results.length).toBe(1)
      expect(results[0]!.isCached).toBe(true)
    })

    it('should filter by streamingOnly', async () => {
      const results = await mv.find({ streamingOnly: true })
      expect(results.length).toBe(1)
      expect(results[0]!.isStreaming).toBe(true)
    })

    it('should respect limit option', async () => {
      const results = await mv.find({ limit: 2 })
      expect(results.length).toBe(2)
    })
  })

  describe('findOne', () => {
    it('should find a single content by contentId', async () => {
      const allContent = await mv.find()
      const targetId = allContent[0]!.contentId

      const result = await mv.findOne(targetId)
      expect(result).not.toBeNull()
      expect(result!.contentId).toBe(targetId)
    })

    it('should return null for non-existent contentId', async () => {
      const result = await mv.findOne('non-existent-id')
      expect(result).toBeNull()
    })
  })

  describe('findByHash', () => {
    it('should find content by hash', async () => {
      const content = 'Unique content for hash test'
      await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content,
      })

      const hash = hashContent(content)
      const results = await mv.findByHash(hash)

      expect(results.length).toBe(1)
      expect(results[0]!.content).toBe(content)
    })
  })
})

// =============================================================================
// Versioning Tests
// =============================================================================

describe('Content Versioning', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(() => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])
  })

  describe('createVersion', () => {
    it('should create a new version linked to parent', async () => {
      // Create initial content
      const original = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Original content',
        contentId: 'content-v1',
      })

      expect(original.version).toBe(1)
      expect(original.rootContentId).toBe('content-v1')

      // Create new version
      const newVersion = await mv.createVersion('content-v1', {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Revised content',
      })

      expect(newVersion.version).toBe(2)
      expect(newVersion.parentContentId).toBe('content-v1')
      expect(newVersion.rootContentId).toBe('content-v1')
    })

    it('should throw error for non-existent parent', async () => {
      await expect(
        mv.createVersion('non-existent', {
          modelId: 'gpt-4',
          providerId: 'openai',
          contentType: 'text',
          content: 'New content',
        })
      ).rejects.toThrow('Parent content not found')
    })

    it('should preserve rootContentId through version chain', async () => {
      // Create original
      const v1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 1',
        contentId: 'root-content',
      })

      // Create v2
      const v2 = await mv.createVersion(v1.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 2',
      })

      // Create v3
      const v3 = await mv.createVersion(v2.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 3',
      })

      expect(v1.rootContentId).toBe('root-content')
      expect(v2.rootContentId).toBe('root-content')
      expect(v3.rootContentId).toBe('root-content')
    })

    it('should record versionReason', async () => {
      const v1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Original',
      })

      const v2 = await mv.createVersion(v1.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Revised',
        versionReason: 'user_edit',
      })

      expect(v2.versionReason).toBe('user_edit')
    })
  })

  describe('getVersionHistory', () => {
    let rootContentId: string

    beforeEach(async () => {
      // Create version chain: v1 -> v2 -> v3
      const v1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 1',
        contentId: 'history-root',
      })
      rootContentId = v1.contentId

      const v2 = await mv.createVersion(v1.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 2',
      })

      await mv.createVersion(v2.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 3',
      })
    })

    it('should return version history for root content', async () => {
      const history = await mv.getVersionHistory(rootContentId)

      expect(history.length).toBe(3)
      expect(history[0]!.version).toBe(1)
      expect(history[1]!.version).toBe(2)
      expect(history[2]!.version).toBe(3)
    })

    it('should return version history from any version in chain', async () => {
      const allContent = await mv.find({ rootContentId })
      const v2 = allContent.find(c => c.version === 2)!

      const history = await mv.getVersionHistory(v2.contentId)

      expect(history.length).toBe(3)
      expect(history[0]!.version).toBe(1)
      expect(history[2]!.version).toBe(3)
    })

    it('should return sorted history (oldest first)', async () => {
      const history = await mv.getVersionHistory(rootContentId)

      for (let i = 0; i < history.length - 1; i++) {
        expect(history[i]!.version).toBeLessThan(history[i + 1]!.version)
      }
    })

    it('should return empty array for non-existent content', async () => {
      const history = await mv.getVersionHistory('non-existent')
      expect(history).toEqual([])
    })

    it('should return single record for content without versions', async () => {
      const standalone = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Standalone content',
        contentId: 'standalone-content',
      })

      const history = await mv.getVersionHistory(standalone.contentId)
      expect(history.length).toBe(1)
      expect(history[0]!.contentId).toBe('standalone-content')
    })
  })

  describe('getLatestVersion', () => {
    beforeEach(async () => {
      // Create version chain
      const v1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 1',
        contentId: 'latest-root',
      })

      const v2 = await mv.createVersion(v1.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 2',
      })

      await mv.createVersion(v2.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Version 3',
      })
    })

    it('should return the latest version from root content id', async () => {
      const latest = await mv.getLatestVersion('latest-root')

      expect(latest).not.toBeNull()
      expect(latest!.version).toBe(3)
      expect(latest!.content).toBe('Version 3')
    })

    it('should return the latest version from any version in chain', async () => {
      const allContent = await mv.find({ rootContentId: 'latest-root' })
      const v2 = allContent.find(c => c.version === 2)!

      const latest = await mv.getLatestVersion(v2.contentId)

      expect(latest).not.toBeNull()
      expect(latest!.version).toBe(3)
    })

    it('should return null for non-existent content', async () => {
      const latest = await mv.getLatestVersion('non-existent')
      expect(latest).toBeNull()
    })

    it('should return the same record for content without versions', async () => {
      const standalone = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'No versions',
        contentId: 'no-versions',
      })

      const latest = await mv.getLatestVersion(standalone.contentId)
      expect(latest).not.toBeNull()
      expect(latest!.contentId).toBe('no-versions')
      expect(latest!.version).toBe(1)
    })
  })
})

// =============================================================================
// Statistics Tests
// =============================================================================

describe('Statistics', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(() => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])
  })

  describe('getStats', () => {
    it('should return empty stats for no data', async () => {
      const stats = await mv.getStats()

      expect(stats.totalRecords).toBe(0)
      expect(stats.uniqueContentCount).toBe(0)
      expect(stats.cacheHits).toBe(0)
      expect(stats.cacheHitRatio).toBe(0)
    })

    it('should calculate basic statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Content 1', tokenCount: 100 },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Content 2', tokenCount: 200 },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'code', content: 'Content 3', tokenCount: 150 },
      ])

      const stats = await mv.getStats()

      expect(stats.totalRecords).toBe(3)
      expect(stats.uniqueContentCount).toBe(3)
    })

    it('should calculate token statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'A', tokenCount: 100, promptTokenCount: 50 },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'B', tokenCount: 200, promptTokenCount: 100 },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'C', tokenCount: 300, promptTokenCount: 150 },
      ])

      const stats = await mv.getStats()

      expect(stats.tokens.totalTokens).toBe(600)
      expect(stats.tokens.totalPromptTokens).toBe(300)
      expect(stats.tokens.avgTokensPerContent).toBe(200)
    })

    it('should calculate content length statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Short' }, // 5
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Medium length' }, // 13
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'This is a longer piece of content' }, // 33
      ])

      const stats = await mv.getStats()

      expect(stats.contentLength.min).toBe(5)
      expect(stats.contentLength.max).toBe(33)
      expect(stats.contentLength.total).toBe(51)
      expect(stats.contentLength.avg).toBe(17)
    })

    it('should calculate cache statistics', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1', isCached: false },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '2', isCached: true },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '3', isCached: false },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '4', isCached: true },
      ])

      const stats = await mv.getStats()

      expect(stats.cacheHits).toBe(2)
      expect(stats.cacheHitRatio).toBe(0.5)
    })

    it('should provide breakdown by content type', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '2' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'code', content: '3' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'json', content: '4' },
      ])

      const stats = await mv.getStats()

      expect(stats.byContentType['text']).toBe(2)
      expect(stats.byContentType['code']).toBe(1)
      expect(stats.byContentType['json']).toBe(1)
    })

    it('should provide breakdown by model', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '2' },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', contentType: 'text', content: '3' },
      ])

      const stats = await mv.getStats()

      expect(stats.byModel['gpt-4']).toBeDefined()
      expect(stats.byModel['gpt-4']!.count).toBe(2)

      expect(stats.byModel['claude-3-sonnet']).toBeDefined()
      expect(stats.byModel['claude-3-sonnet']!.count).toBe(1)
    })

    it('should provide breakdown by provider', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1' },
        { modelId: 'gpt-4o', providerId: 'openai', contentType: 'text', content: '2' },
        { modelId: 'claude-3-sonnet', providerId: 'anthropic', contentType: 'text', content: '3' },
      ])

      const stats = await mv.getStats()

      expect(stats.byProvider['openai']).toBeDefined()
      expect(stats.byProvider['openai']!.count).toBe(2)

      expect(stats.byProvider['anthropic']).toBeDefined()
      expect(stats.byProvider['anthropic']!.count).toBe(1)
    })

    it('should provide breakdown by classification', async () => {
      await mv.recordMany([
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1', classification: 'safe' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '2', classification: 'safe' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '3', classification: 'sensitive' },
        { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '4', classification: 'pii' },
      ])

      const stats = await mv.getStats()

      expect(stats.byClassification['safe']).toBe(2)
      expect(stats.byClassification['sensitive']).toBe(1)
      expect(stats.byClassification['pii']).toBe(1)
    })

    it('should track version statistics', async () => {
      // Create content with versions
      const v1 = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'V1',
        contentId: 'versioned-1',
      })

      await mv.createVersion(v1.contentId, {
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'V2',
      })

      // Create another without versions
      await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Standalone',
      })

      const stats = await mv.getStats()

      expect(stats.totalRecords).toBe(3)
      expect(stats.uniqueContentCount).toBe(2) // 1 version chain + 1 standalone
      expect(stats.totalVersions).toBe(1) // Only v2 counts as a version (version > 1)
    })
  })
})

// =============================================================================
// Cleanup Tests
// =============================================================================

describe('Cleanup', () => {
  it('should delete old content', async () => {
    const db = createMockDB()
    const mv = new GeneratedContentMV(
      db as unknown as Parameters<typeof createGeneratedContentMV>[0],
      { maxAgeMs: 1000 } // 1 second max age
    )

    // Add old content
    const oldTimestamp = new Date(Date.now() - 5000) // 5 seconds ago
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'Old content',
      timestamp: oldTimestamp,
    })

    // Add recent content
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'New content',
    })

    // Run cleanup
    const result = await mv.cleanup()

    // Old content should be deleted
    expect(result.success).toBe(true)
    expect(result.deletedCount).toBe(1)
  })

  it('should return zero when no content to delete', async () => {
    const db = createMockDB()
    const mv = new GeneratedContentMV(
      db as unknown as Parameters<typeof createGeneratedContentMV>[0],
      { maxAgeMs: 1000 }
    )

    // Add only recent content
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'New content',
    })

    const result = await mv.cleanup()

    expect(result.success).toBe(true)
    expect(result.deletedCount).toBe(0)
  })

  it('should support progress callback', async () => {
    const db = createMockDB()
    const mv = new GeneratedContentMV(
      db as unknown as Parameters<typeof createGeneratedContentMV>[0],
      { maxAgeMs: 1000 }
    )

    const oldTimestamp = new Date(Date.now() - 5000)
    await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'Old content',
      timestamp: oldTimestamp,
    })

    const progressCalls: Array<{ deletedSoFar: number; percentage: number }> = []
    const result = await mv.cleanup({
      onProgress: (progress) => progressCalls.push({ ...progress }),
    })

    expect(result.success).toBe(true)
    expect(progressCalls.length).toBeGreaterThan(0)
  })
})

// =============================================================================
// Content Types Tests
// =============================================================================

describe('Content Types', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(() => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])
  })

  it('should handle all content types', async () => {
    const contentTypes = [
      'text', 'code', 'json', 'markdown', 'html',
      'tool_call', 'tool_result', 'image_description', 'embedding', 'other',
    ] as const

    for (const contentType of contentTypes) {
      await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType,
        content: `Content of type ${contentType}`,
      })
    }

    const stats = await mv.getStats()
    expect(stats.totalRecords).toBe(contentTypes.length)

    for (const contentType of contentTypes) {
      expect(stats.byContentType[contentType]).toBe(1)
    }
  })

  it('should handle tool_call with metadata', async () => {
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'tool_call',
      content: { name: 'get_weather', arguments: { city: 'London' } },
      toolName: 'get_weather',
      toolCallId: 'call_abc123',
    })

    expect(result.contentType).toBe('tool_call')
    expect(result.toolName).toBe('get_weather')
    expect(result.toolCallId).toBe('call_abc123')
    expect(result.content).toBe(JSON.stringify({ name: 'get_weather', arguments: { city: 'London' } }))
  })

  it('should handle code with language detection', async () => {
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'code',
      content: 'function hello() { return "world"; }',
      language: 'code:javascript',
    })

    expect(result.contentType).toBe('code')
    expect(result.language).toBe('code:javascript')
  })

  it('should handle embedding content', async () => {
    const embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
    const result = await mv.record({
      modelId: 'text-embedding-ada-002',
      providerId: 'openai',
      contentType: 'embedding',
      content: embedding,
    })

    expect(result.contentType).toBe('embedding')
    expect(result.content).toBe(JSON.stringify(embedding))
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let db: ReturnType<typeof createMockDB>
  let mv: GeneratedContentMV

  beforeEach(() => {
    db = createMockDB()
    mv = new GeneratedContentMV(db as unknown as Parameters<typeof createGeneratedContentMV>[0])
  })

  it('should handle empty content', async () => {
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: '',
    })

    expect(result.content).toBe('')
    expect(result.contentLength).toBe(0)
  })

  it('should handle very long content', async () => {
    const longContent = 'x'.repeat(100000)
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: longContent,
    })

    expect(result.contentLength).toBe(100000)
  })

  it('should handle special characters in content', async () => {
    const specialContent = 'Test with special chars: \n\t\r "quotes" and \'apostrophes\''
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: specialContent,
    })

    expect(result.content).toBe(specialContent)
  })

  it('should handle unicode content', async () => {
    const unicodeContent = 'Unicode: \u{1F600} \u{1F64B} \u4E2D\u6587'
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: unicodeContent,
    })

    expect(result.content).toBe(unicodeContent)
  })

  it('should handle deeply nested JSON objects', async () => {
    const nestedObj = {
      level1: {
        level2: {
          level3: {
            level4: {
              value: 'deep',
            },
          },
        },
      },
    }
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'json',
      content: nestedObj,
    })

    expect(result.content).toBe(JSON.stringify(nestedObj))
  })

  it('should generate unique IDs for each record', async () => {
    const results = await mv.recordMany([
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '1' },
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: '2' },
    ])

    expect(results[0]!.contentId).not.toBe(results[1]!.contentId)
  })

  it('should produce same hash for same content', async () => {
    const content = 'Identical content'
    const [r1, r2] = await mv.recordMany([
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content },
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content },
    ])

    expect(r1!.contentHash).toBe(r2!.contentHash)
  })

  it('should produce different hash for different content', async () => {
    const [r1, r2] = await mv.recordMany([
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Content A' },
      { modelId: 'gpt-4', providerId: 'openai', contentType: 'text', content: 'Content B' },
    ])

    expect(r1!.contentHash).not.toBe(r2!.contentHash)
  })

  it('should handle custom timestamp', async () => {
    const customTimestamp = new Date('2025-01-15T12:00:00.000Z')
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'Test',
      timestamp: customTimestamp,
    })

    expect(result.timestamp).toEqual(customTimestamp)
  })

  it('should handle custom contentId', async () => {
    const result = await mv.record({
      modelId: 'gpt-4',
      providerId: 'openai',
      contentType: 'text',
      content: 'Test',
      contentId: 'my-custom-id-123',
    })

    expect(result.contentId).toBe('my-custom-id-123')
  })
})

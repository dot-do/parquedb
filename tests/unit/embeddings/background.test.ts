/**
 * Unit Tests for Background Embedding Generation
 *
 * Tests the EmbeddingQueue class for asynchronous embedding generation
 * using mock storage and embedding providers.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  EmbeddingQueue,
  createEmbeddingQueue,
  configureBackgroundEmbeddings,
  BackgroundEmbeddingConfigBuilder,
  type BackgroundEmbeddingConfig,
  type EmbeddingQueueItem,
  type EntityLoader,
  type EntityUpdater,
} from '../../../src/embeddings/background'
import type { EmbeddingProvider } from '../../../src/embeddings/provider'

// =============================================================================
// Mock Storage
// =============================================================================

/**
 * Mock implementation of DurableObjectStorage
 */
function createMockStorage(): DurableObjectStorage & {
  _data: Map<string, unknown>
  getAlarmValue(): number | null
} {
  const data = new Map<string, unknown>()
  let alarm: number | null = null

  const storage = {
    _data: data,

    getAlarmValue(): number | null {
      return alarm
    },

    async get<T>(key: string | string[]): Promise<T | Map<string, T>> {
      if (Array.isArray(key)) {
        const result = {} as Record<string, T>
        for (const k of key) {
          if (data.has(k)) {
            result[k] = data.get(k) as T
          }
        }
        return result as Map<string, T>
      }
      return data.get(key) as T
    },

    async put<T>(keyOrEntries: string | Map<string, T>, value?: T): Promise<void> {
      if (typeof keyOrEntries === 'string') {
        data.set(keyOrEntries, value)
      } else {
        for (const [k, v] of keyOrEntries.entries()) {
          data.set(k, v)
        }
      }
    },

    async delete(keys: string | string[]): Promise<boolean | number> {
      if (Array.isArray(keys)) {
        let count = 0
        for (const key of keys) {
          if (data.delete(key)) count++
        }
        return count
      }
      return data.delete(keys)
    },

    async list<T>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>> {
      const result = new Map<string, T>()
      let count = 0

      for (const [key, value] of data.entries()) {
        if (options?.prefix && !key.startsWith(options.prefix)) {
          continue
        }
        if (options?.limit && count >= options.limit) {
          break
        }
        result.set(key, value as T)
        count++
      }

      return result
    },

    async getAlarm(): Promise<number | null> {
      return alarm
    },

    async setAlarm(scheduledTime: number): Promise<void> {
      alarm = scheduledTime
    },

    async deleteAlarm(): Promise<void> {
      alarm = null
    },

    // Additional methods required by the interface (stubbed)
    async deleteAll(): Promise<void> {
      data.clear()
    },

    sync(): Promise<void> {
      return Promise.resolve()
    },

    sql: {} as SqlStorage,

    getBookmarkForTime(): Promise<string | null> {
      return Promise.resolve(null)
    },

    getCurrentBookmark(): Promise<string | null> {
      return Promise.resolve(null)
    },

    onNextSessionRestoreBookmark(): void {},

    transactionSync(): void {},
  }

  return storage as DurableObjectStorage & { _data: Map<string, unknown>; getAlarmValue(): number | null }
}

// =============================================================================
// Mock Embedding Provider
// =============================================================================

function createMockProvider(dimensions = 384): EmbeddingProvider {
  return {
    async embed(text: string): Promise<number[]> {
      // Return a deterministic vector based on text length
      return Array(dimensions).fill(text.length / 100)
    },

    async embedBatch(texts: string[]): Promise<number[][]> {
      return texts.map(text => Array(dimensions).fill(text.length / 100))
    },

    dimensions,
    model: 'mock-model',
  }
}

// =============================================================================
// Test Fixtures
// =============================================================================

function createTestConfig(
  overrides: Partial<BackgroundEmbeddingConfig> = {}
): BackgroundEmbeddingConfig {
  return {
    provider: createMockProvider(),
    fields: ['description'],
    vectorField: 'embedding',
    batchSize: 10,
    retryAttempts: 3,
    processDelay: 1000,
    fieldSeparator: '\n\n',
    ...overrides,
  }
}

// =============================================================================
// EmbeddingQueue Tests
// =============================================================================

describe('EmbeddingQueue', () => {
  let storage: ReturnType<typeof createMockStorage>
  let config: BackgroundEmbeddingConfig
  let queue: EmbeddingQueue

  beforeEach(() => {
    storage = createMockStorage()
    config = createTestConfig()
    queue = new EmbeddingQueue(storage, config)
  })

  describe('enqueue', () => {
    it('adds item to queue', async () => {
      await queue.enqueue('posts', 'abc123')

      const stats = await queue.getStats()
      expect(stats.total).toBe(1)
      expect(stats.pending).toBe(1)
    })

    it('schedules alarm when queue is empty', async () => {
      await queue.enqueue('posts', 'abc123')

      const alarmValue = storage.getAlarmValue()
      expect(alarmValue).not.toBeNull()
      expect(alarmValue).toBeGreaterThan(Date.now() - 1000)
    })

    it('does not duplicate items already in queue', async () => {
      await queue.enqueue('posts', 'abc123')
      await queue.enqueue('posts', 'abc123')

      const stats = await queue.getStats()
      expect(stats.total).toBe(1)
    })

    it('respects priority', async () => {
      await queue.enqueue('posts', 'low', 100)
      await queue.enqueue('posts', 'high', 10)

      const items = await queue.getItems()
      expect(items).toHaveLength(2)
    })
  })

  describe('enqueueBatch', () => {
    it('adds multiple items at once', async () => {
      await queue.enqueueBatch([
        ['posts', 'abc123'],
        ['posts', 'def456'],
        ['users', 'user1'],
      ])

      const stats = await queue.getStats()
      expect(stats.total).toBe(3)
    })

    it('handles empty batch', async () => {
      await queue.enqueueBatch([])

      const stats = await queue.getStats()
      expect(stats.total).toBe(0)
    })
  })

  describe('dequeue', () => {
    it('removes item from queue', async () => {
      await queue.enqueue('posts', 'abc123')
      await queue.dequeue('posts', 'abc123')

      const stats = await queue.getStats()
      expect(stats.total).toBe(0)
    })

    it('handles non-existent item gracefully', async () => {
      await expect(queue.dequeue('posts', 'nonexistent')).resolves.not.toThrow()
    })
  })

  describe('dequeueBatch', () => {
    it('removes multiple items at once', async () => {
      await queue.enqueueBatch([
        ['posts', 'abc123'],
        ['posts', 'def456'],
        ['users', 'user1'],
      ])

      await queue.dequeueBatch([
        ['posts', 'abc123'],
        ['posts', 'def456'],
      ])

      const stats = await queue.getStats()
      expect(stats.total).toBe(1)
    })
  })

  describe('processQueue', () => {
    let entityLoader: EntityLoader
    let entityUpdater: EntityUpdater

    beforeEach(() => {
      entityLoader = vi.fn().mockImplementation(async (type, id) => ({
        $id: `${type}/${id}`,
        $type: 'Post',
        name: 'Test Post',
        description: 'This is a test description for embedding',
      }))

      entityUpdater = vi.fn().mockResolvedValue(undefined)

      queue.setEntityLoader(entityLoader)
      queue.setEntityUpdater(entityUpdater)
    })

    it('processes items and generates embeddings', async () => {
      await queue.enqueue('posts', 'abc123')

      const result = await queue.processQueue()

      expect(result.processed).toBe(1)
      expect(result.failed).toBe(0)
      expect(entityLoader).toHaveBeenCalledWith('posts', 'abc123')
      expect(entityUpdater).toHaveBeenCalledWith(
        'posts',
        'abc123',
        'embedding',
        expect.any(Array)
      )
    })

    it('removes processed items from queue', async () => {
      await queue.enqueue('posts', 'abc123')

      await queue.processQueue()

      const stats = await queue.getStats()
      expect(stats.total).toBe(0)
    })

    it('handles entity not found', async () => {
      entityLoader = vi.fn().mockResolvedValue(null)
      queue.setEntityLoader(entityLoader)

      await queue.enqueue('posts', 'missing')

      const result = await queue.processQueue()

      expect(result.failed).toBe(1)
      expect(result.errors[0]).toEqual({
        entityType: 'posts',
        entityId: 'missing',
        error: 'Entity not found',
      })
    })

    it('handles embedding generation failure with retry', async () => {
      const failingProvider = {
        ...createMockProvider(),
        embedBatch: vi.fn().mockRejectedValue(new Error('API error')),
      }

      const failingQueue = new EmbeddingQueue(storage, {
        ...config,
        provider: failingProvider,
      })
      failingQueue.setEntityLoader(entityLoader)
      failingQueue.setEntityUpdater(entityUpdater)

      await failingQueue.enqueue('posts', 'abc123')

      const result = await failingQueue.processQueue()

      expect(result.failed).toBe(1)
      expect(result.errors[0].error).toBe('API error')

      // Item should still be in queue with incremented attempts
      const stats = await failingQueue.getStats()
      expect(stats.total).toBe(1)
      expect(stats.retrying).toBe(1)
    })

    it('handles entity update failure', async () => {
      entityUpdater = vi.fn().mockRejectedValue(new Error('Update failed'))
      queue.setEntityUpdater(entityUpdater)

      await queue.enqueue('posts', 'abc123')

      const result = await queue.processQueue()

      expect(result.failed).toBe(1)
      expect(result.errors[0].error).toBe('Update failed')
    })

    it('processes in batches', async () => {
      const batchConfig = createTestConfig({ batchSize: 2 })
      const batchQueue = new EmbeddingQueue(storage, batchConfig)
      batchQueue.setEntityLoader(entityLoader)
      batchQueue.setEntityUpdater(entityUpdater)

      await batchQueue.enqueueBatch([
        ['posts', 'post1'],
        ['posts', 'post2'],
        ['posts', 'post3'],
        ['posts', 'post4'],
      ])

      const result = await batchQueue.processQueue()

      expect(result.processed).toBe(2)
      expect(result.remaining).toBeGreaterThan(0)
    })

    it('returns empty result for empty queue', async () => {
      const result = await queue.processQueue()

      expect(result.processed).toBe(0)
      expect(result.failed).toBe(0)
      expect(result.remaining).toBe(0)
    })

    it('throws error if entity loader not set', async () => {
      const noLoaderQueue = new EmbeddingQueue(storage, config)
      noLoaderQueue.setEntityUpdater(entityUpdater)

      await noLoaderQueue.enqueue('posts', 'abc123')

      await expect(noLoaderQueue.processQueue()).rejects.toThrow(
        'Entity loader not set'
      )
    })

    it('throws error if entity updater not set', async () => {
      const noUpdaterQueue = new EmbeddingQueue(storage, config)
      noUpdaterQueue.setEntityLoader(entityLoader)

      await noUpdaterQueue.enqueue('posts', 'abc123')

      await expect(noUpdaterQueue.processQueue()).rejects.toThrow(
        'Entity updater not set'
      )
    })
  })

  describe('getStats', () => {
    it('returns correct statistics', async () => {
      await queue.enqueue('posts', 'post1')
      await queue.enqueue('posts', 'post2')

      const stats = await queue.getStats()

      expect(stats.total).toBe(2)
      expect(stats.pending).toBe(2)
      expect(stats.retrying).toBe(0)
      expect(stats.oldestItem).toBeDefined()
    })

    it('returns empty stats for empty queue', async () => {
      const stats = await queue.getStats()

      expect(stats.total).toBe(0)
      expect(stats.pending).toBe(0)
      expect(stats.retrying).toBe(0)
      expect(stats.oldestItem).toBeUndefined()
    })
  })

  describe('getItems', () => {
    it('returns queue items', async () => {
      await queue.enqueue('posts', 'abc123')
      await queue.enqueue('users', 'user1')

      const items = await queue.getItems()

      expect(items).toHaveLength(2)
      expect(items[0]).toMatchObject({
        entityType: expect.any(String),
        entityId: expect.any(String),
        createdAt: expect.any(Number),
        attempts: 0,
      })
    })

    it('respects limit', async () => {
      await queue.enqueueBatch([
        ['posts', 'post1'],
        ['posts', 'post2'],
        ['posts', 'post3'],
      ])

      const items = await queue.getItems(2)

      expect(items).toHaveLength(2)
    })
  })

  describe('clear', () => {
    it('removes all items from queue', async () => {
      await queue.enqueueBatch([
        ['posts', 'post1'],
        ['posts', 'post2'],
      ])

      const cleared = await queue.clear()

      expect(cleared).toBe(2)

      const stats = await queue.getStats()
      expect(stats.total).toBe(0)
    })

    it('returns 0 for empty queue', async () => {
      const cleared = await queue.clear()

      expect(cleared).toBe(0)
    })
  })

  describe('clearFailed', () => {
    it('removes items that have exhausted retries', async () => {
      // Add item and simulate failed attempts
      await queue.enqueue('posts', 'failing')
      const key = 'embed_queue:posts:failing'
      await storage.put(key, {
        entityType: 'posts',
        entityId: 'failing',
        createdAt: Date.now(),
        attempts: 3, // Max retries
      } as EmbeddingQueueItem)

      const cleared = await queue.clearFailed()

      expect(cleared).toBe(1)
    })

    it('keeps items with remaining retries', async () => {
      await queue.enqueue('posts', 'retryable')

      const cleared = await queue.clearFailed()

      expect(cleared).toBe(0)

      const stats = await queue.getStats()
      expect(stats.total).toBe(1)
    })
  })
})

// =============================================================================
// Configuration Builder Tests
// =============================================================================

describe('BackgroundEmbeddingConfigBuilder', () => {
  it('builds valid configuration', () => {
    const provider = createMockProvider()

    const config = configureBackgroundEmbeddings()
      .provider(provider)
      .fields(['description', 'content'])
      .vectorField('embedding')
      .batchSize(20)
      .retryAttempts(5)
      .processDelay(2000)
      .fieldSeparator(' | ')
      .build()

    expect(config.provider).toBe(provider)
    expect(config.fields).toEqual(['description', 'content'])
    expect(config.vectorField).toBe('embedding')
    expect(config.batchSize).toBe(20)
    expect(config.retryAttempts).toBe(5)
    expect(config.processDelay).toBe(2000)
    expect(config.fieldSeparator).toBe(' | ')
  })

  it('throws error without provider', () => {
    expect(() =>
      configureBackgroundEmbeddings()
        .fields(['description'])
        .vectorField('embedding')
        .build()
    ).toThrow('Embedding provider is required')
  })

  it('throws error without fields', () => {
    const provider = createMockProvider()

    expect(() =>
      configureBackgroundEmbeddings()
        .provider(provider)
        .vectorField('embedding')
        .build()
    ).toThrow('At least one source field is required')
  })

  it('throws error with empty fields', () => {
    const provider = createMockProvider()

    expect(() =>
      configureBackgroundEmbeddings()
        .provider(provider)
        .fields([])
        .vectorField('embedding')
        .build()
    ).toThrow('At least one source field is required')
  })

  it('throws error without vectorField', () => {
    const provider = createMockProvider()

    expect(() =>
      configureBackgroundEmbeddings()
        .provider(provider)
        .fields(['description'])
        .build()
    ).toThrow('Vector field is required')
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createEmbeddingQueue', () => {
  it('creates a configured queue', () => {
    const storage = createMockStorage()
    const config = createTestConfig()

    const queue = createEmbeddingQueue(storage, config)

    expect(queue).toBeInstanceOf(EmbeddingQueue)
  })
})

// =============================================================================
// Text Extraction Tests
// =============================================================================

describe('text extraction', () => {
  let storage: ReturnType<typeof createMockStorage>
  let queue: EmbeddingQueue
  let entityUpdater: EntityUpdater
  let provider: EmbeddingProvider

  beforeEach(() => {
    storage = createMockStorage()
    provider = createMockProvider()
    entityUpdater = vi.fn().mockResolvedValue(undefined)
  })

  it('extracts single field', async () => {
    const config = createTestConfig({
      fields: ['description'],
    })
    queue = new EmbeddingQueue(storage, config)

    const embedBatchSpy = vi.spyOn(provider, 'embedBatch')
    queue = new EmbeddingQueue(storage, { ...config, provider })
    queue.setEntityLoader(async () => ({
      description: 'Test description',
      other: 'Not included',
    }))
    queue.setEntityUpdater(entityUpdater)

    await queue.enqueue('posts', 'abc123')
    await queue.processQueue()

    expect(embedBatchSpy).toHaveBeenCalledWith(['Test description'])
  })

  it('concatenates multiple fields', async () => {
    const config = createTestConfig({
      fields: ['title', 'description'],
      fieldSeparator: ' | ',
    })

    const embedBatchSpy = vi.spyOn(provider, 'embedBatch')
    queue = new EmbeddingQueue(storage, { ...config, provider })
    queue.setEntityLoader(async () => ({
      title: 'My Title',
      description: 'My Description',
    }))
    queue.setEntityUpdater(entityUpdater)

    await queue.enqueue('posts', 'abc123')
    await queue.processQueue()

    expect(embedBatchSpy).toHaveBeenCalledWith(['My Title | My Description'])
  })

  it('handles nested fields', async () => {
    const config = createTestConfig({
      fields: ['content.body'],
    })

    const embedBatchSpy = vi.spyOn(provider, 'embedBatch')
    queue = new EmbeddingQueue(storage, { ...config, provider })
    queue.setEntityLoader(async () => ({
      content: {
        body: 'Nested content',
      },
    }))
    queue.setEntityUpdater(entityUpdater)

    await queue.enqueue('posts', 'abc123')
    await queue.processQueue()

    expect(embedBatchSpy).toHaveBeenCalledWith(['Nested content'])
  })

  it('skips empty fields', async () => {
    const config = createTestConfig({
      fields: ['title', 'description', 'missing'],
      fieldSeparator: ' | ',
    })

    const embedBatchSpy = vi.spyOn(provider, 'embedBatch')
    queue = new EmbeddingQueue(storage, { ...config, provider })
    queue.setEntityLoader(async () => ({
      title: 'My Title',
      description: '',
    }))
    queue.setEntityUpdater(entityUpdater)

    await queue.enqueue('posts', 'abc123')
    await queue.processQueue()

    expect(embedBatchSpy).toHaveBeenCalledWith(['My Title'])
  })
})

// =============================================================================
// Priority Tests
// =============================================================================

describe('priority handling', () => {
  let storage: ReturnType<typeof createMockStorage>
  let queue: EmbeddingQueue
  let processedOrder: string[]

  beforeEach(() => {
    storage = createMockStorage()
    processedOrder = []

    const config = createTestConfig({ batchSize: 1 })
    queue = new EmbeddingQueue(storage, config)

    queue.setEntityLoader(async (_, id) => ({
      description: `Entity ${id}`,
    }))
    queue.setEntityUpdater(async (_, entityId) => {
      processedOrder.push(entityId)
    })
  })

  it('processes higher priority items first', async () => {
    await queue.enqueue('posts', 'low', 100)
    await queue.enqueue('posts', 'high', 10)
    await queue.enqueue('posts', 'medium', 50)

    // Process all items one at a time
    await queue.processQueue()
    await queue.processQueue()
    await queue.processQueue()

    expect(processedOrder[0]).toBe('high')
    expect(processedOrder[1]).toBe('medium')
    expect(processedOrder[2]).toBe('low')
  })

  it('processes older items first at same priority', async () => {
    // Add items with slight delay to ensure different timestamps
    await queue.enqueue('posts', 'first', 50)
    await new Promise(r => setTimeout(r, 10))
    await queue.enqueue('posts', 'second', 50)
    await new Promise(r => setTimeout(r, 10))
    await queue.enqueue('posts', 'third', 50)

    // Process all items one at a time
    await queue.processQueue()
    await queue.processQueue()
    await queue.processQueue()

    expect(processedOrder[0]).toBe('first')
    expect(processedOrder[1]).toBe('second')
    expect(processedOrder[2]).toBe('third')
  })
})

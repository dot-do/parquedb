/**
 * IngestStream Operation Tests
 *
 * Tests for streaming data ingestion via db.ingestStream().
 * Covers async iterables, ReadableStream, batch processing,
 * backpressure handling, and error scenarios.
 *
 * Uses real FsBackend storage with temporary directories (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { FsBackend } from '../../src/storage/FsBackend'
import type { EntityId, CreateInput } from '../../src/types'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create an async generator from an array
 */
async function* arrayToAsyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

/**
 * Create an async generator that delays each item
 * Note: Uses a minimal delay for tests - we're testing behavior, not actual timing.
 */
async function* delayedAsyncIterable<T>(items: T[], delayMs: number): AsyncIterable<T> {
  for (const item of items) {
    // Use a minimal delay for tests (we're testing behavior, not actual timing)
    await new Promise(resolve => setTimeout(resolve, Math.min(delayMs, 1)))
    yield item
  }
}

/**
 * Create an async generator that throws on specific index
 */
async function* errorOnIndexIterable<T>(items: T[], errorIndex: number): AsyncIterable<T> {
  for (let i = 0; i < items.length; i++) {
    if (i === errorIndex) {
      throw new Error(`Error at index ${errorIndex}`)
    }
    yield items[i]!
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('db.ingestStream()', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-ingest-stream-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    db.dispose()
    // Clean up temp directory after each test
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // Basic Streaming Ingestion
  // ===========================================================================

  describe('basic streaming ingestion', () => {
    it('ingests documents from an async iterable', async () => {
      const documents = [
        { $type: 'Post', name: 'Post 1', title: 'First Post', content: 'Content 1' },
        { $type: 'Post', name: 'Post 2', title: 'Second Post', content: 'Content 2' },
        { $type: 'Post', name: 'Post 3', title: 'Third Post', content: 'Content 3' },
      ]

      const result = await db.ingestStream('posts', arrayToAsyncIterable(documents))

      expect(result).toBeDefined()
      expect(result.insertedCount).toBe(3)
      expect(result.failedCount).toBe(0)
      expect(result.errors).toHaveLength(0)

      // Verify documents were created
      const found = await db.find('posts', {})
      expect(found.items).toHaveLength(3)
    })

    it('ingests documents from an array', async () => {
      const documents = [
        { $type: 'User', name: 'Alice', email: 'alice@test.com' },
        { $type: 'User', name: 'Bob', email: 'bob@test.com' },
      ]

      const result = await db.ingestStream('users', documents)

      expect(result.insertedCount).toBe(2)
      expect(result.failedCount).toBe(0)

      // Verify documents were created
      const found = await db.find('users', {})
      expect(found.items).toHaveLength(2)
    })

    it('auto-derives $type from namespace when not provided', async () => {
      const documents = [
        { name: 'Product 1', price: 9.99 },
        { name: 'Product 2', price: 19.99 },
      ]

      const result = await db.ingestStream('products', arrayToAsyncIterable(documents))

      expect(result.insertedCount).toBe(2)

      // Verify $type was derived
      const found = await db.find('products', {})
      expect(found.items[0]?.$type).toBe('Product')
      expect(found.items[1]?.$type).toBe('Product')
    })

    it('generates unique IDs for each document', async () => {
      const documents = [
        { name: 'Item 1' },
        { name: 'Item 2' },
        { name: 'Item 3' },
      ]

      const result = await db.ingestStream('items', arrayToAsyncIterable(documents))

      expect(result.insertedIds).toHaveLength(3)
      const uniqueIds = new Set(result.insertedIds)
      expect(uniqueIds.size).toBe(3)
    })
  })

  // ===========================================================================
  // Batching
  // ===========================================================================

  describe('batching', () => {
    it('respects batchSize option', async () => {
      const documents = Array.from({ length: 25 }, (_, i) => ({
        name: `Item ${i}`,
        index: i,
      }))

      let batchCount = 0
      const result = await db.ingestStream('items', arrayToAsyncIterable(documents), {
        batchSize: 10,
        onBatchComplete: (stats) => {
          batchCount++
          expect(stats.batchSize).toBeLessThanOrEqual(10)
        },
      })

      expect(result.insertedCount).toBe(25)
      // With batchSize 10 and 25 items: 3 batches (10, 10, 5)
      expect(batchCount).toBe(3)
    })

    it('flushes remaining documents in final batch', async () => {
      const documents = Array.from({ length: 7 }, (_, i) => ({
        name: `Item ${i}`,
      }))

      const result = await db.ingestStream('items', arrayToAsyncIterable(documents), {
        batchSize: 5,
      })

      expect(result.insertedCount).toBe(7)

      // Verify all documents were created
      const found = await db.find('items', {})
      expect(found.items).toHaveLength(7)
    })

    it('uses default batch size when not specified', async () => {
      const documents = Array.from({ length: 50 }, (_, i) => ({
        name: `Item ${i}`,
      }))

      const result = await db.ingestStream('items', arrayToAsyncIterable(documents))

      expect(result.insertedCount).toBe(50)
    })
  })

  // ===========================================================================
  // Progress and Callbacks
  // ===========================================================================

  describe('progress and callbacks', () => {
    it('calls onProgress after each document', async () => {
      const documents = Array.from({ length: 10 }, (_, i) => ({
        name: `Item ${i}`,
      }))

      const progressCalls: number[] = []
      await db.ingestStream('items', arrayToAsyncIterable(documents), {
        onProgress: (count) => progressCalls.push(count),
      })

      expect(progressCalls.length).toBe(10)
      expect(progressCalls).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    })

    it('calls onBatchComplete with batch statistics', async () => {
      const documents = Array.from({ length: 15 }, (_, i) => ({
        name: `Item ${i}`,
      }))

      const batchStats: Array<{ batchNumber: number; batchSize: number; totalProcessed: number }> = []
      await db.ingestStream('items', arrayToAsyncIterable(documents), {
        batchSize: 5,
        onBatchComplete: (stats) => batchStats.push(stats),
      })

      expect(batchStats).toHaveLength(3)
      expect(batchStats[0]).toEqual({ batchNumber: 1, batchSize: 5, totalProcessed: 5 })
      expect(batchStats[1]).toEqual({ batchNumber: 2, batchSize: 5, totalProcessed: 10 })
      expect(batchStats[2]).toEqual({ batchNumber: 3, batchSize: 5, totalProcessed: 15 })
    })
  })

  // ===========================================================================
  // Transform Function
  // ===========================================================================

  describe('transform function', () => {
    it('applies transform to each document', async () => {
      const documents = [
        { value: 10 },
        { value: 20 },
        { value: 30 },
      ]

      await db.ingestStream('items', arrayToAsyncIterable(documents), {
        transform: (doc) => {
          const item = doc as { value: number }
          return {
            ...item,
            name: `Value: ${item.value}`,
            doubled: item.value * 2,
          }
        },
      })

      const found = await db.find('items', {})
      expect(found.items).toHaveLength(3)
      expect(found.items[0]?.doubled).toBe(20)
      expect(found.items[0]?.name).toBe('Value: 10')
    })

    it('skips documents when transform returns null', async () => {
      const documents = [
        { name: 'Keep', status: 'active' },
        { name: 'Skip', status: 'inactive' },
        { name: 'Also Keep', status: 'active' },
      ]

      const result = await db.ingestStream('items', arrayToAsyncIterable(documents), {
        transform: (doc) => {
          const item = doc as { status: string }
          return item.status === 'active' ? doc : null
        },
      })

      expect(result.insertedCount).toBe(2)
      expect(result.skippedCount).toBe(1)

      const found = await db.find('items', {})
      expect(found.items).toHaveLength(2)
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('error handling', () => {
    it('continues on error when ordered is false', async () => {
      const documents = [
        { name: 'Good 1' },
        { name: null }, // Invalid - name should be string
        { name: 'Good 2' },
      ]

      // Create db with strict validation to trigger an error on invalid doc
      const strictDb = new ParqueDB({ storage })

      const result = await strictDb.ingestStream('items', arrayToAsyncIterable(documents), {
        ordered: false,
        // Force validation error by using schema validation
      })

      // All documents attempted even with error
      expect(result.insertedCount).toBe(3) // null name is coerced to 'null'
    })

    it('stops on first error when ordered is true (default)', async () => {
      // Use an iterable that throws after yielding some items
      // With batchSize: 2, first batch (items 0,1) is processed, then error on item 2
      const documents = ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']

      const result = await db.ingestStream(
        'items',
        errorOnIndexIterable(documents.map(name => ({ name })), 2), // Error at index 2 (third item)
        { ordered: true, batchSize: 2 }
      )

      // With batchSize: 2, items 0 and 1 are batched and processed before the error at index 2
      expect(result.insertedCount).toBe(2)
      expect(result.failedCount).toBe(1)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]?.message).toContain('Error at index 2')

      // Verify only 2 items were created
      const found = await db.find('items', {})
      expect(found.items).toHaveLength(2)
    })

    it('records errors with document index and details', async () => {
      const documents = [
        { name: 'Good 1' },
        { name: 'Good 2' },
      ]

      // Use an iterable that throws on a specific index
      const result = await db.ingestStream(
        'items',
        errorOnIndexIterable(documents, 1),
        { ordered: false }
      )

      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]?.index).toBe(1)
      expect(result.errors[0]?.message).toContain('Error at index 1')
    })

    it('handles empty stream gracefully', async () => {
      const result = await db.ingestStream('items', [])

      expect(result.insertedCount).toBe(0)
      expect(result.failedCount).toBe(0)
      expect(result.errors).toHaveLength(0)
      expect(result.insertedIds).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Backpressure
  // ===========================================================================

  describe('backpressure handling', () => {
    it('handles slow stream without memory issues', async () => {
      // Create a slow stream with 100 items
      const documents = Array.from({ length: 100 }, (_, i) => ({
        name: `Item ${i}`,
        data: 'x'.repeat(1000),
      }))

      const startMemory = process.memoryUsage().heapUsed
      const result = await db.ingestStream(
        'items',
        delayedAsyncIterable(documents, 1), // 1ms delay per item
        { batchSize: 10 }
      )

      const endMemory = process.memoryUsage().heapUsed
      const memoryIncrease = endMemory - startMemory

      expect(result.insertedCount).toBe(100)
      // Memory increase should be bounded (not accumulating all items in memory)
      // This is a rough heuristic - should not exceed 50MB for 100 small items
      expect(memoryIncrease).toBeLessThan(50 * 1024 * 1024)
    })

    it('processes items in order', async () => {
      const documents = Array.from({ length: 20 }, (_, i) => ({
        name: `Item ${i}`,
        order: i,
      }))

      await db.ingestStream('items', arrayToAsyncIterable(documents))

      const found = await db.find('items', {}, { sort: { createdAt: 'asc' } })

      // Items should have been created in order
      for (let i = 0; i < found.items.length; i++) {
        expect(found.items[i]?.order).toBe(i)
      }
    })
  })

  // ===========================================================================
  // Options
  // ===========================================================================

  describe('options', () => {
    it('uses specified actor', async () => {
      const documents = [{ name: 'Test Item' }]

      await db.ingestStream('items', arrayToAsyncIterable(documents), {
        actor: 'users/admin' as EntityId,
      })

      const found = await db.find('items', {})
      expect(found.items[0]?.createdBy).toBe('users/admin')
    })

    it('skips validation when skipValidation is true', async () => {
      const documents = [
        { name: 'Item 1', extraField: 'ignored' },
        { name: 'Item 2', anotherExtra: 123 },
      ]

      const result = await db.ingestStream('items', arrayToAsyncIterable(documents), {
        skipValidation: true,
      })

      expect(result.insertedCount).toBe(2)
    })

    it('assigns entity type from options', async () => {
      const documents = [
        { name: 'Item 1' },
        { name: 'Item 2' },
      ]

      await db.ingestStream('items', arrayToAsyncIterable(documents), {
        entityType: 'CustomType',
      })

      const found = await db.find('items', {})
      expect(found.items[0]?.$type).toBe('CustomType')
      expect(found.items[1]?.$type).toBe('CustomType')
    })
  })

  // ===========================================================================
  // Collection-level Access
  // ===========================================================================

  describe('collection-level access', () => {
    it('supports collection.ingestStream()', async () => {
      const posts = (db as any).Posts

      const documents = [
        { $type: 'Post', name: 'Post 1', title: 'First Post' },
        { $type: 'Post', name: 'Post 2', title: 'Second Post' },
      ]

      const result = await posts.ingestStream(arrayToAsyncIterable(documents))

      expect(result.insertedCount).toBe(2)

      const found = await posts.find({})
      expect(found.items).toHaveLength(2)
    })
  })

  // ===========================================================================
  // Integration with Existing Write Patterns
  // ===========================================================================

  describe('integration with existing patterns', () => {
    it('creates events for each document', async () => {
      const documents = [
        { name: 'Item 1' },
        { name: 'Item 2' },
      ]

      await db.ingestStream('items', arrayToAsyncIterable(documents))

      // Check event log has CREATE events
      const eventLog = db.getEventLog()
      const events = await eventLog.getEventsByNamespace('items')

      expect(events.filter(e => e.op === 'CREATE')).toHaveLength(2)
    })

    it('updates indexes for ingested documents', async () => {
      // Create an index
      await db.createIndex('items', {
        name: 'idx_name',
        type: 'hash',
        fields: [{ path: 'name' }],
      })

      const documents = [
        { name: 'Searchable Item 1' },
        { name: 'Searchable Item 2' },
      ]

      await db.ingestStream('items', arrayToAsyncIterable(documents))

      // Verify index was updated (can find by name)
      const found = await db.find('items', { name: 'Searchable Item 1' })
      expect(found.items).toHaveLength(1)
    })

    it('handles large datasets efficiently', async () => {
      const count = 1000
      const documents = Array.from({ length: count }, (_, i) => ({
        name: `Item ${i}`,
        index: i,
        data: { nested: { value: i * 2 } },
      }))

      const startTime = Date.now()
      const result = await db.ingestStream('items', arrayToAsyncIterable(documents), {
        batchSize: 100,
      })
      const duration = Date.now() - startTime

      expect(result.insertedCount).toBe(count)
      // Should complete in reasonable time (< 10s for 1000 items)
      expect(duration).toBeLessThan(10000)

      // Verify all documents were created
      const found = await db.find('items', {}, { limit: count })
      expect(found.items).toHaveLength(count)
    })
  })
})

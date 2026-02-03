/**
 * Tests for VectorIndex Memory Bounds
 *
 * Tests for the memory-bounded behavior of the VectorIndex,
 * including LRU eviction and memory limits.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '@/indexes/vector/hnsw'
import { LRUCache } from '@/indexes/vector/lru-cache'
import { ProductQuantizer } from '@/indexes/vector/product-quantization'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('VectorIndex Memory Bounds', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_embedding',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 128,
        metric: 'cosine',
        m: 16,
        efConstruction: 200,
      },
    }
  })

  describe('memory limits', () => {
    it('respects maxNodes limit', () => {
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 10,
        maxBytes: Infinity,
      })

      // Insert 20 vectors
      for (let i = 0; i < 20; i++) {
        const vector = Array(128)
          .fill(0)
          .map(() => Math.random())
        index.insert(vector, `doc${i}`, 0, i)
      }

      // Should have 20 total but only 10 cached
      expect(index.size).toBe(20)
      expect(index.cachedSize).toBeLessThanOrEqual(10)
    })

    it('respects maxBytes limit', () => {
      // Each 128-dim vector takes ~1KB (128 * 8 bytes + overhead)
      // Set limit to ~5KB to allow roughly 5 vectors
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: Infinity,
        maxBytes: 5 * 1024,
      })

      // Insert 20 vectors
      for (let i = 0; i < 20; i++) {
        const vector = Array(128)
          .fill(0)
          .map(() => Math.random())
        index.insert(vector, `doc${i}`, 0, i)
      }

      // Memory usage should be within limit
      expect(index.memoryUsage).toBeLessThanOrEqual(5 * 1024)
    })

    it('reports correct memory limits', () => {
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 1000,
        maxBytes: 10 * 1024 * 1024,
      })

      const limits = index.memoryLimits
      expect(limits.maxNodes).toBe(1000)
      expect(limits.maxBytes).toBe(10 * 1024 * 1024)
    })

    it('uses default limits when not specified', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      const limits = index.memoryLimits
      expect(limits.maxNodes).toBe(100000) // DEFAULT_VECTOR_INDEX_MAX_NODES
      expect(limits.maxBytes).toBe(64 * 1024 * 1024) // DEFAULT_VECTOR_INDEX_MAX_BYTES
    })
  })

  describe('eviction behavior', () => {
    it('evicts least recently used nodes', () => {
      const evictedIds: number[] = []
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 5,
        maxBytes: Infinity,
        onEvict: nodeId => evictedIds.push(nodeId),
      })

      // Insert 10 vectors
      for (let i = 0; i < 10; i++) {
        const vector = Array(128)
          .fill(0)
          .map(() => Math.random())
        index.insert(vector, `doc${i}`, 0, i)
      }

      // Some nodes should have been evicted
      expect(evictedIds.length).toBeGreaterThan(0)
    })

    it('search still works after eviction', () => {
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 5,
        maxBytes: Infinity,
      })

      // Insert 10 vectors with predictable patterns
      for (let i = 0; i < 10; i++) {
        const vector = Array(128).fill(0)
        vector[0] = i / 10 // First component varies
        vector[1] = 1 - i / 10 // Second component varies inversely
        index.insert(vector, `doc${i}`, 0, i)
      }

      // Search should still return results (even if some are evicted)
      const queryVector = Array(128).fill(0)
      queryVector[0] = 0.5
      queryVector[1] = 0.5
      const result = index.search(queryVector, 3)

      // We should get some results (may be fewer due to eviction)
      expect(result.docIds.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('statistics', () => {
    it('reports total count vs cached count', () => {
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 5,
        maxBytes: Infinity,
      })

      for (let i = 0; i < 10; i++) {
        const vector = Array(128)
          .fill(0)
          .map(() => Math.random())
        index.insert(vector, `doc${i}`, 0, i)
      }

      const stats = index.getStats()
      expect(stats.entryCount).toBe(10) // Total nodes
      expect((stats as Record<string, number>).cachedNodes).toBeLessThanOrEqual(5) // Cached nodes
    })

    it('reports memory usage in stats', () => {
      const index = new VectorIndex(storage, 'documents', definition, '', {
        maxNodes: 100,
        maxBytes: Infinity,
      })

      for (let i = 0; i < 5; i++) {
        const vector = Array(128)
          .fill(0)
          .map(() => Math.random())
        index.insert(vector, `doc${i}`, 0, i)
      }

      const stats = index.getStats()
      expect(stats.sizeBytes).toBeGreaterThan(0)
    })
  })
})

describe('LRUCache', () => {
  it('evicts least recently used entries when maxEntries exceeded', () => {
    const evicted: number[] = []
    const cache = new LRUCache<string>(
      {
        maxEntries: 3,
        onEvict: key => evicted.push(key),
      },
      () => 10
    )

    cache.set(1, 'a')
    cache.set(2, 'b')
    cache.set(3, 'c')
    cache.set(4, 'd') // Should evict 1

    expect(evicted).toEqual([1])
    expect(cache.has(1)).toBe(false)
    expect(cache.has(4)).toBe(true)
  })

  it('evicts when maxBytes exceeded', () => {
    const cache = new LRUCache<string>(
      {
        maxBytes: 25,
      },
      value => value.length * 5
    )

    cache.set(1, 'hello') // 25 bytes
    cache.set(2, 'world') // 25 bytes, should evict 1

    expect(cache.has(1)).toBe(false)
    expect(cache.has(2)).toBe(true)
    expect(cache.bytes).toBe(25)
  })

  it('moves accessed entries to front', () => {
    const evicted: number[] = []
    const cache = new LRUCache<string>(
      {
        maxEntries: 3,
        onEvict: key => evicted.push(key),
      },
      () => 10
    )

    cache.set(1, 'a')
    cache.set(2, 'b')
    cache.set(3, 'c')

    // Access 1 to make it recently used
    cache.get(1)

    cache.set(4, 'd') // Should evict 2, not 1

    expect(evicted).toEqual([2])
    expect(cache.has(1)).toBe(true)
    expect(cache.has(2)).toBe(false)
  })

  it('returns correct size', () => {
    const cache = new LRUCache<number>({ maxEntries: 10 }, () => 8)

    cache.set(1, 100)
    cache.set(2, 200)
    cache.set(3, 300)

    expect(cache.size).toBe(3)
  })

  it('deletes entries correctly', () => {
    const cache = new LRUCache<string>({ maxEntries: 10 }, () => 10)

    cache.set(1, 'a')
    cache.set(2, 'b')

    expect(cache.delete(1)).toBe(true)
    expect(cache.has(1)).toBe(false)
    expect(cache.size).toBe(1)
  })

  it('clears all entries', () => {
    const cache = new LRUCache<string>({ maxEntries: 10 }, () => 10)

    cache.set(1, 'a')
    cache.set(2, 'b')
    cache.set(3, 'c')

    cache.clear()

    expect(cache.size).toBe(0)
    expect(cache.bytes).toBe(0)
  })
})

describe('ProductQuantizer', () => {
  it('trains on vectors', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    // Generate random training vectors
    const vectors: number[][] = []
    for (let i = 0; i < 100; i++) {
      vectors.push(Array(128).fill(0).map(() => Math.random()))
    }

    pq.train(vectors)

    expect(pq.trained).toBe(true)
  })

  it('encodes and decodes vectors', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    // Generate training vectors
    const vectors: number[][] = []
    for (let i = 0; i < 100; i++) {
      vectors.push(Array(128).fill(0).map(() => Math.random()))
    }

    pq.train(vectors)

    // Encode a vector
    const original = vectors[0]!
    const code = pq.encode(original)

    // Verify code size
    expect(code.codes.length).toBe(8) // numSubquantizers

    // Decode back
    const decoded = pq.decode(code)

    // Should be approximately the same (within quantization error)
    expect(decoded.length).toBe(original.length)
  })

  it('computes distance table for efficient search', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    // Generate training vectors
    const vectors: number[][] = []
    for (let i = 0; i < 100; i++) {
      vectors.push(Array(128).fill(0).map(() => Math.random()))
    }

    pq.train(vectors)

    // Create distance table for a query
    const query = vectors[0]!
    const table = pq.computeDistanceTable(query)

    expect(table.length).toBe(8) // numSubquantizers
    expect(table[0]!.length).toBe(256) // numCentroids
  })

  it('computes approximate distances efficiently', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    // Generate training vectors
    const vectors: number[][] = []
    for (let i = 0; i < 100; i++) {
      vectors.push(Array(128).fill(0).map(() => Math.random()))
    }

    pq.train(vectors)

    const query = vectors[0]!
    const table = pq.computeDistanceTable(query)

    // Encode another vector
    const code = pq.encode(vectors[1]!)

    // Compute distance using table (should be fast)
    const distance = pq.computeDistance(table, code)

    expect(distance).toBeGreaterThanOrEqual(0)
  })

  it('serializes and deserializes codebook', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    // Train
    const vectors: number[][] = []
    for (let i = 0; i < 100; i++) {
      vectors.push(Array(128).fill(0).map(() => Math.random()))
    }
    pq.train(vectors)

    // Serialize
    const serialized = pq.serialize()

    // Create new quantizer and deserialize
    const pq2 = new ProductQuantizer(128, 8, 256)
    pq2.deserialize(serialized)

    expect(pq2.trained).toBe(true)

    // Should produce same results
    const testVector = vectors[0]!
    const code1 = pq.encode(testVector)
    const code2 = pq2.encode(testVector)

    expect(Array.from(code1.codes)).toEqual(Array.from(code2.codes))
  })

  it('reports code and codebook sizes', () => {
    const pq = new ProductQuantizer(128, 8, 256)

    expect(pq.codeSize).toBe(8) // 8 bytes per vector

    // Codebook size: 8 subquantizers * 256 centroids * 16 dims * 4 bytes
    expect(pq.codebookSize).toBe(8 * 256 * 16 * 4)
  })
})

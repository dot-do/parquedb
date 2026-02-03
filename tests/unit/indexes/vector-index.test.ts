/**
 * Tests for Vector Index (HNSW)
 *
 * Tests for approximate nearest neighbor search using HNSW algorithm.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '@/indexes/vector/hnsw'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition, VectorIndexOptions } from '@/indexes/types'

describe('VectorIndex', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_embedding',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 3,
        metric: 'cosine',
        m: 16,
        efConstruction: 200,
      },
    }
  })

  describe('basic operations', () => {
    it('inserts vectors and looks up nearest neighbors', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      // Insert some vectors
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)
      index.insert([0.0, 0.0, 1.0], 'doc3', 0, 2)
      index.insert([0.9, 0.1, 0.0], 'doc4', 0, 3) // Similar to doc1

      // Search for vectors similar to [1, 0, 0]
      const result = index.search([1.0, 0.0, 0.0], 2)

      expect(result.docIds).toHaveLength(2)
      expect(result.docIds[0]).toBe('doc1') // Exact match should be first
      expect(result.docIds[1]).toBe('doc4') // Most similar should be second
    })

    it('returns k results when enough vectors exist', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      for (let i = 0; i < 10; i++) {
        index.insert([i / 10, 1 - i / 10, 0], `doc${i}`, 0, i)
      }

      const result = index.search([0.5, 0.5, 0], 5)
      expect(result.docIds).toHaveLength(5)
    })

    it('returns fewer results when not enough vectors exist', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)

      const result = index.search([1.0, 0.0, 0.0], 5)
      expect(result.docIds).toHaveLength(2)
    })

    it('handles empty index', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      const result = index.search([1.0, 0.0, 0.0], 5)
      expect(result.docIds).toHaveLength(0)
    })

    it('reports correct statistics', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)
      index.insert([0.0, 0.0, 1.0], 'doc3', 0, 2)

      expect(index.size).toBe(3)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(3)
      expect(stats.sizeBytes).toBeGreaterThan(0)
      expect(stats.dimensions).toBe(3)
    })
  })

  describe('distance metrics', () => {
    it('uses cosine similarity correctly', () => {
      const cosineDef: IndexDefinition = {
        name: 'idx_cosine',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'documents', cosineDef)

      // Two vectors with same direction but different magnitudes should be similar
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([10.0, 0.0, 0.0], 'doc2', 0, 1) // Same direction, different magnitude
      index.insert([0.0, 1.0, 0.0], 'doc3', 0, 2) // Orthogonal

      const result = index.search([2.0, 0.0, 0.0], 2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc2')
      // doc3 should not be in top 2 since it's orthogonal
    })

    it('uses euclidean distance correctly', () => {
      const euclideanDef: IndexDefinition = {
        name: 'idx_euclidean',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'euclidean',
        },
      }

      const index = new VectorIndex(storage, 'documents', euclideanDef)

      index.insert([0.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([1.0, 0.0, 0.0], 'doc2', 0, 1)
      index.insert([10.0, 0.0, 0.0], 'doc3', 0, 2)

      const result = index.search([0.0, 0.0, 0.0], 2)
      expect(result.docIds[0]).toBe('doc1') // Exact match
      expect(result.docIds[1]).toBe('doc2') // Closest by euclidean distance
    })

    it('uses dot product correctly', () => {
      const dotDef: IndexDefinition = {
        name: 'idx_dot',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'dot',
        },
      }

      const index = new VectorIndex(storage, 'documents', dotDef)

      index.insert([1.0, 1.0, 1.0], 'doc1', 0, 0)
      index.insert([0.1, 0.1, 0.1], 'doc2', 0, 1)
      index.insert([-1.0, -1.0, -1.0], 'doc3', 0, 2)

      // For dot product, higher is better
      const result = index.search([1.0, 1.0, 1.0], 2)
      expect(result.docIds[0]).toBe('doc1') // Highest dot product
    })
  })

  describe('remove', () => {
    it('removes entries by docId', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)
      index.insert([0.0, 0.0, 1.0], 'doc3', 0, 2)

      expect(index.size).toBe(3)

      const removed = index.remove('doc2')
      expect(removed).toBe(true)
      expect(index.size).toBe(2)

      // doc2 should no longer appear in search results
      const result = index.search([0.0, 1.0, 0.0], 3)
      expect(result.docIds).not.toContain('doc2')
    })

    it('returns false when entry not found', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)

      expect(index.remove('nonexistent')).toBe(false)
    })
  })

  describe('update', () => {
    it('updates vector for existing document', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)

      // Update doc1's vector to be similar to doc2
      const updated = index.update([0.0, 0.9, 0.1], 'doc1', 0, 0)
      expect(updated).toBe(true)

      // Now doc1 should be near [0, 1, 0]
      const result = index.search([0.0, 1.0, 0.0], 2)
      expect(result.docIds).toContain('doc1')
    })

    it('returns false when entry not found', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)

      expect(index.update([0.0, 1.0, 0.0], 'nonexistent', 0, 0)).toBe(false)
    })
  })

  describe('clear', () => {
    it('removes all entries', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)

      expect(index.size).toBe(2)

      index.clear()

      expect(index.size).toBe(0)
      expect(index.search([1.0, 0.0, 0.0], 5).docIds).toHaveLength(0)
    })
  })

  describe('buildFromArray', () => {
    it('builds index from document array', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      const docs = [
        { doc: { $id: 'doc1', embedding: [1.0, 0.0, 0.0] }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { $id: 'doc2', embedding: [0.0, 1.0, 0.0] }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { doc: { $id: 'doc3', embedding: [0.0, 0.0, 1.0] }, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(3)
      const result = index.search([1.0, 0.0, 0.0], 1)
      expect(result.docIds[0]).toBe('doc1')
    })

    it('handles nested field paths', () => {
      const nestedDef: IndexDefinition = {
        name: 'idx_nested',
        type: 'vector',
        fields: [{ path: 'metadata.embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'documents', nestedDef)

      const docs = [
        { doc: { metadata: { embedding: [1.0, 0.0, 0.0] } }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { metadata: { embedding: [0.0, 1.0, 0.0] } }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(2)
    })

    it('skips documents with missing vectors', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      const docs = [
        { doc: { $id: 'doc1', embedding: [1.0, 0.0, 0.0] }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { $id: 'doc2' }, docId: 'doc2', rowGroup: 0, rowOffset: 1 }, // missing embedding
        { doc: { $id: 'doc3', embedding: [0.0, 0.0, 1.0] }, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(2) // doc2 skipped
    })

    it('validates vector dimensions', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      const docs = [
        { doc: { $id: 'doc1', embedding: [1.0, 0.0, 0.0] }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { $id: 'doc2', embedding: [1.0, 0.0] }, docId: 'doc2', rowGroup: 0, rowOffset: 1 }, // wrong dimensions
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(1) // doc2 skipped due to wrong dimensions
    })
  })

  describe('persistence', () => {
    it('saves and loads index', async () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)
      index.insert([0.0, 0.0, 1.0], 'doc3', 0, 2)

      await index.save()

      // Create new index instance and load
      const loaded = new VectorIndex(storage, 'documents', definition)
      await loaded.load()

      expect(loaded.size).toBe(3)

      // Search should work on loaded index
      const result = loaded.search([1.0, 0.0, 0.0], 2)
      expect(result.docIds).toContain('doc1')
    })

    it('handles empty index', async () => {
      const index = new VectorIndex(storage, 'documents', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })

    it('throws error on metric mismatch during load', async () => {
      // Create and save index with cosine metric
      const cosineDefinition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }
      const index = new VectorIndex(storage, 'documents', cosineDefinition)
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      await index.save()

      // Try to load with euclidean metric - should fail
      const euclideanDefinition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'euclidean',
        },
      }
      const mismatchedIndex = new VectorIndex(storage, 'documents', euclideanDefinition)
      await expect(mismatchedIndex.load()).rejects.toThrow(/metric mismatch/i)
    })

    it('throws error when loading dot metric index with cosine metric', async () => {
      // Create and save index with dot metric
      const dotDefinition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'dot',
        },
      }
      const index = new VectorIndex(storage, 'documents', dotDefinition)
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      await index.save()

      // Try to load with cosine metric - should fail
      const cosineDefinition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }
      const mismatchedIndex = new VectorIndex(storage, 'documents', cosineDefinition)
      await expect(mismatchedIndex.load()).rejects.toThrow(/metric mismatch/i)
    })
  })

  describe('search with score filter', () => {
    it('filters results by minimum score', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.9, 0.1, 0.0], 'doc2', 0, 1) // High similarity
      index.insert([0.0, 1.0, 0.0], 'doc3', 0, 2) // Low similarity

      const result = index.search([1.0, 0.0, 0.0], 10, { minScore: 0.9 })

      // Only doc1 and doc2 should pass the score threshold
      expect(result.docIds.length).toBeLessThanOrEqual(2)
      expect(result.docIds).toContain('doc1')
    })

    it('returns scores with results', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.5, 0.5, 0.0], 'doc2', 0, 1)

      const result = index.search([1.0, 0.0, 0.0], 2)

      expect(result.scores).toBeDefined()
      expect(result.scores!.length).toBe(result.docIds.length)
      expect(result.scores![0]).toBeGreaterThanOrEqual(result.scores![1]!)
    })
  })

  describe('row group tracking', () => {
    it('returns unique row groups for search results', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      // Insert vectors across multiple row groups
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.9, 0.1, 0.0], 'doc2', 1, 0)
      index.insert([0.8, 0.2, 0.0], 'doc3', 2, 0)

      const result = index.search([1.0, 0.0, 0.0], 3)

      expect(result.docIds).toHaveLength(3)
      expect(result.rowGroups).toContain(0)
      expect(result.rowGroups).toContain(1)
      expect(result.rowGroups).toContain(2)
    })
  })

  describe('HNSW parameters', () => {
    it('respects efSearch parameter for recall/speed tradeoff', () => {
      const index = new VectorIndex(storage, 'documents', definition)

      // Insert many vectors
      for (let i = 0; i < 100; i++) {
        const angle = (i / 100) * Math.PI * 2
        index.insert([Math.cos(angle), Math.sin(angle), 0], `doc${i}`, 0, i)
      }

      // Search with different efSearch values
      const lowEf = index.search([1.0, 0.0, 0.0], 5, { efSearch: 10 })
      const highEf = index.search([1.0, 0.0, 0.0], 5, { efSearch: 100 })

      // Both should return results (exact behavior depends on implementation)
      expect(lowEf.docIds.length).toBeGreaterThan(0)
      expect(highEf.docIds.length).toBeGreaterThan(0)
    })
  })
})

describe('Distance functions', () => {
  describe('cosineDistance', () => {
    it('returns 0 for identical vectors', async () => {
      const { cosineDistance } = await import('@/indexes/vector/distance')
      expect(cosineDistance([1, 0, 0], [1, 0, 0])).toBeCloseTo(0, 5)
    })

    it('returns 1 for orthogonal vectors', async () => {
      const { cosineDistance } = await import('@/indexes/vector/distance')
      expect(cosineDistance([1, 0, 0], [0, 1, 0])).toBeCloseTo(1, 5)
    })

    it('returns 2 for opposite vectors', async () => {
      const { cosineDistance } = await import('@/indexes/vector/distance')
      expect(cosineDistance([1, 0, 0], [-1, 0, 0])).toBeCloseTo(2, 5)
    })
  })

  describe('euclideanDistance', () => {
    it('returns 0 for identical vectors', async () => {
      const { euclideanDistance } = await import('@/indexes/vector/distance')
      expect(euclideanDistance([1, 0, 0], [1, 0, 0])).toBe(0)
    })

    it('calculates correct distance', async () => {
      const { euclideanDistance } = await import('@/indexes/vector/distance')
      expect(euclideanDistance([0, 0, 0], [3, 4, 0])).toBeCloseTo(5, 5)
    })
  })

  describe('dotProductDistance', () => {
    it('returns correct distance for dot product', async () => {
      const { dotProductDistance } = await import('@/indexes/vector/distance')
      // Dot product of [1,1,1] and [1,1,1] is 3
      // Distance is negative dot product (lower distance = higher similarity)
      expect(dotProductDistance([1, 1, 1], [1, 1, 1])).toBe(-3)
    })
  })
})

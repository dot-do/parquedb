/**
 * Integration Tests for Vector Similarity Search
 *
 * Tests the end-to-end functionality of vector similarity search
 * using the HNSW index and $vector query operator.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '../../src/indexes/vector/hnsw'
import { IndexManager } from '../../src/indexes/manager'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { IndexDefinition } from '../../src/indexes/types'

describe('Vector Search Integration', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('VectorIndex with movie embeddings', () => {
    it('finds similar movies by embedding', () => {
      const definition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 4,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'movies', definition)

      // Simulated embeddings for different movie genres
      // Action movies: high on first dimension
      // Comedy movies: high on second dimension
      // Drama movies: high on third dimension
      // Sci-fi movies: high on fourth dimension
      const movies = [
        { docId: 'matrix', embedding: [0.9, 0.1, 0.1, 0.8] }, // Action + Sci-fi
        { docId: 'inception', embedding: [0.7, 0.1, 0.3, 0.9] }, // Action + Sci-fi
        { docId: 'hangover', embedding: [0.1, 0.9, 0.1, 0.1] }, // Comedy
        { docId: 'superbad', embedding: [0.2, 0.8, 0.2, 0.1] }, // Comedy
        { docId: 'titanic', embedding: [0.2, 0.1, 0.9, 0.1] }, // Drama
        { docId: 'forrest_gump', embedding: [0.2, 0.3, 0.8, 0.1] }, // Drama + slight comedy
      ]

      for (const movie of movies) {
        index.insert(movie.embedding, movie.docId, 0, 0)
      }

      // Find movies similar to The Matrix
      const matrixResults = index.search([0.9, 0.1, 0.1, 0.8], 3)
      expect(matrixResults.docIds).toHaveLength(3)
      expect(matrixResults.docIds[0]).toBe('matrix') // Exact match
      expect(matrixResults.docIds).toContain('inception') // Similar genre

      // Find movies similar to comedies
      const comedyResults = index.search([0.1, 0.9, 0.1, 0.1], 2)
      expect(comedyResults.docIds).toContain('hangover')
      expect(comedyResults.docIds).toContain('superbad')

      // Verify scores are included and ordered
      expect(matrixResults.scores).toBeDefined()
      expect(matrixResults.scores![0]).toBeGreaterThanOrEqual(matrixResults.scores![1]!)
    })

    it('respects minScore filter', () => {
      const definition: IndexDefinition = {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'docs', definition)

      // Insert vectors with varying similarity to [1, 0, 0]
      index.insert([1.0, 0.0, 0.0], 'identical', 0, 0) // Score ~1.0
      index.insert([0.9, 0.1, 0.0], 'very_similar', 0, 1) // Score ~0.99
      index.insert([0.5, 0.5, 0.0], 'somewhat_similar', 0, 2) // Score ~0.71
      index.insert([0.0, 1.0, 0.0], 'orthogonal', 0, 3) // Score ~0.0
      index.insert([-1.0, 0.0, 0.0], 'opposite', 0, 4) // Score ~-1.0 (negative cosine)

      // Search with high minScore threshold
      const highThresholdResults = index.search([1.0, 0.0, 0.0], 10, {
        minScore: 0.9,
      })

      // Only very similar vectors should match
      expect(highThresholdResults.docIds.length).toBeLessThanOrEqual(3)
      expect(highThresholdResults.docIds).toContain('identical')
    })
  })

  describe('VectorIndex with different metrics', () => {
    it('euclidean distance finds nearest points', () => {
      const definition: IndexDefinition = {
        name: 'idx_euclidean',
        type: 'vector',
        fields: [{ path: 'coords' }],
        vectorOptions: {
          dimensions: 2,
          metric: 'euclidean',
        },
      }

      const index = new VectorIndex(storage, 'points', definition)

      // Grid of points
      index.insert([0.0, 0.0], 'origin', 0, 0)
      index.insert([1.0, 0.0], 'right', 0, 1)
      index.insert([0.0, 1.0], 'up', 0, 2)
      index.insert([1.0, 1.0], 'diagonal', 0, 3)
      index.insert([0.5, 0.5], 'center', 0, 4)

      // Search from [0.4, 0.4] - should be closest to center
      const results = index.search([0.4, 0.4], 3)
      expect(results.docIds[0]).toBe('center')
      // Next should be origin (distance sqrt(0.32)) not diagonal (distance sqrt(0.72))
      expect(results.docIds).toContain('origin')
    })

    it('dot product prefers aligned directions with magnitude', () => {
      const definition: IndexDefinition = {
        name: 'idx_dot',
        type: 'vector',
        fields: [{ path: 'vec' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'dot',
        },
      }

      const index = new VectorIndex(storage, 'vectors', definition)

      // For dot product, larger magnitude in the query direction = higher score
      index.insert([2.0, 0.0, 0.0], 'large_x', 0, 0) // dot with [1,0,0] = 2
      index.insert([1.0, 0.0, 0.0], 'unit_x', 0, 1) // dot with [1,0,0] = 1
      index.insert([0.5, 0.0, 0.0], 'small_x', 0, 2) // dot with [1,0,0] = 0.5
      index.insert([0.0, 1.0, 0.0], 'unit_y', 0, 3) // dot with [1,0,0] = 0

      const results = index.search([1.0, 0.0, 0.0], 4)
      // Highest dot product should be first
      expect(results.docIds[0]).toBe('large_x')
      expect(results.docIds[1]).toBe('unit_x')
      expect(results.docIds[2]).toBe('small_x')
    })
  })

  describe('IndexManager with vector indexes', () => {
    it('creates and manages vector indexes', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('documents', {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 128,
          metric: 'cosine',
        },
      })

      const indexes = await manager.listIndexes('documents')
      expect(indexes).toHaveLength(1)
      expect(indexes[0].definition.type).toBe('vector')
      expect(indexes[0].definition.vectorOptions?.dimensions).toBe(128)
    })

    it('selects vector index for $vector queries', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('products', {
        name: 'idx_product_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 64,
          metric: 'cosine',
        },
      })

      const plan = await manager.selectIndex('products', {
        $vector: {
          $near: new Array(64).fill(0.1),
          $k: 10,
          $field: 'embedding',
        },
      })

      expect(plan).not.toBeNull()
      expect(plan?.type).toBe('vector')
      expect(plan?.index.name).toBe('idx_product_embedding')
    })

    it('executes vector search through IndexManager', async () => {
      const manager = new IndexManager(storage)

      // Create index
      await manager.createIndex('articles', {
        name: 'idx_article_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      })

      // Get the vector index and populate it
      const vectorIndex = await (manager as any).getVectorIndex(
        'articles',
        'idx_article_embedding'
      )

      // Add some test vectors
      vectorIndex.insert([1.0, 0.0, 0.0], 'article1', 0, 0)
      vectorIndex.insert([0.9, 0.1, 0.0], 'article2', 0, 1)
      vectorIndex.insert([0.0, 1.0, 0.0], 'article3', 0, 2)

      // Search via IndexManager
      const results = await manager.vectorSearch(
        'articles',
        'idx_article_embedding',
        [1.0, 0.0, 0.0],
        2
      )

      expect(results.docIds).toHaveLength(2)
      expect(results.docIds[0]).toBe('article1')
      expect(results.docIds[1]).toBe('article2')
    })
  })

  describe('VectorIndex with real-world scenarios', () => {
    it('semantic search simulation', () => {
      const definition: IndexDefinition = {
        name: 'idx_semantic',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 5,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'faqs', definition)

      // Simulated semantic embeddings for FAQ entries
      // Dimension meanings: [tech, billing, shipping, returns, general]
      const faqs = [
        { id: 'tech_reset', embedding: [0.9, 0.0, 0.0, 0.1, 0.1] },
        { id: 'tech_connect', embedding: [0.85, 0.05, 0.0, 0.0, 0.1] },
        { id: 'billing_refund', embedding: [0.1, 0.8, 0.0, 0.3, 0.1] },
        { id: 'billing_update', embedding: [0.0, 0.9, 0.0, 0.0, 0.1] },
        { id: 'shipping_track', embedding: [0.0, 0.1, 0.9, 0.1, 0.1] },
        { id: 'shipping_delay', embedding: [0.0, 0.1, 0.85, 0.0, 0.2] },
        { id: 'returns_policy', embedding: [0.0, 0.2, 0.1, 0.9, 0.1] },
        { id: 'returns_process', embedding: [0.1, 0.1, 0.1, 0.85, 0.2] },
      ]

      for (const faq of faqs) {
        index.insert(faq.embedding, faq.id, 0, 0)
      }

      // User query: "How do I get a refund?" -> billing + returns
      const refundQuery = [0.1, 0.6, 0.0, 0.5, 0.1]
      const refundResults = index.search(refundQuery, 3)
      expect(refundResults.docIds).toContain('billing_refund')
      expect(refundResults.docIds).toContain('returns_policy')

      // User query: "Track my package" -> shipping
      const shippingQuery = [0.0, 0.0, 0.9, 0.1, 0.1]
      const shippingResults = index.search(shippingQuery, 2)
      expect(shippingResults.docIds[0]).toBe('shipping_track')
      expect(shippingResults.docIds[1]).toBe('shipping_delay')
    })

    it('handles high-dimensional vectors', () => {
      const definition: IndexDefinition = {
        name: 'idx_high_dim',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 128,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'docs', definition)

      // Generate 100 random 128-dimensional vectors
      const docs = Array.from({ length: 100 }, (_, i) => {
        const embedding = Array.from({ length: 128 }, () => Math.random())
        return { id: `doc${i}`, embedding }
      })

      for (const doc of docs) {
        index.insert(doc.embedding, doc.id, 0, 0)
      }

      // Search should still work efficiently
      const queryVector = Array.from({ length: 128 }, () => Math.random())
      const startTime = Date.now()
      const results = index.search(queryVector, 10)
      const searchTime = Date.now() - startTime

      expect(results.docIds).toHaveLength(10)
      expect(searchTime).toBeLessThan(100) // Should complete in < 100ms
    })

    it('persistence and reload', async () => {
      const definition: IndexDefinition = {
        name: 'idx_persistent',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }

      // Create and populate index
      const index1 = new VectorIndex(storage, 'test', definition)
      index1.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index1.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)
      index1.insert([0.0, 0.0, 1.0], 'doc3', 0, 2)

      // Save to storage
      await index1.save()

      // Create new instance and load
      const index2 = new VectorIndex(storage, 'test', definition)
      await index2.load()

      // Verify data persisted
      expect(index2.size).toBe(3)

      // Verify search works on reloaded index
      const results = index2.search([1.0, 0.0, 0.0], 1)
      expect(results.docIds[0]).toBe('doc1')
    })
  })

  describe('Edge cases', () => {
    it('handles empty index gracefully', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('empty', {
        name: 'idx_empty',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      })

      const results = await manager.vectorSearch(
        'empty',
        'idx_empty',
        [1.0, 0.0, 0.0],
        10
      )

      expect(results.docIds).toHaveLength(0)
      expect(results.scores).toBeDefined()
    })

    it('handles non-existent index', async () => {
      const manager = new IndexManager(storage)

      const results = await manager.vectorSearch(
        'nonexistent',
        'idx_fake',
        [1.0, 0.0, 0.0],
        10
      )

      expect(results.docIds).toHaveLength(0)
    })

    it('handles k larger than index size', () => {
      const definition: IndexDefinition = {
        name: 'idx_small',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 3,
          metric: 'cosine',
        },
      }

      const index = new VectorIndex(storage, 'small', definition)
      index.insert([1.0, 0.0, 0.0], 'doc1', 0, 0)
      index.insert([0.0, 1.0, 0.0], 'doc2', 0, 1)

      // Request more results than available
      const results = index.search([1.0, 0.0, 0.0], 100)
      expect(results.docIds).toHaveLength(2) // Only returns available docs
    })
  })
})

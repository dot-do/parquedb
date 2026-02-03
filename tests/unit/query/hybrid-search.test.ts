/**
 * Unit Tests for Hybrid Search (Vector + Metadata Filtering)
 *
 * Tests the hybrid search functionality that combines vector similarity search
 * with MongoDB-style metadata filtering.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '../../../src/indexes/vector/hnsw'
import { IndexManager } from '../../../src/indexes/manager'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { IndexDefinition, HybridSearchOptions } from '../../../src/indexes/types'
import { matchesFilter } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

describe('Hybrid Search', () => {
  let storage: MemoryBackend
  let vectorIndex: VectorIndex

  // Test documents with embeddings and metadata
  const testDocs = [
    { id: 'post-1', category: 'tech', status: 'published', score: 95, embedding: [0.9, 0.1, 0.1, 0.1] },
    { id: 'post-2', category: 'tech', status: 'draft', score: 80, embedding: [0.85, 0.15, 0.1, 0.1] },
    { id: 'post-3', category: 'science', status: 'published', score: 90, embedding: [0.1, 0.9, 0.1, 0.1] },
    { id: 'post-4', category: 'science', status: 'published', score: 75, embedding: [0.1, 0.85, 0.15, 0.1] },
    { id: 'post-5', category: 'art', status: 'published', score: 60, embedding: [0.1, 0.1, 0.9, 0.1] },
    { id: 'post-6', category: 'art', status: 'draft', score: 50, embedding: [0.1, 0.1, 0.85, 0.15] },
    { id: 'post-7', category: 'tech', status: 'published', score: 85, embedding: [0.8, 0.2, 0.1, 0.1] },
    { id: 'post-8', category: 'tech', status: 'archived', score: 70, embedding: [0.75, 0.25, 0.1, 0.1] },
  ]

  beforeEach(() => {
    storage = new MemoryBackend()

    const definition: IndexDefinition = {
      name: 'idx_embedding',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 4,
        metric: 'cosine',
      },
    }

    vectorIndex = new VectorIndex(storage, 'posts', definition)

    // Insert all test documents
    for (const doc of testDocs) {
      vectorIndex.insert(doc.embedding, doc.id, 0, 0)
    }
  })

  describe('VectorIndex.hybridSearch', () => {
    it('performs standard search when no candidates provided (uses post-filter with over-fetching)', () => {
      const query = [0.9, 0.1, 0.1, 0.1] // Similar to tech posts
      // Note: Without candidates, hybrid search uses post-filter strategy
      // which over-fetches by default (3 * 3 = 9, but we only have 8 docs)
      const results = vectorIndex.hybridSearch(query, 3)

      // Post-filter returns over-fetched results for caller to filter
      expect(results.strategyUsed).toBe('post-filter')
      expect(results.scores).toBeDefined()
      expect(results.docIds[0]).toBe('post-1') // Most similar
      // Over-fetched but limited by index size
      expect(results.postFilterCount).toBe(8) // All docs returned (3 * 3 = 9 > 8)
    })

    it('performs standard search with explicit post-filter and over-fetch of 1', () => {
      const query = [0.9, 0.1, 0.1, 0.1]
      // Use overFetchMultiplier of 1 to get exact k results
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'post-filter',
        overFetchMultiplier: 1,
      })

      expect(results.docIds).toHaveLength(3)
      expect(results.strategyUsed).toBe('post-filter')
      expect(results.docIds[0]).toBe('post-1')
    })

    it('performs pre-filter search with candidate set', () => {
      // Only consider tech category posts
      const candidateIds = new Set(['post-1', 'post-2', 'post-7', 'post-8'])

      const query = [0.9, 0.1, 0.1, 0.1] // Similar to tech posts
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'pre-filter',
        candidateIds,
      })

      expect(results.strategyUsed).toBe('pre-filter')
      expect(results.preFilterCount).toBe(4)
      expect(results.docIds).toHaveLength(3)

      // All results should be from the candidate set
      for (const docId of results.docIds) {
        expect(candidateIds.has(docId)).toBe(true)
      }
    })

    it('performs post-filter search with over-fetching', () => {
      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 2, {
        strategy: 'post-filter',
        overFetchMultiplier: 3,
      })

      expect(results.strategyUsed).toBe('post-filter')
      expect(results.postFilterCount).toBe(6) // 2 * 3 = 6 over-fetched
      expect(results.docIds.length).toBeLessThanOrEqual(6)
    })

    it('auto strategy chooses pre-filter for selective filters', () => {
      // Small candidate set (25% of total)
      const candidateIds = new Set(['post-1', 'post-2'])

      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 2, {
        strategy: 'auto',
        candidateIds,
      })

      // Should choose pre-filter because candidate set is small
      expect(results.strategyUsed).toBe('pre-filter')
    })

    it('auto strategy chooses post-filter for broad filters', () => {
      // Large candidate set (75% of total)
      const candidateIds = new Set([
        'post-1', 'post-2', 'post-3', 'post-4', 'post-5', 'post-7',
      ])

      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'auto',
        candidateIds,
      })

      // Should choose post-filter because candidate set is large
      expect(results.strategyUsed).toBe('post-filter')
    })

    it('respects minScore with pre-filter', () => {
      const candidateIds = new Set(['post-1', 'post-2', 'post-3', 'post-4'])

      const query = [0.9, 0.1, 0.1, 0.1] // Very similar to tech posts
      const results = vectorIndex.hybridSearch(query, 10, {
        strategy: 'pre-filter',
        candidateIds,
        minScore: 0.9, // High threshold
      })

      // Should only include very similar results
      for (let i = 0; i < results.scores!.length; i++) {
        expect(results.scores![i]).toBeGreaterThanOrEqual(0.9)
      }
    })

    it('returns empty result for empty candidate set with pre-filter', () => {
      const candidateIds = new Set<string>()

      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'pre-filter',
        candidateIds,
      })

      expect(results.docIds).toHaveLength(0)
      expect(results.strategyUsed).toBe('pre-filter')
      expect(results.preFilterCount).toBe(0)
    })

    it('auto strategy falls back to post-filter when candidateIds is undefined', () => {
      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'auto',
        // No candidateIds provided
        overFetchMultiplier: 1,
      })

      // Should use post-filter since no candidates provided
      expect(results.strategyUsed).toBe('post-filter')
      expect(results.docIds).toHaveLength(3)
    })

    it('handles candidates not in index', () => {
      // Include some IDs that don't exist in the index
      const candidateIds = new Set(['post-1', 'post-2', 'nonexistent-1', 'nonexistent-2'])

      const query = [0.9, 0.1, 0.1, 0.1]
      const results = vectorIndex.hybridSearch(query, 3, {
        strategy: 'pre-filter',
        candidateIds,
      })

      // Should only include valid IDs
      expect(results.docIds.every(id => ['post-1', 'post-2'].includes(id))).toBe(true)
    })
  })

  describe('VectorIndex utility methods', () => {
    it('getAllDocIds returns all indexed document IDs', () => {
      const allIds = vectorIndex.getAllDocIds()

      expect(allIds.size).toBe(testDocs.length)
      for (const doc of testDocs) {
        expect(allIds.has(doc.id)).toBe(true)
      }
    })

    it('hasDocument checks document existence', () => {
      expect(vectorIndex.hasDocument('post-1')).toBe(true)
      expect(vectorIndex.hasDocument('nonexistent')).toBe(false)
    })
  })

  describe('IndexManager.hybridSearch', () => {
    let manager: IndexManager

    beforeEach(async () => {
      manager = new IndexManager(storage)

      await manager.createIndex('posts', {
        name: 'idx_embedding',
        type: 'vector',
        fields: [{ path: 'embedding' }],
        vectorOptions: {
          dimensions: 4,
          metric: 'cosine',
        },
      })

      // Get the vector index and populate it
      const idx = await (manager as any).getVectorIndex('posts', 'idx_embedding')
      for (const doc of testDocs) {
        idx.insert(doc.embedding, doc.id, 0, 0)
      }
    })

    it('performs hybrid search via IndexManager', async () => {
      const candidateIds = new Set(['post-1', 'post-2', 'post-7'])

      const results = await manager.hybridSearch(
        'posts',
        'idx_embedding',
        [0.9, 0.1, 0.1, 0.1],
        3,
        { strategy: 'pre-filter', candidateIds }
      )

      expect(results.docIds).toHaveLength(3)
      expect(results.strategyUsed).toBe('pre-filter')
    })

    it('returns empty result for nonexistent index', async () => {
      const results = await manager.hybridSearch(
        'nonexistent',
        'fake_index',
        [0.9, 0.1, 0.1, 0.1],
        3
      )

      expect(results.docIds).toHaveLength(0)
    })

    it('getVectorIndexDocIds returns all document IDs', async () => {
      const docIds = await manager.getVectorIndexDocIds('posts', 'idx_embedding')

      expect(docIds.size).toBe(testDocs.length)
    })
  })

  describe('Simulated end-to-end hybrid search', () => {
    it('simulates pre-filter workflow: category + vector search', () => {
      // Step 1: Filter documents by category (metadata)
      const metadataFilter: Filter = { category: 'tech' }
      const filteredDocs = testDocs.filter(doc =>
        matchesFilter(doc, metadataFilter)
      )
      expect(filteredDocs.length).toBe(4) // 4 tech posts

      // Step 2: Get candidate IDs from filtered docs
      const candidateIds = new Set(filteredDocs.map(d => d.id))

      // Step 3: Perform hybrid vector search on candidates
      const query = [0.9, 0.1, 0.1, 0.1] // Tech-like query
      const results = vectorIndex.hybridSearch(query, 2, {
        strategy: 'pre-filter',
        candidateIds,
      })

      // Results should be tech posts, ordered by similarity
      expect(results.docIds).toHaveLength(2)
      expect(results.docIds[0]).toBe('post-1') // Most similar tech post
      expect(candidateIds.has(results.docIds[1]!)).toBe(true)
    })

    it('simulates post-filter workflow: vector search + status filter', () => {
      // Step 1: Perform vector search with over-fetching
      const query = [0.9, 0.1, 0.1, 0.1]
      const vectorResults = vectorIndex.hybridSearch(query, 6, {
        strategy: 'post-filter',
        overFetchMultiplier: 1, // Just fetch 6
      })

      // Step 2: Filter results by metadata
      const metadataFilter: Filter = { status: 'published' }
      const vectorDocIds = vectorResults.docIds

      // Create map of docId to score
      const scoreMap = new Map<string, number>()
      vectorDocIds.forEach((id, i) => {
        scoreMap.set(id, vectorResults.scores?.[i] ?? 0)
      })

      // Get documents for vector results
      const vectorDocs = testDocs.filter(d => vectorDocIds.includes(d.id))

      // Apply metadata filter
      const filteredResults = vectorDocs.filter(doc =>
        matchesFilter(doc, metadataFilter)
      )

      // Sort by vector score (descending)
      filteredResults.sort((a, b) => {
        return (scoreMap.get(b.id) ?? 0) - (scoreMap.get(a.id) ?? 0)
      })

      // Take top 2
      const finalResults = filteredResults.slice(0, 2)

      expect(finalResults.length).toBeLessThanOrEqual(2)
      for (const doc of finalResults) {
        expect(doc.status).toBe('published')
      }
    })

    it('simulates complex filter: category + status + score + vector', () => {
      // Complex filter: tech OR science category, published status, score >= 80
      const metadataFilter: Filter = {
        $and: [
          { $or: [{ category: 'tech' }, { category: 'science' }] },
          { status: 'published' },
          { score: { $gte: 80 } },
        ],
      }

      // Step 1: Apply metadata filter
      const filteredDocs = testDocs.filter(doc =>
        matchesFilter(doc, metadataFilter)
      )

      // Should match: post-1 (tech, published, 95), post-3 (science, published, 90), post-7 (tech, published, 85)
      expect(filteredDocs.length).toBe(3)

      // Step 2: Get candidates
      const candidateIds = new Set(filteredDocs.map(d => d.id))

      // Step 3: Vector search within candidates
      const query = [0.9, 0.1, 0.1, 0.1] // Prefer tech
      const results = vectorIndex.hybridSearch(query, 2, {
        strategy: 'pre-filter',
        candidateIds,
      })

      expect(results.docIds).toHaveLength(2)
      // Top results should be tech posts (more similar to query)
      expect(['post-1', 'post-7'].includes(results.docIds[0]!)).toBe(true)
    })
  })

  describe('RRF (Reciprocal Rank Fusion) scoring simulation', () => {
    /**
     * RRF combines scores from different ranking systems.
     * Formula: RRF(d) = sum(1 / (k + rank_i(d))) for each ranking i
     * where k is typically 60.
     */
    it('demonstrates RRF score calculation for hybrid results', () => {
      // Simulate two ranking sources:
      // 1. Vector similarity ranking
      // 2. Metadata score ranking

      const k = 60 // RRF constant

      // Vector ranking (ordered by similarity)
      const vectorRanking = ['post-1', 'post-2', 'post-7', 'post-8']

      // Metadata score ranking (ordered by score field)
      const scoreRanking = ['post-1', 'post-7', 'post-2', 'post-8']

      // Calculate RRF scores
      const rrfScores = new Map<string, number>()

      // Add vector ranking scores
      vectorRanking.forEach((docId, rank) => {
        const score = 1 / (k + rank + 1)
        rrfScores.set(docId, (rrfScores.get(docId) ?? 0) + score)
      })

      // Add metadata ranking scores
      scoreRanking.forEach((docId, rank) => {
        const score = 1 / (k + rank + 1)
        rrfScores.set(docId, (rrfScores.get(docId) ?? 0) + score)
      })

      // Sort by RRF score
      const sortedResults = [...rrfScores.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([docId]) => docId)

      // post-1 should be first (rank 1 in both)
      expect(sortedResults[0]).toBe('post-1')

      // Verify RRF score for post-1
      // rank 1 in vector (score = 1/61), rank 1 in metadata (score = 1/61)
      // total = 2/61 = ~0.0328
      const post1RRF = rrfScores.get('post-1')!
      expect(post1RRF).toBeCloseTo(2 / 61, 4)
    })
  })

  describe('Strategy selection heuristics', () => {
    it('measures pre-filter efficiency for selective filters', () => {
      // 2 out of 8 documents = 25% selectivity
      const candidateIds = new Set(['post-1', 'post-2'])

      const startTime = Date.now()
      const results = vectorIndex.hybridSearch([0.9, 0.1, 0.1, 0.1], 2, {
        strategy: 'pre-filter',
        candidateIds,
      })
      const preFilterTime = Date.now() - startTime

      // Pre-filter should scan only 2 entries
      expect(results.entriesScanned).toBe(2)
      expect(preFilterTime).toBeLessThan(50) // Should be very fast
    })

    it('measures post-filter with over-fetching', () => {
      const startTime = Date.now()
      const results = vectorIndex.hybridSearch([0.9, 0.1, 0.1, 0.1], 2, {
        strategy: 'post-filter',
        overFetchMultiplier: 3,
      })
      const postFilterTime = Date.now() - startTime

      // Post-filter fetches 2 * 3 = 6 results
      expect(results.postFilterCount).toBe(6)
      expect(postFilterTime).toBeLessThan(50)
    })
  })
})

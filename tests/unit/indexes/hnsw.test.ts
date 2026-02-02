/**
 * Tests for HNSW Vector Index
 *
 * Comprehensive tests for the Hierarchical Navigable Small World graph
 * used for approximate nearest neighbor search.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex } from '@/indexes/vector/hnsw'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createDefinition(
  overrides: Partial<IndexDefinition & { vectorOptions: Record<string, unknown> }> = {}
): IndexDefinition {
  return {
    name: 'idx_embedding',
    type: 'vector',
    fields: [{ path: 'embedding' }],
    vectorOptions: {
      dimensions: 3,
      metric: 'cosine',
      m: 16,
      efConstruction: 200,
    },
    ...overrides,
  }
}

function createIndex(
  storage: MemoryBackend,
  definition?: IndexDefinition
): VectorIndex {
  return new VectorIndex(storage, 'documents', definition ?? createDefinition())
}

// =============================================================================
// Building Index with Vectors
// =============================================================================

describe('HNSW VectorIndex', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('building index', () => {
    it('builds from individual inserts', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      expect(index.size).toBe(3)
    })

    it('builds from an array using buildFromArray', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: { embedding: [1, 0, 0] }, docId: 'a', rowGroup: 0, rowOffset: 0 },
        { doc: { embedding: [0, 1, 0] }, docId: 'b', rowGroup: 0, rowOffset: 1 },
        { doc: { embedding: [0, 0, 1] }, docId: 'c', rowGroup: 0, rowOffset: 2 },
      ])

      expect(index.size).toBe(3)
    })

    it('builds from async iterable using build()', async () => {
      const index = createIndex(storage)

      async function* dataIterator() {
        yield { doc: { embedding: [1, 0, 0] }, docId: 'a', rowGroup: 0, rowOffset: 0 }
        yield { doc: { embedding: [0, 1, 0] }, docId: 'b', rowGroup: 0, rowOffset: 1 }
        yield { doc: { embedding: [0, 0, 1] }, docId: 'c', rowGroup: 0, rowOffset: 2 }
      }

      await index.build(dataIterator())

      expect(index.size).toBe(3)
    })

    it('reports progress during build', async () => {
      const definition = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'cosine' },
      })
      const index = createIndex(storage, definition)

      // Create 2500 docs so we get at least 2 progress callbacks (every 1000)
      async function* dataIterator() {
        for (let i = 0; i < 2500; i++) {
          const angle = (i / 2500) * Math.PI * 2
          yield {
            doc: { embedding: [Math.cos(angle), Math.sin(angle), 0] },
            docId: `doc${i}`,
            rowGroup: 0,
            rowOffset: i,
          }
        }
      }

      const progressCalls: number[] = []
      await index.build(dataIterator(), {
        onProgress: (processed) => progressCalls.push(processed),
      })

      expect(progressCalls.length).toBeGreaterThanOrEqual(2)
      expect(progressCalls[0]).toBe(1000)
      expect(progressCalls[1]).toBe(2000)
    })

    it('skips vectors with wrong dimensions', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0) // Correct: 3 dims
      index.insert([1, 0], 'doc2', 0, 1) // Wrong: 2 dims
      index.insert([1, 0, 0, 0], 'doc3', 0, 2) // Wrong: 4 dims

      expect(index.size).toBe(1)
    })

    it('skips documents without vector fields in buildFromArray', () => {
      const index = createIndex(storage)

      index.buildFromArray([
        { doc: { embedding: [1, 0, 0] }, docId: 'a', rowGroup: 0, rowOffset: 0 },
        { doc: { name: 'no embedding' }, docId: 'b', rowGroup: 0, rowOffset: 1 },
        { doc: { embedding: 'not an array' }, docId: 'c', rowGroup: 0, rowOffset: 2 },
        { doc: { embedding: [1, 'not a number', 0] }, docId: 'd', rowGroup: 0, rowOffset: 3 },
      ])

      expect(index.size).toBe(1)
    })

    it('handles nested field paths', () => {
      const def = createDefinition()
      def.fields = [{ path: 'metadata.vector' }]
      const index = createIndex(storage, def)

      index.buildFromArray([
        { doc: { metadata: { vector: [1, 0, 0] } }, docId: 'a', rowGroup: 0, rowOffset: 0 },
        { doc: { metadata: { vector: [0, 1, 0] } }, docId: 'b', rowGroup: 0, rowOffset: 1 },
      ])

      expect(index.size).toBe(2)
    })

    it('handles deeply nested field paths', () => {
      const def = createDefinition()
      def.fields = [{ path: 'a.b.c' }]
      const index = createIndex(storage, def)

      index.buildFromArray([
        { doc: { a: { b: { c: [1, 0, 0] } } }, docId: 'x', rowGroup: 0, rowOffset: 0 },
      ])

      expect(index.size).toBe(1)
    })

    it('clears previous data on buildFromArray', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'old1', 0, 0)
      index.insert([0, 1, 0], 'old2', 0, 1)
      expect(index.size).toBe(2)

      index.buildFromArray([
        { doc: { embedding: [0, 0, 1] }, docId: 'new1', rowGroup: 0, rowOffset: 0 },
      ])

      expect(index.size).toBe(1)
      const result = index.search([0, 0, 1], 5)
      expect(result.docIds).toContain('new1')
      expect(result.docIds).not.toContain('old1')
    })

    it('handles duplicate docIds by updating', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc1', 0, 1) // Same docId, different vector

      expect(index.size).toBe(1)

      // Should find the updated vector
      const result = index.search([0, 1, 0], 1)
      expect(result.docIds[0]).toBe('doc1')
    })

    it('builds a large index efficiently', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 16, metric: 'cosine' },
      })
      const index = createIndex(storage, def)

      const count = 500
      for (let i = 0; i < count; i++) {
        const vec = Array.from({ length: 16 }, (_, j) =>
          Math.sin(i * 0.1 + j * 0.3)
        )
        index.insert(vec, `doc${i}`, Math.floor(i / 100), i % 100)
      }

      expect(index.size).toBe(count)
    })
  })

  // ===========================================================================
  // Nearest Neighbor Search (k-NN)
  // ===========================================================================

  describe('nearest neighbor search (k-NN)', () => {
    it('finds the exact match as the closest neighbor', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      const result = index.search([1, 0, 0], 1)
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds[0]).toBe('doc1')
    })

    it('returns results ordered by similarity (cosine)', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'exact', 0, 0)
      index.insert([0.9, 0.1, 0], 'close', 0, 1)
      index.insert([0.5, 0.5, 0], 'medium', 0, 2)
      index.insert([0, 1, 0], 'far', 0, 3)

      const result = index.search([1, 0, 0], 4)
      expect(result.docIds[0]).toBe('exact')
      expect(result.docIds[1]).toBe('close')
    })

    it('returns results ordered by distance (euclidean)', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'euclidean' },
      })
      const index = createIndex(storage, def)

      index.insert([0, 0, 0], 'origin', 0, 0)
      index.insert([1, 0, 0], 'near', 0, 1)
      index.insert([10, 0, 0], 'far', 0, 2)

      const result = index.search([0, 0, 0], 3)
      expect(result.docIds[0]).toBe('origin')
      expect(result.docIds[1]).toBe('near')
      expect(result.docIds[2]).toBe('far')
    })

    it('returns results ordered by dot product', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'dot' },
      })
      const index = createIndex(storage, def)

      index.insert([1, 1, 1], 'high', 0, 0)
      index.insert([0.5, 0.5, 0.5], 'medium', 0, 1)
      index.insert([-1, -1, -1], 'low', 0, 2)

      const result = index.search([1, 1, 1], 3)
      expect(result.docIds[0]).toBe('high')
    })

    it('returns k results when more vectors exist', () => {
      const index = createIndex(storage)

      for (let i = 0; i < 20; i++) {
        const angle = (i / 20) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      const result = index.search([1, 0, 0], 5)
      expect(result.docIds).toHaveLength(5)
    })

    it('returns all vectors when k > total vectors', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      const result = index.search([1, 0, 0], 100)
      expect(result.docIds).toHaveLength(2)
    })

    it('returns scores corresponding to each result', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      const result = index.search([1, 0, 0], 2)
      expect(result.scores).toBeDefined()
      expect(result.scores!.length).toBe(result.docIds.length)

      // First result should have higher score
      expect(result.scores![0]).toBeGreaterThan(result.scores![1]!)
    })

    it('returns row group hints', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0.9, 0.1, 0], 'doc2', 1, 0)
      index.insert([0.8, 0.2, 0], 'doc3', 2, 0)

      const result = index.search([1, 0, 0], 3)
      expect(result.rowGroups).toContain(0)
      expect(result.rowGroups).toContain(1)
      expect(result.rowGroups).toContain(2)
    })

    it('marks results as approximate (not exact)', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const result = index.search([1, 0, 0], 1)
      expect(result.exact).toBe(false)
    })

    it('reports entriesScanned', () => {
      const index = createIndex(storage)

      for (let i = 0; i < 10; i++) {
        index.insert([Math.random(), Math.random(), Math.random()], `doc${i}`, 0, i)
      }

      const result = index.search([1, 0, 0], 5)
      expect(result.entriesScanned).toBeGreaterThan(0)
    })

    it('respects minScore filter', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'exact', 0, 0) // Score ~1.0
      index.insert([0.9, 0.1, 0], 'close', 0, 1) // Score ~0.99
      index.insert([0, 1, 0], 'orthogonal', 0, 2) // Score ~0.0

      const result = index.search([1, 0, 0], 10, { minScore: 0.9 })

      // The orthogonal vector should be filtered out
      expect(result.docIds).toContain('exact')
      expect(result.docIds).not.toContain('orthogonal')
    })

    it('respects efSearch parameter', () => {
      const index = createIndex(storage)

      for (let i = 0; i < 50; i++) {
        const angle = (i / 50) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      // Both should return valid results regardless of efSearch
      const lowEf = index.search([1, 0, 0], 3, { efSearch: 5 })
      const highEf = index.search([1, 0, 0], 3, { efSearch: 100 })

      expect(lowEf.docIds.length).toBeGreaterThan(0)
      expect(highEf.docIds.length).toBeGreaterThan(0)
    })

    it('handles search with a single vector in the index', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'only', 0, 0)

      const result = index.search([0, 1, 0], 5)
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds[0]).toBe('only')
    })
  })

  // ===========================================================================
  // Insert and Delete Operations
  // ===========================================================================

  describe('insert operations', () => {
    it('inserts first vector as entry point', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'first', 0, 0)

      expect(index.size).toBe(1)
      const result = index.search([1, 0, 0], 1)
      expect(result.docIds[0]).toBe('first')
    })

    it('inserts multiple vectors with correct graph connectivity', () => {
      const index = createIndex(storage)

      // Insert enough vectors that the graph builds connections
      for (let i = 0; i < 20; i++) {
        const angle = (i / 20) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      expect(index.size).toBe(20)

      // Verify the graph is navigable: search should find nearby vectors
      const result = index.search([1, 0, 0], 3)
      expect(result.docIds.length).toBe(3)
    })

    it('rejects vectors with wrong dimensions silently', () => {
      const index = createIndex(storage)

      index.insert([1, 0], 'short', 0, 0) // 2 dims, need 3
      index.insert([1, 0, 0, 0], 'long', 0, 1) // 4 dims, need 3

      expect(index.size).toBe(0)
    })

    it('updates existing document when inserting same docId', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      expect(index.size).toBe(1)

      index.insert([0, 1, 0], 'doc1', 0, 1) // Update
      expect(index.size).toBe(1) // Still just one entry

      const result = index.search([0, 1, 0], 1)
      expect(result.docIds[0]).toBe('doc1')
    })
  })

  describe('delete operations', () => {
    it('removes a vector by docId', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      expect(index.size).toBe(3)

      const removed = index.remove('doc2')
      expect(removed).toBe(true)
      expect(index.size).toBe(2)
    })

    it('removed vector does not appear in search results', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      index.remove('doc2')

      const result = index.search([0, 1, 0], 10)
      expect(result.docIds).not.toContain('doc2')
    })

    it('returns false when removing non-existent docId', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      expect(index.remove('nonexistent')).toBe(false)
      expect(index.size).toBe(1)
    })

    it('handles removing the entry point node', () => {
      const index = createIndex(storage)

      // Insert several vectors; the first one is the entry point
      index.insert([1, 0, 0], 'entry', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      // Remove the first inserted (likely entry point)
      index.remove('entry')

      expect(index.size).toBe(2)

      // Search should still work after entry point removal
      const result = index.search([1, 0, 0], 5)
      expect(result.docIds.length).toBeGreaterThan(0)
      expect(result.docIds).not.toContain('entry')
    })

    it('handles removing all vectors', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      index.remove('doc1')
      index.remove('doc2')

      expect(index.size).toBe(0)

      // Search on empty index should return empty results
      const result = index.search([1, 0, 0], 5)
      expect(result.docIds).toHaveLength(0)
    })

    it('maintains graph integrity after multiple deletions', () => {
      const index = createIndex(storage)

      // Insert 10 vectors
      for (let i = 0; i < 10; i++) {
        const angle = (i / 10) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      // Remove half of them
      for (let i = 0; i < 10; i += 2) {
        index.remove(`doc${i}`)
      }

      expect(index.size).toBe(5)

      // Search should still return valid results
      const result = index.search([1, 0, 0], 3)
      expect(result.docIds.length).toBeGreaterThan(0)
      expect(result.docIds.length).toBeLessThanOrEqual(3)

      // None of the removed docs should appear
      for (let i = 0; i < 10; i += 2) {
        expect(result.docIds).not.toContain(`doc${i}`)
      }
    })
  })

  describe('update operations', () => {
    it('updates an existing vector', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      const updated = index.update([0, 0, 1], 'doc1', 0, 0)
      expect(updated).toBe(true)
      expect(index.size).toBe(2)

      const result = index.search([0, 0, 1], 1)
      expect(result.docIds[0]).toBe('doc1')
    })

    it('returns false for non-existent docId (but still inserts)', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const updated = index.update([0, 1, 0], 'new_doc', 0, 1)
      expect(updated).toBe(false)
      // The vector is still inserted despite not existing before
      expect(index.size).toBe(2)
    })

    it('rejects update with wrong dimensions', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const updated = index.update([1, 0], 'doc1', 0, 0)
      expect(updated).toBe(false)
    })
  })

  describe('clear', () => {
    it('removes all entries', () => {
      const index = createIndex(storage)

      for (let i = 0; i < 10; i++) {
        index.insert([Math.random(), Math.random(), Math.random()], `doc${i}`, 0, i)
      }

      expect(index.size).toBe(10)

      index.clear()

      expect(index.size).toBe(0)
    })

    it('allows reinsertion after clear', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.clear()

      index.insert([0, 1, 0], 'doc2', 0, 0)
      expect(index.size).toBe(1)

      const result = index.search([0, 1, 0], 1)
      expect(result.docIds[0]).toBe('doc2')
    })

    it('search returns empty after clear', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.clear()

      const result = index.search([1, 0, 0], 5)
      expect(result.docIds).toHaveLength(0)
      expect(result.scores).toHaveLength(0)
      expect(result.rowGroups).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles empty index search', () => {
      const index = createIndex(storage)

      const result = index.search([1, 0, 0], 5)
      expect(result.docIds).toHaveLength(0)
      expect(result.scores).toHaveLength(0)
      expect(result.rowGroups).toHaveLength(0)
      expect(result.exact).toBe(false)
      expect(result.entriesScanned).toBe(0)
    })

    it('handles k=0 search', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const result = index.search([1, 0, 0], 0)
      expect(result.docIds).toHaveLength(0)
    })

    it('handles k > total vectors in index', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      const result = index.search([1, 0, 0], 1000)
      expect(result.docIds).toHaveLength(2)
    })

    it('handles a single vector in the index', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'only', 0, 0)

      const result = index.search([0, 1, 0], 5)
      expect(result.docIds).toEqual(['only'])
    })

    it('handles many identical vectors', () => {
      const index = createIndex(storage)

      for (let i = 0; i < 10; i++) {
        index.insert([1, 0, 0], `doc${i}`, 0, i)
      }

      expect(index.size).toBe(10)

      const result = index.search([1, 0, 0], 5)
      expect(result.docIds).toHaveLength(5)
    })

    it('handles vectors with very small values', () => {
      const index = createIndex(storage)

      index.insert([1e-10, 1e-10, 1e-10], 'tiny', 0, 0)
      index.insert([1, 0, 0], 'normal', 0, 1)

      expect(index.size).toBe(2)

      const result = index.search([1, 0, 0], 2)
      expect(result.docIds).toHaveLength(2)
    })

    it('handles vectors with very large values', () => {
      const index = createIndex(storage)

      index.insert([1e10, 1e10, 1e10], 'large', 0, 0)
      index.insert([1, 0, 0], 'normal', 0, 1)

      expect(index.size).toBe(2)

      const result = index.search([1e10, 1e10, 1e10], 1)
      expect(result.docIds[0]).toBe('large')
    })

    it('handles negative vector components', () => {
      const index = createIndex(storage)

      index.insert([-1, -1, -1], 'neg', 0, 0)
      index.insert([1, 1, 1], 'pos', 0, 1)

      const result = index.search([-1, -1, -1], 1)
      expect(result.docIds[0]).toBe('neg')
    })
  })

  // ===========================================================================
  // Statistics
  // ===========================================================================

  describe('statistics', () => {
    it('reports correct entry count', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(3)
    })

    it('reports non-zero sizeBytes', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)

      const stats = index.getStats()
      expect(stats.sizeBytes).toBeGreaterThan(0)
    })

    it('reports correct dimensions', () => {
      const index = createIndex(storage)

      const stats = index.getStats()
      expect(stats.dimensions).toBe(3)
    })

    it('reports maxLayer', () => {
      const index = createIndex(storage)

      // With enough inserts, at least some nodes should be on layer > 0
      for (let i = 0; i < 100; i++) {
        const angle = (i / 100) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      const stats = index.getStats()
      expect(stats.maxLayer).toBeGreaterThanOrEqual(0)
    })

    it('reports 0 entries for empty index', () => {
      const index = createIndex(storage)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(0)
      expect(stats.sizeBytes).toBe(0)
    })

    it('size property matches stats.entryCount', () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)

      expect(index.size).toBe(index.getStats().entryCount)
    })
  })

  // ===========================================================================
  // Persistence (Serialization/Deserialization)
  // ===========================================================================

  describe('persistence', () => {
    it('saves and loads an index', async () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      index.insert([0, 1, 0], 'doc2', 0, 1)
      index.insert([0, 0, 1], 'doc3', 0, 2)

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      expect(loaded.size).toBe(3)
      expect(loaded.ready).toBe(true)

      // Search should work on the loaded index
      const result = loaded.search([1, 0, 0], 2)
      expect(result.docIds).toContain('doc1')
    })

    it('loads empty index when file does not exist', async () => {
      const index = createIndex(storage)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })

    it('preserves search quality after save/load', async () => {
      const index = createIndex(storage)

      // Insert a ring of vectors
      for (let i = 0; i < 50; i++) {
        const angle = (i / 50) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0],
          `doc${i}`,
          0,
          i
        )
      }

      // Search before save
      const beforeSave = index.search([1, 0, 0], 3)

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      // Search after load
      const afterLoad = loaded.search([1, 0, 0], 3)

      // Should return the same top results
      expect(afterLoad.docIds).toEqual(beforeSave.docIds)
    })

    it('handles save and load of empty index', async () => {
      const index = createIndex(storage)

      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()

      expect(loaded.size).toBe(0)
      expect(loaded.ready).toBe(true)
    })

    it('does not re-load if already loaded', async () => {
      const index = createIndex(storage)

      index.insert([1, 0, 0], 'doc1', 0, 0)
      await index.save()

      const loaded = createIndex(storage)
      await loaded.load()
      expect(loaded.size).toBe(1)

      // Manually insert another vector
      loaded.insert([0, 1, 0], 'doc2', 0, 1)
      expect(loaded.size).toBe(2)

      // Loading again should be a no-op since already loaded
      await loaded.load()
      expect(loaded.size).toBe(2)
    })
  })

  // ===========================================================================
  // Distance Metric Integration
  // ===========================================================================

  describe('metric integration', () => {
    it('cosine: similar directions rank higher regardless of magnitude', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'cosine' },
      })
      const index = createIndex(storage, def)

      index.insert([1, 0, 0], 'unit_x', 0, 0)
      index.insert([100, 0, 0], 'big_x', 0, 1) // Same direction, larger magnitude
      index.insert([0, 1, 0], 'unit_y', 0, 2) // Orthogonal

      const result = index.search([2, 0, 0], 2)
      // Both x-direction vectors should be in top 2
      expect(result.docIds).toContain('unit_x')
      expect(result.docIds).toContain('big_x')
    })

    it('euclidean: closer vectors rank higher', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'euclidean' },
      })
      const index = createIndex(storage, def)

      index.insert([0, 0, 0], 'origin', 0, 0)
      index.insert([1, 0, 0], 'near', 0, 1)
      index.insert([100, 0, 0], 'far', 0, 2)

      const result = index.search([0.5, 0, 0], 2)
      expect(result.docIds).toContain('origin')
      expect(result.docIds).toContain('near')
    })

    it('dot: higher dot product ranks higher', () => {
      const def = createDefinition({
        vectorOptions: { dimensions: 3, metric: 'dot' },
      })
      const index = createIndex(storage, def)

      index.insert([10, 10, 10], 'high', 0, 0) // dot with query = 30
      index.insert([1, 1, 1], 'medium', 0, 1) // dot with query = 3
      index.insert([-1, -1, -1], 'negative', 0, 2) // dot with query = -3

      const result = index.search([1, 1, 1], 3)
      expect(result.docIds[0]).toBe('high')
    })
  })
})

/**
 * Tests for VectorIndex configurable precision (Float32/Float64)
 *
 * Verifies that VectorIndex serialization supports configurable precision
 * to reduce index size for lower-precision use cases.
 *
 * Issue: parquedb-p57w
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { VectorIndex, VectorIndexConfigError } from '@/indexes/vector/hnsw'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('VectorIndex precision', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  function createDefinition(
    precision?: ('float32' | 'float64') | undefined
  ): IndexDefinition {
    return {
      name: 'idx_embedding',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 4,
        metric: 'cosine',
        m: 4,
        efConstruction: 20,
        precision,
      },
    }
  }

  function insertTestVectors(index: VectorIndex): void {
    index.insert([1.0, 0.0, 0.0, 0.0], 'doc1', 0, 0)
    index.insert([0.0, 1.0, 0.0, 0.0], 'doc2', 0, 1)
    index.insert([0.0, 0.0, 1.0, 0.0], 'doc3', 0, 2)
    index.insert([0.9, 0.1, 0.0, 0.0], 'doc4', 0, 3)
  }

  describe('default precision', () => {
    it('defaults to float32 when precision is not specified', async () => {
      const definition = createDefinition()
      const index = new VectorIndex(storage, 'documents', definition)
      insertTestVectors(index)
      await index.save()

      // Reload with explicit float32 - should work without errors
      const float32Def = createDefinition('float32')
      const loaded = new VectorIndex(storage, 'documents', float32Def)
      await loaded.load()
      expect(loaded.size).toBe(4)
    })
  })

  describe('float32 precision', () => {
    it('serializes and deserializes correctly with float32', async () => {
      const definition = createDefinition('float32')
      const index = new VectorIndex(storage, 'documents', definition)
      insertTestVectors(index)
      await index.save()

      const loaded = new VectorIndex(storage, 'documents', definition)
      await loaded.load()
      expect(loaded.size).toBe(4)

      // Search should still return correct results
      const result = loaded.search([1.0, 0.0, 0.0, 0.0], 2)
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds[0]).toBe('doc1')
      expect(result.docIds[1]).toBe('doc4')
    })

    it('produces smaller serialized size than float64', async () => {
      const float32Def = createDefinition('float32')
      const float64Def = createDefinition('float64')

      const float32Index = new VectorIndex(storage, 'documents', float32Def)
      const float64Index = new VectorIndex(
        storage,
        'documents_f64',
        float64Def
      )

      // Insert many vectors to make the size difference meaningful
      for (let i = 0; i < 50; i++) {
        const angle = (i / 50) * Math.PI * 2
        const vec = [
          Math.cos(angle),
          Math.sin(angle),
          Math.cos(angle * 2),
          Math.sin(angle * 2),
        ]
        float32Index.insert(vec, `doc${i}`, 0, i)
        float64Index.insert(vec, `doc${i}`, 0, i)
      }

      await float32Index.save()
      await float64Index.save()

      const float32Path = 'indexes/vector/documents.idx_embedding.hnsw'
      const float64Path = 'indexes/vector/documents_f64.idx_embedding.hnsw'

      const float32Data = await storage.read(float32Path)
      const float64Data = await storage.read(float64Path)

      // Float32 should be smaller than Float64
      // The difference should be approximately (nodeCount * dimensions * 4) bytes
      // 50 nodes * 4 dimensions * 4 bytes difference = 800 bytes
      // However HNSW randomness causes graph structure to vary, so we use a
      // more relaxed threshold. The key assertion is that float32 < float64.
      expect(float32Data.length).toBeLessThan(float64Data.length)

      // The size difference should be at least 50% of the theoretical difference
      // to account for graph structure variations from random level generation
      const sizeDiff = float64Data.length - float32Data.length
      const expectedDiff = 50 * 4 * 4 // nodeCount * dimensions * (8-4) bytes
      expect(sizeDiff).toBeGreaterThanOrEqual(expectedDiff * 0.5)
    })
  })

  describe('float64 precision', () => {
    it('serializes and deserializes correctly with float64', async () => {
      const definition = createDefinition('float64')
      const index = new VectorIndex(storage, 'documents', definition)
      insertTestVectors(index)
      await index.save()

      const loaded = new VectorIndex(storage, 'documents', definition)
      await loaded.load()
      expect(loaded.size).toBe(4)

      // Search should still return correct results
      const result = loaded.search([1.0, 0.0, 0.0, 0.0], 2)
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds[0]).toBe('doc1')
      expect(result.docIds[1]).toBe('doc4')
    })

    it('preserves full float64 precision on round-trip', async () => {
      const definition = createDefinition('float64')
      const index = new VectorIndex(storage, 'documents', definition)

      // Use a vector with high precision values
      const preciseVector = [
        0.123456789012345, 0.987654321098765, 0.111111111111111,
        0.999999999999999,
      ]
      index.insert(preciseVector, 'precise_doc', 0, 0)
      await index.save()

      const loaded = new VectorIndex(storage, 'documents', definition)
      await loaded.load()

      // Search for exact vector - should be perfect match
      const result = loaded.search(preciseVector, 1)
      expect(result.docIds[0]).toBe('precise_doc')
      expect(result.scores![0]).toBeCloseTo(1.0, 10)
    })
  })

  describe('precision mismatch', () => {
    it('throws VectorIndexConfigError when loading float32 index with float64 config', async () => {
      const float32Def = createDefinition('float32')
      const index = new VectorIndex(storage, 'documents', float32Def)
      insertTestVectors(index)
      await index.save()

      const float64Def = createDefinition('float64')
      const mismatchedIndex = new VectorIndex(
        storage,
        'documents',
        float64Def
      )
      await expect(mismatchedIndex.load()).rejects.toThrow(
        /precision mismatch/i
      )
    })

    it('throws VectorIndexConfigError when loading float64 index with float32 config', async () => {
      const float64Def = createDefinition('float64')
      const index = new VectorIndex(storage, 'documents', float64Def)
      insertTestVectors(index)
      await index.save()

      const float32Def = createDefinition('float32')
      const mismatchedIndex = new VectorIndex(
        storage,
        'documents',
        float32Def
      )
      await expect(mismatchedIndex.load()).rejects.toThrow(
        /precision mismatch/i
      )
    })
  })

  describe('backward compatibility', () => {
    it('loads index saved without explicit precision using float32 default', async () => {
      // Create and save an index without specifying precision (defaults to float32)
      const oldDef = createDefinition()
      const oldIndex = new VectorIndex(storage, 'documents', oldDef)
      insertTestVectors(oldIndex)
      await oldIndex.save()

      // Loading with explicit float32 should work since that is the default
      const newDef = createDefinition('float32')
      const newIndex = new VectorIndex(storage, 'documents', newDef)
      await newIndex.load()
      expect(newIndex.size).toBe(4)

      // Search should still return correct results
      const result = newIndex.search([1.0, 0.0, 0.0, 0.0], 2)
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds[0]).toBe('doc1')
    })
  })

  describe('search accuracy with float32', () => {
    it('returns correct nearest neighbors with float32 precision', async () => {
      const definition = createDefinition('float32')
      const index = new VectorIndex(storage, 'documents', definition)

      // Insert vectors in a circle
      for (let i = 0; i < 20; i++) {
        const angle = (i / 20) * Math.PI * 2
        index.insert(
          [Math.cos(angle), Math.sin(angle), 0, 0],
          `doc${i}`,
          0,
          i
        )
      }

      await index.save()

      const loaded = new VectorIndex(storage, 'documents', definition)
      await loaded.load()

      // Search for [1, 0, 0, 0] - should find doc0 first
      const result = loaded.search([1.0, 0.0, 0.0, 0.0], 3)
      expect(result.docIds).toContain('doc0')
      expect(result.scores![0]).toBeGreaterThan(0.9)
    })

    it('maintains reasonable accuracy despite float32 quantization', () => {
      const float32Def = createDefinition('float32')
      const float64Def = createDefinition('float64')

      const float32Index = new VectorIndex(
        storage,
        'documents_f32',
        float32Def
      )
      const float64Index = new VectorIndex(
        storage,
        'documents_f64',
        float64Def
      )

      // Insert same vectors into both indexes
      for (let i = 0; i < 30; i++) {
        const angle = (i / 30) * Math.PI * 2
        const vec = [
          Math.cos(angle),
          Math.sin(angle),
          Math.cos(angle * 0.5),
          Math.sin(angle * 0.5),
        ]
        float32Index.insert(vec, `doc${i}`, 0, i)
        float64Index.insert(vec, `doc${i}`, 0, i)
      }

      // Search results should be the same (or very close)
      const query = [0.7, 0.3, 0.5, 0.2]
      const f32Result = float32Index.search(query, 5)
      const f64Result = float64Index.search(query, 5)

      // Top result should be the same
      expect(f32Result.docIds[0]).toBe(f64Result.docIds[0])

      // Scores should be very close
      for (let i = 0; i < Math.min(f32Result.scores!.length, f64Result.scores!.length); i++) {
        expect(f32Result.scores![i]).toBeCloseTo(f64Result.scores![i]!, 4)
      }
    })
  })
})

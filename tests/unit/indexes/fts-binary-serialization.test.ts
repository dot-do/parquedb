/**
 * Tests for FTS Inverted Index Binary Serialization
 *
 * Issue: parquedb-pmm8.8 - P2: FTS Index storage format uses JSON - consider binary format
 *
 * Binary format provides:
 * - Smaller file sizes (typically 2-4x smaller than JSON)
 * - Faster serialization/deserialization (no JSON parsing overhead)
 * - Better performance for large indexes
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { InvertedIndex } from '@/indexes/fts/inverted-index'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('FTS Inverted Index Binary Serialization', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_fts',
      type: 'fts',
      fields: [{ path: 'title' }, { path: 'content' }],
      ftsOptions: {
        language: 'en',
        minWordLength: 2,
        indexPositions: true,
      },
    }
  })

  describe('basic serialization', () => {
    it('saves and loads index with binary format', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database Systems', content: 'SQL and NoSQL' })
      index.addDocument('doc2', { title: 'Web Development', content: 'Building modern apps' })
      index.addDocument('doc3', { title: 'Database Performance', content: 'Optimizing queries' })

      await index.save()

      // Verify it was saved
      const path = 'indexes/fts/articles/inverted.idx'
      const exists = await storage.exists(path)
      expect(exists).toBe(true)

      // Load into new index
      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      // Verify data integrity
      expect(loaded.documentCount).toBe(3)
      expect(loaded.vocabularySize).toBe(index.vocabularySize)
      expect(loaded.avgDocLength).toBeCloseTo(index.avgDocLength, 5)

      // Verify postings are preserved
      const postings = loaded.getPostings('databas') // stemmed
      expect(postings).toHaveLength(2)
      expect(postings.map(p => p.docId).sort()).toEqual(['doc1', 'doc3'])
    })

    it('preserves document stats', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Hello World', content: 'Some content here' })

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      const stats = loaded.getDocumentStats('doc1')
      expect(stats).not.toBeNull()
      expect(stats!.docId).toBe('doc1')
      expect(stats!.totalLength).toBeGreaterThan(0)
      expect(stats!.fieldLengths.get('title')).toBeGreaterThan(0)
    })

    it('preserves corpus statistics', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database' })
      index.addDocument('doc2', { title: 'Database Systems' })
      index.addDocument('doc3', { title: 'Web Apps' })

      const originalCorpusStats = index.getCorpusStats()

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      const loadedCorpusStats = loaded.getCorpusStats()
      expect(loadedCorpusStats.documentCount).toBe(originalCorpusStats.documentCount)
      expect(loadedCorpusStats.avgDocLength).toBeCloseTo(originalCorpusStats.avgDocLength, 5)
      expect(loadedCorpusStats.documentFrequency.get('databas')).toBe(
        originalCorpusStats.documentFrequency.get('databas')
      )
    })

    it('preserves term positions when indexPositions is enabled', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database database database' }) // term appears 3 times

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      const postings = loaded.getPostings('databas')
      expect(postings).toHaveLength(1)
      expect(postings[0].positions).toHaveLength(3)
      expect(postings[0].positions).toEqual([0, 1, 2])
    })
  })

  describe('binary format characteristics', () => {
    it('uses binary format (not JSON)', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Test Document' })

      await index.save()

      const path = 'indexes/fts/articles/inverted.idx'
      const data = await storage.read(path)

      // Binary format starts with magic bytes 'FTSI', not '{' (JSON)
      expect(data[0]).toBe(0x46) // 'F'
      expect(data[1]).toBe(0x54) // 'T'
      expect(data[2]).toBe(0x53) // 'S'
      expect(data[3]).toBe(0x49) // 'I'
    })

    it('produces smaller output than JSON for large indexes', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      // Add many documents to create a substantial index
      for (let i = 0; i < 100; i++) {
        index.addDocument(`doc${i}`, {
          title: `Document ${i} about databases and systems`,
          content: `Content ${i} with various words like performance optimization query execution`,
        })
      }

      await index.save()

      const path = 'indexes/fts/articles/inverted.idx'
      const binaryData = await storage.read(path)

      // Estimate JSON size (serialize to JSON for comparison)
      const jsonData = JSON.stringify({
        version: 2,
        index: Array.from(index['index'].entries()),
        docStats: Array.from(index['docStats'].entries()).map(([id, stats]) => ({
          docId: id,
          fieldLengths: Array.from(stats.fieldLengths.entries()),
          totalLength: stats.totalLength,
        })),
        corpusStats: {
          documentCount: index['corpusStats'].documentCount,
          avgDocLength: index['corpusStats'].avgDocLength,
          documentFrequency: Array.from(index['corpusStats'].documentFrequency.entries()),
        },
      })
      const jsonSize = new TextEncoder().encode(jsonData).length

      // Binary format should be significantly smaller
      // Typical compression ratio is 2-4x
      expect(binaryData.length).toBeLessThan(jsonSize * 0.75)
    })
  })

  describe('backward compatibility', () => {
    it('loads legacy JSON format (version 1)', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      // Manually write JSON format (legacy)
      const legacyData = {
        version: 1,
        index: [['test', [{ docId: 'doc1', field: 'title', frequency: 1, positions: [0] }]]],
        docStats: [
          {
            docId: 'doc1',
            fieldLengths: [['title', 1]],
            totalLength: 1,
          },
        ],
        corpusStats: {
          documentCount: 1,
          avgDocLength: 1,
          documentFrequency: [['test', 1]],
        },
      }

      const path = 'indexes/fts/articles/inverted.idx'
      await storage.write(path, new TextEncoder().encode(JSON.stringify(legacyData)))

      // Load and verify it works
      await index.load()

      expect(index.documentCount).toBe(1)
      expect(index.getPostings('test')).toHaveLength(1)
    })
  })

  describe('error handling', () => {
    it('handles corrupted binary data gracefully', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      // Write corrupted data
      const path = 'indexes/fts/articles/inverted.idx'
      await storage.write(path, new Uint8Array([0x46, 0x54, 0x53, 0x49, 0xff, 0xff])) // Valid magic, invalid data

      // Should not throw, but start fresh
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.documentCount).toBe(0)
    })

    it('handles invalid magic bytes', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      // Write data with wrong magic
      const path = 'indexes/fts/articles/inverted.idx'
      await storage.write(path, new Uint8Array([0x00, 0x00, 0x00, 0x00]))

      // Should treat as corrupted and start fresh
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.documentCount).toBe(0)
    })
  })

  describe('empty and edge cases', () => {
    it('handles empty index', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      expect(loaded.documentCount).toBe(0)
      expect(loaded.vocabularySize).toBe(0)
    })

    it('handles documents with empty fields', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: '', content: '' })
      index.addDocument('doc2', { title: 'Valid Title', content: '' })

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      expect(loaded.documentCount).toBe(2)
    })

    it('handles unicode content', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: '数据库系统', content: 'Chinese text about databases' })
      index.addDocument('doc2', { title: 'Système de base de données', content: 'French text' })
      index.addDocument('doc3', { title: '🔥 Emoji Title 🚀', content: 'With emoji content' })

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      expect(loaded.documentCount).toBe(3)

      // Verify unicode doc IDs and content preserved
      const stats = loaded.getDocumentStats('doc1')
      expect(stats).not.toBeNull()
    })

    it('handles very long document IDs', async () => {
      const index = new InvertedIndex(storage, 'articles', definition)

      const longId = 'a'.repeat(1000)
      index.addDocument(longId, { title: 'Test' })

      await index.save()

      const loaded = new InvertedIndex(storage, 'articles', definition)
      await loaded.load()

      expect(loaded.documentCount).toBe(1)
      const stats = loaded.getDocumentStats(longId)
      expect(stats).not.toBeNull()
    })
  })
})

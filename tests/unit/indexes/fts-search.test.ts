/**
 * Tests for Full-Text Search
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '../../../src/indexes/fts/search'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { IndexDefinition } from '../../../src/indexes/types'

describe('FTSIndex', () => {
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

  describe('basic search', () => {
    it('finds documents containing search terms', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Introduction to Databases', content: 'Databases store data' } },
        { docId: 'doc2', doc: { title: 'Web Development', content: 'Building websites' } },
        { docId: 'doc3', doc: { title: 'Database Performance', content: 'Optimizing queries' } },
      ])

      const results = index.search('database')
      expect(results).toHaveLength(2)

      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('doc1')
      expect(docIds).toContain('doc3')
    })

    it('ranks by relevance (BM25)', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'low', doc: { title: 'Other topic', content: 'Some content about database systems' } },
        { docId: 'high', doc: { title: 'Database Database Database', content: 'Database systems use many databases here' } },
      ])

      const results = index.search('database')

      expect(results.length).toBeGreaterThan(0)
      // Document with more occurrences should rank higher
      expect(results[0].docId).toBe('high')
      expect(results.length).toBeGreaterThan(1)
      expect(results[0].score).toBeGreaterThan(results[1].score)
    })

    it('returns matching terms', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems', content: 'Query optimization' } },
      ])

      const results = index.search('database query')

      expect(results).toHaveLength(1)
      expect(results[0].matchedTokens).toContain('databas') // stemmed
      expect(results[0].matchedTokens).toContain('queri') // stemmed
    })

    it('returns empty for no matches', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Web Development', content: 'Building websites' } },
      ])

      const results = index.search('database')
      expect(results).toHaveLength(0)
    })

    it('handles empty query', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Test', content: 'Content' } },
      ])

      const results = index.search('')
      expect(results).toHaveLength(0)
    })

    it('handles empty index', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      const results = index.search('database')
      expect(results).toHaveLength(0)
    })
  })

  describe('search options', () => {
    it('respects limit option', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database 1' } },
        { docId: 'doc2', doc: { title: 'Database 2' } },
        { docId: 'doc3', doc: { title: 'Database 3' } },
        { docId: 'doc4', doc: { title: 'Database 4' } },
        { docId: 'doc5', doc: { title: 'Database 5' } },
      ])

      const results = index.search('database', { limit: 3 })
      expect(results).toHaveLength(3)
    })

    it('respects minScore option', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'high', doc: { title: 'Database Database', content: 'Database systems' } },
        { docId: 'low', doc: { title: 'Other', content: 'mentions database once' } },
      ])

      const results = index.search('database', { minScore: 1 })

      // All results should have score >= 1
      for (const result of results) {
        expect(result.score).toBeGreaterThanOrEqual(1)
      }
    })
  })

  describe('multi-term search', () => {
    it('finds documents matching multiple terms', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'both', doc: { title: 'Database Performance', content: 'Optimizing' } },
        { docId: 'one', doc: { title: 'Database Basics', content: 'Introduction' } },
        { docId: 'other', doc: { title: 'Performance Tips', content: 'Optimization' } },
      ])

      const results = index.search('database performance')

      // Document with both terms should rank first
      expect(results[0].docId).toBe('both')
    })
  })

  describe('stemming', () => {
    it('matches stemmed variants', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Running Applications' } },
        { docId: 'doc2', doc: { title: 'Application Runner' } },
        { docId: 'doc3', doc: { title: 'Run the App' } },
      ])

      const results = index.search('running')

      // Should match all variations of 'run'
      expect(results.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('document management', () => {
    it('adds documents', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'First Document' })
      index.addDocument('doc2', { title: 'Second Document' })

      expect(index.documentCount).toBe(2)

      const results = index.search('document')
      expect(results).toHaveLength(2)
    })

    it('removes documents', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'First Database' })
      index.addDocument('doc2', { title: 'Second Database' })

      const removed = index.removeDocument('doc1')
      expect(removed).toBe(true)
      expect(index.documentCount).toBe(1)

      const results = index.search('database')
      expect(results).toHaveLength(1)
      expect(results[0].docId).toBe('doc2')
    })

    it('clears index', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Document' })
      index.addDocument('doc2', { title: 'Another' })

      index.clear()

      expect(index.documentCount).toBe(0)
      expect(index.vocabularySize).toBe(0)
    })
  })

  describe('statistics', () => {
    it('tracks document count', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      expect(index.documentCount).toBe(0)

      index.addDocument('doc1', { title: 'One' })
      expect(index.documentCount).toBe(1)

      index.addDocument('doc2', { title: 'Two' })
      expect(index.documentCount).toBe(2)
    })

    it('tracks vocabulary size', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      expect(index.vocabularySize).toBe(0)

      index.addDocument('doc1', { title: 'hello world' })
      const sizeAfterFirst = index.vocabularySize

      index.addDocument('doc2', { title: 'hello again' }) // 'hello' already exists
      const sizeAfterSecond = index.vocabularySize

      expect(sizeAfterSecond).toBeGreaterThan(0)
      expect(sizeAfterSecond).toBeLessThan(sizeAfterFirst * 2) // Not doubled
    })

    it('provides stats', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database Systems', content: 'SQL queries' })
      index.addDocument('doc2', { title: 'Web Development', content: 'HTTP requests' })

      const stats = index.getStats()
      expect(stats.vocabularySize).toBeGreaterThan(0)
      expect(stats.avgDocLength).toBeGreaterThan(0)
    })
  })

  describe('persistence', () => {
    it('saves and loads index', async () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database Systems' })
      index.addDocument('doc2', { title: 'Web Development' })

      await index.save()

      const loaded = new FTSIndex(storage, 'articles', definition)
      await loaded.load()

      expect(loaded.documentCount).toBe(2)

      const results = loaded.search('database')
      expect(results).toHaveLength(1)
      expect(results[0].docId).toBe('doc1')
    })

    it('handles empty index load', async () => {
      const index = new FTSIndex(storage, 'articles', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.documentCount).toBe(0)
    })
  })

  describe('document frequency', () => {
    it('tracks document frequency', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.addDocument('doc1', { title: 'Database One' })
      index.addDocument('doc2', { title: 'Database Two' })
      index.addDocument('doc3', { title: 'Other Topic' })

      // 'databas' (stemmed) appears in 2 documents
      const df = index.getDocumentFrequency('databas')
      expect(df).toBe(2)
    })
  })

  describe('nested fields', () => {
    it('indexes nested field paths', () => {
      const nestedDef: IndexDefinition = {
        name: 'idx_nested',
        type: 'fts',
        fields: [{ path: 'meta.title' }],
      }

      const index = new FTSIndex(storage, 'items', nestedDef)

      index.addDocument('doc1', { meta: { title: 'Database Systems' } })
      index.addDocument('doc2', { meta: { title: 'Web Apps' } })

      const results = index.search('database')
      expect(results).toHaveLength(1)
      expect(results[0].docId).toBe('doc1')
    })
  })
})

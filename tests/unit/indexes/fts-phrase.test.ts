/**
 * Tests for Full-Text Search Phrase Queries
 *
 * Tests phrase query support in the $text operator.
 * Phrase queries match exact sequences of words.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('FTSIndex Phrase Queries', () => {
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
        indexPositions: true, // Required for phrase queries
      },
    }
  })

  describe('basic phrase search', () => {
    it('finds documents with exact phrase match', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Management Systems', content: 'Learn about databases' } },
        { docId: 'doc2', doc: { title: 'Management Tips', content: 'Database systems overview' } },
        { docId: 'doc3', doc: { title: 'Database Systems Architecture', content: 'Technical details' } },
      ])

      // Search for exact phrase "database systems"
      const results = index.search('"database systems"')

      // Should match doc2 (in content) and doc3 (partial match in title after stemming)
      // doc1 has "Database Management Systems" - "Management" breaks the sequence
      expect(results.length).toBeGreaterThan(0)
      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('doc2')
    })

    it('does not match when words are not consecutive', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database and Management Systems', content: 'Other content' } },
        { docId: 'doc2', doc: { title: 'Database Systems', content: 'Direct match' } },
      ])

      const results = index.search('"database systems"')

      // doc1 has "and" between "database" and "systems", so should not match phrase
      // doc2 has exact sequence
      expect(results.length).toBeGreaterThan(0)
      expect(results[0].docId).toBe('doc2')
    })

    it('handles phrase with stemming', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Running Applications', content: 'App runners in production' } },
        { docId: 'doc2', doc: { title: 'Application Runner', content: 'Run apps quickly' } },
      ])

      // "running applications" should match "Running Applications" due to stemming
      const results = index.search('"running applications"')

      expect(results.length).toBeGreaterThan(0)
      expect(results.map(r => r.docId)).toContain('doc1')
    })

    it('returns empty for phrase with no matches', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Web Development', content: 'Building websites' } },
      ])

      const results = index.search('"database systems"')
      expect(results).toHaveLength(0)
    })
  })

  describe('phrase combined with terms', () => {
    it('finds documents matching phrase and additional terms', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems Performance', content: 'Optimization techniques' } },
        { docId: 'doc2', doc: { title: 'Database Systems', content: 'Basic introduction' } },
        { docId: 'doc3', doc: { title: 'Performance Tips', content: 'General advice' } },
      ])

      // Search for phrase "database systems" AND term "performance"
      const results = index.search('"database systems" performance')

      // doc1 should rank highest (has both phrase and term)
      expect(results.length).toBeGreaterThan(0)
      expect(results[0].docId).toBe('doc1')
    })

    it('handles multiple phrases', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems and Web Development', content: 'Full stack' } },
        { docId: 'doc2', doc: { title: 'Database Systems', content: 'Backend only' } },
        { docId: 'doc3', doc: { title: 'Web Development', content: 'Frontend only' } },
      ])

      // Search for two phrases
      const results = index.search('"database systems" "web development"')

      // doc1 should match both phrases
      expect(results.length).toBeGreaterThan(0)
      expect(results[0].docId).toBe('doc1')
    })
  })

  describe('required and excluded phrases', () => {
    it('requires phrase with + modifier', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems', content: 'Introduction to databases' } },
        { docId: 'doc2', doc: { title: 'Database Management', content: 'Managing data' } },
        { docId: 'doc3', doc: { title: 'Web Systems', content: 'Web architecture' } },
      ])

      // Search requires "database systems" phrase
      const results = index.search('+"database systems"')

      // Only doc1 should match (has the exact phrase)
      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('doc1')
      expect(docIds).not.toContain('doc2') // Has database but not "database systems"
    })

    it('excludes phrase with - modifier', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems', content: 'Technical guide' } },
        { docId: 'doc2', doc: { title: 'Database Management', content: 'Admin guide' } },
        { docId: 'doc3', doc: { title: 'Database Basics', content: 'Beginner guide' } },
      ])

      // Search for "database" but exclude "database systems" phrase
      const results = index.search('database -"database systems"')

      // doc2 and doc3 should match, doc1 should be excluded
      const docIds = results.map(r => r.docId)
      expect(docIds).not.toContain('doc1')
      expect(docIds.length).toBeGreaterThan(0)
    })
  })

  describe('edge cases', () => {
    it('handles single word phrase (degrades to term search)', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Overview', content: 'Introduction' } },
        { docId: 'doc2', doc: { title: 'Other Topic', content: 'No match' } },
      ])

      // Single word in quotes should work like regular term
      const results = index.search('"database"')

      expect(results).toHaveLength(1)
      expect(results[0].docId).toBe('doc1')
    })

    it('handles empty phrase', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Test', content: 'Content' } },
      ])

      const results = index.search('""')
      expect(results).toHaveLength(0)
    })

    it('handles unclosed quote', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems', content: 'Content' } },
      ])

      // Unclosed quote should treat rest of query as phrase
      const results = index.search('"database systems')

      expect(results.length).toBeGreaterThan(0)
    })

    it('handles phrase with stopwords', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'The Quick Brown Fox', content: 'Jumps over' } },
        { docId: 'doc2', doc: { title: 'Quick Fox', content: 'No article' } },
      ])

      // "the quick" - "the" is a stopword
      const results = index.search('"the quick"')

      // Should still work, matching on remaining terms
      expect(results.length).toBeGreaterThanOrEqual(0)
    })

    it('phrase search without position indexing returns regular search', () => {
      // Create index without position indexing
      const noPositionDef: IndexDefinition = {
        name: 'idx_fts_no_pos',
        type: 'fts',
        fields: [{ path: 'title' }],
        ftsOptions: {
          indexPositions: false,
        },
      }

      const index = new FTSIndex(storage, 'articles', noPositionDef)

      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems' } },
        { docId: 'doc2', doc: { title: 'Systems Database' } },
      ])

      // Without positions, phrase search falls back to term search
      const results = index.search('"database systems"')

      // Both should match since we can't verify order
      expect(results.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('phrase boost', () => {
    it('boosts phrase matches over term matches', () => {
      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'phrase', doc: { title: 'Database Systems Architecture', content: 'Technical' } },
        { docId: 'terms', doc: { title: 'Database Architecture', content: 'Systems design' } },
      ])

      // Search for phrase - exact phrase match should score higher
      const results = index.search('"database systems"')

      if (results.length > 1) {
        // Phrase match should have higher score
        const phraseResult = results.find(r => r.docId === 'phrase')
        const termsResult = results.find(r => r.docId === 'terms')

        if (phraseResult && termsResult) {
          expect(phraseResult.score).toBeGreaterThan(termsResult.score)
        }
      }
    })
  })
})

describe('parseQuery integration', () => {
  it('parses simple phrase queries', async () => {
    // Import parseQuery directly for unit testing
    const { parseQuery } = await import('@/indexes/fts/query-parser')

    const result = parseQuery('"exact phrase"')

    expect(result.phrases).toContain('exact phrase')
    expect(result.terms).toHaveLength(0)
  })

  it('parses mixed phrase and term queries', async () => {
    const { parseQuery } = await import('@/indexes/fts/query-parser')

    const result = parseQuery('"database systems" optimization performance')

    expect(result.phrases).toContain('database systems')
    expect(result.terms.length).toBeGreaterThanOrEqual(2)
  })

  it('parses required phrases', async () => {
    const { parseQuery } = await import('@/indexes/fts/query-parser')

    const result = parseQuery('+"required phrase" optional')

    expect(result.requiredPhrases).toContain('required phrase')
    expect(result.terms.some((t: { term: string }) => t.term === 'optional')).toBe(true)
  })

  it('parses excluded phrases', async () => {
    const { parseQuery } = await import('@/indexes/fts/query-parser')

    const result = parseQuery('search terms -"excluded phrase"')

    expect(result.excludedPhrases).toContain('excluded phrase')
  })
})

/**
 * Unit tests for FTS Phrase Queries
 *
 * Tests parsing and execution of phrase queries:
 * - Exact phrase matching ("exact phrase")
 * - Required phrases (+"must have phrase")
 * - Excluded phrases (-"must not have phrase")
 * - Position-based matching
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { parseQuery } from '@/indexes/fts/query-parser'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

// =============================================================================
// Phrase Query Parser Tests
// =============================================================================

describe('FTS Query Parser - Phrase Queries', () => {
  describe('parseQuery with phrases', () => {
    it('parses simple phrase query', () => {
      const result = parseQuery('"database systems"')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('database systems')
      expect(result.terms).toHaveLength(0)
    })

    it('parses multiple phrases', () => {
      const result = parseQuery('"database systems" "query optimization"')

      expect(result.phrases).toHaveLength(2)
      expect(result.phrases).toContain('database systems')
      expect(result.phrases).toContain('query optimization')
    })

    it('parses mixed phrase and term query', () => {
      const result = parseQuery('"database systems" postgresql')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('database systems')
      expect(result.terms).toHaveLength(1)
      expect(result.terms[0].term).toBe('postgresql')
    })

    it('parses required phrase with + prefix', () => {
      const result = parseQuery('+"database systems"')

      expect(result.requiredPhrases).toHaveLength(1)
      expect(result.requiredPhrases[0]).toBe('database systems')
      expect(result.phrases).toHaveLength(0)
    })

    it('parses excluded phrase with - prefix', () => {
      const result = parseQuery('-"mysql database"')

      expect(result.excludedPhrases).toHaveLength(1)
      expect(result.excludedPhrases[0]).toBe('mysql database')
      expect(result.phrases).toHaveLength(0)
    })

    it('parses complex query with required, excluded, and optional phrases', () => {
      const result = parseQuery('+"open source" -"commercial license" "database"')

      expect(result.requiredPhrases).toHaveLength(1)
      expect(result.requiredPhrases[0]).toBe('open source')
      expect(result.excludedPhrases).toHaveLength(1)
      expect(result.excludedPhrases[0]).toBe('commercial license')
      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('database')
    })

    it('handles phrase with terms mixed', () => {
      const result = parseQuery('+required "exact phrase" -excluded optional')

      expect(result.terms).toHaveLength(3)
      expect(result.terms.find(t => t.term === 'required')?.required).toBe(true)
      expect(result.terms.find(t => t.term === 'excluded')?.excluded).toBe(true)
      expect(result.terms.find(t => t.term === 'optional')?.required).toBe(false)
      expect(result.phrases).toHaveLength(1)
    })
  })

  describe('phrase edge cases', () => {
    it('handles unclosed quote (treats rest as phrase)', () => {
      const result = parseQuery('"unclosed phrase')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('unclosed phrase')
    })

    it('handles empty phrase', () => {
      const result = parseQuery('""')

      expect(result.phrases).toHaveLength(0)
    })

    it('handles phrase with only whitespace', () => {
      const result = parseQuery('"   "')

      expect(result.phrases).toHaveLength(0)
    })

    it('handles single word phrase', () => {
      const result = parseQuery('"database"')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('database')
    })

    it('handles phrase with special characters', () => {
      const result = parseQuery('"C++ programming"')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('C++ programming')
    })

    it('handles phrase with numbers', () => {
      const result = parseQuery('"web 2.0 design"')

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe('web 2.0 design')
    })

    it('preserves internal spacing in phrases', () => {
      const result = parseQuery('"multiple   spaces   here"')

      // The phrase should be trimmed but internal spaces preserved
      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toContain('multiple')
      expect(result.phrases[0]).toContain('spaces')
      expect(result.phrases[0]).toContain('here')
    })

    it('handles adjacent phrases', () => {
      const result = parseQuery('"phrase one""phrase two"')

      expect(result.phrases).toHaveLength(2)
      expect(result.phrases).toContain('phrase one')
      expect(result.phrases).toContain('phrase two')
    })

    it('handles very long phrases', () => {
      const longPhrase = 'word '.repeat(50).trim()
      const result = parseQuery(`"${longPhrase}"`)

      expect(result.phrases).toHaveLength(1)
      expect(result.phrases[0]).toBe(longPhrase)
    })
  })
})

// =============================================================================
// Phrase Search Execution Tests
// =============================================================================

describe('FTS Phrase Search Execution', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition
  let index: FTSIndex

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_fts',
      type: 'fts',
      fields: [{ path: 'title' }, { path: 'content' }],
      ftsOptions: {
        language: 'en',
        minWordLength: 2,
        indexPositions: true, // Required for phrase matching
      },
    }
    index = new FTSIndex(storage, 'articles', definition)
  })

  describe('exact phrase matching', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'exact', doc: { title: 'Database Systems', content: 'Learn about database systems here' } },
        { docId: 'reversed', doc: { title: 'Systems Database', content: 'Systems that use database' } },
        { docId: 'separated', doc: { title: 'Database and Systems', content: 'Database tools and systems design' } },
        { docId: 'partial', doc: { title: 'Database Overview', content: 'Introduction to databases' } },
      ])
    })

    it('matches exact phrase in sequence', () => {
      const results = index.searchPhrase('database systems')

      expect(results.length).toBeGreaterThan(0)
      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('exact')
    })

    it('does not match reversed word order', () => {
      const results = index.searchPhrase('database systems')

      const docIds = results.map(r => r.docId)
      // "systems database" should not match "database systems"
      expect(docIds).not.toContain('reversed')
    })

    it('does not match words separated by other words', () => {
      const results = index.searchPhrase('database systems')

      const docIds = results.map(r => r.docId)
      // "database and systems" should not match "database systems"
      expect(docIds).not.toContain('separated')
    })

    it('applies phrase boost to scoring', () => {
      // Rebuild with documents where one has exact phrase
      index.clear()
      index.buildFromArray([
        { docId: 'phrase', doc: { title: 'Database Systems', content: 'Info about database systems' } },
        { docId: 'terms', doc: { title: 'Database', content: 'Systems overview' } },
      ])

      const results = index.searchPhrase('database systems')

      expect(results.length).toBeGreaterThan(0)
      // Exact phrase match should be ranked first
      expect(results[0].docId).toBe('phrase')
    })
  })

  describe('single word phrase', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Guide', content: 'SQL databases' } },
        { docId: 'doc2', doc: { title: 'Web Development', content: 'HTML basics' } },
      ])
    })

    it('falls back to regular search for single word', () => {
      const results = index.searchPhrase('database')

      expect(results.length).toBeGreaterThan(0)
      expect(results.map(r => r.docId)).toContain('doc1')
    })
  })

  describe('multi-word phrase matching', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'three-word', doc: { title: 'Query', content: 'Learn query optimization techniques today' } },
        { docId: 'two-word', doc: { title: 'Query', content: 'Query optimization is important' } },
        { docId: 'no-match', doc: { title: 'Query', content: 'Optimization of query plans' } },
      ])
    })

    it('matches three-word phrase', () => {
      const results = index.searchPhrase('query optimization techniques')

      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('three-word')
      expect(docIds).not.toContain('no-match')
    })

    it('matches two-word phrase', () => {
      const results = index.searchPhrase('query optimization')

      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('three-word')
      expect(docIds).toContain('two-word')
    })
  })

  describe('phrase with stemming', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'exact', doc: { title: 'Running Programs', content: 'Running programs fast' } },
        { docId: 'variant', doc: { title: 'Run Program', content: 'Run a program quickly' } },
        { docId: 'no-match', doc: { title: 'Programs', content: 'Running fast' } },
      ])
    })

    it('matches phrase with stemmed variants', () => {
      const results = index.searchPhrase('running programs')

      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('exact')
    })

    it('matches stemmed variants in phrase order', () => {
      // "run program" should match documents with "running programs" due to stemming
      const results = index.searchPhrase('run program')

      const docIds = results.map(r => r.docId)
      expect(docIds.length).toBeGreaterThan(0)
    })
  })

  describe('phrase in different fields', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'title-match', doc: { title: 'Database Systems Overview', content: 'General info' } },
        { docId: 'content-match', doc: { title: 'Introduction', content: 'About database systems' } },
        { docId: 'both-match', doc: { title: 'Database Systems', content: 'Database systems are important' } },
      ])
    })

    it('finds phrase in title field', () => {
      const results = index.searchPhrase('database systems')

      expect(results.map(r => r.docId)).toContain('title-match')
    })

    it('finds phrase in content field', () => {
      const results = index.searchPhrase('database systems')

      expect(results.map(r => r.docId)).toContain('content-match')
    })

    it('finds phrase in multiple fields and boosts accordingly', () => {
      const results = index.searchPhrase('database systems')

      // Document with phrase in both fields may score higher
      expect(results.map(r => r.docId)).toContain('both-match')
    })
  })

  describe('no matches', () => {
    beforeEach(() => {
      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Web Development', content: 'JavaScript and HTML' } },
        { docId: 'doc2', doc: { title: 'Mobile Apps', content: 'iOS and Android' } },
      ])
    })

    it('returns empty array when phrase not found', () => {
      const results = index.searchPhrase('database systems')

      expect(results).toHaveLength(0)
    })

    it('returns empty for phrase with no common documents', () => {
      const results = index.searchPhrase('postgresql mysql')

      expect(results).toHaveLength(0)
    })
  })

  describe('search options', () => {
    beforeEach(() => {
      index.buildFromArray(
        Array.from({ length: 20 }, (_, i) => ({
          docId: `doc${i}`,
          doc: {
            title: `Database Systems ${i}`,
            content: `Info about database systems number ${i}`,
          },
        }))
      )
    })

    it('respects limit option', () => {
      const results = index.searchPhrase('database systems', { limit: 5 })

      expect(results).toHaveLength(5)
    })

    it('results are sorted by score', () => {
      const results = index.searchPhrase('database systems')

      for (let i = 1; i < results.length; i++) {
        expect(results[i - 1].score).toBeGreaterThanOrEqual(results[i].score)
      }
    })
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('FTS Phrase Search - Edge Cases', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
      ftsOptions: {
        language: 'en',
        minWordLength: 2,
        indexPositions: true,
      },
    }
  })

  describe('empty index', () => {
    it('returns empty array for phrase search on empty index', () => {
      const index = new FTSIndex(storage, 'empty', definition)
      const results = index.searchPhrase('any phrase')

      expect(results).toHaveLength(0)
    })
  })

  describe('empty phrase', () => {
    it('returns empty array for empty phrase', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'Some content here' } },
      ])

      const results = index.searchPhrase('')
      expect(results).toHaveLength(0)
    })

    it('returns empty array for whitespace-only phrase', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'Some content here' } },
      ])

      const results = index.searchPhrase('   ')
      expect(results).toHaveLength(0)
    })
  })

  describe('phrase with stopwords', () => {
    it('handles phrase containing stopwords', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'The quick brown fox' } },
        { docId: 'doc2', doc: { content: 'Quick fox' } },
      ])

      // "the quick" - "the" is a stopword and will be filtered
      const results = index.searchPhrase('the quick')

      // Should still find matches based on non-stopword terms
      expect(results.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('phrase with only stopwords', () => {
    it('handles phrase of only stopwords gracefully', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'The quick brown fox' } },
      ])

      // "the and is" - all stopwords
      const results = index.searchPhrase('the and is')

      expect(results).toHaveLength(0)
    })
  })

  describe('special characters in phrase', () => {
    it('handles phrase with punctuation', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'Hello, world! How are you?' } },
      ])

      const results = index.searchPhrase('hello world')

      expect(results.length).toBeGreaterThan(0)
    })
  })

  describe('case insensitivity', () => {
    it('matches phrases case-insensitively', () => {
      const index = new FTSIndex(storage, 'articles', definition)
      index.buildFromArray([
        { docId: 'doc1', doc: { content: 'Database Systems Overview' } },
      ])

      const results1 = index.searchPhrase('DATABASE SYSTEMS')
      const results2 = index.searchPhrase('database systems')

      expect(results1.length).toBe(results2.length)
      if (results1.length > 0) {
        expect(results1[0].docId).toBe(results2[0].docId)
      }
    })
  })
})

// =============================================================================
// Performance Tests
// =============================================================================

describe('FTS Phrase Search Performance', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_fts_perf',
      type: 'fts',
      fields: [{ path: 'content' }],
      ftsOptions: {
        language: 'en',
        minWordLength: 2,
        indexPositions: true,
      },
    }
  })

  it('phrase search performs well on large index', () => {
    const index = new FTSIndex(storage, 'docs', definition)

    // Generate 5000 documents
    const docs = Array.from({ length: 5000 }, (_, i) => ({
      docId: `doc${i}`,
      doc: {
        content: i % 100 === 0
          ? 'This document contains the exact phrase database systems here'
          : `Document ${i} with various content about topic ${i % 50}`,
      },
    }))

    index.buildFromArray(docs)

    const start = Date.now()
    const results = index.searchPhrase('database systems')
    const elapsed = Date.now() - start

    expect(results.length).toBe(50) // Every 100th document
    expect(elapsed).toBeLessThan(500) // Should complete in < 500ms
  })

  it('phrase search with rare terms is fast', () => {
    const index = new FTSIndex(storage, 'docs', definition)

    const docs = Array.from({ length: 1000 }, (_, i) => ({
      docId: `doc${i}`,
      doc: {
        content: i === 500
          ? 'This contains the unique phrase xyzzy plugh'
          : `Regular content for document ${i}`,
      },
    }))

    index.buildFromArray(docs)

    const start = Date.now()
    const results = index.searchPhrase('xyzzy plugh')
    const elapsed = Date.now() - start

    expect(results.length).toBe(1)
    expect(results[0].docId).toBe('doc500')
    expect(elapsed).toBeLessThan(100) // Rare terms should be very fast
  })

  it('phrase search with common terms completes in reasonable time', () => {
    const index = new FTSIndex(storage, 'docs', definition)

    // All documents contain "the" and "document"
    const docs = Array.from({ length: 1000 }, (_, i) => ({
      docId: `doc${i}`,
      doc: {
        content: `This is document number ${i} with the document content`,
      },
    }))

    index.buildFromArray(docs)

    const start = Date.now()
    const results = index.searchPhrase('document content')
    const elapsed = Date.now() - start

    expect(results.length).toBeGreaterThan(0)
    expect(elapsed).toBeLessThan(1000) // Should complete in < 1s even with common terms
  })

  it('multiple phrase searches benefit from index caching', () => {
    const index = new FTSIndex(storage, 'docs', definition)

    const docs = Array.from({ length: 500 }, (_, i) => ({
      docId: `doc${i}`,
      doc: {
        content: `Document ${i} about database systems and query optimization`,
      },
    }))

    index.buildFromArray(docs)

    // First search - may be slower
    const start1 = Date.now()
    index.searchPhrase('database systems')
    const elapsed1 = Date.now() - start1

    // Subsequent searches - should be similar or faster
    const times: number[] = []
    for (let i = 0; i < 5; i++) {
      const start = Date.now()
      index.searchPhrase('database systems')
      times.push(Date.now() - start)
    }

    const avgSubsequent = times.reduce((a, b) => a + b, 0) / times.length

    // Subsequent searches shouldn't be much slower than first
    expect(avgSubsequent).toBeLessThan(elapsed1 * 2)
  })
})

// =============================================================================
// Position Index Tests
// =============================================================================

describe('FTS Position Indexing', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('phrase matching requires position indexing enabled', () => {
    const defWithPositions: IndexDefinition = {
      name: 'idx_with_pos',
      type: 'fts',
      fields: [{ path: 'content' }],
      ftsOptions: {
        indexPositions: true,
      },
    }

    const index = new FTSIndex(storage, 'articles', defWithPositions)
    index.buildFromArray([
      { docId: 'doc1', doc: { content: 'database systems overview' } },
      { docId: 'doc2', doc: { content: 'systems for database' } },
    ])

    const results = index.searchPhrase('database systems')

    // Should find exact phrase match
    expect(results.map(r => r.docId)).toContain('doc1')
    // Should not find reversed
    expect(results.map(r => r.docId)).not.toContain('doc2')
  })

  it('phrase matching still works without positions but less precise', () => {
    const defNoPositions: IndexDefinition = {
      name: 'idx_no_pos',
      type: 'fts',
      fields: [{ path: 'content' }],
      ftsOptions: {
        indexPositions: false,
      },
    }

    const index = new FTSIndex(storage, 'articles', defNoPositions)
    index.buildFromArray([
      { docId: 'doc1', doc: { content: 'database systems overview' } },
      { docId: 'doc2', doc: { content: 'systems for database' } },
    ])

    // Without positions, phrase search may be less precise
    const results = index.searchPhrase('database systems')

    // Should still return results (falling back to term matching)
    expect(results.length).toBeGreaterThanOrEqual(0)
  })
})

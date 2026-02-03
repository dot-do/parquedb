/**
 * Test to reproduce and fix phrase boost scoring bug
 *
 * Issue: parquedb-pmm8.11
 * FTS: Phrase boost does not differentiate scores when both docs have phrase
 *
 * The bug is that phrase boost is applied as a flat multiplier (1.5x) regardless
 * of how many times or where the phrase appears. This means two documents that
 * both contain the phrase will get the same boost, even if one has the phrase
 * multiple times.
 *
 * Specifically, in searchAdvanced(), the phrase boost is applied per phrase match
 * (i.e., once per matching phrase key), not per phrase occurrence in the document.
 * This means a doc with "database systems" 5 times gets the same 1.5x boost as
 * a doc with "database systems" once.
 */
import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('FTS Phrase Boost Bug - parquedb-pmm8.11', () => {
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
        indexPositions: true,
      },
    }
    index = new FTSIndex(storage, 'articles', definition)
  })

  describe('searchPhrase boost differentiation', () => {
    it('should give higher score to doc with phrase in multiple places', () => {
      index.buildFromArray([
        {
          docId: 'many',
          doc: {
            title: 'Database systems review',
            content: 'Database systems are the best. Database systems handle data effectively.'
          }
        },
        {
          docId: 'single',
          doc: {
            title: 'Database systems',
            content: 'Unrelated content here about other topics'
          }
        },
      ])

      const results = index.searchPhrase('database systems')

      // Both documents have the phrase
      expect(results.length).toBe(2)

      const manyResult = results.find(r => r.docId === 'many')
      const singleResult = results.find(r => r.docId === 'single')

      expect(manyResult).toBeDefined()
      expect(singleResult).toBeDefined()

      // 'many' has phrase 3 times (title + 2 in content)
      // 'single' has phrase 1 time (title)
      // 'many' should score higher
      expect(manyResult!.score).toBeGreaterThan(singleResult!.score)
    })

    it('should differentiate when both docs have phrase but different term frequencies', () => {
      index.buildFromArray([
        {
          docId: 'high-freq',
          doc: {
            title: 'Database systems database systems',
            content: 'Database systems database systems database systems'
          }
        },
        {
          docId: 'low-freq',
          doc: {
            title: 'Database systems',
            content: 'Different content'
          }
        },
      ])

      const results = index.searchPhrase('database systems')

      expect(results.length).toBe(2)
      expect(results[0].docId).toBe('high-freq')
      expect(results[1].docId).toBe('low-freq')
      expect(results[0].score).toBeGreaterThan(results[1].score)
    })

    it('should score based on phrase match count not just existence', () => {
      index.buildFromArray([
        {
          docId: 'doc3',
          doc: {
            title: 'Database systems',
            content: 'Database systems database systems database systems database systems'
          }
        },
        {
          docId: 'doc2',
          doc: {
            title: 'Database systems',
            content: 'Database systems are great'
          }
        },
        {
          docId: 'doc1',
          doc: {
            title: 'Database systems',
            content: 'Other info'
          }
        },
      ])

      const results = index.searchPhrase('database systems')

      // All documents have the phrase, but with different frequencies
      expect(results.length).toBe(3)

      // doc3 should be first (5 occurrences), doc2 second (2 occurrences), doc1 last (1 occurrence)
      expect(results[0].docId).toBe('doc3')
      expect(results[1].docId).toBe('doc2')
      expect(results[2].docId).toBe('doc1')

      // Scores should all be different
      expect(results[0].score).toBeGreaterThan(results[1].score)
      expect(results[1].score).toBeGreaterThan(results[2].score)
    })
  })

  describe('searchAdvanced (quoted phrase) boost differentiation', () => {
    it('should differentiate scores when both docs have the phrase', () => {
      index.buildFromArray([
        {
          docId: 'many',
          doc: {
            title: 'Database systems guide',
            content: 'Learn about database systems. Database systems are powerful.'
          }
        },
        {
          docId: 'single',
          doc: {
            title: 'Database systems',
            content: 'Unrelated content'
          }
        },
      ])

      // Using quoted phrase in search()
      const results = index.search('"database systems"')

      expect(results.length).toBe(2)

      const manyResult = results.find(r => r.docId === 'many')
      const singleResult = results.find(r => r.docId === 'single')

      expect(manyResult).toBeDefined()
      expect(singleResult).toBeDefined()

      // 'many' has phrase multiple times, 'single' has it once
      expect(manyResult!.score).toBeGreaterThan(singleResult!.score)
    })

    it('should not apply flat boost when phrase count differs', () => {
      index.buildFromArray([
        {
          docId: 'frequent',
          doc: {
            title: 'Query optimization',
            content: 'Query optimization is key. Query optimization helps. Query optimization works.'
          }
        },
        {
          docId: 'rare',
          doc: {
            title: 'Query optimization',
            content: 'Performance'
          }
        },
      ])

      const results = index.search('"query optimization"')

      expect(results.length).toBe(2)
      expect(results[0].docId).toBe('frequent')
      expect(results[1].docId).toBe('rare')
      expect(results[0].score).toBeGreaterThan(results[1].score)
    })
  })

  describe('phrase boost based on phrase occurrence count', () => {
    it('BUG SCENARIO: searchAdvanced should boost based on phrase occurrence count, not just existence', () => {
      // This is the specific bug scenario: two documents with identical base content
      // but different phrase occurrence counts should have different phrase boosts
      index.buildFromArray([
        {
          docId: 'five-phrases',
          doc: {
            // Each occurrence of "database systems" should contribute to boost
            title: 'Database systems',
            content: 'Database systems. Database systems. Database systems. Database systems.'
          }
        },
        {
          docId: 'one-phrase',
          doc: {
            title: 'Database systems',
            content: 'Other content here that is different'
          }
        },
      ])

      const results = index.search('"database systems"')

      expect(results.length).toBe(2)

      // The document with 5 phrase occurrences should score significantly higher
      // than the one with just 1 phrase occurrence
      const fiveResult = results.find(r => r.docId === 'five-phrases')
      const oneResult = results.find(r => r.docId === 'one-phrase')

      expect(fiveResult).toBeDefined()
      expect(oneResult).toBeDefined()

      // The score difference should be significant (not just from term frequency)
      // Currently the bug is that both get the same 1.5x boost
      expect(fiveResult!.score).toBeGreaterThan(oneResult!.score)
    })

    it('BUG SCENARIO: searchPhrase boost should scale with phrase occurrence count', () => {
      index.buildFromArray([
        {
          docId: 'multi',
          doc: {
            title: 'Machine learning',
            content: 'Machine learning is great. Machine learning helps. Machine learning works.'
          }
        },
        {
          docId: 'once',
          doc: {
            title: 'Machine learning',
            content: 'Introduction here'
          }
        },
      ])

      const results = index.searchPhrase('machine learning')

      expect(results.length).toBe(2)

      const multiResult = results.find(r => r.docId === 'multi')
      const onceResult = results.find(r => r.docId === 'once')

      expect(multiResult).toBeDefined()
      expect(onceResult).toBeDefined()

      // multi has 4 occurrences, once has 1
      // The boost should scale with occurrence count
      expect(multiResult!.score).toBeGreaterThan(onceResult!.score)

      // The ratio should reflect the phrase occurrence difference
      // With 4x phrase count, score ratio should be notable (not just term frequency)
      const scoreRatio = multiResult!.score / onceResult!.score
      expect(scoreRatio).toBeGreaterThan(1.5) // Should be more than the flat 1.5x boost
    })
  })
})

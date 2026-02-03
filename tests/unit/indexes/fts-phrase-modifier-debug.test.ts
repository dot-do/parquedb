/**
 * Debug tests for FTS phrase modifiers
 * Issue: parquedb-pmm8.6 - Required (+) and excluded (-) phrase modifiers not working correctly
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { parseQuery } from '@/indexes/fts/query-parser'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('FTS Phrase Modifier Debug', () => {
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

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Database Systems', content: 'Introduction to databases' } },
      { docId: 'doc2', doc: { title: 'Database Management', content: 'Managing data' } },
      { docId: 'doc3', doc: { title: 'Web Systems', content: 'Web architecture' } },
    ])
  })

  describe('parseQuery verification', () => {
    it('parses required phrase correctly', () => {
      const parsed = parseQuery('+"database systems"')
      expect(parsed.requiredPhrases).toContain('database systems')
      expect(parsed.phrases).toHaveLength(0)
      expect(parsed.excludedPhrases).toHaveLength(0)
    })

    it('parses excluded phrase correctly', () => {
      const parsed = parseQuery('-"database systems"')
      expect(parsed.excludedPhrases).toContain('database systems')
      expect(parsed.phrases).toHaveLength(0)
      expect(parsed.requiredPhrases).toHaveLength(0)
    })

    it('parses complex query with term and excluded phrase', () => {
      const parsed = parseQuery('database -"database systems"')
      expect(parsed.terms).toHaveLength(1)
      expect(parsed.terms[0]?.term).toBe('database')
      expect(parsed.excludedPhrases).toContain('database systems')
    })
  })

  describe('required phrase modifier (+)', () => {
    it('only returns docs with the exact required phrase', () => {
      const results = index.search('+"database systems"')
      const docIds = results.map(r => r.docId)

      // Only doc1 has the exact phrase "database systems"
      expect(docIds).toContain('doc1')
      // doc2 has "database" and doc3 has "systems" but not together
      expect(docIds).not.toContain('doc2')
      expect(docIds).not.toContain('doc3')
    })
  })

  describe('excluded phrase modifier (-)', () => {
    it('excludes docs matching the phrase', () => {
      const results = index.search('database -"database systems"')
      const docIds = results.map(r => r.docId)

      // doc1 has "database systems" phrase - should be excluded
      expect(docIds).not.toContain('doc1')
      // doc2 has "database" but not the phrase "database systems"
      expect(docIds).toContain('doc2')
      // doc3 doesn't have "database" at all
      expect(docIds).not.toContain('doc3')
    })
  })

  describe('edge cases for phrase modifiers', () => {
    it('handles only excluded phrase query', () => {
      // With only an exclusion phrase, should return all docs that DON'T match
      const results = index.search('-"database systems"')
      const docIds = results.map(r => r.docId)

      // doc1 has "database systems" - should be excluded
      expect(docIds).not.toContain('doc1')
      // doc2 and doc3 should be included
      expect(docIds).toContain('doc2')
      expect(docIds).toContain('doc3')
    })

    it('handles multiple required phrases', () => {
      // Add more docs for this test
      index.clear()
      index.buildFromArray([
        { docId: 'both', doc: { title: 'Database Systems Overview', content: 'Web Systems information' } },
        { docId: 'only-db', doc: { title: 'Database Systems', content: 'No web' } },
        { docId: 'only-web', doc: { title: 'Web Systems', content: 'No database' } },
        { docId: 'neither', doc: { title: 'Other Topic', content: 'Nothing relevant' } },
      ])

      const results = index.search('+"database systems" +"web systems"')
      const docIds = results.map(r => r.docId)

      // Only 'both' has both phrases
      expect(docIds).toContain('both')
      expect(docIds).not.toContain('only-db')
      expect(docIds).not.toContain('only-web')
      expect(docIds).not.toContain('neither')
    })

    it('handles mix of required phrase and excluded phrase', () => {
      index.clear()
      index.buildFromArray([
        { docId: 'has-both', doc: { title: 'Database Systems', content: 'MySQL database info' } },
        { docId: 'has-db-only', doc: { title: 'Database Systems', content: 'PostgreSQL info' } },
        { docId: 'no-db', doc: { title: 'Web Apps', content: 'JavaScript' } },
      ])

      // Require "database systems" but exclude "mysql"
      const results = index.search('+"database systems" -mysql')
      const docIds = results.map(r => r.docId)

      // has-both has "database systems" but also "mysql" - should be excluded
      expect(docIds).not.toContain('has-both')
      // has-db-only has "database systems" and no "mysql" - should be included
      expect(docIds).toContain('has-db-only')
      // no-db doesn't have "database systems" - should not be included
      expect(docIds).not.toContain('no-db')
    })

    it('handles optional phrase combined with excluded phrase', () => {
      index.clear()
      index.buildFromArray([
        { docId: 'doc1', doc: { title: 'Database Systems', content: 'Technical guide' } },
        { docId: 'doc2', doc: { title: 'Database Management', content: 'Admin guide' } },
        { docId: 'doc3', doc: { title: 'Database Basics', content: 'Beginner guide' } },
      ])

      // Optional phrase "database" + excluded phrase "database systems"
      const results = index.search('"database" -"database systems"')
      const docIds = results.map(r => r.docId)

      // doc1 has "database systems" phrase - should be excluded
      expect(docIds).not.toContain('doc1')
      // doc2 and doc3 have "database" but not "database systems" phrase
      expect(docIds.length).toBeGreaterThan(0)
    })
  })
})

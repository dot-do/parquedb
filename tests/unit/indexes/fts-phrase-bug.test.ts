/**
 * Test to reproduce the phrase position matching bug
 *
 * Issue: parquedb-pmm8.5
 * FTS: Phrase query position matching produces incorrect results
 *
 * The bug is that positions from different fields are merged together,
 * causing false matches when positions happen to align across fields.
 */
import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('FTS Phrase Position Bug - Cross-Field Position Matching', () => {
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

  it('should NOT match phrase when words are in different fields at aligned positions', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      {
        docId: 'cross-field',
        doc: {
          title: 'Database management',  // "database" at position 0
          content: 'systems overview'     // "systems" at position 0
        }
      },
      {
        docId: 'same-field',
        doc: {
          title: 'Database systems guide',  // "database systems" consecutive
          content: 'Introduction'
        }
      },
    ])

    const results = index.searchPhrase('database systems')

    // Should ONLY match 'same-field' where the phrase appears consecutively
    // Should NOT match 'cross-field' where database is in title and systems is in content
    const docIds = results.map(r => r.docId)

    expect(docIds).toContain('same-field')
    expect(docIds).not.toContain('cross-field')
  })

  it('should NOT match phrase when positions happen to align across fields', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      {
        docId: 'aligned-positions',
        doc: {
          title: 'quick brown fox',      // quick=0, brown=1, fox=2
          content: 'lazy dog sleeps'     // lazy=0, dog=1, sleeps=2
        }
      },
      {
        docId: 'actual-match',
        doc: {
          title: 'Quick dog training',   // quick=0, dog=1
          content: 'Other content'
        }
      },
    ])

    const results = index.searchPhrase('quick dog')

    const docIds = results.map(r => r.docId)

    expect(docIds).toContain('actual-match')
    // aligned-positions has quick at 0 in title, dog at 1 in content
    // if we merge positions across fields, it would falsely match
    expect(docIds).not.toContain('aligned-positions')
  })

  it('should correctly match phrase in second field', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      {
        docId: 'content-match',
        doc: {
          title: 'Introduction',
          content: 'Learn about database systems here'  // phrase is in content
        }
      },
      {
        docId: 'no-match',
        doc: {
          title: 'Database overview',   // database in title
          content: 'Systems guide'       // systems in content (different field)
        }
      },
    ])

    const results = index.searchPhrase('database systems')

    const docIds = results.map(r => r.docId)

    expect(docIds).toContain('content-match')
    expect(docIds).not.toContain('no-match')
  })

  it('should match phrase that appears in both fields', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      {
        docId: 'both-fields',
        doc: {
          title: 'Database systems',
          content: 'Info about database systems'
        }
      },
    ])

    const results = index.searchPhrase('database systems')

    expect(results.length).toBeGreaterThan(0)
    expect(results[0].docId).toBe('both-fields')
  })

  it('should handle three-word phrase with cross-field positions', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      {
        docId: 'cross-field-3',
        doc: {
          title: 'quick brown jumps',    // quick=0, brown=1, jumps=2
          content: 'fox over lazy'       // fox=0, over=1, lazy=2
        }
      },
      {
        docId: 'actual-phrase',
        doc: {
          title: 'Quick brown fox',      // quick=0, brown=1, fox=2
          content: 'Other content'
        }
      },
    ])

    const results = index.searchPhrase('quick brown fox')

    const docIds = results.map(r => r.docId)

    expect(docIds).toContain('actual-phrase')
    // cross-field-3 has quick, brown in title and fox in content
    expect(docIds).not.toContain('cross-field-3')
  })
})

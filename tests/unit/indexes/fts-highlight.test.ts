/**
 * Tests for FTS Highlight/Snippet Generation
 */

import { describe, it, expect } from 'vitest'
import {
  highlightText,
  generateSnippet,
  generateHighlights,
  type HighlightOptions,
  type SnippetOptions,
} from '@/indexes/fts/highlight'
import { tokenize } from '@/indexes/fts/tokenizer'

describe('FTS Highlight', () => {
  describe('highlightText', () => {
    it('highlights matching terms in text', () => {
      const text = 'The quick brown fox jumps over the lazy dog'
      const terms = ['quick', 'lazy']

      const result = highlightText(text, terms)
      expect(result).toBe('The <mark>quick</mark> brown fox jumps over the <mark>lazy</mark> dog')
    })

    it('handles case-insensitive matching', () => {
      const text = 'DATABASE systems and Database management'
      const terms = ['database']

      const result = highlightText(text, terms)
      expect(result).toBe('<mark>DATABASE</mark> systems and <mark>Database</mark> management')
    })

    it('uses custom highlight tags', () => {
      const text = 'The quick brown fox'
      const terms = ['quick']
      const options: HighlightOptions = {
        preTag: '<em class="highlight">',
        postTag: '</em>',
      }

      const result = highlightText(text, terms, options)
      expect(result).toBe('The <em class="highlight">quick</em> brown fox')
    })

    it('handles empty terms array', () => {
      const text = 'The quick brown fox'
      const result = highlightText(text, [])
      expect(result).toBe(text)
    })

    it('handles no matches', () => {
      const text = 'The quick brown fox'
      const result = highlightText(text, ['elephant'])
      expect(result).toBe(text)
    })

    it('handles overlapping matches (takes longest)', () => {
      const text = 'database systems'
      const terms = ['data', 'database']

      const result = highlightText(text, terms)
      expect(result).toBe('<mark>database</mark> systems')
    })

    it('matches stemmed terms', () => {
      // "running" stems to "run", "databases" stems to "databas"
      const text = 'Running applications with databases'
      const terms = ['run', 'databas'] // stemmed forms

      const result = highlightText(text, terms, { matchStemmed: true })
      expect(result).toBe('<mark>Running</mark> applications with <mark>databases</mark>')
    })

    it('escapes HTML in original text', () => {
      const text = 'The <script>alert("xss")</script> quick fox'
      const terms = ['quick']

      const result = highlightText(text, terms)
      expect(result).toBe('The &lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt; <mark>quick</mark> fox')
    })
  })

  describe('generateSnippet', () => {
    it('generates snippet around first match', () => {
      const text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. The database stores all the data. Sed do eiusmod tempor.'
      const terms = ['database']

      const result = generateSnippet(text, terms)
      expect(result).toContain('<mark>database</mark>')
      // Should be truncated with ellipsis
      expect(result.length).toBeLessThan(text.length + 50)
    })

    it('respects maxLength option', () => {
      const text = 'The quick brown fox jumps over the lazy dog repeatedly many times'
      const terms = ['fox']
      const options: SnippetOptions = { maxLength: 50 }

      const result = generateSnippet(text, terms, options)
      // Account for highlight tags and ellipsis
      const contentOnly = result.replace(/<\/?mark>/g, '').replace(/\.\.\./g, '')
      expect(contentOnly.length).toBeLessThanOrEqual(60) // Some flexibility for word boundaries
    })

    it('returns beginning of text with no matches', () => {
      const text = 'The quick brown fox jumps over the lazy dog'
      const terms = ['elephant']
      const options: SnippetOptions = { maxLength: 20 }

      const result = generateSnippet(text, terms, options)
      expect(result).toMatch(/^The quick/)
    })

    it('adds ellipsis when truncating', () => {
      const text = 'Before the match. The database is here. After the match with more text.'
      const terms = ['database']
      const options: SnippetOptions = { maxLength: 40, ellipsis: '...' }

      const result = generateSnippet(text, terms, options)
      expect(result).toMatch(/\.\.\./)
    })

    it('handles multiple matches and centers on best', () => {
      const text = 'First mention of database. Second database occurrence is here. Third database mention.'
      const terms = ['database']
      const options: SnippetOptions = { maxLength: 60 }

      const result = generateSnippet(text, terms, options)
      expect(result).toContain('<mark>database</mark>')
    })

    it('uses custom highlight tags in snippet', () => {
      const text = 'The database stores data'
      const terms = ['database']
      const options: SnippetOptions = {
        preTag: '<b>',
        postTag: '</b>',
      }

      const result = generateSnippet(text, terms, options)
      expect(result).toContain('<b>database</b>')
    })
  })

  describe('generateHighlights', () => {
    it('generates highlights for multiple fields', () => {
      const doc = {
        title: 'Database Systems',
        content: 'The database stores important data.',
        author: 'John Doe',
      }
      const fields = ['title', 'content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms)

      expect(result.title).toBeDefined()
      expect(result.title![0]).toBe('<mark>Database</mark> Systems')

      expect(result.content).toBeDefined()
      expect(result.content![0]).toContain('<mark>database</mark>')
    })

    it('returns snippets for long content', () => {
      const longContent = 'Start. '.repeat(20) + 'The database is here. ' + 'End. '.repeat(20)
      const doc = {
        title: 'Short Title',
        content: longContent,
      }
      const fields = ['title', 'content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms, { maxSnippetLength: 80 })

      expect(result.content![0]).toContain('<mark>database</mark>')
      expect(result.content![0].length).toBeLessThan(longContent.length)
    })

    it('skips fields with no matches', () => {
      const doc = {
        title: 'Web Development',
        content: 'Building modern applications',
      }
      const fields = ['title', 'content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms, { includeNonMatching: false })

      expect(result.title).toBeUndefined()
      expect(result.content).toBeUndefined()
    })

    it('handles nested fields', () => {
      const doc = {
        meta: {
          title: 'Database Guide',
        },
        content: 'Main content',
      }
      const fields = ['meta.title', 'content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms)

      expect(result['meta.title']).toBeDefined()
      expect(result['meta.title']![0]).toContain('<mark>Database</mark>')
    })

    it('generates multiple snippets per field', () => {
      const longContent = 'First database mention here. ' +
        'Some filler text. '.repeat(10) +
        'Second database mention here. ' +
        'More filler text. '.repeat(10) +
        'Third database mention here.'

      const doc = { content: longContent }
      const fields = ['content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms, {
        maxSnippets: 3,
        maxSnippetLength: 50,
      })

      expect(result.content).toBeDefined()
      expect(result.content!.length).toBeGreaterThanOrEqual(2)
    })

    it('respects maxSnippets option', () => {
      const content = 'database one. database two. database three. database four. database five.'
      const doc = { content }
      const fields = ['content']
      const terms = ['database']

      const result = generateHighlights(doc, fields, terms, { maxSnippets: 2 })

      expect(result.content).toBeDefined()
      expect(result.content!.length).toBeLessThanOrEqual(2)
    })
  })

  describe('integration with tokenizer', () => {
    it('highlights based on stemmed query terms', () => {
      // "running" stems to "run", "databases" stems to "databas"
      const text = 'Running applications with databases and more running code'
      const queryTerms = tokenize('running databases', { stem: true }).map(t => t.term)

      const result = highlightText(text, queryTerms, { matchStemmed: true })

      expect(result).toContain('<mark>Running</mark>')
      expect(result).toContain('<mark>databases</mark>')
      expect(result).toContain('<mark>running</mark>')
    })
  })
})

describe('FTSIndex highlight integration', () => {
  let storage: any
  let definition: any

  beforeEach(async () => {
    const { MemoryBackend } = await import('@/storage/MemoryBackend')
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

  it('adds highlights to search results using addHighlights', async () => {
    const { FTSIndex } = await import('@/indexes/fts/search')

    const index = new FTSIndex(storage, 'articles', definition)

    const docs = [
      { $id: 'doc1', title: 'Introduction to Databases', content: 'Databases store data efficiently' },
      { $id: 'doc2', title: 'Web Development', content: 'Building websites with frameworks' },
      { $id: 'doc3', title: 'Database Performance', content: 'Optimizing database queries' },
    ]

    index.buildFromArray(docs.map(d => ({ docId: d.$id, doc: d })))

    const results = index.search('database')

    // Create document map
    const documents = new Map(docs.map(d => [d.$id, d]))

    // Add highlights
    const withHighlights = index.addHighlights(results, documents, 'database')

    expect(withHighlights.length).toBeGreaterThan(0)

    // Check that highlights are present
    for (const result of withHighlights) {
      expect(result.highlights).toBeDefined()
      // At least one field should have highlights
      const highlightKeys = Object.keys(result.highlights!)
      expect(highlightKeys.length).toBeGreaterThan(0)

      // Highlights should contain mark tags
      for (const key of highlightKeys) {
        const snippets = result.highlights![key]!
        expect(snippets.length).toBeGreaterThan(0)
        expect(snippets.some(s => s.includes('<mark>'))).toBe(true)
      }
    }
  })

  it('uses searchWithHighlights for combined search and highlight', async () => {
    const { FTSIndex } = await import('@/indexes/fts/search')

    const index = new FTSIndex(storage, 'articles', definition)

    const docs = [
      { $id: 'doc1', title: 'Database Systems', content: 'Learn about SQL databases' },
      { $id: 'doc2', title: 'Other Topic', content: 'Not about databases at all' },
    ]

    index.buildFromArray(docs.map(d => ({ docId: d.$id, doc: d })))

    // Create a document lookup function
    const docMap = new Map(docs.map(d => [d.$id, d]))
    const getDocuments = async (docIds: string[]) => {
      const result = new Map<string, Record<string, unknown>>()
      for (const id of docIds) {
        const doc = docMap.get(id)
        if (doc) result.set(id, doc)
      }
      return result
    }

    const results = await index.searchWithHighlights('database', getDocuments, {
      highlight: true,
    })

    expect(results.length).toBeGreaterThan(0)

    // First result should be doc1 (has database in both fields)
    const firstResult = results[0]!
    expect(firstResult.highlights).toBeDefined()
    expect(firstResult.highlights!.title?.[0]).toContain('<mark>Database</mark>')
  })

  it('respects custom highlight tags', async () => {
    const { FTSIndex } = await import('@/indexes/fts/search')

    const index = new FTSIndex(storage, 'articles', definition)

    const docs = [
      { $id: 'doc1', title: 'Database Guide', content: 'Content here' },
    ]

    index.buildFromArray(docs.map(d => ({ docId: d.$id, doc: d })))

    const documents = new Map(docs.map(d => [d.$id, d as Record<string, unknown>]))
    const results = index.search('database')

    const withHighlights = index.addHighlights(results, documents, 'database', {
      preTag: '<em class="hl">',
      postTag: '</em>',
    })

    expect(withHighlights[0]!.highlights!.title![0]).toContain('<em class="hl">Database</em>')
  })

  it('returns results without highlights when highlight is false', async () => {
    const { FTSIndex } = await import('@/indexes/fts/search')

    const index = new FTSIndex(storage, 'articles', definition)

    const docs = [
      { $id: 'doc1', title: 'Database Guide', content: 'Content here' },
    ]

    index.buildFromArray(docs.map(d => ({ docId: d.$id, doc: d })))

    const docMap = new Map(docs.map(d => [d.$id, d]))
    const getDocuments = async (docIds: string[]) => {
      const result = new Map<string, Record<string, unknown>>()
      for (const id of docIds) {
        const doc = docMap.get(id)
        if (doc) result.set(id, doc)
      }
      return result
    }

    const results = await index.searchWithHighlights('database', getDocuments, {
      highlight: false,
    })

    expect(results.length).toBe(1)
    expect(results[0]!.highlights).toBeUndefined()
  })
})

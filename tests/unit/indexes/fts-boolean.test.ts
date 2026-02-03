/**
 * Tests for Boolean Query Support in Full-Text Search
 *
 * Tests for the $text operator with boolean query syntax:
 * - AND operator: "word1 AND word2"
 * - OR operator: "word1 OR word2"
 * - NOT operator: "-excluded" or "word1 NOT word2"
 * - Combined operators: "word1 AND word2 OR word3"
 * - Phrase with boolean: '"exact phrase" AND word'
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '@/indexes/fts/search'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'
import {
  parseQuery,
  parseBooleanQuery,
  type BooleanQuery,
  type BooleanClause,
} from '@/indexes/fts/query-parser'

describe('FTS Boolean Query Support', () => {
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

    // Build test corpus
    index.buildFromArray([
      {
        docId: 'doc1',
        doc: {
          title: 'Database Systems Introduction',
          content: 'Learn about SQL and NoSQL databases for web applications',
        },
      },
      {
        docId: 'doc2',
        doc: {
          title: 'Web Development Basics',
          content: 'Building modern websites with JavaScript and HTML',
        },
      },
      {
        docId: 'doc3',
        doc: {
          title: 'Database Performance Optimization',
          content: 'Optimizing SQL queries for better performance',
        },
      },
      {
        docId: 'doc4',
        doc: {
          title: 'Mobile App Development',
          content: 'Building apps with React Native and databases',
        },
      },
      {
        docId: 'doc5',
        doc: {
          title: 'Advanced JavaScript',
          content: 'Deep dive into JavaScript patterns and performance',
        },
      },
    ])
  })

  describe('Query Parser - Boolean Operators', () => {
    describe('AND operator', () => {
      it('parses "word1 AND word2" syntax', () => {
        const parsed = parseBooleanQuery('database AND performance')

        expect(parsed.type).toBe('and')
        expect(parsed.clauses).toHaveLength(2)
        expect(parsed.clauses[0].terms).toContain('databas') // stemmed
        expect(parsed.clauses[1].terms).toContain('perform') // stemmed
      })

      it('parses multiple AND operators', () => {
        const parsed = parseBooleanQuery('database AND sql AND performance')

        expect(parsed.type).toBe('and')
        expect(parsed.clauses).toHaveLength(3)
      })

      it('handles case-insensitive AND', () => {
        const parsed1 = parseBooleanQuery('database AND performance')
        const parsed2 = parseBooleanQuery('database and performance')

        expect(parsed1.type).toBe(parsed2.type)
        expect(parsed1.clauses).toHaveLength(parsed2.clauses.length)
      })
    })

    describe('OR operator', () => {
      it('parses "word1 OR word2" syntax', () => {
        const parsed = parseBooleanQuery('database OR website')

        expect(parsed.type).toBe('or')
        expect(parsed.clauses).toHaveLength(2)
      })

      it('parses multiple OR operators', () => {
        const parsed = parseBooleanQuery('database OR sql OR nosql')

        expect(parsed.type).toBe('or')
        expect(parsed.clauses).toHaveLength(3)
      })
    })

    describe('NOT operator', () => {
      it('parses "-excluded" syntax', () => {
        const parsed = parseBooleanQuery('database -mobile')

        // Default is OR for ranking, but exclusion is handled separately
        expect(parsed.type).toBe('or')
        const excludedClause = parsed.clauses.find(c => c.excluded)
        expect(excludedClause).toBeDefined()
        expect(excludedClause?.terms).toContain('mobil') // stemmed
      })

      it('parses "NOT word" syntax', () => {
        const parsed = parseBooleanQuery('database NOT mobile')

        // Default is OR for ranking, but exclusion is handled separately
        expect(parsed.type).toBe('or')
        const excludedClause = parsed.clauses.find(c => c.excluded)
        expect(excludedClause).toBeDefined()
      })
    })

    describe('combined operators', () => {
      it('parses "word1 AND word2 OR word3"', () => {
        const parsed = parseBooleanQuery('database AND sql OR nosql')

        // AND has higher precedence, so: (database AND sql) OR nosql
        expect(parsed.type).toBe('or')
      })

      it('parses "(word1 OR word2) AND word3" with implicit grouping', () => {
        const parsed = parseBooleanQuery('(database OR sql) AND performance')

        expect(parsed.type).toBe('and')
      })

      it('parses "word1 AND -word2"', () => {
        const parsed = parseBooleanQuery('database AND -mobile')

        expect(parsed.type).toBe('and')
        expect(parsed.clauses.some(c => c.excluded)).toBe(true)
      })
    })

    describe('phrase with boolean', () => {
      it('parses \'"exact phrase" AND word\'', () => {
        const parsed = parseBooleanQuery('"database systems" AND performance')

        expect(parsed.type).toBe('and')
        expect(parsed.clauses[0].phrase).toBe('database systems')
      })

      it('parses \'word OR "exact phrase"\'', () => {
        const parsed = parseBooleanQuery('performance OR "web development"')

        expect(parsed.type).toBe('or')
        expect(parsed.clauses[1].phrase).toBe('web development')
      })
    })

    describe('required terms (+)', () => {
      it('parses "+required" as required term', () => {
        const parsed = parseBooleanQuery('+database sql')

        const requiredClause = parsed.clauses.find(c => c.required)
        expect(requiredClause).toBeDefined()
        expect(requiredClause?.terms).toContain('databas')
      })

      it('treats +term as implicit AND', () => {
        const parsed = parseBooleanQuery('+database +performance')

        // Both terms are required = AND semantics
        expect(parsed.clauses.filter(c => c.required)).toHaveLength(2)
      })
    })
  })

  describe('FTSIndex Boolean Search', () => {
    describe('AND queries', () => {
      it('returns documents matching ALL terms with AND', () => {
        const results = index.searchBoolean('database AND performance')

        expect(results.length).toBeGreaterThan(0)
        // Only doc3 has both "database" in title and "performance" in content
        const docIds = results.map(r => r.docId)
        expect(docIds).toContain('doc3')
        // doc1 has "database" but not "performance"
        expect(docIds).not.toContain('doc1')
        // doc5 has "performance" but not "database"
        expect(docIds).not.toContain('doc5')
      })

      it('returns empty for AND with no matching docs', () => {
        const results = index.searchBoolean('database AND mobile AND javascript')

        // No doc has all three
        expect(results).toHaveLength(0)
      })
    })

    describe('OR queries', () => {
      it('returns documents matching ANY term with OR', () => {
        const results = index.searchBoolean('database OR javascript')

        const docIds = results.map(r => r.docId)
        // doc1 has "database"
        expect(docIds).toContain('doc1')
        // doc2 has "javascript"
        expect(docIds).toContain('doc2')
        // doc4 has both "database" and nothing about javascript directly
        expect(docIds).toContain('doc4')
        // doc5 has "javascript"
        expect(docIds).toContain('doc5')
      })

      it('ranks documents with more matches higher', () => {
        const results = index.searchBoolean('database OR sql')

        // doc1 and doc3 both have "database" and "sql"
        // doc1: "Database Systems Introduction" + "Learn about SQL and NoSQL databases..."
        // doc3: "Database Performance Optimization" + "Optimizing SQL queries..."
        // BM25 scoring may favor doc1 due to term frequency and document length
        // Both should be at the top
        const topDocIds = results.slice(0, 2).map(r => r.docId)
        expect(topDocIds).toContain('doc1')
        expect(topDocIds).toContain('doc3')
      })
    })

    describe('NOT queries (exclusion)', () => {
      it('excludes documents with -term', () => {
        const results = index.searchBoolean('database -mobile')

        const docIds = results.map(r => r.docId)
        // doc1, doc3 have "database" without "mobile"
        expect(docIds).toContain('doc1')
        expect(docIds).toContain('doc3')
        // doc4 has "database" but also "mobile" - should be excluded
        expect(docIds).not.toContain('doc4')
      })

      it('excludes documents with NOT keyword', () => {
        const results = index.searchBoolean('javascript NOT performance')

        const docIds = results.map(r => r.docId)
        // doc2 has "javascript" without "performance"
        expect(docIds).toContain('doc2')
        // doc5 has both "javascript" and "performance" - should be excluded
        expect(docIds).not.toContain('doc5')
      })
    })

    describe('combined boolean queries', () => {
      it('handles "A AND B OR C"', () => {
        const results = index.searchBoolean('database AND sql OR javascript')

        const docIds = results.map(r => r.docId)
        // doc1 has "database" and "sql" - matches (database AND sql)
        expect(docIds).toContain('doc1')
        // doc3 has "database" and "sql" - matches (database AND sql)
        expect(docIds).toContain('doc3')
        // doc2 has "javascript" - matches OR javascript
        expect(docIds).toContain('doc2')
      })

      it('handles "A AND -B"', () => {
        const results = index.searchBoolean('database AND -web')

        const docIds = results.map(r => r.docId)
        // doc3 has "database" without "web"
        expect(docIds).toContain('doc3')
        // doc1 has "database" but also "web" - should be excluded
        expect(docIds).not.toContain('doc1')
      })

      it('handles "(A OR B) AND C" with grouping', () => {
        const results = index.searchBoolean('(database OR javascript) AND performance')

        const docIds = results.map(r => r.docId)
        // doc3 has "database" and "performance"
        expect(docIds).toContain('doc3')
        // doc5 has "javascript" and "performance"
        expect(docIds).toContain('doc5')
        // doc1 has "database" but no "performance"
        expect(docIds).not.toContain('doc1')
      })
    })

    describe('phrase with boolean', () => {
      it('handles \'"phrase" AND word\'', () => {
        const results = index.searchBoolean('"database systems" AND sql')

        const docIds = results.map(r => r.docId)
        // Only doc1 has "database systems" as a phrase
        expect(docIds).toContain('doc1')
        expect(results).toHaveLength(1)
      })

      it('handles \'word OR "phrase"\'', () => {
        const results = index.searchBoolean('optimization OR "web development"')

        const docIds = results.map(r => r.docId)
        // doc2 has "web development" as phrase
        expect(docIds).toContain('doc2')
        // doc3 has "optimization"
        expect(docIds).toContain('doc3')
      })
    })

    describe('required terms (+)', () => {
      it('returns only documents with required terms', () => {
        const results = index.searchBoolean('+database sql nosql')

        // All results must have "database"
        for (const result of results) {
          expect(['doc1', 'doc3', 'doc4']).toContain(result.docId)
        }
      })
    })

    describe('edge cases', () => {
      it('handles empty query', () => {
        const results = index.searchBoolean('')
        expect(results).toHaveLength(0)
      })

      it('handles query with only NOT terms', () => {
        // "-mobile" alone should match all docs except those with "mobile"
        const results = index.searchBoolean('-mobile')

        const docIds = results.map(r => r.docId)
        expect(docIds).not.toContain('doc4')
        // Other docs should be included
        expect(docIds).toContain('doc1')
        expect(docIds).toContain('doc2')
        expect(docIds).toContain('doc3')
        expect(docIds).toContain('doc5')
      })

      it('handles complex query with all operators', () => {
        const results = index.searchBoolean('(database OR javascript) AND performance -mobile')

        const docIds = results.map(r => r.docId)
        // doc3: has database + performance, no mobile
        expect(docIds).toContain('doc3')
        // doc5: has javascript + performance, no mobile
        expect(docIds).toContain('doc5')
        // doc4 would match database + has no performance
        expect(docIds).not.toContain('doc4')
      })
    })
  })

  describe('Integration with search()', () => {
    it('detects boolean query and uses searchBoolean', () => {
      // When query contains boolean operators, search() should delegate to searchBoolean()
      const results = index.search('database AND performance')

      expect(results.length).toBeGreaterThan(0)
      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('doc3')
    })

    it('uses regular search for non-boolean queries', () => {
      const results = index.search('database')

      // Regular search returns all docs with "database"
      const docIds = results.map(r => r.docId)
      expect(docIds).toContain('doc1')
      expect(docIds).toContain('doc3')
      expect(docIds).toContain('doc4')
    })
  })
})

describe('Query Parser Boolean Functions', () => {
  describe('isBooleanQuery()', () => {
    it('returns true for queries with AND', () => {
      expect(parseBooleanQuery('a AND b').type).toBe('and')
    })

    it('returns true for queries with OR', () => {
      expect(parseBooleanQuery('a OR b').type).toBe('or')
    })

    it('returns true for queries with NOT', () => {
      const parsed = parseBooleanQuery('a NOT b')
      expect(parsed.clauses.some(c => c.excluded)).toBe(true)
    })

    it('returns true for queries with parentheses', () => {
      const parsed = parseBooleanQuery('(a OR b)')
      expect(parsed.type).toBe('or')
    })

    it('handles default (implicit OR) for simple multi-term queries', () => {
      // "a b" without operators defaults to OR (match any)
      const parsed = parseBooleanQuery('database sql')
      // Default behavior is OR for ranking purposes
      expect(parsed.type).toBe('or')
    })
  })
})

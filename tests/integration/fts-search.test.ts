/**
 * Integration Tests for Full-Text Search
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FTSIndex } from '../../src/indexes/fts/search'
import { IndexManager } from '../../src/indexes/manager'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { IndexDefinition } from '../../src/indexes/types'

describe('Full-Text Search Integration', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('FTSIndex with movie data', () => {
    it('searches movie titles and descriptions', () => {
      const definition: IndexDefinition = {
        name: 'idx_fts_movies',
        type: 'fts',
        fields: [{ path: 'title' }, { path: 'description' }],
        ftsOptions: {
          language: 'en',
          minWordLength: 2,
        },
      }

      const index = new FTSIndex(storage, 'movies', definition)

      const movies = [
        {
          $id: 'tt001',
          title: 'The Matrix',
          description: 'A computer hacker learns about the true nature of reality',
        },
        {
          $id: 'tt002',
          title: 'Inception',
          description: 'A thief who steals corporate secrets through dream sharing technology',
        },
        {
          $id: 'tt003',
          title: 'The Matrix Reloaded',
          description: 'Neo continues the fight against the machines',
        },
        {
          $id: 'tt004',
          title: 'Interstellar',
          description: 'A team of explorers travel through a wormhole in space',
        },
        {
          $id: 'tt005',
          title: 'The Dark Knight',
          description: 'Batman faces the Joker in Gotham City',
        },
      ]

      index.buildFromArray(movies.map(doc => ({ docId: doc.$id, doc })))

      // Search for "matrix"
      const matrixResults = index.search('matrix')
      expect(matrixResults).toHaveLength(2)
      const matrixDocIds = matrixResults.map(r => r.docId)
      expect(matrixDocIds).toContain('tt001')
      expect(matrixDocIds).toContain('tt003')

      // Search for "space exploration"
      const spaceResults = index.search('space exploration')
      expect(spaceResults.length).toBeGreaterThan(0)
      // Interstellar should rank high
      expect(spaceResults.some(r => r.docId === 'tt004')).toBe(true)

      // Search for "computer hacker"
      const hackerResults = index.search('computer hacker')
      expect(hackerResults.length).toBeGreaterThan(0)
      expect(hackerResults[0].docId).toBe('tt001') // The Matrix
    })

    it('handles stemming correctly', () => {
      const definition: IndexDefinition = {
        name: 'idx_fts',
        type: 'fts',
        fields: [{ path: 'content' }],
      }

      const index = new FTSIndex(storage, 'articles', definition)

      index.buildFromArray([
        { docId: 'd1', doc: { content: 'The programmer is programming a program' } },
        { docId: 'd2', doc: { content: 'The developer develops software' } },
      ])

      // Search for "program" should match variations
      const results = index.search('program')
      expect(results.length).toBeGreaterThan(0)
      expect(results[0].docId).toBe('d1')
    })
  })

  describe('FTSIndex with O*NET occupation data', () => {
    it('searches occupation titles and descriptions', () => {
      const definition: IndexDefinition = {
        name: 'idx_fts_occupations',
        type: 'fts',
        fields: [{ path: 'title' }, { path: 'description' }],
      }

      const index = new FTSIndex(storage, 'occupations', definition)

      const occupations = [
        {
          $id: '15-1252',
          title: 'Software Developers',
          description: 'Research, design, and develop computer and network software or specialized utility programs',
        },
        {
          $id: '15-2051',
          title: 'Data Scientists',
          description: 'Develop and implement methods to analyze large amounts of data',
        },
        {
          $id: '29-1141',
          title: 'Registered Nurses',
          description: 'Assess patient health problems and needs, develop and implement nursing care plans',
        },
        {
          $id: '15-1299',
          title: 'Computer Occupations, All Other',
          description: 'All computer occupations not listed separately',
        },
      ]

      index.buildFromArray(occupations.map(doc => ({ docId: doc.$id, doc })))

      // Search for software development
      const softwareResults = index.search('software development')
      expect(softwareResults.length).toBeGreaterThan(0)
      expect(softwareResults[0].docId).toBe('15-1252')

      // Search for data analysis
      const dataResults = index.search('data analysis')
      expect(dataResults.length).toBeGreaterThan(0)
      expect(dataResults.some(r => r.docId === '15-2051')).toBe(true)

      // Search for nursing care
      const nursingResults = index.search('nursing care')
      expect(nursingResults.length).toBeGreaterThan(0)
      expect(nursingResults[0].docId).toBe('29-1141')
    })
  })

  describe('FTSIndex performance characteristics', () => {
    it('handles large document collections', () => {
      const definition: IndexDefinition = {
        name: 'idx_fts_large',
        type: 'fts',
        fields: [{ path: 'content' }],
      }

      const index = new FTSIndex(storage, 'docs', definition)

      // Generate 1000 documents
      const docs = Array.from({ length: 1000 }, (_, i) => ({
        docId: `doc${i}`,
        doc: {
          content: `Document ${i} contains various words like ${
            i % 10 === 0 ? 'database' : 'other'
          } and ${i % 5 === 0 ? 'programming' : 'content'}`,
        },
      }))

      const startBuild = Date.now()
      index.buildFromArray(docs)
      const buildTime = Date.now() - startBuild

      expect(index.documentCount).toBe(1000)
      expect(buildTime).toBeLessThan(5000) // Should build in < 5 seconds

      // Search should be fast
      const startSearch = Date.now()
      const results = index.search('database')
      const searchTime = Date.now() - startSearch

      expect(results.length).toBe(100) // Every 10th document
      expect(searchTime).toBeLessThan(100) // Search should be < 100ms
    })

    it('respects limit option', () => {
      const definition: IndexDefinition = {
        name: 'idx_fts',
        type: 'fts',
        fields: [{ path: 'content' }],
      }

      const index = new FTSIndex(storage, 'docs', definition)

      // Generate 100 documents all containing "test"
      const docs = Array.from({ length: 100 }, (_, i) => ({
        docId: `doc${i}`,
        doc: { content: `Test document number ${i}` },
      }))

      index.buildFromArray(docs)

      const results = index.search('test', { limit: 10 })
      expect(results).toHaveLength(10)
    })
  })

  describe('IndexManager with FTS', () => {
    it('creates and uses FTS index', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('articles', {
        name: 'idx_fts_content',
        type: 'fts',
        fields: [{ path: 'title' }, { path: 'body' }],
      })

      const indexes = await manager.listIndexes('articles')
      expect(indexes).toHaveLength(1)
      expect(indexes[0].definition.type).toBe('fts')

      // Select index for $text query
      const plan = await manager.selectIndex('articles', {
        $text: { $search: 'database' },
      })

      expect(plan).not.toBeNull()
      expect(plan?.type).toBe('fts')
    })
  })
})

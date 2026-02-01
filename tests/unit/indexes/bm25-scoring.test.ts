/**
 * Tests for BM25 Scoring
 */

import { describe, it, expect } from 'vitest'
import {
  BM25Scorer,
  DEFAULT_BM25_CONFIG,
  logScaledTf,
  augmentedTf,
  luceneIdf,
  tfidf,
} from '@/indexes/fts/scoring'
import type { CorpusStats, Posting } from '@/indexes/fts/types'

describe('BM25Scorer', () => {
  describe('idf', () => {
    const scorer = new BM25Scorer()

    it('returns high IDF for rare terms', () => {
      // Term appears in 1 out of 1000 documents
      const idf = scorer.idf(1, 1000)
      expect(idf).toBeGreaterThan(5)
    })

    it('returns low IDF for common terms', () => {
      // Term appears in 900 out of 1000 documents
      const idf = scorer.idf(900, 1000)
      expect(idf).toBeLessThan(1)
    })

    it('returns zero IDF for terms in all documents', () => {
      // Term appears in all documents
      const idf = scorer.idf(1000, 1000)
      expect(idf).toBeCloseTo(0, 1)
    })

    it('handles zero document frequency', () => {
      const idf = scorer.idf(0, 1000)
      expect(idf).toBeGreaterThan(0)
    })
  })

  describe('termScore', () => {
    const scorer = new BM25Scorer()

    it('increases with term frequency', () => {
      const idf = 5 // High IDF for a rare term
      const docLength = 100
      const avgDocLength = 100

      const score1 = scorer.termScore(1, docLength, avgDocLength, idf)
      const score2 = scorer.termScore(2, docLength, avgDocLength, idf)
      const score3 = scorer.termScore(5, docLength, avgDocLength, idf)

      expect(score2).toBeGreaterThan(score1)
      expect(score3).toBeGreaterThan(score2)
    })

    it('saturates at high term frequencies', () => {
      const idf = 5
      const docLength = 100
      const avgDocLength = 100

      const score10 = scorer.termScore(10, docLength, avgDocLength, idf)
      const score100 = scorer.termScore(100, docLength, avgDocLength, idf)

      // Increase should be much smaller at high TF
      const ratio = (score100 - score10) / score10
      expect(ratio).toBeLessThan(1)
    })

    it('penalizes longer documents', () => {
      const idf = 5
      const tf = 3
      const avgDocLength = 100

      const scoreShort = scorer.termScore(tf, 50, avgDocLength, idf)
      const scoreLong = scorer.termScore(tf, 200, avgDocLength, idf)

      expect(scoreShort).toBeGreaterThan(scoreLong)
    })

    it('scales with IDF', () => {
      const docLength = 100
      const avgDocLength = 100
      const tf = 3

      const scoreHighIdf = scorer.termScore(tf, docLength, avgDocLength, 5)
      const scoreLowIdf = scorer.termScore(tf, docLength, avgDocLength, 1)

      expect(scoreHighIdf).toBeGreaterThan(scoreLowIdf)
    })
  })

  describe('score', () => {
    const scorer = new BM25Scorer()

    it('combines multiple term scores', () => {
      const termFreqs = new Map([
        ['term1', 2],
        ['term2', 1],
      ])
      const termIdfs = new Map([
        ['term1', 3],
        ['term2', 5],
      ])
      const docLength = 100
      const avgDocLength = 100

      const totalScore = scorer.score(termFreqs, docLength, avgDocLength, termIdfs)

      const score1 = scorer.termScore(2, docLength, avgDocLength, 3)
      const score2 = scorer.termScore(1, docLength, avgDocLength, 5)

      expect(totalScore).toBeCloseTo(score1 + score2)
    })

    it('returns zero for empty terms', () => {
      const score = scorer.score(new Map(), 100, 100, new Map())
      expect(score).toBe(0)
    })
  })

  describe('scoreQuery', () => {
    it('ranks documents by relevance', () => {
      const scorer = new BM25Scorer()

      // Mock corpus
      const corpusStats: CorpusStats = {
        documentCount: 3,
        avgDocLength: 100,
        documentFrequency: new Map([
          ['search', 2],
          ['engine', 1],
          ['database', 2],
        ]),
      }

      // Mock postings
      const postings: Record<string, Posting[]> = {
        'search': [
          { docId: 'doc1', field: 'title', frequency: 3, positions: [] },
          { docId: 'doc2', field: 'title', frequency: 1, positions: [] },
        ],
        'engine': [
          { docId: 'doc1', field: 'title', frequency: 2, positions: [] },
        ],
        'database': [
          { docId: 'doc2', field: 'title', frequency: 1, positions: [] },
          { docId: 'doc3', field: 'title', frequency: 2, positions: [] },
        ],
      }

      // Mock document lengths
      const docLengths: Record<string, number> = {
        'doc1': 100,
        'doc2': 100,
        'doc3': 100,
      }

      const results = scorer.scoreQuery(
        ['search', 'engine'],
        term => postings[term] ?? [],
        docId => docLengths[docId] ?? 0,
        corpusStats
      )

      // doc1 should rank first (has both 'search' and 'engine')
      expect(results[0].docId).toBe('doc1')
      expect(results[0].matchedTerms).toContain('search')
      expect(results[0].matchedTerms).toContain('engine')

      // doc2 should rank second (has only 'search')
      expect(results.length).toBe(2)
      expect(results[1].docId).toBe('doc2')
    })

    it('returns results sorted by score descending', () => {
      const scorer = new BM25Scorer()

      const corpusStats: CorpusStats = {
        documentCount: 2,
        avgDocLength: 100,
        documentFrequency: new Map([['test', 2]]),
      }

      const postings: Posting[] = [
        { docId: 'high', field: 'title', frequency: 10, positions: [] },
        { docId: 'low', field: 'title', frequency: 1, positions: [] },
      ]

      const docLengths: Record<string, number> = {
        'high': 100,
        'low': 100,
      }

      const results = scorer.scoreQuery(
        ['test'],
        () => postings,
        docId => docLengths[docId] ?? 0,
        corpusStats
      )

      expect(results[0].docId).toBe('high')
      expect(results[0].score).toBeGreaterThan(results[1].score)
    })
  })

  describe('DEFAULT_BM25_CONFIG', () => {
    it('has standard parameters', () => {
      expect(DEFAULT_BM25_CONFIG.k1).toBe(1.2)
      expect(DEFAULT_BM25_CONFIG.b).toBe(0.75)
    })
  })
})

describe('Utility Functions', () => {
  describe('logScaledTf', () => {
    it('returns 0 for zero TF', () => {
      expect(logScaledTf(0)).toBe(0)
    })

    it('returns 1 for TF=1', () => {
      expect(logScaledTf(1)).toBe(1)
    })

    it('grows logarithmically', () => {
      const tf10 = logScaledTf(10)
      const tf100 = logScaledTf(100)
      // Should not be 10x larger
      expect(tf100 / tf10).toBeLessThan(2)
    })
  })

  describe('augmentedTf', () => {
    it('returns 0.5 for zero TF', () => {
      expect(augmentedTf(0, 10)).toBe(0.5)
    })

    it('returns 1.0 for max TF', () => {
      expect(augmentedTf(10, 10)).toBe(1.0)
    })

    it('returns 0.75 for half max TF', () => {
      expect(augmentedTf(5, 10)).toBe(0.75)
    })
  })

  describe('luceneIdf', () => {
    it('returns positive value for any DF', () => {
      expect(luceneIdf(0, 100)).toBeGreaterThan(0)
      expect(luceneIdf(50, 100)).toBeGreaterThan(0)
      expect(luceneIdf(100, 100)).toBeGreaterThan(0)
    })
  })

  describe('tfidf', () => {
    it('multiplies TF and IDF', () => {
      expect(tfidf(3, 2)).toBe(6)
      expect(tfidf(0, 5)).toBe(0)
    })
  })
})

/**
 * BM25 Scoring for Full-Text Search
 *
 * Implements Okapi BM25 ranking function for document scoring.
 * BM25 = sum over query terms of:
 *   IDF(t) * (tf(t,d) * (k1 + 1)) / (tf(t,d) + k1 * (1 - b + b * |d| / avgdl))
 *
 * Where:
 *   - tf(t,d) = term frequency in document
 *   - IDF(t) = inverse document frequency
 *   - |d| = document length
 *   - avgdl = average document length
 *   - k1 = term frequency saturation parameter (default: 1.2)
 *   - b = document length normalization parameter (default: 0.75)
 */

import type { CorpusStats, Posting } from './types'

// =============================================================================
// BM25 Parameters
// =============================================================================

/**
 * BM25 configuration
 */
export interface BM25Config {
  /** Term frequency saturation (default: 1.2) */
  k1: number
  /** Document length normalization (default: 0.75) */
  b: number
  /** Minimum IDF value to avoid negative scores (default: 0) */
  minIdf: number
}

/**
 * Default BM25 configuration
 */
export const DEFAULT_BM25_CONFIG: BM25Config = {
  k1: 1.2,
  b: 0.75,
  minIdf: 0,
}

// =============================================================================
// BM25 Scorer
// =============================================================================

/**
 * BM25 scorer for document ranking
 */
export class BM25Scorer {
  private config: BM25Config

  constructor(config: Partial<BM25Config> = {}) {
    this.config = { ...DEFAULT_BM25_CONFIG, ...config }
  }

  /**
   * Calculate IDF (Inverse Document Frequency) for a term
   *
   * IDF(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
   *
   * @param documentFrequency - Number of documents containing the term
   * @param totalDocuments - Total number of documents
   * @returns IDF score
   */
  idf(documentFrequency: number, totalDocuments: number): number {
    const n = totalDocuments
    const df = documentFrequency

    // Standard BM25 IDF formula
    const idf = Math.log((n - df + 0.5) / (df + 0.5) + 1)

    // Ensure non-negative
    return Math.max(this.config.minIdf, idf)
  }

  /**
   * Calculate BM25 score for a single term in a document
   *
   * @param termFrequency - Frequency of term in document
   * @param documentLength - Length of document (word count)
   * @param avgDocLength - Average document length
   * @param idf - Pre-computed IDF for the term
   * @returns Term score
   */
  termScore(
    termFrequency: number,
    documentLength: number,
    avgDocLength: number,
    idf: number
  ): number {
    const { k1, b } = this.config
    const tf = termFrequency

    // BM25 term weight
    const numerator = tf * (k1 + 1)
    const denominator = tf + k1 * (1 - b + b * (documentLength / avgDocLength))

    return idf * (numerator / denominator)
  }

  /**
   * Calculate total BM25 score for a document given query terms
   *
   * @param termFrequencies - Map of term -> frequency in document
   * @param documentLength - Length of document
   * @param avgDocLength - Average document length
   * @param termIdfs - Map of term -> IDF
   * @returns Total BM25 score
   */
  score(
    termFrequencies: Map<string, number>,
    documentLength: number,
    avgDocLength: number,
    termIdfs: Map<string, number>
  ): number {
    let total = 0

    for (const [term, tf] of termFrequencies) {
      const idf = termIdfs.get(term) ?? 0
      total += this.termScore(tf, documentLength, avgDocLength, idf)
    }

    return total
  }

  /**
   * Score documents for a query
   *
   * @param queryTerms - Normalized query terms
   * @param getPostings - Function to get postings for a term
   * @param getDocLength - Function to get document length
   * @param corpusStats - Corpus statistics
   * @returns Sorted array of (docId, score) pairs
   */
  scoreQuery(
    queryTerms: string[],
    getPostings: (term: string) => Posting[],
    getDocLength: (docId: string) => number,
    corpusStats: CorpusStats
  ): Array<{ docId: string; score: number; matchedTerms: string[] }> {
    // Pre-compute IDFs for query terms
    const termIdfs = new Map<string, number>()
    for (const term of queryTerms) {
      const df = corpusStats.documentFrequency.get(term) ?? 0
      termIdfs.set(term, this.idf(df, corpusStats.documentCount))
    }

    // Aggregate postings by document
    const docScores = new Map<
      string,
      { termFreqs: Map<string, number>; matchedTerms: Set<string> }
    >()

    for (const term of queryTerms) {
      const postings = getPostings(term)
      const idf = termIdfs.get(term) ?? 0

      // Skip terms with zero IDF (appear in all documents)
      if (idf === 0) continue

      for (const posting of postings) {
        let docData = docScores.get(posting.docId)
        if (!docData) {
          docData = { termFreqs: new Map(), matchedTerms: new Set() }
          docScores.set(posting.docId, docData)
        }

        // Aggregate term frequency across fields
        const currentTf = docData.termFreqs.get(term) ?? 0
        docData.termFreqs.set(term, currentTf + posting.frequency)
        docData.matchedTerms.add(term)
      }
    }

    // Calculate final scores
    const results: Array<{ docId: string; score: number; matchedTerms: string[] }> = []

    for (const [docId, { termFreqs, matchedTerms }] of docScores) {
      const docLength = getDocLength(docId)
      const score = this.score(
        termFreqs,
        docLength,
        corpusStats.avgDocLength,
        termIdfs
      )

      results.push({
        docId,
        score,
        matchedTerms: Array.from(matchedTerms),
      })
    }

    // Sort by score descending
    results.sort((a, b) => b.score - a.score)

    return results
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Calculate term frequency with logarithmic scaling
 *
 * @param rawTf - Raw term frequency
 * @returns Scaled term frequency
 */
export function logScaledTf(rawTf: number): number {
  if (rawTf === 0) return 0
  return 1 + Math.log(rawTf)
}

/**
 * Calculate augmented term frequency
 * Prevents bias toward longer documents
 *
 * @param rawTf - Raw term frequency
 * @param maxTf - Maximum term frequency in document
 * @returns Augmented term frequency
 */
export function augmentedTf(rawTf: number, maxTf: number): number {
  if (maxTf === 0) return 0
  return 0.5 + 0.5 * (rawTf / maxTf)
}

/**
 * Calculate Lucene-style IDF
 *
 * @param documentFrequency - Number of documents containing the term
 * @param totalDocuments - Total number of documents
 * @returns IDF score
 */
export function luceneIdf(documentFrequency: number, totalDocuments: number): number {
  return 1 + Math.log(totalDocuments / (documentFrequency + 1))
}

/**
 * Calculate TF-IDF score
 *
 * @param tf - Term frequency
 * @param idf - Inverse document frequency
 * @returns TF-IDF score
 */
export function tfidf(tf: number, idf: number): number {
  return tf * idf
}

/**
 * Full-Text Search Execution
 *
 * Provides the main search interface that combines tokenization,
 * inverted index lookup, and BM25 scoring.
 */

import type { StorageBackend } from '../../types/storage'
import type { IndexDefinition, FTSSearchOptions, FTSSearchResult, IndexStats } from '../types'
import type { TokenizerOptions } from './types'
import { tokenizeQuery } from './tokenizer'
import { InvertedIndex } from './inverted-index'
import { BM25Scorer } from './scoring'

// =============================================================================
// FTS Index
// =============================================================================

/**
 * Full-text search index
 */
export class FTSIndex {
  private invertedIndex: InvertedIndex
  private scorer: BM25Scorer
  private tokenizerOptions: TokenizerOptions
  private loaded: boolean = false

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = ''
  ) {
    this.invertedIndex = new InvertedIndex(storage, namespace, definition, basePath)
    this.scorer = new BM25Scorer()
    this.tokenizerOptions = {
      language: definition.ftsOptions?.language ?? 'en',
      minWordLength: definition.ftsOptions?.minWordLength ?? 2,
      maxWordLength: definition.ftsOptions?.maxWordLength ?? 50,
      stem: true,
    }
  }

  // ===========================================================================
  // Loading and Saving
  // ===========================================================================

  /**
   * Load the index from storage
   */
  async load(): Promise<void> {
    if (this.loaded) return
    await this.invertedIndex.load()
    this.loaded = true
  }

  /**
   * Save the index to storage
   */
  async save(): Promise<void> {
    await this.invertedIndex.save()
  }

  /**
   * Check if index is ready
   */
  get ready(): boolean {
    return this.loaded && this.invertedIndex.ready
  }

  // ===========================================================================
  // Search Operations
  // ===========================================================================

  /**
   * Execute a full-text search
   *
   * @param query - Search query string
   * @param options - Search options
   * @returns Ranked search results
   */
  search(query: string, options: FTSSearchOptions = {}): FTSSearchResult[] {
    const { limit = 100, minScore = 0 } = options

    // Tokenize query
    const queryTerms = tokenizeQuery(query, this.tokenizerOptions)

    if (queryTerms.length === 0) {
      return []
    }

    // Get corpus stats
    const corpusStats = this.invertedIndex.getCorpusStats()

    if (corpusStats.documentCount === 0) {
      return []
    }

    // Score documents
    const scored = this.scorer.scoreQuery(
      queryTerms,
      term => this.invertedIndex.getPostings(term),
      docId => {
        const stats = this.invertedIndex.getDocumentStats(docId)
        return stats?.totalLength ?? 0
      },
      corpusStats
    )

    // Filter by minimum score and limit
    const filtered = scored
      .filter(r => r.score >= minScore)
      .slice(0, limit)

    // Build results
    return filtered.map(r => ({
      docId: r.docId,
      score: r.score,
      matchedTokens: r.matchedTerms,
    }))
  }

  /**
   * Search with phrase matching (requires position indexing)
   *
   * @param phrase - Phrase to search for
   * @param options - Search options
   * @returns Results where words appear in sequence
   */
  searchPhrase(phrase: string, options: FTSSearchOptions = {}): FTSSearchResult[] {
    const queryTerms = tokenizeQuery(phrase, this.tokenizerOptions)

    if (queryTerms.length <= 1) {
      // Single word, use regular search
      return this.search(phrase, options)
    }

    // Get candidate documents (must contain all terms)
    const termPostings = queryTerms.map(term => ({
      term,
      postings: this.invertedIndex.getPostings(term),
    }))

    // Find documents that contain all terms
    const candidateDocs = this.findDocumentsWithAllTerms(
      termPostings.map(tp => new Set(tp.postings.map(p => p.docId)))
    )

    // Check phrase positions in each candidate document
    const results: FTSSearchResult[] = []
    const corpusStats = this.invertedIndex.getCorpusStats()

    for (const docId of candidateDocs) {
      const docTermPositions: Map<string, number[]> = new Map()

      for (const { term, postings } of termPostings) {
        const docPostings = postings.filter(p => p.docId === docId)
        for (const posting of docPostings) {
          if (posting.positions.length > 0) {
            const existing = docTermPositions.get(term) ?? []
            existing.push(...posting.positions)
            docTermPositions.set(term, existing)
          }
        }
      }

      // Check for consecutive positions
      if (this.hasConsecutivePositions(queryTerms, docTermPositions)) {
        // Calculate score
        const docStats = this.invertedIndex.getDocumentStats(docId)
        const termFreqs = new Map<string, number>()
        for (const { term, postings } of termPostings) {
          const freq = postings
            .filter(p => p.docId === docId)
            .reduce((sum, p) => sum + p.frequency, 0)
          termFreqs.set(term, freq)
        }

        // Compute IDFs
        const termIdfs = new Map<string, number>()
        for (const term of queryTerms) {
          const df = corpusStats.documentFrequency.get(term) ?? 0
          termIdfs.set(term, this.scorer.idf(df, corpusStats.documentCount))
        }

        const score = this.scorer.score(
          termFreqs,
          docStats?.totalLength ?? 0,
          corpusStats.avgDocLength,
          termIdfs
        )

        results.push({
          docId,
          score: score * 1.5, // Boost phrase matches
          matchedTokens: queryTerms,
        })
      }
    }

    // Sort by score
    results.sort((a, b) => b.score - a.score)

    return results.slice(0, options.limit ?? 100)
  }

  /**
   * Get document frequency for a term
   */
  getDocumentFrequency(term: string): number {
    return this.invertedIndex.getDocumentFrequency(term)
  }

  // ===========================================================================
  // Indexing Operations
  // ===========================================================================

  /**
   * Add a document to the index
   */
  addDocument(docId: string, doc: Record<string, unknown>): void {
    this.invertedIndex.addDocument(docId, doc)
  }

  /**
   * Remove a document from the index
   */
  removeDocument(docId: string): boolean {
    return this.invertedIndex.removeDocument(docId)
  }

  /**
   * Clear the index
   */
  clear(): void {
    this.invertedIndex.clear()
  }

  /**
   * Build the index from documents
   */
  async build(
    data: AsyncIterable<{ docId: string; doc: Record<string, unknown> }>,
    options?: { onProgress?: (processed: number) => void }
  ): Promise<void> {
    await this.invertedIndex.build(data, options)
  }

  /**
   * Build from an array of documents (for testing)
   */
  buildFromArray(
    data: Array<{ docId: string; doc: Record<string, unknown> }>
  ): void {
    this.invertedIndex.buildFromArray(data)
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    const stats = this.invertedIndex.getStats()
    return {
      entryCount: stats.totalPostings,
      sizeBytes: 0, // Would need to estimate from serialized size
      vocabularySize: stats.vocabularySize,
      avgDocLength: stats.avgDocLength,
    }
  }

  /**
   * Get document count
   */
  get documentCount(): number {
    return this.invertedIndex.documentCount
  }

  /**
   * Get vocabulary size
   */
  get vocabularySize(): number {
    return this.invertedIndex.vocabularySize
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private findDocumentsWithAllTerms(docIdSets: Set<string>[]): string[] {
    if (docIdSets.length === 0) return []
    if (docIdSets.length === 1) return Array.from(docIdSets[0]!)

    // Start with smallest set for efficiency
    const sorted = [...docIdSets].sort((a, b) => a.size - b.size)
    const smallest = sorted[0]!  // sorted has at least 2 elements due to earlier length check
    const rest = sorted.slice(1)

    const result: string[] = []
    for (const docId of smallest) {
      if (rest.every(set => set.has(docId))) {
        result.push(docId)
      }
    }

    return result
  }

  private hasConsecutivePositions(
    terms: string[],
    termPositions: Map<string, number[]>
  ): boolean {
    // Get positions for first term
    const firstTerm = terms[0]
    if (!firstTerm) return false
    const firstPositions = termPositions.get(firstTerm)
    if (!firstPositions || firstPositions.length === 0) {
      return false
    }

    // Check if subsequent terms appear at consecutive positions
    for (const startPos of firstPositions) {
      let found = true
      for (let i = 1; i < terms.length; i++) {
        const term = terms[i]
        if (!term) continue
        const positions = termPositions.get(term)
        if (!positions || !positions.includes(startPos + i)) {
          found = false
          break
        }
      }
      if (found) {
        return true
      }
    }

    return false
  }
}

// =============================================================================
// Builder Utility
// =============================================================================

/**
 * Build an FTS index from Parquet data
 */
export async function buildFTSIndex(
  storage: StorageBackend,
  namespace: string,
  definition: IndexDefinition,
  dataPath: string,
  reader: {
    readAll: <T>(path: string) => Promise<T[]>
  },
  basePath?: string
): Promise<FTSIndex> {
  const index = new FTSIndex(storage, namespace, definition, basePath)

  const rows = await reader.readAll<Record<string, unknown>>(dataPath)

  const entries = rows.map((doc, i) => ({
    docId: (doc.$id as string) || `row_${i}`,
    doc,
  }))

  index.buildFromArray(entries)
  await index.save()

  return index
}

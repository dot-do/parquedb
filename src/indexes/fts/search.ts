/**
 * Full-Text Search Execution
 *
 * Provides the main search interface that combines tokenization,
 * inverted index lookup, and BM25 scoring.
 */

import type { StorageBackend } from '../../types/storage'
import type { IndexDefinition, FTSSearchOptions, FTSSearchResult, IndexStats } from '../types'
import type { TokenizerOptions, CorpusStats } from './types'
import { tokenize, tokenizeQuery } from './tokenizer'
import { InvertedIndex } from './inverted-index'
import { BM25Scorer } from './scoring'
import { parseBooleanQuery, isBooleanQuery, type BooleanQuery, type BooleanClause } from './query-parser'
import {
  DEFAULT_FTS_MIN_WORD_LENGTH,
  DEFAULT_FTS_MAX_WORD_LENGTH,
  DEFAULT_FTS_SEARCH_LIMIT,
} from '../../constants'

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
    storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    basePath: string = ''
  ) {
    this.invertedIndex = new InvertedIndex(storage, namespace, definition, basePath)
    this.scorer = new BM25Scorer()
    this.tokenizerOptions = {
      language: definition.ftsOptions?.language ?? 'en',
      minWordLength: definition.ftsOptions?.minWordLength ?? DEFAULT_FTS_MIN_WORD_LENGTH,
      maxWordLength: definition.ftsOptions?.maxWordLength ?? DEFAULT_FTS_MAX_WORD_LENGTH,
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
    const { limit = DEFAULT_FTS_SEARCH_LIMIT, minScore = 0 } = options

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
   * Execute a boolean search with AND, OR, NOT operators
   *
   * @param query - Boolean query string (e.g., "word1 AND word2 OR word3")
   * @param options - Search options
   * @returns Ranked search results
   */
  searchBoolean(query: string, options: FTSSearchOptions = {}): FTSSearchResult[] {
    const { limit = DEFAULT_FTS_SEARCH_LIMIT, minScore = 0 } = options

    if (!query || query.trim().length === 0) {
      return []
    }

    const parsed = parseBooleanQuery(query)

    if (parsed.clauses.length === 0) {
      return []
    }

    const corpusStats = this.invertedIndex.getCorpusStats()
    if (corpusStats.documentCount === 0) {
      return []
    }

    // Get all document IDs for edge cases (e.g., only NOT clauses)
    const allDocIds = this.getAllDocumentIds()

    // Separate clauses by type
    const includeClauses = parsed.clauses.filter(c => !c.excluded)
    const excludeClauses = parsed.clauses.filter(c => c.excluded)

    // Find matching documents based on include clauses
    let candidateDocs: Set<string>

    if (includeClauses.length === 0) {
      // Only exclusion clauses - start with all documents
      candidateDocs = new Set(allDocIds)
    } else if (parsed.type === 'and') {
      // AND: documents must match ALL include clauses
      candidateDocs = this.findDocsMatchingAllClauses(includeClauses)
    } else {
      // OR: documents match ANY include clause
      candidateDocs = this.findDocsMatchingAnyClauses(includeClauses)
    }

    // Apply exclusions
    for (const excludeClause of excludeClauses) {
      const excludedDocs = this.findDocsMatchingClause(excludeClause)
      for (const docId of excludedDocs) {
        candidateDocs.delete(docId)
      }
    }

    if (candidateDocs.size === 0) {
      return []
    }

    // Score the remaining candidates
    const results = this.scoreDocuments(Array.from(candidateDocs), includeClauses, corpusStats)

    // Filter by minimum score and limit
    const filtered = results
      .filter(r => r.score >= minScore)
      .slice(0, limit)

    return filtered
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

    return results.slice(0, options.limit ?? DEFAULT_FTS_SEARCH_LIMIT)
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

  // ===========================================================================
  // Boolean Search Helpers
  // ===========================================================================

  /**
   * Get all document IDs in the index
   */
  private getAllDocumentIds(): string[] {
    const corpusStats = this.invertedIndex.getCorpusStats()
    // Collect unique doc IDs from all postings
    const docIds = new Set<string>()
    // Iterate through all terms to collect doc IDs
    // Since we don't have direct access to all docs, we collect from posting lists
    for (const term of corpusStats.documentFrequency.keys()) {
      const postings = this.invertedIndex.getPostings(term)
      for (const posting of postings) {
        docIds.add(posting.docId)
      }
    }
    return Array.from(docIds)
  }

  /**
   * Find documents matching a single clause (either by terms or phrase)
   */
  private findDocsMatchingClause(clause: BooleanClause): Set<string> {
    const docIds = new Set<string>()

    if (clause.phrase) {
      // Phrase search - use searchPhrase internally
      const phraseResults = this.searchPhrase(clause.phrase, { limit: 10000 })
      for (const result of phraseResults) {
        docIds.add(result.docId)
      }
    } else if (clause.terms.length > 0) {
      // Term search - check termCombination to determine AND vs OR semantics
      if (clause.terms.length === 1) {
        const postings = this.invertedIndex.getPostings(clause.terms[0]!)
        for (const posting of postings) {
          docIds.add(posting.docId)
        }
      } else if (clause.termCombination === 'or') {
        // OR semantics: match docs with ANY of the terms
        for (const term of clause.terms) {
          const postings = this.invertedIndex.getPostings(term)
          for (const posting of postings) {
            docIds.add(posting.docId)
          }
        }
      } else {
        // Default AND semantics: find docs with ALL terms
        const termDocSets = clause.terms.map(term => {
          const postings = this.invertedIndex.getPostings(term)
          return new Set(postings.map(p => p.docId))
        })
        const matchingDocs = this.findDocumentsWithAllTerms(termDocSets)
        for (const docId of matchingDocs) {
          docIds.add(docId)
        }
      }
    }

    return docIds
  }

  /**
   * Find documents matching ALL clauses (AND semantics)
   */
  private findDocsMatchingAllClauses(clauses: BooleanClause[]): Set<string> {
    if (clauses.length === 0) return new Set()

    // Get doc sets for each clause
    const clauseDocSets = clauses.map(clause => this.findDocsMatchingClause(clause))

    // Return intersection
    const result = new Set<string>(clauseDocSets[0])
    for (let i = 1; i < clauseDocSets.length; i++) {
      const clauseSet = clauseDocSets[i]!
      for (const docId of result) {
        if (!clauseSet.has(docId)) {
          result.delete(docId)
        }
      }
    }

    return result
  }

  /**
   * Find documents matching ANY clause (OR semantics)
   */
  private findDocsMatchingAnyClauses(clauses: BooleanClause[]): Set<string> {
    const result = new Set<string>()

    for (const clause of clauses) {
      const clauseDocs = this.findDocsMatchingClause(clause)
      for (const docId of clauseDocs) {
        result.add(docId)
      }
    }

    return result
  }

  /**
   * Score documents based on matching clauses
   */
  private scoreDocuments(
    docIds: string[],
    clauses: BooleanClause[],
    corpusStats: CorpusStats
  ): FTSSearchResult[] {
    // Collect all terms from clauses for scoring
    const allTerms: string[] = []
    for (const clause of clauses) {
      if (clause.terms.length > 0) {
        allTerms.push(...clause.terms)
      } else if (clause.phrase) {
        // Tokenize phrase for scoring
        const phraseTerms = tokenize(clause.phrase, this.tokenizerOptions)
        allTerms.push(...phraseTerms.map(t => t.term))
      }
    }

    if (allTerms.length === 0) {
      // No terms to score - return with default score
      return docIds.map(docId => ({
        docId,
        score: 1.0,
        matchedTokens: [],
      }))
    }

    // Use BM25 scorer
    const scored = this.scorer.scoreQuery(
      allTerms,
      term => this.invertedIndex.getPostings(term),
      docId => {
        const stats = this.invertedIndex.getDocumentStats(docId)
        return stats?.totalLength ?? 0
      },
      corpusStats
    )

    // Filter to only include our candidate documents
    const docIdSet = new Set(docIds)
    return scored
      .filter(r => docIdSet.has(r.docId))
      .map(r => ({
        docId: r.docId,
        score: r.score,
        matchedTokens: r.matchedTerms,
      }))
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

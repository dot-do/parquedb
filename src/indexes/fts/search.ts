/**
 * Full-Text Search Execution
 *
 * Provides the main search interface that combines tokenization,
 * inverted index lookup, and BM25 scoring.
 */

import type { StorageBackend } from '../../types/storage'
import type { IndexDefinition, FTSSearchOptions, FTSSearchResult, IndexStats } from '../types'
import type { TokenizerOptions, CorpusStats, Posting as _Posting } from './types'
import { tokenize, tokenizeQuery } from './tokenizer'
import { InvertedIndex } from './inverted-index'
import { BM25Scorer } from './scoring'
import { parseBooleanQuery, isBooleanQuery, parseQuery, isAdvancedQuery, type BooleanQuery as _BooleanQuery, type BooleanClause } from './query-parser'
import {
  normalizeFuzzyOptions,
  expandQueryTerms,
  fuzzyScorePenalty,
  type NormalizedFuzzyOptions,
  type FuzzyMatch as _FuzzyMatch,
} from './fuzzy'
import {
  DEFAULT_FTS_MIN_WORD_LENGTH,
  DEFAULT_FTS_MAX_WORD_LENGTH,
  DEFAULT_FTS_SEARCH_LIMIT,
} from '../../constants'
import { generateHighlights } from './highlight'

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
    const { limit = DEFAULT_FTS_SEARCH_LIMIT, minScore = 0, fuzzy } = options

    // Check if query contains boolean operators (AND, OR, NOT, parentheses)
    if (isBooleanQuery(query)) {
      return this.searchBoolean(query, options)
    }

    // Check if query contains phrases or +/- modifiers
    if (isAdvancedQuery(query)) {
      return this.searchAdvanced(query, options)
    }

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

    // Check if fuzzy matching is enabled
    const fuzzyOptions = normalizeFuzzyOptions(fuzzy)

    if (fuzzyOptions.enabled) {
      return this.searchFuzzy(queryTerms, fuzzyOptions, options)
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
   * Execute a fuzzy search with typo tolerance
   *
   * @param queryTerms - Tokenized query terms
   * @param fuzzyOptions - Fuzzy matching options
   * @param searchOptions - Search options
   * @returns Ranked search results with fuzzy matches
   */
  private searchFuzzy(
    queryTerms: string[],
    fuzzyOptions: NormalizedFuzzyOptions,
    searchOptions: FTSSearchOptions = {}
  ): FTSSearchResult[] {
    const { limit = DEFAULT_FTS_SEARCH_LIMIT, minScore = 0 } = searchOptions
    const corpusStats = this.invertedIndex.getCorpusStats()

    // Get vocabulary for fuzzy expansion
    const vocabulary = corpusStats.documentFrequency.keys()

    // Expand query terms with fuzzy matches
    const termExpansions = expandQueryTerms(queryTerms, vocabulary, fuzzyOptions)

    // Collect all documents that match any expanded term
    // Track which terms matched each document and with what penalty
    const docTermMatches = new Map<string, Map<string, { freq: number; penalty: number }>>()

    for (const [originalTerm, matches] of termExpansions) {
      for (const match of matches) {
        const postings = this.invertedIndex.getPostings(match.term)
        const penalty = fuzzyScorePenalty(match.distance, fuzzyOptions.maxDistance)

        for (const posting of postings) {
          let termMap = docTermMatches.get(posting.docId)
          if (!termMap) {
            termMap = new Map()
            docTermMatches.set(posting.docId, termMap)
          }

          // Track the best match for this original term in this document
          const existing = termMap.get(originalTerm)
          if (!existing || existing.penalty < penalty) {
            termMap.set(originalTerm, { freq: posting.frequency, penalty })
          }
        }
      }
    }

    if (docTermMatches.size === 0) {
      return []
    }

    // Score documents with fuzzy penalties applied
    const results: FTSSearchResult[] = []

    for (const [docId, termMap] of docTermMatches) {
      const docStats = this.invertedIndex.getDocumentStats(docId)
      const docLength = docStats?.totalLength ?? 0

      // Calculate BM25 score with fuzzy penalties
      let score = 0
      const matchedTokens: string[] = []

      for (const [originalTerm, { freq, penalty }] of termMap) {
        const df = corpusStats.documentFrequency.get(originalTerm) ?? 0
        const idf = this.scorer.idf(df, corpusStats.documentCount)

        // BM25 term score with fuzzy penalty
        const k1 = 1.2
        const b = 0.75
        const avgDl = corpusStats.avgDocLength
        const tfNorm = (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * (docLength / avgDl)))
        const termScore = idf * tfNorm * penalty

        score += termScore
        matchedTokens.push(originalTerm)
      }

      results.push({
        docId,
        score,
        matchedTokens,
      })
    }

    // Sort by score
    results.sort((a, b) => b.score - a.score)

    // Filter by minimum score and limit
    return results
      .filter(r => r.score >= minScore)
      .slice(0, limit)
  }

  /**
   * Get the vocabulary (all indexed terms)
   *
   * @returns Iterable of all terms in the index
   */
  getVocabulary(): Iterable<string> {
    return this.invertedIndex.getCorpusStats().documentFrequency.keys()
  }

  /**
   * Execute an advanced search with phrases, required/excluded terms
   *
   * @param query - Search query string with phrases and/or modifiers
   * @param options - Search options
   * @returns Ranked search results
   */
  private searchAdvanced(query: string, options: FTSSearchOptions = {}): FTSSearchResult[] {
    const { limit = DEFAULT_FTS_SEARCH_LIMIT, minScore = 0 } = options

    const parsed = parseQuery(query)
    const corpusStats = this.invertedIndex.getCorpusStats()

    if (corpusStats.documentCount === 0) {
      return []
    }

    // Helper to stem a term
    const stemTerm = (term: string): string => {
      const stemmed = tokenizeQuery(term, this.tokenizerOptions)
      return stemmed.length > 0 ? stemmed[0]! : term.toLowerCase()
    }

    // Collect all phrase matches with occurrence counts
    // Map: phrase key -> docId -> occurrence count
    const phraseMatchDocs: Map<string, Set<string>> = new Map()
    const phraseMatchCounts: Map<string, Map<string, number>> = new Map()

    // Process all phrases (regular, required, excluded)
    const allPhrases = [
      ...parsed.phrases.map(p => ({ phrase: p, type: 'normal' as const })),
      ...parsed.requiredPhrases.map(p => ({ phrase: p, type: 'required' as const })),
      ...parsed.excludedPhrases.map(p => ({ phrase: p, type: 'excluded' as const })),
    ]

    for (const { phrase, type } of allPhrases) {
      const key = `${type}:${phrase}`
      // Get phrase occurrence counts for this phrase
      const phraseCounts = this.getPhraseOccurrenceCounts(phrase)
      phraseMatchDocs.set(key, new Set(phraseCounts.keys()))
      phraseMatchCounts.set(key, phraseCounts)
    }

    // Find candidate documents
    let candidateDocs: Set<string>

    // If there are required phrases or terms, start with those
    const hasRequired = parsed.requiredPhrases.length > 0 ||
      parsed.terms.some(t => t.required)
    const hasExcluded = parsed.excludedPhrases.length > 0 ||
      parsed.terms.some(t => t.excluded)

    if (hasRequired) {
      // Must match all required phrases
      candidateDocs = new Set<string>()

      for (const phrase of parsed.requiredPhrases) {
        const matchingDocs = phraseMatchDocs.get(`required:${phrase}`) ?? new Set()
        if (candidateDocs.size === 0 && parsed.requiredPhrases.indexOf(phrase) === 0) {
          // First required phrase - initialize set
          for (const docId of matchingDocs) {
            candidateDocs.add(docId)
          }
        } else {
          // Intersect with existing candidates
          for (const docId of candidateDocs) {
            if (!matchingDocs.has(docId)) {
              candidateDocs.delete(docId)
            }
          }
        }
      }

      // Also handle required terms
      for (const term of parsed.terms.filter(t => t.required)) {
        const stemmed = stemTerm(term.term)
        const postings = this.invertedIndex.getPostings(stemmed)
        const termDocs = new Set(postings.map(p => p.docId))

        if (candidateDocs.size === 0 && parsed.requiredPhrases.length === 0) {
          for (const docId of termDocs) {
            candidateDocs.add(docId)
          }
        } else {
          for (const docId of candidateDocs) {
            if (!termDocs.has(docId)) {
              candidateDocs.delete(docId)
            }
          }
        }
      }
    } else {
      // No required terms - collect all possible matches
      candidateDocs = new Set<string>()

      // Add docs matching regular phrases
      for (const phrase of parsed.phrases) {
        const matchingDocs = phraseMatchDocs.get(`normal:${phrase}`) ?? new Set()
        for (const docId of matchingDocs) {
          candidateDocs.add(docId)
        }
      }

      // Add docs matching regular terms
      for (const term of parsed.terms.filter(t => !t.excluded)) {
        const stemmed = stemTerm(term.term)
        const postings = this.invertedIndex.getPostings(stemmed)
        for (const posting of postings) {
          candidateDocs.add(posting.docId)
        }
      }
    }

    // Apply exclusions
    if (hasExcluded) {
      // Remove docs matching excluded phrases
      for (const phrase of parsed.excludedPhrases) {
        const excludedDocs = phraseMatchDocs.get(`excluded:${phrase}`) ?? new Set()
        for (const docId of excludedDocs) {
          candidateDocs.delete(docId)
        }
      }

      // Remove docs matching excluded terms
      for (const term of parsed.terms.filter(t => t.excluded)) {
        const stemmed = stemTerm(term.term)
        const postings = this.invertedIndex.getPostings(stemmed)
        for (const posting of postings) {
          candidateDocs.delete(posting.docId)
        }
      }
    }

    if (candidateDocs.size === 0) {
      return []
    }

    // Score remaining candidates
    // Phrase matches get a boost scaled by occurrence count
    const results: FTSSearchResult[] = []

    // Collect all stemmed terms for scoring
    const allTerms: string[] = []
    for (const term of parsed.terms.filter(t => !t.excluded)) {
      allTerms.push(stemTerm(term.term))
    }
    for (const phrase of [...parsed.phrases, ...parsed.requiredPhrases]) {
      const phraseTerms = tokenizeQuery(phrase, this.tokenizerOptions)
      allTerms.push(...phraseTerms)
    }

    // Score documents
    for (const docId of candidateDocs) {
      const docStats = this.invertedIndex.getDocumentStats(docId)
      const termFreqs = new Map<string, number>()

      for (const term of allTerms) {
        const postings = this.invertedIndex.getPostings(term)
        const freq = postings
          .filter(p => p.docId === docId)
          .reduce((sum, p) => sum + p.frequency, 0)
        if (freq > 0) {
          termFreqs.set(term, (termFreqs.get(term) ?? 0) + freq)
        }
      }

      // Compute IDFs
      const termIdfs = new Map<string, number>()
      for (const term of allTerms) {
        const df = corpusStats.documentFrequency.get(term) ?? 0
        termIdfs.set(term, this.scorer.idf(df, corpusStats.documentCount))
      }

      let score = this.scorer.score(
        termFreqs,
        docStats?.totalLength ?? 0,
        corpusStats.avgDocLength,
        termIdfs
      )

      // Apply phrase boost for documents that match phrases
      // Boost is scaled by total phrase occurrence count
      let totalPhraseCount = 0
      for (const phrase of [...parsed.phrases, ...parsed.requiredPhrases]) {
        const key = parsed.requiredPhrases.includes(phrase) ? `required:${phrase}` : `normal:${phrase}`
        const counts = phraseMatchCounts.get(key)
        const phraseCount = counts?.get(docId) ?? 0
        totalPhraseCount += phraseCount
      }

      // Apply phrase boost scaled by occurrence count
      // Formula: 1 + 0.5 * log2(1 + phraseCount) gives boost that scales with count
      // e.g., 1 occurrence = 1.5x, 2 = 1.79x, 4 = 2.16x, 8 = 2.5x
      if (totalPhraseCount > 0) {
        const phraseBoost = 1 + 0.5 * Math.log2(1 + totalPhraseCount)
        score *= phraseBoost
      }

      results.push({
        docId,
        score,
        matchedTokens: allTerms.filter(t => termFreqs.has(t)),
      })
    }

    // Sort by score
    results.sort((a, b) => b.score - a.score)

    // Filter by minimum score and limit
    return results
      .filter(r => r.score >= minScore)
      .slice(0, limit)
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

    // Check if all include clauses are required (have + modifier)
    // If so, use AND semantics even if parsed.type is 'or'
    const allRequired = includeClauses.length > 0 && includeClauses.every(c => c.required)

    // Find matching documents based on include clauses
    let candidateDocs: Set<string>

    if (includeClauses.length === 0) {
      // Only exclusion clauses - start with all documents
      candidateDocs = new Set(allDocIds)
    } else if (parsed.type === 'and' || allRequired) {
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

    // Check if position indexing is enabled
    const positionsIndexed = this.definition.ftsOptions?.indexPositions ?? false

    // Get candidate documents (must contain all terms)
    const termPostings = queryTerms.map(term => ({
      term,
      postings: this.invertedIndex.getPostings(term),
    }))

    // Find documents that contain all terms
    const candidateDocs = this.findDocumentsWithAllTerms(
      termPostings.map(tp => new Set(tp.postings.map(p => p.docId)))
    )

    // If positions are not indexed, fall back to term-based search
    // (documents with all terms match, but we can't verify order)
    if (!positionsIndexed) {
      const results: FTSSearchResult[] = []
      const corpusStats = this.invertedIndex.getCorpusStats()

      for (const docId of candidateDocs) {
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
          score,
          matchedTokens: queryTerms,
        })
      }

      results.sort((a, b) => b.score - a.score)
      return results.slice(0, options.limit ?? DEFAULT_FTS_SEARCH_LIMIT)
    }

    // Check phrase positions in each candidate document
    const results: FTSSearchResult[] = []
    const corpusStats = this.invertedIndex.getCorpusStats()

    for (const docId of candidateDocs) {
      // Track positions per field to avoid cross-field false matches
      // Map: field -> term -> positions[]
      const fieldTermPositions: Map<string, Map<string, number[]>> = new Map()

      for (const { term, postings } of termPostings) {
        const docPostings = postings.filter(p => p.docId === docId)
        for (const posting of docPostings) {
          if (posting.positions.length > 0) {
            let fieldMap = fieldTermPositions.get(posting.field)
            if (!fieldMap) {
              fieldMap = new Map()
              fieldTermPositions.set(posting.field, fieldMap)
            }
            const existing = fieldMap.get(term) ?? []
            existing.push(...posting.positions)
            fieldMap.set(term, existing)
          }
        }
      }

      // Count phrase occurrences across all fields
      const phraseCount = this.countConsecutivePositionsInAllFields(queryTerms, fieldTermPositions)

      if (phraseCount > 0) {
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

        // Apply phrase boost scaled by occurrence count
        // Base boost is 1.5, with diminishing returns for additional occurrences
        // Formula: 1 + 0.5 * log2(1 + phraseCount) gives boost that scales with count
        // e.g., 1 occurrence = 1.5x, 2 = 1.79x, 4 = 2.16x, 8 = 2.5x
        const phraseBoost = 1 + 0.5 * Math.log2(1 + phraseCount)

        results.push({
          docId,
          score: score * phraseBoost,
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
  // Highlight Operations
  // ===========================================================================

  /**
   * Add highlights to search results
   *
   * This method enriches search results with highlighted snippets from the
   * original documents. It requires the documents to be provided since
   * the FTS index only stores postings, not original content.
   *
   * @param results - Search results from search()
   * @param documents - Map of docId to document content
   * @param query - Original search query (for term extraction)
   * @param options - Highlight options
   * @returns Search results with highlights field populated
   */
  addHighlights(
    results: FTSSearchResult[],
    documents: Map<string, Record<string, unknown>>,
    query: string,
    options: {
      preTag?: string | undefined
      postTag?: string | undefined
      maxSnippets?: number | undefined
      maxSnippetLength?: number | undefined
    } = {}
  ): FTSSearchResult[] {
    // Get field paths from index definition
    const fields = this.definition.fields.map(f => f.path)

    // Extract query terms (stemmed) for matching
    const queryTerms = tokenizeQuery(query, this.tokenizerOptions)

    return results.map(result => {
      const doc = documents.get(result.docId)
      if (!doc) {
        return result
      }

      const highlights = generateHighlights(doc, fields, queryTerms, {
        preTag: options.preTag,
        postTag: options.postTag,
        maxSnippets: options.maxSnippets,
        maxSnippetLength: options.maxSnippetLength,
        matchStemmed: true,
      })

      return {
        ...result,
        highlights: Object.keys(highlights).length > 0 ? highlights : undefined,
      }
    })
  }

  /**
   * Search with automatic highlight generation
   *
   * Convenience method that combines search() with addHighlights().
   * Requires a document lookup function to retrieve document content.
   *
   * @param query - Search query
   * @param getDocuments - Function to retrieve documents by IDs
   * @param options - Search and highlight options
   * @returns Search results with highlights
   */
  async searchWithHighlights(
    query: string,
    getDocuments: (docIds: string[]) => Promise<Map<string, Record<string, unknown>>>,
    options: FTSSearchOptions & {
      highlightOptions?: {
        preTag?: string | undefined
        postTag?: string | undefined
        maxSnippets?: number | undefined
        maxSnippetLength?: number | undefined
      } | undefined
    } = {}
  ): Promise<FTSSearchResult[]> {
    // Perform the search
    const results = this.search(query, options)

    // If no highlight option or no results, return as-is
    if (!options.highlight || results.length === 0) {
      return results
    }

    // Fetch documents
    const docIds = results.map(r => r.docId)
    const documents = await getDocuments(docIds)

    // Extract highlight options
    const highlightOptions = typeof options.highlight === 'object'
      ? options.highlight
      : options.highlightOptions ?? {}

    // Add highlights
    return this.addHighlights(results, documents, query, highlightOptions)
  }

  /**
   * Get the indexed field paths
   */
  getIndexedFields(): string[] {
    return this.definition.fields.map(f => f.path)
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
    options?: { onProgress?: ((processed: number) => void) | undefined }
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

  /**
   * Get phrase occurrence counts for all documents matching a phrase
   *
   * @param phrase - Phrase to search for
   * @returns Map of docId -> occurrence count
   */
  private getPhraseOccurrenceCounts(phrase: string): Map<string, number> {
    const queryTerms = tokenizeQuery(phrase, this.tokenizerOptions)
    const counts = new Map<string, number>()

    if (queryTerms.length <= 1) {
      // Single word - count is term frequency
      const term = queryTerms[0]
      if (!term) return counts
      const postings = this.invertedIndex.getPostings(term)
      for (const posting of postings) {
        const current = counts.get(posting.docId) ?? 0
        counts.set(posting.docId, current + posting.frequency)
      }
      return counts
    }

    // Check if position indexing is enabled
    const positionsIndexed = this.definition.ftsOptions?.indexPositions ?? false

    // Get candidate documents (must contain all terms)
    const termPostings = queryTerms.map(term => ({
      term,
      postings: this.invertedIndex.getPostings(term),
    }))

    // Find documents that contain all terms
    const candidateDocs = this.findDocumentsWithAllTerms(
      termPostings.map(tp => new Set(tp.postings.map(p => p.docId)))
    )

    if (!positionsIndexed) {
      // Without positions, we can only say the doc matches (count = 1)
      // This is approximate but maintains backward compatibility
      for (const docId of candidateDocs) {
        counts.set(docId, 1)
      }
      return counts
    }

    // Count phrase occurrences in each candidate document
    for (const docId of candidateDocs) {
      // Track positions per field
      const fieldTermPositions: Map<string, Map<string, number[]>> = new Map()

      for (const { term, postings } of termPostings) {
        const docPostings = postings.filter(p => p.docId === docId)
        for (const posting of docPostings) {
          if (posting.positions.length > 0) {
            let fieldMap = fieldTermPositions.get(posting.field)
            if (!fieldMap) {
              fieldMap = new Map()
              fieldTermPositions.set(posting.field, fieldMap)
            }
            const existing = fieldMap.get(term) ?? []
            existing.push(...posting.positions)
            fieldMap.set(term, existing)
          }
        }
      }

      // Count phrase occurrences across all fields
      const phraseCount = this.countConsecutivePositionsInAllFields(queryTerms, fieldTermPositions)
      if (phraseCount > 0) {
        counts.set(docId, phraseCount)
      }
    }

    return counts
  }

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

  /**
   * Check if terms appear consecutively within the same field
   *
   * @param terms - Array of terms to check
   * @param fieldTermPositions - Map of field -> term -> positions
   * @returns true if all terms appear consecutively in at least one field
   */
  /** @internal Check if terms appear consecutively within the same field */
  hasConsecutivePositionsInAnyField(
    terms: string[],
    fieldTermPositions: Map<string, Map<string, number[]>>
  ): boolean {
    // Check each field separately to avoid cross-field false matches
    for (const [_field, termPositions] of fieldTermPositions) {
      if (this.hasConsecutivePositions(terms, termPositions)) {
        return true
      }
    }
    return false
  }

  private hasConsecutivePositions(
    terms: string[],
    termPositions: Map<string, number[]>
  ): boolean {
    return this.countConsecutivePositions(terms, termPositions) > 0
  }

  /**
   * Count how many times terms appear consecutively
   *
   * @param terms - Array of terms to check
   * @param termPositions - Map of term -> positions
   * @returns Number of times the phrase appears consecutively
   */
  private countConsecutivePositions(
    terms: string[],
    termPositions: Map<string, number[]>
  ): number {
    // Get positions for first term
    const firstTerm = terms[0]
    if (!firstTerm) return 0
    const firstPositions = termPositions.get(firstTerm)
    if (!firstPositions || firstPositions.length === 0) {
      return 0
    }

    // Count how many times subsequent terms appear at consecutive positions
    let count = 0
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
        count++
      }
    }

    return count
  }

  /**
   * Count phrase occurrences across all fields
   *
   * @param terms - Array of terms to check
   * @param fieldTermPositions - Map of field -> term -> positions
   * @returns Total count of phrase occurrences across all fields
   */
  private countConsecutivePositionsInAllFields(
    terms: string[],
    fieldTermPositions: Map<string, Map<string, number[]>>
  ): number {
    let totalCount = 0
    for (const [_field, termPositions] of fieldTermPositions) {
      totalCount += this.countConsecutivePositions(terms, termPositions)
    }
    return totalCount
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

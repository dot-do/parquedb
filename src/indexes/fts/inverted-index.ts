/**
 * Inverted Index for Full-Text Search
 *
 * Maps terms to document postings for efficient text search.
 * Stores: term -> [(docId, field, frequency, positions[])]
 */

import type { StorageBackend } from '../../types/storage'
import type { IndexDefinition } from '../types'
import type { Posting, DocumentStats, CorpusStats, TokenizerOptions } from './types'
import { tokenize } from './tokenizer'

// =============================================================================
// Inverted Index
// =============================================================================

/**
 * Inverted index for full-text search
 */
export class InvertedIndex {
  /** Term -> Postings map */
  private index: Map<string, Posting[]> = new Map()
  /** Document stats for scoring */
  private docStats: Map<string, DocumentStats> = new Map()
  /** Corpus statistics */
  private corpusStats: CorpusStats = {
    documentCount: 0,
    avgDocLength: 0,
    documentFrequency: new Map(),
  }
  /** Whether index is loaded */
  private loaded: boolean = false
  /** Tokenizer options */
  private tokenizerOptions: TokenizerOptions

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = ''
  ) {
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

    const path = this.getIndexPath()
    const exists = await this.storage.exists(path)

    if (!exists) {
      this.loaded = true
      return
    }

    try {
      const data = await this.storage.read(path)
      this.deserialize(data)
      this.loaded = true
    } catch (error) {
      // Log error for debugging - FTS index load failures should be visible
      console.error(`[FTS] Failed to load index from ${path}:`, error)
      this.index.clear()
      this.docStats.clear()
      this.loaded = true
    }
  }

  /**
   * Save the index to storage
   */
  async save(): Promise<void> {
    const path = this.getIndexPath()
    const data = this.serialize()
    await this.storage.write(path, data)
  }

  /**
   * Check if index is ready
   */
  get ready(): boolean {
    return this.loaded
  }

  // ===========================================================================
  // Indexing Operations
  // ===========================================================================

  /**
   * Add a document to the index
   *
   * @param docId - Document ID
   * @param doc - Document data
   */
  addDocument(docId: string, doc: Record<string, unknown>): void {
    const fieldLengths = new Map<string, number>()
    let totalLength = 0

    // Track terms we've already counted for DF in this document
    const termsInDoc = new Set<string>()

    // Index each field
    for (const fieldDef of this.definition.fields) {
      const fieldValue = this.getNestedValue(doc, fieldDef.path)
      if (typeof fieldValue !== 'string') continue

      const tokens = tokenize(fieldValue, this.tokenizerOptions)
      fieldLengths.set(fieldDef.path, tokens.length)
      totalLength += tokens.length

      // Count term frequencies
      const termFreqs = new Map<string, { freq: number; positions: number[] }>()
      for (const token of tokens) {
        const existing = termFreqs.get(token.term)
        if (existing) {
          existing.freq++
          existing.positions.push(token.position)
        } else {
          termFreqs.set(token.term, { freq: 1, positions: [token.position] })
        }
      }

      // Add postings
      for (const [term, { freq, positions }] of termFreqs) {
        const posting: Posting = {
          docId,
          field: fieldDef.path,
          frequency: freq,
          positions: this.definition.ftsOptions?.indexPositions ? positions : [],
        }

        let postings = this.index.get(term)
        if (!postings) {
          postings = []
          this.index.set(term, postings)
        }
        postings.push(posting)

        // Update document frequency (only once per document)
        if (!termsInDoc.has(term)) {
          termsInDoc.add(term)
          const df = this.corpusStats.documentFrequency.get(term) ?? 0
          this.corpusStats.documentFrequency.set(term, df + 1)
        }
      }
    }

    // Store document stats
    this.docStats.set(docId, { docId, fieldLengths, totalLength })

    // Update corpus stats
    this.corpusStats.documentCount++
    this.updateAvgDocLength()
  }

  /**
   * Remove a document from the index
   *
   * @param docId - Document ID
   */
  removeDocument(docId: string): boolean {
    const stats = this.docStats.get(docId)
    if (!stats) return false

    // Remove postings for this document
    const termsToCheck: string[] = []
    for (const [term, postings] of this.index) {
      const filtered = postings.filter(p => p.docId !== docId)
      if (filtered.length === 0) {
        this.index.delete(term)
      } else {
        this.index.set(term, filtered)
      }

      if (filtered.length < postings.length) {
        termsToCheck.push(term)
      }
    }

    // Update document frequencies
    for (const term of termsToCheck) {
      const df = this.corpusStats.documentFrequency.get(term) ?? 0
      if (df > 0) {
        this.corpusStats.documentFrequency.set(term, df - 1)
      }
    }

    // Remove document stats
    this.docStats.delete(docId)

    // Update corpus stats
    this.corpusStats.documentCount--
    this.updateAvgDocLength()

    return true
  }

  /**
   * Clear the index
   */
  clear(): void {
    this.index.clear()
    this.docStats.clear()
    this.corpusStats = {
      documentCount: 0,
      avgDocLength: 0,
      documentFrequency: new Map(),
    }
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  /**
   * Get postings for a term
   *
   * @param term - Normalized term
   * @returns Array of postings
   */
  getPostings(term: string): Posting[] {
    return this.index.get(term) ?? []
  }

  /**
   * Get document stats
   *
   * @param docId - Document ID
   * @returns Document stats or null
   */
  getDocumentStats(docId: string): DocumentStats | null {
    return this.docStats.get(docId) ?? null
  }

  /**
   * Get corpus statistics
   */
  getCorpusStats(): CorpusStats {
    return this.corpusStats
  }

  /**
   * Get document frequency for a term
   *
   * @param term - Normalized term
   * @returns Number of documents containing the term
   */
  getDocumentFrequency(term: string): number {
    return this.corpusStats.documentFrequency.get(term) ?? 0
  }

  /**
   * Get vocabulary size
   */
  get vocabularySize(): number {
    return this.index.size
  }

  /**
   * Get total document count
   */
  get documentCount(): number {
    return this.corpusStats.documentCount
  }

  /**
   * Get average document length
   */
  get avgDocLength(): number {
    return this.corpusStats.avgDocLength
  }

  // ===========================================================================
  // Build Operations
  // ===========================================================================

  /**
   * Build the index from documents
   *
   * @param data - Iterable of documents
   * @param options - Build options
   */
  async build(
    data: AsyncIterable<{ docId: string; doc: Record<string, unknown> }>,
    options?: { onProgress?: (processed: number) => void }
  ): Promise<void> {
    this.clear()

    let processed = 0
    for await (const { docId, doc } of data) {
      this.addDocument(docId, doc)

      processed++
      if (options?.onProgress && processed % 10000 === 0) {
        options.onProgress(processed)
      }
    }
  }

  /**
   * Build from an array of documents (for testing)
   */
  buildFromArray(
    data: Array<{ docId: string; doc: Record<string, unknown> }>
  ): void {
    this.clear()

    for (const { docId, doc } of data) {
      this.addDocument(docId, doc)
    }
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): {
    documentCount: number
    vocabularySize: number
    avgDocLength: number
    totalPostings: number
  } {
    let totalPostings = 0
    for (const postings of this.index.values()) {
      totalPostings += postings.length
    }

    return {
      documentCount: this.corpusStats.documentCount,
      vocabularySize: this.index.size,
      avgDocLength: this.corpusStats.avgDocLength,
      totalPostings,
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/fts/${this.namespace}/inverted.idx`
  }

  private getNestedValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current === null || current === undefined) return undefined
      if (typeof current !== 'object') return undefined
      current = (current as Record<string, unknown>)[part]
    }

    return current
  }

  private updateAvgDocLength(): void {
    if (this.docStats.size === 0) {
      this.corpusStats.avgDocLength = 0
      return
    }

    let totalLength = 0
    for (const stats of this.docStats.values()) {
      totalLength += stats.totalLength
    }

    this.corpusStats.avgDocLength = totalLength / this.docStats.size
  }

  // ===========================================================================
  // Serialization
  // ===========================================================================

  private serialize(): Uint8Array {
    const data = {
      version: 1,
      index: Array.from(this.index.entries()),
      docStats: Array.from(this.docStats.entries()).map(([id, stats]) => ({
        docId: id,
        fieldLengths: Array.from(stats.fieldLengths.entries()),
        totalLength: stats.totalLength,
      })),
      corpusStats: {
        documentCount: this.corpusStats.documentCount,
        avgDocLength: this.corpusStats.avgDocLength,
        documentFrequency: Array.from(this.corpusStats.documentFrequency.entries()),
      },
    }

    return new TextEncoder().encode(JSON.stringify(data))
  }

  private deserialize(data: Uint8Array): void {
    const json = new TextDecoder().decode(data)
    const parsed = JSON.parse(json) as {
      version: number
      index: Array<[string, Posting[]]>
      docStats: Array<{
        docId: string
        fieldLengths: Array<[string, number]>
        totalLength: number
      }>
      corpusStats: {
        documentCount: number
        avgDocLength: number
        documentFrequency: Array<[string, number]>
      }
    }

    if (parsed.version !== 1) {
      throw new Error(`Unsupported index version: ${parsed.version}`)
    }

    this.index = new Map(parsed.index)
    this.docStats = new Map(
      parsed.docStats.map(s => [
        s.docId,
        {
          docId: s.docId,
          fieldLengths: new Map(s.fieldLengths),
          totalLength: s.totalLength,
        },
      ])
    )
    this.corpusStats = {
      documentCount: parsed.corpusStats.documentCount,
      avgDocLength: parsed.corpusStats.avgDocLength,
      documentFrequency: new Map(parsed.corpusStats.documentFrequency),
    }
  }
}

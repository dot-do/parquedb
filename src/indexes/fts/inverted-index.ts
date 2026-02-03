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
import { logger } from '../../utils/logger'
import { DEFAULT_FTS_MIN_WORD_LENGTH, DEFAULT_FTS_MAX_WORD_LENGTH } from '../../constants'
import { writeVarint, readVarint, varintSize } from '../encoding'

// =============================================================================
// Binary Format Constants
// =============================================================================

/** Magic bytes for FTS index binary format: 'FTSI' */
const FTS_MAGIC = new Uint8Array([0x46, 0x54, 0x53, 0x49])

/** Binary format version (2 = binary, 1 = legacy JSON) */
const FTS_BINARY_VERSION = 2

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
    } catch (error: unknown) {
      // Log error for debugging - FTS index load failures should be visible
      logger.warn(`[FTS] Failed to load index from ${path}, starting fresh`, error)
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
    options?: { onProgress?: ((processed: number) => void) | undefined }
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

  /**
   * Serialize the index to binary format
   *
   * Binary Format (Version 2):
   * Header:
   *   magic[4]          - 'FTSI' (0x46 0x54 0x53 0x49)
   *   version[1]        - 2
   *   documentCount[varint]
   *   avgDocLength[8]   - Float64
   *   vocabSize[varint] - Number of terms in index
   *   dfSize[varint]    - Number of document frequency entries
   *   docStatsSize[varint] - Number of document stats entries
   *
   * Document Frequency Section:
   *   For each term:
   *     termLen[varint] + term[bytes] + df[varint]
   *
   * Inverted Index Section:
   *   For each term:
   *     termLen[varint] + term[bytes]
   *     postingCount[varint]
   *     For each posting:
   *       docIdLen[varint] + docId[bytes]
   *       fieldLen[varint] + field[bytes]
   *       frequency[varint]
   *       positionCount[varint]
   *       positions[varint...]
   *
   * Document Stats Section:
   *   For each doc:
   *     docIdLen[varint] + docId[bytes]
   *     totalLength[varint]
   *     fieldCount[varint]
   *     For each field:
   *       fieldLen[varint] + field[bytes] + length[varint]
   */
  private serialize(): Uint8Array {
    const encoder = new TextEncoder()

    // Calculate total size
    let totalSize = 4 + 1 // magic + version
    totalSize += varintSize(this.corpusStats.documentCount)
    totalSize += 8 // avgDocLength (Float64)
    totalSize += varintSize(this.index.size)
    totalSize += varintSize(this.corpusStats.documentFrequency.size)
    totalSize += varintSize(this.docStats.size)

    // Document frequency section size
    for (const [term, df] of this.corpusStats.documentFrequency) {
      const termBytes = encoder.encode(term)
      totalSize += varintSize(termBytes.length) + termBytes.length + varintSize(df)
    }

    // Inverted index section size
    for (const [term, postings] of this.index) {
      const termBytes = encoder.encode(term)
      totalSize += varintSize(termBytes.length) + termBytes.length
      totalSize += varintSize(postings.length)

      for (const posting of postings) {
        const docIdBytes = encoder.encode(posting.docId)
        const fieldBytes = encoder.encode(posting.field)
        totalSize += varintSize(docIdBytes.length) + docIdBytes.length
        totalSize += varintSize(fieldBytes.length) + fieldBytes.length
        totalSize += varintSize(posting.frequency)
        totalSize += varintSize(posting.positions.length)
        for (const pos of posting.positions) {
          totalSize += varintSize(pos)
        }
      }
    }

    // Document stats section size
    for (const [docId, stats] of this.docStats) {
      const docIdBytes = encoder.encode(docId)
      totalSize += varintSize(docIdBytes.length) + docIdBytes.length
      totalSize += varintSize(stats.totalLength)
      totalSize += varintSize(stats.fieldLengths.size)

      for (const [field, length] of stats.fieldLengths) {
        const fieldBytes = encoder.encode(field)
        totalSize += varintSize(fieldBytes.length) + fieldBytes.length + varintSize(length)
      }
    }

    // Allocate buffer and write
    const buffer = new Uint8Array(totalSize)
    const view = new DataView(buffer.buffer)
    let offset = 0

    // Header
    buffer.set(FTS_MAGIC, offset)
    offset += 4

    buffer[offset] = FTS_BINARY_VERSION
    offset += 1

    offset += writeVarint(buffer, offset, this.corpusStats.documentCount)

    view.setFloat64(offset, this.corpusStats.avgDocLength, false)
    offset += 8

    offset += writeVarint(buffer, offset, this.index.size)
    offset += writeVarint(buffer, offset, this.corpusStats.documentFrequency.size)
    offset += writeVarint(buffer, offset, this.docStats.size)

    // Document frequency section
    for (const [term, df] of this.corpusStats.documentFrequency) {
      const termBytes = encoder.encode(term)
      offset += writeVarint(buffer, offset, termBytes.length)
      buffer.set(termBytes, offset)
      offset += termBytes.length
      offset += writeVarint(buffer, offset, df)
    }

    // Inverted index section
    for (const [term, postings] of this.index) {
      const termBytes = encoder.encode(term)
      offset += writeVarint(buffer, offset, termBytes.length)
      buffer.set(termBytes, offset)
      offset += termBytes.length

      offset += writeVarint(buffer, offset, postings.length)

      for (const posting of postings) {
        const docIdBytes = encoder.encode(posting.docId)
        offset += writeVarint(buffer, offset, docIdBytes.length)
        buffer.set(docIdBytes, offset)
        offset += docIdBytes.length

        const fieldBytes = encoder.encode(posting.field)
        offset += writeVarint(buffer, offset, fieldBytes.length)
        buffer.set(fieldBytes, offset)
        offset += fieldBytes.length

        offset += writeVarint(buffer, offset, posting.frequency)
        offset += writeVarint(buffer, offset, posting.positions.length)

        for (const pos of posting.positions) {
          offset += writeVarint(buffer, offset, pos)
        }
      }
    }

    // Document stats section
    for (const [docId, stats] of this.docStats) {
      const docIdBytes = encoder.encode(docId)
      offset += writeVarint(buffer, offset, docIdBytes.length)
      buffer.set(docIdBytes, offset)
      offset += docIdBytes.length

      offset += writeVarint(buffer, offset, stats.totalLength)
      offset += writeVarint(buffer, offset, stats.fieldLengths.size)

      for (const [field, length] of stats.fieldLengths) {
        const fieldBytes = encoder.encode(field)
        offset += writeVarint(buffer, offset, fieldBytes.length)
        buffer.set(fieldBytes, offset)
        offset += fieldBytes.length
        offset += writeVarint(buffer, offset, length)
      }
    }

    return buffer.slice(0, offset)
  }

  /**
   * Deserialize from binary or legacy JSON format
   */
  private deserialize(data: Uint8Array): void {
    // Check for binary format (magic bytes)
    if (this.isBinaryFormat(data)) {
      this.deserializeBinary(data)
    } else {
      this.deserializeJSON(data)
    }
  }

  /**
   * Check if data is in binary format
   */
  private isBinaryFormat(data: Uint8Array): boolean {
    if (data.length < 4) return false
    return (
      data[0] === FTS_MAGIC[0] &&
      data[1] === FTS_MAGIC[1] &&
      data[2] === FTS_MAGIC[2] &&
      data[3] === FTS_MAGIC[3]
    )
  }

  /**
   * Deserialize from binary format
   */
  private deserializeBinary(data: Uint8Array): void {
    const decoder = new TextDecoder()
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 4 // Skip magic

    const version = data[offset]
    offset += 1

    if (version !== FTS_BINARY_VERSION) {
      throw new Error(`Unsupported binary index version: ${version}`)
    }

    // Read header
    const { value: documentCount, bytesRead: b1 } = readVarint(data, offset)
    offset += b1

    const avgDocLength = view.getFloat64(offset, false)
    offset += 8

    const { value: vocabSize, bytesRead: b2 } = readVarint(data, offset)
    offset += b2

    const { value: dfSize, bytesRead: b3 } = readVarint(data, offset)
    offset += b3

    const { value: docStatsSize, bytesRead: b4 } = readVarint(data, offset)
    offset += b4

    // Read document frequency section
    const documentFrequency = new Map<string, number>()
    for (let i = 0; i < dfSize; i++) {
      const { value: termLen, bytesRead: tb1 } = readVarint(data, offset)
      offset += tb1
      const term = decoder.decode(data.slice(offset, offset + termLen))
      offset += termLen
      const { value: df, bytesRead: tb2 } = readVarint(data, offset)
      offset += tb2
      documentFrequency.set(term, df)
    }

    // Read inverted index section
    this.index.clear()
    for (let i = 0; i < vocabSize; i++) {
      const { value: termLen, bytesRead: tb1 } = readVarint(data, offset)
      offset += tb1
      const term = decoder.decode(data.slice(offset, offset + termLen))
      offset += termLen

      const { value: postingCount, bytesRead: tb2 } = readVarint(data, offset)
      offset += tb2

      const postings: Posting[] = []
      for (let j = 0; j < postingCount; j++) {
        const { value: docIdLen, bytesRead: db1 } = readVarint(data, offset)
        offset += db1
        const docId = decoder.decode(data.slice(offset, offset + docIdLen))
        offset += docIdLen

        const { value: fieldLen, bytesRead: fb1 } = readVarint(data, offset)
        offset += fb1
        const field = decoder.decode(data.slice(offset, offset + fieldLen))
        offset += fieldLen

        const { value: frequency, bytesRead: frb } = readVarint(data, offset)
        offset += frb

        const { value: posCount, bytesRead: pcb } = readVarint(data, offset)
        offset += pcb

        const positions: number[] = []
        for (let k = 0; k < posCount; k++) {
          const { value: pos, bytesRead: pb } = readVarint(data, offset)
          offset += pb
          positions.push(pos)
        }

        postings.push({ docId, field, frequency, positions })
      }

      this.index.set(term, postings)
    }

    // Read document stats section
    this.docStats.clear()
    for (let i = 0; i < docStatsSize; i++) {
      const { value: docIdLen, bytesRead: db1 } = readVarint(data, offset)
      offset += db1
      const docId = decoder.decode(data.slice(offset, offset + docIdLen))
      offset += docIdLen

      const { value: totalLength, bytesRead: tlb } = readVarint(data, offset)
      offset += tlb

      const { value: fieldCount, bytesRead: fcb } = readVarint(data, offset)
      offset += fcb

      const fieldLengths = new Map<string, number>()
      for (let j = 0; j < fieldCount; j++) {
        const { value: fieldLen, bytesRead: flb1 } = readVarint(data, offset)
        offset += flb1
        const field = decoder.decode(data.slice(offset, offset + fieldLen))
        offset += fieldLen
        const { value: length, bytesRead: lb } = readVarint(data, offset)
        offset += lb
        fieldLengths.set(field, length)
      }

      this.docStats.set(docId, { docId, fieldLengths, totalLength })
    }

    // Set corpus stats
    this.corpusStats = {
      documentCount,
      avgDocLength,
      documentFrequency,
    }
  }

  /**
   * Deserialize from legacy JSON format (version 1)
   */
  private deserializeJSON(data: Uint8Array): void {
    const json = new TextDecoder().decode(data)
    let parsed: {
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
    try {
      parsed = JSON.parse(json) as typeof parsed
    } catch {
      throw new Error('Invalid inverted index data: not valid JSON')
    }

    if (parsed.version !== 1) {
      throw new Error(`Unsupported JSON index version: ${parsed.version}`)
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

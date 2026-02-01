/**
 * SST (Sorted String Table) Index for ParqueDB
 *
 * Provides efficient range queries and point lookups using a sorted key structure.
 * Supports $gt, $gte, $lt, $lte operators.
 *
 * Storage format: indexes/secondary/{ns}.{name}.idx.parquet
 * Columns: key (BYTE_ARRAY), docId (BYTE_ARRAY), rowGroup (INT32), rowOffset (INT32)
 */

import type { StorageBackend } from '../../types/storage'
import type {
  IndexDefinition,
  IndexStats,
  IndexLookupResult,
  SSTIndexEntry,
  RangeQuery,
} from '../types'
import { encodeKey, decodeKey, compareKeys, encodeCompositeKey } from './key-encoder'

// =============================================================================
// SST Index
// =============================================================================

/**
 * SST index for sorted range queries
 */
export class SSTIndex {
  /** Sorted entries */
  private entries: SSTIndexEntry[] = []
  /** Whether index is loaded */
  private loaded: boolean = false
  /** Whether entries are sorted */
  private sorted: boolean = true

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = ''
  ) {}

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
      await this.deserialize(data)
      this.loaded = true
    } catch (error) {
      // Index file corrupted or invalid, start fresh
      this.entries = []
      this.loaded = true
    }
  }

  /**
   * Save the index to storage
   */
  async save(): Promise<void> {
    this.ensureSorted()
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
  // Lookup Operations
  // ===========================================================================

  /**
   * Look up entries by exact key value
   *
   * @param value - Value to look up
   * @returns Matching entries
   */
  lookup(value: unknown): IndexLookupResult {
    this.ensureSorted()
    const key = this.encodeValue(value)

    // Binary search for the key
    const startIdx = this.lowerBound(key)
    const endIdx = this.upperBound(key)

    const matches = this.entries.slice(startIdx, endIdx)

    return {
      docIds: matches.map(e => e.docId),
      rowGroups: [...new Set(matches.map(e => e.rowGroup))],
      exact: true,
      entriesScanned: endIdx - startIdx,
    }
  }

  /**
   * Execute a range query
   *
   * @param range - Range query with $gt, $gte, $lt, $lte
   * @returns Matching entries
   */
  range(range: RangeQuery): IndexLookupResult {
    this.ensureSorted()

    let startIdx = 0
    let endIdx = this.entries.length

    // Apply lower bound
    if (range.$gte !== undefined) {
      const key = this.encodeValue(range.$gte)
      startIdx = this.lowerBound(key)
    } else if (range.$gt !== undefined) {
      const key = this.encodeValue(range.$gt)
      startIdx = this.upperBound(key)
    }

    // Apply upper bound
    if (range.$lte !== undefined) {
      const key = this.encodeValue(range.$lte)
      endIdx = this.upperBound(key)
    } else if (range.$lt !== undefined) {
      const key = this.encodeValue(range.$lt)
      endIdx = this.lowerBound(key)
    }

    // Handle invalid ranges
    if (startIdx >= endIdx) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    const matches = this.entries.slice(startIdx, endIdx)

    return {
      docIds: matches.map(e => e.docId),
      rowGroups: [...new Set(matches.map(e => e.rowGroup))],
      exact: true,
      entriesScanned: matches.length,
    }
  }

  /**
   * Get all entries in sorted order
   *
   * @param options - Options for retrieval
   * @returns All entries
   */
  scan(options?: { limit?: number; offset?: number }): IndexLookupResult {
    this.ensureSorted()

    const offset = options?.offset ?? 0
    const limit = options?.limit ?? this.entries.length

    const matches = this.entries.slice(offset, offset + limit)

    return {
      docIds: matches.map(e => e.docId),
      rowGroups: [...new Set(matches.map(e => e.rowGroup))],
      exact: true,
      entriesScanned: matches.length,
    }
  }

  /**
   * Get minimum value in the index
   */
  min(): { value: unknown; docId: string } | null {
    this.ensureSorted()
    if (this.entries.length === 0) return null

    const entry = this.entries[0]
    return {
      value: decodeKey(entry.key),
      docId: entry.docId,
    }
  }

  /**
   * Get maximum value in the index
   */
  max(): { value: unknown; docId: string } | null {
    this.ensureSorted()
    if (this.entries.length === 0) return null

    const entry = this.entries[this.entries.length - 1]
    return {
      value: decodeKey(entry.key),
      docId: entry.docId,
    }
  }

  // ===========================================================================
  // Modification Operations
  // ===========================================================================

  /**
   * Insert an entry into the index
   *
   * @param value - Indexed field value
   * @param docId - Document ID
   * @param rowGroup - Row group number
   * @param rowOffset - Row offset within row group
   */
  insert(value: unknown, docId: string, rowGroup: number, rowOffset: number): void {
    const key = this.encodeValue(value)

    const entry: SSTIndexEntry = {
      key,
      docId,
      rowGroup,
      rowOffset,
    }

    // Try to insert in sorted position if we're still sorted
    if (this.sorted && this.entries.length > 0) {
      const lastKey = this.entries[this.entries.length - 1].key
      if (compareKeys(key, lastKey) >= 0) {
        // Can append in sorted order
        this.entries.push(entry)
        return
      }
      // Need to re-sort later
      this.sorted = false
    }

    this.entries.push(entry)
  }

  /**
   * Remove an entry from the index
   *
   * @param value - Indexed field value
   * @param docId - Document ID
   * @returns true if entry was removed
   */
  remove(value: unknown, docId: string): boolean {
    const key = this.encodeValue(value)

    for (let i = 0; i < this.entries.length; i++) {
      const entry = this.entries[i]
      if (this.keysEqual(entry.key, key) && entry.docId === docId) {
        this.entries.splice(i, 1)
        return true
      }
    }

    return false
  }

  /**
   * Update an entry's location
   *
   * @param value - Indexed field value
   * @param docId - Document ID
   * @param newRowGroup - New row group number
   * @param newRowOffset - New row offset
   */
  update(value: unknown, docId: string, newRowGroup: number, newRowOffset: number): boolean {
    const key = this.encodeValue(value)

    for (const entry of this.entries) {
      if (this.keysEqual(entry.key, key) && entry.docId === docId) {
        entry.rowGroup = newRowGroup
        entry.rowOffset = newRowOffset
        return true
      }
    }

    return false
  }

  /**
   * Clear all entries from the index
   */
  clear(): void {
    this.entries = []
    this.sorted = true
  }

  // ===========================================================================
  // Build Operations
  // ===========================================================================

  /**
   * Build the index from a data iterator
   *
   * @param data - Iterable of documents
   * @param options - Build options
   */
  async build(
    data: AsyncIterable<{ doc: Record<string, unknown>; docId: string; rowGroup: number; rowOffset: number }>,
    options?: { onProgress?: (processed: number) => void }
  ): Promise<void> {
    this.clear()

    let processed = 0
    for await (const { doc, docId, rowGroup, rowOffset } of data) {
      const value = this.extractValue(doc)
      if (value !== undefined) {
        this.insert(value, docId, rowGroup, rowOffset)
      }

      processed++
      if (options?.onProgress && processed % 10000 === 0) {
        options.onProgress(processed)
      }
    }

    this.ensureSorted()
  }

  /**
   * Build from an array of documents (for testing)
   */
  buildFromArray(
    data: Array<{ doc: Record<string, unknown>; docId: string; rowGroup: number; rowOffset: number }>
  ): void {
    this.clear()

    for (const { doc, docId, rowGroup, rowOffset } of data) {
      const value = this.extractValue(doc)
      if (value !== undefined) {
        this.insert(value, docId, rowGroup, rowOffset)
      }
    }

    this.ensureSorted()
  }

  /**
   * Compact the index (remove duplicates, optimize structure)
   */
  compact(): void {
    this.ensureSorted()
    // Currently just ensures sorted order
    // Future: could merge overlapping entries or create summary levels
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    let sizeBytes = 0
    for (const entry of this.entries) {
      sizeBytes += entry.key.length + entry.docId.length + 8
    }

    return {
      entryCount: this.entries.length,
      sizeBytes,
      levels: 1, // Single level for now
    }
  }

  /**
   * Get the number of entries
   */
  get size(): number {
    return this.entries.length
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/secondary/${this.namespace}.${this.definition.name}.idx.parquet`
  }

  private encodeValue(value: unknown): Uint8Array {
    if (this.definition.fields.length > 1) {
      const values = Array.isArray(value) ? value : [value]
      return encodeCompositeKey(values)
    }
    return encodeKey(value)
  }

  private extractValue(doc: Record<string, unknown>): unknown {
    if (this.definition.fields.length === 1) {
      return this.getNestedValue(doc, this.definition.fields[0].path)
    }

    const values: unknown[] = []
    for (const field of this.definition.fields) {
      const value = this.getNestedValue(doc, field.path)
      if (value === undefined && !this.definition.sparse) {
        return undefined
      }
      values.push(value)
    }
    return values
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

  private keysEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false
    }
    return true
  }

  private ensureSorted(): void {
    if (!this.sorted) {
      this.entries.sort((a, b) => compareKeys(a.key, b.key))
      this.sorted = true
    }
  }

  /**
   * Binary search: find index of first entry >= key
   */
  private lowerBound(key: Uint8Array): number {
    let lo = 0
    let hi = this.entries.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (compareKeys(this.entries[mid].key, key) < 0) {
        lo = mid + 1
      } else {
        hi = mid
      }
    }

    return lo
  }

  /**
   * Binary search: find index of first entry > key
   */
  private upperBound(key: Uint8Array): number {
    let lo = 0
    let hi = this.entries.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (compareKeys(this.entries[mid].key, key) <= 0) {
        lo = mid + 1
      } else {
        hi = mid
      }
    }

    return lo
  }

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize the index to bytes
   * Format: [version:u8][entryCount:u32][entries...]
   */
  private serialize(): Uint8Array {
    let totalSize = 1 + 4
    for (const entry of this.entries) {
      totalSize += 2 + entry.key.length + 2 + entry.docId.length + 4 + 4
    }

    const buffer = new ArrayBuffer(totalSize)
    const view = new DataView(buffer)
    const bytes = new Uint8Array(buffer)

    let offset = 0

    view.setUint8(offset, 1) // version
    offset += 1

    view.setUint32(offset, this.entries.length, false)
    offset += 4

    const textEncoder = new TextEncoder()
    for (const entry of this.entries) {
      view.setUint16(offset, entry.key.length, false)
      offset += 2
      bytes.set(entry.key, offset)
      offset += entry.key.length

      const docIdBytes = textEncoder.encode(entry.docId)
      view.setUint16(offset, docIdBytes.length, false)
      offset += 2
      bytes.set(docIdBytes, offset)
      offset += docIdBytes.length

      view.setUint32(offset, entry.rowGroup, false)
      offset += 4
      view.setUint32(offset, entry.rowOffset, false)
      offset += 4
    }

    return bytes
  }

  /**
   * Deserialize the index from bytes
   */
  private async deserialize(data: Uint8Array): Promise<void> {
    this.entries = []
    this.sorted = true

    if (data.length < 5) {
      throw new Error('Invalid index data: too short')
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 0

    const version = view.getUint8(offset)
    offset += 1

    if (version !== 1) {
      throw new Error(`Unsupported index version: ${version}`)
    }

    const entryCount = view.getUint32(offset, false)
    offset += 4

    const textDecoder = new TextDecoder()
    for (let i = 0; i < entryCount; i++) {
      const keyLen = view.getUint16(offset, false)
      offset += 2
      const key = data.slice(offset, offset + keyLen)
      offset += keyLen

      const docIdLen = view.getUint16(offset, false)
      offset += 2
      const docIdBytes = data.slice(offset, offset + docIdLen)
      const docId = textDecoder.decode(docIdBytes)
      offset += docIdLen

      const rowGroup = view.getUint32(offset, false)
      offset += 4
      const rowOffset = view.getUint32(offset, false)
      offset += 4

      this.entries.push({ key, docId, rowGroup, rowOffset })
    }
  }
}

// =============================================================================
// Builder Utility
// =============================================================================

/**
 * Build an SST index from Parquet data
 */
export async function buildSSTIndex(
  storage: StorageBackend,
  namespace: string,
  definition: IndexDefinition,
  dataPath: string,
  reader: {
    readAll: <T>(path: string) => Promise<T[]>
  },
  basePath?: string
): Promise<SSTIndex> {
  const index = new SSTIndex(storage, namespace, definition, basePath)

  const rows = await reader.readAll<Record<string, unknown>>(dataPath)

  const entries = rows.map((doc, i) => ({
    doc,
    docId: (doc.$id as string) || `row_${i}`,
    rowGroup: 0,
    rowOffset: i,
  }))

  index.buildFromArray(entries)
  await index.save()

  return index
}

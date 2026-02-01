/**
 * Hash Index for ParqueDB
 *
 * Provides O(1) point lookups for equality queries.
 * Stores entries in an in-memory hash table backed by Parquet storage.
 *
 * Storage format: indexes/secondary/{ns}.{name}.idx.parquet
 * Columns: key (BYTE_ARRAY), docId (BYTE_ARRAY), rowGroup (INT32), rowOffset (INT32)
 */

import type { StorageBackend } from '../../types/storage'
import type {
  IndexDefinition,
  IndexStats,
  IndexLookupResult,
  HashIndexEntry,
} from '../types'
import { encodeKey, decodeKey, hashKey, encodeCompositeKey } from './key-encoder'

// =============================================================================
// Hash Index
// =============================================================================

/**
 * Hash index for O(1) equality lookups
 */
export class HashIndex {
  /** Hash buckets: hash -> entries */
  private buckets: Map<number, HashIndexEntry[]> = new Map()
  /** Total entry count */
  private entryCount: number = 0
  /** Whether index is loaded */
  private loaded: boolean = false

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
      this.buckets.clear()
      this.entryCount = 0
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
  // Lookup Operations
  // ===========================================================================

  /**
   * Look up entries by key value
   *
   * @param value - Value to look up
   * @returns Matching entries
   */
  lookup(value: unknown): IndexLookupResult {
    const key = this.encodeValue(value)
    const hash = hashKey(key)
    const bucket = this.buckets.get(hash)

    if (!bucket) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    // Filter bucket entries that match the exact key
    const matches: HashIndexEntry[] = []
    for (const entry of bucket) {
      if (this.keysEqual(entry.key, key)) {
        matches.push(entry)
      }
    }

    return {
      docIds: matches.map(e => e.docId),
      rowGroups: [...new Set(matches.map(e => e.rowGroup))],
      exact: true,
      entriesScanned: bucket.length,
    }
  }

  /**
   * Look up entries for multiple values ($in operator)
   *
   * @param values - Values to look up
   * @returns Matching entries
   */
  lookupIn(values: unknown[]): IndexLookupResult {
    const allDocIds: string[] = []
    const allRowGroups = new Set<number>()
    let totalScanned = 0

    for (const value of values) {
      const result = this.lookup(value)
      allDocIds.push(...result.docIds)
      for (const rg of result.rowGroups) {
        allRowGroups.add(rg)
      }
      totalScanned += result.entriesScanned
    }

    return {
      docIds: allDocIds,
      rowGroups: [...allRowGroups],
      exact: true,
      entriesScanned: totalScanned,
    }
  }

  /**
   * Check if a value exists in the index
   *
   * @param value - Value to check
   * @returns true if value exists
   */
  exists(value: unknown): boolean {
    const result = this.lookup(value)
    return result.docIds.length > 0
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
    const hash = hashKey(key)

    let bucket = this.buckets.get(hash)
    if (!bucket) {
      bucket = []
      this.buckets.set(hash, bucket)
    }

    const entry: HashIndexEntry = {
      key,
      docId,
      rowGroup,
      rowOffset,
    }

    bucket.push(entry)
    this.entryCount++
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
    const hash = hashKey(key)

    const bucket = this.buckets.get(hash)
    if (!bucket) return false

    const initialLength = bucket.length
    const filtered = bucket.filter(
      entry => !(this.keysEqual(entry.key, key) && entry.docId === docId)
    )

    if (filtered.length === initialLength) {
      return false
    }

    if (filtered.length === 0) {
      this.buckets.delete(hash)
    } else {
      this.buckets.set(hash, filtered)
    }

    this.entryCount -= (initialLength - filtered.length)
    return true
  }

  /**
   * Update an entry's location (after compaction)
   *
   * @param value - Indexed field value
   * @param docId - Document ID
   * @param newRowGroup - New row group number
   * @param newRowOffset - New row offset
   */
  update(value: unknown, docId: string, newRowGroup: number, newRowOffset: number): boolean {
    const key = this.encodeValue(value)
    const hash = hashKey(key)

    const bucket = this.buckets.get(hash)
    if (!bucket) return false

    for (const entry of bucket) {
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
    this.buckets.clear()
    this.entryCount = 0
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
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    // Estimate memory usage
    let sizeBytes = 0
    for (const [, bucket] of this.buckets) {
      for (const entry of bucket) {
        sizeBytes += entry.key.length + entry.docId.length + 8 // 8 for rowGroup + rowOffset
      }
    }

    return {
      entryCount: this.entryCount,
      sizeBytes,
      uniqueKeys: this.buckets.size,
    }
  }

  /**
   * Get the number of entries
   */
  get size(): number {
    return this.entryCount
  }

  /**
   * Get the number of unique keys
   */
  get uniqueKeyCount(): number {
    return this.buckets.size
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getIndexPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/secondary/${this.namespace}.${this.definition.name}.idx.parquet`
  }

  private encodeValue(value: unknown): Uint8Array {
    // For composite indexes, encode all field values
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

    // Composite key
    const values: unknown[] = []
    for (const field of this.definition.fields) {
      const value = this.getNestedValue(doc, field.path)
      if (value === undefined && !this.definition.sparse) {
        // Skip documents with missing fields unless sparse
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

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize the index to bytes
   * Format: [version:u8][entryCount:u32][entries...]
   * Entry: [keyLen:u16][key:bytes][docIdLen:u16][docId:bytes][rowGroup:u32][rowOffset:u32]
   */
  private serialize(): Uint8Array {
    // Calculate total size
    let totalSize = 1 + 4 // version + entry count
    for (const [, bucket] of this.buckets) {
      for (const entry of bucket) {
        totalSize += 2 + entry.key.length + 2 + entry.docId.length + 4 + 4
      }
    }

    const buffer = new ArrayBuffer(totalSize)
    const view = new DataView(buffer)
    const bytes = new Uint8Array(buffer)

    let offset = 0

    // Version
    view.setUint8(offset, 1)
    offset += 1

    // Entry count
    view.setUint32(offset, this.entryCount, false)
    offset += 4

    // Entries
    const textEncoder = new TextEncoder()
    for (const [, bucket] of this.buckets) {
      for (const entry of bucket) {
        // Key length and key
        view.setUint16(offset, entry.key.length, false)
        offset += 2
        bytes.set(entry.key, offset)
        offset += entry.key.length

        // DocId length and docId
        const docIdBytes = textEncoder.encode(entry.docId)
        view.setUint16(offset, docIdBytes.length, false)
        offset += 2
        bytes.set(docIdBytes, offset)
        offset += docIdBytes.length

        // Row group and offset
        view.setUint32(offset, entry.rowGroup, false)
        offset += 4
        view.setUint32(offset, entry.rowOffset, false)
        offset += 4
      }
    }

    return bytes
  }

  /**
   * Deserialize the index from bytes
   */
  private async deserialize(data: Uint8Array): Promise<void> {
    this.buckets.clear()
    this.entryCount = 0

    if (data.length < 5) {
      throw new Error('Invalid index data: too short')
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 0

    // Version
    const version = view.getUint8(offset)
    offset += 1

    if (version !== 1) {
      throw new Error(`Unsupported index version: ${version}`)
    }

    // Entry count
    const entryCount = view.getUint32(offset, false)
    offset += 4

    // Entries
    const textDecoder = new TextDecoder()
    for (let i = 0; i < entryCount; i++) {
      // Key length and key
      const keyLen = view.getUint16(offset, false)
      offset += 2
      const key = data.slice(offset, offset + keyLen)
      offset += keyLen

      // DocId length and docId
      const docIdLen = view.getUint16(offset, false)
      offset += 2
      const docIdBytes = data.slice(offset, offset + docIdLen)
      const docId = textDecoder.decode(docIdBytes)
      offset += docIdLen

      // Row group and offset
      const rowGroup = view.getUint32(offset, false)
      offset += 4
      const rowOffset = view.getUint32(offset, false)
      offset += 4

      // Add to buckets
      const hash = hashKey(key)
      let bucket = this.buckets.get(hash)
      if (!bucket) {
        bucket = []
        this.buckets.set(hash, bucket)
      }

      bucket.push({ key, docId, rowGroup, rowOffset })
      this.entryCount++
    }
  }
}

// =============================================================================
// Builder Utility
// =============================================================================

/**
 * Build a hash index from Parquet data
 */
export async function buildHashIndex(
  storage: StorageBackend,
  namespace: string,
  definition: IndexDefinition,
  dataPath: string,
  reader: {
    readAll: <T>(path: string) => Promise<T[]>
  },
  basePath?: string
): Promise<HashIndex> {
  const index = new HashIndex(storage, namespace, definition, basePath)

  // Read all data
  const rows = await reader.readAll<Record<string, unknown>>(dataPath)

  // Build index
  const entries = rows.map((doc, i) => ({
    doc,
    docId: (doc.$id as string) || `row_${i}`,
    rowGroup: 0, // Simplified - real implementation would track row groups
    rowOffset: i,
  }))

  index.buildFromArray(entries)
  await index.save()

  return index
}

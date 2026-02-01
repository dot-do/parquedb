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
import { UniqueConstraintError } from '../errors'
import {
  FORMAT_VERSION_1,
  FORMAT_VERSION_2,
  FORMAT_VERSION_3,
  readVarint,
  readCompactHeader,
  readCompactEntry,
  readCompactEntryWithKey,
} from '../encoding'
import { INDEX_PROGRESS_BATCH } from '../../constants'
import { logger } from '../../utils/logger'

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
    } catch (error: unknown) {
      // Index file corrupted or invalid, start fresh
      logger.warn(`Hash index load failed for ${path}, starting fresh`, error)
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

  /**
   * Check if a value is unique (can be inserted without violating unique constraint)
   *
   * @param value - Value to check
   * @param excludeDocId - Optional docId to exclude from check (for updates)
   * @returns true if value is unique (can be inserted)
   */
  checkUnique(value: unknown, excludeDocId?: string): boolean {
    // For sparse unique indexes, null/undefined values are always allowed
    if (this.definition.sparse && (value === null || value === undefined)) {
      return true
    }

    const key = this.encodeValue(value)
    const hash = hashKey(key)
    const bucket = this.buckets.get(hash)

    if (!bucket) {
      return true
    }

    // Check if any entry matches the key (excluding the specified docId)
    for (const entry of bucket) {
      if (this.keysEqual(entry.key, key)) {
        if (excludeDocId && entry.docId === excludeDocId) {
          continue
        }
        return false
      }
    }

    return true
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
   * @throws UniqueConstraintError if unique constraint is violated
   */
  insert(value: unknown, docId: string, rowGroup: number, rowOffset: number): void {
    const key = this.encodeValue(value)
    const hash = hashKey(key)

    // Check unique constraint if enabled
    if (this.definition.unique) {
      // For sparse unique indexes, null/undefined values are allowed to be duplicated
      const isNullish = value === null || value === undefined
      if (!(this.definition.sparse && isNullish)) {
        // Check if value already exists
        const bucket = this.buckets.get(hash)
        if (bucket) {
          for (const entry of bucket) {
            if (this.keysEqual(entry.key, key)) {
              throw new UniqueConstraintError(this.definition.name, value, this.namespace)
            }
          }
        }
      }
    }

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
      if (options?.onProgress && processed % INDEX_PROGRESS_BATCH === 0) {
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
    const firstField = this.definition.fields[0]
    if (this.definition.fields.length === 1 && firstField) {
      return this.getNestedValue(doc, firstField.path)
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

    if (version === FORMAT_VERSION_3) {
      // Compact format v3
      await this.deserializeCompact(data)
    } else if (version === FORMAT_VERSION_1 || version === FORMAT_VERSION_2) {
      // Original or sharded format (same structure)
      await this.deserializeV1V2(data)
    } else {
      throw new Error(`Unsupported index version: ${version}`)
    }
  }

  /**
   * Deserialize v1/v2 format (original with full key)
   */
  private async deserializeV1V2(data: Uint8Array): Promise<void> {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const textDecoder = new TextDecoder()
    let offset = 0

    // Skip version
    offset += 1

    // Entry count
    const entryCount = view.getUint32(offset, false)
    offset += 4

    // Entries
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

  /**
   * Deserialize compact v3 format
   *
   * V3 format has smaller entries:
   * - Header: [version:u8][flags:u8][entryCount:u32]
   * - Entry (with keyHash): [keyHash:u32][rowGroup:u16][rowOffset:varint][docIdLen:u8][docId:bytes]
   * - Entry (no keyHash): [rowGroup:u16][rowOffset:varint][docIdLen:u8][docId:bytes]
   */
  private async deserializeCompact(data: Uint8Array): Promise<void> {
    let offset = 0

    // Read header
    const { header, bytesRead: headerBytes } = readCompactHeader(data, offset)
    offset += headerBytes

    // Read entries
    for (let i = 0; i < header.entryCount; i++) {
      if (header.hasKeyHash) {
        // Entry with key hash (for non-sharded indexes)
        const { entry, bytesRead } = readCompactEntryWithKey(data, offset)
        offset += bytesRead

        // Use the key hash directly as bucket key
        let bucket = this.buckets.get(entry.keyHash)
        if (!bucket) {
          bucket = []
          this.buckets.set(entry.keyHash, bucket)
        }

        // Create entry with empty key (we don't have the full key in compact format)
        bucket.push({
          key: new Uint8Array(0), // Placeholder - compact format doesn't store full key
          docId: entry.docId,
          rowGroup: entry.rowGroup,
          rowOffset: entry.rowOffset,
        })
      } else {
        // Entry without key hash (for sharded indexes, key is implicit)
        const { entry, bytesRead } = readCompactEntry(data, offset)
        offset += bytesRead

        // For sharded indexes, all entries in the shard have the same key
        // Use a single bucket with hash 0
        let bucket = this.buckets.get(0)
        if (!bucket) {
          bucket = []
          this.buckets.set(0, bucket)
        }

        bucket.push({
          key: new Uint8Array(0), // Key is implicit in shard name
          docId: entry.docId,
          rowGroup: entry.rowGroup,
          rowOffset: entry.rowOffset,
        })
      }

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

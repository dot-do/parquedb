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
import type { Result } from '../../types/result'
import { Ok, Err } from '../../types/result'
import { encodeKey, decodeKey, compareKeys, encodeCompositeKey } from './key-encoder'
import { UniqueConstraintError, IndexLoadError } from '../errors'
import {
  FORMAT_VERSION_1,
  FORMAT_VERSION_2,
  FORMAT_VERSION_3,
  readCompactHeader,
  readCompactEntry,
  readCompactEntryWithKey,
} from '../encoding'
import { INDEX_PROGRESS_BATCH } from '../../constants'
import { logger } from '../../utils/logger'

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
   *
   * Returns a Result indicating success or failure:
   * - Ok(void): Index loaded successfully (or didn't exist)
   * - Err(IndexLoadError): Index exists but failed to load (corrupted/invalid)
   *
   * The index is always marked as loaded after this call, with empty data
   * if loading failed. Callers can check the Result to decide whether to
   * rebuild the index.
   */
  async load(): Promise<Result<void, IndexLoadError>> {
    if (this.loaded) return Ok(undefined)

    const path = this.getIndexPath()
    const exists = await this.storage.exists(path)

    if (!exists) {
      this.loaded = true
      return Ok(undefined)
    }

    try {
      const data = await this.storage.read(path)
      await this.deserialize(data)
      this.loaded = true
      return Ok(undefined)
    } catch (error: unknown) {
      // Index file corrupted or invalid - clear and start fresh
      // but return error so caller knows loading failed
      const cause = error instanceof Error ? error : new Error(String(error))
      const loadError = new IndexLoadError(this.definition.name, path, cause)
      logger.warn(`SST index load failed for ${path}, starting fresh`, loadError)
      this.entries = []
      this.loaded = true
      return Err(loadError)
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

    const entry = this.entries[0]!  // length check above ensures entry exists
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

    const entry = this.entries[this.entries.length - 1]!  // length check above ensures entry exists
    return {
      value: decodeKey(entry.key),
      docId: entry.docId,
    }
  }

  // ===========================================================================
  // Modification Operations
  // ===========================================================================

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

    // Check if any entry matches the key (excluding the specified docId)
    for (const entry of this.entries) {
      if (this.keysEqual(entry.key, key)) {
        if (excludeDocId && entry.docId === excludeDocId) {
          continue
        }
        return false
      }
    }

    return true
  }

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

    // Check unique constraint if enabled
    if (this.definition.unique) {
      // For sparse unique indexes, null/undefined values are allowed to be duplicated
      const isNullish = value === null || value === undefined
      if (!(this.definition.sparse && isNullish)) {
        // Check if value already exists
        for (const entry of this.entries) {
          if (this.keysEqual(entry.key, key)) {
            throw new UniqueConstraintError(this.definition.name, value, this.namespace)
          }
        }
      }
    }

    const entry: SSTIndexEntry = {
      key,
      docId,
      rowGroup,
      rowOffset,
    }

    // Try to insert in sorted position if we're still sorted
    if (this.sorted && this.entries.length > 0) {
      const lastEntry = this.entries[this.entries.length - 1]!  // length > 0 ensures valid
      if (compareKeys(key, lastEntry.key) >= 0) {
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
      const entry = this.entries[i]!  // loop bounds ensure valid index
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
      if (options?.onProgress && processed % INDEX_PROGRESS_BATCH === 0) {
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
    const firstField = this.definition.fields[0]
    if (this.definition.fields.length === 1 && firstField) {
      return this.getNestedValue(doc, firstField.path)
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
      const midEntry = this.entries[mid]!  // mid is between lo and hi < length
      if (compareKeys(midEntry.key, key) < 0) {
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
      const midEntry = this.entries[mid]!  // mid is between lo and hi < length
      if (compareKeys(midEntry.key, key) <= 0) {
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

    // Version
    const version = view.getUint8(0)

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

    const entryCount = view.getUint32(offset, false)
    offset += 4

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

        // SST needs keys for sorting - use empty placeholder
        // For compact format, the key is implicit in the shard
        this.entries.push({
          key: new Uint8Array(0), // Placeholder - full key not stored in compact format
          docId: entry.docId,
          rowGroup: entry.rowGroup,
          rowOffset: entry.rowOffset,
        })
      } else {
        // Entry without key hash (for sharded indexes, key is implicit)
        const { entry, bytesRead } = readCompactEntry(data, offset)
        offset += bytesRead

        this.entries.push({
          key: new Uint8Array(0), // Key is implicit in shard name
          docId: entry.docId,
          rowGroup: entry.rowGroup,
          rowOffset: entry.rowOffset,
        })
      }
    }

    // SST entries are already sorted in the file
    this.sorted = true
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

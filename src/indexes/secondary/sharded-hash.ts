/**
 * Sharded Hash Index for ParqueDB
 *
 * Provides O(1) point lookups with lazy shard loading.
 * Each unique value has its own shard file, loaded on-demand.
 *
 * Storage format: indexes/secondary/{name}/
 *   _manifest.json     - Shard metadata and mapping
 *   {value}.shard.idx  - Individual shard files
 */

import type { StorageBackend } from '../../types/storage'
import type {
  IndexDefinition,
  IndexStats,
  IndexLookupResult,
  HashIndexEntry,
} from '../types'
import { encodeKey, hashKey } from './key-encoder'
import {
  FORMAT_VERSION_3,
  readCompactHeader,
  readCompactEntry,
  readCompactEntryWithKey,
} from '../encoding'

// =============================================================================
// Types
// =============================================================================

/**
 * Manifest format for sharded indexes
 */
export interface ShardManifest {
  version: number
  type: 'hash' | 'sst'
  field: string
  sharding: 'by-value' | 'by-range' | 'by-prefix'
  shards: ShardInfo[]
  totalEntries: number
  rowGroups: number
}

/**
 * Information about a single shard
 */
export interface ShardInfo {
  name: string
  path: string
  entryCount: number
  sizeBytes: number
  /** For by-value sharding, the original value */
  value?: unknown
  /** For by-range sharding, the range start */
  rangeStart?: number
  /** For by-range sharding, the range end */
  rangeEnd?: number
  /** For by-prefix sharding, the prefix */
  prefix?: string
}

// =============================================================================
// Sharded Hash Index
// =============================================================================

/**
 * Sharded hash index for O(1) equality lookups with lazy loading
 */
export class ShardedHashIndex {
  /** Shard manifest */
  private manifest: ShardManifest | null = null
  /** Map from value string to shard info */
  private shardMap: Map<string, ShardInfo> = new Map()
  /** Loaded shards: shard name -> entries */
  private loadedShards: Map<string, HashIndexEntry[]> = new Map()
  /** Whether manifest is loaded */
  private loaded: boolean = false

  constructor(
    private storage: StorageBackend,
    readonly namespace: string,
    readonly definition: IndexDefinition,
    private basePath: string = ''
  ) {}

  // ===========================================================================
  // Loading
  // ===========================================================================

  /**
   * Load the manifest (not the shards)
   */
  async load(): Promise<void> {
    if (this.loaded) return

    const manifestPath = this.getManifestPath()
    const exists = await this.storage.exists(manifestPath)

    if (!exists) {
      this.loaded = true
      return
    }

    try {
      const data = await this.storage.read(manifestPath)
      const text = new TextDecoder().decode(data)
      this.manifest = JSON.parse(text)

      // Build lookup map
      for (const shard of this.manifest!.shards) {
        // For by-value sharding, use the value's string representation
        const key = this.getShardKey(shard)
        this.shardMap.set(key, shard)
      }

      this.loaded = true
    } catch (error) {
      // Manifest corrupted or invalid
      this.manifest = null
      this.loaded = true
    }
  }

  /**
   * Load a specific shard
   */
  private async loadShard(shardInfo: ShardInfo): Promise<HashIndexEntry[]> {
    const existing = this.loadedShards.get(shardInfo.name)
    if (existing) return existing

    const shardPath = this.getShardPath(shardInfo.path)
    const exists = await this.storage.exists(shardPath)

    if (!exists) {
      return []
    }

    const data = await this.storage.read(shardPath)
    const entries = this.deserializeShard(data)
    this.loadedShards.set(shardInfo.name, entries)

    return entries
  }

  /**
   * Check if index is ready
   */
  get ready(): boolean {
    return this.loaded
  }

  /**
   * Check if this is a sharded index
   */
  get isSharded(): boolean {
    return this.manifest !== null
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
  async lookup(value: unknown): Promise<IndexLookupResult> {
    if (!this.manifest) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    // Find the shard for this value
    const shardKey = this.valueToShardKey(value)
    const shardInfo = this.shardMap.get(shardKey)

    if (!shardInfo) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    // Load the shard
    const entries = await this.loadShard(shardInfo)

    // For hash shards, all entries in the shard match the value
    // (since we shard by exact value)
    return {
      docIds: entries.map(e => e.docId),
      rowGroups: [...new Set(entries.map(e => e.rowGroup))],
      exact: true,
      entriesScanned: entries.length,
    }
  }

  /**
   * Look up entries for multiple values ($in operator)
   *
   * @param values - Values to look up
   * @returns Matching entries
   */
  async lookupIn(values: unknown[]): Promise<IndexLookupResult> {
    const allDocIds: string[] = []
    const allRowGroups = new Set<number>()
    let totalScanned = 0

    for (const value of values) {
      const result = await this.lookup(value)
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
  async exists(value: unknown): Promise<boolean> {
    if (!this.manifest) return false

    const shardKey = this.valueToShardKey(value)
    return this.shardMap.has(shardKey)
  }

  /**
   * Get all unique values (shard names)
   */
  getUniqueValues(): unknown[] {
    if (!this.manifest) return []
    return this.manifest.shards
      .filter(s => s.value !== undefined)
      .map(s => s.value!)
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get index statistics
   */
  getStats(): IndexStats {
    if (!this.manifest) {
      return {
        entryCount: 0,
        sizeBytes: 0,
        uniqueKeys: 0,
      }
    }

    const totalSize = this.manifest.shards.reduce((sum, s) => sum + s.sizeBytes, 0)

    return {
      entryCount: this.manifest.totalEntries,
      sizeBytes: totalSize,
      uniqueKeys: this.manifest.shards.length,
    }
  }

  /**
   * Get the number of entries
   */
  get size(): number {
    return this.manifest?.totalEntries ?? 0
  }

  /**
   * Get the number of unique keys (shards)
   */
  get uniqueKeyCount(): number {
    return this.manifest?.shards.length ?? 0
  }

  /**
   * Get the number of loaded shards
   */
  get loadedShardCount(): number {
    return this.loadedShards.size
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getManifestPath(): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/secondary/${this.definition.name}/_manifest.json`
  }

  private getShardPath(shardPath: string): string {
    const base = this.basePath ? `${this.basePath}/` : ''
    return `${base}indexes/secondary/${this.definition.name}/${shardPath}`
  }

  private getShardKey(shard: ShardInfo): string {
    // Use the shard name as the lookup key
    return shard.name.toLowerCase()
  }

  private valueToShardKey(value: unknown): string {
    // Convert a query value to a shard key
    return String(value)
      .toLowerCase()
      .replace(/[^a-z0-9-_]/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 50) || 'default'
  }

  /**
   * Deserialize a shard file
   */
  private deserializeShard(data: Uint8Array): HashIndexEntry[] {
    const entries: HashIndexEntry[] = []

    if (data.length < 5) {
      return entries
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 0

    // Version
    const version = view.getUint8(offset)

    // Handle v3 compact format
    if (version === FORMAT_VERSION_3) {
      const { header, bytesRead } = readCompactHeader(data, 0)
      offset = bytesRead

      for (let i = 0; i < header.entryCount; i++) {
        if (header.hasKeyHash) {
          const { entry, bytesRead: entryBytes } = readCompactEntryWithKey(data, offset)
          offset += entryBytes
          entries.push({
            key: new Uint8Array(0), // Key not stored in v3 sharded format
            docId: entry.docId,
            rowGroup: entry.rowGroup,
            rowOffset: entry.rowOffset,
          })
        } else {
          const { entry, bytesRead: entryBytes } = readCompactEntry(data, offset)
          offset += entryBytes
          entries.push({
            key: new Uint8Array(0), // Key not stored in v3 sharded format
            docId: entry.docId,
            rowGroup: entry.rowGroup,
            rowOffset: entry.rowOffset,
          })
        }
      }

      return entries
    }

    offset += 1 // Skip version byte for v1/v2

    if (version !== 1 && version !== 2) {
      throw new Error(`Unsupported shard version: ${version}`)
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

      entries.push({ key, docId, rowGroup, rowOffset })
    }

    return entries
  }

  /**
   * Clear loaded shards from memory
   */
  clearCache(): void {
    this.loadedShards.clear()
  }
}

// =============================================================================
// Loader Utility
// =============================================================================

/**
 * Load a sharded hash index
 */
export async function loadShardedHashIndex(
  storage: StorageBackend,
  namespace: string,
  definition: IndexDefinition,
  basePath?: string
): Promise<ShardedHashIndex> {
  const index = new ShardedHashIndex(storage, namespace, definition, basePath)
  await index.load()
  return index
}

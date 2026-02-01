/**
 * Sharded SST Index for ParqueDB
 *
 * Provides efficient range queries with lazy shard loading.
 * Shards are divided by value range (numeric) or prefix (string).
 *
 * Storage format: indexes/secondary/{name}/
 *   _manifest.json           - Shard metadata and range mapping
 *   range-{start}-{end}.shard.idx  - Range-based shard files
 *   {prefix}.shard.idx       - Prefix-based shard files
 */

import type { StorageBackend } from '../../types/storage'
import type {
  IndexDefinition,
  IndexStats,
  IndexLookupResult,
  SSTIndexEntry,
  RangeQuery,
} from '../types'
import { encodeKey, compareKeys, decodeKey } from './key-encoder'
import type { ShardManifest, ShardInfo } from './sharded-hash'

// =============================================================================
// Sharded SST Index
// =============================================================================

/**
 * Sharded SST index for range queries with lazy loading
 */
export class ShardedSSTIndex {
  /** Shard manifest */
  private manifest: ShardManifest | null = null
  /** Loaded shards: shard name -> sorted entries */
  private loadedShards: Map<string, SSTIndexEntry[]> = new Map()
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
  private async loadShard(shardInfo: ShardInfo): Promise<SSTIndexEntry[]> {
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
   * Look up entries by exact key value
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

    // Find the shard that could contain this value
    const shardInfo = this.findShardForValue(value)
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
    const key = encodeKey(value)

    // Binary search for matching entries
    const startIdx = this.lowerBound(entries, key)
    const endIdx = this.upperBound(entries, key)

    const matches = entries.slice(startIdx, endIdx)

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
  async range(range: RangeQuery): Promise<IndexLookupResult> {
    if (!this.manifest) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    // Find all shards that could overlap with this range
    const relevantShards = this.findShardsForRange(range)

    if (relevantShards.length === 0) {
      return {
        docIds: [],
        rowGroups: [],
        exact: true,
        entriesScanned: 0,
      }
    }

    const allDocIds: string[] = []
    const allRowGroups = new Set<number>()
    let totalScanned = 0

    // Process each relevant shard
    for (const shardInfo of relevantShards) {
      const entries = await this.loadShard(shardInfo)

      let startIdx = 0
      let endIdx = entries.length

      // Apply lower bound
      if (range.$gte !== undefined) {
        const key = encodeKey(range.$gte)
        startIdx = this.lowerBound(entries, key)
      } else if (range.$gt !== undefined) {
        const key = encodeKey(range.$gt)
        startIdx = this.upperBound(entries, key)
      }

      // Apply upper bound
      if (range.$lte !== undefined) {
        const key = encodeKey(range.$lte)
        endIdx = this.upperBound(entries, key)
      } else if (range.$lt !== undefined) {
        const key = encodeKey(range.$lt)
        endIdx = this.lowerBound(entries, key)
      }

      // Handle invalid ranges
      if (startIdx < endIdx) {
        const matches = entries.slice(startIdx, endIdx)
        allDocIds.push(...matches.map(e => e.docId))
        for (const e of matches) {
          allRowGroups.add(e.rowGroup)
        }
        totalScanned += matches.length
      }
    }

    return {
      docIds: allDocIds,
      rowGroups: [...allRowGroups],
      exact: true,
      entriesScanned: totalScanned,
    }
  }

  /**
   * Get minimum value in the index
   */
  async min(): Promise<{ value: unknown; docId: string } | null> {
    if (!this.manifest || this.manifest.shards.length === 0) return null

    // For range sharding, find the shard with lowest range
    if (this.manifest.sharding === 'by-range') {
      const sortedShards = [...this.manifest.shards].sort((a, b) =>
        (a.rangeStart ?? 0) - (b.rangeStart ?? 0)
      )
      const firstShard = sortedShards[0]
      const entries = await this.loadShard(firstShard)

      if (entries.length === 0) return null

      return {
        value: decodeKey(entries[0].key),
        docId: entries[0].docId,
      }
    }

    // For prefix sharding, need to check all shards
    let minEntry: SSTIndexEntry | null = null

    for (const shardInfo of this.manifest.shards) {
      const entries = await this.loadShard(shardInfo)
      if (entries.length > 0) {
        if (!minEntry || compareKeys(entries[0].key, minEntry.key) < 0) {
          minEntry = entries[0]
        }
      }
    }

    if (!minEntry) return null

    return {
      value: decodeKey(minEntry.key),
      docId: minEntry.docId,
    }
  }

  /**
   * Get maximum value in the index
   */
  async max(): Promise<{ value: unknown; docId: string } | null> {
    if (!this.manifest || this.manifest.shards.length === 0) return null

    // For range sharding, find the shard with highest range
    if (this.manifest.sharding === 'by-range') {
      const sortedShards = [...this.manifest.shards].sort((a, b) =>
        (b.rangeEnd ?? 0) - (a.rangeEnd ?? 0)
      )
      const lastShard = sortedShards[0]
      const entries = await this.loadShard(lastShard)

      if (entries.length === 0) return null

      return {
        value: decodeKey(entries[entries.length - 1].key),
        docId: entries[entries.length - 1].docId,
      }
    }

    // For prefix sharding, need to check all shards
    let maxEntry: SSTIndexEntry | null = null

    for (const shardInfo of this.manifest.shards) {
      const entries = await this.loadShard(shardInfo)
      if (entries.length > 0) {
        const lastEntry = entries[entries.length - 1]
        if (!maxEntry || compareKeys(lastEntry.key, maxEntry.key) > 0) {
          maxEntry = lastEntry
        }
      }
    }

    if (!maxEntry) return null

    return {
      value: decodeKey(maxEntry.key),
      docId: maxEntry.docId,
    }
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
        levels: 1,
      }
    }

    const totalSize = this.manifest.shards.reduce((sum, s) => sum + s.sizeBytes, 0)

    return {
      entryCount: this.manifest.totalEntries,
      sizeBytes: totalSize,
      levels: this.manifest.shards.length,
    }
  }

  /**
   * Get the number of entries
   */
  get size(): number {
    return this.manifest?.totalEntries ?? 0
  }

  /**
   * Get the number of shards
   */
  get shardCount(): number {
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

  /**
   * Find the shard that could contain a specific value
   */
  private findShardForValue(value: unknown): ShardInfo | null {
    if (!this.manifest) return null

    if (this.manifest.sharding === 'by-range' && typeof value === 'number') {
      for (const shard of this.manifest.shards) {
        if (shard.rangeStart !== undefined && shard.rangeEnd !== undefined) {
          if (value >= shard.rangeStart && value < shard.rangeEnd) {
            return shard
          }
        }
      }
    } else if (this.manifest.sharding === 'by-prefix' && typeof value === 'string') {
      const prefix = value.charAt(0).toLowerCase() || '_'
      for (const shard of this.manifest.shards) {
        if (shard.prefix === prefix || shard.name === prefix) {
          return shard
        }
      }
    }

    // Fallback: check first shard
    return this.manifest.shards[0] ?? null
  }

  /**
   * Find all shards that could overlap with a range
   */
  private findShardsForRange(range: RangeQuery): ShardInfo[] {
    if (!this.manifest) return []

    if (this.manifest.sharding === 'by-range') {
      return this.manifest.shards.filter(shard => {
        if (shard.rangeStart === undefined || shard.rangeEnd === undefined) {
          return true  // Include if no range info
        }

        // Check if shard range overlaps with query range
        const shardMin = shard.rangeStart
        const shardMax = shard.rangeEnd

        // Query lower bound
        const queryMin = range.$gte ?? range.$gt
        // Query upper bound
        const queryMax = range.$lte ?? range.$lt

        // Shard overlaps if:
        // - query has no lower bound OR shard max > query min
        // - query has no upper bound OR shard min < query max
        const overlapsLower = queryMin === undefined ||
          shardMax > (typeof queryMin === 'number' ? queryMin : Number.MIN_SAFE_INTEGER)
        const overlapsUpper = queryMax === undefined ||
          shardMin < (typeof queryMax === 'number' ? queryMax : Number.MAX_SAFE_INTEGER)

        return overlapsLower && overlapsUpper
      })
    }

    // For prefix sharding or unknown, return all shards
    return this.manifest.shards
  }

  /**
   * Binary search: find index of first entry >= key
   */
  private lowerBound(entries: SSTIndexEntry[], key: Uint8Array): number {
    let lo = 0
    let hi = entries.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (compareKeys(entries[mid].key, key) < 0) {
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
  private upperBound(entries: SSTIndexEntry[], key: Uint8Array): number {
    let lo = 0
    let hi = entries.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (compareKeys(entries[mid].key, key) <= 0) {
        lo = mid + 1
      } else {
        hi = mid
      }
    }

    return lo
  }

  /**
   * Deserialize a shard file
   */
  private deserializeShard(data: Uint8Array): SSTIndexEntry[] {
    const entries: SSTIndexEntry[] = []

    if (data.length < 5) {
      return entries
    }

    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    let offset = 0

    // Version
    const version = view.getUint8(offset)
    offset += 1

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
 * Load a sharded SST index
 */
export async function loadShardedSSTIndex(
  storage: StorageBackend,
  namespace: string,
  definition: IndexDefinition,
  basePath?: string
): Promise<ShardedSSTIndex> {
  const index = new ShardedSSTIndex(storage, namespace, definition, basePath)
  await index.load()
  return index
}

/**
 * IndexCache for ParqueDB Worker
 *
 * Loads and caches secondary indexes from R2 storage for Worker-based query execution.
 * Supports Hash, SST, and FTS indexes with lazy loading and LRU eviction.
 *
 * Index layout in R2:
 * {dataset}/
 * ├── data.parquet
 * ├── rels.parquet
 * └── indexes/
 *     ├── _catalog.json           # Index metadata
 *     ├── secondary/
 *     │   ├── titleType.hash.idx  # Hash index (binary)
 *     │   └── startYear.sst.idx   # SST index (binary)
 *     └── fts/
 *         └── name.fts.json       # FTS inverted index
 */

import type { IndexDefinition, IndexLookupResult, RangeQuery, FTSSearchResult } from '../indexes/types'
import { HashIndex } from '../indexes/secondary/hash'
import { SSTIndex } from '../indexes/secondary/sst'
import { InvertedIndex } from '../indexes/fts/inverted-index'
import { FTSIndex } from '../indexes/fts/search'
import { logger } from '../utils/logger'
import { isErr } from '../types/result'
import { safeJsonParse, isRecord, isArray } from '../utils/json-validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Index catalog entry in _catalog.json
 */
export interface IndexCatalogEntry {
  /** Index name */
  name: string
  /** Index type */
  type: 'hash' | 'sst' | 'fts'
  /** Indexed field (e.g., '$index_titleType') */
  field: string
  /** Path to index file relative to dataset root */
  path: string
  /** Index file size in bytes */
  sizeBytes: number
  /** Number of entries in the index */
  entryCount: number
  /** Last updated timestamp */
  updatedAt?: string
  /** Whether this index is sharded */
  sharded?: boolean
  /** Path to manifest file (for sharded indexes) */
  manifestPath?: string
}

/**
 * Shard entry in a sharded index manifest
 */
export interface ShardEntry {
  /** Shard name */
  name: string
  /** Path to shard file relative to index directory */
  path: string
  /** Value this shard contains (for hash indexes) */
  value: string | number | boolean
  /** Number of entries in this shard */
  entryCount: number
  /** Min value (for SST range shards) */
  minValue?: unknown
  /** Max value (for SST range shards) */
  maxValue?: unknown
}

/**
 * Sharded index manifest format
 */
export interface ShardedIndexManifest {
  /** Manifest version */
  version: number
  /** Index type */
  type: 'hash' | 'sst'
  /** Indexed field */
  field: string
  /** Sharding strategy */
  sharding: 'by-value' | 'by-range'
  /** Whether shards use compact format */
  compact: boolean
  /** Path to bloom filter file (optional) */
  bloomPath?: string
  /** Total entry count across all shards */
  totalEntries?: number
  /** Shard entries */
  shards: ShardEntry[]
}

/**
 * Compact shard entry (parsed from v3 format)
 */
export interface CompactShardEntry {
  /** Document ID */
  docId: string
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
}

/**
 * Index catalog stored in _catalog.json
 */
export interface IndexCatalog {
  /** Catalog version */
  version: number
  /** Index entries */
  indexes: IndexCatalogEntry[]
}

/**
 * Loaded index wrapper
 */
interface LoadedIndex {
  type: 'hash' | 'sst' | 'fts'
  index: HashIndex | SSTIndex | FTSIndex
  loadedAt: number
  sizeBytes: number
}

/**
 * Loaded sharded index manifest wrapper
 */
interface LoadedManifest {
  manifest: ShardedIndexManifest
  loadedAt: number
}

/**
 * Loaded bloom filter wrapper
 */
interface LoadedBloomFilter {
  filter: BloomFilter
  loadedAt: number
  sizeBytes: number
}

/**
 * Encode a value to bytes for bloom filter hashing
 * Must match build-indexes.mjs encodeValueForBloom()
 *
 * Note: This encoding is DIFFERENT from the index key encoding.
 * Bloom filters use simple encoding without type prefixes.
 */
function encodeValueForBloom(value: unknown): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array([0])
  }

  if (typeof value === 'number') {
    const buffer = new ArrayBuffer(8)
    const view = new DataView(buffer)
    view.setFloat64(0, value, false)
    return new Uint8Array(buffer)
  }

  if (typeof value === 'boolean') {
    return new Uint8Array([value ? 1 : 0])
  }

  if (typeof value === 'string') {
    return new TextEncoder().encode(value)
  }

  // Default: JSON encode
  return new TextEncoder().encode(JSON.stringify(value))
}

/**
 * MurmurHash3 implementation matching build-indexes.mjs
 */
function murmurHash3(key: Uint8Array, seed: number): number {
  const c1 = 0xcc9e2d51
  const c2 = 0x1b873593
  const r1 = 15
  const r2 = 13
  const m = 5
  const n = 0xe6546b64

  let hash = seed
  const len = key.length
  const numBlocks = Math.floor(len / 4)

  // Process 4-byte blocks
  for (let i = 0; i < numBlocks; i++) {
    let k =
      key[i * 4]! |
      (key[i * 4 + 1]! << 8) |
      (key[i * 4 + 2]! << 16) |
      (key[i * 4 + 3]! << 24)

    k = Math.imul(k, c1)
    k = (k << r1) | (k >>> (32 - r1))
    k = Math.imul(k, c2)

    hash ^= k
    hash = (hash << r2) | (hash >>> (32 - r2))
    hash = Math.imul(hash, m) + n
  }

  // Process remaining bytes
  const tail = len - numBlocks * 4
  let k1 = 0
  if (tail >= 3) {
    k1 ^= key[numBlocks * 4 + 2]! << 16
  }
  if (tail >= 2) {
    k1 ^= key[numBlocks * 4 + 1]! << 8
  }
  if (tail >= 1) {
    k1 ^= key[numBlocks * 4]!
    k1 = Math.imul(k1, c1)
    k1 = (k1 << r1) | (k1 >>> (32 - r1))
    k1 = Math.imul(k1, c2)
    hash ^= k1
  }

  // Finalization
  hash ^= len
  hash ^= hash >>> 16
  hash = Math.imul(hash, 0x85ebca6b)
  hash ^= hash >>> 13
  hash = Math.imul(hash, 0xc2b2ae35)
  hash ^= hash >>> 16

  return hash >>> 0
}

/**
 * Generate multiple hash values using double hashing technique
 */
function getBloomHashes(key: Uint8Array, numHashes: number, filterBits: number): number[] {
  const h1 = murmurHash3(key, 0)
  const h2 = murmurHash3(key, h1)

  const hashes: number[] = []
  for (let i = 0; i < numHashes; i++) {
    const hash = ((h1 + i * h2) >>> 0) % filterBits
    hashes.push(hash)
  }

  return hashes
}

/**
 * Bloom filter implementation for pre-filtering
 *
 * Format (from build-indexes.mjs):
 * Header (16 bytes):
 * - bytes 0-3: MAGIC "PQBF" (0x50, 0x51, 0x42, 0x46)
 * - bytes 4-5: VERSION (uint16 big endian) = 1
 * - bytes 6-7: numHashFunctions (uint16 big endian) = 3
 * - bytes 8-11: valueFilterSize (uint32 big endian)
 * - bytes 12-13: numRowGroups (uint16 big endian)
 * - bytes 14-15: reserved
 * Data:
 * - bytes 16 to 16+valueFilterSize: value bloom filter bits
 * - remaining: row group bloom filters
 */
class BloomFilter {
  private bits: Uint8Array
  private numHashes: number
  private numBits: number

  constructor(data: Uint8Array) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)

    // Verify magic bytes
    const magic = [data[0], data[1], data[2], data[3]]
    const expectedMagic = [0x50, 0x51, 0x42, 0x46] // "PQBF"
    const isMagicValid = magic.every((b, i) => b === expectedMagic[i])

    if (isMagicValid) {
      // New format with 16-byte header
      this.numHashes = view.getUint16(6, false) // big endian
      const valueFilterSize = view.getUint32(8, false) // big endian
      this.numBits = valueFilterSize * 8
      this.bits = data.slice(16, 16 + valueFilterSize)
    } else {
      // Fallback to old simple format: [numHashes (1 byte)][numBits (4 bytes LE)][bits...]
      this.numHashes = data[0] ?? 0
      this.numBits = view.getUint32(1, true) // little endian
      this.bits = data.slice(5)
    }
  }

  /**
   * Check if a value might be in the set
   * @returns true if value might exist, false if definitely doesn't exist
   */
  mightContain(value: unknown): boolean {
    const key = encodeValueForBloom(value)
    const hashes = getBloomHashes(key, this.numHashes, this.numBits)

    for (const hash of hashes) {
      const byteIndex = Math.floor(hash / 8)
      const bitOffset = hash % 8
      const byte = this.bits[byteIndex]
      if (byte === undefined || (byte & (1 << bitOffset)) === 0) {
        return false
      }
    }
    return true
  }
}

/**
 * Selected index for query execution
 */
export interface SelectedIndex {
  type: 'hash' | 'sst' | 'fts'
  entry: IndexCatalogEntry
  condition: unknown
}

/**
 * Storage adapter interface for R2/filesystem
 */
export interface IndexStorageAdapter {
  read(path: string): Promise<Uint8Array>
  exists(path: string): Promise<boolean>
}

// =============================================================================
// IndexCache Implementation
// =============================================================================

/**
 * Caches and manages secondary indexes for Worker query execution
 */
export class IndexCache {
  /** Catalog cache: dataset -> catalog */
  private catalogCache = new Map<string, IndexCatalog>()

  /** Index cache: `${dataset}/${name}` -> loaded index */
  private indexCache = new Map<string, LoadedIndex>()

  /** Manifest cache: `${dataset}/${manifestPath}` -> loaded manifest */
  private manifestCache = new Map<string, LoadedManifest>()

  /** Bloom filter cache: `${dataset}/${bloomPath}` -> loaded bloom filter */
  private bloomCache = new Map<string, LoadedBloomFilter>()

  /** Maximum cache size in bytes (default: 50MB) */
  private maxCacheBytes: number

  /** Current cache size in bytes */
  private currentCacheBytes = 0

  constructor(
    private storage: IndexStorageAdapter,
    options?: { maxCacheBytes?: number }
  ) {
    this.maxCacheBytes = options?.maxCacheBytes ?? 50 * 1024 * 1024
  }

  // ===========================================================================
  // Catalog Operations
  // ===========================================================================

  /**
   * Load index catalog for a dataset
   *
   * @param dataset - Dataset ID (e.g., 'imdb-1m')
   * @returns Array of index catalog entries
   */
  async loadCatalog(dataset: string): Promise<IndexCatalogEntry[]> {
    // Check cache first
    const cached = this.catalogCache.get(dataset)
    if (cached) {
      return cached.indexes
    }

    // Load from storage
    const catalogPath = `${dataset}/indexes/_catalog.json`
    try {
      const exists = await this.storage.exists(catalogPath)
      if (!exists) {
        return []
      }

      const data = await this.storage.read(catalogPath)
      const result = safeJsonParse(new TextDecoder().decode(data))
      if (!result.ok || !isRecord(result.value)) {
        logger.warn(`Invalid index catalog JSON at ${catalogPath}`)
        return []
      }
      const catalog = result.value as unknown as IndexCatalog

      // Validate version (support version 1, 2, and 3)
      if (catalog.version < 1 || catalog.version > 3) {
        logger.warn(`Unsupported index catalog version: ${catalog.version}`)
        return []
      }

      // Detect sharded indexes by checking for manifestPath or sharded flag
      for (const entry of catalog.indexes) {
        if (entry.sharded || entry.manifestPath) {
          entry.sharded = true
          // If manifestPath not set, derive it from the entry name
          if (!entry.manifestPath) {
            entry.manifestPath = `indexes/secondary/${entry.name}/_manifest.json`
          }
        }
      }

      // Cache the catalog
      this.catalogCache.set(dataset, catalog)
      return catalog.indexes
    } catch (error: unknown) {
      logger.warn(`Failed to load index catalog for ${dataset}`, error)
      return []
    }
  }

  /**
   * Get a specific index by field name
   *
   * @param dataset - Dataset ID
   * @param field - Field name (e.g., '$index_titleType')
   * @returns Index catalog entry or null
   */
  async getIndexForField(dataset: string, field: string): Promise<IndexCatalogEntry | null> {
    const catalog = await this.loadCatalog(dataset)
    return catalog.find(entry => entry.field === field) ?? null
  }

  // ===========================================================================
  // Sharded Index Operations
  // ===========================================================================

  /**
   * Load manifest for a sharded index
   *
   * @param dataset - Dataset ID
   * @param manifestPath - Path to manifest file relative to dataset root
   * @returns Loaded manifest
   */
  async loadManifest(dataset: string, manifestPath: string): Promise<ShardedIndexManifest> {
    const cacheKey = `${dataset}/${manifestPath}`

    // Check cache first
    const cached = this.manifestCache.get(cacheKey)
    if (cached) {
      return cached.manifest
    }

    // Load from storage
    const fullPath = `${dataset}/${manifestPath}`
    const data = await this.storage.read(fullPath)
    const result = safeJsonParse(new TextDecoder().decode(data))
    if (!result.ok || !isRecord(result.value)) {
      throw new Error(`Invalid manifest JSON at ${fullPath}`)
    }
    const manifest = result.value as unknown as ShardedIndexManifest

    // Validate version
    if (manifest.version < 1 || manifest.version > 3) {
      throw new Error(`Unsupported manifest version: ${manifest.version}`)
    }

    // Cache the manifest
    this.manifestCache.set(cacheKey, {
      manifest,
      loadedAt: Date.now(),
    })

    return manifest
  }

  /**
   * Load bloom filter for a sharded index
   *
   * @param dataset - Dataset ID
   * @param indexDir - Index directory (e.g., 'indexes/secondary/titleType')
   * @param bloomPath - Path to bloom filter file relative to index directory
   * @returns Loaded bloom filter
   */
  async loadBloomFilter(dataset: string, indexDir: string, bloomPath: string): Promise<BloomFilter> {
    const fullPath = `${dataset}/${indexDir}/${bloomPath}`
    const cacheKey = fullPath

    // Check cache first
    const cached = this.bloomCache.get(cacheKey)
    if (cached) {
      return cached.filter
    }

    // Load from storage
    const data = await this.storage.read(fullPath)
    const filter = new BloomFilter(data)

    // Cache the bloom filter (counts toward cache size)
    this.cacheBloomFilter(cacheKey, {
      filter,
      loadedAt: Date.now(),
      sizeBytes: data.byteLength,
    })

    return filter
  }

  /**
   * Load a compact v3 format shard
   *
   * @param shardPath - Full path to shard file
   * @param isCompact - Whether shard uses compact v3 format
   * @returns Array of shard entries
   */
  async loadShardedHashIndex(shardPath: string, isCompact: boolean): Promise<CompactShardEntry[]> {
    const data = await this.storage.read(shardPath)

    if (isCompact) {
      return readCompactShard(data)
    }

    // Fall back to JSON format for non-compact shards
    const result = safeJsonParse(new TextDecoder().decode(data))
    if (!result.ok || !isArray(result.value)) {
      throw new Error(`Invalid shard JSON at ${shardPath}`)
    }
    return result.value as CompactShardEntry[]
  }

  /**
   * Get index directory from manifest path
   */
  private getIndexDir(manifestPath: string): string {
    // manifestPath is like 'indexes/secondary/titleType/_manifest.json'
    // We want 'indexes/secondary/titleType'
    const parts = manifestPath.split('/')
    parts.pop() // Remove '_manifest.json'
    return parts.join('/')
  }

  /**
   * Get FTS index for a dataset (if available)
   *
   * @param dataset - Dataset ID
   * @returns FTS index entry or null
   */
  async getFTSIndex(dataset: string): Promise<IndexCatalogEntry | null> {
    const catalog = await this.loadCatalog(dataset)
    return catalog.find(entry => entry.type === 'fts') ?? null
  }

  // ===========================================================================
  // Index Loading
  // ===========================================================================

  /**
   * Load a hash index
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @returns Loaded hash index
   */
  async loadHashIndex(dataset: string, entry: IndexCatalogEntry): Promise<HashIndex> {
    const cacheKey = `${dataset}/${entry.name}`

    // Check cache
    const cached = this.indexCache.get(cacheKey)
    if (cached && cached.type === 'hash') {
      return cached.index as HashIndex
    }

    // Load from storage
    const indexPath = `${dataset}/${entry.path}`
    const data = await this.storage.read(indexPath)

    // Create hash index with in-memory storage adapter
    const memoryStorage = createMemoryStorageAdapter(data)
    const definition: IndexDefinition = {
      name: entry.name,
      type: 'hash',
      fields: [{ path: entry.field }],
    }

    const index = new HashIndex(memoryStorage as unknown as import('../types/storage').StorageBackend, entry.name, definition)
    const loadResult = await index.load()

    // Log if index load failed (corrupted data), but continue with empty index
    // The index is still usable, just empty
    if (isErr(loadResult)) {
      logger.warn(`Hash index ${entry.name} load returned error (using empty index): ${loadResult.error.message}`)
    }

    // Cache the loaded index
    this.cacheIndex(cacheKey, {
      type: 'hash',
      index,
      loadedAt: Date.now(),
      sizeBytes: data.byteLength,
    })

    return index
  }

  /**
   * Load an SST index
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @returns Loaded SST index
   */
  async loadSSTIndex(dataset: string, entry: IndexCatalogEntry): Promise<SSTIndex> {
    const cacheKey = `${dataset}/${entry.name}`

    // Check cache
    const cached = this.indexCache.get(cacheKey)
    if (cached && cached.type === 'sst') {
      return cached.index as SSTIndex
    }

    // Load from storage
    const indexPath = `${dataset}/${entry.path}`
    const data = await this.storage.read(indexPath)

    // Create SST index with in-memory storage adapter
    const memoryStorage = createMemoryStorageAdapter(data)
    const definition: IndexDefinition = {
      name: entry.name,
      type: 'sst',
      fields: [{ path: entry.field }],
    }

    const index = new SSTIndex(memoryStorage as unknown as import('../types/storage').StorageBackend, entry.name, definition)
    const loadResult = await index.load()

    // Log if index load failed (corrupted data), but continue with empty index
    // The index is still usable, just empty
    if (isErr(loadResult)) {
      logger.warn(`SST index ${entry.name} load returned error (using empty index): ${loadResult.error.message}`)
    }

    // Cache the loaded index
    this.cacheIndex(cacheKey, {
      type: 'sst',
      index,
      loadedAt: Date.now(),
      sizeBytes: data.byteLength,
    })

    return index
  }

  /**
   * Load an FTS index
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @returns Loaded FTS index instance
   */
  async loadFTSIndex(dataset: string, entry: IndexCatalogEntry): Promise<FTSIndex> {
    const cacheKey = `${dataset}/${entry.name}`

    // Check cache
    const cached = this.indexCache.get(cacheKey)
    if (cached && cached.type === 'fts') {
      return cached.index as FTSIndex
    }

    // Load from storage
    const indexPath = `${dataset}/${entry.path}`
    const data = await this.storage.read(indexPath)

    // Create FTS index with in-memory storage adapter
    const memoryStorage = createMemoryStorageAdapter(data)
    const definition: IndexDefinition = {
      name: entry.name,
      type: 'fts',
      fields: [{ path: entry.field }],
    }

    const index = new FTSIndex(memoryStorage as unknown as import('../types/storage').StorageBackend, entry.name, definition)
    await index.load()

    // Cache the loaded index
    this.cacheIndex(cacheKey, {
      type: 'fts',
      index,
      loadedAt: Date.now(),
      sizeBytes: data.byteLength,
    })

    return index
  }

  // ===========================================================================
  // Index Selection
  // ===========================================================================

  /**
   * Select the best index for a filter
   *
   * @param dataset - Dataset ID
   * @param filter - MongoDB-style filter
   * @returns Selected index or null if no index applies
   */
  async selectIndex(dataset: string, filter: Record<string, unknown>): Promise<SelectedIndex | null> {
    const catalog = await this.loadCatalog(dataset)
    if (catalog.length === 0) {
      return null
    }

    // Check for $text operator -> FTS index
    if (filter.$text && typeof filter.$text === 'object' && '$search' in (filter.$text as object)) {
      const ftsIndex = catalog.find(entry => entry.type === 'fts')
      if (ftsIndex) {
        return {
          type: 'fts',
          entry: ftsIndex,
          condition: filter.$text,
        }
      }
    }

    // Check for indexed field conditions
    for (const [field, condition] of Object.entries(filter)) {
      // Skip logical operators ($and, $or, etc.) but allow $index_* indexed fields
      if (field.startsWith('$') && !field.startsWith('$index_')) continue

      // Look for index on this field
      const indexEntry = catalog.find(entry => entry.field === field)
      if (!indexEntry) continue

      if (indexEntry.type === 'hash' && isEqualityCondition(condition)) {
        return {
          type: 'hash',
          entry: indexEntry,
          condition,
        }
      }

      if (indexEntry.type === 'sst') {
        // SST can handle both equality and range
        return {
          type: 'sst',
          entry: indexEntry,
          condition,
        }
      }
    }

    return null
  }

  // ===========================================================================
  // Index Execution
  // ===========================================================================

  /**
   * Execute a hash index lookup
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @param condition - Query condition
   * @returns Index lookup result with document IDs
   */
  async executeHashLookup(
    dataset: string,
    entry: IndexCatalogEntry,
    condition: unknown
  ): Promise<IndexLookupResult> {
    // Check if this is a sharded index
    if (entry.sharded && entry.manifestPath) {
      return this.executeShardedHashLookup(dataset, entry, condition)
    }

    // Non-sharded: use original hash index loading
    const index = await this.loadHashIndex(dataset, entry)

    // Handle $in operator
    if (typeof condition === 'object' && condition !== null && '$in' in (condition as object)) {
      const values = (condition as { $in: unknown[] }).$in
      return index.lookupIn(values)
    }

    // Handle $eq or direct value
    const value = typeof condition === 'object' && condition !== null && '$eq' in (condition as object)
      ? (condition as { $eq: unknown }).$eq
      : condition

    return index.lookup(value)
  }

  /**
   * Execute a sharded hash index lookup with bloom filter pre-filtering
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry (must be sharded)
   * @param condition - Query condition
   * @returns Index lookup result with document IDs
   */
  private async executeShardedHashLookup(
    dataset: string,
    entry: IndexCatalogEntry,
    condition: unknown
  ): Promise<IndexLookupResult> {
    const manifestPath = entry.manifestPath!
    const indexDir = this.getIndexDir(manifestPath)

    // Load manifest
    const manifest = await this.loadManifest(dataset, manifestPath)

    // Extract values to look up
    let values: unknown[]
    if (typeof condition === 'object' && condition !== null && '$in' in (condition as object)) {
      values = (condition as { $in: unknown[] }).$in
    } else if (typeof condition === 'object' && condition !== null && '$eq' in (condition as object)) {
      values = [(condition as { $eq: unknown }).$eq]
    } else {
      values = [condition]
    }

    // Optionally use bloom filter for quick pre-filtering
    let filteredValues = values
    if (manifest.bloomPath) {
      try {
        const bloom = await this.loadBloomFilter(dataset, indexDir, manifest.bloomPath)
        filteredValues = values.filter(v => bloom.mightContain(v))
        if (filteredValues.length === 0) {
          // All values definitely don't exist
          return { docIds: [], rowGroups: [], exact: true, entriesScanned: 0 }
        }
      } catch (error: unknown) {
        // Bloom filter failed to load, continue without pre-filtering
        logger.warn(`Failed to load bloom filter for ${entry.name}`, error)
      }
    }

    // Collect results from matching shards
    const allDocIds: string[] = []
    const allRowGroups: Set<number> = new Set()
    let entriesScanned = 0

    for (const value of filteredValues) {
      // Find the shard for this value
      const shard = manifest.shards.find(s => s.value === value)
      if (!shard) {
        // Value not in any shard
        continue
      }

      // Load the shard
      const shardPath = `${dataset}/${indexDir}/${shard.path}`
      const entries = await this.loadShardedHashIndex(shardPath, manifest.compact)
      entriesScanned += entries.length

      // Collect all entries from the shard (entire shard matches the value)
      for (const e of entries) {
        allDocIds.push(e.docId)
        allRowGroups.add(e.rowGroup)
      }
    }

    return {
      docIds: allDocIds,
      rowGroups: Array.from(allRowGroups).sort((a, b) => a - b),
      exact: true,
      entriesScanned,
    }
  }

  /**
   * Execute an SST index lookup (range or equality)
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @param condition - Query condition
   * @returns Index lookup result with document IDs
   */
  async executeSSTLookup(
    dataset: string,
    entry: IndexCatalogEntry,
    condition: unknown
  ): Promise<IndexLookupResult> {
    // Check if this is a sharded index
    if (entry.sharded && entry.manifestPath) {
      return this.executeShardedSSTLookup(dataset, entry, condition)
    }

    // Non-sharded: use original SST index loading
    const index = await this.loadSSTIndex(dataset, entry)

    // Handle direct equality
    if (typeof condition !== 'object' || condition === null) {
      return index.lookup(condition)
    }

    const condObj = condition as Record<string, unknown>

    // Handle $eq
    if ('$eq' in condObj) {
      return index.lookup(condObj.$eq)
    }

    // Handle range operators
    const rangeQuery: RangeQuery = {}
    if ('$gt' in condObj) rangeQuery.$gt = condObj.$gt
    if ('$gte' in condObj) rangeQuery.$gte = condObj.$gte
    if ('$lt' in condObj) rangeQuery.$lt = condObj.$lt
    if ('$lte' in condObj) rangeQuery.$lte = condObj.$lte

    if (Object.keys(rangeQuery).length > 0) {
      return index.range(rangeQuery)
    }

    // Fallback to full scan
    return index.scan()
  }

  /**
   * Execute a sharded SST index lookup (range queries over sharded data)
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry (must be sharded)
   * @param condition - Query condition
   * @returns Index lookup result with document IDs
   */
  private async executeShardedSSTLookup(
    dataset: string,
    entry: IndexCatalogEntry,
    condition: unknown
  ): Promise<IndexLookupResult> {
    const manifestPath = entry.manifestPath!
    const indexDir = this.getIndexDir(manifestPath)

    // Load manifest
    const manifest = await this.loadManifest(dataset, manifestPath)

    // Parse condition to determine which shards to query
    let targetValue: unknown = undefined
    let rangeQuery: RangeQuery | undefined = undefined

    if (typeof condition !== 'object' || condition === null) {
      // Direct equality
      targetValue = condition
    } else {
      const condObj = condition as Record<string, unknown>
      if ('$eq' in condObj) {
        targetValue = condObj.$eq
      } else {
        // Range query
        rangeQuery = {}
        if ('$gt' in condObj) rangeQuery.$gt = condObj.$gt
        if ('$gte' in condObj) rangeQuery.$gte = condObj.$gte
        if ('$lt' in condObj) rangeQuery.$lt = condObj.$lt
        if ('$lte' in condObj) rangeQuery.$lte = condObj.$lte
      }
    }

    // Collect results from matching shards
    const allDocIds: string[] = []
    const allRowGroups: Set<number> = new Set()
    let entriesScanned = 0

    // Find shards that overlap with our query
    const matchingShards = manifest.shards.filter(shard => {
      if (targetValue !== undefined) {
        // Equality: shard contains exact value or range contains it
        if (shard.value !== undefined) {
          return shard.value === targetValue
        }
        if (shard.minValue !== undefined && shard.maxValue !== undefined) {
          return this.compareValues(shard.minValue, targetValue) <= 0 &&
                 this.compareValues(shard.maxValue, targetValue) >= 0
        }
        return true // Unknown structure, load it
      }

      if (rangeQuery) {
        // Range query: check if shard's range overlaps with query range
        if (shard.minValue !== undefined && shard.maxValue !== undefined) {
          return this.rangeOverlaps(shard.minValue, shard.maxValue, rangeQuery)
        }
        return true // Unknown structure, load it
      }

      return true // Load all if no specific query
    })

    // Load matching shards
    for (const shard of matchingShards) {
      const shardPath = `${dataset}/${indexDir}/${shard.path}`
      const entries = await this.loadShardedHashIndex(shardPath, manifest.compact)
      entriesScanned += entries.length

      for (const e of entries) {
        allDocIds.push(e.docId)
        allRowGroups.add(e.rowGroup)
      }
    }

    return {
      docIds: allDocIds,
      rowGroups: Array.from(allRowGroups).sort((a, b) => a - b),
      exact: true,
      entriesScanned,
    }
  }

  /**
   * Compare two values for ordering
   */
  private compareValues(a: unknown, b: unknown): number {
    if (typeof a === 'number' && typeof b === 'number') {
      return a - b
    }
    if (typeof a === 'string' && typeof b === 'string') {
      return a.localeCompare(b)
    }
    // Fall back to string comparison
    return String(a).localeCompare(String(b))
  }

  /**
   * Check if a shard's range overlaps with a range query
   */
  private rangeOverlaps(shardMin: unknown, shardMax: unknown, query: RangeQuery): boolean {
    // Check if shard is entirely below the query range
    if (query.$gt !== undefined && this.compareValues(shardMax, query.$gt) <= 0) {
      return false
    }
    if (query.$gte !== undefined && this.compareValues(shardMax, query.$gte) < 0) {
      return false
    }

    // Check if shard is entirely above the query range
    if (query.$lt !== undefined && this.compareValues(shardMin, query.$lt) >= 0) {
      return false
    }
    if (query.$lte !== undefined && this.compareValues(shardMin, query.$lte) > 0) {
      return false
    }

    return true
  }

  /**
   * Execute an FTS search
   *
   * @param dataset - Dataset ID
   * @param entry - Index catalog entry
   * @param condition - $text query condition
   * @param options - Search options
   * @returns FTS search results
   */
  async executeFTSSearch(
    dataset: string,
    entry: IndexCatalogEntry,
    condition: { $search: string; $language?: string },
    options?: { limit?: number }
  ): Promise<FTSSearchResult[]> {
    logger.debug(`[FTS] executeFTSSearch: dataset=${dataset}, query="${condition.$search}"`)
    const index = await this.loadFTSIndex(dataset, entry)
    logger.debug(`[FTS] Index loaded: ready=${index.ready}, docCount=${index.documentCount}, vocabSize=${index.vocabularySize}`)
    const results = index.search(condition.$search, {
      limit: options?.limit ?? 100,
    })
    logger.debug(`[FTS] Search results: ${results.length} matches`)
    return results
  }

  // ===========================================================================
  // Cache Management
  // ===========================================================================

  /**
   * Cache an index with LRU eviction
   */
  private cacheIndex(key: string, loaded: LoadedIndex): void {
    // Evict if necessary
    while (this.currentCacheBytes + loaded.sizeBytes > this.maxCacheBytes) {
      if (!this.evictOldestEntry()) break
    }

    // Add to cache
    this.indexCache.set(key, loaded)
    this.currentCacheBytes += loaded.sizeBytes
  }

  /**
   * Cache a bloom filter with LRU eviction
   */
  private cacheBloomFilter(key: string, loaded: LoadedBloomFilter): void {
    // Evict if necessary
    while (this.currentCacheBytes + loaded.sizeBytes > this.maxCacheBytes) {
      if (!this.evictOldestEntry()) break
    }

    // Add to cache
    this.bloomCache.set(key, loaded)
    this.currentCacheBytes += loaded.sizeBytes
  }

  /**
   * Evict the oldest cache entry (either index or bloom filter)
   * @returns true if an entry was evicted, false if cache is empty
   */
  private evictOldestEntry(): boolean {
    let oldestKey: string | null = null
    let oldestTime = Infinity
    let isBloomFilter = false

    // Find oldest index entry
    for (const [key, entry] of this.indexCache) {
      if (entry.loadedAt < oldestTime) {
        oldestTime = entry.loadedAt
        oldestKey = key
        isBloomFilter = false
      }
    }

    // Find oldest bloom filter entry
    for (const [key, entry] of this.bloomCache) {
      if (entry.loadedAt < oldestTime) {
        oldestTime = entry.loadedAt
        oldestKey = key
        isBloomFilter = true
      }
    }

    if (!oldestKey) return false

    // Evict from the appropriate cache
    if (isBloomFilter) {
      const evicted = this.bloomCache.get(oldestKey)
      if (evicted) {
        this.currentCacheBytes -= evicted.sizeBytes
        this.bloomCache.delete(oldestKey)
        return true
      }
    } else {
      const evicted = this.indexCache.get(oldestKey)
      if (evicted) {
        this.currentCacheBytes -= evicted.sizeBytes
        this.indexCache.delete(oldestKey)
        return true
      }
    }

    return false
  }

  /**
   * Find the oldest cache entry for eviction (across indexes and bloom filters)
   * @deprecated Use evictOldestEntry() instead for proper cache-aware eviction
   */
  private findOldestCacheEntry(): string | null {
    let oldestKey: string | null = null
    let oldestTime = Infinity

    for (const [key, entry] of this.indexCache) {
      if (entry.loadedAt < oldestTime) {
        oldestTime = entry.loadedAt
        oldestKey = key
      }
    }

    // Also check bloom filters for oldest entry
    for (const [key, entry] of this.bloomCache) {
      if (entry.loadedAt < oldestTime) {
        oldestTime = entry.loadedAt
        oldestKey = `bloom:${key}`
      }
    }

    return oldestKey
  }

  /**
   * Clear all cached indexes
   */
  clearCache(): void {
    this.indexCache.clear()
    this.catalogCache.clear()
    this.manifestCache.clear()
    this.bloomCache.clear()
    this.currentCacheBytes = 0
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): {
    catalogCount: number
    indexCount: number
    manifestCount: number
    bloomFilterCount: number
    cacheBytes: number
    maxBytes: number
  } {
    return {
      catalogCount: this.catalogCache.size,
      indexCount: this.indexCache.size,
      manifestCount: this.manifestCache.size,
      bloomFilterCount: this.bloomCache.size,
      cacheBytes: this.currentCacheBytes,
      maxBytes: this.maxCacheBytes,
    }
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Check if a condition is an equality condition (usable by hash index)
 */
function isEqualityCondition(condition: unknown): boolean {
  if (condition === null || typeof condition !== 'object') {
    return true // Direct value comparison
  }

  const obj = condition as Record<string, unknown>

  // Explicit $eq
  if ('$eq' in obj) return true

  // $in is also an equality-based lookup
  if ('$in' in obj) return true

  // Has range operators -> not equality
  if ('$gt' in obj || '$gte' in obj || '$lt' in obj || '$lte' in obj) {
    return false
  }

  return true
}

/**
 * Create a memory-based storage adapter from loaded data
 * This allows reusing the existing Hash/SST/FTS index classes
 */
function createMemoryStorageAdapter(data: Uint8Array): MemoryStorageAdapter {
  return new MemoryStorageAdapter(data)
}

/**
 * Memory storage adapter that holds index data in memory
 * Implements just enough of StorageBackend for index classes
 */
class MemoryStorageAdapter {
  readonly type = 'memory'
  private stored: Uint8Array

  constructor(data: Uint8Array) {
    this.stored = data
  }

  async read(_path: string): Promise<Uint8Array> {
    return this.stored
  }

  async readRange(_path: string, start: number, end: number): Promise<Uint8Array> {
    return this.stored.slice(start, end)
  }

  async write(_path: string, newData: Uint8Array): Promise<{ bytesWritten: number }> {
    this.stored = newData
    return { bytesWritten: newData.byteLength }
  }

  async writeAtomic(_path: string, newData: Uint8Array): Promise<{ bytesWritten: number }> {
    this.stored = newData
    return { bytesWritten: newData.byteLength }
  }

  async append(_path: string, data: Uint8Array): Promise<void> {
    const combined = new Uint8Array(this.stored.byteLength + data.byteLength)
    combined.set(this.stored, 0)
    combined.set(data, this.stored.byteLength)
    this.stored = combined
  }

  async exists(_path: string): Promise<boolean> {
    return this.stored.byteLength > 0
  }

  async stat(_path: string): Promise<{ path: string; size: number; mtime: Date; isDirectory: boolean } | null> {
    return {
      path: _path,
      size: this.stored.byteLength,
      mtime: new Date(),
      isDirectory: false,
    }
  }

  async list(_prefix: string): Promise<{ files: string[]; hasMore: boolean }> {
    return { files: [], hasMore: false }
  }

  async delete(_path: string): Promise<boolean> {
    this.stored = new Uint8Array(0)
    return true
  }

  async deletePrefix(_prefix: string): Promise<number> {
    return 0
  }

  async mkdir(_path: string): Promise<void> {}
  async rmdir(_path: string): Promise<void> {}
  async copy(_from: string, _to: string): Promise<void> {}
  async move(_from: string, _to: string): Promise<void> {}
}

// =============================================================================
// Compact Shard Reader (v3 format)
// =============================================================================

/**
 * Read a compact v3 format shard file
 *
 * Format:
 * - version: 1 byte (should be 3)
 * - flags: 1 byte (bit 0 = hasKeyHash)
 * - entryCount: 4 bytes (big endian)
 * - entries: [keyHash?][rowGroup (2 bytes BE)][rowOffset (varint)][docIdLen (1 byte)][docId (string)]
 *
 * @param buffer - Raw shard data
 * @returns Array of parsed entries
 */
function readCompactShard(buffer: Uint8Array): CompactShardEntry[] {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  let offset = 0

  // Read header
  const version = buffer[offset++]
  if (version !== 3) {
    throw new Error(`Unsupported compact shard version: ${version}, expected 3`)
  }

  const flags = buffer[offset++]!
  const hasKeyHash = (flags & 0x01) !== 0

  const entryCount = view.getUint32(offset, false) // big endian
  offset += 4

  const entries: CompactShardEntry[] = []
  const decoder = new TextDecoder()

  for (let i = 0; i < entryCount; i++) {
    // Skip key hash if present
    if (hasKeyHash) {
      offset += 4
    }

    // Read row group (2 bytes big endian)
    const rowGroup = view.getUint16(offset, false)
    offset += 2

    // Read varint rowOffset
    let rowOffset = 0
    let shift = 0
    while (true) {
      const byte = buffer[offset++]!
      rowOffset |= (byte & 0x7f) << shift
      if ((byte & 0x80) === 0) break
      shift += 7
      if (shift > 35) {
        throw new Error('Varint too long')
      }
    }

    // Read docId length and string
    const docIdLen = buffer[offset++]!
    const docId = decoder.decode(buffer.subarray(offset, offset + docIdLen))
    offset += docIdLen

    entries.push({ docId, rowGroup, rowOffset })
  }

  return entries
}

// =============================================================================
// R2 Storage Adapter
// =============================================================================

/**
 * Create an IndexStorageAdapter for R2 bucket
 */
export function createR2IndexStorageAdapter(bucket: R2Bucket): IndexStorageAdapter {
  return {
    async read(path: string): Promise<Uint8Array> {
      const obj = await bucket.get(path)
      if (!obj) {
        throw new Error(`Index file not found: ${path}`)
      }
      return new Uint8Array(await obj.arrayBuffer())
    },
    async exists(path: string): Promise<boolean> {
      const head = await bucket.head(path)
      return head !== null
    },
  }
}

// Type for R2Bucket (minimal interface)
interface R2Bucket {
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  head(key: string): Promise<{ size: number } | null>
}

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
      const catalog = JSON.parse(new TextDecoder().decode(data)) as IndexCatalog

      // Validate version
      if (catalog.version !== 1) {
        console.warn(`Unsupported index catalog version: ${catalog.version}`)
        return []
      }

      // Cache the catalog
      this.catalogCache.set(dataset, catalog)
      return catalog.indexes
    } catch (error) {
      console.error(`Failed to load index catalog for ${dataset}:`, error)
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
    await index.load()

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
    await index.load()

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
    const index = await this.loadFTSIndex(dataset, entry)
    return index.search(condition.$search, {
      limit: options?.limit ?? 100,
    })
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
      const oldestKey = this.findOldestCacheEntry()
      if (!oldestKey) break

      const evicted = this.indexCache.get(oldestKey)
      if (evicted) {
        this.currentCacheBytes -= evicted.sizeBytes
        this.indexCache.delete(oldestKey)
      }
    }

    // Add to cache
    this.indexCache.set(key, loaded)
    this.currentCacheBytes += loaded.sizeBytes
  }

  /**
   * Find the oldest cache entry for eviction
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

    return oldestKey
  }

  /**
   * Clear all cached indexes
   */
  clearCache(): void {
    this.indexCache.clear()
    this.catalogCache.clear()
    this.currentCacheBytes = 0
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): {
    catalogCount: number
    indexCount: number
    cacheBytes: number
    maxBytes: number
  } {
    return {
      catalogCount: this.catalogCache.size,
      indexCount: this.indexCache.size,
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

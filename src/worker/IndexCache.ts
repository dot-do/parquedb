/**
 * IndexCache for ParqueDB Worker
 *
 * Loads and caches secondary indexes from R2 storage for Worker-based query execution.
 * Supports FTS indexes with lazy loading and LRU eviction.
 *
 * NOTE: Hash and SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for equality and range queries.
 *
 * Index layout in R2:
 * {dataset}/
 * ├── data.parquet
 * ├── rels.parquet
 * └── indexes/
 *     ├── _catalog.json           # Index metadata
 *     └── fts/
 *         └── name.fts.json       # FTS inverted index
 */

import type { IndexDefinition, FTSSearchResult } from '../indexes/types'
import { FTSIndex } from '../indexes/fts/search'
import { logger } from '../utils/logger'
import { safeJsonParse, isRecord } from '../utils/json-validation'
import { DEFAULT_INDEX_CACHE_MAX_BYTES, DEFAULT_FTS_SEARCH_LIMIT } from '../constants'
import { asFTSStorageBackend } from '../types/cast'

// =============================================================================
// Types
// =============================================================================

/**
 * Index catalog entry in _catalog.json
 *
 * NOTE: Hash and SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for equality and range queries.
 */
export interface IndexCatalogEntry {
  /** Index name */
  name: string
  /** Index type */
  type: 'fts'
  /** Indexed field (e.g., 'name') */
  field: string
  /** Path to index file relative to dataset root */
  path: string
  /** Index file size in bytes */
  sizeBytes: number
  /** Number of entries in the index */
  entryCount: number
  /** Last updated timestamp */
  updatedAt?: string | undefined
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
 * Type guard for IndexCatalog
 */
function isIndexCatalog(value: unknown): value is IndexCatalog {
  if (!isRecord(value)) return false
  return (
    typeof value.version === 'number' &&
    Array.isArray(value.indexes)
  )
}

/**
 * Loaded index wrapper
 */
interface LoadedIndex {
  type: 'fts'
  index: FTSIndex
  loadedAt: number
  sizeBytes: number
}

/**
 * Selected index for query execution
 */
export interface SelectedIndex {
  type: 'fts'
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
    options?: { maxCacheBytes?: number | undefined }
  ) {
    this.maxCacheBytes = options?.maxCacheBytes ?? DEFAULT_INDEX_CACHE_MAX_BYTES
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
        logger.debug(`No index catalog found at ${catalogPath}`)
        return []
      }

      const data = await this.storage.read(catalogPath)
      const result = safeJsonParse(new TextDecoder().decode(data))
      if (!result.ok || !isIndexCatalog(result.value)) {
        logger.warn(`Invalid index catalog JSON at ${catalogPath}`)
        return []
      }
      const catalog = result.value

      // Validate version (support version 1, 2, and 3)
      if (catalog.version < 1 || catalog.version > 3) {
        logger.warn(`Unsupported index catalog version: ${catalog.version}`)
        return []
      }

      // Filter to only FTS indexes (hash indexes have been removed)
      const ftsOnlyIndexes = catalog.indexes.filter(entry => entry.type === 'fts')
      const filteredCatalog: IndexCatalog = {
        version: catalog.version,
        indexes: ftsOnlyIndexes,
      }

      // Cache the catalog
      this.catalogCache.set(dataset, filteredCatalog)
      return ftsOnlyIndexes
    } catch (error: unknown) {
      logger.warn(`Failed to load index catalog for ${dataset}`, error)
      return []
    }
  }

  /**
   * Get a specific index by field name
   *
   * @param dataset - Dataset ID
   * @param field - Field name
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

    // Cast required: MemoryStorageAdapter implements StorageBackend interface methods
    // but doesn't formally declare 'implements StorageBackend' to avoid circular imports
    const index = new FTSIndex(asFTSStorageBackend(memoryStorage), entry.name, definition)
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
   * NOTE: Hash indexes have been removed. This method only selects FTS indexes
   * for $text queries. Equality queries now use native parquet predicate pushdown.
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

    // NOTE: Hash indexes removed - equality queries now use native parquet predicate pushdown on $index_* columns

    return null
  }

  // ===========================================================================
  // Index Execution
  // ===========================================================================

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
    condition: { $search: string; $language?: string | undefined },
    options?: { limit?: number | undefined }
  ): Promise<FTSSearchResult[]> {
    logger.debug(`[FTS] executeFTSSearch: dataset=${dataset}, query="${condition.$search}"`)
    const index = await this.loadFTSIndex(dataset, entry)
    logger.debug(`[FTS] Index loaded: ready=${index.ready}, docCount=${index.documentCount}, vocabSize=${index.vocabularySize}`)
    const results = index.search(condition.$search, {
      limit: options?.limit ?? DEFAULT_FTS_SEARCH_LIMIT,
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
   * Evict the oldest cache entry
   * @returns true if an entry was evicted, false if cache is empty
   */
  private evictOldestEntry(): boolean {
    let oldestKey: string | null = null
    let oldestTime = Infinity

    // Find oldest index entry
    for (const [key, entry] of this.indexCache) {
      if (entry.loadedAt < oldestTime) {
        oldestTime = entry.loadedAt
        oldestKey = key
      }
    }

    if (!oldestKey) return false

    const evicted = this.indexCache.get(oldestKey)
    if (evicted) {
      this.currentCacheBytes -= evicted.sizeBytes
      this.indexCache.delete(oldestKey)
      return true
    }

    return false
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
 * Create a memory-based storage adapter from loaded data
 * This allows reusing the existing FTS index classes
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

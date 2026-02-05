/**
 * RowGroupCache - In-memory row group data cache for QueryExecutor
 *
 * Caches decoded row group data to avoid re-reading and re-decoding
 * the same row groups repeatedly for hot datasets.
 *
 * Features:
 * - LRU eviction based on timestamp
 * - Configurable size limit (bytes) and entry count limit
 * - TTL support for automatic expiration
 * - Cache versioning for format changes
 * - Comprehensive statistics tracking
 *
 * Expected improvement: 10-100x for repeated queries on same data
 */

import {
  DEFAULT_ROW_GROUP_CACHE_MAX_BYTES,
  DEFAULT_ROW_GROUP_CACHE_MAX_ENTRIES,
} from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Cache entry for a row group
 */
export interface CachedRowGroup {
  /** Decoded row data */
  data: unknown[]
  /** Last access timestamp (ms since epoch) */
  timestamp: number
  /** Creation timestamp (ms since epoch) */
  createdAt: number
  /** Estimated size in bytes */
  sizeBytes: number
}

/**
 * Cache statistics
 */
export interface RowGroupCacheStats {
  /** Number of entries currently in cache */
  entryCount: number
  /** Current cache size in bytes */
  sizeBytes: number
  /** Maximum cache size in bytes */
  maxSizeBytes: number
  /** Number of cache hits */
  hits: number
  /** Number of cache misses */
  misses: number
  /** Hit rate (0-1) */
  hitRate: number
  /** Number of evictions */
  evictions: number
}

/**
 * Cache configuration options
 */
export interface RowGroupCacheOptions {
  /** Maximum cache size in bytes (default: 100MB) */
  maxSizeBytes?: number | undefined
  /** Maximum number of entries (default: 50 row groups) */
  maxEntries?: number | undefined
  /** Cache version for key generation (default: 1) */
  cacheVersion?: number | undefined
  /** TTL in milliseconds (default: undefined = no TTL) */
  ttlMs?: number | undefined
}

// =============================================================================
// Constants
// =============================================================================

/** Default cache version */
const DEFAULT_CACHE_VERSION = 1

// =============================================================================
// RowGroupCache Implementation
// =============================================================================

/**
 * In-memory LRU cache for decoded row group data
 */
export class RowGroupCache {
  /** Cache storage: key -> cached row group */
  private cache = new Map<string, CachedRowGroup>()

  /** Current cache size in bytes */
  private currentSizeBytes = 0

  /** Maximum cache size in bytes */
  private maxSizeBytes: number

  /** Maximum number of entries */
  private maxEntries: number

  /** Cache version for key generation */
  private cacheVersion: number

  /** TTL in milliseconds (undefined = no TTL) */
  private ttlMs: number | undefined

  /** Cache hit count */
  private hits = 0

  /** Cache miss count */
  private misses = 0

  /** Eviction count */
  private evictions = 0

  constructor(options?: RowGroupCacheOptions) {
    this.maxSizeBytes = options?.maxSizeBytes ?? DEFAULT_ROW_GROUP_CACHE_MAX_BYTES
    this.maxEntries = options?.maxEntries ?? DEFAULT_ROW_GROUP_CACHE_MAX_ENTRIES
    this.cacheVersion = options?.cacheVersion ?? DEFAULT_CACHE_VERSION
    this.ttlMs = options?.ttlMs
  }

  // ===========================================================================
  // Cache Key Generation
  // ===========================================================================

  /**
   * Generate cache key from file path and row group index
   *
   * Format: `${filePath}:rg${rowGroupIndex}:v${cacheVersion}`
   */
  private createKey(filePath: string, rowGroupIndex: number): string {
    return `${filePath}:rg${rowGroupIndex}:v${this.cacheVersion}`
  }

  // ===========================================================================
  // Core Operations
  // ===========================================================================

  /**
   * Get cached row group data
   *
   * @param filePath - Path to the parquet file
   * @param rowGroupIndex - Row group index
   * @returns Cached entry or undefined if not found
   */
  get(filePath: string, rowGroupIndex: number): CachedRowGroup | undefined {
    const key = this.createKey(filePath, rowGroupIndex)
    const entry = this.cache.get(key)

    if (!entry) {
      this.misses++
      return undefined
    }

    // Check TTL expiration
    if (this.ttlMs !== undefined) {
      const age = Date.now() - entry.createdAt
      if (age > this.ttlMs) {
        // Entry expired - remove and return miss
        this.removeEntry(key, entry)
        this.misses++
        return undefined
      }
    }

    // Update timestamp for LRU tracking
    entry.timestamp = Date.now()
    this.hits++

    return entry
  }

  /**
   * Store row group data in cache
   *
   * @param filePath - Path to the parquet file
   * @param rowGroupIndex - Row group index
   * @param data - Decoded row data
   */
  set(filePath: string, rowGroupIndex: number, data: unknown[]): void {
    const key = this.createKey(filePath, rowGroupIndex)
    const now = Date.now()

    // Estimate size of data
    const sizeBytes = this.estimateSize(data)

    // Check if entry already exists (update case)
    const existing = this.cache.get(key)
    if (existing) {
      this.currentSizeBytes -= existing.sizeBytes
    }

    // Evict entries if necessary to make room
    this.evictIfNeeded(sizeBytes)

    // Create and store entry
    const entry: CachedRowGroup = {
      data,
      timestamp: now,
      createdAt: now,
      sizeBytes,
    }

    this.cache.set(key, entry)
    this.currentSizeBytes += sizeBytes
  }

  // ===========================================================================
  // Eviction
  // ===========================================================================

  /**
   * Evict entries if cache exceeds limits
   */
  private evictIfNeeded(incomingSizeBytes: number): void {
    // Evict while over size limit
    while (
      this.cache.size > 0 &&
      (this.currentSizeBytes + incomingSizeBytes > this.maxSizeBytes ||
        this.cache.size >= this.maxEntries)
    ) {
      const evicted = this.evictLRU()
      if (!evicted) break
    }
  }

  /**
   * Evict the least recently used entry
   *
   * @returns true if an entry was evicted
   */
  private evictLRU(): boolean {
    let oldestKey: string | null = null
    let oldestTime = Infinity

    // Find oldest entry
    this.cache.forEach((entry, key) => {
      if (entry.timestamp < oldestTime) {
        oldestTime = entry.timestamp
        oldestKey = key
      }
    })

    if (!oldestKey) return false

    const entry = this.cache.get(oldestKey)
    if (entry) {
      this.removeEntry(oldestKey, entry)
      this.evictions++
    }

    return true
  }

  /**
   * Remove an entry from the cache
   */
  private removeEntry(key: string, entry: CachedRowGroup): void {
    this.currentSizeBytes -= entry.sizeBytes
    this.cache.delete(key)
  }

  // ===========================================================================
  // Size Estimation
  // ===========================================================================

  /**
   * Estimate size of data in bytes
   *
   * Uses JSON serialization as approximation (not exact but reasonable)
   */
  private estimateSize(data: unknown[]): number {
    try {
      return JSON.stringify(data).length
    } catch {
      // Fallback: assume 100 bytes per row
      return data.length * 100
    }
  }

  // ===========================================================================
  // Cache Clearing
  // ===========================================================================

  /**
   * Clear all entries from the cache
   */
  clear(): void {
    this.cache.clear()
    this.currentSizeBytes = 0
    this.hits = 0
    this.misses = 0
    this.evictions = 0
  }

  /**
   * Invalidate all entries matching a namespace prefix
   *
   * @param namespace - Namespace prefix to invalidate (e.g., 'dataset1')
   */
  invalidate(namespace: string): void {
    const keysToDelete: string[] = []

    // Use forEach to iterate over keys (avoids downlevelIteration issues)
    this.cache.forEach((_, key) => {
      // Key format: ${filePath}:rg${index}:v${version}
      // filePath may be like: namespace/data.parquet
      if (key.startsWith(namespace + '/') || key.startsWith(namespace + ':')) {
        keysToDelete.push(key)
      }
    })

    for (let i = 0; i < keysToDelete.length; i++) {
      const key = keysToDelete[i]!
      const entry = this.cache.get(key)
      if (entry) {
        this.removeEntry(key, entry)
      }
    }
  }

  /**
   * Invalidate all entries for a specific file
   *
   * @param filePath - File path to invalidate
   */
  invalidateFile(filePath: string): void {
    const keysToDelete: string[] = []

    // Use forEach to iterate over keys (avoids downlevelIteration issues)
    this.cache.forEach((_, key) => {
      // Key format: ${filePath}:rg${index}:v${version}
      if (key.startsWith(filePath + ':')) {
        keysToDelete.push(key)
      }
    })

    for (let i = 0; i < keysToDelete.length; i++) {
      const key = keysToDelete[i]!
      const entry = this.cache.get(key)
      if (entry) {
        this.removeEntry(key, entry)
      }
    }
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get cache statistics
   */
  getStats(): RowGroupCacheStats {
    const totalRequests = this.hits + this.misses
    return {
      entryCount: this.cache.size,
      sizeBytes: this.currentSizeBytes,
      maxSizeBytes: this.maxSizeBytes,
      hits: this.hits,
      misses: this.misses,
      hitRate: totalRequests > 0 ? this.hits / totalRequests : 0,
      evictions: this.evictions,
    }
  }
}

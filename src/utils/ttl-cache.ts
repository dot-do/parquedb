/**
 * Unified LRU Cache with optional TTL and byte-size tracking
 *
 * A single generic cache implementation that supports:
 * - Generic key types (string, number, or any type)
 * - Time-to-live (TTL) for automatic entry expiration (optional)
 * - Maximum entry limit with LRU (Least Recently Used) eviction
 * - Maximum byte-size limit with configurable size calculator
 * - Manual invalidation by key, prefix, or pattern (string keys)
 * - Cache statistics (hits, misses, evictions, hit rate)
 * - Iterable interface (values, keys, entries)
 *
 * This is the single shared LRU cache implementation for ParqueDB.
 * Previously, duplicate implementations existed in Collection.ts (LRUMap)
 * and indexes/vector/lru-cache.ts (LRUCache). Those have been consolidated
 * here.
 *
 * @example
 * ```typescript
 * // Simple LRU cache (no TTL)
 * const cache = new LRUCache<string, Entity>({ maxEntries: 1000 })
 * cache.set('entity-1', entity)
 * const e = cache.get('entity-1')
 *
 * // TTL cache with LRU eviction
 * const ttlCache = new TTLCache<unknown[]>({
 *   ttlMs: 5 * 60 * 1000, // 5 minutes
 *   maxEntries: 100,
 * })
 * ttlCache.set('posts/data.parquet', rows)
 *
 * // Memory-bounded cache with byte tracking
 * const memCache = new LRUCache<number, HNSWNode>({
 *   maxEntries: 10000,
 *   maxBytes: 256 * 1024 * 1024,
 *   sizeCalculator: (node) => node.vector.length * 8,
 * })
 * ```
 */

import { getGlobalTelemetry } from '../observability/telemetry'

/**
 * Entry in the LRU cache with linked list pointers and metadata
 */
interface CacheEntry<K, V> {
  key: K
  value: V
  sizeBytes: number
  expiresAt: number // 0 = no TTL
  prev: CacheEntry<K, V> | null
  next: CacheEntry<K, V> | null
}

/**
 * Configuration options for the LRU cache
 */
export interface LRUCacheOptions<K = string, V = unknown> {
  /** Maximum number of entries (0 or undefined = unlimited) */
  maxEntries?: number | undefined
  /** Maximum total size in bytes (0 or undefined = unlimited). Requires sizeCalculator. */
  maxBytes?: number | undefined
  /** Callback when entry is evicted due to capacity limits */
  onEvict?: ((key: K, value: V) => void) | undefined
  /** Function to calculate the byte size of a value. Required when maxBytes is set. */
  sizeCalculator?: ((value: V) => number) | undefined
  /** Time-to-live in milliseconds (0 or undefined = no expiration) */
  ttlMs?: number | undefined
  /** Cache identifier for telemetry tracking */
  cacheId?: string | undefined
}

/**
 * Cache statistics
 */
export interface LRUCacheStats {
  /** Number of cache hits */
  hits: number
  /** Number of cache misses */
  misses: number
  /** Number of entries evicted due to capacity limits */
  evictions: number
  /** Current number of entries */
  size: number
  /** Maximum entries allowed (0 = unlimited) */
  maxEntries: number
  /** Maximum bytes allowed (0 = unlimited) */
  maxBytes: number
  /** Current memory usage in bytes */
  currentBytes: number
  /** TTL in milliseconds (0 = no TTL) */
  ttlMs: number
  /** Hit rate (hits / total accesses) */
  hitRate: number
}

/**
 * LRU Cache with optional TTL and byte-size tracking
 *
 * Provides O(1) get/set/delete operations using a Map for lookup
 * and a doubly-linked list for LRU ordering. Eviction key lookup
 * is also O(1) because the key is stored in each linked list node.
 *
 * @typeParam K - Key type (e.g., string, number)
 * @typeParam V - Value type
 */
export class LRUCache<K, V> {
  private cache = new Map<K, CacheEntry<K, V>>()
  private head: CacheEntry<K, V> | null = null // Most recently used
  private tail: CacheEntry<K, V> | null = null // Least recently used
  private currentBytes = 0

  private readonly _maxEntries: number
  private readonly _maxBytes: number
  private readonly _ttlMs: number
  private readonly _onEvict?: ((key: K, value: V) => void) | undefined
  private readonly _sizeCalculator: (value: V) => number
  private readonly _cacheId: string | undefined

  // Stats
  private _hits = 0
  private _misses = 0
  private _evictions = 0

  constructor(options: LRUCacheOptions<K, V> = {}) {
    this._maxEntries = options.maxEntries ?? 0
    this._maxBytes = options.maxBytes ?? 0
    // Use -1 to represent "no TTL" so that ttlMs: 0 means "immediate expiration"
    this._ttlMs = options.ttlMs !== undefined ? options.ttlMs : -1
    this._onEvict = options.onEvict
    this._sizeCalculator = options.sizeCalculator ?? (() => 0)
    this._cacheId = options.cacheId
  }

  /**
   * Get a value from the cache, marking it as recently used.
   * Returns undefined if key doesn't exist or has expired.
   */
  get(key: K): V | undefined {
    const entry = this.cache.get(key)

    if (!entry) {
      this._misses++
      if (this._cacheId) getGlobalTelemetry().recordCacheMiss(this._cacheId)
      return undefined
    }

    // Check TTL expiration
    if (entry.expiresAt > 0 && Date.now() >= entry.expiresAt) {
      this.removeEntry(entry)
      this._misses++
      if (this._cacheId) getGlobalTelemetry().recordCacheMiss(this._cacheId)
      return undefined
    }

    // Move to head (most recently used)
    this.moveToHead(entry)
    this._hits++
    if (this._cacheId) getGlobalTelemetry().recordCacheHit(this._cacheId)
    return entry.value
  }

  /**
   * Check if key exists and is not expired (does not affect LRU order)
   */
  has(key: K): boolean {
    const entry = this.cache.get(key)

    if (!entry) {
      return false
    }

    // Check TTL expiration
    if (entry.expiresAt > 0 && Date.now() >= entry.expiresAt) {
      this.removeEntry(entry)
      return false
    }

    return true
  }

  /**
   * Set a value in the cache.
   * Overwrites existing value and resets TTL if applicable.
   * May evict least recently used entries if capacity limits are exceeded.
   */
  set(key: K, value: V): this {
    const sizeBytes = this._sizeCalculator(value)
    const expiresAt = this._ttlMs >= 0 ? Date.now() + this._ttlMs : 0

    const existing = this.cache.get(key)

    if (existing) {
      // Update existing entry
      this.currentBytes -= existing.sizeBytes
      existing.value = value
      existing.sizeBytes = sizeBytes
      existing.expiresAt = expiresAt
      this.currentBytes += sizeBytes
      this.moveToHead(existing)
    } else {
      // Create new entry
      const entry: CacheEntry<K, V> = {
        key,
        value,
        sizeBytes,
        expiresAt,
        prev: null,
        next: this.head,
      }

      if (this.head) {
        this.head.prev = entry
      }
      this.head = entry

      if (!this.tail) {
        this.tail = entry
      }

      this.cache.set(key, entry)
      this.currentBytes += sizeBytes
    }

    // Evict if necessary
    this.evictIfNeeded()

    // Update cache size telemetry
    if (this._cacheId) {
      getGlobalTelemetry().updateCacheSize(this._cacheId, this.cache.size, this._maxEntries)
    }

    return this
  }

  /**
   * Delete a specific key from the cache
   */
  delete(key: K): boolean {
    const entry = this.cache.get(key)
    if (!entry) {
      return false
    }

    this.removeEntry(entry)
    return true
  }

  /**
   * Clear all entries and reset stats
   */
  clear(): void {
    this.cache.clear()
    this.head = null
    this.tail = null
    this.currentBytes = 0
    this._hits = 0
    this._misses = 0
    this._evictions = 0
  }

  /**
   * Get current number of entries (including potentially expired)
   */
  get size(): number {
    return this.cache.size
  }

  /**
   * Get current memory usage in bytes
   */
  get bytes(): number {
    return this.currentBytes
  }

  /**
   * Get cache statistics
   */
  getStats(): LRUCacheStats {
    const totalAccesses = this._hits + this._misses
    return {
      hits: this._hits,
      misses: this._misses,
      evictions: this._evictions,
      size: this.cache.size,
      maxEntries: this._maxEntries,
      maxBytes: this._maxBytes,
      currentBytes: this.currentBytes,
      ttlMs: this._ttlMs,
      hitRate: totalAccesses > 0 ? this._hits / totalAccesses : 0,
    }
  }

  /**
   * Iterate over all values (most recently used first)
   */
  *values(): IterableIterator<V> {
    let current = this.head
    while (current) {
      yield current.value
      current = current.next
    }
  }

  /**
   * Iterate over all keys (from the underlying Map)
   */
  keys(): IterableIterator<K> {
    return this.cache.keys()
  }

  /**
   * Iterate over all entries (most recently used first)
   */
  *entries(): IterableIterator<[K, V]> {
    let current = this.head
    while (current) {
      yield [current.key, current.value]
      current = current.next
    }
  }

  /**
   * Make the cache iterable with for...of
   */
  [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.entries()
  }

  /**
   * Invalidate all entries with string keys starting with prefix.
   * Only works when K is string; no-op otherwise.
   *
   * @param prefix - Key prefix to match
   * @returns Number of entries invalidated
   */
  invalidateByPrefix(prefix: string): number {
    let count = 0
    const keysToDelete: K[] = []

    for (const key of this.cache.keys()) {
      if (typeof key === 'string' && key.startsWith(prefix)) {
        keysToDelete.push(key)
      }
    }

    for (const key of keysToDelete) {
      if (this.delete(key)) {
        count++
      }
    }

    return count
  }

  /**
   * Invalidate all entries with string keys matching pattern.
   * Only works when K is string; no-op otherwise.
   *
   * @param pattern - Regular expression to match keys
   * @returns Number of entries invalidated
   */
  invalidateByPattern(pattern: RegExp): number {
    let count = 0
    const keysToDelete: K[] = []

    for (const key of this.cache.keys()) {
      if (typeof key === 'string' && pattern.test(key)) {
        keysToDelete.push(key)
      }
    }

    for (const key of keysToDelete) {
      if (this.delete(key)) {
        count++
      }
    }

    return count
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

  /**
   * Move entry to head of LRU list (most recently used)
   */
  private moveToHead(entry: CacheEntry<K, V>): void {
    if (entry === this.head) {
      return
    }

    // Remove from current position
    this.removeFromList(entry)

    // Add to head
    entry.prev = null
    entry.next = this.head
    if (this.head) {
      this.head.prev = entry
    }
    this.head = entry

    if (!this.tail) {
      this.tail = entry
    }
  }

  /**
   * Remove entry from doubly-linked list (but not from Map)
   */
  private removeFromList(entry: CacheEntry<K, V>): void {
    if (entry.prev) {
      entry.prev.next = entry.next
    } else {
      this.head = entry.next
    }

    if (entry.next) {
      entry.next.prev = entry.prev
    } else {
      this.tail = entry.prev
    }

    entry.prev = null
    entry.next = null
  }

  /**
   * Remove entry completely (from list, Map, and byte tracking)
   */
  private removeEntry(entry: CacheEntry<K, V>): void {
    this.removeFromList(entry)
    this.cache.delete(entry.key)
    this.currentBytes -= entry.sizeBytes
  }

  /**
   * Evict least recently used entries until within all limits
   */
  private evictIfNeeded(): void {
    while (this.tail && this.shouldEvict()) {
      const evictedEntry = this.tail
      this.removeEntry(evictedEntry)
      this._evictions++
      if (this._cacheId) getGlobalTelemetry().recordCacheEviction(this._cacheId)

      if (this._onEvict) {
        this._onEvict(evictedEntry.key, evictedEntry.value)
      }
    }
  }

  /**
   * Check if eviction is needed based on entry count or byte limits
   */
  private shouldEvict(): boolean {
    if (this._maxEntries > 0 && this.cache.size > this._maxEntries) {
      return true
    }
    if (this._maxBytes > 0 && this.currentBytes > this._maxBytes) {
      return true
    }
    return false
  }
}

// ===========================================================================
// TTLCache - Backward-compatible alias for LRUCache with required TTL
// ===========================================================================

/**
 * TTL Cache configuration options (backward-compatible)
 */
export interface TTLCacheOptions {
  /** Time-to-live in milliseconds */
  ttlMs: number
  /** Maximum number of entries (0 or undefined = unlimited) */
  maxEntries?: number | undefined
  /** Callback when entry is evicted */
  onEvict?: ((key: string, value: unknown) => void) | undefined
  /** Cache identifier for telemetry tracking */
  cacheId?: string | undefined
}

/**
 * TTL Cache statistics (backward-compatible)
 */
export interface TTLCacheStats {
  /** Number of cache hits */
  hits: number
  /** Number of cache misses */
  misses: number
  /** Number of entries evicted due to max size */
  evictions: number
  /** Current number of entries */
  size: number
  /** Maximum entries allowed (0 = unlimited) */
  maxEntries: number
  /** TTL in milliseconds */
  ttlMs: number
  /** Hit rate (hits / total accesses) */
  hitRate: number
}

/**
 * TTLCache - Convenience wrapper around LRUCache for string-keyed caches with TTL.
 *
 * This is a backward-compatible class that delegates to the unified LRUCache.
 * Use this when you need TTL-based expiration with string keys.
 *
 * @example
 * ```typescript
 * const cache = new TTLCache<unknown[]>({
 *   ttlMs: 5 * 60 * 1000, // 5 minutes
 *   maxEntries: 100,
 * })
 * cache.set('posts/data.parquet', rows)
 * ```
 */
export class TTLCache<V> {
  private readonly inner: LRUCache<string, V>
  private readonly _ttlMs: number
  private readonly _maxEntries: number

  constructor(options: TTLCacheOptions) {
    this._ttlMs = options.ttlMs
    this._maxEntries = options.maxEntries ?? 0
    this.inner = new LRUCache<string, V>({
      ttlMs: options.ttlMs,
      maxEntries: options.maxEntries,
      onEvict: options.onEvict as ((key: string, value: V) => void) | undefined,
      cacheId: options.cacheId,
    })
  }

  get(key: string): V | undefined {
    return this.inner.get(key)
  }

  has(key: string): boolean {
    return this.inner.has(key)
  }

  set(key: string, value: V): void {
    this.inner.set(key, value)
  }

  delete(key: string): boolean {
    return this.inner.delete(key)
  }

  clear(): void {
    this.inner.clear()
  }

  get size(): number {
    return this.inner.size
  }

  getStats(): TTLCacheStats {
    const stats = this.inner.getStats()
    return {
      hits: stats.hits,
      misses: stats.misses,
      evictions: stats.evictions,
      size: stats.size,
      maxEntries: this._maxEntries,
      ttlMs: this._ttlMs,
      hitRate: stats.hitRate,
    }
  }

  invalidateByPrefix(prefix: string): number {
    return this.inner.invalidateByPrefix(prefix)
  }

  invalidateByPattern(pattern: RegExp): number {
    return this.inner.invalidateByPattern(pattern)
  }
}

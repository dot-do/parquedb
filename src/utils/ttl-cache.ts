/**
 * TTL Cache - Time-to-Live Cache with LRU Eviction
 *
 * A generic cache implementation that supports:
 * - Time-to-live (TTL) for automatic entry expiration
 * - Maximum entry limit with LRU (Least Recently Used) eviction
 * - Manual invalidation by key, prefix, or pattern
 * - Cache statistics (hits, misses, evictions, hit rate)
 *
 * Used by QueryExecutor for caching parsed Parquet data.
 *
 * @example
 * ```typescript
 * const cache = new TTLCache<unknown[]>({
 *   ttlMs: 5 * 60 * 1000, // 5 minutes
 *   maxEntries: 100,
 * })
 *
 * cache.set('posts/data.parquet', rows)
 * const data = cache.get('posts/data.parquet')
 *
 * // Invalidate after writes
 * cache.invalidateByPrefix('posts/')
 * ```
 */

/**
 * Cache entry with TTL tracking
 */
interface CacheEntry<V> {
  value: V
  expiresAt: number
  // Doubly-linked list pointers for LRU
  prev: CacheEntry<V> | null
  next: CacheEntry<V> | null
}

/**
 * Cache configuration options
 */
export interface TTLCacheOptions {
  /** Time-to-live in milliseconds */
  ttlMs: number
  /** Maximum number of entries (0 or undefined = unlimited) */
  maxEntries?: number
  /** Callback when entry is evicted */
  onEvict?: (key: string, value: unknown) => void
}

/**
 * Cache statistics
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
 * TTL Cache with LRU eviction
 *
 * Provides O(1) get/set operations with automatic TTL expiration
 * and LRU eviction when max entries is exceeded.
 */
export class TTLCache<V> {
  private cache = new Map<string, CacheEntry<V>>()
  private head: CacheEntry<V> | null = null // Most recently used
  private tail: CacheEntry<V> | null = null // Least recently used

  private readonly ttlMs: number
  private readonly maxEntries: number
  private readonly onEvict?: (key: string, value: V) => void

  // Stats
  private hits = 0
  private misses = 0
  private evictions = 0

  constructor(options: TTLCacheOptions) {
    this.ttlMs = options.ttlMs
    this.maxEntries = options.maxEntries ?? 0 // 0 = unlimited
    this.onEvict = options.onEvict as ((key: string, value: V) => void) | undefined
  }

  /**
   * Get a value from the cache
   *
   * Returns undefined if key doesn't exist or has expired.
   * Accessing a key updates its LRU position.
   */
  get(key: string): V | undefined {
    const entry = this.cache.get(key)

    if (!entry) {
      this.misses++
      return undefined
    }

    // Check if expired
    if (Date.now() >= entry.expiresAt) {
      this.removeEntry(key, entry)
      this.misses++
      return undefined
    }

    // Move to head (most recently used)
    this.moveToHead(entry)
    this.hits++
    return entry.value
  }

  /**
   * Check if key exists and is not expired
   */
  has(key: string): boolean {
    const entry = this.cache.get(key)

    if (!entry) {
      return false
    }

    // Check if expired
    if (Date.now() >= entry.expiresAt) {
      this.removeEntry(key, entry)
      return false
    }

    return true
  }

  /**
   * Set a value in the cache
   *
   * Overwrites existing value and resets TTL.
   * May evict least recently used entry if max entries exceeded.
   */
  set(key: string, value: V): void {
    const now = Date.now()
    const expiresAt = now + this.ttlMs

    const existing = this.cache.get(key)

    if (existing) {
      // Update existing entry
      existing.value = value
      existing.expiresAt = expiresAt
      this.moveToHead(existing)
    } else {
      // Create new entry
      const entry: CacheEntry<V> = {
        value,
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

      // Evict if needed
      this.evictIfNeeded()
    }
  }

  /**
   * Delete a specific key from the cache
   */
  delete(key: string): boolean {
    const entry = this.cache.get(key)
    if (!entry) {
      return false
    }

    this.removeEntry(key, entry)
    return true
  }

  /**
   * Clear all entries and reset stats
   */
  clear(): void {
    this.cache.clear()
    this.head = null
    this.tail = null
    this.hits = 0
    this.misses = 0
    this.evictions = 0
  }

  /**
   * Get current number of entries (including potentially expired)
   */
  get size(): number {
    return this.cache.size
  }

  /**
   * Get cache statistics
   */
  getStats(): TTLCacheStats {
    const totalAccesses = this.hits + this.misses
    return {
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      size: this.cache.size,
      maxEntries: this.maxEntries,
      ttlMs: this.ttlMs,
      hitRate: totalAccesses > 0 ? this.hits / totalAccesses : 0,
    }
  }

  /**
   * Invalidate all entries with keys starting with prefix
   *
   * @param prefix - Key prefix to match
   * @returns Number of entries invalidated
   */
  invalidateByPrefix(prefix: string): number {
    let count = 0
    const keysToDelete: string[] = []

    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
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
   * Invalidate all entries with keys matching pattern
   *
   * @param pattern - Regular expression to match keys
   * @returns Number of entries invalidated
   */
  invalidateByPattern(pattern: RegExp): number {
    let count = 0
    const keysToDelete: string[] = []

    for (const key of this.cache.keys()) {
      if (pattern.test(key)) {
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
   * Move entry to head of LRU list (most recently used)
   */
  private moveToHead(entry: CacheEntry<V>): void {
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
  private removeFromList(entry: CacheEntry<V>): void {
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
   * Remove entry completely (from list and Map)
   */
  private removeEntry(key: string, entry: CacheEntry<V>): void {
    this.removeFromList(entry)
    this.cache.delete(key)
  }

  /**
   * Evict least recently used entries until within max limit
   */
  private evictIfNeeded(): void {
    if (this.maxEntries === 0) {
      return // No limit
    }

    while (this.cache.size > this.maxEntries && this.tail) {
      // Find key for tail entry
      let keyToEvict: string | null = null
      for (const [key, entry] of this.cache) {
        if (entry === this.tail) {
          keyToEvict = key
          break
        }
      }

      if (!keyToEvict) {
        break
      }

      const evictedEntry = this.tail
      this.removeEntry(keyToEvict, evictedEntry)
      this.evictions++

      if (this.onEvict) {
        this.onEvict(keyToEvict, evictedEntry.value)
      }
    }
  }
}

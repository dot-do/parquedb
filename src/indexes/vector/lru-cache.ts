/**
 * LRU Cache for Vector Index
 *
 * A memory-bounded Least Recently Used (LRU) cache for vector index nodes.
 * Automatically evicts oldest entries when memory or count limits are exceeded.
 */

/**
 * Entry in the LRU cache with size tracking
 */
interface CacheEntry<V> {
  value: V
  sizeBytes: number
  prev: CacheEntry<V> | null
  next: CacheEntry<V> | null
}

/**
 * Options for LRU cache
 */
export interface LRUCacheOptions {
  /** Maximum number of entries */
  maxEntries?: number
  /** Maximum size in bytes */
  maxBytes?: number
  /** Callback when entry is evicted */
  onEvict?: (key: number, value: unknown) => void
}

/**
 * LRU Cache with both count and memory limits
 *
 * Provides O(1) get/set/delete operations while maintaining LRU order.
 * Automatically evicts least recently used entries when limits are exceeded.
 */
export class LRUCache<V> {
  private cache = new Map<number, CacheEntry<V>>()
  private head: CacheEntry<V> | null = null // Most recently used
  private tail: CacheEntry<V> | null = null // Least recently used
  private currentBytes = 0
  private readonly maxEntries: number
  private readonly maxBytes: number
  private readonly onEvict?: (key: number, value: V) => void
  private readonly sizeCalculator: (value: V) => number

  constructor(
    options: LRUCacheOptions,
    sizeCalculator: (value: V) => number = () => 0
  ) {
    this.maxEntries = options.maxEntries ?? Infinity
    this.maxBytes = options.maxBytes ?? Infinity
    this.onEvict = options.onEvict as ((key: number, value: V) => void) | undefined
    this.sizeCalculator = sizeCalculator
  }

  /**
   * Get a value from the cache, marking it as recently used
   */
  get(key: number): V | undefined {
    const entry = this.cache.get(key)
    if (!entry) return undefined

    // Move to head (most recently used)
    this.moveToHead(entry)
    return entry.value
  }

  /**
   * Check if key exists without affecting LRU order
   */
  has(key: number): boolean {
    return this.cache.has(key)
  }

  /**
   * Set a value in the cache
   */
  set(key: number, value: V): void {
    const sizeBytes = this.sizeCalculator(value)
    const existing = this.cache.get(key)

    if (existing) {
      // Update existing entry
      this.currentBytes -= existing.sizeBytes
      existing.value = value
      existing.sizeBytes = sizeBytes
      this.currentBytes += sizeBytes
      this.moveToHead(existing)
    } else {
      // Create new entry
      const entry: CacheEntry<V> = {
        value,
        sizeBytes,
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
  }

  /**
   * Delete a value from the cache
   */
  delete(key: number): boolean {
    const entry = this.cache.get(key)
    if (!entry) return false

    this.removeFromList(entry)
    this.cache.delete(key)
    this.currentBytes -= entry.sizeBytes
    return true
  }

  /**
   * Clear all entries
   */
  clear(): void {
    this.cache.clear()
    this.head = null
    this.tail = null
    this.currentBytes = 0
  }

  /**
   * Get current number of entries
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
   * Iterate over all entries (most recent first)
   */
  *entries(): IterableIterator<[number, V]> {
    let current = this.head
    while (current) {
      // Find the key for this entry
      for (const [key, entry] of this.cache) {
        if (entry === current) {
          yield [key, current.value]
          break
        }
      }
      current = current.next
    }
  }

  /**
   * Get all values (most recent first)
   */
  *values(): IterableIterator<V> {
    let current = this.head
    while (current) {
      yield current.value
      current = current.next
    }
  }

  /**
   * Get all keys (most recent first)
   */
  keys(): IterableIterator<number> {
    return this.cache.keys()
  }

  /**
   * Move entry to head of list (most recently used)
   */
  private moveToHead(entry: CacheEntry<V>): void {
    if (entry === this.head) return

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
   * Remove entry from doubly linked list
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
   * Evict least recently used entries until within limits
   */
  private evictIfNeeded(): void {
    while (
      this.tail &&
      (this.cache.size > this.maxEntries || this.currentBytes > this.maxBytes)
    ) {
      // Find key for tail entry
      let keyToEvict: number | null = null
      for (const [key, entry] of this.cache) {
        if (entry === this.tail) {
          keyToEvict = key
          break
        }
      }

      if (keyToEvict === null) break

      const evictedEntry = this.tail
      this.removeFromList(evictedEntry)
      this.cache.delete(keyToEvict)
      this.currentBytes -= evictedEntry.sizeBytes

      if (this.onEvict) {
        this.onEvict(keyToEvict, evictedEntry.value)
      }
    }
  }
}

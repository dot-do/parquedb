/**
 * ParqueDB Store Module
 *
 * Global shared state management for ParqueDB instances.
 * Uses WeakMap with storage backend reference as key for shared state.
 *
 * DEPRECATION NOTICE:
 * This in-memory store pattern is being replaced by EventSourcedBackend,
 * which provides consistent event-sourcing semantics across all environments.
 * See: src/storage/EventSourcedBackend.ts
 * See: docs/architecture/entity-storage.md
 *
 * For new code, prefer using EventSourcedBackend:
 * ```typescript
 * import { withEventSourcing, MemoryBackend } from '@parquedb/storage'
 * const storage = withEventSourcing(new MemoryBackend())
 * ```
 */

import type { Entity, StorageBackend, Event } from '../types'
import type { Snapshot, SnapshotQueryStats } from './types'
import { LRUCache } from '../utils/ttl-cache'

// =============================================================================
// Entity Store Configuration
// =============================================================================

/** Default maximum number of entities to keep in memory */
export const DEFAULT_MAX_ENTITIES = 10000

/**
 * Configuration for the entity store cache
 */
export interface EntityStoreConfig {
  /** Maximum number of entities to cache (default: 10000) */
  maxEntities?: number | undefined
  /** Callback when an entity is evicted from cache */
  onEvict?: ((key: string, entity: Entity) => void) | undefined
}

/** Global configuration for entity stores by storage backend */
const globalEntityStoreConfig = new WeakMap<StorageBackend, EntityStoreConfig>()

// =============================================================================
// LRU Entity Cache
// =============================================================================

/**
 * LRU-based entity cache that implements the Map interface.
 *
 * This wraps the generic LRUCache to provide a Map-compatible interface
 * while adding LRU eviction when the cache exceeds its size limit.
 *
 * @example
 * ```typescript
 * const cache = new LRUEntityCache({ maxEntities: 1000 })
 * cache.set('posts/abc', entity)
 * const e = cache.get('posts/abc') // Marks as recently used
 * ```
 */
export class LRUEntityCache implements Map<string, Entity> {
  private readonly cache: LRUCache<string, Entity>
  private readonly config: EntityStoreConfig

  constructor(config: EntityStoreConfig = {}) {
    this.config = config
    const maxEntries = config.maxEntities ?? DEFAULT_MAX_ENTITIES
    this.cache = new LRUCache<string, Entity>({
      maxEntries: maxEntries > 0 ? maxEntries : undefined,
      onEvict: config.onEvict,
      cacheId: 'entity-store',
    })
  }

  // Map interface implementation

  get size(): number {
    return this.cache.size
  }

  get(key: string): Entity | undefined {
    return this.cache.get(key)
  }

  set(key: string, value: Entity): this {
    this.cache.set(key, value)
    return this
  }

  has(key: string): boolean {
    return this.cache.has(key)
  }

  delete(key: string): boolean {
    return this.cache.delete(key)
  }

  clear(): void {
    this.cache.clear()
  }

  forEach(callback: (value: Entity, key: string, map: Map<string, Entity>) => void, thisArg?: unknown): void {
    for (const [key, value] of this.cache.entries()) {
      callback.call(thisArg, value, key, this)
    }
  }

  *entries(): IterableIterator<[string, Entity]> {
    yield* this.cache.entries()
  }

  *keys(): IterableIterator<string> {
    yield* this.cache.keys()
  }

  *values(): IterableIterator<Entity> {
    yield* this.cache.values()
  }

  [Symbol.iterator](): IterableIterator<[string, Entity]> {
    return this.entries()
  }

  get [Symbol.toStringTag](): string {
    return 'LRUEntityCache'
  }

  // Additional methods for cache management

  /**
   * Get the current cache configuration
   */
  getConfig(): EntityStoreConfig {
    return { ...this.config }
  }

  /**
   * Get cache statistics
   */
  getStats(): {
    size: number
    maxEntries: number
    hits: number
    misses: number
    evictions: number
    hitRate: number
  } {
    const stats = this.cache.getStats()
    return {
      size: stats.size,
      maxEntries: stats.maxEntries,
      hits: stats.hits,
      misses: stats.misses,
      evictions: stats.evictions,
      hitRate: stats.hitRate,
    }
  }

  /**
   * Invalidate all entries with keys starting with a prefix
   * @param prefix - Key prefix to match (e.g., "posts/" for all posts)
   * @returns Number of entries invalidated
   */
  invalidateByPrefix(prefix: string): number {
    return this.cache.invalidateByPrefix(prefix)
  }
}

// =============================================================================
// Global Shared State
// =============================================================================

/**
 * Global storage for shared entity state across ParqueDB instances.
 * Uses WeakMap with storage backend reference as key for shared state, allowing
 * multiple ParqueDB instances with the same storage to share entities.
 *
 * WeakMap allows automatic garbage collection when StorageBackend objects
 * are no longer referenced, preventing memory leaks from orphaned state.
 * Call dispose() for explicit cleanup when a ParqueDB instance is no longer needed.
 *
 * The entity store now uses an LRU cache with configurable size limits to prevent
 * unbounded memory growth. When the limit is reached, least recently used entities
 * are evicted automatically.
 *
 * @deprecated This in-memory store is intended for Node.js/testing use only.
 * For Cloudflare Workers, use ParqueDBDO (SQLite) as the source of truth for writes
 * and R2 (via QueryExecutor/ReadPath) for reads. See docs/architecture/ENTITY_STORAGE.md
 * for the full architecture documentation.
 *
 * Architecture summary:
 * - Node.js/Testing: ParqueDB.ts uses globalEntityStore (in-memory) + storage backend for persistence
 * - Workers (writes): ParqueDBDO uses SQLite as source of truth, flushes to Parquet/R2
 * - Workers (reads): QueryExecutor reads directly from R2 Parquet files with caching
 *
 * Future plans: Consolidate to always read/write through storage backend abstractions.
 */
const globalEntityStore = new WeakMap<StorageBackend, LRUEntityCache>()
const globalEventStore = new WeakMap<StorageBackend, Event[]>()
const globalArchivedEventStore = new WeakMap<StorageBackend, Event[]>()
const globalSnapshotStore = new WeakMap<StorageBackend, Snapshot[]>()
const globalQueryStats = new WeakMap<StorageBackend, Map<string, SnapshotQueryStats>>()

/**
 * Reverse relationship index for O(1) lookups of inbound references.
 * Maps: targetEntityId -> Map<(sourceNs + "." + sourceField) -> Set<sourceEntityId>>
 *
 * This index eliminates the N+1 query pattern when traversing reverse relationships.
 * Instead of scanning all entities to find those that reference a target,
 * we can directly look up the source entities in this index.
 *
 * Example: When looking up all Posts that reference a User via the "author" field:
 * - Without index: O(n) scan of all posts
 * - With index: O(1) lookup in reverseRelIndex["users/123"]["posts.author"]
 */
const globalReverseRelIndex = new WeakMap<StorageBackend, Map<string, Map<string, Set<string>>>>()

/**
 * Entity event index for O(1) lookups of events by entity target.
 * Maps: entityTarget (e.g., "posts:abc123") -> Event[]
 *
 * Events are stored in chronological order (sorted by timestamp, then by ID).
 * This eliminates the O(n) filter pattern in reconstructEntityAtTime by providing
 * direct access to an entity's events without scanning all events.
 *
 * Example: When reconstructing "posts/abc" at a specific time:
 * - Without index: O(n) scan of all events, then sort
 * - With index: O(1) lookup, events already sorted
 */
const globalEntityEventIndex = new WeakMap<StorageBackend, Map<string, Event[]>>()

/**
 * LRU cache for recent entity reconstructions.
 * Maps: cacheKey (e.g., "posts/abc:1704067200000") -> Entity
 *
 * This avoids replaying events for frequently accessed time-travel queries.
 * The cache key combines entity ID and asOf timestamp for uniqueness.
 */
const globalReconstructionCache = new WeakMap<StorageBackend, Map<string, { entity: Entity | null; timestamp: number }>>()

/** Maximum number of entries in the reconstruction cache */
const RECONSTRUCTION_CACHE_MAX_SIZE = 1000

/** Maximum age of cache entries in milliseconds (5 minutes) */
const RECONSTRUCTION_CACHE_MAX_AGE = 5 * 60 * 1000

// =============================================================================
// Store Accessor Functions
// =============================================================================

/**
 * Configure the entity store for a storage backend before first use.
 *
 * This should be called before getEntityStore() if you want to customize
 * the cache behavior. If not called, defaults will be used.
 *
 * @param storage - The storage backend to configure
 * @param config - Cache configuration options
 *
 * @example
 * ```typescript
 * // Configure with custom limit
 * configureEntityStore(storage, { maxEntities: 5000 })
 *
 * // Configure with eviction callback
 * configureEntityStore(storage, {
 *   maxEntities: 10000,
 *   onEvict: (key, entity) => console.log(`Evicted: ${key}`)
 * })
 * ```
 */
export function configureEntityStore(storage: StorageBackend, config: EntityStoreConfig): void {
  globalEntityStoreConfig.set(storage, config)

  // If store already exists, we need to create a new one with the updated config
  const existingStore = globalEntityStore.get(storage)
  if (existingStore) {
    // Create new cache with new config and migrate entries
    const newStore = new LRUEntityCache(config)

    // Copy entries from old store (this may trigger evictions if new limit is lower)
    for (const [key, value] of existingStore.entries()) {
      newStore.set(key, value)
    }

    globalEntityStore.set(storage, newStore)
  }
}

/**
 * Get or create the entity store for a storage backend.
 *
 * The store is an LRU cache with configurable size limits to prevent
 * unbounded memory growth. Configure the cache before first use with
 * configureEntityStore() or pass config through ParqueDBConfig.maxCacheSize.
 *
 * @param storage - The storage backend to get the store for
 * @param config - Optional cache configuration (used only when creating new store)
 * @returns Map-compatible LRU cache for entities
 */
export function getEntityStore(storage: StorageBackend, config?: EntityStoreConfig): Map<string, Entity> {
  let store = globalEntityStore.get(storage)
  if (!store) {
    // Use provided config, stored config, or defaults
    const finalConfig = config ?? globalEntityStoreConfig.get(storage) ?? {}
    store = new LRUEntityCache(finalConfig)
    globalEntityStore.set(storage, store)
  }
  return store
}

/**
 * Get the LRU entity cache directly for access to cache-specific methods.
 *
 * @param storage - The storage backend
 * @returns The LRUEntityCache instance, or undefined if not created yet
 */
export function getEntityCacheStats(storage: StorageBackend): ReturnType<LRUEntityCache['getStats']> | undefined {
  const store = globalEntityStore.get(storage)
  if (store) {
    return store.getStats()
  }
  return undefined
}

/**
 * Get or create the event store for a storage backend
 */
export function getEventStore(storage: StorageBackend): Event[] {
  let store = globalEventStore.get(storage)
  if (!store) {
    store = []
    globalEventStore.set(storage, store)
  }
  return store
}

/**
 * Get or create the archived event store for a storage backend
 */
export function getArchivedEventStore(storage: StorageBackend): Event[] {
  let store = globalArchivedEventStore.get(storage)
  if (!store) {
    store = []
    globalArchivedEventStore.set(storage, store)
  }
  return store
}

/**
 * Get or create the snapshot store for a storage backend
 */
export function getSnapshotStore(storage: StorageBackend): Snapshot[] {
  let store = globalSnapshotStore.get(storage)
  if (!store) {
    store = []
    globalSnapshotStore.set(storage, store)
  }
  return store
}

/**
 * Get or create the query stats store for a storage backend
 */
export function getQueryStatsStore(storage: StorageBackend): Map<string, SnapshotQueryStats> {
  let store = globalQueryStats.get(storage)
  if (!store) {
    store = new Map()
    globalQueryStats.set(storage, store)
  }
  return store
}

/**
 * Get or create the reverse relationship index for a storage backend
 *
 * The index structure is:
 * Map<targetEntityId, Map<sourceKey, Set<sourceEntityId>>>
 *
 * Where sourceKey = `${sourceNamespace}.${sourceFieldName}`
 *
 * This allows efficient lookups like:
 * "Find all posts that reference users/123 via the author field"
 * -> reverseRelIndex.get("users/123")?.get("posts.author")
 */
export function getReverseRelIndex(storage: StorageBackend): Map<string, Map<string, Set<string>>> {
  let index = globalReverseRelIndex.get(storage)
  if (!index) {
    index = new Map()
    globalReverseRelIndex.set(storage, index)
  }
  return index
}

/**
 * Add a relationship to the reverse index.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID that has the relationship (e.g., "posts/abc")
 * @param sourceField - The field name on the source (e.g., "author")
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 */
export function addToReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  sourceField: string,
  targetId: string
): void {
  const sourceNs = sourceId.split('/')[0]
  const sourceKey = `${sourceNs}.${sourceField}`

  let targetMap = index.get(targetId)
  if (!targetMap) {
    targetMap = new Map()
    index.set(targetId, targetMap)
  }

  let sourceSet = targetMap.get(sourceKey)
  if (!sourceSet) {
    sourceSet = new Set()
    targetMap.set(sourceKey, sourceSet)
  }

  sourceSet.add(sourceId)
}

/**
 * Remove a relationship from the reverse index.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID that had the relationship (e.g., "posts/abc")
 * @param sourceField - The field name on the source (e.g., "author")
 * @param targetId - The entity ID that was referenced (e.g., "users/123")
 */
export function removeFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  sourceField: string,
  targetId: string
): void {
  const sourceNs = sourceId.split('/')[0]
  const sourceKey = `${sourceNs}.${sourceField}`

  const targetMap = index.get(targetId)
  if (!targetMap) return

  const sourceSet = targetMap.get(sourceKey)
  if (!sourceSet) return

  sourceSet.delete(sourceId)

  // Clean up empty sets and maps
  if (sourceSet.size === 0) {
    targetMap.delete(sourceKey)
  }
  if (targetMap.size === 0) {
    index.delete(targetId)
  }
}

/**
 * Get all entities that reference a target via a specific field.
 *
 * @param index - The reverse relationship index
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 * @param sourceNs - The namespace of source entities (e.g., "posts")
 * @param sourceField - The field name on the source (e.g., "author")
 * @returns Set of source entity IDs, or empty set if none
 */
export function getFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  targetId: string,
  sourceNs: string,
  sourceField: string
): Set<string> {
  const sourceKey = `${sourceNs}.${sourceField}`
  const targetMap = index.get(targetId)
  if (!targetMap) return new Set()
  return targetMap.get(sourceKey) || new Set()
}

/**
 * Get all entities that reference a target from a specific namespace (any field).
 *
 * @param index - The reverse relationship index
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 * @param sourceNs - The namespace of source entities (e.g., "posts")
 * @returns Map of field names to source entity IDs
 */
export function getAllFromReverseRelIndexByNs(
  index: Map<string, Map<string, Set<string>>>,
  targetId: string,
  sourceNs: string
): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>()
  const targetMap = index.get(targetId)
  if (!targetMap) return result

  const prefix = `${sourceNs}.`
  for (const [sourceKey, sourceSet] of targetMap) {
    if (sourceKey.startsWith(prefix)) {
      const fieldName = sourceKey.slice(prefix.length)
      result.set(fieldName, sourceSet)
    }
  }
  return result
}

/**
 * Remove all reverse index entries for a source entity.
 * Call this when an entity is deleted.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID being removed
 * @param entities - The entity store, to find all references from this entity
 */
export function removeAllFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  entity: Entity | undefined
): void {
  if (!entity) return

  // Iterate over all fields that could be relationships
  for (const [fieldName, fieldValue] of Object.entries(entity)) {
    if (fieldName.startsWith('$')) continue // Skip meta fields
    if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
      // This could be a relationship field: { displayName: targetId }
      for (const targetId of Object.values(fieldValue as Record<string, unknown>)) {
        if (typeof targetId === 'string' && targetId.includes('/')) {
          removeFromReverseRelIndex(index, sourceId, fieldName, targetId)
        }
      }
    }
  }
}

/**
 * Get or create the entity event index for a storage backend.
 *
 * @param storage - The storage backend to get the index for
 * @returns Map of entity targets to their sorted events
 */
export function getEntityEventIndex(storage: StorageBackend): Map<string, Event[]> {
  let index = globalEntityEventIndex.get(storage)
  if (!index) {
    index = new Map()
    globalEntityEventIndex.set(storage, index)
  }
  return index
}

/**
 * Add an event to the entity event index.
 * Maintains sorted order by timestamp, then by ID.
 *
 * @param index - The entity event index
 * @param entityTarget - The entity target (e.g., "posts:abc123")
 * @param event - The event to add
 */
export function addToEntityEventIndex(
  index: Map<string, Event[]>,
  entityTarget: string,
  event: Event
): void {
  let events = index.get(entityTarget)
  if (!events) {
    events = []
    index.set(entityTarget, events)
  }

  // Insert in sorted order (by timestamp, then by ID)
  // Most events are appended at the end, so check that case first
  if (events.length === 0 || compareEvents(event, events[events.length - 1]!) >= 0) {
    events.push(event)
  } else {
    // Binary search to find insertion point
    let left = 0
    let right = events.length
    while (left < right) {
      const mid = Math.floor((left + right) / 2)
      if (compareEvents(event, events[mid]!) < 0) {
        right = mid
      } else {
        left = mid + 1
      }
    }
    events.splice(left, 0, event)
  }
}

/**
 * Compare two events for sorting (by timestamp, then by ID).
 * Returns negative if a < b, positive if a > b, 0 if equal.
 */
function compareEvents(a: Event, b: Event): number {
  const timeDiff = a.ts - b.ts
  if (timeDiff !== 0) return timeDiff
  return a.id.localeCompare(b.id)
}

/**
 * Get events for an entity from the index.
 *
 * @param index - The entity event index
 * @param entityTarget - The entity target (e.g., "posts:abc123")
 * @returns Array of events for the entity, sorted by timestamp
 */
export function getFromEntityEventIndex(
  index: Map<string, Event[]>,
  entityTarget: string
): Event[] {
  return index.get(entityTarget) || []
}

/**
 * Get or create the reconstruction cache for a storage backend.
 *
 * @param storage - The storage backend to get the cache for
 * @returns Map of cache keys to cached entities
 */
export function getReconstructionCache(storage: StorageBackend): Map<string, { entity: Entity | null; timestamp: number }> {
  let cache = globalReconstructionCache.get(storage)
  if (!cache) {
    cache = new Map()
    globalReconstructionCache.set(storage, cache)
  }
  return cache
}

/**
 * Get a cached reconstruction result.
 *
 * @param cache - The reconstruction cache
 * @param fullId - The entity ID (e.g., "posts/abc123")
 * @param asOfTime - The timestamp of the reconstruction
 * @returns The cached entity or undefined if not cached/expired
 */
export function getFromReconstructionCache(
  cache: Map<string, { entity: Entity | null; timestamp: number }>,
  fullId: string,
  asOfTime: number
): Entity | null | undefined {
  const cacheKey = `${fullId}:${asOfTime}`
  const entry = cache.get(cacheKey)
  if (!entry) return undefined

  // Check if entry has expired
  if (Date.now() - entry.timestamp > RECONSTRUCTION_CACHE_MAX_AGE) {
    cache.delete(cacheKey)
    return undefined
  }

  return entry.entity
}

/**
 * Add a reconstruction result to the cache.
 *
 * @param cache - The reconstruction cache
 * @param fullId - The entity ID (e.g., "posts/abc123")
 * @param asOfTime - The timestamp of the reconstruction
 * @param entity - The reconstructed entity (or null if didn't exist)
 */
export function addToReconstructionCache(
  cache: Map<string, { entity: Entity | null; timestamp: number }>,
  fullId: string,
  asOfTime: number,
  entity: Entity | null
): void {
  const cacheKey = `${fullId}:${asOfTime}`

  // Evict oldest entries if cache is full (simple LRU approximation)
  if (cache.size >= RECONSTRUCTION_CACHE_MAX_SIZE) {
    // Delete first 10% of entries (oldest by insertion order in Map)
    const toDelete = Math.floor(RECONSTRUCTION_CACHE_MAX_SIZE * 0.1)
    let deleted = 0
    for (const key of cache.keys()) {
      if (deleted >= toDelete) break
      cache.delete(key)
      deleted++
    }
  }

  cache.set(cacheKey, { entity, timestamp: Date.now() })
}

/**
 * Invalidate cache entries for an entity.
 * Called when an entity is modified.
 *
 * @param cache - The reconstruction cache
 * @param fullId - The entity ID to invalidate
 */
export function invalidateReconstructionCache(
  cache: Map<string, { entity: Entity | null; timestamp: number }>,
  fullId: string
): void {
  // Remove all entries for this entity (any timestamp)
  const prefix = `${fullId}:`
  for (const key of cache.keys()) {
    if (key.startsWith(prefix)) {
      cache.delete(key)
    }
  }
}

/**
 * Clear global state for a specific storage backend.
 * This is called by dispose() for explicit cleanup.
 */
export function clearGlobalState(storage: StorageBackend): void {
  globalEntityStore.delete(storage)
  globalEventStore.delete(storage)
  globalArchivedEventStore.delete(storage)
  globalSnapshotStore.delete(storage)
  globalQueryStats.delete(storage)
  globalReverseRelIndex.delete(storage)
  globalEntityEventIndex.delete(storage)
  globalReconstructionCache.delete(storage)
}

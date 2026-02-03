/**
 * Entity Cache and Invalidation Manager
 *
 * Manages in-memory entity cache with LRU eviction and provides
 * cache invalidation signaling for distributed Workers.
 */

import type { Entity } from '../../types'
import type { CacheInvalidationSignal } from './types'
import {
  ENTITY_CACHE_MAX_SIZE,
  MAX_PENDING_INVALIDATIONS,
} from '../../constants'

/**
 * Entity cache with LRU eviction
 */
export class EntityCacheManager {
  /** LRU cache for recent entity states */
  private entityCache: Map<string, { entity: Entity; version: number }> = new Map()

  /** Cache invalidation version per namespace */
  private invalidationVersions: Map<string, number> = new Map()

  /** Pending invalidation signals for Workers to poll */
  private pendingInvalidations: CacheInvalidationSignal[] = []

  // ===========================================================================
  // Entity Cache Operations
  // ===========================================================================

  /**
   * Get an entity from cache
   */
  get(ns: string, id: string): { entity: Entity; version: number } | undefined {
    return this.entityCache.get(`${ns}/${id}`)
  }

  /**
   * Add or update an entity in the cache (LRU)
   */
  set(ns: string, id: string, entity: Entity): void {
    const key = `${ns}/${id}`
    // Delete and re-add to maintain LRU order
    this.entityCache.delete(key)
    this.entityCache.set(key, { entity, version: entity.version })

    // Evict oldest if over capacity
    if (this.entityCache.size > ENTITY_CACHE_MAX_SIZE) {
      const oldestKey = this.entityCache.keys().next().value
      if (oldestKey) {
        this.entityCache.delete(oldestKey)
      }
    }
  }

  /**
   * Invalidate an entity in the cache
   */
  invalidate(ns: string, id: string): void {
    this.entityCache.delete(`${ns}/${id}`)
  }

  /**
   * Check if an entity is cached
   */
  has(ns: string, id: string): boolean {
    return this.entityCache.has(`${ns}/${id}`)
  }

  /**
   * Clear the entire cache
   */
  clear(): void {
    this.entityCache.clear()
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; maxSize: number } {
    return { size: this.entityCache.size, maxSize: ENTITY_CACHE_MAX_SIZE }
  }

  /**
   * Get the full cache map (for transaction snapshots)
   */
  getFullCache(): Map<string, { entity: Entity; version: number }> {
    return this.entityCache
  }

  /**
   * Replace the full cache (for transaction rollback)
   */
  setFullCache(cache: Map<string, { entity: Entity; version: number }>): void {
    this.entityCache = cache
  }

  // ===========================================================================
  // Cache Invalidation Signaling
  // ===========================================================================

  /**
   * Signal cache invalidation for a namespace
   *
   * Called after write operations to notify Workers that caches are stale.
   * Workers poll getInvalidationVersion() or getPendingInvalidations() to
   * detect when to invalidate their caches.
   *
   * @param ns - Namespace that was modified
   * @param type - Type of invalidation (entity, relationship, or full)
   * @param entityId - Optional entity ID for entity-specific invalidation
   */
  signal(
    ns: string,
    type: 'entity' | 'relationship' | 'full',
    entityId?: string
  ): void {
    // Bump version for this namespace
    const currentVersion = this.invalidationVersions.get(ns) ?? 0
    const newVersion = currentVersion + 1
    this.invalidationVersions.set(ns, newVersion)

    // Create invalidation signal
    const signal: CacheInvalidationSignal = {
      ns,
      type,
      timestamp: Date.now(),
      version: newVersion,
      entityId,
    }

    // Add to pending invalidations (circular buffer)
    this.pendingInvalidations.push(signal)
    if (this.pendingInvalidations.length > MAX_PENDING_INVALIDATIONS) {
      this.pendingInvalidations.shift()
    }
  }

  /**
   * Get the current invalidation version for a namespace
   *
   * Workers can compare this with their cached version to detect stale caches.
   * If the DO version is higher, caches should be invalidated.
   *
   * @param ns - Namespace to check
   * @returns Current invalidation version (0 if never modified)
   */
  getInvalidationVersion(ns: string): number {
    return this.invalidationVersions.get(ns) ?? 0
  }

  /**
   * Get invalidation versions for all namespaces
   *
   * Useful for Workers to batch-check multiple namespaces.
   *
   * @returns Map of namespace to invalidation version
   */
  getAllInvalidationVersions(): Record<string, number> {
    const result: Record<string, number> = {}
    for (const [ns, version] of this.invalidationVersions) {
      result[ns] = version
    }
    return result
  }

  /**
   * Get pending invalidation signals since a given version
   *
   * Workers can poll this to get detailed invalidation information.
   * Useful for surgical cache invalidation rather than full namespace purge.
   *
   * @param ns - Namespace to get signals for (or undefined for all)
   * @param sinceVersion - Only return signals with version > this value
   * @returns Array of invalidation signals
   */
  getPendingInvalidations(ns?: string, sinceVersion?: number): CacheInvalidationSignal[] {
    let signals = this.pendingInvalidations

    if (ns) {
      signals = signals.filter(s => s.ns === ns)
    }

    if (sinceVersion !== undefined) {
      signals = signals.filter(s => s.version > sinceVersion)
    }

    return signals
  }

  /**
   * Check if caches for a namespace are stale
   *
   * Convenience method for Workers to quickly check if invalidation is needed.
   *
   * @param ns - Namespace to check
   * @param workerVersion - Worker's cached version for this namespace
   * @returns true if Worker should invalidate its caches
   */
  shouldInvalidate(ns: string, workerVersion: number): boolean {
    const doVersion = this.invalidationVersions.get(ns) ?? 0
    return doVersion > workerVersion
  }
}

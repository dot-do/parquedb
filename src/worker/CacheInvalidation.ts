/**
 * CacheInvalidation - Cache invalidation for ParqueDB CQRS architecture
 *
 * When Durable Objects write data, cached reads on Workers may become stale.
 * This module provides mechanisms to invalidate caches after writes:
 *
 * 1. **Cache API Purge**: Direct deletion of cached entries using Cloudflare Cache API
 * 2. **Version-based Invalidation**: Use ETags/versions in cache keys for automatic invalidation
 * 3. **Broadcast Invalidation**: Notify all Workers of cache changes via lightweight signaling
 *
 * Design Considerations:
 * - Cloudflare Cache API operates per-colo (data center), so global invalidation
 *   requires either cache tags (Enterprise) or version-based cache keys
 * - For consistency, writes increment a version counter that's embedded in cache keys
 * - stale-while-revalidate provides eventual consistency with good latency
 */

import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Paths that should be invalidated for a namespace
 */
export interface NamespaceCachePaths {
  /** Main data file */
  data: string
  /** Bloom filter index */
  bloom: string
  /** Forward relationship index */
  forwardRels: string
  /** Reverse relationship index */
  reverseRels: string
  /** Metadata/footer cache keys */
  metadata: string[]
}

/**
 * Invalidation result
 */
export interface InvalidationResult {
  /** Number of cache entries deleted */
  entriesDeleted: number
  /** Paths that were invalidated */
  paths: string[]
  /** Whether the invalidation was successful */
  success: boolean
  /** Error message if invalidation failed */
  error?: string | undefined
  /** Time taken in milliseconds */
  durationMs: number
}

/**
 * Cache version information stored per namespace
 */
export interface CacheVersion {
  /** Current version number */
  version: number
  /** Last update timestamp */
  updatedAt: number
  /** Namespace this version applies to */
  ns: string
}

// =============================================================================
// Cache Path Generation
// =============================================================================

/**
 * Generate all cache paths for a namespace
 *
 * @param ns - Namespace (e.g., 'posts', 'users')
 * @returns Object containing all paths that should be cached/invalidated
 */
export function getNamespaceCachePaths(ns: string): NamespaceCachePaths {
  return {
    data: `data/${ns}/data.parquet`,
    bloom: `indexes/bloom/${ns}.bloom`,
    forwardRels: `rels/forward/${ns}.parquet`,
    reverseRels: `rels/reverse/${ns}.parquet`,
    metadata: [
      `data/${ns}/data.parquet#footer`,
      `data/${ns}/data.parquet#metadata`,
    ],
  }
}

/**
 * Get all paths as a flat array for a namespace
 */
export function getAllCachePaths(ns: string): string[] {
  const paths = getNamespaceCachePaths(ns)
  return [
    paths.data,
    paths.bloom,
    paths.forwardRels,
    paths.reverseRels,
    ...paths.metadata,
  ]
}

// =============================================================================
// CacheInvalidator Implementation
// =============================================================================

/**
 * Cache invalidator for ParqueDB
 *
 * Handles cache invalidation after DO writes to ensure read consistency.
 * Uses Cloudflare Cache API for direct cache deletion.
 *
 * @example
 * ```typescript
 * const invalidator = new CacheInvalidator(cache)
 *
 * // Invalidate after entity write
 * await invalidator.invalidateNamespace('posts')
 *
 * // Invalidate specific paths
 * await invalidator.invalidatePaths(['data/posts/data.parquet'])
 *
 * // Invalidate with version bump (for version-based cache keys)
 * await invalidator.invalidateWithVersion('posts', newVersion)
 * ```
 */
export class CacheInvalidator {
  /** Cache key prefix for namespacing */
  private readonly cachePrefix = 'https://parquedb/'

  /** Version cache for namespace versions */
  private versionCache = new Map<string, CacheVersion>()

  constructor(
    private cache: Cache,
    private options: {
      /** Log invalidation events */
      logInvalidations?: boolean | undefined
      /** Callback when invalidation occurs */
      onInvalidate?: ((ns: string, paths: string[]) => void) | undefined
    } = {}
  ) {}

  // ===========================================================================
  // Public Methods
  // ===========================================================================

  /**
   * Invalidate all caches for a namespace
   *
   * Called by ParqueDBDO after write operations to ensure consistency.
   *
   * @param ns - Namespace to invalidate
   * @returns Invalidation result
   */
  async invalidateNamespace(ns: string): Promise<InvalidationResult> {
    const startTime = performance.now()
    const paths = getAllCachePaths(ns)

    try {
      const results = await Promise.allSettled(
        paths.map(path => this.deleteCacheEntry(path))
      )

      const entriesDeleted = results.filter(
        r => r.status === 'fulfilled' && r.value
      ).length

      // Bump version for version-based cache keys
      this.bumpVersion(ns)

      // Call callback if provided
      if (this.options.onInvalidate) {
        this.options.onInvalidate(ns, paths)
      }

      // Log if enabled
      if (this.options.logInvalidations) {
        logger.info(`Cache invalidated for namespace '${ns}'`, {
          entriesDeleted,
          paths,
        })
      }

      return {
        entriesDeleted,
        paths,
        success: true,
        durationMs: performance.now() - startTime,
      }
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))
      logger.warn(`Cache invalidation failed for namespace '${ns}'`, { error: err })

      return {
        entriesDeleted: 0,
        paths,
        success: false,
        error: err.message,
        durationMs: performance.now() - startTime,
      }
    }
  }

  /**
   * Invalidate specific cache paths
   *
   * Use for targeted invalidation (e.g., only relationships changed).
   *
   * @param paths - Array of paths to invalidate
   * @returns Invalidation result
   */
  async invalidatePaths(paths: string[]): Promise<InvalidationResult> {
    const startTime = performance.now()

    try {
      const results = await Promise.allSettled(
        paths.map(path => this.deleteCacheEntry(path))
      )

      const entriesDeleted = results.filter(
        r => r.status === 'fulfilled' && r.value
      ).length

      return {
        entriesDeleted,
        paths,
        success: true,
        durationMs: performance.now() - startTime,
      }
    } catch (error: unknown) {
      const err = error instanceof Error ? error : new Error(String(error))

      return {
        entriesDeleted: 0,
        paths,
        success: false,
        error: err.message,
        durationMs: performance.now() - startTime,
      }
    }
  }

  /**
   * Invalidate entity-specific caches
   *
   * For single-entity updates, invalidate data file and potentially
   * trigger incremental updates rather than full namespace invalidation.
   *
   * @param ns - Namespace
   * @param id - Entity ID
   * @returns Invalidation result
   */
  async invalidateEntity(ns: string, _id: string): Promise<InvalidationResult> {
    // For now, entity-level invalidation invalidates the whole namespace
    // because data is stored in namespace-level Parquet files.
    // Future optimization: track row group mappings for surgical invalidation
    return this.invalidateNamespace(ns)
  }

  /**
   * Invalidate relationship caches between two namespaces
   *
   * @param fromNs - Source namespace
   * @param toNs - Target namespace
   * @returns Invalidation result
   */
  async invalidateRelationships(fromNs: string, toNs: string): Promise<InvalidationResult> {
    const paths = [
      `rels/forward/${fromNs}.parquet`,
      `rels/reverse/${toNs}.parquet`,
    ]
    return this.invalidatePaths(paths)
  }

  // ===========================================================================
  // Version-Based Cache Keys
  // ===========================================================================

  /**
   * Get current version for a namespace
   *
   * Version is embedded in cache keys so that old caches automatically
   * become unreachable when version changes.
   *
   * @param ns - Namespace
   * @returns Current version number
   */
  getVersion(ns: string): number {
    return this.versionCache.get(ns)?.version ?? 0
  }

  /**
   * Bump version for a namespace
   *
   * Called after writes to ensure subsequent reads use new cache keys.
   *
   * @param ns - Namespace
   * @returns New version number
   */
  bumpVersion(ns: string): number {
    const current = this.versionCache.get(ns)
    const newVersion = (current?.version ?? 0) + 1

    this.versionCache.set(ns, {
      version: newVersion,
      updatedAt: Date.now(),
      ns,
    })

    return newVersion
  }

  /**
   * Generate a versioned cache key
   *
   * Use for cache keys that should auto-invalidate on writes.
   *
   * @param path - Original path
   * @param ns - Namespace for version lookup
   * @returns Path with version suffix
   */
  getVersionedCacheKey(path: string, ns: string): string {
    const version = this.getVersion(ns)
    return `${path}?v=${version}`
  }

  // ===========================================================================
  // QueryExecutor Integration
  // ===========================================================================

  /**
   * Clear in-memory caches in QueryExecutor
   *
   * QueryExecutor has its own in-memory caches (dataCache, metadataCache, bloomCache)
   * that need to be cleared on invalidation.
   *
   * @param queryExecutor - QueryExecutor instance to clear
   * @param ns - Namespace to clear (or undefined for all)
   */
  clearQueryExecutorCache(
    queryExecutor: { invalidateCache: (ns: string) => void; clearCache?: (() => void) | undefined },
    ns?: string
  ): void {
    if (ns) {
      queryExecutor.invalidateCache(ns)
    } else if (queryExecutor.clearCache) {
      queryExecutor.clearCache()
    }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Delete a single cache entry
   *
   * @param path - Path to delete
   * @returns true if entry was deleted
   */
  private async deleteCacheEntry(path: string): Promise<boolean> {
    const cacheKey = this.createCacheKey(path)
    return this.cache.delete(cacheKey)
  }

  /**
   * Create a cache key from a path
   */
  private createCacheKey(path: string): Request {
    return new Request(`${this.cachePrefix}${path}`)
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a CacheInvalidator instance
 *
 * @param cache - Cloudflare Cache instance
 * @param options - Configuration options
 * @returns Configured CacheInvalidator
 */
export function createCacheInvalidator(
  cache: Cache,
  options?: {
    logInvalidations?: boolean | undefined
    onInvalidate?: ((ns: string, paths: string[]) => void) | undefined
  }
): CacheInvalidator {
  return new CacheInvalidator(cache, options)
}

// =============================================================================
// DO Integration Helper
// =============================================================================

/**
 * Invalidate caches after a DO write operation
 *
 * This is the main integration point for ParqueDBDO to call after writes.
 * It handles:
 * 1. Cache API invalidation (current colo)
 * 2. Version bump for version-based cache keys
 * 3. Logging and metrics
 *
 * @param cache - Cloudflare Cache instance
 * @param ns - Namespace that was modified
 * @param operation - Type of operation ('create' | 'update' | 'delete' | 'link' | 'unlink')
 * @returns Invalidation result
 */
export async function invalidateAfterWrite(
  cache: Cache,
  ns: string,
  operation: 'create' | 'update' | 'delete' | 'link' | 'unlink'
): Promise<InvalidationResult> {
  const invalidator = new CacheInvalidator(cache, {
    logInvalidations: true,
  })

  // For relationship operations, only invalidate relationship caches
  if (operation === 'link' || operation === 'unlink') {
    const paths = getNamespaceCachePaths(ns)
    return invalidator.invalidatePaths([paths.forwardRels, paths.reverseRels])
  }

  // For entity operations, invalidate full namespace
  return invalidator.invalidateNamespace(ns)
}

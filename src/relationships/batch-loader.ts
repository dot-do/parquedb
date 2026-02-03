/**
 * Relationship Batch Loader for N+1 Query Elimination
 *
 * This module provides a dataloader-style batch loading capability
 * for relationships that automatically batches multiple relationship
 * queries within a time window to eliminate the N+1 query problem.
 *
 * @example
 * ```typescript
 * const loader = new RelationshipBatchLoader(db)
 *
 * // These will be batched together
 * const [users1, users2, users3] = await Promise.all([
 *   loader.load('Post', 'post-1', 'author'),
 *   loader.load('Post', 'post-2', 'author'),
 *   loader.load('Post', 'post-3', 'author'),
 * ])
 * ```
 *
 * @packageDocumentation
 */

import type { Entity } from '../types/entity'
import type { GetRelatedOptions, GetRelatedResult } from '../ParqueDB/types'
import { DEFAULT_BATCH_WINDOW_MS, DEFAULT_BATCH_MAX_SIZE } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * A batch load request for a relationship
 */
export interface BatchLoadRequest {
  /** Entity type (e.g., 'Post', 'User') */
  type: string
  /** Entity ID */
  id: string
  /** Relationship field name (e.g., 'author', 'categories') */
  relation: string
}

/**
 * Result of a batch load operation
 */
export interface BatchLoadResult {
  /** The original request */
  request: BatchLoadRequest
  /** Related entities found */
  results: Entity[]
}

/**
 * Options for configuring the batch loader
 */
export interface BatchLoaderOptions {
  /**
   * Time window in milliseconds to collect requests before flushing.
   * Default: 10ms
   */
  windowMs?: number | undefined

  /**
   * Maximum number of requests to batch before forcing a flush.
   * Default: 100
   */
  maxBatchSize?: number | undefined

  /**
   * Whether to deduplicate identical requests.
   * Default: true
   */
  deduplicate?: boolean | undefined
}

/**
 * Internal pending request with promise callbacks
 */
interface PendingRequest extends BatchLoadRequest {
  resolve: (value: Entity[]) => void
  reject: (error: Error) => void
  options?: GetRelatedOptions | undefined
}

/**
 * Interface for the database used by the batch loader
 */
export interface BatchLoaderDB {
  /**
   * Get related entities
   */
  getRelated<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    relationField: string,
    options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>>

  /**
   * Get multiple entities by their IDs (optional optimization)
   */
  getByIds?<T = Record<string, unknown>>(
    namespace: string,
    ids: string[]
  ): Promise<Entity<T>[]>
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Convert type name to namespace (lowercase, pluralized)
 */
function typeToNamespace(type: string): string {
  const lower = type.toLowerCase()
  return lower.endsWith('s') ? lower : lower + 's'
}

/**
 * Generate a unique cache key for a request
 */
function requestKey(type: string, id: string, relation: string): string {
  return `${type}:${id}:${relation}`
}

// =============================================================================
// Batch Loader Implementation
// =============================================================================

/**
 * RelationshipBatchLoader batches multiple relationship load requests
 * together to eliminate N+1 query problems.
 *
 * When you call `load()`, the request is queued and a promise is returned.
 * After a configurable time window (default 10ms), all pending requests
 * are batched together and executed efficiently.
 *
 * @example
 * ```typescript
 * // Create loader with database instance
 * const loader = new RelationshipBatchLoader(db)
 *
 * // Load relationships for multiple entities
 * // These will be batched into a single query
 * const posts = await Promise.all([
 *   loader.load('User', 'user-1', 'posts'),
 *   loader.load('User', 'user-2', 'posts'),
 *   loader.load('User', 'user-3', 'posts'),
 * ])
 *
 * // With custom options
 * const loader = new RelationshipBatchLoader(db, {
 *   windowMs: 20,    // 20ms batching window
 *   maxBatchSize: 50 // Flush after 50 requests
 * })
 * ```
 */
export class RelationshipBatchLoader {
  private db: BatchLoaderDB
  private options: Required<BatchLoaderOptions>

  /**
   * Pending requests grouped by relation type for efficient batching.
   * Key format: "type:relation" (e.g., "Post:author")
   */
  private pending: Map<string, PendingRequest[]> = new Map()

  /**
   * Promise cache for deduplication.
   * Key format: "type:id:relation"
   */
  private promises: Map<string, Promise<Entity[]>> = new Map()

  /**
   * Timer for batching window
   */
  private timer: ReturnType<typeof setTimeout> | null = null

  /**
   * Total pending request count
   */
  private pendingCount = 0

  /**
   * Create a new RelationshipBatchLoader
   *
   * @param db - Database instance with getRelated method
   * @param options - Configuration options
   */
  constructor(db: BatchLoaderDB, options: BatchLoaderOptions = {}) {
    this.db = db
    this.options = {
      windowMs: options.windowMs ?? DEFAULT_BATCH_WINDOW_MS,
      maxBatchSize: options.maxBatchSize ?? DEFAULT_BATCH_MAX_SIZE,
      deduplicate: options.deduplicate ?? true,
    }
  }

  /**
   * Queue a relationship load request for batching.
   *
   * Multiple calls with the same type/id/relation within the batching
   * window will be deduplicated and share the same result.
   *
   * @param type - Entity type (e.g., 'Post')
   * @param id - Entity ID
   * @param relation - Relationship field name (e.g., 'author')
   * @param options - Optional query options
   * @returns Promise resolving to related entities
   *
   * @example
   * ```typescript
   * const authors = await loader.load('Post', 'post-123', 'author')
   * ```
   */
  async load(
    type: string,
    id: string,
    relation: string,
    options?: GetRelatedOptions
  ): Promise<Entity[]> {
    const cacheKey = requestKey(type, id, relation)

    // Return existing promise if deduplicated and already queued
    if (this.options.deduplicate) {
      const existing = this.promises.get(cacheKey)
      if (existing) {
        return existing
      }
    }

    // Create promise for this request
    const promise = new Promise<Entity[]>((resolve, reject) => {
      // Group by type:relation for efficient batching
      const bucketKey = `${type}:${relation}`
      const bucket = this.pending.get(bucketKey) || []

      bucket.push({
        type,
        id,
        relation,
        resolve,
        reject,
        options,
      })

      this.pending.set(bucketKey, bucket)
      this.pendingCount++

      // Schedule flush
      this.scheduleFlush()

      // Force flush if max batch size reached
      if (this.pendingCount >= this.options.maxBatchSize) {
        this.flush()
      }
    })

    // Cache promise for deduplication
    if (this.options.deduplicate) {
      this.promises.set(cacheKey, promise)
    }

    return promise
  }

  /**
   * Load multiple relationships at once.
   * All requests will be batched together.
   *
   * @param requests - Array of batch load requests
   * @returns Promise resolving to array of results
   *
   * @example
   * ```typescript
   * const results = await loader.loadMany([
   *   { type: 'Post', id: 'post-1', relation: 'author' },
   *   { type: 'Post', id: 'post-2', relation: 'author' },
   *   { type: 'User', id: 'user-1', relation: 'posts' },
   * ])
   * ```
   */
  async loadMany(requests: BatchLoadRequest[]): Promise<BatchLoadResult[]> {
    const promises = requests.map(async (request) => {
      const results = await this.load(request.type, request.id, request.relation)
      return { request, results }
    })
    return Promise.all(promises)
  }

  /**
   * Schedule a flush after the batching window
   */
  private scheduleFlush(): void {
    if (this.timer) return

    this.timer = setTimeout(() => {
      // Call flush and catch any errors to prevent unhandled rejections.
      // Individual request errors are handled in processBatch, so this catch
      // is only for unexpected errors in the flush logic itself.
      this.flush().catch(() => {
        // Errors are handled by rejecting individual promises in processBatch
      })
    }, this.options.windowMs)
  }

  /**
   * Flush all pending requests immediately
   */
  private async flush(): Promise<void> {
    // Clear timer
    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = null
    }

    // Atomically swap out the pending collections to prevent race conditions.
    // New requests added during flush processing will go into the new collections
    // and be batched in a subsequent flush.
    const batches = this.pending
    this.pending = new Map()
    this.pendingCount = 0

    // Clear promise cache so new requests get fresh promises
    this.promises = new Map()

    // Process all batches in parallel
    await Promise.all(
      Array.from(batches.entries()).map(([bucketKey, requests]) =>
        this.processBatch(bucketKey, requests)
      )
    )
  }

  /**
   * Process a batch of requests for the same type:relation
   */
  private async processBatch(bucketKey: string, requests: PendingRequest[]): Promise<void> {
    if (requests.length === 0) return

    const [type, relation] = bucketKey.split(':')
    if (!type || !relation) {
      // Invalid bucket key - reject all
      const error = new Error(`Invalid batch key: ${bucketKey}`)
      requests.forEach((req) => req.reject(error))
      return
    }

    const namespace = typeToNamespace(type)

    try {
      // Deduplicate IDs
      const uniqueIds = [...new Set(requests.map((r) => r.id))]
      const idToRequests = new Map<string, PendingRequest[]>()

      for (const req of requests) {
        const existing = idToRequests.get(req.id) || []
        existing.push(req)
        idToRequests.set(req.id, existing)
      }

      // Fetch all related entities in parallel
      // Each unique ID gets one getRelated call
      const results = await Promise.all(
        uniqueIds.map(async (id) => {
          // Strip namespace prefix if present
          const localId = id.includes('/') ? id.split('/').slice(1).join('/') : id
          try {
            const result = await this.db.getRelated(namespace, localId, relation)
            return { id, items: result.items, error: null }
          } catch (err) {
            return { id, items: [], error: err as Error }
          }
        })
      )

      // Build result map
      const resultMap = new Map<string, { items: Entity[]; error: Error | null }>()
      for (const result of results) {
        resultMap.set(result.id, { items: result.items, error: result.error })
      }

      // Resolve all requests
      for (const [id, reqs] of idToRequests) {
        const result = resultMap.get(id)
        if (!result) {
          reqs.forEach((req) => req.resolve([]))
        } else if (result.error) {
          reqs.forEach((req) => req.reject(result.error!))
        } else {
          reqs.forEach((req) => req.resolve(result.items))
        }
      }
    } catch (err) {
      // Batch-level error - reject all
      const error = err instanceof Error ? err : new Error(String(err))
      requests.forEach((req) => req.reject(error))
    }
  }

  /**
   * Clear all pending requests and caches.
   * Useful for cleanup between requests in server environments.
   */
  clear(): void {
    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = null
    }

    // Reject all pending requests
    for (const requests of this.pending.values()) {
      const error = new Error('Batch loader cleared')
      requests.forEach((req) => req.reject(error))
    }

    this.pending.clear()
    this.promises.clear()
    this.pendingCount = 0
  }

  /**
   * Get statistics about the current batch state
   */
  getStats(): {
    pendingRequests: number
    pendingBatches: number
    cachedPromises: number
  } {
    return {
      pendingRequests: this.pendingCount,
      pendingBatches: this.pending.size,
      cachedPromises: this.promises.size,
    }
  }
}

/**
 * Create a relationship batch loader for a database instance
 *
 * @param db - Database instance with getRelated method
 * @param options - Batch loader options
 * @returns New RelationshipBatchLoader instance
 *
 * @example
 * ```typescript
 * const loader = createBatchLoader(db, { windowMs: 20 })
 * const authors = await loader.load('Post', 'post-1', 'author')
 * ```
 */
export function createBatchLoader(
  db: BatchLoaderDB,
  options?: BatchLoaderOptions
): RelationshipBatchLoader {
  return new RelationshipBatchLoader(db, options)
}

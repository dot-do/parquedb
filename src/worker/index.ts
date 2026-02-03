/**
 * ParqueDB Worker - Main worker entrypoint with CQRS architecture
 *
 * CQRS (Command Query Responsibility Segregation):
 * - Writes: Routed through Durable Objects for consistency
 * - Reads: Direct to R2 with Cache API for performance
 *
 * This architecture provides:
 * - Strong consistency for writes via single-writer DO
 * - High read throughput via distributed Workers + caching
 * - Cache invalidation on writes for eventual read consistency
 */

import { WorkerEntrypoint } from 'cloudflare:workers'
import type { Env } from '../types/worker'
import { getDOStubByName } from '../utils/type-utils'
import type { ParqueDBDOStub } from '../types/worker'
import { handleBenchmarkRequest } from './benchmark'
import { handleDatasetBenchmarkRequest } from './benchmark-datasets'
import { handleIndexedBenchmarkRequest } from './benchmark-indexed'
import { handleBackendsBenchmarkRequest } from './benchmark-backends'
import { handleDatasetBackendsBenchmarkRequest } from './benchmark-datasets-backends'
import type { Filter } from '../types/filter'
import type {
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  RelatedOptions,
} from '../types/options'
import type { EntityRecord, UpdateResult, DeleteResult, PaginatedResult } from '../types/entity'
import type { Update } from '../types/update'
import { ReadPath } from './ReadPath'
import { QueryExecutor, FindResult } from './QueryExecutor'
import { DEFAULT_CACHE_CONFIG } from './CacheStrategy'
import { logger } from '../utils/logger'

// Import response helpers
import {
  buildErrorResponse,
  buildCorsPreflightResponse,
  type StorageStats,
} from './responses'

// Import R2 error handling
import {
  MissingBucketError,
  BucketOperationError,
  handleBucketError,
} from './r2-errors'

// Import routing utilities
import { RoutePatterns, matchRoute } from './routing'

// Import public routes handler
import { handlePublicRoutes } from './public-routes'

// Import sync routes handler
import { handleSyncRoutes, handleUpload, handleDownload } from './sync-routes'

// Import handlers
import {
  handleRoot,
  handleHealth,
  handleDebugR2,
  handleDebugEntity,
  handleDebugIndexes,
  handleDebugQuery,
  handleDebugCache,
  handleDatasetsList,
  handleDatasetDetail,
  handleCollectionList,
  handleEntityDetail,
  handleRelationshipTraversal,
  handleNsRoute,
  type HandlerContext,
} from './handlers'

// Re-export for external use
export { ReadPath, NotFoundError, ReadError } from './ReadPath'
export { QueryExecutor } from './QueryExecutor'
export type { CacheConfig } from './CacheStrategy'
export {
  CacheStrategy,
  DEFAULT_CACHE_CONFIG,
  READ_HEAVY_CACHE_CONFIG,
  WRITE_HEAVY_CACHE_CONFIG,
  NO_CACHE_CONFIG,
  createCacheStrategy,
} from './CacheStrategy'

// Re-export response helpers for handler use
export {
  buildResponse,
  buildErrorResponse,
  buildCorsPreflightResponse,
  createTimingContext,
  markTiming,
  measureTiming,
  buildServerTimingHeader,
  type TimingContext,
  type CfProperties,
  type StorageStats,
  type ResponseData,
} from './responses'

// Re-export R2 error handling utilities
export {
  MissingBucketError,
  BucketOperationError,
  requireBucket,
  getCdnBucket,
  hasBucket,
  hasCdnBucket,
  handleBucketError,
  buildMissingBucketResponse,
  buildBucketOperationErrorResponse,
  safeGet,
  safePut,
  safeHead,
  safeList,
  safeDelete,
} from './r2-errors'

// Re-export routing utilities
export {
  parseQueryFilter,
  parseQueryOptions,
  RoutePatterns,
  matchRoute,
  QueryParamError,
} from './routing'

// Re-export datasets config
export { DATASETS, getDataset, getDatasetIds, type DatasetConfig } from './datasets'

// Export Durable Objects for Cloudflare Workers runtime
// These are required for the DOs to be available as bindings
export { ParqueDBDO, type CacheInvalidationSignal } from './ParqueDBDO'
export { MigrationDO } from './MigrationDO'

// Re-export cache invalidation utilities
export {
  CacheInvalidator,
  createCacheInvalidator,
  invalidateAfterWrite,
  getNamespaceCachePaths,
  getAllCachePaths,
  type NamespaceCachePaths,
  type InvalidationResult,
  type CacheVersion,
} from './CacheInvalidation'
export {
  DatabaseIndexDO,
  getUserDatabaseIndex,
  type DatabaseInfo,
  type RegisterDatabaseOptions,
  type UpdateDatabaseOptions,
} from './DatabaseIndexDO'
export {
  RateLimitDO,
  getRateLimiter,
  getClientId,
  buildRateLimitHeaders,
  buildRateLimitResponse,
  DEFAULT_RATE_LIMITS,
  type RateLimitConfig,
  type RateLimitResult,
} from './RateLimitDO'

// Re-export TailDO and related types for event-driven compaction
export {
  TailDO,
  type TailDOEnv,
  type TailWorkerMessage,
  type TailAckMessage,
  type TailErrorMessage,
  type RawEventsFile,
} from './TailDO'

// Re-export subrequest tracking utilities for Snippets compliance monitoring
export {
  countFetchSubrequests,
  countFetchSubrequestsFromUnknown,
  extractFetchSubrequests,
  getSubrequestSummary,
  isSnippetsCompliant,
  SNIPPETS_SUBREQUEST_LIMIT,
  WORKERS_FREE_SUBREQUEST_LIMIT,
  WORKERS_PAID_SUBREQUEST_LIMIT,
  type DiagnosticsChannelEvent,
  type FetchSubrequest,
  type SubrequestSummary,
} from './subrequest-tracking'

// Re-export Compaction Consumer for event-driven mode
export {
  CompactionConsumer,
  createCompactionConsumer,
  type CompactionConsumerEnv,
  type R2EventNotification,
  type DownstreamMessage,
  type ProcessingResult,
  type BatchResult,
} from './compaction-consumer'

// =============================================================================
// Worker Implementation
// =============================================================================

/**
 * ParqueDB Worker with CQRS architecture
 *
 * Provides RPC methods for database operations:
 * - READ operations go directly to R2 with caching
 * - WRITE operations are delegated to Durable Objects
 *
 * @example
 * ```typescript
 * // In your worker
 * export default {
 *   fetch(request, env, ctx) {
 *     const parquedb = new ParqueDBWorker(ctx, env)
 *     // ... handle requests
 *   }
 * }
 *
 * // Via service binding
 * const posts = await env.PARQUEDB_SERVICE.find('posts', { status: 'published' })
 * ```
 */
export class ParqueDBWorker extends WorkerEntrypoint<Env> {
  private readPath!: ReadPath
  private queryExecutor!: QueryExecutor
  #cache!: Cache

  /**
   * Initialize worker with environment bindings
   */
  constructor(ctx: ExecutionContext, env: Env) {
    super(ctx, env)

    // Initialize read path with caching
    // Note: caches.default is accessed via caches.open('default') in workers
    this.initializeCache()
  }

  /**
   * Initialize cache asynchronously
   */
  private async initializeCache(): Promise<void> {
    this.#cache = await caches.open('parquedb')
    this.readPath = new ReadPath(this.env.BUCKET, this.#cache, DEFAULT_CACHE_CONFIG)
    // Set execution context for proper background revalidation lifecycle
    this.readPath.setExecutionContext(this.ctx)
    // Pass R2Buckets and r2.dev URL to QueryExecutor for edge caching
    this.queryExecutor = new QueryExecutor(
      this.readPath,
      this.env.BUCKET,
      this.env.CDN_BUCKET,
      this.env.CDN_R2_DEV_URL
    )
  }

  /**
   * Ensure cache is initialized before operations
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.readPath) {
      await this.initializeCache()
    }
  }

  /**
   * Get storage statistics for debugging
   */
  getStorageStats(): StorageStats {
    return this.queryExecutor?.getStorageStats() || { cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false }
  }

  /**
   * Get relationships from rels.parquet
   *
   * @param dataset - Dataset prefix
   * @param fromId - Source entity ID
   * @param predicate - Optional predicate filter
   * @param options - Optional filters for shredded fields (matchMode, similarity)
   */
  async getRelationships(
    dataset: string,
    fromId: string,
    predicate?: string,
    options?: {
      matchMode?: 'exact' | 'fuzzy'
      minSimilarity?: number
      maxSimilarity?: number
    }
  ): Promise<Array<{
    to_ns: string
    to_id: string
    to_name: string
    to_type: string
    predicate: string
    importance: number | null
    level: number | null
    // Shredded fields for efficient querying
    matchMode: 'exact' | 'fuzzy' | null
    similarity: number | null
  }>> {
    await this.ensureInitialized()
    return this.queryExecutor.getRelationships(dataset, fromId, predicate, options)
  }

  // ===========================================================================
  // READ Operations - Direct to R2 with Cache
  // ===========================================================================

  /**
   * Find entities matching a filter
   *
   * Goes directly to R2 with Cache API for performance.
   * Uses predicate pushdown for efficient Parquet scanning.
   *
   * @param ns - Namespace to query
   * @param filter - MongoDB-style filter
   * @param options - Query options (sort, limit, project, etc.)
   * @returns Matching entities with pagination info
   */
  async find<T = EntityRecord>(
    ns: string,
    filter: Filter = {},
    options: FindOptions<T> = {}
  ): Promise<FindResult<T>> {
    await this.ensureInitialized()
    return this.queryExecutor.find<T>(ns, filter, options)
  }

  /**
   * Get a single entity by ID
   *
   * Uses bloom filter for fast negative lookups.
   *
   * @param ns - Namespace
   * @param id - Entity ID
   * @param options - Get options
   * @returns Entity or null if not found
   */
  async get<T = EntityRecord>(
    ns: string,
    id: string,
    _options: GetOptions = {}
  ): Promise<T | null> {
    await this.ensureInitialized()
    return this.queryExecutor.get<T>(ns, id, _options)
  }

  /**
   * Count entities matching a filter
   *
   * @param ns - Namespace
   * @param filter - Filter to apply
   * @returns Count of matching entities
   */
  async count(ns: string, filter: Filter = {}): Promise<number> {
    await this.ensureInitialized()
    return this.queryExecutor.count(ns, filter)
  }

  /**
   * Check if any entity matches the filter
   *
   * @param ns - Namespace
   * @param filter - Filter to check
   * @returns true if at least one match exists
   */
  async exists(ns: string, filter: Filter): Promise<boolean> {
    await this.ensureInitialized()
    return this.queryExecutor.exists(ns, filter)
  }

  /**
   * Explain query plan without executing
   *
   * @param ns - Namespace
   * @param filter - Filter to analyze
   * @param options - Query options
   * @returns Query plan details
   */
  async explain(ns: string, filter: Filter, options: FindOptions = {}) {
    await this.ensureInitialized()
    return this.queryExecutor.explain(ns, filter, options)
  }

  // ===========================================================================
  // WRITE Operations - Delegate to Durable Object
  // ===========================================================================

  /**
   * Create a new entity
   *
   * Delegates to Durable Object for consistency, then invalidates cache.
   *
   * @param ns - Namespace
   * @param data - Entity data
   * @param options - Create options
   * @returns Created entity
   */
  async create<T = EntityRecord>(
    ns: string,
    data: Partial<T>,
    options: CreateOptions = {}
  ): Promise<T> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    const result = await stub.create(ns, data, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result as T
  }

  /**
   * Update an entity by ID
   *
   * Delegates to Durable Object for consistency, then invalidates cache.
   *
   * @param ns - Namespace
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   * @returns Update result
   */
  async update(
    ns: string,
    id: string,
    update: Update,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    const result = await stub.update(ns, id, update, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result as UpdateResult
  }

  /**
   * Update multiple entities matching a filter
   *
   * @param ns - Namespace
   * @param filter - Filter for entities to update
   * @param update - Update operations
   * @param options - Update options
   * @returns Update result
   */
  async updateMany(
    ns: string,
    filter: Filter,
    update: Update,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    const result = await stub.updateMany(ns, filter, update, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result as UpdateResult
  }

  /**
   * Delete an entity by ID
   *
   * @param ns - Namespace
   * @param id - Entity ID
   * @param options - Delete options
   * @returns Delete result
   */
  async delete(
    ns: string,
    id: string,
    options: DeleteOptions = {}
  ): Promise<DeleteResult> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    const result = await stub.delete(ns, id, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result as DeleteResult
  }

  /**
   * Delete multiple entities matching a filter
   *
   * @param ns - Namespace
   * @param filter - Filter for entities to delete
   * @param options - Delete options
   * @returns Delete result
   */
  async deleteMany(
    ns: string,
    filter: Filter,
    options: DeleteOptions = {}
  ): Promise<DeleteResult> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    const result = await stub.deleteMany(ns, filter, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result as DeleteResult
  }

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  /**
   * Link two entities
   *
   * @param fromNs - Source namespace
   * @param fromId - Source entity ID
   * @param predicate - Relationship name
   * @param toNs - Target namespace
   * @param toId - Target entity ID
   */
  async link(
    fromNs: string,
    fromId: string,
    predicate: string,
    toNs: string,
    toId: string
  ): Promise<void> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    // DO expects entity IDs in "ns/id" format
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, fromNs)
    await stub.link(`${fromNs}/${fromId}`, predicate, `${toNs}/${toId}`)

    // Invalidate relationship caches
    await this.readPath.invalidate([
      `rels/forward/${fromNs}.parquet`,
      `rels/reverse/${toNs}.parquet`,
    ])
  }

  /**
   * Unlink two entities
   *
   * @param fromNs - Source namespace
   * @param fromId - Source entity ID
   * @param predicate - Relationship name
   * @param toNs - Target namespace
   * @param toId - Target entity ID
   */
  async unlink(
    fromNs: string,
    fromId: string,
    predicate: string,
    toNs: string,
    toId: string
  ): Promise<void> {
    await this.ensureInitialized()

    // Delegate to DO via RPC
    // DO expects entity IDs in "ns/id" format
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, fromNs)
    await stub.unlink(`${fromNs}/${fromId}`, predicate, `${toNs}/${toId}`)

    // Invalidate relationship caches
    await this.readPath.invalidate([
      `rels/forward/${fromNs}.parquet`,
      `rels/reverse/${toNs}.parquet`,
    ])
  }

  /**
   * Get related entities
   *
   * @param ns - Source namespace
   * @param id - Source entity ID
   * @param options - Related options
   * @returns Related entities
   */
  async related<T = EntityRecord>(
    ns: string,
    id: string,
    options: RelatedOptions = {}
  ): Promise<PaginatedResult<T>> {
    await this.ensureInitialized()

    // TODO: Implement relationship traversal via R2
    // For now, delegate to DO via RPC
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    return stub.related(ns, id, options) as Promise<PaginatedResult<T>>
  }

  // ===========================================================================
  // Cache Management
  // ===========================================================================

  /**
   * Invalidate cache for a namespace
   *
   * Called internally after writes. Can also be called externally
   * for manual cache invalidation.
   *
   * @param ns - Namespace to invalidate
   */
  async invalidateCache(ns: string): Promise<void> {
    await this.ensureInitialized()
    await this.invalidateCacheForNamespace(ns)
  }

  /**
   * Get cache statistics
   *
   * @returns Current cache stats
   */
  async getCacheStats() {
    await this.ensureInitialized()
    return this.readPath.getStats()
  }

  /**
   * Reset cache statistics
   */
  async resetCacheStats(): Promise<void> {
    await this.ensureInitialized()
    this.readPath.resetStats()
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Invalidate all caches for a namespace
   */
  private async invalidateCacheForNamespace(ns: string): Promise<void> {
    // Invalidate data file
    await this.readPath.invalidate([`data/${ns}/data.parquet`])

    // Invalidate query executor cache
    this.queryExecutor.invalidateCache(ns)

    // Invalidate bloom filter
    await this.readPath.invalidate([`indexes/bloom/${ns}.bloom`])
  }

  // ===========================================================================
  // Cache Invalidation Version Checking
  // ===========================================================================

  /**
   * Get invalidation version for a namespace from DO
   *
   * Workers can use this to check if their caches are stale.
   * Compare with locally tracked version to detect stale caches.
   *
   * @param ns - Namespace to check
   * @returns Current invalidation version from DO
   */
  async getInvalidationVersion(ns: string): Promise<number> {
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    return stub.getInvalidationVersion(ns)
  }

  /**
   * Check if Worker's cache for a namespace is stale
   *
   * Compares the DO's invalidation version with the Worker's tracked version.
   * Returns true if Worker should invalidate its caches.
   *
   * @param ns - Namespace to check
   * @param workerVersion - Worker's last known version for this namespace
   * @returns true if cache is stale and should be invalidated
   */
  async shouldInvalidateCache(ns: string, workerVersion: number): Promise<boolean> {
    const stub = getDOStubByName<ParqueDBDOStub>(this.env.PARQUEDB, ns)
    return stub.shouldInvalidate(ns, workerVersion)
  }

  /**
   * Check cache validity and invalidate if stale
   *
   * This is the main method for CQRS cache coherence. Call before reads
   * to ensure cache consistency with DO writes.
   *
   * @param ns - Namespace to validate
   * @returns Object with wasInvalidated flag and new version
   */
  async validateAndInvalidateCache(ns: string): Promise<{
    wasInvalidated: boolean
    version: number
  }> {
    await this.ensureInitialized()

    // Get current version from DO
    const doVersion = await this.getInvalidationVersion(ns)

    // Get Worker's tracked version (from a local cache)
    const workerVersion = this.getCachedVersion(ns)

    if (doVersion > workerVersion) {
      // Cache is stale - invalidate
      await this.invalidateCacheForNamespace(ns)
      this.setCachedVersion(ns, doVersion)
      return { wasInvalidated: true, version: doVersion }
    }

    return { wasInvalidated: false, version: workerVersion }
  }

  /** Track invalidation versions per namespace locally in Worker */
  private cachedVersions = new Map<string, number>()

  private getCachedVersion(ns: string): number {
    return this.cachedVersions.get(ns) ?? 0
  }

  private setCachedVersion(ns: string, version: number): void {
    this.cachedVersions.set(ns, version)
  }
}

// =============================================================================
// HTTP Handler
// =============================================================================

/**
 * HTTP fetch handler for REST API access
 *
 * Provides REST endpoints for ParqueDB operations:
 * - GET /ns/:namespace - Find entities
 * - GET /ns/:namespace/:id - Get entity by ID
 * - POST /ns/:namespace - Create entity
 * - PATCH /ns/:namespace/:id - Update entity
 * - DELETE /ns/:namespace/:id - Delete entity
 *
 * Also provides dataset browsing:
 * - GET /datasets - List datasets
 * - GET /datasets/:dataset - Dataset detail
 * - GET /datasets/:dataset/:collection - Collection list
 * - GET /datasets/:dataset/:collection/:id - Entity detail
 * - GET /datasets/:dataset/:collection/:id/:predicate - Relationships
 */
export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const startTime = performance.now()
    const url = new URL(request.url)
    const path = url.pathname
    const baseUrl = `${url.protocol}//${url.host}`

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return buildCorsPreflightResponse()
    }

    // Create worker instance
    const worker = new ParqueDBWorker(ctx, env)

    // Create handler context
    const context: HandlerContext = {
      request,
      url,
      baseUrl,
      path,
      worker,
      startTime,
      ctx,
    }

    try {
      // =======================================================================
      // Root - API Overview
      // =======================================================================
      if (path === '/' || path === '') {
        return handleRoot(context)
      }

      // =======================================================================
      // Benchmark - Real R2 I/O Performance Tests
      // =======================================================================
      if (path === '/benchmark') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for benchmark operations.')
        }
        return handleBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleBenchmarkRequest>[1])
      }

      // =======================================================================
      // Benchmark Datasets - Real Dataset I/O Performance Tests
      // =======================================================================
      if (path === '/benchmark-datasets') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for dataset benchmark operations.')
        }
        return handleDatasetBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleDatasetBenchmarkRequest>[1])
      }

      // =======================================================================
      // Benchmark Indexed - Secondary Index Performance Tests
      // =======================================================================
      if (path === '/benchmark-indexed') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for indexed benchmark operations.')
        }
        return handleIndexedBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleIndexedBenchmarkRequest>[1])
      }

      // =======================================================================
      // Benchmark Backends - Compare Native/Iceberg/Delta (Synthetic Data)
      // =======================================================================
      if (path === '/benchmark/backends') {
        if (!env.CDN_BUCKET) {
          throw new MissingBucketError('CDN_BUCKET', 'Required for backend comparison benchmarks.')
        }
        return handleBackendsBenchmarkRequest(request, env.CDN_BUCKET)
      }

      // =======================================================================
      // Benchmark Datasets + Backends - Real Data Across All Formats
      // =======================================================================
      if (path === '/benchmark/datasets/backends') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for dataset backend benchmarks.')
        }
        return handleDatasetBackendsBenchmarkRequest(request, env.BUCKET)
      }

      // =======================================================================
      // Backend Migration - Durable Object-based batch migration
      // Handles migrations in batches to work within subrequest limits (1000/request)
      // Uses DO alarm() to continue processing across invocations
      //
      // Endpoints:
      // - POST /migrate - Start migration { to: 'iceberg'|'delta', namespaces?: string[] }
      // - GET /migrate/status - Get current migration status
      // - POST /migrate/cancel - Cancel running migration
      // - GET /migrate/jobs - List migration history
      // =======================================================================
      if (path.startsWith('/migrate')) {
        if (!env.MIGRATION) {
          return buildErrorResponse(500, 'MIGRATION_DO_NOT_CONFIGURED', 'Migration DO not available')
        }

        const id = env.MIGRATION.idFromName('default')
        const stub = env.MIGRATION.get(id)

        // Forward to Migration DO
        // Map: /migrate -> /migrate (POST starts migration)
        //      /migrate/status -> /status
        //      /migrate/cancel -> /cancel
        //      /migrate/jobs -> /jobs
        let migrationPath = path.replace('/migrate', '')
        if (migrationPath === '' && request.method === 'GET') {
          migrationPath = '/status'
        } else if (migrationPath === '') {
          migrationPath = '/migrate'
        }

        const migrationUrl = new URL(request.url)
        migrationUrl.pathname = migrationPath

        return stub.fetch(new Request(migrationUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: request.body,
        }))
      }

      // =======================================================================
      // Health Check
      // =======================================================================
      if (path === '/health') {
        return handleHealth(context)
      }

      // =======================================================================
      // Debug Endpoints
      // =======================================================================
      if (path === '/debug/r2') {
        return handleDebugR2(context, env)
      }

      if (path === '/debug/entity') {
        return handleDebugEntity(context)
      }

      if (path === '/debug/indexes') {
        return handleDebugIndexes(context, env)
      }

      if (path === '/debug/query') {
        return handleDebugQuery(context)
      }

      if (path === '/debug/cache') {
        return handleDebugCache(context)
      }

      // =======================================================================
      // Sync API Routes (push/pull/sync)
      // =======================================================================
      const syncResponse = await handleSyncRoutes(request, env, path)
      if (syncResponse) {
        return syncResponse
      }

      // Handle sync upload/download endpoints
      const uploadMatch = path.match(/^\/api\/sync\/upload\/([^/]+)\/(.+)$/)
      if (uploadMatch && request.method === 'PUT') {
        const [, databaseId, filePath] = uploadMatch
        return handleUpload(request, env, databaseId!, decodeURIComponent(filePath!))
      }

      const downloadMatch = path.match(/^\/api\/sync\/download\/([^/]+)\/(.+)$/)
      if (downloadMatch && request.method === 'GET') {
        const [, databaseId, filePath] = downloadMatch
        return handleDownload(request, env, databaseId!, decodeURIComponent(filePath!))
      }

      // =======================================================================
      // Public Database Access Routes
      // =======================================================================
      const publicResponse = await handlePublicRoutes(request, env, path, baseUrl)
      if (publicResponse) {
        return publicResponse
      }

      // =======================================================================
      // Datasets Overview
      // =======================================================================
      if (path === '/datasets') {
        return handleDatasetsList(context)
      }

      // =======================================================================
      // Dataset Detail - /datasets/:dataset
      // =======================================================================
      const datasetMatch = matchRoute<[string]>(path, RoutePatterns.dataset)
      if (datasetMatch) {
        const [datasetId] = datasetMatch
        return handleDatasetDetail(context, datasetId)
      }

      // =======================================================================
      // Collection List - /datasets/:dataset/:collection
      // =======================================================================
      const collectionMatch = matchRoute<[string, string]>(path, RoutePatterns.collection)
      if (collectionMatch) {
        const [datasetId, collectionId] = collectionMatch
        return handleCollectionList(context, datasetId, collectionId)
      }

      // =======================================================================
      // Relationship Traversal - /datasets/:dataset/:collection/:id/:predicate
      // (Must be checked before entity detail due to matching order)
      // =======================================================================
      const relMatch = matchRoute<[string, string, string, string]>(path, RoutePatterns.relationship)
      if (relMatch) {
        const [datasetId, collectionId, entityId, predicate] = relMatch
        return handleRelationshipTraversal(context, datasetId, collectionId, decodeURIComponent(entityId), predicate)
      }

      // =======================================================================
      // Entity Detail - /datasets/:dataset/:collection/:id
      // =======================================================================
      const entityMatch = matchRoute<[string, string, string]>(path, RoutePatterns.entity)
      if (entityMatch) {
        const [datasetId, collectionId, entityId] = entityMatch
        return handleEntityDetail(context, datasetId, collectionId, decodeURIComponent(entityId))
      }

      // =======================================================================
      // Legacy /ns routes (backwards compatibility)
      // =======================================================================
      const nsMatch = path.match(RoutePatterns.ns)
      if (nsMatch) {
        const ns = nsMatch[1]!
        const id = nsMatch[2]
        return handleNsRoute(context, ns, id)
      }

      // =======================================================================
      // 404 - Not Found
      // =======================================================================
      return buildErrorResponse(request, new Error(`Route '${path}' not found`), 404, startTime)

    } catch (error: unknown) {
      // Handle R2 bucket errors with specific error responses
      const bucketErrorResponse = handleBucketError(error)
      if (bucketErrorResponse) {
        return bucketErrorResponse
      }

      logger.error('ParqueDB error', error)
      const err = error instanceof Error ? error : new Error(String(error))
      return buildErrorResponse(request, err, 500, startTime)
    }
  },
}

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

// Import rate limiting utilities
import {
  getClientId,
  getEndpointTypeFromPath,
  buildRateLimitResponse,
  addRateLimitHeadersToResponse,
  type RateLimitResult,
} from './rate-limit-utils'
import { type RateLimitDO } from './RateLimitDO'

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

// Export Workflows and supporting DOs
export { CompactionMigrationWorkflow } from '../workflows/compaction-migration'
export { MigrationWorkflow } from '../workflows/migration-workflow'
export { VacuumWorkflow } from '../workflows/vacuum-workflow'
export { CompactionStateDO, handleCompactionQueue } from '../workflows/compaction-queue-consumer'

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
  getEndpointTypeFromPath,
  addRateLimitHeadersToResponse,
  type RateLimitConfig,
  type RateLimitResult,
  type EndpointType,
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
// Rate Limiting Helper
// =============================================================================

/**
 * Check rate limit for the current request
 *
 * @param request - Incoming request
 * @param env - Worker environment
 * @param path - URL pathname
 * @returns Rate limit result, or null if rate limiting is not configured
 */
async function checkRateLimitForRequest(
  request: Request,
  env: Env,
  path: string
): Promise<RateLimitResult | null> {
  // Rate limiting requires RATE_LIMITER binding
  if (!env.RATE_LIMITER) {
    return null
  }

  const clientId = getClientId(request)
  const endpointType = getEndpointTypeFromPath(path, request.method)

  try {
    const rateLimitId = env.RATE_LIMITER.idFromName(clientId)
    const limiter = env.RATE_LIMITER.get(rateLimitId) as unknown as RateLimitDO
    return await limiter.checkLimit(endpointType)
  } catch (error) {
    // If rate limiting fails, log but allow the request through
    // This prevents rate limiter failures from blocking legitimate traffic
    logger.error('Rate limit check failed', error)
    return null
  }
}

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

    // =========================================================================
    // Rate Limiting - Applied at fetch handler level for all routes
    // Excludes: health check (/ and /health) to allow monitoring
    // =========================================================================
    const isExemptFromRateLimit = path === '/' || path === '' || path === '/health'
    let rateLimitResult: RateLimitResult | null = null

    if (!isExemptFromRateLimit) {
      rateLimitResult = await checkRateLimitForRequest(request, env, path)

      // If rate limited, return 429 response immediately
      if (rateLimitResult && !rateLimitResult.allowed) {
        return buildRateLimitResponse(rateLimitResult)
      }
    }

    // Helper to add rate limit headers to successful responses
    const withRateLimitHeaders = (response: Response): Response => {
      if (rateLimitResult) {
        return addRateLimitHeadersToResponse(response, rateLimitResult)
      }
      return response
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
      // Root - API Overview (exempt from rate limiting)
      // =======================================================================
      if (path === '/' || path === '') {
        return handleRoot(context)
      }

      // =======================================================================
      // Benchmark - Real R2 I/O Performance Tests
      // Rate limited via 'benchmark' endpoint type (10 req/min)
      // =======================================================================
      if (path === '/benchmark') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for benchmark operations.')
        }
        const response = await handleBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleBenchmarkRequest>[1])
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Benchmark Datasets - Real Dataset I/O Performance Tests
      // Rate limited via 'benchmark' endpoint type (10 req/min)
      // =======================================================================
      if (path === '/benchmark-datasets') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for dataset benchmark operations.')
        }
        const response = await handleDatasetBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleDatasetBenchmarkRequest>[1])
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Benchmark Indexed - Secondary Index Performance Tests
      // Rate limited via 'benchmark' endpoint type (10 req/min)
      // =======================================================================
      if (path === '/benchmark-indexed') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for indexed benchmark operations.')
        }
        const response = await handleIndexedBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleIndexedBenchmarkRequest>[1])
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Benchmark Backends - Compare Native/Iceberg/Delta (Synthetic Data)
      // Rate limited via 'benchmark' endpoint type (10 req/min)
      // =======================================================================
      if (path === '/benchmark/backends') {
        if (!env.CDN_BUCKET) {
          throw new MissingBucketError('CDN_BUCKET', 'Required for backend comparison benchmarks.')
        }
        const response = await handleBackendsBenchmarkRequest(request, env.CDN_BUCKET)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Benchmark Datasets + Backends - Real Data Across All Formats
      // Rate limited via 'benchmark' endpoint type (10 req/min)
      // =======================================================================
      if (path === '/benchmark/datasets/backends') {
        if (!env.BUCKET) {
          throw new MissingBucketError('BUCKET', 'Required for dataset backend benchmarks.')
        }
        const response = await handleDatasetBackendsBenchmarkRequest(request, env.BUCKET)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Backend Migration - Durable Object-based batch migration
      // Rate limited via 'migration' endpoint type (5 req/min)
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
          return withRateLimitHeaders(buildErrorResponse(request, new Error('Migration DO not available'), 500, startTime))
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

        const response = await stub.fetch(new Request(migrationUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: request.body,
        }))
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Vacuum - Orphaned file cleanup workflow
      // Rate limited via 'vacuum' endpoint type (5 req/min)
      //
      // Endpoints:
      // - POST /vacuum/start - Start vacuum { namespace: string, retentionMs?: number, dryRun?: boolean }
      // - GET /vacuum/status/:id - Get vacuum workflow status
      // =======================================================================
      if (path === '/vacuum/start' && request.method === 'POST') {
        if (!env.VACUUM_WORKFLOW) {
          return withRateLimitHeaders(buildErrorResponse(request, new Error('Vacuum Workflow not available'), 500, startTime))
        }

        try {
          const body = await request.json() as {
            namespace?: string
            format?: 'iceberg' | 'delta' | 'auto'
            retentionMs?: number
            dryRun?: boolean
            warehouse?: string
            database?: string
          }

          if (!body.namespace) {
            return withRateLimitHeaders(buildErrorResponse(
              request,
              new Error('namespace is required'),
              400,
              startTime
            ))
          }

          // Start vacuum workflow
          const instance = await env.VACUUM_WORKFLOW.create({
            params: {
              namespace: body.namespace,
              format: body.format ?? 'auto',
              retentionMs: body.retentionMs ?? 24 * 60 * 60 * 1000, // 24 hours default
              dryRun: body.dryRun ?? false,
              warehouse: body.warehouse ?? '',
              database: body.database ?? '',
            },
          })

          return withRateLimitHeaders(new Response(JSON.stringify({
            success: true,
            workflowId: instance.id,
            message: `Vacuum workflow started for namespace '${body.namespace}'`,
            statusUrl: `/vacuum/status/${instance.id}`,
          }, null, 2), {
            status: 202,
            headers: { 'Content-Type': 'application/json' },
          }))
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error))
          return withRateLimitHeaders(buildErrorResponse(request, err, 500, startTime))
        }
      }

      // GET /vacuum/status/:id - Get vacuum workflow status
      const vacuumStatusMatch = path.match(/^\/vacuum\/status\/([^/]+)$/)
      if (vacuumStatusMatch && request.method === 'GET') {
        if (!env.VACUUM_WORKFLOW) {
          return withRateLimitHeaders(buildErrorResponse(request, new Error('Vacuum Workflow not available'), 500, startTime))
        }

        const workflowId = vacuumStatusMatch[1]!
        try {
          const instance = await env.VACUUM_WORKFLOW.get(workflowId)
          const status = await instance.status()

          return withRateLimitHeaders(new Response(JSON.stringify(status, null, 2), {
            headers: { 'Content-Type': 'application/json' },
          }))
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error))
          return withRateLimitHeaders(buildErrorResponse(request, err, 404, startTime))
        }
      }

      // =======================================================================
      // Compaction Status - Event-driven compaction tracking (namespace-sharded)
      // Rate limited via 'compaction' endpoint type (30 req/min)
      //
      // Query parameters:
      // - namespace: Get status for a specific namespace (recommended)
      // - namespaces: Comma-separated list of namespaces to aggregate
      //
      // Without parameters, returns usage instructions since the DO is now sharded
      // =======================================================================
      if (path === '/compaction/status') {
        if (!env.COMPACTION_STATE) {
          return withRateLimitHeaders(buildErrorResponse(request, new Error('Compaction State DO not available'), 500, startTime))
        }

        const namespaceParam = url.searchParams.get('namespace')
        const namespacesParam = url.searchParams.get('namespaces')

        // Single namespace query - direct to its sharded DO
        if (namespaceParam) {
          const id = env.COMPACTION_STATE.idFromName(namespaceParam)
          const stub = env.COMPACTION_STATE.get(id)
          const response = await stub.fetch(new Request(new URL('/status', request.url).toString()))
          return withRateLimitHeaders(response)
        }

        // Multiple namespaces query - aggregate from multiple DOs
        if (namespacesParam) {
          const namespaces = namespacesParam.split(',').map(ns => ns.trim()).filter(Boolean)
          if (namespaces.length === 0) {
            return withRateLimitHeaders(buildErrorResponse(
              request,
              new Error('namespaces parameter must contain at least one namespace'),
              400,
              startTime
            ))
          }

          // Query all namespace DOs in parallel
          const results = await Promise.all(
            namespaces.map(async (namespace) => {
              const id = env.COMPACTION_STATE.idFromName(namespace)
              const stub = env.COMPACTION_STATE.get(id)
              try {
                const response = await stub.fetch(new Request(new URL('/status', request.url).toString()))
                const data = await response.json() as Record<string, unknown>
                return { namespace, ...data }
              } catch (err) {
                return { namespace, error: err instanceof Error ? err.message : 'Unknown error' }
              }
            })
          )

          // Aggregate statistics
          const aggregated = {
            namespaces: results,
            summary: {
              totalNamespaces: results.length,
              totalActiveWindows: results.reduce((sum, r) => {
                const windows = (r as { activeWindows?: number }).activeWindows ?? 0
                return sum + windows
              }, 0),
              totalKnownWriters: [...new Set(results.flatMap(r => {
                const writers = (r as { knownWriters?: string[] }).knownWriters ?? []
                return writers
              }))],
            },
          }

          return withRateLimitHeaders(new Response(JSON.stringify(aggregated, null, 2), {
            headers: { 'Content-Type': 'application/json' },
          }))
        }

        // No namespace specified - return usage instructions
        // (Can't query 'default' DO anymore since sharding is by namespace)
        return withRateLimitHeaders(new Response(JSON.stringify({
          message: 'CompactionStateDO is sharded by namespace. Please specify a namespace parameter.',
          usage: {
            single: '/compaction/status?namespace=posts',
            multiple: '/compaction/status?namespaces=posts,comments,users',
          },
          note: 'Each namespace has its own CompactionStateDO instance for scalability.',
        }, null, 2), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        }))
      }

      // =======================================================================
      // Compaction Health - Aggregated health check for alerting/monitoring
      // Rate limited via 'compaction' endpoint type (30 req/min)
      //
      // Query parameters:
      // - namespaces: Comma-separated list of namespaces to check (required)
      // - maxPendingWindows: Threshold for degraded status (default: 10)
      // - maxWindowAgeHours: Threshold for degraded status (default: 2)
      //
      // Returns aggregated health status with alerts for monitoring systems
      // =======================================================================
      if (path === '/compaction/health') {
        if (!env.COMPACTION_STATE) {
          return withRateLimitHeaders(buildErrorResponse(request, new Error('Compaction State DO not available'), 500, startTime))
        }

        const namespacesParam = url.searchParams.get('namespaces')
        if (!namespacesParam) {
          return withRateLimitHeaders(new Response(JSON.stringify({
            error: 'namespaces parameter is required',
            usage: '/compaction/health?namespaces=users,posts,comments',
            optional: {
              maxPendingWindows: 'Threshold for degraded status (default: 10)',
              maxWindowAgeHours: 'Threshold for degraded status (default: 2)',
            },
          }, null, 2), {
            status: 400,
            headers: { 'Content-Type': 'application/json' },
          }))
        }

        const namespaces = namespacesParam.split(',').map(ns => ns.trim()).filter(Boolean)
        if (namespaces.length === 0) {
          return withRateLimitHeaders(buildErrorResponse(
            request,
            new Error('namespaces parameter must contain at least one namespace'),
            400,
            startTime
          ))
        }

        // Parse optional config parameters
        const maxPendingWindows = parseInt(url.searchParams.get('maxPendingWindows') ?? '10', 10)
        const maxWindowAgeHours = parseFloat(url.searchParams.get('maxWindowAgeHours') ?? '2')
        const healthConfig = { maxPendingWindows, maxWindowAgeHours }

        // Import health evaluation functions
        const {
          evaluateNamespaceHealth,
          aggregateHealthStatus,
          isCompactionStatusResponse,
        } = await import('../workflows/compaction-queue-consumer')

        // Query all namespace DOs in parallel
        const namespaceHealthMap: Record<string, import('../workflows/compaction-queue-consumer').NamespaceHealth> = {}

        await Promise.all(
          namespaces.map(async (namespace) => {
            const id = env.COMPACTION_STATE.idFromName(namespace)
            const stub = env.COMPACTION_STATE.get(id)
            try {
              const response = await stub.fetch(new Request(new URL('/status', request.url).toString()))
              const data = await response.json()

              if (isCompactionStatusResponse(data)) {
                namespaceHealthMap[namespace] = evaluateNamespaceHealth(namespace, data, healthConfig)
              } else {
                // Namespace has no data yet - treat as healthy
                namespaceHealthMap[namespace] = {
                  namespace,
                  status: 'healthy',
                  metrics: {
                    activeWindows: 0,
                    oldestWindowAge: 0,
                    totalPendingFiles: 0,
                    windowsStuckInProcessing: 0,
                  },
                  issues: [],
                }
              }
            } catch (err) {
              // Error querying namespace - mark as unhealthy
              namespaceHealthMap[namespace] = {
                namespace,
                status: 'unhealthy',
                metrics: {
                  activeWindows: 0,
                  oldestWindowAge: 0,
                  totalPendingFiles: 0,
                  windowsStuckInProcessing: 0,
                },
                issues: [`Failed to query status: ${err instanceof Error ? err.message : 'Unknown error'}`],
              }
            }
          })
        )

        // Aggregate health status
        const healthResponse = aggregateHealthStatus(namespaceHealthMap)

        // Return appropriate HTTP status code based on health
        const httpStatus = healthResponse.status === 'healthy' ? 200 : healthResponse.status === 'degraded' ? 200 : 503

        return withRateLimitHeaders(new Response(JSON.stringify(healthResponse, null, 2), {
          status: httpStatus,
          headers: { 'Content-Type': 'application/json' },
        }))
      }

      // =======================================================================
      // Health Check (exempt from rate limiting for monitoring)
      // =======================================================================
      if (path === '/health') {
        return handleHealth(context)
      }

      // =======================================================================
      // Compaction Dashboard - HTML monitoring page
      // Rate limited via 'compaction' endpoint type (30 req/min)
      //
      // Query parameters:
      // - namespaces: Comma-separated list of namespaces to monitor (required)
      // =======================================================================
      if (path === '/compaction/dashboard') {
        const { generateDashboardHtml } = await import('../observability/compaction')

        const namespacesParam = url.searchParams.get('namespaces')
        if (!namespacesParam) {
          return withRateLimitHeaders(
            new Response(
              JSON.stringify(
                {
                  error: 'namespaces parameter is required',
                  usage: '/compaction/dashboard?namespaces=users,posts,comments',
                },
                null,
                2
              ),
              {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
              }
            )
          )
        }

        const namespaces = namespacesParam
          .split(',')
          .map((ns) => ns.trim())
          .filter(Boolean)
        if (namespaces.length === 0) {
          return withRateLimitHeaders(
            buildErrorResponse(
              request,
              new Error('namespaces parameter must contain at least one namespace'),
              400,
              startTime
            )
          )
        }

        const html = generateDashboardHtml(baseUrl, namespaces)
        return withRateLimitHeaders(
          new Response(html, {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          })
        )
      }

      // =======================================================================
      // Compaction Metrics - Prometheus format export
      // Rate limited via 'compaction' endpoint type (30 req/min)
      //
      // Query parameters:
      // - namespaces: Optional comma-separated list of namespaces to include
      // =======================================================================
      if (path === '/compaction/metrics') {
        const { exportPrometheusMetrics } = await import('../observability/compaction')

        const namespacesParam = url.searchParams.get('namespaces')
        const namespaces = namespacesParam
          ? namespacesParam
              .split(',')
              .map((ns) => ns.trim())
              .filter(Boolean)
          : undefined

        const prometheusOutput = exportPrometheusMetrics(namespaces)
        return withRateLimitHeaders(
          new Response(prometheusOutput, {
            headers: {
              'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
            },
          })
        )
      }

      // =======================================================================
      // Compaction Metrics JSON - JSON time-series export
      // Rate limited via 'compaction' endpoint type (30 req/min)
      //
      // Query parameters:
      // - namespaces: Optional comma-separated list of namespaces to include
      // - since: Optional Unix timestamp (ms) to filter data points from
      // - limit: Optional max data points per series (default: 100)
      // =======================================================================
      if (path === '/compaction/metrics/json') {
        const { exportJsonTimeSeries } = await import('../observability/compaction')

        const namespacesParam = url.searchParams.get('namespaces')
        const namespaces = namespacesParam
          ? namespacesParam
              .split(',')
              .map((ns) => ns.trim())
              .filter(Boolean)
          : undefined

        const sinceParam = url.searchParams.get('since')
        const since = sinceParam ? parseInt(sinceParam, 10) : undefined

        const limitParam = url.searchParams.get('limit')
        const limit = limitParam ? parseInt(limitParam, 10) : 100

        const jsonData = exportJsonTimeSeries(namespaces, since, limit)
        return withRateLimitHeaders(
          new Response(JSON.stringify(jsonData, null, 2), {
            headers: { 'Content-Type': 'application/json' },
          })
        )
      }

      // =======================================================================
      // Debug Endpoints
      // Rate limited via 'debug' endpoint type (30 req/min)
      // =======================================================================
      if (path === '/debug/r2') {
        const response = await handleDebugR2(context, env)
        return withRateLimitHeaders(response)
      }

      if (path === '/debug/entity') {
        const response = await handleDebugEntity(context, env)
        return withRateLimitHeaders(response)
      }

      if (path === '/debug/indexes') {
        const response = await handleDebugIndexes(context, env)
        return withRateLimitHeaders(response)
      }

      if (path === '/debug/query') {
        const response = await handleDebugQuery(context, env)
        return withRateLimitHeaders(response)
      }

      if (path === '/debug/cache') {
        const response = await handleDebugCache(context, env)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Sync API Routes (push/pull/sync)
      // Rate limited via 'sync' endpoint type (100 req/min)
      // Note: sync-routes.ts doesn't apply its own rate limiting, so we apply it here
      // =======================================================================
      const syncResponse = await handleSyncRoutes(request, env, path)
      if (syncResponse) {
        return withRateLimitHeaders(syncResponse)
      }

      // Handle sync upload/download endpoints
      // Rate limited via 'sync_file' endpoint type (500 req/min)
      const uploadMatch = path.match(/^\/api\/sync\/upload\/([^/]+)\/(.+)$/)
      if (uploadMatch && request.method === 'PUT') {
        const [, databaseId, filePath] = uploadMatch
        const response = await handleUpload(request, env, databaseId!, decodeURIComponent(filePath!))
        return withRateLimitHeaders(response)
      }

      const downloadMatch = path.match(/^\/api\/sync\/download\/([^/]+)\/(.+)$/)
      if (downloadMatch && request.method === 'GET') {
        const [, databaseId, filePath] = downloadMatch
        const response = await handleDownload(request, env, databaseId!, decodeURIComponent(filePath!))
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Public Database Access Routes
      // Note: public-routes.ts applies its own rate limiting internally
      // We still add headers here for consistency, but skip if already rate limited
      // =======================================================================
      const publicResponse = await handlePublicRoutes(request, env, path, baseUrl)
      if (publicResponse) {
        // public-routes.ts already handles rate limiting and headers
        return publicResponse
      }

      // =======================================================================
      // Datasets Overview
      // Rate limited via 'datasets' endpoint type (200 req/min)
      // =======================================================================
      if (path === '/datasets') {
        const response = await handleDatasetsList(context)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Dataset Detail - /datasets/:dataset
      // Rate limited via 'datasets' endpoint type (200 req/min)
      // =======================================================================
      const datasetMatch = matchRoute<[string]>(path, RoutePatterns.dataset)
      if (datasetMatch) {
        const [datasetId] = datasetMatch
        const response = await handleDatasetDetail(context, datasetId)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Collection List - /datasets/:dataset/:collection
      // Rate limited via 'datasets' endpoint type (200 req/min)
      // =======================================================================
      const collectionMatch = matchRoute<[string, string]>(path, RoutePatterns.collection)
      if (collectionMatch) {
        const [datasetId, collectionId] = collectionMatch
        const response = await handleCollectionList(context, datasetId, collectionId)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Relationship Traversal - /datasets/:dataset/:collection/:id/:predicate
      // Rate limited via 'datasets' endpoint type (200 req/min)
      // (Must be checked before entity detail due to matching order)
      // =======================================================================
      const relMatch = matchRoute<[string, string, string, string]>(path, RoutePatterns.relationship)
      if (relMatch) {
        const [datasetId, collectionId, entityId, predicate] = relMatch
        const response = await handleRelationshipTraversal(context, datasetId, collectionId, decodeURIComponent(entityId), predicate)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Entity Detail - /datasets/:dataset/:collection/:id
      // Rate limited via 'datasets' endpoint type (200 req/min)
      // =======================================================================
      const entityMatch = matchRoute<[string, string, string]>(path, RoutePatterns.entity)
      if (entityMatch) {
        const [datasetId, collectionId, entityId] = entityMatch
        const response = await handleEntityDetail(context, datasetId, collectionId, decodeURIComponent(entityId))
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // Legacy /ns routes (backwards compatibility)
      // Rate limited via 'ns_read' (300 req/min) or 'ns_write' (60 req/min)
      // based on HTTP method
      // =======================================================================
      const nsMatch = path.match(RoutePatterns.ns)
      if (nsMatch) {
        const ns = nsMatch[1]!
        const id = nsMatch[2]
        const response = await handleNsRoute(context, ns, id)
        return withRateLimitHeaders(response)
      }

      // =======================================================================
      // 404 - Not Found
      // =======================================================================
      return withRateLimitHeaders(buildErrorResponse(request, new Error(`Route '${path}' not found`), 404, startTime))

    } catch (error: unknown) {
      // Handle R2 bucket errors with specific error responses
      const bucketErrorResponse = handleBucketError(error)
      if (bucketErrorResponse) {
        return withRateLimitHeaders(bucketErrorResponse)
      }

      logger.error('ParqueDB error', error)
      const err = error instanceof Error ? error : new Error(String(error))
      return withRateLimitHeaders(buildErrorResponse(request, err, 500, startTime))
    }
  },

  /**
   * Queue handler for R2 event notifications
   * Batches events and triggers compaction workflows when windows are ready
   */
  async queue(
    batch: MessageBatch<import('../workflows/compaction-queue-consumer').R2EventMessage>,
    env: Env,
    _ctx: ExecutionContext
  ): Promise<void> {
    const { handleCompactionQueue } = await import('../workflows/compaction-queue-consumer')

    await handleCompactionQueue(batch, env as Parameters<typeof handleCompactionQueue>[1], {
      windowSizeMs: 60 * 60 * 1000, // 1 hour windows
      minFilesToCompact: 10,
      maxWaitTimeMs: 5 * 60 * 1000, // 5 minute grace period
      targetFormat: 'iceberg', // Progressive migration to Iceberg
      namespacePrefix: 'data/',
    })
  },
}

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
import pluralize from 'pluralize'
import type { Env } from '../types/worker'
import { handleBenchmarkRequest } from './benchmark'
import { handleDatasetBenchmarkRequest } from './benchmark-datasets'
import { handleIndexedBenchmarkRequest } from './benchmark-indexed'
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

// Export Durable Object for Cloudflare Workers runtime
// This is required for the DO to be available as a binding
export { ParqueDBDO } from './ParqueDBDO'

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
  getStorageStats(): { cdnHits: number; primaryHits: number; edgeHits: number; cacheHits: number; totalReads: number; usingCdn: boolean; usingEdge: boolean } {
    return this.queryExecutor?.getStorageStats() || { cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false }
  }

  /**
   * Get relationships from rels.parquet
   */
  async getRelationships(
    dataset: string,
    fromId: string,
    predicate?: string
  ): Promise<Array<{
    to_ns: string
    to_id: string
    to_name: string
    to_type: string
    predicate: string
    importance: number | null
    level: number | null
  }>> {
    await this.ensureInitialized()
    return this.queryExecutor.getRelationships(dataset, fromId, predicate)
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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      create(ns: string, data: unknown, options?: unknown): Promise<T>
    }
    const result = await stub.create(ns, data, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result
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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      update(ns: string, id: string, update: unknown, options?: unknown): Promise<UpdateResult>
    }
    const result = await stub.update(ns, id, update, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result
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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      updateMany(ns: string, filter: unknown, update: unknown, options?: unknown): Promise<UpdateResult>
    }
    const result = await stub.updateMany(ns, filter, update, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result
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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      delete(ns: string, id: string, options?: unknown): Promise<DeleteResult>
    }
    const result = await stub.delete(ns, id, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result
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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      deleteMany(ns: string, filter: unknown, options?: unknown): Promise<DeleteResult>
    }
    const result = await stub.deleteMany(ns, filter, options)

    // Invalidate cache after write
    await this.invalidateCacheForNamespace(ns)

    return result
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
    const doId = this.env.PARQUEDB.idFromName(fromNs)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      link(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
    }
    await stub.link(fromNs, fromId, predicate, toNs, toId)

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
    const doId = this.env.PARQUEDB.idFromName(fromNs)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      unlink(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
    }
    await stub.unlink(fromNs, fromId, predicate, toNs, toId)

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
    const doId = this.env.PARQUEDB.idFromName(ns)
    const stub = this.env.PARQUEDB.get(doId) as unknown as {
      related(ns: string, id: string, options?: unknown): Promise<PaginatedResult<T>>
    }
    return stub.related(ns, id, options)
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
}

// =============================================================================
// HTTP Handler (Optional)
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
 */
// =============================================================================
// Example Datasets Configuration
// =============================================================================

const DATASETS = {
  imdb: {
    name: 'IMDB',
    description: 'Internet Movie Database - 7M+ titles, ratings, cast & crew',
    collections: ['titles', 'names', 'ratings', 'principals', 'crew'],
    source: 'https://datasets.imdbws.com/',
    prefix: 'imdb', // R2 path prefix
  },
  'onet-graph': {
    name: 'O*NET',
    description: 'Occupational Information Network - 1,016 occupations with skills, abilities, knowledge relationships',
    collections: ['occupations', 'skills', 'abilities', 'knowledge'],
    source: 'https://www.onetcenter.org/database.html',
    prefix: 'onet-graph', // R2 path prefix
    // Relationship predicates for graph navigation
    predicates: {
      occupations: ['skills', 'abilities', 'knowledge'],
      skills: ['requiredBy'],
      abilities: ['requiredBy'],
      knowledge: ['requiredBy'],
    },
    // Singular form of predicates (for *Scores field lookup)
    singular: {
      skills: 'skill',
      abilities: 'ability',
      knowledge: 'knowledge',
      requiredBy: 'requiredBy',
    },
  },
  'onet-optimized': {
    name: 'O*NET (Optimized)',
    description: 'O*NET with optimized single-column format for fast lookups',
    collections: ['occupations', 'skills', 'abilities', 'knowledge'],
    source: 'https://www.onetcenter.org/database.html',
    prefix: 'onet-optimized',
  },
  unspsc: {
    name: 'UNSPSC',
    description: 'United Nations Standard Products and Services Code - Product taxonomy',
    collections: ['segments', 'families', 'classes', 'commodities'],
    source: 'https://www.unspsc.org/',
    prefix: 'unspsc',
  },
  wikidata: {
    name: 'Wikidata',
    description: 'Structured knowledge base - Entities, properties, claims',
    collections: ['entities', 'properties'],
    source: 'https://www.wikidata.org/',
    prefix: 'wikidata',
  },
}

// =============================================================================
// Response Helpers
// =============================================================================

interface CfProperties {
  colo?: string
  country?: string
  city?: string
  region?: string
  timezone?: string
  latitude?: string
  longitude?: string
  asn?: number
  asOrganization?: string
}

// Timing context for Server-Timing headers
interface TimingContext {
  startTime: number
  marks: Map<string, number>
  durations: Map<string, number>
}

function createTimingContext(): TimingContext {
  return {
    startTime: performance.now(),
    marks: new Map(),
    durations: new Map(),
  }
}

function markTiming(ctx: TimingContext, name: string): void {
  ctx.marks.set(name, performance.now())
}

function measureTiming(ctx: TimingContext, name: string, startMark?: string): void {
  const start = startMark ? ctx.marks.get(startMark) : ctx.startTime
  if (start !== undefined) {
    ctx.durations.set(name, performance.now() - start)
  }
}

function buildServerTimingHeader(ctx: TimingContext): string {
  const parts: string[] = []

  // Add total time
  const total = performance.now() - ctx.startTime
  parts.push(`total;dur=${total.toFixed(1)}`)

  // Add individual durations
  for (const [name, dur] of ctx.durations) {
    parts.push(`${name};dur=${dur.toFixed(1)}`)
  }

  return parts.join(', ')
}

function buildResponse(
  request: Request,
  data: {
    api: Record<string, unknown>
    links: Record<string, string>
    data?: unknown
    items?: unknown[]
    stats?: Record<string, unknown>
    relationships?: Record<string, unknown>
  },
  timing?: TimingContext | number,
  storageStats?: { cdnHits: number; primaryHits: number; edgeHits: number; cacheHits: number; totalReads: number; usingCdn: boolean; usingEdge: boolean }
): Response {
  const cf = (request.cf || {}) as CfProperties & { httpProtocol?: string }

  // Handle both old startTime number and new TimingContext
  const startTime = typeof timing === 'number' ? timing : timing?.startTime
  const timingCtx = typeof timing === 'object' ? timing : undefined

  // Calculate latency
  const latency = startTime ? Math.round(performance.now() - startTime) : undefined

  // Format timestamp in user's timezone
  const now = new Date()
  const requestedAt = cf.timezone
    ? now.toLocaleString('en-US', { timeZone: cf.timezone, hour12: false }).replace(',', '')
    : now.toISOString()

  // Get IP and ray from headers
  const ip = request.headers.get('cf-connecting-ip') || request.headers.get('x-forwarded-for')?.split(',')[0]
  const rayId = request.headers.get('cf-ray')?.split('-')[0]
  const ray = rayId && cf.colo ? `${rayId}-${cf.colo}` : rayId

  // Build timing info for response body
  const timingInfo: Record<string, string> | undefined = timingCtx ? {} : undefined
  if (timingCtx && timingInfo) {
    for (const [name, dur] of timingCtx.durations) {
      timingInfo[name] = `${dur.toFixed(0)}ms`
    }
  }

  const response = {
    api: data.api,
    links: data.links,
    ...(data.data !== undefined ? { data: data.data } : {}),
    ...(data.relationships !== undefined ? { relationships: data.relationships } : {}),
    ...(data.items !== undefined ? { items: data.items } : {}),
    ...(data.stats !== undefined ? { stats: data.stats } : {}),
    user: {
      ip,
      ray,
      colo: cf.colo,
      country: cf.country,
      city: cf.city,
      region: cf.region,
      timezone: cf.timezone,
      requestedAt,
      ...(latency !== undefined ? { latency: `${latency}ms` } : {}),
      ...(timingInfo && Object.keys(timingInfo).length > 0 ? { timing: timingInfo } : {}),
      ...(storageStats?.totalReads || storageStats?.cacheHits ? {
        storage: {
          cacheHits: storageStats.cacheHits,
          edgeHits: storageStats.edgeHits,
          cdnHits: storageStats.cdnHits,
          primaryHits: storageStats.primaryHits,
          totalReads: storageStats.totalReads,
          usingEdge: storageStats.usingEdge,
          usingCdn: storageStats.usingCdn,
        }
      } : {}),
    },
  }

  // Build headers
  const headers: Record<string, string> = {
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=60',
  }

  // Add Server-Timing header if we have timing context
  if (timingCtx) {
    headers['Server-Timing'] = buildServerTimingHeader(timingCtx)
  }

  return Response.json(response, { headers })
}

function buildErrorResponse(
  request: Request,
  error: Error,
  status: number = 500,
  startTime?: number
): Response {
  const cf = (request.cf || {}) as CfProperties

  // Calculate latency
  const latency = startTime ? Math.round(performance.now() - startTime) : undefined

  // Format timestamp in user's timezone
  const now = new Date()
  const requestedAt = cf.timezone
    ? now.toLocaleString('en-US', { timeZone: cf.timezone, hour12: false }).replace(',', '')
    : now.toISOString()

  // Get IP and ray from headers
  const ip = request.headers.get('cf-connecting-ip') || request.headers.get('x-forwarded-for')?.split(',')[0]
  const rayId = request.headers.get('cf-ray')?.split('-')[0]
  const ray = rayId && cf.colo ? `${rayId}-${cf.colo}` : rayId

  return Response.json({
    api: {
      error: true,
      message: error.message,
      status,
    },
    links: {
      home: '/',
      datasets: '/datasets',
    },
    user: {
      ip,
      ray,
      colo: cf.colo,
      country: cf.country,
      city: cf.city,
      region: cf.region,
      timezone: cf.timezone,
      requestedAt,
      ...(latency !== undefined ? { latency: `${latency}ms` } : {}),
    },
  }, {
    status,
    headers: {
      'Access-Control-Allow-Origin': '*',
    },
  })
}

// =============================================================================
// HTTP Handler
// =============================================================================

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
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      })
    }

    // Create worker instance
    const worker = new ParqueDBWorker(ctx, env)

    try {
      // =======================================================================
      // Root - API Overview
      // =======================================================================
      if (path === '/' || path === '') {
        return buildResponse(request, {
          api: {
            name: 'ParqueDB',
            version: '0.1.0',
            description: 'A hybrid relational/document/graph database built on Apache Parquet',
            documentation: 'https://github.com/parquedb/parquedb',
          },
          links: {
            self: baseUrl,
            datasets: `${baseUrl}/datasets`,
            imdb: `${baseUrl}/datasets/imdb`,
            onet: `${baseUrl}/datasets/onet`,
            health: `${baseUrl}/health`,
            benchmark: `${baseUrl}/benchmark`,
            benchmarkDatasets: `${baseUrl}/benchmark-datasets`,
            benchmarkIndexed: `${baseUrl}/benchmark-indexed`,
          },
        }, startTime)
      }

      // =======================================================================
      // Benchmark - Real R2 I/O Performance Tests
      // =======================================================================
      if (path === '/benchmark') {
        // Use type assertion to the minimal R2Bucket interface expected by benchmark
        return handleBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleBenchmarkRequest>[1])
      }

      // =======================================================================
      // Benchmark Datasets - Real Dataset I/O Performance Tests
      // =======================================================================
      if (path === '/benchmark-datasets') {
        // Test real dataset files from data-v3 uploaded to R2
        return handleDatasetBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleDatasetBenchmarkRequest>[1])
      }

      // =======================================================================
      // Benchmark Indexed - Secondary Index Performance Tests
      // =======================================================================
      if (path === '/benchmark-indexed') {
        // Compare indexed vs scan query performance across real datasets
        return handleIndexedBenchmarkRequest(request, env.BUCKET as Parameters<typeof handleIndexedBenchmarkRequest>[1])
      }

      // =======================================================================
      // Health Check
      // =======================================================================
      if (path === '/health') {
        return buildResponse(request, {
          api: {
            status: 'healthy',
            uptime: 'ok',
            storage: 'r2',
            compute: 'durable-objects',
          },
          links: {
            home: baseUrl,
            datasets: `${baseUrl}/datasets`,
          },
        }, startTime)
      }

      // =======================================================================
      // Debug R2 endpoint
      // =======================================================================
      if (path === '/debug/r2') {
        const testPath = url.searchParams.get('path') || 'data/onet/skills/data.parquet'
        const headResult = await env.BUCKET.head(testPath)
        const listResult = await env.BUCKET.list({ prefix: 'data/onet/', limit: 10 })

        return Response.json({
          testPath,
          exists: !!headResult,
          size: headResult?.size,
          etag: headResult?.etag,
          objects: listResult.objects.map(o => ({ key: o.key, size: o.size })),
        })
      }

      // Debug: raw entity data
      if (path === '/debug/entity') {
        const ns = url.searchParams.get('ns') || 'onet-graph/occupations'
        const id = url.searchParams.get('id') || '11-1011.00'
        const entity = await worker.get(ns, id)
        return Response.json({
          ns,
          id,
          entity,
          keys: entity ? Object.keys(entity) : null,
        })
      }

      // Debug: index selection
      if (path === '/debug/indexes') {
        const dataset = url.searchParams.get('dataset') || 'onet-full/occupations'
        const filter = JSON.parse(url.searchParams.get('filter') || '{"$index_jobZone": 3}')

        const { IndexCache, createR2IndexStorageAdapter } = await import('./IndexCache')
        const indexCache = new IndexCache(createR2IndexStorageAdapter(env.BUCKET as Parameters<typeof createR2IndexStorageAdapter>[0]))

        // Load catalog
        const catalog = await indexCache.loadCatalog(dataset)

        // Select index
        const selected = await indexCache.selectIndex(dataset, filter)

        return Response.json({
          dataset,
          filter,
          catalogPath: `${dataset}/indexes/_catalog.json`,
          catalogEntries: catalog,
          selectedIndex: selected ? { type: selected.type, name: selected.entry.name, field: selected.entry.field } : null,
        })
      }

      // Debug: run a query with full diagnostics
      if (path === '/debug/query') {
        const dataset = url.searchParams.get('dataset') || 'benchmark-data/onet-full/occupations'
        const filter = JSON.parse(url.searchParams.get('filter') || '{"$index_jobZone": 3}')
        const limit = parseInt(url.searchParams.get('limit') || '10')

        try {
          const start = performance.now()
          const result = await worker.find(dataset, filter, { limit })
          const elapsed = performance.now() - start

          return Response.json({
            dataset,
            filter,
            limit,
            elapsedMs: Math.round(elapsed),
            resultCount: result.items?.length ?? 0,
            stats: result.stats,
            firstItem: result.items?.[0] ?? null,
          })
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error)
          const stack = error instanceof Error ? error.stack : undefined
          return Response.json({
            dataset,
            filter,
            error: message,
            stack,
          }, { status: 500 })
        }
      }

      // Debug: cache stats
      if (path === '/debug/cache') {
        const cacheStats = await worker.getCacheStats()
        return Response.json({
          cacheStats,
          timestamp: new Date().toISOString(),
        })
      }

      // =======================================================================
      // Datasets Overview
      // =======================================================================
      if (path === '/datasets') {
        const datasetLinks: Record<string, string> = { self: `${baseUrl}/datasets` }
        const datasetList = Object.entries(DATASETS).map(([key, ds]) => {
          datasetLinks[key] = `${baseUrl}/datasets/${key}`
          return {
            id: key,
            ...ds,
            href: `${baseUrl}/datasets/${key}`,
          }
        })

        return buildResponse(request, {
          api: {
            resource: 'datasets',
            description: 'Available example datasets',
            count: datasetList.length,
          },
          links: {
            home: baseUrl,
            ...datasetLinks,
          },
          items: datasetList,
        }, startTime)
      }

      // =======================================================================
      // Dataset Detail - /datasets/:dataset
      // =======================================================================
      const datasetMatch = path.match(/^\/datasets\/([^/]+)$/)
      if (datasetMatch) {
        const datasetId = datasetMatch[1]!
        const dataset = DATASETS[datasetId as keyof typeof DATASETS]

        if (!dataset) {
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404, startTime)
        }

        const collectionLinks: Record<string, string> = {}
        for (const col of dataset.collections) {
          collectionLinks[col] = `${baseUrl}/datasets/${datasetId}/${col}`
        }

        // Build collections as object map {name: href} or array [{name, href}] with ?arrays
        const useArrays = url.searchParams.has('arrays')
        const collectionsData = useArrays
          ? dataset.collections.map(col => ({
              name: col,
              href: `${baseUrl}/datasets/${datasetId}/${col}`,
            }))
          : Object.fromEntries(dataset.collections.map(col => [
              col,
              `${baseUrl}/datasets/${datasetId}/${col}`,
            ]))

        return buildResponse(request, {
          api: {
            resource: 'dataset',
            id: datasetId,
            name: dataset.name,
            description: dataset.description,
            source: dataset.source,
          },
          links: {
            self: `${baseUrl}/datasets/${datasetId}`,
            home: baseUrl,
            datasets: `${baseUrl}/datasets`,
            ...collectionLinks,
          },
          data: {
            collections: collectionsData,
          },
        }, startTime)
      }

      // =======================================================================
      // Collection List - /datasets/:dataset/:collection
      // =======================================================================
      const collectionMatch = path.match(/^\/datasets\/([^/]+)\/([^/]+)$/)
      if (collectionMatch) {
        const datasetId = collectionMatch[1]!
        const collectionId = collectionMatch[2]!
        const dataset = DATASETS[datasetId as keyof typeof DATASETS]

        if (!dataset) {
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404, startTime)
        }

        // Map dataset/collection to namespace using prefix
        const prefix = (dataset as { prefix?: string }).prefix || datasetId
        const ns = `${prefix}/${collectionId}`

        const filter = parseQueryFilter(url.searchParams)
        const options = parseQueryOptions(url.searchParams)
        if (!options.limit) options.limit = 20

        const result = await worker.find(ns, filter, options)

        // Build enriched items with href links
        const enrichedItems: unknown[] = []
        const itemLinks: Record<string, string> = {}

        // Get known predicates for this collection
        const datasetPredicates = (dataset as { predicates?: Record<string, string[]> }).predicates
        const knownPredicates = datasetPredicates?.[collectionId] || []

        if (result.items) {
          for (const item of result.items) {
            const entity = item as Record<string, unknown>
            const entityId = entity.$id || entity.id
            if (entityId) {
              const localId = String(entityId).split('/').pop() || ''
              const href = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(localId)}`

              // Add to quick links (first 10)
              if (Object.keys(itemLinks).length < 10) {
                const linkName = String(entity.name || localId)
                itemLinks[linkName] = href
              }

              // Find relationship predicates - check for JSON-encoded relationships
              const predicates: string[] = []
              for (const predicate of knownPredicates) {
                const rawValue = entity[predicate]
                if (rawValue) {
                  // Parse JSON-encoded relationship data
                  try {
                    const parsed = typeof rawValue === 'string' ? JSON.parse(rawValue) : rawValue
                    if (parsed && typeof parsed === 'object' && parsed.$count > 0) {
                      predicates.push(predicate)
                    }
                  } catch {
                    // Not JSON, check if it's an object
                    if (typeof rawValue === 'object') {
                      predicates.push(predicate)
                    }
                  }
                }
              }

              // Build relationship links
              const relLinks: Record<string, string> = {}
              for (const pred of predicates) {
                relLinks[pred] = `${href}/${pred}`
              }

              // Enrich item with href and relationship links
              enrichedItems.push({
                $id: entity.$id,
                $type: entity.$type,
                name: entity.name,
                description: entity.description,
                // Show relationship counts
                ...(predicates.length > 0 ? {
                  _relationships: predicates.map(p => {
                    const rawValue = entity[p]
                    try {
                      const parsed = typeof rawValue === 'string' ? JSON.parse(rawValue) : rawValue
                      return { predicate: p, count: parsed?.$count || 0 }
                    } catch {
                      return { predicate: p, count: 0 }
                    }
                  }),
                } : {}),
                _links: {
                  self: href,
                  ...relLinks,
                },
              })
            } else {
              enrichedItems.push(item)
            }
          }
        }

        // Build pagination links with different limits
        const currentLimit = options.limit || 20
        const currentSkip = options.skip || 0
        const basePath = `${baseUrl}${path}`
        const useArrays = url.searchParams.has('arrays')
        const arrayParam = useArrays ? '&arrays' : ''

        const paginationLinks: Record<string, string> = {}

        // Add limit option links
        const limitOptions = [20, 50, 100, 500, 1000]
        for (const limit of limitOptions) {
          if (limit !== currentLimit) {
            paginationLinks[`limit${limit}`] = `${basePath}?limit=${limit}${arrayParam}`
          }
        }

        // Add next/prev links if applicable
        if (result.hasMore) {
          const nextCursor = (result.stats as unknown as Record<string, unknown>)?.nextCursor
          if (nextCursor) {
            paginationLinks.next = `${basePath}?cursor=${nextCursor}&limit=${currentLimit}${arrayParam}`
          } else {
            paginationLinks.next = `${basePath}?skip=${currentSkip + currentLimit}&limit=${currentLimit}${arrayParam}`
          }
        }
        if (currentSkip > 0) {
          const prevSkip = Math.max(0, currentSkip - currentLimit)
          paginationLinks.prev = `${basePath}?skip=${prevSkip}&limit=${currentLimit}${arrayParam}`
        }

        return buildResponse(request, {
          api: {
            resource: 'collection',
            dataset: datasetId,
            collection: collectionId,
            filter: Object.keys(filter).length > 0 ? filter : undefined,
            limit: options.limit,
            skip: options.skip,
            returned: enrichedItems.length,
            hasMore: result.hasMore,
          },
          links: {
            self: `${baseUrl}${path}${url.search}`,
            dataset: `${baseUrl}/datasets/${datasetId}`,
            home: baseUrl,
            ...paginationLinks,
            ...itemLinks,
          },
          items: enrichedItems,
          stats: result.stats as unknown as Record<string, unknown>,
        }, startTime)
      }

      // =======================================================================
      // Entity Detail - /datasets/:dataset/:collection/:id
      // GENERIC: Reads from data.parquet + rels.parquet, no hardcoded config
      // =======================================================================
      const entityMatch = path.match(/^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)$/)
      if (entityMatch) {
        const timing = createTimingContext()
        const datasetId = entityMatch[1]!
        const collectionId = entityMatch[2]!
        const entityId = decodeURIComponent(entityMatch[3]!)

        // Check Cache API for cached data response (bypasses all data processing)
        markTiming(timing, 'cache_check_start')
        const cache = await caches.open('parquedb-responses')
        const cacheKey = new Request(`https://parquedb/entity/${datasetId}/${collectionId}/${entityId}`)
        const cachedResponse = await cache.match(cacheKey)
        measureTiming(timing, 'cache_check', 'cache_check_start')

        if (cachedResponse) {
          // Return cached response with updated timing info
          const cachedData = await cachedResponse.json() as { api: unknown; links: unknown; data: unknown; relationships: unknown }
          return buildResponse(request, cachedData as Parameters<typeof buildResponse>[1], timing, worker.getStorageStats())
        }

        // Construct the full $id (e.g., "knowledge/2.C.2.b")
        const fullId = `${collectionId}/${entityId}`

        // PARALLEL: Fetch entity and relationships simultaneously (2 subrequests)
        markTiming(timing, 'parallel_start')
        const [entityResult, allRels] = await Promise.all([
          worker.find<EntityRecord>(datasetId, { $id: fullId }, { limit: 1 }),
          worker.getRelationships(datasetId, entityId),
        ])
        measureTiming(timing, 'parallel', 'parallel_start')

        if (entityResult.items.length === 0) {
          return buildErrorResponse(request, new Error(`Entity '${fullId}' not found in ${datasetId}`), 404, startTime)
        }

        const entity = entityResult.items[0]
        const entityRaw = entity as unknown as Record<string, unknown>

        // Group relationships by predicate
        const relationships: Record<string, {
          count: number
          href: string
          items: Array<{ name: string; href: string; importance?: number; level?: number }>
        }> = {}

        for (const rel of allRels) {
          if (!relationships[rel.predicate]) {
            relationships[rel.predicate] = {
              count: 0,
              href: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/${rel.predicate}`,
              items: [],
            }
          }
          relationships[rel.predicate].count++
          relationships[rel.predicate].items.push({
            name: rel.to_name,
            href: `${baseUrl}/datasets/${datasetId}/${rel.to_ns}/${encodeURIComponent(rel.to_id)}`,
            ...(rel.importance ? { importance: rel.importance } : {}),
            ...(rel.level ? { level: rel.level } : {}),
          })
        }

        // Sort items by importance (descending) within each predicate
        for (const pred of Object.keys(relationships)) {
          relationships[pred].items.sort((a, b) => (b.importance || 0) - (a.importance || 0))
        }

        // Build response
        const useArrays = url.searchParams.has('arrays')
        const selfUrl = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`

        // Convert to object map format unless ?arrays is set
        const relWithItems: Record<string, Record<string, string>> = {}
        for (const [pred, rel] of Object.entries(relationships)) {
          const itemsMap: Record<string, string> = { $id: rel.href }
          for (const item of rel.items) {
            itemsMap[item.name] = item.href
          }
          relWithItems[pred] = itemsMap
        }

        // Build response data
        const responseData = {
          api: {
            resource: 'entity',
            dataset: datasetId,
            collection: collectionId,
            id: entityId,
            type: entityRaw.$type,
          },
          links: {
            self: selfUrl,
            collection: `${baseUrl}/datasets/${datasetId}/${collectionId}`,
            dataset: `${baseUrl}/datasets/${datasetId}`,
            home: baseUrl,
          },
          data: {
            $id: selfUrl,
            $type: entityRaw.$type,
            ...entityRaw,  // Include all entity fields
          },
          relationships: Object.keys(relationships).length > 0
            ? (useArrays ? relationships : relWithItems)
            : undefined,
        }

        // Cache the response data for 1 hour (without user-specific info)
        ctx.waitUntil(
          cache.put(cacheKey, Response.json(responseData, {
            headers: { 'Cache-Control': 'public, max-age=3600' }
          }))
        )

        return buildResponse(request, responseData, timing, worker.getStorageStats())
      }

      // =======================================================================
      // Relationship Traversal - /datasets/:dataset/:collection/:id/:predicate
      // GENERIC: Reads from rels.parquet, no hardcoded config
      // =======================================================================
      const relMatch = path.match(/^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)\/([^/]+)$/)
      if (relMatch) {
        const timing = createTimingContext()
        const datasetId = relMatch[1]!
        const collectionId = relMatch[2]!
        const entityId = decodeURIComponent(relMatch[3]!)
        const predicate = relMatch[4]!

        // Read relationships from rels.parquet
        markTiming(timing, 'rels_start')
        const rels = await worker.getRelationships(datasetId, entityId, predicate)
        measureTiming(timing, 'rels', 'rels_start')

        // Convert to display format
        const items = rels.map(rel => ({
          $id: `${baseUrl}/datasets/${datasetId}/${rel.to_ns}/${encodeURIComponent(rel.to_id)}`,
          name: rel.to_name,
          type: rel.to_type,
          ...(rel.importance ? { importance: rel.importance } : {}),
          ...(rel.level ? { level: rel.level } : {}),
        }))

        // Sort by importance
        items.sort((a, b) => (b.importance || 0) - (a.importance || 0))

        // Pagination
        const limit = parseInt(url.searchParams.get('limit') || '100')
        const skip = parseInt(url.searchParams.get('skip') || '0')
        const paginatedItems = items.slice(skip, skip + limit)

        return buildResponse(request, {
          api: {
            resource: 'relationships',
            dataset: datasetId,
            collection: collectionId,
            id: entityId,
            predicate,
            count: items.length,
          },
          links: {
            self: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/${predicate}`,
            entity: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`,
          },
          items: paginatedItems,
        }, timing, worker.getStorageStats())
      }

      // =======================================================================
      // Legacy /ns routes (backwards compatibility)
      // =======================================================================
      const nsMatch = path.match(/^\/ns\/([^/]+)(?:\/([^/]+))?$/)
      if (nsMatch) {
        const ns = nsMatch[1]!
        const id = nsMatch[2]

        switch (request.method) {
          case 'GET': {
            if (id) {
              const entity = await worker.get(ns, id)
              if (!entity) {
                return buildErrorResponse(request, new Error(`Entity not found`), 404, startTime)
              }
              return buildResponse(request, {
                api: { resource: 'entity', namespace: ns, id },
                links: {
                  self: `${baseUrl}${path}`,
                  collection: `${baseUrl}/ns/${ns}`,
                  home: baseUrl,
                },
                data: entity,
              }, startTime)
            } else {
              const filter = parseQueryFilter(url.searchParams)
              const options = parseQueryOptions(url.searchParams)
              const result = await worker.find(ns, filter, options)
              return buildResponse(request, {
                api: { resource: 'collection', namespace: ns },
                links: {
                  self: `${baseUrl}${path}`,
                  home: baseUrl,
                },
                items: result.items,
                stats: result.stats as unknown as Record<string, unknown>,
              }, startTime)
            }
          }

          case 'POST': {
            const data = (await request.json()) as Partial<EntityRecord>
            const entity = await worker.create(ns, data)
            return Response.json(entity, { status: 201 })
          }

          case 'PATCH': {
            if (!id) {
              return buildErrorResponse(request, new Error('ID required for update'), 400, startTime)
            }
            const updateData = (await request.json()) as Update
            const result = await worker.update(ns, id, updateData)
            return Response.json(result)
          }

          case 'DELETE': {
            if (!id) {
              return buildErrorResponse(request, new Error('ID required for delete'), 400, startTime)
            }
            const result = await worker.delete(ns, id)
            return Response.json(result)
          }

          default:
            return buildErrorResponse(request, new Error('Method not allowed'), 405, startTime)
        }
      }

      // =======================================================================
      // 404 - Not Found
      // =======================================================================
      return buildErrorResponse(request, new Error(`Route '${path}' not found`), 404, startTime)

    } catch (error: unknown) {
      logger.error('ParqueDB error', error)
      const err = error instanceof Error ? error : new Error(String(error))
      return buildErrorResponse(request, err, 500, startTime)
    }
  },
}

// =============================================================================
// Query Parsing Helpers
// =============================================================================

/**
 * Parse filter from URL search params
 */
function parseQueryFilter(params: URLSearchParams): Filter {
  const filterParam = params.get('filter')
  if (filterParam) {
    try {
      return JSON.parse(filterParam)
    } catch {
      return {}
    }
  }
  return {}
}

/**
 * Parse options from URL search params
 */
function parseQueryOptions(params: URLSearchParams): FindOptions {
  const options: FindOptions = {}

  const limit = params.get('limit')
  if (limit) options.limit = parseInt(limit, 10)

  const skip = params.get('skip')
  if (skip) options.skip = parseInt(skip, 10)

  const cursor = params.get('cursor')
  if (cursor) options.cursor = cursor

  const sort = params.get('sort')
  if (sort) {
    try {
      options.sort = JSON.parse(sort)
    } catch {
      // Parse simple format: "field:asc,field2:desc"
      const sortSpec: Record<string, 1 | -1> = {}
      for (const part of sort.split(',')) {
        const [field, dir] = part.split(':')
        if (field) {
          sortSpec[field] = dir === 'desc' ? -1 : 1
        }
      }
      options.sort = sortSpec
    }
  }

  const project = params.get('project')
  if (project) {
    try {
      options.project = JSON.parse(project)
    } catch {
      // Parse simple format: "field1,field2,-field3"
      const projection: Record<string, 0 | 1> = {}
      for (const field of project.split(',')) {
        if (field.startsWith('-')) {
          projection[field.slice(1)] = 0
        } else if (field) {
          projection[field] = 1
        }
      }
      options.project = projection
    }
  }

  return options
}

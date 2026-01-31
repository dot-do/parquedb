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
    // Pass R2Bucket to QueryExecutor for real Parquet reading
    this.queryExecutor = new QueryExecutor(this.readPath, this.env.BUCKET)
  }

  /**
   * Ensure cache is initialized before operations
   */
  private async ensureInitialized(): Promise<void> {
    if (!this.readPath) {
      await this.initializeCache()
    }
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

function buildResponse(
  request: Request,
  data: {
    api: Record<string, unknown>
    links: Record<string, string>
    data?: unknown
    items?: unknown[]
    stats?: Record<string, unknown>
    relationships?: Record<string, unknown>
  }
): Response {
  const cf = (request.cf || {}) as CfProperties

  const response = {
    api: data.api,
    links: data.links,
    ...(data.data !== undefined ? { data: data.data } : {}),
    ...(data.relationships !== undefined ? { relationships: data.relationships } : {}),
    ...(data.items !== undefined ? { items: data.items } : {}),
    ...(data.stats !== undefined ? { stats: data.stats } : {}),
    user: {
      colo: cf.colo,
      country: cf.country,
      city: cf.city,
      region: cf.region,
      timezone: cf.timezone,
      coordinates: cf.latitude && cf.longitude ? {
        lat: parseFloat(cf.latitude),
        lng: parseFloat(cf.longitude),
      } : undefined,
      asn: cf.asn,
      asOrganization: cf.asOrganization,
      requestedAt: new Date().toISOString(),
    },
  }

  return Response.json(response, {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=60',
    },
  })
}

function buildErrorResponse(
  request: Request,
  error: Error,
  status: number = 500
): Response {
  const cf = (request.cf || {}) as CfProperties

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
      colo: cf.colo,
      country: cf.country,
      requestedAt: new Date().toISOString(),
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
          },
        })
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
        })
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
        })
      }

      // =======================================================================
      // Dataset Detail - /datasets/:dataset
      // =======================================================================
      const datasetMatch = path.match(/^\/datasets\/([^/]+)$/)
      if (datasetMatch) {
        const datasetId = datasetMatch[1]!
        const dataset = DATASETS[datasetId as keyof typeof DATASETS]

        if (!dataset) {
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404)
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
        })
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
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404)
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
            ...(result.hasMore ? { next: `${baseUrl}${path}?cursor=${(result.stats as unknown as Record<string, unknown>)?.nextCursor || ''}&limit=${options.limit}` } : {}),
            ...itemLinks,
          },
          items: enrichedItems,
          stats: result.stats as unknown as Record<string, unknown>,
        })
      }

      // =======================================================================
      // Entity Detail - /datasets/:dataset/:collection/:id
      // =======================================================================
      const entityMatch = path.match(/^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)$/)
      if (entityMatch) {
        const datasetId = entityMatch[1]!
        const collectionId = entityMatch[2]!
        const entityId = decodeURIComponent(entityMatch[3]!)
        const dataset = DATASETS[datasetId as keyof typeof DATASETS]

        if (!dataset) {
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404)
        }

        // Use prefix for R2 path
        const prefix = (dataset as { prefix?: string }).prefix || datasetId
        const ns = `${prefix}/${collectionId}`
        const entity = await worker.get(ns, entityId) as EntityRecord | null

        if (!entity) {
          return buildErrorResponse(request, new Error(`Entity '${entityId}' not found in ${ns}`), 404)
        }

        const entityRaw = entity as unknown as Record<string, unknown>

        // Get known predicates for this collection
        const datasetPredicates = (dataset as { predicates?: Record<string, string[]> }).predicates
        const knownPredicates = datasetPredicates?.[collectionId] || []

        // Parse relationships and build clickable links
        const relationships: Record<string, {
          count: number
          href: string
          items?: Array<{ name: string; href: string; importance?: number; level?: number }>
        }> = {}

        // Get singular forms from config (with pluralize as fallback)
        const singularConfig = (dataset as { singular?: Record<string, string> }).singular || {}
        const toSingular = (word: string): string => {
          if (singularConfig[word]) return singularConfig[word]
          return pluralize.singular(word)
        }

        for (const predicate of knownPredicates) {
          const rawValue = entityRaw[predicate]
          // Scores key: "skills" -> "skillScores" (singular from config)
          const singularPredicate = toSingular(predicate)
          const scoresKey = `${singularPredicate}Scores`
          const rawScores = entityRaw[scoresKey]

          if (rawValue) {
            try {
              // Parse JSON-encoded relationship
              const parsed = typeof rawValue === 'string' ? JSON.parse(rawValue) : rawValue
              const scores = rawScores ? (typeof rawScores === 'string' ? JSON.parse(rawScores) : rawScores) : {}

              if (parsed && typeof parsed === 'object') {
                const count = parsed.$count || 0
                const relHref = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/${predicate}`

                // Build items with links and scores
                const items: Array<{ name: string; href: string; importance?: number; level?: number }> = []
                for (const [name, targetId] of Object.entries(parsed)) {
                  if (name.startsWith('$')) continue
                  if (typeof targetId === 'string' && targetId.includes('/')) {
                    const [targetCollection, ...idParts] = targetId.split('/')
                    const targetLocalId = idParts.join('/')
                    const itemScores = scores[name] || {}
                    items.push({
                      name,
                      href: `${baseUrl}/datasets/${datasetId}/${targetCollection}/${encodeURIComponent(targetLocalId)}`,
                      ...(itemScores.importance ? { importance: itemScores.importance } : {}),
                      ...(itemScores.level ? { level: itemScores.level } : {}),
                    })
                  }
                }

                // Sort items by importance (descending) and always include them
                items.sort((a, b) => (b.importance || 0) - (a.importance || 0))

                relationships[predicate] = {
                  count,
                  href: relHref,
                  items,  // Always include items for entity detail view
                }
              }
            } catch {
              // Not valid JSON, skip
            }
          }
        }

        // For skills/abilities/knowledge, compute reverse "requiredBy" relationship
        // by scanning occupations that reference this entity
        const shortId = String(entityRaw.$id).split('/').pop() || entityId

        if (['skills', 'abilities', 'knowledge'].includes(collectionId) && knownPredicates.includes('requiredBy')) {
          const occupationsNs = `${prefix}/occupations`
          const occupationsResult = await worker.find(occupationsNs, {}, { limit: 2000 })

          const reverseItems: Array<{ name: string; href: string; importance?: number; level?: number }> = []
          const entityLocalId = String(entityRaw.elementId || shortId || entityId)

          for (const occ of occupationsResult.items) {
            const occRaw = occ as unknown as Record<string, unknown>
            const relField = occRaw[collectionId]  // e.g., occupations.skills
            const scoresField = occRaw[`${toSingular(collectionId)}Scores`]

            if (relField) {
              try {
                const parsed = typeof relField === 'string' ? JSON.parse(relField) : relField
                const scores = scoresField ? (typeof scoresField === 'string' ? JSON.parse(scoresField) : scoresField) : {}

                // Check if this occupation has this skill/ability/knowledge
                for (const [displayName, targetId] of Object.entries(parsed as Record<string, unknown>)) {
                  if (displayName.startsWith('$')) continue
                  if (typeof targetId === 'string') {
                    const targetLocalId = targetId.split('/').pop()
                    if (targetLocalId === entityLocalId || targetId === `${collectionId}/${entityLocalId}`) {
                      const occCode = String(occRaw.code || occRaw.$id).split('/').pop()
                      const itemScores = (scores as Record<string, { importance?: number; level?: number }>)[displayName] || {}
                      reverseItems.push({
                        name: String(occRaw.name),
                        href: `${baseUrl}/datasets/${datasetId}/occupations/${encodeURIComponent(occCode || '')}`,
                        ...(itemScores.importance ? { importance: itemScores.importance } : {}),
                        ...(itemScores.level ? { level: itemScores.level } : {}),
                      })
                      break  // Found this entity in this occupation, move to next
                    }
                  }
                }
              } catch {
                // Skip invalid JSON
              }
            }
          }

          // Sort by importance (descending)
          reverseItems.sort((a, b) => (b.importance || 0) - (a.importance || 0))

          if (reverseItems.length > 0) {
            // Include top items by importance (already sorted)
            relationships['requiredBy'] = {
              count: reverseItems.length,
              href: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}/requiredBy`,
              items: reverseItems,  // Always include - they're sorted by importance
            }
          }
        }

        // Build relationship links as object map {name: href} for each predicate
        const useArrays = url.searchParams.has('arrays')
        const selfUrl = `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`

        // Convert relationships to rich format with items as object maps
        const relWithItems: Record<string, Record<string, string>> = {}
        for (const [pred, rel] of Object.entries(relationships)) {
          // Build object map {name: href} for items
          const itemsMap: Record<string, string> = {
            $id: rel.href,  // Link to full relationship list
          }
          if (rel.items) {
            for (const item of rel.items) {
              itemsMap[item.name] = item.href
            }
          }
          // If no items were included (count > 10), add a $count indicator
          if (!rel.items && rel.count > 0) {
            itemsMap.$count = String(rel.count)
          }
          relWithItems[pred] = itemsMap
        }

        // Build the response with relationships prominently featured
        // $id is now the full clickable URL, id is the short form (computed earlier)
        return buildResponse(request, {
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
            $id: selfUrl,  // Full clickable URL
            $type: entityRaw.$type,
            id: shortId,   // Short form for display/reference
            name: entityRaw.name,
            description: entityRaw.description,
            ...(entityRaw.code ? { code: entityRaw.code } : {}),
            ...(entityRaw.elementId ? { elementId: entityRaw.elementId } : {}),
          },
          // Relationships with items as object maps {name: href}
          // ?arrays gives the original format with count/items arrays
          relationships: Object.keys(relationships).length > 0
            ? (useArrays ? relationships : relWithItems)
            : undefined,
        })
      }

      // =======================================================================
      // Relationship Traversal - /datasets/:dataset/:collection/:id/:predicate
      // =======================================================================
      const relMatch = path.match(/^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)\/([^/]+)$/)
      if (relMatch) {
        const datasetId = relMatch[1]!
        const collectionId = relMatch[2]!
        const entityId = decodeURIComponent(relMatch[3]!)
        const predicate = relMatch[4]!
        const dataset = DATASETS[datasetId as keyof typeof DATASETS]

        if (!dataset) {
          return buildErrorResponse(request, new Error(`Dataset '${datasetId}' not found`), 404)
        }

        // Use prefix for R2 path
        const prefix = (dataset as { prefix?: string }).prefix || datasetId
        const ns = `${prefix}/${collectionId}`
        const entity = await worker.get(ns, entityId) as EntityRecord | null

        if (!entity) {
          return buildErrorResponse(request, new Error(`Entity '${entityId}' not found`), 404)
        }

        const entityRaw = entity as unknown as Record<string, unknown>

        // Get singular forms from config (with pluralize as fallback)
        const singularConfig = (dataset as { singular?: Record<string, string> }).singular || {}
        const toSingular = (word: string): string => {
          if (singularConfig[word]) return singularConfig[word]
          return pluralize.singular(word)
        }

        // Special handling for "requiredBy" reverse relationship on skills/abilities/knowledge
        if (predicate === 'requiredBy' && ['skills', 'abilities', 'knowledge'].includes(collectionId)) {
          const occupationsNs = `${prefix}/occupations`
          const occupationsResult = await worker.find(occupationsNs, {}, { limit: 2000 })

          const useArrays = url.searchParams.has('arrays')
          const items: Array<{ $id: string; id: string; name: string; importance?: number; level?: number }> = []
          const itemsMap: Record<string, string> = {}
          const entityLocalId = String(entityRaw.elementId || String(entityRaw.$id).split('/').pop() || entityId)

          for (const occ of occupationsResult.items) {
            const occRaw = occ as unknown as Record<string, unknown>
            const relField = occRaw[collectionId]
            const scoresField = occRaw[`${toSingular(collectionId)}Scores`]

            if (relField) {
              try {
                const parsed = typeof relField === 'string' ? JSON.parse(relField) : relField
                const scores = scoresField ? (typeof scoresField === 'string' ? JSON.parse(scoresField) : scoresField) : {}

                for (const [displayName, targetId] of Object.entries(parsed as Record<string, unknown>)) {
                  if (displayName.startsWith('$')) continue
                  if (typeof targetId === 'string') {
                    const targetLocalId = targetId.split('/').pop()
                    if (targetLocalId === entityLocalId || targetId === `${collectionId}/${entityLocalId}`) {
                      const occCode = String(occRaw.code || String(occRaw.$id).split('/').pop())
                      const occName = String(occRaw.name)
                      const fullUrl = `${baseUrl}/datasets/${datasetId}/occupations/${encodeURIComponent(occCode)}`
                      const itemScores = (scores as Record<string, { importance?: number; level?: number }>)[displayName] || {}

                      items.push({
                        $id: fullUrl,
                        id: `occupations/${occCode}`,
                        name: occName,
                        ...(itemScores.importance ? { importance: itemScores.importance } : {}),
                        ...(itemScores.level ? { level: itemScores.level } : {}),
                      })
                      itemsMap[occName] = fullUrl
                      break
                    }
                  }
                }
              } catch {
                // Skip invalid JSON
              }
            }
          }

          // Sort by importance (descending)
          items.sort((a, b) => (b.importance || 0) - (a.importance || 0))

          return buildResponse(request, {
            api: {
              resource: 'relationship',
              dataset: datasetId,
              collection: collectionId,
              entityId,
              predicate: 'requiredBy',
              count: items.length,
            },
            links: {
              self: `${baseUrl}${path}`,
              entity: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`,
              collection: `${baseUrl}/datasets/${datasetId}/${collectionId}`,
              dataset: `${baseUrl}/datasets/${datasetId}`,
              home: baseUrl,
            },
            data: useArrays ? items : itemsMap,
            ...(useArrays ? {} : { items }),
          })
        }

        // Get the relationship field (may be JSON-encoded)
        const rawRelField = entityRaw[predicate]
        // Scores key: "skills" -> "skillScores" (singular), "abilities" -> "abilityScores"
        const singularPredicate = singularConfig[predicate] || pluralize.singular(predicate)
        const scoresKey = `${singularPredicate}Scores`
        const rawScores = entityRaw[scoresKey]

        if (!rawRelField) {
          return buildErrorResponse(request, new Error(`Relationship '${predicate}' not found on entity`), 404)
        }

        // Parse JSON-encoded relationship
        let relObj: Record<string, unknown>
        let scores: Record<string, { importance?: number; level?: number }> = {}
        try {
          relObj = typeof rawRelField === 'string' ? JSON.parse(rawRelField) : rawRelField as Record<string, unknown>
          if (rawScores) {
            const parsedScores = typeof rawScores === 'string' ? JSON.parse(rawScores) : rawScores
            scores = parsedScores as Record<string, { importance?: number; level?: number }>
          }
        } catch (e) {
          console.error('Parse error:', e)
          return buildErrorResponse(request, new Error(`Invalid relationship data for '${predicate}'`), 500)
        }

        const count = relObj.$count as number | undefined

        // Build linked items with scores - $id is now full URL
        const useArrays = url.searchParams.has('arrays')
        const items: Array<{ $id: string; id: string; name: string; importance?: number; level?: number }> = []
        const itemsMap: Record<string, string> = {}  // {name: $id} for object format

        for (const [displayName, targetId] of Object.entries(relObj)) {
          if (displayName.startsWith('$')) continue
          if (typeof targetId === 'string' && targetId.includes('/')) {
            const [targetNs, ...idParts] = targetId.split('/')
            const targetLocalId = idParts.join('/')
            const itemScores = scores[displayName] || {}
            const fullUrl = `${baseUrl}/datasets/${datasetId}/${targetNs}/${encodeURIComponent(targetLocalId)}`

            items.push({
              $id: fullUrl,  // Full clickable URL
              id: targetId,  // Short form like "skills/2.B.4.e"
              name: displayName,
              ...(itemScores.importance !== undefined ? { importance: itemScores.importance } : {}),
              ...(itemScores.level !== undefined ? { level: itemScores.level } : {}),
            })

            itemsMap[displayName] = fullUrl
          }
        }

        // Sort by importance (descending) for better UX
        items.sort((a, b) => (b.importance || 0) - (a.importance || 0))

        return buildResponse(request, {
          api: {
            resource: 'relationship',
            dataset: datasetId,
            collection: collectionId,
            entityId,
            predicate,
            count: count || items.length,
          },
          links: {
            self: `${baseUrl}${path}`,
            entity: `${baseUrl}/datasets/${datasetId}/${collectionId}/${encodeURIComponent(entityId)}`,
            collection: `${baseUrl}/datasets/${datasetId}/${collectionId}`,
            dataset: `${baseUrl}/datasets/${datasetId}`,
            home: baseUrl,
          },
          // Return {name: $id} object by default, or array with ?arrays
          data: useArrays ? items : itemsMap,
          // Include full items array for detail when using arrays format
          ...(useArrays ? {} : { items }),
        })
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
                return buildErrorResponse(request, new Error(`Entity not found`), 404)
              }
              return buildResponse(request, {
                api: { resource: 'entity', namespace: ns, id },
                links: {
                  self: `${baseUrl}${path}`,
                  collection: `${baseUrl}/ns/${ns}`,
                  home: baseUrl,
                },
                data: entity,
              })
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
              })
            }
          }

          case 'POST': {
            const data = (await request.json()) as Partial<EntityRecord>
            const entity = await worker.create(ns, data)
            return Response.json(entity, { status: 201 })
          }

          case 'PATCH': {
            if (!id) {
              return buildErrorResponse(request, new Error('ID required for update'), 400)
            }
            const updateData = (await request.json()) as Update
            const result = await worker.update(ns, id, updateData)
            return Response.json(result)
          }

          case 'DELETE': {
            if (!id) {
              return buildErrorResponse(request, new Error('ID required for delete'), 400)
            }
            const result = await worker.delete(ns, id)
            return Response.json(result)
          }

          default:
            return buildErrorResponse(request, new Error('Method not allowed'), 405)
        }
      }

      // =======================================================================
      // 404 - Not Found
      // =======================================================================
      return buildErrorResponse(request, new Error(`Route '${path}' not found`), 404)

    } catch (error) {
      console.error('ParqueDB error:', error)
      return buildErrorResponse(request, error as Error, 500)
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

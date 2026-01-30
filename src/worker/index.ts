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
    this.queryExecutor = new QueryExecutor(this.readPath)
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
export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Create worker instance
    const worker = new ParqueDBWorker(ctx, env)

    try {
      // Parse path: /ns/:namespace[/:id]
      const match = path.match(/^\/ns\/([^/]+)(?:\/([^/]+))?$/)
      if (!match) {
        return new Response('Not Found', { status: 404 })
      }

      const ns = match[1]
      const id = match[2]

      if (!ns) {
        return new Response('Namespace required', { status: 400 })
      }

      switch (request.method) {
        case 'GET': {
          if (id) {
            // Get single entity
            const entity = await worker.get(ns, id)
            if (!entity) {
              return new Response('Not Found', { status: 404 })
            }
            return Response.json(entity)
          } else {
            // Find entities
            const filter = parseQueryFilter(url.searchParams)
            const options = parseQueryOptions(url.searchParams)
            const result = await worker.find(ns, filter, options)
            return Response.json(result)
          }
        }

        case 'POST': {
          // Create entity
          const data = (await request.json()) as Partial<EntityRecord>
          const entity = await worker.create(ns, data)
          return Response.json(entity, { status: 201 })
        }

        case 'PATCH': {
          if (!id) {
            return new Response('ID required for update', { status: 400 })
          }
          const updateData = (await request.json()) as Update
          const result = await worker.update(ns, id, updateData)
          return Response.json(result)
        }

        case 'DELETE': {
          if (!id) {
            return new Response('ID required for delete', { status: 400 })
          }
          const result = await worker.delete(ns, id)
          return Response.json(result)
        }

        default:
          return new Response('Method Not Allowed', { status: 405 })
      }
    } catch (error) {
      console.error('ParqueDB error:', error)
      return new Response(
        JSON.stringify({ error: (error as Error).message }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      )
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

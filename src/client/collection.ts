/**
 * CollectionClient - RPC client for a specific collection/namespace
 *
 * Alternative implementation using capnweb patterns for RpcPromise chaining.
 * Use this when you need server-side .map() operations.
 *
 * For standard usage, use the Collection interface from ParqueDBClient.
 */

import type {
  Entity,
  EntityId,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  PaginatedResult,
  DeleteResult,
} from '../types'
import type { RpcTargetMarker, RpcPromiseMarker } from '../types/integrations'
import { createRpcPromise, type RpcService } from './rpc-promise'
import { chainRpcPromise } from '../types/cast'

// Re-export RpcService for backwards compatibility
export type { RpcService }

// =============================================================================
// Types
// =============================================================================

/**
 * Create input for collection operations
 */
export interface CollectionCreateInput<T = Record<string, unknown>> {
  /** Entity type */
  $type: string
  /** Display name */
  name: string
  /** Additional fields */
  [key: string]: unknown
}

// =============================================================================
// CollectionClient Class
// =============================================================================

/**
 * Client for interacting with a single collection/namespace
 *
 * This implementation uses RpcPromise for chainable operations with
 * server-side .map() support. For standard async/await usage, use
 * the Collection interface from ParqueDBClient.
 *
 * Implements RpcTargetMarker to enable pass-by-reference semantics
 * when used with capnweb RPC.
 *
 * @example
 * ```typescript
 * // Create client with RpcPromise support
 * const client: RpcService = { ... }
 * const posts = new CollectionClient<Post>(client, 'posts')
 *
 * // Chain with map (single RPC call)
 * const titles = await posts
 *   .find({ status: 'published' })
 *   .map(p => p.title)
 *
 * // Server-side filtering and mapping
 * const authorNames = await posts
 *   .find({ status: 'published' })
 *   .map(p => p.author)
 *   .map(a => a.name)
 * ```
 *
 * @typeParam T - Entity data type
 */
export class CollectionClient<T = unknown> implements RpcTargetMarker {
  readonly __rpcTarget = true as const

  constructor(
    private client: RpcService,
    private ns: string
  ) {}

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Find entities matching a filter
   *
   * Returns an RpcPromise that supports chaining with `.map()`
   * for server-side iteration without transferring full datasets.
   *
   * @example
   * ```typescript
   * // Simple find
   * const posts = await collection.find({ status: 'published' })
   *
   * // With options
   * const posts = await collection.find(
   *   { status: 'published' },
   *   { sort: { createdAt: -1 }, limit: 10 }
   * )
   *
   * // Chain with map (processed on server)
   * const titles = await collection
   *   .find({ status: 'published' })
   *   .map(p => p.title)
   * ```
   *
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @returns RpcPromise of paginated results
   */
  find(filter?: Filter, options?: FindOptions): RpcPromiseMarker<PaginatedResult<Entity<T>>> {
    return this.rpc('find', [this.ns, filter, options])
  }

  /**
   * Get a single entity by ID
   *
   * @example
   * ```typescript
   * // Full EntityId
   * const post = await collection.get('posts/123')
   *
   * // Just the ID part
   * const post = await collection.get('123')
   *
   * // With options
   * const post = await collection.get('123', {
   *   hydrate: ['author', 'categories']
   * })
   * ```
   *
   * @param id - Entity ID (full EntityId or just the id part)
   * @param options - Get options
   * @returns RpcPromise of entity or null
   */
  get(id: EntityId | string, options?: GetOptions): RpcPromiseMarker<Entity<T> | null> {
    return this.rpc('get', [this.ns, id, options])
  }

  /**
   * Count entities matching a filter
   *
   * @param filter - MongoDB-style filter
   * @returns RpcPromise of count
   */
  count(filter?: Filter): RpcPromiseMarker<number> {
    return this.rpc('count', [this.ns, filter])
  }

  /**
   * Check if an entity exists
   *
   * @param id - Entity ID
   * @returns RpcPromise of boolean
   */
  exists(id: EntityId | string): RpcPromiseMarker<boolean> {
    return this.rpc('exists', [this.ns, id])
  }

  // ===========================================================================
  // Write Operations (via Durable Object)
  // ===========================================================================

  /**
   * Create a new entity
   *
   * Write operations are routed through Durable Objects
   * for consistency guarantees.
   *
   * @example
   * ```typescript
   * const post = await collection.create({
   *   $type: 'Post',
   *   name: 'My Post',
   *   title: 'Hello World',
   *   content: 'Content here',
   *   status: 'draft'
   * })
   * ```
   *
   * @param data - Entity data
   * @param options - Create options
   * @returns RpcPromise of created entity
   */
  create(data: CollectionCreateInput<T>, options?: CreateOptions): RpcPromiseMarker<Entity<T>> {
    return this.rpc('create', [this.ns, data, options])
  }

  /**
   * Update an entity
   *
   * Supports MongoDB-style update operators ($set, $inc, etc.)
   * and ParqueDB relationship operators ($link, $unlink).
   *
   * @example
   * ```typescript
   * // Simple update
   * const post = await collection.update('posts/123', {
   *   $set: { status: 'published' }
   * })
   *
   * // With operators
   * const post = await collection.update('posts/123', {
   *   $set: { status: 'published' },
   *   $inc: { viewCount: 1 },
   *   $currentDate: { publishedAt: true }
   * })
   *
   * // With optimistic concurrency
   * const post = await collection.update('posts/123',
   *   { $set: { status: 'published' } },
   *   { expectedVersion: 2 }
   * )
   * ```
   *
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   * @returns RpcPromise of updated entity
   */
  update(
    id: EntityId | string,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): RpcPromiseMarker<Entity<T> | null> {
    return this.rpc('update', [this.ns, id, update, options])
  }

  /**
   * Delete an entity
   *
   * By default performs soft delete (sets deletedAt).
   * Use { hard: true } for permanent deletion.
   *
   * @example
   * ```typescript
   * // Soft delete
   * const result = await collection.delete('posts/123')
   *
   * // Hard delete
   * const result = await collection.delete('posts/123', { hard: true })
   *
   * // With optimistic concurrency
   * const result = await collection.delete('posts/123', {
   *   expectedVersion: 3
   * })
   * ```
   *
   * @param id - Entity ID
   * @param options - Delete options
   * @returns RpcPromise of delete result
   */
  delete(id: EntityId | string, options?: DeleteOptions): RpcPromiseMarker<DeleteResult> {
    return this.rpc('delete', [this.ns, id, options])
  }

  // ===========================================================================
  // Convenience Methods with Map
  // ===========================================================================

  /**
   * Find entities and map results on the server
   *
   * This is a convenience method that combines find() and map()
   * into a single call. The mapper function is serialized and
   * executed on the server, avoiding transfer of large datasets.
   *
   * @example
   * ```typescript
   * // Extract just titles
   * const titles = await collection.findAndMap(
   *   { status: 'published' },
   *   post => post.title
   * )
   *
   * // Extract and transform
   * const summaries = await collection.findAndMap(
   *   { status: 'published' },
   *   post => ({
   *     id: post.$id,
   *     title: post.title,
   *     excerpt: post.content.slice(0, 100)
   *   })
   * )
   * ```
   *
   * @param filter - MongoDB-style filter
   * @param mapper - Mapping function (executed on server)
   * @returns RpcPromise of mapped results
   */
  findAndMap<U>(
    filter: Filter,
    mapper: (entity: Entity<T>) => U
  ): RpcPromiseMarker<U[]> {
    // The map function on RpcPromise takes (T extends (infer E)[] ? E : T) => U
    // Since find() returns PaginatedResult<Entity<T>>.items which is Entity<T>[], the mapper is correct
    return chainRpcPromise<RpcPromiseMarker<U[]>>(this.find(filter).map(mapper as (entity: Entity<T>) => U))
  }

  /**
   * Find one entity matching the filter
   *
   * @param filter - MongoDB-style filter
   * @param options - Find options (limit is ignored)
   * @returns RpcPromise of entity or null
   */
  findOne(filter: Filter, options?: Omit<FindOptions, 'limit'>): RpcPromiseMarker<Entity<T> | null> {
    return this.rpc('findOne', [this.ns, filter, options])
  }

  // ===========================================================================
  // Bulk Operations
  // ===========================================================================

  /**
   * Create multiple entities
   *
   * @param data - Array of entity data
   * @param options - Create options
   * @returns RpcPromise of created entities
   */
  createMany(
    data: CollectionCreateInput<T>[],
    options?: CreateOptions
  ): RpcPromiseMarker<Entity<T>[]> {
    return this.rpc('createMany', [this.ns, data, options])
  }

  /**
   * Update multiple entities matching a filter
   *
   * @param filter - Filter for entities to update
   * @param update - Update operations
   * @param options - Update options
   * @returns RpcPromise of update count
   */
  updateMany(
    filter: Filter,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): RpcPromiseMarker<{ matchedCount: number; modifiedCount: number }> {
    return this.rpc('updateMany', [this.ns, filter, update, options])
  }

  /**
   * Delete multiple entities matching a filter
   *
   * @param filter - Filter for entities to delete
   * @param options - Delete options
   * @returns RpcPromise of delete count
   */
  deleteMany(filter: Filter, options?: DeleteOptions): RpcPromiseMarker<DeleteResult> {
    return this.rpc('deleteMany', [this.ns, filter, options])
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  /**
   * Get the namespace for this collection
   */
  get namespace(): string {
    return this.ns
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Create an RpcPromise for a method call
   * Used internally to create chainable promises
   */
  private rpc<R>(method: string, args: unknown[]): RpcPromiseMarker<R> {
    return createRpcPromise<R>(this.client, method, args)
  }
}

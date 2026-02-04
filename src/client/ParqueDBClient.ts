/**
 * ParqueDB RPC Client
 *
 * Type-safe client for interacting with ParqueDB workers via RPC.
 * Provides both explicit collection access and Proxy-based dynamic access.
 */

import type {
  Entity,
  EntityId,
  CreateInput,
  UpdateInput,
  Filter,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  PaginatedResult,
  DeleteResult,
  Relationship,
} from '../types'
import type { ParqueDBWorker } from '../worker'

// =============================================================================
// Types
// =============================================================================

/**
 * Service stub type for RPC calls
 * This matches the pattern used by Cloudflare's Service Bindings
 */
export type ParqueDBService = Service<ParqueDBWorker>

/**
 * Collection interface for a specific namespace
 */
export interface Collection<T = Record<string, unknown>> {
  /** Namespace name */
  readonly namespace: string

  /**
   * Find entities matching a filter
   */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /**
   * Find a single entity matching a filter
   */
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>

  /**
   * Get entity by ID
   */
  get(id: string, options?: GetOptions): Promise<Entity<T> | null>

  /**
   * Create a new entity
   */
  create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>

  /**
   * Create multiple entities
   */
  createMany(items: CreateInput<T>[], options?: CreateOptions): Promise<Entity<T>[]>

  /**
   * Update an entity by ID
   */
  update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T>>

  /**
   * Delete an entity by ID
   */
  delete(id: string, options?: DeleteOptions): Promise<DeleteResult>

  /**
   * Delete multiple entities matching a filter
   */
  deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult>

  /**
   * Count entities matching a filter
   */
  count(filter?: Filter): Promise<number>

  /**
   * Check if an entity exists
   */
  exists(id: string): Promise<boolean>

  /**
   * Get relationships for an entity
   */
  getRelationships(
    id: string,
    predicate?: string | undefined,
    direction?: 'outbound' | 'inbound' | undefined
  ): Promise<Relationship[]>

  /**
   * Link two entities
   */
  link(id: string, predicate: string, targetId: string): Promise<void>

  /**
   * Unlink two entities
   */
  unlink(id: string, predicate: string, targetId: string): Promise<void>
}

/**
 * Client options
 */
export interface ParqueDBClientOptions {
  /** Default actor for write operations */
  actor?: string | undefined

  /** Request timeout in milliseconds */
  timeout?: number | undefined

  /** Enable request/response logging */
  debug?: boolean | undefined
}

// =============================================================================
// Collection Implementation
// =============================================================================

/**
 * Collection implementation that delegates to the RPC service
 */
class CollectionImpl<T = Record<string, unknown>> implements Collection<T> {
  constructor(
    public readonly namespace: string,
    private stub: ParqueDBService,
    private options: ParqueDBClientOptions
  ) {}

  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    return this.stub.find(this.namespace, filter, options) as Promise<PaginatedResult<Entity<T>>>
  }

  async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
    const result = await this.find(filter, { ...options, limit: 1 })
    return result.items[0] || null
  }

  async get(id: string, options?: GetOptions): Promise<Entity<T> | null> {
    return this.stub.get(this.namespace, id, options) as Promise<Entity<T> | null>
  }

  async create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>> {
    const opts = {
      ...options,
      actor: options?.actor || this.options.actor,
    }
    return this.stub.create(this.namespace, data as CreateInput, opts) as Promise<Entity<T>>
  }

  async createMany(items: CreateInput<T>[], options?: CreateOptions): Promise<Entity<T>[]> {
    const opts = {
      ...options,
      actor: options?.actor || this.options.actor,
    }
    return this.stub.createMany(this.namespace, items as CreateInput[], opts) as Promise<Entity<T>[]>
  }

  async update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T>> {
    const opts = {
      ...options,
      actor: options?.actor || this.options.actor,
    }
    return this.stub.update(this.namespace, id, update as UpdateInput, opts) as Promise<Entity<T>>
  }

  async delete(id: string, options?: DeleteOptions): Promise<DeleteResult> {
    const opts = {
      ...options,
      actor: options?.actor || this.options.actor,
    }
    return this.stub.delete(this.namespace, id, opts)
  }

  async deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    const opts = {
      ...options,
      actor: options?.actor || this.options.actor,
    }
    return this.stub.deleteMany(this.namespace, filter, opts)
  }

  async count(filter?: Filter): Promise<number> {
    return this.stub.count(this.namespace, filter)
  }

  async exists(id: string): Promise<boolean> {
    return this.stub.exists(this.namespace, id)
  }

  async getRelationships(
    id: string,
    predicate?: string,
    direction: 'outbound' | 'inbound' = 'outbound'
  ): Promise<Relationship[]> {
    return this.stub.getRelationships(this.namespace, id, predicate, direction)
  }

  async link(id: string, predicate: string, targetId: string): Promise<void> {
    const entityId = `${this.namespace}/${id}`
    return this.stub.link(entityId, predicate, targetId)
  }

  async unlink(id: string, predicate: string, targetId: string): Promise<void> {
    const entityId = `${this.namespace}/${id}`
    return this.stub.unlink(entityId, predicate, targetId)
  }
}

// =============================================================================
// ParqueDB Client
// =============================================================================

/**
 * ParqueDB RPC Client
 *
 * Provides type-safe access to ParqueDB via RPC service bindings.
 * Supports both explicit collection() method and Proxy-based property access.
 *
 * @example
 * // Create client from service binding
 * const client = new ParqueDBClient(env.PARQUEDB_SERVICE)
 *
 * // Proxy-based access
 * const post = await client.Posts.create({ $type: 'Post', name: 'Hello' })
 *
 * // Explicit access
 * const users = client.collection('users')
 * const user = await users.get('user-123')
 *
 * // With typed data
 * interface PostData { title: string; content: string }
 * const posts = client.collection<PostData>('posts')
 * const published = await posts.find({ status: 'published' })
 */
export class ParqueDBClient {
  /** Internal service stub */
  private stub: ParqueDBService

  /** Client options */
  private options: ParqueDBClientOptions

  /** Collection cache */
  private collections = new Map<string, Collection>()

  /**
   * Create a new ParqueDB client
   *
   * @param stub - Service binding to ParqueDB worker
   * @param options - Client options
   */
  constructor(stub: ParqueDBService, options: ParqueDBClientOptions = {}) {
    this.stub = stub
    this.options = options

    // Return a Proxy for dynamic property access
    return new Proxy(this, {
      get(target, prop: string | symbol) {
        // Return actual methods/properties
        if (prop in target || typeof prop === 'symbol') {
          return (target as Record<string | symbol, unknown>)[prop]
        }

        // Convert PascalCase property to namespace (e.g., Posts -> posts)
        const namespace = String(prop).charAt(0).toLowerCase() + String(prop).slice(1)

        // Return cached or new collection
        return target.collection(namespace)
      },
    })
  }

  // Dynamic property access for collections (Posts, Users, etc.)
  // TypeScript declaration for Proxy-based access
  [key: string]: Collection | unknown

  /**
   * Get a collection by namespace
   *
   * @param namespace - Collection namespace
   * @returns Collection interface
   */
  collection<T = Record<string, unknown>>(namespace: string): Collection<T> {
    if (!this.collections.has(namespace)) {
      this.collections.set(namespace, new CollectionImpl<T>(namespace, this.stub, this.options))
    }
    return this.collections.get(namespace) as Collection<T>
  }

  // ===========================================================================
  // Convenience Getters for Common Collections
  // ===========================================================================

  /** Posts collection */
  get Posts(): Collection {
    return this.collection('posts')
  }

  /** Users collection */
  get Users(): Collection {
    return this.collection('users')
  }

  /** Comments collection */
  get Comments(): Collection {
    return this.collection('comments')
  }

  /** Categories collection */
  get Categories(): Collection {
    return this.collection('categories')
  }

  /** Tags collection */
  get Tags(): Collection {
    return this.collection('tags')
  }

  // ===========================================================================
  // Direct Access Methods
  // ===========================================================================

  /**
   * Find entities in a namespace
   */
  async find(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<PaginatedResult<Entity>> {
    return this.stub.find(ns, filter, options)
  }

  /**
   * Get an entity by namespace and ID
   */
  async get(ns: string, id: string, options?: GetOptions): Promise<Entity | null> {
    return this.stub.get(ns, id, options)
  }

  /**
   * Create an entity in a namespace
   */
  async create(ns: string, data: CreateInput, options?: CreateOptions): Promise<Entity> {
    return this.stub.create(ns, data, {
      ...options,
      actor: options?.actor || this.options.actor,
    })
  }

  /**
   * Update an entity
   */
  async update(
    ns: string,
    id: string,
    update: UpdateInput,
    options?: UpdateOptions
  ): Promise<Entity> {
    return this.stub.update(ns, id, update, {
      ...options,
      actor: options?.actor || this.options.actor,
    })
  }

  /**
   * Delete an entity
   */
  async delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
    return this.stub.delete(ns, id, {
      ...options,
      actor: options?.actor || this.options.actor,
    })
  }

  /**
   * Link two entities
   */
  async link(
    fromId: string,
    predicate: string,
    toId: string,
    data?: Record<string, unknown>
  ): Promise<void> {
    return this.stub.link(fromId, predicate, toId, { data })
  }

  /**
   * Unlink two entities
   */
  async unlink(fromId: string, predicate: string, toId: string): Promise<void> {
    return this.stub.unlink(fromId, predicate, toId)
  }

  // ===========================================================================
  // Admin Operations
  // ===========================================================================

  /**
   * Trigger a flush of events to Parquet
   */
  async flush(ns?: string): Promise<void> {
    return this.stub.flush(ns)
  }

  /**
   * Get flush status
   */
  async getFlushStatus(ns?: string): Promise<{ unflushedCount: number }> {
    return this.stub.getFlushStatus(ns)
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a ParqueDB client from a service binding
 *
 * @param stub - Service binding to ParqueDB worker
 * @param options - Client options
 * @returns ParqueDB client
 *
 * @example
 * const client = createParqueDBClient(env.PARQUEDB_SERVICE)
 * const post = await client.Posts.create({ $type: 'Post', name: 'Hello' })
 */
export function createParqueDBClient(
  stub: ParqueDBService,
  options?: ParqueDBClientOptions
): ParqueDBClient {
  return new ParqueDBClient(stub, options)
}

// =============================================================================
// Type Helpers
// =============================================================================

/**
 * Type helper for creating typed collections
 *
 * @example
 * interface Post {
 *   title: string
 *   content: string
 *   status: 'draft' | 'published'
 * }
 *
 * const Posts = client.collection<Post>('posts')
 * const post = await Posts.create({
 *   $type: 'Post',
 *   name: 'Hello World',
 *   title: 'Hello World',
 *   content: 'This is my first post',
 *   status: 'draft'
 * })
 */
export type TypedCollection<T> = Collection<T>

/**
 * Extract entity type from collection
 */
export type EntityOf<C> = C extends Collection<infer T> ? Entity<T> : never

/**
 * Create input type for a collection
 */
export type CreateInputOf<C> = C extends Collection<infer T> ? CreateInput<T> : never

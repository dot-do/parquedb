/**
 * ParqueDB RPC Client with rpc.do Promise Pipelining
 *
 * Integrates rpc.do's batching capabilities for efficient batch loading of
 * relationships, solving the N+1 query problem.
 *
 * @example
 * ```typescript
 * import { createParqueDBRPCClient } from 'parquedb/integrations/rpc-do'
 *
 * const client = createParqueDBRPCClient({
 *   url: 'https://my-parquedb.workers.dev/rpc',
 *   batchingOptions: {
 *     windowMs: 10,    // Collect requests for 10ms
 *     maxBatchSize: 50 // Send batch when 50 requests accumulate
 *   }
 * })
 *
 * // Collections proxy - automatic batching
 * const posts = client.collection('posts')
 * const users = await posts.find({ published: true })
 *
 * // Batch load relationships (critical for N+1)
 * const authorsWithPosts = await client.batchGetRelated([
 *   { type: 'users', id: 'user-1', relation: 'posts' },
 *   { type: 'users', id: 'user-2', relation: 'posts' },
 *   { type: 'users', id: 'user-3', relation: 'posts' },
 * ])
 * ```
 *
 * @packageDocumentation
 */

import type { Entity, Filter, FindOptions, PaginatedResult, UpdateInput, DeleteResult } from '../../types'

// =============================================================================
// Types
// =============================================================================

/**
 * Transport interface for rpc.do
 * Matches the minimal transport contract from rpc.do
 */
export interface Transport {
  call(method: string, args: unknown[]): Promise<unknown>
  close?: (() => void) | undefined
}

/**
 * Batching options for request batching
 */
export interface BatchingOptions {
  /**
   * Time window in milliseconds to collect requests before sending batch.
   * Requests made within this window will be grouped together.
   * @default 10
   */
  windowMs?: number | undefined

  /**
   * Maximum number of requests to include in a single batch.
   * When this limit is reached, the batch is sent immediately.
   * @default 50
   */
  maxBatchSize?: number | undefined

  /**
   * Callback when a batch is about to be sent.
   * Useful for logging and debugging.
   */
  onBatch?: ((requests: BatchedRequest[]) => void) | undefined
}

/**
 * Represents a single request in a batch
 */
export interface BatchedRequest {
  /** Unique identifier for matching response to request */
  id: number
  /** RPC method name (e.g., "db.get") */
  method: string
  /** Arguments passed to the method */
  args: unknown[]
}

/**
 * Represents a single response in a batch
 */
export interface BatchedResponse {
  /** Matches the request id */
  id: number
  /** Result if successful */
  result?: unknown | undefined
  /** Error if failed */
  error?: { message: string; code?: string | number | undefined; data?: unknown | undefined } | undefined
}

/**
 * Options for creating a ParqueDB RPC client
 */
/** Default timeout in milliseconds (30 seconds) */
const DEFAULT_TIMEOUT_MS = 30000

export interface ParqueDBRPCClientOptions {
  /** RPC endpoint URL */
  url: string

  /** Authentication token or provider function */
  auth?: string | (() => string | null | Promise<string | null>) | undefined

  /** Request timeout in milliseconds (default: 30000) */
  timeout?: number | undefined

  /** Batching options for automatic request batching */
  batchingOptions?: BatchingOptions | undefined

  /** Custom headers for HTTP requests */
  headers?: Record<string, string> | undefined
}

/**
 * Request for batch loading related entities
 */
export interface BatchRelatedRequest {
  /** Entity type/collection name */
  type: string
  /** Entity ID */
  id: string
  /** Relation predicate name */
  relation: string
}

/**
 * Collection interface for CRUD operations
 */
export interface RPCCollection<T extends object = Record<string, unknown>> {
  /** Find entities matching a filter */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /** Find a single entity */
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>

  /** Get entity by ID */
  get(id: string): Promise<Entity<T> | null>

  /** Create a new entity */
  create(data: Partial<T> & { $type?: string | undefined; name?: string | undefined }): Promise<Entity<T>>

  /** Update an entity */
  update(id: string, update: UpdateInput<T>): Promise<Entity<T> | null>

  /** Delete an entity */
  delete(id: string): Promise<DeleteResult>

  /** Count entities matching a filter */
  count(filter?: Filter): Promise<number>

  /** Check if an entity exists */
  exists(id: string): Promise<boolean>

  /** Get related entities */
  getRelated(id: string, relation: string): Promise<PaginatedResult<Entity>>
}

/**
 * ParqueDB RPC Client interface
 */
export interface ParqueDBRPCClient {
  /** Get a collection by name */
  collection<T extends object = Record<string, unknown>>(name: string): RPCCollection<T>

  /**
   * Batch load related entities for multiple source entities.
   * Critical for solving N+1 query problems when loading relationships.
   *
   * @example
   * ```typescript
   * const authorsWithPosts = await client.batchGetRelated([
   *   { type: 'users', id: 'user-1', relation: 'posts' },
   *   { type: 'users', id: 'user-2', relation: 'posts' },
   * ])
   * // Returns: [[user1's posts], [user2's posts]]
   * ```
   */
  batchGetRelated(requests: BatchRelatedRequest[]): Promise<Array<PaginatedResult<Entity>>>

  /**
   * Batch get entities by IDs.
   * Efficiently loads multiple entities in a single batched request.
   *
   * @example
   * ```typescript
   * const users = await client.batchGet('users', ['id-1', 'id-2', 'id-3'])
   * ```
   */
  batchGet<T extends object = Record<string, unknown>>(type: string, ids: string[]): Promise<Array<Entity<T> | null>>

  /** Close the client and clean up resources */
  close(): void

  /** Access to the underlying transport for advanced usage */
  readonly transport: Transport
}

// =============================================================================
// Internal Pending Request
// =============================================================================

interface PendingRequest {
  id: number
  method: string
  args: unknown[]
  resolve: (value: unknown) => void
  reject: (error: unknown) => void
}

// =============================================================================
// Batching Transport Wrapper
// =============================================================================

/**
 * Create a batching transport wrapper
 *
 * Collects multiple RPC requests within a time window and sends them as a
 * single batch request for improved efficiency.
 */
function withBatching(
  transport: Transport,
  options: BatchingOptions = {}
): Transport {
  const { windowMs = 10, maxBatchSize = 50, onBatch } = options

  let pendingRequests: PendingRequest[] = []
  let batchTimer: ReturnType<typeof setTimeout> | null = null
  let requestIdCounter = 0

  async function flushBatch(): Promise<void> {
    if (batchTimer !== null) {
      clearTimeout(batchTimer)
      batchTimer = null
    }

    const batch = pendingRequests
    pendingRequests = []

    if (batch.length === 0) {
      return
    }

    const batchRequest: BatchedRequest[] = batch.map((req) => ({
      id: req.id,
      method: req.method,
      args: req.args,
    }))

    if (onBatch) {
      onBatch(batchRequest)
    }

    try {
      // Send batch request to transport using __batch method
      const responses = (await transport.call('__batch', [batchRequest])) as BatchedResponse[]

      // Create a map of responses by id
      const responseMap = new Map<number, BatchedResponse>()
      for (const response of responses) {
        responseMap.set(response.id, response)
      }

      // Resolve/reject each pending request
      for (const request of batch) {
        const response = responseMap.get(request.id)

        if (!response) {
          request.reject(new Error(`No response received for request ${request.id}`))
        } else if (response.error) {
          const error = new Error(response.error.message) as Error & { code?: string | number | undefined; data?: unknown | undefined }
          if (response.error.code !== undefined) {
            error.code = response.error.code
          }
          if (response.error.data !== undefined) {
            error.data = response.error.data
          }
          request.reject(error)
        } else {
          request.resolve(response.result)
        }
      }
    } catch (error) {
      for (const request of batch) {
        request.reject(error)
      }
    }
  }

  function scheduleBatchFlush(): void {
    if (batchTimer === null) {
      batchTimer = setTimeout(() => {
        void flushBatch()
      }, windowMs)
    }
  }

  const wrapped: Transport = {
    call(method: string, args: unknown[]): Promise<unknown> {
      return new Promise((resolve, reject) => {
        const id = ++requestIdCounter

        pendingRequests.push({
          id,
          method,
          args,
          resolve,
          reject,
        })

        if (pendingRequests.length >= maxBatchSize) {
          void flushBatch()
        } else {
          scheduleBatchFlush()
        }
      })
    },

    close() {
      // Clear the batch timer first
      if (batchTimer !== null) {
        clearTimeout(batchTimer)
        batchTimer = null
      }

      // Reject all pending requests to prevent memory leaks
      const pendingToReject = pendingRequests
      pendingRequests = []

      const closeError = new Error('Transport closed')
      for (const request of pendingToReject) {
        request.reject(closeError)
      }

      transport.close?.()
    }
  }

  return wrapped
}

// =============================================================================
// HTTP Transport
// =============================================================================

/**
 * Create an HTTP transport for the ParqueDB RPC endpoint
 */
function createHttpTransport(options: ParqueDBRPCClientOptions): Transport {
  const { url, auth, headers = {} } = options
  // Use provided timeout or default to 30 seconds
  const timeout = options.timeout ?? DEFAULT_TIMEOUT_MS

  return {
    async call(method: string, args: unknown[]): Promise<unknown> {
      const controller = new AbortController()
      let timeoutId: ReturnType<typeof setTimeout> | undefined

      if (timeout > 0) {
        timeoutId = setTimeout(() => controller.abort(), timeout)
      }

      try {
        // Resolve auth token
        let authToken: string | null = null
        if (typeof auth === 'function') {
          authToken = await auth()
        } else if (typeof auth === 'string') {
          authToken = auth
        }

        const requestHeaders: Record<string, string> = {
          'Content-Type': 'application/json',
          ...headers,
        }

        if (authToken) {
          requestHeaders['Authorization'] = `Bearer ${authToken}`
        }

        const response = await fetch(url, {
          method: 'POST',
          headers: requestHeaders,
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: Date.now(),
            method,
            params: args,
          }),
          signal: controller.signal,
        })

        if (!response.ok) {
          throw new Error(`RPC request failed: ${response.status} ${response.statusText}`)
        }

        const data = await response.json() as {
          result?: unknown | undefined
          error?: { message: string; code?: string | number | undefined; data?: unknown | undefined } | undefined
        }

        if (data.error) {
          const error = new Error(data.error.message) as Error & { code?: string | number | undefined; data?: unknown | undefined }
          if (data.error.code !== undefined) error.code = data.error.code
          if (data.error.data !== undefined) error.data = data.error.data
          throw error
        }

        return data.result
      } finally {
        if (timeoutId !== undefined) {
          clearTimeout(timeoutId)
        }
      }
    },
  }
}

// =============================================================================
// Collection Implementation
// =============================================================================

/**
 * Create a collection client for a specific namespace
 */
function createCollection<T extends object = Record<string, unknown>>(
  transport: Transport,
  name: string
): RPCCollection<T> {
  return {
    async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
      return transport.call('db.find', [name, filter, options]) as Promise<PaginatedResult<Entity<T>>>
    },

    async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
      const result = await this.find(filter, { ...options, limit: 1 })
      return result.items[0] ?? null
    },

    async get(id: string): Promise<Entity<T> | null> {
      return transport.call('db.get', [name, id]) as Promise<Entity<T> | null>
    },

    async create(data: Partial<T> & { $type?: string | undefined; name?: string | undefined }): Promise<Entity<T>> {
      return transport.call('db.create', [name, data]) as Promise<Entity<T>>
    },

    async update(id: string, update: UpdateInput<T>): Promise<Entity<T> | null> {
      return transport.call('db.update', [name, id, update]) as Promise<Entity<T> | null>
    },

    async delete(id: string): Promise<DeleteResult> {
      return transport.call('db.delete', [name, id]) as Promise<DeleteResult>
    },

    async count(filter?: Filter): Promise<number> {
      return transport.call('db.count', [name, filter]) as Promise<number>
    },

    async exists(id: string): Promise<boolean> {
      return transport.call('db.exists', [name, id]) as Promise<boolean>
    },

    async getRelated(id: string, relation: string): Promise<PaginatedResult<Entity>> {
      return transport.call('db.getRelated', [name, id, relation]) as Promise<PaginatedResult<Entity>>
    },
  }
}

// =============================================================================
// Client Implementation
// =============================================================================

/**
 * Create a ParqueDB RPC client with batching support
 *
 * This client integrates rpc.do's batching capabilities for efficient batch
 * loading of relationships, solving the N+1 query problem.
 *
 * @example
 * ```typescript
 * import { createParqueDBRPCClient } from 'parquedb/integrations/rpc-do'
 *
 * const client = createParqueDBRPCClient({
 *   url: 'https://my-parquedb.workers.dev/rpc',
 *   batchingOptions: {
 *     windowMs: 10,
 *     maxBatchSize: 50
 *   }
 * })
 *
 * // Collection access
 * const posts = await client.collection('posts').find({ published: true })
 *
 * // Batch load relationships (N+1 solution)
 * const authorsWithPosts = await client.batchGetRelated([
 *   { type: 'users', id: 'user-1', relation: 'posts' },
 *   { type: 'users', id: 'user-2', relation: 'posts' },
 * ])
 *
 * // Batch get by IDs
 * const users = await client.batchGet('users', ['id-1', 'id-2', 'id-3'])
 * ```
 */
export function createParqueDBRPCClient(options: ParqueDBRPCClientOptions): ParqueDBRPCClient {
  // Create base HTTP transport
  const baseTransport = createHttpTransport(options)

  // Wrap with batching if options provided
  const transport = options.batchingOptions
    ? withBatching(baseTransport, options.batchingOptions)
    : baseTransport

  // Collection cache - use object to satisfy generic type constraints
  const collections = new Map<string, RPCCollection<object>>()

  return {
    collection<T extends object = Record<string, unknown>>(name: string): RPCCollection<T> {
      if (!collections.has(name)) {
        collections.set(name, createCollection(transport, name) as RPCCollection<object>)
      }
      return collections.get(name) as RPCCollection<T>
    },

    async batchGetRelated(requests: BatchRelatedRequest[]): Promise<Array<PaginatedResult<Entity>>> {
      // These get batched automatically by the batching transport
      // Each request becomes a separate call that gets batched
      return Promise.all(
        requests.map(r => transport.call('db.getRelated', [r.type, r.id, r.relation]) as Promise<PaginatedResult<Entity>>)
      )
    },

    async batchGet<T extends object = Record<string, unknown>>(type: string, ids: string[]): Promise<Array<Entity<T> | null>> {
      // Single batched call for efficiency
      return transport.call('db.batchGet', [type, ids]) as Promise<Array<Entity<T> | null>>
    },

    close() {
      transport.close?.()
    },

    get transport() {
      return transport
    },
  }
}

// =============================================================================
// Integration with ai-database Adapter
// =============================================================================

/**
 * Interface matching BatchLoaderDB from the relationships module.
 * This allows the RPC client to be used as a data source for the
 * RelationshipBatchLoader in the ai-database integration.
 */
export interface RPCBatchLoaderDB {
  getRelated<T extends object = Record<string, unknown>>(
    namespace: string,
    id: string,
    relationField: string,
    options?: Record<string, unknown>
  ): Promise<{ items: Entity<T>[]; total?: number | undefined; hasMore?: boolean | undefined }>

  getByIds?<T extends object = Record<string, unknown>>(
    namespace: string,
    ids: string[]
  ): Promise<Entity<T>[]>
}

/**
 * Create a BatchLoaderDB-compatible interface from the RPC client.
 *
 * This allows the RelationshipBatchLoader from ai-database to use
 * an RPC client as its data source, enabling efficient batched
 * relationship loading over RPC.
 *
 * @example
 * ```typescript
 * import { createParqueDBRPCClient, createBatchLoaderDB } from 'parquedb/integrations/rpc-do'
 * import { RelationshipBatchLoader } from 'parquedb/relationships'
 *
 * const client = createParqueDBRPCClient({
 *   url: 'https://my-parquedb.workers.dev/rpc',
 *   batchingOptions: { windowMs: 10, maxBatchSize: 50 }
 * })
 *
 * // Create a BatchLoaderDB-compatible interface
 * const loaderDB = createBatchLoaderDB(client)
 *
 * // Use with RelationshipBatchLoader
 * const loader = new RelationshipBatchLoader(loaderDB)
 *
 * // Now relationship loads will use RPC with automatic batching
 * const authors = await Promise.all([
 *   loader.load('Post', 'post-1', 'author'),
 *   loader.load('Post', 'post-2', 'author'),
 *   loader.load('Post', 'post-3', 'author'),
 * ])
 * ```
 */
export function createBatchLoaderDB(client: ParqueDBRPCClient): RPCBatchLoaderDB {
  return {
    async getRelated<T extends object = Record<string, unknown>>(
      namespace: string,
      id: string,
      relationField: string,
      _options?: Record<string, unknown>
    ): Promise<{ items: Entity<T>[]; total?: number | undefined; hasMore?: boolean | undefined }> {
      const result = await client.collection(namespace).getRelated(id, relationField)
      return {
        items: result.items as Entity<T>[],
        total: result.total,
        hasMore: result.hasMore,
      }
    },

    async getByIds<T extends object = Record<string, unknown>>(
      namespace: string,
      ids: string[]
    ): Promise<Entity<T>[]> {
      const results = await client.batchGet(namespace, ids)
      return results.filter((r): r is Entity<T> => r !== null)
    },
  }
}

// =============================================================================
// Re-exports
// =============================================================================

export type {
  Entity,
  Filter,
  FindOptions,
  PaginatedResult,
  UpdateInput,
  DeleteResult,
} from '../../types'

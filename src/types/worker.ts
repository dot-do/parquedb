/**
 * Worker environment types for ParqueDB Cloudflare Workers
 */

import type { AIBinding } from '../embeddings/workers-ai'

// =============================================================================
// Environment Bindings
// =============================================================================

/**
 * Durable Object interface for write operations
 * This is a forward declaration - the actual implementation is in ParqueDBDO
 *
 * @typeParam TEntity - Entity type returned from operations. Must satisfy DOEntityConstraint.
 *                      Defaults to DOEntity for flexibility.
 * @typeParam TRelationship - Relationship type returned from related(). Must satisfy
 *                            DORelationshipConstraint. Defaults to DORelationship.
 * @typeParam TData - Custom data type for create/update inputs. Must extend DOEntityData.
 *                    Defaults to DOEntityData (Record<string, unknown>).
 */
export interface ParqueDBDOInterface<
  TEntity extends DOEntityConstraint = DOEntity,
  TRelationship extends DORelationshipConstraint = DORelationship,
  TData extends DOEntityData = DOEntityData
> {
  /**
   * Create a new entity
   * @param ns - Namespace (collection name)
   * @param data - Entity data with required $type and name
   * @param options - Create options
   * @returns Created entity
   */
  create(ns: string, data: DOCreateInput<TData>, options?: DOCreateOptions): Promise<TEntity>

  /**
   * Update an existing entity
   * @param ns - Namespace (collection name)
   * @param id - Entity ID to update
   * @param update - Update operators
   * @param options - Update options
   * @returns Updated entity
   */
  update(ns: string, id: string, update: DOUpdateInput, options?: DOUpdateOptions): Promise<TEntity>

  /**
   * Update multiple entities matching a filter
   * @param ns - Namespace (collection name)
   * @param filter - MongoDB-style filter to match entities
   * @param update - Update operators to apply
   * @param options - Update options
   * @returns Update result with matched and modified counts
   */
  updateMany(ns: string, filter: DOFilter, update: DOUpdateInput, options?: DOUpdateOptions): Promise<DOUpdateManyResult>

  /**
   * Delete an entity (soft delete by default)
   * @param ns - Namespace (collection name)
   * @param id - Entity ID to delete
   * @param options - Delete options
   * @returns Delete result with count
   */
  delete(ns: string, id: string, options?: DODeleteOptions): Promise<DODeleteResult>

  /**
   * Delete multiple entities matching a filter
   * @param ns - Namespace (collection name)
   * @param filter - MongoDB-style filter to match entities
   * @param options - Delete options
   * @returns Delete result with total count
   */
  deleteMany(ns: string, filter: DOFilter, options?: DODeleteOptions): Promise<DODeleteResult>

  /**
   * Create a relationship between two entities (legacy 5-arg signature)
   * @param fromNs - Source entity namespace
   * @param fromId - Source entity ID
   * @param predicate - Outbound relationship name
   * @param toNs - Target entity namespace
   * @param toId - Target entity ID
   */
  link(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>

  /**
   * Remove a relationship between two entities (legacy 5-arg signature)
   * @param fromNs - Source entity namespace
   * @param fromId - Source entity ID
   * @param predicate - Outbound relationship name
   * @param toNs - Target entity namespace
   * @param toId - Target entity ID
   */
  unlink(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>

  /**
   * Get relationships for an entity
   * @param ns - Namespace (collection name)
   * @param id - Entity ID
   * @param options - Related query options
   * @returns Array of relationships
   */
  related(ns: string, id: string, options?: DORelatedOptions): Promise<TRelationship[]>
}

/**
 * Cloudflare Worker environment bindings
 */
export interface Env {
  /** Durable Object namespace for ParqueDB write operations */
  PARQUEDB: DurableObjectNamespace

  /** R2 bucket for Parquet file storage (primary) */
  BUCKET: R2Bucket

  /** R2 bucket for CDN-cached reads (cdn bucket with edge caching) */
  CDN_BUCKET?: R2Bucket | undefined

  /** Optional FSX binding for POSIX-style file access */
  FSX?: Fetcher | undefined

  /** Workers AI binding for embedding generation */
  AI?: AIBinding | undefined

  /** Durable Object namespace for database index (user's database registry) */
  DATABASE_INDEX?: DurableObjectNamespace | undefined

  /** Durable Object namespace for rate limiting */
  RATE_LIMITER?: DurableObjectNamespace | undefined

  /** Durable Object namespace for backend migrations (batch processing) */
  MIGRATION?: DurableObjectNamespace | undefined

  /** Durable Object namespace for compaction state tracking */
  COMPACTION_STATE?: DurableObjectNamespace | undefined

  // =============================================================================
  // Cloudflare Workflows (resumable long-running tasks)
  // =============================================================================

  /** Compaction + Migration Workflow binding */
  COMPACTION_WORKFLOW?: {
    create(options: { params: unknown }): Promise<{ id: string }>
    get(id: string): Promise<{ status(): Promise<unknown> }>
  } | undefined

  /** Migration Workflow binding */
  MIGRATION_WORKFLOW?: {
    create(options: { params: unknown }): Promise<{ id: string }>
    get(id: string): Promise<{ status(): Promise<unknown> }>
  } | undefined

  /** Vacuum Workflow binding */
  VACUUM_WORKFLOW?: {
    create(options: { params: unknown }): Promise<{ id: string }>
    get(id: string): Promise<{ status(): Promise<unknown> }>
  } | undefined

  // =============================================================================
  // Queues (for R2 event notifications and compaction batching)
  // =============================================================================

  /** Queue producer for compaction events */
  COMPACTION_QUEUE?: Queue<unknown> | undefined

  // Note: Caching uses the free Cloudflare Cache API (caches.default), not KV.
  // Cache API provides 500MB on free accounts, 5GB+ on enterprise.
  // No binding needed - caches.default is globally available in Workers.

  /** Environment name (development, staging, production) */
  ENVIRONMENT?: string | undefined

  /** Optional secret for authentication */
  AUTH_SECRET?: string | undefined

  /** CDN r2.dev URL for public access, e.g. 'https://pub-xxx.r2.dev/parquedb' */
  CDN_R2_DEV_URL?: string | undefined

  /** Secret key for HMAC signing of sync upload/download tokens (required for sync routes) */
  SYNC_SECRET?: string | undefined

  /** JWKS URI for JWT token verification (e.g., 'https://api.workos.com/sso/jwks/client_xxx') */
  JWKS_URI?: string | undefined

  /**
   * KV namespace for tracking used upload token nonces (replay protection).
   * When configured, provides cross-isolate replay attack prevention.
   * Falls back to in-memory tracking (per-isolate only) when not available.
   */
  USED_TOKENS?: KVNamespace | undefined
}

// =============================================================================
// RPC Types
// =============================================================================

/**
 * Service binding type for RPC calls between workers
 * Use import type for better tree-shaking
 */

// =============================================================================
// DO Stub Types - Stricter generics for ParqueDBDOStub
// =============================================================================

/**
 * Options for DO create operations
 * Subset of CreateOptions with string actor for DO context
 */
export interface DOCreateOptions {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
  /** Skip validation entirely */
  skipValidation?: boolean | undefined
  /** Validation mode */
  validateOnWrite?: boolean | 'strict' | 'permissive' | 'warn' | undefined
  /** Return the created entity (default: true) */
  returnDocument?: boolean | undefined
}

/**
 * Options for DO update operations
 * Subset of UpdateOptions with string actor for DO context
 */
export interface DOUpdateOptions {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined
  /** Create if not exists (upsert) */
  upsert?: boolean | undefined
  /** Skip validation entirely */
  skipValidation?: boolean | undefined
  /** Validation mode */
  validateOnWrite?: boolean | 'strict' | 'permissive' | 'warn' | undefined
}

/**
 * Options for DO delete operations
 * Subset of DeleteOptions with string actor for DO context
 */
export interface DODeleteOptions {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
  /** Hard delete (permanent, skip soft delete) */
  hard?: boolean | undefined
  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined
}

/**
 * Options for DO link/unlink operations
 */
export interface DOLinkOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /**
   * How the relationship was matched (SHREDDED)
   * - 'exact': Precise match (user explicitly linked)
   * - 'fuzzy': Approximate match (entity resolution, text similarity)
   */
  matchMode?: 'exact' | 'fuzzy' | undefined
  /**
   * Similarity score for fuzzy matches (SHREDDED)
   * Range: 0.0 to 1.0
   * Only meaningful when matchMode is 'fuzzy'
   */
  similarity?: number | undefined
  /** Edge data (remaining metadata in Variant) */
  data?: Record<string, unknown> | undefined
}

/**
 * Options for DO related() queries
 *
 * Controls how relationships are traversed and filtered.
 */
export interface DORelatedOptions {
  /** Predicate name for outbound relationships */
  predicate?: string | undefined
  /** Reverse name for inbound relationships */
  reverse?: string | undefined
  /** Direction of traversal */
  direction?: 'outbound' | 'inbound' | undefined
  /** Maximum number of relationships to return */
  limit?: number | undefined
  /** Cursor for pagination */
  cursor?: string | undefined
  /** Include soft-deleted relationships */
  includeDeleted?: boolean | undefined
}

/**
 * MongoDB-style filter for DO operations
 *
 * Simplified filter type for DO context. Supports basic comparison
 * and logical operators.
 *
 * @example
 * ```typescript
 * // Simple equality
 * { status: 'published' }
 *
 * // Comparison operators
 * { views: { $gt: 100 } }
 *
 * // Logical operators
 * { $or: [{ status: 'published' }, { featured: true }] }
 * ```
 */
export interface DOFilter {
  /** Field filters - key is field name, value is filter condition */
  [field: string]: DOFieldFilter | undefined

  /** Logical AND */
  $and?: DOFilter[] | undefined

  /** Logical OR */
  $or?: DOFilter[] | undefined

  /** Logical NOT */
  $not?: DOFilter | undefined

  /** Logical NOR */
  $nor?: DOFilter[] | undefined
}

/**
 * Field-level filter value for DO operations
 *
 * Can be a direct value for equality or an operator object.
 */
export type DOFieldFilter =
  | string
  | number
  | boolean
  | null
  | Date
  | DOComparisonOperator
  | DOStringOperator
  | DOArrayOperator
  | DOExistenceOperator

/**
 * Comparison operators for DO filters
 */
export interface DOComparisonOperator {
  /** Equal to */
  $eq?: unknown | undefined
  /** Not equal to */
  $ne?: unknown | undefined
  /** Greater than */
  $gt?: unknown | undefined
  /** Greater than or equal */
  $gte?: unknown | undefined
  /** Less than */
  $lt?: unknown | undefined
  /** Less than or equal */
  $lte?: unknown | undefined
  /** In array */
  $in?: unknown[] | undefined
  /** Not in array */
  $nin?: unknown[] | undefined
}

/**
 * String operators for DO filters
 */
export interface DOStringOperator {
  /** Regular expression match */
  $regex?: string | RegExp | undefined
  /** Regex options (i, m, s) */
  $options?: string | undefined
}

/**
 * Array operators for DO filters
 */
export interface DOArrayOperator {
  /** Array contains all values */
  $all?: unknown[] | undefined
  /** Array element matches filter */
  $elemMatch?: DOFilter | undefined
  /** Array has exact size */
  $size?: number | undefined
}

/**
 * Existence operators for DO filters
 */
export interface DOExistenceOperator {
  /** Field exists */
  $exists?: boolean | undefined
  /** Field type */
  $type?: 'null' | 'boolean' | 'number' | 'string' | 'array' | 'object' | 'date' | undefined
}

/**
 * Result of updateMany operations in DO context
 */
export interface DOUpdateManyResult {
  /** Number of documents that matched the filter */
  matchedCount: number
  /** Number of documents that were actually modified */
  modifiedCount: number
}

/**
 * Base constraint for entity data in DO operations.
 *
 * Ensures that custom entity types have the minimum required structure
 * for ParqueDB entities while allowing additional user-defined fields.
 *
 * @example
 * ```typescript
 * // Valid custom data type
 * interface PostData extends DOEntityData {
 *   title: string
 *   content: string
 *   views: number
 * }
 * ```
 */
export type DOEntityData = Record<string, unknown>

/**
 * Reserved fields managed by ParqueDB that should not be in user data.
 * These are the system fields present on all entities.
 *
 * @internal
 */
export type DOReservedFields =
  | '$id'
  | '$type'
  | 'name'
  | 'createdAt'
  | 'createdBy'
  | 'updatedAt'
  | 'updatedBy'
  | 'deletedAt'
  | 'deletedBy'
  | 'version'

/**
 * Create input data shape for DO operations
 * Requires $type and name, allows additional fields
 *
 * @typeParam TData - Custom data fields to include (must not conflict with reserved fields)
 */
export interface DOCreateInput<TData extends DOEntityData = DOEntityData> {
  /** Entity type name */
  $type: string
  /** Human-readable display name */
  name: string
  /** Additional data fields - typed if TData is provided */
  [key: string]: TData[keyof TData] | string | unknown
}

/**
 * Update operators for DO update operations
 */
export interface DOUpdateInput {
  /** Set field values */
  $set?: Record<string, unknown> | undefined
  /** Remove fields */
  $unset?: Record<string, '' | 1 | true> | undefined
  /** Rename fields */
  $rename?: Record<string, string> | undefined
  /** Set on insert only (for upsert) */
  $setOnInsert?: Record<string, unknown> | undefined
  /** Increment numeric fields */
  $inc?: Record<string, number> | undefined
  /** Multiply numeric fields */
  $mul?: Record<string, number> | undefined
  /** Set to minimum */
  $min?: Record<string, unknown> | undefined
  /** Set to maximum */
  $max?: Record<string, unknown> | undefined
  /** Push to array */
  $push?: Record<string, unknown> | undefined
  /** Pull from array */
  $pull?: Record<string, unknown> | undefined
  /** Pull all from array */
  $pullAll?: Record<string, unknown[]> | undefined
  /** Add to set (unique push) */
  $addToSet?: Record<string, unknown> | undefined
  /** Pop from array */
  $pop?: Record<string, -1 | 1> | undefined
  /** Set to current date */
  $currentDate?: Record<string, true | { $type: 'date' | 'timestamp' }> | undefined
  /** Link relationships */
  $link?: Record<string, string | string[]> | undefined
  /** Unlink relationships */
  $unlink?: Record<string, string | string[] | '$all'> | undefined
  /** Bitwise operations */
  $bit?: Record<string, { and?: number | undefined; or?: number | undefined; xor?: number | undefined }> | undefined
  /** Generate embeddings */
  $embed?: Record<string, string | { field: string; model?: string | undefined; overwrite?: boolean | undefined }> | undefined
}

/**
 * Delete result from DO delete operations
 */
export interface DODeleteResult {
  /** Number of documents that were deleted */
  deletedCount: number
}

/**
 * Entity shape returned from DO operations
 * Includes $id, $type, name, audit fields, and user data
 */
export interface DOEntity {
  /** Full entity identifier (ns/id format) */
  $id: string
  /** Entity type name */
  $type: string
  /** Human-readable display name */
  name: string
  /** Creation timestamp */
  createdAt: Date
  /** Creator identifier */
  createdBy: string
  /** Last update timestamp */
  updatedAt: Date
  /** Last updater identifier */
  updatedBy: string
  /** Soft delete timestamp (if deleted) */
  deletedAt?: Date | undefined
  /** Deleter identifier (if deleted) */
  deletedBy?: string | undefined
  /** Optimistic concurrency version */
  version: number
  /** Additional data fields */
  [key: string]: unknown
}

/**
 * Relationship shape returned from DO getRelationships
 *
 * This interface satisfies DORelationshipConstraint and includes all standard
 * relationship fields plus an index signature for extensibility.
 */
export interface DORelationship {
  /** Source namespace */
  fromNs: string
  /** Source entity ID */
  fromId: string
  /** Source entity type */
  fromType: string
  /** Source entity name */
  fromName: string
  /** Outbound relationship name */
  predicate: string
  /** Inbound relationship name */
  reverse: string
  /** Target namespace */
  toNs: string
  /** Target entity ID */
  toId: string
  /** Target entity type */
  toType: string
  /** Target entity name */
  toName: string
  /** Match mode (exact or fuzzy) */
  matchMode?: 'exact' | 'fuzzy' | undefined
  /** Similarity score for fuzzy matches */
  similarity?: number | undefined
  /** Creation timestamp */
  createdAt: Date
  /** Creator identifier */
  createdBy: string
  /** Soft delete timestamp (if deleted) */
  deletedAt?: Date | undefined
  /** Deleter identifier (if deleted) */
  deletedBy?: string | undefined
  /** Optimistic concurrency version */
  version: number
  /** Additional edge data */
  data?: Record<string, unknown> | undefined
  /** Allow additional properties for extensibility */
  [key: string]: unknown
}

/**
 * Base constraint for custom entity types in ParqueDBDOStub.
 *
 * Custom entity types must have at minimum:
 * - $id: Full entity identifier (ns/id format)
 * - $type: Entity type name
 * - name: Human-readable display name
 * - Audit fields (createdAt, createdBy, updatedAt, updatedBy, version)
 *
 * This constraint ensures type safety while allowing custom fields.
 *
 * @example
 * ```typescript
 * // Custom entity type
 * interface Post extends DOEntityConstraint {
 *   title: string
 *   content: string
 *   views: number
 * }
 *
 * // Use with ParqueDBDOStub
 * const stub: ParqueDBDOStub<Post> = asDOStub(env.PARQUEDB.get(id))
 * const post = await stub.get('posts', 'p1') // Returns Post | null
 * ```
 */
export interface DOEntityConstraint {
  /** Full entity identifier (ns/id format) */
  $id: string
  /** Entity type name */
  $type: string
  /** Human-readable display name */
  name: string
  /** Creation timestamp */
  createdAt: Date
  /** Creator identifier */
  createdBy: string
  /** Last update timestamp */
  updatedAt: Date
  /** Last updater identifier */
  updatedBy: string
  /** Optimistic concurrency version */
  version: number
  /** Allow additional properties */
  [key: string]: unknown
}

/**
 * Base constraint for custom relationship types in ParqueDBDOStub.
 *
 * Custom relationship types must have at minimum:
 * - Source entity info (fromNs, fromId, fromType, fromName)
 * - Predicate names (predicate, reverse)
 * - Target entity info (toNs, toId, toType, toName)
 * - Audit fields (createdAt, createdBy, version)
 *
 * @example
 * ```typescript
 * // Custom relationship type with additional edge data
 * interface WeightedRelationship extends DORelationshipConstraint {
 *   weight: number
 *   priority: 'high' | 'low'
 * }
 * ```
 */
export interface DORelationshipConstraint {
  /** Source namespace */
  fromNs: string
  /** Source entity ID */
  fromId: string
  /** Source entity type */
  fromType: string
  /** Source entity name */
  fromName: string
  /** Outbound relationship name */
  predicate: string
  /** Inbound relationship name */
  reverse: string
  /** Target namespace */
  toNs: string
  /** Target entity ID */
  toId: string
  /** Target entity type */
  toType: string
  /** Target entity name */
  toName: string
  /** Creation timestamp */
  createdAt: Date
  /** Creator identifier */
  createdBy: string
  /** Optimistic concurrency version */
  version: number
  /** Allow additional properties */
  [key: string]: unknown
}

/**
 * Typed stub interface for Durable Object RPC calls
 *
 * Used instead of `as unknown as { ... }` casts when calling DO methods.
 * This provides a single source of truth for the DO RPC contract.
 *
 * @remarks The DO's link/unlink methods expect entity IDs in "ns/id" format.
 *
 * @typeParam TEntity - Entity type returned from operations. Must satisfy DOEntityConstraint
 *                      to ensure all required entity fields are present. Defaults to DOEntity.
 * @typeParam TRelationship - Relationship type returned from getRelationships. Must satisfy
 *                            DORelationshipConstraint for required relationship fields.
 *                            Defaults to DORelationship.
 * @typeParam TData - Custom data type for create/update inputs. Must extend DOEntityData.
 *                    Defaults to DOEntityData (Record<string, unknown>).
 *
 * @example
 * ```typescript
 * // Basic usage with default types
 * const stub: ParqueDBDOStub = asDOStub(env.PARQUEDB.get(id))
 *
 * // Custom entity type for stronger typing
 * interface Post extends DOEntity {
 *   title: string
 *   content: string
 *   views: number
 * }
 * const typedStub: ParqueDBDOStub<Post> = asDOStub(env.PARQUEDB.get(id))
 * const post = await typedStub.get('posts', 'p1')
 * if (post) {
 *   console.log(post.title) // string - properly typed!
 * }
 *
 * // With custom data constraint for inputs
 * interface PostData { title: string; content: string }
 * const stub: ParqueDBDOStub<Post, DORelationship, PostData> = ...
 * await stub.create('posts', { $type: 'Post', name: 'My Post', title: '...', content: '...' })
 * ```
 */
export interface ParqueDBDOStub<
  TEntity extends DOEntityConstraint = DOEntity,
  TRelationship extends DORelationshipConstraint = DORelationship,
  TData extends DOEntityData = DOEntityData
> {
  // Entity operations
  /**
   * Get a single entity by namespace and ID
   * @param ns - Namespace (collection name)
   * @param id - Entity ID within namespace
   * @param includeDeleted - Include soft-deleted entities (default: false)
   * @returns Entity or null if not found
   */
  get(ns: string, id: string, includeDeleted?: boolean): Promise<TEntity | null>

  /**
   * Create a new entity
   * @param ns - Namespace (collection name)
   * @param data - Entity data with required $type and name. When TData is specified,
   *               the data is typed accordingly for compile-time safety.
   * @param options - Create options
   * @returns Created entity with full type information
   */
  create(ns: string, data: DOCreateInput<TData>, options?: DOCreateOptions): Promise<TEntity>

  /**
   * Create multiple entities in a single operation
   * @param ns - Namespace (collection name)
   * @param items - Array of entity data. When TData is specified,
   *                each item is typed accordingly for compile-time safety.
   * @param options - Create options
   * @returns Array of created entities with full type information
   */
  createMany(ns: string, items: DOCreateInput<TData>[], options?: DOCreateOptions): Promise<TEntity[]>

  /**
   * Update an existing entity
   * @param ns - Namespace (collection name)
   * @param id - Entity ID to update
   * @param update - Update operators
   * @param options - Update options
   * @returns Updated entity
   */
  update(ns: string, id: string, update: DOUpdateInput, options?: DOUpdateOptions): Promise<TEntity>

  /**
   * Delete an entity (soft delete by default)
   * @param ns - Namespace (collection name)
   * @param id - Entity ID to delete
   * @param options - Delete options
   * @returns Delete result with count
   */
  delete(ns: string, id: string, options?: DODeleteOptions): Promise<DODeleteResult>

  /**
   * Delete multiple entities by ID
   * @param ns - Namespace (collection name)
   * @param ids - Array of entity IDs to delete
   * @param options - Delete options
   * @returns Delete result with total count
   */
  deleteMany(ns: string, ids: string[], options?: DODeleteOptions): Promise<DODeleteResult>

  // Relationship operations (entity IDs in "ns/id" format)
  /**
   * Create a relationship between two entities
   * @param fromId - Source entity ID in "ns/id" format
   * @param predicate - Outbound relationship name
   * @param toId - Target entity ID in "ns/id" format
   * @param options - Link options
   */
  link(fromId: string, predicate: string, toId: string, options?: DOLinkOptions): Promise<void>

  /**
   * Remove a relationship between two entities
   * @param fromId - Source entity ID in "ns/id" format
   * @param predicate - Outbound relationship name
   * @param toId - Target entity ID in "ns/id" format
   * @param options - Unlink options
   */
  unlink(fromId: string, predicate: string, toId: string, options?: DOLinkOptions): Promise<void>

  /**
   * Get relationships for an entity
   * @param ns - Namespace (collection name)
   * @param id - Entity ID
   * @param predicate - Filter by predicate name (optional)
   * @param direction - Traversal direction (default: 'outbound')
   * @returns Array of relationships
   */
  getRelationships(
    ns: string,
    id: string,
    predicate?: string,
    direction?: 'outbound' | 'inbound'
  ): Promise<TRelationship[]>

  // Cache invalidation methods
  /**
   * Get the current cache invalidation version for a namespace
   * @param ns - Namespace (collection name)
   * @returns Current version number
   */
  getInvalidationVersion(ns: string): number

  /**
   * Check if worker cache should be invalidated
   * @param ns - Namespace (collection name)
   * @param workerVersion - Worker's cached version
   * @returns True if cache should be invalidated
   */
  shouldInvalidate(ns: string, workerVersion: number): boolean

  // Event-sourced entity state
  /**
   * Get entity state reconstructed from events (event sourcing)
   * @param ns - Namespace (collection name)
   * @param id - Entity ID
   * @returns Entity or null if not found
   */
  getEntityFromEvents(ns: string, id: string): Promise<TEntity | null>
}

export type ParqueDBService = Fetcher

/**
 * RPC request context
 */
export interface RpcContext {
  /** Request ID for tracing */
  requestId: string

  /** Actor making the request (for audit trails) */
  actor?: string | undefined

  /** Timestamp of the request */
  timestamp: Date

  /** Optional correlation ID for distributed tracing */
  correlationId?: string | undefined
}

// =============================================================================
// DO Routing
// =============================================================================

/**
 * Strategy for routing to Durable Objects
 * - 'global': Single global DO for all writes (simple, but potential bottleneck)
 * - 'namespace': One DO per namespace (good for multi-tenant)
 * - 'partition': Hash-based partitioning across multiple DOs
 */
export type DORoutingStrategy = 'global' | 'namespace' | 'partition'

/**
 * Configuration for DO routing
 */
export interface DORoutingConfig {
  /** Routing strategy */
  strategy: DORoutingStrategy

  /** Number of partitions (only used with 'partition' strategy) */
  partitions?: number | undefined

  /** Custom partition key function (for 'partition' strategy) */
  partitionKey?: ((ns: string, id?: string) => string) | undefined
}

// =============================================================================
// Write Transaction Types
// =============================================================================

/**
 * Write transaction for batching multiple operations
 */
export interface WriteTransaction {
  /** Transaction ID */
  id: string

  /** Operations in this transaction */
  operations: WriteOperation[]

  /** Transaction status */
  status: 'pending' | 'committed' | 'rolled_back' | 'failed'

  /** Created timestamp */
  createdAt: Date

  /** Committed timestamp */
  committedAt?: Date | undefined
}

/**
 * Individual write operation within a transaction
 */
export interface WriteOperation {
  /** Operation type */
  type: 'create' | 'update' | 'delete' | 'link' | 'unlink'

  /** Target namespace */
  ns: string

  /** Target entity ID (not required for create) */
  id?: string | undefined

  /** Operation payload */
  payload: unknown

  /** Sequence number within transaction */
  seq: number
}

// =============================================================================
// Flush Configuration
// =============================================================================

/**
 * Configuration for flushing events to Parquet
 */
export interface FlushConfig {
  /** Minimum number of events before flushing */
  minEvents: number

  /** Maximum time between flushes (ms) */
  maxInterval: number

  /** Maximum events before forced flush */
  maxEvents: number

  /** Target Parquet row group size */
  rowGroupSize: number
}

/**
 * Default flush configuration
 */
export const DEFAULT_FLUSH_CONFIG: FlushConfig = {
  minEvents: 100,
  maxInterval: 60000, // 1 minute
  maxEvents: 10000,
  rowGroupSize: 1000,
}

// =============================================================================
// SQLite Schema for DO Storage
// =============================================================================

/**
 * Schema definitions for DO SQLite tables
 * These are the single source of truth for all DO SQLite schemas.
 * Used by ParqueDBDO.ensureInitialized() and tests.
 */
export const DO_SQLITE_SCHEMA = {
  /** Entity metadata table */
  entities: `
    CREATE TABLE IF NOT EXISTS entities (
      ns TEXT NOT NULL,
      id TEXT NOT NULL,
      type TEXT NOT NULL,
      name TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      created_by TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      updated_by TEXT NOT NULL,
      deleted_at TEXT,
      deleted_by TEXT,
      data TEXT NOT NULL,
      PRIMARY KEY (ns, id)
    )
  `,

  /** Relationships table with shredded fields for efficient querying */
  relationships: `
    CREATE TABLE IF NOT EXISTS relationships (
      from_ns TEXT NOT NULL,
      from_id TEXT NOT NULL,
      predicate TEXT NOT NULL,
      to_ns TEXT NOT NULL,
      to_id TEXT NOT NULL,
      reverse TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      created_by TEXT NOT NULL,
      deleted_at TEXT,
      deleted_by TEXT,
      -- Shredded fields (top-level columns for efficient querying)
      match_mode TEXT,           -- 'exact' or 'fuzzy'
      similarity REAL,           -- 0.0 to 1.0 for fuzzy matches
      -- Remaining metadata in Variant
      data TEXT,
      PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
    )
  `,

  /** WAL table for event batching with namespace-based counters */
  events_wal: `
    CREATE TABLE IF NOT EXISTS events_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `,

  /** WAL table for relationship event batching */
  rels_wal: `
    CREATE TABLE IF NOT EXISTS rels_wal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ns TEXT NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      events BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `,

  /** Flush checkpoints */
  checkpoints: `
    CREATE TABLE IF NOT EXISTS checkpoints (
      id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      event_count INTEGER NOT NULL,
      first_event_id TEXT NOT NULL,
      last_event_id TEXT NOT NULL,
      parquet_path TEXT NOT NULL
    )
  `,

  /** Pending row groups table - tracks bulk writes to R2 pending files */
  pending_row_groups: `
    CREATE TABLE IF NOT EXISTS pending_row_groups (
      id TEXT PRIMARY KEY,
      ns TEXT NOT NULL,
      path TEXT NOT NULL,
      row_count INTEGER NOT NULL,
      first_seq INTEGER NOT NULL,
      last_seq INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
  `,

  /** Indexes for common queries */
  indexes: [
    // Entity indexes
    'CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)',
    'CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)',
    // Relationship indexes
    'CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)',
    'CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)',
    'CREATE INDEX IF NOT EXISTS idx_relationships_match_mode ON relationships(match_mode) WHERE match_mode IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS idx_relationships_similarity ON relationships(similarity) WHERE similarity IS NOT NULL',
    // WAL indexes
    'CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)',
    'CREATE INDEX IF NOT EXISTS idx_rels_wal_ns ON rels_wal(ns, last_seq)',
    // Pending row groups index
    'CREATE INDEX IF NOT EXISTS idx_pending_row_groups_ns ON pending_row_groups(ns, created_at)',
  ],
} as const

/**
 * Helper function to initialize all DO SQLite tables and indexes
 * @param sql - SqlStorage instance from Durable Object context
 */
export function initDOSqliteSchema(sql: { exec: (query: string) => unknown }): void {
  // Create all tables
  sql.exec(DO_SQLITE_SCHEMA.entities)
  sql.exec(DO_SQLITE_SCHEMA.relationships)
  sql.exec(DO_SQLITE_SCHEMA.events_wal)
  sql.exec(DO_SQLITE_SCHEMA.rels_wal)
  sql.exec(DO_SQLITE_SCHEMA.checkpoints)
  sql.exec(DO_SQLITE_SCHEMA.pending_row_groups)

  // Create all indexes
  for (const indexSql of DO_SQLITE_SCHEMA.indexes) {
    sql.exec(indexSql)
  }
}

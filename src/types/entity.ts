/**
 * Core entity types for ParqueDB
 */

// =============================================================================
// Branded Types (for type safety)
// =============================================================================

/** Namespace identifier */
export type Namespace = string & { readonly __brand: unique symbol }

/** Entity ID within a namespace */
export type Id = string & { readonly __brand: unique symbol }

/** Full entity identifier: ns/id */
export type EntityId = `${string}/${string}` & { readonly __brand: unique symbol }

/** Create an EntityId from namespace and id */
export function entityId(ns: string, id: string): EntityId {
  return `${ns}/${id}` as EntityId
}

/** Parse an EntityId into namespace and id */
export function parseEntityId(entityId: EntityId): { ns: Namespace; id: Id } {
  const [ns, ...rest] = entityId.split('/')
  return { ns: ns as Namespace, id: rest.join('/') as Id }
}

// =============================================================================
// Audit Fields
// =============================================================================

/** Standard audit fields present on every entity */
export interface AuditFields {
  /** When the entity was created */
  createdAt: Date
  /** Who created the entity (EntityId of actor) */
  createdBy: EntityId
  /** When the entity was last updated */
  updatedAt: Date
  /** Who last updated the entity */
  updatedBy: EntityId
  /** Soft delete timestamp (null if not deleted) */
  deletedAt?: Date
  /** Who deleted the entity */
  deletedBy?: EntityId
  /** Optimistic concurrency version */
  version: number
}

// =============================================================================
// Relationship Types
// =============================================================================

/** A relationship link: maps display name to EntityId or meta fields */
export interface RelLink {
  [displayName: string]: EntityId | number | string | undefined
}

/** A paginated set of relationship links */
export interface RelSet extends RelLink {
  /** Total count of relationships */
  $count?: number
  /** Cursor for fetching next page */
  $next?: string
}

/** Relationship definition in rels.parquet */
export interface Relationship {
  /** Source namespace */
  fromNs: Namespace
  /** Source entity ID */
  fromId: Id
  /** Source entity type */
  fromType: string
  /** Source entity display name */
  fromName: string

  /** Outbound relationship name (e.g., "author", "category") */
  predicate: string
  /** Inbound relationship name (e.g., "posts", "items") */
  reverse: string

  /** Target namespace */
  toNs: Namespace
  /** Target entity ID */
  toId: Id
  /** Target entity type */
  toType: string
  /** Target entity display name */
  toName: string

  /** When the relationship was created */
  createdAt: Date
  /** Who created the relationship */
  createdBy: EntityId
  /** Soft delete timestamp */
  deletedAt?: Date
  /** Who deleted the relationship */
  deletedBy?: EntityId
  /** Optimistic concurrency version */
  version: number

  /** Optional edge properties (Variant) */
  data?: Record<string, unknown>
}

// =============================================================================
// Entity Types
// =============================================================================

/** Minimal entity reference */
export interface EntityRef {
  /** Full entity ID (ns/id) */
  $id: EntityId
  /** Entity type */
  $type: string
  /** Human-readable display name */
  name: string
}

/** Base entity structure (what's stored in data.parquet) */
export interface EntityRecord {
  /** Namespace */
  ns: Namespace
  /** Entity ID within namespace */
  id: Id
  /** Entity type */
  type: string
  /** Human-readable display name */
  name: string

  /** Audit fields */
  createdAt: Date
  createdBy: EntityId
  updatedAt: Date
  updatedBy: EntityId
  deletedAt?: Date
  deletedBy?: EntityId
  version: number

  /** Flexible data payload (Variant type) */
  data: Record<string, unknown>
}

/**
 * Full entity as returned by API (includes relationships)
 *
 * The generic parameter TData allows typing of the entity's data fields.
 * When TData is specified (e.g., Entity<Post>), the entity's data properties
 * are typed according to TData.
 *
 * @example
 * interface Post { title: string; content: string; views: number }
 * const post: Entity<Post> = await db.Posts.get('post-1')
 * console.log(post.title) // string
 */
export type Entity<TData = Record<string, unknown>> = EntityRef & AuditFields & TData & {
  /** Data fields and relationships - allows additional properties */
  [key: string]: unknown
}

// =============================================================================
// Input Types
// =============================================================================

/** Input for creating a new entity */
export interface CreateInput<T = Record<string, unknown>> {
  /** Entity type (required) */
  $type: string
  /** Human-readable display name (required) */
  name: string

  /** Data fields */
  [key: string]: unknown

  // Outbound relationships can be specified inline:
  // author: { 'Nathan': 'users/nathan' }
  // categories: { 'Tech': 'categories/tech', 'DB': 'categories/db' }
}

/** Input for replacing an entity */
export interface ReplaceInput<T = Record<string, unknown>> extends CreateInput<T> {
  /** Expected version for optimistic concurrency */
  version?: number
}

// =============================================================================
// Variant Type (Semi-structured data)
// =============================================================================

/** Supported primitive types in Variant */
export type VariantPrimitive =
  | null
  | boolean
  | number
  | string
  | Date

/** Variant value (recursive semi-structured data) */
export type VariantValue =
  | VariantPrimitive
  | VariantValue[]
  | { [key: string]: VariantValue }

/** Variant object (what's stored in data column) */
export type Variant = Record<string, VariantValue>

// =============================================================================
// Event Types (CDC / WAL)
// =============================================================================

/** Event operation types */
export type EventOp = 'CREATE' | 'UPDATE' | 'DELETE'

/**
 * CDC event record
 *
 * Events are the source of truth in ParqueDB. The data.parquet and rels.parquet
 * files are materialized views that can be reconstructed from the events log.
 *
 * Target format:
 * - Entity: "users:u1", "posts:p5" (ns:id as URL-like path)
 * - Relationship: "users:u1:authored:posts:p5" (from:pred:to)
 */
export interface Event {
  /** Event ID (ULID for ordering and deduplication) */
  id: string
  /** Event timestamp (ms since epoch) */
  ts: number
  /** Operation type */
  op: EventOp
  /**
   * Target identifier:
   * - Entity: "ns:id" (e.g., "users:u1")
   * - Relationship: "from:pred:to" (e.g., "users:u1:authored:posts:p5")
   */
  target: string
  /** State before change (undefined for CREATE) */
  before?: Variant
  /** State after change (undefined for DELETE) */
  after?: Variant
  /** Who made the change (e.g., "users:admin") */
  actor?: string
  /** Additional metadata (request ID, correlation ID, etc.) */
  metadata?: Variant
}

/**
 * Check if an event target is a relationship (contains 2+ colons)
 */
export function isRelationshipTarget(target: string): boolean {
  return target.split(':').length >= 4
}

/**
 * Parse an entity target into ns and id
 */
export function parseEntityTarget(target: string): { ns: string; id: string } {
  const colonIdx = target.indexOf(':')
  if (colonIdx === -1) throw new Error(`Invalid entity target: ${target}`)
  return {
    ns: target.slice(0, colonIdx),
    id: target.slice(colonIdx + 1),
  }
}

/**
 * Parse a relationship target into from, predicate, and to
 */
export function parseRelTarget(target: string): {
  from: string
  predicate: string
  to: string
} {
  const parts = target.split(':')
  if (parts.length < 4) throw new Error(`Invalid relationship target: ${target}`)
  // Format: from_ns:from_id:predicate:to_ns:to_id
  // e.g., "users:u1:authored:posts:p5"
  const fromNs = parts[0]!
  const fromId = parts[1]!
  const predicate = parts[2]!
  const toNs = parts[3]!
  const toId = parts.slice(4).join(':') // Handle ids with colons
  return {
    from: `${fromNs}:${fromId}`,
    predicate,
    to: `${toNs}:${toId}`,
  }
}

/**
 * Create an entity target string
 */
export function entityTarget(ns: string, id: string): string {
  return `${ns}:${id}`
}

/**
 * Create a relationship target string
 */
export function relTarget(from: string, predicate: string, to: string): string {
  return `${from}:${predicate}:${to}`
}

// =============================================================================
// Result Types
// =============================================================================

/** Result of an update operation */
export interface UpdateResult {
  /** Number of documents that matched the filter */
  matchedCount: number
  /** Number of documents that were modified */
  modifiedCount: number
}

/** Result of a delete operation */
export interface DeleteResult {
  /** Number of documents that were deleted */
  deletedCount: number
}

/** Paginated result set */
export interface PaginatedResult<T> {
  /** Result items */
  items: T[]
  /** Total count (if available) */
  total?: number
  /** Cursor for next page */
  nextCursor?: string
  /** Whether there are more results */
  hasMore: boolean
}

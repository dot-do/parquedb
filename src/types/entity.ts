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

/** Full entity as returned by API (includes relationships) */
export interface Entity<TData = Record<string, unknown>> extends EntityRef, AuditFields {
  /** Data fields merged to root */
  [key: string]: unknown

  // Note: Outbound predicates and inbound reverses are dynamically added
  // based on schema and relationship queries
}

/** Entity with typed data fields */
export type TypedEntity<T> = EntityRef & AuditFields & T & {
  [predicate: string]: RelLink | RelSet | unknown
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
// Event Types (CDC)
// =============================================================================

/** Event operation types */
export type EventOp = 'CREATE' | 'UPDATE' | 'DELETE'

/** Event target types */
export type EventTarget = 'entity' | 'rel'

/** CDC event record */
export interface Event {
  /** Event ID (ULID for ordering) */
  id: string
  /** Event timestamp */
  ts: Date
  /** What was affected */
  target: EventTarget
  /** Operation type */
  op: EventOp
  /** Namespace of affected entity */
  ns: Namespace
  /** ID of affected entity */
  entityId: Id
  /** State before change (null for CREATE) */
  before: Variant | null
  /** State after change (null for DELETE) */
  after: Variant | null
  /** Who made the change */
  actor: EntityId
  /** Additional metadata (request ID, correlation ID, etc.) */
  metadata?: Variant
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

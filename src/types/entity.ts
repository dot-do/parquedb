/**
 * Core entity types for ParqueDB
 *
 * This module defines the fundamental types for entities, relationships, and events
 * in ParqueDB. These types form the foundation of the database's data model.
 *
 * @module types/entity
 */

// =============================================================================
// Branded Types (for type safety)
// =============================================================================

/**
 * Namespace identifier (branded type for type safety)
 *
 * Namespaces are used to organize entities into logical collections (e.g., "users", "posts").
 * The branded type prevents accidental mixing of namespace strings with regular strings.
 *
 * @example
 * ```typescript
 * const ns: Namespace = 'users' as Namespace
 * const collection = db.collection(ns)
 * ```
 */
export type Namespace = string & { readonly __brand: unique symbol }

/**
 * Entity ID within a namespace (branded type for type safety)
 *
 * IDs uniquely identify entities within a namespace. By default, ParqueDB generates
 * ULIDs (Universally Unique Lexicographically Sortable Identifiers) for new entities.
 *
 * @example
 * ```typescript
 * const id: Id = '01HZXYZ...' as Id
 * const entity = await db.Users.get(id)
 * ```
 */
export type Id = string & { readonly __brand: unique symbol }

/**
 * Full entity identifier combining namespace and ID: "ns/id"
 *
 * EntityIds are the canonical way to reference entities across the database,
 * especially in relationships and audit trails.
 *
 * @example
 * ```typescript
 * const entityId: EntityId = 'users/01HZXYZ...' as EntityId
 * const ref = { author: entityId }
 * ```
 */
export type EntityId = `${string}/${string}` & { readonly __brand: unique symbol }

/**
 * Create an EntityId from namespace and id components
 *
 * @param ns - The namespace (collection name)
 * @param id - The entity ID within the namespace
 * @returns A combined EntityId in "ns/id" format
 *
 * @example
 * ```typescript
 * const id = entityId('users', '01HZXYZ...')
 * // Returns: 'users/01HZXYZ...' as EntityId
 * ```
 */
export function entityId(ns: string, id: string): EntityId {
  return `${ns}/${id}` as EntityId
}

/**
 * Parse an EntityId into its namespace and id components
 *
 * @param entityId - The full entity identifier to parse
 * @returns An object with ns (namespace) and id properties
 *
 * @example
 * ```typescript
 * const { ns, id } = parseEntityId('users/01HZXYZ...' as EntityId)
 * // ns = 'users', id = '01HZXYZ...'
 * ```
 */
export function parseEntityId(entityId: EntityId): { ns: Namespace; id: Id } {
  const [ns, ...rest] = entityId.split('/')
  return { ns: ns as Namespace, id: rest.join('/') as Id }
}

// =============================================================================
// Type Guards and Validation
// =============================================================================

/**
 * Check if a string is a valid EntityId format (contains at least one '/')
 */
export function isValidEntityId(value: unknown): value is EntityId {
  return typeof value === 'string' && value.includes('/') && value.indexOf('/') > 0
}

/**
 * Check if a string is a valid Namespace (non-empty string without '/')
 */
export function isValidNamespace(value: unknown): value is Namespace {
  return typeof value === 'string' && value.length > 0 && !value.includes('/')
}

/**
 * Check if a string is a valid Id (non-empty string)
 */
export function isValidId(value: unknown): value is Id {
  return typeof value === 'string' && value.length > 0
}

// =============================================================================
// Type-Safe Cast Functions
// =============================================================================

/**
 * Cast a string to EntityId with validation.
 * Use when the string format is unknown and needs validation.
 *
 * @throws Error if the value is not a valid EntityId format
 */
export function toEntityId(value: string): EntityId {
  if (!isValidEntityId(value)) {
    throw new Error(`Invalid EntityId: "${value}" (must be in "ns/id" format)`)
  }
  return value
}

/**
 * Cast a string to EntityId without throwing.
 * Returns null if the value is not a valid EntityId format.
 */
export function toEntityIdOrNull(value: unknown): EntityId | null {
  return isValidEntityId(value) ? value : null
}

/**
 * Cast a string to Namespace with validation.
 *
 * @throws Error if the value is not a valid Namespace
 */
export function toNamespace(value: string): Namespace {
  if (!isValidNamespace(value)) {
    throw new Error(`Invalid Namespace: "${value}" (must be non-empty without '/')`)
  }
  return value as Namespace
}

/**
 * Cast a string to Id with validation.
 *
 * @throws Error if the value is not a valid Id
 */
export function toId(value: string): Id {
  if (!isValidId(value)) {
    throw new Error(`Invalid Id: "${value}" (must be non-empty)`)
  }
  return value as Id
}

/**
 * Cast a string to EntityId, assuming it's already validated.
 * Use when the value comes from a trusted source (database, validated input).
 *
 * IMPORTANT: Only use this when you're certain the value is valid.
 * Prefer toEntityId() for user input or untrusted sources.
 */
export function asEntityId(value: string): EntityId {
  return value as EntityId
}

/**
 * Cast a string to Namespace, assuming it's already validated.
 * Use when the value comes from a trusted source.
 */
export function asNamespace(value: string): Namespace {
  return value as Namespace
}

/**
 * Cast a string to Id, assuming it's already validated.
 * Use when the value comes from a trusted source.
 */
export function asId(value: string): Id {
  return value as Id
}

/**
 * Default system actor EntityId for automated operations.
 */
export const SYSTEM_ACTOR: EntityId = 'system/parquedb' as EntityId

// =============================================================================
// Audit Fields
// =============================================================================

/**
 * Standard audit fields present on every entity
 *
 * ParqueDB automatically manages these fields on all entities to provide
 * comprehensive audit trails and support for optimistic concurrency control.
 *
 * @example
 * ```typescript
 * const post = await db.Posts.get('post-1')
 * console.log(`Created by ${post.createdBy} at ${post.createdAt}`)
 * console.log(`Last updated by ${post.updatedBy} at ${post.updatedAt}`)
 * console.log(`Version: ${post.version}`)
 * ```
 */
export interface AuditFields {
  /**
   * Timestamp when the entity was created
   * Automatically set on entity creation
   */
  createdAt: Date

  /**
   * EntityId of the actor who created the entity
   * Used for audit trails and access control
   */
  createdBy: EntityId

  /**
   * Timestamp when the entity was last updated
   * Automatically updated on every modification
   */
  updatedAt: Date

  /**
   * EntityId of the actor who last updated the entity
   * Used for audit trails
   */
  updatedBy: EntityId

  /**
   * Soft delete timestamp (undefined if not deleted)
   * When set, the entity is considered deleted but retained for audit purposes
   */
  deletedAt?: Date | undefined

  /**
   * EntityId of the actor who deleted the entity
   * Only set when deletedAt is set
   */
  deletedBy?: EntityId | undefined

  /**
   * Optimistic concurrency version number
   * Incremented on every update; used to prevent concurrent modification conflicts
   */
  version: number
}

// =============================================================================
// Relationship Types
// =============================================================================

/**
 * A relationship link mapping display names to EntityIds or metadata
 *
 * RelLinks represent the target of a relationship with a human-readable display name
 * as the key and the EntityId as the value. Additional metadata fields may be present.
 *
 * @example
 * ```typescript
 * const authorLink: RelLink = {
 *   'Nathan Clevenger': 'users/nathan-123'
 * }
 * ```
 */
export interface RelLink {
  [displayName: string]: EntityId | number | string | undefined
}

/**
 * A paginated set of relationship links with optional metadata
 *
 * RelSets extend RelLink to include pagination information for relationships
 * that may have many targets (e.g., a post with many comments).
 *
 * @example
 * ```typescript
 * const comments: RelSet = {
 *   'First comment': 'comments/c1',
 *   'Second comment': 'comments/c2',
 *   $count: 150,
 *   $next: 'cursor-abc123'
 * }
 * ```
 */
export interface RelSet extends RelLink {
  /**
   * Total count of relationships (when requested)
   * May be present even when not all relationships are loaded
   */
  $count?: number | undefined

  /**
   * Cursor for fetching the next page of relationships
   * Use with the cursor option in related() or find() calls
   */
  $next?: string | undefined
}

/**
 * Match mode for relationship matching
 *
 * Controls how relationships are established between entities:
 *
 * | Mode   | Description                                    | Use Case                          |
 * |--------|------------------------------------------------|-----------------------------------|
 * | exact  | User explicitly linked entities                | Manual relationships              |
 * | fuzzy  | Approximate match via similarity algorithms    | Entity resolution, auto-linking   |
 *
 * @example
 * ```typescript
 * // Schema definition with fuzzy relationships
 * const schema = {
 *   Article: {
 *     topics: '~> Topic.articles[]' // Fuzzy forward relationship
 *   }
 * }
 * ```
 */
export type MatchMode = 'exact' | 'fuzzy'

/**
 * Relationship definition stored in rels.parquet files
 *
 * Relationships in ParqueDB are bidirectional and stored in two indexes:
 * - Forward index (rels/forward/*.parquet): Sorted by fromNs, fromId, predicate
 * - Reverse index (rels/reverse/*.parquet): Sorted by toNs, toId, reverse
 *
 * This dual storage enables efficient traversal in both directions.
 *
 * @example
 * ```typescript
 * // A "post authored by user" relationship
 * const rel: Relationship = {
 *   fromNs: 'posts' as Namespace,
 *   fromId: 'post-1' as Id,
 *   fromType: 'Post',
 *   fromName: 'My Blog Post',
 *   predicate: 'author',
 *   reverse: 'posts',
 *   toNs: 'users' as Namespace,
 *   toId: 'user-1' as Id,
 *   toType: 'User',
 *   toName: 'Nathan',
 *   createdAt: new Date(),
 *   createdBy: 'system/parquedb' as EntityId,
 *   version: 1
 * }
 * ```
 */
export interface Relationship {
  /** Namespace of the source entity */
  fromNs: Namespace

  /** ID of the source entity within its namespace */
  fromId: Id

  /** Type name of the source entity (e.g., "Post", "User") */
  fromType: string

  /** Human-readable display name of the source entity */
  fromName: string

  /**
   * Outbound relationship name (predicate)
   * Used when traversing from source to target (e.g., "author", "category")
   */
  predicate: string

  /**
   * Inbound relationship name (reverse)
   * Used when traversing from target to source (e.g., "posts", "items")
   */
  reverse: string

  /** Namespace of the target entity */
  toNs: Namespace

  /** ID of the target entity within its namespace */
  toId: Id

  /** Type name of the target entity */
  toType: string

  /** Human-readable display name of the target entity */
  toName: string

  // ===========================================================================
  // Shredded Fields (top-level columns for efficient querying)
  // ===========================================================================

  /**
   * How the relationship was matched
   *
   * SHREDDED: Stored as top-level Parquet column for predicate pushdown.
   */
  matchMode?: MatchMode | undefined

  /**
   * Similarity score for fuzzy matches (0.0 to 1.0)
   *
   * SHREDDED: Stored as top-level Parquet column for predicate pushdown.
   * Only meaningful when matchMode is 'fuzzy'.
   */
  similarity?: number | undefined

  // ===========================================================================
  // Audit Fields
  // ===========================================================================

  /** When the relationship was created */
  createdAt: Date
  /** Who created the relationship */
  createdBy: EntityId
  /** Soft delete timestamp */
  deletedAt?: Date | undefined
  /** Who deleted the relationship */
  deletedBy?: EntityId | undefined
  /** Optimistic concurrency version */
  version: number

  /** Optional edge properties (Variant) - remaining metadata after shredding */
  data?: import('./common').EdgeData | undefined
}

// =============================================================================
// Generic Constraints
// =============================================================================

/**
 * Base constraint for entity data shapes.
 *
 * This constraint ensures that generic type parameters represent valid entity data:
 * - Must be an object type (not primitives)
 * - Can have string keys with any values
 *
 * Use this as a constraint for generic type parameters when defining entity-related types.
 *
 * @example
 * ```typescript
 * // Valid entity data types
 * interface Post extends EntityData {
 *   title: string
 *   content: string
 *   views: number
 * }
 *
 * // Also valid (plain objects)
 * type UserData = { email: string; age: number }
 *
 * // Constrain a function parameter
 * function processEntity<T extends EntityData>(entity: Entity<T>) { ... }
 * ```
 */
export type EntityData = Record<string, unknown>

/**
 * Reserved fields that are managed by ParqueDB.
 * These fields should not be overwritten in entity data.
 *
 * @internal
 */
export type ReservedEntityFields =
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
 * Utility type to check if a type has reserved fields.
 * Used for compile-time validation of entity data types.
 *
 * @example
 * ```typescript
 * type HasBadFields = HasReservedFields<{ $id: string; title: string }> // true
 * type HasGoodFields = HasReservedFields<{ title: string }> // false
 * ```
 */
export type HasReservedFields<T> = Extract<keyof T, ReservedEntityFields> extends never ? false : true

/**
 * Strict entity data constraint that excludes reserved fields.
 * Use this when you want to ensure entity data doesn't override system fields.
 *
 * This type strips out any reserved field keys from the input type, providing
 * a clean data shape that won't conflict with ParqueDB's internal fields.
 *
 * @example
 * ```typescript
 * // Reserved fields are stripped
 * type CleanData = StrictEntityData<{ $id: string; title: string }>
 * // Result: { title: string }
 * ```
 */
export type StrictEntityData<T extends EntityData = EntityData> = {
  [K in keyof T as K extends ReservedEntityFields ? never : K]: T[K]
}

/**
 * Extract the keys of an entity data type that are valid for updates.
 * Excludes reserved fields.
 *
 * @example
 * ```typescript
 * interface Post { title: string; content: string; views: number }
 * type PostKeys = UpdateableKeys<Post> // 'title' | 'content' | 'views'
 * ```
 */
export type UpdateableKeys<T extends EntityData> = Exclude<keyof T, ReservedEntityFields>

/**
 * Make all properties in T optional, suitable for partial updates.
 * Preserves the type of each property while making it optional.
 *
 * @example
 * ```typescript
 * interface Post { title: string; content: string; views: number }
 * type PostUpdate = PartialEntityData<Post>
 * // { title?: string; content?: string; views?: number }
 * ```
 */
export type PartialEntityData<T extends EntityData> = {
  [K in keyof StrictEntityData<T>]?: StrictEntityData<T>[K]
}

// =============================================================================
// Entity Types
// =============================================================================

/**
 * Minimal entity reference containing only identity information
 *
 * EntityRef is used in relationship links and anywhere a lightweight
 * reference to an entity is needed without full data.
 *
 * @example
 * ```typescript
 * const authorRef: EntityRef = {
 *   $id: 'users/nathan-123' as EntityId,
 *   $type: 'User',
 *   name: 'Nathan Clevenger'
 * }
 * ```
 */
export interface EntityRef {
  /**
   * Full entity identifier in "ns/id" format
   * This is the canonical identifier for the entity
   */
  $id: EntityId

  /**
   * Entity type name (e.g., "User", "Post", "Comment")
   * Corresponds to the schema type definition
   */
  $type: string

  /**
   * Human-readable display name
   * Used in UI, relationship links, and search results
   */
  name: string
}

/**
 * Base entity structure as stored in data.parquet files
 *
 * This is the internal storage format. When entities are returned via the API,
 * they are transformed to the Entity type with $id instead of separate ns/id fields.
 *
 * @internal
 */
export interface EntityRecord {
  /** Namespace (collection name) */
  ns: Namespace

  /** Entity ID within the namespace */
  id: Id

  /** Entity type name */
  type: string

  /** Human-readable display name */
  name: string

  // Audit fields (denormalized for query efficiency)
  /** Creation timestamp */
  createdAt: Date
  /** Creator EntityId */
  createdBy: EntityId
  /** Last update timestamp */
  updatedAt: Date
  /** Last updater EntityId */
  updatedBy: EntityId
  /** Soft deletion timestamp */
  deletedAt?: Date | undefined
  /** Deleter EntityId */
  deletedBy?: EntityId | undefined
  /** Optimistic concurrency version */
  version: number

  /**
   * Flexible data payload stored as Variant type
   * Contains all user-defined fields not part of the base schema
   */
  data: Record<string, unknown>
}

/**
 * Full entity as returned by API (includes relationships)
 *
 * The generic parameter TData allows typing of the entity's data fields.
 * When TData is specified (e.g., Entity<Post>), the entity's data properties
 * are typed according to TData.
 *
 * @typeParam TData - The shape of the entity's custom data fields. Must extend EntityData.
 *                    Defaults to EntityData (Record<string, unknown>) for maximum flexibility.
 *
 * @example
 * ```typescript
 * interface Post { title: string; content: string; views: number }
 * const post: Entity<Post> = await db.Posts.get('post-1')
 * console.log(post.title) // string - properly typed!
 * console.log(post.$id)   // EntityId - always available
 * ```
 *
 * @example
 * ```typescript
 * // Without a type parameter, all custom fields are unknown
 * const entity: Entity = await db.collection('items').get('item-1')
 * const title = entity.title // unknown
 * ```
 */
export type Entity<TData = EntityData> = EntityRef & AuditFields & TData & {
  /** Data fields and relationships - allows additional properties */
  [key: string]: unknown
}

/**
 * Strictly-typed entity with enforced data shape constraints
 *
 * Use this type when you want to ensure the generic type parameter
 * is a valid EntityData shape. This provides compile-time checking
 * that the data type doesn't conflict with reserved fields.
 *
 * @typeParam TData - Must extend EntityData
 *
 * @example
 * ```typescript
 * interface Post { title: string; content: string; views: number }
 *
 * // Compile-time verification that Post is valid entity data
 * const post: StrictEntity<Post> = await db.Posts.get('post-1')
 * ```
 */
export type StrictEntity<TData extends EntityData> = Entity<StrictEntityData<TData>>

// =============================================================================
// Input Types
// =============================================================================

/**
 * Input for creating a new entity
 *
 * CreateInput defines the shape of data passed to create() or insert() operations.
 * The $type and name fields are required; all other fields are user-defined.
 *
 * @typeParam T - Optional type parameter for strongly-typed data fields
 *
 * @example
 * ```typescript
 * // Create a new post
 * const input: CreateInput = {
 *   $type: 'Post',
 *   name: 'My First Post',
 *   title: 'Hello World',
 *   content: 'Welcome to my blog!',
 *   status: 'draft',
 *   // Relationships can be specified inline
 *   author: { 'Nathan': 'users/nathan-123' }
 * }
 *
 * const post = await db.Posts.create(input)
 * ```
 */
export interface CreateInput<_T = EntityData> {
  /**
   * Entity type name (required)
   * Must match a type defined in the schema if schema validation is enabled
   */
  $type: string

  /**
   * Human-readable display name (required)
   * Used in relationship links, search results, and UI displays
   */
  name: string

  /**
   * Additional data fields and relationship definitions
   * Relationships can be specified as: { displayName: entityId }
   */
  [key: string]: unknown
}

/**
 * Input for replacing an entity entirely
 *
 * ReplaceInput extends CreateInput with an optional version for optimistic concurrency.
 * Unlike UpdateInput (which uses operators), ReplaceInput completely overwrites the entity.
 *
 * @typeParam T - Optional type parameter for strongly-typed data fields
 *
 * @example
 * ```typescript
 * const replacement: ReplaceInput = {
 *   $type: 'Post',
 *   name: 'Updated Post Title',
 *   title: 'New Title',
 *   content: 'Completely new content',
 *   version: 5 // Fails if entity version !== 5
 * }
 *
 * await db.Posts.replace('post-1', replacement)
 * ```
 */
export interface ReplaceInput<T = EntityData> extends CreateInput<T> {
  /**
   * Expected version for optimistic concurrency control
   * If specified and doesn't match current version, the operation fails
   */
  version?: number | undefined
}

// =============================================================================
// Variant Type (Semi-structured data)
// =============================================================================

/**
 * Supported primitive types in Variant columns
 *
 * These are the atomic value types that can be stored in Parquet Variant columns.
 * Variant is ParqueDB's approach to semi-structured data, allowing flexible schemas
 * while maintaining query performance through optional field shredding.
 */
export type VariantPrimitive =
  | null
  | boolean
  | number
  | string
  | Date

/**
 * Variant value (recursive semi-structured data)
 *
 * VariantValue can be a primitive, array, or nested object. This recursive type
 * allows storing arbitrary JSON-like structures in the Variant column.
 *
 * @example
 * ```typescript
 * const value: VariantValue = {
 *   tags: ['parquet', 'database'],
 *   metadata: {
 *     views: 1000,
 *     featured: true
 *   }
 * }
 * ```
 */
export type VariantValue =
  | VariantPrimitive
  | VariantValue[]
  | { [key: string]: VariantValue }

/**
 * Variant object stored in entity data columns
 *
 * The Variant type is the foundation of ParqueDB's flexible schema system.
 * User-defined fields are stored in a Variant column, with frequently-queried
 * fields optionally "shredded" into separate columns for predicate pushdown.
 *
 * @see {@link https://github.com/apache/parquet-format/blob/master/Variant.md} Parquet Variant specification
 *
 * @example
 * ```typescript
 * const data: Variant = {
 *   title: 'My Post',
 *   views: 1000,
 *   tags: ['tech', 'database'],
 *   metadata: { featured: true }
 * }
 * ```
 */
export type Variant = Record<string, VariantValue>

// =============================================================================
// Event Types (CDC / WAL)
// =============================================================================

/**
 * Event operation types for the Change Data Capture (CDC) log
 *
 * ParqueDB records all mutations as events, enabling:
 * - Time-travel queries (asOf)
 * - Audit trails
 * - Event sourcing patterns
 * - Replication and sync
 *
 * | Operation   | Description                           |
 * |-------------|---------------------------------------|
 * | CREATE      | New entity created                    |
 * | UPDATE      | Entity data modified                  |
 * | DELETE      | Entity deleted (soft or hard)         |
 * | REL_CREATE  | Relationship created between entities |
 * | REL_DELETE  | Relationship removed                  |
 */
export type EventOp = 'CREATE' | 'UPDATE' | 'DELETE' | 'REL_CREATE' | 'REL_DELETE'

/**
 * Relationship event payload for CDC events
 *
 * This structure is stored in Event.after for REL_CREATE events
 * and Event.before for REL_DELETE events, capturing the full
 * relationship metadata.
 *
 * @example
 * ```typescript
 * const relEvent: RelationshipEventData = {
 *   predicate: 'author',
 *   reverse: 'posts',
 *   fromNs: 'posts',
 *   fromId: 'post-1',
 *   toNs: 'users',
 *   toId: 'user-1'
 * }
 * ```
 */
export interface RelationshipEventData {
  /** Outbound relationship name (e.g., "author", "categories") */
  predicate: string

  /** Inbound relationship name (e.g., "posts", "items") */
  reverse: string

  /** Namespace of the source entity */
  fromNs: string

  /** ID of the source entity */
  fromId: string

  /** Namespace of the target entity */
  toNs: string

  /** ID of the target entity */
  toId: string

  /** Optional edge properties/metadata */
  data?: Record<string, unknown> | undefined
}

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
  before?: Variant | undefined
  /** State after change (undefined for DELETE) */
  after?: Variant | undefined
  /** Who made the change (e.g., "users:admin") */
  actor?: string | undefined
  /** Additional metadata (request ID, correlation ID, etc.) */
  metadata?: Variant | undefined
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

/**
 * Result of an update operation (updateOne or updateMany)
 *
 * Follows MongoDB-style result format for familiarity.
 *
 * @example
 * ```typescript
 * const result = await db.Posts.updateMany(
 *   { status: 'draft' },
 *   { $set: { status: 'published' } }
 * )
 * console.log(`Updated ${result.modifiedCount} of ${result.matchedCount} posts`)
 * ```
 */
export interface UpdateResult {
  /**
   * Number of documents that matched the filter
   * May be higher than modifiedCount if some were already in desired state
   */
  matchedCount: number

  /**
   * Number of documents that were actually modified
   * Zero if all matched documents already had the target values
   */
  modifiedCount: number
}

/**
 * Result of a delete operation (deleteOne or deleteMany)
 *
 * @example
 * ```typescript
 * const result = await db.Posts.deleteMany({ status: 'archived' })
 * console.log(`Deleted ${result.deletedCount} archived posts`)
 * ```
 */
export interface DeleteResult {
  /** Number of documents that were deleted */
  deletedCount: number
}

/**
 * Paginated result set for list operations
 *
 * Used when fetching large result sets with cursor-based pagination.
 *
 * @typeParam T - Type of items in the result set
 *
 * @example
 * ```typescript
 * let cursor: string | undefined
 * do {
 *   const result: PaginatedResult<Entity> = await db.Posts.find(
 *     { status: 'published' },
 *     { limit: 100, cursor }
 *   )
 *   processItems(result.items)
 *   cursor = result.nextCursor
 * } while (result.hasMore)
 * ```
 */
export interface PaginatedResult<T> {
  /** Array of result items for this page */
  items: T[]

  /**
   * Total count of matching items (if available)
   * May require additional query to compute; not always present
   */
  total?: number | undefined

  /**
   * Opaque cursor for fetching the next page
   * Pass to cursor option in subsequent queries
   */
  nextCursor?: string | undefined

  /**
   * Whether there are more results after this page
   * True if nextCursor can be used to fetch more
   */
  hasMore: boolean
}

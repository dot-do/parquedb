/**
 * JSONL Line Types for the MergeTree Engine
 *
 * Every mutation in ParqueDB's MergeTree engine is a single line of JSONL.
 * There are four line types:
 *
 * - DataLine: entity mutations (create/update/delete) written to {table}.jsonl
 * - RelLine: relationship mutations (link/unlink) written to rels.jsonl
 * - EventLine: CDC/audit events written to events.jsonl
 * - SchemaLine: schema definitions/migrations written to events.jsonl
 */

// =============================================================================
// Operation Type Literals
// =============================================================================

/** DataLine operations: create, update, delete */
export type DataOp = 'c' | 'u' | 'd'

/** RelLine operations: link, unlink */
export type RelOp = 'l' | 'u'

/** EventLine operations: create, update, delete */
export type EventOp = 'c' | 'u' | 'd'

/** SchemaLine operation */
export type SchemaOp = 's'

// =============================================================================
// DataLine -- entity state (full entity after mutation)
// =============================================================================

/**
 * DataLine represents entity mutations written to {table}.jsonl.
 *
 * System fields are prefixed with `$`. All other fields are entity data.
 * On delete operations, only system fields are present.
 *
 * @example
 * // Create
 * {"$id":"01J5A..","$op":"c","$v":1,"$ts":1738857600000,"name":"Alice","email":"alice@a.co"}
 * // Update
 * {"$id":"01J5A..","$op":"u","$v":2,"$ts":1738857600050,"name":"Alice Smith","email":"alice@a.co"}
 * // Delete
 * {"$id":"01J5B..","$op":"d","$v":2,"$ts":1738857600099}
 */
export interface DataLine {
  /** Entity ID (ULID) */
  $id: string
  /** Operation: create, update, delete */
  $op: DataOp
  /** Version number, monotonically increasing per entity */
  $v: number
  /** Epoch milliseconds timestamp */
  $ts: number
  /** Entity data fields (any additional key-value pairs) */
  [key: string]: unknown
}

// =============================================================================
// RelLine -- relationship mutation
// =============================================================================

/**
 * RelLine represents relationship mutations written to rels.jsonl.
 *
 * @example
 * // Link
 * {"$op":"l","$ts":1738857600000,"f":"01J5A..","p":"author","r":"posts","t":"01J5X.."}
 * // Unlink
 * {"$op":"u","$ts":1738857600050,"f":"01J5A..","p":"author","r":"posts","t":"01J5X.."}
 */
export interface RelLine {
  /** Operation: link or unlink */
  $op: RelOp
  /** Epoch milliseconds timestamp */
  $ts: number
  /** From entity $id */
  f: string
  /** Predicate (forward relationship name) */
  p: string
  /** Reverse relationship name */
  r: string
  /** To entity $id */
  t: string
}

// =============================================================================
// EventLine -- CDC/audit event
// =============================================================================

/**
 * EventLine represents CDC/audit events written to events.jsonl.
 *
 * @example
 * {"id":"01J5Z..","ts":1738857600000,"op":"c","ns":"users","eid":"01J5A..","after":{"name":"Alice"}}
 */
export interface EventLine {
  /** Unique event ID (ULID) */
  id: string
  /** Epoch milliseconds timestamp */
  ts: number
  /** Operation: create, update, delete */
  op: 'c' | 'u' | 'd'
  /** Table/namespace name */
  ns: string
  /** Entity $id */
  eid: string
  /** Previous state (present on update and delete) */
  before?: Record<string, unknown>
  /** New state (present on create and update) */
  after?: Record<string, unknown>
  /** Who performed the operation */
  actor?: string
}

// =============================================================================
// SchemaLine -- schema definition/migration
// =============================================================================

/**
 * Migration describes what changed in a schema evolution.
 */
export interface Migration {
  /** Newly added field names */
  added?: string[]
  /** Dropped field names */
  dropped?: string[]
  /** Renamed fields: { oldName: newName } */
  renamed?: Record<string, string>
  /** Changed field types: { fieldName: newType } */
  changed?: Record<string, string>
  /** Default values for new fields: { fieldName: defaultValue } */
  default?: Record<string, unknown>
}

/**
 * SchemaLine represents schema definitions/migrations written to events.jsonl.
 *
 * @example
 * // Initial schema
 * {"id":"01J..","ts":1738857600000,"op":"s","ns":"users","schema":{"name":"string","email":"string"}}
 * // Migration
 * {"id":"01J..","ts":1738857700000,"op":"s","ns":"users","schema":{...},"migration":{"added":["role"]}}
 */
export interface SchemaLine {
  /** Unique event ID (ULID) */
  id: string
  /** Epoch milliseconds timestamp */
  ts: number
  /** Operation: always 's' for schema */
  op: SchemaOp
  /** Table/namespace name */
  ns: string
  /** Full schema at this point: { fieldName: typeString } */
  schema: Record<string, string>
  /** Migration details (what changed) */
  migration?: Migration
}

// =============================================================================
// Union Type
// =============================================================================

/** Any JSONL line type in the MergeTree engine */
export type Line = DataLine | RelLine | EventLine | SchemaLine

// =============================================================================
// Update Operators (engine-level)
// =============================================================================

export interface UpdateOps {
  $set?: Record<string, unknown>
  $inc?: Record<string, number>
  $unset?: Record<string, boolean>
}

// =============================================================================
// Find Options (engine-level)
// =============================================================================

export interface FindOptions {
  limit?: number
  skip?: number
  sort?: Record<string, 1 | -1>
}

// =============================================================================
// Branded Types
// =============================================================================

/** Branded type for entity IDs */
export type EntityId = string & { readonly __brand: 'EntityId' }

/** Branded type for table/namespace names */
export type TableName = string & { readonly __brand: 'TableName' }

/** Helper to create a branded EntityId from a plain string */
export function entityId(id: string): EntityId {
  return id as EntityId
}

/** Helper to create a branded TableName from a plain string */
export function tableName(name: string): TableName {
  return name as TableName
}

/**
 * ParqueDB Namespace-Sharded Architecture - TypeScript Schema Definitions
 *
 * These types define the Parquet schemas for namespace-sharded multi-file storage.
 */

// ============================================================================
// Variant Type (Reused from graph-first architecture)
// ============================================================================

export type Variant =
  | { type: 'null' }
  | { type: 'boolean'; value: boolean }
  | { type: 'int64'; value: bigint }
  | { type: 'float64'; value: number }
  | { type: 'string'; value: string }
  | { type: 'binary'; value: Uint8Array }
  | { type: 'date'; value: number }
  | { type: 'timestamp'; value: bigint }
  | { type: 'array'; elements: Variant[] }
  | { type: 'object'; fields: Record<string, Variant> }

// ============================================================================
// Branded Types
// ============================================================================

declare const brand: unique symbol
type Brand<B> = { [brand]: B }

/** Namespace identifier - validated for allowed characters */
export type Namespace = string & Brand<'Namespace'>

/** Entity identifier within a namespace */
export type EntityId = string & Brand<'EntityId'>

/** Schema definition identifier (ULID) */
export type SchemaId = string & Brand<'SchemaId'>

/** Cross-namespace reference identifier (ULID) */
export type RefId = string & Brand<'RefId'>

/** Event identifier (ULID) */
export type EventId = string & Brand<'EventId'>

/** Timestamp in microseconds since epoch */
export type Timestamp = bigint & Brand<'Timestamp'>

// Constructors with validation
export function createNamespace(ns: string): Namespace {
  if (!isValidNamespace(ns)) {
    throw new Error(`Invalid namespace: ${ns}. Must match [a-z0-9-_.] and not start with underscore.`)
  }
  return ns as Namespace
}

export function isValidNamespace(ns: string): boolean {
  return /^[a-z0-9][a-z0-9-_.]{0,127}$/.test(ns)
}

export const createEntityId = (id: string): EntityId => id as EntityId
export const createSchemaId = (id: string): SchemaId => id as SchemaId
export const createRefId = (id: string): RefId => id as RefId
export const createEventId = (id: string): EventId => id as EventId
export const createTimestamp = (ts: bigint): Timestamp => ts as Timestamp

// ============================================================================
// System Constants
// ============================================================================

/** Reserved system namespaces (prefixed with underscore) */
export const SYSTEM_NAMESPACES = {
  SYSTEM: '_system',
  REFS: '_refs',
  ARCHIVE: '_archive',
  TEMP: '_temp',
} as const

/** Standard files within a namespace */
export const NAMESPACE_FILES = {
  DATA: 'data.parquet',
  EDGES: 'edges.parquet',
  EVENTS: 'events.parquet',
  SCHEMA: '_schema.parquet',
  META: '_meta.parquet',
  SHARDS_DIR: '_shards',
} as const

/** System files */
export const SYSTEM_FILES = {
  SCHEMA: `${SYSTEM_NAMESPACES.SYSTEM}/schema.parquet`,
  NAMESPACES: `${SYSTEM_NAMESPACES.SYSTEM}/namespaces.parquet`,
  REFS: `${SYSTEM_NAMESPACES.SYSTEM}/refs.parquet`,
  EVENTS: `${SYSTEM_NAMESPACES.SYSTEM}/events.parquet`,
  CONFIG: `${SYSTEM_NAMESPACES.SYSTEM}/config.parquet`,
} as const

// ============================================================================
// _system/namespaces.parquet Schema
// ============================================================================

/** Schema inheritance mode */
export type SchemaMode = 'global' | 'local' | 'hybrid'

/** Multi-tenant isolation level */
export type IsolationLevel = 'strict' | 'shared'

/** Namespace status */
export type NamespaceStatus = 'active' | 'suspended' | 'archived' | 'deleted'

/** Sharding strategy */
export type ShardStrategy = 'none' | 'type' | 'time' | 'hash'

/**
 * Namespace registry record stored in _system/namespaces.parquet
 */
export interface NamespaceRecord {
  // Identity
  ns: Namespace
  display_name: string

  // Ownership
  owner_id: string
  created_at: Timestamp
  created_by: string

  // Configuration
  schema_mode: SchemaMode
  isolation_level: IsolationLevel

  // Quotas
  max_entities: bigint | null
  max_storage_bytes: bigint | null

  // Status
  status: NamespaceStatus

  // Sharding
  shard_strategy: ShardStrategy
  shard_count: number

  // Time-travel
  retention_days: number
  checkpoint_interval_hours: number

  // Metadata
  metadata: Variant
}

export const NAMESPACE_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  display_name: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  owner_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  schema_mode: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  isolation_level: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  max_entities: { type: 'INT64', encoding: 'PLAIN', optional: true },
  max_storage_bytes: { type: 'INT64', encoding: 'PLAIN', optional: true },
  status: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  shard_strategy: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  shard_count: { type: 'INT32', encoding: 'PLAIN' },
  retention_days: { type: 'INT32', encoding: 'PLAIN' },
  checkpoint_interval_hours: { type: 'INT32', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// _system/schema.parquet Schema
// ============================================================================

/** Relationship operator from graphdl */
export type RelationshipOperator = '->' | '~>' | '<-' | '<~'

/**
 * Field definition within a schema
 */
export interface FieldDefinition {
  name: string
  type: string
  required: boolean
  array: boolean

  // For relationships
  is_relation: boolean
  relation_operator: RelationshipOperator | null
  relation_target: string | null
  relation_backref: string | null

  // Validation constraints
  constraints: Variant
}

/**
 * Schema definition stored in _system/schema.parquet
 */
export interface SchemaDefinition {
  // Identity
  schema_id: SchemaId
  type_name: string
  namespace: Namespace | null  // null = global scope

  // Schema.org/graphdl type
  schema_type: string

  // Version
  version: number
  created_at: Timestamp
  deprecated_at: Timestamp | null

  // Definition
  definition: Variant          // Full graphdl definition
  fields: Variant              // Array<FieldDefinition>
  relationships: Variant       // Extracted relationship definitions
  validators: Variant          // JSON Schema validators

  // Documentation
  description: string
  metadata: Variant
}

export const SCHEMA_PARQUET_SCHEMA = {
  schema_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  type_name: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  namespace: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  schema_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  version: { type: 'INT32', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  deprecated_at: { type: 'INT64', encoding: 'PLAIN', optional: true },
  definition: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  fields: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  relationships: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  validators: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  description: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// _system/refs.parquet Schema
// ============================================================================

/**
 * Cross-namespace reference stored in _system/refs.parquet
 */
export interface CrossNamespaceRef {
  // Reference identity
  ref_id: RefId

  // Source
  source_ns: Namespace
  source_id: EntityId
  source_type: string

  // Target
  target_ns: Namespace
  target_id: EntityId
  target_type: string

  // Relationship
  rel_type: string
  operator: RelationshipOperator
  bidirectional: boolean

  // Metadata
  created_at: Timestamp
  created_by: string

  // Reference properties
  data: Variant
  confidence: number | null  // For fuzzy references

  // Status
  deleted: boolean
  verified: boolean
  last_verified_at: Timestamp | null
}

export const REFS_PARQUET_SCHEMA = {
  ref_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  source_ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  source_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  source_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  target_ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  target_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  target_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  rel_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  operator: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  confidence: { type: 'FLOAT', encoding: 'PLAIN', optional: true },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
  verified: { type: 'BOOLEAN', encoding: 'PLAIN' },
  last_verified_at: { type: 'INT64', encoding: 'PLAIN', optional: true },
} as const

// ============================================================================
// {ns}/data.parquet Schema
// ============================================================================

/**
 * Entity stored in {ns}/data.parquet
 *
 * Note: No namespace field - it's implicit from the file path.
 * Sort order: (type, id, ts DESC)
 */
export interface NamespaceEntity {
  // Identity
  id: EntityId
  type: string

  // Version
  ts: Timestamp
  version: number

  // Data
  data: Variant

  // Audit
  created_at: Timestamp
  created_by: string
  updated_at: Timestamp
  updated_by: string

  // Status
  deleted: boolean

  // Cross-namespace tracking
  external_refs: Variant | null  // Array of RefId
}

export const DATA_PARQUET_SCHEMA = {
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  version: { type: 'INT32', encoding: 'PLAIN' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  created_at: { type: 'INT64', encoding: 'PLAIN' },
  created_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  updated_at: { type: 'INT64', encoding: 'PLAIN' },
  updated_by: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
  external_refs: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
} as const

// ============================================================================
// {ns}/edges.parquet Schema (Optional graph mode)
// ============================================================================

/**
 * Edge stored in {ns}/edges.parquet (for graph-enabled namespaces)
 *
 * Sort order: (from_id, rel_type, to_id, ts DESC)
 */
export interface NamespaceEdge {
  from_id: EntityId
  to_id: EntityId
  rel_type: string

  ts: Timestamp
  version: number

  data: Variant
  operator: RelationshipOperator
  bidirectional: boolean
  confidence: number | null

  deleted: boolean
}

export const EDGES_PARQUET_SCHEMA = {
  from_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  to_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  rel_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  version: { type: 'INT32', encoding: 'PLAIN' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  operator: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  confidence: { type: 'FLOAT', encoding: 'PLAIN', optional: true },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

// ============================================================================
// {ns}/events.parquet Schema
// ============================================================================

/** CDC operation type */
export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE'

/**
 * CDC event stored in {ns}/events.parquet
 *
 * Sort order: (ts, event_id)
 */
export interface NamespaceEvent {
  // Event identity
  event_id: EventId

  // Target
  entity_id: EntityId
  entity_type: string

  // Event
  ts: Timestamp
  op: CDCOperation

  // State
  before: Variant | null
  after: Variant | null

  // Change details
  changed_fields: string[]

  // Context
  tx_id: string
  user_id: string
  trace_id: string | null

  // Cross-namespace
  triggers_external: boolean

  // Metadata
  metadata: Variant
}

export const EVENTS_PARQUET_SCHEMA = {
  event_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  entity_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  entity_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  op: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  before: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  after: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  changed_fields: { type: 'LIST', element: { type: 'BYTE_ARRAY' } },
  tx_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  user_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  trace_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  triggers_external: { type: 'BOOLEAN', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// {ns}/_meta.parquet Schema
// ============================================================================

/** Metadata stat type */
export type MetaStatType = 'count' | 'checkpoint' | 'config' | 'index' | 'shard'

/**
 * Namespace metadata stored in {ns}/_meta.parquet
 */
export interface NamespaceMetadataRecord {
  stat_type: MetaStatType

  // For count stats
  entity_type: string | null
  entity_count: bigint | null

  // For checkpoints
  checkpoint_ts: Timestamp | null
  checkpoint_event_id: EventId | null

  // For index stats
  index_name: string | null
  index_size_bytes: bigint | null

  // For shard info
  shard_id: string | null
  shard_path: string | null

  // Common
  updated_at: Timestamp
  data: Variant
}

export const META_PARQUET_SCHEMA = {
  stat_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  entity_type: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  entity_count: { type: 'INT64', encoding: 'PLAIN', optional: true },
  checkpoint_ts: { type: 'INT64', encoding: 'PLAIN', optional: true },
  checkpoint_event_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  index_name: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  index_size_bytes: { type: 'INT64', encoding: 'PLAIN', optional: true },
  shard_id: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  shard_path: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  updated_at: { type: 'INT64', encoding: 'PLAIN' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// _system/events.parquet Schema (Global event sequence)
// ============================================================================

/**
 * Global event sequence for cross-namespace ordering
 */
export interface GlobalEventSequence {
  seq: bigint                   // Global sequence number
  ns: Namespace
  event_id: EventId
  ts: Timestamp
}

export const GLOBAL_EVENTS_PARQUET_SCHEMA = {
  seq: { type: 'INT64', encoding: 'PLAIN' },
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  event_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  ts: { type: 'INT64', encoding: 'PLAIN' },
} as const

// ============================================================================
// Query Types
// ============================================================================

/**
 * Filter operators for queries
 */
export type FilterOperator<T = unknown> =
  | { eq: T }
  | { neq: T }
  | { gt: T }
  | { gte: T }
  | { lt: T }
  | { lte: T }
  | { in: T[] }
  | { nin: T[] }
  | { contains: string }
  | { startsWith: string }
  | { endsWith: string }
  | { isNull: boolean }

/**
 * Query filter definition
 */
export type QueryFilter<T = Record<string, unknown>> = {
  [K in keyof T]?: FilterOperator<T[K]>
}

/**
 * Sort specification
 */
export interface SortSpec {
  column: string
  order: 'asc' | 'desc'
}

/**
 * Query options
 */
export interface QueryOptions {
  columns?: string[]
  filter?: QueryFilter
  sort?: SortSpec[]
  limit?: number
  offset?: number
  asOf?: Timestamp
}

// ============================================================================
// Sharding Types
// ============================================================================

/**
 * Shard metadata
 */
export interface ShardMetadata {
  ns: Namespace
  shard_id: string
  strategy: ShardStrategy

  // Stats
  entity_count: bigint
  file_size_bytes: bigint
  row_group_count: number

  // Bounds
  min_ts: Timestamp | null
  max_ts: Timestamp | null
  entity_type: string | null    // For type sharding
  hash_min: number | null       // For hash sharding
  hash_max: number | null

  // Maintenance
  last_compaction: Timestamp
  needs_compaction: boolean
}

/**
 * Sharding thresholds for automatic sharding
 */
export interface ShardingThresholds {
  maxFileSize: number           // bytes
  maxEntityCount: number
  maxRowGroupCount: number
}

export const DEFAULT_SHARDING_THRESHOLDS: ShardingThresholds = {
  maxFileSize: 1024 * 1024 * 1024,  // 1GB
  maxEntityCount: 10_000_000,
  maxRowGroupCount: 1000,
}

// ============================================================================
// Checkpoint Types
// ============================================================================

/**
 * Checkpoint record
 */
export interface Checkpoint {
  ns: Namespace
  ts: Timestamp
  event_id: EventId | null
  global_seq: bigint | null

  // Snapshot paths
  data_snapshot: string
  edges_snapshot: string | null

  // Stats
  entity_count: bigint
  event_count: bigint
}

// ============================================================================
// Storage Path Helpers
// ============================================================================

export const STORAGE_PATHS = {
  /** System files */
  systemSchema: () => SYSTEM_FILES.SCHEMA,
  systemNamespaces: () => SYSTEM_FILES.NAMESPACES,
  systemRefs: () => SYSTEM_FILES.REFS,
  systemEvents: () => SYSTEM_FILES.EVENTS,

  /** Namespace files */
  namespaceData: (ns: Namespace) => `${ns}/${NAMESPACE_FILES.DATA}`,
  namespaceEdges: (ns: Namespace) => `${ns}/${NAMESPACE_FILES.EDGES}`,
  namespaceEvents: (ns: Namespace) => `${ns}/${NAMESPACE_FILES.EVENTS}`,
  namespaceSchema: (ns: Namespace) => `${ns}/${NAMESPACE_FILES.SCHEMA}`,
  namespaceMeta: (ns: Namespace) => `${ns}/${NAMESPACE_FILES.META}`,

  /** Shards */
  typeShardData: (ns: Namespace, entityType: string) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/type=${entityType}/data.parquet`,
  timeShardData: (ns: Namespace, period: string) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/period=${period}/data.parquet`,
  hashShardData: (ns: Namespace, shardNum: number) =>
    `${ns}/${NAMESPACE_FILES.SHARDS_DIR}/shard=${shardNum}/data.parquet`,

  /** Checkpoints */
  checkpoint: (ns: Namespace, ts: Timestamp) =>
    `${ns}/checkpoints/${ts}/data.parquet`,

  /** Archive */
  archived: (ns: Namespace, file: string) =>
    `${SYSTEM_NAMESPACES.ARCHIVE}/${ns}/${file}`,
} as const

// ============================================================================
// Configuration Types
// ============================================================================

/**
 * Namespace creation configuration
 */
export interface NamespaceConfig {
  display_name?: string
  owner_id?: string
  created_by?: string
  schema_mode?: SchemaMode
  isolation_level?: IsolationLevel
  max_entities?: bigint
  max_storage_bytes?: bigint
  retention_days?: number
  checkpoint_interval_hours?: number
  metadata?: Variant
}

/**
 * Default namespace configuration
 */
export const DEFAULT_NAMESPACE_CONFIG: Required<Omit<NamespaceConfig, 'display_name' | 'metadata'>> = {
  owner_id: 'system',
  created_by: 'system',
  schema_mode: 'hybrid',
  isolation_level: 'strict',
  max_entities: null as unknown as bigint,
  max_storage_bytes: null as unknown as bigint,
  retention_days: 30,
  checkpoint_interval_hours: 24,
}

/**
 * Global store configuration
 */
export interface StoreConfig {
  bucket: string
  prefix: string
  defaultShardingThresholds: ShardingThresholds
  writeBufferSize: number
  writeBufferFlushMs: number
  enableBloomFilters: boolean
  bloomFilterFPR: number
}

export const DEFAULT_STORE_CONFIG: StoreConfig = {
  bucket: 'parquedb',
  prefix: 'warehouse/',
  defaultShardingThresholds: DEFAULT_SHARDING_THRESHOLDS,
  writeBufferSize: 10000,
  writeBufferFlushMs: 1000,
  enableBloomFilters: true,
  bloomFilterFPR: 0.01,
}

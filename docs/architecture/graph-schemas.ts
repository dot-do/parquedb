/**
 * ParqueDB Graph-First Architecture - TypeScript Schema Definitions
 *
 * These types define the Parquet schemas for the graph-first storage model.
 */

// ============================================================================
// Variant Type (Semi-structured data)
// ============================================================================

/**
 * Variant type for semi-structured data storage in Parquet.
 * Inspired by ClickHouse's variant type and Iceberg's variant shredding.
 */
export type Variant =
  | { type: 'null' }
  | { type: 'boolean'; value: boolean }
  | { type: 'int64'; value: bigint }
  | { type: 'float64'; value: number }
  | { type: 'string'; value: string }
  | { type: 'binary'; value: Uint8Array }
  | { type: 'date'; value: number } // days since epoch
  | { type: 'timestamp'; value: bigint } // microseconds since epoch
  | { type: 'array'; elements: Variant[] }
  | { type: 'object'; fields: Record<string, Variant> }

/**
 * JSON-compatible representation for API serialization.
 */
export type VariantJSON =
  | null
  | boolean
  | number
  | string
  | VariantJSON[]
  | { [key: string]: VariantJSON }

// ============================================================================
// Branded Types (Type-safe identifiers)
// ============================================================================

declare const brand: unique symbol
type Brand<B> = { [brand]: B }

/** Namespace identifier (e.g., "example.com/crm") */
export type Namespace = string & Brand<'Namespace'>

/** Entity identifier within a namespace */
export type EntityId = string & Brand<'EntityId'>

/** Relationship type identifier */
export type RelationType = string & Brand<'RelationType'>

/** Event identifier (ULID format) */
export type EventId = string & Brand<'EventId'>

/** Transaction identifier */
export type TransactionId = string & Brand<'TransactionId'>

/** Timestamp in microseconds since epoch */
export type Timestamp = bigint & Brand<'Timestamp'>

// Constructors
export const createNamespace = (ns: string): Namespace => ns as Namespace
export const createEntityId = (id: string): EntityId => id as EntityId
export const createRelationType = (rt: string): RelationType => rt as RelationType
export const createEventId = (id: string): EventId => id as EventId
export const createTransactionId = (id: string): TransactionId => id as TransactionId
export const createTimestamp = (ts: bigint): Timestamp => ts as Timestamp

// ============================================================================
// nodes.parquet Schema
// ============================================================================

/**
 * Node (Entity) stored in nodes.parquet
 *
 * Sort order: (ns, type, id, ts DESC)
 */
export interface Node {
  /** Namespace (e.g., "example.com/crm") */
  ns: Namespace

  /** Entity identifier */
  id: EntityId

  /** Timestamp (microseconds since epoch) */
  ts: Timestamp

  /** Entity type (e.g., "Person", "Organization") */
  type: string

  /** Semi-structured entity payload */
  data: Variant

  /** Optimistic concurrency version */
  version: number

  /** Soft delete flag */
  deleted: boolean

  /** System metadata (created_by, updated_by, etc.) */
  metadata: Variant
}

/**
 * Parquet schema definition for nodes.parquet
 */
export const NODE_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Shredded variant
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// edges.parquet Schema
// ============================================================================

/**
 * Relationship operator from graphdl
 *
 * | Operator | Direction | Match Mode | Use Case |
 * |----------|-----------|------------|----------|
 * | `->` | forward | exact | Foreign key reference |
 * | `~>` | forward | fuzzy | AI-matched semantic reference |
 * | `<-` | backward | exact | Backlink/parent reference |
 * | `<~` | backward | fuzzy | AI-matched backlink |
 */
export type RelationshipOperator = '->' | '~>' | '<-' | '<~'

/**
 * Edge (Relationship) stored in edges.parquet
 *
 * Sort orders (multiple files):
 * - Forward: (ns, from_id, rel_type, to_id, ts DESC)
 * - Reverse: (ns, to_id, rel_type, from_id, ts DESC)
 * - Type: (ns, rel_type, from_id, to_id, ts DESC)
 */
export interface Edge {
  /** Namespace */
  ns: Namespace

  /** Source entity ID */
  from_id: EntityId

  /** Target entity ID */
  to_id: EntityId

  /** Relationship type (e.g., "author", "knows") */
  rel_type: RelationType

  /** Timestamp */
  ts: Timestamp

  /** Edge properties (weight, metadata) */
  data: Variant

  /** graphdl operator */
  operator: RelationshipOperator

  /** If true, implicit reverse edge exists */
  bidirectional: boolean

  /** For fuzzy (~>) relationships: confidence 0.0-1.0 */
  confidence: number | null

  /** Optimistic concurrency version */
  version: number

  /** Soft delete flag */
  deleted: boolean
}

/**
 * Parquet schema definition for edges.parquet
 */
export const EDGE_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  from_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  to_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  rel_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  data: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  operator: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // 4 values max
  bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  confidence: { type: 'FLOAT', encoding: 'PLAIN', optional: true },
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

// ============================================================================
// events.parquet Schema (CDC Log)
// ============================================================================

/** Event target type */
export type EventTarget = 'node' | 'edge'

/** CDC operation type */
export type CDCOperation = 'INSERT' | 'UPDATE' | 'DELETE'

/**
 * CDC Event stored in events.parquet
 *
 * Sort order: (ts, event_id)
 * Partitioning: day={YYYY-MM-DD}
 */
export interface CDCEvent {
  /** Unique event identifier (ULID) */
  event_id: EventId

  /** Target type: "node" or "edge" */
  target: EventTarget

  /** Namespace */
  ns: Namespace

  /** Entity ID (for nodes: id; for edges: from_id|rel_type|to_id) */
  entity_id: string

  /** Event timestamp */
  ts: Timestamp

  /** Operation type */
  op: CDCOperation

  /** Previous state (null for INSERT) */
  before: Variant | null

  /** New state (null for DELETE) */
  after: Variant | null

  /** Transaction ID for grouping related events */
  tx_id: TransactionId

  /** User who made the change */
  user_id: string

  /** Additional context (reason, trace_id) */
  metadata: Variant
}

/**
 * Parquet schema definition for events.parquet
 */
export const EVENT_PARQUET_SCHEMA = {
  event_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  target: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // 2 values
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  entity_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  op: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // 3 values
  before: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  after: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  tx_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  user_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  metadata: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
} as const

// ============================================================================
// Adjacency List Schema (High-degree nodes)
// ============================================================================

/**
 * Outgoing adjacency list entry
 */
export interface OutgoingAdjacency {
  rel_type: RelationType
  to_id: EntityId
  ts: Timestamp
  data: Variant
  operator: RelationshipOperator
  confidence: number | null
}

/**
 * Incoming adjacency list entry
 */
export interface IncomingAdjacency {
  rel_type: RelationType
  from_id: EntityId
  ts: Timestamp
  data: Variant
  operator: RelationshipOperator
  confidence: number | null
}

// ============================================================================
// Materialized Paths Schema
// ============================================================================

/**
 * Pre-computed path stored in paths.parquet
 */
export interface MaterializedPath {
  /** Namespace */
  ns: Namespace

  /** Path type (e.g., "org_hierarchy", "social_2hop") */
  path_type: string

  /** Starting node ID */
  start_id: EntityId

  /** Ending node ID */
  end_id: EntityId

  /** Number of edges in path */
  hops: number

  /** Path as alternating [node1, edge1, node2, edge2, ...] */
  path: string[]

  /** Aggregated edge weights */
  total_weight: number

  /** When this path was computed */
  computed_at: Timestamp

  /** Expiration timestamp */
  valid_until: Timestamp
}

/**
 * Parquet schema definition for paths.parquet
 */
export const PATH_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  path_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  start_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  end_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  hops: { type: 'INT32', encoding: 'PLAIN' },
  path: { type: 'LIST', element: { type: 'BYTE_ARRAY' } },
  total_weight: { type: 'DOUBLE', encoding: 'PLAIN' },
  computed_at: { type: 'INT64', encoding: 'PLAIN' },
  valid_until: { type: 'INT64', encoding: 'PLAIN' },
} as const

// ============================================================================
// Index Metadata
// ============================================================================

/**
 * Row group statistics for predicate pushdown
 */
export interface RowGroupStats<T> {
  rowGroup: number
  rowCount: number
  stats: {
    [K in keyof T]?: {
      min: T[K]
      max: T[K]
      nullCount: number
      distinctCount?: number
    }
  }
}

/**
 * Bloom filter for edge existence checks
 */
export interface EdgeBloomFilter {
  namespace: Namespace
  rowGroup: number
  /** Bloom filter keyed on: hash(from_id + rel_type + to_id) */
  filter: Uint8Array
  /** Expected false positive rate */
  falsePositiveRate: number
  /** Number of edges in this row group */
  edgeCount: number
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Filter operators for queries
 */
export type FilterOperator =
  | { eq: unknown }
  | { neq: unknown }
  | { gt: unknown }
  | { gte: unknown }
  | { lt: unknown }
  | { lte: unknown }
  | { in: unknown[] }
  | { contains: unknown }
  | { startsWith: string }

/**
 * Query filter definition
 */
export type QueryFilter<T> = {
  [K in keyof T]?: FilterOperator
}

/**
 * Traversal options
 */
export interface TraversalOptions {
  /** Maximum results */
  limit?: number

  /** Point-in-time query */
  asOf?: Timestamp

  /** Minimum confidence for fuzzy relationships */
  minConfidence?: number

  /** Include soft-deleted edges */
  includeDeleted?: boolean
}

/**
 * Path traversal result
 */
export interface PathResult {
  /** Node IDs in the path */
  nodes: EntityId[]

  /** Depth of the path */
  depth: number

  /** Edge IDs in the path */
  edges?: string[]

  /** Total path weight (if edges have weights) */
  totalWeight?: number
}

// ============================================================================
// Storage Layout
// ============================================================================

/**
 * R2/fsx storage path conventions
 */
export const STORAGE_PATHS = {
  /** Root warehouse path */
  warehouse: '/warehouse',

  /** Node data files */
  nodes: (ns: Namespace) => `/warehouse/${ns}/nodes/data`,

  /** Node metadata (Iceberg-style) */
  nodeMetadata: (ns: Namespace) => `/warehouse/${ns}/nodes/metadata`,

  /** Forward edge index */
  edgesForward: (ns: Namespace) => `/warehouse/${ns}/edges/forward`,

  /** Reverse edge index */
  edgesReverse: (ns: Namespace) => `/warehouse/${ns}/edges/reverse`,

  /** Type-based edge index */
  edgesType: (ns: Namespace) => `/warehouse/${ns}/edges/type`,

  /** CDC events (day-partitioned) */
  events: (ns: Namespace, day: string) => `/warehouse/${ns}/events/day=${day}`,

  /** Adjacency lists for high-degree nodes */
  adjacency: (ns: Namespace, nodeId: EntityId) =>
    `/warehouse/${ns}/adjacency/${nodeId}`,

  /** Materialized paths */
  paths: (ns: Namespace, pathType: string) =>
    `/warehouse/${ns}/paths/${pathType}`,

  /** Time-travel checkpoints */
  checkpoints: (ns: Namespace, timestamp: string) =>
    `/warehouse/${ns}/checkpoints/${timestamp}`,
} as const

// ============================================================================
// Configuration
// ============================================================================

/**
 * Path materialization configuration
 */
export interface PathMaterializationConfig {
  /** Path type identifier */
  pathType: string

  /** Filter to select starting nodes */
  startNodeFilter: (node: Node) => boolean

  /** Relationship types to traverse */
  relTypes: RelationType[]

  /** Maximum traversal depth */
  maxDepth: number

  /** How to aggregate multiple paths */
  aggregation: 'shortest' | 'all' | 'weighted'

  /** How often to refresh (seconds) */
  refreshInterval: number
}

/**
 * Adjacency list threshold configuration
 */
export interface AdjacencyConfig {
  /** Minimum edges before creating dedicated adjacency files */
  threshold: number

  /** How often to check for high-degree nodes (seconds) */
  checkInterval: number

  /** Include reverse edges in adjacency files */
  includeReverse: boolean
}

/**
 * Graph-first storage configuration
 */
export interface GraphStorageConfig {
  /** R2 bucket for storage */
  bucket: string

  /** Default namespace */
  defaultNamespace: Namespace

  /** Adjacency list settings */
  adjacency: AdjacencyConfig

  /** Enabled path materializations */
  materializedPaths: PathMaterializationConfig[]

  /** Event retention (days) */
  eventRetentionDays: number

  /** Checkpoint interval (hours) */
  checkpointIntervalHours: number

  /** Enable Bloom filters for edge existence */
  enableBloomFilters: boolean

  /** Bloom filter false positive rate */
  bloomFilterFPR: number
}

/**
 * Default configuration
 */
export const DEFAULT_CONFIG: GraphStorageConfig = {
  bucket: 'parquedb',
  defaultNamespace: createNamespace('default'),
  adjacency: {
    threshold: 1000,
    checkInterval: 3600,
    includeReverse: true,
  },
  materializedPaths: [],
  eventRetentionDays: 7,
  checkpointIntervalHours: 24,
  enableBloomFilters: true,
  bloomFilterFPR: 0.01,
}

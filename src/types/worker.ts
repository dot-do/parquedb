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
 */
export interface ParqueDBDOInterface {
  create(ns: string, data: unknown, options?: unknown): Promise<unknown>
  update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
  updateMany(ns: string, filter: unknown, update: unknown, options?: unknown): Promise<unknown>
  delete(ns: string, id: string, options?: unknown): Promise<unknown>
  deleteMany(ns: string, filter: unknown, options?: unknown): Promise<unknown>
  link(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
  unlink(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
  related(ns: string, id: string, options?: unknown): Promise<unknown>
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
  CDN_BUCKET?: R2Bucket

  /** Optional FSX binding for POSIX-style file access */
  FSX?: Fetcher

  /** Workers AI binding for embedding generation */
  AI?: AIBinding

  /** Durable Object namespace for database index (user's database registry) */
  DATABASE_INDEX?: DurableObjectNamespace

  /** Durable Object namespace for rate limiting */
  RATE_LIMITER?: DurableObjectNamespace

  /** Durable Object namespace for backend migrations (batch processing) */
  MIGRATION?: DurableObjectNamespace

  // Note: Caching uses the free Cloudflare Cache API (caches.default), not KV.
  // Cache API provides 500MB on free accounts, 5GB+ on enterprise.
  // No binding needed - caches.default is globally available in Workers.

  /** Environment name (development, staging, production) */
  ENVIRONMENT?: string

  /** Optional secret for authentication */
  AUTH_SECRET?: string

  /** CDN r2.dev URL for public access, e.g. 'https://pub-xxx.r2.dev/parquedb' */
  CDN_R2_DEV_URL?: string
}

// =============================================================================
// RPC Types
// =============================================================================

/**
 * Service binding type for RPC calls between workers
 * Use import type for better tree-shaking
 */
/**
 * Typed stub interface for Durable Object RPC calls
 *
 * Used instead of `as unknown as { ... }` casts when calling DO methods.
 * This provides a single source of truth for the DO RPC contract.
 *
 * @remarks The DO's link/unlink methods expect entity IDs in "ns/id" format.
 */
export interface ParqueDBDOStub {
  // Entity operations
  get(ns: string, id: string, includeDeleted?: boolean): Promise<unknown>
  create(ns: string, data: unknown, options?: unknown): Promise<unknown>
  createMany(ns: string, items: unknown[], options?: unknown): Promise<unknown[]>
  update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
  delete(ns: string, id: string, options?: unknown): Promise<unknown>
  deleteMany(ns: string, ids: string[], options?: unknown): Promise<unknown>

  // Relationship operations (entity IDs in "ns/id" format)
  link(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
  unlink(fromId: string, predicate: string, toId: string, options?: unknown): Promise<void>
  getRelationships(ns: string, id: string, predicate?: string, direction?: 'outbound' | 'inbound'): Promise<unknown[]>

  // Cache invalidation methods
  getInvalidationVersion(ns: string): number
  shouldInvalidate(ns: string, workerVersion: number): boolean

  // Event-sourced entity state
  getEntityFromEvents(ns: string, id: string): Promise<unknown>
}

export type ParqueDBService = Fetcher

/**
 * RPC request context
 */
export interface RpcContext {
  /** Request ID for tracing */
  requestId: string

  /** Actor making the request (for audit trails) */
  actor?: string

  /** Timestamp of the request */
  timestamp: Date

  /** Optional correlation ID for distributed tracing */
  correlationId?: string
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
  partitions?: number

  /** Custom partition key function (for 'partition' strategy) */
  partitionKey?: (ns: string, id?: string) => string
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
  committedAt?: Date
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
  id?: string

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

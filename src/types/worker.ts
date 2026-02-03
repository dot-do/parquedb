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
 */
export interface ParqueDBDOStub {
  get(ns: string, id: string): Promise<unknown>
  find(ns: string, filter?: unknown, options?: unknown): Promise<unknown>
  create(ns: string, data: unknown, options?: unknown): Promise<unknown>
  update(ns: string, id: string, update: unknown, options?: unknown): Promise<unknown>
  updateMany(ns: string, filter: unknown, update: unknown, options?: unknown): Promise<unknown>
  delete(ns: string, id: string, options?: unknown): Promise<unknown>
  deleteMany(ns: string, filter: unknown, options?: unknown): Promise<unknown>
  link(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
  unlink(fromNs: string, fromId: string, predicate: string, toNs: string, toId: string): Promise<void>
  related(ns: string, id: string, options?: unknown): Promise<unknown>
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

  /** Relationships table */
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
      data TEXT,
      PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
    )
  `,

  /** Event log table (before flush to Parquet) */
  events: `
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      ts TEXT NOT NULL,
      target TEXT NOT NULL,
      op TEXT NOT NULL,
      ns TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      before TEXT,
      after TEXT,
      actor TEXT NOT NULL,
      metadata TEXT,
      flushed INTEGER NOT NULL DEFAULT 0
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

  /** Indexes for common queries */
  indexes: [
    'CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)',
    'CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)',
    'CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)',
    'CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)',
    'CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)',
    'CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)',
  ],
} as const

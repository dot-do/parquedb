/**
 * Pluggable Entity Backend Interface
 *
 * Allows ParqueDB to use different table formats for entity storage:
 * - Native: ParqueDB's simple Parquet format
 * - Iceberg: Apache Iceberg format (compatible with DuckDB, Spark, Snowflake)
 * - Delta Lake: Delta Lake format
 *
 * Relationships are always stored in ParqueDB's format regardless of entity backend.
 */

import type { Entity, EntityData, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'

// =============================================================================
// Backend Type Identifiers
// =============================================================================

/** Supported backend types */
export type BackendType = 'native' | 'iceberg' | 'delta'

// =============================================================================
// Entity Backend Interface
// =============================================================================

/**
 * Entity storage backend interface
 *
 * Implementations handle entity CRUD operations using different table formats.
 * The relationship index is managed separately by ParqueDB.
 */
export interface EntityBackend {
  // ===========================================================================
  // Metadata
  // ===========================================================================

  /** Backend type identifier */
  readonly type: BackendType

  /** Whether this backend supports time-travel queries */
  readonly supportsTimeTravel: boolean

  /** Whether this backend supports schema evolution */
  readonly supportsSchemaEvolution: boolean

  /** Whether this backend is read-only */
  readonly readOnly: boolean

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Initialize the backend (create tables, load metadata, etc.)
   */
  initialize(): Promise<void>

  /**
   * Close the backend and release resources
   */
  close(): Promise<void>

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Get a single entity by ID
   */
  get<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    options?: GetOptions<T> | undefined
  ): Promise<Entity<T> | null>

  /**
   * Find entities matching a filter
   */
  find<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter | undefined,
    options?: FindOptions<T> | undefined
  ): Promise<Entity<T>[]>

  /**
   * Count entities matching a filter
   */
  count(ns: string, filter?: Filter): Promise<number>

  /**
   * Check if an entity exists
   */
  exists(ns: string, id: string): Promise<boolean>

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  /**
   * Create a new entity
   */
  create<T extends EntityData = EntityData>(
    ns: string,
    input: CreateInput<T>,
    options?: CreateOptions | undefined
  ): Promise<Entity<T>>

  /**
   * Update an existing entity
   */
  update<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    update: Update,
    options?: UpdateOptions | undefined
  ): Promise<Entity<T>>

  /**
   * Delete an entity
   */
  delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult>

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  /**
   * Create multiple entities
   */
  bulkCreate<T extends EntityData = EntityData>(
    ns: string,
    inputs: CreateInput<T>[],
    options?: CreateOptions | undefined
  ): Promise<Entity<T>[]>

  /**
   * Update multiple entities matching a filter
   */
  bulkUpdate(
    ns: string,
    filter: Filter,
    update: Update,
    options?: UpdateOptions | undefined
  ): Promise<UpdateResult>

  /**
   * Delete multiple entities matching a filter
   */
  bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>

  // ===========================================================================
  // Time Travel (optional)
  // ===========================================================================

  /**
   * Get a snapshot of the backend at a specific version/time
   * Returns a read-only backend that queries historical data
   */
  snapshot?(ns: string, version: number | Date): Promise<EntityBackend>

  /**
   * List available snapshots/versions
   */
  listSnapshots?(ns: string): Promise<SnapshotInfo[]>

  // ===========================================================================
  // Schema
  // ===========================================================================

  /**
   * Get the schema for a namespace
   */
  getSchema(ns: string): Promise<EntitySchema | null>

  /**
   * Set/update the schema for a namespace
   */
  setSchema?(ns: string, schema: EntitySchema): Promise<void>

  /**
   * List all namespaces
   */
  listNamespaces(): Promise<string[]>

  // ===========================================================================
  // Maintenance
  // ===========================================================================

  /**
   * Compact/optimize storage (implementation-specific)
   */
  compact?(ns: string, options?: CompactOptions): Promise<CompactResult>

  /**
   * Vacuum/clean up old data
   */
  vacuum?(ns: string, options?: VacuumOptions): Promise<VacuumResult>

  /**
   * Get storage statistics
   */
  stats?(ns: string): Promise<BackendStats>
}

// =============================================================================
// Supporting Types
// =============================================================================

/** Snapshot/version information */
export interface SnapshotInfo {
  /** Snapshot/version identifier */
  id: string | number

  /** When the snapshot was created */
  timestamp: Date

  /** Operation that created this snapshot */
  operation?: string | undefined

  /** Number of records in this snapshot */
  recordCount?: number | undefined

  /** Summary of changes */
  summary?: Record<string, unknown> | undefined
}

/** Entity schema definition */
export interface EntitySchema {
  /** Schema name/identifier */
  name: string

  /** Schema version */
  version?: number | undefined

  /** Field definitions */
  fields: SchemaField[]

  /** Primary key field(s) */
  primaryKey?: string[] | undefined

  /** Partition key field(s) */
  partitionBy?: string[] | undefined

  /** Sort key field(s) */
  sortBy?: string[] | undefined

  /** Additional properties */
  properties?: Record<string, string> | undefined
}

/** Schema field definition */
export interface SchemaField {
  /** Field name */
  name: string

  /** Field type */
  type: SchemaFieldType

  /** Whether the field is required */
  required?: boolean | undefined

  /** Whether the field is nullable */
  nullable?: boolean | undefined

  /** Default value */
  default?: unknown | undefined

  /** Field documentation */
  doc?: string | undefined

  /** Field ID (for Iceberg) */
  id?: number | undefined

  /** Previous field name (for rename tracking during schema evolution) */
  renamedFrom?: string | undefined
}

/** Supported field types */
export type SchemaFieldType =
  | 'string'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'boolean'
  | 'timestamp'
  | 'date'
  | 'time'
  | 'uuid'
  | 'binary'
  | 'decimal'
  | 'json'
  | 'variant'
  | { type: 'list'; element: SchemaFieldType }
  | { type: 'map'; key: SchemaFieldType; value: SchemaFieldType }
  | { type: 'struct'; fields: SchemaField[] }

/** Compaction options */
export interface CompactOptions {
  /** Target file size in bytes */
  targetFileSize?: number | undefined

  /** Maximum files to compact per run */
  maxFiles?: number | undefined

  /** Only compact files smaller than this size */
  minFileSize?: number | undefined

  /** Dry run (don't actually compact) */
  dryRun?: boolean | undefined
}

/** Compaction result */
export interface CompactResult {
  /** Number of files compacted */
  filesCompacted: number

  /** Number of new files created */
  filesCreated: number

  /** Bytes before compaction */
  bytesBefore: number

  /** Bytes after compaction */
  bytesAfter: number

  /** Time taken in milliseconds */
  durationMs: number
}

/** Vacuum options */
export interface VacuumOptions {
  /** Retain snapshots newer than this duration (ms) */
  retentionMs?: number | undefined

  /** Minimum snapshots to keep */
  minSnapshots?: number | undefined

  /** Dry run (don't actually delete) */
  dryRun?: boolean | undefined
}

/** Vacuum result */
export interface VacuumResult {
  /** Number of files deleted */
  filesDeleted: number

  /** Bytes reclaimed */
  bytesReclaimed: number

  /** Number of snapshots expired */
  snapshotsExpired: number
}

/** Backend statistics */
export interface BackendStats {
  /** Total number of records */
  recordCount: number

  /** Total size in bytes */
  totalBytes: number

  /** Number of data files */
  fileCount: number

  /** Number of snapshots/versions */
  snapshotCount?: number | undefined

  /** Last modified time */
  lastModified?: Date | undefined

  /** Backend-specific stats */
  [key: string]: unknown
}

// =============================================================================
// Backend Configuration
// =============================================================================

/** Base configuration for all backends */
export interface BaseBackendConfig {
  /** Underlying storage backend for file I/O */
  storage: StorageBackend

  /** Base path/location for data */
  location?: string | undefined

  /** Read-only mode */
  readOnly?: boolean | undefined
}

/** Native backend configuration */
export interface NativeBackendConfig extends BaseBackendConfig {
  type: 'native'
}

/** Iceberg backend configuration */
export interface IcebergBackendConfig extends BaseBackendConfig {
  type: 'iceberg'

  /** Iceberg catalog configuration */
  catalog?: IcebergCatalogConfig | undefined

  /** Warehouse location */
  warehouse?: string | undefined

  /** Default database/namespace */
  database?: string | undefined

  /** Timeout for acquiring write locks in milliseconds (default: 30000) */
  writeLockTimeoutMs?: number | undefined
}

/** Iceberg catalog configuration */
export type IcebergCatalogConfig =
  | { type: 'filesystem' }
  | { type: 'r2-data-catalog'; accountId: string; apiToken: string; bucketName?: string | undefined }
  | { type: 'rest'; uri: string; credential?: string | undefined; warehouse?: string | undefined }

/** Delta Lake backend configuration */
export interface DeltaBackendConfig extends BaseBackendConfig {
  type: 'delta'

  /** Maximum number of retries on commit conflict (default: 10) */
  maxRetries?: number | undefined

  /** Base backoff time in ms for exponential backoff (default: 100) */
  baseBackoffMs?: number | undefined

  /** Maximum backoff time in ms (default: 10000) */
  maxBackoffMs?: number | undefined
}

/** Union of all backend configurations */
export type BackendConfig =
  | NativeBackendConfig
  | IcebergBackendConfig
  | DeltaBackendConfig

// =============================================================================
// Factory Function Type
// =============================================================================

/**
 * Create an entity backend from configuration
 */
export type CreateBackendFn = (config: BackendConfig) => Promise<EntityBackend>

// =============================================================================
// Entity Backend Capabilities
// =============================================================================

/**
 * Entity backend capabilities
 *
 * Describes what features an entity backend supports at runtime.
 * Use getEntityBackendCapabilities() to query a backend's capabilities.
 */
export interface EntityBackendCapabilities {
  /** Backend type identifier (e.g., 'native', 'iceberg', 'delta') */
  type: BackendType

  // ---------------------------------------------------------------------------
  // Core Capabilities (from EntityBackend interface)
  // ---------------------------------------------------------------------------

  /** Whether the backend supports time-travel queries */
  timeTravel: boolean

  /** Whether the backend supports schema evolution */
  schemaEvolution: boolean

  /** Whether the backend is read-only */
  readOnly: boolean

  // ---------------------------------------------------------------------------
  // Optional Operations
  // ---------------------------------------------------------------------------

  /** Whether the backend supports snapshots */
  snapshots: boolean

  /** Whether the backend supports setting schemas */
  setSchema: boolean

  /** Whether the backend supports compaction */
  compact: boolean

  /** Whether the backend supports vacuum/cleanup */
  vacuum: boolean

  /** Whether the backend supports statistics */
  stats: boolean

  // ---------------------------------------------------------------------------
  // Table Format Features
  // ---------------------------------------------------------------------------

  /** Whether the backend supports ACID transactions */
  acidTransactions: boolean

  /** Whether the backend supports partitioning */
  partitioning: boolean

  /** Whether the backend supports column statistics */
  columnStatistics: boolean

  /** Whether the backend supports merge-on-read */
  mergeOnRead: boolean

  /** Whether the backend supports copy-on-write */
  copyOnWrite: boolean

  // ---------------------------------------------------------------------------
  // Interoperability
  // ---------------------------------------------------------------------------

  /** Whether the backend is compatible with external query engines */
  externalQueryEngines: boolean

  /** List of compatible query engines (e.g., ['duckdb', 'spark', 'snowflake']) */
  compatibleEngines?: string[] | undefined
}

/**
 * Get the capabilities of an entity backend
 *
 * This function introspects an EntityBackend instance to determine
 * what features it supports at runtime.
 *
 * @example
 * ```typescript
 * const backend = await createIcebergBackend(config)
 * const caps = getEntityBackendCapabilities(backend)
 *
 * if (caps.timeTravel) {
 *   // Query historical data
 *   const snapshot = await backend.snapshot?.('users', version)
 * }
 *
 * if (caps.compact) {
 *   // Run compaction
 *   await backend.compact?.('users')
 * }
 * ```
 */
export function getEntityBackendCapabilities(backend: EntityBackend): EntityBackendCapabilities {
  const type = backend.type

  // Detect optional operations via method presence
  const snapshots = typeof backend.snapshot === 'function' && typeof backend.listSnapshots === 'function'
  const setSchema = typeof backend.setSchema === 'function'
  const compact = typeof backend.compact === 'function'
  const vacuum = typeof backend.vacuum === 'function'
  const stats = typeof backend.stats === 'function'

  // Backend-specific capability profiles
  const profiles: Record<BackendType, Partial<EntityBackendCapabilities>> = {
    native: {
      acidTransactions: false,
      partitioning: false,
      columnStatistics: true,
      mergeOnRead: false,
      copyOnWrite: true,
      externalQueryEngines: false,
      compatibleEngines: [],
    },
    iceberg: {
      acidTransactions: true,
      partitioning: true,
      columnStatistics: true,
      mergeOnRead: true,
      copyOnWrite: true,
      externalQueryEngines: true,
      compatibleEngines: ['duckdb', 'spark', 'snowflake', 'trino', 'presto', 'athena'],
    },
    delta: {
      acidTransactions: true,
      partitioning: true,
      columnStatistics: true,
      mergeOnRead: false,
      copyOnWrite: true,
      externalQueryEngines: true,
      compatibleEngines: ['duckdb', 'spark', 'databricks'],
    },
  }

  const profile = profiles[type] ?? profiles.native

  return {
    type,
    timeTravel: backend.supportsTimeTravel,
    schemaEvolution: backend.supportsSchemaEvolution,
    readOnly: backend.readOnly,
    snapshots,
    setSchema,
    compact,
    vacuum,
    stats,
    ...profile,
  } as EntityBackendCapabilities
}

/**
 * Check if an entity backend supports a specific capability
 *
 * @example
 * ```typescript
 * if (hasEntityBackendCapability(backend, 'timeTravel')) {
 *   // Use time-travel queries
 * }
 * ```
 */
export function hasEntityBackendCapability(
  backend: EntityBackend,
  capability: keyof Omit<EntityBackendCapabilities, 'type' | 'compatibleEngines'>
): boolean {
  const caps = getEntityBackendCapabilities(backend)
  return caps[capability] === true
}

/**
 * Check if an entity backend is compatible with a specific query engine
 *
 * @example
 * ```typescript
 * if (isCompatibleWithEngine(backend, 'duckdb')) {
 *   // Use DuckDB to query the data
 * }
 * ```
 */
export function isCompatibleWithEngine(backend: EntityBackend, engine: string): boolean {
  const caps = getEntityBackendCapabilities(backend)
  return caps.compatibleEngines?.includes(engine.toLowerCase()) ?? false
}

// =============================================================================
// Backend Errors
// =============================================================================
// Re-export error classes from the centralized errors module.
// These provide consistent error handling with error codes and serialization.

// Import from centralized errors module
import {
  BackendError as _BackendErrorBase,
  CommitConflictError as CommitConflictErrorBase,
  ReadOnlyError as ReadOnlyErrorBase,
  TableNotFoundError as TableNotFoundErrorBase,
  InvalidNamespaceError as InvalidNamespaceErrorBase,
  SchemaNotFoundError as SchemaNotFoundErrorBase,
  WriteLockTimeoutError as WriteLockTimeoutErrorBase,
  EntityNotFoundError,
  SnapshotNotFoundError as _BaseSnapshotNotFoundError,
  ErrorCode,
  ParqueDBError,
  isCommitConflictError,
  isReadOnlyError,
  isWriteLockTimeoutError,
  isBackendError,
} from '../errors'

// Re-export the error classes
export {
  CommitConflictErrorBase as CommitConflictError,
  ReadOnlyErrorBase as ReadOnlyError,
  TableNotFoundErrorBase as TableNotFoundError,
  InvalidNamespaceErrorBase as InvalidNamespaceError,
  SchemaNotFoundErrorBase as SchemaNotFoundError,
  WriteLockTimeoutErrorBase as WriteLockTimeoutError,
  // Type guards
  isCommitConflictError,
  isReadOnlyError,
  isWriteLockTimeoutError,
  isBackendError,
}

/**
 * Error thrown when an entity is not found in a backend
 *
 * @deprecated Use EntityNotFoundError from '../errors' instead
 */
export class BackendEntityNotFoundError extends EntityNotFoundError {
  override name = 'BackendEntityNotFoundError'

  constructor(
    ns: string,
    entityId: string
  ) {
    super(ns, entityId)
    Object.setPrototypeOf(this, BackendEntityNotFoundError.prototype)
  }

  get ns(): string {
    return this.namespace
  }
}

/**
 * Error thrown when a snapshot version is not found
 *
 * This error provides additional context for time-travel queries.
 */
export class SnapshotNotFoundError extends ParqueDBError {
  override name = 'SnapshotNotFoundError'

  constructor(
    ns: string,
    version: number | Date
  ) {
    const versionStr = version instanceof Date ? version.toISOString() : String(version)
    super(
      `Snapshot not found for ${ns}: version ${versionStr}`,
      ErrorCode.SNAPSHOT_NOT_FOUND,
      { namespace: ns, version: versionStr }
    )
    Object.setPrototypeOf(this, SnapshotNotFoundError.prototype)
  }

  get ns(): string {
    return this.context.namespace as string
  }

  get version(): string {
    return this.context.version as string
  }
}

// =============================================================================
// Variant Shredding Configuration
// =============================================================================

/**
 * Configuration for variant column shredding
 *
 * Variant shredding extracts frequently-accessed fields from a Variant column
 * into separate typed columns for better predicate pushdown and statistics.
 */
export interface VariantShredConfig {
  /** Variant column name (e.g., '$index') */
  column: string
  /** Shredded field names */
  fields: string[]
}

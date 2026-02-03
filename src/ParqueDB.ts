/**
 * ParqueDB - A Parquet-based database
 *
 * This module provides the main ParqueDB class with support for both
 * explicit and Proxy-based collection access patterns.
 *
 * The implementation is split into multiple modules under src/ParqueDB/:
 * - types.ts: Type definitions and interfaces
 * - validation.ts: Input validation utilities
 * - store.ts: Global state management
 * - collection.ts: CollectionImpl class
 * - core.ts: ParqueDBImpl class with all database operations
 */

// =============================================================================
// Re-exports from modules
// =============================================================================

// Export all types
export type {
  // Configuration types
  SnapshotConfig,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyError,
  UpsertManyResult,
  EventLogConfig,
  ArchiveEventsResult,
  ParqueDBConfig,

  // Collection interface
  Collection,

  // History and diff types
  HistoryItem,
  HistoryResult,
  DiffResult,
  RevertOptions,
  GetRelatedOptions,
  GetRelatedResult,

  // Transaction types
  ParqueDBTransaction,

  // Snapshot types
  Snapshot,
  RawSnapshot,
  SnapshotQueryStats,
  SnapshotStorageStats,
  PruneSnapshotsOptions,
  SnapshotManager,

  // Event log types
  EventLog,

  // IngestStream types
  IngestStreamOptions,
  IngestStreamResult,
  IngestStreamError,
  IngestBatchStats,
} from './ParqueDB/types'

// Export values (not types)
export {
  DEFAULT_EVENT_LOG_CONFIG,
  VersionConflictError,
} from './ParqueDB/types'

// Export validation utilities (for advanced use cases)
export { normalizeNamespace } from './ParqueDB/validation'

// =============================================================================
// Imports for ParqueDB class
// =============================================================================

import type {
  Entity,
  EntityId,
  CreateInput,
  PaginatedResult,
  DeleteResult,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  Schema,
  HistoryOptions,
} from './types'

import type { IndexDefinition, IndexMetadata, IndexStats } from './indexes/types'
import type { IndexManager } from './indexes/manager'
import type { IStorageRouter, StorageMode } from './storage/router'
import type { CollectionOptions } from './types/collection-options'

import type {
  ParqueDBConfig,
  Collection,
  HistoryResult,
  ParqueDBTransaction,
  SnapshotManager,
  EventLog,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
  GetRelatedOptions,
  GetRelatedResult,
  DiffResult,
  RevertOptions,
  IngestStreamOptions,
  IngestStreamResult,
} from './ParqueDB/types'

import { normalizeNamespace } from './ParqueDB/validation'
import { ParqueDBImpl } from './ParqueDB/core'

// =============================================================================
// ParqueDB Interface (Type Declarations)
// =============================================================================

/**
 * ParqueDB interface for type declarations.
 * All methods are implemented via Proxy delegation to ParqueDBImpl.
 */
export interface IParqueDB {
  /** Dynamic collection access via Proxy */
  [key: string]: Collection | unknown

  // Schema Management
  registerSchema(schema: Schema): void

  // Collection Access
  collection<T = Record<string, unknown>>(namespace: string): Collection<T>

  // CRUD Operations
  find<T = Record<string, unknown>>(
    namespace: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>>

  get<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null>

  create<T = Record<string, unknown>>(
    namespace: string,
    data: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>>

  update<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): Promise<Entity<T> | null>

  delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>

  // Bulk Operations
  upsert<T = Record<string, unknown>>(
    namespace: string,
    filter: Filter,
    update: UpdateInput<T>,
    options?: { returnDocument?: 'before' | 'after' }
  ): Promise<Entity<T> | null>

  upsertMany<T = Record<string, unknown>>(
    namespace: string,
    items: UpsertManyItem<T>[],
    options?: UpsertManyOptions
  ): Promise<UpsertManyResult>

  deleteMany(namespace: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>

  restore<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: { actor?: EntityId }
  ): Promise<Entity<T> | null>

  ingestStream<T = Record<string, unknown>>(
    namespace: string,
    source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>,
    options?: IngestStreamOptions<Partial<T>>
  ): Promise<IngestStreamResult>

  // History & Time-Travel
  history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult>

  getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult>

  getAtVersion<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    version: number
  ): Promise<Entity<T> | null>

  diff(namespace: string, id: string, fromVersion: number, toVersion: number): Promise<DiffResult>

  revert<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    toVersion: number,
    options?: RevertOptions
  ): Promise<Entity<T> | null>

  // Relationships
  getRelated<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    relationField: string,
    options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>>

  // Transactions & Snapshots
  beginTransaction(): ParqueDBTransaction

  getSnapshotManager(): SnapshotManager

  getEventLog(): EventLog

  // Index Management
  createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata>

  dropIndex(ns: string, indexName: string): Promise<void>

  listIndexes(ns: string): Promise<IndexMetadata[]>

  getIndex(ns: string, indexName: string): Promise<IndexMetadata | null>

  rebuildIndex(ns: string, indexName: string): Promise<void>

  getIndexStats(ns: string, indexName: string): Promise<IndexStats>

  getIndexManager(): IndexManager

  // Storage Router
  getStorageMode(namespace: string): StorageMode

  getDataPath(namespace: string): string

  hasTypedSchema(namespace: string): boolean

  getCollectionOptions(namespace: string): CollectionOptions | undefined

  getStorageRouter(): IStorageRouter | null

  // Resource Management
  /** Wait for any pending flush operations to complete */
  flush(): Promise<void>
  /** Synchronously dispose without waiting for pending flushes */
  dispose(): void
  /** Asynchronously dispose, waiting for pending flushes first */
  disposeAsync(): Promise<void>
}

// =============================================================================
// ParqueDB Class (Public API with Proxy)
// =============================================================================

/**
 * ParqueDB - A Parquet-based database with proxy-based collection access
 *
 * All methods are implemented via a Proxy that delegates to ParqueDBImpl.
 * The class itself only defines the constructor; method signatures are
 * declared via the IParqueDB interface for TypeScript type checking.
 *
 * @example
 * // Explicit collection access
 * const db = new ParqueDB({ storage })
 * await db.find('posts', { status: 'published' })
 * await db.get('posts', 'posts/123')
 *
 * @example
 * // Proxy-based collection access
 * const db = new ParqueDB({ storage })
 * await db.Posts.find({ status: 'published' })
 * await db.Posts.get('posts/123')
 */
export interface ParqueDB extends IParqueDB {}

export class ParqueDB {
  constructor(config: ParqueDBConfig) {
    const impl = new ParqueDBImpl(config)

    // Build method map once at construction time (not on every property access)
    const methodMap: Record<string, unknown> = {
      // Schema Management
      registerSchema: impl.registerSchema.bind(impl),
      // Collection Access
      collection: impl.collection.bind(impl),
      // CRUD Operations
      find: impl.find.bind(impl),
      get: impl.get.bind(impl),
      create: impl.create.bind(impl),
      update: impl.update.bind(impl),
      delete: impl.delete.bind(impl),
      // Bulk Operations
      upsert: impl.upsert.bind(impl),
      upsertMany: impl.upsertMany.bind(impl),
      deleteMany: impl.deleteMany.bind(impl),
      restore: impl.restore.bind(impl),
      ingestStream: impl.ingestStream.bind(impl),
      // History & Time-Travel
      history: impl.history.bind(impl),
      getHistory: impl.getHistory.bind(impl),
      getAtVersion: impl.getAtVersion.bind(impl),
      diff: impl.diff.bind(impl),
      revert: impl.revert.bind(impl),
      // Relationships
      getRelated: impl.getRelated.bind(impl),
      // Transactions & Snapshots
      beginTransaction: impl.beginTransaction.bind(impl),
      getSnapshotManager: impl.getSnapshotManager.bind(impl),
      getEventLog: impl.getEventLog.bind(impl),
      // Index Management
      createIndex: impl.createIndex.bind(impl),
      dropIndex: impl.dropIndex.bind(impl),
      listIndexes: impl.listIndexes.bind(impl),
      getIndex: impl.getIndex.bind(impl),
      rebuildIndex: impl.rebuildIndex.bind(impl),
      getIndexStats: impl.getIndexStats.bind(impl),
      getIndexManager: impl.getIndexManager.bind(impl),
      // Storage Router
      getStorageMode: impl.getStorageMode.bind(impl),
      getDataPath: impl.getDataPath.bind(impl),
      hasTypedSchema: impl.hasTypedSchema.bind(impl),
      getCollectionOptions: impl.getCollectionOptions.bind(impl),
      getStorageRouter: impl.getStorageRouter.bind(impl),
      // Resource Management
      dispose: impl.dispose.bind(impl),
      flush: impl.flush.bind(impl),
      disposeAsync: impl.disposeAsync.bind(impl),
    }

    // Return a Proxy for dynamic collection access
    // All method calls are delegated to impl via the pre-built methodMap
    return new Proxy(this, {
      get(_target, prop, _receiver) {
        // Handle known methods from pre-built map
        if (typeof prop === 'string' && prop in methodMap) {
          return methodMap[prop]
        }

        // Handle Symbol properties
        if (typeof prop === 'symbol') {
          return undefined
        }

        // Check if property was explicitly set (e.g., db.sql = ...)
        // This allows attaching custom properties like SQL executor
        if (prop in _target) {
          return (_target as Record<string | symbol, unknown>)[prop]
        }

        // Handle dynamic collection access for any string property
        // (Posts, Users, posts, users, etc.)
        if (typeof prop === 'string') {
          const ns = normalizeNamespace(prop)
          return impl.collection(ns)
        }

        return undefined
      },

      // Make instanceof work correctly
      getPrototypeOf() {
        return ParqueDB.prototype
      },
    }) as ParqueDB
  }
}

export default ParqueDB

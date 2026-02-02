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
import { IndexManager } from './indexes/manager'

import type {
  ParqueDBConfig,
  Collection,
  HistoryResult,
  ParqueDBTransaction,
  SnapshotManager,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
  GetRelatedOptions,
  GetRelatedResult,
} from './ParqueDB/types'

import { normalizeNamespace } from './ParqueDB/validation'
import { ParqueDBImpl } from './ParqueDB/core'

// =============================================================================
// ParqueDB Class (Public API with Proxy)
// =============================================================================

/**
 * ParqueDB - A Parquet-based database with proxy-based collection access
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
export class ParqueDB {
  /** Dynamic collection access via Proxy */
  [key: string]: Collection | unknown

  constructor(config: ParqueDBConfig) {
    const impl = new ParqueDBImpl(config)

    // Return a Proxy for dynamic collection access
    return new Proxy(this, {
      get(_target, prop, _receiver) {
        // Handle known methods that delegate to impl FIRST
        // (before checking if property exists on target, since stubs exist there)
        if (prop === 'registerSchema') {
          return impl.registerSchema.bind(impl)
        }
        if (prop === 'collection') {
          return impl.collection.bind(impl)
        }
        if (prop === 'find') {
          return impl.find.bind(impl)
        }
        if (prop === 'get') {
          return impl.get.bind(impl)
        }
        if (prop === 'create') {
          return impl.create.bind(impl)
        }
        if (prop === 'update') {
          return impl.update.bind(impl)
        }
        if (prop === 'delete') {
          return impl.delete.bind(impl)
        }
        if (prop === 'history') {
          return impl.history.bind(impl)
        }
        if (prop === 'getAtVersion') {
          return impl.getAtVersion.bind(impl)
        }
        if (prop === 'beginTransaction') {
          return impl.beginTransaction.bind(impl)
        }
        if (prop === 'getSnapshotManager') {
          return impl.getSnapshotManager.bind(impl)
        }
        if (prop === 'getEventLog') {
          return impl.getEventLog.bind(impl)
        }
        if (prop === 'upsert') {
          return impl.upsert.bind(impl)
        }
        if (prop === 'upsertMany') {
          return impl.upsertMany.bind(impl)
        }
        if (prop === 'deleteMany') {
          return impl.deleteMany.bind(impl)
        }
        if (prop === 'restore') {
          return impl.restore.bind(impl)
        }
        if (prop === 'getHistory') {
          return impl.getHistory.bind(impl)
        }
        if (prop === 'diff') {
          return impl.diff.bind(impl)
        }
        if (prop === 'revert') {
          return impl.revert.bind(impl)
        }
        if (prop === 'getRelated') {
          return impl.getRelated.bind(impl)
        }
        // Index management methods
        if (prop === 'createIndex') {
          return impl.createIndex.bind(impl)
        }
        if (prop === 'dropIndex') {
          return impl.dropIndex.bind(impl)
        }
        if (prop === 'listIndexes') {
          return impl.listIndexes.bind(impl)
        }
        if (prop === 'getIndex') {
          return impl.getIndex.bind(impl)
        }
        if (prop === 'rebuildIndex') {
          return impl.rebuildIndex.bind(impl)
        }
        if (prop === 'getIndexStats') {
          return impl.getIndexStats.bind(impl)
        }
        if (prop === 'getIndexManager') {
          return impl.getIndexManager.bind(impl)
        }
        // Resource cleanup
        if (prop === 'dispose') {
          return impl.dispose.bind(impl)
        }

        // Handle Symbol properties
        if (typeof prop === 'symbol') {
          return undefined
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

  /**
   * Register a schema for validation
   * @param schema - Schema definition
   */
  registerSchema(_schema: Schema): void {
    throw new Error('Not implemented')
  }

  /**
   * Get a collection by namespace
   * @param namespace - Collection namespace
   * @returns Collection interface
   */
  collection<T = Record<string, unknown>>(_namespace: string): Collection<T> {
    throw new Error('Not implemented')
  }

  /**
   * Find entities in a namespace
   * @param namespace - Target namespace
   * @param filter - MongoDB-style filter
   * @param options - Query options
   */
  find<T = Record<string, unknown>>(
    _namespace: string,
    _filter?: Filter,
    _options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>> {
    throw new Error('Not implemented')
  }

  /**
   * Get a single entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Get options
   */
  get<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _options?: GetOptions
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Create a new entity
   * @param namespace - Target namespace
   * @param data - Entity data
   * @param options - Create options
   */
  create<T = Record<string, unknown>>(
    _namespace: string,
    _data: CreateInput<T>,
    _options?: CreateOptions
  ): Promise<Entity<T>> {
    throw new Error('Not implemented')
  }

  /**
   * Update an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   */
  update<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _update: UpdateInput<T>,
    _options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Delete an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Delete options
   */
  delete(_namespace: string, _id: string, _options?: DeleteOptions): Promise<DeleteResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get entity history
   * @param entityId - Entity ID
   * @param options - History options
   */
  history(_entityId: EntityId, _options?: HistoryOptions): Promise<HistoryResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get entity at a specific version
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param version - Target version
   */
  getAtVersion<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _version: number
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Begin a transaction
   */
  beginTransaction(): ParqueDBTransaction {
    throw new Error('Not implemented')
  }

  /**
   * Get snapshot manager
   */
  getSnapshotManager(): SnapshotManager {
    throw new Error('Not implemented')
  }

  /**
   * Upsert an entity (filter-based: update if exists, create if not)
   * @param namespace - Target namespace
   * @param filter - Filter to find existing entity
   * @param update - Update operations
   * @param options - Upsert options
   */
  upsert<T = Record<string, unknown>>(
    _namespace: string,
    _filter: Filter,
    _update: UpdateInput<T>,
    _options?: { returnDocument?: 'before' | 'after' }
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Upsert multiple entities in a single operation
   * @param namespace - Target namespace
   * @param items - Array of upsert items with filter and update
   * @param options - UpsertMany options
   */
  upsertMany<T = Record<string, unknown>>(
    _namespace: string,
    _items: UpsertManyItem<T>[],
    _options?: UpsertManyOptions
  ): Promise<UpsertManyResult> {
    throw new Error('Not implemented')
  }

  /**
   * Delete multiple entities matching a filter
   * @param namespace - Target namespace
   * @param filter - Filter to match entities
   * @param options - Delete options
   */
  deleteMany(
    _namespace: string,
    _filter: Filter,
    _options?: DeleteOptions
  ): Promise<DeleteResult> {
    throw new Error('Not implemented')
  }

  /**
   * Restore a soft-deleted entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - Restore options
   */
  restore<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _options?: { actor?: EntityId }
  ): Promise<Entity<T> | null> {
    throw new Error('Not implemented')
  }

  /**
   * Get history for an entity
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param options - History options
   */
  getHistory(
    _namespace: string,
    _id: string,
    _options?: HistoryOptions
  ): Promise<HistoryResult> {
    throw new Error('Not implemented')
  }

  /**
   * Get related entities with pagination support
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param relationField - Field name of the relationship
   * @param options - Options for pagination, filtering, sorting
   */
  getRelated<T = Record<string, unknown>>(
    _namespace: string,
    _id: string,
    _relationField: string,
    _options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>> {
    throw new Error('Not implemented')
  }

  // ===========================================================================
  // Index Management API
  // ===========================================================================

  /**
   * Create a new index on a namespace
   * @param ns - Namespace
   * @param definition - Index definition
   */
  createIndex(_ns: string, _definition: IndexDefinition): Promise<IndexMetadata> {
    throw new Error('Not implemented')
  }

  /**
   * Drop an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  dropIndex(_ns: string, _indexName: string): Promise<void> {
    throw new Error('Not implemented')
  }

  /**
   * List all indexes for a namespace
   * @param ns - Namespace
   */
  listIndexes(_ns: string): Promise<IndexMetadata[]> {
    throw new Error('Not implemented')
  }

  /**
   * Get metadata for a specific index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  getIndex(_ns: string, _indexName: string): Promise<IndexMetadata | null> {
    throw new Error('Not implemented')
  }

  /**
   * Rebuild an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  rebuildIndex(_ns: string, _indexName: string): Promise<void> {
    throw new Error('Not implemented')
  }

  /**
   * Get statistics for an index
   * @param ns - Namespace
   * @param indexName - Index name
   */
  getIndexStats(_ns: string, _indexName: string): Promise<IndexStats> {
    throw new Error('Not implemented')
  }

  /**
   * Get the index manager instance
   */
  getIndexManager(): IndexManager {
    throw new Error('Not implemented')
  }

  // ===========================================================================
  // Resource Management
  // ===========================================================================

  /**
   * Dispose of this ParqueDB instance and clean up associated global state.
   * Call this when you are done using a ParqueDB instance to prevent memory leaks.
   *
   * After calling dispose():
   * - The global state (entities, events, snapshots, query stats) for this storage backend is cleared
   * - The instance should not be used anymore
   * - Other ParqueDB instances using the same storage backend will also lose their shared state
   */
  dispose(): void {
    throw new Error('Not implemented')
  }
}

export default ParqueDB

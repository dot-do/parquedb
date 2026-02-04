/**
 * ParqueDB Module
 *
 * This is the main entry point for the ParqueDB module.
 * It re-exports all public types and the ParqueDB class.
 */

// Re-export types
export * from './types'

// Re-export validation utilities
export {
  /**
   * Validates a namespace string to ensure it follows naming conventions.
   *
   * @param namespace - The namespace string to validate
   * @throws {ValidationError} If namespace is empty, contains '/', starts with '_' or '$'
   *
   * @example
   * ```typescript
   * validateNamespace('posts')     // OK
   * validateNamespace('_system')   // throws ValidationError
   * validateNamespace('ns/sub')    // throws ValidationError
   * ```
   */
  validateNamespace,
  /**
   * Validates a MongoDB-style filter object for correct operator syntax.
   *
   * @param filter - The filter object to validate
   * @throws {ValidationError} If filter contains invalid operators
   *
   * @example
   * ```typescript
   * validateFilter({ status: 'active' })                    // OK
   * validateFilter({ $and: [{ a: 1 }, { b: 2 }] })          // OK
   * validateFilter({ $invalid: 'value' })                   // throws ValidationError
   * ```
   */
  validateFilter,
  /**
   * Validates update operators in an update document.
   *
   * @param update - The update document to validate
   * @throws {ValidationError} If update contains invalid operators
   *
   * @example
   * ```typescript
   * validateUpdateOperators({ $set: { name: 'Alice' } })   // OK
   * validateUpdateOperators({ $inc: { count: 1 } })        // OK
   * validateUpdateOperators({ $badOp: {} })                // throws ValidationError
   * ```
   */
  validateUpdateOperators,
  /**
   * Normalizes a namespace to lowercase for case-insensitive matching.
   *
   * @param name - The namespace to normalize
   * @returns The normalized lowercase namespace
   *
   * @example
   * ```typescript
   * normalizeNamespace('Posts')       // 'posts'
   * normalizeNamespace('BlogPosts')   // 'blogposts'
   * ```
   */
  normalizeNamespace,
  /**
   * Validates a full EntityId string (format: "namespace/localId").
   *
   * @param id - The entity ID to validate
   * @throws {ValidationError} If the entity ID format is invalid
   *
   * @example
   * ```typescript
   * validateEntityId('users/user-123')  // OK
   * validateEntityId('invalidid')       // throws ValidationError
   * ```
   */
  validateEntityId,
  /**
   * Validates a local ID (the part after the namespace).
   *
   * @param id - The local ID to validate
   * @throws {ValidationError} If the local ID is empty or invalid
   *
   * @example
   * ```typescript
   * validateLocalId('user-123')  // OK
   * validateLocalId('')          // throws ValidationError
   * ```
   */
  validateLocalId,
  /**
   * Normalizes an entity ID with namespace context.
   * If the ID already contains a '/', it's validated as a full EntityId.
   * Otherwise, it's prefixed with the provided namespace.
   *
   * @param namespace - The default namespace to use if ID doesn't have one
   * @param id - The entity ID (can be full "ns/id" or just "id")
   * @returns The normalized EntityId in "namespace/id" format
   * @throws {ValidationError} If validation fails
   *
   * @example
   * ```typescript
   * normalizeEntityId('users', 'user-123')        // 'users/user-123'
   * normalizeEntityId('posts', 'users/user-123')  // 'users/user-123' (unchanged)
   * ```
   */
  normalizeEntityId,
  /**
   * Converts namespace and id to a full entity ID string.
   * Does NOT perform validation - use when validation has already been done.
   *
   * @param namespace - The namespace to use if ID doesn't have one
   * @param id - The entity ID (can be full "ns/id" or just "id")
   * @returns The full ID in "namespace/id" format
   *
   * @example
   * ```typescript
   * toFullId('users', 'user-123')        // 'users/user-123'
   * toFullId('posts', 'users/user-123')  // 'users/user-123' (unchanged)
   * ```
   */
  toFullId,
} from './validation'

// Re-export store utilities (for testing)
export {
  /**
   * Gets the entity store Map for a given storage backend.
   * Used internally for testing and direct store access.
   *
   * The entity store is an LRU cache with configurable size limits.
   * Configure the cache before first use with configureEntityStore()
   * or via ParqueDBConfig.maxCacheSize.
   *
   * @param storage - The storage backend instance
   * @param config - Optional cache configuration (used only when creating new store)
   * @returns Map of entity ID to Entity objects
   */
  getEntityStore,
  /**
   * Gets the event store array for a given storage backend.
   * Contains all CDC events for time-travel queries.
   *
   * @param storage - The storage backend instance
   * @returns Array of Event objects
   */
  getEventStore,
  /**
   * Gets the archived event store array for a given storage backend.
   * Contains events that have been archived from the main event log.
   *
   * @param storage - The storage backend instance
   * @returns Array of archived Event objects
   */
  getArchivedEventStore,
  /**
   * Gets the snapshot store array for a given storage backend.
   * Contains point-in-time snapshots for efficient time-travel.
   *
   * @param storage - The storage backend instance
   * @returns Array of Snapshot objects
   */
  getSnapshotStore,
  /**
   * Gets the query stats store for a given storage backend.
   * Contains performance metrics for snapshot queries.
   *
   * @param storage - The storage backend instance
   * @returns Map of snapshot ID to SnapshotQueryStats
   */
  getQueryStatsStore,
  /**
   * Clears all global state for a storage backend.
   * Use this to reset database state in tests.
   *
   * @param storage - The storage backend instance
   *
   * @example
   * ```typescript
   * // In test cleanup
   * afterEach(() => {
   *   clearGlobalState(storage)
   * })
   * ```
   */
  clearGlobalState,
  /**
   * Configure the entity store for a storage backend before first use.
   *
   * @param storage - The storage backend to configure
   * @param config - Cache configuration options
   *
   * @example
   * ```typescript
   * configureEntityStore(storage, { maxEntities: 5000 })
   * ```
   */
  configureEntityStore,
  /**
   * Get cache statistics for the entity store.
   *
   * @param storage - The storage backend
   * @returns Cache statistics or undefined if store not created
   */
  getEntityCacheStats,
  /**
   * LRU-based entity cache that implements the Map interface.
   * Provides LRU eviction when the cache exceeds its size limit.
   */
  LRUEntityCache,
  /**
   * Default maximum number of entities to keep in memory
   */
  DEFAULT_MAX_ENTITIES,
  /**
   * Configuration interface for the entity store cache
   */
  type EntityStoreConfig,
} from './store'

// Re-export collection
/**
 * CollectionImpl provides a fluent API for entity operations within a namespace.
 *
 * @see Collection interface for full API documentation
 * @see CollectionManager for collection caching and creation
 *
 * @example
 * ```typescript
 * const posts = new CollectionImpl<Post>(db, 'posts')
 * await posts.find({ status: 'published' })
 * await posts.create({ title: 'Hello', content: 'World' })
 * ```
 */
export { CollectionImpl } from './collection'

// Re-export collections manager
/**
 * CollectionManager handles collection instance caching and creation.
 * Collections are singletons per namespace - requesting the same namespace
 * multiple times returns the same CollectionImpl instance.
 *
 * @example
 * ```typescript
 * const manager = new CollectionManager(db)
 * const posts = manager.get<Post>('posts')
 * const samePosts = manager.get<Post>('Posts')  // Same instance (normalized)
 * ```
 */
export { CollectionManager, createCollection } from './collections'
export type { CollectionManagerContext } from './collections'

// Re-export snapshot manager
/**
 * SnapshotManagerImpl provides point-in-time snapshot functionality
 * for efficient time-travel queries and entity state reconstruction.
 *
 * @example
 * ```typescript
 * const snapshots = db.getSnapshotManager()
 * const snapshot = await snapshots.createSnapshot()
 * const entity = await snapshots.getEntityAtSnapshot('users/123', snapshot.id)
 * ```
 */
export { SnapshotManagerImpl } from './snapshots'

// Re-export event log
/**
 * EventLogImpl provides access to the CDC event log for time-travel,
 * audit trails, and event replay functionality.
 *
 * @example
 * ```typescript
 * const eventLog = db.getEventLog()
 * const events = eventLog.getEvents({ since: new Date('2024-01-01') })
 * ```
 */
export { EventLogImpl } from './events'

// Re-export core
/**
 * ParqueDBImpl is the main database class that coordinates all operations.
 * It provides CRUD operations, time-travel, indexing, and transaction support.
 *
 * @see ParqueDB for the public-facing class (wraps ParqueDBImpl)
 *
 * @example
 * ```typescript
 * const db = new ParqueDBImpl({ storage: new MemoryBackend() })
 * const posts = await db.find('posts', { status: 'published' })
 * const user = await db.get('users', 'user-123')
 * ```
 */
export { ParqueDBImpl } from './core'

// Re-export ParqueDBImpl as ParqueDB for backwards compatibility
export { ParqueDBImpl as ParqueDB } from './core'

// Re-export entity operations types and helpers
export {
  /**
   * Derives an entity type name from a namespace.
   * Converts lowercase namespace to PascalCase singular form.
   *
   * @param namespace - The namespace (e.g., 'posts', 'users')
   * @returns The derived type name (e.g., 'Post', 'User')
   *
   * @example
   * ```typescript
   * deriveTypeFromNamespace('posts')     // 'Post'
   * deriveTypeFromNamespace('users')     // 'User'
   * deriveTypeFromNamespace('categories') // 'Category'
   * ```
   */
  deriveTypeFromNamespace,
  /**
   * Checks if a schema field is required (non-nullable).
   *
   * @param fieldDef - The field definition from schema
   * @returns True if the field is required
   */
  isFieldRequired,
  /**
   * Checks if a schema field has a default value.
   *
   * @param fieldDef - The field definition from schema
   * @returns True if the field has a default
   */
  hasDefault,
  /**
   * Validates a value against a field's type definition.
   *
   * @param value - The value to validate
   * @param fieldDef - The field definition from schema
   * @param fieldName - The field name for error messages
   * @throws {ValidationError} If the value doesn't match the expected type
   */
  validateFieldType,
  /**
   * Applies schema default values to an entity.
   *
   * @param data - The entity data
   * @param schema - The schema containing default values
   * @param namespace - The namespace for type lookup
   * @returns Entity data with defaults applied
   */
  applySchemaDefaults,
  type EntityOperationsContext,
  type EntityStoreContext,
  type EntityQueryContext,
  type EntityMutationContext,
} from './entity-operations'

// Re-export relationship operations types and helpers
export {
  indexRelationshipsForEntity,
  unindexRelationshipsForEntity,
  applyRelationshipOperators,
  parseReverseRelation,
  getReverseRelatedIds,
  hydrateEntity,
  applyMaxInboundToEntity,
  getRelatedEntities,
  type RelationshipOperationsContext,
} from './relationship-operations'

// Re-export event operations types and helpers
export {
  recordEvent,
  flushEvents,
  archiveEvents as archiveEventsOp,
  reconstructEntityAtTime,
  reconstructFromEvents,
  binarySearchLastEventBeforeTime,
  getEntityHistory,
  getEntityAtVersion,
  computeDiff,
  revertEntity,
  type EventOperationsContext,
  type EventReadContext,
  type EventReconstructionContext,
  type EventFlushContext,
  type EventArchivalContext,
  type EventRecordingContext,
} from './event-operations'

// Re-export schema operations types and helpers
export {
  registerSchema,
  validateAgainstSchema,
  legacyValidateAgainstSchema,
  hasTypeSchema,
  getTypeSchema,
  getFieldSchema,
  isRelationshipField,
  isReverseRelationshipField,
  getRelationshipFields,
  getReverseRelationshipFields,
  type SchemaOperationsContext,
} from './schema-operations'

// Re-export read path operations
/**
 * Read operations module providing find, get, and query functions.
 * @see findEntities - Find entities matching a filter
 * @see getEntity - Get a single entity by ID
 * @see queryEntities - Query entities with advanced options
 */
export {
  findEntities as findEntitiesFromReadPath,
  getEntity as getEntityFromReadPath,
  queryEntities,
  type ReadPathContext,
} from './read-path'

// Re-export write path operations
/**
 * Write operations module providing create, update, delete, and restore functions.
 * @see createEntity - Create a new entity
 * @see updateEntity - Update an existing entity
 * @see deleteEntity - Delete an entity
 * @see deleteManyEntities - Delete multiple entities
 * @see restoreEntity - Restore a soft-deleted entity
 */
export {
  createEntity as createEntityFromWritePath,
  updateEntity as updateEntityFromWritePath,
  deleteEntity as deleteEntityFromWritePath,
  deleteManyEntities as deleteManyEntitiesFromWritePath,
  restoreEntity as restoreEntityFromWritePath,
  type WritePathContext,
} from './write-path'

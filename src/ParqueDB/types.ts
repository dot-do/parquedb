/**
 * ParqueDB Types Module
 *
 * Contains all the type definitions and interfaces used by ParqueDB.
 */

import type {
  Entity,
  EntityId,
  EntityData,
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
  Event,
  EventOp,
  SortSpec,
  Projection,
  StorageBackend,
  Schema,
  HistoryOptions,
  Metadata,
  EntityState,
} from '../types'

// HybridSearchStrategy is available from '../indexes/types' if needed externally

import type { IStorageRouter } from '../storage/router'
import type { CollectionOptions } from '../types/collection-options'
import type { EmbeddingProvider } from '../embeddings/provider'

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Snapshot configuration options
 */
export interface SnapshotConfig {
  /** Automatically create snapshot after this many events */
  autoSnapshotThreshold?: number | undefined
}

// =============================================================================
// UpsertMany Types
// =============================================================================

/**
 * Item for upsertMany operation
 */
/**
 * Semantic search options for Collection interface
 */
export interface SemanticSearchOptions {
  /** Minimum similarity score (0-1) */
  minScore?: number | undefined
  /** Maximum results to return */
  limit?: number | undefined
  /** Field containing embeddings */
  field?: string | undefined
}

/**
 * Semantic search result with similarity score
 */
export interface SemanticSearchResult<_T = Record<string, unknown>> {
  $id: string
  $type: string
  $score: number
  [key: string]: unknown
}

/**
 * Hybrid search options for Collection interface
 */
export interface HybridSearchOptionsCollection {
  /** Minimum similarity score (0-1) */
  minScore?: number | undefined
  /** Maximum results to return */
  limit?: number | undefined
  /** Additional filter criteria */
  filter?: Filter | undefined
  /** Strategy for combining vector + filter results */
  strategy?: 'pre-filter' | 'post-filter' | 'auto' | undefined
}

/**
 * Item for upsertMany operation
 */
export interface UpsertManyItem<T extends EntityData = EntityData> {
  /** Filter to find existing document */
  filter: Filter
  /** Update operations to apply */
  update: UpdateInput<T>
  /** Per-item options */
  options?: {
    /** Expected version for optimistic concurrency */
    expectedVersion?: number | undefined
  } | undefined
}

/**
 * Options for upsertMany operation
 */
export interface UpsertManyOptions {
  /** Stop on first error if true (default: true) */
  ordered?: boolean | undefined
  /** Actor performing the operation */
  actor?: EntityId | undefined
}

/**
 * Error entry in upsertMany result
 */
export interface UpsertManyError {
  /** Index of the failed item */
  index: number
  /** Filter that was used */
  filter: Filter
  /** Error details */
  error: Error
}

/**
 * Result of upsertMany operation
 */
export interface UpsertManyResult {
  /** Whether all operations succeeded */
  ok: boolean
  /** Number of documents that were inserted */
  insertedCount: number
  /** Number of documents that were modified */
  modifiedCount: number
  /** Number of documents that matched filters */
  matchedCount: number
  /** Number of documents that were upserted (inserted) */
  upsertedCount: number
  /** IDs of upserted documents */
  upsertedIds: EntityId[]
  /** Errors that occurred */
  errors: UpsertManyError[]
}

// =============================================================================
// IngestStream Types
// =============================================================================

/**
 * Options for ingestStream operation
 */
export interface IngestStreamOptions<T = Record<string, unknown>> {
  /** Batch size for bulk inserts (default: 100) */
  batchSize?: number | undefined
  /** Stop on first error if true (default: true) */
  ordered?: boolean | undefined
  /** Actor performing the operation */
  actor?: EntityId | undefined
  /** Skip validation */
  skipValidation?: boolean | undefined
  /** Override entity type for all documents */
  entityType?: string | undefined
  /** Transform function to apply to each document before insertion */
  transform?: ((doc: T) => T | null) | undefined
  /** Progress callback called after each document is processed */
  onProgress?: ((count: number) => void) | undefined
  /** Callback called after each batch is completed */
  onBatchComplete?: ((stats: IngestBatchStats) => void) | undefined
}

/**
 * Statistics for a completed batch
 */
export interface IngestBatchStats {
  /** Batch number (1-indexed) */
  batchNumber: number
  /** Number of documents in this batch */
  batchSize: number
  /** Total documents processed so far */
  totalProcessed: number
}

/**
 * Error entry in ingestStream result
 */
export interface IngestStreamError {
  /** Index of the failed document */
  index: number
  /** Error message */
  message: string
  /** Original error */
  error?: Error | undefined
}

/**
 * Result of ingestStream operation
 */
export interface IngestStreamResult {
  /** Number of documents successfully inserted */
  insertedCount: number
  /** Number of documents that failed */
  failedCount: number
  /** Number of documents skipped (transform returned null) */
  skippedCount: number
  /** IDs of inserted documents */
  insertedIds: EntityId[]
  /** Errors that occurred */
  errors: IngestStreamError[]
}

/**
 * Event log configuration options
 */
export interface EventLogConfig {
  /** Maximum number of events to keep in the log (default: 10000) */
  maxEvents?: number | undefined
  /** Maximum age of events in milliseconds (default: 7 days = 604800000) */
  maxAge?: number | undefined
  /** Whether to archive rotated events instead of dropping them (default: false) */
  archiveOnRotation?: boolean | undefined
  /** Maximum number of archived events to keep in memory (default: 50000). Only applies when archiveOnRotation is true. Older archived events are pruned when this limit is exceeded. */
  maxArchivedEvents?: number | undefined
  /** Maximum number of pending events before backpressure is applied (default: 10000). When exceeded, recordEvent will throw a BackpressureError. Set to 0 to disable the limit. */
  maxPendingEvents?: number | undefined
}

/**
 * Result of an event archival operation
 */
export interface ArchiveEventsResult {
  /** Number of events archived */
  archivedCount: number
  /** Number of events dropped (if archiveOnRotation is false) */
  droppedCount: number
  /** Number of old archived events pruned due to maxArchivedEvents limit */
  prunedCount?: number | undefined
  /** Timestamp of the oldest remaining event */
  oldestEventTs?: number | undefined
  /** Timestamp of the newest archived event */
  newestArchivedTs?: number | undefined
}

/**
 * Default event log configuration
 */
export const DEFAULT_EVENT_LOG_CONFIG: Required<EventLogConfig> = {
  maxEvents: 10000,
  maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days in milliseconds
  archiveOnRotation: false,
  maxArchivedEvents: 50000, // Default limit for archived events to prevent unbounded growth
  maxPendingEvents: 10000, // Default limit for pending events to prevent unbounded memory growth
}

/**
 * ParqueDB configuration options
 */
export interface ParqueDBConfig {
  /** Storage backend for data persistence */
  storage: StorageBackend

  /** Schema definition for entity validation */
  schema?: Schema | undefined

  /** Default namespace for operations */
  defaultNamespace?: string | undefined

  /** Snapshot configuration */
  snapshotConfig?: SnapshotConfig | undefined

  /** Event log configuration for rotation and archival */
  eventLogConfig?: EventLogConfig | undefined

  /** Storage router for determining storage mode and paths */
  storageRouter?: IStorageRouter | undefined

  /** Per-collection options from DB() schema */
  collectionOptions?: Map<string, CollectionOptions> | undefined

  /**
   * Maximum number of entities to cache in memory (default: 10000)
   *
   * The in-memory entity cache uses LRU (Least Recently Used) eviction
   * to prevent unbounded memory growth. When the cache exceeds this limit,
   * the least recently accessed entities are evicted automatically.
   *
   * Set to 0 to disable the cache limit (not recommended for production).
   *
   * @example
   * ```typescript
   * const db = new ParqueDB({
   *   storage,
   *   maxCacheSize: 5000, // Cache up to 5000 entities
   * })
   * ```
   */
  maxCacheSize?: number | undefined

  /**
   * Callback invoked when an entity is evicted from the cache due to LRU limits.
   *
   * This is useful for logging, metrics, or implementing write-through caching.
   *
   * @example
   * ```typescript
   * const db = new ParqueDB({
   *   storage,
   *   maxCacheSize: 1000,
   *   onCacheEvict: (key, entity) => {
   *     console.log(`Entity evicted: ${key}`)
   *     metrics.increment('cache.evictions')
   *   }
   * })
   * ```
   */
  onCacheEvict?: ((key: string, entity: Entity) => void) | undefined

  /**
   * Embedding provider for query-time text-to-vector conversion
   *
   * When configured, enables automatic embedding of text queries in $vector filters:
   *
   * @example
   * ```typescript
   * const db = new ParqueDB({
   *   storage,
   *   embeddingProvider: createWorkersAIProvider(env.AI)
   * })
   *
   * // Now can use text in vector queries
   * await db.Posts.find({
   *   $vector: {
   *     field: 'embedding',
   *     query: 'machine learning tutorials',  // auto-embedded
   *     topK: 10
   *   }
   * })
   * ```
   */
  embeddingProvider?: EmbeddingProvider | undefined

  /**
   * Event callback for materialized view integration
   *
   * When configured, this callback is invoked after every event is recorded
   * (CREATE, UPDATE, DELETE, REL_CREATE, REL_DELETE). This enables automatic
   * MV updates when data changes.
   *
   * @example
   * ```typescript
   * import { createMVIntegration } from 'parquedb/materialized-views'
   *
   * const { emitter, engine, bridge, mutationHook } = createMVIntegration()
   *
   * const db = new ParqueDB({
   *   storage,
   *   onEvent: (event) => emitter.emit(event)
   * })
   *
   * // Register MV handlers
   * engine.registerMV({
   *   name: 'OrderAnalytics',
   *   sourceNamespaces: ['orders'],
   *   async process(events) {
   *     // Update MV based on events
   *   }
   * })
   *
   * // Start engine and connect bridge
   * await engine.start()
   * bridge.connect()
   *
   * // Now all ParqueDB mutations automatically trigger MV updates
   * await db.create('orders', { total: 100 })
   * ```
   */
  onEvent?: ((event: Event) => void | Promise<void>) | undefined
}

// =============================================================================
// Collection Interface
// =============================================================================

/**
 * Collection interface for type-safe entity operations
 * Provides a fluent API for working with entities in a namespace
 */
export interface Collection<T extends EntityData = EntityData> {
  /** Namespace for this collection */
  readonly namespace: string

  /**
   * Find entities matching a filter
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @returns Paginated result set
   */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /**
   * Find a single entity matching a filter
   * @param filter - MongoDB-style filter
   * @param options - Query options
   * @returns Single entity or null if not found
   */
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>

  /**
   * Get a single entity by ID
   * @param id - Entity ID (can be full EntityId or just the id part)
   * @param options - Get options
   * @returns Entity or null if not found
   */
  get(id: string, options?: GetOptions): Promise<Entity<T> | null>

  /**
   * Create a new entity
   * @param data - Entity data
   * @param options - Create options
   * @returns Created entity
   */
  create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>

  /**
   * Update an entity
   * @param id - Entity ID
   * @param update - Update operations
   * @param options - Update options
   * @returns Updated entity or null
   */
  update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>

  /**
   * Delete an entity
   * @param id - Entity ID
   * @param options - Delete options
   * @returns Delete result
   */
  delete(id: string, options?: DeleteOptions): Promise<DeleteResult>

  /**
   * Delete multiple entities matching a filter
   * @param filter - MongoDB-style filter
   * @param options - Delete options
   * @returns Delete result with count
   */
  deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult>

  /**
   * Semantic search using vector similarity
   *
   * Searches for entities with vectors similar to the query.
   * Query can be a text string (requires embeddingProvider) or a pre-computed vector.
   *
   * @param query - Search query (text string or vector array)
   * @param options - Search options
   * @returns Array of entities with similarity scores
   *
   * @example
   * ```typescript
   * // Text query (requires embeddingProvider configured on db)
   * const results = await collection.semanticSearch('machine learning tutorials', {
   *   limit: 10,
   *   minScore: 0.7
   * })
   *
   * // Vector query
   * const results = await collection.semanticSearch(queryVector, {
   *   limit: 10,
   *   field: 'embedding'
   * })
   *
   * // Results include $score
   * results.forEach(r => console.log(r.name, r.$score))
   * ```
   */
  semanticSearch(
    query: string | number[],
    options?: SemanticSearchOptions | undefined
  ): Promise<SemanticSearchResult<T>[]>

  /**
   * Hybrid search combining vector similarity with metadata filtering
   *
   * Supports multiple strategies for combining vector search with filters:
   * - 'pre-filter': Apply filters first, then vector search on filtered set
   * - 'post-filter': Vector search first, then filter results
   * - 'auto': Automatically choose based on estimated filter selectivity
   *
   * @param query - Search query (text string or vector array)
   * @param options - Hybrid search options
   * @returns Array of entities with similarity scores
   *
   * @example
   * ```typescript
   * // Search with metadata filter
   * const results = await collection.hybridSearch('ML tutorials', {
   *   filter: { category: 'tech', status: 'published' },
   *   limit: 10,
   *   strategy: 'auto'
   * })
   *
   * // RRF-weighted hybrid with text + vector
   * const results = await collection.hybridSearch(queryVector, {
   *   filter: { author: 'alice' },
   *   limit: 20,
   *   strategy: 'pre-filter'
   * })
   * ```
   */
  hybridSearch(
    query: string | number[],
    options?: HybridSearchOptionsCollection | undefined
  ): Promise<SemanticSearchResult<T>[]>

  /**
   * Ingest a stream of documents into the collection
   *
   * Efficiently bulk-inserts documents from an async iterable or array,
   * with support for batching, transform functions, and progress callbacks.
   *
   * @param source - Async iterable or array of documents to ingest
   * @param options - Ingest options (batchSize, transform, callbacks, etc.)
   * @returns Result with counts of inserted, failed, and skipped documents
   *
   * @example
   * ```typescript
   * // Ingest from an array
   * const result = await collection.ingestStream([
   *   { name: 'Item 1', value: 10 },
   *   { name: 'Item 2', value: 20 },
   * ])
   *
   * // Ingest from async generator with transform
   * const result = await collection.ingestStream(asyncGenerator, {
   *   batchSize: 100,
   *   transform: (doc) => ({ ...doc, imported: true }),
   *   onProgress: (count) => console.log(`Processed ${count} documents`),
   * })
   * ```
   */
  ingestStream(
    source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>,
    options?: IngestStreamOptions<Partial<T>> | undefined
  ): Promise<IngestStreamResult>
}

// =============================================================================
// History and Diff Types
// =============================================================================

/**
 * History result item
 */
export interface HistoryItem {
  id: string
  ts: Date
  op: EventOp
  entityId: string
  ns: string
  before: Entity | null
  after: Entity | null
  actor?: EntityId | undefined
  metadata?: Metadata | undefined
}

/**
 * History result
 */
export interface HistoryResult {
  items: HistoryItem[]
  hasMore: boolean
  nextCursor?: string | undefined
}

/**
 * Diff result between two entity states
 */
export interface DiffResult {
  /** Fields that were added */
  added: string[]
  /** Fields that were removed */
  removed: string[]
  /** Fields that were changed */
  changed: string[]
  /** Before/after values for changed fields */
  values: {
    [field: string]: { before: unknown; after: unknown }
  }
}

/**
 * Options for revert operation
 */
export interface RevertOptions {
  /** Actor performing the revert */
  actor?: EntityId | undefined
}

/**
 * Options for getRelated operation
 */
export interface GetRelatedOptions {
  /** Cursor for pagination */
  cursor?: string | undefined
  /** Maximum results */
  limit?: number | undefined
  /** Filter related entities */
  filter?: Filter | undefined
  /** Sort order */
  sort?: SortSpec | undefined
  /** Field projection */
  project?: Projection | undefined
  /** Include soft-deleted */
  includeDeleted?: boolean | undefined
}

/**
 * Result of getRelated operation
 */
export interface GetRelatedResult<T extends EntityData = EntityData> {
  /** Related entities */
  items: Entity<T>[]
  /** Total count of related entities */
  total: number
  /** Whether there are more results */
  hasMore: boolean
  /** Cursor for next page */
  nextCursor?: string | undefined
}

// =============================================================================
// Transaction Types
// =============================================================================

/**
 * Transaction interface for ParqueDB
 */
export interface ParqueDBTransaction {
  create<T extends EntityData = EntityData>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T extends EntityData = EntityData>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  commit(): Promise<void>
  rollback(): Promise<void>
}

// =============================================================================
// Snapshot Types
// =============================================================================

/**
 * Snapshot data structure
 */
export interface Snapshot {
  id: string
  entityId: EntityId
  ns: string
  sequenceNumber: number
  eventId?: string | undefined
  createdAt: Date
  state: EntityState
  compressed: boolean
  size?: number | undefined
}

/**
 * Raw snapshot data (includes size info)
 */
export interface RawSnapshot {
  id: string
  size: number
  data: Uint8Array
}

/**
 * Query stats for snapshot usage
 */
export interface SnapshotQueryStats {
  snapshotsUsed: number
  eventsReplayed: number
  snapshotUsedAt?: number | undefined
}

/**
 * Storage stats for snapshots
 */
export interface SnapshotStorageStats {
  totalSize: number
  snapshotCount: number
  avgSnapshotSize: number
}

/**
 * Options for pruning snapshots
 */
export interface PruneSnapshotsOptions {
  olderThan?: Date | undefined
  keepMinimum?: number | undefined
}

/**
 * Snapshot manager interface
 */
export interface SnapshotManager {
  createSnapshot(entityId: EntityId): Promise<Snapshot>
  createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot>
  listSnapshots(entityId: EntityId): Promise<Snapshot[]>
  deleteSnapshot(snapshotId: string): Promise<void>
  pruneSnapshots(options: PruneSnapshotsOptions): Promise<void>
  getRawSnapshot(snapshotId: string): Promise<RawSnapshot>
  getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats>
  getStorageStats(): Promise<SnapshotStorageStats>
}

// =============================================================================
// Event Log Types
// =============================================================================

/**
 * Event Log interface for querying events
 */
export interface EventLog {
  /** Get events for a specific entity */
  getEvents(entityId: EntityId): Promise<Event[]>
  /** Get events by namespace */
  getEventsByNamespace(ns: string): Promise<Event[]>
  /** Get events by time range */
  getEventsByTimeRange(from: Date, to: Date): Promise<Event[]>
  /** Get events by operation type */
  getEventsByOp(op: EventOp): Promise<Event[]>
  /** Get raw event data (for compression check) */
  getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }>
  /** Get total event count */
  getEventCount(): Promise<number>
  /** Get current event log configuration */
  getConfig(): EventLogConfig
  /** Archive old events based on configuration or manual threshold */
  archiveEvents(options?: { olderThan?: Date | undefined; maxEvents?: number | undefined }): Promise<ArchiveEventsResult>
  /** Get archived events (if archiveOnRotation is enabled) */
  getArchivedEvents(): Promise<Event[]>
}

// =============================================================================
// Error Classes
// =============================================================================

// Re-export from the centralized errors module for backward compatibility
// These errors now extend ParqueDBError and support serialization for RPC
export {
  VersionConflictError,
  EntityNotFoundError,
  RelationshipError,
  EventError,
  BackpressureError,
} from '../errors'

// Import and re-export ValidationError with backward-compatible constructor
import {
  ValidationError as BaseValidationError,
  // ErrorCode is available from '../errors' if needed externally
} from '../errors'

/**
 * Error thrown when validation fails
 *
 * @deprecated Use ValidationError from '../errors' directly for new code.
 * This wrapper provides backward compatibility with the old constructor signature.
 */
export class ValidationError extends BaseValidationError {
  /** @deprecated Use context.field instead */
  readonly operation: string
  /** @deprecated Use field getter instead - namespace is available via inherited getter */
  readonly fieldName?: string | undefined

  constructor(
    operation: string,
    namespace: string,
    message: string,
    context?: {
      fieldName?: string | undefined
      expectedType?: string | undefined
      actualType?: string | undefined
    }
  ) {
    const contextMsg = context?.fieldName
      ? ` (field: ${context.fieldName}${context.expectedType ? `, expected: ${context.expectedType}` : ''}${context.actualType ? `, got: ${context.actualType}` : ''})`
      : ''
    super(
      `Validation failed for '${namespace}' ${operation}: ${message}${contextMsg}`,
      {
        field: context?.fieldName,
        expectedType: context?.expectedType,
        actualType: context?.actualType,
        namespace,
        operation,
      }
    )
    this.operation = operation
    this.fieldName = context?.fieldName
    // Note: namespace is already set via context in base class
    Object.setPrototypeOf(this, ValidationError.prototype)
  }
}

// Re-export types that are used externally
export type {
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
  Event,
  EventOp,
  SortSpec,
  Projection,
  StorageBackend,
  Schema,
  HistoryOptions,
}

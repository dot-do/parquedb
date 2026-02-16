/**
 * ParqueDB - A Parquet-based database for Node.js, browsers, and Cloudflare Workers
 *
 * @packageDocumentation
 */

// =============================================================================
// DB Factory (Recommended Entry Point)
// =============================================================================

export {
  /**
   * Creates a ParqueDB instance with optional schema definition.
   *
   * DB() is the recommended entry point for creating database instances.
   * It supports typed schemas, flexible (schema-less) mode, or mixed mode.
   *
   * @param input - Schema definition or flexible mode config. Defaults to flexible mode.
   * @param config - Optional configuration (storage backend, default namespace, etc.)
   * @returns A ParqueDB instance with typed collections and SQL executor attached
   *
   * @example
   * ```typescript
   * import { DB } from 'parquedb'
   *
   * // Typed schema with relationships
   * const db = DB({
   *   User: {
   *     email: 'string!#',    // required + indexed
   *     name: 'string',
   *     posts: '<- Post.author[]'  // reverse relationship
   *   },
   *   Post: {
   *     title: 'string!',
   *     content: 'text',
   *     author: '-> User'     // forward relationship
   *   }
   * })
   *
   * // Flexible mode (schema-less)
   * const flexibleDB = DB({ schema: 'flexible' })
   *
   * // Access collections
   * await db.User.create({ email: 'alice@example.com', name: 'Alice' })
   * await db.Post.find({ author: 'users/123' })
   *
   * // SQL queries
   * const users = await db.sql`SELECT * FROM users WHERE age > ${21}`
   * ```
   */
  DB,
  type CollectionSchema,
  type CollectionOptions,
  type DBSchema,
  type DBConfig,
  type DBInput,
  type DBInstance,
  // Type inference (compile-time schema -> TypeScript)
  type DBSchemaInput,
  type TypedDBInstance,
  // Collection options helpers
  DEFAULT_COLLECTION_OPTIONS,
  /**
   * Extracts $options from a collection schema, returning defaults if not specified.
   *
   * @param schema - The collection schema definition
   * @returns Required collection options with defaults applied
   */
  extractCollectionOptions,
  /**
   * Gets field definitions from a collection schema, excluding $-prefixed config.
   *
   * @param schema - The collection schema definition
   * @returns Object mapping field names to type strings
   */
  getFieldsWithoutOptions,
  /**
   * Extracts all collection options from a database schema.
   *
   * @param schema - The database schema definition
   * @returns Map of normalized collection name (lowercase) to options
   */
  extractAllCollectionOptions,
} from './db'

// Auto-configured db and sql (lazy initialization)
export {
  /**
   * Pre-configured ParqueDB instance with automatic runtime detection.
   * Uses lazy initialization - created on first access.
   *
   * @example
   * ```typescript
   * import { db } from 'parquedb'
   *
   * await db.Posts.create({ title: 'Hello World' })
   * await db.Posts.find({ status: 'published' })
   * ```
   */
  db,
  /**
   * SQL template tag for the auto-configured database instance.
   *
   * @example
   * ```typescript
   * import { sql } from 'parquedb'
   *
   * const posts = await sql`SELECT * FROM posts WHERE status = ${'published'}`
   * ```
   */
  sql,
  /**
   * Explicitly initializes the auto-configured database with custom options.
   * Call this before using db/sql if you need custom configuration.
   *
   * @param config - Configuration options for the database
   * @returns The initialized ParqueDB instance
   */
  initializeDB,
  /**
   * Gets the current auto-configured database instance.
   * Throws if not initialized and auto-initialization fails.
   *
   * @returns The ParqueDB instance
   */
  getDB,
  /**
   * Resets the auto-configured database instance.
   * Useful for testing or reconfiguration.
   */
  resetDB
} from './config/auto'

// Configuration utilities
export {
  /**
   * Defines a ParqueDB configuration object with type safety.
   *
   * @param config - The configuration options
   * @returns The configuration object (identity function for typing)
   *
   * @example
   * ```typescript
   * export default defineConfig({
   *   storage: new R2Backend(env.BUCKET),
   *   schema: { Posts: { title: 'string!' } }
   * })
   * ```
   */
  defineConfig,
  /**
   * Defines a database schema with type safety.
   *
   * @param schema - The schema definition
   * @returns The schema object (identity function for typing)
   */
  defineSchema,
  /**
   * Detects the current runtime environment.
   *
   * @returns 'node', 'workers', 'browser', or 'unknown'
   */
  detectRuntime,
  /**
   * Checks if running in a Node.js server environment.
   */
  isServer,
  /**
   * Checks if running in Cloudflare Workers environment.
   */
  isWorkers,
  /**
   * Checks if running in a browser environment.
   */
  isBrowser,
  /**
   * Loads Cloudflare Workers environment bindings.
   *
   * @param env - The Workers environment object
   * @returns Parsed environment configuration
   */
  loadWorkersEnv,
  type ParqueDBConfig as AutoConfig,
  type Runtime,
} from './config'

// =============================================================================
// Main Classes
// =============================================================================

export {
  /**
   * Main ParqueDB class providing a hybrid relational/document/graph database.
   *
   * ParqueDB is built on Apache Parquet and provides:
   * - MongoDB-style API with typed collections
   * - Bidirectional relationships with graph traversal
   * - Time-travel queries via CDC event log
   * - RPC pipelining support for Cloudflare Workers
   * - Full-text search, vector similarity, and secondary indexes
   *
   * @example
   * ```typescript
   * import { ParqueDB, MemoryBackend } from 'parquedb'
   *
   * const db = new ParqueDB({ storage: new MemoryBackend() })
   *
   * // CRUD operations
   * const post = await db.create('posts', { title: 'Hello', content: 'World' })
   * const found = await db.find('posts', { status: 'published' })
   * await db.update('posts', post.$id, { $set: { status: 'published' } })
   *
   * // Collection API (recommended)
   * const posts = await db.Posts.find({ author: 'users/123' })
   *
   * // Time-travel
   * const history = await db.history('posts/abc123')
   * const oldVersion = await db.get('posts', 'abc123', { asOf: new Date('2024-01-01') })
   * ```
   */
  ParqueDB,
  type ParqueDBConfig,
  type EventLogConfig,
  type ArchiveEventsResult,
  type EventLog,
  DEFAULT_EVENT_LOG_CONFIG,
} from './ParqueDB'

/**
 * Standalone in-memory Collection for testing/development.
 *
 * WARNING: This Collection uses module-level global Maps and does NOT use storage backends.
 * For production, use ParqueDB.collection() which properly delegates to storage backends.
 *
 * @example
 * ```typescript
 * import { Collection, clearGlobalStorage } from 'parquedb'
 *
 * // For testing only
 * const posts = Collection('posts')
 * await posts.create({ title: 'Test' })
 *
 * // Clean up after tests
 * clearGlobalStorage()
 * ```
 *
 * @see ParqueDB.collection() for production use
 */
export { Collection, clearGlobalStorage, getEventsForEntity, getEntityStateAtTime, clearEventLog } from './Collection'

// =============================================================================
// Types
// =============================================================================

export * from './types'

// =============================================================================
// Storage Backends
// =============================================================================

export {
  /**
   * In-memory storage backend for testing and development.
   * Data is stored in JavaScript Maps and does not persist.
   *
   * @example
   * ```typescript
   * import { ParqueDB, MemoryBackend } from 'parquedb'
   *
   * const db = new ParqueDB({ storage: new MemoryBackend() })
   * ```
   */
  MemoryBackend,
  /**
   * Node.js filesystem storage backend.
   * Stores data as Parquet files on the local filesystem.
   *
   * @example
   * ```typescript
   * import { ParqueDB, FsBackend } from 'parquedb'
   *
   * const db = new ParqueDB({
   *   storage: new FsBackend({ basePath: './data' })
   * })
   * ```
   */
  FsBackend,
  /**
   * Cloudflare R2 storage backend for production deployments.
   * Supports multipart uploads, ETags for optimistic concurrency, and streaming.
   *
   * @example
   * ```typescript
   * import { ParqueDB, R2Backend } from 'parquedb'
   *
   * export default {
   *   async fetch(request, env) {
   *     const db = new ParqueDB({
   *       storage: new R2Backend(env.MY_BUCKET)
   *     })
   *     // ...
   *   }
   * }
   * ```
   */
  R2Backend,
  /** Error thrown when an R2 operation fails */
  R2OperationError,
  /** Error thrown when ETag doesn't match (optimistic concurrency conflict) */
  R2ETagMismatchError,
  /** Error thrown when an R2 object is not found */
  R2NotFoundError,
  // Capability introspection
  /**
   * Gets the capabilities of a storage backend.
   *
   * @param backend - The storage backend to inspect
   * @returns Object describing backend capabilities
   */
  getStorageCapabilities,
  /**
   * Checks if a storage backend has a specific capability.
   *
   * @param backend - The storage backend to inspect
   * @param capability - The capability to check for
   * @returns True if the backend has the capability
   */
  hasStorageCapability,
  /** Checks if a storage backend supports streaming reads */
  isStreamable,
  /** Checks if a storage backend supports multipart uploads */
  isMultipart,
  /** Checks if a storage backend supports transactions */
  isTransactional,
  // FsxBackend,
  /**
   * Cloudflare Durable Object SQLite storage backend.
   * Stores parquet file blocks directly in DO SQLite as blobs.
   * Sub-millisecond latency — co-located with the DO.
   *
   * @example
   * ```typescript
   * import { ParqueDB, DOSqliteBackend } from 'parquedb'
   *
   * export class MyDO extends DurableObject {
   *   constructor(state, env) {
   *     const db = new ParqueDB({
   *       storage: new DOSqliteBackend(state.storage.sql)
   *     })
   *   }
   * }
   * ```
   */
  DOSqliteBackend,
} from './storage'
export type { R2BackendOptions, DOSqliteBackendOptions, StorageCapabilities } from './storage'

// =============================================================================
// Schema
// =============================================================================

export {
  // Parsing
  parseSchema,
  parseFieldType,
  parseRelation,
  isRelationString,
  isValidFieldType,
  isValidRelationString,
  parseNestedField,
  // Schema Validation
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  validateEntityCoreFields,
  validateEntityFull,
  // Runtime Validation
  SchemaValidationError,
  SchemaValidator,
  createValidator,
  validate,
  // Schema Inference
  inferSchema,
  inferSchemaFromCollections,
  inferredToTypeDefinition,
} from './schema'

export type {
  ValidationMode,
  SchemaValidatorOptions,
} from './schema'

// =============================================================================
// Client (for RPC) - temporarily disabled for Worker build
// =============================================================================

// export {
//   ParqueDBClient,
//   createParqueDBClient,
//   type ParqueDBClientOptions,
// } from './client'

// =============================================================================
// Query Utilities
// =============================================================================

export {
  /**
   * Tests if an entity matches a MongoDB-style filter.
   *
   * @param entity - The entity to test
   * @param filter - The filter criteria
   * @returns True if the entity matches the filter
   *
   * @example
   * ```typescript
   * import { matchesFilter } from 'parquedb'
   *
   * const entity = { status: 'published', views: 100 }
   * matchesFilter(entity, { status: 'published' })           // true
   * matchesFilter(entity, { views: { $gt: 50 } })            // true
   * matchesFilter(entity, { $and: [{ status: 'draft' }] })   // false
   * ```
   */
  matchesFilter,
  /**
   * Creates a predicate function from a MongoDB-style filter.
   *
   * @param filter - The filter criteria
   * @returns A function that tests entities against the filter
   *
   * @example
   * ```typescript
   * const isPublished = createPredicate({ status: 'published' })
   * const published = entities.filter(isPublished)
   * ```
   */
  createPredicate,
} from './query/filter'

export {
  /**
   * Applies MongoDB-style update operators to an entity.
   *
   * @param entity - The entity to update
   * @param update - The update operations to apply
   * @returns The updated entity
   *
   * @example
   * ```typescript
   * import { applyUpdate } from 'parquedb'
   *
   * const entity = { count: 5, tags: ['a'] }
   * const updated = applyUpdate(entity, {
   *   $inc: { count: 1 },
   *   $push: { tags: 'b' }
   * })
   * // { count: 6, tags: ['a', 'b'] }
   * ```
   */
  applyUpdate,
} from './query/update'

export {
  /**
   * Fluent query builder for constructing filters programmatically.
   *
   * @example
   * ```typescript
   * import { QueryBuilder } from 'parquedb'
   *
   * const filter = new QueryBuilder()
   *   .where('status', 'eq', 'published')
   *   .where('views', 'gt', 100)
   *   .or([
   *     { category: 'tech' },
   *     { featured: true }
   *   ])
   *   .build()
   * ```
   */
  QueryBuilder,
  type ComparisonOp,
  type StringOp,
  type ExistenceOp,
  type QueryOp,
} from './query/builder'

// =============================================================================
// Aggregation Framework
// =============================================================================

export {
  // Executor
  /**
   * Executes a MongoDB-style aggregation pipeline on a collection.
   *
   * Supports stages: $match, $group, $sort, $limit, $skip, $project,
   * $unwind, $lookup, $count, $addFields, $set, $unset, $replaceRoot,
   * $facet, $bucket, $sample
   *
   * @param db - The ParqueDB instance
   * @param namespace - The collection namespace
   * @param pipeline - Array of aggregation stages
   * @param options - Optional aggregation options
   * @returns Array of aggregation results
   *
   * @example
   * ```typescript
   * import { executeAggregation } from 'parquedb'
   *
   * const results = await executeAggregation(db, 'orders', [
   *   { $match: { status: 'completed' } },
   *   { $group: { _id: '$customerId', total: { $sum: '$amount' } } },
   *   { $sort: { total: -1 } },
   *   { $limit: 10 }
   * ])
   * ```
   */
  executeAggregation,
  /**
   * Aggregation pipeline executor with chainable API.
   *
   * @example
   * ```typescript
   * const executor = new AggregationExecutor(db, 'orders')
   * const results = await executor
   *   .match({ status: 'completed' })
   *   .group({ _id: '$category', count: { $sum: 1 } })
   *   .execute()
   * ```
   */
  AggregationExecutor,
  // Types
  type AggregationStage,
  type AggregationOptions,
  type AggregationExplain,
  // Stage types
  type MatchStage,
  type GroupStage,
  type GroupSpec,
  type SortStage,
  type SortOrder,
  type LimitStage,
  type SkipStage,
  type ProjectStage,
  type ProjectionValue,
  type UnwindStage,
  type UnwindOptions,
  type LookupStage,
  type LookupOptions,
  type CountStage,
  type AddFieldsStage,
  type SetStage,
  type UnsetStage,
  type ReplaceRootStage,
  type FacetStage,
  type BucketStage,
  type SampleStage,
  // Accumulator types
  type Accumulator,
  type SumAccumulator,
  type AvgAccumulator,
  type MinAccumulator,
  type MaxAccumulator,
  type CountAccumulator,
  type FirstAccumulator,
  type LastAccumulator,
  type PushAccumulator,
  type AddToSetAccumulator,
  // Type guards
  isMatchStage,
  isGroupStage,
  isSortStage,
  isLimitStage,
  isSkipStage,
  isProjectStage,
  isUnwindStage,
  isLookupStage,
  isCountStage,
  isAddFieldsStage,
  isSetStage,
  isUnsetStage,
  isReplaceRootStage,
  isFacetStage,
  isBucketStage,
  isSampleStage,
} from './aggregation'

// =============================================================================
// Indexes
// =============================================================================

export {
  // Index Manager
  /**
   * Manages indexes for ParqueDB collections.
   * Supports secondary indexes, full-text search, vector similarity, and bloom filters.
   *
   * @example
   * ```typescript
   * const indexManager = db.getIndexManager()
   *
   * // Create a secondary index
   * await indexManager.createIndex('users', {
   *   name: 'email_idx',
   *   type: 'secondary',
   *   fields: ['email'],
   *   unique: true
   * })
   *
   * // Create a full-text search index
   * await indexManager.createIndex('posts', {
   *   name: 'content_fts',
   *   type: 'fts',
   *   fields: ['title', 'content']
   * })
   * ```
   */
  IndexManager,
  // Full-Text Search
  /**
   * Full-text search index using inverted index and BM25 scoring.
   *
   * @example
   * ```typescript
   * const fts = new FTSIndex()
   * fts.add('doc1', 'The quick brown fox')
   * fts.add('doc2', 'The lazy dog')
   *
   * const results = fts.search('quick fox')
   * ```
   */
  FTSIndex,
  /** Inverted index data structure for full-text search */
  InvertedIndex,
  /** BM25 scoring algorithm for relevance ranking */
  BM25Scorer,
  /**
   * Tokenizes text into words for indexing.
   *
   * @param text - The text to tokenize
   * @returns Array of tokens
   */
  tokenize,
  /**
   * Tokenizes a search query with query-specific handling.
   *
   * @param query - The search query
   * @returns Array of query tokens
   */
  tokenizeQuery,
  /**
   * Applies Porter stemming algorithm to a word.
   *
   * @param word - The word to stem
   * @returns The stemmed word
   */
  porterStem,
  // Key Encoding (kept for backward compatibility)
  /** Encodes a value as a sortable index key */
  encodeKey,
  /** Decodes an index key back to its original value */
  decodeKey,
  /** Compares two encoded keys for sorting */
  compareKeys,
  /** Generates a hash from a key for bloom filter indexing */
  hashKey,
  // Types
  type IndexDefinition,
  type IndexMetadata,
  type IndexStats,
  type IndexLookupResult,
  type RangeQuery,
  type FTSSearchOptions,
  type FTSSearchResult,
} from './indexes'

// =============================================================================
// Events (CDC / Time-Travel)
// =============================================================================

export {
  // Types
  type Event,
  type EventBatch,
  type EventSegment,
  type EventManifest,
  type EventWriterConfig,
  type DatasetConfig,
  type TimeTravelOptions,
  // Utilities
  isRelationshipTarget,
  parseEntityTarget,
  parseRelTarget,
  entityTarget,
  relTarget,
} from './events'

// =============================================================================
// Embeddings (Workers AI & Vercel AI SDK)
// =============================================================================

export {
  // Workers AI Embeddings (Cloudflare Workers)
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL as DEFAULT_EMBEDDING_MODEL,
  DEFAULT_DIMENSIONS as DEFAULT_EMBEDDING_DIMENSIONS,
  EMBEDDING_MODELS,
  // Vercel AI SDK Embeddings (Node.js)
  AISDKEmbeddings,
  createAISDKEmbeddings,
  getAISDKModelDimensions,
  listAISDKModels,
  AI_SDK_MODELS,
  DEFAULT_AI_SDK_DIMENSIONS,
  // Auto-Embedding
  processEmbedOperator,
  autoEmbedFields,
  hasEmbedOperator,
  extractEmbedOperator,
  buildAutoEmbedConfig,
  // Types
  type AIBinding,
  type EmbeddingModelConfig,
  type EmbedOptions,
  type EmbeddingProvider,
  type AISDKProvider,
  type AISDKEmbeddingsConfig,
  type AISDKEmbedOptions,
  type AutoEmbedFieldConfig,
  type AutoEmbedConfig,
  type ProcessEmbeddingsOptions,
} from './embeddings'

// =============================================================================
// Observability (Hooks & Metrics)
// =============================================================================

export {
  // Hook Registry
  HookRegistry,
  MetricsCollector,
  globalHookRegistry,
  // Context Factories
  generateOperationId,
  createQueryContext,
  createMutationContext,
  createStorageContext,
  // Types
  type HookContext,
  type QueryContext,
  type MutationContext,
  type StorageContext,
  type QueryResult as ObservabilityQueryResult,
  type MutationResult as ObservabilityMutationResult,
  type StorageResult,
  type QueryHook,
  type MutationHook,
  type StorageHook,
  type ObservabilityHook,
  type OperationMetrics,
  type AggregatedMetrics,
} from './observability'

// =============================================================================
// Migration Utilities
// =============================================================================

export {
  // JSON import
  /**
   * Imports entities from a JSON array file.
   *
   * @param db - The ParqueDB instance
   * @param namespace - Target collection namespace
   * @param filePath - Path to JSON file (array of objects)
   * @param options - Optional import options
   * @returns Migration result with counts and errors
   *
   * @example
   * ```typescript
   * import { importFromJson } from 'parquedb'
   *
   * const result = await importFromJson(db, 'users', './users.json', {
   *   batchSize: 100,
   *   transform: (doc) => ({ ...doc, importedAt: new Date() })
   * })
   * console.log(`Imported ${result.insertedCount} users`)
   * ```
   */
  importFromJson,
  /**
   * Imports entities from a JSONL (JSON Lines) file.
   * Each line is a separate JSON object.
   *
   * @param db - The ParqueDB instance
   * @param namespace - Target collection namespace
   * @param filePath - Path to JSONL file
   * @param options - Optional import options
   * @returns Migration result with counts and errors
   */
  importFromJsonl,
  // CSV import
  /**
   * Imports entities from a CSV file.
   *
   * @param db - The ParqueDB instance
   * @param namespace - Target collection namespace
   * @param filePath - Path to CSV file
   * @param options - Optional import options (delimiter, headers, etc.)
   * @returns Migration result with counts and errors
   *
   * @example
   * ```typescript
   * import { importFromCsv } from 'parquedb'
   *
   * const result = await importFromCsv(db, 'products', './products.csv', {
   *   delimiter: ',',
   *   headers: true,
   *   transform: (row) => ({
   *     ...row,
   *     price: parseFloat(row.price)
   *   })
   * })
   * ```
   */
  importFromCsv,
  // MongoDB import
  /**
   * Imports entities from a running MongoDB database.
   *
   * @param db - The ParqueDB instance
   * @param mongoUri - MongoDB connection URI
   * @param options - Import options including database and collection mapping
   * @returns Migration result with counts and errors
   */
  importFromMongodb,
  /**
   * Imports entities from a MongoDB BSON dump file.
   *
   * @param db - The ParqueDB instance
   * @param namespace - Target collection namespace
   * @param filePath - Path to BSON file
   * @param options - Optional import options
   * @returns Migration result with counts and errors
   */
  importFromBson,
  // Utilities
  /**
   * Infers the field type from a JavaScript value.
   *
   * @param value - The value to analyze
   * @returns Inferred type string (e.g., 'string', 'int', 'date')
   */
  inferType,
  /**
   * Parses a single CSV line into an array of values.
   *
   * @param line - The CSV line to parse
   * @param delimiter - Field delimiter (default: ',')
   * @returns Array of field values
   */
  parseCsvLine,
  /**
   * Converts a BSON value to a ParqueDB-compatible value.
   *
   * @param value - The BSON value
   * @returns Converted JavaScript value
   */
  convertBsonValue,
  // Types
  type MigrationOptions,
  type JsonImportOptions,
  type CsvImportOptions,
  type BsonImportOptions,
  type MigrationResult,
  type MigrationError,
} from './migration'

// =============================================================================
// Error Handling
// =============================================================================

export {
  // Base class
  ParqueDBError,
  ErrorCode,
  // Validation errors
  ValidationError as ParqueDBValidationError,
  // Not found errors
  NotFoundError as ParqueDBNotFoundError,
  EntityNotFoundError as ParqueDBEntityNotFoundError,
  IndexNotFoundError as ParqueDBIndexNotFoundError,
  EventNotFoundError,
  SnapshotNotFoundError,
  FileNotFoundError as ParqueDBFileNotFoundError,
  // Conflict errors
  ConflictError,
  VersionConflictError as ParqueDBVersionConflictError,
  AlreadyExistsError as ParqueDBAlreadyExistsError,
  ETagMismatchError as ParqueDBETagMismatchError,
  UniqueConstraintError as ParqueDBUniqueConstraintError,
  // Relationship errors
  RelationshipError as ParqueDBRelationshipError,
  // Query errors
  QueryError,
  InvalidFilterError,
  // Storage errors
  StorageError as ParqueDBStorageError,
  QuotaExceededError as ParqueDBQuotaExceededError,
  InvalidPathError as ParqueDBInvalidPathError,
  PathTraversalError as ParqueDBPathTraversalError,
  NetworkError as ParqueDBNetworkError,
  // Authorization errors
  AuthorizationError,
  PermissionDeniedError as ParqueDBPermissionDeniedError,
  // Configuration errors
  ConfigurationError,
  // Timeout error
  TimeoutError,
  // RPC errors
  RpcError as ParqueDBRpcError,
  // Index errors
  IndexError,
  IndexBuildError as ParqueDBIndexBuildError,
  IndexLoadError as ParqueDBIndexLoadError,
  IndexAlreadyExistsError as ParqueDBIndexAlreadyExistsError,
  // Event errors
  EventError as ParqueDBEventError,
  // Type guards
  isParqueDBError,
  isValidationError,
  isNotFoundError,
  isEntityNotFoundError,
  isConflictError,
  isVersionConflictError,
  isETagMismatchError,
  isAlreadyExistsError,
  isStorageError,
  isRelationshipError,
  isQueryError,
  isAuthorizationError,
  isRpcError,
  isIndexError,
  isEventError,
  // Factory functions
  wrapError,
  errorFromStatus,
  assertValid,
  assertFound,
  // Types
  type SerializedError,
} from './errors'

// =============================================================================
// Mutation Layer
// =============================================================================

export {
  // Main Executor
  MutationExecutor,
  VersionConflictError,
  // Types
  type MutationContext as MutationLayerContext,
  type CreateResult as MutationCreateResult,
  type UpdateResult as MutationLayerUpdateResult,
  type DeleteResult as MutationLayerDeleteResult,
  type MutationEvent,
  type MutationError,
  type MutationErrorCode,
  MutationErrorCodes,
  MutationOperationError,
  type MutationHooks,
  type MutationExecutorConfig,
  type EntityStore,
  // Operations
  executeCreate,
  executeUpdate,
  executeDelete,
  // Operators
  applyOperators,
  getField,
  setField,
  unsetField,
  validateUpdateOperators,
} from './mutation'

// =============================================================================
// Constants
// =============================================================================

export {
  // Limits
  DEFAULT_MAX_INBOUND,
  DEFAULT_PAGE_SIZE,
  MAX_BATCH_SIZE,
  // Concurrency
  DEFAULT_CONCURRENCY,
  // Storage
  MIN_PART_SIZE,
  DEFAULT_PART_SIZE,
  MAX_PARTS,
  // Cache
  DEFAULT_CACHE_TTL,
  MAX_CACHE_SIZE,
  DATA_CACHE_TTL_MS,
  DATA_CACHE_MAX_ENTRIES,
  // Parquet
  DEFAULT_ROW_GROUP_SIZE,
  DEFAULT_PARQUET_PAGE_SIZE,
  /**
   * Default compression codec for Parquet files.
   * Set to 'none' (UNCOMPRESSED) because on Cloudflare Workers,
   * storage is cheap and CPU is expensive - decompression hurts latency.
   */
  DEFAULT_COMPRESSION,
  // Bloom Filters
  DEFAULT_BLOOM_SIZE,
  DEFAULT_NUM_HASH_FUNCTIONS,
  ROW_GROUP_BLOOM_SIZE,
  // HNSW Vector Index
  DEFAULT_HNSW_M,
  DEFAULT_HNSW_EF_CONSTRUCTION,
  DEFAULT_HNSW_EF_SEARCH,
  // Events
  DEFAULT_MAX_EVENTS,
  DEFAULT_MAX_EVENT_AGE,
  DEFAULT_EVENT_BUFFER_SIZE,
  DEFAULT_EVENT_BUFFER_BYTES,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_R2_THRESHOLD_BYTES,
  // Retry
  DEFAULT_MAX_RETRIES,
  DEFAULT_RETRY_BASE_DELAY,
  DEFAULT_RETRY_MAX_DELAY,
  DEFAULT_RETRY_MULTIPLIER,
  DEFAULT_RETRY_JITTER_FACTOR,
} from './constants'

// =============================================================================
// SQL Integration
// =============================================================================

export {
  // SQL Template Tag
  /**
   * Creates a SQL template tag executor for a ParqueDB instance.
   *
   * @param db - The ParqueDB instance
   * @param options - Optional SQL execution options
   * @returns A template tag function for SQL queries
   *
   * @example
   * ```typescript
   * import { ParqueDB, createSQL } from 'parquedb'
   *
   * const db = new ParqueDB({ storage })
   * const sql = createSQL(db)
   *
   * // Execute SQL queries
   * const users = await sql`SELECT * FROM users WHERE age > ${21}`
   * const posts = await sql`SELECT title, content FROM posts LIMIT ${10}`
   * ```
   */
  createSQL,
  /**
   * Builds a SQL query string from template parts.
   *
   * @param strings - Template string parts
   * @param values - Interpolated values
   * @returns Object with query string and parameter values
   */
  buildQuery,
  /** Escapes an identifier (table/column name) for SQL */
  escapeIdentifier,
  /** Escapes a string value for SQL */
  escapeString,
  // Drizzle ORM
  /**
   * Creates a Drizzle ORM proxy for ParqueDB.
   * Allows using Drizzle's type-safe query builder with ParqueDB.
   *
   * @param db - The ParqueDB instance
   * @param options - Optional proxy options
   * @returns A Drizzle-compatible proxy
   */
  createDrizzleProxy,
  // Prisma ORM
  /**
   * Creates a Prisma adapter for ParqueDB.
   * Allows using Prisma Client with ParqueDB as the backend.
   *
   * @param db - The ParqueDB instance
   * @param options - Optional adapter options
   * @returns A Prisma-compatible adapter
   */
  createPrismaAdapter,
  /** Prisma adapter class for ParqueDB integration */
  PrismaParqueDBAdapter,
  // Parser & Translator
  /**
   * Parses a SQL string into an AST.
   *
   * @param sql - The SQL string to parse
   * @returns Parsed SQL statement AST
   */
  parseSQL,
  /** Translates a SQL statement to ParqueDB operations */
  translateStatement,
  /** Translates a SELECT statement to a find query */
  translateSelect,
  /** Translates an INSERT statement to a create operation */
  translateInsert,
  /** Translates an UPDATE statement to an update operation */
  translateUpdate,
  /** Translates a DELETE statement to a delete operation */
  translateDelete,
  /** Translates a SQL WHERE clause to filter operations */
  translateWhere,
  /** Converts a SQL WHERE clause to a MongoDB-style filter */
  whereToFilter,
  // Types
  type SQLExecutor,
  type CreateSQLOptions,
  type DrizzleProxyOptions,
  type DrizzleProxyCallback,
  type PrismaAdapterOptions,
  type SQLStatement,
  type SQLSelect,
  type SQLInsert,
  type SQLUpdate,
  type SQLDelete,
  type SQLQueryResult,
  type TranslatedQuery,
  type TranslatedMutation,
} from './integrations/sql'

// =============================================================================
// Entity Backends (Pluggable Storage Formats)
// =============================================================================

export {
  // Factory
  /**
   * Factory function to create an entity backend of the specified type.
   *
   * @param type - Backend type: 'native', 'iceberg', or 'delta'
   * @param config - Backend-specific configuration
   * @returns The configured entity backend
   *
   * @example
   * ```typescript
   * import { createBackend } from 'parquedb'
   *
   * const backend = createBackend('iceberg', {
   *   storage: r2Backend,
   *   catalogPath: 'catalog'
   * })
   * ```
   */
  createBackend,
  // Iceberg Backend
  /**
   * Apache Iceberg format backend for lakehouse-style storage.
   * Provides ACID transactions, schema evolution, time-travel, and partition pruning.
   *
   * @example
   * ```typescript
   * import { IcebergBackend, R2Backend } from 'parquedb'
   *
   * const storage = new R2Backend(env.MY_BUCKET)
   * const backend = new IcebergBackend({
   *   storage,
   *   catalogPath: 'iceberg-catalog'
   * })
   *
   * // Write with ACID guarantees
   * await backend.write('users', entities, schema)
   *
   * // Time-travel query
   * const oldData = await backend.read('users', { snapshotId: 'abc123' })
   * ```
   */
  IcebergBackend,
  /**
   * Creates an IcebergBackend with the specified storage.
   *
   * @param storage - The storage backend for Parquet files
   * @param options - Iceberg-specific options
   * @returns Configured IcebergBackend instance
   */
  createIcebergBackend,
  /**
   * Creates an IcebergBackend configured for Cloudflare R2.
   *
   * @param bucket - The R2 bucket binding
   * @param options - Optional configuration
   * @returns IcebergBackend configured for R2
   */
  createR2IcebergBackend,
  // Capability introspection
  /** Gets the capabilities of an entity backend */
  getEntityBackendCapabilities,
  /** Checks if an entity backend has a specific capability */
  hasEntityBackendCapability,
  /** Checks if an entity backend is compatible with a specific engine */
  isCompatibleWithEngine,
  // Types
  type EntityBackend,
  type BackendType,
  type BackendConfig,
  type NativeBackendConfig,
  type IcebergBackendConfig,
  type IcebergCatalogConfig,
  type DeltaBackendConfig,
  type EntitySchema,
  type SchemaField,
  type SchemaFieldType,
  type SnapshotInfo,
  type CompactOptions,
  type CompactResult,
  type VacuumOptions,
  type VacuumResult,
  type BackendStats,
  type EntityBackendCapabilities,
} from './backends'

// =============================================================================
// Relationships (Batch Loading)
// =============================================================================

export {
  // Batch Loader for N+1 elimination
  RelationshipBatchLoader,
  createBatchLoader,
  // Types
  type BatchLoadRequest,
  type BatchLoadResult,
  type BatchLoaderOptions,
  type BatchLoaderDB,
} from './relationships'

// =============================================================================
// Integrations (Iceberg, etc.)
// =============================================================================

export {
  // Iceberg Metadata (Basic)
  IcebergMetadataManager,
  IcebergStorageAdapter,
  createIcebergMetadataManager,
  enableIcebergMetadata,
  parqueDBTypeToIceberg,
  icebergTypeToParqueDB,
  // Iceberg Metadata (Native - requires @dotdo/iceberg)
  NativeIcebergMetadataManager,
  NativeIcebergStorageAdapter,
  createNativeIcebergManager,
  enableNativeIcebergMetadata,
  // Types (Basic)
  type IcebergMetadataOptions,
  type IcebergSnapshotRef,
  type IcebergDataFile,
  type IcebergSchema,
  type IcebergField,
  type IcebergType,
  type IcebergCommitResult,
  // Types (Native)
  type NativeIcebergOptions,
  type IcebergNativeSchema,
  type PartitionSpecDefinition,
  type SortOrderDefinition,
  type NativeDataFile,
  type NativeCommitResult,
} from './integrations'

// =============================================================================
// Sync Module (Push/Pull/Sync)
// =============================================================================

export {
  // Manifest types
  /**
   * Creates a sync manifest from the current database state.
   *
   * @param storage - The storage backend
   * @returns The sync manifest with file hashes
   */
  createManifest,
  /**
   * Computes the diff between two manifests.
   *
   * @param local - The local manifest
   * @param remote - The remote manifest
   * @returns Diff showing added, modified, deleted files
   */
  diffManifests,
  /**
   * Resolves conflicts between local and remote changes.
   *
   * @param conflicts - Array of conflicts to resolve
   * @param strategy - Conflict resolution strategy
   * @returns Resolved file operations
   */
  resolveConflicts,
  /** Updates a file entry in a manifest */
  updateManifestFile,
  /** Removes a file entry from a manifest */
  removeManifestFile,
  // Sync engine
  /**
   * Sync engine for bidirectional database synchronization.
   * Supports push, pull, and bidirectional sync with conflict resolution.
   *
   * @example
   * ```typescript
   * import { SyncEngine } from 'parquedb'
   *
   * const engine = new SyncEngine({
   *   local: localStorage,
   *   remote: remoteStorage,
   *   conflictStrategy: 'remote-wins'
   * })
   *
   * // Push local changes to remote
   * const pushResult = await engine.push()
   *
   * // Pull remote changes to local
   * const pullResult = await engine.pull()
   *
   * // Bidirectional sync
   * const syncResult = await engine.sync()
   * ```
   */
  SyncEngine,
  /**
   * Creates a SyncEngine with the specified options.
   *
   * @param options - Sync engine configuration
   * @returns Configured SyncEngine instance
   */
  createSyncEngine,
  // Branch management
  /**
   * Manages database branches for git-style version control.
   * Create, switch, and merge branches of your database.
   */
  BranchManager,
  type BranchManagerOptions,
  createBranchManager,
  // Types
  type SyncManifest,
  type SyncFileEntry,
  type SyncDiff,
  type SyncConflict,
  type ConflictStrategy,
  type SyncOptions,
  type SyncProgress,
  type SyncResult,
  type SyncError,
  type SyncEngineOptions,
  // Event-level bidirectional sync (lightweight CDC sync)
  resolveEventSync,
  computeEventSyncCursor,
  type EventSyncEvent,
  type EventSyncConflictStrategy,
  type EventSyncConflictInfo,
  type EventSyncResult,
  type ResolveEventSyncParams,
  type ResolveEventSyncResult,
} from './sync'

// =============================================================================
// Remote Client (Public Database Access)
// =============================================================================

export {
  // Remote database client
  /**
   * Opens a read-only connection to a public ParqueDB database.
   * Useful for accessing shared datasets without deploying a database.
   *
   * @param url - The URL of the remote ParqueDB instance
   * @param options - Optional connection options
   * @returns A RemoteDB instance for read-only queries
   *
   * @example
   * ```typescript
   * import { openRemoteDB } from 'parquedb'
   *
   * const db = await openRemoteDB('https://data.example.com/mydb')
   *
   * // Query the remote database
   * const posts = await db.Posts.find({ status: 'published' })
   * ```
   */
  openRemoteDB,
  /**
   * Checks if a remote ParqueDB database is accessible.
   *
   * @param url - The URL to check
   * @returns Database info if accessible, null otherwise
   */
  checkRemoteDB,
  /**
   * Lists publicly available ParqueDB databases.
   *
   * @param registryUrl - Optional registry URL
   * @returns Array of available database info
   */
  listPublicDatabases,
  // Types
  type RemoteDB,
  type RemoteCollection,
  type RemoteDBInfo,
  type OpenRemoteDBOptions,
} from './client/remote'

// =============================================================================
// Visibility Types
// =============================================================================

export {
  // Visibility types
  DEFAULT_VISIBILITY,
  VISIBILITY_VALUES,
  isValidVisibility,
  parseVisibility,
  allowsAnonymousRead,
  allowsDiscovery,
  type Visibility,
} from './types/visibility'

// =============================================================================
// Security (CSRF Protection)
// =============================================================================

export {
  // CSRF Middleware (for Hono)
  csrf,
  csrfToken,
  validateCsrf,
  // Token Functions
  generateCsrfToken,
  verifyCsrfToken,
  // CORS Helpers
  buildSecureCorsHeaders,
  getAllowedOriginHeader,
  // Types
  type CsrfOptions,
  type CsrfValidationResult,
  type CsrfTokenPayload,
} from './security'

// =============================================================================
// Search Client (Tree-Shakable)
// =============================================================================

export {
  /**
   * Performs a full-text search on a dataset.
   *
   * @param dataset - The dataset to search
   * @param query - The search query string
   * @param options - Optional search parameters
   * @returns Search results with relevance scores
   *
   * @example
   * ```typescript
   * import { search } from 'parquedb'
   *
   * const results = await search('products', 'wireless headphones', {
   *   limit: 20,
   *   fields: ['title', 'description']
   * })
   * ```
   */
  search,
  /**
   * Performs a vector similarity search.
   *
   * @param dataset - The dataset to search
   * @param vector - The query vector (embedding)
   * @param options - Optional search parameters
   * @returns Nearest neighbors by cosine similarity
   */
  vectorSearch,
  /**
   * Performs a hybrid search combining full-text and vector similarity.
   *
   * @param dataset - The dataset to search
   * @param query - The search query
   * @param options - Optional parameters including vector weights
   * @returns Combined and re-ranked results
   */
  hybridSearch,
  /**
   * Gets search suggestions/autocomplete for a query prefix.
   *
   * @param dataset - The dataset to search
   * @param prefix - The query prefix
   * @param options - Optional suggestion parameters
   * @returns Suggested completions
   */
  suggest,
  /**
   * Creates a search client for a specific dataset.
   *
   * @param dataset - The dataset identifier
   * @returns A SearchClient instance
   */
  createSearchClient,
  type IMDBTitle,
  type ONETOccupation,
  // Entity search (cross-type full-text scoring)
  scoreEntity,
  searchEntities,
  type ScoreResult,
  type EntitySearchParams,
  type EntitySearchResult,
} from './search'

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'

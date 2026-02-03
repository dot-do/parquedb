/**
 * ParqueDB - A Parquet-based database for Node.js, browsers, and Cloudflare Workers
 *
 * @packageDocumentation
 */

// =============================================================================
// DB Factory (Recommended Entry Point)
// =============================================================================

export {
  DB,
  type CollectionSchema,
  type DBSchema,
  type DBConfig,
  type DBInput,
  type DBInstance,
} from './db'

// Auto-configured db and sql (lazy initialization)
export { db, sql, initializeDB, getDB, resetDB } from './config/auto'

// Configuration utilities
export {
  defineConfig,
  detectRuntime,
  isServer,
  isWorkers,
  isBrowser,
  loadWorkersEnv,
  type ParqueDBConfig as AutoConfig,
  type Runtime,
} from './config'

// =============================================================================
// Main Classes
// =============================================================================

export {
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
 * @see src/Collection.ts for full documentation
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
  MemoryBackend,
  FsBackend,
  R2Backend,
  R2OperationError,
  R2ETagMismatchError,
  R2NotFoundError,
  // FsxBackend,
} from './storage'
export type { R2BackendOptions } from './storage'

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
  matchesFilter,
  createPredicate,
} from './query/filter'

export {
  applyUpdate,
} from './query/update'

export {
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
  executeAggregation,
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
  IndexManager,
  // Full-Text Search
  FTSIndex,
  InvertedIndex,
  BM25Scorer,
  tokenize,
  tokenizeQuery,
  porterStem,
  // Key Encoding (kept for backward compatibility)
  encodeKey,
  decodeKey,
  compareKeys,
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
// Embeddings (Workers AI)
// =============================================================================

export {
  // Core Embeddings
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL as DEFAULT_EMBEDDING_MODEL,
  DEFAULT_DIMENSIONS as DEFAULT_EMBEDDING_DIMENSIONS,
  EMBEDDING_MODELS,
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
  importFromJson,
  importFromJsonl,
  // CSV import
  importFromCsv,
  // MongoDB import
  importFromMongodb,
  importFromBson,
  // Utilities
  inferType,
  parseCsvLine,
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
  // Parquet
  DEFAULT_ROW_GROUP_SIZE,
  DEFAULT_PARQUET_PAGE_SIZE,
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
  createSQL,
  buildQuery,
  escapeIdentifier,
  escapeString,
  // Drizzle ORM
  createDrizzleProxy,
  // Prisma ORM
  createPrismaAdapter,
  PrismaParqueDBAdapter,
  // Parser & Translator
  parseSQL,
  translateStatement,
  translateSelect,
  translateInsert,
  translateUpdate,
  translateDelete,
  translateWhere,
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
  createBackend,
  // Iceberg Backend
  IcebergBackend,
  createIcebergBackend,
  createR2IcebergBackend,
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
} from './backends'

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
// Version
// =============================================================================

export const VERSION = '0.1.0'

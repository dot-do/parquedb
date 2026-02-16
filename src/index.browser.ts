/**
 * ParqueDB - A Parquet-based database for Node.js, browsers, and Cloudflare Workers
 *
 * Browser Build - Excludes oauth.do and native module dependencies.
 *
 * @packageDocumentation
 */

// =============================================================================
// DB Factory (Recommended Entry Point)
// =============================================================================

export {
  DB,
  type CollectionSchema,
  type CollectionOptions,
  type DBSchema,
  type DBConfig,
  type DBInput,
  type DBInstance,
  type DBSchemaInput,
  type TypedDBInstance,
  DEFAULT_COLLECTION_OPTIONS,
  extractCollectionOptions,
  getFieldsWithoutOptions,
  extractAllCollectionOptions,
} from './db'

// Auto-configured db and sql (lazy initialization) - browser version
export {
  db,
  sql,
  initializeDB,
  getDB,
  resetDB
} from './config/auto.browser'

// Configuration utilities - browser version
export {
  defineConfig,
  defineSchema,
  detectRuntime,
  isServer,
  isWorkers,
  isBrowser,
  loadWorkersEnv,
  type ParqueDBConfig as AutoConfig,
  type Runtime,
} from './config/index.browser'

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

export { Collection, clearGlobalStorage, getEventsForEntity, getEntityStateAtTime, clearEventLog } from './Collection'

// =============================================================================
// Types
// =============================================================================

export * from './types/index'

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
  DOSqliteBackend,
  getStorageCapabilities,
  hasStorageCapability,
  isStreamable,
  isMultipart,
  isTransactional,
} from './storage/index'
export type { R2BackendOptions, DOSqliteBackendOptions, StorageCapabilities } from './storage/index'

// =============================================================================
// Schema
// =============================================================================

export {
  parseSchema,
  parseFieldType,
  parseRelation,
  isRelationString,
  isValidFieldType,
  isValidRelationString,
  parseNestedField,
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  validateEntityCoreFields,
  validateEntityFull,
  SchemaValidationError,
  SchemaValidator,
  createValidator,
  validate,
  inferSchema,
  inferSchemaFromCollections,
  inferredToTypeDefinition,
} from './schema/index'

export type {
  ValidationMode,
  SchemaValidatorOptions,
} from './schema/index'

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
  executeAggregation,
  AggregationExecutor,
  type AggregationStage,
  type AggregationOptions,
  type AggregationExplain,
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
} from './aggregation/index'

// =============================================================================
// Indexes
// =============================================================================

export {
  IndexManager,
  FTSIndex,
  InvertedIndex,
  BM25Scorer,
  tokenize,
  tokenizeQuery,
  porterStem,
  encodeKey,
  decodeKey,
  compareKeys,
  hashKey,
  type IndexDefinition,
  type IndexMetadata,
  type IndexStats,
  type IndexLookupResult,
  type RangeQuery,
  type FTSSearchOptions,
  type FTSSearchResult,
} from './indexes/index'

// =============================================================================
// Events (CDC / Time-Travel)
// =============================================================================

export {
  type Event,
  type EventBatch,
  type EventSegment,
  type EventManifest,
  type EventWriterConfig,
  type DatasetConfig,
  type TimeTravelOptions,
  isRelationshipTarget,
  parseEntityTarget,
  parseRelTarget,
  entityTarget,
  relTarget,
} from './events/index'

// =============================================================================
// Embeddings (Workers AI & Vercel AI SDK)
// =============================================================================

export {
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL as DEFAULT_EMBEDDING_MODEL,
  DEFAULT_DIMENSIONS as DEFAULT_EMBEDDING_DIMENSIONS,
  EMBEDDING_MODELS,
  AISDKEmbeddings,
  createAISDKEmbeddings,
  getAISDKModelDimensions,
  listAISDKModels,
  AI_SDK_MODELS,
  DEFAULT_AI_SDK_DIMENSIONS,
  processEmbedOperator,
  autoEmbedFields,
  hasEmbedOperator,
  extractEmbedOperator,
  buildAutoEmbedConfig,
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
} from './embeddings/index'

// =============================================================================
// Observability (Hooks & Metrics)
// =============================================================================

export {
  HookRegistry,
  MetricsCollector,
  globalHookRegistry,
  generateOperationId,
  createQueryContext,
  createMutationContext,
  createStorageContext,
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
} from './observability/index'

// =============================================================================
// Migration Utilities
// =============================================================================

export {
  importFromJson,
  importFromJsonl,
  importFromCsv,
  importFromMongodb,
  importFromBson,
  inferType,
  parseCsvLine,
  convertBsonValue,
  type MigrationOptions,
  type JsonImportOptions,
  type CsvImportOptions,
  type BsonImportOptions,
  type MigrationResult,
  type MigrationError,
} from './migration/index'

// =============================================================================
// Error Handling
// =============================================================================

export {
  ParqueDBError,
  ErrorCode,
  ValidationError as ParqueDBValidationError,
  NotFoundError as ParqueDBNotFoundError,
  EntityNotFoundError as ParqueDBEntityNotFoundError,
  IndexNotFoundError as ParqueDBIndexNotFoundError,
  EventNotFoundError,
  SnapshotNotFoundError,
  FileNotFoundError as ParqueDBFileNotFoundError,
  ConflictError,
  VersionConflictError as ParqueDBVersionConflictError,
  AlreadyExistsError as ParqueDBAlreadyExistsError,
  ETagMismatchError as ParqueDBETagMismatchError,
  UniqueConstraintError as ParqueDBUniqueConstraintError,
  RelationshipError as ParqueDBRelationshipError,
  QueryError,
  InvalidFilterError,
  StorageError as ParqueDBStorageError,
  QuotaExceededError as ParqueDBQuotaExceededError,
  InvalidPathError as ParqueDBInvalidPathError,
  PathTraversalError as ParqueDBPathTraversalError,
  NetworkError as ParqueDBNetworkError,
  AuthorizationError,
  PermissionDeniedError as ParqueDBPermissionDeniedError,
  ConfigurationError,
  TimeoutError,
  RpcError as ParqueDBRpcError,
  IndexError,
  IndexBuildError as ParqueDBIndexBuildError,
  IndexLoadError as ParqueDBIndexLoadError,
  IndexAlreadyExistsError as ParqueDBIndexAlreadyExistsError,
  EventError as ParqueDBEventError,
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
  wrapError,
  errorFromStatus,
  assertValid,
  assertFound,
  type SerializedError,
} from './errors/index'

// =============================================================================
// Mutation Layer
// =============================================================================

export {
  MutationExecutor,
  VersionConflictError,
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
  executeCreate,
  executeUpdate,
  executeDelete,
  applyOperators,
  getField,
  setField,
  unsetField,
  validateUpdateOperators,
} from './mutation/index'

// =============================================================================
// Constants
// =============================================================================

export {
  DEFAULT_MAX_INBOUND,
  DEFAULT_PAGE_SIZE,
  MAX_BATCH_SIZE,
  DEFAULT_CONCURRENCY,
  MIN_PART_SIZE,
  DEFAULT_PART_SIZE,
  MAX_PARTS,
  DEFAULT_CACHE_TTL,
  MAX_CACHE_SIZE,
  DATA_CACHE_TTL_MS,
  DATA_CACHE_MAX_ENTRIES,
  DEFAULT_ROW_GROUP_SIZE,
  DEFAULT_PARQUET_PAGE_SIZE,
  DEFAULT_BLOOM_SIZE,
  DEFAULT_NUM_HASH_FUNCTIONS,
  ROW_GROUP_BLOOM_SIZE,
  DEFAULT_HNSW_M,
  DEFAULT_HNSW_EF_CONSTRUCTION,
  DEFAULT_HNSW_EF_SEARCH,
  DEFAULT_MAX_EVENTS,
  DEFAULT_MAX_EVENT_AGE,
  DEFAULT_EVENT_BUFFER_SIZE,
  DEFAULT_EVENT_BUFFER_BYTES,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_R2_THRESHOLD_BYTES,
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
  createSQL,
  buildQuery,
  escapeIdentifier,
  escapeString,
  createDrizzleProxy,
  createPrismaAdapter,
  PrismaParqueDBAdapter,
  parseSQL,
  translateStatement,
  translateSelect,
  translateInsert,
  translateUpdate,
  translateDelete,
  translateWhere,
  whereToFilter,
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
} from './integrations/sql/index'

// =============================================================================
// Entity Backends (Pluggable Storage Formats)
// =============================================================================

export {
  createBackend,
  IcebergBackend,
  createIcebergBackend,
  createR2IcebergBackend,
  getEntityBackendCapabilities,
  hasEntityBackendCapability,
  isCompatibleWithEngine,
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
} from './backends/index'

// =============================================================================
// Relationships (Batch Loading)
// =============================================================================

export {
  RelationshipBatchLoader,
  createBatchLoader,
  type BatchLoadRequest,
  type BatchLoadResult,
  type BatchLoaderOptions,
  type BatchLoaderDB,
} from './relationships/index'

// =============================================================================
// Integrations (Iceberg, etc.)
// =============================================================================

export {
  IcebergMetadataManager,
  IcebergStorageAdapter,
  createIcebergMetadataManager,
  enableIcebergMetadata,
  parqueDBTypeToIceberg,
  icebergTypeToParqueDB,
  type IcebergMetadataOptions,
  type IcebergSnapshotRef,
  type IcebergDataFile,
  type IcebergSchema,
  type IcebergField,
  type IcebergType,
  type IcebergCommitResult,
} from './integrations/iceberg'

export {
  NativeIcebergMetadataManager,
  NativeIcebergStorageAdapter,
  createNativeIcebergManager,
  enableNativeIcebergMetadata,
  type NativeIcebergOptions,
  type IcebergNativeSchema,
  type PartitionSpecDefinition,
  type SortOrderDefinition,
  type NativeDataFile,
  type NativeCommitResult,
} from './integrations/iceberg-native'

// =============================================================================
// Sync Module (Push/Pull/Sync)
// =============================================================================

export {
  createManifest,
  diffManifests,
  resolveConflicts,
  updateManifestFile,
  removeManifestFile,
  SyncEngine,
  createSyncEngine,
  resolveEventSync,
  computeEventSyncCursor,
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
  type EventSyncEvent,
  type EventSyncConflictStrategy,
  type EventSyncConflictInfo,
  type EventSyncResult,
  type ResolveEventSyncParams,
  type ResolveEventSyncResult,
} from './sync/index'

// =============================================================================
// Remote Client (Public Database Access)
// =============================================================================

export {
  openRemoteDB,
  checkRemoteDB,
  listPublicDatabases,
  type RemoteDB,
  type RemoteCollection,
  type RemoteDBInfo,
  type OpenRemoteDBOptions,
} from './client/remote'

// =============================================================================
// Visibility Types
// =============================================================================

export {
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
  csrf,
  csrfToken,
  validateCsrf,
  generateCsrfToken,
  verifyCsrfToken,
  buildSecureCorsHeaders,
  getAllowedOriginHeader,
  type CsrfOptions,
  type CsrfValidationResult,
  type CsrfTokenPayload,
} from './security/index'

// =============================================================================
// Search Client (Tree-Shakable)
// =============================================================================

export {
  search,
  vectorSearch,
  hybridSearch,
  suggest,
  createSearchClient,
  scoreEntity,
  searchEntities,
  type IMDBTitle,
  type ONETOccupation,
  type ScoreResult,
  type EntitySearchParams,
  type EntitySearchResult,
} from './search/index'

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'

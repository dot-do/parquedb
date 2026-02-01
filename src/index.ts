/**
 * ParqueDB - A Parquet-based database for Node.js, browsers, and Cloudflare Workers
 *
 * @packageDocumentation
 */

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
export { Collection } from './Collection'

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

// =============================================================================
// Indexes
// =============================================================================

export {
  // Index Manager
  IndexManager,
  // Secondary Indexes
  HashIndex,
  SSTIndex,
  // Full-Text Search
  FTSIndex,
  InvertedIndex,
  BM25Scorer,
  tokenize,
  tokenizeQuery,
  porterStem,
  // Key Encoding
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
// Version
// =============================================================================

export const VERSION = '0.1.0'

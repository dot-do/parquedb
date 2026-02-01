/**
 * ParqueDB Delta Lake Utilities
 *
 * Shared utilities between ParqueDB and Delta Lake for:
 * - Storage backend abstraction
 * - Parquet file handling (AsyncBuffer, variant encoding)
 * - MongoDB-style filters and zone map pruning
 * - Transaction log management
 * - Change Data Capture (CDC)
 * - Retry with exponential backoff
 *
 * This module provides a foundation for future integration between
 * ParqueDB and Delta Lake, enabling:
 * - Shared storage backends
 * - Consistent filter semantics
 * - Interoperable CDC records
 * - Common transaction log format
 *
 * @example
 * ```typescript
 * import {
 *   // Storage
 *   createAsyncBuffer,
 *   VersionMismatchError,
 *   isConditionalStorage,
 *
 *   // Filters
 *   matchesFilter,
 *   filterToZoneMapPredicates,
 *   canSkipZoneMap,
 *
 *   // Transaction log
 *   serializeCommit,
 *   parseCommit,
 *   formatVersion,
 *
 *   // CDC
 *   CDCProducer,
 *   CDCConsumer,
 *
 *   // Retry
 *   withRetry,
 *   isRetryableError,
 *
 *   // Variant
 *   encodeVariant,
 *   decodeVariant,
 * } from './delta-utils'
 * ```
 *
 * @module delta-utils
 */

// =============================================================================
// Storage
// =============================================================================

export {
  // Types
  type MinimalStorageBackend,
  type ConditionalStorageBackend,
  type FileStat,
  type AsyncBuffer,
  type StorageType,
  type S3Credentials,
  type R2BucketLike,

  // Errors
  VersionMismatchError,

  // Functions
  createAsyncBuffer,
  isConditionalStorage,

  // Path utilities
  normalizePath,
  joinPath,
  dirname,
  basename,
} from './storage.js'

// =============================================================================
// Filters
// =============================================================================

export {
  // Types
  type Filter,
  type ComparisonOperators,
  type LogicalOperators,
  type ZoneMap,
  type ZoneMapFilter,

  // Functions
  matchesFilter,
  getNestedValue,
  isComparisonObject,
  canSkipZoneMap,
  filterToZoneMapPredicates,
} from './filter.js'

// =============================================================================
// Transaction Log
// =============================================================================

export {
  // Types
  type BaseAction,
  type AddAction,
  type RemoveAction,
  type MetadataAction,
  type ProtocolAction,
  type CommitInfoAction,
  type LogAction,
  type Commit,
  type Snapshot,
  type FileStats,
  type ValidationResult,

  // Serialization
  serializeAction,
  parseAction,
  serializeCommit,
  parseCommit,

  // Version handling
  formatVersion,
  parseVersionFromFilename,
  getLogFilePath,
  getCheckpointPath,

  // Validation
  validateAction,

  // Type guards
  isAddAction,
  isRemoveAction,
  isMetadataAction,
  isProtocolAction,
  isCommitInfoAction,

  // Stats
  parseStats,
  encodeStats,

  // Action creation
  createAddAction,
  createRemoveAction,
} from './transaction-log.js'

// =============================================================================
// CDC (Change Data Capture)
// =============================================================================

export {
  // Types
  type CDCOperation,
  type CDCRecord,
  type CDCSource,
  type DeltaCDCChangeType,
  type DeltaCDCRecord,
  type CDCConfig,
  type CDCProducerOptions,
  type CDCConsumerOptions,

  // Classes
  CDCProducer,
  CDCConsumer,

  // Conversion utilities
  cdcOpToDeltaChangeType,
  deltaChangeTypeToCDCOp,
  cdcRecordToDeltaRecords,
  deltaCDCRecordToCDCRecord,
} from './cdc.js'

// =============================================================================
// Retry
// =============================================================================

export {
  // Types
  type RetryInfo,
  type SuccessInfo,
  type FailureInfo,
  type RetryMetrics,
  type RetryConfig,
  type RetryResultWithMetrics,

  // Constants
  DEFAULT_RETRY_CONFIG,

  // Classes
  AbortError,

  // Functions
  isRetryableError,
  withRetry,
} from './retry.js'

// =============================================================================
// Variant Encoding
// =============================================================================

export {
  // Types
  type VariantValue,
  type EncodedVariant,

  // Functions
  encodeVariant,
  decodeVariant,
  isEncodable,
  estimateVariantSize,
  variantEquals,
} from './variant.js'

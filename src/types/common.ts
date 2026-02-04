/**
 * Common Interfaces for ParqueDB
 *
 * This module provides properly typed interfaces to replace generic
 * `Record<string, unknown>` usage throughout the codebase.
 *
 * @module types/common
 */

// =============================================================================
// Metadata Types
// =============================================================================

/**
 * Generic metadata interface for extensible key-value data.
 *
 * Use this when the metadata structure is truly dynamic and user-defined.
 * For known structures, prefer more specific interfaces.
 *
 * @example
 * ```typescript
 * const metadata: Metadata = {
 *   requestId: 'req-123',
 *   correlationId: 'corr-456',
 *   source: 'webhook'
 * }
 * ```
 */
export interface Metadata {
  [key: string]: unknown
}

/**
 * Request metadata commonly attached to operations.
 *
 * Provides a typed structure for tracing and debugging information
 * that accompanies database operations.
 */
export interface RequestMetadata extends Metadata {
  /** Unique identifier for the HTTP request */
  requestId?: string | undefined
  /** Correlation ID for distributed tracing */
  correlationId?: string | undefined
  /** Source system or service name */
  source?: string | undefined
  /** User agent string */
  userAgent?: string | undefined
  /** IP address of the client */
  ipAddress?: string | undefined
}

/**
 * Event metadata attached to CDC events.
 *
 * Contains contextual information about what triggered the change.
 */
export interface EventMetadata extends RequestMetadata {
  /** Reason for the operation (e.g., 'user-action', 'system-cleanup') */
  reason?: string | undefined
  /** Whether this was part of a batch operation */
  isBatch?: boolean | undefined
  /** Batch operation ID if part of a batch */
  batchId?: string | undefined
  /** Transaction ID if part of a transaction */
  transactionId?: string | undefined
}

/**
 * Connection metadata for subscription connections.
 *
 * Information about the client connection for subscriptions.
 */
export interface ConnectionMetadata extends Metadata {
  /** Client identifier */
  clientId?: string | undefined
  /** User ID if authenticated */
  userId?: string | undefined
  /** Session ID */
  sessionId?: string | undefined
  /** Connection source (e.g., 'websocket', 'sse') */
  transport?: 'websocket' | 'sse' | string | undefined
  /** Geographic location/colo */
  colo?: string | undefined
}

/**
 * Index metadata for custom index information.
 *
 * Additional information stored with index entries.
 */
export interface IndexMetadataData extends Metadata {
  /** Score for ranked indexes (e.g., FTS BM25 score) */
  score?: number | undefined
  /** Frequency count for term indexes */
  frequency?: number | undefined
  /** Boost factor applied */
  boost?: number | undefined
}

// =============================================================================
// Document State Types
// =============================================================================

/**
 * Entity state for before/after snapshots in mutations and events.
 *
 * Represents the complete state of an entity at a point in time,
 * including system fields and user data.
 */
export interface EntityState {
  /** Entity ID within namespace */
  $id?: string | undefined
  /** Entity type */
  $type?: string | undefined
  /** Display name */
  name?: string | undefined
  /** Version number */
  version?: number | undefined
  /** Creation timestamp */
  createdAt?: Date | string | number | undefined
  /** Creator ID */
  createdBy?: string | undefined
  /** Last update timestamp */
  updatedAt?: Date | string | number | undefined
  /** Last updater ID */
  updatedBy?: string | undefined
  /** Deletion timestamp (soft delete) */
  deletedAt?: Date | string | number | undefined
  /** Deleter ID */
  deletedBy?: string | undefined
  /** Additional user-defined fields */
  [key: string]: unknown
}

// =============================================================================
// Filter and Operator Types
// =============================================================================

/**
 * Filter condition with MongoDB-style operators.
 *
 * Used for query filters and condition matching.
 */
export interface FilterCondition {
  /** Equality match */
  $eq?: unknown | undefined
  /** Not equal */
  $ne?: unknown | undefined
  /** Greater than */
  $gt?: unknown | undefined
  /** Greater than or equal */
  $gte?: unknown | undefined
  /** Less than */
  $lt?: unknown | undefined
  /** Less than or equal */
  $lte?: unknown | undefined
  /** In array */
  $in?: unknown[] | undefined
  /** Not in array */
  $nin?: unknown[] | undefined
  /** Exists check */
  $exists?: boolean | undefined
  /** Type check */
  $type?: string | undefined
  /** Regex match */
  $regex?: string | RegExp | undefined
  /** Element match for arrays */
  $elemMatch?: FilterCondition | undefined
  /** Array size */
  $size?: number | undefined
  /** All elements in array */
  $all?: unknown[] | undefined
  /** Additional operators */
  [key: string]: unknown
}

/**
 * Comparison operators for numeric/date comparisons.
 *
 * Subset of FilterCondition for range queries.
 */
export interface ComparisonOperators {
  $gt?: number | Date | undefined
  $gte?: number | Date | undefined
  $lt?: number | Date | undefined
  $lte?: number | Date | undefined
  $eq?: number | Date | undefined
  $ne?: number | Date | undefined
}

// =============================================================================
// Writer Options Types
// =============================================================================

/**
 * Options for Parquet write operations.
 *
 * Configuration passed to hyparquet-writer.
 */
export interface ParquetWriteOptions {
  /** Compression codec */
  compression?: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'BROTLI' | 'ZSTD' | 'LZ4' | undefined
  /** Row group size */
  rowGroupSize?: number | undefined
  /** Page size */
  pageSize?: number | undefined
  /** Write statistics */
  statistics?: boolean | undefined
  /** Enable dictionary encoding */
  dictionary?: boolean | undefined
  /** Additional options */
  [key: string]: unknown
}

// =============================================================================
// Edge/Relationship Data Types
// =============================================================================

/**
 * Edge properties for relationship metadata.
 *
 * Additional data stored on relationship edges.
 */
export interface EdgeData {
  /** Weight or strength of the relationship */
  weight?: number | undefined
  /** Relationship label/type qualifier */
  label?: string | undefined
  /** Timestamp for temporal relationships */
  timestamp?: Date | string | number | undefined
  /** Order/rank for ordered relationships */
  order?: number | undefined
  /** Confidence score for inferred relationships */
  confidence?: number | undefined
  /** Additional custom properties */
  [key: string]: unknown
}

// =============================================================================
// API Response Types
// =============================================================================

/**
 * Error response structure for API errors.
 */
export interface ErrorResponse {
  /** Error code */
  code: string
  /** Human-readable message */
  message: string
  /** Additional error details */
  details?: Metadata | undefined
  /** Stack trace (development only) */
  stack?: string | undefined
}

/**
 * Generic API result wrapper.
 */
export interface ApiResult<T> {
  /** Whether the operation succeeded */
  ok: boolean
  /** Result data on success */
  data?: T | undefined
  /** Error information on failure */
  error?: ErrorResponse | undefined
}

/**
 * ParqueDB Error Handling Module
 *
 * Provides a standardized error hierarchy for the entire ParqueDB codebase.
 * All errors extend from ParqueDBError which provides:
 * - Error codes for programmatic handling
 * - Serialization support for RPC
 * - Cause chaining for debugging
 * - Type guards for error checking
 *
 * Error Hierarchy:
 * - ParqueDBError (base class)
 *   - ValidationError (input/schema validation failures)
 *   - NotFoundError (entity/resource not found)
 *   - ConflictError (version conflicts, duplicates)
 *   - RelationshipError (relationship operation failures)
 *   - QueryError (query execution failures)
 *   - StorageError (storage backend failures)
 *   - AuthorizationError (permission/access denied)
 *   - ConfigurationError (invalid configuration)
 *   - TimeoutError (operation timeout)
 *   - RpcError (RPC communication failures)
 *
 * @module errors
 */

// =============================================================================
// Error Codes
// =============================================================================

/**
 * Error codes for ParqueDB operations.
 * These codes are stable and can be used for programmatic error handling.
 */
export enum ErrorCode {
  // General errors (1xxx)
  UNKNOWN = 'UNKNOWN',
  INTERNAL = 'INTERNAL',
  TIMEOUT = 'TIMEOUT',
  CANCELLED = 'CANCELLED',

  // Validation errors (2xxx)
  VALIDATION_FAILED = 'VALIDATION_FAILED',
  INVALID_INPUT = 'INVALID_INPUT',
  INVALID_TYPE = 'INVALID_TYPE',
  REQUIRED_FIELD = 'REQUIRED_FIELD',
  INVALID_FORMAT = 'INVALID_FORMAT',
  SCHEMA_MISMATCH = 'SCHEMA_MISMATCH',
  INVALID_OPERATOR = 'INVALID_OPERATOR',

  // Not found errors (3xxx)
  NOT_FOUND = 'NOT_FOUND',
  ENTITY_NOT_FOUND = 'ENTITY_NOT_FOUND',
  COLLECTION_NOT_FOUND = 'COLLECTION_NOT_FOUND',
  INDEX_NOT_FOUND = 'INDEX_NOT_FOUND',
  EVENT_NOT_FOUND = 'EVENT_NOT_FOUND',
  SNAPSHOT_NOT_FOUND = 'SNAPSHOT_NOT_FOUND',
  FILE_NOT_FOUND = 'FILE_NOT_FOUND',
  DIRECTORY_NOT_FOUND = 'DIRECTORY_NOT_FOUND',

  // Conflict errors (4xxx)
  CONFLICT = 'CONFLICT',
  VERSION_CONFLICT = 'VERSION_CONFLICT',
  ALREADY_EXISTS = 'ALREADY_EXISTS',
  ETAG_MISMATCH = 'ETAG_MISMATCH',
  UNIQUE_CONSTRAINT = 'UNIQUE_CONSTRAINT',
  CONCURRENT_MODIFICATION = 'CONCURRENT_MODIFICATION',

  // Relationship errors (5xxx)
  RELATIONSHIP_ERROR = 'RELATIONSHIP_ERROR',
  INVALID_RELATIONSHIP = 'INVALID_RELATIONSHIP',
  RELATIONSHIP_NOT_FOUND = 'RELATIONSHIP_NOT_FOUND',
  CIRCULAR_RELATIONSHIP = 'CIRCULAR_RELATIONSHIP',
  RELATIONSHIP_TARGET_DELETED = 'RELATIONSHIP_TARGET_DELETED',

  // Query errors (6xxx)
  QUERY_ERROR = 'QUERY_ERROR',
  INVALID_FILTER = 'INVALID_FILTER',
  INVALID_SORT = 'INVALID_SORT',
  INVALID_PROJECTION = 'INVALID_PROJECTION',
  QUERY_TIMEOUT = 'QUERY_TIMEOUT',

  // Storage errors (7xxx)
  STORAGE_ERROR = 'STORAGE_ERROR',
  STORAGE_READ_ERROR = 'STORAGE_READ_ERROR',
  STORAGE_WRITE_ERROR = 'STORAGE_WRITE_ERROR',
  QUOTA_EXCEEDED = 'QUOTA_EXCEEDED',
  INVALID_PATH = 'INVALID_PATH',
  PATH_TRAVERSAL = 'PATH_TRAVERSAL',
  DIRECTORY_NOT_EMPTY = 'DIRECTORY_NOT_EMPTY',
  NETWORK_ERROR = 'NETWORK_ERROR',

  // Authorization errors (8xxx)
  AUTHORIZATION_ERROR = 'AUTHORIZATION_ERROR',
  PERMISSION_DENIED = 'PERMISSION_DENIED',
  AUTHENTICATION_REQUIRED = 'AUTHENTICATION_REQUIRED',
  TOKEN_EXPIRED = 'TOKEN_EXPIRED',

  // Configuration errors (9xxx)
  CONFIGURATION_ERROR = 'CONFIGURATION_ERROR',
  INVALID_CONFIG = 'INVALID_CONFIG',
  MISSING_CONFIG = 'MISSING_CONFIG',

  // RPC errors (10xxx)
  RPC_ERROR = 'RPC_ERROR',
  RPC_TIMEOUT = 'RPC_TIMEOUT',
  RPC_UNAVAILABLE = 'RPC_UNAVAILABLE',
  RPC_INVALID_RESPONSE = 'RPC_INVALID_RESPONSE',

  // Index errors (11xxx)
  INDEX_ERROR = 'INDEX_ERROR',
  INDEX_BUILD_ERROR = 'INDEX_BUILD_ERROR',
  INDEX_LOAD_ERROR = 'INDEX_LOAD_ERROR',
  INDEX_CATALOG_ERROR = 'INDEX_CATALOG_ERROR',
  INDEX_ALREADY_EXISTS = 'INDEX_ALREADY_EXISTS',
  INDEX_VALIDATION_ERROR = 'INDEX_VALIDATION_ERROR',

  // Event/Snapshot errors (12xxx)
  EVENT_ERROR = 'EVENT_ERROR',
  SNAPSHOT_ERROR = 'SNAPSHOT_ERROR',
  REPLAY_ERROR = 'REPLAY_ERROR',
}

// =============================================================================
// Serialized Error Format
// =============================================================================

/**
 * Serializable error format for RPC transmission
 */
export interface SerializedError {
  /** Error class name */
  name: string
  /** Error code for programmatic handling */
  code: ErrorCode
  /** Human-readable error message */
  message: string
  /** Stack trace (included in development mode) */
  stack?: string
  /** Additional context data */
  context?: Record<string, unknown>
  /** Serialized cause (if error chaining) */
  cause?: SerializedError
}

// =============================================================================
// Base Error Class
// =============================================================================

/**
 * Base error class for all ParqueDB errors.
 *
 * Provides:
 * - Error code for programmatic handling
 * - Context data for debugging
 * - Serialization for RPC
 * - Cause chaining
 *
 * @example
 * ```typescript
 * throw new ParqueDBError('Operation failed', ErrorCode.INTERNAL, {
 *   operation: 'create',
 *   namespace: 'users'
 * })
 * ```
 */
export class ParqueDBError extends Error {
  override readonly name: string = 'ParqueDBError'
  readonly code: ErrorCode
  readonly context: Record<string, unknown>
  override readonly cause?: Error

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.UNKNOWN,
    context?: Record<string, unknown>,
    cause?: Error
  ) {
    super(message)
    this.code = code
    this.context = context ?? {}
    this.cause = cause
    Object.setPrototypeOf(this, new.target.prototype)
  }

  /**
   * Serialize error for RPC transmission
   */
  toJSON(): SerializedError {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      stack: process.env.NODE_ENV !== 'production' ? this.stack : undefined,
      context: Object.keys(this.context).length > 0 ? this.context : undefined,
      cause: this.cause instanceof ParqueDBError ? this.cause.toJSON() : undefined,
    }
  }

  /**
   * Create error from serialized format
   */
  static fromJSON(data: SerializedError): ParqueDBError {
    const cause = data.cause ? ParqueDBError.fromJSON(data.cause) : undefined
    const error = new ParqueDBError(data.message, data.code, data.context, cause)
    if (data.stack) {
      error.stack = data.stack
    }
    return error
  }

  /**
   * Check if error matches a specific code
   */
  is(code: ErrorCode): boolean {
    return this.code === code
  }

  /**
   * Check if error is in a category (e.g., all NOT_FOUND variants)
   */
  isCategory(category: string): boolean {
    return this.code.includes(category)
  }
}

// =============================================================================
// Validation Errors
// =============================================================================

/**
 * Error thrown when input validation fails.
 *
 * Used for:
 * - Schema validation failures
 * - Invalid field types
 * - Missing required fields
 * - Invalid formats
 */
export class ValidationError extends ParqueDBError {
  override readonly name = 'ValidationError'

  constructor(
    message: string,
    context?: {
      field?: string
      expectedType?: string
      actualType?: string
      namespace?: string
      operation?: string
      value?: unknown
    },
    cause?: Error
  ) {
    const code = context?.field
      ? context.expectedType
        ? ErrorCode.INVALID_TYPE
        : ErrorCode.REQUIRED_FIELD
      : ErrorCode.VALIDATION_FAILED

    super(message, code, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, ValidationError.prototype)
  }

  /** Field that failed validation */
  get field(): string | undefined {
    return this.context.field as string | undefined
  }

  /** Expected type */
  get expectedType(): string | undefined {
    return this.context.expectedType as string | undefined
  }

  /** Actual type received */
  get actualType(): string | undefined {
    return this.context.actualType as string | undefined
  }

  /** Namespace where validation failed */
  get namespace(): string | undefined {
    return this.context.namespace as string | undefined
  }
}

// =============================================================================
// Not Found Errors
// =============================================================================

/**
 * Error thrown when a requested resource is not found.
 */
export class NotFoundError extends ParqueDBError {
  override readonly name = 'NotFoundError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.NOT_FOUND,
    context?: Record<string, unknown>,
    cause?: Error
  ) {
    super(message, code, context, cause)
    Object.setPrototypeOf(this, NotFoundError.prototype)
  }
}

/**
 * Error thrown when an entity is not found.
 */
export class EntityNotFoundError extends NotFoundError {
  override readonly name = 'EntityNotFoundError'

  constructor(
    namespace: string,
    entityId: string,
    cause?: Error
  ) {
    super(
      `Entity not found: ${namespace}/${entityId}`,
      ErrorCode.ENTITY_NOT_FOUND,
      { namespace, entityId },
      cause
    )
    Object.setPrototypeOf(this, EntityNotFoundError.prototype)
  }

  get namespace(): string {
    return this.context.namespace as string
  }

  get entityId(): string {
    return this.context.entityId as string
  }
}

/**
 * Error thrown when an index is not found.
 */
export class IndexNotFoundError extends NotFoundError {
  override readonly name = 'IndexNotFoundError'

  constructor(
    indexName: string,
    namespace: string,
    cause?: Error
  ) {
    super(
      `Index "${indexName}" not found in namespace "${namespace}"`,
      ErrorCode.INDEX_NOT_FOUND,
      { indexName, namespace },
      cause
    )
    Object.setPrototypeOf(this, IndexNotFoundError.prototype)
  }

  get indexName(): string {
    return this.context.indexName as string
  }

  get namespace(): string {
    return this.context.namespace as string
  }
}

/**
 * Error thrown when an event is not found.
 */
export class EventNotFoundError extends NotFoundError {
  override readonly name = 'EventNotFoundError'

  constructor(eventId: string, cause?: Error) {
    super(
      `Event not found: ${eventId}`,
      ErrorCode.EVENT_NOT_FOUND,
      { eventId },
      cause
    )
    Object.setPrototypeOf(this, EventNotFoundError.prototype)
  }

  get eventId(): string {
    return this.context.eventId as string
  }
}

/**
 * Error thrown when a snapshot is not found.
 */
export class SnapshotNotFoundError extends NotFoundError {
  override readonly name = 'SnapshotNotFoundError'

  constructor(snapshotId: string, cause?: Error) {
    super(
      `Snapshot not found: ${snapshotId}`,
      ErrorCode.SNAPSHOT_NOT_FOUND,
      { snapshotId },
      cause
    )
    Object.setPrototypeOf(this, SnapshotNotFoundError.prototype)
  }

  get snapshotId(): string {
    return this.context.snapshotId as string
  }
}

/**
 * Error thrown when a file is not found in storage.
 */
export class FileNotFoundError extends NotFoundError {
  override readonly name = 'FileNotFoundError'

  constructor(path: string, cause?: Error) {
    super(
      `File not found: ${path}`,
      ErrorCode.FILE_NOT_FOUND,
      { path },
      cause
    )
    Object.setPrototypeOf(this, FileNotFoundError.prototype)
  }

  get path(): string {
    return this.context.path as string
  }
}

// =============================================================================
// Conflict Errors
// =============================================================================

/**
 * Error thrown when a conflict occurs (version, duplicate, etc.)
 */
export class ConflictError extends ParqueDBError {
  override readonly name = 'ConflictError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.CONFLICT,
    context?: Record<string, unknown>,
    cause?: Error
  ) {
    super(message, code, context, cause)
    Object.setPrototypeOf(this, ConflictError.prototype)
  }
}

/**
 * Error thrown when optimistic concurrency check fails.
 */
export class VersionConflictError extends ConflictError {
  override readonly name = 'VersionConflictError'

  constructor(
    expectedVersion: number,
    actualVersion: number | undefined,
    context?: { namespace?: string; entityId?: string }
  ) {
    const entityPath = context?.namespace && context?.entityId
      ? ` for ${context.namespace}/${context.entityId}`
      : context?.entityId
        ? ` for entity ${context.entityId}`
        : ''

    super(
      `Version conflict: expected ${expectedVersion}, got ${actualVersion}${entityPath}`,
      ErrorCode.VERSION_CONFLICT,
      {
        expectedVersion,
        actualVersion,
        namespace: context?.namespace,
        entityId: context?.entityId,
      }
    )
    Object.setPrototypeOf(this, VersionConflictError.prototype)
  }

  get expectedVersion(): number {
    return this.context.expectedVersion as number
  }

  get actualVersion(): number | undefined {
    return this.context.actualVersion as number | undefined
  }

  get namespace(): string | undefined {
    return this.context.namespace as string | undefined
  }

  get entityId(): string | undefined {
    return this.context.entityId as string | undefined
  }
}

/**
 * Error thrown when a resource already exists.
 */
export class AlreadyExistsError extends ConflictError {
  override readonly name = 'AlreadyExistsError'

  constructor(
    resource: string,
    identifier: string,
    cause?: Error
  ) {
    super(
      `${resource} already exists: ${identifier}`,
      ErrorCode.ALREADY_EXISTS,
      { resource, identifier },
      cause
    )
    Object.setPrototypeOf(this, AlreadyExistsError.prototype)
  }

  get resource(): string {
    return this.context.resource as string
  }

  get identifier(): string {
    return this.context.identifier as string
  }
}

/**
 * Error thrown when ETag/version mismatch occurs for conditional operations.
 */
export class ETagMismatchError extends ConflictError {
  override readonly name = 'ETagMismatchError'

  constructor(
    path: string,
    expectedEtag: string | null,
    actualEtag: string | null,
    cause?: Error
  ) {
    super(
      `ETag mismatch for ${path}: expected ${expectedEtag}, got ${actualEtag}`,
      ErrorCode.ETAG_MISMATCH,
      { path, expectedEtag, actualEtag },
      cause
    )
    Object.setPrototypeOf(this, ETagMismatchError.prototype)
  }

  get path(): string {
    return this.context.path as string
  }

  get expectedEtag(): string | null {
    return this.context.expectedEtag as string | null
  }

  get actualEtag(): string | null {
    return this.context.actualEtag as string | null
  }
}

/**
 * Error thrown when a unique constraint is violated.
 */
export class UniqueConstraintError extends ConflictError {
  override readonly name = 'UniqueConstraintError'

  constructor(
    indexName: string,
    value: unknown,
    namespace?: string
  ) {
    const valueStr = typeof value === 'string' ? value : JSON.stringify(value)
    super(
      `Unique constraint violation on index "${indexName}": duplicate value "${valueStr}"`,
      ErrorCode.UNIQUE_CONSTRAINT,
      { indexName, value, namespace }
    )
    Object.setPrototypeOf(this, UniqueConstraintError.prototype)
  }

  get indexName(): string {
    return this.context.indexName as string
  }

  get value(): unknown {
    return this.context.value
  }

  get namespace(): string | undefined {
    return this.context.namespace as string | undefined
  }
}

// =============================================================================
// Relationship Errors
// =============================================================================

/**
 * Error thrown when a relationship operation fails.
 */
export class RelationshipError extends ParqueDBError {
  override readonly name = 'RelationshipError'

  constructor(
    operation: string,
    namespace: string,
    message: string,
    context?: {
      entityId?: string
      relationshipName?: string
      targetId?: string
    },
    cause?: Error
  ) {
    const entityMsg = context?.entityId ? ` ${namespace}/${context.entityId}` : ` '${namespace}'`
    const relMsg = context?.relationshipName ? ` relationship '${context.relationshipName}'` : ''
    const targetMsg = context?.targetId ? ` with target ${context.targetId}` : ''

    super(
      `${operation} failed for${entityMsg}${relMsg}${targetMsg}: ${message}`,
      ErrorCode.RELATIONSHIP_ERROR,
      {
        operation,
        namespace,
        entityId: context?.entityId,
        relationshipName: context?.relationshipName,
        targetId: context?.targetId,
      },
      cause
    )
    Object.setPrototypeOf(this, RelationshipError.prototype)
  }

  get operation(): string {
    return this.context.operation as string
  }

  get namespace(): string {
    return this.context.namespace as string
  }

  get entityId(): string | undefined {
    return this.context.entityId as string | undefined
  }

  get relationshipName(): string | undefined {
    return this.context.relationshipName as string | undefined
  }

  get targetId(): string | undefined {
    return this.context.targetId as string | undefined
  }
}

// =============================================================================
// Query Errors
// =============================================================================

/**
 * Error thrown when a query operation fails.
 */
export class QueryError extends ParqueDBError {
  override readonly name = 'QueryError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.QUERY_ERROR,
    context?: {
      namespace?: string
      filter?: unknown
      sort?: unknown
      projection?: unknown
    },
    cause?: Error
  ) {
    super(message, code, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, QueryError.prototype)
  }

  get namespace(): string | undefined {
    return this.context.namespace as string | undefined
  }
}

/**
 * Error thrown for invalid filter operators or syntax.
 */
export class InvalidFilterError extends QueryError {
  override readonly name = 'InvalidFilterError'

  constructor(
    message: string,
    filter?: unknown,
    cause?: Error
  ) {
    super(message, ErrorCode.INVALID_FILTER, { filter }, cause)
    Object.setPrototypeOf(this, InvalidFilterError.prototype)
  }

  get filter(): unknown {
    return this.context.filter
  }
}

// =============================================================================
// Storage Errors
// =============================================================================

/**
 * Error thrown when a storage operation fails.
 */
export class StorageError extends ParqueDBError {
  override readonly name = 'StorageError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.STORAGE_ERROR,
    context?: {
      path?: string
      operation?: string
    },
    cause?: Error
  ) {
    super(message, code, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, StorageError.prototype)
  }

  get path(): string | undefined {
    return this.context.path as string | undefined
  }

  get operation(): string | undefined {
    return this.context.operation as string | undefined
  }
}

/**
 * Error thrown when storage quota is exceeded.
 */
export class QuotaExceededError extends StorageError {
  override readonly name = 'QuotaExceededError'

  constructor(
    path: string,
    quotaBytes?: number,
    usedBytes?: number,
    cause?: Error
  ) {
    let message = `Quota exceeded for ${path}`
    if (quotaBytes !== undefined && usedBytes !== undefined) {
      message += ` (used ${usedBytes} of ${quotaBytes} bytes)`
    }
    super(message, ErrorCode.QUOTA_EXCEEDED, { path, quotaBytes, usedBytes }, cause)
    Object.setPrototypeOf(this, QuotaExceededError.prototype)
  }

  get quotaBytes(): number | undefined {
    return this.context.quotaBytes as number | undefined
  }

  get usedBytes(): number | undefined {
    return this.context.usedBytes as number | undefined
  }
}

/**
 * Error thrown when a path is invalid.
 */
export class InvalidPathError extends StorageError {
  override readonly name = 'InvalidPathError'

  constructor(path: string, reason?: string, cause?: Error) {
    const reasonPart = reason ? `: ${reason}` : ''
    super(`Invalid path: ${path}${reasonPart}`, ErrorCode.INVALID_PATH, { path, reason }, cause)
    Object.setPrototypeOf(this, InvalidPathError.prototype)
  }

  get reason(): string | undefined {
    return this.context.reason as string | undefined
  }
}

/**
 * Error thrown when a path traversal attempt is detected.
 */
export class PathTraversalError extends StorageError {
  override readonly name = 'PathTraversalError'

  constructor(path: string, cause?: Error) {
    super(
      `Path traversal attempt detected: ${path}`,
      ErrorCode.PATH_TRAVERSAL,
      { path },
      cause
    )
    Object.setPrototypeOf(this, PathTraversalError.prototype)
  }
}

/**
 * Error thrown for network-related failures.
 */
export class NetworkError extends StorageError {
  override readonly name = 'NetworkError'

  constructor(message: string, path?: string, cause?: Error) {
    super(message, ErrorCode.NETWORK_ERROR, { path }, cause)
    Object.setPrototypeOf(this, NetworkError.prototype)
  }
}

// =============================================================================
// Authorization Errors
// =============================================================================

/**
 * Error thrown when authorization fails.
 */
export class AuthorizationError extends ParqueDBError {
  override readonly name = 'AuthorizationError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.AUTHORIZATION_ERROR,
    context?: {
      resource?: string
      action?: string
      actor?: string
    },
    cause?: Error
  ) {
    super(message, code, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, AuthorizationError.prototype)
  }
}

/**
 * Error thrown when permission is denied.
 */
export class PermissionDeniedError extends AuthorizationError {
  override readonly name = 'PermissionDeniedError'

  constructor(
    resource: string,
    action?: string,
    cause?: Error
  ) {
    const actionPart = action ? ` (${action})` : ''
    super(
      `Permission denied: ${resource}${actionPart}`,
      ErrorCode.PERMISSION_DENIED,
      { resource, action },
      cause
    )
    Object.setPrototypeOf(this, PermissionDeniedError.prototype)
  }

  get resource(): string {
    return this.context.resource as string
  }

  get action(): string | undefined {
    return this.context.action as string | undefined
  }
}

// =============================================================================
// Configuration Errors
// =============================================================================

/**
 * Error thrown when configuration is invalid.
 */
export class ConfigurationError extends ParqueDBError {
  override readonly name = 'ConfigurationError'

  constructor(
    message: string,
    context?: {
      configKey?: string
      expectedValue?: unknown
      actualValue?: unknown
    },
    cause?: Error
  ) {
    super(message, ErrorCode.CONFIGURATION_ERROR, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, ConfigurationError.prototype)
  }
}

// =============================================================================
// Timeout Error
// =============================================================================

/**
 * Error thrown when an operation times out.
 */
export class TimeoutError extends ParqueDBError {
  override readonly name = 'TimeoutError'

  constructor(
    operation: string,
    timeoutMs: number,
    cause?: Error
  ) {
    super(
      `Operation "${operation}" timed out after ${timeoutMs}ms`,
      ErrorCode.TIMEOUT,
      { operation, timeoutMs },
      cause
    )
    Object.setPrototypeOf(this, TimeoutError.prototype)
  }

  get operation(): string {
    return this.context.operation as string
  }

  get timeoutMs(): number {
    return this.context.timeoutMs as number
  }
}

// =============================================================================
// RPC Errors
// =============================================================================

/**
 * Error thrown when RPC communication fails.
 */
export class RpcError extends ParqueDBError {
  override readonly name = 'RpcError'

  constructor(
    message: string,
    status: number,
    context?: {
      method?: string
      endpoint?: string
      requestId?: string
    },
    cause?: Error
  ) {
    super(message, ErrorCode.RPC_ERROR, { status, ...context } as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, RpcError.prototype)
  }

  get status(): number {
    return this.context.status as number
  }

  get method(): string | undefined {
    return this.context.method as string | undefined
  }

  get endpoint(): string | undefined {
    return this.context.endpoint as string | undefined
  }
}

// =============================================================================
// Index Errors
// =============================================================================

/**
 * Error thrown when an index operation fails.
 */
export class IndexError extends ParqueDBError {
  override readonly name = 'IndexError'

  constructor(
    message: string,
    code: ErrorCode = ErrorCode.INDEX_ERROR,
    context?: {
      indexName?: string
      namespace?: string
    },
    cause?: Error
  ) {
    super(message, code, context as Record<string, unknown>, cause)
    Object.setPrototypeOf(this, IndexError.prototype)
  }

  get indexName(): string | undefined {
    return this.context.indexName as string | undefined
  }

  get namespace(): string | undefined {
    return this.context.namespace as string | undefined
  }
}

/**
 * Error thrown when an index build fails.
 */
export class IndexBuildError extends IndexError {
  override readonly name = 'IndexBuildError'

  constructor(indexName: string, cause: Error) {
    super(
      `Failed to build index "${indexName}": ${cause.message}`,
      ErrorCode.INDEX_BUILD_ERROR,
      { indexName },
      cause
    )
    Object.setPrototypeOf(this, IndexBuildError.prototype)
  }
}

/**
 * Error thrown when an index fails to load.
 */
export class IndexLoadError extends IndexError {
  override readonly name = 'IndexLoadError'

  constructor(indexName: string, path: string, cause: Error) {
    super(
      `Failed to load index "${indexName}" from "${path}": ${cause.message}`,
      ErrorCode.INDEX_LOAD_ERROR,
      { indexName, path },
      cause
    )
    Object.setPrototypeOf(this, IndexLoadError.prototype)
  }

  get path(): string {
    return this.context.path as string
  }
}

/**
 * Error thrown when an index already exists.
 */
export class IndexAlreadyExistsError extends IndexError {
  override readonly name = 'IndexAlreadyExistsError'

  constructor(indexName: string, namespace: string) {
    super(
      `Index "${indexName}" already exists in namespace "${namespace}"`,
      ErrorCode.INDEX_ALREADY_EXISTS,
      { indexName, namespace }
    )
    Object.setPrototypeOf(this, IndexAlreadyExistsError.prototype)
  }
}

// =============================================================================
// Event/Snapshot Errors
// =============================================================================

/**
 * Error thrown when an event operation fails.
 */
export class EventError extends ParqueDBError {
  override readonly name = 'EventError'

  constructor(
    operation: string,
    message: string,
    context?: {
      eventId?: string
      snapshotId?: string
      entityId?: string
    },
    cause?: Error
  ) {
    const contextMsg = context?.eventId
      ? ` event ${context.eventId}`
      : context?.snapshotId
        ? ` snapshot ${context.snapshotId}`
        : context?.entityId
          ? ` for entity ${context.entityId}`
          : ''

    super(
      `${operation} failed${contextMsg}: ${message}`,
      ErrorCode.EVENT_ERROR,
      { operation, ...context },
      cause
    )
    Object.setPrototypeOf(this, EventError.prototype)
  }

  get operation(): string {
    return this.context.operation as string
  }

  get eventId(): string | undefined {
    return this.context.eventId as string | undefined
  }

  get snapshotId(): string | undefined {
    return this.context.snapshotId as string | undefined
  }

  get entityId(): string | undefined {
    return this.context.entityId as string | undefined
  }
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if an error is a ParqueDBError
 */
export function isParqueDBError(error: unknown): error is ParqueDBError {
  return error instanceof ParqueDBError
}

/**
 * Check if an error is a ValidationError
 */
export function isValidationError(error: unknown): error is ValidationError {
  return error instanceof ValidationError
}

/**
 * Check if an error is a NotFoundError (or any subclass)
 */
export function isNotFoundError(error: unknown): error is NotFoundError {
  return error instanceof NotFoundError ||
    (isParqueDBError(error) && error.code.includes('NOT_FOUND'))
}

/**
 * Check if an error is an EntityNotFoundError
 */
export function isEntityNotFoundError(error: unknown): error is EntityNotFoundError {
  return error instanceof EntityNotFoundError
}

/**
 * Check if an error is a ConflictError (or any subclass)
 */
export function isConflictError(error: unknown): error is ConflictError {
  return error instanceof ConflictError ||
    (isParqueDBError(error) && (
      error.code === ErrorCode.CONFLICT ||
      error.code === ErrorCode.VERSION_CONFLICT ||
      error.code === ErrorCode.ALREADY_EXISTS ||
      error.code === ErrorCode.ETAG_MISMATCH ||
      error.code === ErrorCode.UNIQUE_CONSTRAINT
    ))
}

/**
 * Check if an error is a VersionConflictError
 */
export function isVersionConflictError(error: unknown): error is VersionConflictError {
  return error instanceof VersionConflictError
}

/**
 * Check if an error is an ETagMismatchError
 */
export function isETagMismatchError(error: unknown): error is ETagMismatchError {
  return error instanceof ETagMismatchError ||
    (isParqueDBError(error) && error.code === ErrorCode.ETAG_MISMATCH)
}

/**
 * Check if an error is an AlreadyExistsError
 */
export function isAlreadyExistsError(error: unknown): error is AlreadyExistsError {
  return error instanceof AlreadyExistsError ||
    (isParqueDBError(error) && error.code === ErrorCode.ALREADY_EXISTS)
}

/**
 * Check if an error is a StorageError (or any subclass)
 */
export function isStorageError(error: unknown): error is StorageError {
  return error instanceof StorageError ||
    (isParqueDBError(error) && error.code.includes('STORAGE'))
}

/**
 * Check if an error is a RelationshipError
 */
export function isRelationshipError(error: unknown): error is RelationshipError {
  return error instanceof RelationshipError
}

/**
 * Check if an error is a QueryError
 */
export function isQueryError(error: unknown): error is QueryError {
  return error instanceof QueryError
}

/**
 * Check if an error is an AuthorizationError
 */
export function isAuthorizationError(error: unknown): error is AuthorizationError {
  return error instanceof AuthorizationError ||
    (isParqueDBError(error) && (
      error.code === ErrorCode.AUTHORIZATION_ERROR ||
      error.code === ErrorCode.PERMISSION_DENIED ||
      error.code === ErrorCode.AUTHENTICATION_REQUIRED
    ))
}

/**
 * Check if an error is an RpcError
 */
export function isRpcError(error: unknown): error is RpcError {
  return error instanceof RpcError ||
    (isParqueDBError(error) && error.code.includes('RPC'))
}

/**
 * Check if an error is an IndexError (or any subclass)
 */
export function isIndexError(error: unknown): error is IndexError {
  return error instanceof IndexError ||
    (isParqueDBError(error) && error.code.includes('INDEX'))
}

/**
 * Check if an error is an EventError
 */
export function isEventError(error: unknown): error is EventError {
  return error instanceof EventError
}

// =============================================================================
// Error Factory Functions
// =============================================================================

/**
 * Wrap an unknown error in a ParqueDBError
 */
export function wrapError(error: unknown, context?: Record<string, unknown>): ParqueDBError {
  if (error instanceof ParqueDBError) {
    return error
  }

  if (error instanceof Error) {
    return new ParqueDBError(error.message, ErrorCode.INTERNAL, context, error)
  }

  return new ParqueDBError(String(error), ErrorCode.UNKNOWN, context)
}

/**
 * Create an error from an HTTP status code
 */
export function errorFromStatus(status: number, message?: string): ParqueDBError {
  switch (status) {
    case 400:
      return new ValidationError(message ?? 'Bad request')
    case 401:
      return new AuthorizationError(
        message ?? 'Authentication required',
        ErrorCode.AUTHENTICATION_REQUIRED
      )
    case 403:
      return new PermissionDeniedError(message ?? 'Forbidden')
    case 404:
      return new NotFoundError(message ?? 'Not found')
    case 409:
      return new ConflictError(message ?? 'Conflict')
    case 429:
      return new ParqueDBError(message ?? 'Too many requests', ErrorCode.TIMEOUT)
    case 500:
      return new ParqueDBError(message ?? 'Internal server error', ErrorCode.INTERNAL)
    case 503:
      return new ParqueDBError(message ?? 'Service unavailable', ErrorCode.RPC_UNAVAILABLE)
    default:
      return new ParqueDBError(message ?? `HTTP error ${status}`, ErrorCode.UNKNOWN)
  }
}

/**
 * Assert a condition, throwing a ValidationError if false
 */
export function assertValid(
  condition: boolean,
  message: string,
  context?: Record<string, unknown>
): asserts condition {
  if (!condition) {
    throw new ValidationError(message, context)
  }
}

/**
 * Assert a value is defined, throwing NotFoundError if undefined/null
 */
export function assertFound<T>(
  value: T | null | undefined,
  message: string,
  code: ErrorCode = ErrorCode.NOT_FOUND
): asserts value is T {
  if (value == null) {
    throw new NotFoundError(message, code)
  }
}

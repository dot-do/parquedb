/**
 * Shared error classes for storage backends
 *
 * All storage errors extend from ParqueDBError via StorageError, providing:
 * - Consistent error codes for programmatic handling
 * - Serialization support for RPC
 * - Cause chaining for debugging
 *
 * Error Hierarchy (extends ParqueDBError):
 * - StorageError (base class)
 *   - NotFoundError (file/object not found)
 *   - AlreadyExistsError (file already exists)
 *   - ETagMismatchError (conditional write failed)
 *   - PermissionDeniedError (access denied)
 *   - NetworkError (connection/timeout issues)
 *   - InvalidPathError (path validation failed)
 *   - QuotaExceededError (storage limit reached)
 *   - DirectoryNotEmptyError (rmdir on non-empty dir)
 *   - DirectoryNotFoundError (directory does not exist)
 *   - PathTraversalError (security: path traversal attempt)
 *
 * @module storage/errors
 */

import {
  ParqueDBError,
  ErrorCode,
  StorageError as _BaseStorageError,
  FileNotFoundError as _BaseFileNotFoundError,
  QuotaExceededError as _BaseQuotaExceededError,
  InvalidPathError as _BaseInvalidPathError,
  PathTraversalError as _BasePathTraversalError,
  NetworkError as _BaseNetworkError,
  ETagMismatchError as _BaseETagMismatchError,
  AlreadyExistsError as _BaseAlreadyExistsError,
  isParqueDBError,
} from '../errors'

// =============================================================================
// Storage Error Codes (Legacy - mapped to ErrorCode)
// =============================================================================

/**
 * Legacy storage error codes - mapped to ErrorCode for backward compatibility.
 *
 * @deprecated Use ErrorCode from '../errors' instead
 */
export enum StorageErrorCode {
  /** File or object not found */
  NOT_FOUND = 'NOT_FOUND',
  /** File or object already exists (for create-only operations) */
  ALREADY_EXISTS = 'ALREADY_EXISTS',
  /** ETag/version mismatch for conditional operations */
  ETAG_MISMATCH = 'ETAG_MISMATCH',
  /** Permission denied */
  PERMISSION_DENIED = 'PERMISSION_DENIED',
  /** Network or connection error */
  NETWORK_ERROR = 'NETWORK_ERROR',
  /** Invalid path (empty, malformed, or traversal attempt) */
  INVALID_PATH = 'INVALID_PATH',
  /** Storage quota exceeded */
  QUOTA_EXCEEDED = 'QUOTA_EXCEEDED',
  /** Directory not empty (for non-recursive rmdir) */
  DIRECTORY_NOT_EMPTY = 'DIRECTORY_NOT_EMPTY',
  /** Directory not found */
  DIRECTORY_NOT_FOUND = 'DIRECTORY_NOT_FOUND',
  /** Generic operation error */
  OPERATION_ERROR = 'OPERATION_ERROR',
}

/**
 * Map legacy StorageErrorCode to ErrorCode
 */
function mapStorageErrorCode(code: StorageErrorCode): ErrorCode {
  switch (code) {
    case StorageErrorCode.NOT_FOUND:
      return ErrorCode.FILE_NOT_FOUND
    case StorageErrorCode.ALREADY_EXISTS:
      return ErrorCode.ALREADY_EXISTS
    case StorageErrorCode.ETAG_MISMATCH:
      return ErrorCode.ETAG_MISMATCH
    case StorageErrorCode.PERMISSION_DENIED:
      return ErrorCode.PERMISSION_DENIED
    case StorageErrorCode.NETWORK_ERROR:
      return ErrorCode.NETWORK_ERROR
    case StorageErrorCode.INVALID_PATH:
      return ErrorCode.INVALID_PATH
    case StorageErrorCode.QUOTA_EXCEEDED:
      return ErrorCode.QUOTA_EXCEEDED
    case StorageErrorCode.DIRECTORY_NOT_EMPTY:
      return ErrorCode.DIRECTORY_NOT_EMPTY
    case StorageErrorCode.DIRECTORY_NOT_FOUND:
      return ErrorCode.DIRECTORY_NOT_FOUND
    case StorageErrorCode.OPERATION_ERROR:
      return ErrorCode.STORAGE_ERROR
    default:
      return ErrorCode.STORAGE_ERROR
  }
}

// =============================================================================
// Base Storage Error (Extends ParqueDBError)
// =============================================================================

/**
 * Base error class for all storage operations.
 *
 * Extends ParqueDBError to provide:
 * - Unified error code system (ErrorCode)
 * - Serialization for RPC
 * - Cause chaining
 *
 * Also provides legacy compatibility with StorageErrorCode.
 */
export class StorageError extends ParqueDBError {
  override name: string = 'StorageError'

  /** Legacy storage error code for backward compatibility */
  readonly storageCode: StorageErrorCode

  constructor(
    message: string,
    storageCode: StorageErrorCode,
    path?: string,
    cause?: Error
  ) {
    super(message, mapStorageErrorCode(storageCode), { path, operation: 'storage' }, cause)
    this.storageCode = storageCode
    Object.setPrototypeOf(this, StorageError.prototype)
  }

  /**
   * Get the path associated with this error
   */
  get path(): string | undefined {
    return this.context.path as string | undefined
  }

  /**
   * Check if this error represents a "not found" condition
   */
  isNotFound(): boolean {
    return this.storageCode === StorageErrorCode.NOT_FOUND ||
           this.storageCode === StorageErrorCode.DIRECTORY_NOT_FOUND
  }

  /**
   * Check if this error represents a precondition failure (etag mismatch)
   */
  isPreconditionFailed(): boolean {
    return this.storageCode === StorageErrorCode.ETAG_MISMATCH
  }

  /**
   * Check if this error represents a conflict (already exists)
   */
  isConflict(): boolean {
    return this.storageCode === StorageErrorCode.ALREADY_EXISTS
  }
}

// =============================================================================
// Specific Storage Errors
// =============================================================================

/**
 * Error thrown when a file or object is not found
 */
export class NotFoundError extends StorageError {
  override name = 'NotFoundError'

  constructor(path: string, cause?: Error) {
    super(`File not found: ${path}`, StorageErrorCode.NOT_FOUND, path, cause)
    Object.setPrototypeOf(this, NotFoundError.prototype)
  }
}

/**
 * Error thrown when a file already exists (for create-only operations)
 *
 * Typically thrown when:
 * - Using ifNoneMatch: '*' option
 * - Attempting to create a file that already exists
 */
export class AlreadyExistsError extends StorageError {
  override name = 'AlreadyExistsError'

  constructor(path: string, cause?: Error) {
    super(`File already exists: ${path}`, StorageErrorCode.ALREADY_EXISTS, path, cause)
    Object.setPrototypeOf(this, AlreadyExistsError.prototype)
  }
}

/**
 * Error thrown when a conditional write fails due to ETag/version mismatch
 *
 * Thrown when:
 * - ifMatch option fails (current etag doesn't match expected)
 * - Optimistic concurrency control detects conflict
 */
export class ETagMismatchError extends StorageError {
  override name = 'ETagMismatchError'

  readonly expectedEtag: string | null
  readonly actualEtag: string | null

  constructor(
    path: string,
    expectedEtag: string | null,
    actualEtag: string | null,
    cause?: Error
  ) {
    super(
      `ETag mismatch for ${path}: expected ${expectedEtag}, got ${actualEtag}`,
      StorageErrorCode.ETAG_MISMATCH,
      path,
      cause
    )
    this.expectedEtag = expectedEtag
    this.actualEtag = actualEtag
    // Add to context for serialization
    this.context.expectedEtag = expectedEtag
    this.context.actualEtag = actualEtag
    Object.setPrototypeOf(this, ETagMismatchError.prototype)
  }
}

/**
 * Error thrown when access to a resource is denied
 */
export class PermissionDeniedError extends StorageError {
  override name = 'PermissionDeniedError'

  constructor(path: string, operation?: string, cause?: Error) {
    const opPart = operation ? ` (${operation})` : ''
    super(
      `Permission denied: ${path}${opPart}`,
      StorageErrorCode.PERMISSION_DENIED,
      path,
      cause
    )
    if (operation) {
      this.context.operation = operation
    }
    Object.setPrototypeOf(this, PermissionDeniedError.prototype)
  }
}

/**
 * Error thrown for network-related failures
 */
export class NetworkError extends StorageError {
  override name = 'NetworkError'

  constructor(message: string, path?: string, cause?: Error) {
    super(message, StorageErrorCode.NETWORK_ERROR, path, cause)
    Object.setPrototypeOf(this, NetworkError.prototype)
  }
}

/**
 * Error thrown when a path is invalid
 *
 * This includes:
 * - Empty paths (when not allowed)
 * - Malformed paths
 * - Path traversal attempts (e.g., containing "..")
 */
export class InvalidPathError extends StorageError {
  override name = 'InvalidPathError'

  constructor(path: string, reason?: string, cause?: Error) {
    const reasonPart = reason ? `: ${reason}` : ''
    super(
      `Invalid path: ${path}${reasonPart}`,
      StorageErrorCode.INVALID_PATH,
      path,
      cause
    )
    if (reason) {
      this.context.reason = reason
    }
    Object.setPrototypeOf(this, InvalidPathError.prototype)
  }
}

/**
 * Error thrown when storage quota is exceeded
 */
export class QuotaExceededError extends StorageError {
  override name = 'QuotaExceededError'

  readonly quotaBytes: number | undefined
  readonly usedBytes: number | undefined

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
    super(message, StorageErrorCode.QUOTA_EXCEEDED, path, cause)
    this.quotaBytes = quotaBytes
    this.usedBytes = usedBytes
    if (quotaBytes !== undefined) {
      this.context.quotaBytes = quotaBytes
    }
    if (usedBytes !== undefined) {
      this.context.usedBytes = usedBytes
    }
    Object.setPrototypeOf(this, QuotaExceededError.prototype)
  }
}

/**
 * Error thrown when attempting to remove a non-empty directory without recursive flag
 */
export class DirectoryNotEmptyError extends StorageError {
  override name = 'DirectoryNotEmptyError'

  constructor(path: string, cause?: Error) {
    super(`Directory not empty: ${path}`, StorageErrorCode.DIRECTORY_NOT_EMPTY, path, cause)
    Object.setPrototypeOf(this, DirectoryNotEmptyError.prototype)
  }
}

/**
 * Error thrown when a directory does not exist
 */
export class DirectoryNotFoundError extends StorageError {
  override name = 'DirectoryNotFoundError'

  constructor(path: string, cause?: Error) {
    super(`Directory not found: ${path}`, StorageErrorCode.DIRECTORY_NOT_FOUND, path, cause)
    Object.setPrototypeOf(this, DirectoryNotFoundError.prototype)
  }
}

/**
 * Error thrown when a path traversal attempt is detected
 *
 * This is a security error indicating an attempt to access files
 * outside the allowed root directory.
 */
export class PathTraversalError extends StorageError {
  override name = 'PathTraversalError'

  constructor(path: string, cause?: Error) {
    super(
      `Path traversal attempt detected: ${path}`,
      StorageErrorCode.INVALID_PATH,
      path,
      cause
    )
    Object.setPrototypeOf(this, PathTraversalError.prototype)
  }
}

/**
 * Generic operation error for backend-specific failures
 *
 * Used when a specific error type doesn't apply but we want to provide
 * context about what operation failed.
 */
export class OperationError extends StorageError {
  override name = 'OperationError'

  readonly operation: string

  constructor(
    message: string,
    operation: string,
    path?: string,
    cause?: Error
  ) {
    super(message, StorageErrorCode.OPERATION_ERROR, path, cause)
    this.operation = operation
    this.context.operation = operation
    Object.setPrototypeOf(this, OperationError.prototype)
  }
}

/**
 * Error thrown when a Parquet write operation fails.
 *
 * This error is critical because Parquet write failures should NEVER be
 * silently ignored as this causes data loss.
 */
export class ParquetWriteError extends StorageError {
  override name = 'ParquetWriteError'

  constructor(
    message: string,
    path?: string,
    cause?: Error
  ) {
    super(message, StorageErrorCode.OPERATION_ERROR, path, cause)
    Object.setPrototypeOf(this, ParquetWriteError.prototype)
  }
}

// =============================================================================
// Backward Compatibility Aliases
// =============================================================================
// These aliases maintain backward compatibility with existing code that
// imports error classes from specific backend modules.

/**
 * @deprecated Use NotFoundError instead
 * Alias for backward compatibility with MemoryBackend
 */
export const FileNotFoundError = NotFoundError

/**
 * @deprecated Use ETagMismatchError instead
 * Alias for backward compatibility with MemoryBackend
 */
export const VersionMismatchError = ETagMismatchError

/**
 * @deprecated Use AlreadyExistsError instead
 * Alias for backward compatibility with MemoryBackend
 */
export const FileExistsError = AlreadyExistsError

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if an error is a StorageError (includes ParqueDBError storage errors)
 */
export function isStorageError(error: unknown): error is StorageError {
  return error instanceof StorageError ||
    (isParqueDBError(error) && (
      error.code === ErrorCode.STORAGE_ERROR ||
      error.code === ErrorCode.STORAGE_READ_ERROR ||
      error.code === ErrorCode.STORAGE_WRITE_ERROR ||
      error.code === ErrorCode.FILE_NOT_FOUND ||
      error.code === ErrorCode.DIRECTORY_NOT_FOUND ||
      error.code === ErrorCode.DIRECTORY_NOT_EMPTY ||
      error.code === ErrorCode.INVALID_PATH ||
      error.code === ErrorCode.PATH_TRAVERSAL ||
      error.code === ErrorCode.QUOTA_EXCEEDED ||
      error.code === ErrorCode.NETWORK_ERROR ||
      error.code === ErrorCode.ETAG_MISMATCH ||
      error.code === ErrorCode.ALREADY_EXISTS
    ))
}

/**
 * Check if an error is a NotFoundError (file not found)
 */
export function isNotFoundError(error: unknown): error is NotFoundError {
  return error instanceof NotFoundError ||
    error instanceof DirectoryNotFoundError ||
    (error instanceof StorageError && (
      error.storageCode === StorageErrorCode.NOT_FOUND ||
      error.storageCode === StorageErrorCode.DIRECTORY_NOT_FOUND
    )) ||
    (isParqueDBError(error) && (
      error.code === ErrorCode.FILE_NOT_FOUND ||
      error.code === ErrorCode.DIRECTORY_NOT_FOUND
    ))
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
 * Check if an error is a PermissionDeniedError
 */
export function isPermissionDeniedError(error: unknown): error is PermissionDeniedError {
  return error instanceof PermissionDeniedError ||
    (isParqueDBError(error) && error.code === ErrorCode.PERMISSION_DENIED)
}

/**
 * Check if an error is a NetworkError
 */
export function isNetworkError(error: unknown): error is NetworkError {
  return error instanceof NetworkError ||
    (isParqueDBError(error) && error.code === ErrorCode.NETWORK_ERROR)
}

/**
 * Check if an error is an InvalidPathError (includes PathTraversalError)
 */
export function isInvalidPathError(error: unknown): error is InvalidPathError {
  return error instanceof InvalidPathError ||
    error instanceof PathTraversalError ||
    (isParqueDBError(error) && (
      error.code === ErrorCode.INVALID_PATH ||
      error.code === ErrorCode.PATH_TRAVERSAL
    ))
}

/**
 * Check if an error is a QuotaExceededError
 */
export function isQuotaExceededError(error: unknown): error is QuotaExceededError {
  return error instanceof QuotaExceededError ||
    (isParqueDBError(error) && error.code === ErrorCode.QUOTA_EXCEEDED)
}

/**
 * Check if an error is a DirectoryNotEmptyError
 */
export function isDirectoryNotEmptyError(error: unknown): error is DirectoryNotEmptyError {
  return error instanceof DirectoryNotEmptyError ||
    (isParqueDBError(error) && error.code === ErrorCode.DIRECTORY_NOT_EMPTY)
}

/**
 * Check if an error is a ParquetWriteError
 */
export function isParquetWriteError(error: unknown): error is ParquetWriteError {
  return error instanceof ParquetWriteError
}

// =============================================================================
// Re-exports from main errors module
// =============================================================================

// Re-export ErrorCode for convenience
export { ErrorCode } from '../errors'

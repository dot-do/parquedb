/**
 * Shared error classes for storage backends
 *
 * Provides a consistent error hierarchy for all storage backend implementations.
 * Each backend can use these shared errors instead of defining their own,
 * ensuring consistent error handling across the codebase.
 *
 * Error Hierarchy:
 * - StorageError (base class with error code)
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
 */

/**
 * Error codes for storage operations
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
 * Base error class for all storage operations
 *
 * Provides structured error information including:
 * - Error code for programmatic handling
 * - Optional path for context
 * - Optional cause for error chaining
 */
export class StorageError extends Error {
  override readonly name: string = 'StorageError'
  readonly code: StorageErrorCode
  readonly path: string | undefined
  override readonly cause: Error | undefined

  constructor(
    message: string,
    code: StorageErrorCode,
    path?: string,
    cause?: Error
  ) {
    super(message)
    this.code = code
    this.path = path
    this.cause = cause
    Object.setPrototypeOf(this, StorageError.prototype)
  }

  /**
   * Check if this error represents a "not found" condition
   */
  isNotFound(): boolean {
    return this.code === StorageErrorCode.NOT_FOUND
  }

  /**
   * Check if this error represents a precondition failure (etag mismatch)
   */
  isPreconditionFailed(): boolean {
    return this.code === StorageErrorCode.ETAG_MISMATCH
  }

  /**
   * Check if this error represents a conflict (already exists)
   */
  isConflict(): boolean {
    return this.code === StorageErrorCode.ALREADY_EXISTS
  }
}

/**
 * Error thrown when a file or object is not found
 */
export class NotFoundError extends StorageError {
  override readonly name = 'NotFoundError'

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
  override readonly name = 'AlreadyExistsError'

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
  override readonly name = 'ETagMismatchError'

  constructor(
    path: string,
    public readonly expectedEtag: string | null,
    public readonly actualEtag: string | null,
    cause?: Error
  ) {
    super(
      `ETag mismatch for ${path}: expected ${expectedEtag}, got ${actualEtag}`,
      StorageErrorCode.ETAG_MISMATCH,
      path,
      cause
    )
    Object.setPrototypeOf(this, ETagMismatchError.prototype)
  }
}

/**
 * Error thrown when access to a resource is denied
 */
export class PermissionDeniedError extends StorageError {
  override readonly name = 'PermissionDeniedError'

  constructor(path: string, operation?: string, cause?: Error) {
    const opPart = operation ? ` (${operation})` : ''
    super(
      `Permission denied: ${path}${opPart}`,
      StorageErrorCode.PERMISSION_DENIED,
      path,
      cause
    )
    Object.setPrototypeOf(this, PermissionDeniedError.prototype)
  }
}

/**
 * Error thrown for network-related failures
 */
export class NetworkError extends StorageError {
  override readonly name = 'NetworkError'

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
  override readonly name = 'InvalidPathError'

  constructor(path: string, reason?: string, cause?: Error) {
    const reasonPart = reason ? `: ${reason}` : ''
    super(
      `Invalid path: ${path}${reasonPart}`,
      StorageErrorCode.INVALID_PATH,
      path,
      cause
    )
    Object.setPrototypeOf(this, InvalidPathError.prototype)
  }
}

/**
 * Error thrown when storage quota is exceeded
 */
export class QuotaExceededError extends StorageError {
  override readonly name = 'QuotaExceededError'

  constructor(
    path: string,
    public readonly quotaBytes?: number,
    public readonly usedBytes?: number,
    cause?: Error
  ) {
    let message = `Quota exceeded for ${path}`
    if (quotaBytes !== undefined && usedBytes !== undefined) {
      message += ` (used ${usedBytes} of ${quotaBytes} bytes)`
    }
    super(message, StorageErrorCode.QUOTA_EXCEEDED, path, cause)
    Object.setPrototypeOf(this, QuotaExceededError.prototype)
  }
}

/**
 * Error thrown when attempting to remove a non-empty directory without recursive flag
 */
export class DirectoryNotEmptyError extends StorageError {
  override readonly name = 'DirectoryNotEmptyError'

  constructor(path: string, cause?: Error) {
    super(`Directory not empty: ${path}`, StorageErrorCode.DIRECTORY_NOT_EMPTY, path, cause)
    Object.setPrototypeOf(this, DirectoryNotEmptyError.prototype)
  }
}

/**
 * Error thrown when a directory does not exist
 */
export class DirectoryNotFoundError extends StorageError {
  override readonly name = 'DirectoryNotFoundError'

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
  override readonly name = 'PathTraversalError'

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
  override readonly name = 'OperationError'

  constructor(
    message: string,
    public readonly operation: string,
    path?: string,
    cause?: Error
  ) {
    super(message, StorageErrorCode.OPERATION_ERROR, path, cause)
    Object.setPrototypeOf(this, OperationError.prototype)
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
 * Check if an error is a StorageError
 */
export function isStorageError(error: unknown): error is StorageError {
  return error instanceof StorageError
}

/**
 * Check if an error is a NotFoundError
 */
export function isNotFoundError(error: unknown): error is NotFoundError {
  return error instanceof NotFoundError ||
    (isStorageError(error) && error.code === StorageErrorCode.NOT_FOUND)
}

/**
 * Check if an error is an ETagMismatchError
 */
export function isETagMismatchError(error: unknown): error is ETagMismatchError {
  return error instanceof ETagMismatchError ||
    (isStorageError(error) && error.code === StorageErrorCode.ETAG_MISMATCH)
}

/**
 * Check if an error is an AlreadyExistsError
 */
export function isAlreadyExistsError(error: unknown): error is AlreadyExistsError {
  return error instanceof AlreadyExistsError ||
    (isStorageError(error) && error.code === StorageErrorCode.ALREADY_EXISTS)
}

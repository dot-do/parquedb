/**
 * BaseBackend - Abstract base class for storage backend implementations
 *
 * Provides shared functionality for all storage backends:
 * - Path validation and normalization
 * - Range parameter validation
 * - Error conversion utilities
 * - Common error handling patterns
 *
 * Subclasses should implement the abstract methods and call the protected
 * helper methods for consistent behavior across all backends.
 */

import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import { validateRange as validateRangeUtil, InvalidRangeError } from './validation'
import { normalizePath as normalizePathUtil, toError as toErrorUtil } from './utils'
import {
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  DirectoryNotEmptyError,
  PathTraversalError,
  OperationError,
} from './errors'

// Re-export errors for backward compatibility with backends that extend BaseBackend
export {
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  DirectoryNotEmptyError,
  PathTraversalError,
  OperationError,
  InvalidRangeError,
}

/**
 * Options for BaseBackend initialization
 */
export interface BaseBackendOptions {
  /**
   * Root path or prefix for all operations
   */
  root?: string | undefined

  /**
   * Whether to enforce path traversal protection
   * Default: true for filesystem backends
   */
  enforcePathTraversal?: boolean | undefined
}

/**
 * Abstract base class for storage backends
 *
 * Provides common functionality and enforces consistent behavior across
 * all storage backend implementations.
 *
 * @example
 * ```typescript
 * class MyBackend extends BaseBackend {
 *   readonly type = 'my-backend'
 *
 *   protected resolvePathInternal(path: string): string {
 *     return this.root + '/' + path
 *   }
 *
 *   async read(path: string): Promise<Uint8Array> {
 *     const resolvedPath = this.resolvePath(path)
 *     // ... implementation
 *   }
 * }
 * ```
 */
export abstract class BaseBackend implements StorageBackend {
  /**
   * Backend type identifier (e.g., 'fs', 'r2', 'memory')
   * Must be overridden by subclasses
   */
  abstract readonly type: string

  /**
   * Root path or prefix for all operations
   */
  protected readonly root: string

  /**
   * Whether to enforce path traversal protection
   */
  protected readonly enforcePathTraversal: boolean

  constructor(options?: BaseBackendOptions) {
    this.root = options?.root ?? ''
    this.enforcePathTraversal = options?.enforcePathTraversal ?? false
  }

  // ===========================================================================
  // Path Utilities
  // ===========================================================================

  /**
   * Normalize a path by removing leading slashes
   *
   * This is the primary normalization used for most backends.
   * Subclasses can override for backend-specific normalization.
   *
   * @param path - The path to normalize
   * @returns The normalized path
   */
  protected normalizePath(path: string): string {
    return normalizePathUtil(path)
  }

  /**
   * Validate a path for security (path traversal prevention)
   *
   * @param path - The path to validate
   * @throws PathTraversalError if path contains traversal attempts
   */
  protected validatePathSecurity(path: string): void {
    if (!this.enforcePathTraversal) {
      return
    }

    // Check for null bytes
    if (path.includes('\x00')) {
      throw new PathTraversalError(path)
    }

    // Check for URL-encoded traversal attempts
    try {
      const decodedPath = decodeURIComponent(path)
      if (decodedPath.includes('..')) {
        throw new PathTraversalError(path)
      }
    } catch {
      // decodeURIComponent can throw on malformed input - treat as potential attack
      if (path.includes('%')) {
        throw new PathTraversalError(path)
      }
    }

    // Check for direct .. traversal
    if (path.includes('..')) {
      throw new PathTraversalError(path)
    }
  }

  /**
   * Resolve a path for use in operations
   *
   * This method normalizes the path and validates it for security.
   * Subclasses should call this method at the start of each operation,
   * then may apply additional backend-specific resolution.
   *
   * @param path - The input path
   * @returns The validated and normalized path
   * @throws PathTraversalError if path validation fails
   */
  protected resolvePath(path: string): string {
    // Normalize first
    const normalized = this.normalizePath(path)

    // Validate for security
    this.validatePathSecurity(normalized)

    return normalized
  }

  // ===========================================================================
  // Validation Utilities
  // ===========================================================================

  /**
   * Validate range parameters for readRange operations
   *
   * @param start - Start byte position (must be >= 0)
   * @param end - End byte position (must be >= start)
   * @throws InvalidRangeError if parameters are invalid
   */
  protected validateRange(start: number, end: number): void {
    validateRangeUtil(start, end)
  }

  // ===========================================================================
  // Error Handling Utilities
  // ===========================================================================

  /**
   * Convert an unknown error to an Error instance
   *
   * This ensures consistent error handling across all backends.
   *
   * @param error - The unknown error to convert
   * @returns An Error instance
   */
  protected toError(error: unknown): Error {
    return toErrorUtil(error)
  }

  /**
   * Check if an error indicates a "not found" condition
   *
   * Override this method to handle backend-specific error codes.
   *
   * @param error - The error to check
   * @returns true if this is a not-found error
   */
  protected isNotFoundError(error: unknown): boolean {
    if (error instanceof NotFoundError) {
      return true
    }
    if (error && typeof error === 'object' && 'code' in error) {
      return error.code === 'ENOENT'
    }
    return false
  }

  /**
   * Check if an error indicates an "already exists" condition
   *
   * Override this method to handle backend-specific error codes.
   *
   * @param error - The error to check
   * @returns true if this is an already-exists error
   */
  protected isExistsError(error: unknown): boolean {
    if (error instanceof AlreadyExistsError) {
      return true
    }
    if (error && typeof error === 'object' && 'code' in error) {
      return error.code === 'EEXIST'
    }
    return false
  }

  /**
   * Check if an error indicates a "directory not empty" condition
   *
   * @param error - The error to check
   * @returns true if this is a directory-not-empty error
   */
  protected isNotEmptyError(error: unknown): boolean {
    if (error instanceof DirectoryNotEmptyError) {
      return true
    }
    if (error && typeof error === 'object' && 'code' in error) {
      return error.code === 'ENOTEMPTY'
    }
    return false
  }

  /**
   * Wrap an error with context for a specific operation
   *
   * @param error - The original error
   * @param operation - The operation that failed
   * @param path - The path involved in the operation
   * @returns An OperationError with context
   */
  protected wrapError(error: unknown, operation: string, path: string): OperationError {
    const err = this.toError(error)
    return new OperationError(
      `Failed to ${operation} ${path}: ${err.message}`,
      operation,
      path,
      err
    )
  }

  // ===========================================================================
  // Abstract Methods (must be implemented by subclasses)
  // ===========================================================================

  abstract read(path: string): Promise<Uint8Array>
  abstract readRange(path: string, start: number, end: number): Promise<Uint8Array>
  abstract exists(path: string): Promise<boolean>
  abstract stat(path: string): Promise<FileStat | null>
  abstract list(prefix: string, options?: ListOptions): Promise<ListResult>
  abstract write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  abstract writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>
  abstract append(path: string, data: Uint8Array): Promise<void>
  abstract delete(path: string): Promise<boolean>
  abstract deletePrefix(prefix: string): Promise<number>
  abstract mkdir(path: string): Promise<void>
  abstract rmdir(path: string, options?: RmdirOptions): Promise<void>
  abstract writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult>
  abstract copy(source: string, dest: string): Promise<void>
  abstract move(source: string, dest: string): Promise<void>
}

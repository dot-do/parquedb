/**
 * ParqueDB Storage Module
 *
 * This module provides storage backend implementations for ParqueDB.
 * It abstracts filesystem, R2, S3, and other storage systems.
 *
 * Implementations:
 * - MemoryBackend: In-memory storage for testing
 * - FsBackend: Node.js filesystem
 * - FsxBackend: Cloudflare fsx
 * - R2Backend: Cloudflare R2
 * - DOSqliteBackend: Cloudflare Durable Object SQLite
 */

// Re-export storage types
export type {
  StorageBackend,
  StreamableBackend,
  MultipartBackend,
  TransactionalBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
  StreamOptions,
  MultipartUpload,
  UploadedPart,
  Transaction,
  // Capability types
  StorageCapabilities,
} from '../types/storage'

// Re-export type guards
export {
  isStreamable,
  isMultipart,
  isTransactional,
} from '../types/storage'

// Re-export capability introspection
export {
  getStorageCapabilities,
  hasStorageCapability,
} from '../types/storage'

// Re-export storage paths
export { StoragePaths, parseStoragePath } from '../types/storage'

// =============================================================================
// Shared Error Classes
// =============================================================================

// Export all shared error classes and types
export {
  StorageError,
  StorageErrorCode,
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  PermissionDeniedError,
  NetworkError,
  InvalidPathError,
  QuotaExceededError,
  DirectoryNotEmptyError,
  DirectoryNotFoundError,
  PathTraversalError,
  OperationError,
  // Backward compatibility aliases
  FileNotFoundError,
  VersionMismatchError,
  FileExistsError,
  // Type guards
  isStorageError,
  isNotFoundError,
  isETagMismatchError,
  isAlreadyExistsError,
} from './errors'

// =============================================================================
// Backend Implementations
// =============================================================================

// MemoryBackend - In-memory storage for testing
export { MemoryBackend } from './MemoryBackend'

// Shared validation utilities
export {
  validateRange,
  validatePath,
  validatePartNumber,
  validateData,
  InvalidRangeError,
} from './validation'

// Shared utility functions
export {
  globToRegex,
  matchGlob,
  generateEtag,
  generateDeterministicEtag,
  normalizePath,
  normalizeFilePath,
} from './utils'

// FsBackend - Node.js filesystem
export { FsBackend } from './FsBackend'

// FsxBackend - Cloudflare fsx
export { FsxBackend } from './FsxBackend'
export type { FsxBackendOptions } from './FsxBackend'
export type { Fsx, FsxStorageTier, FsxError, FsxErrorCode, FsxErrorCodes } from './types/fsx'

// R2Backend - Cloudflare R2
export {
  R2Backend,
  R2OperationError,
  R2ETagMismatchError,
  R2NotFoundError,
  type R2BackendOptions,
} from './R2Backend'
export type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  R2HTTPMetadata,
  R2GetOptions,
  R2PutOptions,
  R2ListOptions,
  R2Objects,
  R2MultipartUpload,
  R2UploadedPart,
  R2MultipartOptions,
} from './types/r2'

// DOSqliteBackend - Cloudflare Durable Object SQLite
export {
  DOSqliteBackend,
  DOSqliteNotFoundError,
  DOSqliteETagMismatchError,
  DOSqliteFileExistsError,
  type SqlStorage,
  type SqlStatement,
  type DOSqliteBackendOptions,
} from './DOSqliteBackend'

// ObservedBackend - Storage wrapper with observability hooks
export { ObservedBackend, withObservability } from './ObservedBackend'

// StorageRouter - Routes storage operations based on collection mode
export {
  StorageRouter,
  type IStorageRouter,
  type StorageMode,
  type RouterSchema,
  type StorageRouterOptions,
  type CollectionSchema,
} from './router'

// RemoteBackend - HTTP-based read-only storage for remote databases
export {
  RemoteBackend,
  createRemoteBackend,
  type RemoteBackendOptions,
} from './RemoteBackend'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Generic test suite factory for StorageBackend implementations
 *
 * This creates a comprehensive test suite that can be used to verify
 * any StorageBackend implementation conforms to the interface contract.
 *
 * Usage:
 * ```typescript
 * import { createStorageBackendTests } from '@parquedb/storage'
 * import { MemoryBackend } from '@parquedb/storage'
 *
 * // Run tests against MemoryBackend
 * createStorageBackendTests(() => new MemoryBackend())
 *
 * // Run tests against custom backend with cleanup
 * createStorageBackendTests(
 *   () => new MyCustomBackend(),
 *   async (backend) => { await backend.cleanup() }
 * )
 * ```
 *
 * Test location: src/storage/__tests__/StorageBackend.test.ts
 */
// export { createStorageBackendTests } from './__tests__/StorageBackend.test'

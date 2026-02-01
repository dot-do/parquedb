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
} from '../types/storage'

// Re-export type guards
export {
  isStreamable,
  isMultipart,
  isTransactional,
} from '../types/storage'

// Re-export storage paths
export { StoragePaths, parseStoragePath } from '../types/storage'

// =============================================================================
// Backend Implementations
// =============================================================================

// MemoryBackend - In-memory storage for testing
export {
  MemoryBackend,
  FileNotFoundError,
  VersionMismatchError,
  FileExistsError,
  DirectoryNotEmptyError,
} from './MemoryBackend'

// FsBackend - Node.js filesystem
export { FsBackend, PathTraversalError } from './FsBackend'

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

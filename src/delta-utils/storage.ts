/**
 * Shared Storage Backend Types
 *
 * Common storage interface that can be used by both ParqueDB and Delta Lake.
 * This provides a minimal abstraction over different storage backends (R2, S3, filesystem, memory).
 *
 * Key features:
 * - Byte-range reads for efficient Parquet file access
 * - Conditional writes for optimistic concurrency control
 * - Cross-platform (Node.js, Cloudflare Workers, browsers)
 */

// =============================================================================
// VERSION MISMATCH ERROR
// =============================================================================

/**
 * Error thrown when a conditional write fails due to version mismatch.
 * This indicates another writer has modified the file since we read it.
 */
export class VersionMismatchError extends Error {
  readonly path: string
  readonly expectedVersion: string | null
  readonly actualVersion: string | null

  constructor(
    path: string,
    expectedVersion: string | null,
    actualVersion: string | null
  ) {
    super(
      `Version mismatch for ${path}: expected ${expectedVersion ?? 'null (create)'}, got ${actualVersion ?? 'null (not found)'}`
    )
    this.name = 'VersionMismatchError'
    this.path = path
    this.expectedVersion = expectedVersion
    this.actualVersion = actualVersion
  }
}

// =============================================================================
// CORE STORAGE INTERFACE
// =============================================================================

/**
 * File statistics
 */
export interface FileStat {
  /** File size in bytes */
  size: number
  /** Last modified time */
  lastModified: Date
  /** ETag/version for conditional operations */
  etag?: string
}

/**
 * Minimal storage backend interface
 *
 * This interface is intentionally minimal to maximize compatibility
 * between different storage implementations. Additional capabilities
 * can be added through extension interfaces.
 */
export interface MinimalStorageBackend {
  /** Read entire file */
  read(path: string): Promise<Uint8Array>

  /** Write file (overwrite if exists) */
  write(path: string, data: Uint8Array): Promise<void>

  /** List files with prefix */
  list(prefix: string): Promise<string[]>

  /** Delete file */
  delete(path: string): Promise<void>

  /** Check if file exists */
  exists(path: string): Promise<boolean>

  /** Get file metadata */
  stat(path: string): Promise<FileStat | null>

  /** Read byte range from file (for Parquet partial reads) */
  readRange(path: string, start: number, end: number): Promise<Uint8Array>
}

/**
 * Extended storage backend with conditional write support
 *
 * This enables optimistic concurrency control for Delta Lake transactions
 * and other scenarios requiring atomic operations.
 */
export interface ConditionalStorageBackend extends MinimalStorageBackend {
  /**
   * Conditionally write a file only if the version matches.
   *
   * @param path - Path to the file
   * @param data - Data to write
   * @param expectedVersion - Expected version/etag, or null for create-if-not-exists
   * @returns The new version after successful write
   * @throws VersionMismatchError if the version doesn't match
   */
  writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string>

  /**
   * Get the current version of a file.
   * Returns null if the file doesn't exist.
   */
  getVersion(path: string): Promise<string | null>
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a storage backend supports conditional writes
 */
export function isConditionalStorage(backend: MinimalStorageBackend): backend is ConditionalStorageBackend {
  return 'writeConditional' in backend && 'getVersion' in backend
}

// =============================================================================
// ASYNC BUFFER (for Parquet integration)
// =============================================================================

/**
 * AsyncBuffer interface for byte-range reads.
 * This is compatible with hyparquet's AsyncBuffer interface.
 */
export interface AsyncBuffer {
  /** Total byte length of the file */
  byteLength: number
  /** Read a byte range [start, end) - can return ArrayBuffer or Uint8Array */
  slice(start: number, end?: number): Promise<ArrayBuffer | Uint8Array> | ArrayBuffer | Uint8Array
}

/**
 * Create an AsyncBuffer from a storage backend.
 * This allows hyparquet to read Parquet files efficiently using byte ranges.
 *
 * @example
 * ```typescript
 * const storage = createStorage({ type: 'memory' })
 * const buffer = await createAsyncBuffer(storage, 'data/table.parquet')
 * const data = await parquetReadObjects({ file: buffer })
 * ```
 */
export async function createAsyncBuffer(
  storage: MinimalStorageBackend,
  path: string
): Promise<AsyncBuffer> {
  const stat = await storage.stat(path)
  if (!stat) throw new Error(`File not found: ${path}`)

  return {
    byteLength: stat.size,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const data = await storage.readRange(path, start, end ?? stat.size)
      // Return a proper ArrayBuffer (not SharedArrayBuffer)
      const arrayBuffer = new ArrayBuffer(data.byteLength)
      new Uint8Array(arrayBuffer).set(data)
      return arrayBuffer
    },
  }
}

// =============================================================================
// STORAGE OPTIONS
// =============================================================================

/**
 * Storage type options for factory functions
 */
export type StorageType = 'filesystem' | 'r2' | 's3' | 'memory'

/**
 * S3 credentials for AWS S3 storage
 */
export interface S3Credentials {
  accessKeyId: string
  secretAccessKey: string
}

/**
 * R2Bucket-like interface for type compatibility
 */
export interface R2BucketLike {
  get(key: string, options?: unknown): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  put(key: string, data: Uint8Array | ArrayBuffer): Promise<unknown>
  delete(key: string): Promise<void>
  head(key: string): Promise<{ size: number; uploaded: Date; etag: string } | null>
  list(options?: { prefix?: string; cursor?: string }): Promise<{
    objects: Array<{ key: string }>
    truncated: boolean
    cursor?: string
  }>
}

// =============================================================================
// STORAGE PATH UTILITIES
// =============================================================================

/**
 * Normalize a storage path
 * - Removes leading slashes
 * - Removes duplicate slashes
 * - Handles . and .. (basic)
 */
export function normalizePath(path: string): string {
  // Remove leading slashes
  let normalized = path.replace(/^\/+/, '')
  // Remove duplicate slashes
  normalized = normalized.replace(/\/+/g, '/')
  // Remove trailing slash (except for empty string)
  if (normalized.length > 0 && normalized.endsWith('/')) {
    normalized = normalized.slice(0, -1)
  }
  return normalized
}

/**
 * Join path segments
 */
export function joinPath(...segments: string[]): string {
  return normalizePath(segments.filter(Boolean).join('/'))
}

/**
 * Get parent directory of a path
 */
export function dirname(path: string): string {
  const normalized = normalizePath(path)
  const lastSlash = normalized.lastIndexOf('/')
  if (lastSlash === -1) return ''
  return normalized.slice(0, lastSlash)
}

/**
 * Get filename from a path
 */
export function basename(path: string): string {
  const normalized = normalizePath(path)
  const lastSlash = normalized.lastIndexOf('/')
  if (lastSlash === -1) return normalized
  return normalized.slice(lastSlash + 1)
}

/**
 * Type definitions for fsx package
 *
 * fsx provides POSIX filesystem semantics for Cloudflare Workers
 * with tiered storage (SQLite for metadata, R2 for content).
 *
 * @see https://github.com/nicholascelestin/fsx
 */

// =============================================================================
// Core FSX Interface
// =============================================================================

/**
 * Main fsx filesystem interface
 * Provides POSIX-like filesystem operations backed by SQLite metadata + R2 content
 */
export interface Fsx {
  /**
   * Read entire file contents
   */
  readFile(path: string, options?: FsxReadOptions): Promise<Uint8Array>

  /**
   * Read file as text
   */
  readFile(path: string, options: FsxReadOptions & { encoding: 'utf-8' }): Promise<string>

  /**
   * Write file contents (creates parent directories if needed)
   */
  writeFile(
    path: string,
    data: Uint8Array | string,
    options?: FsxWriteOptions
  ): Promise<FsxWriteResult>

  /**
   * Write file atomically (write to temp, then rename)
   */
  writeFileAtomic(
    path: string,
    data: Uint8Array | string,
    options?: FsxWriteOptions
  ): Promise<FsxWriteResult>

  /**
   * Append data to file
   */
  appendFile(path: string, data: Uint8Array | string): Promise<void>

  /**
   * Delete a file
   */
  unlink(path: string): Promise<void>

  /**
   * Check if path exists
   */
  exists(path: string): Promise<boolean>

  /**
   * Get file/directory stats
   */
  stat(path: string): Promise<FsxStats>

  /**
   * Get file/directory stats without following symlinks
   */
  lstat(path: string): Promise<FsxStats>

  /**
   * Create directory (and parents with recursive option)
   */
  mkdir(path: string, options?: FsxMkdirOptions): Promise<void>

  /**
   * Remove directory
   */
  rmdir(path: string, options?: FsxRmdirOptions): Promise<void>

  /**
   * Read directory contents
   */
  readdir(path: string, options?: FsxReaddirOptions): Promise<string[] | FsxDirent[]>

  /**
   * Copy file or directory
   */
  cp(source: string, dest: string, options?: FsxCopyOptions): Promise<void>

  /**
   * Rename/move file or directory
   */
  rename(oldPath: string, newPath: string): Promise<void>

  /**
   * Read byte range from file (for partial reads)
   */
  readRange(path: string, start: number, end: number): Promise<Uint8Array>

  /**
   * Glob pattern matching
   */
  glob(pattern: string, options?: FsxGlobOptions): Promise<string[]>

  /**
   * Access check (for permissions)
   */
  access(path: string, mode?: number): Promise<void>

  /**
   * Truncate file to specified length
   */
  truncate(path: string, length?: number): Promise<void>

  // =========================================================================
  // Tiered Storage Operations (fsx-specific)
  // =========================================================================

  /**
   * Get storage tier for a file
   */
  getTier(path: string): Promise<FsxStorageTier>

  /**
   * Move file to specific storage tier
   */
  setTier(path: string, tier: FsxStorageTier): Promise<void>

  /**
   * Promote file to hot tier (cache in SQLite)
   */
  promote(path: string): Promise<void>

  /**
   * Demote file to cold tier (move to R2 only)
   */
  demote(path: string): Promise<void>

  /**
   * Get storage statistics
   */
  storageStats(): Promise<FsxStorageStats>

  // =========================================================================
  // Transaction Support
  // =========================================================================

  /**
   * Begin a transaction
   */
  beginTransaction(): Promise<FsxTransaction>
}

// =============================================================================
// Read/Write Options
// =============================================================================

/**
 * Options for reading files
 */
export interface FsxReadOptions {
  /**
   * Text encoding (if reading as string)
   */
  encoding?: 'utf-8' | 'utf8' | null | undefined

  /**
   * AbortSignal for cancellation
   */
  signal?: AbortSignal | undefined
}

/**
 * Options for writing files
 */
export interface FsxWriteOptions {
  /**
   * File mode (permissions)
   */
  mode?: number | undefined

  /**
   * Text encoding for string data
   */
  encoding?: 'utf-8' | 'utf8' | undefined

  /**
   * Create parent directories if needed
   */
  recursive?: boolean | undefined

  /**
   * Content type (MIME)
   */
  contentType?: string | undefined

  /**
   * Custom metadata
   */
  metadata?: Record<string, string> | undefined

  /**
   * Storage tier to use
   */
  tier?: FsxStorageTier | undefined

  /**
   * Only write if file doesn't exist
   */
  exclusive?: boolean | undefined

  /**
   * Only write if current file's etag matches this value (conditional write).
   * This enables atomic compare-and-swap operations.
   * If the etag doesn't match, the write fails with ECONFLICT error.
   */
  ifMatch?: string | undefined
}

/**
 * Result of write operation
 */
export interface FsxWriteResult {
  /**
   * ETag/checksum of written content
   */
  etag: string

  /**
   * Size in bytes
   */
  size: number

  /**
   * Storage tier used
   */
  tier: FsxStorageTier
}

// =============================================================================
// Directory Options
// =============================================================================

/**
 * Options for mkdir
 */
export interface FsxMkdirOptions {
  /**
   * Create parent directories (mkdir -p)
   */
  recursive?: boolean | undefined

  /**
   * Directory mode (permissions)
   */
  mode?: number | undefined
}

/**
 * Options for rmdir
 */
export interface FsxRmdirOptions {
  /**
   * Remove non-empty directories (rm -rf)
   */
  recursive?: boolean | undefined

  /**
   * Force removal without errors for non-existent
   */
  force?: boolean | undefined
}

/**
 * Options for readdir
 */
export interface FsxReaddirOptions {
  /**
   * Return FsxDirent objects instead of strings
   */
  withFileTypes?: boolean | undefined

  /**
   * Include hidden files (starting with .)
   */
  includeHidden?: boolean | undefined

  /**
   * Recursively list all files
   */
  recursive?: boolean | undefined
}

/**
 * Options for copy
 */
export interface FsxCopyOptions {
  /**
   * Copy directories recursively
   */
  recursive?: boolean | undefined

  /**
   * Overwrite existing files
   */
  force?: boolean | undefined

  /**
   * Preserve timestamps and permissions
   */
  preserveTimestamps?: boolean | undefined
}

/**
 * Options for glob
 */
export interface FsxGlobOptions {
  /**
   * Base directory for glob
   */
  cwd?: string | undefined

  /**
   * Include dotfiles
   */
  dot?: boolean | undefined

  /**
   * Return absolute paths
   */
  absolute?: boolean | undefined

  /**
   * Only return directories
   */
  onlyDirectories?: boolean | undefined

  /**
   * Only return files
   */
  onlyFiles?: boolean | undefined

  /**
   * Patterns to ignore
   */
  ignore?: string[] | undefined
}

// =============================================================================
// File Stats
// =============================================================================

/**
 * File/directory statistics (similar to Node.js fs.Stats)
 */
export interface FsxStats {
  /**
   * File size in bytes
   */
  size: number

  /**
   * Last access time
   */
  atime: Date

  /**
   * Last modification time
   */
  mtime: Date

  /**
   * Creation time
   */
  birthtime: Date

  /**
   * Change time (metadata change)
   */
  ctime: Date

  /**
   * File mode (permissions)
   */
  mode: number

  /**
   * User ID
   */
  uid: number

  /**
   * Group ID
   */
  gid: number

  /**
   * Is regular file
   */
  isFile(): boolean

  /**
   * Is directory
   */
  isDirectory(): boolean

  /**
   * Is symbolic link
   */
  isSymbolicLink(): boolean

  /**
   * ETag/version identifier
   */
  etag?: string | undefined

  /**
   * Content type (MIME)
   */
  contentType?: string | undefined

  /**
   * Custom metadata
   */
  metadata?: Record<string, string> | undefined

  /**
   * Storage tier
   */
  tier?: FsxStorageTier | undefined
}

/**
 * Directory entry (for readdir with withFileTypes)
 */
export interface FsxDirent {
  /**
   * Entry name
   */
  name: string

  /**
   * Entry path (when using recursive)
   */
  path?: string | undefined

  /**
   * Is regular file
   */
  isFile(): boolean

  /**
   * Is directory
   */
  isDirectory(): boolean

  /**
   * Is symbolic link
   */
  isSymbolicLink(): boolean
}

// =============================================================================
// Tiered Storage
// =============================================================================

/**
 * Storage tier for file placement
 */
export type FsxStorageTier = 'hot' | 'warm' | 'cold'

/**
 * Storage statistics
 */
export interface FsxStorageStats {
  /**
   * Total files
   */
  totalFiles: number

  /**
   * Total size in bytes
   */
  totalSize: number

  /**
   * Files in hot tier (SQLite)
   */
  hotFiles: number

  /**
   * Size of hot tier
   */
  hotSize: number

  /**
   * Files in warm tier (both)
   */
  warmFiles: number

  /**
   * Size of warm tier
   */
  warmSize: number

  /**
   * Files in cold tier (R2 only)
   */
  coldFiles: number

  /**
   * Size of cold tier
   */
  coldSize: number
}

// =============================================================================
// Transactions
// =============================================================================

/**
 * Transaction handle for atomic operations
 */
export interface FsxTransaction {
  /**
   * Transaction ID
   */
  id: string

  /**
   * Read file within transaction
   */
  readFile(path: string): Promise<Uint8Array>

  /**
   * Write file within transaction
   */
  writeFile(path: string, data: Uint8Array | string): Promise<void>

  /**
   * Delete file within transaction
   */
  unlink(path: string): Promise<void>

  /**
   * Commit transaction
   */
  commit(): Promise<void>

  /**
   * Rollback transaction
   */
  rollback(): Promise<void>
}

// =============================================================================
// Errors
// =============================================================================

/**
 * Error codes (POSIX-compatible)
 */
export const FsxErrorCodes = {
  ENOENT: 'ENOENT', // No such file or directory
  EEXIST: 'EEXIST', // File exists
  ENOTDIR: 'ENOTDIR', // Not a directory
  EISDIR: 'EISDIR', // Is a directory
  ENOTEMPTY: 'ENOTEMPTY', // Directory not empty
  EACCES: 'EACCES', // Permission denied
  EINVAL: 'EINVAL', // Invalid argument
  ENOSPC: 'ENOSPC', // No space left
  EIO: 'EIO', // I/O error
  ETIMEDOUT: 'ETIMEDOUT', // Operation timed out
  ECONFLICT: 'ECONFLICT', // Conditional write conflict (etag mismatch)
} as const

export type FsxErrorCode = (typeof FsxErrorCodes)[keyof typeof FsxErrorCodes]

/**
 * Error thrown by fsx operations
 */
export interface FsxError extends Error {
  /**
   * POSIX error code
   */
  code: FsxErrorCode

  /**
   * Path that caused the error
   */
  path?: string | undefined

  /**
   * System call that failed
   */
  syscall?: string | undefined

  /**
   * Error number
   */
  errno?: number | undefined
}

// =============================================================================
// Factory Function Types
// =============================================================================

/**
 * Options for creating fsx instance
 */
export interface FsxOptions {
  /**
   * SQLite database for metadata
   */
  db: unknown // D1Database in Cloudflare

  /**
   * R2 bucket for content storage
   */
  bucket: unknown // R2Bucket in Cloudflare

  /**
   * Root path prefix
   */
  root?: string | undefined

  /**
   * Default storage tier for new files
   */
  defaultTier?: FsxStorageTier | undefined

  /**
   * Maximum size for hot tier (SQLite inlining)
   */
  hotThreshold?: number | undefined

  /**
   * Enable caching
   */
  cache?: boolean | undefined
}

/**
 * Factory function to create fsx instance
 */
export type CreateFsx = (options: FsxOptions) => Fsx

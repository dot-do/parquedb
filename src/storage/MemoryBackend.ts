/**
 * MemoryBackend - In-memory implementation of StorageBackend
 *
 * Used for testing and browser environments where filesystem access is not available.
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
import { validateRange, InvalidRangeError } from './validation'

/**
 * Error thrown when a file is not found
 */
export class FileNotFoundError extends Error {
  override readonly name = 'FileNotFoundError'
  constructor(path: string) {
    super(`File not found: ${path}`)
    Object.setPrototypeOf(this, FileNotFoundError.prototype)
  }
}

/**
 * Error thrown when a conditional write fails due to version mismatch
 */
export class VersionMismatchError extends Error {
  override readonly name = 'VersionMismatchError'
  constructor(path: string, expected: string | null, actual: string | null) {
    super(`Version mismatch for ${path}: expected ${expected}, got ${actual}`)
    Object.setPrototypeOf(this, VersionMismatchError.prototype)
  }
}

/**
 * Error thrown when a file already exists (for ifNoneMatch: '*')
 */
export class FileExistsError extends Error {
  override readonly name = 'FileExistsError'
  constructor(path: string) {
    super(`File already exists: ${path}`)
    Object.setPrototypeOf(this, FileExistsError.prototype)
  }
}

/**
 * Error thrown when directory is not empty
 */
export class DirectoryNotEmptyError extends Error {
  override readonly name = 'DirectoryNotEmptyError'
  constructor(path: string) {
    super(`Directory not empty: ${path}`)
    Object.setPrototypeOf(this, DirectoryNotEmptyError.prototype)
  }
}

/**
 * Error thrown when a directory is not found
 */
export class DirectoryNotFoundError extends Error {
  override readonly name = 'DirectoryNotFoundError'
  constructor(path: string) {
    super(`Directory not found: ${path}`)
    Object.setPrototypeOf(this, DirectoryNotFoundError.prototype)
  }
}

/** Stored file entry */
interface FileEntry {
  data: Uint8Array
  metadata: FileStat
}

/**
 * Simple hash function for generating ETags
 */
function generateEtag(data: Uint8Array): string {
  // Simple FNV-1a hash
  let hash = 2166136261
  for (let i = 0; i < data.length; i++) {
    hash ^= data[i]!
    hash = (hash * 16777619) >>> 0
  }
  // Include timestamp to ensure different etags even for same content
  const timestamp = Date.now().toString(36)
  return `${hash.toString(16)}-${timestamp}`
}

/**
 * Simple glob pattern matching
 */
function matchPattern(filename: string, pattern: string): boolean {
  // Convert glob pattern to regex
  const regexPattern = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&') // Escape special chars except * and ?
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.')
  const regex = new RegExp(`^${regexPattern}$`)
  return regex.test(filename)
}

/**
 * In-memory storage backend for testing and browser use
 */
export class MemoryBackend implements StorageBackend {
  readonly type = 'memory'

  /** In-memory storage for files */
  private files = new Map<string, FileEntry>()

  /** Set of directories (virtual) */
  private directories = new Set<string>()

  /**
   * Normalize path (remove leading slash if present, handle trailing slashes)
   */
  private normalizePath(path: string): string {
    // Remove leading slash
    if (path.startsWith('/')) {
      path = path.slice(1)
    }
    return path
  }

  /**
   * Read entire file
   */
  async read(path: string): Promise<Uint8Array> {
    path = this.normalizePath(path)
    const entry = this.files.get(path)
    if (!entry) {
      throw new FileNotFoundError(path)
    }
    // Return a copy to prevent external mutation
    return new Uint8Array(entry.data)
  }

  /**
   * Read byte range from file
   */
  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Validate range parameters
    validateRange(start, end)

    path = this.normalizePath(path)
    const entry = this.files.get(path)
    if (!entry) {
      throw new FileNotFoundError(path)
    }

    // Handle edge cases
    if (start >= entry.data.length) {
      return new Uint8Array(0)
    }

    // Clamp end to file size
    const actualEnd = Math.min(end, entry.data.length)
    if (start >= actualEnd) {
      return new Uint8Array(0)
    }

    return entry.data.slice(start, actualEnd)
  }

  /**
   * Check if file exists
   */
  async exists(path: string): Promise<boolean> {
    path = this.normalizePath(path)
    return this.files.has(path)
  }

  /**
   * Get file metadata
   */
  async stat(path: string): Promise<FileStat | null> {
    path = this.normalizePath(path)

    // Check if it's a file
    const entry = this.files.get(path)
    if (entry) {
      return { ...entry.metadata }
    }

    // Check if it's a directory
    const dirPath = path.endsWith('/') ? path : path + '/'
    if (this.directories.has(path) || this.directories.has(dirPath)) {
      return {
        path,
        size: 0,
        mtime: new Date(),
        isDirectory: true,
      }
    }

    // Check if any files exist under this path (implicit directory)
    for (const filePath of this.files.keys()) {
      if (filePath.startsWith(dirPath)) {
        return {
          path,
          size: 0,
          mtime: new Date(),
          isDirectory: true,
        }
      }
    }

    return null
  }

  /**
   * List files with prefix
   */
  async list(prefix: string, options: ListOptions = {}): Promise<ListResult> {
    prefix = this.normalizePath(prefix)
    const { limit, cursor, delimiter, pattern, includeMetadata } = options

    // Get all matching files
    let matchingFiles: string[] = []
    const prefixes = new Set<string>()

    for (const [filePath] of this.files) {
      // Check if file matches prefix
      if (!filePath.startsWith(prefix)) {
        continue
      }

      // If prefix doesn't end with '/', ensure it matches a complete path segment
      // e.g., prefix 'dat' should NOT match 'data/file.txt'
      // but prefix 'data/' or 'data' should match 'data/file.txt'
      if (prefix.length > 0 && !prefix.endsWith('/')) {
        const charAfterPrefix = filePath[prefix.length]
        // The char after prefix must be '/' or undefined (exact match)
        if (charAfterPrefix !== undefined && charAfterPrefix !== '/') {
          continue
        }
      }

      // Extract the part after the prefix
      const relativePath = filePath.slice(prefix.length)

      // If delimiter is set, check for "directories"
      if (delimiter) {
        const delimIndex = relativePath.indexOf(delimiter)
        if (delimIndex !== -1) {
          // This file is in a subdirectory, add the prefix
          const dirPrefix = prefix + relativePath.slice(0, delimIndex + 1)
          prefixes.add(dirPrefix)
          continue // Don't include the file itself
        }
      }

      // Apply pattern filter if specified
      if (pattern) {
        const filename = filePath.split('/').pop() || filePath
        if (!matchPattern(filename, pattern)) {
          continue
        }
      }

      matchingFiles.push(filePath)
    }

    // Sort files for consistent ordering
    matchingFiles.sort()

    // Handle cursor-based pagination
    let startIndex = 0
    if (cursor) {
      // Decode cursor to get the starting index
      startIndex = parseInt(cursor, 10)
      if (isNaN(startIndex)) {
        startIndex = 0
      }
    }

    // Apply pagination
    let endIndex = matchingFiles.length
    let hasMore = false
    let nextCursor: string | undefined

    if (limit !== undefined) {
      endIndex = Math.min(startIndex + limit, matchingFiles.length)
      hasMore = endIndex < matchingFiles.length
      if (hasMore) {
        nextCursor = endIndex.toString()
      }
    }

    const files = matchingFiles.slice(startIndex, endIndex)

    // Build result
    const result: ListResult = {
      files,
      hasMore,
    }

    if (prefixes.size > 0) {
      result.prefixes = Array.from(prefixes).sort()
    }

    if (nextCursor) {
      result.cursor = nextCursor
    }

    // Include metadata if requested
    if (includeMetadata) {
      result.stats = files.map((filePath) => {
        const entry = this.files.get(filePath)!
        return { ...entry.metadata }
      })
    }

    return result
  }

  /**
   * Write file (overwrite if exists)
   */
  async write(path: string, data: Uint8Array, options: WriteOptions = {}): Promise<WriteResult> {
    path = this.normalizePath(path)

    // Check ifNoneMatch option (only create if not exists)
    if (options.ifNoneMatch === '*' && this.files.has(path)) {
      throw new FileExistsError(path)
    }

    // Check ifMatch option (only update if version matches)
    if (options.ifMatch) {
      const existing = this.files.get(path)
      if (!existing || existing.metadata.etag !== options.ifMatch) {
        throw new VersionMismatchError(
          path,
          options.ifMatch,
          existing?.metadata.etag || null
        )
      }
    }

    const etag = generateEtag(data)
    const now = new Date()

    const metadata: FileStat = {
      path,
      size: data.length,
      mtime: now,
      ctime: this.files.get(path)?.metadata.ctime || now,
      isDirectory: false,
      etag,
      contentType: options.contentType,
      metadata: options.metadata,
    }

    this.files.set(path, {
      data: new Uint8Array(data),
      metadata,
    })

    return {
      etag,
      size: data.length,
    }
  }

  /**
   * Write file atomically (in memory, this is the same as regular write)
   */
  async writeAtomic(path: string, data: Uint8Array, options: WriteOptions = {}): Promise<WriteResult> {
    // In memory, all writes are atomic
    return this.write(path, data, options)
  }

  /**
   * Append to file (for event logs)
   */
  async append(path: string, data: Uint8Array): Promise<void> {
    path = this.normalizePath(path)

    const existing = this.files.get(path)
    if (existing) {
      // Append to existing file
      const newData = new Uint8Array(existing.data.length + data.length)
      newData.set(existing.data, 0)
      newData.set(data, existing.data.length)
      await this.write(path, newData)
    } else {
      // Create new file
      await this.write(path, data)
    }
  }

  /**
   * Delete file
   */
  async delete(path: string): Promise<boolean> {
    path = this.normalizePath(path)
    return this.files.delete(path)
  }

  /**
   * Delete files with prefix
   */
  async deletePrefix(prefix: string): Promise<number> {
    prefix = this.normalizePath(prefix)
    let count = 0

    for (const filePath of Array.from(this.files.keys())) {
      if (filePath.startsWith(prefix)) {
        this.files.delete(filePath)
        count++
      }
    }

    return count
  }

  /**
   * Create directory (and parents if needed)
   */
  async mkdir(path: string): Promise<void> {
    path = this.normalizePath(path)

    // Add directory and all parent directories
    const parts = path.split('/').filter(Boolean)
    let currentPath = ''

    for (const part of parts) {
      currentPath = currentPath ? `${currentPath}/${part}` : part
      this.directories.add(currentPath)
    }
  }

  /**
   * Remove directory
   */
  async rmdir(path: string, options: RmdirOptions = {}): Promise<void> {
    path = this.normalizePath(path)
    const dirPrefix = path.endsWith('/') ? path : path + '/'

    // Check if directory exists (either explicitly created or has files)
    const hasFiles = Array.from(this.files.keys()).some(
      (filePath) => filePath.startsWith(dirPrefix)
    )
    const dirExists = this.directories.has(path) || hasFiles

    if (!dirExists) {
      throw new DirectoryNotFoundError(path)
    }

    if (hasFiles) {
      if (options.recursive) {
        // Delete all files under this directory
        for (const filePath of Array.from(this.files.keys())) {
          if (filePath.startsWith(dirPrefix)) {
            this.files.delete(filePath)
          }
        }
        // Also remove subdirectories
        for (const dir of Array.from(this.directories)) {
          if (dir.startsWith(path)) {
            this.directories.delete(dir)
          }
        }
      } else {
        throw new DirectoryNotEmptyError(path)
      }
    }

    this.directories.delete(path)
  }

  /**
   * Conditional write (for optimistic concurrency)
   * Only writes if current version matches expected
   */
  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options: WriteOptions = {}
  ): Promise<WriteResult> {
    path = this.normalizePath(path)

    const existing = this.files.get(path)
    const currentVersion = existing?.metadata.etag || null

    if (expectedVersion === null) {
      // Expecting file to not exist
      if (existing) {
        throw new VersionMismatchError(path, expectedVersion, currentVersion)
      }
    } else {
      // Expecting file to exist with specific version
      if (!existing) {
        throw new VersionMismatchError(path, expectedVersion, null)
      }
      if (currentVersion !== expectedVersion) {
        throw new VersionMismatchError(path, expectedVersion, currentVersion)
      }
    }

    return this.write(path, data, options)
  }

  /**
   * Copy file
   */
  async copy(source: string, dest: string): Promise<void> {
    source = this.normalizePath(source)
    dest = this.normalizePath(dest)

    const sourceEntry = this.files.get(source)
    if (!sourceEntry) {
      throw new FileNotFoundError(source)
    }

    // Copy data and create new metadata for destination
    await this.write(dest, sourceEntry.data, {
      contentType: sourceEntry.metadata.contentType,
      metadata: sourceEntry.metadata.metadata,
    })
  }

  /**
   * Move/rename file
   */
  async move(source: string, dest: string): Promise<void> {
    source = this.normalizePath(source)
    dest = this.normalizePath(dest)

    const sourceEntry = this.files.get(source)
    if (!sourceEntry) {
      throw new FileNotFoundError(source)
    }

    // Copy to destination then delete source
    await this.write(dest, sourceEntry.data, {
      contentType: sourceEntry.metadata.contentType,
      metadata: sourceEntry.metadata.metadata,
    })
    this.files.delete(source)
  }
}

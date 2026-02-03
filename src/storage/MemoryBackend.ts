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
import { validateRange } from './validation'
import { generateEtag, matchGlob, normalizePath as normalizePathUtil } from './utils'
import {
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  DirectoryNotEmptyError as SharedDirectoryNotEmptyError,
  DirectoryNotFoundError as SharedDirectoryNotFoundError,
} from './errors'

// Re-export shared errors with their original names for backward compatibility
// These are deprecated - use imports from './errors' or '../storage' instead
export {
  NotFoundError as FileNotFoundError,
  ETagMismatchError as VersionMismatchError,
  AlreadyExistsError as FileExistsError,
  SharedDirectoryNotEmptyError as DirectoryNotEmptyError,
  SharedDirectoryNotFoundError as DirectoryNotFoundError,
}

/** Stored file entry */
interface FileEntry {
  data: Uint8Array
  metadata: FileStat
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
    return normalizePathUtil(path)
  }

  /**
   * Read entire file
   */
  async read(path: string): Promise<Uint8Array> {
    path = this.normalizePath(path)
    const entry = this.files.get(path)
    if (!entry) {
      throw new NotFoundError(path)
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
      throw new NotFoundError(path)
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
        if (!matchGlob(filename, pattern)) {
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
      throw new AlreadyExistsError(path)
    }

    // Check ifMatch option (only update if version matches)
    if (options.ifMatch) {
      const existing = this.files.get(path)
      if (!existing || existing.metadata.etag !== options.ifMatch) {
        throw new ETagMismatchError(
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
      throw new SharedDirectoryNotFoundError(path)
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
        throw new SharedDirectoryNotEmptyError(path)
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
        throw new ETagMismatchError(path, expectedVersion, currentVersion)
      }
    } else {
      // Expecting file to exist with specific version
      if (!existing) {
        throw new ETagMismatchError(path, expectedVersion, null)
      }
      if (currentVersion !== expectedVersion) {
        throw new ETagMismatchError(path, expectedVersion, currentVersion)
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
      throw new NotFoundError(source)
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
      throw new NotFoundError(source)
    }

    // Copy to destination then delete source
    await this.write(dest, sourceEntry.data, {
      contentType: sourceEntry.metadata.contentType,
      metadata: sourceEntry.metadata.metadata,
    })
    this.files.delete(source)
  }
}

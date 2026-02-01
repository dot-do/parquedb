/**
 * FsBackend - Node.js filesystem implementation of StorageBackend
 *
 * Uses node:fs/promises for file operations with support for:
 * - Atomic writes (write to .tmp then rename)
 * - Byte range reads (for Parquet partial file access)
 * - Conditional writes using mtime as version
 * - Path traversal prevention for security
 */

import { promises as fs } from 'node:fs'
import { join, dirname, normalize, resolve } from 'node:path'
import type { Stats } from 'node:fs'
import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import {
  FileNotFoundError,
  VersionMismatchError,
  FileExistsError,
  DirectoryNotEmptyError,
} from './MemoryBackend'

/**
 * Error thrown when a path traversal attempt is detected
 */
export class PathTraversalError extends Error {
  constructor(path: string) {
    super(`Path traversal attempt detected: ${path}`)
    this.name = 'PathTraversalError'
  }
}

/**
 * Node.js filesystem storage backend
 */
export class FsBackend implements StorageBackend {
  readonly type = 'fs'
  private readonly resolvedRootPath: string

  /**
   * Create a new FsBackend
   * @param rootPath - The root directory for all operations
   */
  constructor(public readonly rootPath: string) {
    this.resolvedRootPath = resolve(rootPath)
  }

  /**
   * Resolve and validate a path, preventing path traversal attacks
   */
  private resolvePath(path: string): string {
    // Check for null bytes
    if (path.includes('\x00')) {
      throw new PathTraversalError(path)
    }

    // Check for URL-encoded traversal attempts
    const decodedPath = decodeURIComponent(path)
    if (decodedPath.includes('..')) {
      throw new PathTraversalError(path)
    }

    // Reject absolute paths
    if (path.startsWith('/')) {
      throw new PathTraversalError(path)
    }

    // Normalize the path (removes // and resolves ./ but we need to check .. ourselves)
    const normalizedPath = normalize(path)

    // Resolve the full path
    const fullPath = resolve(this.resolvedRootPath, normalizedPath)

    // Security: ensure the resolved path starts with the root path
    if (!fullPath.startsWith(this.resolvedRootPath + '/') && fullPath !== this.resolvedRootPath) {
      throw new PathTraversalError(path)
    }

    return fullPath
  }

  /**
   * Generate an ETag from file stats
   */
  private generateEtag(stat: Stats): string {
    return `"${stat.mtimeMs.toString(36)}-${stat.size.toString(36)}"`
  }

  /**
   * Convert fs.Stats to FileStat
   */
  private toFileStat(path: string, stat: Stats): FileStat {
    return {
      path,
      size: stat.size,
      mtime: stat.mtime,
      ctime: stat.ctime,
      isDirectory: stat.isDirectory(),
      etag: this.generateEtag(stat),
    }
  }

  async read(path: string): Promise<Uint8Array> {
    const fullPath = this.resolvePath(path)
    try {
      const buffer = await fs.readFile(fullPath)
      return new Uint8Array(buffer)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(path)
      }
      throw error
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    if (start < 0) {
      throw new RangeError('Start position cannot be negative')
    }
    if (start > end) {
      throw new RangeError('Start position cannot be greater than end position')
    }

    const fullPath = this.resolvePath(path)

    let handle: import('node:fs/promises').FileHandle | undefined
    try {
      handle = await fs.open(fullPath, 'r')
      const fileStat = await handle.stat()

      // Adjust end if it exceeds file size
      const actualEnd = Math.min(end, fileStat.size)
      const length = actualEnd - start

      if (length <= 0) {
        return new Uint8Array(0)
      }

      const buffer = Buffer.alloc(length)
      await handle.read(buffer, 0, length, start)
      return new Uint8Array(buffer)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(path)
      }
      throw error
    } finally {
      if (handle) {
        await handle.close()
      }
    }
  }

  async exists(path: string): Promise<boolean> {
    const fullPath = this.resolvePath(path)
    try {
      await fs.access(fullPath)
      return true
    } catch {
      return false
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    const fullPath = this.resolvePath(path)
    try {
      const stat = await fs.stat(fullPath)
      return this.toFileStat(path, stat)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        return null
      }
      throw error
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const fullPath = this.resolvePath(prefix || '.')
    const files: string[] = []
    const prefixes: string[] = []
    const stats: FileStat[] = []

    try {
      await this.collectFiles(
        fullPath,
        prefix,
        files,
        prefixes,
        stats,
        options?.delimiter,
        options?.pattern,
        options?.includeMetadata ?? false
      )
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        return { files: [], hasMore: false }
      }
      throw error
    }

    // Sort files for consistent ordering
    files.sort()
    prefixes.sort()

    // Handle pagination
    let startIndex = 0
    if (options?.cursor) {
      startIndex = parseInt(options.cursor, 10)
    }

    const limit = options?.limit ?? files.length
    const endIndex = startIndex + limit
    const paginatedFiles = files.slice(startIndex, endIndex)
    const hasMore = endIndex < files.length

    const result: ListResult = {
      files: paginatedFiles,
      hasMore,
    }

    if (hasMore) {
      result.cursor = endIndex.toString()
    }

    if (options?.delimiter) {
      result.prefixes = prefixes
    }

    if (options?.includeMetadata) {
      // Filter stats to match paginated files
      const paginatedStats = paginatedFiles.map(file =>
        stats.find(s => s.path === file)!
      ).filter(Boolean)
      result.stats = paginatedStats
    }

    return result
  }

  private async collectFiles(
    dirPath: string,
    prefix: string,
    files: string[],
    prefixes: string[],
    stats: FileStat[],
    delimiter?: string,
    pattern?: string,
    includeMetadata: boolean = false
  ): Promise<void> {
    let entries: import('node:fs').Dirent[]
    try {
      entries = await fs.readdir(dirPath, { withFileTypes: true })
    } catch {
      return
    }

    for (const entry of entries) {
      const entryPath = join(dirPath, entry.name)
      const relativePath = prefix ? join(prefix, entry.name) : entry.name

      if (entry.isDirectory()) {
        if (delimiter) {
          // When using delimiter, add directory as prefix
          const prefixPath = relativePath.endsWith('/') ? relativePath : relativePath + '/'
          if (!prefixes.includes(prefixPath)) {
            prefixes.push(prefixPath)
          }
        } else {
          // Recurse into subdirectory
          await this.collectFiles(entryPath, relativePath, files, prefixes, stats, delimiter, pattern, includeMetadata)
        }
      } else {
        // Check pattern matching
        if (pattern) {
          const regex = this.globToRegex(pattern)
          if (!regex.test(entry.name)) {
            continue
          }
        }

        files.push(relativePath)

        if (includeMetadata) {
          try {
            const fileStat = await fs.stat(entryPath)
            stats.push(this.toFileStat(relativePath, fileStat))
          } catch {
            // Skip files that can't be stat'd
          }
        }
      }
    }
  }

  private globToRegex(pattern: string): RegExp {
    const escaped = pattern
      .replace(/[.+^${}()|[\]\\]/g, '\\$&')
      .replace(/\*/g, '.*')
      .replace(/\?/g, '.')
    return new RegExp(`^${escaped}$`)
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)

    // Handle ifNoneMatch: '*' (fail if file exists)
    if (options?.ifNoneMatch === '*') {
      try {
        await fs.access(fullPath)
        throw new FileExistsError(path)
      } catch (error: unknown) {
        if (error instanceof FileExistsError) {
          throw error
        }
        // File doesn't exist, proceed with write
      }
    }

    // Create parent directories
    await fs.mkdir(dirname(fullPath), { recursive: true })

    // Write the file
    await fs.writeFile(fullPath, data)

    const stat = await fs.stat(fullPath)
    return {
      etag: this.generateEtag(stat),
      size: data.length,
    }
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)
    const tempPath = `${fullPath}.tmp.${Date.now()}.${Math.random().toString(36).slice(2)}`

    // Handle ifNoneMatch: '*' (fail if file exists)
    if (options?.ifNoneMatch === '*') {
      try {
        await fs.access(fullPath)
        throw new FileExistsError(path)
      } catch (error: unknown) {
        if (error instanceof FileExistsError) {
          throw error
        }
        // File doesn't exist, proceed with write
      }
    }

    // Create parent directories
    await fs.mkdir(dirname(fullPath), { recursive: true })

    try {
      // Write to temp file
      await fs.writeFile(tempPath, data)

      // Atomically rename temp to target
      await fs.rename(tempPath, fullPath)

      const stat = await fs.stat(fullPath)
      return {
        etag: this.generateEtag(stat),
        size: data.length,
      }
    } catch (error: unknown) {
      // Clean up temp file if it exists
      try {
        await fs.unlink(tempPath)
      } catch {
        // Ignore cleanup errors
      }
      throw error
    }
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    const fullPath = this.resolvePath(path)

    // Create parent directories
    await fs.mkdir(dirname(fullPath), { recursive: true })

    await fs.appendFile(fullPath, data)
  }

  async delete(path: string): Promise<boolean> {
    const fullPath = this.resolvePath(path)
    try {
      const stat = await fs.stat(fullPath)
      if (stat.isDirectory()) {
        return false
      }
      await fs.unlink(fullPath)
      return true
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        return false
      }
      throw error
    }
  }

  async deletePrefix(prefix: string): Promise<number> {
    const fullPath = this.resolvePath(prefix || '.')
    let count = 0

    // Get all files matching the prefix
    const filesToDelete: string[] = []

    const collectFilesToDelete = async (dirPath: string, relativePath: string): Promise<void> => {
      let entries: import('node:fs').Dirent[]
      try {
        entries = await fs.readdir(dirPath, { withFileTypes: true })
      } catch {
        return
      }

      for (const entry of entries) {
        const entryFullPath = join(dirPath, entry.name)
        const entryRelativePath = relativePath ? join(relativePath, entry.name) : entry.name

        if (entry.isDirectory()) {
          await collectFilesToDelete(entryFullPath, entryRelativePath)
        } else {
          // Check if the file matches the prefix
          if (entryRelativePath.startsWith(prefix) || prefix === '' || prefix === '.') {
            filesToDelete.push(entryFullPath)
          }
        }
      }
    }

    // Start collection from appropriate directory
    const prefixDir = dirname(fullPath)
    const prefixBase = prefix.split('/').pop() || ''

    try {
      const stat = await fs.stat(fullPath)
      if (stat.isDirectory()) {
        // Prefix is a directory, collect all files inside
        await collectFilesToDelete(fullPath, prefix)
      } else if (stat.isFile()) {
        // Prefix matches a single file
        filesToDelete.push(fullPath)
      }
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        // Check if prefix matches files in parent directory
        try {
          const entries = await fs.readdir(prefixDir, { withFileTypes: true })
          for (const entry of entries) {
            if (!entry.isDirectory() && entry.name.startsWith(prefixBase)) {
              const entryRelativePath = prefix.includes('/')
                ? join(dirname(prefix), entry.name)
                : entry.name
              if (entryRelativePath.startsWith(prefix)) {
                filesToDelete.push(join(prefixDir, entry.name))
              }
            } else if (entry.isDirectory() && entry.name.startsWith(prefixBase)) {
              const entryRelativePath = prefix.includes('/')
                ? join(dirname(prefix), entry.name)
                : entry.name
              if (entryRelativePath.startsWith(prefix.replace(/\/$/, ''))) {
                await collectFilesToDelete(join(prefixDir, entry.name), entryRelativePath)
              }
            }
          }
        } catch {
          // Parent directory doesn't exist
          return 0
        }
      } else {
        throw error
      }
    }

    // Delete all collected files
    for (const file of filesToDelete) {
      try {
        await fs.unlink(file)
        count++
      } catch {
        // Ignore errors for individual files
      }
    }

    return count
  }

  async mkdir(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    await fs.mkdir(fullPath, { recursive: true })
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    const fullPath = this.resolvePath(path)

    if (options?.recursive) {
      await fs.rm(fullPath, { recursive: true, force: false })
    } else {
      try {
        await fs.rmdir(fullPath)
      } catch (error: unknown) {
        if (error && typeof error === 'object' && 'code' in error) {
          if (error.code === 'ENOTEMPTY') {
            throw new DirectoryNotEmptyError(path)
          }
        }
        throw error
      }
    }
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)

    // Check current state
    let currentStat: Stats | null = null
    try {
      currentStat = await fs.stat(fullPath)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        currentStat = null
      } else {
        throw error
      }
    }

    if (expectedVersion === null) {
      // Expecting file to not exist
      if (currentStat !== null) {
        throw new VersionMismatchError(path, null, this.generateEtag(currentStat))
      }
    } else {
      // Expecting specific version
      if (currentStat === null) {
        throw new VersionMismatchError(path, expectedVersion, null)
      }
      const currentEtag = this.generateEtag(currentStat)
      if (currentEtag !== expectedVersion) {
        throw new VersionMismatchError(path, expectedVersion, currentEtag)
      }
    }

    // Proceed with atomic write
    return this.writeAtomic(path, data, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    const sourceFullPath = this.resolvePath(source)
    const destFullPath = this.resolvePath(dest)

    // Check if source exists
    try {
      await fs.access(sourceFullPath)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(source)
      }
      throw error
    }

    // Create parent directories for destination
    await fs.mkdir(dirname(destFullPath), { recursive: true })

    // Copy the file
    await fs.copyFile(sourceFullPath, destFullPath)
  }

  async move(source: string, dest: string): Promise<void> {
    const sourceFullPath = this.resolvePath(source)
    const destFullPath = this.resolvePath(dest)

    // Check if source exists
    try {
      await fs.access(sourceFullPath)
    } catch (error: unknown) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(source)
      }
      throw error
    }

    // Create parent directories for destination
    await fs.mkdir(dirname(destFullPath), { recursive: true })

    // Move the file (rename)
    await fs.rename(sourceFullPath, destFullPath)
  }
}

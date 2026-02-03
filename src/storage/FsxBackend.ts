/**
 * FsxBackend - Storage backend using fsx for Cloudflare Workers
 *
 * fsx provides POSIX filesystem semantics with tiered storage:
 * - SQLite for metadata and small/hot files
 * - R2 for large/cold content storage
 *
 * This backend is optimized for Cloudflare Workers deployments.
 *
 * @see https://github.com/dot-do/fsx
 */

import { logger } from '../utils/logger'
import type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import type { Fsx, FsxStorageTier, FsxError } from './types/fsx'
import { validateRange } from './validation'
import {
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  OperationError,
  DirectoryNotEmptyError,
} from './errors'
import { toError } from './utils'

/**
 * Options for FsxBackend
 */
export interface FsxBackendOptions {
  /**
   * Root path prefix for all operations
   */
  root?: string

  /**
   * Default storage tier for writes
   */
  defaultTier?: FsxStorageTier
}

/**
 * Storage backend implementation using fsx
 *
 * Provides POSIX-style filesystem operations backed by
 * SQLite (metadata) + R2 (content) tiered storage.
 */
export class FsxBackend implements StorageBackend {
  private readonly fsx: Fsx
  private readonly root: string
  private readonly defaultTier?: FsxStorageTier

  constructor(fsx: Fsx, options?: FsxBackendOptions) {
    this.fsx = fsx
    // Normalize root path by removing trailing slash
    this.root = options?.root?.replace(/\/$/, '') ?? ''
    this.defaultTier = options?.defaultTier

    // Make 'type' truly readonly at runtime
    Object.defineProperty(this, 'type', {
      value: 'fsx',
      writable: false,
      enumerable: true,
      configurable: false,
    })
  }

  readonly type!: 'fsx'

  /**
   * Resolve a path with the root prefix
   */
  private resolvePath(path: string): string {
    if (this.root) {
      return `${this.root}/${path}`
    }
    return path
  }

  async read(path: string): Promise<Uint8Array> {
    const fullPath = this.resolvePath(path)
    try {
      return await this.fsx.readFile(fullPath, {})
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to read ${path}: ${error.message}`,
        'read',
        path,
        error
      )
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Validate range parameters using shared validation
    validateRange(start, end)
    const fullPath = this.resolvePath(path)
    try {
      return await this.fsx.readRange(fullPath, start, end)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to read range ${start}-${end} from ${path}: ${error.message}`,
        'readRange',
        path,
        error
      )
    }
  }

  async exists(path: string): Promise<boolean> {
    const fullPath = this.resolvePath(path)
    try {
      return await this.fsx.exists(fullPath)
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to check existence of ${path}: ${error.message}`,
        'exists',
        path,
        error
      )
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    const fullPath = this.resolvePath(path)
    try {
      const stats = await this.fsx.stat(fullPath)
      return {
        path,
        size: stats.size,
        mtime: stats.mtime,
        ctime: stats.birthtime,
        isDirectory: stats.isDirectory(),
        etag: stats.etag,
        contentType: stats.contentType,
        metadata: stats.metadata,
      }
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        return null
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to stat ${path}: ${error.message}`,
        'stat',
        path,
        error
      )
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const fullPrefix = this.resolvePath(prefix)

    try {
      // If delimiter is specified, use readdir for directory listing
      if (options?.delimiter) {
        const dirents = await this.fsx.readdir(fullPrefix, { withFileTypes: true }) as Array<{ name: string; isFile(): boolean; isDirectory(): boolean }>
        const files: string[] = []
        const prefixes: string[] = []

        for (const dirent of dirents) {
          if (dirent.isDirectory()) {
            prefixes.push(`${prefix}${dirent.name}/`)
          } else {
            files.push(`${prefix}${dirent.name}`)
          }
        }

        return {
          files,
          prefixes,
          hasMore: false,
        }
      }

      // Use glob for pattern matching
      const pattern = options?.pattern
        ? `${fullPrefix}${options.pattern}`
        : `${fullPrefix}**/*`

      const allFiles = await this.fsx.glob(pattern, {})

    const limit = options?.limit
    let files: string[]
    let hasMore = false
    let cursor: string | undefined

    // Decode cursor to get the starting offset
    let startOffset = 0
    if (options?.cursor) {
      try {
        const decoded = JSON.parse(atob(options.cursor))
        if (typeof decoded.offset === 'number' && decoded.offset >= 0) {
          startOffset = decoded.offset
        }
      } catch {
        // Invalid cursor, start from beginning
        startOffset = 0
      }
    }

    if (limit) {
      const endOffset = startOffset + limit
      files = allFiles.slice(startOffset, endOffset)
      hasMore = allFiles.length > endOffset
      if (hasMore) {
        cursor = btoa(JSON.stringify({ offset: endOffset }))
      }
    } else {
      files = allFiles.slice(startOffset)
    }

    const result: ListResult = {
      files,
      hasMore,
      cursor,
    }

      // Include metadata if requested
      if (options?.includeMetadata) {
        const stats: FileStat[] = []
        for (const file of files) {
          const stat = await this.stat(file)
          if (stat) {
            stats.push(stat)
          }
        }
        result.stats = stats
      }

      return result
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to list ${prefix}: ${error.message}`,
        'list',
        prefix,
        error
      )
    }
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)
    try {
      const writeResult = await this.fsx.writeFile(fullPath, data, {
        recursive: true,
        contentType: options?.contentType,
        metadata: options?.metadata,
        tier: this.defaultTier,
      })

      return {
        etag: writeResult.etag,
        size: writeResult.size,
      }
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to write ${path}: ${error.message}`,
        'write',
        path,
        error
      )
    }
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)
    try {
      const writeResult = await this.fsx.writeFileAtomic(fullPath, data, {
        recursive: true,
        contentType: options?.contentType,
        metadata: options?.metadata,
        tier: this.defaultTier,
      })

      return {
        etag: writeResult.etag,
        size: writeResult.size,
      }
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to write atomically to ${path}: ${error.message}`,
        'writeAtomic',
        path,
        error
      )
    }
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.appendFile(fullPath, data)
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to append to ${path}: ${error.message}`,
        'append',
        path,
        error
      )
    }
  }

  async delete(path: string): Promise<boolean> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.unlink(fullPath)
      return true
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        return false
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to delete ${path}: ${error.message}`,
        'delete',
        path,
        error
      )
    }
  }

  async deletePrefix(prefix: string): Promise<number> {
    const fullPrefix = this.resolvePath(prefix)
    try {
      const files = await this.fsx.glob(`${fullPrefix}**/*`, {})

      let deleted = 0
      for (const file of files) {
        try {
          await this.fsx.unlink(file)
          deleted++
        } catch (error: unknown) {
          // Continue even if some files fail to delete
          logger.debug(`Failed to delete file ${file} during deletePrefix`, error)
        }
      }

      return deleted
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to delete prefix ${prefix}: ${error.message}`,
        'deletePrefix',
        prefix,
        error
      )
    }
  }

  async mkdir(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.mkdir(fullPath, { recursive: true })
    } catch (err) {
      const error = toError(err)
      throw new OperationError(
        `Failed to create directory ${path}: ${error.message}`,
        'mkdir',
        path,
        error
      )
    }
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.rmdir(fullPath, { recursive: options?.recursive ?? false })
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOTEMPTY') {
        throw new DirectoryNotEmptyError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to remove directory ${path}: ${error.message}`,
        'rmdir',
        path,
        error
      )
    }
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)

    // Handle ifNoneMatch option (only write if file doesn't exist)
    // Use fsx's exclusive option for atomic "create if not exists" to avoid TOCTOU race
    if (options?.ifNoneMatch === '*') {
      try {
        const writeResult = await this.fsx.writeFile(fullPath, data, {
          recursive: true,
          contentType: options?.contentType,
          metadata: options?.metadata,
          tier: this.defaultTier,
          exclusive: true, // Atomic: fails if file exists
        })

        return {
          etag: writeResult.etag,
          size: writeResult.size,
        }
      } catch (err) {
        const fsxErr = err as FsxError
        if (fsxErr.code === 'EEXIST') {
          throw new AlreadyExistsError(path)
        }
        const error = toError(err)
        throw new OperationError(
          `Failed conditional write to ${path}: ${error.message}`,
          'writeConditional',
          path,
          error
        )
      }
    }

    // For expectedVersion === null (file should not exist), use exclusive write
    if (expectedVersion === null) {
      try {
        const writeResult = await this.fsx.writeFile(fullPath, data, {
          recursive: true,
          contentType: options?.contentType,
          metadata: options?.metadata,
          tier: this.defaultTier,
          exclusive: true, // Atomic: fails if file exists
        })

        return {
          etag: writeResult.etag,
          size: writeResult.size,
        }
      } catch (err) {
        const fsxErr = err as FsxError
        if (fsxErr.code === 'EEXIST') {
          // File exists but we expected it not to - get actual etag for error message
          try {
            const stats = await this.fsx.stat(fullPath)
            throw new ETagMismatchError(path, null, stats.etag ?? null)
          } catch (statErr) {
            // If stat also fails, just report null for actual etag
            if (statErr instanceof ETagMismatchError) {
              throw statErr
            }
            throw new ETagMismatchError(path, null, null)
          }
        }
        const error = toError(err)
        throw new OperationError(
          `Failed conditional write to ${path}: ${error.message}`,
          'writeConditional',
          path,
          error
        )
      }
    }

    // For specific ETag matching, use fsx transaction for atomicity
    // This prevents TOCTOU race between stat() and writeFile()
    const txn = await this.fsx.beginTransaction()
    try {
      // Read current file state within transaction
      let currentEtag: string | null = null
      try {
        const currentData = await txn.readFile(fullPath)
        // Get etag from stat (transaction read doesn't return etag directly)
        const stats = await this.fsx.stat(fullPath)
        currentEtag = stats?.etag ?? null
      } catch (readErr) {
        const fsxErr = readErr as FsxError
        if (fsxErr.code !== 'ENOENT') {
          throw readErr
        }
        // File doesn't exist, currentEtag remains null
      }

      // Check if current version matches expected
      if (currentEtag !== expectedVersion) {
        await txn.rollback()
        throw new ETagMismatchError(path, expectedVersion, currentEtag)
      }

      // Write within transaction
      await txn.writeFile(fullPath, data)
      await txn.commit()

      // Get the new etag after commit
      const newStats = await this.fsx.stat(fullPath)
      return {
        etag: newStats?.etag ?? '',
        size: data.length,
      }
    } catch (err) {
      // Ensure rollback on any error
      try {
        await txn.rollback()
      } catch {
        // Ignore rollback errors
      }

      // Re-throw our own errors
      if (err instanceof ETagMismatchError) {
        throw err
      }
      const error = toError(err)
      throw new OperationError(
        `Failed conditional write to ${path}: ${error.message}`,
        'writeConditional',
        path,
        error
      )
    }
  }

  async copy(source: string, dest: string): Promise<void> {
    const fullSource = this.resolvePath(source)
    const fullDest = this.resolvePath(dest)
    try {
      await this.fsx.cp(fullSource, fullDest, {})
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(source)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to copy ${source} to ${dest}: ${error.message}`,
        'copy',
        source,
        error
      )
    }
  }

  async move(source: string, dest: string): Promise<void> {
    const fullSource = this.resolvePath(source)
    const fullDest = this.resolvePath(dest)
    try {
      await this.fsx.rename(fullSource, fullDest)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(source)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to move ${source} to ${dest}: ${error.message}`,
        'move',
        source,
        error
      )
    }
  }

  // =========================================================================
  // FsxBackend-specific methods (tiered storage)
  // =========================================================================

  /**
   * Get storage tier for a file
   */
  async getTier(path: string): Promise<FsxStorageTier> {
    const fullPath = this.resolvePath(path)
    try {
      return await this.fsx.getTier(fullPath)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to get tier for ${path}: ${error.message}`,
        'getTier',
        path,
        error
      )
    }
  }

  /**
   * Set storage tier for a file
   */
  async setTier(path: string, tier: FsxStorageTier): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.setTier(fullPath, tier)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to set tier for ${path}: ${error.message}`,
        'setTier',
        path,
        error
      )
    }
  }

  /**
   * Promote file to hot tier (cache in SQLite)
   */
  async promote(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.promote(fullPath)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to promote ${path}: ${error.message}`,
        'promote',
        path,
        error
      )
    }
  }

  /**
   * Demote file to cold tier (R2 only)
   */
  async demote(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    try {
      await this.fsx.demote(fullPath)
    } catch (err) {
      const fsxErr = err as FsxError
      if (fsxErr.code === 'ENOENT') {
        throw new NotFoundError(path)
      }
      const error = toError(err)
      throw new OperationError(
        `Failed to demote ${path}: ${error.message}`,
        'demote',
        path,
        error
      )
    }
  }
}

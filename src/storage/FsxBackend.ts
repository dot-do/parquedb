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
    return this.fsx.readFile(fullPath, {})
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Validate range parameters using shared validation
    validateRange(start, end)
    const fullPath = this.resolvePath(path)
    return this.fsx.readRange(fullPath, start, end)
  }

  async exists(path: string): Promise<boolean> {
    const fullPath = this.resolvePath(path)
    return this.fsx.exists(fullPath)
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
      throw err
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const fullPrefix = this.resolvePath(prefix)

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

    if (limit) {
      files = allFiles.slice(0, limit)
      hasMore = allFiles.length > limit
      if (hasMore) {
        cursor = btoa(JSON.stringify({ offset: limit }))
      }
    } else {
      files = allFiles
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
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)
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
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)
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
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.appendFile(fullPath, data)
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
      throw err
    }
  }

  async deletePrefix(prefix: string): Promise<number> {
    const fullPrefix = this.resolvePath(prefix)
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
  }

  async mkdir(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.mkdir(fullPath, { recursive: true })
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.rmdir(fullPath, { recursive: options?.recursive ?? false })
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    const fullPath = this.resolvePath(path)

    // Handle ifNoneMatch option (only write if file doesn't exist)
    if (options?.ifNoneMatch === '*') {
      const fileExists = await this.fsx.exists(fullPath)
      if (fileExists) {
        throw new Error('File already exists')
      }
      return this.write(path, data, options)
    }

    // Check expected version
    if (expectedVersion === null) {
      // Expected file to not exist
      try {
        await this.fsx.stat(fullPath)
        // File exists but we expected it not to
        throw new Error('Version mismatch: file exists but expected it not to')
      } catch (err) {
        const fsxErr = err as FsxError
        if (fsxErr.code === 'ENOENT') {
          // File doesn't exist, write it
          return this.write(path, data, options)
        }
        throw err
      }
    } else {
      // Check if current version matches expected
      const stats = await this.fsx.stat(fullPath)
      if (stats.etag !== expectedVersion) {
        throw new Error(`Version mismatch: expected ${expectedVersion}, got ${stats.etag}`)
      }
      return this.write(path, data, options)
    }
  }

  async copy(source: string, dest: string): Promise<void> {
    const fullSource = this.resolvePath(source)
    const fullDest = this.resolvePath(dest)
    await this.fsx.cp(fullSource, fullDest, {})
  }

  async move(source: string, dest: string): Promise<void> {
    const fullSource = this.resolvePath(source)
    const fullDest = this.resolvePath(dest)
    await this.fsx.rename(fullSource, fullDest)
  }

  // =========================================================================
  // FsxBackend-specific methods (tiered storage)
  // =========================================================================

  /**
   * Get storage tier for a file
   */
  async getTier(path: string): Promise<FsxStorageTier> {
    const fullPath = this.resolvePath(path)
    return this.fsx.getTier(fullPath)
  }

  /**
   * Set storage tier for a file
   */
  async setTier(path: string, tier: FsxStorageTier): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.setTier(fullPath, tier)
  }

  /**
   * Promote file to hot tier (cache in SQLite)
   */
  async promote(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.promote(fullPath)
  }

  /**
   * Demote file to cold tier (R2 only)
   */
  async demote(path: string): Promise<void> {
    const fullPath = this.resolvePath(path)
    await this.fsx.demote(fullPath)
  }
}

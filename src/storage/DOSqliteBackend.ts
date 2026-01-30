/**
 * DOSqliteBackend - Cloudflare Durable Object SQLite storage backend
 *
 * Stores parquet file blocks directly in Durable Object SQLite as blobs.
 *
 * Key design points:
 * 1. Cloudflare charges per row read/written in DO SQLite regardless of blob size (up to 2MB per blob)
 * 2. So storing entire parquet row groups as blobs is very efficient
 * 3. SQLite provides ACID transactions and locality to the DO
 *
 * Schema:
 * ```sql
 * CREATE TABLE IF NOT EXISTS parquet_blocks (
 *   path TEXT PRIMARY KEY,        -- file path like 'users/data.parquet'
 *   data BLOB NOT NULL,           -- parquet file content (up to 2MB)
 *   size INTEGER NOT NULL,        -- size in bytes
 *   etag TEXT NOT NULL,           -- content hash
 *   created_at TEXT NOT NULL,     -- ISO timestamp
 *   updated_at TEXT NOT NULL      -- ISO timestamp
 * );
 *
 * CREATE INDEX IF NOT EXISTS idx_blocks_prefix ON parquet_blocks(path);
 * ```
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

/**
 * Error thrown when a file is not found
 */
export class DOSqliteNotFoundError extends Error {
  constructor(public readonly path: string) {
    super(`File not found: ${path}`)
    this.name = 'DOSqliteNotFoundError'
  }
}

/**
 * Error thrown when a conditional write fails due to ETag mismatch
 */
export class DOSqliteETagMismatchError extends Error {
  constructor(
    public readonly path: string,
    public readonly expectedEtag: string | null,
    public readonly actualEtag: string | null
  ) {
    super(`ETag mismatch for ${path}: expected ${expectedEtag}, got ${actualEtag}`)
    this.name = 'DOSqliteETagMismatchError'
  }
}

/**
 * Error thrown when file already exists (for ifNoneMatch: '*')
 */
export class DOSqliteFileExistsError extends Error {
  constructor(public readonly path: string) {
    super(`File already exists: ${path}`)
    this.name = 'DOSqliteFileExistsError'
  }
}

/**
 * Cloudflare Durable Object SqlStorage interface
 * This is a minimal type definition for the DO SQLite API
 */
export interface SqlStorage {
  exec(query: string): void
  prepare(query: string): SqlStatement
}

/**
 * Prepared SQL statement
 */
export interface SqlStatement {
  bind(...params: unknown[]): SqlStatement
  first<T = Record<string, unknown>>(): T | null
  all<T = Record<string, unknown>>(): { results: T[] }
  run(): { changes: number }
}

/**
 * Row type for parquet_blocks table
 */
interface ParquetBlockRow {
  path: string
  data: ArrayBuffer
  size: number
  etag: string
  created_at: string
  updated_at: string
}

/**
 * Options for DOSqliteBackend
 */
export interface DOSqliteBackendOptions {
  /**
   * Path prefix for all operations
   */
  prefix?: string
}

/**
 * Generate an ETag from data using a simple hash
 */
function generateEtag(data: Uint8Array): string {
  // Simple FNV-1a hash
  let hash = 2166136261
  for (let i = 0; i < data.length; i++) {
    hash ^= data[i]
    hash = (hash * 16777619) >>> 0
  }
  // Include timestamp to ensure different etags even for same content
  const timestamp = Date.now().toString(36)
  return `${hash.toString(16)}-${timestamp}`
}

/**
 * Normalize path by removing leading/trailing slashes
 */
function normalizePath(path: string): string {
  // Remove leading slash
  if (path.startsWith('/')) {
    path = path.slice(1)
  }
  // Remove trailing slash for files
  if (path.endsWith('/')) {
    path = path.slice(0, -1)
  }
  return path
}

/**
 * Storage backend using Cloudflare Durable Object SQLite
 *
 * Optimized for storing parquet blocks as blobs:
 * - Each row group stored as single blob (efficient billing)
 * - ACID transactions via SQLite
 * - Co-located with DO for low latency
 */
export class DOSqliteBackend implements StorageBackend {
  readonly type = 'do-sqlite'
  private readonly sql: SqlStorage
  private readonly prefix: string
  private initialized = false

  constructor(sql: SqlStorage, options?: DOSqliteBackendOptions) {
    this.sql = sql
    // Normalize prefix: ensure it ends with / if provided
    const rawPrefix = options?.prefix ?? ''
    this.prefix = rawPrefix && !rawPrefix.endsWith('/') ? rawPrefix + '/' : rawPrefix
  }

  /**
   * Initialize the database schema
   */
  private ensureSchema(): void {
    if (this.initialized) return

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS parquet_blocks (
        path TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `)

    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_blocks_prefix ON parquet_blocks(path)
    `)

    this.initialized = true
  }

  /**
   * Apply prefix to a path
   */
  private withPrefix(path: string): string {
    return this.prefix + normalizePath(path)
  }

  /**
   * Remove prefix from a path
   */
  private withoutPrefix(path: string): string {
    if (this.prefix && path.startsWith(this.prefix)) {
      return path.slice(this.prefix.length)
    }
    return path
  }

  async read(path: string): Promise<Uint8Array> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    const row = this.sql
      .prepare('SELECT data FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Pick<ParquetBlockRow, 'data'>>()

    if (!row) {
      throw new DOSqliteNotFoundError(path)
    }

    return new Uint8Array(row.data)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    // Validate range parameters
    if (start < 0) {
      throw new Error(`Invalid range: start (${start}) must be non-negative`)
    }
    if (end < start) {
      throw new Error(`Invalid range: end (${end}) must be >= start (${start})`)
    }

    this.ensureSchema()
    const key = this.withPrefix(path)

    const row = this.sql
      .prepare('SELECT data FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Pick<ParquetBlockRow, 'data'>>()

    if (!row) {
      throw new DOSqliteNotFoundError(path)
    }

    const data = new Uint8Array(row.data)
    // end is exclusive in our interface
    const actualEnd = Math.min(end, data.length)
    return data.slice(start, actualEnd)
  }

  async exists(path: string): Promise<boolean> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    const row = this.sql
      .prepare('SELECT 1 FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first()

    return row !== null
  }

  async stat(path: string): Promise<FileStat | null> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    const row = this.sql
      .prepare('SELECT path, size, etag, created_at, updated_at FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Omit<ParquetBlockRow, 'data'>>()

    if (!row) {
      return null
    }

    return {
      path: this.withoutPrefix(row.path),
      size: row.size,
      mtime: new Date(row.updated_at),
      ctime: new Date(row.created_at),
      isDirectory: false,
      etag: row.etag,
    }
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    this.ensureSchema()
    const fullPrefix = this.withPrefix(prefix)

    // Build query with LIKE for prefix matching
    // Use fullPrefix% to match all paths starting with the prefix
    const likePattern = fullPrefix + '%'

    let query = 'SELECT path, size, etag, created_at, updated_at FROM parquet_blocks WHERE path LIKE ? ORDER BY path'
    const params: unknown[] = [likePattern]

    // Handle pagination
    if (options?.limit !== undefined) {
      query += ' LIMIT ?'
      params.push(options.limit + 1) // Fetch one extra to check hasMore
    }

    if (options?.cursor) {
      // Cursor is the last path seen
      query = 'SELECT path, size, etag, created_at, updated_at FROM parquet_blocks WHERE path LIKE ? AND path > ? ORDER BY path'
      params.splice(1, 0, options.cursor)
      if (options?.limit !== undefined) {
        query += ' LIMIT ?'
      }
    }

    const stmt = this.sql.prepare(query)
    const result = stmt.bind(...params).all<Omit<ParquetBlockRow, 'data'>>()

    let files = result.results.map(row => this.withoutPrefix(row.path))
    let hasMore = false
    let cursor: string | undefined

    // Check if there are more results
    if (options?.limit !== undefined && files.length > options.limit) {
      hasMore = true
      files = files.slice(0, options.limit)
      cursor = this.withPrefix(files[files.length - 1])
    }

    // Handle delimiter for directory-style listing
    const prefixes: string[] = []
    if (options?.delimiter) {
      const seen = new Set<string>()
      const filteredFiles: string[] = []

      for (const file of files) {
        const relativePath = file.startsWith(prefix) ? file.slice(prefix.length) : file
        const delimIndex = relativePath.indexOf(options.delimiter)

        if (delimIndex !== -1) {
          // This is in a "subdirectory"
          const dirPrefix = prefix + relativePath.slice(0, delimIndex + 1)
          if (!seen.has(dirPrefix)) {
            seen.add(dirPrefix)
            prefixes.push(dirPrefix)
          }
        } else {
          filteredFiles.push(file)
        }
      }

      files = filteredFiles
    }

    // Apply pattern filter if specified
    if (options?.pattern) {
      const regex = globToRegex(options.pattern)
      files = files.filter(file => {
        const filename = file.split('/').pop() || file
        return regex.test(filename)
      })
    }

    const listResult: ListResult = {
      files,
      hasMore,
    }

    if (prefixes.length > 0) {
      listResult.prefixes = prefixes.sort()
    }

    if (cursor) {
      listResult.cursor = cursor
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
      listResult.stats = stats
    }

    return listResult
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    this.ensureSchema()
    const key = this.withPrefix(path)
    const now = new Date().toISOString()
    const etag = generateEtag(data)
    const size = data.length

    // Handle ifNoneMatch: '*' (only write if doesn't exist)
    if (options?.ifNoneMatch === '*') {
      const exists = this.sql
        .prepare('SELECT 1 FROM parquet_blocks WHERE path = ?')
        .bind(key)
        .first()
      if (exists) {
        throw new DOSqliteFileExistsError(path)
      }
    }

    // Handle ifMatch (only write if etag matches)
    if (options?.ifMatch) {
      const existing = this.sql
        .prepare('SELECT etag FROM parquet_blocks WHERE path = ?')
        .bind(key)
        .first<Pick<ParquetBlockRow, 'etag'>>()

      if (!existing || existing.etag !== options.ifMatch) {
        throw new DOSqliteETagMismatchError(
          path,
          options.ifMatch,
          existing?.etag || null
        )
      }
    }

    // Check if row exists for created_at handling
    const existing = this.sql
      .prepare('SELECT created_at FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Pick<ParquetBlockRow, 'created_at'>>()

    const createdAt = existing?.created_at || now

    // Use INSERT OR REPLACE for upsert behavior
    this.sql
      .prepare(`
        INSERT OR REPLACE INTO parquet_blocks (path, data, size, etag, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `)
      .bind(key, data, size, etag, createdAt, now)
      .run()

    return {
      etag,
      size,
    }
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    // SQLite operations are inherently atomic within a transaction
    // The write operation already uses INSERT OR REPLACE which is atomic
    return this.write(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    // Read existing data
    const existing = this.sql
      .prepare('SELECT data FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Pick<ParquetBlockRow, 'data'>>()

    if (existing) {
      // Append to existing data
      const existingData = new Uint8Array(existing.data)
      const newData = new Uint8Array(existingData.length + data.length)
      newData.set(existingData, 0)
      newData.set(data, existingData.length)
      await this.write(path, newData)
    } else {
      // Create new file
      await this.write(path, data)
    }
  }

  async delete(path: string): Promise<boolean> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    const result = this.sql
      .prepare('DELETE FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .run()

    return result.changes > 0
  }

  async deletePrefix(prefix: string): Promise<number> {
    this.ensureSchema()
    const fullPrefix = this.withPrefix(prefix)
    const likePattern = fullPrefix + '%'

    const result = this.sql
      .prepare('DELETE FROM parquet_blocks WHERE path LIKE ?')
      .bind(likePattern)
      .run()

    return result.changes
  }

  async mkdir(_path: string): Promise<void> {
    // SQLite doesn't have real directories, so mkdir is a no-op
    // Files are stored with their full path as the key
    return
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    if (options?.recursive) {
      // Delete all files with this prefix
      await this.deletePrefix(path.endsWith('/') ? path : path + '/')
    }
    // Non-recursive rmdir is a no-op for this backend
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    this.ensureSchema()
    const key = this.withPrefix(path)

    // Get current state
    const existing = this.sql
      .prepare('SELECT etag FROM parquet_blocks WHERE path = ?')
      .bind(key)
      .first<Pick<ParquetBlockRow, 'etag'>>()

    const currentEtag = existing?.etag || null

    if (expectedVersion === null) {
      // Expecting file to not exist
      if (existing) {
        throw new DOSqliteETagMismatchError(path, expectedVersion, currentEtag)
      }
    } else {
      // Expecting file to exist with specific version
      if (!existing) {
        throw new DOSqliteETagMismatchError(path, expectedVersion, null)
      }
      if (currentEtag !== expectedVersion) {
        throw new DOSqliteETagMismatchError(path, expectedVersion, currentEtag)
      }
    }

    return this.write(path, data, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    this.ensureSchema()
    const sourceKey = this.withPrefix(source)
    const destKey = this.withPrefix(dest)

    // Read source
    const sourceRow = this.sql
      .prepare('SELECT data FROM parquet_blocks WHERE path = ?')
      .bind(sourceKey)
      .first<Pick<ParquetBlockRow, 'data'>>()

    if (!sourceRow) {
      throw new DOSqliteNotFoundError(source)
    }

    // Write to destination
    const data = new Uint8Array(sourceRow.data)
    await this.write(dest, data)
  }

  async move(source: string, dest: string): Promise<void> {
    this.ensureSchema()
    const sourceKey = this.withPrefix(source)
    const destKey = this.withPrefix(dest)

    // Check source exists
    const sourceRow = this.sql
      .prepare('SELECT data, etag, created_at FROM parquet_blocks WHERE path = ?')
      .bind(sourceKey)
      .first<Pick<ParquetBlockRow, 'data' | 'etag' | 'created_at'>>()

    if (!sourceRow) {
      throw new DOSqliteNotFoundError(source)
    }

    const now = new Date().toISOString()
    const data = new Uint8Array(sourceRow.data)

    // Insert at destination
    this.sql
      .prepare(`
        INSERT OR REPLACE INTO parquet_blocks (path, data, size, etag, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `)
      .bind(destKey, data, data.length, sourceRow.etag, sourceRow.created_at, now)
      .run()

    // Delete source
    this.sql
      .prepare('DELETE FROM parquet_blocks WHERE path = ?')
      .bind(sourceKey)
      .run()
  }
}

/**
 * Convert glob pattern to regex
 */
function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.')
  return new RegExp(`^${escaped}$`)
}

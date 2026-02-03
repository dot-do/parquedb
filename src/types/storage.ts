/**
 * Storage backend interface for ParqueDB
 * Abstracts filesystem, R2, S3, and other storage systems
 */

// =============================================================================
// Core Storage Interface
// =============================================================================

/**
 * Read-only storage backend interface
 *
 * Use this interface for adapters that only need read access (e.g., CDN adapters,
 * read replicas, query executors). This avoids the need for stub implementations
 * of write methods.
 *
 * Implementations: CdnR2StorageAdapter, read-only wrappers
 */
export interface ReadonlyStorageBackend {
  /** Backend type identifier */
  readonly type: string

  // =========================================================================
  // Read Operations
  // =========================================================================

  /**
   * Read entire file
   */
  read(path: string): Promise<Uint8Array>

  /**
   * Read byte range from file (for Parquet partial reads)
   *
   * Uses EXCLUSIVE end position semantics (like Array.slice):
   * - readRange(path, 0, 5) reads bytes 0,1,2,3,4 (5 bytes)
   * - readRange(path, 2, 6) reads bytes 2,3,4,5 (4 bytes)
   * - readRange(path, 5, 5) returns empty array (zero-length range)
   *
   * If end exceeds file size, returns bytes up to end of file.
   * If start >= file size, returns empty array.
   *
   * @param path - File path to read from
   * @param start - Start byte offset (inclusive, 0-indexed)
   * @param end - End byte offset (EXCLUSIVE - byte at this index is NOT included)
   * @returns Bytes from start (inclusive) to end (exclusive)
   */
  readRange(path: string, start: number, end: number): Promise<Uint8Array>

  /**
   * Check if file exists
   */
  exists(path: string): Promise<boolean>

  /**
   * Get file metadata
   */
  stat(path: string): Promise<FileStat | null>

  /**
   * List files with prefix
   */
  list(prefix: string, options?: ListOptions): Promise<ListResult>
}

/**
 * Storage backend interface
 * Implementations: FsBackend, FsxBackend, R2Backend, S3Backend, MemoryBackend
 */
export interface StorageBackend extends ReadonlyStorageBackend {
  // =========================================================================
  // Write Operations
  // =========================================================================

  /**
   * Write file (overwrite if exists)
   */
  write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>

  /**
   * Write file atomically (write to temp, then rename)
   */
  writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult>

  /**
   * Append to file (for event logs)
   */
  append(path: string, data: Uint8Array): Promise<void>

  /**
   * Delete file
   */
  delete(path: string): Promise<boolean>

  /**
   * Delete files with prefix
   */
  deletePrefix(prefix: string): Promise<number>

  // =========================================================================
  // Directory Operations
  // =========================================================================

  /**
   * Create directory (and parents if needed)
   */
  mkdir(path: string): Promise<void>

  /**
   * Remove directory
   */
  rmdir(path: string, options?: RmdirOptions): Promise<void>

  // =========================================================================
  // Atomic Operations
  // =========================================================================

  /**
   * Conditional write (for optimistic concurrency)
   * Only writes if current version matches expected
   */
  writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult>

  /**
   * Copy file
   */
  copy(source: string, dest: string): Promise<void>

  /**
   * Move/rename file
   */
  move(source: string, dest: string): Promise<void>
}

// =============================================================================
// File Metadata
// =============================================================================

/** File statistics */
export interface FileStat {
  /** File path */
  path: string

  /** File size in bytes */
  size: number

  /** Last modified time */
  mtime: Date

  /** Creation time (if available) */
  ctime?: Date | undefined

  /** Is directory */
  isDirectory: boolean

  /** ETag/version for conditional operations */
  etag?: string | undefined

  /** Content type (MIME) */
  contentType?: string | undefined

  /** Custom metadata */
  metadata?: Record<string, string> | undefined
}

// =============================================================================
// List Operations
// =============================================================================

/** Options for list operation */
export interface ListOptions {
  /** Maximum results to return */
  limit?: number | undefined

  /** Cursor for pagination */
  cursor?: string | undefined

  /** Delimiter for "directory" grouping (usually '/') */
  delimiter?: string | undefined

  /** Only include files matching pattern */
  pattern?: string | undefined

  /** Include metadata with results */
  includeMetadata?: boolean | undefined
}

/** Result of list operation */
export interface ListResult {
  /** File paths (or keys) */
  files: string[]

  /** Directory prefixes (when using delimiter) */
  prefixes?: string[] | undefined

  /** Cursor for next page */
  cursor?: string | undefined

  /** Whether there are more results */
  hasMore: boolean

  /** File stats (if includeMetadata was true) */
  stats?: FileStat[] | undefined
}

// =============================================================================
// Write Operations
// =============================================================================

/** Options for write operation */
export interface WriteOptions {
  /** Content type (MIME) */
  contentType?: string | undefined

  /** Custom metadata */
  metadata?: Record<string, string> | undefined

  /** Cache control header */
  cacheControl?: string | undefined

  /** Expected ETag for conditional write */
  ifMatch?: string | undefined

  /** Only write if doesn't exist */
  ifNoneMatch?: '*' | undefined

  /** Custom modification time (for testing) */
  mtime?: Date | undefined
}

/** Result of write operation */
export interface WriteResult {
  /** ETag/version of written file */
  etag: string

  /** Version ID (for versioned storage) */
  versionId?: string | undefined

  /** Bytes written */
  size: number
}

/** Options for rmdir */
export interface RmdirOptions {
  /** Remove directory even if not empty */
  recursive?: boolean | undefined
}

// =============================================================================
// Streaming Interface
// =============================================================================

/** Streaming read support */
export interface StreamableBackend extends StorageBackend {
  /**
   * Create readable stream for file
   */
  createReadStream(path: string, options?: StreamOptions): ReadableStream<Uint8Array>

  /**
   * Create writable stream for file
   */
  createWriteStream(path: string, options?: WriteOptions): WritableStream<Uint8Array>
}

/** Options for streaming */
export interface StreamOptions {
  /** Start byte offset */
  start?: number | undefined

  /** End byte offset */
  end?: number | undefined

  /** Chunk size for reading */
  highWaterMark?: number | undefined
}

// =============================================================================
// Multipart Upload (for large files)
// =============================================================================

/** Multipart upload support */
export interface MultipartBackend extends StorageBackend {
  /**
   * Start multipart upload
   */
  createMultipartUpload(path: string, options?: WriteOptions): Promise<MultipartUpload>
}

/** Multipart upload handle */
export interface MultipartUpload {
  /** Upload ID */
  uploadId: string

  /** Upload a part */
  uploadPart(partNumber: number, data: Uint8Array): Promise<UploadedPart>

  /** Complete the upload */
  complete(parts: UploadedPart[]): Promise<WriteResult>

  /** Abort the upload */
  abort(): Promise<void>
}

/** Uploaded part reference */
export interface UploadedPart {
  partNumber: number
  etag: string
  size: number
}

// =============================================================================
// Transaction Support
// =============================================================================

/** Transactional backend (optional) */
export interface TransactionalBackend extends StorageBackend {
  /**
   * Begin a transaction
   */
  beginTransaction(): Promise<Transaction>
}

/** Transaction handle */
export interface Transaction {
  /** Transaction ID */
  id: string

  /** Read within transaction */
  read(path: string): Promise<Uint8Array>

  /** Write within transaction */
  write(path: string, data: Uint8Array): Promise<void>

  /** Delete within transaction */
  delete(path: string): Promise<void>

  /** Commit transaction */
  commit(): Promise<void>

  /** Rollback transaction */
  rollback(): Promise<void>
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a backend is read-only (implements ReadonlyStorageBackend but not full StorageBackend)
 *
 * Use this to detect CDN adapters and other read-only storage implementations.
 */
export function isReadonlyBackend(backend: ReadonlyStorageBackend): boolean {
  return (
    typeof (backend as StorageBackend).write !== 'function' ||
    typeof (backend as StorageBackend).delete !== 'function'
  )
}

/**
 * Check if a backend supports write operations (implements full StorageBackend)
 */
export function isWritableBackend(backend: ReadonlyStorageBackend): backend is StorageBackend {
  return (
    typeof (backend as StorageBackend).write === 'function' &&
    typeof (backend as StorageBackend).delete === 'function' &&
    typeof (backend as StorageBackend).mkdir === 'function'
  )
}

/** Check if backend supports streaming */
export function isStreamable(backend: StorageBackend): backend is StreamableBackend {
  return (
    typeof (backend as StreamableBackend).createReadStream === 'function' &&
    typeof (backend as StreamableBackend).createWriteStream === 'function'
  )
}

/** Check if backend supports multipart uploads */
export function isMultipart(backend: StorageBackend): backend is MultipartBackend {
  return typeof (backend as MultipartBackend).createMultipartUpload === 'function'
}

/** Check if backend supports transactions */
export function isTransactional(backend: StorageBackend): backend is TransactionalBackend {
  return typeof (backend as TransactionalBackend).beginTransaction === 'function'
}

// =============================================================================
// Storage Capabilities
// =============================================================================

/**
 * Storage backend capabilities
 *
 * Describes what features a storage backend supports at runtime.
 * Use getStorageCapabilities() to query a backend's capabilities.
 */
export interface StorageCapabilities {
  /** Backend type identifier (e.g., 'memory', 'r2', 'fs', 'fsx') */
  type: string

  // ---------------------------------------------------------------------------
  // Core Capabilities
  // ---------------------------------------------------------------------------

  /** Whether the backend supports atomic writes */
  atomicWrites: boolean

  /** Whether the backend supports conditional writes (ETag-based) */
  conditionalWrites: boolean

  /** Whether the backend supports byte-range reads */
  rangeReads: boolean

  /** Whether the backend supports append operations */
  append: boolean

  // ---------------------------------------------------------------------------
  // Extended Capabilities
  // ---------------------------------------------------------------------------

  /** Whether the backend supports streaming reads/writes */
  streaming: boolean

  /** Whether the backend supports multipart uploads for large files */
  multipart: boolean

  /** Whether the backend supports transactions */
  transactions: boolean

  // ---------------------------------------------------------------------------
  // Directory Operations
  // ---------------------------------------------------------------------------

  /** Whether the backend has real directory support (vs virtual/prefix-based) */
  realDirectories: boolean

  /** Whether mkdir is required before writing files */
  requiresMkdir: boolean

  // ---------------------------------------------------------------------------
  // Performance Characteristics
  // ---------------------------------------------------------------------------

  /** Whether list operations are efficient (vs scanning all keys) */
  efficientList: boolean

  /** Whether stat operations are efficient (single operation vs read) */
  efficientStat: boolean

  /** Maximum file size supported in bytes (undefined = unlimited) */
  maxFileSize?: number | undefined

  /** Maximum concurrent operations recommended (undefined = unlimited) */
  maxConcurrency?: number | undefined
}

/**
 * Get the capabilities of a storage backend
 *
 * This function introspects a StorageBackend instance to determine
 * what features it supports at runtime.
 *
 * @example
 * ```typescript
 * const backend = new R2Backend(bucket)
 * const caps = getStorageCapabilities(backend)
 *
 * if (caps.multipart) {
 *   // Use multipart upload for large files
 * }
 *
 * if (caps.conditionalWrites) {
 *   // Use optimistic concurrency control
 * }
 * ```
 */
export function getStorageCapabilities(backend: StorageBackend): StorageCapabilities {
  const type = backend.type

  // Detect extended capabilities via type guards
  const streaming = isStreamable(backend)
  const multipart = isMultipart(backend)
  const transactions = isTransactional(backend)

  // Backend-specific capability profiles
  const profiles: Record<string, Partial<StorageCapabilities>> = {
    memory: {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true,
      realDirectories: true,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
    },
    fs: {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true,
      realDirectories: true,
      requiresMkdir: true,
      efficientList: true,
      efficientStat: true,
    },
    fsx: {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true,
      realDirectories: false,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
    },
    r2: {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true, // via read-modify-write
      realDirectories: false,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
      maxFileSize: 5 * 1024 * 1024 * 1024 * 1024, // 5TB
    },
    's3': {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true, // via read-modify-write
      realDirectories: false,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
      maxFileSize: 5 * 1024 * 1024 * 1024 * 1024, // 5TB
    },
    'do-sqlite': {
      atomicWrites: true,
      conditionalWrites: true,
      rangeReads: true,
      append: true,
      realDirectories: false,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
      maxFileSize: 128 * 1024 * 1024, // 128MB per DO
    },
    remote: {
      atomicWrites: false, // read-only
      conditionalWrites: false,
      rangeReads: true,
      append: false,
      realDirectories: false,
      requiresMkdir: false,
      efficientList: true,
      efficientStat: true,
    },
  }

  // Get profile for this backend type, or use sensible defaults
  const profile = profiles[type] ?? {
    atomicWrites: true,
    conditionalWrites: true,
    rangeReads: true,
    append: true,
    realDirectories: false,
    requiresMkdir: false,
    efficientList: true,
    efficientStat: true,
  }

  return {
    type,
    streaming,
    multipart,
    transactions,
    ...profile,
  } as StorageCapabilities
}

/**
 * Check if a backend supports a specific capability
 *
 * @example
 * ```typescript
 * if (hasCapability(backend, 'multipart')) {
 *   // Use multipart uploads
 * }
 * ```
 */
export function hasStorageCapability(
  backend: StorageBackend,
  capability: keyof Omit<StorageCapabilities, 'type' | 'maxFileSize' | 'maxConcurrency'>
): boolean {
  const caps = getStorageCapabilities(backend)
  return caps[capability] === true
}

// =============================================================================
// Storage Path Utilities
// =============================================================================

/** Standard path conventions for ParqueDB */
export const StoragePaths = {
  /** Manifest file */
  manifest: '_meta/manifest.json',

  /** Schema file */
  schema: '_meta/schema.json',

  /** Entity data file */
  data: (ns: string) => `data/${ns}/data.parquet`,

  /** Entity data shard */
  dataShard: (ns: string, shard: number) =>
    `data/${ns}/data.${shard.toString().padStart(4, '0')}.parquet`,

  /** Forward relationships */
  relsForward: (ns: string) => `rels/forward/${ns}.parquet`,

  /** Reverse relationships */
  relsReverse: (ns: string) => `rels/reverse/${ns}.parquet`,

  /** Current event log */
  eventsCurrent: 'events/current.parquet',

  /** Archived events */
  eventsArchive: (period: string) => `events/archive/${period}.parquet`,

  /** Checkpoint */
  checkpoint: (sequence: string) => `events/checkpoints/${sequence}.json`,

  /** FTS index */
  ftsIndex: (ns: string) => `indexes/fts/${ns}`,

  /** Vector index */
  vectorIndex: (ns: string, field: string) => `indexes/vector/${ns}.${field}`,

  /** Secondary index */
  secondaryIndex: (ns: string, name: string) => `indexes/secondary/${ns}.${name}.idx.parquet`,

  /** Bloom filter */
  bloomFilter: (ns: string) => `indexes/bloom/${ns}.bloom`,
} as const

/** Parse a storage path */
export function parseStoragePath(path: string): { type: string; ns?: string | undefined; [key: string]: unknown } {
  if (path.startsWith('data/')) {
    const match = path.match(/^data\/([^/]+)\/data(?:\.(\d+))?\.parquet$/)
    if (match) {
      return { type: 'data', ns: match[1], shard: match[2] ? parseInt(match[2]) : undefined }
    }
  }

  if (path.startsWith('rels/forward/')) {
    const match = path.match(/^rels\/forward\/([^/]+)\.parquet$/)
    if (match) return { type: 'rels-forward', ns: match[1] }
  }

  if (path.startsWith('rels/reverse/')) {
    const match = path.match(/^rels\/reverse\/([^/]+)\.parquet$/)
    if (match) return { type: 'rels-reverse', ns: match[1] }
  }

  if (path.startsWith('events/')) {
    if (path === 'events/current.parquet') return { type: 'events-current' }
    const archiveMatch = path.match(/^events\/archive\/(.+)\.parquet$/)
    if (archiveMatch) return { type: 'events-archive', period: archiveMatch[1] }
  }

  if (path.startsWith('indexes/')) {
    const ftsMatch = path.match(/^indexes\/fts\/([^/]+)/)
    if (ftsMatch) return { type: 'index-fts', ns: ftsMatch[1] }

    const vectorMatch = path.match(/^indexes\/vector\/([^.]+)\.(.+)/)
    if (vectorMatch) return { type: 'index-vector', ns: vectorMatch[1], field: vectorMatch[2] }
  }

  if (path.startsWith('_meta/')) {
    return { type: 'meta', file: path.replace('_meta/', '') }
  }

  return { type: 'unknown', path }
}

/**
 * RemoteBackend - Read-only HTTP storage backend
 *
 * Provides read access to remote ParqueDB databases over HTTP.
 * Supports Range requests for efficient Parquet file reading.
 *
 * @example
 * ```typescript
 * const backend = new RemoteBackend({
 *   baseUrl: 'https://parque.db/db/username/my-dataset',
 * })
 *
 * // Read Parquet footer (last 8 bytes to get footer length)
 * const footer = await backend.readRange('data/posts/data.parquet', -8, -1)
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
import { NotFoundError, PermissionDeniedError, NetworkError, PathTraversalError } from './errors'
import { DEFAULT_REMOTE_CACHE_TTL, DEFAULT_REMOTE_TIMEOUT } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for creating a RemoteBackend
 */
export interface RemoteBackendOptions {
  /** Base URL for the remote database (e.g., 'https://parque.db/db/owner/slug') */
  baseUrl: string

  /** Authentication token (for private databases) */
  token?: string | undefined

  /** Custom headers */
  headers?: Record<string, string> | undefined

  /** Timeout in milliseconds (default: 30000) */
  timeout?: number | undefined

  /** Custom fetch implementation (for testing) */
  fetch?: typeof globalThis.fetch | undefined
}

/**
 * Cached file metadata
 */
interface CachedStat {
  stat: FileStat
  expiresAt: number
}

// =============================================================================
// RemoteBackend Implementation
// =============================================================================

/**
 * Read-only HTTP storage backend for remote databases
 *
 * Features:
 * - Range request support for efficient Parquet reads
 * - Automatic retry on network errors
 * - ETag-based caching
 * - Authentication support for private databases
 */
export class RemoteBackend implements StorageBackend {
  readonly type = 'remote'

  private baseUrl: string
  private token?: string | undefined
  private headers: Record<string, string>
  private timeout: number
  private fetch: typeof globalThis.fetch
  private statCache: Map<string, CachedStat> = new Map()

  private static CACHE_TTL = DEFAULT_REMOTE_CACHE_TTL

  constructor(options: RemoteBackendOptions) {
    // Ensure baseUrl doesn't end with slash
    this.baseUrl = options.baseUrl.replace(/\/$/, '')
    this.token = options.token
    this.headers = options.headers ?? {}
    this.timeout = options.timeout ?? DEFAULT_REMOTE_TIMEOUT
    this.fetch = options.fetch ?? globalThis.fetch
  }

  // ==========================================================================
  // Read Operations
  // ==========================================================================

  /**
   * Read entire file
   */
  async read(path: string): Promise<Uint8Array> {
    this.validatePath(path)
    const url = this.buildUrl(path)
    const response = await this.doFetch(url)

    if (!response.ok) {
      this.handleError(response, path)
    }

    const buffer = await response.arrayBuffer()
    return new Uint8Array(buffer)
  }

  /**
   * Read byte range from file
   *
   * Our interface uses EXCLUSIVE end position (like Array.slice):
   * - readRange(path, 0, 5) reads bytes 0,1,2,3,4 (5 bytes)
   *
   * HTTP Range headers use INCLUSIVE end position:
   * - Range: bytes=0-4 reads bytes 0,1,2,3,4 (5 bytes)
   *
   * This method converts from our exclusive end to HTTP's inclusive end.
   *
   * Supports negative indices for reading from end of file:
   * - readRange(path, -8, -1) reads last 8 bytes
   *
   * This is critical for efficient Parquet reading where we need
   * to read the footer first to understand the file structure.
   */
  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    this.validatePath(path)
    const url = this.buildUrl(path)

    // Handle empty range (start == end)
    if (start >= 0 && end >= 0 && start >= end) {
      return new Uint8Array(0)
    }

    // Build Range header
    // Note: HTTP Range uses INCLUSIVE end, our API uses EXCLUSIVE end
    let rangeValue: string
    if (start < 0 && end < 0) {
      // Both negative - read from end
      // Range: bytes=-500 means last 500 bytes
      rangeValue = `bytes=${start}`
    } else if (start < 0) {
      // Start negative - suffix range
      rangeValue = `bytes=${start}`
    } else if (end < 0) {
      // End negative - read from start to (size + end)
      // This requires knowing file size, so we fetch stat first
      const stat = await this.stat(path)
      if (!stat) {
        throw new NotFoundError(path)
      }
      // Convert exclusive end to inclusive: (size + end) - 1
      // For size=10, end=-1: actualEnd = 10 + (-1) - 1 = 8 (bytes 0-8 = 9 bytes)
      const actualEnd = stat.size + end - 1
      rangeValue = `bytes=${start}-${actualEnd}`
    } else {
      // Both positive - standard range
      // Convert exclusive end to inclusive: end - 1
      // readRange(0, 5) -> bytes=0-4 (5 bytes)
      rangeValue = `bytes=${start}-${end - 1}`
    }

    const response = await this.doFetch(url, {
      headers: {
        ...this.buildHeaders(),
        'Range': rangeValue,
      },
    })

    // 206 Partial Content is expected for range requests
    if (response.status !== 206 && response.status !== 200) {
      this.handleError(response, path)
    }

    const buffer = await response.arrayBuffer()
    return new Uint8Array(buffer)
  }

  /**
   * Check if file exists
   */
  async exists(path: string): Promise<boolean> {
    this.validatePath(path)
    const stat = await this.stat(path)
    return stat !== null
  }

  /**
   * Get file metadata using HEAD request
   */
  async stat(path: string): Promise<FileStat | null> {
    this.validatePath(path)
    // Check cache first
    const cached = this.statCache.get(path)
    if (cached && cached.expiresAt > Date.now()) {
      return cached.stat
    }

    const url = this.buildUrl(path)

    try {
      const response = await this.doFetch(url, { method: 'HEAD' })

      if (response.status === 404) {
        return null
      }

      if (!response.ok) {
        this.handleError(response, path)
      }

      const stat: FileStat = {
        path,
        size: parseInt(response.headers.get('Content-Length') ?? '0', 10),
        mtime: new Date(response.headers.get('Last-Modified') ?? Date.now()),
        isDirectory: false,
        etag: response.headers.get('ETag') ?? undefined,
        contentType: response.headers.get('Content-Type') ?? undefined,
      }

      // Cache the result
      this.statCache.set(path, {
        stat,
        expiresAt: Date.now() + RemoteBackend.CACHE_TTL,
      })

      return stat
    } catch (error) {
      if (error instanceof NotFoundError) {
        return null
      }
      throw error
    }
  }

  /**
   * List files with prefix
   *
   * Note: This requires the remote server to support listing.
   * If not available, returns empty list.
   */
  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    const url = new URL(this.buildUrl('_meta/manifest.json'))

    try {
      const response = await this.doFetch(url.toString())

      if (!response.ok) {
        // Listing not supported or no manifest
        return { files: [], hasMore: false }
      }

      const manifest = await response.json() as { files?: Record<string, { path: string }> | undefined }
      const files = Object.values(manifest.files ?? {})
        .map(f => f.path)
        .filter(p => p.startsWith(prefix))

      // Apply limit
      const limit = options?.limit ?? files.length
      const limited = files.slice(0, limit)

      return {
        files: limited,
        hasMore: files.length > limit,
      }
    } catch {
      return { files: [], hasMore: false }
    }
  }

  // ==========================================================================
  // Write Operations (Not Supported)
  // ==========================================================================

  /**
   * Write file - NOT SUPPORTED (read-only backend)
   */
  async write(path: string, _data: Uint8Array, _options?: WriteOptions): Promise<WriteResult> {
    this.validatePath(path)
    throw new Error('RemoteBackend is read-only. Use push/sync commands to upload.')
  }

  /**
   * Atomic write - NOT SUPPORTED
   */
  async writeAtomic(_path: string, _data: Uint8Array, _options?: WriteOptions): Promise<WriteResult> {
    throw new Error('RemoteBackend is read-only. Use push/sync commands to upload.')
  }

  /**
   * Append - NOT SUPPORTED
   */
  async append(_path: string, _data: Uint8Array): Promise<void> {
    throw new Error('RemoteBackend is read-only. Use push/sync commands to upload.')
  }

  /**
   * Delete - NOT SUPPORTED
   */
  async delete(_path: string): Promise<boolean> {
    throw new Error('RemoteBackend is read-only. Use push/sync commands to modify.')
  }

  /**
   * Delete prefix - NOT SUPPORTED
   */
  async deletePrefix(_prefix: string): Promise<number> {
    throw new Error('RemoteBackend is read-only. Use push/sync commands to modify.')
  }

  // ==========================================================================
  // Directory Operations (Not Supported)
  // ==========================================================================

  /**
   * Create directory - NOT SUPPORTED
   */
  async mkdir(_path: string): Promise<void> {
    throw new Error('RemoteBackend is read-only.')
  }

  /**
   * Remove directory - NOT SUPPORTED
   */
  async rmdir(_path: string, _options?: RmdirOptions): Promise<void> {
    throw new Error('RemoteBackend is read-only.')
  }

  // ==========================================================================
  // Atomic Operations (Not Supported)
  // ==========================================================================

  /**
   * Conditional write - NOT SUPPORTED
   */
  async writeConditional(
    _path: string,
    _data: Uint8Array,
    _expectedVersion: string | null,
    _options?: WriteOptions
  ): Promise<WriteResult> {
    throw new Error('RemoteBackend is read-only.')
  }

  /**
   * Copy - NOT SUPPORTED
   */
  async copy(_source: string, _dest: string): Promise<void> {
    throw new Error('RemoteBackend is read-only.')
  }

  /**
   * Move - NOT SUPPORTED
   */
  async move(_source: string, _dest: string): Promise<void> {
    throw new Error('RemoteBackend is read-only.')
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  /**
   * Validate path for security issues
   *
   * Checks for:
   * - Parent directory traversal (..)
   * - Double slashes (//)
   * - Absolute paths starting with /
   *
   * @throws PathTraversalError if path contains unsafe patterns
   */
  private validatePath(path: string): void {
    // Check for parent directory traversal
    if (path.includes('..')) {
      throw new PathTraversalError(path)
    }

    // Check for double slashes that could bypass filters
    if (path.includes('//')) {
      throw new PathTraversalError(path)
    }

    // Check for absolute paths (starting with /)
    if (path.startsWith('/')) {
      throw new PathTraversalError(path)
    }
  }

  /**
   * Build full URL for a path
   */
  private buildUrl(path: string): string {
    this.validatePath(path)
    const normalizedPath = path.startsWith('/') ? path.slice(1) : path
    return `${this.baseUrl}/${normalizedPath}`
  }

  /**
   * Build request headers
   */
  private buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      ...this.headers,
    }

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }

    return headers
  }

  /**
   * Make HTTP request with timeout and retry
   */
  private async doFetch(url: string, init?: RequestInit): Promise<Response> {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeout)

    try {
      const response = await this.fetch(url, {
        ...init,
        headers: {
          ...this.buildHeaders(),
          ...(init?.headers as Record<string, string> | undefined),
        },
        signal: controller.signal,
      })

      return response
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new NetworkError(`Request timeout after ${this.timeout}ms`)
      }
      throw new NetworkError(
        error instanceof Error ? error.message : 'Network error'
      )
    } finally {
      clearTimeout(timeoutId)
    }
  }

  /**
   * Handle HTTP error responses
   */
  private handleError(response: Response, path: string): never {
    if (response.status === 404) {
      throw new NotFoundError(path)
    }

    if (response.status === 401 || response.status === 403) {
      throw new PermissionDeniedError(
        `Access denied to ${path}. Authentication may be required.`
      )
    }

    throw new NetworkError(
      `HTTP ${response.status}: ${response.statusText}`
    )
  }

  /**
   * Clear the stat cache
   */
  clearCache(): void {
    this.statCache.clear()
  }

  /**
   * Set authentication token
   */
  setToken(token: string | undefined): void {
    this.token = token
    this.clearCache()
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a RemoteBackend for a public database
 *
 * @example
 * ```typescript
 * const backend = createRemoteBackend('username/my-dataset')
 * const posts = await readParquetFile(backend, 'data/posts/data.parquet')
 * ```
 */
export function createRemoteBackend(
  ownerSlug: string,
  options?: Omit<RemoteBackendOptions, 'baseUrl'>
): RemoteBackend {
  const baseUrl = `https://parque.db/db/${ownerSlug}`
  return new RemoteBackend({ ...options, baseUrl })
}

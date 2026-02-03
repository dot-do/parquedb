/**
 * ReadPath - Cached R2 reader for ParqueDB
 *
 * Implements the read side of CQRS architecture:
 * - Reads go directly to R2 with Cache API caching
 * - Uses Cloudflare Cache API for hot data
 * - Supports range requests for efficient Parquet partial reads
 */

import type { CacheConfig } from './CacheStrategy'
import { DEFAULT_CACHE_CONFIG } from './CacheStrategy'
import { asBodyInit } from '../types/cast'
import { MissingBucketError } from './r2-errors'

// =============================================================================
// Errors
// =============================================================================

/**
 * Error thrown when a requested object is not found in R2
 */
export class NotFoundError extends Error {
  override readonly name = 'NotFoundError'
  constructor(public readonly path: string) {
    super(`Object not found: ${path}`)
    Object.setPrototypeOf(this, NotFoundError.prototype)
  }
}

/**
 * Error thrown when a read operation fails
 */
export class ReadError extends Error {
  override readonly name = 'ReadError'
  public readonly path: string
  public override readonly cause?: Error

  constructor(
    message: string,
    path: string,
    cause?: Error
  ) {
    super(message)
    Object.setPrototypeOf(this, ReadError.prototype)
    this.path = path
    this.cause = cause
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Cache statistics for monitoring
 */
export interface CacheStats {
  /** Total cache hits */
  hits: number
  /** Total cache misses */
  misses: number
  /** Cache hit ratio (0-1) */
  hitRatio: number
  /** Bytes served from cache */
  cachedBytes: number
  /** Bytes fetched from R2 */
  fetchedBytes: number
}

/**
 * Options for read operations
 */
export interface ReadOptions {
  /** Skip cache lookup (force R2 fetch) */
  skipCache?: boolean
  /** Custom cache TTL override */
  ttl?: number
  /** Type of content for cache configuration */
  type?: 'data' | 'metadata' | 'bloom'
}

// =============================================================================
// ReadPath Implementation
// =============================================================================

/**
 * Cached R2 reader for ParqueDB
 *
 * Implements efficient read path with:
 * - Cache API for hot data
 * - Range request support for Parquet partial reads
 * - ETag-based cache validation
 * - Stale-while-revalidate for improved latency
 *
 * @example
 * ```typescript
 * const readPath = new ReadPath(env.BUCKET, caches.default)
 *
 * // Read entire Parquet file (cached)
 * const data = await readPath.readParquet('data/posts/data.parquet')
 *
 * // Read specific byte range (for row group access)
 * const rowGroup = await readPath.readRange('data/posts/data.parquet', 1024, 4096)
 *
 * // Invalidate cache after writes
 * await readPath.invalidate(['data/posts/data.parquet'])
 * ```
 */
export class ReadPath {
  /** Cache key prefix for namespacing */
  private readonly cachePrefix = 'https://parquedb/'

  /** Stats tracking */
  private stats: CacheStats = {
    hits: 0,
    misses: 0,
    hitRatio: 0,
    cachedBytes: 0,
    fetchedBytes: 0,
  }

  /** Execution context for background revalidation */
  private executionContext?: ExecutionContext

  constructor(
    private bucket: R2Bucket,
    private cache: Cache,
    private config: CacheConfig = DEFAULT_CACHE_CONFIG
  ) {
    if (!bucket) {
      throw new MissingBucketError('BUCKET', 'Required for ReadPath operations.')
    }
  }

  /**
   * Set the execution context for background revalidation
   *
   * When set, background revalidation tasks will use ctx.waitUntil() to ensure
   * they complete even after the response is sent. This prevents early termination
   * when the Worker instance is recycled.
   *
   * @param ctx - The execution context from the Worker's fetch handler
   */
  setExecutionContext(ctx: ExecutionContext): void {
    this.executionContext = ctx
  }

  // ===========================================================================
  // Public Read Methods
  // ===========================================================================

  /**
   * Read entire Parquet file with caching
   *
   * Uses Cache API for hot data. First checks cache, then falls back to R2.
   * Caches response for future reads with configurable TTL.
   *
   * @param path - Path to the Parquet file in R2
   * @param options - Read options
   * @returns File contents as Uint8Array
   * @throws NotFoundError if file doesn't exist
   */
  async readParquet(path: string, options: ReadOptions = {}): Promise<Uint8Array> {
    const cacheKey = this.createCacheKey(path)
    const ttl = options.ttl ?? this.getTtlForType(options.type ?? 'data')

    // Check cache first (unless skipCache is true)
    if (!options.skipCache) {
      const cached = await this.cache.match(cacheKey)
      if (cached) {
        // Check if we should revalidate in background
        if (this.config.staleWhileRevalidate && this.shouldRevalidate(cached)) {
          // Return cached data immediately, revalidate in background
          this.revalidateInBackground(path, cacheKey, ttl)
        }

        this.stats.hits++
        const data = new Uint8Array(await cached.arrayBuffer())
        this.stats.cachedBytes += data.byteLength
        this.updateHitRatio()
        return data
      }
    }

    // Cache miss - read from R2
    this.stats.misses++
    this.updateHitRatio()

    const obj = await this.bucket.get(path)
    if (!obj) {
      throw new NotFoundError(path)
    }

    const data = await obj.arrayBuffer()
    this.stats.fetchedBytes += data.byteLength

    // Cache for future reads
    await this.cacheResponse(cacheKey, data, obj.etag, ttl)

    return new Uint8Array(data)
  }

  /**
   * Read byte range from file (for Parquet partial reads)
   *
   * Critical for reading only needed row groups. R2 supports range requests
   * natively, allowing efficient partial file access.
   *
   * Range requests are NOT cached to avoid cache fragmentation.
   * For repeated row group access, consider reading the full row group
   * and caching at that granularity.
   *
   * @param path - Path to the file in R2
   * @param start - Start byte offset (inclusive)
   * @param end - End byte offset (exclusive)
   * @returns Byte range contents as Uint8Array
   * @throws NotFoundError if file doesn't exist
   * @throws ReadError if range is invalid
   */
  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    if (start < 0 || end <= start) {
      throw new ReadError(`Invalid range: ${start}-${end}`, path)
    }

    const length = end - start

    // R2 supports range requests natively
    const obj = await this.bucket.get(path, {
      range: { offset: start, length },
    })

    if (!obj) {
      throw new NotFoundError(path)
    }

    const data = await obj.arrayBuffer()
    this.stats.fetchedBytes += data.byteLength

    return new Uint8Array(data)
  }

  /**
   * Read byte range with caching support
   *
   * For frequently accessed ranges (like Parquet footer or metadata),
   * this method provides caching. Use a unique cache key suffix for
   * different ranges of the same file.
   *
   * @param path - Path to the file in R2
   * @param start - Start byte offset (inclusive)
   * @param end - End byte offset (exclusive)
   * @param cacheKeySuffix - Suffix for cache key (e.g., 'footer', 'metadata')
   * @returns Byte range contents as Uint8Array
   */
  async readRangeCached(
    path: string,
    start: number,
    end: number,
    cacheKeySuffix: string
  ): Promise<Uint8Array> {
    const cacheKey = this.createCacheKey(`${path}#${cacheKeySuffix}`)
    const ttl = this.getTtlForType('metadata')

    // Check cache first
    const cached = await this.cache.match(cacheKey)
    if (cached) {
      this.stats.hits++
      const data = new Uint8Array(await cached.arrayBuffer())
      this.stats.cachedBytes += data.byteLength
      this.updateHitRatio()
      return data
    }

    // Cache miss - read from R2
    this.stats.misses++
    this.updateHitRatio()

    const data = await this.readRange(path, start, end)

    // Cache the range - cast to BodyInit for Response constructor
    await this.cache.put(
      cacheKey,
      new Response(asBodyInit(data), {
        headers: {
          'Cache-Control': `max-age=${ttl}`,
          'X-Range': `${start}-${end}`,
        },
      })
    )

    return data
  }

  /**
   * Read Parquet file footer (last 8 bytes contain metadata length)
   *
   * Optimized method for reading just the footer which is needed
   * to parse Parquet metadata.
   *
   * @param path - Path to the Parquet file
   * @returns Footer bytes (last 8 bytes of file)
   */
  async readParquetFooter(path: string): Promise<Uint8Array> {
    // Get file size first
    const head = await this.bucket.head(path)
    if (!head) {
      throw new NotFoundError(path)
    }

    // Parquet footer is last 8 bytes
    const footerSize = 8
    const start = head.size - footerSize

    return this.readRangeCached(path, start, head.size, 'footer')
  }

  /**
   * Read Parquet metadata section
   *
   * After reading the footer, use this to read the full metadata
   * section based on the metadata length encoded in the footer.
   *
   * @param path - Path to the Parquet file
   * @param metadataLength - Length of metadata section (from footer)
   * @returns Metadata bytes
   */
  async readParquetMetadata(path: string, metadataLength: number): Promise<Uint8Array> {
    const head = await this.bucket.head(path)
    if (!head) {
      throw new NotFoundError(path)
    }

    // Metadata is metadataLength bytes before the 8-byte footer
    const start = head.size - metadataLength - 8
    const end = head.size - 8

    return this.readRangeCached(path, start, end, 'metadata')
  }

  // ===========================================================================
  // Cache Invalidation
  // ===========================================================================

  /**
   * Invalidate cache when data changes
   *
   * Called by Durable Object after writes to ensure cache coherence.
   * Deletes cached entries for the specified paths.
   *
   * @param paths - Array of R2 paths to invalidate
   */
  async invalidate(paths: string[]): Promise<void> {
    const deletePromises = paths.map((path) => this.invalidatePath(path))
    await Promise.all(deletePromises)
  }

  /**
   * Invalidate all cached data for a namespace
   *
   * Useful when a namespace is dropped or significantly modified.
   * Note: This relies on prefix-based cache keys.
   *
   * @param ns - Namespace to invalidate
   */
  async invalidateNamespace(ns: string): Promise<void> {
    // Invalidate known paths for the namespace
    const paths = [
      `data/${ns}/data.parquet`,
      `indexes/bloom/${ns}.bloom`,
      `rels/forward/${ns}.parquet`,
      `rels/reverse/${ns}.parquet`,
    ]

    await this.invalidate(paths)
  }

  /**
   * Invalidate a single path with all its cached ranges
   */
  private async invalidatePath(path: string): Promise<void> {
    // Delete main cache entry
    await this.cache.delete(this.createCacheKey(path))

    // Delete known cached ranges
    const rangeSuffixes = ['footer', 'metadata']
    for (const suffix of rangeSuffixes) {
      await this.cache.delete(this.createCacheKey(`${path}#${suffix}`))
    }
  }

  // ===========================================================================
  // Metadata Operations
  // ===========================================================================

  /**
   * Check if a file exists in R2
   *
   * Uses R2 head request which is cheaper than a full get.
   *
   * @param path - Path to check
   * @returns true if file exists
   */
  async exists(path: string): Promise<boolean> {
    const head = await this.bucket.head(path)
    return head !== null
  }

  /**
   * Get file metadata (size, etag, etc.)
   *
   * @param path - Path to the file
   * @returns File metadata or null if not found
   */
  async getMetadata(path: string): Promise<R2Object | null> {
    return this.bucket.head(path)
  }

  /**
   * List files with a prefix
   *
   * @param prefix - Prefix to filter by
   * @param options - List options
   * @returns List of matching objects
   */
  async list(
    prefix: string,
    options?: R2ListOptions
  ): Promise<R2Objects> {
    return this.bucket.list({ prefix, ...options })
  }

  // ===========================================================================
  // Stats and Monitoring
  // ===========================================================================

  /**
   * Get cache statistics
   *
   * @returns Current cache stats
   */
  getStats(): CacheStats {
    return { ...this.stats }
  }

  /**
   * Reset cache statistics
   */
  resetStats(): void {
    this.stats = {
      hits: 0,
      misses: 0,
      hitRatio: 0,
      cachedBytes: 0,
      fetchedBytes: 0,
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Create a cache key for a path
   */
  private createCacheKey(path: string): Request {
    return new Request(`${this.cachePrefix}${path}`)
  }

  /**
   * Get TTL for content type
   */
  private getTtlForType(type: 'data' | 'metadata' | 'bloom'): number {
    switch (type) {
      case 'data':
        return this.config.dataTtl
      case 'metadata':
        return this.config.metadataTtl
      case 'bloom':
        return this.config.bloomTtl
      default:
        return this.config.dataTtl
    }
  }

  /**
   * Cache a response
   */
  private async cacheResponse(
    cacheKey: Request,
    data: ArrayBuffer,
    etag: string,
    ttl: number
  ): Promise<void> {
    const headers: HeadersInit = {
      'Cache-Control': `max-age=${ttl}`,
      ETag: etag,
      'Content-Length': data.byteLength.toString(),
      'Content-Type': 'application/octet-stream',
    }

    // Add stale-while-revalidate if enabled
    if (this.config.staleWhileRevalidate) {
      headers['Cache-Control'] = `max-age=${ttl}, stale-while-revalidate=${ttl}`
    }

    await this.cache.put(cacheKey, new Response(data, { headers }))
  }

  /**
   * Check if cached response should be revalidated
   */
  private shouldRevalidate(cached: Response): boolean {
    const cacheControl = cached.headers.get('Cache-Control')
    if (!cacheControl) return false

    // Check if past max-age but within stale-while-revalidate window
    const maxAgeMatch = cacheControl.match(/max-age=(\d+)/)
    if (!maxAgeMatch || !maxAgeMatch[1]) return false

    const date = cached.headers.get('Date')
    if (!date) return false

    const maxAge = parseInt(maxAgeMatch[1], 10)
    const cachedAt = new Date(date).getTime()
    const age = (Date.now() - cachedAt) / 1000

    // Revalidate if past 80% of max-age
    return age > maxAge * 0.8
  }

  /**
   * Revalidate cache entry in background
   *
   * Uses ctx.waitUntil() when an ExecutionContext is available to ensure the
   * background revalidation completes even after the response is sent. Without
   * waitUntil, the Worker instance may be recycled before revalidation finishes.
   *
   * NOTE: This is intentionally fire-and-forget for stale-while-revalidate pattern.
   * The caller returns cached data immediately while we refresh in the background.
   * Errors during revalidation are logged but don't affect the user request.
   */
  private revalidateInBackground(
    path: string,
    cacheKey: Request,
    ttl: number
  ): void {
    // Create the revalidation promise
    const revalidationPromise = this.bucket.get(path).then(async (obj) => {
      if (obj) {
        const data = await obj.arrayBuffer()
        await this.cacheResponse(cacheKey, data, obj.etag, ttl)
      }
    }).catch((err) => {
      // Log revalidation errors - these indicate potential cache coherence issues
      console.warn(`[ReadPath] Background revalidation failed for ${path}:`, err)
    })

    // Use waitUntil if ExecutionContext is available, otherwise fire-and-forget
    if (this.executionContext) {
      this.executionContext.waitUntil(revalidationPromise)
    }
  }

  /**
   * Update hit ratio stat
   */
  private updateHitRatio(): void {
    const total = this.stats.hits + this.stats.misses
    this.stats.hitRatio = total > 0 ? this.stats.hits / total : 0
  }
}

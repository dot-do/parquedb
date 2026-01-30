/**
 * CacheStrategy - Cache configuration for ParqueDB
 *
 * Defines caching policies for different content types:
 * - Data files: Parquet row group data (shorter TTL, frequently updated)
 * - Metadata: Parquet schema and file metadata (longer TTL, rarely changes)
 * - Bloom filters: ID lookup filters (longest TTL, stable after creation)
 */

// =============================================================================
// Cache Configuration
// =============================================================================

/**
 * Cache configuration for ParqueDB
 */
export interface CacheConfig {
  /** TTL for Parquet data files (seconds) */
  dataTtl: number

  /** TTL for metadata/schema (seconds) */
  metadataTtl: number

  /** TTL for bloom filters (seconds) */
  bloomTtl: number

  /** Use stale-while-revalidate for improved latency */
  staleWhileRevalidate: boolean

  /** Maximum size to cache (bytes, 0 = no limit) */
  maxCacheSize?: number

  /** Enable compression for cached responses */
  compression?: boolean
}

/**
 * Default cache configuration
 *
 * Balanced for typical ParqueDB workloads:
 * - Data: 1 minute (frequent updates possible)
 * - Metadata: 5 minutes (changes with schema updates)
 * - Bloom: 10 minutes (stable after creation)
 */
export const DEFAULT_CACHE_CONFIG: CacheConfig = {
  dataTtl: 60, // 1 minute
  metadataTtl: 300, // 5 minutes
  bloomTtl: 600, // 10 minutes
  staleWhileRevalidate: true,
}

/**
 * Aggressive caching for read-heavy workloads
 */
export const READ_HEAVY_CACHE_CONFIG: CacheConfig = {
  dataTtl: 300, // 5 minutes
  metadataTtl: 900, // 15 minutes
  bloomTtl: 1800, // 30 minutes
  staleWhileRevalidate: true,
}

/**
 * Conservative caching for write-heavy workloads
 */
export const WRITE_HEAVY_CACHE_CONFIG: CacheConfig = {
  dataTtl: 15, // 15 seconds
  metadataTtl: 60, // 1 minute
  bloomTtl: 120, // 2 minutes
  staleWhileRevalidate: false,
}

/**
 * No caching (for development/debugging)
 */
export const NO_CACHE_CONFIG: CacheConfig = {
  dataTtl: 0,
  metadataTtl: 0,
  bloomTtl: 0,
  staleWhileRevalidate: false,
}

// =============================================================================
// Cache Content Types
// =============================================================================

/**
 * Types of cacheable content
 */
export type CacheContentType = 'data' | 'metadata' | 'bloom' | 'index' | 'schema'

/**
 * Extended configuration per content type
 */
export interface ContentTypeCacheConfig {
  /** TTL in seconds */
  ttl: number

  /** Use stale-while-revalidate */
  staleWhileRevalidate: boolean

  /** Priority (higher = more likely to stay in cache) */
  priority: 'low' | 'medium' | 'high'

  /** Whether to cache range requests for this type */
  cacheRanges: boolean
}

/**
 * Full cache configuration with per-type settings
 */
export interface AdvancedCacheConfig {
  /** Default settings */
  defaults: CacheConfig

  /** Per-type overrides */
  overrides?: Partial<Record<CacheContentType, Partial<ContentTypeCacheConfig>>>
}

// =============================================================================
// CacheStrategy Implementation
// =============================================================================

/**
 * Cache strategy helper for ParqueDB
 *
 * Provides utilities for:
 * - Generating appropriate cache headers
 * - Determining revalidation needs
 * - Cache key generation
 * - TTL management
 *
 * @example
 * ```typescript
 * const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
 *
 * // Get cache headers for data files
 * const headers = await strategy.getCacheHeaders('data')
 *
 * // Check if cached response should be revalidated
 * const needsRevalidation = await strategy.shouldRevalidate(cachedResponse)
 * ```
 */
export class CacheStrategy {
  private readonly config: AdvancedCacheConfig

  constructor(config: CacheConfig | AdvancedCacheConfig) {
    if ('defaults' in config) {
      this.config = config
    } else {
      this.config = { defaults: config }
    }
  }

  // ===========================================================================
  // Header Generation
  // ===========================================================================

  /**
   * Get cache headers for a content type
   *
   * @param type - Type of content being cached
   * @param options - Additional options
   * @returns Headers for cache response
   */
  async getCacheHeaders(
    type: CacheContentType,
    options?: { etag?: string; size?: number }
  ): Promise<Headers> {
    const headers = new Headers()
    const ttl = this.getTtl(type)

    // Set Cache-Control
    let cacheControl = `public, max-age=${ttl}`
    if (this.shouldUseStaleWhileRevalidate(type)) {
      cacheControl += `, stale-while-revalidate=${ttl}`
    }
    headers.set('Cache-Control', cacheControl)

    // Set content type
    headers.set('Content-Type', this.getContentType(type))

    // Set ETag if provided
    if (options?.etag) {
      headers.set('ETag', options.etag)
    }

    // Set Content-Length if provided
    if (options?.size !== undefined) {
      headers.set('Content-Length', options.size.toString())
    }

    // Set custom headers for debugging
    headers.set('X-ParqueDB-Cache-Type', type)
    headers.set('X-ParqueDB-Cache-TTL', ttl.toString())

    return headers
  }

  /**
   * Get headers for a non-cacheable response
   */
  getNoCacheHeaders(): Headers {
    const headers = new Headers()
    headers.set('Cache-Control', 'no-store, no-cache, must-revalidate')
    headers.set('Pragma', 'no-cache')
    return headers
  }

  // ===========================================================================
  // Revalidation
  // ===========================================================================

  /**
   * Check if a cached response should be revalidated
   *
   * Uses age-based heuristic: revalidate when past 80% of max-age
   *
   * @param cached - Cached response to check
   * @returns true if should revalidate
   */
  async shouldRevalidate(cached: Response): Promise<boolean> {
    const cacheControl = cached.headers.get('Cache-Control')
    if (!cacheControl) return true

    const maxAgeMatch = cacheControl.match(/max-age=(\d+)/)
    if (!maxAgeMatch || !maxAgeMatch[1]) return true

    const maxAge = parseInt(maxAgeMatch[1], 10)
    const age = this.getResponseAge(cached)

    // Revalidate if past 80% of max-age
    return age > maxAge * 0.8
  }

  /**
   * Check if a cached response is stale (past max-age)
   *
   * @param cached - Cached response to check
   * @returns true if stale
   */
  isStale(cached: Response): boolean {
    const age = this.getResponseAge(cached)
    const cacheControl = cached.headers.get('Cache-Control')
    if (!cacheControl) return true

    const maxAgeMatch = cacheControl.match(/max-age=(\d+)/)
    if (!maxAgeMatch || !maxAgeMatch[1]) return true

    return age > parseInt(maxAgeMatch[1], 10)
  }

  /**
   * Check if a stale response can still be used (within stale-while-revalidate window)
   *
   * @param cached - Cached response to check
   * @returns true if usable while stale
   */
  canUseWhileStale(cached: Response): boolean {
    if (!this.isStale(cached)) return true

    const cacheControl = cached.headers.get('Cache-Control')
    if (!cacheControl) return false

    const swrMatch = cacheControl.match(/stale-while-revalidate=(\d+)/)
    if (!swrMatch || !swrMatch[1]) return false

    const maxAgeMatch = cacheControl.match(/max-age=(\d+)/)
    if (!maxAgeMatch || !maxAgeMatch[1]) return false

    const age = this.getResponseAge(cached)
    const maxAge = parseInt(maxAgeMatch[1], 10)
    const swr = parseInt(swrMatch[1], 10)

    return age <= maxAge + swr
  }

  // ===========================================================================
  // TTL Management
  // ===========================================================================

  /**
   * Get TTL for a content type
   *
   * @param type - Content type
   * @returns TTL in seconds
   */
  getTtl(type: CacheContentType): number {
    // Check for override
    const override = this.config.overrides?.[type]?.ttl
    if (override !== undefined) {
      return override
    }

    // Use default based on type
    switch (type) {
      case 'data':
        return this.config.defaults.dataTtl
      case 'metadata':
      case 'schema':
        return this.config.defaults.metadataTtl
      case 'bloom':
      case 'index':
        return this.config.defaults.bloomTtl
      default:
        return this.config.defaults.dataTtl
    }
  }

  /**
   * Check if stale-while-revalidate should be used for a type
   */
  private shouldUseStaleWhileRevalidate(type: CacheContentType): boolean {
    const override = this.config.overrides?.[type]?.staleWhileRevalidate
    if (override !== undefined) {
      return override
    }
    return this.config.defaults.staleWhileRevalidate
  }

  // ===========================================================================
  // Cache Key Generation
  // ===========================================================================

  /**
   * Generate a cache key for a path
   *
   * @param path - R2 path
   * @param options - Key generation options
   * @returns Cache key as Request
   */
  createCacheKey(
    path: string,
    options?: { version?: string; range?: { start: number; end: number } }
  ): Request {
    let url = `https://parquedb/${path}`

    // Add version to key if provided (for versioned caching)
    if (options?.version) {
      url += `?v=${options.version}`
    }

    // Add range to key if provided (for range caching)
    if (options?.range) {
      url += `#${options.range.start}-${options.range.end}`
    }

    return new Request(url)
  }

  /**
   * Parse a cache key back to path and options
   *
   * @param key - Cache key Request
   * @returns Parsed path and options
   */
  parseCacheKey(key: Request): {
    path: string
    version?: string
    range?: { start: number; end: number }
  } {
    const url = new URL(key.url)
    const path = url.pathname.replace(/^\//, '')
    const versionParam = url.searchParams.get('v')
    const version = versionParam ?? undefined

    let range: { start: number; end: number } | undefined
    if (url.hash) {
      const parts = url.hash.slice(1).split('-')
      const start = parseInt(parts[0] ?? '', 10)
      const end = parseInt(parts[1] ?? '', 10)
      if (!isNaN(start) && !isNaN(end)) {
        range = { start, end }
      }
    }

    return { path, version, range }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Get the age of a cached response in seconds
   */
  private getResponseAge(cached: Response): number {
    // Check Age header first (set by CDN)
    const ageHeader = cached.headers.get('Age')
    if (ageHeader) {
      const age = parseInt(ageHeader, 10)
      if (!isNaN(age)) return age
    }

    // Calculate from Date header
    const dateHeader = cached.headers.get('Date')
    if (dateHeader) {
      const cachedAt = new Date(dateHeader).getTime()
      if (!isNaN(cachedAt)) {
        return (Date.now() - cachedAt) / 1000
      }
    }

    // Unknown age - assume stale
    return Infinity
  }

  /**
   * Get MIME type for content type
   */
  private getContentType(type: CacheContentType): string {
    switch (type) {
      case 'data':
        return 'application/octet-stream'
      case 'metadata':
      case 'schema':
        return 'application/json'
      case 'bloom':
        return 'application/octet-stream'
      case 'index':
        return 'application/octet-stream'
      default:
        return 'application/octet-stream'
    }
  }
}

// =============================================================================
// Cache Utilities
// =============================================================================

/**
 * Create a CacheStrategy from environment configuration
 */
export function createCacheStrategy(env?: {
  CACHE_DATA_TTL?: string
  CACHE_METADATA_TTL?: string
  CACHE_BLOOM_TTL?: string
  CACHE_STALE_WHILE_REVALIDATE?: string
}): CacheStrategy {
  const config: CacheConfig = {
    dataTtl: env?.CACHE_DATA_TTL ? parseInt(env.CACHE_DATA_TTL, 10) : DEFAULT_CACHE_CONFIG.dataTtl,
    metadataTtl: env?.CACHE_METADATA_TTL
      ? parseInt(env.CACHE_METADATA_TTL, 10)
      : DEFAULT_CACHE_CONFIG.metadataTtl,
    bloomTtl: env?.CACHE_BLOOM_TTL
      ? parseInt(env.CACHE_BLOOM_TTL, 10)
      : DEFAULT_CACHE_CONFIG.bloomTtl,
    staleWhileRevalidate: env?.CACHE_STALE_WHILE_REVALIDATE !== 'false',
  }

  return new CacheStrategy(config)
}

/**
 * Determine content type from path
 */
export function getContentTypeFromPath(path: string): CacheContentType {
  if (path.includes('/data.parquet') || path.includes('.parquet')) {
    return 'data'
  }
  if (path.includes('.bloom')) {
    return 'bloom'
  }
  if (path.includes('/indexes/')) {
    return 'index'
  }
  if (path.includes('_meta/') || path.includes('schema')) {
    return 'schema'
  }
  return 'metadata'
}

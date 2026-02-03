/**
 * Rate Limiter Durable Object
 *
 * Implements distributed rate limiting using a sliding window algorithm.
 * Uses Cloudflare Durable Objects for consistent state across edge locations.
 *
 * Features:
 * - Sliding window rate limiting for accurate request counting
 * - Per-IP and per-API-key rate limiting
 * - Configurable limits for different endpoint types
 * - Returns remaining requests and reset time in headers
 *
 * @example
 * ```typescript
 * // Get rate limiter stub for an IP
 * const limiter = getRateLimiter(env, clientIP)
 * const { allowed, remaining, resetAt } = await limiter.checkLimit('public', 100, 60000)
 * ```
 */

import { DurableObject } from 'cloudflare:workers'

// Re-export utility types and functions for convenience
export {
  type RateLimitConfig,
  type RateLimitResult,
  DEFAULT_RATE_LIMITS,
  getClientId,
  buildRateLimitHeaders,
  buildRateLimitResponse,
} from './rate-limit-utils'

import {
  type RateLimitResult,
  DEFAULT_RATE_LIMITS,
} from './rate-limit-utils'

/**
 * Bindings expected by RateLimitDO
 */
export interface RateLimitEnv {
  // No specific bindings required - uses internal storage
}

// =============================================================================
// RateLimitDO Class
// =============================================================================

/**
 * Durable Object for distributed rate limiting
 *
 * Uses a sliding window algorithm that tracks individual request timestamps.
 * This provides more accurate rate limiting than a fixed window approach.
 *
 * The DO ID should be derived from the client identifier (IP or API key):
 * ```typescript
 * const rateLimitId = env.RATE_LIMITER.idFromName(`ip:${clientIP}`)
 * // or
 * const rateLimitId = env.RATE_LIMITER.idFromName(`key:${apiKey}`)
 * ```
 */
export class RateLimitDO extends DurableObject<RateLimitEnv> {
  /** SQLite storage for request tracking */
  private sql: SqlStorage

  /** Whether schema has been initialized */
  private initialized = false

  constructor(ctx: DurableObjectState, env: RateLimitEnv) {
    super(ctx, env)
    this.sql = ctx.storage.sql
  }

  /**
   * Initialize SQLite schema if not already done
   */
  private ensureInitialized(): void {
    if (this.initialized) return

    // Create table for tracking requests per endpoint type
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint_type TEXT NOT NULL,
        timestamp INTEGER NOT NULL
      )
    `)

    // Index for efficient cleanup and counting
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_requests_type_ts
      ON requests(endpoint_type, timestamp)
    `)

    this.initialized = true
  }

  /**
   * Check if a request is allowed under the rate limit
   *
   * @param endpointType - Type of endpoint (public, database, query, file)
   * @param maxRequests - Maximum requests allowed in the window (optional, uses default)
   * @param windowMs - Window duration in milliseconds (optional, uses default)
   * @returns Rate limit result with allowed status and metadata
   */
  async checkLimit(
    endpointType: string = 'default',
    maxRequests?: number,
    windowMs?: number
  ): Promise<RateLimitResult> {
    this.ensureInitialized()

    // Get configuration for this endpoint type
    const config = DEFAULT_RATE_LIMITS[endpointType] || DEFAULT_RATE_LIMITS.default!
    const limit = maxRequests ?? config.maxRequests
    const window = windowMs ?? config.windowMs

    const now = Date.now()
    const windowStart = now - window

    // Clean up old requests outside the window
    this.sql.exec(
      'DELETE FROM requests WHERE endpoint_type = ? AND timestamp < ?',
      endpointType,
      windowStart
    )

    // Count current requests in the window
    interface CountRow {
      [key: string]: SqlStorageValue
      count: number
    }
    const countResult = [...this.sql.exec<CountRow>(
      'SELECT COUNT(*) as count FROM requests WHERE endpoint_type = ? AND timestamp >= ?',
      endpointType,
      windowStart
    )]
    const currentCount = countResult[0]?.count ?? 0

    // Calculate reset time (end of current window)
    // Find the oldest request in the window to calculate accurate reset
    interface OldestRow {
      [key: string]: SqlStorageValue
      oldest: number | null
    }
    const oldestResult = [...this.sql.exec<OldestRow>(
      'SELECT MIN(timestamp) as oldest FROM requests WHERE endpoint_type = ? AND timestamp >= ?',
      endpointType,
      windowStart
    )]
    const oldestTimestamp = oldestResult[0]?.oldest ?? now
    const resetAt = (oldestTimestamp ?? now) + window

    // Check if request is allowed
    const allowed = currentCount < limit

    if (allowed) {
      // Record this request
      this.sql.exec(
        'INSERT INTO requests (endpoint_type, timestamp) VALUES (?, ?)',
        endpointType,
        now
      )
    }

    return {
      allowed,
      remaining: Math.max(0, limit - currentCount - (allowed ? 1 : 0)),
      resetAt,
      limit,
      current: currentCount + (allowed ? 1 : 0),
    }
  }

  /**
   * Get current rate limit status without consuming a request
   *
   * @param endpointType - Type of endpoint to check
   * @returns Current rate limit status
   */
  async getStatus(endpointType: string = 'default'): Promise<RateLimitResult> {
    this.ensureInitialized()

    const config = DEFAULT_RATE_LIMITS[endpointType] || DEFAULT_RATE_LIMITS.default!
    const now = Date.now()
    const windowStart = now - config.windowMs

    // Count current requests in the window
    interface CountRow {
      [key: string]: SqlStorageValue
      count: number
    }
    const countResult = [...this.sql.exec<CountRow>(
      'SELECT COUNT(*) as count FROM requests WHERE endpoint_type = ? AND timestamp >= ?',
      endpointType,
      windowStart
    )]
    const currentCount = countResult[0]?.count ?? 0

    // Find oldest request for reset calculation
    interface OldestRow {
      [key: string]: SqlStorageValue
      oldest: number | null
    }
    const oldestResult = [...this.sql.exec<OldestRow>(
      'SELECT MIN(timestamp) as oldest FROM requests WHERE endpoint_type = ? AND timestamp >= ?',
      endpointType,
      windowStart
    )]
    const oldestTimestamp = oldestResult[0]?.oldest ?? now
    const resetAt = (oldestTimestamp ?? now) + config.windowMs

    return {
      allowed: currentCount < config.maxRequests,
      remaining: Math.max(0, config.maxRequests - currentCount),
      resetAt,
      limit: config.maxRequests,
      current: currentCount,
    }
  }

  /**
   * Reset rate limits for this client (admin operation)
   *
   * @param endpointType - Optional: reset only for specific endpoint type
   */
  async reset(endpointType?: string): Promise<void> {
    this.ensureInitialized()

    if (endpointType) {
      this.sql.exec('DELETE FROM requests WHERE endpoint_type = ?', endpointType)
    } else {
      this.sql.exec('DELETE FROM requests')
    }
  }

  /**
   * Handle HTTP requests to the rate limiter DO
   * Provides a REST API for rate limit operations
   */
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    try {
      // GET /check/:endpointType - Check rate limit
      if (request.method === 'GET' && path.startsWith('/check/')) {
        const endpointType = path.slice('/check/'.length)
        const result = await this.checkLimit(endpointType)
        return Response.json(result)
      }

      // GET /status/:endpointType - Get status without consuming
      if (request.method === 'GET' && path.startsWith('/status/')) {
        const endpointType = path.slice('/status/'.length)
        const result = await this.getStatus(endpointType)
        return Response.json(result)
      }

      // POST /reset - Reset rate limits
      if (request.method === 'POST' && path === '/reset') {
        const body = await request.json().catch(() => ({})) as { endpointType?: string }
        await this.reset(body.endpointType)
        return Response.json({ success: true })
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      console.error('[RateLimitDO] Error:', error)
      return Response.json(
        { error: error instanceof Error ? error.message : 'Internal error' },
        { status: 500 }
      )
    }
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get the rate limiter stub for a client identifier
 *
 * @param env - Worker environment with RATE_LIMITER binding
 * @param clientId - Client identifier (IP address or API key)
 * @returns Durable Object stub for rate limiting
 *
 * @example
 * ```typescript
 * // Rate limit by IP
 * const limiter = getRateLimiter(env, request.headers.get('CF-Connecting-IP') || 'unknown')
 *
 * // Rate limit by API key
 * const apiKey = request.headers.get('X-API-Key')
 * const limiter = getRateLimiter(env, `key:${apiKey}`)
 * ```
 */
export function getRateLimiter(
  env: { RATE_LIMITER: DurableObjectNamespace<RateLimitDO> },
  clientId: string
): DurableObjectStub<RateLimitDO> {
  const id = env.RATE_LIMITER.idFromName(clientId)
  return env.RATE_LIMITER.get(id)
}

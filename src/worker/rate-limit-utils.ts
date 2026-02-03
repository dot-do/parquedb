/**
 * Rate Limiting Utilities
 *
 * Pure functions for rate limiting that can be used independently
 * of the Durable Object. These are testable in Node.js without
 * cloudflare:workers imports.
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Rate limit configuration for different endpoint types
 */
export interface RateLimitConfig {
  /** Maximum requests allowed in the window */
  maxRequests: number
  /** Window duration in milliseconds */
  windowMs: number
}

/**
 * Result of a rate limit check
 */
export interface RateLimitResult {
  /** Whether the request is allowed */
  allowed: boolean
  /** Number of requests remaining in the current window */
  remaining: number
  /** Unix timestamp (ms) when the rate limit resets */
  resetAt: number
  /** Total limit for the window */
  limit: number
  /** Current request count in the window */
  current: number
}

/**
 * Default rate limits for different endpoint types
 */
export const DEFAULT_RATE_LIMITS: Record<string, RateLimitConfig> = {
  /** Public endpoints (list public databases) - generous limits */
  public: { maxRequests: 100, windowMs: 60 * 1000 }, // 100 req/min

  /** Database metadata endpoints - moderate limits */
  database: { maxRequests: 200, windowMs: 60 * 1000 }, // 200 req/min

  /** Collection query endpoints - moderate limits */
  query: { maxRequests: 300, windowMs: 60 * 1000 }, // 300 req/min

  /** Raw file access - higher limits for Parquet reading */
  file: { maxRequests: 1000, windowMs: 60 * 1000 }, // 1000 req/min

  // ==========================================================================
  // Write operations - stricter limits to prevent resource exhaustion
  // ==========================================================================

  /** /ns/:namespace write operations (POST, PATCH, DELETE) */
  ns_write: { maxRequests: 60, windowMs: 60 * 1000 }, // 60 req/min (1/sec avg)

  /** /ns/:namespace read operations (GET) */
  ns_read: { maxRequests: 300, windowMs: 60 * 1000 }, // 300 req/min

  // ==========================================================================
  // Debug endpoints - moderate limits (resource-intensive operations)
  // ==========================================================================

  /** Debug endpoints (/debug/*) */
  debug: { maxRequests: 30, windowMs: 60 * 1000 }, // 30 req/min

  // ==========================================================================
  // Benchmark endpoints - strict limits (very resource-intensive)
  // ==========================================================================

  /** Benchmark endpoints (/benchmark*) */
  benchmark: { maxRequests: 10, windowMs: 60 * 1000 }, // 10 req/min

  // ==========================================================================
  // Migration endpoints - very strict limits (long-running operations)
  // ==========================================================================

  /** Migration endpoints (/migrate*) */
  migration: { maxRequests: 5, windowMs: 60 * 1000 }, // 5 req/min

  // ==========================================================================
  // Sync endpoints - moderate limits for CLI operations
  // ==========================================================================

  /** Sync API endpoints (/api/sync/*) */
  sync: { maxRequests: 100, windowMs: 60 * 1000 }, // 100 req/min

  /** Sync file upload/download */
  sync_file: { maxRequests: 500, windowMs: 60 * 1000 }, // 500 req/min

  // ==========================================================================
  // Dataset endpoints - read operations
  // ==========================================================================

  /** Dataset browsing endpoints (/datasets/*) */
  datasets: { maxRequests: 200, windowMs: 60 * 1000 }, // 200 req/min

  // ==========================================================================
  // Compaction endpoints
  // ==========================================================================

  /** Compaction status endpoint */
  compaction: { maxRequests: 30, windowMs: 60 * 1000 }, // 30 req/min

  // ==========================================================================
  // Vacuum endpoints - strict limits (long-running operations)
  // ==========================================================================

  /** Vacuum endpoints (/vacuum/*) */
  vacuum: { maxRequests: 5, windowMs: 60 * 1000 }, // 5 req/min

  /** Global fallback */
  default: { maxRequests: 100, windowMs: 60 * 1000 }, // 100 req/min
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Extract client identifier from request
 * Prefers CF-Connecting-IP, falls back to X-Forwarded-For, then 'unknown'
 *
 * @param request - Incoming request
 * @returns Client identifier string
 */
export function getClientId(request: Request): string {
  // Cloudflare's connecting IP header (most reliable)
  const cfConnectingIp = request.headers.get('CF-Connecting-IP')
  if (cfConnectingIp) return `ip:${cfConnectingIp}`

  // Fallback to X-Forwarded-For (first IP in chain)
  const xForwardedFor = request.headers.get('X-Forwarded-For')
  if (xForwardedFor) {
    const firstIp = xForwardedFor.split(',')[0]?.trim()
    if (firstIp) return `ip:${firstIp}`
  }

  // Check for API key
  const apiKey = request.headers.get('X-API-Key')
  if (apiKey) return `key:${apiKey}`

  // Fallback
  return 'ip:unknown'
}

/**
 * Build rate limit headers for response
 *
 * @param result - Rate limit check result
 * @returns Headers object with rate limit information
 */
export function buildRateLimitHeaders(result: RateLimitResult): Record<string, string> {
  return {
    'X-RateLimit-Limit': result.limit.toString(),
    'X-RateLimit-Remaining': result.remaining.toString(),
    'X-RateLimit-Reset': Math.ceil(result.resetAt / 1000).toString(),
    'RateLimit-Limit': result.limit.toString(),
    'RateLimit-Remaining': result.remaining.toString(),
    'RateLimit-Reset': Math.ceil(result.resetAt / 1000).toString(),
  }
}

/**
 * Build 429 Too Many Requests response
 *
 * @param result - Rate limit check result
 * @returns Response with rate limit headers and retry-after
 */
export function buildRateLimitResponse(result: RateLimitResult): Response {
  const retryAfter = Math.ceil((result.resetAt - Date.now()) / 1000)

  return new Response(
    JSON.stringify({
      error: 'Too Many Requests',
      message: `Rate limit exceeded. Please retry after ${retryAfter} seconds.`,
      limit: result.limit,
      remaining: result.remaining,
      resetAt: result.resetAt,
      retryAfter,
    }),
    {
      status: 429,
      headers: {
        'Content-Type': 'application/json',
        'Retry-After': retryAfter.toString(),
        ...buildRateLimitHeaders(result),
      },
    }
  )
}

/**
 * Endpoint type for rate limiting categorization
 */
export type EndpointType =
  | 'public'
  | 'database'
  | 'query'
  | 'file'
  | 'ns_write'
  | 'ns_read'
  | 'debug'
  | 'benchmark'
  | 'migration'
  | 'sync'
  | 'sync_file'
  | 'datasets'
  | 'compaction'
  | 'vacuum'
  | 'default'

/**
 * Determine the endpoint type from the request path and method
 * for rate limiting categorization.
 *
 * @param path - URL pathname
 * @param method - HTTP method
 * @returns Endpoint type for rate limiting
 */
export function getEndpointTypeFromPath(path: string, method: string): EndpointType {
  // /ns routes - separate read/write
  if (path.match(/^\/ns\//)) {
    if (method === 'GET') {
      return 'ns_read'
    }
    // POST, PATCH, DELETE are write operations
    return 'ns_write'
  }

  // Debug endpoints
  if (path.startsWith('/debug/')) {
    return 'debug'
  }

  // Benchmark endpoints (multiple variants)
  if (path.startsWith('/benchmark')) {
    return 'benchmark'
  }

  // Migration endpoints
  if (path.startsWith('/migrate')) {
    return 'migration'
  }

  // Compaction status
  if (path.startsWith('/compaction/')) {
    return 'compaction'
  }

  // Vacuum endpoints
  if (path.startsWith('/vacuum/')) {
    return 'vacuum'
  }

  // Sync routes
  if (path.startsWith('/api/sync/upload/') || path.startsWith('/api/sync/download/')) {
    return 'sync_file'
  }
  if (path.startsWith('/api/sync/')) {
    return 'sync'
  }

  // Dataset routes
  if (path.startsWith('/datasets')) {
    return 'datasets'
  }

  // Public routes (handled by public-routes.ts, but included for completeness)
  if (path === '/api/public') {
    return 'public'
  }
  if (path.match(/^\/api\/db\/[^/]+\/[^/]+$/)) {
    return 'database'
  }
  if (path.match(/^\/api\/db\/[^/]+\/[^/]+\/[^/]+$/)) {
    return 'query'
  }
  if (path.startsWith('/db/')) {
    return 'file'
  }

  // Default fallback
  return 'default'
}

/**
 * Add rate limit headers to an existing response
 *
 * @param response - Original response
 * @param result - Rate limit check result
 * @returns New response with rate limit headers added
 */
export function addRateLimitHeadersToResponse(
  response: Response,
  result: RateLimitResult
): Response {
  const headers = new Headers(response.headers)
  const rateLimitHeaders = buildRateLimitHeaders(result)
  for (const [key, value] of Object.entries(rateLimitHeaders)) {
    headers.set(key, value)
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

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

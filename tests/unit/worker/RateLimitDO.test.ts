/**
 * Rate Limiting Tests
 *
 * Tests for the rate limiting utility functions.
 * Tests the pure functions from rate-limit-utils.ts which are testable
 * without Cloudflare Workers runtime.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  getClientId,
  buildRateLimitHeaders,
  buildRateLimitResponse,
  DEFAULT_RATE_LIMITS,
  type RateLimitResult,
} from '@/worker/rate-limit-utils'

// =============================================================================
// getClientId Tests
// =============================================================================

describe('getClientId', () => {
  it('extracts IP from CF-Connecting-IP header', () => {
    const request = new Request('https://example.com', {
      headers: { 'CF-Connecting-IP': '192.168.1.1' },
    })
    expect(getClientId(request)).toBe('ip:192.168.1.1')
  })

  it('falls back to X-Forwarded-For when CF-Connecting-IP is missing', () => {
    const request = new Request('https://example.com', {
      headers: { 'X-Forwarded-For': '10.0.0.1, 10.0.0.2' },
    })
    expect(getClientId(request)).toBe('ip:10.0.0.1')
  })

  it('uses API key when X-API-Key header is present and no IP headers', () => {
    const request = new Request('https://example.com', {
      headers: { 'X-API-Key': 'my-api-key-123' },
    })
    expect(getClientId(request)).toBe('key:my-api-key-123')
  })

  it('prefers CF-Connecting-IP over X-Forwarded-For', () => {
    const request = new Request('https://example.com', {
      headers: {
        'CF-Connecting-IP': '192.168.1.1',
        'X-Forwarded-For': '10.0.0.1',
      },
    })
    expect(getClientId(request)).toBe('ip:192.168.1.1')
  })

  it('prefers IP headers over API key', () => {
    const request = new Request('https://example.com', {
      headers: {
        'CF-Connecting-IP': '192.168.1.1',
        'X-API-Key': 'my-api-key',
      },
    })
    expect(getClientId(request)).toBe('ip:192.168.1.1')
  })

  it('returns unknown when no identifying headers present', () => {
    const request = new Request('https://example.com')
    expect(getClientId(request)).toBe('ip:unknown')
  })

  it('trims whitespace from X-Forwarded-For', () => {
    const request = new Request('https://example.com', {
      headers: { 'X-Forwarded-For': '  10.0.0.1  ,  10.0.0.2  ' },
    })
    expect(getClientId(request)).toBe('ip:10.0.0.1')
  })
})

// =============================================================================
// buildRateLimitHeaders Tests
// =============================================================================

describe('buildRateLimitHeaders', () => {
  it('builds correct headers from rate limit result', () => {
    const result: RateLimitResult = {
      allowed: true,
      remaining: 50,
      resetAt: 1700000000000,
      limit: 100,
      current: 50,
    }

    const headers = buildRateLimitHeaders(result)

    // Check X-RateLimit-* headers
    expect(headers['X-RateLimit-Limit']).toBe('100')
    expect(headers['X-RateLimit-Remaining']).toBe('50')
    expect(headers['X-RateLimit-Reset']).toBe('1700000000')

    // Check RateLimit-* headers (draft standard)
    expect(headers['RateLimit-Limit']).toBe('100')
    expect(headers['RateLimit-Remaining']).toBe('50')
    expect(headers['RateLimit-Reset']).toBe('1700000000')
  })

  it('handles zero remaining requests', () => {
    const result: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt: 1700000000000,
      limit: 100,
      current: 100,
    }

    const headers = buildRateLimitHeaders(result)
    expect(headers['X-RateLimit-Remaining']).toBe('0')
  })

  it('rounds reset time to seconds', () => {
    const result: RateLimitResult = {
      allowed: true,
      remaining: 99,
      resetAt: 1700000000999, // 999ms should round up to next second
      limit: 100,
      current: 1,
    }

    const headers = buildRateLimitHeaders(result)
    expect(headers['X-RateLimit-Reset']).toBe('1700000001')
  })
})

// =============================================================================
// buildRateLimitResponse Tests
// =============================================================================

describe('buildRateLimitResponse', () => {
  it('returns 429 status code', () => {
    const result: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt: Date.now() + 30000,
      limit: 100,
      current: 100,
    }

    const response = buildRateLimitResponse(result)
    expect(response.status).toBe(429)
  })

  it('includes Retry-After header', async () => {
    const resetAt = Date.now() + 30000 // 30 seconds from now
    const result: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt,
      limit: 100,
      current: 100,
    }

    const response = buildRateLimitResponse(result)
    const retryAfter = response.headers.get('Retry-After')

    // Should be approximately 30 seconds (allowing for test execution time)
    expect(Number(retryAfter)).toBeGreaterThanOrEqual(29)
    expect(Number(retryAfter)).toBeLessThanOrEqual(31)
  })

  it('includes rate limit headers', () => {
    const result: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt: Date.now() + 30000,
      limit: 100,
      current: 100,
    }

    const response = buildRateLimitResponse(result)

    expect(response.headers.get('X-RateLimit-Limit')).toBe('100')
    expect(response.headers.get('X-RateLimit-Remaining')).toBe('0')
    expect(response.headers.get('Content-Type')).toBe('application/json')
  })

  it('returns JSON body with error message', async () => {
    const result: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt: Date.now() + 30000,
      limit: 100,
      current: 100,
    }

    const response = buildRateLimitResponse(result)
    const body = await response.json() as {
      error: string
      message: string
      limit: number
      remaining: number
    }

    expect(body.error).toBe('Too Many Requests')
    expect(body.message).toContain('Rate limit exceeded')
    expect(body.limit).toBe(100)
    expect(body.remaining).toBe(0)
  })
})

// =============================================================================
// DEFAULT_RATE_LIMITS Tests
// =============================================================================

describe('DEFAULT_RATE_LIMITS', () => {
  it('has configuration for public endpoints', () => {
    expect(DEFAULT_RATE_LIMITS.public).toBeDefined()
    expect(DEFAULT_RATE_LIMITS.public!.maxRequests).toBeGreaterThan(0)
    expect(DEFAULT_RATE_LIMITS.public!.windowMs).toBeGreaterThan(0)
  })

  it('has configuration for database endpoints', () => {
    expect(DEFAULT_RATE_LIMITS.database).toBeDefined()
    expect(DEFAULT_RATE_LIMITS.database!.maxRequests).toBeGreaterThan(0)
  })

  it('has configuration for query endpoints', () => {
    expect(DEFAULT_RATE_LIMITS.query).toBeDefined()
    expect(DEFAULT_RATE_LIMITS.query!.maxRequests).toBeGreaterThan(0)
  })

  it('has configuration for file endpoints', () => {
    expect(DEFAULT_RATE_LIMITS.file).toBeDefined()
    expect(DEFAULT_RATE_LIMITS.file!.maxRequests).toBeGreaterThan(0)
  })

  it('has default fallback configuration', () => {
    expect(DEFAULT_RATE_LIMITS.default).toBeDefined()
  })

  it('file endpoints have higher limits than public endpoints', () => {
    // File access (Parquet range reads) should have higher limits
    // since a single query might need multiple range reads
    expect(DEFAULT_RATE_LIMITS.file!.maxRequests).toBeGreaterThan(
      DEFAULT_RATE_LIMITS.public!.maxRequests
    )
  })

  it('all endpoints use 1-minute windows', () => {
    const oneMinuteMs = 60 * 1000
    for (const [key, config] of Object.entries(DEFAULT_RATE_LIMITS)) {
      expect(config.windowMs).toBe(oneMinuteMs)
    }
  })
})

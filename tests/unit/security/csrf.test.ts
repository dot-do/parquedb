/**
 * CSRF Protection Tests
 *
 * Tests for the CSRF protection middleware and utilities.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { validateCsrfRequest, csrfErrorResponse } from '../../../src/worker/handlers/csrf-validation'
import {
  generateCsrfToken,
  verifyCsrfToken,
  validateCsrf,
  buildSecureCorsHeaders,
  getAllowedOriginHeader,
} from '../../../src/security/csrf'

// =============================================================================
// Worker Handler CSRF Validation Tests
// =============================================================================

describe('validateCsrfRequest', () => {
  const baseUrl = 'https://api.example.com'

  function createRequest(options: {
    method?: string
    origin?: string | null
    referer?: string | null
    requestedWith?: string | null
  }): Request {
    const headers = new Headers()

    if (options.origin !== null && options.origin !== undefined) {
      headers.set('Origin', options.origin)
    }
    if (options.referer !== null && options.referer !== undefined) {
      headers.set('Referer', options.referer)
    }
    if (options.requestedWith !== null && options.requestedWith !== undefined) {
      headers.set('X-Requested-With', options.requestedWith)
    }

    return new Request(`${baseUrl}/api/entities`, {
      method: options.method || 'POST',
      headers,
    })
  }

  describe('same-origin requests', () => {
    it('should accept same-origin request with X-Requested-With header', () => {
      const request = createRequest({
        origin: baseUrl,
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should accept same-origin request with custom header value', () => {
      const request = createRequest({
        origin: baseUrl,
        requestedWith: 'ParqueDB',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should reject same-origin request without X-Requested-With header', () => {
      const request = createRequest({
        origin: baseUrl,
        requestedWith: null,
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('X-Requested-With')
    })
  })

  describe('cross-origin requests', () => {
    it('should reject cross-origin request without allowed origins', () => {
      const request = createRequest({
        origin: 'https://attacker.com',
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('not allowed')
    })

    it('should accept cross-origin request with allowed origins', () => {
      const request = createRequest({
        origin: 'https://trusted.com',
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request, ['https://trusted.com'])
      expect(result.valid).toBe(true)
    })

    it('should accept cross-origin with wildcard allowed origins', () => {
      const request = createRequest({
        origin: 'https://any.com',
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request, ['*'])
      expect(result.valid).toBe(true)
    })
  })

  describe('referer fallback', () => {
    it('should use Referer header when Origin is missing', () => {
      const request = createRequest({
        origin: null,
        referer: `${baseUrl}/some-page`,
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should reject invalid Referer header', () => {
      const request = createRequest({
        origin: null,
        referer: 'not-a-valid-url',
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Invalid Referer')
    })

    it('should reject cross-origin Referer', () => {
      const request = createRequest({
        origin: null,
        referer: 'https://attacker.com/page',
        requestedWith: 'XMLHttpRequest',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('not allowed')
    })
  })

  describe('missing origin headers', () => {
    it('should accept request without Origin/Referer if X-Requested-With is present', () => {
      const request = createRequest({
        origin: null,
        referer: null,
        requestedWith: 'XMLHttpRequest',
      })

      // This allows direct API calls (curl, Postman, etc.)
      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should reject request without any headers', () => {
      const request = createRequest({
        origin: null,
        referer: null,
        requestedWith: null,
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Missing')
    })
  })
})

describe('csrfErrorResponse', () => {
  it('should return 403 response with error details', async () => {
    const response = csrfErrorResponse('Test reason')

    expect(response.status).toBe(403)

    const body = await response.json()
    expect(body.error).toBe('CSRF validation failed')
    expect(body.code).toBe('CSRF_VALIDATION_FAILED')
    expect(body.reason).toBe('Test reason')
    expect(body.hint).toBeDefined()
  })
})

// =============================================================================
// Token-based CSRF Tests
// =============================================================================

describe('CSRF Token Generation and Verification', () => {
  const testSecret = 'test-secret-key-for-csrf-tokens'
  const testSubject = 'user-123'

  describe('generateCsrfToken', () => {
    it('should generate a valid token string', async () => {
      const token = await generateCsrfToken(testSecret, testSubject)

      expect(typeof token).toBe('string')
      expect(token.split('.').length).toBe(2) // payload.signature format
    })

    it('should generate unique tokens', async () => {
      const token1 = await generateCsrfToken(testSecret, testSubject)
      const token2 = await generateCsrfToken(testSecret, testSubject)

      expect(token1).not.toBe(token2)
    })
  })

  describe('verifyCsrfToken', () => {
    it('should verify a valid token', async () => {
      const token = await generateCsrfToken(testSecret, testSubject)
      const result = await verifyCsrfToken(testSecret, token, testSubject)

      expect(result.valid).toBe(true)
      expect(result.payload).toBeDefined()
      expect(result.payload?.sub).toBe(testSubject)
    })

    it('should reject token with wrong secret', async () => {
      const token = await generateCsrfToken(testSecret, testSubject)
      const result = await verifyCsrfToken('wrong-secret', token, testSubject)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('signature')
    })

    it('should reject token with wrong subject', async () => {
      const token = await generateCsrfToken(testSecret, testSubject)
      const result = await verifyCsrfToken(testSecret, token, 'different-user')

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('subject')
    })

    it('should reject expired token', async () => {
      // Create token with very short TTL
      const token = await generateCsrfToken(testSecret, testSubject, 1) // 1ms TTL

      // Wait for expiration
      await new Promise(resolve => setTimeout(resolve, 10))

      const result = await verifyCsrfToken(testSecret, token, testSubject)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('expired')
    })

    it('should reject malformed token', async () => {
      const result = await verifyCsrfToken(testSecret, 'invalid-token', testSubject)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('format')
    })

    it('should reject missing token', async () => {
      const result = await verifyCsrfToken(testSecret, '', testSubject)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Missing')
    })
  })
})

// =============================================================================
// CORS Helper Tests
// =============================================================================

describe('buildSecureCorsHeaders', () => {
  it('should build headers with default values', () => {
    const headers = buildSecureCorsHeaders({
      allowedOrigins: ['https://example.com'],
    })

    expect(headers['Access-Control-Allow-Methods']).toBe('GET, HEAD, OPTIONS')
    expect(headers['Access-Control-Allow-Headers']).toContain('X-Requested-With')
    expect(headers['Access-Control-Max-Age']).toBe('86400')
  })

  it('should include custom methods', () => {
    const headers = buildSecureCorsHeaders({
      allowedOrigins: ['https://example.com'],
      allowedMethods: ['GET', 'POST', 'PUT', 'DELETE'],
    })

    expect(headers['Access-Control-Allow-Methods']).toBe('GET, POST, PUT, DELETE')
  })

  it('should include credentials header when enabled', () => {
    const headers = buildSecureCorsHeaders({
      allowedOrigins: ['https://example.com'],
      credentials: true,
    })

    expect(headers['Access-Control-Allow-Credentials']).toBe('true')
  })

  it('should include exposed headers', () => {
    const headers = buildSecureCorsHeaders({
      allowedOrigins: ['https://example.com'],
      exposeHeaders: ['X-Custom-Header', 'X-Another-Header'],
    })

    expect(headers['Access-Control-Expose-Headers']).toBe('X-Custom-Header, X-Another-Header')
  })
})

describe('getAllowedOriginHeader', () => {
  const allowedOrigins = ['https://app.example.com', 'https://admin.example.com']

  it('should return origin if allowed', () => {
    const result = getAllowedOriginHeader('https://app.example.com', allowedOrigins)
    expect(result).toBe('https://app.example.com')
  })

  it('should return null if not allowed', () => {
    const result = getAllowedOriginHeader('https://attacker.com', allowedOrigins)
    expect(result).toBeNull()
  })

  it('should return * for wildcard (without credentials)', () => {
    const result = getAllowedOriginHeader('https://any.com', ['*'], false)
    expect(result).toBe('*')
  })

  it('should not return * for wildcard with credentials', () => {
    const result = getAllowedOriginHeader('https://any.com', ['*'], true)
    expect(result).toBeNull()
  })

  it('should return null for null origin', () => {
    const result = getAllowedOriginHeader(null, allowedOrigins)
    expect(result).toBeNull()
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('CSRF Protection Integration', () => {
  describe('Mutation Request Flow', () => {
    it('should protect POST requests', () => {
      const request = new Request('https://api.example.com/api/entities', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'ParqueDB',
        },
        body: JSON.stringify({ name: 'test' }),
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should protect PATCH requests', () => {
      const request = new Request('https://api.example.com/api/entities/123', {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
        body: JSON.stringify({ $set: { name: 'updated' } }),
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should protect DELETE requests', () => {
      const request = new Request('https://api.example.com/api/entities/123', {
        method: 'DELETE',
        headers: {
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'fetch',
        },
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(true)
    })

    it('should reject form-based CSRF attack (no custom header)', () => {
      // Simulates a malicious form submission from another site
      const request = new Request('https://api.example.com/api/entities', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Origin': 'https://attacker.com',
          // No X-Requested-With header (forms can't set custom headers)
        },
        body: 'name=evil',
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
    })

    it('should reject cross-origin fetch attack', () => {
      // Simulates a cross-origin fetch that somehow got through
      // (shouldn't happen with CORS, but defense in depth)
      const request = new Request('https://api.example.com/api/entities', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Origin': 'https://attacker.com',
          // Attacker would need CORS preflight to set this header
        },
        body: JSON.stringify({ name: 'evil' }),
      })

      const result = validateCsrfRequest(request)
      expect(result.valid).toBe(false)
    })
  })
})

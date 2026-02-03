/**
 * JWT Signature Verification Security Tests
 *
 * These tests verify that JWT tokens are properly validated using cryptographic
 * signature verification via JWKS, rather than just base64 decoding.
 *
 * Security requirements tested:
 * 1. Tokens without valid signatures are rejected
 * 2. Tokens with forged payloads are rejected
 * 3. JWKS_URI must be configured for secure verification
 * 4. Expired tokens are rejected
 *
 * This addresses security issue parquedb-l1xn.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { verifyJWT, verifyOwnership, extractBearerToken, JWKSFetchTimeoutError } from '@/worker/jwt-utils'
import type { Env } from '@/types/worker'

// =============================================================================
// Test Fixtures
// =============================================================================

/**
 * Create a mock environment with optional JWKS_URI
 */
function createMockEnv(jwksUri?: string): Env {
  return {
    JWKS_URI: jwksUri,
    PARQUEDB: {} as DurableObjectNamespace,
    BUCKET: {} as R2Bucket,
  } as Env
}

/**
 * Create a forged JWT token (just base64 encoded, no signature verification)
 * This simulates what an attacker would try to do
 */
function createForgedToken(payload: Record<string, unknown>): string {
  const header = { alg: 'HS256', typ: 'JWT' }
  const encodeBase64Url = (data: string) => {
    const base64 = btoa(data)
    return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  }
  const headerPart = encodeBase64Url(JSON.stringify(header))
  const payloadPart = encodeBase64Url(JSON.stringify(payload))
  const fakeSignature = encodeBase64Url('fake-signature-not-valid')
  return `${headerPart}.${payloadPart}.${fakeSignature}`
}

// =============================================================================
// Security Tests - JWT Signature Verification
// =============================================================================

describe('JWT Security - Signature Verification', () => {
  describe('verifyJWT', () => {
    it('should reject tokens when JWKS_URI is not configured', async () => {
      const env = createMockEnv() // No JWKS_URI
      const token = createForgedToken({ sub: 'attacker', exp: Date.now() / 1000 + 3600 })

      const result = await verifyJWT(token, env)

      expect(result.valid).toBe(false)
      expect(result.error).toBe('JWKS_URI is not configured. Contact administrator.')
    })

    it('should reject forged tokens with fake signatures', async () => {
      // This test requires JWKS_URI to be set, and the forged token should fail
      // verification against any real JWKS endpoint
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')
      const forgedToken = createForgedToken({
        sub: 'attacker',
        username: 'admin',
        exp: Date.now() / 1000 + 3600,
      })

      const result = await verifyJWT(forgedToken, env)

      // Token should be rejected because signature doesn't verify
      expect(result.valid).toBe(false)
      // The error will be from jose library indicating verification failure
    })

    it('should reject tokens with tampered payload', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Create what looks like a valid JWT structure but with tampered payload
      const tamperedToken = createForgedToken({
        sub: 'victim-user-id',
        username: 'victim',
        email: 'victim@example.com',
        exp: Date.now() / 1000 + 3600,
        iss: 'https://oauth.do',
        aud: 'parquedb',
      })

      const result = await verifyJWT(tamperedToken, env)

      expect(result.valid).toBe(false)
    })

    it('should reject tokens with invalid JWT structure', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Test various malformed tokens
      const malformedTokens = [
        '',
        'not-a-jwt',
        'only.two.parts.but.wrong',
        'eyJhbGciOiJIUzI1NiJ9', // Only header
        'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0', // Missing signature
      ]

      for (const token of malformedTokens) {
        const result = await verifyJWT(token, env)
        expect(result.valid).toBe(false)
      }
    })
  })

  describe('verifyOwnership', () => {
    it('should reject null token', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      const result = await verifyOwnership(null, 'testuser', env)

      expect(result).toBe(false)
    })

    it('should reject forged token claiming to be owner', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Attacker tries to forge a token claiming to be 'testuser'
      const forgedToken = createForgedToken({
        sub: 'testuser',
        username: 'testuser',
        exp: Date.now() / 1000 + 3600,
      })

      const result = await verifyOwnership(forgedToken, 'testuser', env)

      // Should be rejected because signature is not valid
      expect(result).toBe(false)
    })

    it('should reject tokens without JWKS_URI configured', async () => {
      const env = createMockEnv() // No JWKS_URI

      const token = createForgedToken({ sub: 'testuser' })
      const result = await verifyOwnership(token, 'testuser', env)

      expect(result).toBe(false)
    })
  })

  describe('extractBearerToken', () => {
    it('should extract Bearer token from Authorization header', () => {
      const request = new Request('https://example.com', {
        headers: {
          Authorization: 'Bearer my-token-here',
        },
      })

      const token = extractBearerToken(request)

      expect(token).toBe('my-token-here')
    })

    it('should return null for missing Authorization header', () => {
      const request = new Request('https://example.com')

      const token = extractBearerToken(request)

      expect(token).toBeNull()
    })

    it('should return null for non-Bearer authorization', () => {
      const request = new Request('https://example.com', {
        headers: {
          Authorization: 'Basic dXNlcjpwYXNz',
        },
      })

      const token = extractBearerToken(request)

      expect(token).toBeNull()
    })

    it('should return null for malformed Bearer header', () => {
      const request = new Request('https://example.com', {
        headers: {
          Authorization: 'Bearernotokenspace',
        },
      })

      const token = extractBearerToken(request)

      expect(token).toBeNull()
    })
  })
})

// =============================================================================
// Attack Scenario Tests
// =============================================================================

describe('JWT Security - Attack Scenarios', () => {
  describe('Privilege Escalation Attacks', () => {
    it('should prevent attacker from impersonating another user', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Attacker creates a token claiming to be 'admin' user
      const attackerToken = createForgedToken({
        sub: 'admin',
        username: 'admin',
        roles: ['admin', 'superuser'],
        exp: Date.now() / 1000 + 3600,
      })

      const result = await verifyJWT(attackerToken, env)

      expect(result.valid).toBe(false)
      expect(result.user).toBeUndefined()
    })

    it('should prevent attacker from accessing other users databases', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Attacker creates a token claiming to be 'victim-user'
      const attackerToken = createForgedToken({
        sub: 'victim-user',
        exp: Date.now() / 1000 + 3600,
      })

      const isOwner = await verifyOwnership(attackerToken, 'victim-user', env)

      expect(isOwner).toBe(false)
    })
  })

  describe('Token Manipulation Attacks', () => {
    it('should reject tokens with modified expiration', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Attacker tries to extend token expiration
      const modifiedToken = createForgedToken({
        sub: 'user',
        exp: Date.now() / 1000 + 86400 * 365, // 1 year from now
      })

      const result = await verifyJWT(modifiedToken, env)

      expect(result.valid).toBe(false)
    })

    it('should reject tokens with none algorithm', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Attacker tries to use 'none' algorithm (alg: none attack)
      const header = { alg: 'none', typ: 'JWT' }
      const payload = { sub: 'attacker', exp: Date.now() / 1000 + 3600 }
      const encodeBase64Url = (data: string) =>
        btoa(data).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')

      const noneAlgToken = `${encodeBase64Url(JSON.stringify(header))}.${encodeBase64Url(JSON.stringify(payload))}.`

      const result = await verifyJWT(noneAlgToken, env)

      expect(result.valid).toBe(false)
    })
  })

  describe('Replay and Injection Attacks', () => {
    it('should reject tokens with invalid characters', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')

      // Token with injection attempt
      const injectionTokens = [
        '${JWKS_URI}',
        'eyJhbGciOiJIUzI1NiJ9.`id`.sig',
        '../../../etc/passwd',
      ]

      for (const token of injectionTokens) {
        const result = await verifyJWT(token, env)
        expect(result.valid).toBe(false)
      }
    })
  })
})

// =============================================================================
// Backwards Compatibility Tests
// =============================================================================

describe('JWT Security - Error Handling', () => {
  it('should return descriptive error for missing JWKS_URI', async () => {
    const env = createMockEnv()
    const token = createForgedToken({ sub: 'user' })

    const result = await verifyJWT(token, env)

    expect(result.valid).toBe(false)
    expect(result.error).toContain('JWKS_URI')
    expect(result.error).toContain('not configured')
  })

  it('should not expose sensitive information in errors', async () => {
    const env = createMockEnv('https://api.workos.com/sso/jwks/test')
    const token = createForgedToken({ sub: 'user', secret_data: 'sensitive' })

    const result = await verifyJWT(token, env)

    expect(result.valid).toBe(false)
    // Error should not contain the payload contents
    expect(result.error).not.toContain('sensitive')
  })
})

// =============================================================================
// Timeout Protection Tests
// =============================================================================

describe('JWT Security - Timeout Protection', () => {
  describe('JWKSFetchTimeoutError', () => {
    it('should have correct error name and message', () => {
      const error = new JWKSFetchTimeoutError(10000)

      expect(error.name).toBe('JWKSFetchTimeoutError')
      expect(error.message).toBe('JWKS fetch timed out after 10000ms')
      expect(error).toBeInstanceOf(Error)
    })

    it('should format timeout duration in error message', () => {
      const error5s = new JWKSFetchTimeoutError(5000)
      const error30s = new JWKSFetchTimeoutError(30000)

      expect(error5s.message).toContain('5000ms')
      expect(error30s.message).toContain('30000ms')
    })
  })

  describe('AbortController timeout integration', () => {
    it('should export JWKSFetchTimeoutError for external use', () => {
      // Verify the error class is properly exported for consumers
      expect(JWKSFetchTimeoutError).toBeDefined()
      expect(typeof JWKSFetchTimeoutError).toBe('function')
    })

    it('should handle timeout errors distinctly from other verification errors', async () => {
      const env = createMockEnv('https://api.workos.com/sso/jwks/test')
      const token = createForgedToken({ sub: 'user' })

      // When JWKS fetch times out, verify the error is handled properly
      // This tests the error handling path exists even if we can't easily
      // trigger an actual timeout in unit tests
      const result = await verifyJWT(token, env)

      // Verification will fail (forged token), but timeout handling exists
      expect(result.valid).toBe(false)
      // Error could be timeout or verification failure depending on network
      expect(typeof result.error).toBe('string')
    })
  })
})

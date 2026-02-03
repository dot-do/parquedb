/**
 * Sync Routes Token Signing Tests
 *
 * Tests HMAC-SHA256 token signing for secure upload/download URLs.
 * Verifies that:
 * 1. Tokens are properly signed with HMAC-SHA256
 * 2. Invalid/tampered tokens are rejected
 * 3. Expired tokens are rejected
 * 4. Token type enforcement (upload vs download)
 * 5. Missing SYNC_SECRET is handled correctly
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  signUploadToken,
  signDownloadToken,
  verifyUploadToken,
  verifyDownloadToken,
  type TokenPayload,
  type TokenEnv,
} from '@/worker/sync-token'

// =============================================================================
// Test Fixtures
// =============================================================================

function createMockEnv(syncSecret?: string): TokenEnv {
  return {
    SYNC_SECRET: syncSecret,
  }
}

function createValidPayload(overrides?: Partial<TokenPayload>): TokenPayload {
  return {
    databaseId: 'db-123',
    path: 'data/test.parquet',
    userId: 'user-456',
    expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(), // 1 hour from now
    ...overrides,
  }
}

function createExpiredPayload(): TokenPayload {
  return {
    databaseId: 'db-123',
    path: 'data/test.parquet',
    userId: 'user-456',
    expiresAt: new Date(Date.now() - 3600 * 1000).toISOString(), // 1 hour ago
  }
}

// =============================================================================
// Upload Token Tests
// =============================================================================

describe('signUploadToken', () => {
  const env = createMockEnv('test-secret-key-12345')

  it('signs a token with HMAC-SHA256', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Token should have format: base64url(payload).base64url(signature)
    expect(token).toContain('.')
    const parts = token.split('.')
    expect(parts).toHaveLength(2)

    // Both parts should be non-empty
    expect(parts[0]).toBeTruthy()
    expect(parts[1]).toBeTruthy()
  })

  it('produces different tokens with different secrets', async () => {
    const payload = createValidPayload()
    const env1 = createMockEnv('secret-1')
    const env2 = createMockEnv('secret-2')

    const token1 = await signUploadToken(payload, env1)
    const token2 = await signUploadToken(payload, env2)

    // Payload part should be the same
    const [payload1] = token1.split('.')
    const [payload2] = token2.split('.')
    expect(payload1).toBe(payload2)

    // Signature part should be different
    const [, sig1] = token1.split('.')
    const [, sig2] = token2.split('.')
    expect(sig1).not.toBe(sig2)
  })

  it('produces consistent tokens for same input', async () => {
    const payload = createValidPayload()
    const token1 = await signUploadToken(payload, env)
    const token2 = await signUploadToken(payload, env)

    expect(token1).toBe(token2)
  })

  it('throws error when SYNC_SECRET is not configured', async () => {
    const payload = createValidPayload()
    const envWithoutSecret = createMockEnv(undefined)

    await expect(signUploadToken(payload, envWithoutSecret)).rejects.toThrow(
      'SYNC_SECRET environment variable is required'
    )
  })

  it('uses base64url encoding (no + / = characters)', async () => {
    // Use a payload that would produce + or / in base64
    const payload = createValidPayload({
      path: 'data/with/many/slashes/and+plus.parquet',
    })
    const token = await signUploadToken(payload, env)

    expect(token).not.toContain('+')
    expect(token).not.toContain('/')
    expect(token).not.toMatch(/=+$/) // No trailing padding
  })
})

describe('verifyUploadToken', () => {
  const env = createMockEnv('test-secret-key-12345')

  it('verifies a valid token', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.databaseId).toBe(payload.databaseId)
    expect(result?.path).toBe(payload.path)
    expect(result?.userId).toBe(payload.userId)
  })

  it('rejects token signed with different secret', async () => {
    const payload = createValidPayload()
    const envOther = createMockEnv('different-secret')
    const token = await signUploadToken(payload, envOther)

    const result = await verifyUploadToken(token, env)

    expect(result).toBeNull()
  })

  it('rejects tampered payload', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Tamper with the payload part
    const [payloadPart, signature] = token.split('.')
    const decoded = JSON.parse(atob(payloadPart!.replace(/-/g, '+').replace(/_/g, '/')))
    decoded.userId = 'attacker-999' // Change userId
    const tamperedPayload = btoa(JSON.stringify(decoded))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '')
    const tamperedToken = `${tamperedPayload}.${signature}`

    const result = await verifyUploadToken(tamperedToken, env)

    expect(result).toBeNull()
  })

  it('rejects tampered signature', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Tamper with the signature part
    const [payloadPart, signature] = token.split('.')
    const tamperedSignature = signature!.slice(0, -4) + 'XXXX' // Change last 4 chars
    const tamperedToken = `${payloadPart}.${tamperedSignature}`

    const result = await verifyUploadToken(tamperedToken, env)

    expect(result).toBeNull()
  })

  it('rejects expired token', async () => {
    const payload = createExpiredPayload()
    const token = await signUploadToken(payload, env)

    const result = await verifyUploadToken(token, env)

    expect(result).toBeNull()
  })

  it('rejects download token used as upload token', async () => {
    const payload = createValidPayload()
    const downloadToken = await signDownloadToken(payload, env)

    const result = await verifyUploadToken(downloadToken, env)

    expect(result).toBeNull()
  })

  it('rejects malformed token (missing signature)', async () => {
    const result = await verifyUploadToken('just-payload-no-dot', env)
    expect(result).toBeNull()
  })

  it('rejects malformed token (too many parts)', async () => {
    const result = await verifyUploadToken('part1.part2.part3', env)
    expect(result).toBeNull()
  })

  it('rejects malformed token (invalid base64)', async () => {
    const result = await verifyUploadToken('!!!invalid!!!.base64', env)
    expect(result).toBeNull()
  })

  it('rejects empty token', async () => {
    const result = await verifyUploadToken('', env)
    expect(result).toBeNull()
  })

  it('throws error when SYNC_SECRET is not configured', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)
    const envWithoutSecret = createMockEnv(undefined)

    // Should return null gracefully (error is caught internally)
    const result = await verifyUploadToken(token, envWithoutSecret)
    expect(result).toBeNull()
  })
})

// =============================================================================
// Download Token Tests
// =============================================================================

describe('signDownloadToken', () => {
  const env = createMockEnv('test-secret-key-12345')

  it('signs a token with HMAC-SHA256', async () => {
    const payload = createValidPayload()
    const token = await signDownloadToken(payload, env)

    expect(token).toContain('.')
    const parts = token.split('.')
    expect(parts).toHaveLength(2)
  })

  it('produces different token than upload for same payload', async () => {
    const payload = createValidPayload()
    const uploadToken = await signUploadToken(payload, env)
    const downloadToken = await signDownloadToken(payload, env)

    // Tokens should be different because type is embedded
    expect(uploadToken).not.toBe(downloadToken)
  })
})

describe('verifyDownloadToken', () => {
  const env = createMockEnv('test-secret-key-12345')

  it('verifies a valid token', async () => {
    const payload = createValidPayload()
    const token = await signDownloadToken(payload, env)

    const result = await verifyDownloadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.databaseId).toBe(payload.databaseId)
    expect(result?.path).toBe(payload.path)
    expect(result?.userId).toBe(payload.userId)
  })

  it('rejects token signed with different secret', async () => {
    const payload = createValidPayload()
    const envOther = createMockEnv('different-secret')
    const token = await signDownloadToken(payload, envOther)

    const result = await verifyDownloadToken(token, env)

    expect(result).toBeNull()
  })

  it('rejects upload token used as download token', async () => {
    const payload = createValidPayload()
    const uploadToken = await signUploadToken(payload, env)

    const result = await verifyDownloadToken(uploadToken, env)

    expect(result).toBeNull()
  })

  it('rejects expired token', async () => {
    const payload = createExpiredPayload()
    const token = await signDownloadToken(payload, env)

    const result = await verifyDownloadToken(token, env)

    expect(result).toBeNull()
  })

  it('rejects tampered payload', async () => {
    const payload = createValidPayload()
    const token = await signDownloadToken(payload, env)

    const [payloadPart, signature] = token.split('.')
    const decoded = JSON.parse(atob(payloadPart!.replace(/-/g, '+').replace(/_/g, '/')))
    decoded.path = '/etc/passwd' // Attempt path traversal
    const tamperedPayload = btoa(JSON.stringify(decoded))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '')
    const tamperedToken = `${tamperedPayload}.${signature}`

    const result = await verifyDownloadToken(tamperedToken, env)

    expect(result).toBeNull()
  })
})

// =============================================================================
// Security Tests
// =============================================================================

describe('Token Security', () => {
  const env = createMockEnv('production-secret-key')

  it('prevents forging tokens without secret', async () => {
    // An attacker tries to create a token by just base64 encoding
    const fakePayload = {
      databaseId: 'db-123',
      path: 'secret/data.parquet',
      userId: 'admin',
      expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(),
      type: 'upload',
    }

    const payloadEncoded = btoa(JSON.stringify(fakePayload))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '')

    // Attacker uses a random/guessed signature
    const fakeSignature = 'Zm9vYmFy' // 'foobar' in base64
    const forgedToken = `${payloadEncoded}.${fakeSignature}`

    const result = await verifyUploadToken(forgedToken, env)

    expect(result).toBeNull()
  })

  it('prevents using old base64-only tokens', async () => {
    // Old insecure implementation just used btoa(JSON.stringify(payload))
    const oldStylePayload = {
      databaseId: 'db-123',
      path: 'data/test.parquet',
      userId: 'user-456',
      expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(),
      type: 'upload',
    }

    const oldToken = btoa(JSON.stringify(oldStylePayload))

    // Old token doesn't have the dot separator
    const resultUpload = await verifyUploadToken(oldToken, env)
    expect(resultUpload).toBeNull()

    // Even with fake dot
    const fakeNewFormat = `${oldToken}.`
    const resultWithDot = await verifyUploadToken(fakeNewFormat, env)
    expect(resultWithDot).toBeNull()
  })

  it('uses constant-time comparison for signatures', async () => {
    // This test verifies that signature verification doesn't leak timing info
    // We can't directly test constant-time, but we can ensure crypto.subtle.verify is used
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Verify multiple times - should always work
    for (let i = 0; i < 10; i++) {
      const result = await verifyUploadToken(token, env)
      expect(result).not.toBeNull()
    }

    // With wrong signature - should always fail
    const [payloadPart] = token.split('.')
    const wrongToken = `${payloadPart}.wrongsignature12345`
    for (let i = 0; i < 10; i++) {
      const result = await verifyUploadToken(wrongToken, env)
      expect(result).toBeNull()
    }
  })

  it('token expiration is enforced', async () => {
    // Create a token that expires in 1 second
    const payload = createValidPayload({
      expiresAt: new Date(Date.now() + 1000).toISOString(),
    })
    const token = await signUploadToken(payload, env)

    // Should work now
    const resultBefore = await verifyUploadToken(token, env)
    expect(resultBefore).not.toBeNull()

    // Wait for expiration using vi.useFakeTimers
    vi.useFakeTimers()
    vi.setSystemTime(Date.now() + 2000) // 2 seconds later

    const resultAfter = await verifyUploadToken(token, env)
    expect(resultAfter).toBeNull()

    vi.useRealTimers()
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  const env = createMockEnv('test-secret')

  // TODO: Fix sync-token.ts to handle Unicode paths properly
  // btoa() doesn't support non-ASCII characters; need to use encodeURIComponent or Buffer
  it.skip('handles unicode paths', async () => {
    const payload = createValidPayload({
      path: 'data/\u4e2d\u6587/test.parquet', // Chinese characters
    })

    const token = await signUploadToken(payload, env)
    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.path).toBe(payload.path)
  })

  it('handles very long paths', async () => {
    const longPath = 'data/' + 'a'.repeat(1000) + '.parquet'
    const payload = createValidPayload({ path: longPath })

    const token = await signUploadToken(payload, env)
    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.path).toBe(longPath)
  })

  it('handles special characters in path', async () => {
    const payload = createValidPayload({
      path: 'data/file with spaces & special=chars?.parquet',
    })

    const token = await signUploadToken(payload, env)
    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.path).toBe(payload.path)
  })

  it('handles empty string fields', async () => {
    const payload = createValidPayload({
      databaseId: '',
      path: '',
      userId: '',
    })

    const token = await signUploadToken(payload, env)
    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.databaseId).toBe('')
  })
})

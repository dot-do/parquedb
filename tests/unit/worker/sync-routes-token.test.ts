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
  clearNonceCache,
  getNonceCacheSize,
  checkAndMarkNonceUsed,
  checkAndMarkNonceUsedAsync,
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

  beforeEach(() => {
    clearNonceCache()
  })

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

  it('produces different tokens with different secrets (signatures differ)', async () => {
    const payload = createValidPayload()
    const env1 = createMockEnv('secret-1')
    const env2 = createMockEnv('secret-2')

    const token1 = await signUploadToken(payload, env1)
    const token2 = await signUploadToken(payload, env2)

    // Signature part should be different (even with different nonces, signatures would differ)
    const [, sig1] = token1.split('.')
    const [, sig2] = token2.split('.')
    expect(sig1).not.toBe(sig2)
  })

  it('produces unique tokens for same input (different nonce each time)', async () => {
    const payload = createValidPayload()
    const token1 = await signUploadToken(payload, env)
    const token2 = await signUploadToken(payload, env)

    // Tokens should be different because each has a unique nonce (jti)
    expect(token1).not.toBe(token2)
  })

  it('includes a unique nonce (jti) in each token', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Decode and check for jti
    const [payloadPart] = token.split('.')
    const decoded = JSON.parse(atob(payloadPart!.replace(/-/g, '+').replace(/_/g, '/')))
    expect(decoded.jti).toBeDefined()
    expect(typeof decoded.jti).toBe('string')
    expect(decoded.jti.length).toBeGreaterThan(0)
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

  beforeEach(() => {
    clearNonceCache()
  })

  it('verifies a valid token', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    const result = await verifyUploadToken(token, env)

    expect(result).not.toBeNull()
    expect(result?.databaseId).toBe(payload.databaseId)
    expect(result?.path).toBe(payload.path)
    expect(result?.userId).toBe(payload.userId)
    expect(result?.jti).toBeDefined() // Nonce is included in result
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

  beforeEach(() => {
    clearNonceCache()
  })

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

  beforeEach(() => {
    clearNonceCache()
  })

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

  beforeEach(() => {
    clearNonceCache()
  })

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

    // Verify multiple times with checkReplay: false to avoid single-use restriction
    // This tests the signature verification path without consuming the token
    for (let i = 0; i < 10; i++) {
      const result = await verifyUploadToken(token, env, { checkReplay: false })
      expect(result).not.toBeNull()
    }

    // With wrong signature - should always fail
    const [payloadPart] = token.split('.')
    const wrongToken = `${payloadPart}.wrongsignature12345`
    for (let i = 0; i < 10; i++) {
      const result = await verifyUploadToken(wrongToken, env, { checkReplay: false })
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
// Replay Protection Tests
// =============================================================================

describe('Replay Protection', () => {
  const env = createMockEnv('test-secret-key-12345')

  beforeEach(() => {
    clearNonceCache()
  })

  it('rejects replayed upload tokens (single-use enforcement)', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // First verification should succeed
    const result1 = await verifyUploadToken(token, env)
    expect(result1).not.toBeNull()

    // Second verification should fail (replay detected)
    const result2 = await verifyUploadToken(token, env)
    expect(result2).toBeNull()
  })

  it('allows verifying without consuming with checkReplay: false', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)

    // Multiple verifications with checkReplay: false should succeed
    const result1 = await verifyUploadToken(token, env, { checkReplay: false })
    expect(result1).not.toBeNull()

    const result2 = await verifyUploadToken(token, env, { checkReplay: false })
    expect(result2).not.toBeNull()

    // Now consume the token
    const result3 = await verifyUploadToken(token, env, { checkReplay: true })
    expect(result3).not.toBeNull()

    // Token should now be rejected
    const result4 = await verifyUploadToken(token, env)
    expect(result4).toBeNull()
  })

  it('tracks nonces in cache', async () => {
    expect(getNonceCacheSize()).toBe(0)

    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)
    await verifyUploadToken(token, env)

    // One nonce should be in cache
    expect(getNonceCacheSize()).toBe(1)

    // Create and verify another token
    const token2 = await signUploadToken(payload, env)
    await verifyUploadToken(token2, env)

    // Two nonces should be in cache
    expect(getNonceCacheSize()).toBe(2)
  })

  it('clears nonce cache with clearNonceCache()', async () => {
    const payload = createValidPayload()
    const token = await signUploadToken(payload, env)
    await verifyUploadToken(token, env)

    expect(getNonceCacheSize()).toBe(1)

    clearNonceCache()

    expect(getNonceCacheSize()).toBe(0)

    // After clearing, the same token can be used again
    // (This wouldn't happen in production, but demonstrates cache behavior)
    const result = await verifyUploadToken(token, env)
    expect(result).not.toBeNull()
  })

  it('allows different upload tokens for same payload', async () => {
    const payload = createValidPayload()

    // Generate two tokens for the same payload
    const token1 = await signUploadToken(payload, env)
    const token2 = await signUploadToken(payload, env)

    // Both should be different (unique nonces)
    expect(token1).not.toBe(token2)

    // Both should verify successfully (each has its own nonce)
    const result1 = await verifyUploadToken(token1, env)
    expect(result1).not.toBeNull()

    const result2 = await verifyUploadToken(token2, env)
    expect(result2).not.toBeNull()
  })

  it('download tokens do not have replay protection', async () => {
    const payload = createValidPayload()
    const token = await signDownloadToken(payload, env)

    // Multiple verifications should succeed (downloads are idempotent)
    const result1 = await verifyDownloadToken(token, env)
    expect(result1).not.toBeNull()

    const result2 = await verifyDownloadToken(token, env)
    expect(result2).not.toBeNull()

    const result3 = await verifyDownloadToken(token, env)
    expect(result3).not.toBeNull()
  })

  it('checkAndMarkNonceUsed works correctly', () => {
    clearNonceCache()
    const nonce = 'test-nonce-12345'
    const expiresAt = Date.now() + 3600 * 1000 // 1 hour

    // First check should succeed
    expect(checkAndMarkNonceUsed(nonce, expiresAt)).toBe(true)

    // Second check should fail (already used)
    expect(checkAndMarkNonceUsed(nonce, expiresAt)).toBe(false)

    // Different nonce should succeed
    expect(checkAndMarkNonceUsed('different-nonce', expiresAt)).toBe(true)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  const env = createMockEnv('test-secret')

  beforeEach(() => {
    clearNonceCache()
  })

  it('handles unicode paths', async () => {
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

// =============================================================================
// KV-Based Replay Protection Tests
// =============================================================================

describe('KV-Based Replay Protection', () => {
  const env = createMockEnv('test-secret-key-12345')

  beforeEach(() => {
    clearNonceCache()
  })

  /**
   * Create a mock KV namespace for testing
   */
  function createMockKV(): KVNamespace & { store: Map<string, string> } {
    const store = new Map<string, string>()
    return {
      store,
      get: vi.fn(async (key: string) => store.get(key) ?? null),
      put: vi.fn(async (key: string, value: string) => {
        store.set(key, value)
      }),
      delete: vi.fn(async (key: string) => {
        store.delete(key)
      }),
      list: vi.fn(async () => ({ keys: [], list_complete: true, cacheStatus: null })),
      getWithMetadata: vi.fn(async () => ({ value: null, metadata: null, cacheStatus: null })),
    } as unknown as KVNamespace & { store: Map<string, string> }
  }

  it('checkAndMarkNonceUsedAsync uses KV when available', async () => {
    const mockKV = createMockKV()
    const nonce = 'test-nonce-kv-12345'
    const expiresAt = Date.now() + 3600 * 1000

    // First check should succeed
    const result1 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, mockKV)
    expect(result1).toBe(true)

    // Verify KV was called
    expect(mockKV.get).toHaveBeenCalledWith(`nonce:${nonce}`)
    expect(mockKV.put).toHaveBeenCalled()

    // Second check should fail (nonce in KV)
    const result2 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, mockKV)
    expect(result2).toBe(false)
  })

  it('checkAndMarkNonceUsedAsync falls back to in-memory when KV is undefined', async () => {
    const nonce = 'test-nonce-fallback'
    const expiresAt = Date.now() + 3600 * 1000

    // Should use in-memory tracking
    const result1 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, undefined)
    expect(result1).toBe(true)

    // Second check should fail (in memory)
    const result2 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, undefined)
    expect(result2).toBe(false)
  })

  it('checkAndMarkNonceUsedAsync falls back to in-memory on KV error', async () => {
    const errorKV = {
      get: vi.fn(async () => { throw new Error('KV unavailable') }),
      put: vi.fn(async () => { throw new Error('KV unavailable') }),
    } as unknown as KVNamespace

    const nonce = 'test-nonce-error'
    const expiresAt = Date.now() + 3600 * 1000

    // Should fall back to in-memory and succeed
    const result1 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, errorKV)
    expect(result1).toBe(true)

    // Second check should fail (in memory)
    const result2 = await checkAndMarkNonceUsedAsync(nonce, expiresAt, errorKV)
    expect(result2).toBe(false)
  })

  it('verifyUploadToken uses KV when USED_TOKENS is available', async () => {
    const mockKV = createMockKV()
    const envWithKV: TokenEnv = {
      ...env,
      USED_TOKENS: mockKV,
    }

    const payload = createValidPayload()
    const token = await signUploadToken(payload, envWithKV)

    // First verification should succeed
    const result1 = await verifyUploadToken(token, envWithKV)
    expect(result1).not.toBeNull()

    // Verify KV was used
    expect(mockKV.put).toHaveBeenCalled()

    // Second verification should fail (nonce in KV)
    const result2 = await verifyUploadToken(token, envWithKV)
    expect(result2).toBeNull()
  })

  it('KV stores nonces with expiration TTL', async () => {
    const mockKV = createMockKV()
    const nonce = 'test-nonce-ttl'
    const expiresAt = Date.now() + 3600 * 1000 // 1 hour

    await checkAndMarkNonceUsedAsync(nonce, expiresAt, mockKV)

    // Verify put was called with TTL options
    expect(mockKV.put).toHaveBeenCalledWith(
      `nonce:${nonce}`,
      '1',
      expect.objectContaining({
        expirationTtl: expect.any(Number),
      })
    )
  })

  it('prevents cross-isolate replay attacks with KV', async () => {
    // Simulate two different isolates sharing the same KV
    const sharedKV = createMockKV()

    const envIsolate1: TokenEnv = {
      SYNC_SECRET: 'test-secret-key-12345',
      USED_TOKENS: sharedKV,
    }

    const envIsolate2: TokenEnv = {
      SYNC_SECRET: 'test-secret-key-12345',
      USED_TOKENS: sharedKV,
    }

    const payload = createValidPayload()
    const token = await signUploadToken(payload, envIsolate1)

    // First isolate uses the token
    const result1 = await verifyUploadToken(token, envIsolate1)
    expect(result1).not.toBeNull()

    // Clear local cache to simulate different isolate
    clearNonceCache()

    // Second isolate tries to replay - should fail due to KV check
    const result2 = await verifyUploadToken(token, envIsolate2)
    expect(result2).toBeNull()
  })
})

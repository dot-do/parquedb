/**
 * Sync Token Signing Module
 *
 * Provides HMAC-SHA256 signed tokens for secure upload/download URLs.
 * Tokens are used by sync-routes.ts to authorize file uploads and downloads.
 *
 * Token format: base64url(payload).base64url(hmac-sha256-signature)
 *
 * Security features:
 * - HMAC-SHA256 signature prevents tampering and forgery
 * - Type field prevents upload/download token confusion
 * - Expiration time limits token validity window (with 5s clock skew tolerance)
 * - Single-use nonces prevent replay attacks (see REPLAY_PROTECTION_NOTE below)
 * - Base64url encoding ensures URL safety
 *
 * REPLAY_PROTECTION_NOTE:
 * Upload tokens include a unique nonce (jti) that is tracked to prevent replay attacks.
 *
 * Two levels of protection are available:
 *
 * 1. In-memory tracking (default, always enabled):
 *    - Nonces are NOT shared across Worker isolates
 *    - Provides protection within the same isolate (most common case)
 *    - Falls back to this when KV is unavailable
 *
 * 2. KV-based tracking (recommended for production):
 *    - Enabled by configuring USED_TOKENS KV namespace binding
 *    - Provides cross-isolate replay protection
 *    - Nonces are stored with automatic TTL expiration
 *    - Falls back gracefully to in-memory on KV errors
 *
 * To enable KV-based protection, add to wrangler.toml:
 *   [[kv_namespaces]]
 *   binding = "USED_TOKENS"
 *   id = "your-kv-namespace-id"
 */

import { logger } from '../utils/logger'
import {
  MAX_NONCE_CACHE_SIZE as IMPORTED_MAX_NONCE_CACHE_SIZE,
  NONCE_CLEANUP_THRESHOLD,
  CLOCK_SKEW_TOLERANCE_MS,
} from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Token payload for upload/download signed URLs
 */
export interface TokenPayload {
  databaseId: string
  path: string
  userId: string
  expiresAt: string
  /** Unique token ID (nonce) for replay protection */
  jti?: string | undefined
}

/**
 * Internal token data including type and nonce
 */
interface TokenData extends TokenPayload {
  type: 'upload' | 'download'
  /** Unique token ID (nonce) for replay protection - required internally */
  jti: string
}

/**
 * Environment interface for token operations
 * Only requires SYNC_SECRET from the full Env
 */
export interface TokenEnv {
  SYNC_SECRET?: string | undefined
  /**
   * Optional KV namespace for cross-isolate nonce tracking.
   * When provided, used nonces are stored in KV for stronger replay protection.
   * Falls back to in-memory tracking when not available.
   */
  USED_TOKENS?: KVNamespace | undefined
}

// =============================================================================
// Replay Protection (Single-Use Token Tracking)
// =============================================================================

/**
 * In-memory cache for used upload token nonces.
 *
 * NOTE: This cache is per-isolate and won't prevent cross-isolate replay.
 * See REPLAY_PROTECTION_NOTE at the top of this file for details.
 *
 * Structure: Map<nonce, expiresAt timestamp>
 * Expired entries are lazily cleaned up during cache operations.
 */
const usedTokenNonces = new Map<string, number>()

/** Maximum cache size before forced cleanup */
const MAX_NONCE_CACHE_SIZE = IMPORTED_MAX_NONCE_CACHE_SIZE

/** Cleanup interval - remove expired entries when cache grows */
const CLEANUP_THRESHOLD = NONCE_CLEANUP_THRESHOLD

/**
 * Generate a unique nonce for a token
 * Uses crypto.randomUUID() for strong uniqueness
 */
function generateNonce(): string {
  return crypto.randomUUID()
}

/**
 * Check if a nonce has been used and mark it as used if not (in-memory version).
 * Returns true if the nonce is valid (not previously used), false if replay detected.
 *
 * @param nonce - The token nonce to check
 * @param expiresAt - When the token expires (for cache cleanup)
 * @returns true if nonce is valid, false if already used (replay attack)
 */
export function checkAndMarkNonceUsed(nonce: string, expiresAt: number): boolean {
  // Periodic cleanup when cache grows large
  if (usedTokenNonces.size > CLEANUP_THRESHOLD) {
    cleanupExpiredNonces()
  }

  // Force cleanup if we hit max size (prevents memory issues)
  if (usedTokenNonces.size >= MAX_NONCE_CACHE_SIZE) {
    cleanupExpiredNonces()
    // If still too large after cleanup, reject (fail safe)
    if (usedTokenNonces.size >= MAX_NONCE_CACHE_SIZE) {
      logger.warn('[sync-token] Nonce cache full, rejecting token as safety measure')
      return false
    }
  }

  // Check if already used
  if (usedTokenNonces.has(nonce)) {
    return false // Replay detected!
  }

  // Mark as used
  usedTokenNonces.set(nonce, expiresAt)
  return true
}

/**
 * Check if a nonce has been used and mark it as used if not (KV-backed version).
 * Provides cross-isolate replay protection using Cloudflare KV.
 *
 * When USED_TOKENS KV is available, this provides stronger replay protection
 * across all Worker isolates. Falls back to in-memory tracking if KV is unavailable.
 *
 * @param nonce - The token nonce to check
 * @param expiresAt - When the token expires (for KV TTL)
 * @param kv - Optional KV namespace for cross-isolate tracking
 * @returns true if nonce is valid, false if already used (replay attack)
 */
export async function checkAndMarkNonceUsedAsync(
  nonce: string,
  expiresAt: number,
  kv?: KVNamespace
): Promise<boolean> {
  // If KV is not available, fall back to in-memory tracking
  if (!kv) {
    return checkAndMarkNonceUsed(nonce, expiresAt)
  }

  try {
    // Key format: "nonce:{jti}" to avoid conflicts with other KV usage
    const key = `nonce:${nonce}`

    // Check if nonce exists in KV
    const existing = await kv.get(key)
    if (existing !== null) {
      // Nonce already used - replay detected!
      logger.warn('[sync-token] KV replay detected for nonce:', nonce.slice(0, 8) + '...')
      return false
    }

    // Calculate TTL in seconds (KV requires seconds, not milliseconds)
    // Add 60 seconds buffer to ensure token expires before KV entry
    const now = Date.now()
    const ttlSeconds = Math.max(60, Math.ceil((expiresAt - now) / 1000) + 60)

    // Mark nonce as used in KV with expiration
    // The value is just "1" - we only care about existence
    await kv.put(key, '1', { expirationTtl: ttlSeconds })

    // Also mark in memory for faster checking within same isolate
    checkAndMarkNonceUsed(nonce, expiresAt)

    return true
  } catch (error) {
    // On KV error, fall back to in-memory tracking
    // Log error but don't fail the request - better to have some protection than none
    logger.error('[sync-token] KV error, falling back to in-memory:', error)
    return checkAndMarkNonceUsed(nonce, expiresAt)
  }
}

/**
 * Remove expired nonces from the cache
 */
function cleanupExpiredNonces(): void {
  const now = Date.now()
  for (const [nonce, expiresAt] of usedTokenNonces) {
    if (expiresAt < now) {
      usedTokenNonces.delete(nonce)
    }
  }
}

/**
 * Clear the nonce cache (for testing purposes)
 * @internal
 */
export function clearNonceCache(): void {
  usedTokenNonces.clear()
}

/**
 * Get the current nonce cache size (for testing purposes)
 * @internal
 */
export function getNonceCacheSize(): number {
  return usedTokenNonces.size
}

// =============================================================================
// Unicode-safe Base64 Encoding/Decoding
// =============================================================================

/**
 * Encode a string to base64url, supporting Unicode characters.
 * Uses TextEncoder to convert to UTF-8 bytes first.
 */
function encodeBase64Url(str: string): string {
  const encoder = new TextEncoder()
  const bytes = encoder.encode(str)
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/**
 * Decode a base64url string back to a Unicode string.
 * Uses TextDecoder to interpret the bytes as UTF-8.
 */
function decodeBase64Url(base64url: string): string {
  const base64 = base64url.replace(/-/g, '+').replace(/_/g, '/')
  const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4)
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  const decoder = new TextDecoder()
  return decoder.decode(bytes)
}

// =============================================================================
// HMAC Key Management
// =============================================================================

/**
 * Get the HMAC key from the environment
 * @throws Error if SYNC_SECRET is not configured
 */
async function getHmacKey(env: TokenEnv): Promise<CryptoKey> {
  if (!env.SYNC_SECRET) {
    throw new Error('SYNC_SECRET environment variable is required for token signing')
  }

  const encoder = new TextEncoder()
  const keyData = encoder.encode(env.SYNC_SECRET)

  return crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  )
}

// =============================================================================
// HMAC Operations
// =============================================================================

/**
 * Sign data with HMAC-SHA256 and return base64url-encoded signature
 */
async function hmacSign(data: string, key: CryptoKey): Promise<string> {
  const encoder = new TextEncoder()
  const dataBuffer = encoder.encode(data)

  const signature = await crypto.subtle.sign('HMAC', key, dataBuffer)
  const signatureArray = new Uint8Array(signature)

  // Convert to base64url
  let binary = ''
  for (const byte of signatureArray) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/**
 * Verify HMAC-SHA256 signature
 */
async function hmacVerify(data: string, signature: string, key: CryptoKey): Promise<boolean> {
  try {
    const encoder = new TextEncoder()
    const dataBuffer = encoder.encode(data)

    // Convert base64url signature back to ArrayBuffer
    const base64 = signature.replace(/-/g, '+').replace(/_/g, '/')
    const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4)
    const binary = atob(padded)
    const signatureBuffer = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
      signatureBuffer[i] = binary.charCodeAt(i)
    }

    return crypto.subtle.verify('HMAC', key, signatureBuffer, dataBuffer)
  } catch {
    return false
  }
}

// =============================================================================
// Token Signing
// =============================================================================

/**
 * Sign an upload token using HMAC-SHA256
 * Token format: base64url(payload).base64url(signature)
 *
 * Each upload token includes a unique nonce (jti) for replay protection.
 * The nonce is checked when the token is verified to prevent reuse.
 */
export async function signUploadToken(payload: TokenPayload, env: TokenEnv): Promise<string> {
  // Generate unique nonce for replay protection
  const jti = generateNonce()

  const data = JSON.stringify({
    ...payload,
    type: 'upload',
    jti,
  })

  const key = await getHmacKey(env)
  const payloadEncoded = encodeBase64Url(data)
  const signature = await hmacSign(data, key)

  return `${payloadEncoded}.${signature}`
}

/**
 * Sign a download token using HMAC-SHA256
 * Token format: base64url(payload).base64url(signature)
 */
export async function signDownloadToken(payload: TokenPayload, env: TokenEnv): Promise<string> {
  const data = JSON.stringify({
    ...payload,
    type: 'download',
  })

  const key = await getHmacKey(env)
  const payloadEncoded = encodeBase64Url(data)
  const signature = await hmacSign(data, key)

  return `${payloadEncoded}.${signature}`
}

// =============================================================================
// Token Verification
// =============================================================================

/**
 * Verify an upload token
 * Returns the payload if valid, null otherwise.
 *
 * Upload tokens are single-use: once verified successfully, the token's nonce
 * is marked as used and subsequent verification attempts will fail.
 *
 * When USED_TOKENS KV is available in env, provides cross-isolate replay protection.
 * Otherwise falls back to in-memory tracking (per-isolate only).
 *
 * @param token - The upload token to verify
 * @param env - Environment with SYNC_SECRET and optional USED_TOKENS KV
 * @param options - Verification options
 * @param options.checkReplay - Whether to check and mark the nonce (default: true)
 *                              Set to false only for testing token validity without consuming it
 */
export async function verifyUploadToken(
  token: string,
  env: TokenEnv,
  options?: { checkReplay?: boolean | undefined }
): Promise<TokenPayload | null> {
  const checkReplay = options?.checkReplay !== false
  return verifyToken(token, 'upload', env, checkReplay)
}

/**
 * Verify a download token
 * Returns the payload if valid, null otherwise
 */
export async function verifyDownloadToken(token: string, env: TokenEnv): Promise<TokenPayload | null> {
  return verifyToken(token, 'download', env)
}

/**
 * Internal token verification
 *
 * @param token - The token to verify
 * @param expectedType - Expected token type ('upload' or 'download')
 * @param env - Environment with SYNC_SECRET and optional USED_TOKENS KV
 * @param checkReplay - For upload tokens, check and mark the nonce as used
 */
async function verifyToken(
  token: string,
  expectedType: 'upload' | 'download',
  env: TokenEnv,
  checkReplay = true
): Promise<TokenPayload | null> {
  try {
    const parts = token.split('.')
    if (parts.length !== 2) {
      return null
    }

    const [payloadEncoded, signature] = parts as [string, string]

    // Decode payload using Unicode-safe decoding
    const jsonStr = decodeBase64Url(payloadEncoded)
    const data = JSON.parse(jsonStr) as TokenData

    // Verify type
    if (data.type !== expectedType) {
      return null
    }

    // Check expiration with clock skew tolerance (5 seconds)
    // This prevents issues with slight clock differences between systems
    const expiresAt = new Date(data.expiresAt)
    const now = new Date()
    if (expiresAt.getTime() + CLOCK_SKEW_TOLERANCE_MS < now.getTime()) {
      logger.debug('[sync-token] Token expired:', { expiresAt: data.expiresAt, now: now.toISOString() })
      return null
    }

    // Verify HMAC signature
    const key = await getHmacKey(env)
    const isValid = await hmacVerify(jsonStr, signature, key)
    if (!isValid) {
      return null
    }

    // For upload tokens, check replay protection
    // Upload tokens must have a nonce (jti) and can only be used once
    if (expectedType === 'upload' && checkReplay) {
      if (!data.jti) {
        // Legacy tokens without nonce are rejected for security
        logger.warn('[sync-token] Upload token missing nonce (jti), rejecting')
        return null
      }

      // Check and mark the nonce as used (with KV if available for cross-isolate protection)
      const isNonceValid = await checkAndMarkNonceUsedAsync(data.jti, expiresAt.getTime(), env.USED_TOKENS)
      if (!isNonceValid) {
        logger.warn('[sync-token] Token replay detected, nonce already used:', data.jti.slice(0, 8) + '...')
        return null
      }
    }

    return {
      databaseId: data.databaseId,
      path: data.path,
      userId: data.userId,
      expiresAt: data.expiresAt,
      jti: data.jti,
    }
  } catch {
    return null
  }
}

/**
 * JWT Utilities for Worker Authentication
 *
 * Provides secure JWT token verification using the jose library.
 * Tokens are verified against the JWKS endpoint specified in JWKS_URI.
 *
 * SECURITY: These functions perform cryptographic signature verification.
 * Never use simple base64 decoding without signature verification for auth.
 */

import { createRemoteJWKSet, jwtVerify, type JWTPayload } from 'jose'
import type { Env } from '../types/worker'
import { JWKS_CACHE_TTL as IMPORTED_JWKS_CACHE_TTL, JWKS_FETCH_TIMEOUT_MS as IMPORTED_JWKS_FETCH_TIMEOUT_MS } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * User information extracted from a verified JWT
 */
export interface JWTUserInfo {
  /** User ID (from 'sub' claim) */
  id: string
  /** Username (from 'username' or 'preferred_username' claim) */
  username?: string
  /** Email (from 'email' claim) */
  email?: string
}

/**
 * Result of JWT verification
 */
export interface JWTVerifyResult {
  valid: boolean
  user?: JWTUserInfo
  payload?: JWTPayload
  error?: string
}

// =============================================================================
// JWKS Cache
// =============================================================================

// JWKS cache to avoid fetching on every request
const jwksCache = new Map<string, { jwks: ReturnType<typeof createRemoteJWKSet>; expiresAt: number }>()
const JWKS_CACHE_TTL = IMPORTED_JWKS_CACHE_TTL
const JWKS_FETCH_TIMEOUT_MS = IMPORTED_JWKS_FETCH_TIMEOUT_MS

/**
 * Custom error for JWKS fetch timeout
 */
export class JWKSFetchTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`JWKS fetch timed out after ${timeoutMs}ms`)
    this.name = 'JWKSFetchTimeoutError'
  }
}

/**
 * Get or create a cached JWKS instance for the given URI
 */
function getJWKS(jwksUri: string): ReturnType<typeof createRemoteJWKSet> {
  const cached = jwksCache.get(jwksUri)
  if (cached && cached.expiresAt > Date.now()) {
    return cached.jwks
  }

  const jwks = createRemoteJWKSet(new URL(jwksUri), {
    timeoutDuration: JWKS_FETCH_TIMEOUT_MS,
  })
  jwksCache.set(jwksUri, { jwks, expiresAt: Date.now() + JWKS_CACHE_TTL })
  return jwks
}

/**
 * Race a promise against an AbortController timeout.
 * This provides protection against hung connections that the jose library's
 * timeoutDuration may not catch.
 */
async function withAbortTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number
): Promise<T> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const result = await operation(controller.signal)
    return result
  } finally {
    clearTimeout(timeoutId)
  }
}

/**
 * Verify JWT with timeout protection for JWKS fetch.
 * Wraps jwtVerify with an AbortController to handle hung connections.
 */
async function verifyJWTWithTimeout(
  token: string,
  jwks: ReturnType<typeof createRemoteJWKSet>,
  options: { clockTolerance?: number },
  timeoutMs: number = JWKS_FETCH_TIMEOUT_MS
): Promise<Awaited<ReturnType<typeof jwtVerify>>> {
  return withAbortTimeout(
    async (signal) => {
      // Create a promise that rejects on abort
      const abortPromise = new Promise<never>((_, reject) => {
        signal.addEventListener('abort', () => {
          reject(new JWKSFetchTimeoutError(timeoutMs))
        }, { once: true })
      })

      // Race the verification against the abort signal
      return Promise.race([
        jwtVerify(token, jwks, options),
        abortPromise,
      ])
    },
    timeoutMs
  )
}

// =============================================================================
// Token Verification
// =============================================================================

/**
 * Verify a JWT token and extract user information.
 *
 * This function performs cryptographic signature verification using the JWKS
 * endpoint configured in the environment. It also checks token expiration.
 *
 * @param token - The JWT token to verify
 * @param env - Worker environment containing JWKS_URI
 * @returns Verification result with user info if valid
 */
export async function verifyJWT(token: string, env: Env): Promise<JWTVerifyResult> {
  // Check for JWKS_URI configuration
  if (!env.JWKS_URI) {
    return {
      valid: false,
      error: 'JWKS_URI is not configured. Contact administrator.',
    }
  }

  try {
    const jwks = getJWKS(env.JWKS_URI)

    // Use timeout-protected verification to handle hung connections
    const { payload } = await verifyJWTWithTimeout(token, jwks, {
      clockTolerance: 60, // Allow 60 seconds clock skew
    }, JWKS_FETCH_TIMEOUT_MS)

    // Extract user information from payload
    const user: JWTUserInfo = {
      id: (payload.sub as string) ?? (payload.id as string),
      username: (payload.username as string) ?? (payload.preferred_username as string),
      email: payload.email as string | undefined,
    }

    // Ensure we have at least an ID
    if (!user.id) {
      return {
        valid: false,
        error: 'Token missing required user ID claim',
      }
    }

    return {
      valid: true,
      user,
      payload,
    }
  } catch (error) {
    // Provide specific error message for timeout errors
    if (error instanceof JWKSFetchTimeoutError) {
      return {
        valid: false,
        error: error.message,
      }
    }
    return {
      valid: false,
      error: error instanceof Error ? error.message : 'Token verification failed',
    }
  }
}

/**
 * Verify a JWT token and check if it belongs to a specific owner.
 *
 * Compares the user identity from the verified JWT with the requested owner.
 * Supports common JWT claims: sub, username, preferred_username.
 *
 * @param token - The JWT token to verify (or null if not provided)
 * @param owner - The expected owner to check against
 * @param env - Worker environment containing JWKS_URI
 * @returns True if the token is valid and belongs to the owner
 */
export async function verifyOwnership(
  token: string | null,
  owner: string,
  env: Env
): Promise<boolean> {
  if (!token) return false

  const result = await verifyJWT(token, env)
  if (!result.valid || !result.payload) return false

  // Normalize owner for comparison (case-insensitive)
  const normalizedOwner = owner.toLowerCase()

  // Check common identity claims
  // 'sub' is the standard JWT subject claim (usually user ID)
  if (typeof result.payload.sub === 'string' && result.payload.sub.toLowerCase() === normalizedOwner) {
    return true
  }

  // 'username' is commonly used by oauth providers
  if (typeof result.payload.username === 'string' && (result.payload.username as string).toLowerCase() === normalizedOwner) {
    return true
  }

  // 'preferred_username' is the OIDC standard claim
  if (typeof result.payload.preferred_username === 'string' && (result.payload.preferred_username as string).toLowerCase() === normalizedOwner) {
    return true
  }

  return false
}

/**
 * Extract Bearer token from Authorization header
 */
export function extractBearerToken(request: Request): string | null {
  const auth = request.headers.get('Authorization')
  if (!auth?.startsWith('Bearer ')) {
    return null
  }
  return auth.slice(7)
}

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
const JWKS_CACHE_TTL = 3600 * 1000 // 1 hour
const JWKS_FETCH_TIMEOUT_MS = 10000 // 10 second timeout for JWKS fetch

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

    const { payload } = await jwtVerify(token, jwks, {
      clockTolerance: 60, // Allow 60 seconds clock skew
    })

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

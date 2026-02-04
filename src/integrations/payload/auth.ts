/**
 * Payload CMS OAuth.do Authentication Adapter
 *
 * Integrates oauth.do JWT authentication with Payload CMS.
 * Replaces Payload's built-in auth with oauth.do token verification.
 *
 * @example
 * ```typescript
 * import { buildConfig } from 'payload/config'
 * import { parquedbAdapter } from 'parquedb/payload'
 * import { oauthStrategy, oauthUsers } from 'parquedb/payload'
 *
 * export default buildConfig({
 *   db: parquedbAdapter({ storage }),
 *   admin: {
 *     user: 'users',
 *   },
 *   collections: [
 *     oauthUsers({
 *       jwksUri: process.env.WORKOS_JWKS_URI!,
 *       adminRoles: ['admin'],
 *     }),
 *   ],
 * })
 * ```
 */

import type { EntityId } from '../../types'
import { asJWTPayload } from '../../types/cast'
import {
  JWKS_CACHE_TTL as IMPORTED_JWKS_CACHE_TTL,
  JWKS_FETCH_TIMEOUT_MS as IMPORTED_JWKS_FETCH_TIMEOUT_MS,
} from '../../constants'
import { LRUCache } from '../../utils/ttl-cache'

// =============================================================================
// Types
// =============================================================================

/**
 * OAuth.do JWT payload structure
 */
export interface OAuthJWTPayload {
  /** User ID (subject) */
  sub: string
  /** Email address */
  email?: string | undefined
  /** Display name */
  name?: string | undefined
  /** First name */
  firstName?: string | undefined
  /** Last name */
  lastName?: string | undefined
  /** Profile picture URL */
  profilePictureUrl?: string | undefined
  /** Organization ID */
  org_id?: string | undefined
  /** User roles */
  roles?: string[] | undefined
  /** Fine-grained permissions */
  permissions?: string[] | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
  /** Issuer */
  iss: string
  /** Audience (client ID) */
  aud: string | string[]
  /** Expiration timestamp */
  exp: number
  /** Issued at timestamp */
  iat: number
}

/**
 * Payload user document synced from oauth.do
 */
export interface OAuthUser {
  id: string
  email: string
  name?: string | undefined
  firstName?: string | undefined
  lastName?: string | undefined
  profilePictureUrl?: string | undefined
  roles?: string[] | undefined
  permissions?: string[] | undefined
  organizationId?: string | undefined
  externalId: string
  lastLoginAt?: Date | undefined
  createdAt: Date
  updatedAt: Date
}

/**
 * Configuration for oauth.do authentication
 */
export interface OAuthConfig {
  /**
   * WorkOS JWKS URI for token verification
   * @example 'https://api.workos.com/sso/jwks/client_xxx'
   */
  jwksUri: string

  /**
   * OAuth client ID (audience claim in JWT)
   * If not provided, audience check is skipped
   */
  clientId?: string | undefined

  /**
   * Cookie name for oauth.do token
   * @default 'auth'
   */
  cookieName?: string | undefined

  /**
   * Roles that grant admin access
   * Users without these roles cannot access Payload admin
   * @default ['admin']
   */
  adminRoles?: string[] | undefined

  /**
   * Roles that grant editor access (read/write but limited settings)
   * @default ['editor']
   */
  editorRoles?: string[] | undefined

  /**
   * Allow any authenticated user to access admin
   * @default false
   */
  allowAllAuthenticated?: boolean | undefined

  /**
   * Custom function to check if user can access admin
   */
  canAccessAdmin?: ((user: OAuthJWTPayload) => boolean | Promise<boolean>) | undefined

  /**
   * Sync user data to Payload on each login
   * @default true
   */
  syncUserOnLogin?: boolean | undefined

  /**
   * Clock tolerance for JWT verification (seconds)
   * @default 60
   */
  clockTolerance?: number | undefined
}

/**
 * Resolved configuration with defaults
 */
interface ResolvedOAuthConfig extends Required<Omit<OAuthConfig, 'clientId' | 'canAccessAdmin'>> {
  clientId?: string | undefined
  canAccessAdmin?: ((user: OAuthJWTPayload) => boolean | Promise<boolean>) | undefined
}

// =============================================================================
// Token Extraction
// =============================================================================

/**
 * Extract JWT token from request
 */
export function extractToken(
  request: Request,
  cookieName: string = 'auth'
): string | null {
  // Try Authorization header first
  const authHeader = request.headers.get('Authorization')
  if (authHeader?.startsWith('Bearer ')) {
    return authHeader.slice(7)
  }

  // Fall back to cookie
  const cookieHeader = request.headers.get('Cookie')
  if (cookieHeader) {
    const cookies = parseCookies(cookieHeader)
    const token = cookies[cookieName]
    if (token) {
      return token
    }
  }

  return null
}

/**
 * Parse cookie header into key-value pairs
 *
 * Handles:
 * - URL-encoded values (decodeURIComponent)
 * - Quoted string values per RFC 6265
 * - Values containing equals signs
 */
export function parseCookies(cookieHeader: string): Record<string, string> {
  const cookies: Record<string, string> = {}
  const pairs = cookieHeader.split(';')

  for (const pair of pairs) {
    const trimmedPair = pair.trim()
    if (!trimmedPair) continue

    const equalsIndex = trimmedPair.indexOf('=')
    if (equalsIndex === -1) continue

    const key = trimmedPair.slice(0, equalsIndex).trim()
    if (!key) continue

    let value = trimmedPair.slice(equalsIndex + 1)

    // Handle quoted strings per RFC 6265
    // Quoted values are wrapped in double quotes
    if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
      value = value.slice(1, -1)
    }

    // Decode URL-encoded values
    try {
      value = decodeURIComponent(value)
    } catch {
      // If decoding fails, use the raw value
      // This handles malformed percent-encoding gracefully
    }

    cookies[key] = value
  }

  return cookies
}

// =============================================================================
// JWT Verification
// =============================================================================

// JWKS cache to avoid fetching on every request
// Uses LRU cache with max 100 entries to prevent memory exhaustion
const JWKS_CACHE_MAX_ENTRIES = 100
const JWKS_CACHE_TTL = IMPORTED_JWKS_CACHE_TTL
const JWKS_FETCH_TIMEOUT_MS = IMPORTED_JWKS_FETCH_TIMEOUT_MS

const jwksCache = new LRUCache<string, ReturnType<typeof createRemoteJWKSet>>({
  maxEntries: JWKS_CACHE_MAX_ENTRIES,
  ttlMs: JWKS_CACHE_TTL,
  cacheId: 'jwks-cache',
})

// Import jose for JWT verification
import { createRemoteJWKSet, jwtVerify, type JWTVerifyResult, type JWTPayload, type JWTVerifyOptions } from 'jose'

/**
 * Custom error for JWKS fetch timeout
 */
export class JWKSFetchTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`JWKS fetch timed out after ${timeoutMs}ms`)
    this.name = 'JWKSFetchTimeoutError'
  }
}

function getJWKS(jwksUri: string) {
  const cached = jwksCache.get(jwksUri)
  if (cached) {
    return cached
  }

  const jwks = createRemoteJWKSet(new URL(jwksUri), {
    timeoutDuration: JWKS_FETCH_TIMEOUT_MS,
  })
  jwksCache.set(jwksUri, jwks)
  return jwks
}

/**
 * Get JWKS cache statistics for monitoring and testing.
 * Returns hit rate, eviction count, and current size.
 */
export function getJWKSCacheStats() {
  return jwksCache.getStats()
}

/**
 * Clear the JWKS cache. Useful for testing or when JWKS keys are rotated.
 */
export function clearJWKSCache() {
  jwksCache.clear()
}

/**
 * Get the maximum number of entries allowed in the JWKS cache.
 */
export const JWKS_CACHE_MAX_ENTRIES_VALUE = JWKS_CACHE_MAX_ENTRIES

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
  options: JWTVerifyOptions,
  timeoutMs: number = JWKS_FETCH_TIMEOUT_MS
): Promise<JWTVerifyResult<JWTPayload>> {
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

/**
 * Verify JWT token against oauth.do/WorkOS JWKS
 *
 * Uses AbortController timeout protection to handle hung JWKS fetch connections.
 */
export async function verifyOAuthToken(
  token: string,
  config: ResolvedOAuthConfig
): Promise<{ valid: boolean; payload?: OAuthJWTPayload | undefined; error?: string | undefined }> {
  try {
    const jwks = getJWKS(config.jwksUri)

    const verifyOptions: JWTVerifyOptions = {
      ...(config.clockTolerance ? { clockTolerance: config.clockTolerance } : {}),
      ...(config.clientId ? { audience: config.clientId } : {}),
    }

    // Use timeout-protected verification to handle hung connections
    const { payload } = await verifyJWTWithTimeout(token, jwks, verifyOptions, JWKS_FETCH_TIMEOUT_MS)

    return {
      valid: true,
      payload: asJWTPayload<OAuthJWTPayload>(payload),
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
      error: error instanceof Error ? error.message : 'Verification error',
    }
  }
}

// =============================================================================
// Access Control
// =============================================================================

/**
 * Check if user can access Payload admin
 */
export async function canAccessPayloadAdmin(
  payload: OAuthJWTPayload,
  config: ResolvedOAuthConfig
): Promise<boolean> {
  // Custom access check
  if (config.canAccessAdmin) {
    return config.canAccessAdmin(payload)
  }

  // Allow all authenticated users
  if (config.allowAllAuthenticated) {
    return true
  }

  // Check roles
  const userRoles = payload.roles || []
  const adminRoles = config.adminRoles ?? []
  const editorRoles = config.editorRoles ?? []

  // Check admin roles
  if (adminRoles.some(role => userRoles.includes(role))) {
    return true
  }

  // Check editor roles
  if (editorRoles.some(role => userRoles.includes(role))) {
    return true
  }

  return false
}

/**
 * Get user's Payload role based on oauth.do roles
 */
export function getPayloadRole(
  payload: OAuthJWTPayload,
  config: ResolvedOAuthConfig
): 'admin' | 'editor' | 'user' {
  const userRoles = payload.roles || []
  const adminRoles = config.adminRoles ?? []
  const editorRoles = config.editorRoles ?? []

  if (adminRoles.some(role => userRoles.includes(role))) {
    return 'admin'
  }

  if (editorRoles.some(role => userRoles.includes(role))) {
    return 'editor'
  }

  return 'user'
}

// =============================================================================
// Middleware
// =============================================================================

/**
 * Create oauth.do authentication middleware for Payload
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { createOAuthMiddleware } from 'parquedb/payload'
 *
 * const app = new Hono()
 *
 * // Apply to admin routes
 * app.use('/admin/*', createOAuthMiddleware({
 *   jwksUri: env.WORKOS_JWKS_URI,
 *   adminRoles: ['admin'],
 * }))
 * ```
 */
export function createOAuthMiddleware(userConfig: OAuthConfig) {
  const config: ResolvedOAuthConfig = {
    jwksUri: userConfig.jwksUri,
    clientId: userConfig.clientId,
    cookieName: userConfig.cookieName ?? 'auth',
    adminRoles: userConfig.adminRoles ?? ['admin'],
    editorRoles: userConfig.editorRoles ?? ['editor'],
    allowAllAuthenticated: userConfig.allowAllAuthenticated ?? false,
    canAccessAdmin: userConfig.canAccessAdmin,
    syncUserOnLogin: userConfig.syncUserOnLogin ?? true,
    clockTolerance: userConfig.clockTolerance ?? 60,
  }

  return async (c: { req: { raw: Request }; set: (key: string, value: unknown) => void; json: (data: unknown, status?: number) => Response }, next: () => Promise<void>) => {
    const request = c.req.raw

    // Extract token
    const token = extractToken(request, config.cookieName)

    if (!token) {
      // No token - let Payload handle (might be public route)
      return next()
    }

    // Verify token
    const result = await verifyOAuthToken(token, config)

    if (!result.valid || !result.payload) {
      // Invalid token - clear user and continue
      c.set('user', null)
      return next()
    }

    // Check admin access
    const hasAccess = await canAccessPayloadAdmin(result.payload, config)

    if (!hasAccess) {
      return c.json({ error: 'Insufficient permissions' }, 403)
    }

    // Build Payload user from JWT
    const payloadUser: OAuthUser = {
      id: result.payload.sub,
      email: result.payload.email || `${result.payload.sub}@oauth.do`,
      name: result.payload.name,
      firstName: result.payload.firstName,
      lastName: result.payload.lastName,
      profilePictureUrl: result.payload.profilePictureUrl,
      roles: result.payload.roles,
      permissions: result.payload.permissions,
      organizationId: result.payload.org_id,
      externalId: result.payload.sub,
      lastLoginAt: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    // Set user on context
    c.set('user', payloadUser)

    return next()
  }
}

// =============================================================================
// Payload Collection Config
// =============================================================================

/**
 * Create a Payload users collection configured for oauth.do
 *
 * This replaces Payload's default users collection with one that:
 * - Uses oauth.do for authentication
 * - Syncs user data from JWT claims
 * - Supports role-based access control
 *
 * @example
 * ```typescript
 * import { buildConfig } from 'payload/config'
 * import { oauthUsers } from 'parquedb/payload'
 *
 * export default buildConfig({
 *   admin: { user: 'users' },
 *   collections: [
 *     oauthUsers({
 *       jwksUri: process.env.WORKOS_JWKS_URI!,
 *       adminRoles: ['admin'],
 *     }),
 *     // ... other collections
 *   ],
 * })
 * ```
 */
export function oauthUsers(config: OAuthConfig) {
  const resolvedConfig: ResolvedOAuthConfig = {
    jwksUri: config.jwksUri,
    clientId: config.clientId,
    cookieName: config.cookieName ?? 'auth',
    adminRoles: config.adminRoles ?? ['admin'],
    editorRoles: config.editorRoles ?? ['editor'],
    allowAllAuthenticated: config.allowAllAuthenticated ?? false,
    canAccessAdmin: config.canAccessAdmin,
    syncUserOnLogin: config.syncUserOnLogin ?? true,
    clockTolerance: config.clockTolerance ?? 60,
  }

  return {
    slug: 'users',
    admin: {
      useAsTitle: 'email',
      description: 'Users authenticated via oauth.do',
    },
    auth: {
      // Disable password-based auth
      disableLocalStrategy: true,
      // Custom strategy using oauth.do
      strategies: [
        {
          name: 'oauth.do',
          authenticate: async ({ payload: _payload, headers }: { payload: unknown; headers: Headers }) => {
            const request = new Request('http://localhost', { headers })
            const token = extractToken(request, resolvedConfig.cookieName)

            if (!token) {
              return { user: null }
            }

            const result = await verifyOAuthToken(token, resolvedConfig)

            if (!result.valid || !result.payload) {
              return { user: null }
            }

            const hasAccess = await canAccessPayloadAdmin(result.payload, resolvedConfig)
            if (!hasAccess) {
              return { user: null }
            }

            // Return user for Payload
            return {
              user: {
                id: result.payload.sub,
                email: result.payload.email || `${result.payload.sub}@oauth.do`,
                collection: 'users',
                _strategy: 'oauth.do',
              },
            }
          },
        },
      ],
    },
    access: {
      // Only admins can manage users
      read: ({ req }: { req: { user?: OAuthUser | null | undefined } }) => {
        const user = req.user
        if (!user) return false
        const role = getPayloadRole(
          { sub: user.externalId, roles: user.roles } as OAuthJWTPayload,
          resolvedConfig
        )
        return role === 'admin'
      },
      create: () => false, // Users are created via oauth.do
      update: ({ req }: { req: { user?: OAuthUser | null | undefined } }) => {
        const user = req.user
        if (!user) return false
        const role = getPayloadRole(
          { sub: user.externalId, roles: user.roles } as OAuthJWTPayload,
          resolvedConfig
        )
        return role === 'admin'
      },
      delete: () => false, // Users are managed via oauth.do
    },
    fields: [
      {
        name: 'email',
        type: 'email',
        required: true,
        unique: true,
        admin: { readOnly: true },
      },
      {
        name: 'name',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'firstName',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'lastName',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'profilePictureUrl',
        type: 'text',
        admin: { readOnly: true },
      },
      {
        name: 'externalId',
        type: 'text',
        required: true,
        unique: true,
        admin: {
          readOnly: true,
          description: 'User ID from oauth.do',
        },
      },
      {
        name: 'organizationId',
        type: 'text',
        admin: {
          readOnly: true,
          description: 'Organization ID from oauth.do',
        },
      },
      {
        name: 'roles',
        type: 'array',
        admin: { readOnly: true },
        fields: [
          { name: 'role', type: 'text' },
        ],
      },
      {
        name: 'permissions',
        type: 'array',
        admin: { readOnly: true },
        fields: [
          { name: 'permission', type: 'text' },
        ],
      },
      {
        name: 'lastLoginAt',
        type: 'date',
        admin: { readOnly: true },
      },
    ],
  }
}

// =============================================================================
// Actor Resolver
// =============================================================================

/**
 * Create an actor resolver that extracts user from oauth.do token
 *
 * @example
 * ```typescript
 * import { createOAuthActorResolver } from 'parquedb/payload'
 *
 * const getActor = createOAuthActorResolver({
 *   jwksUri: env.WORKOS_JWKS_URI,
 * })
 *
 * // In request handler
 * const actor = await getActor(request)
 * await db.Posts.create(data, { actor })
 * ```
 */
export function createOAuthActorResolver(config: Pick<OAuthConfig, 'jwksUri' | 'clientId' | 'cookieName'>) {
  const resolvedConfig: ResolvedOAuthConfig = {
    jwksUri: config.jwksUri,
    clientId: config.clientId,
    cookieName: config.cookieName ?? 'auth',
    adminRoles: [],
    editorRoles: [],
    allowAllAuthenticated: true,
    syncUserOnLogin: false,
    clockTolerance: 60,
  }

  return async (request: Request): Promise<EntityId | null> => {
    const token = extractToken(request, resolvedConfig.cookieName)

    if (!token) {
      return null
    }

    const result = await verifyOAuthToken(token, resolvedConfig)

    if (!result.valid || !result.payload) {
      return null
    }

    return `users/${result.payload.sub}` as EntityId
  }
}

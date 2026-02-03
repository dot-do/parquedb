/**
 * Hono Authentication Middleware for ParqueDB
 *
 * Integrates oauth.do authentication with Hono-based Workers.
 * Provides JWT verification and user context for ParqueDB operations.
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { parqueAuth, requireAuth } from 'parquedb/hono'
 *
 * const app = new Hono()
 *
 * // Add auth middleware (populates c.var.user and c.var.actor)
 * app.use('*', parqueAuth())
 *
 * // Protected route
 * app.post('/api/posts', requireAuth(), async (c) => {
 *   const actor = c.var.actor
 *   await db.Posts.create(data, { actor })
 * })
 * ```
 */

import type { Context, MiddlewareHandler, Next } from 'hono'
import type { EntityId } from '../../types/entity'

/**
 * Auth user from oauth.do
 */
export interface AuthUser {
  id: string
  email?: string
  firstName?: string
  lastName?: string
  profilePictureUrl?: string
  organizationId?: string
  roles?: string[]
  permissions?: string[]
}

/**
 * Variables added to Hono context by auth middleware
 */
export interface AuthVariables {
  user: AuthUser | null
  actor: EntityId | null
  token: string | null
}

/**
 * Options for parqueAuth middleware
 */
export interface ParqueAuthOptions {
  /**
   * JWKS URI for token verification (from WorkOS)
   * Required for JWT verification
   * @example 'https://api.workos.com/sso/jwks/client_xxx'
   */
  jwksUri: string
  /** Custom token extractor (defaults to Bearer token from Authorization header) */
  extractToken?: (c: Context) => string | null
  /** Namespace prefix for actor IDs (default: 'users') */
  actorNamespace?: string
}

/**
 * Extract Bearer token from Authorization header
 */
function defaultExtractToken(c: Context): string | null {
  const auth = c.req.header('Authorization')
  if (!auth?.startsWith('Bearer ')) {
    return null
  }
  return auth.slice(7)
}

/**
 * ParqueDB authentication middleware for Hono
 *
 * Extracts and verifies JWT tokens, populates c.var.user and c.var.actor.
 * Works with oauth.do tokens verified against WorkOS JWKS.
 *
 * @example
 * ```typescript
 * app.use('*', parqueAuth({
 *   jwksUri: 'https://api.workos.com/sso/jwks/client_xxx'
 * }))
 * ```
 */
export function parqueAuth(options: ParqueAuthOptions): MiddlewareHandler {
  const {
    extractToken = defaultExtractToken,
    actorNamespace = 'users',
  } = options

  return async (c: Context, next: Next) => {
    const token = extractToken(c)

    // Set default values
    c.set('token' as never, token)
    c.set('user' as never, null)
    c.set('actor' as never, null)

    if (!token) {
      return next()
    }

    try {
      // Try to verify with oauth.do
      const { auth } = await import('oauth.do/hono')

      // Create oauth.do auth middleware
      const authMiddleware = auth({
        jwksUri: options.jwksUri,
      })

      // Run oauth.do middleware
      await new Promise<void>((resolve, reject) => {
        authMiddleware(c, async () => {
          resolve()
        }).catch(reject)
      })

      // Get user from oauth.do middleware result
      const user = (c as any).var?.user as AuthUser | undefined

      if (user) {
        c.set('user' as never, user)
        c.set('actor' as never, `${actorNamespace}/${user.id}` as EntityId)
      }
    } catch (error) {
      // Token verification failed, continue without user
      if (process.env.DEBUG || process.env.PARQUEDB_DEBUG) {
        console.warn('[ParqueDB] Auth verification failed:', error)
      }
    }

    return next()
  }
}

/**
 * Require authentication middleware
 *
 * Returns 401 if user is not authenticated.
 * Optionally checks for specific roles.
 *
 * @example
 * ```typescript
 * // Require any authenticated user
 * app.use('/api/*', requireAuth())
 *
 * // Require admin role
 * app.use('/admin/*', requireAuth({ roles: ['admin'] }))
 * ```
 */
export function requireAuth(options: {
  roles?: string[]
  message?: string
} = {}): MiddlewareHandler {
  return async (c: Context, next: Next) => {
    const user = (c as any).var?.user as AuthUser | null

    if (!user) {
      return c.json(
        { error: options.message ?? 'Authentication required' },
        401
      )
    }

    // Check roles if specified
    if (options.roles && options.roles.length > 0) {
      const userRoles = user.roles ?? []
      const hasRole = options.roles.some(role => userRoles.includes(role))

      if (!hasRole) {
        return c.json(
          { error: `Required role: ${options.roles.join(' or ')}` },
          403
        )
      }
    }

    return next()
  }
}

/**
 * Get actor from Hono context
 *
 * Helper to extract actor for ParqueDB operations.
 *
 * @example
 * ```typescript
 * app.post('/api/posts', async (c) => {
 *   const actor = getActor(c)
 *   await db.Posts.create(data, { actor })
 * })
 * ```
 */
export function getActor(c: Context): EntityId | null {
  return (c.var as any).actor ?? null
}

/**
 * Get user from Hono context
 */
export function getUser(c: Context): AuthUser | null {
  return (c.var as any).user ?? null
}

/**
 * Assert user is authenticated (throws if not)
 */
export function assertAuth(c: Context): AuthUser {
  const user = getUser(c)
  if (!user) {
    throw new Error('Authentication required')
  }
  return user
}

/**
 * Assert user has specific role (throws if not)
 */
export function assertRole(c: Context, role: string): AuthUser {
  const user = assertAuth(c)
  if (!user.roles?.includes(role)) {
    throw new Error(`Required role: ${role}`)
  }
  return user
}

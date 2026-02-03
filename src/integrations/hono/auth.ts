/**
 * Hono Authentication Middleware for ParqueDB
 *
 * Integrates oauth.do authentication with Hono-based Workers.
 * Provides JWT verification and user context for ParqueDB operations.
 *
 * The actor from auth middleware flows to createdBy/updatedBy audit fields:
 * - auth() sets c.var.actor (EntityId like "users/abc123")
 * - Pass { actor: c.var.actor } to mutations
 * - ParqueDB automatically populates createdBy, updatedBy, deletedBy
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { auth, requireAuth, getUser } from 'parquedb/hono'
 * import { db } from 'parquedb'
 *
 * const app = new Hono()
 *
 * // Add auth middleware (populates c.var.user and c.var.actor)
 * app.use('*', auth({ jwksUri: env.JWKS_URI }))
 *
 * // Protected route - actor flows to createdBy/updatedBy
 * app.post('/api/posts', requireAuth(), async (c) => {
 *   const post = await db.Posts.create(
 *     { $type: 'Post', name: 'My Post', title: '...' },
 *     { actor: c.var.actor }  // → createdBy: "users/abc123"
 *   )
 *   return c.json(post)
 * })
 *
 * // Get current user info
 * app.get('/api/me', async (c) => {
 *   const user = getUser(c)
 *   if (!user) return c.json({ error: 'Not authenticated' }, 401)
 *   return c.json(user)
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
 * Options for auth middleware
 */
export interface AuthOptions {
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
 * import { auth, requireAuth } from 'parquedb/hono'
 *
 * app.use('*', auth({
 *   jwksUri: 'https://api.workos.com/sso/jwks/client_xxx'
 * }))
 *
 * app.post('/api/posts', requireAuth(), async (c) => {
 *   await db.Posts.create(data, { actor: c.var.actor })
 * })
 * ```
 */
export function auth(options: AuthOptions): MiddlewareHandler {
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
 * Get authenticated user from Hono context
 *
 * @example
 * ```typescript
 * app.get('/api/me', async (c) => {
 *   const user = getUser(c)
 *   if (!user) return c.json({ error: 'Not authenticated' }, 401)
 *   return c.json({ id: user.id, email: user.email })
 * })
 * ```
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

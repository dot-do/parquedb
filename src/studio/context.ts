/**
 * Database Context Management for ParqueDB Studio
 *
 * Provides cookie-based database context management for multi-database mode.
 * Enables persistent database selection across browser sessions while supporting
 * path-based routing for multiple tabs with different databases.
 *
 * ## Usage
 *
 * ```typescript
 * import { Hono } from 'hono'
 * import {
 *   databaseContextMiddleware,
 *   requireDatabaseContext,
 *   getDatabaseContext,
 *   setDatabaseContext,
 *   clearDatabaseContext,
 * } from 'parquedb/studio'
 *
 * const app = new Hono()
 *
 * // Apply context middleware (reads cookie, sets c.var.database)
 * app.use('/admin/*', databaseContextMiddleware({
 *   getStorage: (db) => new R2Backend(env.BUCKET, db.prefix),
 *   getDatabaseIndex: (userId) => getUserDatabaseIndex(env, userId),
 * }))
 *
 * // Require database context (redirects to selector if none)
 * app.use('/admin/:databaseId/*', requireDatabaseContext())
 *
 * // Get context in handler
 * app.get('/admin/:databaseId/dashboard', async (c) => {
 *   const ctx = getDatabaseContext(c)
 *   if (!ctx) return c.redirect('/admin')
 *   return c.json({ database: ctx.database.name })
 * })
 *
 * // Set database context (stores in cookie)
 * app.post('/admin/select/:databaseId', async (c) => {
 *   return setDatabaseContext(c, c.req.param('databaseId'), '/admin')
 * })
 *
 * // Clear context (logout from database)
 * app.post('/admin/logout', async (c) => {
 *   return clearDatabaseContext(c, '/admin')
 * })
 * ```
 *
 * @module
 */

import type { Context, MiddlewareHandler, Next } from 'hono'
import type { StorageBackend } from '../types/storage'
import type { EntityId } from '../types'
import type { DatabaseInfo } from '../worker/DatabaseIndexDO'
import { parseRoute, resolveDatabase, generateDatabaseNotFoundHtml } from './database'

// =============================================================================
// Constants
// =============================================================================

/**
 * Cookie name for storing active database ID
 */
export const PAYLOAD_DATABASE_COOKIE = 'PAYLOAD_DATABASE'

/**
 * Default cookie max age (30 days in seconds)
 */
export const DEFAULT_COOKIE_MAX_AGE = 30 * 24 * 60 * 60

// =============================================================================
// Types
// =============================================================================

/**
 * Database context stored in request variables
 */
export interface DatabaseContextData {
  /** Database ID */
  databaseId: string
  /** Database metadata from index */
  database: DatabaseInfo
  /** Storage backend configured for this database */
  storage: StorageBackend
  /** Base path for this database's admin routes */
  basePath: string
}

/**
 * Variables added to Hono context by database context middleware
 */
export interface DatabaseContextVariables {
  /** Active database context (null if not selected) */
  databaseContext: DatabaseContextData | null
  /** Database ID from cookie (for fallback) */
  cookieDatabaseId: string | null
}

/**
 * Configuration for database context middleware
 */
export interface DatabaseContextConfig {
  /**
   * Get storage backend for a database
   */
  getStorage: (database: DatabaseInfo) => StorageBackend | Promise<StorageBackend>

  /**
   * Get database index for a user
   */
  getDatabaseIndex: (userId: string) => Promise<{
    get(id: string): Promise<DatabaseInfo | null>
    getBySlug(owner: string, slug: string): Promise<DatabaseInfo | null>
    recordAccess(id: string): Promise<void>
  }>

  /**
   * Path prefix for admin routes
   * @default '/admin'
   */
  pathPrefix?: string

  /**
   * Cookie name for storing active database
   * @default 'PAYLOAD_DATABASE'
   */
  cookieName?: string

  /**
   * Cookie max age in seconds
   * @default 2592000 (30 days)
   */
  cookieMaxAge?: number

  /**
   * Cookie domain (if not set, uses current domain)
   */
  cookieDomain?: string

  /**
   * Cookie path
   * @default '/'
   */
  cookiePath?: string

  /**
   * Use secure cookies (HTTPS only)
   * @default true in production
   */
  cookieSecure?: boolean

  /**
   * SameSite cookie attribute
   * @default 'lax'
   */
  cookieSameSite?: 'strict' | 'lax' | 'none'
}

/**
 * Cookie options for set/clear operations
 */
export interface CookieOptions {
  /** Cookie max age in seconds */
  maxAge?: number
  /** Cookie domain */
  domain?: string
  /** Cookie path */
  path?: string
  /** Secure flag (HTTPS only) */
  secure?: boolean
  /** SameSite attribute */
  sameSite?: 'strict' | 'lax' | 'none'
  /** HttpOnly flag (not accessible to JavaScript) */
  httpOnly?: boolean
}

// =============================================================================
// Cookie Utilities
// =============================================================================

/**
 * Parse cookies from Cookie header string
 *
 * @param cookieHeader - Cookie header value
 * @returns Object mapping cookie names to values
 *
 * @example
 * ```typescript
 * const cookies = parseCookies('foo=bar; PAYLOAD_DATABASE=db_123')
 * // { foo: 'bar', PAYLOAD_DATABASE: 'db_123' }
 * ```
 */
export function parseCookies(cookieHeader: string): Record<string, string> {
  const cookies: Record<string, string> = {}

  if (!cookieHeader) {
    return cookies
  }

  const pairs = cookieHeader.split(';')

  for (const pair of pairs) {
    const trimmed = pair.trim()
    const equalsIndex = trimmed.indexOf('=')

    if (equalsIndex > 0) {
      const key = trimmed.slice(0, equalsIndex).trim()
      const value = trimmed.slice(equalsIndex + 1).trim()

      // Decode URI-encoded values and remove quotes if present
      if (key) {
        try {
          cookies[key] = decodeURIComponent(
            value.startsWith('"') && value.endsWith('"')
              ? value.slice(1, -1)
              : value
          )
        } catch {
          // If decoding fails, use raw value
          cookies[key] = value
        }
      }
    }
  }

  return cookies
}

/**
 * Build Set-Cookie header value
 *
 * @param name - Cookie name
 * @param value - Cookie value
 * @param options - Cookie options
 * @returns Set-Cookie header value
 *
 * @example
 * ```typescript
 * const cookie = buildSetCookie('PAYLOAD_DATABASE', 'db_123', {
 *   maxAge: 86400,
 *   path: '/',
 *   httpOnly: true,
 *   secure: true,
 *   sameSite: 'lax',
 * })
 * // 'PAYLOAD_DATABASE=db_123; Max-Age=86400; Path=/; HttpOnly; Secure; SameSite=Lax'
 * ```
 */
export function buildSetCookie(
  name: string,
  value: string,
  options: CookieOptions = {}
): string {
  const parts: string[] = [`${name}=${encodeURIComponent(value)}`]

  if (options.maxAge !== undefined) {
    parts.push(`Max-Age=${options.maxAge}`)
  }

  if (options.domain) {
    parts.push(`Domain=${options.domain}`)
  }

  if (options.path) {
    parts.push(`Path=${options.path}`)
  }

  if (options.httpOnly) {
    parts.push('HttpOnly')
  }

  if (options.secure) {
    parts.push('Secure')
  }

  if (options.sameSite) {
    parts.push(`SameSite=${capitalize(options.sameSite)}`)
  }

  return parts.join('; ')
}

/**
 * Build Set-Cookie header to clear/delete a cookie
 *
 * @param name - Cookie name to clear
 * @param options - Cookie options (path and domain should match original)
 * @returns Set-Cookie header value that expires the cookie
 *
 * @example
 * ```typescript
 * const cookie = buildClearCookie('PAYLOAD_DATABASE', { path: '/' })
 * // 'PAYLOAD_DATABASE=; Max-Age=0; Path=/'
 * ```
 */
export function buildClearCookie(
  name: string,
  options: Pick<CookieOptions, 'domain' | 'path'> = {}
): string {
  const parts: string[] = [`${name}=`]

  parts.push('Max-Age=0')

  if (options.domain) {
    parts.push(`Domain=${options.domain}`)
  }

  if (options.path) {
    parts.push(`Path=${options.path}`)
  }

  return parts.join('; ')
}

/**
 * Helper to capitalize first letter
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase()
}

// =============================================================================
// Context Functions
// =============================================================================

/**
 * Get database context from Hono context
 *
 * Returns the active database context if one has been set by middleware,
 * or null if no database is selected.
 *
 * @param c - Hono context
 * @returns Database context or null
 *
 * @example
 * ```typescript
 * app.get('/admin/:databaseId/dashboard', async (c) => {
 *   const ctx = getDatabaseContext(c)
 *   if (!ctx) {
 *     return c.redirect('/admin')
 *   }
 *   return c.json({
 *     databaseId: ctx.databaseId,
 *     name: ctx.database.name,
 *     basePath: ctx.basePath,
 *   })
 * })
 * ```
 */
export function getDatabaseContext(c: Context): DatabaseContextData | null {
  return (c.var as DatabaseContextVariables).databaseContext ?? null
}

/**
 * Get database ID from cookie
 *
 * Reads the PAYLOAD_DATABASE cookie to get the last selected database.
 * Useful for redirecting to the last used database.
 *
 * @param c - Hono context
 * @param cookieName - Cookie name (default: PAYLOAD_DATABASE)
 * @returns Database ID from cookie or null
 *
 * @example
 * ```typescript
 * app.get('/admin', async (c) => {
 *   const lastDb = getCookieDatabaseId(c)
 *   if (lastDb) {
 *     return c.redirect(`/admin/${lastDb}`)
 *   }
 *   // Show database selector
 * })
 * ```
 */
export function getCookieDatabaseId(
  c: Context,
  cookieName: string = PAYLOAD_DATABASE_COOKIE
): string | null {
  // Check if already parsed by middleware
  const fromVar = (c.var as DatabaseContextVariables).cookieDatabaseId
  if (fromVar !== undefined) {
    return fromVar
  }

  // Parse from header
  const cookieHeader = c.req.header('Cookie')
  if (!cookieHeader) {
    return null
  }

  const cookies = parseCookies(cookieHeader)
  return cookies[cookieName] || null
}

/**
 * Set database context cookie
 *
 * Creates a response that sets the PAYLOAD_DATABASE cookie to remember
 * the selected database. Optionally redirects to a URL.
 *
 * @param c - Hono context
 * @param databaseId - Database ID to store
 * @param redirectTo - Optional URL to redirect to
 * @param config - Cookie configuration options
 * @returns Response with Set-Cookie header
 *
 * @example
 * ```typescript
 * // Set cookie and redirect to dashboard
 * app.post('/admin/select/:databaseId', async (c) => {
 *   const databaseId = c.req.param('databaseId')
 *   return setDatabaseContext(c, databaseId, `/admin/${databaseId}`)
 * })
 *
 * // Set cookie without redirect
 * app.post('/api/select-database', async (c) => {
 *   const { databaseId } = await c.req.json()
 *   return setDatabaseContext(c, databaseId)
 * })
 * ```
 */
export function setDatabaseContext(
  c: Context,
  databaseId: string,
  redirectTo?: string,
  config: Partial<DatabaseContextConfig> = {}
): Response {
  const cookieName = config.cookieName ?? PAYLOAD_DATABASE_COOKIE
  const maxAge = config.cookieMaxAge ?? DEFAULT_COOKIE_MAX_AGE
  const path = config.cookiePath ?? '/'
  const sameSite = config.cookieSameSite ?? 'lax'
  const secure = config.cookieSecure ?? isSecureContext(c)

  const cookie = buildSetCookie(cookieName, databaseId, {
    maxAge,
    path,
    domain: config.cookieDomain,
    httpOnly: true,
    secure,
    sameSite,
  })

  const headers = new Headers()
  headers.set('Set-Cookie', cookie)

  if (redirectTo) {
    headers.set('Location', redirectTo)
    return new Response(null, {
      status: 302,
      headers,
    })
  }

  headers.set('Content-Type', 'application/json')

  return new Response(JSON.stringify({ success: true, databaseId }), {
    status: 200,
    headers,
  })
}

/**
 * Clear database context cookie
 *
 * Creates a response that clears the PAYLOAD_DATABASE cookie.
 * Typically used when logging out or switching accounts.
 *
 * @param c - Hono context
 * @param redirectTo - Optional URL to redirect to (default: /admin)
 * @param config - Cookie configuration options
 * @returns Response with Set-Cookie header that clears the cookie
 *
 * @example
 * ```typescript
 * // Clear cookie and redirect to selector
 * app.post('/admin/logout', async (c) => {
 *   return clearDatabaseContext(c, '/admin')
 * })
 *
 * // Clear cookie without redirect
 * app.post('/api/clear-database', async (c) => {
 *   return clearDatabaseContext(c)
 * })
 * ```
 */
export function clearDatabaseContext(
  c: Context,
  redirectTo?: string,
  config: Partial<DatabaseContextConfig> = {}
): Response {
  const cookieName = config.cookieName ?? PAYLOAD_DATABASE_COOKIE
  const path = config.cookiePath ?? '/'

  const cookie = buildClearCookie(cookieName, {
    path,
    domain: config.cookieDomain,
  })

  const headers = new Headers()
  headers.set('Set-Cookie', cookie)

  if (redirectTo) {
    headers.set('Location', redirectTo)
    return new Response(null, {
      status: 302,
      headers,
    })
  }

  headers.set('Content-Type', 'application/json')

  return new Response(JSON.stringify({ success: true }), {
    status: 200,
    headers,
  })
}

/**
 * Check if running in secure context (HTTPS)
 */
function isSecureContext(c: Context): boolean {
  const proto = c.req.header('X-Forwarded-Proto')
  if (proto) {
    return proto === 'https'
  }

  const url = new URL(c.req.url)
  return url.protocol === 'https:'
}

// =============================================================================
// Middleware
// =============================================================================

/**
 * Database context middleware for Hono
 *
 * Extracts database ID from path parameter or cookie and resolves
 * the full database context (info, storage backend, etc.).
 *
 * Sets the following context variables:
 * - `c.var.databaseContext`: Full database context or null
 * - `c.var.cookieDatabaseId`: Database ID from cookie (for fallback)
 *
 * @param config - Middleware configuration
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { databaseContextMiddleware } from 'parquedb/studio'
 *
 * const app = new Hono()
 *
 * app.use('/admin/*', databaseContextMiddleware({
 *   getStorage: (db) => new R2Backend(env.BUCKET, db.prefix),
 *   getDatabaseIndex: (userId) => getUserDatabaseIndex(env, userId),
 *   pathPrefix: '/admin',
 *   cookieMaxAge: 7 * 24 * 60 * 60, // 7 days
 * }))
 *
 * app.get('/admin/:databaseId/dashboard', async (c) => {
 *   const ctx = c.var.databaseContext
 *   return c.json({ database: ctx?.database.name })
 * })
 * ```
 */
export function databaseContextMiddleware(
  config: DatabaseContextConfig
): MiddlewareHandler {
  const pathPrefix = config.pathPrefix ?? '/admin'
  const cookieName = config.cookieName ?? PAYLOAD_DATABASE_COOKIE

  return async (c: Context, next: Next) => {
    // Parse cookie for fallback database ID
    const cookieHeader = c.req.header('Cookie')
    const cookies = cookieHeader ? parseCookies(cookieHeader) : {}
    const cookieDatabaseId = cookies[cookieName] || null

    // Set cookie database ID in context
    ;(c as Context & { var: DatabaseContextVariables }).set(
      'cookieDatabaseId' as never,
      cookieDatabaseId
    )

    // Try to get database ID from path parameter
    const databaseId = c.req.param('databaseId')

    if (!databaseId) {
      // No database in path - context is null
      ;(c as Context & { var: DatabaseContextVariables }).set(
        'databaseContext' as never,
        null
      )
      return next()
    }

    // Get user from context (set by auth middleware)
    const user = (c as Context & { var: { user?: { id: string } | null } }).var
      ?.user

    if (!user) {
      // Not authenticated - context is null
      ;(c as Context & { var: DatabaseContextVariables }).set(
        'databaseContext' as never,
        null
      )
      return next()
    }

    try {
      // Resolve database
      const database = await resolveDatabase(
        databaseId,
        `users/${user.id}` as EntityId,
        config.getDatabaseIndex
      )

      if (!database) {
        // Database not found - context is null
        ;(c as Context & { var: DatabaseContextVariables }).set(
          'databaseContext' as never,
          null
        )
        return next()
      }

      // Get storage backend
      const storage = await config.getStorage(database)

      // Build context
      const context: DatabaseContextData = {
        databaseId: database.id,
        database,
        storage,
        basePath: `${pathPrefix}/${databaseId}`,
      }

      ;(c as Context & { var: DatabaseContextVariables }).set(
        'databaseContext' as never,
        context
      )

      return next()
    } catch (error) {
      // Error resolving database - context is null
      console.error('[databaseContextMiddleware] Error:', error)
      ;(c as Context & { var: DatabaseContextVariables }).set(
        'databaseContext' as never,
        null
      )
      return next()
    }
  }
}

/**
 * Require database context middleware
 *
 * Middleware that ensures a valid database context exists.
 * If no database is selected, redirects to the database selector.
 *
 * @param options - Middleware options
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { requireDatabaseContext, databaseContextMiddleware } from 'parquedb/studio'
 *
 * const app = new Hono()
 *
 * // First, apply context middleware
 * app.use('/admin/*', databaseContextMiddleware({ ... }))
 *
 * // Then, require context for database-specific routes
 * app.use('/admin/:databaseId/*', requireDatabaseContext())
 *
 * // Or with custom redirect
 * app.use('/admin/:databaseId/*', requireDatabaseContext({
 *   redirectTo: '/admin/select',
 * }))
 *
 * // Or show error instead of redirect
 * app.use('/api/databases/:databaseId/*', requireDatabaseContext({
 *   onMissing: (c, databaseId) => {
 *     return c.json({ error: `Database ${databaseId} not found` }, 404)
 *   },
 * }))
 * ```
 */
export function requireDatabaseContext(
  options: {
    /**
     * URL to redirect to when no database context
     * @default '/admin'
     */
    redirectTo?: string

    /**
     * Path prefix for admin routes
     * @default '/admin'
     */
    pathPrefix?: string

    /**
     * Custom handler when database context is missing
     * If provided, overrides redirectTo behavior
     */
    onMissing?: (c: Context, databaseId: string | null) => Response | Promise<Response>

    /**
     * Whether to show HTML error page for invalid database
     * @default true
     */
    showErrorPage?: boolean
  } = {}
): MiddlewareHandler {
  const {
    redirectTo = '/admin',
    pathPrefix = '/admin',
    onMissing,
    showErrorPage = true,
  } = options

  return async (c: Context, next: Next) => {
    const context = getDatabaseContext(c)
    const databaseId = c.req.param('databaseId')

    if (context) {
      // Valid context exists - continue
      return next()
    }

    // No valid context
    if (onMissing) {
      return onMissing(c, databaseId || null)
    }

    // Check if this was a specific database request that failed
    if (databaseId && showErrorPage) {
      // Show not found page for invalid database
      return c.html(generateDatabaseNotFoundHtml(databaseId, pathPrefix), 404)
    }

    // Redirect to database selector
    return c.redirect(redirectTo)
  }
}

/**
 * Auto-select database middleware
 *
 * Middleware that automatically redirects to the last used database
 * (from cookie) when accessing the root admin path without a database ID.
 *
 * @param options - Middleware options
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { autoSelectDatabase, databaseContextMiddleware } from 'parquedb/studio'
 *
 * const app = new Hono()
 *
 * // Apply context middleware first
 * app.use('/admin/*', databaseContextMiddleware({ ... }))
 *
 * // Auto-redirect to last database on /admin
 * app.use('/admin', autoSelectDatabase())
 *
 * // Now /admin will redirect to /admin/{lastDatabaseId} if cookie exists
 * ```
 */
export function autoSelectDatabase(
  options: {
    /**
     * Cookie name for stored database ID
     * @default 'PAYLOAD_DATABASE'
     */
    cookieName?: string

    /**
     * Path prefix for admin routes
     * @default '/admin'
     */
    pathPrefix?: string

    /**
     * Default path when no database in cookie
     * If not set, continues to next handler (shows selector)
     */
    defaultPath?: string
  } = {}
): MiddlewareHandler {
  const {
    cookieName = PAYLOAD_DATABASE_COOKIE,
    pathPrefix = '/admin',
  } = options

  return async (c: Context, next: Next) => {
    // Only apply to exact path (no database ID)
    const parsed = parseRoute(c.req.path, pathPrefix)
    if (parsed) {
      // Already has a database ID - continue
      return next()
    }

    // Check for database in cookie
    const cookieDatabaseId = getCookieDatabaseId(c, cookieName)

    if (cookieDatabaseId) {
      // Redirect to last used database
      const redirectUrl = `${pathPrefix}/${cookieDatabaseId}`
      return c.redirect(redirectUrl)
    }

    // No cookie - continue to selector
    if (options.defaultPath) {
      return c.redirect(options.defaultPath)
    }

    return next()
  }
}

// =============================================================================
// Helper Types for Hono Integration
// =============================================================================

/**
 * Type helper for Hono app with database context
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import type { HonoWithDatabaseContext } from 'parquedb/studio'
 *
 * const app = new Hono<HonoWithDatabaseContext>()
 *
 * app.get('/admin/:databaseId/info', (c) => {
 *   // c.var.databaseContext is typed
 *   const ctx = c.var.databaseContext
 *   return c.json({ name: ctx?.database.name })
 * })
 * ```
 */
export type HonoWithDatabaseContext = {
  Variables: DatabaseContextVariables
}

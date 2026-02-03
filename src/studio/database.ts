/**
 * Database Routing for Studio
 *
 * Enables path-based multi-database support: /admin/:databaseId/...
 * Users can have multiple databases open in different browser tabs.
 *
 * @example
 * ```typescript
 * // Route: /admin/db_abc123/collections/posts
 * const dbContext = await getDatabaseContext(request, env, userId)
 * // dbContext.database = { id: 'db_abc123', bucket: '...', prefix: '...' }
 * // dbContext.storage = R2Backend configured for this database
 * ```
 */

import type { StorageBackend } from '../types/storage'
import type { EntityId } from '../types'
import type { DatabaseInfo } from '../worker/DatabaseIndexDO'

// =============================================================================
// Types
// =============================================================================

/**
 * Database context for a request
 */
export interface DatabaseContext {
  /** Database metadata from index */
  database: DatabaseInfo
  /** Storage backend configured for this database */
  storage: StorageBackend
  /** User ID (owner) */
  userId: EntityId
  /** Base path for this database's admin routes */
  basePath: string
}

/**
 * Parsed route info
 */
export interface ParsedRoute {
  /** Database ID from path */
  databaseId: string
  /** Remaining path after database ID */
  remainingPath: string
  /** Original full path */
  fullPath: string
}

/**
 * Configuration for database routing
 */
export interface DatabaseRoutingConfig {
  /** Path prefix (default: '/admin') */
  pathPrefix?: string
  /** Cookie name for storing last accessed database */
  lastDatabaseCookie?: string
  /** Get storage backend for a database */
  getStorage: (database: DatabaseInfo) => StorageBackend | Promise<StorageBackend>
}

// =============================================================================
// Route Parsing
// =============================================================================

/**
 * Parse database ID from request path
 *
 * Matches: /admin/:databaseId/...
 *
 * @example
 * ```typescript
 * parseRoute('/admin/db_abc123/collections/posts')
 * // { databaseId: 'db_abc123', remainingPath: '/collections/posts' }
 *
 * parseRoute('/admin/select')
 * // null (no database ID)
 * ```
 */
export function parseRoute(
  path: string,
  pathPrefix: string = '/admin'
): ParsedRoute | null {
  // Normalize path prefix
  const prefix = pathPrefix.endsWith('/') ? pathPrefix.slice(0, -1) : pathPrefix

  // Check if path starts with prefix
  if (!path.startsWith(prefix)) {
    return null
  }

  // Remove prefix and get remaining path
  const afterPrefix = path.slice(prefix.length)

  // Must start with /
  if (!afterPrefix.startsWith('/')) {
    return null
  }

  // Split remaining path
  const parts = afterPrefix.slice(1).split('/')

  // First part should be database ID (starts with db_ or is a valid ID)
  const databaseId = parts[0]

  if (!databaseId || !isValidDatabaseId(databaseId)) {
    return null
  }

  // Build remaining path
  const remainingPath = '/' + parts.slice(1).join('/')

  return {
    databaseId,
    remainingPath,
    fullPath: path,
  }
}

/**
 * Check if a string is a valid database ID
 */
export function isValidDatabaseId(id: string): boolean {
  // Database IDs start with 'db_' or match owner/slug pattern
  if (id.startsWith('db_')) {
    return true
  }

  // Could be owner/slug format - check for valid slug characters
  if (/^[a-z0-9][a-z0-9-]*[a-z0-9]$/.test(id) || /^[a-z0-9]{1,3}$/.test(id)) {
    return true
  }

  return false
}

/**
 * Build admin URL for a database
 */
export function buildDatabaseUrl(
  databaseId: string,
  path: string = '',
  pathPrefix: string = '/admin'
): string {
  const prefix = pathPrefix.endsWith('/') ? pathPrefix.slice(0, -1) : pathPrefix
  const suffix = path.startsWith('/') ? path : `/${path}`
  return `${prefix}/${databaseId}${suffix}`
}

/**
 * Build admin URL using owner/slug instead of database ID
 */
export function buildPublicDatabaseUrl(
  owner: string,
  slug: string,
  path: string = '',
  pathPrefix: string = '/admin'
): string {
  const prefix = pathPrefix.endsWith('/') ? pathPrefix.slice(0, -1) : pathPrefix
  const suffix = path.startsWith('/') ? path : `/${path}`
  return `${prefix}/${owner}/${slug}${suffix}`
}

// =============================================================================
// Database Resolution
// =============================================================================

/**
 * Resolve database from request path
 *
 * Uses DatabaseIndex DO to look up database metadata.
 */
export async function resolveDatabase(
  databaseId: string,
  userId: EntityId,
  getDatabaseIndex: (userId: string) => Promise<{
    get(id: string): Promise<DatabaseInfo | null>
    getBySlug(owner: string, slug: string): Promise<DatabaseInfo | null>
    recordAccess(id: string): Promise<void>
  }>
): Promise<DatabaseInfo | null> {
  const userIdStr = userId.split('/')[1] || userId

  // Get database index for this user
  const index = await getDatabaseIndex(userIdStr)

  // Try direct ID lookup first
  let database = await index.get(databaseId)

  // If not found, could be owner/slug from path
  if (!database && databaseId.includes('/')) {
    const [owner, slug] = databaseId.split('/')
    if (owner && slug) {
      database = await index.getBySlug(owner, slug)
    }
  }

  // Record access for analytics
  if (database) {
    await index.recordAccess(database.id).catch(() => {
      // Ignore access recording failures
    })
  }

  return database
}

// =============================================================================
// Database Selection UI
// =============================================================================

/**
 * Generate database selection HTML page
 */
export function generateDatabaseSelectHtml(
  databases: DatabaseInfo[],
  pathPrefix: string = '/admin'
): string {
  const databaseList = databases.length > 0
    ? databases.map(db => `
        <a href="${pathPrefix}/${db.id}" class="database-card">
          <div class="database-name">${escapeHtml(db.name)}</div>
          ${db.description ? `<div class="database-description">${escapeHtml(db.description)}</div>` : ''}
          <div class="database-meta">
            <span>${db.entityCount ?? 0} entities</span>
            <span>${formatDate(db.lastAccessedAt || db.createdAt)}</span>
          </div>
        </a>
      `).join('\n')
    : '<p class="no-databases">No databases yet. Create one to get started.</p>'

  return `
<!DOCTYPE html>
<html>
<head>
  <title>Select Database - ParqueDB Studio</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: system-ui, -apple-system, sans-serif;
      padding: 2rem;
      max-width: 800px;
      margin: 0 auto;
      background: #f5f5f5;
    }
    h1 { color: #333; margin-bottom: 0.5rem; }
    .subtitle { color: #666; margin-bottom: 2rem; }
    .database-grid {
      display: grid;
      gap: 1rem;
    }
    .database-card {
      display: block;
      background: white;
      padding: 1.5rem;
      border-radius: 8px;
      text-decoration: none;
      color: inherit;
      border: 2px solid transparent;
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    .database-card:hover {
      border-color: #0066cc;
      box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    .database-name {
      font-size: 1.25rem;
      font-weight: 600;
      color: #333;
      margin-bottom: 0.25rem;
    }
    .database-description {
      color: #666;
      margin-bottom: 0.75rem;
    }
    .database-meta {
      display: flex;
      gap: 1rem;
      font-size: 0.875rem;
      color: #888;
    }
    .no-databases {
      color: #666;
      text-align: center;
      padding: 2rem;
    }
    .create-btn {
      display: inline-block;
      background: #0066cc;
      color: white;
      padding: 0.75rem 1.5rem;
      border-radius: 6px;
      text-decoration: none;
      font-weight: 500;
      margin-top: 1rem;
    }
    .create-btn:hover {
      background: #0055aa;
    }
  </style>
</head>
<body>
  <h1>ParqueDB Studio</h1>
  <p class="subtitle">Select a database to manage</p>

  <div class="database-grid">
    ${databaseList}
  </div>

  <div style="text-align: center; margin-top: 2rem;">
    <a href="${pathPrefix}/new" class="create-btn">+ Create New Database</a>
  </div>
</body>
</html>
  `
}

/**
 * Generate database not found HTML
 */
export function generateDatabaseNotFoundHtml(
  databaseId: string,
  pathPrefix: string = '/admin'
): string {
  return `
<!DOCTYPE html>
<html>
<head>
  <title>Database Not Found - ParqueDB Studio</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {
      font-family: system-ui, -apple-system, sans-serif;
      padding: 2rem;
      max-width: 600px;
      margin: 0 auto;
      text-align: center;
    }
    h1 { color: #cc0000; }
    p { color: #666; }
    a { color: #0066cc; }
    .back-btn {
      display: inline-block;
      background: #0066cc;
      color: white;
      padding: 0.75rem 1.5rem;
      border-radius: 6px;
      text-decoration: none;
      margin-top: 1rem;
    }
  </style>
</head>
<body>
  <h1>Database Not Found</h1>
  <p>The database "${escapeHtml(databaseId)}" was not found or you don't have access to it.</p>
  <a href="${pathPrefix}" class="back-btn">Back to Database List</a>
</body>
</html>
  `
}

// =============================================================================
// Helpers
// =============================================================================

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
}

function formatDate(date: Date): string {
  const now = new Date()
  const diff = now.getTime() - date.getTime()

  if (diff < 60000) return 'just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  if (diff < 604800000) return `${Math.floor(diff / 86400000)}d ago`

  return date.toLocaleDateString()
}

// =============================================================================
// Middleware
// =============================================================================

/**
 * Database context middleware for Hono
 *
 * Extracts database ID from path and resolves database info.
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { databaseMiddleware } from 'parquedb/studio'
 *
 * const app = new Hono()
 *
 * app.use('/admin/:databaseId/*', databaseMiddleware({
 *   getStorage: (db) => new R2Backend(env.BUCKET, db.prefix),
 *   getDatabaseIndex: (userId) => getUserDatabaseIndex(env, userId),
 * }))
 * ```
 */
export function databaseMiddleware(config: {
  getStorage: (database: DatabaseInfo) => StorageBackend | Promise<StorageBackend>
  getDatabaseIndex: (userId: string) => Promise<{
    get(id: string): Promise<DatabaseInfo | null>
    getBySlug(owner: string, slug: string): Promise<DatabaseInfo | null>
    recordAccess(id: string): Promise<void>
  }>
  pathPrefix?: string
}) {
  const pathPrefix = config.pathPrefix ?? '/admin'

  return async (
    c: {
      req: { raw: Request; param: (name: string) => string }
      var: Record<string, unknown>
      set: (key: string, value: unknown) => void
      html: (html: string, status?: number) => Response
    },
    next: () => Promise<void>
  ) => {
    const databaseId = c.req.param('databaseId')
    const userId = c.var.user as { id: string } | null

    if (!userId) {
      // Not authenticated - return 401
      return c.html(generateDatabaseNotFoundHtml(databaseId, pathPrefix), 401)
    }

    // Resolve database
    const database = await resolveDatabase(
      databaseId,
      `users/${userId.id}` as EntityId,
      config.getDatabaseIndex
    )

    if (!database) {
      return c.html(generateDatabaseNotFoundHtml(databaseId, pathPrefix), 404)
    }

    // Get storage backend for this database
    const storage = await config.getStorage(database)

    // Set context
    const context: DatabaseContext = {
      database,
      storage,
      userId: `users/${userId.id}` as EntityId,
      basePath: `${pathPrefix}/${databaseId}`,
    }

    c.set('database', context)

    return next()
  }
}

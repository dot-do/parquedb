/**
 * ParqueDB Studio API Routes
 *
 * Hono route handlers for the database dashboard in ParqueDB Studio.
 * These routes interact with the DatabaseIndexDO Durable Object to manage
 * user databases.
 *
 * CSRF Protection:
 * All mutation endpoints (POST, PATCH, DELETE) require CSRF validation.
 * Clients must include the X-Requested-With header with their requests.
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { databaseRoutes } from 'parquedb/studio'
 * import { auth, requireAuth } from 'parquedb/hono'
 * import { csrf } from 'parquedb/security'
 *
 * const app = new Hono()
 *
 * // Add auth middleware
 * app.use('*', auth({ jwksUri: env.JWKS_URI }))
 *
 * // Add CSRF protection for mutation routes
 * app.use('/api/databases/*', csrf())
 *
 * // Mount database routes
 * app.route('/api/databases', databaseRoutes)
 * ```
 *
 * @module
 */

import { Hono } from 'hono'
import type { Context } from 'hono'
import type { EntityId } from '../types/entity'
import type { Visibility } from '../types/visibility'
import type {
  DatabaseInfo,
  DatabaseIndexDO,
  RegisterDatabaseOptions,
  UpdateDatabaseOptions,
} from '../worker/DatabaseIndexDO'
import { getUserDatabaseIndex } from '../worker/DatabaseIndexDO'
import { getUser, type AuthUser, type AuthVariables } from '../integrations/hono/auth'
import { validateCsrf } from '../security/csrf'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Environment bindings required for database routes
 */
export interface DatabaseApiEnv {
  /** DatabaseIndex Durable Object namespace */
  DATABASE_INDEX: DurableObjectNamespace<DatabaseIndexDO>
  /** Default R2 bucket for new databases */
  DEFAULT_BUCKET?: string | undefined
}

/**
 * Context variables for database routes
 */
export interface DatabaseApiVariables extends AuthVariables {
  /** Authenticated user */
  user: AuthUser | null
  /** Actor entity ID for mutations */
  actor: EntityId | null
}

/**
 * Request body for creating a database
 */
export interface CreateDatabaseBody {
  /** Human-readable name */
  name: string
  /** Description */
  description?: string | undefined
  /** URL-friendly slug for public access */
  slug?: string | undefined
  /** Visibility level */
  visibility?: Visibility | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Request body for updating a database
 */
export interface UpdateDatabaseBody {
  /** New name */
  name?: string | undefined
  /** New description */
  description?: string | undefined
  /** New visibility level */
  visibility?: Visibility | undefined
  /** New URL-friendly slug */
  slug?: string | undefined
  /** Stats to update */
  stats?: {
    sizeBytes?: number | undefined
    collectionCount?: number | undefined
    entityCount?: number | undefined
  } | undefined
  /** Custom metadata to merge */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Response type for list databases endpoint
 */
export interface ListDatabasesResponse {
  databases: DatabaseInfo[]
}

/**
 * Response type for delete database endpoint
 */
export interface DeleteDatabaseResponse {
  deleted: boolean
}

// =============================================================================
// Route Handlers
// =============================================================================

/**
 * Create Hono app with database API routes
 *
 * Provides REST API endpoints for managing user databases:
 * - GET / - List all databases for the authenticated user
 * - POST /create - Create a new database
 * - GET /:id - Get a single database by ID
 * - PATCH /:id - Update a database
 * - DELETE /:id - Soft delete (unregister) a database
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { createDatabaseRoutes } from 'parquedb/studio'
 *
 * const app = new Hono<{ Bindings: Env }>()
 * app.route('/api/databases', createDatabaseRoutes())
 * ```
 */
export function createDatabaseRoutes<
  E extends DatabaseApiEnv = DatabaseApiEnv,
  V extends DatabaseApiVariables = DatabaseApiVariables
>() {
  const app = new Hono<{ Bindings: E; Variables: V }>()

  /**
   * GET /api/databases
   *
   * List all databases for the authenticated user.
   *
   * @returns { databases: DatabaseInfo[] }
   */
  app.get('/', async (c) => {
    const user = getUser(c)
    if (!user) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    try {
      const index = getUserDatabaseIndex(c.env, user.id)
      const databases = await index.list()

      return c.json({ databases } satisfies ListDatabasesResponse)
    } catch (error) {
      logger.error('[DatabaseAPI] Error listing databases:', error)
      return c.json(
        { error: error instanceof Error ? error.message : 'Failed to list databases' },
        500
      )
    }
  })

  /**
   * POST /api/databases/create
   *
   * Create a new database.
   * Requires CSRF validation (X-Requested-With header).
   *
   * @body { name, description?, slug?, visibility?, metadata? }
   * @returns DatabaseInfo
   */
  app.post('/create', async (c) => {
    // Validate CSRF protection
    const csrfResult = validateCsrf(c)
    if (!csrfResult.valid) {
      return c.json(
        {
          error: 'CSRF validation failed',
          code: 'CSRF_VALIDATION_FAILED',
          reason: csrfResult.reason,
        },
        403
      )
    }

    const user = getUser(c)
    const actor = (c.var as V).actor

    if (!user || !actor) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    let body: CreateDatabaseBody
    try {
      body = await c.req.json<CreateDatabaseBody>()
    } catch {
      return c.json({ error: 'Invalid JSON body' }, 400)
    }

    // Validate required fields
    if (!body.name?.trim()) {
      return c.json({ error: 'Database name is required' }, 400)
    }

    try {
      const index = getUserDatabaseIndex(c.env, user.id)

      // Build registration options
      const options: RegisterDatabaseOptions = {
        name: body.name.trim(),
        description: body.description?.trim(),
        slug: body.slug?.trim(),
        visibility: body.visibility,
        metadata: body.metadata,
        // Use default bucket or derive from user
        bucket: c.env.DEFAULT_BUCKET ?? `parquedb-${user.id}`,
        owner: user.id,
      }

      const database = await index.register(options, actor)

      return c.json(database, 201)
    } catch (error) {
      logger.error('[DatabaseAPI] Error creating database:', error)

      // Handle duplicate slug error
      if (error instanceof Error && error.message.includes('already exists')) {
        return c.json({ error: error.message }, 409)
      }

      return c.json(
        { error: error instanceof Error ? error.message : 'Failed to create database' },
        500
      )
    }
  })

  /**
   * GET /api/databases/:id
   *
   * Get a single database by ID.
   *
   * @param id - Database ID
   * @returns DatabaseInfo or 404
   */
  app.get('/:id', async (c) => {
    const user = getUser(c)
    if (!user) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    const id = c.req.param('id')

    try {
      const index = getUserDatabaseIndex(c.env, user.id)
      const database = await index.get(id)

      if (!database) {
        return c.json({ error: 'Database not found' }, 404)
      }

      // Record access for last-accessed tracking
      await index.recordAccess(id)

      return c.json(database)
    } catch (error) {
      logger.error('[DatabaseAPI] Error getting database:', error)
      return c.json(
        { error: error instanceof Error ? error.message : 'Failed to get database' },
        500
      )
    }
  })

  /**
   * PATCH /api/databases/:id
   *
   * Update a database's metadata.
   * Requires CSRF validation (X-Requested-With header).
   *
   * @param id - Database ID
   * @body UpdateDatabaseOptions
   * @returns Updated DatabaseInfo or 404
   */
  app.patch('/:id', async (c) => {
    // Validate CSRF protection
    const csrfResult = validateCsrf(c)
    if (!csrfResult.valid) {
      return c.json(
        {
          error: 'CSRF validation failed',
          code: 'CSRF_VALIDATION_FAILED',
          reason: csrfResult.reason,
        },
        403
      )
    }

    const user = getUser(c)
    if (!user) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    const id = c.req.param('id')

    let body: UpdateDatabaseBody
    try {
      body = await c.req.json<UpdateDatabaseBody>()
    } catch {
      return c.json({ error: 'Invalid JSON body' }, 400)
    }

    try {
      const index = getUserDatabaseIndex(c.env, user.id)

      // Verify database exists and belongs to user
      const existing = await index.get(id)
      if (!existing) {
        return c.json({ error: 'Database not found' }, 404)
      }

      // Build update options
      const options: UpdateDatabaseOptions = {}

      if (body.name !== undefined) {
        options.name = body.name.trim()
      }
      if (body.description !== undefined) {
        options.description = body.description.trim()
      }
      if (body.visibility !== undefined) {
        options.visibility = body.visibility
      }
      if (body.slug !== undefined) {
        options.slug = body.slug.trim() || undefined
      }
      if (body.stats !== undefined) {
        options.stats = body.stats
      }
      if (body.metadata !== undefined) {
        options.metadata = body.metadata
      }

      const database = await index.update(id, options)

      if (!database) {
        return c.json({ error: 'Database not found' }, 404)
      }

      return c.json(database)
    } catch (error) {
      logger.error('[DatabaseAPI] Error updating database:', error)

      // Handle duplicate slug error
      if (error instanceof Error && error.message.includes('already exists')) {
        return c.json({ error: error.message }, 409)
      }

      // Handle invalid visibility error
      if (error instanceof Error && error.message.includes('Invalid visibility')) {
        return c.json({ error: error.message }, 400)
      }

      // Handle invalid slug error
      if (error instanceof Error && error.message.includes('Invalid slug')) {
        return c.json({ error: error.message }, 400)
      }

      return c.json(
        { error: error instanceof Error ? error.message : 'Failed to update database' },
        500
      )
    }
  })

  /**
   * DELETE /api/databases/:id
   *
   * Soft delete (unregister) a database.
   * Requires CSRF validation (X-Requested-With header).
   * Note: This does NOT delete the actual data in R2.
   *
   * @param id - Database ID
   * @returns { deleted: true } or 404
   */
  app.delete('/:id', async (c) => {
    // Validate CSRF protection
    const csrfResult = validateCsrf(c)
    if (!csrfResult.valid) {
      return c.json(
        {
          error: 'CSRF validation failed',
          code: 'CSRF_VALIDATION_FAILED',
          reason: csrfResult.reason,
        },
        403
      )
    }

    const user = getUser(c)
    if (!user) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    const id = c.req.param('id')

    try {
      const index = getUserDatabaseIndex(c.env, user.id)

      // Verify database exists before deletion
      const existing = await index.get(id)
      if (!existing) {
        return c.json({ error: 'Database not found' }, 404)
      }

      const deleted = await index.unregister(id)

      if (!deleted) {
        return c.json({ error: 'Database not found' }, 404)
      }

      return c.json({ deleted: true } satisfies DeleteDatabaseResponse)
    } catch (error) {
      logger.error('[DatabaseAPI] Error deleting database:', error)
      return c.json(
        { error: error instanceof Error ? error.message : 'Failed to delete database' },
        500
      )
    }
  })

  return app
}

/**
 * Pre-configured database routes instance
 *
 * @example
 * ```typescript
 * import { databaseRoutes } from 'parquedb/studio'
 *
 * app.route('/api/databases', databaseRoutes)
 * ```
 */
export const databaseRoutes = createDatabaseRoutes()

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Require authentication helper for use in route handlers
 *
 * @throws Error if user is not authenticated
 */
export function requireAuthUser(c: Context): AuthUser {
  const user = getUser(c)
  if (!user) {
    throw new Error('Authentication required')
  }
  return user
}

/**
 * Get actor from context or throw
 *
 * @throws Error if actor is not available
 */
export function requireActor<V extends DatabaseApiVariables>(c: Context<{ Variables: V }>): EntityId {
  const actor = (c.var as V).actor
  if (!actor) {
    throw new Error('Actor required')
  }
  return actor
}

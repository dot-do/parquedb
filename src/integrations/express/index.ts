/**
 * Express Integration for ParqueDB
 *
 * Middleware adapter for easily integrating ParqueDB with Express applications.
 *
 * @example
 * ```typescript
 * import express from 'express'
 * import { createParqueDBMiddleware } from 'parquedb/express'
 *
 * const app = express()
 *
 * // Attach ParqueDB to all requests
 * app.use(createParqueDBMiddleware({
 *   basePath: './data',
 *   schema: {
 *     users: { name: 'string!', email: 'string!' },
 *     posts: { title: 'string!', content: 'text' }
 *   }
 * }))
 *
 * // Access db in route handlers
 * app.get('/users', async (req, res) => {
 *   const users = await req.db.users.find()
 *   res.json(users)
 * })
 *
 * app.post('/users', async (req, res) => {
 *   const user = await req.db.users.create(req.body)
 *   res.json(user)
 * })
 * ```
 *
 * @packageDocumentation
 */

import { ParqueDB } from '../../ParqueDB'
import type { ParqueDBConfig } from '../../ParqueDB/types'
import { FsBackend, MemoryBackend } from '../../storage'
import type { StorageBackend, Schema } from '../../types'
import { DB } from '../../db'
import type { DBSchema, DBInstance } from '../../db'

// =============================================================================
// Types
// =============================================================================

/**
 * Express Request with ParqueDB attached
 */
export interface ParqueDBRequest {
  /** ParqueDB instance */
  db: ParqueDB
}

/**
 * Configuration options for ParqueDB Express middleware
 */
export interface ParqueDBMiddlewareOptions {
  /**
   * Base path for file-based storage
   * If provided, uses FsBackend with this path
   */
  basePath?: string | undefined

  /**
   * Custom storage backend
   * Takes precedence over basePath if provided
   */
  storage?: StorageBackend | undefined

  /**
   * Schema definition (GraphDL format)
   * Use for validation and type inference
   */
  schema?: Schema | undefined

  /**
   * DB schema definition (DB() format)
   * Alternative to schema - provides inline field definitions
   */
  dbSchema?: DBSchema | undefined

  /**
   * Default namespace for operations
   */
  defaultNamespace?: string | undefined

  /**
   * Request property name for db instance
   * @default 'db'
   */
  propertyName?: string | undefined

  /**
   * Lifecycle hooks
   */
  hooks?: {
    /**
     * Called when middleware is initialized
     */
    onInit?: ((db: ParqueDB) => void | Promise<void>) | undefined

    /**
     * Called on each request before handler
     */
    onRequest?: ((db: ParqueDB, req: unknown) => void | Promise<void>) | undefined

    /**
     * Called on each response after handler
     */
    onResponse?: ((db: ParqueDB, req: unknown, res: unknown) => void | Promise<void>) | undefined

    /**
     * Called on errors
     */
    onError?: ((error: Error, db: ParqueDB) => void | Promise<void>) | undefined
  } | undefined
}

/**
 * Express-style middleware function type
 */
export type ExpressMiddleware = (
  req: Record<string, unknown>,
  res: Record<string, unknown>,
  next: (error?: Error) => void
) => void | Promise<void>

/**
 * Express-style error middleware function type
 */
export type ExpressErrorMiddleware = (
  error: Error,
  req: Record<string, unknown>,
  res: Record<string, unknown>,
  next: (error?: Error) => void
) => void | Promise<void>

// =============================================================================
// Middleware Factory
// =============================================================================

/**
 * Singleton instance holder for shared db across requests
 */
let sharedDbInstance: ParqueDB | null = null
let sharedDbConfig: ParqueDBMiddlewareOptions | null = null

/**
 * Creates Express middleware that attaches ParqueDB to each request
 *
 * @example
 * ```typescript
 * import { createParqueDBMiddleware } from 'parquedb/express'
 *
 * // File-based storage
 * app.use(createParqueDBMiddleware({ basePath: './data' }))
 *
 * // In-memory storage (for testing)
 * app.use(createParqueDBMiddleware({}))
 *
 * // Custom storage backend
 * app.use(createParqueDBMiddleware({
 *   storage: new R2Backend(env.BUCKET)
 * }))
 *
 * // With schema
 * app.use(createParqueDBMiddleware({
 *   basePath: './data',
 *   schema: mySchema
 * }))
 *
 * // With DB-style schema
 * app.use(createParqueDBMiddleware({
 *   basePath: './data',
 *   dbSchema: {
 *     users: { name: 'string!', email: 'string!' },
 *     posts: { title: 'string!', content: 'text' }
 *   }
 * }))
 * ```
 */
export function createParqueDBMiddleware(
  options: ParqueDBMiddlewareOptions = {}
): ExpressMiddleware {
  const {
    basePath,
    storage,
    schema,
    dbSchema,
    defaultNamespace,
    propertyName = 'db',
    hooks,
  } = options

  // Create or reuse db instance
  let db: ParqueDB | DBInstance

  const initPromise = (async () => {
    // Check if we can reuse an existing instance
    if (sharedDbInstance && sharedDbConfig === options) {
      db = sharedDbInstance
      return
    }

    // Create storage backend
    let storageBackend: StorageBackend
    if (storage) {
      storageBackend = storage
    } else if (basePath) {
      storageBackend = new FsBackend(basePath)
    } else {
      storageBackend = new MemoryBackend()
    }

    // Create db instance
    if (dbSchema) {
      // Use DB() factory for inline schema
      db = DB(dbSchema, { storage: storageBackend, defaultNamespace })
    } else {
      // Use ParqueDB directly
      const config: ParqueDBConfig = {
        storage: storageBackend,
        schema,
        defaultNamespace,
      }
      db = new ParqueDB(config)
    }

    // Store for reuse
    sharedDbInstance = db as ParqueDB
    sharedDbConfig = options

    // Call init hook
    if (hooks?.onInit) {
      await hooks.onInit(db as ParqueDB)
    }
  })()

  // Return middleware function
  return async (req, res, next) => {
    try {
      // Ensure db is initialized
      await initPromise

      // Attach db to request
      ;(req as Record<string, unknown>)[propertyName] = db

      // Call request hook
      if (hooks?.onRequest) {
        await hooks.onRequest(db as ParqueDB, req)
      }

      // Intercept response to call onResponse hook
      if (hooks?.onResponse) {
        const originalEnd = (res as { end?: (...args: unknown[]) => unknown }).end
        ;(res as { end: (...args: unknown[]) => unknown }).end = function (...args: unknown[]) {
          // Call response hook (fire and forget)
          const result = hooks.onResponse!(db as ParqueDB, req, res)
          if (result && typeof result === 'object' && 'catch' in result) {
            ;(result as Promise<void>).catch(() => {
              // Ignore errors in response hook
            })
          }
          return (originalEnd as (...args: unknown[]) => unknown)?.apply(this, args)
        }
      }

      next()
    } catch (error) {
      // Call error hook
      if (hooks?.onError && db) {
        try {
          await hooks.onError(error as Error, db as ParqueDB)
        } catch {
          // Ignore errors in error hook
        }
      }
      next(error as Error)
    }
  }
}

/**
 * Creates error handling middleware for ParqueDB errors
 *
 * @example
 * ```typescript
 * import { createParqueDBMiddleware, createErrorMiddleware } from 'parquedb/express'
 *
 * app.use(createParqueDBMiddleware({ basePath: './data' }))
 *
 * // Your routes...
 *
 * // Add error handler at the end
 * app.use(createErrorMiddleware())
 * ```
 */
export function createErrorMiddleware(): ExpressErrorMiddleware {
  return (error, _req, res, next) => {
    // Check if it's a ParqueDB error
    if (error && typeof error === 'object' && 'code' in error) {
      const err = error as Error & { code?: string; status?: number }

      // Map error codes to HTTP status codes
      let status = err.status || 500
      if (err.code === 'NOT_FOUND' || err.code === 'ENTITY_NOT_FOUND') {
        status = 404
      } else if (err.code === 'VALIDATION_ERROR') {
        status = 400
      } else if (err.code === 'CONFLICT' || err.code === 'VERSION_CONFLICT') {
        status = 409
      } else if (err.code === 'UNAUTHORIZED' || err.code === 'PERMISSION_DENIED') {
        status = 403
      }

      // Send JSON error response
      const resMethods = res as {
        status?: (code: number) => { json: (data: unknown) => void }
        json?: (data: unknown) => void
      }

      if (resMethods.status && typeof resMethods.status === 'function') {
        resMethods.status(status).json({
          error: {
            code: err.code,
            message: err.message,
          },
        })
        return
      }
    }

    // Pass to default error handler
    next(error)
  }
}

/**
 * Get the shared ParqueDB instance (for use outside of requests)
 *
 * @example
 * ```typescript
 * import { getSharedDB } from 'parquedb/express'
 *
 * // In a background job or startup script
 * const db = getSharedDB()
 * if (db) {
 *   await db.users.find()
 * }
 * ```
 */
export function getSharedDB(): ParqueDB | null {
  return sharedDbInstance
}

/**
 * Reset the shared db instance (useful for testing)
 */
export function resetSharedDB(): void {
  sharedDbInstance = null
  sharedDbConfig = null
}

// =============================================================================
// Type Augmentation for Express
// =============================================================================

/**
 * Augment Express Request type to include db
 *
 * Users can add this to their types:
 * ```typescript
 * declare global {
 *   namespace Express {
 *     interface Request {
 *       db: ParqueDB
 *     }
 *   }
 * }
 * ```
 */

// Export ParqueDB types for convenience
export { ParqueDB } from '../../ParqueDB'
export type { ParqueDBConfig } from '../../ParqueDB/types'
export type { Entity, Filter, FindOptions, CreateOptions, UpdateOptions, DeleteOptions } from '../../types'

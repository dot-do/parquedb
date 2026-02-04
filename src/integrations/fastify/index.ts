/**
 * Fastify Integration for ParqueDB
 *
 * Plugin adapter for easily integrating ParqueDB with Fastify applications.
 *
 * @example
 * ```typescript
 * import Fastify from 'fastify'
 * import { parquedbPlugin } from 'parquedb/fastify'
 *
 * const fastify = Fastify()
 *
 * // Register ParqueDB plugin
 * await fastify.register(parquedbPlugin, {
 *   basePath: './data',
 *   schema: {
 *     users: { name: 'string!', email: 'string!' },
 *     posts: { title: 'string!', content: 'text' }
 *   }
 * })
 *
 * // Access db via decorator
 * fastify.get('/users', async (request, reply) => {
 *   const users = await fastify.db.users.find()
 *   return users
 * })
 *
 * // Or via request
 * fastify.get('/users/:id', async (request, reply) => {
 *   const user = await request.db.users.get(request.params.id)
 *   return user
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
import { asPluginWithMetadata } from '../../types/cast'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration options for ParqueDB Fastify plugin
 */
export interface ParqueDBPluginOptions {
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
   * Decorator name for db instance on fastify and request
   * @default 'db'
   */
  decoratorName?: string | undefined

  /**
   * Whether to decorate request objects with db
   * @default true
   */
  decorateRequest?: boolean | undefined

  /**
   * Lifecycle hooks
   */
  hooks?: {
    /**
     * Called when plugin is registered
     */
    onInit?: ((db: ParqueDB) => void | Promise<void>) | undefined

    /**
     * Called on each request
     */
    onRequest?: ((db: ParqueDB, request: unknown) => void | Promise<void>) | undefined

    /**
     * Called on each response
     */
    onResponse?: ((db: ParqueDB, request: unknown, reply: unknown) => void | Promise<void>) | undefined

    /**
     * Called on errors
     */
    onError?: ((error: Error, db: ParqueDB) => void | Promise<void>) | undefined

    /**
     * Called when fastify is closing
     */
    onClose?: ((db: ParqueDB) => void | Promise<void>) | undefined
  } | undefined
}

/**
 * Fastify instance type (minimal interface)
 */
export interface FastifyInstance {
  decorate: (name: string, value: unknown) => void
  decorateRequest: (name: string, value: unknown) => void
  hasDecorator: (name: string) => boolean
  hasRequestDecorator: (name: string) => boolean
  addHook: (name: string, handler: (...args: unknown[]) => unknown) => void
  db?: ParqueDB
}

/**
 * Fastify plugin function type
 */
export type FastifyPluginAsync<T = Record<string, unknown>> = (
  instance: FastifyInstance,
  opts: T
) => Promise<void>

// =============================================================================
// Plugin Factory
// =============================================================================

/**
 * Creates a Fastify plugin that integrates ParqueDB
 *
 * @example
 * ```typescript
 * import Fastify from 'fastify'
 * import { parquedbPlugin } from 'parquedb/fastify'
 *
 * const fastify = Fastify()
 *
 * // File-based storage
 * await fastify.register(parquedbPlugin, { basePath: './data' })
 *
 * // In-memory storage (for testing)
 * await fastify.register(parquedbPlugin, {})
 *
 * // With schema
 * await fastify.register(parquedbPlugin, {
 *   basePath: './data',
 *   dbSchema: {
 *     users: { name: 'string!', email: 'string!' }
 *   }
 * })
 *
 * // Access db
 * fastify.db.users.find()
 * ```
 */
export const parquedbPlugin: FastifyPluginAsync<ParqueDBPluginOptions> = async (
  fastify,
  options
) => {
  const {
    basePath,
    storage,
    schema,
    dbSchema,
    defaultNamespace,
    decoratorName = 'db',
    decorateRequest = true,
    hooks,
  } = options

  // Check if already registered
  if (fastify.hasDecorator(decoratorName)) {
    throw new Error(`ParqueDB plugin already registered with decorator name '${decoratorName}'`)
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
  let db: ParqueDB | DBInstance

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

  // Call init hook
  if (hooks?.onInit) {
    await hooks.onInit(db as ParqueDB)
  }

  // Decorate fastify instance
  fastify.decorate(decoratorName, db)

  // Decorate request if enabled
  if (decorateRequest) {
    if (!fastify.hasRequestDecorator(decoratorName)) {
      // Use getter to reference the shared db instance
      fastify.decorateRequest(decoratorName, {
        getter() {
          return db
        },
      })
    }
  }

  // Add onRequest hook if needed
  if (hooks?.onRequest) {
    fastify.addHook('onRequest', async (request: unknown) => {
      await hooks.onRequest!(db as ParqueDB, request)
    })
  }

  // Add onResponse hook if needed
  if (hooks?.onResponse) {
    fastify.addHook('onResponse', async (request: unknown, reply: unknown) => {
      await hooks.onResponse!(db as ParqueDB, request, reply)
    })
  }

  // Add onError hook if needed
  if (hooks?.onError) {
    fastify.addHook('onError', async (...args: unknown[]) => {
      const error = args[2] as Error
      await hooks.onError!(error, db as ParqueDB)
    })
  }

  // Add onClose hook if needed
  if (hooks?.onClose) {
    fastify.addHook('onClose', async () => {
      await hooks.onClose!(db as ParqueDB)
    })
  }
}

// Add plugin metadata for fastify-plugin compatibility
;asPluginWithMetadata(parquedbPlugin)[Symbol.for('skip-override')] = true
;asPluginWithMetadata(parquedbPlugin)['@@fastify-plugin'] = {
  name: 'parquedb',
  fastify: '>=4.0.0',
}

/**
 * Alternative: Create middleware-style hook for manual integration
 *
 * @example
 * ```typescript
 * import Fastify from 'fastify'
 * import { createParqueDBHook } from 'parquedb/fastify'
 *
 * const fastify = Fastify()
 * const { onRequest, db } = createParqueDBHook({ basePath: './data' })
 *
 * // Manually add hook
 * fastify.addHook('onRequest', onRequest)
 *
 * // Use db directly
 * db.users.find()
 * ```
 */
export function createParqueDBHook(options: ParqueDBPluginOptions = {}): {
  db: ParqueDB
  onRequest: (request: Record<string, unknown>) => void
} {
  const { basePath, storage, schema, dbSchema, defaultNamespace, decoratorName = 'db' } = options

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
  let db: ParqueDB | DBInstance

  if (dbSchema) {
    db = DB(dbSchema, { storage: storageBackend, defaultNamespace })
  } else {
    const config: ParqueDBConfig = {
      storage: storageBackend,
      schema,
      defaultNamespace,
    }
    db = new ParqueDB(config)
  }

  return {
    db: db as ParqueDB,
    onRequest: (request: Record<string, unknown>) => {
      request[decoratorName] = db
    },
  }
}

/**
 * Error handler plugin for ParqueDB errors
 *
 * @example
 * ```typescript
 * import { parquedbPlugin, parquedbErrorHandler } from 'parquedb/fastify'
 *
 * await fastify.register(parquedbPlugin, { basePath: './data' })
 * await fastify.register(parquedbErrorHandler)
 * ```
 */
export const parquedbErrorHandler: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('onError', async (_request, reply: unknown, error: unknown) => {
    const err = error as Error & { code?: string; status?: number }

    // Check if it's a ParqueDB error
    if (err && typeof err === 'object' && 'code' in err) {
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
      const replyMethods = reply as {
        code?: (code: number) => { send: (data: unknown) => void }
        send?: (data: unknown) => void
      }

      if (replyMethods.code && typeof replyMethods.code === 'function') {
        replyMethods.code(status).send({
          error: {
            code: err.code,
            message: err.message,
          },
        })
      }
    }
  })
}

// Add plugin metadata
;asPluginWithMetadata(parquedbErrorHandler)[Symbol.for('skip-override')] = true
;asPluginWithMetadata(parquedbErrorHandler)['@@fastify-plugin'] = {
  name: 'parquedb-error-handler',
  fastify: '>=4.0.0',
}

// =============================================================================
// Type Augmentation for Fastify
// =============================================================================

/**
 * Augment Fastify types to include db
 *
 * Users can add this to their types:
 * ```typescript
 * declare module 'fastify' {
 *   interface FastifyInstance {
 *     db: ParqueDB
 *   }
 *   interface FastifyRequest {
 *     db: ParqueDB
 *   }
 * }
 * ```
 */

// Export ParqueDB types for convenience
export { ParqueDB } from '../../ParqueDB'
export type { ParqueDBConfig } from '../../ParqueDB/types'
export type { Entity, Filter, FindOptions, CreateOptions, UpdateOptions, DeleteOptions } from '../../types'

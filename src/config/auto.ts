/**
 * Auto-configured ParqueDB Instance
 *
 * Provides `db` and `sql` exports that automatically detect
 * the runtime environment and configure storage appropriately.
 *
 * @example
 * ```typescript
 * import { db, sql } from 'parquedb'
 *
 * // Just works - auto-configured for your environment
 * const users = await sql`SELECT * FROM users WHERE status = ${'active'}`
 * await db.Users.create({ name: 'Alice', email: 'alice@example.com' })
 * ```
 */

import { DB, type DBInstance } from '../db'
import { MemoryBackend } from '../storage/MemoryBackend'
import type { StorageBackend } from '../types'
import type { Env } from '../types/worker'
import { isServer, isWorkers } from './runtime'
import {
  loadWorkersEnv,
  detectBindings,
  detectStoragePaths,
  ensureDataDir,
  setWorkersEnv,
} from './env'
import { loadConfig, type ParqueDBConfig } from './loader'
import {
  setActorResolver,
  createOAuthActorResolver,
  createStaticActorResolver,
  createCombinedActorResolver,
  createEnvActorResolver,
} from './auth'
import type { EntityId } from '../types/entity'

// Module-scoped instance cache
let _db: DBInstance | null = null
let _initialized = false
let _initializing: Promise<DBInstance> | null = null

/**
 * Initialize the auto-configured database
 *
 * Detection order:
 * 1. Check for parquedb.config.ts (Node.js/Bun/Deno)
 * 2. Check for Workers env bindings (R2, DO)
 * 3. Check for data directory (.parquedb/, .db/, data/)
 * 4. Fall back to memory storage
 */
export async function initializeDB(env?: Env): Promise<DBInstance> {
  // Return cached instance if already initialized
  if (_db && _initialized) {
    return _db
  }

  // Prevent concurrent initialization
  if (_initializing) {
    return _initializing
  }

  _initializing = (async () => {
    let storage: StorageBackend | undefined
    let schema: Parameters<typeof DB>[0] = { schema: 'flexible' }

    // If Workers env is passed, cache it
    if (env) {
      setWorkersEnv(env)
    }

    // 1. Try to load config file (server environments only)
    let config: ParqueDBConfig | null = null
    if (isServer()) {
      config = await loadConfig()
      if (config) {
        storage = await resolveStorage(config)
        if (config.schema) {
          schema = config.schema
        }
        // Set up actor resolver from config
        setupActorResolver(config)
      }
    }

    // 2. Try Workers env bindings
    if (!storage && isWorkers()) {
      const workersEnv = env ?? (await loadWorkersEnv())
      if (workersEnv) {
        storage = await createWorkersStorage(workersEnv)
      }
    }

    // 3. Try filesystem detection (server environments)
    if (!storage && isServer()) {
      storage = await createServerStorage()
    }

    // 4. Fall back to memory
    if (!storage) {
      storage = new MemoryBackend()
      if (process.env.DEBUG || process.env.PARQUEDB_DEBUG) {
        console.debug('[ParqueDB] Using MemoryBackend (no storage configured)')
      }
    }

    _db = DB(schema, { storage })
    _initialized = true
    _initializing = null

    return _db
  })()

  return _initializing
}

/**
 * Get the auto-configured database instance
 *
 * Initializes on first call, returns cached instance thereafter.
 */
export async function getDB(env?: Env): Promise<DBInstance> {
  return initializeDB(env)
}

/**
 * Set up actor resolver from config
 */
function setupActorResolver(config: ParqueDBConfig): void {
  // Priority: resolveActor > useOAuth > actor > env var
  if (config.resolveActor) {
    setActorResolver(config.resolveActor)
    return
  }

  if (config.useOAuth) {
    // Use oauth.do for actor resolution
    setActorResolver(createCombinedActorResolver([
      createOAuthActorResolver(),
      createEnvActorResolver('PARQUEDB_ACTOR'),
      config.actor ? createStaticActorResolver(config.actor as EntityId) : async () => null,
    ]))
    return
  }

  if (config.actor) {
    // Static actor
    setActorResolver(createStaticActorResolver(config.actor as EntityId))
    return
  }

  // Default: try env var, then oauth.do
  setActorResolver(createCombinedActorResolver([
    createEnvActorResolver('PARQUEDB_ACTOR'),
    createOAuthActorResolver(),
  ]))
}

/**
 * Resolve storage from config
 */
async function resolveStorage(
  config: ParqueDBConfig
): Promise<StorageBackend | undefined> {
  if (!config.storage) return undefined

  // Already a StorageBackend instance
  if (typeof config.storage === 'object' && 'read' in config.storage) {
    return config.storage as StorageBackend
  }

  // String shorthand
  if (config.storage === 'memory') {
    return new MemoryBackend()
  }

  if (config.storage === 'fs') {
    const paths = await detectStoragePaths()
    const dataDir = paths?.dataDir ?? (await ensureDataDir())
    if (dataDir) {
      const { FsBackend } = await import('../storage/FsBackend')
      return new FsBackend(dataDir)
    }
    return new MemoryBackend()
  }

  // Object config
  if (typeof config.storage === 'object') {
    if (config.storage.type === 'fs') {
      const { FsBackend } = await import('../storage/FsBackend')
      return new FsBackend(config.storage.path)
    }

    if (config.storage.type === 'r2') {
      // R2 requires Workers env
      const workersEnv = await loadWorkersEnv()
      if (workersEnv?.BUCKET) {
        const { R2Backend } = await import('../storage/R2Backend')
        // Cast to any since Workers R2Bucket type differs from internal type
        return new R2Backend(workersEnv.BUCKET as any)
      }
    }

    // TODO: S3 support
  }

  return undefined
}

/**
 * Create storage from Workers environment bindings
 */
async function createWorkersStorage(env: Env): Promise<StorageBackend | undefined> {
  const bindings = detectBindings(env)

  // Prefer R2 for storage
  if (bindings.hasR2 && env.BUCKET) {
    const { R2Backend } = await import('../storage/R2Backend')
    // Cast to any since Workers R2Bucket type differs from internal type
    return new R2Backend(env.BUCKET as any)
  }

  // Could add DO-based storage here in the future

  return undefined
}

/**
 * Create storage for server environments
 */
async function createServerStorage(): Promise<StorageBackend | undefined> {
  const paths = await detectStoragePaths()

  // Use existing data directory
  if (paths?.dataDir) {
    const { FsBackend } = await import('../storage/FsBackend')
    return new FsBackend(paths.dataDir)
  }

  // Create default .parquedb directory
  const dataDir = await ensureDataDir()
  if (dataDir) {
    const { FsBackend } = await import('../storage/FsBackend')
    return new FsBackend(dataDir)
  }

  return undefined
}

/**
 * Reset the auto-configured instance (useful for testing)
 */
export function resetDB(): void {
  _db = null
  _initialized = false
  _initializing = null
}

// =============================================================================
// Proxy-based Lazy Access
// =============================================================================

/**
 * Create a proxy that lazily initializes on first access
 */
function createLazyProxy(): DBInstance {
  const handler: ProxyHandler<object> = {
    get(_target, prop: string | symbol) {
      // Handle special properties
      if (prop === 'then' || prop === Symbol.toStringTag) {
        return undefined
      }

      // sql property - return lazy sql executor
      if (prop === 'sql') {
        return createLazySqlProxy()
      }

      // Collection access - return proxy that initializes on method call
      if (typeof prop === 'string') {
        return createLazyCollectionProxy(prop)
      }

      return undefined
    },
  }

  return new Proxy({}, handler) as unknown as DBInstance
}

/**
 * Create a proxy for lazy collection access
 */
function createLazyCollectionProxy(collectionName: string) {
  const handler: ProxyHandler<object> = {
    get(_target, method: string | symbol) {
      if (typeof method !== 'string') return undefined

      // Return async function that initializes and calls method
      return async function (...args: unknown[]) {
        const db = await initializeDB()
        const collection = (db as any)[collectionName]
        if (collection && typeof collection[method] === 'function') {
          return collection[method](...args)
        }
        throw new Error(`Method ${method} not found on collection ${collectionName}`)
      }
    },
  }

  return new Proxy({}, handler)
}

/**
 * Create a proxy for lazy sql executor access
 */
function createLazySqlProxy() {
  // Template tag function
  const sqlProxy = async function (
    strings: TemplateStringsArray,
    ...values: unknown[]
  ) {
    const db = await initializeDB()
    return db.sql(strings, ...values)
  }

  // Add .raw method
  ;(sqlProxy as any).raw = async function (
    query: string,
    params?: unknown[],
    options?: unknown
  ) {
    const db = await initializeDB()
    return db.sql.raw(query, params, options as any)
  }

  return sqlProxy
}

// =============================================================================
// Exports
// =============================================================================

/**
 * Auto-configured ParqueDB instance
 *
 * Lazily initializes on first use, detecting:
 * - parquedb.config.ts (Node.js/Bun/Deno)
 * - Workers R2/DO bindings
 * - Data directories (.parquedb/, .db/, data/)
 *
 * @example
 * ```typescript
 * import { db } from 'parquedb'
 *
 * await db.Users.create({ name: 'Alice' })
 * const users = await db.Users.find({ status: 'active' })
 * ```
 */
export const db: DBInstance = createLazyProxy()

/**
 * Auto-configured SQL executor
 *
 * @example
 * ```typescript
 * import { sql } from 'parquedb'
 *
 * const users = await sql`SELECT * FROM users WHERE age > ${21}`
 * ```
 */
export const sql = createLazySqlProxy()

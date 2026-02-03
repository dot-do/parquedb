/**
 * Config File Loader
 *
 * Loads parquedb.config.ts/js files for configuration.
 * Similar to vite.config.ts, next.config.js patterns.
 */

import type { StorageBackend } from '../types'
import type { EntityId } from '../types/entity'
import type { Visibility, CorsConfig } from '../types/visibility'
import type { DBSchema } from '../db'
import { detectRuntime } from './runtime'
import { detectStoragePaths } from './env'
import type { ActorResolver } from './auth'

/**
 * Layout configuration - array for no tabs, object for tabs
 *
 * Array = rows without tabs: [['title', 'slug'], 'content']
 * Object = tabs: { Content: [['title'], 'content'], Settings: ['status'] }
 */
export type LayoutConfig =
  | (string | string[])[]  // No tabs
  | Record<string, (string | string[])[]>  // With tabs

/**
 * Field-level studio/UI configuration
 */
export interface FieldStudioConfig {
  /** Display label */
  label?: string
  /** Help text */
  description?: string
  /** For select fields - simple string array or label/value pairs */
  options?: string[] | Array<{ label: string; value: string }>
  /** Hide from list view */
  hideInList?: boolean
  /** Read-only field */
  readOnly?: boolean
  /** Validation */
  min?: number
  max?: number
  minLength?: number
  maxLength?: number
  /** Relationship target */
  relationTo?: string | string[]
}

/**
 * Collection-level studio configuration
 */
export interface CollectionStudioConfig {
  /** Layout: array = rows, object = tabs */
  layout?: LayoutConfig
  /** Sidebar fields */
  sidebar?: string[]
  /** Collection display label */
  label?: string
  /** Singular label */
  labelSingular?: string
  /** Description */
  description?: string
  /** Field to use as title in admin */
  useAsTitle?: string
  /** Default columns in list view */
  defaultColumns?: string[]
  /** Group in admin nav */
  group?: string
  /** Hide from admin nav */
  hidden?: boolean
  /** Field-level config */
  fields?: Record<string, FieldStudioConfig>
}

/**
 * Global studio configuration
 */
export interface StudioConfig {
  /** Theme: 'light' | 'dark' | 'auto' */
  theme?: 'light' | 'dark' | 'auto'
  /** Default fields to always put in sidebar */
  defaultSidebar?: string[]
  /** Port for studio server */
  port?: number
  /** Collection-specific studio config (alternative to $studio in schema) */
  collections?: Record<string, CollectionStudioConfig>
}

/**
 * ParqueDB configuration options
 */
export interface ParqueDBConfig {
  /** Storage backend instance or configuration */
  storage?:
    | StorageBackend
    | 'memory'
    | 'fs'
    | { type: 'fs'; path: string }
    | { type: 'r2'; bucket: string }
    | { type: 's3'; bucket: string; region?: string }

  /** Database schema definition (supports $layout, $studio, $rows, $tabs, $sidebar) */
  schema?: DBSchema

  /** Default namespace for collections */
  defaultNamespace?: string

  /** Enable debug logging */
  debug?: boolean

  /** Default actor for mutations (static) */
  actor?: string | EntityId

  /** Dynamic actor resolver function */
  resolveActor?: ActorResolver

  /** Use oauth.do for actor resolution (shorthand for resolveActor) */
  useOAuth?: boolean

  /** Studio/admin UI configuration */
  studio?: StudioConfig

  /**
   * Default visibility level for the database
   * - 'public': Discoverable and accessible by anyone
   * - 'unlisted': Accessible with direct link, not discoverable
   * - 'private': Requires authentication (default)
   *
   * Collections inherit this unless they specify their own $visibility
   */
  visibility?: Visibility

  /**
   * CORS configuration for public/unlisted database access
   *
   * Controls which origins can access the database via browser requests.
   * Only applies to public and unlisted databases.
   *
   * @example
   * ```typescript
   * defineConfig({
   *   visibility: 'public',
   *   cors: {
   *     origins: ['https://myapp.com'],
   *     credentials: true
   *   }
   * })
   * ```
   */
  cors?: CorsConfig

  /** Environment-specific overrides */
  environments?: {
    development?: Partial<ParqueDBConfig>
    production?: Partial<ParqueDBConfig>
    test?: Partial<ParqueDBConfig>
  }
}

/**
 * Define configuration with type safety
 *
 * @example
 * ```typescript
 * // parquedb.config.ts
 * import { defineConfig } from 'parquedb/config'
 *
 * export default defineConfig({
 *   storage: { type: 'fs', path: './data' },
 *   schema: {
 *     User: { email: 'string!#', name: 'string' }
 *   }
 * })
 * ```
 */
export function defineConfig(config: ParqueDBConfig): ParqueDBConfig {
  return config
}

/**
 * Define a schema with type safety
 *
 * Use this to export your schema for reuse with DB() for full type inference.
 *
 * @example
 * ```typescript
 * // parquedb.config.ts
 * import { defineConfig, defineSchema } from 'parquedb/config'
 *
 * // Export schema for type inference
 * export const schema = defineSchema({
 *   User: {
 *     email: 'string!#',
 *     name: 'string',
 *     age: 'int?'
 *   },
 *   Post: {
 *     title: 'string!',
 *     content: 'text',
 *     author: '-> User'
 *   }
 * })
 *
 * export default defineConfig({
 *   storage: { type: 'fs', path: './data' },
 *   schema
 * })
 *
 * // Then in your app:
 * // src/db.ts
 * import { DB } from 'parquedb'
 * import { schema } from '../parquedb.config'
 *
 * // Fully typed!
 * export const db = DB(schema)
 * export const { sql } = db
 * ```
 */
export function defineSchema<T extends DBSchema>(schema: T): T {
  return schema
}

// Module-scoped cache
let _config: ParqueDBConfig | null = null
let _configLoaded = false

/**
 * Load configuration from parquedb.config.ts/js
 *
 * Only works in Node.js/Bun/Deno environments.
 * Returns null in Workers/Browser (no filesystem access).
 */
export async function loadConfig(): Promise<ParqueDBConfig | null> {
  if (_configLoaded) return _config

  const runtime = detectRuntime()

  // Can't load config files in Workers or Browser
  if (runtime === 'cloudflare-workers' || runtime === 'browser') {
    _configLoaded = true
    return null
  }

  try {
    const paths = await detectStoragePaths()
    if (!paths?.configFile) {
      _configLoaded = true
      return null
    }

    // Dynamic import the config file
    // Works with .ts in Bun, tsx, ts-node
    // Works with .js/.mjs in Node.js
    const configModule = await import(paths.configFile)

    // Support both default export and named 'config' export
    const config = configModule.default ?? configModule.config ?? configModule.parquedb

    if (config && typeof config === 'object') {
      _config = config as ParqueDBConfig
      _configLoaded = true

      // Apply environment-specific overrides
      const env = detectEnvironment()
      if (env && _config.environments?.[env]) {
        _config = { ..._config, ..._config.environments[env] }
      }

      return _config
    }

    _configLoaded = true
    return null
  } catch (error) {
    // Config file exists but failed to load
    if (process.env.DEBUG || process.env.PARQUEDB_DEBUG) {
      console.warn('[ParqueDB] Failed to load config:', error)
    }
    _configLoaded = true
    return null
  }
}

/**
 * Get cached config (must call loadConfig first)
 */
export function getConfig(): ParqueDBConfig | null {
  return _config
}

/**
 * Set config manually (useful for testing or explicit configuration)
 */
export function setConfig(config: ParqueDBConfig): void {
  _config = config
  _configLoaded = true
}

/**
 * Clear cached config (useful for testing)
 */
export function clearConfig(): void {
  _config = null
  _configLoaded = false
}

/**
 * Detect current environment (development/production/test)
 */
function detectEnvironment(): 'development' | 'production' | 'test' | null {
  // Check common environment variables
  const env =
    process.env.NODE_ENV ??
    process.env.ENVIRONMENT ??
    process.env.CF_ENVIRONMENT

  if (env === 'development' || env === 'dev') return 'development'
  if (env === 'production' || env === 'prod') return 'production'
  if (env === 'test' || env === 'testing') return 'test'

  return null
}

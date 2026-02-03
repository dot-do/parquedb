/**
 * DB() Factory - Simple API for creating ParqueDB instances with schema
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 *
 * // Typed collections with inline schema
 * const db = DB({
 *   Occupation: {
 *     name: 'string!',       // required
 *     socCode: 'string!#',   // required + indexed
 *     jobZone: 'int',
 *     skills: '-> Skill.requiredBy[]'
 *   },
 *   Skill: {
 *     name: 'string!#',
 *     category: 'string'
 *   }
 * })
 *
 * // Flexible mode (schema-less)
 * const db = DB({ schema: 'flexible' })
 *
 * // Mixed mode
 * const db = DB({
 *   Occupation: { name: 'string!#' },  // typed
 *   Posts: 'flexible'                   // schema-less
 * })
 * ```
 *
 * @packageDocumentation
 */

import { Graph, graphToIceType } from '@icetype/core'
import type { ParsedGraph } from '@icetype/core'
import { ParqueDB } from './ParqueDB'
import type { ParqueDBConfig } from './ParqueDB/types'
import type { StorageBackend } from './types'
import { fromIceType } from './types/integrations'
import { MemoryBackend } from './storage'
import { createSQL } from './integrations/sql'
import type { SQLExecutor } from './integrations/sql'

// =============================================================================
// Types
// =============================================================================

/**
 * Field-level studio/UI configuration (inline in schema)
 */
export interface InlineFieldStudio {
  /** Display label */
  label?: string
  /** Help text */
  description?: string
  /** For select fields - simple array or label/value pairs */
  options?: string[] | Array<{ label: string; value: string }>
  /** Hide from list */
  hideInList?: boolean
  /** Read-only */
  readOnly?: boolean
}

/**
 * Collection-level studio configuration (inline in schema via $studio)
 */
export interface InlineStudioConfig {
  /** Collection label */
  label?: string
  /** Field to use as title */
  useAsTitle?: string
  /** Default list columns */
  defaultColumns?: string[]
  /** Admin nav group */
  group?: string
  /** Per-field UI config */
  [field: string]: string | string[] | InlineFieldStudio | undefined
}

/**
 * Layout configuration - array for no tabs, object for tabs
 *
 * @example
 * ```typescript
 * // No tabs - just rows
 * $layout: [['title', 'slug'], 'content', ['status', 'author']]
 *
 * // With tabs (object keys = tab names)
 * $layout: {
 *   Content: [['title', 'slug'], 'content'],
 *   Settings: ['status', 'publishedAt'],
 * }
 * ```
 */
export type LayoutConfig =
  | (string | string[])[]  // No tabs: array of rows
  | Record<string, (string | string[])[]>  // Tabs: object with tab names

/**
 * Per-collection storage and behavior options
 *
 * @example
 * ```typescript
 * const db = DB({
 *   Occupation: {
 *     $options: {
 *       includeDataVariant: true,  // Include $data variant column (default: true)
 *     },
 *     name: 'string!',
 *     socCode: 'string!#',
 *   },
 *   Logs: {
 *     $options: { includeDataVariant: false },  // Omit $data for write-heavy logs
 *     level: 'string',
 *     message: 'string',
 *   }
 * })
 * ```
 */
export interface CollectionOptions {
  /** Include $data variant column for fast full-row reads (default: true) */
  includeDataVariant?: boolean
  // Future options can be added here:
  // compression?: 'snappy' | 'gzip' | 'zstd' | 'none'
  // partitionBy?: string[]
  // rowGroupSize?: number
}

/**
 * Schema definition for a single collection
 * Supports field definitions plus optional layout/studio/options config
 *
 * @example
 * ```typescript
 * {
 *   title: 'string!',
 *   slug: 'string!',
 *   content: 'text',
 *   status: 'string',
 *
 *   // Storage/behavior options
 *   $options: { includeDataVariant: true },
 *
 *   // Layout - array = no tabs, object = tabs
 *   $layout: [['title', 'slug'], 'content'],
 *   // or with tabs:
 *   $layout: { Main: [['title', 'slug'], 'content'], Meta: ['status'] },
 *
 *   // Sidebar fields
 *   $sidebar: ['$id', 'status', 'createdAt'],
 *
 *   // UI config
 *   $studio: {
 *     label: 'Blog Posts',
 *     status: { options: ['draft', 'published'] }
 *   }
 * }
 * ```
 */
export interface CollectionSchemaWithLayout {
  /** Storage and behavior options */
  $options?: CollectionOptions
  /** Layout: array = rows, object = tabs with rows */
  $layout?: LayoutConfig
  /** Sidebar fields */
  $sidebar?: string[]
  /** UI/studio configuration */
  $studio?: InlineStudioConfig
  /** Field definitions (type strings like 'string!', 'int', '-> User') */
  [field: string]: string | CollectionOptions | LayoutConfig | string[] | InlineStudioConfig | undefined
}

/**
 * Schema definition for a single collection
 * Either an object with field definitions or 'flexible' for schema-less mode
 */
export type CollectionSchema = CollectionSchemaWithLayout | 'flexible'

/**
 * Schema definition for the entire database
 */
export interface DBSchema {
  [collection: string]: CollectionSchema
}

/**
 * Special marker for flexible mode
 */
export interface FlexibleModeConfig {
  schema: 'flexible'
}

/**
 * Configuration options for DB()
 */
export interface DBConfig {
  /** Storage backend (defaults to MemoryBackend) */
  storage?: StorageBackend
  /** Default namespace for operations */
  defaultNamespace?: string
}

/**
 * Input to DB() - either a schema object or flexible mode config
 */
export type DBInput = DBSchema | FlexibleModeConfig

/**
 * ParqueDB instance with SQL executor attached
 */
export type DBInstance = ParqueDB & {
  /** SQL template tag for queries */
  sql: SQLExecutor
}

// =============================================================================
// Type Guards
// =============================================================================

function isFlexibleMode(input: DBInput): input is FlexibleModeConfig {
  return 'schema' in input && input.schema === 'flexible'
}

function isCollectionFlexible(schema: CollectionSchema): schema is 'flexible' {
  return schema === 'flexible'
}

// =============================================================================
// Schema Conversion
// =============================================================================

/**
 * Convert DB schema notation to GraphDL input format
 * Filters out $-prefixed layout/studio config, keeping only field definitions
 */
function convertToGraphDLInput(schema: DBSchema): Record<string, Record<string, string>> {
  const result: Record<string, Record<string, string>> = {}

  for (const [name, collectionSchema] of Object.entries(schema)) {
    if (isCollectionFlexible(collectionSchema)) {
      // Skip flexible collections - they don't have a schema
      continue
    }

    // Filter out $-prefixed keys (layout/studio config)
    const fields: Record<string, string> = {}
    for (const [key, value] of Object.entries(collectionSchema)) {
      if (!key.startsWith('$') && typeof value === 'string') {
        fields[key] = value
      }
    }
    result[name] = fields
  }

  return result
}

/**
 * Get list of flexible collections from schema
 */
function getFlexibleCollections(schema: DBSchema): string[] {
  return Object.entries(schema)
    .filter(([, collectionSchema]) => isCollectionFlexible(collectionSchema))
    .map(([name]) => name)
}

// =============================================================================
// Collection Options Helpers
// =============================================================================

/**
 * Default collection options
 */
export const DEFAULT_COLLECTION_OPTIONS: Required<CollectionOptions> = {
  includeDataVariant: true,
}

/**
 * Extract $options from a collection schema
 * Returns default options if not specified or for flexible collections
 */
export function extractCollectionOptions(schema: CollectionSchema): CollectionOptions {
  if (schema === 'flexible') {
    return { ...DEFAULT_COLLECTION_OPTIONS }
  }
  return { ...DEFAULT_COLLECTION_OPTIONS, ...schema.$options }
}

/**
 * Get field definitions from a collection schema, excluding $-prefixed config
 * Returns empty object for flexible collections
 */
export function getFieldsWithoutOptions(schema: CollectionSchema): Record<string, string> {
  if (schema === 'flexible') {
    return {}
  }
  const fields: Record<string, string> = {}
  for (const [key, value] of Object.entries(schema)) {
    if (!key.startsWith('$') && typeof value === 'string') {
      fields[key] = value
    }
  }
  return fields
}

/**
 * Extract all collection options from a database schema
 * Returns a map of collection name -> options
 */
export function extractAllCollectionOptions(schema: DBSchema): Map<string, CollectionOptions> {
  const optionsMap = new Map<string, CollectionOptions>()
  for (const [name, collectionSchema] of Object.entries(schema)) {
    optionsMap.set(name, extractCollectionOptions(collectionSchema))
  }
  return optionsMap
}

// =============================================================================
// DB Factory
// =============================================================================

/**
 * Create a ParqueDB instance with optional schema
 *
 * @param input - Schema definition or flexible mode config
 * @param config - Optional configuration (storage backend, etc.)
 * @returns ParqueDB instance with typed collections
 *
 * @example
 * ```typescript
 * // With typed schema
 * const db = DB({
 *   User: {
 *     email: 'string!#',
 *     name: 'string',
 *     age: 'int?',
 *     posts: '<- Post.author[]'
 *   },
 *   Post: {
 *     title: 'string!',
 *     content: 'string',
 *     author: '-> User'
 *   }
 * })
 *
 * // Access collections
 * await db.User.create({ email: 'alice@example.com', name: 'Alice' })
 * await db.Post.find({ author: 'user/123' })
 *
 * // SQL queries
 * const users = await db.sql`SELECT * FROM users WHERE age > ${21}`
 *
 * // Destructured
 * const { sql } = DB()
 * const posts = await sql`SELECT * FROM posts LIMIT ${10}`
 * ```
 */
export function DB(input: DBInput = { schema: 'flexible' }, config: DBConfig = {}): DBInstance {
  const storage = config.storage ?? new MemoryBackend()

  const parqueDBConfig: ParqueDBConfig = {
    storage,
    defaultNamespace: config.defaultNamespace,
  }

  const db = new ParqueDB(parqueDBConfig)

  // If flexible mode, attach SQL and return
  if (isFlexibleMode(input)) {
    const dbWithSql = db as DBInstance
    dbWithSql.sql = createSQL(db)
    return dbWithSql
  }

  // Parse typed schemas using GraphDL
  const graphInput = convertToGraphDLInput(input)

  if (Object.keys(graphInput).length > 0) {
    // Parse with GraphDL
    const graph: ParsedGraph = Graph(graphInput)

    // Convert to IceType schemas
    const iceTypeSchemas = graphToIceType(graph)

    // Convert IceType schemas to ParqueDB Schema format
    const parqueDBSchema = fromIceType(iceTypeSchemas)

    // Register the converted schema
    db.registerSchema(parqueDBSchema)
  }

  // Track flexible collections (for future use - different storage mode)
  const flexibleCollections = getFlexibleCollections(input)
  if (flexibleCollections.length > 0) {
    // TODO: Mark these collections for flexible/variant storage mode
    // For now they work the same as typed collections
  }

  // Attach SQL executor
  const dbWithSql = db as DBInstance
  dbWithSql.sql = createSQL(db)

  return dbWithSql
}

// =============================================================================
// Default Instance
// =============================================================================

/**
 * Default ParqueDB instance in flexible mode
 *
 * @example
 * ```typescript
 * import { db } from 'parquedb'
 *
 * await db.Posts.create({ title: 'Hello', content: 'World' })
 * await db.Posts.find({ title: 'Hello' })
 *
 * // SQL queries
 * const posts = await db.sql`SELECT * FROM posts`
 * ```
 */
export const db: DBInstance = DB()

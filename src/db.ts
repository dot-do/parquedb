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

// =============================================================================
// Types
// =============================================================================

/**
 * Schema definition for a single collection
 * Either an object with field definitions or 'flexible' for schema-less mode
 */
export type CollectionSchema = Record<string, string> | 'flexible'

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
 */
function convertToGraphDLInput(schema: DBSchema): Record<string, Record<string, string>> {
  const result: Record<string, Record<string, string>> = {}

  for (const [name, collectionSchema] of Object.entries(schema)) {
    if (isCollectionFlexible(collectionSchema)) {
      // Skip flexible collections - they don't have a schema
      continue
    }
    result[name] = collectionSchema
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
 * ```
 */
export function DB(input: DBInput = { schema: 'flexible' }, config: DBConfig = {}): ParqueDB {
  const storage = config.storage ?? new MemoryBackend()

  const parqueDBConfig: ParqueDBConfig = {
    storage,
    defaultNamespace: config.defaultNamespace,
  }

  const db = new ParqueDB(parqueDBConfig)

  // If flexible mode, just return the db as-is
  if (isFlexibleMode(input)) {
    return db
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

  return db
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
 * ```
 */
export const db = DB()

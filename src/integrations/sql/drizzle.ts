/**
 * Drizzle ORM Adapter for ParqueDB
 *
 * Provides a Drizzle proxy callback that translates SQL to ParqueDB queries.
 * Uses hyparquet's parquetQuery() with full predicate pushdown.
 *
 * @example
 * ```typescript
 * import { drizzle } from 'drizzle-orm/pg-proxy'
 * import { createDrizzleProxy } from 'parquedb/sql'
 *
 * const proxy = createDrizzleProxy(parquedb)
 * const db = drizzle(proxy)
 *
 * // Now use Drizzle normally
 * const users = await db.select().from(users).where(eq(users.status, 'active'))
 * ```
 */

import type { ParqueDB } from '../../ParqueDB.js'
import type {
  DrizzleProxyCallback,
  DrizzleProxyResult,
  DrizzleMethod,
  SQLSelect,
} from './types.js'
import { parseSQL } from './parser.js'
import { translateSelect, translateInsert, translateUpdate, translateDelete } from './translator.js'

// ============================================================================
// Drizzle Proxy Factory
// ============================================================================

export interface DrizzleProxyOptions {
  /** Log SQL queries for debugging */
  debug?: boolean
  /** Default actor for mutations */
  actor?: string
}

/**
 * Create a Drizzle proxy callback for ParqueDB
 *
 * @param db - ParqueDB instance
 * @param options - Configuration options
 * @returns Drizzle proxy callback function
 */
export function createDrizzleProxy(
  db: ParqueDB,
  options: DrizzleProxyOptions = {}
): DrizzleProxyCallback {
  const { debug = false, actor = 'drizzle' } = options

  return async (sql: string, params: unknown[], method: DrizzleMethod): Promise<DrizzleProxyResult> => {
    if (debug) {
      console.log('[drizzle-parquedb] SQL:', sql)
      console.log('[drizzle-parquedb] Params:', params)
      console.log('[drizzle-parquedb] Method:', method)
    }

    try {
      const stmt = parseSQL(sql)

      switch (stmt.type) {
        case 'SELECT': {
          const query = translateSelect(stmt, params)
          const collection = db.collection(query.collection)

          const results = await collection.find(query.filter, {
            limit: query.limit,
            skip: query.offset,
            sort: query.orderBy ? { [query.orderBy]: query.desc ? -1 : 1 } : undefined,
          })

          // Format results for Drizzle
          const rows = formatResultsForDrizzle(results, query.columns, method)
          return { rows }
        }

        case 'INSERT': {
          const mutation = translateInsert(stmt, params)
          const collection = db.collection(mutation.collection)

          const result = await collection.create(mutation.data!, { actor })

          // Handle RETURNING
          const rows = mutation.returning
            ? formatResultsForDrizzle([result], mutation.returning === '*' ? undefined : mutation.returning, method)
            : []

          return { rows }
        }

        case 'UPDATE': {
          const mutation = translateUpdate(stmt, params)
          const collection = db.collection(mutation.collection)

          // Find entities matching filter, then update each
          const entities = await collection.find(mutation.filter || {})
          const results: Record<string, unknown>[] = []

          for (const entity of entities) {
            const updated = await collection.update(entity.$id, { $set: mutation.data }, { actor })
            if (updated) {
              results.push(updated)
            }
          }

          // Handle RETURNING
          const rows = mutation.returning
            ? formatResultsForDrizzle(results, mutation.returning === '*' ? undefined : mutation.returning, method)
            : []

          return { rows }
        }

        case 'DELETE': {
          const mutation = translateDelete(stmt, params)
          const collection = db.collection(mutation.collection)

          // Find entities matching filter
          const entities = await collection.find(mutation.filter || {})
          const results: Record<string, unknown>[] = []

          for (const entity of entities) {
            await collection.delete(entity.$id, { actor })
            results.push(entity)
          }

          // Handle RETURNING
          const rows = mutation.returning
            ? formatResultsForDrizzle(results, mutation.returning === '*' ? undefined : mutation.returning, method)
            : []

          return { rows }
        }

        default:
          throw new Error(`Unsupported statement type: ${(stmt as any).type}`)
      }
    } catch (error) {
      if (debug) {
        console.error('[drizzle-parquedb] Error:', error)
      }
      throw error
    }
  }
}

// ============================================================================
// Result Formatting
// ============================================================================

/**
 * Format ParqueDB results for Drizzle's expected format
 */
function formatResultsForDrizzle(
  results: Record<string, unknown>[],
  columns: string[] | undefined,
  method: DrizzleMethod
): unknown[][] | unknown[] {
  // Project columns if specified
  const projected = columns
    ? results.map((row) => {
        const obj: Record<string, unknown> = {}
        for (const col of columns) {
          obj[col] = getNestedValue(row, col)
        }
        return obj
      })
    : results

  switch (method) {
    case 'get':
      // Return first row as array of values
      if (projected.length === 0) return []
      return Object.values(projected[0])

    case 'all':
      // Return array of arrays (rows × columns)
      return projected.map((row) => Object.values(row))

    case 'values':
      // Return array of arrays (rows × columns), similar to 'all'
      return projected.map((row) => Object.values(row))

    case 'run':
      // For mutations, return affected rows info
      return [[projected.length]]

    default:
      return projected.map((row) => Object.values(row))
  }
}

/**
 * Get nested value from object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

// ============================================================================
// Type Helpers for Drizzle Schema Generation
// ============================================================================

/**
 * Generate a Drizzle table schema from ParqueDB collection schema
 * (For future use with @icetype/drizzle integration)
 */
export function getTableName(collection: string): string {
  return collection.toLowerCase()
}

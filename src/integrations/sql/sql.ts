/**
 * SQL Template Tag for ParqueDB
 *
 * Provides a simple way to execute SQL queries against ParqueDB
 * with automatic parameter binding.
 *
 * @example
 * ```typescript
 * import { createSQL } from 'parquedb/sql'
 *
 * const sql = createSQL(db)
 *
 * // Simple queries
 * const users = await sql`SELECT * FROM users WHERE status = ${'active'}`
 *
 * // With parameters
 * const posts = await sql`
 *   SELECT * FROM posts
 *   WHERE author_id = ${userId}
 *   AND created_at > ${startDate}
 *   ORDER BY created_at DESC
 *   LIMIT ${10}
 * `
 * ```
 */

import type { ParqueDB } from '../../ParqueDB.js'
import type { SQLQueryResult, SQLQueryOptions } from './types.js'
import { parseSQL } from './parser.js'
import { translateSelect, translateInsert, translateUpdate, translateDelete } from './translator.js'

// ============================================================================
// SQL Template Tag
// ============================================================================

export interface SQLExecutor {
  /**
   * Execute a SQL query using template literal syntax
   *
   * @example
   * const users = await sql`SELECT * FROM users WHERE age > ${25}`
   */
  <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<SQLQueryResult<T>>

  /**
   * Execute a raw SQL query string with parameters
   *
   * @example
   * const users = await sql.raw('SELECT * FROM users WHERE age > $1', [25])
   */
  raw<T = Record<string, unknown>>(
    query: string,
    params?: unknown[],
    options?: SQLQueryOptions
  ): Promise<SQLQueryResult<T>>
}

export interface CreateSQLOptions {
  /** Log SQL queries for debugging */
  debug?: boolean
  /** Default actor for mutations */
  actor?: string
}

/**
 * Create a SQL executor for a ParqueDB instance
 *
 * @param db - ParqueDB instance
 * @param options - Configuration options
 * @returns SQL executor function
 */
export function createSQL(db: ParqueDB, options: CreateSQLOptions = {}): SQLExecutor {
  const { debug = false, actor = 'sql' } = options

  const execute = async <T = Record<string, unknown>>(
    query: string,
    params: unknown[] = [],
    queryOptions: SQLQueryOptions = {}
  ): Promise<SQLQueryResult<T>> => {
    if (debug) {
      console.log('[sql] Query:', query)
      console.log('[sql] Params:', params)
    }

    const stmt = parseSQL(query)

    switch (stmt.type) {
      case 'SELECT': {
        const translated = translateSelect(stmt, params)
        const collection = db.collection(translated.collection)

        const results = await collection.find(translated.filter, {
          limit: translated.limit,
          skip: translated.offset,
          sort: translated.orderBy
            ? { [translated.orderBy]: translated.desc ? -1 : 1 }
            : undefined,
        })

        // Apply column projection if specified
        const projected = translated.columns
          ? results.map((row) => {
              const obj: Record<string, unknown> = {}
              for (const col of translated.columns!) {
                obj[col] = getNestedValue(row, col)
              }
              return obj as T
            })
          : (results as T[])

        return {
          rows: projected,
          rowCount: projected.length,
          command: 'SELECT',
        }
      }

      case 'INSERT': {
        const mutation = translateInsert(stmt, params)
        const collection = db.collection(mutation.collection)

        const result = await collection.create(mutation.data!, { actor })

        return {
          rows: [result as T],
          rowCount: 1,
          command: 'INSERT',
        }
      }

      case 'UPDATE': {
        const mutation = translateUpdate(stmt, params)
        const collection = db.collection(mutation.collection)

        // Find matching entities and update each
        const entities = await collection.find(mutation.filter || {})
        const results: T[] = []

        for (const entity of entities) {
          const updated = await collection.update(
            entity.$id,
            { $set: mutation.data },
            { actor }
          )
          if (updated) {
            results.push(updated as T)
          }
        }

        return {
          rows: results,
          rowCount: results.length,
          command: 'UPDATE',
        }
      }

      case 'DELETE': {
        const mutation = translateDelete(stmt, params)
        const collection = db.collection(mutation.collection)

        // Find matching entities and delete each
        const entities = await collection.find(mutation.filter || {})
        const results: T[] = []

        for (const entity of entities) {
          await collection.delete(entity.$id, { actor })
          results.push(entity as T)
        }

        return {
          rows: results,
          rowCount: results.length,
          command: 'DELETE',
        }
      }

      default:
        throw new Error(`Unsupported statement type: ${(stmt as any).type}`)
    }
  }

  // Template tag function
  const sql = async <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<SQLQueryResult<T>> => {
    // Build query with $1, $2, etc. placeholders
    let query = strings[0]
    for (let i = 0; i < values.length; i++) {
      query += `$${i + 1}${strings[i + 1]}`
    }

    return execute<T>(query, values)
  }

  // Attach raw method
  sql.raw = execute

  return sql as SQLExecutor
}

// ============================================================================
// Helper Functions
// ============================================================================

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
// Convenience Helpers
// ============================================================================

/**
 * Build a parameterized query from a template
 *
 * @example
 * const { query, params } = buildQuery`SELECT * FROM users WHERE age > ${25}`
 * // query: "SELECT * FROM users WHERE age > $1"
 * // params: [25]
 */
export function buildQuery(
  strings: TemplateStringsArray,
  ...values: unknown[]
): { query: string; params: unknown[] } {
  let query = strings[0]
  for (let i = 0; i < values.length; i++) {
    query += `$${i + 1}${strings[i + 1]}`
  }
  return { query, params: values }
}

/**
 * Escape a SQL identifier (table name, column name)
 */
export function escapeIdentifier(identifier: string): string {
  // Double any existing double quotes and wrap in double quotes
  return `"${identifier.replace(/"/g, '""')}"`
}

/**
 * Escape a SQL string literal
 */
export function escapeString(value: string): string {
  // Escape single quotes by doubling them
  return `'${value.replace(/'/g, "''")}'`
}

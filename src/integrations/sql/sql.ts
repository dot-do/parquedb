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
import type { EntityId } from '../../types/entity.js'
import type { SQLQueryResult, SQLQueryOptions } from './types.js'
import { parseSQL } from './parser.js'
import { translateSelect, translateInsert, translateUpdate, translateDelete } from './translator.js'
import { sqlResultAs, sqlItemsAs } from '../../types/cast.js'

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
  const { debug = false, actor = 'system/sql' } = options

  const execute = async <T = Record<string, unknown>>(
    query: string,
    params: unknown[] = []
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

        const result = await collection.find(translated.filter, {
          limit: translated.limit,
          skip: translated.offset,
          sort: translated.orderBy
            ? { [translated.orderBy]: translated.desc ? -1 : 1 }
            : undefined,
        })

        // Apply column projection if specified (PaginatedResult has .items)
        const items = result.items as Record<string, unknown>[]
        const projected = translated.columns
          ? items.map((row) => {
              const obj: Record<string, unknown> = {}
              for (const col of translated.columns!) {
                obj[col] = getNestedValue(row, col)
              }
              return obj as T
            })
          : sqlItemsAs<T>(items)

        return {
          rows: projected,
          rowCount: projected.length,
          command: 'SELECT',
        }
      }

      case 'INSERT': {
        const mutation = translateInsert(stmt, params)
        const collection = db.collection(mutation.collection)

        // Derive $type and name from collection and data
        const $type = capitalize(mutation.collection)
        const data = mutation.data || {}
        const name = (data.name as string) || (data.title as string) || generateName()

        const result = await collection.create(
          { ...data, $type, name } as Parameters<typeof collection.create>[0],
          { actor: actor as EntityId }
        )

        return {
          rows: [sqlResultAs<T>(result)],
          rowCount: 1,
          command: 'INSERT',
        }
      }

      case 'UPDATE': {
        const mutation = translateUpdate(stmt, params)
        const collection = db.collection(mutation.collection)

        // Find matching entities and update each
        const findResult = await collection.find(mutation.filter || {})
        const results: T[] = []

        for (const entity of findResult.items) {
          const localId = extractLocalId(entity.$id)
          const updated = await collection.update(
            localId,
            { $set: mutation.data } as Parameters<typeof collection.update>[1],
            { actor: actor as EntityId }
          )
          if (updated) {
            results.push(sqlResultAs<T>(updated))
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
        const findResult = await collection.find(mutation.filter || {})
        const results: T[] = []

        for (const entity of findResult.items) {
          const localId = extractLocalId(entity.$id)
          await collection.delete(localId, { actor: actor as EntityId })
          results.push(sqlResultAs<T>(entity))
        }

        return {
          rows: results,
          rowCount: results.length,
          command: 'DELETE',
        }
      }

      default:
        throw new Error(`Unsupported statement type: ${(stmt as { type: string }).type}`)
    }
  }

  // Template tag function
  const sql = async <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<SQLQueryResult<T>> => {
    // Build query with $1, $2, etc. placeholders
    // Note: For template literals, strings always has one more element than values
    let query = strings[0] || ''
    for (let i = 0; i < values.length; i++) {
      query += `$${i + 1}${strings[i + 1] || ''}`
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

/**
 * Extract local ID from full EntityId (e.g., "users/123" -> "123")
 */
function extractLocalId(entityId: EntityId | string): string {
  const parts = String(entityId).split('/')
  return parts[parts.length - 1] || entityId as string
}

/**
 * Capitalize first letter of string
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Generate a simple name for entities without one
 */
function generateName(): string {
  return `item-${Date.now()}`
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
  // Note: For template literals, strings always has one more element than values
  let query = strings[0] || ''
  for (let i = 0; i < values.length; i++) {
    query += `$${i + 1}${strings[i + 1] || ''}`
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

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
import type { EntityId } from '../../types/entity.js'
import { logger } from '../../utils/logger.js'
import type {
  DrizzleProxyCallback,
  DrizzleProxyResult,
  DrizzleMethod,
  SQLStatement,
  TranslatedMutation,
} from './types.js'
import { parseSQL } from './parser.js'
import { translateSelect, translateInsert, translateUpdate, translateDelete } from './translator.js'

// ============================================================================
// Transaction State
// ============================================================================

/**
 * In-memory transaction state for managing BEGIN/COMMIT/ROLLBACK
 */
interface TransactionState {
  active: boolean
  operations: Array<{
    type: 'create' | 'update' | 'delete'
    collection: string
    entityId?: string
    beforeState?: Record<string, unknown>
  }>
  savepoints: Map<string, number> // savepoint name -> operations index
}

// ============================================================================
// Drizzle Proxy Factory
// ============================================================================

export interface DrizzleProxyOptions {
  /** Log SQL queries for debugging */
  debug?: boolean | undefined
  /** Default actor for mutations */
  actor?: string | undefined
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
  const { debug = false, actor = 'system/drizzle' } = options

  // Transaction state (per proxy instance)
  const txState: TransactionState = {
    active: false,
    operations: [],
    savepoints: new Map(),
  }

  return async (sql: string, params: unknown[], method: DrizzleMethod): Promise<DrizzleProxyResult> => {
    if (debug) {
      logger.debug('[drizzle-parquedb] SQL:', sql)
      logger.debug('[drizzle-parquedb] Params:', params)
      logger.debug('[drizzle-parquedb] Method:', method)
    }

    try {
      const stmt = parseSQL(sql)

      switch (stmt.type) {
        case 'TRANSACTION':
          return handleTransaction(stmt, txState, db, actor as EntityId)

        case 'SELECT':
          return handleSelect(stmt, params, db, method)

        case 'INSERT':
          return handleInsert(stmt, params, db, txState, actor as EntityId, method)

        case 'UPDATE':
          return handleUpdate(stmt, params, db, txState, actor as EntityId, method)

        case 'DELETE':
          return handleDelete(stmt, params, db, txState, actor as EntityId, method)

        default:
          throw new Error(`Unsupported statement type: ${(stmt as { type: string }).type}`)
      }
    } catch (error) {
      if (debug) {
        logger.error('[drizzle-parquedb] Error:', error)
      }
      throw error
    }
  }
}

// ============================================================================
// Statement Handlers
// ============================================================================

/**
 * Handle transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
 */
async function handleTransaction(
  stmt: SQLStatement & { type: 'TRANSACTION' },
  txState: TransactionState,
  db: ParqueDB,
  actor: EntityId
): Promise<DrizzleProxyResult> {
  switch (stmt.action) {
    case 'BEGIN':
      txState.active = true
      txState.operations = []
      txState.savepoints.clear()
      return { rows: [] }

    case 'COMMIT':
      // Commit is a no-op since we apply changes immediately
      // In a real implementation, you'd batch changes and apply here
      txState.active = false
      txState.operations = []
      txState.savepoints.clear()
      return { rows: [] }

    case 'ROLLBACK':
      if (stmt.savepoint) {
        // Rollback to savepoint
        const savepointIndex = txState.savepoints.get(stmt.savepoint)
        if (savepointIndex !== undefined) {
          // Rollback operations after savepoint
          const opsToRollback = txState.operations.slice(savepointIndex)
          await rollbackOperations(opsToRollback, db, actor)
          txState.operations = txState.operations.slice(0, savepointIndex)
        }
      } else {
        // Full rollback
        await rollbackOperations(txState.operations, db, actor)
        txState.active = false
        txState.operations = []
        txState.savepoints.clear()
      }
      return { rows: [] }

    case 'SAVEPOINT':
      if (stmt.savepoint) {
        txState.savepoints.set(stmt.savepoint, txState.operations.length)
      }
      return { rows: [] }

    default:
      return { rows: [] }
  }
}

/**
 * Handle SELECT statements
 */
async function handleSelect(
  stmt: SQLStatement & { type: 'SELECT' },
  params: unknown[],
  db: ParqueDB,
  method: DrizzleMethod
): Promise<DrizzleProxyResult> {
  const query = translateSelect(stmt, params)
  const collection = db.collection(query.collection)

  const result = await collection.find(query.filter, {
    limit: query.limit,
    skip: query.offset,
    sort: query.orderBy ? { [query.orderBy]: query.desc ? -1 : 1 } : undefined,
  })

  const rows = formatResultsForDrizzle(
    result.items as Record<string, unknown>[],
    query.columns,
    method
  )
  return { rows }
}

/**
 * Handle INSERT statements
 */
async function handleInsert(
  stmt: SQLStatement & { type: 'INSERT' },
  params: unknown[],
  db: ParqueDB,
  txState: TransactionState,
  actor: EntityId,
  method: DrizzleMethod
): Promise<DrizzleProxyResult> {
  const mutation = translateInsert(stmt, params)
  const collection = db.collection(mutation.collection)

  const $type = capitalize(mutation.collection)
  const data = mutation.data || {}
  const name = (data.name as string) || (data.title as string) || generateName()

  // Apply default values based on collection type
  const dataWithDefaults = applyDefaultValues(mutation.collection, data)

  const result = await collection.create(
    { ...dataWithDefaults, $type, name } as Parameters<typeof collection.create>[0],
    { actor }
  )

  // Track for potential rollback
  if (txState.active) {
    txState.operations.push({
      type: 'create',
      collection: mutation.collection,
      entityId: result.$id,
    })
  }

  const rows = mutation.returning
    ? formatResultsForDrizzle([result], getReturningColumns(mutation.returning), method)
    : []

  return { rows }
}

/**
 * Handle UPDATE statements
 */
async function handleUpdate(
  stmt: SQLStatement & { type: 'UPDATE' },
  params: unknown[],
  db: ParqueDB,
  txState: TransactionState,
  actor: EntityId,
  method: DrizzleMethod
): Promise<DrizzleProxyResult> {
  const mutation = translateUpdate(stmt, params)
  const collection = db.collection(mutation.collection)

  const findResult = await collection.find(mutation.filter || {})
  const results: Record<string, unknown>[] = []

  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)
    const beforeState = txState.active ? { ...entity } : undefined

    const updated = await collection.update(
      localId,
      { $set: mutation.data } as Parameters<typeof collection.update>[1],
      { actor }
    )

    if (updated) {
      results.push(updated as Record<string, unknown>)

      if (txState.active && beforeState) {
        txState.operations.push({
          type: 'update',
          collection: mutation.collection,
          entityId: entity.$id,
          beforeState: beforeState as Record<string, unknown>,
        })
      }
    }
  }

  const rows = mutation.returning
    ? formatResultsForDrizzle(results, getReturningColumns(mutation.returning), method)
    : []

  return { rows }
}

/**
 * Handle DELETE statements
 */
async function handleDelete(
  stmt: SQLStatement & { type: 'DELETE' },
  params: unknown[],
  db: ParqueDB,
  txState: TransactionState,
  actor: EntityId,
  method: DrizzleMethod
): Promise<DrizzleProxyResult> {
  const mutation = translateDelete(stmt, params)
  const collection = db.collection(mutation.collection)

  const findResult = await collection.find(mutation.filter || {})
  const results: Record<string, unknown>[] = []

  for (const entity of findResult.items) {
    const localId = extractLocalId(entity.$id)
    const beforeState = txState.active ? { ...entity } : undefined

    await collection.delete(localId, { actor })
    results.push(entity as Record<string, unknown>)

    if (txState.active && beforeState) {
      txState.operations.push({
        type: 'delete',
        collection: mutation.collection,
        entityId: entity.$id,
        beforeState: beforeState as Record<string, unknown>,
      })
    }
  }

  const rows = mutation.returning
    ? formatResultsForDrizzle(results, getReturningColumns(mutation.returning), method)
    : []

  return { rows }
}

/**
 * Rollback operations in reverse order
 */
async function rollbackOperations(
  operations: TransactionState['operations'],
  db: ParqueDB,
  actor: EntityId
): Promise<void> {
  // Rollback in reverse order
  for (let i = operations.length - 1; i >= 0; i--) {
    const op = operations[i]
    if (!op) continue

    const collection = db.collection(op.collection)

    switch (op.type) {
      case 'create':
        // Delete the created entity
        if (op.entityId) {
          const localId = extractLocalId(op.entityId)
          await collection.delete(localId, { actor })
        }
        break

      case 'update':
        // Restore to before state
        if (op.entityId && op.beforeState) {
          const localId = extractLocalId(op.entityId)
          // Use $set to restore all fields from beforeState
          await collection.update(
            localId,
            { $set: op.beforeState } as Parameters<typeof collection.update>[1],
            { actor }
          )
        }
        break

      case 'delete':
        // Restore the deleted entity
        if (op.beforeState) {
          await collection.create(
            op.beforeState as Parameters<typeof collection.create>[0],
            { actor }
          )
        }
        break
    }
  }
}

/**
 * Extract columns from RETURNING clause
 */
function getReturningColumns(returning: TranslatedMutation['returning']): string[] | undefined {
  return returning === '*' ? undefined : returning
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
      return Object.values(projected[0] || {})

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

/**
 * Apply default values for specific collections/tables
 */
function applyDefaultValues(
  collection: string,
  data: Record<string, unknown>
): Record<string, unknown> {
  const result = { ...data }

  // Apply defaults based on collection type
  switch (collection.toLowerCase()) {
    case 'posts':
      // Default views to 0 if not provided
      if (result.views === undefined) {
        result.views = 0
      }
      break
    // Add more collection-specific defaults as needed
  }

  return result
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

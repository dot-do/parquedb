/**
 * Prisma Driver Adapter for ParqueDB
 *
 * Implements the Prisma Driver Adapter interface to use ParqueDB as a backend.
 * Translates Prisma's SQL queries to ParqueDB operations.
 *
 * @example
 * ```typescript
 * import { PrismaClient } from '@prisma/client'
 * import { PrismaParqueDBAdapter } from 'parquedb/sql'
 *
 * const adapter = new PrismaParqueDBAdapter(parquedb)
 * const prisma = new PrismaClient({ adapter })
 *
 * // Now use Prisma normally
 * const users = await prisma.user.findMany({ where: { status: 'active' } })
 * ```
 */

import type { ParqueDB } from '../../ParqueDB.js'
import type { EntityId } from '../../types/entity.js'
import { logger } from '../../utils/logger.js'
import type {
  PrismaDriverAdapter,
  PrismaQuery,
  PrismaResultSet,
  PrismaTransaction,
} from './types.js'
import { parseSQL } from './parser.js'
import { translateSelect, translateInsert, translateUpdate, translateDelete } from './translator.js'

// ============================================================================
// Constants
// ============================================================================

/** Log prefix for consistent logging */
const LOG_PREFIX = '[prisma-parquedb]'

/** Default actor for mutations when not specified */
const DEFAULT_ACTOR = 'system/prisma'

// ============================================================================
// Configuration Types
// ============================================================================

/**
 * Options for creating a Prisma adapter
 */
export interface PrismaAdapterOptions {
  /** Log SQL queries for debugging */
  debug?: boolean | undefined
  /** Default actor for mutations */
  actor?: string | undefined
}

/**
 * Internal configuration passed between adapter and transaction
 */
interface AdapterConfig {
  debug: boolean
  actor: string
}

/**
 * Transaction state for tracking commit/rollback status
 */
type TransactionState = 'active' | 'committed' | 'rolled_back'

/**
 * Prisma Driver Adapter for ParqueDB
 *
 * Implements the DriverAdapter interface required by Prisma Client
 * when using the driverAdapters preview feature.
 */
export class PrismaParqueDBAdapter implements PrismaDriverAdapter {
  readonly provider = 'sqlite' as const // We mimic SQLite for type mapping
  readonly adapterName = 'parquedb'

  private readonly db: ParqueDB
  private readonly config: AdapterConfig

  constructor(db: ParqueDB, options: PrismaAdapterOptions = {}) {
    this.db = db
    this.config = {
      debug: options.debug ?? false,
      actor: options.actor ?? DEFAULT_ACTOR,
    }
  }

  /** Whether debug logging is enabled */
  private get debug(): boolean {
    return this.config.debug
  }

  /** Actor for mutations */
  private get actor(): string {
    return this.config.actor
  }

  /**
   * Execute a raw query and return results
   *
   * @param query - SQL query with parameters
   * @returns Query results in Prisma format
   * @throws Error if query is not a SELECT statement
   */
  async queryRaw(query: PrismaQuery): Promise<PrismaResultSet> {
    const { sql, args } = query

    this.logQuery('queryRaw', sql, args)

    try {
      const stmt = parseSQL(sql)

      if (stmt.type !== 'SELECT') {
        throw new Error(`queryRaw expects SELECT, got ${stmt.type}`)
      }

      const translated = translateSelect(stmt, args)
      const collection = this.db.collection(translated.collection)

      const result = await collection.find(translated.filter, {
        limit: translated.limit,
        skip: translated.offset,
        sort: translated.orderBy
          ? { [translated.orderBy]: translated.desc ? -1 : 1 }
          : undefined,
      })

      return formatResults(result.items as Record<string, unknown>[], translated.columns)
    } catch (error) {
      this.logError('queryRaw', error)
      throw error
    }
  }

  /**
   * Execute a raw query for mutations (INSERT/UPDATE/DELETE)
   *
   * @param query - SQL mutation query with parameters
   * @returns Number of affected rows
   * @throws Error if query is not a mutation statement
   */
  async executeRaw(query: PrismaQuery): Promise<number> {
    const { sql, args } = query

    this.logQuery('executeRaw', sql, args)

    try {
      const stmt = parseSQL(sql)

      switch (stmt.type) {
        case 'INSERT':
          return this.executeInsert(stmt, args)

        case 'UPDATE':
          return this.executeUpdate(stmt, args)

        case 'DELETE':
          return this.executeDelete(stmt, args)

        default:
          throw new Error(`executeRaw expects INSERT/UPDATE/DELETE, got ${stmt.type}`)
      }
    } catch (error) {
      this.logError('executeRaw', error)
      throw error
    }
  }

  /**
   * Execute an INSERT statement
   */
  private async executeInsert(stmt: import('./types.js').SQLInsert, args: unknown[]): Promise<number> {
    const mutation = translateInsert(stmt, args)
    const collection = this.db.collection(mutation.collection)

    // Derive $type and name from collection and data
    const $type = capitalize(mutation.collection)
    const data = mutation.data || {}
    const name = deriveEntityName(data)

    await collection.create(
      { ...data, $type, name } as Parameters<typeof collection.create>[0],
      { actor: this.actor as EntityId }
    )
    return 1
  }

  /**
   * Execute an UPDATE statement
   */
  private async executeUpdate(stmt: import('./types.js').SQLUpdate, args: unknown[]): Promise<number> {
    const mutation = translateUpdate(stmt, args)
    const collection = this.db.collection(mutation.collection)

    // Find matching entities and update each
    const findResult = await collection.find(mutation.filter || {})
    let count = 0

    for (const entity of findResult.items) {
      const localId = extractLocalId(entity.$id)
      const updated = await collection.update(
        localId,
        { $set: mutation.data } as Parameters<typeof collection.update>[1],
        { actor: this.actor as EntityId }
      )
      if (updated) count++
    }

    return count
  }

  /**
   * Execute a DELETE statement
   */
  private async executeDelete(stmt: import('./types.js').SQLDelete, args: unknown[]): Promise<number> {
    const mutation = translateDelete(stmt, args)
    const collection = this.db.collection(mutation.collection)

    // Find matching entities and delete each
    const findResult = await collection.find(mutation.filter || {})
    let count = 0

    for (const entity of findResult.items) {
      const localId = extractLocalId(entity.$id)
      await collection.delete(localId, { actor: this.actor as EntityId })
      count++
    }

    return count
  }

  /**
   * Start a new transaction
   *
   * @returns A new transaction instance
   */
  async startTransaction(): Promise<PrismaTransaction> {
    return new PrismaParqueDBTransaction(this.db, this.config)
  }

  /**
   * Log a query if debug mode is enabled
   */
  private logQuery(method: string, sql: string, args: unknown[]): void {
    if (this.debug) {
      logger.debug(`${LOG_PREFIX} ${method}:`, sql)
      logger.debug(`${LOG_PREFIX} args:`, args)
    }
  }

  /**
   * Log an error if debug mode is enabled
   */
  private logError(method: string, error: unknown): void {
    if (this.debug) {
      logger.error(`${LOG_PREFIX} ${method} error:`, error)
    }
  }
}

// ============================================================================
// Prisma Transaction
// ============================================================================

/**
 * Prisma transaction implementation for ParqueDB
 *
 * Note: ParqueDB doesn't have true ACID transactions for Parquet files.
 * This implementation executes operations immediately (no batching).
 * For true transactional semantics, use ParqueDB with SQLite backend.
 *
 * Limitations:
 * - Rollback does not undo previous operations (append-only storage)
 * - Isolation is not guaranteed between concurrent transactions
 */
class PrismaParqueDBTransaction implements PrismaTransaction {
  private readonly db: ParqueDB
  private readonly config: AdapterConfig
  private state: TransactionState = 'active'

  constructor(db: ParqueDB, config: AdapterConfig) {
    this.db = db
    this.config = config
  }

  /**
   * Execute a query within this transaction
   */
  async queryRaw(query: PrismaQuery): Promise<PrismaResultSet> {
    this.assertActive('queryRaw')
    return this.getAdapter().queryRaw(query)
  }

  /**
   * Execute a mutation within this transaction
   */
  async executeRaw(query: PrismaQuery): Promise<number> {
    this.assertActive('executeRaw')
    return this.getAdapter().executeRaw(query)
  }

  /**
   * Commit the transaction
   *
   * @throws Error if transaction is already committed or rolled back
   */
  async commit(): Promise<void> {
    this.assertActive('commit')
    this.state = 'committed'
    // Operations already executed - nothing to flush
    // In a true transactional system, we would flush buffered operations here
  }

  /**
   * Roll back the transaction
   *
   * Note: ParqueDB uses append-only storage, so rollback cannot undo
   * operations that have already been executed.
   *
   * @throws Error if transaction is already committed or rolled back
   */
  async rollback(): Promise<void> {
    this.assertActive('rollback')
    this.state = 'rolled_back'

    if (this.config.debug) {
      logger.warn(`${LOG_PREFIX} Rollback called, but ParqueDB uses append-only storage`)
    }
  }

  /**
   * Get an adapter instance for executing operations
   */
  private getAdapter(): PrismaParqueDBAdapter {
    return new PrismaParqueDBAdapter(this.db, this.config)
  }

  /**
   * Assert that the transaction is still active
   */
  private assertActive(operation: string): void {
    if (this.state === 'committed') {
      throw new Error(`Cannot ${operation}: Transaction already committed`)
    }
    if (this.state === 'rolled_back') {
      throw new Error(`Cannot ${operation}: Transaction already rolled back`)
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Format ParqueDB results to Prisma's expected format
 *
 * @param results - Array of entity records
 * @param columns - Optional column list to include (all columns if not specified)
 * @returns Prisma-compatible result set
 */
function formatResults(
  results: Record<string, unknown>[],
  columns?: string[]
): PrismaResultSet {
  if (results.length === 0) {
    return {
      columns: columns || [],
      rows: [],
      columnTypes: [],
    }
  }

  // Determine columns from first result if not specified
  const actualColumns = columns || (results[0] ? Object.keys(results[0]) : [])

  // Build rows as arrays of values
  const rows = results.map((row) =>
    actualColumns.map((col) => getNestedValue(row, col))
  )

  // Prisma expects column types as numbers (we use 0 for unknown/text)
  const columnTypes = actualColumns.map(() => 0)

  return {
    columns: actualColumns,
    rows,
    columnTypes,
  }
}

/**
 * Get nested value from object using dot notation
 *
 * @param obj - Object to extract value from
 * @param path - Dot-separated path (e.g., "user.profile.name")
 * @returns The value at the path, or null if not found
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return null
    if (typeof current !== 'object') return null
    current = (current as Record<string, unknown>)[part]
  }

  return current ?? null
}

/**
 * Extract local ID from full EntityId
 *
 * @example
 * extractLocalId("users/123") // "123"
 * extractLocalId("123") // "123"
 */
function extractLocalId(entityId: EntityId | string): string {
  const parts = String(entityId).split('/')
  return parts[parts.length - 1] || (entityId as string)
}

/**
 * Capitalize first letter of string
 */
function capitalize(str: string): string {
  if (!str) return str
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Derive entity name from data fields
 *
 * Looks for common name fields: name, title
 * Falls back to timestamp-based name
 */
function deriveEntityName(data: Record<string, unknown>): string {
  if (typeof data.name === 'string' && data.name) {
    return data.name
  }
  if (typeof data.title === 'string' && data.title) {
    return data.title
  }
  return `item-${Date.now()}`
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a Prisma Driver Adapter for ParqueDB
 *
 * @param db - ParqueDB instance
 * @param options - Adapter options
 * @returns Prisma Driver Adapter
 */
export function createPrismaAdapter(
  db: ParqueDB,
  options: PrismaAdapterOptions = {}
): PrismaParqueDBAdapter {
  return new PrismaParqueDBAdapter(db, options)
}

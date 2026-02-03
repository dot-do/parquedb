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
// Prisma Driver Adapter
// ============================================================================

export interface PrismaAdapterOptions {
  /** Log SQL queries for debugging */
  debug?: boolean | undefined
  /** Default actor for mutations */
  actor?: string | undefined
}

/**
 * Prisma Driver Adapter for ParqueDB
 *
 * Implements the DriverAdapter interface required by Prisma Client
 * when using the driverAdapters preview feature.
 */
export class PrismaParqueDBAdapter implements PrismaDriverAdapter {
  readonly provider = 'sqlite' as const // We mimic SQLite for type mapping
  readonly adapterName = 'parquedb'

  private db: ParqueDB
  private debug: boolean
  private actor: string

  constructor(db: ParqueDB, options: PrismaAdapterOptions = {}) {
    this.db = db
    this.debug = options.debug ?? false
    this.actor = options.actor ?? 'system/prisma'
  }

  /**
   * Execute a raw query and return results
   */
  async queryRaw(query: PrismaQuery): Promise<PrismaResultSet> {
    const { sql, args } = query

    if (this.debug) {
      logger.debug('[prisma-parquedb] queryRaw:', sql)
      logger.debug('[prisma-parquedb] args:', args)
    }

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

      // Format for Prisma (PaginatedResult has .items)
      return this.formatResults(result.items as Record<string, unknown>[], translated.columns)
    } catch (error) {
      if (this.debug) {
        logger.error('[prisma-parquedb] queryRaw error:', error)
      }
      throw error
    }
  }

  /**
   * Execute a raw query for mutations (INSERT/UPDATE/DELETE)
   * Returns the number of affected rows
   */
  async executeRaw(query: PrismaQuery): Promise<number> {
    const { sql, args } = query

    if (this.debug) {
      logger.debug('[prisma-parquedb] executeRaw:', sql)
      logger.debug('[prisma-parquedb] args:', args)
    }

    try {
      const stmt = parseSQL(sql)

      switch (stmt.type) {
        case 'INSERT': {
          const mutation = translateInsert(stmt, args)
          const collection = this.db.collection(mutation.collection)

          // Derive $type and name from collection and data
          const $type = capitalize(mutation.collection)
          const data = mutation.data || {}
          const name = (data.name as string) || (data.title as string) || generateName()

          await collection.create(
            { ...data, $type, name } as Parameters<typeof collection.create>[0],
            { actor: this.actor as EntityId }
          )
          return 1
        }

        case 'UPDATE': {
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

        case 'DELETE': {
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

        default:
          throw new Error(`executeRaw expects INSERT/UPDATE/DELETE, got ${stmt.type}`)
      }
    } catch (error) {
      if (this.debug) {
        logger.error('[prisma-parquedb] executeRaw error:', error)
      }
      throw error
    }
  }

  /**
   * Start a new transaction
   */
  async startTransaction(): Promise<PrismaTransaction> {
    return new PrismaParqueDBTransaction(this.db, {
      debug: this.debug,
      actor: this.actor,
    })
  }

  /**
   * Format ParqueDB results to Prisma's expected format
   */
  private formatResults(
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

    // Build rows as arrays
    const rows = results.map((row) =>
      actualColumns.map((col) => this.getNestedValue(row, col))
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
   */
  private getNestedValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current === null || current === undefined) return null
      if (typeof current !== 'object') return null
      current = (current as Record<string, unknown>)[part]
    }

    return current ?? null
  }
}

// ============================================================================
// Prisma Transaction
// ============================================================================

/**
 * Prisma transaction implementation for ParqueDB
 *
 * Note: ParqueDB doesn't have true ACID transactions for Parquet files.
 * This implementation batches operations and executes them sequentially.
 * For true transactional semantics, use ParqueDB with SQLite backend.
 */
class PrismaParqueDBTransaction implements PrismaTransaction {
  private db: ParqueDB
  private debug: boolean
  private actor: string
  private committed = false
  private rolledBack = false

  constructor(db: ParqueDB, options: { debug: boolean; actor: string }) {
    this.db = db
    this.debug = options.debug
    this.actor = options.actor
  }

  async queryRaw(query: PrismaQuery): Promise<PrismaResultSet> {
    const adapter = new PrismaParqueDBAdapter(this.db, {
      debug: this.debug,
      actor: this.actor,
    })
    return adapter.queryRaw(query)
  }

  async executeRaw(query: PrismaQuery): Promise<number> {
    const adapter = new PrismaParqueDBAdapter(this.db, {
      debug: this.debug,
      actor: this.actor,
    })
    return adapter.executeRaw(query)
  }

  async commit(): Promise<void> {
    if (this.rolledBack) {
      throw new Error('Transaction already rolled back')
    }
    this.committed = true
    // All operations have already been executed
    // In a true transactional system, we would flush here
  }

  async rollback(): Promise<void> {
    if (this.committed) {
      throw new Error('Transaction already committed')
    }
    this.rolledBack = true
    // ParqueDB doesn't support true rollback for Parquet files
    // Log a warning
    if (this.debug) {
      logger.warn('[prisma-parquedb] Rollback called, but ParqueDB uses append-only storage')
    }
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

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

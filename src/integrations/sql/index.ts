/**
 * ParqueDB SQL Integration
 *
 * Provides SQL query support for ParqueDB with three interfaces:
 *
 * 1. **sql`` template tag** - Direct SQL queries with automatic parameter binding
 * 2. **Drizzle ORM** - Full Drizzle ORM support via proxy callback
 * 3. **Prisma** - Prisma Driver Adapter for using ParqueDB as a Prisma backend
 *
 * All three use hyparquet's query engine with full predicate pushdown:
 * - Row group skipping via min/max statistics
 * - Page-level filtering via ColumnIndex/OffsetIndex
 * - Column pruning (only read columns you need)
 * - Variant shredding for nested JSON field pushdown
 *
 * @example SQL Template Tag
 * ```typescript
 * import { createSQL } from 'parquedb/sql'
 *
 * const sql = createSQL(db)
 * const users = await sql`SELECT * FROM users WHERE status = ${'active'}`
 * ```
 *
 * @example Drizzle ORM
 * ```typescript
 * import { drizzle } from 'drizzle-orm/pg-proxy'
 * import { createDrizzleProxy } from 'parquedb/sql'
 *
 * const db = drizzle(createDrizzleProxy(parquedb))
 * const users = await db.select().from(users).where(eq(users.status, 'active'))
 * ```
 *
 * @example Prisma
 * ```typescript
 * import { PrismaClient } from '@prisma/client'
 * import { createPrismaAdapter } from 'parquedb/sql'
 *
 * const prisma = new PrismaClient({ adapter: createPrismaAdapter(parquedb) })
 * const users = await prisma.user.findMany({ where: { status: 'active' } })
 * ```
 *
 * @packageDocumentation
 */

// ============================================================================
// SQL Template Tag
// ============================================================================

export {
  createSQL,
  buildQuery,
  escapeIdentifier,
  escapeString,
  type SQLExecutor,
  type CreateSQLOptions,
} from './sql.js'

// ============================================================================
// Drizzle ORM Adapter
// ============================================================================

export {
  createDrizzleProxy,
  getTableName,
  type DrizzleProxyOptions,
} from './drizzle.js'

// ============================================================================
// Prisma Driver Adapter
// ============================================================================

export {
  PrismaParqueDBAdapter,
  createPrismaAdapter,
  type PrismaAdapterOptions,
} from './prisma.js'

// ============================================================================
// SQL Parser (for advanced use cases)
// ============================================================================

export { parseSQL } from './parser.js'

// ============================================================================
// Translator (for advanced use cases)
// ============================================================================

export {
  translateSelect,
  translateInsert,
  translateUpdate,
  translateDelete,
  translateStatement,
  translateWhere,
  whereToFilter,
} from './translator.js'

// ============================================================================
// Types
// ============================================================================

export type {
  // SQL AST Types
  SQLStatement,
  SQLSelect,
  SQLInsert,
  SQLUpdate,
  SQLDelete,
  SQLWhere,
  SQLCondition,
  SQLColumn,
  SQLValue,
  SQLOrderBy,
  SQLOperator,

  // Query Types
  SQLQueryOptions,
  SQLQueryResult,
  VariantShredConfig,
  TranslatedQuery,
  TranslatedMutation,

  // Drizzle Types
  DrizzleMethod,
  DrizzleProxyCallback,
  DrizzleProxyResult,

  // Prisma Types
  PrismaQuery,
  PrismaResultSet,
  PrismaTransaction,
  PrismaDriverAdapter,
} from './types.js'

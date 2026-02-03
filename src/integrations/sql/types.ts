/**
 * SQL Integration Types
 *
 * Types for SQL parsing, translation, and ORM adapters
 */

import type { Filter } from '../../types/filter.js'

// ============================================================================
// SQL AST Types (simplified from pgsql-ast-parser)
// ============================================================================

export type SQLOperator = '=' | '!=' | '<>' | '>' | '>=' | '<' | '<=' | 'LIKE' | 'ILIKE' | 'IN' | 'NOT IN' | 'IS' | 'IS NOT'

export interface SQLColumn {
  name: string
  table?: string
  alias?: string
}

export interface SQLValue {
  type: 'string' | 'number' | 'boolean' | 'null' | 'parameter'
  value: unknown
  paramIndex?: number
}

export interface SQLCondition {
  left: SQLColumn | SQLValue
  operator: SQLOperator
  right: SQLValue | SQLValue[]
}

export interface SQLWhere {
  type: 'and' | 'or' | 'condition'
  conditions?: SQLWhere[]
  condition?: SQLCondition
}

export interface SQLOrderBy {
  column: string
  direction: 'ASC' | 'DESC'
}

export interface SQLSelect {
  type: 'SELECT'
  columns: SQLColumn[] | '*'
  from: string
  where?: SQLWhere
  orderBy?: SQLOrderBy[]
  limit?: number
  offset?: number
}

export interface SQLInsert {
  type: 'INSERT'
  into: string
  columns: string[]
  values: SQLValue[][]
  returning?: SQLColumn[] | '*'
}

export interface SQLUpdate {
  type: 'UPDATE'
  table: string
  set: Record<string, SQLValue>
  where?: SQLWhere
  returning?: SQLColumn[] | '*'
}

export interface SQLDelete {
  type: 'DELETE'
  from: string
  where?: SQLWhere
  returning?: SQLColumn[] | '*'
}

export type SQLStatement = SQLSelect | SQLInsert | SQLUpdate | SQLDelete

// ============================================================================
// Query Options
// ============================================================================

export interface SQLQueryOptions {
  /** Parameter values for prepared statements ($1, $2, etc.) */
  params?: unknown[]
  /** Variant shredding configuration for nested field pushdown */
  variantConfig?: VariantShredConfig[]
}

export interface VariantShredConfig {
  column: string
  fields: string[]
}

// ============================================================================
// Query Result
// ============================================================================

export interface SQLQueryResult<T = Record<string, unknown>> {
  rows: T[]
  rowCount: number
  /** For INSERT/UPDATE/DELETE with RETURNING */
  command?: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE'
}

// ============================================================================
// Translation Result
// ============================================================================

export interface TranslatedQuery {
  collection: string
  filter: Filter
  columns?: string[]
  orderBy?: string
  desc?: boolean
  limit?: number
  offset?: number
}

export interface TranslatedMutation {
  collection: string
  type: 'create' | 'update' | 'delete'
  filter?: Filter
  data?: Record<string, unknown>
  returning?: string[] | '*'
}

// ============================================================================
// Drizzle Types
// ============================================================================

export type DrizzleMethod = 'all' | 'get' | 'run' | 'values'

export interface DrizzleProxyResult {
  rows: unknown[][] | unknown[]
}

export type DrizzleProxyCallback = (
  sql: string,
  params: unknown[],
  method: DrizzleMethod
) => Promise<DrizzleProxyResult>

// ============================================================================
// Prisma Types
// ============================================================================

export interface PrismaQuery {
  sql: string
  args: unknown[]
}

export interface PrismaResultSet {
  columns: string[]
  rows: unknown[][]
  columnTypes: number[]
}

export interface PrismaTransaction {
  queryRaw(query: PrismaQuery): Promise<PrismaResultSet>
  executeRaw(query: PrismaQuery): Promise<number>
  commit(): Promise<void>
  rollback(): Promise<void>
}

export interface PrismaDriverAdapter {
  readonly provider: 'sqlite' | 'postgres' | 'mysql'
  readonly adapterName: string

  queryRaw(query: PrismaQuery): Promise<PrismaResultSet>
  executeRaw(query: PrismaQuery): Promise<number>
  startTransaction(): Promise<PrismaTransaction>
}

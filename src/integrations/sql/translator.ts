/**
 * SQL to MongoDB Filter Translator
 *
 * Translates SQL AST to ParqueDB/MongoDB-style filter objects
 * that can be passed to hyparquet's parquetQuery().
 */

import type { Filter } from '../../types/filter.js'
import type {
  SQLStatement,
  SQLSelect,
  SQLInsert,
  SQLUpdate,
  SQLDelete,
  SQLWhere,
  SQLCondition,
  SQLValue,
  TranslatedQuery,
  TranslatedMutation,
} from './types.js'

// ============================================================================
// Main Translator
// ============================================================================

/**
 * Translate a SELECT statement to ParqueDB query options
 */
export function translateSelect(
  stmt: SQLSelect,
  params: unknown[] = []
): TranslatedQuery {
  const result: TranslatedQuery = {
    collection: stmt.from.toLowerCase(),
    filter: {},
  }

  // Translate WHERE to filter
  if (stmt.where) {
    result.filter = translateWhere(stmt.where, params)
  }

  // Translate columns (skip * for all columns)
  if (stmt.columns !== '*') {
    result.columns = stmt.columns.map((c) => c.alias || c.name)
  }

  // Translate ORDER BY
  if (stmt.orderBy && stmt.orderBy.length > 0) {
    const firstOrder = stmt.orderBy[0]
    if (firstOrder) {
      result.orderBy = firstOrder.column
      result.desc = firstOrder.direction === 'DESC'
    }
  }

  // Translate LIMIT/OFFSET
  if (stmt.limit !== undefined) {
    result.limit = stmt.limit
  }
  if (stmt.offset !== undefined) {
    result.offset = stmt.offset
  }

  return result
}

/**
 * Translate an INSERT statement to ParqueDB create operation
 */
export function translateInsert(
  stmt: SQLInsert,
  params: unknown[] = []
): TranslatedMutation {
  const data: Record<string, unknown> = {}

  // Map columns to values
  if (stmt.values.length > 0) {
    const values = stmt.values[0] // First row of values
    if (values) {
      for (let i = 0; i < stmt.columns.length; i++) {
        const col = stmt.columns[i]
        const val = values[i]
        if (col && val) {
          data[col] = resolveValue(val, params)
        }
      }
    }
  }

  return {
    collection: stmt.into.toLowerCase(),
    type: 'create',
    data,
    returning: stmt.returning === '*' ? '*' : stmt.returning?.map((c) => c.alias || c.name),
  }
}

/**
 * Translate an UPDATE statement to ParqueDB update operation
 */
export function translateUpdate(
  stmt: SQLUpdate,
  params: unknown[] = []
): TranslatedMutation {
  const data: Record<string, unknown> = {}

  // Translate SET clause
  for (const [col, val] of Object.entries(stmt.set)) {
    data[col] = resolveValue(val, params)
  }

  const result: TranslatedMutation = {
    collection: stmt.table.toLowerCase(),
    type: 'update',
    data,
    returning: stmt.returning === '*' ? '*' : stmt.returning?.map((c) => c.alias || c.name),
  }

  // Translate WHERE to filter
  if (stmt.where) {
    result.filter = translateWhere(stmt.where, params)
  }

  return result
}

/**
 * Translate a DELETE statement to ParqueDB delete operation
 */
export function translateDelete(
  stmt: SQLDelete,
  params: unknown[] = []
): TranslatedMutation {
  const result: TranslatedMutation = {
    collection: stmt.from.toLowerCase(),
    type: 'delete',
    returning: stmt.returning === '*' ? '*' : stmt.returning?.map((c) => c.alias || c.name),
  }

  // Translate WHERE to filter
  if (stmt.where) {
    result.filter = translateWhere(stmt.where, params)
  }

  return result
}

/**
 * Translate any SQL statement
 */
export function translateStatement(
  stmt: SQLStatement,
  params: unknown[] = []
): TranslatedQuery | TranslatedMutation {
  switch (stmt.type) {
    case 'SELECT':
      return translateSelect(stmt, params)
    case 'INSERT':
      return translateInsert(stmt, params)
    case 'UPDATE':
      return translateUpdate(stmt, params)
    case 'DELETE':
      return translateDelete(stmt, params)
    default:
      throw new Error(`Unsupported statement type: ${(stmt as SQLStatement).type}`)
  }
}

// ============================================================================
// WHERE Clause Translation
// ============================================================================

/**
 * Translate SQL WHERE clause to MongoDB-style filter
 */
export function translateWhere(where: SQLWhere, params: unknown[] = []): Filter {
  if (where.type === 'and' && where.conditions) {
    const conditions = where.conditions.map((c) => translateWhere(c, params))
    // If all conditions are simple, merge them
    if (conditions.every(isSimpleFilter)) {
      return Object.assign({}, ...conditions)
    }
    return { $and: conditions }
  }

  if (where.type === 'or' && where.conditions) {
    return { $or: where.conditions.map((c) => translateWhere(c, params)) }
  }

  if (where.type === 'condition' && where.condition) {
    return translateCondition(where.condition, params)
  }

  return {}
}

/**
 * Translate a single SQL condition to MongoDB filter
 */
function translateCondition(cond: SQLCondition, params: unknown[]): Filter {
  const column = 'name' in cond.left ? cond.left.name : String(resolveValue(cond.left as SQLValue, params))

  switch (cond.operator) {
    case '=': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: value }
    }

    case '!=':
    case '<>': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: { $ne: value } }
    }

    case '>': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: { $gt: value } }
    }

    case '>=': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: { $gte: value } }
    }

    case '<': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: { $lt: value } }
    }

    case '<=': {
      const value = resolveValue(cond.right as SQLValue, params)
      return { [column]: { $lte: value } }
    }

    case 'IN': {
      const values = (cond.right as SQLValue[]).map((v) => resolveValue(v, params))
      return { [column]: { $in: values } }
    }

    case 'NOT IN': {
      const values = (cond.right as SQLValue[]).map((v) => resolveValue(v, params))
      return { [column]: { $nin: values } }
    }

    case 'LIKE':
    case 'ILIKE': {
      const pattern = resolveValue(cond.right as SQLValue, params) as string
      const regex = likeToRegex(pattern, cond.operator === 'ILIKE')
      return { [column]: { $regex: regex } }
    }

    case 'IS': {
      // IS NULL
      return { [column]: { $exists: false } }
    }

    case 'IS NOT': {
      // IS NOT NULL
      return { [column]: { $exists: true } }
    }

    default:
      throw new Error(`Unsupported operator: ${cond.operator}`)
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Resolve a SQLValue to its actual value, substituting parameters
 */
function resolveValue(val: SQLValue, params: unknown[]): unknown {
  if (val.type === 'parameter') {
    const index = val.paramIndex ?? 0
    if (index >= 0 && index < params.length) {
      return params[index]
    }
    // For positional ? parameters, we need a counter
    // This is handled by the caller
    return params[index] ?? null
  }
  return val.value
}

/**
 * Convert SQL LIKE pattern to regex
 *
 * SQL LIKE:
 * - % matches any sequence of characters
 * - _ matches any single character
 */
function likeToRegex(pattern: string, caseInsensitive: boolean = false): string {
  // Escape regex special characters except % and _
  let regex = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/%/g, '.*')
    .replace(/_/g, '.')

  // Anchor the pattern
  regex = `^${regex}$`

  // Return as string - hyparquet will handle the regex
  return caseInsensitive ? `(?i)${regex}` : regex
}

/**
 * Check if a filter is a simple key-value object (no $and/$or)
 */
function isSimpleFilter(filter: Filter): boolean {
  return !('$and' in filter) && !('$or' in filter) && !('$nor' in filter)
}

// ============================================================================
// Utility: Create filter from WHERE string directly
// ============================================================================

import { parseSQL } from './parser.js'

/**
 * Quick helper to translate a WHERE clause string to filter
 *
 * @example
 * const filter = whereToFilter("status = $1 AND age > $2", ['active', 25])
 * // Returns: { status: 'active', age: { $gt: 25 } }
 */
export function whereToFilter(whereClause: string, params: unknown[] = []): Filter {
  // Wrap in a dummy SELECT to parse the WHERE clause
  const sql = `SELECT * FROM dummy WHERE ${whereClause}`
  const stmt = parseSQL(sql) as SQLSelect
  if (stmt.where) {
    return translateWhere(stmt.where, params)
  }
  return {}
}

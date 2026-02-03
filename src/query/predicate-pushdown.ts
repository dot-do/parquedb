/**
 * Predicate Pushdown for Typed Mode Reads
 *
 * Converts ParqueDB MongoDB-style filters to Parquet predicates that can be
 * used with hyparquet's parquetQuery() for efficient row-group and page-level filtering.
 *
 * Typed mode collections have native Parquet columns with statistics (min/max values)
 * that enable predicate pushdown - skipping entire row groups or pages that cannot
 * contain matching values.
 *
 * @example
 * ```typescript
 * import { filterToPredicates, predicatesToQueryFilter } from './predicate-pushdown'
 * import { parquetQuery } from 'hyparquet'
 *
 * const filter = { age: { $gte: 18 }, status: 'active' }
 * const predicates = filterToPredicates(filter)
 * const queryFilter = predicatesToQueryFilter(predicates)
 *
 * const rows = await parquetQuery({
 *   file: asyncBuffer,
 *   filter: queryFilter,
 *   columns: ['$id', 'age', 'status', 'data'],
 * })
 * ```
 */

import type { Filter } from '../types/filter'

// =============================================================================
// Types
// =============================================================================

/**
 * Supported predicate operations for Parquet pushdown
 *
 * These map directly to operations hyparquet can optimize with statistics:
 * - eq: Uses min/max to check if value could be in range
 * - ne: Can only prune if min === max === excluded value
 * - gt/gte/lt/lte: Range comparisons using min/max bounds
 * - in: Checks if any value in set could be in range
 */
export type PredicateOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in'

/**
 * A single Parquet predicate for pushdown
 *
 * Represents a condition on a native Parquet column that can be evaluated
 * against row-group and page-level statistics to skip data that cannot match.
 */
export interface ParquetPredicate {
  /** Column name (must be a native Parquet column, not nested in Variant) */
  column: string
  /** Comparison operator */
  op: PredicateOp
  /** Value to compare against (scalar or array for $in) */
  value: unknown
}

/**
 * hyparquet filter format for parquetQuery()
 *
 * The filter object uses column names as keys with values that are either:
 * - Direct values for equality comparison
 * - Objects with operators like { $eq, $gt, $gte, $lt, $lte, $in }
 *
 * Supports logical operators:
 * - $and: Array of conditions (all must match)
 * - $or: Array of conditions (any must match)
 * - $not: Single condition (must not match)
 */
export type HyparquetFilter = Record<string, unknown> | { $and?: HyparquetFilter[]; $or?: HyparquetFilter[] }

/**
 * Result of filter conversion with pushdown analysis
 */
export interface PredicatePushdownResult {
  /** Predicates that can be pushed down to Parquet */
  pushdownPredicates: ParquetPredicate[]
  /** Filter conditions that must be applied after reading */
  remainingFilter: Filter
  /** Whether any predicates can be pushed down */
  canPushdown: boolean
  /** Columns involved in pushdown */
  pushdownColumns: string[]
}

// =============================================================================
// Filter to Predicates Conversion
// =============================================================================

/**
 * Convert ParqueDB filter to Parquet predicates for pushdown
 *
 * Extracts comparison operators from the filter that can be evaluated against
 * Parquet column statistics. Only supports operators that hyparquet can optimize:
 * - $eq (or implicit equality)
 * - $gt, $gte, $lt, $lte
 * - $in
 *
 * Logical operators $and and $or are handled recursively when all their
 * conditions are pushable. Complex operators like $regex, $elemMatch, etc.
 * are NOT pushed down as they require row-level evaluation.
 *
 * @param filter - ParqueDB MongoDB-style filter
 * @param typedColumns - Optional set of column names that are native Parquet columns.
 *                       If provided, only these columns will be pushed down.
 *                       If not provided, all filter columns are assumed pushable.
 * @returns Array of predicates for Parquet pushdown
 *
 * @example
 * ```typescript
 * // Simple equality
 * filterToPredicates({ status: 'active' })
 * // => [{ column: 'status', op: 'eq', value: 'active' }]
 *
 * // Range query
 * filterToPredicates({ age: { $gte: 18, $lt: 65 } })
 * // => [{ column: 'age', op: 'gte', value: 18 }, { column: 'age', op: 'lt', value: 65 }]
 *
 * // $in query
 * filterToPredicates({ category: { $in: ['A', 'B', 'C'] } })
 * // => [{ column: 'category', op: 'in', value: ['A', 'B', 'C'] }]
 * ```
 */
export function filterToPredicates(
  filter: Filter,
  typedColumns?: Set<string>
): ParquetPredicate[] {
  const predicates: ParquetPredicate[] = []

  if (!filter || typeof filter !== 'object') {
    return predicates
  }

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators at this level (handled separately)
    if (key === '$and' || key === '$or' || key === '$not' || key === '$nor') {
      continue
    }

    // Skip special operators (text search, vector, geo)
    if (key === '$text' || key === '$vector' || key === '$geo') {
      continue
    }

    // Check if this is a typed column (if typedColumns provided)
    if (typedColumns && !typedColumns.has(key)) {
      continue
    }

    // Skip nested fields (dot notation) - these need Variant access
    if (key.includes('.') && !key.startsWith('$')) {
      continue
    }

    // Extract predicates from the value
    const columnPredicates = extractColumnPredicates(key, value)
    predicates.push(...columnPredicates)
  }

  // Handle $and - extract predicates from all conditions
  if (filter.$and && Array.isArray(filter.$and)) {
    for (const subFilter of filter.$and) {
      const subPredicates = filterToPredicates(subFilter, typedColumns)
      predicates.push(...subPredicates)
    }
  }

  return predicates
}

/**
 * Extract predicates for a single column from its filter value
 *
 * @param column - Column name
 * @param value - Filter value (direct value or operator object)
 * @returns Array of predicates for this column
 */
function extractColumnPredicates(column: string, value: unknown): ParquetPredicate[] {
  const predicates: ParquetPredicate[] = []

  // Handle null/undefined
  if (value === null || value === undefined) {
    // Can't push down null checks effectively with min/max stats
    return predicates
  }

  // Handle operator objects
  if (typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
    const operators = value as Record<string, unknown>

    // Check for pushable operators
    if ('$eq' in operators) {
      predicates.push({ column, op: 'eq', value: operators.$eq })
    }

    if ('$ne' in operators) {
      predicates.push({ column, op: 'ne', value: operators.$ne })
    }

    if ('$gt' in operators) {
      predicates.push({ column, op: 'gt', value: operators.$gt })
    }

    if ('$gte' in operators) {
      predicates.push({ column, op: 'gte', value: operators.$gte })
    }

    if ('$lt' in operators) {
      predicates.push({ column, op: 'lt', value: operators.$lt })
    }

    if ('$lte' in operators) {
      predicates.push({ column, op: 'lte', value: operators.$lte })
    }

    if ('$in' in operators && Array.isArray(operators.$in)) {
      predicates.push({ column, op: 'in', value: operators.$in })
    }

    // Note: $nin, $regex, $startsWith, $endsWith, $contains, $all, $elemMatch, $size
    // are NOT pushed down as they can't be evaluated with min/max statistics

    return predicates
  }

  // Direct value = implicit equality
  predicates.push({ column, op: 'eq', value })

  return predicates
}

// =============================================================================
// Predicates to hyparquet Filter
// =============================================================================

/**
 * Build hyparquet query filter from predicates
 *
 * Converts our intermediate predicate format to hyparquet's filter format
 * which uses:
 * - Direct values for equality: { column: value }
 * - Operator objects: { column: { $eq: value } }
 * - Logical $and for multiple conditions on same column
 *
 * @param predicates - Array of Parquet predicates
 * @returns Filter object for hyparquet parquetQuery()
 *
 * @example
 * ```typescript
 * predicatesToQueryFilter([
 *   { column: 'age', op: 'gte', value: 18 },
 *   { column: 'age', op: 'lt', value: 65 },
 *   { column: 'status', op: 'eq', value: 'active' },
 * ])
 * // => { age: { $gte: 18, $lt: 65 }, status: 'active' }
 * ```
 */
export function predicatesToQueryFilter(predicates: ParquetPredicate[]): HyparquetFilter {
  if (predicates.length === 0) {
    return {}
  }

  // Group predicates by column
  const byColumn = new Map<string, ParquetPredicate[]>()

  for (const pred of predicates) {
    const existing = byColumn.get(pred.column) || []
    existing.push(pred)
    byColumn.set(pred.column, existing)
  }

  // Build filter object
  const filter: Record<string, unknown> = {}

  for (const [column, preds] of byColumn) {
    if (preds.length === 1) {
      // Single predicate for this column
      const pred = preds[0]!
      filter[column] = predicateToCondition(pred)
    } else {
      // Multiple predicates for same column - combine into single object
      const conditions: Record<string, unknown> = {}

      for (const pred of preds) {
        const opKey = `$${pred.op}`
        conditions[opKey] = pred.value
      }

      filter[column] = conditions
    }
  }

  return filter
}

/**
 * Convert a single predicate to a condition value
 */
function predicateToCondition(pred: ParquetPredicate): unknown {
  // Equality can use direct value
  if (pred.op === 'eq') {
    return pred.value
  }

  // Other operators need object format
  return { [`$${pred.op}`]: pred.value }
}

// =============================================================================
// Full Filter Analysis
// =============================================================================

/**
 * Analyze filter for predicate pushdown
 *
 * Separates filter conditions into:
 * 1. Predicates that can be pushed to Parquet (use statistics)
 * 2. Remaining filter that must be applied after reading
 *
 * This enables a two-phase query:
 * 1. Use pushed predicates to skip row groups/pages
 * 2. Apply remaining filter to loaded rows
 *
 * @param filter - ParqueDB MongoDB-style filter
 * @param typedColumns - Set of column names that are native Parquet columns
 * @returns Pushdown analysis result
 *
 * @example
 * ```typescript
 * analyzeFilterForPushdown(
 *   { age: { $gte: 18 }, name: { $regex: /^John/ } },
 *   new Set(['age', 'name'])
 * )
 * // => {
 * //   pushdownPredicates: [{ column: 'age', op: 'gte', value: 18 }],
 * //   remainingFilter: { name: { $regex: /^John/ } },
 * //   canPushdown: true,
 * //   pushdownColumns: ['age']
 * // }
 * ```
 */
export function analyzeFilterForPushdown(
  filter: Filter,
  typedColumns: Set<string>
): PredicatePushdownResult {
  const predicates = filterToPredicates(filter, typedColumns)
  const pushdownColumns = [...new Set(predicates.map(p => p.column))]
  const remainingFilter = extractNonPushableFilter(filter, typedColumns)

  return {
    pushdownPredicates: predicates,
    remainingFilter,
    canPushdown: predicates.length > 0,
    pushdownColumns,
  }
}

/**
 * Extract filter conditions that cannot be pushed down
 *
 * Returns a filter containing only conditions that must be evaluated
 * after reading from Parquet (regex, string ops, array ops, etc.)
 *
 * @param filter - Original filter
 * @param typedColumns - Set of typed (pushable) columns
 * @returns Filter with non-pushable conditions
 */
export function extractNonPushableFilter(
  filter: Filter,
  typedColumns: Set<string>
): Filter {
  const remaining: Filter = {}

  if (!filter || typeof filter !== 'object') {
    return remaining
  }

  for (const [key, value] of Object.entries(filter)) {
    // Keep logical operators - they may contain non-pushable conditions
    if (key === '$and') {
      const subFilters = (value as Filter[]).map(f => extractNonPushableFilter(f, typedColumns))
      const nonEmpty = subFilters.filter(f => Object.keys(f).length > 0)
      if (nonEmpty.length > 0) {
        remaining.$and = nonEmpty
      }
      continue
    }

    if (key === '$or') {
      // $or always needs row-level evaluation for correctness
      remaining.$or = value as Filter[]
      continue
    }

    if (key === '$not') {
      remaining.$not = value as Filter
      continue
    }

    if (key === '$nor') {
      remaining.$nor = value as Filter[]
      continue
    }

    // Special operators always need row-level evaluation
    if (key === '$text' || key === '$vector' || key === '$geo') {
      (remaining as Record<string, unknown>)[key] = value
      continue
    }

    // Non-typed columns need row-level evaluation
    if (!typedColumns.has(key)) {
      (remaining as Record<string, unknown>)[key] = value
      continue
    }

    // Nested fields need row-level evaluation
    if (key.includes('.') && !key.startsWith('$')) {
      (remaining as Record<string, unknown>)[key] = value
      continue
    }

    // Check for non-pushable operators in value
    if (typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)) {
      const ops = value as Record<string, unknown>
      const nonPushableOps: Record<string, unknown> = {}

      // These operators cannot be pushed down
      const nonPushable = ['$ne', '$nin', '$regex', '$options', '$startsWith', '$endsWith',
        '$contains', '$all', '$elemMatch', '$size', '$exists', '$type']

      for (const op of nonPushable) {
        if (op in ops) {
          // $ne can be pushed but also needs row-level evaluation for correctness
          if (op === '$ne') {
            nonPushableOps[op] = ops[op]
          } else {
            nonPushableOps[op] = ops[op]
          }
        }
      }

      if (Object.keys(nonPushableOps).length > 0) {
        (remaining as Record<string, unknown>)[key] = nonPushableOps
      }
    }
  }

  return remaining
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Check if a filter can be fully pushed down (no remaining filter needed)
 *
 * @param filter - Filter to check
 * @param typedColumns - Set of typed columns
 * @returns true if entire filter can be pushed down
 */
export function canFullyPushdown(filter: Filter, typedColumns: Set<string>): boolean {
  const analysis = analyzeFilterForPushdown(filter, typedColumns)
  return Object.keys(analysis.remainingFilter).length === 0
}

/**
 * Get list of columns required for pushdown evaluation
 *
 * These columns must be read from Parquet for the pushdown to work.
 *
 * @param predicates - Predicates to analyze
 * @returns Array of column names
 */
export function getPredicateColumns(predicates: ParquetPredicate[]): string[] {
  return [...new Set(predicates.map(p => p.column))]
}

/**
 * Merge multiple predicate arrays into one
 *
 * Useful when combining predicates from multiple filter parts.
 *
 * @param predicateArrays - Arrays of predicates to merge
 * @returns Merged array of predicates
 */
export function mergePredicates(...predicateArrays: ParquetPredicate[][]): ParquetPredicate[] {
  return predicateArrays.flat()
}

/**
 * Check if a filter has any conditions that can be pushed down
 *
 * Quick check without full analysis.
 *
 * @param filter - Filter to check
 * @returns true if any conditions might be pushable
 */
export function hasPushableConditions(filter: Filter): boolean {
  if (!filter || typeof filter !== 'object') {
    return false
  }

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical and special operators
    if (key.startsWith('$')) {
      if (key === '$and' && Array.isArray(value)) {
        if (value.some(f => hasPushableConditions(f))) {
          return true
        }
      }
      continue
    }

    // Check for pushable operators
    if (typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)) {
      const ops = value as Record<string, unknown>
      if ('$eq' in ops || '$gt' in ops || '$gte' in ops || '$lt' in ops || '$lte' in ops || '$in' in ops) {
        return true
      }
    } else if (value !== null && value !== undefined) {
      // Direct value = implicit equality (pushable)
      return true
    }
  }

  return false
}

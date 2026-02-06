/**
 * Shared Filter Module -- Unified MongoDB-style filter evaluation.
 *
 * This module provides the canonical filter evaluation logic used across all
 * engine modes:
 * - TableBuffer (in-memory scans)
 * - ParqueEngine (JSONL + buffer reads)
 * - DOReadPath (R2 Parquet + SQLite WAL merge-on-read)
 *
 * Supported operators:
 * - Comparison: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex
 * - Logical: $or, $and, $not
 * - Implicit $eq (shorthand like { name: 'Alice' })
 * - Dot-notation for nested field paths (e.g., 'address.city')
 *
 * All field conditions within a filter are ANDed together.
 */

// =============================================================================
// Types
// =============================================================================

/** Comparison operators supported in filters */
export interface ComparisonFilter {
  $eq?: unknown
  $ne?: unknown
  $gt?: number | string
  $gte?: number | string
  $lt?: number | string
  $lte?: number | string
  $in?: unknown[]
  $nin?: unknown[]
  $exists?: boolean
  $regex?: string | RegExp
  $not?: ComparisonFilter
}

// =============================================================================
// Nested value resolution
// =============================================================================

/**
 * Resolve a dot-notation path on an object.
 * e.g. getNestedValue({ address: { city: 'NYC' } }, 'address.city') => 'NYC'
 *
 * Returns undefined if any intermediate segment is null, undefined, or not an object.
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

// =============================================================================
// Internal helpers
// =============================================================================

/**
 * Set of all comparison operator keys.
 * Used to distinguish operator objects from plain value objects.
 */
const COMPARISON_OPERATORS = new Set([
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte',
  '$in', '$nin', '$exists', '$regex', '$not',
])

/**
 * Check whether a value is a comparison filter object (has operator keys).
 */
function isComparisonFilter(value: unknown): value is ComparisonFilter {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false
  }
  // RegExp objects are not filter objects
  if (value instanceof RegExp) {
    return false
  }
  const keys = Object.keys(value as object)
  return keys.length > 0 && keys.every(k => COMPARISON_OPERATORS.has(k))
}

/**
 * Evaluate a comparison filter (without $not) against a value.
 * All operators in the filter must match (implicit AND).
 */
function evaluateComparison(value: unknown, filter: ComparisonFilter): boolean {
  if ('$eq' in filter && value !== filter.$eq) return false
  if ('$ne' in filter && value === filter.$ne) return false

  if ('$gt' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) > (filter.$gt as number))) return false
  }
  if ('$gte' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) >= (filter.$gte as number))) return false
  }
  if ('$lt' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) < (filter.$lt as number))) return false
  }
  if ('$lte' in filter) {
    if (value === undefined || value === null) return false
    if (!((value as number) <= (filter.$lte as number))) return false
  }

  if ('$in' in filter) {
    if (!Array.isArray(filter.$in)) return false
    if (!filter.$in.includes(value)) return false
  }

  if ('$nin' in filter) {
    if (!Array.isArray(filter.$nin)) return false
    if (filter.$nin.includes(value)) return false
  }

  if ('$exists' in filter) {
    const exists = value !== undefined
    if (filter.$exists && !exists) return false
    if (!filter.$exists && exists) return false
  }

  if ('$regex' in filter) {
    if (typeof value !== 'string') return false
    // Pre-compile string patterns to RegExp on first use, then cache on the
    // filter object so subsequent per-entity evaluations skip recompilation.
    if (!(filter.$regex instanceof RegExp)) {
      ;(filter as { $regex: RegExp }).$regex = new RegExp(filter.$regex as string)
    }
    if (!filter.$regex.test(value)) return false
  }

  return true
}

/**
 * Evaluate a single field condition against an entity value.
 * Handles both operator objects and implicit $eq (direct equality).
 */
function matchFieldCondition(entityValue: unknown, condition: unknown): boolean {
  if (isComparisonFilter(condition)) {
    return matchComparisonEval(entityValue, condition)
  }
  // Otherwise it's a direct equality check (implicit $eq)
  return entityValue === condition
}

/**
 * Full comparison filter evaluation including $not support.
 *
 * When $not is present, it inverts the result of its sub-filter.
 * Other operators alongside $not are evaluated normally (all must pass).
 */
function matchComparisonEval(value: unknown, filter: ComparisonFilter): boolean {
  if ('$not' in filter) {
    const { $not: notFilter, ...rest } = filter
    // If there are other operators alongside $not, they all must pass
    if (Object.keys(rest).length > 0) {
      if (!evaluateComparison(value, rest as ComparisonFilter)) return false
    }
    // $not inverts: if the inner filter matches, $not does NOT match
    if (evaluateComparison(value, notFilter!)) return false
    return true
  }

  return evaluateComparison(value, filter)
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Check whether an entity matches all conditions in a filter.
 *
 * The filter is a Record where:
 * - Non-$ keys are field paths (supporting dot notation for nesting)
 * - $or: array of sub-filters, at least one must match
 * - $and: array of sub-filters, all must match
 * - Field values can be literals (implicit $eq) or operator objects
 *
 * All top-level conditions are ANDed together.
 */
export function matchesFilter(
  entity: Record<string, unknown>,
  filter: Record<string, unknown>,
): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    if (key === '$or') {
      const subFilters = condition as Record<string, unknown>[]
      if (!Array.isArray(subFilters) || subFilters.length === 0) return false
      const orMatch = subFilters.some(sub =>
        matchesFilter(entity, sub),
      )
      if (!orMatch) return false
    } else if (key === '$and') {
      const subFilters = condition as Record<string, unknown>[]
      if (!Array.isArray(subFilters)) return false
      // Empty $and is vacuously true
      const andMatch = subFilters.every(sub =>
        matchesFilter(entity, sub),
      )
      if (!andMatch) return false
    } else {
      // Regular field condition
      const value = getNestedValue(entity, key)
      if (!matchFieldCondition(value, condition)) return false
    }
  }
  return true
}

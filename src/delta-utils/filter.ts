/**
 * Shared Filter Types and Matching
 *
 * MongoDB-style filter operators that can be shared between
 * ParqueDB and Delta Lake for query operations.
 *
 * Features:
 * - Comparison operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin)
 * - Logical operators ($and, $or, $not, $nor)
 * - Existence operators ($exists)
 * - String operators ($regex)
 * - Parquet zone map integration for predicate pushdown
 */

import { createSafeRegex } from '../utils/safe-regex'

// =============================================================================
// FILTER TYPES
// =============================================================================

/**
 * Filter for a specific type T
 */
export type Filter<T = unknown> = {
  [K in keyof T]?: T[K] | ComparisonOperators<T[K]>
} & LogicalOperators<T>

/**
 * Comparison operators
 */
export interface ComparisonOperators<T> {
  $eq?: T
  $ne?: T
  $gt?: T
  $gte?: T
  $lt?: T
  $lte?: T
  $in?: T[]
  $nin?: T[]
  $exists?: boolean
  $type?: string
  $regex?: string | RegExp
}

/**
 * Logical operators
 */
export interface LogicalOperators<T> {
  $and?: Filter<T>[]
  $or?: Filter<T>[]
  $not?: Filter<T>
  $nor?: Filter<T>[]
}

// =============================================================================
// ZONE MAP TYPES (for Parquet predicate pushdown)
// =============================================================================

/**
 * Zone map statistics for a column in a row group
 */
export interface ZoneMap {
  column: string
  min: unknown
  max: unknown
  nullCount: number
}

/**
 * Filter condition for zone map pruning
 */
export interface ZoneMapFilter {
  column: string
  operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'between'
  value: unknown
  value2?: unknown // For 'between'
}

// =============================================================================
// FILTER MATCHING
// =============================================================================

/**
 * Check if a document matches a filter
 */
export function matchesFilter<T extends Record<string, unknown>>(
  doc: T,
  filter: Filter<T>
): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and') {
      if (!Array.isArray(condition)) return false
      if (!condition.every(f => matchesFilter(doc, f))) return false
      continue
    }
    if (key === '$or') {
      if (!Array.isArray(condition)) return false
      if (!condition.some(f => matchesFilter(doc, f))) return false
      continue
    }
    if (key === '$not') {
      if (matchesFilter(doc, condition as Filter<T>)) return false
      continue
    }
    if (key === '$nor') {
      if (!Array.isArray(condition)) return false
      if (condition.some(f => matchesFilter(doc, f))) return false
      continue
    }

    // Get document value (supports nested paths like 'user.name')
    const docValue = getNestedValue(doc, key)

    // Handle comparison operators
    if (isComparisonObject(condition)) {
      if (!matchesComparisonOperators(docValue, condition)) return false
    } else {
      // Direct equality
      if (docValue !== condition) return false
    }
  }

  return true
}

/**
 * Get a nested value from an object using dot notation
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let value: unknown = obj
  for (const part of parts) {
    if (value == null || typeof value !== 'object') return undefined
    value = (value as Record<string, unknown>)[part]
  }
  return value
}

/**
 * Check if a value is a comparison operator object
 */
export function isComparisonObject(value: unknown): value is ComparisonOperators<unknown> {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.some(k => k.startsWith('$'))
}

/**
 * Match a value against comparison operators
 */
function matchesComparisonOperators<T>(
  docValue: unknown,
  operators: ComparisonOperators<T>
): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    switch (op) {
      case '$eq':
        if (docValue !== opValue) return false
        break
      case '$ne':
        if (docValue === opValue) return false
        break
      case '$gt':
        if (docValue == null || !isComparable(docValue) || !isComparable(opValue) || docValue <= opValue) return false
        break
      case '$gte':
        if (docValue == null || !isComparable(docValue) || !isComparable(opValue) || docValue < opValue) return false
        break
      case '$lt':
        if (docValue == null || !isComparable(docValue) || !isComparable(opValue) || docValue >= opValue) return false
        break
      case '$lte':
        if (docValue == null || !isComparable(docValue) || !isComparable(opValue) || docValue > opValue) return false
        break
      case '$in':
        if (!Array.isArray(opValue) || !opValue.includes(docValue)) return false
        break
      case '$nin':
        if (Array.isArray(opValue) && opValue.includes(docValue)) return false
        break
      case '$exists':
        if (opValue && docValue === undefined) return false
        if (!opValue && docValue !== undefined) return false
        break
      case '$regex': {
        if (typeof docValue !== 'string') return false
        const regex = createSafeRegex(opValue as string | RegExp)
        if (!regex.test(docValue)) return false
        break
      }
    }
  }
  return true
}

/**
 * Type guard for comparable values (number, string, Date)
 */
function isComparable(v: unknown): v is number | string | Date {
  return typeof v === 'number' || typeof v === 'string' || v instanceof Date
}

// =============================================================================
// ZONE MAP PRUNING
// =============================================================================

/**
 * Check if a zone map can be skipped based on filter.
 * Returns true if the entire row group can be skipped (no matching rows).
 */
export function canSkipZoneMap(zoneMap: ZoneMap, filter: ZoneMapFilter): boolean {
  const { min, max } = zoneMap
  const { operator, value, value2 } = filter

  // Type guard for comparable values
  const isZoneMapComparable = (v: unknown): v is number | string | Date =>
    typeof v === 'number' || typeof v === 'string' || v instanceof Date

  switch (operator) {
    case 'eq':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return value < min || value > max
    case 'ne':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return min === max && min === value
    case 'gt':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return max <= value
    case 'gte':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return max < value
    case 'lt':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return min >= value
    case 'lte':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value)) {
        return false
      }
      return min > value
    case 'in':
      if (!Array.isArray(value)) return false
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max)) return false
      // Skip if ALL values in the array are outside the range
      return value.every(v => isZoneMapComparable(v) && (v < min || v > max))
    case 'between':
      if (!isZoneMapComparable(min) || !isZoneMapComparable(max) || !isZoneMapComparable(value) || !isZoneMapComparable(value2)) {
        return false
      }
      return max < value || min > value2
    default:
      return false
  }
}

// =============================================================================
// PARQUET PREDICATE TRANSLATION
// =============================================================================

/**
 * Convert a MongoDB-style filter to Parquet zone map filters
 */
export function filterToZoneMapPredicates<T>(
  filter: Filter<T>
): ZoneMapFilter[] {
  const predicates: ZoneMapFilter[] = []

  for (const [key, condition] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and') {
      // Process $and by recursively converting each subfilter
      if (Array.isArray(condition)) {
        for (const subFilter of condition) {
          predicates.push(...filterToZoneMapPredicates(subFilter))
        }
      }
      continue
    }

    // Skip other logical operators (not pushable to zone maps)
    if (key.startsWith('$')) continue

    if (isComparisonObject(condition)) {
      // Normal processing - convert each operator
      for (const [op, value] of Object.entries(condition)) {
        const zoneMapOp = mongoOpToZoneMapOp(op)
        if (zoneMapOp) {
          predicates.push({
            column: key,
            operator: zoneMapOp,
            value,
          })
        }
      }

      // Check for range query optimization ($gte and $lte on same field)
      const hasGte = '$gte' in condition
      const hasLte = '$lte' in condition
      if (hasGte && hasLte) {
        predicates.push({
          column: key,
          operator: 'between',
          value: condition.$gte,
          value2: condition.$lte,
        })
      }
    } else {
      // Direct equality
      predicates.push({
        column: key,
        operator: 'eq',
        value: condition,
      })
    }
  }

  return predicates
}

/**
 * Map MongoDB operator to zone map operator
 */
function mongoOpToZoneMapOp(op: string): ZoneMapFilter['operator'] | null {
  switch (op) {
    case '$eq': return 'eq'
    case '$ne': return 'ne'
    case '$gt': return 'gt'
    case '$gte': return 'gte'
    case '$lt': return 'lt'
    case '$lte': return 'lte'
    case '$in': return 'in'
    default: return null
  }
}

/**
 * Filter Evaluation for ParqueDB
 *
 * Provides in-memory filter evaluation for MongoDB-style queries.
 * This module is used for post-pushdown filtering of rows.
 */

import type { Filter } from '../types/filter'

// =============================================================================
// Main Filter Evaluation
// =============================================================================

/**
 * Check if a row matches a filter
 *
 * @param row - The row to check
 * @param filter - MongoDB-style filter
 * @returns true if the row matches the filter
 */
export function matchesFilter(row: unknown, filter: Filter): boolean {
  if (!filter || Object.keys(filter).length === 0) {
    return true
  }

  if (row === null || row === undefined) {
    return false
  }

  const obj = row as Record<string, unknown>

  // Handle logical operators
  if (filter.$and) {
    return filter.$and.every(subFilter => matchesFilter(row, subFilter))
  }

  if (filter.$or) {
    return filter.$or.some(subFilter => matchesFilter(row, subFilter))
  }

  if (filter.$not) {
    return !matchesFilter(row, filter.$not)
  }

  if (filter.$nor) {
    return !filter.$nor.some(subFilter => matchesFilter(row, subFilter))
  }

  // Handle special operators
  if (filter.$text || filter.$vector || filter.$geo) {
    return true // These need specialized handling
  }

  // Check each field filter
  for (const [field, condition] of Object.entries(filter)) {
    if (field.startsWith('$')) continue

    const fieldValue = getNestedValue(obj, field)

    if (!matchesCondition(fieldValue, condition)) {
      return false
    }
  }

  return true
}

/**
 * Create a predicate function from a filter
 *
 * @param filter - MongoDB-style filter
 * @returns Predicate function
 */
export function createPredicate(filter: Filter): (row: unknown) => boolean {
  return (row: unknown) => matchesFilter(row, filter)
}

/**
 * Check if a value matches a condition
 *
 * @param value - The value to check
 * @param condition - The condition to match against
 * @returns true if the value matches
 */
export function matchesCondition(value: unknown, condition: unknown): boolean {
  // Null condition
  if (condition === null) {
    return value === null || value === undefined
  }

  if (condition === undefined) {
    return true
  }

  // Operator object
  if (isOperatorObject(condition)) {
    return evaluateOperators(value, condition as Record<string, unknown>)
  }

  // Direct equality
  return deepEqual(value, condition)
}

// =============================================================================
// Value Comparison
// =============================================================================

/**
 * Deep equality check
 *
 * @param a - First value
 * @param b - Second value
 * @returns true if values are deeply equal
 */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || a === undefined) return b === null || b === undefined

  // Date comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime()
  }

  // Array comparison
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((v, i) => deepEqual(v, b[i]))
  }

  // Object comparison
  if (typeof a === 'object' && typeof b === 'object') {
    const aObj = a as Record<string, unknown>
    const bObj = b as Record<string, unknown>
    const aKeys = Object.keys(aObj)
    const bKeys = Object.keys(bObj)
    if (aKeys.length !== bKeys.length) return false
    return aKeys.every(k => deepEqual(aObj[k], bObj[k]))
  }

  return false
}

/**
 * Compare two values for ordering
 *
 * @param a - First value
 * @param b - Second value
 * @returns -1 if a < b, 0 if equal, 1 if a > b
 */
export function compareValues(a: unknown, b: unknown): number {
  // Nulls sort first
  if (a === null || a === undefined) {
    return b === null || b === undefined ? 0 : -1
  }
  if (b === null || b === undefined) return 1

  // Same type comparisons
  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b)
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
  if (typeof a === 'boolean' && typeof b === 'boolean') return (a ? 1 : 0) - (b ? 1 : 0)

  // Fallback to string comparison
  return String(a).localeCompare(String(b))
}

/**
 * Get the type of a value (matching MongoDB's $type operator)
 *
 * @param value - Value to check
 * @returns Type string
 */
export function getValueType(value: unknown): string {
  if (value === null) return 'null'
  if (value === undefined) return 'null'
  if (Array.isArray(value)) return 'array'
  if (value instanceof Date) return 'date'
  return typeof value
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get a nested value from an object using dot notation
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
 * Check if value is an operator object
 */
function isOperatorObject(value: unknown): boolean {
  if (typeof value !== 'object' || value === null || Array.isArray(value) || value instanceof Date) {
    return false
  }
  const keys = Object.keys(value as object)
  return keys.some(k => k.startsWith('$'))
}

/**
 * Evaluate operator conditions
 */
function evaluateOperators(value: unknown, operators: Record<string, unknown>): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    switch (op) {
      case '$eq':
        if (!deepEqual(value, opValue)) return false
        break

      case '$ne':
        if (deepEqual(value, opValue)) return false
        break

      case '$gt':
        if (value === null || value === undefined) return false
        if (compareValues(value, opValue) <= 0) return false
        break

      case '$gte':
        if (value === null || value === undefined) return false
        if (compareValues(value, opValue) < 0) return false
        break

      case '$lt':
        if (value === null || value === undefined) return false
        if (compareValues(value, opValue) >= 0) return false
        break

      case '$lte':
        if (value === null || value === undefined) return false
        if (compareValues(value, opValue) > 0) return false
        break

      case '$in':
        if (!Array.isArray(opValue)) return false
        if (!opValue.some(v => deepEqual(value, v))) return false
        break

      case '$nin':
        if (!Array.isArray(opValue)) return false
        if (opValue.some(v => deepEqual(value, v))) return false
        break

      case '$regex': {
        if (typeof value !== 'string') return false
        const pattern = opValue instanceof RegExp
          ? opValue
          : new RegExp(opValue as string, (operators.$options as string) || '')
        if (!pattern.test(value)) return false
        break
      }

      case '$options':
        break // Handled with $regex

      case '$startsWith':
        if (typeof value !== 'string') return false
        if (!value.startsWith(opValue as string)) return false
        break

      case '$endsWith':
        if (typeof value !== 'string') return false
        if (!value.endsWith(opValue as string)) return false
        break

      case '$contains':
        if (typeof value !== 'string') return false
        if (!value.includes(opValue as string)) return false
        break

      case '$all': {
        if (!Array.isArray(value)) return false
        const required = opValue as unknown[]
        if (!required.every(v => value.some(fv => deepEqual(fv, v)))) return false
        break
      }

      case '$elemMatch': {
        if (!Array.isArray(value)) return false
        const subFilter = opValue as Filter
        if (!value.some(elem => matchesFilter(elem, subFilter))) return false
        break
      }

      case '$size':
        if (!Array.isArray(value)) return false
        if (value.length !== opValue) return false
        break

      case '$exists':
        if (opValue === true) {
          if (value === undefined) return false
        } else {
          if (value !== undefined) return false
        }
        break

      case '$type': {
        const actualType = getValueType(value)
        if (actualType !== opValue) return false
        break
      }

      default:
        // Unknown operator - ignore
        break
    }
  }

  return true
}

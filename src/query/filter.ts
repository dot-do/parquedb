/**
 * Filter Evaluation for ParqueDB
 *
 * Provides in-memory filter evaluation for MongoDB-style queries.
 * This module is used for post-pushdown filtering of rows.
 *
 * ## Null vs Undefined Handling
 *
 * ParqueDB follows MongoDB's conventions for null/undefined handling in filters:
 *
 * ### Equality Operators ($eq, $ne, $in, $nin)
 * - null and undefined are treated as equivalent for equality comparisons
 * - `{ field: null }` matches documents where field is null OR missing (undefined)
 * - `{ field: { $eq: null } }` also matches null or missing fields
 * - `{ field: { $in: [null] } }` matches null or missing fields
 *
 * ### Comparison Operators ($gt, $gte, $lt, $lte)
 * - null and undefined values always return false for comparison operators
 * - You cannot meaningfully compare null/undefined with other values
 * - `{ field: { $gt: 0 } }` returns false if field is null or missing
 *
 * ### Existence Operator ($exists)
 * - `{ field: { $exists: true } }` matches if field is present, even if null
 * - `{ field: { $exists: false } }` matches only if field is missing (undefined)
 * - This is the key distinction: $exists checks for undefined, not null
 *
 * ### Type Operator ($type)
 * - Both null and undefined have type 'null'
 * - `{ field: { $type: 'null' } }` matches null or missing fields
 *
 * ### Sorting (compareValues)
 * - null and undefined are treated as equivalent for sorting
 * - Both sort before all other values (nulls first)
 */

import type { Filter } from '../types/filter'
import { deepEqual, compareValues, getNestedValue, getValueType, createSafeRegex } from '../utils'

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

  // Handle primitives - treat filter as direct operators against the value
  // This is used by $pull operator when filtering primitive values in arrays
  if (typeof row !== 'object') {
    // For primitives, interpret the filter as operators against the value directly
    for (const [op, opValue] of Object.entries(filter)) {
      if (!op.startsWith('$')) {
        // Non-operator field - primitives don't have fields, so no match
        return false
      }
      switch (op) {
        case '$eq':
          if (!deepEqual(row, opValue)) return false
          break
        case '$ne':
          if (deepEqual(row, opValue)) return false
          break
        case '$gt':
          if (compareValues(row, opValue) <= 0) return false
          break
        case '$gte':
          if (compareValues(row, opValue) < 0) return false
          break
        case '$lt':
          if (compareValues(row, opValue) >= 0) return false
          break
        case '$lte':
          if (compareValues(row, opValue) > 0) return false
          break
        case '$in':
          if (!Array.isArray(opValue)) return false
          if (!opValue.some(v => deepEqual(row, v))) return false
          break
        case '$nin':
          if (!Array.isArray(opValue)) return false
          if (opValue.some(v => deepEqual(row, v))) return false
          break
        default:
          // Unknown operator for primitives - ignore
          break
      }
    }
    return true
  }

  const obj = row as Record<string, unknown>

  // Handle logical operators - these must be combined with field conditions
  // First check $and if present
  if (filter.$and) {
    if (!filter.$and.every(subFilter => matchesFilter(row, subFilter))) {
      return false
    }
  }

  // Check $or if present
  if (filter.$or) {
    if (!filter.$or.some(subFilter => matchesFilter(row, subFilter))) {
      return false
    }
  }

  // Check $not if present
  if (filter.$not) {
    if (matchesFilter(row, filter.$not)) {
      return false
    }
  }

  // Check $nor if present
  if (filter.$nor) {
    if (filter.$nor.some(subFilter => matchesFilter(row, subFilter))) {
      return false
    }
  }

  // Handle special operators - skip for now (these need specialized handling)
  // $text, $vector, $geo are handled by specialized index queries

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
 * **Null/Undefined Behavior:**
 * - If condition is null: matches if value is null OR undefined (equivalent for equality)
 * - If condition is undefined: always returns true (no condition specified)
 * - For equality comparisons via deepEqual: null and undefined are treated as equivalent
 *
 * @param value - The value to check
 * @param condition - The condition to match against
 * @returns true if the value matches
 */
export function matchesCondition(value: unknown, condition: unknown): boolean {
  // Null condition - matches both null and undefined (MongoDB behavior)
  if (condition === null) {
    return value === null || value === undefined
  }

  // Undefined condition - no condition specified, always matches
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
// Helper Functions
// =============================================================================

// deepEqual, compareValues, getNestedValue, and getValueType are imported from ../utils

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

      case '$gt': {
        if (value === null || value === undefined) return false
        const cmp = compareValues(value, opValue)
        if (Number.isNaN(cmp) || cmp <= 0) return false
        break
      }

      case '$gte': {
        if (value === null || value === undefined) return false
        const cmp = compareValues(value, opValue)
        if (Number.isNaN(cmp) || cmp < 0) return false
        break
      }

      case '$lt': {
        if (value === null || value === undefined) return false
        const cmp = compareValues(value, opValue)
        if (Number.isNaN(cmp) || cmp >= 0) return false
        break
      }

      case '$lte': {
        if (value === null || value === undefined) return false
        const cmp = compareValues(value, opValue)
        if (Number.isNaN(cmp) || cmp > 0) return false
        break
      }

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
        // Only pass flags if $options is explicitly provided, otherwise let RegExp keep its own flags
        const flags = operators.$options !== undefined ? (operators.$options as string) : undefined
        const pattern = createSafeRegex(opValue as string | RegExp, flags)
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

// Re-export utility functions for backwards compatibility
export { deepEqual, compareValues, getValueType } from '../utils'

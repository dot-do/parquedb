/**
 * Filter Evaluation for ParqueDB
 *
 * Provides in-memory filter evaluation for MongoDB-style queries.
 * This module is used for post-pushdown filtering of rows.
 */

import type { Filter } from '../types/filter'
import { deepEqual, compareValues, getNestedValue, getValueType } from '../utils'

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

// Re-export utility functions for backwards compatibility
export { deepEqual, compareValues, getValueType } from '../utils'

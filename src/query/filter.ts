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
import { deepEqual, compareValues, getNestedValue, getValueType, createSafeRegex, UnsafeRegexError, safeRegexTest, logger, isNullish } from '../utils'

// =============================================================================
// Configuration
// =============================================================================

/**
 * Configuration for filter evaluation behavior
 */
export interface FilterConfig {
  /**
   * Behavior when an unknown operator is encountered:
   * - 'ignore': Silently ignore unknown operators (default, backward compatible)
   * - 'warn': Log a warning to console
   * - 'error': Throw an error
   */
  unknownOperatorBehavior?: 'ignore' | 'warn' | 'error' | undefined
}

/**
 * Default filter configuration
 * This is the recommended way to use configuration - pass it explicitly to filter functions
 */
export const DEFAULT_FILTER_CONFIG: Readonly<FilterConfig> = Object.freeze({
  unknownOperatorBehavior: 'ignore',
})


/**
 * Known operators for validation
 */
const KNOWN_OPERATORS = new Set([
  // Comparison operators
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin', '$mod',
  // String operators
  '$regex', '$options', '$startsWith', '$endsWith', '$contains',
  // Array operators
  '$all', '$elemMatch', '$size',
  // Existence operators
  '$exists', '$type',
  // Logical operators (top-level and field-level)
  '$and', '$or', '$not', '$nor',
  // Special operators (handled elsewhere)
  '$text', '$vector', '$geo', '$expr', '$comment',
])

/**
 * Handle unknown operator
 *
 * Uses the explicit config parameter if provided, otherwise falls back to DEFAULT_FILTER_CONFIG.
 * There is no global mutable state - configuration must be passed explicitly for non-default behavior.
 */
function handleUnknownOperator(operator: string, config?: FilterConfig): void {
  const behavior = config?.unknownOperatorBehavior
    ?? DEFAULT_FILTER_CONFIG.unknownOperatorBehavior
    ?? 'ignore'

  if (behavior === 'ignore') {
    return
  }

  const message = `Unknown query operator: ${operator}`

  if (behavior === 'warn') {
    logger.warn(message)
  } else if (behavior === 'error') {
    throw new Error(message)
  }
}

// =============================================================================
// Main Filter Evaluation
// =============================================================================

/**
 * Check if a row matches a filter
 *
 * @param row - The row to check
 * @param filter - MongoDB-style filter
 * @param config - Optional filter configuration
 * @returns true if the row matches the filter
 */
export function matchesFilter(row: unknown, filter: Filter, config?: FilterConfig): boolean {
  if (!filter || Object.keys(filter).length === 0) {
    return true
  }

  if (isNullish(row)) {
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

      // Use shared comparison operator evaluation
      const result = evaluateComparisonOperator(row, op, opValue)
      if (result !== undefined) {
        if (!result) return false
        continue
      }

      // Unknown operator for primitives
      if (!KNOWN_OPERATORS.has(op)) {
        handleUnknownOperator(op, config)
      }
    }
    return true
  }

  const obj = row as Record<string, unknown>

  // Handle logical operators - these must be combined with field conditions
  // First check $and if present
  if (filter.$and) {
    if (!filter.$and.every(subFilter => matchesFilter(row, subFilter, config))) {
      return false
    }
  }

  // Check $or if present
  if (filter.$or) {
    if (!filter.$or.some(subFilter => matchesFilter(row, subFilter, config))) {
      return false
    }
  }

  // Check $not if present
  if (filter.$not) {
    if (matchesFilter(row, filter.$not, config)) {
      return false
    }
  }

  // Check $nor if present
  if (filter.$nor) {
    if (filter.$nor.some(subFilter => matchesFilter(row, subFilter, config))) {
      return false
    }
  }

  // Handle $expr - expression evaluation comparing fields within document
  if (filter.$expr) {
    if (typeof filter.$expr !== 'object' || filter.$expr === null) {
      return false // Invalid $expr format
    }
    if (!evaluateExpr(obj, filter.$expr as Record<string, unknown>)) {
      return false
    }
  }

  // Handle $comment - no effect on matching, purely for logging/debugging
  // (no code needed - it's simply ignored)

  // Handle special operators - skip for now (these need specialized handling)
  // $text, $vector, $geo are handled by specialized index queries

  // Check each field filter
  for (const [field, condition] of Object.entries(filter)) {
    if (field.startsWith('$')) continue

    const fieldValue = getNestedValue(obj, field)

    if (!matchesCondition(fieldValue, condition, config)) {
      return false
    }
  }

  return true
}

/**
 * Create a predicate function from a filter
 *
 * @param filter - MongoDB-style filter
 * @param config - Optional filter configuration
 * @returns Predicate function
 */
export function createPredicate(filter: Filter, config?: FilterConfig): (row: unknown) => boolean {
  return (row: unknown) => matchesFilter(row, filter, config)
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
 * @param config - Optional filter configuration
 * @returns true if the value matches
 */
export function matchesCondition(value: unknown, condition: unknown, config?: FilterConfig): boolean {
  // Null condition - matches both null and undefined (MongoDB behavior)
  if (condition === null) {
    return isNullish(value)
  }

  // Undefined condition - no condition specified, always matches
  if (condition === undefined) {
    return true
  }

  // Operator object
  if (isOperatorObject(condition)) {
    return evaluateOperators(value, condition as Record<string, unknown>, config)
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
 * Evaluate a single comparison operator against a value
 *
 * This is the core evaluation logic shared between:
 * - Primitive value matching in matchesFilter
 * - Object field matching in evaluateOperators
 *
 * @param value - The value to test
 * @param op - The operator (e.g., '$eq', '$gt')
 * @param opValue - The operand value
 * @returns true if matches, false if doesn't match, undefined if unknown operator
 */
function evaluateComparisonOperator(value: unknown, op: string, opValue: unknown): boolean | undefined {
  switch (op) {
    case '$eq':
      return deepEqual(value, opValue)

    case '$ne':
      return !deepEqual(value, opValue)

    case '$gt': {
      if (isNullish(value)) return false
      const cmp = compareValues(value, opValue)
      return !Number.isNaN(cmp) && cmp > 0
    }

    case '$gte': {
      if (isNullish(value)) return false
      const cmp = compareValues(value, opValue)
      return !Number.isNaN(cmp) && cmp >= 0
    }

    case '$lt': {
      if (isNullish(value)) return false
      const cmp = compareValues(value, opValue)
      return !Number.isNaN(cmp) && cmp < 0
    }

    case '$lte': {
      if (isNullish(value)) return false
      const cmp = compareValues(value, opValue)
      return !Number.isNaN(cmp) && cmp <= 0
    }

    case '$in':
      if (!Array.isArray(opValue)) return false
      return opValue.some(v => deepEqual(value, v))

    case '$nin':
      if (!Array.isArray(opValue)) return false
      return !opValue.some(v => deepEqual(value, v))

    case '$mod': {
      // $mod: [divisor, remainder] - matches when value % divisor === remainder
      if (typeof value !== 'number' || isNullish(value)) return false
      if (!Array.isArray(opValue) || opValue.length !== 2) return false
      const [divisor, remainder] = opValue as [number, number]
      if (typeof divisor !== 'number' || typeof remainder !== 'number') return false
      if (divisor === 0) return false // Avoid division by zero
      return value % divisor === remainder
    }

    default:
      return undefined // Unknown operator - caller should handle
  }
}

/**
 * Evaluate $expr expression - compares fields within the same document
 *
 * @param obj - The document object
 * @param expr - The expression object containing comparison operators
 * @returns true if expression evaluates to true
 */
function evaluateExpr(obj: Record<string, unknown>, expr: Record<string, unknown>): boolean {
  // Resolve a value - if it starts with $, it's a field reference
  const resolveValue = (val: unknown): unknown => {
    if (typeof val === 'string' && val.startsWith('$')) {
      const fieldPath = val.slice(1) // Remove leading $
      return getNestedValue(obj, fieldPath)
    }
    return val
  }

  // Check each comparison operator in the expression
  for (const [op, args] of Object.entries(expr)) {
    if (!Array.isArray(args) || args.length !== 2) {
      return false // Invalid expression format
    }

    const [left, right] = args
    const leftVal = resolveValue(left)
    const rightVal = resolveValue(right)

    switch (op) {
      case '$eq':
        if (!deepEqual(leftVal, rightVal)) return false
        break

      case '$ne':
        if (deepEqual(leftVal, rightVal)) return false
        break

      case '$gt': {
        const cmp = compareValues(leftVal, rightVal)
        if (Number.isNaN(cmp) || cmp <= 0) return false
        break
      }

      case '$gte': {
        const cmp = compareValues(leftVal, rightVal)
        if (Number.isNaN(cmp) || cmp < 0) return false
        break
      }

      case '$lt': {
        const cmp = compareValues(leftVal, rightVal)
        if (Number.isNaN(cmp) || cmp >= 0) return false
        break
      }

      case '$lte': {
        const cmp = compareValues(leftVal, rightVal)
        if (Number.isNaN(cmp) || cmp > 0) return false
        break
      }

      default:
        // Unknown expression operator - return false
        return false
    }
  }

  return true
}

/**
 * Evaluate operator conditions
 */
function evaluateOperators(value: unknown, operators: Record<string, unknown>, config?: FilterConfig): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    // First, try the shared comparison operator evaluation
    const comparisonResult = evaluateComparisonOperator(value, op, opValue)
    if (comparisonResult !== undefined) {
      if (!comparisonResult) return false
      continue
    }

    // Handle non-comparison operators
    switch (op) {
      case '$not': {
        // Field-level $not: negate the result of evaluating the inner operators
        if (typeof opValue !== 'object' || opValue === null) return false
        const innerResult = evaluateOperators(value, opValue as Record<string, unknown>, config)
        if (innerResult) return false  // If inner matches, $not should NOT match
        break
      }

      case '$regex': {
        if (typeof value !== 'string') return false
        // Validate opValue type
        if (typeof opValue !== 'string' && !(opValue instanceof RegExp)) return false
        // Only pass flags if $options is explicitly provided, otherwise let RegExp keep its own flags
        const flags = operators.$options !== undefined ? String(operators.$options) : undefined
        let pattern: RegExp
        try {
          pattern = createSafeRegex(opValue, flags)
        } catch (err) {
          // Re-throw UnsafeRegexError (security concern - must not silently accept)
          if (err instanceof UnsafeRegexError) throw err
          // Other errors from createSafeRegex (e.g., SyntaxError) - treat as no match
          return false
        }
        try {
          // Use safeRegexTest for runtime protection against ReDoS attacks
          // This provides defense-in-depth beyond static pattern analysis
          if (!safeRegexTest(pattern, value)) return false
        } catch (err) {
          // Re-throw all errors from safeRegexTest - they all represent security concerns
          // (RegexTimeoutError, input length exceeded, etc.)
          throw err
        }
        break
      }

      case '$options':
        break // Handled with $regex

      case '$startsWith':
        if (typeof value !== 'string') return false
        if (typeof opValue !== 'string') return false
        if (!value.startsWith(opValue)) return false
        break

      case '$endsWith':
        if (typeof value !== 'string') return false
        if (typeof opValue !== 'string') return false
        if (!value.endsWith(opValue)) return false
        break

      case '$contains':
        if (typeof value !== 'string') return false
        if (typeof opValue !== 'string') return false
        if (!value.includes(opValue)) return false
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
        if (!value.some(elem => matchesFilter(elem, subFilter, config))) return false
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
        // Unknown operator - validate and handle
        if (!KNOWN_OPERATORS.has(op)) {
          handleUnknownOperator(op, config)
        }
        break
    }
  }

  return true
}

// Re-export utility functions for backwards compatibility
export { deepEqual, compareValues, getValueType } from '../utils'

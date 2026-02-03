/**
 * Minimal Filter Evaluation for Cloudflare Snippets
 *
 * Stripped-down filter evaluation optimized for:
 * - Small bundle size (<2KB minified)
 * - Basic operators only (=, <, >, <=, >=, $in)
 * - No regex or array operators
 *
 * For full filter support, use ParqueDB's query module.
 */

import type { Filter, FilterValue, Row } from './types'

// =============================================================================
// Filter Evaluation
// =============================================================================

/**
 * Check if a row matches a filter
 *
 * @param row - Row to check
 * @param filter - Filter to apply
 * @returns true if row matches all filter conditions
 */
export function matchesFilter(row: Row, filter: Filter): boolean {
  for (const [field, condition] of Object.entries(filter)) {
    if (condition === undefined) continue

    const value = getNestedValue(row, field)

    if (!matchesCondition(value, condition)) {
      return false
    }
  }
  return true
}

/**
 * Check if a value matches a condition
 */
function matchesCondition(value: unknown, condition: FilterValue): boolean {
  // Null condition
  if (condition === null) {
    return value === null || value === undefined
  }

  // Direct equality (non-operator value)
  if (!isOperatorObject(condition)) {
    return deepEqual(value, condition)
  }

  // Operator object
  const ops = condition as Record<string, unknown>

  // $eq
  if ('$eq' in ops) {
    return deepEqual(value, ops.$eq)
  }

  // $ne
  if ('$ne' in ops) {
    return !deepEqual(value, ops.$ne)
  }

  // $gt
  if ('$gt' in ops) {
    if (value === null || value === undefined) return false
    return compare(value, ops.$gt) > 0
  }

  // $gte
  if ('$gte' in ops) {
    if (value === null || value === undefined) return false
    return compare(value, ops.$gte) >= 0
  }

  // $lt
  if ('$lt' in ops) {
    if (value === null || value === undefined) return false
    return compare(value, ops.$lt) < 0
  }

  // $lte
  if ('$lte' in ops) {
    if (value === null || value === undefined) return false
    return compare(value, ops.$lte) <= 0
  }

  // $in
  if ('$in' in ops) {
    const arr = ops.$in as unknown[]
    return arr.some(v => deepEqual(value, v))
  }

  // Unknown operator - default to no match
  return true
}

/**
 * Check if value is an operator object (has $ keys)
 */
function isOperatorObject(value: unknown): boolean {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false
  }
  return Object.keys(value).some(k => k.startsWith('$'))
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get a nested value from an object using dot notation
 *
 * @example
 * getNestedValue({ a: { b: 1 } }, 'a.b') // => 1
 */
function getNestedValue(obj: Row, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined
    }
    if (typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Deep equality check
 */
function deepEqual(a: unknown, b: unknown): boolean {
  // Handle null/undefined equivalence
  if (a === null || a === undefined) {
    return b === null || b === undefined
  }
  if (b === null || b === undefined) {
    return false
  }

  // Primitives
  if (typeof a !== 'object' || typeof b !== 'object') {
    return a === b
  }

  // Arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((v, i) => deepEqual(v, b[i]))
  }

  // Objects
  if (Array.isArray(a) || Array.isArray(b)) {
    return false
  }

  const aObj = a as Record<string, unknown>
  const bObj = b as Record<string, unknown>
  const aKeys = Object.keys(aObj)
  const bKeys = Object.keys(bObj)

  if (aKeys.length !== bKeys.length) return false

  return aKeys.every(k => deepEqual(aObj[k], bObj[k]))
}

/**
 * Compare two values for ordering
 *
 * @returns negative if a < b, 0 if a == b, positive if a > b
 */
function compare(a: unknown, b: unknown): number {
  // Same type comparisons
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b
  }

  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b)
  }

  // Date comparisons
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime()
  }

  // Mixed types - convert to string
  return String(a).localeCompare(String(b))
}

// =============================================================================
// Filter Rows
// =============================================================================

/**
 * Filter an array of rows
 *
 * @param rows - Rows to filter
 * @param filter - Filter to apply
 * @returns Rows that match the filter
 */
export function filterRows(rows: Row[], filter: Filter): Row[] {
  return rows.filter(row => matchesFilter(row, filter))
}

/**
 * Find the first row matching a filter
 *
 * @param rows - Rows to search
 * @param filter - Filter to apply
 * @returns First matching row or undefined
 */
export function findRow(rows: Row[], filter: Filter): Row | undefined {
  return rows.find(row => matchesFilter(row, filter))
}

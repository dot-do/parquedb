/**
 * MongoDB-style Update Operators Engine for ParqueDB
 *
 * This module provides a simplified interface to the canonical update operators
 * implementation in src/mutation/operators.ts.
 *
 * For full operator support and relationship operations, use applyOperators from
 * the mutation module directly.
 *
 * Supported operators:
 * - Field: $set, $unset, $rename, $setOnInsert
 * - Numeric: $inc, $mul, $min, $max
 * - Array: $push, $pull, $pullAll, $addToSet, $pop
 * - Date: $currentDate
 * - Bitwise: $bit
 */

import type { UpdateInput } from '../types/update'
import {
  applyOperators,
  getField,
  validateUpdateOperators,
} from '../mutation/operators'
// Re-export types for consumers
export type { ApplyOperatorsOptions, ApplyOperatorsResult } from '../mutation/types'

// Re-export the canonical field access helpers
export { getField, setField, unsetField } from '../mutation/operators'

// Re-export comparison utilities from utils
export { compareValues, deepEqual } from '../utils'

// Re-export matchesFilter from filter module
export { matchesFilter } from './filter'

// =============================================================================
// Options
// =============================================================================

/**
 * Options for applyUpdate function
 */
export interface UpdateApplyOptions {
  /**
   * Whether this is an insert operation (for $setOnInsert)
   */
  isInsert?: boolean
}

// =============================================================================
// Main Update Function
// =============================================================================

/**
 * Apply update operators to a document
 * Returns new document (immutable - does not modify the original)
 *
 * This is a simplified wrapper around applyOperators that returns just the
 * updated document. For relationship operations and metadata about modified
 * fields, use applyOperators from the mutation module directly.
 *
 * @param doc - Original document to update
 * @param update - MongoDB-style update operators
 * @param options - Optional settings (e.g., for $setOnInsert during upserts)
 * @returns New document with updates applied
 *
 * @example
 * const doc = { name: 'John', count: 5, tags: ['a'] }
 * const updated = applyUpdate(doc, {
 *   $set: { name: 'Jane' },
 *   $inc: { count: 1 },
 *   $push: { tags: 'b' }
 * })
 * // updated = { name: 'Jane', count: 6, tags: ['a', 'b'] }
 */
export function applyUpdate<T extends Record<string, unknown>>(
  doc: T,
  update: UpdateInput<T>,
  options?: UpdateApplyOptions
): T {
  const result = applyOperators(doc, update, {
    isInsert: options?.isInsert,
    timestamp: new Date(),
  })
  return result.document as T
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate an update operation
 * @throws Error if update is invalid
 */
export function validateUpdate(update: UpdateInput): void {
  validateUpdateOperators(update)
}

// =============================================================================
// Array Helpers (for backwards compatibility)
// =============================================================================

/**
 * Sort an array in place using comparison values
 * @internal
 */
export function sortArray(arr: unknown[], sort: 1 | -1 | Record<string, 1 | -1>): void {
  const { compareValues } = require('../utils')

  if (typeof sort === 'number') {
    arr.sort((a, b) => {
      const cmp = compareValues(a, b)
      return sort === 1 ? cmp : -cmp
    })
  } else {
    const sortFields = Object.entries(sort)
    arr.sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aVal = getField(a as Record<string, unknown>, field)
        const bVal = getField(b as Record<string, unknown>, field)
        const cmp = compareValues(aVal, bVal)
        if (cmp !== 0) {
          return direction === 1 ? cmp : -cmp
        }
      }
      return 0
    })
  }
}

/**
 * Apply $slice to an array (in place)
 * @internal
 */
export function applySlice(arr: unknown[], slice: number): void {
  if (slice === 0) {
    arr.length = 0
  } else if (slice > 0) {
    if (arr.length > slice) {
      arr.length = slice
    }
  } else {
    const keep = Math.abs(slice)
    if (arr.length > keep) {
      arr.splice(0, arr.length - keep)
    }
  }
}

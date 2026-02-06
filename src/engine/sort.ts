/**
 * Sort Utilities — Pure functions for sorting DataLine arrays.
 *
 * Extracted from engine.ts to isolate the sort comparison logic
 * from the engine's query orchestration.
 *
 * sortEntities() is a pure function that returns a new sorted array
 * without mutating the input.
 */

import type { DataLine } from './types'
import { getNestedValue } from './filter'
import { DATA_SYSTEM_FIELDS } from './utils'

// =============================================================================
// resolveDataField
// =============================================================================

/**
 * Resolve a field value from a DataLine, checking $data first for user fields,
 * then falling back to top-level properties (system fields or flat compat).
 * Supports dot-notation paths for nested field access.
 */
export function resolveDataField(entity: DataLine, field: string): unknown {
  // System fields live at top level
  if (DATA_SYSTEM_FIELDS.has(field)) {
    return getNestedValue(entity as unknown as Record<string, unknown>, field)
  }
  // User fields: check $data first, then fall back to top-level for backward compat
  if (entity.$data) {
    const val = getNestedValue(entity.$data, field)
    if (val !== undefined) return val
  }
  // Fallback to flat top-level (backward compat with legacy DataLines)
  return getNestedValue(entity as unknown as Record<string, unknown>, field)
}

// =============================================================================
// sortEntities
// =============================================================================

/**
 * Sort entities by the given sort specification.
 *
 * The sort object maps field paths to 1 (ascending) or -1 (descending).
 * Multiple fields are used as tiebreakers in insertion order.
 *
 * Field paths support dot notation for nested fields (e.g., 'address.city').
 * User fields are resolved from $data first, falling back to flat top-level
 * for backward compatibility with legacy DataLines.
 *
 * Null/undefined values sort before all other values.
 *
 * Returns a new array; the original is not mutated.
 */
export function sortEntities(entities: DataLine[], sort: Record<string, 1 | -1>): DataLine[] {
  const sortKeys = Object.entries(sort)
  return [...entities].sort((a, b) => {
    for (const [field, direction] of sortKeys) {
      // Resolve sort field from $data (user data), falling back to top-level (system fields)
      const aVal = resolveDataField(a, field)
      const bVal = resolveDataField(b, field)

      let cmp = 0
      if (aVal === bVal) {
        cmp = 0
      } else if (aVal === undefined || aVal === null) {
        cmp = -1
      } else if (bVal === undefined || bVal === null) {
        cmp = 1
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      } else {
        cmp = String(aVal) < String(bVal) ? -1 : 1
      }

      if (cmp !== 0) return cmp * direction
    }
    return 0
  })
}

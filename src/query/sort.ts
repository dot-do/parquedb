/**
 * Sort utility for ParqueDB entities
 *
 * Provides a reusable sorting function for entity arrays that supports
 * multi-field sorting with configurable direction (ascending/descending).
 */

import type { SortSpec, SortDirection } from '../types/options'
import { normalizeSortDirection } from '../types/options'
import { getNestedValue, compareValues, isNullish } from '../utils'

/**
 * Compare two values for sorting with direction.
 * Wraps compareValues and applies the sort direction multiplier.
 *
 * @param a - First value to compare
 * @param b - Second value to compare
 * @param direction - Sort direction (1 for ascending, -1 for descending)
 * @returns Negative if a < b, positive if a > b, zero if equal (adjusted for direction)
 */
export function compareValuesWithDirection(a: unknown, b: unknown, direction: 1 | -1): number {
  return direction * compareValues(a, b)
}

/**
 * Compare two values for sorting with nulls-last behavior.
 * Null/undefined values sort after all non-null values regardless of sort direction.
 *
 * @param a - First value to compare
 * @param b - Second value to compare
 * @param direction - Sort direction (1 for ascending, -1 for descending)
 * @returns Negative if a < b, positive if a > b, zero if equal (adjusted for direction)
 */
export function compareValuesNullsLast(a: unknown, b: unknown, direction: 1 | -1): number {
  const aIsNull = isNullish(a)
  const bIsNull = isNullish(b)

  // Handle nulls last: null/undefined sort after all non-null values
  if (aIsNull && bIsNull) return 0 // Both null, equal
  if (aIsNull) return 1  // a is null, sort after b
  if (bIsNull) return -1 // b is null, sort after a

  return direction * compareValues(a, b)
}

/**
 * Sort an array of entities according to a sort specification.
 *
 * Supports multi-field sorting where each field can have its own direction.
 * Fields are compared in order; the first non-zero comparison determines the result.
 * Null/undefined values sort last (after all non-null values).
 *
 * @param entities - Array of entities to sort (mutated in place)
 * @param sortSpec - Sort specification mapping field names to directions
 * @returns The sorted array (same reference as input)
 *
 * @example
 * // Sort by createdAt descending
 * sortEntities(posts, { createdAt: -1 })
 *
 * @example
 * // Sort by status ascending, then by name descending
 * sortEntities(posts, { status: 1, name: -1 })
 *
 * @example
 * // Sort using string directions
 * sortEntities(posts, { createdAt: 'desc', title: 'asc' })
 */
export function sortEntities<T>(entities: T[], sortSpec: SortSpec): T[] {
  const sortEntries = Object.entries(sortSpec)

  // Validate sort directions upfront to fail fast on invalid input
  for (const [, direction] of sortEntries) {
    normalizeSortDirection(direction as SortDirection)
  }

  entities.sort((a, b) => {
    for (const [field, direction] of sortEntries) {
      const dir = normalizeSortDirection(direction as SortDirection)
      const aValue = getNestedValue(a as Record<string, unknown>, field)
      const bValue = getNestedValue(b as Record<string, unknown>, field)
      const cmp = compareValuesNullsLast(aValue, bValue, dir)
      if (cmp !== 0) return cmp
    }
    return 0
  })

  return entities
}

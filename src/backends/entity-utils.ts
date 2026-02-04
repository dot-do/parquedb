/**
 * Shared Entity Utilities for Backends
 *
 * Common utility functions used by IcebergBackend, BaseEntityBackend, and other backends.
 * Extracts duplicate code for update operations, sorting, pagination, and entity creation.
 */

import type { Entity, EntityId, EntityData } from '../types/entity'
import { entityId, SYSTEM_ACTOR } from '../types/entity'
import type { Update } from '../types/update'
import type { SortSpec, SortDirection } from '../types/options'
import { normalizeSortDirection } from '../types/options'

// Import update operators
import { applyOperators } from '../mutation/operators'

// =============================================================================
// Update Operations
// =============================================================================

/**
 * Apply update operators to an entity
 *
 * Supports all MongoDB-style operators via the mutation/operators module:
 * - Field: $set, $unset, $rename, $setOnInsert
 * - Numeric: $inc, $mul, $min, $max
 * - Array: $push, $pull, $pullAll, $addToSet, $pop
 * - Date: $currentDate
 * - Bitwise: $bit
 *
 * @param entity - The entity to update
 * @param update - MongoDB-style update document with operators
 * @returns Updated entity
 */
export function applyUpdate<T extends EntityData = EntityData>(entity: Entity<T>, update: Update): Entity<T> {
  const result = applyOperators(entity as Record<string, unknown>, update)
  return result.document as Entity<T>
}

// =============================================================================
// Default Entity Creation
// =============================================================================

/**
 * Create a default entity for upsert operations
 *
 * Creates a minimal entity with required fields for use when upserting
 * a non-existent entity. The entity is marked as version 0 and type 'unknown'.
 *
 * @param ns - Namespace for the entity
 * @param id - ID for the entity
 * @returns A minimal entity with default values
 */
export function createDefaultEntity<T extends EntityData = EntityData>(ns: string, id: string): Entity<T> {
  const now = new Date()
  return {
    $id: entityId(ns, id),
    $type: 'unknown',
    name: id,
    createdAt: now,
    createdBy: SYSTEM_ACTOR,
    updatedAt: now,
    updatedBy: SYSTEM_ACTOR,
    version: 0,
  } as Entity<T>
}

// =============================================================================
// Sorting Utilities
// =============================================================================

/**
 * Compare two values for sorting
 *
 * Handles null/undefined (sorted to end), strings, numbers, and dates.
 *
 * @param aVal - First value
 * @param bVal - Second value
 * @returns Negative if a < b, positive if a > b, 0 if equal
 */
export function compareValues(aVal: unknown, bVal: unknown): number {
  if (aVal === bVal) return 0
  if (aVal === null || aVal === undefined) return 1
  if (bVal === null || bVal === undefined) return -1
  if (typeof aVal === 'string' && typeof bVal === 'string') {
    return aVal.localeCompare(bVal)
  }
  if (typeof aVal === 'number' && typeof bVal === 'number') {
    return aVal - bVal
  }
  if (aVal instanceof Date && bVal instanceof Date) {
    return aVal.getTime() - bVal.getTime()
  }
  return String(aVal).localeCompare(String(bVal))
}

/**
 * Sort entities by specified fields (mutates array in place)
 *
 * Handles null/undefined values (sorted to end), strings, numbers, and dates.
 * Supports both numeric (1/-1) and string ('asc'/'desc') direction values.
 *
 * @param entities - Array of entities to sort (mutated in place)
 * @param sort - Sort specification mapping field names to directions
 */
export function sortEntities<T extends EntityData = EntityData>(
  entities: Entity<T>[],
  sort?: SortSpec
): void {
  if (!sort) return

  const sortFields = Object.entries(sort)
  entities.sort((a, b) => {
    for (const [field, direction] of sortFields) {
      const aVal = (a as Record<string, unknown>)[field]
      const bVal = (b as Record<string, unknown>)[field]

      const cmp = compareValues(aVal, bVal)
      if (cmp !== 0) {
        const normalizedDir = normalizeSortDirection(direction)
        return normalizedDir === -1 ? -cmp : cmp
      }
    }
    return 0
  })
}

/**
 * Sort entities and return a new sorted array (immutable version)
 *
 * Same as sortEntities but returns a new array instead of mutating.
 *
 * @param entities - Array of entities to sort
 * @param sort - Sort specification mapping field names to directions
 * @returns New sorted array
 */
export function sortEntitiesImmutable<T extends EntityData = EntityData>(
  entities: Entity<T>[],
  sort: Record<string, 1 | -1>
): Entity<T>[] {
  const sortFields = Object.entries(sort)

  return [...entities].sort((a, b) => {
    for (const [field, direction] of sortFields) {
      const aVal = (a as Record<string, unknown>)[field]
      const bVal = (b as Record<string, unknown>)[field]

      const cmp = compareValues(aVal, bVal)
      if (cmp !== 0) {
        return direction === -1 ? -cmp : cmp
      }
    }
    return 0
  })
}

// =============================================================================
// Pagination Utilities
// =============================================================================

/**
 * Apply skip and limit pagination to an array of entities
 *
 * @param entities - Array of entities to paginate
 * @param skip - Number of entities to skip (optional)
 * @param limit - Maximum number of entities to return (optional)
 * @returns Paginated slice of entities
 */
export function applyPagination<T extends EntityData = EntityData>(
  entities: Entity<T>[],
  skip?: number,
  limit?: number
): Entity<T>[] {
  let result = entities
  if (skip) {
    result = result.slice(skip)
  }
  if (limit) {
    result = result.slice(0, limit)
  }
  return result
}

/**
 * Apply pagination from options object
 *
 * Convenience wrapper for applyPagination that accepts an options object.
 *
 * @param entities - Array of entities to paginate
 * @param options - Options object with optional skip and limit properties
 * @returns Paginated slice of entities
 */
export function applyPaginationFromOptions<T extends EntityData = EntityData>(
  entities: Entity<T>[],
  options?: { skip?: number | undefined; limit?: number | undefined }
): Entity<T>[] {
  return applyPagination(entities, options?.skip, options?.limit)
}

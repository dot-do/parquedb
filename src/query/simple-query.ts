/**
 * Simple Query Execution for ParqueDB
 *
 * Provides a unified query execution function for filter, project, and sort operations.
 * This module is used by:
 * - MV refresh to apply view queries
 * - Query executor for post-processing
 * - Any other code needing in-memory query execution
 *
 * Consolidates query logic to avoid duplication across the codebase.
 */

import type { Filter } from '../types/filter'
import type { SortSpec } from '../types/options'
import { matchesFilter } from './filter'
import { sortEntities } from './sort'

// =============================================================================
// Types
// =============================================================================

/**
 * Projection specification - maps field names to inclusion/exclusion flags
 */
export type ProjectionSpec = Record<string, 0 | 1 | boolean>

/**
 * Simple query definition (filter/project/sort)
 *
 * Used for MV refresh and other in-memory query operations.
 * For full aggregation pipelines, use the aggregation executor instead.
 */
export interface SimpleQuery {
  /**
   * MongoDB-style filter to apply to the data
   */
  filter?: Filter

  /**
   * Projection to apply (field inclusion/exclusion)
   */
  project?: ProjectionSpec

  /**
   * Sort specification (field names to sort directions)
   */
  sort?: SortSpec
}

// =============================================================================
// Projection Implementation
// =============================================================================

/**
 * Apply projection to an array of records.
 *
 * Supports two modes:
 * - Inclusion mode: Only include fields set to 1 or true
 * - Exclusion mode: Include all fields except those set to 0 or false
 *
 * The mode is determined by the first non-zero/non-false value encountered.
 * If any field is set to 1/true, inclusion mode is used.
 * Otherwise, exclusion mode is used.
 *
 * @param data - Array of records to project
 * @param projection - Projection specification
 * @returns Array of projected records
 *
 * @example
 * // Inclusion mode - only include $id and name
 * applyProjection(data, { $id: 1, name: 1 })
 *
 * @example
 * // Exclusion mode - include all except password
 * applyProjection(data, { password: 0, secret: 0 })
 */
export function applyProjection<T extends Record<string, unknown>>(
  data: T[],
  projection: ProjectionSpec
): T[] {
  const fields = Object.keys(projection)
  if (fields.length === 0) {
    return data
  }

  // Determine mode: inclusion if any field is 1/true
  const isInclusion = fields.some(f => projection[f] === 1 || projection[f] === true)

  return data.map(row => {
    const result = {} as Record<string, unknown>

    if (isInclusion) {
      // Include only specified fields
      for (const field of fields) {
        if (projection[field] === 1 || projection[field] === true) {
          if (field in row) {
            result[field] = row[field]
          }
        }
      }
    } else {
      // Exclude specified fields (include all others)
      // Both 0 and false mean exclusion
      for (const [key, value] of Object.entries(row)) {
        const projValue = projection[key]
        if (!(key in projection) || (projValue !== 0 && projValue !== false)) {
          result[key] = value
        }
      }
    }

    return result as T
  })
}

// =============================================================================
// Simple Query Execution
// =============================================================================

/**
 * Execute a simple query (filter/project/sort) on in-memory data.
 *
 * This is the shared implementation used by:
 * - MV refresh (`fullRefresh`)
 * - Query executor post-processing
 * - Any other code needing filter/project/sort
 *
 * For aggregation pipelines, use `executeAggregation` from `src/aggregation/executor.ts`.
 *
 * @param data - Array of records to query
 * @param query - Simple query definition
 * @returns Filtered, projected, and sorted records
 *
 * @example
 * // Apply filter, project, and sort
 * const result = executeSimpleQuery(users, {
 *   filter: { status: 'active' },
 *   project: { $id: 1, name: 1, email: 1 },
 *   sort: { name: 1 }
 * })
 *
 * @example
 * // Filter only
 * const activeUsers = executeSimpleQuery(users, {
 *   filter: { status: 'active' }
 * })
 */
export function executeSimpleQuery<T extends Record<string, unknown>>(
  data: T[],
  query: SimpleQuery
): T[] {
  let result = data

  // Step 1: Apply filter
  if (query.filter && Object.keys(query.filter).length > 0) {
    result = result.filter(row => matchesFilter(row, query.filter!))
  }

  // Step 2: Apply projection
  if (query.project && Object.keys(query.project).length > 0) {
    result = applyProjection(result, query.project)
  }

  // Step 3: Apply sort
  if (query.sort && Object.keys(query.sort).length > 0) {
    // sortEntities mutates in place, so we need to copy first
    result = sortEntities([...result], query.sort)
  }

  return result
}

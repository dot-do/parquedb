/**
 * Routing Utilities for ParqueDB Worker
 *
 * Provides URL parsing and query parameter handling for HTTP routes.
 */

import type { Filter } from '../types/filter'
import type { FindOptions } from '../types/options'

// =============================================================================
// Query Parsing Helpers
// =============================================================================

/**
 * Parse filter from URL search params
 *
 * Supports JSON filter in the 'filter' query parameter:
 * ?filter={"status":"published"}
 */
export function parseQueryFilter(params: URLSearchParams): Filter {
  const filterParam = params.get('filter')
  if (filterParam) {
    try {
      return JSON.parse(filterParam)
    } catch {
      return {}
    }
  }
  return {}
}

/**
 * Parse query options from URL search params
 *
 * Supports:
 * - limit: number of results to return
 * - skip: number of results to skip
 * - cursor: cursor for pagination
 * - sort: JSON object or simple format "field:asc,field2:desc"
 * - project: JSON object or simple format "field1,field2,-field3"
 */
export function parseQueryOptions(params: URLSearchParams): FindOptions {
  const options: FindOptions = {}

  const limit = params.get('limit')
  if (limit) options.limit = parseInt(limit, 10)

  const skip = params.get('skip')
  if (skip) options.skip = parseInt(skip, 10)

  const cursor = params.get('cursor')
  if (cursor) options.cursor = cursor

  const sort = params.get('sort')
  if (sort) {
    try {
      options.sort = JSON.parse(sort)
    } catch {
      // Parse simple format: "field:asc,field2:desc"
      const sortSpec: Record<string, 1 | -1> = {}
      for (const part of sort.split(',')) {
        const [field, dir] = part.split(':')
        if (field) {
          sortSpec[field] = dir === 'desc' ? -1 : 1
        }
      }
      options.sort = sortSpec
    }
  }

  const project = params.get('project')
  if (project) {
    try {
      options.project = JSON.parse(project)
    } catch {
      // Parse simple format: "field1,field2,-field3"
      const projection: Record<string, 0 | 1> = {}
      for (const field of project.split(',')) {
        if (field.startsWith('-')) {
          projection[field.slice(1)] = 0
        } else if (field) {
          projection[field] = 1
        }
      }
      options.project = projection
    }
  }

  return options
}

// =============================================================================
// Route Matching Patterns
// =============================================================================

/**
 * Route pattern matchers for common routes
 */
export const RoutePatterns = {
  /** Match /datasets/:dataset */
  dataset: /^\/datasets\/([^/]+)$/,

  /** Match /datasets/:dataset/:collection */
  collection: /^\/datasets\/([^/]+)\/([^/]+)$/,

  /** Match /datasets/:dataset/:collection/:id */
  entity: /^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)$/,

  /** Match /datasets/:dataset/:collection/:id/:predicate */
  relationship: /^\/datasets\/([^/]+)\/([^/]+)\/([^/]+)\/([^/]+)$/,

  /** Match /ns/:namespace or /ns/:namespace/:id */
  ns: /^\/ns\/([^/]+)(?:\/([^/]+))?$/,
} as const

/**
 * Extract route parameters from a path using a pattern
 */
export function matchRoute<T extends string[]>(
  path: string,
  pattern: RegExp
): T | null {
  const match = path.match(pattern)
  if (!match) return null
  return match.slice(1) as unknown as T
}

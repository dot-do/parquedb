/**
 * Routing Utilities for ParqueDB Worker
 *
 * Provides URL parsing and query parameter handling for HTTP routes.
 */

import type { Filter } from '../types/filter'
import type { FindOptions } from '../types/options'
import { matchGroupsAs } from '../types/cast'

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error thrown when query parameters are invalid.
 * Carries a `status` of 400 for HTTP response mapping.
 */
export class QueryParamError extends Error {
  status = 400

  constructor(message: string) {
    super(message)
    this.name = 'QueryParamError'
  }
}

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
    let parsed: unknown
    try {
      parsed = JSON.parse(filterParam)
    } catch {
      throw new QueryParamError('Invalid filter: must be valid JSON')
    }
    if (
      parsed === null ||
      Array.isArray(parsed) ||
      typeof parsed !== 'object'
    ) {
      throw new QueryParamError('Invalid filter: must be a JSON object')
    }
    return parsed as Filter
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
  if (limit !== null && limit !== '') {
    const parsed = parseInt(limit, 10)
    if (!Number.isInteger(parsed) || parsed < 0) {
      throw new QueryParamError('Invalid limit: must be a non-negative integer')
    }
    options.limit = parsed
  }

  const skip = params.get('skip')
  if (skip) {
    const parsed = parseInt(skip, 10)
    if (!Number.isInteger(parsed) || parsed < 0) {
      throw new QueryParamError('Invalid skip: must be a non-negative integer')
    }
    options.skip = parsed
  }

  const cursor = params.get('cursor')
  if (cursor) options.cursor = cursor

  const sort = params.get('sort')
  if (sort) {
    try {
      options.sort = JSON.parse(sort)
    } catch {
      // Intentionally ignored: not JSON, parse as simple format "field:asc,field2:desc" instead
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
      // Intentionally ignored: not JSON, parse as simple format "field1,field2,-field3" instead
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
  return matchGroupsAs<T>(match.slice(1))
}

/**
 * Pagination utilities for ParqueDB queries
 *
 * Provides shared pagination logic for cursor-based and offset-based pagination
 * used by both core.ts (ParqueDBImpl) and query.ts (findEntities).
 */

import type { SortSpec, FindOptions, PaginatedResult } from '../types'
import { sortEntities } from './sort'

/**
 * Options for applying pagination to a result set
 */
export interface PaginationOptions {
  /** Sort specification */
  sort?: SortSpec
  /** Maximum number of results */
  limit?: number
  /** Number of results to skip (offset) */
  skip?: number
  /** Cursor for pagination (alternative to skip) */
  cursor?: string
  /** Field to use for cursor (default: '$id') */
  cursorField?: string
}

/**
 * Apply sorting, pagination, and cursor handling to a result set.
 *
 * This function consolidates the common pagination logic used throughout
 * ParqueDB to ensure consistent behavior across all query paths.
 *
 * @param items - Array of items to paginate
 * @param options - Pagination options (sort, limit, skip, cursor)
 * @returns PaginatedResult with items, hasMore, nextCursor, and total
 *
 * @example
 * ```typescript
 * const items = [{ $id: '1', name: 'A' }, { $id: '2', name: 'B' }, ...]
 * const result = applyPagination(items, { sort: { name: 1 }, limit: 10 })
 * // { items: [...], hasMore: true, nextCursor: '10', total: 100 }
 * ```
 */
export function applyPagination<T extends { $id?: string }>(
  items: T[],
  options?: PaginationOptions
): PaginatedResult<T> {
  let result = items

  // Apply sort using the shared sortEntities utility
  if (options?.sort) {
    sortEntities(result, options.sort)
  }

  // Calculate total count before pagination
  const totalCount = result.length

  // Apply cursor-based pagination
  if (options?.cursor) {
    const cursorField = options.cursorField ?? '$id'
    const cursorIndex = result.findIndex(
      e => (e as Record<string, unknown>)[cursorField] === options.cursor
    )
    if (cursorIndex >= 0) {
      result = result.slice(cursorIndex + 1)
    } else {
      // Cursor not found - return empty result
      result = []
    }
  }

  // Apply skip (offset)
  if (options?.skip && options.skip > 0) {
    result = result.slice(options.skip)
  }

  // Apply limit and determine hasMore
  const limit = options?.limit
  let hasMore = false
  let nextCursor: string | undefined

  if (limit !== undefined && limit > 0) {
    hasMore = result.length > limit
    if (hasMore) {
      result = result.slice(0, limit)
    }

    // Set nextCursor to last item's cursor field if there are more results
    if (hasMore && result.length > 0) {
      const lastItem = result[result.length - 1]
      const cursorField = options?.cursorField ?? '$id'
      nextCursor = (lastItem as Record<string, unknown>)[cursorField] as string | undefined
    }
  }

  return {
    items: result,
    hasMore,
    nextCursor,
    total: totalCount,
  }
}

/**
 * Extract pagination options from FindOptions
 *
 * Helper to convert FindOptions to PaginationOptions for use with applyPagination.
 *
 * @param options - FindOptions from query
 * @returns PaginationOptions subset
 */
export function extractPaginationOptions<T>(options?: FindOptions<T>): PaginationOptions {
  return {
    sort: options?.sort,
    limit: options?.limit,
    skip: options?.skip,
    cursor: options?.cursor,
  }
}

/**
 * Options for offset-based pagination (used by getRelated)
 */
export interface OffsetPaginationOptions {
  /** Sort specification */
  sort?: SortSpec
  /** Maximum number of results */
  limit?: number
  /** Numeric offset cursor (e.g., "0", "10", "20") */
  cursor?: string
}

/**
 * Result type for offset-based pagination
 */
export interface OffsetPaginatedResult<T> {
  /** Paginated items */
  items: T[]
  /** Total count before pagination */
  total: number
  /** Whether there are more results */
  hasMore: boolean
  /** Cursor for next page (numeric offset string) */
  nextCursor?: string
}

/**
 * Apply offset-based pagination to a result set.
 *
 * Unlike applyPagination which uses ID-based cursors, this function uses
 * numeric offset cursors (e.g., "0", "10", "20") for simpler pagination.
 * This is used by getRelated operations.
 *
 * @param items - Array of items to paginate
 * @param options - Offset pagination options (sort, limit, cursor)
 * @returns OffsetPaginatedResult with items, hasMore, nextCursor, and total
 *
 * @example
 * ```typescript
 * const items = [{ name: 'A' }, { name: 'B' }, ...]
 * const result = applyOffsetPagination(items, { limit: 10, cursor: '0' })
 * // { items: [...], hasMore: true, nextCursor: '10', total: 100 }
 * ```
 */
export function applyOffsetPagination<T>(
  items: T[],
  options?: OffsetPaginationOptions
): OffsetPaginatedResult<T> {
  let result = items

  // Apply sort using the shared sortEntities utility
  if (options?.sort) {
    sortEntities(result, options.sort)
  }

  const total = result.length
  const limit = options?.limit ?? total
  const cursor = options?.cursor ? parseInt(options.cursor, 10) : 0

  // Apply offset-based pagination
  const paginatedItems = result.slice(cursor, cursor + limit)
  const hasMore = cursor + limit < total
  const nextCursor = hasMore ? String(cursor + limit) : undefined

  return {
    items: paginatedItems,
    total,
    hasMore,
    nextCursor,
  }
}

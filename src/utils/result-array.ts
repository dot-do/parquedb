/**
 * ResultArray - An array wrapper with pagination metadata properties
 *
 * Creates an array that can be used like a normal array (iteration, map, filter, etc.)
 * but also has $total, $next, and $prev as special properties for pagination metadata.
 *
 * Key features:
 * - Direct array iteration (no .items wrapper needed)
 * - All standard array methods work (map, filter, reduce, etc.)
 * - Bidirectional pagination via $next and $prev cursors
 * - Clean JSON serialization (metadata properties are non-enumerable)
 * - Backward compatibility with PaginatedResult (.items, .total, .hasMore, .nextCursor)
 *
 * @module utils/result-array
 */

/**
 * Type for array with pagination metadata.
 *
 * Extends Array<T> with:
 * - $total: Total count of matching items (before pagination)
 * - $next: Cursor for next page (undefined if no more pages)
 * - $prev: Cursor for previous page (undefined if on first page)
 *
 * For backward compatibility with PaginatedResult:
 * - items: Returns the array itself
 * - total: Alias for $total
 * - hasMore: Boolean indicating if more results exist
 * - nextCursor: Alias for $next
 */
export type ResultArray<T> = T[] & {
  /** Total count of matching items (before pagination) */
  readonly $total: number
  /** Cursor for fetching the next page, undefined if no more results */
  readonly $next: string | undefined
  /** Cursor for fetching the previous page, undefined if on first page */
  readonly $prev: string | undefined
  // Backward compatibility with PaginatedResult
  /** @deprecated Use the array directly instead of .items */
  readonly items: T[]
  /** @deprecated Use $total instead */
  readonly total: number
  /** @deprecated Use $next !== undefined instead */
  readonly hasMore: boolean
  /** @deprecated Use $next instead */
  readonly nextCursor: string | undefined
}

/**
 * Metadata for creating a ResultArray
 */
export interface ResultArrayMetadata {
  /** Total count of matching items */
  total: number
  /** Cursor for next page */
  next?: string | undefined
  /** Cursor for previous page */
  prev?: string | undefined
}

/**
 * Creates a ResultArray from items and metadata.
 *
 * The returned array:
 * - Is directly iterable with for...of
 * - Has working .map(), .filter(), .length, indexing
 * - Has $total, $next, and $prev as proxy properties for pagination
 * - Has backward-compatible .items, .total, .hasMore, .nextCursor
 * - Serializes to clean JSON (metadata properties are non-enumerable)
 *
 * @param items - The array items
 * @param metadata - Pagination metadata (total count, next/prev cursors)
 * @returns A ResultArray with metadata properties
 *
 * @example
 * ```typescript
 * const results = createResultArray(
 *   [{ id: 1 }, { id: 2 }],
 *   { total: 100, next: 'cursor123', prev: 'cursor001' }
 * )
 *
 * // Direct iteration (new API)
 * for (const item of results) {
 *   console.log(item.id)
 * }
 *
 * // Array methods work
 * results.map(x => x.id)  // [1, 2]
 * results.length          // 2
 * results[0]              // { id: 1 }
 *
 * // New metadata access
 * results.$total          // 100
 * results.$next           // 'cursor123'
 * results.$prev           // 'cursor001'
 *
 * // Bidirectional pagination
 * if (results.$next) {
 *   // fetch next page with cursor: results.$next
 * }
 * if (results.$prev) {
 *   // fetch previous page with cursor: results.$prev
 * }
 *
 * // Backward compatible access (deprecated)
 * results.items           // same as results
 * results.total           // 100
 * results.hasMore         // true
 * results.nextCursor      // 'cursor123'
 *
 * // Clean JSON serialization (metadata excluded)
 * JSON.stringify(results) // '[{"id":1},{"id":2}]'
 * ```
 */
export function createResultArray<T>(
  items: T[],
  metadata: ResultArrayMetadata
): ResultArray<T> {
  // Create a copy of the items array
  const arr = [...items] as T[]
  const hasMore = metadata.next !== undefined

  // Use a Proxy to add special properties
  return new Proxy(arr, {
    get(target, prop, receiver) {
      // New API
      if (prop === '$total') {
        return metadata.total
      }
      if (prop === '$next') {
        return metadata.next
      }
      if (prop === '$prev') {
        return metadata.prev
      }
      // Backward compatibility with PaginatedResult
      if (prop === 'items') {
        return target // Return the array itself
      }
      if (prop === 'total') {
        return metadata.total
      }
      if (prop === 'hasMore') {
        return hasMore
      }
      if (prop === 'nextCursor') {
        return metadata.next
      }
      // For all other properties, delegate to the array
      return Reflect.get(target, prop, receiver)
    },
    // Property descriptors for special properties
    getOwnPropertyDescriptor(target, prop) {
      if (prop === '$total' || prop === 'total') {
        return {
          configurable: true,
          enumerable: false,
          value: metadata.total,
          writable: false,
        }
      }
      if (prop === '$next' || prop === 'nextCursor') {
        return {
          configurable: true,
          enumerable: false,
          value: metadata.next,
          writable: false,
        }
      }
      if (prop === '$prev') {
        return {
          configurable: true,
          enumerable: false,
          value: metadata.prev,
          writable: false,
        }
      }
      if (prop === 'items') {
        return {
          configurable: true,
          enumerable: false,
          value: target,
          writable: false,
        }
      }
      if (prop === 'hasMore') {
        return {
          configurable: true,
          enumerable: false,
          value: hasMore,
          writable: false,
        }
      }
      return Reflect.getOwnPropertyDescriptor(target, prop)
    },
    // Ensure 'in' operator works for special properties
    has(target, prop) {
      if (prop === '$total' || prop === '$next' || prop === '$prev' || prop === 'items' || prop === 'total' || prop === 'hasMore' || prop === 'nextCursor') {
        return true
      }
      return Reflect.has(target, prop)
    },
  }) as ResultArray<T>
}

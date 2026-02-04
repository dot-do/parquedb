/**
 * Query and operation options for ParqueDB
 */

import type { EntityId } from './entity'
import type { Filter } from './filter'

// =============================================================================
// Sort Options
// =============================================================================

/** Sort direction */
export type SortDirection = 1 | -1 | 'asc' | 'desc'

/** Sort specification */
export interface SortSpec {
  [field: string]: SortDirection
}

/** Normalize sort direction to 1 or -1 */
export function normalizeSortDirection(dir: SortDirection): 1 | -1 {
  if (dir === 'asc' || dir === 1) return 1
  if (dir === 'desc' || dir === -1) return -1
  throw new Error(`Invalid sort direction: ${dir}`)
}

// =============================================================================
// Projection Options
// =============================================================================

/** Field projection - include (1) or exclude (0) */
export interface Projection {
  [field: string]: 0 | 1 | boolean
}

// =============================================================================
// Populate Options
// =============================================================================

/** Options for populating related entities */
export interface PopulateOptions {
  /** Maximum related entities to include */
  limit?: number | undefined
  /** Sort order for related entities */
  sort?: SortSpec | undefined
  /** Cursor for pagination */
  cursor?: string | undefined
  /** Filter for related entities */
  filter?: Filter | undefined
  /** Nested populate */
  populate?: PopulateSpec | undefined
}

/** Populate specification */
export type PopulateSpec =
  | string[]                                    // ['author', 'categories']
  | { [predicate: string]: boolean | PopulateOptions }  // { author: true, comments: { limit: 5 } }

// =============================================================================
// Find Options
// =============================================================================

/**
 * Options for find operations
 *
 * @example
 * // Simple find with limit
 * { limit: 20 }
 *
 * @example
 * // With sort and pagination
 * { sort: { createdAt: -1 }, limit: 20, cursor: 'abc...' }
 *
 * @example
 * // With projection
 * { project: { title: 1, content: 1, author: 1 } }
 *
 * @example
 * // With populate
 * { populate: ['author'], limit: 20 }
 *
 * @example
 * // Full options
 * {
 *   filter: { status: 'published' },
 *   sort: { createdAt: -1 },
 *   limit: 20,
 *   skip: 0,
 *   project: { title: 1, content: 1 },
 *   populate: { author: true, comments: { limit: 5 } },
 *   includeDeleted: false,
 *   asOf: new Date('2024-01-01')
 * }
 */
export interface FindOptions<T = unknown> {
  /** Filter (alternative to passing filter as first arg) */
  filter?: Filter | undefined

  /** Sort order */
  sort?: SortSpec | undefined

  /** Maximum number of results */
  limit?: number | undefined

  /** Number of results to skip (offset) */
  skip?: number | undefined

  /** Cursor for pagination (alternative to skip) */
  cursor?: string | undefined

  /** Field projection */
  project?: Projection | undefined

  /** Populate related entities */
  populate?: PopulateSpec | undefined

  /** Include soft-deleted entities */
  includeDeleted?: boolean | undefined

  /** Time-travel: query as of specific time */
  asOf?: Date | undefined

  /** Explain query plan without executing */
  explain?: boolean | undefined

  /** Hint for index to use */
  hint?: string | { [field: string]: 1 | -1 } | undefined

  /** Maximum time in milliseconds */
  maxTimeMs?: number | undefined
}

// =============================================================================
// Get Options
// =============================================================================

/**
 * Options for get (single entity) operations
 */
export interface GetOptions {
  /** Include soft-deleted entity */
  includeDeleted?: boolean | undefined

  /** Time-travel: get entity as of specific time */
  asOf?: Date | undefined

  /** Hydrate related entities (fetch full entity, not just link) */
  hydrate?: string[] | undefined

  /** Maximum inbound references to inline */
  maxInbound?: number | undefined

  /** Field projection */
  project?: Projection | undefined
}

// =============================================================================
// Related Options
// =============================================================================

/**
 * Options for traversing relationships
 */
export interface RelatedOptions {
  /** Predicate name (for outbound) */
  predicate?: string | undefined

  /** Reverse name (for inbound) */
  reverse?: string | undefined

  /** Maximum results */
  limit?: number | undefined

  /** Cursor for pagination */
  cursor?: string | undefined

  /** Filter related entities */
  filter?: Filter | undefined

  /** Sort order */
  sort?: SortSpec | undefined

  /** Include soft-deleted relationships */
  includeDeleted?: boolean | undefined

  /** Time-travel */
  asOf?: Date | undefined
}

// =============================================================================
// Create Options
// =============================================================================

/**
 * Validation mode for schema validation
 */
export type ValidationMode = 'strict' | 'permissive' | 'warn'

/**
 * Options for create operations
 */
export interface CreateOptions {
  /** Actor performing the create (for audit) */
  actor?: EntityId | undefined

  /** Skip validation entirely */
  skipValidation?: boolean | undefined

  /**
   * Enable schema validation on write (default: true when schema is provided)
   * - 'strict': Throws on any validation error
   * - 'permissive': Only validates required fields and types, allows unknown fields
   * - 'warn': Logs warnings instead of throwing errors
   * - true: Same as 'strict'
   * - false: Same as skipValidation
   */
  validateOnWrite?: boolean | ValidationMode | undefined

  /** Return the created entity (default: true) */
  returnDocument?: boolean | undefined
}

// =============================================================================
// Update Options
// =============================================================================

/**
 * Options for update operations
 */
export interface UpdateOptions {
  /** Actor performing the update (for audit) */
  actor?: EntityId | undefined

  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined

  /** Create if not exists (upsert) */
  upsert?: boolean | undefined

  /** Return the updated document */
  returnDocument?: 'before' | 'after' | undefined

  /** Skip validation entirely */
  skipValidation?: boolean | undefined

  /**
   * Enable schema validation on write (default: true when schema is provided)
   * - 'strict': Throws on any validation error
   * - 'permissive': Only validates required fields and types, allows unknown fields
   * - 'warn': Logs warnings instead of throwing errors
   * - true: Same as 'strict'
   * - false: Same as skipValidation
   */
  validateOnWrite?: boolean | ValidationMode | undefined

  /** Array filters for positional updates */
  arrayFilters?: Filter[] | undefined
}

// =============================================================================
// Delete Options
// =============================================================================

/**
 * Options for delete operations
 */
export interface DeleteOptions {
  /** Actor performing the delete (for audit) */
  actor?: EntityId | undefined

  /** Hard delete (permanent, skip soft delete) */
  hard?: boolean | undefined

  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined
}

// =============================================================================
// Bulk Options
// =============================================================================

/**
 * Options for bulk operations
 */
export interface BulkOptions {
  /** Continue on error */
  ordered?: boolean | undefined

  /** Actor performing the operations */
  actor?: EntityId | undefined

  /** Skip validation */
  skipValidation?: boolean | undefined
}

// =============================================================================
// History Options
// =============================================================================

/**
 * Options for entity history queries
 */
export interface HistoryOptions {
  /** Start of time range */
  from?: Date | undefined

  /** End of time range */
  to?: Date | undefined

  /** Maximum number of events */
  limit?: number | undefined

  /** Cursor for pagination */
  cursor?: string | undefined

  /** Filter by operation type */
  op?: 'CREATE' | 'UPDATE' | 'DELETE' | undefined

  /** Filter by actor */
  actor?: EntityId | undefined
}

// =============================================================================
// Aggregate Options
// =============================================================================

/**
 * Options for aggregation pipeline
 */
export interface AggregateOptions {
  /** Maximum time in milliseconds */
  maxTimeMs?: number | undefined

  /** Allow disk use for large aggregations */
  allowDiskUse?: boolean | undefined

  /** Hint for index */
  hint?: string | { [field: string]: 1 | -1 } | undefined

  /** Include soft-deleted entities */
  includeDeleted?: boolean | undefined

  /** Time-travel */
  asOf?: Date | undefined

  /** Explain without executing */
  explain?: boolean | undefined

  /**
   * Index manager for index-aware $match stage execution.
   * When provided, the aggregation executor will attempt to use
   * secondary indexes (hash, sst, fts, vector) for the first $match stage.
   */
  indexManager?: import('../indexes/manager').IndexManager | undefined
}

// =============================================================================
// Result Types (for handlers that need them)
// =============================================================================

/** Result of a find operation */
export interface FindResult<T> {
  /** Array of matching entities */
  items: T[]
  /** Total count (if countTotal was requested) */
  total?: number | undefined
  /** Cursor for pagination */
  cursor?: string | undefined
  /** Whether there are more results */
  hasMore: boolean
  /** Query statistics */
  stats?: {
    bytesRead?: number | undefined
    filesRead?: number | undefined
  } | undefined
}

/** Result of an update operation */
export interface UpdateResult {
  /** Number of documents that matched the filter */
  matchedCount: number
  /** Number of documents that were actually modified */
  modifiedCount: number
}

/** Result of a delete operation */
export interface DeleteResult {
  /** Number of documents that were deleted */
  deletedCount: number
}

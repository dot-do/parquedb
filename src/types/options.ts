/**
 * Query and operation options for ParqueDB
 */

import type { EntityId, Entity, Filter } from './index'

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
  limit?: number
  /** Sort order for related entities */
  sort?: SortSpec
  /** Cursor for pagination */
  cursor?: string
  /** Filter for related entities */
  filter?: Filter
  /** Nested populate */
  populate?: PopulateSpec
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
  filter?: Filter

  /** Sort order */
  sort?: SortSpec

  /** Maximum number of results */
  limit?: number

  /** Number of results to skip (offset) */
  skip?: number

  /** Cursor for pagination (alternative to skip) */
  cursor?: string

  /** Field projection */
  project?: Projection

  /** Populate related entities */
  populate?: PopulateSpec

  /** Include soft-deleted entities */
  includeDeleted?: boolean

  /** Time-travel: query as of specific time */
  asOf?: Date

  /** Explain query plan without executing */
  explain?: boolean

  /** Hint for index to use */
  hint?: string | { [field: string]: 1 | -1 }

  /** Maximum time in milliseconds */
  maxTimeMs?: number
}

// =============================================================================
// Get Options
// =============================================================================

/**
 * Options for get (single entity) operations
 */
export interface GetOptions {
  /** Include soft-deleted entity */
  includeDeleted?: boolean

  /** Time-travel: get entity as of specific time */
  asOf?: Date

  /** Hydrate related entities (fetch full entity, not just link) */
  hydrate?: string[]

  /** Maximum inbound references to inline */
  maxInbound?: number

  /** Field projection */
  project?: Projection
}

// =============================================================================
// Related Options
// =============================================================================

/**
 * Options for traversing relationships
 */
export interface RelatedOptions {
  /** Predicate name (for outbound) */
  predicate?: string

  /** Reverse name (for inbound) */
  reverse?: string

  /** Maximum results */
  limit?: number

  /** Cursor for pagination */
  cursor?: string

  /** Filter related entities */
  filter?: Filter

  /** Sort order */
  sort?: SortSpec

  /** Include soft-deleted relationships */
  includeDeleted?: boolean

  /** Time-travel */
  asOf?: Date
}

// =============================================================================
// Create Options
// =============================================================================

/**
 * Options for create operations
 */
export interface CreateOptions {
  /** Actor performing the create (for audit) */
  actor?: EntityId

  /** Skip validation */
  skipValidation?: boolean

  /** Return the created entity (default: true) */
  returnDocument?: boolean
}

// =============================================================================
// Update Options
// =============================================================================

/**
 * Options for update operations
 */
export interface UpdateOptions {
  /** Actor performing the update (for audit) */
  actor?: EntityId

  /** Expected version for optimistic concurrency */
  expectedVersion?: number

  /** Create if not exists (upsert) */
  upsert?: boolean

  /** Return the updated document */
  returnDocument?: 'before' | 'after'

  /** Skip validation */
  skipValidation?: boolean

  /** Array filters for positional updates */
  arrayFilters?: Filter[]
}

// =============================================================================
// Delete Options
// =============================================================================

/**
 * Options for delete operations
 */
export interface DeleteOptions {
  /** Actor performing the delete (for audit) */
  actor?: EntityId

  /** Hard delete (permanent, skip soft delete) */
  hard?: boolean

  /** Expected version for optimistic concurrency */
  expectedVersion?: number
}

// =============================================================================
// Bulk Options
// =============================================================================

/**
 * Options for bulk operations
 */
export interface BulkOptions {
  /** Continue on error */
  ordered?: boolean

  /** Actor performing the operations */
  actor?: EntityId

  /** Skip validation */
  skipValidation?: boolean
}

// =============================================================================
// History Options
// =============================================================================

/**
 * Options for entity history queries
 */
export interface HistoryOptions {
  /** Start of time range */
  from?: Date

  /** End of time range */
  to?: Date

  /** Maximum number of events */
  limit?: number

  /** Cursor for pagination */
  cursor?: string

  /** Filter by operation type */
  op?: 'CREATE' | 'UPDATE' | 'DELETE'

  /** Filter by actor */
  actor?: EntityId
}

// =============================================================================
// Aggregate Options
// =============================================================================

/**
 * Options for aggregation pipeline
 */
export interface AggregateOptions {
  /** Maximum time in milliseconds */
  maxTimeMs?: number

  /** Allow disk use for large aggregations */
  allowDiskUse?: boolean

  /** Hint for index */
  hint?: string | { [field: string]: 1 | -1 }

  /** Include soft-deleted entities */
  includeDeleted?: boolean

  /** Time-travel */
  asOf?: Date

  /** Explain without executing */
  explain?: boolean
}

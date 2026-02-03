/**
 * Materialized View Types for ParqueDB
 *
 * Defines the core types and interfaces for materialized views.
 * MVs are identified by the presence of `$from` - no special directive needed.
 *
 * Stream collections use `$ingest` to wire up automatic data ingestion
 * from external sources (AI SDK, tail events, evalite, etc.).
 */

import type { Filter } from '../types/filter'

// =============================================================================
// Branded Types
// =============================================================================

/** Materialized view identifier */
export type MVId = string & { readonly __brand: unique symbol }

/** Create an MVId from a string */
export function mvId(id: string): MVId {
  return id as MVId
}

/** View name (branded type) */
export type ViewName = string & { readonly __brand: unique symbol }

/** Create a ViewName from a string */
export function viewName(name: string): ViewName {
  return name as ViewName
}

// =============================================================================
// Refresh Configuration
// =============================================================================

/**
 * How the materialized view is refreshed
 *
 * - 'streaming': Updates automatically when source data changes (default)
 * - 'scheduled': Updates on a schedule (cron expression)
 * - 'manual': Only refreshed when explicitly requested
 */
export type RefreshMode = 'streaming' | 'scheduled' | 'manual'

/**
 * Refresh mode enum-like object for programmatic access
 */
export const RefreshMode = {
  Streaming: 'streaming' as const,
  Scheduled: 'scheduled' as const,
  Manual: 'manual' as const,
} as const

/**
 * Strategy for applying updates to the materialized view
 *
 * - 'replace': Atomically replace the entire view (default for scheduled)
 * - 'append': Append new data (for INSERT-only workloads)
 */
export type RefreshStrategy = 'replace' | 'append'

/**
 * Refresh configuration for an MV
 */
export interface RefreshConfig {
  /**
   * Refresh mode
   * @default 'streaming'
   */
  mode: RefreshMode

  /**
   * Cron schedule (required for scheduled mode)
   * @example '0 * * * *' - Every hour
   * @example '0 0 * * *' - Daily at midnight
   */
  schedule?: string

  /**
   * Refresh strategy
   * @default 'replace'
   */
  strategy?: RefreshStrategy

  /**
   * Grace period for stale data (allows querying stale MV)
   * @example '15m' - Allow 15 minutes of staleness
   */
  gracePeriod?: string

  /**
   * Timezone for cron schedule
   * @default 'UTC'
   */
  timezone?: string

  /**
   * Override storage backend for the MV
   * By default, uses the same backend as source tables
   */
  backend?: 'native' | 'iceberg' | 'delta'
}

// =============================================================================
// Aggregate Expressions
// =============================================================================

/**
 * Aggregate function expressions for $compute
 */
export interface AggregateExpr {
  $count?: '*' | string
  $sum?: string | ConditionalExpr
  $avg?: string | ConditionalExpr
  $min?: string
  $max?: string
  $first?: string
  $last?: string
}

/**
 * Conditional expression for computed aggregates
 */
export interface ConditionalExpr {
  $cond: [Filter | [string, unknown], number, number]
}

// =============================================================================
// Ingest Configuration (for Stream Collections)
// =============================================================================

/**
 * Known ingest sources
 *
 * - 'ai-sdk': AI SDK middleware (generates AIRequests, Generations)
 * - 'tail': Cloudflare Workers tail events
 * - 'evalite': Evalite evaluation framework
 * - Or a custom string for user-defined ingest handlers
 */
export type IngestSource = 'ai-sdk' | 'tail' | 'evalite' | string

// =============================================================================
// Collection Definition (with optional $ingest)
// =============================================================================

/**
 * A collection definition with optional stream ingestion
 *
 * When `$ingest` is present, the collection automatically receives data
 * from the specified source (AI SDK, tail events, etc.).
 *
 * @example
 * // Stream collection for AI requests
 * const AIRequests = {
 *   $type: 'AIRequest',
 *   modelId: 'string!',
 *   tokens: 'int?',
 *   latencyMs: 'int!',
 *   timestamp: 'timestamp!',
 *   $ingest: 'ai-sdk',  // Wires up middleware ingestion
 * }
 */
export interface CollectionDefinition {
  /** Entity type name */
  $type?: string

  /** Field definitions (IceType format) */
  [field: string]: unknown

  /** Stream ingest source (makes this a stream collection) */
  $ingest?: IngestSource
}

// =============================================================================
// Materialized View Definition
// =============================================================================

/**
 * Group by specification
 *
 * Can be:
 * - A field name string (e.g., 'status')
 * - An object mapping alias to expression (e.g., { date: '$createdAt' })
 */
export type GroupBySpec = string | Record<string, string>

/**
 * Materialized View Definition (IceType Directives style)
 *
 * MVs are identified by the presence of `$from` - this is the key indicator
 * that distinguishes an MV from a regular collection.
 *
 * @example
 * // Simple denormalized view
 * const OrderAnalytics = {
 *   $from: 'Order',
 *   $expand: ['customer', 'items.product'],
 *   $flatten: { 'customer': 'buyer' },
 * }
 *
 * @example
 * // Aggregated daily sales
 * const DailySales = {
 *   $from: 'Order',
 *   $groupBy: [{ date: '$createdAt' }, 'status'],
 *   $compute: {
 *     orderCount: { $count: '*' },
 *     revenue: { $sum: 'total' },
 *     avgOrder: { $avg: 'total' },
 *   },
 *   $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
 * }
 *
 * @example
 * // Filtered view from stream collection
 * const WorkerErrors = {
 *   $from: 'TailEvents',
 *   $filter: { outcome: { $ne: 'ok' } },
 * }
 */
export interface MVDefinition {
  /**
   * Source collection name (REQUIRED for MVs)
   * The presence of $from identifies this as a materialized view
   */
  $from: string

  /**
   * Relations to expand (denormalize) into the view
   * @example ['customer', 'items.product']
   */
  $expand?: string[]

  /**
   * Rename expanded field prefixes
   * @example { 'customer': 'buyer' } // customer_name → buyer_name
   */
  $flatten?: Record<string, string>

  /**
   * Filter to apply to source data
   */
  $filter?: Filter

  /**
   * Fields to select (for projection views)
   * Maps output field names to source expressions
   */
  $select?: Record<string, string>

  /**
   * Group by dimensions (for aggregation views)
   */
  $groupBy?: GroupBySpec[]

  /**
   * Computed aggregates
   */
  $compute?: Record<string, AggregateExpr>

  /**
   * Unnest an array field (flatten array to rows)
   */
  $unnest?: string

  /**
   * Refresh configuration
   * @default { mode: 'streaming' }
   */
  $refresh?: RefreshConfig
}

/**
 * Union type for schema entries: either a collection or an MV
 */
export type SchemaEntry = CollectionDefinition | MVDefinition

// =============================================================================
// MV Status
// =============================================================================

/**
 * Status of a materialized view
 *
 * - 'creating': View is being created for the first time
 * - 'ready': View is ready and up-to-date (fresh)
 * - 'refreshing': View is currently being refreshed
 * - 'stale': View needs refresh (data has changed since last refresh)
 * - 'error': View refresh failed
 * - 'disabled': View has been disabled
 */
export type MVStatus =
  | 'creating'
  | 'ready'
  | 'refreshing'
  | 'stale'
  | 'error'
  | 'disabled'

/**
 * MV Status enum-like object for programmatic access
 */
export const MVStatus = {
  Creating: 'creating' as const,
  Ready: 'ready' as const,
  Refreshing: 'refreshing' as const,
  Stale: 'stale' as const,
  Error: 'error' as const,
  Disabled: 'disabled' as const,
} as const

/**
 * Staleness state for query routing
 */
export type MVState = 'fresh' | 'stale' | 'invalid'

// =============================================================================
// MV Lineage (for staleness detection)
// =============================================================================

/**
 * Lineage information for tracking MV freshness
 */
export interface MVLineage {
  /** Snapshot ID of each source table at last refresh */
  sourceSnapshots: Map<string, string>

  /** Version ID of the MV definition when last refreshed */
  refreshVersionId: string

  /** Timestamp of last refresh */
  lastRefreshTime: Date
}

// =============================================================================
// MV Metadata
// =============================================================================

/**
 * Metadata for a materialized view
 *
 * Contains information about the view's state, timing, and statistics.
 */
export interface MVMetadata {
  /** Unique identifier for the view */
  id: MVId

  /** Human-readable name */
  name: string

  /** The MV definition */
  definition: MVDefinition

  /** Current status of the view */
  status: MVStatus

  /** When the view was created */
  createdAt: Date

  /** When the view was last refreshed */
  lastRefreshedAt?: Date

  /** Duration of the last refresh in milliseconds */
  lastRefreshDurationMs?: number

  /** Next scheduled refresh time (for scheduled views) */
  nextRefreshAt?: Date

  /** Number of rows in the view */
  rowCount?: number

  /** Size of the view in bytes */
  sizeBytes?: number

  /** Version number (incremented on each refresh) */
  version: number

  /** Lineage for staleness detection */
  lineage: MVLineage

  /** Error message if status is 'error' */
  errorMessage?: string

  /** Custom metadata */
  meta?: Record<string, unknown>
}

// =============================================================================
// View Stats
// =============================================================================

/**
 * Statistics for a materialized view
 */
export interface MVStats {
  /** Total number of refreshes */
  totalRefreshes: number

  /** Number of successful refreshes */
  successfulRefreshes: number

  /** Number of failed refreshes */
  failedRefreshes: number

  /** Average refresh duration in milliseconds */
  avgRefreshDurationMs: number

  /** Total query count against this view */
  queryCount: number

  /** Cache hit ratio (0-1) */
  cacheHitRatio: number
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if an entry is a Materialized View (has $from)
 */
export function isMVDefinition(entry: unknown): entry is MVDefinition {
  if (typeof entry !== 'object' || entry === null) return false
  return '$from' in entry && typeof (entry as MVDefinition).$from === 'string'
}

/**
 * Check if an entry is a Stream Collection (has $ingest)
 */
export function isStreamCollection(entry: unknown): entry is CollectionDefinition {
  if (typeof entry !== 'object' || entry === null) return false
  return '$ingest' in entry && typeof (entry as CollectionDefinition).$ingest === 'string'
}

/**
 * Check if an entry is a regular Collection (no $from, no $ingest)
 */
export function isRegularCollection(entry: unknown): entry is CollectionDefinition {
  if (typeof entry !== 'object' || entry === null) return false
  return !('$from' in entry) && !('$ingest' in entry)
}

/**
 * Check if a value is a valid RefreshMode
 */
export function isRefreshMode(value: unknown): value is RefreshMode {
  return value === 'streaming' || value === 'scheduled' || value === 'manual'
}

/**
 * Check if a value is a valid MVStatus
 */
export function isMVStatus(value: unknown): value is MVStatus {
  return (
    value === 'creating' ||
    value === 'ready' ||
    value === 'refreshing' ||
    value === 'stale' ||
    value === 'error' ||
    value === 'disabled'
  )
}

/**
 * Check if a string is a valid view name
 *
 * View names must:
 * - Be non-empty
 * - Start with a letter or underscore
 * - Contain only alphanumeric characters and underscores
 */
export function isValidViewName(name: string): boolean {
  if (!name || typeof name !== 'string') return false
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)
}

/**
 * Check if a cron expression is valid (basic validation)
 *
 * Basic format: minute hour day-of-month month day-of-week
 */
export function isValidCronExpression(cron: string): boolean {
  if (!cron || typeof cron !== 'string') return false
  const parts = cron.trim().split(/\s+/)
  // Cron should have 5 parts (standard) or 6 parts (with seconds)
  return parts.length >= 5 && parts.length <= 6
}

/**
 * Check if an MV is an aggregation view (has $groupBy or $compute)
 */
export function isAggregationMV(mv: MVDefinition): boolean {
  return Boolean(mv.$groupBy || mv.$compute)
}

/**
 * Check if an MV is a projection view (has $select)
 */
export function isProjectionMV(mv: MVDefinition): boolean {
  return Boolean(mv.$select)
}

/**
 * Check if an MV is a filter view (only has $filter)
 */
export function isFilterMV(mv: MVDefinition): boolean {
  return Boolean(mv.$filter) && !mv.$groupBy && !mv.$compute && !mv.$select
}

/**
 * Check if an MV is a denormalization view (has $expand)
 */
export function isDenormalizationMV(mv: MVDefinition): boolean {
  return Boolean(mv.$expand)
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validation error for MV definitions
 */
export interface MVValidationError {
  field: string
  message: string
}

/**
 * Validate an MV definition and return errors
 */
export function validateMVDefinition(name: string, def: MVDefinition): MVValidationError[] {
  const errors: MVValidationError[] = []

  // Validate name
  if (!name) {
    errors.push({ field: 'name', message: 'Name is required' })
  } else if (!isValidViewName(name)) {
    errors.push({
      field: 'name',
      message: 'Name must start with a letter or underscore and contain only alphanumeric characters and underscores',
    })
  }

  // Validate $from (required)
  if (!def.$from) {
    errors.push({ field: '$from', message: '$from (source collection) is required' })
  } else if (typeof def.$from !== 'string') {
    errors.push({ field: '$from', message: '$from must be a string' })
  }

  // Validate $expand
  if (def.$expand !== undefined) {
    if (!Array.isArray(def.$expand)) {
      errors.push({ field: '$expand', message: '$expand must be an array of strings' })
    } else if (def.$expand.some(e => typeof e !== 'string')) {
      errors.push({ field: '$expand', message: 'All $expand entries must be strings' })
    }
  }

  // Validate $flatten
  if (def.$flatten !== undefined && typeof def.$flatten !== 'object') {
    errors.push({ field: '$flatten', message: '$flatten must be an object mapping prefixes to aliases' })
  }

  // Validate $groupBy
  if (def.$groupBy !== undefined) {
    if (!Array.isArray(def.$groupBy)) {
      errors.push({ field: '$groupBy', message: '$groupBy must be an array' })
    }
  }

  // Validate $refresh
  if (def.$refresh !== undefined) {
    if (typeof def.$refresh !== 'object') {
      errors.push({ field: '$refresh', message: '$refresh must be an object' })
    } else {
      // Validate mode
      if (def.$refresh.mode && !isRefreshMode(def.$refresh.mode)) {
        errors.push({
          field: '$refresh.mode',
          message: 'Refresh mode must be streaming, scheduled, or manual',
        })
      }

      // Validate schedule for scheduled mode
      if (def.$refresh.mode === 'scheduled') {
        if (!def.$refresh.schedule) {
          errors.push({
            field: '$refresh.schedule',
            message: 'Schedule is required for scheduled refresh mode',
          })
        } else if (!isValidCronExpression(def.$refresh.schedule)) {
          errors.push({
            field: '$refresh.schedule',
            message: 'Invalid cron expression',
          })
        }
      }
    }
  }

  return errors
}

// =============================================================================
// Default Values
// =============================================================================

/**
 * Default refresh configuration
 */
export const DEFAULT_REFRESH_CONFIG: RefreshConfig = {
  mode: 'streaming',
  strategy: 'replace',
  timezone: 'UTC',
}

/**
 * Apply defaults to an MV definition
 */
export function applyMVDefaults(def: MVDefinition): MVDefinition {
  return {
    ...def,
    $refresh: {
      ...DEFAULT_REFRESH_CONFIG,
      ...def.$refresh,
    },
  }
}

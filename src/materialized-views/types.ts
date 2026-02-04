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
import type { IngestSource, KnownIngestSource, CustomIngestSource } from './ingest-source'
import {
  KNOWN_INGEST_SOURCES,
  customIngestSource,
  isKnownIngestSource,
  isCustomIngestSource,
  isIngestSource,
  getCustomSourceHandler,
} from './ingest-source'

// Re-export cron validation from cron.ts
export { validateCronExpression, type CronValidationResult } from './cron'

// Re-export IngestSource types from the canonical source
export type { IngestSource, KnownIngestSource, CustomIngestSource }
export {
  KNOWN_INGEST_SOURCES,
  customIngestSource,
  isKnownIngestSource,
  isCustomIngestSource,
  isIngestSource,
  getCustomSourceHandler,
}

// =============================================================================
// Branded Types
// =============================================================================

/**
 * Materialized view unique identifier (branded type)
 *
 * MVId is a unique, immutable identifier for a materialized view instance.
 * Used for internal tracking and referencing specific view versions.
 *
 * @example
 * const id = mvId('mv_01HXYZ...')  // ULID or UUID format
 */
export type MVId = string & { readonly __brand: unique symbol }

/** Create an MVId from a string */
export function mvId(id: string): MVId {
  return id as MVId
}

/**
 * Materialized view name (branded type)
 *
 * ViewName is the human-readable name used to reference a view in queries
 * and API calls. Must be a valid identifier (alphanumeric + underscore).
 *
 * @example
 * const name = viewName('active_users')
 * const name = viewName('DailySales')
 */
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
 * - 'full': Rebuild the entire view from source (for scheduled/manual modes)
 * - 'incremental': Apply only changed data (efficient delta updates)
 * - 'streaming': Real-time updates as changes occur
 * - 'replace': Alias for 'full' - atomically replace the entire view
 * - 'append': Append new data (for INSERT-only workloads)
 */
export type RefreshStrategy = 'full' | 'incremental' | 'streaming' | 'replace' | 'append'

/**
 * Refresh strategy enum-like object for programmatic access
 */
export const RefreshStrategy = {
  Full: 'full' as const,
  Incremental: 'incremental' as const,
  Streaming: 'streaming' as const,
  Replace: 'replace' as const,
  Append: 'append' as const,
} as const

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
  schedule?: string | undefined

  /**
   * Refresh strategy
   * @default 'replace'
   */
  strategy?: RefreshStrategy | undefined

  /**
   * Grace period for stale data (allows querying stale MV)
   * @example '15m' - Allow 15 minutes of staleness
   */
  gracePeriod?: string | undefined

  /**
   * Timezone for cron schedule
   * @default 'UTC'
   */
  timezone?: string | undefined

  /**
   * Override storage backend for the MV
   * By default, uses the same backend as source tables
   */
  backend?: 'native' | 'iceberg' | 'delta' | undefined
}

// =============================================================================
// Aggregate Expressions
// =============================================================================

/**
 * Aggregate function expressions for $compute
 */
export interface AggregateExpr {
  $count?: '*' | string | undefined
  $sum?: string | ConditionalExpr | undefined
  $avg?: string | ConditionalExpr | undefined
  $min?: string | undefined
  $max?: string | undefined
  $first?: string | undefined
  $last?: string | undefined
}

/**
 * Conditional expression for computed aggregates
 */
export interface ConditionalExpr {
  $cond: [Filter | [string, unknown], number, number]
}

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
  $type?: string | undefined

  /** Field definitions (IceType format) */
  [field: string]: unknown

  /** Stream ingest source (makes this a stream collection) */
  $ingest?: IngestSource | undefined
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
  $expand?: string[] | undefined

  /**
   * Rename expanded field prefixes
   * @example { 'customer': 'buyer' } // customer_name → buyer_name
   */
  $flatten?: Record<string, string> | undefined

  /**
   * Filter to apply to source data
   */
  $filter?: Filter | undefined

  /**
   * Fields to select (for projection views)
   * Maps output field names to source expressions
   */
  $select?: Record<string, string> | undefined

  /**
   * Group by dimensions (for aggregation views)
   */
  $groupBy?: GroupBySpec[] | undefined

  /**
   * Computed aggregates
   */
  $compute?: Record<string, AggregateExpr> | undefined

  /**
   * Unnest an array field (flatten array to rows)
   */
  $unnest?: string | undefined

  /**
   * Refresh configuration
   * @default { mode: 'streaming' }
   */
  $refresh?: RefreshConfig | undefined
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
 * Storage-layer view state
 *
 * - 'pending': View has been created but not yet built
 * - 'building': View is being built for the first time
 * - 'ready': View is ready for queries
 * - 'stale': View data is outdated
 * - 'error': View is in an error state
 * - 'disabled': View has been disabled
 */
export type ViewState =
  | 'pending'
  | 'building'
  | 'ready'
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
  lastRefreshedAt?: Date | undefined

  /** Duration of the last refresh in milliseconds */
  lastRefreshDurationMs?: number | undefined

  /** Next scheduled refresh time (for scheduled views) */
  nextRefreshAt?: Date | undefined

  /** Number of rows in the view */
  rowCount?: number | undefined

  /** Size of the view in bytes */
  sizeBytes?: number | undefined

  /** Version number (incremented on each refresh) */
  version: number

  /** Lineage for staleness detection */
  lineage: MVLineage

  /** Error message if status is 'error' */
  errorMessage?: string | undefined

  /** Custom metadata */
  meta?: Record<string, unknown> | undefined
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

/**
 * Statistics for a materialized view (storage layer alias)
 */
export type ViewStats = MVStats

// =============================================================================
// Storage Layer Types
// =============================================================================

/**
 * Aggregation pipeline stage
 */
export interface PipelineStage {
  /** Stage type */
  $match?: Filter | undefined
  $group?: Record<string, unknown> | undefined
  $project?: Record<string, unknown> | undefined
  $sort?: Record<string, 1 | -1> | undefined
  $limit?: number | undefined
  $skip?: number | undefined
}

/**
 * Query specification for storage layer
 *
 * Represents the query/filter to apply when building the view.
 */
export interface ViewQuery {
  /** Filter to apply to source data */
  filter?: Filter | undefined

  /** Projection/field selection (1 = include, 0 = exclude) */
  project?: Record<string, 0 | 1> | undefined

  /** Sort specification (1 = ascending, -1 = descending) */
  sort?: Record<string, 1 | -1> | undefined

  /** Aggregation pipeline (for complex transformations) */
  pipeline?: PipelineStage[] | undefined
}

/**
 * Schedule configuration for storage layer
 */
export interface ViewSchedule {
  /** Cron expression for scheduled refresh */
  cron: string
}

/**
 * Options for storage layer view
 *
 * Configuration for how the view is refreshed and maintained.
 */
export interface ViewOptions {
  /** Refresh mode: 'manual', 'streaming', or 'scheduled' */
  refreshMode: 'manual' | 'streaming' | 'scheduled'

  /** Schedule configuration (required for scheduled mode) */
  schedule?: ViewSchedule | { cron?: string | undefined; intervalMs?: number | undefined; timezone?: string | undefined } | undefined

  /** Maximum staleness in milliseconds (for streaming mode) */
  maxStalenessMs?: number | undefined

  /** Refresh strategy: 'full' or 'incremental' */
  refreshStrategy?: 'full' | 'incremental' | 'streaming' | undefined

  /** Whether to populate view data on creation */
  populateOnCreate?: boolean | undefined

  /** Indexes to create on the view */
  indexes?: string[] | undefined

  /** Description of the view */
  description?: string | undefined

  /** Tags for categorization */
  tags?: string[] | undefined

  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined

  /** Grace period in milliseconds before view becomes stale */
  gracePeriod?: number | undefined
}

/**
 * Storage layer view definition
 *
 * This is the format used by MVStorageManager for persisting view definitions.
 * It differs from MVDefinition which is the declarative user-facing syntax.
 */
export interface ViewDefinition {
  /** View name */
  name: ViewName

  /** Source collection name */
  source: string

  /** Query specification */
  query: ViewQuery

  /** View options */
  options: ViewOptions
}

/**
 * Storage layer view metadata
 *
 * Tracks the state and configuration of a stored view.
 */
export interface ViewMetadata {
  /** View definition */
  definition: ViewDefinition

  /** Current state of the view */
  state: ViewState

  /** When the view was created */
  createdAt: Date

  /** When the view was last refreshed */
  lastRefreshedAt?: Date | undefined

  /** Duration of the last refresh in milliseconds */
  lastRefreshDurationMs?: number | undefined

  /** Next scheduled refresh time */
  nextRefreshAt?: Date | undefined

  /** Version number (incremented on each refresh) */
  version: number

  /** Number of documents in the view */
  documentCount?: number | undefined

  /** Size of the view in bytes */
  sizeBytes?: number | undefined

  /** Error message if state is 'error' */
  error?: string | undefined
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
 * Check if a value is a valid RefreshStrategy
 */
export function isRefreshStrategy(value: unknown): value is RefreshStrategy {
  return (
    value === 'full' ||
    value === 'incremental' ||
    value === 'streaming' ||
    value === 'replace' ||
    value === 'append'
  )
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
 * Check if a value is a valid ViewState
 */
export function isViewState(value: unknown): value is ViewState {
  return (
    value === 'pending' ||
    value === 'building' ||
    value === 'ready' ||
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
 * Check if a cron expression is valid
 *
 * Standard format: minute hour day-of-month month day-of-week
 *
 * Validates both structure and field ranges:
 * - minute: 0-59
 * - hour: 0-23
 * - day of month: 1-31
 * - month: 1-12
 * - day of week: 0-6 (0 = Sunday)
 */
export function isValidCronExpression(cron: string): boolean {
  if (!cron || typeof cron !== 'string') return false
  const parts = cron.trim().split(/\s+/)
  // Cron should have exactly 5 parts (standard format)
  if (parts.length !== 5) return false

  const ranges: [number, number][] = [
    [0, 59],   // minute
    [0, 23],   // hour
    [1, 31],   // day of month
    [1, 12],   // month
    [0, 6],    // day of week
  ]

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]!
    const [min, max] = ranges[i]!
    if (!isValidCronField(part, min, max)) return false
  }

  return true
}

/**
 * Check if a single cron field is valid
 */
function isValidCronField(field: string, min: number, max: number): boolean {
  if (field === '*') return true

  // Handle step values (e.g., */15, 0-30/5)
  if (field.includes('/')) {
    const [range, stepStr] = field.split('/')
    if (!stepStr || stepStr === '') return false
    const step = Number(stepStr)
    if (isNaN(step) || step <= 0 || !Number.isInteger(step)) return false
    if (range !== '*' && !isValidCronField(range!, min, max)) return false
    return true
  }

  // Handle lists (e.g., 1,3,5)
  if (field.includes(',')) {
    return field.split(',').every((part) => isValidCronField(part, min, max))
  }

  // Handle ranges (e.g., 1-5)
  if (field.includes('-')) {
    const [startStr, endStr] = field.split('-')
    if (!startStr || !endStr) return false
    const start = Number(startStr)
    const end = Number(endStr)
    if (isNaN(start) || isNaN(end)) return false
    if (!Number.isInteger(start) || !Number.isInteger(end)) return false
    if (start < min || start > max) return false
    if (end < min || end > max) return false
    if (start > end) return false
    return true
  }

  // Single value
  const num = Number(field)
  if (isNaN(num) || !Number.isInteger(num)) return false
  return num >= min && num <= max
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

/**
 * Check if a value is a valid ViewDefinition (storage layer format)
 */
export function isValidViewDefinition(value: unknown): value is ViewDefinition {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  if (typeof v.name !== 'string') return false
  if (typeof v.source !== 'string') return false
  if (typeof v.query !== 'object' || v.query === null) return false
  if (typeof v.options !== 'object' || v.options === null) return false
  return true
}

/**
 * Check if a query is a pipeline query (has non-empty pipeline array)
 */
export function isPipelineQuery(query: ViewQuery): boolean {
  return Array.isArray(query.pipeline) && query.pipeline.length > 0
}

/**
 * Check if a query is a simple query (no pipeline, uses filter/project/sort)
 */
export function isSimpleQuery(query: ViewQuery): boolean {
  return !isPipelineQuery(query)
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
 * Validation error alias for ViewDefinition validation
 */
export type ViewValidationError = MVValidationError

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

// =============================================================================
// Storage Layer Types - Extended
// =============================================================================

/**
 * Schedule options for scheduled refresh views
 */
export interface ScheduleOptions {
  /** Cron expression for scheduling */
  cron?: string | undefined
  /** Interval in milliseconds (alternative to cron) */
  intervalMs?: number | undefined
  /** Timezone for cron schedule */
  timezone?: string | undefined
}

/**
 * Extended view options with full support for all options
 */
export interface ExtendedViewOptions extends ViewOptions {
  /** View description */
  description?: string | undefined
  /** Tags for categorization */
  tags?: string[] | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
  /** Indexes to create on the view */
  indexes?: string[] | undefined
  /** Whether to populate view on creation */
  populateOnCreate?: boolean | undefined
}

/**
 * Default view options
 */
export const DEFAULT_VIEW_OPTIONS: Required<Pick<ExtendedViewOptions, 'refreshMode' | 'refreshStrategy' | 'populateOnCreate'>> = {
  refreshMode: 'manual',
  refreshStrategy: 'full',
  populateOnCreate: false,
}

// =============================================================================
// defineView API
// =============================================================================

/**
 * Input for defineView API
 */
export interface DefineViewInput {
  /** View name */
  name: string
  /** Source collection */
  source: string
  /** Query specification */
  query: ViewQuery
  /** View options */
  options?: Partial<ExtendedViewOptions> | undefined
}

/**
 * Result of defineView API
 */
export interface DefineViewResult {
  /** Whether the view definition is valid */
  success: boolean
  /** The validated view definition (if successful) */
  definition?: ViewDefinition & { options: ExtendedViewOptions } | undefined
  /** Validation errors (if unsuccessful) */
  errors?: ViewValidationError[] | undefined
}

/**
 * Validate a ViewDefinition and return errors
 */
export function validateViewDefinition(def: Partial<ViewDefinition & { options?: Partial<ExtendedViewOptions> | undefined }>): ViewValidationError[] {
  const errors: ViewValidationError[] = []

  // Validate name
  if (!def.name) {
    errors.push({ field: 'name', message: 'Name is required' })
  } else if (!isValidViewName(def.name as string)) {
    errors.push({
      field: 'name',
      message: 'Name must start with a letter or underscore and contain only alphanumeric characters and underscores',
    })
  }

  // Validate source
  if (!def.source) {
    errors.push({ field: 'source', message: 'Source collection is required' })
  } else if (typeof def.source !== 'string') {
    errors.push({ field: 'source', message: 'Source must be a string' })
  }

  // Validate query
  if (!def.query) {
    errors.push({ field: 'query', message: 'Query is required' })
  } else if (typeof def.query !== 'object') {
    errors.push({ field: 'query', message: 'Query must be an object' })
  }

  // Validate options
  if (!def.options) {
    errors.push({ field: 'options', message: 'Options are required' })
  } else if (typeof def.options !== 'object') {
    errors.push({ field: 'options', message: 'Options must be an object' })
  } else {
    // Validate refresh mode
    if (def.options.refreshMode && !isRefreshMode(def.options.refreshMode)) {
      errors.push({
        field: 'options.refreshMode',
        message: 'Refresh mode must be streaming, scheduled, or manual',
      })
    }

    // Validate refresh strategy
    if (def.options.refreshStrategy && !isRefreshStrategy(def.options.refreshStrategy)) {
      errors.push({
        field: 'options.refreshStrategy',
        message: 'Refresh strategy must be full, incremental, or streaming',
      })
    }

    // Validate schedule for scheduled mode
    if (def.options.refreshMode === 'scheduled') {
      if (!def.options.schedule) {
        errors.push({
          field: 'options.schedule',
          message: 'Schedule is required for scheduled refresh mode',
        })
      } else {
        const schedule = def.options.schedule as ScheduleOptions
        if (!schedule.cron && !schedule.intervalMs) {
          errors.push({
            field: 'options.schedule',
            message: 'Schedule must specify cron or intervalMs',
          })
        }
        if (schedule.cron && !isValidCronExpression(schedule.cron)) {
          errors.push({
            field: 'options.schedule.cron',
            message: 'Invalid cron expression',
          })
        }
        if (schedule.intervalMs !== undefined && (typeof schedule.intervalMs !== 'number' || schedule.intervalMs <= 0)) {
          errors.push({
            field: 'options.schedule.intervalMs',
            message: 'Interval must be a positive number',
          })
        }
      }
    }
  }

  return errors
}

/**
 * Define a materialized view with validation and defaults
 */
export function defineView(input: DefineViewInput): DefineViewResult {
  // Apply defaults
  const options: ExtendedViewOptions = {
    ...DEFAULT_VIEW_OPTIONS,
    ...input.options,
  }

  const def = {
    name: viewName(input.name),
    source: input.source,
    query: input.query,
    options,
  }

  // Validate
  const errors = validateViewDefinition(def)

  if (errors.length > 0) {
    return { success: false, errors }
  }

  return {
    success: true,
    definition: def as ViewDefinition & { options: ExtendedViewOptions },
  }
}

// =============================================================================
// MaterializedViewDefinition (Legacy API format)
// =============================================================================

/**
 * Materialized View Definition (alternative format for conversion)
 *
 * This format is used for programmatic MV definitions that can be
 * converted to/from the storage layer ViewDefinition format.
 */
export interface MaterializedViewDefinition {
  /** View name */
  name: string
  /** Description */
  description?: string | undefined
  /** Source collection */
  source: string
  /** Filter to apply */
  filter?: Filter | undefined
  /** Projection/field selection */
  project?: Record<string, 0 | 1> | undefined
  /** Sort specification */
  sort?: Record<string, 1 | -1> | undefined
  /** Aggregation pipeline */
  pipeline?: PipelineStage[] | undefined
  /** Refresh strategy */
  refreshStrategy: 'full' | 'incremental' | 'streaming'
  /** Refresh mode */
  refreshMode?: RefreshMode | undefined
  /** Schedule for refresh */
  schedule?: ScheduleOptions | undefined
  /** Maximum staleness in milliseconds */
  maxStalenessMs?: number | undefined
  /** Whether to populate on create */
  populateOnCreate?: boolean | undefined
  /** Indexes to create */
  indexes?: string[] | undefined
  /** Dependencies (other views this depends on) */
  dependencies?: string[] | undefined
  /** Custom metadata */
  meta?: Record<string, unknown> | undefined
}

/**
 * Check if a value is a valid MaterializedViewDefinition
 */
export function isValidMaterializedViewDefinition(value: unknown): value is MaterializedViewDefinition {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  if (typeof v.name !== 'string') return false
  if (typeof v.source !== 'string') return false
  if (!v.refreshStrategy || !isRefreshStrategy(v.refreshStrategy)) return false
  return true
}

/**
 * Convert MaterializedViewDefinition to ViewDefinition (storage format)
 */
export function toViewDefinition(mvDef: MaterializedViewDefinition): ViewDefinition & { options: ExtendedViewOptions } {
  const query: ViewQuery = {}

  if (mvDef.filter) {
    query.filter = mvDef.filter
  }
  if (mvDef.project) {
    query.project = mvDef.project
  }
  if (mvDef.sort) {
    query.sort = mvDef.sort
  }
  if (mvDef.pipeline) {
    query.pipeline = mvDef.pipeline
  }

  const options: ExtendedViewOptions = {
    refreshMode: mvDef.refreshMode || 'manual',
    refreshStrategy: mvDef.refreshStrategy,
    maxStalenessMs: mvDef.maxStalenessMs,
    populateOnCreate: mvDef.populateOnCreate,
    indexes: mvDef.indexes,
    description: mvDef.description,
    metadata: mvDef.meta,
  }

  if (mvDef.schedule) {
    options.schedule = mvDef.schedule
  }

  return {
    name: viewName(mvDef.name),
    source: mvDef.source,
    query,
    options,
  }
}

/**
 * Convert ViewDefinition (storage format) to MaterializedViewDefinition
 */
export function fromViewDefinition(viewDef: ViewDefinition & { options?: Partial<ExtendedViewOptions> | undefined }): MaterializedViewDefinition {
  const mvDef: MaterializedViewDefinition = {
    name: viewDef.name as string,
    source: viewDef.source,
    refreshStrategy: (viewDef.options?.refreshStrategy || 'full') as 'full' | 'incremental' | 'streaming',
  }

  // Copy query fields
  if (viewDef.query.filter) {
    mvDef.filter = viewDef.query.filter
  }
  if (viewDef.query.project) {
    mvDef.project = viewDef.query.project
  }
  if (viewDef.query.sort) {
    mvDef.sort = viewDef.query.sort
  }
  if (viewDef.query.pipeline) {
    mvDef.pipeline = viewDef.query.pipeline
  }

  // Copy options
  if (viewDef.options) {
    if (viewDef.options.refreshMode) {
      mvDef.refreshMode = viewDef.options.refreshMode
    }
    if (viewDef.options.schedule) {
      mvDef.schedule = viewDef.options.schedule as ScheduleOptions
    }
    if (viewDef.options.maxStalenessMs) {
      mvDef.maxStalenessMs = viewDef.options.maxStalenessMs
    }
    if (viewDef.options.description) {
      mvDef.description = viewDef.options.description
    }
    if (viewDef.options.indexes) {
      mvDef.indexes = viewDef.options.indexes
    }
    if (viewDef.options.metadata) {
      mvDef.meta = viewDef.options.metadata
    }
    if (viewDef.options.populateOnCreate !== undefined) {
      mvDef.populateOnCreate = viewDef.options.populateOnCreate
    }
  }

  return mvDef
}

// =============================================================================
// Stream Processor Types (shared between stream-processor.ts and stream-persistence.ts)
// =============================================================================

/**
 * Represents a batch of records that failed to write.
 * Used by StreamProcessor for retry logic and dead-letter queue.
 */
export interface FailedBatch<T> {
  /** Records that failed to write */
  records: T[]

  /** Batch number */
  batchNumber: number

  /** File path that was attempted */
  filePath: string

  /** The error that caused the failure */
  error: Error

  /** Timestamp when failure occurred */
  failedAt: number

  /** Number of retry attempts made */
  attempts: number
}

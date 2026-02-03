/**
 * defineView() API for ParqueDB Materialized Views
 *
 * Provides a simple API for creating materialized view definitions.
 * MVs are identified by the presence of `$from` - no special directive needed.
 *
 * Stream collections use `$ingest` to wire up automatic data ingestion.
 */

import type {
  MVDefinition,
  MVValidationError,
  RefreshConfig,
  RefreshMode,
  GroupBySpec,
  AggregateExpr,
  CollectionDefinition,
  IngestSource,
} from './types'
import {
  isMVDefinition,
  isValidViewName,
  isValidCronExpression,
  isRefreshMode,
  validateMVDefinition,
  applyMVDefaults,
  DEFAULT_REFRESH_CONFIG,
} from './types'
import type { Filter } from '../types/filter'

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error codes for MV definition validation errors
 */
export type MVDefinitionErrorCode =
  | 'INVALID_NAME'
  | 'MISSING_FROM'
  | 'INVALID_FROM'
  | 'INVALID_EXPAND'
  | 'INVALID_FLATTEN'
  | 'INVALID_FILTER'
  | 'INVALID_GROUP_BY'
  | 'INVALID_COMPUTE'
  | 'INVALID_REFRESH_MODE'
  | 'INVALID_SCHEDULE'
  | 'MISSING_SCHEDULE'

/**
 * Error thrown when MV definition validation fails
 */
export class MVDefinitionError extends Error {
  readonly code: MVDefinitionErrorCode
  readonly field: string

  constructor(code: MVDefinitionErrorCode, field: string, message: string) {
    super(message)
    this.name = 'MVDefinitionError'
    this.code = code
    this.field = field
  }
}

// =============================================================================
// Validation Helpers
// =============================================================================

/**
 * Validate MV name and throw if invalid
 */
function validateNameStrict(name: string): void {
  if (!name || typeof name !== 'string') {
    throw new MVDefinitionError(
      'INVALID_NAME',
      'name',
      'View name is required and must be a non-empty string'
    )
  }

  if (name.startsWith('_')) {
    throw new MVDefinitionError(
      'INVALID_NAME',
      'name',
      'View name cannot start with underscore (reserved for system views)'
    )
  }

  if (!isValidViewName(name)) {
    throw new MVDefinitionError(
      'INVALID_NAME',
      'name',
      'View name must start with a letter and contain only alphanumeric characters or underscores'
    )
  }
}

/**
 * Validate $from and throw if invalid
 */
function validateFromStrict(from: string | undefined): void {
  if (!from) {
    throw new MVDefinitionError(
      'MISSING_FROM',
      '$from',
      '$from (source collection) is required for materialized views'
    )
  }

  if (typeof from !== 'string') {
    throw new MVDefinitionError(
      'INVALID_FROM',
      '$from',
      '$from must be a string (source collection name)'
    )
  }
}

/**
 * Validate $refresh and throw if invalid
 */
function validateRefreshStrict(refresh: RefreshConfig | undefined): void {
  if (!refresh) return

  if (refresh.mode && !isRefreshMode(refresh.mode)) {
    throw new MVDefinitionError(
      'INVALID_REFRESH_MODE',
      '$refresh.mode',
      `Invalid refresh mode: "${refresh.mode}". Must be "streaming", "scheduled", or "manual"`
    )
  }

  if (refresh.mode === 'scheduled') {
    if (!refresh.schedule) {
      throw new MVDefinitionError(
        'MISSING_SCHEDULE',
        '$refresh.schedule',
        'Schedule is required when refresh mode is "scheduled"'
      )
    }

    if (!isValidCronExpression(refresh.schedule)) {
      throw new MVDefinitionError(
        'INVALID_SCHEDULE',
        '$refresh.schedule',
        `Invalid cron expression: "${refresh.schedule}". Expected format: "minute hour day month weekday"`
      )
    }
  }
}

// =============================================================================
// Public API: defineView
// =============================================================================

/**
 * Input for defineView - the MV definition without name (name is passed separately)
 */
export interface DefineViewInput {
  /** Source collection name (REQUIRED) */
  $from: string

  /** Relations to expand (denormalize) */
  $expand?: string[]

  /** Rename expanded field prefixes */
  $flatten?: Record<string, string>

  /** Filter to apply to source data */
  $filter?: Filter

  /** Fields to select */
  $select?: Record<string, string>

  /** Group by dimensions */
  $groupBy?: GroupBySpec[]

  /** Computed aggregates */
  $compute?: Record<string, AggregateExpr>

  /** Unnest an array field */
  $unnest?: string

  /** Refresh configuration */
  $refresh?: RefreshConfig
}

/**
 * Define a materialized view
 *
 * Creates a type-safe MV definition that can be used in a DB schema.
 * Validates the definition and applies defaults for optional fields.
 *
 * @param input - MV definition with $from and optional directives
 * @returns A validated MVDefinition with defaults applied
 * @throws {MVDefinitionError} If validation fails
 *
 * @example
 * // Simple denormalized view
 * export const OrderAnalytics = defineView({
 *   $from: 'Order',
 *   $expand: ['customer', 'items.product'],
 *   $flatten: { 'customer': 'buyer' },
 * })
 *
 * @example
 * // Aggregated daily sales with scheduled refresh
 * export const DailySales = defineView({
 *   $from: 'Order',
 *   $groupBy: [{ date: '$createdAt' }, 'status'],
 *   $compute: {
 *     orderCount: { $count: '*' },
 *     revenue: { $sum: 'total' },
 *   },
 *   $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
 * })
 *
 * @example
 * // Filtered view from stream collection
 * export const WorkerErrors = defineView({
 *   $from: 'TailEvents',
 *   $filter: { outcome: { $ne: 'ok' } },
 * })
 *
 * @example
 * // Use in DB schema
 * const db = DB({
 *   Order: { ... },
 *   OrderAnalytics,  // Include the MV
 *   DailySales,
 * })
 */
export function defineView(input: DefineViewInput): MVDefinition {
  // Validate
  validateFromStrict(input.$from)
  validateRefreshStrict(input.$refresh)

  // Build definition
  const definition: MVDefinition = {
    $from: input.$from,
  }

  // Copy optional directives
  if (input.$expand) definition.$expand = input.$expand
  if (input.$flatten) definition.$flatten = input.$flatten
  if (input.$filter) definition.$filter = input.$filter
  if (input.$select) definition.$select = input.$select
  if (input.$groupBy) definition.$groupBy = input.$groupBy
  if (input.$compute) definition.$compute = input.$compute
  if (input.$unnest) definition.$unnest = input.$unnest

  // Apply refresh defaults
  definition.$refresh = {
    ...DEFAULT_REFRESH_CONFIG,
    ...input.$refresh,
  }

  return definition
}

// =============================================================================
// Public API: defineCollection
// =============================================================================

/**
 * Define a stream collection with $ingest
 *
 * Creates a collection definition that automatically ingests data
 * from an external source (AI SDK, tail events, evalite, etc.).
 *
 * @param type - Entity type name
 * @param fields - Field definitions (IceType format)
 * @param ingestSource - The ingest source identifier
 * @returns A CollectionDefinition with $ingest wired up
 *
 * @example
 * // Define a stream collection for AI requests
 * export const AIRequests = defineCollection('AIRequest', {
 *   modelId: 'string!',
 *   tokens: 'int?',
 *   latencyMs: 'int!',
 *   timestamp: 'timestamp!',
 * }, 'ai-sdk')
 *
 * @example
 * // Use in DB schema
 * const db = DB({
 *   AIRequests,  // Include the stream collection
 *   DailyAIUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }],
 *     $compute: { count: { $count: '*' } },
 *   },
 * })
 */
export function defineCollection(
  type: string,
  fields: Record<string, unknown>,
  ingestSource?: IngestSource
): CollectionDefinition {
  const definition: CollectionDefinition = {
    $type: type,
    ...fields,
  }

  if (ingestSource) {
    definition.$ingest = ingestSource
  }

  return definition
}

// =============================================================================
// Builder API
// =============================================================================

/**
 * Type-safe builder for MV definitions
 *
 * Alternative to defineView() for more complex configuration scenarios
 * or when building MVs programmatically.
 *
 * @example
 * const view = new MVBuilder('Order')
 *   .expand(['customer', 'items.product'])
 *   .flatten({ 'customer': 'buyer' })
 *   .scheduled('0 * * * *')
 *   .build()
 */
export class MVBuilder {
  private _from: string
  private _expand?: string[]
  private _flatten?: Record<string, string>
  private _filter?: Filter
  private _select?: Record<string, string>
  private _groupBy?: GroupBySpec[]
  private _compute?: Record<string, AggregateExpr>
  private _unnest?: string
  private _refresh: RefreshConfig = { ...DEFAULT_REFRESH_CONFIG }

  constructor(from: string) {
    this._from = from
  }

  /**
   * Set relations to expand (denormalize)
   */
  expand(relations: string[]): this {
    this._expand = relations
    return this
  }

  /**
   * Set prefix renames for expanded fields
   */
  flatten(mapping: Record<string, string>): this {
    this._flatten = mapping
    return this
  }

  /**
   * Set filter for source data
   */
  filter(filter: Filter): this {
    this._filter = filter
    return this
  }

  /**
   * Set fields to select
   */
  select(fields: Record<string, string>): this {
    this._select = fields
    return this
  }

  /**
   * Set group by dimensions
   */
  groupBy(specs: GroupBySpec[]): this {
    this._groupBy = specs
    return this
  }

  /**
   * Set computed aggregates
   */
  compute(aggregates: Record<string, AggregateExpr>): this {
    this._compute = aggregates
    return this
  }

  /**
   * Set array field to unnest
   */
  unnest(field: string): this {
    this._unnest = field
    return this
  }

  /**
   * Set streaming refresh mode (default)
   */
  streaming(): this {
    this._refresh.mode = 'streaming'
    return this
  }

  /**
   * Set scheduled refresh mode with cron schedule
   */
  scheduled(schedule: string, options?: { strategy?: 'replace' | 'append'; gracePeriod?: string }): this {
    this._refresh.mode = 'scheduled'
    this._refresh.schedule = schedule
    if (options?.strategy) this._refresh.strategy = options.strategy
    if (options?.gracePeriod) this._refresh.gracePeriod = options.gracePeriod
    return this
  }

  /**
   * Set manual refresh mode
   */
  manual(): this {
    this._refresh.mode = 'manual'
    return this
  }

  /**
   * Set refresh strategy
   */
  strategy(strategy: 'replace' | 'append'): this {
    this._refresh.strategy = strategy
    return this
  }

  /**
   * Set grace period for stale data
   */
  gracePeriod(period: string): this {
    this._refresh.gracePeriod = period
    return this
  }

  /**
   * Override storage backend
   */
  backend(backend: 'native' | 'iceberg' | 'delta'): this {
    this._refresh.backend = backend
    return this
  }

  /**
   * Build the MV definition
   */
  build(): MVDefinition {
    return defineView({
      $from: this._from,
      $expand: this._expand,
      $flatten: this._flatten,
      $filter: this._filter,
      $select: this._select,
      $groupBy: this._groupBy,
      $compute: this._compute,
      $unnest: this._unnest,
      $refresh: this._refresh,
    })
  }
}

// =============================================================================
// Schema Helpers
// =============================================================================

/**
 * Result of parsing a schema
 */
export interface ParsedSchema {
  /** Regular collections (no $from, no $ingest) */
  collections: Map<string, CollectionDefinition>

  /** Stream collections (has $ingest) */
  streamCollections: Map<string, CollectionDefinition>

  /** Materialized views (has $from) */
  materializedViews: Map<string, MVDefinition>
}

/**
 * Parse a DB schema and categorize entries
 *
 * Separates collections, stream collections, and MVs based on their directives:
 * - Has `$from` -> Materialized View
 * - Has `$ingest` -> Stream Collection
 * - Neither -> Regular Collection
 *
 * @param schema - The schema object passed to DB()
 * @returns Categorized schema entries
 *
 * @example
 * const schema = {
 *   Customer: { name: 'string!' },           // Collection
 *   TailEvents: { $ingest: 'tail', ... },    // Stream Collection
 *   WorkerErrors: { $from: 'TailEvents' },   // MV
 * }
 *
 * const parsed = parseSchema(schema)
 * // parsed.collections: Map { 'Customer' => { name: 'string!' } }
 * // parsed.streamCollections: Map { 'TailEvents' => { $ingest: 'tail', ... } }
 * // parsed.materializedViews: Map { 'WorkerErrors' => { $from: 'TailEvents' } }
 */
export function parseSchema(schema: Record<string, unknown>): ParsedSchema {
  const collections = new Map<string, CollectionDefinition>()
  const streamCollections = new Map<string, CollectionDefinition>()
  const materializedViews = new Map<string, MVDefinition>()

  for (const [name, entry] of Object.entries(schema)) {
    if (typeof entry !== 'object' || entry === null) continue

    const obj = entry as Record<string, unknown>

    if ('$from' in obj && typeof obj.$from === 'string') {
      // MV (has $from)
      materializedViews.set(name, applyMVDefaults(obj as unknown as MVDefinition))
    } else if ('$ingest' in obj && typeof obj.$ingest === 'string') {
      // Stream collection (has $ingest)
      streamCollections.set(name, obj as CollectionDefinition)
    } else {
      // Regular collection
      collections.set(name, obj as CollectionDefinition)
    }
  }

  return { collections, streamCollections, materializedViews }
}

/**
 * Validate an entire schema and return all errors
 *
 * @param schema - The schema to validate
 * @returns Map of entry name to validation errors (empty array = valid)
 */
export function validateSchema(schema: Record<string, unknown>): Map<string, MVValidationError[]> {
  const errors = new Map<string, MVValidationError[]>()
  const parsed = parseSchema(schema)

  // Validate MVs
  for (const [name, mv] of Array.from(parsed.materializedViews)) {
    const mvErrors = validateMVDefinition(name, mv)

    // Check that $from references a valid source
    const source = mv.$from
    const sourceExists =
      parsed.collections.has(source) ||
      parsed.streamCollections.has(source) ||
      parsed.materializedViews.has(source)

    if (!sourceExists) {
      mvErrors.push({
        field: '$from',
        message: `Source "${source}" not found in schema`,
      })
    }

    if (mvErrors.length > 0) {
      errors.set(name, mvErrors)
    }
  }

  return errors
}

// Re-export relevant types
export type {
  MVDefinition,
  MVValidationError,
  RefreshConfig,
  RefreshMode,
  GroupBySpec,
  AggregateExpr,
  CollectionDefinition,
  IngestSource,
}

export {
  isMVDefinition,
  isValidViewName,
  validateMVDefinition,
  applyMVDefaults,
  DEFAULT_REFRESH_CONFIG,
}

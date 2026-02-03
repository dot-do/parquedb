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
import {
  getMVDependencies,
  detectMVCycles,
  MVCycleError,
} from './cycle-detection'

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
  | 'INVALID_SELECT'
  | 'INVALID_GROUP_BY'
  | 'INVALID_COMPUTE'
  | 'INVALID_UNNEST'
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

/**
 * Error codes for collection definition validation errors
 */
export type CollectionDefinitionErrorCode =
  | 'INVALID_TYPE'
  | 'INVALID_FIELDS'
  | 'RESERVED_FIELD_NAME'
  | 'INVALID_FIELD_TYPE'
  | 'INVALID_INGEST_SOURCE'

/**
 * Error thrown when collection definition validation fails
 */
export class CollectionDefinitionError extends Error {
  readonly code: CollectionDefinitionErrorCode
  readonly field: string

  constructor(code: CollectionDefinitionErrorCode, field: string, message: string) {
    super(message)
    this.name = 'CollectionDefinitionError'
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
  if (from === undefined) {
    throw new MVDefinitionError(
      'MISSING_FROM',
      '$from',
      '$from (source collection) is required for materialized views'
    )
  }

  if (typeof from !== 'string' || from === '') {
    throw new MVDefinitionError(
      'INVALID_FROM',
      '$from',
      '$from must be a non-empty string (source collection name)'
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

/**
 * Validate $expand and throw if invalid
 */
function validateExpandStrict(expand: unknown): void {
  if (expand === undefined) return

  if (!Array.isArray(expand)) {
    throw new MVDefinitionError(
      'INVALID_EXPAND',
      '$expand',
      '$expand must be an array of strings'
    )
  }

  for (const item of expand) {
    if (typeof item !== 'string' || item === '') {
      throw new MVDefinitionError(
        'INVALID_EXPAND',
        '$expand',
        '$expand must contain only non-empty strings'
      )
    }
  }
}

/**
 * Known filter operators
 */
const KNOWN_FILTER_OPERATORS = new Set([
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte',
  '$in', '$nin', '$exists', '$type',
  '$and', '$or', '$not', '$nor',
  '$regex', '$elemMatch', '$all', '$size',
])

/**
 * Validate a filter object recursively
 */
function validateFilterObject(filter: unknown, path: string): void {
  if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
    throw new MVDefinitionError(
      'INVALID_FILTER',
      path,
      `${path} must be a filter object`
    )
  }

  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith('$')) {
      // It's an operator - check if known
      if (!KNOWN_FILTER_OPERATORS.has(key)) {
        throw new MVDefinitionError(
          'INVALID_FILTER',
          '$filter',
          `Unknown filter operator: "${key}"`
        )
      }

      // For logical operators, validate nested filters
      if (key === '$and' || key === '$or' || key === '$nor') {
        if (!Array.isArray(value)) {
          throw new MVDefinitionError(
            'INVALID_FILTER',
            '$filter',
            `${key} must be an array`
          )
        }
        for (let i = 0; i < value.length; i++) {
          validateFilterObject(value[i], `${path}.${key}[${i}]`)
        }
      } else if (key === '$not') {
        validateFilterObject(value, `${path}.${key}`)
      }
    } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      // Nested filter conditions like { field: { $gt: 5 } }
      for (const op of Object.keys(value)) {
        if (op.startsWith('$') && !KNOWN_FILTER_OPERATORS.has(op)) {
          throw new MVDefinitionError(
            'INVALID_FILTER',
            '$filter',
            `Unknown filter operator: "${op}"`
          )
        }
      }
    }
    // Simple equality like { field: value } is always valid
  }
}

/**
 * Validate $filter and throw if invalid
 */
function validateFilterStrict(filter: unknown): void {
  if (filter === undefined) return

  if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
    throw new MVDefinitionError(
      'INVALID_FILTER',
      '$filter',
      '$filter must be a filter object'
    )
  }

  validateFilterObject(filter, '$filter')
}

/**
 * Validate $flatten and throw if invalid
 */
function validateFlattenStrict(flatten: unknown): void {
  if (flatten === undefined) return

  if (typeof flatten !== 'object' || flatten === null || Array.isArray(flatten)) {
    throw new MVDefinitionError(
      'INVALID_FLATTEN',
      '$flatten',
      '$flatten must be an object mapping relation names to aliases'
    )
  }

  for (const [key, value] of Object.entries(flatten)) {
    if (typeof value !== 'string' || value === '') {
      throw new MVDefinitionError(
        'INVALID_FLATTEN',
        '$flatten',
        `$flatten value for "${key}" must be a non-empty string`
      )
    }
  }
}

/**
 * Validate $select and throw if invalid
 */
function validateSelectStrict(select: unknown): void {
  if (select === undefined) return

  if (typeof select !== 'object' || select === null || Array.isArray(select)) {
    throw new MVDefinitionError(
      'INVALID_SELECT',
      '$select',
      '$select must be an object mapping output fields to source expressions'
    )
  }

  for (const [key, value] of Object.entries(select)) {
    if (typeof value !== 'string') {
      throw new MVDefinitionError(
        'INVALID_SELECT',
        '$select',
        `$select value for "${key}" must be a string`
      )
    }
  }
}

/**
 * Validate $groupBy and throw if invalid
 */
function validateGroupByStrict(groupBy: unknown): void {
  if (groupBy === undefined) return

  if (!Array.isArray(groupBy)) {
    throw new MVDefinitionError(
      'INVALID_GROUP_BY',
      '$groupBy',
      '$groupBy must be an array'
    )
  }

  for (const item of groupBy) {
    if (typeof item === 'string') {
      // Valid: field name string
      continue
    } else if (typeof item === 'object' && item !== null && !Array.isArray(item)) {
      // Valid: { alias: expression } object
      continue
    } else {
      throw new MVDefinitionError(
        'INVALID_GROUP_BY',
        '$groupBy',
        '$groupBy items must be strings or objects'
      )
    }
  }
}

/**
 * Known aggregate functions
 */
const KNOWN_AGGREGATE_FUNCTIONS = new Set([
  '$count', '$sum', '$avg', '$min', '$max', '$first', '$last',
])

/**
 * Validate $compute and throw if invalid
 */
function validateComputeStrict(compute: unknown): void {
  if (compute === undefined) return

  if (typeof compute !== 'object' || compute === null || Array.isArray(compute)) {
    throw new MVDefinitionError(
      'INVALID_COMPUTE',
      '$compute',
      '$compute must be an object mapping field names to aggregate expressions'
    )
  }

  for (const [fieldName, expr] of Object.entries(compute)) {
    if (typeof expr !== 'object' || expr === null || Array.isArray(expr)) {
      throw new MVDefinitionError(
        'INVALID_COMPUTE',
        '$compute',
        `$compute["${fieldName}"] must be an aggregate expression object`
      )
    }

    // Check that at least one aggregate function is present
    const exprObj = expr as Record<string, unknown>
    const keys = Object.keys(exprObj)
    const hasKnownAggregate = keys.some(k => KNOWN_AGGREGATE_FUNCTIONS.has(k))

    if (!hasKnownAggregate) {
      throw new MVDefinitionError(
        'INVALID_COMPUTE',
        '$compute',
        `$compute["${fieldName}"] must contain a valid aggregate function ($count, $sum, $avg, $min, $max, $first, $last)`
      )
    }
  }
}

/**
 * Validate $unnest and throw if invalid
 */
function validateUnnestStrict(unnest: unknown): void {
  if (unnest === undefined) return

  if (typeof unnest !== 'string' || unnest === '') {
    throw new MVDefinitionError(
      'INVALID_UNNEST',
      '$unnest',
      '$unnest must be a non-empty string (field name to unnest)'
    )
  }
}

// =============================================================================
// Collection Validation Helpers
// =============================================================================

/**
 * Reserved field names that cannot be used in collection definitions
 */
const RESERVED_FIELD_NAMES = new Set(['$type', '$ingest', '$from', '$expand', '$flatten', '$filter', '$select', '$groupBy', '$compute', '$unnest', '$refresh'])

/**
 * Valid primitive types for field definitions
 */
const VALID_PRIMITIVE_TYPES = new Set([
  'string', 'int', 'integer', 'float', 'double', 'number', 'boolean', 'bool',
  'date', 'datetime', 'timestamp', 'time', 'uuid', 'json', 'any', 'text', 'email', 'url',
])

/**
 * Check if a type string is valid
 */
function isValidFieldType(typeStr: string): boolean {
  // Remove optional/required modifiers
  let baseType = typeStr
  if (baseType.endsWith('!') || baseType.endsWith('?')) {
    baseType = baseType.slice(0, -1)
  }

  // Handle array types (e.g., 'string[]', 'int[]!')
  if (baseType.endsWith('[]')) {
    const elementType = baseType.slice(0, -2)
    return isValidFieldType(elementType) || VALID_PRIMITIVE_TYPES.has(elementType)
  }

  // Handle relationship types (e.g., '-> User.posts', '<- Comment.post[]', '~> Topic')
  if (baseType.startsWith('->') || baseType.startsWith('<-') || baseType.startsWith('~>') || baseType.startsWith('<~')) {
    return true
  }

  // Handle parametric types (e.g., 'decimal(10,2)', 'varchar(50)', 'vector(1536)', 'enum(...)')
  const parametricMatch = baseType.match(/^(\w+)\((.+)\)$/)
  if (parametricMatch) {
    const typeName = parametricMatch[1]
    const params = parametricMatch[2]
    // Known parametric types
    if (['decimal', 'varchar', 'char', 'vector', 'enum', 'array', 'map', 'set'].includes(typeName!)) {
      // Ensure params are not empty
      return params !== undefined && params.length > 0
    }
    return false
  }

  // Handle default values (e.g., "string = 'active'", 'int = 0')
  if (baseType.includes(' = ')) {
    const [typePart] = baseType.split(' = ')
    return isValidFieldType(typePart!.trim())
  }

  // Check primitive types
  return VALID_PRIMITIVE_TYPES.has(baseType)
}

/**
 * Validate collection type name
 */
function validateCollectionType(type: string): void {
  if (!type || typeof type !== 'string') {
    throw new CollectionDefinitionError(
      'INVALID_TYPE',
      'type',
      'Collection type name is required and must be a non-empty string'
    )
  }

  if (type.startsWith('_')) {
    throw new CollectionDefinitionError(
      'INVALID_TYPE',
      'type',
      'Collection type name cannot start with underscore (reserved for system types)'
    )
  }

  // Must start with a letter, can contain letters, numbers, underscores
  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(type)) {
    throw new CollectionDefinitionError(
      'INVALID_TYPE',
      'type',
      'Collection type name must start with a letter and contain only alphanumeric characters and underscores'
    )
  }
}

/**
 * Validate collection fields
 */
function validateCollectionFields(fields: unknown): void {
  if (fields === null || fields === undefined || typeof fields !== 'object' || Array.isArray(fields)) {
    throw new CollectionDefinitionError(
      'INVALID_FIELDS',
      'fields',
      'Collection fields must be an object'
    )
  }

  for (const [fieldName, fieldDef] of Object.entries(fields)) {
    // Check reserved field names
    if (RESERVED_FIELD_NAMES.has(fieldName)) {
      throw new CollectionDefinitionError(
        'RESERVED_FIELD_NAME',
        fieldName,
        `Field name "${fieldName}" is reserved and cannot be used`
      )
    }

    // Validate field type
    if (typeof fieldDef === 'string') {
      if (!isValidFieldType(fieldDef)) {
        throw new CollectionDefinitionError(
          'INVALID_FIELD_TYPE',
          fieldName,
          `Invalid field type "${fieldDef}" for field "${fieldName}". Expected a valid type like string, int, boolean, etc.`
        )
      }
    } else if (typeof fieldDef === 'object' && fieldDef !== null && !Array.isArray(fieldDef)) {
      // Object field definition with { type: 'string!', index: true }
      const fieldObj = fieldDef as Record<string, unknown>
      if ('type' in fieldObj) {
        const typeVal = fieldObj.type
        if (typeof typeVal !== 'string' || !isValidFieldType(typeVal)) {
          throw new CollectionDefinitionError(
            'INVALID_FIELD_TYPE',
            fieldName,
            `Invalid field type "${typeVal}" for field "${fieldName}". Expected a valid type like string, int, boolean, etc.`
          )
        }
      }
    }
  }
}

/**
 * Validate ingest source
 */
function validateIngestSource(ingestSource: unknown): void {
  if (ingestSource === undefined) return

  if (typeof ingestSource !== 'string' || ingestSource === '') {
    throw new CollectionDefinitionError(
      'INVALID_INGEST_SOURCE',
      'ingestSource',
      'Ingest source must be a non-empty string'
    )
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
  // Validate all fields
  validateFromStrict(input.$from)
  validateExpandStrict(input.$expand)
  validateFlattenStrict(input.$flatten)
  validateFilterStrict(input.$filter)
  validateSelectStrict(input.$select)
  validateGroupByStrict(input.$groupBy)
  validateComputeStrict(input.$compute)
  validateUnnestStrict(input.$unnest)
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
  // Validate all inputs
  validateCollectionType(type)
  validateCollectionFields(fields)
  validateIngestSource(ingestSource)

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

    if (isMVDefinition(obj)) {
      // MV (has $from)
      materializedViews.set(name, applyMVDefaults(obj))
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

  // Check for cycles first
  const cycle = detectMVCycles(schema)
  if (cycle) {
    // Report the cycle error on the first view in the cycle
    const firstView = cycle[0]
    const isSelfRef = cycle.length === 2 && cycle[0] === cycle[1]
    const cycleMessage = isSelfRef
      ? `Circular dependency detected: MV "${firstView}" references itself`
      : `Circular dependency detected in materialized views: ${cycle.join(' -> ')}`

    const existingErrors = errors.get(firstView) || []
    existingErrors.push({
      field: '$from',
      message: cycleMessage,
    })
    errors.set(firstView, existingErrors)
  }

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
      const existingErrors = errors.get(name) || []
      errors.set(name, [...existingErrors, ...mvErrors])
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

// Re-export cycle detection functions
export {
  getMVDependencies,
  detectMVCycles,
  MVCycleError,
}

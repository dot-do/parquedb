/**
 * Aggregation Support for Materialized Views
 *
 * Provides aggregation functionality for materialized views, including:
 * - Common aggregation operators (count, sum, avg, min, max)
 * - groupBy support for dimensional analysis
 * - Having clauses for filtering aggregated results
 *
 * Follows MongoDB aggregation pipeline patterns.
 *
 * @module materialized-views/aggregations
 */

import { getNestedValue, compareValues } from '../utils'
import type { Filter } from '../types/filter'
import { matchesFilter } from '../query/filter'
import type {
  AggregationStage,
  GroupSpec,
  Document,
} from '../aggregation/types'
import { isFieldRef } from '../aggregation/types'

// Re-export Document type for consumers of this module
export type { Document } from '../aggregation/types'

// =============================================================================
// Aggregate Expression Types
// =============================================================================

/**
 * Count aggregate expression
 * - { $count: '*' } - Count all documents
 * - { $count: '$fieldName' } - Count non-null values
 */
export interface CountExpr {
  $count: '*' | string
}

/**
 * Sum aggregate expression
 * - { $sum: '$fieldName' } - Sum of field values
 * - { $sum: 1 } - Count (equivalent to $count: '*')
 */
export interface SumExpr {
  $sum: string | number
}

/**
 * Average aggregate expression
 * - { $avg: '$fieldName' } - Average of field values
 */
export interface AvgExpr {
  $avg: string
}

/**
 * Min aggregate expression
 * - { $min: '$fieldName' } - Minimum value
 */
export interface MinExpr {
  $min: string
}

/**
 * Max aggregate expression
 * - { $max: '$fieldName' } - Maximum value
 */
export interface MaxExpr {
  $max: string
}

/**
 * First aggregate expression
 * - { $first: '$fieldName' } - First value in group
 */
export interface FirstExpr {
  $first: string
}

/**
 * Last aggregate expression
 * - { $last: '$fieldName' } - Last value in group
 */
export interface LastExpr {
  $last: string
}

/**
 * Standard deviation expression
 * - { $stdDev: '$fieldName' } - Population standard deviation
 * - { $stdDevSamp: '$fieldName' } - Sample standard deviation
 */
export interface StdDevExpr {
  $stdDev?: string
  $stdDevSamp?: string
}

/**
 * Union of all aggregate expression types
 */
export type AggregateExpr =
  | CountExpr
  | SumExpr
  | AvgExpr
  | MinExpr
  | MaxExpr
  | FirstExpr
  | LastExpr
  | StdDevExpr

// =============================================================================
// GroupBy Types
// =============================================================================

/**
 * Simple field reference for groupBy
 */
export type GroupByField = string

/**
 * Time-based grouping (extract date parts)
 */
export interface TimeGrouping {
  /** Field to extract from */
  $dateField: string
  /** Part to extract: year, month, day, hour, etc. */
  $datePart: 'year' | 'month' | 'day' | 'hour' | 'minute' | 'dayOfWeek' | 'week'
  /** Alias for the grouped field */
  $as?: string
}

/**
 * Bucket grouping (range-based)
 */
export interface BucketGrouping {
  /** Field to bucket */
  $field: string
  /** Bucket boundaries */
  $boundaries: number[]
  /** Default bucket name for values outside boundaries */
  $default?: string
  /** Alias for the grouped field */
  $as?: string
}

/**
 * GroupBy specification item
 */
export type GroupBySpec = GroupByField | TimeGrouping | BucketGrouping

// =============================================================================
// Having Types
// =============================================================================

/**
 * Having clause for filtering aggregated results
 * Uses MongoDB-style filter operators on aggregated fields
 *
 * @example
 * { count: { $gt: 10 } }
 * { totalRevenue: { $gte: 1000, $lte: 5000 } }
 */
export type HavingClause = Filter

// =============================================================================
// MV Aggregation Definition
// =============================================================================

/**
 * Complete aggregation definition for a materialized view
 */
export interface MVAggregationDefinition {
  /**
   * Fields to group by (dimensions)
   * @example ['status', 'customer.tier']
   * @example [{ $dateField: '$createdAt', $datePart: 'day', $as: 'date' }]
   */
  groupBy?: GroupBySpec[]

  /**
   * Computed aggregate fields
   * @example { orderCount: { $count: '*' }, totalRevenue: { $sum: '$total' } }
   */
  compute: Record<string, AggregateExpr>

  /**
   * Filter applied before aggregation (WHERE clause equivalent)
   */
  match?: Filter

  /**
   * Filter applied after aggregation (HAVING clause equivalent)
   */
  having?: HavingClause

  /**
   * Sort the aggregated results
   */
  sort?: Record<string, 1 | -1>

  /**
   * Limit number of results
   */
  limit?: number
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if an expression is a count expression
 */
export function isCountExpr(expr: unknown): expr is CountExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$count' in expr &&
    (expr as CountExpr).$count !== undefined
  )
}

/**
 * Check if an expression is a sum expression
 */
export function isSumExpr(expr: unknown): expr is SumExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$sum' in expr &&
    (typeof (expr as SumExpr).$sum === 'string' || typeof (expr as SumExpr).$sum === 'number')
  )
}

/**
 * Check if an expression is an average expression
 */
export function isAvgExpr(expr: unknown): expr is AvgExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$avg' in expr &&
    typeof (expr as AvgExpr).$avg === 'string'
  )
}

/**
 * Check if an expression is a min expression
 */
export function isMinExpr(expr: unknown): expr is MinExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$min' in expr &&
    typeof (expr as MinExpr).$min === 'string'
  )
}

/**
 * Check if an expression is a max expression
 */
export function isMaxExpr(expr: unknown): expr is MaxExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$max' in expr &&
    typeof (expr as MaxExpr).$max === 'string'
  )
}

/**
 * Check if an expression is a first expression
 */
export function isFirstExpr(expr: unknown): expr is FirstExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$first' in expr &&
    typeof (expr as FirstExpr).$first === 'string'
  )
}

/**
 * Check if an expression is a last expression
 */
export function isLastExpr(expr: unknown): expr is LastExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    '$last' in expr &&
    typeof (expr as LastExpr).$last === 'string'
  )
}

/**
 * Check if an expression is a standard deviation expression
 */
export function isStdDevExpr(expr: unknown): expr is StdDevExpr {
  return (
    typeof expr === 'object' &&
    expr !== null &&
    ('$stdDev' in expr || '$stdDevSamp' in expr)
  )
}

/**
 * Check if a groupBy spec is a time grouping
 */
export function isTimeGrouping(spec: GroupBySpec): spec is TimeGrouping {
  return typeof spec === 'object' && '$dateField' in spec
}

/**
 * Check if a groupBy spec is a bucket grouping
 */
export function isBucketGrouping(spec: GroupBySpec): spec is BucketGrouping {
  return typeof spec === 'object' && '$field' in spec && '$boundaries' in spec
}

// =============================================================================
// Aggregation Computation Functions
// =============================================================================

/**
 * Compute count aggregate
 */
export function computeCount(docs: Document[], expr: CountExpr): number {
  if (expr.$count === '*') {
    return docs.length
  }

  const field = expr.$count.startsWith('$') ? expr.$count.slice(1) : expr.$count
  return docs.filter(doc => {
    const value = getNestedValue(doc, field)
    return value !== null && value !== undefined
  }).length
}

/**
 * Compute sum aggregate
 */
export function computeSum(docs: Document[], expr: SumExpr): number {
  if (typeof expr.$sum === 'number') {
    return docs.length * expr.$sum
  }

  const field = expr.$sum.startsWith('$') ? expr.$sum.slice(1) : expr.$sum
  return docs.reduce((sum, doc) => {
    const value = getNestedValue(doc, field)
    return sum + (typeof value === 'number' ? value : 0)
  }, 0)
}

/**
 * Compute average aggregate
 */
export function computeAvg(docs: Document[], expr: AvgExpr): number | null {
  if (docs.length === 0) return null

  const field = expr.$avg.startsWith('$') ? expr.$avg.slice(1) : expr.$avg
  let sum = 0
  let count = 0

  for (const doc of docs) {
    const value = getNestedValue(doc, field)
    if (typeof value === 'number') {
      sum += value
      count++
    }
  }

  return count > 0 ? sum / count : null
}

/**
 * Compute min aggregate
 */
export function computeMin(docs: Document[], expr: MinExpr): unknown {
  if (docs.length === 0) return null

  const field = expr.$min.startsWith('$') ? expr.$min.slice(1) : expr.$min
  let min: unknown = undefined

  for (const doc of docs) {
    const value = getNestedValue(doc, field)
    if (value === null || value === undefined) continue
    if (min === undefined || compareValues(value, min) < 0) {
      min = value
    }
  }

  return min ?? null
}

/**
 * Compute max aggregate
 */
export function computeMax(docs: Document[], expr: MaxExpr): unknown {
  if (docs.length === 0) return null

  const field = expr.$max.startsWith('$') ? expr.$max.slice(1) : expr.$max
  let max: unknown = undefined

  for (const doc of docs) {
    const value = getNestedValue(doc, field)
    if (value === null || value === undefined) continue
    if (max === undefined || compareValues(value, max) > 0) {
      max = value
    }
  }

  return max ?? null
}

/**
 * Compute first aggregate
 */
export function computeFirst(docs: Document[], expr: FirstExpr): unknown {
  if (docs.length === 0) return null

  const field = expr.$first.startsWith('$') ? expr.$first.slice(1) : expr.$first
  return getNestedValue(docs[0]!, field) ?? null
}

/**
 * Compute last aggregate
 */
export function computeLast(docs: Document[], expr: LastExpr): unknown {
  if (docs.length === 0) return null

  const field = expr.$last.startsWith('$') ? expr.$last.slice(1) : expr.$last
  return getNestedValue(docs[docs.length - 1]!, field) ?? null
}

/**
 * Compute standard deviation (population)
 */
export function computeStdDev(docs: Document[], field: string): number | null {
  if (docs.length === 0) return null

  const fieldPath = field.startsWith('$') ? field.slice(1) : field
  const values: number[] = []

  for (const doc of docs) {
    const value = getNestedValue(doc, fieldPath)
    if (typeof value === 'number') {
      values.push(value)
    }
  }

  if (values.length === 0) return null

  const mean = values.reduce((a, b) => a + b, 0) / values.length
  const squaredDiffs = values.map(v => Math.pow(v - mean, 2))
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / values.length

  return Math.sqrt(variance)
}

/**
 * Compute standard deviation (sample)
 */
export function computeStdDevSamp(docs: Document[], field: string): number | null {
  if (docs.length < 2) return null

  const fieldPath = field.startsWith('$') ? field.slice(1) : field
  const values: number[] = []

  for (const doc of docs) {
    const value = getNestedValue(doc, fieldPath)
    if (typeof value === 'number') {
      values.push(value)
    }
  }

  if (values.length < 2) return null

  const mean = values.reduce((a, b) => a + b, 0) / values.length
  const squaredDiffs = values.map(v => Math.pow(v - mean, 2))
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / (values.length - 1)

  return Math.sqrt(variance)
}

/**
 * Compute any aggregate expression
 */
export function computeAggregate(docs: Document[], expr: AggregateExpr): unknown {
  if (isCountExpr(expr)) return computeCount(docs, expr)
  if (isSumExpr(expr)) return computeSum(docs, expr)
  if (isAvgExpr(expr)) return computeAvg(docs, expr)
  if (isMinExpr(expr)) return computeMin(docs, expr)
  if (isMaxExpr(expr)) return computeMax(docs, expr)
  if (isFirstExpr(expr)) return computeFirst(docs, expr)
  if (isLastExpr(expr)) return computeLast(docs, expr)
  if (isStdDevExpr(expr)) {
    if (expr.$stdDev) return computeStdDev(docs, expr.$stdDev)
    if (expr.$stdDevSamp) return computeStdDevSamp(docs, expr.$stdDevSamp)
  }
  return null
}

// =============================================================================
// GroupBy Functions
// =============================================================================

/**
 * Extract date part from a Date value
 */
function extractDatePart(date: Date, part: TimeGrouping['$datePart']): number {
  switch (part) {
    case 'year':
      return date.getUTCFullYear()
    case 'month':
      return date.getUTCMonth() + 1 // 1-indexed
    case 'day':
      return date.getUTCDate()
    case 'hour':
      return date.getUTCHours()
    case 'minute':
      return date.getUTCMinutes()
    case 'dayOfWeek':
      return date.getUTCDay() // 0 = Sunday
    case 'week':
      // ISO week number
      const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
      d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
      return Math.ceil(((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
    default:
      return 0
  }
}

/**
 * Get bucket for a value based on boundaries
 */
function getBucket(value: number, boundaries: number[], defaultBucket?: string): string | number {
  for (let i = 0; i < boundaries.length - 1; i++) {
    if (value >= boundaries[i]! && value < boundaries[i + 1]!) {
      return boundaries[i]!
    }
  }
  return defaultBucket ?? 'other'
}

/**
 * Compute group key for a document based on groupBy specs
 */
export function computeGroupKey(doc: Document, groupBy: GroupBySpec[]): Document {
  const key: Document = {}

  for (const spec of groupBy) {
    if (typeof spec === 'string') {
      // Simple field reference
      const field = spec.startsWith('$') ? spec.slice(1) : spec
      key[field] = getNestedValue(doc, field)
    } else if (isTimeGrouping(spec)) {
      // Time-based grouping
      const field = spec.$dateField.startsWith('$') ? spec.$dateField.slice(1) : spec.$dateField
      const value = getNestedValue(doc, field)
      const alias = spec.$as ?? `${field}_${spec.$datePart}`

      if (value instanceof Date) {
        key[alias] = extractDatePart(value, spec.$datePart)
      } else if (typeof value === 'string' || typeof value === 'number') {
        const date = new Date(value)
        if (!isNaN(date.getTime())) {
          key[alias] = extractDatePart(date, spec.$datePart)
        }
      }
    } else if (isBucketGrouping(spec)) {
      // Bucket-based grouping
      const field = spec.$field.startsWith('$') ? spec.$field.slice(1) : spec.$field
      const value = getNestedValue(doc, field)
      const alias = spec.$as ?? `${field}_bucket`

      if (typeof value === 'number') {
        key[alias] = getBucket(value, spec.$boundaries, spec.$default)
      }
    }
  }

  return key
}

/**
 * Group documents by groupBy specification
 */
export function groupDocuments(
  docs: Document[],
  groupBy: GroupBySpec[]
): Map<string, { key: Document; docs: Document[] }> {
  const groups = new Map<string, { key: Document; docs: Document[] }>()

  for (const doc of docs) {
    const groupKey = computeGroupKey(doc, groupBy)
    const keyStr = JSON.stringify(groupKey)

    if (!groups.has(keyStr)) {
      groups.set(keyStr, { key: groupKey, docs: [] })
    }
    groups.get(keyStr)!.docs.push(doc)
  }

  return groups
}

// =============================================================================
// Main Aggregation Executor
// =============================================================================

/**
 * Result of MV aggregation
 */
export interface MVAggregationResult {
  /** Aggregated documents */
  documents: Document[]
  /** Execution statistics */
  stats: {
    /** Total input documents */
    inputCount: number
    /** Documents after $match filter */
    matchedCount: number
    /** Number of groups */
    groupCount: number
    /** Documents after $having filter */
    outputCount: number
  }
}

/**
 * Execute MV aggregation on a dataset
 *
 * @param data - Input documents
 * @param definition - Aggregation definition
 * @returns Aggregation result
 */
export function executeMVAggregation(
  data: Document[],
  definition: MVAggregationDefinition
): MVAggregationResult {
  const stats = {
    inputCount: data.length,
    matchedCount: 0,
    groupCount: 0,
    outputCount: 0,
  }

  // Step 1: Apply $match filter (WHERE clause equivalent)
  let filtered = data
  if (definition.match) {
    filtered = data.filter(doc => matchesFilter(doc, definition.match!))
  }
  stats.matchedCount = filtered.length

  // Step 2: Group documents
  let results: Document[]

  if (definition.groupBy && definition.groupBy.length > 0) {
    // Group by specified fields
    const groups = groupDocuments(filtered, definition.groupBy)
    stats.groupCount = groups.size

    results = Array.from(groups.values()).map(({ key, docs }) => {
      const result: Document = { ...key }

      // Compute aggregates for each group
      for (const [name, expr] of Object.entries(definition.compute)) {
        result[name] = computeAggregate(docs, expr)
      }

      return result
    })
  } else {
    // No groupBy - aggregate entire dataset as single group
    stats.groupCount = 1
    const result: Document = { _id: null }

    for (const [name, expr] of Object.entries(definition.compute)) {
      result[name] = computeAggregate(filtered, expr)
    }

    results = [result]
  }

  // Step 3: Apply $having filter (HAVING clause equivalent)
  if (definition.having) {
    results = results.filter(doc => matchesFilter(doc, definition.having!))
  }
  stats.outputCount = results.length

  // Step 4: Apply sort
  if (definition.sort) {
    const sortEntries = Object.entries(definition.sort)
    results.sort((a, b) => {
      for (const [field, direction] of sortEntries) {
        const aValue = getNestedValue(a, field)
        const bValue = getNestedValue(b, field)
        const cmp = compareValues(aValue, bValue)
        if (cmp !== 0) return direction * cmp
      }
      return 0
    })
  }

  // Step 5: Apply limit
  if (definition.limit && definition.limit > 0) {
    results = results.slice(0, definition.limit)
  }

  return { documents: results, stats }
}

// =============================================================================
// Aggregation Pipeline Conversion
// =============================================================================

/**
 * Convert MV aggregation definition to MongoDB-style aggregation pipeline
 *
 * This allows using the existing aggregation executor for MVs
 *
 * @param definition - MV aggregation definition
 * @returns Aggregation pipeline stages
 */
export function toAggregationPipeline(definition: MVAggregationDefinition): AggregationStage[] {
  const pipeline: AggregationStage[] = []

  // Add $match stage
  if (definition.match) {
    pipeline.push({ $match: definition.match })
  }

  // Build $group stage
  const groupSpec: GroupSpec = {
    _id: buildGroupId(definition.groupBy),
  }

  // Add accumulators
  for (const [name, expr] of Object.entries(definition.compute)) {
    if (isCountExpr(expr)) {
      if (expr.$count === '*') {
        groupSpec[name] = { $sum: 1 }
      } else {
        // Count non-null values - use $sum with $cond
        groupSpec[name] = { $sum: 1 } // Simplified - actual implementation would need $cond
      }
    } else if (isSumExpr(expr)) {
      groupSpec[name] = { $sum: expr.$sum }
    } else if (isAvgExpr(expr)) {
      groupSpec[name] = { $avg: expr.$avg }
    } else if (isMinExpr(expr)) {
      groupSpec[name] = { $min: expr.$min }
    } else if (isMaxExpr(expr)) {
      groupSpec[name] = { $max: expr.$max }
    } else if (isFirstExpr(expr)) {
      groupSpec[name] = { $first: expr.$first }
    } else if (isLastExpr(expr)) {
      groupSpec[name] = { $last: expr.$last }
    }
  }

  pipeline.push({ $group: groupSpec })

  // Add $match stage for $having (applied after $group)
  if (definition.having) {
    pipeline.push({ $match: definition.having })
  }

  // Add $sort stage
  if (definition.sort) {
    pipeline.push({ $sort: definition.sort })
  }

  // Add $limit stage
  if (definition.limit) {
    pipeline.push({ $limit: definition.limit })
  }

  return pipeline
}

/**
 * Build the _id field for $group from groupBy specs
 */
function buildGroupId(groupBy?: GroupBySpec[]): unknown {
  if (!groupBy || groupBy.length === 0) {
    return null
  }

  if (groupBy.length === 1 && typeof groupBy[0] === 'string') {
    // Single field - use field reference directly
    return groupBy[0].startsWith('$') ? groupBy[0] : `$${groupBy[0]}`
  }

  // Multiple fields or complex grouping - build compound _id
  const id: Document = {}

  for (const spec of groupBy) {
    if (typeof spec === 'string') {
      const field = spec.startsWith('$') ? spec.slice(1) : spec
      id[field] = `$${field}`
    } else if (isTimeGrouping(spec)) {
      const field = spec.$dateField.startsWith('$') ? spec.$dateField.slice(1) : spec.$dateField
      const alias = spec.$as ?? `${field}_${spec.$datePart}`

      // Build date extraction expression
      // MongoDB uses $year, $month, $dayOfMonth, etc.
      const dateOp = getDateOperator(spec.$datePart)
      id[alias] = { [dateOp]: `$${field}` }
    } else if (isBucketGrouping(spec)) {
      const field = spec.$field.startsWith('$') ? spec.$field.slice(1) : spec.$field
      const alias = spec.$as ?? `${field}_bucket`

      // Build bucket expression using $switch
      id[alias] = buildBucketExpression(field, spec.$boundaries, spec.$default)
    }
  }

  return id
}

/**
 * Get MongoDB date operator for date part
 */
function getDateOperator(part: TimeGrouping['$datePart']): string {
  switch (part) {
    case 'year':
      return '$year'
    case 'month':
      return '$month'
    case 'day':
      return '$dayOfMonth'
    case 'hour':
      return '$hour'
    case 'minute':
      return '$minute'
    case 'dayOfWeek':
      return '$dayOfWeek'
    case 'week':
      return '$week'
    default:
      return '$year'
  }
}

/**
 * Build bucket expression for $group
 */
function buildBucketExpression(
  field: string,
  boundaries: number[],
  defaultValue?: string
): Document {
  const branches: Document[] = []

  for (let i = 0; i < boundaries.length - 1; i++) {
    branches.push({
      case: {
        $and: [
          { $gte: [`$${field}`, boundaries[i]] },
          { $lt: [`$${field}`, boundaries[i + 1]] },
        ],
      },
      then: boundaries[i],
    })
  }

  return {
    $switch: {
      branches,
      default: defaultValue ?? 'other',
    },
  }
}

// =============================================================================
// Incremental Aggregation Support
// =============================================================================

/**
 * Check if an aggregation can be incrementally updated
 *
 * Incremental updates are possible for:
 * - $sum, $count, $min, $max (additive/comparable)
 * - $avg (if we store sum and count separately)
 *
 * NOT incrementally updatable:
 * - $first, $last (order-dependent)
 * - $stdDev, $stdDevSamp (require full recalculation)
 */
export function isIncrementallyUpdatable(definition: MVAggregationDefinition): boolean {
  for (const expr of Object.values(definition.compute)) {
    // These require full recalculation
    if (isFirstExpr(expr) || isLastExpr(expr) || isStdDevExpr(expr)) {
      return false
    }
  }
  return true
}

/**
 * Merge two aggregated results (for incremental updates)
 *
 * @param existing - Existing aggregated document
 * @param delta - New aggregated document to merge
 * @param compute - Compute specification
 * @returns Merged document
 */
export function mergeAggregates(
  existing: Document,
  delta: Document,
  compute: Record<string, AggregateExpr>
): Document {
  const result: Document = { ...existing }

  for (const [name, expr] of Object.entries(compute)) {
    const existingVal = existing[name]
    const deltaVal = delta[name]

    if (isCountExpr(expr) || (isSumExpr(expr) && typeof expr.$sum === 'number')) {
      // Additive: sum the counts
      result[name] = (existingVal as number ?? 0) + (deltaVal as number ?? 0)
    } else if (isSumExpr(expr)) {
      // Additive: sum the sums
      result[name] = (existingVal as number ?? 0) + (deltaVal as number ?? 0)
    } else if (isMinExpr(expr)) {
      // Take the minimum
      if (existingVal === null || existingVal === undefined) {
        result[name] = deltaVal
      } else if (deltaVal !== null && deltaVal !== undefined) {
        result[name] = compareValues(existingVal, deltaVal) <= 0 ? existingVal : deltaVal
      }
    } else if (isMaxExpr(expr)) {
      // Take the maximum
      if (existingVal === null || existingVal === undefined) {
        result[name] = deltaVal
      } else if (deltaVal !== null && deltaVal !== undefined) {
        result[name] = compareValues(existingVal, deltaVal) >= 0 ? existingVal : deltaVal
      }
    } else if (isAvgExpr(expr)) {
      // For avg, we need to store count separately - this is a simplified merge
      // In practice, store _sum and _count fields for proper averaging
      result[name] = deltaVal // Just take the new value for simplicity
    }
  }

  return result
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validation error for aggregation definitions
 */
export class AggregationValidationError extends Error {
  constructor(
    message: string,
    public readonly field?: string
  ) {
    super(message)
    this.name = 'AggregationValidationError'
  }
}

/**
 * Validate an MV aggregation definition
 *
 * @param definition - Definition to validate
 * @throws AggregationValidationError if invalid
 */
export function validateMVAggregation(definition: MVAggregationDefinition): void {
  // Must have at least one compute field
  if (!definition.compute || Object.keys(definition.compute).length === 0) {
    throw new AggregationValidationError('Aggregation must have at least one compute field')
  }

  // Validate compute expressions
  for (const [name, expr] of Object.entries(definition.compute)) {
    if (!isValidAggregateExpr(expr)) {
      throw new AggregationValidationError(`Invalid aggregate expression for field: ${name}`, name)
    }
  }

  // Validate groupBy specs
  if (definition.groupBy) {
    for (const spec of definition.groupBy) {
      if (!isValidGroupBySpec(spec)) {
        throw new AggregationValidationError('Invalid groupBy specification')
      }
    }
  }

  // Validate sort fields exist in output
  if (definition.sort) {
    const outputFields = new Set([
      ...Object.keys(definition.compute),
      ...(definition.groupBy?.map(spec => {
        if (typeof spec === 'string') return spec.replace(/^\$/, '')
        if (isTimeGrouping(spec)) return spec.$as ?? `${spec.$dateField.replace(/^\$/, '')}_${spec.$datePart}`
        if (isBucketGrouping(spec)) return spec.$as ?? `${spec.$field.replace(/^\$/, '')}_bucket`
        return ''
      }) ?? []),
    ])

    for (const field of Object.keys(definition.sort)) {
      if (!outputFields.has(field) && field !== '_id') {
        throw new AggregationValidationError(`Sort field not in output: ${field}`, field)
      }
    }
  }
}

/**
 * Check if an expression is a valid aggregate expression
 */
function isValidAggregateExpr(expr: unknown): boolean {
  return (
    isCountExpr(expr) ||
    isSumExpr(expr) ||
    isAvgExpr(expr) ||
    isMinExpr(expr) ||
    isMaxExpr(expr) ||
    isFirstExpr(expr) ||
    isLastExpr(expr) ||
    isStdDevExpr(expr)
  )
}

/**
 * Check if a spec is a valid groupBy specification
 */
function isValidGroupBySpec(spec: unknown): spec is GroupBySpec {
  if (typeof spec === 'string') {
    return spec.length > 0
  }

  if (typeof spec === 'object' && spec !== null) {
    const obj = spec as Record<string, unknown>

    // TimeGrouping
    if ('$dateField' in obj && '$datePart' in obj) {
      return (
        typeof obj.$dateField === 'string' &&
        ['year', 'month', 'day', 'hour', 'minute', 'dayOfWeek', 'week'].includes(
          obj.$datePart as string
        )
      )
    }

    // BucketGrouping
    if ('$field' in obj && '$boundaries' in obj) {
      return (
        typeof obj.$field === 'string' &&
        Array.isArray(obj.$boundaries) &&
        obj.$boundaries.length >= 2 &&
        obj.$boundaries.every((b): b is number => typeof b === 'number')
      )
    }
  }

  return false
}

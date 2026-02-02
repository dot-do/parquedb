/**
 * Aggregation Pipeline Types for ParqueDB
 *
 * MongoDB-style aggregation pipeline with support for:
 * - $match - Filter documents
 * - $group - Group by field with accumulators
 * - $project - Reshape documents
 * - $sort - Sort results
 * - $limit / $skip - Pagination
 * - $unwind - Deconstruct array fields
 * - $lookup - Join with other collections
 * - $count - Count documents
 * - $addFields / $set - Add new fields
 * - $unset - Remove fields
 */

import type { Filter } from '../types/filter'
import type { IndexManager } from '../indexes/manager'

// =============================================================================
// Accumulator Operators
// =============================================================================

/**
 * Sum accumulator - sums numeric values
 * - { $sum: 1 } - counts documents
 * - { $sum: '$fieldName' } - sums field values
 */
export interface SumAccumulator {
  $sum: number | string
}

/**
 * Average accumulator - calculates average of numeric values
 * - { $avg: '$fieldName' }
 */
export interface AvgAccumulator {
  $avg: string
}

/**
 * Min accumulator - finds minimum value
 * - { $min: '$fieldName' }
 */
export interface MinAccumulator {
  $min: string
}

/**
 * Max accumulator - finds maximum value
 * - { $max: '$fieldName' }
 */
export interface MaxAccumulator {
  $max: string
}

/**
 * Count accumulator - counts non-null values
 * - { $count: {} }
 */
export interface CountAccumulator {
  $count: Record<string, never>
}

/**
 * First accumulator - returns first value in group
 * - { $first: '$fieldName' }
 */
export interface FirstAccumulator {
  $first: string
}

/**
 * Last accumulator - returns last value in group
 * - { $last: '$fieldName' }
 */
export interface LastAccumulator {
  $last: string
}

/**
 * Push accumulator - creates array of values
 * - { $push: '$fieldName' }
 */
export interface PushAccumulator {
  $push: string
}

/**
 * AddToSet accumulator - creates array of unique values
 * - { $addToSet: '$fieldName' }
 */
export interface AddToSetAccumulator {
  $addToSet: string
}

/**
 * Union of all accumulator types
 */
export type Accumulator =
  | SumAccumulator
  | AvgAccumulator
  | MinAccumulator
  | MaxAccumulator
  | CountAccumulator
  | FirstAccumulator
  | LastAccumulator
  | PushAccumulator
  | AddToSetAccumulator

// =============================================================================
// Accumulator Type Guards
// =============================================================================

/**
 * Type guard for SumAccumulator
 */
export function isSumAccumulator(acc: unknown): acc is SumAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$sum' in acc &&
    (typeof (acc as SumAccumulator).$sum === 'number' ||
      typeof (acc as SumAccumulator).$sum === 'string')
  )
}

/**
 * Type guard for AvgAccumulator
 */
export function isAvgAccumulator(acc: unknown): acc is AvgAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$avg' in acc &&
    typeof (acc as AvgAccumulator).$avg === 'string'
  )
}

/**
 * Type guard for MinAccumulator
 */
export function isMinAccumulator(acc: unknown): acc is MinAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$min' in acc &&
    typeof (acc as MinAccumulator).$min === 'string'
  )
}

/**
 * Type guard for MaxAccumulator
 */
export function isMaxAccumulator(acc: unknown): acc is MaxAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$max' in acc &&
    typeof (acc as MaxAccumulator).$max === 'string'
  )
}

/**
 * Type guard for CountAccumulator
 */
export function isCountAccumulator(acc: unknown): acc is CountAccumulator {
  return typeof acc === 'object' && acc !== null && '$count' in acc
}

/**
 * Type guard for FirstAccumulator
 */
export function isFirstAccumulator(acc: unknown): acc is FirstAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$first' in acc &&
    typeof (acc as FirstAccumulator).$first === 'string'
  )
}

/**
 * Type guard for LastAccumulator
 */
export function isLastAccumulator(acc: unknown): acc is LastAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$last' in acc &&
    typeof (acc as LastAccumulator).$last === 'string'
  )
}

/**
 * Type guard for PushAccumulator
 */
export function isPushAccumulator(acc: unknown): acc is PushAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$push' in acc &&
    typeof (acc as PushAccumulator).$push === 'string'
  )
}

/**
 * Type guard for AddToSetAccumulator
 */
export function isAddToSetAccumulator(acc: unknown): acc is AddToSetAccumulator {
  return (
    typeof acc === 'object' &&
    acc !== null &&
    '$addToSet' in acc &&
    typeof (acc as AddToSetAccumulator).$addToSet === 'string'
  )
}

/**
 * Check if an accumulator field reference is a valid field path (starts with $)
 */
export function isFieldPath(value: string): boolean {
  return value.startsWith('$')
}

// =============================================================================
// Document Types
// =============================================================================

/**
 * Base document type - represents a generic document in the aggregation pipeline
 */
export type Document = Record<string, unknown>

/**
 * Field reference string (e.g., '$fieldName')
 */
export type FieldRef = `$${string}`

/**
 * Check if a value is a field reference
 */
export function isFieldRef(value: unknown): value is FieldRef {
  return typeof value === 'string' && value.startsWith('$')
}

// =============================================================================
// Pipeline Stages
// =============================================================================

/**
 * $match stage - filters documents
 *
 * @example
 * { $match: { status: 'published' } }
 * { $match: { views: { $gt: 100 } } }
 */
export interface MatchStage {
  $match: Filter
}

/**
 * Group specification for $group stage
 */
export interface GroupSpec {
  /** Field to group by (use $fieldName) or null for all documents */
  _id: unknown
  /** Additional accumulator fields */
  [key: string]: unknown
}

/**
 * $group stage - groups documents and applies accumulators
 *
 * @example
 * { $group: { _id: '$status', count: { $sum: 1 } } }
 * { $group: { _id: null, total: { $sum: '$views' } } }
 */
export interface GroupStage {
  $group: GroupSpec
}

/**
 * Sort specification
 */
export type SortOrder = 1 | -1

/**
 * $sort stage - sorts documents
 *
 * @example
 * { $sort: { createdAt: -1 } }
 * { $sort: { status: 1, views: -1 } }
 */
export interface SortStage {
  $sort: Record<string, SortOrder>
}

/**
 * $limit stage - limits number of documents
 *
 * @example
 * { $limit: 10 }
 */
export interface LimitStage {
  $limit: number
}

/**
 * $skip stage - skips documents
 *
 * @example
 * { $skip: 20 }
 */
export interface SkipStage {
  $skip: number
}

/**
 * Projection value
 * - 0 or false: exclude field
 * - 1 or true: include field
 * - Expression: computed field
 */
export type ProjectionValue = 0 | 1 | boolean | unknown

/**
 * $project stage - reshapes documents
 *
 * @example
 * { $project: { title: 1, status: 1 } }
 * { $project: { content: 0 } }
 */
export interface ProjectStage {
  $project: Record<string, ProjectionValue>
}

/**
 * Unwind options
 */
export interface UnwindOptions {
  /** Path to array field (use $fieldName) */
  path: string
  /** Include documents where array is null or empty */
  preserveNullAndEmptyArrays?: boolean
  /** Include index of array element */
  includeArrayIndex?: string
}

/**
 * $unwind stage - deconstructs array field
 *
 * @example
 * { $unwind: '$tags' }
 * { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } }
 */
export interface UnwindStage {
  $unwind: string | UnwindOptions
}

/**
 * Lookup options for joining collections
 */
export interface LookupOptions {
  /** Collection to join */
  from: string
  /** Field in current documents */
  localField: string
  /** Field in foreign documents */
  foreignField: string
  /** Output array field name */
  as: string
}

/**
 * $lookup stage - performs left outer join
 *
 * @example
 * { $lookup: { from: 'users', localField: 'authorId', foreignField: '$id', as: 'author' } }
 */
export interface LookupStage {
  $lookup: LookupOptions
}

/**
 * $count stage - counts documents
 *
 * @example
 * { $count: 'totalPosts' }
 */
export interface CountStage {
  $count: string
}

/**
 * $addFields stage - adds new fields
 *
 * @example
 * { $addFields: { isPopular: { $gt: ['$views', 1000] } } }
 */
export interface AddFieldsStage {
  $addFields: Record<string, unknown>
}

/**
 * $set stage - alias for $addFields
 *
 * @example
 * { $set: { processed: true } }
 */
export interface SetStage {
  $set: Record<string, unknown>
}

/**
 * $unset stage - removes fields
 *
 * @example
 * { $unset: 'content' }
 * { $unset: ['content', 'views'] }
 */
export interface UnsetStage {
  $unset: string | string[]
}

/**
 * $replaceRoot stage - replaces document with specified field
 *
 * @example
 * { $replaceRoot: { newRoot: '$nested' } }
 */
export interface ReplaceRootStage {
  $replaceRoot: { newRoot: unknown }
}

/**
 * $facet stage - processes multiple pipelines in parallel
 *
 * @example
 * { $facet: { byStatus: [...], byTag: [...] } }
 */
export interface FacetStage {
  $facet: Record<string, AggregationStage[]>
}

/**
 * $bucket stage - categorizes documents into buckets
 */
export interface BucketStage {
  $bucket: {
    groupBy: string
    boundaries: unknown[]
    default?: string
    output?: Record<string, unknown>
  }
}

/**
 * $sample stage - randomly selects documents
 *
 * @example
 * { $sample: { size: 5 } }
 */
export interface SampleStage {
  $sample: { size: number }
}

// =============================================================================
// Aggregation Pipeline
// =============================================================================

/**
 * Union of all pipeline stage types
 */
export type AggregationStage =
  | MatchStage
  | GroupStage
  | SortStage
  | LimitStage
  | SkipStage
  | ProjectStage
  | UnwindStage
  | LookupStage
  | CountStage
  | AddFieldsStage
  | SetStage
  | UnsetStage
  | ReplaceRootStage
  | FacetStage
  | BucketStage
  | SampleStage

/**
 * Type guard for MatchStage
 */
export function isMatchStage(stage: AggregationStage): stage is MatchStage {
  return '$match' in stage
}

/**
 * Type guard for GroupStage
 */
export function isGroupStage(stage: AggregationStage): stage is GroupStage {
  return '$group' in stage
}

/**
 * Type guard for SortStage
 */
export function isSortStage(stage: AggregationStage): stage is SortStage {
  return '$sort' in stage
}

/**
 * Type guard for LimitStage
 */
export function isLimitStage(stage: AggregationStage): stage is LimitStage {
  return '$limit' in stage
}

/**
 * Type guard for SkipStage
 */
export function isSkipStage(stage: AggregationStage): stage is SkipStage {
  return '$skip' in stage
}

/**
 * Type guard for ProjectStage
 */
export function isProjectStage(stage: AggregationStage): stage is ProjectStage {
  return '$project' in stage
}

/**
 * Type guard for UnwindStage
 */
export function isUnwindStage(stage: AggregationStage): stage is UnwindStage {
  return '$unwind' in stage
}

/**
 * Type guard for LookupStage
 */
export function isLookupStage(stage: AggregationStage): stage is LookupStage {
  return '$lookup' in stage
}

/**
 * Type guard for CountStage
 */
export function isCountStage(stage: AggregationStage): stage is CountStage {
  return '$count' in stage
}

/**
 * Type guard for AddFieldsStage
 */
export function isAddFieldsStage(stage: AggregationStage): stage is AddFieldsStage {
  return '$addFields' in stage
}

/**
 * Type guard for SetStage
 */
export function isSetStage(stage: AggregationStage): stage is SetStage {
  return '$set' in stage
}

/**
 * Type guard for UnsetStage
 */
export function isUnsetStage(stage: AggregationStage): stage is UnsetStage {
  return '$unset' in stage
}

/**
 * Type guard for ReplaceRootStage
 */
export function isReplaceRootStage(stage: AggregationStage): stage is ReplaceRootStage {
  return '$replaceRoot' in stage
}

/**
 * Type guard for FacetStage
 */
export function isFacetStage(stage: AggregationStage): stage is FacetStage {
  return '$facet' in stage
}

/**
 * Type guard for BucketStage
 */
export function isBucketStage(stage: AggregationStage): stage is BucketStage {
  return '$bucket' in stage
}

/**
 * Type guard for SampleStage
 */
export function isSampleStage(stage: AggregationStage): stage is SampleStage {
  return '$sample' in stage
}

// =============================================================================
// Aggregation Options
// =============================================================================

/**
 * Options for aggregation pipeline execution
 */
export interface AggregationOptions {
  /** Maximum time in milliseconds */
  maxTimeMs?: number

  /** Allow disk use for large aggregations */
  allowDiskUse?: boolean

  /** Hint for index */
  hint?: string | Record<string, 1 | -1>

  /** Include soft-deleted entities */
  includeDeleted?: boolean

  /** Time-travel: query as of specific time */
  asOf?: Date

  /** Explain without executing */
  explain?: boolean

  /** Batch size for cursor */
  batchSize?: number

  /** Collation options */
  collation?: {
    locale: string
    strength?: 1 | 2 | 3 | 4 | 5
    caseLevel?: boolean
    numericOrdering?: boolean
  }

  /**
   * Index manager for index-aware $match stage execution.
   * When provided, the aggregation executor will attempt to use
   * secondary indexes (hash, sst, fts, vector) for the first $match stage.
   */
  indexManager?: IndexManager

  /**
   * Namespace for index lookups.
   * Required when indexManager is provided.
   */
  namespace?: string
}

/**
 * Result of aggregation explain
 */
export interface AggregationExplain {
  /** Stages in the pipeline */
  stages: {
    name: string
    inputCount?: number
    outputCount?: number
    durationMs?: number
  }[]

  /** Total execution time */
  totalDurationMs: number

  /** Estimated document count */
  estimatedDocuments: number
}

/**
 * Aggregation Framework for ParqueDB
 *
 * Provides MongoDB-style aggregation pipeline support including:
 * - Pipeline stages ($match, $group, $sort, $project, etc.)
 * - Accumulator operators ($sum, $avg, $min, $max, etc.)
 * - Expression evaluation for computed fields
 *
 * @module aggregation
 */

// Types
export {
  // Accumulator types
  type SumAccumulator,
  type AvgAccumulator,
  type MinAccumulator,
  type MaxAccumulator,
  type CountAccumulator,
  type FirstAccumulator,
  type LastAccumulator,
  type PushAccumulator,
  type AddToSetAccumulator,
  type Accumulator,

  // Stage types
  type MatchStage,
  type GroupStage,
  type GroupSpec,
  type SortStage,
  type SortOrder,
  type LimitStage,
  type SkipStage,
  type ProjectStage,
  type ProjectionValue,
  type UnwindStage,
  type UnwindOptions,
  type LookupStage,
  type LookupOptions,
  type CountStage,
  type AddFieldsStage,
  type SetStage,
  type UnsetStage,
  type ReplaceRootStage,
  type FacetStage,
  type BucketStage,
  type SampleStage,

  // Union type
  type AggregationStage,

  // Type guards
  isMatchStage,
  isGroupStage,
  isSortStage,
  isLimitStage,
  isSkipStage,
  isProjectStage,
  isUnwindStage,
  isLookupStage,
  isCountStage,
  isAddFieldsStage,
  isSetStage,
  isUnsetStage,
  isReplaceRootStage,
  isFacetStage,
  isBucketStage,
  isSampleStage,

  // Options
  type AggregationOptions,
  type AggregationExplain,
} from './types'

// Executor
export {
  executeAggregation,
  AggregationExecutor,
} from './executor'

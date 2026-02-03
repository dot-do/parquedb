/**
 * Eval Scores Observability Module for ParqueDB
 *
 * Provides materialized views and utilities for tracking evaluation scores,
 * aggregations, trends, and performance metrics.
 *
 * @example
 * ```typescript
 * import { EvalScoresMV, createEvalScoresMV } from 'parquedb/observability/evals'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 *
 * // Create eval scores materialized view
 * const scoresMV = createEvalScoresMV(db, {
 *   granularity: 'day',
 *   aggregationDimensions: ['scorerName', 'modelId']
 * })
 *
 * // Refresh to process new scores
 * await scoresMV.refresh()
 *
 * // Get score summary
 * const summary = await scoresMV.getSummary({
 *   from: new Date('2026-02-01'),
 *   to: new Date('2026-02-03')
 * })
 *
 * console.log(`Total scores: ${summary.totalScores.toLocaleString()}`)
 * console.log(`Global average: ${(summary.globalAverageScore * 100).toFixed(1)}%`)
 * ```
 *
 * @module observability/evals
 */

// Types
export type {
  // Time Granularity
  TimeGranularity,

  // Score Record Types
  EvalScoreRecord,

  // Aggregation Types
  EvalScoreAggregate,
  EvalScoreSummary,
  ScoreTrendPoint,
  ScoreComparison,

  // Configuration Types
  AggregationDimension,
  EvalScoresMVConfig,
  ResolvedEvalScoresMVConfig,

  // Query/Filter Types
  EvalScoreQueryOptions,
  TrendQueryOptions,

  // Result Types
  RefreshResult,
  ScorerPerformance,
} from './types'

// EvalScoresMV
export { EvalScoresMV, createEvalScoresMV } from './EvalScoresMV'

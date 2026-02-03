/**
 * Eval Scores Observability Types for ParqueDB
 *
 * Type definitions for evaluation score tracking, aggregation, and analytics.
 * These types support the EvalScoresMV materialized view.
 *
 * @module observability/evals/types
 */

import type { MultiTenantConfig, CrossTenantAggregate, TenantUsageSummary } from '../ai/types'

// Re-export multi-tenancy types for convenience
export type { MultiTenantConfig, CrossTenantAggregate, TenantUsageSummary }

// =============================================================================
// Time Granularity
// =============================================================================

/**
 * Time granularity for score aggregation
 */
export type TimeGranularity = 'hour' | 'day' | 'week' | 'month'

// =============================================================================
// Score Record Types
// =============================================================================

/**
 * Individual eval score record (source data for materialized views)
 *
 * This matches the EvalScore structure from evalite integration.
 */
export interface EvalScoreRecord {
  /** Unique score ID */
  id: string
  /** Timestamp of the score */
  timestamp: Date
  /** Run ID this score belongs to */
  runId: number
  /** Suite ID this score belongs to */
  suiteId: number
  /** Suite name (evaluation file path) */
  suiteName: string
  /** Eval ID this score is for */
  evalId: number
  /** Scorer name (the scorer function used) */
  scorerName: string
  /** Score value (typically 0-1 range) */
  score: number
  /** Optional description from the scorer */
  description?: string
  /** Model ID used for this evaluation (if applicable) */
  modelId?: string
  /** Prompt template ID (if applicable) */
  promptId?: string
  /** Additional metadata */
  metadata?: Record<string, unknown>
}

// =============================================================================
// Score Aggregation Types
// =============================================================================

/**
 * Aggregated eval scores for a time period
 *
 * This is the output schema for EvalScoresMV.
 */
export interface EvalScoreAggregate {
  /** Unique aggregate ID */
  $id: string
  /** Type identifier */
  $type: 'EvalScoreAggregate'
  /** Display name */
  name: string
  /** Date key for aggregation (YYYY-MM-DD for daily, etc.) */
  dateKey: string
  /** Time granularity */
  granularity: TimeGranularity

  // Dimension Keys (what we're aggregating by)
  /** Scorer name (required dimension) */
  scorerName: string
  /** Suite name (optional dimension) */
  suiteName?: string
  /** Model ID (optional dimension) */
  modelId?: string
  /** Prompt ID (optional dimension) */
  promptId?: string

  // Score Statistics
  /** Total number of scores */
  scoreCount: number
  /** Sum of all scores */
  scoreSum: number
  /** Average score */
  scoreAvg: number
  /** Minimum score */
  scoreMin: number
  /** Maximum score */
  scoreMax: number
  /** Standard deviation of scores */
  scoreStdDev: number
  /** Variance of scores */
  scoreVariance: number

  // Score Distribution (10 buckets: 0-0.1, 0.1-0.2, ..., 0.9-1.0)
  /** Distribution buckets for histogram */
  distribution: number[]

  // Percentiles (estimated using reservoir sampling)
  /** P50 (median) score */
  p50Score: number
  /** P90 score */
  p90Score: number
  /** P95 score */
  p95Score: number
  /** P99 score */
  p99Score: number

  // Eval/Run counts
  /** Unique eval IDs in this aggregate */
  uniqueEvalCount: number
  /** Unique run IDs in this aggregate */
  uniqueRunCount: number
  /** Unique suite IDs in this aggregate */
  uniqueSuiteCount: number

  // Score Outcome Counts
  /** Number of scores >= 0.9 (excellent) */
  excellentCount: number
  /** Number of scores >= 0.7 and < 0.9 (good) */
  goodCount: number
  /** Number of scores >= 0.5 and < 0.7 (fair) */
  fairCount: number
  /** Number of scores < 0.5 (poor) */
  poorCount: number

  // Metadata
  /** When this aggregate was first created */
  createdAt: Date
  /** When this aggregate was last updated */
  updatedAt: Date
  /** Version for optimistic concurrency */
  version: number
  /** Tenant identifier (for multi-tenant deployments) */
  tenantId?: string
}

/**
 * Summary of eval scores across all scorers/suites
 */
export interface EvalScoreSummary {
  /** Total scores across all dimensions */
  totalScores: number
  /** Global average score */
  globalAverageScore: number
  /** Global min score */
  globalMinScore: number
  /** Global max score */
  globalMaxScore: number

  /** Unique scorers */
  uniqueScorers: number
  /** Unique suites */
  uniqueSuites: number
  /** Unique runs */
  uniqueRuns: number
  /** Unique models (if tracked) */
  uniqueModels: number

  /** Breakdown by scorer */
  byScorer: Record<string, {
    scoreCount: number
    averageScore: number
    minScore: number
    maxScore: number
    stdDev: number
  }>

  /** Breakdown by suite */
  bySuite: Record<string, {
    scoreCount: number
    averageScore: number
    minScore: number
    maxScore: number
  }>

  /** Breakdown by model (if tracked) */
  byModel: Record<string, {
    scoreCount: number
    averageScore: number
    minScore: number
    maxScore: number
  }>

  /** Breakdown by prompt (if tracked) */
  byPrompt: Record<string, {
    scoreCount: number
    averageScore: number
    minScore: number
    maxScore: number
  }>

  /** Time range of the summary */
  timeRange: {
    from: Date
    to: Date
  }
}

/**
 * Score trend data point for time-series analysis
 */
export interface ScoreTrendPoint {
  /** Date key (e.g., '2026-02-03') */
  dateKey: string
  /** Average score for this time period */
  averageScore: number
  /** Number of scores in this period */
  scoreCount: number
  /** Min score in this period */
  minScore: number
  /** Max score in this period */
  maxScore: number
  /** Standard deviation */
  stdDev: number
}

/**
 * Comparison result between two time periods or configurations
 */
export interface ScoreComparison {
  /** Baseline period/config name */
  baseline: string
  /** Comparison period/config name */
  comparison: string
  /** Baseline average score */
  baselineAvg: number
  /** Comparison average score */
  comparisonAvg: number
  /** Absolute difference */
  absoluteDiff: number
  /** Relative difference (percentage) */
  relativeDiff: number
  /** Whether the change is statistically significant */
  isSignificant: boolean
  /** P-value for significance test */
  pValue?: number
}

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Aggregation dimensions for EvalScoresMV
 */
export type AggregationDimension = 'scorerName' | 'suiteName' | 'modelId' | 'promptId'

/**
 * Configuration options for EvalScoresMV
 */
export interface EvalScoresMVConfig {
  /** Collection name for source eval scores (default: 'eval_scores') */
  sourceCollection?: string
  /** Collection name for aggregated scores (default: 'eval_score_aggregates') */
  targetCollection?: string
  /** Time granularity for aggregation (default: 'day') */
  granularity?: TimeGranularity
  /** Dimensions to aggregate by (default: ['scorerName']) */
  aggregationDimensions?: AggregationDimension[]
  /** Maximum age of scores to process in milliseconds (default: 90 days) */
  maxAgeMs?: number
  /** Batch size for processing (default: 1000) */
  batchSize?: number
  /** Number of distribution buckets (default: 10) */
  distributionBuckets?: number
  /** Whether to enable debug logging (default: false) */
  debug?: boolean
}

/**
 * Resolved configuration with defaults applied
 */
export interface ResolvedEvalScoresMVConfig {
  sourceCollection: string
  targetCollection: string
  granularity: TimeGranularity
  aggregationDimensions: AggregationDimension[]
  maxAgeMs: number
  batchSize: number
  distributionBuckets: number
  debug: boolean
}

// =============================================================================
// Query/Filter Types
// =============================================================================

/**
 * Filter options for querying eval score aggregates
 */
export interface EvalScoreQueryOptions {
  /** Filter by scorer name */
  scorerName?: string
  /** Filter by suite name */
  suiteName?: string
  /** Filter by model ID */
  modelId?: string
  /** Filter by prompt ID */
  promptId?: string
  /** Start date (inclusive) */
  from?: Date
  /** End date (inclusive) */
  to?: Date
  /** Time granularity */
  granularity?: TimeGranularity
  /** Maximum results to return */
  limit?: number
  /** Sort order */
  sort?: 'dateKey' | '-dateKey' | 'scoreAvg' | '-scoreAvg' | 'scoreCount' | '-scoreCount'
}

/**
 * Options for trend queries
 */
export interface TrendQueryOptions {
  /** Filter by scorer name */
  scorerName?: string
  /** Filter by suite name */
  suiteName?: string
  /** Filter by model ID */
  modelId?: string
  /** Start date */
  from?: Date
  /** End date */
  to?: Date
  /** Time granularity for trend buckets */
  granularity?: TimeGranularity
  /** Maximum number of data points */
  limit?: number
}

// =============================================================================
// Result Types
// =============================================================================

/**
 * Result of a refresh operation
 */
export interface RefreshResult {
  /** Whether the refresh was successful */
  success: boolean
  /** Number of source records processed */
  recordsProcessed: number
  /** Number of aggregates created/updated */
  aggregatesUpdated: number
  /** Duration of the refresh in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string
}

/**
 * Scorer performance statistics
 */
export interface ScorerPerformance {
  /** Scorer name */
  scorerName: string
  /** Total number of scores */
  totalScores: number
  /** Average score */
  averageScore: number
  /** Score trend (positive = improving) */
  trend: number
  /** Standard deviation */
  stdDev: number
  /** Percentage of excellent scores (>= 0.9) */
  excellentRate: number
  /** Percentage of poor scores (< 0.5) */
  poorRate: number
}

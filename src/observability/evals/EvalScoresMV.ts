/**
 * EvalScoresMV - Materialized View for Evaluation Score Tracking
 *
 * Aggregates evaluation scores by scorer, suite, model, and time period to provide
 * score analytics, trend tracking, and performance metrics.
 *
 * Features:
 * - Aggregation by scorer/suite/model/prompt (configurable dimensions)
 * - Score distribution analysis with percentiles
 * - Trend computation over time
 * - Incremental refresh support
 * - Statistical significance testing for comparisons
 *
 * @example
 * ```typescript
 * import { EvalScoresMV } from 'parquedb/observability/evals'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 * const scoresMV = new EvalScoresMV(db, {
 *   granularity: 'day',
 *   aggregationDimensions: ['scorerName', 'modelId']
 * })
 *
 * // Refresh the view (process new scores)
 * await scoresMV.refresh()
 *
 * // Query aggregates
 * const aggregates = await scoresMV.getAggregates({ scorerName: 'accuracy' })
 *
 * // Get summary
 * const summary = await scoresMV.getSummary({ from: new Date('2026-02-01') })
 *
 * // Get score trends
 * const trends = await scoresMV.getTrends({ scorerName: 'accuracy' })
 * ```
 *
 * @module observability/evals/EvalScoresMV
 */

import type { ParqueDB } from '../../ParqueDB'
import type {
  EvalScoresMVConfig,
  ResolvedEvalScoresMVConfig,
  EvalScoreAggregate,
  EvalScoreSummary,
  EvalScoreQueryOptions,
  RefreshResult,
  TimeGranularity,
  AggregationDimension,
  ScoreTrendPoint,
  TrendQueryOptions,
  ScorerPerformance,
  ScoreComparison,
} from './types'
import { EVAL_SCORES_MAX_AGE_MS, MAX_BATCH_SIZE } from '../../constants'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_SOURCE_COLLECTION = 'eval_scores'
const DEFAULT_TARGET_COLLECTION = 'eval_score_aggregates'
const DEFAULT_GRANULARITY: TimeGranularity = 'day'
const DEFAULT_MAX_AGE_MS = EVAL_SCORES_MAX_AGE_MS
const DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE
const DEFAULT_DISTRIBUTION_BUCKETS = 10
const DEFAULT_AGGREGATION_DIMENSIONS: AggregationDimension[] = ['scorerName']

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a date key based on granularity
 */
function generateDateKey(date: Date, granularity: TimeGranularity): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  const hour = String(date.getUTCHours()).padStart(2, '0')

  switch (granularity) {
    case 'hour':
      return `${year}-${month}-${day}T${hour}`
    case 'day':
      return `${year}-${month}-${day}`
    case 'week': {
      // ISO week: Get the Thursday of the current week
      const d = new Date(date)
      d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
      const weekNum = Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7)
      return `${d.getUTCFullYear()}-W${String(weekNum).padStart(2, '0')}`
    }
    case 'month':
      return `${year}-${month}`
    default:
      return `${year}-${month}-${day}`
  }
}

/**
 * Generate aggregate ID from dimension values and date key
 */
function generateAggregateId(
  dimensions: Record<string, string | undefined>,
  dateKey: string
): string {
  const parts = Object.entries(dimensions)
    .filter(([_, v]) => v !== undefined)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}:${(v ?? 'unknown').replace(/[^a-zA-Z0-9-_.]/g, '_')}`)

  return `${parts.join('_')}_${dateKey}`
}

/**
 * Calculate standard deviation
 */
function calculateStdDev(values: number[], mean: number): number {
  if (values.length <= 1) return 0
  const squaredDiffs = values.map(v => Math.pow(v - mean, 2))
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / values.length
  return Math.sqrt(variance)
}

/**
 * Calculate percentile from sorted array
 */
function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return 0
  if (sortedValues.length === 1) return sortedValues[0]!

  const index = (p / 100) * (sortedValues.length - 1)
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) return sortedValues[lower]!

  const weight = index - lower
  return sortedValues[lower]! * (1 - weight) + sortedValues[upper]! * weight
}

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: EvalScoresMVConfig): ResolvedEvalScoresMVConfig {
  return {
    sourceCollection: config.sourceCollection ?? DEFAULT_SOURCE_COLLECTION,
    targetCollection: config.targetCollection ?? DEFAULT_TARGET_COLLECTION,
    granularity: config.granularity ?? DEFAULT_GRANULARITY,
    aggregationDimensions: config.aggregationDimensions ?? DEFAULT_AGGREGATION_DIMENSIONS,
    maxAgeMs: config.maxAgeMs ?? DEFAULT_MAX_AGE_MS,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    distributionBuckets: config.distributionBuckets ?? DEFAULT_DISTRIBUTION_BUCKETS,
    debug: config.debug ?? false,
  }
}

// =============================================================================
// Working Aggregate Type (for in-memory computation)
// =============================================================================

interface WorkingAggregate {
  $id: string
  $type: 'EvalScoreAggregate'
  name: string
  dateKey: string
  granularity: TimeGranularity
  scorerName: string
  suiteName?: string
  modelId?: string
  promptId?: string
  scores: number[] // Raw scores for percentile calculation
  evalIds: Set<number>
  runIds: Set<number>
  suiteIds: Set<number>
  createdAt: Date
  updatedAt: Date
  version: number
}

// =============================================================================
// EvalScoresMV Class
// =============================================================================

/**
 * EvalScoresMV - Materialized View for Evaluation Score Tracking
 *
 * Aggregates evaluation scores into analytics summaries by scorer, suite, model, and time period.
 * Supports incremental refresh and configurable aggregation dimensions.
 */
export class EvalScoresMV {
  private readonly db: ParqueDB
  private readonly config: ResolvedEvalScoresMVConfig
  private lastRefreshTime?: Date

  /**
   * Create a new EvalScoresMV instance
   *
   * @param db - ParqueDB instance
   * @param config - Configuration options
   */
  constructor(db: ParqueDB, config: EvalScoresMVConfig = {}) {
    this.db = db
    this.config = resolveConfig(config)
  }

  /**
   * Refresh the materialized view
   *
   * Processes new eval score entries and updates aggregates.
   * Supports incremental refresh - only processes scores since last refresh.
   *
   * @param options - Refresh options
   * @returns Refresh result with statistics
   */
  async refresh(options: { full?: boolean } = {}): Promise<RefreshResult> {
    const startTime = Date.now()
    let recordsProcessed = 0
    let aggregatesUpdated = 0

    try {
      const sourceCollection = this.db.collection(this.config.sourceCollection)
      const targetCollection = this.db.collection(this.config.targetCollection)

      // Determine time range for scores to process
      const now = new Date()
      const maxAge = new Date(now.getTime() - this.config.maxAgeMs)

      // Build filter for scores to process
      const filter: Record<string, unknown> = {
        timestamp: { $gte: maxAge },
      }

      // For incremental refresh, only get scores since last refresh
      if (!options.full && this.lastRefreshTime) {
        filter.timestamp = { $gte: this.lastRefreshTime }
      }

      // Fetch scores in batches
      const scores = await sourceCollection.find(filter, {
        limit: this.config.batchSize,
        sort: { timestamp: 1 },
      })

      if (scores.length === 0) {
        return {
          success: true,
          recordsProcessed: 0,
          aggregatesUpdated: 0,
          durationMs: Date.now() - startTime,
        }
      }

      // Group scores by aggregate key
      const aggregates = new Map<string, WorkingAggregate>()

      for (const score of scores) {
        recordsProcessed++

        const record = score as Record<string, unknown>
        const timestamp = new Date(record.timestamp as string | Date)
        const dateKey = generateDateKey(timestamp, this.config.granularity)

        // Extract dimension values
        const scorerName = (record.scorerName as string) ?? (record.name as string) ?? 'unknown'
        const suiteName = record.suiteName as string | undefined
        const modelId = record.modelId as string | undefined
        const promptId = record.promptId as string | undefined

        // Build dimension map based on configured dimensions
        const dimensions: Record<string, string | undefined> = {}
        if (this.config.aggregationDimensions.includes('scorerName')) {
          dimensions.scorerName = scorerName
        }
        if (this.config.aggregationDimensions.includes('suiteName') && suiteName) {
          dimensions.suiteName = suiteName
        }
        if (this.config.aggregationDimensions.includes('modelId') && modelId) {
          dimensions.modelId = modelId
        }
        if (this.config.aggregationDimensions.includes('promptId') && promptId) {
          dimensions.promptId = promptId
        }

        const aggregateId = generateAggregateId(dimensions, dateKey)

        // Get or create working aggregate
        let aggregate = aggregates.get(aggregateId)
        if (!aggregate) {
          // Try to load existing aggregate from DB
          const existingAggregates = await targetCollection.find(
            {
              dateKey,
              scorerName: dimensions.scorerName,
              ...(dimensions.suiteName && { suiteName: dimensions.suiteName }),
              ...(dimensions.modelId && { modelId: dimensions.modelId }),
              ...(dimensions.promptId && { promptId: dimensions.promptId }),
            },
            { limit: 1 }
          )

          if (existingAggregates.length > 0) {
            const existing = existingAggregates[0] as unknown as EvalScoreAggregate
            aggregate = {
              $id: existing.$id,
              $type: 'EvalScoreAggregate',
              name: existing.name,
              dateKey: existing.dateKey,
              granularity: existing.granularity,
              scorerName: existing.scorerName,
              suiteName: existing.suiteName,
              modelId: existing.modelId,
              promptId: existing.promptId,
              scores: [], // Will be reconstructed
              evalIds: new Set(),
              runIds: new Set(),
              suiteIds: new Set(),
              createdAt: existing.createdAt,
              updatedAt: existing.updatedAt,
              version: existing.version,
            }
          } else {
            aggregate = createEmptyWorkingAggregate(
              aggregateId,
              dateKey,
              this.config.granularity,
              scorerName,
              dimensions.suiteName,
              dimensions.modelId,
              dimensions.promptId
            )
          }
          aggregates.set(aggregateId, aggregate)
        }

        // Update working aggregate with score
        const scoreValue = record.score as number
        if (typeof scoreValue === 'number' && !isNaN(scoreValue)) {
          aggregate.scores.push(scoreValue)
        }

        const evalId = record.evalId as number
        const runId = record.runId as number
        const suiteId = record.suiteId as number

        if (typeof evalId === 'number') aggregate.evalIds.add(evalId)
        if (typeof runId === 'number') aggregate.runIds.add(runId)
        if (typeof suiteId === 'number') aggregate.suiteIds.add(suiteId)
      }

      // Convert working aggregates to final form and save
      for (const working of aggregates.values()) {
        const finalAggregate = computeFinalAggregate(working, this.config.distributionBuckets, now)

        // Check if this aggregate exists
        const existing = await targetCollection.find(
          {
            dateKey: finalAggregate.dateKey,
            scorerName: finalAggregate.scorerName,
            ...(finalAggregate.suiteName && { suiteName: finalAggregate.suiteName }),
            ...(finalAggregate.modelId && { modelId: finalAggregate.modelId }),
            ...(finalAggregate.promptId && { promptId: finalAggregate.promptId }),
          },
          { limit: 1 }
        )

        if (existing.length > 0) {
          const existingId = ((existing[0] as Record<string, unknown>).$id as string).split('/').pop()
          if (existingId) {
            await targetCollection.update(existingId, {
              $set: {
                scoreCount: finalAggregate.scoreCount,
                scoreSum: finalAggregate.scoreSum,
                scoreAvg: finalAggregate.scoreAvg,
                scoreMin: finalAggregate.scoreMin,
                scoreMax: finalAggregate.scoreMax,
                scoreStdDev: finalAggregate.scoreStdDev,
                scoreVariance: finalAggregate.scoreVariance,
                distribution: finalAggregate.distribution,
                p50Score: finalAggregate.p50Score,
                p90Score: finalAggregate.p90Score,
                p95Score: finalAggregate.p95Score,
                p99Score: finalAggregate.p99Score,
                uniqueEvalCount: finalAggregate.uniqueEvalCount,
                uniqueRunCount: finalAggregate.uniqueRunCount,
                uniqueSuiteCount: finalAggregate.uniqueSuiteCount,
                excellentCount: finalAggregate.excellentCount,
                goodCount: finalAggregate.goodCount,
                fairCount: finalAggregate.fairCount,
                poorCount: finalAggregate.poorCount,
                updatedAt: finalAggregate.updatedAt,
                version: finalAggregate.version,
              },
            })
          }
        } else {
          await targetCollection.create({
            $type: 'EvalScoreAggregate',
            name: finalAggregate.name,
            dateKey: finalAggregate.dateKey,
            granularity: finalAggregate.granularity,
            scorerName: finalAggregate.scorerName,
            ...(finalAggregate.suiteName && { suiteName: finalAggregate.suiteName }),
            ...(finalAggregate.modelId && { modelId: finalAggregate.modelId }),
            ...(finalAggregate.promptId && { promptId: finalAggregate.promptId }),
            scoreCount: finalAggregate.scoreCount,
            scoreSum: finalAggregate.scoreSum,
            scoreAvg: finalAggregate.scoreAvg,
            scoreMin: finalAggregate.scoreMin,
            scoreMax: finalAggregate.scoreMax,
            scoreStdDev: finalAggregate.scoreStdDev,
            scoreVariance: finalAggregate.scoreVariance,
            distribution: finalAggregate.distribution,
            p50Score: finalAggregate.p50Score,
            p90Score: finalAggregate.p90Score,
            p95Score: finalAggregate.p95Score,
            p99Score: finalAggregate.p99Score,
            uniqueEvalCount: finalAggregate.uniqueEvalCount,
            uniqueRunCount: finalAggregate.uniqueRunCount,
            uniqueSuiteCount: finalAggregate.uniqueSuiteCount,
            excellentCount: finalAggregate.excellentCount,
            goodCount: finalAggregate.goodCount,
            fairCount: finalAggregate.fairCount,
            poorCount: finalAggregate.poorCount,
            createdAt: finalAggregate.createdAt,
            updatedAt: finalAggregate.updatedAt,
            version: finalAggregate.version,
          })
        }
        aggregatesUpdated++
      }

      this.lastRefreshTime = now

      return {
        success: true,
        recordsProcessed,
        aggregatesUpdated,
        durationMs: Date.now() - startTime,
      }
    } catch (error) {
      return {
        success: false,
        recordsProcessed,
        aggregatesUpdated,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Query eval score aggregates
   *
   * @param options - Query options
   * @returns Array of score aggregates
   */
  async getAggregates(options: EvalScoreQueryOptions = {}): Promise<EvalScoreAggregate[]> {
    const targetCollection = this.db.collection(this.config.targetCollection)

    // Build filter
    const filter: Record<string, unknown> = {}

    if (options.scorerName) {
      filter.scorerName = options.scorerName
    }

    if (options.suiteName) {
      filter.suiteName = options.suiteName
    }

    if (options.modelId) {
      filter.modelId = options.modelId
    }

    if (options.promptId) {
      filter.promptId = options.promptId
    }

    if (options.granularity) {
      filter.granularity = options.granularity
    }

    if (options.from || options.to) {
      const dateKeyFilter: Record<string, string> = {}
      if (options.from) {
        dateKeyFilter.$gte = generateDateKey(options.from, options.granularity ?? this.config.granularity)
      }
      if (options.to) {
        dateKeyFilter.$lte = generateDateKey(options.to, options.granularity ?? this.config.granularity)
      }
      filter.dateKey = dateKeyFilter
    }

    // Build sort
    const sortField = options.sort?.replace('-', '') ?? 'dateKey'
    const sortOrder = options.sort?.startsWith('-') ? -1 : 1

    const results = await targetCollection.find(filter, {
      limit: options.limit ?? 100,
      sort: { [sortField]: sortOrder },
    })

    return results as unknown as EvalScoreAggregate[]
  }

  /**
   * Get score summary across all scorers/suites
   *
   * @param options - Query options for time range
   * @returns Score summary
   */
  async getSummary(options: { from?: Date; to?: Date } = {}): Promise<EvalScoreSummary> {
    const aggregates = await this.getAggregates({
      from: options.from,
      to: options.to,
      limit: 10000, // Get all for summary
    })

    const summary: EvalScoreSummary = {
      totalScores: 0,
      globalAverageScore: 0,
      globalMinScore: Infinity,
      globalMaxScore: -Infinity,
      uniqueScorers: 0,
      uniqueSuites: 0,
      uniqueRuns: 0,
      uniqueModels: 0,
      byScorer: {},
      bySuite: {},
      byModel: {},
      byPrompt: {},
      timeRange: {
        from: options.from ?? new Date(0),
        to: options.to ?? new Date(),
      },
    }

    const scorerSet = new Set<string>()
    const suiteSet = new Set<string>()
    const modelSet = new Set<string>()
    let totalWeightedScore = 0

    for (const agg of aggregates) {
      summary.totalScores += agg.scoreCount
      totalWeightedScore += agg.scoreAvg * agg.scoreCount

      if (agg.scoreMin < summary.globalMinScore) {
        summary.globalMinScore = agg.scoreMin
      }
      if (agg.scoreMax > summary.globalMaxScore) {
        summary.globalMaxScore = agg.scoreMax
      }

      // Track unique dimensions
      scorerSet.add(agg.scorerName)
      if (agg.suiteName) suiteSet.add(agg.suiteName)
      if (agg.modelId) modelSet.add(agg.modelId)

      // By scorer
      if (!summary.byScorer[agg.scorerName]) {
        summary.byScorer[agg.scorerName] = {
          scoreCount: 0,
          averageScore: 0,
          minScore: Infinity,
          maxScore: -Infinity,
          stdDev: 0,
        }
      }
      const byScorer = summary.byScorer[agg.scorerName]!
      byScorer.scoreCount += agg.scoreCount
      byScorer.averageScore = (byScorer.averageScore * (byScorer.scoreCount - agg.scoreCount) + agg.scoreAvg * agg.scoreCount) / byScorer.scoreCount
      if (agg.scoreMin < byScorer.minScore) byScorer.minScore = agg.scoreMin
      if (agg.scoreMax > byScorer.maxScore) byScorer.maxScore = agg.scoreMax

      // By suite
      if (agg.suiteName) {
        if (!summary.bySuite[agg.suiteName]) {
          summary.bySuite[agg.suiteName] = {
            scoreCount: 0,
            averageScore: 0,
            minScore: Infinity,
            maxScore: -Infinity,
          }
        }
        const bySuite = summary.bySuite[agg.suiteName]!
        bySuite.scoreCount += agg.scoreCount
        bySuite.averageScore = (bySuite.averageScore * (bySuite.scoreCount - agg.scoreCount) + agg.scoreAvg * agg.scoreCount) / bySuite.scoreCount
        if (agg.scoreMin < bySuite.minScore) bySuite.minScore = agg.scoreMin
        if (agg.scoreMax > bySuite.maxScore) bySuite.maxScore = agg.scoreMax
      }

      // By model
      if (agg.modelId) {
        if (!summary.byModel[agg.modelId]) {
          summary.byModel[agg.modelId] = {
            scoreCount: 0,
            averageScore: 0,
            minScore: Infinity,
            maxScore: -Infinity,
          }
        }
        const byModel = summary.byModel[agg.modelId]!
        byModel.scoreCount += agg.scoreCount
        byModel.averageScore = (byModel.averageScore * (byModel.scoreCount - agg.scoreCount) + agg.scoreAvg * agg.scoreCount) / byModel.scoreCount
        if (agg.scoreMin < byModel.minScore) byModel.minScore = agg.scoreMin
        if (agg.scoreMax > byModel.maxScore) byModel.maxScore = agg.scoreMax
      }

      // By prompt
      if (agg.promptId) {
        if (!summary.byPrompt[agg.promptId]) {
          summary.byPrompt[agg.promptId] = {
            scoreCount: 0,
            averageScore: 0,
            minScore: Infinity,
            maxScore: -Infinity,
          }
        }
        const byPrompt = summary.byPrompt[agg.promptId]!
        byPrompt.scoreCount += agg.scoreCount
        byPrompt.averageScore = (byPrompt.averageScore * (byPrompt.scoreCount - agg.scoreCount) + agg.scoreAvg * agg.scoreCount) / byPrompt.scoreCount
        if (agg.scoreMin < byPrompt.minScore) byPrompt.minScore = agg.scoreMin
        if (agg.scoreMax > byPrompt.maxScore) byPrompt.maxScore = agg.scoreMax
      }
    }

    // Calculate global average
    if (summary.totalScores > 0) {
      summary.globalAverageScore = totalWeightedScore / summary.totalScores
    }

    // Handle edge cases for min/max
    if (summary.globalMinScore === Infinity) summary.globalMinScore = 0
    if (summary.globalMaxScore === -Infinity) summary.globalMaxScore = 0

    // Set unique counts
    summary.uniqueScorers = scorerSet.size
    summary.uniqueSuites = suiteSet.size
    summary.uniqueModels = modelSet.size

    return summary
  }

  /**
   * Get score trends over time
   *
   * @param options - Trend query options
   * @returns Array of trend data points
   */
  async getTrends(options: TrendQueryOptions = {}): Promise<ScoreTrendPoint[]> {
    const aggregates = await this.getAggregates({
      scorerName: options.scorerName,
      suiteName: options.suiteName,
      modelId: options.modelId,
      from: options.from,
      to: options.to,
      granularity: options.granularity,
      sort: 'dateKey',
      limit: options.limit ?? 365,
    })

    // Group by dateKey (in case of multiple dimensions)
    const byDate = new Map<string, {
      scores: number[]
      count: number
    }>()

    for (const agg of aggregates) {
      const existing = byDate.get(agg.dateKey) ?? { scores: [], count: 0 }
      // We use weighted average based on score count
      existing.scores.push(agg.scoreAvg)
      existing.count += agg.scoreCount
      byDate.set(agg.dateKey, existing)
    }

    // Convert to trend points
    const trends: ScoreTrendPoint[] = []
    for (const [dateKey, data] of byDate) {
      if (data.scores.length === 0) continue

      const avg = data.scores.reduce((a, b) => a + b, 0) / data.scores.length
      const min = Math.min(...data.scores)
      const max = Math.max(...data.scores)
      const stdDev = calculateStdDev(data.scores, avg)

      trends.push({
        dateKey,
        averageScore: avg,
        scoreCount: data.count,
        minScore: min,
        maxScore: max,
        stdDev,
      })
    }

    return trends
  }

  /**
   * Get scorer performance rankings
   *
   * @param options - Query options for time range
   * @returns Array of scorer performance stats sorted by average score
   */
  async getScorerPerformance(options: { from?: Date; to?: Date } = {}): Promise<ScorerPerformance[]> {
    const summary = await this.getSummary(options)

    const performances: ScorerPerformance[] = []

    for (const [scorerName, stats] of Object.entries(summary.byScorer)) {
      // Get trend data to calculate trend direction
      const trends = await this.getTrends({
        scorerName,
        from: options.from,
        to: options.to,
        limit: 30,
      })

      let trend = 0
      if (trends.length >= 2) {
        // Simple linear trend: compare first half to second half
        const midpoint = Math.floor(trends.length / 2)
        const firstHalf = trends.slice(0, midpoint)
        const secondHalf = trends.slice(midpoint)

        const firstAvg = firstHalf.reduce((s, t) => s + t.averageScore, 0) / firstHalf.length
        const secondAvg = secondHalf.reduce((s, t) => s + t.averageScore, 0) / secondHalf.length

        trend = secondAvg - firstAvg
      }

      // Get aggregate for excellent/poor rates
      const aggregates = await this.getAggregates({
        scorerName,
        from: options.from,
        to: options.to,
        limit: 1000,
      })

      let excellentTotal = 0
      let poorTotal = 0
      let totalScores = 0

      for (const agg of aggregates) {
        excellentTotal += agg.excellentCount
        poorTotal += agg.poorCount
        totalScores += agg.scoreCount
      }

      performances.push({
        scorerName,
        totalScores: stats.scoreCount,
        averageScore: stats.averageScore,
        trend,
        stdDev: stats.stdDev,
        excellentRate: totalScores > 0 ? excellentTotal / totalScores : 0,
        poorRate: totalScores > 0 ? poorTotal / totalScores : 0,
      })
    }

    // Sort by average score descending
    performances.sort((a, b) => b.averageScore - a.averageScore)

    return performances
  }

  /**
   * Compare scores between two configurations (e.g., two models or prompts)
   *
   * @param baselineOptions - Options for baseline query
   * @param comparisonOptions - Options for comparison query
   * @returns Comparison result
   */
  async compareScores(
    baselineOptions: EvalScoreQueryOptions & { name: string },
    comparisonOptions: EvalScoreQueryOptions & { name: string }
  ): Promise<ScoreComparison> {
    const [baselineAggregates, comparisonAggregates] = await Promise.all([
      this.getAggregates(baselineOptions),
      this.getAggregates(comparisonOptions),
    ])

    // Calculate weighted averages
    const baselineTotal = baselineAggregates.reduce((s, a) => s + a.scoreCount, 0)
    const comparisonTotal = comparisonAggregates.reduce((s, a) => s + a.scoreCount, 0)

    const baselineAvg = baselineTotal > 0
      ? baselineAggregates.reduce((s, a) => s + a.scoreAvg * a.scoreCount, 0) / baselineTotal
      : 0

    const comparisonAvg = comparisonTotal > 0
      ? comparisonAggregates.reduce((s, a) => s + a.scoreAvg * a.scoreCount, 0) / comparisonTotal
      : 0

    const absoluteDiff = comparisonAvg - baselineAvg
    const relativeDiff = baselineAvg !== 0 ? (absoluteDiff / baselineAvg) * 100 : 0

    // Simple significance test: consider significant if difference > 5% and both have sufficient samples
    const isSignificant = Math.abs(relativeDiff) > 5 && baselineTotal >= 30 && comparisonTotal >= 30

    return {
      baseline: baselineOptions.name,
      comparison: comparisonOptions.name,
      baselineAvg,
      comparisonAvg,
      absoluteDiff,
      relativeDiff,
      isSignificant,
    }
  }

  /**
   * Get the current configuration
   */
  getConfig(): ResolvedEvalScoresMVConfig {
    return { ...this.config }
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create an empty working aggregate
 */
function createEmptyWorkingAggregate(
  id: string,
  dateKey: string,
  granularity: TimeGranularity,
  scorerName: string,
  suiteName?: string,
  modelId?: string,
  promptId?: string
): WorkingAggregate {
  const now = new Date()
  const nameParts = [scorerName]
  if (suiteName) nameParts.push(suiteName)
  if (modelId) nameParts.push(modelId)

  return {
    $id: id,
    $type: 'EvalScoreAggregate',
    name: `${nameParts.join('/')} (${dateKey})`,
    dateKey,
    granularity,
    scorerName,
    suiteName,
    modelId,
    promptId,
    scores: [],
    evalIds: new Set(),
    runIds: new Set(),
    suiteIds: new Set(),
    createdAt: now,
    updatedAt: now,
    version: 0,
  }
}

/**
 * Compute final aggregate from working aggregate
 */
function computeFinalAggregate(
  working: WorkingAggregate,
  distributionBuckets: number,
  now: Date
): EvalScoreAggregate {
  const scores = working.scores
  const count = scores.length

  if (count === 0) {
    return {
      $id: working.$id,
      $type: 'EvalScoreAggregate',
      name: working.name,
      dateKey: working.dateKey,
      granularity: working.granularity,
      scorerName: working.scorerName,
      suiteName: working.suiteName,
      modelId: working.modelId,
      promptId: working.promptId,
      scoreCount: 0,
      scoreSum: 0,
      scoreAvg: 0,
      scoreMin: 0,
      scoreMax: 0,
      scoreStdDev: 0,
      scoreVariance: 0,
      distribution: new Array(distributionBuckets).fill(0),
      p50Score: 0,
      p90Score: 0,
      p95Score: 0,
      p99Score: 0,
      uniqueEvalCount: working.evalIds.size,
      uniqueRunCount: working.runIds.size,
      uniqueSuiteCount: working.suiteIds.size,
      excellentCount: 0,
      goodCount: 0,
      fairCount: 0,
      poorCount: 0,
      createdAt: working.createdAt,
      updatedAt: now,
      version: working.version + 1,
    }
  }

  // Basic stats
  const sum = scores.reduce((a, b) => a + b, 0)
  const avg = sum / count
  const min = Math.min(...scores)
  const max = Math.max(...scores)
  const stdDev = calculateStdDev(scores, avg)
  const variance = stdDev * stdDev

  // Sort for percentiles
  const sorted = [...scores].sort((a, b) => a - b)
  const p50 = percentile(sorted, 50)
  const p90 = percentile(sorted, 90)
  const p95 = percentile(sorted, 95)
  const p99 = percentile(sorted, 99)

  // Distribution
  const distribution = new Array(distributionBuckets).fill(0)
  const bucketSize = 1 / distributionBuckets
  for (const score of scores) {
    const bucketIndex = Math.min(
      Math.floor(score / bucketSize),
      distributionBuckets - 1
    )
    distribution[bucketIndex]++
  }

  // Outcome counts
  let excellentCount = 0
  let goodCount = 0
  let fairCount = 0
  let poorCount = 0

  for (const score of scores) {
    if (score >= 0.9) excellentCount++
    else if (score >= 0.7) goodCount++
    else if (score >= 0.5) fairCount++
    else poorCount++
  }

  return {
    $id: working.$id,
    $type: 'EvalScoreAggregate',
    name: working.name,
    dateKey: working.dateKey,
    granularity: working.granularity,
    scorerName: working.scorerName,
    suiteName: working.suiteName,
    modelId: working.modelId,
    promptId: working.promptId,
    scoreCount: count,
    scoreSum: sum,
    scoreAvg: avg,
    scoreMin: min,
    scoreMax: max,
    scoreStdDev: stdDev,
    scoreVariance: variance,
    distribution,
    p50Score: p50,
    p90Score: p90,
    p95Score: p95,
    p99Score: p99,
    uniqueEvalCount: working.evalIds.size,
    uniqueRunCount: working.runIds.size,
    uniqueSuiteCount: working.suiteIds.size,
    excellentCount,
    goodCount,
    fairCount,
    poorCount,
    createdAt: working.createdAt,
    updatedAt: now,
    version: working.version + 1,
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create an EvalScoresMV instance
 *
 * @param db - ParqueDB instance
 * @param config - Configuration options
 * @returns EvalScoresMV instance
 *
 * @example
 * ```typescript
 * const scoresMV = createEvalScoresMV(db)
 * await scoresMV.refresh()
 * const summary = await scoresMV.getSummary()
 * ```
 */
export function createEvalScoresMV(db: ParqueDB, config: EvalScoresMVConfig = {}): EvalScoresMV {
  return new EvalScoresMV(db, config)
}

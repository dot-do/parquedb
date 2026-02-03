/**
 * Eval Scores Materialized View
 *
 * A streaming MV that tracks, indexes, and analyzes AI evaluation scores
 * from evalite runs. Provides real-time score analytics and trend tracking.
 *
 * Features:
 * - Score tracking by run, suite, and scorer
 * - Score distribution analysis
 * - Trend computation over time
 * - Aggregation by multiple dimensions
 * - Rolling statistics computation
 */

import type { Event } from '../types/entity'
import type { MVHandler, StreamingStats } from './types'

// =============================================================================
// Eval Score Types
// =============================================================================

/**
 * Individual evaluation score record
 */
export interface EvalScoreRecord {
  /** Unique score ID */
  id: string
  /** Timestamp of the score */
  ts: number
  /** Run ID this score belongs to */
  runId: number
  /** Suite name (evaluation file name) */
  suiteName: string
  /** Scorer name (the scorer function used) */
  scorerName: string
  /** Score value (typically 0-1 range) */
  score: number
  /** Optional description from the scorer */
  description?: string | undefined
  /** Eval ID this score is for */
  evalId: number
  /** Additional metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Score statistics for a specific scorer/suite combination
 */
export interface ScoreStatistics {
  /** Total number of scores */
  count: number
  /** Sum of all scores */
  sum: number
  /** Average score */
  average: number
  /** Minimum score */
  min: number
  /** Maximum score */
  max: number
  /** Standard deviation */
  stdDev: number
  /** Score distribution buckets (0-0.1, 0.1-0.2, ..., 0.9-1.0) */
  distribution: number[]
  /** Latest score timestamp */
  lastUpdated: number
}

/**
 * Score trend point for time-series analysis
 */
export interface ScoreTrendPoint {
  /** Timestamp bucket */
  timestamp: number
  /** Run ID */
  runId: number
  /** Average score in this time bucket */
  averageScore: number
  /** Number of scores in this bucket */
  count: number
  /** Min score in bucket */
  minScore: number
  /** Max score in bucket */
  maxScore: number
}

/**
 * Aggregated eval scores statistics
 */
export interface EvalScoresStats {
  /** Total score records */
  totalScores: number
  /** Unique runs */
  uniqueRuns: number
  /** Unique suites */
  uniqueSuites: number
  /** Unique scorers */
  uniqueScorers: number
  /** Global average score */
  globalAverageScore: number
  /** Scores by scorer */
  byScorer: Record<string, ScoreStatistics>
  /** Scores by suite */
  bySuite: Record<string, ScoreStatistics>
  /** Scores by run */
  byRun: Record<number, ScoreStatistics>
  /** Time range covered */
  timeRange: {
    start: number
    end: number
  }
}

// =============================================================================
// Eval Scores MV Configuration
// =============================================================================

/**
 * Configuration for the EvalScores MV
 */
export interface EvalScoresConfig {
  /** Maximum scores to retain in memory (default: 50000) */
  maxScores?: number | undefined
  /** Window size for rolling stats in ms (default: 3600000 = 1 hour) */
  statsWindowMs?: number | undefined
  /** Namespaces to listen for score events (default: ['evalite_scores', 'scores']) */
  sourceNamespaces?: string[] | undefined
  /** Number of distribution buckets (default: 10) */
  distributionBuckets?: number | undefined
}

const DEFAULT_CONFIG: Required<EvalScoresConfig> = {
  maxScores: 50000,
  statsWindowMs: 3600000, // 1 hour
  sourceNamespaces: ['evalite_scores', 'scores'],
  distributionBuckets: 10,
}

// =============================================================================
// Eval Scores MV Implementation
// =============================================================================

/**
 * Materialized View for Eval Scores
 *
 * Processes score events and maintains:
 * - Score records list
 * - Indexes by run, suite, scorer, eval
 * - Rolling statistics per dimension
 */
export class EvalScoresMV implements MVHandler {
  readonly name = 'EvalScores'
  readonly sourceNamespaces: string[]

  private readonly config: Required<EvalScoresConfig>

  // Score storage
  private scores: EvalScoreRecord[] = []

  // Indexes for fast lookup
  private byId: Map<string, EvalScoreRecord> = new Map()
  private byRunId: Map<number, Set<string>> = new Map()
  private bySuiteName: Map<string, Set<string>> = new Map()
  private byScorerName: Map<string, Set<string>> = new Map()
  private byEvalId: Map<number, Set<string>> = new Map()

  // Cached statistics (lazily computed)
  private statsCache: EvalScoresStats | null = null
  private statsCacheTime = 0
  private readonly statsCacheTtlMs = 1000 // 1 second TTL

  constructor(config: EvalScoresConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config }
    this.sourceNamespaces = this.config.sourceNamespaces
  }

  // ===========================================================================
  // MVHandler Implementation
  // ===========================================================================

  /**
   * Process a batch of events
   */
  async process(events: Event[]): Promise<void> {
    for (const event of events) {
      // Only process score-like events
      if (!this.isScoreEvent(event)) continue

      const score = this.extractScore(event)
      if (score) {
        this.addScore(score)
      }
    }
  }

  // ===========================================================================
  // Query Methods
  // ===========================================================================

  /**
   * Get all scores (most recent first)
   */
  getScores(limit?: number): EvalScoreRecord[] {
    const result = [...this.scores].reverse()
    return limit ? result.slice(0, limit) : result
  }

  /**
   * Get scores by run ID
   */
  getScoresByRun(runId: number, limit?: number): EvalScoreRecord[] {
    const ids = this.byRunId.get(runId) ?? new Set()
    return this.getScoresById(ids, limit)
  }

  /**
   * Get scores by suite name
   */
  getScoresBySuite(suiteName: string, limit?: number): EvalScoreRecord[] {
    const ids = this.bySuiteName.get(suiteName) ?? new Set()
    return this.getScoresById(ids, limit)
  }

  /**
   * Get scores by scorer name
   */
  getScoresByScorer(scorerName: string, limit?: number): EvalScoreRecord[] {
    const ids = this.byScorerName.get(scorerName) ?? new Set()
    return this.getScoresById(ids, limit)
  }

  /**
   * Get scores by eval ID
   */
  getScoresByEval(evalId: number, limit?: number): EvalScoreRecord[] {
    const ids = this.byEvalId.get(evalId) ?? new Set()
    return this.getScoresById(ids, limit)
  }

  /**
   * Get scores within a time range
   */
  getScoresInRange(startTs: number, endTs: number): EvalScoreRecord[] {
    return this.scores.filter(s => s.ts >= startTs && s.ts <= endTs)
  }

  /**
   * Get score by ID
   */
  getScore(id: string): EvalScoreRecord | undefined {
    return this.byId.get(id)
  }

  /**
   * Get score trends over time for a specific scorer/suite
   */
  getScoreTrends(
    options: {
      scorerName?: string | undefined
      suiteName?: string | undefined
      limit?: number | undefined
      bucketSizeMs?: number | undefined
    } = {}
  ): ScoreTrendPoint[] {
    const { scorerName, suiteName, limit = 100, bucketSizeMs = 3600000 } = options

    // Filter scores
    let filteredScores = this.scores
    if (scorerName) {
      const ids = this.byScorerName.get(scorerName) ?? new Set()
      filteredScores = filteredScores.filter(s => ids.has(s.id))
    }
    if (suiteName) {
      const ids = this.bySuiteName.get(suiteName) ?? new Set()
      filteredScores = filteredScores.filter(s => ids.has(s.id))
    }

    if (filteredScores.length === 0) return []

    // Group by time buckets
    const buckets = new Map<number, { scores: number[]; runIds: Set<number> }>()

    for (const score of filteredScores) {
      const bucketTime = Math.floor(score.ts / bucketSizeMs) * bucketSizeMs
      let bucket = buckets.get(bucketTime)
      if (!bucket) {
        bucket = { scores: [], runIds: new Set() }
        buckets.set(bucketTime, bucket)
      }
      bucket.scores.push(score.score)
      bucket.runIds.add(score.runId)
    }

    // Convert to trend points
    const trends: ScoreTrendPoint[] = []
    for (const [timestamp, bucket] of buckets) {
      const avgScore = bucket.scores.reduce((a, b) => a + b, 0) / bucket.scores.length
      trends.push({
        timestamp,
        runId: Math.max(...bucket.runIds), // Use most recent run ID
        averageScore: avgScore,
        count: bucket.scores.length,
        minScore: Math.min(...bucket.scores),
        maxScore: Math.max(...bucket.scores),
      })
    }

    // Sort by timestamp descending and limit
    trends.sort((a, b) => b.timestamp - a.timestamp)
    return trends.slice(0, limit)
  }

  /**
   * Get statistics for a specific scorer
   */
  getScorerStats(scorerName: string): ScoreStatistics | null {
    const ids = this.byScorerName.get(scorerName)
    if (!ids || ids.size === 0) return null
    return this.computeStatistics(ids)
  }

  /**
   * Get statistics for a specific suite
   */
  getSuiteStats(suiteName: string): ScoreStatistics | null {
    const ids = this.bySuiteName.get(suiteName)
    if (!ids || ids.size === 0) return null
    return this.computeStatistics(ids)
  }

  /**
   * Get statistics for a specific run
   */
  getRunStats(runId: number): ScoreStatistics | null {
    const ids = this.byRunId.get(runId)
    if (!ids || ids.size === 0) return null
    return this.computeStatistics(ids)
  }

  /**
   * Get aggregated statistics
   */
  getStats(): EvalScoresStats {
    const now = Date.now()

    // Return cached stats if still valid
    if (this.statsCache && now - this.statsCacheTime < this.statsCacheTtlMs) {
      return this.statsCache
    }

    // Compute fresh stats
    const stats = this.computeAggregatedStats()
    this.statsCache = stats
    this.statsCacheTime = now

    return stats
  }

  /**
   * Get score count
   */
  count(): number {
    return this.scores.length
  }

  /**
   * Get unique scorer names
   */
  getScorerNames(): string[] {
    return Array.from(this.byScorerName.keys())
  }

  /**
   * Get unique suite names
   */
  getSuiteNames(): string[] {
    return Array.from(this.bySuiteName.keys())
  }

  /**
   * Get unique run IDs
   */
  getRunIds(): number[] {
    return Array.from(this.byRunId.keys()).sort((a, b) => b - a)
  }

  /**
   * Clear all scores
   */
  clear(): void {
    this.scores = []
    this.byId.clear()
    this.byRunId.clear()
    this.bySuiteName.clear()
    this.byScorerName.clear()
    this.byEvalId.clear()
    this.statsCache = null
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Check if an event represents a score
   */
  private isScoreEvent(event: Event): boolean {
    if (event.after) {
      const after = event.after as Record<string, unknown>
      // Check for score-like fields
      if (typeof after.score === 'number') return true
      if (typeof after.value === 'number') return true
      if (after.scorerName && typeof after.score === 'number') return true
      if (after.$type === 'EvalScore') return true
      // Check for nested result.score
      if (after.result && typeof (after.result as Record<string, unknown>).score === 'number') {
        return true
      }
    }
    return false
  }

  /**
   * Extract an EvalScoreRecord from an event
   */
  private extractScore(event: Event): EvalScoreRecord | null {
    const after = event.after as Record<string, unknown> | undefined
    if (!after) return null

    // Extract score value
    const score = this.extractScoreValue(after)
    if (score === null || score === undefined) return null

    // Extract required fields
    const runId = this.extractNumber(after, ['runId', 'run_id'])
    const suiteName = this.extractString(after, ['suiteName', 'suite_name', 'name', 'suite'])
    const scorerName = this.extractString(after, ['scorerName', 'scorer_name', 'scorer'])

    if (!suiteName || !scorerName) return null

    return {
      id: event.id,
      ts: event.ts,
      runId: runId ?? 0,
      suiteName,
      scorerName,
      score,
      description: this.extractString(after, ['description', 'desc']),
      evalId: this.extractNumber(after, ['evalId', 'eval_id']) ?? 0,
      metadata: this.extractMetadata(after),
    }
  }

  /**
   * Extract score value from event data
   */
  private extractScoreValue(data: Record<string, unknown>): number | null {
    if (typeof data.score === 'number') return data.score
    if (typeof data.value === 'number') return data.value
    if (data.result && typeof (data.result as Record<string, unknown>).score === 'number') {
      return (data.result as Record<string, unknown>).score as number
    }
    return null
  }

  /**
   * Extract a string value from multiple possible field names
   */
  private extractString(data: Record<string, unknown>, fields: string[]): string | undefined {
    for (const field of fields) {
      if (typeof data[field] === 'string') return data[field]
    }
    return undefined
  }

  /**
   * Extract a number value from multiple possible field names
   */
  private extractNumber(data: Record<string, unknown>, fields: string[]): number | undefined {
    for (const field of fields) {
      if (typeof data[field] === 'number') return data[field]
    }
    return undefined
  }

  /**
   * Extract additional metadata from event data
   */
  private extractMetadata(data: Record<string, unknown>): Record<string, unknown> | undefined {
    const metadata: Record<string, unknown> = {}

    // Extract common metadata fields
    if (data.input !== undefined) metadata.input = data.input
    if (data.output !== undefined) metadata.output = data.output
    if (data.expected !== undefined) metadata.expected = data.expected
    if (typeof data.duration === 'number') metadata.duration = data.duration
    if (typeof data.tokens === 'number') metadata.tokens = data.tokens
    if (data.trace !== undefined) metadata.trace = data.trace

    return Object.keys(metadata).length > 0 ? metadata : undefined
  }

  /**
   * Add a score to storage and indexes
   */
  private addScore(score: EvalScoreRecord): void {
    // Check for duplicate
    if (this.byId.has(score.id)) return

    // Invalidate stats cache
    this.statsCache = null

    // Enforce max size
    while (this.scores.length >= this.config.maxScores) {
      const oldest = this.scores.shift()
      if (oldest) {
        this.removeFromIndexes(oldest)
      }
    }

    // Add to storage
    this.scores.push(score)
    this.byId.set(score.id, score)

    // Add to indexes
    this.addToIndex(this.byRunId, score.runId, score.id)
    this.addToIndex(this.bySuiteName, score.suiteName, score.id)
    this.addToIndex(this.byScorerName, score.scorerName, score.id)
    this.addToIndex(this.byEvalId, score.evalId, score.id)
  }

  /**
   * Add to an index map
   */
  private addToIndex<K>(index: Map<K, Set<string>>, key: K, id: string): void {
    let set = index.get(key)
    if (!set) {
      set = new Set()
      index.set(key, set)
    }
    set.add(id)
  }

  /**
   * Remove a score from indexes
   */
  private removeFromIndexes(score: EvalScoreRecord): void {
    this.byId.delete(score.id)
    this.byRunId.get(score.runId)?.delete(score.id)
    this.bySuiteName.get(score.suiteName)?.delete(score.id)
    this.byScorerName.get(score.scorerName)?.delete(score.id)
    this.byEvalId.get(score.evalId)?.delete(score.id)
  }

  /**
   * Get scores by a set of IDs
   */
  private getScoresById(ids: Set<string>, limit?: number): EvalScoreRecord[] {
    const result: EvalScoreRecord[] = []
    // Iterate in reverse for most recent first
    for (let i = this.scores.length - 1; i >= 0 && (!limit || result.length < limit); i--) {
      const score = this.scores[i]!
      if (ids.has(score.id)) {
        result.push(score)
      }
    }
    return result
  }

  /**
   * Compute statistics for a set of score IDs
   */
  private computeStatistics(ids: Set<string>): ScoreStatistics {
    const scores: number[] = []
    let lastUpdated = 0

    for (const id of ids) {
      const record = this.byId.get(id)
      if (record) {
        scores.push(record.score)
        if (record.ts > lastUpdated) lastUpdated = record.ts
      }
    }

    if (scores.length === 0) {
      return {
        count: 0,
        sum: 0,
        average: 0,
        min: 0,
        max: 0,
        stdDev: 0,
        distribution: new Array(this.config.distributionBuckets).fill(0),
        lastUpdated: 0,
      }
    }

    const sum = scores.reduce((a, b) => a + b, 0)
    const average = sum / scores.length
    const min = Math.min(...scores)
    const max = Math.max(...scores)

    // Calculate standard deviation
    const squaredDiffs = scores.map(s => Math.pow(s - average, 2))
    const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / scores.length
    const stdDev = Math.sqrt(avgSquaredDiff)

    // Calculate distribution
    const distribution = new Array(this.config.distributionBuckets).fill(0)
    const bucketSize = 1 / this.config.distributionBuckets
    for (const score of scores) {
      const bucketIndex = Math.min(
        Math.floor(score / bucketSize),
        this.config.distributionBuckets - 1
      )
      distribution[bucketIndex]++
    }

    return {
      count: scores.length,
      sum,
      average,
      min,
      max,
      stdDev,
      distribution,
      lastUpdated,
    }
  }

  /**
   * Compute aggregated statistics across all dimensions
   */
  private computeAggregatedStats(): EvalScoresStats {
    const now = Date.now()

    // By scorer
    const byScorer: Record<string, ScoreStatistics> = {}
    for (const [scorerName, ids] of this.byScorerName) {
      byScorer[scorerName] = this.computeStatistics(ids)
    }

    // By suite
    const bySuite: Record<string, ScoreStatistics> = {}
    for (const [suiteName, ids] of this.bySuiteName) {
      bySuite[suiteName] = this.computeStatistics(ids)
    }

    // By run
    const byRun: Record<number, ScoreStatistics> = {}
    for (const [runId, ids] of this.byRunId) {
      byRun[runId] = this.computeStatistics(ids)
    }

    // Global average
    const allScores = this.scores.map(s => s.score)
    const globalAverageScore = allScores.length > 0
      ? allScores.reduce((a, b) => a + b, 0) / allScores.length
      : 0

    // Time range
    const timeRange = {
      start: this.scores.length > 0 ? this.scores[0]!.ts : now,
      end: this.scores.length > 0 ? this.scores[this.scores.length - 1]!.ts : now,
    }

    return {
      totalScores: this.scores.length,
      uniqueRuns: this.byRunId.size,
      uniqueSuites: this.bySuiteName.size,
      uniqueScorers: this.byScorerName.size,
      globalAverageScore,
      byScorer,
      bySuite,
      byRun,
      timeRange,
    }
  }
}

/**
 * Create a new EvalScores MV instance
 */
export function createEvalScoresMV(config?: EvalScoresConfig): EvalScoresMV {
  return new EvalScoresMV(config)
}

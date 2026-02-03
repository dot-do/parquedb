/**
 * Evalite Adapter - Materialized Views Integration
 *
 * Integrates the Evalite adapter with ParqueDB's materialized views system
 * for AI evaluation observability. This enables real-time analytics on
 * evaluation runs, scores, and trends by streaming data to registered
 * materialized views.
 *
 * @example
 * ```typescript
 * import { createEvaliteAdapter, createEvaliteMVIntegration } from 'parquedb/integrations/evalite'
 * import { MemoryBackend } from 'parquedb/storage'
 *
 * // Create the MV integration
 * const mvIntegration = createEvaliteMVIntegration({
 *   enableBuiltinViews: true,
 * })
 *
 * // Create adapter with MV integration
 * const adapter = createEvaliteAdapter({
 *   storage: new MemoryBackend(),
 *   mvIntegration,
 * })
 *
 * // Query analytics
 * const scoresByModel = await mvIntegration.query('score_trends')
 * const runStats = await mvIntegration.query('run_statistics')
 * ```
 *
 * @packageDocumentation
 */

import type {
  StreamingRefreshEngine,
  MVHandler,
  StreamingStats,
} from '../../materialized-views/streaming'
import { createStreamingRefreshEngine } from '../../materialized-views/streaming'
import type { Event, EventOp } from '../../types/entity'
import type {
  EvalRun,
  EvalSuite,
  EvalResult,
  EvalScore,
  EvalTrace,
} from './types'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for Evalite MV integration
 */
export interface EvaliteMVConfig {
  /**
   * Batch size for processing entries
   * @default 50
   */
  batchSize?: number

  /**
   * Batch timeout in milliseconds
   * @default 1000
   */
  batchTimeoutMs?: number

  /**
   * Enable built-in analytics views
   * @default true
   */
  enableBuiltinViews?: boolean

  /**
   * Retention period for raw data in milliseconds
   * @default 30 * 24 * 60 * 60 * 1000 (30 days)
   */
  retentionMs?: number
}

/**
 * Entity types that can be processed by the MV integration
 */
export type EvaliteEntityType = 'run' | 'suite' | 'eval' | 'score' | 'trace'

/**
 * Base interface for MV events
 */
export interface EvaliteMVEvent<T = unknown> {
  entityType: EvaliteEntityType
  operation: 'create' | 'update' | 'delete'
  data: T
  timestamp: Date
}

/**
 * Custom analytics view definition for Evalite
 */
export interface EvaliteAnalyticsView<T = unknown> {
  /** Unique name for the view */
  name: string

  /** Description of what the view computes */
  description?: string

  /** Which entity types this view processes */
  entityTypes: EvaliteEntityType[]

  /**
   * Aggregate function to compute view data from events
   * Called incrementally with batches of new events
   */
  aggregate: (events: EvaliteMVEvent[], existingState?: T) => T | Promise<T>

  /**
   * Optional reduce function to merge partial aggregations
   */
  reduce?: (states: T[]) => T
}

/**
 * Built-in view names for Evalite analytics
 */
export type EvaliteBuiltinViewName =
  | 'score_trends'
  | 'run_statistics'
  | 'suite_performance'
  | 'scorer_analysis'
  | 'token_usage_by_suite'
  | 'failure_patterns'

/**
 * Score trend data over time
 */
export interface ScoreTrendData {
  suiteName: string
  scorerName: string
  dataPoints: Array<{
    timestamp: Date
    runId: number
    avgScore: number
    minScore: number
    maxScore: number
    evalCount: number
  }>
}

/**
 * Run statistics aggregated data
 */
export interface RunStatisticsData {
  totalRuns: number
  totalSuites: number
  totalEvals: number
  totalScores: number
  avgEvalsPerRun: number
  avgScoreOverall: number
  successRate: number
  runsByType: Record<string, number>
  lastRunAt?: Date
}

/**
 * Suite performance data
 */
export interface SuitePerformanceData {
  suiteName: string
  runCount: number
  avgDuration: number
  successRate: number
  avgScores: Record<string, number>
  lastRunAt?: Date
}

/**
 * Scorer analysis data
 */
export interface ScorerAnalysisData {
  scorerName: string
  totalScores: number
  avgScore: number
  minScore: number
  maxScore: number
  stdDev: number
  distribution: {
    bucket: string
    count: number
    percentage: number
  }[]
}

/**
 * Token usage data by suite
 */
export interface TokenUsageBySuiteData {
  suiteName: string
  totalTokens: number
  inputTokens: number
  outputTokens: number
  avgTokensPerEval: number
  traceCount: number
}

/**
 * Failure pattern data
 */
export interface FailurePatternData {
  suiteName: string
  totalEvals: number
  failedEvals: number
  failureRate: number
  commonFailures: Array<{
    scorerName: string
    count: number
    avgFailedScore: number
  }>
}

/**
 * State of the Evalite MV integration
 */
export interface EvaliteMVState {
  /** Whether the integration is running */
  isRunning: boolean

  /** Number of events processed */
  eventsProcessed: number

  /** Number of registered views */
  viewCount: number

  /** Names of registered views */
  viewNames: string[]

  /** Streaming engine statistics */
  engineStats?: StreamingStats
}

/**
 * Query options for analytics views
 */
export interface EvaliteQueryOptions {
  /**
   * Time range start (inclusive)
   */
  since?: Date

  /**
   * Time range end (exclusive)
   */
  until?: Date

  /**
   * Filter by suite name
   */
  suiteName?: string

  /**
   * Filter by scorer name
   */
  scorerName?: string

  /**
   * Filter by run ID
   */
  runId?: number

  /**
   * Limit results
   */
  limit?: number
}

// =============================================================================
// Raw Data Storage
// =============================================================================

interface RawEvaliteData {
  runs: EvalRun[]
  suites: EvalSuite[]
  evals: EvalResult[]
  scores: EvalScore[]
  traces: EvalTrace[]
}

// =============================================================================
// Evalite MV Integration
// =============================================================================

/**
 * Evalite MV Integration
 *
 * Manages materialized views for evaluation analytics and observability.
 */
export class EvaliteMVIntegration {
  private config: Required<EvaliteMVConfig>
  private engine: StreamingRefreshEngine
  private customViews = new Map<string, EvaliteAnalyticsView>()
  private viewData = new Map<string, unknown>()
  private rawData: RawEvaliteData = {
    runs: [],
    suites: [],
    evals: [],
    scores: [],
    traces: [],
  }
  private eventsProcessed = 0

  constructor(config: EvaliteMVConfig = {}) {
    this.config = {
      batchSize: config.batchSize ?? 50,
      batchTimeoutMs: config.batchTimeoutMs ?? 1000,
      enableBuiltinViews: config.enableBuiltinViews ?? true,
      retentionMs: config.retentionMs ?? 30 * 24 * 60 * 60 * 1000,
    }

    this.engine = createStreamingRefreshEngine({
      batchSize: this.config.batchSize,
      batchTimeoutMs: this.config.batchTimeoutMs,
    })

    // Register the main handlers
    this.engine.registerMV(this.createRunHandler())
    this.engine.registerMV(this.createSuiteHandler())
    this.engine.registerMV(this.createEvalHandler())
    this.engine.registerMV(this.createScoreHandler())
    this.engine.registerMV(this.createTraceHandler())

    // Initialize built-in views
    if (this.config.enableBuiltinViews) {
      this.initializeBuiltinViews()
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Start the MV integration
   */
  async start(): Promise<void> {
    await this.engine.start()
  }

  /**
   * Stop the MV integration
   */
  async stop(): Promise<void> {
    await this.engine.stop()
  }

  /**
   * Check if the integration is running
   */
  isRunning(): boolean {
    return this.engine.isRunning()
  }

  /**
   * Process a run creation/update event
   */
  processRun = async (run: EvalRun, operation: 'create' | 'update' = 'create'): Promise<void> => {
    const event = this.createEvent('run', run, operation)
    await this.engine.processEvent(event)
  }

  /**
   * Process a suite creation/update event
   */
  processSuite = async (suite: EvalSuite, operation: 'create' | 'update' = 'create'): Promise<void> => {
    const event = this.createEvent('suite', suite, operation)
    await this.engine.processEvent(event)
  }

  /**
   * Process an eval creation/update event
   */
  processEval = async (evalResult: EvalResult, operation: 'create' | 'update' = 'create'): Promise<void> => {
    const event = this.createEvent('eval', evalResult, operation)
    await this.engine.processEvent(event)
  }

  /**
   * Process a score creation event
   */
  processScore = async (score: EvalScore): Promise<void> => {
    const event = this.createEvent('score', score, 'create')
    await this.engine.processEvent(event)
  }

  /**
   * Process a trace creation event
   */
  processTrace = async (trace: EvalTrace): Promise<void> => {
    const event = this.createEvent('trace', trace, 'create')
    await this.engine.processEvent(event)
  }

  /**
   * Register a custom analytics view
   */
  registerView<T>(view: EvaliteAnalyticsView<T>): void {
    // Store with unknown type since we can't preserve the generic at runtime
    this.customViews.set(view.name, view as EvaliteAnalyticsView<unknown>)

    // Register corresponding MV handler
    const handler = this.createViewHandler(view as EvaliteAnalyticsView<unknown>)
    this.engine.registerMV(handler)
  }

  /**
   * Unregister a custom analytics view
   */
  unregisterView(name: string): void {
    this.customViews.delete(name)
    this.viewData.delete(name)
    this.engine.unregisterMV(`evalite_view_${name}`)
  }

  /**
   * Query a built-in or custom analytics view
   */
  async query<T = unknown>(
    viewName: string | EvaliteBuiltinViewName,
    options?: EvaliteQueryOptions
  ): Promise<T | undefined> {
    // Wait for any pending batches
    await this.engine.flush()

    // Get view data
    const data = this.viewData.get(viewName)
    if (!data) {
      return undefined
    }

    // Apply filters if this is raw data that supports filtering
    if (options && Array.isArray(data)) {
      return this.filterData(data, options) as T
    }

    return data as T
  }

  /**
   * Query raw runs
   */
  async queryRuns(options?: EvaliteQueryOptions): Promise<EvalRun[]> {
    await this.engine.flush()
    let runs = [...this.rawData.runs]

    if (options?.since) {
      runs = runs.filter(r => new Date(r.createdAt) >= options.since!)
    }
    if (options?.until) {
      runs = runs.filter(r => new Date(r.createdAt) < options.until!)
    }
    if (options?.runId !== undefined) {
      runs = runs.filter(r => r.id === options.runId)
    }
    if (options?.limit) {
      runs = runs.slice(0, options.limit)
    }

    return runs
  }

  /**
   * Query raw suites
   */
  async querySuites(options?: EvaliteQueryOptions): Promise<EvalSuite[]> {
    await this.engine.flush()
    let suites = [...this.rawData.suites]

    if (options?.since) {
      suites = suites.filter(s => new Date(s.createdAt) >= options.since!)
    }
    if (options?.until) {
      suites = suites.filter(s => new Date(s.createdAt) < options.until!)
    }
    if (options?.suiteName) {
      suites = suites.filter(s => s.name === options.suiteName)
    }
    if (options?.runId !== undefined) {
      suites = suites.filter(s => s.runId === options.runId)
    }
    if (options?.limit) {
      suites = suites.slice(0, options.limit)
    }

    return suites
  }

  /**
   * Query raw scores
   */
  async queryScores(options?: EvaliteQueryOptions): Promise<EvalScore[]> {
    await this.engine.flush()
    let scores = [...this.rawData.scores]

    if (options?.since) {
      scores = scores.filter(s => new Date(s.createdAt) >= options.since!)
    }
    if (options?.until) {
      scores = scores.filter(s => new Date(s.createdAt) < options.until!)
    }
    if (options?.scorerName) {
      scores = scores.filter(s => s.name === options.scorerName)
    }
    if (options?.limit) {
      scores = scores.slice(0, options.limit)
    }

    return scores
  }

  /**
   * Get the current state of the integration
   */
  getState(): EvaliteMVState {
    const registeredMVs = this.engine.getRegisteredMVs()
    const viewNames = registeredMVs
      .filter(n => n.startsWith('evalite_view_'))
      .map(n => n.replace('evalite_view_', ''))

    return {
      isRunning: this.engine.isRunning(),
      eventsProcessed: this.eventsProcessed,
      viewCount: this.customViews.size + (this.config.enableBuiltinViews ? BUILTIN_VIEW_NAMES.length : 0),
      viewNames: this.config.enableBuiltinViews
        ? [...BUILTIN_VIEW_NAMES, ...viewNames]
        : viewNames,
      engineStats: this.engine.getStats(),
    }
  }

  /**
   * Get the list of available view names
   */
  getViewNames(): string[] {
    const customNames = Array.from(this.customViews.keys())
    return this.config.enableBuiltinViews
      ? [...BUILTIN_VIEW_NAMES, ...customNames]
      : customNames
  }

  /**
   * Manually refresh all views
   */
  async refresh(): Promise<void> {
    await this.engine.flush()

    // Re-aggregate all built-in views from raw data
    if (this.config.enableBuiltinViews) {
      this.refreshBuiltinViews()
    }

    // Re-aggregate custom views
    for (const [name, view] of this.customViews) {
      const events = this.createEventsFromRawData(view.entityTypes)
      const data = await view.aggregate(events)
      this.viewData.set(name, data)
    }
  }

  /**
   * Clear all data and reset state
   */
  clear(): void {
    this.rawData = {
      runs: [],
      suites: [],
      evals: [],
      scores: [],
      traces: [],
    }
    this.viewData.clear()
    this.eventsProcessed = 0

    if (this.config.enableBuiltinViews) {
      this.initializeBuiltinViews()
    }
  }

  /**
   * Apply retention policy to remove old data
   */
  applyRetention(): number {
    const cutoff = new Date(Date.now() - this.config.retentionMs)
    let removed = 0

    const initialRunCount = this.rawData.runs.length
    this.rawData.runs = this.rawData.runs.filter(
      r => new Date(r.createdAt) > cutoff
    )
    removed += initialRunCount - this.rawData.runs.length

    const initialSuiteCount = this.rawData.suites.length
    this.rawData.suites = this.rawData.suites.filter(
      s => new Date(s.createdAt) > cutoff
    )
    removed += initialSuiteCount - this.rawData.suites.length

    const initialEvalCount = this.rawData.evals.length
    this.rawData.evals = this.rawData.evals.filter(
      e => new Date(e.createdAt) > cutoff
    )
    removed += initialEvalCount - this.rawData.evals.length

    const initialScoreCount = this.rawData.scores.length
    this.rawData.scores = this.rawData.scores.filter(
      s => new Date(s.createdAt) > cutoff
    )
    removed += initialScoreCount - this.rawData.scores.length

    const initialTraceCount = this.rawData.traces.length
    this.rawData.traces = this.rawData.traces.filter(
      t => new Date(t.createdAt) > cutoff
    )
    removed += initialTraceCount - this.rawData.traces.length

    // Refresh views after retention
    if (removed > 0) {
      this.refreshBuiltinViews()
    }

    return removed
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Create an Event for the streaming engine
   */
  private createEvent(
    entityType: EvaliteEntityType,
    data: unknown,
    operation: 'create' | 'update' | 'delete'
  ): Event {
    const opMap: Record<string, EventOp> = {
      create: 'CREATE',
      update: 'UPDATE',
      delete: 'DELETE',
    }

    return {
      id: `evalite_${entityType}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
      ts: Date.now(),
      op: opMap[operation] as EventOp,
      target: `evalite_${entityType}:${(data as { id?: number }).id || 'unknown'}`,
      after: {
        entityType,
        operation,
        data,
        timestamp: new Date(),
      } as unknown as Record<string, unknown>,
      actor: 'evalite-adapter',
    }
  }

  /**
   * Create handler for runs
   */
  private createRunHandler(): MVHandler {
    return {
      name: 'evalite_runs',
      sourceNamespaces: ['evalite_run'],
      process: async (events) => {
        for (const event of events) {
          if (event.after) {
            const mvEvent = event.after as unknown as EvaliteMVEvent<EvalRun>
            if (mvEvent.entityType === 'run') {
              const existingIndex = this.rawData.runs.findIndex(r => r.id === mvEvent.data.id)
              if (existingIndex >= 0) {
                this.rawData.runs[existingIndex] = mvEvent.data
              } else {
                this.rawData.runs.push(mvEvent.data)
              }
              this.eventsProcessed++
            }
          }
        }
        this.updateBuiltinViewsForEntityType('run')
      },
    }
  }

  /**
   * Create handler for suites
   */
  private createSuiteHandler(): MVHandler {
    return {
      name: 'evalite_suites',
      sourceNamespaces: ['evalite_suite'],
      process: async (events) => {
        for (const event of events) {
          if (event.after) {
            const mvEvent = event.after as unknown as EvaliteMVEvent<EvalSuite>
            if (mvEvent.entityType === 'suite') {
              const existingIndex = this.rawData.suites.findIndex(s => s.id === mvEvent.data.id)
              if (existingIndex >= 0) {
                this.rawData.suites[existingIndex] = mvEvent.data
              } else {
                this.rawData.suites.push(mvEvent.data)
              }
              this.eventsProcessed++
            }
          }
        }
        this.updateBuiltinViewsForEntityType('suite')
      },
    }
  }

  /**
   * Create handler for evals
   */
  private createEvalHandler(): MVHandler {
    return {
      name: 'evalite_evals',
      sourceNamespaces: ['evalite_eval'],
      process: async (events) => {
        for (const event of events) {
          if (event.after) {
            const mvEvent = event.after as unknown as EvaliteMVEvent<EvalResult>
            if (mvEvent.entityType === 'eval') {
              const existingIndex = this.rawData.evals.findIndex(e => e.id === mvEvent.data.id)
              if (existingIndex >= 0) {
                this.rawData.evals[existingIndex] = mvEvent.data
              } else {
                this.rawData.evals.push(mvEvent.data)
              }
              this.eventsProcessed++
            }
          }
        }
        this.updateBuiltinViewsForEntityType('eval')
      },
    }
  }

  /**
   * Create handler for scores
   */
  private createScoreHandler(): MVHandler {
    return {
      name: 'evalite_scores',
      sourceNamespaces: ['evalite_score'],
      process: async (events) => {
        for (const event of events) {
          if (event.after) {
            const mvEvent = event.after as unknown as EvaliteMVEvent<EvalScore>
            if (mvEvent.entityType === 'score') {
              this.rawData.scores.push(mvEvent.data)
              this.eventsProcessed++
            }
          }
        }
        this.updateBuiltinViewsForEntityType('score')
      },
    }
  }

  /**
   * Create handler for traces
   */
  private createTraceHandler(): MVHandler {
    return {
      name: 'evalite_traces',
      sourceNamespaces: ['evalite_trace'],
      process: async (events) => {
        for (const event of events) {
          if (event.after) {
            const mvEvent = event.after as unknown as EvaliteMVEvent<EvalTrace>
            if (mvEvent.entityType === 'trace') {
              this.rawData.traces.push(mvEvent.data)
              this.eventsProcessed++
            }
          }
        }
        this.updateBuiltinViewsForEntityType('trace')
      },
    }
  }

  /**
   * Create handler for a custom view
   */
  private createViewHandler(view: EvaliteAnalyticsView): MVHandler {
    const sourceNamespaces = view.entityTypes.map(t => `evalite_${t}`)

    return {
      name: `evalite_view_${view.name}`,
      sourceNamespaces,
      process: async (events) => {
        const mvEvents = events
          .filter(e => e.after)
          .map(e => e.after as unknown as EvaliteMVEvent)
          .filter(e => view.entityTypes.includes(e.entityType))

        if (mvEvents.length === 0) return

        const existingState = this.viewData.get(view.name)
        const newState = await view.aggregate(mvEvents, existingState)
        this.viewData.set(view.name, newState)
      },
    }
  }

  /**
   * Initialize built-in view data
   */
  private initializeBuiltinViews(): void {
    this.viewData.set('score_trends', new Map<string, ScoreTrendData>())
    this.viewData.set('run_statistics', {
      totalRuns: 0,
      totalSuites: 0,
      totalEvals: 0,
      totalScores: 0,
      avgEvalsPerRun: 0,
      avgScoreOverall: 0,
      successRate: 0,
      runsByType: {},
    } as RunStatisticsData)
    this.viewData.set('suite_performance', new Map<string, SuitePerformanceData>())
    this.viewData.set('scorer_analysis', new Map<string, ScorerAnalysisData>())
    this.viewData.set('token_usage_by_suite', new Map<string, TokenUsageBySuiteData>())
    this.viewData.set('failure_patterns', new Map<string, FailurePatternData>())
  }

  /**
   * Update built-in views for a specific entity type
   */
  private updateBuiltinViewsForEntityType(entityType: EvaliteEntityType): void {
    if (!this.config.enableBuiltinViews) return

    switch (entityType) {
      case 'run':
        this.updateRunStatistics()
        break
      case 'suite':
        this.updateRunStatistics()
        this.updateSuitePerformance()
        this.updateTokenUsageBySuite()
        this.updateFailurePatterns()
        break
      case 'eval':
        this.updateRunStatistics()
        this.updateFailurePatterns()
        break
      case 'score':
        this.updateScoreTrends()
        this.updateRunStatistics()
        this.updateSuitePerformance()
        this.updateScorerAnalysis()
        this.updateFailurePatterns()
        break
      case 'trace':
        this.updateTokenUsageBySuite()
        break
    }
  }

  /**
   * Refresh all built-in views from scratch
   */
  private refreshBuiltinViews(): void {
    this.initializeBuiltinViews()
    this.updateScoreTrends()
    this.updateRunStatistics()
    this.updateSuitePerformance()
    this.updateScorerAnalysis()
    this.updateTokenUsageBySuite()
    this.updateFailurePatterns()
  }

  /**
   * Update score_trends view
   */
  private updateScoreTrends(): void {
    const trends = new Map<string, ScoreTrendData>()

    // Group scores by suite and scorer
    for (const score of this.rawData.scores) {
      // Find the eval and suite for this score
      const evalResult = this.rawData.evals.find(e => e.id === score.evalId)
      if (!evalResult) continue

      const suite = this.rawData.suites.find(s => s.id === evalResult.suiteId)
      if (!suite) continue

      const key = `${suite.name}:${score.name}`
      let trend = trends.get(key)
      if (!trend) {
        trend = {
          suiteName: suite.name,
          scorerName: score.name,
          dataPoints: [],
        }
        trends.set(key, trend)
      }

      // Find or create data point for this run
      let dataPoint = trend.dataPoints.find(dp => dp.runId === suite.runId)
      if (!dataPoint) {
        dataPoint = {
          timestamp: new Date(suite.createdAt),
          runId: suite.runId,
          avgScore: 0,
          minScore: 1,
          maxScore: 0,
          evalCount: 0,
        }
        trend.dataPoints.push(dataPoint)
      }

      // Update data point
      const scores = this.rawData.scores.filter(s => {
        const e = this.rawData.evals.find(ev => ev.id === s.evalId)
        if (!e) return false
        const su = this.rawData.suites.find(su => su.id === e.suiteId)
        return su?.runId === suite.runId && su?.name === suite.name && s.name === score.name
      })

      if (scores.length > 0) {
        const scoreValues = scores.map(s => s.score)
        dataPoint.avgScore = scoreValues.reduce((a, b) => a + b, 0) / scoreValues.length
        dataPoint.minScore = Math.min(...scoreValues)
        dataPoint.maxScore = Math.max(...scoreValues)
        dataPoint.evalCount = scores.length
      }
    }

    // Sort data points by timestamp
    for (const trend of trends.values()) {
      trend.dataPoints.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())
    }

    this.viewData.set('score_trends', trends)
  }

  /**
   * Update run_statistics view
   */
  private updateRunStatistics(): void {
    const stats: RunStatisticsData = {
      totalRuns: this.rawData.runs.length,
      totalSuites: this.rawData.suites.length,
      totalEvals: this.rawData.evals.length,
      totalScores: this.rawData.scores.length,
      avgEvalsPerRun: 0,
      avgScoreOverall: 0,
      successRate: 0,
      runsByType: {},
      lastRunAt: undefined,
    }

    if (stats.totalRuns > 0) {
      stats.avgEvalsPerRun = stats.totalEvals / stats.totalRuns

      // Calculate success rate
      const successfulEvals = this.rawData.evals.filter(e => e.status === 'success').length
      stats.successRate = stats.totalEvals > 0 ? successfulEvals / stats.totalEvals : 0

      // Calculate average score
      if (stats.totalScores > 0) {
        const totalScore = this.rawData.scores.reduce((sum, s) => sum + s.score, 0)
        stats.avgScoreOverall = totalScore / stats.totalScores
      }

      // Group by run type
      for (const run of this.rawData.runs) {
        stats.runsByType[run.runType] = (stats.runsByType[run.runType] || 0) + 1
      }

      // Find last run
      const sortedRuns = [...this.rawData.runs].sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      )
      if (sortedRuns.length > 0) {
        stats.lastRunAt = new Date(sortedRuns[0]!.createdAt)
      }
    }

    this.viewData.set('run_statistics', stats)
  }

  /**
   * Update suite_performance view
   */
  private updateSuitePerformance(): void {
    const performance = new Map<string, SuitePerformanceData>()

    // Group suites by name
    const suitesByName = new Map<string, EvalSuite[]>()
    for (const suite of this.rawData.suites) {
      const existing = suitesByName.get(suite.name) || []
      existing.push(suite)
      suitesByName.set(suite.name, existing)
    }

    for (const [name, suites] of suitesByName) {
      const data: SuitePerformanceData = {
        suiteName: name,
        runCount: suites.length,
        avgDuration: 0,
        successRate: 0,
        avgScores: {},
        lastRunAt: undefined,
      }

      // Calculate averages
      const durations = suites.map(s => s.duration)
      data.avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length

      // Success rate
      const successfulSuites = suites.filter(s => s.status === 'success').length
      data.successRate = successfulSuites / suites.length

      // Average scores by scorer
      const suiteIds = new Set(suites.map(s => s.id))
      const relatedEvalIds = new Set(
        this.rawData.evals
          .filter(e => suiteIds.has(e.suiteId))
          .map(e => e.id)
      )
      const relatedScores = this.rawData.scores.filter(s => relatedEvalIds.has(s.evalId))

      const scoresByScorer = new Map<string, number[]>()
      for (const score of relatedScores) {
        const existing = scoresByScorer.get(score.name) || []
        existing.push(score.score)
        scoresByScorer.set(score.name, existing)
      }

      for (const [scorerName, scores] of scoresByScorer) {
        data.avgScores[scorerName] = scores.reduce((a, b) => a + b, 0) / scores.length
      }

      // Last run
      const sortedSuites = [...suites].sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      )
      if (sortedSuites.length > 0) {
        data.lastRunAt = new Date(sortedSuites[0]!.createdAt)
      }

      performance.set(name, data)
    }

    this.viewData.set('suite_performance', performance)
  }

  /**
   * Update scorer_analysis view
   */
  private updateScorerAnalysis(): void {
    const analysis = new Map<string, ScorerAnalysisData>()

    // Group scores by scorer name
    const scoresByScorer = new Map<string, number[]>()
    for (const score of this.rawData.scores) {
      const existing = scoresByScorer.get(score.name) || []
      existing.push(score.score)
      scoresByScorer.set(score.name, existing)
    }

    for (const [scorerName, scores] of scoresByScorer) {
      const data: ScorerAnalysisData = {
        scorerName,
        totalScores: scores.length,
        avgScore: 0,
        minScore: Math.min(...scores),
        maxScore: Math.max(...scores),
        stdDev: 0,
        distribution: [],
      }

      // Calculate average
      data.avgScore = scores.reduce((a, b) => a + b, 0) / scores.length

      // Calculate standard deviation
      const variance = scores.reduce((sum, s) => sum + Math.pow(s - data.avgScore, 2), 0) / scores.length
      data.stdDev = Math.sqrt(variance)

      // Calculate distribution (10 buckets from 0 to 1)
      const buckets = Array(10).fill(0) as number[]
      for (const score of scores) {
        const bucketIndex = Math.min(9, Math.floor(score * 10))
        buckets[bucketIndex]++
      }

      data.distribution = buckets.map((count, i) => ({
        bucket: `${(i * 0.1).toFixed(1)}-${((i + 1) * 0.1).toFixed(1)}`,
        count,
        percentage: (count / scores.length) * 100,
      }))

      analysis.set(scorerName, data)
    }

    this.viewData.set('scorer_analysis', analysis)
  }

  /**
   * Update token_usage_by_suite view
   */
  private updateTokenUsageBySuite(): void {
    const usage = new Map<string, TokenUsageBySuiteData>()

    // Get suite IDs for each suite name
    const suiteIdsByName = new Map<string, Set<number>>()
    for (const suite of this.rawData.suites) {
      const existing = suiteIdsByName.get(suite.name) || new Set()
      existing.add(suite.id)
      suiteIdsByName.set(suite.name, existing)
    }

    // Get eval IDs for each suite
    const evalIdsBySuiteName = new Map<string, Set<number>>()
    for (const [suiteName, suiteIds] of suiteIdsByName) {
      const evalIds = new Set(
        this.rawData.evals
          .filter(e => suiteIds.has(e.suiteId))
          .map(e => e.id)
      )
      evalIdsBySuiteName.set(suiteName, evalIds)
    }

    // Aggregate traces by suite
    for (const [suiteName, evalIds] of evalIdsBySuiteName) {
      const traces = this.rawData.traces.filter(t => evalIds.has(t.evalId))

      const data: TokenUsageBySuiteData = {
        suiteName,
        totalTokens: 0,
        inputTokens: 0,
        outputTokens: 0,
        avgTokensPerEval: 0,
        traceCount: traces.length,
      }

      for (const trace of traces) {
        data.totalTokens += trace.totalTokens ?? 0
        data.inputTokens += trace.inputTokens ?? 0
        data.outputTokens += trace.outputTokens ?? 0
      }

      if (evalIds.size > 0) {
        data.avgTokensPerEval = data.totalTokens / evalIds.size
      }

      usage.set(suiteName, data)
    }

    this.viewData.set('token_usage_by_suite', usage)
  }

  /**
   * Update failure_patterns view
   */
  private updateFailurePatterns(): void {
    const patterns = new Map<string, FailurePatternData>()

    // Group evals by suite name
    const evalsBySuite = new Map<string, EvalResult[]>()
    for (const evalResult of this.rawData.evals) {
      const suite = this.rawData.suites.find(s => s.id === evalResult.suiteId)
      if (!suite) continue

      const existing = evalsBySuite.get(suite.name) || []
      existing.push(evalResult)
      evalsBySuite.set(suite.name, existing)
    }

    for (const [suiteName, evals] of evalsBySuite) {
      const failedEvals = evals.filter(e => e.status === 'fail')

      const data: FailurePatternData = {
        suiteName,
        totalEvals: evals.length,
        failedEvals: failedEvals.length,
        failureRate: failedEvals.length / evals.length,
        commonFailures: [],
      }

      // Find common failure patterns by scorer
      if (failedEvals.length > 0) {
        const failedEvalIds = new Set(failedEvals.map(e => e.id))
        const failedScores = this.rawData.scores.filter(
          s => failedEvalIds.has(s.evalId) && s.score < 0.5
        )

        // Group by scorer
        const failuresByScorer = new Map<string, { count: number; totalScore: number }>()
        for (const score of failedScores) {
          const existing = failuresByScorer.get(score.name) || { count: 0, totalScore: 0 }
          existing.count++
          existing.totalScore += score.score
          failuresByScorer.set(score.name, existing)
        }

        data.commonFailures = Array.from(failuresByScorer.entries())
          .map(([scorerName, { count, totalScore }]) => ({
            scorerName,
            count,
            avgFailedScore: totalScore / count,
          }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 5)
      }

      patterns.set(suiteName, data)
    }

    this.viewData.set('failure_patterns', patterns)
  }

  /**
   * Create MV events from raw data for custom views
   */
  private createEventsFromRawData(entityTypes: EvaliteEntityType[]): EvaliteMVEvent[] {
    const events: EvaliteMVEvent[] = []

    if (entityTypes.includes('run')) {
      for (const run of this.rawData.runs) {
        events.push({
          entityType: 'run',
          operation: 'create',
          data: run,
          timestamp: new Date(run.createdAt),
        })
      }
    }

    if (entityTypes.includes('suite')) {
      for (const suite of this.rawData.suites) {
        events.push({
          entityType: 'suite',
          operation: 'create',
          data: suite,
          timestamp: new Date(suite.createdAt),
        })
      }
    }

    if (entityTypes.includes('eval')) {
      for (const evalResult of this.rawData.evals) {
        events.push({
          entityType: 'eval',
          operation: 'create',
          data: evalResult,
          timestamp: new Date(evalResult.createdAt),
        })
      }
    }

    if (entityTypes.includes('score')) {
      for (const score of this.rawData.scores) {
        events.push({
          entityType: 'score',
          operation: 'create',
          data: score,
          timestamp: new Date(score.createdAt),
        })
      }
    }

    if (entityTypes.includes('trace')) {
      for (const trace of this.rawData.traces) {
        events.push({
          entityType: 'trace',
          operation: 'create',
          data: trace,
          timestamp: new Date(trace.createdAt),
        })
      }
    }

    return events
  }

  /**
   * Filter array data by query options
   */
  private filterData<T extends { timestamp?: Date; suiteName?: string; scorerName?: string; runId?: number }>(
    data: T[],
    options: EvaliteQueryOptions
  ): T[] {
    let result = [...data]

    if (options.since) {
      result = result.filter(
        item => item.timestamp && new Date(item.timestamp) >= options.since!
      )
    }
    if (options.until) {
      result = result.filter(
        item => item.timestamp && new Date(item.timestamp) < options.until!
      )
    }
    if (options.suiteName) {
      result = result.filter(item => item.suiteName === options.suiteName)
    }
    if (options.scorerName) {
      result = result.filter(item => item.scorerName === options.scorerName)
    }
    if (options.runId !== undefined) {
      result = result.filter(item => item.runId === options.runId)
    }
    if (options.limit) {
      result = result.slice(0, options.limit)
    }

    return result
  }
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Built-in view names
 */
const BUILTIN_VIEW_NAMES: EvaliteBuiltinViewName[] = [
  'score_trends',
  'run_statistics',
  'suite_performance',
  'scorer_analysis',
  'token_usage_by_suite',
  'failure_patterns',
]

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create an Evalite MV integration instance
 *
 * @param config - Configuration options
 * @returns EvaliteMVIntegration instance
 *
 * @example
 * ```typescript
 * const mvIntegration = createEvaliteMVIntegration({
 *   batchSize: 100,
 *   enableBuiltinViews: true,
 * })
 *
 * await mvIntegration.start()
 *
 * // Process evaluation data
 * const run = await adapter.runs.create({})
 * await mvIntegration.processRun(run)
 *
 * // Query analytics
 * const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')
 * console.log(`Total runs: ${stats?.totalRuns}`)
 * ```
 */
export function createEvaliteMVIntegration(
  config?: EvaliteMVConfig
): EvaliteMVIntegration {
  return new EvaliteMVIntegration(config)
}

/**
 * Type-safe query helper for built-in views
 */
export async function queryEvaliteBuiltinView<T extends EvaliteBuiltinViewName>(
  integration: EvaliteMVIntegration,
  viewName: T,
  options?: EvaliteQueryOptions
): Promise<EvaliteBuiltinViewData[T] | undefined> {
  return integration.query<EvaliteBuiltinViewData[T]>(viewName, options)
}

/**
 * Type mapping for built-in view data
 */
type EvaliteBuiltinViewData = {
  score_trends: Map<string, ScoreTrendData>
  run_statistics: RunStatisticsData
  suite_performance: Map<string, SuitePerformanceData>
  scorer_analysis: Map<string, ScorerAnalysisData>
  token_usage_by_suite: Map<string, TokenUsageBySuiteData>
  failure_patterns: Map<string, FailurePatternData>
}

/**
 * ParqueDB Evalite Adapter
 *
 * Implements Evalite's Storage interface for storing evaluation runs,
 * results, scores, and traces in ParqueDB.
 *
 * @example
 * ```typescript
 * import { defineConfig } from 'evalite'
 * import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
 * import { R2Backend } from 'parquedb/storage'
 *
 * export default defineConfig({
 *   storage: () => createEvaliteAdapter({
 *     storage: new R2Backend(env.EVALITE_BUCKET),
 *   }),
 * })
 * ```
 */

import { ParqueDB } from '../../ParqueDB'
import type { EntityId, Entity, Filter } from '../../types'
import type {
  EvaliteAdapterConfig,
  ResolvedEvaliteConfig,
  EvalRun,
  EvalSuite,
  EvalResult,
  EvalScore,
  EvalTrace,
  CreateRunOptions,
  GetRunsOptions,
  CreateSuiteOptions,
  UpdateSuiteOptions,
  GetSuitesOptions,
  CreateEvalOptions,
  UpdateEvalOptions,
  GetEvalsOptions,
  CreateScoreOptions,
  GetScoresOptions,
  CreateTraceOptions,
  GetTracesOptions,
  ScoreHistoryOptions,
  ScorePoint,
  RunStats,
  RunWithResults,
  EvalWithDetails,
  RunType,
  SuiteStatus,
  EvalStatus,
} from './types'

// =============================================================================
// Collection Names
// =============================================================================

const COLLECTIONS = {
  runs: 'runs',
  suites: 'suites',
  evals: 'evals',
  scores: 'scores',
  traces: 'traces',
} as const

// =============================================================================
// ID Counter (in-memory for now, could be persisted)
// =============================================================================

class IdCounter {
  private counters: Map<string, number> = new Map()

  next(collection: string): number {
    const current = this.counters.get(collection) ?? 0
    const next = current + 1
    this.counters.set(collection, next)
    return next
  }

  async initialize(db: ParqueDB, prefix: string): Promise<void> {
    // Load max IDs from existing data
    for (const [, collection] of Object.entries(COLLECTIONS)) {
      const fullCollection = `${prefix}_${collection}`
      try {
        const result = await db.find(fullCollection, {}, { limit: 1, sort: { id: 'desc' } })
        if (result.items.length > 0) {
          const maxId = (result.items[0] as unknown as { id: number }).id
          this.counters.set(collection, maxId)
        }
      } catch {
        // Collection doesn't exist yet, start from 0
      }
    }
  }
}

// =============================================================================
// ParqueDB Evalite Adapter
// =============================================================================

/**
 * ParqueDB adapter implementing Evalite's Storage interface
 *
 * Collections created:
 * - {prefix}_runs: Evaluation runs
 * - {prefix}_suites: Test suites within runs
 * - {prefix}_evals: Individual evaluation results
 * - {prefix}_scores: Scorer outputs
 * - {prefix}_traces: LLM execution traces
 */
export class ParqueDBEvaliteAdapter {
  private config: ResolvedEvaliteConfig
  private db: ParqueDB
  private idCounter: IdCounter
  private initialized = false

  constructor(userConfig: EvaliteAdapterConfig) {
    this.config = {
      storage: userConfig.storage,
      collectionPrefix: userConfig.collectionPrefix ?? 'evalite',
      defaultActor: userConfig.defaultActor ?? ('system/evalite' as EntityId),
      debug: userConfig.debug ?? false,
    }

    this.db = new ParqueDB({ storage: this.config.storage })
    this.idCounter = new IdCounter()
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Initialize the adapter
   */
  async init(): Promise<void> {
    if (this.initialized) return

    await this.idCounter.initialize(this.db, this.config.collectionPrefix)
    this.initialized = true

    if (this.config.debug) {
      console.log('[EvaliteAdapter] Initialized with ParqueDB storage')
    }
  }

  /**
   * Close the adapter (cleanup)
   */
  async close(): Promise<void> {
    if (this.config.debug) {
      console.log('[EvaliteAdapter] Closed')
    }
  }

  /**
   * Async dispose (for using statement)
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  // ===========================================================================
  // Collection Name Helpers
  // ===========================================================================

  private collectionName(name: keyof typeof COLLECTIONS): string {
    return `${this.config.collectionPrefix}_${COLLECTIONS[name]}`
  }

  // ===========================================================================
  // Runs
  // ===========================================================================

  /**
   * Entity managers for Evalite compatibility
   */
  runs = {
    /**
     * Create a new evaluation run
     */
    create: async (opts: CreateRunOptions = {}): Promise<EvalRun> => {
      await this.init()

      const id = this.idCounter.next('runs')

      const entity = await this.db.create(this.collectionName('runs'), {
        $type: 'EvalRun',
        name: `Run ${id}`,
        id,
        runType: opts.runType ?? 'full',
      }, {
        actor: this.config.defaultActor,
      })

      return {
        id,
        runType: (opts.runType ?? 'full') as RunType,
        createdAt: entity.createdAt.toISOString(),
      }
    },

    /**
     * Get runs with optional filtering
     */
    getMany: async (opts: GetRunsOptions = {}): Promise<EvalRun[]> => {
      await this.init()

      const filter: Filter = {}
      if (opts.runType) {
        filter.runType = opts.runType
      }

      const sortDirection = opts.orderBy === 'createdAt' ? 'asc' : 'desc'
      const result = await this.db.find(this.collectionName('runs'), filter, {
        limit: opts.limit ?? 100,
        skip: opts.offset,
        sort: { createdAt: sortDirection },
      })

      return result.items.map(entity => this.entityToRun(entity))
    },
  }

  // ===========================================================================
  // Suites
  // ===========================================================================

  suites = {
    /**
     * Create a new evaluation suite
     */
    create: async (opts: CreateSuiteOptions): Promise<EvalSuite> => {
      await this.init()

      const id = this.idCounter.next('suites')

      const entity = await this.db.create(this.collectionName('suites'), {
        $type: 'EvalSuite',
        name: opts.name,
        id,
        runId: opts.runId,
        status: opts.status ?? 'running',
        duration: 0,
      }, {
        actor: this.config.defaultActor,
      })

      return this.entityToSuite(entity)
    },

    /**
     * Update a suite
     */
    update: async (opts: UpdateSuiteOptions): Promise<EvalSuite> => {
      await this.init()

      const update: Record<string, unknown> = {}
      if (opts.status !== undefined) update.status = opts.status
      if (opts.duration !== undefined) update.duration = opts.duration

      // Find by numeric id field
      const result = await this.db.find(this.collectionName('suites'), { id: opts.id }, { limit: 1 })
      if (result.items.length === 0) {
        throw new Error(`Suite not found: ${opts.id}`)
      }

      const entityId = result.items[0]!.$id.split('/')[1]!
      const entity = await this.db.update(this.collectionName('suites'), entityId, { $set: update }, {
        actor: this.config.defaultActor,
      })

      if (!entity) {
        throw new Error(`Suite not found: ${opts.id}`)
      }

      return this.entityToSuite(entity)
    },

    /**
     * Get suites with optional filtering
     */
    getMany: async (opts: GetSuitesOptions = {}): Promise<EvalSuite[]> => {
      await this.init()

      const filter: Filter = {}
      if (opts.runId !== undefined) filter.runId = opts.runId
      if (opts.status !== undefined) filter.status = opts.status

      const result = await this.db.find(this.collectionName('suites'), filter, {
        limit: opts.limit ?? 100,
        skip: opts.offset,
        sort: { createdAt: 'desc' },
      })

      return result.items.map(entity => this.entityToSuite(entity))
    },
  }

  // ===========================================================================
  // Evals
  // ===========================================================================

  evals = {
    /**
     * Create a new evaluation
     */
    create: async (opts: CreateEvalOptions): Promise<EvalResult> => {
      await this.init()

      const id = this.idCounter.next('evals')

      const entity = await this.db.create(this.collectionName('evals'), {
        $type: 'EvalResult',
        name: `Eval ${id}`,
        id,
        suiteId: opts.suiteId,
        input: opts.input,
        output: opts.output,
        expected: opts.expected,
        status: opts.status ?? 'running',
        colOrder: opts.colOrder ?? 0,
        duration: 0,
      }, {
        actor: this.config.defaultActor,
      })

      return this.entityToEval(entity)
    },

    /**
     * Update an evaluation
     */
    update: async (opts: UpdateEvalOptions): Promise<EvalResult> => {
      await this.init()

      const update: Record<string, unknown> = {}
      if (opts.output !== undefined) update.output = opts.output
      if (opts.status !== undefined) update.status = opts.status
      if (opts.duration !== undefined) update.duration = opts.duration
      if (opts.renderedColumns !== undefined) update.renderedColumns = opts.renderedColumns

      // Find by numeric id field
      const result = await this.db.find(this.collectionName('evals'), { id: opts.id }, { limit: 1 })
      if (result.items.length === 0) {
        throw new Error(`Eval not found: ${opts.id}`)
      }

      const entityId = result.items[0]!.$id.split('/')[1]!
      const entity = await this.db.update(this.collectionName('evals'), entityId, { $set: update }, {
        actor: this.config.defaultActor,
      })

      if (!entity) {
        throw new Error(`Eval not found: ${opts.id}`)
      }

      return this.entityToEval(entity)
    },

    /**
     * Get evals with optional filtering
     */
    getMany: async (opts: GetEvalsOptions = {}): Promise<EvalResult[]> => {
      await this.init()

      const filter: Filter = {}
      if (opts.suiteId !== undefined) filter.suiteId = opts.suiteId
      if (opts.status !== undefined) filter.status = opts.status

      const result = await this.db.find(this.collectionName('evals'), filter, {
        limit: opts.limit ?? 100,
        skip: opts.offset,
        sort: { colOrder: 'asc' },
      })

      const evals = result.items.map(entity => this.entityToEval(entity))

      // Optionally include scores and traces
      if (opts.includeScores || opts.includeTraces) {
        for (const evalResult of evals) {
          if (opts.includeScores) {
            evalResult.scores = await this.scores.getMany({ evalId: evalResult.id })
          }
          if (opts.includeTraces) {
            evalResult.traces = await this.traces.getMany({ evalId: evalResult.id })
          }
        }
      }

      return evals
    },
  }

  // ===========================================================================
  // Scores
  // ===========================================================================

  scores = {
    /**
     * Create a new score
     */
    create: async (opts: CreateScoreOptions): Promise<EvalScore> => {
      await this.init()

      const id = this.idCounter.next('scores')

      const entity = await this.db.create(this.collectionName('scores'), {
        $type: 'EvalScore',
        name: opts.name,
        id,
        evalId: opts.evalId,
        score: opts.score,
        description: opts.description,
        metadata: opts.metadata,
      }, {
        actor: this.config.defaultActor,
      })

      return this.entityToScore(entity)
    },

    /**
     * Get scores with optional filtering
     */
    getMany: async (opts: GetScoresOptions = {}): Promise<EvalScore[]> => {
      await this.init()

      const filter: Filter = {}
      if (opts.evalId !== undefined) filter.evalId = opts.evalId
      if (opts.name !== undefined) filter.name = opts.name

      const result = await this.db.find(this.collectionName('scores'), filter, {
        limit: opts.limit ?? 100,
        skip: opts.offset,
        sort: { createdAt: 'asc' },
      })

      return result.items.map(entity => this.entityToScore(entity))
    },
  }

  // ===========================================================================
  // Traces
  // ===========================================================================

  traces = {
    /**
     * Create a new trace
     */
    create: async (opts: CreateTraceOptions): Promise<EvalTrace> => {
      await this.init()

      const id = this.idCounter.next('traces')

      const entity = await this.db.create(this.collectionName('traces'), {
        $type: 'EvalTrace',
        name: `Trace ${id}`,
        id,
        evalId: opts.evalId,
        input: opts.input,
        output: opts.output,
        startTime: opts.startTime,
        endTime: opts.endTime,
        inputTokens: opts.inputTokens,
        outputTokens: opts.outputTokens,
        totalTokens: opts.totalTokens,
        colOrder: opts.colOrder ?? 0,
      }, {
        actor: this.config.defaultActor,
      })

      return this.entityToTrace(entity)
    },

    /**
     * Get traces with optional filtering
     */
    getMany: async (opts: GetTracesOptions = {}): Promise<EvalTrace[]> => {
      await this.init()

      const filter: Filter = {}
      if (opts.evalId !== undefined) filter.evalId = opts.evalId

      const result = await this.db.find(this.collectionName('traces'), filter, {
        limit: opts.limit ?? 100,
        skip: opts.offset,
        sort: { colOrder: 'asc' },
      })

      return result.items.map(entity => this.entityToTrace(entity))
    },
  }

  // ===========================================================================
  // Extended Query Methods (Dashboard/Analytics)
  // ===========================================================================

  /**
   * Get a complete run with all results populated
   */
  async getRunWithResults(runId: number): Promise<RunWithResults | null> {
    await this.init()

    // Get run
    const runs = await this.runs.getMany({ limit: 1 })
    const run = runs.find(r => r.id === runId)
    if (!run) return null

    // Get suites for this run
    const suites = await this.suites.getMany({ runId })

    // Build suite results with evals
    const suiteResults: RunWithResults['suites'] = []

    let totalEvals = 0
    let successCount = 0
    let failCount = 0
    let runningCount = 0
    let totalScore = 0
    let scoreCount = 0
    let totalDuration = 0
    let totalTokens = 0

    for (const suite of suites) {
      // Get evals for this suite
      const evals = await this.evals.getMany({
        suiteId: suite.id,
        includeScores: true,
        includeTraces: true,
      })

      const evalsWithDetails: EvalWithDetails[] = evals.map(e => ({
        ...e,
        scores: e.scores ?? [],
        traces: e.traces ?? [],
      }))

      // Calculate stats
      totalEvals += evals.length
      for (const evalResult of evals) {
        if (evalResult.status === 'success') successCount++
        else if (evalResult.status === 'fail') failCount++
        else runningCount++

        totalDuration += evalResult.duration

        // Calculate average score from scores
        if (evalResult.scores && evalResult.scores.length > 0) {
          const avgScore = evalResult.scores.reduce((sum, s) => sum + s.score, 0) / evalResult.scores.length
          totalScore += avgScore
          scoreCount++
        }

        // Sum tokens from traces
        if (evalResult.traces) {
          for (const trace of evalResult.traces) {
            totalTokens += trace.totalTokens ?? 0
          }
        }
      }

      suiteResults.push({
        ...suite,
        evals: evalsWithDetails,
      })
    }

    const stats: RunStats = {
      totalSuites: suites.length,
      totalEvals,
      successCount,
      failCount,
      runningCount,
      averageScore: scoreCount > 0 ? totalScore / scoreCount : 0,
      totalDuration,
      totalTokens,
    }

    return {
      ...run,
      suites: suiteResults,
      stats,
    }
  }

  /**
   * Get score history for a specific evaluation name
   * Useful for dashboards showing score trends over time
   */
  async getScoreHistory(evalName: string, options: ScoreHistoryOptions = {}): Promise<ScorePoint[]> {
    await this.init()

    const limit = options.limit ?? 100

    // Get suites with matching name
    const suitesResult = await this.db.find(this.collectionName('suites'), { name: evalName }, {
      limit,
      sort: { createdAt: 'desc' },
    })

    const scorePoints: ScorePoint[] = []

    for (const suiteEntity of suitesResult.items) {
      const suite = this.entityToSuite(suiteEntity)

      // Apply date filters
      const suiteDate = new Date(suite.createdAt)
      if (options.from && suiteDate < options.from) continue
      if (options.to && suiteDate > options.to) continue

      // Get evals for this suite
      const evals = await this.evals.getMany({
        suiteId: suite.id,
        includeScores: true,
      })

      if (evals.length === 0) continue

      // Calculate score statistics
      let minScore = 1
      let maxScore = 0
      let totalScore = 0
      let scoreCount = 0

      for (const evalResult of evals) {
        if (evalResult.scores && evalResult.scores.length > 0) {
          const evalScores = options.scorerName
            ? evalResult.scores.filter(s => s.name === options.scorerName)
            : evalResult.scores

          for (const score of evalScores) {
            minScore = Math.min(minScore, score.score)
            maxScore = Math.max(maxScore, score.score)
            totalScore += score.score
            scoreCount++
          }
        }
      }

      if (scoreCount > 0) {
        scorePoints.push({
          timestamp: suite.createdAt,
          runId: suite.runId,
          averageScore: totalScore / scoreCount,
          minScore,
          maxScore,
          evalCount: evals.length,
        })
      }
    }

    // Sort by timestamp ascending for charting
    scorePoints.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())

    return scorePoints
  }

  /**
   * Get runs with optional filtering
   */
  async getRuns(filter?: GetRunsOptions): Promise<EvalRun[]> {
    return this.runs.getMany(filter)
  }

  /**
   * Store evaluation run (convenience method)
   */
  async saveRun(run: Omit<EvalRun, 'createdAt'>): Promise<EvalRun> {
    return this.runs.create({ runType: run.runType })
  }

  /**
   * Store evaluation results for a run (convenience method)
   */
  async saveResults(
    runId: number,
    suiteName: string,
    results: Array<{
      input: unknown
      output: unknown
      expected?: unknown
      scores: Array<{ name: string; score: number; description?: string }>
      traces?: Array<{
        input: unknown
        output: unknown
        startTime: number
        endTime: number
        inputTokens?: number
        outputTokens?: number
        totalTokens?: number
      }>
    }>
  ): Promise<void> {
    await this.init()

    // Create suite
    const suite = await this.suites.create({
      runId,
      name: suiteName,
      status: 'running',
    })

    let allSuccess = true

    // Create evals with scores and traces
    for (let i = 0; i < results.length; i++) {
      const result = results[i]!

      const evalResult = await this.evals.create({
        suiteId: suite.id,
        input: result.input,
        output: result.output,
        expected: result.expected,
        colOrder: i,
        status: 'success',
      })

      // Create scores
      for (const score of result.scores) {
        await this.scores.create({
          evalId: evalResult.id,
          name: score.name,
          score: score.score,
          description: score.description,
        })

        // If any score is 0, mark as failed
        if (score.score === 0) allSuccess = false
      }

      // Create traces
      if (result.traces) {
        for (let j = 0; j < result.traces.length; j++) {
          const trace = result.traces[j]!
          await this.traces.create({
            evalId: evalResult.id,
            input: trace.input,
            output: trace.output,
            startTime: trace.startTime,
            endTime: trace.endTime,
            inputTokens: trace.inputTokens,
            outputTokens: trace.outputTokens,
            totalTokens: trace.totalTokens,
            colOrder: j,
          })
        }
      }
    }

    // Update suite status
    await this.suites.update({
      id: suite.id,
      status: allSuccess ? 'success' : 'fail',
    })
  }

  // ===========================================================================
  // Entity Conversion Helpers
  // ===========================================================================

  private entityToRun(entity: Entity): EvalRun {
    return {
      id: (entity as unknown as { id: number }).id,
      runType: (entity as unknown as { runType: RunType }).runType,
      createdAt: entity.createdAt.toISOString(),
    }
  }

  private entityToSuite(entity: Entity): EvalSuite {
    const data = entity as unknown as {
      id: number
      runId: number
      status: SuiteStatus
      duration: number
    }
    return {
      id: data.id,
      runId: data.runId,
      name: entity.name,
      status: data.status,
      duration: data.duration,
      createdAt: entity.createdAt.toISOString(),
    }
  }

  private entityToEval(entity: Entity): EvalResult {
    const data = entity as unknown as {
      id: number
      suiteId: number
      duration: number
      input: unknown
      output: unknown
      expected?: unknown
      status: EvalStatus
      colOrder: number
      renderedColumns?: unknown
    }
    return {
      id: data.id,
      suiteId: data.suiteId,
      duration: data.duration,
      input: data.input,
      output: data.output,
      expected: data.expected,
      status: data.status,
      colOrder: data.colOrder,
      renderedColumns: data.renderedColumns,
      createdAt: entity.createdAt.toISOString(),
    }
  }

  private entityToScore(entity: Entity): EvalScore {
    const data = entity as unknown as {
      id: number
      evalId: number
      score: number
      description?: string
      metadata?: unknown
    }
    return {
      id: data.id,
      evalId: data.evalId,
      name: entity.name,
      score: data.score,
      description: data.description,
      metadata: data.metadata,
      createdAt: entity.createdAt.toISOString(),
    }
  }

  private entityToTrace(entity: Entity): EvalTrace {
    const data = entity as unknown as {
      id: number
      evalId: number
      input: unknown
      output: unknown
      startTime: number
      endTime: number
      inputTokens?: number
      outputTokens?: number
      totalTokens?: number
      colOrder: number
    }
    return {
      id: data.id,
      evalId: data.evalId,
      input: data.input,
      output: data.output,
      startTime: data.startTime,
      endTime: data.endTime,
      inputTokens: data.inputTokens,
      outputTokens: data.outputTokens,
      totalTokens: data.totalTokens,
      colOrder: data.colOrder,
      createdAt: entity.createdAt.toISOString(),
    }
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Get the underlying ParqueDB instance
   */
  getDB(): ParqueDB {
    return this.db
  }

  /**
   * Get the resolved configuration
   */
  getConfig(): ResolvedEvaliteConfig {
    return this.config
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a ParqueDB adapter for Evalite
 *
 * @example
 * ```typescript
 * import { defineConfig } from 'evalite'
 * import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
 * import { MemoryBackend } from 'parquedb/storage'
 *
 * export default defineConfig({
 *   storage: () => createEvaliteAdapter({
 *     storage: new MemoryBackend(),
 *   }),
 * })
 * ```
 */
export function createEvaliteAdapter(config: EvaliteAdapterConfig): ParqueDBEvaliteAdapter {
  return new ParqueDBEvaliteAdapter(config)
}

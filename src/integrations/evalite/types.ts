/**
 * Type definitions for Evalite Storage Adapter
 *
 * These types align with Evalite's storage interface and data model.
 * Evalite organizes evaluations into:
 * - Runs: A collection of evaluation suites
 * - Suites: A group of related evaluations (e.g., a test file)
 * - Evals: Individual evaluation results with input/output/expected
 * - Scores: Scorer outputs for each evaluation
 * - Traces: LLM execution traces with token usage
 */

import type { StorageBackend, EntityId } from '../../types'

// Forward declare types used in interface to avoid circular import
// These are defined later in this file
type EvalRunForInterface = { id: number; runType: 'full' | 'partial'; createdAt: string }
type EvalSuiteForInterface = { id: number; runId: number; name: string; status: 'running' | 'success' | 'fail'; duration: number; createdAt: string }
type EvalResultForInterface = { id: number; suiteId: number; duration: number; input: unknown; output: unknown; expected?: unknown | undefined; status: 'running' | 'success' | 'fail'; colOrder: number; renderedColumns?: unknown | undefined; createdAt: string; averageScore?: number | undefined }
type EvalScoreForInterface = { id: number; evalId: number; name: string; score: number; description?: string | undefined; metadata?: unknown | undefined; createdAt: string }
type EvalTraceForInterface = { id: number; evalId: number; input: unknown; output: unknown; startTime: number; endTime: number; inputTokens?: number | undefined; outputTokens?: number | undefined; totalTokens?: number | undefined; colOrder: number; createdAt: string }

/**
 * Materialized views integration interface for Evalite adapter.
 * Defines the minimal interface needed to avoid circular dependencies
 * between types.ts and mv-integration.ts.
 */
export interface IEvaliteMVIntegration {
  /** Process a run event */
  processRun(run: EvalRunForInterface, op?: 'create' | 'update'): Promise<void>
  /** Process a suite event */
  processSuite(suite: EvalSuiteForInterface, op?: 'create' | 'update'): Promise<void>
  /** Process an eval event */
  processEval(evalResult: EvalResultForInterface, op?: 'create' | 'update'): Promise<void>
  /** Process a score event */
  processScore(score: EvalScoreForInterface, op?: 'create' | 'update'): Promise<void>
  /** Process a trace event */
  processTrace(trace: EvalTraceForInterface, op?: 'create' | 'update'): Promise<void>
}

// =============================================================================
// Run Types
// =============================================================================

/**
 * Run type - full or partial evaluation run
 */
export type RunType = 'full' | 'partial'

/**
 * Evaluation run - a collection of suites executed together
 */
export interface EvalRun {
  /** Unique run ID */
  id: number
  /** Run type */
  runType: RunType
  /** When the run was created */
  createdAt: string
}

/**
 * Options for creating a run
 */
export interface CreateRunOptions {
  /** Run type (default: 'full') */
  runType?: RunType | undefined
}

/**
 * Options for querying runs
 */
export interface GetRunsOptions {
  /** Maximum number of runs to return */
  limit?: number | undefined
  /** Offset for pagination */
  offset?: number | undefined
  /** Filter by run type */
  runType?: RunType | undefined
  /** Order by (default: createdAt descending) */
  orderBy?: 'createdAt' | '-createdAt' | undefined
}

// =============================================================================
// Suite Types
// =============================================================================

/**
 * Suite status
 */
export type SuiteStatus = 'running' | 'success' | 'fail'

/**
 * Evaluation suite - a group of related evaluations
 */
export interface EvalSuite {
  /** Unique suite ID */
  id: number
  /** Parent run ID */
  runId: number
  /** Suite name (typically file path) */
  name: string
  /** Suite status */
  status: SuiteStatus
  /** Total duration in milliseconds */
  duration: number
  /** When the suite was created */
  createdAt: string
}

/**
 * Options for creating a suite
 */
export interface CreateSuiteOptions {
  /** Parent run ID */
  runId: number
  /** Suite name */
  name: string
  /** Initial status (default: 'running') */
  status?: SuiteStatus | undefined
}

/**
 * Options for updating a suite
 */
export interface UpdateSuiteOptions {
  /** Suite ID to update */
  id: number
  /** New status */
  status?: SuiteStatus | undefined
  /** Duration in milliseconds */
  duration?: number | undefined
}

/**
 * Options for querying suites
 */
export interface GetSuitesOptions {
  /** Filter by run ID */
  runId?: number | undefined
  /** Filter by status */
  status?: SuiteStatus | undefined
  /** Maximum number of suites to return */
  limit?: number | undefined
  /** Offset for pagination */
  offset?: number | undefined
}

// =============================================================================
// Eval Types
// =============================================================================

/**
 * Evaluation status
 */
export type EvalStatus = 'running' | 'success' | 'fail'

/**
 * Individual evaluation result
 */
export interface EvalResult {
  /** Unique eval ID */
  id: number
  /** Parent suite ID */
  suiteId: number
  /** Execution duration in milliseconds */
  duration: number
  /** Input data passed to the task */
  input: unknown
  /** Output produced by the task */
  output: unknown
  /** Expected output (if provided) */
  expected?: unknown | undefined
  /** Evaluation status */
  status: EvalStatus
  /** Column ordering for display */
  colOrder: number
  /** Custom rendered columns */
  renderedColumns?: unknown | undefined
  /** When the eval was created */
  createdAt: string
  /** Average score across all scorers */
  averageScore?: number | undefined
  /** All scores for this eval (populated on read) */
  scores?: EvalScore[] | undefined
  /** All traces for this eval (populated on read) */
  traces?: EvalTrace[] | undefined
}

/**
 * Options for creating an eval
 */
export interface CreateEvalOptions {
  /** Parent suite ID */
  suiteId: number
  /** Input data */
  input: unknown
  /** Output data */
  output?: unknown | undefined
  /** Expected output */
  expected?: unknown | undefined
  /** Initial status (default: 'running') */
  status?: EvalStatus | undefined
  /** Column order */
  colOrder?: number | undefined
}

/**
 * Options for updating an eval
 */
export interface UpdateEvalOptions {
  /** Eval ID to update */
  id: number
  /** Output data */
  output?: unknown | undefined
  /** Status */
  status?: EvalStatus | undefined
  /** Duration in milliseconds */
  duration?: number | undefined
  /** Rendered columns */
  renderedColumns?: unknown | undefined
}

/**
 * Options for querying evals
 */
export interface GetEvalsOptions {
  /** Filter by suite ID */
  suiteId?: number | undefined
  /** Filter by status */
  status?: EvalStatus | undefined
  /** Include scores in results */
  includeScores?: boolean | undefined
  /** Include traces in results */
  includeTraces?: boolean | undefined
  /** Maximum number of evals to return */
  limit?: number | undefined
  /** Offset for pagination */
  offset?: number | undefined
}

// =============================================================================
// Score Types
// =============================================================================

/**
 * Scorer result for an evaluation
 */
export interface EvalScore {
  /** Unique score ID */
  id: number
  /** Parent eval ID */
  evalId: number
  /** Scorer name */
  name: string
  /** Score value (0-1 range) */
  score: number
  /** Optional description */
  description?: string | undefined
  /** Additional metadata */
  metadata?: unknown | undefined
  /** When the score was created */
  createdAt: string
}

/**
 * Options for creating a score
 */
export interface CreateScoreOptions {
  /** Parent eval ID */
  evalId: number
  /** Scorer name */
  name: string
  /** Score value (0-1 range) */
  score: number
  /** Description */
  description?: string | undefined
  /** Metadata */
  metadata?: unknown | undefined
}

/**
 * Options for querying scores
 */
export interface GetScoresOptions {
  /** Filter by eval ID */
  evalId?: number | undefined
  /** Filter by scorer name */
  name?: string | undefined
  /** Maximum number of scores to return */
  limit?: number | undefined
  /** Offset for pagination */
  offset?: number | undefined
}

// =============================================================================
// Trace Types
// =============================================================================

/**
 * LLM execution trace
 */
export interface EvalTrace {
  /** Unique trace ID */
  id: number
  /** Parent eval ID */
  evalId: number
  /** Input to the LLM call */
  input: unknown
  /** Output from the LLM call */
  output: unknown
  /** Start time (ms since epoch) */
  startTime: number
  /** End time (ms since epoch) */
  endTime: number
  /** Input tokens used */
  inputTokens?: number | undefined
  /** Output tokens generated */
  outputTokens?: number | undefined
  /** Total tokens (input + output) */
  totalTokens?: number | undefined
  /** Column ordering for display */
  colOrder: number
  /** When the trace was created */
  createdAt: string
}

/**
 * Options for creating a trace
 */
export interface CreateTraceOptions {
  /** Parent eval ID */
  evalId: number
  /** Input to the LLM */
  input: unknown
  /** Output from the LLM */
  output: unknown
  /** Start time */
  startTime: number
  /** End time */
  endTime: number
  /** Input tokens */
  inputTokens?: number | undefined
  /** Output tokens */
  outputTokens?: number | undefined
  /** Total tokens */
  totalTokens?: number | undefined
  /** Column order */
  colOrder?: number | undefined
}

/**
 * Options for querying traces
 */
export interface GetTracesOptions {
  /** Filter by eval ID */
  evalId?: number | undefined
  /** Maximum number of traces to return */
  limit?: number | undefined
  /** Offset for pagination */
  offset?: number | undefined
}

// =============================================================================
// Adapter Configuration
// =============================================================================

/**
 * Configuration for the ParqueDB Evalite adapter
 */
export interface EvaliteAdapterConfig {
  /**
   * Storage backend for data persistence
   */
  storage: StorageBackend

  /**
   * Collection prefix for Evalite data
   * @default 'evalite'
   */
  collectionPrefix?: string | undefined

  /**
   * Default actor for audit fields
   * @default 'system/evalite'
   */
  defaultActor?: EntityId | undefined

  /**
   * Enable debug logging
   * @default false
   */
  debug?: boolean | undefined

  /**
   * Materialized views integration for analytics
   */
  mvIntegration?: IEvaliteMVIntegration | undefined
}

/**
 * Resolved adapter configuration with defaults applied
 */
export interface ResolvedEvaliteConfig {
  storage: StorageBackend
  collectionPrefix: string
  defaultActor: EntityId
  debug: boolean
  mvIntegration?: IEvaliteMVIntegration | undefined
}

// =============================================================================
// Dashboard/Analytics Types
// =============================================================================

/**
 * Options for score history queries
 */
export interface ScoreHistoryOptions {
  /** Maximum number of data points */
  limit?: number | undefined
  /** Start date */
  from?: Date | undefined
  /** End date */
  to?: Date | undefined
  /** Filter by scorer name */
  scorerName?: string | undefined
}

/**
 * Score data point for time-series visualization
 */
export interface ScorePoint {
  /** Timestamp */
  timestamp: string
  /** Run ID */
  runId: number
  /** Average score */
  averageScore: number
  /** Minimum score */
  minScore: number
  /** Maximum score */
  maxScore: number
  /** Number of evaluations */
  evalCount: number
}

/**
 * Run statistics summary
 */
export interface RunStats {
  /** Total number of suites */
  totalSuites: number
  /** Total number of evals */
  totalEvals: number
  /** Successful evals */
  successCount: number
  /** Failed evals */
  failCount: number
  /** Running evals */
  runningCount: number
  /** Average score across all evals */
  averageScore: number
  /** Total duration in milliseconds */
  totalDuration: number
  /** Total tokens used */
  totalTokens: number
}

/**
 * Eval with full details (scores and traces populated)
 */
export interface EvalWithDetails extends EvalResult {
  scores: EvalScore[]
  traces: EvalTrace[]
}

/**
 * Run with all results populated
 */
export interface RunWithResults extends EvalRun {
  suites: Array<EvalSuite & {
    evals: EvalWithDetails[]
  }>
  stats: RunStats
}

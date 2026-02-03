/**
 * Evalite Integration for ParqueDB
 *
 * Provides a ParqueDB storage adapter for Evalite, the TypeScript
 * AI evaluation framework.
 *
 * @example
 * ```typescript
 * import { defineConfig } from 'evalite'
 * import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
 * import { MemoryBackend, R2Backend } from 'parquedb/storage'
 *
 * // For local development with in-memory storage
 * export default defineConfig({
 *   storage: () => createEvaliteAdapter({
 *     storage: new MemoryBackend(),
 *   }),
 * })
 *
 * // For production with R2 storage
 * export default defineConfig({
 *   storage: () => createEvaliteAdapter({
 *     storage: new R2Backend(env.EVALITE_BUCKET),
 *     collectionPrefix: 'evalite',
 *   }),
 * })
 * ```
 *
 * @packageDocumentation
 */

// Main adapter
export { ParqueDBEvaliteAdapter, createEvaliteAdapter } from './adapter'

// Types
export type {
  // Configuration
  EvaliteAdapterConfig,
  ResolvedEvaliteConfig,

  // Run types
  RunType,
  EvalRun,
  CreateRunOptions,
  GetRunsOptions,

  // Suite types
  SuiteStatus,
  EvalSuite,
  CreateSuiteOptions,
  UpdateSuiteOptions,
  GetSuitesOptions,

  // Eval types
  EvalStatus,
  EvalResult,
  CreateEvalOptions,
  UpdateEvalOptions,
  GetEvalsOptions,

  // Score types
  EvalScore,
  CreateScoreOptions,
  GetScoresOptions,

  // Trace types
  EvalTrace,
  CreateTraceOptions,
  GetTracesOptions,

  // Analytics types
  ScoreHistoryOptions,
  ScorePoint,
  RunStats,
  RunWithResults,
  EvalWithDetails,
} from './types'

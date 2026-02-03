/**
 * ParqueDB Streaming Module
 *
 * This module provides:
 * 1. **Stream Collections** - Pre-built collection schemas with $ingest directive
 *    for automatic data ingestion (AI SDK, tail events, evalite)
 *
 * 2. **Stream Views** - CDC-based windowed views using defineStreamView() API
 *
 * NEW PATTERN (per materialized-views.md):
 * - Stream collections use `$ingest` to wire up automatic ingestion
 * - MVs use `$from` to reference source collections (no `$stream` directive)
 * - Import stream collections directly into your DB schema
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { AIRequests, TailEvents, EvalScoresCollection } from 'parquedb/streaming'
 *
 * const db = DB({
 *   // Stream collections (auto-ingest via $ingest directive)
 *   AIRequests,
 *   TailEvents,
 *   EvalScores: EvalScoresCollection,  // Renamed import
 *
 *   // MVs reference stream collections via $from
 *   DailyAIUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }],
 *     $compute: { count: { $count: '*' } },
 *   },
 * }, { storage })
 * ```
 */

export {
  // =============================================================================
  // Stream Collections ($ingest directive)
  // =============================================================================
  // Pre-built collection schemas that wire up automatic data ingestion.
  // Import these directly into your DB schema.

  // AI SDK stream collections
  AIRequests,
  Generations,

  // Cloudflare tail stream collections
  TailEvents,

  // Evalite stream collections
  EvalRuns,
  EvalScores as EvalScoresCollection,  // Renamed to avoid conflict with EvalScoresMV

  // Stream collection utilities
  isStreamCollection,

  // Stream collection types
  type IngestSource,
  type StreamCollectionSchema,

  // =============================================================================
  // Stream View API (CDC-based windowed views)
  // =============================================================================
  // For advanced use cases requiring windowing, watermarks, and custom transforms.

  // Main API
  defineStreamView,
  defineStreamViews,
  // Validation
  validateStreamViewDefinition,
  // Utilities
  durationToMs,
  streamViewName,
  // Type Guards - Windows
  isTumblingWindow,
  isSlidingWindow,
  isSessionWindow,
  isGlobalWindow,
  // Type Guards - Sinks
  isCollectionSink,
  isWebhookSink,
  isQueueSink,
  isConsoleSink,
  // Types
  type StreamViewName,
  type WindowDuration,
  type TumblingWindow,
  type SlidingWindow,
  type SessionWindow,
  type GlobalWindow,
  type WindowConfig,
  type StreamSourceConfig,
  type BuiltInTransform,
  type TransformFunction,
  type TransformContext,
  type TransformConfig,
  type OutputSinkType,
  type CollectionSink,
  type WebhookSink,
  type QueueSink,
  type ConsoleSink,
  type OutputSink,
  type OutputConfig,
  type WatermarkConfig,
  type StreamViewDefinition,
  type StreamViewState,
  type StreamView,
  type StreamPosition,
  type StreamViewStats,
} from './views'

// Streaming Engine
export {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
} from './engine'

// Worker Errors MV
export type { WorkerErrorsConfig } from './worker-errors'
export {
  WorkerErrorsMV,
  createWorkerErrorsMV,
} from './worker-errors'

// Worker Logs MV
export type {
  TailEvent,
  TailItem,
  TailLog,
  TailException,
  TailRequest,
  TailResponse,
  FetchEventInfo,
  WorkerOutcome,
  LogLevel,
  WorkerLogRecord,
  WorkerLogsStats,
  WorkerLogsMVConfig,
} from './worker-logs'
export {
  WorkerLogsMV,
  createWorkerLogsMV,
  createWorkerLogsMVHandler,
  WORKER_LOGS_SCHEMA,
} from './worker-logs'

// Worker Requests MV
export type {
  WorkerRequestsMVOptions,
  HttpMethod,
  StatusCategory,
  WorkerRequest,
  RecordRequestInput,
  TimeBucket,
  RequestMetrics,
  GetMetricsOptions,
} from './worker-requests'
export {
  // Recording functions
  recordRequest,
  recordRequests,
  // Metrics functions
  getRequestMetrics,
  getCurrentMetrics,
  getPathLatency,
  getErrorSummary,
  // MV definition
  createWorkerRequestsMV,
  // Buffer for high-throughput
  RequestBuffer,
  createRequestBuffer,
  // Helper functions
  getStatusCategory,
  generateRequestId,
  percentile,
  getBucketStart,
  getBucketEnd,
  // Constants
  DEFAULT_REQUESTS_COLLECTION,
  DEFAULT_METRICS_COLLECTION,
  DEFAULT_FLUSH_INTERVAL_MS,
  DEFAULT_BUFFER_SIZE,
} from './worker-requests'

// Eval Scores MV
export type {
  EvalScoreRecord,
  ScoreStatistics,
  ScoreTrendPoint,
  EvalScoresStats,
  EvalScoresConfig,
} from './eval-scores'
export {
  EvalScoresMV,
  createEvalScoresMV,
} from './eval-scores'

// Generated Content MV
export type {
  GeneratedContentType,
  ContentClassification,
  FinishReason,
  GeneratedContentRecord,
  RecordContentInput,
  GeneratedContentStats,
  GeneratedContentMVConfig,
} from './generated-content'
export {
  GeneratedContentMV,
  createGeneratedContentMV,
  createGeneratedContentMVHandler,
  GENERATED_CONTENT_SCHEMA,
  detectContentType,
  detectCodeLanguage,
  estimateTokenCount,
} from './generated-content'

// AI Requests MV
export type {
  AIRequestsMVOptions,
  AIRequestType,
  AIProvider,
  AIRequest,
  RecordAIRequestInput,
  AITimeBucket,
  AIMetrics,
  GetAIMetricsOptions,
  ModelPricing,
} from './ai-requests'
export {
  // Recording functions
  recordAIRequest,
  recordAIRequests,
  // Metrics functions
  getAIMetrics,
  getCurrentAIMetrics,
  getAICostSummary,
  getAIErrorSummary,
  // MV definition
  createAIRequestsMV,
  // Buffer for high-throughput
  AIRequestBuffer,
  createAIRequestBuffer,
  // Helper functions
  generateAIRequestId,
  calculateCost,
  percentile as aiPercentile,
  getAIBucketStart,
  getAIBucketEnd,
  // Constants
  DEFAULT_AI_REQUESTS_COLLECTION,
  DEFAULT_AI_METRICS_COLLECTION,
  DEFAULT_AI_FLUSH_INTERVAL_MS,
  DEFAULT_AI_BUFFER_SIZE,
  DEFAULT_MODEL_PRICING,
} from './ai-requests'

// Types - Streaming engine types (re-exported from materialized-views/streaming via ./types)
export type {
  MVHandler,
  StreamingRefreshConfig,
  StreamingStats,
  ErrorHandler,
  WarningHandler,
} from './types'

// Types - Worker error types (defined in ./types)
export type {
  WorkerError,
  WorkerErrorStats,
  ErrorCategory,
  ErrorSeverity,
  ErrorPattern,
} from './types'

export {
  DEFAULT_ERROR_PATTERNS,
  classifyError,
  severityFromStatus,
  categoryFromStatus,
} from './types'

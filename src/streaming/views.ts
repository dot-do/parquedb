/**
 * Stream Collections and View Definitions for ParqueDB
 *
 * This module provides:
 * 1. **Stream Collections** - Importable collection schemas with `$ingest` directive
 *    that automatically wire up data ingestion from external sources (AI SDK, tail events, etc.)
 *
 * 2. **Stream Views** - CDC-based windowed views using `defineStreamView()` API
 *
 * The NEW pattern for materialized views:
 * - Stream collections have `$ingest` directive to wire up automatic ingestion
 * - MVs use `$from` to reference source collections (no `$stream` directive needed)
 * - Data flows: Source -> Stream Collection -> MV (via $from)
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { AIRequests, TailEvents } from 'parquedb/streaming'
 *
 * const db = DB({
 *   // Import stream collections (auto-ingestion via $ingest)
 *   AIRequests,
 *   TailEvents,
 *
 *   // MVs use $from to reference stream collections
 *   DailyAIUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }, 'modelId'],
 *     $compute: {
 *       requestCount: { $count: '*' },
 *       totalTokens: { $sum: 'tokens' },
 *     },
 *   },
 *
 *   WorkerErrors: {
 *     $from: 'TailEvents',
 *     $filter: { outcome: { $ne: 'ok' } },
 *   },
 * }, { storage })
 * ```
 */

import type { Filter } from '../types/filter'
import type { AggregationStage } from '../aggregation/types'
import type { MVState, IngestSource } from '../materialized-views/types'

// Re-export IngestSource from the canonical source
export type { IngestSource }

/**
 * Stream collection schema definition
 *
 * Stream collections are regular collections that handle ingestion automatically.
 * When included in a DB schema, ParqueDB:
 * 1. Creates the collection
 * 2. Wires up ingestion from the source (via `$ingest`)
 * 3. Makes data available for MVs via `$from`
 */
export interface StreamCollectionSchema {
  /** Entity type name */
  $type: string

  /** Ingest source - wires up automatic data ingestion */
  $ingest: IngestSource

  /** Field definitions (IceType format) */
  [field: string]: string | StreamCollectionSchema | IngestSource | undefined
}

/**
 * Check if a schema is a stream collection (has $ingest directive)
 */
export function isStreamCollection(schema: unknown): schema is StreamCollectionSchema {
  return (
    typeof schema === 'object' &&
    schema !== null &&
    '$ingest' in schema &&
    typeof (schema as Record<string, unknown>).$ingest === 'string'
  )
}

// =============================================================================
// Pre-built Stream Collections
// =============================================================================

/**
 * AI Requests stream collection
 *
 * Captures all AI SDK requests (generate, stream, embed).
 * Use with AI SDK middleware for automatic ingestion.
 *
 * @example
 * ```typescript
 * import { AIRequests } from 'parquedb/streaming'
 *
 * const db = DB({
 *   AIRequests,  // Auto-ingests from AI SDK middleware
 *
 *   DailyUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }],
 *     $compute: { count: { $count: '*' } },
 *   },
 * })
 * ```
 */
export const AIRequests: StreamCollectionSchema = {
  $type: 'AIRequest',
  $ingest: 'ai-sdk',
  modelId: 'string!',
  providerId: 'string!',
  requestType: 'string!',     // 'generate' | 'stream' | 'embed'
  tokens: 'int?',
  promptTokens: 'int?',
  completionTokens: 'int?',
  latencyMs: 'int!',
  cached: 'boolean!',
  error: 'variant?',
  timestamp: 'timestamp!',
}

/**
 * AI Generations stream collection
 *
 * Captures generated text and objects from AI SDK.
 *
 * @example
 * ```typescript
 * import { Generations } from 'parquedb/streaming'
 *
 * const db = DB({
 *   Generations,
 *
 *   GeneratedObjects: {
 *     $from: 'Generations',
 *     $filter: { contentType: 'object' },
 *   },
 * })
 * ```
 */
export const Generations: StreamCollectionSchema = {
  $type: 'Generation',
  $ingest: 'ai-sdk',
  modelId: 'string!',
  contentType: 'string!',     // 'text' | 'object'
  content: 'variant!',        // The generated text or object
  prompt: 'string?',
  tokens: 'int?',
  timestamp: 'timestamp!',
}

/**
 * Tail Events stream collection
 *
 * Captures Cloudflare Worker tail events (logs, errors, requests).
 * Use with tail handler for automatic ingestion.
 *
 * @example
 * ```typescript
 * import { TailEvents } from 'parquedb/streaming'
 *
 * const db = DB({
 *   TailEvents,
 *
 *   WorkerErrors: {
 *     $from: 'TailEvents',
 *     $filter: { outcome: { $ne: 'ok' } },
 *   },
 * })
 * ```
 */
export const TailEvents: StreamCollectionSchema = {
  $type: 'TailEvent',
  $ingest: 'tail',
  scriptName: 'string!',
  outcome: 'string!',         // 'ok' | 'exception' | 'exceededCpu' | ...
  eventTimestamp: 'timestamp!',
  event: 'variant?',          // { request, response } for fetch events
  logs: 'variant[]',          // Console.log messages
  exceptions: 'variant[]',    // Uncaught exceptions
  timestamp: 'timestamp!',
}

/**
 * Eval Runs stream collection
 *
 * Captures evalite evaluation runs.
 *
 * @example
 * ```typescript
 * import { EvalRuns } from 'parquedb/streaming'
 *
 * const db = DB({
 *   EvalRuns,
 *
 *   RecentRuns: {
 *     $from: 'EvalRuns',
 *     $filter: { timestamp: { $gte: '$lastWeek' } },
 *   },
 * })
 * ```
 */
export const EvalRuns: StreamCollectionSchema = {
  $type: 'EvalRun',
  $ingest: 'evalite',
  runId: 'int!',
  runType: 'string!',
  timestamp: 'timestamp!',
}

/**
 * Eval Scores stream collection
 *
 * Captures evalite evaluation scores.
 *
 * @example
 * ```typescript
 * import { EvalScores } from 'parquedb/streaming'
 *
 * const db = DB({
 *   EvalScores,
 *
 *   ScoreTrends: {
 *     $from: 'EvalScores',
 *     $groupBy: ['suiteName', { week: '$timestamp' }],
 *     $compute: { avgScore: { $avg: 'score' } },
 *   },
 * })
 * ```
 */
export const EvalScores: StreamCollectionSchema = {
  $type: 'EvalScore',
  $ingest: 'evalite',
  runId: 'int!',
  suiteName: 'string!',
  scorerName: 'string!',
  score: 'decimal(5,4)!',
  timestamp: 'timestamp!',
}

// =============================================================================
// Stream View Name (Branded Type)
// =============================================================================

/**
 * Branded type for stream view names
 */
export type StreamViewName = string & { __brand: 'StreamViewName' }

/**
 * Create a StreamViewName from a string
 */
export function streamViewName(name: string): StreamViewName {
  return name as StreamViewName
}

// =============================================================================
// Window Types
// =============================================================================

/**
 * Duration specification for windows
 */
export interface WindowDuration {
  /** Milliseconds */
  ms?: number | undefined
  /** Seconds */
  seconds?: number | undefined
  /** Minutes */
  minutes?: number | undefined
  /** Hours */
  hours?: number | undefined
  /** Days */
  days?: number | undefined
}

/**
 * Tumbling window - non-overlapping fixed-size windows
 *
 * @example
 * ```
 * |---Window 1---|---Window 2---|---Window 3---|
 * |  events A    |  events B    |  events C    |
 * ```
 */
export interface TumblingWindow {
  type: 'tumbling'
  /** Window size */
  size: WindowDuration
  /** Optional offset for window alignment */
  offset?: WindowDuration | undefined
}

/**
 * Sliding window - overlapping windows with fixed size and slide interval
 *
 * @example
 * ```
 * |---Window 1---------|
 *      |---Window 2---------|
 *           |---Window 3---------|
 * ```
 */
export interface SlidingWindow {
  type: 'sliding'
  /** Window size */
  size: WindowDuration
  /** Slide interval (how often a new window starts) */
  slide: WindowDuration
}

/**
 * Session window - windows based on activity gaps
 * Groups events that arrive within the gap timeout of each other
 *
 * @example
 * ```
 * |--Session 1--|  (gap)  |--Session 2--|
 * | e1 e2 e3    |         | e4 e5       |
 * ```
 */
export interface SessionWindow {
  type: 'session'
  /** Maximum gap between events in the same session */
  gap: WindowDuration
  /** Maximum session duration (optional) */
  maxDuration?: WindowDuration | undefined
}

/**
 * Global window - all events in a single window (no windowing)
 * Useful for accumulating all-time aggregates
 */
export interface GlobalWindow {
  type: 'global'
}

/**
 * Union of all window types
 */
export type WindowConfig = TumblingWindow | SlidingWindow | SessionWindow | GlobalWindow

// =============================================================================
// Source Configuration
// =============================================================================

/**
 * Stream source configuration
 */
export interface StreamSourceConfig {
  /**
   * Source collection name
   */
  collection: string

  /**
   * Optional filter to apply to source events
   * Only events matching this filter will be processed
   */
  filter?: Filter | undefined

  /**
   * Event types to process
   * @default ['CREATE', 'UPDATE', 'DELETE']
   */
  eventTypes?: Array<'CREATE' | 'UPDATE' | 'DELETE'> | undefined

  /**
   * Start position for processing events
   * - 'earliest': Process all historical events
   * - 'latest': Only process new events
   * - timestamp: Start from specific timestamp (ms since epoch)
   * @default 'latest'
   */
  startPosition?: 'earliest' | 'latest' | number | undefined
}

// =============================================================================
// Transform Configuration
// =============================================================================

/**
 * Built-in transform functions
 */
export type BuiltInTransform =
  | 'passthrough' // No transformation, forward events as-is
  | 'count'       // Count events
  | 'sum'         // Sum a numeric field
  | 'avg'         // Average a numeric field
  | 'min'         // Minimum value
  | 'max'         // Maximum value
  | 'first'       // First event in window
  | 'last'        // Last event in window

/**
 * Custom transform function type
 */
export type TransformFunction<TInput = unknown, TOutput = unknown> = (
  events: TInput[],
  context: TransformContext
) => TOutput[] | Promise<TOutput[]>

/**
 * Context passed to transform functions
 */
export interface TransformContext {
  /** Window start timestamp (ms since epoch) */
  windowStart: number
  /** Window end timestamp (ms since epoch) */
  windowEnd: number
  /** View name */
  viewName: StreamViewName
  /** Current window key (for keyed aggregations) */
  windowKey?: string | undefined
}

/**
 * Transform configuration
 */
export interface TransformConfig<TInput = unknown, TOutput = unknown> {
  /**
   * Aggregation pipeline (MongoDB-style)
   * Applied to events in each window
   */
  pipeline?: AggregationStage[] | undefined

  /**
   * Built-in transform shorthand
   */
  builtin?: BuiltInTransform | undefined

  /**
   * Field for builtin aggregations (required for sum, avg, min, max)
   */
  field?: string | undefined

  /**
   * Custom transform function
   */
  fn?: TransformFunction<TInput, TOutput> | undefined

  /**
   * Field to group by before applying transform
   * Creates separate windows per group
   */
  groupBy?: string | string[] | undefined
}

// =============================================================================
// Output Configuration
// =============================================================================

/**
 * Output sink types
 */
export type OutputSinkType = 'collection' | 'webhook' | 'queue' | 'console'

/**
 * Collection output sink
 */
export interface CollectionSink {
  type: 'collection'
  /** Target collection name */
  collection: string
  /** How to write results: 'upsert' | 'append' */
  mode?: 'upsert' | 'append' | undefined
  /** Fields to use as key for upsert */
  keyFields?: string[] | undefined
}

/**
 * Webhook output sink
 */
export interface WebhookSink {
  type: 'webhook'
  /** Webhook URL */
  url: string
  /** HTTP method */
  method?: 'POST' | 'PUT' | undefined
  /** Custom headers */
  headers?: Record<string, string> | undefined
  /** Batch size for webhook calls */
  batchSize?: number | undefined
}

/**
 * Queue output sink (for Workers Queue, SQS, etc.)
 */
export interface QueueSink {
  type: 'queue'
  /** Queue name/binding */
  queue: string
  /** Message format */
  format?: 'json' | 'msgpack' | undefined
}

/**
 * Console output sink (for debugging)
 */
export interface ConsoleSink {
  type: 'console'
  /** Log level */
  level?: 'log' | 'info' | 'debug' | undefined
}

/**
 * Union of all output sink types
 */
export type OutputSink = CollectionSink | WebhookSink | QueueSink | ConsoleSink

/**
 * Output configuration
 */
export interface OutputConfig {
  /**
   * Output sink (defaults to collection with same name as view)
   */
  sink?: OutputSink | undefined

  /**
   * Batch results before writing
   */
  batchSize?: number | undefined

  /**
   * Maximum delay before flushing batch (ms)
   */
  batchTimeoutMs?: number | undefined
}

// =============================================================================
// Watermark Configuration
// =============================================================================

/**
 * Watermark strategy for handling late events
 */
export interface WatermarkConfig {
  /**
   * Maximum allowed lateness for events
   * Events arriving later than this are dropped
   */
  maxLateness?: WindowDuration | undefined

  /**
   * How to handle late events
   * - 'drop': Discard late events
   * - 'update': Update previously emitted results
   * - 'sideOutput': Send to separate collection
   * @default 'drop'
   */
  lateEventPolicy?: 'drop' | 'update' | 'sideOutput' | undefined

  /**
   * Collection name for side output (required if lateEventPolicy is 'sideOutput')
   */
  sideOutputCollection?: string | undefined
}

// =============================================================================
// Stream View Definition
// =============================================================================

/**
 * Complete stream view definition
 */
export interface StreamViewDefinition<TInput = unknown, TOutput = unknown> {
  /**
   * Unique name for the stream view
   */
  name: string

  /**
   * Source configuration
   */
  source: StreamSourceConfig

  /**
   * Transform configuration
   */
  transform?: TransformConfig<TInput, TOutput> | undefined

  /**
   * Window configuration
   * @default { type: 'global' }
   */
  window?: WindowConfig | undefined

  /**
   * Output configuration
   */
  output?: OutputConfig | undefined

  /**
   * Watermark configuration for late event handling
   */
  watermark?: WatermarkConfig | undefined

  /**
   * Whether the view is enabled
   * @default true
   */
  enabled?: boolean | undefined

  /**
   * Description for documentation
   */
  description?: string | undefined

  /**
   * Tags for organizing views
   */
  tags?: string[] | undefined

  /**
   * Custom metadata
   */
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// Stream View Instance
// =============================================================================

/**
 * Runtime state of a stream view
 */
export type StreamViewState = MVState

/**
 * Stream view instance with runtime methods
 */
export interface StreamView<TInput = unknown, TOutput = unknown> {
  /** View definition */
  readonly definition: StreamViewDefinition<TInput, TOutput>

  /** Validated and normalized name */
  readonly name: StreamViewName

  /** Current state */
  readonly state: StreamViewState

  /**
   * Start processing events
   */
  start(): Promise<void>

  /**
   * Stop processing events
   */
  stop(): Promise<void>

  /**
   * Pause processing (keeps position)
   */
  pause(): Promise<void>

  /**
   * Resume processing from paused position
   */
  resume(): Promise<void>

  /**
   * Get current processing position
   */
  getPosition(): Promise<StreamPosition>

  /**
   * Reset to a specific position
   */
  resetPosition(position: 'earliest' | 'latest' | number): Promise<void>

  /**
   * Get view statistics
   */
  getStats(): Promise<StreamViewStats>
}

/**
 * Stream processing position
 */
export interface StreamPosition {
  /** Last processed event timestamp */
  timestamp: number
  /** Last processed event ID */
  eventId?: string | undefined
  /** Sequence number */
  sequence?: number | undefined
}

/**
 * Stream view statistics
 */
export interface StreamViewStats {
  /** Total events processed */
  eventsProcessed: number
  /** Total events filtered out */
  eventsFiltered: number
  /** Total events dropped (late) */
  eventsDropped: number
  /** Total windows completed */
  windowsCompleted: number
  /** Total outputs written */
  outputsWritten: number
  /** Average processing latency (ms) */
  avgLatencyMs: number
  /** Last event timestamp */
  lastEventTimestamp?: number | undefined
  /** Last output timestamp */
  lastOutputTimestamp?: number | undefined
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Convert WindowDuration to milliseconds
 */
export function durationToMs(duration: WindowDuration): number {
  let ms = 0
  if (duration.ms) ms += duration.ms
  if (duration.seconds) ms += duration.seconds * 1000
  if (duration.minutes) ms += duration.minutes * 60 * 1000
  if (duration.hours) ms += duration.hours * 60 * 60 * 1000
  if (duration.days) ms += duration.days * 24 * 60 * 60 * 1000
  return ms
}

/**
 * Validate a stream view definition
 */
export function validateStreamViewDefinition(
  definition: StreamViewDefinition
): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  // Name validation
  if (!definition.name || typeof definition.name !== 'string') {
    errors.push('Stream view name is required')
  } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(definition.name)) {
    errors.push('Stream view name must be a valid identifier (letters, numbers, underscores)')
  }

  // Source validation
  if (!definition.source) {
    errors.push('Source configuration is required')
  } else {
    if (!definition.source.collection || typeof definition.source.collection !== 'string') {
      errors.push('Source collection is required')
    }
  }

  // Window validation
  if (definition.window) {
    const { window } = definition
    if (window.type === 'tumbling') {
      if (!window.size || durationToMs(window.size) <= 0) {
        errors.push('Tumbling window requires a positive size')
      }
    } else if (window.type === 'sliding') {
      if (!window.size || durationToMs(window.size) <= 0) {
        errors.push('Sliding window requires a positive size')
      }
      if (!window.slide || durationToMs(window.slide) <= 0) {
        errors.push('Sliding window requires a positive slide interval')
      }
      if (window.slide && window.size && durationToMs(window.slide) > durationToMs(window.size)) {
        errors.push('Sliding window slide interval should not exceed window size')
      }
    } else if (window.type === 'session') {
      if (!window.gap || durationToMs(window.gap) <= 0) {
        errors.push('Session window requires a positive gap timeout')
      }
    }
  }

  // Transform validation
  if (definition.transform) {
    const { transform } = definition
    if (transform.builtin) {
      const needsField = ['sum', 'avg', 'min', 'max']
      if (needsField.includes(transform.builtin) && !transform.field) {
        errors.push(`Transform '${transform.builtin}' requires a field to be specified`)
      }
    }
  }

  // Watermark validation
  if (definition.watermark?.lateEventPolicy === 'sideOutput') {
    if (!definition.watermark.sideOutputCollection) {
      errors.push('Side output policy requires sideOutputCollection to be specified')
    }
  }

  return { valid: errors.length === 0, errors }
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a window config is a tumbling window
 */
export function isTumblingWindow(window: WindowConfig): window is TumblingWindow {
  return window.type === 'tumbling'
}

/**
 * Check if a window config is a sliding window
 */
export function isSlidingWindow(window: WindowConfig): window is SlidingWindow {
  return window.type === 'sliding'
}

/**
 * Check if a window config is a session window
 */
export function isSessionWindow(window: WindowConfig): window is SessionWindow {
  return window.type === 'session'
}

/**
 * Check if a window config is a global window
 */
export function isGlobalWindow(window: WindowConfig): window is GlobalWindow {
  return window.type === 'global'
}

/**
 * Check if an output sink is a collection sink
 */
export function isCollectionSink(sink: OutputSink): sink is CollectionSink {
  return sink.type === 'collection'
}

/**
 * Check if an output sink is a webhook sink
 */
export function isWebhookSink(sink: OutputSink): sink is WebhookSink {
  return sink.type === 'webhook'
}

/**
 * Check if an output sink is a queue sink
 */
export function isQueueSink(sink: OutputSink): sink is QueueSink {
  return sink.type === 'queue'
}

/**
 * Check if an output sink is a console sink
 */
export function isConsoleSink(sink: OutputSink): sink is ConsoleSink {
  return sink.type === 'console'
}

// =============================================================================
// Main API
// =============================================================================

/**
 * Define a stream-based view with type safety
 *
 * Stream views automatically process CDC events and maintain materialized results.
 * They support windowing, transformations, and multiple output sinks.
 *
 * @example
 * ```typescript
 * // Simple view - filter and project users
 * const activeUsers = defineStreamView({
 *   name: 'active_users',
 *   source: {
 *     collection: 'users',
 *     filter: { status: 'active' }
 *   },
 *   transform: {
 *     pipeline: [
 *       { $project: { name: 1, email: 1 } }
 *     ]
 *   }
 * })
 *
 * // Windowed aggregation - count events per minute
 * const eventCounts = defineStreamView({
 *   name: 'event_counts',
 *   source: { collection: 'events' },
 *   transform: {
 *     builtin: 'count',
 *     groupBy: 'type'
 *   },
 *   window: {
 *     type: 'tumbling',
 *     size: { minutes: 1 }
 *   }
 * })
 *
 * // Session-based aggregation
 * const userSessions = defineStreamView({
 *   name: 'user_sessions',
 *   source: { collection: 'page_views' },
 *   transform: {
 *     groupBy: 'userId',
 *     pipeline: [
 *       { $group: {
 *         _id: null,
 *         pageCount: { $sum: 1 },
 *         pages: { $push: '$path' }
 *       }}
 *     ]
 *   },
 *   window: {
 *     type: 'session',
 *     gap: { minutes: 30 }
 *   }
 * })
 * ```
 *
 * @param definition - Stream view definition
 * @returns Validated stream view definition
 */
export function defineStreamView<TInput = unknown, TOutput = unknown>(
  definition: StreamViewDefinition<TInput, TOutput>
): StreamViewDefinition<TInput, TOutput> {
  // Validate the definition
  const validation = validateStreamViewDefinition(definition)
  if (!validation.valid) {
    throw new Error(`Invalid stream view definition: ${validation.errors.join(', ')}`)
  }

  // Return the definition with defaults applied
  return {
    ...definition,
    // Apply defaults
    window: definition.window ?? { type: 'global' },
    enabled: definition.enabled ?? true,
    source: {
      ...definition.source,
      eventTypes: definition.source.eventTypes ?? ['CREATE', 'UPDATE', 'DELETE'],
      startPosition: definition.source.startPosition ?? 'latest',
    },
    output: definition.output ?? {
      sink: {
        type: 'collection',
        collection: definition.name,
        mode: 'upsert',
      },
    },
    watermark: definition.watermark ?? {
      lateEventPolicy: 'drop',
    },
  }
}

/**
 * Create multiple stream views at once
 *
 * @example
 * ```typescript
 * const views = defineStreamViews({
 *   activeUsers: {
 *     source: { collection: 'users', filter: { active: true } }
 *   },
 *   recentPosts: {
 *     source: { collection: 'posts' },
 *     window: { type: 'tumbling', size: { hours: 1 } }
 *   }
 * })
 * ```
 */
export function defineStreamViews<T extends Record<string, Omit<StreamViewDefinition, 'name'>>>(
  definitions: T
): { [K in keyof T]: StreamViewDefinition } {
  const result = {} as { [K in keyof T]: StreamViewDefinition }

  for (const [name, def] of Object.entries(definitions)) {
    result[name as keyof T] = defineStreamView({
      ...def,
      name,
    } as StreamViewDefinition)
  }

  return result
}

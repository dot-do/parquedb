/**
 * ParqueDB Tail Integration
 *
 * Provides TailEvents as a stream collection for ingesting Cloudflare Worker
 * tail events into ParqueDB.
 *
 * @example
 * ```typescript
 * import { TailEvents } from 'parquedb/tail'
 *
 * const db = DB({
 *   TailEvents,
 *
 *   // Create derived views
 *   WorkerErrors: {
 *     $from: 'TailEvents',
 *     $filter: { outcome: { $ne: 'ok' } },
 *   },
 *
 *   SlowRequests: {
 *     $from: 'TailEvents',
 *     $filter: { 'event.durationMs': { $gt: 1000 } },
 *   },
 * }, { storage })
 * ```
 *
 * @see https://developers.cloudflare.com/workers/observability/logs/tail-workers/
 */

import { createSafeRegex } from '../../utils/safe-regex'
import { DEFAULT_TAIL_BATCH_MAX_WAIT_MS } from '../../constants'

// =============================================================================
// TailEvents Stream Collection Definition
// =============================================================================

/**
 * TailEvents stream collection schema
 *
 * This collection is designed to receive events from Cloudflare Tail Workers.
 * The `$ingest: 'tail'` directive wires up the tail handler automatically.
 *
 * Schema matches the Cloudflare TraceItem structure:
 * @see https://developers.cloudflare.com/workers/observability/logs/tail-workers/#trace-item
 */
export const TailEvents = {
  /** Entity type for tail events */
  $type: 'TailEvent',

  /** Name of the producer Worker script */
  scriptName: 'string!',

  /**
   * Execution outcome
   * - 'ok': Successful execution
   * - 'exception': Uncaught exception
   * - 'exceededCpu': CPU time limit exceeded
   * - 'exceededMemory': Memory limit exceeded
   * - 'scriptNotFound': Worker script not found
   * - 'canceled': Request was canceled
   * - 'unknown': Unknown outcome
   */
  outcome: 'string!',

  /** Unix timestamp when the event occurred (ms since epoch) */
  eventTimestamp: 'timestamp!',

  /**
   * Event information (request, response, scheduled, queue, etc.)
   * Structure depends on the trigger type:
   * - fetch: { request: TailRequest, response?: TailResponse }
   * - scheduled: { scheduledTime: number, cron: string }
   * - queue: { queue: string, batchSize: number }
   */
  event: 'variant?',

  /**
   * Array of console log entries
   * Each log has: { timestamp, level, message }
   */
  logs: 'variant[]',

  /**
   * Array of exceptions that occurred
   * Each exception has: { timestamp, name, message }
   */
  exceptions: 'variant[]',

  /**
   * Ingest directive - wires up the tail handler
   * When a Worker with this collection receives tail events,
   * they are automatically ingested into this collection.
   */
  $ingest: 'tail',
} as const

/**
 * Type for a TailEvents entity
 */
export interface TailEvent {
  $id: string
  $type: 'TailEvent'
  name: string
  scriptName: string
  outcome: TailOutcome
  eventTimestamp: Date
  event?: TailEventInfo | undefined
  logs: TailLog[]
  exceptions: TailException[]
  createdAt: Date
  createdBy: string
  updatedAt: Date
  updatedBy: string
  version: number
}

// =============================================================================
// Supporting Types (from Cloudflare Workers Runtime)
// =============================================================================

/**
 * Outcome of a traced Worker execution
 */
export type TailOutcome =
  | 'unknown'
  | 'ok'
  | 'exception'
  | 'exceededCpu'
  | 'exceededMemory'
  | 'scriptNotFound'
  | 'canceled'
  | 'responseStreamDisconnected'

/**
 * Log level for console output
 */
export type TailLogLevel = 'debug' | 'info' | 'log' | 'warn' | 'error'

/**
 * A log message from console.log/warn/error etc.
 */
export interface TailLog {
  /** Timestamp when the log was recorded (ms since epoch) */
  timestamp: number
  /** Log level */
  level: TailLogLevel
  /** Log message arguments (serialized) */
  message: unknown[]
}

/**
 * An exception that occurred during Worker execution
 */
export interface TailException {
  /** Timestamp when the exception occurred (ms since epoch) */
  timestamp: number
  /** Exception name/type */
  name: string
  /** Exception message */
  message: string
}

/**
 * HTTP request information from a traced Worker
 */
export interface TailRequest {
  /** HTTP method */
  method: string
  /** Request URL */
  url: string
  /** Request headers */
  headers: Record<string, string>
  /** Cloudflare-specific request properties */
  cf?: TailCfProperties | undefined
}

/**
 * Cloudflare-specific request properties
 */
export interface TailCfProperties {
  /** Cloudflare data center (colo) */
  colo?: string | undefined
  /** Country code */
  country?: string | undefined
  /** City name */
  city?: string | undefined
  /** ASN */
  asn?: number | undefined
  /** AS Organization */
  asOrganization?: string | undefined
  /** Client trust score */
  clientTrustScore?: number | undefined
  /** HTTP protocol version */
  httpProtocol?: string | undefined
  /** Request priority */
  requestPriority?: string | undefined
  /** TLS cipher */
  tlsCipher?: string | undefined
  /** TLS version */
  tlsVersion?: string | undefined
  /** Additional properties */
  [key: string]: unknown
}

/**
 * HTTP response information from a traced Worker
 */
export interface TailResponse {
  /** HTTP status code */
  status: number
}

/**
 * Event information for different trigger types
 */
export interface TailEventInfo {
  /** Request details (for fetch events) */
  request?: TailRequest | undefined
  /** Response details (for fetch events) */
  response?: TailResponse | undefined
  /** Scheduled event time (for cron triggers) */
  scheduledTime?: number | undefined
  /** Cron expression (for cron triggers) */
  cron?: string | undefined
  /** Queue name (for queue consumers) */
  queue?: string | undefined
  /** Batch size (for queue consumers) */
  batchSize?: number | undefined
  /** Request duration in milliseconds */
  durationMs?: number | undefined
}

/**
 * A single trace item from a Tail Worker (raw Cloudflare format)
 */
export interface TraceItem {
  /** Name of the producer Worker script */
  scriptName: string | null
  /** Execution outcome */
  outcome: string
  /** Unix timestamp when the event occurred */
  eventTimestamp: number | null
  /** Event information */
  event: TailEventInfo | null
  /** Array of log entries */
  logs: TailLog[]
  /** Array of exceptions */
  exceptions: TailException[]
  /** Diagnostics channel events */
  diagnosticsChannelEvents?: unknown[] | undefined
}

// =============================================================================
// Helper Collections (Common Derived Views)
// =============================================================================

/**
 * WorkerErrors - Derived view for failed Worker executions
 *
 * @example
 * ```typescript
 * import { TailEvents, WorkerErrors } from 'parquedb/tail'
 *
 * const db = DB({ TailEvents, WorkerErrors }, { storage })
 *
 * // Query errors from the last hour
 * const recentErrors = await db.WorkerErrors.find({
 *   eventTimestamp: { $gte: new Date(Date.now() - 3600000) }
 * })
 * ```
 */
export const WorkerErrors = {
  $type: 'WorkerError',
  $from: 'TailEvents',
  $filter: { outcome: { $ne: 'ok' } },
} as const

/**
 * WorkerExceptions - Derived view for Worker exceptions
 *
 * @example
 * ```typescript
 * import { TailEvents, WorkerExceptions } from 'parquedb/tail'
 *
 * const db = DB({ TailEvents, WorkerExceptions }, { storage })
 *
 * // Find TypeError exceptions
 * const typeErrors = await db.WorkerExceptions.find({
 *   'exceptions.name': 'TypeError'
 * })
 * ```
 */
export const WorkerExceptions = {
  $type: 'WorkerException',
  $from: 'TailEvents',
  $filter: { 'exceptions.0': { $exists: true } },
} as const

/**
 * WorkerLogs - Derived view for Worker logs at warn/error level
 *
 * @example
 * ```typescript
 * import { TailEvents, WorkerLogs } from 'parquedb/tail'
 *
 * const db = DB({ TailEvents, WorkerLogs }, { storage })
 *
 * // Find error logs
 * const errorLogs = await db.WorkerLogs.find({
 *   'logs.level': 'error'
 * })
 * ```
 */
export const WorkerLogs = {
  $type: 'WorkerLog',
  $from: 'TailEvents',
  $filter: {
    $or: [
      { 'logs.level': 'warn' },
      { 'logs.level': 'error' },
    ],
  },
} as const

// =============================================================================
// Tail Handler Factory
// =============================================================================

/**
 * Configuration for the tail handler
 */
export interface TailHandlerConfig {
  /** Filter events before ingestion */
  filter?: {
    /** Only include events from these script names */
    scriptNames?: string[] | undefined
    /** Only include events with these outcomes */
    outcomes?: TailOutcome[] | undefined
    /** Only include events with exceptions */
    exceptionsOnly?: boolean | undefined
  } | undefined
  /** Transform events before ingestion */
  transform?: ((item: TraceItem) => TraceItem | null) | undefined
  /** Generate custom entity name */
  nameGenerator?: ((item: TraceItem) => string) | undefined
}

/**
 * Create a tail handler for ingesting events into ParqueDB
 *
 * @example
 * ```typescript
 * import { createTailHandler, TailEvents } from 'parquedb/tail'
 * import { DB } from 'parquedb'
 *
 * const db = DB({ TailEvents }, { storage })
 *
 * export default {
 *   async tail(events: TraceItem[]) {
 *     const handler = createTailHandler(db)
 *     await handler(events)
 *   }
 * }
 * ```
 */
export function createTailHandler<T extends { TailEvents: { create: (data: unknown) => Promise<unknown> } }>(
  db: T,
  config: TailHandlerConfig = {}
): (events: TraceItem[]) => Promise<void> {
  const { filter, transform, nameGenerator } = config

  return async (events: TraceItem[]) => {
    for (const item of events) {
      // Apply filter
      if (filter) {
        if (filter.scriptNames && item.scriptName && !filter.scriptNames.includes(item.scriptName)) {
          continue
        }
        if (filter.outcomes && !filter.outcomes.includes(item.outcome as TailOutcome)) {
          continue
        }
        if (filter.exceptionsOnly && item.exceptions.length === 0) {
          continue
        }
      }

      // Apply transform
      let processedItem = item
      if (transform) {
        const result = transform(item)
        if (result === null) continue
        processedItem = result
      }

      // Generate name
      const name = nameGenerator
        ? nameGenerator(processedItem)
        : `${processedItem.scriptName || 'unknown'}:${processedItem.outcome}`

      // Create entity
      await db.TailEvents.create({
        $type: 'TailEvent',
        name,
        scriptName: processedItem.scriptName || 'unknown',
        outcome: processedItem.outcome,
        eventTimestamp: processedItem.eventTimestamp
          ? new Date(processedItem.eventTimestamp)
          : new Date(),
        event: processedItem.event,
        logs: processedItem.logs,
        exceptions: processedItem.exceptions,
      })
    }
  }
}

// =============================================================================
// Filter Utilities
// =============================================================================

/**
 * Filter configuration for tail events
 */
export interface TailEventFilter {
  /** Only include events from these script names */
  scriptNames?: string[] | undefined
  /** Only include events with these outcomes */
  outcomes?: TailOutcome[] | undefined
  /** Only include events with logs at these levels */
  logLevels?: TailLogLevel[] | undefined
  /** Only include events with exceptions */
  exceptionsOnly?: boolean | undefined
  /** Minimum number of logs to include */
  minLogs?: number | undefined
  /** URL patterns to include (glob-style) */
  urlPatterns?: string[] | undefined
}

/**
 * Default filter configuration
 */
export const DEFAULT_FILTER: TailEventFilter = {
  outcomes: ['ok', 'exception', 'exceededCpu', 'exceededMemory', 'unknown'],
  logLevels: ['log', 'info', 'warn', 'error'],
}

/**
 * Check if a URL matches a glob pattern
 */
function matchUrlPattern(url: string, pattern: string): boolean {
  const regexPattern = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.')
  return createSafeRegex(`^${regexPattern}$`).test(url)
}

/**
 * Apply filters to a trace item
 */
export function filterTraceItem(item: TraceItem, filter: TailEventFilter): boolean {
  // Filter by script name
  if (filter.scriptNames && filter.scriptNames.length > 0) {
    if (!item.scriptName || !filter.scriptNames.includes(item.scriptName)) {
      return false
    }
  }

  // Filter by outcome
  if (filter.outcomes && filter.outcomes.length > 0) {
    if (!filter.outcomes.includes(item.outcome as TailOutcome)) {
      return false
    }
  }

  // Filter by exceptions only
  if (filter.exceptionsOnly && item.exceptions.length === 0) {
    return false
  }

  // Filter by minimum logs
  if (filter.minLogs !== undefined && item.logs.length < filter.minLogs) {
    return false
  }

  // Filter by log levels
  if (filter.logLevels && filter.logLevels.length > 0) {
    const hasMatchingLog = item.logs.some((log) =>
      filter.logLevels!.includes(log.level)
    )
    if (item.logs.length > 0 && !hasMatchingLog) {
      return false
    }
  }

  // Filter by URL patterns
  if (filter.urlPatterns && filter.urlPatterns.length > 0 && item.event?.request?.url) {
    const url = item.event.request.url
    const matchesPattern = filter.urlPatterns.some((pattern) => matchUrlPattern(url, pattern))
    if (!matchesPattern) {
      return false
    }
  }

  return true
}

/**
 * Filter an array of trace items
 */
export function filterTraceItems(items: TraceItem[], filter: TailEventFilter): TraceItem[] {
  return items.filter((item) => filterTraceItem(item, filter))
}

// =============================================================================
// Batching Utilities
// =============================================================================

/**
 * Batching configuration
 */
export interface BatchConfig {
  /** Maximum events per batch before flush */
  maxEvents: number
  /** Maximum time (ms) to hold events before flush */
  maxWaitMs: number
  /** Minimum events before considering a flush */
  minEvents: number
}

/**
 * Default batch configuration
 */
export const DEFAULT_BATCH_CONFIG: BatchConfig = {
  maxEvents: 100,
  maxWaitMs: DEFAULT_TAIL_BATCH_MAX_WAIT_MS,
  minEvents: 1,
}

/**
 * Batch state for accumulating events
 */
export interface BatchState<T = TraceItem> {
  events: T[]
  startTime: number
  lastFlush: number
}

/**
 * Create a new batch state
 */
export function createBatchState<T = TraceItem>(): BatchState<T> {
  const now = Date.now()
  return {
    events: [],
    startTime: now,
    lastFlush: now,
  }
}

/**
 * Check if a batch should be flushed
 */
export function shouldFlushBatch<T>(state: BatchState<T>, config: BatchConfig): boolean {
  if (state.events.length === 0) {
    return false
  }

  // Max events reached
  if (state.events.length >= config.maxEvents) {
    return true
  }

  // Max wait time reached
  const elapsed = Date.now() - state.startTime
  if (elapsed >= config.maxWaitMs && state.events.length >= config.minEvents) {
    return true
  }

  return false
}

/**
 * Add events to batch and return events to flush (if any)
 */
export function addToBatch<T>(
  state: BatchState<T>,
  events: T[],
  config: BatchConfig
): T[] {
  state.events.push(...events)

  if (shouldFlushBatch(state, config)) {
    const toFlush = state.events
    state.events = []
    state.startTime = Date.now()
    state.lastFlush = Date.now()
    return toFlush
  }

  return []
}

// =============================================================================
// Processed Event Type (for analytics/storage)
// =============================================================================

/**
 * Processed event ready for storage/transmission
 */
export interface ProcessedEvent {
  /** Unique event ID */
  id: string
  /** Timestamp of the event */
  timestamp: string
  /** Producer script name */
  scriptName: string
  /** Execution outcome */
  outcome: string
  /** HTTP method (if applicable) */
  method?: string | undefined
  /** Request URL (if applicable) */
  url?: string | undefined
  /** Cloudflare colo */
  colo?: string | undefined
  /** Country code */
  country?: string | undefined
  /** Log count */
  logCount: number
  /** Exception count */
  exceptionCount: number
  /** Error messages (if any) */
  errors?: string[] | undefined
  /** Log messages (filtered) */
  logs?: Array<{ level: string; message: string; timestamp: number }> | undefined
  /** Duration estimate (if available) */
  durationMs?: number | undefined
}

/**
 * Generate a unique event ID
 */
function generateEventId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `${timestamp}-${random}`
}

/**
 * Transform a trace item into a processed event
 */
export function processTraceItem(item: TraceItem): ProcessedEvent {
  const event: ProcessedEvent = {
    id: generateEventId(),
    timestamp: item.eventTimestamp
      ? new Date(item.eventTimestamp).toISOString()
      : new Date().toISOString(),
    scriptName: item.scriptName || 'unknown',
    outcome: item.outcome,
    logCount: item.logs.length,
    exceptionCount: item.exceptions.length,
  }

  // Add request info
  if (item.event?.request) {
    event.method = item.event.request.method
    event.url = item.event.request.url
    event.colo = item.event.request.cf?.colo
    event.country = item.event.request.cf?.country
  }

  // Add duration if available
  if (item.event?.durationMs) {
    event.durationMs = item.event.durationMs
  }

  // Add errors
  if (item.exceptions.length > 0) {
    event.errors = item.exceptions.map((e) => `${e.name}: ${e.message}`)
  }

  // Add filtered logs (warn and error only for storage efficiency)
  const significantLogs = item.logs.filter(
    (log) => log.level === 'warn' || log.level === 'error'
  )
  if (significantLogs.length > 0) {
    event.logs = significantLogs.map((log) => ({
      level: log.level,
      message: typeof log.message === 'string' ? log.message : JSON.stringify(log.message),
      timestamp: log.timestamp,
    }))
  }

  return event
}

/**
 * Process multiple trace items
 */
export function processTraceItems(items: TraceItem[]): ProcessedEvent[] {
  return items.map(processTraceItem)
}

/**
 * ParqueDB Tail Worker - Analytics and Observability
 *
 * A Cloudflare Tail Worker that receives execution traces from producer Workers
 * and processes them for analytics, debugging, and observability.
 *
 * Features:
 * - Event filtering by outcome, script name, log level
 * - Batching with configurable thresholds
 * - R2 storage for historical logs
 * - Analytics Engine integration
 * - Custom event transformation
 * - Runtime input validation for graceful error handling
 *
 * @see https://developers.cloudflare.com/workers/observability/logs/tail-workers/
 */

import {
  validateTraceItems,
  type ValidatedTraceItem,
  type TailValidationConfig,
  type TraceItemsValidationResult,
} from './tail-validation'

// Re-export validation utilities
export {
  validateTraceItem,
  validateTraceItems,
  isValidTraceItem,
  createTailValidationError,
  type ValidatedTraceItem,
  type TailValidationConfig,
  type TraceItemsValidationResult,
  type TraceItemValidationResult,
} from './tail-validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Tail Worker environment bindings
 */
export interface TailWorkerEnv {
  /** R2 bucket for storing processed logs */
  LOGS_BUCKET?: R2Bucket

  /** Analytics Engine binding for metrics */
  ANALYTICS?: AnalyticsEngineDataset

  /** KV namespace for quick lookups */
  LOGS_KV?: KVNamespace

  /** Optional webhook URL for real-time alerts */
  ALERT_WEBHOOK_URL?: string

  /** Environment name for filtering */
  ENVIRONMENT?: string
}

/**
 * Log entry from console.log, console.error, etc.
 */
export interface TailLog {
  /** Unix timestamp in milliseconds */
  timestamp: number

  /** Log level: log, debug, info, warn, error */
  level: string

  /** Log message content */
  message: unknown
}

/**
 * Exception captured from the producer Worker
 */
export interface TailException {
  /** Exception name (e.g., "TypeError") */
  name: string

  /** Exception message */
  message: string

  /** Unix timestamp when exception was thrown */
  timestamp: number
}

/**
 * Request information from the triggering event
 */
export interface TailRequest {
  /** Full URL of the request */
  url: string

  /** HTTP method */
  method: string

  /** Request headers (sanitized) */
  headers: Record<string, string>

  /** Cloudflare-specific request metadata */
  cf?: {
    /** Cloudflare data center (colo) */
    colo?: string
    /** Country code */
    country?: string
    /** City name */
    city?: string
    /** ASN */
    asn?: number
    /** AS Organization */
    asOrganization?: string
  }
}

/**
 * Event information from the trace
 */
export interface TailEventInfo {
  /** Request details (for fetch events) */
  request?: TailRequest

  /** Scheduled event time (for cron triggers) */
  scheduledTime?: number

  /** Queue name (for queue consumers) */
  queue?: string
}

/**
 * A single trace item from a producer Worker execution
 */
export interface TraceItem {
  /** Name of the producer Worker script */
  scriptName: string | null

  /** Execution outcome: "ok", "exception", "exceededCpu", "exceededMemory", "unknown" */
  outcome: string

  /** Unix timestamp when the event occurred */
  eventTimestamp: number | null

  /** Event information (request, scheduled, queue, etc.) */
  event: TailEventInfo | null

  /** Array of log entries */
  logs: TailLog[]

  /** Array of exceptions */
  exceptions: TailException[]

  /** Diagnostics channel events */
  diagnosticsChannelEvents: unknown[]
}

/**
 * Filter configuration for tail events
 */
export interface TailEventFilter {
  /** Only include events from these script names */
  scriptNames?: string[]

  /** Only include events with these outcomes */
  outcomes?: ('ok' | 'exception' | 'exceededCpu' | 'exceededMemory' | 'unknown')[]

  /** Only include events with logs at these levels */
  logLevels?: ('log' | 'debug' | 'info' | 'warn' | 'error')[]

  /** Only include events with exceptions */
  exceptionsOnly?: boolean

  /** Minimum number of logs to include */
  minLogs?: number

  /** URL patterns to include (glob-style) */
  urlPatterns?: string[]
}

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
  method?: string

  /** Request URL (if applicable) */
  url?: string

  /** Cloudflare colo */
  colo?: string

  /** Country code */
  country?: string

  /** Log count */
  logCount: number

  /** Exception count */
  exceptionCount: number

  /** Error messages (if any) */
  errors?: string[]

  /** Log messages (filtered) */
  logs?: Array<{ level: string; message: string; timestamp: number }>

  /** Duration estimate (if available) */
  durationMs?: number
}

// =============================================================================
// Default Configuration
// =============================================================================

/**
 * Default filter configuration
 */
export const DEFAULT_FILTER: TailEventFilter = {
  outcomes: ['ok', 'exception', 'exceededCpu', 'exceededMemory', 'unknown'],
  logLevels: ['log', 'info', 'warn', 'error'],
}

/**
 * Default batch configuration
 */
export const DEFAULT_BATCH_CONFIG: BatchConfig = {
  maxEvents: 100,
  maxWaitMs: 10000,
  minEvents: 1,
}

// =============================================================================
// Event Filtering
// =============================================================================

/**
 * Check if a URL matches a glob pattern
 */
function matchUrlPattern(url: string, pattern: string): boolean {
  // Convert glob pattern to regex
  const regexPattern = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&') // Escape special regex chars
    .replace(/\*/g, '.*') // Convert * to .*
    .replace(/\?/g, '.') // Convert ? to .

  return new RegExp(`^${regexPattern}$`).test(url)
}

/**
 * Apply filters to a trace item
 *
 * @param item - The trace item to filter
 * @param filter - Filter configuration
 * @returns true if the item should be included
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
    if (!filter.outcomes.includes(item.outcome as typeof filter.outcomes[number])) {
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
      filter.logLevels!.includes(log.level as typeof filter.logLevels![number])
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
 *
 * @param items - Array of trace items
 * @param filter - Filter configuration
 * @returns Filtered array
 */
export function filterTraceItems(items: TraceItem[], filter: TailEventFilter): TraceItem[] {
  return items.filter((item) => filterTraceItem(item, filter))
}

// =============================================================================
// Event Processing
// =============================================================================

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
 *
 * @param item - Raw trace item
 * @returns Processed event
 */
export function processTraceItem(item: TraceItem): ProcessedEvent {
  const event: ProcessedEvent = {
    id: generateEventId(),
    timestamp: item.eventTimestamp
      ? new Date(item.eventTimestamp).toISOString()
      : new Date().toISOString(),
    scriptName: item.scriptName ?? 'unknown',
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
 *
 * @param items - Array of trace items
 * @returns Array of processed events
 */
export function processTraceItems(items: TraceItem[]): ProcessedEvent[] {
  return items.map(processTraceItem)
}

// =============================================================================
// Batching
// =============================================================================

/**
 * Batch state for accumulating events
 */
export interface BatchState {
  events: ProcessedEvent[]
  startTime: number
  lastFlush: number
}

/**
 * Create a new batch state
 */
export function createBatchState(): BatchState {
  const now = Date.now()
  return {
    events: [],
    startTime: now,
    lastFlush: now,
  }
}

/**
 * Check if a batch should be flushed
 *
 * @param state - Current batch state
 * @param config - Batch configuration
 * @returns true if batch should be flushed
 */
export function shouldFlushBatch(state: BatchState, config: BatchConfig): boolean {
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
 *
 * @param state - Current batch state
 * @param events - New events to add
 * @param config - Batch configuration
 * @returns Events to flush (empty if not ready)
 */
export function addToBatch(
  state: BatchState,
  events: ProcessedEvent[],
  config: BatchConfig
): ProcessedEvent[] {
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
// Storage
// =============================================================================

/**
 * Store processed events in R2
 *
 * @param bucket - R2 bucket
 * @param events - Events to store
 * @returns Storage key
 */
export async function storeEventsInR2(
  bucket: R2Bucket,
  events: ProcessedEvent[]
): Promise<string> {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const key = `logs/${timestamp}.json`

  await bucket.put(key, JSON.stringify(events, null, 2), {
    httpMetadata: {
      contentType: 'application/json',
    },
    customMetadata: {
      eventCount: String(events.length),
      createdAt: new Date().toISOString(),
    },
  })

  return key
}

/**
 * Write metrics to Analytics Engine
 *
 * @param analytics - Analytics Engine binding
 * @param events - Events to record
 */
export async function writeToAnalytics(
  analytics: AnalyticsEngineDataset,
  events: ProcessedEvent[]
): Promise<void> {
  for (const event of events) {
    analytics.writeDataPoint({
      blobs: [
        event.scriptName,
        event.outcome,
        event.method ?? 'unknown',
        event.country ?? 'unknown',
      ],
      doubles: [
        1, // count
        event.logCount,
        event.exceptionCount,
      ],
      indexes: [event.colo ?? 'unknown'],
    })
  }
}

/**
 * Send alert for critical events
 *
 * @param webhookUrl - Webhook URL
 * @param events - Events that triggered alert
 */
export async function sendAlert(webhookUrl: string, events: ProcessedEvent[]): Promise<void> {
  const criticalEvents = events.filter(
    (e) => e.outcome !== 'ok' || e.exceptionCount > 0
  )

  if (criticalEvents.length === 0) {
    return
  }

  await fetch(webhookUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      text: `ParqueDB Alert: ${criticalEvents.length} critical event(s)`,
      events: criticalEvents.slice(0, 10), // Limit to first 10
    }),
  })
}

// =============================================================================
// Tail Worker Handler
// =============================================================================

/**
 * Tail Worker configuration
 */
export interface TailWorkerConfig {
  /** Event filter */
  filter?: TailEventFilter

  /** Batch configuration */
  batch?: BatchConfig

  /** Enable R2 storage */
  enableR2Storage?: boolean

  /** Enable Analytics Engine */
  enableAnalytics?: boolean

  /** Enable alerting */
  enableAlerts?: boolean

  /** Input validation configuration */
  validation?: TailValidationConfig

  /** Callback for handling validation errors */
  onValidationError?: (result: TraceItemsValidationResult) => void
}

/**
 * Default tail worker configuration
 */
export const DEFAULT_TAIL_CONFIG: TailWorkerConfig = {
  filter: DEFAULT_FILTER,
  batch: DEFAULT_BATCH_CONFIG,
  enableR2Storage: true,
  enableAnalytics: true,
  enableAlerts: true,
}

/**
 * Create a tail handler with custom configuration
 *
 * Input validation is performed on all incoming events. Invalid events are
 * skipped by default, allowing partial processing of batches with some
 * malformed items.
 *
 * @param config - Tail worker configuration
 * @returns Tail handler function
 *
 * @example
 * ```typescript
 * const handler = createTailHandler({
 *   validation: {
 *     skipInvalidItems: true,
 *     maxItems: 1000,
 *   },
 *   onValidationError: (result) => {
 *     console.warn(`Skipped ${result.invalidCount} invalid items`)
 *   }
 * })
 * ```
 */
export function createTailHandler(config: TailWorkerConfig = DEFAULT_TAIL_CONFIG) {
  const filter = config.filter || DEFAULT_FILTER
  const batchConfig = config.batch || DEFAULT_BATCH_CONFIG
  const validationConfig = config.validation || {}

  // Note: In a real implementation, batch state would be managed externally
  // (e.g., in Durable Object) since Workers are stateless
  let batchState = createBatchState()

  return async function tail(events: unknown, env: TailWorkerEnv): Promise<void> {
    // Validate input - this will gracefully handle non-array input
    const validationResult = validateTraceItems(events, validationConfig)

    // Notify caller of validation errors if callback provided
    if (validationResult.invalidCount > 0 && config.onValidationError) {
      config.onValidationError(validationResult)
    }

    // If no valid items, return early
    if (validationResult.validCount === 0) {
      return
    }

    // Convert validated items to TraceItem format for filtering
    const traceItems: TraceItem[] = validationResult.validItems.map((item) => ({
      scriptName: item.scriptName,
      outcome: item.outcome,
      eventTimestamp: item.eventTimestamp,
      event: item.event as TailEventInfo | null,
      logs: item.logs as TailLog[],
      exceptions: item.exceptions as TailException[],
      diagnosticsChannelEvents: item.diagnosticsChannelEvents,
    }))

    // Filter events
    const filteredEvents = filterTraceItems(traceItems, filter)

    if (filteredEvents.length === 0) {
      return
    }

    // Process events
    const processedEvents = processTraceItems(filteredEvents)

    // Add to batch and check if we should flush
    const toFlush = addToBatch(batchState, processedEvents, batchConfig)

    if (toFlush.length === 0) {
      return
    }

    // Store in R2
    if (config.enableR2Storage && env.LOGS_BUCKET) {
      await storeEventsInR2(env.LOGS_BUCKET, toFlush)
    }

    // Write to Analytics Engine
    if (config.enableAnalytics && env.ANALYTICS) {
      await writeToAnalytics(env.ANALYTICS, toFlush)
    }

    // Send alerts
    if (config.enableAlerts && env.ALERT_WEBHOOK_URL) {
      await sendAlert(env.ALERT_WEBHOOK_URL, toFlush)
    }
  }
}

// =============================================================================
// Tail Worker Export
// =============================================================================

/**
 * ParqueDB Tail Worker
 *
 * Default export for use as a Cloudflare Worker.
 *
 * @example wrangler.toml configuration:
 * ```toml
 * name = "parquedb-tail"
 * main = "src/worker/tail.ts"
 *
 * [[tail_consumers]]
 * service = "parquedb-tail"
 *
 * [r2_buckets]
 * [[r2_buckets]]
 * binding = "LOGS_BUCKET"
 * bucket_name = "parquedb-logs"
 *
 * [analytics_engine_datasets]
 * [[analytics_engine_datasets]]
 * binding = "ANALYTICS"
 * dataset = "parquedb_metrics"
 * ```
 */
export default {
  /**
   * Tail handler - processes events from producer Workers
   *
   * Input is validated at runtime. Invalid events are skipped gracefully,
   * allowing partial processing of batches with malformed items.
   *
   * @param events - Array of trace items from producer Workers (validated at runtime)
   * @param env - Environment bindings
   */
  async tail(events: unknown, env: TailWorkerEnv): Promise<void> {
    const handler = createTailHandler(DEFAULT_TAIL_CONFIG)
    await handler(events, env)
  },
}

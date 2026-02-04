/**
 * Streaming Refresh Engine for Materialized Views
 *
 * Processes CDC events incrementally to keep materialized views up-to-date.
 * Provides:
 * - Event buffering and batching for efficiency
 * - Backpressure handling to prevent memory exhaustion
 * - Error isolation (one MV failure doesn't affect others)
 * - Statistics and monitoring
 *
 * @example
 * ```typescript
 * const engine = createStreamingRefreshEngine({
 *   batchSize: 100,
 *   batchTimeoutMs: 500,
 * })
 *
 * engine.registerMV({
 *   name: 'OrderAnalytics',
 *   sourceNamespaces: ['orders', 'products'],
 *   async process(events) {
 *     // Update MV based on events
 *   }
 * })
 *
 * await engine.start()
 *
 * // Events from CDC log
 * await engine.processEvent(event)
 * ```
 */

import type { Event, EventOp } from '../types/entity'
import { parseEntityTarget, isRelationshipTarget, parseRelTarget } from '../types/entity'
import {
  DEFAULT_STREAMING_BATCH_SIZE,
  DEFAULT_STREAMING_BATCH_TIMEOUT_MS,
  DEFAULT_STREAMING_MAX_BUFFER_SIZE,
  DEFAULT_MAX_RETRIES,
  DEFAULT_RETRY_BASE_DELAY,
  DEFAULT_RETRY_MAX_DELAY,
} from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for the streaming refresh engine
 */
export interface StreamingRefreshConfig {
  /** Number of events to batch before processing (default: 100) */
  batchSize?: number | undefined
  /** Maximum time to wait before processing a partial batch in ms (default: 500) */
  batchTimeoutMs?: number | undefined
  /** Maximum buffer size before applying backpressure (default: 1000) */
  maxBufferSize?: number | undefined
  /** Retry configuration for failed batches */
  retry?: {
    /** Maximum retry attempts (default: 3) */
    maxAttempts?: number | undefined
    /** Base delay between retries in ms (default: 100) */
    baseDelayMs?: number | undefined
    /** Maximum delay between retries in ms (default: 5000) */
    maxDelayMs?: number | undefined
  } | undefined
}

/**
 * Handler for a materialized view that processes events
 */
export interface MVHandler {
  /** Unique name for this MV */
  name: string
  /** Source namespaces this MV tracks */
  sourceNamespaces: string[]
  /** Process a batch of events */
  process(events: Event[]): Promise<void>
}

/**
 * Statistics about the streaming refresh engine
 */
export interface StreamingStats {
  /** Total events received */
  eventsReceived: number
  /** Total events successfully processed */
  eventsProcessed: number
  /** Total batches processed */
  batchesProcessed: number
  /** Total failed batches */
  failedBatches: number
  /** Events by operation type */
  eventsByOp: Record<EventOp, number>
  /** Events by namespace */
  eventsByNamespace: Record<string, number>
  /** Events by MV */
  eventsByMV: Record<string, number>
  /** Number of times backpressure was applied */
  backpressureEvents: number
  /** Current buffer size */
  currentBufferSize: number
  /** Average batch processing time in ms */
  avgBatchProcessingMs: number
  /** Engine start time */
  startedAt: number | null
  /** Last event processed time */
  lastEventAt: number | null
}

/**
 * Error handler callback type
 */
export type ErrorHandler = (error: Error, context?: { mvName?: string | undefined; batch?: Event[] | undefined }) => void

/**
 * Warning handler callback type for capacity/eviction warnings
 */
export type WarningHandler = (message: string, context?: Record<string, unknown>) => void

/**
 * Default configuration values
 */
const DEFAULT_CONFIG: Required<StreamingRefreshConfig> = {
  batchSize: DEFAULT_STREAMING_BATCH_SIZE,
  batchTimeoutMs: DEFAULT_STREAMING_BATCH_TIMEOUT_MS,
  maxBufferSize: DEFAULT_STREAMING_MAX_BUFFER_SIZE,
  retry: {
    maxAttempts: DEFAULT_MAX_RETRIES,
    baseDelayMs: DEFAULT_RETRY_BASE_DELAY,
    maxDelayMs: DEFAULT_RETRY_MAX_DELAY,
  },
}

// =============================================================================
// StreamingRefreshEngine Class
// =============================================================================

/**
 * Engine for streaming refresh of materialized views.
 *
 * Buffers incoming events and processes them in batches for efficiency.
 * Implements backpressure when the buffer fills up to prevent memory issues.
 */
export class StreamingRefreshEngine {
  private config: Required<StreamingRefreshConfig>
  private mvHandlers = new Map<string, MVHandler>()
  private namespaceToMVs = new Map<string, Set<string>>()

  private buffer: Event[] = []
  private bufferByMV = new Map<string, Event[]>()
  private running = false
  private batchTimer: ReturnType<typeof setTimeout> | null = null
  private processingPromise: Promise<void> | null = null
  private _flushing = false // Mutex flag to prevent concurrent flush operations
  private _warningEmitted80 = false // Track if 80% warning already emitted

  private errorHandlers: ErrorHandler[] = []
  private warningHandlers: WarningHandler[] = []

  // Stats
  private stats: StreamingStats = this.createEmptyStats()
  private batchProcessingTimes: number[] = []

  constructor(config: StreamingRefreshConfig = {}) {
    this.config = {
      ...DEFAULT_CONFIG,
      ...config,
      retry: {
        ...DEFAULT_CONFIG.retry,
        ...config.retry,
      },
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Register a materialized view handler
   */
  registerMV(handler: MVHandler): void {
    this.mvHandlers.set(handler.name, handler)
    this.bufferByMV.set(handler.name, [])

    // Index namespaces for fast routing
    for (const ns of handler.sourceNamespaces) {
      let mvs = this.namespaceToMVs.get(ns)
      if (!mvs) {
        mvs = new Set()
        this.namespaceToMVs.set(ns, mvs)
      }
      mvs.add(handler.name)
    }
  }

  /**
   * Unregister a materialized view handler
   */
  unregisterMV(name: string): void {
    const handler = this.mvHandlers.get(name)
    if (!handler) return

    // Remove from namespace index
    for (const ns of handler.sourceNamespaces) {
      const mvs = this.namespaceToMVs.get(ns)
      if (mvs) {
        mvs.delete(name)
        if (mvs.size === 0) {
          this.namespaceToMVs.delete(ns)
        }
      }
    }

    this.mvHandlers.delete(name)
    this.bufferByMV.delete(name)
  }

  /**
   * Get list of registered MV names
   */
  getRegisteredMVs(): string[] {
    return Array.from(this.mvHandlers.keys())
  }

  /**
   * Start the streaming refresh engine
   */
  async start(): Promise<void> {
    if (this.running) return

    this.running = true
    this.stats.startedAt = Date.now()
    this.startBatchTimer()
  }

  /**
   * Stop the streaming refresh engine
   */
  async stop(): Promise<void> {
    if (!this.running) return

    this.running = false
    this.stopBatchTimer()

    // Flush any remaining events
    await this.flush()

    // Wait for any in-flight processing
    if (this.processingPromise) {
      await this.processingPromise
    }
  }

  /**
   * Check if the engine is running
   */
  isRunning(): boolean {
    return this.running
  }

  /**
   * Check if a flush operation is currently in progress
   */
  isFlushing(): boolean {
    return this._flushing
  }

  /**
   * Process a single event
   */
  async processEvent(event: Event): Promise<void> {
    if (!this.running) {
      throw new Error('StreamingRefreshEngine is not running')
    }

    // Update stats
    this.stats.eventsReceived++
    this.stats.lastEventAt = Date.now()
    this.updateOpStats(event.op)

    // Get namespace from event target
    const namespace = this.getNamespaceFromTarget(event.target)
    if (namespace) {
      this.stats.eventsByNamespace[namespace] =
        (this.stats.eventsByNamespace[namespace] || 0) + 1
    }

    // Route event to relevant MVs
    const relevantMVs = this.getRelevantMVs(event.target)
    if (relevantMVs.length === 0) {
      // No MVs interested in this event
      return
    }

    // Apply backpressure if buffer is full
    await this.applyBackpressure()

    // Add to per-MV buffers
    for (const mvName of relevantMVs) {
      const buffer = this.bufferByMV.get(mvName)
      if (buffer) {
        buffer.push(event)
        this.stats.eventsByMV[mvName] = (this.stats.eventsByMV[mvName] || 0) + 1
      }
    }

    this.buffer.push(event)
    this.stats.currentBufferSize = this.buffer.length

    // Emit warning at 80% buffer capacity (only once until reset)
    const maxBufferSize = this.config.maxBufferSize ?? DEFAULT_STREAMING_MAX_BUFFER_SIZE
    const threshold = Math.floor(maxBufferSize * 0.8)
    if (this.buffer.length >= threshold && !this._warningEmitted80) {
      this._warningEmitted80 = true
      this.emitWarning('Buffer reached 80% capacity', {
        bufferSize: this.buffer.length,
        maxBufferSize: this.config.maxBufferSize,
      })
    }

    // Check if we should flush (batch size reached)
    await this.maybeFlush()
  }

  /**
   * Process multiple events
   */
  async processEvents(events: Event[]): Promise<void> {
    for (const event of events) {
      await this.processEvent(event)
    }
  }

  /**
   * Flush all buffered events immediately
   *
   * Uses the mutex flag to coordinate with maybeFlush() and prevent
   * concurrent flush operations.
   */
  async flush(): Promise<void> {
    // Wait for any in-flight flushing operation to complete
    while (this._flushing) {
      if (this.processingPromise) {
        await this.processingPromise
      } else {
        // Brief yield if flushing but no promise yet
        await new Promise(resolve => setTimeout(resolve, 1))
      }
    }

    // Acquire flush lock
    this._flushing = true
    try {
      // Wait for any in-flight processing
      if (this.processingPromise) {
        await this.processingPromise
      }

      // Process any remaining buffered events
      if (this.buffer.length > 0) {
        this.processingPromise = this.processBatches()
        await this.processingPromise
        this.processingPromise = null
      }

      // Reset the 80% warning flag after successful flush
      this._warningEmitted80 = false
    } finally {
      this._flushing = false
    }
  }

  /**
   * Register an error handler
   * @returns Unsubscribe function to remove this specific listener
   */
  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.push(handler)
    let unsubscribed = false
    return () => {
      if (unsubscribed) return // Idempotent
      unsubscribed = true
      const index = this.errorHandlers.indexOf(handler)
      if (index !== -1) {
        this.errorHandlers.splice(index, 1)
      }
    }
  }

  /**
   * Register a warning handler for capacity/eviction warnings
   * @returns Unsubscribe function to remove this specific listener
   */
  onWarning(handler: WarningHandler): () => void {
    this.warningHandlers.push(handler)
    let unsubscribed = false
    return () => {
      if (unsubscribed) return // Idempotent
      unsubscribed = true
      const index = this.warningHandlers.indexOf(handler)
      if (index !== -1) {
        this.warningHandlers.splice(index, 1)
      }
    }
  }

  /**
   * Remove all error listeners
   */
  removeAllErrorListeners(): void {
    this.errorHandlers = []
  }

  /**
   * Remove all warning listeners
   */
  removeAllWarningListeners(): void {
    this.warningHandlers = []
  }

  /**
   * Dispose the engine and release all resources
   *
   * Clears all:
   * - Registered MVs
   * - Error listeners
   * - Warning listeners
   * - Buffers
   * - Statistics
   *
   * Can be called multiple times safely.
   * Engine can be re-used after dispose by registering new MVs and calling start().
   */
  async dispose(): Promise<void> {
    // Stop the engine if running
    await this.stop()

    // Clear all MVs
    this.mvHandlers.clear()
    this.namespaceToMVs.clear()
    this.bufferByMV.clear()

    // Clear listeners
    this.errorHandlers = []
    this.warningHandlers = []

    // Clear buffers (already done by stop() -> flush(), but be explicit)
    this.buffer = []

    // Reset statistics
    this.stats = this.createEmptyStats()
    this.batchProcessingTimes = []

    // Reset warning flag
    this._warningEmitted80 = false
  }

  /**
   * Get current statistics
   */
  getStats(): StreamingStats {
    // Calculate average batch processing time
    if (this.batchProcessingTimes.length > 0) {
      const sum = this.batchProcessingTimes.reduce((a, b) => a + b, 0)
      this.stats.avgBatchProcessingMs = sum / this.batchProcessingTimes.length
    }

    return { ...this.stats }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = this.createEmptyStats()
    this.stats.startedAt = this.running ? Date.now() : null
    this.batchProcessingTimes = []
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Create empty stats object
   */
  private createEmptyStats(): StreamingStats {
    return {
      eventsReceived: 0,
      eventsProcessed: 0,
      batchesProcessed: 0,
      failedBatches: 0,
      eventsByOp: {
        CREATE: 0,
        UPDATE: 0,
        DELETE: 0,
        REL_CREATE: 0,
        REL_DELETE: 0,
      },
      eventsByNamespace: {},
      eventsByMV: {},
      backpressureEvents: 0,
      currentBufferSize: 0,
      avgBatchProcessingMs: 0,
      startedAt: null,
      lastEventAt: null,
    }
  }

  /**
   * Update operation-specific stats
   */
  private updateOpStats(op: EventOp): void {
    this.stats.eventsByOp[op] = (this.stats.eventsByOp[op] || 0) + 1
  }

  /**
   * Get namespace from event target
   */
  private getNamespaceFromTarget(target: string): string | null {
    if (isRelationshipTarget(target)) {
      const parsed = parseRelTarget(target)
      // Return the "from" namespace for relationships
      const colonIdx = parsed.from.indexOf(':')
      return colonIdx !== -1 ? parsed.from.slice(0, colonIdx) : null
    } else {
      const parsed = parseEntityTarget(target)
      return parsed.ns
    }
  }

  /**
   * Get MVs that should receive this event
   */
  private getRelevantMVs(target: string): string[] {
    const namespace = this.getNamespaceFromTarget(target)
    if (!namespace) return []

    const mvs = this.namespaceToMVs.get(namespace)
    return mvs ? Array.from(mvs) : []
  }

  /**
   * Apply backpressure if buffer is full
   */
  private async applyBackpressure(): Promise<void> {
    const maxBufferSize = this.config.maxBufferSize ?? DEFAULT_STREAMING_MAX_BUFFER_SIZE
    while (this.buffer.length >= maxBufferSize && this.running) {
      this.stats.backpressureEvents++
      // Wait for processing to complete
      if (this.processingPromise) {
        await this.processingPromise
      } else {
        // Force flush
        this.processingPromise = this.processBatches()
        await this.processingPromise
        this.processingPromise = null
      }
    }
  }

  /**
   * Check if we should flush based on batch size
   *
   * Uses a mutex flag (_flushing) to prevent race conditions where multiple
   * concurrent calls could start duplicate flush operations between the
   * check and the assignment of processingPromise.
   */
  private async maybeFlush(): Promise<void> {
    // Early exit if already flushing (mutex check)
    if (this._flushing) {
      return
    }

    // Check if any MV buffer has reached batch size
    let shouldFlush = false
    const batchSize = this.config.batchSize ?? DEFAULT_STREAMING_BATCH_SIZE
    for (const [_mvName, buffer] of this.bufferByMV) {
      if (buffer.length >= batchSize) {
        shouldFlush = true
        break
      }
    }

    if (!shouldFlush) {
      return
    }

    // Acquire the flush lock before checking processingPromise
    // This prevents the TOCTOU race condition
    this._flushing = true
    try {
      // Wait for any existing processing to complete
      if (this.processingPromise) {
        await this.processingPromise
      }

      // Re-check if we still need to flush (buffer may have been cleared)
      let stillNeedsFlush = false
      for (const [_mvName, buffer] of this.bufferByMV) {
        if (buffer.length > 0) {
          stillNeedsFlush = true
          break
        }
      }

      if (stillNeedsFlush) {
        this.processingPromise = this.processBatches()
        await this.processingPromise
        this.processingPromise = null
      }
    } finally {
      this._flushing = false
    }
  }

  /**
   * Start the batch timeout timer
   *
   * Uses the same mutex flag (_flushing) to prevent race conditions
   * with concurrent flush operations from maybeFlush().
   */
  private startBatchTimer(): void {
    if (this.batchTimer) return

    this.batchTimer = setInterval(() => {
      // Skip if already flushing or no events to process
      if (this._flushing || this.buffer.length === 0 || this.processingPromise) {
        return
      }

      // Acquire flush lock
      this._flushing = true
      this.processingPromise = this.processBatches()
      this.processingPromise.finally(() => {
        this.processingPromise = null
        this._flushing = false
      })
    }, this.config.batchTimeoutMs)
  }

  /**
   * Stop the batch timeout timer
   */
  private stopBatchTimer(): void {
    if (this.batchTimer) {
      clearInterval(this.batchTimer)
      this.batchTimer = null
    }
  }

  /**
   * Process all buffered batches
   */
  private async processBatches(): Promise<void> {
    // Snapshot and clear buffers
    const buffersToProcess = new Map<string, Event[]>()
    for (const [mvName, buffer] of this.bufferByMV) {
      if (buffer.length > 0) {
        buffersToProcess.set(mvName, [...buffer])
        buffer.length = 0 // Clear the buffer
      }
    }
    this.buffer = []
    this.stats.currentBufferSize = 0

    // Process each MV's events
    const promises: Promise<void>[] = []
    for (const [mvName, events] of buffersToProcess) {
      const handler = this.mvHandlers.get(mvName)
      if (!handler) continue

      promises.push(this.processMVBatch(handler, events))
    }

    await Promise.all(promises)
  }

  /**
   * Process a batch for a specific MV with retry logic
   */
  private async processMVBatch(handler: MVHandler, events: Event[]): Promise<void> {
    const startTime = Date.now()
    let lastError: Error | null = null

    const maxAttempts = this.config.retry?.maxAttempts ?? DEFAULT_MAX_RETRIES
    const baseDelayMs = this.config.retry?.baseDelayMs ?? DEFAULT_RETRY_BASE_DELAY
    const maxDelayMs = this.config.retry?.maxDelayMs ?? DEFAULT_RETRY_MAX_DELAY

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        await handler.process(events)

        // Success
        this.stats.eventsProcessed += events.length
        this.stats.batchesProcessed++
        this.batchProcessingTimes.push(Date.now() - startTime)

        // Keep only last 100 timing samples
        if (this.batchProcessingTimes.length > 100) {
          this.batchProcessingTimes.shift()
        }

        return
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err))

        // Exponential backoff
        if (attempt < maxAttempts - 1) {
          const delay = Math.min(
            baseDelayMs * Math.pow(2, attempt),
            maxDelayMs
          )
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }

    // All retries failed
    this.stats.failedBatches++
    this.emitError(lastError!, { mvName: handler.name, batch: events })
  }

  /**
   * Emit an error to registered handlers
   */
  private emitError(error: Error, context?: { mvName?: string | undefined; batch?: Event[] | undefined }): void {
    for (const handler of this.errorHandlers) {
      try {
        handler(error, context)
      } catch {
        // Ignore errors in error handlers
      }
    }
  }

  /**
   * Emit a warning to registered handlers
   */
  private emitWarning(message: string, context?: Record<string, unknown>): void {
    for (const handler of this.warningHandlers) {
      try {
        handler(message, context)
      } catch {
        // Ignore errors in warning handlers
      }
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new StreamingRefreshEngine instance
 */
export function createStreamingRefreshEngine(
  config?: StreamingRefreshConfig
): StreamingRefreshEngine {
  return new StreamingRefreshEngine(config)
}

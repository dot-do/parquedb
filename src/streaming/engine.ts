/**
 * Streaming Refresh Engine
 *
 * Processes events in real-time and routes them to registered
 * materialized views (MVs) based on namespace subscriptions.
 *
 * Features:
 * - Batching for efficient processing
 * - Backpressure handling
 * - Error recovery
 * - Statistics tracking
 */

import type { Event, EventOp } from '../types/entity'
import type { MVHandler, StreamingRefreshConfig, StreamingStats } from './types'

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG: Required<StreamingRefreshConfig> = {
  batchSize: 10,
  batchTimeoutMs: 100,
  maxBufferSize: 100,
}

// =============================================================================
// Streaming Refresh Engine
// =============================================================================

/**
 * Engine for streaming event processing to materialized views
 */
export class StreamingRefreshEngine {
  private readonly config: Required<StreamingRefreshConfig>
  private readonly handlers: Map<string, MVHandler> = new Map()
  private readonly namespaceToHandlers: Map<string, Set<string>> = new Map()

  private buffer: Event[] = []
  private running = false
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private processing = false
  private errorListeners: Array<(err: Error) => void> = []

  private stats: StreamingStats = {
    eventsReceived: 0,
    eventsProcessed: 0,
    batchesProcessed: 0,
    failedBatches: 0,
    backpressureEvents: 0,
    eventsByOp: {},
  }

  constructor(config: StreamingRefreshConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config }
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Start the streaming engine
   */
  async start(): Promise<void> {
    if (this.running) return
    this.running = true
    this.scheduleFlush()
  }

  /**
   * Stop the streaming engine, flushing pending events
   */
  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = null
    }

    // Flush any remaining events
    await this.flush()
  }

  /**
   * Check if the engine is running
   */
  isRunning(): boolean {
    return this.running
  }

  // ===========================================================================
  // MV Registration
  // ===========================================================================

  /**
   * Register a materialized view handler
   */
  registerMV(handler: MVHandler): void {
    this.handlers.set(handler.name, handler)

    // Index by namespace for routing
    for (const ns of handler.sourceNamespaces) {
      let handlers = this.namespaceToHandlers.get(ns)
      if (!handlers) {
        handlers = new Set()
        this.namespaceToHandlers.set(ns, handlers)
      }
      handlers.add(handler.name)
    }
  }

  /**
   * Unregister a materialized view handler
   */
  unregisterMV(name: string): void {
    const handler = this.handlers.get(name)
    if (!handler) return

    this.handlers.delete(name)

    // Remove from namespace index
    for (const ns of handler.sourceNamespaces) {
      const handlers = this.namespaceToHandlers.get(ns)
      if (handlers) {
        handlers.delete(name)
        if (handlers.size === 0) {
          this.namespaceToHandlers.delete(ns)
        }
      }
    }
  }

  /**
   * Get list of registered MV names
   */
  getRegisteredMVs(): string[] {
    return Array.from(this.handlers.keys())
  }

  // ===========================================================================
  // Event Processing
  // ===========================================================================

  /**
   * Process a single event
   */
  async processEvent(event: Event): Promise<void> {
    if (!this.running) {
      throw new Error('StreamingRefreshEngine is not running')
    }

    // Apply backpressure if buffer is full
    while (this.buffer.length >= this.config.maxBufferSize) {
      this.stats.backpressureEvents++
      await this.flush()
      // Small delay to allow processing
      await new Promise(resolve => setTimeout(resolve, 10))
    }

    this.buffer.push(event)
    this.stats.eventsReceived++

    // Track by operation type
    this.stats.eventsByOp[event.op] = (this.stats.eventsByOp[event.op] || 0) + 1

    // Flush if batch is full
    if (this.buffer.length >= this.config.batchSize) {
      await this.flushBuffer()
    }
  }

  /**
   * Flush all pending events
   */
  async flush(): Promise<void> {
    if (this.buffer.length > 0) {
      await this.flushBuffer()
    }
  }

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  /**
   * Register an error listener
   */
  onError(listener: (err: Error) => void): void {
    this.errorListeners.push(listener)
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get current statistics
   */
  getStats(): StreamingStats {
    return { ...this.stats }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      eventsReceived: 0,
      eventsProcessed: 0,
      batchesProcessed: 0,
      failedBatches: 0,
      backpressureEvents: 0,
      eventsByOp: {},
    }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private scheduleFlush(): void {
    if (!this.running) return

    this.flushTimer = setTimeout(async () => {
      await this.flushBuffer()
      this.scheduleFlush()
    }, this.config.batchTimeoutMs)
  }

  private async flushBuffer(): Promise<void> {
    if (this.buffer.length === 0 || this.processing) return

    this.processing = true
    const events = this.buffer
    this.buffer = []

    try {
      // Group events by namespace for routing
      const eventsByNamespace = this.groupEventsByNamespace(events)

      // Process each namespace's events through its handlers
      for (const [ns, nsEvents] of eventsByNamespace) {
        const handlerNames = this.namespaceToHandlers.get(ns)
        if (!handlerNames) continue

        for (const handlerName of handlerNames) {
          const handler = this.handlers.get(handlerName)
          if (!handler) continue

          try {
            await handler.process(nsEvents)
            this.stats.eventsProcessed += nsEvents.length
          } catch (err) {
            this.stats.failedBatches++
            this.emitError(err instanceof Error ? err : new Error(String(err)))
          }
        }
      }

      this.stats.batchesProcessed++
    } finally {
      this.processing = false
    }
  }

  private groupEventsByNamespace(events: Event[]): Map<string, Event[]> {
    const grouped = new Map<string, Event[]>()

    for (const event of events) {
      // Extract namespace from target (format: "ns:id" or "ns:id:pred:ns:id")
      const ns = event.target.split(':')[0]
      if (!ns) continue

      let nsEvents = grouped.get(ns)
      if (!nsEvents) {
        nsEvents = []
        grouped.set(ns, nsEvents)
      }
      nsEvents.push(event)
    }

    return grouped
  }

  private emitError(err: Error): void {
    for (const listener of this.errorListeners) {
      try {
        listener(err)
      } catch {
        // Ignore listener errors
      }
    }
  }
}

/**
 * Create a new streaming refresh engine
 */
export function createStreamingRefreshEngine(
  config?: StreamingRefreshConfig
): StreamingRefreshEngine {
  return new StreamingRefreshEngine(config)
}

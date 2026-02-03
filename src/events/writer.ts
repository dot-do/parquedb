/**
 * Event Writer - Buffers events in memory before flushing
 *
 * The EventWriter accumulates events and flushes them to storage when
 * thresholds are reached (count, bytes, time). Flush handlers determine
 * where the batch goes (SQLite blob, R2 segment, etc.).
 */

import type { Event } from '../types/entity'
import type { EventBatch, EventWriterConfig } from './types'
import { DEFAULT_WRITER_CONFIG } from './types'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Handler called when a batch is ready to be flushed
 */
export type FlushHandler = (batch: EventBatch) => Promise<void>

/**
 * Statistics about the writer's current state
 */
export interface WriterStats {
  /** Events currently buffered */
  bufferedEvents: number
  /** Estimated bytes in buffer */
  bufferedBytes: number
  /** Total events written since creation */
  totalEventsWritten: number
  /** Total flushes since creation */
  totalFlushes: number
  /** Time of last flush (ms since epoch) */
  lastFlushAt: number | null
  /** Whether a flush is currently in progress */
  flushInProgress: boolean
}

// =============================================================================
// EventWriter Class
// =============================================================================

/**
 * Buffers events in memory and flushes to storage based on configurable thresholds.
 *
 * @example
 * ```typescript
 * const writer = new EventWriter({
 *   maxBufferSize: 1000,
 *   flushIntervalMs: 5000,
 * })
 *
 * writer.onFlush(async (batch) => {
 *   await sqliteWal.writeBatch(batch)
 * })
 *
 * await writer.write(event)
 * await writer.flush() // explicit flush
 * ```
 */
export class EventWriter {
  private buffer: Event[] = []
  private bufferBytes = 0
  private minTs: number | null = null
  private maxTs: number | null = null

  private config: Required<EventWriterConfig>
  private flushHandlers: FlushHandler[] = []
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private flushPromise: Promise<void> | null = null

  private totalEventsWritten = 0
  private totalFlushes = 0
  private lastFlushAt: number | null = null

  constructor(config: EventWriterConfig = {}) {
    this.config = { ...DEFAULT_WRITER_CONFIG, ...config }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Register a flush handler. Multiple handlers can be registered.
   * All handlers are called in parallel when a batch is flushed.
   */
  onFlush(handler: FlushHandler): void {
    this.flushHandlers.push(handler)
  }

  /**
   * Remove a flush handler
   */
  offFlush(handler: FlushHandler): void {
    const idx = this.flushHandlers.indexOf(handler)
    if (idx !== -1) {
      this.flushHandlers.splice(idx, 1)
    }
  }

  /**
   * Write a single event to the buffer.
   * May trigger a flush if thresholds are reached.
   */
  async write(event: Event): Promise<void> {
    this.addToBuffer(event)
    await this.maybeFlush()
  }

  /**
   * Write multiple events to the buffer.
   * May trigger a flush if thresholds are reached.
   */
  async writeMany(events: Event[]): Promise<void> {
    for (const event of events) {
      this.addToBuffer(event)
    }
    await this.maybeFlush()
  }

  /**
   * Explicitly flush the buffer, regardless of thresholds.
   * Returns immediately if buffer is empty.
   */
  async flush(): Promise<void> {
    // If a flush is already in progress, wait for it
    if (this.flushPromise) {
      await this.flushPromise
    }

    if (this.buffer.length === 0) {
      return
    }

    this.flushPromise = this.doFlush()
    try {
      await this.flushPromise
    } finally {
      this.flushPromise = null
    }
  }

  /**
   * Get current writer statistics
   */
  getStats(): WriterStats {
    return {
      bufferedEvents: this.buffer.length,
      bufferedBytes: this.bufferBytes,
      totalEventsWritten: this.totalEventsWritten,
      totalFlushes: this.totalFlushes,
      lastFlushAt: this.lastFlushAt,
      flushInProgress: this.flushPromise !== null,
    }
  }

  /**
   * Get the current buffer contents without flushing.
   * Returns a copy of the buffer.
   */
  getBuffer(): Event[] {
    return [...this.buffer]
  }

  /**
   * Check if the buffer has any events
   */
  hasEvents(): boolean {
    return this.buffer.length > 0
  }

  /**
   * Start the periodic flush timer
   *
   * NOTE: The timer callback is fire-and-forget by design - errors during
   * periodic flushes are logged but don't propagate. This prevents a single
   * failed flush from stopping all future flushes. Critical data loss is
   * prevented by the explicit flush() call in close().
   */
  startTimer(): void {
    if (this.flushTimer) {
      return
    }

    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0 && !this.flushPromise) {
        // Wrap in promise chain to properly handle async errors
        // Errors are logged but don't stop the timer - next interval will retry
        this.flush().catch((err) => {
          logger.error('[EventWriter] Periodic flush failed:', err)
          // Periodic flush errors are logged but don't stop the timer.
          // The next interval will attempt another flush, providing
          // automatic retry behavior. For more sophisticated error handling,
          // consider adding an error callback to the config.
        })
      }
    }, this.config.flushIntervalMs)
  }

  /**
   * Stop the periodic flush timer
   */
  stopTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Close the writer - flush remaining events and stop timer
   */
  async close(): Promise<void> {
    this.stopTimer()
    await this.flush()
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Add an event to the buffer and update tracking
   */
  private addToBuffer(event: Event): void {
    this.buffer.push(event)

    // Estimate size (rough approximation)
    const eventSize = this.estimateEventSize(event)
    this.bufferBytes += eventSize

    // Track timestamps
    if (this.minTs === null || event.ts < this.minTs) {
      this.minTs = event.ts
    }
    if (this.maxTs === null || event.ts > this.maxTs) {
      this.maxTs = event.ts
    }
  }

  /**
   * Estimate the serialized size of an event
   */
  private estimateEventSize(event: Event): number {
    // Rough estimate: JSON stringify length * 1.1 for msgpack overhead
    // This is conservative - msgpack is usually smaller than JSON
    let size = 50 // base overhead for event structure

    size += event.id.length
    size += event.target.length
    size += event.op.length

    if (event.before) {
      size += JSON.stringify(event.before).length
    }
    if (event.after) {
      size += JSON.stringify(event.after).length
    }
    if (event.actor) {
      size += event.actor.length
    }
    if (event.metadata) {
      size += JSON.stringify(event.metadata).length
    }

    return Math.ceil(size * 1.1)
  }

  /**
   * Check thresholds and flush if needed
   */
  private async maybeFlush(): Promise<void> {
    const shouldFlush =
      this.buffer.length >= this.config.maxBufferSize ||
      this.bufferBytes >= this.config.maxBufferBytes

    if (shouldFlush) {
      await this.flush()
    }
  }

  /**
   * Actually perform the flush
   */
  private async doFlush(): Promise<void> {
    if (this.buffer.length === 0) {
      return
    }

    // Create the batch
    const batch: EventBatch = {
      events: this.buffer,
      minTs: this.minTs!,
      maxTs: this.maxTs!,
      count: this.buffer.length,
      sizeBytes: this.bufferBytes,
    }

    // Call all flush handlers in parallel
    if (this.flushHandlers.length > 0) {
      await Promise.all(this.flushHandlers.map(handler => handler(batch)))
    }

    // Update stats
    this.totalEventsWritten += this.buffer.length
    this.totalFlushes++
    this.lastFlushAt = Date.now()

    // Reset buffer
    this.buffer = []
    this.bufferBytes = 0
    this.minTs = null
    this.maxTs = null
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an EventWriter with default configuration
 */
export function createEventWriter(config?: EventWriterConfig): EventWriter {
  return new EventWriter(config)
}

/**
 * Create an EventWriter that automatically starts the periodic flush timer
 */
export function createTimedEventWriter(config?: EventWriterConfig): EventWriter {
  const writer = new EventWriter(config)
  writer.startTimer()
  return writer
}

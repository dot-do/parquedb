/**
 * Event Source Implementations
 *
 * Provides ways to connect the subscription manager to database events.
 */

import type { Event } from '../types/entity'
import type { SubscriptionEventSource } from './types'
import { DEFAULT_EVENT_SOURCE_BUFFER_SIZE, DEFAULT_POLLING_INTERVAL_MS } from '../constants'

// =============================================================================
// In-Memory Event Source (for testing)
// =============================================================================

/**
 * Simple in-memory event source for testing
 *
 * Events are pushed manually via the emit() method.
 *
 * @example
 * ```typescript
 * const source = new InMemoryEventSource()
 * manager.setEventSource(source)
 *
 * // Emit events
 * source.emit({
 *   id: 'evt1',
 *   ts: Date.now(),
 *   op: 'CREATE',
 *   target: 'posts:post1',
 *   after: { title: 'Hello' },
 * })
 * ```
 */
export class InMemoryEventSource implements SubscriptionEventSource {
  private handlers: Set<(event: Event) => void> = new Set()
  private started = false

  onEvent(handler: (event: Event) => void): () => void {
    this.handlers.add(handler)
    return () => {
      this.handlers.delete(handler)
    }
  }

  async start(): Promise<void> {
    this.started = true
  }

  async stop(): Promise<void> {
    this.started = false
  }

  /**
   * Emit an event to all registered handlers
   */
  emit(event: Event): void {
    if (!this.started) return

    for (const handler of this.handlers) {
      try {
        handler(event)
      } catch (error) {
        console.error('[InMemoryEventSource] Handler error:', error)
      }
    }
  }

  /**
   * Emit multiple events
   */
  emitMany(events: Event[]): void {
    for (const event of events) {
      this.emit(event)
    }
  }

  /**
   * Check if started
   */
  isStarted(): boolean {
    return this.started
  }
}

// =============================================================================
// EventWriter Integration
// =============================================================================

/**
 * Options for EventWriterSource
 */
export interface EventWriterSourceOptions {
  /** Maximum events to buffer while not started */
  maxBufferSize?: number
  /** Replay buffered events when starting */
  replayOnStart?: boolean
}

/**
 * Event source that integrates with EventWriter flush handlers
 *
 * Use this to connect the subscription manager to the database's
 * event writer system.
 *
 * @example
 * ```typescript
 * import { EventWriter } from '../events/writer'
 *
 * const writer = new EventWriter()
 * const source = new EventWriterSource()
 *
 * // Connect to event writer
 * writer.onFlush(async (batch) => {
 *   for (const event of batch.events) {
 *     source.handleEvent(event)
 *   }
 * })
 *
 * // Connect to subscription manager
 * await manager.setEventSource(source)
 * ```
 */
export class EventWriterSource implements SubscriptionEventSource {
  private handlers: Set<(event: Event) => void> = new Set()
  private started = false
  private buffer: Event[] = []
  private options: Required<EventWriterSourceOptions>

  constructor(options: EventWriterSourceOptions = {}) {
    this.options = {
      maxBufferSize: options.maxBufferSize ?? DEFAULT_EVENT_SOURCE_BUFFER_SIZE,
      replayOnStart: options.replayOnStart ?? false,
    }
  }

  onEvent(handler: (event: Event) => void): () => void {
    this.handlers.add(handler)
    return () => {
      this.handlers.delete(handler)
    }
  }

  async start(): Promise<void> {
    this.started = true

    // Replay buffered events if configured
    if (this.options.replayOnStart && this.buffer.length > 0) {
      const events = this.buffer
      this.buffer = []

      for (const event of events) {
        this.dispatchEvent(event)
      }
    } else {
      this.buffer = []
    }
  }

  async stop(): Promise<void> {
    this.started = false
  }

  /**
   * Handle an event from the EventWriter
   *
   * Call this from the EventWriter's flush handler.
   */
  handleEvent(event: Event): void {
    if (this.started) {
      this.dispatchEvent(event)
    } else {
      // Buffer while not started
      if (this.buffer.length < this.options.maxBufferSize) {
        this.buffer.push(event)
      }
    }
  }

  /**
   * Handle multiple events
   */
  handleEvents(events: Event[]): void {
    for (const event of events) {
      this.handleEvent(event)
    }
  }

  private dispatchEvent(event: Event): void {
    for (const handler of this.handlers) {
      try {
        handler(event)
      } catch (error) {
        console.error('[EventWriterSource] Handler error:', error)
      }
    }
  }

  /**
   * Get current buffer size
   */
  getBufferSize(): number {
    return this.buffer.length
  }

  /**
   * Check if started
   */
  isStarted(): boolean {
    return this.started
  }
}

// =============================================================================
// Polling Event Source (for Node.js)
// =============================================================================

/**
 * Options for PollingEventSource
 */
export interface PollingEventSourceOptions {
  /** Polling interval in ms (default: 1000) */
  intervalMs?: number
  /** Last event ID to start from */
  startAfter?: string
}

/**
 * Function to fetch events from a data source
 */
export type EventFetcher = (lastEventId?: string) => Promise<Event[]>

/**
 * Event source that polls for new events
 *
 * Useful for Node.js environments where there's no push mechanism.
 *
 * @example
 * ```typescript
 * const source = new PollingEventSource(async (lastEventId) => {
 *   // Fetch events from database
 *   const events = await db.events.find({
 *     id: lastEventId ? { $gt: lastEventId } : undefined
 *   })
 *   return events
 * }, { intervalMs: 500 })
 *
 * await manager.setEventSource(source)
 * ```
 */
export class PollingEventSource implements SubscriptionEventSource {
  private handlers: Set<(event: Event) => void> = new Set()
  private fetcher: EventFetcher
  private intervalMs: number
  private lastEventId?: string
  private timer?: ReturnType<typeof setInterval>
  private started = false

  constructor(fetcher: EventFetcher, options: PollingEventSourceOptions = {}) {
    this.fetcher = fetcher
    this.intervalMs = options.intervalMs ?? DEFAULT_POLLING_INTERVAL_MS
    this.lastEventId = options.startAfter
  }

  onEvent(handler: (event: Event) => void): () => void {
    this.handlers.add(handler)
    return () => {
      this.handlers.delete(handler)
    }
  }

  async start(): Promise<void> {
    if (this.started) return

    this.started = true
    this.timer = setInterval(() => {
      this.poll().catch((err) => {
        console.error('[PollingEventSource] Unexpected poll error:', err)
      })
    }, this.intervalMs)

    // Initial poll
    await this.poll()
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = undefined
    }
    this.started = false
  }

  private async poll(): Promise<void> {
    if (!this.started) return

    try {
      const events = await this.fetcher(this.lastEventId)

      for (const event of events) {
        // Update last event ID
        if (!this.lastEventId || event.id > this.lastEventId) {
          this.lastEventId = event.id
        }

        // Dispatch to handlers
        for (const handler of this.handlers) {
          try {
            handler(event)
          } catch (error) {
            console.error('[PollingEventSource] Handler error:', error)
          }
        }
      }
    } catch (error) {
      console.error('[PollingEventSource] Poll error:', error)
    }
  }

  /**
   * Get the last processed event ID
   */
  getLastEventId(): string | undefined {
    return this.lastEventId
  }

  /**
   * Check if started
   */
  isStarted(): boolean {
    return this.started
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an in-memory event source
 */
export function createInMemoryEventSource(): InMemoryEventSource {
  return new InMemoryEventSource()
}

/**
 * Create an event writer source
 */
export function createEventWriterSource(
  options?: EventWriterSourceOptions
): EventWriterSource {
  return new EventWriterSource(options)
}

/**
 * Create a polling event source
 */
export function createPollingEventSource(
  fetcher: EventFetcher,
  options?: PollingEventSourceOptions
): PollingEventSource {
  return new PollingEventSource(fetcher, options)
}

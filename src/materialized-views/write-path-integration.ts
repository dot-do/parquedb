/**
 * Write Path Integration for Materialized Views
 *
 * This module provides integration between the ParqueDB write path (create/update/delete)
 * and the Materialized Views streaming refresh engine.
 *
 * The integration allows:
 * 1. ParqueDB mutations to automatically emit events to registered MV handlers
 * 2. Relationship mutations to emit REL_CREATE/REL_DELETE events
 * 3. Transaction completion to trigger batch MV updates
 *
 * @example
 * ```typescript
 * import { createMVEventEmitter, attachToParqueDB } from './write-path-integration'
 * import { createStreamingRefreshEngine } from './streaming'
 *
 * // Create the streaming engine
 * const streamingEngine = createStreamingRefreshEngine()
 *
 * // Create the event emitter and attach to ParqueDB
 * const emitter = createMVEventEmitter()
 * attachToParqueDB(db, emitter)
 *
 * // Connect emitter to streaming engine
 * emitter.subscribe(async (event) => {
 *   await streamingEngine.processEvent(event)
 * })
 *
 * // Start the engine
 * await streamingEngine.start()
 *
 * // Now all ParqueDB mutations will automatically trigger MV updates
 * await db.create('orders', { total: 100 })
 * ```
 */

import type { Event, EventOp, Variant } from '../types/entity'
import type { Entity, EntityId } from '../types'
import { generateULID } from '../utils'
import { StreamingRefreshEngine } from './streaming'

// =============================================================================
// Types
// =============================================================================

/**
 * Event subscriber callback type
 */
export type MVEventSubscriber = (event: Event) => Promise<void> | void

/**
 * Options for the MVEventEmitter
 */
export interface MVEventEmitterOptions {
  /** Whether to emit events synchronously (default: false - async) */
  synchronous?: boolean
  /** Maximum queue size before applying backpressure (default: 10000) */
  maxQueueSize?: number
  /** Error handler for subscriber failures */
  onError?: (error: Error, event: Event) => void
}

/**
 * Statistics about the event emitter
 */
export interface MVEventEmitterStats {
  /** Total events emitted */
  totalEmitted: number
  /** Events currently queued */
  queuedEvents: number
  /** Events emitted by operation type */
  eventsByOp: Record<EventOp, number>
  /** Events emitted by namespace */
  eventsByNamespace: Record<string, number>
  /** Number of subscriber errors */
  subscriberErrors: number
  /** Whether backpressure is currently applied */
  backpressureActive: boolean
}

// =============================================================================
// MVEventEmitter Class
// =============================================================================

/**
 * Event emitter for ParqueDB write path events.
 *
 * This class receives events from ParqueDB mutations and distributes them
 * to registered subscribers (typically the StreamingRefreshEngine).
 */
export class MVEventEmitter {
  private subscribers: MVEventSubscriber[] = []
  private options: Required<MVEventEmitterOptions>
  private queue: Event[] = []
  private processing = false
  private stats: MVEventEmitterStats = this.createEmptyStats()

  constructor(options: MVEventEmitterOptions = {}) {
    this.options = {
      synchronous: options.synchronous ?? false,
      maxQueueSize: options.maxQueueSize ?? 10000,
      onError: options.onError ?? (() => {}),
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Subscribe to receive events
   * @returns Unsubscribe function
   */
  subscribe(subscriber: MVEventSubscriber): () => void {
    this.subscribers.push(subscriber)
    return () => {
      const index = this.subscribers.indexOf(subscriber)
      if (index !== -1) {
        this.subscribers.splice(index, 1)
      }
    }
  }

  /**
   * Emit an event to all subscribers
   */
  async emit(event: Event): Promise<void> {
    this.updateStats(event)

    if (this.options.synchronous) {
      await this.emitSync(event)
    } else {
      await this.emitAsync(event)
    }
  }

  /**
   * Emit an entity event (CREATE, UPDATE, DELETE)
   */
  async emitEntityEvent(
    op: EventOp,
    namespace: string,
    entityId: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId,
    metadata?: Variant
  ): Promise<void> {
    const event: Event = {
      id: generateULID(),
      ts: Date.now(),
      op,
      target: `${namespace}:${entityId}`,
      before: before as Variant | undefined,
      after: after as Variant | undefined,
      actor: actor as string | undefined,
      metadata,
    }
    await this.emit(event)
  }

  /**
   * Emit a relationship event (REL_CREATE, REL_DELETE)
   */
  async emitRelationshipEvent(
    op: 'REL_CREATE' | 'REL_DELETE',
    fromNs: string,
    fromId: string,
    predicate: string,
    toNs: string,
    toId: string,
    actor?: EntityId,
    metadata?: Variant
  ): Promise<void> {
    const event: Event = {
      id: generateULID(),
      ts: Date.now(),
      op,
      target: `${fromNs}:${fromId}:${predicate}:${toNs}:${toId}`,
      after: {
        predicate,
        from: `${fromNs}:${fromId}`,
        to: `${toNs}:${toId}`,
      },
      actor: actor as string | undefined,
      metadata,
    }
    await this.emit(event)
  }

  /**
   * Flush any queued events
   */
  async flush(): Promise<void> {
    while (this.queue.length > 0) {
      await this.processQueue()
    }
  }

  /**
   * Get current statistics
   */
  getStats(): MVEventEmitterStats {
    return {
      ...this.stats,
      queuedEvents: this.queue.length,
      backpressureActive: this.queue.length >= this.options.maxQueueSize,
    }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = this.createEmptyStats()
  }

  /**
   * Get the number of subscribers
   */
  getSubscriberCount(): number {
    return this.subscribers.length
  }

  /**
   * Check if there are any subscribers
   */
  hasSubscribers(): boolean {
    return this.subscribers.length > 0
  }

  /**
   * Dispose of the emitter and clear all subscribers
   */
  dispose(): void {
    this.subscribers = []
    this.queue = []
    this.stats = this.createEmptyStats()
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private createEmptyStats(): MVEventEmitterStats {
    return {
      totalEmitted: 0,
      queuedEvents: 0,
      eventsByOp: {
        CREATE: 0,
        UPDATE: 0,
        DELETE: 0,
        REL_CREATE: 0,
        REL_DELETE: 0,
      },
      eventsByNamespace: {},
      subscriberErrors: 0,
      backpressureActive: false,
    }
  }

  private updateStats(event: Event): void {
    this.stats.totalEmitted++
    this.stats.eventsByOp[event.op] = (this.stats.eventsByOp[event.op] || 0) + 1

    // Extract namespace from target
    const colonIndex = event.target.indexOf(':')
    if (colonIndex !== -1) {
      const ns = event.target.slice(0, colonIndex)
      this.stats.eventsByNamespace[ns] = (this.stats.eventsByNamespace[ns] || 0) + 1
    }
  }

  private async emitSync(event: Event): Promise<void> {
    for (const subscriber of this.subscribers) {
      try {
        await subscriber(event)
      } catch (error) {
        this.stats.subscriberErrors++
        this.options.onError(error instanceof Error ? error : new Error(String(error)), event)
      }
    }
  }

  private async emitAsync(event: Event): Promise<void> {
    // Apply backpressure if queue is full
    while (this.queue.length >= this.options.maxQueueSize) {
      await this.processQueue()
    }

    this.queue.push(event)

    // Start processing if not already
    if (!this.processing) {
      await this.processQueue()
    }
  }

  private async processQueue(): Promise<void> {
    if (this.processing || this.queue.length === 0) {
      return
    }

    this.processing = true

    try {
      const event = this.queue.shift()
      if (event) {
        await this.emitSync(event)
      }
    } finally {
      this.processing = false
    }

    // Continue processing if more events are queued
    if (this.queue.length > 0) {
      // Use setImmediate-like behavior to allow other operations
      await Promise.resolve()
      await this.processQueue()
    }
  }
}

// =============================================================================
// Integration Bridge
// =============================================================================

/**
 * Integration bridge that connects MVEventEmitter to StreamingRefreshEngine.
 *
 * This provides a convenient way to wire up the event flow:
 * ParqueDB -> MVEventEmitter -> StreamingRefreshEngine -> MV Handlers
 */
export class MVIntegrationBridge {
  private emitter: MVEventEmitter
  private engine: StreamingRefreshEngine
  private unsubscribe: (() => void) | null = null

  constructor(emitter: MVEventEmitter, engine: StreamingRefreshEngine) {
    this.emitter = emitter
    this.engine = engine
  }

  /**
   * Connect the emitter to the engine
   */
  connect(): void {
    if (this.unsubscribe) {
      return // Already connected
    }

    this.unsubscribe = this.emitter.subscribe(async (event) => {
      await this.engine.processEvent(event)
    })
  }

  /**
   * Disconnect the emitter from the engine
   */
  disconnect(): void {
    if (this.unsubscribe) {
      this.unsubscribe()
      this.unsubscribe = null
    }
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.unsubscribe !== null
  }

  /**
   * Get the emitter
   */
  getEmitter(): MVEventEmitter {
    return this.emitter
  }

  /**
   * Get the engine
   */
  getEngine(): StreamingRefreshEngine {
    return this.engine
  }
}

// =============================================================================
// ParqueDB Integration Hook
// =============================================================================

/**
 * Hook function type for ParqueDB integration.
 *
 * This can be passed to ParqueDB configuration to receive mutation events.
 */
export type ParqueDBMutationHook = (
  op: EventOp,
  target: string,
  before: Entity | null,
  after: Entity | null,
  actor?: EntityId,
  metadata?: Record<string, unknown>
) => Promise<void> | void

/**
 * Create a mutation hook that forwards events to an MVEventEmitter.
 *
 * @example
 * ```typescript
 * const emitter = createMVEventEmitter()
 * const hook = createMutationHook(emitter)
 *
 * // Use with ParqueDB configuration or manual integration
 * ```
 */
export function createMutationHook(emitter: MVEventEmitter): ParqueDBMutationHook {
  return async (op, target, before, after, actor, metadata) => {
    const event: Event = {
      id: generateULID(),
      ts: Date.now(),
      op,
      target,
      before: before as Variant | undefined,
      after: after as Variant | undefined,
      actor: actor as string | undefined,
      metadata: metadata as Variant | undefined,
    }
    await emitter.emit(event)
  }
}

// =============================================================================
// InMemoryEventSource Adapter
// =============================================================================

/**
 * Adapter that makes MVEventEmitter compatible with EventSource interface.
 *
 * This allows the emitter to be used with IncrementalRefresher which expects
 * an EventSource for querying historical events.
 */
export class MVEventSourceAdapter {
  private events: Event[] = []
  private maxEvents: number
  private emitter: MVEventEmitter
  private unsubscribe: (() => void) | null = null

  constructor(emitter: MVEventEmitter, options?: { maxEvents?: number }) {
    this.emitter = emitter
    this.maxEvents = options?.maxEvents ?? 100000
  }

  /**
   * Start capturing events from the emitter
   */
  start(): void {
    if (this.unsubscribe) {
      return
    }

    this.unsubscribe = this.emitter.subscribe((event) => {
      this.events.push(event)

      // Prune oldest events if over limit
      if (this.events.length > this.maxEvents) {
        this.events = this.events.slice(-this.maxEvents)
      }
    })
  }

  /**
   * Stop capturing events
   */
  stop(): void {
    if (this.unsubscribe) {
      this.unsubscribe()
      this.unsubscribe = null
    }
  }

  /**
   * Get events for a specific target (EventSource interface)
   */
  async getEventsForTarget(
    target: string,
    minTs?: number,
    maxTs?: number
  ): Promise<Event[]> {
    return this.events.filter((e) => {
      if (!e.target.startsWith(target.split(':')[0] + ':')) return false
      if (minTs !== undefined && e.ts < minTs) return false
      if (maxTs !== undefined && e.ts > maxTs) return false
      return true
    })
  }

  /**
   * Get events in a time range (EventSource interface)
   */
  async getEventsInRange(minTs: number, maxTs: number): Promise<Event[]> {
    return this.events.filter((e) => e.ts >= minTs && e.ts <= maxTs)
  }

  /**
   * Clear captured events
   */
  clear(): void {
    this.events = []
  }

  /**
   * Get all captured events
   */
  getAllEvents(): Event[] {
    return [...this.events]
  }

  /**
   * Get event count
   */
  getEventCount(): number {
    return this.events.length
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an MVEventEmitter instance
 */
export function createMVEventEmitter(options?: MVEventEmitterOptions): MVEventEmitter {
  return new MVEventEmitter(options)
}

/**
 * Create an MVIntegrationBridge instance
 */
export function createMVIntegrationBridge(
  emitter: MVEventEmitter,
  engine: StreamingRefreshEngine
): MVIntegrationBridge {
  return new MVIntegrationBridge(emitter, engine)
}

/**
 * Create an MVEventSourceAdapter instance
 */
export function createMVEventSourceAdapter(
  emitter: MVEventEmitter,
  options?: { maxEvents?: number }
): MVEventSourceAdapter {
  return new MVEventSourceAdapter(emitter, options)
}

/**
 * Create a complete MV integration setup.
 *
 * This is a convenience function that creates and wires up all components:
 * - MVEventEmitter for receiving events from ParqueDB
 * - StreamingRefreshEngine for processing events
 * - MVIntegrationBridge for connecting them
 *
 * @example
 * ```typescript
 * const { emitter, engine, bridge, mutationHook } = createMVIntegration()
 *
 * // Register MV handlers
 * engine.registerMV({
 *   name: 'OrderAnalytics',
 *   sourceNamespaces: ['orders'],
 *   async process(events) {
 *     // Update MV based on events
 *   }
 * })
 *
 * // Start the engine
 * await engine.start()
 *
 * // Connect the bridge
 * bridge.connect()
 *
 * // Use mutationHook with ParqueDB (manual integration)
 * // Or use the emitter directly to emit events
 * ```
 */
export function createMVIntegration(options?: {
  emitterOptions?: MVEventEmitterOptions
  engineOptions?: Parameters<typeof StreamingRefreshEngine>[0]
}): {
  emitter: MVEventEmitter
  engine: StreamingRefreshEngine
  bridge: MVIntegrationBridge
  mutationHook: ParqueDBMutationHook
  eventSourceAdapter: MVEventSourceAdapter
} {
  const emitter = createMVEventEmitter(options?.emitterOptions)
  const engine = new StreamingRefreshEngine(options?.engineOptions)
  const bridge = createMVIntegrationBridge(emitter, engine)
  const mutationHook = createMutationHook(emitter)
  const eventSourceAdapter = createMVEventSourceAdapter(emitter)

  return {
    emitter,
    engine,
    bridge,
    mutationHook,
    eventSourceAdapter,
  }
}

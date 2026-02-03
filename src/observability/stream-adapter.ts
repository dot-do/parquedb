/**
 * Stream Adapters for Local AI Observability
 *
 * Provides EventEmitter, async iterator, and polling adapters for streaming
 * data processing in Node.js environments. Enables local AI analytics from
 * AI SDK logs, evalite traces, and generated content.
 *
 * @module observability/stream-adapter
 */

import { EventEmitter } from 'events'
import type {
  ObservabilityHook,
  QueryContext,
  MutationContext,
  StorageContext,
  QueryResult,
  MutationResult,
  StorageResult,
} from './hooks'

// =============================================================================
// Stream Event Types
// =============================================================================

/**
 * Base event for all stream events
 */
export interface StreamEvent {
  /** Unique event ID */
  id: string
  /** Timestamp when event occurred */
  ts: number
  /** Event category */
  category: 'query' | 'mutation' | 'storage'
  /** Event type within category */
  type: 'start' | 'end' | 'error'
}

/**
 * Query stream event
 */
export interface QueryStreamEvent extends StreamEvent {
  category: 'query'
  context: QueryContext
  result?: QueryResult | undefined
  error?: Error | undefined
}

/**
 * Mutation stream event
 */
export interface MutationStreamEvent extends StreamEvent {
  category: 'mutation'
  context: MutationContext
  result?: MutationResult | undefined
  error?: Error | undefined
}

/**
 * Storage stream event
 */
export interface StorageStreamEvent extends StreamEvent {
  category: 'storage'
  context: StorageContext
  result?: StorageResult | undefined
  error?: Error | undefined
}

/**
 * Union type for all stream events
 */
export type ObservabilityStreamEvent =
  | QueryStreamEvent
  | MutationStreamEvent
  | StorageStreamEvent

// =============================================================================
// Stream Adapter Configuration
// =============================================================================

/**
 * Configuration for stream adapters
 */
export interface StreamAdapterConfig {
  /** Maximum events to buffer (default: 10000) */
  maxBufferSize?: number | undefined
  /** Whether to include full context in events (default: true) */
  includeFullContext?: boolean | undefined
  /** Filter events by category */
  categories?: Array<'query' | 'mutation' | 'storage'> | undefined
  /** Filter events by type */
  types?: Array<'start' | 'end' | 'error'> | undefined
  /** Custom filter function */
  filter?: ((event: ObservabilityStreamEvent) => boolean) | undefined
  /** Batch size for batch processing (default: 100) */
  batchSize?: number | undefined
  /** Batch timeout in ms (default: 1000) */
  batchTimeoutMs?: number | undefined
}

/**
 * Default stream adapter configuration
 */
export const DEFAULT_STREAM_ADAPTER_CONFIG: Required<StreamAdapterConfig> = {
  maxBufferSize: 10000,
  includeFullContext: true,
  categories: ['query', 'mutation', 'storage'],
  types: ['start', 'end', 'error'],
  filter: () => true,
  batchSize: 100,
  batchTimeoutMs: 1000,
}

// =============================================================================
// Stream Adapter Interface
// =============================================================================

/**
 * Interface for stream adapters
 */
export interface StreamAdapter {
  /** Get the observability hook for registration */
  getHook(): ObservabilityHook
  /** Close the adapter and clean up resources */
  close(): void
  /** Check if the adapter is active */
  isActive(): boolean
  /** Get current buffer size */
  getBufferSize(): number
  /** Get total events processed */
  getEventsProcessed(): number
}

// =============================================================================
// EventEmitter Adapter
// =============================================================================

/**
 * Events emitted by the EventEmitter adapter
 */
export interface StreamAdapterEvents {
  event: [ObservabilityStreamEvent]
  query: [QueryStreamEvent]
  mutation: [MutationStreamEvent]
  storage: [StorageStreamEvent]
  batch: [ObservabilityStreamEvent[]]
  error: [Error]
  overflow: [{ dropped: number; bufferSize: number }]
}

/**
 * Type-safe event emitter for stream events
 */
export class StreamEventEmitter extends EventEmitter {
  override emit<K extends keyof StreamAdapterEvents>(
    event: K,
    ...args: StreamAdapterEvents[K]
  ): boolean {
    return super.emit(event, ...args)
  }

  override on<K extends keyof StreamAdapterEvents>(
    event: K,
    listener: (...args: StreamAdapterEvents[K]) => void
  ): this {
    return super.on(event, listener)
  }

  override once<K extends keyof StreamAdapterEvents>(
    event: K,
    listener: (...args: StreamAdapterEvents[K]) => void
  ): this {
    return super.once(event, listener)
  }

  override off<K extends keyof StreamAdapterEvents>(
    event: K,
    listener: (...args: StreamAdapterEvents[K]) => void
  ): this {
    return super.off(event, listener)
  }
}

/**
 * EventEmitter-based stream adapter
 *
 * Emits events as they occur for real-time processing.
 *
 * @example
 * ```typescript
 * const adapter = createStreamAdapter({ type: 'emitter' })
 * const { emitter, hook, close } = adapter
 *
 * emitter.on('event', (event) => {
 *   console.log('Event:', event)
 * })
 *
 * emitter.on('query', (event) => {
 *   console.log('Query event:', event)
 * })
 *
 * globalHookRegistry.registerHook(hook)
 *
 * // When done
 * close()
 * ```
 */
export class EventEmitterAdapter implements StreamAdapter {
  private emitter: StreamEventEmitter
  private config: Required<StreamAdapterConfig>
  private active: boolean = true
  private eventCounter: number = 0
  private buffer: ObservabilityStreamEvent[] = []
  private batchTimer?: ReturnType<typeof setTimeout>

  constructor(config: StreamAdapterConfig = {}) {
    this.config = { ...DEFAULT_STREAM_ADAPTER_CONFIG, ...config }
    this.emitter = new StreamEventEmitter()
  }

  /**
   * Get the event emitter instance
   */
  getEmitter(): StreamEventEmitter {
    return this.emitter
  }

  getHook(): ObservabilityHook {
    return {
      onQueryStart: (context) => this.handleQueryStart(context),
      onQueryEnd: (context, result) => this.handleQueryEnd(context, result),
      onQueryError: (context, error) => this.handleQueryError(context, error),
      onMutationStart: (context) => this.handleMutationStart(context),
      onMutationEnd: (context, result) => this.handleMutationEnd(context, result),
      onMutationError: (context, error) => this.handleMutationError(context, error),
      onRead: (context, result) => this.handleStorageRead(context, result),
      onWrite: (context, result) => this.handleStorageWrite(context, result),
      onDelete: (context, result) => this.handleStorageDelete(context, result),
      onStorageError: (context, error) => this.handleStorageError(context, error),
    }
  }

  close(): void {
    this.active = false
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = undefined
    }
    // Flush remaining buffer
    if (this.buffer.length > 0) {
      this.emitter.emit('batch', [...this.buffer])
      this.buffer = []
    }
    this.emitter.removeAllListeners()
  }

  isActive(): boolean {
    return this.active
  }

  getBufferSize(): number {
    return this.buffer.length
  }

  getEventsProcessed(): number {
    return this.eventCounter
  }

  private generateEventId(): string {
    const timestamp = Date.now().toString(36)
    const random = Math.random().toString(36).substring(2, 8)
    return `${timestamp}-${random}`
  }

  private shouldEmit(event: ObservabilityStreamEvent): boolean {
    if (!this.active) return false
    if (!this.config.categories.includes(event.category)) return false
    if (!this.config.types.includes(event.type)) return false
    if (!this.config.filter(event)) return false
    return true
  }

  private emitEvent(event: ObservabilityStreamEvent): void {
    if (!this.shouldEmit(event)) return

    this.eventCounter++

    // Check buffer overflow
    if (this.buffer.length >= this.config.maxBufferSize) {
      const dropped = this.buffer.length - this.config.maxBufferSize + 1
      this.buffer = this.buffer.slice(dropped)
      this.emitter.emit('overflow', { dropped, bufferSize: this.config.maxBufferSize })
    }

    this.buffer.push(event)

    // Emit individual event
    this.emitter.emit('event', event)

    // Emit category-specific event
    switch (event.category) {
      case 'query':
        this.emitter.emit('query', event as QueryStreamEvent)
        break
      case 'mutation':
        this.emitter.emit('mutation', event as MutationStreamEvent)
        break
      case 'storage':
        this.emitter.emit('storage', event as StorageStreamEvent)
        break
    }

    // Check if batch is ready
    if (this.buffer.length >= this.config.batchSize) {
      this.flushBatch()
    } else if (!this.batchTimer) {
      // Start batch timer
      this.batchTimer = setTimeout(() => {
        this.flushBatch()
      }, this.config.batchTimeoutMs)
    }
  }

  private flushBatch(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = undefined
    }

    if (this.buffer.length > 0) {
      this.emitter.emit('batch', [...this.buffer])
      this.buffer = []
    }
  }

  private handleQueryStart(context: QueryContext): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.emitEvent(event)
  }

  private handleQueryEnd(context: QueryContext, result: QueryResult): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.emitEvent(event)
  }

  private handleQueryError(context: QueryContext, error: Error): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.emitEvent(event)
  }

  private handleMutationStart(context: MutationContext): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.emitEvent(event)
  }

  private handleMutationEnd(context: MutationContext, result: MutationResult): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.emitEvent(event)
  }

  private handleMutationError(context: MutationContext, error: Error): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.emitEvent(event)
  }

  private handleStorageRead(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.emitEvent(event)
  }

  private handleStorageWrite(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.emitEvent(event)
  }

  private handleStorageDelete(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.emitEvent(event)
  }

  private handleStorageError(context: StorageContext, error: Error): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.emitEvent(event)
  }
}

// =============================================================================
// Async Iterator Adapter
// =============================================================================

/**
 * Async iterator-based stream adapter
 *
 * Provides an async iterable for consuming events with for-await-of loops.
 *
 * @example
 * ```typescript
 * const adapter = createStreamAdapter({ type: 'iterator' })
 * const { iterator, hook, close } = adapter
 *
 * globalHookRegistry.registerHook(hook)
 *
 * // Process events as they arrive
 * for await (const event of iterator) {
 *   console.log('Event:', event)
 *   if (event.type === 'end') {
 *     // Process completed operation
 *   }
 * }
 * ```
 */
export class AsyncIteratorAdapter implements StreamAdapter {
  private config: Required<StreamAdapterConfig>
  private active: boolean = true
  private eventCounter: number = 0
  private buffer: ObservabilityStreamEvent[] = []
  private resolvers: Array<(value: IteratorResult<ObservabilityStreamEvent>) => void> = []
  private pendingPromise: Promise<IteratorResult<ObservabilityStreamEvent>> | null = null

  constructor(config: StreamAdapterConfig = {}) {
    this.config = { ...DEFAULT_STREAM_ADAPTER_CONFIG, ...config }
  }

  /**
   * Get the async iterable for consuming events
   */
  getIterator(): AsyncIterable<ObservabilityStreamEvent> {
    const self = this
    return {
      [Symbol.asyncIterator](): AsyncIterator<ObservabilityStreamEvent> {
        return {
          async next(): Promise<IteratorResult<ObservabilityStreamEvent>> {
            if (!self.active && self.buffer.length === 0) {
              return { done: true, value: undefined }
            }

            if (self.buffer.length > 0) {
              const event = self.buffer.shift()!
              return { done: false, value: event }
            }

            // Wait for next event
            return new Promise((resolve) => {
              self.resolvers.push(resolve)
            })
          },
          async return(): Promise<IteratorResult<ObservabilityStreamEvent>> {
            self.close()
            return { done: true, value: undefined }
          },
        }
      },
    }
  }

  getHook(): ObservabilityHook {
    return {
      onQueryStart: (context) => this.handleQueryStart(context),
      onQueryEnd: (context, result) => this.handleQueryEnd(context, result),
      onQueryError: (context, error) => this.handleQueryError(context, error),
      onMutationStart: (context) => this.handleMutationStart(context),
      onMutationEnd: (context, result) => this.handleMutationEnd(context, result),
      onMutationError: (context, error) => this.handleMutationError(context, error),
      onRead: (context, result) => this.handleStorageRead(context, result),
      onWrite: (context, result) => this.handleStorageWrite(context, result),
      onDelete: (context, result) => this.handleStorageDelete(context, result),
      onStorageError: (context, error) => this.handleStorageError(context, error),
    }
  }

  close(): void {
    this.active = false
    // Resolve any pending iterators with done
    for (const resolver of this.resolvers) {
      resolver({ done: true, value: undefined })
    }
    this.resolvers = []
  }

  isActive(): boolean {
    return this.active
  }

  getBufferSize(): number {
    return this.buffer.length
  }

  getEventsProcessed(): number {
    return this.eventCounter
  }

  private generateEventId(): string {
    const timestamp = Date.now().toString(36)
    const random = Math.random().toString(36).substring(2, 8)
    return `${timestamp}-${random}`
  }

  private shouldEmit(event: ObservabilityStreamEvent): boolean {
    if (!this.active) return false
    if (!this.config.categories.includes(event.category)) return false
    if (!this.config.types.includes(event.type)) return false
    if (!this.config.filter(event)) return false
    return true
  }

  private pushEvent(event: ObservabilityStreamEvent): void {
    if (!this.shouldEmit(event)) return

    this.eventCounter++

    // Check buffer overflow
    if (this.buffer.length >= this.config.maxBufferSize) {
      // Drop oldest events
      this.buffer.shift()
    }

    // If there's a waiting consumer, resolve immediately
    if (this.resolvers.length > 0) {
      const resolver = this.resolvers.shift()!
      resolver({ done: false, value: event })
    } else {
      // Otherwise buffer the event
      this.buffer.push(event)
    }
  }

  private handleQueryStart(context: QueryContext): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.pushEvent(event)
  }

  private handleQueryEnd(context: QueryContext, result: QueryResult): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleQueryError(context: QueryContext, error: Error): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }

  private handleMutationStart(context: MutationContext): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.pushEvent(event)
  }

  private handleMutationEnd(context: MutationContext, result: MutationResult): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleMutationError(context: MutationContext, error: Error): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }

  private handleStorageRead(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageWrite(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageDelete(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageError(context: StorageContext, error: Error): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }
}

// =============================================================================
// Polling Adapter
// =============================================================================

/**
 * Polling-based stream adapter configuration
 */
export interface PollingAdapterConfig extends StreamAdapterConfig {
  /** Polling interval in ms (default: 100) */
  pollingIntervalMs?: number | undefined
}

/**
 * Default polling adapter configuration
 */
export const DEFAULT_POLLING_ADAPTER_CONFIG: Required<PollingAdapterConfig> = {
  ...DEFAULT_STREAM_ADAPTER_CONFIG,
  pollingIntervalMs: 100,
}

/**
 * Polling-based stream adapter
 *
 * Buffers events and allows polling for batches. Useful for periodic processing.
 *
 * @example
 * ```typescript
 * const adapter = createStreamAdapter({ type: 'polling', pollingIntervalMs: 500 })
 * const { poll, hook, close } = adapter
 *
 * globalHookRegistry.registerHook(hook)
 *
 * // Poll for events periodically
 * setInterval(async () => {
 *   const events = poll()
 *   if (events.length > 0) {
 *     await processEvents(events)
 *   }
 * }, 500)
 * ```
 */
export class PollingAdapter implements StreamAdapter {
  private config: Required<PollingAdapterConfig>
  private active: boolean = true
  private eventCounter: number = 0
  private buffer: ObservabilityStreamEvent[] = []

  constructor(config: PollingAdapterConfig = {}) {
    this.config = { ...DEFAULT_POLLING_ADAPTER_CONFIG, ...config }
  }

  /**
   * Poll for buffered events
   *
   * Returns and clears all buffered events.
   */
  poll(): ObservabilityStreamEvent[] {
    const events = this.buffer
    this.buffer = []
    return events
  }

  /**
   * Peek at buffered events without removing them
   */
  peek(): ObservabilityStreamEvent[] {
    return [...this.buffer]
  }

  /**
   * Get a specific number of events
   */
  take(count: number): ObservabilityStreamEvent[] {
    const events = this.buffer.slice(0, count)
    this.buffer = this.buffer.slice(count)
    return events
  }

  getHook(): ObservabilityHook {
    return {
      onQueryStart: (context) => this.handleQueryStart(context),
      onQueryEnd: (context, result) => this.handleQueryEnd(context, result),
      onQueryError: (context, error) => this.handleQueryError(context, error),
      onMutationStart: (context) => this.handleMutationStart(context),
      onMutationEnd: (context, result) => this.handleMutationEnd(context, result),
      onMutationError: (context, error) => this.handleMutationError(context, error),
      onRead: (context, result) => this.handleStorageRead(context, result),
      onWrite: (context, result) => this.handleStorageWrite(context, result),
      onDelete: (context, result) => this.handleStorageDelete(context, result),
      onStorageError: (context, error) => this.handleStorageError(context, error),
    }
  }

  close(): void {
    this.active = false
    this.buffer = []
  }

  isActive(): boolean {
    return this.active
  }

  getBufferSize(): number {
    return this.buffer.length
  }

  getEventsProcessed(): number {
    return this.eventCounter
  }

  private generateEventId(): string {
    const timestamp = Date.now().toString(36)
    const random = Math.random().toString(36).substring(2, 8)
    return `${timestamp}-${random}`
  }

  private shouldEmit(event: ObservabilityStreamEvent): boolean {
    if (!this.active) return false
    if (!this.config.categories.includes(event.category)) return false
    if (!this.config.types.includes(event.type)) return false
    if (!this.config.filter(event)) return false
    return true
  }

  private pushEvent(event: ObservabilityStreamEvent): void {
    if (!this.shouldEmit(event)) return

    this.eventCounter++

    // Check buffer overflow
    if (this.buffer.length >= this.config.maxBufferSize) {
      // Drop oldest events
      this.buffer.shift()
    }

    this.buffer.push(event)
  }

  private handleQueryStart(context: QueryContext): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.pushEvent(event)
  }

  private handleQueryEnd(context: QueryContext, result: QueryResult): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleQueryError(context: QueryContext, error: Error): void {
    const event: QueryStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'query',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }

  private handleMutationStart(context: MutationContext): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'start',
      context: this.config.includeFullContext ? { ...context } : context,
    }
    this.pushEvent(event)
  }

  private handleMutationEnd(context: MutationContext, result: MutationResult): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleMutationError(context: MutationContext, error: Error): void {
    const event: MutationStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'mutation',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }

  private handleStorageRead(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageWrite(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageDelete(context: StorageContext, result: StorageResult): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'end',
      context: this.config.includeFullContext ? { ...context } : context,
      result,
    }
    this.pushEvent(event)
  }

  private handleStorageError(context: StorageContext, error: Error): void {
    const event: StorageStreamEvent = {
      id: this.generateEventId(),
      ts: Date.now(),
      category: 'storage',
      type: 'error',
      context: this.config.includeFullContext ? { ...context } : context,
      error,
    }
    this.pushEvent(event)
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Options for creating a stream adapter
 */
export type CreateStreamAdapterOptions =
  | ({ type: 'emitter' } & StreamAdapterConfig)
  | ({ type: 'iterator' } & StreamAdapterConfig)
  | ({ type: 'polling' } & PollingAdapterConfig)

/**
 * Result from creating an emitter adapter
 */
export interface EmitterAdapterResult {
  type: 'emitter'
  adapter: EventEmitterAdapter
  emitter: StreamEventEmitter
  hook: ObservabilityHook
  close: () => void
}

/**
 * Result from creating an iterator adapter
 */
export interface IteratorAdapterResult {
  type: 'iterator'
  adapter: AsyncIteratorAdapter
  iterator: AsyncIterable<ObservabilityStreamEvent>
  hook: ObservabilityHook
  close: () => void
}

/**
 * Result from creating a polling adapter
 */
export interface PollingAdapterResult {
  type: 'polling'
  adapter: PollingAdapter
  poll: () => ObservabilityStreamEvent[]
  peek: () => ObservabilityStreamEvent[]
  take: (count: number) => ObservabilityStreamEvent[]
  hook: ObservabilityHook
  close: () => void
}

/**
 * Union type for all adapter results
 */
export type StreamAdapterResult =
  | EmitterAdapterResult
  | IteratorAdapterResult
  | PollingAdapterResult

/**
 * Create a stream adapter for consuming observability events
 *
 * Supports three modes:
 * - `emitter`: EventEmitter-based for real-time event handling
 * - `iterator`: Async iterator for for-await-of consumption
 * - `polling`: Polling-based for periodic batch processing
 *
 * @example
 * ```typescript
 * // EventEmitter mode
 * const { emitter, hook, close } = createStreamAdapter({ type: 'emitter' })
 * emitter.on('event', console.log)
 *
 * // Async iterator mode
 * const { iterator, hook, close } = createStreamAdapter({ type: 'iterator' })
 * for await (const event of iterator) {
 *   console.log(event)
 * }
 *
 * // Polling mode
 * const { poll, hook, close } = createStreamAdapter({ type: 'polling' })
 * setInterval(() => {
 *   const events = poll()
 *   processEvents(events)
 * }, 1000)
 * ```
 */
export function createStreamAdapter(options: { type: 'emitter' } & StreamAdapterConfig): EmitterAdapterResult
export function createStreamAdapter(options: { type: 'iterator' } & StreamAdapterConfig): IteratorAdapterResult
export function createStreamAdapter(options: { type: 'polling' } & PollingAdapterConfig): PollingAdapterResult
export function createStreamAdapter(options: CreateStreamAdapterOptions): StreamAdapterResult
export function createStreamAdapter(options: CreateStreamAdapterOptions): StreamAdapterResult {
  const { type, ...config } = options

  switch (type) {
    case 'emitter': {
      const adapter = new EventEmitterAdapter(config)
      return {
        type: 'emitter',
        adapter,
        emitter: adapter.getEmitter(),
        hook: adapter.getHook(),
        close: () => adapter.close(),
      }
    }
    case 'iterator': {
      const adapter = new AsyncIteratorAdapter(config)
      return {
        type: 'iterator',
        adapter,
        iterator: adapter.getIterator(),
        hook: adapter.getHook(),
        close: () => adapter.close(),
      }
    }
    case 'polling': {
      const adapter = new PollingAdapter(config)
      return {
        type: 'polling',
        adapter,
        poll: () => adapter.poll(),
        peek: () => adapter.peek(),
        take: (count: number) => adapter.take(count),
        hook: adapter.getHook(),
        close: () => adapter.close(),
      }
    }
    default:
      throw new Error(`Unknown stream adapter type: ${(options as { type: string }).type}`)
  }
}

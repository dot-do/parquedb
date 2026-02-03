/**
 * TailDO - Hibernatable WebSocket Durable Object for Tail Events
 *
 * Receives tail events from tail workers via WebSocket and writes them to R2
 * as NDJSON files, which trigger R2 event notifications. A separate
 * Compaction Consumer worker processes these into Parquet files.
 *
 * Event-Driven Architecture:
 * ```
 * TailDO -> writes raw events to R2
 *              ↓ object-create notification
 *        Queue -> Compaction Worker (observable via tail)
 *              ↓ writes Parquet segments
 *        R2 -> object-create notification
 *              ↓
 *        Queue -> MV refresh / downstream
 * ```
 *
 * Benefits:
 * - Fully observable (compaction worker is tailable)
 * - Decoupled processing for reliability
 * - Horizontal scaling via queue consumers
 * - Easier debugging and replay capability
 *
 * Features:
 * - Hibernatable WebSocket API for cost savings
 * - Raw event writing to R2
 * - Automatic flushing on connection close or threshold
 * - Per-connection metadata tracking
 *
 * @see https://developers.cloudflare.com/durable-objects/api/hibernatable-websockets/
 * @see https://developers.cloudflare.com/r2/buckets/event-notifications/
 */

import { DurableObject } from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import type { StorageBackend } from '../types/storage'
import type { R2Bucket as InternalR2Bucket } from '../storage/types/r2'
import { toR2Bucket } from '../utils/type-utils'
import type { ValidatedTraceItem } from './tail-validation'
import { logger } from '../utils/logger'
import { DEFAULT_TAIL_BUFFER_SIZE, DEFAULT_DO_FLUSH_INTERVAL_MS } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Environment bindings for TailDO
 */
export interface TailDOEnv {
  /** R2 bucket for storing raw event files */
  LOGS_BUCKET: R2Bucket

  /** Optional: Flush interval in ms (default: 30000) */
  FLUSH_INTERVAL_MS?: string | undefined

  /**
   * Prefix for raw event files in R2
   * @default "raw-events"
   */
  RAW_EVENTS_PREFIX?: string | undefined

  /**
   * Batch size before writing raw events file
   * @default 100
   */
  RAW_EVENTS_BATCH_SIZE?: string | undefined
}

/**
 * Raw events file format for event-driven mode
 */
export interface RawEventsFile {
  /** TailDO instance ID */
  doId: string
  /** Timestamp when the file was created */
  createdAt: number
  /** Batch sequence number (for ordering) */
  batchSeq: number
  /** Array of validated trace items */
  events: ValidatedTraceItem[]
}

/**
 * Message sent from tail worker to TailDO
 */
export interface TailWorkerMessage {
  /** Message type */
  type: 'tail_events'

  /** Tail worker instance ID */
  instanceId: string

  /** Timestamp when message was sent */
  timestamp: number

  /** Array of validated trace items from the tail worker */
  events: ValidatedTraceItem[]
}

/**
 * Acknowledgment message sent back to tail worker
 */
export interface TailAckMessage {
  type: 'ack'
  /** Number of events processed */
  count: number
  /** Timestamp */
  timestamp: number
}

/**
 * Error message sent back to tail worker
 */
export interface TailErrorMessage {
  type: 'error'
  /** Error message */
  message: string
  /** Timestamp */
  timestamp: number
}

/**
 * Connection metadata stored per WebSocket
 */
interface ConnectionMeta {
  /** Tail worker instance ID */
  instanceId: string
  /** When connection was established */
  connectedAt: number
  /** Total events received on this connection */
  eventsReceived: number
  /** Last message timestamp */
  lastMessageAt: number
}

// =============================================================================
// TailDO Class
// =============================================================================

/**
 * Durable Object for receiving and processing tail events
 *
 * Uses hibernatable WebSockets to minimize costs when idle.
 * Raw events are buffered and written to R2 as NDJSON files,
 * triggering R2 event notifications for downstream processing.
 */
export class TailDO extends DurableObject<TailDOEnv> {
  /** Storage backend for R2 */
  private storage: StorageBackend | null = null

  /** Connection metadata keyed by WebSocket */
  private connections: Map<WebSocket, ConnectionMeta> = new Map()

  /** Total events processed since DO instantiation */
  private totalEventsProcessed = 0

  /** Buffer for raw events */
  private rawEventsBuffer: ValidatedTraceItem[] = []

  /** Batch sequence number for ordering raw event files */
  private batchSeq = 0

  /** Unique ID for this DO instance */
  private doId: string

  /** Whether a flush alarm is currently scheduled */
  private alarmScheduled = false

  /** Whether persisted state has been loaded */
  private stateLoaded = false

  /** Maximum buffer size before forced flush (prevents unbounded memory growth) */
  private static readonly MAX_BUFFER_SIZE = DEFAULT_TAIL_BUFFER_SIZE

  /** Storage keys for persisted state */
  private static readonly STORAGE_KEY_BUFFER = 'rawEventsBuffer'
  private static readonly STORAGE_KEY_BATCH_SEQ = 'batchSeq'

  constructor(ctx: DurableObjectState, env: TailDOEnv) {
    super(ctx, env)
    this.doId = ctx.id.toString()

    // Restore state from storage on wake from hibernation
    // Use blockConcurrencyWhile to ensure state is loaded before handling requests
    this.ctx.blockConcurrencyWhile(async () => {
      await this.loadPersistedState()
    })
  }

  // ===========================================================================
  // State Persistence (Hibernation Support)
  // ===========================================================================

  /**
   * Load persisted state from Durable Object storage
   *
   * Called on construction to restore state after hibernation.
   */
  private async loadPersistedState(): Promise<void> {
    if (this.stateLoaded) return

    const [buffer, batchSeq] = await Promise.all([
      this.ctx.storage.get<ValidatedTraceItem[]>(TailDO.STORAGE_KEY_BUFFER),
      this.ctx.storage.get<number>(TailDO.STORAGE_KEY_BATCH_SEQ),
    ])

    if (buffer !== undefined) {
      this.rawEventsBuffer = buffer
      logger.debug(`[TailDO] Restored ${buffer.length} events from storage after hibernation`)
    }

    if (batchSeq !== undefined) {
      this.batchSeq = batchSeq
    }

    this.stateLoaded = true
  }

  /**
   * Persist the current buffer and batch sequence to storage
   *
   * Called after buffer modifications to ensure data survives hibernation.
   */
  private async persistState(): Promise<void> {
    await Promise.all([
      this.ctx.storage.put(TailDO.STORAGE_KEY_BUFFER, this.rawEventsBuffer),
      this.ctx.storage.put(TailDO.STORAGE_KEY_BATCH_SEQ, this.batchSeq),
    ])
  }

  /**
   * Clear persisted buffer from storage after successful flush
   */
  private async clearPersistedBuffer(): Promise<void> {
    // Keep batchSeq but clear the buffer
    await this.ctx.storage.put(TailDO.STORAGE_KEY_BUFFER, [])
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize the storage backend lazily
   */
  private ensureInitialized(): void {
    if (this.storage) return

    // Bridge between @cloudflare/workers-types R2Bucket and our internal type
    const bucket = toR2Bucket<InternalR2Bucket>(this.env.LOGS_BUCKET)
    this.storage = new R2Backend(bucket)

    logger.info('[TailDO] Initialized, raw events will be written to R2')
  }

  /**
   * Get the batch size for raw events
   */
  private getRawEventsBatchSize(): number {
    return this.env.RAW_EVENTS_BATCH_SIZE
      ? parseInt(this.env.RAW_EVENTS_BATCH_SIZE, 10)
      : 100
  }

  /**
   * Get the prefix for raw event files
   */
  private getRawEventsPrefix(): string {
    return this.env.RAW_EVENTS_PREFIX || 'raw-events'
  }

  /**
   * Write raw events to R2 as NDJSON file
   *
   * File format:
   * - First line: metadata (doId, createdAt, batchSeq)
   * - Subsequent lines: one ValidatedTraceItem per line
   */
  private async writeRawEventsToR2(events: ValidatedTraceItem[]): Promise<void> {
    if (!this.storage || events.length === 0) return

    const timestamp = Date.now()
    const prefix = this.getRawEventsPrefix()
    const filePath = `${prefix}/${timestamp}-${this.doId}-${this.batchSeq}.ndjson`

    // Build NDJSON content
    const metadata: Omit<RawEventsFile, 'events'> = {
      doId: this.doId,
      createdAt: timestamp,
      batchSeq: this.batchSeq,
    }

    const lines = [JSON.stringify(metadata)]
    for (const event of events) {
      lines.push(JSON.stringify(event))
    }
    const content = lines.join('\n')

    // Write to R2 (this triggers R2 event notification)
    await this.storage.write(filePath, new TextEncoder().encode(content), {
      contentType: 'application/x-ndjson',
      metadata: {
        doId: this.doId,
        batchSeq: String(this.batchSeq),
        eventCount: String(events.length),
      },
    })

    this.batchSeq++

    // Persist the updated batchSeq to survive hibernation
    await this.ctx.storage.put(TailDO.STORAGE_KEY_BATCH_SEQ, this.batchSeq)

    logger.debug(`[TailDO] Wrote ${events.length} raw events to ${filePath}`)
  }

  /**
   * Flush raw events buffer to R2
   *
   * Clears persisted buffer after successful write to prevent data loss.
   */
  private async flushRawEvents(): Promise<void> {
    if (this.rawEventsBuffer.length === 0) return

    const events = this.rawEventsBuffer
    this.rawEventsBuffer = []

    await this.writeRawEventsToR2(events)

    // Clear persisted buffer after successful flush
    await this.clearPersistedBuffer()
  }

  /**
   * Maybe flush raw events if batch size or max size is reached.
   * Also schedules a time-based flush alarm to prevent unbounded memory growth.
   */
  private async maybeFlushRawEvents(): Promise<void> {
    const batchSize = this.getRawEventsBatchSize()

    // Flush immediately if we hit batch size or max buffer size
    if (this.rawEventsBuffer.length >= batchSize || this.rawEventsBuffer.length >= TailDO.MAX_BUFFER_SIZE) {
      await this.flushRawEvents()
      return
    }

    // Schedule alarm for time-based flush if not already scheduled
    if (!this.alarmScheduled && this.rawEventsBuffer.length > 0) {
      const flushIntervalMs = this.env.FLUSH_INTERVAL_MS
        ? parseInt(this.env.FLUSH_INTERVAL_MS, 10)
        : DEFAULT_DO_FLUSH_INTERVAL_MS
      await this.ctx.storage.setAlarm(Date.now() + flushIntervalMs)
      this.alarmScheduled = true
    }
  }

  // ===========================================================================
  // HTTP Handler - WebSocket Upgrade
  // ===========================================================================

  /**
   * Handle incoming HTTP requests
   *
   * Only accepts WebSocket upgrade requests. Returns 426 for non-upgrade requests.
   */
  override async fetch(request: Request): Promise<Response> {
    // Check for WebSocket upgrade
    const upgradeHeader = request.headers.get('Upgrade')
    if (upgradeHeader !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 })
    }

    // Accept the WebSocket connection using hibernatable API
    const webSocketPair = new WebSocketPair()
    const client = webSocketPair[0]
    const server = webSocketPair[1]

    // Accept the server socket with hibernation enabled
    this.ctx.acceptWebSocket(server)

    // Initialize if needed
    this.ensureInitialized()

    // Store initial connection metadata
    this.connections.set(server, {
      instanceId: 'unknown',
      connectedAt: Date.now(),
      eventsReceived: 0,
      lastMessageAt: Date.now(),
    })

    return new Response(null, {
      status: 101,
      webSocket: client,
    })
  }

  // ===========================================================================
  // WebSocket Event Handlers (Hibernatable API)
  // ===========================================================================

  /**
   * Called when a WebSocket message is received
   *
   * Parses the message, validates it, and buffers events for R2.
   */
  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    this.ensureInitialized()

    try {
      // Parse message
      const text = typeof message === 'string'
        ? message
        : new TextDecoder().decode(message)
      const parsed = JSON.parse(text) as TailWorkerMessage

      // Validate message type
      if (parsed.type !== 'tail_events') {
        this.sendError(ws, `Unknown message type: ${parsed.type}`)
        return
      }

      // Update connection metadata
      const meta = this.connections.get(ws)
      if (meta) {
        if (parsed.instanceId) {
          meta.instanceId = parsed.instanceId
        }
        meta.lastMessageAt = Date.now()
        meta.eventsReceived += parsed.events.length
      }

      // Process events: buffer and write to R2
      if (parsed.events && parsed.events.length > 0) {
        this.rawEventsBuffer.push(...parsed.events)
        this.totalEventsProcessed += parsed.events.length

        // Persist buffer to survive hibernation
        await this.persistState()

        // Maybe flush if batch size reached
        await this.maybeFlushRawEvents()

        // Send acknowledgment
        this.sendAck(ws, parsed.events.length)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      logger.error('[TailDO] Error processing message:', message)
      this.sendError(ws, message)
    }
  }

  /**
   * Called when a WebSocket connection is closed
   *
   * Flushes any pending events and cleans up connection metadata.
   */
  override async webSocketClose(
    ws: WebSocket,
    code: number,
    _reason: string,
    wasClean: boolean
  ): Promise<void> {
    const meta = this.connections.get(ws)
    if (meta) {
      logger.info(
        `[TailDO] Connection closed: instanceId=${meta.instanceId}, ` +
        `eventsReceived=${meta.eventsReceived}, code=${code}, wasClean=${wasClean}`
      )
    }

    // Remove connection metadata
    this.connections.delete(ws)

    // If no more connections, flush pending events to R2
    if (this.connections.size === 0) {
      await this.flushRawEvents()
    }
  }

  /**
   * Called when a WebSocket error occurs
   */
  override async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : 'Unknown error'
    logger.error('[TailDO] WebSocket error:', message)

    // Clean up connection
    this.connections.delete(ws)
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Send acknowledgment message to tail worker
   */
  private sendAck(ws: WebSocket, count: number): void {
    const ack: TailAckMessage = {
      type: 'ack',
      count,
      timestamp: Date.now(),
    }

    try {
      ws.send(JSON.stringify(ack))
    } catch {
      // Connection may have closed, ignore
    }
  }

  /**
   * Send error message to tail worker
   */
  private sendError(ws: WebSocket, message: string): void {
    const error: TailErrorMessage = {
      type: 'error',
      message,
      timestamp: Date.now(),
    }

    try {
      ws.send(JSON.stringify(error))
    } catch {
      // Connection may have closed, ignore
    }
  }

  // ===========================================================================
  // Alarm Handler
  // ===========================================================================

  /**
   * Alarm handler for periodic flush
   *
   * Flushes raw events buffer to R2 and reschedules if needed.
   */
  override async alarm(): Promise<void> {
    // Reset alarm flag since this alarm has fired
    this.alarmScheduled = false

    if (this.rawEventsBuffer.length > 0) {
      logger.debug(`[TailDO] Alarm flush: ${this.rawEventsBuffer.length} raw events`)
      await this.flushRawEvents()
    }

    // Schedule next alarm if we have active connections or pending events
    if (this.connections.size > 0 || this.rawEventsBuffer.length > 0) {
      const flushIntervalMs = this.env.FLUSH_INTERVAL_MS
        ? parseInt(this.env.FLUSH_INTERVAL_MS, 10)
        : DEFAULT_DO_FLUSH_INTERVAL_MS
      await this.ctx.storage.setAlarm(Date.now() + flushIntervalMs)
      this.alarmScheduled = true
    }
  }

  // ===========================================================================
  // Stats API (for debugging/monitoring)
  // ===========================================================================

  /**
   * Get current statistics
   */
  getStats(): {
    connections: number
    totalEventsProcessed: number
    rawEventsBufferSize: number
    batchSeq: number
  } {
    return {
      connections: this.connections.size,
      totalEventsProcessed: this.totalEventsProcessed,
      rawEventsBufferSize: this.rawEventsBuffer.length,
      batchSeq: this.batchSeq,
    }
  }
}

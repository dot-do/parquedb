/**
 * TailDO - Hibernatable WebSocket Durable Object for Tail Events
 *
 * Receives tail events from tail workers via WebSocket. Supports two modes:
 *
 * 1. **Direct Mode** (default): Processes events through WorkerLogsMV and
 *    flushes directly to Parquet files in R2.
 *
 * 2. **Event-Driven Mode** (RAW_EVENTS_ENABLED=true): Writes raw events to R2
 *    as NDJSON files, which trigger R2 event notifications. A separate
 *    Compaction Consumer worker processes these into Parquet files.
 *
 * Event-Driven Architecture (Option D):
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
 * Benefits of Event-Driven Mode:
 * - Fully observable (compaction worker is tailable)
 * - Decoupled processing for reliability
 * - Horizontal scaling via queue consumers
 * - Easier debugging and replay capability
 *
 * Features:
 * - Hibernatable WebSocket API for cost savings
 * - Batched processing through WorkerLogsMV (direct mode)
 * - Raw event writing to R2 (event-driven mode)
 * - Automatic flushing on connection close or threshold
 * - Per-connection metadata tracking
 *
 * @see https://developers.cloudflare.com/durable-objects/api/hibernatable-websockets/
 * @see https://developers.cloudflare.com/r2/buckets/event-notifications/
 */

import { DurableObject } from 'cloudflare:workers'
import { WorkerLogsMV, createWorkerLogsMV } from '../streaming/worker-logs'
import type { TailItem, TailEvent } from '../streaming/worker-logs'
import { R2Backend } from '../storage/R2Backend'
import type { StorageBackend } from '../types/storage'
import type { R2Bucket as InternalR2Bucket } from '../storage/types/r2'
import { toR2Bucket } from '../utils/type-utils'
import type { ValidatedTraceItem } from './tail-validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Environment bindings for TailDO
 */
export interface TailDOEnv {
  /** R2 bucket for storing log Parquet files */
  LOGS_BUCKET: R2Bucket

  /** Optional: Prefix for log files in R2 */
  LOGS_PREFIX?: string

  /** Optional: Flush threshold (default: 1000) */
  FLUSH_THRESHOLD?: string

  /** Optional: Flush interval in ms (default: 30000) */
  FLUSH_INTERVAL_MS?: string

  /**
   * Enable event-driven mode (Option D)
   *
   * When enabled, TailDO writes raw events to R2 as NDJSON files instead of
   * processing them directly. This triggers R2 event notifications which are
   * processed by the Compaction Consumer worker.
   *
   * Set to "true" to enable.
   * @default false
   */
  RAW_EVENTS_ENABLED?: string

  /**
   * Prefix for raw event files in R2 (event-driven mode only)
   * @default "raw-events"
   */
  RAW_EVENTS_PREFIX?: string

  /**
   * Batch size before writing raw events file (event-driven mode only)
   * @default 100
   */
  RAW_EVENTS_BATCH_SIZE?: string
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
 *
 * In **direct mode**: State is stored in the WorkerLogsMV buffer and
 * periodically flushed to R2 as Parquet files.
 *
 * In **event-driven mode**: Raw events are buffered and written to R2
 * as NDJSON files, triggering R2 event notifications for downstream processing.
 */
export class TailDO extends DurableObject<TailDOEnv> {
  /** WorkerLogsMV instance for buffering and flushing logs (direct mode) */
  private mv: WorkerLogsMV | null = null

  /** Storage backend for R2 */
  private storage: StorageBackend | null = null

  /** Connection metadata keyed by WebSocket */
  private connections: Map<WebSocket, ConnectionMeta> = new Map()

  /** Total events processed since DO instantiation */
  private totalEventsProcessed = 0

  /** Whether event-driven mode is enabled */
  private rawEventsEnabled = false

  /** Buffer for raw events (event-driven mode) */
  private rawEventsBuffer: ValidatedTraceItem[] = []

  /** Batch sequence number for ordering raw event files */
  private batchSeq = 0

  /** Unique ID for this DO instance */
  private doId: string

  constructor(ctx: DurableObjectState, env: TailDOEnv) {
    super(ctx, env)
    this.doId = ctx.id.toString()
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize the MV and storage backend lazily
   */
  private ensureInitialized(): void {
    if (this.storage) return

    // Check if event-driven mode is enabled
    this.rawEventsEnabled = this.env.RAW_EVENTS_ENABLED === 'true'

    // Create R2 storage backend
    const prefix = this.env.LOGS_PREFIX || 'logs/workers'
    // Bridge between @cloudflare/workers-types R2Bucket and our internal type
    const bucket = toR2Bucket<InternalR2Bucket>(this.env.LOGS_BUCKET)
    this.storage = new R2Backend(bucket)

    // In event-driven mode, we don't need WorkerLogsMV
    if (this.rawEventsEnabled) {
      console.log('[TailDO] Event-driven mode enabled, raw events will be written to R2')
      return
    }

    // Direct mode: Create WorkerLogsMV instance
    const flushThreshold = this.env.FLUSH_THRESHOLD
      ? parseInt(this.env.FLUSH_THRESHOLD, 10)
      : 1000
    const flushIntervalMs = this.env.FLUSH_INTERVAL_MS
      ? parseInt(this.env.FLUSH_INTERVAL_MS, 10)
      : 30000

    this.mv = createWorkerLogsMV({
      storage: this.storage,
      datasetPath: prefix,
      flushThreshold,
      flushIntervalMs,
      compression: 'lz4',
    })

    // Start the MV (enables periodic flushing via timer)
    this.mv.start()
  }

  /**
   * Get the batch size for raw events (event-driven mode)
   */
  private getRawEventsBatchSize(): number {
    return this.env.RAW_EVENTS_BATCH_SIZE
      ? parseInt(this.env.RAW_EVENTS_BATCH_SIZE, 10)
      : 100
  }

  /**
   * Get the prefix for raw event files (event-driven mode)
   */
  private getRawEventsPrefix(): string {
    return this.env.RAW_EVENTS_PREFIX || 'raw-events'
  }

  /**
   * Write raw events to R2 as NDJSON file (event-driven mode)
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
    console.log(`[TailDO] Wrote ${events.length} raw events to ${filePath}`)
  }

  /**
   * Flush raw events buffer to R2 (event-driven mode)
   */
  private async flushRawEvents(): Promise<void> {
    if (this.rawEventsBuffer.length === 0) return

    const events = this.rawEventsBuffer
    this.rawEventsBuffer = []

    await this.writeRawEventsToR2(events)
  }

  /**
   * Maybe flush raw events if batch size is reached (event-driven mode)
   */
  private async maybeFlushRawEvents(): Promise<void> {
    const batchSize = this.getRawEventsBatchSize()
    if (this.rawEventsBuffer.length >= batchSize) {
      await this.flushRawEvents()
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
   * Parses the message, validates it, and passes events to WorkerLogsMV.
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

      // Process events
      if (parsed.events && parsed.events.length > 0) {
        if (this.rawEventsEnabled) {
          // Event-driven mode: buffer events and write to R2
          this.rawEventsBuffer.push(...parsed.events)
          this.totalEventsProcessed += parsed.events.length

          // Maybe flush if batch size reached
          await this.maybeFlushRawEvents()
        } else {
          // Direct mode: process through WorkerLogsMV
          // Convert validated items to TailItem format expected by WorkerLogsMV
          // ValidatedTraceItem has scriptName: string | null, TailItem expects string
          const traces: TailItem[] = parsed.events.map(event => ({
            scriptName: event.scriptName ?? 'unknown',
            outcome: event.outcome as TailItem['outcome'],
            eventTimestamp: event.eventTimestamp ?? Date.now(),
            event: event.event ? {
              request: event.event.request ? {
                method: event.event.request.method,
                url: event.event.request.url,
                headers: event.event.request.headers,
                cf: event.event.request.cf,
              } : undefined!,
              response: event.event.response,
            } : null,
            logs: event.logs.map(log => ({
              timestamp: log.timestamp,
              level: log.level as TailItem['logs'][number]['level'],
              message: [log.message], // WorkerLogsMV expects message as array
            })),
            exceptions: event.exceptions,
          }))

          const tailEvent: TailEvent = {
            type: 'tail',
            traces,
          }

          await this.mv!.ingestTailEvent(tailEvent)
          this.totalEventsProcessed += parsed.events.length
        }

        // Send acknowledgment
        this.sendAck(ws, parsed.events.length)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      console.error('[TailDO] Error processing message:', message)
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
    reason: string,
    wasClean: boolean
  ): Promise<void> {
    const meta = this.connections.get(ws)
    if (meta) {
      console.log(
        `[TailDO] Connection closed: instanceId=${meta.instanceId}, ` +
        `eventsReceived=${meta.eventsReceived}, code=${code}, wasClean=${wasClean}`
      )
    }

    // Remove connection metadata
    this.connections.delete(ws)

    // If no more connections, flush pending events
    if (this.connections.size === 0) {
      if (this.rawEventsEnabled) {
        // Event-driven mode: flush raw events to R2
        await this.flushRawEvents()
      } else if (this.mv) {
        // Direct mode: flush WorkerLogsMV
        await this.mv.flush()
      }
    }
  }

  /**
   * Called when a WebSocket error occurs
   */
  override async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : 'Unknown error'
    console.error('[TailDO] WebSocket error:', message)

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
   * In direct mode: WorkerLogsMV uses setInterval internally, but we also
   * set an alarm as a backup in case the DO is hibernated.
   *
   * In event-driven mode: Flushes raw events buffer to R2.
   */
  override async alarm(): Promise<void> {
    if (this.rawEventsEnabled) {
      // Event-driven mode: flush raw events
      if (this.rawEventsBuffer.length > 0) {
        console.log(`[TailDO] Alarm flush: ${this.rawEventsBuffer.length} raw events`)
        await this.flushRawEvents()
      }
    } else if (this.mv) {
      // Direct mode: flush WorkerLogsMV
      const stats = this.mv.getStats()
      if (stats.bufferSize > 0) {
        console.log(`[TailDO] Alarm flush: ${stats.bufferSize} records`)
        await this.mv.flush()
      }
    }

    // Schedule next alarm if we have active connections
    if (this.connections.size > 0) {
      const flushIntervalMs = this.env.FLUSH_INTERVAL_MS
        ? parseInt(this.env.FLUSH_INTERVAL_MS, 10)
        : 30000
      await this.ctx.storage.setAlarm(Date.now() + flushIntervalMs)
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
    mode: 'direct' | 'event-driven'
    mvStats: ReturnType<WorkerLogsMV['getStats']> | null
    rawEventsBufferSize: number
    batchSeq: number
  } {
    return {
      connections: this.connections.size,
      totalEventsProcessed: this.totalEventsProcessed,
      mode: this.rawEventsEnabled ? 'event-driven' : 'direct',
      mvStats: this.mv?.getStats() ?? null,
      rawEventsBufferSize: this.rawEventsBuffer.length,
      batchSeq: this.batchSeq,
    }
  }
}

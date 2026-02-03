/**
 * ParqueDB Streaming Tail Worker
 *
 * A simplified tail worker that streams events to TailDO via WebSocket
 * for efficient batching and Parquet storage.
 *
 * Architecture:
 * Tail events -> Tail Worker -> WebSocket -> TailDO -> WorkerLogsMV -> Parquet in R2
 *
 * Features:
 * - Minimal processing in tail worker (just metadata addition)
 * - WebSocket connection pooling and reconnection
 * - Local batching before sending (configurable)
 * - Instance ID tracking for debugging
 *
 * @see TailDO for the Durable Object that receives these events
 * @see WorkerLogsMV for the materialized view that buffers and flushes to Parquet
 */

import {
  validateTraceItems,
  type ValidatedTraceItem,
  type TailValidationConfig,
} from './tail-validation'

// Re-export validation types for consumers
export type { ValidatedTraceItem, TailValidationConfig }

// =============================================================================
// Types
// =============================================================================

/**
 * Environment bindings for the streaming tail worker
 */
export interface StreamingTailEnv {
  /** Durable Object namespace for TailDO */
  TAIL_DO: DurableObjectNamespace

  /** Optional: Instance ID for this worker (for debugging) */
  INSTANCE_ID?: string

  /** Optional: Batch size before sending (default: 50) */
  BATCH_SIZE?: string

  /** Optional: Max wait time before sending batch in ms (default: 1000) */
  BATCH_WAIT_MS?: string
}

/**
 * Message sent to TailDO
 */
export interface TailMessage {
  type: 'tail_events'
  instanceId: string
  timestamp: number
  events: ValidatedTraceItem[]
}

/**
 * Configuration for the streaming tail handler
 */
export interface StreamingTailConfig {
  /** Validation configuration */
  validation?: TailValidationConfig

  /** Batch size before sending to TailDO */
  batchSize?: number

  /** Max wait time before sending batch in ms */
  batchWaitMs?: number

  /** DO ID strategy: 'global' (single DO) or 'hourly' (one per hour) */
  doIdStrategy?: 'global' | 'hourly'
}

/**
 * Default configuration
 */
export const DEFAULT_STREAMING_CONFIG: Required<StreamingTailConfig> = {
  validation: {
    skipInvalidItems: true,
    maxItems: 10000,
    maxLogsPerItem: 1000,
    maxExceptionsPerItem: 100,
  },
  batchSize: 50,
  batchWaitMs: 1000,
  doIdStrategy: 'global',
}

// =============================================================================
// Connection State
// =============================================================================

/**
 * WebSocket connection state (stored per-worker instance)
 */
interface ConnectionState {
  ws: WebSocket | null
  connecting: boolean
  lastConnectAttempt: number
  pendingEvents: ValidatedTraceItem[]
  lastSendTime: number
}

/** Global connection state per DO */
const connectionStates = new Map<string, ConnectionState>()

/**
 * Get or create connection state for a DO
 */
function getConnectionState(doId: string): ConnectionState {
  let state = connectionStates.get(doId)
  if (!state) {
    state = {
      ws: null,
      connecting: false,
      lastConnectAttempt: 0,
      pendingEvents: [],
      lastSendTime: Date.now(),
    }
    connectionStates.set(doId, state)
  }
  return state
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a unique instance ID for this worker
 */
function generateInstanceId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `tail-${timestamp}-${random}`
}

/**
 * Get the DO ID based on strategy
 */
function getDOId(strategy: 'global' | 'hourly'): string {
  if (strategy === 'global') {
    return 'tail-global'
  }
  // Hourly: use current hour as ID for load distribution
  const now = new Date()
  const hourKey = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}-${String(now.getUTCHours()).padStart(2, '0')}`
  return `tail-${hourKey}`
}

/**
 * Connect to TailDO via WebSocket
 */
async function connectToTailDO(
  env: StreamingTailEnv,
  doId: string,
  state: ConnectionState
): Promise<WebSocket | null> {
  // Don't reconnect too frequently
  const now = Date.now()
  if (state.connecting || (now - state.lastConnectAttempt) < 1000) {
    return state.ws
  }

  state.connecting = true
  state.lastConnectAttempt = now

  try {
    // Get DO stub
    const id = env.TAIL_DO.idFromName(doId)
    const stub = env.TAIL_DO.get(id)

    // Create WebSocket connection using fetch with Upgrade header
    const response = await stub.fetch('https://tail-do/ws', {
      headers: {
        Upgrade: 'websocket',
      },
    })

    if (response.status !== 101) {
      console.error('[StreamingTail] Failed to upgrade to WebSocket:', response.status)
      return null
    }

    const ws = response.webSocket
    if (!ws) {
      console.error('[StreamingTail] No WebSocket in response')
      return null
    }

    // Accept the WebSocket
    ws.accept()

    // Set up event handlers
    ws.addEventListener('close', () => {
      state.ws = null
    })

    ws.addEventListener('error', (event) => {
      console.error('[StreamingTail] WebSocket error:', event)
      state.ws = null
    })

    state.ws = ws
    return ws
  } catch (error) {
    console.error('[StreamingTail] Connection error:', error)
    return null
  } finally {
    state.connecting = false
  }
}

/**
 * Send events to TailDO
 */
async function sendEvents(
  env: StreamingTailEnv,
  doId: string,
  events: ValidatedTraceItem[],
  instanceId: string
): Promise<boolean> {
  if (events.length === 0) {
    return true
  }

  const state = getConnectionState(doId)

  // Try to connect if not connected
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    await connectToTailDO(env, doId, state)
  }

  // If still not connected, queue events for later
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    state.pendingEvents.push(...events)
    return false
  }

  try {
    // Include any pending events
    const allEvents = [...state.pendingEvents, ...events]
    state.pendingEvents = []

    const message: TailMessage = {
      type: 'tail_events',
      instanceId,
      timestamp: Date.now(),
      events: allEvents,
    }

    state.ws.send(JSON.stringify(message))
    state.lastSendTime = Date.now()
    return true
  } catch (error) {
    console.error('[StreamingTail] Send error:', error)
    state.pendingEvents.push(...events)
    state.ws = null
    return false
  }
}

// =============================================================================
// Tail Handler
// =============================================================================

/**
 * Create a streaming tail handler
 */
export function createStreamingTailHandler(config: StreamingTailConfig = {}) {
  const cfg = { ...DEFAULT_STREAMING_CONFIG, ...config }
  const instanceId = generateInstanceId()

  // Local batch buffer
  let eventBuffer: ValidatedTraceItem[] = []
  let lastFlushTime = Date.now()

  return async function tail(events: unknown, env: StreamingTailEnv): Promise<void> {
    // Validate input events
    const validationResult = validateTraceItems(events, cfg.validation)

    if (validationResult.validCount === 0) {
      return
    }

    // Add events to buffer
    eventBuffer.push(...validationResult.validItems)

    // Check if we should flush
    const now = Date.now()
    const shouldFlush =
      eventBuffer.length >= cfg.batchSize ||
      (now - lastFlushTime) >= cfg.batchWaitMs

    if (!shouldFlush) {
      return
    }

    // Get DO ID and send events
    const doId = getDOId(cfg.doIdStrategy)
    const eventsToSend = eventBuffer
    eventBuffer = []
    lastFlushTime = now

    await sendEvents(env, doId, eventsToSend, instanceId)
  }
}

// =============================================================================
// Exports
// =============================================================================

/**
 * Default streaming tail worker export
 *
 * @example wrangler.toml configuration:
 * ```toml
 * name = "parquedb-tail-streaming"
 * main = "src/worker/tail-streaming.ts"
 *
 * # Tail consumers - which workers to tail
 * [[tail_consumers]]
 * service = "parquedb-tail-streaming"
 *
 * # Durable Object binding
 * [[durable_objects.bindings]]
 * name = "TAIL_DO"
 * class_name = "TailDO"
 *
 * # R2 bucket for the DO to write logs
 * [[r2_buckets]]
 * binding = "LOGS_BUCKET"
 * bucket_name = "parquedb-logs"
 *
 * # Environment variables
 * [vars]
 * BATCH_SIZE = "100"
 * BATCH_WAIT_MS = "2000"
 * ```
 */
export default {
  /**
   * Tail handler - receives events and streams to TailDO
   */
  async tail(events: unknown, env: StreamingTailEnv): Promise<void> {
    const handler = createStreamingTailHandler(DEFAULT_STREAMING_CONFIG)
    await handler(events, env)
  },
}

/**
 * Export TailDO class for wrangler to discover
 */
export { TailDO } from './TailDO'

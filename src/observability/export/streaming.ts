/**
 * Streaming Export
 *
 * Functions for real-time streaming of observability data via SSE and WebSocket.
 * Provides live dashboard updates and metric streaming.
 *
 * @module observability/export/streaming
 */

import type {
  SSEEvent,
  SSEMetricEvent,
  SSEAlertEvent,
  SSEHeartbeatEvent,
  SSEErrorEvent,
  WSMessage,
  WSSubscribeMessage,
  WSUnsubscribeMessage,
  WSMetricMessage,
  WSAlertMessage,
  WSAckMessage,
} from './types'
import type { CompactionMetrics } from '../compaction/types'
import type { AIUsageAggregate } from '../ai/types'

// =============================================================================
// SSE Functions
// =============================================================================

/**
 * Format an SSE event
 */
export function formatSSEEvent(event: SSEEvent): string {
  const data = JSON.stringify(event)
  let output = `event: ${event.type}\n`
  output += `data: ${data}\n\n`
  return output
}

/**
 * Create an SSE metric event
 */
export function createSSEMetricEvent(
  namespace: string,
  metrics: Record<string, number>
): SSEMetricEvent {
  return {
    type: 'metric',
    timestamp: Date.now(),
    namespace,
    metrics,
  }
}

/**
 * Create an SSE alert event
 */
export function createSSEAlertEvent(
  severity: 'info' | 'warning' | 'critical',
  title: string,
  message: string,
  namespace?: string,
  metadata?: Record<string, unknown>
): SSEAlertEvent {
  return {
    type: 'alert',
    timestamp: Date.now(),
    severity,
    namespace,
    title,
    message,
    metadata,
  }
}

/**
 * Create an SSE heartbeat event
 */
export function createSSEHeartbeat(): SSEHeartbeatEvent {
  return {
    type: 'heartbeat',
    timestamp: Date.now(),
  }
}

/**
 * Create an SSE error event
 */
export function createSSEError(error: string, code?: string): SSEErrorEvent {
  return {
    type: 'error',
    timestamp: Date.now(),
    error,
    code,
  }
}

/**
 * Convert compaction metrics to SSE metric event
 */
export function compactionMetricsToSSE(metrics: CompactionMetrics): SSEMetricEvent {
  return createSSEMetricEvent(metrics.namespace, {
    windows_pending: metrics.windows_pending,
    windows_processing: metrics.windows_processing,
    windows_dispatched: metrics.windows_dispatched,
    windows_stuck: metrics.windows_stuck,
    files_pending: metrics.files_pending,
    bytes_pending: metrics.bytes_pending,
    oldest_window_age_ms: metrics.oldest_window_age_ms,
    known_writers: metrics.known_writers,
    active_writers: metrics.active_writers,
  })
}

/**
 * Convert AI usage aggregate to SSE metric event
 */
export function aiUsageToSSE(aggregate: AIUsageAggregate): SSEMetricEvent {
  const namespace = `${aggregate.modelId}/${aggregate.providerId}`
  return createSSEMetricEvent(namespace, {
    requests_total: aggregate.requestCount,
    requests_success: aggregate.successCount,
    requests_error: aggregate.errorCount,
    requests_cached: aggregate.cachedCount,
    tokens_prompt: aggregate.totalPromptTokens,
    tokens_completion: aggregate.totalCompletionTokens,
    tokens_total: aggregate.totalTokens,
    cost_total: aggregate.estimatedTotalCost,
    latency_avg: aggregate.avgLatencyMs,
    error_rate: aggregate.requestCount > 0 ? aggregate.errorCount / aggregate.requestCount : 0,
  })
}

/**
 * Create SSE response with headers
 */
export function createSSEResponse(
  body: ReadableStream,
  headers?: Record<string, string>
): Response {
  return new Response(body, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      ...headers,
    },
  })
}

// =============================================================================
// SSE Stream Manager
// =============================================================================

/**
 * Configuration for SSE stream
 */
export interface SSEStreamConfig {
  /** Interval between heartbeats in ms (default: 30000) */
  heartbeatInterval?: number | undefined
  /** Interval between metric updates in ms (default: 5000) */
  metricInterval?: number | undefined
  /** Namespaces to stream (all if empty) */
  namespaces?: string[] | undefined
  /** Metrics to include (all if empty) */
  metrics?: string[] | undefined
}

/**
 * Create an SSE stream for metrics
 *
 * @param getMetrics - Function to get current metrics
 * @param config - Stream configuration
 * @returns ReadableStream for SSE
 */
export function createSSEStream(
  getMetrics: () => Promise<Array<{ namespace: string; metrics: Record<string, number> }>>,
  config: SSEStreamConfig = {}
): ReadableStream {
  const heartbeatInterval = config.heartbeatInterval ?? 30000
  const metricInterval = config.metricInterval ?? 5000
  const namespaceFilter = config.namespaces ? new Set(config.namespaces) : null
  const metricFilter = config.metrics ? new Set(config.metrics) : null

  let heartbeatTimer: ReturnType<typeof setInterval> | null = null
  let metricTimer: ReturnType<typeof setInterval> | null = null
  let encoder: TextEncoder

  return new ReadableStream({
    start(controller) {
      encoder = new TextEncoder()

      // Send initial connection event
      const connectEvent = formatSSEEvent({
        type: 'heartbeat',
        timestamp: Date.now(),
      })
      controller.enqueue(encoder.encode(connectEvent))

      // Set up heartbeat
      heartbeatTimer = setInterval(() => {
        try {
          const event = formatSSEEvent(createSSEHeartbeat())
          controller.enqueue(encoder.encode(event))
        } catch {
          // Stream closed
          if (heartbeatTimer) clearInterval(heartbeatTimer)
        }
      }, heartbeatInterval)

      // Set up metric streaming
      metricTimer = setInterval(async () => {
        try {
          const allMetrics = await getMetrics()

          for (const { namespace, metrics } of allMetrics) {
            // Apply namespace filter
            if (namespaceFilter && !namespaceFilter.has(namespace)) {
              continue
            }

            // Apply metric filter
            let filteredMetrics = metrics
            if (metricFilter) {
              filteredMetrics = {}
              for (const [key, value] of Object.entries(metrics)) {
                if (metricFilter.has(key)) {
                  filteredMetrics[key] = value
                }
              }
            }

            if (Object.keys(filteredMetrics).length > 0) {
              const event = formatSSEEvent(createSSEMetricEvent(namespace, filteredMetrics))
              controller.enqueue(encoder.encode(event))
            }
          }
        } catch (error) {
          const errorEvent = formatSSEEvent(
            createSSEError(
              error instanceof Error ? error.message : 'Unknown error',
              'METRIC_FETCH_ERROR'
            )
          )
          controller.enqueue(encoder.encode(errorEvent))
        }
      }, metricInterval)
    },

    cancel() {
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      if (metricTimer) clearInterval(metricTimer)
    },
  })
}

// =============================================================================
// WebSocket Functions
// =============================================================================

/**
 * Parse WebSocket message
 */
export function parseWSMessage(data: string): WSMessage | null {
  try {
    return JSON.parse(data) as WSMessage
  } catch {
    return null
  }
}

/**
 * Format WebSocket message
 */
export function formatWSMessage(message: WSMessage): string {
  return JSON.stringify(message)
}

/**
 * Create WebSocket metric message
 */
export function createWSMetricMessage(
  subscriptionId: string,
  namespace: string,
  metrics: Record<string, number>
): WSMetricMessage {
  return {
    type: 'metric',
    subscriptionId,
    timestamp: Date.now(),
    namespace,
    metrics,
  }
}

/**
 * Create WebSocket alert message
 */
export function createWSAlertMessage(
  severity: 'info' | 'warning' | 'critical',
  title: string,
  message: string,
  namespace?: string
): WSAlertMessage {
  return {
    type: 'alert',
    timestamp: Date.now(),
    severity,
    namespace,
    title,
    message,
  }
}

/**
 * Create WebSocket ack message
 */
export function createWSAckMessage(
  id: string,
  status: 'subscribed' | 'unsubscribed' | 'error',
  message?: string
): WSAckMessage {
  return {
    type: 'ack',
    id,
    status,
    message,
  }
}

// =============================================================================
// WebSocket Subscription Manager
// =============================================================================

/**
 * Subscription state
 */
export interface WSSubscription {
  id: string
  namespaces: string[]
  metrics: string[]
  interval: number
  timer?: ReturnType<typeof setInterval> | undefined
}

/**
 * WebSocket connection state
 */
export interface WSConnectionState {
  subscriptions: Map<string, WSSubscription>
  onSend: (message: string) => void
}

/**
 * Handle WebSocket subscribe message
 */
export function handleWSSubscribe(
  message: WSSubscribeMessage,
  state: WSConnectionState,
  getMetrics: () => Promise<Array<{ namespace: string; metrics: Record<string, number> }>>
): void {
  // Check if already subscribed
  if (state.subscriptions.has(message.id)) {
    state.onSend(formatWSMessage(createWSAckMessage(message.id, 'error', 'Already subscribed')))
    return
  }

  const subscription: WSSubscription = {
    id: message.id,
    namespaces: message.namespaces ?? [],
    metrics: message.metrics ?? [],
    interval: message.interval ?? 5000,
  }

  // Set up metric streaming
  subscription.timer = setInterval(async () => {
    try {
      const allMetrics = await getMetrics()
      const namespaceFilter = subscription.namespaces.length > 0
        ? new Set(subscription.namespaces)
        : null
      const metricFilter = subscription.metrics.length > 0
        ? new Set(subscription.metrics)
        : null

      for (const { namespace, metrics } of allMetrics) {
        // Apply namespace filter
        if (namespaceFilter && !namespaceFilter.has(namespace)) {
          continue
        }

        // Apply metric filter
        let filteredMetrics = metrics
        if (metricFilter) {
          filteredMetrics = {}
          for (const [key, value] of Object.entries(metrics)) {
            if (metricFilter.has(key)) {
              filteredMetrics[key] = value
            }
          }
        }

        if (Object.keys(filteredMetrics).length > 0) {
          const wsMessage = createWSMetricMessage(subscription.id, namespace, filteredMetrics)
          state.onSend(formatWSMessage(wsMessage))
        }
      }
    } catch (error) {
      state.onSend(formatWSMessage({
        type: 'error',
        error: error instanceof Error ? error.message : 'Unknown error',
        code: 'METRIC_FETCH_ERROR',
      }))
    }
  }, subscription.interval)

  state.subscriptions.set(message.id, subscription)
  state.onSend(formatWSMessage(createWSAckMessage(message.id, 'subscribed')))
}

/**
 * Handle WebSocket unsubscribe message
 */
export function handleWSUnsubscribe(
  message: WSUnsubscribeMessage,
  state: WSConnectionState
): void {
  const subscription = state.subscriptions.get(message.id)
  if (!subscription) {
    state.onSend(formatWSMessage(createWSAckMessage(message.id, 'error', 'Not subscribed')))
    return
  }

  if (subscription.timer) {
    clearInterval(subscription.timer)
  }
  state.subscriptions.delete(message.id)
  state.onSend(formatWSMessage(createWSAckMessage(message.id, 'unsubscribed')))
}

/**
 * Handle WebSocket message
 */
export function handleWSMessage(
  data: string,
  state: WSConnectionState,
  getMetrics: () => Promise<Array<{ namespace: string; metrics: Record<string, number> }>>
): void {
  const message = parseWSMessage(data)
  if (!message) {
    state.onSend(formatWSMessage({
      type: 'error',
      error: 'Invalid message format',
      code: 'PARSE_ERROR',
    }))
    return
  }

  switch (message.type) {
    case 'subscribe':
      handleWSSubscribe(message, state, getMetrics)
      break
    case 'unsubscribe':
      handleWSUnsubscribe(message, state)
      break
    default:
      state.onSend(formatWSMessage({
        type: 'error',
        error: `Unknown message type: ${(message as { type: string }).type}`,
        code: 'UNKNOWN_TYPE',
      }))
  }
}

/**
 * Clean up WebSocket connection state
 */
export function cleanupWSConnection(state: WSConnectionState): void {
  for (const subscription of state.subscriptions.values()) {
    if (subscription.timer) {
      clearInterval(subscription.timer)
    }
  }
  state.subscriptions.clear()
}

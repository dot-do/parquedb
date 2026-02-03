/**
 * Real-time Subscriptions Module for ParqueDB
 *
 * Provides real-time subscriptions to collection changes with support
 * for both Cloudflare Workers (WebSocket via Durable Objects) and
 * Node.js (SSE or WebSocket) environments.
 *
 * @example
 * ```typescript
 * // Server-side: Create subscription manager
 * import { SubscriptionManager, createSSEResponse, InMemoryEventSource } from 'parquedb/subscriptions'
 *
 * const manager = new SubscriptionManager()
 * const eventSource = new InMemoryEventSource()
 * await manager.setEventSource(eventSource)
 * await manager.start()
 *
 * // Handle SSE endpoint
 * function handleSubscribe(request: Request) {
 *   const { response, writer } = createSSEResponse()
 *   const connection = manager.addConnection(writer)
 *
 *   // Subscribe to posts collection
 *   manager.subscribe(connection.id, {
 *     ns: 'posts',
 *     filter: { status: 'published' },
 *     ops: ['CREATE', 'UPDATE'],
 *   })
 *
 *   return response
 * }
 *
 * // Client-side: Connect via SSE
 * const eventSource = new EventSource('/subscribe')
 * eventSource.onmessage = (event) => {
 *   const message = JSON.parse(event.data)
 *   if (message.type === 'change') {
 *     console.log('Entity changed:', message.data)
 *   }
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Cloudflare Workers: WebSocket via Durable Object
 * import { SubscriptionManager, handleWebSocketUpgrade } from 'parquedb/subscriptions'
 *
 * export class SubscriptionDO extends DurableObject {
 *   private manager = new SubscriptionManager()
 *
 *   async fetch(request: Request) {
 *     if (request.headers.get('Upgrade') === 'websocket') {
 *       return handleWebSocketUpgrade(request, this.manager, {
 *         onMessage: (connId, data) => {
 *           if (data.type === 'subscribe') {
 *             this.manager.subscribe(connId, data)
 *           } else if (data.type === 'unsubscribe') {
 *             this.manager.unsubscribe(connId, data.subscriptionId)
 *           }
 *         }
 *       })
 *     }
 *     return new Response('WebSocket required', { status: 400 })
 *   }
 * }
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

export type {
  // Event types
  SubscriptionOp,
  ChangeEvent,
  SubscriptionControlMessage,
  SubscriptionMessage,
  // Configuration
  SubscriptionOptions,
  Subscription,
  Connection,
  SubscriptionManagerConfig,
  // Transport
  SubscriptionWriter,
  // Event Source
  SubscriptionEventSource,
  // Reconnection
  ReconnectionState,
  ResumeResult,
  // Statistics
  SubscriptionStats,
} from './types'

export { DEFAULT_SUBSCRIPTION_CONFIG } from './types'

// =============================================================================
// Subscription Manager
// =============================================================================

export {
  SubscriptionManager,
  createSubscriptionManager,
} from './manager'

// =============================================================================
// Transport Implementations
// =============================================================================

export {
  // WebSocket
  WebSocketWriter,
  handleWebSocketUpgrade,
  type WebSocketHandlerConfig,
  // SSE
  SSEWriter,
  createSSEResponse,
  // Node.js SSE
  NodeSSEWriter,
  type NodeResponseLike,
  // Testing
  MockWriter,
} from './transports'

// =============================================================================
// Event Sources
// =============================================================================

export {
  // In-memory (testing)
  InMemoryEventSource,
  createInMemoryEventSource,
  // EventWriter integration
  EventWriterSource,
  createEventWriterSource,
  type EventWriterSourceOptions,
  // Polling (Node.js)
  PollingEventSource,
  createPollingEventSource,
  type PollingEventSourceOptions,
  type EventFetcher,
} from './event-source'

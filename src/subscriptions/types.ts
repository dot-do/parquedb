/**
 * Real-time Subscription Types for ParqueDB
 *
 * Supports subscribing to collection changes with MongoDB-style filters.
 * Works in both Cloudflare Workers (WebSocket via Durable Objects) and
 * Node.js (SSE or WebSocket) environments.
 */

import type { Filter } from '../types/filter'
import type { Event, EventOp, EntityId, Variant } from '../types/entity'

// =============================================================================
// Subscription Event Types
// =============================================================================

/**
 * Operation types that can be subscribed to
 */
export type SubscriptionOp = 'CREATE' | 'UPDATE' | 'DELETE' | 'ALL'

/**
 * A change event emitted to subscribers
 */
export interface ChangeEvent {
  /** Unique event ID */
  id: string
  /** Event timestamp (ms since epoch) */
  ts: number
  /** Operation type */
  op: EventOp
  /** Namespace/collection */
  ns: string
  /** Entity ID */
  entityId: string
  /** Full entity ID (ns/id) */
  fullId: EntityId
  /** State before change (undefined for CREATE) */
  before?: Variant | undefined
  /** State after change (undefined for DELETE) */
  after?: Variant | undefined
  /** Who made the change */
  actor?: string | undefined
  /** Additional metadata */
  metadata?: Variant | undefined
}

/**
 * Control message for subscription management
 */
export interface SubscriptionControlMessage {
  type: 'subscribe' | 'unsubscribe' | 'ping' | 'pong' | 'error' | 'ack'
  /** Subscription ID */
  subscriptionId?: string | undefined
  /** Namespace to subscribe to */
  ns?: string | undefined
  /** Filter for the subscription */
  filter?: Filter | undefined
  /** Operations to subscribe to */
  ops?: SubscriptionOp[] | undefined
  /** Error message (for error type) */
  error?: string | undefined
  /** Error code */
  code?: string | undefined
}

/**
 * Message sent to clients
 */
export type SubscriptionMessage =
  | { type: 'change'; data: ChangeEvent }
  | { type: 'subscribed'; subscriptionId: string; ns: string }
  | { type: 'unsubscribed'; subscriptionId: string }
  | { type: 'error'; error: string; code?: string | undefined }
  | { type: 'pong'; ts: number }
  | { type: 'connected'; connectionId: string }

// =============================================================================
// Subscription Configuration
// =============================================================================

/**
 * Options for creating a subscription
 */
export interface SubscriptionOptions {
  /** Namespace/collection to subscribe to */
  ns: string
  /** MongoDB-style filter for matching entities */
  filter?: Filter | undefined
  /** Operations to subscribe to (default: ALL) */
  ops?: SubscriptionOp[] | undefined
  /** Include full before/after state (default: true) */
  includeState?: boolean | undefined
  /** Cursor to resume from (for reconnection) */
  resumeAfter?: string | undefined
  /** Maximum events per second (rate limiting) */
  maxEventsPerSecond?: number | undefined
}

/**
 * A registered subscription
 */
export interface Subscription {
  /** Unique subscription ID */
  id: string
  /** Connection ID this subscription belongs to */
  connectionId: string
  /** Namespace being watched */
  ns: string
  /** Filter being applied */
  filter: Filter
  /** Operations being watched */
  ops: SubscriptionOp[]
  /** Include full state in events */
  includeState: boolean
  /** Last event ID processed (for resumption) */
  lastEventId?: string | undefined
  /** Created timestamp */
  createdAt: number
}

/**
 * Connection state for a single client
 */
export interface Connection {
  /** Unique connection ID */
  id: string
  /** Active subscriptions for this connection */
  subscriptions: Map<string, Subscription>
  /** Last activity timestamp */
  lastActivity: number
  /** WebSocket or SSE response writer */
  writer: SubscriptionWriter
  /** Connection metadata */
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// Transport Abstraction
// =============================================================================

/**
 * Abstraction for writing subscription messages to different transports
 */
export interface SubscriptionWriter {
  /** Send a message to the client */
  send(message: SubscriptionMessage): Promise<void>
  /** Close the connection */
  close(): Promise<void>
  /** Check if connection is still open */
  isOpen(): boolean
}

/**
 * Configuration for the subscription manager
 */
export interface SubscriptionManagerConfig {
  /** Maximum subscriptions per connection (default: 10) */
  maxSubscriptionsPerConnection?: number | undefined
  /** Connection timeout in ms (default: 30000) */
  connectionTimeoutMs?: number | undefined
  /** Heartbeat interval in ms (default: 15000) */
  heartbeatIntervalMs?: number | undefined
  /** Maximum pending events per subscription (default: 1000) */
  maxPendingEvents?: number | undefined
  /** Enable debug logging */
  debug?: boolean | undefined
}

import {
  MAX_SUBSCRIPTIONS_PER_CONNECTION,
  DEFAULT_CONNECTION_TIMEOUT_MS,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  MAX_PENDING_EVENTS,
} from '../constants'

/**
 * Default subscription manager configuration
 */
export const DEFAULT_SUBSCRIPTION_CONFIG: Required<SubscriptionManagerConfig> = {
  maxSubscriptionsPerConnection: MAX_SUBSCRIPTIONS_PER_CONNECTION,
  connectionTimeoutMs: DEFAULT_CONNECTION_TIMEOUT_MS,
  heartbeatIntervalMs: DEFAULT_HEARTBEAT_INTERVAL_MS,
  maxPendingEvents: MAX_PENDING_EVENTS,
  debug: false,
}

// =============================================================================
// Event Source Interface
// =============================================================================

/**
 * Interface for receiving events from the database
 *
 * The subscription manager listens to this source and distributes
 * events to matching subscribers.
 */
export interface SubscriptionEventSource {
  /** Register a handler for new events */
  onEvent(handler: (event: Event) => void): () => void
  /** Start receiving events */
  start(): Promise<void>
  /** Stop receiving events */
  stop(): Promise<void>
}

// =============================================================================
// Reconnection Support
// =============================================================================

/**
 * State needed for reconnection
 */
export interface ReconnectionState {
  /** Connection ID to resume */
  connectionId: string
  /** Last event ID received per subscription */
  lastEventIds: Record<string, string>
  /** Subscriptions to re-establish */
  subscriptions: Array<{
    id: string
    ns: string
    filter?: Filter | undefined
    ops?: SubscriptionOp[] | undefined
  }>
}

/**
 * Result of attempting to resume a connection
 */
export interface ResumeResult {
  /** Whether resume was successful */
  success: boolean
  /** New connection ID (may differ if old connection expired) */
  connectionId: string
  /** Events that occurred during disconnection */
  missedEvents?: ChangeEvent[] | undefined
  /** Subscriptions that were re-established */
  resumedSubscriptions: string[]
  /** Subscriptions that failed to resume */
  failedSubscriptions: Array<{ id: string; reason: string }>
}

// =============================================================================
// Statistics
// =============================================================================

/**
 * Statistics about the subscription system
 */
export interface SubscriptionStats {
  /** Number of active connections */
  activeConnections: number
  /** Total subscriptions across all connections */
  totalSubscriptions: number
  /** Events processed since start */
  eventsProcessed: number
  /** Events delivered to subscribers */
  eventsDelivered: number
  /** Events filtered out (didn't match any subscription) */
  eventsFiltered: number
  /** Current queue depth */
  queueDepth: number
  /** Subscriptions by namespace */
  subscriptionsByNs: Record<string, number>
}

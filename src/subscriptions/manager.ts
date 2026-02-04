/**
 * Subscription Manager
 *
 * Core subscription system that manages connections, subscriptions,
 * and distributes events to matching subscribers.
 *
 * Works in both Node.js and Cloudflare Workers environments.
 */

import type { Event } from '../types/entity'
import { entityId as makeEntityId } from '../types/entity'
import { matchesFilter } from '../query/filter'
import { parseEntityTarget } from '../types/entity'
import { logger } from '../utils/logger'
import type {
  ChangeEvent,
  Connection,
  Subscription,
  SubscriptionOptions,
  SubscriptionManagerConfig,
  SubscriptionMessage,
  SubscriptionWriter,
  SubscriptionEventSource,
  SubscriptionStats,
  SubscriptionOp,
  ReconnectionState,
  ResumeResult,
} from './types'
import { DEFAULT_SUBSCRIPTION_CONFIG } from './types'

// =============================================================================
// ID Generation
// =============================================================================

function generateId(prefix: string): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).slice(2, 10)
  return `${prefix}_${timestamp}_${random}`
}

// =============================================================================
// Subscription Manager
// =============================================================================

/**
 * Manages real-time subscriptions to database changes
 *
 * @example
 * ```typescript
 * // Create manager with event source
 * const manager = new SubscriptionManager({
 *   maxSubscriptionsPerConnection: 10,
 * })
 *
 * // Register event source (e.g., from EventWriter flush handler)
 * manager.setEventSource(eventSource)
 *
 * // Add a connection (from WebSocket or SSE)
 * const connection = manager.addConnection(writer)
 *
 * // Subscribe to changes
 * manager.subscribe(connection.id, {
 *   ns: 'posts',
 *   filter: { status: 'published' },
 *   ops: ['CREATE', 'UPDATE'],
 * })
 * ```
 */
export class SubscriptionManager {
  private config: Required<SubscriptionManagerConfig>
  private connections: Map<string, Connection> = new Map()
  private eventSource?: SubscriptionEventSource
  private eventSourceUnsubscribe?: () => void
  private heartbeatTimer?: ReturnType<typeof setInterval> | undefined
  private stats: SubscriptionStats = {
    activeConnections: 0,
    totalSubscriptions: 0,
    eventsProcessed: 0,
    eventsDelivered: 0,
    eventsFiltered: 0,
    queueDepth: 0,
    subscriptionsByNs: {},
  }

  constructor(config: SubscriptionManagerConfig = {}) {
    this.config = { ...DEFAULT_SUBSCRIPTION_CONFIG, ...config }
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  /**
   * Add a new connection
   *
   * @param writer - Transport writer for sending messages
   * @param metadata - Optional connection metadata
   * @returns The new connection
   */
  addConnection(writer: SubscriptionWriter, metadata?: Record<string, unknown>): Connection {
    const connection: Connection = {
      id: generateId('conn'),
      subscriptions: new Map(),
      lastActivity: Date.now(),
      writer,
      metadata,
    }

    this.connections.set(connection.id, connection)
    this.stats.activeConnections++

    // Send connected message
    this.sendToConnection(connection, {
      type: 'connected',
      connectionId: connection.id,
    })

    if (this.config.debug) {
      logger.debug(`[SubscriptionManager] Connection added: ${connection.id}`)
    }

    return connection
  }

  /**
   * Remove a connection and all its subscriptions
   *
   * @param connectionId - ID of connection to remove
   */
  async removeConnection(connectionId: string): Promise<void> {
    const connection = this.connections.get(connectionId)
    if (!connection) return

    // Remove all subscriptions
    for (const subId of connection.subscriptions.keys()) {
      this.removeSubscriptionFromStats(connection.subscriptions.get(subId)!)
    }

    // Close the writer
    try {
      await connection.writer.close()
    } catch {
      // Ignore close errors
    }

    this.connections.delete(connectionId)
    this.stats.activeConnections--

    if (this.config.debug) {
      logger.debug(`[SubscriptionManager] Connection removed: ${connectionId}`)
    }
  }

  /**
   * Get a connection by ID
   */
  getConnection(connectionId: string): Connection | undefined {
    return this.connections.get(connectionId)
  }

  // ===========================================================================
  // Subscription Management
  // ===========================================================================

  /**
   * Create a new subscription for a connection
   *
   * @param connectionId - ID of the connection
   * @param options - Subscription options
   * @returns Subscription ID or null if failed
   */
  subscribe(connectionId: string, options: SubscriptionOptions): string | null {
    const connection = this.connections.get(connectionId)
    if (!connection) {
      if (this.config.debug) {
        logger.debug(`[SubscriptionManager] Subscribe failed: connection ${connectionId} not found`)
      }
      return null
    }

    // Check subscription limit
    if (connection.subscriptions.size >= this.config.maxSubscriptionsPerConnection) {
      this.sendToConnection(connection, {
        type: 'error',
        error: 'Maximum subscriptions reached',
        code: 'MAX_SUBSCRIPTIONS',
      })
      return null
    }

    const subscription: Subscription = {
      id: generateId('sub'),
      connectionId,
      ns: options.ns,
      filter: options.filter || {},
      ops: options.ops || ['ALL'],
      includeState: options.includeState ?? true,
      lastEventId: options.resumeAfter,
      createdAt: Date.now(),
    }

    connection.subscriptions.set(subscription.id, subscription)
    connection.lastActivity = Date.now()

    // Update stats
    this.stats.totalSubscriptions++
    this.stats.subscriptionsByNs[options.ns] =
      (this.stats.subscriptionsByNs[options.ns] || 0) + 1

    // Send acknowledgment
    this.sendToConnection(connection, {
      type: 'subscribed',
      subscriptionId: subscription.id,
      ns: options.ns,
    })

    if (this.config.debug) {
      logger.debug(`[SubscriptionManager] Subscription created: ${subscription.id} for ns=${options.ns}`)
    }

    return subscription.id
  }

  /**
   * Remove a subscription
   *
   * @param connectionId - ID of the connection
   * @param subscriptionId - ID of the subscription to remove
   * @returns true if removed, false if not found
   */
  unsubscribe(connectionId: string, subscriptionId: string): boolean {
    const connection = this.connections.get(connectionId)
    if (!connection) return false

    const subscription = connection.subscriptions.get(subscriptionId)
    if (!subscription) return false

    connection.subscriptions.delete(subscriptionId)
    connection.lastActivity = Date.now()

    this.removeSubscriptionFromStats(subscription)

    // Send acknowledgment
    this.sendToConnection(connection, {
      type: 'unsubscribed',
      subscriptionId,
    })

    if (this.config.debug) {
      logger.debug(`[SubscriptionManager] Subscription removed: ${subscriptionId}`)
    }

    return true
  }

  private removeSubscriptionFromStats(subscription: Subscription): void {
    this.stats.totalSubscriptions--
    const currentCount = this.stats.subscriptionsByNs[subscription.ns] || 1
    this.stats.subscriptionsByNs[subscription.ns] = currentCount - 1
    if ((this.stats.subscriptionsByNs[subscription.ns] ?? 0) <= 0) {
      delete this.stats.subscriptionsByNs[subscription.ns]
    }
  }

  // ===========================================================================
  // Event Processing
  // ===========================================================================

  /**
   * Set the event source for receiving database events
   *
   * @param source - Event source implementation
   */
  async setEventSource(source: SubscriptionEventSource): Promise<void> {
    // Clean up existing source
    if (this.eventSourceUnsubscribe) {
      this.eventSourceUnsubscribe()
    }
    if (this.eventSource) {
      await this.eventSource.stop()
    }

    this.eventSource = source
    this.eventSourceUnsubscribe = source.onEvent((event) => {
      this.handleEvent(event)
    })

    await source.start()

    if (this.config.debug) {
      logger.debug('[SubscriptionManager] Event source set and started')
    }
  }

  /**
   * Process a database event and distribute to matching subscribers
   *
   * This method can be called directly if not using an event source.
   *
   * @param event - Database event to process
   */
  handleEvent(event: Event): void {
    this.stats.eventsProcessed++

    // Parse the event target to get namespace and entity ID
    let ns: string
    let entityId: string

    try {
      const parsed = parseEntityTarget(event.target)
      ns = parsed.ns
      entityId = parsed.id
    } catch {
      // Skip invalid event targets (e.g., relationship events)
      if (this.config.debug) {
        logger.debug(`[SubscriptionManager] Skipping event with invalid target: ${event.target}`)
      }
      return
    }

    // Convert to ChangeEvent
    const changeEvent: ChangeEvent = {
      id: event.id,
      ts: event.ts,
      op: event.op,
      ns,
      entityId,
      fullId: makeEntityId(ns, entityId),
      before: event.before,
      after: event.after,
      actor: event.actor,
      metadata: event.metadata,
    }

    // Find matching subscriptions and deliver
    let delivered = false

    for (const connection of this.connections.values()) {
      for (const subscription of connection.subscriptions.values()) {
        if (this.matchesSubscription(subscription, changeEvent)) {
          // Create event payload based on includeState flag
          const eventPayload: ChangeEvent = subscription.includeState
            ? changeEvent
            : {
                ...changeEvent,
                before: undefined,
                after: undefined,
              }

          this.sendToConnection(connection, {
            type: 'change',
            data: eventPayload,
          })

          subscription.lastEventId = event.id
          delivered = true
        }
      }
    }

    if (delivered) {
      this.stats.eventsDelivered++
    } else {
      this.stats.eventsFiltered++
    }
  }

  /**
   * Check if an event matches a subscription
   */
  private matchesSubscription(subscription: Subscription, event: ChangeEvent): boolean {
    // Check namespace
    if (subscription.ns !== event.ns) {
      return false
    }

    // Check operation
    if (!subscription.ops.includes('ALL') && !subscription.ops.includes(event.op as SubscriptionOp)) {
      return false
    }

    // Check filter against the after state (or before for DELETE)
    const state = event.after ?? event.before
    if (state && Object.keys(subscription.filter).length > 0) {
      try {
        return matchesFilter(state as Record<string, unknown>, subscription.filter)
      } catch {
        // If filter matching fails, don't match
        return false
      }
    }

    return true
  }

  // ===========================================================================
  // Message Sending
  // ===========================================================================

  private async sendToConnection(connection: Connection, message: SubscriptionMessage): Promise<void> {
    if (!connection.writer.isOpen()) {
      // Connection closed, remove it
      await this.removeConnection(connection.id)
      return
    }

    try {
      await connection.writer.send(message)
    } catch (error) {
      if (this.config.debug) {
        logger.error(`[SubscriptionManager] Failed to send to ${connection.id}:`, error)
      }
      // Remove connection on send failure
      await this.removeConnection(connection.id)
    }
  }

  // ===========================================================================
  // Heartbeat / Keep-alive
  // ===========================================================================

  /**
   * Start the heartbeat timer
   */
  startHeartbeat(): void {
    if (this.heartbeatTimer) return

    this.heartbeatTimer = setInterval(() => {
      const now = Date.now()

      for (const connection of this.connections.values()) {
        // Check for timeout
        if (now - connection.lastActivity > this.config.connectionTimeoutMs) {
          if (this.config.debug) {
            logger.debug(`[SubscriptionManager] Connection timeout: ${connection.id}`)
          }
          // Fire-and-forget with error handling - removeConnection is async but heartbeat is sync
          this.removeConnection(connection.id).catch((err) => {
            logger.error(`[SubscriptionManager] Failed to remove timed-out connection ${connection.id}:`, err)
          })
          continue
        }

        // Send ping - sendToConnection is async, fire-and-forget with internal error handling
        void this.sendToConnection(connection, {
          type: 'pong',
          ts: now,
        })
      }
    }, this.config.heartbeatIntervalMs)

    if (this.config.debug) {
      logger.debug('[SubscriptionManager] Heartbeat started')
    }
  }

  /**
   * Stop the heartbeat timer
   */
  stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = undefined

      if (this.config.debug) {
        logger.debug('[SubscriptionManager] Heartbeat stopped')
      }
    }
  }

  /**
   * Handle a ping from a connection
   */
  handlePing(connectionId: string): void {
    const connection = this.connections.get(connectionId)
    if (connection) {
      connection.lastActivity = Date.now()
      // Fire-and-forget with error handling - sendToConnection handles errors internally
      void this.sendToConnection(connection, {
        type: 'pong',
        ts: Date.now(),
      })
    }
  }

  // ===========================================================================
  // Reconnection Support
  // ===========================================================================

  /**
   * Resume a connection after reconnect
   *
   * @param writer - New transport writer
   * @param state - Previous connection state
   * @returns Resume result
   */
  async resumeConnection(
    writer: SubscriptionWriter,
    state: ReconnectionState
  ): Promise<ResumeResult> {
    // Create new connection
    const connection = this.addConnection(writer)

    const result: ResumeResult = {
      success: true,
      connectionId: connection.id,
      resumedSubscriptions: [],
      failedSubscriptions: [],
    }

    // Re-establish subscriptions
    for (const sub of state.subscriptions) {
      const lastEventId = state.lastEventIds[sub.id]
      const subId = this.subscribe(connection.id, {
        ns: sub.ns,
        filter: sub.filter,
        ops: sub.ops,
        resumeAfter: lastEventId,
      })

      if (subId) {
        result.resumedSubscriptions.push(subId)
      } else {
        result.failedSubscriptions.push({
          id: sub.id,
          reason: 'Failed to subscribe',
        })
      }
    }

    // Note: Missed events are replayed via the resumeAfter parameter on subscribe.
    // If the event source supports historical replay (e.g., from event log),
    // events since lastEventId will be delivered through normal subscription flow.

    return result
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get current subscription statistics
   */
  getStats(): SubscriptionStats {
    return { ...this.stats }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      ...this.stats,
      eventsProcessed: 0,
      eventsDelivered: 0,
      eventsFiltered: 0,
      queueDepth: 0,
    }
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Start the subscription manager
   */
  async start(): Promise<void> {
    this.startHeartbeat()

    if (this.config.debug) {
      logger.debug('[SubscriptionManager] Started')
    }
  }

  /**
   * Stop the subscription manager and clean up resources
   */
  async stop(): Promise<void> {
    this.stopHeartbeat()

    // Stop event source
    if (this.eventSourceUnsubscribe) {
      this.eventSourceUnsubscribe()
    }
    if (this.eventSource) {
      await this.eventSource.stop()
    }

    // Close all connections
    for (const connectionId of this.connections.keys()) {
      await this.removeConnection(connectionId)
    }

    if (this.config.debug) {
      logger.debug('[SubscriptionManager] Stopped')
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a subscription manager
 *
 * @param config - Manager configuration
 * @returns SubscriptionManager instance
 */
export function createSubscriptionManager(
  config?: SubscriptionManagerConfig
): SubscriptionManager {
  return new SubscriptionManager(config)
}

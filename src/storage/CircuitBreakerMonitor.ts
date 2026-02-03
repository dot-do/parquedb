/**
 * CircuitBreakerMonitor - Metrics and monitoring for circuit breakers
 *
 * Provides centralized monitoring of circuit breaker state across all backends.
 * Integrates with the observability system for unified metrics collection.
 *
 * @example
 * ```typescript
 * import { CircuitBreakerMonitor } from './CircuitBreakerMonitor'
 * import { withCircuitBreaker } from './CircuitBreakerBackend'
 *
 * // Create a monitor
 * const monitor = new CircuitBreakerMonitor()
 *
 * // Create backends with monitored circuit breakers
 * const r2 = withCircuitBreaker(r2Backend, {
 *   onStateChange: monitor.createStateChangeHandler('r2'),
 * })
 *
 * // Get health status
 * const health = monitor.getHealthStatus()
 * ```
 */

import { CircuitState, type CircuitBreakerMetrics } from './CircuitBreaker'
import type { CircuitBreakerBackend } from './CircuitBreakerBackend'

// =============================================================================
// Types
// =============================================================================

/**
 * State change event for circuit breakers
 */
export interface CircuitStateChangeEvent {
  /** Name of the circuit breaker */
  name: string
  /** Previous state */
  fromState: CircuitState
  /** New state */
  toState: CircuitState
  /** Timestamp of the change */
  timestamp: number
  /** Reason for the change (if known) */
  reason?: string | undefined
}

/**
 * Health status for a single circuit breaker
 */
export interface CircuitHealth {
  /** Name of the circuit breaker */
  name: string
  /** Current state */
  state: CircuitState
  /** Whether the circuit is healthy (CLOSED) */
  isHealthy: boolean
  /** Current failure count */
  failureCount: number
  /** Total requests */
  totalRequests: number
  /** Success rate (0-1) */
  successRate: number
  /** Time until next recovery attempt (for OPEN state) */
  timeToRecoveryMs?: number | undefined
  /** Last state change timestamp */
  lastStateChangeTime?: number | undefined
}

/**
 * Aggregated health status for all monitored circuit breakers
 */
export interface CircuitBreakerHealthStatus {
  /** Overall health (true if all circuits are CLOSED) */
  healthy: boolean
  /** Number of circuits currently OPEN */
  openCount: number
  /** Number of circuits currently HALF_OPEN */
  halfOpenCount: number
  /** Number of circuits currently CLOSED */
  closedCount: number
  /** Total number of monitored circuits */
  totalCircuits: number
  /** Individual circuit health */
  circuits: Record<string, CircuitHealth>
  /** Recent state change events */
  recentEvents: CircuitStateChangeEvent[]
  /** Timestamp of this status */
  timestamp: number
}

/**
 * Callback for state change notifications
 */
export type CircuitStateChangeCallback = (event: CircuitStateChangeEvent) => void

// =============================================================================
// CircuitBreakerMonitor Implementation
// =============================================================================

/**
 * Centralized monitor for circuit breaker health and metrics
 */
export class CircuitBreakerMonitor {
  private backends: Map<string, CircuitBreakerBackend> = new Map()
  private stateChangeHistory: CircuitStateChangeEvent[] = []
  private maxHistorySize: number
  private stateChangeCallbacks: CircuitStateChangeCallback[] = []

  constructor(options?: { maxHistorySize?: number | undefined }) {
    this.maxHistorySize = options?.maxHistorySize ?? 100
  }

  // ===========================================================================
  // Backend Registration
  // ===========================================================================

  /**
   * Register a circuit breaker backend for monitoring
   *
   * @param name - Name for this backend (e.g., 'r2-primary', 'remote-api')
   * @param backend - The CircuitBreakerBackend to monitor
   */
  register(name: string, backend: CircuitBreakerBackend): void {
    this.backends.set(name, backend)
  }

  /**
   * Unregister a backend
   */
  unregister(name: string): void {
    this.backends.delete(name)
  }

  /**
   * Get all registered backend names
   */
  getRegisteredBackends(): string[] {
    return Array.from(this.backends.keys())
  }

  // ===========================================================================
  // State Change Handler
  // ===========================================================================

  /**
   * Create a state change handler for use with CircuitBreakerOptions
   *
   * @param backendName - Name to identify this backend in events
   * @returns State change callback compatible with CircuitBreakerOptions
   *
   * @example
   * ```typescript
   * const monitor = new CircuitBreakerMonitor()
   * const backend = withCircuitBreaker(r2, {
   *   onStateChange: monitor.createStateChangeHandler('r2'),
   * })
   * ```
   */
  createStateChangeHandler(backendName: string): (from: CircuitState, to: CircuitState, circuitName?: string) => void {
    return (from: CircuitState, to: CircuitState, circuitName?: string) => {
      const event: CircuitStateChangeEvent = {
        name: circuitName ? `${backendName}:${circuitName}` : backendName,
        fromState: from,
        toState: to,
        timestamp: Date.now(),
        reason: this.determineReason(from, to),
      }

      // Add to history
      this.stateChangeHistory.push(event)
      if (this.stateChangeHistory.length > this.maxHistorySize) {
        this.stateChangeHistory.shift()
      }

      // Notify callbacks
      for (const callback of this.stateChangeCallbacks) {
        try {
          callback(event)
        } catch {
          // Ignore callback errors
        }
      }
    }
  }

  /**
   * Determine the reason for a state change
   */
  private determineReason(from: CircuitState, to: CircuitState): string {
    if (from === CircuitState.CLOSED && to === CircuitState.OPEN) {
      return 'Failure threshold exceeded'
    }
    if (from === CircuitState.OPEN && to === CircuitState.HALF_OPEN) {
      return 'Recovery timeout elapsed'
    }
    if (from === CircuitState.HALF_OPEN && to === CircuitState.CLOSED) {
      return 'Recovery successful'
    }
    if (from === CircuitState.HALF_OPEN && to === CircuitState.OPEN) {
      return 'Recovery failed'
    }
    return 'Manual state change'
  }

  // ===========================================================================
  // State Change Notifications
  // ===========================================================================

  /**
   * Subscribe to state change events
   *
   * @param callback - Function to call on state changes
   * @returns Unsubscribe function
   */
  onStateChange(callback: CircuitStateChangeCallback): () => void {
    this.stateChangeCallbacks.push(callback)
    return () => {
      const index = this.stateChangeCallbacks.indexOf(callback)
      if (index > -1) {
        this.stateChangeCallbacks.splice(index, 1)
      }
    }
  }

  // ===========================================================================
  // Health Status
  // ===========================================================================

  /**
   * Get health status for all monitored circuit breakers
   */
  getHealthStatus(): CircuitBreakerHealthStatus {
    const circuits: Record<string, CircuitHealth> = {}
    let openCount = 0
    let halfOpenCount = 0
    let closedCount = 0

    for (const [name, backend] of this.backends) {
      const metrics = backend.getMetrics()

      // Track read circuit
      const readHealth = this.metricsToHealth(`${name}:read`, metrics.read)
      circuits[`${name}:read`] = readHealth
      this.countState(readHealth.state, { openCount, halfOpenCount, closedCount }, (counts) => {
        openCount = counts.openCount
        halfOpenCount = counts.halfOpenCount
        closedCount = counts.closedCount
      })

      // Track write circuit (if separate)
      if (metrics.write !== metrics.read) {
        const writeHealth = this.metricsToHealth(`${name}:write`, metrics.write)
        circuits[`${name}:write`] = writeHealth
        this.countState(writeHealth.state, { openCount, halfOpenCount, closedCount }, (counts) => {
          openCount = counts.openCount
          halfOpenCount = counts.halfOpenCount
          closedCount = counts.closedCount
        })
      }
    }

    const totalCircuits = Object.keys(circuits).length

    return {
      healthy: openCount === 0 && halfOpenCount === 0,
      openCount,
      halfOpenCount,
      closedCount,
      totalCircuits,
      circuits,
      recentEvents: [...this.stateChangeHistory].slice(-20),
      timestamp: Date.now(),
    }
  }

  /**
   * Convert circuit breaker metrics to health status
   */
  private metricsToHealth(name: string, metrics: CircuitBreakerMetrics): CircuitHealth {
    const successRate = metrics.totalRequests > 0
      ? metrics.totalSuccesses / metrics.totalRequests
      : 1

    return {
      name,
      state: metrics.state,
      isHealthy: metrics.state === CircuitState.CLOSED,
      failureCount: metrics.failureCount,
      totalRequests: metrics.totalRequests,
      successRate,
      lastStateChangeTime: metrics.lastStateChangeTime,
    }
  }

  /**
   * Count states
   */
  private countState(
    state: CircuitState,
    counts: { openCount: number; halfOpenCount: number; closedCount: number },
    update: (counts: { openCount: number; halfOpenCount: number; closedCount: number }) => void
  ): void {
    switch (state) {
      case CircuitState.OPEN:
        update({ ...counts, openCount: counts.openCount + 1 })
        break
      case CircuitState.HALF_OPEN:
        update({ ...counts, halfOpenCount: counts.halfOpenCount + 1 })
        break
      case CircuitState.CLOSED:
        update({ ...counts, closedCount: counts.closedCount + 1 })
        break
    }
  }

  /**
   * Get health for a specific backend
   */
  getBackendHealth(name: string): CircuitHealth | null {
    const backend = this.backends.get(name)
    if (!backend) {
      return null
    }

    const metrics = backend.getMetrics()
    return this.metricsToHealth(name, metrics.read)
  }

  /**
   * Check if any circuit breaker is currently open
   */
  hasOpenCircuits(): boolean {
    for (const backend of this.backends.values()) {
      const metrics = backend.getMetrics()
      if (metrics.read.state === CircuitState.OPEN || metrics.write.state === CircuitState.OPEN) {
        return true
      }
    }
    return false
  }

  // ===========================================================================
  // State Change History
  // ===========================================================================

  /**
   * Get recent state change events
   *
   * @param limit - Maximum number of events to return (default: 20)
   */
  getRecentEvents(limit: number = 20): CircuitStateChangeEvent[] {
    return [...this.stateChangeHistory].slice(-limit)
  }

  /**
   * Get events for a specific backend
   */
  getBackendEvents(backendName: string, limit: number = 20): CircuitStateChangeEvent[] {
    return this.stateChangeHistory
      .filter(e => e.name.startsWith(backendName))
      .slice(-limit)
  }

  /**
   * Clear event history
   */
  clearHistory(): void {
    this.stateChangeHistory = []
  }

  // ===========================================================================
  // Bulk Operations
  // ===========================================================================

  /**
   * Reset all circuit breakers to CLOSED state
   */
  resetAll(): void {
    for (const backend of this.backends.values()) {
      backend.reset()
    }
  }

  /**
   * Get aggregated metrics across all backends
   */
  getAggregatedMetrics(): {
    totalRequests: number
    totalSuccesses: number
    totalFailures: number
    overallSuccessRate: number
  } {
    let totalRequests = 0
    let totalSuccesses = 0
    let totalFailures = 0

    for (const backend of this.backends.values()) {
      const metrics = backend.getMetrics()
      totalRequests += metrics.read.totalRequests + metrics.write.totalRequests
      totalSuccesses += metrics.read.totalSuccesses + metrics.write.totalSuccesses
      totalFailures += metrics.read.totalFailures + metrics.write.totalFailures
    }

    return {
      totalRequests,
      totalSuccesses,
      totalFailures,
      overallSuccessRate: totalRequests > 0 ? totalSuccesses / totalRequests : 1,
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a circuit breaker monitor
 */
export function createCircuitBreakerMonitor(options?: { maxHistorySize?: number | undefined }): CircuitBreakerMonitor {
  return new CircuitBreakerMonitor(options)
}

// =============================================================================
// Global Monitor Instance
// =============================================================================

/**
 * Global circuit breaker monitor instance
 *
 * Use this for application-wide circuit breaker monitoring.
 */
export const globalCircuitBreakerMonitor = new CircuitBreakerMonitor()

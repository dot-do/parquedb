/**
 * CircuitBreaker - Implements the circuit breaker pattern for resilient external calls
 *
 * The circuit breaker pattern prevents repeated calls to failing services,
 * allowing them time to recover while providing fast failure responses.
 *
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Failure threshold exceeded, requests fail fast
 * - HALF_OPEN: Testing if service recovered, limited requests allowed
 *
 * @example
 * ```typescript
 * const breaker = new CircuitBreaker({
 *   failureThreshold: 5,
 *   resetTimeoutMs: 30000,
 * })
 *
 * // Execute operation with circuit breaker protection
 * const result = await breaker.execute(() => externalService.call())
 * ```
 */

import {
  DEFAULT_FAILURE_THRESHOLD,
  DEFAULT_SUCCESS_THRESHOLD,
  DEFAULT_CIRCUIT_RESET_TIMEOUT_MS,
  DEFAULT_FAILURE_WINDOW_MS,
  FAST_CIRCUIT_RESET_TIMEOUT_MS,
  FAST_FAILURE_WINDOW_MS,
  SLOW_CIRCUIT_RESET_TIMEOUT_MS,
  SLOW_FAILURE_WINDOW_MS,
} from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Circuit breaker states
 */
export enum CircuitState {
  /** Normal operation - requests pass through */
  CLOSED = 'CLOSED',
  /** Failure threshold exceeded - requests fail fast */
  OPEN = 'OPEN',
  /** Testing recovery - limited requests allowed */
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * Configuration options for CircuitBreaker
 */
export interface CircuitBreakerOptions {
  /**
   * Number of consecutive failures before opening the circuit
   * @default 5
   */
  failureThreshold?: number

  /**
   * Number of consecutive successes in HALF_OPEN state before closing
   * @default 2
   */
  successThreshold?: number

  /**
   * Time in milliseconds before attempting to recover (OPEN -> HALF_OPEN)
   * @default 30000 (30 seconds)
   */
  resetTimeoutMs?: number

  /**
   * Optional name for logging and metrics
   */
  name?: string

  /**
   * Callback when circuit state changes
   */
  onStateChange?: (from: CircuitState, to: CircuitState, name?: string) => void

  /**
   * Function to determine if an error should count as a failure
   * By default, all errors count as failures except NotFoundError
   * @default (error) => !isNotFoundError(error)
   */
  isFailure?: (error: Error) => boolean

  /**
   * Time window in milliseconds for counting failures
   * Failures older than this window are ignored
   * @default 60000 (60 seconds)
   */
  failureWindowMs?: number
}

/**
 * Circuit breaker metrics
 */
export interface CircuitBreakerMetrics {
  /** Current circuit state */
  state: CircuitState
  /** Number of failures in the current window */
  failureCount: number
  /** Number of consecutive successes in HALF_OPEN */
  successCount: number
  /** Total number of requests */
  totalRequests: number
  /** Total number of failures */
  totalFailures: number
  /** Total number of successful requests */
  totalSuccesses: number
  /** Time when circuit last opened */
  lastFailureTime?: number
  /** Time when circuit last transitioned to HALF_OPEN */
  lastHalfOpenTime?: number
  /** Time of the last state change */
  lastStateChangeTime?: number
}

// =============================================================================
// Errors
// =============================================================================

/**
 * Error thrown when circuit breaker is open
 */
export class CircuitOpenError extends Error {
  override readonly name = 'CircuitOpenError'
  readonly circuitName?: string
  readonly remainingMs: number

  constructor(circuitName?: string, remainingMs: number = 0) {
    const nameStr = circuitName ? ` (${circuitName})` : ''
    super(`Circuit breaker is open${nameStr}. Retry after ${remainingMs}ms.`)
    Object.setPrototypeOf(this, CircuitOpenError.prototype)
    this.circuitName = circuitName
    this.remainingMs = remainingMs
  }
}

// =============================================================================
// CircuitBreaker Implementation
// =============================================================================

/**
 * Circuit breaker for protecting against cascading failures
 *
 * Implements the classic circuit breaker pattern with three states:
 * 1. CLOSED - Normal operation, calls pass through
 * 2. OPEN - After failure threshold, calls fail immediately
 * 3. HALF_OPEN - After timeout, test calls to see if service recovered
 */
export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED
  private failureCount = 0
  private successCount = 0
  private lastFailureTime?: number
  private lastHalfOpenTime?: number
  private lastStateChangeTime = Date.now()
  private failureTimestamps: number[] = []

  // Metrics
  private totalRequests = 0
  private totalFailures = 0
  private totalSuccesses = 0

  // Configuration with defaults
  private readonly failureThreshold: number
  private readonly successThreshold: number
  private readonly resetTimeoutMs: number
  private readonly name?: string
  private readonly onStateChange?: (from: CircuitState, to: CircuitState, name?: string) => void
  private readonly isFailure: (error: Error) => boolean
  private readonly failureWindowMs: number

  constructor(options: CircuitBreakerOptions = {}) {
    this.failureThreshold = options.failureThreshold ?? DEFAULT_FAILURE_THRESHOLD
    this.successThreshold = options.successThreshold ?? DEFAULT_SUCCESS_THRESHOLD
    this.resetTimeoutMs = options.resetTimeoutMs ?? DEFAULT_CIRCUIT_RESET_TIMEOUT_MS
    this.name = options.name
    this.onStateChange = options.onStateChange
    this.failureWindowMs = options.failureWindowMs ?? DEFAULT_FAILURE_WINDOW_MS

    // Default isFailure: all errors except "not found" type errors
    this.isFailure = options.isFailure ?? ((error: Error) => {
      const errorName = error.name?.toLowerCase() || ''
      const errorMessage = error.message?.toLowerCase() || ''
      return !errorName.includes('notfound') &&
             !errorName.includes('not_found') &&
             !errorMessage.includes('not found') &&
             !errorMessage.includes('file not found')
    })
  }

  /**
   * Execute an operation with circuit breaker protection
   *
   * @param operation - Async operation to execute
   * @returns Result of the operation
   * @throws CircuitOpenError if circuit is open
   * @throws Original error if operation fails
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    this.totalRequests++

    // Check if we should allow the request
    if (!this.shouldAllowRequest()) {
      const remainingMs = this.getRemainingTimeoutMs()
      throw new CircuitOpenError(this.name, remainingMs)
    }

    try {
      const result = await operation()
      this.recordSuccess()
      return result
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      this.recordFailure(err)
      throw error
    }
  }

  /**
   * Check if circuit breaker should allow a request
   */
  private shouldAllowRequest(): boolean {
    const now = Date.now()

    switch (this.state) {
      case CircuitState.CLOSED:
        return true

      case CircuitState.OPEN:
        // Check if reset timeout has elapsed
        if (this.lastFailureTime && now - this.lastFailureTime >= this.resetTimeoutMs) {
          this.transitionTo(CircuitState.HALF_OPEN)
          this.lastHalfOpenTime = now
          return true
        }
        return false

      case CircuitState.HALF_OPEN:
        // Allow limited requests to test recovery
        return true

      default:
        return true
    }
  }

  /**
   * Record a successful operation
   */
  private recordSuccess(): void {
    this.totalSuccesses++

    switch (this.state) {
      case CircuitState.CLOSED:
        // Reset failure count on success in closed state
        this.failureCount = 0
        this.failureTimestamps = []
        break

      case CircuitState.HALF_OPEN:
        this.successCount++
        if (this.successCount >= this.successThreshold) {
          // Enough successes - close the circuit
          this.transitionTo(CircuitState.CLOSED)
          this.failureCount = 0
          this.successCount = 0
          this.failureTimestamps = []
        }
        break

      case CircuitState.OPEN:
        // Should not happen, but handle gracefully
        break
    }
  }

  /**
   * Record a failed operation
   */
  private recordFailure(error: Error): void {
    // Check if this error should be counted as a failure
    if (!this.isFailure(error)) {
      return
    }

    this.totalFailures++
    const now = Date.now()

    switch (this.state) {
      case CircuitState.CLOSED:
        // Add failure timestamp and clean old ones
        this.failureTimestamps.push(now)
        this.failureTimestamps = this.failureTimestamps.filter(
          ts => now - ts < this.failureWindowMs
        )
        this.failureCount = this.failureTimestamps.length
        this.lastFailureTime = now

        if (this.failureCount >= this.failureThreshold) {
          this.transitionTo(CircuitState.OPEN)
        }
        break

      case CircuitState.HALF_OPEN:
        // Any failure in half-open state opens the circuit again
        this.lastFailureTime = now
        this.successCount = 0
        this.transitionTo(CircuitState.OPEN)
        break

      case CircuitState.OPEN:
        // Already open, update last failure time
        this.lastFailureTime = now
        break
    }
  }

  /**
   * Transition to a new state
   */
  private transitionTo(newState: CircuitState): void {
    const oldState = this.state
    if (oldState === newState) return

    this.state = newState
    this.lastStateChangeTime = Date.now()

    if (this.onStateChange) {
      this.onStateChange(oldState, newState, this.name)
    }
  }

  /**
   * Get remaining time until circuit breaker attempts recovery
   */
  private getRemainingTimeoutMs(): number {
    if (this.state !== CircuitState.OPEN || !this.lastFailureTime) {
      return 0
    }
    const elapsed = Date.now() - this.lastFailureTime
    return Math.max(0, this.resetTimeoutMs - elapsed)
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Get current circuit state
   */
  getState(): CircuitState {
    // Check if OPEN state should transition to HALF_OPEN
    if (this.state === CircuitState.OPEN) {
      const now = Date.now()
      if (this.lastFailureTime && now - this.lastFailureTime >= this.resetTimeoutMs) {
        this.transitionTo(CircuitState.HALF_OPEN)
        this.lastHalfOpenTime = now
      }
    }
    return this.state
  }

  /**
   * Get circuit breaker metrics
   */
  getMetrics(): CircuitBreakerMetrics {
    return {
      state: this.getState(),
      failureCount: this.failureCount,
      successCount: this.successCount,
      totalRequests: this.totalRequests,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses,
      lastFailureTime: this.lastFailureTime,
      lastHalfOpenTime: this.lastHalfOpenTime,
      lastStateChangeTime: this.lastStateChangeTime,
    }
  }

  /**
   * Manually reset the circuit breaker to CLOSED state
   */
  reset(): void {
    this.transitionTo(CircuitState.CLOSED)
    this.failureCount = 0
    this.successCount = 0
    this.failureTimestamps = []
    this.lastFailureTime = undefined
    this.lastHalfOpenTime = undefined
  }

  /**
   * Manually trip the circuit breaker to OPEN state
   */
  trip(): void {
    this.lastFailureTime = Date.now()
    this.transitionTo(CircuitState.OPEN)
  }

  /**
   * Check if circuit is currently allowing requests
   */
  isAllowingRequests(): boolean {
    return this.shouldAllowRequest()
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a circuit breaker with default settings for storage backends
 */
export function createStorageCircuitBreaker(
  name?: string,
  onStateChange?: (from: CircuitState, to: CircuitState, name?: string) => void
): CircuitBreaker {
  return new CircuitBreaker({
    name,
    failureThreshold: DEFAULT_FAILURE_THRESHOLD,
    successThreshold: DEFAULT_SUCCESS_THRESHOLD,
    resetTimeoutMs: DEFAULT_CIRCUIT_RESET_TIMEOUT_MS,
    failureWindowMs: DEFAULT_FAILURE_WINDOW_MS,
    onStateChange,
    // Don't count "not found" errors as failures
    isFailure: (error) => {
      const errorName = error.name?.toLowerCase() || ''
      return !errorName.includes('notfound') && !errorName.includes('not_found')
    },
  })
}

/**
 * Create a circuit breaker with aggressive settings for latency-sensitive operations
 */
export function createFastFailCircuitBreaker(
  name?: string,
  onStateChange?: (from: CircuitState, to: CircuitState, name?: string) => void
): CircuitBreaker {
  return new CircuitBreaker({
    name,
    failureThreshold: 3,
    successThreshold: 1,
    resetTimeoutMs: FAST_CIRCUIT_RESET_TIMEOUT_MS,
    failureWindowMs: FAST_FAILURE_WINDOW_MS,
    onStateChange,
  })
}

/**
 * Create a circuit breaker with conservative settings for critical operations
 */
export function createConservativeCircuitBreaker(
  name?: string,
  onStateChange?: (from: CircuitState, to: CircuitState, name?: string) => void
): CircuitBreaker {
  return new CircuitBreaker({
    name,
    failureThreshold: 10,
    successThreshold: 5,
    resetTimeoutMs: SLOW_CIRCUIT_RESET_TIMEOUT_MS,
    failureWindowMs: SLOW_FAILURE_WINDOW_MS,
    onStateChange,
  })
}

/**
 * Backpressure Manager for Compaction Queue Consumer
 *
 * Implements protection mechanisms for when compaction is overwhelmed:
 *
 * 1. **Token Bucket Rate Limiting** - Controls dispatches per second
 * 2. **Circuit Breaker for Workflows** - Prevents cascading failures
 * 3. **Backpressure Detection** - Checks DO status before dispatching
 * 4. **Adaptive Batch Sizing** - Prioritizes namespaces under load
 *
 * @example
 * ```typescript
 * const manager = new BackpressureManager({
 *   dispatchesPerSecond: 100,
 *   backpressureThreshold: 50,
 * })
 *
 * // Check before dispatching
 * if (await manager.canDispatch()) {
 *   const result = await manager.executeWorkflowCreate(() => workflow.create(params))
 * }
 * ```
 */

import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for BackpressureManager
 */
export interface BackpressureConfig {
  /**
   * Maximum DO dispatches per second allowed
   * @default 100
   */
  dispatchesPerSecond?: number

  /**
   * Time window for rate limiting in milliseconds
   * @default 1000 (1 second)
   */
  rateLimitWindowMs?: number

  /**
   * Number of pending windows that triggers backpressure
   * @default 50
   */
  backpressureThreshold?: number

  /**
   * Consecutive workflow failures before circuit opens
   * @default 5
   */
  circuitBreakerThreshold?: number

  /**
   * Base delay in ms when circuit is open
   * @default 5000 (5 seconds)
   */
  circuitBreakerBaseDelayMs?: number

  /**
   * Maximum delay in ms for exponential backoff
   * @default 60000 (60 seconds)
   */
  circuitBreakerMaxDelayMs?: number

  /**
   * Time window for counting failures in ms
   * @default 60000 (60 seconds)
   */
  failureWindowMs?: number

  /**
   * Namespaces considered high-priority (processed even under backpressure)
   * @default []
   */
  highPriorityNamespaces?: string[]
}

/**
 * Status of the backpressure system
 */
export interface BackpressureStatus {
  /** Whether rate limiting is currently active */
  rateLimitActive: boolean

  /** Tokens remaining in the current window */
  tokensRemaining: number

  /** Whether circuit breaker is open */
  circuitBreakerOpen: boolean

  /** Time until circuit breaker resets (0 if closed) */
  circuitBreakerResetMs: number

  /** Number of consecutive failures */
  consecutiveFailures: number

  /** Whether backpressure signal is active from DO */
  backpressureSignalActive: boolean

  /** Total dispatches in current window */
  dispatchesInWindow: number

  /** Window reset time */
  windowResetAt: number
}

/**
 * Result of a workflow execution attempt
 */
export interface WorkflowExecutionResult<T> {
  success: boolean
  result?: T
  error?: Error
  skipped?: boolean
  reason?: 'rate_limited' | 'circuit_open' | 'backpressure'
}

/**
 * Token bucket state for rate limiting
 */
interface TokenBucketState {
  /** Current number of tokens available */
  tokens: number

  /** Last time tokens were refilled */
  lastRefillTime: number

  /** Dispatch timestamps for sliding window */
  dispatchTimestamps: number[]
}

/**
 * Circuit breaker state
 */
interface CircuitBreakerState {
  /** Whether circuit is currently open */
  isOpen: boolean

  /** Number of consecutive failures */
  consecutiveFailures: number

  /** Failure timestamps for windowed counting */
  failureTimestamps: number[]

  /** Time when circuit opened */
  openedAt?: number

  /** Current backoff multiplier for exponential backoff */
  backoffMultiplier: number
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_DISPATCHES_PER_SECOND = 100
const DEFAULT_RATE_LIMIT_WINDOW_MS = 1000
const DEFAULT_BACKPRESSURE_THRESHOLD = 50
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
const DEFAULT_CIRCUIT_BREAKER_BASE_DELAY_MS = 5000
const DEFAULT_CIRCUIT_BREAKER_MAX_DELAY_MS = 60000
const DEFAULT_FAILURE_WINDOW_MS = 60000

// =============================================================================
// BackpressureManager
// =============================================================================

/**
 * Manages backpressure and rate limiting for compaction queue consumer
 *
 * Provides:
 * - Token bucket rate limiting for DO dispatches
 * - Circuit breaker for workflow creation failures
 * - Backpressure detection from CompactionStateDO
 * - Adaptive batch sizing for high-priority namespaces
 */
export class BackpressureManager {
  // Configuration
  private readonly dispatchesPerSecond: number
  private readonly rateLimitWindowMs: number
  private readonly backpressureThreshold: number
  private readonly circuitBreakerThreshold: number
  private readonly circuitBreakerBaseDelayMs: number
  private readonly circuitBreakerMaxDelayMs: number
  private readonly failureWindowMs: number
  private readonly highPriorityNamespaces: Set<string>

  // State
  private tokenBucket: TokenBucketState
  private circuitBreaker: CircuitBreakerState
  private backpressureActive: boolean = false

  constructor(config: BackpressureConfig = {}) {
    this.dispatchesPerSecond = config.dispatchesPerSecond ?? DEFAULT_DISPATCHES_PER_SECOND
    this.rateLimitWindowMs = config.rateLimitWindowMs ?? DEFAULT_RATE_LIMIT_WINDOW_MS
    this.backpressureThreshold = config.backpressureThreshold ?? DEFAULT_BACKPRESSURE_THRESHOLD
    this.circuitBreakerThreshold = config.circuitBreakerThreshold ?? DEFAULT_CIRCUIT_BREAKER_THRESHOLD
    this.circuitBreakerBaseDelayMs = config.circuitBreakerBaseDelayMs ?? DEFAULT_CIRCUIT_BREAKER_BASE_DELAY_MS
    this.circuitBreakerMaxDelayMs = config.circuitBreakerMaxDelayMs ?? DEFAULT_CIRCUIT_BREAKER_MAX_DELAY_MS
    this.failureWindowMs = config.failureWindowMs ?? DEFAULT_FAILURE_WINDOW_MS
    this.highPriorityNamespaces = new Set(config.highPriorityNamespaces ?? [])

    // Initialize token bucket
    this.tokenBucket = {
      tokens: this.dispatchesPerSecond,
      lastRefillTime: Date.now(),
      dispatchTimestamps: [],
    }

    // Initialize circuit breaker
    this.circuitBreaker = {
      isOpen: false,
      consecutiveFailures: 0,
      failureTimestamps: [],
      backoffMultiplier: 1,
    }
  }

  // ===========================================================================
  // Token Bucket Rate Limiting
  // ===========================================================================

  /**
   * Refill tokens based on elapsed time (sliding window)
   */
  private refillTokens(): void {
    const now = Date.now()

    // Clean up old timestamps outside the window
    this.tokenBucket.dispatchTimestamps = this.tokenBucket.dispatchTimestamps.filter(
      ts => now - ts < this.rateLimitWindowMs
    )

    // Calculate available tokens based on remaining capacity
    const usedTokens = this.tokenBucket.dispatchTimestamps.length
    this.tokenBucket.tokens = Math.max(0, this.dispatchesPerSecond - usedTokens)
    this.tokenBucket.lastRefillTime = now
  }

  /**
   * Try to consume a token for rate limiting
   * @returns true if token was consumed, false if rate limited
   */
  private tryConsumeToken(): boolean {
    this.refillTokens()

    if (this.tokenBucket.tokens > 0) {
      this.tokenBucket.tokens--
      this.tokenBucket.dispatchTimestamps.push(Date.now())
      return true
    }

    return false
  }

  /**
   * Check if dispatch is currently rate limited
   */
  isRateLimited(): boolean {
    this.refillTokens()
    return this.tokenBucket.tokens <= 0
  }

  /**
   * Get estimated delay until next token is available
   */
  getDelayUntilToken(): number {
    if (!this.isRateLimited()) {
      return 0
    }

    // Find the oldest timestamp that will expire
    const now = Date.now()
    const oldestTimestamp = Math.min(...this.tokenBucket.dispatchTimestamps)
    const delay = this.rateLimitWindowMs - (now - oldestTimestamp)
    return Math.max(0, delay)
  }

  // ===========================================================================
  // Circuit Breaker
  // ===========================================================================

  /**
   * Clean up old failures outside the window
   */
  private cleanupFailures(): void {
    const now = Date.now()
    this.circuitBreaker.failureTimestamps = this.circuitBreaker.failureTimestamps.filter(
      ts => now - ts < this.failureWindowMs
    )
  }

  /**
   * Check if circuit breaker should remain open
   */
  private shouldCircuitRemainOpen(): boolean {
    if (!this.circuitBreaker.isOpen || !this.circuitBreaker.openedAt) {
      return false
    }

    const now = Date.now()
    const delayMs = Math.min(
      this.circuitBreakerBaseDelayMs * this.circuitBreaker.backoffMultiplier,
      this.circuitBreakerMaxDelayMs
    )

    return now - this.circuitBreaker.openedAt < delayMs
  }

  /**
   * Record a workflow creation failure
   */
  recordFailure(): void {
    const now = Date.now()
    this.circuitBreaker.consecutiveFailures++
    this.circuitBreaker.failureTimestamps.push(now)
    this.cleanupFailures()

    // Check if we should open the circuit
    if (this.circuitBreaker.consecutiveFailures >= this.circuitBreakerThreshold) {
      if (!this.circuitBreaker.isOpen) {
        logger.warn('Circuit breaker opened', {
          consecutiveFailures: this.circuitBreaker.consecutiveFailures,
          failuresInWindow: this.circuitBreaker.failureTimestamps.length,
          backoffMultiplier: this.circuitBreaker.backoffMultiplier,
        })
      }
      this.circuitBreaker.isOpen = true
      this.circuitBreaker.openedAt = now
      // Increase backoff for next time (exponential backoff)
      this.circuitBreaker.backoffMultiplier = Math.min(
        this.circuitBreaker.backoffMultiplier * 2,
        this.circuitBreakerMaxDelayMs / this.circuitBreakerBaseDelayMs
      )
    }
  }

  /**
   * Record a workflow creation success
   */
  recordSuccess(): void {
    this.circuitBreaker.consecutiveFailures = 0

    // If circuit was open, close it and reduce backoff
    if (this.circuitBreaker.isOpen) {
      logger.info('Circuit breaker closed after successful operation')
      this.circuitBreaker.isOpen = false
      this.circuitBreaker.openedAt = undefined
      // Reduce backoff multiplier on success (but not below 1)
      this.circuitBreaker.backoffMultiplier = Math.max(1, this.circuitBreaker.backoffMultiplier / 2)
    }
  }

  /**
   * Check if circuit breaker is currently open
   */
  isCircuitOpen(): boolean {
    // Check if circuit should auto-close after delay
    if (this.circuitBreaker.isOpen && !this.shouldCircuitRemainOpen()) {
      logger.info('Circuit breaker transitioning to half-open (testing)')
      // Allow one request through to test if service recovered
      return false
    }
    return this.circuitBreaker.isOpen
  }

  /**
   * Get time remaining until circuit breaker resets
   */
  getCircuitResetTime(): number {
    if (!this.circuitBreaker.isOpen || !this.circuitBreaker.openedAt) {
      return 0
    }

    const delayMs = Math.min(
      this.circuitBreakerBaseDelayMs * this.circuitBreaker.backoffMultiplier,
      this.circuitBreakerMaxDelayMs
    )
    const elapsed = Date.now() - this.circuitBreaker.openedAt
    return Math.max(0, delayMs - elapsed)
  }

  // ===========================================================================
  // Backpressure Detection
  // ===========================================================================

  /**
   * Update backpressure state from CompactionStateDO status
   * @param pendingWindows Number of pending windows from DO status
   */
  updateBackpressureFromStatus(pendingWindows: number): void {
    const wasActive = this.backpressureActive
    this.backpressureActive = pendingWindows > this.backpressureThreshold

    if (this.backpressureActive && !wasActive) {
      logger.warn('Backpressure activated', {
        pendingWindows,
        threshold: this.backpressureThreshold,
      })
    } else if (!this.backpressureActive && wasActive) {
      logger.info('Backpressure deactivated', {
        pendingWindows,
        threshold: this.backpressureThreshold,
      })
    }
  }

  /**
   * Set backpressure state directly (e.g., from DO response)
   */
  setBackpressureActive(active: boolean): void {
    this.backpressureActive = active
  }

  /**
   * Check if backpressure is currently active
   */
  isBackpressureActive(): boolean {
    return this.backpressureActive
  }

  // ===========================================================================
  // Adaptive Batch Sizing
  // ===========================================================================

  /**
   * Check if a namespace is high-priority
   */
  isHighPriority(namespace: string): boolean {
    return this.highPriorityNamespaces.has(namespace)
  }

  /**
   * Filter namespaces based on current load conditions
   * Under backpressure, only high-priority namespaces are processed
   *
   * @param namespaces List of namespaces to filter
   * @returns Filtered list of namespaces to process
   */
  filterNamespacesForProcessing(namespaces: string[]): string[] {
    if (!this.backpressureActive) {
      return namespaces
    }

    // Under backpressure, only process high-priority namespaces
    const filtered = namespaces.filter(ns => this.highPriorityNamespaces.has(ns))

    if (filtered.length < namespaces.length) {
      logger.debug('Filtered namespaces due to backpressure', {
        original: namespaces.length,
        filtered: filtered.length,
        skipped: namespaces.length - filtered.length,
      })
    }

    return filtered
  }

  // ===========================================================================
  // Main API
  // ===========================================================================

  /**
   * Check if a dispatch operation can proceed
   *
   * @param namespace Optional namespace to check priority
   * @returns true if dispatch can proceed
   */
  canDispatch(namespace?: string): boolean {
    // High-priority namespaces bypass backpressure (but not rate limits or circuit breaker)
    const bypassBackpressure = namespace ? this.isHighPriority(namespace) : false

    // Check circuit breaker first
    if (this.isCircuitOpen()) {
      return false
    }

    // Check rate limiting
    if (this.isRateLimited()) {
      return false
    }

    // Check backpressure (unless bypassed)
    if (!bypassBackpressure && this.isBackpressureActive()) {
      return false
    }

    return true
  }

  /**
   * Execute a workflow creation with backpressure protection
   *
   * @param createFn Function that creates the workflow
   * @param namespace Optional namespace for priority checking
   * @returns Result of the execution attempt
   */
  async executeWorkflowCreate<T>(
    createFn: () => Promise<T>,
    namespace?: string
  ): Promise<WorkflowExecutionResult<T>> {
    // Check if we can dispatch
    if (this.isCircuitOpen()) {
      return {
        success: false,
        skipped: true,
        reason: 'circuit_open',
      }
    }

    if (this.isRateLimited()) {
      return {
        success: false,
        skipped: true,
        reason: 'rate_limited',
      }
    }

    const bypassBackpressure = namespace ? this.isHighPriority(namespace) : false
    if (!bypassBackpressure && this.isBackpressureActive()) {
      return {
        success: false,
        skipped: true,
        reason: 'backpressure',
      }
    }

    // Consume a token for rate limiting
    if (!this.tryConsumeToken()) {
      return {
        success: false,
        skipped: true,
        reason: 'rate_limited',
      }
    }

    // Execute the workflow creation
    try {
      const result = await createFn()
      this.recordSuccess()
      return {
        success: true,
        result,
      }
    } catch (error) {
      this.recordFailure()
      return {
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
      }
    }
  }

  /**
   * Get current backpressure status
   */
  getStatus(): BackpressureStatus {
    this.refillTokens()
    this.cleanupFailures()

    return {
      rateLimitActive: this.tokenBucket.tokens <= 0,
      tokensRemaining: this.tokenBucket.tokens,
      circuitBreakerOpen: this.circuitBreaker.isOpen,
      circuitBreakerResetMs: this.getCircuitResetTime(),
      consecutiveFailures: this.circuitBreaker.consecutiveFailures,
      backpressureSignalActive: this.backpressureActive,
      dispatchesInWindow: this.tokenBucket.dispatchTimestamps.length,
      windowResetAt: this.tokenBucket.lastRefillTime + this.rateLimitWindowMs,
    }
  }

  /**
   * Reset all state (useful for testing)
   */
  reset(): void {
    this.tokenBucket = {
      tokens: this.dispatchesPerSecond,
      lastRefillTime: Date.now(),
      dispatchTimestamps: [],
    }
    this.circuitBreaker = {
      isOpen: false,
      consecutiveFailures: 0,
      failureTimestamps: [],
      backoffMultiplier: 1,
    }
    this.backpressureActive = false
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a BackpressureManager with default settings for production
 */
export function createDefaultBackpressureManager(): BackpressureManager {
  return new BackpressureManager({
    dispatchesPerSecond: 100,
    backpressureThreshold: 50,
    circuitBreakerThreshold: 5,
    circuitBreakerBaseDelayMs: 5000,
  })
}

/**
 * Create a BackpressureManager with aggressive settings for testing
 */
export function createTestBackpressureManager(): BackpressureManager {
  return new BackpressureManager({
    dispatchesPerSecond: 10,
    backpressureThreshold: 5,
    circuitBreakerThreshold: 2,
    circuitBreakerBaseDelayMs: 100,
    circuitBreakerMaxDelayMs: 1000,
  })
}

/**
 * Create a BackpressureManager with conservative settings for high-load scenarios
 */
export function createConservativeBackpressureManager(): BackpressureManager {
  return new BackpressureManager({
    dispatchesPerSecond: 50,
    backpressureThreshold: 25,
    circuitBreakerThreshold: 3,
    circuitBreakerBaseDelayMs: 10000,
    circuitBreakerMaxDelayMs: 120000,
  })
}

export default BackpressureManager

/**
 * Fire-and-Forget Error Handler
 *
 * Provides centralized error handling for fire-and-forget operations
 * with logging, metrics tracking, and optional retry logic.
 *
 * Fire-and-forget operations are intentionally non-blocking - errors
 * should not fail the main operation. However, they should be:
 * 1. Logged for debugging and alerting
 * 2. Tracked in metrics for observability
 * 3. Optionally retried for transient failures
 *
 * @module utils/fire-and-forget
 */

import { logger, type Logger } from './logger'
import { withRetry, type RetryConfig } from '../delta-utils/retry'

// =============================================================================
// Types
// =============================================================================

/**
 * Type of fire-and-forget operation
 */
export type FireAndForgetOperationType =
  | 'auto-snapshot'
  | 'periodic-flush'
  | 'background-revalidation'
  | 'cache-cleanup'
  | 'metrics-flush'
  | 'index-update'
  | 'custom'

/**
 * Metrics for a specific operation type
 */
export interface FireAndForgetMetrics {
  /** Total number of operations started */
  started: number
  /** Total number of successful operations */
  succeeded: number
  /** Total number of failed operations */
  failed: number
  /** Total number of retries attempted */
  retries: number
  /** Last error message (if any) */
  lastError?: string
  /** Timestamp of last error */
  lastErrorAt?: number
  /** Timestamp of last success */
  lastSuccessAt?: number
}

/**
 * Aggregated metrics across all operation types
 */
export interface AggregatedFireAndForgetMetrics {
  /** Metrics by operation type */
  byType: Record<FireAndForgetOperationType, FireAndForgetMetrics>
  /** Total started across all types */
  totalStarted: number
  /** Total succeeded across all types */
  totalSucceeded: number
  /** Total failed across all types */
  totalFailed: number
  /** Total retries across all types */
  totalRetries: number
}

/**
 * Error handler callback signature
 */
export type ErrorHandler = (
  error: Error,
  operation: FireAndForgetOperationType,
  context?: Record<string, unknown>
) => void

/**
 * Configuration for fire-and-forget operations
 */
export interface FireAndForgetConfig {
  /** Optional custom logger (defaults to global logger) */
  logger?: Logger
  /** Additional error handler callback */
  onError?: ErrorHandler
  /** Whether to enable retry for transient failures */
  enableRetry?: boolean
  /** Retry configuration (if enableRetry is true) */
  retryConfig?: RetryConfig
}

// =============================================================================
// Default Configuration
// =============================================================================

/**
 * Default retry configuration for fire-and-forget operations
 * Uses fewer retries and shorter delays than critical operations
 */
export const DEFAULT_FIRE_AND_FORGET_RETRY_CONFIG: RetryConfig = {
  maxRetries: 2,
  baseDelay: 50,
  maxDelay: 500,
  jitter: true,
  // Mark transient errors as retryable
  isRetryable: (error: Error) => {
    const message = error.message.toLowerCase()
    // Retry on common transient errors
    return (
      message.includes('timeout') ||
      message.includes('network') ||
      message.includes('econnreset') ||
      message.includes('econnrefused') ||
      message.includes('temporarily unavailable') ||
      (error as { retryable?: boolean }).retryable === true
    )
  },
}

// =============================================================================
// Metrics Collector
// =============================================================================

/**
 * Create empty metrics for an operation type
 */
function createEmptyMetrics(): FireAndForgetMetrics {
  return {
    started: 0,
    succeeded: 0,
    failed: 0,
    retries: 0,
  }
}

/**
 * Collector for fire-and-forget operation metrics
 */
export class FireAndForgetMetricsCollector {
  private metrics: Map<FireAndForgetOperationType, FireAndForgetMetrics> = new Map()

  /**
   * Record that an operation started
   */
  recordStart(operation: FireAndForgetOperationType): void {
    const m = this.getOrCreateMetrics(operation)
    m.started++
  }

  /**
   * Record that an operation succeeded
   */
  recordSuccess(operation: FireAndForgetOperationType): void {
    const m = this.getOrCreateMetrics(operation)
    m.succeeded++
    m.lastSuccessAt = Date.now()
  }

  /**
   * Record that an operation failed
   */
  recordFailure(operation: FireAndForgetOperationType, error: Error): void {
    const m = this.getOrCreateMetrics(operation)
    m.failed++
    m.lastError = error.message
    m.lastErrorAt = Date.now()
  }

  /**
   * Record a retry attempt
   */
  recordRetry(operation: FireAndForgetOperationType): void {
    const m = this.getOrCreateMetrics(operation)
    m.retries++
  }

  /**
   * Get metrics for a specific operation type
   */
  getMetrics(operation: FireAndForgetOperationType): FireAndForgetMetrics {
    return { ...this.getOrCreateMetrics(operation) }
  }

  /**
   * Get aggregated metrics across all operation types
   */
  getAggregatedMetrics(): AggregatedFireAndForgetMetrics {
    const byType = {} as Record<FireAndForgetOperationType, FireAndForgetMetrics>
    let totalStarted = 0
    let totalSucceeded = 0
    let totalFailed = 0
    let totalRetries = 0

    for (const [type, metrics] of this.metrics) {
      byType[type] = { ...metrics }
      totalStarted += metrics.started
      totalSucceeded += metrics.succeeded
      totalFailed += metrics.failed
      totalRetries += metrics.retries
    }

    return {
      byType,
      totalStarted,
      totalSucceeded,
      totalFailed,
      totalRetries,
    }
  }

  /**
   * Get failure rate for a specific operation type
   */
  getFailureRate(operation: FireAndForgetOperationType): number {
    const m = this.metrics.get(operation)
    if (!m || m.started === 0) return 0
    return m.failed / m.started
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    this.metrics.clear()
  }

  private getOrCreateMetrics(operation: FireAndForgetOperationType): FireAndForgetMetrics {
    let m = this.metrics.get(operation)
    if (!m) {
      m = createEmptyMetrics()
      this.metrics.set(operation, m)
    }
    return m
  }
}

// =============================================================================
// Global Metrics Collector
// =============================================================================

/**
 * Global metrics collector for fire-and-forget operations
 */
export const globalFireAndForgetMetrics = new FireAndForgetMetricsCollector()

// =============================================================================
// Fire-and-Forget Executor
// =============================================================================

/**
 * Execute an operation in fire-and-forget mode with proper error handling
 *
 * This function wraps a promise and handles its result/error without blocking.
 * Errors are logged and tracked in metrics but do not propagate.
 *
 * @param operation - Type of operation for metrics/logging
 * @param fn - Async function to execute
 * @param config - Optional configuration
 * @param context - Optional context for logging
 *
 * @example
 * ```typescript
 * // Basic usage - fire and forget a snapshot operation
 * fireAndForget('auto-snapshot', async () => {
 *   await snapshotManager.createSnapshot(entityId)
 * })
 *
 * // With context for better logging
 * fireAndForget('auto-snapshot', async () => {
 *   await snapshotManager.createSnapshot(entityId)
 * }, {}, { entityId, eventCount: 50 })
 *
 * // With retry for transient failures
 * fireAndForget('background-revalidation', async () => {
 *   await cache.revalidate(key)
 * }, { enableRetry: true })
 * ```
 */
export function fireAndForget(
  operation: FireAndForgetOperationType,
  fn: () => Promise<void>,
  config: FireAndForgetConfig = {},
  context?: Record<string, unknown>
): void {
  const log = config.logger ?? logger
  const metrics = globalFireAndForgetMetrics

  // Record that we started
  metrics.recordStart(operation)

  // Build the execution promise
  const execute = async (): Promise<void> => {
    if (config.enableRetry) {
      const retryConfig: RetryConfig = {
        ...DEFAULT_FIRE_AND_FORGET_RETRY_CONFIG,
        ...config.retryConfig,
        onRetry: (info) => {
          metrics.recordRetry(operation)
          log.debug(
            `[ParqueDB] Retrying ${operation} (attempt ${info.attempt}): ${info.error.message}`,
            context
          )
          // Call user's onRetry if provided
          return config.retryConfig?.onRetry?.(info)
        },
      }
      await withRetry(fn, retryConfig)
    } else {
      await fn()
    }
  }

  // Execute and handle result
  execute()
    .then(() => {
      metrics.recordSuccess(operation)
    })
    .catch((err: unknown) => {
      const error = err instanceof Error ? err : new Error(String(err))

      // Record failure in metrics
      metrics.recordFailure(operation, error)

      // Log the error with context
      log.warn(
        `[ParqueDB] Fire-and-forget ${operation} failed: ${error.message}`,
        context
      )

      // Call custom error handler if provided
      if (config.onError) {
        try {
          config.onError(error, operation, context)
        } catch (handlerErr) {
          // Swallow errors in the error handler to prevent infinite loops
          log.error(
            `[ParqueDB] Error in fire-and-forget error handler`,
            handlerErr
          )
        }
      }
    })
}

/**
 * Create a fire-and-forget executor with pre-configured options
 *
 * @param defaultConfig - Default configuration to use for all operations
 * @returns A configured fireAndForget function
 *
 * @example
 * ```typescript
 * // Create a configured executor with retry enabled
 * const bgTask = createFireAndForgetExecutor({
 *   enableRetry: true,
 *   onError: (err, op, ctx) => {
 *     alerting.sendAlert('background-task-failed', { err, op, ctx })
 *   },
 * })
 *
 * // Use it for background tasks
 * bgTask('auto-snapshot', () => snapshot.create())
 * bgTask('cache-cleanup', () => cache.prune())
 * ```
 */
export function createFireAndForgetExecutor(
  defaultConfig: FireAndForgetConfig = {}
): (
  operation: FireAndForgetOperationType,
  fn: () => Promise<void>,
  config?: FireAndForgetConfig,
  context?: Record<string, unknown>
) => void {
  return (operation, fn, config = {}, context) => {
    fireAndForget(operation, fn, { ...defaultConfig, ...config }, context)
  }
}

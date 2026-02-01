/**
 * Retry with Exponential Backoff
 *
 * Provides retry logic with exponential backoff and jitter for handling
 * transient failures like ConcurrencyError during write operations.
 *
 * This is a shared utility that can be used by both ParqueDB and Delta Lake
 * for handling optimistic concurrency conflicts and other transient errors.
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Information passed to the onRetry callback
 */
export interface RetryInfo {
  /** The retry attempt number (1-indexed, so first retry is 1) */
  attempt: number
  /** The error that triggered the retry */
  error: Error
  /** The delay in milliseconds before this retry */
  delay: number
}

/**
 * Information passed to the onSuccess callback
 */
export interface SuccessInfo<T = unknown> {
  /** The result of the successful operation */
  result: T
  /** Total number of attempts (including the successful one) */
  attempts: number
}

/**
 * Information passed to the onFailure callback
 */
export interface FailureInfo {
  /** The final error after all retries were exhausted */
  error: Error
  /** Total number of attempts made */
  attempts: number
}

/**
 * Metrics collected during retry execution
 */
export interface RetryMetrics {
  /** Total number of attempts made */
  attempts: number
  /** Number of retries (attempts - 1) */
  retries: number
  /** Whether the operation succeeded */
  succeeded: boolean
  /** Total delay time in milliseconds */
  totalDelayMs: number
  /** Total elapsed time in milliseconds */
  elapsedMs: number
  /** Array of individual delay times */
  delays: number[]
  /** Array of errors encountered */
  errors: Error[]
}

/**
 * Configuration options for retry behavior
 */
export interface RetryConfig {
  /** Maximum number of retry attempts (default: 3) */
  maxRetries?: number
  /** Base delay in milliseconds (default: 100) */
  baseDelay?: number
  /** Maximum delay in milliseconds (default: 10000) */
  maxDelay?: number
  /** Multiplier for exponential backoff (default: 2) */
  multiplier?: number
  /** Whether to add random jitter to delays (default: true) */
  jitter?: boolean
  /** Jitter factor (default: 0.5, meaning +/- 50%) */
  jitterFactor?: number
  /** Custom predicate to determine if error is retryable */
  isRetryable?: (error: Error) => boolean
  /** Called before each retry attempt. Return false to abort retries. */
  onRetry?: (info: RetryInfo) => boolean | void
  /** Called on successful completion */
  onSuccess?: (info: SuccessInfo) => void
  /** Called when all retries are exhausted */
  onFailure?: (info: FailureInfo) => void
  /** Whether to return metrics with result */
  returnMetrics?: boolean
  /** AbortController signal to cancel retries */
  signal?: AbortSignal
  /** Internal: custom delay function for testing */
  _delayFn?: (ms: number) => Promise<void>
}

/**
 * Result type when returnMetrics is true
 */
export interface RetryResultWithMetrics<T> {
  result: T
  metrics: RetryMetrics
}

// =============================================================================
// DEFAULT CONFIG
// =============================================================================

/**
 * Default retry configuration values
 */
export const DEFAULT_RETRY_CONFIG = {
  maxRetries: 3,
  baseDelay: 100,
  maxDelay: 10000,
  jitter: true,
  multiplier: 2,
  jitterFactor: 0.5,
} as const

// =============================================================================
// ERROR UTILITIES
// =============================================================================

/**
 * Custom error class for abort operations
 */
export class AbortError extends Error {
  override name = 'AbortError'

  constructor(message: string = 'Operation was aborted') {
    super(message)
  }
}

/**
 * Check if an error is retryable
 *
 * Returns true for:
 * - ConcurrencyError instances
 * - VersionMismatchError instances
 * - Errors with a `retryable: true` property
 *
 * Returns false for:
 * - null/undefined
 * - Non-Error objects
 * - Generic errors (TypeError, SyntaxError, etc.)
 */
export function isRetryableError(error: unknown): boolean {
  // Null/undefined check
  if (error == null) {
    return false
  }

  // Must be an Error instance
  if (!(error instanceof Error)) {
    return false
  }

  // Check for known retryable error types by name
  const retryableNames = ['ConcurrencyError', 'VersionMismatchError']
  if (retryableNames.includes(error.name)) {
    return true
  }

  // Check for explicit retryable property
  if ((error as { retryable?: boolean }).retryable === true) {
    return true
  }

  return false
}

// =============================================================================
// DELAY UTILITIES
// =============================================================================

/**
 * Default delay function using setTimeout
 */
async function defaultDelay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Calculate the delay for a given retry attempt with exponential backoff
 */
function calculateDelay(
  attempt: number,
  config: {
    baseDelay: number
    maxDelay: number
    multiplier: number
    jitter: boolean
    jitterFactor: number
  }
): number {
  // Calculate base exponential delay
  let delay = config.baseDelay * Math.pow(config.multiplier, attempt - 1)

  // Apply jitter if enabled
  if (config.jitter) {
    const jitterRange = delay * config.jitterFactor
    // Random value between -jitterRange and +jitterRange
    const jitter = (Math.random() * 2 - 1) * jitterRange
    delay = delay + jitter
  }

  // Ensure delay is non-negative
  delay = Math.max(0, delay)

  // Cap at maxDelay
  delay = Math.min(delay, config.maxDelay)

  return Math.floor(delay)
}

// =============================================================================
// MAIN RETRY FUNCTION
// =============================================================================

/**
 * Wrap a function with retry logic using exponential backoff
 *
 * @param fn - The async function to retry
 * @param config - Retry configuration options
 * @returns The result of the function, or throws after all retries exhausted
 *
 * @example
 * ```typescript
 * // Basic usage
 * const result = await withRetry(async () => {
 *   return await table.write(rows)
 * })
 *
 * // With custom config
 * const result = await withRetry(async () => {
 *   return await table.write(rows)
 * }, {
 *   maxRetries: 5,
 *   baseDelay: 200,
 *   onRetry: ({ attempt, error }) => {
 *     console.log(`Retry ${attempt} after error: ${error.message}`)
 *   }
 * })
 *
 * // With metrics
 * const { result, metrics } = await withRetry(async () => {
 *   return await table.write(rows)
 * }, {
 *   returnMetrics: true
 * })
 * console.log(`Succeeded after ${metrics.attempts} attempts`)
 * ```
 */
export async function withRetry<T>(
  fn: () => T | Promise<T>,
  config?: RetryConfig & { returnMetrics: true }
): Promise<RetryResultWithMetrics<T>>

export async function withRetry<T>(
  fn: () => T | Promise<T>,
  config?: RetryConfig & { returnMetrics?: false }
): Promise<T>

export async function withRetry<T>(
  fn: () => T | Promise<T>,
  config: RetryConfig = {}
): Promise<T | RetryResultWithMetrics<T>> {
  // Merge with defaults
  const maxRetries = config.maxRetries ?? DEFAULT_RETRY_CONFIG.maxRetries
  const baseDelay = config.baseDelay ?? DEFAULT_RETRY_CONFIG.baseDelay
  const maxDelay = config.maxDelay ?? DEFAULT_RETRY_CONFIG.maxDelay
  const multiplier = config.multiplier ?? DEFAULT_RETRY_CONFIG.multiplier
  const jitter = config.jitter ?? DEFAULT_RETRY_CONFIG.jitter
  const jitterFactor = config.jitterFactor ?? DEFAULT_RETRY_CONFIG.jitterFactor
  const isRetryableFn = config.isRetryable ?? isRetryableError
  const delayFn = config._delayFn ?? defaultDelay
  const returnMetrics = config.returnMetrics ?? false

  // Track metrics
  const startTime = Date.now()
  const delays: number[] = []
  const errors: Error[] = []
  let attempts = 0

  // Check if already aborted
  if (config.signal?.aborted) {
    throw new AbortError('Operation was aborted')
  }

  // Main retry loop
  while (true) {
    attempts++

    try {
      // Execute the function (handle both sync and async)
      const result = await Promise.resolve(fn())

      // Success! Build metrics and return
      const metrics: RetryMetrics = {
        attempts,
        retries: attempts - 1,
        succeeded: true,
        totalDelayMs: delays.reduce((sum, d) => sum + d, 0),
        elapsedMs: Date.now() - startTime,
        delays,
        errors,
      }

      // Call onSuccess callback
      if (config.onSuccess) {
        config.onSuccess({ result, attempts })
      }

      if (returnMetrics) {
        return { result, metrics }
      }

      return result
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      errors.push(err)

      // Check if we should retry
      const canRetry = isRetryableFn(err)
      const hasRetriesLeft = attempts <= maxRetries

      if (!canRetry || !hasRetriesLeft) {
        // Build failure metrics
        const metrics: RetryMetrics = {
          attempts,
          retries: attempts - 1,
          succeeded: false,
          totalDelayMs: delays.reduce((sum, d) => sum + d, 0),
          elapsedMs: Date.now() - startTime,
          delays,
          errors,
        }

        // Call onFailure callback
        if (config.onFailure) {
          config.onFailure({ error: err, attempts })
        }

        // Attach metrics to error if returnMetrics is enabled
        if (returnMetrics) {
          (err as Error & { metrics?: RetryMetrics }).metrics = metrics
        }

        throw err
      }

      // Calculate delay for this retry
      const delay = calculateDelay(attempts, {
        baseDelay,
        maxDelay,
        multiplier,
        jitter,
        jitterFactor,
      })

      // Call onRetry callback
      if (config.onRetry) {
        const shouldContinue = config.onRetry({
          attempt: attempts,
          error: err,
          delay,
        })

        // If onRetry explicitly returns false, abort
        if (shouldContinue === false) {
          // Build failure metrics
          const metrics: RetryMetrics = {
            attempts,
            retries: attempts - 1,
            succeeded: false,
            totalDelayMs: delays.reduce((sum, d) => sum + d, 0),
            elapsedMs: Date.now() - startTime,
            delays,
            errors,
          }

          // Call onFailure callback
          if (config.onFailure) {
            config.onFailure({ error: err, attempts })
          }

          // Attach metrics to error if returnMetrics is enabled
          if (returnMetrics) {
            (err as Error & { metrics?: RetryMetrics }).metrics = metrics
          }

          throw err
        }
      }

      // Track the delay
      delays.push(delay)

      // Check for abort before delay
      if (config.signal?.aborted) {
        throw new AbortError('Operation was aborted')
      }

      // Wait before retrying
      try {
        await delayFn(delay)
      } catch (delayError) {
        // If delay function throws, propagate the error
        throw delayError
      }

      // Check for abort after delay
      if (config.signal?.aborted) {
        throw new AbortError('Operation was aborted')
      }
    }
  }
}

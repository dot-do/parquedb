/**
 * Safe Callback Wrapper Utility
 *
 * Provides a reusable utility for wrapping callbacks that may be sync or async,
 * ensuring errors are properly caught and handled without causing unhandled
 * promise rejections.
 *
 * This is designed for fire-and-forget callback scenarios where:
 * - The callback may return void or Promise<void>
 * - Errors should be logged but not propagate to the caller
 * - We want consistent error handling with context information
 *
 * @module utils/safe-callback
 */

import { logger, type Logger } from './logger'

// =============================================================================
// Types
// =============================================================================

/**
 * A callback that can be either synchronous or asynchronous.
 * Returns void or a Promise that resolves to void.
 */
export type MaybeAsyncCallback<TArgs extends unknown[] = []> = (
  ...args: TArgs
) => void | Promise<void>

/**
 * Error handler callback signature.
 * Called when an error occurs in the wrapped callback.
 */
export type SafeCallbackErrorHandler<TContext = unknown> = (
  error: Error,
  context: TContext
) => void

/**
 * Success handler callback signature.
 * Called when the callback completes successfully.
 */
export type SafeCallbackSuccessHandler = () => void

/**
 * Configuration options for the safe callback wrapper.
 */
export interface SafeCallbackOptions<TContext = unknown> {
  /**
   * Custom logger to use for error logging.
   * Defaults to the global logger.
   */
  logger?: Logger | undefined

  /**
   * Prefix for log messages.
   * @default '[SafeCallback]'
   */
  logPrefix?: string | undefined

  /**
   * Custom error handler called when the callback fails.
   * Called in addition to logging.
   */
  onError?: SafeCallbackErrorHandler<TContext> | undefined

  /**
   * Custom success handler called when the callback succeeds.
   * Useful for resetting error counters or circuit breakers.
   */
  onSuccess?: SafeCallbackSuccessHandler | undefined

  /**
   * Context information to include in error logs and handler calls.
   * This can be any data that helps identify what operation failed.
   */
  context?: TContext | undefined

  /**
   * If true, synchronous errors will be re-thrown after logging.
   * Async errors are never re-thrown (to prevent unhandled rejections).
   * @default false
   */
  rethrowSync?: boolean | undefined
}

/**
 * Result type for safeCallback execution.
 * Indicates whether the callback was sync, async, or errored synchronously.
 */
export type SafeCallbackResult =
  | { type: 'sync'; success: true }
  | { type: 'sync'; success: false; error: Error }
  | { type: 'async'; promise: Promise<void> }

// =============================================================================
// Implementation
// =============================================================================

/**
 * Invoke a callback safely, catching both sync throws and async rejections.
 *
 * This utility wraps callbacks that may be synchronous or asynchronous,
 * ensuring that errors are always caught and handled properly. This prevents
 * unhandled promise rejections and provides consistent error logging with context.
 *
 * @param callback - The callback to invoke (may be sync or async)
 * @param options - Configuration options for error handling
 * @param args - Arguments to pass to the callback
 * @returns A result indicating sync success/failure or an async promise
 *
 * @example
 * ```typescript
 * // Basic usage with event callback
 * const result = safeCallback(
 *   onEvent,
 *   {
 *     context: { eventId: event.id, operation: event.op },
 *     logPrefix: '[ParqueDB]',
 *   },
 *   event
 * )
 *
 * // With circuit breaker integration
 * const result = safeCallback(
 *   myCallback,
 *   {
 *     onSuccess: () => { circuitBreaker.reset() },
 *     onError: (err, ctx) => { circuitBreaker.recordFailure() },
 *     context: { operation: 'processEvent' },
 *   }
 * )
 *
 * // Check if it was async
 * if (result.type === 'async') {
 *   // The promise is already being handled internally
 *   // No need to await unless you want to
 * }
 * ```
 */
export function safeCallback<TArgs extends unknown[], TContext = unknown>(
  callback: MaybeAsyncCallback<TArgs>,
  options: SafeCallbackOptions<TContext> = {},
  ...args: TArgs
): SafeCallbackResult {
  const log = options.logger ?? logger
  const prefix = options.logPrefix ?? '[SafeCallback]'
  const context = options.context

  /**
   * Handle an error from the callback
   */
  const handleError = (error: unknown): Error => {
    const err = error instanceof Error ? error : new Error(String(error))

    // Log the error with context
    const contextStr = context ? ` (context: ${formatContext(context)})` : ''
    log.warn(`${prefix} Callback error${contextStr}: ${err.message}`, err)

    // Call custom error handler if provided
    if (options.onError && context !== undefined) {
      try {
        options.onError(err, context)
      } catch (handlerError) {
        // Log but don't propagate error handler failures
        log.warn(
          `${prefix} Error in error handler: ${handlerError instanceof Error ? handlerError.message : String(handlerError)}`
        )
      }
    }

    return err
  }

  /**
   * Handle successful completion
   */
  const handleSuccess = (): void => {
    if (options.onSuccess) {
      try {
        options.onSuccess()
      } catch (handlerError) {
        // Log but don't propagate success handler failures
        log.warn(
          `${prefix} Error in success handler: ${handlerError instanceof Error ? handlerError.message : String(handlerError)}`
        )
      }
    }
  }

  try {
    // Call the callback - may return a promise
    const result = callback(...args)

    // Check if the result is a promise (async callback)
    if (result instanceof Promise) {
      // Handle the promise asynchronously - fire and forget
      const handledPromise = result
        .then(() => {
          handleSuccess()
        })
        .catch((error) => {
          handleError(error)
          // Don't re-throw - this prevents unhandled promise rejections
        })

      return { type: 'async', promise: handledPromise }
    }

    // Synchronous success
    handleSuccess()
    return { type: 'sync', success: true }
  } catch (error) {
    // Synchronous throw
    const err = handleError(error)

    if (options.rethrowSync) {
      throw err
    }

    return { type: 'sync', success: false, error: err }
  }
}

/**
 * Create a wrapped version of a callback that is always safe to call.
 *
 * This is useful when you want to pass a callback to external code and ensure
 * it will never throw or cause unhandled rejections.
 *
 * @param callback - The callback to wrap
 * @param options - Configuration options for error handling
 * @returns A wrapped callback that catches all errors
 *
 * @example
 * ```typescript
 * // Create a safe version of an event handler
 * const safeOnEvent = createSafeCallback(
 *   ctx.onEvent,
 *   {
 *     logPrefix: '[ParqueDB]',
 *     context: { component: 'EventRecorder' },
 *   }
 * )
 *
 * // Use it anywhere - it will never throw
 * safeOnEvent(event)
 * ```
 */
export function createSafeCallback<TArgs extends unknown[], TContext = unknown>(
  callback: MaybeAsyncCallback<TArgs>,
  options: SafeCallbackOptions<TContext> = {}
): (...args: TArgs) => SafeCallbackResult {
  return (...args: TArgs) => safeCallback(callback, options, ...args)
}

/**
 * Invoke a callback safely only if it is defined (not null/undefined).
 *
 * This is a convenience wrapper that checks for callback existence before
 * invoking, which is common in optional callback scenarios.
 *
 * @param callback - The callback to invoke (may be undefined)
 * @param options - Configuration options for error handling
 * @param args - Arguments to pass to the callback
 * @returns Result if callback was invoked, null if callback was undefined
 *
 * @example
 * ```typescript
 * // Only invoke if onEvent is defined
 * safeCallbackIfDefined(
 *   ctx.onEvent,
 *   { context: { eventType: 'CREATE' } },
 *   event
 * )
 * ```
 */
export function safeCallbackIfDefined<TArgs extends unknown[], TContext = unknown>(
  callback: MaybeAsyncCallback<TArgs> | undefined | null,
  options: SafeCallbackOptions<TContext> = {},
  ...args: TArgs
): SafeCallbackResult | null {
  if (callback == null) {
    return null
  }
  return safeCallback(callback, options, ...args)
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Format context object for logging.
 * Handles various context types gracefully.
 */
function formatContext(context: unknown): string {
  if (context === null || context === undefined) {
    return 'none'
  }

  if (typeof context === 'string') {
    return context
  }

  if (typeof context === 'object') {
    try {
      // Try to create a concise representation
      const entries = Object.entries(context as Record<string, unknown>)
      if (entries.length === 0) {
        return '{}'
      }

      const parts = entries
        .slice(0, 5) // Limit to first 5 entries
        .map(([key, value]) => {
          const valueStr = typeof value === 'string'
            ? value
            : typeof value === 'number' || typeof value === 'boolean'
              ? String(value)
              : typeof value
          return `${key}=${valueStr}`
        })

      const result = parts.join(', ')
      return entries.length > 5 ? `${result}, ...` : result
    } catch {
      return '[object]'
    }
  }

  return String(context)
}

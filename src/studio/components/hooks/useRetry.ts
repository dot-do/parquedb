/**
 * useRetry Hook
 *
 * A React hook for handling async operations with automatic retry and exponential backoff.
 *
 * Features:
 * - Exponential backoff with configurable base delay and max delay
 * - Manual retry trigger
 * - Automatic retry on mount (optional)
 * - Loading, error, and success states
 * - Retry count tracking
 *
 * @example
 * ```typescript
 * const { data, loading, error, retry, retryCount, isRetrying } = useRetry(
 *   async () => {
 *     const response = await fetch('/api/data')
 *     if (!response.ok) throw new Error('Failed to fetch')
 *     return response.json()
 *   },
 *   { maxRetries: 3, baseDelay: 1000 }
 * )
 * ```
 */

import { useState, useCallback, useEffect, useRef } from 'react'

/**
 * Configuration options for useRetry hook
 */
export interface UseRetryOptions {
  /** Maximum number of automatic retries (default: 3) */
  maxRetries?: number | undefined
  /** Base delay in ms for exponential backoff (default: 1000) */
  baseDelay?: number | undefined
  /** Maximum delay in ms (default: 30000) */
  maxDelay?: number | undefined
  /** Jitter factor to add randomness (0-1, default: 0.1) */
  jitter?: number | undefined
  /** Whether to automatically retry on failure (default: false) */
  autoRetry?: boolean | undefined
  /** Whether to fetch on mount (default: true) */
  fetchOnMount?: boolean | undefined
  /** Callback when an error occurs */
  onError?: ((error: Error, retryCount: number) => void) | undefined
  /** Callback when retry succeeds */
  onSuccess?: ((data: unknown) => void) | undefined
  /** Callback when all retries exhausted */
  onExhausted?: ((error: Error) => void) | undefined
}

/**
 * Return type for useRetry hook
 */
export interface UseRetryResult<T> {
  /** The fetched data (undefined until first success) */
  data: T | undefined
  /** Whether the operation is in progress */
  loading: boolean
  /** Error from the last failed attempt */
  error: Error | null
  /** Number of retry attempts made */
  retryCount: number
  /** Whether a retry is currently in progress */
  isRetrying: boolean
  /** Whether max retries has been reached */
  isExhausted: boolean
  /** Time until next automatic retry (ms) */
  nextRetryIn: number | null
  /** Manually trigger a retry */
  retry: () => Promise<void>
  /** Reset state and fetch fresh */
  reset: () => Promise<void>
  /** Cancel any pending retry */
  cancel: () => void
}

/**
 * Calculate delay with exponential backoff and jitter
 */
function calculateDelay(
  attempt: number,
  baseDelay: number,
  maxDelay: number,
  jitter: number
): number {
  // Exponential backoff: baseDelay * 2^attempt
  const exponentialDelay = baseDelay * Math.pow(2, attempt)
  // Cap at maxDelay
  const cappedDelay = Math.min(exponentialDelay, maxDelay)
  // Add jitter (random variation)
  const jitterAmount = cappedDelay * jitter * Math.random()
  return Math.floor(cappedDelay + jitterAmount)
}

/**
 * useRetry - Hook for async operations with automatic retry
 */
export function useRetry<T>(
  operation: () => Promise<T>,
  options: UseRetryOptions = {}
): UseRetryResult<T> {
  const {
    maxRetries = 3,
    baseDelay = 1000,
    maxDelay = 30000,
    jitter = 0.1,
    autoRetry = false,
    fetchOnMount = true,
    onError,
    onSuccess,
    onExhausted,
  } = options

  const [data, setData] = useState<T | undefined>(undefined)
  const [loading, setLoading] = useState(fetchOnMount)
  const [error, setError] = useState<Error | null>(null)
  const [retryCount, setRetryCount] = useState(0)
  const [isRetrying, setIsRetrying] = useState(false)
  const [nextRetryIn, setNextRetryIn] = useState<number | null>(null)

  // Refs for cleanup
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const isMountedRef = useRef(true)

  // Clear timers
  const clearTimers = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
      timeoutRef.current = null
    }
    if (intervalRef.current) {
      clearInterval(intervalRef.current)
      intervalRef.current = null
    }
  }, [])

  // Cancel pending retry
  const cancel = useCallback(() => {
    clearTimers()
    setNextRetryIn(null)
    setIsRetrying(false)
  }, [clearTimers])

  // Execute the operation
  const execute = useCallback(async (isManualRetry = false) => {
    if (!isMountedRef.current) return

    clearTimers()
    setLoading(true)
    setNextRetryIn(null)

    if (isManualRetry) {
      setIsRetrying(true)
    }

    try {
      const result = await operation()
      if (!isMountedRef.current) return

      setData(result)
      setError(null)
      setRetryCount(0)
      setIsRetrying(false)
      onSuccess?.(result)
    } catch (err) {
      if (!isMountedRef.current) return

      const error = err instanceof Error ? err : new Error(String(err))
      setError(error)
      setIsRetrying(false)

      const newRetryCount = retryCount + (isManualRetry || loading ? 0 : 1)
      setRetryCount(newRetryCount)

      onError?.(error, newRetryCount)

      // Check if we should auto-retry
      if (autoRetry && newRetryCount < maxRetries) {
        const delay = calculateDelay(newRetryCount, baseDelay, maxDelay, jitter)
        setNextRetryIn(delay)

        // Update countdown
        const startTime = Date.now()
        intervalRef.current = setInterval(() => {
          const remaining = delay - (Date.now() - startTime)
          if (remaining <= 0) {
            clearInterval(intervalRef.current!)
            intervalRef.current = null
          } else {
            setNextRetryIn(remaining)
          }
        }, 100)

        // Schedule retry
        timeoutRef.current = setTimeout(() => {
          if (isMountedRef.current) {
            setRetryCount(newRetryCount + 1)
            execute(false)
          }
        }, delay)
      } else if (newRetryCount >= maxRetries) {
        onExhausted?.(error)
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false)
      }
    }
  }, [
    operation,
    retryCount,
    loading,
    autoRetry,
    maxRetries,
    baseDelay,
    maxDelay,
    jitter,
    clearTimers,
    onError,
    onSuccess,
    onExhausted,
  ])

  // Manual retry
  const retry = useCallback(async () => {
    setRetryCount(0)
    await execute(true)
  }, [execute])

  // Reset and fetch fresh
  const reset = useCallback(async () => {
    setData(undefined)
    setError(null)
    setRetryCount(0)
    setIsRetrying(false)
    setNextRetryIn(null)
    clearTimers()
    await execute(false)
  }, [execute, clearTimers])

  // Fetch on mount
  useEffect(() => {
    isMountedRef.current = true
    if (fetchOnMount) {
      execute(false)
    }
    return () => {
      isMountedRef.current = false
      clearTimers()
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return {
    data,
    loading,
    error,
    retryCount,
    isRetrying,
    isExhausted: retryCount >= maxRetries && error !== null,
    nextRetryIn,
    retry,
    reset,
    cancel,
  }
}

export default useRetry

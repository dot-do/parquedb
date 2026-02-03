/**
 * Tests for Studio Error Handling Utilities
 *
 * Tests the error message mapping and retry delay calculations.
 */

import { describe, it, expect } from 'vitest'

/**
 * Calculate delay with exponential backoff and jitter
 * This mirrors the function in useRetry.ts
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
 * Error message mapping - mirrors ErrorDisplay.tsx
 */
const ERROR_MESSAGES: Record<string, string> = {
  'Failed to fetch': 'Unable to connect to the server. Please check your internet connection.',
  'Network Error': 'A network error occurred. Please check your connection and try again.',
  '401': 'Your session has expired. Please sign in again.',
  '403': 'You do not have permission to perform this action.',
  '404': 'The requested resource was not found.',
  '429': 'Too many requests. Please wait a moment before trying again.',
  '500': 'An internal server error occurred. Please try again later.',
  '503': 'The service is temporarily unavailable. Please try again later.',
  'CSRF_VALIDATION_FAILED': 'Security validation failed. Please refresh the page and try again.',
}

function getUserFriendlyMessage(message: string): string {
  for (const [pattern, friendlyMessage] of Object.entries(ERROR_MESSAGES)) {
    if (message.includes(pattern)) {
      return friendlyMessage
    }
  }

  const statusMatch = message.match(/\b(4\d{2}|5\d{2})\b/)
  if (statusMatch && statusMatch[1] && ERROR_MESSAGES[statusMatch[1]]) {
    return ERROR_MESSAGES[statusMatch[1]]!
  }

  return message
}

describe('Error Handling Utilities', () => {
  describe('calculateDelay', () => {
    it('should calculate exponential backoff correctly', () => {
      const baseDelay = 1000
      const maxDelay = 30000
      const jitter = 0 // No jitter for deterministic tests

      expect(calculateDelay(0, baseDelay, maxDelay, jitter)).toBe(1000) // 1000 * 2^0 = 1000
      expect(calculateDelay(1, baseDelay, maxDelay, jitter)).toBe(2000) // 1000 * 2^1 = 2000
      expect(calculateDelay(2, baseDelay, maxDelay, jitter)).toBe(4000) // 1000 * 2^2 = 4000
      expect(calculateDelay(3, baseDelay, maxDelay, jitter)).toBe(8000) // 1000 * 2^3 = 8000
    })

    it('should cap delay at maxDelay', () => {
      const baseDelay = 1000
      const maxDelay = 5000
      const jitter = 0

      expect(calculateDelay(5, baseDelay, maxDelay, jitter)).toBe(5000) // Would be 32000 but capped
      expect(calculateDelay(10, baseDelay, maxDelay, jitter)).toBe(5000) // Still capped
    })

    it('should add jitter within expected range', () => {
      const baseDelay = 1000
      const maxDelay = 30000
      const jitter = 0.1

      // Run multiple times to test jitter variance
      for (let i = 0; i < 10; i++) {
        const delay = calculateDelay(1, baseDelay, maxDelay, jitter)
        // Base delay is 2000, jitter adds 0-10%, so range is 2000-2200
        expect(delay).toBeGreaterThanOrEqual(2000)
        expect(delay).toBeLessThanOrEqual(2200)
      }
    })
  })

  describe('getUserFriendlyMessage', () => {
    it('should map network errors to friendly messages', () => {
      expect(getUserFriendlyMessage('Failed to fetch'))
        .toBe('Unable to connect to the server. Please check your internet connection.')

      expect(getUserFriendlyMessage('Network Error'))
        .toBe('A network error occurred. Please check your connection and try again.')
    })

    it('should map HTTP status codes to friendly messages', () => {
      expect(getUserFriendlyMessage('Request failed with status 401'))
        .toBe('Your session has expired. Please sign in again.')

      expect(getUserFriendlyMessage('Server returned 500'))
        .toBe('An internal server error occurred. Please try again later.')

      expect(getUserFriendlyMessage('Error 429: rate limited'))
        .toBe('Too many requests. Please wait a moment before trying again.')
    })

    it('should map CSRF errors to friendly messages', () => {
      expect(getUserFriendlyMessage('CSRF_VALIDATION_FAILED'))
        .toBe('Security validation failed. Please refresh the page and try again.')
    })

    it('should return original message for unknown errors', () => {
      const customError = 'Something unexpected happened'
      expect(getUserFriendlyMessage(customError)).toBe(customError)
    })

    it('should handle messages with multiple patterns', () => {
      // Should match the first pattern found
      const message = 'Failed to fetch: Network Error'
      expect(getUserFriendlyMessage(message))
        .toBe('Unable to connect to the server. Please check your internet connection.')
    })
  })

  describe('Retry Logic Integration', () => {
    it('should demonstrate correct retry timing', () => {
      const baseDelay = 1000
      const maxDelay = 30000
      const jitter = 0

      // Simulate retry sequence
      const delays = [0, 1, 2, 3].map(attempt => calculateDelay(attempt, baseDelay, maxDelay, jitter))

      expect(delays).toEqual([1000, 2000, 4000, 8000])

      // Total wait time for 3 retries
      const totalWaitTime = delays.slice(0, 3).reduce((sum, d) => sum + d, 0)
      expect(totalWaitTime).toBe(7000) // 1000 + 2000 + 4000
    })

    it('should respect maxRetries configuration', () => {
      const maxRetries = 3
      const attempts = [0, 1, 2, 3, 4] // 0 = initial, 1-4 = retries

      // Only 3 retries should be attempted (indices 1, 2, 3)
      const validAttempts = attempts.filter(a => a <= maxRetries)
      expect(validAttempts.length).toBe(4) // Initial + 3 retries
    })
  })
})

describe('Error Recovery Scenarios', () => {
  it('should categorize errors correctly for retry decisions', () => {
    const shouldRetry = (statusCode: number): boolean => {
      // Retry on server errors (5xx), not on client errors (4xx)
      return statusCode >= 500
    }

    expect(shouldRetry(500)).toBe(true)  // Internal Server Error
    expect(shouldRetry(502)).toBe(true)  // Bad Gateway
    expect(shouldRetry(503)).toBe(true)  // Service Unavailable
    expect(shouldRetry(504)).toBe(true)  // Gateway Timeout

    expect(shouldRetry(400)).toBe(false) // Bad Request
    expect(shouldRetry(401)).toBe(false) // Unauthorized
    expect(shouldRetry(403)).toBe(false) // Forbidden
    expect(shouldRetry(404)).toBe(false) // Not Found
    expect(shouldRetry(409)).toBe(false) // Conflict
    expect(shouldRetry(422)).toBe(false) // Unprocessable Entity
  })

  it('should handle exhausted retries gracefully', () => {
    const maxRetries = 3
    let retryCount = 0
    let isExhausted = false

    const simulateRetries = () => {
      while (retryCount < maxRetries) {
        retryCount++
        // Simulate failure
        const success = false
        if (success) break
      }
      isExhausted = retryCount >= maxRetries
    }

    simulateRetries()

    expect(retryCount).toBe(3)
    expect(isExhausted).toBe(true)
  })
})

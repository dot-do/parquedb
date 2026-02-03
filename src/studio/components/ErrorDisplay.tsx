/**
 * ErrorDisplay Component
 *
 * A reusable component for displaying errors with retry functionality.
 * Supports various error types, retry states, and countdown timers.
 *
 * @example Basic Usage
 * ```typescript
 * <ErrorDisplay
 *   error={error}
 *   onRetry={handleRetry}
 * />
 * ```
 *
 * @example With Auto-retry Countdown
 * ```typescript
 * <ErrorDisplay
 *   error={error}
 *   onRetry={handleRetry}
 *   retryCount={2}
 *   maxRetries={3}
 *   nextRetryIn={5000}
 *   isRetrying={false}
 * />
 * ```
 */

'use client'

import { useMemo } from 'react'

/**
 * Error severity levels for styling
 */
export type ErrorSeverity = 'error' | 'warning' | 'info'

/**
 * Props for ErrorDisplay component
 */
export interface ErrorDisplayProps {
  /** The error to display */
  error: Error | string | null
  /** Callback when retry is clicked */
  onRetry?: () => void
  /** Number of retry attempts made */
  retryCount?: number
  /** Maximum retry attempts allowed */
  maxRetries?: number
  /** Time until next automatic retry (ms) */
  nextRetryIn?: number | null
  /** Whether a retry is currently in progress */
  isRetrying?: boolean
  /** Whether the error is dismissible */
  dismissible?: boolean
  /** Callback when error is dismissed */
  onDismiss?: () => void
  /** Custom title for the error */
  title?: string
  /** Visual severity level */
  severity?: ErrorSeverity
  /** Compact mode for inline display */
  compact?: boolean
  /** Additional CSS class name */
  className?: string
  /** Additional inline styles */
  style?: React.CSSProperties
}

/**
 * Map error codes to user-friendly messages
 */
const ERROR_MESSAGES: Record<string, string> = {
  // Network errors
  'Failed to fetch': 'Unable to connect to the server. Please check your internet connection.',
  'Network Error': 'A network error occurred. Please check your connection and try again.',
  'NetworkError': 'A network error occurred. Please check your connection and try again.',
  'TypeError: Failed to fetch': 'Unable to reach the server. Please try again.',

  // HTTP errors
  '401': 'Your session has expired. Please sign in again.',
  '403': 'You do not have permission to perform this action.',
  '404': 'The requested resource was not found.',
  '408': 'The request timed out. Please try again.',
  '429': 'Too many requests. Please wait a moment before trying again.',
  '500': 'An internal server error occurred. Please try again later.',
  '502': 'The server is temporarily unavailable. Please try again.',
  '503': 'The service is temporarily unavailable. Please try again later.',
  '504': 'The server took too long to respond. Please try again.',

  // Common application errors
  'CSRF_VALIDATION_FAILED': 'Security validation failed. Please refresh the page and try again.',
  'Authentication required': 'Please sign in to continue.',
  'Session expired': 'Your session has expired. Please sign in again.',
}

/**
 * Get severity colors
 */
function getSeverityColors(severity: ErrorSeverity): {
  bg: string
  border: string
  text: string
  icon: string
  button: string
  buttonHover: string
} {
  switch (severity) {
    case 'warning':
      return {
        bg: 'var(--theme-warning-50, #fffbeb)',
        border: 'var(--theme-warning-200, #fde68a)',
        text: 'var(--theme-warning-700, #b45309)',
        icon: 'var(--theme-warning-500, #f59e0b)',
        button: 'var(--theme-warning-500, #f59e0b)',
        buttonHover: 'var(--theme-warning-600, #d97706)',
      }
    case 'info':
      return {
        bg: 'var(--theme-info-50, #eff6ff)',
        border: 'var(--theme-info-200, #bfdbfe)',
        text: 'var(--theme-info-700, #1d4ed8)',
        icon: 'var(--theme-info-500, #3b82f6)',
        button: 'var(--theme-info-500, #3b82f6)',
        buttonHover: 'var(--theme-info-600, #2563eb)',
      }
    case 'error':
    default:
      return {
        bg: 'var(--theme-error-50, #fef2f2)',
        border: 'var(--theme-error-200, #fecaca)',
        text: 'var(--theme-error-700, #b91c1c)',
        icon: 'var(--theme-error-500, #ef4444)',
        button: 'var(--theme-error-500, #ef4444)',
        buttonHover: 'var(--theme-error-600, #dc2626)',
      }
  }
}

/**
 * Format time remaining for display
 */
function formatTimeRemaining(ms: number): string {
  if (ms < 1000) return 'less than a second'
  const seconds = Math.ceil(ms / 1000)
  if (seconds === 1) return '1 second'
  return `${seconds} seconds`
}

/**
 * Get user-friendly error message
 */
function getUserFriendlyMessage(error: Error | string): string {
  const message = typeof error === 'string' ? error : error.message

  // Check for known error patterns
  for (const [pattern, friendlyMessage] of Object.entries(ERROR_MESSAGES)) {
    if (message.includes(pattern)) {
      return friendlyMessage
    }
  }

  // Check for HTTP status codes in error
  const statusMatch = message.match(/\b(4\d{2}|5\d{2})\b/)
  if (statusMatch && statusMatch[1] && ERROR_MESSAGES[statusMatch[1]]) {
    return ERROR_MESSAGES[statusMatch[1]]!
  }

  return message
}

/**
 * ErrorDisplay - Component for displaying errors with retry functionality
 */
export function ErrorDisplay({
  error,
  onRetry,
  retryCount = 0,
  maxRetries = 3,
  nextRetryIn = null,
  isRetrying = false,
  dismissible = false,
  onDismiss,
  title,
  severity = 'error',
  compact = false,
  className,
  style,
}: ErrorDisplayProps) {
  const colors = useMemo(() => getSeverityColors(severity), [severity])

  if (!error) return null

  const errorMessage = getUserFriendlyMessage(error)
  const isExhausted = retryCount >= maxRetries && maxRetries > 0
  const showRetryInfo = retryCount > 0 && maxRetries > 0 && !isExhausted
  const showAutoRetry = nextRetryIn !== null && nextRetryIn > 0

  // Compact inline display
  if (compact) {
    return (
      <div
        className={className}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem',
          padding: '0.5rem 0.75rem',
          background: colors.bg,
          border: `1px solid ${colors.border}`,
          borderRadius: '6px',
          fontSize: '0.875rem',
          color: colors.text,
          ...style,
        }}
        role="alert"
      >
        {/* Icon */}
        <svg
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke={colors.icon}
          strokeWidth="2"
          style={{ flexShrink: 0 }}
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>

        {/* Message */}
        <span style={{ flex: 1 }}>{errorMessage}</span>

        {/* Retry button */}
        {onRetry && !isExhausted && (
          <button
            onClick={onRetry}
            disabled={isRetrying}
            style={{
              padding: '0.25rem 0.5rem',
              background: 'transparent',
              border: `1px solid ${colors.icon}`,
              borderRadius: '4px',
              fontSize: '0.75rem',
              color: colors.icon,
              cursor: isRetrying ? 'not-allowed' : 'pointer',
              opacity: isRetrying ? 0.6 : 1,
            }}
          >
            {isRetrying ? 'Retrying...' : 'Retry'}
          </button>
        )}

        {/* Dismiss button */}
        {dismissible && onDismiss && (
          <button
            onClick={onDismiss}
            style={{
              padding: '0.25rem',
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
              color: colors.icon,
              opacity: 0.7,
            }}
            aria-label="Dismiss"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        )}
      </div>
    )
  }

  // Full display
  return (
    <div
      className={className}
      style={{
        padding: '1rem 1.25rem',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: '8px',
        ...style,
      }}
      role="alert"
    >
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: '0.75rem' }}>
        {/* Icon */}
        <svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke={colors.icon}
          strokeWidth="2"
          style={{ flexShrink: 0, marginTop: '0.125rem' }}
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>

        <div style={{ flex: 1 }}>
          {/* Title */}
          {title && (
            <h4
              style={{
                margin: '0 0 0.25rem',
                fontSize: '0.875rem',
                fontWeight: 600,
                color: colors.text,
              }}
            >
              {title}
            </h4>
          )}

          {/* Message */}
          <p
            style={{
              margin: 0,
              fontSize: '0.875rem',
              color: colors.text,
              lineHeight: 1.5,
            }}
          >
            {errorMessage}
          </p>

          {/* Retry info */}
          {showRetryInfo && (
            <p
              style={{
                margin: '0.5rem 0 0',
                fontSize: '0.75rem',
                color: colors.icon,
              }}
            >
              Attempt {retryCount} of {maxRetries}
              {showAutoRetry && ` - Retrying in ${formatTimeRemaining(nextRetryIn)}`}
            </p>
          )}

          {/* Exhausted message */}
          {isExhausted && (
            <p
              style={{
                margin: '0.5rem 0 0',
                fontSize: '0.75rem',
                color: colors.icon,
              }}
            >
              Maximum retry attempts reached.
            </p>
          )}

          {/* Actions */}
          {(onRetry || (dismissible && onDismiss)) && (
            <div style={{ marginTop: '0.75rem', display: 'flex', gap: '0.5rem' }}>
              {onRetry && !isExhausted && (
                <button
                  onClick={onRetry}
                  disabled={isRetrying}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '0.375rem',
                    padding: '0.375rem 0.75rem',
                    background: colors.button,
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    fontSize: '0.8125rem',
                    fontWeight: 500,
                    cursor: isRetrying ? 'not-allowed' : 'pointer',
                    opacity: isRetrying ? 0.7 : 1,
                    transition: 'background 0.2s',
                  }}
                  onMouseEnter={(e) => {
                    if (!isRetrying) e.currentTarget.style.background = colors.buttonHover
                  }}
                  onMouseLeave={(e) => {
                    if (!isRetrying) e.currentTarget.style.background = colors.button
                  }}
                >
                  {isRetrying ? (
                    <>
                      <svg
                        width="14"
                        height="14"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        style={{
                          animation: 'spin 1s linear infinite',
                        }}
                      >
                        <path d="M21 12a9 9 0 11-6.219-8.56" />
                      </svg>
                      Retrying...
                    </>
                  ) : (
                    <>
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M1 4v6h6" />
                        <path d="M3.51 15a9 9 0 102.13-9.36L1 10" />
                      </svg>
                      {showAutoRetry ? 'Retry Now' : 'Try Again'}
                    </>
                  )}
                </button>
              )}

              {dismissible && onDismiss && (
                <button
                  onClick={onDismiss}
                  style={{
                    padding: '0.375rem 0.75rem',
                    background: 'transparent',
                    border: `1px solid ${colors.border}`,
                    borderRadius: '4px',
                    fontSize: '0.8125rem',
                    color: colors.text,
                    cursor: 'pointer',
                  }}
                >
                  Dismiss
                </button>
              )}
            </div>
          )}
        </div>

        {/* Close button (top right) */}
        {dismissible && onDismiss && (
          <button
            onClick={onDismiss}
            style={{
              padding: '0.25rem',
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
              color: colors.icon,
              opacity: 0.7,
            }}
            aria-label="Dismiss"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        )}
      </div>

      {/* CSS for spinner animation */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

export default ErrorDisplay

/**
 * ErrorBoundary Component
 *
 * A React error boundary that catches JavaScript errors in child components
 * and displays a fallback UI with retry functionality.
 *
 * Features:
 * - Catches render errors, lifecycle errors, and errors in constructors
 * - Displays user-friendly error messages
 * - Provides retry button to attempt recovery
 * - Optional error reporting callback
 * - Customizable fallback UI
 *
 * @example Basic Usage
 * ```typescript
 * <ErrorBoundary>
 *   <MyComponent />
 * </ErrorBoundary>
 * ```
 *
 * @example With Custom Fallback
 * ```typescript
 * <ErrorBoundary
 *   fallback={({ error, retry }) => (
 *     <div>
 *       <p>Something went wrong: {error.message}</p>
 *       <button onClick={retry}>Try Again</button>
 *     </div>
 *   )}
 * >
 *   <MyComponent />
 * </ErrorBoundary>
 * ```
 */

'use client'

import { Component, type ReactNode, type ErrorInfo } from 'react'

/**
 * Props for the fallback render function
 */
export interface ErrorFallbackProps {
  /** The error that was caught */
  error: Error
  /** Error info from React */
  errorInfo: ErrorInfo | null
  /** Function to reset and retry */
  retry: () => void
  /** Number of times retry has been attempted */
  retryCount: number
}

/**
 * Props for ErrorBoundary component
 */
export interface ErrorBoundaryProps {
  /** Child components to render */
  children: ReactNode
  /** Custom fallback component or render function */
  fallback?: ReactNode | ((props: ErrorFallbackProps) => ReactNode)
  /** Callback when an error is caught */
  onError?: (error: Error, errorInfo: ErrorInfo) => void
  /** Callback when retry is attempted */
  onRetry?: (retryCount: number) => void
  /** Maximum retry attempts before showing permanent error */
  maxRetries?: number
  /** Whether to show detailed error info (dev mode) */
  showDetails?: boolean
}

/**
 * State for ErrorBoundary
 */
interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
  errorInfo: ErrorInfo | null
  retryCount: number
}

/**
 * Default error fallback UI
 */
function DefaultErrorFallback({
  error,
  errorInfo,
  retry,
  retryCount,
  maxRetries = 3,
  showDetails = false,
}: ErrorFallbackProps & { maxRetries?: number; showDetails?: boolean }) {
  const isExhausted = retryCount >= maxRetries

  return (
    <div
      style={{
        padding: '2rem',
        margin: '1rem',
        background: 'var(--theme-error-50, #fef2f2)',
        border: '1px solid var(--theme-error-200, #fecaca)',
        borderRadius: '8px',
        textAlign: 'center',
      }}
      role="alert"
    >
      {/* Icon */}
      <div style={{ marginBottom: '1rem' }}>
        <svg
          width="48"
          height="48"
          viewBox="0 0 24 24"
          fill="none"
          stroke="var(--theme-error-500, #ef4444)"
          strokeWidth="2"
          style={{ margin: '0 auto' }}
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>
      </div>

      {/* Title */}
      <h2
        style={{
          margin: '0 0 0.5rem',
          fontSize: '1.25rem',
          fontWeight: 600,
          color: 'var(--theme-error-700, #b91c1c)',
        }}
      >
        Something went wrong
      </h2>

      {/* Message */}
      <p
        style={{
          margin: '0 0 1.5rem',
          fontSize: '0.875rem',
          color: 'var(--theme-error-600, #dc2626)',
        }}
      >
        {error.message || 'An unexpected error occurred'}
      </p>

      {/* Retry info */}
      {retryCount > 0 && !isExhausted && (
        <p
          style={{
            margin: '0 0 1rem',
            fontSize: '0.75rem',
            color: 'var(--theme-error-500, #ef4444)',
          }}
        >
          Retry attempt {retryCount} of {maxRetries}
        </p>
      )}

      {/* Exhausted message */}
      {isExhausted && (
        <p
          style={{
            margin: '0 0 1rem',
            fontSize: '0.75rem',
            color: 'var(--theme-error-500, #ef4444)',
          }}
        >
          Maximum retry attempts reached. Please refresh the page or contact support.
        </p>
      )}

      {/* Retry button */}
      {!isExhausted && (
        <button
          onClick={retry}
          style={{
            padding: '0.625rem 1.5rem',
            background: 'var(--theme-error-500, #ef4444)',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            fontSize: '0.875rem',
            fontWeight: 500,
            cursor: 'pointer',
            transition: 'background 0.2s',
          }}
          onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--theme-error-600, #dc2626)')}
          onMouseLeave={(e) => (e.currentTarget.style.background = 'var(--theme-error-500, #ef4444)')}
        >
          Try Again
        </button>
      )}

      {/* Refresh button when exhausted */}
      {isExhausted && (
        <button
          onClick={() => window.location.reload()}
          style={{
            padding: '0.625rem 1.5rem',
            background: 'var(--theme-elevation-600, #666)',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            fontSize: '0.875rem',
            fontWeight: 500,
            cursor: 'pointer',
          }}
        >
          Refresh Page
        </button>
      )}

      {/* Error details (dev mode) */}
      {showDetails && errorInfo && (
        <details
          style={{
            marginTop: '1.5rem',
            textAlign: 'left',
            background: 'var(--theme-error-100, #fee2e2)',
            padding: '1rem',
            borderRadius: '6px',
          }}
        >
          <summary
            style={{
              cursor: 'pointer',
              fontSize: '0.75rem',
              fontWeight: 500,
              color: 'var(--theme-error-700, #b91c1c)',
            }}
          >
            Error Details
          </summary>
          <pre
            style={{
              margin: '0.5rem 0 0',
              fontSize: '0.75rem',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              color: 'var(--theme-error-800, #991b1b)',
              fontFamily: 'monospace',
            }}
          >
            {error.stack}
            {'\n\nComponent Stack:'}
            {errorInfo.componentStack}
          </pre>
        </details>
      )}
    </div>
  )
}

/**
 * ErrorBoundary - React component for catching and handling errors
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props)
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
      retryCount: 0,
    }
  }

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return { hasError: true, error }
  }

  override componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    this.setState({ errorInfo })
    this.props.onError?.(error, errorInfo)

    // Log to console in development
    if (process.env.NODE_ENV === 'development') {
      console.error('ErrorBoundary caught an error:', error, errorInfo)
    }
  }

  handleRetry = (): void => {
    const newRetryCount = this.state.retryCount + 1
    this.props.onRetry?.(newRetryCount)
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
      retryCount: newRetryCount,
    })
  }

  override render(): ReactNode {
    const { hasError, error, errorInfo, retryCount } = this.state
    const { children, fallback, maxRetries = 3, showDetails = false } = this.props

    if (hasError && error) {
      // Custom fallback
      if (fallback) {
        if (typeof fallback === 'function') {
          return fallback({
            error,
            errorInfo,
            retry: this.handleRetry,
            retryCount,
          })
        }
        return fallback
      }

      // Default fallback
      return (
        <DefaultErrorFallback
          error={error}
          errorInfo={errorInfo}
          retry={this.handleRetry}
          retryCount={retryCount}
          maxRetries={maxRetries}
          showDetails={showDetails}
        />
      )
    }

    return children
  }
}

export default ErrorBoundary

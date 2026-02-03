/**
 * LoadingSpinner Component
 *
 * A reusable loading spinner with various sizes and optional message.
 *
 * @example Basic Usage
 * ```typescript
 * <LoadingSpinner />
 * ```
 *
 * @example With Message
 * ```typescript
 * <LoadingSpinner message="Loading databases..." />
 * ```
 *
 * @example Different Sizes
 * ```typescript
 * <LoadingSpinner size="sm" />
 * <LoadingSpinner size="lg" />
 * ```
 */

'use client'

/**
 * Available spinner sizes
 */
export type SpinnerSize = 'xs' | 'sm' | 'md' | 'lg' | 'xl'

/**
 * Props for LoadingSpinner component
 */
export interface LoadingSpinnerProps {
  /** Size of the spinner */
  size?: SpinnerSize | undefined
  /** Optional loading message */
  message?: string | undefined
  /** Whether to center the spinner in its container */
  centered?: boolean | undefined
  /** Whether to show a full-page overlay */
  overlay?: boolean | undefined
  /** Additional CSS class name */
  className?: string | undefined
  /** Additional inline styles */
  style?: React.CSSProperties | undefined
}

/**
 * Size configurations
 */
const SIZES: Record<SpinnerSize, { spinner: number; border: number; fontSize: string }> = {
  xs: { spinner: 16, border: 2, fontSize: '0.75rem' },
  sm: { spinner: 20, border: 2, fontSize: '0.8125rem' },
  md: { spinner: 32, border: 3, fontSize: '0.875rem' },
  lg: { spinner: 48, border: 4, fontSize: '1rem' },
  xl: { spinner: 64, border: 5, fontSize: '1.125rem' },
}

/**
 * LoadingSpinner - Consistent loading indicator
 */
export function LoadingSpinner({
  size = 'md',
  message,
  centered = true,
  overlay = false,
  className,
  style,
}: LoadingSpinnerProps) {
  const config = SIZES[size]

  const spinnerElement = (
    <div
      style={{
        width: config.spinner,
        height: config.spinner,
        border: `${config.border}px solid var(--theme-elevation-150, #eee)`,
        borderTopColor: 'var(--theme-elevation-600, #666)',
        borderRadius: '50%',
        animation: 'spin 0.8s linear infinite',
      }}
      role="status"
      aria-label="Loading"
    />
  )

  const content = (
    <div
      className={className}
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: message ? '0.75rem' : 0,
        ...(centered && !overlay
          ? {
              padding: size === 'xl' ? '4rem' : size === 'lg' ? '3rem' : '2rem',
            }
          : {}),
        ...style,
      }}
    >
      {spinnerElement}
      {message && (
        <span
          style={{
            fontSize: config.fontSize,
            color: 'var(--theme-elevation-500, #888)',
          }}
        >
          {message}
        </span>
      )}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )

  if (overlay) {
    return (
      <div
        style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(255, 255, 255, 0.8)',
          backdropFilter: 'blur(2px)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 9999,
        }}
      >
        {content}
      </div>
    )
  }

  return content
}

export default LoadingSpinner

/**
 * ConfirmationDialog Component
 *
 * A reusable modal dialog for confirming destructive actions like delete operations.
 * Provides clear warning messages with cancel and confirm buttons.
 *
 * @example
 * ```tsx
 * <ConfirmationDialog
 *   isOpen={showDeleteConfirm}
 *   onClose={() => setShowDeleteConfirm(false)}
 *   onConfirm={handleDelete}
 *   title="Delete Database"
 *   message="Are you sure you want to delete this database? This action cannot be undone."
 *   confirmLabel="Delete"
 *   variant="danger"
 * />
 * ```
 */

'use client'

import { useEffect, useRef, useCallback } from 'react'

export type ConfirmationVariant = 'danger' | 'warning' | 'info'

export interface ConfirmationDialogProps {
  /** Whether the dialog is open */
  isOpen: boolean
  /** Callback when dialog should close */
  onClose: () => void
  /** Callback when action is confirmed */
  onConfirm: () => void | Promise<void>
  /** Dialog title */
  title: string
  /** Main message/warning to display */
  message: string
  /** Optional additional details or consequences */
  details?: string
  /** Label for cancel button (default: "Cancel") */
  cancelLabel?: string
  /** Label for confirm button (default: "Confirm") */
  confirmLabel?: string
  /** Visual variant affecting confirm button styling (default: "danger") */
  variant?: ConfirmationVariant
  /** Whether confirm action is in progress */
  loading?: boolean
  /** Item name to display (for "Delete X" patterns) */
  itemName?: string
  /** Item type for context (e.g., "database", "entity", "collection") */
  itemType?: string
}

/**
 * Get variant-specific styles
 */
function getVariantStyles(variant: ConfirmationVariant) {
  switch (variant) {
    case 'danger':
      return {
        iconColor: 'var(--theme-error-500, #dc2626)',
        iconBg: 'var(--theme-error-100, #fef2f2)',
        buttonBg: 'var(--theme-error-500, #dc2626)',
        buttonHoverBg: 'var(--theme-error-600, #b91c1c)',
      }
    case 'warning':
      return {
        iconColor: 'var(--theme-warning-500, #d97706)',
        iconBg: 'var(--theme-warning-100, #fef3c7)',
        buttonBg: 'var(--theme-warning-500, #d97706)',
        buttonHoverBg: 'var(--theme-warning-600, #b45309)',
      }
    case 'info':
    default:
      return {
        iconColor: 'var(--theme-elevation-600, #666)',
        iconBg: 'var(--theme-elevation-100, #f5f5f5)',
        buttonBg: 'var(--theme-elevation-900, #111)',
        buttonHoverBg: 'var(--theme-elevation-700, #333)',
      }
  }
}

/**
 * Warning icon SVG component
 */
function WarningIcon({ color }: { color: string }) {
  return (
    <svg
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke={color}
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
      <line x1="12" y1="9" x2="12" y2="13" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
    </svg>
  )
}

/**
 * Trash icon SVG component
 */
function TrashIcon({ color }: { color: string }) {
  return (
    <svg
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke={color}
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <polyline points="3 6 5 6 21 6" />
      <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
      <line x1="10" y1="11" x2="10" y2="17" />
      <line x1="14" y1="11" x2="14" y2="17" />
    </svg>
  )
}

/**
 * Confirmation Dialog Component
 *
 * A modal dialog for confirming destructive operations with clear warning messages.
 */
export function ConfirmationDialog({
  isOpen,
  onClose,
  onConfirm,
  title,
  message,
  details,
  cancelLabel = 'Cancel',
  confirmLabel = 'Confirm',
  variant = 'danger',
  loading = false,
  itemName,
  itemType,
}: ConfirmationDialogProps) {
  const confirmButtonRef = useRef<HTMLButtonElement>(null)
  const styles = getVariantStyles(variant)

  // Handle escape key
  const handleKeyDown = useCallback((event: KeyboardEvent) => {
    if (event.key === 'Escape' && !loading) {
      onClose()
    }
  }, [onClose, loading])

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown)
      // Focus the cancel button for safety (not the confirm button)
      return () => {
        document.removeEventListener('keydown', handleKeyDown)
      }
    }
    return undefined
  }, [isOpen, handleKeyDown])

  // Handle confirm with async support
  const handleConfirm = async () => {
    try {
      await onConfirm()
    } catch (error) {
      // Let the parent handle errors
      console.error('[ConfirmationDialog] Confirm action failed:', error)
    }
  }

  if (!isOpen) {
    return null
  }

  return (
    <div
      className="confirmation-dialog-backdrop"
      role="dialog"
      aria-modal="true"
      aria-labelledby="confirmation-dialog-title"
      aria-describedby="confirmation-dialog-message"
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1100,
        padding: '1rem',
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget && !loading) {
          onClose()
        }
      }}
    >
      <div
        className="confirmation-dialog-content"
        style={{
          background: 'var(--theme-elevation-0, #fff)',
          borderRadius: '12px',
          padding: '1.5rem',
          width: '100%',
          maxWidth: '420px',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
        }}
      >
        {/* Icon and title */}
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: '1rem', marginBottom: '1rem' }}>
          <div
            style={{
              padding: '0.75rem',
              borderRadius: '50%',
              background: styles.iconBg,
              flexShrink: 0,
            }}
          >
            {variant === 'danger' ? (
              <TrashIcon color={styles.iconColor} />
            ) : (
              <WarningIcon color={styles.iconColor} />
            )}
          </div>
          <div>
            <h2
              id="confirmation-dialog-title"
              style={{
                margin: 0,
                fontSize: '1.125rem',
                fontWeight: 600,
                color: 'var(--theme-text, #111)',
              }}
            >
              {title}
            </h2>
            {itemName && (
              <p style={{
                margin: '0.25rem 0 0',
                fontSize: '0.875rem',
                color: 'var(--theme-elevation-600, #666)',
                fontWeight: 500,
              }}>
                {itemType ? `${itemType}: ` : ''}{itemName}
              </p>
            )}
          </div>
        </div>

        {/* Message */}
        <div
          id="confirmation-dialog-message"
          style={{
            marginBottom: '1.5rem',
            paddingLeft: '3.5rem',
          }}
        >
          <p style={{
            margin: 0,
            fontSize: '0.875rem',
            color: 'var(--theme-elevation-700, #444)',
            lineHeight: 1.5,
          }}>
            {message}
          </p>
          {details && (
            <p style={{
              margin: '0.75rem 0 0',
              fontSize: '0.8125rem',
              color: 'var(--theme-elevation-500, #666)',
              lineHeight: 1.5,
            }}>
              {details}
            </p>
          )}
        </div>

        {/* Actions */}
        <div style={{
          display: 'flex',
          gap: '0.75rem',
          justifyContent: 'flex-end',
        }}>
          <button
            type="button"
            onClick={onClose}
            disabled={loading}
            style={{
              padding: '0.625rem 1.25rem',
              background: 'transparent',
              border: '1px solid var(--theme-elevation-200, #ddd)',
              borderRadius: '6px',
              fontSize: '0.875rem',
              cursor: loading ? 'not-allowed' : 'pointer',
              color: 'var(--theme-text, #333)',
              opacity: loading ? 0.6 : 1,
            }}
          >
            {cancelLabel}
          </button>
          <button
            ref={confirmButtonRef}
            type="button"
            onClick={handleConfirm}
            disabled={loading}
            style={{
              padding: '0.625rem 1.5rem',
              background: loading ? 'var(--theme-elevation-300, #ccc)' : styles.buttonBg,
              color: 'var(--theme-elevation-0, #fff)',
              border: 'none',
              borderRadius: '6px',
              fontSize: '0.875rem',
              fontWeight: 500,
              cursor: loading ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
            }}
            onMouseEnter={(e) => {
              if (!loading) {
                e.currentTarget.style.background = styles.buttonHoverBg
              }
            }}
            onMouseLeave={(e) => {
              if (!loading) {
                e.currentTarget.style.background = styles.buttonBg
              }
            }}
          >
            {loading && (
              <span
                style={{
                  width: '14px',
                  height: '14px',
                  border: '2px solid transparent',
                  borderTopColor: 'currentColor',
                  borderRadius: '50%',
                  animation: 'confirmation-spin 0.6s linear infinite',
                }}
              />
            )}
            {loading ? 'Deleting...' : confirmLabel}
          </button>
        </div>

        {/* Spinner animation */}
        <style>{`
          @keyframes confirmation-spin {
            to { transform: rotate(360deg); }
          }
        `}</style>
      </div>
    </div>
  )
}

export default ConfirmationDialog

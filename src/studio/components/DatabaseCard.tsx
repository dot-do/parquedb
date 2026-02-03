/**
 * DatabaseCard Component
 *
 * Displays a single database in the dashboard grid.
 * Uses Payload's native UI components for consistent styling.
 * Includes delete functionality with confirmation dialog.
 */

'use client'

import { useState } from 'react'
import type { DatabaseInfo } from '../../worker/DatabaseIndexDO'
import { ConfirmationDialog } from './ConfirmationDialog'

export interface DatabaseCardProps {
  database: DatabaseInfo
  basePath?: string
  onSelect?: (database: DatabaseInfo) => void
  /** Callback when delete is confirmed */
  onDelete?: (database: DatabaseInfo) => Promise<void>
  /** Whether delete is in progress */
  deleting?: boolean
}

/**
 * Format relative time for display
 */
function formatRelativeTime(date: Date | undefined): string {
  if (!date) return 'Never'

  const now = new Date()
  const diff = now.getTime() - date.getTime()

  if (diff < 60000) return 'Just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  if (diff < 604800000) return `${Math.floor(diff / 86400000)}d ago`

  return date.toLocaleDateString()
}

/**
 * Format file size for display
 */
function formatSize(bytes: number | undefined): string {
  if (!bytes) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

/**
 * Database card component for the dashboard
 */
export function DatabaseCard({
  database,
  basePath = '/admin',
  onSelect,
  onDelete,
  deleting = false,
}: DatabaseCardProps) {
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const href = `${basePath}/${database.id}`

  const handleClick = (e: React.MouseEvent) => {
    if (onSelect) {
      e.preventDefault()
      onSelect(database)
    }
  }

  const handleDeleteClick = (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setShowDeleteConfirm(true)
  }

  const handleDeleteConfirm = async () => {
    if (onDelete) {
      await onDelete(database)
      setShowDeleteConfirm(false)
    }
  }

  return (
    <>
      <a
        href={href}
        onClick={handleClick}
        className="database-card"
        style={{
          display: 'block',
          padding: '1.5rem',
          background: 'var(--theme-elevation-50, #fff)',
          borderRadius: '8px',
          border: '1px solid var(--theme-elevation-100, #e5e5e5)',
          textDecoration: 'none',
          color: 'inherit',
          transition: 'all 0.2s ease',
          position: 'relative',
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.borderColor = 'var(--theme-elevation-400, #0066cc)'
          e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)'
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.borderColor = 'var(--theme-elevation-100, #e5e5e5)'
          e.currentTarget.style.boxShadow = 'none'
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
          <div style={{ flex: 1 }}>
            <h3 style={{
              margin: 0,
              fontSize: '1.125rem',
              fontWeight: 600,
              color: 'var(--theme-text, #333)',
            }}>
              {database.name}
            </h3>
            {database.owner && database.slug && (
              <span style={{
                fontSize: '0.875rem',
                color: 'var(--theme-elevation-500, #666)',
              }}>
                {database.owner}/{database.slug}
              </span>
            )}
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            {/* Delete button */}
            {onDelete && (
              <button
                onClick={handleDeleteClick}
                disabled={deleting}
                title="Delete database"
                aria-label={`Delete ${database.name}`}
                style={{
                  padding: '0.375rem',
                  background: 'transparent',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: deleting ? 'not-allowed' : 'pointer',
                  color: 'var(--theme-elevation-400, #999)',
                  opacity: deleting ? 0.5 : 1,
                  transition: 'color 0.2s, background 0.2s',
                }}
                onMouseEnter={(e) => {
                  if (!deleting) {
                    e.currentTarget.style.color = 'var(--theme-error-500, #dc2626)'
                    e.currentTarget.style.background = 'var(--theme-error-100, #fef2f2)'
                  }
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = 'var(--theme-elevation-400, #999)'
                  e.currentTarget.style.background = 'transparent'
                }}
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polyline points="3 6 5 6 21 6" />
                  <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                </svg>
              </button>
            )}

            {/* Visibility badge */}
            <span
              style={{
                padding: '0.25rem 0.5rem',
                borderRadius: '4px',
                fontSize: '0.75rem',
                fontWeight: 500,
                textTransform: 'uppercase',
                background: database.visibility === 'public'
                  ? 'var(--theme-success-100, #dcfce7)'
                  : database.visibility === 'unlisted'
                    ? 'var(--theme-warning-100, #fef3c7)'
                    : 'var(--theme-elevation-100, #f5f5f5)',
                color: database.visibility === 'public'
                  ? 'var(--theme-success-500, #16a34a)'
                  : database.visibility === 'unlisted'
                    ? 'var(--theme-warning-500, #d97706)'
                    : 'var(--theme-elevation-500, #666)',
              }}
            >
              {database.visibility}
            </span>
          </div>
        </div>

        {/* Description */}
        {database.description && (
          <p style={{
            margin: '0 0 1rem 0',
            fontSize: '0.875rem',
            color: 'var(--theme-elevation-600, #555)',
            lineHeight: 1.5,
          }}>
            {database.description}
          </p>
        )}

        {/* Stats */}
        <div style={{
          display: 'flex',
          gap: '1.5rem',
          fontSize: '0.8125rem',
          color: 'var(--theme-elevation-500, #888)',
        }}>
          <span title="Collections">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: '0.25rem', verticalAlign: 'middle' }}>
              <rect x="3" y="3" width="7" height="7" />
              <rect x="14" y="3" width="7" height="7" />
              <rect x="3" y="14" width="7" height="7" />
              <rect x="14" y="14" width="7" height="7" />
            </svg>
            {database.collectionCount ?? 0} collections
          </span>

          <span title="Entities">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: '0.25rem', verticalAlign: 'middle' }}>
              <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
              <circle cx="9" cy="7" r="4" />
            </svg>
            {(database.entityCount ?? 0).toLocaleString()} entities
          </span>

          <span title="Size">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: '0.25rem', verticalAlign: 'middle' }}>
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="7,10 12,15 17,10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            {formatSize(database.sizeBytes)}
          </span>
        </div>

        {/* Footer - Last accessed */}
        <div style={{
          marginTop: '1rem',
          paddingTop: '0.75rem',
          borderTop: '1px solid var(--theme-elevation-100, #eee)',
          fontSize: '0.75rem',
          color: 'var(--theme-elevation-400, #999)',
        }}>
          Last accessed {formatRelativeTime(database.lastAccessedAt)}
        </div>
      </a>

      {/* Delete confirmation dialog */}
      <ConfirmationDialog
        isOpen={showDeleteConfirm}
        onClose={() => setShowDeleteConfirm(false)}
        onConfirm={handleDeleteConfirm}
        title="Delete Database"
        itemName={database.name}
        itemType="Database"
        message="Are you sure you want to delete this database? This will unregister the database from your account."
        details="Note: The underlying data in storage will not be deleted and can be re-registered later."
        confirmLabel="Delete Database"
        cancelLabel="Cancel"
        variant="danger"
        loading={deleting}
      />
    </>
  )
}

export default DatabaseCard

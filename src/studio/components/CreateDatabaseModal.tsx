/**
 * CreateDatabaseModal Component
 *
 * Modal for creating a new database with name, description, and visibility settings.
 */

'use client'

import { useState } from 'react'
import type { DatabaseInfo } from '../../worker/DatabaseIndexDO'
import type { Visibility } from '../../types/visibility'
import { ErrorDisplay } from './ErrorDisplay'

export interface CreateDatabaseModalProps {
  /** Close the modal */
  onClose: () => void
  /** Callback when database is created */
  onCreate: (database: DatabaseInfo) => void
  /** API endpoint for creating database */
  apiEndpoint?: string | undefined
}

/**
 * Modal for creating a new database
 */
export function CreateDatabaseModal({
  onClose,
  onCreate,
  apiEndpoint = '/api/databases/create',
}: CreateDatabaseModalProps) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [slug, setSlug] = useState('')
  const [visibility, setVisibility] = useState<Visibility>('private')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Auto-generate slug from name
  const handleNameChange = (value: string) => {
    setName(value)
    // Only auto-generate if user hasn't manually edited the slug
    if (!slug || slug === generateSlug(name)) {
      setSlug(generateSlug(value))
    }
  }

  const handleSubmit = async (e: React.FormEvent, retryCount = 0) => {
    e.preventDefault()
    setError(null)

    if (!name.trim()) {
      setError('Database name is required')
      return
    }

    const maxRetries = 2

    try {
      setLoading(true)
      const response = await fetch(apiEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest', // CSRF protection
        },
        body: JSON.stringify({
          name: name.trim(),
          description: description.trim() || undefined,
          slug: slug.trim() || undefined,
          visibility,
        }),
      })

      if (!response.ok) {
        const data = await response.json().catch(() => ({})) as { error?: string | undefined }
        const error = new Error(data.error || `Failed to create database (${response.status})`)

        // Retry on server errors (5xx) but not on client errors (4xx)
        if (response.status >= 500 && retryCount < maxRetries) {
          console.warn(`[CreateDatabase] Server error, retrying (${retryCount + 1}/${maxRetries})...`)
          const delay = Math.pow(2, retryCount) * 1000 // Exponential backoff
          await new Promise(resolve => setTimeout(resolve, delay))
          setLoading(false)
          return handleSubmit(e, retryCount + 1)
        }

        throw error
      }

      const database = await response.json() as DatabaseInfo
      onCreate(database)
    } catch (err) {
      // Provide user-friendly error messages
      const errorMessage = err instanceof Error ? err.message : 'Failed to create database'
      const userMessage = errorMessage.includes('Failed to fetch')
        ? 'Unable to connect to the server. Please check your connection and try again.'
        : errorMessage
      setError(userMessage)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div
      className="modal-backdrop"
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        className="modal-content"
        style={{
          background: 'var(--theme-elevation-0, #fff)',
          borderRadius: '12px',
          padding: '2rem',
          width: '100%',
          maxWidth: '480px',
          maxHeight: '90vh',
          overflow: 'auto',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
          <h2 style={{ margin: 0, fontSize: '1.25rem', fontWeight: 600, color: 'var(--theme-text, #111)' }}>
            Create New Database
          </h2>
          <button
            onClick={onClose}
            style={{
              padding: '0.5rem',
              background: 'transparent',
              border: 'none',
              cursor: 'pointer',
              color: 'var(--theme-elevation-500, #888)',
            }}
            aria-label="Close"
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        {/* Error message */}
        {error && (
          <ErrorDisplay
            error={error}
            compact
            dismissible
            onDismiss={() => setError(null)}
            style={{ marginBottom: '1rem' }}
          />
        )}

        {/* Form */}
        <form onSubmit={handleSubmit}>
          {/* Name field */}
          <div style={{ marginBottom: '1.25rem' }}>
            <label style={{
              display: 'block',
              marginBottom: '0.5rem',
              fontSize: '0.875rem',
              fontWeight: 500,
              color: 'var(--theme-text, #333)',
            }}>
              Name <span style={{ color: 'var(--theme-error-500, #dc2626)' }}>*</span>
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => handleNameChange(e.target.value)}
              placeholder="My Database"
              required
              autoFocus
              style={{
                width: '100%',
                padding: '0.625rem 1rem',
                border: '1px solid var(--theme-elevation-150, #ddd)',
                borderRadius: '6px',
                fontSize: '0.875rem',
                background: 'var(--theme-input-bg, #fff)',
                color: 'var(--theme-text, #333)',
              }}
            />
          </div>

          {/* Description field */}
          <div style={{ marginBottom: '1.25rem' }}>
            <label style={{
              display: 'block',
              marginBottom: '0.5rem',
              fontSize: '0.875rem',
              fontWeight: 500,
              color: 'var(--theme-text, #333)',
            }}>
              Description
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="A brief description of your database"
              rows={3}
              style={{
                width: '100%',
                padding: '0.625rem 1rem',
                border: '1px solid var(--theme-elevation-150, #ddd)',
                borderRadius: '6px',
                fontSize: '0.875rem',
                background: 'var(--theme-input-bg, #fff)',
                color: 'var(--theme-text, #333)',
                resize: 'vertical',
              }}
            />
          </div>

          {/* Slug field */}
          <div style={{ marginBottom: '1.25rem' }}>
            <label style={{
              display: 'block',
              marginBottom: '0.5rem',
              fontSize: '0.875rem',
              fontWeight: 500,
              color: 'var(--theme-text, #333)',
            }}>
              URL Slug
            </label>
            <input
              type="text"
              value={slug}
              onChange={(e) => setSlug(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))}
              placeholder="my-database"
              pattern="^[a-z0-9][a-z0-9-]*[a-z0-9]$"
              style={{
                width: '100%',
                padding: '0.625rem 1rem',
                border: '1px solid var(--theme-elevation-150, #ddd)',
                borderRadius: '6px',
                fontSize: '0.875rem',
                background: 'var(--theme-input-bg, #fff)',
                color: 'var(--theme-text, #333)',
                fontFamily: 'monospace',
              }}
            />
            <p style={{
              margin: '0.375rem 0 0',
              fontSize: '0.75rem',
              color: 'var(--theme-elevation-500, #888)',
            }}>
              Used for public URLs: parque.db/username/{slug || 'my-database'}
            </p>
          </div>

          {/* Visibility field */}
          <div style={{ marginBottom: '1.5rem' }}>
            <label style={{
              display: 'block',
              marginBottom: '0.5rem',
              fontSize: '0.875rem',
              fontWeight: 500,
              color: 'var(--theme-text, #333)',
            }}>
              Visibility
            </label>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              {([
                { value: 'private', label: 'Private', desc: 'Only you can access' },
                { value: 'unlisted', label: 'Unlisted', desc: 'Anyone with link can access' },
                { value: 'public', label: 'Public', desc: 'Discoverable by anyone' },
              ] as const).map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => setVisibility(option.value)}
                  style={{
                    flex: 1,
                    padding: '0.75rem',
                    border: `2px solid ${visibility === option.value ? 'var(--theme-elevation-800, #333)' : 'var(--theme-elevation-150, #ddd)'}`,
                    borderRadius: '6px',
                    background: visibility === option.value ? 'var(--theme-elevation-50, #f9f9f9)' : 'transparent',
                    cursor: 'pointer',
                    textAlign: 'left',
                  }}
                >
                  <div style={{
                    fontWeight: 500,
                    fontSize: '0.875rem',
                    color: 'var(--theme-text, #333)',
                    marginBottom: '0.25rem',
                  }}>
                    {option.label}
                  </div>
                  <div style={{
                    fontSize: '0.75rem',
                    color: 'var(--theme-elevation-500, #888)',
                  }}>
                    {option.desc}
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
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
                cursor: 'pointer',
                color: 'var(--theme-text, #333)',
              }}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading || !name.trim()}
              style={{
                padding: '0.625rem 1.5rem',
                background: loading || !name.trim() ? 'var(--theme-elevation-300, #ccc)' : 'var(--theme-elevation-900, #111)',
                color: 'var(--theme-elevation-0, #fff)',
                border: 'none',
                borderRadius: '6px',
                fontSize: '0.875rem',
                fontWeight: 500,
                cursor: loading || !name.trim() ? 'not-allowed' : 'pointer',
              }}
            >
              {loading ? 'Creating...' : 'Create Database'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

/**
 * Generate URL-friendly slug from name
 */
function generateSlug(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
}

export default CreateDatabaseModal

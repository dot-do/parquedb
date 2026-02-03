/**
 * DatabaseDashboard Component
 *
 * Main dashboard view showing all databases for a user.
 * Integrates with Payload CMS admin panel as a custom root view.
 * Includes delete functionality with confirmation dialogs.
 *
 * Features:
 * - Automatic retry with exponential backoff on fetch failures
 * - Clear error messages with user-friendly descriptions
 * - Loading states during fetches and retries
 * - Error boundaries for render errors
 *
 * @example Payload Config Integration
 * ```typescript
 * // payload.config.ts
 * import { DatabaseDashboard } from 'parquedb/studio'
 *
 * export default buildConfig({
 *   admin: {
 *     components: {
 *       views: {
 *         // Replace default dashboard with database selector
 *         Dashboard: DatabaseDashboard,
 *       },
 *     },
 *   },
 * })
 * ```
 */

'use client'

import { useState, useCallback } from 'react'
import type { DatabaseInfo } from '../../worker/DatabaseIndexDO'
import { DatabaseCard } from './DatabaseCard'
import { CreateDatabaseModal } from './CreateDatabaseModal'
import { CloneDatabaseModal } from './CloneDatabaseModal'
import { QuickSwitcher } from './QuickSwitcher'
import { ConfirmationDialog } from './ConfirmationDialog'
import { ErrorDisplay } from './ErrorDisplay'
import { LoadingSpinner } from './LoadingSpinner'
import { ErrorBoundary } from './ErrorBoundary'
import { useRetry } from './hooks/useRetry'
import { useKeyboardNavigation } from './hooks/useKeyboardNavigation'
import { ResponsiveStyles } from './ResponsiveStyles'

export interface DatabaseDashboardProps {
  /** Base path for admin routes (default: '/admin') */
  basePath?: string | undefined
  /** Initial databases (for SSR) */
  initialDatabases?: DatabaseInfo[] | undefined
  /** API endpoint for fetching databases */
  apiEndpoint?: string | undefined
  /** Callback when database is selected */
  onSelectDatabase?: ((database: DatabaseInfo) => void) | undefined
  /** Show create button */
  showCreate?: boolean | undefined
  /** Show delete buttons on cards */
  showDelete?: boolean | undefined
  /** Custom header component */
  header?: React.ReactNode | undefined
}

/**
 * Database Dashboard - Main view for selecting/managing databases
 */
export function DatabaseDashboard({
  basePath = '/admin',
  initialDatabases = [],
  apiEndpoint = '/api/databases',
  onSelectDatabase,
  showCreate = true,
  showDelete = true,
  header,
}: DatabaseDashboardProps) {
  const [localDatabases, setLocalDatabases] = useState<DatabaseInfo[]>(initialDatabases)
  const [searchQuery, setSearchQuery] = useState('')
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [sortBy, setSortBy] = useState<'name' | 'lastAccessed' | 'created'>('lastAccessed')
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  // Batch delete state
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [showBatchDeleteConfirm, setShowBatchDeleteConfirm] = useState(false)
  const [batchDeleting, setBatchDeleting] = useState(false)

  // Clone state
  const [cloneSource, setCloneSource] = useState<DatabaseInfo | null>(null)

  // Use retry hook for fetching databases with exponential backoff
  const {
    data: fetchedData,
    loading,
    error,
    retryCount,
    isRetrying,
    isExhausted,
    nextRetryIn,
    retry: retryFetch,
    reset: resetFetch,
  } = useRetry<{ databases: DatabaseInfo[] }>(
    async () => {
      const response = await fetch(apiEndpoint)
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({})) as { error?: string | undefined }
        throw new Error(errorData.error || `Failed to fetch databases (${response.status})`)
      }
      return response.json()
    },
    {
      fetchOnMount: !initialDatabases.length,
      maxRetries: 3,
      baseDelay: 1000,
      autoRetry: true,
      onError: (err, count) => {
        console.error(`[DatabaseDashboard] Fetch error (attempt ${count}):`, err.message)
      },
      onSuccess: (data) => {
        // Sync local state with fetched data
        const result = data as { databases: DatabaseInfo[] }
        setLocalDatabases(result.databases || [])
      },
      onExhausted: (err) => {
        console.error('[DatabaseDashboard] Max retries exhausted:', err.message)
      },
    }
  )

  // Merge fetched data with local state (for optimistic updates)
  const databases = fetchedData?.databases ?? localDatabases

  // Filter and sort databases
  const filteredDatabases = databases
    .filter((db) => {
      if (!searchQuery) return true
      const query = searchQuery.toLowerCase()
      return (
        db.name.toLowerCase().includes(query) ||
        db.description?.toLowerCase().includes(query) ||
        db.slug?.toLowerCase().includes(query)
      )
    })
    .sort((a, b) => {
      switch (sortBy) {
        case 'name':
          return a.name.localeCompare(b.name)
        case 'lastAccessed':
          return (b.lastAccessedAt?.getTime() ?? 0) - (a.lastAccessedAt?.getTime() ?? 0)
        case 'created':
          return b.createdAt.getTime() - a.createdAt.getTime()
        default:
          return 0
      }
    })

  const handleDatabaseCreated = (database: DatabaseInfo) => {
    setLocalDatabases([database, ...databases])
    setShowCreateModal(false)
    if (onSelectDatabase) {
      onSelectDatabase(database)
    } else {
      // Navigate to the new database
      window.location.href = `${basePath}/${database.id}`
    }
  }

  // Delete a single database with retry support
  const handleDeleteDatabase = async (database: DatabaseInfo, retries = 0): Promise<void> => {
    const maxDeleteRetries = 2
    setDeletingId(database.id)
    setDeleteError(null)

    try {
      const response = await fetch(`${apiEndpoint}/${database.id}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest', // CSRF protection
        },
      })

      if (!response.ok) {
        const data = await response.json().catch(() => ({})) as { error?: string | undefined }
        const error = new Error(data.error || `Failed to delete database (${response.status})`)

        // Retry on server errors (5xx) but not on client errors (4xx)
        if (response.status >= 500 && retries < maxDeleteRetries) {
          console.warn(`[DatabaseDashboard] Delete failed, retrying (${retries + 1}/${maxDeleteRetries})...`)
          const delay = Math.pow(2, retries) * 1000 // Exponential backoff
          await new Promise(resolve => setTimeout(resolve, delay))
          return handleDeleteDatabase(database, retries + 1)
        }

        throw error
      }

      // Remove from local state (optimistic update)
      setLocalDatabases(prev => prev.filter(db => db.id !== database.id))
      setSelectedIds(prev => {
        const next = new Set(prev)
        next.delete(database.id)
        return next
      })
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to delete database'
      setDeleteError(`Could not delete "${database.name}": ${errorMessage}`)
    } finally {
      setDeletingId(null)
    }
  }

  // Batch delete selected databases with retry logic per database
  const handleBatchDelete = async () => {
    if (selectedIds.size === 0) return

    setBatchDeleting(true)
    setDeleteError(null)

    const errors: string[] = []
    const deletedIds: string[] = []
    const maxRetries = 2

    for (const id of selectedIds) {
      let success = false
      let lastError = ''

      for (let attempt = 0; attempt <= maxRetries && !success; attempt++) {
        try {
          // Add delay for retries (exponential backoff)
          if (attempt > 0) {
            await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt - 1) * 1000))
          }

          const response = await fetch(`${apiEndpoint}/${id}`, {
            method: 'DELETE',
            headers: {
              'Content-Type': 'application/json',
              'X-Requested-With': 'XMLHttpRequest',
            },
          })

          if (response.ok) {
            success = true
            deletedIds.push(id)
          } else {
            const data = await response.json().catch(() => ({})) as { error?: string | undefined }
            lastError = data.error || `HTTP ${response.status}`

            // Don't retry on client errors (4xx)
            if (response.status < 500) break
          }
        } catch (err) {
          lastError = err instanceof Error ? err.message : 'Network error'
        }
      }

      if (!success) {
        const db = databases.find(d => d.id === id)
        errors.push(`${db?.name || id}: ${lastError}`)
      }
    }

    // Update local state (optimistic update)
    setLocalDatabases(prev => prev.filter(db => !deletedIds.includes(db.id)))
    setSelectedIds(new Set())
    setShowBatchDeleteConfirm(false)
    setBatchDeleting(false)

    if (errors.length > 0) {
      const successCount = deletedIds.length
      const failCount = errors.length
      const header = successCount > 0
        ? `Deleted ${successCount} database${successCount !== 1 ? 's' : ''}, but ${failCount} failed:`
        : `Could not delete ${failCount} database${failCount !== 1 ? 's' : ''}:`
      setDeleteError(`${header}\n${errors.join('\n')}`)
    }
  }

  // Toggle selection for batch delete
  const toggleSelection = (id: string) => {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  // Select all visible databases
  const selectAll = () => {
    if (selectedIds.size === filteredDatabases.length) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(filteredDatabases.map(db => db.id)))
    }
  }

  // Handle database clone
  const handleCloneDatabase = useCallback((database: DatabaseInfo) => {
    setCloneSource(database)
  }, [])

  const handleDatabaseCloned = useCallback((newDatabase: DatabaseInfo) => {
    setLocalDatabases(prev => [newDatabase, ...prev])
    setCloneSource(null)
    if (onSelectDatabase) {
      onSelectDatabase(newDatabase)
    } else {
      window.location.href = `${basePath}/${newDatabase.id}`
    }
  }, [onSelectDatabase, basePath])

  // Keyboard navigation for database grid
  const {
    focusedIndex,
    setFocusedIndex,
    handleKeyDown: gridKeyDown,
    containerRef: gridRef,
  } = useKeyboardNavigation({
    itemCount: filteredDatabases.length,
    columns: 3, // Approximate grid columns
    onSelect: (index) => {
      const db = filteredDatabases[index]
      if (db) {
        if (onSelectDatabase) {
          onSelectDatabase(db)
        } else {
          window.location.href = `${basePath}/${db.id}`
        }
      }
    },
    onEscape: () => {
      setSearchQuery('')
    },
    onDelete: (index) => {
      const db = filteredDatabases[index]
      if (db && showDelete) {
        handleDeleteDatabase(db)
      }
    },
    enabled: !showCreateModal && !cloneSource && !showBatchDeleteConfirm,
  })

  return (
    <ErrorBoundary
      onError={(err, info) => {
        console.error('[DatabaseDashboard] Render error:', err, info)
      }}
      maxRetries={2}
      showDetails={process.env.NODE_ENV === 'development'}
    >
    <div className="database-dashboard" style={{ padding: '2rem', maxWidth: '1400px', margin: '0 auto' }}>
      <ResponsiveStyles />
      {/* Header */}
      {header || (
        <div className="dashboard-header" style={{ marginBottom: '2rem' }}>
          <h1 style={{
            margin: 0,
            fontSize: '1.75rem',
            fontWeight: 600,
            color: 'var(--theme-text, #111)',
          }}>
            Your Databases
          </h1>
          <p style={{
            margin: '0.5rem 0 0',
            color: 'var(--theme-elevation-600, #666)',
          }}>
            Select a database to manage or create a new one
          </p>
        </div>
      )}

      {/* Delete error notification */}
      {deleteError && (
        <ErrorDisplay
          error={deleteError}
          title="Delete Operation Failed"
          dismissible
          onDismiss={() => setDeleteError(null)}
          style={{ marginBottom: '1rem' }}
        />
      )}

      {/* Toolbar */}
      <div className="dashboard-toolbar" style={{
        display: 'flex',
        gap: '1rem',
        marginBottom: '1.5rem',
        flexWrap: 'wrap',
        alignItems: 'center',
      }}>
        {/* Batch selection controls */}
        {showDelete && filteredDatabases.length > 0 && (
          <div className="batch-controls" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <input
              type="checkbox"
              checked={selectedIds.size === filteredDatabases.length && filteredDatabases.length > 0}
              onChange={selectAll}
              style={{ width: '18px', height: '18px', cursor: 'pointer' }}
              title={selectedIds.size === filteredDatabases.length ? 'Deselect all' : 'Select all'}
            />
            {selectedIds.size > 0 && (
              <button
                onClick={() => setShowBatchDeleteConfirm(true)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.5rem 0.75rem',
                  background: 'var(--theme-error-500, #dc2626)',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '0.8125rem',
                  fontWeight: 500,
                  cursor: 'pointer',
                }}
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polyline points="3 6 5 6 21 6" />
                  <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                </svg>
                Delete ({selectedIds.size})
              </button>
            )}
          </div>
        )}

        {/* Search */}
        <div className="toolbar-search" style={{ position: 'relative', flex: '1 1 300px' }}>
          <input
            type="text"
            placeholder="Search databases..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            style={{
              width: '100%',
              padding: '0.625rem 1rem 0.625rem 2.5rem',
              border: '1px solid var(--theme-elevation-150, #ddd)',
              borderRadius: '6px',
              fontSize: '0.875rem',
              background: 'var(--theme-input-bg, #fff)',
              color: 'var(--theme-text, #333)',
            }}
          />
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            style={{
              position: 'absolute',
              left: '0.75rem',
              top: '50%',
              transform: 'translateY(-50%)',
              color: 'var(--theme-elevation-400, #999)',
            }}
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        </div>

        {/* Sort and Create */}
        <div className="toolbar-actions" style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
          {/* Sort dropdown */}
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
            style={{
              padding: '0.625rem 1rem',
              border: '1px solid var(--theme-elevation-150, #ddd)',
              borderRadius: '6px',
              fontSize: '0.875rem',
              background: 'var(--theme-input-bg, #fff)',
              color: 'var(--theme-text, #333)',
              cursor: 'pointer',
            }}
          >
            <option value="lastAccessed">Last Accessed</option>
            <option value="name">Name</option>
            <option value="created">Date Created</option>
          </select>

          {/* Create button */}
          {showCreate && (
            <button
              className="btn-create"
              onClick={() => setShowCreateModal(true)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.625rem 1.25rem',
                background: 'var(--theme-elevation-900, #111)',
                color: 'var(--theme-elevation-0, #fff)',
                border: 'none',
                borderRadius: '6px',
                fontSize: '0.875rem',
                fontWeight: 500,
                cursor: 'pointer',
                transition: 'background 0.2s',
              }}
              onMouseEnter={(e) => e.currentTarget.style.background = 'var(--theme-elevation-700, #333)'}
              onMouseLeave={(e) => e.currentTarget.style.background = 'var(--theme-elevation-900, #111)'}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <line x1="12" y1="5" x2="12" y2="19" />
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              New Database
            </button>
          )}
        </div>
      </div>

      {/* Loading state (initial load) */}
      {loading && !isRetrying && (
        <LoadingSpinner
          size="md"
          message="Loading databases..."
          centered
        />
      )}

      {/* Retrying state */}
      {isRetrying && (
        <LoadingSpinner
          size="md"
          message={`Retrying... (attempt ${retryCount + 1})`}
          centered
        />
      )}

      {/* Error state */}
      {error && !loading && !isRetrying && (
        <ErrorDisplay
          error={error}
          onRetry={retryFetch}
          retryCount={retryCount}
          maxRetries={3}
          nextRetryIn={nextRetryIn}
          isRetrying={isRetrying}
          title="Unable to load databases"
          style={{ marginBottom: '1rem' }}
        />
      )}

      {/* Exhausted state with additional troubleshooting help */}
      {isExhausted && (
        <div style={{
          padding: '1rem',
          background: 'var(--theme-elevation-50, #f5f5f5)',
          borderRadius: '6px',
          marginBottom: '1rem',
          fontSize: '0.875rem',
          color: 'var(--theme-elevation-600, #666)',
        }}>
          <strong>Troubleshooting tips:</strong>
          <ul style={{ margin: '0.5rem 0 0', paddingLeft: '1.5rem' }}>
            <li>Check your internet connection</li>
            <li>Try refreshing the page</li>
            <li>Clear your browser cache</li>
            <li>If the problem persists, contact support</li>
          </ul>
          <button
            onClick={() => resetFetch()}
            style={{
              marginTop: '1rem',
              padding: '0.5rem 1rem',
              background: 'var(--theme-elevation-800, #333)',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              fontSize: '0.8125rem',
              cursor: 'pointer',
            }}
          >
            Try Again
          </button>
        </div>
      )}

      {/* Database grid */}
      {!loading && !isRetrying && !error && (
        <>
          {filteredDatabases.length > 0 ? (
            <div
              ref={gridRef}
              className="database-grid"
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
                gap: '1rem',
                outline: 'none',
              }}
              onKeyDown={gridKeyDown}
              tabIndex={0}
              role="grid"
              aria-label="Database list"
            >
              {filteredDatabases.map((database, index) => (
                <div
                  key={database.id}
                  onMouseEnter={() => setFocusedIndex(index)}
                  style={{
                    position: 'relative',
                    ...(selectedIds.has(database.id) ? {
                      outline: '2px solid var(--theme-elevation-800, #333)',
                      borderRadius: '8px',
                    } : {}),
                  }}
                >
                  {/* Selection checkbox overlay */}
                  {showDelete && (
                    <div
                      style={{
                        position: 'absolute',
                        top: '0.75rem',
                        left: '0.75rem',
                        zIndex: 10,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={selectedIds.has(database.id)}
                        onChange={() => toggleSelection(database.id)}
                        onClick={(e) => e.stopPropagation()}
                        style={{ width: '18px', height: '18px', cursor: 'pointer' }}
                        title={selectedIds.has(database.id) ? 'Deselect' : 'Select'}
                      />
                    </div>
                  )}
                  <DatabaseCard
                    database={database}
                    basePath={basePath}
                    onSelect={onSelectDatabase}
                    onDelete={showDelete ? handleDeleteDatabase : undefined}
                    onClone={handleCloneDatabase}
                    deleting={deletingId === database.id}
                    focused={index === focusedIndex}
                  />
                </div>
              ))}
            </div>
          ) : (
            <div className="dashboard-empty" style={{
              textAlign: 'center',
              padding: '4rem',
              color: 'var(--theme-elevation-500, #888)',
            }}>
              {searchQuery ? (
                <>
                  <p>No databases match "{searchQuery}"</p>
                  <button
                    onClick={() => setSearchQuery('')}
                    style={{
                      marginTop: '0.5rem',
                      padding: '0.5rem 1rem',
                      background: 'var(--theme-elevation-100, #f5f5f5)',
                      border: 'none',
                      borderRadius: '6px',
                      cursor: 'pointer',
                    }}
                  >
                    Clear search
                  </button>
                </>
              ) : (
                <>
                  <svg
                    width="48"
                    height="48"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    style={{ margin: '0 auto 1rem', display: 'block', opacity: 0.5 }}
                  >
                    <ellipse cx="12" cy="5" rx="9" ry="3" />
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                  </svg>
                  <p style={{ fontSize: '1.125rem', marginBottom: '0.5rem' }}>No databases yet</p>
                  <p style={{ marginBottom: '1.5rem' }}>Create your first database to get started</p>
                  {showCreate && (
                    <button
                      onClick={() => setShowCreateModal(true)}
                      style={{
                        padding: '0.75rem 1.5rem',
                        background: 'var(--theme-elevation-900, #111)',
                        color: 'var(--theme-elevation-0, #fff)',
                        border: 'none',
                        borderRadius: '6px',
                        fontSize: '0.875rem',
                        fontWeight: 500,
                        cursor: 'pointer',
                      }}
                    >
                      Create Database
                    </button>
                  )}
                </>
              )}
            </div>
          )}
        </>
      )}

      {/* Stats footer */}
      {!loading && databases.length > 0 && (
        <div className="dashboard-footer" style={{
          marginTop: '2rem',
          paddingTop: '1rem',
          borderTop: '1px solid var(--theme-elevation-100, #eee)',
          display: 'flex',
          gap: '2rem',
          fontSize: '0.8125rem',
          color: 'var(--theme-elevation-500, #888)',
        }}>
          <span>{databases.length} database{databases.length !== 1 ? 's' : ''}</span>
          <span>
            {databases.reduce((sum, db) => sum + (db.entityCount ?? 0), 0).toLocaleString()} total entities
          </span>
        </div>
      )}

      {/* Create modal */}
      {showCreateModal && (
        <CreateDatabaseModal
          onClose={() => setShowCreateModal(false)}
          onCreate={handleDatabaseCreated}
          apiEndpoint={`${apiEndpoint}/create`}
        />
      )}

      {/* Clone modal */}
      {cloneSource && (
        <CloneDatabaseModal
          sourceDatabase={cloneSource}
          onClose={() => setCloneSource(null)}
          onClone={handleDatabaseCloned}
          apiEndpoint={apiEndpoint}
        />
      )}

      {/* Quick database switcher (Cmd+K / Ctrl+K) */}
      <QuickSwitcher
        databases={databases}
        basePath={basePath}
        onSelect={onSelectDatabase}
      />

      {/* Batch delete confirmation dialog */}
      <ConfirmationDialog
        isOpen={showBatchDeleteConfirm}
        onClose={() => setShowBatchDeleteConfirm(false)}
        onConfirm={handleBatchDelete}
        title="Delete Multiple Databases"
        message={`Are you sure you want to delete ${selectedIds.size} database${selectedIds.size === 1 ? '' : 's'}? This will unregister them from your account.`}
        details="Note: The underlying data in storage will not be deleted and can be re-registered later."
        confirmLabel={`Delete ${selectedIds.size} Database${selectedIds.size === 1 ? '' : 's'}`}
        cancelLabel="Cancel"
        variant="danger"
        loading={batchDeleting}
      />
    </div>
    </ErrorBoundary>
  )
}

export default DatabaseDashboard

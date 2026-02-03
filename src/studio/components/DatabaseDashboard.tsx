/**
 * DatabaseDashboard Component
 *
 * Main dashboard view showing all databases for a user.
 * Integrates with Payload CMS admin panel as a custom root view.
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

import { useState, useEffect, useCallback } from 'react'
import type { DatabaseInfo } from '../../worker/DatabaseIndexDO'
import { DatabaseCard } from './DatabaseCard'
import { CreateDatabaseModal } from './CreateDatabaseModal'

export interface DatabaseDashboardProps {
  /** Base path for admin routes (default: '/admin') */
  basePath?: string
  /** Initial databases (for SSR) */
  initialDatabases?: DatabaseInfo[]
  /** API endpoint for fetching databases */
  apiEndpoint?: string
  /** Callback when database is selected */
  onSelectDatabase?: (database: DatabaseInfo) => void
  /** Show create button */
  showCreate?: boolean
  /** Custom header component */
  header?: React.ReactNode
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
  header,
}: DatabaseDashboardProps) {
  const [databases, setDatabases] = useState<DatabaseInfo[]>(initialDatabases)
  const [loading, setLoading] = useState(!initialDatabases.length)
  const [error, setError] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [sortBy, setSortBy] = useState<'name' | 'lastAccessed' | 'created'>('lastAccessed')

  // Fetch databases
  const fetchDatabases = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await fetch(apiEndpoint)
      if (!response.ok) {
        throw new Error('Failed to fetch databases')
      }
      const data = await response.json() as { databases?: DatabaseInfo[] }
      setDatabases(data.databases || [])
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load databases')
    } finally {
      setLoading(false)
    }
  }, [apiEndpoint])

  useEffect(() => {
    if (!initialDatabases.length) {
      fetchDatabases()
    }
  }, [fetchDatabases, initialDatabases.length])

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
    setDatabases([database, ...databases])
    setShowCreateModal(false)
    if (onSelectDatabase) {
      onSelectDatabase(database)
    } else {
      // Navigate to the new database
      window.location.href = `${basePath}/${database.id}`
    }
  }

  return (
    <div className="database-dashboard" style={{ padding: '2rem', maxWidth: '1400px', margin: '0 auto' }}>
      {/* Header */}
      {header || (
        <div style={{ marginBottom: '2rem' }}>
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

      {/* Toolbar */}
      <div style={{
        display: 'flex',
        gap: '1rem',
        marginBottom: '1.5rem',
        flexWrap: 'wrap',
        alignItems: 'center',
      }}>
        {/* Search */}
        <div style={{ position: 'relative', flex: '1 1 300px' }}>
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

      {/* Loading state */}
      {loading && (
        <div style={{
          textAlign: 'center',
          padding: '4rem',
          color: 'var(--theme-elevation-500, #888)',
        }}>
          <div style={{
            width: '32px',
            height: '32px',
            border: '3px solid var(--theme-elevation-150, #eee)',
            borderTopColor: 'var(--theme-elevation-600, #666)',
            borderRadius: '50%',
            margin: '0 auto 1rem',
            animation: 'spin 1s linear infinite',
          }} />
          Loading databases...
          <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        </div>
      )}

      {/* Error state */}
      {error && (
        <div style={{
          padding: '1rem',
          background: 'var(--theme-error-100, #fef2f2)',
          color: 'var(--theme-error-500, #dc2626)',
          borderRadius: '6px',
          marginBottom: '1rem',
        }}>
          {error}
          <button
            onClick={fetchDatabases}
            style={{
              marginLeft: '1rem',
              padding: '0.25rem 0.5rem',
              background: 'transparent',
              border: '1px solid currentColor',
              borderRadius: '4px',
              color: 'inherit',
              cursor: 'pointer',
            }}
          >
            Retry
          </button>
        </div>
      )}

      {/* Database grid */}
      {!loading && !error && (
        <>
          {filteredDatabases.length > 0 ? (
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
              gap: '1rem',
            }}>
              {filteredDatabases.map((database) => (
                <DatabaseCard
                  key={database.id}
                  database={database}
                  basePath={basePath}
                  onSelect={onSelectDatabase}
                />
              ))}
            </div>
          ) : (
            <div style={{
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
        <div style={{
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
    </div>
  )
}

export default DatabaseDashboard

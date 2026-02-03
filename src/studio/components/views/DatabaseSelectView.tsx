/**
 * DatabaseSelectView
 *
 * Full-page database selection view for multi-database mode.
 * Displayed at /admin when no database is selected.
 *
 * Unlike DatabaseDashboardView which replaces the Payload dashboard,
 * this is a standalone page for database context selection.
 */

'use client'

import type { DatabaseInfo } from '../../../worker/DatabaseIndexDO'
import { DatabaseCard } from '../DatabaseCard'

export interface DatabaseSelectViewProps {
  /** User's databases */
  databases: DatabaseInfo[]
  /** User info */
  user?: {
    id: string
    email: string
    name?: string | undefined
    avatarUrl?: string | undefined
  } | undefined
  /** Base path for admin routes */
  basePath?: string | undefined
  /** Brand/logo URL */
  logoUrl?: string | undefined
  /** App name */
  appName?: string | undefined
}

/**
 * Full-page database selection view
 */
export function DatabaseSelectView({
  databases,
  user,
  basePath = '/admin',
  logoUrl,
  appName = 'ParqueDB Studio',
}: DatabaseSelectViewProps) {
  return (
    <div style={{
      minHeight: '100vh',
      background: 'var(--theme-bg, #f5f5f5)',
      display: 'flex',
      flexDirection: 'column',
    }}>
      {/* Header */}
      <header style={{
        padding: '1rem 2rem',
        background: 'var(--theme-elevation-0, #fff)',
        borderBottom: '1px solid var(--theme-elevation-100, #e5e5e5)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          {logoUrl ? (
            <img src={logoUrl} alt={appName} style={{ height: '32px' }} />
          ) : (
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
              <ellipse cx="12" cy="5" rx="9" ry="3" />
              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
            </svg>
          )}
          <span style={{
            fontWeight: 600,
            fontSize: '1.125rem',
            color: 'var(--theme-text, #111)',
          }}>
            {appName}
          </span>
        </div>

        {/* User menu */}
        {user && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <span style={{
              fontSize: '0.875rem',
              color: 'var(--theme-elevation-600, #666)',
            }}>
              {user.name || user.email}
            </span>
            {user.avatarUrl ? (
              <img
                src={user.avatarUrl}
                alt={user.name || user.email}
                style={{
                  width: '32px',
                  height: '32px',
                  borderRadius: '50%',
                  objectFit: 'cover',
                }}
              />
            ) : (
              <div style={{
                width: '32px',
                height: '32px',
                borderRadius: '50%',
                background: 'var(--theme-elevation-200, #ddd)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'var(--theme-elevation-600, #666)',
                fontWeight: 500,
              }}>
                {(user.name || user.email).charAt(0).toUpperCase() || '?'}
              </div>
            )}
          </div>
        )}
      </header>

      {/* Main content */}
      <main style={{
        flex: 1,
        padding: '3rem 2rem',
        maxWidth: '1200px',
        margin: '0 auto',
        width: '100%',
      }}>
        <div style={{ textAlign: 'center', marginBottom: '3rem' }}>
          <h1 style={{
            margin: '0 0 0.5rem',
            fontSize: '2rem',
            fontWeight: 600,
            color: 'var(--theme-text, #111)',
          }}>
            Select a Database
          </h1>
          <p style={{
            margin: 0,
            color: 'var(--theme-elevation-600, #666)',
            fontSize: '1.125rem',
          }}>
            Choose a database to manage its collections and data
          </p>
        </div>

        {databases.length > 0 ? (
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))',
            gap: '1.25rem',
          }}>
            {databases.map((database) => (
              <DatabaseCard
                key={database.id}
                database={database}
                basePath={basePath}
              />
            ))}
          </div>
        ) : (
          <div style={{
            textAlign: 'center',
            padding: '4rem',
            background: 'var(--theme-elevation-0, #fff)',
            borderRadius: '12px',
            border: '1px solid var(--theme-elevation-100, #e5e5e5)',
          }}>
            <svg
              width="64"
              height="64"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1"
              style={{
                margin: '0 auto 1.5rem',
                display: 'block',
                color: 'var(--theme-elevation-300, #ccc)',
              }}
            >
              <ellipse cx="12" cy="5" rx="9" ry="3" />
              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
            </svg>
            <h2 style={{
              margin: '0 0 0.75rem',
              fontSize: '1.25rem',
              fontWeight: 600,
              color: 'var(--theme-text, #333)',
            }}>
              No databases yet
            </h2>
            <p style={{
              margin: '0 0 1.5rem',
              color: 'var(--theme-elevation-600, #666)',
            }}>
              Create your first database to start managing your data
            </p>
            <a
              href={`${basePath}/new`}
              style={{
                display: 'inline-block',
                padding: '0.75rem 1.5rem',
                background: 'var(--theme-elevation-900, #111)',
                color: 'var(--theme-elevation-0, #fff)',
                borderRadius: '6px',
                textDecoration: 'none',
                fontWeight: 500,
              }}
            >
              Create Database
            </a>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer style={{
        padding: '1.5rem 2rem',
        textAlign: 'center',
        color: 'var(--theme-elevation-500, #888)',
        fontSize: '0.875rem',
      }}>
        Powered by <a href="https://parque.db" style={{ color: 'inherit' }}>ParqueDB</a>
      </footer>
    </div>
  )
}

export default DatabaseSelectView

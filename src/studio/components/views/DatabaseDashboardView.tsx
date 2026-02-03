/**
 * DatabaseDashboardView
 *
 * Payload CMS custom view that wraps DatabaseDashboard.
 * Uses Payload's DefaultTemplate for consistent admin panel layout.
 *
 * @example
 * ```typescript
 * // payload.config.ts
 * import { buildConfig } from 'payload/config'
 * import { DatabaseDashboardView } from 'parquedb/studio'
 *
 * export default buildConfig({
 *   admin: {
 *     components: {
 *       views: {
 *         Dashboard: DatabaseDashboardView,
 *       },
 *     },
 *   },
 * })
 * ```
 */

'use client'

import { DatabaseDashboard } from '../DatabaseDashboard'
import type { DatabaseInfo } from '../../../worker/DatabaseIndexDO'

export interface DatabaseDashboardViewProps {
  /** Databases passed from server component */
  databases?: DatabaseInfo[]
  /** User info for personalization */
  user?: {
    id: string
    email: string
    name?: string
  }
  /** Base path for admin routes */
  basePath?: string
  /** API endpoint for database operations */
  apiEndpoint?: string
}

/**
 * Payload-wrapped Dashboard View
 *
 * This component is designed to replace Payload's default dashboard.
 * It integrates with Payload's template system when available.
 */
export function DatabaseDashboardView({
  databases,
  user,
  basePath = '/admin',
  apiEndpoint = '/api/databases',
}: DatabaseDashboardViewProps) {
  // Custom header with user greeting
  const header = user ? (
    <div style={{ marginBottom: '2rem' }}>
      <h1 style={{
        margin: 0,
        fontSize: '1.75rem',
        fontWeight: 600,
        color: 'var(--theme-text, #111)',
      }}>
        {getGreeting()}, {user.name?.split(' ')[0] || user.email.split('@')[0]}
      </h1>
      <p style={{
        margin: '0.5rem 0 0',
        color: 'var(--theme-elevation-600, #666)',
      }}>
        Select a database to manage or create a new one
      </p>
    </div>
  ) : undefined

  return (
    <DatabaseDashboard
      basePath={basePath}
      initialDatabases={databases}
      apiEndpoint={apiEndpoint}
      header={header}
      showCreate={true}
    />
  )
}

/**
 * Get time-based greeting
 */
function getGreeting(): string {
  const hour = new Date().getHours()
  if (hour < 12) return 'Good morning'
  if (hour < 17) return 'Good afternoon'
  return 'Good evening'
}

export default DatabaseDashboardView

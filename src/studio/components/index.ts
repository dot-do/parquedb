/**
 * Studio Components
 *
 * React components for the ParqueDB Studio dashboard.
 * These components integrate with Payload CMS admin panel.
 *
 * @example Basic Usage
 * ```typescript
 * import { DatabaseDashboard, DatabaseCard } from 'parquedb/studio'
 * ```
 *
 * @example Payload CMS Integration
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

// Core components
export { DatabaseCard, type DatabaseCardProps } from './DatabaseCard'
export { DatabaseDashboard, type DatabaseDashboardProps } from './DatabaseDashboard'
export { CreateDatabaseModal, type CreateDatabaseModalProps } from './CreateDatabaseModal'

// Payload-specific views
export { DatabaseDashboardView, type DatabaseDashboardViewProps } from './views/DatabaseDashboardView'
export { DatabaseSelectView, type DatabaseSelectViewProps } from './views/DatabaseSelectView'

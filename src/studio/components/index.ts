/**
 * Studio Components
 *
 * React components for the ParqueDB Studio dashboard.
 * These components integrate with Payload CMS admin panel.
 *
 * @example Basic Usage
 * ```typescript
 * import { DatabaseDashboard, DatabaseCard, SettingsPage } from 'parquedb/studio'
 * ```
 *
 * @example Payload CMS Integration
 * ```typescript
 * // payload.config.ts
 * import { buildConfig } from 'payload/config'
 * import { DatabaseDashboardView, SettingsView } from 'parquedb/studio'
 *
 * export default buildConfig({
 *   admin: {
 *     components: {
 *       views: {
 *         Dashboard: DatabaseDashboardView,
 *         Settings: {
 *           Component: SettingsView,
 *           path: '/settings',
 *         },
 *       },
 *     },
 *   },
 * })
 * ```
 *
 * @example Error Handling
 * ```typescript
 * import { ErrorBoundary, ErrorDisplay, useRetry } from 'parquedb/studio'
 *
 * // Wrap components with ErrorBoundary
 * <ErrorBoundary onError={handleError}>
 *   <MyComponent />
 * </ErrorBoundary>
 *
 * // Use retry hook for async operations
 * const { data, loading, error, retry } = useRetry(fetchData, { maxRetries: 3 })
 * ```
 */

// Core components
export { DatabaseCard, type DatabaseCardProps } from './DatabaseCard'
export { DatabaseDashboard, type DatabaseDashboardProps } from './DatabaseDashboard'
export { CreateDatabaseModal, type CreateDatabaseModalProps } from './CreateDatabaseModal'
export { CloneDatabaseModal, type CloneDatabaseModalProps } from './CloneDatabaseModal'
export { QuickSwitcher, type QuickSwitcherProps } from './QuickSwitcher'
export {
  ConfirmationDialog,
  type ConfirmationDialogProps,
  type ConfirmationVariant,
} from './ConfirmationDialog'
export { SettingsPage, type SettingsPageProps, type StudioSettings } from './SettingsPage'

// Responsive styles
export { ResponsiveStyles } from './ResponsiveStyles'

// Error handling components
export { ErrorBoundary, type ErrorBoundaryProps, type ErrorFallbackProps } from './ErrorBoundary'
export { ErrorDisplay, type ErrorDisplayProps, type ErrorSeverity } from './ErrorDisplay'
export { LoadingSpinner, type LoadingSpinnerProps, type SpinnerSize } from './LoadingSpinner'

// Hooks
export { useRetry, type UseRetryOptions, type UseRetryResult } from './hooks/useRetry'
export {
  useKeyboardNavigation,
  type UseKeyboardNavigationOptions,
  type UseKeyboardNavigationResult,
} from './hooks/useKeyboardNavigation'

// Payload-specific views
export { DatabaseDashboardView, type DatabaseDashboardViewProps } from './views/DatabaseDashboardView'
export { DatabaseSelectView, type DatabaseSelectViewProps } from './views/DatabaseSelectView'
export { SettingsView, type SettingsViewProps } from './views/SettingsView'

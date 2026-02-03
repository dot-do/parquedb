/**
 * SettingsView
 *
 * Payload CMS custom view that wraps SettingsPage.
 * Uses Payload's DefaultTemplate for consistent admin panel layout.
 *
 * @example
 * ```typescript
 * // payload.config.ts
 * import { buildConfig } from 'payload/config'
 * import { SettingsView } from 'parquedb/studio'
 *
 * export default buildConfig({
 *   admin: {
 *     components: {
 *       views: {
 *         Settings: {
 *           Component: SettingsView,
 *           path: '/settings',
 *         },
 *       },
 *     },
 *   },
 * })
 * ```
 */

'use client'

import { SettingsPage } from '../SettingsPage'
import type { StudioSettings } from '../SettingsPage'

export interface SettingsViewProps {
  /** Initial settings (from server) */
  settings?: Partial<StudioSettings> | undefined
  /** User info for personalization */
  user?: {
    id: string
    email: string
    name?: string | undefined
  } | undefined
  /** Base path for admin routes */
  basePath?: string | undefined
  /** API endpoint for settings operations */
  apiEndpoint?: string | undefined
  /** Callback when settings are saved */
  onSave?: ((settings: StudioSettings) => void) | undefined
}

/**
 * Payload-wrapped Settings View
 *
 * This component is designed to be used as a custom view in Payload CMS.
 * It provides a full settings management interface.
 */
export function SettingsView({
  settings,
  user,
  basePath = '/admin',
  apiEndpoint = '/api/settings',
  onSave,
}: SettingsViewProps) {
  return (
    <SettingsPage
      initialSettings={settings}
      apiEndpoint={apiEndpoint}
      onSave={onSave}
      basePath={basePath}
    />
  )
}

export default SettingsView

/**
 * SettingsPage Component
 *
 * Main settings page for ParqueDB Studio.
 * Allows configuring database connection, display preferences,
 * authentication, and export/import options.
 *
 * Settings are persisted to localStorage by default,
 * with optional backend persistence.
 */

'use client'

import { useState, useEffect, useCallback } from 'react'

// =============================================================================
// Types
// =============================================================================

export interface StudioSettings {
  /** Display preferences */
  display: {
    /** Number of items per page in list views */
    pageSize: number
    /** Date format pattern (e.g., 'YYYY-MM-DD', 'MM/DD/YYYY') */
    dateFormat: string
    /** Time format ('12h' or '24h') */
    timeFormat: '12h' | '24h'
    /** Theme preference */
    theme: 'light' | 'dark' | 'auto'
    /** Show entity IDs in list views */
    showIds: boolean
    /** Compact mode for dense data display */
    compactMode: boolean
  }
  /** Database connection settings */
  connection: {
    /** API endpoint URL */
    apiEndpoint: string
    /** Connection timeout in milliseconds */
    timeout: number
    /** Retry failed requests */
    retryOnFailure: boolean
    /** Number of retry attempts */
    retryAttempts: number
  }
  /** Authentication settings */
  auth: {
    /** Remember login session */
    rememberSession: boolean
    /** Session timeout in minutes (0 = never) */
    sessionTimeout: number
    /** Auto-logout on idle */
    autoLogoutOnIdle: boolean
    /** Idle timeout in minutes */
    idleTimeout: number
  }
  /** Export/Import preferences */
  exportImport: {
    /** Default export format */
    defaultExportFormat: 'json' | 'csv' | 'parquet'
    /** Include metadata in exports */
    includeMetadata: boolean
    /** Include relationships in exports */
    includeRelationships: boolean
    /** Pretty print JSON exports */
    prettyPrintJson: boolean
    /** CSV delimiter */
    csvDelimiter: ',' | ';' | '\t'
  }
}

export interface SettingsPageProps {
  /** Initial settings (from server or localStorage) */
  initialSettings?: Partial<StudioSettings> | undefined
  /** API endpoint for saving settings to backend */
  apiEndpoint?: string | undefined
  /** Callback when settings are saved */
  onSave?: ((settings: StudioSettings) => void) | undefined
  /** Storage key for localStorage */
  storageKey?: string | undefined
  /** Base path for navigation */
  basePath?: string | undefined
}

// =============================================================================
// Default Settings
// =============================================================================

const DEFAULT_SETTINGS: StudioSettings = {
  display: {
    pageSize: 25,
    dateFormat: 'YYYY-MM-DD',
    timeFormat: '24h',
    theme: 'auto',
    showIds: true,
    compactMode: false,
  },
  connection: {
    apiEndpoint: '/api',
    timeout: 30000,
    retryOnFailure: true,
    retryAttempts: 3,
  },
  auth: {
    rememberSession: true,
    sessionTimeout: 0,
    autoLogoutOnIdle: false,
    idleTimeout: 30,
  },
  exportImport: {
    defaultExportFormat: 'json',
    includeMetadata: true,
    includeRelationships: true,
    prettyPrintJson: true,
    csvDelimiter: ',',
  },
}

// =============================================================================
// Utility Functions
// =============================================================================

function deepMerge<T extends Record<string, unknown>>(target: T, source: Partial<T>): T {
  const result = { ...target }
  for (const key in source) {
    if (source[key] !== undefined) {
      if (
        typeof source[key] === 'object' &&
        source[key] !== null &&
        !Array.isArray(source[key]) &&
        typeof target[key] === 'object' &&
        target[key] !== null
      ) {
        result[key] = deepMerge(
          target[key] as Record<string, unknown>,
          source[key] as Record<string, unknown>
        ) as T[Extract<keyof T, string>]
      } else {
        result[key] = source[key] as T[Extract<keyof T, string>]
      }
    }
  }
  return result
}

function loadSettings(storageKey: string, initial?: Partial<StudioSettings>): StudioSettings {
  // Start with defaults
  let settings = { ...DEFAULT_SETTINGS }

  // Try to load from localStorage
  if (typeof window !== 'undefined') {
    try {
      const stored = localStorage.getItem(storageKey)
      if (stored) {
        const parsed = JSON.parse(stored) as Partial<StudioSettings>
        settings = deepMerge(settings, parsed)
      }
    } catch {
      // Ignore localStorage errors
    }
  }

  // Apply initial settings from props
  if (initial) {
    settings = deepMerge(settings, initial)
  }

  return settings
}

function saveSettings(storageKey: string, settings: StudioSettings): void {
  if (typeof window !== 'undefined') {
    try {
      localStorage.setItem(storageKey, JSON.stringify(settings))
    } catch {
      // Ignore localStorage errors
    }
  }
}

// =============================================================================
// Styles
// =============================================================================

const styles = {
  container: {
    padding: '2rem',
    maxWidth: '900px',
    margin: '0 auto',
  },
  header: {
    marginBottom: '2rem',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  title: {
    margin: 0,
    fontSize: '1.75rem',
    fontWeight: 600,
    color: 'var(--theme-text, #111)',
  },
  subtitle: {
    margin: '0.5rem 0 0',
    color: 'var(--theme-elevation-600, #666)',
  },
  section: {
    background: 'var(--theme-elevation-50, #fff)',
    borderRadius: '8px',
    border: '1px solid var(--theme-elevation-100, #e5e5e5)',
    marginBottom: '1.5rem',
    overflow: 'hidden',
  },
  sectionHeader: {
    padding: '1rem 1.5rem',
    borderBottom: '1px solid var(--theme-elevation-100, #e5e5e5)',
    background: 'var(--theme-elevation-25, #fafafa)',
  },
  sectionTitle: {
    margin: 0,
    fontSize: '1rem',
    fontWeight: 600,
    color: 'var(--theme-text, #333)',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
  },
  sectionContent: {
    padding: '1.5rem',
  },
  field: {
    marginBottom: '1.25rem',
  },
  fieldLast: {
    marginBottom: 0,
  },
  label: {
    display: 'block',
    marginBottom: '0.5rem',
    fontSize: '0.875rem',
    fontWeight: 500,
    color: 'var(--theme-text, #333)',
  },
  labelDescription: {
    fontWeight: 400,
    color: 'var(--theme-elevation-500, #888)',
    marginLeft: '0.25rem',
  },
  input: {
    width: '100%',
    padding: '0.625rem 1rem',
    border: '1px solid var(--theme-elevation-150, #ddd)',
    borderRadius: '6px',
    fontSize: '0.875rem',
    background: 'var(--theme-input-bg, #fff)',
    color: 'var(--theme-text, #333)',
  },
  select: {
    width: '100%',
    padding: '0.625rem 1rem',
    border: '1px solid var(--theme-elevation-150, #ddd)',
    borderRadius: '6px',
    fontSize: '0.875rem',
    background: 'var(--theme-input-bg, #fff)',
    color: 'var(--theme-text, #333)',
    cursor: 'pointer',
  },
  checkbox: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.75rem',
    cursor: 'pointer',
  },
  checkboxInput: {
    width: '18px',
    height: '18px',
    cursor: 'pointer',
  },
  row: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
    gap: '1rem',
  },
  actions: {
    display: 'flex',
    gap: '0.75rem',
    justifyContent: 'flex-end',
    marginTop: '1.5rem',
    paddingTop: '1.5rem',
    borderTop: '1px solid var(--theme-elevation-100, #eee)',
  },
  buttonPrimary: {
    padding: '0.75rem 1.5rem',
    background: 'var(--theme-elevation-900, #111)',
    color: 'var(--theme-elevation-0, #fff)',
    border: 'none',
    borderRadius: '6px',
    fontSize: '0.875rem',
    fontWeight: 500,
    cursor: 'pointer',
  },
  buttonSecondary: {
    padding: '0.75rem 1.5rem',
    background: 'transparent',
    border: '1px solid var(--theme-elevation-200, #ddd)',
    borderRadius: '6px',
    fontSize: '0.875rem',
    cursor: 'pointer',
    color: 'var(--theme-text, #333)',
  },
  buttonDanger: {
    padding: '0.75rem 1.5rem',
    background: 'var(--theme-error-500, #dc2626)',
    color: '#fff',
    border: 'none',
    borderRadius: '6px',
    fontSize: '0.875rem',
    fontWeight: 500,
    cursor: 'pointer',
  },
  success: {
    padding: '0.75rem 1rem',
    background: 'var(--theme-success-100, #dcfce7)',
    color: 'var(--theme-success-700, #15803d)',
    borderRadius: '6px',
    marginBottom: '1rem',
    fontSize: '0.875rem',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
  },
  error: {
    padding: '0.75rem 1rem',
    background: 'var(--theme-error-100, #fef2f2)',
    color: 'var(--theme-error-500, #dc2626)',
    borderRadius: '6px',
    marginBottom: '1rem',
    fontSize: '0.875rem',
  },
} as const

// =============================================================================
// Component
// =============================================================================

export function SettingsPage({
  initialSettings,
  apiEndpoint,
  onSave,
  storageKey = 'parquedb-studio-settings',
  basePath = '/admin',
}: SettingsPageProps) {
  const [settings, setSettings] = useState<StudioSettings>(() =>
    loadSettings(storageKey, initialSettings)
  )
  const [saving, setSaving] = useState(false)
  const [success, setSuccess] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [hasChanges, setHasChanges] = useState(false)

  // Track changes
  useEffect(() => {
    const original = loadSettings(storageKey, initialSettings)
    setHasChanges(JSON.stringify(settings) !== JSON.stringify(original))
  }, [settings, storageKey, initialSettings])

  // Update a nested setting
  const updateSetting = useCallback(<K extends keyof StudioSettings>(
    section: K,
    key: keyof StudioSettings[K],
    value: StudioSettings[K][keyof StudioSettings[K]]
  ) => {
    setSettings((prev) => ({
      ...prev,
      [section]: {
        ...prev[section],
        [key]: value,
      },
    }))
    setSuccess(false)
    setError(null)
  }, [])

  // Save settings
  const handleSave = async () => {
    setSaving(true)
    setError(null)
    setSuccess(false)

    try {
      // Save to localStorage
      saveSettings(storageKey, settings)

      // Optionally save to backend
      if (apiEndpoint) {
        const response = await fetch(apiEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(settings),
        })

        if (!response.ok) {
          const data = (await response.json().catch(() => ({}))) as { error?: string | undefined }
          throw new Error(data.error || 'Failed to save settings')
        }
      }

      // Call callback
      if (onSave) {
        onSave(settings)
      }

      setSuccess(true)
      setHasChanges(false)

      // Apply theme immediately
      if (typeof document !== 'undefined') {
        const theme = settings.display.theme
        if (theme === 'dark') {
          document.documentElement.setAttribute('data-theme', 'dark')
        } else if (theme === 'light') {
          document.documentElement.setAttribute('data-theme', 'light')
        } else {
          document.documentElement.removeAttribute('data-theme')
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  // Reset to defaults
  const handleReset = () => {
    if (confirm('Are you sure you want to reset all settings to their default values?')) {
      setSettings(DEFAULT_SETTINGS)
      setSuccess(false)
      setError(null)
    }
  }

  // Export settings
  const handleExport = () => {
    const blob = new Blob([JSON.stringify(settings, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'parquedb-studio-settings.json'
    a.click()
    URL.revokeObjectURL(url)
  }

  // Import settings
  const handleImport = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const imported = JSON.parse(e.target?.result as string) as Partial<StudioSettings>
        setSettings(deepMerge(DEFAULT_SETTINGS, imported))
        setSuccess(false)
        setError(null)
      } catch {
        setError('Invalid settings file')
      }
    }
    reader.readAsText(file)
    event.target.value = ''
  }

  return (
    <div style={styles.container}>
      {/* Header */}
      <div style={styles.header}>
        <div>
          <h1 style={styles.title}>Settings</h1>
          <p style={styles.subtitle}>
            Configure your ParqueDB Studio preferences
          </p>
        </div>
        <a
          href={basePath}
          style={{
            ...styles.buttonSecondary,
            textDecoration: 'none',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.5rem',
          }}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <line x1="19" y1="12" x2="5" y2="12" />
            <polyline points="12,19 5,12 12,5" />
          </svg>
          Back to Dashboard
        </a>
      </div>

      {/* Success message */}
      {success && (
        <div style={styles.success}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <polyline points="20,6 9,17 4,12" />
          </svg>
          Settings saved successfully
        </div>
      )}

      {/* Error message */}
      {error && (
        <div style={styles.error}>
          {error}
        </div>
      )}

      {/* Display Preferences */}
      <section style={styles.section}>
        <div style={styles.sectionHeader}>
          <h2 style={styles.sectionTitle}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <rect x="2" y="3" width="20" height="14" rx="2" ry="2" />
              <line x1="8" y1="21" x2="16" y2="21" />
              <line x1="12" y1="17" x2="12" y2="21" />
            </svg>
            Display Preferences
          </h2>
        </div>
        <div style={styles.sectionContent}>
          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.label}>
                Theme
              </label>
              <select
                style={styles.select}
                value={settings.display.theme}
                onChange={(e) => updateSetting('display', 'theme', e.target.value as 'light' | 'dark' | 'auto')}
              >
                <option value="auto">System Default</option>
                <option value="light">Light</option>
                <option value="dark">Dark</option>
              </select>
            </div>
            <div style={styles.field}>
              <label style={styles.label}>
                Items Per Page
              </label>
              <select
                style={styles.select}
                value={settings.display.pageSize}
                onChange={(e) => updateSetting('display', 'pageSize', parseInt(e.target.value, 10))}
              >
                <option value="10">10</option>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
              </select>
            </div>
          </div>

          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.label}>
                Date Format
              </label>
              <select
                style={styles.select}
                value={settings.display.dateFormat}
                onChange={(e) => updateSetting('display', 'dateFormat', e.target.value)}
              >
                <option value="YYYY-MM-DD">2024-01-15 (ISO)</option>
                <option value="MM/DD/YYYY">01/15/2024 (US)</option>
                <option value="DD/MM/YYYY">15/01/2024 (EU)</option>
                <option value="DD.MM.YYYY">15.01.2024 (DE)</option>
                <option value="MMM D, YYYY">Jan 15, 2024</option>
              </select>
            </div>
            <div style={styles.field}>
              <label style={styles.label}>
                Time Format
              </label>
              <select
                style={styles.select}
                value={settings.display.timeFormat}
                onChange={(e) => updateSetting('display', 'timeFormat', e.target.value as '12h' | '24h')}
              >
                <option value="24h">24-hour (14:30)</option>
                <option value="12h">12-hour (2:30 PM)</option>
              </select>
            </div>
          </div>

          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.checkbox}>
                <input
                  type="checkbox"
                  style={styles.checkboxInput}
                  checked={settings.display.showIds}
                  onChange={(e) => updateSetting('display', 'showIds', e.target.checked)}
                />
                <span>
                  Show Entity IDs
                  <span style={styles.labelDescription}> - Display IDs in list views</span>
                </span>
              </label>
            </div>
            <div style={styles.field}>
              <label style={styles.checkbox}>
                <input
                  type="checkbox"
                  style={styles.checkboxInput}
                  checked={settings.display.compactMode}
                  onChange={(e) => updateSetting('display', 'compactMode', e.target.checked)}
                />
                <span>
                  Compact Mode
                  <span style={styles.labelDescription}> - Denser data display</span>
                </span>
              </label>
            </div>
          </div>
        </div>
      </section>

      {/* Connection Settings */}
      <section style={styles.section}>
        <div style={styles.sectionHeader}>
          <h2 style={styles.sectionTitle}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
              <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
            </svg>
            Connection Settings
          </h2>
        </div>
        <div style={styles.sectionContent}>
          <div style={styles.field}>
            <label style={styles.label}>
              API Endpoint
            </label>
            <input
              type="text"
              style={styles.input}
              value={settings.connection.apiEndpoint}
              onChange={(e) => updateSetting('connection', 'apiEndpoint', e.target.value)}
              placeholder="/api"
            />
          </div>

          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.label}>
                Request Timeout
                <span style={styles.labelDescription}> (milliseconds)</span>
              </label>
              <input
                type="number"
                style={styles.input}
                value={settings.connection.timeout}
                onChange={(e) => updateSetting('connection', 'timeout', parseInt(e.target.value, 10) || 30000)}
                min={1000}
                max={300000}
                step={1000}
              />
            </div>
            <div style={styles.field}>
              <label style={styles.label}>
                Retry Attempts
              </label>
              <input
                type="number"
                style={styles.input}
                value={settings.connection.retryAttempts}
                onChange={(e) => updateSetting('connection', 'retryAttempts', parseInt(e.target.value, 10) || 3)}
                min={0}
                max={10}
                disabled={!settings.connection.retryOnFailure}
              />
            </div>
          </div>

          <div style={styles.field}>
            <label style={styles.checkbox}>
              <input
                type="checkbox"
                style={styles.checkboxInput}
                checked={settings.connection.retryOnFailure}
                onChange={(e) => updateSetting('connection', 'retryOnFailure', e.target.checked)}
              />
              <span>
                Retry on Failure
                <span style={styles.labelDescription}> - Automatically retry failed requests</span>
              </span>
            </label>
          </div>
        </div>
      </section>

      {/* Authentication Settings */}
      <section style={styles.section}>
        <div style={styles.sectionHeader}>
          <h2 style={styles.sectionTitle}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
              <path d="M7 11V7a5 5 0 0 1 10 0v4" />
            </svg>
            Authentication
          </h2>
        </div>
        <div style={styles.sectionContent}>
          <div style={styles.field}>
            <label style={styles.checkbox}>
              <input
                type="checkbox"
                style={styles.checkboxInput}
                checked={settings.auth.rememberSession}
                onChange={(e) => updateSetting('auth', 'rememberSession', e.target.checked)}
              />
              <span>
                Remember Session
                <span style={styles.labelDescription}> - Stay logged in between visits</span>
              </span>
            </label>
          </div>

          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.label}>
                Session Timeout
                <span style={styles.labelDescription}> (minutes, 0 = never)</span>
              </label>
              <input
                type="number"
                style={styles.input}
                value={settings.auth.sessionTimeout}
                onChange={(e) => updateSetting('auth', 'sessionTimeout', parseInt(e.target.value, 10) || 0)}
                min={0}
                max={1440}
              />
            </div>
            <div style={styles.field}>
              <label style={styles.label}>
                Idle Timeout
                <span style={styles.labelDescription}> (minutes)</span>
              </label>
              <input
                type="number"
                style={styles.input}
                value={settings.auth.idleTimeout}
                onChange={(e) => updateSetting('auth', 'idleTimeout', parseInt(e.target.value, 10) || 30)}
                min={1}
                max={120}
                disabled={!settings.auth.autoLogoutOnIdle}
              />
            </div>
          </div>

          <div style={styles.field}>
            <label style={styles.checkbox}>
              <input
                type="checkbox"
                style={styles.checkboxInput}
                checked={settings.auth.autoLogoutOnIdle}
                onChange={(e) => updateSetting('auth', 'autoLogoutOnIdle', e.target.checked)}
              />
              <span>
                Auto-logout on Idle
                <span style={styles.labelDescription}> - Log out after period of inactivity</span>
              </span>
            </label>
          </div>
        </div>
      </section>

      {/* Export/Import Preferences */}
      <section style={styles.section}>
        <div style={styles.sectionHeader}>
          <h2 style={styles.sectionTitle}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="17,8 12,3 7,8" />
              <line x1="12" y1="3" x2="12" y2="15" />
            </svg>
            Export / Import
          </h2>
        </div>
        <div style={styles.sectionContent}>
          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.label}>
                Default Export Format
              </label>
              <select
                style={styles.select}
                value={settings.exportImport.defaultExportFormat}
                onChange={(e) => updateSetting('exportImport', 'defaultExportFormat', e.target.value as 'json' | 'csv' | 'parquet')}
              >
                <option value="json">JSON</option>
                <option value="csv">CSV</option>
                <option value="parquet">Parquet</option>
              </select>
            </div>
            <div style={styles.field}>
              <label style={styles.label}>
                CSV Delimiter
              </label>
              <select
                style={styles.select}
                value={settings.exportImport.csvDelimiter}
                onChange={(e) => updateSetting('exportImport', 'csvDelimiter', e.target.value as ',' | ';' | '\t')}
              >
                <option value=",">Comma (,)</option>
                <option value=";">Semicolon (;)</option>
                <option value={'\t'}>Tab</option>
              </select>
            </div>
          </div>

          <div style={styles.row}>
            <div style={styles.field}>
              <label style={styles.checkbox}>
                <input
                  type="checkbox"
                  style={styles.checkboxInput}
                  checked={settings.exportImport.includeMetadata}
                  onChange={(e) => updateSetting('exportImport', 'includeMetadata', e.target.checked)}
                />
                <span>
                  Include Metadata
                  <span style={styles.labelDescription}> - Export audit fields</span>
                </span>
              </label>
            </div>
            <div style={styles.field}>
              <label style={styles.checkbox}>
                <input
                  type="checkbox"
                  style={styles.checkboxInput}
                  checked={settings.exportImport.includeRelationships}
                  onChange={(e) => updateSetting('exportImport', 'includeRelationships', e.target.checked)}
                />
                <span>
                  Include Relationships
                  <span style={styles.labelDescription}> - Export linked entities</span>
                </span>
              </label>
            </div>
          </div>

          <div style={styles.field}>
            <label style={styles.checkbox}>
              <input
                type="checkbox"
                style={styles.checkboxInput}
                checked={settings.exportImport.prettyPrintJson}
                onChange={(e) => updateSetting('exportImport', 'prettyPrintJson', e.target.checked)}
              />
              <span>
                Pretty Print JSON
                <span style={styles.labelDescription}> - Format JSON with indentation</span>
              </span>
            </label>
          </div>
        </div>
      </section>

      {/* Settings Import/Export */}
      <section style={styles.section}>
        <div style={styles.sectionHeader}>
          <h2 style={styles.sectionTitle}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <circle cx="12" cy="12" r="3" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
            Settings Backup
          </h2>
        </div>
        <div style={styles.sectionContent}>
          <p style={{ margin: '0 0 1rem', color: 'var(--theme-elevation-600, #666)', fontSize: '0.875rem' }}>
            Export your settings to a file or import from a previously saved backup.
          </p>
          <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
            <button
              type="button"
              onClick={handleExport}
              style={styles.buttonSecondary}
            >
              Export Settings
            </button>
            <label style={{ ...styles.buttonSecondary, cursor: 'pointer', display: 'inline-block' }}>
              Import Settings
              <input
                type="file"
                accept=".json"
                onChange={handleImport}
                style={{ display: 'none' }}
              />
            </label>
            <button
              type="button"
              onClick={handleReset}
              style={{ ...styles.buttonSecondary, color: 'var(--theme-error-500, #dc2626)' }}
            >
              Reset to Defaults
            </button>
          </div>
        </div>
      </section>

      {/* Actions */}
      <div style={styles.actions}>
        {hasChanges && (
          <span style={{ marginRight: 'auto', fontSize: '0.875rem', color: 'var(--theme-warning-500, #d97706)' }}>
            You have unsaved changes
          </span>
        )}
        <button
          type="button"
          onClick={() => setSettings(loadSettings(storageKey, initialSettings))}
          disabled={!hasChanges || saving}
          style={{
            ...styles.buttonSecondary,
            opacity: !hasChanges || saving ? 0.5 : 1,
            cursor: !hasChanges || saving ? 'not-allowed' : 'pointer',
          }}
        >
          Discard Changes
        </button>
        <button
          type="button"
          onClick={handleSave}
          disabled={saving}
          style={{
            ...styles.buttonPrimary,
            opacity: saving ? 0.7 : 1,
            cursor: saving ? 'wait' : 'pointer',
          }}
        >
          {saving ? 'Saving...' : 'Save Settings'}
        </button>
      </div>
    </div>
  )
}

export default SettingsPage

/**
 * Payload Configuration Factory for ParqueDB Studio
 *
 * Generates Payload CMS configuration with:
 * - ParqueDB database adapter
 * - OAuth.do authentication
 * - Multi-database dashboard
 * - Auto-discovered collections
 *
 * ## Payload v3 Import Map
 *
 * Payload v3 uses file path strings for custom components, not direct imports.
 * Since parquedb is a library, you need to create wrapper files in your project:
 *
 * ```typescript
 * // src/components/DatabaseDashboard.tsx
 * export { DatabaseDashboardView as default } from 'parquedb/studio'
 * ```
 *
 * Then reference it in your Payload config:
 *
 * ```typescript
 * admin: {
 *   components: {
 *     views: {
 *       Dashboard: '/src/components/DatabaseDashboard',
 *     },
 *   },
 * }
 * ```
 *
 * @example Single Database Mode
 * ```typescript
 * import { createPayloadConfig } from 'parquedb/studio'
 * import { FsBackend } from 'parquedb'
 *
 * export default createPayloadConfig({
 *   storage: new FsBackend('.db'),
 *   secret: process.env.PAYLOAD_SECRET!,
 * })
 * ```
 *
 * @example Multi-Database Mode with Component Paths
 * ```typescript
 * import { createPayloadConfig, getComponentPaths } from 'parquedb/studio'
 *
 * // Get suggested component paths (you create these wrapper files)
 * const components = getComponentPaths('/src/components/parquedb')
 *
 * export default createPayloadConfig({
 *   multiDatabase: true,
 *   componentPaths: components,
 *   oauth: {
 *     jwksUri: 'https://api.workos.com/sso/jwks/client_xxx',
 *   },
 *   secret: process.env.PAYLOAD_SECRET!,
 * })
 * ```
 */

import type { StudioConfig, DiscoveredCollection } from './types'
import type { StorageBackend } from '../types/storage'

// =============================================================================
// Types
// =============================================================================

/**
 * Component paths for Payload v3 import map
 *
 * These are file paths relative to your project root.
 * Create wrapper files that re-export parquedb components.
 */
export interface ComponentPaths {
  /**
   * Path to Dashboard component
   * Should export DatabaseDashboardView as default
   */
  Dashboard?: string

  /**
   * Path to DatabaseSelector component
   * Should export DatabaseSelectView as default
   */
  DatabaseSelector?: string

  /**
   * Path to Logo component (optional)
   */
  Logo?: string

  /**
   * Path to Icon component (optional)
   */
  Icon?: string
}

export interface PayloadConfigOptions {
  /**
   * Storage backend for single-database mode
   * Not required if multiDatabase is enabled
   */
  storage?: StorageBackend

  /**
   * Payload secret for encryption
   */
  secret: string

  /**
   * Enable multi-database mode
   * When true, shows database selector dashboard
   */
  multiDatabase?: boolean

  /**
   * Paths to custom components (Payload v3 import map)
   *
   * Since Payload v3 requires file paths (not imports), you need to create
   * wrapper files in your project that re-export parquedb components.
   *
   * @example
   * ```typescript
   * componentPaths: {
   *   Dashboard: '/src/components/parquedb/Dashboard',
   *   DatabaseSelector: '/src/components/parquedb/DatabaseSelector',
   * }
   * ```
   */
  componentPaths?: ComponentPaths

  /**
   * OAuth.do configuration for authentication
   */
  oauth?: {
    jwksUri: string
    clientId?: string
    cookieName?: string
    adminRoles?: string[]
  }

  /**
   * Studio configuration
   */
  studio?: Partial<StudioConfig>

  /**
   * Pre-discovered collections (skip discovery)
   */
  collections?: DiscoveredCollection[]

  /**
   * Custom admin UI configuration
   */
  admin?: {
    /** Custom logo URL */
    logoUrl?: string
    /** App name */
    appName?: string
    /** Custom theme */
    theme?: 'light' | 'dark' | 'auto'
  }

  /**
   * Debug mode
   */
  debug?: boolean
}

// =============================================================================
// Configuration Factory
// =============================================================================

/**
 * Create Payload CMS configuration for ParqueDB Studio
 *
 * This is the main entry point for setting up Payload with ParqueDB.
 */
export function createPayloadConfig(options: PayloadConfigOptions) {
  const {
    storage,
    secret,
    multiDatabase = false,
    componentPaths,
    oauth,
    studio,
    admin,
    debug = false,
  } = options

  // Build views config for multi-database mode
  let viewsConfig: Record<string, unknown> | undefined
  if (multiDatabase && componentPaths?.Dashboard) {
    viewsConfig = {
      // Replace default dashboard with database selector
      // Uses Payload v3 file path syntax
      Dashboard: {
        Component: componentPaths.Dashboard,
      },
    }
  }

  // Build graphics config
  let graphicsConfig: Record<string, unknown> | undefined
  if (componentPaths?.Logo || componentPaths?.Icon || admin?.logoUrl) {
    graphicsConfig = {}
    if (componentPaths?.Logo) {
      graphicsConfig.Logo = { path: componentPaths.Logo }
    } else if (admin?.logoUrl) {
      graphicsConfig.Logo = { path: admin.logoUrl }
    }
    if (componentPaths?.Icon) {
      graphicsConfig.Icon = { path: componentPaths.Icon }
    } else if (admin?.logoUrl) {
      graphicsConfig.Icon = { path: admin.logoUrl }
    }
  }

  // Build the config object
  const config: Record<string, unknown> = {
    secret,
    admin: {
      user: 'users',
      meta: {
        titleSuffix: ` - ${admin?.appName || 'ParqueDB Studio'}`,
      },
      components: {
        views: viewsConfig,
        graphics: graphicsConfig,
      },
    },
    collections: [], // Will be populated dynamically
  }

  // Add database adapter if storage provided
  if (storage) {
    // This will be dynamically imported
    config.db = {
      adapter: 'parquedb',
      storage,
      debug,
    }
  }

  // Add OAuth users collection if oauth configured
  if (oauth) {
    config.collections = [
      {
        slug: 'users',
        auth: {
          disableLocalStrategy: true,
          strategies: [{
            name: 'oauth.do',
            // Strategy will be configured at runtime
          }],
        },
        fields: [
          { name: 'email', type: 'email', required: true, unique: true },
          { name: 'name', type: 'text' },
          { name: 'externalId', type: 'text', required: true, unique: true },
          { name: 'roles', type: 'array', fields: [{ name: 'role', type: 'text' }] },
        ],
      },
    ]
  }

  // Add studio config
  if (studio) {
    config.studio = {
      dataDir: studio.dataDir || '.db',
      metadataDir: studio.metadataDir || '.studio',
      readOnly: studio.readOnly || false,
      ...studio,
    }
  }

  return config
}

/**
 * Get component paths for a given base directory
 *
 * Returns an object with suggested file paths for Payload v3 components.
 * You still need to create these wrapper files in your project.
 *
 * @param baseDir - Base directory for components (e.g., '/src/components/parquedb')
 * @returns Component paths object
 *
 * @example
 * ```typescript
 * const paths = getComponentPaths('/src/components/parquedb')
 * // {
 * //   Dashboard: '/src/components/parquedb/Dashboard',
 * //   DatabaseSelector: '/src/components/parquedb/DatabaseSelector',
 * // }
 * ```
 */
export function getComponentPaths(baseDir: string): ComponentPaths {
  const base = baseDir.endsWith('/') ? baseDir.slice(0, -1) : baseDir
  return {
    Dashboard: `${base}/Dashboard`,
    DatabaseSelector: `${base}/DatabaseSelector`,
    Logo: `${base}/Logo`,
    Icon: `${base}/Icon`,
  }
}

/**
 * Generate wrapper file content for a parquedb component
 *
 * Creates the code for a wrapper file that re-exports a parquedb component.
 * This is needed because Payload v3 requires file paths, not module imports.
 *
 * @param componentName - Name of the component to wrap
 * @returns TypeScript file content
 *
 * @example
 * ```typescript
 * const content = generateWrapperFile('DatabaseDashboardView')
 * // 'use client'
 * // export { DatabaseDashboardView as default } from 'parquedb/studio'
 *
 * // Write this to src/components/parquedb/Dashboard.tsx
 * ```
 */
export function generateWrapperFile(
  componentName: 'DatabaseDashboardView' | 'DatabaseSelectView' | 'DatabaseCard' | 'CreateDatabaseModal'
): string {
  return `'use client'

/**
 * ParqueDB Studio Component Wrapper
 *
 * This file re-exports a parquedb component for Payload v3's import map.
 * Payload requires file paths to components, so library components
 * need to be wrapped like this.
 *
 * @see https://payloadcms.com/docs/admin/components
 */
export { ${componentName} as default } from 'parquedb/studio'
`
}

/**
 * Generate all wrapper files for parquedb studio components
 *
 * Returns a map of file paths to file content. Use this with your build
 * tool or a setup script to create the wrapper files.
 *
 * @param baseDir - Base directory for wrapper files
 * @returns Map of file paths to file content
 *
 * @example
 * ```typescript
 * const wrappers = generateAllWrapperFiles('/src/components/parquedb')
 * // Map {
 * //   '/src/components/parquedb/Dashboard.tsx' => '...',
 * //   '/src/components/parquedb/DatabaseSelector.tsx' => '...',
 * // }
 *
 * // In a setup script:
 * for (const [path, content] of wrappers) {
 *   await fs.writeFile(path, content)
 * }
 * ```
 */
export function generateAllWrapperFiles(baseDir: string): Map<string, string> {
  const base = baseDir.endsWith('/') ? baseDir.slice(0, -1) : baseDir
  const wrappers = new Map<string, string>()

  wrappers.set(
    `${base}/Dashboard.tsx`,
    generateWrapperFile('DatabaseDashboardView')
  )

  wrappers.set(
    `${base}/DatabaseSelector.tsx`,
    generateWrapperFile('DatabaseSelectView')
  )

  wrappers.set(
    `${base}/DatabaseCard.tsx`,
    generateWrapperFile('DatabaseCard')
  )

  wrappers.set(
    `${base}/CreateDatabaseModal.tsx`,
    generateWrapperFile('CreateDatabaseModal')
  )

  return wrappers
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Create a minimal Payload config for development
 */
export function createDevConfig(dataDir: string = '.db') {
  return {
    secret: 'dev-secret-change-in-production',
    admin: {
      user: 'users',
    },
    db: {
      adapter: 'parquedb',
      dataDir,
    },
    collections: [],
  }
}

/**
 * Generate Payload collection configs from discovered collections
 */
export function generatePayloadCollections(
  collections: DiscoveredCollection[],
  options: { readOnly?: boolean } = {}
) {
  // Import at runtime to avoid circular dependencies
  const { generateCollections } = require('./collections')
  return generateCollections(collections, {}, options)
}

export default createPayloadConfig

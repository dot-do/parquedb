/**
 * Payload Configuration Factory for ParqueDB Studio
 *
 * Generates Payload CMS configuration with:
 * - ParqueDB database adapter
 * - OAuth.do authentication
 * - Multi-database dashboard
 * - Auto-discovered collections
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
 * @example Multi-Database Mode (Cloudflare Workers)
 * ```typescript
 * import { createPayloadConfig } from 'parquedb/studio'
 *
 * export default createPayloadConfig({
 *   multiDatabase: true,
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
    oauth,
    studio,
    admin,
    debug = false,
  } = options

  // Build the config object
  const config: Record<string, unknown> = {
    secret,
    admin: {
      user: 'users',
      meta: {
        titleSuffix: ` - ${admin?.appName || 'ParqueDB Studio'}`,
      },
      components: {
        // Custom views for multi-database mode
        views: multiDatabase ? {
          // Replace default dashboard with database selector
          Dashboard: {
            Component: '@parquedb/studio/components/views/DatabaseDashboardView',
            path: '/admin',
          },
        } : undefined,
        // Graphics customization
        graphics: admin?.logoUrl ? {
          Logo: {
            path: admin.logoUrl,
          },
          Icon: {
            path: admin.logoUrl,
          },
        } : undefined,
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

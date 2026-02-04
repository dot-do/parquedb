/**
 * Payload CMS Configuration with ParqueDB and OAuth.do Authentication
 *
 * This configuration demonstrates using ParqueDB as the database
 * backend with oauth.do for authentication instead of password-based auth.
 *
 * Key differences from the standard Payload example:
 * - Uses oauthUsers() instead of a custom Users collection
 * - Authentication is handled by external OAuth provider (WorkOS)
 * - No password fields or login forms - users authenticate via OAuth flow
 */

import path from 'path'
import { fileURLToPath } from 'url'
import { buildConfig } from 'payload'
import { lexicalEditor } from '@payloadcms/richtext-lexical'
import { parquedbAdapter, oauthUsers } from 'parquedb/payload'
import { FsBackend } from 'parquedb'

// Collections (no Users - provided by oauthUsers())
import { Posts, Categories, Media } from './collections'

// Globals
import { SiteSettings } from './globals'

const filename = fileURLToPath(import.meta.url)
const dirname = path.dirname(filename)

/**
 * Get the storage backend based on environment
 *
 * For local development: Use FsBackend with local filesystem
 * For Workers: Use R2Backend with Cloudflare R2 (injected via env)
 */
function getStorageBackend() {
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data')
  return new FsBackend(dataDir)
}

/**
 * Parse comma-separated roles from environment variable
 */
function parseRoles(envVar: string | undefined, defaultRoles: string[]): string[] {
  if (!envVar) return defaultRoles
  return envVar.split(',').map(role => role.trim()).filter(Boolean)
}

export default buildConfig({
  // Server URL for admin panel
  serverURL: process.env.PAYLOAD_PUBLIC_SERVER_URL || 'http://localhost:3000',

  // Admin panel configuration
  admin: {
    user: 'users', // oauthUsers() creates a 'users' collection
    importMap: {
      baseDir: path.resolve(dirname),
    },
    meta: {
      titleSuffix: '- ParqueDB OAuth Example',
    },
  },

  // Rich text editor
  editor: lexicalEditor(),

  // Database adapter - ParqueDB!
  db: parquedbAdapter({
    storage: getStorageBackend(),
    debug: process.env.NODE_ENV === 'development',
  }),

  // Collections (data models)
  // The oauthUsers() collection handles authentication via OAuth
  collections: [
    oauthUsers({
      // WorkOS JWKS URI for JWT verification
      // In production: https://api.workos.com/sso/jwks/client_XXX
      // In tests: http://localhost:3456/.well-known/jwks.json
      jwksUri: process.env.WORKOS_JWKS_URI!,

      // Optional: OAuth client ID for audience verification
      clientId: process.env.OAUTH_CLIENT_ID,

      // Cookie name for the auth token
      cookieName: process.env.OAUTH_COOKIE_NAME || 'auth',

      // Roles that grant admin access to Payload
      adminRoles: parseRoles(process.env.OAUTH_ADMIN_ROLES, ['admin']),

      // Roles that grant editor access (limited admin)
      editorRoles: parseRoles(process.env.OAUTH_EDITOR_ROLES, ['editor']),

      // Clock tolerance for JWT verification (handles time drift)
      clockTolerance: 60,
    }),
    Posts,
    Categories,
    Media,
  ],

  // Globals (singleton data)
  globals: [SiteSettings],

  // TypeScript output for generated types
  typescript: {
    outputFile: path.resolve(dirname, 'payload-types.ts'),
  },

  // Secret for signing (note: not used for OAuth, but required by Payload)
  secret: process.env.PAYLOAD_SECRET || 'your-secret-key-change-in-production',
})

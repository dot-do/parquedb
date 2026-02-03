/**
 * Payload CMS Configuration using ParqueDB
 *
 * This example shows how to configure Payload CMS with ParqueDB
 * as the database backend. ParqueDB stores data in Parquet files,
 * which can be stored locally or in cloud object storage like R2.
 */

import { buildConfig } from 'payload'
import { parquedbAdapter } from 'parquedb/payload'
import { FsBackend } from 'parquedb'
import path from 'path'

// Import collections
import { Posts, Categories, Media, Users } from './collections'

// Import globals
import { SiteSettings } from './globals'

/**
 * Get the storage backend based on environment
 *
 * For local development: Use FsBackend with local filesystem
 * For Workers: Use R2Backend with Cloudflare R2
 */
function getStorageBackend() {
  // In Node.js, use filesystem storage
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data')
  return new FsBackend(dataDir)
}

export default buildConfig({
  // Server URL for admin panel
  serverURL: process.env.PAYLOAD_PUBLIC_SERVER_URL || 'http://localhost:3000',

  // Admin panel configuration
  admin: {
    user: Users.slug,
    meta: {
      titleSuffix: '- ParqueDB Example',
    },
  },

  // Database adapter - ParqueDB!
  db: parquedbAdapter({
    storage: getStorageBackend(),
    debug: process.env.NODE_ENV === 'development',
  }),

  // Collections (data models)
  collections: [
    Users,
    Posts,
    Categories,
    Media,
  ],

  // Globals (singleton data)
  globals: [
    SiteSettings,
  ],

  // TypeScript output for generated types
  typescript: {
    outputFile: path.resolve(process.cwd(), 'src/payload-types.ts'),
  },

  // Secret for signing tokens
  secret: process.env.PAYLOAD_SECRET || 'your-secret-key-change-in-production',
})

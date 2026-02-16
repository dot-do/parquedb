/**
 * Payload CMS Configuration with ParqueDB
 *
 * This configuration demonstrates using ParqueDB as the database
 * backend for Payload CMS. Works in both local development and
 * Cloudflare Workers deployment via OpenNext.
 */

import path from 'path'
import { fileURLToPath } from 'url'
import { buildConfig } from 'payload'
import { lexicalEditor } from '@payloadcms/richtext-lexical'
import { parquedbAdapter } from 'parquedb/payload'
import { FsBackend } from 'parquedb'

// Collections
import { Users, Posts, Categories, Media } from './collections'

// Globals
import { SiteSettings } from './globals'

const filename = fileURLToPath(import.meta.url)
const dirname = path.dirname(filename)

/**
 * Get the storage backend based on environment
 *
 * For local development: Use FileSystemBackend with local filesystem
 * For Workers: Use R2Backend with Cloudflare R2 (injected via env)
 */
function getStorageBackend() {
  // Check if we have an R2 bucket binding (Workers environment)
  // The R2Backend will be configured in the worker entry point
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data')
  return new FsBackend(dataDir)
}

export default buildConfig({
  // Server URL for admin panel
  serverURL: process.env.PAYLOAD_PUBLIC_SERVER_URL || 'http://localhost:3000',

  // Admin panel configuration
  admin: {
    user: Users.slug,
    importMap: {
      baseDir: path.resolve(dirname),
    },
    meta: {
      titleSuffix: '- ParqueDB Example',
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
  collections: [Users, Posts, Categories, Media],

  // Globals (singleton data)
  globals: [SiteSettings],

  // TypeScript output for generated types
  typescript: {
    outputFile: path.resolve(dirname, 'payload-types.ts'),
  },

  // Secret for signing tokens
  secret: process.env.PAYLOAD_SECRET || 'your-secret-key-change-in-production',
})

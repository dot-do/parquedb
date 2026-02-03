/**
 * Studio Server
 *
 * Embedded Payload CMS server for the ParqueDB Studio.
 * Provides a web-based admin interface for editing Parquet data.
 */

import type { StorageBackend } from '../types/storage'
import type { StudioConfig, StudioServer, DiscoveredCollection } from './types'
import { discoverCollections } from './discovery'
import { generateCollections } from './collections'
import { loadMetadata, saveMetadata, mergeMetadata } from './metadata'

// =============================================================================
// Server Implementation
// =============================================================================

/**
 * Create a studio server instance
 *
 * Note: This generates configuration for manual Payload setup.
 * Full embedded Payload server requires the payload package.
 *
 * @param config - Studio configuration
 * @param storage - Storage backend
 * @returns Studio server instance
 */
export async function createStudioServer(
  config: StudioConfig,
  storage: StorageBackend
): Promise<StudioServer> {
  let collections: DiscoveredCollection[] = []
  let serverUrl = `http://localhost:${config.port}`

  // Discovery function
  const discover = async () => {
    // Discover collections
    collections = await discoverCollections(storage, config.dataDir)

    // Load existing metadata
    const existingMetadata = await loadMetadata(storage, config.metadataDir)

    // Merge with new discoveries
    const metadata = mergeMetadata(existingMetadata, collections)

    // Save updated metadata
    await saveMetadata(storage, metadata, config.metadataDir)

    return { collections, metadata }
  }

  // Initial discovery
  const { metadata } = await discover()

  // Generate Payload collection configs
  const payloadCollections = generateCollections(
    collections,
    metadata.collections,
    { readOnly: config.readOnly }
  )

  // Create server object
  const server: StudioServer = {
    async start() {
      console.log('\n' + '='.repeat(60))
      console.log('ParqueDB Studio')
      console.log('='.repeat(60))
      console.log('')
      console.log(`Data directory: ${config.dataDir}`)
      console.log(`Metadata directory: ${config.metadataDir}`)
      console.log(`Collections discovered: ${collections.length}`)
      console.log('')

      for (const col of collections) {
        console.log(`  - ${col.slug}: ${col.rowCount.toLocaleString()} rows, ${col.fields.length} fields`)
      }

      console.log('')
      console.log(`Authentication: ${config.auth}`)
      console.log(`Read-only: ${config.readOnly}`)
      console.log('')

      // Save generated config for reference
      await saveGeneratedConfig(storage, config.metadataDir, payloadCollections)

      // Try to start actual Payload server
      try {
        await startPayloadServer(config, payloadCollections, storage, serverUrl)
      } catch (error) {
        // Payload not available - show setup instructions
        printSetupInstructions(config, error as Error)
      }
    },

    async stop() {
      console.log('Studio stopped')
    },

    getUrl() {
      return serverUrl
    },

    async refresh() {
      await discover()
    },
  }

  return server
}

// =============================================================================
// Payload Server Startup
// =============================================================================

/**
 * Start the actual Payload server
 */
async function startPayloadServer(
  config: StudioConfig,
  collections: ReturnType<typeof generateCollections>,
  storage: StorageBackend,
  serverUrl: string
): Promise<void> {
  // Dynamic imports to make Payload optional
  // @ts-ignore - payload is an optional peer dependency
  let payload: { getPayload: (opts: unknown) => Promise<unknown> } | null = null

  try {
    // @ts-ignore - payload is an optional peer dependency
    payload = await import('payload') as { getPayload: (opts: unknown) => Promise<unknown> }
  } catch {
    throw new Error('Payload CMS not installed')
  }

  if (!payload) {
    throw new Error('Payload CMS not installed')
  }

  const { parquedbAdapter } = await import('../integrations/payload')

  // Build Payload config dynamically
  const payloadConfig = {
    secret: process.env.PAYLOAD_SECRET ?? 'dev-secret-change-in-production',
    db: parquedbAdapter({
      storage,
      debug: config.debug,
    }),
    collections: collections.map((col) => ({
      ...col,
    })),
    admin: {
      user: 'users',
    },
  }

  // Initialize Payload
  await payload.getPayload({
    config: payloadConfig,
  })

  console.log('')
  console.log('Studio running at:')
  console.log(`  ${serverUrl}/admin`)
  console.log('')

  // Simple HTTP server for development
  const http = await import('http')

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', `http://localhost:${config.port}`)

    if (url.pathname === '/admin' || url.pathname.startsWith('/admin/')) {
      res.writeHead(200, { 'Content-Type': 'text/html' })
      res.end(generateAdminHTML(collections))
    } else if (url.pathname === '/') {
      res.writeHead(302, { Location: '/admin' })
      res.end()
    } else {
      res.writeHead(404)
      res.end('Not Found')
    }
  })

  httpServer.listen(config.port, () => {
    console.log(`Listening on port ${config.port}`)
  })
}

/**
 * Print setup instructions when Payload is not available
 */
function printSetupInstructions(config: StudioConfig, error: Error): void {
  console.log('To start the full studio, install Payload CMS:')
  console.log('')
  console.log('  npm install payload @payloadcms/next @payloadcms/richtext-lexical')
  console.log('')
  console.log('Then create a Next.js app with the generated collection config.')
  console.log('')
  console.log('Generated collection config saved to:')
  console.log(`  ${config.metadataDir}/payload.collections.json`)
  console.log('')

  if (config.debug) {
    console.error('Error details:', error.message)
  }
}

/**
 * Generate admin HTML page
 */
function generateAdminHTML(collections: ReturnType<typeof generateCollections>): string {
  const collectionList = collections
    .map((c) => `<li><strong>${c.labels?.plural ?? c.slug}</strong> (${c.slug})</li>`)
    .join('\n')

  return `
<!DOCTYPE html>
<html>
<head>
  <title>ParqueDB Studio</title>
  <style>
    body { font-family: system-ui; padding: 2rem; max-width: 800px; margin: 0 auto; }
    h1 { color: #333; }
    .info { background: #f5f5f5; padding: 1rem; border-radius: 4px; margin: 1rem 0; }
    code { background: #e5e5e5; padding: 0.2rem 0.4rem; border-radius: 3px; }
    ul { padding-left: 1.5rem; }
    li { margin: 0.5rem 0; }
  </style>
</head>
<body>
  <h1>ParqueDB Studio</h1>
  <div class="info">
    <p>Welcome to ParqueDB Studio - a Payload CMS-powered admin interface for your Parquet data.</p>
  </div>

  <h2>Discovered Collections</h2>
  <ul>
    ${collectionList}
  </ul>

  <h2>Setup Instructions</h2>
  <p>To enable the full admin interface:</p>
  <ol>
    <li>Install Payload CMS: <code>npm install payload @payloadcms/next</code></li>
    <li>Create a Next.js app with the generated collection config</li>
    <li>Run the Payload admin panel</li>
  </ol>

  <p>See the generated config in <code>.studio/payload.collections.json</code></p>
</body>
</html>
  `
}

/**
 * Save generated Payload collection config for reference
 */
async function saveGeneratedConfig(
  storage: StorageBackend,
  metadataDir: string,
  collections: ReturnType<typeof generateCollections>
): Promise<void> {
  const path = `${metadataDir}/payload.collections.json`

  // Ensure directory exists
  const exists = await storage.exists(metadataDir)
  if (!exists) {
    await storage.mkdir(metadataDir)
  }

  const data = new TextEncoder().encode(JSON.stringify(collections, null, 2))
  await storage.write(path, data)
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Print discovery summary
 */
export function printDiscoverySummary(collections: DiscoveredCollection[]): void {
  console.log('\nDiscovered collections:')
  console.log('')

  for (const col of collections) {
    const size = formatFileSize(col.fileSize)
    const parquedbBadge = col.isParqueDB ? ' [ParqueDB]' : ''

    console.log(`  ${col.slug}${parquedbBadge}`)
    console.log(`    Path: ${col.path}`)
    console.log(`    Rows: ${col.rowCount.toLocaleString()}`)
    console.log(`    Size: ${size}`)
    console.log(`    Fields: ${col.fields.length}`)
    console.log('')
  }
}

/**
 * Format file size for display
 */
function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

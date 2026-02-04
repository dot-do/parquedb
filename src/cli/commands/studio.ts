/**
 * Studio Command
 *
 * Launch the ParqueDB Studio - a Payload CMS-based admin interface
 * for viewing and editing Parquet files.
 *
 * Usage:
 *   parquedb studio [directory]
 *   parquedb studio --port 8080
 *   parquedb studio --read-only
 */

import { print, printError, type ParsedArgs } from '../types'
import { FsBackend } from '../../storage'
import {
  createStudioServer,
  discoverCollections,
  printDiscoverySummary,
  DEFAULT_STUDIO_CONFIG,
} from '../../studio'
import type { StudioConfig } from '../../studio/types'
import {
  loadConfig,
  extractSchemaStudio,
  mergeStudioConfig,
  type ParqueDBConfig,
} from '../../config'

// =============================================================================
// Command Implementation
// =============================================================================

/**
 * Execute the studio command
 */
export async function studioCommand(args: ParsedArgs): Promise<number> {
  try {
    // Show help if requested
    if (args.options.help) {
      printStudioHelp()
      return 0
    }

    // Load parquedb.config.ts if available
    const fileConfig = await loadConfig()

    // Parse command-line arguments (CLI args override config file)
    const config = parseStudioArgs(args, fileConfig)

    // Create storage backend
    const storage = new FsBackend(config.dataDir)

    // Discover collections from Parquet files
    // Note: storage is already rooted at config.dataDir, so we pass '.' to scan from root
    print(`Scanning for Parquet files in ${config.dataDir}...`)
    const collections = await discoverCollections(storage, '.')

    if (collections.length === 0) {
      print('')
      print('No Parquet files found.')
      print('')
      print('Create some Parquet files or specify a different directory:')
      print('  parquedb studio ./path/to/data')
      print('')
      print('Or initialize a ParqueDB database:')
      print('  parquedb init')
      return 0
    }

    // Print discovery summary
    printDiscoverySummary(collections)

    // Extract studio config from schema if available
    if (fileConfig?.schema) {
      const schemaStudio = extractSchemaStudio(fileConfig.schema)
      const mergedStudio = mergeStudioConfig(fileConfig.studio, schemaStudio)

      // Apply merged studio config
      if (mergedStudio.port) config.port = mergedStudio.port
      if (mergedStudio.theme) config.theme = mergedStudio.theme

      print('')
      print(`Loaded config from parquedb.config.ts`)
      if (Object.keys(schemaStudio).length > 0) {
        print(`  Schema studio config: ${Object.keys(schemaStudio).join(', ')}`)
      }
    }

    // Create and start server
    const server = await createStudioServer(config, storage)
    await server.start()

    // Keep process alive
    process.on('SIGINT', async () => {
      print('\nShutting down...')
      await server.stop()
      process.exit(0)
    })

    // Block until terminated
    await new Promise(() => {})

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

// =============================================================================
// Argument Parsing
// =============================================================================

/**
 * Parse studio-specific arguments, merging with config file
 */
function parseStudioArgs(args: ParsedArgs, fileConfig: ParqueDBConfig | null): StudioConfig {
  // Start with defaults
  const config: StudioConfig = { ...DEFAULT_STUDIO_CONFIG }

  // Apply config file settings
  if (fileConfig?.studio) {
    if (fileConfig.studio.port) config.port = fileConfig.studio.port
    if (fileConfig.studio.theme) config.theme = fileConfig.studio.theme
    if (fileConfig.studio.defaultSidebar) config.defaultSidebar = fileConfig.studio.defaultSidebar
  }

  // Determine data directory from config file storage
  if (fileConfig?.storage) {
    if (typeof fileConfig.storage === 'object' && 'path' in fileConfig.storage) {
      config.dataDir = fileConfig.storage.path
    }
  }

  // CLI arguments override config file
  // Data directory (first positional arg)
  if (args.args[0]) {
    config.dataDir = args.args[0]
  } else if (!config.dataDir || config.dataDir === DEFAULT_STUDIO_CONFIG.dataDir) {
    config.dataDir = findDataDirectory()
  }

  // Note: args.options.directory defaults to cwd, so we don't use it here
  // The -d/--directory flag is handled in the rawArgs parsing below

  // Parse additional flags from raw args
  const rawArgs = process.argv.slice(2)

  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i]

    if (arg === '--port' || arg === '-p') {
      const port = parseInt(rawArgs[++i] ?? '', 10)
      if (!isNaN(port) && port > 0) {
        config.port = port
      }
    } else if (arg === '--read-only' || arg === '-r') {
      config.readOnly = true
    } else if (arg === '--debug') {
      config.debug = true
    } else if (arg === '--auth') {
      const auth = rawArgs[++i]
      if (auth === 'none' || auth === 'local' || auth === 'env') {
        config.auth = auth
      }
    } else if (arg === '--metadata-dir' || arg === '-m') {
      config.metadataDir = rawArgs[++i] ?? '.studio'
    } else if (arg === '--directory' || arg === '-d') {
      config.dataDir = rawArgs[++i] ?? config.dataDir
    } else if (arg === '--admin-email') {
      config.adminEmail = rawArgs[++i]
    } else if (arg === '--admin-password') {
      config.adminPassword = rawArgs[++i]
    }
  }

  return config
}

/**
 * Find the data directory
 *
 * Looks for common patterns:
 * 1. .db/ directory (ParqueDB standard)
 * 2. data/ directory
 * 3. Current directory
 */
function findDataDirectory(): string {
  const candidates = ['.db', 'data', '.']

  for (const dir of candidates) {
    try {
      const fs = require('fs')
      if (fs.existsSync(dir)) {
        const stats = fs.statSync(dir)
        if (stats.isDirectory()) {
          // Check for parquet files
          const files = fs.readdirSync(dir)
          const hasParquet = files.some(
            (f: string) => f.endsWith('.parquet') || fs.existsSync(`${dir}/${f}/data.parquet`)
          )
          if (hasParquet) {
            return dir
          }
        }
      }
    } catch {
      // Continue to next candidate
    }
  }

  return '.'
}

// =============================================================================
// Help
// =============================================================================

/**
 * Print studio command help
 */
function printStudioHelp(): void {
  print(`
ParqueDB Studio

Launch a Payload CMS-based admin interface for viewing and editing Parquet files.

USAGE:
  parquedb studio [directory] [options]

ARGUMENTS:
  directory                   Directory containing Parquet files
                             (default: .db, data, or current directory)

OPTIONS:
  -p, --port <port>          Port to run the server on (default: 3000)
  -r, --read-only            Run in read-only mode (no edits allowed)
  -m, --metadata-dir <dir>   Directory for UI metadata (default: .studio)
  --auth <mode>              Authentication mode: none, local, env (default: none)
  --admin-email <email>      Admin email for local auth (default: admin@localhost)
  --admin-password <pass>    Admin password for local auth (default: admin)
  --debug                    Enable debug logging
  -h, --help                 Show this help message

CONFIG FILE:
  If parquedb.config.ts exists, studio settings and schema layout are loaded from it:

  export default defineConfig({
    storage: { type: 'fs', path: './data' },
    schema: {
      Post: {
        title: 'string!',
        content: 'text',
        status: 'string',

        // Layout (array = rows, object = tabs)
        $layout: [['title'], 'content'],
        $sidebar: ['$id', 'status', 'createdAt'],
        $studio: {
          label: 'Blog Posts',
          status: { options: ['draft', 'published'] }
        }
      }
    },
    studio: {
      theme: 'auto',
      port: 3000,
    }
  })

EXAMPLES:
  # Start studio with auto-discovery
  parquedb studio

  # Specify data directory
  parquedb studio ./my-data

  # Read-only mode on custom port
  parquedb studio --read-only --port 8080

  # With local authentication
  parquedb studio --auth local --admin-email admin@example.com
`)
}

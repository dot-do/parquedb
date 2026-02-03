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

// =============================================================================
// Command Implementation
// =============================================================================

/**
 * Execute the studio command
 */
export async function studioCommand(args: ParsedArgs): Promise<number> {
  try {
    // Parse command-line arguments
    const config = parseStudioArgs(args)

    // Show help if requested
    if (args.options.help) {
      printStudioHelp()
      return 0
    }

    // Create storage backend
    const storage = new FsBackend(config.dataDir)

    // Discover collections
    print(`Scanning for Parquet files in ${config.dataDir}...`)
    const collections = await discoverCollections(storage, config.dataDir)

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
 * Parse studio-specific arguments
 */
function parseStudioArgs(args: ParsedArgs): StudioConfig {
  const config: StudioConfig = { ...DEFAULT_STUDIO_CONFIG }

  // Data directory (first positional arg or current directory)
  config.dataDir = args.args[0] ?? findDataDirectory()

  // Options from generic parser
  if (args.options.directory) {
    config.dataDir = args.options.directory
  }

  // Parse additional flags from raw args
  // Note: These would normally be handled by the argument parser,
  // but we extend with studio-specific options
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

EXAMPLES:
  # Start studio with auto-discovery
  parquedb studio

  # Specify data directory
  parquedb studio ./my-data

  # Read-only mode on custom port
  parquedb studio --read-only --port 8080

  # With local authentication
  parquedb studio --auth local --admin-email admin@example.com

METADATA:
  UI customization is stored in the .studio/ directory:
  - .studio/metadata.json    Field labels, descriptions, UI configuration

  This keeps data definitions separate from UI rendering concerns.

AUTO-DISCOVERY:
  The studio automatically discovers Parquet files and generates
  admin collections from their schemas. It detects:
  - .db/*.parquet files
  - .db/{namespace}/data.parquet files (ParqueDB format)
  - Custom directory structures
`)
}

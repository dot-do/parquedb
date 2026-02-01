/**
 * Init Command
 *
 * Initialize a ParqueDB database in a directory.
 *
 * Creates the following structure:
 *   ./parquedb.json    - Configuration file
 *   ./data/            - Data directory for namespaces
 *   ./events/          - Event log directory
 */

import { promises as fs } from 'node:fs'
import { join, resolve } from 'node:path'
import type { ParsedArgs } from '../index'
import { print, printSuccess, printError } from '../index'

// =============================================================================
// Types
// =============================================================================

/**
 * ParqueDB configuration file structure
 */
interface ParqueDBConfig {
  /** Config file version */
  version: '1.0'
  /** Database name */
  name: string
  /** Storage configuration */
  storage: {
    /** Storage type (fs for filesystem) */
    type: 'fs'
    /** Data directory relative to config file */
    dataDir: string
    /** Events directory relative to config file */
    eventsDir: string
  }
  /** Schema definition (optional) */
  schema?: Record<string, unknown>
  /** Created timestamp */
  createdAt: string
}

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'
const DATA_DIR = 'data'
const EVENTS_DIR = 'events'

// =============================================================================
// Init Command
// =============================================================================

/**
 * Initialize a ParqueDB database
 */
export async function initCommand(parsed: ParsedArgs): Promise<number> {
  // Get target directory from args or options
  const targetDir = parsed.args[0]
    ? resolve(parsed.args[0])
    : parsed.options.directory

  const configPath = join(targetDir, CONFIG_FILENAME)
  const dataPath = join(targetDir, DATA_DIR)
  const eventsPath = join(targetDir, EVENTS_DIR)

  // Check if already initialized
  try {
    await fs.access(configPath)
    printError(`ParqueDB is already initialized in ${targetDir}`)
    print(`Config file exists: ${configPath}`)
    return 1
  } catch {
    // Config doesn't exist, we can proceed
  }

  try {
    // Create directories
    await fs.mkdir(dataPath, { recursive: true })
    await fs.mkdir(eventsPath, { recursive: true })

    // Create config file
    const config: ParqueDBConfig = {
      version: '1.0',
      name: getDefaultDbName(targetDir),
      storage: {
        type: 'fs',
        dataDir: DATA_DIR,
        eventsDir: EVENTS_DIR,
      },
      createdAt: new Date().toISOString(),
    }

    await fs.writeFile(configPath, JSON.stringify(config, null, 2) + '\n')

    // Print success message
    if (!parsed.options.quiet) {
      printSuccess(`Initialized ParqueDB database in ${targetDir}`)
      print('')
      print('Created:')
      print(`  ${CONFIG_FILENAME}   - Configuration file`)
      print(`  ${DATA_DIR}/         - Data directory`)
      print(`  ${EVENTS_DIR}/       - Event log directory`)
      print('')
      print('Next steps:')
      print('  parquedb import <namespace> <file>   Import data')
      print('  parquedb query <namespace>           Query data')
      print('  parquedb stats                       View statistics')
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to initialize database: ${message}`)
    return 1
  }
}

/**
 * Get a default database name from the directory path
 */
function getDefaultDbName(dirPath: string): string {
  const parts = dirPath.split(/[/\\]/)
  const lastPart = parts[parts.length - 1]
  return lastPart || 'parquedb'
}

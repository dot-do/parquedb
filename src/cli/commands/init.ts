/**
 * Init Command
 *
 * Initialize a ParqueDB database in a directory.
 *
 * Creates the following structure:
 *   ./parquedb.json    - Configuration file
 *   ./data/            - Data directory for namespaces
 *   ./events/          - Event log directory
 *
 * Features:
 *   - Non-interactive mode (default): Uses sensible defaults
 *   - Interactive wizard mode (--interactive/-i): Guides user through setup
 */

import { promises as fs } from 'node:fs'
import { join, resolve, basename } from 'node:path'
import type { ParsedArgs } from '../types'
import { print, printSuccess, printError } from '../types'
import {
  isInteractive,
  promptText,
  promptSelect,
  promptConfirm,
  promptList,
  printWizardHeader,
  printWizardSummary,
  type PromptIO,
} from '../prompt'

// =============================================================================
// Types
// =============================================================================

/**
 * Storage type options
 */
export type StorageType = 'fs' | 'memory' | 'r2' | 's3'

/**
 * ParqueDB configuration file structure
 */
export interface ParqueDBConfig {
  /** Config file version */
  version: '1.0'
  /** Database name */
  name: string
  /** Storage configuration */
  storage: {
    /** Storage type */
    type: StorageType
    /** Data directory relative to config file */
    dataDir: string
    /** Events directory relative to config file */
    eventsDir: string
    /** R2 bucket name (if type is r2) */
    bucket?: string | undefined
    /** S3 bucket name (if type is s3) */
    region?: string | undefined
  }
  /** Initial namespaces to create */
  namespaces?: string[] | undefined
  /** Schema definition (optional) */
  schema?: Record<string, unknown> | undefined
  /** Created timestamp */
  createdAt: string
}

/**
 * Init command options parsed from args
 */
export interface InitOptions {
  /** Target directory */
  targetDir: string
  /** Run in interactive mode */
  interactive: boolean
  /** Database name override */
  name?: string | undefined
  /** Storage type override */
  storageType?: StorageType | undefined
  /** Initial namespaces */
  namespaces?: string[] | undefined
  /** Quiet mode */
  quiet: boolean
}

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'
const DATA_DIR = 'data'
const EVENTS_DIR = 'events'

const STORAGE_CHOICES: Array<{ value: StorageType; label: string; description: string }> = [
  { value: 'fs', label: 'Filesystem', description: 'Local file storage (default)' },
  { value: 'memory', label: 'In-Memory', description: 'For testing only' },
  { value: 'r2', label: 'Cloudflare R2', description: 'Cloud object storage' },
  { value: 's3', label: 'AWS S3', description: 'Amazon S3 storage' },
]

// =============================================================================
// Argument Parsing
// =============================================================================

/**
 * Parse init-specific arguments from the command
 */
export function parseInitArgs(parsed: ParsedArgs): InitOptions {
  const args = parsed.args
  let targetDir = parsed.options.directory
  let interactive = false
  let name: string | undefined
  let storageType: StorageType | undefined
  let namespaces: string[] | undefined

  let i = 0
  while (i < args.length) {
    const arg = args[i]

    if (arg === '-i' || arg === '--interactive') {
      interactive = true
    } else if (arg === '--name' || arg === '-n') {
      name = args[++i]
    } else if (arg === '--storage' || arg === '-s') {
      const type = args[++i] as StorageType
      if (['fs', 'memory', 'r2', 's3'].includes(type)) {
        storageType = type
      }
    } else if (arg === '--namespace' || arg === '--ns') {
      namespaces = namespaces || []
      const ns = args[++i]
      if (ns) namespaces.push(ns)
    } else if (arg && !arg.startsWith('-')) {
      // First positional argument is the directory
      targetDir = resolve(arg)
    }
    i++
  }

  return {
    targetDir,
    interactive,
    name,
    storageType,
    namespaces,
    quiet: parsed.options.quiet,
  }
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate a database name
 */
export function validateDbName(name: string): string | true {
  if (!name) {
    return 'Database name is required'
  }
  if (!/^[a-zA-Z][a-zA-Z0-9_-]*$/.test(name)) {
    return 'Name must start with a letter and contain only letters, numbers, hyphens, and underscores'
  }
  if (name.length > 64) {
    return 'Name must be 64 characters or less'
  }
  return true
}

/**
 * Validate a namespace name
 */
export function validateNamespace(name: string): string | true {
  if (!name) {
    return 'Namespace name is required'
  }
  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
    return 'Namespace must start with a letter and contain only letters, numbers, and underscores'
  }
  return true
}

/**
 * Validate a list of namespaces
 */
export function validateNamespaces(names: string[]): string | true {
  for (const name of names) {
    const result = validateNamespace(name)
    if (result !== true) {
      return `Invalid namespace "${name}": ${result}`
    }
  }
  return true
}

// =============================================================================
// Interactive Wizard
// =============================================================================

/**
 * Collected wizard values
 */
export interface WizardValues {
  name: string
  storageType: StorageType
  namespaces: string[]
  createSchema: boolean
}

/**
 * Run the interactive init wizard
 */
export async function runInitWizard(
  targetDir: string,
  defaults: Partial<WizardValues> = {},
  io?: PromptIO
): Promise<WizardValues> {
  const defaultName = defaults.name || getDefaultDbName(targetDir)

  printWizardHeader('ParqueDB Setup Wizard', 1, 3)

  // Step 1: Database name
  const name = await promptText(
    'Database name:',
    {
      default: defaultName,
      validate: validateDbName,
    },
    io
  )

  printWizardHeader('Storage Configuration', 2, 3)

  // Step 2: Storage type
  const storageType = await promptSelect<StorageType>(
    'Select storage type:',
    {
      choices: STORAGE_CHOICES,
      default: defaults.storageType || 'fs',
    },
    io
  )

  printWizardHeader('Initial Setup', 3, 3)

  // Step 3: Initial namespaces
  const namespaces = await promptList(
    'Initial namespaces:',
    {
      default: defaults.namespaces || ['users', 'posts'],
      validate: validateNamespaces,
    },
    io
  )

  // Optional: Create schema file
  const createSchema = await promptConfirm(
    'Create schema file (parquedb.schema.ts)?',
    { default: false },
    io
  )

  // Show summary
  printWizardSummary('Configuration Summary', {
    'Database Name': name,
    'Storage Type': storageType,
    Namespaces: namespaces,
    'Create Schema': createSchema,
  })

  return {
    name,
    storageType,
    namespaces,
    createSchema,
  }
}

// =============================================================================
// Init Command
// =============================================================================

/**
 * Initialize a ParqueDB database
 */
export async function initCommand(parsed: ParsedArgs, io?: PromptIO): Promise<number> {
  const options = parseInitArgs(parsed)
  const { targetDir, quiet } = options

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
    // Intentionally ignored: fs.access throws when config doesn't exist, meaning we can proceed with init
  }

  // Determine configuration values
  let config: ParqueDBConfig

  if (options.interactive && isInteractive(io)) {
    // Interactive wizard mode
    try {
      const values = await runInitWizard(
        targetDir,
        {
          ...(options.name !== undefined ? { name: options.name } : {}),
          ...(options.storageType !== undefined ? { storageType: options.storageType } : {}),
          ...(options.namespaces !== undefined ? { namespaces: options.namespaces } : {}),
        },
        io
      )

      // Confirm before proceeding
      const proceed = await promptConfirm('Create database with these settings?', { default: true }, io)

      if (!proceed) {
        print('Cancelled.')
        return 0
      }

      config = {
        version: '1.0',
        name: values.name,
        storage: {
          type: values.storageType,
          dataDir: DATA_DIR,
          eventsDir: EVENTS_DIR,
        },
        namespaces: values.namespaces.length > 0 ? values.namespaces : undefined,
        createdAt: new Date().toISOString(),
      }

      // Create schema file if requested
      if (values.createSchema) {
        await createSchemaFile(targetDir, values.name, values.namespaces)
      }
    } catch (error) {
      // Handle Ctrl+C or other interruption
      if ((error as NodeJS.ErrnoException).code === 'ERR_USE_AFTER_CLOSE') {
        print('\nCancelled.')
        return 0
      }
      throw error
    }
  } else {
    // Non-interactive mode: use defaults or provided options
    config = {
      version: '1.0',
      name: options.name || getDefaultDbName(targetDir),
      storage: {
        type: options.storageType || 'fs',
        dataDir: DATA_DIR,
        eventsDir: EVENTS_DIR,
      },
      namespaces: options.namespaces,
      createdAt: new Date().toISOString(),
    }
  }

  try {
    // Create directories
    await fs.mkdir(dataPath, { recursive: true })
    await fs.mkdir(eventsPath, { recursive: true })

    // Create namespace directories if specified
    if (config.namespaces) {
      for (const ns of config.namespaces) {
        await fs.mkdir(join(dataPath, ns), { recursive: true })
      }
    }

    // Write config file
    await fs.writeFile(configPath, JSON.stringify(config, null, 2) + '\n')

    // Print success message
    if (!quiet) {
      printSuccess(`Initialized ParqueDB database in ${targetDir}`)
      print('')
      print('Created:')
      print(`  ${CONFIG_FILENAME}   - Configuration file`)
      print(`  ${DATA_DIR}/         - Data directory`)
      print(`  ${EVENTS_DIR}/       - Event log directory`)

      if (config.namespaces && config.namespaces.length > 0) {
        print('')
        print('Namespaces:')
        for (const ns of config.namespaces) {
          print(`  ${DATA_DIR}/${ns}/`)
        }
      }

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

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get a default database name from the directory path
 */
export function getDefaultDbName(dirPath: string): string {
  const name = basename(dirPath)
  // If the name is empty or just a path separator, use 'parquedb'
  if (!name || name === '.' || name === '/' || name === '\\') {
    return 'parquedb'
  }
  // Sanitize to valid database name
  const sanitized = name
    .replace(/[^a-zA-Z0-9_-]/g, '-')
    .replace(/^[^a-zA-Z]+/, '')
    .replace(/-+/g, '-')
    .replace(/-$/, '')
  return sanitized || 'parquedb'
}

/**
 * Create a basic schema file
 */
async function createSchemaFile(targetDir: string, dbName: string, namespaces: string[]): Promise<void> {
  const schemaPath = join(targetDir, 'parquedb.schema.ts')

  const namespaceSchemas = namespaces
    .map(
      (ns) => `  ${ns}: {
    // Define fields for ${ns}
    // id: 'string'
    // name: 'string'
    // createdAt: 'datetime'
  }`
    )
    .join(',\n')

  const content = `/**
 * ParqueDB Schema Definition
 *
 * This file defines the schema for your ParqueDB database.
 * Update this file to match your data model.
 */

import { defineSchema } from 'parquedb/config'

export default defineSchema({
  name: '${dbName}',
  namespaces: {
${namespaceSchemas}
  }
})
`

  await fs.writeFile(schemaPath, content)
}

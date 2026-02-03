#!/usr/bin/env node
/**
 * ParqueDB CLI
 *
 * Command-line interface for ParqueDB database management.
 *
 * Commands:
 *   init            Initialize a ParqueDB database
 *   query           Run a query against the database
 *   import          Import data from a file
 *   export          Export data to a file
 *   stats           Show database statistics
 *
 * Usage:
 *   parquedb init [directory]
 *   parquedb query <namespace> [filter]
 *   parquedb import <namespace> <file>
 *   parquedb export <namespace> <file>
 *   parquedb stats [namespace]
 */

import { registry } from './registry'
import { initCommand } from './commands/init'
import { queryCommand } from './commands/query'
import { importCommand } from './commands/import'
import { exportCommand } from './commands/export'
import { statsCommand } from './commands/stats'
import { studioCommand } from './commands/studio'
import { generateCommand } from './commands/generate'
import { loginCommand, logoutCommand, whoamiCommand, authStatusCommand } from './commands/auth'
import { pushCommand, pullCommand, syncCommand } from './commands/sync'
import { branchCommand } from './commands/branch'
import { checkoutCommand } from './commands/checkout'
import { logCommand } from './commands/log'

// =============================================================================
// Register Built-in Commands
// =============================================================================

registry.register({
  name: 'init',
  description: 'Initialize a ParqueDB database',
  usage: 'parquedb init [directory]',
  category: 'Database',
  execute: initCommand,
})

registry.register({
  name: 'query',
  description: 'Run a query against the database',
  usage: 'parquedb query <namespace> [filter]',
  category: 'Data',
  execute: queryCommand,
})

registry.register({
  name: 'import',
  description: 'Import data from JSON/NDJSON/CSV file',
  usage: 'parquedb import <namespace> <file>',
  category: 'Data',
  execute: importCommand,
})

registry.register({
  name: 'export',
  description: 'Export data to JSON/NDJSON/CSV file',
  usage: 'parquedb export <namespace> <file>',
  category: 'Data',
  execute: exportCommand,
})

registry.register({
  name: 'stats',
  description: 'Show database statistics',
  usage: 'parquedb stats [namespace]',
  category: 'Database',
  execute: statsCommand,
})

registry.register({
  name: 'studio',
  description: 'Launch ParqueDB Studio (Payload CMS admin interface)',
  usage: 'parquedb studio [directory]',
  category: 'Admin',
  execute: studioCommand,
})

registry.register({
  name: 'generate',
  description: 'Generate typed exports from parquedb.config.ts',
  usage: 'parquedb generate [--output path]',
  category: 'Development',
  execute: generateCommand,
})

// =============================================================================
// Authentication Commands
// =============================================================================

registry.register({
  name: 'login',
  description: 'Authenticate with oauth.do',
  usage: 'parquedb login',
  category: 'Auth',
  execute: loginCommand,
})

registry.register({
  name: 'logout',
  description: 'Clear authentication tokens',
  usage: 'parquedb logout',
  category: 'Auth',
  execute: logoutCommand,
})

registry.register({
  name: 'whoami',
  description: 'Show current user info',
  usage: 'parquedb whoami',
  category: 'Auth',
  execute: whoamiCommand,
})

registry.register({
  name: 'auth',
  description: 'Check authentication status',
  usage: 'parquedb auth',
  category: 'Auth',
  execute: authStatusCommand,
})

// =============================================================================
// Sync Commands
// =============================================================================

registry.register({
  name: 'push',
  description: 'Push local database to remote',
  usage: 'parquedb push [--visibility <public|unlisted|private>] [--slug <name>]',
  category: 'Sync',
  execute: pushCommand,
})

registry.register({
  name: 'pull',
  description: 'Pull remote database to local',
  usage: 'parquedb pull <owner/database> [--directory <path>]',
  category: 'Sync',
  execute: pullCommand,
})

registry.register({
  name: 'sync',
  description: 'Bidirectional sync with conflict resolution',
  usage: 'parquedb sync [--strategy <local-wins|remote-wins|newest>]',
  category: 'Sync',
  execute: syncCommand,
})

// =============================================================================
// Branch Commands
// =============================================================================

registry.register({
  name: 'branch',
  description: 'List, create, or delete branches',
  usage: 'parquedb branch [name] [base] [-d <branch>] [-m <old> <new>]',
  category: 'Branching',
  execute: branchCommand,
})

registry.register({
  name: 'checkout',
  description: 'Switch branches or restore database state',
  usage: 'parquedb checkout <branch> [-b <branch>] [--from-git]',
  category: 'Branching',
  execute: checkoutCommand,
})

registry.register({
  name: 'log',
  description: 'Show commit history',
  usage: 'parquedb log [branch] [--oneline] [-n <count>]',
  category: 'Branching',
  execute: logCommand,
})

// =============================================================================
// Constants
// =============================================================================

const VERSION = '0.1.0'

const HELP_TEXT = `
ParqueDB CLI v${VERSION}

A command-line interface for ParqueDB database management.

USAGE:
  parquedb <command> [options]

COMMANDS:
  init [directory]              Initialize a ParqueDB database
  query <namespace> [filter]    Run a query against the database
  import <namespace> <file>     Import data from JSON/NDJSON/CSV file
  export <namespace> <file>     Export data to JSON/NDJSON/CSV file
  stats [namespace]             Show database statistics
  studio [directory]            Launch ParqueDB Studio (admin UI)
  generate [--output path]      Generate typed exports from config
  branch [name] [base]          List, create, or delete branches
  checkout <branch>             Switch branches or restore state
  log [branch]                  Show commit history
  push [options]                Push local database to remote
  pull <owner/database>         Pull remote database to local
  sync [options]                Bidirectional sync with remote
  login                         Authenticate with oauth.do
  logout                        Clear authentication tokens
  whoami                        Show current user info
  auth                          Check authentication status

OPTIONS:
  -h, --help                    Show this help message
  -v, --version                 Show version number
  -d, --directory <path>        Database directory (default: current directory)
  -f, --format <format>         Output format: json, ndjson, csv (default: json)
  -l, --limit <n>               Limit number of results
  -p, --pretty                  Pretty print JSON output

EXAMPLES:
  # Initialize a database in the current directory
  parquedb init

  # Initialize in a specific directory
  parquedb init ./mydb

  # Query all posts
  parquedb query posts

  # Query with filter
  parquedb query posts '{"status": "published"}'

  # Import data from JSON file
  parquedb import users ./users.json

  # Export data to NDJSON
  parquedb export posts ./posts.ndjson -f ndjson

  # Show all database stats
  parquedb stats

  # Show stats for specific namespace
  parquedb stats posts

  # Launch admin studio
  parquedb studio

  # Studio with specific data directory
  parquedb studio ./my-data --port 8080

  # List all branches
  parquedb branch

  # Create a new branch
  parquedb branch feature/new-schema

  # Switch to a branch
  parquedb checkout feature/new-schema

  # Create and switch to a branch
  parquedb checkout -b feature/new-schema

  # Show commit history
  parquedb log

  # Delete a branch
  parquedb branch -d feature/new-schema

  # Login to oauth.do
  parquedb login

  # Check who you're logged in as
  parquedb whoami

  # Logout
  parquedb logout

  # Push database to remote (public with custom slug)
  parquedb push --visibility public --slug my-dataset

  # Pull a public database
  parquedb pull username/my-dataset

  # Sync with remote (newest wins strategy)
  parquedb sync --strategy newest
`

// =============================================================================
// Argument Parser
// =============================================================================

/**
 * Parsed CLI arguments
 */
export interface ParsedArgs {
  command: string
  args: string[]
  options: {
    help: boolean
    version: boolean
    directory: string
    format: 'json' | 'ndjson' | 'csv'
    limit?: number
    pretty: boolean
    quiet: boolean
  }
}

/**
 * Parse command line arguments
 */
export function parseArgs(argv: string[]): ParsedArgs {
  const result: ParsedArgs = {
    command: '',
    args: [],
    options: {
      help: false,
      version: false,
      directory: process.cwd(),
      format: 'json',
      pretty: false,
      quiet: false,
    },
  }

  let i = 0
  while (i < argv.length) {
    const arg = argv[i]

    if (!arg) {
      i++
      continue
    }

    // Handle flags
    if (arg.startsWith('-')) {
      switch (arg) {
        case '-h':
        case '--help':
          result.options.help = true
          break
        case '-v':
        case '--version':
          result.options.version = true
          break
        case '-d':
        case '--directory':
          result.options.directory = argv[++i] ?? process.cwd()
          break
        case '-f':
        case '--format': {
          const format = argv[++i]
          if (format === 'json' || format === 'ndjson' || format === 'csv') {
            result.options.format = format
          } else {
            throw new Error(`Invalid format: ${format}. Valid formats: json, ndjson, csv`)
          }
          break
        }
        case '-l':
        case '--limit': {
          const limit = parseInt(argv[++i] ?? '', 10)
          if (isNaN(limit) || limit < 0) {
            throw new Error(`Invalid limit: ${argv[i]}`)
          }
          result.options.limit = limit
          break
        }
        case '-p':
        case '--pretty':
          result.options.pretty = true
          break
        case '-q':
        case '--quiet':
          result.options.quiet = true
          break
        default:
          throw new Error(`Unknown option: ${arg}`)
      }
    } else if (!result.command) {
      // First non-option is the command
      result.command = arg
    } else {
      // Rest are command arguments
      result.args.push(arg)
    }
    i++
  }

  return result
}

// =============================================================================
// Output Utilities
// =============================================================================

/**
 * Print to stdout
 */
export function print(message: string): void {
  process.stdout.write(message + '\n')
}

/**
 * Print to stderr
 */
export function printError(message: string): void {
  process.stderr.write('Error: ' + message + '\n')
}

/**
 * Print success message
 */
export function printSuccess(message: string): void {
  process.stdout.write('OK ' + message + '\n')
}

// =============================================================================
// Main Entry Point
// =============================================================================

/**
 * Main CLI entry point
 */
export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  try {
    const parsed = parseArgs(argv)

    // Handle help
    if (parsed.options.help) {
      print(HELP_TEXT)
      return 0
    }

    // Handle version
    if (parsed.options.version) {
      print(`parquedb v${VERSION}`)
      return 0
    }

    // No command provided
    if (!parsed.command) {
      print(HELP_TEXT)
      return 0
    }

    // Handle 'help' as a special case
    if (parsed.command === 'help') {
      print(HELP_TEXT)
      return 0
    }

    // Look up command in registry
    const command = registry.get(parsed.command)
    if (!command) {
      printError(`Unknown command: ${parsed.command}`)
      print('\nRun "parquedb --help" for usage.')
      return 1
    }

    // Execute the command
    return await command.execute(parsed)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

// Run CLI if this is the main module
// @ts-ignore - import.meta.url check for ESM
if (process.argv[1]?.endsWith('/cli/index.js') || process.argv[1]?.endsWith('/cli/index.ts')) {
  main().then(code => process.exit(code))
}

// =============================================================================
// Re-exports for Plugin Authors
// =============================================================================

export { registry } from './registry'
export type { Command, CommandRegistry } from './registry'

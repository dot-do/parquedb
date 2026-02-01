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

import { initCommand } from './commands/init'
import { queryCommand } from './commands/query'
import { importCommand } from './commands/import'
import { exportCommand } from './commands/export'
import { statsCommand } from './commands/stats'

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

    // Execute command
    switch (parsed.command) {
      case 'init':
        return await initCommand(parsed)
      case 'query':
        return await queryCommand(parsed)
      case 'import':
        return await importCommand(parsed)
      case 'export':
        return await exportCommand(parsed)
      case 'stats':
        return await statsCommand(parsed)
      case 'help':
        print(HELP_TEXT)
        return 0
      default:
        printError(`Unknown command: ${parsed.command}`)
        print('\nRun "parquedb --help" for usage.')
        return 1
    }
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

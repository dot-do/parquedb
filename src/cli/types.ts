/**
 * CLI Types and Utilities
 *
 * Shared types and utility functions for the ParqueDB CLI.
 * This module is imported by commands and the registry without
 * creating circular dependencies.
 */

// =============================================================================
// Types
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

// =============================================================================
// Argument Parser
// =============================================================================

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

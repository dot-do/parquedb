/**
 * CLI Argument Parser
 *
 * Pure functions for parsing command line arguments.
 * This file has no external dependencies to allow for easy testing.
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Output format options
 */
export type OutputFormatType = 'json' | 'ndjson' | 'csv' | 'table' | 'highlighted'

/**
 * Valid format strings for CLI output
 */
export const VALID_FORMATS: OutputFormatType[] = ['json', 'ndjson', 'csv', 'table', 'highlighted']

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
    format: OutputFormatType
    limit?: number
    pretty: boolean
    quiet: boolean
    noColor: boolean
  }
}

// =============================================================================
// Parser
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
      noColor: false,
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
          const format = argv[++i] as OutputFormatType
          if (VALID_FORMATS.includes(format)) {
            result.options.format = format
          } else {
            throw new Error(`Invalid format: ${format}. Valid formats: ${VALID_FORMATS.join(', ')}`)
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
        case '--no-color':
          result.options.noColor = true
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

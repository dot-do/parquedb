/**
 * MV (Materialized Views) Command
 *
 * CLI commands for managing materialized views.
 *
 * Usage:
 *   parquedb mv create <name> --from <source> [options]   # Create an MV
 *   parquedb mv list [--json] [--state <state>]           # List MVs
 *   parquedb mv show <name> [--json]                      # Show MV details
 *   parquedb mv refresh <name> [--force]                  # Refresh an MV
 *   parquedb mv drop <name> [--force]                     # Drop an MV
 */

import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { FsBackend } from '../../storage/FsBackend'
import {
  MVStorageManager,
  MVNotFoundError,
  MVAlreadyExistsError,
} from '../../materialized-views/storage'
import { refreshView } from '../../materialized-views/refresh'
import type {
  ViewDefinition,
  ViewState,
  ViewOptions,
  ViewQuery,
  RefreshMode,
} from '../../materialized-views/types'
import { viewName, isValidViewName } from '../../materialized-views/types'

// =============================================================================
// Local Helpers (to avoid utils.ts dependency on @dotdo/cli)
// =============================================================================

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const k = 1024
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  const value = bytes / Math.pow(k, i)
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

/**
 * Format a duration in milliseconds to human-readable string
 */
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  if (ms < 3600000) {
    const mins = Math.floor(ms / 60000)
    const secs = Math.floor((ms % 60000) / 1000)
    return `${mins}m ${secs}s`
  }
  const hours = Math.floor(ms / 3600000)
  const mins = Math.floor((ms % 3600000) / 60000)
  return `${hours}h ${mins}m`
}

/**
 * ANSI color codes
 */
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  gray: '\x1b[90m',
}

/**
 * Check if color output is enabled
 */
function isColorEnabled(): boolean {
  // Disable colors if NO_COLOR env is set or not a TTY
  if (process.env.NO_COLOR !== undefined) return false
  if (!process.stdout.isTTY) return false
  return true
}

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'

// =============================================================================
// Types
// =============================================================================

/**
 * Options parsed from CLI args for MV creation
 */
interface CreateOptions {
  from: string
  filter?: string
  refreshMode: RefreshMode
  schedule?: string
  gracePeriod?: string
}

// =============================================================================
// Main Command Entry
// =============================================================================

/**
 * Execute the MV command
 */
export async function mvCommand(args: ParsedArgs): Promise<number> {
  const subcommand = args.args[0]

  switch (subcommand) {
    case 'create':
      return createMVCommand(args)

    case 'list':
    case 'ls':
      return listMVCommand(args)

    case 'show':
    case 'get':
      return showMVCommand(args)

    case 'refresh':
      return refreshMVCommand(args)

    case 'drop':
    case 'delete':
    case 'rm':
      return dropMVCommand(args)

    default:
      printMVHelp()
      return 0
  }
}

// =============================================================================
// Subcommands
// =============================================================================

/**
 * Create a new materialized view
 *
 * Usage:
 *   parquedb mv create <name> --from <source> [options]
 *
 * Options:
 *   --from <source>         Source collection (required)
 *   --filter <json>         Filter expression (JSON)
 *   --refresh <mode>        Refresh mode: streaming|scheduled|manual (default: streaming)
 *   --schedule <cron>       Cron schedule (for scheduled mode)
 *   --grace-period <dur>    Grace period for stale data (e.g., 15m)
 */
async function createMVCommand(args: ParsedArgs): Promise<number> {
  const name = args.args[1]
  const directory = args.options.directory

  // Validate name
  if (!name) {
    printError('Missing required argument: <name>')
    print('')
    print('Usage: parquedb mv create <name> --from <source>')
    return 1
  }

  if (!isValidViewName(name)) {
    printError(`Invalid view name: "${name}"`)
    print('View names must start with a letter and contain only alphanumeric characters or underscores.')
    return 1
  }

  // Parse options from raw args
  const options = parseMVCreateOptions()

  if (!options.from) {
    printError('Missing required option: --from <source>')
    print('')
    print('Usage: parquedb mv create <name> --from <source>')
    return 1
  }

  // Validate scheduled mode has schedule
  if (options.refreshMode === 'scheduled' && !options.schedule) {
    printError('--schedule is required when --refresh is "scheduled"')
    return 1
  }

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    // Create storage backend and MV manager
    const storage = new FsBackend(directory)
    const mvStorage = new MVStorageManager(storage)

    // Build query
    const query: ViewQuery = {}
    if (options.filter) {
      try {
        query.filter = JSON.parse(options.filter)
      } catch {
        printError(`Invalid JSON in --filter: ${options.filter}`)
        return 1
      }
    }

    // Build view options
    const viewOptions: ViewOptions = {
      refreshMode: options.refreshMode,
    }

    if (options.schedule) {
      viewOptions.schedule = { cron: options.schedule }
    }

    if (options.gracePeriod) {
      viewOptions.gracePeriod = options.gracePeriod
    }

    // Build view definition
    const definition: ViewDefinition = {
      name: viewName(name),
      source: options.from,
      query,
      options: viewOptions,
    }

    // Create the view
    const metadata = await mvStorage.createView(definition)

    printSuccess(`Created materialized view: ${name}`)
    print('')
    print(`  Source:        ${options.from}`)
    print(`  Refresh mode:  ${options.refreshMode}`)
    if (options.schedule) {
      print(`  Schedule:      ${options.schedule}`)
    }
    if (options.filter) {
      print(`  Filter:        ${options.filter}`)
    }
    print(`  State:         ${metadata.state}`)
    print('')
    print(`Run "parquedb mv refresh ${name}" to populate the view.`)

    return 0
  } catch (error) {
    if (error instanceof MVAlreadyExistsError) {
      printError(`View "${name}" already exists`)
      print('')
      print(`Use "parquedb mv drop ${name}" to remove it first.`)
      return 1
    }

    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to create view: ${message}`)
    return 1
  }
}

/**
 * List all materialized views
 *
 * Usage:
 *   parquedb mv list [options]
 *
 * Options:
 *   --json           Output as JSON
 *   --state <state>  Filter by state (pending|ready|stale|building|error|disabled)
 */
async function listMVCommand(args: ParsedArgs): Promise<number> {
  const directory = args.options.directory
  const json = hasFlag('json')
  const stateFilter = parseOption('state')

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const mvStorage = new MVStorageManager(storage)

    // Get all views
    let views = await mvStorage.listViews()

    // Filter by state if specified
    if (stateFilter) {
      views = views.filter((v) => v.state === stateFilter)
    }

    if (json) {
      print(JSON.stringify(views, null, 2))
      return 0
    }

    if (views.length === 0) {
      print('No materialized views found.')
      if (stateFilter) {
        print(`(filtered by state: ${stateFilter})`)
      }
      print('')
      print('Create one with: parquedb mv create <name> --from <source>')
      return 0
    }

    // Print table header
    print('')
    print('Materialized Views')
    print('='.repeat(70))
    print('')
    print(
      'Name'.padEnd(25) +
        'Source'.padEnd(20) +
        'State'.padEnd(12) +
        'Last Refresh'.padEnd(15)
    )
    print('-'.repeat(70))

    // Print each view
    for (const view of views) {
      const lastRefresh = view.lastRefreshedAt
        ? formatRelativeTime(new Date(view.lastRefreshedAt))
        : 'never'

      const stateColored = formatState(view.state)

      print(
        view.name.padEnd(25) +
          view.source.padEnd(20) +
          stateColored.padEnd(12 + (stateColored.length - view.state.length)) +
          lastRefresh.padEnd(15)
      )
    }

    print('')
    print(`Total: ${views.length} view${views.length === 1 ? '' : 's'}`)

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to list views: ${message}`)
    return 1
  }
}

/**
 * Show details of a specific materialized view
 *
 * Usage:
 *   parquedb mv show <name> [options]
 *
 * Options:
 *   --json    Output as JSON
 */
async function showMVCommand(args: ParsedArgs): Promise<number> {
  const name = args.args[1]
  const directory = args.options.directory
  const json = hasFlag('json')

  if (!name) {
    printError('Missing required argument: <name>')
    print('')
    print('Usage: parquedb mv show <name>')
    return 1
  }

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const mvStorage = new MVStorageManager(storage)

    // Get view metadata
    const metadata = await mvStorage.getViewMetadata(name)

    // Get view stats
    const stats = await mvStorage.getViewStats(name)

    if (json) {
      print(
        JSON.stringify(
          {
            metadata: {
              ...metadata,
              createdAt: metadata.createdAt.toISOString(),
              lastRefreshedAt: metadata.lastRefreshedAt?.toISOString(),
              nextRefreshAt: metadata.nextRefreshAt?.toISOString(),
            },
            stats,
          },
          null,
          2
        )
      )
      return 0
    }

    // Print formatted output
    print('')
    print(`Materialized View: ${name}`)
    print('='.repeat(50))
    print('')

    // Definition
    print('Definition:')
    print(`  Source:         ${metadata.definition.source}`)
    print(`  Refresh mode:   ${metadata.definition.options.refreshMode}`)
    if (metadata.definition.options.schedule?.cron) {
      print(`  Schedule:       ${metadata.definition.options.schedule.cron}`)
    }
    if (metadata.definition.query.filter) {
      print(`  Filter:         ${JSON.stringify(metadata.definition.query.filter)}`)
    }
    if (metadata.definition.options.gracePeriod) {
      print(`  Grace period:   ${metadata.definition.options.gracePeriod}`)
    }
    print('')

    // State
    print('State:')
    print(`  Status:         ${formatState(metadata.state)}`)
    print(`  Created:        ${metadata.createdAt.toISOString()}`)
    if (metadata.lastRefreshedAt) {
      print(`  Last refresh:   ${metadata.lastRefreshedAt.toISOString()}`)
    }
    if (metadata.lastRefreshDurationMs) {
      print(`  Refresh time:   ${formatDuration(metadata.lastRefreshDurationMs)}`)
    }
    if (metadata.nextRefreshAt) {
      print(`  Next refresh:   ${metadata.nextRefreshAt.toISOString()}`)
    }
    if (metadata.documentCount !== undefined) {
      print(`  Documents:      ${metadata.documentCount.toLocaleString()}`)
    }
    if (metadata.sizeBytes !== undefined) {
      print(`  Size:           ${formatBytes(metadata.sizeBytes)}`)
    }
    print(`  Version:        ${metadata.version}`)
    if (metadata.error) {
      print(`  Error:          ${metadata.error}`)
    }
    print('')

    // Stats
    print('Statistics:')
    print(`  Total refreshes:    ${stats.totalRefreshes}`)
    print(`  Successful:         ${stats.successfulRefreshes}`)
    print(`  Failed:             ${stats.failedRefreshes}`)
    if (stats.avgRefreshDurationMs > 0) {
      print(`  Avg refresh time:   ${formatDuration(stats.avgRefreshDurationMs)}`)
    }
    print(`  Query count:        ${stats.queryCount}`)
    print(`  Cache hit ratio:    ${(stats.cacheHitRatio * 100).toFixed(1)}%`)
    print('')

    return 0
  } catch (error) {
    if (error instanceof MVNotFoundError) {
      printError(`View "${name}" not found`)
      print('')
      print('Use "parquedb mv list" to see available views.')
      return 1
    }

    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to show view: ${message}`)
    return 1
  }
}

/**
 * Refresh a materialized view
 *
 * Usage:
 *   parquedb mv refresh <name> [options]
 *
 * Options:
 *   --force    Force refresh even if view is up to date
 *   --async    Run refresh asynchronously (return immediately)
 */
async function refreshMVCommand(args: ParsedArgs): Promise<number> {
  const name = args.args[1]
  const directory = args.options.directory
  const force = hasFlag('force')

  if (!name) {
    printError('Missing required argument: <name>')
    print('')
    print('Usage: parquedb mv refresh <name>')
    return 1
  }

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const mvStorage = new MVStorageManager(storage)

    // Check view exists
    const exists = await mvStorage.viewExists(name)
    if (!exists) {
      printError(`View "${name}" not found`)
      print('')
      print('Use "parquedb mv list" to see available views.')
      return 1
    }

    // Get current metadata
    const metadata = await mvStorage.getViewMetadata(name)

    // Check if refresh is needed
    if (!force && metadata.state === 'ready') {
      print(`View "${name}" is already up to date.`)
      print('')
      print('Use --force to refresh anyway.')
      return 0
    }

    // Check if already refreshing
    if (metadata.state === 'building') {
      printError(`View "${name}" is currently being refreshed.`)
      print('Please wait for the current refresh to complete.')
      return 1
    }

    print(`Refreshing materialized view: ${name}...`)

    // Perform the refresh
    const startTime = Date.now()
    const result = await refreshView(storage, name)
    const duration = Date.now() - startTime

    if (result.success) {
      printSuccess(`Refreshed "${name}" successfully`)
      print('')
      print(`  Rows:           ${result.rowCount.toLocaleString()}`)
      print(`  Size:           ${formatBytes(result.sizeBytes)}`)
      print(`  Duration:       ${formatDuration(duration)}`)
      print(`  Source rows:    ${result.sourceRowsRead.toLocaleString()}`)
      return 0
    } else {
      printError(`Refresh failed: ${result.error}`)
      return 1
    }
  } catch (error) {
    if (error instanceof MVNotFoundError) {
      printError(`View "${name}" not found`)
      print('')
      print('Use "parquedb mv list" to see available views.')
      return 1
    }

    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to refresh view: ${message}`)
    return 1
  }
}

/**
 * Drop (delete) a materialized view
 *
 * Usage:
 *   parquedb mv drop <name> [options]
 *
 * Options:
 *   --force    Skip confirmation
 */
async function dropMVCommand(args: ParsedArgs): Promise<number> {
  const name = args.args[1]
  const directory = args.options.directory
  const force = hasFlag('force')

  if (!name) {
    printError('Missing required argument: <name>')
    print('')
    print('Usage: parquedb mv drop <name>')
    return 1
  }

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const mvStorage = new MVStorageManager(storage)

    // Check view exists
    const exists = await mvStorage.viewExists(name)
    if (!exists) {
      printError(`View "${name}" not found`)
      print('')
      print('Use "parquedb mv list" to see available views.')
      return 1
    }

    // Confirm unless --force
    if (!force) {
      print(`This will permanently delete the materialized view "${name}" and all its data.`)
      print('')
      print('Use --force to skip this confirmation.')
      print('')
      // In a real CLI, we'd prompt for confirmation here
      // For now, require --force
      return 1
    }

    // Delete the view
    const deleted = await mvStorage.deleteView(name)

    if (deleted) {
      printSuccess(`Dropped materialized view: ${name}`)
      return 0
    } else {
      printError(`Failed to drop view: ${name}`)
      return 1
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to drop view: ${message}`)
    return 1
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Parse option from raw args
 */
function parseOption(name: string): string | undefined {
  const rawArgs = process.argv.slice(2)

  for (let i = 0; i < rawArgs.length; i++) {
    if (rawArgs[i] === `--${name}`) {
      return rawArgs[i + 1]
    }
  }

  return undefined
}

/**
 * Check if flag is present in raw args
 */
function hasFlag(name: string): boolean {
  const rawArgs = process.argv.slice(2)
  return rawArgs.includes(`--${name}`)
}

/**
 * Parse MV create options from raw args
 */
function parseMVCreateOptions(): CreateOptions {
  const from = parseOption('from')
  const filter = parseOption('filter')
  const refreshModeStr = parseOption('refresh') ?? 'streaming'
  const schedule = parseOption('schedule')
  const gracePeriod = parseOption('grace-period')

  // Validate refresh mode
  let refreshMode: RefreshMode = 'streaming'
  if (refreshModeStr === 'streaming' || refreshModeStr === 'scheduled' || refreshModeStr === 'manual') {
    refreshMode = refreshModeStr
  }

  return {
    from: from ?? '',
    filter,
    refreshMode,
    schedule,
    gracePeriod,
  }
}

/**
 * Format a date as relative time (e.g., "5 minutes ago")
 */
function formatRelativeTime(date: Date): string {
  const now = Date.now()
  const diff = now - date.getTime()

  if (diff < 60000) {
    return 'just now'
  }

  if (diff < 3600000) {
    const mins = Math.floor(diff / 60000)
    return `${mins}m ago`
  }

  if (diff < 86400000) {
    const hours = Math.floor(diff / 3600000)
    return `${hours}h ago`
  }

  const days = Math.floor(diff / 86400000)
  return `${days}d ago`
}

/**
 * Format state with color
 */
function formatState(state: ViewState): string {
  if (!isColorEnabled()) {
    return state
  }

  switch (state) {
    case 'ready':
      return `${colors.green}${state}${colors.reset}`
    case 'pending':
    case 'building':
      return `${colors.yellow}${state}${colors.reset}`
    case 'error':
      return `${colors.red}${state}${colors.reset}`
    case 'stale':
      return `${colors.yellow}${state}${colors.reset}`
    case 'disabled':
      return `${colors.gray}${state}${colors.reset}`
    default:
      return state
  }
}

/**
 * Print MV command help
 */
function printMVHelp(): void {
  print(`
ParqueDB Materialized Views (MV) Commands

Manage materialized views for pre-computed query results.

USAGE:
  parquedb mv <command> [options]

COMMANDS:
  create <name>     Create a new materialized view
  list              List all materialized views
  show <name>       Show details of a view
  refresh <name>    Refresh a view's data
  drop <name>       Delete a view

CREATE OPTIONS:
  --from <source>         Source collection (required)
  --filter <json>         Filter expression as JSON
  --refresh <mode>        Refresh mode: streaming|scheduled|manual (default: streaming)
  --schedule <cron>       Cron schedule (required for scheduled mode)
  --grace-period <dur>    Grace period for stale data (e.g., 15m, 1h)

LIST OPTIONS:
  --json                  Output as JSON
  --state <state>         Filter by state (pending|ready|stale|building|error|disabled)

SHOW OPTIONS:
  --json                  Output as JSON

REFRESH OPTIONS:
  --force                 Force refresh even if view is up to date

DROP OPTIONS:
  --force                 Skip confirmation prompt

EXAMPLES:
  # Create a simple filtered view
  parquedb mv create active_users --from users --filter '{"status": "active"}'

  # Create a scheduled view (refreshes hourly)
  parquedb mv create hourly_stats --from events --refresh scheduled --schedule "0 * * * *"

  # Create a manual refresh view
  parquedb mv create reports --from orders --refresh manual

  # List all views
  parquedb mv list

  # List only ready views
  parquedb mv list --state ready

  # Show view details
  parquedb mv show active_users

  # Refresh a view
  parquedb mv refresh active_users

  # Force refresh
  parquedb mv refresh hourly_stats --force

  # Delete a view
  parquedb mv drop active_users --force

REFRESH MODES:
  streaming     View is updated automatically when source data changes (default)
  scheduled     View is refreshed on a cron schedule
  manual        View is only refreshed when explicitly requested

VIEW STATES:
  pending       View is created but not yet populated
  ready         View is populated and up to date
  building      View is currently being refreshed
  stale         View needs refresh (source data has changed)
  error         View refresh failed
  disabled      View is disabled and not being maintained
`)
}

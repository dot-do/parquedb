/**
 * Compaction CLI Commands
 *
 * CLI commands for compaction operations:
 * - parquedb compaction status - Show current compaction state and history
 * - parquedb compaction retry <job-id> - Manually retry a failed compaction job
 * - parquedb compaction cleanup - Force cleanup of orphaned files
 * - parquedb compaction trigger - Manually trigger compaction cycle
 */

import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { FsBackend } from '../../storage/FsBackend'
import type { CompactionMetrics } from '../../observability/compaction/types'

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'
const COMPACTION_STATE_FILE = '.compaction-state.json'
const DATA_DIR = 'data'
const EVENTS_DIR = 'events'

// =============================================================================
// Types
// =============================================================================

/**
 * Compaction job state
 */
type JobStatus = 'pending' | 'processing' | 'completed' | 'failed'

interface CompactionJob {
  id: string
  namespace: string
  windowStart: number
  windowEnd: number
  files: string[]
  status: JobStatus
  createdAt: number
  startedAt?: number | undefined
  completedAt?: number | undefined
  error?: string | undefined
  outputFile?: string | undefined
  eventsProcessed?: number | undefined
}

/**
 * Stored compaction state
 */
interface CompactionState {
  lastCompactedAt?: number | undefined
  jobs: CompactionJob[]
  orphanedFiles?: string[] | undefined
}

/**
 * Status output structure
 */
interface CompactionStatusOutput {
  lastCompactedAt?: string | undefined
  pendingJobs: number
  processingJobs: number
  completedJobs: number
  failedJobs: number
  recentJobs: Array<{
    id: string
    namespace: string
    status: JobStatus
    createdAt: string
    files: number
    error?: string | undefined
  }>
  metrics?: CompactionMetrics | undefined
  orphanedFiles?: string[] | undefined
}

// =============================================================================
// Main Command Handler
// =============================================================================

/**
 * Main compaction command handler
 * Routes to subcommands based on args
 */
export async function compactionCommand(parsed: ParsedArgs): Promise<number> {
  const subcommand = parsed.args[0]

  if (!subcommand || subcommand === 'help') {
    printCompactionHelp()
    return 0
  }

  switch (subcommand) {
    case 'status':
      return statusSubcommand(parsed)
    case 'retry':
      return retrySubcommand(parsed)
    case 'cleanup':
      return cleanupSubcommand(parsed)
    case 'trigger':
      return triggerSubcommand(parsed)
    default:
      printError(`Unknown compaction subcommand: ${subcommand}`)
      print('\nRun "parquedb compaction help" for usage.')
      return 1
  }
}

// =============================================================================
// Help
// =============================================================================

function printCompactionHelp(): void {
  print(`
Compaction Commands

USAGE:
  parquedb compaction <command> [options]

COMMANDS:
  status              Show current compaction state and history
  retry <job-id>      Manually retry a failed compaction job
  cleanup             Force cleanup of orphaned files
  trigger [namespace] Manually trigger compaction cycle

OPTIONS:
  -h, --help          Show this help message
  -d, --directory     Database directory (default: current directory)
  -f, --format        Output format: json, ndjson (default: json)
  -p, --pretty        Pretty print JSON output
  -n, --namespace     Filter by namespace
  --force             Force operation without confirmation

EXAMPLES:
  # Show compaction status
  parquedb compaction status

  # Show status for a specific namespace
  parquedb compaction status --namespace users

  # Retry a failed job
  parquedb compaction retry job-123abc

  # Clean up orphaned files
  parquedb compaction cleanup

  # Force cleanup without confirmation
  parquedb compaction cleanup --force

  # Trigger compaction for all namespaces
  parquedb compaction trigger

  # Trigger compaction for a specific namespace
  parquedb compaction trigger users
`)
}

// =============================================================================
// Status Subcommand
// =============================================================================

/**
 * Show current compaction state and history
 */
async function statusSubcommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory
  const namespace = parsed.args[1] // Optional namespace filter

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
    const state = await loadCompactionState(directory)
    const storage = new FsBackend(directory)

    // Gather status information
    const status = await gatherCompactionStatus(storage, state, namespace)

    // Output
    if (parsed.options.format === 'json') {
      print(JSON.stringify(status, null, parsed.options.pretty ? 2 : 0))
    } else {
      printCompactionStatus(status)
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to get compaction status: ${message}`)
    return 1
  }
}

/**
 * Gather compaction status information
 */
async function gatherCompactionStatus(
  storage: FsBackend,
  state: CompactionState,
  namespace?: string
): Promise<CompactionStatusOutput> {
  // Filter jobs by namespace if specified
  let jobs = state.jobs
  if (namespace) {
    jobs = jobs.filter(j => j.namespace === namespace)
  }

  // Count jobs by status
  const pending = jobs.filter(j => j.status === 'pending')
  const processing = jobs.filter(j => j.status === 'processing')
  const completed = jobs.filter(j => j.status === 'completed')
  const failed = jobs.filter(j => j.status === 'failed')

  // Get recent jobs (last 10)
  const recentJobs = jobs
    .sort((a, b) => b.createdAt - a.createdAt)
    .slice(0, 10)
    .map(j => ({
      id: j.id,
      namespace: j.namespace,
      status: j.status,
      createdAt: new Date(j.createdAt).toISOString(),
      files: j.files.length,
      error: j.error,
    }))

  // Find orphaned files
  const orphanedFiles = await findOrphanedFiles(storage)

  return {
    lastCompactedAt: state.lastCompactedAt
      ? new Date(state.lastCompactedAt).toISOString()
      : undefined,
    pendingJobs: pending.length,
    processingJobs: processing.length,
    completedJobs: completed.length,
    failedJobs: failed.length,
    recentJobs,
    orphanedFiles: orphanedFiles.length > 0 ? orphanedFiles : undefined,
  }
}

/**
 * Print compaction status in human-readable format
 */
function printCompactionStatus(status: CompactionStatusOutput): void {
  print('Compaction Status')
  print('='.repeat(50))
  print('')

  if (status.lastCompactedAt) {
    print(`Last Compacted: ${status.lastCompactedAt}`)
  } else {
    print('Last Compacted: Never')
  }

  print('')
  print('Job Summary:')
  print(`  Pending:    ${status.pendingJobs}`)
  print(`  Processing: ${status.processingJobs}`)
  print(`  Completed:  ${status.completedJobs}`)
  print(`  Failed:     ${status.failedJobs}`)

  if (status.recentJobs.length > 0) {
    print('')
    print('Recent Jobs:')
    print('-'.repeat(50))
    print('ID'.padEnd(15) + 'Namespace'.padEnd(15) + 'Status'.padEnd(12) + 'Files')
    print('-'.repeat(50))

    for (const job of status.recentJobs) {
      print(
        job.id.slice(0, 12).padEnd(15) +
        job.namespace.padEnd(15) +
        job.status.padEnd(12) +
        String(job.files)
      )
      if (job.error) {
        print(`  Error: ${job.error}`)
      }
    }
  }

  if (status.orphanedFiles && status.orphanedFiles.length > 0) {
    print('')
    print(`Orphaned Files: ${status.orphanedFiles.length}`)
    print('Run "parquedb compaction cleanup" to remove them.')
  }
}

// =============================================================================
// Retry Subcommand
// =============================================================================

/**
 * Retry a failed compaction job
 */
async function retrySubcommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory
  const jobId = parsed.args[1]

  if (!jobId) {
    printError('Job ID is required')
    print('Usage: parquedb compaction retry <job-id>')
    return 1
  }

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    return 1
  }

  try {
    const state = await loadCompactionState(directory)

    // Find the job
    const job = state.jobs.find(j => j.id === jobId || j.id.startsWith(jobId))
    if (!job) {
      printError(`Job not found: ${jobId}`)
      return 1
    }

    if (job.status !== 'failed') {
      printError(`Job ${job.id} is not in failed state (current: ${job.status})`)
      return 1
    }

    // Reset job to pending
    job.status = 'pending'
    job.error = undefined
    job.startedAt = undefined
    job.completedAt = undefined

    // Save state
    await saveCompactionState(directory, state)

    if (parsed.options.format === 'json') {
      print(JSON.stringify({ success: true, jobId: job.id, status: 'pending' }, null, parsed.options.pretty ? 2 : 0))
    } else {
      printSuccess(`Job ${job.id} queued for retry`)
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to retry job: ${message}`)
    return 1
  }
}

// =============================================================================
// Cleanup Subcommand
// =============================================================================

/**
 * Force cleanup of orphaned files
 */
async function cleanupSubcommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory
  const force = parsed.args.includes('--force')

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const orphanedFiles = await findOrphanedFiles(storage)

    if (orphanedFiles.length === 0) {
      if (parsed.options.format === 'json') {
        print(JSON.stringify({ success: true, filesRemoved: 0 }, null, parsed.options.pretty ? 2 : 0))
      } else {
        print('No orphaned files found.')
      }
      return 0
    }

    if (!force) {
      print(`Found ${orphanedFiles.length} orphaned files:`)
      for (const file of orphanedFiles.slice(0, 10)) {
        print(`  ${file}`)
      }
      if (orphanedFiles.length > 10) {
        print(`  ... and ${orphanedFiles.length - 10} more`)
      }
      print('')
      print('Run with --force to remove these files.')
      return 0
    }

    // Remove orphaned files
    let removed = 0
    const errors: string[] = []

    for (const file of orphanedFiles) {
      try {
        await storage.delete(file)
        removed++
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err)
        errors.push(`${file}: ${msg}`)
      }
    }

    if (parsed.options.format === 'json') {
      print(JSON.stringify({
        success: errors.length === 0,
        filesRemoved: removed,
        errors: errors.length > 0 ? errors : undefined,
      }, null, parsed.options.pretty ? 2 : 0))
    } else {
      printSuccess(`Removed ${removed} orphaned files`)
      if (errors.length > 0) {
        print(`\nFailed to remove ${errors.length} files:`)
        for (const err of errors) {
          print(`  ${err}`)
        }
      }
    }

    return errors.length > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to cleanup: ${message}`)
    return 1
  }
}

// =============================================================================
// Trigger Subcommand
// =============================================================================

/**
 * Manually trigger compaction cycle
 */
async function triggerSubcommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory
  const namespace = parsed.args[1] // Optional namespace

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    return 1
  }

  try {
    const storage = new FsBackend(directory)
    const state = await loadCompactionState(directory)

    // Find namespaces to compact
    const namespaces = namespace
      ? [namespace]
      : await discoverNamespaces(storage)

    if (namespaces.length === 0) {
      if (parsed.options.format === 'json') {
        print(JSON.stringify({ success: true, message: 'No namespaces found' }, null, parsed.options.pretty ? 2 : 0))
      } else {
        print('No namespaces found to compact.')
      }
      return 0
    }

    // Create compaction jobs for each namespace
    const jobs: CompactionJob[] = []

    for (const ns of namespaces) {
      const files = await findCompactableFiles(storage, ns)
      if (files.length === 0) continue

      const job: CompactionJob = {
        id: generateJobId(),
        namespace: ns,
        windowStart: Date.now() - (24 * 60 * 60 * 1000), // Last 24 hours
        windowEnd: Date.now(),
        files,
        status: 'pending',
        createdAt: Date.now(),
      }

      jobs.push(job)
      state.jobs.push(job)
    }

    if (jobs.length === 0) {
      if (parsed.options.format === 'json') {
        print(JSON.stringify({ success: true, message: 'No files need compaction' }, null, parsed.options.pretty ? 2 : 0))
      } else {
        print('No files need compaction.')
      }
      return 0
    }

    // Save state
    await saveCompactionState(directory, state)

    if (parsed.options.format === 'json') {
      print(JSON.stringify({
        success: true,
        jobsCreated: jobs.length,
        jobs: jobs.map(j => ({
          id: j.id,
          namespace: j.namespace,
          files: j.files.length,
        })),
      }, null, parsed.options.pretty ? 2 : 0))
    } else {
      printSuccess(`Created ${jobs.length} compaction job(s)`)
      for (const job of jobs) {
        print(`  ${job.id}: ${job.namespace} (${job.files.length} files)`)
      }
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to trigger compaction: ${message}`)
    return 1
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Load compaction state from disk
 */
async function loadCompactionState(directory: string): Promise<CompactionState> {
  const statePath = join(directory, COMPACTION_STATE_FILE)

  try {
    const content = await fs.readFile(statePath, 'utf-8')
    return JSON.parse(content) as CompactionState
  } catch {
    // Return empty state if file doesn't exist
    return { jobs: [] }
  }
}

/**
 * Save compaction state to disk
 */
async function saveCompactionState(directory: string, state: CompactionState): Promise<void> {
  const statePath = join(directory, COMPACTION_STATE_FILE)
  await fs.writeFile(statePath, JSON.stringify(state, null, 2))
}

/**
 * Find orphaned files (temporary files from failed compactions)
 */
async function findOrphanedFiles(storage: FsBackend): Promise<string[]> {
  const orphaned: string[] = []

  try {
    // Look for .tmp files in data directory (recursive by default when no delimiter)
    const dataResult = await storage.list(DATA_DIR)
    for (const file of dataResult.files) {
      if (file.endsWith('.tmp') || file.includes('.partial')) {
        orphaned.push(file)
      }
    }
  } catch {
    // Directory may not exist, which is fine
  }

  try {
    // Look for orphaned event segments
    const eventsResult = await storage.list(EVENTS_DIR)
    for (const file of eventsResult.files) {
      if (file.endsWith('.tmp') || file.includes('.partial')) {
        orphaned.push(file)
      }
    }
  } catch {
    // Directory may not exist, which is fine
  }

  return orphaned
}

/**
 * Discover namespaces in the database
 */
async function discoverNamespaces(storage: FsBackend): Promise<string[]> {
  const namespaces: string[] = []

  try {
    const dataResult = await storage.list(DATA_DIR, { delimiter: '/' })
    for (const prefix of dataResult.prefixes || []) {
      const ns = prefix.replace(`${DATA_DIR}/`, '').replace(/\/$/, '')
      if (ns) {
        namespaces.push(ns)
      }
    }
  } catch {
    // Data directory may not exist
  }

  return namespaces
}

/**
 * Find files that can be compacted for a namespace
 */
async function findCompactableFiles(storage: FsBackend, namespace: string): Promise<string[]> {
  const files: string[] = []

  try {
    const prefix = `${DATA_DIR}/${namespace}`
    // Recursive by default when no delimiter is specified
    const result = await storage.list(prefix)

    for (const file of result.files) {
      // Include parquet files that aren't already compacted
      if (file.endsWith('.parquet') && !file.includes('/compacted-')) {
        files.push(file)
      }
    }
  } catch {
    // Namespace directory may not exist
  }

  return files
}

/**
 * Generate a unique job ID
 */
function generateJobId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `job-${timestamp}-${random}`
}

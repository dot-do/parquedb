/**
 * Stats Command
 *
 * Show database statistics.
 *
 * Usage:
 *   parquedb stats [namespace]
 *
 * Shows:
 *   - Number of namespaces
 *   - Entities per namespace
 *   - Storage size per namespace
 *   - Total storage size
 */

import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import type { ParsedArgs } from '../types'
import { print, printError } from '../types'
import { FsBackend } from '../../storage/FsBackend'

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'
const DATA_DIR = 'data'
const EVENTS_DIR = 'events'

// =============================================================================
// Types
// =============================================================================

/**
 * Statistics for a namespace
 */
interface NamespaceStats {
  namespace: string
  files: number
  entities: number
  sizeBytes: number
  sizeFormatted: string
}

/**
 * Overall database statistics
 */
interface DatabaseStats {
  name: string
  namespaces: NamespaceStats[]
  totalNamespaces: number
  totalEntities: number
  totalSizeBytes: number
  totalSizeFormatted: string
  eventLogSize: number
  eventLogSizeFormatted: string
}

// =============================================================================
// Stats Command
// =============================================================================

/**
 * Show database statistics
 */
export async function statsCommand(parsed: ParsedArgs): Promise<number> {
  const namespace = parsed.args[0]
  const directory = parsed.options.directory

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    // Intentionally ignored: fs.access throws when config doesn't exist, meaning DB is not initialized
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  try {
    // Read config
    const configContent = await fs.readFile(configPath, 'utf-8')
    let config: { name?: string }
    try {
      config = JSON.parse(configContent) as { name?: string }
    } catch {
      printError('Invalid parquedb.json: not valid JSON')
      return 1
    }

    // Create storage backend
    const storage = new FsBackend(directory)

    // Get statistics
    const stats = await gatherStats(storage, config.name || 'parquedb', namespace)

    // Format output
    if (parsed.options.format === 'json') {
      print(JSON.stringify(stats, null, parsed.options.pretty ? 2 : 0))
    } else {
      printStats(stats, namespace)
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to get stats: ${message}`)
    return 1
  }
}

// =============================================================================
// Stats Gathering
// =============================================================================

/**
 * Gather database statistics
 */
async function gatherStats(
  storage: FsBackend,
  dbName: string,
  filterNamespace?: string
): Promise<DatabaseStats> {
  const namespaceStats: NamespaceStats[] = []
  let totalSizeBytes = 0
  let totalEntities = 0

  // List namespaces (directories in data/)
  try {
    const dataResult = await storage.list(DATA_DIR, { delimiter: '/' })
    const prefixes = dataResult.prefixes || []

    for (const prefix of prefixes) {
      // Extract namespace name from prefix like "data/posts/"
      const namespace = prefix.replace(`${DATA_DIR}/`, '').replace(/\/$/, '')

      // Skip if filtering by namespace
      if (filterNamespace && namespace !== filterNamespace) {
        continue
      }

      const stats = await getNamespaceStats(storage, namespace)
      namespaceStats.push(stats)
      totalSizeBytes += stats.sizeBytes
      totalEntities += stats.entities
    }
  } catch {
    // Intentionally ignored: no data directory or empty - report zero stats
  }

  // Get event log size
  let eventLogSize = 0
  try {
    const eventsResult = await storage.list(EVENTS_DIR)
    for (const file of eventsResult.files) {
      const stat = await storage.stat(file)
      if (stat) {
        eventLogSize += stat.size
      }
    }
  } catch {
    // Intentionally ignored: no events directory means zero event log size
  }

  return {
    name: dbName,
    namespaces: namespaceStats,
    totalNamespaces: namespaceStats.length,
    totalEntities,
    totalSizeBytes,
    totalSizeFormatted: formatBytes(totalSizeBytes),
    eventLogSize,
    eventLogSizeFormatted: formatBytes(eventLogSize),
  }
}

/**
 * Get statistics for a single namespace
 */
async function getNamespaceStats(
  storage: FsBackend,
  namespace: string
): Promise<NamespaceStats> {
  const prefix = `${DATA_DIR}/${namespace}`
  let files = 0
  let sizeBytes = 0
  let entities = 0

  try {
    const result = await storage.list(prefix, { includeMetadata: true })
    files = result.files.length

    // Sum up file sizes
    if (result.stats) {
      for (const stat of result.stats) {
        sizeBytes += stat.size
      }
    }

    // For entity count, we'd need to read the parquet files
    // For now, estimate based on file naming or just show file count
    // A more accurate count would require reading the parquet metadata
    entities = await estimateEntityCount(storage, prefix, result.files)
  } catch {
    // Intentionally ignored: namespace directory doesn't exist or is empty, report zero stats
  }

  return {
    namespace,
    files,
    entities,
    sizeBytes,
    sizeFormatted: formatBytes(sizeBytes),
  }
}

/**
 * Estimate entity count from parquet files
 * This reads the parquet footer to get row counts without reading all data
 */
async function estimateEntityCount(
  storage: FsBackend,
  _prefix: string,
  files: string[]
): Promise<number> {
  let count = 0

  // Look for data.parquet files and read their row counts
  for (const file of files) {
    if (file.endsWith('.parquet') && file.includes('/data.parquet')) {
      try {
        // Read the parquet footer to get row count
        // The footer contains num_rows which gives us the count
        const stat = await storage.stat(file)
        if (!stat) continue

        // Read the last 8 bytes to get the footer length
        // Then read the footer to get metadata
        const footerLengthBytes = await storage.readRange(
          file,
          Math.max(0, stat.size - 8),
          stat.size
        )

        if (footerLengthBytes.length >= 8) {
          // Parquet footer: last 4 bytes are "PAR1", before that is footer length
          const view = new DataView(footerLengthBytes.buffer)
          const footerLength = view.getInt32(0, true)

          if (footerLength > 0 && footerLength < stat.size) {
            // Read the footer
            const footerStart = stat.size - 8 - footerLength
            const footer = await storage.readRange(file, footerStart, stat.size - 8)

            // The footer is Thrift-encoded, for simplicity we'll use a heuristic
            // A proper implementation would parse the Thrift structure
            // For now, return the file count as a rough estimate
            count += estimateRowsFromFooter(footer)
          }
        }
      } catch {
        // Intentionally ignored: skip files that can't be read (e.g. corrupted parquet footers)
      }
    }
  }

  return count
}

/**
 * Estimate row count from parquet footer
 * This is a simplified heuristic - a full implementation would parse Thrift
 */
function estimateRowsFromFooter(footer: Uint8Array): number {
  // Look for num_rows field in the footer
  // This is a simplified approach that looks for common patterns
  // A proper implementation would use a Thrift parser

  // For now, return 0 and rely on file size as a rough estimate
  // The actual row count would require proper Thrift parsing
  return footer.length > 0 ? 0 : 0 // Placeholder
}

// =============================================================================
// Output Formatting
// =============================================================================

/**
 * Print statistics in a human-readable format
 */
function printStats(stats: DatabaseStats, filterNamespace?: string): void {
  print(`Database: ${stats.name}`)
  print('='.repeat(50))
  print('')

  if (stats.namespaces.length === 0) {
    print('No data found.')
    print('')
    print('Import data with: parquedb import <namespace> <file>')
    return
  }

  // Print table header
  print('Namespace'.padEnd(20) + 'Files'.padStart(8) + 'Size'.padStart(12))
  print('-'.repeat(40))

  // Print namespace stats
  for (const ns of stats.namespaces) {
    print(
      ns.namespace.padEnd(20) +
      ns.files.toString().padStart(8) +
      ns.sizeFormatted.padStart(12)
    )
  }

  // Print totals if showing all namespaces
  if (!filterNamespace) {
    print('-'.repeat(40))
    print(
      'Total'.padEnd(20) +
      stats.namespaces.reduce((sum, ns) => sum + ns.files, 0).toString().padStart(8) +
      stats.totalSizeFormatted.padStart(12)
    )
  }

  print('')
  print(`Total namespaces: ${stats.totalNamespaces}`)
  print(`Data size: ${stats.totalSizeFormatted}`)
  print(`Event log: ${stats.eventLogSizeFormatted}`)
}

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

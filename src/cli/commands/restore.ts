/**
 * Restore Command
 *
 * Restore database from a backup file.
 *
 * Supported formats:
 *   - json: JSON backup format
 *   - ndjson: Newline-delimited JSON backup
 *   - parquet: Parquet backup format
 *
 * Usage:
 *   parquedb restore <file>
 *   parquedb restore ./backup.json -n posts
 *   parquedb restore ./backup.ndjson --dry-run
 */

import { join, extname, isAbsolute, resolve } from 'node:path'
import { promises as fs } from 'node:fs'
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { ParqueDB } from '../../ParqueDB'
import { FsBackend } from '../../storage/FsBackend'
import type { CreateInput, Entity } from '../../types'
import {
  validateFilePathWithAllowedDirs,
  PathValidationError,
} from '../../utils/fs-path-safety'

// =============================================================================
// Constants
// =============================================================================

import { MAX_BATCH_SIZE } from '../../constants'

const CONFIG_FILENAME = 'parquedb.json'
const BATCH_SIZE = MAX_BATCH_SIZE

// =============================================================================
// Types
// =============================================================================

/**
 * Backup metadata from the backup file
 */
interface BackupMetadata {
  version: string
  createdAt: string
  namespaces: string[]
  entityCounts: Record<string, number>
  format: 'json' | 'ndjson' | 'parquet'
}

/**
 * JSON backup format
 */
interface JsonBackup {
  _metadata: BackupMetadata
  data: Record<string, Entity[]> | Array<Entity & { _namespace: string }>
}

// =============================================================================
// Restore Command
// =============================================================================

/**
 * Restore database from a backup file
 */
export async function restoreCommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory

  // Parse restore-specific options from args
  const options = parseRestoreOptions(parsed.args)

  // Validate that we have a file path
  if (!options.file) {
    printError('Missing backup file path')
    print('Usage: parquedb restore <file> [options]')
    print('       parquedb restore ./backup.json')
    print('       parquedb restore ./backup.json -n posts --dry-run')
    return 1
  }

  // Validate file path
  const rawFilePath = options.file
  const cwd = process.cwd()
  const allowedDirs = [cwd, resolve(directory)]

  try {
    validateFilePathWithAllowedDirs(cwd, rawFilePath, allowedDirs)
  } catch (error) {
    if (error instanceof PathValidationError) {
      printError(`Invalid file path: ${error.message}`)
      return 1
    }
    throw error
  }

  const filePath = isAbsolute(rawFilePath) ? rawFilePath : resolve(cwd, rawFilePath)

  // Check if backup file exists
  try {
    await fs.access(filePath)
  } catch {
    printError(`Backup file not found: ${filePath}`)
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

  // Determine format from extension
  const ext = extname(filePath).toLowerCase()
  const format = ext === '.ndjson' || ext === '.jsonl' ? 'ndjson' : 'json'

  try {
    // Create storage backend and database
    const storage = new FsBackend(directory)
    const db = new ParqueDB({ storage })

    // Perform restore
    const result = await performRestore(
      db,
      filePath,
      format,
      options.namespace,
      options.dryRun,
      parsed.options.quiet
    )

    // Print summary
    if (!parsed.options.quiet) {
      if (options.dryRun) {
        print('DRY RUN - No changes made')
        print('')
      }
      printSuccess(`Restore complete: ${result.totalRestored} entities to ${result.namespaces.length} namespace(s)`)
      print(`  Source: ${filePath}`)
      if (result.errors > 0) {
        print(`  Errors: ${result.errors}`)
      }
      for (const ns of result.namespaces) {
        print(`  - ${ns.namespace}: ${ns.restored} entities${ns.errors > 0 ? ` (${ns.errors} errors)` : ''}`)
      }
    }

    return result.errors > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Restore failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Option Parsing
// =============================================================================

interface RestoreOptions {
  file?: string | undefined
  namespace?: string | undefined
  dryRun: boolean
}

/**
 * Parse restore-specific options from args
 */
function parseRestoreOptions(args: string[]): RestoreOptions {
  const options: RestoreOptions = {
    dryRun: false,
  }

  let i = 0
  while (i < args.length) {
    const arg = args[i]

    switch (arg) {
      case '-n':
      case '--namespace':
        options.namespace = args[++i]
        break
      case '--dry-run':
        options.dryRun = true
        break
      default:
        // First non-option argument is the file path
        if (!arg?.startsWith('-') && !options.file) {
          options.file = arg
        }
        break
    }
    i++
  }

  return options
}

// =============================================================================
// Restore Functions
// =============================================================================

interface RestoreResult {
  totalRestored: number
  errors: number
  namespaces: Array<{ namespace: string; restored: number; errors: number }>
}

/**
 * Perform the restore operation
 */
async function performRestore(
  db: ParqueDB,
  filePath: string,
  format: 'json' | 'ndjson',
  filterNamespace: string | undefined,
  dryRun: boolean,
  quiet: boolean
): Promise<RestoreResult> {
  const results: Array<{ namespace: string; restored: number; errors: number }> = []
  let totalRestored = 0
  let totalErrors = 0

  if (format === 'json') {
    // Read and parse JSON backup
    const content = await fs.readFile(filePath, 'utf-8')
    let backup: JsonBackup
    try {
      backup = JSON.parse(content) as JsonBackup
    } catch {
      throw new Error('Invalid JSON backup file')
    }

    // Validate backup format
    if (!backup._metadata) {
      throw new Error('Invalid backup file: missing _metadata')
    }

    if (!quiet) {
      print(`Backup from: ${backup._metadata.createdAt}`)
      print(`Backup version: ${backup._metadata.version}`)
      print(`Namespaces: ${backup._metadata.namespaces.join(', ')}`)
      print('')
    }

    // Handle both array format (Parquet backup) and object format (JSON backup)
    if (Array.isArray(backup.data)) {
      // Parquet/array format: each item has _namespace field
      const byNamespace = new Map<string, Entity[]>()

      for (const item of backup.data) {
        const ns = (item as Entity & { _namespace: string })._namespace
        if (filterNamespace && ns !== filterNamespace) continue

        if (!byNamespace.has(ns)) {
          byNamespace.set(ns, [])
        }
        // Remove _namespace from entity before restore
        const { _namespace, ...entity } = item as Entity & { _namespace: string }
        byNamespace.get(ns)!.push(entity as Entity)
      }

      for (const [namespace, entities] of byNamespace) {
        const result = await restoreNamespace(db, namespace, entities, dryRun, quiet)
        results.push({ namespace, ...result })
        totalRestored += result.restored
        totalErrors += result.errors
      }
    } else {
      // Object format: keyed by namespace
      for (const [namespace, entities] of Object.entries(backup.data as Record<string, Entity[]>)) {
        if (filterNamespace && namespace !== filterNamespace) continue

        const result = await restoreNamespace(db, namespace, entities, dryRun, quiet)
        results.push({ namespace, ...result })
        totalRestored += result.restored
        totalErrors += result.errors
      }
    }
  } else {
    // NDJSON format
    const result = await restoreFromNdjson(db, filePath, filterNamespace, dryRun, quiet)
    results.push(...result.namespaces)
    totalRestored = result.totalRestored
    totalErrors = result.errors
  }

  return { totalRestored, errors: totalErrors, namespaces: results }
}

/**
 * Restore entities to a namespace
 */
async function restoreNamespace(
  db: ParqueDB,
  namespace: string,
  entities: Entity[],
  dryRun: boolean,
  quiet: boolean
): Promise<{ restored: number; errors: number }> {
  let restored = 0
  let errors = 0

  if (!quiet) {
    process.stdout.write(`\rRestoring ${namespace}...`)
  }

  for (const entity of entities) {
    try {
      if (!dryRun) {
        // Prepare entity for creation (remove system fields that will be regenerated)
        const input = prepareEntityForRestore(entity)
        await db.create(namespace, input)
      }
      restored++

      if (!quiet && restored % 100 === 0) {
        process.stdout.write(`\r  ${namespace}: ${restored}/${entities.length} entities...`)
      }
    } catch (error) {
      errors++
      if (!quiet) {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`\nError restoring entity: ${message}`)
      }
    }
  }

  if (!quiet) {
    process.stdout.write('\r' + ' '.repeat(60) + '\r')
  }

  return { restored, errors }
}

/**
 * Restore from NDJSON file (streaming)
 */
async function restoreFromNdjson(
  db: ParqueDB,
  filePath: string,
  filterNamespace: string | undefined,
  dryRun: boolean,
  quiet: boolean
): Promise<RestoreResult> {
  const namespaceResults = new Map<string, { restored: number; errors: number }>()
  let metadata: BackupMetadata | undefined
  let batch: Array<{ namespace: string; entity: Entity }> = []

  const fileStream = createReadStream(filePath)
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  for await (const line of rl) {
    if (!line.trim()) continue

    try {
      const item = JSON.parse(line) as Record<string, unknown>

      // Check if this is the metadata line
      if (item._metadata) {
        metadata = item._metadata as BackupMetadata
        if (!quiet) {
          print(`Backup from: ${metadata.createdAt}`)
          print(`Backup version: ${metadata.version}`)
          print('')
        }
        continue
      }

      // Extract namespace and entity
      const namespace = item._namespace as string
      if (!namespace) continue

      if (filterNamespace && namespace !== filterNamespace) continue

      // Initialize namespace results
      if (!namespaceResults.has(namespace)) {
        namespaceResults.set(namespace, { restored: 0, errors: 0 })
      }

      // Remove _namespace from entity
      const { _namespace, ...entity } = item
      batch.push({ namespace, entity: entity as Entity })

      // Process batch when it reaches BATCH_SIZE
      if (batch.length >= BATCH_SIZE) {
        await processBatch(db, batch, namespaceResults, dryRun, quiet)
        batch = []
      }
    } catch (error) {
      // Count parse errors
      if (!quiet) {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`Parse error: ${message}`)
      }
    }
  }

  // Process remaining items
  if (batch.length > 0) {
    await processBatch(db, batch, namespaceResults, dryRun, quiet)
  }

  // Build results
  const results: Array<{ namespace: string; restored: number; errors: number }> = []
  let totalRestored = 0
  let totalErrors = 0

  for (const [namespace, result] of namespaceResults) {
    results.push({ namespace, ...result })
    totalRestored += result.restored
    totalErrors += result.errors
  }

  return { totalRestored, errors: totalErrors, namespaces: results }
}

/**
 * Process a batch of entities for restoration
 */
async function processBatch(
  db: ParqueDB,
  batch: Array<{ namespace: string; entity: Entity }>,
  results: Map<string, { restored: number; errors: number }>,
  dryRun: boolean,
  quiet: boolean
): Promise<void> {
  for (const { namespace, entity } of batch) {
    const nsResult = results.get(namespace)!

    try {
      if (!dryRun) {
        const input = prepareEntityForRestore(entity)
        await db.create(namespace, input)
      }
      nsResult.restored++
    } catch (error) {
      nsResult.errors++
      if (!quiet) {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`Error restoring entity to ${namespace}: ${message}`)
      }
    }
  }

  if (!quiet) {
    let total = 0
    for (const [, result] of results) {
      total += result.restored
    }
    process.stdout.write(`\rRestored ${total} entities...`)
  }
}

/**
 * Prepare an entity for restoration by removing system fields
 */
function prepareEntityForRestore(entity: Entity): CreateInput {
  // Remove system fields that will be regenerated
  const {
    $id: _id,
    createdAt: _createdAt,
    updatedAt: _updatedAt,
    ...rest
  } = entity as Entity & { createdAt?: unknown | undefined; updatedAt?: unknown | undefined }

  // If the entity has an explicit $id we want to preserve, include it
  // This allows restoring with the same IDs
  if (entity.$id) {
    return { ...rest, $id: entity.$id } as CreateInput
  }

  return rest as CreateInput
}

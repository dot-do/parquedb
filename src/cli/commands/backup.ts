/**
 * Backup Command
 *
 * Backup database to a file (Parquet or JSON format).
 *
 * Supported formats:
 *   - parquet: Native Parquet format (preserves schema)
 *   - json: JSON array format (human-readable)
 *   - ndjson: Newline-delimited JSON (streaming-friendly)
 *
 * Usage:
 *   parquedb backup [options]
 *   parquedb backup -n posts -o ./backup.parquet
 *   parquedb backup --all -o ./full-backup.json -f json
 */

import { join, extname, isAbsolute, resolve } from 'node:path'
import { promises as fs } from 'node:fs'
import { createWriteStream } from 'node:fs'
import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { ParqueDB } from '../../ParqueDB'
import { FsBackend } from '../../storage/FsBackend'
import type { FindOptions, Entity } from '../../types'
import {
  validateFilePathWithAllowedDirs,
  PathValidationError,
} from '../../utils/fs-path-safety'

// =============================================================================
// Constants
// =============================================================================

import { MAX_BATCH_SIZE } from '../../constants'

const CONFIG_FILENAME = 'parquedb.json'
const DATA_DIR = 'data'
const BATCH_SIZE = MAX_BATCH_SIZE

// =============================================================================
// Types
// =============================================================================

/**
 * Backup metadata included in the backup file
 */
interface BackupMetadata {
  version: string
  createdAt: string
  namespaces: string[]
  entityCounts: Record<string, number>
  format: 'json' | 'ndjson' | 'parquet'
}

// =============================================================================
// Backup Command
// =============================================================================

/**
 * Backup database to a file
 */
export async function backupCommand(parsed: ParsedArgs): Promise<number> {
  const directory = parsed.options.directory

  // Parse backup-specific options from args
  const options = parseBackupOptions(parsed.args)

  // Validate that we have either --namespace or --all
  if (!options.namespace && !options.all) {
    printError('Must specify either -n/--namespace <ns> or -a/--all')
    print('Usage: parquedb backup -n <namespace> -o <file>')
    print('       parquedb backup --all -o <file>')
    return 1
  }

  // Determine output path
  const rawFilePath = options.output || generateBackupFilename(options.format)
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

  // Check if database is initialized
  const configPath = join(directory, CONFIG_FILENAME)
  try {
    await fs.access(configPath)
  } catch {
    printError(`ParqueDB is not initialized in ${directory}`)
    print('Run "parquedb init" to initialize a database.')
    return 1
  }

  // Determine format from extension or option
  let format = options.format
  const ext = extname(filePath).toLowerCase()
  if (ext === '.ndjson' || ext === '.jsonl') {
    format = 'ndjson'
  } else if (ext === '.parquet') {
    format = 'parquet'
  }

  try {
    // Create storage backend and database
    const storage = new FsBackend(directory)
    const db = new ParqueDB({ storage })

    // Get namespaces to backup
    const namespacesToBackup = options.all
      ? await listNamespaces(storage)
      : [options.namespace!]

    if (namespacesToBackup.length === 0) {
      printError('No namespaces found to backup')
      return 1
    }

    // Perform backup
    const result = await performBackup(
      db,
      namespacesToBackup,
      filePath,
      format,
      parsed.options.quiet
    )

    // Print summary
    if (!parsed.options.quiet) {
      printSuccess(`Backup complete: ${result.totalEntities} entities from ${result.namespaces.length} namespace(s)`)
      print(`  Output: ${filePath}`)
      print(`  Format: ${format}`)
      for (const ns of result.namespaces) {
        print(`  - ${ns.namespace}: ${ns.count} entities`)
      }
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Backup failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Option Parsing
// =============================================================================

interface BackupOptions {
  namespace?: string | undefined
  all: boolean
  output?: string | undefined
  format: 'json' | 'ndjson' | 'parquet'
}

/**
 * Parse backup-specific options from args
 */
function parseBackupOptions(args: string[]): BackupOptions {
  const options: BackupOptions = {
    all: false,
    format: 'parquet',
  }

  let i = 0
  while (i < args.length) {
    const arg = args[i]

    switch (arg) {
      case '-n':
      case '--namespace':
        options.namespace = args[++i]
        break
      case '-a':
      case '--all':
        options.all = true
        break
      case '-o':
      case '--output':
        options.output = args[++i]
        break
      case '-f':
      case '--format': {
        const fmt = args[++i]
        if (fmt === 'json' || fmt === 'ndjson' || fmt === 'parquet') {
          options.format = fmt
        } else {
          throw new Error(`Invalid format: ${fmt}. Valid formats: json, ndjson, parquet`)
        }
        break
      }
      default:
        // Ignore unknown options
        break
    }
    i++
  }

  return options
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a default backup filename with timestamp
 */
function generateBackupFilename(format: string): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const ext = format === 'ndjson' ? 'ndjson' : format
  return `backup-${timestamp}.${ext}`
}

/**
 * List all namespaces in the database
 */
async function listNamespaces(storage: FsBackend): Promise<string[]> {
  try {
    const result = await storage.list(DATA_DIR, { delimiter: '/' })
    const prefixes = result.prefixes || []
    return prefixes.map(prefix =>
      prefix.replace(`${DATA_DIR}/`, '').replace(/\/$/, '')
    )
  } catch {
    return []
  }
}

/**
 * Perform the backup operation
 */
async function performBackup(
  db: ParqueDB,
  namespaces: string[],
  filePath: string,
  format: 'json' | 'ndjson' | 'parquet',
  quiet: boolean
): Promise<{
  totalEntities: number
  namespaces: Array<{ namespace: string; count: number }>
}> {
  const results: Array<{ namespace: string; count: number }> = []
  let totalEntities = 0

  // Prepare metadata
  const metadata: BackupMetadata = {
    version: '1.0',
    createdAt: new Date().toISOString(),
    namespaces,
    entityCounts: {},
    format,
  }

  switch (format) {
    case 'json': {
      const allData: Record<string, Entity[]> = {}

      for (const namespace of namespaces) {
        if (!quiet) {
          process.stdout.write(`\rBacking up ${namespace}...`)
        }

        const data = await fetchAllData(db, namespace, quiet)
        allData[namespace] = data
        metadata.entityCounts[namespace] = data.length
        results.push({ namespace, count: data.length })
        totalEntities += data.length
      }

      if (!quiet) {
        process.stdout.write('\r' + ' '.repeat(40) + '\r')
      }

      // Write JSON file with metadata
      const backup = {
        _metadata: metadata,
        data: allData,
      }
      await fs.writeFile(filePath, JSON.stringify(backup, dateReplacer, 2) + '\n')
      break
    }

    case 'ndjson': {
      const writeStream = createWriteStream(filePath)

      try {
        for (const namespace of namespaces) {
          if (!quiet) {
            process.stdout.write(`\rBacking up ${namespace}...`)
          }

          const count = await writeNdjsonNamespace(db, namespace, writeStream)
          metadata.entityCounts[namespace] = count
          results.push({ namespace, count })
          totalEntities += count
        }

        if (!quiet) {
          process.stdout.write('\r' + ' '.repeat(40) + '\r')
        }

        // Write metadata as final line
        writeStream.write('{"_metadata":' + JSON.stringify(metadata, dateReplacer) + '}\n')
      } finally {
        writeStream.end()
        await new Promise<void>((resolve, reject) => {
          writeStream.on('finish', resolve)
          writeStream.on('error', reject)
        })
      }
      break
    }

    case 'parquet': {
      // For Parquet format, we write all data to a single file
      // with namespace as a column
      const allRows: Array<Entity & { _namespace: string }> = []

      for (const namespace of namespaces) {
        if (!quiet) {
          process.stdout.write(`\rBacking up ${namespace}...`)
        }

        const data = await fetchAllData(db, namespace, quiet)
        for (const entity of data) {
          allRows.push({ ...entity, _namespace: namespace })
        }
        metadata.entityCounts[namespace] = data.length
        results.push({ namespace, count: data.length })
        totalEntities += data.length
      }

      if (!quiet) {
        process.stdout.write('\r' + ' '.repeat(40) + '\r')
      }

      // Write to JSON for now (proper Parquet writing would require hyparquet-writer)
      // TODO: Implement native Parquet backup when hyparquet-writer supports schema inference
      const backup = {
        _metadata: metadata,
        data: allRows,
      }
      await fs.writeFile(filePath, JSON.stringify(backup, dateReplacer, 2) + '\n')
      break
    }
  }

  return { totalEntities, namespaces: results }
}

/**
 * Fetch all data from a namespace (with pagination)
 */
async function fetchAllData(
  db: ParqueDB,
  namespace: string,
  quiet: boolean
): Promise<Entity[]> {
  const allData: Entity[] = []
  let cursor: string | undefined
  const options: FindOptions = { limit: BATCH_SIZE }

  do {
    const result = await db.find(namespace, undefined, {
      ...options,
      cursor,
    })

    allData.push(...result.items)
    cursor = result.nextCursor

    if (!quiet && allData.length > BATCH_SIZE) {
      process.stdout.write(`\r  ${namespace}: ${allData.length} entities...`)
    }
  } while (cursor)

  return allData
}

/**
 * Write a namespace to NDJSON stream
 */
async function writeNdjsonNamespace(
  db: ParqueDB,
  namespace: string,
  writeStream: ReturnType<typeof createWriteStream>
): Promise<number> {
  let cursor: string | undefined
  let count = 0
  const options: FindOptions = { limit: BATCH_SIZE }

  do {
    const result = await db.find(namespace, undefined, {
      ...options,
      cursor,
    })

    for (const item of result.items) {
      const row = { _namespace: namespace, ...item }
      writeStream.write(JSON.stringify(row, dateReplacer) + '\n')
      count++
    }

    cursor = result.nextCursor
  } while (cursor)

  return count
}

/**
 * JSON replacer that converts Date objects to ISO strings
 */
function dateReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Date) {
    return value.toISOString()
  }
  return value
}

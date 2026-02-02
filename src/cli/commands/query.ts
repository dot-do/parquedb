/**
 * Query Command
 *
 * Run a query against the ParqueDB database.
 *
 * Usage:
 *   parquedb query <namespace> [filter]
 *
 * Examples:
 *   parquedb query posts
 *   parquedb query posts '{"status": "published"}'
 *   parquedb query users '{"$or": [{"role": "admin"}, {"role": "editor"}]}'
 */

import { join } from 'node:path'
import { promises as fs } from 'node:fs'
import type { ParsedArgs } from '../index'
import { print, printError } from '../index'
import { ParqueDB } from '../../ParqueDB'
import { FsBackend } from '../../storage/FsBackend'
import type { Filter, FindOptions } from '../../types'

// =============================================================================
// Constants
// =============================================================================

const CONFIG_FILENAME = 'parquedb.json'

// =============================================================================
// Query Command
// =============================================================================

/**
 * Execute a query against the database
 */
export async function queryCommand(parsed: ParsedArgs): Promise<number> {
  // Validate arguments
  if (parsed.args.length < 1) {
    printError('Missing namespace argument')
    print('Usage: parquedb query <namespace> [filter]')
    return 1
  }

  const namespace = parsed.args[0]
  const filterArg = parsed.args[1]
  const directory = parsed.options.directory

  // Parse filter if provided
  let filter: Filter | undefined
  if (filterArg) {
    try {
      filter = JSON.parse(filterArg) as Filter
    } catch {
      // Intentionally ignored: JSON.parse failure means user provided invalid filter syntax
      printError(`Invalid JSON filter: ${filterArg}`)
      return 1
    }
  }

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
    // Create storage backend and database
    const storage = new FsBackend(directory)
    const db = new ParqueDB({ storage })

    // Build find options
    const options: FindOptions = {}
    if (parsed.options.limit !== undefined) {
      options.limit = parsed.options.limit
    }

    // Execute query
    const result = await db.find(namespace!, filter, options)

    // Format output
    const format = parsed.options.format
    const pretty = parsed.options.pretty

    if (result.items.length === 0) {
      if (!parsed.options.quiet) {
        print('No results found.')
      }
      return 0
    }

    switch (format) {
      case 'json':
        if (pretty) {
          print(JSON.stringify(result.items, dateReplacer, 2))
        } else {
          print(JSON.stringify(result.items, dateReplacer))
        }
        break

      case 'ndjson':
        for (const item of result.items) {
          print(JSON.stringify(item, dateReplacer))
        }
        break

      case 'csv':
        printCsv(result.items)
        break
    }

    // Print summary if not quiet
    if (!parsed.options.quiet && format === 'json') {
      const total = result.total ?? result.items.length
      const shown = result.items.length
      if (total > shown) {
        print(`\n(Showing ${shown} of ${total} results)`)
      }
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Query failed: ${message}`)
    return 1
  }
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

/**
 * Print data as CSV
 */
function printCsv(data: Record<string, unknown>[]): void {
  if (data.length === 0) return

  // Collect all unique keys from all objects
  const keys = new Set<string>()
  for (const item of data) {
    for (const key of Object.keys(item)) {
      keys.add(key)
    }
  }
  const headers = Array.from(keys)

  // Print header row
  print(headers.map(escapeCsvValue).join(','))

  // Print data rows
  for (const item of data) {
    const row = headers.map(key => {
      const value = item[key]
      return escapeCsvValue(formatCsvValue(value))
    })
    print(row.join(','))
  }
}

/**
 * Format a value for CSV output
 */
function formatCsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

/**
 * Escape a value for CSV (quote if contains comma, quote, or newline)
 */
function escapeCsvValue(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return '"' + value.replace(/"/g, '""') + '"'
  }
  return value
}

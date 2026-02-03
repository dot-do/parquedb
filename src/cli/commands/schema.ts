/**
 * Schema Command
 *
 * Schema management and inspection
 *
 * Usage:
 *   parquedb schema show                       # Show current schema
 *   parquedb schema show --at v1.0.0          # Show schema at specific version
 *   parquedb schema diff main feature/new     # Compare schemas
 *   parquedb schema check                      # Check compatibility
 */

import { print, printError, printSuccess, type ParsedArgs } from '../types'
import { loadConfig } from '../../config/loader'
import { captureSchema, loadSchemaAtCommit, diffSchemas } from '../../sync/schema-snapshot'
import { FsBackend } from '../../storage/FsBackend'
import { RefManager } from '../../sync/refs'
import { detectBreakingChanges, generateMigrationHints } from '../../sync/schema-evolution'

/**
 * Execute the schema command
 */
export async function schemaCommand(args: ParsedArgs): Promise<number> {
  const subcommand = args.args[0]

  switch (subcommand) {
    case 'show':
      return showSchemaCommand(args)

    case 'diff':
      return diffSchemaCommand(args)

    case 'check':
      return checkSchemaCommand(args)

    default:
      printSchemaHelp()
      return 0
  }
}

/**
 * Show schema at current or specific version
 */
async function showSchemaCommand(args: ParsedArgs): Promise<number> {
  try {
    const directory = args.options.directory
    const atRef = parseOption(args, 'at')
    const json = hasFlag(args, 'json')

    const storage = new FsBackend(directory)

    // Load schema
    let schema
    let sourceDescription

    if (atRef) {
      // Load from specific ref
      const refManager = new RefManager(storage)
      const commitHash = await refManager.resolveRef(atRef)

      if (!commitHash) {
        printError(`Reference not found: ${atRef}`)
        return 1
      }

      schema = await loadSchemaAtCommit(storage, commitHash)
      sourceDescription = `${atRef} (${commitHash.substring(0, 8)})`
    } else {
      // Load from current config
      const config = await loadConfig()
      if (!config) {
        printError('No parquedb.config.ts found.')
        return 1
      }

      if (!config.schema) {
        printError('No schema defined in parquedb.config.ts')
        return 1
      }

      schema = await captureSchema(config)
      sourceDescription = 'current'
    }

    // Output
    if (json) {
      print(JSON.stringify(schema, null, 2))
    } else {
      print(`Schema: ${sourceDescription}`)
      print(`Hash: ${schema.hash}`)
      print(`Captured: ${new Date(schema.capturedAt).toISOString()}`)
      if (schema.commitHash) {
        print(`Commit: ${schema.commitHash}`)
      }
      print('')

      const collectionNames = Object.keys(schema.collections)
      if (collectionNames.length === 0) {
        print('No collections defined.')
      } else {
        print(`Collections (${collectionNames.length}):`)
        print('')

        for (const name of collectionNames) {
          const collection = schema.collections[name]!
          print(`  ${name}`)
          print(`    Hash: ${collection.hash}`)
          print(`    Version: ${collection.version}`)
          print(`    Fields (${collection.fields.length}):`)

          for (const field of collection.fields) {
            const modifiers: string[] = []
            if (field.required) modifiers.push('required')
            if (field.indexed) modifiers.push('indexed')
            if (field.unique) modifiers.push('unique')
            if (field.array) modifiers.push('array')
            if (field.relationship) {
              modifiers.push(`rel:${field.relationship.direction}:${field.relationship.target}`)
            }

            const mods = modifiers.length > 0 ? ` (${modifiers.join(', ')})` : ''
            print(`      - ${field.name}: ${field.type}${mods}`)
          }

          if (collection.options) {
            print(`    Options:`)
            for (const [key, value] of Object.entries(collection.options)) {
              print(`      ${key}: ${value}`)
            }
          }

          print('')
        }
      }
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

/**
 * Compare schemas between two refs
 */
async function diffSchemaCommand(args: ParsedArgs): Promise<number> {
  try {
    const fromRef = args.args[1]
    const toRef = args.args[2] ?? 'HEAD'
    const breakingOnly = hasFlag(args, 'breaking-only')

    if (!fromRef) {
      printError('Missing required argument: <from>')
      print('')
      print('Usage: parquedb schema diff <from> [to]')
      return 1
    }

    const directory = args.options.directory
    const storage = new FsBackend(directory)
    const refManager = new RefManager(storage)

    // Resolve refs
    const fromCommit = await refManager.resolveRef(fromRef)
    const toCommit = await refManager.resolveRef(toRef)

    if (!fromCommit) {
      printError(`Reference not found: ${fromRef}`)
      return 1
    }

    if (!toCommit) {
      printError(`Reference not found: ${toRef}`)
      return 1
    }

    // Load schemas
    const fromSchema = await loadSchemaAtCommit(storage, fromCommit)
    const toSchema = await loadSchemaAtCommit(storage, toCommit)

    // Compare
    const changes = diffSchemas(fromSchema, toSchema)

    // Display
    print(`Schema diff: ${fromRef}..${toRef}`)
    print('')

    if (changes.changes.length === 0) {
      print('No schema changes.')
      return 0
    }

    print(changes.summary)
    print('')

    if (breakingOnly) {
      // Only show breaking changes
      if (changes.breakingChanges.length === 0) {
        print('No breaking changes.')
        return 0
      }

      const breaking = detectBreakingChanges(changes)
      for (const change of breaking) {
        print(`${change.description}`)
        print(`  Severity: ${change.severity}`)
        print(`  Impact: ${change.impact}`)
        print('')
      }
    } else {
      // Show all changes
      for (const change of changes.changes) {
        const icon = change.breaking ? '⚠️ ' : '✓ '
        print(`${icon} ${change.description}`)
      }
      print('')

      if (changes.breakingChanges.length > 0) {
        print('See migration hints with: parquedb schema diff --help')
      }
    }

    return changes.breakingChanges.length > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

/**
 * Check schema compatibility
 */
async function checkSchemaCommand(args: ParsedArgs): Promise<number> {
  try {
    const directory = args.options.directory
    // staged flag is parsed but not yet used (future: check staged changes)

    const storage = new FsBackend(directory)
    const refManager = new RefManager(storage)

    // Get current HEAD
    const headCommit = await refManager.resolveRef('HEAD')
    if (!headCommit) {
      printError('No HEAD commit found. Initialize with: parquedb init')
      return 1
    }

    // Load HEAD schema
    const headSchema = await loadSchemaAtCommit(storage, headCommit)

    // Load current/staged schema
    const config = await loadConfig()
    if (!config || !config.schema) {
      printError('No schema in current config')
      return 1
    }

    const currentSchema = await captureSchema(config)

    // Compare
    const changes = diffSchemas(headSchema, currentSchema)

    print('Schema compatibility check')
    print('')

    if (changes.changes.length === 0) {
      printSuccess('No schema changes detected.')
      return 0
    }

    print(changes.summary)
    print('')

    if (changes.compatible) {
      printSuccess('Schema changes are compatible (no breaking changes)')
      print('')
      print('Changes:')
      for (const change of changes.changes) {
        print(`  ✓ ${change.description}`)
      }
    } else {
      printError('Schema changes include breaking changes!')
      print('')

      const breaking = detectBreakingChanges(changes)
      for (const change of breaking) {
        print(`⚠️  ${change.description}`)
        print(`   Severity: ${change.severity}`)
        print(`   Impact: ${change.impact}`)
        print('')
      }

      // Show migration hints
      const hints = generateMigrationHints(changes)
      print('Migration required:')
      print('')
      for (const hint of hints) {
        print(hint)
      }

      return 1
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

/**
 * Parse option from args
 */
function parseOption(args: ParsedArgs, name: string): string | undefined {
  const rawArgs = process.argv.slice(2)

  for (let i = 0; i < rawArgs.length; i++) {
    if (rawArgs[i] === `--${name}`) {
      return rawArgs[i + 1]
    }
  }

  return undefined
}

/**
 * Check if flag is present
 */
function hasFlag(args: ParsedArgs, name: string): boolean {
  const rawArgs = process.argv.slice(2)
  return rawArgs.includes(`--${name}`)
}

/**
 * Print schema command help
 */
function printSchemaHelp(): void {
  print(`
ParqueDB Schema Command

Inspect and manage database schema

USAGE:
  parquedb schema show [options]           Show schema
  parquedb schema diff <from> [to]         Compare schemas
  parquedb schema check [options]          Check compatibility

SHOW OPTIONS:
  --at <ref>              Show schema at specific commit/branch/tag
  --json                  Output as JSON
  -h, --help              Show this help message

DIFF OPTIONS:
  <from>                  Base reference
  <to>                    Target reference (default: HEAD)
  --breaking-only         Show only breaking changes

CHECK OPTIONS:
  --staged                Check staged changes (not committed)

EXAMPLES:
  # Show current schema
  parquedb schema show

  # Show schema at specific version
  parquedb schema show --at v1.0.0

  # Compare schemas
  parquedb schema diff main feature/new-schema

  # Check for breaking changes
  parquedb schema diff main --breaking-only

  # Check compatibility before commit
  parquedb schema check

  # Output as JSON for tools
  parquedb schema show --json > schema.json
`)
}

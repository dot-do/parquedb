/**
 * Types Command
 *
 * Generate TypeScript type definitions from schema snapshots
 *
 * Usage:
 *   parquedb types generate                    # Generate from current schema
 *   parquedb types generate --at main          # Generate from specific ref
 *   parquedb types generate -o types/db.d.ts   # Custom output path
 *   parquedb types diff main feature/new       # Compare schemas
 */

import * as fs from 'fs'
import * as path from 'path'
import { print, printError, printSuccess, type ParsedArgs } from '../types'
import { loadConfig } from '../../config/loader'
import { captureSchema, loadSchemaAtCommit, diffSchemas } from '../../sync/schema-snapshot'
import { generateTypeScript } from '../../codegen/typescript'
import { FsBackend } from '../../storage/FsBackend'
import { RefManager } from '../../sync/refs'
import { detectBreakingChanges, generateMigrationHints } from '../../sync/schema-evolution'

/**
 * Execute the types command
 */
export async function typesCommand(args: ParsedArgs): Promise<number> {
  const subcommand = args.args[0]

  switch (subcommand) {
    case 'generate':
    case 'gen':
      return generateTypesCommand(args)

    case 'diff':
      return diffTypesCommand(args)

    default:
      printTypesHelp()
      return 0
  }
}

/**
 * Generate types from schema
 */
async function generateTypesCommand(args: ParsedArgs): Promise<number> {
  try {
    const directory = args.options.directory
    const outputPath = parseOption(args, 'output', 'o') ?? 'types/db.d.ts'
    const atRef = parseOption(args, 'at')
    const namespace = parseOption(args, 'namespace')

    // Load storage backend
    const storage = new FsBackend(directory)

    // Determine which schema to use
    let schema
    let sourceDescription

    if (atRef) {
      // Load schema from specific commit/ref
      const refManager = new RefManager(storage)
      const commitHash = await refManager.resolveRef(atRef)

      if (!commitHash) {
        printError(`Reference not found: ${atRef}`)
        return 1
      }

      schema = await loadSchemaAtCommit(storage, commitHash)
      sourceDescription = `ref ${atRef} (${commitHash.substring(0, 8)})`
    } else {
      // Load schema from current config
      const config = await loadConfig()
      if (!config) {
        printError('No parquedb.config.ts found.')
        print('')
        print('Create a config file or use --at to load from a commit')
        return 1
      }

      if (!config.schema) {
        printError('No schema defined in parquedb.config.ts')
        return 1
      }

      schema = await captureSchema(config)
      sourceDescription = 'current config'
    }

    // Generate TypeScript code
    const code = generateTypeScript(schema, {
      namespace,
      includeMetadata: true,
      includeImports: true
    })

    // Write to output file
    const outputDir = path.dirname(outputPath)
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true })
    }

    fs.writeFileSync(outputPath, code, 'utf-8')

    printSuccess(`Generated types from ${sourceDescription}`)
    print(`Output: ${outputPath}`)
    print('')
    print('Usage:')
    print(`  import type { UserEntity, Database } from './${path.relative(process.cwd(), outputPath).replace(/\.ts$/, '')}'`)

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(message)
    return 1
  }
}

/**
 * Compare types between two refs
 */
async function diffTypesCommand(args: ParsedArgs): Promise<number> {
  try {
    const fromRef = args.args[1]
    const toRef = args.args[2] ?? 'HEAD'

    if (!fromRef) {
      printError('Missing required argument: <from>')
      print('')
      print('Usage: parquedb types diff <from> [to]')
      print('')
      print('Examples:')
      print('  parquedb types diff main              # Compare main to HEAD')
      print('  parquedb types diff v1.0.0 v2.0.0     # Compare two versions')
      return 1
    }

    const directory = args.options.directory
    const storage = new FsBackend(directory)
    const refManager = new RefManager(storage)

    // Resolve refs to commit hashes
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

    // Compare schemas
    const changes = diffSchemas(fromSchema, toSchema)

    // Display results
    print(`Schema diff: ${fromRef}..${toRef}`)
    print('')

    if (changes.changes.length === 0) {
      print('No schema changes detected.')
      return 0
    }

    print(changes.summary)
    print('')

    // Show breaking changes
    if (changes.breakingChanges.length > 0) {
      const breaking = detectBreakingChanges(changes)

      print('Breaking Changes:')
      print('')
      for (const change of breaking) {
        print(`  ${change.description}`)
        print(`  Severity: ${change.severity}`)
        print(`  Impact: ${change.impact}`)
        if (change.migrationHint) {
          print('')
          print('  Migration hint:')
          for (const line of change.migrationHint.split('\n')) {
            print(`    ${line}`)
          }
        }
        print('')
      }
    }

    // Show all changes
    if (changes.changes.length > 0) {
      print('All Changes:')
      print('')
      for (const change of changes.changes) {
        const icon = change.breaking ? '⚠️ ' : '✓ '
        print(`  ${icon}${change.description}`)
      }
      print('')
    }

    // Show migration hints
    const hints = generateMigrationHints(changes)
    if (hints.length > 0) {
      print('Migration Hints:')
      print('')
      for (const hint of hints) {
        print(hint)
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
 * Parse option from args (supports both --long and -short forms)
 */
function parseOption(args: ParsedArgs, longName: string, shortName?: string): string | undefined {
  const rawArgs = process.argv.slice(2)

  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i]
    if (arg === `--${longName}` || (shortName && arg === `-${shortName}`)) {
      return rawArgs[i + 1]
    }
  }

  return undefined
}

/**
 * Print types command help
 */
function printTypesHelp(): void {
  print(`
ParqueDB Types Command

Generate TypeScript type definitions from schema snapshots

USAGE:
  parquedb types generate [options]        Generate types
  parquedb types diff <from> [to]          Compare schemas

GENERATE OPTIONS:
  --at <ref>              Generate from specific commit/branch/tag
  -o, --output <file>     Output file path (default: types/db.d.ts)
  --namespace <name>      Wrap types in namespace
  -h, --help              Show this help message

DIFF OPTIONS:
  <from>                  Base reference (commit/branch/tag)
  <to>                    Target reference (default: HEAD)

EXAMPLES:
  # Generate types from current config
  parquedb types generate

  # Generate from specific commit
  parquedb types generate --at v1.0.0

  # Custom output path
  parquedb types generate -o src/db.d.ts

  # Wrap in namespace
  parquedb types generate --namespace DB

  # Compare schemas
  parquedb types diff main feature/new-schema

  # Compare tagged versions
  parquedb types diff v1.0.0 v2.0.0

GENERATED OUTPUT:
  The generated file includes:
  - Entity type interfaces for each collection
  - Input types for create/update operations
  - Collection interfaces with typed methods
  - Schema metadata with commit hash

  Example usage:
    import type { UserEntity, PostEntity, Database } from './types/db'

    const user: UserEntity = await db.User.get(id)
    const posts: PostEntity[] = await db.Post.find({ author: user.$id })
`)
}

/**
 * Conflicts Command
 *
 * List and view merge conflicts.
 */

import type { ParsedArgs } from '../types'
import { print, printError } from '../types'

// =============================================================================
// Helper Functions
// =============================================================================

function hasFlag(args: string[], ...flags: string[]): boolean {
  return flags.some((flag) => args.includes(flag))
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

// =============================================================================
// Conflicts Command
// =============================================================================

/**
 * Conflicts command - list merge conflicts
 *
 * Usage:
 *   parquedb conflicts              List all conflicts
 *   parquedb conflicts <entity>     Show details for specific entity
 *   parquedb conflicts --json       Output as JSON
 */
export async function conflictsCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options
    const entity = parsed.args.find((arg) => !arg.startsWith('-'))
    const jsonOutput = hasFlag(parsed.args, '--json')

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Check for merge in progress
    const { hasMergeInProgress, loadMergeState } = await import('../../sync/merge-state')
    const mergeInProgress = await hasMergeInProgress(storage)

    if (!mergeInProgress) {
      printError('No merge in progress')
      return 1
    }

    const state = await loadMergeState(storage)
    if (!state) {
      printError('No merge state found')
      return 1
    }

    // If entity specified, show details for that entity
    if (entity) {
      return showEntityConflicts(state, entity, jsonOutput)
    }

    // Otherwise, list all conflicts
    return listAllConflicts(state, jsonOutput)
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Display Functions
// =============================================================================

/**
 * List all conflicts
 */
function listAllConflicts(
  state: Awaited<ReturnType<typeof import('../../sync/merge-state').loadMergeState>>,
  jsonOutput: boolean
): number {
  if (!state) return 1

  if (jsonOutput) {
    print(
      JSON.stringify(
        {
          source: state.source,
          target: state.target,
          status: state.status,
          conflicts: state.conflicts,
        },
        null,
        2
      )
    )
    return 0
  }

  print(`Merge conflicts: ${state.source} -> ${state.target}`)
  print(`Status: ${state.status}`)
  print(`Strategy: ${state.strategy}`)
  print('')

  if (state.conflicts.length === 0) {
    print('No conflicts')
    return 0
  }

  const unresolved = state.conflicts.filter((c) => !c.resolved)
  const resolved = state.conflicts.filter((c) => c.resolved)

  if (unresolved.length > 0) {
    print(`Unresolved conflicts (${unresolved.length}):`)
    print('')
    for (const conflict of unresolved) {
      const fields = conflict.fields.length > 0 ? ` [${conflict.fields.join(', ')}]` : ''
      print(`  ${conflict.entityId}${fields}`)
      print(`    Collection: ${conflict.collection}`)
    }
    print('')
  }

  if (resolved.length > 0) {
    print(`Resolved conflicts (${resolved.length}):`)
    print('')
    for (const conflict of resolved) {
      const fields = conflict.fields.length > 0 ? ` [${conflict.fields.join(', ')}]` : ''
      const resolution = conflict.resolution ? ` (${conflict.resolution})` : ''
      print(`  ${conflict.entityId}${fields}${resolution}`)
    }
    print('')
  }

  if (unresolved.length > 0) {
    print('To resolve conflicts:')
    print('  parquedb resolve <entity> --ours|--theirs|--newest')
    print('')
    print('To view details:')
    print('  parquedb conflicts <entity>')
  }

  return 0
}

/**
 * Show conflicts for a specific entity
 */
function showEntityConflicts(
  state: Awaited<ReturnType<typeof import('../../sync/merge-state').loadMergeState>>,
  entityId: string,
  jsonOutput: boolean
): number {
  if (!state) return 1

  const conflicts = state.conflicts.filter((c) => c.entityId === entityId)

  if (conflicts.length === 0) {
    printError(`No conflicts found for entity: ${entityId}`)
    return 1
  }

  if (jsonOutput) {
    print(JSON.stringify(conflicts, null, 2))
    return 0
  }

  for (const conflict of conflicts) {
    print(`Entity: ${conflict.entityId}`)
    print(`Collection: ${conflict.collection}`)
    print(`Status: ${conflict.resolved ? 'Resolved' : 'Unresolved'}`)

    if (conflict.resolution) {
      print(`Resolution: ${conflict.resolution}`)
    }

    print('')
    print('Conflicting fields:')
    for (const field of conflict.fields) {
      print(`  - ${field}`)
    }

    print('')
    print('Values:')
    print(`  Ours (${state.target}):`)
    print(`    ${JSON.stringify(conflict.ourValue, null, 4)}`)
    print('')
    print(`  Theirs (${state.source}):`)
    print(`    ${JSON.stringify(conflict.theirValue, null, 4)}`)
    print('')

    if (conflict.baseValue !== undefined) {
      print(`  Base (common ancestor):`)
      print(`    ${JSON.stringify(conflict.baseValue, null, 4)}`)
      print('')
    }

    if (conflict.resolvedValue !== undefined) {
      print(`  Resolved value:`)
      print(`    ${JSON.stringify(conflict.resolvedValue, null, 4)}`)
      print('')
    }

    if (!conflict.resolved) {
      print('To resolve this conflict:')
      print(`  parquedb resolve ${entityId} --ours      # Use our value`)
      print(`  parquedb resolve ${entityId} --theirs    # Use their value`)
      print(`  parquedb resolve ${entityId} --newest    # Use newest value`)
      print('')
    }
  }

  return 0
}

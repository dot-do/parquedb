/**
 * Resolve Command
 *
 * Resolve merge conflicts.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'

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
// Resolve Command
// =============================================================================

/**
 * Resolve command - resolve merge conflicts
 *
 * Usage:
 *   parquedb resolve <entity> --ours      Accept current branch version
 *   parquedb resolve <entity> --theirs    Accept incoming branch version
 *   parquedb resolve <entity> --newest    Accept most recently updated
 *   parquedb resolve --all --ours         Resolve all conflicts with strategy
 */
export async function resolveCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options
    const entity = parsed.args.find((arg) => !arg.startsWith('-'))
    const ours = hasFlag(parsed.args, '--ours')
    const theirs = hasFlag(parsed.args, '--theirs')
    const newest = hasFlag(parsed.args, '--newest')
    const all = hasFlag(parsed.args, '--all')

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Check for merge in progress
    const { hasMergeInProgress, loadMergeState, saveMergeState } = await import(
      '../../sync/merge-state'
    )
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

    // Determine resolution strategy
    const strategies = [ours, theirs, newest].filter(Boolean)
    if (strategies.length === 0) {
      printError('Must specify resolution strategy: --ours, --theirs, or --newest')
      print('')
      print('Usage:')
      print('  parquedb resolve <entity> --ours      # Use our value')
      print('  parquedb resolve <entity> --theirs    # Use their value')
      print('  parquedb resolve <entity> --newest    # Use newest value')
      print('  parquedb resolve --all --ours         # Resolve all with strategy')
      return 1
    }

    if (strategies.length > 1) {
      printError('Can only specify one resolution strategy')
      return 1
    }

    const strategy = ours ? 'ours' : theirs ? 'theirs' : 'newest'

    // Resolve all conflicts or specific entity
    if (all) {
      return resolveAllConflicts(storage, state, strategy)
    }

    if (!entity) {
      printError('Must specify entity ID or use --all to resolve all conflicts')
      print('')
      print('Usage:')
      print('  parquedb resolve <entity> --ours')
      print('  parquedb resolve --all --ours')
      return 1
    }

    return resolveEntityConflict(storage, state, entity, strategy)
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Resolution Functions
// =============================================================================

/**
 * Resolve conflicts for a specific entity
 */
async function resolveEntityConflict(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>,
  state: Awaited<ReturnType<typeof import('../../sync/merge-state').loadMergeState>>,
  entityPattern: string,
  strategy: 'ours' | 'theirs' | 'newest'
): Promise<number> {
  if (!state) return 1

  const { resolveConflict, saveMergeState, getConflictsByPattern, getUnresolvedConflicts } =
    await import('../../sync/merge-state')

  // Find conflicts matching the pattern
  const matchingConflicts = getConflictsByPattern(state, entityPattern)

  if (matchingConflicts.length === 0) {
    printError(`No conflicts found matching: ${entityPattern}`)
    return 1
  }

  // Resolve each matching conflict
  let updatedState = state
  for (const conflict of matchingConflicts) {
    if (conflict.resolved) {
      print(`Skipping already resolved conflict: ${conflict.entityId}`)
      continue
    }

    updatedState = resolveConflict(updatedState, conflict.entityId, strategy)
    printSuccess(`Resolved conflict for ${conflict.entityId} using ${strategy}`)
  }

  // Save updated state
  await saveMergeState(storage, updatedState)

  // Check if all conflicts are resolved
  const remaining = getUnresolvedConflicts(updatedState)
  if (remaining.length === 0) {
    print('')
    printSuccess('All conflicts resolved!')
    print('')
    print('Continue the merge with:')
    print('  parquedb merge --continue')
  } else {
    print('')
    print(`${remaining.length} conflict(s) remaining`)
    print('')
    print('To view remaining conflicts:')
    print('  parquedb conflicts')
  }

  return 0
}

/**
 * Resolve all conflicts with the same strategy
 */
async function resolveAllConflicts(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>,
  state: Awaited<ReturnType<typeof import('../../sync/merge-state').loadMergeState>>,
  strategy: 'ours' | 'theirs' | 'newest'
): Promise<number> {
  if (!state) return 1

  const { resolveConflict, saveMergeState, getUnresolvedConflicts } = await import(
    '../../sync/merge-state'
  )

  const unresolved = getUnresolvedConflicts(state)

  if (unresolved.length === 0) {
    print('No unresolved conflicts')
    return 0
  }

  print(`Resolving ${unresolved.length} conflict(s) using ${strategy}...`)
  print('')

  // Resolve each conflict
  let updatedState = state
  for (const conflict of unresolved) {
    updatedState = resolveConflict(updatedState, conflict.entityId, strategy)
    print(`  Resolved: ${conflict.entityId}`)
  }

  // Save updated state
  await saveMergeState(storage, updatedState)

  print('')
  printSuccess('All conflicts resolved!')
  print('')
  print('Continue the merge with:')
  print('  parquedb merge --continue')

  return 0
}

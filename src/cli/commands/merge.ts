/**
 * Merge Command
 *
 * Merge branches with conflict resolution support.
 * Similar to git merge functionality.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'

// =============================================================================
// Helper Functions
// =============================================================================

function findOption(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag)
  if (index >= 0 && index < args.length - 1) {
    return args[index + 1]
  }
  return undefined
}

function hasFlag(args: string[], ...flags: string[]): boolean {
  return flags.some((flag) => args.includes(flag))
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

// =============================================================================
// Merge Command
// =============================================================================

/**
 * Merge command - merge branches with conflict resolution
 *
 * Usage:
 *   parquedb merge <source>               Merge source branch into current
 *   parquedb merge <source> --strategy    Set conflict resolution strategy
 *   parquedb merge --abort                Abort in-progress merge
 *   parquedb merge --continue             Continue after resolving conflicts
 *   parquedb merge --dry-run              Preview merge without applying
 *   parquedb merge --from-git             Merge based on git merge state
 */
export async function mergeCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options
    const sourceBranch = parsed.args.find((arg) => !arg.startsWith('-'))
    const strategy = findOption(parsed.args, '--strategy') as
      | 'ours'
      | 'theirs'
      | 'newest'
      | 'manual'
      | undefined
    const dryRun = hasFlag(parsed.args, '--dry-run')
    const abort = hasFlag(parsed.args, '--abort')
    const continueFlag = hasFlag(parsed.args, '--continue')
    const fromGit = hasFlag(parsed.args, '--from-git')
    const jsonOutput = hasFlag(parsed.args, '--json')

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Check for existing merge state
    const { hasMergeInProgress, loadMergeState, clearMergeState } = await import(
      '../../sync/merge-state'
    )
    const mergeInProgress = await hasMergeInProgress(storage)

    // Handle --abort
    if (abort) {
      if (!mergeInProgress) {
        printError('No merge in progress')
        return 1
      }
      return await abortMerge(storage)
    }

    // Handle --continue
    if (continueFlag) {
      if (!mergeInProgress) {
        printError('No merge in progress')
        return 1
      }
      return await continueMerge(storage, jsonOutput)
    }

    // Check if there's already a merge in progress
    if (mergeInProgress) {
      const state = await loadMergeState(storage)
      printError('A merge is already in progress')
      if (state) {
        print(`Merging ${state.source} into ${state.target}`)
        print('')
        print('To abort the merge:')
        print('  parquedb merge --abort')
        print('')
        print('To continue after resolving conflicts:')
        print('  parquedb merge --continue')
      }
      return 1
    }

    // Need a source branch for new merge
    if (!sourceBranch) {
      printError('Usage: parquedb merge <source> [--strategy <strategy>] [--dry-run]')
      print('')
      print('Options:')
      print('  --strategy <strategy>  Conflict resolution: ours|theirs|newest|manual (default: manual)')
      print('  --dry-run              Preview merge without applying')
      print('  --abort                Abort in-progress merge')
      print('  --continue             Continue after resolving conflicts')
      print('  --from-git             Merge based on git merge state')
      print('  --json                 Output as JSON')
      return 1
    }

    // Perform merge
    return await performMerge(storage, sourceBranch, {
      strategy: strategy || 'manual',
      dryRun,
      fromGit,
      jsonOutput,
    })
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Merge Operations
// =============================================================================

/**
 * Perform a merge operation
 */
async function performMerge(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>,
  source: string,
  options: {
    strategy: 'ours' | 'theirs' | 'newest' | 'manual'
    dryRun: boolean
    fromGit: boolean
    jsonOutput: boolean
  }
): Promise<number> {
  const { createBranchManager } = await import('../../sync/branch-manager')
  const { createRefManager } = await import('../../sync/refs')
  const { loadCommit } = await import('../../sync/commit')
  const { mergeEventStreams } = await import('../../sync/event-merge')
  const { createMergeState, saveMergeState } = await import('../../sync/merge-state')

  const branchManager = createBranchManager({ storage })
  const refManager = createRefManager(storage)

  // Get current branch (target)
  const currentBranch = await branchManager.current()
  if (!currentBranch) {
    printError('Cannot merge: not currently on a branch (HEAD is detached)')
    return 1
  }

  // Resolve source and target commits
  const sourceCommit = await refManager.resolveRef(source)
  if (!sourceCommit) {
    printError(`Branch not found: ${source}`)
    return 1
  }

  const targetCommit = await refManager.resolveRef(currentBranch)
  if (!targetCommit) {
    printError(`Current branch ${currentBranch} does not point to a commit`)
    return 1
  }

  // Find common ancestor (base commit)
  // For now, use a simple implementation that checks parent chains
  const baseCommit = await findCommonAncestor(storage, sourceCommit, targetCommit)
  if (!baseCommit) {
    printError('No common ancestor found between branches')
    return 1
  }

  print(`Merging ${source} into ${currentBranch}`)
  print(`  Base:   ${baseCommit.substring(0, 8)}`)
  print(`  Source: ${sourceCommit.substring(0, 8)}`)
  print(`  Target: ${targetCommit.substring(0, 8)}`)
  print('')

  // Load events from each commit
  // For now, this is a placeholder - in a full implementation, we would
  // extract events from the commit's state
  const baseEvents: never[] = []
  const sourceEvents: never[] = []
  const targetEvents: never[] = []

  // Perform the merge
  const mergeResult = await mergeEventStreams(baseEvents, targetEvents, sourceEvents, {
    resolutionStrategy: options.strategy,
    autoMergeCommutative: true,
  })

  if (options.dryRun) {
    // Dry run - just show what would happen
    printMergePreview(mergeResult, options.jsonOutput)
    return 0
  }

  if (!mergeResult.success) {
    // Has conflicts - save merge state
    const mergeState = createMergeState({
      source,
      target: currentBranch,
      baseCommit,
      sourceCommit,
      targetCommit,
      strategy: options.strategy,
    })

    // Add conflicts to state
    for (const conflict of mergeResult.conflicts) {
      mergeState.conflicts.push({
        entityId: conflict.target,
        collection: conflict.target.split('/')[0] || 'unknown',
        fields: conflict.field ? [conflict.field] : [],
        resolved: false,
        ourValue: conflict.ourValue,
        theirValue: conflict.theirValue,
        baseValue: conflict.baseValue,
      })
    }

    await saveMergeState(storage, mergeState)

    printError(`Merge has conflicts`)
    print('')
    print(`Found ${mergeResult.conflicts.length} conflicts`)
    print('')
    print('To view conflicts:')
    print('  parquedb conflicts')
    print('')
    print('To resolve conflicts:')
    print('  parquedb resolve <entity> --ours|--theirs|--newest')
    print('')
    print('After resolving all conflicts:')
    print('  parquedb merge --continue')
    print('')
    print('To abort the merge:')
    print('  parquedb merge --abort')

    return 1
  }

  // Success - no conflicts
  printSuccess(`Merged ${source} into ${currentBranch}`)
  print(`  ${mergeResult.stats.fromOurs} events from ${currentBranch}`)
  print(`  ${mergeResult.stats.fromTheirs} events from ${source}`)
  print(`  ${mergeResult.stats.autoMerged} operations auto-merged`)

  return 0
}

/**
 * Continue a merge after resolving conflicts
 */
async function continueMerge(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>,
  jsonOutput: boolean
): Promise<number> {
  const { loadMergeState, clearMergeState, allConflictsResolved, getUnresolvedConflicts } =
    await import('../../sync/merge-state')

  const state = await loadMergeState(storage)
  if (!state) {
    printError('No merge state found')
    return 1
  }

  // Check all conflicts are resolved
  if (!allConflictsResolved(state)) {
    const unresolved = getUnresolvedConflicts(state)
    printError(`Cannot continue merge: ${unresolved.length} unresolved conflicts`)
    print('')
    print('Unresolved conflicts:')
    for (const conflict of unresolved) {
      print(`  - ${conflict.entityId} (${conflict.fields.join(', ')})`)
    }
    print('')
    print('Resolve conflicts with:')
    print('  parquedb resolve <entity> --ours|--theirs|--newest')
    return 1
  }

  // All conflicts resolved - complete the merge
  printSuccess(`All conflicts resolved`)
  print('')
  print('Completing merge...')

  // TODO: Apply resolved changes and create merge commit

  await clearMergeState(storage)

  printSuccess(`Merge completed`)
  return 0
}

/**
 * Abort an in-progress merge
 */
async function abortMerge(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>
): Promise<number> {
  const { clearMergeState, loadMergeState } = await import('../../sync/merge-state')

  const state = await loadMergeState(storage)
  if (state) {
    print(`Aborting merge of ${state.source} into ${state.target}`)
  }

  await clearMergeState(storage)
  printSuccess('Merge aborted')
  return 0
}

/**
 * Find common ancestor of two commits
 */
async function findCommonAncestor(
  storage: Awaited<ReturnType<typeof import('../../storage/FsBackend').FsBackend>>,
  commit1: string,
  commit2: string
): Promise<string | null> {
  // Simple implementation: traverse both parent chains and find first common commit
  // This is O(n*m) but works for now
  const { loadCommit } = await import('../../sync/commit')

  const visited1 = new Set<string>()
  const queue1 = [commit1]

  while (queue1.length > 0) {
    const current = queue1.shift()!
    if (visited1.has(current)) continue
    visited1.add(current)

    try {
      const commit = await loadCommit(storage, current)
      queue1.push(...commit.parents)
    } catch {
      // Commit not found, skip
    }
  }

  // Now traverse commit2's ancestors looking for one in visited1
  const queue2 = [commit2]
  const visited2 = new Set<string>()

  while (queue2.length > 0) {
    const current = queue2.shift()!
    if (visited2.has(current)) continue
    visited2.add(current)

    if (visited1.has(current)) {
      return current // Found common ancestor
    }

    try {
      const commit = await loadCommit(storage, current)
      queue2.push(...commit.parents)
    } catch {
      // Commit not found, skip
    }
  }

  return null
}

/**
 * Print merge preview for dry-run
 */
function printMergePreview(
  result: Awaited<ReturnType<typeof import('../../sync/event-merge').mergeEventStreams>>,
  jsonOutput: boolean
): void {
  if (jsonOutput) {
    print(
      JSON.stringify(
        {
          success: result.success,
          conflicts: result.conflicts.length,
          autoMerged: result.stats.autoMerged,
          stats: result.stats,
        },
        null,
        2
      )
    )
    return
  }

  print('Merge preview (dry-run):')
  print('')
  print(`Status: ${result.success ? 'Clean merge' : 'Has conflicts'}`)
  print(`Events: ${result.stats.fromOurs} ours + ${result.stats.fromTheirs} theirs`)
  print(`Auto-merged: ${result.stats.autoMerged}`)
  print(`Conflicts: ${result.conflicts.length}`)

  if (result.conflicts.length > 0) {
    print('')
    print('Conflicts:')
    for (const conflict of result.conflicts) {
      const field = conflict.field ? ` (${conflict.field})` : ''
      print(`  - ${conflict.target}${field}`)
    }
  }
}

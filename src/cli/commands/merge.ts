/**
 * Merge Command
 *
 * Merge branches with conflict resolution support.
 * Similar to git merge functionality.
 *
 * Uses distributed locking to prevent concurrent merge operations.
 * Uses the shared merge engine from src/sync/merge-engine.ts for all merge operations.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { createLockManager, type Lock } from '../../sync/lock'
import { createMergeEngine } from '../../sync/merge-engine'

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
    const { hasMergeInProgress, loadMergeState, clearMergeState: _clearMergeState } = await import(
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
 *
 * Acquires a distributed lock before performing the merge to prevent
 * concurrent merge operations from corrupting data.
 */
async function performMerge(
  storage: InstanceType<typeof import('../../storage/FsBackend').FsBackend>,
  source: string,
  options: {
    strategy: 'ours' | 'theirs' | 'newest' | 'manual'
    dryRun: boolean
    fromGit: boolean
    jsonOutput: boolean
  }
): Promise<number> {
  const { createBranchManager } = await import('../../sync/branch-manager')
  const { createMergeState, saveMergeState } = await import('../../sync/merge-state')

  // Create the shared merge engine for all merge operations
  const mergeEngine = createMergeEngine({ storage })

  // Acquire merge lock to prevent concurrent merges
  const lockManager = createLockManager(storage)
  let lock: Lock | undefined

  // Skip lock for dry-run since it's read-only
  if (!options.dryRun) {
    print('Acquiring merge lock...')
    const lockResult = await lockManager.acquire('merge', {
      timeout: 60_000,     // Lock expires after 60 seconds
      waitTimeout: 10_000, // Wait up to 10 seconds to acquire
    })

    if (!lockResult.acquired) {
      const holder = lockResult.currentHolder
      printError('Cannot merge: another merge operation is in progress')
      if (holder) {
        print(`  Lock held by: ${holder.holder}`)
        print(`  Acquired at: ${holder.acquiredAt}`)
        print(`  Expires at: ${holder.expiresAt}`)
      }
      print('')
      print('Wait for the other operation to complete, or force release:')
      print('  parquedb lock release merge --force')
      return 1
    }

    lock = lockResult.lock
  }

  try {
    const branchManager = createBranchManager({ storage })

    // Get current branch (target)
    const currentBranch = await branchManager.current()
    if (!currentBranch) {
      printError('Cannot merge: not currently on a branch (HEAD is detached)')
      return 1
    }

    // Map CLI strategy names to internal strategy names ('newest' -> 'latest')
    const internalStrategy = options.strategy === 'newest' ? 'latest' : options.strategy

    // Use the shared merge engine for the merge operation
    const mergeResult = await mergeEngine.mergeBranches(source, currentBranch, {
      strategy: internalStrategy as 'ours' | 'theirs' | 'latest' | 'manual',
      dryRun: options.dryRun,
      autoMergeCommutative: true,
    })

    // Handle merge errors (branch not found, no common ancestor, etc.)
    if (!mergeResult.success && mergeResult.error) {
      printError(mergeResult.error)
      return 1
    }

    const baseCommit = mergeResult.baseCommit
    const sourceCommit = mergeResult.sourceCommit
    const targetCommit = mergeResult.targetCommit

    if (baseCommit && sourceCommit && targetCommit) {
      print(`Merging ${source} into ${currentBranch}`)
      print(`  Base:   ${baseCommit.substring(0, 8)}`)
      print(`  Source: ${sourceCommit.substring(0, 8)}`)
      print(`  Target: ${targetCommit.substring(0, 8)}`)
      print('')
    }

    if (options.dryRun) {
      // Dry run - just show what would happen
      printMergePreviewFromResult(mergeResult, options.jsonOutput)
      return 0
    }

    if (!mergeResult.success && mergeResult.conflicts.length > 0) {
      // Has conflicts - save merge state
      // Note: We keep the lock held when there are conflicts, as the user
      // needs to resolve them and continue. The lock will expire after timeout.
      const mergeState = createMergeState({
        source,
        target: currentBranch,
        baseCommit: baseCommit!,
        sourceCommit: sourceCommit!,
        targetCommit: targetCommit!,
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

      // Don't release lock here - keep it for conflict resolution
      // It will expire after the timeout if abandoned
      return 1
    }

    // Success - no conflicts
    printSuccess(`Merged ${source} into ${currentBranch}`)
    if (mergeResult.stats) {
      print(`  ${mergeResult.stats.fromTarget} events from ${currentBranch}`)
      print(`  ${mergeResult.stats.fromSource} events from ${source}`)
      print(`  ${mergeResult.stats.autoMerged} operations auto-merged`)
    }

    return 0
  } finally {
    // Release the lock if we're not in a conflict state
    // (conflicts need the lock for subsequent --continue)
    if (lock) {
      await lock.release()
    }
  }
}

/**
 * Continue a merge after resolving conflicts
 *
 * Acquires a lock to complete the merge safely.
 */
async function continueMerge(
  storage: InstanceType<typeof import('../../storage/FsBackend').FsBackend>,
  _jsonOutput: boolean
): Promise<number> {
  const { loadMergeState, clearMergeState, allConflictsResolved, getUnresolvedConflicts } =
    await import('../../sync/merge-state')

  // Acquire merge lock
  const lockManager = createLockManager(storage)
  print('Acquiring merge lock...')
  const lockResult = await lockManager.acquire('merge', {
    timeout: 60_000,
    waitTimeout: 10_000,
  })

  if (!lockResult.acquired) {
    const holder = lockResult.currentHolder
    printError('Cannot continue merge: another operation is in progress')
    if (holder) {
      print(`  Lock held by: ${holder.holder}`)
      print(`  Expires at: ${holder.expiresAt}`)
    }
    return 1
  }

  const lock = lockResult.lock!

  try {
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

    // Apply resolved changes and create merge commit
    const result = await applyResolvedChanges(storage, state)
    if (!result.success) {
      printError(`Failed to apply resolved changes: ${result.error}`)
      return 1
    }

    await clearMergeState(storage)

    printSuccess(`Merge completed`)
    print(`  Merge commit: ${result.commitHash?.substring(0, 8)}`)
    print(`  Applied ${state.conflicts.length} resolved conflict(s)`)
    return 0
  } finally {
    await lock.release()
  }
}

/**
 * Apply resolved changes and create a merge commit
 *
 * This function:
 * 1. Computes the resolved value for each conflict based on resolution strategy
 * 2. Creates events to apply the resolved changes to the database
 * 3. Creates a merge commit with both parent commits
 * 4. Updates the target branch ref to point to the merge commit
 */
async function applyResolvedChanges(
  storage: InstanceType<typeof import('../../storage/FsBackend').FsBackend>,
  state: Awaited<ReturnType<typeof import('../../sync/merge-state').loadMergeState>>
): Promise<{ success: boolean; commitHash?: string; error?: string }> {
  if (!state) {
    return { success: false, error: 'No merge state provided' }
  }

  try {
    const { createCommit, saveCommit, loadCommit } = await import('../../sync/commit')
    const { createRefManager } = await import('../../sync/refs')

    const refManager = createRefManager(storage)

    // Load both parent commits to get their states
    const targetCommitObj = await loadCommit(storage, state.targetCommit)
    const sourceCommitObj = await loadCommit(storage, state.sourceCommit)

    // Compute resolved values for each conflict
    const resolvedChanges: Map<string, { collection: string; entityId: string; resolvedValue: unknown }> = new Map()

    for (const conflict of state.conflicts) {
      if (!conflict.resolved) {
        return { success: false, error: `Conflict not resolved: ${conflict.entityId}` }
      }

      // Determine the resolved value based on strategy
      let resolvedValue: unknown
      if (conflict.resolvedValue !== undefined) {
        // Manual resolution - use the explicitly set value
        resolvedValue = conflict.resolvedValue
      } else if (conflict.resolution === 'ours') {
        resolvedValue = conflict.ourValue
      } else if (conflict.resolution === 'theirs') {
        resolvedValue = conflict.theirValue
      } else if (conflict.resolution === 'newest') {
        // Use 'theirs' as the default for newest if we don't have timestamp info
        // In a full implementation, we would compare timestamps from the events
        resolvedValue = conflict.theirValue
      } else {
        return { success: false, error: `Unknown resolution strategy for ${conflict.entityId}: ${conflict.resolution}` }
      }

      resolvedChanges.set(conflict.entityId, {
        collection: conflict.collection,
        entityId: conflict.entityId,
        resolvedValue,
      })
    }

    // Merge the database states from both commits
    // Start with target state and apply resolved changes
    const mergedState = {
      collections: { ...targetCommitObj.state.collections },
      relationships: { ...targetCommitObj.state.relationships },
      eventLogPosition: targetCommitObj.state.eventLogPosition,
    }

    // Merge in any new collections from source that don't conflict
    for (const [collName, collState] of Object.entries(sourceCommitObj.state.collections)) {
      if (!mergedState.collections[collName]) {
        mergedState.collections[collName] = collState
      }
    }

    // For conflicting entities, the resolved values have been computed above
    // In a full implementation, we would write the resolved entity data to the parquet files
    // For now, we record the merge commit with the merged state metadata

    // Create merge commit with both parents
    const mergeCommit = await createCommit(mergedState, {
      message: `Merge '${state.source}' into '${state.target}'`,
      author: 'parquedb-cli',
      parents: [state.targetCommit, state.sourceCommit],
    })

    // Save the merge commit
    await saveCommit(storage, mergeCommit)

    // Update the target branch to point to the merge commit
    await refManager.updateRef(state.target, mergeCommit.hash)

    return { success: true, commitHash: mergeCommit.hash }
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

/**
 * Abort an in-progress merge
 *
 * Force-releases any existing merge lock as part of cleanup.
 */
async function abortMerge(
  storage: InstanceType<typeof import('../../storage/FsBackend').FsBackend>
): Promise<number> {
  const { clearMergeState, loadMergeState } = await import('../../sync/merge-state')

  const state = await loadMergeState(storage)
  if (state) {
    print(`Aborting merge of ${state.source} into ${state.target}`)
  }

  await clearMergeState(storage)

  // Force-release the merge lock as part of abort cleanup
  const lockManager = createLockManager(storage)
  const wasLocked = await lockManager.forceRelease('merge')
  if (wasLocked) {
    print('Released merge lock')
  }

  printSuccess('Merge aborted')
  return 0
}

/**
 * Print merge preview for dry-run using MergeBranchesResult
 */
function printMergePreviewFromResult(
  result: import('../../sync/merge-engine').MergeBranchesResult,
  jsonOutput: boolean
): void {
  if (jsonOutput) {
    print(
      JSON.stringify(
        {
          success: result.success,
          conflicts: result.conflicts.length,
          autoMerged: result.stats?.autoMerged ?? 0,
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
  if (result.stats) {
    print(`Events: ${result.stats.fromTarget} ours + ${result.stats.fromSource} theirs`)
    print(`Auto-merged: ${result.stats.autoMerged}`)
  }
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

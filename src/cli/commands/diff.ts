/**
 * Diff Command
 *
 * Show changes between branches or commits.
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
// Diff Command
// =============================================================================

/**
 * Diff command - show changes between branches or commits
 *
 * Usage:
 *   parquedb diff [target]        Show changes between current and target
 *   parquedb diff --stat           Show summary only
 *   parquedb diff --events         Show event-level diff
 *   parquedb diff --json           Output as JSON
 */
export async function diffCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options
    const target = parsed.args.find((arg) => !arg.startsWith('-'))
    const stat = hasFlag(parsed.args, '--stat')
    const events = hasFlag(parsed.args, '--events')
    const jsonOutput = hasFlag(parsed.args, '--json')

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Create managers
    const { createBranchManager } = await import('../../sync/branch-manager')
    const { createRefManager } = await import('../../sync/refs')
    const branchManager = createBranchManager({ storage })
    const refManager = createRefManager(storage)

    // Get current branch
    const currentBranch = await branchManager.current()
    if (!currentBranch) {
      printError('Cannot diff: not currently on a branch (HEAD is detached)')
      return 1
    }

    // Resolve target (default to 'main' if not specified)
    const targetRef = target || 'main'
    const currentCommit = await refManager.resolveRef(currentBranch)
    const targetCommit = await refManager.resolveRef(targetRef)

    if (!currentCommit) {
      printError(`Current branch ${currentBranch} does not point to a commit`)
      return 1
    }

    if (!targetCommit) {
      printError(`Target not found: ${targetRef}`)
      return 1
    }

    // Check if they're the same
    if (currentCommit === targetCommit) {
      print(`No differences between ${currentBranch} and ${targetRef}`)
      return 0
    }

    // Load commits
    const { loadCommit } = await import('../../sync/commit')
    const current = await loadCommit(storage, currentCommit)
    const targetBranchCommit = await loadCommit(storage, targetCommit)

    // Show diff
    if (jsonOutput) {
      return showDiffJson(current, targetBranchCommit, events)
    } else if (stat) {
      return showDiffStat(current, targetBranchCommit, currentBranch, targetRef)
    } else {
      return showDiffDetailed(current, targetBranchCommit, currentBranch, targetRef, events)
    }
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Diff Display Functions
// =============================================================================

/**
 * Show detailed diff
 */
function showDiffDetailed(
  current: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  target: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  currentBranch: string,
  targetRef: string,
  showEvents: boolean
): number {
  print(`Comparing ${currentBranch} with ${targetRef}`)
  print(`  Current: ${current.hash.substring(0, 8)} - ${current.message}`)
  print(`  Target:  ${target.hash.substring(0, 8)} - ${target.message}`)
  print('')

  // Compare collections
  const currentCollections = Object.keys(current.state.collections)
  const targetCollections = Object.keys(target.state.collections)

  const allCollections = new Set([...currentCollections, ...targetCollections])
  let hasChanges = false

  for (const ns of allCollections) {
    const currentCol = current.state.collections[ns]
    const targetCol = target.state.collections[ns]

    if (!targetCol) {
      print(`+ ${ns} (new collection)`)
      print(`    ${currentCol.rowCount} rows`)
      hasChanges = true
    } else if (!currentCol) {
      print(`- ${ns} (removed)`)
      hasChanges = true
    } else {
      // Compare
      const rowDiff = currentCol.rowCount - targetCol.rowCount
      const dataChanged = currentCol.dataHash !== targetCol.dataHash
      const schemaChanged = currentCol.schemaHash !== targetCol.schemaHash

      if (dataChanged || schemaChanged || rowDiff !== 0) {
        print(`M ${ns}`)
        if (rowDiff !== 0) {
          const sign = rowDiff > 0 ? '+' : ''
          print(`    ${sign}${rowDiff} rows (${targetCol.rowCount} -> ${currentCol.rowCount})`)
        }
        if (dataChanged) {
          print(`    Data: ${targetCol.dataHash.substring(0, 8)} -> ${currentCol.dataHash.substring(0, 8)}`)
        }
        if (schemaChanged) {
          print(`    Schema changed`)
        }
        hasChanges = true
      }
    }
  }

  // Compare relationships
  const relsChanged =
    current.state.relationships.forwardHash !== target.state.relationships.forwardHash ||
    current.state.relationships.reverseHash !== target.state.relationships.reverseHash

  if (relsChanged) {
    print('')
    print('M Relationships')
    print(`    Forward: ${target.state.relationships.forwardHash.substring(0, 8)} -> ${current.state.relationships.forwardHash.substring(0, 8)}`)
    print(`    Reverse: ${target.state.relationships.reverseHash.substring(0, 8)} -> ${current.state.relationships.reverseHash.substring(0, 8)}`)
    hasChanges = true
  }

  if (!hasChanges) {
    print('No differences found')
  }

  if (showEvents) {
    print('')
    print('Event log position:')
    print(`  Current: ${current.state.eventLogPosition.segmentId}:${current.state.eventLogPosition.offset}`)
    print(`  Target:  ${target.state.eventLogPosition.segmentId}:${target.state.eventLogPosition.offset}`)
  }

  return 0
}

/**
 * Show diff statistics only
 */
function showDiffStat(
  current: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  target: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  currentBranch: string,
  targetRef: string
): number {
  print(`${currentBranch}...${targetRef}`)
  print('')

  const currentCollections = Object.keys(current.state.collections)
  const targetCollections = Object.keys(target.state.collections)
  const allCollections = new Set([...currentCollections, ...targetCollections])

  let added = 0
  let removed = 0
  let modified = 0

  for (const ns of allCollections) {
    const currentCol = current.state.collections[ns]
    const targetCol = target.state.collections[ns]

    if (!targetCol) {
      added++
    } else if (!currentCol) {
      removed++
    } else if (
      currentCol.dataHash !== targetCol.dataHash ||
      currentCol.schemaHash !== targetCol.schemaHash
    ) {
      modified++
    }
  }

  print(`${added} collection(s) added`)
  print(`${removed} collection(s) removed`)
  print(`${modified} collection(s) modified`)

  const relsChanged =
    current.state.relationships.forwardHash !== target.state.relationships.forwardHash ||
    current.state.relationships.reverseHash !== target.state.relationships.reverseHash

  if (relsChanged) {
    print(`Relationships modified`)
  }

  return 0
}

/**
 * Show diff as JSON
 */
function showDiffJson(
  current: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  target: Awaited<ReturnType<typeof import('../../sync/commit').loadCommit>>,
  showEvents: boolean
): number {
  const currentCollections = Object.keys(current.state.collections)
  const targetCollections = Object.keys(target.state.collections)
  const allCollections = new Set([...currentCollections, ...targetCollections])

  const collections: Record<string, unknown> = {}

  for (const ns of allCollections) {
    const currentCol = current.state.collections[ns]
    const targetCol = target.state.collections[ns]

    if (!targetCol) {
      collections[ns] = { status: 'added', rows: currentCol.rowCount }
    } else if (!currentCol) {
      collections[ns] = { status: 'removed' }
    } else {
      collections[ns] = {
        status: 'modified',
        rowDiff: currentCol.rowCount - targetCol.rowCount,
        dataChanged: currentCol.dataHash !== targetCol.dataHash,
        schemaChanged: currentCol.schemaHash !== targetCol.schemaHash,
      }
    }
  }

  const result: Record<string, unknown> = {
    current: {
      hash: current.hash,
      message: current.message,
    },
    target: {
      hash: target.hash,
      message: target.message,
    },
    collections,
    relationships: {
      changed:
        current.state.relationships.forwardHash !== target.state.relationships.forwardHash ||
        current.state.relationships.reverseHash !== target.state.relationships.reverseHash,
    },
  }

  if (showEvents) {
    result.eventLogPosition = {
      current: current.state.eventLogPosition,
      target: target.state.eventLogPosition,
    }
  }

  print(JSON.stringify(result, null, 2))
  return 0
}

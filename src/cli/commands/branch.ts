/**
 * Branch Command
 *
 * List, create, or delete database branches.
 * Similar to git branch functionality.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Find an option value in args
 */
function findOption(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag)
  if (index >= 0 && index < args.length - 1) {
    return args[index + 1]
  }
  return undefined
}

/**
 * Check if flag is present in args
 */
function hasFlag(args: string[], ...flags: string[]): boolean {
  return flags.some(flag => args.includes(flag))
}

/**
 * Get error message from unknown error
 */
function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

// =============================================================================
// Branch Command
// =============================================================================

/**
 * Branch command - list, create, rename, or delete branches
 *
 * Usage:
 *   parquedb branch                      List all branches
 *   parquedb branch <name>               Create a new branch
 *   parquedb branch <name> <base>        Create a new branch from base
 *   parquedb branch -d <name>            Delete a branch
 *   parquedb branch -m <old> <new>       Rename a branch
 *   parquedb branch --list               List all branches
 */
export async function branchCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options and arguments
    const deleteFlag = findOption(parsed.args, '-d') || findOption(parsed.args, '--delete')
    const renameOld = findOption(parsed.args, '-m') || findOption(parsed.args, '--move')
    // listFlag is implicitly used (when no other action, we list branches)
    const allFlag = hasFlag(parsed.args, '-a', '--all')
    const remoteFlag = hasFlag(parsed.args, '-r', '--remote')
    const forceFlag = hasFlag(parsed.args, '--force')

    // Find positional arguments (not flags)
    const positionalArgs = parsed.args.filter(
      arg => !arg.startsWith('-') && arg !== deleteFlag && arg !== renameOld
    )

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Create branch manager
    const { createBranchManager } = await import('../../sync/branch-manager')
    const branchManager = createBranchManager({ storage })

    // Handle delete operation
    if (deleteFlag) {
      return await deleteBranch(branchManager, deleteFlag, forceFlag)
    }

    // Handle rename operation
    if (renameOld) {
      const newName = positionalArgs[0]
      if (!newName) {
        printError('Usage: parquedb branch -m <old-name> <new-name>')
        return 1
      }
      return await renameBranch(branchManager, renameOld, newName)
    }

    // Handle create operation
    if (positionalArgs.length > 0) {
      const name = positionalArgs[0]!
      const base = positionalArgs[1]
      return await createBranch(branchManager, name, base)
    }

    // Default: list branches
    return await listBranches(branchManager, { all: allFlag, remote: remoteFlag })
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Operation Handlers
// =============================================================================

/**
 * List all branches
 */
async function listBranches(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  options: { all?: boolean | undefined; remote?: boolean | undefined }
): Promise<number> {
  const branches = await branchManager.list()

  if (branches.length === 0) {
    print('No branches found.')
    print('')
    print('Create a branch with: parquedb branch <name>')
    return 0
  }

  // Sort branches: current first, then alphabetically
  branches.sort((a, b) => {
    if (a.isCurrent && !b.isCurrent) return -1
    if (!a.isCurrent && b.isCurrent) return 1
    return a.name.localeCompare(b.name)
  })

  // Filter based on options
  const filtered = branches.filter(branch => {
    if (options.remote) return branch.isRemote
    if (options.all) return true
    return !branch.isRemote
  })

  print('')
  for (const branch of filtered) {
    const marker = branch.isCurrent ? '* ' : '  '
    const remoteSuffix = branch.isRemote ? ' (remote)' : ''
    const shortCommit = branch.commit.substring(0, 8)
    print(`${marker}${branch.name}${remoteSuffix} [${shortCommit}]`)
  }
  print('')

  return 0
}

/**
 * Create a new branch
 */
async function createBranch(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  name: string,
  base?: string
): Promise<number> {
  try {
    await branchManager.create(name, base ? { from: base } : undefined)

    const baseMsg = base ? ` from ${base}` : ''
    printSuccess(`Created branch '${name}'${baseMsg}`)
    print('')
    print('Switch to the new branch with:')
    print(`  parquedb checkout ${name}`)
    print('')

    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

/**
 * Delete a branch
 */
async function deleteBranch(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  name: string,
  force: boolean
): Promise<number> {
  try {
    await branchManager.delete(name, { force })
    printSuccess(`Deleted branch '${name}'`)
    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    if (!force && error instanceof Error && error.message.includes('unmerged')) {
      print('')
      print('Use --force to delete the branch anyway:')
      print(`  parquedb branch -d ${name} --force`)
    }
    return 1
  }
}

/**
 * Rename a branch
 */
async function renameBranch(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  oldName: string,
  newName: string
): Promise<number> {
  try {
    await branchManager.rename(oldName, newName)
    printSuccess(`Renamed branch '${oldName}' to '${newName}'`)
    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

/**
 * Checkout Command
 *
 * Switch branches or restore database state to a specific commit.
 * Similar to git checkout functionality.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import { execSync } from 'child_process'

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

/**
 * Get current git branch name
 */
function getCurrentGitBranch(): string | null {
  try {
    const branch = execSync('git rev-parse --abbrev-ref HEAD', {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'ignore'],
    }).trim()

    // git returns 'HEAD' if in detached state
    return branch === 'HEAD' ? null : branch
  } catch {
    // Not a git repo or git not installed
    return null
  }
}

// =============================================================================
// Checkout Command
// =============================================================================

/**
 * Checkout command - switch branches or restore files
 *
 * Usage:
 *   parquedb checkout <branch>           Switch to a branch
 *   parquedb checkout -b <branch>        Create and switch to new branch
 *   parquedb checkout --from-git         Sync to current git branch
 *   parquedb checkout -f <branch>        Force checkout, discard changes
 */
export async function checkoutCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options and arguments
    const createBranch = findOption(parsed.args, '-b') || findOption(parsed.args, '--create')
    const fromGit = hasFlag(parsed.args, '--from-git')
    const force = hasFlag(parsed.args, '-f', '--force')

    // Find positional arguments (not flags)
    const positionalArgs = parsed.args.filter(
      arg => !arg.startsWith('-') && arg !== createBranch
    )

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Create branch manager
    const { createBranchManager } = await import('../../sync/branch-manager')
    const branchManager = createBranchManager({ storage })

    // Handle --from-git flag
    if (fromGit) {
      return await checkoutFromGit(branchManager, force)
    }

    // Handle -b (create and checkout)
    if (createBranch) {
      return await createAndCheckout(branchManager, createBranch)
    }

    // Get branch/commit name from positional args
    const target = positionalArgs[0]
    if (!target) {
      printError('Usage: parquedb checkout <branch>')
      print('')
      print('Options:')
      print('  -b, --create <branch>    Create and switch to new branch')
      print('  --from-git               Sync to current git branch')
      print('  -f, --force              Discard uncommitted changes')
      return 1
    }

    return await checkoutBranch(branchManager, target, force)
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Operation Handlers
// =============================================================================

/**
 * Checkout a branch or commit
 */
async function checkoutBranch(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  target: string,
  force: boolean
): Promise<number> {
  try {
    await branchManager.checkout(target, { force })

    printSuccess(`Switched to branch '${target}'`)
    print('')
    print('Database state has been updated to match this branch.')
    print('')

    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    const errorMessage = error instanceof Error ? error.message : String(error)

    if (errorMessage.includes('uncommitted changes')) {
      print('')
      print('Options:')
      print(`  parquedb checkout -f ${target}  # Force checkout, discard changes`)
      print('  parquedb commit                 # Commit changes first')
    } else if (errorMessage.includes('not found')) {
      print('')
      print('Did you mean to create a new branch?')
      print(`  parquedb checkout -b ${target}`)
    }
    return 1
  }
}

/**
 * Create a new branch and check it out
 */
async function createAndCheckout(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  name: string
): Promise<number> {
  try {
    await branchManager.checkout(name, { create: true })

    printSuccess(`Created and switched to branch '${name}'`)
    print('')

    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

/**
 * Checkout branch matching current git branch
 */
async function checkoutFromGit(
  branchManager: Awaited<ReturnType<typeof import('../../sync/branch-manager').createBranchManager>>,
  force: boolean
): Promise<number> {
  try {
    // Get current git branch
    const gitBranch = getCurrentGitBranch()
    if (!gitBranch) {
      printError('Not in a git repository or in detached HEAD state')
      print('')
      print('The --from-git flag requires you to be on a git branch.')
      return 1
    }

    print(`Current git branch: ${gitBranch}`)
    print('')

    // Check if ParqueDB branch exists
    const exists = await branchManager.exists(gitBranch)

    if (exists) {
      // Switch to existing branch
      await branchManager.checkout(gitBranch, { force })
      printSuccess(`Switched to existing branch '${gitBranch}'`)
    } else {
      // Create branch from current HEAD
      print(`Branch '${gitBranch}' does not exist. Creating...`)
      await branchManager.checkout(gitBranch, { create: true, force })
      printSuccess(`Created and switched to branch '${gitBranch}'`)
    }

    print('')
    print('ParqueDB branch now matches git branch.')
    print('')

    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

/**
 * Log Command
 *
 * Show commit history for a branch.
 * Similar to git log functionality.
 */

import type { ParsedArgs } from '../types'
import { print, printError } from '../types'

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
 * Format a timestamp as a relative time string
 */
function formatRelativeTime(timestamp: number): string {
  const now = Date.now()
  const diff = now - timestamp
  const seconds = Math.floor(diff / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  const weeks = Math.floor(days / 7)
  const months = Math.floor(days / 30)
  const years = Math.floor(days / 365)

  if (years > 0) return `${years} year${years > 1 ? 's' : ''} ago`
  if (months > 0) return `${months} month${months > 1 ? 's' : ''} ago`
  if (weeks > 0) return `${weeks} week${weeks > 1 ? 's' : ''} ago`
  if (days > 0) return `${days} day${days > 1 ? 's' : ''} ago`
  if (hours > 0) return `${hours} hour${hours > 1 ? 's' : ''} ago`
  if (minutes > 0) return `${minutes} minute${minutes > 1 ? 's' : ''} ago`
  return `${seconds} second${seconds !== 1 ? 's' : ''} ago`
}

/**
 * Format a date for display
 */
function formatDate(timestamp: number): string {
  const date = new Date(timestamp)
  return date.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC')
}

// =============================================================================
// Log Command
// =============================================================================

/**
 * Log command - show commit history
 *
 * Usage:
 *   parquedb log                         Show history for current branch
 *   parquedb log <branch>                Show history for a specific branch
 *   parquedb log --oneline               Compact format
 *   parquedb log -n 10                   Limit to 10 commits
 *   parquedb log --graph                 Show ASCII graph (TODO)
 */
export async function logCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Parse options and arguments
    const oneline = hasFlag(parsed.args, '--oneline')
    const graph = hasFlag(parsed.args, '--graph')
    const maxCountStr = findOption(parsed.args, '-n') || findOption(parsed.args, '--max-count')
    const maxCount = maxCountStr ? parseInt(maxCountStr, 10) : undefined

    // Validate max count
    if (maxCountStr && (isNaN(maxCount!) || maxCount! < 1)) {
      printError(`Invalid max count: ${maxCountStr}`)
      return 1
    }

    // Find positional arguments (not flags)
    const positionalArgs = parsed.args.filter(
      arg => !arg.startsWith('-') && arg !== maxCountStr
    )

    // Get branch to show history for (default to current)
    const branch = positionalArgs[0]

    // Create storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const storage = new FsBackend(parsed.options.directory)

    // Create ref manager
    const { createRefManager } = await import('../../sync/refs')
    const refManager = createRefManager(storage)

    // Resolve branch to commit hash
    const startCommit = branch
      ? await refManager.resolveRef(branch)
      : await refManager.resolveRef('HEAD')

    if (!startCommit) {
      printError(branch ? `Branch not found: ${branch}` : 'No commits yet (HEAD is empty)')
      print('')
      print('Create your first commit with:')
      print('  parquedb commit -m "Initial commit"')
      return 1
    }

    // Load commit history
    const { loadCommit } = await import('../../sync/commit')
    const commits: Array<Awaited<ReturnType<typeof loadCommit>>> = []
    const visited = new Set<string>()
    const queue = [startCommit]

    // BFS traversal to get commit history
    while (queue.length > 0 && (!maxCount || commits.length < maxCount)) {
      const commitHash = queue.shift()!

      // Skip if already visited
      if (visited.has(commitHash)) continue
      visited.add(commitHash)

      try {
        const commit = await loadCommit(storage, commitHash)
        commits.push(commit)

        // Add parents to queue (oldest first for chronological order)
        queue.push(...commit.parents.reverse())
      } catch (error) {
        // Commit not found - this might be OK if it's a dangling reference
        // Just skip it and continue
        continue
      }
    }

    if (commits.length === 0) {
      print('No commits found.')
      return 0
    }

    // Display commits
    print('')
    if (oneline) {
      displayOneline(commits)
    } else if (graph) {
      // TODO: Implement graph display
      print('Graph display not yet implemented. Showing oneline format:')
      print('')
      displayOneline(commits)
    } else {
      displayFull(commits)
    }
    print('')

    return 0
  } catch (error) {
    printError(getErrorMessage(error))
    return 1
  }
}

// =============================================================================
// Display Functions
// =============================================================================

/**
 * Display commits in oneline format
 */
function displayOneline(
  commits: Array<{ hash: string; timestamp: number; author: string; message: string }>
): void {
  for (const commit of commits) {
    const shortHash = commit.hash.substring(0, 8)
    const firstLine = commit.message.split('\n')[0] ?? ''
    const truncated = firstLine.length > 60 ? firstLine.substring(0, 57) + '...' : firstLine
    print(`${shortHash} ${truncated}`)
  }
}

/**
 * Display commits in full format
 */
function displayFull(
  commits: Array<{
    hash: string
    parents: string[]
    timestamp: number
    author: string
    message: string
  }>
): void {
  for (let i = 0; i < commits.length; i++) {
    const commit = commits[i]
    if (!commit) continue

    // shortHash computed but not displayed in full format (only used in header)
    const relativeTime = formatRelativeTime(commit.timestamp)
    const absoluteTime = formatDate(commit.timestamp)

    print(`commit ${commit.hash}`)

    if (commit.parents.length > 0) {
      const parentHashes = commit.parents.map(p => p.substring(0, 8)).join(' ')
      print(`Parents: ${parentHashes}`)
    }

    print(`Author: ${commit.author}`)
    print(`Date:   ${absoluteTime} (${relativeTime})`)
    print('')

    // Indent commit message
    const lines = commit.message.split('\n')
    for (const line of lines) {
      print(`    ${line}`)
    }

    // Add separator between commits (except after last one)
    if (i < commits.length - 1) {
      print('')
    }
  }
}

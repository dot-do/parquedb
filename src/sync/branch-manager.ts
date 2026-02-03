/**
 * Branch Manager
 *
 * Manages database branches for version control operations.
 * Similar to git branches, allows creating, switching, listing, and deleting branches.
 */

import { RefManager, createRefManager } from './refs'
import { DatabaseCommit, loadCommit } from './commit'
import type { StorageBackend } from '../types/storage'

// =============================================================================
// Types
// =============================================================================

/**
 * Information about a branch
 */
export interface BranchInfo {
  /** Branch name (e.g., 'main', 'feature/new-schema') */
  name: string
  /** Commit hash the branch points to */
  commit: string
  /** Whether this is the current branch (HEAD points to it) */
  isCurrent: boolean
  /** Whether this is a remote tracking branch */
  isRemote: boolean
}

/**
 * Options for creating BranchManager
 */
export interface BranchManagerOptions {
  /** Storage backend to use */
  storage: StorageBackend
}

/**
 * Options for checkout operation
 */
export interface CheckoutOptions {
  /** Create branch if it doesn't exist */
  create?: boolean
}

/**
 * Options for delete operation
 */
export interface DeleteOptions {
  /** Force delete even if branch has unmerged changes */
  force?: boolean
}

/**
 * Options for create operation
 */
export interface CreateOptions {
  /** Base branch or commit to create from (defaults to current HEAD) */
  from?: string
}

// =============================================================================
// Branch Manager
// =============================================================================

/**
 * Manages database branches
 */
export class BranchManager {
  private refs: RefManager

  constructor(private opts: BranchManagerOptions) {
    this.refs = createRefManager(opts.storage)
  }

  /**
   * Get current branch name, or null if HEAD is detached
   */
  async current(): Promise<string | null> {
    const headState = await this.refs.getHead()
    return headState.type === 'branch' ? headState.ref : null
  }

  /**
   * List all branches
   * @returns Array of branch information
   */
  async list(): Promise<BranchInfo[]> {
    const branches: BranchInfo[] = []

    // Get current HEAD state
    const headState = await this.refs.getHead()
    const currentBranch = headState.type === 'branch' ? headState.ref : null

    // List all local branches (refs/heads/*)
    const localRefs = await this.refs.listRefs('heads')

    for (const ref of localRefs) {
      // Extract branch name from refs/heads/name
      const name = ref.substring('refs/heads/'.length)
      const commit = await this.refs.resolveRef(ref)

      if (commit) {
        branches.push({
          name,
          commit,
          isCurrent: name === currentBranch,
          isRemote: false,
        })
      }
    }

    return branches
  }

  /**
   * Create a new branch
   * @param name Branch name to create
   * @param opts Options including base commit/branch
   */
  async create(name: string, opts?: CreateOptions): Promise<void> {
    // Validate branch name
    if (!this.isValidBranchName(name)) {
      throw new Error(
        `Invalid branch name: ${name}. Branch names must be alphanumeric with hyphens, slashes, or underscores.`
      )
    }

    // Check if branch already exists
    const exists = await this.exists(name)
    if (exists) {
      throw new Error(`Branch already exists: ${name}`)
    }

    // Resolve base commit
    let baseCommit: string | null
    if (opts?.from) {
      // Try to resolve as a ref first
      baseCommit = await this.refs.resolveRef(opts.from)

      // If not found, check if it's a raw commit hash
      if (!baseCommit) {
        // Verify the commit exists in storage
        try {
          await loadCommit(this.opts.storage, opts.from)
          baseCommit = opts.from // Use the hash directly
        } catch {
          throw new Error(`Cannot create branch: base ref not found: ${opts.from}`)
        }
      }
    } else {
      // Use current HEAD
      baseCommit = await this.refs.resolveRef('HEAD')
      if (!baseCommit) {
        throw new Error('Cannot create branch: HEAD does not point to any commit')
      }
    }

    // Create the branch by updating the ref
    await this.refs.updateRef(name, baseCommit)
  }

  /**
   * Delete a branch
   * @param name Branch name to delete
   * @param opts Options including force flag
   */
  async delete(name: string, opts?: DeleteOptions): Promise<void> {
    // Check if branch exists
    const exists = await this.exists(name)
    if (!exists) {
      throw new Error(`Branch not found: ${name}`)
    }

    // Don't allow deleting current branch
    const currentBranch = await this.current()
    if (currentBranch === name) {
      throw new Error(`Cannot delete current branch: ${name}. Switch to another branch first.`)
    }

    // In a full implementation, we would check if the branch has unmerged changes
    // and require --force if it does. For now, we'll skip that check.
    if (!opts?.force) {
      // TODO: Check for unmerged commits
      // For now, allow deletion without force
    }

    // Delete the branch ref
    await this.refs.deleteRef(name)
  }

  /**
   * Rename a branch
   * @param oldName Current branch name
   * @param newName New branch name
   */
  async rename(oldName: string, newName: string): Promise<void> {
    // Validate new branch name
    if (!this.isValidBranchName(newName)) {
      throw new Error(
        `Invalid branch name: ${newName}. Branch names must be alphanumeric with hyphens, slashes, or underscores.`
      )
    }

    // Check if old branch exists
    const exists = await this.exists(oldName)
    if (!exists) {
      throw new Error(`Branch not found: ${oldName}`)
    }

    // Check if new branch already exists
    const newExists = await this.exists(newName)
    if (newExists) {
      throw new Error(`Branch already exists: ${newName}`)
    }

    // Get commit hash from old branch
    const commit = await this.refs.resolveRef(oldName)
    if (!commit) {
      throw new Error(`Cannot rename branch: ${oldName} does not point to a commit`)
    }

    // Create new branch pointing to same commit
    await this.refs.updateRef(newName, commit)

    // Update HEAD if we're renaming the current branch
    const currentBranch = await this.current()
    if (currentBranch === oldName) {
      await this.refs.setHead(newName)
    }

    // Delete old branch
    await this.refs.deleteRef(oldName)
  }

  /**
   * Check if a branch exists
   * @param name Branch name to check
   * @returns True if branch exists
   */
  async exists(name: string): Promise<boolean> {
    const commit = await this.refs.resolveRef(name)
    return commit !== null
  }

  /**
   * Switch to a branch (checkout)
   *
   * This updates HEAD to point to the branch and reconstructs the database
   * state to match the commit the branch points to.
   *
   * @param name Branch name to switch to
   * @param opts Options including create flag
   */
  async checkout(name: string, opts?: CheckoutOptions): Promise<void> {
    // Check if branch exists
    let exists = await this.exists(name)

    if (!exists && opts?.create) {
      // Create the branch first
      await this.create(name)
      exists = true
    }

    if (!exists) {
      throw new Error(
        `Branch not found: ${name}. Use --create flag to create it.`
      )
    }

    // Get the commit the branch points to
    const commit = await this.refs.resolveRef(name)
    if (!commit) {
      throw new Error(`Branch ${name} does not point to a valid commit`)
    }

    // Verify the commit exists
    try {
      await loadCommit(this.opts.storage, commit)
    } catch (error) {
      throw new Error(
        `Cannot checkout ${name}: commit ${commit} not found. ${
          error instanceof Error ? error.message : String(error)
        }`
      )
    }

    // Update HEAD to point to the branch
    await this.refs.setHead(name)

    // TODO: In a full implementation, we would:
    // 1. Load the commit object
    // 2. Reconstruct the database state from the commit's state snapshot
    // 3. Update all data files, relationships, and event log to match
    //
    // For now, we just update HEAD. The actual state reconstruction
    // will be implemented as part of the restore functionality.
  }

  /**
   * Validate branch name format
   * @param name Branch name to validate
   * @returns True if valid
   */
  private isValidBranchName(name: string): boolean {
    // Branch names can contain:
    // - alphanumeric characters
    // - hyphens, slashes, underscores
    // - must not start or end with slash
    // - must not contain spaces or special characters
    const validPattern = /^[a-zA-Z0-9_-]+([/][a-zA-Z0-9_-]+)*$/
    return validPattern.test(name)
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new BranchManager instance
 * @param opts BranchManager options
 * @returns BranchManager instance
 */
export function createBranchManager(opts: BranchManagerOptions): BranchManager {
  return new BranchManager(opts)
}

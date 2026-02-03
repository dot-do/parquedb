import { describe, it, expect, beforeEach } from 'vitest'
import { BranchManager, createBranchManager } from '../../../src/sync/branch-manager'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { createRefManager } from '../../../src/sync/refs'
import { createCommit, saveCommit } from '../../../src/sync/commit'

describe('branch-manager', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
  })

  /**
   * Helper to create a commit and save it to storage
   */
  async function createAndSaveCommit(message: string, parents: string[] = []): Promise<string> {
    const commit = await createCommit(
      {
        collections: {},
        relationships: {
          forwardHash: 'forward-hash',
          reverseHash: 'reverse-hash',
        },
        eventLogPosition: {
          segmentId: 'segment-0',
          offset: 0,
        },
      },
      {
        message,
        author: 'test-author',
        parents,
      }
    )

    await saveCommit(storage, commit)
    return commit.hash
  }

  describe('createBranchManager', () => {
    it('should create a BranchManager instance', () => {
      expect(branchManager).toBeInstanceOf(BranchManager)
    })
  })

  describe('current', () => {
    it('should return null when HEAD is detached', async () => {
      const refManager = createRefManager(storage)
      await refManager.detachHead('abc123')

      const current = await branchManager.current()
      expect(current).toBeNull()
    })

    it('should return branch name when HEAD points to branch', async () => {
      const refManager = createRefManager(storage)
      await refManager.setHead('main')

      const current = await branchManager.current()
      expect(current).toBe('main')
    })

    it('should return default branch name when HEAD does not exist', async () => {
      const current = await branchManager.current()
      expect(current).toBe('main')
    })
  })

  describe('list', () => {
    it('should return empty array when no branches exist', async () => {
      const branches = await branchManager.list()
      expect(branches).toEqual([])
    })

    it('should list all branches', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', 'hash1')
      await refManager.updateRef('develop', 'hash2')
      await refManager.updateRef('feature/test', 'hash3')

      const branches = await branchManager.list()
      expect(branches).toHaveLength(3)

      const names = branches.map(b => b.name)
      expect(names).toContain('main')
      expect(names).toContain('develop')
      expect(names).toContain('feature/test')
    })

    it('should mark current branch correctly', async () => {
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', 'hash1')
      await refManager.updateRef('develop', 'hash2')

      const branches = await branchManager.list()

      const mainBranch = branches.find(b => b.name === 'main')
      const developBranch = branches.find(b => b.name === 'develop')

      expect(mainBranch?.isCurrent).toBe(true)
      expect(developBranch?.isCurrent).toBe(false)
    })

    it('should include commit hashes', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', 'abcdef1234567890')

      const branches = await branchManager.list()
      const mainBranch = branches.find(b => b.name === 'main')

      expect(mainBranch?.commit).toBe('abcdef1234567890')
    })

    it('should mark all branches as non-remote for local branches', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', 'hash1')
      await refManager.updateRef('develop', 'hash2')

      const branches = await branchManager.list()

      for (const branch of branches) {
        expect(branch.isRemote).toBe(false)
      }
    })
  })

  describe('create', () => {
    it('should create a branch from current HEAD', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)

      await branchManager.create('develop')

      const resolved = await refManager.resolveRef('develop')
      expect(resolved).toBe(commitHash)
    })

    it('should create a branch from specified base', async () => {
      const commit1 = await createAndSaveCommit('Commit 1')
      const commit2 = await createAndSaveCommit('Commit 2', [commit1])
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commit2)

      await branchManager.create('release', { from: 'main' })

      const resolved = await refManager.resolveRef('release')
      expect(resolved).toBe(commit2)
    })

    it('should create a branch from a commit hash', async () => {
      const commit1 = await createAndSaveCommit('Commit 1')
      const commit2 = await createAndSaveCommit('Commit 2', [commit1])
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commit2)

      await branchManager.create('hotfix', { from: commit1 })

      const resolved = await refManager.resolveRef('hotfix')
      expect(resolved).toBe(commit1)
    })

    it('should throw when branch already exists', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)

      await expect(branchManager.create('main')).rejects.toThrow('Branch already exists')
    })

    it('should throw when base ref does not exist', async () => {
      await expect(branchManager.create('develop', { from: 'nonexistent' })).rejects.toThrow(
        'base ref not found'
      )
    })

    it('should throw when HEAD is empty and no base specified', async () => {
      await expect(branchManager.create('develop')).rejects.toThrow(
        'HEAD does not point to any commit'
      )
    })

    it('should validate branch name format', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)

      await expect(branchManager.create('invalid branch name')).rejects.toThrow(
        'Invalid branch name'
      )
      await expect(branchManager.create('invalid//')).rejects.toThrow('Invalid branch name')
      await expect(branchManager.create('/invalid')).rejects.toThrow('Invalid branch name')
      await expect(branchManager.create('invalid/')).rejects.toThrow('Invalid branch name')
    })

    it('should allow valid branch names', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)

      await expect(branchManager.create('feature/new-thing')).resolves.not.toThrow()
      await expect(branchManager.create('bug-fix-123')).resolves.not.toThrow()
      await expect(branchManager.create('feature_v2')).resolves.not.toThrow()
    })
  })

  describe('delete', () => {
    it('should delete a branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('develop', commitHash)

      await branchManager.delete('develop')

      const resolved = await refManager.resolveRef('develop')
      expect(resolved).toBeNull()
    })

    it('should throw when deleting non-existent branch', async () => {
      await expect(branchManager.delete('nonexistent')).rejects.toThrow('Branch not found')
    })

    it('should throw when deleting current branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)

      await expect(branchManager.delete('main')).rejects.toThrow('Cannot delete current branch')
    })

    it('should allow force delete', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('develop', commitHash)

      await branchManager.delete('develop', { force: true })

      const resolved = await refManager.resolveRef('develop')
      expect(resolved).toBeNull()
    })
  })

  describe('rename', () => {
    it('should rename a branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('old-name', commitHash)

      await branchManager.rename('old-name', 'new-name')

      const oldResolved = await refManager.resolveRef('old-name')
      const newResolved = await refManager.resolveRef('new-name')

      expect(oldResolved).toBeNull()
      expect(newResolved).toBe(commitHash)
    })

    it('should throw when old branch does not exist', async () => {
      await expect(branchManager.rename('nonexistent', 'new-name')).rejects.toThrow(
        'Branch not found'
      )
    })

    it('should throw when new branch already exists', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)
      await refManager.updateRef('develop', commitHash)

      await expect(branchManager.rename('main', 'develop')).rejects.toThrow(
        'Branch already exists'
      )
    })

    it('should update HEAD when renaming current branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)

      await branchManager.rename('main', 'primary')

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'primary' })
    })

    it('should validate new branch name', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)

      await expect(branchManager.rename('main', 'invalid name')).rejects.toThrow(
        'Invalid branch name'
      )
    })
  })

  describe('exists', () => {
    it('should return true for existing branch', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', 'hash1')

      const exists = await branchManager.exists('main')
      expect(exists).toBe(true)
    })

    it('should return false for non-existent branch', async () => {
      const exists = await branchManager.exists('nonexistent')
      expect(exists).toBe(false)
    })
  })

  describe('checkout', () => {
    it('should switch to an existing branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)
      await refManager.updateRef('develop', commitHash)

      // Use skipStateReconstruction since test commits don't have valid object store data
      await branchManager.checkout('develop', { skipStateReconstruction: true })

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'develop' })
    })

    it('should throw when branch does not exist', async () => {
      await expect(branchManager.checkout('nonexistent')).rejects.toThrow('Branch not found')
    })

    it('should create and checkout when create flag is set', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)

      // Use skipStateReconstruction since test commits don't have valid object store data
      await branchManager.checkout('new-branch', { create: true, skipStateReconstruction: true })

      const head = await refManager.getHead()
      expect(head).toEqual({ type: 'branch', ref: 'new-branch' })

      const resolved = await refManager.resolveRef('new-branch')
      expect(resolved).toBe(commitHash)
    })

    it('should throw when commit does not exist', async () => {
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', 'nonexistent-commit-hash')

      await expect(branchManager.checkout('main')).rejects.toThrow('commit')
    })
  })

  describe('integration scenarios', () => {
    it('should support complete branching workflow', async () => {
      // Create initial commit on main
      const commit1 = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commit1)

      // List branches
      let branches = await branchManager.list()
      expect(branches).toHaveLength(1)
      expect(branches[0].name).toBe('main')
      expect(branches[0].isCurrent).toBe(true)

      // Create feature branch
      await branchManager.create('feature/new-thing')
      branches = await branchManager.list()
      expect(branches).toHaveLength(2)

      // Switch to feature branch
      await branchManager.checkout('feature/new-thing')
      const current = await branchManager.current()
      expect(current).toBe('feature/new-thing')

      // Create commit on feature branch
      const commit2 = await createAndSaveCommit('Feature commit', [commit1])
      await refManager.updateRef('feature/new-thing', commit2)

      // Switch back to main
      await branchManager.checkout('main')
      const mainResolved = await refManager.resolveRef('HEAD')
      expect(mainResolved).toBe(commit1)

      // Delete feature branch
      await branchManager.delete('feature/new-thing')
      branches = await branchManager.list()
      expect(branches).toHaveLength(1)
    })

    it('should support renaming current branch', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('master')
      await refManager.updateRef('master', commitHash)

      // Rename master to main
      await branchManager.rename('master', 'main')

      const current = await branchManager.current()
      expect(current).toBe('main')

      const resolved = await refManager.resolveRef('HEAD')
      expect(resolved).toBe(commitHash)
    })

    it('should support create and checkout in one operation', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.setHead('main')
      await refManager.updateRef('main', commitHash)

      await branchManager.checkout('hotfix', { create: true })

      const current = await branchManager.current()
      expect(current).toBe('hotfix')

      const resolved = await refManager.resolveRef('hotfix')
      expect(resolved).toBe(commitHash)
    })

    it('should support hierarchical branch names', async () => {
      const commitHash = await createAndSaveCommit('Initial commit')
      const refManager = createRefManager(storage)
      await refManager.updateRef('main', commitHash)

      await branchManager.create('feature/user-auth')
      await branchManager.create('feature/user-auth/login')
      await branchManager.create('bugfix/issue-123')

      const branches = await branchManager.list()
      const names = branches.map(b => b.name)

      expect(names).toContain('feature/user-auth')
      expect(names).toContain('feature/user-auth/login')
      expect(names).toContain('bugfix/issue-123')
    })
  })
})

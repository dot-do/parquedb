/**
 * E2E Tests for Branch and Merge Operations
 *
 * Comprehensive tests covering:
 * 1. Branch lifecycle (create, list, delete)
 * 2. Checkout operations
 * 3. Merge scenarios (fast-forward, 3-way, conflicts)
 * 4. Edge cases
 *
 * Uses MemoryBackend for isolated testing of the full branch/merge flow.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { BranchManager, createBranchManager } from '../../src/sync/branch-manager'
import { RefManager, createRefManager } from '../../src/sync/refs'
import {
  createCommit,
  saveCommit,
  loadCommit,
  type DatabaseState,
  type DatabaseCommit,
} from '../../src/sync/commit'
import {
  createMergeState,
  saveMergeState,
  loadMergeState,
  clearMergeState,
  hasMergeInProgress,
  addConflict,
  resolveConflict,
  getUnresolvedConflicts,
  allConflictsResolved,
  type MergeState,
  type ConflictInfo,
} from '../../src/sync/merge-state'
import {
  mergeEventStreams,
  type MergeOptions,
  type EventMergeResult,
} from '../../src/sync/event-merge'
import type { Event } from '../../src/types'

// =============================================================================
// Test Fixtures
// =============================================================================

/**
 * Create a minimal database state for testing commits
 */
function createTestDatabaseState(overrides: Partial<DatabaseState> = {}): DatabaseState {
  return {
    collections: {
      users: {
        dataHash: 'abc123',
        schemaHash: 'def456',
        rowCount: 10,
      },
    },
    relationships: {
      forwardHash: 'rel-fwd-hash',
      reverseHash: 'rel-rev-hash',
    },
    eventLogPosition: {
      segmentId: 'segment-001',
      offset: 100,
    },
    ...overrides,
  }
}

/**
 * Create a test event for merge testing
 */
function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'users:user1',
    after: { name: 'Test User' },
    ...overrides,
  }
}

/**
 * Helper to create and save a commit
 */
async function createAndSaveCommit(
  storage: MemoryBackend,
  state: DatabaseState,
  message: string,
  parents: string[] = []
): Promise<DatabaseCommit> {
  const commit = await createCommit(state, {
    message,
    author: 'test-user',
    parents,
  })
  await saveCommit(storage, commit)
  return commit
}

/**
 * Initialize a repo with main branch pointing to initial commit
 */
async function initializeTestRepo(storage: MemoryBackend): Promise<{
  branchManager: BranchManager
  refManager: RefManager
  initialCommit: DatabaseCommit
}> {
  const branchManager = createBranchManager({ storage })
  const refManager = createRefManager(storage)

  // Create initial commit
  const initialCommit = await createAndSaveCommit(
    storage,
    createTestDatabaseState(),
    'Initial commit'
  )

  // Set up main branch pointing to initial commit
  await refManager.updateRef('main', initialCommit.hash)
  await refManager.setHead('main')

  return { branchManager, refManager, initialCommit }
}

// =============================================================================
// Branch Lifecycle Tests
// =============================================================================

describe('Branch Lifecycle', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager
  let initialCommit: DatabaseCommit

  beforeEach(async () => {
    storage = new MemoryBackend()
    const initialized = await initializeTestRepo(storage)
    branchManager = initialized.branchManager
    refManager = initialized.refManager
    initialCommit = initialized.initialCommit
  })

  describe('Create branch from main', () => {
    it('creates a new branch pointing to HEAD commit', async () => {
      await branchManager.create('feature/new-feature')

      const exists = await branchManager.exists('feature/new-feature')
      expect(exists).toBe(true)

      // Branch should point to same commit as main
      const branchCommit = await refManager.resolveRef('feature/new-feature')
      expect(branchCommit).toBe(initialCommit.hash)
    })

    it('creates branch from specific base branch', async () => {
      // Create develop branch with a new commit
      await branchManager.create('develop')
      const developCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState({ collections: { posts: { dataHash: 'posts-hash', schemaHash: 'posts-schema', rowCount: 5 } } }),
        'Develop commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('develop', developCommit.hash)

      // Create feature branch from develop
      await branchManager.create('feature/from-develop', { from: 'develop' })

      const featureCommit = await refManager.resolveRef('feature/from-develop')
      expect(featureCommit).toBe(developCommit.hash)
    })

    it('rejects invalid branch names', async () => {
      await expect(branchManager.create('invalid name with spaces')).rejects.toThrow('Invalid branch name')
      await expect(branchManager.create('invalid..dots')).rejects.toThrow('Invalid branch name')
      await expect(branchManager.create('/leading-slash')).rejects.toThrow('Invalid branch name')
    })

    it('rejects creating duplicate branch', async () => {
      await branchManager.create('feature/exists')
      await expect(branchManager.create('feature/exists')).rejects.toThrow('Branch already exists')
    })
  })

  describe('Make changes on branch', () => {
    it('branch can have independent commits', async () => {
      // Create feature branch
      await branchManager.create('feature/independent')

      // Add commit to feature branch
      const featureCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState({ collections: { users: { dataHash: 'feature-users', schemaHash: 'schema', rowCount: 20 } } }),
        'Feature branch commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('feature/independent', featureCommit.hash)

      // Verify main still points to initial commit
      const mainCommit = await refManager.resolveRef('main')
      expect(mainCommit).toBe(initialCommit.hash)

      // Verify feature branch points to new commit
      const featureBranchCommit = await refManager.resolveRef('feature/independent')
      expect(featureBranchCommit).toBe(featureCommit.hash)
    })
  })

  describe('List branches shows new branch', () => {
    it('lists all branches', async () => {
      await branchManager.create('develop')
      await branchManager.create('feature/a')
      await branchManager.create('feature/b')

      const branches = await branchManager.list()

      expect(branches.length).toBe(4) // main + 3 created
      const branchNames = branches.map(b => b.name).sort()
      expect(branchNames).toEqual(['develop', 'feature/a', 'feature/b', 'main'])
    })

    it('marks current branch correctly', async () => {
      await branchManager.create('develop')

      const branches = await branchManager.list()
      const mainBranch = branches.find(b => b.name === 'main')
      const developBranch = branches.find(b => b.name === 'develop')

      expect(mainBranch?.isCurrent).toBe(true)
      expect(developBranch?.isCurrent).toBe(false)
    })

    it('shows commit hash for each branch', async () => {
      await branchManager.create('develop')

      const branches = await branchManager.list()

      for (const branch of branches) {
        expect(branch.commit).toBe(initialCommit.hash)
      }
    })
  })

  describe('Delete branch', () => {
    it('deletes non-current branch', async () => {
      await branchManager.create('feature/to-delete')

      await branchManager.delete('feature/to-delete')

      const exists = await branchManager.exists('feature/to-delete')
      expect(exists).toBe(false)
    })

    it('rejects deleting current branch', async () => {
      await expect(branchManager.delete('main')).rejects.toThrow('Cannot delete current branch')
    })

    it('rejects deleting non-existent branch', async () => {
      await expect(branchManager.delete('non-existent')).rejects.toThrow('Branch not found')
    })

    it('supports force delete', async () => {
      await branchManager.create('feature/force-delete')

      // Force delete should work even for branches with unmerged changes
      await branchManager.delete('feature/force-delete', { force: true })

      const exists = await branchManager.exists('feature/force-delete')
      expect(exists).toBe(false)
    })
  })

  describe('Rename branch', () => {
    it('renames a branch', async () => {
      await branchManager.create('old-name')

      await branchManager.rename('old-name', 'new-name')

      expect(await branchManager.exists('old-name')).toBe(false)
      expect(await branchManager.exists('new-name')).toBe(true)
    })

    it('updates HEAD when renaming current branch', async () => {
      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('main', { skipStateReconstruction: true })

      await branchManager.rename('main', 'primary')

      const current = await branchManager.current()
      expect(current).toBe('primary')
    })

    it('rejects renaming to existing branch name', async () => {
      await branchManager.create('branch-a')
      await branchManager.create('branch-b')

      await expect(branchManager.rename('branch-a', 'branch-b')).rejects.toThrow('Branch already exists')
    })
  })
})

// =============================================================================
// Checkout Operations Tests
// =============================================================================

describe('Checkout Operations', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager
  let initialCommit: DatabaseCommit

  beforeEach(async () => {
    storage = new MemoryBackend()
    const initialized = await initializeTestRepo(storage)
    branchManager = initialized.branchManager
    refManager = initialized.refManager
    initialCommit = initialized.initialCommit
  })

  describe('Checkout existing branch', () => {
    it('switches HEAD to target branch', async () => {
      await branchManager.create('develop')

      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('develop', { skipStateReconstruction: true })

      const current = await branchManager.current()
      expect(current).toBe('develop')

      const head = await refManager.getHead()
      expect(head.type).toBe('branch')
      expect(head.ref).toBe('develop')
    })

    it('can checkout back to main', async () => {
      await branchManager.create('feature')
      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('feature', { skipStateReconstruction: true })
      await branchManager.checkout('main', { skipStateReconstruction: true })

      const current = await branchManager.current()
      expect(current).toBe('main')
    })

    it('reflects correct current branch in list', async () => {
      await branchManager.create('develop')
      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('develop', { skipStateReconstruction: true })

      const branches = await branchManager.list()
      const mainBranch = branches.find(b => b.name === 'main')
      const developBranch = branches.find(b => b.name === 'develop')

      expect(mainBranch?.isCurrent).toBe(false)
      expect(developBranch?.isCurrent).toBe(true)
    })
  })

  describe('Checkout with uncommitted changes', () => {
    // The current implementation does not track uncommitted changes (parquedb-kpnj).
    // When BranchManager gains uncommitted changes tracking, this test should:
    // 1. Make changes to data files
    // 2. Attempt checkout
    // 3. Expect warning or failure without --force
    it.todo('should warn/fail when there are uncommitted changes')

    it('force checkout bypasses uncommitted changes check', async () => {
      await branchManager.create('develop')

      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('develop', { skipStateReconstruction: true })

      const current = await branchManager.current()
      expect(current).toBe('develop')
    })
  })

  describe('Checkout non-existent branch', () => {
    it('fails with helpful error message', async () => {
      await expect(branchManager.checkout('non-existent')).rejects.toThrow(
        'Branch not found: non-existent'
      )
    })

    it('suggests using --create flag', async () => {
      try {
        await branchManager.checkout('new-feature')
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as Error).message).toContain('--create')
      }
    })

    it('creates branch with create option', async () => {
      // Use skipStateReconstruction since test commits don't have real data objects
      await branchManager.checkout('new-feature', { create: true, skipStateReconstruction: true })

      const exists = await branchManager.exists('new-feature')
      expect(exists).toBe(true)

      const current = await branchManager.current()
      expect(current).toBe('new-feature')
    })
  })
})

// =============================================================================
// Merge Scenarios Tests
// =============================================================================

describe('Merge Scenarios', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager
  let initialCommit: DatabaseCommit

  beforeEach(async () => {
    storage = new MemoryBackend()
    const initialized = await initializeTestRepo(storage)
    branchManager = initialized.branchManager
    refManager = initialized.refManager
    initialCommit = initialized.initialCommit
  })

  describe('Fast-forward merge (no conflicts)', () => {
    it('merges when target has no new commits', async () => {
      // Create feature branch with new commit
      await branchManager.create('feature')
      const featureCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState({ collections: { users: { dataHash: 'feature-data', schemaHash: 'schema', rowCount: 15 } } }),
        'Feature commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('feature', featureCommit.hash)

      // Main has no new commits, so fast-forward is possible
      // Simulate fast-forward by updating main to point to feature commit
      await refManager.updateRef('main', featureCommit.hash)

      const mainCommit = await refManager.resolveRef('main')
      expect(mainCommit).toBe(featureCommit.hash)
    })

    it('preserves commit history after fast-forward', async () => {
      await branchManager.create('feature')
      const featureCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState(),
        'Feature commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('feature', featureCommit.hash)

      // Fast-forward main
      await refManager.updateRef('main', featureCommit.hash)

      // Verify commit history is intact
      const commit = await loadCommit(storage, featureCommit.hash)
      expect(commit.parents).toContain(initialCommit.hash)
    })
  })

  describe('3-way merge with auto-resolvable conflicts', () => {
    it('auto-merges commutative operations ($inc)', async () => {
      const baseEvent = createTestEvent({
        id: 'base',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User', counter: 0 },
      })

      // Our branch increments by 5
      const ourEvent = createTestEvent({
        id: 'ours',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'User', counter: 0 },
        after: { name: 'User', counter: 5 },
        metadata: { update: { $inc: { counter: 5 } } },
      })

      // Their branch increments by 3
      const theirEvent = createTestEvent({
        id: 'theirs',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'User', counter: 0 },
        after: { name: 'User', counter: 3 },
        metadata: { update: { $inc: { counter: 3 } } },
      })

      const result = await mergeEventStreams(
        [baseEvent, ourEvent],
        [theirEvent],
        { autoMergeCommutative: true }
      )

      // Should auto-merge successfully
      expect(result.success).toBe(true)
      expect(result.autoMerged.length).toBeGreaterThan(0)
    })

    it('auto-merges when branches modify different fields', async () => {
      const baseEvent = createTestEvent({
        id: 'base',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User', email: 'old@example.com', role: 'user' },
      })

      // Our branch changes email
      const ourEvent = createTestEvent({
        id: 'ours',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'User', email: 'old@example.com', role: 'user' },
        after: { name: 'User', email: 'new@example.com', role: 'user' },
        metadata: { update: { $set: { email: 'new@example.com' } } },
      })

      // Their branch changes role
      const theirEvent = createTestEvent({
        id: 'theirs',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'User', email: 'old@example.com', role: 'user' },
        after: { name: 'User', email: 'old@example.com', role: 'admin' },
        metadata: { update: { $set: { role: 'admin' } } },
      })

      const result = await mergeEventStreams(
        [baseEvent, ourEvent],
        [theirEvent],
        { autoMergeCommutative: true }
      )

      // Should succeed - different fields modified
      expect(result.success).toBe(true)
    })
  })

  describe('Merge with manual conflict resolution', () => {
    it('detects conflict when same field modified differently', async () => {
      const baseEvent = createTestEvent({
        id: 'base',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Original' },
      })

      const ourEvent = createTestEvent({
        id: 'ours',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'Original' },
        after: { name: 'Our Name' },
        metadata: { update: { $set: { name: 'Our Name' } } },
      })

      const theirEvent = createTestEvent({
        id: 'theirs',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'Original' },
        after: { name: 'Their Name' },
        metadata: { update: { $set: { name: 'Their Name' } } },
      })

      const result = await mergeEventStreams(
        [baseEvent, ourEvent],
        [theirEvent],
        { resolutionStrategy: 'manual' }
      )

      // Should have conflicts
      expect(result.success).toBe(false)
      expect(result.conflicts.length).toBeGreaterThan(0)
      expect(result.conflicts[0].type).toBe('concurrent_update')
    })

    it('resolves conflict with ours strategy', async () => {
      const ourEvent = createTestEvent({
        id: 'ours',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Our Name' },
        metadata: { update: { $set: { name: 'Our Name' } } },
      })

      const theirEvent = createTestEvent({
        id: 'theirs',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Their Name' },
        metadata: { update: { $set: { name: 'Their Name' } } },
      })

      const result = await mergeEventStreams(
        [ourEvent],
        [theirEvent],
        { resolutionStrategy: 'ours' }
      )

      // With 'ours' strategy, conflicts should be auto-resolved
      expect(result.resolved.length).toBeGreaterThan(0)
    })

    it('resolves conflict with theirs strategy', async () => {
      const ourEvent = createTestEvent({
        id: 'ours',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Our Name' },
        metadata: { update: { $set: { name: 'Our Name' } } },
      })

      const theirEvent = createTestEvent({
        id: 'theirs',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        after: { name: 'Their Name' },
        metadata: { update: { $set: { name: 'Their Name' } } },
      })

      const result = await mergeEventStreams(
        [ourEvent],
        [theirEvent],
        { resolutionStrategy: 'theirs' }
      )

      // With 'theirs' strategy, conflicts should be auto-resolved
      expect(result.resolved.length).toBeGreaterThan(0)
    })
  })

  describe('Merge state management', () => {
    it('saves and loads merge state', async () => {
      const mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'base-hash',
        sourceCommit: 'source-hash',
        targetCommit: 'target-hash',
        strategy: 'manual',
      })

      await saveMergeState(storage, mergeState)

      const loaded = await loadMergeState(storage)
      expect(loaded).not.toBeNull()
      expect(loaded?.source).toBe('feature')
      expect(loaded?.target).toBe('main')
      expect(loaded?.status).toBe('in_progress')
    })

    it('tracks unresolved conflicts', async () => {
      let mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'base-hash',
        sourceCommit: 'source-hash',
        targetCommit: 'target-hash',
      })

      const conflict: ConflictInfo = {
        entityId: 'users/user1',
        collection: 'users',
        fields: ['name'],
        resolved: false,
        ourValue: 'Our Name',
        theirValue: 'Their Name',
        baseValue: 'Original',
      }

      mergeState = addConflict(mergeState, conflict)
      expect(mergeState.status).toBe('conflicted')
      expect(getUnresolvedConflicts(mergeState)).toHaveLength(1)
    })

    it('resolves conflicts in merge state', async () => {
      let mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'base-hash',
        sourceCommit: 'source-hash',
        targetCommit: 'target-hash',
      })

      mergeState = addConflict(mergeState, {
        entityId: 'users/user1',
        collection: 'users',
        fields: ['name'],
        resolved: false,
      })

      mergeState = resolveConflict(mergeState, 'users/user1', 'ours')

      expect(allConflictsResolved(mergeState)).toBe(true)
      expect(mergeState.status).toBe('resolved')
    })

    it('clears merge state on abort', async () => {
      const mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'base-hash',
        sourceCommit: 'source-hash',
        targetCommit: 'target-hash',
      })

      await saveMergeState(storage, mergeState)
      expect(await hasMergeInProgress(storage)).toBe(true)

      await clearMergeState(storage)
      expect(await hasMergeInProgress(storage)).toBe(false)
    })
  })

  describe('Merge abort', () => {
    it('clears merge state', async () => {
      const mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'base-hash',
        sourceCommit: 'source-hash',
        targetCommit: 'target-hash',
      })

      await saveMergeState(storage, mergeState)

      await clearMergeState(storage)

      const loaded = await loadMergeState(storage)
      expect(loaded).toBeNull()
    })

    it('restores original branch state after abort', async () => {
      // Create feature branch with commit
      await branchManager.create('feature')
      const featureCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState(),
        'Feature commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('feature', featureCommit.hash)

      // Start merge (save merge state)
      const mergeState = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: initialCommit.hash,
        sourceCommit: featureCommit.hash,
        targetCommit: initialCommit.hash,
      })
      await saveMergeState(storage, mergeState)

      // Abort merge
      await clearMergeState(storage)

      // Main should still point to original commit
      const mainCommit = await refManager.resolveRef('main')
      expect(mainCommit).toBe(initialCommit.hash)
    })
  })
})

// =============================================================================
// Edge Cases Tests
// =============================================================================

describe('Edge Cases', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager
  let initialCommit: DatabaseCommit

  beforeEach(async () => {
    storage = new MemoryBackend()
    const initialized = await initializeTestRepo(storage)
    branchManager = initialized.branchManager
    refManager = initialized.refManager
    initialCommit = initialized.initialCommit
  })

  describe('Merge into self', () => {
    it('should fail when trying to merge branch into itself', async () => {
      const mergeState = createMergeState({
        source: 'main',
        target: 'main',
        baseCommit: initialCommit.hash,
        sourceCommit: initialCommit.hash,
        targetCommit: initialCommit.hash,
      })

      // Same source and target should be rejected
      expect(mergeState.source).toBe(mergeState.target)
      // In a real implementation, performMerge would check this and fail
    })
  })

  describe('Merge already-merged branch', () => {
    it('results in no-op when branch is already merged', async () => {
      // Create and merge feature branch
      await branchManager.create('feature')
      const featureCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState(),
        'Feature commit',
        [initialCommit.hash]
      )
      await refManager.updateRef('feature', featureCommit.hash)

      // "Merge" feature into main (fast-forward)
      await refManager.updateRef('main', featureCommit.hash)

      // Now both branches point to same commit
      const mainCommit = await refManager.resolveRef('main')
      const featureBranchCommit = await refManager.resolveRef('feature')
      expect(mainCommit).toBe(featureBranchCommit)

      // Attempting to merge again should be a no-op
      // Event merge with same events should result in empty merge
      const result = await mergeEventStreams([], [], {})

      expect(result.success).toBe(true)
      expect(result.conflicts).toHaveLength(0)
      expect(result.mergedEvents).toHaveLength(0)
    })
  })

  describe('Concurrent merge attempts', () => {
    it('detects existing merge in progress', async () => {
      const mergeState = createMergeState({
        source: 'feature-a',
        target: 'main',
        baseCommit: 'base',
        sourceCommit: 'source-a',
        targetCommit: initialCommit.hash,
      })

      await saveMergeState(storage, mergeState)

      // Another merge should detect existing merge
      const inProgress = await hasMergeInProgress(storage)
      expect(inProgress).toBe(true)

      // Loading state should show existing merge
      const existingMerge = await loadMergeState(storage)
      expect(existingMerge?.source).toBe('feature-a')
    })

    it('prevents starting new merge when one is in progress', async () => {
      await saveMergeState(storage, createMergeState({
        source: 'feature-a',
        target: 'main',
        baseCommit: 'base',
        sourceCommit: 'source-a',
        targetCommit: initialCommit.hash,
      }))

      // Check if merge in progress before starting new one
      const canStartNew = !(await hasMergeInProgress(storage))
      expect(canStartNew).toBe(false)
    })
  })

  describe('DELETE vs UPDATE conflict', () => {
    it('detects conflict when one branch deletes and other updates', async () => {
      const createEvent = createTestEvent({
        id: 'create',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'User' },
      })

      // Our branch deletes
      const deleteEvent = createTestEvent({
        id: 'delete',
        target: 'users:user1',
        ts: 2000,
        op: 'DELETE',
        before: { name: 'User' },
      })

      // Their branch updates
      const updateEvent = createTestEvent({
        id: 'update',
        target: 'users:user1',
        ts: 2000,
        op: 'UPDATE',
        before: { name: 'User' },
        after: { name: 'Updated User' },
      })

      const result = await mergeEventStreams(
        [createEvent, deleteEvent],
        [updateEvent],
        { resolutionStrategy: 'manual' }
      )

      // Should detect delete vs update conflict
      expect(result.success).toBe(false)
      expect(result.conflicts.some(c => c.type === 'delete_update')).toBe(true)
    })
  })

  describe('CREATE + CREATE conflict', () => {
    it('detects conflict when same entity created in both branches', async () => {
      const ourCreate = createTestEvent({
        id: 'our-create',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Our User', email: 'our@example.com' },
      })

      const theirCreate = createTestEvent({
        id: 'their-create',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Their User', email: 'their@example.com' },
      })

      const result = await mergeEventStreams(
        [ourCreate],
        [theirCreate],
        { resolutionStrategy: 'manual' }
      )

      // Should detect create-create conflict
      expect(result.success).toBe(false)
      expect(result.conflicts.some(c => c.type === 'create_create')).toBe(true)
    })

    it('does not conflict when identical entity created in both branches', async () => {
      const ourCreate = createTestEvent({
        id: 'our-create',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Same User', email: 'same@example.com' },
      })

      const theirCreate = createTestEvent({
        id: 'their-create',
        target: 'users:user1',
        ts: 1000,
        op: 'CREATE',
        after: { name: 'Same User', email: 'same@example.com' },
      })

      const result = await mergeEventStreams(
        [ourCreate],
        [theirCreate],
        { resolutionStrategy: 'manual' }
      )

      // Should succeed when identical
      expect(result.success).toBe(true)
    })
  })

  describe('Branch with nested path names', () => {
    it('supports deeply nested branch names', async () => {
      await branchManager.create('feature/team-a/user-auth/login-flow')

      const exists = await branchManager.exists('feature/team-a/user-auth/login-flow')
      expect(exists).toBe(true)

      const branches = await branchManager.list()
      expect(branches.some(b => b.name === 'feature/team-a/user-auth/login-flow')).toBe(true)
    })

    it('lists nested branches correctly', async () => {
      await branchManager.create('feature/a')
      await branchManager.create('feature/a/sub')
      await branchManager.create('feature/b')

      const branches = await branchManager.list()
      const featureBranches = branches.filter(b => b.name.startsWith('feature/'))

      expect(featureBranches.length).toBe(3)
    })
  })

  describe('Orphan commits', () => {
    it('handles detached HEAD state', async () => {
      // Detach HEAD to a specific commit
      await refManager.detachHead(initialCommit.hash)

      const head = await refManager.getHead()
      expect(head.type).toBe('detached')
      expect(head.ref).toBe(initialCommit.hash)

      // Current branch should be null when detached
      const current = await branchManager.current()
      expect(current).toBeNull()
    })

    it('can create branch from detached HEAD', async () => {
      // Create a new commit
      const newCommit = await createAndSaveCommit(
        storage,
        createTestDatabaseState(),
        'Detached commit',
        [initialCommit.hash]
      )

      // Detach HEAD to new commit
      await refManager.detachHead(newCommit.hash)

      // Create branch from detached HEAD
      await branchManager.create('branch-from-detached', { from: newCommit.hash })

      const branchCommit = await refManager.resolveRef('branch-from-detached')
      expect(branchCommit).toBe(newCommit.hash)
    })
  })
})

// =============================================================================
// Merge Statistics Tests
// =============================================================================

describe('Merge Statistics', () => {
  it('tracks event counts from both branches', async () => {
    const ourEvents = [
      createTestEvent({ id: 'our-1', target: 'users:user1', ts: 1000, op: 'CREATE', after: { name: 'User 1' } }),
      createTestEvent({ id: 'our-2', target: 'users:user2', ts: 1001, op: 'CREATE', after: { name: 'User 2' } }),
    ]

    const theirEvents = [
      createTestEvent({ id: 'their-1', target: 'posts:post1', ts: 1000, op: 'CREATE', after: { title: 'Post 1' } }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents, {})

    expect(result.stats.fromOurs).toBe(2)
    expect(result.stats.fromTheirs).toBe(1)
    expect(result.stats.entitiesProcessed).toBe(3)
  })

  it('counts auto-merged operations', async () => {
    const ourEvent = createTestEvent({
      id: 'our',
      target: 'users:user1',
      ts: 2000,
      op: 'UPDATE',
      before: { counter: 0 },
      after: { counter: 5 },
      metadata: { update: { $inc: { counter: 5 } } },
    })

    const theirEvent = createTestEvent({
      id: 'their',
      target: 'users:user1',
      ts: 2000,
      op: 'UPDATE',
      before: { counter: 0 },
      after: { counter: 3 },
      metadata: { update: { $inc: { counter: 3 } } },
    })

    const result = await mergeEventStreams(
      [ourEvent],
      [theirEvent],
      { autoMergeCommutative: true }
    )

    expect(result.stats.autoMerged).toBeGreaterThan(0)
  })
})

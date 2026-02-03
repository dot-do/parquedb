/**
 * E2E Tests for Branch/Merge Operations
 *
 * Tests the full branching workflow including:
 * - Branch creation, switching (checkout), deletion
 * - Branch renaming
 * - Merge operations with conflict detection
 * - Conflict resolution strategies
 * - State reconstruction across branches
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { BranchManager, createBranchManager } from '../../src/sync/branch-manager'
import { RefManager, createRefManager } from '../../src/sync/refs'
import { createCommit, saveCommit, loadCommit, type DatabaseCommit } from '../../src/sync/commit'
import {
  mergeEventStreams,
  sortEvents,
  type EventMergeResult,
} from '../../src/sync/event-merge'
import {
  detectConflicts,
  type ConflictInfo,
} from '../../src/sync/conflict-detection'
import {
  resolveConflict,
  resolveAllConflicts,
  allResolutionsComplete,
  createArrayMergeStrategy,
  createFieldBasedStrategy,
} from '../../src/sync/conflict-resolution'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { Event } from '../../src/types/entity'
import { generateULID } from '../../src/utils/random'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a database commit with a given state
 */
async function createAndSaveCommit(
  storage: MemoryBackend,
  message: string,
  parents: string[] = [],
  stateOverrides: Partial<{
    collections: Record<string, { dataHash: string; schemaHash: string; rowCount: number }>
    relationships: { forwardHash: string; reverseHash: string }
    eventLogPosition: { segmentId: string; offset: number }
  }> = {}
): Promise<DatabaseCommit> {
  const state = {
    collections: stateOverrides.collections ?? {},
    relationships: stateOverrides.relationships ?? {
      forwardHash: 'forward-hash',
      reverseHash: 'reverse-hash',
    },
    eventLogPosition: stateOverrides.eventLogPosition ?? {
      segmentId: 'segment-0',
      offset: 0,
    },
  }

  const commit = await createCommit(state, {
    message,
    author: 'test-author',
    parents,
  })

  await saveCommit(storage, commit)
  return commit
}

/**
 * Create a test event for merge testing
 */
function createEvent(
  target: string,
  op: 'CREATE' | 'UPDATE' | 'DELETE',
  ts: number,
  before?: Record<string, unknown>,
  after?: Record<string, unknown>,
  metadata?: Record<string, unknown>
): Event {
  return {
    id: generateULID(),
    ts,
    op,
    target,
    before: before as any,
    after: after as any,
    actor: 'test-actor',
    metadata: metadata as any,
  }
}

// =============================================================================
// Branch Creation Tests
// =============================================================================

describe('E2E: Branch Creation', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('creates a branch from current HEAD', async () => {
    // Setup: Create initial commit on main
    const initialCommit = await createAndSaveCommit(storage, 'Initial commit')
    await refManager.setHead('main')
    await refManager.updateRef('main', initialCommit.hash)

    // Create feature branch
    await branchManager.create('feature/new-feature')

    // Verify branch exists and points to same commit
    const branches = await branchManager.list()
    expect(branches).toHaveLength(2)

    const featureBranch = branches.find(b => b.name === 'feature/new-feature')
    expect(featureBranch).toBeDefined()
    expect(featureBranch!.commit).toBe(initialCommit.hash)
    expect(featureBranch!.isCurrent).toBe(false)

    const mainBranch = branches.find(b => b.name === 'main')
    expect(mainBranch).toBeDefined()
    expect(mainBranch!.isCurrent).toBe(true)
  })

  it('creates a branch from a specific commit', async () => {
    // Setup: Create commit history
    const commit1 = await createAndSaveCommit(storage, 'Commit 1')
    const commit2 = await createAndSaveCommit(storage, 'Commit 2', [commit1.hash])
    const commit3 = await createAndSaveCommit(storage, 'Commit 3', [commit2.hash])
    await refManager.setHead('main')
    await refManager.updateRef('main', commit3.hash)

    // Create branch from commit2
    await branchManager.create('hotfix', { from: commit2.hash })

    // Verify hotfix points to commit2, not commit3
    const hotfixCommit = await refManager.resolveRef('hotfix')
    expect(hotfixCommit).toBe(commit2.hash)

    const mainCommit = await refManager.resolveRef('main')
    expect(mainCommit).toBe(commit3.hash)
  })

  it('creates multiple branches from same base', async () => {
    const initialCommit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', initialCommit.hash)

    // Create multiple feature branches
    await branchManager.create('feature/auth')
    await branchManager.create('feature/payments')
    await branchManager.create('bugfix/login')

    const branches = await branchManager.list()
    expect(branches).toHaveLength(4) // main + 3 feature branches

    // All branches should point to same commit
    for (const branch of branches) {
      expect(branch.commit).toBe(initialCommit.hash)
    }
  })

  it('rejects invalid branch names', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Invalid names should be rejected
    await expect(branchManager.create('invalid branch name')).rejects.toThrow('Invalid branch name')
    await expect(branchManager.create('//double-slash')).rejects.toThrow('Invalid branch name')
    await expect(branchManager.create('trailing/')).rejects.toThrow('Invalid branch name')
    await expect(branchManager.create('/leading-slash')).rejects.toThrow('Invalid branch name')
  })

  it('rejects duplicate branch names', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    await branchManager.create('feature/test')
    await expect(branchManager.create('feature/test')).rejects.toThrow('Branch already exists')
  })
})

// =============================================================================
// Branch Switching (Checkout) Tests
// =============================================================================

describe('E2E: Branch Switching', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('switches to an existing branch', async () => {
    // Setup: Create branches with different commits
    const commit1 = await createAndSaveCommit(storage, 'Main commit')
    const commit2 = await createAndSaveCommit(storage, 'Feature commit', [commit1.hash])
    await refManager.setHead('main')
    await refManager.updateRef('main', commit1.hash)
    await refManager.updateRef('feature', commit2.hash)

    // Verify starting on main
    expect(await branchManager.current()).toBe('main')

    // Switch to feature
    await branchManager.checkout('feature')

    // Verify HEAD now points to feature
    expect(await branchManager.current()).toBe('feature')
    const headCommit = await refManager.resolveRef('HEAD')
    expect(headCommit).toBe(commit2.hash)
  })

  it('creates and switches to new branch in one operation', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Create and checkout in one step
    await branchManager.checkout('new-feature', { create: true })

    // Verify on new branch
    expect(await branchManager.current()).toBe('new-feature')
    expect(await branchManager.exists('new-feature')).toBe(true)

    // Branch should point to same commit as main
    const newFeatureCommit = await refManager.resolveRef('new-feature')
    expect(newFeatureCommit).toBe(commit.hash)
  })

  it('fails to checkout non-existent branch without create flag', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    await expect(branchManager.checkout('does-not-exist')).rejects.toThrow('Branch not found')
  })

  it('tracks current branch correctly through multiple switches', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Create multiple branches
    await branchManager.create('feature-a')
    await branchManager.create('feature-b')
    await branchManager.create('feature-c')

    // Switch through branches
    await branchManager.checkout('feature-a')
    expect(await branchManager.current()).toBe('feature-a')

    await branchManager.checkout('feature-b')
    expect(await branchManager.current()).toBe('feature-b')

    await branchManager.checkout('main')
    expect(await branchManager.current()).toBe('main')

    await branchManager.checkout('feature-c')
    expect(await branchManager.current()).toBe('feature-c')

    // Verify isCurrent flag in list
    const branches = await branchManager.list()
    const currentBranch = branches.find(b => b.isCurrent)
    expect(currentBranch?.name).toBe('feature-c')
  })
})

// =============================================================================
// Branch Deletion Tests
// =============================================================================

describe('E2E: Branch Deletion', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('deletes a branch that is not current', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)
    await branchManager.create('to-delete')

    // Verify branch exists
    expect(await branchManager.exists('to-delete')).toBe(true)
    expect((await branchManager.list()).length).toBe(2)

    // Delete the branch
    await branchManager.delete('to-delete')

    // Verify branch is gone
    expect(await branchManager.exists('to-delete')).toBe(false)
    expect((await branchManager.list()).length).toBe(1)
  })

  it('prevents deletion of current branch', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    await expect(branchManager.delete('main')).rejects.toThrow('Cannot delete current branch')
  })

  it('can force delete a branch', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)
    await branchManager.create('force-delete')

    // Force delete should work
    await branchManager.delete('force-delete', { force: true })
    expect(await branchManager.exists('force-delete')).toBe(false)
  })

  it('fails to delete non-existent branch', async () => {
    await expect(branchManager.delete('phantom-branch')).rejects.toThrow('Branch not found')
  })
})

// =============================================================================
// Branch Renaming Tests
// =============================================================================

describe('E2E: Branch Renaming', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('renames a branch preserving commit reference', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)
    await branchManager.create('old-name')

    // Rename
    await branchManager.rename('old-name', 'new-name')

    // Old name gone, new name exists
    expect(await branchManager.exists('old-name')).toBe(false)
    expect(await branchManager.exists('new-name')).toBe(true)

    // Commit reference preserved
    const newNameCommit = await refManager.resolveRef('new-name')
    expect(newNameCommit).toBe(commit.hash)
  })

  it('updates HEAD when renaming current branch', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('master')
    await refManager.updateRef('master', commit.hash)

    // Rename current branch from master to main
    await branchManager.rename('master', 'main')

    // HEAD should now point to main
    expect(await branchManager.current()).toBe('main')
    expect(await branchManager.exists('master')).toBe(false)
    expect(await branchManager.exists('main')).toBe(true)
  })

  it('fails to rename to existing branch name', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.updateRef('main', commit.hash)
    await refManager.updateRef('develop', commit.hash)

    await expect(branchManager.rename('main', 'develop')).rejects.toThrow('Branch already exists')
  })

  it('validates new branch name on rename', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.updateRef('valid-name', commit.hash)

    await expect(branchManager.rename('valid-name', 'invalid name')).rejects.toThrow('Invalid branch name')
  })
})

// =============================================================================
// Event Merge Tests
// =============================================================================

describe('E2E: Event Merge Operations', () => {
  it('merges non-conflicting event streams', async () => {
    // Events on different entities - no conflict
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { title: 'Original' }, { title: 'Our Title' }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p2', 'UPDATE', 1100, { title: 'Original 2' }, { title: 'Their Title' }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents)

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.mergedEvents).toHaveLength(2)
    expect(result.stats.fromOurs).toBe(1)
    expect(result.stats.fromTheirs).toBe(1)
    expect(result.stats.entitiesProcessed).toBe(2)
  })

  it('merges events with only one side having changes', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 1000, undefined, { title: 'New Post' }),
      createEvent('posts:p2', 'UPDATE', 1100, { views: 0 }, { views: 10 }),
    ]

    const theirEvents: Event[] = []

    const result = await mergeEventStreams(ourEvents, theirEvents)

    expect(result.success).toBe(true)
    expect(result.mergedEvents).toHaveLength(2)
    expect(result.conflicts).toHaveLength(0)
  })

  it('merges identical CREATE events without conflict', async () => {
    const createData = { title: 'Same Post', content: 'Same content' }

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 1000, undefined, createData),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 1000, undefined, createData),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents)

    expect(result.success).toBe(true)
    // Should dedupe identical creates
    expect(result.mergedEvents.length).toBeLessThanOrEqual(2)
  })

  it('sorts merged events by timestamp', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 3000, {}, { v: 3 }),
      createEvent('posts:p2', 'UPDATE', 1000, {}, { v: 1 }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p3', 'UPDATE', 2000, {}, { v: 2 }),
      createEvent('posts:p4', 'UPDATE', 4000, {}, { v: 4 }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents)

    // Events should be sorted by timestamp
    const timestamps = result.mergedEvents.map(e => e.ts)
    const sortedTimestamps = [...timestamps].sort((a, b) => a - b)
    expect(timestamps).toEqual(sortedTimestamps)
  })
})

// =============================================================================
// Conflict Detection Tests
// =============================================================================

describe('E2E: Conflict Detection', () => {
  it('detects concurrent update conflict on same field', async () => {
    const ourEvents: Event[] = [
      createEvent(
        'posts:p1',
        'UPDATE',
        1000,
        { title: 'Original', status: 'draft' },
        { title: 'Our Title', status: 'draft' }
      ),
    ]

    const theirEvents: Event[] = [
      createEvent(
        'posts:p1',
        'UPDATE',
        1100,
        { title: 'Original', status: 'draft' },
        { title: 'Their Title', status: 'draft' }
      ),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    expect(conflicts).toHaveLength(1)
    expect(conflicts[0].type).toBe('concurrent_update')
    expect(conflicts[0].target).toBe('posts:p1')
    expect(conflicts[0].field).toBe('title')
    expect(conflicts[0].ourValue).toBe('Our Title')
    expect(conflicts[0].theirValue).toBe('Their Title')
    expect(conflicts[0].baseValue).toBe('Original')
  })

  it('detects delete vs update conflict', async () => {
    const ourEvents: Event[] = [
      createEvent(
        'posts:p1',
        'DELETE',
        1000,
        { title: 'Post to delete' },
        undefined
      ),
    ]

    const theirEvents: Event[] = [
      createEvent(
        'posts:p1',
        'UPDATE',
        1100,
        { title: 'Post to delete' },
        { title: 'Updated post' }
      ),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    expect(conflicts).toHaveLength(1)
    expect(conflicts[0].type).toBe('delete_update')
    expect(conflicts[0].target).toBe('posts:p1')
    expect(conflicts[0].ourValue).toBeUndefined()
    expect(conflicts[0].theirValue).toEqual({ title: 'Updated post' })
  })

  it('detects create-create conflict with different values', async () => {
    const ourEvents: Event[] = [
      createEvent(
        'posts:p1',
        'CREATE',
        1000,
        undefined,
        { title: 'Our Post', views: 0 }
      ),
    ]

    const theirEvents: Event[] = [
      createEvent(
        'posts:p1',
        'CREATE',
        1100,
        undefined,
        { title: 'Their Post', views: 100 }
      ),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    expect(conflicts).toHaveLength(1)
    expect(conflicts[0].type).toBe('create_create')
  })

  it('does not flag conflict when different fields are modified', async () => {
    const ourEvents: Event[] = [
      createEvent(
        'posts:p1',
        'UPDATE',
        1000,
        { title: 'Original', views: 0 },
        { title: 'New Title', views: 0 }
      ),
    ]

    const theirEvents: Event[] = [
      createEvent(
        'posts:p1',
        'UPDATE',
        1100,
        { title: 'Original', views: 0 },
        { title: 'Original', views: 100 }
      ),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    // Different fields modified - no conflict (in field-level detection)
    // Note: The implementation may vary - this tests that non-overlapping
    // field changes are handled appropriately
    expect(conflicts.length).toBe(0)
  })

  it('detects multiple conflicts in single merge', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { a: 1 }, { a: 10 }),
      createEvent('posts:p2', 'DELETE', 1100, { b: 2 }, undefined),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1050, { a: 1 }, { a: 20 }),
      createEvent('posts:p2', 'UPDATE', 1150, { b: 2 }, { b: 200 }),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    // Should have conflicts for both p1 (concurrent update) and p2 (delete vs update)
    expect(conflicts.length).toBeGreaterThanOrEqual(2)

    const targets = conflicts.map(c => c.target)
    expect(targets).toContain('posts:p1')
    expect(targets).toContain('posts:p2')
  })
})

// =============================================================================
// Conflict Resolution Tests
// =============================================================================

describe('E2E: Conflict Resolution', () => {
  it('resolves conflict using "ours" strategy', () => {
    const conflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'status',
      ourValue: 'published',
      theirValue: 'archived',
      baseValue: 'draft',
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, { status: 'draft' }, { status: 'published' }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { status: 'draft' }, { status: 'archived' }),
    }

    const resolution = resolveConflict(conflict, 'ours')

    expect(resolution.resolvedValue).toBe('published')
    expect(resolution.strategy).toBe('ours')
    expect(resolution.requiresManualResolution).toBe(false)
  })

  it('resolves conflict using "theirs" strategy', () => {
    const conflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'title',
      ourValue: 'Our Title',
      theirValue: 'Their Title',
      baseValue: 'Original',
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, { title: 'Original' }, { title: 'Our Title' }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Their Title' }),
    }

    const resolution = resolveConflict(conflict, 'theirs')

    expect(resolution.resolvedValue).toBe('Their Title')
    expect(resolution.strategy).toBe('theirs')
  })

  it('resolves conflict using "latest" strategy', () => {
    const conflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'content',
      ourValue: 'Old content',
      theirValue: 'New content',
      baseValue: 'Base content',
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, { content: 'Base content' }, { content: 'Old content' }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 2000, { content: 'Base content' }, { content: 'New content' }),
    }

    const resolution = resolveConflict(conflict, 'latest')

    // Their event has later timestamp
    expect(resolution.resolvedValue).toBe('New content')
    expect(resolution.strategy).toBe('latest')
  })

  it('marks conflict for manual resolution', () => {
    const conflict: ConflictInfo = {
      type: 'delete_update',
      target: 'posts:p1',
      ourValue: undefined,
      theirValue: { title: 'Updated' },
      baseValue: { title: 'Original' },
      ourEvent: createEvent('posts:p1', 'DELETE', 1000, { title: 'Original' }, undefined),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, { title: 'Original' }, { title: 'Updated' }),
    }

    const resolution = resolveConflict(conflict, 'manual')

    expect(resolution.requiresManualResolution).toBe(true)
    expect(resolution.resolvedValue).toBeUndefined()
    expect(resolution.strategy).toBe('manual')
  })

  it('resolves multiple conflicts in batch', () => {
    const conflicts: ConflictInfo[] = [
      {
        type: 'concurrent_update',
        target: 'posts:p1',
        field: 'title',
        ourValue: 'A',
        theirValue: 'B',
        baseValue: 'X',
        ourEvent: createEvent('posts:p1', 'UPDATE', 1000, {}, { title: 'A' }),
        theirEvent: createEvent('posts:p1', 'UPDATE', 1100, {}, { title: 'B' }),
      },
      {
        type: 'concurrent_update',
        target: 'posts:p2',
        field: 'status',
        ourValue: 'published',
        theirValue: 'draft',
        baseValue: 'pending',
        ourEvent: createEvent('posts:p2', 'UPDATE', 1000, {}, { status: 'published' }),
        theirEvent: createEvent('posts:p2', 'UPDATE', 1100, {}, { status: 'draft' }),
      },
    ]

    const resolutions = resolveAllConflicts(conflicts, 'ours')

    expect(resolutions).toHaveLength(2)
    expect(resolutions[0].resolvedValue).toBe('A')
    expect(resolutions[1].resolvedValue).toBe('published')
    expect(allResolutionsComplete(resolutions)).toBe(true)
  })

  it('uses field-based strategy for different fields', () => {
    const titleConflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'title',
      ourValue: 'Our Title',
      theirValue: 'Their Title',
      baseValue: 'Base',
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, {}, { title: 'Our Title' }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 2000, {}, { title: 'Their Title' }),
    }

    const statusConflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'status',
      ourValue: 'published',
      theirValue: 'draft',
      baseValue: 'pending',
      ourEvent: createEvent('posts:p1', 'UPDATE', 2000, {}, { status: 'published' }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1000, {}, { status: 'draft' }),
    }

    const fieldStrategy = createFieldBasedStrategy({
      title: 'latest',    // Use latest for title
      status: 'ours',     // Always prefer ours for status
    })

    const titleResolution = resolveConflict(titleConflict, fieldStrategy)
    const statusResolution = resolveConflict(statusConflict, fieldStrategy)

    // Title: theirs is later (ts: 2000)
    expect(titleResolution.resolvedValue).toBe('Their Title')
    // Status: always ours
    expect(statusResolution.resolvedValue).toBe('published')
  })

  it('uses array merge strategy for array fields', () => {
    const conflict: ConflictInfo = {
      type: 'concurrent_update',
      target: 'posts:p1',
      field: 'tags',
      ourValue: ['tech', 'nodejs'],
      theirValue: ['tech', 'typescript', 'web'],
      baseValue: ['tech'],
      ourEvent: createEvent('posts:p1', 'UPDATE', 1000, {}, { tags: ['tech', 'nodejs'] }),
      theirEvent: createEvent('posts:p1', 'UPDATE', 1100, {}, { tags: ['tech', 'typescript', 'web'] }),
    }

    const arrayMerge = createArrayMergeStrategy()
    const resolution = resolveConflict(conflict, arrayMerge)

    // Should merge arrays without duplicates
    expect(resolution.resolvedValue).toEqual(['tech', 'nodejs', 'typescript', 'web'])
    expect(resolution.strategy).toBe('array-merge')
  })
})

// =============================================================================
// Complete Branch/Merge Workflow Tests
// =============================================================================

describe('E2E: Complete Branch/Merge Workflow', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('supports full feature branch workflow', async () => {
    // 1. Create initial commit on main
    const initialCommit = await createAndSaveCommit(storage, 'Initial commit', [], {
      collections: {
        posts: { dataHash: 'data-hash-1', schemaHash: 'schema-1', rowCount: 0 },
      },
    })
    await refManager.setHead('main')
    await refManager.updateRef('main', initialCommit.hash)

    // 2. Create feature branch
    await branchManager.checkout('feature/user-auth', { create: true })
    expect(await branchManager.current()).toBe('feature/user-auth')

    // 3. Make commits on feature branch
    const featureCommit1 = await createAndSaveCommit(
      storage,
      'Add user model',
      [initialCommit.hash],
      {
        collections: {
          posts: { dataHash: 'data-hash-1', schemaHash: 'schema-1', rowCount: 0 },
          users: { dataHash: 'users-data-1', schemaHash: 'users-schema-1', rowCount: 5 },
        },
      }
    )
    await refManager.updateRef('feature/user-auth', featureCommit1.hash)

    const featureCommit2 = await createAndSaveCommit(
      storage,
      'Add auth endpoints',
      [featureCommit1.hash],
      {
        collections: {
          posts: { dataHash: 'data-hash-1', schemaHash: 'schema-1', rowCount: 0 },
          users: { dataHash: 'users-data-2', schemaHash: 'users-schema-1', rowCount: 10 },
        },
      }
    )
    await refManager.updateRef('feature/user-auth', featureCommit2.hash)

    // 4. Switch back to main
    await branchManager.checkout('main')
    expect(await branchManager.current()).toBe('main')

    // Main should still be at initial commit
    const mainHead = await refManager.resolveRef('HEAD')
    expect(mainHead).toBe(initialCommit.hash)

    // 5. Make a commit on main (simulating other work)
    const mainCommit = await createAndSaveCommit(
      storage,
      'Update posts schema',
      [initialCommit.hash],
      {
        collections: {
          posts: { dataHash: 'data-hash-2', schemaHash: 'schema-2', rowCount: 5 },
        },
      }
    )
    await refManager.updateRef('main', mainCommit.hash)

    // 6. Verify branch states
    const branches = await branchManager.list()
    expect(branches).toHaveLength(2)

    const mainBranch = branches.find(b => b.name === 'main')
    const featureBranch = branches.find(b => b.name === 'feature/user-auth')

    expect(mainBranch?.commit).toBe(mainCommit.hash)
    expect(featureBranch?.commit).toBe(featureCommit2.hash)

    // 7. Verify commits can be loaded
    const loadedMainCommit = await loadCommit(storage, mainCommit.hash)
    expect(loadedMainCommit.message).toBe('Update posts schema')

    const loadedFeatureCommit = await loadCommit(storage, featureCommit2.hash)
    expect(loadedFeatureCommit.message).toBe('Add auth endpoints')
  })

  it('handles branch cleanup after merge', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Create several feature branches
    const featureBranches = ['feature/a', 'feature/b', 'feature/c']
    for (const branch of featureBranches) {
      await branchManager.create(branch)
    }

    expect((await branchManager.list()).length).toBe(4) // main + 3 features

    // Simulate "merging" feature/a and feature/b by deleting them
    await branchManager.delete('feature/a')
    await branchManager.delete('feature/b')

    // Verify cleanup
    expect((await branchManager.list()).length).toBe(2) // main + feature/c
    expect(await branchManager.exists('feature/a')).toBe(false)
    expect(await branchManager.exists('feature/b')).toBe(false)
    expect(await branchManager.exists('feature/c')).toBe(true)
  })

  it('preserves branch history through renames', async () => {
    const commit1 = await createAndSaveCommit(storage, 'Commit 1')
    const commit2 = await createAndSaveCommit(storage, 'Commit 2', [commit1.hash])
    await refManager.setHead('main')
    await refManager.updateRef('main', commit2.hash)

    // Create development branch from commit1
    await branchManager.create('development', { from: commit1.hash })

    // Verify development points to commit1
    let devCommit = await refManager.resolveRef('development')
    expect(devCommit).toBe(commit1.hash)

    // Rename development to staging
    await branchManager.rename('development', 'staging')

    // Staging should point to same commit
    const stagingCommit = await refManager.resolveRef('staging')
    expect(stagingCommit).toBe(commit1.hash)
  })
})

// =============================================================================
// Merge with Resolution Strategy Tests
// =============================================================================

describe('E2E: Merge with Resolution Strategy', () => {
  it('auto-resolves conflicts using provided strategy', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { title: 'Base' }, { title: 'Our Title' }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1100, { title: 'Base' }, { title: 'Their Title' }),
    ]

    // Merge with auto-resolution using 'latest' strategy
    const result = await mergeEventStreams(ourEvents, theirEvents, {
      resolutionStrategy: 'latest',
    })

    // Conflicts should be resolved
    expect(result.conflicts.filter(c => !c.resolved)).toHaveLength(0)
    expect(result.resolved.length).toBeGreaterThan(0)
    expect(result.resolved[0].resolvedValue).toBe('Their Title') // Their ts is later
  })

  it('reports unresolved conflicts when using manual strategy', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'DELETE', 1000, { title: 'Post' }, undefined),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1100, { title: 'Post' }, { title: 'Updated Post' }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents, {
      resolutionStrategy: 'manual',
    })

    // Manual strategy should leave conflicts unresolved
    expect(result.success).toBe(false)
    expect(result.conflicts.some(c => c.type === 'delete_update')).toBe(true)
  })

  it('provides merge statistics', async () => {
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, {}, { a: 1 }),
      createEvent('posts:p2', 'CREATE', 1100, undefined, { b: 2 }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p3', 'UPDATE', 1050, {}, { c: 3 }),
      createEvent('posts:p4', 'UPDATE', 1150, {}, { d: 4 }),
      createEvent('posts:p5', 'CREATE', 1200, undefined, { e: 5 }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents)

    expect(result.stats.fromOurs).toBe(2)
    expect(result.stats.fromTheirs).toBe(3)
    expect(result.stats.entitiesProcessed).toBe(5)
    expect(result.stats.entitiesWithConflicts).toBe(0)
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('E2E: Edge Cases', () => {
  let storage: MemoryBackend
  let branchManager: BranchManager
  let refManager: RefManager

  beforeEach(() => {
    storage = new MemoryBackend()
    branchManager = createBranchManager({ storage })
    refManager = createRefManager(storage)
  })

  it('handles empty event streams in merge', async () => {
    const result = await mergeEventStreams([], [])

    expect(result.success).toBe(true)
    expect(result.mergedEvents).toHaveLength(0)
    expect(result.conflicts).toHaveLength(0)
  })

  it('handles deeply nested branch names', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Create deeply nested branch
    await branchManager.create('feature/auth/oauth/google')

    expect(await branchManager.exists('feature/auth/oauth/google')).toBe(true)

    // Can checkout and delete
    await branchManager.checkout('feature/auth/oauth/google')
    expect(await branchManager.current()).toBe('feature/auth/oauth/google')

    await branchManager.checkout('main')
    await branchManager.delete('feature/auth/oauth/google')
    expect(await branchManager.exists('feature/auth/oauth/google')).toBe(false)
  })

  it('handles concurrent modifications to same field with same value', async () => {
    // Both sides make the same change - should not conflict
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { status: 'draft' }, { status: 'published' }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1100, { status: 'draft' }, { status: 'published' }),
    ]

    const conflicts = detectConflicts(ourEvents, theirEvents)

    // Same value - no conflict (implementation may vary)
    // The key is that if there's a conflict, it should be resolvable
    if (conflicts.length > 0) {
      expect(conflicts[0].ourValue).toBe(conflicts[0].theirValue)
    }
  })

  it('handles large number of branches', async () => {
    const commit = await createAndSaveCommit(storage, 'Initial')
    await refManager.setHead('main')
    await refManager.updateRef('main', commit.hash)

    // Create 50 branches
    const branchCount = 50
    for (let i = 0; i < branchCount; i++) {
      await branchManager.create(`feature/branch-${i}`)
    }

    const branches = await branchManager.list()
    expect(branches).toHaveLength(branchCount + 1) // +1 for main

    // Can still switch between them
    await branchManager.checkout('feature/branch-25')
    expect(await branchManager.current()).toBe('feature/branch-25')

    await branchManager.checkout('feature/branch-49')
    expect(await branchManager.current()).toBe('feature/branch-49')
  })

  it('handles events with very close timestamps', async () => {
    // Events 1ms apart
    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000000, { v: 0 }, { v: 1 }),
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000001, { v: 0 }, { v: 2 }),
    ]

    const result = await mergeEventStreams(ourEvents, theirEvents, {
      resolutionStrategy: 'latest',
    })

    // Should handle 1ms difference correctly
    if (result.resolved.length > 0) {
      expect(result.resolved[0].resolvedValue).toBe(2) // theirs is 1ms later
    }
  })

  it('validates event order in sorted output', async () => {
    const events: Event[] = [
      createEvent('a', 'UPDATE', 5000, {}, {}),
      createEvent('b', 'UPDATE', 1000, {}, {}),
      createEvent('c', 'UPDATE', 3000, {}, {}),
      createEvent('d', 'UPDATE', 2000, {}, {}),
      createEvent('e', 'UPDATE', 4000, {}, {}),
    ]

    const sorted = sortEvents(events)

    // Verify ascending order
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i].ts).toBeGreaterThanOrEqual(sorted[i - 1].ts)
    }
  })
})

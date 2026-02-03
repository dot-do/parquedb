/**
 * Tests for merge CLI commands
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  createMergeState,
  saveMergeState,
  loadMergeState,
  clearMergeState,
  hasMergeInProgress,
  resolveConflict,
  getUnresolvedConflicts,
  allConflictsResolved,
  getConflictsByPattern,
  addConflict,
} from '../../../src/sync/merge-state'
import { createBranchManager } from '../../../src/sync/branch-manager'
import { createRefManager } from '../../../src/sync/refs'
import { createCommit, saveCommit } from '../../../src/sync/commit'

describe('Merge State Management', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  afterEach(async () => {
    // Clean up
    await clearMergeState(storage)
  })

  describe('createMergeState', () => {
    it('should create merge state with required fields', () => {
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      expect(state.source).toBe('feature')
      expect(state.target).toBe('main')
      expect(state.baseCommit).toBe('abc123')
      expect(state.sourceCommit).toBe('def456')
      expect(state.targetCommit).toBe('ghi789')
      expect(state.status).toBe('in_progress')
      expect(state.strategy).toBe('manual')
      expect(state.conflicts).toEqual([])
    })

    it('should accept custom strategy', () => {
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
        strategy: 'ours',
      })

      expect(state.strategy).toBe('ours')
    })
  })

  describe('saveMergeState and loadMergeState', () => {
    it('should save and load merge state', async () => {
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      await saveMergeState(storage, state)
      const loaded = await loadMergeState(storage)

      expect(loaded).not.toBeNull()
      expect(loaded?.source).toBe('feature')
      expect(loaded?.target).toBe('main')
      expect(loaded?.baseCommit).toBe('abc123')
    })

    it('should return null if no merge state exists', async () => {
      const loaded = await loadMergeState(storage)
      expect(loaded).toBeNull()
    })
  })

  describe('clearMergeState', () => {
    it('should clear merge state', async () => {
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      await saveMergeState(storage, state)
      expect(await hasMergeInProgress(storage)).toBe(true)

      await clearMergeState(storage)
      expect(await hasMergeInProgress(storage)).toBe(false)
    })
  })

  describe('hasMergeInProgress', () => {
    it('should return false when no merge state', async () => {
      expect(await hasMergeInProgress(storage)).toBe(false)
    })

    it('should return true when merge state exists', async () => {
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      await saveMergeState(storage, state)
      expect(await hasMergeInProgress(storage)).toBe(true)
    })
  })

  describe('Conflict Management', () => {
    it('should add conflicts to state', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })

      expect(state.conflicts).toHaveLength(1)
      expect(state.status).toBe('conflicted')
      expect(state.conflicts[0]?.entityId).toBe('posts/1')
    })

    it('should resolve conflicts with strategy', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })

      state = resolveConflict(state, 'posts/1', 'ours')

      expect(state.conflicts[0]?.resolved).toBe(true)
      expect(state.conflicts[0]?.resolution).toBe('ours')
      expect(state.status).toBe('resolved')
    })

    it('should resolve conflicts with manual value', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })

      state = resolveConflict(state, 'posts/1', {
        strategy: 'manual',
        value: 'Merged Title',
      })

      expect(state.conflicts[0]?.resolved).toBe(true)
      expect(state.conflicts[0]?.resolution).toBe('manual')
      expect(state.conflicts[0]?.resolvedValue).toBe('Merged Title')
    })

    it('should get unresolved conflicts', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
      })

      state = addConflict(state, {
        entityId: 'posts/2',
        collection: 'posts',
        fields: ['content'],
        resolved: false,
      })

      state = resolveConflict(state, 'posts/1', 'ours')

      const unresolved = getUnresolvedConflicts(state)
      expect(unresolved).toHaveLength(1)
      expect(unresolved[0]?.entityId).toBe('posts/2')
    })

    it('should check if all conflicts are resolved', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      expect(allConflictsResolved(state)).toBe(false)

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
      })

      expect(allConflictsResolved(state)).toBe(false)

      state = resolveConflict(state, 'posts/1', 'ours')

      expect(allConflictsResolved(state)).toBe(true)
    })

    it('should get conflicts by pattern', () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
      })

      state = addConflict(state, {
        entityId: 'posts/2',
        collection: 'posts',
        fields: ['content'],
        resolved: false,
      })

      state = addConflict(state, {
        entityId: 'users/1',
        collection: 'users',
        fields: ['name'],
        resolved: false,
      })

      // Exact match
      let matches = getConflictsByPattern(state, 'posts/1')
      expect(matches).toHaveLength(1)
      expect(matches[0]?.entityId).toBe('posts/1')

      // Wildcard match
      matches = getConflictsByPattern(state, 'posts/*')
      expect(matches).toHaveLength(2)

      // All match
      matches = getConflictsByPattern(state, '*')
      expect(matches).toHaveLength(3)
    })
  })

  describe('Integration with Branch Manager', () => {
    it('should create merge state for branch merge', async () => {
      const branchManager = createBranchManager({ storage })
      const refManager = createRefManager(storage)

      // Create initial commit
      const commit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        {
          message: 'Initial commit',
          author: 'test',
        }
      )

      await saveCommit(storage, commit)
      await refManager.updateRef('main', commit.hash)
      await refManager.setHead('main')

      // Create feature branch
      await branchManager.create('feature')

      // Create merge state
      const state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: commit.hash,
        sourceCommit: commit.hash,
        targetCommit: commit.hash,
      })

      await saveMergeState(storage, state)

      // Verify
      const loaded = await loadMergeState(storage)
      expect(loaded).not.toBeNull()
      expect(loaded?.source).toBe('feature')
      expect(loaded?.target).toBe('main')
    })
  })

  describe('Merge State Persistence', () => {
    it('should persist conflict resolution state', async () => {
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: 'abc123',
        sourceCommit: 'def456',
        targetCommit: 'ghi789',
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })

      await saveMergeState(storage, state)

      // Simulate resolving in another session
      let loaded = await loadMergeState(storage)
      expect(loaded).not.toBeNull()
      expect(loaded?.conflicts[0]?.resolved).toBe(false)

      loaded = resolveConflict(loaded!, 'posts/1', 'ours')
      await saveMergeState(storage, loaded)

      // Load again to verify persistence
      loaded = await loadMergeState(storage)
      expect(loaded?.conflicts[0]?.resolved).toBe(true)
      expect(loaded?.conflicts[0]?.resolution).toBe('ours')
    })
  })

  describe('Merge Completion (continueMerge)', () => {
    it('should create merge commit with both parents after resolving conflicts', async () => {
      const { applyMergeAndCommit } = await import('../../../src/sync/merge-commit')
      const refManager = createRefManager(storage)

      // Create base commit
      const baseCommit = await createCommit(
        {
          collections: { posts: { dataHash: 'base-data', schemaHash: 'base-schema', rowCount: 1 } },
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        { message: 'Base commit', author: 'test' }
      )
      await saveCommit(storage, baseCommit)

      // Create source commit (branched from base)
      const sourceCommit = await createCommit(
        {
          collections: { posts: { dataHash: 'source-data', schemaHash: 'source-schema', rowCount: 2 } },
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 10 },
        },
        { message: 'Source changes', author: 'test', parents: [baseCommit.hash] }
      )
      await saveCommit(storage, sourceCommit)

      // Create target commit (main branch advanced from base)
      const targetCommit = await createCommit(
        {
          collections: { posts: { dataHash: 'target-data', schemaHash: 'target-schema', rowCount: 3 } },
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 20 },
        },
        { message: 'Target changes', author: 'test', parents: [baseCommit.hash] }
      )
      await saveCommit(storage, targetCommit)

      // Set up refs
      await refManager.updateRef('main', targetCommit.hash)
      await refManager.updateRef('feature', sourceCommit.hash)
      await refManager.setHead('main')

      // Create merge state with resolved conflict
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: baseCommit.hash,
        sourceCommit: sourceCommit.hash,
        targetCommit: targetCommit.hash,
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: true,
        resolution: 'ours',
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })
      state.status = 'resolved'

      await saveMergeState(storage, state)

      // Apply merge and create commit
      const mergeCommit = await applyMergeAndCommit(storage, state, {
        message: 'Merge feature into main',
        author: 'test-user',
      })

      // Verify merge commit has two parents
      expect(mergeCommit.parents).toHaveLength(2)
      expect(mergeCommit.parents).toContain(targetCommit.hash)
      expect(mergeCommit.parents).toContain(sourceCommit.hash)

      // Verify merge commit message
      expect(mergeCommit.message).toBe('Merge feature into main')
      expect(mergeCommit.author).toBe('test-user')

      // Verify main branch now points to merge commit
      const mainRef = await refManager.resolveRef('main')
      expect(mainRef).toBe(mergeCommit.hash)
    })

    it('should apply resolved conflict values in merge commit state', async () => {
      const { applyMergeAndCommit, getResolvedValue } = await import('../../../src/sync/merge-commit')

      // Create base commit
      const baseCommit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        { message: 'Base commit', author: 'test' }
      )
      await saveCommit(storage, baseCommit)

      // Create source and target commits
      const sourceCommit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 10 },
        },
        { message: 'Source', author: 'test', parents: [baseCommit.hash] }
      )
      await saveCommit(storage, sourceCommit)

      const targetCommit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 20 },
        },
        { message: 'Target', author: 'test', parents: [baseCommit.hash] }
      )
      await saveCommit(storage, targetCommit)

      // Create merge state with different resolution strategies
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: baseCommit.hash,
        sourceCommit: sourceCommit.hash,
        targetCommit: targetCommit.hash,
      })

      // Test 'ours' resolution
      const oursConflict = {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: true,
        resolution: 'ours' as const,
        ourValue: 'Our Value',
        theirValue: 'Their Value',
      }
      expect(getResolvedValue(oursConflict)).toBe('Our Value')

      // Test 'theirs' resolution
      const theirsConflict = {
        entityId: 'posts/2',
        collection: 'posts',
        fields: ['content'],
        resolved: true,
        resolution: 'theirs' as const,
        ourValue: 'Our Content',
        theirValue: 'Their Content',
      }
      expect(getResolvedValue(theirsConflict)).toBe('Their Content')

      // Test 'manual' resolution with custom value
      const manualConflict = {
        entityId: 'posts/3',
        collection: 'posts',
        fields: ['status'],
        resolved: true,
        resolution: 'manual' as const,
        resolvedValue: 'Custom Merged Value',
        ourValue: 'Our Status',
        theirValue: 'Their Status',
      }
      expect(getResolvedValue(manualConflict)).toBe('Custom Merged Value')
    })

    it('should throw if merge state has unresolved conflicts', async () => {
      const { applyMergeAndCommit } = await import('../../../src/sync/merge-commit')

      // Create base commit
      const baseCommit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        { message: 'Base commit', author: 'test' }
      )
      await saveCommit(storage, baseCommit)

      // Create merge state with unresolved conflict
      let state = createMergeState({
        source: 'feature',
        target: 'main',
        baseCommit: baseCommit.hash,
        sourceCommit: baseCommit.hash,
        targetCommit: baseCommit.hash,
      })

      state = addConflict(state, {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['title'],
        resolved: false,
        ourValue: 'Our Title',
        theirValue: 'Their Title',
      })

      await saveMergeState(storage, state)

      // Should throw because conflicts are not resolved
      await expect(
        applyMergeAndCommit(storage, state, { message: 'Merge', author: 'test' })
      ).rejects.toThrow('Cannot complete merge: 1 unresolved conflict')
    })

    it('should use newest value when resolution is newest', async () => {
      const { getResolvedValue } = await import('../../../src/sync/merge-commit')

      // Create conflict with timestamps in values
      const newestConflict = {
        entityId: 'posts/1',
        collection: 'posts',
        fields: ['updatedAt'],
        resolved: true,
        resolution: 'newest' as const,
        ourValue: { value: 'our', ts: 1000 },
        theirValue: { value: 'their', ts: 2000 },
      }

      // 'newest' should pick the value with the higher timestamp
      // For this test, we check that the function handles the newest resolution type
      const result = getResolvedValue(newestConflict)
      expect(result).toEqual({ value: 'their', ts: 2000 })
    })
  })
})

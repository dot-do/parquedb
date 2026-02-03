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
})

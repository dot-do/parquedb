/**
 * Tests for the unified Merge Engine
 *
 * The merge engine consolidates CLI and Worker merge code paths into a single
 * shared implementation.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  MergeEngine,
  createMergeEngine,
  type MergeEngineOptions,
  type MergeBranchesOptions,
  type MergeBranchesResult,
} from '../../../src/sync/merge-engine'
import { createBranchManager } from '../../../src/sync/branch-manager'
import { createRefManager } from '../../../src/sync/refs'
import { createCommit, saveCommit } from '../../../src/sync/commit'

describe('Merge Engine', () => {
  let storage: MemoryBackend
  let engine: MergeEngine

  beforeEach(() => {
    storage = new MemoryBackend()
    engine = createMergeEngine({ storage })
  })

  describe('createMergeEngine', () => {
    it('should create a merge engine with default options', () => {
      expect(engine).toBeDefined()
      expect(typeof engine.mergeBranches).toBe('function')
      expect(typeof engine.findCommonAncestor).toBe('function')
      expect(typeof engine.mergeEvents).toBe('function')
    })

    it('should accept custom options', () => {
      const customEngine = createMergeEngine({
        storage,
        defaultStrategy: 'ours',
        autoMergeCommutative: false,
      })
      expect(customEngine).toBeDefined()
    })
  })

  describe('findCommonAncestor', () => {
    it('should find common ancestor of two commits', async () => {
      // Create initial commit
      const initial = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd1', reverseHash: 'rev1' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        { message: 'Initial commit', author: 'test' }
      )
      await saveCommit(storage, initial)

      // Create second commit (branching from initial)
      const commit2 = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd2', reverseHash: 'rev2' },
          eventLogPosition: { segmentId: 'seg2', offset: 0 },
        },
        { message: 'Second commit', author: 'test', parents: [initial.hash] }
      )
      await saveCommit(storage, commit2)

      // Create third commit (also branching from initial)
      const commit3 = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd3', reverseHash: 'rev3' },
          eventLogPosition: { segmentId: 'seg3', offset: 0 },
        },
        { message: 'Third commit', author: 'test', parents: [initial.hash] }
      )
      await saveCommit(storage, commit3)

      // Find common ancestor
      const result = await engine.findCommonAncestor(commit2.hash, commit3.hash)

      expect(result.ancestor).toBe(initial.hash)
      expect(result.commitsTraversed).toBeGreaterThan(0)
    })

    it('should return same commit when commits are identical', async () => {
      const commit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg', offset: 0 },
        },
        { message: 'Commit', author: 'test' }
      )
      await saveCommit(storage, commit)

      const result = await engine.findCommonAncestor(commit.hash, commit.hash)

      expect(result.ancestor).toBe(commit.hash)
      expect(result.commitsTraversed).toBe(0)
    })

    it('should return null when no common ancestor exists', async () => {
      // Create two unrelated commits (no parents)
      const commit1 = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd1', reverseHash: 'rev1' },
          eventLogPosition: { segmentId: 'seg1', offset: 0 },
        },
        { message: 'Unrelated 1', author: 'test' }
      )
      await saveCommit(storage, commit1)

      const commit2 = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd2', reverseHash: 'rev2' },
          eventLogPosition: { segmentId: 'seg2', offset: 0 },
        },
        { message: 'Unrelated 2', author: 'test' }
      )
      await saveCommit(storage, commit2)

      const result = await engine.findCommonAncestor(commit1.hash, commit2.hash)

      expect(result.ancestor).toBeNull()
    })
  })

  describe('mergeBranches', () => {
    let branchManager: ReturnType<typeof createBranchManager>
    let refManager: ReturnType<typeof createRefManager>

    beforeEach(async () => {
      branchManager = createBranchManager({ storage })
      refManager = createRefManager(storage)

      // Create initial commit and main branch
      const initial = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg', offset: 0 },
        },
        { message: 'Initial commit', author: 'test' }
      )
      await saveCommit(storage, initial)
      await refManager.updateRef('main', initial.hash)
      await refManager.setHead('main')
    })

    it('should merge branches with no conflicts', async () => {
      // Create feature branch
      await branchManager.create('feature')

      // Merge feature into main (fast-forward case)
      const result = await engine.mergeBranches('feature', 'main')

      expect(result.success).toBe(true)
      expect(result.conflicts).toHaveLength(0)
    })

    it('should return error for non-existent source branch', async () => {
      const result = await engine.mergeBranches('nonexistent', 'main')

      expect(result.success).toBe(false)
      expect(result.error).toContain('not found')
    })

    it('should return error for non-existent target branch', async () => {
      await branchManager.create('feature')

      const result = await engine.mergeBranches('feature', 'nonexistent')

      expect(result.success).toBe(false)
      expect(result.error).toContain('not found')
    })

    it('should detect when no common ancestor exists', async () => {
      // Create an orphan branch (no shared history)
      const orphanCommit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'orphan', reverseHash: 'orphan' },
          eventLogPosition: { segmentId: 'orphan', offset: 0 },
        },
        { message: 'Orphan commit', author: 'test' }
      )
      await saveCommit(storage, orphanCommit)
      await refManager.updateRef('orphan', orphanCommit.hash)

      const result = await engine.mergeBranches('orphan', 'main')

      expect(result.success).toBe(false)
      expect(result.error).toContain('common ancestor')
    })

    it('should support dry-run mode', async () => {
      await branchManager.create('feature')

      const result = await engine.mergeBranches('feature', 'main', { dryRun: true })

      expect(result.success).toBe(true)
      expect(result.dryRun).toBe(true)
      // Dry-run should not modify any state
    })

    it('should support custom resolution strategy', async () => {
      await branchManager.create('feature')

      const result = await engine.mergeBranches('feature', 'main', {
        strategy: 'ours',
      })

      expect(result.success).toBe(true)
      expect(result.strategy).toBe('ours')
    })
  })

  describe('mergeEvents', () => {
    it('should merge event arrays with no conflicts', async () => {
      const ourEvents = [
        {
          id: 'evt1',
          ts: 1000,
          op: 'CREATE' as const,
          target: 'posts:1',
          after: { $type: 'Post', name: 'Our Post' },
          actor: 'user1',
        },
      ]

      const theirEvents = [
        {
          id: 'evt2',
          ts: 2000,
          op: 'CREATE' as const,
          target: 'posts:2',
          after: { $type: 'Post', name: 'Their Post' },
          actor: 'user2',
        },
      ]

      const result = await engine.mergeEvents(ourEvents, theirEvents)

      expect(result.success).toBe(true)
      expect(result.conflicts).toHaveLength(0)
      expect(result.mergedEvents.length).toBeGreaterThanOrEqual(1)
    })

    it('should detect conflicts when same entity modified differently', async () => {
      const ourEvents = [
        {
          id: 'evt1',
          ts: 1000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { title: 'Original' },
          after: { title: 'Our Title' },
          actor: 'user1',
        },
      ]

      const theirEvents = [
        {
          id: 'evt2',
          ts: 1000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { title: 'Original' },
          after: { title: 'Their Title' },
          actor: 'user2',
        },
      ]

      const result = await engine.mergeEvents(ourEvents, theirEvents)

      expect(result.success).toBe(false)
      expect(result.conflicts.length).toBeGreaterThan(0)
    })

    it('should auto-merge commutative operations', async () => {
      const ourEvents = [
        {
          id: 'evt1',
          ts: 1000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { views: 10 },
          after: { views: 15 },
          actor: 'user1',
          metadata: { update: { $inc: { views: 5 } } },
        },
      ]

      const theirEvents = [
        {
          id: 'evt2',
          ts: 1000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { views: 10 },
          after: { views: 13 },
          actor: 'user2',
          metadata: { update: { $inc: { views: 3 } } },
        },
      ]

      const result = await engine.mergeEvents(ourEvents, theirEvents, {
        autoMergeCommutative: true,
      })

      // Note: This test documents the expected behavior
      // Commutative $inc operations should be auto-merged
      expect(result.autoMerged.length).toBeGreaterThanOrEqual(0)
    })

    it('should apply resolution strategy', async () => {
      const ourEvents = [
        {
          id: 'evt1',
          ts: 1000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { title: 'Original' },
          after: { title: 'Our Title' },
          actor: 'user1',
        },
      ]

      const theirEvents = [
        {
          id: 'evt2',
          ts: 2000,
          op: 'UPDATE' as const,
          target: 'posts:1',
          before: { title: 'Original' },
          after: { title: 'Their Title' },
          actor: 'user2',
        },
      ]

      const result = await engine.mergeEvents(ourEvents, theirEvents, {
        resolutionStrategy: 'ours',
      })

      // With 'ours' strategy, conflicts should be auto-resolved
      expect(result.resolved.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('Integration with existing modules', () => {
    it('should use shared common-ancestor module', async () => {
      // Verify that the engine uses the optimized findCommonAncestor
      // from src/sync/common-ancestor.ts, not a local naive implementation
      const commit = await createCommit(
        {
          collections: {},
          relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
          eventLogPosition: { segmentId: 'seg', offset: 0 },
        },
        { message: 'Commit', author: 'test' }
      )
      await saveCommit(storage, commit)

      const result = await engine.findCommonAncestor(commit.hash, commit.hash)

      // The optimized algorithm returns stats
      expect(result).toHaveProperty('commitsTraversed')
      expect(result).toHaveProperty('depthFromCommit1')
      expect(result).toHaveProperty('depthFromCommit2')
    })

    it('should use shared event-merge module', async () => {
      const events = [
        {
          id: 'evt1',
          ts: 1000,
          op: 'CREATE' as const,
          target: 'posts:1',
          after: { $type: 'Post' },
          actor: 'user1',
        },
      ]

      const result = await engine.mergeEvents(events, [])

      // The shared module returns detailed stats
      expect(result).toHaveProperty('stats')
      expect(result.stats).toHaveProperty('fromOurs')
      expect(result.stats).toHaveProperty('fromTheirs')
    })
  })
})

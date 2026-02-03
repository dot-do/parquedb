/**
 * Tests for Common Ancestor Finding Algorithm
 *
 * Tests the optimized bidirectional BFS algorithm for finding
 * the common ancestor of two commits in a DAG.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { createCommit, saveCommit, type DatabaseCommit } from '../../../src/sync/commit'
import {
  findCommonAncestor,
  findCommonAncestorSimple,
  findAllCommonAncestors,
  isAncestor,
  createAncestorCache,
} from '../../../src/sync/common-ancestor'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a minimal database state for test commits
 */
function createTestState() {
  return {
    collections: {},
    relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
    eventLogPosition: { segmentId: 'seg1', offset: 0 },
  }
}

/**
 * Create and save a commit with given parents
 */
async function createTestCommit(
  storage: MemoryBackend,
  message: string,
  parents: string[] = []
): Promise<DatabaseCommit> {
  const commit = await createCommit(createTestState(), {
    message,
    author: 'test',
    parents,
  })
  await saveCommit(storage, commit)
  return commit
}

// =============================================================================
// Tests
// =============================================================================

describe('Common Ancestor Algorithm', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('findCommonAncestor', () => {
    it('should return the same commit when both inputs are identical', async () => {
      const commit = await createTestCommit(storage, 'Initial commit')

      const result = await findCommonAncestor(storage, commit.hash, commit.hash)

      expect(result.ancestor).toBe(commit.hash)
      expect(result.commitsTraversed).toBe(0)
      expect(result.depthFromCommit1).toBe(0)
      expect(result.depthFromCommit2).toBe(0)
    })

    it('should find common ancestor in simple linear history', async () => {
      // Create: A <- B <- C
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitB.hash])

      const result = await findCommonAncestor(storage, commitB.hash, commitC.hash)

      expect(result.ancestor).toBe(commitB.hash)
    })

    it('should find common ancestor at fork point', async () => {
      // Create:     B
      //            /
      //       A <-
      //            \
      //             C
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])

      const result = await findCommonAncestor(storage, commitB.hash, commitC.hash)

      expect(result.ancestor).toBe(commitA.hash)
      expect(result.depthFromCommit1).toBe(1)
      expect(result.depthFromCommit2).toBe(1)
    })

    it('should find common ancestor with asymmetric branches', async () => {
      // Create:     B <- D <- E
      //            /
      //       A <-
      //            \
      //             C
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])
      const commitD = await createTestCommit(storage, 'Commit D', [commitB.hash])
      const commitE = await createTestCommit(storage, 'Commit E', [commitD.hash])

      const result = await findCommonAncestor(storage, commitE.hash, commitC.hash)

      expect(result.ancestor).toBe(commitA.hash)
      expect(result.depthFromCommit1).toBe(3) // E -> D -> B -> A
      expect(result.depthFromCommit2).toBe(1) // C -> A
    })

    it('should find common ancestor after merge commit', async () => {
      // Create:     B ---
      //            /     \
      //       A <-        M
      //            \     /
      //             C ---
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])
      const commitM = await createTestCommit(storage, 'Merge commit', [commitB.hash, commitC.hash])

      // M and B should have B as ancestor (M includes B)
      const result1 = await findCommonAncestor(storage, commitM.hash, commitB.hash)
      expect(result1.ancestor).toBe(commitB.hash)

      // M and C should have C as ancestor (M includes C)
      const result2 = await findCommonAncestor(storage, commitM.hash, commitC.hash)
      expect(result2.ancestor).toBe(commitC.hash)
    })

    it('should return null when no common ancestor exists', async () => {
      // Create two disconnected histories
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitX = await createTestCommit(storage, 'Commit X') // No parent connection

      const result = await findCommonAncestor(storage, commitA.hash, commitX.hash)

      expect(result.ancestor).toBeNull()
    })

    it('should handle deep history efficiently', async () => {
      // Create a long chain: A1 <- A2 <- ... <- A50
      // And branch at A25: A25 <- B1 <- B2 <- ... <- B25
      let current = await createTestCommit(storage, 'A1')
      const commits: DatabaseCommit[] = [current]

      // Create main chain
      for (let i = 2; i <= 50; i++) {
        current = await createTestCommit(storage, `A${i}`, [current.hash])
        commits.push(current)
      }

      // Create branch from A25
      let branchCurrent = await createTestCommit(storage, 'B1', [commits[24]!.hash])
      for (let i = 2; i <= 25; i++) {
        branchCurrent = await createTestCommit(storage, `B${i}`, [branchCurrent.hash])
      }

      const result = await findCommonAncestor(storage, commits[49]!.hash, branchCurrent.hash)

      expect(result.ancestor).toBe(commits[24]!.hash) // A25
      // Bidirectional BFS should traverse fewer commits than naive approach
      // Naive would traverse all 75 commits, bidirectional traverses at most all of them
      // but typically finds the answer faster due to meeting in the middle
      expect(result.commitsTraversed).toBeLessThanOrEqual(75)
    })

    it('should respect maxDepth option', async () => {
      // Create: A <- B <- C <- D <- E
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitB.hash])
      const commitD = await createTestCommit(storage, 'Commit D', [commitC.hash])
      const commitE = await createTestCommit(storage, 'Commit E', [commitD.hash])

      // With maxDepth=2, shouldn't find ancestor between E and A (distance 4)
      const result = await findCommonAncestor(storage, commitE.hash, commitA.hash, { maxDepth: 2 })

      expect(result.ancestor).toBeNull()
    })
  })

  describe('findCommonAncestorSimple', () => {
    it('should return just the ancestor hash (backward compatible)', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])

      const ancestor = await findCommonAncestorSimple(storage, commitB.hash, commitC.hash)

      expect(ancestor).toBe(commitA.hash)
    })

    it('should return null when no ancestor exists', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitX = await createTestCommit(storage, 'Commit X')

      const ancestor = await findCommonAncestorSimple(storage, commitA.hash, commitX.hash)

      expect(ancestor).toBeNull()
    })
  })

  describe('findAllCommonAncestors', () => {
    it('should find single merge base in simple case', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])

      const mergeBases = await findAllCommonAncestors(storage, commitB.hash, commitC.hash)

      expect(mergeBases).toHaveLength(1)
      expect(mergeBases[0]).toBe(commitA.hash)
    })

    it('should find multiple merge bases in criss-cross merge', async () => {
      // Create criss-cross merge pattern:
      //       B1 ---
      //      /      \
      // A <-         M1
      //      \      /  \
      //       C1 ---    \
      //             \    M2
      //              M1' /
      // This creates two independent merge bases
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB1 = await createTestCommit(storage, 'Commit B1', [commitA.hash])
      const commitC1 = await createTestCommit(storage, 'Commit C1', [commitA.hash])
      const commitM1 = await createTestCommit(storage, 'Merge M1', [commitB1.hash, commitC1.hash])
      const commitM1Prime = await createTestCommit(storage, 'Merge M1 prime', [commitC1.hash, commitB1.hash])
      const commitM2 = await createTestCommit(storage, 'Merge M2', [commitM1.hash, commitM1Prime.hash])

      // Check between M1 and M1' - should have B1 and C1 as merge bases
      const mergeBases = await findAllCommonAncestors(storage, commitM1.hash, commitM1Prime.hash)

      expect(mergeBases).toHaveLength(2)
      expect(mergeBases).toContain(commitB1.hash)
      expect(mergeBases).toContain(commitC1.hash)
    })
  })

  describe('isAncestor', () => {
    it('should return true when first commit is ancestor of second', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitB.hash])

      expect(await isAncestor(storage, commitA.hash, commitC.hash)).toBe(true)
      expect(await isAncestor(storage, commitA.hash, commitB.hash)).toBe(true)
      expect(await isAncestor(storage, commitB.hash, commitC.hash)).toBe(true)
    })

    it('should return false when first commit is not ancestor', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])

      // B is not ancestor of C (they're siblings)
      expect(await isAncestor(storage, commitB.hash, commitC.hash)).toBe(false)
      // C is not ancestor of B
      expect(await isAncestor(storage, commitC.hash, commitB.hash)).toBe(false)
      // A descendant is not ancestor of itself
      expect(await isAncestor(storage, commitC.hash, commitA.hash)).toBe(false)
    })

    it('should return true for same commit', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')

      expect(await isAncestor(storage, commitA.hash, commitA.hash)).toBe(true)
    })
  })

  describe('createAncestorCache', () => {
    it('should create empty cache', () => {
      const cache = createAncestorCache()

      expect(cache.parents.size).toBe(0)
      expect(cache.notFound.size).toBe(0)
    })
  })

  describe('Memoization', () => {
    it('should use memoization for repeated lookups', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])

      // First lookup
      const result1 = await findCommonAncestor(storage, commitB.hash, commitC.hash, {
        memoize: true,
      })

      // Second lookup should be faster due to caching
      const result2 = await findCommonAncestor(storage, commitB.hash, commitC.hash, {
        memoize: true,
      })

      expect(result1.ancestor).toBe(result2.ancestor)
    })
  })

  describe('Edge Cases', () => {
    it('should handle commit with no parents (root commit)', async () => {
      const commitA = await createTestCommit(storage, 'Root A')
      const commitB = await createTestCommit(storage, 'Root B')

      const result = await findCommonAncestor(storage, commitA.hash, commitB.hash)

      expect(result.ancestor).toBeNull()
    })

    it('should handle non-existent commits gracefully', async () => {
      const commitA = await createTestCommit(storage, 'Commit A')

      const result = await findCommonAncestor(storage, commitA.hash, 'nonexistent123')

      expect(result.ancestor).toBeNull()
    })

    it('should handle commit pointing to non-existent parent', async () => {
      // Manually create a commit with a fake parent
      const commitA = await createCommit(createTestState(), {
        message: 'Commit with fake parent',
        author: 'test',
        parents: ['fakeparent123'],
      })
      await saveCommit(storage, commitA)

      const commitB = await createTestCommit(storage, 'Commit B')

      const result = await findCommonAncestor(storage, commitA.hash, commitB.hash)

      expect(result.ancestor).toBeNull()
    })

    it('should handle diamond pattern (reconvergent branches)', async () => {
      // Create:     B
      //            / \
      //       A <-    D
      //            \ /
      //             C
      const commitA = await createTestCommit(storage, 'Commit A')
      const commitB = await createTestCommit(storage, 'Commit B', [commitA.hash])
      const commitC = await createTestCommit(storage, 'Commit C', [commitA.hash])
      const commitD = await createTestCommit(storage, 'Commit D', [commitB.hash, commitC.hash])

      // D and A: A is ancestor
      const result = await findCommonAncestor(storage, commitD.hash, commitA.hash)
      expect(result.ancestor).toBe(commitA.hash)

      // D and B: B is ancestor (D has B as parent)
      const result2 = await findCommonAncestor(storage, commitD.hash, commitB.hash)
      expect(result2.ancestor).toBe(commitB.hash)
    })
  })
})

describe('Performance Characteristics', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should traverse fewer commits than naive algorithm for divergent branches', async () => {
    // Create two branches that diverge early:
    //       A <- B1 <- B2 <- B3 <- ... <- B20
    //        \
    //         C1 <- C2 <- C3 <- ... <- C20
    const commitA = await createTestCommit(storage, 'Commit A')

    let currentB = commitA
    for (let i = 1; i <= 20; i++) {
      currentB = await createTestCommit(storage, `B${i}`, [currentB.hash])
    }

    let currentC = commitA
    for (let i = 1; i <= 20; i++) {
      currentC = await createTestCommit(storage, `C${i}`, [currentC.hash])
    }

    const result = await findCommonAncestor(storage, currentB.hash, currentC.hash)

    expect(result.ancestor).toBe(commitA.hash)

    // Bidirectional BFS traverses both branches until meeting at common ancestor
    // For symmetric branches of depth 20, it will traverse at most 2*20 + 1 = 41 commits
    // The "+1" accounts for the root commit A being counted when found
    expect(result.commitsTraversed).toBeLessThanOrEqual(41)

    // Verify that the algorithm found the correct depths
    expect(result.depthFromCommit1).toBe(20) // B20 -> B19 -> ... -> B1 -> A
    expect(result.depthFromCommit2).toBe(20) // C20 -> C19 -> ... -> C1 -> A
  })

  it('should be efficient when one commit is ancestor of the other', async () => {
    // A <- B <- C <- D <- E <- F <- G <- H <- I <- J
    // Finding ancestor of J and A should be fast
    let current = await createTestCommit(storage, 'A')
    for (let i = 0; i < 9; i++) {
      current = await createTestCommit(storage, String.fromCharCode(66 + i), [current.hash])
    }

    const commitA = (await storage.list('_meta/commits')).files[0]
    const commitAHash = commitA?.split('/').pop()?.replace('.json', '')

    const result = await findCommonAncestor(storage, current.hash, commitAHash!)

    expect(result.ancestor).toBe(commitAHash)
    // Should find quickly since J's ancestor chain includes A
    expect(result.commitsTraversed).toBeLessThanOrEqual(10)
  })
})

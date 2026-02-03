/**
 * Benchmarks for Common Ancestor Algorithm
 *
 * Compares the performance of the optimized bidirectional BFS algorithm
 * against the naive O(n*m) approach.
 *
 * Run with: pnpm bench tests/benchmarks/common-ancestor.bench.ts
 */

import { bench, describe } from 'vitest'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { createCommit, saveCommit, loadCommit, type DatabaseCommit } from '../../src/sync/commit'
import {
  findCommonAncestor,
  findCommonAncestorSimple,
} from '../../src/sync/common-ancestor'

// =============================================================================
// Test Helpers
// =============================================================================

function createTestState() {
  return {
    collections: {},
    relationships: { forwardHash: 'fwd', reverseHash: 'rev' },
    eventLogPosition: { segmentId: 'seg1', offset: 0 },
  }
}

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

/**
 * Naive O(n*m) algorithm (the old implementation)
 */
async function findCommonAncestorNaive(
  storage: MemoryBackend,
  commit1: string,
  commit2: string
): Promise<string | null> {
  const visited1 = new Set<string>()
  const queue1 = [commit1]

  while (queue1.length > 0) {
    const current = queue1.shift()!
    if (visited1.has(current)) continue
    visited1.add(current)

    try {
      const commit = await loadCommit(storage, current)
      queue1.push(...commit.parents)
    } catch {
      // Commit not found, skip
    }
  }

  const queue2 = [commit2]
  const visited2 = new Set<string>()

  while (queue2.length > 0) {
    const current = queue2.shift()!
    if (visited2.has(current)) continue
    visited2.add(current)

    if (visited1.has(current)) {
      return current
    }

    try {
      const commit = await loadCommit(storage, current)
      queue2.push(...commit.parents)
    } catch {
      // Commit not found, skip
    }
  }

  return null
}

// =============================================================================
// Setup Functions
// =============================================================================

interface TestGraph {
  storage: MemoryBackend
  commit1: string
  commit2: string
  ancestor: string
}

/**
 * Create a simple fork: A <- B, A <- C
 */
async function createSimpleFork(): Promise<TestGraph> {
  const storage = new MemoryBackend()
  const commitA = await createTestCommit(storage, 'A')
  const commitB = await createTestCommit(storage, 'B', [commitA.hash])
  const commitC = await createTestCommit(storage, 'C', [commitA.hash])
  return {
    storage,
    commit1: commitB.hash,
    commit2: commitC.hash,
    ancestor: commitA.hash,
  }
}

/**
 * Create asymmetric branches:
 * A <- B1 <- B2 <- ... <- B{depth}
 * A <- C1
 */
async function createAsymmetricBranches(depth: number): Promise<TestGraph> {
  const storage = new MemoryBackend()
  const commitA = await createTestCommit(storage, 'A')

  let currentB = commitA
  for (let i = 1; i <= depth; i++) {
    currentB = await createTestCommit(storage, `B${i}`, [currentB.hash])
  }

  const commitC = await createTestCommit(storage, 'C1', [commitA.hash])

  return {
    storage,
    commit1: currentB.hash,
    commit2: commitC.hash,
    ancestor: commitA.hash,
  }
}

/**
 * Create symmetric divergent branches:
 * A <- B1 <- B2 <- ... <- B{depth}
 * A <- C1 <- C2 <- ... <- C{depth}
 */
async function createSymmetricBranches(depth: number): Promise<TestGraph> {
  const storage = new MemoryBackend()
  const commitA = await createTestCommit(storage, 'A')

  let currentB = commitA
  for (let i = 1; i <= depth; i++) {
    currentB = await createTestCommit(storage, `B${i}`, [currentB.hash])
  }

  let currentC = commitA
  for (let i = 1; i <= depth; i++) {
    currentC = await createTestCommit(storage, `C${i}`, [currentC.hash])
  }

  return {
    storage,
    commit1: currentB.hash,
    commit2: currentC.hash,
    ancestor: commitA.hash,
  }
}

/**
 * Create a long linear chain with branch near end:
 * A1 <- A2 <- ... <- A{depth-branchPoint} <- ... <- A{depth}
 *                          \
 *                           B1 <- B2 <- ... <- B{branchDepth}
 */
async function createLateForkedChain(depth: number, branchPoint: number, branchDepth: number): Promise<TestGraph> {
  const storage = new MemoryBackend()

  let commits: DatabaseCommit[] = []
  let current = await createTestCommit(storage, 'A1')
  commits.push(current)

  for (let i = 2; i <= depth; i++) {
    current = await createTestCommit(storage, `A${i}`, [current.hash])
    commits.push(current)
  }

  // Create branch from branchPoint
  let branchCurrent = commits[branchPoint - 1]!
  for (let i = 1; i <= branchDepth; i++) {
    branchCurrent = await createTestCommit(storage, `B${i}`, [branchCurrent.hash])
  }

  return {
    storage,
    commit1: current.hash,
    commit2: branchCurrent.hash,
    ancestor: commits[branchPoint - 1]!.hash,
  }
}

// =============================================================================
// Benchmarks
// =============================================================================

describe('Common Ancestor - Simple Fork', async () => {
  const graph = await createSimpleFork()

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })
})

describe('Common Ancestor - Asymmetric (50 deep vs 1)', async () => {
  const graph = await createAsymmetricBranches(50)

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })
})

describe('Common Ancestor - Symmetric (25 deep each side)', async () => {
  const graph = await createSymmetricBranches(25)

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })
})

describe('Common Ancestor - Late Fork (100 deep, branch at 90)', async () => {
  const graph = await createLateForkedChain(100, 90, 10)

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })
})

describe('Common Ancestor - Deep History (200 commits)', async () => {
  const graph = await createSymmetricBranches(100)

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized with memoization', async () => {
    const result = await findCommonAncestor(graph.storage, graph.commit1, graph.commit2, {
      memoize: true,
    })
    return result.ancestor
  })
})

describe('Common Ancestor - Very Deep (500 commits each side)', async () => {
  const graph = await createSymmetricBranches(250)

  bench('naive algorithm', async () => {
    await findCommonAncestorNaive(graph.storage, graph.commit1, graph.commit2)
  })

  bench('optimized bidirectional BFS', async () => {
    await findCommonAncestorSimple(graph.storage, graph.commit1, graph.commit2)
  })
})

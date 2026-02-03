/**
 * Common Ancestor Finding Algorithm
 *
 * Optimized algorithm for finding the common ancestor of two commits in a DAG.
 * Uses bidirectional BFS that expands from both commits simultaneously,
 * meeting in the middle for O(sqrt(n)) performance on average.
 *
 * Previous algorithm: O(n*m) - traversed all of commit1's ancestors, then searched commit2's ancestors
 * New algorithm: O(min(n,m)) - bidirectional BFS with early termination
 */

import type { StorageBackend } from '../types/storage'
import { loadCommit, type DatabaseCommit } from './commit'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for common ancestor search
 */
export interface FindCommonAncestorOptions {
  /**
   * Maximum depth to search before giving up.
   * Set to Infinity for unlimited search (default).
   */
  maxDepth?: number | undefined

  /**
   * Enable memoization of parent lookups.
   * Useful when performing multiple ancestor searches on the same commit graph.
   */
  memoize?: boolean | undefined
}

/**
 * Result of common ancestor search
 */
export interface CommonAncestorResult {
  /** The common ancestor commit hash, or null if not found */
  ancestor: string | null

  /** Total commits traversed during search (for benchmarking) */
  commitsTraversed: number

  /** Depth of the common ancestor from commit1 */
  depthFromCommit1?: number | undefined

  /** Depth of the common ancestor from commit2 */
  depthFromCommit2?: number | undefined
}

/**
 * Memoization cache for parent lookups
 */
export interface AncestorCache {
  /** Map of commit hash to parent hashes */
  parents: Map<string, string[]>

  /** Set of commits known to not exist */
  notFound: Set<string>
}

// =============================================================================
// Main Functions
// =============================================================================

/**
 * Find common ancestor of two commits using bidirectional BFS
 *
 * Algorithm:
 * 1. Start BFS from both commits simultaneously
 * 2. Alternate between expanding commit1's frontier and commit2's frontier
 * 3. When a commit is found in both frontiers, it's the common ancestor
 * 4. Early termination provides significant performance gains for close branches
 *
 * Time Complexity: O(min(n, m)) where n and m are distances to common ancestor
 * Space Complexity: O(n + m) for visited sets
 *
 * @param storage Storage backend to load commits from
 * @param commit1 First commit hash
 * @param commit2 Second commit hash
 * @param options Search options
 * @returns Common ancestor result with statistics
 *
 * @example
 * ```typescript
 * const result = await findCommonAncestor(storage, 'abc123', 'def456')
 * if (result.ancestor) {
 *   console.log(`Common ancestor: ${result.ancestor}`)
 *   console.log(`Traversed ${result.commitsTraversed} commits`)
 * }
 * ```
 */
export async function findCommonAncestor(
  storage: StorageBackend,
  commit1: string,
  commit2: string,
  options: FindCommonAncestorOptions = {}
): Promise<CommonAncestorResult> {
  const maxDepth = options.maxDepth ?? Infinity
  let commitsTraversed = 0

  // Fast path: same commit
  if (commit1 === commit2) {
    return {
      ancestor: commit1,
      commitsTraversed: 0,
      depthFromCommit1: 0,
      depthFromCommit2: 0,
    }
  }

  // Initialize bidirectional search
  // visited1 stores commits reachable from commit1 with their depth
  const visited1 = new Map<string, number>()
  // visited2 stores commits reachable from commit2 with their depth
  const visited2 = new Map<string, number>()

  // Frontiers: commits to explore next at each level
  let frontier1: string[] = [commit1]
  let frontier2: string[] = [commit2]

  // Depths for the frontiers
  let depth1 = 0
  let depth2 = 0

  // Mark initial commits as visited
  visited1.set(commit1, 0)
  visited2.set(commit2, 0)

  // Cache for memoization (optional)
  const cache: AncestorCache | null = options.memoize
    ? { parents: new Map(), notFound: new Set() }
    : null

  // Bidirectional BFS - alternate between frontiers
  while (
    (frontier1.length > 0 || frontier2.length > 0) &&
    depth1 < maxDepth &&
    depth2 < maxDepth
  ) {
    // Expand the smaller frontier first (optimization)
    if (frontier1.length > 0 && (frontier2.length === 0 || frontier1.length <= frontier2.length)) {
      const { nextFrontier, foundAncestor } = await expandFrontier(
        storage,
        frontier1,
        visited1,
        visited2,
        depth1 + 1,
        cache
      )
      commitsTraversed += frontier1.length

      if (foundAncestor) {
        return {
          ancestor: foundAncestor,
          commitsTraversed,
          depthFromCommit1: visited1.get(foundAncestor)!,
          depthFromCommit2: visited2.get(foundAncestor)!,
        }
      }

      frontier1 = nextFrontier
      depth1++
    } else if (frontier2.length > 0) {
      const { nextFrontier, foundAncestor } = await expandFrontier(
        storage,
        frontier2,
        visited2,
        visited1,
        depth2 + 1,
        cache
      )
      commitsTraversed += frontier2.length

      if (foundAncestor) {
        return {
          ancestor: foundAncestor,
          commitsTraversed,
          depthFromCommit1: visited1.get(foundAncestor)!,
          depthFromCommit2: visited2.get(foundAncestor)!,
        }
      }

      frontier2 = nextFrontier
      depth2++
    }
  }

  // No common ancestor found
  return {
    ancestor: null,
    commitsTraversed,
  }
}

/**
 * Find common ancestor with simple interface (backward compatible)
 *
 * This is the drop-in replacement for the old naive algorithm.
 * Returns just the ancestor hash or null.
 *
 * @param storage Storage backend
 * @param commit1 First commit hash
 * @param commit2 Second commit hash
 * @returns Common ancestor hash or null
 */
export async function findCommonAncestorSimple(
  storage: StorageBackend,
  commit1: string,
  commit2: string
): Promise<string | null> {
  const result = await findCommonAncestor(storage, commit1, commit2)
  return result.ancestor
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Expand a frontier by one level
 *
 * @param storage Storage backend
 * @param frontier Current frontier to expand
 * @param ourVisited Commits we've visited
 * @param theirVisited Commits the other side has visited
 * @param depth Current depth being explored
 * @param cache Optional memoization cache
 * @returns Next frontier and potential common ancestor
 */
async function expandFrontier(
  storage: StorageBackend,
  frontier: string[],
  ourVisited: Map<string, number>,
  theirVisited: Map<string, number>,
  depth: number,
  cache: AncestorCache | null
): Promise<{ nextFrontier: string[]; foundAncestor: string | null }> {
  const nextFrontier: string[] = []
  let foundAncestor: string | null = null

  // Load all parents in parallel for better I/O performance
  const parentResults = await Promise.all(
    frontier.map((commitHash) => getCommitParents(storage, commitHash, cache))
  )

  for (const parents of parentResults) {
    for (const parent of parents) {
      // Check if this commit was already visited by the other side
      if (theirVisited.has(parent)) {
        // Found common ancestor!
        if (!ourVisited.has(parent)) {
          ourVisited.set(parent, depth)
        }
        foundAncestor = parent
        // Return immediately - we found a valid common ancestor
        return { nextFrontier, foundAncestor }
      }

      // Add to our visited set if not already visited
      if (!ourVisited.has(parent)) {
        ourVisited.set(parent, depth)
        nextFrontier.push(parent)
      }
    }
  }

  return { nextFrontier, foundAncestor }
}

/**
 * Get parent commit hashes, with optional memoization
 *
 * @param storage Storage backend
 * @param commitHash Commit hash to get parents for
 * @param cache Optional memoization cache
 * @returns Array of parent commit hashes
 */
async function getCommitParents(
  storage: StorageBackend,
  commitHash: string,
  cache: AncestorCache | null
): Promise<string[]> {
  // Check cache first
  if (cache) {
    if (cache.notFound.has(commitHash)) {
      return []
    }
    const cached = cache.parents.get(commitHash)
    if (cached !== undefined) {
      return cached
    }
  }

  try {
    const commit = await loadCommit(storage, commitHash)
    // Copy to mutable array since commit.parents may be readonly
    const parents = [...commit.parents]

    // Cache the result
    if (cache) {
      cache.parents.set(commitHash, parents)
    }

    return parents
  } catch {
    // Commit not found - treat as having no parents
    if (cache) {
      cache.notFound.add(commitHash)
    }
    return []
  }
}

// =============================================================================
// Advanced Functions
// =============================================================================

/**
 * Create a reusable ancestor cache for multiple lookups
 *
 * Use this when performing multiple ancestor searches on the same commit graph
 * to avoid redundant commit loading.
 *
 * @returns Empty ancestor cache
 */
export function createAncestorCache(): AncestorCache {
  return {
    parents: new Map(),
    notFound: new Set(),
  }
}

/**
 * Find common ancestors (plural) - returns all merge bases
 *
 * In DAGs with multiple paths to common ancestors (e.g., after criss-cross merges),
 * there may be multiple "merge bases" - commits that are ancestors of both
 * input commits but are not ancestors of each other.
 *
 * This is equivalent to git merge-base --all.
 *
 * @param storage Storage backend
 * @param commit1 First commit hash
 * @param commit2 Second commit hash
 * @param options Search options
 * @returns Array of all common ancestor hashes
 */
export async function findAllCommonAncestors(
  storage: StorageBackend,
  commit1: string,
  commit2: string,
  options: FindCommonAncestorOptions = {}
): Promise<string[]> {
  const maxDepth = options.maxDepth ?? Infinity

  // Same commit case
  if (commit1 === commit2) {
    return [commit1]
  }

  // Get all ancestors of both commits in parallel
  const [ancestors1, ancestors2] = await Promise.all([
    getAllAncestors(storage, commit1, maxDepth),
    getAllAncestors(storage, commit2, maxDepth),
  ])

  // Find intersection (common ancestors)
  const commonAncestors = new Set<string>()
  for (const hash of Array.from(ancestors1)) {
    if (ancestors2.has(hash)) {
      commonAncestors.add(hash)
    }
  }

  if (commonAncestors.size === 0) {
    return []
  }

  // Filter to only "merge bases" - ancestors that are not ancestors of each other
  const ancestorAncestors = new Map<string, Set<string>>()

  // Get ancestors of each common ancestor in parallel
  const ancestorArray = [...commonAncestors]
  const ancestorResults = await Promise.all(
    ancestorArray.map((ancestor) => getAllAncestors(storage, ancestor, maxDepth))
  )

  for (let i = 0; i < ancestorArray.length; i++) {
    ancestorAncestors.set(ancestorArray[i]!, ancestorResults[i]!)
  }

  // A commit is a merge base if no other common ancestor is its descendant
  const mergeBases: string[] = []
  const commonAncestorArray = Array.from(commonAncestors)
  for (const candidate of commonAncestorArray) {
    let isMergeBase = true
    for (const other of commonAncestorArray) {
      if (other !== candidate) {
        const otherAncestors = ancestorAncestors.get(other)!
        if (otherAncestors.has(candidate)) {
          // candidate is an ancestor of other, so it's not a merge base
          isMergeBase = false
          break
        }
      }
    }
    if (isMergeBase) {
      mergeBases.push(candidate)
    }
  }

  return mergeBases
}

/**
 * Get all ancestors of a commit up to maxDepth
 * Uses level-based BFS with parallel loading for efficiency
 */
async function getAllAncestors(
  storage: StorageBackend,
  commit: string,
  maxDepth: number
): Promise<Set<string>> {
  const ancestors = new Set<string>()
  let currentLevel = [commit]
  let depth = 0

  while (currentLevel.length > 0 && depth < maxDepth) {
    // Mark all commits in current level as visited
    for (const hash of currentLevel) {
      ancestors.add(hash)
    }

    // Load all parents in parallel
    const parentResults = await Promise.all(
      currentLevel.map(async (hash) => {
        try {
          const commitObj = await loadCommit(storage, hash)
          return commitObj.parents
        } catch {
          return []
        }
      })
    )

    // Collect next level, filtering already-visited commits
    const nextLevel: string[] = []
    for (const parents of parentResults) {
      for (const parent of parents) {
        if (!ancestors.has(parent)) {
          nextLevel.push(parent)
        }
      }
    }

    currentLevel = nextLevel
    depth++
  }

  return ancestors
}

/**
 * Check if commit1 is an ancestor of commit2
 * Uses level-based BFS with parallel loading for efficiency
 *
 * @param storage Storage backend
 * @param potentialAncestor Commit that might be an ancestor
 * @param descendant Commit to check ancestry for
 * @returns True if potentialAncestor is an ancestor of descendant
 */
export async function isAncestor(
  storage: StorageBackend,
  potentialAncestor: string,
  descendant: string
): Promise<boolean> {
  if (potentialAncestor === descendant) {
    return true
  }

  const visited = new Set<string>()
  let currentLevel = [descendant]

  while (currentLevel.length > 0) {
    // Check if any commit in current level is the ancestor
    for (const hash of currentLevel) {
      if (hash === potentialAncestor) {
        return true
      }
      visited.add(hash)
    }

    // Load all parents in parallel
    const parentResults = await Promise.all(
      currentLevel.map(async (hash) => {
        try {
          const commit = await loadCommit(storage, hash)
          return commit.parents
        } catch {
          return []
        }
      })
    )

    // Collect next level, filtering already-visited commits
    const nextLevel: string[] = []
    for (const parents of parentResults) {
      for (const parent of parents) {
        if (!visited.has(parent)) {
          nextLevel.push(parent)
        }
      }
    }

    currentLevel = nextLevel
  }

  return false
}

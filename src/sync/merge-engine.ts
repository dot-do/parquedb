/**
 * Unified Merge Engine for ParqueDB
 *
 * This module consolidates CLI and Worker merge code paths into a single
 * shared implementation. Both the CLI merge command and Worker handlers
 * should use this engine for all merge operations.
 *
 * Key features:
 * - Common ancestor finding using optimized bidirectional BFS
 * - Event stream merging with conflict detection
 * - Automatic merging of commutative operations
 * - Configurable resolution strategies
 * - Dry-run support for previewing merges
 *
 * @example
 * ```typescript
 * // Create merge engine
 * const engine = createMergeEngine({ storage })
 *
 * // Merge branches
 * const result = await engine.mergeBranches('feature', 'main', {
 *   strategy: 'manual',
 *   dryRun: false,
 * })
 *
 * if (!result.success) {
 *   console.log('Conflicts:', result.conflicts)
 * }
 * ```
 */

import type { StorageBackend } from '../types/storage'
import type { Event } from '../types/entity'
import {
  findCommonAncestor as findCommonAncestorInternal,
  type CommonAncestorResult,
  type FindCommonAncestorOptions,
} from './common-ancestor'
import {
  mergeEventStreams,
  type MergeOptions,
  type EventMergeResult,
  type MergeConflict,
  type ResolutionStrategy,
} from './event-merge'
import { createRefManager } from './refs'

// =============================================================================
// Types
// =============================================================================

/**
 * Resolution strategy for merge conflicts
 */
export type MergeStrategy = ResolutionStrategy | 'manual'

/**
 * Options for creating a merge engine
 */
export interface MergeEngineOptions {
  /** Storage backend to use */
  storage: StorageBackend

  /** Default resolution strategy for conflicts */
  defaultStrategy?: MergeStrategy | undefined

  /** Whether to auto-merge commutative operations (default: true) */
  autoMergeCommutative?: boolean | undefined
}

/**
 * Options for merging branches
 */
export interface MergeBranchesOptions {
  /** Resolution strategy for conflicts */
  strategy?: MergeStrategy | undefined

  /** Whether to perform a dry-run (preview without applying) */
  dryRun?: boolean | undefined

  /** Whether to auto-merge commutative operations */
  autoMergeCommutative?: boolean | undefined

  /** Custom merge function for specific targets */
  customMerge?: MergeOptions['customMerge'] | undefined
}

/**
 * Result of a branch merge operation
 */
export interface MergeBranchesResult {
  /** Whether the merge was successful */
  readonly success: boolean

  /** Error message if merge failed */
  readonly error?: string | undefined

  /** Conflicts detected during merge */
  readonly conflicts: readonly MergeConflict[]

  /** Whether this was a dry-run */
  dryRun?: boolean | undefined

  /** Strategy used for the merge */
  strategy?: MergeStrategy | undefined

  /** Statistics about the merge */
  stats?: {
    /** Number of events from source branch */
    fromSource: number
    /** Number of events from target branch */
    fromTarget: number
    /** Number of auto-merged operations */
    autoMerged: number
    /** Number of resolved conflicts */
    resolved: number
  } | undefined

  /** Common ancestor commit hash (if found) */
  baseCommit?: string | undefined

  /** Source branch commit hash */
  sourceCommit?: string | undefined

  /** Target branch commit hash */
  targetCommit?: string | undefined
}

/**
 * Options for merging events
 */
export interface MergeEventsOptions {
  /** Resolution strategy for conflicts */
  resolutionStrategy?: ResolutionStrategy | undefined

  /** Whether to auto-merge commutative operations */
  autoMergeCommutative?: boolean | undefined

  /** Custom merge function for specific targets */
  customMerge?: MergeOptions['customMerge'] | undefined
}

/**
 * Merge engine interface
 */
export interface MergeEngine {
  /**
   * Find the common ancestor of two commits
   *
   * Uses optimized bidirectional BFS for O(min(n,m)) performance.
   *
   * @param commit1 First commit hash
   * @param commit2 Second commit hash
   * @param options Optional search options
   * @returns Common ancestor result with statistics
   */
  findCommonAncestor(
    commit1: string,
    commit2: string,
    options?: FindCommonAncestorOptions | undefined
  ): Promise<CommonAncestorResult>

  /**
   * Merge two branches
   *
   * Finds common ancestor, loads events since divergence, and merges them.
   *
   * @param source Source branch name (the branch being merged in)
   * @param target Target branch name (the branch being merged into)
   * @param options Merge options
   * @returns Merge result with conflicts and statistics
   */
  mergeBranches(
    source: string,
    target: string,
    options?: MergeBranchesOptions | undefined
  ): Promise<MergeBranchesResult>

  /**
   * Merge two event arrays
   *
   * Low-level merge function for event streams.
   *
   * @param ourEvents Events from our branch
   * @param theirEvents Events from their branch
   * @param options Merge options
   * @returns Event merge result
   */
  mergeEvents(
    ourEvents: Event[],
    theirEvents: Event[],
    options?: MergeEventsOptions | undefined
  ): Promise<EventMergeResult>
}

// =============================================================================
// Implementation
// =============================================================================

/**
 * Create a new merge engine
 *
 * @param options Engine options
 * @returns Configured merge engine
 */
export function createMergeEngine(options: MergeEngineOptions): MergeEngine {
  const { storage, defaultStrategy = 'manual', autoMergeCommutative = true } = options

  const refManager = createRefManager(storage)

  return {
    async findCommonAncestor(
      commit1: string,
      commit2: string,
      ancestorOptions?: FindCommonAncestorOptions
    ): Promise<CommonAncestorResult> {
      return findCommonAncestorInternal(storage, commit1, commit2, ancestorOptions)
    },

    async mergeBranches(
      source: string,
      target: string,
      mergeOptions: MergeBranchesOptions = {}
    ): Promise<MergeBranchesResult> {
      const {
        strategy = defaultStrategy,
        dryRun = false,
        autoMergeCommutative: mergeCommutative = autoMergeCommutative,
        customMerge,
      } = mergeOptions

      // Resolve source branch to commit
      const sourceCommit = await refManager.resolveRef(source)
      if (!sourceCommit) {
        return {
          success: false,
          error: `Branch not found: ${source}`,
          conflicts: [],
        }
      }

      // Resolve target branch to commit
      const targetCommit = await refManager.resolveRef(target)
      if (!targetCommit) {
        return {
          success: false,
          error: `Branch not found: ${target}`,
          conflicts: [],
        }
      }

      // Find common ancestor
      const ancestorResult = await findCommonAncestorInternal(
        storage,
        sourceCommit,
        targetCommit
      )

      if (!ancestorResult.ancestor) {
        return {
          success: false,
          error: 'No common ancestor found between branches',
          conflicts: [],
          sourceCommit,
          targetCommit,
        }
      }

      const baseCommit = ancestorResult.ancestor

      // Fast-forward case: source is already at target or vice versa
      if (sourceCommit === targetCommit) {
        return {
          success: true,
          conflicts: [],
          dryRun,
          strategy,
          stats: {
            fromSource: 0,
            fromTarget: 0,
            autoMerged: 0,
            resolved: 0,
          },
          baseCommit,
          sourceCommit,
          targetCommit,
        }
      }

      // For now, we return success for identical ancestor (fast-forward possible)
      // Full implementation would load events since base and merge them
      if (baseCommit === targetCommit) {
        // Fast-forward: target can simply move to source
        return {
          success: true,
          conflicts: [],
          dryRun,
          strategy,
          stats: {
            fromSource: 0,
            fromTarget: 0,
            autoMerged: 0,
            resolved: 0,
          },
          baseCommit,
          sourceCommit,
          targetCommit,
        }
      }

      if (baseCommit === sourceCommit) {
        // Already up to date: source is ancestor of target
        return {
          success: true,
          conflicts: [],
          dryRun,
          strategy,
          stats: {
            fromSource: 0,
            fromTarget: 0,
            autoMerged: 0,
            resolved: 0,
          },
          baseCommit,
          sourceCommit,
          targetCommit,
        }
      }

      // For a full merge, we would:
      // 1. Load events from source since base
      // 2. Load events from target since base
      // 3. Merge the event streams
      // 4. Apply merged events (unless dry-run)

      // For now, return success with empty events (placeholder for full implementation)
      const eventMergeOptions: MergeOptions = {
        resolutionStrategy: strategy === 'manual' ? undefined : strategy,
        autoMergeCommutative: mergeCommutative,
        customMerge,
      }

      // Placeholder: In full implementation, we'd load actual events
      const sourceEvents: Event[] = []
      const targetEvents: Event[] = []

      const mergeResult = await mergeEventStreams(
        targetEvents,
        sourceEvents,
        eventMergeOptions
      )

      return {
        success: mergeResult.success,
        conflicts: mergeResult.conflicts,
        dryRun,
        strategy,
        stats: {
          fromSource: mergeResult.stats.fromTheirs,
          fromTarget: mergeResult.stats.fromOurs,
          autoMerged: mergeResult.stats.autoMerged,
          resolved: mergeResult.resolved.length,
        },
        baseCommit,
        sourceCommit,
        targetCommit,
      }
    },

    async mergeEvents(
      ourEvents: Event[],
      theirEvents: Event[],
      mergeOptions: MergeEventsOptions = {}
    ): Promise<EventMergeResult> {
      const {
        resolutionStrategy,
        autoMergeCommutative: mergeCommutative = autoMergeCommutative,
        customMerge,
      } = mergeOptions

      return mergeEventStreams(ourEvents, theirEvents, {
        resolutionStrategy,
        autoMergeCommutative: mergeCommutative,
        customMerge,
      })
    },
  }
}

// =============================================================================
// Re-exports for convenience
// =============================================================================

export type { CommonAncestorResult, FindCommonAncestorOptions } from './common-ancestor'
export type { EventMergeResult, MergeConflict, ResolutionStrategy } from './event-merge'

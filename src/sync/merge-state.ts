/**
 * Merge State Management
 *
 * Manages merge state for multi-step merge operations with conflict resolution.
 * Similar to git's MERGE_HEAD and MERGE_MSG files.
 */

import type { StorageBackend } from '../types/storage'

// =============================================================================
// Types
// =============================================================================

/**
 * Status of a merge operation
 */
export type MergeStatus = 'in_progress' | 'conflicted' | 'resolved'

/**
 * Resolution strategy for a conflict
 */
export type ConflictResolutionStrategy = 'ours' | 'theirs' | 'newest' | 'manual'

/**
 * Information about a single conflict
 */
export interface ConflictInfo {
  /** Entity ID that has conflicts */
  entityId: string

  /** Collection/namespace the entity belongs to */
  collection: string

  /** Fields that have conflicts */
  fields: string[]

  /** Whether this conflict has been resolved */
  resolved: boolean

  /** Resolution strategy used (if resolved) */
  resolution?: ConflictResolutionStrategy

  /** Resolved value (if manually resolved) */
  resolvedValue?: unknown

  /** Our value for conflicting fields */
  ourValue?: unknown

  /** Their value for conflicting fields */
  theirValue?: unknown

  /** Base value (common ancestor) */
  baseValue?: unknown
}

/**
 * State of an in-progress merge operation
 */
export interface MergeState {
  /** Status of the merge */
  status: MergeStatus

  /** Branch being merged in (source) */
  source: string

  /** Branch being merged into (target/current) */
  target: string

  /** Common ancestor commit */
  baseCommit: string

  /** Source branch head commit */
  sourceCommit: string

  /** Target branch head at merge start */
  targetCommit: string

  /** List of conflicts */
  conflicts: ConflictInfo[]

  /** Timestamp when merge started */
  startedAt: string

  /** Default resolution strategy */
  strategy: ConflictResolutionStrategy

  /** Additional metadata */
  metadata?: Record<string, unknown>
}

// =============================================================================
// Constants
// =============================================================================

const MERGE_STATE_PATH = '_meta/MERGE_STATE'

// =============================================================================
// State Management Functions
// =============================================================================

/**
 * Load merge state from storage
 * @param storage StorageBackend to read from
 * @returns MergeState if exists, null otherwise
 */
export async function loadMergeState(storage: StorageBackend): Promise<MergeState | null> {
  try {
    const exists = await storage.exists(MERGE_STATE_PATH)
    if (!exists) {
      return null
    }

    const data = await storage.read(MERGE_STATE_PATH)
    const json = new TextDecoder().decode(data)
    const state = JSON.parse(json) as MergeState

    // Validate the loaded state
    validateMergeState(state)

    return state
  } catch (error) {
    // If there's any error reading or parsing, treat as no merge in progress
    return null
  }
}

/**
 * Save merge state to storage
 * @param storage StorageBackend to write to
 * @param state MergeState to save
 */
export async function saveMergeState(storage: StorageBackend, state: MergeState): Promise<void> {
  validateMergeState(state)

  const json = JSON.stringify(state, null, 2)
  await storage.write(MERGE_STATE_PATH, new TextEncoder().encode(json))
}

/**
 * Clear merge state from storage
 * @param storage StorageBackend to delete from
 */
export async function clearMergeState(storage: StorageBackend): Promise<void> {
  const exists = await storage.exists(MERGE_STATE_PATH)
  if (exists) {
    await storage.delete(MERGE_STATE_PATH)
  }
}

/**
 * Check if a merge is currently in progress
 * @param storage StorageBackend to check
 * @returns true if merge state exists
 */
export async function hasMergeInProgress(storage: StorageBackend): Promise<boolean> {
  return await storage.exists(MERGE_STATE_PATH)
}

// =============================================================================
// Conflict Management
// =============================================================================

/**
 * Add a conflict to merge state
 * @param state Current merge state
 * @param conflict Conflict to add
 * @returns Updated merge state
 */
export function addConflict(state: MergeState, conflict: ConflictInfo): MergeState {
  return {
    ...state,
    status: 'conflicted',
    conflicts: [...state.conflicts, conflict],
  }
}

/**
 * Resolve a conflict in merge state
 * @param state Current merge state
 * @param entityId Entity ID to resolve
 * @param resolution Resolution strategy or value
 * @returns Updated merge state
 */
export function resolveConflict(
  state: MergeState,
  entityId: string,
  resolution: ConflictResolutionStrategy | { strategy: 'manual'; value: unknown }
): MergeState {
  const conflicts = state.conflicts.map((conflict): ConflictInfo => {
    if (conflict.entityId === entityId) {
      if (typeof resolution === 'string') {
        return {
          ...conflict,
          resolved: true,
          resolution,
        }
      } else {
        return {
          ...conflict,
          resolved: true,
          resolution: 'manual' as ConflictResolutionStrategy,
          resolvedValue: resolution.value,
        }
      }
    }
    return conflict
  })

  // Check if all conflicts are resolved
  const allResolved = conflicts.every((c) => c.resolved)
  const status: MergeStatus = allResolved ? 'resolved' : 'conflicted'

  return {
    ...state,
    status,
    conflicts,
  }
}

/**
 * Get unresolved conflicts from merge state
 * @param state Current merge state
 * @returns Array of unresolved conflicts
 */
export function getUnresolvedConflicts(state: MergeState): ConflictInfo[] {
  return state.conflicts.filter((c) => !c.resolved)
}

/**
 * Get conflicts for a specific entity or pattern
 * @param state Current merge state
 * @param pattern Entity ID or glob pattern
 * @returns Array of matching conflicts
 */
export function getConflictsByPattern(state: MergeState, pattern: string): ConflictInfo[] {
  // Simple glob matching for now (supports * wildcard)
  const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$')
  return state.conflicts.filter((c) => regex.test(c.entityId))
}

/**
 * Check if all conflicts in merge state are resolved
 * @param state Current merge state
 * @returns true if all conflicts resolved
 */
export function allConflictsResolved(state: MergeState): boolean {
  return state.conflicts.length > 0 && state.conflicts.every((c) => c.resolved)
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate merge state structure
 * @param state MergeState to validate
 * @throws Error if validation fails
 */
function validateMergeState(state: MergeState): void {
  if (!state.status || !['in_progress', 'conflicted', 'resolved'].includes(state.status)) {
    throw new Error('Invalid merge state: status must be in_progress, conflicted, or resolved')
  }

  if (!state.source || typeof state.source !== 'string') {
    throw new Error('Invalid merge state: source must be a string')
  }

  if (!state.target || typeof state.target !== 'string') {
    throw new Error('Invalid merge state: target must be a string')
  }

  if (!state.baseCommit || typeof state.baseCommit !== 'string') {
    throw new Error('Invalid merge state: baseCommit must be a string')
  }

  if (!state.sourceCommit || typeof state.sourceCommit !== 'string') {
    throw new Error('Invalid merge state: sourceCommit must be a string')
  }

  if (!state.targetCommit || typeof state.targetCommit !== 'string') {
    throw new Error('Invalid merge state: targetCommit must be a string')
  }

  if (!Array.isArray(state.conflicts)) {
    throw new Error('Invalid merge state: conflicts must be an array')
  }

  if (!state.startedAt || typeof state.startedAt !== 'string') {
    throw new Error('Invalid merge state: startedAt must be a string')
  }

  if (!state.strategy || !['ours', 'theirs', 'newest', 'manual'].includes(state.strategy)) {
    throw new Error('Invalid merge state: strategy must be ours, theirs, newest, or manual')
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new merge state
 * @param params Merge state parameters
 * @returns New MergeState
 */
export function createMergeState(params: {
  source: string
  target: string
  baseCommit: string
  sourceCommit: string
  targetCommit: string
  strategy?: ConflictResolutionStrategy
}): MergeState {
  return {
    status: 'in_progress',
    source: params.source,
    target: params.target,
    baseCommit: params.baseCommit,
    sourceCommit: params.sourceCommit,
    targetCommit: params.targetCommit,
    conflicts: [],
    startedAt: new Date().toISOString(),
    strategy: params.strategy || 'manual',
  }
}

/**
 * Event-Based Merge Engine for ParqueDB
 *
 * This module provides the core merge algorithm for combining event streams
 * from different branches/replicas. It merges events rather than final states,
 * enabling better conflict detection and automatic merging of commutative operations.
 *
 * Key features:
 * - Automatic merge of commutative operations ($inc, $addToSet, etc.)
 * - Field-level conflict detection
 * - Configurable resolution strategies
 * - Preserves event ordering
 */

import type { Event, Variant } from '../types/entity'
import { deepEqual } from '../utils'
import { opsAsVariant } from '../types/cast'
import {
  isCommutative,
  combineOperations,
  extractOperations,
  type UpdateOps,
} from './commutative-ops'
import {
  detectConflicts as _detectConflicts,
  type ConflictInfo,
} from './conflict-detection'
import {
  resolveConflict,
  resolveAllConflicts as _resolveAllConflicts,
  allResolutionsComplete as _allResolutionsComplete,
  type ConflictResolution,
  type ResolutionStrategy,
} from './conflict-resolution'

// =============================================================================
// Types
// =============================================================================

/**
 * Custom merge function type
 */
export type CustomMergeFn = (
  target: string,
  ourEvents: readonly Event[],
  theirEvents: readonly Event[]
) => MergeTargetResult | null

/**
 * Options for the merge operation
 */
export interface MergeOptions {
  /** Resolution strategy for conflicts (default: no auto-resolution) */
  readonly resolutionStrategy?: ResolutionStrategy | undefined

  /** Whether to auto-merge commutative operations (default: true) */
  readonly autoMergeCommutative?: boolean | undefined

  /** Custom merge function for specific targets */
  readonly customMerge?: CustomMergeFn | undefined
}

/**
 * Result of merging a specific target (entity or relationship)
 */
export interface MergeTargetResult {
  /** Whether the merge was successful */
  readonly success: boolean

  /** Combined operations (if auto-merged) */
  readonly combinedOps?: UpdateOps | undefined

  /** Conflicts that were detected */
  readonly conflicts: readonly ConflictInfo[]

  /** Events from both streams for this target */
  readonly events: readonly Event[]
}

/**
 * Information about an auto-merged operation
 */
export interface AutoMergeInfo {
  /** Target that was auto-merged */
  readonly target: string

  /** The combined operations */
  readonly combinedOps: UpdateOps

  /** Our original operations */
  readonly ourOps: UpdateOps

  /** Their original operations */
  readonly theirOps: UpdateOps

  /** Events that were combined */
  readonly events: readonly Event[]
}

/**
 * A conflict that occurred during merge
 */
export interface MergeConflict extends ConflictInfo {
  /** Whether the conflict was automatically resolved */
  readonly resolved: boolean

  /** Resolution if resolved */
  readonly resolution?: ConflictResolution | undefined
}

/**
 * Statistics about the merge operation
 */
export interface MergeStats {
  /** Number of events from our stream */
  readonly fromOurs: number

  /** Number of events from their stream */
  readonly fromTheirs: number

  /** Number of entities processed */
  readonly entitiesProcessed: number

  /** Number of entities with conflicts */
  readonly entitiesWithConflicts: number

  /** Number of auto-merged operations */
  readonly autoMerged: number
}

/**
 * Result of the merge operation
 */
export interface EventMergeResult {
  /** Whether the merge was successful (no unresolved conflicts) */
  readonly success: boolean

  /** All conflicts detected during merge */
  readonly conflicts: readonly MergeConflict[]

  /** Operations that were auto-merged */
  readonly autoMerged: readonly AutoMergeInfo[]

  /** Conflicts that were resolved by strategy */
  readonly resolved: readonly ConflictResolution[]

  /** Merged event stream (sorted by timestamp) */
  readonly mergedEvents: readonly Event[]

  /** Statistics about the merge */
  readonly stats: MergeStats

  /** Events from our stream */
  readonly ourEvents: readonly Event[]

  /** Events from their stream */
  readonly theirEvents: readonly Event[]
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Sort events by timestamp, then by ID for deterministic ordering
 */
export function sortEvents(events: Event[]): Event[] {
  return [...events].sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts
    return a.id.localeCompare(b.id)
  })
}

/**
 * Validate that events are in correct timestamp order
 */
export function validateEventOrder(events: Event[]): boolean {
  for (let i = 1; i < events.length; i++) {
    const prev = events[i - 1]!
    const curr = events[i]!
    if (curr.ts < prev.ts) return false
    if (curr.ts === prev.ts && curr.id < prev.id) return false
  }
  return true
}

// =============================================================================
// Main Merge Function
// =============================================================================

/**
 * Merge two event streams: ours and theirs
 *
 * This implements a two-way merge for divergent event streams:
 * - ours: Our branch's events
 * - theirs: Their branch's events
 *
 * @param ourEvents Events from our branch
 * @param theirEvents Events from their branch
 * @param options Merge options
 * @returns Merge result with conflicts, auto-merges, and merged events
 */
export async function mergeEventStreams(
  ourEvents: Event[],
  theirEvents: Event[],
  options: MergeOptions = {}
): Promise<EventMergeResult> {
  const {
    resolutionStrategy,
    autoMergeCommutative = true,
    customMerge,
  } = options

  const conflicts: MergeConflict[] = []
  const autoMerged: AutoMergeInfo[] = []
  const resolved: ConflictResolution[] = []
  const mergedEvents: Event[] = []
  const entitiesWithConflicts = new Set<string>()

  // Group events by target for processing
  const ourByTarget = groupEventsByTarget(ourEvents)
  const theirByTarget = groupEventsByTarget(theirEvents)

  // Get all targets that have events in either stream
  const allTargets = new Set([...ourByTarget.keys(), ...theirByTarget.keys()])

  // Process each target
  for (const target of allTargets) {
    const ourTargetEvents = ourByTarget.get(target) || []
    const theirTargetEvents = theirByTarget.get(target) || []

    // Try custom merge first
    if (customMerge) {
      const customResult = customMerge(target, ourTargetEvents, theirTargetEvents)
      if (customResult) {
        if (!customResult.success) {
          for (const conflict of customResult.conflicts) {
            conflicts.push({ ...conflict, resolved: false })
            entitiesWithConflicts.add(target)
          }
        }
        mergedEvents.push(...customResult.events)
        continue
      }
    }

    // If only one side has events, just include them
    if (ourTargetEvents.length === 0) {
      mergedEvents.push(...theirTargetEvents)
      continue
    }

    if (theirTargetEvents.length === 0) {
      mergedEvents.push(...ourTargetEvents)
      continue
    }

    // Both sides have events - need to check for conflicts
    const result = mergeTargetEvents(
      target,
      ourTargetEvents,
      theirTargetEvents,
      autoMergeCommutative
    )

    if (result.success) {
      // Successfully merged (either no overlap or auto-merged)
      mergedEvents.push(...result.events)

      if (result.combinedOps) {
        autoMerged.push({
          target,
          combinedOps: result.combinedOps,
          ourOps: extractOpsFromEvents(ourTargetEvents),
          theirOps: extractOpsFromEvents(theirTargetEvents),
          events: result.events,
        })
      }
    } else {
      // Conflicts detected
      entitiesWithConflicts.add(target)

      for (const conflict of result.conflicts) {
        let mergeConflict: MergeConflict = { ...conflict, resolved: false }

        // Try to resolve if strategy provided
        if (resolutionStrategy) {
          const resolution = resolveConflict(conflict, resolutionStrategy)
          if (!resolution.requiresManualResolution) {
            mergeConflict = { ...conflict, resolved: true, resolution }
            resolved.push(resolution)
          }
        }

        conflicts.push(mergeConflict)
      }

      // Include events even if there are conflicts
      mergedEvents.push(...result.events)
    }
  }

  // Sort merged events by timestamp
  const sortedMergedEvents = sortEvents(mergedEvents)

  // Determine overall success
  const unresolvedConflicts = conflicts.filter(c => !c.resolved)
  const success = unresolvedConflicts.length === 0

  return {
    success,
    conflicts: unresolvedConflicts,
    autoMerged,
    resolved,
    mergedEvents: sortedMergedEvents,
    stats: {
      fromOurs: ourEvents.length,
      fromTheirs: theirEvents.length,
      entitiesProcessed: allTargets.size,
      entitiesWithConflicts: entitiesWithConflicts.size,
      autoMerged: autoMerged.length,
    },
    ourEvents,
    theirEvents,
  }
}

// =============================================================================
// Target-Level Merge
// =============================================================================

/**
 * Extract operations from the latest event in a list
 */
function extractOpsFromEvents(events: Event[]): UpdateOps {
  const latest = getLatestEvent(events)
  if (!latest) return {}

  // Check metadata for update operations
  if (latest.metadata?.update) {
    return latest.metadata.update as UpdateOps
  }

  // Check after._ops
  return extractOperations(latest.after as Record<string, unknown>)
}

/**
 * Merge events for a specific target
 */
function mergeTargetEvents(
  target: string,
  ourEvents: Event[],
  theirEvents: Event[],
  autoMergeCommutative: boolean
): MergeTargetResult {
  // Get latest events from each stream
  const ourLatest = getLatestEvent(ourEvents)
  const theirLatest = getLatestEvent(theirEvents)

  if (!ourLatest || !theirLatest) {
    return {
      success: true,
      conflicts: [],
      events: [...ourEvents, ...theirEvents],
    }
  }

  // Check for DELETE vs other operations
  if (ourLatest.op === 'DELETE' && theirLatest.op !== 'DELETE') {
    return {
      success: false,
      conflicts: [{
        type: 'delete_update',
        target,
        ourValue: undefined,
        theirValue: theirLatest.after,
        baseValue: ourLatest.before,
        ourEvent: ourLatest,
        theirEvent: theirLatest,
      }],
      events: [...ourEvents, ...theirEvents],
    }
  }

  if (theirLatest.op === 'DELETE' && ourLatest.op !== 'DELETE') {
    return {
      success: false,
      conflicts: [{
        type: 'delete_update',
        target,
        ourValue: ourLatest.after,
        theirValue: undefined,
        baseValue: theirLatest.before,
        ourEvent: ourLatest,
        theirEvent: theirLatest,
      }],
      events: [...ourEvents, ...theirEvents],
    }
  }

  // Check for CREATE + CREATE (same entity created in both)
  if (ourLatest.op === 'CREATE' && theirLatest.op === 'CREATE') {
    if (!deepEqual(ourLatest.after, theirLatest.after)) {
      return {
        success: false,
        conflicts: [{
          type: 'create_create',
          target,
          ourValue: ourLatest.after,
          theirValue: theirLatest.after,
          baseValue: undefined,
          ourEvent: ourLatest,
          theirEvent: theirLatest,
        }],
        events: [...ourEvents, ...theirEvents],
      }
    }
    // Same create - no conflict, dedupe events
    return {
      success: true,
      conflicts: [],
      events: ourEvents, // Just use our events since they're identical
    }
  }

  // Extract operations if embedded
  const ourOps = extractOpsFromEvents(ourEvents)
  const theirOps = extractOpsFromEvents(theirEvents)

  // Try auto-merge for commutative operations
  if (autoMergeCommutative && Object.keys(ourOps).length > 0 && Object.keys(theirOps).length > 0) {
    if (isCommutative(ourOps, theirOps)) {
      const combinedOps = combineOperations(ourOps, theirOps)

      // Create a synthetic merged event
      const mergedEvent = createMergedEvent(ourLatest, theirLatest, combinedOps)

      return {
        success: true,
        combinedOps,
        conflicts: [],
        events: [mergedEvent],
      }
    }
  }

  // Check for field-level conflicts by comparing before/after states
  const conflicts = detectFieldConflicts(target, ourLatest, theirLatest)

  if (conflicts.length === 0) {
    // No conflicts - can combine events
    return {
      success: true,
      conflicts: [],
      events: [...ourEvents, ...theirEvents],
    }
  }

  return {
    success: false,
    conflicts,
    events: [...ourEvents, ...theirEvents],
  }
}

/**
 * Detect field-level conflicts between two update events
 */
function detectFieldConflicts(
  target: string,
  ourEvent: Event,
  theirEvent: Event
): ConflictInfo[] {
  const conflicts: ConflictInfo[] = []

  const ourBefore = ourEvent.before as Record<string, unknown> | undefined
  const ourAfter = ourEvent.after as Record<string, unknown> | undefined
  const theirBefore = theirEvent.before as Record<string, unknown> | undefined
  const theirAfter = theirEvent.after as Record<string, unknown> | undefined

  if (!ourAfter || !theirAfter) {
    return conflicts
  }

  // Get operations from metadata or embedded _ops
  const ourOps = extractOpsFromEvents([ourEvent])
  const theirOps = extractOpsFromEvents([theirEvent])

  // If we have operations, check them
  if (Object.keys(ourOps).length > 0 && Object.keys(theirOps).length > 0) {
    // Check for $push conflicts (non-commutative)
    if (ourOps.$push && theirOps.$push) {
      const ourPushFields = Object.keys(ourOps.$push)
      const theirPushFields = Object.keys(theirOps.$push)

      for (const field of ourPushFields) {
        if (theirPushFields.includes(field)) {
          conflicts.push({
            type: 'concurrent_update',
            target,
            field,
            ourValue: (ourAfter as Record<string, unknown>)[field],
            theirValue: (theirAfter as Record<string, unknown>)[field],
            baseValue: (ourBefore as Record<string, unknown>)?.[field],
            ourEvent,
            theirEvent,
          })
        }
      }
    }

    // Check for $set conflicts on same field with different values
    if (ourOps.$set && theirOps.$set) {
      const ourSetFields = Object.keys(ourOps.$set)
      const theirSetFields = Object.keys(theirOps.$set)

      for (const field of ourSetFields) {
        if (theirSetFields.includes(field)) {
          const ourValue = (ourOps.$set as Record<string, unknown>)[field]
          const theirValue = (theirOps.$set as Record<string, unknown>)[field]

          if (!deepEqual(ourValue, theirValue)) {
            conflicts.push({
              type: 'concurrent_update',
              target,
              field,
              ourValue,
              theirValue,
              baseValue: (ourBefore as Record<string, unknown>)?.[field],
              ourEvent,
              theirEvent,
            })
          }
        }
      }
    }

    return conflicts
  }

  // Fall back to field-by-field comparison
  const ourChangedFields = getChangedFields(ourBefore, ourAfter)
  const theirChangedFields = getChangedFields(theirBefore, theirAfter)

  for (const field of ourChangedFields) {
    if (theirChangedFields.has(field)) {
      const ourValue = ourAfter[field]
      const theirValue = theirAfter[field]

      if (!deepEqual(ourValue, theirValue)) {
        conflicts.push({
          type: 'concurrent_update',
          target,
          field,
          ourValue,
          theirValue,
          baseValue: ourBefore?.[field],
          ourEvent,
          theirEvent,
        })
      }
    }
  }

  return conflicts
}

/**
 * Get fields that changed between before and after states
 */
function getChangedFields(
  before: Record<string, unknown> | undefined,
  after: Record<string, unknown> | undefined
): Set<string> {
  const changed = new Set<string>()

  if (!after) return changed

  for (const [key, value] of Object.entries(after)) {
    if (key === '_ops') continue
    const beforeValue = before?.[key]
    if (!deepEqual(value, beforeValue)) {
      changed.add(key)
    }
  }

  if (before) {
    for (const key of Object.keys(before)) {
      if (!(key in after)) {
        changed.add(key)
      }
    }
  }

  return changed
}

/**
 * Create a merged event from two events with combined operations
 */
function createMergedEvent(
  ourEvent: Event,
  theirEvent: Event,
  combinedOps: UpdateOps
): Event {
  // Use the later timestamp
  const ts = Math.max(ourEvent.ts, theirEvent.ts)

  // Merge the after states
  const mergedAfter: Record<string, unknown> = {
    ...ourEvent.after as Record<string, unknown>,
    ...theirEvent.after as Record<string, unknown>,
  }

  // Apply combined operations to determine final state
  if (combinedOps.$inc) {
    for (const [field, delta] of Object.entries(combinedOps.$inc)) {
      const baseValue = (ourEvent.before as Record<string, unknown>)?.[field] as number || 0
      mergedAfter[field] = baseValue + (delta || 0)
    }
  }

  // Store combined ops in _ops for future merges
  mergedAfter._ops = combinedOps

  return {
    id: `merged-${ourEvent.id}-${theirEvent.id}`,
    ts,
    op: 'UPDATE',
    target: ourEvent.target,
    before: ourEvent.before,
    after: mergedAfter as Variant,
    actor: ourEvent.actor,
    metadata: {
      merged: true,
      sources: [ourEvent.id, theirEvent.id],
      update: opsAsVariant(combinedOps),
    } as Variant,
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Group events by their target identifier
 */
function groupEventsByTarget(events: Event[]): Map<string, Event[]> {
  const grouped = new Map<string, Event[]>()

  for (const event of events) {
    const existing = grouped.get(event.target) || []
    existing.push(event)
    grouped.set(event.target, existing)
  }

  return grouped
}

/**
 * Get the latest event from a list (by timestamp)
 */
function getLatestEvent(events: Event[]): Event | undefined {
  if (events.length === 0) return undefined

  return events.reduce((latest, event) => {
    if (event.ts > latest.ts) return event
    if (event.ts === latest.ts && event.id > latest.id) return event
    return latest
  })
}

// =============================================================================
// Exports
// =============================================================================

export type { ConflictInfo } from './conflict-detection'
export type { ConflictResolution, ResolutionStrategy } from './conflict-resolution'

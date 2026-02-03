/**
 * Conflict Detection for Event-Based Merge
 *
 * This module identifies conflicts between concurrent event streams by
 * analyzing operations and their affected fields.
 *
 * Conflict types:
 * - concurrent_update: Same field modified with different values
 * - delete_update: Entity deleted by one stream, updated by another
 * - create_create: Same entity created in both streams (rare)
 */

import type { Event, Variant } from '../types/entity'
import { deepEqual } from '../utils'
import {
  isCommutative,
  extractOperations,
  getAffectedFields,
  type UpdateOps,
} from './commutative-ops'

// =============================================================================
// Types
// =============================================================================

/**
 * Types of conflicts that can be detected
 */
export type ConflictType =
  | 'concurrent_update'   // Same field modified with different values
  | 'delete_update'       // Entity deleted and updated concurrently
  | 'create_create'       // Same entity created in both streams

/**
 * Information about a detected conflict
 */
export interface ConflictInfo {
  /** Type of conflict */
  readonly type: ConflictType

  /** Target entity/relationship identifier */
  readonly target: string

  /** Field that has a conflict (undefined for entity-level conflicts like delete_update) */
  readonly field?: string | undefined

  /** Our value for the field */
  readonly ourValue: unknown

  /** Their value for the field */
  readonly theirValue: unknown

  /** Base value before either change (undefined for CREATE) */
  readonly baseValue: unknown

  /** Our event that caused the conflict */
  readonly ourEvent: Event

  /** Their event that caused the conflict */
  readonly theirEvent: Event

  /** Operations from our event (if embedded) */
  readonly ourOps?: UpdateOps | undefined

  /** Operations from their event (if embedded) */
  readonly theirOps?: UpdateOps | undefined
}

// =============================================================================
// Conflict Detection
// =============================================================================

/**
 * Detect conflicts between two event streams
 *
 * @param ourEvents Events from our branch/stream
 * @param theirEvents Events from their branch/stream
 * @returns Array of detected conflicts
 */
export function detectConflicts(ourEvents: Event[], theirEvents: Event[]): ConflictInfo[] {
  const conflicts: ConflictInfo[] = []

  // Group events by target
  const ourByTarget = groupEventsByTarget(ourEvents)
  const theirByTarget = groupEventsByTarget(theirEvents)

  // Find targets with events in both streams
  const commonTargets = new Set<string>()
  for (const target of ourByTarget.keys()) {
    if (theirByTarget.has(target)) {
      commonTargets.add(target)
    }
  }

  // Check each common target for conflicts
  for (const target of commonTargets) {
    const ourTargetEvents = ourByTarget.get(target)!
    const theirTargetEvents = theirByTarget.get(target)!

    const targetConflicts = detectTargetConflicts(target, ourTargetEvents, theirTargetEvents)
    conflicts.push(...targetConflicts)
  }

  return conflicts
}

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
 * Detect conflicts for a specific target entity
 */
function detectTargetConflicts(
  target: string,
  ourEvents: Event[],
  theirEvents: Event[]
): ConflictInfo[] {
  const conflicts: ConflictInfo[] = []

  // Get the latest event from each stream for this target
  const ourLatest = getLatestEvent(ourEvents)
  const theirLatest = getLatestEvent(theirEvents)

  if (!ourLatest || !theirLatest) {
    return conflicts
  }

  // Check for DELETE vs UPDATE conflict
  if (ourLatest.op === 'DELETE' && theirLatest.op === 'UPDATE') {
    conflicts.push({
      type: 'delete_update',
      target,
      ourValue: undefined,
      theirValue: theirLatest.after,
      baseValue: ourLatest.before,
      ourEvent: ourLatest,
      theirEvent: theirLatest,
    })
    return conflicts
  }

  if (theirLatest.op === 'DELETE' && ourLatest.op === 'UPDATE') {
    conflicts.push({
      type: 'delete_update',
      target,
      ourValue: ourLatest.after,
      theirValue: undefined,
      baseValue: theirLatest.before,
      ourEvent: ourLatest,
      theirEvent: theirLatest,
    })
    return conflicts
  }

  // Check for CREATE + CREATE conflict (same target created in both)
  if (ourLatest.op === 'CREATE' && theirLatest.op === 'CREATE') {
    if (!deepEqual(ourLatest.after, theirLatest.after)) {
      conflicts.push({
        type: 'create_create',
        target,
        ourValue: ourLatest.after,
        theirValue: theirLatest.after,
        baseValue: undefined,
        ourEvent: ourLatest,
        theirEvent: theirLatest,
      })
    }
    return conflicts
  }

  // Check for UPDATE vs UPDATE conflicts
  if (ourLatest.op === 'UPDATE' && theirLatest.op === 'UPDATE') {
    // Extract operations from events (if embedded in _ops)
    const ourOps = extractOperations(ourLatest.after as Record<string, unknown>)
    const theirOps = extractOperations(theirLatest.after as Record<string, unknown>)

    // If both have embedded operations, check commutativity
    if (Object.keys(ourOps).length > 0 && Object.keys(theirOps).length > 0) {
      if (isCommutative(ourOps, theirOps)) {
        // Operations are commutative, no conflict
        return conflicts
      }

      // Operations are not commutative - find specific field conflicts
      const fieldConflicts = detectFieldConflicts(target, ourLatest, theirLatest, ourOps, theirOps)
      conflicts.push(...fieldConflicts)
    } else {
      // No embedded operations - compare field-by-field
      const fieldConflicts = detectFieldLevelConflicts(target, ourLatest, theirLatest)
      conflicts.push(...fieldConflicts)
    }
  }

  return conflicts
}

/**
 * Get the latest event from a list (by timestamp or ID order)
 */
function getLatestEvent(events: Event[]): Event | undefined {
  if (events.length === 0) return undefined

  return events.reduce((latest, event) => {
    if (event.ts > latest.ts) return event
    if (event.ts === latest.ts && event.id > latest.id) return event
    return latest
  })
}

/**
 * Detect field-level conflicts for operations with embedded _ops
 */
function detectFieldConflicts(
  target: string,
  ourEvent: Event,
  theirEvent: Event,
  ourOps: UpdateOps,
  theirOps: UpdateOps
): ConflictInfo[] {
  const conflicts: ConflictInfo[] = []

  // Get fields affected by each operation
  const ourFields = getAffectedFields(ourOps)
  const theirFields = getAffectedFields(theirOps)

  // Find overlapping fields
  for (const field of ourFields) {
    if (theirFields.has(field)) {
      // Both operations affect the same field
      // Check if the specific operators on this field are commutative
      const ourFieldOps = extractFieldOperations(ourOps, field)
      const theirFieldOps = extractFieldOperations(theirOps, field)

      if (!isCommutative(ourFieldOps, theirFieldOps)) {
        // Get the values from before/after states
        const ourValue = getFieldValue(ourEvent.after as Variant, field)
        const theirValue = getFieldValue(theirEvent.after as Variant, field)
        const baseValue = getFieldValue(ourEvent.before as Variant, field)

        // Only conflict if values are actually different
        if (!deepEqual(ourValue, theirValue)) {
          conflicts.push({
            type: 'concurrent_update',
            target,
            field,
            ourValue,
            theirValue,
            baseValue,
            ourEvent,
            theirEvent,
            ourOps,
            theirOps,
          })
        }
      }
    }
  }

  return conflicts
}

/**
 * Extract operations that affect a specific field
 */
function extractFieldOperations(ops: UpdateOps, field: string): UpdateOps {
  const result: UpdateOps = {}

  for (const [opType, opValue] of Object.entries(ops)) {
    if (opType.startsWith('$') && opValue && typeof opValue === 'object') {
      const fieldValue = (opValue as Record<string, unknown>)[field]
      if (fieldValue !== undefined) {
        result[opType] = { [field]: fieldValue }
      }
    }
  }

  return result
}

/**
 * Get a field value from a Variant object (supports dot notation)
 */
function getFieldValue(obj: Variant | undefined, field: string): unknown {
  if (!obj) return undefined

  const parts = field.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Detect field-level conflicts by comparing before/after states
 * Used when events don't have embedded _ops
 */
function detectFieldLevelConflicts(
  target: string,
  ourEvent: Event,
  theirEvent: Event
): ConflictInfo[] {
  const conflicts: ConflictInfo[] = []

  const ourBefore = ourEvent.before as Variant | undefined
  const ourAfter = ourEvent.after as Variant | undefined
  const theirBefore = theirEvent.before as Variant | undefined
  const theirAfter = theirEvent.after as Variant | undefined

  if (!ourAfter || !theirAfter) {
    return conflicts
  }

  // Find fields that changed in our event
  const ourChangedFields = getChangedFields(ourBefore, ourAfter)

  // Find fields that changed in their event
  const theirChangedFields = getChangedFields(theirBefore, theirAfter)

  // Check for overlapping changed fields
  for (const field of ourChangedFields) {
    if (theirChangedFields.has(field)) {
      const ourValue = getFieldValue(ourAfter, field)
      const theirValue = getFieldValue(theirAfter, field)
      const baseValue = getFieldValue(ourBefore, field)

      // Only conflict if values are different
      if (!deepEqual(ourValue, theirValue)) {
        conflicts.push({
          type: 'concurrent_update',
          target,
          field,
          ourValue,
          theirValue,
          baseValue,
          ourEvent,
          theirEvent,
        })
      }
    }
  }

  return conflicts
}

/**
 * Get the set of fields that changed between two states
 */
function getChangedFields(before: Variant | undefined, after: Variant | undefined): Set<string> {
  const changed = new Set<string>()

  if (!after) return changed

  // Check all fields in after
  for (const [key, value] of Object.entries(after)) {
    // Skip internal fields
    if (key === '_ops') continue

    const beforeValue = before?.[key]
    if (!deepEqual(value, beforeValue)) {
      changed.add(key)
    }
  }

  // Check for removed fields
  if (before) {
    for (const key of Object.keys(before)) {
      if (!(key in after)) {
        changed.add(key)
      }
    }
  }

  return changed
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Check if an event represents a delete operation
 */
export function isDeleteEvent(event: Event): boolean {
  return event.op === 'DELETE' || event.op === 'REL_DELETE'
}

/**
 * Check if an event represents an update operation
 */
export function isUpdateEvent(event: Event): boolean {
  return event.op === 'UPDATE'
}

/**
 * Check if an event represents a create operation
 */
export function isCreateEvent(event: Event): boolean {
  return event.op === 'CREATE' || event.op === 'REL_CREATE'
}

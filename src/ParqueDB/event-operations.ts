/**
 * ParqueDB Event Operations Module
 *
 * Contains operations for event sourcing: recording events, flushing to storage,
 * time-travel queries, history, diff, and revert operations.
 */

import type {
  Entity,
  EntityId,
  StorageBackend,
  Event,
  EventOp,
  Variant,
  HistoryOptions,
} from '../types'

import { entityTarget, parseEntityTarget, isRelationshipTarget } from '../types'
import { generateId } from '../utils'
import { variantAsEntity, variantAsEntityOrNull } from '../types/cast'
import { logger } from '../utils/logger'

import type {
  Snapshot,
  SnapshotConfig,
  SnapshotQueryStats,
  SnapshotManager,
  HistoryItem,
  HistoryResult,
  DiffResult,
  RevertOptions,
  EventLogConfig,
  ArchiveEventsResult,
} from './types'

import { EntityNotFoundError, EventError } from './types'

import {
  addToEntityEventIndex,
  getFromEntityEventIndex,
  addToReconstructionCache,
  getFromReconstructionCache,
  invalidateReconstructionCache,
} from './store'

import { validateNamespace, toFullId } from './validation'
import { pruneArchivedEvents } from './events'

// =============================================================================
// Types - Focused Context Interfaces
// =============================================================================

/**
 * Minimal context for event log read operations.
 * Used by functions that only need to read events.
 */
export interface EventReadContext {
  events: Event[]
  entities: Map<string, Entity>
}

/**
 * Context for event time-travel reconstruction.
 * Used by functions that reconstruct entity state from events.
 */
export interface EventReconstructionContext extends EventReadContext {
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
  entityEventIndex: Map<string, Event[]>
  reconstructionCache: Map<string, { entity: Entity | null; timestamp: number }>
}

/**
 * Context for event flush operations.
 * Used by functions that flush pending events to storage.
 */
export interface EventFlushContext extends EventReconstructionContext {
  storage: StorageBackend
  pendingEvents: Event[]
  flushPromise: Promise<void> | null
  setFlushPromise: (promise: Promise<void> | null) => void
  setPendingEvents: (events: Event[]) => void
}

/**
 * Context for event archival operations.
 * Used by functions that manage event log rotation and archival.
 */
export interface EventArchivalContext extends EventFlushContext {
  archivedEvents: Event[]
  eventLogConfig: Required<EventLogConfig>
}

/**
 * Context for event recording operations.
 * Used by functions that write events.
 */
export interface EventRecordingContext extends EventArchivalContext {
  snapshotConfig: SnapshotConfig
  inTransaction: boolean
  getSnapshotManager: () => SnapshotManager
  /**
   * Optional callback invoked after each event is recorded.
   * Used for materialized view integration to emit events to the MV system.
   */
  onEvent?: ((event: Event) => void | Promise<void>) | undefined
}

/**
 * Full context object for event operations.
 * Provides access to all shared state and dependencies.
 *
 * @deprecated Prefer using focused contexts (EventReadContext, EventReconstructionContext, etc.)
 * where possible for better testability and reduced coupling.
 */
export interface EventOperationsContext extends EventRecordingContext {
  // All properties are inherited from EventRecordingContext
  // This interface exists for backward compatibility
}

// =============================================================================
// Event Recording
// =============================================================================

/**
 * Record an event for an entity operation.
 * Returns a promise that resolves when the event is flushed to storage.
 *
 * Uses EventRecordingContext - requires event recording dependencies.
 */
export function recordEvent(
  ctx: EventRecordingContext,
  op: EventOp,
  target: string,
  before: Entity | null,
  after: Entity | null,
  actor?: EntityId,
  meta?: Record<string, unknown>
): Promise<void> {
  // Deep copy to prevent mutation of stored event state
  const deepCopy = <T>(obj: T | null): T | undefined => {
    if (obj === null) return undefined
    return JSON.parse(JSON.stringify(obj))
  }

  const processAfter = (obj: Entity | null): Variant | undefined => {
    if (obj === null) return undefined
    return deepCopy(obj) as Variant | undefined
  }

  const event: Event = {
    id: generateId(),
    ts: Date.now(),
    op,
    target,
    before: deepCopy(before) as Variant | undefined,
    after: processAfter(after),
    actor: actor as string | undefined,
    metadata: meta as Variant | undefined,
  }

  ctx.events.push(event)

  // Update entity event index for O(1) lookups
  if (!isRelationshipTarget(target)) {
    addToEntityEventIndex(ctx.entityEventIndex, target, event)

    // Invalidate reconstruction cache for this entity since its state changed
    const { ns, id } = parseEntityTarget(target)
    const fullId = `${ns}/${id}`
    invalidateReconstructionCache(ctx.reconstructionCache, fullId)
  }

  // Emit event to MV system if callback is configured
  // This is fire-and-forget - we don't want MV processing to block writes
  if (ctx.onEvent) {
    try {
      // Call asynchronously to avoid blocking the write path
      const result = ctx.onEvent(event)
      if (result instanceof Promise) {
        // Don't await - fire and forget for MV updates
        result.catch((err) => {
          logger.warn('[ParqueDB] MV event callback error:', err)
        })
      }
    } catch (err) {
      logger.warn('[ParqueDB] MV event callback error:', err)
    }
  }

  // Add to pending events buffer
  ctx.pendingEvents.push(event)

  // Schedule a batched flush (unless in transaction)
  const flushPromise = ctx.inTransaction ? null : scheduleFlush(ctx)

  // Perform event log rotation if needed
  maybeRotateEventLog(ctx)

  // Auto-snapshot if threshold is configured and reached
  if (ctx.snapshotConfig.autoSnapshotThreshold && after && !isRelationshipTarget(target)) {
    const { ns, id } = parseEntityTarget(target)
    const fullEntityId = `${ns}/${id}` as EntityId
    const entityEventCount = ctx.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === id
    }).length
    const existingSnapshots = ctx.snapshots.filter(s => s.entityId === fullEntityId)
    const lastSnapshot = existingSnapshots.length > 0 ? existingSnapshots[existingSnapshots.length - 1] : undefined
    const lastSnapshotSeq = lastSnapshot?.sequenceNumber ?? 0
    const eventsSinceLastSnapshot = entityEventCount - lastSnapshotSeq
    if (eventsSinceLastSnapshot >= ctx.snapshotConfig.autoSnapshotThreshold) {
      ctx.getSnapshotManager().createSnapshot(fullEntityId).catch((err) => {
        logger.warn(`[ParqueDB] Auto-snapshot failed for ${fullEntityId}:`, err)
      })
    }
  }

  return flushPromise ?? Promise.resolve()
}

// =============================================================================
// Flush Operations
// =============================================================================

/**
 * Schedule a batched flush of pending events using microtask timing.
 *
 * Uses EventFlushContext - requires flush dependencies.
 */
export function scheduleFlush(ctx: EventFlushContext): Promise<void> {
  if (ctx.flushPromise) return ctx.flushPromise

  const promise = Promise.resolve().then(() => flushEvents(ctx))
  ctx.setFlushPromise(promise)
  return promise
}

/**
 * Flush pending events to storage in a transactional manner.
 *
 * Uses EventFlushContext - requires flush dependencies.
 */
export async function flushEvents(ctx: EventFlushContext): Promise<void> {
  if (ctx.pendingEvents.length === 0) {
    ctx.setFlushPromise(null)
    return
  }

  const eventsToFlush = [...ctx.pendingEvents]

  const eventData = JSON.stringify(eventsToFlush)
  try {
    // Step 1: Write to event log
    await ctx.storage.write(`data/events.jsonl`, new TextEncoder().encode(eventData))

    // Step 2: Write entity data for each affected namespace
    const affectedNamespaces = new Set(eventsToFlush.map(e => {
      if (isRelationshipTarget(e.target)) return null
      return parseEntityTarget(e.target).ns
    }).filter((ns): ns is string => ns !== null))

    for (const ns of affectedNamespaces) {
      const nsEntities: Entity[] = []
      ctx.entities.forEach((entity, id) => {
        if (id.startsWith(`${ns}/`)) {
          nsEntities.push(entity)
        }
      })
      const entityData = JSON.stringify(nsEntities)
      await ctx.storage.write(`data/${ns}/data.json`, new TextEncoder().encode(entityData))
    }

    // Step 3: Write namespace event logs
    for (const ns of affectedNamespaces) {
      const nsEvents = eventsToFlush.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        return parseEntityTarget(e.target).ns === ns
      })
      const nsEventData = JSON.stringify(nsEvents)
      await ctx.storage.write(`${ns}/events.json`, new TextEncoder().encode(nsEventData))
    }

    // Only clear the events that were successfully flushed
    ctx.setPendingEvents(ctx.pendingEvents.slice(eventsToFlush.length))

    // Schedule another flush if more events arrived
    if (ctx.pendingEvents.length > 0) {
      ctx.setFlushPromise(Promise.resolve().then(() => flushEvents(ctx)))
    } else {
      ctx.setFlushPromise(null)
    }
  } catch (error: unknown) {
    // On write failure, rollback the in-memory changes
    for (const event of eventsToFlush) {
      const idx = ctx.events.indexOf(event)
      if (idx !== -1) {
        ctx.events.splice(idx, 1)
      }

      const { ns, id } = isRelationshipTarget(event.target) ? { ns: '', id: '' } : parseEntityTarget(event.target)
      const fullId = `${ns}/${id}`
      if (event.op === 'CREATE') {
        ctx.entities.delete(fullId)
      } else if (event.op === 'UPDATE' && event.before) {
        ctx.entities.set(fullId, variantAsEntity(event.before))
      } else if (event.op === 'DELETE' && event.before) {
        ctx.entities.set(fullId, variantAsEntity(event.before))
      }
    }

    ctx.setPendingEvents(ctx.pendingEvents.filter(e => !eventsToFlush.includes(e)))

    if (ctx.pendingEvents.length > 0) {
      ctx.setFlushPromise(Promise.resolve().then(() => flushEvents(ctx)))
    } else {
      ctx.setFlushPromise(null)
    }

    throw error
  }
}

// =============================================================================
// Event Log Rotation
// =============================================================================

/**
 * Check and perform event log rotation if configured limits are exceeded.
 *
 * Uses EventArchivalContext - requires archival dependencies.
 */
export function maybeRotateEventLog(ctx: EventArchivalContext): void {
  const maxEvents = ctx.eventLogConfig.maxEvents ?? 10000
  const maxAge = ctx.eventLogConfig.maxAge ?? 7 * 24 * 60 * 60 * 1000
  const archiveOnRotation = ctx.eventLogConfig.archiveOnRotation ?? false
  const maxArchivedEvents = ctx.eventLogConfig.maxArchivedEvents ?? 50000
  const now = Date.now()
  const ageCutoff = now - maxAge

  let eventsToRotate: Event[] = []
  let eventsToKeep: Event[] = []

  for (const event of ctx.events) {
    if (event.ts < ageCutoff) {
      eventsToRotate.push(event)
    } else {
      eventsToKeep.push(event)
    }
  }

  if (eventsToKeep.length > maxEvents) {
    eventsToKeep.sort((a, b) => a.ts - b.ts)
    const excessCount = eventsToKeep.length - maxEvents
    const excessEvents = eventsToKeep.slice(0, excessCount)
    eventsToRotate = [...eventsToRotate, ...excessEvents]
    eventsToKeep = eventsToKeep.slice(excessCount)
  }

  if (eventsToRotate.length > 0) {
    if (archiveOnRotation) {
      ctx.archivedEvents.push(...eventsToRotate)
      pruneArchivedEvents(ctx.archivedEvents, maxArchivedEvents)
    }

    ctx.events.length = 0
    ctx.events.push(...eventsToKeep)
  }
}

/**
 * Archive events manually based on criteria.
 *
 * Uses EventArchivalContext - requires archival dependencies.
 */
export function archiveEvents(
  ctx: EventArchivalContext,
  options?: { olderThan?: Date | undefined; maxEvents?: number | undefined }
): ArchiveEventsResult {
  const now = Date.now()
  const configMaxAge = ctx.eventLogConfig.maxAge ?? 7 * 24 * 60 * 60 * 1000
  const configMaxEvents = ctx.eventLogConfig.maxEvents ?? 10000
  const archiveOnRotation = ctx.eventLogConfig.archiveOnRotation ?? false
  const maxArchivedEvents = ctx.eventLogConfig.maxArchivedEvents ?? 50000
  const olderThanTs = options?.olderThan?.getTime() ?? (now - configMaxAge)
  const maxEventsToKeep = options?.maxEvents ?? configMaxEvents

  let archivedCount = 0
  let droppedCount = 0
  let prunedCount = 0
  let newestArchivedTs: number | undefined
  let eventsToArchive: Event[] = []
  let eventsToKeep: Event[] = []

  for (const event of ctx.events) {
    if (event.ts < olderThanTs) {
      eventsToArchive.push(event)
      if (newestArchivedTs === undefined || event.ts > newestArchivedTs) {
        newestArchivedTs = event.ts
      }
    } else {
      eventsToKeep.push(event)
    }
  }

  if (eventsToKeep.length > maxEventsToKeep) {
    eventsToKeep.sort((a, b) => a.ts - b.ts)
    const excessCount = eventsToKeep.length - maxEventsToKeep
    const excessEvents = eventsToKeep.slice(0, excessCount)
    for (const event of excessEvents) {
      eventsToArchive.push(event)
      if (newestArchivedTs === undefined || event.ts > newestArchivedTs) {
        newestArchivedTs = event.ts
      }
    }
    eventsToKeep = eventsToKeep.slice(excessCount)
  }

  if (archiveOnRotation) {
    ctx.archivedEvents.push(...eventsToArchive)
    archivedCount = eventsToArchive.length
    prunedCount = pruneArchivedEvents(ctx.archivedEvents, maxArchivedEvents)
  } else {
    droppedCount = eventsToArchive.length
  }

  ctx.events.length = 0
  ctx.events.push(...eventsToKeep)

  const oldestEventTs = eventsToKeep.length > 0
    ? Math.min(...eventsToKeep.map(e => e.ts))
    : undefined

  return {
    archivedCount,
    droppedCount,
    prunedCount,
    oldestEventTs,
    newestArchivedTs,
  }
}

// =============================================================================
// Time-Travel Reconstruction
// =============================================================================

/**
 * Reconstruct entity state at a specific point in time using event sourcing.
 *
 * Uses EventReconstructionContext - only needs reconstruction-related dependencies.
 */
export function reconstructEntityAtTime(
  ctx: EventReconstructionContext,
  fullId: string,
  asOf: Date
): Entity | null {
  const [ns, ...idParts] = fullId.split('/')
  const entityId = idParts.join('/')
  const asOfTime = asOf.getTime()

  // Check cache first
  const cachedResult = getFromReconstructionCache(ctx.reconstructionCache, fullId, asOfTime)
  if (cachedResult !== undefined) {
    return cachedResult
  }

  // Use entity event index for O(1) lookup
  const target = entityTarget(ns ?? '', entityId)
  const indexedEvents = getFromEntityEventIndex(ctx.entityEventIndex, target)

  if (indexedEvents.length === 0) {
    // Fall back to O(n) filter for backwards compatibility
    const filteredEvents = ctx.events
      .filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === entityId
      })
      .sort((a, b) => {
        const timeDiff = a.ts - b.ts
        if (timeDiff !== 0) return timeDiff
        return a.id.localeCompare(b.id)
      })

    if (filteredEvents.length === 0) {
      addToReconstructionCache(ctx.reconstructionCache, fullId, asOfTime, null)
      return null
    }

    // Populate the index for future queries
    for (const event of filteredEvents) {
      addToEntityEventIndex(ctx.entityEventIndex, target, event)
    }

    return reconstructFromEvents(ctx, fullId, asOfTime, filteredEvents)
  }

  return reconstructFromEvents(ctx, fullId, asOfTime, indexedEvents)
}

/**
 * Helper method to reconstruct entity state from a sorted array of events.
 *
 * Uses EventReconstructionContext - only needs reconstruction-related dependencies.
 */
export function reconstructFromEvents(
  ctx: EventReconstructionContext,
  fullId: string,
  asOfTime: number,
  allEvents: Event[]
): Entity | null {
  const targetEventIndex = binarySearchLastEventBeforeTime(allEvents, asOfTime)

  if (targetEventIndex === -1) {
    addToReconstructionCache(ctx.reconstructionCache, fullId, asOfTime, null)
    return null
  }

  // Check if we can use a snapshot for optimization
  const entitySnapshots = ctx.snapshots
    .filter(s => s.entityId === fullId)
    .sort((a, b) => a.sequenceNumber - b.sequenceNumber)

  let bestSnapshot: Snapshot | null = null
  for (const snapshot of entitySnapshots) {
    if (snapshot.sequenceNumber - 1 <= targetEventIndex) {
      bestSnapshot = snapshot
    } else {
      break
    }
  }

  const stats: SnapshotQueryStats = {
    snapshotsUsed: 0,
    eventsReplayed: 0,
    snapshotUsedAt: undefined,
  }

  let entity: Entity | null = null
  let startIndex = 0

  if (bestSnapshot) {
    entity = { ...bestSnapshot.state } as Entity
    startIndex = bestSnapshot.sequenceNumber
    stats.snapshotsUsed = 1
    stats.snapshotUsedAt = bestSnapshot.sequenceNumber
    stats.eventsReplayed = targetEventIndex - (bestSnapshot.sequenceNumber - 1)
  } else {
    stats.snapshotsUsed = 0
    stats.eventsReplayed = targetEventIndex + 1
  }

  for (let i = startIndex; i <= targetEventIndex; i++) {
    const event = allEvents[i]!
    if (event.after) {
      entity = { ...event.after } as Entity
    } else if (event.op === 'DELETE') {
      entity = null
    }
  }

  ctx.queryStats.set(fullId, stats)
  addToReconstructionCache(ctx.reconstructionCache, fullId, asOfTime, entity)

  return entity
}

/**
 * Binary search to find the last event with timestamp <= target.
 */
export function binarySearchLastEventBeforeTime(events: Event[], targetTime: number): number {
  if (events.length === 0) return -1

  let left = 0
  let right = events.length - 1
  let result = -1

  while (left <= right) {
    const mid = Math.floor((left + right) / 2)
    const event = events[mid]!

    if (event.ts <= targetTime) {
      result = mid
      left = mid + 1
    } else {
      right = mid - 1
    }
  }

  return result
}

// =============================================================================
// History Operations
// =============================================================================

/**
 * Get entity history.
 *
 * Uses EventReadContext - only needs read access to events.
 */
export async function getEntityHistory(
  ctx: EventReadContext,
  entityId: EntityId,
  options?: HistoryOptions
): Promise<HistoryResult> {
  const fullId = entityId as string
  const [ns, ...idParts] = fullId.split('/')
  const id = idParts.join('/')

  let relevantEvents = ctx.events.filter(e => {
    if (isRelationshipTarget(e.target)) return false
    const info = parseEntityTarget(e.target)
    return info.ns === ns && info.id === id
  })

  if (options?.from) {
    const fromTime = options.from.getTime()
    relevantEvents = relevantEvents.filter(e => e.ts > fromTime)
  }

  if (options?.to) {
    const toTime = options.to.getTime()
    relevantEvents = relevantEvents.filter(e => e.ts <= toTime)
  }

  if (options?.op) {
    relevantEvents = relevantEvents.filter(e => e.op === options.op)
  }

  if (options?.actor) {
    relevantEvents = relevantEvents.filter(e => e.actor === options.actor)
  }

  relevantEvents.sort((a, b) => {
    const timeDiff = a.ts - b.ts
    if (timeDiff !== 0) return timeDiff
    return a.id.localeCompare(b.id)
  })

  if (options?.cursor) {
    const cursorIndex = relevantEvents.findIndex(e => e.id === options.cursor)
    if (cursorIndex !== -1) {
      relevantEvents = relevantEvents.slice(cursorIndex + 1)
    }
  }

  const limit = options?.limit ?? 1000
  const hasMore = relevantEvents.length > limit
  const items: HistoryItem[] = relevantEvents.slice(0, limit).map(e => {
    const targetInfo = parseEntityTarget(e.target)
    return {
      id: e.id,
      ts: new Date(e.ts),
      op: e.op,
      entityId: targetInfo.id,
      ns: targetInfo.ns,
      before: variantAsEntityOrNull(e.before),
      after: variantAsEntityOrNull(e.after),
      actor: e.actor as EntityId | undefined,
      metadata: e.metadata,
    }
  })

  return {
    items,
    hasMore,
    nextCursor: hasMore && items.length > 0 ? items[items.length - 1]!.id : undefined,
  }
}

/**
 * Get entity at a specific version.
 *
 * Uses EventReadContext - only needs read access to events.
 */
export async function getEntityAtVersion<T = Record<string, unknown>>(
  ctx: EventReadContext,
  namespace: string,
  id: string,
  version: number
): Promise<Entity<T> | null> {
  validateNamespace(namespace)

  const fullId = toFullId(namespace, id)
  const [ns, ...idParts] = fullId.split('/')
  const entityId = idParts.join('/')

  const relevantEvents = ctx.events
    .filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === entityId
    })
    .sort((a, b) => a.ts - b.ts)

  let entity: Entity | null = null
  let currentVersion = 0

  for (const event of relevantEvents) {
    if (event.op === 'CREATE') {
      entity = event.after ? { ...event.after } as Entity : null
      currentVersion = entity?.version ?? 1
    } else if (event.op === 'UPDATE' && entity) {
      entity = event.after ? { ...event.after } as Entity : entity
      currentVersion = entity?.version ?? currentVersion + 1
    } else if (event.op === 'DELETE' && entity) {
      if (event.after) {
        entity = { ...event.after } as Entity
        currentVersion = entity?.version ?? currentVersion + 1
      }
    }

    if (currentVersion >= version) {
      break
    }
  }

  if (!entity || entity.version !== version) {
    return null
  }

  return entity as Entity<T>
}

// =============================================================================
// Diff and Revert Operations
// =============================================================================

/**
 * Compute diff between entity states at two timestamps.
 *
 * Uses EventReconstructionContext - needs reconstruction to compute diffs.
 */
export async function computeDiff(
  ctx: EventReconstructionContext,
  entityId: EntityId,
  t1: Date,
  t2: Date
): Promise<DiffResult> {
  const fullId = entityId as string
  const state1 = reconstructEntityAtTime(ctx, fullId, t1)
  const state2 = reconstructEntityAtTime(ctx, fullId, t2)

  const added: string[] = []
  const removed: string[] = []
  const changed: string[] = []
  const values: { [field: string]: { before: unknown; after: unknown } } = {}

  const metaFields = new Set(['$id', '$type', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'version', 'deletedAt', 'deletedBy'])

  const getAllPaths = (obj: Record<string, unknown> | null, prefix = ''): Map<string, unknown> => {
    const paths = new Map<string, unknown>()
    if (!obj) return paths

    for (const [key, value] of Object.entries(obj)) {
      if (metaFields.has(key)) continue
      const path = prefix ? `${prefix}.${key}` : key

      if (value && typeof value === 'object' && !Array.isArray(value)) {
        const nestedPaths = getAllPaths(value as Record<string, unknown>, path)
        for (const [nestedPath, nestedValue] of nestedPaths) {
          paths.set(nestedPath, nestedValue)
        }
      } else {
        paths.set(path, value)
      }
    }

    return paths
  }

  const paths1 = getAllPaths(state1 as Record<string, unknown> | null)
  const paths2 = getAllPaths(state2 as Record<string, unknown> | null)

  for (const [path, value2] of paths2) {
    if (!paths1.has(path)) {
      added.push(path)
      values[path] = { before: undefined, after: value2 }
    }
  }

  for (const [path, value1] of paths1) {
    if (!paths2.has(path)) {
      removed.push(path)
      values[path] = { before: value1, after: undefined }
    }
  }

  for (const [path, value1] of paths1) {
    if (paths2.has(path)) {
      const value2 = paths2.get(path)
      const v1Str = JSON.stringify(value1)
      const v2Str = JSON.stringify(value2)
      if (v1Str !== v2Str) {
        changed.push(path)
        values[path] = { before: value1, after: value2 }
      }
    }
  }

  return { added, removed, changed, values }
}

/**
 * Revert entity to its state at a specific timestamp.
 *
 * Uses EventReconstructionContext - needs reconstruction plus entity access.
 */
export async function revertEntity<T = Record<string, unknown>>(
  ctx: EventReconstructionContext,
  recordEventFn: (op: EventOp, target: string, before: Entity | null, after: Entity | null, actor?: EntityId, meta?: Record<string, unknown>) => Promise<void>,
  entityId: EntityId,
  targetTime: Date,
  options?: RevertOptions
): Promise<Entity<T>> {
  const fullId = entityId as string
  const [ns, ...idParts] = fullId.split('/')
  const id = idParts.join('/')

  if (targetTime.getTime() > Date.now()) {
    throw new EventError('Revert entity', 'Cannot revert to a future time', {
      entityId: fullId,
    })
  }

  const stateAtTarget = reconstructEntityAtTime(ctx, fullId, targetTime)
  if (!stateAtTarget) {
    throw new EventError('Revert entity', 'Entity did not exist at the target time', {
      entityId: fullId,
    })
  }

  const currentEntity = ctx.entities.get(fullId)
  if (!currentEntity) {
    throw new EntityNotFoundError(ns ?? '', id)
  }

  const actor = options?.actor || currentEntity.updatedBy
  const now = new Date()
  const beforeEntityForEvent = { ...currentEntity } as Entity

  const newState = {
    ...stateAtTarget,
    $id: currentEntity.$id,
    createdAt: currentEntity.createdAt,
    createdBy: currentEntity.createdBy,
    updatedAt: now,
    updatedBy: actor,
    version: (currentEntity.version || 1) + 1,
  } as Entity

  delete newState.deletedAt
  delete newState.deletedBy

  ctx.entities.set(fullId, newState)

  await recordEventFn('UPDATE', entityTarget(ns ?? '', id), beforeEntityForEvent, newState, actor, { revert: true })

  return newState as Entity<T>
}

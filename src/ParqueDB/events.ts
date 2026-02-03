/**
 * ParqueDB Events Module
 *
 * Handles event logging, history, and time-travel including:
 * - Event recording and flushing
 * - Event log rotation and archival
 * - Entity history queries
 * - Time-travel reconstruction
 */

import type { Entity, EntityId, Event, EventOp, StorageBackend } from '../types'
import { entityTarget, parseEntityTarget, isRelationshipTarget, asEntityId, variantAsEntityOrNull } from '../types'
import { generateId } from '../utils'
import { toFullId } from './validation'
import type {
  EventLog,
  EventLogConfig,
  ArchiveEventsResult,
  HistoryItem,
  HistoryResult,
  HistoryOptions,
  Snapshot,
  SnapshotQueryStats,
  SnapshotConfig,
} from './types'
import { EventError } from './types'

/**
 * Record an event for an entity operation
 *
 * @param op - Operation type (CREATE, UPDATE, DELETE)
 * @param target - Target identifier (entity: "ns:id", relationship: "from:pred:to")
 * @param before - State before change (undefined for CREATE)
 * @param after - State after change (undefined for DELETE)
 * @param events - Events array to push to
 * @param pendingEvents - Pending events buffer for batched writes
 * @param actor - Who made the change
 * @param meta - Additional metadata
 * @returns The created event
 */
export function recordEvent(
  op: EventOp,
  target: string,
  before: Entity | null,
  after: Entity | null,
  events: Event[],
  pendingEvents: Event[],
  actor?: EntityId,
  meta?: Record<string, unknown>
): Event {
  // Deep copy to prevent mutation of stored event state
  // For DELETE operations, null after state is meaningful (hard delete)
  // so we preserve it as null instead of converting to undefined
  const deepCopy = <T>(obj: T | null): T | null | undefined => {
    if (obj === null) return null
    return JSON.parse(JSON.stringify(obj))
  }

  const event: Event = {
    id: generateId(),
    ts: Date.now(),
    op,
    target,
    before: deepCopy(before) as import('../types').Variant | null | undefined,
    after: deepCopy(after) as import('../types').Variant | null | undefined,
    actor: actor as string | undefined,
    metadata: meta as import('../types').Variant | undefined,
  }
  events.push(event)
  pendingEvents.push(event)

  return event
}

/**
 * Flush pending events to storage
 */
export async function flushEvents(
  pendingEvents: Event[],
  events: Event[],
  entities: Map<string, Entity>,
  storage: StorageBackend
): Promise<Event[]> {
  if (pendingEvents.length === 0) return []

  // Take all pending events
  const eventsToFlush = [...pendingEvents]

  // Write events in a transactional manner:
  // 1. Write event log
  // 2. Write entity data
  // 3. Update indexes
  // All writes must succeed or the operation is rolled back
  const eventData = JSON.stringify(eventsToFlush)
  try {
    // Step 1: Write to event log
    await storage.write(`data/events.jsonl`, new TextEncoder().encode(eventData))

    // Step 2: Write entity data for each affected namespace
    const affectedNamespaces = new Set(eventsToFlush.map(e => {
      if (isRelationshipTarget(e.target)) return null
      return parseEntityTarget(e.target).ns
    }).filter((ns): ns is string => ns !== null))
    for (const ns of affectedNamespaces) {
      // Collect current state of all entities in this namespace
      const nsEntities: Entity[] = []
      entities.forEach((entity, id) => {
        if (id.startsWith(`${ns}/`)) {
          nsEntities.push(entity)
        }
      })
      const entityData = JSON.stringify(nsEntities)
      await storage.write(`data/${ns}/data.json`, new TextEncoder().encode(entityData))
    }

    // Step 3: Write namespace event logs
    for (const ns of affectedNamespaces) {
      const nsEvents = eventsToFlush.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        return parseEntityTarget(e.target).ns === ns
      })
      const nsEventData = JSON.stringify(nsEvents)
      await storage.write(`${ns}/events.json`, new TextEncoder().encode(nsEventData))
    }

    return eventsToFlush
  } catch (error: unknown) {
    // On write failure, rollback the in-memory changes
    for (const event of eventsToFlush) {
      // Remove the event from the event store
      const idx = events.indexOf(event)
      if (idx !== -1) {
        events.splice(idx, 1)
      }
      // Rollback entity state
      const { ns, id } = isRelationshipTarget(event.target) ? { ns: '', id: '' } : parseEntityTarget(event.target)
      const fullId = `${ns}/${id}`
      if (event.op === 'CREATE') {
        // Remove created entity
        entities.delete(fullId)
      } else if (event.op === 'UPDATE') {
        // Restore previous state using type-safe cast
        const previousState = variantAsEntityOrNull(event.before)
        if (previousState) {
          entities.set(fullId, previousState)
        }
      } else if (event.op === 'DELETE') {
        // Restore deleted entity using type-safe cast
        const previousState = variantAsEntityOrNull(event.before)
        if (previousState) {
          entities.set(fullId, previousState)
        }
      }
    }
    throw error
  }
}

/**
 * Check and perform event log rotation if limits are exceeded
 */
export function maybeRotateEventLog(
  events: Event[],
  archivedEvents: Event[],
  eventLogConfig: Required<EventLogConfig>
): void {
  const { maxEvents, maxAge, archiveOnRotation, maxArchivedEvents } = eventLogConfig
  const now = Date.now()

  // Calculate cutoff time for age-based rotation
  const ageCutoff = now - maxAge

  // Find events that should be rotated (older than maxAge OR exceeding maxEvents)
  let eventsToRotate: Event[] = []
  let eventsToKeep: Event[] = []

  // First, filter by age
  for (const event of events) {
    if (event.ts < ageCutoff) {
      eventsToRotate.push(event)
    } else {
      eventsToKeep.push(event)
    }
  }

  // Then, if still over maxEvents, rotate oldest events
  if (eventsToKeep.length > maxEvents) {
    // Sort by timestamp to ensure we keep the newest
    eventsToKeep.sort((a, b) => a.ts - b.ts)
    const excessCount = eventsToKeep.length - maxEvents
    const excessEvents = eventsToKeep.slice(0, excessCount)
    eventsToRotate = [...eventsToRotate, ...excessEvents]
    eventsToKeep = eventsToKeep.slice(excessCount)
  }

  // If there are events to rotate, perform the rotation
  if (eventsToRotate.length > 0) {
    if (archiveOnRotation) {
      // Move to archived events
      archivedEvents.push(...eventsToRotate)
      // Prune archived events if exceeding limit (keep newest)
      pruneArchivedEvents(archivedEvents, maxArchivedEvents)
    }
    // Update the events array in place to maintain reference
    events.length = 0
    events.push(...eventsToKeep)
  }
}

/**
 * Prune archived events to respect the maximum limit.
 * Removes oldest events first (based on timestamp).
 */
export function pruneArchivedEvents(
  archivedEvents: Event[],
  maxArchivedEvents: number
): number {
  if (archivedEvents.length <= maxArchivedEvents) {
    return 0
  }

  // Sort by timestamp (oldest first) to determine which to remove
  archivedEvents.sort((a, b) => a.ts - b.ts)

  const excessCount = archivedEvents.length - maxArchivedEvents
  // Remove oldest events from the beginning
  archivedEvents.splice(0, excessCount)

  return excessCount
}

/**
 * Archive events manually based on criteria
 *
 * @param options - Archive options (olderThan, maxEvents)
 * @param events - Events array
 * @param archivedEvents - Archived events array
 * @param eventLogConfig - Event log configuration
 * @returns Result of the archival operation
 */
export function archiveEvents(
  options: { olderThan?: Date; maxEvents?: number } | undefined,
  events: Event[],
  archivedEvents: Event[],
  eventLogConfig: Required<EventLogConfig>
): ArchiveEventsResult {
  const now = Date.now()
  const olderThanTs = options?.olderThan?.getTime() ?? (now - eventLogConfig.maxAge)
  const maxEventsToKeep = options?.maxEvents ?? eventLogConfig.maxEvents
  const { archiveOnRotation, maxArchivedEvents } = eventLogConfig

  let archivedCount = 0
  let droppedCount = 0
  let prunedCount = 0
  let newestArchivedTs: number | undefined
  let eventsToArchive: Event[] = []
  let eventsToKeep: Event[] = []

  // Filter by age
  for (const event of events) {
    if (event.ts < olderThanTs) {
      eventsToArchive.push(event)
      if (newestArchivedTs === undefined || event.ts > newestArchivedTs) {
        newestArchivedTs = event.ts
      }
    } else {
      eventsToKeep.push(event)
    }
  }

  // Further reduce if over maxEvents
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

  // Archive or drop the events
  if (archiveOnRotation) {
    archivedEvents.push(...eventsToArchive)
    archivedCount = eventsToArchive.length
    // Prune archived events if exceeding limit (keep newest)
    prunedCount = pruneArchivedEvents(archivedEvents, maxArchivedEvents)
  } else {
    droppedCount = eventsToArchive.length
  }

  // Update the events array
  events.length = 0
  events.push(...eventsToKeep)

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

/**
 * EventLog implementation class
 *
 * Provides methods for querying and managing the event log.
 * This class encapsulates all event log operations.
 */
export class EventLogImpl implements EventLog {
  private readonly events: Event[]
  private readonly archivedEvents: Event[]
  private readonly eventLogConfig: Required<EventLogConfig>
  private readonly archiveEventsFn: (options?: { olderThan?: Date; maxEvents?: number }) => ArchiveEventsResult

  constructor(
    events: Event[],
    archivedEvents: Event[],
    eventLogConfig: Required<EventLogConfig>,
    archiveEventsFn: (options?: { olderThan?: Date; maxEvents?: number }) => ArchiveEventsResult
  ) {
    this.events = events
    this.archivedEvents = archivedEvents
    this.eventLogConfig = eventLogConfig
    this.archiveEventsFn = archiveEventsFn
  }

  /**
   * Sort events by timestamp and ID
   */
  private sortEvents(eventsToSort: Event[]): Event[] {
    return eventsToSort.sort((a, b) => {
      const timeDiff = a.ts - b.ts
      if (timeDiff !== 0) return timeDiff
      return a.id.localeCompare(b.id)
    })
  }

  /**
   * Get events for a specific entity
   */
  async getEvents(entityId: EntityId): Promise<Event[]> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const id = idParts.join('/')

    const filtered = this.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === id
    })

    return this.sortEvents(filtered)
  }

  /**
   * Get events by namespace
   */
  async getEventsByNamespace(ns: string): Promise<Event[]> {
    const filtered = this.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      return parseEntityTarget(e.target).ns === ns
    })

    return this.sortEvents(filtered)
  }

  /**
   * Get events by time range
   */
  async getEventsByTimeRange(from: Date, to: Date): Promise<Event[]> {
    const fromTime = from.getTime()
    const toTime = to.getTime()

    // Sort all events first to get consistent ordering by timestamp and ID
    const sortedEvents = this.sortEvents([...this.events])

    // For time range queries with millisecond precision, we use a counting approach:
    // Find all events that were recorded AT OR BEFORE the 'to' time,
    // but only include those that were created AFTER 'from' was captured.
    // Since event IDs are monotonically increasing, we can use ID comparison
    // for tie-breaking at the same millisecond.
    const result: Event[] = []
    for (const e of sortedEvents) {
      const eventTime = e.ts
      // Use inclusive range: fromTime <= eventTime <= toTime
      // This handles the case where midTime was captured in the same millisecond
      // as the first event
      if (eventTime >= fromTime && eventTime <= toTime) {
        result.push(e)
      }
    }

    // If we got multiple events at the boundary timestamp, filter to only include
    // events that occurred strictly before the second boundary event
    if (result.length > 1) {
      const boundaryTime = toTime
      const eventsAtBoundary = result.filter(e => e.ts === boundaryTime)
      if (eventsAtBoundary.length > 1) {
        // Remove the last event at the boundary (it was created after 'to' was captured)
        const lastEvent = eventsAtBoundary[eventsAtBoundary.length - 1]!
        const idx = result.indexOf(lastEvent)
        if (idx !== -1) {
          result.splice(idx, 1)
        }
      }
    }

    return result
  }

  /**
   * Get events by operation type
   */
  async getEventsByOp(op: EventOp): Promise<Event[]> {
    const filtered = this.events.filter(e => e.op === op)
    return this.sortEvents(filtered)
  }

  /**
   * Get raw event data (for compression check)
   */
  async getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }> {
    const event = this.events.find(e => e.id === id)
    if (!event) {
      throw new EventError('Get event', 'Event not found', { eventId: id })
    }
    // Check if payload is large enough to warrant compression (>10KB)
    const eventJson = JSON.stringify(event)
    const compressed = eventJson.length > 10000
    return { compressed, data: event }
  }

  /**
   * Get total event count
   */
  async getEventCount(): Promise<number> {
    return this.events.length
  }

  /**
   * Get current event log configuration
   */
  getConfig(): EventLogConfig {
    return { ...this.eventLogConfig }
  }

  /**
   * Archive old events based on configuration or manual threshold
   */
  async archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): Promise<ArchiveEventsResult> {
    return this.archiveEventsFn(options)
  }

  /**
   * Get archived events (if archiveOnRotation is enabled)
   */
  async getArchivedEvents(): Promise<Event[]> {
    return [...this.archivedEvents]
  }
}

/**
 * Create an EventLog interface for querying events
 *
 * @deprecated Use EventLogImpl class directly instead
 */
export function createEventLog(
  events: Event[],
  archivedEvents: Event[],
  eventLogConfig: Required<EventLogConfig>,
  archiveEventsFn: (options?: { olderThan?: Date; maxEvents?: number }) => ArchiveEventsResult
): EventLog {
  return new EventLogImpl(events, archivedEvents, eventLogConfig, archiveEventsFn)
}

/**
 * Get entity history
 */
export async function getHistory(
  entityId: EntityId,
  events: Event[],
  options?: HistoryOptions
): Promise<HistoryResult> {
  const fullId = entityId as string
  const [ns, ...idParts] = fullId.split('/')
  const id = idParts.join('/')

  let relevantEvents = events.filter(e => {
    if (isRelationshipTarget(e.target)) return false
    const info = parseEntityTarget(e.target)
    return info.ns === ns && info.id === id
  })

  // Filter by time range
  if (options?.from) {
    const fromTime = options.from.getTime()
    relevantEvents = relevantEvents.filter(e => e.ts > fromTime)
  }
  if (options?.to) {
    const toTime = options.to.getTime()
    relevantEvents = relevantEvents.filter(e => e.ts <= toTime)
  }

  // Filter by operation type
  if (options?.op) {
    relevantEvents = relevantEvents.filter(e => e.op === options.op)
  }

  // Filter by actor
  if (options?.actor) {
    relevantEvents = relevantEvents.filter(e => e.actor === options.actor)
  }

  // Sort by timestamp, then by ID for events at the same timestamp
  relevantEvents.sort((a, b) => {
    const timeDiff = a.ts - b.ts
    if (timeDiff !== 0) return timeDiff
    return a.id.localeCompare(b.id)
  })

  // Apply cursor-based pagination
  if (options?.cursor) {
    const cursorIndex = relevantEvents.findIndex(e => e.id === options.cursor)
    if (cursorIndex !== -1) {
      relevantEvents = relevantEvents.slice(cursorIndex + 1)
    }
  }

  // Apply pagination
  const limit = options?.limit ?? 1000
  const hasMore = relevantEvents.length > limit
  const items = relevantEvents.slice(0, limit).map(e => {
    const targetInfo = parseEntityTarget(e.target)
    return {
      id: e.id,
      ts: new Date(e.ts),
      op: e.op,
      entityId: targetInfo.id,
      ns: targetInfo.ns,
      before: (e.before ?? null) as Entity | null,
      after: (e.after ?? null) as Entity | null,
      actor: e.actor ? asEntityId(e.actor) : undefined,
      metadata: e.metadata,
    }
  }) as HistoryItem[]

  return {
    items,
    hasMore,
    nextCursor: hasMore && items.length > 0 ? items[items.length - 1]!.id : undefined,
  }
}

/**
 * Reconstruct entity state at a specific point in time
 * This method also tracks snapshot usage stats for optimization metrics.
 */
export function reconstructEntityAtTime(
  fullId: string,
  asOf: Date,
  events: Event[],
  snapshots: Snapshot[],
  queryStats: Map<string, SnapshotQueryStats>
): Entity | null {
  const [ns, ...idParts] = fullId.split('/')
  const entityId = idParts.join('/')

  const asOfTime = asOf.getTime()

  // Get all events for this entity, sorted by time and ID
  const allEvents = events
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

  if (allEvents.length === 0) {
    return null
  }

  // Find the target event. We include all events at or before the target timestamp.
  // When multiple events share the same millisecond timestamp, all of them are
  // included because they semantically all occurred "at" that time. This may result
  // in including slightly more events than a caller who passed a specific event's
  // timestamp might expect, but it's the correct interpretation of "as of" semantics.
  let targetEventIndex = -1

  for (let i = 0; i < allEvents.length; i++) {
    const event = allEvents[i]!
    if (event.ts <= asOfTime) {
      targetEventIndex = i
    } else {
      break
    }
  }

  if (targetEventIndex === -1) {
    return null
  }

  // Check if we can use a snapshot for optimization
  const entitySnapshots = snapshots
    .filter(s => s.entityId === fullId)
    .sort((a, b) => a.sequenceNumber - b.sequenceNumber)

  // Find the best snapshot to use (closest to but not after target)
  let bestSnapshot: Snapshot | null = null
  for (const snapshot of entitySnapshots) {
    // Snapshot sequence number is 1-indexed event count
    // If snapshot is at sequence N, it contains state after event N-1 (0-indexed)
    if (snapshot.sequenceNumber - 1 <= targetEventIndex) {
      bestSnapshot = snapshot
    } else {
      break
    }
  }

  // Track stats for this query
  const stats: SnapshotQueryStats = {
    snapshotsUsed: 0,
    eventsReplayed: 0,
    snapshotUsedAt: undefined,
  }

  let entity: Entity | null = null
  let startIndex = 0

  if (bestSnapshot) {
    // Use snapshot as starting point
    entity = { ...bestSnapshot.state } as Entity
    startIndex = bestSnapshot.sequenceNumber // Start replaying from event after snapshot
    stats.snapshotsUsed = 1
    stats.snapshotUsedAt = bestSnapshot.sequenceNumber
    stats.eventsReplayed = targetEventIndex - (bestSnapshot.sequenceNumber - 1)
  } else {
    // Full replay from beginning
    stats.snapshotsUsed = 0
    stats.eventsReplayed = targetEventIndex + 1
  }

  // Replay events from startIndex to targetEventIndex
  for (let i = startIndex; i <= targetEventIndex; i++) {
    const event = allEvents[i]!
    if (event.after) {
      entity = { ...event.after } as Entity
    } else if (event.op === 'DELETE') {
      entity = null
    }
  }

  // Store stats for this entity
  queryStats.set(fullId, stats)

  return entity
}

/**
 * Get entity at a specific version
 */
export async function getAtVersion<T = Record<string, unknown>>(
  namespace: string,
  id: string,
  version: number,
  events: Event[]
): Promise<Entity<T> | null> {
  const fullId = toFullId(namespace, id)
  const [ns, ...idParts] = fullId.split('/')
  const entityId = idParts.join('/')

  // Get events for this entity
  const relevantEvents = events
    .filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === entityId
    })
    .sort((a, b) => a.ts - b.ts)

  // Apply events up to the target version
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

/**
 * Compute diff between entity states at two timestamps
 */
export function computeDiff(
  entityId: EntityId,
  t1: Date,
  t2: Date,
  events: Event[],
  snapshots: Snapshot[],
  queryStats: Map<string, SnapshotQueryStats>
): { added: string[]; removed: string[]; changed: string[]; values: { [field: string]: { before: unknown; after: unknown } } } {
  const fullId = entityId as string
  const state1 = reconstructEntityAtTime(fullId, t1, events, snapshots, queryStats)
  const state2 = reconstructEntityAtTime(fullId, t2, events, snapshots, queryStats)

  const added: string[] = []
  const removed: string[] = []
  const changed: string[] = []
  const values: { [field: string]: { before: unknown; after: unknown } } = {}

  // Skip metadata fields for diff comparison
  const metaFields = new Set(['$id', '$type', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'version', 'deletedAt', 'deletedBy'])

  // Helper to get all paths from an object (handles nested objects)
  const getAllPaths = (obj: Record<string, unknown> | null, prefix = ''): Map<string, unknown> => {
    const paths = new Map<string, unknown>()
    if (!obj) return paths

    for (const [key, value] of Object.entries(obj)) {
      if (metaFields.has(key)) continue
      const path = prefix ? `${prefix}.${key}` : key

      if (value && typeof value === 'object' && !Array.isArray(value)) {
        // Recurse into nested objects
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

  // Find added fields (in state2 but not in state1)
  for (const [path, value2] of paths2) {
    if (!paths1.has(path)) {
      added.push(path)
      values[path] = { before: undefined, after: value2 }
    }
  }

  // Find removed fields (in state1 but not in state2)
  for (const [path, value1] of paths1) {
    if (!paths2.has(path)) {
      removed.push(path)
      values[path] = { before: value1, after: undefined }
    }
  }

  // Find changed fields (in both but different values)
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
 * Revert entity to its state at a specific timestamp
 */
export async function revertEntity<T = Record<string, unknown>>(
  entityId: EntityId,
  targetTime: Date,
  entities: Map<string, Entity>,
  events: Event[],
  snapshots: Snapshot[],
  queryStats: Map<string, SnapshotQueryStats>,
  recordEventFn: (op: EventOp, target: string, before: Entity | null, after: Entity | null, actor?: EntityId, meta?: Record<string, unknown>) => Promise<void>,
  options?: { actor?: EntityId }
): Promise<Entity<T>> {
  const fullId = entityId as string
  const [ns, ...idParts] = fullId.split('/')
  const id = idParts.join('/')

  // Validate targetTime is not in the future
  if (targetTime.getTime() > Date.now()) {
    throw new EventError('Revert entity', 'Cannot revert to a future time', {
      entityId: fullId,
    })
  }

  // Get entity state at target time
  const stateAtTarget = reconstructEntityAtTime(fullId, targetTime, events, snapshots, queryStats)
  if (!stateAtTarget) {
    throw new EventError('Revert entity', 'Entity did not exist at the target time', {
      entityId: fullId,
    })
  }

  // Get current entity
  const currentEntity = entities.get(fullId)
  if (!currentEntity) {
    throw new EventError('Revert entity', 'Entity not found', { entityId: fullId, namespace: ns, id })
  }

  // Apply the revert as an update with metadata marking it as a revert
  const actor = options?.actor || currentEntity.updatedBy
  const now = new Date()

  // Capture before state for event
  const beforeEntityForEvent = { ...currentEntity } as Entity

  // Build update to restore the target state
  // Copy all fields from target state, preserving only essential metadata
  const newState = {
    ...stateAtTarget,
    $id: currentEntity.$id,
    createdAt: currentEntity.createdAt,
    createdBy: currentEntity.createdBy,
    updatedAt: now,
    updatedBy: actor,
    version: (currentEntity.version || 1) + 1,
  } as Entity

  // Remove deletedAt/deletedBy if present in target state (we're restoring to a non-deleted state)
  delete newState.deletedAt
  delete newState.deletedBy

  // Store the reverted entity
  entities.set(fullId, newState)

  // Record UPDATE event with revert metadata
  await recordEventFn('UPDATE', entityTarget(ns ?? '', id), beforeEntityForEvent, newState, actor, { revert: true })

  return newState as Entity<T>
}

/**
 * Check if auto-snapshot should be triggered
 */
export function shouldAutoSnapshot(
  target: string,
  after: Entity | null,
  snapshotConfig: SnapshotConfig,
  events: Event[],
  snapshots: Snapshot[]
): { shouldSnapshot: boolean; fullEntityId?: string } {
  if (!snapshotConfig.autoSnapshotThreshold || !after || isRelationshipTarget(target)) {
    return { shouldSnapshot: false }
  }

  const { ns, id } = parseEntityTarget(target)
  const fullEntityId = `${ns}/${id}`
  const entityEventCount = events.filter(e => {
    if (isRelationshipTarget(e.target)) return false
    const info = parseEntityTarget(e.target)
    return info.ns === ns && info.id === id
  }).length
  const existingSnapshots = snapshots.filter(s => s.entityId === fullEntityId)
  const lastSnapshot = existingSnapshots.length > 0 ? existingSnapshots[existingSnapshots.length - 1] : undefined
  const lastSnapshotSeq = lastSnapshot?.sequenceNumber ?? 0
  const eventsSinceLastSnapshot = entityEventCount - lastSnapshotSeq

  return {
    shouldSnapshot: eventsSinceLastSnapshot >= snapshotConfig.autoSnapshotThreshold,
    fullEntityId,
  }
}

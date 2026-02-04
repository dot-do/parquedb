/**
 * Event Replay for Time-Travel Queries and Event Sourcing
 *
 * This module provides true event sourcing capabilities:
 * - Reconstruct entity state at any point in time
 * - Replay events forward from the beginning or from snapshots
 * - Replay events backward from current state
 * - Support for event versioning and schema evolution
 *
 * The events log is the source of truth. Entity state can always be
 * reconstructed by replaying events from the beginning.
 */

import type { Event, Variant } from '../types/entity'
import { parseEntityTarget as _parseEntityTarget, isRelationshipTarget as _isRelationshipTarget, entityTarget } from '../types/entity'
import type { EventBatch, ReplayResult } from './types'

// =============================================================================
// Types
// =============================================================================

/**
 * Source of events for replay
 */
export interface EventSource {
  /** Get events for an entity target in a time range */
  getEventsForTarget(target: string, minTs?: number, maxTs?: number): Promise<Event[]>
  /** Get all events in a time range */
  getEventsInRange(minTs: number, maxTs: number): Promise<Event[]>
}

/**
 * Options for replay
 */
export interface ReplayOptions {
  /** Target timestamp to replay to */
  at: number
  /** Starting state (if known) */
  currentState?: Variant | null | undefined
  /** Starting timestamp (when currentState was captured) */
  currentTs?: number | undefined
}

/**
 * Options for batch replay
 */
export interface BatchReplayOptions {
  /** Target timestamp to replay to */
  at: number
  /** Map of target -> current state */
  currentStates?: Map<string, Variant | null> | undefined
  /** Timestamp when current states were captured */
  currentTs?: number | undefined
}

/**
 * Options for the replayEvents convenience function
 */
export interface ReplayEventsOptions {
  /** Target timestamp to replay to (defaults to current time) */
  at?: number | undefined
  /** Use snapshot if available within this age (ms, default: Infinity) */
  maxSnapshotAge?: number | undefined
}

/**
 * Snapshot for fast event replay
 */
export interface Snapshot {
  /** Target this snapshot is for (ns:id) */
  target: string
  /** Timestamp when snapshot was taken */
  ts: number
  /** State at snapshot time */
  state: Variant | null
  /** Number of events that were replayed to create this snapshot */
  eventCount: number
  /** Event ID of the last event included in this snapshot */
  lastEventId: string
}

/**
 * Snapshot storage interface
 */
export interface SnapshotStorage {
  /** Get the latest snapshot for a target at or before a timestamp */
  getSnapshot(target: string, beforeTs?: number): Promise<Snapshot | null>
  /** Save a snapshot */
  saveSnapshot(snapshot: Snapshot): Promise<void>
  /** Delete snapshots older than a timestamp */
  deleteSnapshotsBefore(target: string, beforeTs: number): Promise<number>
  /** Get all snapshots for a target */
  getSnapshots(target: string): Promise<Snapshot[]>
}

/**
 * Event upgrader for schema evolution
 */
export type EventUpgrader = (event: Event, fromVersion: number, toVersion: number) => Event

/**
 * Configuration for event versioning
 */
export interface VersioningConfig {
  /** Current event schema version */
  currentVersion: number
  /** Upgrader function to migrate events to newer versions */
  upgrader?: EventUpgrader | undefined
}

/**
 * Extended replay result with additional metadata
 */
export interface ExtendedReplayResult extends ReplayResult {
  /** Whether a snapshot was used */
  usedSnapshot: boolean
  /** Snapshot timestamp if used */
  snapshotTs?: number | undefined
  /** Events replayed from snapshot (if snapshot was used) */
  eventsFromSnapshot?: number | undefined
  /** Version of the final state */
  version?: number | undefined
}

// =============================================================================
// EventReplayer Class
// =============================================================================

/**
 * Replays events to reconstruct state at a point in time.
 *
 * @example
 * ```typescript
 * const replayer = new EventReplayer(eventSource)
 *
 * // Reconstruct entity state at a specific time
 * const result = await replayer.replayEntity('users:u1', { at: timestamp })
 *
 * if (result.existed) {
 *   console.log('State at', timestamp, ':', result.state)
 * }
 *
 * // Use with snapshots for better performance
 * const snapshotReplayer = new EventReplayer(eventSource, {
 *   snapshotStorage,
 *   versioning: { currentVersion: 2, upgrader: myUpgrader }
 * })
 * ```
 */
export class EventReplayer {
  private eventSource: EventSource
  private snapshotStorage?: SnapshotStorage | undefined
  private versioning?: VersioningConfig | undefined

  constructor(
    eventSource: EventSource,
    options?: {
      snapshotStorage?: SnapshotStorage | undefined
      versioning?: VersioningConfig | undefined
    }
  ) {
    this.eventSource = eventSource
    this.snapshotStorage = options?.snapshotStorage
    this.versioning = options?.versioning
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Reconstruct an entity's state at a specific timestamp
   */
  async replayEntity(
    target: string,
    options: ReplayOptions
  ): Promise<ReplayResult> {
    const { at, currentState, currentTs } = options

    // Get all events for this entity
    const events = await this.eventSource.getEventsForTarget(target)

    if (events.length === 0) {
      // No events at all - entity never existed
      return {
        state: null,
        eventsReplayed: 0,
        existed: false,
      }
    }

    // Sort events by timestamp and ID
    const sortedEvents = this.sortEvents(events)

    // Find events up to the target timestamp
    const relevantEvents = sortedEvents.filter(e => e.ts <= at)

    if (relevantEvents.length === 0) {
      // No events before target time - entity didn't exist yet
      return {
        state: null,
        eventsReplayed: 0,
        existed: false,
      }
    }

    // Upgrade events if versioning is configured
    let eventsToReplay = relevantEvents
    if (this.versioning?.upgrader) {
      eventsToReplay = eventsToReplay.map(e => this.upgradeEvent(e))
    }

    // If we have a current state and it's after the target, replay backwards
    if (currentState !== undefined && currentTs !== undefined && currentTs > at) {
      // For backward replay, we need to upgrade all events
      let allEventsToReplay = sortedEvents
      if (this.versioning?.upgrader) {
        allEventsToReplay = allEventsToReplay.map(e => this.upgradeEvent(e))
      }
      return this.replayBackward(allEventsToReplay, currentState, currentTs, at)
    }

    // Otherwise, replay forward from the beginning
    return this.replayForward(eventsToReplay)
  }

  /**
   * Reconstruct multiple entities' states at a specific timestamp
   */
  async replayEntities(
    targets: string[],
    options: BatchReplayOptions
  ): Promise<Map<string, ReplayResult>> {
    const results = new Map<string, ReplayResult>()

    // For each target, replay independently
    // (Could be optimized to batch event fetching)
    await Promise.all(targets.map(async target => {
      const currentState = options.currentStates?.get(target) ?? undefined
      const result = await this.replayEntity(target, {
        at: options.at,
        currentState,
        currentTs: options.currentTs,
      })
      results.set(target, result)
    }))

    return results
  }

  /**
   * Get the state of an entity at each event (for debugging/audit)
   */
  async getStateHistory(
    target: string,
    options?: { minTs?: number | undefined; maxTs?: number | undefined }
  ): Promise<Array<{ ts: number; state: Variant | null; op: string }>> {
    const events = await this.eventSource.getEventsForTarget(
      target,
      options?.minTs,
      options?.maxTs
    )

    const sortedEvents = this.sortEvents(events)
    const history: Array<{ ts: number; state: Variant | null; op: string }> = []

    let state: Variant | null = null

    for (const event of sortedEvents) {
      state = this.applyEvent(state, event)
      history.push({
        ts: event.ts,
        state: state ? { ...state } : null,
        op: event.op,
      })
    }

    return history
  }

  /**
   * Reconstruct entity state using snapshots for efficiency.
   *
   * This is the recommended method for production use as it:
   * 1. Finds the nearest snapshot before the target timestamp
   * 2. Replays only the events after the snapshot
   * 3. Creates a new snapshot if many events were replayed
   */
  async replayWithSnapshot(
    target: string,
    options: ReplayOptions & { createSnapshot?: boolean | undefined; snapshotThreshold?: number | undefined }
  ): Promise<ExtendedReplayResult> {
    const { at, createSnapshot = true, snapshotThreshold = 100 } = options

    // Try to find a snapshot
    let snapshot: Snapshot | null = null
    if (this.snapshotStorage) {
      snapshot = await this.snapshotStorage.getSnapshot(target, at)
    }

    let events: Event[]
    let startState: Variant | null = null
    let eventsFromSnapshot = 0

    if (snapshot && snapshot.ts <= at) {
      // Replay from snapshot
      startState = snapshot.state
      events = await this.eventSource.getEventsForTarget(target, snapshot.ts)
      // Filter to events after snapshot and up to target time
      events = events.filter(e => e.id > snapshot!.lastEventId && e.ts <= at)
    } else {
      // Replay from beginning
      events = await this.eventSource.getEventsForTarget(target, undefined, at)
    }

    if (events.length === 0 && !snapshot) {
      return {
        state: null,
        eventsReplayed: 0,
        existed: false,
        usedSnapshot: false,
      }
    }

    // Sort and potentially upgrade events
    let sortedEvents = this.sortEvents(events)
    if (this.versioning?.upgrader) {
      sortedEvents = sortedEvents.map(e => this.upgradeEvent(e))
    }

    // Replay events
    let state = startState
    for (const event of sortedEvents) {
      state = this.applyEvent(state, event)
      eventsFromSnapshot++
    }

    // Create a new snapshot if many events were replayed
    if (
      createSnapshot &&
      this.snapshotStorage &&
      eventsFromSnapshot >= snapshotThreshold &&
      sortedEvents.length > 0
    ) {
      const lastEvent = sortedEvents[sortedEvents.length - 1]!
      await this.snapshotStorage.saveSnapshot({
        target,
        ts: at,
        state,
        eventCount: (snapshot?.eventCount ?? 0) + eventsFromSnapshot,
        lastEventId: lastEvent.id,
      })
    }

    return {
      state,
      eventsReplayed: eventsFromSnapshot,
      existed: state !== null,
      usedSnapshot: snapshot !== null,
      snapshotTs: snapshot?.ts,
      eventsFromSnapshot,
    }
  }

  /**
   * Create a snapshot at the current time for faster future replays
   */
  async createSnapshot(target: string): Promise<Snapshot | null> {
    if (!this.snapshotStorage) {
      throw new Error('SnapshotStorage not configured')
    }

    const events = await this.eventSource.getEventsForTarget(target)
    if (events.length === 0) {
      return null
    }

    const sortedEvents = this.sortEvents(events)
    let state: Variant | null = null

    for (const event of sortedEvents) {
      state = this.applyEvent(state, event)
    }

    const lastEvent = sortedEvents[sortedEvents.length - 1]!
    const snapshot: Snapshot = {
      target,
      ts: lastEvent.ts,
      state,
      eventCount: sortedEvents.length,
      lastEventId: lastEvent.id,
    }

    await this.snapshotStorage.saveSnapshot(snapshot)
    return snapshot
  }

  /**
   * Upgrade an event to the current schema version
   */
  private upgradeEvent(event: Event): Event {
    if (!this.versioning?.upgrader) {
      return event
    }

    // Check event version (stored in metadata)
    const eventVersion = (event.metadata as Record<string, unknown>)?.schemaVersion as number | undefined
    const currentVersion = this.versioning.currentVersion

    if (eventVersion === undefined || eventVersion >= currentVersion) {
      return event
    }

    return this.versioning.upgrader(event, eventVersion, currentVersion)
  }

  /**
   * Compact snapshots for a target - keep only the most recent one
   */
  async compactSnapshots(target: string, keepCount: number = 1): Promise<number> {
    if (!this.snapshotStorage) {
      return 0
    }

    const snapshots = await this.snapshotStorage.getSnapshots(target)
    if (snapshots.length <= keepCount) {
      return 0
    }

    // Sort by timestamp descending
    snapshots.sort((a, b) => b.ts - a.ts)

    // Keep the most recent ones
    const toDelete = snapshots.slice(keepCount)
    if (toDelete.length === 0) {
      return 0
    }

    const oldestToKeep = snapshots[keepCount - 1]!.ts
    return this.snapshotStorage.deleteSnapshotsBefore(target, oldestToKeep)
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Sort events by timestamp and ID
   */
  private sortEvents(events: Event[]): Event[] {
    return [...events].sort((a, b) => {
      const timeDiff = a.ts - b.ts
      if (timeDiff !== 0) return timeDiff
      return a.id.localeCompare(b.id)
    })
  }

  /**
   * Replay events forward to reconstruct state
   */
  private replayForward(events: Event[]): ReplayResult {
    let state: Variant | null = null
    let eventsReplayed = 0

    for (const event of events) {
      state = this.applyEvent(state, event)
      eventsReplayed++
    }

    return {
      state,
      eventsReplayed,
      existed: state !== null,
    }
  }

  /**
   * Replay events backward from a known state
   */
  private replayBackward(
    allEvents: Event[],
    currentState: Variant | null,
    currentTs: number,
    targetTs: number
  ): ReplayResult {
    // Get events between target and current, in reverse order
    const eventsToUndo = allEvents
      .filter(e => e.ts > targetTs && e.ts <= currentTs)
      .reverse()

    let state = currentState ? { ...currentState } : null
    let eventsReplayed = 0

    for (const event of eventsToUndo) {
      state = this.unapplyEvent(state, event)
      eventsReplayed++
    }

    return {
      state,
      eventsReplayed,
      existed: state !== null,
    }
  }

  /**
   * Apply an event to a state (forward)
   */
  private applyEvent(state: Variant | null, event: Event): Variant | null {
    switch (event.op) {
      case 'CREATE':
        return event.after ? { ...event.after } : null

      case 'UPDATE':
        if (!state || !event.after) return event.after ? { ...event.after } : null
        return { ...event.after }

      case 'DELETE':
        return null

      default:
        return state
    }
  }

  /**
   * Unapply an event to a state (backward)
   */
  private unapplyEvent(state: Variant | null, event: Event): Variant | null {
    switch (event.op) {
      case 'CREATE':
        // Undoing a CREATE means the entity didn't exist before
        return null

      case 'UPDATE':
        // Undoing an UPDATE restores the before state
        return event.before ? { ...event.before } : null

      case 'DELETE':
        // Undoing a DELETE restores the before state
        return event.before ? { ...event.before } : null

      default:
        return state
    }
  }
}

// =============================================================================
// In-Memory Event Source
// =============================================================================

/**
 * Simple in-memory event source for testing or small datasets
 */
export class InMemoryEventSource implements EventSource {
  private events: Event[] = []

  /**
   * Add events to the source
   */
  addEvents(events: Event[]): void {
    this.events.push(...events)
  }

  /**
   * Add a single event
   */
  addEvent(event: Event): void {
    this.events.push(event)
  }

  /**
   * Clear all events
   */
  clear(): void {
    this.events = []
  }

  /**
   * Get events for a specific target
   */
  async getEventsForTarget(
    target: string,
    minTs?: number,
    maxTs?: number
  ): Promise<Event[]> {
    return this.events.filter(e => {
      if (e.target !== target) return false
      if (minTs !== undefined && e.ts < minTs) return false
      if (maxTs !== undefined && e.ts > maxTs) return false
      return true
    })
  }

  /**
   * Get events in a time range
   */
  async getEventsInRange(minTs: number, maxTs: number): Promise<Event[]> {
    return this.events.filter(e => e.ts >= minTs && e.ts <= maxTs)
  }

  /**
   * Get all events
   */
  getAllEvents(): Event[] {
    return [...this.events]
  }
}

// =============================================================================
// Batch Event Source (for segment-based storage)
// =============================================================================

/**
 * Event source that reads from event batches (segments)
 */
export class BatchEventSource implements EventSource {
  private getBatches: (minTs?: number, maxTs?: number) => Promise<EventBatch[]>

  constructor(
    getBatches: (minTs?: number, maxTs?: number) => Promise<EventBatch[]>
  ) {
    this.getBatches = getBatches
  }

  async getEventsForTarget(
    target: string,
    minTs?: number,
    maxTs?: number
  ): Promise<Event[]> {
    const batches = await this.getBatches(minTs, maxTs)
    const events: Event[] = []

    for (const batch of batches) {
      for (const event of batch.events) {
        if (event.target !== target) continue
        if (minTs !== undefined && event.ts < minTs) continue
        if (maxTs !== undefined && event.ts > maxTs) continue
        events.push(event)
      }
    }

    return events
  }

  async getEventsInRange(minTs: number, maxTs: number): Promise<Event[]> {
    const batches = await this.getBatches(minTs, maxTs)
    const events: Event[] = []

    for (const batch of batches) {
      for (const event of batch.events) {
        if (event.ts >= minTs && event.ts <= maxTs) {
          events.push(event)
        }
      }
    }

    return events
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an EventReplayer instance
 */
export function createEventReplayer(eventSource: EventSource): EventReplayer {
  return new EventReplayer(eventSource)
}

/**
 * Create an InMemoryEventSource instance
 */
export function createInMemoryEventSource(): InMemoryEventSource {
  return new InMemoryEventSource()
}

/**
 * Create a BatchEventSource instance
 */
export function createBatchEventSource(
  getBatches: (minTs?: number, maxTs?: number) => Promise<EventBatch[]>
): BatchEventSource {
  return new BatchEventSource(getBatches)
}

// =============================================================================
// In-Memory Snapshot Storage
// =============================================================================

/**
 * Simple in-memory snapshot storage for testing or small datasets
 */
export class InMemorySnapshotStorage implements SnapshotStorage {
  private snapshots = new Map<string, Snapshot[]>()

  async getSnapshot(target: string, beforeTs?: number): Promise<Snapshot | null> {
    const targetSnapshots = this.snapshots.get(target)
    if (!targetSnapshots || targetSnapshots.length === 0) {
      return null
    }

    // Sort by timestamp descending
    const sorted = [...targetSnapshots].sort((a, b) => b.ts - a.ts)

    if (beforeTs === undefined) {
      return sorted[0] ?? null
    }

    // Find the latest snapshot before or at the timestamp
    for (const snapshot of sorted) {
      if (snapshot.ts <= beforeTs) {
        return snapshot
      }
    }

    return null
  }

  async saveSnapshot(snapshot: Snapshot): Promise<void> {
    const existing = this.snapshots.get(snapshot.target) ?? []
    existing.push(snapshot)
    this.snapshots.set(snapshot.target, existing)
  }

  async deleteSnapshotsBefore(target: string, beforeTs: number): Promise<number> {
    const existing = this.snapshots.get(target)
    if (!existing) {
      return 0
    }

    const toKeep = existing.filter(s => s.ts >= beforeTs)
    const deletedCount = existing.length - toKeep.length
    this.snapshots.set(target, toKeep)
    return deletedCount
  }

  async getSnapshots(target: string): Promise<Snapshot[]> {
    return [...(this.snapshots.get(target) ?? [])]
  }

  /**
   * Clear all snapshots
   */
  clear(): void {
    this.snapshots.clear()
  }
}

// =============================================================================
// Convenience Functions for Event Sourcing
// =============================================================================

/**
 * Replay events for an entity to reconstruct its current state.
 *
 * This is the core function for true event sourcing - it rebuilds
 * entity state purely from the event log.
 *
 * @example
 * ```typescript
 * // Reconstruct current state from events
 * const result = await replayEvents(eventSource, 'posts', 'post-123')
 *
 * if (result.existed) {
 *   console.log('Current state:', result.state)
 * }
 *
 * // Reconstruct state at a specific point in time
 * const historicResult = await replayEvents(
 *   eventSource,
 *   'posts',
 *   'post-123',
 *   { at: Date.now() - 3600000 } // 1 hour ago
 * )
 * ```
 */
export async function replayEvents(
  eventSource: EventSource,
  namespace: string,
  entityId: string,
  options?: ReplayEventsOptions
): Promise<ReplayResult> {
  const target = entityTarget(namespace, entityId)
  const at = options?.at ?? Date.now()

  const replayer = new EventReplayer(eventSource)
  return replayer.replayEntity(target, { at })
}

/**
 * Replay events with snapshot support for better performance.
 *
 * Use this when you have many events per entity and need
 * fast state reconstruction.
 *
 * @example
 * ```typescript
 * const result = await replayEventsWithSnapshots(
 *   eventSource,
 *   snapshotStorage,
 *   'posts',
 *   'post-123'
 * )
 *
 * console.log(`Replayed ${result.eventsReplayed} events`)
 * console.log(`Used snapshot: ${result.usedSnapshot}`)
 * ```
 */
export async function replayEventsWithSnapshots(
  eventSource: EventSource,
  snapshotStorage: SnapshotStorage,
  namespace: string,
  entityId: string,
  options?: ReplayEventsOptions & { createSnapshot?: boolean | undefined }
): Promise<ExtendedReplayResult> {
  const target = entityTarget(namespace, entityId)
  const at = options?.at ?? Date.now()

  const replayer = new EventReplayer(eventSource, { snapshotStorage })
  return replayer.replayWithSnapshot(target, {
    at,
    createSnapshot: options?.createSnapshot ?? true,
  })
}

/**
 * Batch replay events for multiple entities.
 *
 * More efficient than calling replayEvents multiple times
 * when you need to reconstruct many entities.
 */
export async function replayEventsBatch(
  eventSource: EventSource,
  targets: Array<{ namespace: string; entityId: string }>,
  options?: ReplayEventsOptions
): Promise<Map<string, ReplayResult>> {
  const at = options?.at ?? Date.now()
  const replayer = new EventReplayer(eventSource)

  const targetStrings = targets.map(t => entityTarget(t.namespace, t.entityId))
  return replayer.replayEntities(targetStrings, { at })
}

/**
 * Get the full state history of an entity.
 *
 * Returns the state after each event, useful for debugging
 * and audit trails.
 */
export async function getEntityHistory(
  eventSource: EventSource,
  namespace: string,
  entityId: string,
  options?: { minTs?: number | undefined; maxTs?: number | undefined }
): Promise<Array<{ ts: number; state: Variant | null; op: string }>> {
  const target = entityTarget(namespace, entityId)
  const replayer = new EventReplayer(eventSource)
  return replayer.getStateHistory(target, options)
}

/**
 * Create an InMemorySnapshotStorage instance
 */
export function createInMemorySnapshotStorage(): InMemorySnapshotStorage {
  return new InMemorySnapshotStorage()
}

// =============================================================================
// Event Versioning Helpers
// =============================================================================

/**
 * Create a versioned event with schema version metadata
 */
export function createVersionedEvent(
  event: Event,
  schemaVersion: number
): Event {
  return {
    ...event,
    metadata: {
      ...(event.metadata ?? {}),
      schemaVersion,
    },
  }
}

/**
 * Get the schema version of an event
 */
export function getEventVersion(event: Event): number | undefined {
  return (event.metadata as Record<string, unknown>)?.schemaVersion as number | undefined
}

/**
 * Create a simple field-rename upgrader
 */
export function createFieldRenameUpgrader(
  renames: Record<string, string>
): EventUpgrader {
  return (event: Event, fromVersion: number, toVersion: number): Event => {
    const upgraded = { ...event }

    if (upgraded.before) {
      upgraded.before = renameFields(upgraded.before, renames)
    }
    if (upgraded.after) {
      upgraded.after = renameFields(upgraded.after, renames)
    }

    upgraded.metadata = {
      ...(upgraded.metadata ?? {}),
      schemaVersion: toVersion,
      upgradedFrom: fromVersion,
    }

    return upgraded
  }
}

/**
 * Helper to rename fields in a Variant
 */
function renameFields(variant: Variant, renames: Record<string, string>): Variant {
  const result: Variant = {}

  for (const [key, value] of Object.entries(variant)) {
    const newKey = renames[key] ?? key
    result[newKey] = value
  }

  return result
}

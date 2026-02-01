/**
 * Event Replay for Time-Travel Queries
 *
 * Reconstructs entity state at any point in time by:
 * 1. Starting from current state (or a snapshot)
 * 2. Reading events from the event log
 * 3. Replaying events backwards/forwards to the target timestamp
 *
 * Supports the { at: timestamp } query option.
 */

import type { Event, Variant } from '../types/entity'
import { parseEntityTarget, isRelationshipTarget } from '../types/entity'
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
  currentState?: Variant | null
  /** Starting timestamp (when currentState was captured) */
  currentTs?: number
}

/**
 * Options for batch replay
 */
export interface BatchReplayOptions {
  /** Target timestamp to replay to */
  at: number
  /** Map of target -> current state */
  currentStates?: Map<string, Variant | null>
  /** Timestamp when current states were captured */
  currentTs?: number
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
 * ```
 */
export class EventReplayer {
  private eventSource: EventSource

  constructor(eventSource: EventSource) {
    this.eventSource = eventSource
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

    // If we have a current state and it's after the target, replay backwards
    if (currentState !== undefined && currentTs !== undefined && currentTs > at) {
      return this.replayBackward(sortedEvents, currentState, currentTs, at)
    }

    // Otherwise, replay forward from the beginning
    return this.replayForward(relevantEvents)
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
    options?: { minTs?: number; maxTs?: number }
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

/**
 * Event Sync Engine — pure conflict resolution logic for bidirectional sync.
 *
 * This module provides the sync algorithm as standalone, I/O-free functions.
 * The caller handles all database reads/writes, but delegates conflict
 * detection and resolution to these pure functions.
 *
 * Conflict resolution strategies:
 *   local-wins  — client events always win on conflict
 *   remote-wins — server events always win on conflict
 *   newest      — the event with the later timestamp wins
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Sync event (CDC) — the unit of change exchanged during bidirectional sync.
 */
export interface SyncEvent {
  id: string
  timestamp: string
  operation: 'create' | 'update' | 'delete' | 'soft-delete'
  type: string
  entityId: string
  data?: Record<string, unknown>
  checksum?: string
}

export type EventSyncConflictStrategy = 'local-wins' | 'remote-wins' | 'newest'

export interface EventSyncConflictInfo {
  entityId: string
  type: string
  localVersion: string
  remoteVersion: string
  resolution: 'local' | 'remote'
}

export interface EventSyncResult {
  applied: number
  conflicts: EventSyncConflictInfo[]
  newEvents: SyncEvent[]
  cursor: string
}

export interface ResolveEventSyncParams {
  clientEvents: SyncEvent[]
  serverEvents: SyncEvent[]
  strategy: EventSyncConflictStrategy
  since: string | null
}

export interface ResolveEventSyncResult {
  eventsToApply: SyncEvent[]
  eventsForClient: SyncEvent[]
  conflicts: EventSyncConflictInfo[]
}

// ---------------------------------------------------------------------------
// Pure Functions
// ---------------------------------------------------------------------------

/**
 * Build a deduplication map that keeps only the latest event per entity key.
 * Key format: `${type}:${entityId}`
 */
function buildLatestByKey(events: SyncEvent[]): Map<string, SyncEvent> {
  const map = new Map<string, SyncEvent>()
  for (const evt of events) {
    const key = `${evt.type}:${evt.entityId}`
    const existing = map.get(key)
    if (!existing || evt.timestamp > existing.timestamp) {
      map.set(key, evt)
    }
  }
  return map
}

/**
 * Resolve a bidirectional sync between client and server event sets.
 *
 * Returns:
 *   - eventsToApply:  client events that should be written to the server
 *   - eventsForClient: server events that should be sent back to the client
 *   - conflicts:       details of every conflict and how it was resolved
 *
 * This is a pure function — no I/O, no side effects.
 */
export function resolveEventSync(params: ResolveEventSyncParams): ResolveEventSyncResult {
  const { clientEvents, serverEvents, strategy } = params

  const serverEventsByKey = buildLatestByKey(serverEvents)
  const clientEventsByKey = buildLatestByKey(clientEvents)

  // Detect conflicting keys (same entity touched on both sides)
  const conflictKeys = new Set<string>()
  for (const key of clientEventsByKey.keys()) {
    if (serverEventsByKey.has(key)) {
      conflictKeys.add(key)
    }
  }

  const conflicts: EventSyncConflictInfo[] = []
  const eventsToApply: SyncEvent[] = []
  const eventsForClient: SyncEvent[] = []

  // Resolve each conflict
  for (const key of conflictKeys) {
    const clientEvt = clientEventsByKey.get(key)!
    const serverEvt = serverEventsByKey.get(key)!

    let resolution: 'local' | 'remote'

    if (strategy === 'local-wins') {
      resolution = 'local'
    } else if (strategy === 'remote-wins') {
      resolution = 'remote'
    } else {
      // newest — compare timestamps
      resolution = clientEvt.timestamp > serverEvt.timestamp ? 'local' : 'remote'
    }

    conflicts.push({
      entityId: clientEvt.entityId,
      type: clientEvt.type,
      localVersion: clientEvt.id,
      remoteVersion: serverEvt.id,
      resolution,
    })

    if (resolution === 'local') {
      eventsToApply.push(clientEvt)
    } else {
      eventsForClient.push(serverEvt)
    }
  }

  // Non-conflicting client events -> apply to server
  for (const [key, evt] of clientEventsByKey) {
    if (!conflictKeys.has(key)) {
      eventsToApply.push(evt)
    }
  }

  // Non-conflicting server events -> send to client
  for (const [key, evt] of serverEventsByKey) {
    if (!conflictKeys.has(key)) {
      eventsForClient.push(evt)
    }
  }

  return { eventsToApply, eventsForClient, conflicts }
}

/**
 * Compute the sync cursor for the next sync request.
 *
 * The cursor is the latest timestamp among all server events and any newly
 * applied events (represented by `appliedCount > 0`). Falls back to `since`
 * or the current time if there are no events.
 */
export function computeEventSyncCursor(serverEvents: SyncEvent[], appliedCount: number, since: string | null): string {
  const timestamps: string[] = []

  for (const evt of serverEvents) {
    timestamps.push(evt.timestamp)
  }

  // If we applied client events, they are now on the server as of "now"
  const now = new Date().toISOString()
  if (appliedCount > 0) {
    timestamps.push(now)
  }

  if (timestamps.length > 0) {
    timestamps.sort()
    return timestamps[timestamps.length - 1]!
  }

  return since ?? now
}

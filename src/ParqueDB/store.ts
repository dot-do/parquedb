/**
 * ParqueDB Store Module
 *
 * Global shared state management for ParqueDB instances.
 * Uses WeakMap with storage backend reference as key for shared state.
 */

import type { Entity, StorageBackend, Event } from '../types'
import type { Snapshot, SnapshotQueryStats } from './types'

// =============================================================================
// Global Shared State
// =============================================================================

/**
 * Global storage for shared entity state across ParqueDB instances.
 * Uses WeakMap with storage backend reference as key for shared state, allowing
 * multiple ParqueDB instances with the same storage to share entities.
 *
 * WeakMap allows automatic garbage collection when StorageBackend objects
 * are no longer referenced, preventing memory leaks from orphaned state.
 * Call dispose() for explicit cleanup when a ParqueDB instance is no longer needed.
 *
 * @deprecated This in-memory store is intended for Node.js/testing use only.
 * For Cloudflare Workers, use ParqueDBDO (SQLite) as the source of truth for writes
 * and R2 (via QueryExecutor/ReadPath) for reads. See docs/architecture/ENTITY_STORAGE.md
 * for the full architecture documentation.
 *
 * Architecture summary:
 * - Node.js/Testing: ParqueDB.ts uses globalEntityStore (in-memory) + storage backend for persistence
 * - Workers (writes): ParqueDBDO uses SQLite as source of truth, flushes to Parquet/R2
 * - Workers (reads): QueryExecutor reads directly from R2 Parquet files with caching
 *
 * Future plans: Consolidate to always read/write through storage backend abstractions.
 */
const globalEntityStore = new WeakMap<StorageBackend, Map<string, Entity>>()
const globalEventStore = new WeakMap<StorageBackend, Event[]>()
const globalArchivedEventStore = new WeakMap<StorageBackend, Event[]>()
const globalSnapshotStore = new WeakMap<StorageBackend, Snapshot[]>()
const globalQueryStats = new WeakMap<StorageBackend, Map<string, SnapshotQueryStats>>()

// =============================================================================
// Store Accessor Functions
// =============================================================================

/**
 * Get or create the entity store for a storage backend
 */
export function getEntityStore(storage: StorageBackend): Map<string, Entity> {
  if (!globalEntityStore.has(storage)) {
    globalEntityStore.set(storage, new Map())
  }
  return globalEntityStore.get(storage)!
}

/**
 * Get or create the event store for a storage backend
 */
export function getEventStore(storage: StorageBackend): Event[] {
  if (!globalEventStore.has(storage)) {
    globalEventStore.set(storage, [])
  }
  return globalEventStore.get(storage)!
}

/**
 * Get or create the archived event store for a storage backend
 */
export function getArchivedEventStore(storage: StorageBackend): Event[] {
  if (!globalArchivedEventStore.has(storage)) {
    globalArchivedEventStore.set(storage, [])
  }
  return globalArchivedEventStore.get(storage)!
}

/**
 * Get or create the snapshot store for a storage backend
 */
export function getSnapshotStore(storage: StorageBackend): Snapshot[] {
  if (!globalSnapshotStore.has(storage)) {
    globalSnapshotStore.set(storage, [])
  }
  return globalSnapshotStore.get(storage)!
}

/**
 * Get or create the query stats store for a storage backend
 */
export function getQueryStatsStore(storage: StorageBackend): Map<string, SnapshotQueryStats> {
  if (!globalQueryStats.has(storage)) {
    globalQueryStats.set(storage, new Map())
  }
  return globalQueryStats.get(storage)!
}

/**
 * Clear global state for a specific storage backend.
 * This is called by dispose() for explicit cleanup.
 */
export function clearGlobalState(storage: StorageBackend): void {
  globalEntityStore.delete(storage)
  globalEventStore.delete(storage)
  globalArchivedEventStore.delete(storage)
  globalSnapshotStore.delete(storage)
  globalQueryStats.delete(storage)
}

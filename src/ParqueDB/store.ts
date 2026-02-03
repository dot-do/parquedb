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

/**
 * Reverse relationship index for O(1) lookups of inbound references.
 * Maps: targetEntityId -> Map<(sourceNs + "." + sourceField) -> Set<sourceEntityId>>
 *
 * This index eliminates the N+1 query pattern when traversing reverse relationships.
 * Instead of scanning all entities to find those that reference a target,
 * we can directly look up the source entities in this index.
 *
 * Example: When looking up all Posts that reference a User via the "author" field:
 * - Without index: O(n) scan of all posts
 * - With index: O(1) lookup in reverseRelIndex["users/123"]["posts.author"]
 */
const globalReverseRelIndex = new WeakMap<StorageBackend, Map<string, Map<string, Set<string>>>>()

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
 * Get or create the reverse relationship index for a storage backend
 *
 * The index structure is:
 * Map<targetEntityId, Map<sourceKey, Set<sourceEntityId>>>
 *
 * Where sourceKey = `${sourceNamespace}.${sourceFieldName}`
 *
 * This allows efficient lookups like:
 * "Find all posts that reference users/123 via the author field"
 * -> reverseRelIndex.get("users/123")?.get("posts.author")
 */
export function getReverseRelIndex(storage: StorageBackend): Map<string, Map<string, Set<string>>> {
  if (!globalReverseRelIndex.has(storage)) {
    globalReverseRelIndex.set(storage, new Map())
  }
  return globalReverseRelIndex.get(storage)!
}

/**
 * Add a relationship to the reverse index.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID that has the relationship (e.g., "posts/abc")
 * @param sourceField - The field name on the source (e.g., "author")
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 */
export function addToReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  sourceField: string,
  targetId: string
): void {
  const sourceNs = sourceId.split('/')[0]
  const sourceKey = `${sourceNs}.${sourceField}`

  let targetMap = index.get(targetId)
  if (!targetMap) {
    targetMap = new Map()
    index.set(targetId, targetMap)
  }

  let sourceSet = targetMap.get(sourceKey)
  if (!sourceSet) {
    sourceSet = new Set()
    targetMap.set(sourceKey, sourceSet)
  }

  sourceSet.add(sourceId)
}

/**
 * Remove a relationship from the reverse index.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID that had the relationship (e.g., "posts/abc")
 * @param sourceField - The field name on the source (e.g., "author")
 * @param targetId - The entity ID that was referenced (e.g., "users/123")
 */
export function removeFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  sourceField: string,
  targetId: string
): void {
  const sourceNs = sourceId.split('/')[0]
  const sourceKey = `${sourceNs}.${sourceField}`

  const targetMap = index.get(targetId)
  if (!targetMap) return

  const sourceSet = targetMap.get(sourceKey)
  if (!sourceSet) return

  sourceSet.delete(sourceId)

  // Clean up empty sets and maps
  if (sourceSet.size === 0) {
    targetMap.delete(sourceKey)
  }
  if (targetMap.size === 0) {
    index.delete(targetId)
  }
}

/**
 * Get all entities that reference a target via a specific field.
 *
 * @param index - The reverse relationship index
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 * @param sourceNs - The namespace of source entities (e.g., "posts")
 * @param sourceField - The field name on the source (e.g., "author")
 * @returns Set of source entity IDs, or empty set if none
 */
export function getFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  targetId: string,
  sourceNs: string,
  sourceField: string
): Set<string> {
  const sourceKey = `${sourceNs}.${sourceField}`
  const targetMap = index.get(targetId)
  if (!targetMap) return new Set()
  return targetMap.get(sourceKey) || new Set()
}

/**
 * Get all entities that reference a target from a specific namespace (any field).
 *
 * @param index - The reverse relationship index
 * @param targetId - The entity ID being referenced (e.g., "users/123")
 * @param sourceNs - The namespace of source entities (e.g., "posts")
 * @returns Map of field names to source entity IDs
 */
export function getAllFromReverseRelIndexByNs(
  index: Map<string, Map<string, Set<string>>>,
  targetId: string,
  sourceNs: string
): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>()
  const targetMap = index.get(targetId)
  if (!targetMap) return result

  const prefix = `${sourceNs}.`
  for (const [sourceKey, sourceSet] of targetMap) {
    if (sourceKey.startsWith(prefix)) {
      const fieldName = sourceKey.slice(prefix.length)
      result.set(fieldName, sourceSet)
    }
  }
  return result
}

/**
 * Remove all reverse index entries for a source entity.
 * Call this when an entity is deleted.
 *
 * @param index - The reverse relationship index
 * @param sourceId - The entity ID being removed
 * @param entities - The entity store, to find all references from this entity
 */
export function removeAllFromReverseRelIndex(
  index: Map<string, Map<string, Set<string>>>,
  sourceId: string,
  entity: Entity | undefined
): void {
  if (!entity) return

  // Iterate over all fields that could be relationships
  for (const [fieldName, fieldValue] of Object.entries(entity)) {
    if (fieldName.startsWith('$')) continue // Skip meta fields
    if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
      // This could be a relationship field: { displayName: targetId }
      for (const targetId of Object.values(fieldValue as Record<string, unknown>)) {
        if (typeof targetId === 'string' && targetId.includes('/')) {
          removeFromReverseRelIndex(index, sourceId, fieldName, targetId)
        }
      }
    }
  }
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
  globalReverseRelIndex.delete(storage)
}

/**
 * ParqueDB Snapshots Module
 *
 * Contains SnapshotManager class for managing entity snapshots.
 */

import type { Entity, EntityId, StorageBackend, Event } from '../types'
import { parseEntityTarget, isRelationshipTarget } from '../types'
import { generateId } from '../utils'

import type {
  Snapshot,
  SnapshotManager,
  SnapshotQueryStats,
  SnapshotStorageStats,
  PruneSnapshotsOptions,
  RawSnapshot,
} from './types'

import { EntityNotFoundError, EventError } from './types'
import { ParqueDBError, ErrorCode, SnapshotNotFoundError } from '../errors'

export interface SnapshotContext {
  storage: StorageBackend
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
}

/**
 * SnapshotManager implementation class
 *
 * Manages entity snapshots for efficient time-travel queries and state reconstruction.
 * Snapshots are point-in-time captures of entity state that can be used to avoid
 * replaying all events from the beginning.
 */
export class SnapshotManagerImpl implements SnapshotManager {
  private readonly storage: StorageBackend
  private readonly entities: Map<string, Entity>
  private readonly events: Event[]
  private readonly snapshots: Snapshot[]
  private readonly queryStats: Map<string, SnapshotQueryStats>

  constructor(ctx: SnapshotContext) {
    this.storage = ctx.storage
    this.entities = ctx.entities
    this.events = ctx.events
    this.snapshots = ctx.snapshots
    this.queryStats = ctx.queryStats
  }

  /**
   * Create a snapshot of the current state of an entity
   */
  async createSnapshot(entityId: EntityId): Promise<Snapshot> {
    const fullId = entityId as string
    const entity = this.entities.get(fullId)
    if (!entity) {
      const [ns, ...idParts] = fullId.split('/')
      throw new EntityNotFoundError(ns ?? '', idParts.join('/'))
    }
    if (entity.deletedAt) {
      throw new EventError('Create snapshot', 'Cannot create snapshot of deleted entity', {
        entityId: fullId,
      })
    }

    const [ns, ...idParts] = fullId.split('/')
    const entityEvents = this.events.filter((e) => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === idParts.join('/')
    })

    const sequenceNumber = entityEvents.length
    const stateJson = JSON.stringify(entity)
    const stateSize = stateJson.length
    const compressed = stateSize > 1000

    const snapshot: Snapshot = {
      id: generateId(),
      entityId,
      ns: ns ?? '',
      sequenceNumber,
      createdAt: new Date(),
      state: { ...entity } as Record<string, unknown>,
      compressed,
      size: compressed ? Math.floor(stateSize * 0.3) : stateSize
    }

    this.snapshots.push(snapshot)
    const snapshotPath = `data/${ns}/snapshots/${snapshot.id}.parquet`
    await this.storage.write(snapshotPath, new TextEncoder().encode(stateJson))

    return snapshot
  }

  /**
   * Create a snapshot at a specific event in history
   */
  async createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const entityIdPart = idParts.join('/')

    const event = this.events.find((e) => e.id === eventId)
    if (!event) {
      throw new EventError('Create snapshot at event', 'Event not found', { eventId })
    }

    const state = event.after ? { ...event.after } : null
    if (!state) {
      throw new EventError('Create snapshot at event', 'Event has no after state', { eventId })
    }

    const entityEvents = this.events.filter((e) => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === entityIdPart
    }).sort((a, b) => a.ts - b.ts)

    const eventIndex = entityEvents.findIndex((e) => e.id === eventId)
    const sequenceNumber = eventIndex + 1
    const stateJson = JSON.stringify(state)
    const stateSize = stateJson.length
    const compressed = stateSize > 1000

    const snapshot: Snapshot = {
      id: generateId(),
      entityId,
      ns: ns ?? '',
      sequenceNumber,
      eventId,
      createdAt: new Date(),
      state: state as Record<string, unknown>,
      compressed,
      size: compressed ? Math.floor(stateSize * 0.3) : stateSize
    }

    this.snapshots.push(snapshot)
    await this.storage.write(`data/${ns}/snapshots/${snapshot.id}.parquet`, new TextEncoder().encode(stateJson))

    return snapshot
  }

  /**
   * List all snapshots for an entity
   */
  async listSnapshots(entityId: EntityId): Promise<Snapshot[]> {
    return this.snapshots
      .filter((s) => s.entityId === (entityId as string))
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
  }

  /**
   * Delete a specific snapshot
   *
   * @throws {SnapshotNotFoundError} If the snapshot does not exist
   * @throws {ParqueDBError} If the storage operation fails (with code STORAGE_ERROR)
   */
  async deleteSnapshot(snapshotId: string): Promise<void> {
    const index = this.snapshots.findIndex((s) => s.id === snapshotId)
    if (index === -1) {
      throw new SnapshotNotFoundError(snapshotId)
    }

    const snapshot = this.snapshots[index]!
    const snapshotPath = `data/${snapshot.ns}/snapshots/${snapshotId}.parquet`

    // Delete from storage first, then remove from in-memory array
    // This ensures consistency: if storage fails, we don't have orphaned in-memory state
    try {
      await this.storage.delete(snapshotPath)
    } catch (error) {
      throw new ParqueDBError(
        `Failed to delete snapshot ${snapshotId} from storage`,
        ErrorCode.STORAGE_ERROR,
        { path: snapshotPath, operation: 'delete', snapshotId },
        error instanceof Error ? error : undefined
      )
    }

    // Only remove from in-memory array after successful storage deletion
    this.snapshots.splice(index, 1)
  }

  /**
   * Prune old snapshots based on age and/or keeping a minimum count
   *
   * @throws {ParqueDBError} If any storage delete operation fails (partial pruning may have occurred, with code STORAGE_ERROR)
   */
  async pruneSnapshots(options: PruneSnapshotsOptions): Promise<void> {
    const { olderThan, keepMinimum = 0 } = options
    const snapshotsByEntity = new Map<string, Snapshot[]>()

    for (const snapshot of this.snapshots) {
      const eid = snapshot.entityId as string
      let entitySnapshots = snapshotsByEntity.get(eid)
      if (!entitySnapshots) {
        entitySnapshots = []
        snapshotsByEntity.set(eid, entitySnapshots)
      }
      entitySnapshots.push(snapshot)
    }

    // Collect snapshots to prune first, then delete them
    const snapshotsToPrune: Snapshot[] = []

    for (const [, entitySnapshots] of snapshotsByEntity) {
      // Sort newest first by sequence number (most accurate measure of "age")
      entitySnapshots.sort((a, b) => b.sequenceNumber - a.sequenceNumber)
      // Keep at least keepMinimum snapshots (the newest ones)
      // When pruning by age (olderThan is set), the sort order determines age
      const candidates = entitySnapshots.slice(keepMinimum)

      for (const snapshot of candidates) {
        // Prune if:
        // 1. No olderThan specified (prune all candidates), or
        // 2. Timestamp <= olderThan, or
        // 3. Timestamp equals the newest snapshot's but this is not the newest
        //    (handles same-millisecond snapshots by sequence number ordering)
        const shouldPrune = !olderThan ||
          snapshot.createdAt.getTime() <= olderThan.getTime() ||
          (entitySnapshots.length > 1 && entitySnapshots[0] && snapshot.sequenceNumber < entitySnapshots[0].sequenceNumber)

        if (shouldPrune) {
          snapshotsToPrune.push(snapshot)
        }
      }
    }

    // Delete snapshots one by one, removing from in-memory array only after successful storage delete
    const errors: Array<{ snapshotId: string; error: Error }> = []

    for (const snapshot of snapshotsToPrune) {
      const snapshotPath = `data/${snapshot.ns}/snapshots/${snapshot.id}.parquet`
      try {
        await this.storage.delete(snapshotPath)
        // Only remove from in-memory array after successful storage deletion
        const idx = this.snapshots.findIndex((s) => s.id === snapshot.id)
        if (idx !== -1) {
          this.snapshots.splice(idx, 1)
        }
      } catch (error) {
        errors.push({
          snapshotId: snapshot.id,
          error: error instanceof Error ? error : new Error(String(error)),
        })
      }
    }

    // If any errors occurred, throw an aggregate error with details
    if (errors.length > 0) {
      const failedIds = errors.map((e) => e.snapshotId).join(', ')
      throw new ParqueDBError(
        `Failed to delete ${errors.length} snapshot(s) during pruning: ${failedIds}`,
        ErrorCode.STORAGE_ERROR,
        {
          operation: 'pruneSnapshots',
          failedCount: errors.length,
          totalCount: snapshotsToPrune.length,
          failedSnapshotIds: errors.map((e) => e.snapshotId),
        },
        errors[0]?.error
      )
    }
  }

  /**
   * Get raw snapshot data by ID
   */
  async getRawSnapshot(snapshotId: string): Promise<RawSnapshot> {
    const snapshot = this.snapshots.find((s) => s.id === snapshotId)
    if (!snapshot) {
      throw new EventError('Get snapshot', 'Snapshot not found', { snapshotId })
    }
    const data = new TextEncoder().encode(JSON.stringify(snapshot.state))
    return { id: snapshotId, size: snapshot.size || data.length, data }
  }

  /**
   * Get query stats for an entity (how snapshots were used in time-travel queries)
   */
  async getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats> {
    return this.queryStats.get(entityId as string) || { snapshotsUsed: 0, eventsReplayed: 0 }
  }

  /**
   * Get overall storage statistics for snapshots
   */
  async getStorageStats(): Promise<SnapshotStorageStats> {
    const totalSize = this.snapshots.reduce((sum, s) => sum + (s.size || 0), 0)
    const snapshotCount = this.snapshots.length
    return {
      totalSize,
      snapshotCount,
      avgSnapshotSize: snapshotCount > 0 ? totalSize / snapshotCount : 0
    }
  }
}

/**
 * Create a snapshot manager instance
 *
 * @deprecated Use SnapshotManagerImpl class directly instead
 */
export function createSnapshotManager(ctx: SnapshotContext): SnapshotManager {
  return new SnapshotManagerImpl(ctx)
}

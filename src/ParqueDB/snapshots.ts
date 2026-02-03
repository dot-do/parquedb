/**
 * ParqueDB Snapshots Module
 *
 * Contains snapshot management functionality extracted from core.ts.
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

export interface SnapshotContext {
  storage: StorageBackend
  entities: Map<string, Entity>
  events: Event[]
  snapshots: Snapshot[]
  queryStats: Map<string, SnapshotQueryStats>
}

/**
 * Create a snapshot manager instance
 */
export function createSnapshotManager(ctx: SnapshotContext): SnapshotManager {
  return {
    async createSnapshot(entityId: EntityId): Promise<Snapshot> {
      const fullId = entityId as string
      const entity = ctx.entities.get(fullId)
      if (!entity) {
        const [ns, ...idParts] = fullId.split('/')
        throw new EntityNotFoundError(ns, idParts.join('/'))
      }
      if (entity.deletedAt) {
        throw new EventError('Create snapshot', 'Cannot create snapshot of deleted entity', {
          entityId: fullId,
        })
      }
      const [ns, ...idParts] = fullId.split('/')
      const entityEvents = ctx.events.filter((e) => {
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
      ctx.snapshots.push(snapshot)
      const snapshotPath = `data/${ns}/snapshots/${snapshot.id}.parquet`
      await ctx.storage.write(snapshotPath, new TextEncoder().encode(stateJson))
      return snapshot
    },

    async createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot> {
      const fullId = entityId as string
      const [ns, ...idParts] = fullId.split('/')
      const entityIdPart = idParts.join('/')
      const event = ctx.events.find((e) => e.id === eventId)
      if (!event) {
        throw new EventError('Create snapshot at event', 'Event not found', { eventId })
      }
      const state = event.after ? { ...event.after } : null
      if (!state) {
        throw new EventError('Create snapshot at event', 'Event has no after state', { eventId })
      }
      const entityEvents = ctx.events.filter((e) => {
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
      ctx.snapshots.push(snapshot)
      await ctx.storage.write(`data/${ns}/snapshots/${snapshot.id}.parquet`, new TextEncoder().encode(stateJson))
      return snapshot
    },

    async listSnapshots(entityId: EntityId): Promise<Snapshot[]> {
      return ctx.snapshots
        .filter((s) => s.entityId === (entityId as string))
        .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
    },

    async deleteSnapshot(snapshotId: string): Promise<void> {
      const index = ctx.snapshots.findIndex((s) => s.id === snapshotId)
      if (index !== -1) {
        const snapshot = ctx.snapshots[index]!
        ctx.snapshots.splice(index, 1)
        await ctx.storage.delete(`data/${snapshot.ns}/snapshots/${snapshotId}.parquet`)
      }
    },

    async pruneSnapshots(options: PruneSnapshotsOptions): Promise<void> {
      const { olderThan, keepMinimum = 0 } = options
      const snapshotsByEntity = new Map<string, Snapshot[]>()
      for (const snapshot of ctx.snapshots) {
        const eid = snapshot.entityId as string
        if (!snapshotsByEntity.has(eid)) snapshotsByEntity.set(eid, [])
        snapshotsByEntity.get(eid)!.push(snapshot)
      }
      for (const [, entitySnapshots] of snapshotsByEntity) {
        // Sort newest first by sequence number
        entitySnapshots.sort((a, b) => b.sequenceNumber - a.sequenceNumber)
        const candidates = entitySnapshots.slice(keepMinimum)
        for (const snapshot of candidates) {
          const shouldPrune = !olderThan ||
            snapshot.createdAt.getTime() <= olderThan.getTime() ||
            (entitySnapshots.length > 1 && entitySnapshots[0] && snapshot.sequenceNumber < entitySnapshots[0].sequenceNumber)
          if (shouldPrune) {
            const idx = ctx.snapshots.findIndex((s) => s.id === snapshot.id)
            if (idx !== -1) {
              ctx.snapshots.splice(idx, 1)
              await ctx.storage.delete(`data/${snapshot.ns}/snapshots/${snapshot.id}.parquet`)
            }
          }
        }
      }
    },

    async getRawSnapshot(snapshotId: string): Promise<RawSnapshot> {
      const snapshot = ctx.snapshots.find((s) => s.id === snapshotId)
      if (!snapshot) {
        throw new EventError('Get snapshot', 'Snapshot not found', { snapshotId })
      }
      const data = new TextEncoder().encode(JSON.stringify(snapshot.state))
      return { id: snapshotId, size: snapshot.size || data.length, data }
    },

    async getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats> {
      return ctx.queryStats.get(entityId as string) || { snapshotsUsed: 0, eventsReplayed: 0 }
    },

    async getStorageStats(): Promise<SnapshotStorageStats> {
      const totalSize = ctx.snapshots.reduce((sum, s) => sum + (s.size || 0), 0)
      const snapshotCount = ctx.snapshots.length
      return {
        totalSize,
        snapshotCount,
        avgSnapshotSize: snapshotCount > 0 ? totalSize / snapshotCount : 0
      }
    },
  }
}

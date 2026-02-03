/**
 * Transaction Manager
 *
 * Handles transaction state and rollback for ParqueDB Durable Object operations.
 */

import type { Entity } from '../../types'
import type { TransactionSnapshot, StoredEntity, StoredRelationship, EventBuffer } from './types'
import type { EntityCacheManager } from './cache-manager'
import type { EventWalManager } from './event-wal'
import type { RelationshipWalManager } from './relationship-wal'

/**
 * Transaction Manager
 *
 * Provides transaction support with snapshot-based rollback.
 */
export class TransactionManager {
  /** Transaction snapshot for rollback */
  private transactionSnapshot: TransactionSnapshot | null = null

  /** Whether currently in a transaction */
  private inTransaction = false

  constructor(
    private sql: SqlStorage,
    private bucket: R2Bucket,
    private counters: Map<string, number>,
    private cacheManager: EntityCacheManager,
    private eventWal: EventWalManager,
    private relWal: RelationshipWalManager
  ) {}

  /**
   * Begin a transaction
   *
   * Captures a snapshot of all mutable state that can be restored on rollback.
   *
   * @returns Transaction ID (for tracking purposes)
   */
  beginTransaction(): string {
    if (this.inTransaction) {
      throw new Error('Transaction already in progress')
    }

    this.inTransaction = true

    // Deep copy all mutable in-memory state
    this.transactionSnapshot = {
      counters: new Map(this.counters),
      entityCache: new Map(),
      eventBuffer: [...this.eventWal.getEventBuffer()],
      eventBufferSize: this.eventWal.getEventBufferSize(),
      nsEventBuffers: new Map(),
      relEventBuffers: new Map(),
      sqlRollbackOps: [],
      pendingR2Paths: [],
    }

    // Deep copy entity cache entries
    const fullCache = this.cacheManager.getFullCache()
    for (const [key, value] of fullCache) {
      this.transactionSnapshot.entityCache.set(key, {
        entity: JSON.parse(JSON.stringify(value.entity)),
        version: value.version,
      })
    }

    // Deep copy namespace event buffers
    const nsBuffers = this.eventWal.getNsEventBuffers()
    for (const [ns, buffer] of nsBuffers) {
      this.transactionSnapshot.nsEventBuffers.set(ns, {
        events: [...buffer.events],
        firstSeq: buffer.firstSeq,
        lastSeq: buffer.lastSeq,
        sizeBytes: buffer.sizeBytes,
      })
    }

    // Deep copy relationship event buffers
    const relBuffers = this.relWal.getRelEventBuffers()
    for (const [ns, buffer] of relBuffers) {
      this.transactionSnapshot.relEventBuffers.set(ns, {
        events: [...buffer.events],
        firstSeq: buffer.firstSeq,
        lastSeq: buffer.lastSeq,
        sizeBytes: buffer.sizeBytes,
      })
    }

    return `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  }

  /**
   * Record a SQL operation for potential rollback
   */
  recordSqlOperation(
    type:
      | 'entity_insert'
      | 'entity_update'
      | 'entity_delete'
      | 'rel_insert'
      | 'rel_update'
      | 'rel_delete'
      | 'pending_row_group',
    ns: string,
    id: string,
    beforeState?: StoredEntity | StoredRelationship | null,
    extra?: { predicate?: string | undefined; toNs?: string | undefined; toId?: string | undefined }
  ): void {
    if (!this.inTransaction || !this.transactionSnapshot) return
    this.transactionSnapshot.sqlRollbackOps.push({ type, ns, id, beforeState, ...extra })
  }

  /**
   * Record an R2 path for potential rollback
   */
  recordR2Write(path: string): void {
    if (!this.inTransaction || !this.transactionSnapshot) return
    this.transactionSnapshot.pendingR2Paths.push(path)
  }

  /**
   * Commit the current transaction
   */
  async commitTransaction(): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No transaction in progress')
    }
    this.transactionSnapshot = null
    this.inTransaction = false
  }

  /**
   * Rollback the current transaction
   *
   * Restores all state from the snapshot and reverses any SQL/R2 operations.
   */
  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction || !this.transactionSnapshot) {
      throw new Error('No transaction in progress')
    }

    const snapshot = this.transactionSnapshot

    // 1. Restore in-memory state - counters
    this.counters.clear()
    for (const [k, v] of snapshot.counters) {
      this.counters.set(k, v)
    }

    // 2. Restore entity cache
    const newCache = new Map<string, { entity: Entity; version: number }>()
    for (const [k, v] of snapshot.entityCache) {
      newCache.set(k, v)
    }
    this.cacheManager.setFullCache(newCache)

    // 3. Restore event buffers
    this.eventWal.setEventBuffer([...snapshot.eventBuffer], snapshot.eventBufferSize)

    // 4. Restore namespace event buffers
    const newNsBuffers = new Map<string, EventBuffer>()
    for (const [ns, buffer] of snapshot.nsEventBuffers) {
      newNsBuffers.set(ns, { ...buffer, events: [...buffer.events] })
    }
    this.eventWal.setNsEventBuffers(newNsBuffers)

    // 5. Restore relationship event buffers
    const newRelBuffers = new Map<string, EventBuffer>()
    for (const [ns, buffer] of snapshot.relEventBuffers) {
      newRelBuffers.set(ns, { ...buffer, events: [...buffer.events] })
    }
    this.relWal.setRelEventBuffers(newRelBuffers)

    // 6. Reverse SQL operations (in reverse order)
    for (const op of snapshot.sqlRollbackOps.reverse()) {
      switch (op.type) {
        case 'entity_insert':
          this.sql.exec('DELETE FROM entities WHERE ns = ? AND id = ?', op.ns, op.id)
          break
        case 'entity_update':
          if (op.beforeState) {
            const before = op.beforeState as StoredEntity
            this.sql.exec(
              `UPDATE entities SET type = ?, name = ?, version = ?, created_at = ?, created_by = ?,
               updated_at = ?, updated_by = ?, deleted_at = ?, deleted_by = ?, data = ?
               WHERE ns = ? AND id = ?`,
              before.type,
              before.name,
              before.version,
              before.created_at,
              before.created_by,
              before.updated_at,
              before.updated_by,
              before.deleted_at,
              before.deleted_by,
              before.data,
              op.ns,
              op.id
            )
          }
          break
        case 'entity_delete':
          if (op.beforeState) {
            const before = op.beforeState as StoredEntity
            this.sql.exec(
              `INSERT OR REPLACE INTO entities (ns, id, type, name, version, created_at, created_by,
               updated_at, updated_by, deleted_at, deleted_by, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              op.ns,
              op.id,
              before.type,
              before.name,
              before.version,
              before.created_at,
              before.created_by,
              before.updated_at,
              before.updated_by,
              before.deleted_at,
              before.deleted_by,
              before.data
            )
          }
          break
        case 'rel_insert':
          this.sql.exec(
            'DELETE FROM relationships WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?',
            op.ns,
            op.id,
            op.predicate,
            op.toNs,
            op.toId
          )
          break
        case 'rel_update':
          if (op.beforeState) {
            const before = op.beforeState as StoredRelationship
            this.sql.exec(
              `UPDATE relationships SET version = ?, deleted_at = ?, deleted_by = ?, data = ?
               WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
              before.version,
              before.deleted_at,
              before.deleted_by,
              before.data,
              op.ns,
              op.id,
              op.predicate,
              op.toNs,
              op.toId
            )
          }
          break
        case 'rel_delete':
          if (op.beforeState) {
            const before = op.beforeState as StoredRelationship
            this.sql.exec(
              `INSERT OR REPLACE INTO relationships (from_ns, from_id, predicate, to_ns, to_id,
               reverse, version, created_at, created_by, deleted_at, deleted_by, data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              op.ns,
              op.id,
              before.predicate,
              before.to_ns,
              before.to_id,
              before.reverse,
              before.version,
              before.created_at,
              before.created_by,
              before.deleted_at,
              before.deleted_by,
              before.data
            )
          }
          break
        case 'pending_row_group':
          this.sql.exec('DELETE FROM pending_row_groups WHERE id = ?', op.id)
          break
      }
    }

    // 7. Delete R2 files written during transaction
    for (const path of snapshot.pendingR2Paths) {
      try {
        await this.bucket.delete(path)
      } catch {
        // Ignore errors - file might not exist
      }
    }

    this.transactionSnapshot = null
    this.inTransaction = false
  }

  /**
   * Check if currently in a transaction
   */
  isInTransaction(): boolean {
    return this.inTransaction
  }

  /**
   * Get transaction snapshot for testing
   */
  getTransactionSnapshot(): TransactionSnapshot | null {
    return this.transactionSnapshot
  }
}

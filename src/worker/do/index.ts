/**
 * ParqueDB Durable Object (MODULAR VERSION)
 *
 * @deprecated Use `ParqueDBDO` from `../ParqueDBDO.ts` instead.
 * This modular version was a refactoring attempt but the canonical ParqueDBDO
 * in the parent directory is the one used in production. The modular manager
 * files (cache-manager.ts, event-wal.ts, relationship-wal.ts, flush-manager.ts,
 * transaction-manager.ts) are still useful and may be imported independently.
 *
 * The canonical ParqueDBDO now exposes `protected` fields for subclassing
 * (e.g., by @dotdo/db's DatabaseDO).
 *
 * EVENT-SOURCED ARCHITECTURE:
 * - events_wal is the SINGLE SOURCE OF TRUTH for entity state
 * - Entity state is derived by replaying events (event sourcing)
 * - Entity cache provides fast lookups for recent entities
 * - Relationships table maintained for graph traversal performance
 * - Periodic flush to R2 as Parquet files for reads
 */

import { DurableObject } from 'cloudflare:workers'
import Sqids from 'sqids'
import type {
  Entity,
  EntityId,
  Event,
  Relationship,
  UpdateInput,
  CreateInput,
  Variant,
  DeleteResult,
} from '../../types'
import { entityTarget, relTarget } from '../../types'
import type { Env } from '../../types/worker'
import { parseStoredData } from '../../utils'

// Import types
import type {
  CacheInvalidationSignal,
  DOCreateOptions,
  DOUpdateOptions,
  DODeleteOptions,
  DOLinkOptions,
  StoredEntity,
  StoredRelationship,
  TransactionSnapshot,
  FlushConfig,
} from './types'
import { toEntity, toRelationship } from './types'

// Import modules
import { generateULID } from './ulid'
import { initializeSchema, initializeCounters } from './schema'
import { EntityCacheManager } from './cache-manager'
import { EventWalManager, deserializeEventBatch } from './event-wal'
import { RelationshipWalManager } from './relationship-wal'
import { FlushManager, DEFAULT_FLUSH_CONFIG } from './flush-manager'
import { TransactionManager } from './transaction-manager'

// Re-export types for external use
export type {
  CacheInvalidationSignal,
  DOCreateOptions,
  DOUpdateOptions,
  DODeleteOptions,
  DOLinkOptions,
  StoredEntity,
  StoredRelationship,
  TransactionSnapshot,
  FlushConfig,
}

// Initialize Sqids for short ID generation
const sqids = new Sqids()

/** Bulk write threshold - 5+ entities go directly to R2 instead of SQLite buffer */
const BULK_THRESHOLD = 5

/**
 * Durable Object for ParqueDB write operations
 *
 * All write operations go through this DO to ensure consistency.
 * The DO maintains state using EVENT SOURCING:
 * - events_wal is the SINGLE SOURCE OF TRUTH
 * - Entity state is reconstructed by replaying events
 * - Entity cache provides fast in-memory access
 * - Relationship graph in SQLite for traversal performance
 * - Event log periodically flushed to Parquet for reads
 */
export class ParqueDBDO extends DurableObject<Env> {
  /** SQLite storage */
  private sql: SqlStorage

  /** Whether schema has been initialized */
  private initialized = false

  /** Namespace sequence counters for short ID generation with Sqids */
  private counters: Map<string, number> = new Map()

  /** Whether counters have been initialized from SQLite */
  private countersInitialized = false

  // Module instances
  private cacheManager!: EntityCacheManager
  private eventWal!: EventWalManager
  private relWal!: RelationshipWalManager
  private flushManager!: FlushManager
  private transactionManager!: TransactionManager

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize SQLite schema if not already done
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Initialize schema
    initializeSchema(this.sql)
    this.initialized = true

    // Initialize sequence counters from events_wal
    await this.initializeCountersFromDB()

    // Initialize modules
    this.cacheManager = new EntityCacheManager()
    this.eventWal = new EventWalManager(this.sql, this.counters)
    this.relWal = new RelationshipWalManager(this.sql, this.counters)
    this.flushManager = new FlushManager(this.sql, this.env.BUCKET, this.ctx, DEFAULT_FLUSH_CONFIG)
    this.transactionManager = new TransactionManager(
      this.sql,
      this.env.BUCKET,
      this.counters,
      this.cacheManager,
      this.eventWal,
      this.relWal
    )
  }

  /**
   * Initialize namespace counters from events_wal and rels_wal tables
   */
  private async initializeCountersFromDB(): Promise<void> {
    if (this.countersInitialized) return

    const loadedCounters = initializeCounters(this.sql)
    for (const [ns, seq] of loadedCounters) {
      this.counters.set(ns, seq)
    }

    this.countersInitialized = true
  }

  /**
   * Get next sequence number for namespace and generate short ID with Sqids
   */
  private getNextId(ns: string): string {
    const seq = this.counters.get(ns) || 1
    this.counters.set(ns, seq + 1)
    return sqids.encode([seq])
  }

  // ===========================================================================
  // Entity Operations
  // ===========================================================================

  /**
   * Create a new entity
   */
  async create(ns: string, data: CreateInput, options: DOCreateOptions = {}): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const id = this.getNextId(ns)
    const entityIdValue = `${ns}/${id}`

    // Extract $type and name, rest goes to data
    const { $type, name, ...rest } = data
    if (!$type) {
      throw new Error('Entity must have $type')
    }
    if (!name) {
      throw new Error('Entity must have name')
    }

    // Extract relationship links from data
    const dataWithoutLinks: Record<string, unknown> = {}
    const links: Array<{ predicate: string; targetId: string }> = []

    for (const [key, value] of Object.entries(rest)) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        // Check if this looks like a relationship { displayName: entityId }
        const entries = Object.entries(value as Record<string, unknown>)
        if (entries.length > 0 && entries.every(([_, v]) => typeof v === 'string' && (v as string).includes('/'))) {
          // This is a relationship
          for (const [_, targetId] of entries) {
            links.push({ predicate: key, targetId: targetId as string })
          }
          continue
        }
      }
      dataWithoutLinks[key] = value
    }

    const dataJson = JSON.stringify(dataWithoutLinks)

    // Create relationships
    for (const link of links) {
      await this.link(entityIdValue, link.predicate, link.targetId, { actor })
    }

    // Append create event
    await this.eventWal.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: entityTarget(ns, id),
      before: undefined,
      after: { ...dataWithoutLinks, $type, name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation for Workers
    this.cacheManager.signal(ns, 'entity', id)

    // Schedule flush if needed
    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)

    return toEntity({
      ns,
      id,
      type: $type,
      name,
      version: 1,
      created_at: now,
      created_by: actor,
      updated_at: now,
      updated_by: actor,
      deleted_at: null,
      deleted_by: null,
      data: dataJson,
    })
  }

  /**
   * Create multiple entities at once
   */
  async createMany(ns: string, items: CreateInput[], options: DOCreateOptions = {}): Promise<Entity[]> {
    await this.ensureInitialized()

    if (items.length === 0) {
      return []
    }

    // For bulk creates (5+ entities), write directly to R2 pending files
    if (items.length >= BULK_THRESHOLD) {
      return this.bulkWriteToR2(ns, items, options)
    }

    // For small batches, use standard event buffering
    const entities: Entity[] = []
    for (const item of items) {
      const entity = await this.create(ns, item, options)
      entities.push(entity)
    }
    return entities
  }

  /**
   * Bulk write entities directly to R2 as a pending Parquet file
   */
  private async bulkWriteToR2(ns: string, items: CreateInput[], options: DOCreateOptions = {}): Promise<Entity[]> {
    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const pendingId = generateULID()

    // Reserve sequence numbers for all items
    const firstSeq = this.counters.get(ns) || 1
    const lastSeq = firstSeq + items.length - 1
    this.counters.set(ns, lastSeq + 1)

    // Build entities with assigned IDs
    const entities: Entity[] = []
    const rows: Array<{ $id: string; data: string }> = []

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!
      const seq = firstSeq + i
      const id = sqids.encode([seq])
      const entityId = `${ns}/${id}` as EntityId

      const { $type, name, ...rest } = item
      if (!$type) {
        throw new Error('Entity must have $type')
      }
      if (!name) {
        throw new Error('Entity must have name')
      }

      const entity: Entity = {
        $id: entityId,
        $type,
        name,
        createdAt: new Date(now),
        createdBy: actor as EntityId,
        updatedAt: new Date(now),
        updatedBy: actor as EntityId,
        version: 1,
        ...rest,
      }
      entities.push(entity)

      const dataObj = { $type, name, ...rest }
      rows.push({
        $id: entityId,
        data: JSON.stringify(dataObj),
      })
    }

    // Write pending Parquet file to R2
    const pendingPath = `data/${ns}/pending/${pendingId}.parquet`

    try {
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const columnData = [
        { name: '$id', data: rows.map(r => r.$id) },
        { name: 'data', data: rows.map(r => r.data) },
      ]
      const buffer = parquetWriteBuffer({ columnData })
      await this.env.BUCKET.put(pendingPath, buffer)
    } catch (error: unknown) {
      // CRITICAL: Parquet write failed - propagate the error to prevent silent data loss
      // Falling back to JSON format could cause inconsistencies as readers expect Parquet
      const message = error instanceof Error ? error.message : 'Unknown error'
      throw new Error(`Failed to write pending Parquet file to R2: ${message}`)
    }

    // Record pending row group metadata
    this.sql.exec(
      `INSERT INTO pending_row_groups (id, ns, path, row_count, first_seq, last_seq, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      pendingId,
      ns,
      pendingPath,
      items.length,
      firstSeq,
      lastSeq,
      now
    )

    // Append CREATE events
    for (let i = 0; i < items.length; i++) {
      const item = items[i]!
      const seq = firstSeq + i
      const entityId = sqids.encode([seq])
      const { $type, name, ...rest } = item

      await this.eventWal.appendEvent({
        id: generateULID(),
        ts: Date.now(),
        op: 'CREATE',
        target: entityTarget(ns, entityId),
        before: undefined,
        after: { ...rest, $type, name } as Variant,
        actor: actor as string,
      })
    }

    // Signal cache invalidation for bulk writes
    this.cacheManager.signal(ns, 'full')

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)

    return entities
  }

  /**
   * Update an entity by ID
   */
  async update(ns: string, id: string, update: UpdateInput, options: DOUpdateOptions = {}): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Get current entity state from events
    let current: StoredEntity | null = null
    const currentEntity = await this.getEntityFromEvents(ns, id)
    if (currentEntity && !currentEntity.deletedAt) {
      const {
        $id: _$id,
        $type,
        name: entityName,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy,
        deletedAt: _deletedAt,
        deletedBy: _deletedBy,
        version,
        ...rest
      } = currentEntity
      current = {
        ns,
        id,
        type: $type,
        name: entityName,
        version,
        created_at: createdAt.toISOString(),
        created_by: createdBy as string,
        updated_at: updatedAt.toISOString(),
        updated_by: updatedBy as string,
        deleted_at: null,
        deleted_by: null,
        data: JSON.stringify(rest),
      }
    }

    if (!current) {
      if (options.upsert) {
        const createData: CreateInput = {
          $type: 'Unknown',
          name: id,
          ...(update.$set ?? {}),
        }
        return this.create(ns, createData, { actor })
      }
      throw new Error(`Entity ${ns}/${id} not found`)
    }

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Apply update operators
    let data = parseStoredData(current.data)
    let name = current.name
    let type = current.type

    // $set operator
    if (update.$set) {
      for (const [key, value] of Object.entries(update.$set)) {
        if (key === 'name') {
          name = value as string
        } else if (key === '$type') {
          type = value as string
        } else {
          data[key] = value
        }
      }
    }

    // $unset operator
    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete data[key]
      }
    }

    // $inc operator
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        const curr = (data[key] as number | undefined) ?? 0
        data[key] = curr + (value as number)
      }
    }

    // $push operator
    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const arr = (data[key] as unknown[] | undefined) ?? []
        if (typeof value === 'object' && value !== null && '$each' in value) {
          arr.push(...(value as { $each: unknown[] }).$each)
        } else {
          arr.push(value)
        }
        data[key] = arr
      }
    }

    // $pull operator
    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        const arr = (data[key] as unknown[] | undefined) ?? []
        data[key] = arr.filter(item => item !== value)
      }
    }

    // $link operator
    if (update.$link) {
      const entityIdValue = `${ns}/${id}`
      for (const [predicate, targets] of Object.entries(update.$link)) {
        const targetList = Array.isArray(targets) ? targets : [targets]
        for (const targetId of targetList) {
          await this.link(entityIdValue, predicate, targetId as string, { actor })
        }
      }
    }

    // $unlink operator
    if (update.$unlink) {
      const entityIdValue = `${ns}/${id}`
      for (const [predicate, targets] of Object.entries(update.$unlink)) {
        const targetList = Array.isArray(targets) ? targets : [targets]
        for (const targetId of targetList) {
          await this.unlink(entityIdValue, predicate, targetId as string, { actor })
        }
      }
    }

    const newVersion = current.version + 1
    const dataJson = JSON.stringify(data)

    // Invalidate cache
    this.cacheManager.invalidate(ns, id)

    // Append update event
    await this.eventWal.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { ...parseStoredData(current.data), $type: current.type, name: current.name } as Variant,
      after: { ...data, $type: type, name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.cacheManager.signal(ns, 'entity', id)

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)

    return toEntity({
      ns,
      id,
      type,
      name,
      version: newVersion,
      created_at: current.created_at,
      created_by: current.created_by,
      updated_at: now,
      updated_by: actor,
      deleted_at: null,
      deleted_by: null,
      data: dataJson,
    })
  }

  /**
   * Delete an entity by ID
   *
   * @returns DeleteResult with deletedCount (consistent with local ParqueDB API)
   */
  async delete(ns: string, id: string, options: DODeleteOptions = {}): Promise<DeleteResult> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Get current entity state
    let current: StoredEntity | null = null
    const currentEntity = await this.getEntityFromEvents(ns, id)
    if (currentEntity) {
      const {
        $id: _$id,
        $type,
        name: entityName,
        createdAt,
        createdBy,
        updatedAt,
        updatedBy,
        deletedAt,
        deletedBy,
        version,
        ...rest
      } = currentEntity
      current = {
        ns,
        id,
        type: $type,
        name: entityName,
        version,
        created_at: createdAt.toISOString(),
        created_by: createdBy as string,
        updated_at: updatedAt.toISOString(),
        updated_by: updatedBy as string,
        deleted_at: deletedAt?.toISOString() ?? null,
        deleted_by: (deletedBy as string) ?? null,
        data: JSON.stringify(rest),
      }
    }

    if (!current) {
      return { deletedCount: 0 }
    }

    // Check version
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Handle relationships
    if (options.hard) {
      this.sql.exec(
        'DELETE FROM relationships WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)',
        ns,
        id,
        ns,
        id
      )
    } else {
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ?
         WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)`,
        now,
        actor,
        ns,
        id,
        ns,
        id
      )
    }

    // Invalidate cache
    this.cacheManager.invalidate(ns, id)

    // Append delete event
    await this.eventWal.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'DELETE',
      target: entityTarget(ns, id),
      before: { ...parseStoredData(current.data), $type: current.type, name: current.name } as Variant,
      after: undefined,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.cacheManager.signal(ns, 'entity', id)

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)

    return { deletedCount: 1 }
  }

  /**
   * Delete multiple entities by IDs
   *
   * Note: This is a simplified implementation that deletes by IDs.
   * For complex filtering, use QueryExecutor to find matching entities first.
   *
   * @returns DeleteResult with total deletedCount
   */
  async deleteMany(ns: string, ids: string[], options: DODeleteOptions = {}): Promise<DeleteResult> {
    let deletedCount = 0

    for (const id of ids) {
      const result = await this.delete(ns, id, options)
      deletedCount += result.deletedCount
    }

    return { deletedCount }
  }

  /**
   * Restore a soft-deleted entity
   *
   * @returns Restored entity or null if not found
   */
  async restore(ns: string, id: string, options: { actor?: string | undefined } = {}): Promise<Entity | null> {
    await this.ensureInitialized()

    const actor = options.actor || 'system/anonymous'

    // Get current entity state from events
    const currentEntity = await this.getEntityFromEvents(ns, id)
    if (!currentEntity) {
      return null // Entity doesn't exist
    }

    if (!currentEntity.deletedAt) {
      return currentEntity // Entity is not deleted, return as-is
    }

    // Create a restore event (UPDATE that removes deletedAt/deletedBy)
    const { deletedAt: _deletedAt, deletedBy: _deletedBy, ...rest } = currentEntity
    const restoredData = {
      ...rest,
      updatedAt: new Date(),
      updatedBy: actor as EntityId,
      version: currentEntity.version + 1,
    }

    await this.eventWal.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { $type: currentEntity.$type, name: currentEntity.name, deletedAt: currentEntity.deletedAt } as Variant,
      after: { $type: currentEntity.$type, name: currentEntity.name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.cacheManager.signal(ns, 'entity', id)
    this.cacheManager.invalidate(ns, id)

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)

    return restoredData as Entity
  }

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  /**
   * Create a relationship between two entities
   */
  async link(fromId: string, predicate: string, toId: string, options: DOLinkOptions = {}): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Parse entity IDs
    const [fromNsPart, ...fromIdParts] = fromId.split('/')
    const fromNs = fromNsPart!
    const fromEntityId = fromIdParts.join('/')
    const [toNsPart, ...toIdParts] = toId.split('/')
    const toNs = toNsPart!
    const toEntityId = toIdParts.join('/')

    // Generate reverse predicate
    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    // Check if relationship exists
    const existing = [...this.sql.exec<StoredRelationship>(
      `SELECT * FROM relationships
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
      fromNs,
      fromEntityId,
      predicate,
      toNs,
      toEntityId
    )]

    if (existing.length > 0 && existing[0]!.deleted_at === null) {
      return
    }

    const dataJson = options.data ? JSON.stringify(options.data) : null
    const matchMode = options.matchMode ?? null
    const similarity = options.similarity ?? null

    // Validate similarity
    if (similarity !== null && (similarity < 0 || similarity > 1)) {
      throw new Error(`Similarity must be between 0 and 1, got ${similarity}`)
    }

    if (matchMode === 'exact' && similarity !== null && similarity !== 1.0) {
      throw new Error(`Exact match mode should have similarity of 1.0 or null, got ${similarity}`)
    }

    if (existing.length > 0) {
      // Undelete and update
      this.sql.exec(
        `UPDATE relationships
         SET deleted_at = NULL, deleted_by = NULL, version = version + 1,
             match_mode = ?, similarity = ?, data = ?
         WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
        matchMode,
        similarity,
        dataJson,
        fromNs,
        fromEntityId,
        predicate,
        toNs,
        toEntityId
      )
    } else {
      // Insert new relationship
      this.sql.exec(
        `INSERT INTO relationships
         (from_ns, from_id, predicate, to_ns, to_id, reverse, version, created_at, created_by, match_mode, similarity, data)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
        fromNs,
        fromEntityId,
        predicate,
        toNs,
        toEntityId,
        reverse,
        now,
        actor,
        matchMode,
        similarity,
        dataJson
      )
    }

    // Append relationship event
    await this.relWal.appendRelEvent(fromNs, {
      ts: Date.now(),
      op: 'CREATE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: undefined,
      after: {
        predicate,
        to: toId,
        matchMode: options.matchMode,
        similarity: options.similarity,
        data: options.data,
      } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.cacheManager.signal(fromNs, 'relationship')
    if (fromNs !== toNs) {
      this.cacheManager.signal(toNs, 'relationship')
    }

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)
  }

  /**
   * Remove a relationship between two entities
   */
  async unlink(fromId: string, predicate: string, toId: string, options: DOLinkOptions = {}): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Parse entity IDs
    const [fromNsPart, ...fromIdParts] = fromId.split('/')
    const fromNs = fromNsPart!
    const fromEntityId = fromIdParts.join('/')
    const [toNsPart, ...toIdParts] = toId.split('/')
    const toNs = toNsPart!
    const toEntityId = toIdParts.join('/')

    // Soft delete the relationship
    this.sql.exec(
      `UPDATE relationships
       SET deleted_at = ?, deleted_by = ?, version = version + 1
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?
       AND deleted_at IS NULL`,
      now,
      actor,
      fromNs,
      fromEntityId,
      predicate,
      toNs,
      toEntityId
    )

    // Append relationship event
    await this.relWal.appendRelEvent(fromNs, {
      ts: Date.now(),
      op: 'DELETE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: { predicate, to: toId } as Variant,
      after: undefined,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.cacheManager.signal(fromNs, 'relationship')
    if (fromNs !== toNs) {
      this.cacheManager.signal(toNs, 'relationship')
    }

    const unflushedCount = await this.eventWal.getUnflushedEventCount()
    await this.flushManager.maybeScheduleFlush(unflushedCount)
  }

  /**
   * Get relationships for an entity
   */
  async getRelationships(
    ns: string,
    id: string,
    predicate?: string,
    direction: 'outbound' | 'inbound' = 'outbound'
  ): Promise<Relationship[]> {
    await this.ensureInitialized()

    let query: string
    let params: unknown[]

    if (direction === 'outbound') {
      query = predicate
        ? 'SELECT * FROM relationships WHERE from_ns = ? AND from_id = ? AND predicate = ? AND deleted_at IS NULL'
        : 'SELECT * FROM relationships WHERE from_ns = ? AND from_id = ? AND deleted_at IS NULL'
      params = predicate ? [ns, id, predicate] : [ns, id]
    } else {
      query = predicate
        ? 'SELECT * FROM relationships WHERE to_ns = ? AND to_id = ? AND reverse = ? AND deleted_at IS NULL'
        : 'SELECT * FROM relationships WHERE to_ns = ? AND to_id = ? AND deleted_at IS NULL'
      params = predicate ? [ns, id, predicate] : [ns, id]
    }

    const rows = [...this.sql.exec<StoredRelationship>(query, ...params)]
    return rows.map(row => toRelationship(row))
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Get an entity by ID from DO storage
   */
  async get(ns: string, id: string, includeDeleted = false): Promise<Entity | null> {
    await this.ensureInitialized()

    // Check cache first
    const cached = this.cacheManager.get(ns, id)
    if (cached) {
      const entity = cached.entity
      if (!includeDeleted && entity.deletedAt) {
        return null
      }
      return entity
    }

    // Reconstruct from events
    const entityFromEvents = await this.getEntityFromEvents(ns, id)
    if (entityFromEvents) {
      if (!includeDeleted && entityFromEvents.deletedAt) {
        return null
      }
      this.cacheManager.set(ns, id, entityFromEvents)
      return entityFromEvents
    }

    return null
  }

  /**
   * Reconstruct entity state from unflushed events
   */
  async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
    await this.ensureInitialized()

    const target = `${ns}:${id}`
    let entity: Entity | null = null

    // 1. Read from event_batches
    interface EventBatchRow extends Record<string, SqlStorageValue> {
      batch: ArrayBuffer
    }

    const batchRows = [...this.sql.exec<EventBatchRow>(
      `SELECT batch FROM event_batches WHERE flushed = 0 ORDER BY min_ts ASC`
    )]

    for (const row of batchRows) {
      const batchEvents = deserializeEventBatch(row.batch)
      for (const event of batchEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // 2. Read from in-memory event buffer
    const eventBuffer = this.eventWal.getEventBuffer()
    for (const event of eventBuffer) {
      if (event.target === target) {
        entity = this.applyEventToEntity(entity, event, ns, id)
      }
    }

    // 3. Check events_wal
    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const walRows = [...this.sql.exec<WalRow>(
      `SELECT events FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of walRows) {
      const walEvents = deserializeEventBatch(row.events)
      for (const event of walEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // 4. Check namespace event buffers
    const walEvents = await this.eventWal.readUnflushedWalEvents(ns)
    for (const event of walEvents) {
      if (event.target === target) {
        entity = this.applyEventToEntity(entity, event, ns, id)
      }
    }

    return entity
  }

  /**
   * Apply a single event to an entity state
   */
  private applyEventToEntity(current: Entity | null, event: Event, ns: string, id: string): Entity | null {
    const entityId = `${ns}/${id}` as EntityId

    switch (event.op) {
      case 'CREATE': {
        if (!event.after) return current
        const { $type, name, ...rest } = event.after as { $type: string; name: string; [key: string]: unknown }
        return {
          $id: entityId,
          $type: $type || 'Unknown',
          name: name || id,
          createdAt: new Date(event.ts),
          createdBy: (event.actor || 'system/anonymous') as EntityId,
          updatedAt: new Date(event.ts),
          updatedBy: (event.actor || 'system/anonymous') as EntityId,
          version: 1,
          ...rest,
        } as Entity
      }

      case 'UPDATE': {
        if (!current || !event.after) return current
        const { $type, name, ...rest } = event.after as { $type?: string | undefined; name?: string | undefined; [key: string]: unknown }
        return {
          ...current,
          $type: $type || current.$type,
          name: name || current.name,
          updatedAt: new Date(event.ts),
          updatedBy: (event.actor || 'system/anonymous') as EntityId,
          version: current.version + 1,
          ...rest,
        } as Entity
      }

      case 'DELETE': {
        if (!current) return null
        return {
          ...current,
          deletedAt: new Date(event.ts),
          deletedBy: (event.actor || 'system/anonymous') as EntityId,
          version: current.version + 1,
        } as Entity
      }

      default:
        return current
    }
  }

  // ===========================================================================
  // Pending Row Groups
  // ===========================================================================

  /**
   * Get pending row groups for a namespace
   */
  async getPendingRowGroups(ns: string): Promise<
    Array<{
      id: string
      path: string
      rowCount: number
      firstSeq: number
      lastSeq: number
      createdAt: string
    }>
  > {
    await this.ensureInitialized()

    interface PendingRowGroupRow {
      [key: string]: SqlStorageValue
      id: string
      path: string
      row_count: number
      first_seq: number
      last_seq: number
      created_at: string
    }

    const rows = [...this.sql.exec<PendingRowGroupRow>(
      `SELECT id, path, row_count, first_seq, last_seq, created_at
       FROM pending_row_groups
       WHERE ns = ?
       ORDER BY first_seq ASC`,
      ns
    )]

    return rows.map(row => ({
      id: row.id,
      path: row.path,
      rowCount: row.row_count,
      firstSeq: row.first_seq,
      lastSeq: row.last_seq,
      createdAt: row.created_at,
    }))
  }

  /**
   * Delete pending row groups after promotion
   */
  async deletePendingRowGroups(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()

    this.sql.exec(`DELETE FROM pending_row_groups WHERE ns = ? AND last_seq <= ?`, ns, upToSeq)
  }

  /**
   * Promote pending files to committed data file
   */
  async flushPendingToCommitted(ns: string): Promise<number> {
    await this.ensureInitialized()

    const pending = await this.getPendingRowGroups(ns)
    if (pending.length === 0) {
      return 0
    }

    const totalRows = pending.reduce((sum, p) => sum + p.rowCount, 0)
    const maxSeq = Math.max(...pending.map(p => p.lastSeq))

    await this.deletePendingRowGroups(ns, maxSeq)

    return totalRows
  }

  // ===========================================================================
  // Cache Invalidation API
  // ===========================================================================

  /**
   * Get invalidation version for a namespace
   */
  getInvalidationVersion(ns: string): number {
    return this.cacheManager.getInvalidationVersion(ns)
  }

  /**
   * Get invalidation versions for all namespaces
   */
  getAllInvalidationVersions(): Record<string, number> {
    return this.cacheManager.getAllInvalidationVersions()
  }

  /**
   * Get pending invalidation signals
   */
  getPendingInvalidations(ns?: string, sinceVersion?: number): CacheInvalidationSignal[] {
    return this.cacheManager.getPendingInvalidations(ns, sinceVersion)
  }

  /**
   * Check if caches should be invalidated
   */
  shouldInvalidate(ns: string, workerVersion: number): boolean {
    return this.cacheManager.shouldInvalidate(ns, workerVersion)
  }

  // ===========================================================================
  // Cache Stats API
  // ===========================================================================

  getCacheStats(): { size: number; maxSize: number } {
    return this.cacheManager.getStats()
  }

  clearEntityCache(): void {
    this.cacheManager.clear()
  }

  isEntityCached(ns: string, id: string): boolean {
    return this.cacheManager.has(ns, id)
  }

  // ===========================================================================
  // Event WAL API
  // ===========================================================================

  async appendEventWithSeq(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    await this.ensureInitialized()
    return this.eventWal.appendEventWithSeq(ns, event)
  }

  async flushNsEventBatch(ns: string): Promise<void> {
    await this.ensureInitialized()
    await this.eventWal.flushNsEventBatch(ns)
  }

  async flushAllNsEventBatches(): Promise<void> {
    await this.ensureInitialized()
    await this.eventWal.flushAllNsEventBatches()
  }

  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()
    return this.eventWal.getUnflushedEventCount()
  }

  async getUnflushedWalEventCount(ns: string): Promise<number> {
    await this.ensureInitialized()
    return this.eventWal.getUnflushedWalEventCount(ns)
  }

  async getTotalUnflushedWalEventCount(): Promise<number> {
    await this.ensureInitialized()
    return this.eventWal.getTotalUnflushedWalEventCount()
  }

  async getUnflushedWalBatchCount(): Promise<number> {
    await this.ensureInitialized()
    return this.eventWal.getUnflushedWalBatchCount()
  }

  async readUnflushedEvents(): Promise<Event[]> {
    await this.ensureInitialized()
    return this.eventWal.readUnflushedEvents()
  }

  async readUnflushedWalEvents(ns: string): Promise<Event[]> {
    await this.ensureInitialized()
    return this.eventWal.readUnflushedWalEvents(ns)
  }

  async deleteWalBatches(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()
    await this.eventWal.deleteWalBatches(ns, upToSeq)
  }

  getSequenceCounter(ns: string): number {
    return this.eventWal.getSequenceCounter(ns)
  }

  getNsBufferState(ns: string): { eventCount: number; firstSeq: number; lastSeq: number; sizeBytes: number } | null {
    return this.eventWal.getNsBufferState(ns)
  }

  // ===========================================================================
  // Relationship WAL API
  // ===========================================================================

  async appendRelEvent(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    await this.ensureInitialized()
    return this.relWal.appendRelEvent(ns, event)
  }

  async flushRelEventBatch(ns: string): Promise<void> {
    await this.ensureInitialized()
    await this.relWal.flushRelEventBatch(ns)
  }

  async flushAllRelEventBatches(): Promise<void> {
    await this.ensureInitialized()
    await this.relWal.flushAllRelEventBatches()
  }

  async getUnflushedRelEventCount(ns: string): Promise<number> {
    await this.ensureInitialized()
    return this.relWal.getUnflushedRelEventCount(ns)
  }

  async getTotalUnflushedRelEventCount(): Promise<number> {
    await this.ensureInitialized()
    return this.relWal.getTotalUnflushedRelEventCount()
  }

  async getUnflushedRelBatchCount(ns?: string): Promise<number> {
    await this.ensureInitialized()
    return this.relWal.getUnflushedRelBatchCount(ns)
  }

  async readUnflushedRelEvents(ns: string): Promise<Event[]> {
    await this.ensureInitialized()
    return this.relWal.readUnflushedRelEvents(ns)
  }

  async deleteRelWalBatches(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()
    await this.relWal.deleteRelWalBatches(ns, upToSeq)
  }

  getRelSequenceCounter(ns: string): number {
    return this.relWal.getRelSequenceCounter(ns)
  }

  getRelBufferState(ns: string): {
    eventCount: number
    firstSeq: number
    lastSeq: number
    sizeBytes: number
  } | null {
    return this.relWal.getRelBufferState(ns)
  }

  // ===========================================================================
  // Flush API
  // ===========================================================================

  async flushToParquet(): Promise<void> {
    await this.ensureInitialized()
    await this.eventWal.flushAllNsEventBatches()
    await this.flushManager.flushToParquet()
  }

  // ===========================================================================
  // Transaction API
  // ===========================================================================

  beginTransaction(): string {
    return this.transactionManager.beginTransaction()
  }

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
    this.transactionManager.recordSqlOperation(type, ns, id, beforeState, extra)
  }

  recordR2Write(path: string): void {
    this.transactionManager.recordR2Write(path)
  }

  async commitTransaction(): Promise<void> {
    await this.transactionManager.commitTransaction()
  }

  async rollbackTransaction(): Promise<void> {
    await this.transactionManager.rollbackTransaction()
  }

  isInTransaction(): boolean {
    return this.transactionManager.isInTransaction()
  }

  getTransactionSnapshot(): TransactionSnapshot | null {
    return this.transactionManager.getTransactionSnapshot()
  }

  // ===========================================================================
  // Alarm Handler
  // ===========================================================================

  override async alarm(): Promise<void> {
    this.flushManager.resetAlarmFlag()

    // Flush all buffered events to Parquet
    await this.flushToParquet()
  }
}

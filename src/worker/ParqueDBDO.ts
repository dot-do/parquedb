/**
 * ParqueDB Durable Object
 *
 * Handles all write operations for consistency. Reads can bypass the DO
 * and go directly to R2 for better performance.
 *
 * EVENT-SOURCED ARCHITECTURE:
 * - events_wal is the SINGLE SOURCE OF TRUTH for entity state
 * - Entity state is derived by replaying events (event sourcing)
 * - Entity cache provides fast lookups for recent entities
 * - Relationships table maintained for graph traversal performance
 * - Periodic flush to R2 as Parquet files for reads
 *
 * This follows true event-sourcing principles:
 * 1. All mutations append events to events_wal
 * 2. Entity state is reconstructed by replaying events
 * 3. Cache invalidation happens on write
 * 4. Snapshots can be used for performance (future enhancement)
 *
 * @see ParqueDBWorker - The Worker entrypoint that coordinates reads (R2) and writes (this DO)
 * @see QueryExecutor - Handles reads directly from R2 Parquet files
 * @see ReadPath - Caching layer for R2 reads
 */

import { DurableObject } from 'cloudflare:workers'
import Sqids from 'sqids'
import type {
  Entity,
  EntityId,
  Event,
  Namespace,
  Id,
  Relationship,
  UpdateInput,
  CreateInput,
  Variant,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  DeleteResult,
} from '../types'
import { entityTarget, relTarget } from '../types'
import type { Env, FlushConfig } from '../types/worker'
import { initDOSqliteSchema } from '../types/worker'
import { parseStoredData } from '../utils'
import { generateULID } from './do/ulid'
import {
  EVENT_BATCH_COUNT_THRESHOLD as IMPORTED_EVENT_BATCH_COUNT_THRESHOLD,
  DEFAULT_EVENT_BATCH_SIZE_BYTES,
  BULK_WRITE_THRESHOLD,
  DEFAULT_ENTITY_CACHE_SIZE,
  MAX_PENDING_INVALIDATIONS as IMPORTED_MAX_PENDING_INVALIDATIONS,
  DEFAULT_DO_FLUSH_INTERVAL_MS,
} from '../constants'

// =============================================================================
// Cache Invalidation Types
// =============================================================================

/**
 * Cache invalidation signal stored in DO
 * Workers poll this to know when to invalidate their caches
 */
export interface CacheInvalidationSignal {
  /** Namespace that was modified */
  ns: string
  /** Type of invalidation */
  type: 'entity' | 'relationship' | 'full'
  /** Timestamp of the modification */
  timestamp: number
  /** Version number (monotonically increasing) */
  version: number
  /** Optional entity ID for entity-specific invalidation */
  entityId?: string | undefined
}

// Initialize Sqids for short ID generation
const sqids = new Sqids()

// =============================================================================
// Types
// =============================================================================

/**
 * DO-specific create options.
 * Extends the standard CreateOptions with additional DO-specific behavior.
 * @deprecated Use CreateOptions from '../types' directly for consistency
 */
export interface DOCreateOptions extends Omit<CreateOptions, 'actor'> {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
}

/**
 * DO-specific update options.
 * Extends the standard UpdateOptions with additional DO-specific behavior.
 * @deprecated Use UpdateOptions from '../types' directly for consistency
 */
export interface DOUpdateOptions extends Omit<UpdateOptions, 'actor' | 'returnDocument'> {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
}

/**
 * DO-specific delete options.
 * Extends the standard DeleteOptions with additional DO-specific behavior.
 * @deprecated Use DeleteOptions from '../types' directly for consistency
 */
export interface DODeleteOptions extends Omit<DeleteOptions, 'actor'> {
  /** Actor performing the operation (string in DO context) */
  actor?: string | undefined
}

/** Options for link operation */
export interface DOLinkOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /**
   * How the relationship was matched (SHREDDED)
   * - 'exact': Precise match (user explicitly linked)
   * - 'fuzzy': Approximate match (entity resolution, text similarity)
   */
  matchMode?: 'exact' | 'fuzzy' | undefined
  /**
   * Similarity score for fuzzy matches (SHREDDED)
   * Range: 0.0 to 1.0
   * Only meaningful when matchMode is 'fuzzy'
   */
  similarity?: number | undefined
  /** Edge data (remaining metadata in Variant) */
  data?: Record<string, unknown> | undefined
}

/** Entity as stored in SQLite */
interface StoredEntity {
  [key: string]: SqlStorageValue
  ns: string
  id: string
  type: string
  name: string
  version: number
  created_at: string
  created_by: string
  updated_at: string
  updated_by: string
  deleted_at: string | null
  deleted_by: string | null
  data: string
}

/** Relationship as stored in SQLite */
interface StoredRelationship {
  [key: string]: SqlStorageValue
  from_ns: string
  from_id: string
  predicate: string
  to_ns: string
  to_id: string
  reverse: string
  version: number
  created_at: string
  created_by: string
  deleted_at: string | null
  deleted_by: string | null
  // Shredded fields (top-level columns for efficient querying)
  match_mode: string | null  // 'exact' | 'fuzzy'
  similarity: number | null  // 0.0 to 1.0
  // Remaining metadata in Variant
  data: string | null
}

// Note: StoredEvent interface removed - legacy table kept for backward compatibility only

// =============================================================================
// ParqueDBDO Class
// =============================================================================

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
/** WAL batch thresholds */
const EVENT_BATCH_COUNT_THRESHOLD = IMPORTED_EVENT_BATCH_COUNT_THRESHOLD
const EVENT_BATCH_SIZE_THRESHOLD = DEFAULT_EVENT_BATCH_SIZE_BYTES

/** Bulk write threshold - 5+ entities go directly to R2 instead of SQLite buffer */
const BULK_THRESHOLD = BULK_WRITE_THRESHOLD

export class ParqueDBDO extends DurableObject<Env> {
  /** SQLite storage */
  private sql: SqlStorage

  /** Whether schema has been initialized */
  private initialized = false

  /** Flush configuration */
  private flushConfig: FlushConfig = {
    minEvents: EVENT_BATCH_COUNT_THRESHOLD,
    maxInterval: DEFAULT_DO_FLUSH_INTERVAL_MS * 2, // 60 seconds
    maxEvents: 10000, // DEFAULT_MAX_EVENTS from constants
    rowGroupSize: 1000, // MAX_BATCH_SIZE from constants
  }

  /** Pending flush alarm */
  private flushAlarmSet = false

  /** Namespace sequence counters for short ID generation with Sqids */
  private counters: Map<string, number> = new Map()

  /** Consolidated event buffers per namespace for WAL batching with sequence tracking
   *  This is the SINGLE event buffering system - events are routed here by appendEvent()
   *  and flushed to events_wal table */
  private nsEventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }> = new Map()

  /** Relationship event buffers per namespace for WAL batching (Phase 4) */
  private relEventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }> = new Map()

  /** Whether counters have been initialized from SQLite */
  private countersInitialized = false

  /** LRU cache for recent entity states (derived from events) */
  private entityCache: Map<string, { entity: Entity; version: number }> = new Map()

  /** Maximum size of the entity cache */
  private static readonly ENTITY_CACHE_MAX_SIZE = DEFAULT_ENTITY_CACHE_SIZE

  /** Cache invalidation version per namespace - used by Workers to detect stale caches */
  private invalidationVersions: Map<string, number> = new Map()

  /** Pending invalidation signals - Workers poll this to know what to invalidate */
  private pendingInvalidations: CacheInvalidationSignal[] = []

  /** Maximum pending invalidation signals to keep (circular buffer behavior) */
  private static readonly MAX_PENDING_INVALIDATIONS = IMPORTED_MAX_PENDING_INVALIDATIONS

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

    // Create all tables and indexes using centralized schema definitions
    initDOSqliteSchema(this.sql)

    this.initialized = true

    // Initialize sequence counters from events_wal
    await this.initializeCounters()
  }

  /**
   * Initialize namespace counters from events_wal and rels_wal tables
   * On DO startup, load the max sequence for each namespace
   */
  private async initializeCounters(): Promise<void> {
    if (this.countersInitialized) return

    interface CounterRow {
      [key: string]: SqlStorageValue
      ns: string
      max_seq: number
    }

    // Get max sequence for each namespace from events_wal
    const rows = [...this.sql.exec<CounterRow>(
      `SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`
    )]

    for (const row of rows) {
      // Next ID starts after max_seq
      this.counters.set(row.ns, row.max_seq + 1)
    }

    // Also initialize from rels_wal for relationship sequences
    // Use separate counter namespace to avoid conflicts
    const relRows = [...this.sql.exec<CounterRow>(
      `SELECT ns, MAX(last_seq) as max_seq FROM rels_wal GROUP BY ns`
    )]

    for (const row of relRows) {
      // Store relationship counters with 'rel:' prefix to avoid conflicts
      const relCounterKey = `rel:${row.ns}`
      this.counters.set(relCounterKey, row.max_seq + 1)
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
   *
   * @param ns - Namespace for the entity
   * @param data - Entity data including $type and name
   * @param options - Create options
   * @returns Created entity
   */
  async create(
    ns: string,
    data: CreateInput,
    options: DOCreateOptions = {}
  ): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    // Use Sqids for short, human-friendly IDs based on namespace counter
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

    // EVENT SOURCING: Entity state is derived from events, no need to write to entities table
    // The events_wal is the single source of truth

    // Create relationships
    for (const link of links) {
      await this.link(entityIdValue, link.predicate, link.targetId, { actor })
    }

    // Append create event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: entityTarget(ns, id),
      before: undefined,
      after: { ...dataWithoutLinks, $type, name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation for Workers
    this.signalCacheInvalidation(ns, 'entity', id)

    // Schedule flush if needed
    await this.maybeScheduleFlush()

    return this.toEntity({
      ns, id, type: $type, name, version: 1,
      created_at: now, created_by: actor,
      updated_at: now, updated_by: actor,
      deleted_at: null, deleted_by: null,
      data: dataJson,
    })
  }

  /**
   * Create multiple entities at once
   *
   * For 5+ entities, bypasses SQLite buffering and writes directly to R2
   * as a pending Parquet file for better performance.
   *
   * @param ns - Namespace for the entities
   * @param items - Array of entity data to create
   * @param options - Create options
   * @returns Array of created entities
   */
  async createMany(
    ns: string,
    items: CreateInput[],
    options: DOCreateOptions = {}
  ): Promise<Entity[]> {
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
   *
   * This bypasses SQLite event buffering for better cost efficiency
   * when creating 5+ entities at once. The pending file will be merged
   * with the main data file during the next flush.
   *
   * @param ns - Namespace
   * @param items - Entity data to write
   * @param options - Create options
   * @returns Created entities
   */
  private async bulkWriteToR2(
    ns: string,
    items: CreateInput[],
    options: DOCreateOptions = {}
  ): Promise<Entity[]> {
    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const pendingId = generateULID()

    // Reserve sequence numbers for all items
    const firstSeq = this.counters.get(ns) || 1
    const lastSeq = firstSeq + items.length - 1
    this.counters.set(ns, lastSeq + 1)

    // Build entities with assigned IDs
    const entities: Entity[] = []
    const rows: Array<{
      $id: string
      data: string
    }> = []

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!
      const seq = firstSeq + i
      const id = sqids.encode([seq])
      const entityId = `${ns}/${id}` as EntityId

      // Extract $type and name, rest goes to data
      const { $type, name, ...rest } = item
      if (!$type) {
        throw new Error('Entity must have $type')
      }
      if (!name) {
        throw new Error('Entity must have name')
      }

      // Build entity object
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

      // Build Parquet row with $id and data columns
      const dataObj = { $type, name, ...rest }
      rows.push({
        $id: entityId,
        data: JSON.stringify(dataObj),
      })

      // EVENT SOURCING: Entity state is derived from events, no need to write to entities table
    }

    // Write pending Parquet file to R2
    const pendingPath = `data/${ns}/pending/${pendingId}.parquet`

    try {
      // Import and use hyparquet-writer for Parquet creation
      const { parquetWriteBuffer } = await import('hyparquet-writer')

      const columnData = [
        { name: '$id', data: rows.map(r => r.$id) },
        { name: 'data', data: rows.map(r => r.data) },
      ]

      const buffer = parquetWriteBuffer({ columnData })
      await this.env.BUCKET.put(pendingPath, buffer)
    } catch (error: unknown) {
      // If Parquet writing fails, fall back to JSON format
      // This ensures bulk operations still work even without hyparquet-writer
      const jsonData = JSON.stringify(rows)
      await this.env.BUCKET.put(pendingPath + '.json', jsonData)
    }

    // Record pending row group metadata (1 SQLite row for the whole batch)
    this.sql.exec(
      `INSERT INTO pending_row_groups (id, ns, path, row_count, first_seq, last_seq, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      pendingId, ns, pendingPath, items.length, firstSeq, lastSeq, now
    )

    // Phase 3 WAL: Append CREATE events so getEntityFromEvents can find them
    for (let i = 0; i < items.length; i++) {
      const item = items[i]!
      const seq = firstSeq + i
      const entityId = sqids.encode([seq])
      const { $type, name, ...rest } = item

      await this.appendEvent({
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
    this.signalCacheInvalidation(ns, 'full')

    await this.maybeScheduleFlush()

    return entities
  }

  /**
   * Get pending row groups for a namespace
   *
   * @param ns - Namespace to query
   * @returns Array of pending row group metadata
   */
  async getPendingRowGroups(ns: string): Promise<Array<{
    id: string
    path: string
    rowCount: number
    firstSeq: number
    lastSeq: number
    createdAt: string
  }>> {
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
   * Delete pending row groups after they've been promoted to committed
   *
   * @param ns - Namespace
   * @param upToSeq - Delete pending groups with last_seq <= this value
   */
  async deletePendingRowGroups(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()

    this.sql.exec(
      `DELETE FROM pending_row_groups WHERE ns = ? AND last_seq <= ?`,
      ns, upToSeq
    )
  }

  /**
   * Promote pending files to committed data file
   *
   * Reads all pending Parquet files for a namespace, merges them with
   * the existing data file, and writes a new committed file.
   *
   * @param ns - Namespace to flush
   * @returns Number of entities promoted
   */
  async flushPendingToCommitted(ns: string): Promise<number> {
    await this.ensureInitialized()

    const pending = await this.getPendingRowGroups(ns)
    if (pending.length === 0) {
      return 0
    }

    // This is a placeholder - actual implementation would:
    // 1. Read existing data/{ns}/data.parquet
    // 2. Read all pending files
    // 3. Merge and write new data.parquet
    // 4. Delete pending files from R2
    // 5. Delete pending_row_groups records

    // For now, just track what would be promoted
    const totalRows = pending.reduce((sum, p) => sum + p.rowCount, 0)
    const maxSeq = Math.max(...pending.map(p => p.lastSeq))

    // Delete the pending records
    await this.deletePendingRowGroups(ns, maxSeq)

    return totalRows
  }

  /**
   * Update an entity by ID
   *
   * @param ns - Namespace
   * @param id - Entity ID (without namespace prefix)
   * @param update - Update operations ($set, $inc, etc.)
   * @param options - Update options
   * @returns Updated entity
   */
  async update(
    ns: string,
    id: string,
    update: UpdateInput,
    options: DOUpdateOptions = {}
  ): Promise<Entity> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // EVENT SOURCING: Get current entity state from events (single source of truth)
    let current: StoredEntity | null = null

    const currentEntity = await this.getEntityFromEvents(ns, id)
    if (currentEntity && !currentEntity.deletedAt) {
      const { $id: _$id, $type, name: entityName, createdAt, createdBy, updatedAt, updatedBy, deletedAt: _deletedAt, deletedBy: _deletedBy, version, ...rest } = currentEntity
      current = {
        ns, id,
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
          ...(update.$set || {}),
        }
        return this.create(ns, createData, { actor })
      }
      throw new Error(`Entity ${ns}/${id} not found`)
    }

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Apply update operators - validate stored data is a valid object
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
        const current = (data[key] as number) || 0
        data[key] = current + (value as number)
      }
    }

    // $push operator
    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const arr = (data[key] as unknown[]) || []
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
        const arr = (data[key] as unknown[]) || []
        data[key] = arr.filter(item => item !== value)
      }
    }

    // $link operator (ParqueDB-specific)
    if (update.$link) {
      const entityIdValue = `${ns}/${id}`
      for (const [predicate, targets] of Object.entries(update.$link)) {
        const targetList = Array.isArray(targets) ? targets : [targets]
        for (const targetId of targetList) {
          await this.link(entityIdValue, predicate, targetId as string, { actor })
        }
      }
    }

    // $unlink operator (ParqueDB-specific)
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

    // EVENT SOURCING: Entity state is derived from events, no need to update entities table

    // Invalidate cache for this entity
    this.invalidateCache(ns, id)

    // Append update event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { ...parseStoredData(current.data), $type: current.type, name: current.name } as Variant,
      after: { ...data, $type: type, name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation for Workers
    this.signalCacheInvalidation(ns, 'entity', id)

    await this.maybeScheduleFlush()

    return this.toEntity({
      ns, id, type, name, version: newVersion,
      created_at: current.created_at, created_by: current.created_by,
      updated_at: now, updated_by: actor,
      deleted_at: null, deleted_by: null,
      data: dataJson,
    })
  }

  /**
   * Delete an entity by ID
   *
   * @param ns - Namespace
   * @param id - Entity ID (without namespace prefix)
   * @param options - Delete options
   * @returns DeleteResult with deletedCount (consistent with local ParqueDB API)
   */
  async delete(
    ns: string,
    id: string,
    options: DODeleteOptions = {}
  ): Promise<DeleteResult> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // EVENT SOURCING: Get current entity state from events (single source of truth)
    let current: StoredEntity | null = null

    const currentEntity = await this.getEntityFromEvents(ns, id)
    if (currentEntity) {
      const { $id: _$id, $type, name: entityName, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy, version, ...rest } = currentEntity
      current = {
        ns, id,
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

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // EVENT SOURCING: Entity state is derived from events, no need to update entities table

    // Handle relationships (still use relationships table)
    if (options.hard) {
      this.sql.exec(
        'DELETE FROM relationships WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)',
        ns, id, ns, id
      )
    } else {
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ?
         WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)`,
        now, actor, ns, id, ns, id
      )
    }

    // Invalidate cache for this entity
    this.invalidateCache(ns, id)

    // Append delete event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'DELETE',
      target: entityTarget(ns, id),
      before: { ...parseStoredData(current.data), $type: current.type, name: current.name } as Variant,
      after: undefined,
      actor: actor as string,
    })

    // Signal cache invalidation for Workers
    this.signalCacheInvalidation(ns, 'entity', id)

    await this.maybeScheduleFlush()

    return { deletedCount: 1 }
  }

  /**
   * Delete multiple entities matching a filter
   *
   * Note: This is a simplified implementation that deletes by IDs.
   * For complex filtering, use QueryExecutor to find matching entities first.
   *
   * @param ns - Namespace
   * @param ids - Array of entity IDs to delete
   * @param options - Delete options
   * @returns DeleteResult with total deletedCount
   */
  async deleteMany(
    ns: string,
    ids: string[],
    options: DODeleteOptions = {}
  ): Promise<DeleteResult> {
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
   * @param ns - Namespace
   * @param id - Entity ID (without namespace prefix)
   * @param options - Restore options
   * @returns Restored entity or null if not found
   */
  async restore(
    ns: string,
    id: string,
    options: { actor?: string | undefined } = {}
  ): Promise<Entity | null> {
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

    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { $type: currentEntity.$type, name: currentEntity.name, deletedAt: currentEntity.deletedAt } as Variant,
      after: { $type: currentEntity.$type, name: currentEntity.name } as Variant,
      actor: actor as string,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(ns, 'entity', id)
    this.invalidateCache(ns, id)

    await this.maybeScheduleFlush()

    return restoredData as Entity
  }

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  /**
   * Create a relationship between two entities
   *
   * @param fromId - Source entity ID (ns/id format)
   * @param predicate - Relationship predicate (e.g., "author", "category")
   * @param toId - Target entity ID (ns/id format)
   * @param options - Link options
   */
  async link(
    fromId: string,
    predicate: string,
    toId: string,
    options: DOLinkOptions = {}
  ): Promise<void> {
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

    // Generate reverse predicate name (simple pluralization for now)
    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    // Check if relationship already exists
    const existing = [...this.sql.exec<StoredRelationship>(
      `SELECT * FROM relationships
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
      fromNs, fromEntityId, predicate, toNs, toEntityId
    )]

    if (existing.length > 0 && existing[0]!.deleted_at === null) {
      // Already exists and not deleted
      return
    }

    const dataJson = options.data ? JSON.stringify(options.data) : null
    const matchMode = options.matchMode ?? null
    const similarity = options.similarity ?? null

    // Validate similarity range
    if (similarity !== null && (similarity < 0 || similarity > 1)) {
      throw new Error(`Similarity must be between 0 and 1, got ${similarity}`)
    }

    // Validate matchMode/similarity consistency
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
        matchMode, similarity, dataJson, fromNs, fromEntityId, predicate, toNs, toEntityId
      )
    } else {
      // Insert new relationship
      this.sql.exec(
        `INSERT INTO relationships
         (from_ns, from_id, predicate, to_ns, to_id, reverse, version, created_at, created_by, match_mode, similarity, data)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
        fromNs, fromEntityId, predicate, toNs, toEntityId, reverse, now, actor, matchMode, similarity, dataJson
      )
    }

    // Phase 4: Append relationship event to rels_wal for batching
    // Include shredded fields in the event for full reconstruction
    await this.appendRelEvent(fromNs, {
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

    // Signal cache invalidation for relationship caches
    this.signalCacheInvalidation(fromNs, 'relationship')
    if (fromNs !== toNs) {
      this.signalCacheInvalidation(toNs, 'relationship')
    }

    await this.maybeScheduleFlush()
  }

  /**
   * Remove a relationship between two entities
   *
   * Phase 4: Relationships are events in the WAL like entities.
   *
   * @param fromId - Source entity ID (ns/id format)
   * @param predicate - Relationship predicate
   * @param toId - Target entity ID (ns/id format)
   * @param options - Unlink options
   */
  async unlink(
    fromId: string,
    predicate: string,
    toId: string,
    options: DOLinkOptions = {}
  ): Promise<void> {
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
      now, actor, fromNs, fromEntityId, predicate, toNs, toEntityId
    )

    // Phase 4: Append relationship event to rels_wal for batching
    await this.appendRelEvent(fromNs, {
      ts: Date.now(),
      op: 'DELETE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: { predicate, to: toId } as Variant,
      after: undefined,
      actor: actor as string,
    })

    // Signal cache invalidation for relationship caches
    this.signalCacheInvalidation(fromNs, 'relationship')
    if (fromNs !== toNs) {
      this.signalCacheInvalidation(toNs, 'relationship')
    }

    await this.maybeScheduleFlush()
  }

  // ===========================================================================
  // Read Operations (for consistency, reads through DO)
  // ===========================================================================

  /**
   * Get an entity by ID from DO storage
   *
   * EVENT SOURCING: Entity state is derived from events (single source of truth).
   * Priority order:
   * 1. Check in-memory entity cache
   * 2. Reconstruct from events
   */
  async get(ns: string, id: string, includeDeleted = false): Promise<Entity | null> {
    await this.ensureInitialized()

    const cacheKey = `${ns}/${id}`

    // 1. Check cache first
    const cached = this.entityCache.get(cacheKey)
    if (cached) {
      const entity = cached.entity
      if (!includeDeleted && entity.deletedAt) {
        return null
      }
      return entity
    }

    // 2. Reconstruct from events (single source of truth)
    const entityFromEvents = await this.getEntityFromEvents(ns, id)
    if (entityFromEvents) {
      if (!includeDeleted && entityFromEvents.deletedAt) {
        return null
      }
      this.cacheEntity(cacheKey, entityFromEvents)
      return entityFromEvents
    }

    // Entity not found in events - does not exist
    return null
  }

  /**
   * Add an entity to the LRU cache
   */
  private cacheEntity(key: string, entity: Entity): void {
    this.entityCache.delete(key)
    this.entityCache.set(key, { entity, version: entity.version })

    if (this.entityCache.size > ParqueDBDO.ENTITY_CACHE_MAX_SIZE) {
      const oldestKey = this.entityCache.keys().next().value
      if (oldestKey) {
        this.entityCache.delete(oldestKey)
      }
    }
  }

  /**
   * Invalidate an entity in the cache
   */
  private invalidateCache(ns: string, id: string): void {
    this.entityCache.delete(`${ns}/${id}`)
  }

  /**
   * Reconstruct entity state from unflushed events
   */
  /**
   * Reconstruct entity state from unflushed events
   *
   * Uses the CONSOLIDATED event buffering system:
   * 1. events_wal table (flushed batches)
   * 2. nsEventBuffers (in-memory, not yet flushed)
   */
  async getEntityFromEvents(ns: string, id: string): Promise<Entity | null> {
    await this.ensureInitialized()

    const target = `${ns}:${id}`
    let entity: Entity | null = null

    // 1. Read from events_wal (flushed batches)
    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const walRows = [...this.sql.exec<WalRow>(
      `SELECT events FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of walRows) {
      const walEvents = this.deserializeEventBatch(row.events)
      for (const event of walEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // 2. Read from in-memory namespace event buffer (not yet flushed)
    const nsBuffer = this.nsEventBuffers.get(ns)
    if (nsBuffer) {
      for (const event of nsBuffer.events) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    return entity
  }

  /**
   * Apply a single event to an entity state
   */
  private applyEventToEntity(
    current: Entity | null,
    event: Event,
    ns: string,
    id: string
  ): Entity | null {
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

  // Helper methods for testing/monitoring

  getCacheStats(): { size: number; maxSize: number } {
    return { size: this.entityCache.size, maxSize: ParqueDBDO.ENTITY_CACHE_MAX_SIZE }
  }

  clearEntityCache(): void {
    this.entityCache.clear()
  }

  isEntityCached(ns: string, id: string): boolean {
    return this.entityCache.has(`${ns}/${id}`)
  }

  // ===========================================================================
  // Cache Invalidation Signaling
  // ===========================================================================

  /**
   * Signal cache invalidation for a namespace
   *
   * Called after write operations to notify Workers that caches are stale.
   * Workers poll getInvalidationVersion() or getPendingInvalidations() to
   * detect when to invalidate their caches.
   *
   * @param ns - Namespace that was modified
   * @param type - Type of invalidation (entity, relationship, or full)
   * @param entityId - Optional entity ID for entity-specific invalidation
   */
  private signalCacheInvalidation(
    ns: string,
    type: 'entity' | 'relationship' | 'full',
    entityId?: string
  ): void {
    // Bump version for this namespace
    const currentVersion = this.invalidationVersions.get(ns) ?? 0
    const newVersion = currentVersion + 1
    this.invalidationVersions.set(ns, newVersion)

    // Create invalidation signal
    const signal: CacheInvalidationSignal = {
      ns,
      type,
      timestamp: Date.now(),
      version: newVersion,
      entityId,
    }

    // Add to pending invalidations (circular buffer)
    this.pendingInvalidations.push(signal)
    if (this.pendingInvalidations.length > ParqueDBDO.MAX_PENDING_INVALIDATIONS) {
      this.pendingInvalidations.shift()
    }
  }

  /**
   * Get the current invalidation version for a namespace
   *
   * Workers can compare this with their cached version to detect stale caches.
   * If the DO version is higher, caches should be invalidated.
   *
   * @param ns - Namespace to check
   * @returns Current invalidation version (0 if never modified)
   */
  getInvalidationVersion(ns: string): number {
    return this.invalidationVersions.get(ns) ?? 0
  }

  /**
   * Get invalidation versions for all namespaces
   *
   * Useful for Workers to batch-check multiple namespaces.
   *
   * @returns Map of namespace to invalidation version
   */
  getAllInvalidationVersions(): Record<string, number> {
    const result: Record<string, number> = {}
    for (const [ns, version] of this.invalidationVersions) {
      result[ns] = version
    }
    return result
  }

  /**
   * Get pending invalidation signals since a given version
   *
   * Workers can poll this to get detailed invalidation information.
   * Useful for surgical cache invalidation rather than full namespace purge.
   *
   * @param ns - Namespace to get signals for (or undefined for all)
   * @param sinceVersion - Only return signals with version > this value
   * @returns Array of invalidation signals
   */
  getPendingInvalidations(ns?: string, sinceVersion?: number): CacheInvalidationSignal[] {
    let signals = this.pendingInvalidations

    if (ns) {
      signals = signals.filter(s => s.ns === ns)
    }

    if (sinceVersion !== undefined) {
      signals = signals.filter(s => s.version > sinceVersion)
    }

    return signals
  }

  /**
   * Check if caches for a namespace are stale
   *
   * Convenience method for Workers to quickly check if invalidation is needed.
   *
   * @param ns - Namespace to check
   * @param workerVersion - Worker's cached version for this namespace
   * @returns true if Worker should invalidate its caches
   */
  shouldInvalidate(ns: string, workerVersion: number): boolean {
    const doVersion = this.invalidationVersions.get(ns) ?? 0
    return doVersion > workerVersion
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

    if (rows.length === 0) {
      return []
    }

    // Collect unique entity keys to look up
    const entityKeys = new Set<string>()
    for (const row of rows) {
      entityKeys.add(`${row.from_ns}/${row.from_id}`)
      entityKeys.add(`${row.to_ns}/${row.to_id}`)
    }

    // Batch lookup entity type/name info
    const entityInfoMap = await this.getEntityInfoBatch([...entityKeys])

    return rows.map(row => this.toRelationship(row, entityInfoMap))
  }

  /**
   * Batch lookup entity type and name for multiple entities
   *
   * Since this DO uses event sourcing, entities are reconstructed from events.
   * We use the existing get() method which handles event reconstruction.
   *
   * @param entityKeys - Array of entity keys in "ns/id" format
   * @returns Map of entity key to type/name info
   */
  private async getEntityInfoBatch(
    entityKeys: string[]
  ): Promise<Map<string, { type: string; name: string }>> {
    const result = new Map<string, { type: string; name: string }>()

    if (entityKeys.length === 0) {
      return result
    }

    // Look up each entity using the event-sourcing aware get() method
    // This correctly reconstructs entity state from events
    await Promise.all(
      entityKeys.map(async key => {
        const slashIndex = key.indexOf('/')
        const ns = key.substring(0, slashIndex)
        const id = key.substring(slashIndex + 1)

        const entity = await this.get(ns, id)
        if (entity) {
          result.set(key, { type: entity.$type, name: entity.name })
        }
      })
    )

    return result
  }

  // ===========================================================================
  // Event Log with WAL Batching
  // ===========================================================================

  /**
   * Append an event to the log
   *
   * Events are buffered in memory and flushed as batches to reduce SQLite row costs.
   * Each batch is stored as a single row with events serialized as a blob.
   */
  /**
   * Append an event to the log
   *
   * Events are buffered in memory per-namespace and flushed as batches to events_wal
   * to reduce SQLite row costs. The namespace is extracted from event.target.
   *
   * This is the CONSOLIDATED event buffering system - all entity events go through here.
   */
  async appendEvent(event: Event): Promise<void> {
    await this.ensureInitialized()

    // Extract namespace from event.target (format: "ns:id")
    const ns = event.target.split(':')[0]!

    // Get or create buffer for this namespace
    let buffer = this.nsEventBuffers.get(ns)
    if (!buffer) {
      // Initialize with current counter
      const seq = this.counters.get(ns) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.nsEventBuffers.set(ns, buffer)
    }

    // Add event to buffer
    buffer.events.push(event)
    buffer.lastSeq++
    this.counters.set(ns, buffer.lastSeq)

    // Estimate size
    const eventJson = JSON.stringify(event)
    buffer.sizeBytes += eventJson.length

    // Check if we should flush
    if (buffer.events.length >= EVENT_BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushNsEventBatch(ns)
    }
  }

  /**
   * Append an event with namespace-based sequence tracking
   * Uses Sqids-based counter for short IDs instead of ULIDs
   *
   * @deprecated Use appendEvent() directly - it now routes to the namespace-based system
   * @param ns - Namespace for the event
   * @param event - Event without ID (ID will be generated)
   * @returns Event ID generated with Sqids
   */
  async appendEventWithSeq(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    await this.ensureInitialized()

    // Get or create buffer for this namespace
    let buffer = this.nsEventBuffers.get(ns)
    if (!buffer) {
      // Initialize with current counter
      const seq = this.counters.get(ns) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.nsEventBuffers.set(ns, buffer)
    }

    // Generate event ID using Sqids with current sequence
    const eventId = sqids.encode([buffer.lastSeq])
    const fullEvent: Event = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++
    this.counters.set(ns, buffer.lastSeq)

    // Estimate size
    const eventJson = JSON.stringify(fullEvent)
    buffer.sizeBytes += eventJson.length

    // Check if we should flush
    if (buffer.events.length >= EVENT_BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushNsEventBatch(ns)
    }

    return eventId
  }

  /**
   * Flush buffered events for a specific namespace to events_wal
   */
  async flushNsEventBatch(ns: string): Promise<void> {
    const buffer = this.nsEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    await this.ensureInitialized()

    // Serialize events to blob
    const json = JSON.stringify(buffer.events)
    const data = new TextEncoder().encode(json)
    const now = new Date().toISOString()

    this.sql.exec(
      `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      ns,
      buffer.firstSeq,
      buffer.lastSeq - 1, // last_seq is inclusive
      data,
      now
    )

    // Reset buffer for next batch
    this.nsEventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  /**
   * Flush all namespace event buffers to events_wal
   */
  async flushAllNsEventBatches(): Promise<void> {
    for (const ns of this.nsEventBuffers.keys()) {
      await this.flushNsEventBatch(ns)
    }
  }

  /**
   * Get unflushed event count (from events_wal + buffers)
   */
  async getUnflushedEventCount(): Promise<number> {
    // Delegate to the consolidated WAL-based counter
    return this.getTotalUnflushedWalEventCount()
  }

  /**
   * Get unflushed WAL event count for a specific namespace
   */
  async getUnflushedWalEventCount(ns: string): Promise<number> {
    await this.ensureInitialized()

    // Count events in WAL batches
    const rows = [...this.sql.exec<{ total: number }>(
      `SELECT SUM(last_seq - first_seq + 1) as total FROM events_wal WHERE ns = ?`,
      ns
    )]

    const walCount = rows[0]?.total || 0

    // Add buffered events not yet written
    const buffer = this.nsEventBuffers.get(ns)
    const bufferCount = buffer?.events.length || 0

    return walCount + bufferCount
  }

  /**
   * Get total unflushed WAL event count across all namespaces
   */
  async getTotalUnflushedWalEventCount(): Promise<number> {
    await this.ensureInitialized()

    // Count events in WAL batches
    const rows = [...this.sql.exec<{ total: number }>(
      `SELECT SUM(last_seq - first_seq + 1) as total FROM events_wal`
    )]

    let total = rows[0]?.total || 0

    // Add all buffered events
    for (const buffer of this.nsEventBuffers.values()) {
      total += buffer.events.length
    }

    return total
  }

  /**
   * Get unflushed WAL batch count
   */
  async getUnflushedWalBatchCount(): Promise<number> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM events_wal'
    )]

    return rows[0]?.count || 0
  }


  /**
   * Read all unflushed WAL events for a namespace
   */
  async readUnflushedWalEvents(ns: string): Promise<Event[]> {
    await this.ensureInitialized()

    const allEvents: Event[] = []

    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq
       FROM events_wal
       WHERE ns = ?
       ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of rows) {
      const batchEvents = this.deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    const buffer = this.nsEventBuffers.get(ns)
    if (buffer) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  /**
   * Read all unflushed events (from batches + buffer)
   */
  /**
   * Read all unflushed events (from events_wal + buffers)
   *
   * Uses the CONSOLIDATED event buffering system.
   */
  async readUnflushedEvents(): Promise<Event[]> {
    await this.ensureInitialized()

    const allEvents: Event[] = []

    // Read from events_wal (all namespaces)
    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT events FROM events_wal ORDER BY id ASC`
    )]

    for (const row of rows) {
      const batchEvents = this.deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add events from all namespace buffers
    for (const buffer of this.nsEventBuffers.values()) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  /**
   * Delete WAL batches for a namespace (after archiving to R2)
   */
  async deleteWalBatches(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()

    this.sql.exec(
      `DELETE FROM events_wal WHERE ns = ? AND last_seq <= ?`,
      ns,
      upToSeq
    )
  }

  /**
   * Get current sequence counter for a namespace
   */
  getSequenceCounter(ns: string): number {
    return this.counters.get(ns) || 1
  }

  /**
   * Get buffer state for a namespace (for testing)
   */
  getNsBufferState(ns: string): { eventCount: number; firstSeq: number; lastSeq: number; sizeBytes: number } | null {
    const buffer = this.nsEventBuffers.get(ns)
    if (!buffer) return null
    return {
      eventCount: buffer.events.length,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    }
  }

  // ===========================================================================
  // Relationship Event Batching (Phase 4)
  // ===========================================================================

  // Note: Relationship sequence generation is handled inline in appendRelEvent
  // This keeps the sequence counter management close to its usage

  /**
   * Append a relationship event with namespace-based batching
   */
  async appendRelEvent(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    await this.ensureInitialized()

    // Get or create buffer for this namespace
    let buffer = this.relEventBuffers.get(ns)
    if (!buffer) {
      const relCounterKey = `rel:${ns}`
      const seq = this.counters.get(relCounterKey) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.relEventBuffers.set(ns, buffer)
    }

    // Generate event ID using sequence
    const eventId = `rel_${buffer.lastSeq}`
    const fullEvent: Event = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++
    this.counters.set(`rel:${ns}`, buffer.lastSeq)

    // Estimate size
    const eventJson = JSON.stringify(fullEvent)
    buffer.sizeBytes += eventJson.length

    // Check if we should flush
    if (
      buffer.events.length >= EVENT_BATCH_COUNT_THRESHOLD ||
      buffer.sizeBytes >= EVENT_BATCH_SIZE_THRESHOLD
    ) {
      await this.flushRelEventBatch(ns)
    }

    return eventId
  }

  /**
   * Flush buffered relationship events for a namespace to rels_wal
   */
  async flushRelEventBatch(ns: string): Promise<void> {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    await this.ensureInitialized()

    // Serialize events to blob
    const json = JSON.stringify(buffer.events)
    const data = new TextEncoder().encode(json)
    const now = new Date().toISOString()

    this.sql.exec(
      `INSERT INTO rels_wal (ns, first_seq, last_seq, events, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      ns,
      buffer.firstSeq,
      buffer.lastSeq - 1, // last_seq is inclusive
      data,
      now
    )

    // Reset buffer for next batch
    this.relEventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  /**
   * Flush all namespace relationship event buffers
   */
  async flushAllRelEventBatches(): Promise<void> {
    for (const ns of this.relEventBuffers.keys()) {
      await this.flushRelEventBatch(ns)
    }
  }

  /**
   * Get unflushed WAL relationship event count for a namespace
   */
  async getUnflushedRelEventCount(ns: string): Promise<number> {
    await this.ensureInitialized()

    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT events FROM rels_wal WHERE ns = ?`,
      ns
    )]

    let count = 0
    for (const row of rows) {
      const events = this.deserializeEventBatch(row.events)
      count += events.length
    }

    // Add buffered events not yet written
    const buffer = this.relEventBuffers.get(ns)
    count += buffer?.events.length || 0

    return count
  }

  /**
   * Get total unflushed WAL relationship event count across all namespaces
   */
  async getTotalUnflushedRelEventCount(): Promise<number> {
    await this.ensureInitialized()

    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(`SELECT events FROM rels_wal`)]

    let total = 0
    for (const row of rows) {
      const events = this.deserializeEventBatch(row.events)
      total += events.length
    }

    // Add all buffered events
    for (const buffer of this.relEventBuffers.values()) {
      total += buffer.events.length
    }

    return total
  }

  /**
   * Get unflushed WAL batch count for relationships
   */
  async getUnflushedRelBatchCount(ns?: string): Promise<number> {
    await this.ensureInitialized()

    let query = 'SELECT COUNT(*) as count FROM rels_wal'
    const params: unknown[] = []

    if (ns) {
      query += ' WHERE ns = ?'
      params.push(ns)
    }

    const rows = [...this.sql.exec<{ count: number }>(query, ...params)]
    return rows[0]?.count || 0
  }

  /**
   * Read all unflushed relationship WAL events for a namespace
   */
  async readUnflushedRelEvents(ns: string): Promise<Event[]> {
    await this.ensureInitialized()

    const allEvents: Event[] = []

    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq FROM rels_wal WHERE ns = ? ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of rows) {
      const batchEvents = this.deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    const buffer = this.relEventBuffers.get(ns)
    if (buffer) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  /**
   * Delete relationship WAL batches for a namespace (after archiving to R2)
   */
  async deleteRelWalBatches(ns: string, upToSeq: number): Promise<void> {
    await this.ensureInitialized()

    this.sql.exec(
      `DELETE FROM rels_wal WHERE ns = ? AND last_seq <= ?`,
      ns,
      upToSeq
    )
  }

  /**
   * Get relationship sequence counter for a namespace
   */
  getRelSequenceCounter(ns: string): number {
    const relCounterKey = `rel:${ns}`
    return this.counters.get(relCounterKey) || 1
  }

  /**
   * Get relationship buffer state for a namespace (for testing)
   */
  getRelBufferState(ns: string): {
    eventCount: number
    firstSeq: number
    lastSeq: number
    sizeBytes: number
  } | null {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer) return null
    return {
      eventCount: buffer.events.length,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    }
  }

  /**
   * Deserialize a batch blob back to events
   */
  private deserializeEventBatch(batch: Uint8Array | ArrayBuffer): Event[] {
    if (!batch) return []

    let data: Uint8Array
    if (batch instanceof Uint8Array) {
      data = batch
    } else if (batch instanceof ArrayBuffer) {
      data = new Uint8Array(batch)
    } else if (ArrayBuffer.isView(batch)) {
      data = new Uint8Array((batch as ArrayBufferView).buffer)
    } else {
      // Assume it's already a buffer-like object
      data = new Uint8Array(batch as ArrayBuffer)
    }

    const json = new TextDecoder().decode(data)
    try {
      return JSON.parse(json) as Event[]
    } catch {
      // Invalid JSON in event batch - return empty array
      return []
    }
  }

  /**
   * Flush namespace-specific events from events_wal to Parquet files in R2
   *
   * Writes namespace-specific event files to data/{ns}/events/ for
   * efficient namespace-scoped queries.
   */
  async flushNsWalToParquet(): Promise<void> {
    await this.ensureInitialized()

    // First flush any buffered namespace events to events_wal
    await this.flushAllNsEventBatches()

    // Get all namespaces with unflushed events
    interface NsRow extends Record<string, SqlStorageValue> {
      ns: string
    }

    const namespaces = [...this.sql.exec<NsRow>(
      `SELECT DISTINCT ns FROM events_wal`
    )]

    for (const { ns } of namespaces) {
      await this.flushNsWalToParquetForNamespace(ns)
    }
  }

  /**
   * Flush events_wal for a specific namespace to Parquet
   */
  private async flushNsWalToParquetForNamespace(ns: string): Promise<void> {
    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const walRows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq
       FROM events_wal
       WHERE ns = ?
       ORDER BY first_seq ASC`,
      ns
    )]

    if (walRows.length === 0) return

    // Collect all events
    const allEvents: Event[] = []
    const walIds: number[] = []
    let minSeq = Infinity
    let maxSeq = -Infinity

    for (const row of walRows) {
      const events = this.deserializeEventBatch(row.events)
      allEvents.push(...events)
      walIds.push(row.id)
      minSeq = Math.min(minSeq, row.first_seq)
      maxSeq = Math.max(maxSeq, row.last_seq)

      // Limit batch size
      if (allEvents.length >= this.flushConfig.maxEvents) break
    }

    if (allEvents.length < this.flushConfig.minEvents) {
      // Not enough events to flush
      return
    }

    const checkpointId = generateULID()

    // Generate namespace-specific parquet path
    // Format: data/{ns}/events/{checkpointId}.parquet
    const parquetPath = `data/${ns}/events/${checkpointId}.parquet`

    // Convert events to columnar format for Parquet
    // Include ns and entity_id as separate columns for efficient filtering
    const columnData = [
      { name: 'id', data: allEvents.map(e => e.id) },
      { name: 'ts', data: allEvents.map(e => e.ts) },
      { name: 'op', data: allEvents.map(e => e.op) },
      { name: 'target', data: allEvents.map(e => e.target) },
      { name: 'ns', data: allEvents.map(() => ns) },
      { name: 'entity_id', data: allEvents.map(e => {
        // Extract entity_id from target (format: "ns:id" or "ns:id:predicate:to_ns:to_id")
        const colonIdx = e.target.indexOf(':')
        return colonIdx !== -1 ? e.target.slice(colonIdx + 1).split(':')[0] : null
      }) },
      { name: 'before', data: allEvents.map(e => e.before ? JSON.stringify(e.before) : null) },
      { name: 'after', data: allEvents.map(e => e.after ? JSON.stringify(e.after) : null) },
      { name: 'actor', data: allEvents.map(e => e.actor ?? null) },
      { name: 'metadata', data: allEvents.map(e => e.metadata ? JSON.stringify(e.metadata) : null) },
    ]

    // Write Parquet file to R2 using hyparquet-writer
    try {
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const parquetBuffer = parquetWriteBuffer({ columnData })
      await this.env.BUCKET.put(parquetPath, parquetBuffer)
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      throw new Error(`Failed to write namespace events Parquet file to R2: ${message}`)
    }

    // Delete flushed WAL entries
    if (walIds.length > 0) {
      const placeholders = walIds.map(() => '?').join(',')
      this.sql.exec(
        `DELETE FROM events_wal WHERE id IN (${placeholders})`,
        ...walIds
      )
    }

    // Record checkpoint for namespace events
    this.sql.exec(
      `INSERT INTO checkpoints (id, created_at, event_count, first_event_id, last_event_id, parquet_path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      checkpointId,
      new Date().toISOString(),
      allEvents.length,
      allEvents[0]!.id,
      allEvents[allEvents.length - 1]!.id,
      parquetPath
    )
  }

  /**
   * Flush relationship events from rels_wal to Parquet files in R2
   *
   * Similar to flushNsWalToParquet but for relationship events.
   * Writes to rels/{ns}/events/{checkpointId}.parquet
   */
  async flushRelWalToParquet(): Promise<void> {
    await this.ensureInitialized()

    // First flush any buffered relationship events to rels_wal
    await this.flushAllRelEventBatches()

    // Get all namespaces with unflushed relationship events
    interface NsRow extends Record<string, SqlStorageValue> {
      ns: string
    }

    const namespaces = [...this.sql.exec<NsRow>(
      `SELECT DISTINCT ns FROM rels_wal`
    )]

    for (const { ns } of namespaces) {
      await this.flushRelWalToParquetForNamespace(ns)
    }
  }

  /**
   * Flush rels_wal for a specific namespace to Parquet
   */
  private async flushRelWalToParquetForNamespace(ns: string): Promise<void> {
    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const walRows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq
       FROM rels_wal
       WHERE ns = ?
       ORDER BY first_seq ASC`,
      ns
    )]

    if (walRows.length === 0) return

    // Collect all events
    const allEvents: Event[] = []
    const walIds: number[] = []

    for (const row of walRows) {
      const events = this.deserializeEventBatch(row.events)
      allEvents.push(...events)
      walIds.push(row.id)

      // Limit batch size
      if (allEvents.length >= this.flushConfig.maxEvents) break
    }

    if (allEvents.length < this.flushConfig.minEvents) {
      // Not enough events to flush
      return
    }

    const checkpointId = generateULID()

    // Generate relationship-specific parquet path
    // Format: rels/{ns}/events/{checkpointId}.parquet
    const parquetPath = `rels/${ns}/events/${checkpointId}.parquet`

    // Convert events to columnar format for Parquet
    const columnData = [
      { name: 'id', data: allEvents.map(e => e.id) },
      { name: 'ts', data: allEvents.map(e => e.ts) },
      { name: 'op', data: allEvents.map(e => e.op) },
      { name: 'target', data: allEvents.map(e => e.target) },
      { name: 'ns', data: allEvents.map(() => ns) },
      { name: 'before', data: allEvents.map(e => e.before ? JSON.stringify(e.before) : null) },
      { name: 'after', data: allEvents.map(e => e.after ? JSON.stringify(e.after) : null) },
      { name: 'actor', data: allEvents.map(e => e.actor ?? null) },
      { name: 'metadata', data: allEvents.map(e => e.metadata ? JSON.stringify(e.metadata) : null) },
    ]

    // Write Parquet file to R2 using hyparquet-writer
    try {
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const parquetBuffer = parquetWriteBuffer({ columnData })
      await this.env.BUCKET.put(parquetPath, parquetBuffer)
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      throw new Error(`Failed to write relationship events Parquet file to R2: ${message}`)
    }

    // Delete flushed WAL entries
    if (walIds.length > 0) {
      const placeholders = walIds.map(() => '?').join(',')
      this.sql.exec(
        `DELETE FROM rels_wal WHERE id IN (${placeholders})`,
        ...walIds
      )
    }

    // Record checkpoint for relationship events
    this.sql.exec(
      `INSERT INTO checkpoints (id, created_at, event_count, first_event_id, last_event_id, parquet_path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      checkpointId,
      new Date().toISOString(),
      allEvents.length,
      allEvents[0]!.id,
      allEvents[allEvents.length - 1]!.id,
      parquetPath
    )
  }

  /**
   * Flush events to Parquet and store in R2
   *
   * This flushes:
   * 1. Namespace events_wal to data/{ns}/events/ (namespace-specific)
   * 2. Relationship rels_wal to rels/{ns}/events/ (relationship-specific)
   */
  async flushToParquet(): Promise<void> {
    await this.ensureInitialized()

    // First flush any buffered events to their respective tables
    await this.flushAllNsEventBatches()
    await this.flushAllRelEventBatches()

    // Flush namespace-specific events from events_wal
    await this.flushNsWalToParquet()

    // Flush relationship events from rels_wal
    await this.flushRelWalToParquet()
  }

  // ===========================================================================
  // Alarm Handler (for scheduled flush)
  // ===========================================================================

  /**
   * Handle scheduled alarm for flushing events
   */
  override async alarm(): Promise<void> {
    this.flushAlarmSet = false

    // Flush all buffered events to Parquet/R2
    await this.flushToParquet()
  }

  /**
   * Schedule a flush if conditions are met
   */
  private async maybeScheduleFlush(): Promise<void> {
    if (this.flushAlarmSet) return

    const count = await this.getUnflushedEventCount()

    if (count >= this.flushConfig.maxEvents) {
      // Flush immediately if we hit max events
      await this.flushToParquet()
    } else if (count >= this.flushConfig.minEvents && !this.flushAlarmSet) {
      // Schedule flush after interval
      await this.ctx.storage.setAlarm(Date.now() + this.flushConfig.maxInterval)
      this.flushAlarmSet = true
    }
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Convert stored entity to API entity format
   */
  private toEntity(stored: StoredEntity): Entity {
    const data = parseStoredData(stored.data)

    return {
      $id: `${stored.ns}/${stored.id}` as EntityId,
      $type: stored.type,
      name: stored.name,
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by as EntityId,
      updatedAt: new Date(stored.updated_at),
      updatedBy: stored.updated_by as EntityId,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by as EntityId | undefined,
      version: stored.version,
      ...data,
    } as Entity
  }

  /**
   * Convert stored relationship to API relationship format
   *
   * @param stored - The stored relationship from SQLite
   * @param entityInfoMap - Optional map of entity keys (ns/id) to type/name info
   */
  private toRelationship(
    stored: StoredRelationship,
    entityInfoMap?: Map<string, { type: string; name: string }>
  ): Relationship {
    const fromKey = `${stored.from_ns}/${stored.from_id}`
    const toKey = `${stored.to_ns}/${stored.to_id}`
    const fromInfo = entityInfoMap?.get(fromKey)
    const toInfo = entityInfoMap?.get(toKey)

    return {
      fromNs: stored.from_ns as Namespace,
      fromId: stored.from_id as Id,
      fromType: fromInfo?.type ?? '',
      fromName: fromInfo?.name ?? '',
      predicate: stored.predicate,
      reverse: stored.reverse,
      toNs: stored.to_ns as Namespace,
      toId: stored.to_id as Id,
      toType: toInfo?.type ?? '',
      toName: toInfo?.name ?? '',
      // Shredded fields
      matchMode: stored.match_mode as Relationship['matchMode'],
      similarity: stored.similarity ?? undefined,
      // Audit fields
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by as EntityId,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by as EntityId | undefined,
      version: stored.version,
      data: stored.data ? parseStoredData(stored.data) : undefined,
    }
  }

  // ===========================================================================
  // Transaction Management
  // ===========================================================================

  /** Transaction snapshot for rollback */
  private transactionSnapshot: TransactionSnapshot | null = null

  /** Whether currently in a transaction */
  private inTransaction = false

  /**
   * Begin a transaction
   *
   * Captures a snapshot of all mutable state that can be restored on rollback.
   * This includes:
   * - Namespace sequence counters
   * - Entity cache
   * - Namespace event buffers
   * - Relationship event buffers
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
      nsEventBuffers: new Map(),
      relEventBuffers: new Map(),
      sqlRollbackOps: [],
      pendingR2Paths: [],
    }

    // Deep copy entity cache entries
    for (const [key, value] of this.entityCache) {
      this.transactionSnapshot.entityCache.set(key, {
        entity: JSON.parse(JSON.stringify(value.entity)),
        version: value.version,
      })
    }

    // Deep copy namespace event buffers
    for (const [ns, buffer] of this.nsEventBuffers) {
      this.transactionSnapshot.nsEventBuffers.set(ns, {
        events: [...buffer.events],
        firstSeq: buffer.firstSeq,
        lastSeq: buffer.lastSeq,
        sizeBytes: buffer.sizeBytes,
      })
    }

    // Deep copy relationship event buffers
    for (const [ns, buffer] of this.relEventBuffers) {
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
    type: 'entity_insert' | 'entity_update' | 'entity_delete' | 'rel_insert' | 'rel_update' | 'rel_delete' | 'pending_row_group',
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

    // 1. Restore in-memory state
    this.counters.clear()
    for (const [k, v] of snapshot.counters) {
      this.counters.set(k, v)
    }

    this.entityCache.clear()
    for (const [k, v] of snapshot.entityCache) {
      this.entityCache.set(k, v)
    }

    this.nsEventBuffers.clear()
    for (const [ns, buffer] of snapshot.nsEventBuffers) {
      this.nsEventBuffers.set(ns, { ...buffer, events: [...buffer.events] })
    }

    this.relEventBuffers.clear()
    for (const [ns, buffer] of snapshot.relEventBuffers) {
      this.relEventBuffers.set(ns, { ...buffer, events: [...buffer.events] })
    }

    // 2. Reverse SQL operations (in reverse order)
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
              before.type, before.name, before.version, before.created_at, before.created_by,
              before.updated_at, before.updated_by, before.deleted_at, before.deleted_by, before.data,
              op.ns, op.id
            )
          }
          break
        case 'entity_delete':
          if (op.beforeState) {
            const before = op.beforeState as StoredEntity
            this.sql.exec(
              `INSERT OR REPLACE INTO entities (ns, id, type, name, version, created_at, created_by,
               updated_at, updated_by, deleted_at, deleted_by, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              op.ns, op.id, before.type, before.name, before.version, before.created_at,
              before.created_by, before.updated_at, before.updated_by, before.deleted_at,
              before.deleted_by, before.data
            )
          }
          break
        case 'rel_insert':
          this.sql.exec(
            'DELETE FROM relationships WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?',
            op.ns, op.id, op.predicate, op.toNs, op.toId
          )
          break
        case 'rel_update':
          if (op.beforeState) {
            const before = op.beforeState as StoredRelationship
            this.sql.exec(
              `UPDATE relationships SET version = ?, deleted_at = ?, deleted_by = ?, data = ?
               WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
              before.version, before.deleted_at, before.deleted_by, before.data,
              op.ns, op.id, op.predicate, op.toNs, op.toId
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
              op.ns, op.id, before.predicate, before.to_ns, before.to_id, before.reverse,
              before.version, before.created_at, before.created_by, before.deleted_at,
              before.deleted_by, before.data
            )
          }
          break
        case 'pending_row_group':
          this.sql.exec('DELETE FROM pending_row_groups WHERE id = ?', op.id)
          break
      }
    }

    // 3. Delete R2 files written during transaction
    for (const path of snapshot.pendingR2Paths) {
      try {
        await this.env.BUCKET.delete(path)
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

/**
 * Transaction snapshot interface
 */
interface TransactionSnapshot {
  counters: Map<string, number>
  entityCache: Map<string, { entity: Entity; version: number }>
  /** Consolidated namespace event buffers (single source of truth for entity events) */
  nsEventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
  /** Relationship event buffers (separate from entity events) */
  relEventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
  sqlRollbackOps: Array<{
    type: 'entity_insert' | 'entity_update' | 'entity_delete' | 'rel_insert' | 'rel_update' | 'rel_delete' | 'pending_row_group'
    ns: string
    id: string
    predicate?: string | undefined
    toNs?: string | undefined
    toId?: string | undefined
    beforeState?: StoredEntity | StoredRelationship | null | undefined
  }>
  pendingR2Paths: string[]
}

/**
 * ParqueDB Durable Object
 *
 * Handles all write operations for consistency. Reads can bypass the DO
 * and go directly to R2 for better performance.
 *
 * Architecture:
 * - SQLite for entity metadata and indexes (fast lookups)
 * - Event log accumulates changes before flushing to Parquet
 * - Periodic flush to R2 as Parquet files
 */

import { DurableObject } from 'cloudflare:workers'
import type {
  Entity,
  EntityId,
  EntityRecord,
  Event,
  EventOp,
  Namespace,
  Id,
  Relationship,
  UpdateInput,
  CreateInput,
  Variant,
} from '../types'
import { entityTarget, relTarget, parseEntityTarget, isRelationshipTarget } from '../types'
import type { Env, FlushConfig, DEFAULT_FLUSH_CONFIG, DO_SQLITE_SCHEMA } from '../types/worker'
import { getRandom48Bit } from '../utils'

// =============================================================================
// ULID Generation (simplified, for event IDs)
// =============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
let lastTime = 0
let lastRandom = 0

function generateULID(): string {
  let now = Date.now()
  if (now === lastTime) {
    lastRandom++
  } else {
    lastTime = now
    // Use cryptographically secure random for ULID random component
    lastRandom = getRandom48Bit()
  }

  let time = ''
  for (let i = 0; i < 10; i++) {
    time = ENCODING[now % 32] + time
    now = Math.floor(now / 32)
  }

  let random = ''
  let r = lastRandom
  for (let i = 0; i < 16; i++) {
    random = ENCODING[r % 32] + random
    r = Math.floor(r / 32)
  }

  return time + random
}

// =============================================================================
// Types
// =============================================================================

/** Options for create operation */
export interface DOCreateOptions {
  /** Actor performing the operation */
  actor?: string
  /** Skip validation */
  skipValidation?: boolean
}

/** Options for update operation */
export interface DOUpdateOptions {
  /** Actor performing the operation */
  actor?: string
  /** Expected version for optimistic concurrency */
  expectedVersion?: number
  /** Create if not exists */
  upsert?: boolean
}

/** Options for delete operation */
export interface DODeleteOptions {
  /** Actor performing the operation */
  actor?: string
  /** Hard delete (permanent) */
  hard?: boolean
  /** Expected version for optimistic concurrency */
  expectedVersion?: number
}

/** Options for link operation */
export interface DOLinkOptions {
  /** Actor performing the operation */
  actor?: string
  /** Edge data */
  data?: Record<string, unknown>
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
  data: string | null
}

/** Event as stored in SQLite */
interface StoredEvent {
  [key: string]: SqlStorageValue
  id: string
  ts: string
  target: string
  op: string
  ns: string
  entity_id: string
  before: string | null
  after: string | null
  actor: string
  metadata: string | null
  flushed: number
}

// =============================================================================
// ParqueDBDO Class
// =============================================================================

/**
 * Durable Object for ParqueDB write operations
 *
 * All write operations go through this DO to ensure consistency.
 * The DO maintains:
 * - Entity metadata in SQLite for fast lookups
 * - Relationship graph in SQLite
 * - Event log that gets periodically flushed to Parquet
 */
export class ParqueDBDO extends DurableObject<Env> {
  /** SQLite storage */
  private sql: SqlStorage

  /** Whether schema has been initialized */
  private initialized = false

  /** Flush configuration */
  private flushConfig: FlushConfig = {
    minEvents: 100,
    maxInterval: 60000,
    maxEvents: 10000,
    rowGroupSize: 1000,
  }

  /** Pending flush alarm */
  private flushAlarmSet = false

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

    // Create tables
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        ns TEXT NOT NULL,
        id TEXT NOT NULL,
        type TEXT NOT NULL,
        name TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        updated_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        data TEXT NOT NULL,
        PRIMARY KEY (ns, id)
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS relationships (
        from_ns TEXT NOT NULL,
        from_id TEXT NOT NULL,
        predicate TEXT NOT NULL,
        to_ns TEXT NOT NULL,
        to_id TEXT NOT NULL,
        reverse TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        deleted_at TEXT,
        deleted_by TEXT,
        data TEXT,
        PRIMARY KEY (from_ns, from_id, predicate, to_ns, to_id)
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        target TEXT NOT NULL,
        op TEXT NOT NULL,
        ns TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        before TEXT,
        after TEXT,
        actor TEXT NOT NULL,
        metadata TEXT,
        flushed INTEGER NOT NULL DEFAULT 0
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        event_count INTEGER NOT NULL,
        first_event_id TEXT NOT NULL,
        last_event_id TEXT NOT NULL,
        parquet_path TEXT NOT NULL
      )
    `)

    // Create indexes
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)')

    this.initialized = true
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
    const id = generateULID().toLowerCase()
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

    // Insert entity
    this.sql.exec(
      `INSERT INTO entities (ns, id, type, name, version, created_at, created_by, updated_at, updated_by, data)
       VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
      ns, id, $type, name, now, actor, now, actor, dataJson
    )

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

    // Get current entity
    const rows = [...this.sql.exec<StoredEntity>(
      'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL',
      ns, id
    )]

    if (rows.length === 0) {
      if (options.upsert) {
        // Create the entity if upsert is enabled
        const createData: CreateInput = {
          $type: 'Unknown',
          name: id,
          ...(update.$set || {}),
        }
        return this.create(ns, createData, { actor })
      }
      throw new Error(`Entity ${ns}/${id} not found`)
    }

    const current = rows[0]

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Apply update operators
    let data = JSON.parse(current.data) as Record<string, unknown>
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

    // Update entity
    this.sql.exec(
      `UPDATE entities
       SET type = ?, name = ?, version = ?, updated_at = ?, updated_by = ?, data = ?
       WHERE ns = ? AND id = ?`,
      type, name, newVersion, now, actor, dataJson, ns, id
    )

    // Append update event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: entityTarget(ns, id),
      before: { ...JSON.parse(current.data), $type: current.type, name: current.name } as Variant,
      after: { ...data, $type: type, name } as Variant,
      actor: actor as string,
    })

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
   * @returns True if deleted
   */
  async delete(
    ns: string,
    id: string,
    options: DODeleteOptions = {}
  ): Promise<boolean> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Get current entity
    const rows = [...this.sql.exec<StoredEntity>(
      'SELECT * FROM entities WHERE ns = ? AND id = ?',
      ns, id
    )]

    if (rows.length === 0) {
      return false
    }

    const current = rows[0]

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    if (options.hard) {
      // Hard delete - remove from database
      this.sql.exec('DELETE FROM entities WHERE ns = ? AND id = ?', ns, id)
      // Also delete relationships
      this.sql.exec(
        'DELETE FROM relationships WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)',
        ns, id, ns, id
      )
    } else {
      // Soft delete - set deleted_at
      this.sql.exec(
        `UPDATE entities SET deleted_at = ?, deleted_by = ?, version = version + 1 WHERE ns = ? AND id = ?`,
        now, actor, ns, id
      )
      // Soft delete relationships
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ?
         WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)`,
        now, actor, ns, id, ns, id
      )
    }

    // Append delete event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'DELETE',
      target: entityTarget(ns, id),
      before: { ...JSON.parse(current.data), $type: current.type, name: current.name } as Variant,
      after: undefined,
      actor: actor as string,
    })

    await this.maybeScheduleFlush()

    return true
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
    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    // Generate reverse predicate name (simple pluralization for now)
    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    // Check if relationship already exists
    const existing = [...this.sql.exec<StoredRelationship>(
      `SELECT * FROM relationships
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
      fromNs, fromEntityId, predicate, toNs, toEntityId
    )]

    if (existing.length > 0 && existing[0].deleted_at === null) {
      // Already exists and not deleted
      return
    }

    const dataJson = options.data ? JSON.stringify(options.data) : null

    if (existing.length > 0) {
      // Undelete and update
      this.sql.exec(
        `UPDATE relationships
         SET deleted_at = NULL, deleted_by = NULL, version = version + 1, data = ?
         WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
        dataJson, fromNs, fromEntityId, predicate, toNs, toEntityId
      )
    } else {
      // Insert new relationship
      this.sql.exec(
        `INSERT INTO relationships
         (from_ns, from_id, predicate, to_ns, to_id, reverse, version, created_at, created_by, data)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
        fromNs, fromEntityId, predicate, toNs, toEntityId, reverse, now, actor, dataJson
      )
    }

    // Append relationship event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: undefined,
      after: { predicate, to: toId, data: options.data } as Variant,
      actor: actor as string,
    })

    await this.maybeScheduleFlush()
  }

  /**
   * Remove a relationship between two entities
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
    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    // Soft delete the relationship
    this.sql.exec(
      `UPDATE relationships
       SET deleted_at = ?, deleted_by = ?, version = version + 1
       WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?
       AND deleted_at IS NULL`,
      now, actor, fromNs, fromEntityId, predicate, toNs, toEntityId
    )

    // Append relationship event
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'DELETE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: { predicate, to: toId } as Variant,
      after: undefined,
      actor: actor as string,
    })

    await this.maybeScheduleFlush()
  }

  // ===========================================================================
  // Read Operations (for consistency, reads through DO)
  // ===========================================================================

  /**
   * Get an entity by ID from DO storage
   *
   * Note: For performance, reads can bypass DO and go directly to R2.
   * This method is for cases where strong consistency is required.
   */
  async get(ns: string, id: string, includeDeleted = false): Promise<Entity | null> {
    await this.ensureInitialized()

    const query = includeDeleted
      ? 'SELECT * FROM entities WHERE ns = ? AND id = ?'
      : 'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL'

    const rows = [...this.sql.exec<StoredEntity>(query, ns, id)]

    if (rows.length === 0) {
      return null
    }

    return this.toEntity(rows[0])
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

    return rows.map(row => this.toRelationship(row))
  }

  // ===========================================================================
  // Event Log
  // ===========================================================================

  /**
   * Append an event to the log
   */
  async appendEvent(event: Event): Promise<void> {
    await this.ensureInitialized()

    // Extract ns and entity_id from target for SQLite storage
    let ns: string | null = null
    let entityId: string | null = null
    if (!isRelationshipTarget(event.target)) {
      const info = parseEntityTarget(event.target)
      ns = info.ns
      entityId = info.id
    }

    this.sql.exec(
      `INSERT INTO events (id, ts, target, op, ns, entity_id, before, after, actor, metadata, flushed)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
      event.id,
      new Date(event.ts).toISOString(),
      event.target,
      event.op,
      ns,
      entityId,
      event.before ? JSON.stringify(event.before) : null,
      event.after ? JSON.stringify(event.after) : null,
      event.actor || null,
      event.metadata ? JSON.stringify(event.metadata) : null
    )
  }

  /**
   * Get unflushed event count
   */
  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM events WHERE flushed = 0'
    )]

    return rows[0]?.count || 0
  }

  /**
   * Flush events to Parquet and store in R2
   */
  async flushToParquet(): Promise<void> {
    await this.ensureInitialized()

    // Get unflushed events
    const events = [...this.sql.exec<StoredEvent>(
      'SELECT * FROM events WHERE flushed = 0 ORDER BY ts LIMIT ?',
      this.flushConfig.maxEvents
    )]

    if (events.length < this.flushConfig.minEvents) {
      // Not enough events to flush
      return
    }

    const firstEvent = events[0]
    const lastEvent = events[events.length - 1]
    const checkpointId = generateULID()

    // TODO: Actually write Parquet file to R2
    // For now, we'll just mark events as flushed

    // Generate parquet path
    const date = new Date(firstEvent.ts)
    const datePath = `${date.getUTCFullYear()}/${String(date.getUTCMonth() + 1).padStart(2, '0')}/${String(date.getUTCDate()).padStart(2, '0')}`
    const parquetPath = `events/archive/${datePath}/${checkpointId}.parquet`

    // In a real implementation, we would:
    // 1. Convert events to Parquet format
    // 2. Write to R2: this.env.BUCKET.put(parquetPath, parquetBuffer)
    // 3. Verify the write succeeded

    // Mark events as flushed
    const eventIds = events.map(e => e.id)
    this.sql.exec(
      `UPDATE events SET flushed = 1 WHERE id IN (${eventIds.map(() => '?').join(',')})`,
      ...eventIds
    )

    // Record checkpoint
    this.sql.exec(
      `INSERT INTO checkpoints (id, created_at, event_count, first_event_id, last_event_id, parquet_path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      checkpointId,
      new Date().toISOString(),
      events.length,
      firstEvent.id,
      lastEvent.id,
      parquetPath
    )
  }

  // ===========================================================================
  // Alarm Handler (for scheduled flush)
  // ===========================================================================

  /**
   * Handle scheduled alarm for flushing events
   */
  override async alarm(): Promise<void> {
    this.flushAlarmSet = false
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
    const data = JSON.parse(stored.data) as Record<string, unknown>

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
   */
  private toRelationship(stored: StoredRelationship): Relationship {
    return {
      fromNs: stored.from_ns as Namespace,
      fromId: stored.from_id as Id,
      fromType: '', // Would need to look up from entity
      fromName: '', // Would need to look up from entity
      predicate: stored.predicate,
      reverse: stored.reverse,
      toNs: stored.to_ns as Namespace,
      toId: stored.to_id as Id,
      toType: '', // Would need to look up from entity
      toName: '', // Would need to look up from entity
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by as EntityId,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by as EntityId | undefined,
      version: stored.version,
      data: stored.data ? JSON.parse(stored.data) : undefined,
    }
  }
}

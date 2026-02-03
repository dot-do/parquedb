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
 *
 * SOURCE OF TRUTH:
 * In Cloudflare Workers, this SQLite store is the authoritative source for writes.
 * Reads go through QueryExecutor/ReadPath to R2 for performance.
 *
 * This is separate from ParqueDB.ts which uses globalEntityStore (in-memory) for
 * Node.js/testing environments. See docs/architecture/ENTITY_STORAGE.md for details.
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
} from '../types'
import { entityTarget, relTarget } from '../types'
import type { Env, FlushConfig } from '../types/worker'
import { getRandom48Bit, parseStoredData } from '../utils'

// Initialize Sqids for short ID generation
const sqids = new Sqids()

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

// Note: StoredEvent interface removed - legacy table kept for backward compatibility only

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
/** WAL batch thresholds */
const EVENT_BATCH_COUNT_THRESHOLD = 100
const EVENT_BATCH_SIZE_THRESHOLD = 64 * 1024 // 64KB

/** Bulk write threshold - 5+ entities go directly to R2 instead of SQLite buffer */
const BULK_THRESHOLD = 5

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

  /** Event buffer for WAL batching (reduces SQLite row costs) */
  private eventBuffer: Event[] = []

  /** Approximate size of buffered events in bytes */
  private eventBufferSize = 0

  /** Namespace sequence counters for short ID generation with Sqids */
  private counters: Map<string, number> = new Map()

  /** Event buffers per namespace for WAL batching with sequence tracking */
  private nsEventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }> = new Map()

  /** Whether counters have been initialized from SQLite */
  private countersInitialized = false

  /** LRU cache for recent entity states (derived from events) */
  private entityCache: Map<string, { entity: Entity; version: number }> = new Map()

  /** Maximum size of the entity cache */
  private static readonly ENTITY_CACHE_MAX_SIZE = 1000

  /** Flag to skip entity table writes (Phase 3 WAL) */
  private skipEntityTableWrites = true

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

    // NEW: events_wal table for WAL batching with namespace-based counters
    // Each row contains a batch of events with Sqids sequence range for short IDs
    // first_seq/last_seq track the counter range for ID generation
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events_wal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ns TEXT NOT NULL,
        first_seq INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        events BLOB NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    // Legacy event_batches table - kept for backward compatibility
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS event_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch BLOB NOT NULL,
        min_ts INTEGER NOT NULL,
        max_ts INTEGER NOT NULL,
        event_count INTEGER NOT NULL,
        flushed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `)

    // Legacy events table - kept for backward compatibility during migration
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        target TEXT NOT NULL,
        op TEXT NOT NULL,
        ns TEXT,
        entity_id TEXT,
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

    // Pending row groups table - tracks bulk writes to R2 pending files
    // Used by Phase 2 bulk bypass: 5+ entities stream directly to R2
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS pending_row_groups (
        id TEXT PRIMARY KEY,
        ns TEXT NOT NULL,
        path TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        first_seq INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        created_at TEXT NOT NULL
      )
    `)

    // Create indexes
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(ns, type)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_updated ON entities(ns, updated_at)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_from ON relationships(from_ns, from_id, predicate)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_to ON relationships(to_ns, to_id, reverse)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_event_batches_flushed ON event_batches(flushed, min_ts)')
    // Legacy index kept for backward compatibility
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_unflushed ON events(flushed, ts)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_ns ON events(ns, entity_id)')
    // Index for pending row groups queries
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_pending_row_groups_ns ON pending_row_groups(ns, created_at)')

    this.initialized = true

    // Initialize sequence counters from events_wal
    await this.initializeCounters()
  }

  /**
   * Initialize namespace counters from events_wal table
   * On DO startup, load the max sequence for each namespace
   */
  private async initializeCounters(): Promise<void> {
    if (this.countersInitialized) return

    interface CounterRow {
      [key: string]: SqlStorageValue
      ns: string
      max_seq: number
    }

    // Get max sequence for each namespace
    const rows = [...this.sql.exec<CounterRow>(
      `SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`
    )]

    for (const row of rows) {
      // Next ID starts after max_seq
      this.counters.set(row.ns, row.max_seq + 1)
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

    // Phase 3 WAL: Skip entity table writes - state derived from events
    if (!this.skipEntityTableWrites) {
      this.sql.exec(
        `INSERT INTO entities (ns, id, type, name, version, created_at, created_by, updated_at, updated_by, data)
         VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
        ns, id, $type, name, now, actor, now, actor, dataJson
      )
    }

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

      // Phase 3 WAL: Skip entity table writes - state derived from events
      if (!this.skipEntityTableWrites) {
        const dataJson = JSON.stringify(rest)
        this.sql.exec(
          `INSERT INTO entities (ns, id, type, name, version, created_at, created_by, updated_at, updated_by, data)
           VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
          ns, id, $type, name, now, actor, now, actor, dataJson
        )
      }
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

    // Phase 3 WAL: Get current entity state from events first, fall back to table
    let current: StoredEntity | null = null

    if (this.skipEntityTableWrites) {
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
    }

    if (!current) {
      const rows = [...this.sql.exec<StoredEntity>(
        'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL',
        ns, id
      )]
      if (rows.length > 0) {
        current = rows[0]!
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

    // Phase 3 WAL: Skip entity table writes - state derived from events
    if (!this.skipEntityTableWrites) {
      this.sql.exec(
        `UPDATE entities
         SET type = ?, name = ?, version = ?, updated_at = ?, updated_by = ?, data = ?
         WHERE ns = ? AND id = ?`,
        type, name, newVersion, now, actor, dataJson, ns, id
      )
    }

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

    // Phase 3 WAL: Get current entity state from events first, fall back to table
    let current: StoredEntity | null = null

    if (this.skipEntityTableWrites) {
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
    }

    if (!current) {
      const rows = [...this.sql.exec<StoredEntity>(
        'SELECT * FROM entities WHERE ns = ? AND id = ?',
        ns, id
      )]
      if (rows.length > 0) {
        current = rows[0]!
      }
    }

    if (!current) {
      return false
    }

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Phase 3 WAL: Skip entity table writes - state derived from events
    if (!this.skipEntityTableWrites) {
      if (options.hard) {
        this.sql.exec('DELETE FROM entities WHERE ns = ? AND id = ?', ns, id)
      } else {
        this.sql.exec(
          `UPDATE entities SET deleted_at = ?, deleted_by = ?, version = version + 1 WHERE ns = ? AND id = ?`,
          now, actor, ns, id
        )
      }
    }

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

    // Append REL_CREATE event to WAL (Phase 4 relationship batching)
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'REL_CREATE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: undefined,
      after: {
        predicate,
        reverse,
        fromNs,
        fromId: fromEntityId,
        toNs,
        toId: toEntityId,
        data: options.data,
      } as Variant,
      actor: actor as string,
    })

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

    // Generate reverse predicate name
    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    // Append REL_DELETE event to WAL (Phase 4 relationship batching)
    await this.appendEvent({
      id: generateULID(),
      ts: Date.now(),
      op: 'REL_DELETE',
      target: relTarget(entityTarget(fromNs, fromEntityId), predicate, entityTarget(toNs, toEntityId)),
      before: {
        predicate,
        reverse,
        fromNs,
        fromId: fromEntityId,
        toNs,
        toId: toEntityId,
      } as Variant,
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
   * Phase 3 WAL: Entity state is derived from events, not stored in entities table.
   * Priority order:
   * 1. Check in-memory entity cache
   * 2. Reconstruct from unflushed events
   * 3. Fall back to entities table (backward compatibility)
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

    // 2. Try to reconstruct from events
    const entityFromEvents = await this.getEntityFromEvents(ns, id)
    if (entityFromEvents) {
      if (!includeDeleted && entityFromEvents.deletedAt) {
        return null
      }
      this.cacheEntity(cacheKey, entityFromEvents)
      return entityFromEvents
    }

    // 3. Fall back to entities table (backward compatibility)
    const query = includeDeleted
      ? 'SELECT * FROM entities WHERE ns = ? AND id = ?'
      : 'SELECT * FROM entities WHERE ns = ? AND id = ? AND deleted_at IS NULL'

    const rows = [...this.sql.exec<StoredEntity>(query, ns, id)]

    if (rows.length === 0) {
      return null
    }

    const entity = this.toEntity(rows[0]!)
    this.cacheEntity(cacheKey, entity)
    return entity
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
      const batchEvents = this.deserializeEventBatch(row.batch)
      for (const event of batchEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // 2. Read from in-memory event buffer
    for (const event of this.eventBuffer) {
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
      const walEvents = this.deserializeEventBatch(row.events)
      for (const event of walEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // 4. Check namespace event buffers
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
        const { $type, name, ...rest } = event.after as { $type?: string; name?: string; [key: string]: unknown }
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

  // Phase 3 helper methods for testing/migration
  setSkipEntityTableWrites(skip: boolean): void {
    this.skipEntityTableWrites = skip
  }

  getSkipEntityTableWrites(): boolean {
    return this.skipEntityTableWrites
  }

  getCacheStats(): { size: number; maxSize: number } {
    return { size: this.entityCache.size, maxSize: ParqueDBDO.ENTITY_CACHE_MAX_SIZE }
  }

  clearEntityCache(): void {
    this.entityCache.clear()
  }

  isEntityCached(ns: string, id: string): boolean {
    return this.entityCache.has(`${ns}/${id}`)
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
  // Event Log with WAL Batching
  // ===========================================================================

  /**
   * Append an event to the log
   *
   * Events are buffered in memory and flushed as batches to reduce SQLite row costs.
   * Each batch is stored as a single row with events serialized as a blob.
   */
  async appendEvent(event: Event): Promise<void> {
    await this.ensureInitialized()

    // Buffer the event
    this.eventBuffer.push(event)

    // Estimate size (rough approximation)
    const eventJson = JSON.stringify(event)
    this.eventBufferSize += eventJson.length

    // Check if we should flush the batch
    if (this.eventBuffer.length >= EVENT_BATCH_COUNT_THRESHOLD ||
        this.eventBufferSize >= EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushEventBatch()
    }
  }

  /**
   * Append an event with namespace-based sequence tracking (new events_wal format)
   * Uses Sqids-based counter for short IDs instead of ULIDs
   *
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
   * Flush all namespace event buffers
   */
  async flushAllNsEventBatches(): Promise<void> {
    for (const ns of this.nsEventBuffers.keys()) {
      await this.flushNsEventBatch(ns)
    }
  }

  /**
   * Flush buffered events as a single batch row
   *
   * This is called automatically when thresholds are reached,
   * and should also be called on DO shutdown to persist partial batches.
   */
  async flushEventBatch(): Promise<void> {
    if (this.eventBuffer.length === 0) return

    await this.ensureInitialized()

    const events = this.eventBuffer
    const minTs = Math.min(...events.map(e => e.ts))
    const maxTs = Math.max(...events.map(e => e.ts))

    // Serialize events to blob
    const json = JSON.stringify(events)
    const data = new TextEncoder().encode(json)

    this.sql.exec(
      `INSERT INTO event_batches (batch, min_ts, max_ts, event_count, flushed)
       VALUES (?, ?, ?, ?, 0)`,
      data,
      minTs,
      maxTs,
      events.length
    )

    // Clear buffer
    this.eventBuffer = []
    this.eventBufferSize = 0
  }

  /**
   * Get unflushed event count (from batches + buffer)
   */
  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()

    // Count events in unflushed batches
    const rows = [...this.sql.exec<{ total: number }>(
      'SELECT SUM(event_count) as total FROM event_batches WHERE flushed = 0'
    )]

    const batchCount = rows[0]?.total || 0

    // Add buffered events not yet written
    return batchCount + this.eventBuffer.length
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
   * Get unflushed batch count
   */
  async getUnflushedBatchCount(): Promise<number> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM event_batches WHERE flushed = 0'
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
  async readUnflushedEvents(): Promise<Event[]> {
    await this.ensureInitialized()

    const allEvents: Event[] = []

    // Read from batches
    interface EventBatchRow extends Record<string, SqlStorageValue> {
      id: number
      batch: ArrayBuffer
      min_ts: number
      max_ts: number
      event_count: number
    }

    const rows = [...this.sql.exec<EventBatchRow>(
      `SELECT id, batch, min_ts, max_ts, event_count
       FROM event_batches
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    for (const row of rows) {
      const batchEvents = this.deserializeEventBatch(row.batch)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    allEvents.push(...this.eventBuffer)

    return allEvents
  }

  /**
   * Mark event batches as flushed (after writing to R2/Parquet)
   */
  async markEventBatchesFlushed(batchIds: number[]): Promise<void> {
    if (batchIds.length === 0) return

    await this.ensureInitialized()

    const placeholders = batchIds.map(() => '?').join(',')
    this.sql.exec(
      `UPDATE event_batches SET flushed = 1 WHERE id IN (${placeholders})`,
      ...batchIds
    )
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
    return JSON.parse(json) as Event[]
  }

  /**
   * Flush events to Parquet and store in R2
   */
  async flushToParquet(): Promise<void> {
    await this.ensureInitialized()

    // First flush any buffered events to a batch
    await this.flushEventBatch()

    // Get unflushed batches
    interface FlushEventBatchRow extends Record<string, SqlStorageValue> {
      id: number
      batch: ArrayBuffer
      min_ts: number
      max_ts: number
      event_count: number
    }

    const batches = [...this.sql.exec<FlushEventBatchRow>(
      `SELECT id, batch, min_ts, max_ts, event_count
       FROM event_batches
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    // Collect all events and count
    let totalCount = 0
    const allEvents: Event[] = []
    const batchIds: number[] = []

    for (const batch of batches) {
      const events = this.deserializeEventBatch(batch.batch)
      allEvents.push(...events)
      totalCount += events.length
      batchIds.push(batch.id)

      if (totalCount >= this.flushConfig.maxEvents) break
    }

    if (totalCount < this.flushConfig.minEvents) {
      // Not enough events to flush
      return
    }

    const firstEvent = allEvents[0]!
    const lastEvent = allEvents[allEvents.length - 1]!
    const checkpointId = generateULID()

    // Generate parquet path
    const date = new Date(firstEvent.ts)
    const datePath = `${date.getUTCFullYear()}/${String(date.getUTCMonth() + 1).padStart(2, '0')}/${String(date.getUTCDate()).padStart(2, '0')}`
    const parquetPath = `events/archive/${datePath}/${checkpointId}.parquet`

    // TODO: Actually write Parquet file to R2
    // In a real implementation, we would:
    // 1. Convert events to Parquet format
    // 2. Write to R2: this.env.BUCKET.put(parquetPath, parquetBuffer)
    // 3. Verify the write succeeded

    // Mark batches as flushed
    await this.markEventBatchesFlushed(batchIds)

    // Record checkpoint
    this.sql.exec(
      `INSERT INTO checkpoints (id, created_at, event_count, first_event_id, last_event_id, parquet_path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      checkpointId,
      new Date().toISOString(),
      allEvents.length,
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

    // First flush any buffered events to a batch
    await this.flushEventBatch()

    // Then flush batches to Parquet/R2
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
      data: stored.data ? parseStoredData(stored.data) : undefined,
    }
  }
}

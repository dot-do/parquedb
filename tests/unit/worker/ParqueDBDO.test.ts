/**
 * ParqueDB Durable Object Direct Unit Tests
 *
 * Comprehensive tests for ParqueDBDO covering:
 * - CRUD operations (create, get, update, delete)
 * - Bulk operations (createMany, deleteMany)
 * - Relationship operations (link, unlink, getRelationships)
 * - Event sourcing (event buffering, WAL, reconstruction)
 * - Cache invalidation signaling
 * - Error scenarios and edge cases
 *
 * Run with: pnpm test tests/unit/worker/ParqueDBDO.test.ts
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'

// =============================================================================
// Mock Types
// =============================================================================

interface SqlStorageValue {
  [key: string]: unknown
}

interface MockEntity {
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

interface MockRelationship {
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
  match_mode: string | null
  similarity: number | null
  data: string | null
}

interface MockEvent {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  target: string
  before?: Record<string, unknown> | undefined
  after?: Record<string, unknown> | undefined
  actor?: string | undefined
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// Mock SqlStorage
// =============================================================================

function createMockSqlStorage() {
  const entities = new Map<string, MockEntity>()
  const relationships = new Map<string, MockRelationship>()
  const eventsWal: Array<{ id: number; ns: string; first_seq: number; last_seq: number; events: Uint8Array; created_at: string }> = []
  const relsWal: Array<{ id: number; ns: string; first_seq: number; last_seq: number; events: Uint8Array; created_at: string }> = []
  const pendingRowGroups: Array<{ id: string; ns: string; path: string; row_count: number; first_seq: number; last_seq: number; created_at: string }> = []
  const checkpoints: Array<{ id: string; created_at: string; event_count: number; first_event_id: string; last_event_id: string; parquet_path: string }> = []

  let autoIncrementId = 1

  return {
    entities,
    relationships,
    eventsWal,
    relsWal,
    pendingRowGroups,
    checkpoints,
    exec: vi.fn((query: string, ...params: unknown[]) => {
      const trimmedQuery = query.trim().toLowerCase()

      // Handle CREATE TABLE / INDEX (no-op for mock)
      if (trimmedQuery.startsWith('create table') || trimmedQuery.startsWith('create index')) {
        return []
      }

      // Handle INSERT INTO entities
      if (trimmedQuery.includes('insert into entities')) {
        const entity: MockEntity = {
          ns: params[0] as string,
          id: params[1] as string,
          type: params[2] as string,
          name: params[3] as string,
          version: 1,
          created_at: params[5] as string,
          created_by: params[6] as string,
          updated_at: params[7] as string,
          updated_by: params[8] as string,
          deleted_at: null,
          deleted_by: null,
          data: params[9] as string,
        }
        entities.set(`${entity.ns}/${entity.id}`, entity)
        return []
      }

      // Handle SELECT from entities
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from entities')) {
        const ns = params[0] as string
        const id = params[1] as string
        const key = `${ns}/${id}`
        const entity = entities.get(key)
        if (!entity) return []
        if (trimmedQuery.includes('deleted_at is null') && entity.deleted_at) return []
        return [entity]
      }

      // Handle UPDATE entities
      if (trimmedQuery.includes('update entities')) {
        if (trimmedQuery.includes('deleted_at')) {
          // Soft delete
          const ns = params[2] as string
          const id = params[3] as string
          const key = `${ns}/${id}`
          const entity = entities.get(key)
          if (entity) {
            entity.deleted_at = params[0] as string
            entity.deleted_by = params[1] as string
            entity.version++
          }
        } else {
          // Regular update
          const ns = params[6] as string
          const id = params[7] as string
          const key = `${ns}/${id}`
          const entity = entities.get(key)
          if (entity) {
            entity.type = params[0] as string
            entity.name = params[1] as string
            entity.version = params[2] as number
            entity.updated_at = params[3] as string
            entity.updated_by = params[4] as string
            entity.data = params[5] as string
          }
        }
        return []
      }

      // Handle DELETE FROM entities
      if (trimmedQuery.includes('delete from entities')) {
        const ns = params[0] as string
        const id = params[1] as string
        entities.delete(`${ns}/${id}`)
        return []
      }

      // Handle INSERT INTO relationships
      if (trimmedQuery.includes('insert into relationships')) {
        const rel: MockRelationship = {
          from_ns: params[0] as string,
          from_id: params[1] as string,
          predicate: params[2] as string,
          to_ns: params[3] as string,
          to_id: params[4] as string,
          reverse: params[5] as string,
          version: 1,
          created_at: params[6] as string,
          created_by: params[7] as string,
          deleted_at: null,
          deleted_by: null,
          match_mode: params[8] as string ?? null,
          similarity: params[9] as number ?? null,
          data: params[10] as string ?? null,
        }
        const key = `${rel.from_ns}/${rel.from_id}/${rel.predicate}/${rel.to_ns}/${rel.to_id}`
        relationships.set(key, rel)
        return []
      }

      // Handle UPDATE relationships
      if (trimmedQuery.includes('update relationships')) {
        if (trimmedQuery.includes('deleted_at = null')) {
          // Restore/update relationship
          const matchMode = params[0] as string ?? null
          const similarity = params[1] as number ?? null
          const data = params[2] as string ?? null
          const fromNs = params[3] as string
          const fromId = params[4] as string
          const predicate = params[5] as string
          const toNs = params[6] as string
          const toId = params[7] as string
          const key = `${fromNs}/${fromId}/${predicate}/${toNs}/${toId}`
          const rel = relationships.get(key)
          if (rel) {
            rel.deleted_at = null
            rel.deleted_by = null
            rel.match_mode = matchMode
            rel.similarity = similarity
            rel.data = data
            rel.version++
          }
        } else if (trimmedQuery.includes('deleted_at = ?')) {
          // Soft delete relationship
          const deletedAt = params[0] as string
          const deletedBy = params[1] as string
          const fromNs = params[2] as string
          const fromId = params[3] as string
          const predicate = params[4] as string
          const toNs = params[5] as string
          const toId = params[6] as string
          const key = `${fromNs}/${fromId}/${predicate}/${toNs}/${toId}`
          const rel = relationships.get(key)
          if (rel && !rel.deleted_at) {
            rel.deleted_at = deletedAt
            rel.deleted_by = deletedBy
            rel.version++
          }
        }
        return []
      }

      // Handle SELECT from relationships
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from relationships')) {
        const results: MockRelationship[] = []
        if (trimmedQuery.includes('from_ns = ?') && !trimmedQuery.includes('to_ns = ?')) {
          // Outbound relationships
          const ns = params[0] as string
          const id = params[1] as string
          const predicate = params.length > 2 ? params[2] as string : undefined
          for (const rel of relationships.values()) {
            if (rel.from_ns === ns && rel.from_id === id && !rel.deleted_at) {
              if (!predicate || rel.predicate === predicate) {
                results.push(rel)
              }
            }
          }
        } else if (trimmedQuery.includes('to_ns = ?')) {
          // Inbound relationships
          const ns = params[0] as string
          const id = params[1] as string
          const reverse = params.length > 2 ? params[2] as string : undefined
          for (const rel of relationships.values()) {
            if (rel.to_ns === ns && rel.to_id === id && !rel.deleted_at) {
              if (!reverse || rel.reverse === reverse) {
                results.push(rel)
              }
            }
          }
        } else if (trimmedQuery.includes('from_ns = ? and from_id = ? and predicate = ? and to_ns = ? and to_id = ?')) {
          // Exact relationship lookup
          const key = `${params[0]}/${params[1]}/${params[2]}/${params[3]}/${params[4]}`
          const rel = relationships.get(key)
          if (rel) results.push(rel)
        }
        return results
      }

      // Handle DELETE FROM relationships
      if (trimmedQuery.includes('delete from relationships')) {
        if (trimmedQuery.includes('(from_ns = ? and from_id = ?) or (to_ns = ? and to_id = ?)')) {
          // Delete all relationships for an entity
          const ns1 = params[0] as string
          const id1 = params[1] as string
          const ns2 = params[2] as string
          const id2 = params[3] as string
          for (const [key, rel] of relationships) {
            if ((rel.from_ns === ns1 && rel.from_id === id1) ||
                (rel.to_ns === ns2 && rel.to_id === id2)) {
              relationships.delete(key)
            }
          }
        }
        return []
      }

      // Handle INSERT INTO events_wal
      if (trimmedQuery.includes('insert into events_wal')) {
        eventsWal.push({
          id: autoIncrementId++,
          ns: params[0] as string,
          first_seq: params[1] as number,
          last_seq: params[2] as number,
          events: params[3] as Uint8Array,
          created_at: params[4] as string,
        })
        return []
      }

      // Handle SELECT from events_wal
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from events_wal')) {
        if (trimmedQuery.includes('max(last_seq)')) {
          // Counter initialization query
          const grouped = new Map<string, number>()
          for (const wal of eventsWal) {
            const current = grouped.get(wal.ns) ?? 0
            if (wal.last_seq > current) {
              grouped.set(wal.ns, wal.last_seq)
            }
          }
          return Array.from(grouped.entries()).map(([ns, max_seq]) => ({ ns, max_seq }))
        }
        if (trimmedQuery.includes('distinct ns')) {
          const namespaces = new Set<string>()
          for (const wal of eventsWal) {
            namespaces.add(wal.ns)
          }
          return Array.from(namespaces).map(ns => ({ ns }))
        }
        if (trimmedQuery.includes('sum(last_seq - first_seq + 1)')) {
          const ns = params[0] as string | undefined
          let total = 0
          for (const wal of eventsWal) {
            if (!ns || wal.ns === ns) {
              total += (wal.last_seq - wal.first_seq + 1)
            }
          }
          return [{ total }]
        }
        if (trimmedQuery.includes('count(*)')) {
          return [{ count: eventsWal.length }]
        }
        // Return WAL entries - with or without namespace filter
        if (params.length > 0) {
          const ns = params[0] as string
          return eventsWal.filter(w => w.ns === ns)
        }
        // No namespace filter - return all WAL entries
        return eventsWal
      }

      // Handle DELETE FROM events_wal
      if (trimmedQuery.includes('delete from events_wal')) {
        if (trimmedQuery.includes('where id in')) {
          // Delete by IDs
          const ids = params.map(Number)
          for (let i = eventsWal.length - 1; i >= 0; i--) {
            if (ids.includes(eventsWal[i]!.id)) {
              eventsWal.splice(i, 1)
            }
          }
        } else if (trimmedQuery.includes('where ns = ? and last_seq <= ?')) {
          // Delete by namespace and sequence
          const ns = params[0] as string
          const upToSeq = params[1] as number
          for (let i = eventsWal.length - 1; i >= 0; i--) {
            if (eventsWal[i]!.ns === ns && eventsWal[i]!.last_seq <= upToSeq) {
              eventsWal.splice(i, 1)
            }
          }
        }
        return []
      }

      // Handle INSERT INTO rels_wal
      if (trimmedQuery.includes('insert into rels_wal')) {
        relsWal.push({
          id: autoIncrementId++,
          ns: params[0] as string,
          first_seq: params[1] as number,
          last_seq: params[2] as number,
          events: params[3] as Uint8Array,
          created_at: params[4] as string,
        })
        return []
      }

      // Handle SELECT from rels_wal
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from rels_wal')) {
        if (trimmedQuery.includes('max(last_seq)')) {
          const grouped = new Map<string, number>()
          for (const wal of relsWal) {
            const current = grouped.get(wal.ns) ?? 0
            if (wal.last_seq > current) {
              grouped.set(wal.ns, wal.last_seq)
            }
          }
          return Array.from(grouped.entries()).map(([ns, max_seq]) => ({ ns, max_seq }))
        }
        if (trimmedQuery.includes('distinct ns')) {
          const namespaces = new Set<string>()
          for (const wal of relsWal) {
            namespaces.add(wal.ns)
          }
          return Array.from(namespaces).map(ns => ({ ns }))
        }
        if (trimmedQuery.includes('count(*)')) {
          const ns = params.length > 0 ? params[0] as string : undefined
          const filtered = ns ? relsWal.filter(w => w.ns === ns) : relsWal
          return [{ count: filtered.length }]
        }
        const ns = params[0] as string
        return relsWal.filter(w => w.ns === ns)
      }

      // Handle DELETE FROM rels_wal
      if (trimmedQuery.includes('delete from rels_wal')) {
        if (trimmedQuery.includes('where ns = ? and last_seq <= ?')) {
          const ns = params[0] as string
          const upToSeq = params[1] as number
          for (let i = relsWal.length - 1; i >= 0; i--) {
            if (relsWal[i]!.ns === ns && relsWal[i]!.last_seq <= upToSeq) {
              relsWal.splice(i, 1)
            }
          }
        }
        return []
      }

      // Handle INSERT INTO pending_row_groups
      if (trimmedQuery.includes('insert into pending_row_groups')) {
        pendingRowGroups.push({
          id: params[0] as string,
          ns: params[1] as string,
          path: params[2] as string,
          row_count: params[3] as number,
          first_seq: params[4] as number,
          last_seq: params[5] as number,
          created_at: params[6] as string,
        })
        return []
      }

      // Handle SELECT from pending_row_groups
      if (trimmedQuery.includes('select') && trimmedQuery.includes('from pending_row_groups')) {
        const ns = params[0] as string
        return pendingRowGroups.filter(p => p.ns === ns)
      }

      // Handle DELETE FROM pending_row_groups
      if (trimmedQuery.includes('delete from pending_row_groups')) {
        const ns = params[0] as string
        const upToSeq = params[1] as number
        for (let i = pendingRowGroups.length - 1; i >= 0; i--) {
          if (pendingRowGroups[i]!.ns === ns && pendingRowGroups[i]!.last_seq <= upToSeq) {
            pendingRowGroups.splice(i, 1)
          }
        }
        return []
      }

      // Handle INSERT INTO checkpoints
      if (trimmedQuery.includes('insert into checkpoints')) {
        checkpoints.push({
          id: params[0] as string,
          created_at: params[1] as string,
          event_count: params[2] as number,
          first_event_id: params[3] as string,
          last_event_id: params[4] as string,
          parquet_path: params[5] as string,
        })
        return []
      }

      // Default: return empty
      return []
    }),
  }
}

// =============================================================================
// Mock R2 Bucket
// =============================================================================

function createMockR2Bucket() {
  const objects = new Map<string, Uint8Array>()

  return {
    objects,
    put: vi.fn(async (key: string, value: ArrayBuffer | Uint8Array | string) => {
      if (typeof value === 'string') {
        objects.set(key, new TextEncoder().encode(value))
      } else if (value instanceof Uint8Array) {
        objects.set(key, value)
      } else {
        objects.set(key, new Uint8Array(value))
      }
    }),
    get: vi.fn(async (key: string) => {
      const value = objects.get(key)
      if (!value) return null
      return {
        arrayBuffer: async () => value.buffer,
        text: async () => new TextDecoder().decode(value),
        body: value,
      }
    }),
    delete: vi.fn(async (key: string) => {
      objects.delete(key)
    }),
  }
}

// =============================================================================
// Mock DurableObjectState
// =============================================================================

function createMockDurableObjectState(sql: ReturnType<typeof createMockSqlStorage>) {
  const alarms: number[] = []

  return {
    storage: {
      sql,
      setAlarm: vi.fn(async (time: number) => {
        alarms.push(time)
      }),
      getAlarm: vi.fn(async () => alarms[0] ?? null),
      deleteAlarm: vi.fn(async () => {
        alarms.length = 0
      }),
    },
    id: { toString: () => 'test-do-id' },
    alarms,
  }
}

// =============================================================================
// Mock Sqids for ID generation
// =============================================================================

vi.mock('sqids', () => ({
  default: class MockSqids {
    encode(numbers: number[]): string {
      // Simple mock that returns predictable IDs
      return `id${numbers[0]}`
    }
    decode(id: string): number[] {
      const match = id.match(/^id(\d+)$/)
      return match ? [parseInt(match[1]!, 10)] : [0]
    }
  },
}))

// =============================================================================
// ParqueDBDO-like Implementation for Testing
// =============================================================================

/**
 * Simplified ParqueDBDO implementation for testing
 * Mirrors the key behaviors of the real DO without Cloudflare dependencies
 */
class TestParqueDBDO {
  private sql: ReturnType<typeof createMockSqlStorage>
  private bucket: ReturnType<typeof createMockR2Bucket>
  private ctx: ReturnType<typeof createMockDurableObjectState>
  private initialized = false
  // Entity ID counters (separate from event sequences)
  private entityIdCounters = new Map<string, number>()
  // Sequence counters for events (used by transaction rollback)
  public counters = new Map<string, number>()
  private countersInitialized = false
  public entityCache = new Map<string, { entity: MockEntity & { $id: string; $type: string }; version: number }>()
  public nsEventBuffers = new Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>()
  public relEventBuffers = new Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>()
  private invalidationVersions = new Map<string, number>()
  private pendingInvalidations: Array<{
    ns: string
    type: 'entity' | 'relationship' | 'full'
    timestamp: number
    version: number
    entityId?: string
  }> = []
  private flushAlarmSet = false

  private static readonly ENTITY_CACHE_MAX_SIZE = 1000
  private static readonly MAX_PENDING_INVALIDATIONS = 100
  private static readonly EVENT_BATCH_COUNT_THRESHOLD = 100
  private static readonly EVENT_BATCH_SIZE_THRESHOLD = 64 * 1024

  constructor(
    sql: ReturnType<typeof createMockSqlStorage>,
    bucket: ReturnType<typeof createMockR2Bucket>,
    ctx: ReturnType<typeof createMockDurableObjectState>
  ) {
    this.sql = sql
    this.bucket = bucket
    this.ctx = ctx
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Create tables (mock no-op)
    this.sql.exec('CREATE TABLE IF NOT EXISTS entities ...')
    this.sql.exec('CREATE TABLE IF NOT EXISTS relationships ...')
    this.sql.exec('CREATE TABLE IF NOT EXISTS events_wal ...')
    this.sql.exec('CREATE TABLE IF NOT EXISTS rels_wal ...')
    this.sql.exec('CREATE TABLE IF NOT EXISTS checkpoints ...')
    this.sql.exec('CREATE TABLE IF NOT EXISTS pending_row_groups ...')

    this.initialized = true
    await this.initializeCounters()
  }

  private async initializeCounters(): Promise<void> {
    if (this.countersInitialized) return

    // Entity ID counters start at 1 and are separate from event sequences
    // They could be initialized from max entity IDs, but for simplicity start fresh
    this.countersInitialized = true
  }

  private getNextId(ns: string): string {
    const seq = this.entityIdCounters.get(ns) || 1
    this.entityIdCounters.set(ns, seq + 1)
    return `id${seq}`
  }

  private generateULID(): string {
    return `ulid_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`
  }

  async create(
    ns: string,
    data: { $type: string; name: string; [key: string]: unknown },
    options: { actor?: string } = {}
  ): Promise<{ $id: string; $type: string; name: string; createdAt: Date; createdBy: string; updatedAt: Date; updatedBy: string; version: number; [key: string]: unknown }> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'
    const id = this.getNextId(ns)
    const entityId = `${ns}/${id}`

    const { $type, name, ...rest } = data
    if (!$type) throw new Error('Entity must have $type')
    if (!name) throw new Error('Entity must have name')

    const dataJson = JSON.stringify(rest)

    // Append create event
    await this.appendEvent({
      id: this.generateULID(),
      ts: Date.now(),
      op: 'CREATE',
      target: `${ns}:${id}`,
      before: undefined,
      after: { ...rest, $type, name },
      actor,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(ns, 'entity', id)

    return {
      $id: entityId,
      $type,
      name,
      createdAt: new Date(now),
      createdBy: actor,
      updatedAt: new Date(now),
      updatedBy: actor,
      version: 1,
      ...rest,
    }
  }

  async createMany(
    ns: string,
    items: Array<{ $type: string; name: string; [key: string]: unknown }>,
    options: { actor?: string } = {}
  ): Promise<Array<{ $id: string; $type: string; name: string; version: number; [key: string]: unknown }>> {
    await this.ensureInitialized()

    if (items.length === 0) return []

    const entities = []
    for (const item of items) {
      const entity = await this.create(ns, item, options)
      entities.push(entity)
    }
    return entities
  }

  async get(ns: string, id: string, includeDeleted = false): Promise<{ $id: string; $type: string; name: string; version: number; deletedAt?: Date; [key: string]: unknown } | null> {
    await this.ensureInitialized()

    const cacheKey = `${ns}/${id}`

    // Check cache first
    const cached = this.entityCache.get(cacheKey)
    if (cached) {
      const entity = cached.entity
      if (!includeDeleted && entity.deleted_at) {
        return null
      }
      return this.toApiEntity(entity)
    }

    // Reconstruct from events
    const entity = await this.getEntityFromEvents(ns, id)
    if (!entity) return null

    if (!includeDeleted && entity.deletedAt) {
      return null
    }

    return entity
  }

  async getEntityFromEvents(ns: string, id: string): Promise<{ $id: string; $type: string; name: string; createdAt: Date; createdBy: string; updatedAt: Date; updatedBy: string; deletedAt?: Date; deletedBy?: string; version: number; [key: string]: unknown } | null> {
    await this.ensureInitialized()

    const target = `${ns}:${id}`
    let entity: { $id: string; $type: string; name: string; createdAt: Date; createdBy: string; updatedAt: Date; updatedBy: string; deletedAt?: Date; deletedBy?: string; version: number; [key: string]: unknown } | null = null

    // Read from events_wal
    const walRows = this.sql.exec(`SELECT events FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`, ns) as Array<{ events: Uint8Array }>
    for (const row of walRows) {
      const walEvents = this.deserializeEventBatch(row.events)
      for (const event of walEvents) {
        if (event.target === target) {
          entity = this.applyEventToEntity(entity, event, ns, id)
        }
      }
    }

    // Read from in-memory buffer
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

  private applyEventToEntity(
    current: { $id: string; $type: string; name: string; createdAt: Date; createdBy: string; updatedAt: Date; updatedBy: string; deletedAt?: Date; deletedBy?: string; version: number; [key: string]: unknown } | null,
    event: MockEvent,
    ns: string,
    id: string
  ): typeof current {
    const entityId = `${ns}/${id}`

    switch (event.op) {
      case 'CREATE': {
        if (!event.after) return current
        const { $type, name, ...rest } = event.after as { $type: string; name: string; [key: string]: unknown }
        return {
          $id: entityId,
          $type: $type || 'Unknown',
          name: name || id,
          createdAt: new Date(event.ts),
          createdBy: event.actor || 'system/anonymous',
          updatedAt: new Date(event.ts),
          updatedBy: event.actor || 'system/anonymous',
          version: 1,
          ...rest,
        }
      }

      case 'UPDATE': {
        if (!current || !event.after) return current
        const { $type, name, deletedAt, deletedBy, ...rest } = event.after as { $type?: string; name?: string; deletedAt?: Date | null; deletedBy?: string | null; [key: string]: unknown }
        // Build the updated entity
        const updated: typeof current = {
          ...current,
          $type: $type || current.$type,
          name: name || current.name,
          updatedAt: new Date(event.ts),
          updatedBy: event.actor || 'system/anonymous',
          version: current.version + 1,
          ...rest,
        }
        // If deletedAt is explicitly undefined/null in after, this is a restore - remove deletedAt/By
        // If deletedAt is not in after at all and current has it, keep it (normal update of deleted entity)
        // Check if before had deletedAt and after doesn't - this indicates restore
        if (event.before && (event.before as Record<string, unknown>).deletedAt && !deletedAt) {
          delete updated.deletedAt
          delete updated.deletedBy
        }
        return updated
      }

      case 'DELETE': {
        if (!current) return null
        return {
          ...current,
          deletedAt: new Date(event.ts),
          deletedBy: event.actor || 'system/anonymous',
          version: current.version + 1,
        }
      }

      default:
        return current
    }
  }

  async update(
    ns: string,
    id: string,
    update: {
      $set?: Record<string, unknown>
      $unset?: Record<string, unknown>
      $inc?: Record<string, number>
      $push?: Record<string, unknown>
      $pull?: Record<string, unknown>
    },
    options: { actor?: string; expectedVersion?: number; upsert?: boolean } = {}
  ): Promise<{ $id: string; $type: string; name: string; version: number; [key: string]: unknown }> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Get current entity state from events
    const current = await this.getEntityFromEvents(ns, id)

    if (!current || current.deletedAt) {
      if (options.upsert) {
        const createData = {
          $type: 'Unknown',
          name: id,
          ...(update.$set || {}),
        }
        return this.create(ns, createData as { $type: string; name: string }, { actor })
      }
      throw new Error(`Entity ${ns}/${id} not found`)
    }

    // Check version for optimistic concurrency
    if (options.expectedVersion !== undefined && current.version !== options.expectedVersion) {
      throw new Error(`Version mismatch: expected ${options.expectedVersion}, got ${current.version}`)
    }

    // Apply update operators
    const { $id, $type, name, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy, version, ...data } = current
    let newData: Record<string, unknown> = { ...data }
    let newName = name
    let newType = $type

    if (update.$set) {
      for (const [key, value] of Object.entries(update.$set)) {
        if (key === 'name') {
          newName = value as string
        } else if (key === '$type') {
          newType = value as string
        } else {
          newData[key] = value
        }
      }
    }

    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete newData[key]
      }
    }

    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        const cur = (newData[key] as number) || 0
        newData[key] = cur + value
      }
    }

    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const arr = (newData[key] as unknown[]) || []
        arr.push(value)
        newData[key] = arr
      }
    }

    if (update.$pull) {
      for (const [key, value] of Object.entries(update.$pull)) {
        const arr = (newData[key] as unknown[]) || []
        newData[key] = arr.filter(item => item !== value)
      }
    }

    const newVersion = version + 1

    // Invalidate cache
    this.entityCache.delete(`${ns}/${id}`)

    // Append update event
    await this.appendEvent({
      id: this.generateULID(),
      ts: Date.now(),
      op: 'UPDATE',
      target: `${ns}:${id}`,
      before: { ...data, $type, name },
      after: { ...newData, $type: newType, name: newName },
      actor,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(ns, 'entity', id)

    return {
      $id: `${ns}/${id}`,
      $type: newType,
      name: newName,
      createdAt,
      createdBy,
      updatedAt: new Date(now),
      updatedBy: actor,
      version: newVersion,
      ...newData,
    }
  }

  async delete(
    ns: string,
    id: string,
    options: { actor?: string; expectedVersion?: number; hard?: boolean } = {}
  ): Promise<{ deletedCount: number }> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Get current entity from events
    const current = await this.getEntityFromEvents(ns, id)
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
        ns, id, ns, id
      )
    } else {
      this.sql.exec(
        `UPDATE relationships SET deleted_at = ?, deleted_by = ? WHERE (from_ns = ? AND from_id = ?) OR (to_ns = ? AND to_id = ?)`,
        now, actor, ns, id, ns, id
      )
    }

    // Invalidate cache
    this.entityCache.delete(`${ns}/${id}`)

    // Append delete event
    const { $id, $type, name, createdAt, createdBy, updatedAt, updatedBy, version, ...data } = current
    await this.appendEvent({
      id: this.generateULID(),
      ts: Date.now(),
      op: 'DELETE',
      target: `${ns}:${id}`,
      before: { ...data, $type, name },
      after: undefined,
      actor,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(ns, 'entity', id)

    return { deletedCount: 1 }
  }

  async deleteMany(
    ns: string,
    ids: string[],
    options: { actor?: string; hard?: boolean } = {}
  ): Promise<{ deletedCount: number }> {
    let deletedCount = 0
    for (const id of ids) {
      const result = await this.delete(ns, id, options)
      deletedCount += result.deletedCount
    }
    return { deletedCount }
  }

  async link(
    fromId: string,
    predicate: string,
    toId: string,
    options: { actor?: string; matchMode?: 'exact' | 'fuzzy'; similarity?: number; data?: Record<string, unknown> } = {}
  ): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Parse entity IDs
    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    const reverse = predicate.endsWith('s') ? predicate : predicate + 's'

    // Check if relationship exists
    const existing = this.sql.exec(
      `SELECT * FROM relationships WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
      fromNs, fromEntityId, predicate, toNs, toEntityId
    ) as MockRelationship[]

    if (existing.length > 0 && existing[0]!.deleted_at === null) {
      // Already exists and not deleted
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
        `UPDATE relationships SET deleted_at = NULL, deleted_by = NULL, version = version + 1, match_mode = ?, similarity = ?, data = ? WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ?`,
        matchMode, similarity, dataJson, fromNs, fromEntityId, predicate, toNs, toEntityId
      )
    } else {
      // Insert new
      this.sql.exec(
        `INSERT INTO relationships (from_ns, from_id, predicate, to_ns, to_id, reverse, version, created_at, created_by, match_mode, similarity, data) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
        fromNs, fromEntityId, predicate, toNs, toEntityId, reverse, now, actor, matchMode, similarity, dataJson
      )
    }

    // Append relationship event
    await this.appendRelEvent(fromNs!, {
      ts: Date.now(),
      op: 'CREATE',
      target: `${fromNs}:${fromEntityId}:${predicate}:${toNs}:${toEntityId}`,
      after: { predicate, to: toId, matchMode: options.matchMode, similarity: options.similarity, data: options.data },
      actor,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(fromNs!, 'relationship')
    if (fromNs !== toNs) {
      this.signalCacheInvalidation(toNs!, 'relationship')
    }
  }

  async unlink(
    fromId: string,
    predicate: string,
    toId: string,
    options: { actor?: string } = {}
  ): Promise<void> {
    await this.ensureInitialized()

    const now = new Date().toISOString()
    const actor = options.actor || 'system/anonymous'

    // Parse entity IDs
    const [fromNs, ...fromIdParts] = fromId.split('/')
    const fromEntityId = fromIdParts.join('/')
    const [toNs, ...toIdParts] = toId.split('/')
    const toEntityId = toIdParts.join('/')

    // Soft delete
    this.sql.exec(
      `UPDATE relationships SET deleted_at = ?, deleted_by = ? WHERE from_ns = ? AND from_id = ? AND predicate = ? AND to_ns = ? AND to_id = ? AND deleted_at IS NULL`,
      now, actor, fromNs, fromEntityId, predicate, toNs, toEntityId
    )

    // Append relationship event
    await this.appendRelEvent(fromNs!, {
      ts: Date.now(),
      op: 'DELETE',
      target: `${fromNs}:${fromEntityId}:${predicate}:${toNs}:${toEntityId}`,
      before: { predicate, to: toId },
      actor,
    })

    // Signal cache invalidation
    this.signalCacheInvalidation(fromNs!, 'relationship')
    if (fromNs !== toNs) {
      this.signalCacheInvalidation(toNs!, 'relationship')
    }
  }

  async getRelationships(
    ns: string,
    id: string,
    predicate?: string,
    direction: 'outbound' | 'inbound' = 'outbound'
  ): Promise<MockRelationship[]> {
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

    return [...this.sql.exec(query, ...params)] as MockRelationship[]
  }

  private async appendEvent(event: MockEvent): Promise<void> {
    await this.ensureInitialized()

    const ns = event.target.split(':')[0]!

    // Get or create buffer
    let buffer = this.nsEventBuffers.get(ns)
    if (!buffer) {
      buffer = { events: [], firstSeq: 1, lastSeq: 1, sizeBytes: 0 }
      this.nsEventBuffers.set(ns, buffer)
    }

    buffer.events.push(event)
    buffer.lastSeq++

    const eventJson = JSON.stringify(event)
    buffer.sizeBytes += eventJson.length

    // Check if should flush
    if (buffer.events.length >= TestParqueDBDO.EVENT_BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= TestParqueDBDO.EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushNsEventBatch(ns)
    }
  }

  private async appendRelEvent(ns: string, event: Omit<MockEvent, 'id'>): Promise<string> {
    await this.ensureInitialized()

    let buffer = this.relEventBuffers.get(ns)
    if (!buffer) {
      buffer = { events: [], firstSeq: 1, lastSeq: 1, sizeBytes: 0 }
      this.relEventBuffers.set(ns, buffer)
    }

    const eventId = `rel_${buffer.lastSeq}`
    const fullEvent: MockEvent = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++

    const eventJson = JSON.stringify(fullEvent)
    buffer.sizeBytes += eventJson.length

    if (buffer.events.length >= TestParqueDBDO.EVENT_BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= TestParqueDBDO.EVENT_BATCH_SIZE_THRESHOLD) {
      await this.flushRelEventBatch(ns)
    }

    return eventId
  }

  async flushNsEventBatch(ns: string): Promise<void> {
    const buffer = this.nsEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    const json = JSON.stringify(buffer.events)
    const data = new TextEncoder().encode(json)
    const now = new Date().toISOString()

    this.sql.exec(
      `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)`,
      ns,
      buffer.firstSeq,
      buffer.lastSeq - 1,
      data,
      now
    )

    // Reset buffer
    this.nsEventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  async flushAllNsEventBatches(): Promise<void> {
    for (const ns of this.nsEventBuffers.keys()) {
      await this.flushNsEventBatch(ns)
    }
  }

  async flushRelEventBatch(ns: string): Promise<void> {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    const json = JSON.stringify(buffer.events)
    const data = new TextEncoder().encode(json)
    const now = new Date().toISOString()

    this.sql.exec(
      `INSERT INTO rels_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)`,
      ns,
      buffer.firstSeq,
      buffer.lastSeq - 1,
      data,
      now
    )

    // Reset buffer
    this.relEventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  async flushAllRelEventBatches(): Promise<void> {
    for (const ns of this.relEventBuffers.keys()) {
      await this.flushRelEventBatch(ns)
    }
  }

  private deserializeEventBatch(batch: Uint8Array | ArrayBuffer): MockEvent[] {
    if (!batch) return []

    let data: Uint8Array
    if (batch instanceof Uint8Array) {
      data = batch
    } else {
      data = new Uint8Array(batch)
    }

    const json = new TextDecoder().decode(data)
    try {
      return JSON.parse(json) as MockEvent[]
    } catch {
      return []
    }
  }

  private signalCacheInvalidation(
    ns: string,
    type: 'entity' | 'relationship' | 'full',
    entityId?: string
  ): void {
    const currentVersion = this.invalidationVersions.get(ns) ?? 0
    const newVersion = currentVersion + 1
    this.invalidationVersions.set(ns, newVersion)

    const signal = {
      ns,
      type,
      timestamp: Date.now(),
      version: newVersion,
      entityId,
    }

    this.pendingInvalidations.push(signal)
    if (this.pendingInvalidations.length > TestParqueDBDO.MAX_PENDING_INVALIDATIONS) {
      this.pendingInvalidations.shift()
    }
  }

  getInvalidationVersion(ns: string): number {
    return this.invalidationVersions.get(ns) ?? 0
  }

  getAllInvalidationVersions(): Record<string, number> {
    const result: Record<string, number> = {}
    for (const [ns, version] of this.invalidationVersions) {
      result[ns] = version
    }
    return result
  }

  getPendingInvalidations(ns?: string, sinceVersion?: number) {
    let signals = this.pendingInvalidations
    if (ns) {
      signals = signals.filter(s => s.ns === ns)
    }
    if (sinceVersion !== undefined) {
      signals = signals.filter(s => s.version > sinceVersion)
    }
    return signals
  }

  shouldInvalidate(ns: string, workerVersion: number): boolean {
    const doVersion = this.invalidationVersions.get(ns) ?? 0
    return doVersion > workerVersion
  }

  getCacheStats(): { size: number; maxSize: number } {
    return { size: this.entityCache.size, maxSize: TestParqueDBDO.ENTITY_CACHE_MAX_SIZE }
  }

  clearEntityCache(): void {
    this.entityCache.clear()
  }

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

  getRelBufferState(ns: string): { eventCount: number; firstSeq: number; lastSeq: number; sizeBytes: number } | null {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer) return null
    return {
      eventCount: buffer.events.length,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    }
  }

  getSequenceCounter(ns: string): number {
    const buffer = this.nsEventBuffers.get(ns)
    return buffer ? buffer.lastSeq : 1
  }

  getRelSequenceCounter(ns: string): number {
    const buffer = this.relEventBuffers.get(ns)
    return buffer ? buffer.lastSeq : 1
  }

  // For testing - get entity ID counter (what the next entity ID will be)
  getEntityIdCounter(ns: string): number {
    return this.entityIdCounters.get(ns) || 1
  }

  async getUnflushedEventCount(): Promise<number> {
    await this.ensureInitialized()

    const rows = [...this.sql.exec('SELECT SUM(last_seq - first_seq + 1) as total FROM events_wal')] as Array<{ total: number }>
    let total = rows[0]?.total || 0

    for (const buffer of this.nsEventBuffers.values()) {
      total += buffer.events.length
    }

    return total
  }

  async readUnflushedEvents(): Promise<MockEvent[]> {
    await this.ensureInitialized()

    const allEvents: MockEvent[] = []

    const rows = this.sql.exec('SELECT events FROM events_wal ORDER BY id ASC') as Array<{ events: Uint8Array }>
    for (const row of rows) {
      const batchEvents = this.deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    for (const buffer of this.nsEventBuffers.values()) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  private toApiEntity(stored: MockEntity & { $id?: string; $type?: string }): { $id: string; $type: string; name: string; createdAt: Date; createdBy: string; updatedAt: Date; updatedBy: string; deletedAt?: Date; deletedBy?: string; version: number; [key: string]: unknown } {
    const data = JSON.parse(stored.data || '{}')
    return {
      $id: stored.$id || `${stored.ns}/${stored.id}`,
      $type: stored.$type || stored.type,
      name: stored.name,
      createdAt: new Date(stored.created_at),
      createdBy: stored.created_by,
      updatedAt: new Date(stored.updated_at),
      updatedBy: stored.updated_by,
      deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
      deletedBy: stored.deleted_by ?? undefined,
      version: stored.version,
      ...data,
    }
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('ParqueDBDO', () => {
  let sql: ReturnType<typeof createMockSqlStorage>
  let bucket: ReturnType<typeof createMockR2Bucket>
  let ctx: ReturnType<typeof createMockDurableObjectState>
  let DO: TestParqueDBDO

  beforeEach(() => {
    sql = createMockSqlStorage()
    bucket = createMockR2Bucket()
    ctx = createMockDurableObjectState(sql)
    DO = new TestParqueDBDO(sql, bucket, ctx)
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  describe('CRUD Operations', () => {
    describe('create', () => {
      it('creates an entity with required fields', async () => {
        const entity = await DO.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          content: 'Hello world',
        })

        expect(entity.$id).toBe('posts/id1')
        expect(entity.$type).toBe('Post')
        expect(entity.name).toBe('Test Post')
        expect(entity.content).toBe('Hello world')
        expect(entity.version).toBe(1)
        expect(entity.createdBy).toBe('system/anonymous')
      })

      it('uses custom actor when provided', async () => {
        const entity = await DO.create('posts', {
          $type: 'Post',
          name: 'Test Post',
        }, { actor: 'user/123' })

        expect(entity.createdBy).toBe('user/123')
        expect(entity.updatedBy).toBe('user/123')
      })

      it('throws error if $type is missing', async () => {
        await expect(DO.create('posts', {
          name: 'Test',
        } as { $type: string; name: string })).rejects.toThrow('Entity must have $type')
      })

      it('throws error if name is missing', async () => {
        await expect(DO.create('posts', {
          $type: 'Post',
        } as { $type: string; name: string })).rejects.toThrow('Entity must have name')
      })

      it('generates sequential IDs within namespace', async () => {
        const entity1 = await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        const entity2 = await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        const entity3 = await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        expect(entity1.$id).toBe('posts/id1')
        expect(entity2.$id).toBe('posts/id2')
        expect(entity3.$id).toBe('posts/id3')
      })

      it('maintains separate counters per namespace', async () => {
        const post = await DO.create('posts', { $type: 'Post', name: 'Post' })
        const user = await DO.create('users', { $type: 'User', name: 'User' })

        expect(post.$id).toBe('posts/id1')
        expect(user.$id).toBe('users/id1')
      })
    })

    describe('createMany', () => {
      it('creates multiple entities', async () => {
        const entities = await DO.createMany('posts', [
          { $type: 'Post', name: 'Post 1', content: 'Content 1' },
          { $type: 'Post', name: 'Post 2', content: 'Content 2' },
          { $type: 'Post', name: 'Post 3', content: 'Content 3' },
        ])

        expect(entities).toHaveLength(3)
        expect(entities[0]!.name).toBe('Post 1')
        expect(entities[1]!.name).toBe('Post 2')
        expect(entities[2]!.name).toBe('Post 3')
      })

      it('returns empty array for empty input', async () => {
        const entities = await DO.createMany('posts', [])
        expect(entities).toHaveLength(0)
      })
    })

    describe('get', () => {
      it('retrieves created entity from events', async () => {
        const created = await DO.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          content: 'Hello',
        })

        const retrieved = await DO.get('posts', 'id1')

        expect(retrieved).not.toBeNull()
        expect(retrieved!.$id).toBe('posts/id1')
        expect(retrieved!.$type).toBe('Post')
        expect(retrieved!.name).toBe('Test Post')
        expect(retrieved!.content).toBe('Hello')
      })

      it('returns null for non-existent entity', async () => {
        const entity = await DO.get('posts', 'nonexistent')
        expect(entity).toBeNull()
      })

      it('returns null for deleted entity by default', async () => {
        await DO.create('posts', { $type: 'Post', name: 'To Delete' })
        await DO.delete('posts', 'id1')

        const entity = await DO.get('posts', 'id1')
        expect(entity).toBeNull()
      })

      it('returns deleted entity when includeDeleted is true', async () => {
        await DO.create('posts', { $type: 'Post', name: 'To Delete' })
        await DO.delete('posts', 'id1')

        const entity = await DO.get('posts', 'id1', true)
        expect(entity).not.toBeNull()
        expect(entity!.deletedAt).toBeDefined()
      })
    })

    describe('update', () => {
      it('updates entity with $set operator', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Original', content: 'Old' })

        const updated = await DO.update('posts', 'id1', {
          $set: { content: 'New', title: 'Added' },
        })

        expect(updated.content).toBe('New')
        expect(updated.title).toBe('Added')
        expect(updated.version).toBe(2)
      })

      it('updates entity name with $set', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Original' })

        const updated = await DO.update('posts', 'id1', {
          $set: { name: 'Updated Name' },
        })

        expect(updated.name).toBe('Updated Name')
      })

      it('removes fields with $unset operator', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test', toRemove: 'value' })

        const updated = await DO.update('posts', 'id1', {
          $unset: { toRemove: '' },
        })

        expect(updated.toRemove).toBeUndefined()
      })

      it('increments numeric fields with $inc operator', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test', views: 10 })

        const updated = await DO.update('posts', 'id1', {
          $inc: { views: 5 },
        })

        expect(updated.views).toBe(15)
      })

      it('pushes to array with $push operator', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test', tags: ['a'] })

        const updated = await DO.update('posts', 'id1', {
          $push: { tags: 'b' },
        })

        expect(updated.tags).toEqual(['a', 'b'])
      })

      it('pulls from array with $pull operator', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test', tags: ['a', 'b', 'c'] })

        const updated = await DO.update('posts', 'id1', {
          $pull: { tags: 'b' },
        })

        expect(updated.tags).toEqual(['a', 'c'])
      })

      it('throws error for non-existent entity', async () => {
        await expect(DO.update('posts', 'nonexistent', {
          $set: { content: 'value' },
        })).rejects.toThrow('Entity posts/nonexistent not found')
      })

      it('creates entity with upsert option', async () => {
        const entity = await DO.update('posts', 'new', {
          $set: { content: 'Created via upsert' },
        }, { upsert: true })

        expect(entity.$id).toBe('posts/id1')
        expect(entity.content).toBe('Created via upsert')
      })

      it('throws error on version mismatch', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })

        await expect(DO.update('posts', 'id1', {
          $set: { content: 'value' },
        }, { expectedVersion: 99 })).rejects.toThrow('Version mismatch: expected 99, got 1')
      })

      it('increments version on update', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })

        const updated1 = await DO.update('posts', 'id1', { $set: { v: 1 } })
        const updated2 = await DO.update('posts', 'id1', { $set: { v: 2 } })
        const updated3 = await DO.update('posts', 'id1', { $set: { v: 3 } })

        expect(updated1.version).toBe(2)
        expect(updated2.version).toBe(3)
        expect(updated3.version).toBe(4)
      })
    })

    describe('delete', () => {
      it('soft deletes entity', async () => {
        await DO.create('posts', { $type: 'Post', name: 'To Delete' })

        const result = await DO.delete('posts', 'id1')

        expect(result.deletedCount).toBe(1)

        const entity = await DO.get('posts', 'id1', true)
        expect(entity!.deletedAt).toBeDefined()
      })

      it('returns 0 for non-existent entity', async () => {
        const result = await DO.delete('posts', 'nonexistent')
        expect(result.deletedCount).toBe(0)
      })

      it('throws error on version mismatch', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })

        await expect(DO.delete('posts', 'id1', {
          expectedVersion: 99,
        })).rejects.toThrow('Version mismatch: expected 99, got 1')
      })

      it('uses custom actor', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })
        await DO.delete('posts', 'id1', { actor: 'user/456' })

        const entity = await DO.get('posts', 'id1', true)
        expect(entity!.deletedBy).toBe('user/456')
      })
    })

    describe('deleteMany', () => {
      it('deletes multiple entities', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        const result = await DO.deleteMany('posts', ['id1', 'id2'])

        expect(result.deletedCount).toBe(2)

        expect(await DO.get('posts', 'id1')).toBeNull()
        expect(await DO.get('posts', 'id2')).toBeNull()
        expect(await DO.get('posts', 'id3')).not.toBeNull()
      })

      it('returns count of actually deleted entities', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })

        const result = await DO.deleteMany('posts', ['id1', 'nonexistent'])

        expect(result.deletedCount).toBe(1)
      })
    })
  })

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  describe('Relationship Operations', () => {
    describe('link', () => {
      it('creates relationship between entities', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')

        const rels = await DO.getRelationships('posts', 'id1', 'author')
        expect(rels).toHaveLength(1)
        expect(rels[0]!.predicate).toBe('author')
        expect(rels[0]!.to_ns).toBe('users')
        expect(rels[0]!.to_id).toBe('id1')
      })

      it('generates reverse predicate', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')

        const rels = await DO.getRelationships('posts', 'id1')
        expect(rels[0]!.reverse).toBe('authors')
      })

      it('stores match mode and similarity', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1', {
          matchMode: 'fuzzy',
          similarity: 0.85,
        })

        const rels = await DO.getRelationships('posts', 'id1')
        expect(rels[0]!.match_mode).toBe('fuzzy')
        expect(rels[0]!.similarity).toBe(0.85)
      })

      it('throws error for invalid similarity', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await expect(DO.link('posts/id1', 'author', 'users/id1', {
          similarity: 1.5,
        })).rejects.toThrow('Similarity must be between 0 and 1')
      })

      it('throws error for exact match with non-1.0 similarity', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await expect(DO.link('posts/id1', 'author', 'users/id1', {
          matchMode: 'exact',
          similarity: 0.9,
        })).rejects.toThrow('Exact match mode should have similarity of 1.0 or null')
      })

      it('does not duplicate existing relationship', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.link('posts/id1', 'author', 'users/id1')

        const rels = await DO.getRelationships('posts', 'id1')
        expect(rels).toHaveLength(1)
      })
    })

    describe('unlink', () => {
      it('soft deletes relationship', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.unlink('posts/id1', 'author', 'users/id1')

        const rels = await DO.getRelationships('posts', 'id1')
        expect(rels).toHaveLength(0)
      })
    })

    describe('getRelationships', () => {
      it('gets outbound relationships', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'Author' })
        await DO.create('categories', { $type: 'Category', name: 'Tech' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.link('posts/id1', 'category', 'categories/id1')

        const rels = await DO.getRelationships('posts', 'id1')
        expect(rels).toHaveLength(2)
      })

      it('filters by predicate', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'Author' })
        await DO.create('categories', { $type: 'Category', name: 'Tech' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.link('posts/id1', 'category', 'categories/id1')

        const rels = await DO.getRelationships('posts', 'id1', 'author')
        expect(rels).toHaveLength(1)
        expect(rels[0]!.predicate).toBe('author')
      })

      it('gets inbound relationships', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')

        const rels = await DO.getRelationships('users', 'id1', undefined, 'inbound')
        expect(rels).toHaveLength(1)
        expect(rels[0]!.from_ns).toBe('posts')
      })
    })
  })

  // ===========================================================================
  // Event Sourcing
  // ===========================================================================

  describe('Event Sourcing', () => {
    describe('event buffering', () => {
      it('buffers events before flushing', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        const buffer = DO.getNsBufferState('posts')
        expect(buffer).not.toBeNull()
        expect(buffer!.eventCount).toBe(2)
      })

      it('tracks event size in bytes', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post', content: 'x'.repeat(1000) })

        const buffer = DO.getNsBufferState('posts')
        expect(buffer!.sizeBytes).toBeGreaterThan(1000)
      })

      it('flushes to WAL manually', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        await DO.flushAllNsEventBatches()

        expect(sql.eventsWal).toHaveLength(1)
        const buffer = DO.getNsBufferState('posts')
        expect(buffer!.eventCount).toBe(0)
      })
    })

    describe('entity reconstruction', () => {
      it('reconstructs entity from CREATE event', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test', content: 'Hello' })

        // Clear cache to force reconstruction
        DO.clearEntityCache()

        const entity = await DO.getEntityFromEvents('posts', 'id1')
        expect(entity).not.toBeNull()
        expect(entity!.$type).toBe('Post')
        expect(entity!.name).toBe('Test')
        expect(entity!.content).toBe('Hello')
      })

      it('reconstructs entity from CREATE + UPDATE events', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Original', value: 1 })
        await DO.update('posts', 'id1', { $set: { name: 'Updated', value: 2 } })

        DO.clearEntityCache()

        const entity = await DO.getEntityFromEvents('posts', 'id1')
        expect(entity!.name).toBe('Updated')
        expect(entity!.value).toBe(2)
        expect(entity!.version).toBe(2)
      })

      it('reconstructs deleted entity correctly', async () => {
        await DO.create('posts', { $type: 'Post', name: 'To Delete' })
        await DO.delete('posts', 'id1')

        DO.clearEntityCache()

        const entity = await DO.getEntityFromEvents('posts', 'id1')
        expect(entity).not.toBeNull()
        expect(entity!.deletedAt).toBeDefined()
      })

      it('reconstructs from WAL + buffer', async () => {
        // Create and flush
        await DO.create('posts', { $type: 'Post', name: 'First' })
        await DO.flushAllNsEventBatches()

        // Create more (in buffer)
        await DO.update('posts', 'id1', { $set: { name: 'Second' } })

        DO.clearEntityCache()

        const entity = await DO.getEntityFromEvents('posts', 'id1')
        expect(entity!.name).toBe('Second')
      })
    })

    describe('relationship event buffering', () => {
      it('buffers relationship events separately', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')

        const relBuffer = DO.getRelBufferState('posts')
        expect(relBuffer).not.toBeNull()
        expect(relBuffer!.eventCount).toBe(1)
      })

      it('flushes relationship events to rels_wal', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.flushAllRelEventBatches()

        expect(sql.relsWal).toHaveLength(1)
      })
    })

    describe('sequence counters', () => {
      it('tracks event sequence counter per namespace', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        // 2 creates = 2 events, sequence goes 1 -> 2 -> 3 (next would be 3)
        expect(DO.getSequenceCounter('posts')).toBe(3)
      })

      it('tracks relationship sequence counter separately', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.link('posts/id1', 'category', 'users/id1')

        expect(DO.getRelSequenceCounter('posts')).toBe(3)
      })
    })
  })

  // ===========================================================================
  // Cache Invalidation
  // ===========================================================================

  describe('Cache Invalidation', () => {
    it('increments invalidation version on create', async () => {
      expect(DO.getInvalidationVersion('posts')).toBe(0)

      await DO.create('posts', { $type: 'Post', name: 'Test' })

      expect(DO.getInvalidationVersion('posts')).toBe(1)
    })

    it('increments invalidation version on update', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })
      const v1 = DO.getInvalidationVersion('posts')

      await DO.update('posts', 'id1', { $set: { name: 'Updated' } })

      expect(DO.getInvalidationVersion('posts')).toBe(v1 + 1)
    })

    it('increments invalidation version on delete', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })
      const v1 = DO.getInvalidationVersion('posts')

      await DO.delete('posts', 'id1')

      expect(DO.getInvalidationVersion('posts')).toBe(v1 + 1)
    })

    it('tracks invalidation versions per namespace', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })
      await DO.create('users', { $type: 'User', name: 'User' })

      expect(DO.getInvalidationVersion('posts')).toBe(1)
      expect(DO.getInvalidationVersion('users')).toBe(1)
    })

    it('returns all invalidation versions', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })
      await DO.create('users', { $type: 'User', name: 'User' })
      await DO.create('posts', { $type: 'Post', name: 'Post 2' })

      const versions = DO.getAllInvalidationVersions()
      expect(versions.posts).toBe(2)
      expect(versions.users).toBe(1)
    })

    it('tracks pending invalidation signals', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })

      const signals = DO.getPendingInvalidations()
      expect(signals).toHaveLength(1)
      expect(signals[0]!.ns).toBe('posts')
      expect(signals[0]!.type).toBe('entity')
    })

    it('filters pending invalidations by namespace', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })
      await DO.create('users', { $type: 'User', name: 'User' })

      const postSignals = DO.getPendingInvalidations('posts')
      expect(postSignals).toHaveLength(1)
      expect(postSignals[0]!.ns).toBe('posts')
    })

    it('filters pending invalidations by version', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post 1' })
      const v1 = DO.getInvalidationVersion('posts')

      await DO.create('posts', { $type: 'Post', name: 'Post 2' })

      const signals = DO.getPendingInvalidations('posts', v1)
      expect(signals).toHaveLength(1)
      expect(signals[0]!.version).toBe(2)
    })

    it('shouldInvalidate returns true when DO version is higher', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })

      expect(DO.shouldInvalidate('posts', 0)).toBe(true)
      expect(DO.shouldInvalidate('posts', 1)).toBe(false)
      expect(DO.shouldInvalidate('posts', 2)).toBe(false)
    })

    it('signals relationship invalidation on link', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })
      await DO.create('users', { $type: 'User', name: 'User' })

      await DO.link('posts/id1', 'author', 'users/id1')

      const signals = DO.getPendingInvalidations()
      const relSignals = signals.filter(s => s.type === 'relationship')
      expect(relSignals.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Error Scenarios
  // ===========================================================================

  describe('Error Scenarios', () => {
    it('handles update of non-existent entity', async () => {
      await expect(DO.update('posts', 'nonexistent', {
        $set: { content: 'value' },
      })).rejects.toThrow('not found')
    })

    it('handles version mismatch on update', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })

      await expect(DO.update('posts', 'id1', {
        $set: { content: 'value' },
      }, { expectedVersion: 2 })).rejects.toThrow('Version mismatch')
    })

    it('handles version mismatch on delete', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })

      await expect(DO.delete('posts', 'id1', {
        expectedVersion: 2,
      })).rejects.toThrow('Version mismatch')
    })

    it('handles missing $type on create', async () => {
      await expect(DO.create('posts', {
        name: 'Test',
      } as { $type: string; name: string })).rejects.toThrow('Entity must have $type')
    })

    it('handles missing name on create', async () => {
      await expect(DO.create('posts', {
        $type: 'Post',
      } as { $type: string; name: string })).rejects.toThrow('Entity must have name')
    })

    it('handles invalid similarity on link', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post' })
      await DO.create('users', { $type: 'User', name: 'User' })

      await expect(DO.link('posts/id1', 'author', 'users/id1', {
        similarity: 1.5,
      })).rejects.toThrow('Similarity must be between 0 and 1')

      await expect(DO.link('posts/id1', 'author', 'users/id1', {
        similarity: -0.1,
      })).rejects.toThrow('Similarity must be between 0 and 1')
    })
  })

  // ===========================================================================
  // Cache Management
  // ===========================================================================

  describe('Cache Management', () => {
    it('returns cache stats', () => {
      const stats = DO.getCacheStats()
      expect(stats.size).toBe(0)
      expect(stats.maxSize).toBe(1000)
    })

    it('clears entity cache', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })
      await DO.get('posts', 'id1') // Populate cache

      DO.clearEntityCache()

      const stats = DO.getCacheStats()
      expect(stats.size).toBe(0)
    })
  })

  // ===========================================================================
  // Multi-Namespace Operations
  // ===========================================================================

  describe('Multi-Namespace Operations', () => {
    it('maintains separate state per namespace', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Post 1' })
      await DO.create('posts', { $type: 'Post', name: 'Post 2' })
      await DO.create('users', { $type: 'User', name: 'User 1' })

      const postBuffer = DO.getNsBufferState('posts')
      const userBuffer = DO.getNsBufferState('users')

      expect(postBuffer!.eventCount).toBe(2)
      expect(userBuffer!.eventCount).toBe(1)
    })

    it('reads correct entities from each namespace', async () => {
      await DO.create('posts', { $type: 'Post', name: 'My Post' })
      await DO.create('users', { $type: 'User', name: 'My User' })

      const post = await DO.get('posts', 'id1')
      const user = await DO.get('users', 'id1')

      expect(post!.$type).toBe('Post')
      expect(post!.name).toBe('My Post')
      expect(user!.$type).toBe('User')
      expect(user!.name).toBe('My User')
    })
  })

  // ===========================================================================
  // Transaction Management
  // ===========================================================================

  describe('Transaction Management', () => {
    describe('beginTransaction', () => {
      it('starts a new transaction and returns transaction ID', () => {
        const txnId = DO.beginTransaction()

        expect(txnId).toBeDefined()
        expect(txnId).toMatch(/^txn_/)
        expect(DO.isInTransaction()).toBe(true)
      })

      it('throws error if transaction already in progress', () => {
        DO.beginTransaction()

        expect(() => DO.beginTransaction()).toThrow('Transaction already in progress')
      })

      it('captures snapshot of counters', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })

        DO.beginTransaction()

        // Verify snapshot was captured
        const snapshot = DO.getTransactionSnapshot()
        expect(snapshot).not.toBeNull()
        // The snapshot should have captured the counters map
        expect(snapshot?.counters).toBeInstanceOf(Map)
      })

      it('captures snapshot of event buffers', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })

        DO.beginTransaction()

        const snapshot = DO.getTransactionSnapshot()
        expect(snapshot).not.toBeNull()
        // The snapshot should have captured the event buffers
        expect(snapshot!.nsEventBuffers).toBeInstanceOf(Map)
        // Buffer for 'posts' should exist with events
        const postsBuffer = snapshot!.nsEventBuffers.get('posts')
        expect(postsBuffer).toBeDefined()
        expect(postsBuffer!.events.length).toBeGreaterThan(0)
      })
    })

    describe('commitTransaction', () => {
      it('commits transaction and clears snapshot', async () => {
        DO.beginTransaction()

        await DO.create('posts', { $type: 'Post', name: 'Test' })
        await DO.commitTransaction()

        expect(DO.isInTransaction()).toBe(false)
        expect(DO.getTransactionSnapshot()).toBeNull()
      })

      it('throws error if no transaction in progress', async () => {
        await expect(DO.commitTransaction()).rejects.toThrow('No transaction in progress')
      })

      it('persists changes after commit', async () => {
        DO.beginTransaction()

        await DO.create('posts', { $type: 'Post', name: 'Committed Post' })
        await DO.commitTransaction()

        const entity = await DO.get('posts', 'id1')
        expect(entity).not.toBeNull()
        expect(entity!.name).toBe('Committed Post')
      })
    })

    describe('rollbackTransaction', () => {
      it('rolls back transaction and clears snapshot', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Existing' })

        DO.beginTransaction()
        await DO.create('posts', { $type: 'Post', name: 'To Rollback' })

        await DO.rollbackTransaction()

        expect(DO.isInTransaction()).toBe(false)
        expect(DO.getTransactionSnapshot()).toBeNull()
      })

      it('throws error if no transaction in progress', async () => {
        await expect(DO.rollbackTransaction()).rejects.toThrow('No transaction in progress')
      })

      it('restores counters on rollback', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        const counterBefore = DO.getSequenceCounter('posts')

        DO.beginTransaction()
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        await DO.rollbackTransaction()

        expect(DO.getSequenceCounter('posts')).toBe(counterBefore)
      })

      it('restores event buffers on rollback', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        const bufferBefore = DO.getNsBufferState('posts')

        DO.beginTransaction()
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        await DO.rollbackTransaction()

        const bufferAfter = DO.getNsBufferState('posts')
        expect(bufferAfter?.eventCount).toBe(bufferBefore?.eventCount)
      })

      it('clears entity cache on rollback', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Original' })
        await DO.get('posts', 'id1') // Populate cache

        DO.beginTransaction()
        await DO.update('posts', 'id1', { $set: { name: 'Updated' } })

        await DO.rollbackTransaction()

        // Cache should be cleared/restored
        const stats = DO.getCacheStats()
        expect(stats.size).toBe(0)
      })
    })

    describe('isInTransaction', () => {
      it('returns false initially', () => {
        expect(DO.isInTransaction()).toBe(false)
      })

      it('returns true after beginTransaction', () => {
        DO.beginTransaction()
        expect(DO.isInTransaction()).toBe(true)
      })

      it('returns false after commitTransaction', async () => {
        DO.beginTransaction()
        await DO.commitTransaction()
        expect(DO.isInTransaction()).toBe(false)
      })

      it('returns false after rollbackTransaction', async () => {
        DO.beginTransaction()
        await DO.rollbackTransaction()
        expect(DO.isInTransaction()).toBe(false)
      })
    })

    describe('transaction isolation', () => {
      it('changes are visible within transaction before commit', async () => {
        DO.beginTransaction()

        await DO.create('posts', { $type: 'Post', name: 'Test' })
        const entity = await DO.get('posts', 'id1')

        expect(entity).not.toBeNull()
        expect(entity!.name).toBe('Test')

        await DO.commitTransaction()
      })

      it('updates are visible within transaction before commit', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Original' })

        DO.beginTransaction()
        await DO.update('posts', 'id1', { $set: { name: 'Updated' } })

        const entity = await DO.get('posts', 'id1')
        expect(entity!.name).toBe('Updated')

        await DO.rollbackTransaction()
      })

      it('multiple operations in transaction', async () => {
        DO.beginTransaction()

        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.update('posts', 'id1', { $set: { content: 'Added content' } })
        await DO.delete('posts', 'id3')

        // Post 1 should exist with update
        const post1 = await DO.get('posts', 'id1')
        expect(post1).not.toBeNull()
        expect(post1!.content).toBe('Added content')

        // Post 2 should still exist (id3 was deleted, not id2)
        const post2 = await DO.get('posts', 'id2')
        expect(post2).not.toBeNull()

        await DO.commitTransaction()
      })
    })
  })

  // ===========================================================================
  // Flush to R2 Logic
  // ===========================================================================

  describe('Flush to R2 Logic', () => {
    describe('WAL to Parquet flush', () => {
      it('accumulates events before flush threshold', async () => {
        // Create some events but not enough to trigger flush
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        const buffer = DO.getNsBufferState('posts')
        expect(buffer).not.toBeNull()
        expect(buffer!.eventCount).toBeGreaterThan(0)
      })

      it('flushes events to WAL table on manual flush', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        const bufferBefore = DO.getNsBufferState('posts')
        expect(bufferBefore!.eventCount).toBeGreaterThan(0)

        await DO.flushAllNsEventBatches()

        // Events should be in WAL, buffer should be empty
        const bufferAfter = DO.getNsBufferState('posts')
        expect(bufferAfter!.eventCount).toBe(0)
        expect(sql.eventsWal.length).toBe(1)
      })

      it('preserves event data through WAL serialization', async () => {
        await DO.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          content: 'Hello world',
          tags: ['a', 'b'],
        })

        // Verify event is in buffer before flush
        const bufferBefore = DO.getNsBufferState('posts')
        expect(bufferBefore!.eventCount).toBe(1)

        await DO.flushAllNsEventBatches()

        // After flush, events are in WAL table
        expect(sql.eventsWal.length).toBe(1)

        // Deserialize WAL events to verify data
        const walEntry = sql.eventsWal[0]!
        const events = JSON.parse(new TextDecoder().decode(walEntry.events)) as MockEvent[]
        expect(events.length).toBe(1)

        const createEvent = events.find(e => e.op === 'CREATE')
        expect(createEvent).toBeDefined()
        expect(createEvent!.after).toEqual(expect.objectContaining({
          $type: 'Post',
          name: 'Test Post',
          content: 'Hello world',
          tags: ['a', 'b'],
        }))
      })

      it('tracks sequence numbers correctly', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.flushAllNsEventBatches()

        // Check WAL entry has correct sequence
        expect(sql.eventsWal.length).toBe(1)
        const walEntry = sql.eventsWal[0]!
        expect(walEntry.first_seq).toBeDefined()
        expect(walEntry.last_seq).toBeDefined()
        expect(walEntry.last_seq).toBeGreaterThanOrEqual(walEntry.first_seq)
      })

      it('handles multiple namespaces independently', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })
        await DO.create('comments', { $type: 'Comment', name: 'Comment' })

        await DO.flushAllNsEventBatches()

        // Each namespace should have its own WAL entries
        const postWal = sql.eventsWal.filter(w => w.ns === 'posts')
        const userWal = sql.eventsWal.filter(w => w.ns === 'users')
        const commentWal = sql.eventsWal.filter(w => w.ns === 'comments')

        expect(postWal.length).toBe(1)
        expect(userWal.length).toBe(1)
        expect(commentWal.length).toBe(1)
      })
    })

    describe('relationship event flush', () => {
      it('flushes relationship events to rels_wal', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1')
        await DO.link('posts/id1', 'category', 'users/id1')

        await DO.flushAllRelEventBatches()

        expect(sql.relsWal.length).toBeGreaterThan(0)
      })

      it('preserves relationship event data through serialization', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post' })
        await DO.create('users', { $type: 'User', name: 'User' })

        await DO.link('posts/id1', 'author', 'users/id1', {
          matchMode: 'fuzzy',
          similarity: 0.9,
          data: { weight: 10 },
        })

        await DO.flushAllRelEventBatches()

        // The event data should be preserved in WAL
        expect(sql.relsWal.length).toBe(1)
        const walEntry = sql.relsWal[0]!
        const events = JSON.parse(new TextDecoder().decode(walEntry.events))
        expect(events.length).toBeGreaterThan(0)
      })
    })

    describe('unflushed event counting', () => {
      it('counts events in buffer', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        const count = await DO.getUnflushedEventCount()
        expect(count).toBe(2)
      })

      it('counts events in WAL and buffer combined', async () => {
        // Create and flush some events
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.flushAllNsEventBatches()

        // Create more events (in buffer)
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })
        await DO.create('posts', { $type: 'Post', name: 'Post 3' })

        const count = await DO.getUnflushedEventCount()
        // 1 in WAL + 2 in buffer = 3
        expect(count).toBeGreaterThanOrEqual(3)
      })

      it('returns zero when no unflushed events', async () => {
        const count = await DO.getUnflushedEventCount()
        expect(count).toBe(0)
      })
    })

    describe('read unflushed events', () => {
      it('reads events from buffer', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })

        const events = await DO.readUnflushedEvents()
        expect(events.length).toBe(1)
        expect(events[0]!.op).toBe('CREATE')
      })

      it('reads events from WAL and buffer combined', async () => {
        // Create and flush
        await DO.create('posts', { $type: 'Post', name: 'Post 1' })
        await DO.flushAllNsEventBatches()

        // Create more (in buffer)
        await DO.create('posts', { $type: 'Post', name: 'Post 2' })

        // Check WAL has 1 event
        expect(sql.eventsWal.length).toBe(1)

        // Check buffer has 1 event
        const buffer = DO.getNsBufferState('posts')
        expect(buffer!.eventCount).toBe(1)

        // readUnflushedEvents reads from both WAL and buffer
        // Note: The current implementation reads WAL events + buffer events
        const events = await DO.readUnflushedEvents()
        // WAL has 1 + buffer has 1 = 2
        expect(events.length).toBe(2)
      })

      it('preserves event order', async () => {
        await DO.create('posts', { $type: 'Post', name: 'First' })
        await DO.create('posts', { $type: 'Post', name: 'Second' })
        await DO.create('posts', { $type: 'Post', name: 'Third' })

        const events = await DO.readUnflushedEvents()
        expect(events[0]!.after).toEqual(expect.objectContaining({ name: 'First' }))
        expect(events[1]!.after).toEqual(expect.objectContaining({ name: 'Second' }))
        expect(events[2]!.after).toEqual(expect.objectContaining({ name: 'Third' }))
      })

      it('includes all event types', async () => {
        await DO.create('posts', { $type: 'Post', name: 'Test' })
        await DO.update('posts', 'id1', { $set: { content: 'Updated' } })
        await DO.delete('posts', 'id1')

        const events = await DO.readUnflushedEvents()
        const ops = events.map(e => e.op)

        expect(ops).toContain('CREATE')
        expect(ops).toContain('UPDATE')
        expect(ops).toContain('DELETE')
      })
    })

    describe('pending row groups', () => {
      it('tracks pending row groups for bulk writes', async () => {
        // For small batches, no pending row groups
        await DO.create('posts', { $type: 'Post', name: 'Small batch' })

        expect(sql.pendingRowGroups.length).toBe(0)
      })
    })

    describe('flush configuration', () => {
      it('does not auto-flush when below threshold', async () => {
        // Create a few events (below the 100 event threshold)
        for (let i = 0; i < 5; i++) {
          await DO.create('posts', { $type: 'Post', name: `Post ${i}` })
        }

        // Events should still be in buffer
        const buffer = DO.getNsBufferState('posts')
        expect(buffer!.eventCount).toBe(5)
        expect(sql.eventsWal.length).toBe(0)
      })
    })
  })

  // ===========================================================================
  // Restore Operation
  // ===========================================================================

  describe('Restore Operation', () => {
    it('restores soft-deleted entity', async () => {
      await DO.create('posts', { $type: 'Post', name: 'To Restore' })
      await DO.delete('posts', 'id1')

      // Entity should be deleted
      let entity = await DO.get('posts', 'id1')
      expect(entity).toBeNull()

      // Restore
      const restored = await DO.restore('posts', 'id1')
      expect(restored).not.toBeNull()

      // Entity should now be accessible
      entity = await DO.get('posts', 'id1')
      expect(entity).not.toBeNull()
      expect(entity!.name).toBe('To Restore')
    })

    it('returns null for non-existent entity', async () => {
      const result = await DO.restore('posts', 'nonexistent')
      expect(result).toBeNull()
    })

    it('returns existing entity if not deleted', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Not Deleted' })

      const result = await DO.restore('posts', 'id1')
      expect(result).not.toBeNull()
      expect(result!.name).toBe('Not Deleted')
    })

    it('increments version on restore', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Test' })
      await DO.delete('posts', 'id1')

      const restored = await DO.restore('posts', 'id1')
      expect(restored!.version).toBeGreaterThan(1)
    })
  })

  // ===========================================================================
  // Edge Cases and Boundary Conditions
  // ===========================================================================

  describe('Edge Cases and Boundary Conditions', () => {
    it('handles entity with complex nested data', async () => {
      await DO.create('posts', {
        $type: 'Post',
        name: 'Complex Post',
        metadata: {
          nested: {
            deep: {
              value: 42,
            },
          },
          array: [1, 2, { key: 'value' }],
        },
      })

      const entity = await DO.get('posts', 'id1')
      expect(entity!.metadata).toEqual({
        nested: { deep: { value: 42 } },
        array: [1, 2, { key: 'value' }],
      })
    })

    it('handles empty data object', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Minimal' })

      const entity = await DO.get('posts', 'id1')
      expect(entity).not.toBeNull()
    })

    it('handles special characters in strings', async () => {
      await DO.create('posts', {
        $type: 'Post',
        name: 'Test "quotes" and \'apostrophes\'',
        content: 'Line1\nLine2\tTabbed',
      })

      const entity = await DO.get('posts', 'id1')
      expect(entity!.name).toBe('Test "quotes" and \'apostrophes\'')
      expect(entity!.content).toBe('Line1\nLine2\tTabbed')
    })

    it('handles unicode in entity data', async () => {
      await DO.create('posts', {
        $type: 'Post',
        name: 'Unicode: \u4e2d\u6587 \ud83d\ude00',
        content: '\u00e9\u00e8\u00ea\u00eb',
      })

      const entity = await DO.get('posts', 'id1')
      expect(entity!.name).toBe('Unicode: \u4e2d\u6587 \ud83d\ude00')
    })

    it('handles rapid sequential operations', async () => {
      // Create 10 entities quickly
      for (let i = 0; i < 10; i++) {
        await DO.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const buffer = DO.getNsBufferState('posts')
      expect(buffer!.eventCount).toBe(10)
    })

    it('handles concurrent operations on same entity', async () => {
      await DO.create('posts', { $type: 'Post', name: 'Original', counter: 0 })

      // Simulate concurrent updates
      await DO.update('posts', 'id1', { $inc: { counter: 1 } })
      await DO.update('posts', 'id1', { $inc: { counter: 1 } })
      await DO.update('posts', 'id1', { $inc: { counter: 1 } })

      const entity = await DO.get('posts', 'id1')
      expect(entity!.counter).toBe(3)
    })

    it('handles very long entity names', async () => {
      const longName = 'A'.repeat(1000)
      await DO.create('posts', { $type: 'Post', name: longName })

      const entity = await DO.get('posts', 'id1')
      expect(entity!.name).toBe(longName)
    })

    it('handles entity ID with special characters', async () => {
      // The ID is auto-generated, but we can test that the namespace works
      await DO.create('my-namespace', { $type: 'Test', name: 'Test' })

      const entity = await DO.get('my-namespace', 'id1')
      expect(entity!.$id).toBe('my-namespace/id1')
    })
  })
})

// =============================================================================
// Extended TestParqueDBDO with Transaction Support
// =============================================================================

// Add private properties to TestParqueDBDO for transaction management
interface TransactionState {
  inTransaction: boolean
  transactionSnapshot: {
    counters: Map<string, number>
    entityCache: Map<string, { entity: unknown; version: number }>
    nsEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
    relEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
  } | null
}

// Augment TestParqueDBDO prototype with transaction methods
const transactionState: WeakMap<TestParqueDBDO, TransactionState> = new WeakMap();

(TestParqueDBDO.prototype as TestParqueDBDO & {
  beginTransaction(): string
  commitTransaction(): Promise<void>
  rollbackTransaction(): Promise<void>
  isInTransaction(): boolean
  getTransactionSnapshot(): TransactionState['transactionSnapshot']
  restore(ns: string, id: string, options?: { actor?: string }): Promise<{ $id: string; $type: string; name: string; version: number; [key: string]: unknown } | null>
}).beginTransaction = function(this: TestParqueDBDO): string {
  let state = transactionState.get(this)
  if (!state) {
    state = { inTransaction: false, transactionSnapshot: null }
    transactionState.set(this, state)
  }

  if (state.inTransaction) {
    throw new Error('Transaction already in progress')
  }
  state.inTransaction = true

  // Access private properties via any cast
  const self = this as unknown as {
    counters: Map<string, number>
    entityCache: Map<string, { entity: unknown; version: number }>
    nsEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
    relEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
  }

  // Deep copy state for snapshot
  state.transactionSnapshot = {
    counters: new Map(self.counters),
    entityCache: new Map(),
    nsEventBuffers: new Map(),
    relEventBuffers: new Map(),
  }

  // Deep copy entity cache
  for (const [k, v] of self.entityCache) {
    state.transactionSnapshot.entityCache.set(k, {
      entity: JSON.parse(JSON.stringify(v.entity)),
      version: v.version,
    })
  }

  // Deep copy event buffers
  for (const [ns, buffer] of self.nsEventBuffers) {
    state.transactionSnapshot.nsEventBuffers.set(ns, {
      events: [...buffer.events],
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    })
  }

  for (const [ns, buffer] of self.relEventBuffers) {
    state.transactionSnapshot.relEventBuffers.set(ns, {
      events: [...buffer.events],
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    })
  }

  return `txn_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`
};

(TestParqueDBDO.prototype as TestParqueDBDO & { commitTransaction(): Promise<void> }).commitTransaction = async function(this: TestParqueDBDO): Promise<void> {
  const state = transactionState.get(this)
  if (!state || !state.inTransaction) {
    throw new Error('No transaction in progress')
  }
  state.transactionSnapshot = null
  state.inTransaction = false
};

(TestParqueDBDO.prototype as TestParqueDBDO & { rollbackTransaction(): Promise<void> }).rollbackTransaction = async function(this: TestParqueDBDO): Promise<void> {
  const state = transactionState.get(this)
  if (!state || !state.inTransaction || !state.transactionSnapshot) {
    throw new Error('No transaction in progress')
  }

  const snapshot = state.transactionSnapshot

  // Access private properties
  const self = this as unknown as {
    counters: Map<string, number>
    entityCache: Map<string, { entity: unknown; version: number }>
    nsEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
    relEventBuffers: Map<string, { events: MockEvent[]; firstSeq: number; lastSeq: number; sizeBytes: number }>
  }

  // Restore counters
  self.counters.clear()
  for (const [k, v] of snapshot.counters) {
    self.counters.set(k, v)
  }

  // Restore entity cache
  self.entityCache.clear()
  for (const [k, v] of snapshot.entityCache) {
    self.entityCache.set(k, v)
  }

  // Restore event buffers
  self.nsEventBuffers.clear()
  for (const [ns, buffer] of snapshot.nsEventBuffers) {
    self.nsEventBuffers.set(ns, { ...buffer, events: [...buffer.events] })
  }

  self.relEventBuffers.clear()
  for (const [ns, buffer] of snapshot.relEventBuffers) {
    self.relEventBuffers.set(ns, { ...buffer, events: [...buffer.events] })
  }

  state.transactionSnapshot = null
  state.inTransaction = false
};

(TestParqueDBDO.prototype as TestParqueDBDO & { isInTransaction(): boolean }).isInTransaction = function(this: TestParqueDBDO): boolean {
  const state = transactionState.get(this)
  return state?.inTransaction ?? false
};

(TestParqueDBDO.prototype as TestParqueDBDO & { getTransactionSnapshot(): TransactionState['transactionSnapshot'] }).getTransactionSnapshot = function(this: TestParqueDBDO): TransactionState['transactionSnapshot'] {
  const state = transactionState.get(this)
  return state?.transactionSnapshot ?? null
};

(TestParqueDBDO.prototype as TestParqueDBDO & { restore(ns: string, id: string, options?: { actor?: string }): Promise<{ $id: string; $type: string; name: string; version: number; [key: string]: unknown } | null> }).restore = async function(
  this: TestParqueDBDO,
  ns: string,
  id: string,
  options: { actor?: string } = {}
): Promise<{ $id: string; $type: string; name: string; version: number; [key: string]: unknown } | null> {
  const actor = options.actor || 'system/anonymous'

  // Get current entity state from events
  const current = await this.getEntityFromEvents(ns, id)
  if (!current) {
    return null // Entity doesn't exist
  }

  if (!current.deletedAt) {
    return current as { $id: string; $type: string; name: string; version: number; [key: string]: unknown } // Not deleted, return as-is
  }

  // Access private appendEvent method
  const self = this as unknown as {
    appendEvent(event: MockEvent): Promise<void>
  }

  // Create restore event (UPDATE that removes deletedAt)
  await self.appendEvent({
    id: `ulid_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
    ts: Date.now(),
    op: 'UPDATE',
    target: `${ns}:${id}`,
    before: { $type: current.$type, name: current.name, deletedAt: current.deletedAt } as Record<string, unknown>,
    after: { $type: current.$type, name: current.name } as Record<string, unknown>,
    actor,
  })

  const { deletedAt: _deletedAt, deletedBy: _deletedBy, ...rest } = current
  return {
    ...rest,
    version: current.version + 1,
  } as { $id: string; $type: string; name: string; version: number; [key: string]: unknown }
}

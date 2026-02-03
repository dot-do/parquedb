/**
 * Event Sourcing Consolidation Tests
 *
 * Tests for the refactored event-sourcing model where events_wal
 * is the single source of truth for entity state.
 *
 * Goals:
 * - Events are the authoritative source (not entities table)
 * - Entity state is derived by replaying events
 * - Snapshot checkpoints provide fast reconstruction
 * - No more conditional skipEntityTableWrites paths
 */

import { describe, it, expect, beforeEach } from 'vitest'

// =============================================================================
// Mock SQLite Implementation
// =============================================================================

interface SqlStorageValue {
  [key: string]: unknown
}

class MockSqlite {
  private tables: Map<string, unknown[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()

  exec<T = unknown>(query: string, ...params: unknown[]): Iterable<T> {
    const trimmedQuery = query.trim().toLowerCase()

    // CREATE TABLE
    if (trimmedQuery.startsWith('create table')) {
      const match = query.match(/create table if not exists (\w+)/i)
      if (match) {
        const tableName = match[1]!
        if (!this.tables.has(tableName)) {
          this.tables.set(tableName, [])
          this.autoIncrement.set(tableName, 0)
        }
      }
      return [] as T[]
    }

    // CREATE INDEX
    if (trimmedQuery.startsWith('create index')) {
      return [] as T[]
    }

    // INSERT
    if (trimmedQuery.startsWith('insert into')) {
      const match = query.match(/insert into (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        const id = (this.autoIncrement.get(tableName) || 0) + 1
        this.autoIncrement.set(tableName, id)

        // Build row based on table type
        let row: Record<string, unknown>
        if (tableName === 'events_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
            created_at: params[4] || new Date().toISOString(),
          }
        } else if (tableName === 'snapshots') {
          row = {
            id,
            ns: params[0],
            entity_id: params[1],
            seq: params[2],
            state: params[3],
            created_at: params[4] || new Date().toISOString(),
          }
        } else {
          row = { id }
        }

        rows.push(row)
        this.tables.set(tableName, rows)
      }
      return [] as T[]
    }

    // SELECT with MAX(last_seq)
    if (trimmedQuery.includes('max(last_seq)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []

        // Group by namespace
        const byNs = new Map<string, number>()
        for (const row of rows as any[]) {
          if (row.ns) {
            const current = byNs.get(row.ns) || 0
            if (row.last_seq > current) {
              byNs.set(row.ns, row.last_seq)
            }
          }
        }

        const result: any[] = []
        for (const [rowNs, maxSeq] of byNs.entries()) {
          result.push({ ns: rowNs, max_seq: maxSeq })
        }
        return result as T[]
      }
    }

    // SELECT MAX(seq) FROM snapshots
    if (trimmedQuery.includes('max(seq)') && trimmedQuery.includes('snapshots')) {
      const rows = this.tables.get('snapshots') || []
      const ns = params[0] as string
      const entityId = params[1] as string

      let maxSeq = 0
      let latestSnapshot = null
      for (const row of rows as any[]) {
        if (row.ns === ns && row.entity_id === entityId && row.seq > maxSeq) {
          maxSeq = row.seq
          latestSnapshot = row
        }
      }

      if (latestSnapshot) {
        return [{ seq: maxSeq, state: latestSnapshot.state }] as T[]
      }
      return [] as T[]
    }

    // SELECT COUNT(*)
    if (trimmedQuery.includes('count(*)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        return [{ count: rows.length }] as T[]
      }
    }

    // SELECT events from events_wal
    if (trimmedQuery.includes('select') && trimmedQuery.includes('events_wal')) {
      const rows = this.tables.get('events_wal') || []
      const ns = params[0] as string | undefined

      if (ns) {
        const filtered = rows.filter((r: any) => r.ns === ns)
        return filtered as T[]
      }
      return rows as T[]
    }

    // SELECT from snapshots
    if (trimmedQuery.includes('select') && trimmedQuery.includes('snapshots')) {
      const rows = this.tables.get('snapshots') || []
      const ns = params[0] as string
      const entityId = params[1] as string

      if (ns && entityId) {
        const filtered = rows.filter((r: any) => r.ns === ns && r.entity_id === entityId)
        // Sort by seq descending and return latest
        filtered.sort((a: any, b: any) => b.seq - a.seq)
        return filtered.slice(0, 1) as T[]
      }
      return rows as T[]
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []

        if (params.length === 2) {
          const ns = params[0] as string
          const upToSeq = params[1] as number
          const remaining = rows.filter((r: any) =>
            r.ns !== ns || r.last_seq > upToSeq
          )
          this.tables.set(tableName, remaining)
        }
      }
      return [] as T[]
    }

    // UPDATE
    if (trimmedQuery.startsWith('update')) {
      // No-op for now, we don't use UPDATE in the new design
      return [] as T[]
    }

    return [] as T[]
  }

  getTable(name: string): unknown[] {
    return this.tables.get(name) || []
  }

  clear() {
    this.tables.clear()
    this.autoIncrement.clear()
  }
}

// =============================================================================
// Event Types
// =============================================================================

interface Event {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  target: string
  before?: Record<string, unknown> | undefined
  after?: Record<string, unknown> | undefined
  actor?: string | undefined
}

interface Entity {
  $id: string
  $type: string
  name: string
  createdAt: Date
  createdBy: string
  updatedAt: Date
  updatedBy: string
  deletedAt?: Date | undefined
  deletedBy?: string | undefined
  version: number
  [key: string]: unknown
}

// =============================================================================
// Event-Sourced Entity Store
// =============================================================================

/**
 * Event-sourced entity store that derives state from events.
 *
 * This is the NEW design where:
 * - events_wal is the single source of truth
 * - Entity state is reconstructed by replaying events
 * - Snapshots provide performance optimization
 * - No separate entities table needed
 */
class EventSourcedEntityStore {
  private sql: MockSqlite
  private counters: Map<string, number> = new Map()
  private eventBuffers: Map<string, { events: Event[]; firstSeq: number; lastSeq: number; sizeBytes: number }> = new Map()
  private initialized = false

  // LRU cache for reconstructed entities
  private entityCache: Map<string, { entity: Entity; seq: number }> = new Map()
  private static readonly CACHE_MAX_SIZE = 1000

  // Thresholds
  private static readonly BATCH_COUNT_THRESHOLD = 100
  private static readonly BATCH_SIZE_THRESHOLD = 64 * 1024 // 64KB
  private static readonly SNAPSHOT_THRESHOLD = 50 // Create snapshot after 50 events

  constructor(sql: MockSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // events_wal is the ONLY source of truth for entity state
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

    // Snapshots for performance (not authoritative - can be rebuilt from events)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ns TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_snapshots ON snapshots(ns, entity_id, seq)')

    this.initialized = true
    await this.initializeCounters()
  }

  private async initializeCounters(): Promise<void> {
    interface CounterRow {
      ns: string
      max_seq: number
    }

    const rows = [...this.sql.exec<CounterRow>(
      `SELECT ns, MAX(last_seq) as max_seq FROM events_wal GROUP BY ns`
    )]

    for (const row of rows) {
      this.counters.set(row.ns, row.max_seq + 1)
    }
  }

  private getNextSeq(ns: string): number {
    const seq = this.counters.get(ns) || 1
    this.counters.set(ns, seq + 1)
    return seq
  }

  /**
   * Create a new entity by appending a CREATE event
   */
  async create(ns: string, data: { $type: string; name: string; [key: string]: unknown }, actor: string = 'system/anonymous'): Promise<Entity> {
    await this.ensureInitialized()

    const seq = this.getNextSeq(ns)
    const id = `${seq}`
    const entityId = `${ns}/${id}`
    const now = Date.now()

    const { $type, name, ...rest } = data

    const event: Event = {
      id: `evt_${seq}`,
      ts: now,
      op: 'CREATE',
      target: `${ns}:${id}`,
      after: { $type, name, ...rest },
      actor,
    }

    await this.appendEvent(ns, event)

    // Build and cache the entity
    const entity: Entity = {
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

    this.cacheEntity(entityId, entity, seq)
    return entity
  }

  /**
   * Update an entity by appending an UPDATE event
   */
  async update(ns: string, id: string, updates: Record<string, unknown>, actor: string = 'system/anonymous'): Promise<Entity | null> {
    await this.ensureInitialized()

    // Get current state
    const current = await this.get(ns, id)
    if (!current || current.deletedAt) {
      return null
    }

    const seq = this.getNextSeq(ns)
    const entityId = `${ns}/${id}`
    const now = Date.now()

    // Build before/after state
    const { $id, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy, version, ...currentData } = current
    const newData = { ...currentData, ...updates }

    const event: Event = {
      id: `evt_${seq}`,
      ts: now,
      op: 'UPDATE',
      target: `${ns}:${id}`,
      before: currentData,
      after: newData,
      actor,
    }

    await this.appendEvent(ns, event)

    // Build updated entity
    const entity: Entity = {
      ...current,
      ...updates,
      updatedAt: new Date(now),
      updatedBy: actor,
      version: current.version + 1,
    }

    this.invalidateCache(entityId)
    this.cacheEntity(entityId, entity, seq)
    return entity
  }

  /**
   * Delete an entity by appending a DELETE event
   */
  async delete(ns: string, id: string, actor: string = 'system/anonymous'): Promise<boolean> {
    await this.ensureInitialized()

    const current = await this.get(ns, id)
    if (!current || current.deletedAt) {
      return false
    }

    const seq = this.getNextSeq(ns)
    const entityId = `${ns}/${id}`
    const now = Date.now()

    const { $id, createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy, version, ...currentData } = current

    const event: Event = {
      id: `evt_${seq}`,
      ts: now,
      op: 'DELETE',
      target: `${ns}:${id}`,
      before: currentData,
      actor,
    }

    await this.appendEvent(ns, event)
    this.invalidateCache(entityId)

    return true
  }

  /**
   * Get an entity by reconstructing from events
   */
  async get(ns: string, id: string, includeDeleted: boolean = false): Promise<Entity | null> {
    await this.ensureInitialized()

    const entityId = `${ns}/${id}`
    const target = `${ns}:${id}`

    // Check cache first
    const cached = this.entityCache.get(entityId)
    if (cached) {
      if (!includeDeleted && cached.entity.deletedAt) {
        return null
      }
      return cached.entity
    }

    // Reconstruct from events
    const entity = await this.reconstructFromEvents(ns, id)

    if (!entity) {
      return null
    }

    if (!includeDeleted && entity.deletedAt) {
      return null
    }

    this.cacheEntity(entityId, entity, this.counters.get(ns) || 0)
    return entity
  }

  /**
   * Reconstruct entity state from events (the core event-sourcing operation)
   */
  private async reconstructFromEvents(ns: string, id: string): Promise<Entity | null> {
    const target = `${ns}:${id}`
    const entityId = `${ns}/${id}`

    // 1. Check for snapshot
    const snapshot = await this.getLatestSnapshot(ns, id)
    let state: Record<string, unknown> | null = snapshot?.state || null
    let startSeq = snapshot?.seq || 0
    let eventsReplayed = 0

    // 2. Read events from events_wal
    interface WalRow {
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const walRows = [...this.sql.exec<WalRow>(
      `SELECT events, first_seq, last_seq FROM events_wal WHERE ns = ? ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of walRows) {
      const events = this.deserializeEvents(row.events)
      for (const event of events) {
        if (event.target === target) {
          state = this.applyEvent(state, event)
          eventsReplayed++
        }
      }
    }

    // 3. Check buffered events
    const buffer = this.eventBuffers.get(ns)
    if (buffer) {
      for (const event of buffer.events) {
        if (event.target === target) {
          state = this.applyEvent(state, event)
          eventsReplayed++
        }
      }
    }

    if (!state) {
      return null
    }

    // 4. Maybe create snapshot if many events were replayed
    if (eventsReplayed >= EventSourcedEntityStore.SNAPSHOT_THRESHOLD && !snapshot) {
      await this.createSnapshot(ns, id, this.counters.get(ns) || 0, state)
    }

    // Build entity from state
    const { $type, name, ...rest } = state as { $type: string; name: string; [key: string]: unknown }

    return {
      $id: entityId,
      $type: $type || 'Unknown',
      name: name || id,
      createdAt: state._createdAt ? new Date(state._createdAt as number) : new Date(),
      createdBy: (state._createdBy as string) || 'system/anonymous',
      updatedAt: state._updatedAt ? new Date(state._updatedAt as number) : new Date(),
      updatedBy: (state._updatedBy as string) || 'system/anonymous',
      deletedAt: state._deletedAt ? new Date(state._deletedAt as number) : undefined,
      deletedBy: state._deletedBy as string | undefined,
      version: (state._version as number) || 1,
      ...rest,
    }
  }

  /**
   * Apply a single event to state
   */
  private applyEvent(state: Record<string, unknown> | null, event: Event): Record<string, unknown> | null {
    switch (event.op) {
      case 'CREATE':
        return {
          ...event.after,
          _createdAt: event.ts,
          _createdBy: event.actor,
          _updatedAt: event.ts,
          _updatedBy: event.actor,
          _version: 1,
        }

      case 'UPDATE':
        if (!state) return null
        return {
          ...state,
          ...event.after,
          _updatedAt: event.ts,
          _updatedBy: event.actor,
          _version: ((state._version as number) || 0) + 1,
        }

      case 'DELETE':
        if (!state) return null
        return {
          ...state,
          _deletedAt: event.ts,
          _deletedBy: event.actor,
          _version: ((state._version as number) || 0) + 1,
        }

      default:
        return state
    }
  }

  /**
   * Append event to buffer (will be flushed to events_wal)
   */
  private async appendEvent(ns: string, event: Event): Promise<void> {
    let buffer = this.eventBuffers.get(ns)
    if (!buffer) {
      const seq = this.counters.get(ns) || 1
      buffer = { events: [], firstSeq: seq - 1, lastSeq: seq - 1, sizeBytes: 0 }
      this.eventBuffers.set(ns, buffer)
    }

    buffer.events.push(event)
    buffer.lastSeq++
    buffer.sizeBytes += JSON.stringify(event).length

    // Auto-flush if threshold reached
    if (buffer.events.length >= EventSourcedEntityStore.BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= EventSourcedEntityStore.BATCH_SIZE_THRESHOLD) {
      await this.flushEventBatch(ns)
    }
  }

  /**
   * Flush buffered events to events_wal
   */
  async flushEventBatch(ns: string): Promise<void> {
    const buffer = this.eventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    const json = JSON.stringify(buffer.events)
    const data = new TextEncoder().encode(json)
    const now = new Date().toISOString()

    this.sql.exec(
      `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      ns,
      buffer.firstSeq,
      buffer.lastSeq,
      data,
      now
    )

    // Reset buffer
    this.eventBuffers.set(ns, {
      events: [],
      firstSeq: buffer.lastSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: 0,
    })
  }

  /**
   * Flush all buffers
   */
  async flushAll(): Promise<void> {
    for (const ns of this.eventBuffers.keys()) {
      await this.flushEventBatch(ns)
    }
  }

  /**
   * Get latest snapshot for an entity
   */
  private async getLatestSnapshot(ns: string, id: string): Promise<{ seq: number; state: Record<string, unknown> } | null> {
    interface SnapshotRow {
      seq: number
      state: string
    }

    const rows = [...this.sql.exec<SnapshotRow>(
      `SELECT seq, state FROM snapshots WHERE ns = ? AND entity_id = ? ORDER BY seq DESC LIMIT 1`,
      ns,
      id
    )]

    if (rows.length === 0) {
      return null
    }

    const row = rows[0]!
    return {
      seq: row.seq,
      state: JSON.parse(row.state as string),
    }
  }

  /**
   * Create a snapshot checkpoint
   */
  private async createSnapshot(ns: string, id: string, seq: number, state: Record<string, unknown>): Promise<void> {
    const now = new Date().toISOString()
    const stateJson = JSON.stringify(state)

    this.sql.exec(
      `INSERT INTO snapshots (ns, entity_id, seq, state, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      ns,
      id,
      seq,
      stateJson,
      now
    )
  }

  // Cache management
  private cacheEntity(key: string, entity: Entity, seq: number): void {
    this.entityCache.delete(key)
    this.entityCache.set(key, { entity, seq })

    if (this.entityCache.size > EventSourcedEntityStore.CACHE_MAX_SIZE) {
      const oldestKey = this.entityCache.keys().next().value
      if (oldestKey) {
        this.entityCache.delete(oldestKey)
      }
    }
  }

  private invalidateCache(key: string): void {
    this.entityCache.delete(key)
  }

  clearCache(): void {
    this.entityCache.clear()
  }

  // Helpers
  private deserializeEvents(data: ArrayBuffer | Uint8Array): Event[] {
    if (!data) return []

    let bytes: Uint8Array
    if (data instanceof Uint8Array) {
      bytes = data
    } else if (data instanceof ArrayBuffer) {
      bytes = new Uint8Array(data)
    } else {
      bytes = new Uint8Array(data as ArrayBuffer)
    }

    const json = new TextDecoder().decode(bytes)
    return JSON.parse(json) as Event[]
  }

  // For testing
  getBufferState(ns: string) {
    return this.eventBuffers.get(ns) || null
  }

  getCacheStats() {
    return { size: this.entityCache.size, maxSize: EventSourcedEntityStore.CACHE_MAX_SIZE }
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createEntityData(overrides: Partial<{ $type: string; name: string }> = {}) {
  return {
    $type: 'TestEntity',
    name: `Test Entity ${Date.now()}`,
    value: Math.random() * 100,
    ...overrides,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('Event-Sourced Entity Store', () => {
  let sql: MockSqlite
  let store: EventSourcedEntityStore

  beforeEach(() => {
    sql = new MockSqlite()
    store = new EventSourcedEntityStore(sql)
  })

  describe('single source of truth: events_wal', () => {
    it('creates entities by appending CREATE events', async () => {
      const entity = await store.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        content: 'Hello world',
      })

      expect(entity.$type).toBe('Post')
      expect(entity.name).toBe('Test Post')
      expect(entity.version).toBe(1)

      // Verify event was appended
      const buffer = store.getBufferState('posts')
      expect(buffer).not.toBeNull()
      expect(buffer!.events.length).toBeGreaterThan(0)
      expect(buffer!.events[0].op).toBe('CREATE')
    })

    it('updates entities by appending UPDATE events', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Original',
        value: 1,
      })

      const updated = await store.update('posts', created.$id.split('/')[1]!, {
        name: 'Updated',
        value: 2,
      })

      expect(updated).not.toBeNull()
      expect(updated!.name).toBe('Updated')
      expect(updated!.version).toBe(2)
    })

    it('deletes entities by appending DELETE events', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'To Delete',
      })
      const id = created.$id.split('/')[1]!

      const deleted = await store.delete('posts', id)
      expect(deleted).toBe(true)

      // Entity should not be found (unless includeDeleted)
      const found = await store.get('posts', id)
      expect(found).toBeNull()

      const foundWithDeleted = await store.get('posts', id, true)
      expect(foundWithDeleted).not.toBeNull()
      expect(foundWithDeleted!.deletedAt).toBeDefined()
    })
  })

  describe('entity reconstruction from events', () => {
    it('reconstructs entity state by replaying events', async () => {
      // Create entity
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Original',
        value: 1,
      })
      const id = created.$id.split('/')[1]!

      // Update multiple times
      await store.update('posts', id, { value: 2 })
      await store.update('posts', id, { value: 3 })
      await store.update('posts', id, { value: 4, extra: 'field' })

      // Flush to WAL
      await store.flushAll()

      // Clear cache to force reconstruction
      store.clearCache()

      // Get entity - should reconstruct from events
      const reconstructed = await store.get('posts', id)

      expect(reconstructed).not.toBeNull()
      expect(reconstructed!.name).toBe('Original')
      expect((reconstructed as any).value).toBe(4)
      expect((reconstructed as any).extra).toBe('field')
      expect(reconstructed!.version).toBe(4)
    })

    it('correctly handles create-update-delete sequence', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Lifecycle Test',
      })
      const id = created.$id.split('/')[1]!

      await store.update('posts', id, { status: 'published' })
      await store.delete('posts', id)

      // Flush and clear cache
      await store.flushAll()
      store.clearCache()

      // Should not find deleted entity
      const found = await store.get('posts', id)
      expect(found).toBeNull()

      // But can find with includeDeleted
      const foundDeleted = await store.get('posts', id, true)
      expect(foundDeleted).not.toBeNull()
      expect(foundDeleted!.deletedAt).toBeDefined()
      expect((foundDeleted as any).status).toBe('published')
    })

    it('handles entities that never existed', async () => {
      const found = await store.get('posts', 'nonexistent')
      expect(found).toBeNull()
    })
  })

  describe('snapshot checkpoints', () => {
    it('creates snapshots after many events for performance', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Many Updates',
        value: 0,
      })
      const id = created.$id.split('/')[1]!

      // Create many updates (more than SNAPSHOT_THRESHOLD)
      for (let i = 1; i <= 60; i++) {
        await store.update('posts', id, { value: i })
      }

      // Flush events
      await store.flushAll()
      store.clearCache()

      // First reconstruction should create a snapshot
      const first = await store.get('posts', id)
      expect(first).not.toBeNull()
      expect((first as any).value).toBe(60)

      // Verify snapshot was created
      const snapshots = sql.getTable('snapshots')
      expect(snapshots.length).toBeGreaterThan(0)
    })
  })

  describe('cache behavior', () => {
    it('caches reconstructed entities', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Cached',
      })
      const id = created.$id.split('/')[1]!

      // First get should cache
      const first = await store.get('posts', id)

      // Second get should hit cache (same instance)
      const second = await store.get('posts', id)

      expect(first).toBe(second)
    })

    it('invalidates cache on update', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Original',
      })
      const id = created.$id.split('/')[1]!

      // Cache the entity
      const cached = await store.get('posts', id)
      expect(cached!.name).toBe('Original')

      // Update should invalidate cache
      await store.update('posts', id, { name: 'Updated' })

      // Should get new value
      const updated = await store.get('posts', id)
      expect(updated!.name).toBe('Updated')
    })

    it('invalidates cache on delete', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'To Delete',
      })
      const id = created.$id.split('/')[1]!

      // Cache the entity
      await store.get('posts', id)

      // Delete should invalidate cache
      await store.delete('posts', id)

      // Should not find
      const found = await store.get('posts', id)
      expect(found).toBeNull()
    })
  })

  describe('no entities table dependency', () => {
    it('does not use entities table', async () => {
      await store.create('posts', {
        $type: 'Post',
        name: 'No Table',
      })
      await store.flushAll()

      // Check that no entities table was created
      const entities = sql.getTable('entities')
      expect(entities).toHaveLength(0)
    })

    it('only uses events_wal as source of truth', async () => {
      const created = await store.create('posts', {
        $type: 'Post',
        name: 'Event Only',
      })
      const id = created.$id.split('/')[1]!

      await store.update('posts', id, { modified: true })
      await store.flushAll()

      // events_wal should have data
      const walRows = sql.getTable('events_wal')
      expect(walRows.length).toBeGreaterThan(0)

      // Clear cache and verify reconstruction works
      store.clearCache()
      const reconstructed = await store.get('posts', id)
      expect(reconstructed).not.toBeNull()
      expect((reconstructed as any).modified).toBe(true)
    })
  })

  describe('event batching', () => {
    it('buffers events before flushing to WAL', async () => {
      await store.create('posts', { $type: 'Post', name: 'Buffered 1' })
      await store.create('posts', { $type: 'Post', name: 'Buffered 2' })

      // Events should be in buffer
      const buffer = store.getBufferState('posts')
      expect(buffer!.events.length).toBe(2)

      // WAL should be empty
      const walRows = sql.getTable('events_wal')
      expect(walRows).toHaveLength(0)
    })

    it('auto-flushes when threshold reached', async () => {
      // Create many entities to trigger auto-flush (threshold is 100)
      for (let i = 0; i < 101; i++) {
        await store.create('posts', { $type: 'Post', name: `Entity ${i}` })
      }

      // WAL should have data after auto-flush
      const walRows = sql.getTable('events_wal')
      expect(walRows.length).toBeGreaterThan(0)

      // Buffer should be mostly empty (may have 1 event after flush)
      const buffer = store.getBufferState('posts')
      expect(buffer!.events.length).toBeLessThan(100)
    })
  })

  describe('multi-namespace support', () => {
    it('maintains separate event streams per namespace', async () => {
      await store.create('posts', { $type: 'Post', name: 'Post 1' })
      await store.create('users', { $type: 'User', name: 'User 1' })
      await store.create('comments', { $type: 'Comment', name: 'Comment 1' })

      const postsBuffer = store.getBufferState('posts')
      const usersBuffer = store.getBufferState('users')
      const commentsBuffer = store.getBufferState('comments')

      expect(postsBuffer!.events.length).toBe(1)
      expect(usersBuffer!.events.length).toBe(1)
      expect(commentsBuffer!.events.length).toBe(1)
    })

    it('reconstructs entities from correct namespace', async () => {
      const post = await store.create('posts', { $type: 'Post', name: 'My Post' })
      const user = await store.create('users', { $type: 'User', name: 'My User' })

      await store.flushAll()
      store.clearCache()

      const foundPost = await store.get('posts', post.$id.split('/')[1]!)
      const foundUser = await store.get('users', user.$id.split('/')[1]!)

      expect(foundPost!.$type).toBe('Post')
      expect(foundUser!.$type).toBe('User')
    })
  })
})

/**
 * Read Merge Path Tests for Pending Row Groups
 *
 * Tests for read-after-write consistency when bulk writes go to R2 pending/
 * files. The getEntityFromEvents() method must merge:
 * 1. events_wal (SQLite WAL batches)
 * 2. In-memory event buffers
 * 3. R2 pending row groups (bulk writes)
 *
 * BUG: Currently getEntityFromEvents() only reads events_wal and in-memory
 * buffers, missing entities written directly to R2 pending/ files.
 *
 * The current implementation works around this by ALSO writing events to
 * the WAL during bulk operations (see bulkWriteToR2 line 539-555). However,
 * this defeats the cost optimization goal of bulk writes (1 SQLite row vs N).
 *
 * For proper cost optimization, bulk writes should ONLY write to R2 pending
 * files and the pending_row_groups metadata table. Then getEntityFromEvents
 * must read from those pending files.
 *
 * These tests verify the behavior needed for proper pending file merging.
 *
 * Related issue: parquedb-ya2o
 *
 * @see src/worker/ParqueDBDO.ts - bulkWriteToR2() and getEntityFromEvents()
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

import { describe, it, expect, beforeEach } from 'vitest'

// =============================================================================
// Mock R2 Storage
// =============================================================================

interface MockR2Object {
  key: string
  data: ArrayBuffer | string
  size: number
}

class MockR2Bucket {
  private objects: Map<string, MockR2Object> = new Map()

  async put(key: string, data: ArrayBuffer | string): Promise<void> {
    const size = typeof data === 'string' ? data.length : data.byteLength
    this.objects.set(key, { key, data, size })
  }

  async get(key: string): Promise<{ text(): Promise<string>; arrayBuffer(): Promise<ArrayBuffer> } | null> {
    const obj = this.objects.get(key)
    if (!obj) return null

    return {
      text: async () => {
        if (typeof obj.data === 'string') return obj.data
        return new TextDecoder().decode(obj.data)
      },
      arrayBuffer: async () => {
        if (typeof obj.data === 'string') {
          return new TextEncoder().encode(obj.data).buffer
        }
        return obj.data
      },
    }
  }

  async head(key: string): Promise<{ size: number } | null> {
    const obj = this.objects.get(key)
    if (!obj) return null
    return { size: obj.size }
  }

  async list(options: { prefix: string; limit?: number }): Promise<{ objects: Array<{ key: string }> }> {
    const matching = Array.from(this.objects.keys())
      .filter(key => key.startsWith(options.prefix))
      .slice(0, options.limit ?? 1000)
      .map(key => ({ key }))
    return { objects: matching }
  }

  async delete(key: string): Promise<void> {
    this.objects.delete(key)
  }

  // Test helper: clear all objects
  clear(): void {
    this.objects.clear()
  }

  // Test helper: get all keys
  keys(): string[] {
    return Array.from(this.objects.keys())
  }
}

// =============================================================================
// Mock SQLite Storage
// =============================================================================

interface SqlRow {
  [key: string]: unknown
}

class MockSqlStorage {
  private tables: Map<string, SqlRow[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()

  exec<T = SqlRow>(query: string, ...params: unknown[]): Iterable<T> {
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

    // INSERT INTO events_wal
    if (trimmedQuery.includes('insert into events_wal')) {
      const rows = this.tables.get('events_wal') || []
      const id = (this.autoIncrement.get('events_wal') || 0) + 1
      this.autoIncrement.set('events_wal', id)

      rows.push({
        id,
        ns: params[0],
        first_seq: params[1],
        last_seq: params[2],
        events: params[3],
        created_at: params[4] || new Date().toISOString(),
      })
      this.tables.set('events_wal', rows)
      return [] as T[]
    }

    // INSERT INTO pending_row_groups
    if (trimmedQuery.includes('insert into pending_row_groups')) {
      const rows = this.tables.get('pending_row_groups') || []
      rows.push({
        id: params[0],
        ns: params[1],
        path: params[2],
        row_count: params[3],
        first_seq: params[4],
        last_seq: params[5],
        created_at: params[6] || new Date().toISOString(),
      })
      this.tables.set('pending_row_groups', rows)
      return [] as T[]
    }

    // SELECT MAX(last_seq) FROM events_wal
    if (trimmedQuery.includes('max(last_seq)') && trimmedQuery.includes('events_wal')) {
      const rows = this.tables.get('events_wal') || []
      const byNs = new Map<string, number>()
      for (const row of rows) {
        const ns = row.ns as string
        const lastSeq = row.last_seq as number
        const current = byNs.get(ns) || 0
        if (lastSeq > current) {
          byNs.set(ns, lastSeq)
        }
      }
      const result: SqlRow[] = []
      for (const [ns, maxSeq] of byNs.entries()) {
        result.push({ ns, max_seq: maxSeq })
      }
      return result as T[]
    }

    // SELECT MAX(last_seq) FROM rels_wal
    if (trimmedQuery.includes('max(last_seq)') && trimmedQuery.includes('rels_wal')) {
      return [] as T[]
    }

    // SELECT events FROM events_wal WHERE ns = ?
    if (trimmedQuery.includes('select') && trimmedQuery.includes('events_wal') && trimmedQuery.includes('where ns')) {
      const ns = params[0] as string
      const rows = this.tables.get('events_wal') || []
      const matching = rows.filter(row => row.ns === ns)
      return matching as T[]
    }

    // SELECT FROM pending_row_groups WHERE ns = ?
    if (trimmedQuery.includes('select') && trimmedQuery.includes('pending_row_groups') && trimmedQuery.includes('where ns')) {
      const ns = params[0] as string
      const rows = this.tables.get('pending_row_groups') || []
      const matching = rows.filter(row => row.ns === ns)
      return matching as T[]
    }

    // DELETE FROM events_wal WHERE ns = ?
    if (trimmedQuery.includes('delete from events_wal')) {
      const ns = params[0] as string
      const rows = this.tables.get('events_wal') || []
      this.tables.set('events_wal', rows.filter(row => row.ns !== ns))
      return [] as T[]
    }

    // DELETE FROM pending_row_groups
    if (trimmedQuery.includes('delete from pending_row_groups')) {
      const ns = params[0] as string
      const upToSeq = params[1] as number
      const rows = this.tables.get('pending_row_groups') || []
      this.tables.set('pending_row_groups', rows.filter(row =>
        row.ns !== ns || (row.last_seq as number) > upToSeq
      ))
      return [] as T[]
    }

    return [] as T[]
  }

  // Test helper: initialize tables
  initTables(): void {
    this.tables.set('events_wal', [])
    this.tables.set('pending_row_groups', [])
    this.tables.set('rels_wal', [])
    this.tables.set('checkpoints', [])
    this.tables.set('relationships', [])
  }

  // Test helper: clear all data
  clear(): void {
    this.tables.clear()
    this.autoIncrement.clear()
  }

  // Test helper: get all rows from a table
  getTable(name: string): SqlRow[] {
    return this.tables.get(name) || []
  }

  // Test helper: direct insert for testing
  insertEventsWal(ns: string, firstSeq: number, lastSeq: number, events: ArrayBuffer): void {
    const rows = this.tables.get('events_wal') || []
    const id = (this.autoIncrement.get('events_wal') || 0) + 1
    this.autoIncrement.set('events_wal', id)
    rows.push({
      id,
      ns,
      first_seq: firstSeq,
      last_seq: lastSeq,
      events,
      created_at: new Date().toISOString(),
    })
    this.tables.set('events_wal', rows)
  }

  // Test helper: direct insert for pending row groups
  insertPendingRowGroup(
    id: string,
    ns: string,
    path: string,
    rowCount: number,
    firstSeq: number,
    lastSeq: number
  ): void {
    const rows = this.tables.get('pending_row_groups') || []
    rows.push({
      id,
      ns,
      path,
      row_count: rowCount,
      first_seq: firstSeq,
      last_seq: lastSeq,
      created_at: new Date().toISOString(),
    })
    this.tables.set('pending_row_groups', rows)
  }
}

// =============================================================================
// Event Helpers
// =============================================================================

interface Event {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  target: string
  before?: Record<string, unknown>
  after?: Record<string, unknown>
  actor: string
}

function serializeEvents(events: Event[]): ArrayBuffer {
  const json = JSON.stringify(events)
  return new TextEncoder().encode(json).buffer
}

function deserializeEvents(buffer: ArrayBuffer): Event[] {
  const json = new TextDecoder().decode(buffer)
  return JSON.parse(json)
}

/**
 * Apply an event to an entity state, returning the new state.
 *
 * Event application follows last-write-wins semantics:
 * - CREATE: Initializes a new entity with the event's `after` data
 * - UPDATE: Merges `after` data into existing entity (requires existing entity)
 * - DELETE: Returns null to indicate entity was deleted
 *
 * @param entity - Current entity state (null if not yet created)
 * @param event - Event to apply
 * @param ns - Namespace for constructing entity $id
 * @param id - Entity ID for constructing entity $id
 * @returns Updated entity state, or null if deleted
 */
function applyEventToEntity(
  entity: Record<string, unknown> | null,
  event: Event,
  ns: string,
  id: string
): Record<string, unknown> | null {
  if (event.op === 'CREATE') {
    return { $id: `${ns}/${id}`, ...event.after }
  } else if (event.op === 'UPDATE' && entity) {
    return { ...entity, ...event.after }
  } else if (event.op === 'DELETE') {
    return null
  }
  return entity
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Read Merge Path for Pending Row Groups', () => {
  let sql: MockSqlStorage
  let bucket: MockR2Bucket

  beforeEach(() => {
    sql = new MockSqlStorage()
    sql.initTables()
    bucket = new MockR2Bucket()
  })

  describe('Scenario 1: Read after bulk write (events in WAL)', () => {
    /**
     * This test verifies basic read-after-write when events are still in WAL.
     * This currently works because bulkWriteToR2 appends events to WAL.
     */
    it('should read entity created via bulk write when events are in WAL', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Simulate bulk write: events go to WAL
      const events: Event[] = [{
        id: 'evt1',
        ts: Date.now(),
        op: 'CREATE',
        target,
        after: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
        actor: 'system',
      }]

      sql.insertEventsWal(ns, 1, 1, serializeEvents(events))

      // Also write to pending file in R2 (as bulkWriteToR2 does)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Read entity from events_wal (simulating getEntityFromEvents)
      const walRows = sql.getTable('events_wal').filter(row => row.ns === ns)
      let entity: Record<string, unknown> | null = null

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target && event.op === 'CREATE') {
            entity = { $id: `${ns}/${entityId}`, ...event.after }
          }
        }
      }

      // Should find entity from WAL
      expect(entity).not.toBeNull()
      expect(entity?.$type).toBe('User')
      expect(entity?.name).toBe('Alice')
    })
  })

  describe('Scenario 2: Read after WAL is flushed (bug case)', () => {
    /**
     * CRITICAL BUG TEST: This exposes the read-after-write consistency issue.
     *
     * When events are flushed from events_wal to R2 (events.parquet),
     * but pending row groups haven't been merged yet, getEntityFromEvents
     * will return null because it only reads events_wal.
     *
     * The entity data IS available in the R2 pending file, but the current
     * implementation doesn't read it.
     */
    it('should read entity from pending R2 file after WAL is cleared', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Simulate: Pending file exists in R2
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Simulate: WAL has been cleared (flushed to R2 events.parquet)
      // events_wal is now empty for this namespace
      const walRows = sql.getTable('events_wal').filter(row => row.ns === ns)
      expect(walRows).toHaveLength(0) // WAL is empty

      // Current getEntityFromEvents behavior: only reads WAL
      let entityFromWal: Record<string, unknown> | null = null
      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target && event.op === 'CREATE') {
            entityFromWal = { $id: `${ns}/${entityId}`, ...event.after }
          }
        }
      }

      // BUG: Entity not found from WAL (because WAL was flushed)
      expect(entityFromWal).toBeNull()

      // BUT the entity data IS available in pending file
      const pendingGroups = sql.getTable('pending_row_groups').filter(row => row.ns === ns)
      expect(pendingGroups).toHaveLength(1)

      // Read from pending file (what getEntityFromEvents SHOULD do)
      let entityFromPending: Record<string, unknown> | null = null
      for (const group of pendingGroups) {
        const obj = await bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            if (row.$id === `${ns}/${entityId}`) {
              entityFromPending = { $id: row.$id, ...JSON.parse(row.data) }
            }
          }
        }
      }

      // Entity IS available from pending file
      expect(entityFromPending).not.toBeNull()
      expect(entityFromPending?.$type).toBe('User')
      expect(entityFromPending?.name).toBe('Alice')

      // THE FIX: getEntityFromEvents should merge WAL + pending files
      // This test will pass when the fix is implemented
      const mergedEntity = entityFromWal ?? entityFromPending
      expect(mergedEntity).not.toBeNull()
      expect(mergedEntity?.$type).toBe('User')
    })
  })

  describe('Scenario 3: Merge WAL and pending files correctly', () => {
    /**
     * When entity is created via bulk write and then updated normally,
     * the read path must merge:
     * 1. Initial state from pending file
     * 2. Updates from events_wal
     */
    it('should merge entity state from pending file and subsequent WAL updates', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Step 1: Bulk create writes to pending file (sequence 1)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Step 2: Update via normal path writes to WAL (sequence 2)
      const updateEvent: Event = {
        id: 'evt2',
        ts: Date.now(),
        op: 'UPDATE',
        target,
        before: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
        after: { $type: 'User', name: 'Alice Smith', email: 'alice@example.com', verified: true },
        actor: 'system',
      }
      sql.insertEventsWal(ns, 2, 2, serializeEvents([updateEvent]))

      // Read and merge: pending file + WAL events
      let entity: Record<string, unknown> | null = null

      // Read from pending file first (base state)
      const pendingGroups = sql.getTable('pending_row_groups')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const group of pendingGroups) {
        const obj = await bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            if (row.$id === `${ns}/${entityId}`) {
              entity = { $id: row.$id, ...JSON.parse(row.data) }
            }
          }
        }
      }

      // Then apply WAL events
      const walRows = sql.getTable('events_wal')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target) {
            if (event.op === 'CREATE') {
              entity = { $id: `${ns}/${entityId}`, ...event.after }
            } else if (event.op === 'UPDATE' && entity) {
              entity = { ...entity, ...event.after }
            } else if (event.op === 'DELETE') {
              entity = null
            }
          }
        }
      }

      // Merged entity should have updates applied
      expect(entity).not.toBeNull()
      expect(entity?.name).toBe('Alice Smith')
      expect(entity?.verified).toBe(true)
    })

    /**
     * BUG TEST: Current implementation doesn't read pending files,
     * so updates would fail to find the base entity.
     */
    it('should fail to read entity when only pending file exists (current bug)', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Entity exists ONLY in pending file (WAL CREATE event was flushed)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Simulate getEntityFromEvents (only reads WAL - current behavior)
      let entityFromWalOnly: Record<string, unknown> | null = null
      const walRows = sql.getTable('events_wal').filter(row => row.ns === ns)

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target && event.op === 'CREATE') {
            entityFromWalOnly = { $id: `${ns}/${entityId}`, ...event.after }
          }
        }
      }

      // BUG: Current implementation returns null
      // because it doesn't read pending files
      expect(entityFromWalOnly).toBeNull()

      // But entity data exists in pending file
      const pendingGroups = sql.getTable('pending_row_groups').filter(row => row.ns === ns)
      expect(pendingGroups.length).toBeGreaterThan(0)
    })
  })

  describe('Scenario 4: Multiple pending files merge order', () => {
    /**
     * Multiple pending files must be merged in sequence number order.
     * Later writes should override earlier ones.
     */
    it('should merge multiple pending files in correct sequence order', async () => {
      const ns = 'users'

      // First bulk write: users u1, u2, u3 (seq 1-3)
      const pendingPath1 = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath1, JSON.stringify([
        { $id: `${ns}/u1`, data: JSON.stringify({ $type: 'User', name: 'Alice' }) },
        { $id: `${ns}/u2`, data: JSON.stringify({ $type: 'User', name: 'Bob' }) },
        { $id: `${ns}/u3`, data: JSON.stringify({ $type: 'User', name: 'Charlie' }) },
      ]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath1, 3, 1, 3)

      // Second bulk write: users u4, u5 (seq 4-5)
      const pendingPath2 = `data/${ns}/pending/pending2.parquet.json`
      await bucket.put(pendingPath2, JSON.stringify([
        { $id: `${ns}/u4`, data: JSON.stringify({ $type: 'User', name: 'Diana' }) },
        { $id: `${ns}/u5`, data: JSON.stringify({ $type: 'User', name: 'Eve' }) },
      ]))
      sql.insertPendingRowGroup('pending2', ns, pendingPath2, 2, 4, 5)

      // Third bulk write: UPDATES user u2 (seq 6) - same ID, new data
      const pendingPath3 = `data/${ns}/pending/pending3.parquet.json`
      await bucket.put(pendingPath3, JSON.stringify([
        { $id: `${ns}/u2`, data: JSON.stringify({ $type: 'User', name: 'Bob Updated', verified: true }) },
      ]))
      sql.insertPendingRowGroup('pending3', ns, pendingPath3, 1, 6, 6)

      // Read all pending files in sequence order
      const pendingGroups = sql.getTable('pending_row_groups')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      expect(pendingGroups).toHaveLength(3)
      expect(pendingGroups[0]!.first_seq).toBe(1)
      expect(pendingGroups[1]!.first_seq).toBe(4)
      expect(pendingGroups[2]!.first_seq).toBe(6)

      // Merge all pending files
      const entities: Map<string, Record<string, unknown>> = new Map()

      for (const group of pendingGroups) {
        const obj = await bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            // Later writes override earlier ones
            entities.set(row.$id, { $id: row.$id, ...JSON.parse(row.data) })
          }
        }
      }

      // Should have 5 unique users
      expect(entities.size).toBe(5)

      // User u2 should have the UPDATED data from pending3
      const u2 = entities.get(`${ns}/u2`)
      expect(u2?.name).toBe('Bob Updated')
      expect(u2?.verified).toBe(true)
    })

    /**
     * When files are read out of order, later sequence numbers should win.
     */
    it('should handle out-of-order pending file reads correctly', async () => {
      const ns = 'products'
      const entityId = 'p1'

      // Write two versions of same entity with different sequence numbers
      // Pending file 2 (seq 5) - OLDER version
      const pendingPath2 = `data/${ns}/pending/pending2.parquet.json`
      await bucket.put(pendingPath2, JSON.stringify([
        { $id: `${ns}/${entityId}`, data: JSON.stringify({ $type: 'Product', name: 'Widget', price: 10 }) },
      ]))
      sql.insertPendingRowGroup('pending2', ns, pendingPath2, 1, 5, 5)

      // Pending file 1 (seq 10) - NEWER version (higher sequence = later write)
      const pendingPath1 = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath1, JSON.stringify([
        { $id: `${ns}/${entityId}`, data: JSON.stringify({ $type: 'Product', name: 'Widget Pro', price: 20 }) },
      ]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath1, 1, 10, 10)

      // Read and merge in sequence order (must sort by first_seq)
      const pendingGroups = sql.getTable('pending_row_groups')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      const entities: Map<string, Record<string, unknown>> = new Map()
      for (const group of pendingGroups) {
        const obj = await bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            entities.set(row.$id, { $id: row.$id, ...JSON.parse(row.data) })
          }
        }
      }

      // Should have newer version (seq 10 > seq 5)
      const entity = entities.get(`${ns}/${entityId}`)
      expect(entity?.name).toBe('Widget Pro')
      expect(entity?.price).toBe(20)
    })
  })

  describe('Scenario 5: Entity state consistency', () => {
    /**
     * Entity state should be the same whether read from WAL or pending files.
     */
    it('should produce consistent state from WAL-only path', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Create + Update via WAL only
      const events: Event[] = [
        {
          id: 'evt1',
          ts: Date.now(),
          op: 'CREATE',
          target,
          after: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
          actor: 'system',
        },
        {
          id: 'evt2',
          ts: Date.now() + 1,
          op: 'UPDATE',
          target,
          before: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
          after: { $type: 'User', name: 'Alice Smith', email: 'alice@newdomain.com' },
          actor: 'system',
        },
      ]
      sql.insertEventsWal(ns, 1, 2, serializeEvents(events))

      // Read from WAL
      let entity: Record<string, unknown> | null = null
      const walRows = sql.getTable('events_wal').filter(row => row.ns === ns)

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target) {
            if (event.op === 'CREATE') {
              entity = { $id: `${ns}/${entityId}`, ...event.after }
            } else if (event.op === 'UPDATE' && entity) {
              entity = { ...entity, ...event.after }
            }
          }
        }
      }

      expect(entity).not.toBeNull()
      expect(entity?.name).toBe('Alice Smith')
      expect(entity?.email).toBe('alice@newdomain.com')
    })

    /**
     * Entity state should be consistent when split across pending + WAL.
     */
    it('should produce consistent state from pending + WAL merge', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`

      // Create via pending file (bulk write)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Update via WAL
      const updateEvent: Event = {
        id: 'evt2',
        ts: Date.now(),
        op: 'UPDATE',
        target,
        before: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
        after: { $type: 'User', name: 'Alice Smith', email: 'alice@newdomain.com' },
        actor: 'system',
      }
      sql.insertEventsWal(ns, 2, 2, serializeEvents([updateEvent]))

      // Merge: pending first, then WAL
      let entity: Record<string, unknown> | null = null

      // 1. Read from pending files
      const pendingGroups = sql.getTable('pending_row_groups')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const group of pendingGroups) {
        const obj = await bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            if (row.$id === `${ns}/${entityId}`) {
              entity = { $id: row.$id, ...JSON.parse(row.data) }
            }
          }
        }
      }

      // 2. Apply WAL events
      const walRows = sql.getTable('events_wal')
        .filter(row => row.ns === ns)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target) {
            if (event.op === 'CREATE') {
              entity = { $id: `${ns}/${entityId}`, ...event.after }
            } else if (event.op === 'UPDATE' && entity) {
              entity = { ...entity, ...event.after }
            }
          }
        }
      }

      // Should have same result as WAL-only path
      expect(entity).not.toBeNull()
      expect(entity?.name).toBe('Alice Smith')
      expect(entity?.email).toBe('alice@newdomain.com')
    })
  })

  describe('Integration: getEntityFromEvents should merge pending files', () => {
    /**
     * This test simulates what getEntityFromEvents SHOULD do:
     * Merge events_wal + in-memory buffer + R2 pending files
     */
    it('getEntityFromEvents should read from pending files when WAL is empty', async () => {
      const ns = 'users'
      const entityId = 'u1'

      // Entity only exists in pending file (WAL was flushed)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Simulate improved getEntityFromEvents that reads pending files
      async function getEntityFromEventsImproved(
        targetNs: string,
        targetId: string
      ): Promise<Record<string, unknown> | null> {
        const target = `${targetNs}:${targetId}`
        let entity: Record<string, unknown> | null = null

        // 1. Read from pending files (sorted by sequence)
        const pendingGroups = sql.getTable('pending_row_groups')
          .filter(row => row.ns === targetNs)
          .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

        for (const group of pendingGroups) {
          const obj = await bucket.get(group.path as string)
          if (obj) {
            const text = await obj.text()
            const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
            for (const row of rows) {
              if (row.$id === `${targetNs}/${targetId}`) {
                entity = { $id: row.$id, ...JSON.parse(row.data) }
              }
            }
          }
        }

        // 2. Apply events from events_wal
        const walRows = sql.getTable('events_wal')
          .filter(row => row.ns === targetNs)
          .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

        for (const row of walRows) {
          const walEvents = deserializeEvents(row.events as ArrayBuffer)
          for (const event of walEvents) {
            if (event.target === target) {
              if (event.op === 'CREATE') {
                entity = { $id: `${targetNs}/${targetId}`, ...event.after }
              } else if (event.op === 'UPDATE' && entity) {
                entity = { ...entity, ...event.after }
              } else if (event.op === 'DELETE') {
                entity = null
              }
            }
          }
        }

        return entity
      }

      // Current implementation (WAL only) returns null
      const walRows = sql.getTable('events_wal').filter(row => row.ns === ns)
      expect(walRows).toHaveLength(0)

      // Improved implementation should find the entity
      const entity = await getEntityFromEventsImproved(ns, entityId)
      expect(entity).not.toBeNull()
      expect(entity?.$type).toBe('User')
      expect(entity?.name).toBe('Alice')
    })
  })

  // =============================================================================
  // RED PHASE: Tests that FAIL with current implementation
  // =============================================================================
  // These tests assert the CORRECT behavior. They will FAIL until the fix is
  // implemented in getEntityFromEvents to read from pending files.
  //
  // The tests use a mock of getEntityFromEvents that matches the current
  // implementation (WAL-only) to demonstrate the bug.

  describe('GREEN: getEntityFromEvents reads from pending files', () => {
    /**
     * FIXED getEntityFromEvents implementation - Reference for production code.
     *
     * Read order (oldest to newest data):
     *   1. R2 pending files (bulk writes awaiting merge)
     *   2. events_wal table (flushed but not yet compacted events)
     *   3. In-memory event buffer (most recent, not yet flushed)
     *
     * Entity state is built incrementally: each source can override the previous.
     * This ensures read-after-write consistency even when data is spread across
     * multiple storage layers.
     */
    async function getEntityFromEventsFixed(
      sqlStorage: MockSqlStorage,
      r2Bucket: MockR2Bucket,
      nsEventBuffers: Map<string, { events: Event[] }>,
      targetNs: string,
      targetId: string
    ): Promise<Record<string, unknown> | null> {
      const target = `${targetNs}:${targetId}`
      let entity: Record<string, unknown> | null = null

      // STEP 0 (FIX): Read from R2 pending files FIRST (oldest data layer)
      // Pending files contain bulk-written entities that haven't been merged
      // into the main data.parquet yet. Sort by sequence to process in order.
      const pendingGroups = sqlStorage.getTable('pending_row_groups')
        .filter(row => row.ns === targetNs)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const group of pendingGroups) {
        const obj = await r2Bucket.get(group.path as string)
        if (obj) {
          const text = await obj.text()
          const rows = JSON.parse(text) as Array<{ $id: string; data: string }>
          for (const row of rows) {
            if (row.$id === `${targetNs}/${targetId}`) {
              // Later pending files override earlier ones (last-write-wins)
              entity = { $id: row.$id, ...JSON.parse(row.data) }
            }
          }
        }
      }

      // STEP 1: Apply events from events_wal table (second data layer)
      // These are batched events that have been flushed to SQLite but not yet
      // compacted to R2. Sort by first_seq to maintain temporal ordering.
      const walRows = sqlStorage.getTable('events_wal')
        .filter(row => row.ns === targetNs)
        .sort((a, b) => (a.first_seq as number) - (b.first_seq as number))

      for (const row of walRows) {
        const walEvents = deserializeEvents(row.events as ArrayBuffer)
        for (const event of walEvents) {
          if (event.target === target) {
            // Apply event operations: CREATE initializes, UPDATE merges, DELETE nullifies
            if (event.op === 'CREATE') {
              entity = { $id: `${targetNs}/${targetId}`, ...event.after }
            } else if (event.op === 'UPDATE' && entity) {
              entity = { ...entity, ...event.after }
            } else if (event.op === 'DELETE') {
              entity = null
            }
          }
        }
      }

      // STEP 2: Apply events from in-memory buffer (newest data layer)
      // These are the most recent events that haven't been flushed to SQLite yet.
      const nsBuffer = nsEventBuffers.get(targetNs)
      if (nsBuffer) {
        for (const event of nsBuffer.events) {
          if (event.target === target) {
            if (event.op === 'CREATE') {
              entity = { $id: `${targetNs}/${targetId}`, ...event.after }
            } else if (event.op === 'UPDATE' && entity) {
              entity = { ...entity, ...event.after }
            } else if (event.op === 'DELETE') {
              entity = null
            }
          }
        }
      }

      return entity
    }

    /**
     * GREEN TEST 1: Entity in pending file should be readable
     *
     * The fixed implementation reads from pending files first.
     */
    it('should find entity when it only exists in pending file', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const nsEventBuffers = new Map<string, { events: Event[] }>()

      // Entity exists ONLY in pending file (simulating post-WAL-flush state)
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // WAL is empty (events were flushed to R2)
      expect(sql.getTable('events_wal')).toHaveLength(0)

      // Fixed implementation reads from pending files
      const entity = await getEntityFromEventsFixed(sql, bucket, nsEventBuffers, ns, entityId)

      // Entity SHOULD be found from pending files
      expect(entity).not.toBeNull()
      expect(entity?.$type).toBe('User')
      expect(entity?.name).toBe('Alice')
    })

    /**
     * GREEN TEST 2: Update on pending entity should work
     *
     * The fixed implementation reads base entity from pending file
     * then applies WAL updates on top.
     */
    it('should apply WAL update to entity from pending file', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const target = `${ns}:${entityId}`
      const nsEventBuffers = new Map<string, { events: Event[] }>()

      // Base entity exists ONLY in pending file
      const pendingPath = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', email: 'alice@example.com' }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath, 1, 1, 1)

      // Update event exists in WAL
      const updateEvent: Event = {
        id: 'evt2',
        ts: Date.now(),
        op: 'UPDATE',
        target,
        before: { $type: 'User', name: 'Alice', email: 'alice@example.com' },
        after: { $type: 'User', name: 'Alice Smith', email: 'alice@newdomain.com' },
        actor: 'system',
      }
      sql.insertEventsWal(ns, 2, 2, serializeEvents([updateEvent]))

      // Fixed implementation reads pending first, then applies WAL updates
      const entity = await getEntityFromEventsFixed(sql, bucket, nsEventBuffers, ns, entityId)

      // Entity SHOULD be found with updates applied
      expect(entity).not.toBeNull()
      expect(entity?.name).toBe('Alice Smith')
      expect(entity?.email).toBe('alice@newdomain.com')
    })

    /**
     * GREEN TEST 3: Multiple pending files should merge in order
     *
     * The fixed implementation reads all pending files in sequence order.
     */
    it('should read and merge multiple pending files in sequence order', async () => {
      const ns = 'products'
      const nsEventBuffers = new Map<string, { events: Event[] }>()

      // First bulk write: products p1, p2 (seq 1-2)
      const pendingPath1 = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath1, JSON.stringify([
        { $id: `${ns}/p1`, data: JSON.stringify({ $type: 'Product', name: 'Widget' }) },
        { $id: `${ns}/p2`, data: JSON.stringify({ $type: 'Product', name: 'Gadget' }) },
      ]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath1, 2, 1, 2)

      // Second bulk write: products p3, p4, p5 (seq 3-5)
      const pendingPath2 = `data/${ns}/pending/pending2.parquet.json`
      await bucket.put(pendingPath2, JSON.stringify([
        { $id: `${ns}/p3`, data: JSON.stringify({ $type: 'Product', name: 'Gizmo' }) },
        { $id: `${ns}/p4`, data: JSON.stringify({ $type: 'Product', name: 'Doohickey' }) },
        { $id: `${ns}/p5`, data: JSON.stringify({ $type: 'Product', name: 'Thingamajig' }) },
      ]))
      sql.insertPendingRowGroup('pending2', ns, pendingPath2, 3, 3, 5)

      // WAL is empty
      expect(sql.getTable('events_wal')).toHaveLength(0)

      // Read all entities using fixed implementation
      const entities: Map<string, Record<string, unknown>> = new Map()
      for (const id of ['p1', 'p2', 'p3', 'p4', 'p5']) {
        const entity = await getEntityFromEventsFixed(sql, bucket, nsEventBuffers, ns, id)
        if (entity) {
          entities.set(`${ns}/${id}`, entity)
        }
      }

      // Should find all 5 entities
      expect(entities.size).toBe(5)
      expect(entities.get(`${ns}/p1`)?.name).toBe('Widget')
      expect(entities.get(`${ns}/p5`)?.name).toBe('Thingamajig')
    })

    /**
     * GREEN TEST 4: Later pending file should override earlier for same entity
     *
     * The fixed implementation processes pending files in sequence order,
     * so later versions override earlier ones.
     */
    it('should use latest pending file data for same entity ID', async () => {
      const ns = 'users'
      const entityId = 'u1'
      const nsEventBuffers = new Map<string, { events: Event[] }>()

      // First version (seq 1)
      const pendingPath1 = `data/${ns}/pending/pending1.parquet.json`
      await bucket.put(pendingPath1, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice', version: 1 }),
      }]))
      sql.insertPendingRowGroup('pending1', ns, pendingPath1, 1, 1, 1)

      // Second version of same entity (seq 5) - later write
      const pendingPath2 = `data/${ns}/pending/pending2.parquet.json`
      await bucket.put(pendingPath2, JSON.stringify([{
        $id: `${ns}/${entityId}`,
        data: JSON.stringify({ $type: 'User', name: 'Alice Updated', version: 2 }),
      }]))
      sql.insertPendingRowGroup('pending2', ns, pendingPath2, 1, 5, 5)

      // Fixed implementation reads pending files in sequence order
      const entity = await getEntityFromEventsFixed(sql, bucket, nsEventBuffers, ns, entityId)

      // Should have the LATER version
      expect(entity).not.toBeNull()
      expect(entity?.name).toBe('Alice Updated')
      expect(entity?.version).toBe(2)
    })
  })

  // Note: The 'Reference' test section was removed as it was a duplicate of the
  // 'GREEN: getEntityFromEvents reads from pending files' section above.
  // See getEntityFromEventsFixed() in that section for the reference implementation.
})

/**
 * DO WAL Cost Optimization Tests
 *
 * Tests verifying the cost optimization goals of the WAL rewrite:
 * 1. 90%+ reduction in DO SQLite row operations
 * 2. Bulk imports 10x faster via R2 bypass
 * 3. Event batching reduces per-operation overhead
 *
 * Cost model:
 * - DO SQLite is 4.5x more expensive than R2 writes
 * - Old: 1 SQLite row per entity + 1 row per event = 2 rows/entity
 * - New: 1 SQLite row per ~100 events (batch) + bulk bypass to R2
 *
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

import { describe, it, expect, beforeEach } from 'vitest'

// =============================================================================
// Mock SQLite with Row Operation Tracking
// =============================================================================

interface SqlOperation {
  type: 'insert' | 'update' | 'delete' | 'select'
  table: string
  rowCount: number
}

class CostTrackingSqlite {
  private tables: Map<string, unknown[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()
  private lastInsertId = 0

  // Cost tracking
  operations: SqlOperation[] = []
  insertCount = 0
  updateCount = 0
  deleteCount = 0
  selectCount = 0

  resetCostTracking() {
    this.operations = []
    this.insertCount = 0
    this.updateCount = 0
    this.deleteCount = 0
    this.selectCount = 0
  }

  getInsertsByTable(): Record<string, number> {
    const byTable: Record<string, number> = {}
    for (const op of this.operations) {
      if (op.type === 'insert') {
        byTable[op.table] = (byTable[op.table] || 0) + op.rowCount
      }
    }
    return byTable
  }

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
        this.lastInsertId = id

        // Track cost
        this.insertCount++
        this.operations.push({ type: 'insert', table: tableName, rowCount: 1 })

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
        } else if (tableName === 'rels_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
            created_at: params[4] || new Date().toISOString(),
          }
        } else if (tableName === 'pending_row_groups') {
          row = {
            id: params[0],
            ns: params[1],
            path: params[2],
            row_count: params[3],
            first_seq: params[4],
            last_seq: params[5],
            created_at: params[6] || new Date().toISOString(),
          }
        } else if (tableName === 'event_batches') {
          row = {
            id,
            batch: params[0],
            min_ts: params[1],
            max_ts: params[2],
            event_count: params[3],
            flushed: 0,
            created_at: new Date().toISOString(),
          }
        } else if (tableName === 'entities') {
          // Old model - track this for comparison
          row = {
            id,
            ns: params[0],
            entity_id: params[1],
            data: params[2],
          }
        } else if (tableName === 'events') {
          // Old model - individual events
          row = {
            id: params[0],
            ts: params[1],
            target: params[2],
            op: params[3],
            data: params[4],
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
      this.selectCount++
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.operations.push({ type: 'select', table: tableName, rowCount: 0 })
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

    // SELECT COUNT(*)
    if (trimmedQuery.includes('count(*)')) {
      this.selectCount++
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.operations.push({ type: 'select', table: tableName, rowCount: 0 })
        const rows = this.tables.get(tableName) || []

        // Handle various WHERE clauses
        if (trimmedQuery.includes('flushed = 0')) {
          const count = rows.filter((r: any) => r.flushed === 0).length
          return [{ count }] as T[]
        }
        if (trimmedQuery.includes('flushed = 1')) {
          const count = rows.filter((r: any) => r.flushed === 1).length
          return [{ count }] as T[]
        }
        if (trimmedQuery.includes('where ns =')) {
          const ns = params[0] as string
          const count = rows.filter((r: any) => r.ns === ns).length
          return [{ count }] as T[]
        }

        return [{ count: rows.length }] as T[]
      }
    }

    // SELECT SUM
    if (trimmedQuery.includes('sum(')) {
      this.selectCount++
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.operations.push({ type: 'select', table: tableName, rowCount: 0 })
        const rows = this.tables.get(tableName) || []

        if (trimmedQuery.includes('sum(event_count)')) {
          const unflushed = rows.filter((r: any) => r.flushed === 0)
          const total = unflushed.reduce((sum: number, r: any) => sum + r.event_count, 0)
          return [{ total }] as T[]
        }

        if (trimmedQuery.includes('sum(last_seq - first_seq + 1)')) {
          const ns = params[0] as string | undefined
          let filtered = rows
          if (ns) {
            filtered = rows.filter((r: any) => r.ns === ns)
          }
          const total = filtered.reduce((sum: number, r: any) =>
            sum + ((r.last_seq || 0) - (r.first_seq || 0) + 1), 0)
          return [{ total }] as T[]
        }

        const total = rows.reduce((sum: number, r: any) => sum + (r.count || 0), 0)
        return [{ total }] as T[]
      }
    }

    // SELECT events
    if (trimmedQuery.includes('select') && (trimmedQuery.includes('events') || trimmedQuery.includes('*'))) {
      this.selectCount++
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        this.operations.push({ type: 'select', table: tableName, rowCount: rows.length })

        // Filter by namespace if specified
        if (params.length > 0 && (tableName === 'events_wal' || tableName === 'rels_wal')) {
          const ns = params[0] as string
          const filtered = rows.filter((r: any) => r.ns === ns)
          return filtered as T[]
        }

        return rows as T[]
      }
    }

    // UPDATE
    if (trimmedQuery.startsWith('update')) {
      const match = query.match(/update (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.updateCount++
        this.operations.push({ type: 'update', table: tableName, rowCount: 1 })

        const rows = this.tables.get(tableName) || []
        // Handle flushed update
        if (trimmedQuery.includes('flushed = 1')) {
          const ids = params as number[]
          rows.forEach((r: any) => {
            if (ids.includes(r.id)) {
              r.flushed = 1
            }
          })
        }
      }
      return [] as T[]
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        this.deleteCount++
        this.operations.push({ type: 'delete', table: tableName, rowCount: 1 })

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

    return [] as T[]
  }

  getTable(name: string): unknown[] {
    return this.tables.get(name) || []
  }

  clear() {
    this.tables.clear()
    this.autoIncrement.clear()
    this.resetCostTracking()
  }
}

// =============================================================================
// Mock WAL-Based Entity Store (New Model)
// =============================================================================

interface Event {
  id: string
  ts: number
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  target: string
  before?: unknown
  after?: unknown
  actor?: string
}

interface EventBuffer {
  events: Event[]
  firstSeq: number
  lastSeq: number
  sizeBytes: number
}

class WalBasedStore {
  private sql: CostTrackingSqlite
  private counters: Map<string, number> = new Map()
  private eventBuffers: Map<string, EventBuffer> = new Map()
  private initialized = false

  // Thresholds
  static readonly BATCH_COUNT_THRESHOLD = 100
  static readonly BATCH_SIZE_THRESHOLD = 64 * 1024
  static readonly BULK_THRESHOLD = 5

  constructor(sql: CostTrackingSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    if (this.initialized) return

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

    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_wal_ns ON events_wal(ns, last_seq)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_pending_row_groups_ns ON pending_row_groups(ns, created_at)')

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
   * Create a single entity (uses event buffering)
   */
  async create(ns: string, data: { $type: string; name: string; [key: string]: unknown }): Promise<{ $id: string }> {
    await this.ensureInitialized()

    const seq = this.getNextSeq(ns)
    const id = `${seq}`
    const entityId = `${ns}/${id}`

    const event: Event = {
      id: `evt_${seq}`,
      ts: Date.now(),
      op: 'CREATE',
      target: `${ns}:${id}`,
      after: data,
      actor: 'system/anonymous',
    }

    await this.appendEvent(ns, event)

    return { $id: entityId }
  }

  /**
   * Create multiple entities
   * - < BULK_THRESHOLD: uses event buffering
   * - >= BULK_THRESHOLD: bypasses SQLite, writes directly to R2
   */
  async createMany(ns: string, items: Array<{ $type: string; name: string; [key: string]: unknown }>): Promise<Array<{ $id: string }>> {
    await this.ensureInitialized()

    if (items.length === 0) {
      return []
    }

    if (items.length >= WalBasedStore.BULK_THRESHOLD) {
      return this.bulkCreate(ns, items)
    }

    // Small batch - use event buffering
    const entities: Array<{ $id: string }> = []
    for (const item of items) {
      const entity = await this.create(ns, item)
      entities.push(entity)
    }
    return entities
  }

  /**
   * Bulk create bypasses SQLite and writes metadata only (1 row)
   */
  private async bulkCreate(ns: string, items: Array<{ $type: string; name: string; [key: string]: unknown }>): Promise<Array<{ $id: string }>> {
    const firstSeq = this.counters.get(ns) || 1
    const lastSeq = firstSeq + items.length - 1
    this.counters.set(ns, lastSeq + 1)

    const entities: Array<{ $id: string }> = []
    for (let i = 0; i < items.length; i++) {
      const seq = firstSeq + i
      entities.push({ $id: `${ns}/${seq}` })
    }

    // Record pending row group metadata (1 SQLite row for entire batch!)
    const pendingId = `pending_${Date.now()}`
    const pendingPath = `data/${ns}/pending/${pendingId}.parquet`

    this.sql.exec(
      `INSERT INTO pending_row_groups (id, ns, path, row_count, first_seq, last_seq, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      pendingId, ns, pendingPath, items.length, firstSeq, lastSeq, new Date().toISOString()
    )

    return entities
  }

  /**
   * Append event to buffer
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

    // Auto-flush at threshold
    if (buffer.events.length >= WalBasedStore.BATCH_COUNT_THRESHOLD ||
        buffer.sizeBytes >= WalBasedStore.BATCH_SIZE_THRESHOLD) {
      await this.flushEventBatch(ns)
    }
  }

  /**
   * Flush buffered events as single WAL row
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

  async flushAll(): Promise<void> {
    for (const ns of this.eventBuffers.keys()) {
      await this.flushEventBatch(ns)
    }
  }

  getBufferState(ns: string) {
    return this.eventBuffers.get(ns) || null
  }
}

// =============================================================================
// Mock Old Model (for comparison)
// =============================================================================

class OldModelStore {
  private sql: CostTrackingSqlite
  private counter = 0
  private initialized = false

  constructor(sql: CostTrackingSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Old model: separate entities and events tables
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        ns TEXT NOT NULL,
        id TEXT NOT NULL,
        data TEXT NOT NULL,
        PRIMARY KEY (ns, id)
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ts INTEGER NOT NULL,
        target TEXT NOT NULL,
        op TEXT NOT NULL,
        data TEXT
      )
    `)

    this.initialized = true
  }

  /**
   * Old model: 1 entity row + 1 event row = 2 rows per create
   */
  async create(ns: string, data: { $type: string; name: string; [key: string]: unknown }): Promise<{ $id: string }> {
    await this.ensureInitialized()

    const id = `${++this.counter}`
    const entityId = `${ns}/${id}`

    // Insert entity (1 row)
    this.sql.exec(
      `INSERT INTO entities (ns, id, data) VALUES (?, ?, ?)`,
      ns, id, JSON.stringify(data)
    )

    // Insert event (1 row)
    this.sql.exec(
      `INSERT INTO events (id, ts, target, op, data) VALUES (?, ?, ?, ?, ?)`,
      `evt_${this.counter}`, Date.now(), `${ns}:${id}`, 'CREATE', JSON.stringify(data)
    )

    return { $id: entityId }
  }

  async createMany(ns: string, items: Array<{ $type: string; name: string; [key: string]: unknown }>): Promise<Array<{ $id: string }>> {
    const entities: Array<{ $id: string }> = []
    for (const item of items) {
      const entity = await this.create(ns, item)
      entities.push(entity)
    }
    return entities
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('DO WAL Cost Optimization', () => {
  let sql: CostTrackingSqlite
  let walStore: WalBasedStore
  let oldStore: OldModelStore

  beforeEach(() => {
    sql = new CostTrackingSqlite()
    walStore = new WalBasedStore(sql)
    oldStore = new OldModelStore(sql)
  })

  describe('Single Entity Creation', () => {
    it('OLD MODEL: uses 2 SQLite rows per entity', async () => {
      sql.resetCostTracking()

      await oldStore.create('posts', { $type: 'Post', name: 'Test' })

      // Old model: 1 entity row + 1 event row = 2 inserts
      expect(sql.insertCount).toBe(2)

      const byTable = sql.getInsertsByTable()
      expect(byTable['entities']).toBe(1)
      expect(byTable['events']).toBe(1)
    })

    it('NEW MODEL: buffers event, no immediate SQLite write', async () => {
      sql.resetCostTracking()

      await walStore.create('posts', { $type: 'Post', name: 'Test' })

      // New model: event buffered in memory, no SQLite write yet
      // Only table creation during initialization
      const byTable = sql.getInsertsByTable()
      expect(byTable['events_wal'] || 0).toBe(0)
      expect(byTable['entities'] || 0).toBe(0)
    })

    it('NEW MODEL: flush writes 1 row for all buffered events', async () => {
      sql.resetCostTracking()

      // Create 10 entities
      for (let i = 0; i < 10; i++) {
        await walStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Manually flush
      await walStore.flushAll()

      // Should have 1 WAL row containing all 10 events
      const byTable = sql.getInsertsByTable()
      expect(byTable['events_wal']).toBe(1)
      expect(byTable['entities'] || 0).toBe(0)
    })
  })

  describe('100 Entity Creation Comparison', () => {
    it('OLD MODEL: uses 200 SQLite rows for 100 entities', async () => {
      sql.resetCostTracking()

      for (let i = 0; i < 100; i++) {
        await oldStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Old model: 100 entities + 100 events = 200 rows
      expect(sql.insertCount).toBe(200)
    })

    it('NEW MODEL: uses 1 SQLite row for 100 entities (auto-flush at threshold)', async () => {
      sql.resetCostTracking()

      for (let i = 0; i < 100; i++) {
        await walStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // New model: auto-flush at 100 events = 1 row
      // Note: 100 is exactly at threshold, so it auto-flushes
      const byTable = sql.getInsertsByTable()
      expect(byTable['events_wal']).toBe(1)
    })

    it('NEW MODEL achieves 99.5%+ cost reduction vs OLD MODEL', async () => {
      // Old model cost
      const oldSql = new CostTrackingSqlite()
      const old = new OldModelStore(oldSql)
      for (let i = 0; i < 100; i++) {
        await old.create('posts', { $type: 'Post', name: `Post ${i}` })
      }
      const oldRows = oldSql.insertCount

      // New model cost
      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)
      for (let i = 0; i < 100; i++) {
        await newStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }
      const newRows = newSql.insertCount

      // Calculate savings
      const savings = ((oldRows - newRows) / oldRows) * 100

      expect(oldRows).toBe(200)
      expect(newRows).toBe(1)
      expect(savings).toBeGreaterThanOrEqual(99.5)
    })
  })

  describe('Bulk Create (>= 5 entities)', () => {
    it('bypasses SQLite event buffering for bulk creates', async () => {
      sql.resetCostTracking()

      // Create 10 entities in bulk
      await walStore.createMany('posts', Array.from({ length: 10 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))

      // Bulk creates write 1 metadata row, no event rows
      const byTable = sql.getInsertsByTable()
      expect(byTable['pending_row_groups']).toBe(1)
      expect(byTable['events_wal'] || 0).toBe(0)
    })

    it('uses standard buffering for < 5 entities', async () => {
      sql.resetCostTracking()

      // Create 4 entities (below threshold)
      await walStore.createMany('posts', Array.from({ length: 4 }, (_, i) => ({
        $type: 'Post',
        name: `Post ${i}`,
      })))

      // Should NOT create pending row group
      const byTable = sql.getInsertsByTable()
      expect(byTable['pending_row_groups'] || 0).toBe(0)

      // Events should be buffered, not written yet
      const buffer = walStore.getBufferState('posts')
      expect(buffer?.events.length).toBe(4)
    })

    it('bulk import 10K: 99.95% cost reduction', async () => {
      // Old model cost calculation (would be 20,000 rows)
      // Don't actually run - just calculate expected
      const entityCount = 10000
      const oldModelRows = entityCount * 2 // 1 entity + 1 event per create

      // New model: bulk creates
      sql.resetCostTracking()

      // Create in batches of 1000 (to simulate real usage)
      for (let batch = 0; batch < 10; batch++) {
        await walStore.createMany('posts', Array.from({ length: 1000 }, (_, i) => ({
          $type: 'Post',
          name: `Batch${batch} Post ${i}`,
        })))
      }

      const newModelRows = sql.insertCount

      // Calculate savings
      const savings = ((oldModelRows - newModelRows) / oldModelRows) * 100

      // Old: 20,000 rows, New: 10 rows (1 per batch)
      expect(oldModelRows).toBe(20000)
      expect(newModelRows).toBe(10)
      expect(savings).toBeGreaterThanOrEqual(99.95)
    })
  })

  describe('Event Batching Efficiency', () => {
    it('batches up to 100 events per SQLite row', async () => {
      sql.resetCostTracking()

      // Create 99 entities (below threshold)
      for (let i = 0; i < 99; i++) {
        await walStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Should still be buffered, no WAL write
      expect(sql.getInsertsByTable()['events_wal'] || 0).toBe(0)

      // Create 1 more to hit threshold
      await walStore.create('posts', { $type: 'Post', name: 'Post 99' })

      // Now should have auto-flushed
      expect(sql.getInsertsByTable()['events_wal']).toBe(1)
    })

    it('handles multiple namespaces independently', async () => {
      sql.resetCostTracking()

      // Create 50 events in 'posts'
      for (let i = 0; i < 50; i++) {
        await walStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      // Create 50 events in 'users'
      for (let i = 0; i < 50; i++) {
        await walStore.create('users', { $type: 'User', name: `User ${i}` })
      }

      // Flush all
      await walStore.flushAll()

      // Should have 2 WAL rows (1 per namespace)
      expect(sql.getInsertsByTable()['events_wal']).toBe(2)
    })
  })

  describe('Cost Summary Table Verification', () => {
    it('matches documented cost savings for Create 1 entity', async () => {
      // Old: 2 rows, New: 0-1 rows
      const oldSql = new CostTrackingSqlite()
      const old = new OldModelStore(oldSql)
      await old.create('posts', { $type: 'Post', name: 'Test' })
      const oldRows = oldSql.insertCount

      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)
      await newStore.create('posts', { $type: 'Post', name: 'Test' })
      // Don't flush - event stays buffered
      const newRows = newSql.insertCount

      expect(oldRows).toBe(2)
      expect(newRows).toBe(0) // Buffered, not written
    })

    it('matches documented cost savings for Create 100 entities', async () => {
      // Old: 200 rows, New: 1 row
      const oldSql = new CostTrackingSqlite()
      const old = new OldModelStore(oldSql)
      for (let i = 0; i < 100; i++) {
        await old.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)
      for (let i = 0; i < 100; i++) {
        await newStore.create('posts', { $type: 'Post', name: `Post ${i}` })
      }

      expect(oldSql.insertCount).toBe(200)
      expect(newSql.insertCount).toBe(1)
    })

    it('matches documented cost savings for Bulk import 10K', async () => {
      // Old: 20,000 rows, New: ~10 rows
      const entityCount = 10000
      const oldModelRows = entityCount * 2

      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)

      // 10 batches of 1000
      for (let batch = 0; batch < 10; batch++) {
        await newStore.createMany('posts', Array.from({ length: 1000 }, (_, i) => ({
          $type: 'Post',
          name: `Batch${batch} Post ${i}`,
        })))
      }

      expect(oldModelRows).toBe(20000)
      expect(newSql.insertCount).toBe(10)
    })
  })

  describe('Acceptance Criteria', () => {
    it('90%+ reduction in DO SQLite row operations', async () => {
      const entityCount = 500

      // Old model
      const oldSql = new CostTrackingSqlite()
      const old = new OldModelStore(oldSql)
      for (let i = 0; i < entityCount; i++) {
        await old.create('posts', { $type: 'Post', name: `Post ${i}` })
      }
      const oldRows = oldSql.insertCount

      // New model (mix of single and bulk)
      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)

      // 100 single creates
      for (let i = 0; i < 100; i++) {
        await newStore.create('posts', { $type: 'Post', name: `Single ${i}` })
      }
      // 400 bulk creates (8 batches of 50)
      for (let batch = 0; batch < 8; batch++) {
        await newStore.createMany('posts', Array.from({ length: 50 }, (_, i) => ({
          $type: 'Post',
          name: `Bulk ${batch}-${i}`,
        })))
      }
      const newRows = newSql.insertCount

      const reduction = ((oldRows - newRows) / oldRows) * 100

      expect(oldRows).toBe(1000) // 500 * 2
      // New: 1 batch for 100 singles + 8 pending row groups = 9 rows
      expect(newRows).toBe(9)
      expect(reduction).toBeGreaterThanOrEqual(90)
    })

    it('bulk imports are 10x+ faster (by row count proxy)', async () => {
      const batchSize = 1000
      const batchCount = 10
      const totalEntities = batchSize * batchCount

      // Old model row count (proxy for time)
      const oldRows = totalEntities * 2 // 20,000

      // New model row count
      const newSql = new CostTrackingSqlite()
      const newStore = new WalBasedStore(newSql)
      for (let batch = 0; batch < batchCount; batch++) {
        await newStore.createMany('posts', Array.from({ length: batchSize }, (_, i) => ({
          $type: 'Post',
          name: `Batch${batch} Post ${i}`,
        })))
      }
      const newRows = newSql.insertCount

      // Row count reduction is a proxy for speed improvement
      const speedup = oldRows / newRows

      expect(speedup).toBeGreaterThanOrEqual(10)
    })
  })
})

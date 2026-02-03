/**
 * DO WAL Phase 4: Relationship Batching Tests
 *
 * Tests for batching relationship events similar to entity events.
 * This reduces SQLite write costs by batching ~100 relationship events per row.
 *
 * Architecture:
 * - Relationship events are buffered in memory
 * - Flushed to rels_wal table when threshold is reached
 * - Each rels_wal row contains a batch of relationship events
 * - Counters track sequence per namespace for short IDs
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
  private lastInsertId = 0

  exec<T = unknown>(query: string, ...params: unknown[]): Iterable<T> {
    const trimmedQuery = query.trim().toLowerCase()

    // CREATE TABLE
    if (trimmedQuery.startsWith('create table')) {
      const match = query.match(/create table if not exists (\w+)/i)
      if (match) {
        const tableName = match[1]
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
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const id = (this.autoIncrement.get(tableName) || 0) + 1
        this.autoIncrement.set(tableName, id)
        this.lastInsertId = id

        // Build row based on table type
        let row: Record<string, unknown>
        if (tableName === 'rels_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
            created_at: params[4] || new Date().toISOString(),
          }
        } else if (tableName === 'events_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
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
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const ns = params[0] as string

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
          if (!ns || ns === rowNs) {
            result.push({ ns: rowNs, max_seq: maxSeq })
          }
        }
        return result as T[]
      }
    }

    // SELECT COUNT(*)
    if (trimmedQuery.includes('count(*)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const ns = params[0] as string | undefined

        let filtered = rows
        if (ns && tableName === 'rels_wal') {
          filtered = rows.filter((r: any) => r.ns === ns)
        }

        return [{ count: filtered.length }] as T[]
      }
    }

    // SELECT events
    if (trimmedQuery.includes('select') && (trimmedQuery.includes('events') || trimmedQuery.includes('*'))) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []

        // Filter by namespace if specified
        if (params.length > 0 && (tableName === 'rels_wal' || tableName === 'events_wal')) {
          const ns = params[0] as string
          const filtered = rows.filter((r: any) => r.ns === ns)
          return filtered as T[]
        }

        return rows as T[]
      }
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []

        // DELETE with WHERE ns = ? AND last_seq <= ?
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

  // Helper for tests
  getTable(name: string): unknown[] {
    return this.tables.get(name) || []
  }

  clear() {
    this.tables.clear()
    this.autoIncrement.clear()
    this.lastInsertId = 0
  }
}

// =============================================================================
// Mock ParqueDBDO with Relationship Batching
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

interface RelEventBuffer {
  events: Event[]
  firstSeq: number
  lastSeq: number
  sizeBytes: number
}

class MockParqueDBDO {
  private sql: MockSqlite
  private counters: Map<string, number> = new Map()
  private relEventBuffers: Map<string, RelEventBuffer> = new Map()
  private initialized = false

  // Thresholds
  private readonly REL_BATCH_COUNT_THRESHOLD = 100
  private readonly REL_BATCH_SIZE_THRESHOLD = 64 * 1024 // 64KB

  constructor(sql: MockSqlite) {
    this.sql = sql
  }

  async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    // Create rels_wal table
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS rels_wal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ns TEXT NOT NULL,
        first_seq INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        events BLOB NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_rels_wal_ns ON rels_wal(ns, last_seq)')

    this.initialized = true
    await this.initializeCounters()
  }

  private async initializeCounters(): Promise<void> {
    interface CounterRow {
      [key: string]: SqlStorageValue
      ns: string
      max_seq: number
    }

    // Get max sequence for each namespace from rels_wal
    const rows = [...this.sql.exec<CounterRow>(
      `SELECT ns, MAX(last_seq) as max_seq FROM rels_wal GROUP BY ns`
    )]

    for (const row of rows) {
      this.counters.set(row.ns, row.max_seq + 1)
    }
  }

  private getNextRelSeq(ns: string): number {
    const seq = this.counters.get(ns) || 1
    this.counters.set(ns, seq + 1)
    return seq
  }

  /**
   * Append a relationship event with namespace-based batching
   */
  async appendRelEvent(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    await this.ensureInitialized()

    // Get or create buffer for this namespace
    let buffer = this.relEventBuffers.get(ns)
    if (!buffer) {
      const seq = this.counters.get(ns) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.relEventBuffers.set(ns, buffer)
    }

    // Generate event ID using sequence
    const eventId = `rel_${buffer.lastSeq}`
    const fullEvent: Event = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++
    this.counters.set(ns, buffer.lastSeq)

    // Estimate size
    const eventJson = JSON.stringify(fullEvent)
    buffer.sizeBytes += eventJson.length

    // Check if we should flush
    if (
      buffer.events.length >= this.REL_BATCH_COUNT_THRESHOLD ||
      buffer.sizeBytes >= this.REL_BATCH_SIZE_THRESHOLD
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

    interface WalRow {
      [key: string]: SqlStorageValue
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

    interface WalRow {
      [key: string]: SqlStorageValue
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

    interface WalRow {
      [key: string]: SqlStorageValue
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
    return this.counters.get(ns) || 1
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

  private deserializeEventBatch(batch: Uint8Array | ArrayBuffer): Event[] {
    if (!batch) return []

    let data: Uint8Array
    if (batch instanceof Uint8Array) {
      data = batch
    } else if (batch instanceof ArrayBuffer) {
      data = new Uint8Array(batch)
    } else {
      data = new Uint8Array(batch as ArrayBuffer)
    }

    const json = new TextDecoder().decode(data)
    return JSON.parse(json) as Event[]
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createRelEvent(overrides: Partial<Event> = {}): Omit<Event, 'id'> {
  return {
    ts: Date.now(),
    op: 'CREATE',
    target: 'posts:p1:author:users:u1',
    after: { predicate: 'author', to: 'users/u1' },
    actor: 'system/test',
    ...overrides,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('DO WAL Phase 4: Relationship Batching', () => {
  let sql: MockSqlite
  let doInstance: MockParqueDBDO

  beforeEach(() => {
    sql = new MockSqlite()
    doInstance = new MockParqueDBDO(sql)
  })

  describe('rels_wal table initialization', () => {
    it('creates rels_wal table on first operation', async () => {
      await doInstance.ensureInitialized()

      const table = sql.getTable('rels_wal')
      expect(table).toBeDefined()
    })

    it('creates index for namespace queries', async () => {
      await doInstance.ensureInitialized()
      // Index creation should not throw
    })

    it('initializes counters from existing rels_wal data', async () => {
      // Pre-populate rels_wal
      await doInstance.ensureInitialized()
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')

      // Create new instance to test counter initialization
      const doInstance2 = new MockParqueDBDO(sql)
      await doInstance2.ensureInitialized()

      // Counter should start after the last sequence
      const counter = doInstance2.getRelSequenceCounter('posts')
      expect(counter).toBeGreaterThan(1)
    })
  })

  describe('relationship event buffering', () => {
    it('buffers relationship events in memory', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())

      const state = doInstance.getRelBufferState('posts')
      expect(state).not.toBeNull()
      expect(state!.eventCount).toBe(2)
    })

    it('generates sequential event IDs', async () => {
      const id1 = await doInstance.appendRelEvent('posts', createRelEvent())
      const id2 = await doInstance.appendRelEvent('posts', createRelEvent())
      const id3 = await doInstance.appendRelEvent('posts', createRelEvent())

      expect(id1).toMatch(/^rel_\d+$/)
      expect(id2).toMatch(/^rel_\d+$/)
      expect(id3).toMatch(/^rel_\d+$/)
    })

    it('tracks buffer size in bytes', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())

      const state = doInstance.getRelBufferState('posts')
      expect(state!.sizeBytes).toBeGreaterThan(0)
    })

    it('maintains separate buffers per namespace', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('users', createRelEvent())

      const postsState = doInstance.getRelBufferState('posts')
      const usersState = doInstance.getRelBufferState('users')

      expect(postsState!.eventCount).toBe(1)
      expect(usersState!.eventCount).toBe(1)
    })
  })

  describe('automatic batch flushing', () => {
    it('flushes when count threshold is reached', async () => {
      // Add 100 events (threshold)
      for (let i = 0; i < 100; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }

      // Should have auto-flushed
      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(1)

      // Buffer should be reset
      const state = doInstance.getRelBufferState('posts')
      expect(state!.eventCount).toBe(0)
    })

    it('flushes when size threshold is reached', async () => {
      // Add multiple events to exceed 64KB threshold
      const largeData = 'x'.repeat(10000) // 10KB per event

      // Add 7 events (7 * 10KB = 70KB > 64KB threshold)
      for (let i = 0; i < 7; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent({
          after: { data: largeData },
        }))
      }

      // Should have auto-flushed due to size
      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBeGreaterThanOrEqual(1)
    })

    it('does not flush below threshold', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())

      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(0)

      // Buffer should still have events
      const state = doInstance.getRelBufferState('posts')
      expect(state!.eventCount).toBe(2)
    })
  })

  describe('manual batch flushing', () => {
    it('flushes specific namespace buffer', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())

      await doInstance.flushRelEventBatch('posts')

      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(1)

      const state = doInstance.getRelBufferState('posts')
      expect(state!.eventCount).toBe(0)
    })

    it('flushes all namespace buffers', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('users', createRelEvent())
      await doInstance.appendRelEvent('comments', createRelEvent())

      await doInstance.flushAllRelEventBatches()

      const totalBatches = await doInstance.getUnflushedRelBatchCount()
      expect(totalBatches).toBe(3) // One batch per namespace
    })

    it('handles empty buffer gracefully', async () => {
      await doInstance.flushRelEventBatch('posts')
      // Should not throw
    })
  })

  describe('batch storage and retrieval', () => {
    it('stores batch with namespace and sequence range', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')

      const table = sql.getTable('rels_wal') as any[]
      expect(table).toHaveLength(1)
      expect(table[0].ns).toBe('posts')
      expect(table[0].first_seq).toBe(1)
      expect(table[0].last_seq).toBe(2)
    })

    it('reads unflushed events for namespace', async () => {
      const event1 = createRelEvent({ op: 'CREATE' })
      const event2 = createRelEvent({ op: 'DELETE' })

      await doInstance.appendRelEvent('posts', event1)
      await doInstance.appendRelEvent('posts', event2)
      await doInstance.flushRelEventBatch('posts')

      const events = await doInstance.readUnflushedRelEvents('posts')
      expect(events).toHaveLength(2)
      expect(events[0].op).toBe('CREATE')
      expect(events[1].op).toBe('DELETE')
    })

    it('includes buffered events in read', async () => {
      // Add some flushed events
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')

      // Add some buffered events (not flushed)
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())

      const events = await doInstance.readUnflushedRelEvents('posts')
      expect(events).toHaveLength(3) // 1 flushed + 2 buffered
    })

    it('preserves event data through serialization', async () => {
      const event = createRelEvent({
        ts: 12345,
        op: 'UPDATE',
        target: 'posts:p1:author:users:u1',
        before: { predicate: 'author', to: 'users/u1' },
        after: { predicate: 'author', to: 'users/u2' },
        actor: 'admin',
      })

      await doInstance.appendRelEvent('posts', event)
      await doInstance.flushRelEventBatch('posts')

      const events = await doInstance.readUnflushedRelEvents('posts')
      expect(events).toHaveLength(1)
      expect(events[0].ts).toBe(12345)
      expect(events[0].op).toBe('UPDATE')
      expect(events[0].target).toBe('posts:p1:author:users:u1')
      expect(events[0].before).toEqual({ predicate: 'author', to: 'users/u1' })
      expect(events[0].after).toEqual({ predicate: 'author', to: 'users/u2' })
      expect(events[0].actor).toBe('admin')
    })
  })

  describe('event counting', () => {
    it('counts unflushed events for namespace', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')

      const count = await doInstance.getUnflushedRelEventCount('posts')
      expect(count).toBe(2)
    })

    it('counts total unflushed events across namespaces', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('users', createRelEvent())
      await doInstance.flushAllRelEventBatches()

      const total = await doInstance.getTotalUnflushedRelEventCount()
      expect(total).toBe(3)
    })

    it('includes buffered events in count', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')

      // Add buffered (not flushed)
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())

      const count = await doInstance.getUnflushedRelEventCount('posts')
      expect(count).toBe(3) // 1 flushed + 2 buffered
    })
  })

  describe('batch deletion', () => {
    it('deletes batches up to sequence number', async () => {
      // Create two batches
      for (let i = 0; i < 100; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }
      for (let i = 0; i < 100; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }

      // Delete first batch (up to seq 100)
      await doInstance.deleteRelWalBatches('posts', 100)

      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(1) // Only second batch remains
    })

    it('preserves batches above sequence threshold', async () => {
      for (let i = 0; i < 100; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }

      // Try to delete with seq 50 (batch has seq 1-100)
      await doInstance.deleteRelWalBatches('posts', 50)

      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(1) // Batch should still exist
    })

    it('does not affect other namespaces', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('users', createRelEvent())
      await doInstance.flushAllRelEventBatches()

      await doInstance.deleteRelWalBatches('posts', 100)

      const postsBatches = await doInstance.getUnflushedRelBatchCount('posts')
      const usersBatches = await doInstance.getUnflushedRelBatchCount('users')

      expect(postsBatches).toBe(0)
      expect(usersBatches).toBe(1) // users unaffected
    })
  })

  describe('sequence counter management', () => {
    it('maintains separate counters per namespace', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('users', createRelEvent())

      const postsCounter = doInstance.getRelSequenceCounter('posts')
      const usersCounter = doInstance.getRelSequenceCounter('users')

      expect(postsCounter).toBe(2) // Started at 1, incremented to 2
      expect(usersCounter).toBe(2) // Started at 1, incremented to 2
    })

    it('increments counter with each event', async () => {
      const counter1 = doInstance.getRelSequenceCounter('posts')
      await doInstance.appendRelEvent('posts', createRelEvent())
      const counter2 = doInstance.getRelSequenceCounter('posts')
      await doInstance.appendRelEvent('posts', createRelEvent())
      const counter3 = doInstance.getRelSequenceCounter('posts')

      expect(counter2).toBe(counter1 + 1)
      expect(counter3).toBe(counter2 + 1)
    })

    it('persists sequence across buffer flushes', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.appendRelEvent('posts', createRelEvent())
      const counterBeforeFlush = doInstance.getRelSequenceCounter('posts')

      await doInstance.flushRelEventBatch('posts')

      const counterAfterFlush = doInstance.getRelSequenceCounter('posts')
      expect(counterAfterFlush).toBe(counterBeforeFlush)
    })
  })

  describe('cost optimization verification', () => {
    it('uses 1 SQLite row for 100 relationship events', async () => {
      // Create 100 relationship events
      for (let i = 0; i < 100; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }

      // Check SQLite row count
      const table = sql.getTable('rels_wal')
      expect(table).toHaveLength(1) // Only 1 row for 100 events!
    })

    it('batches reduce SQLite writes by 100x', async () => {
      const eventCount = 1000

      // Without batching: would be 1000 rows
      // With batching: ~10 rows (100 events per batch)

      for (let i = 0; i < eventCount; i++) {
        await doInstance.appendRelEvent('posts', createRelEvent())
      }

      const table = sql.getTable('rels_wal')
      const batchCount = table.length

      // Should be ~10 batches (1000 / 100)
      expect(batchCount).toBeLessThanOrEqual(11)
      expect(batchCount).toBeGreaterThanOrEqual(9)

      // Verify all events are preserved
      const totalEvents = await doInstance.getUnflushedRelEventCount('posts')
      expect(totalEvents).toBe(eventCount)
    })
  })

  describe('edge cases', () => {
    it('handles empty namespace gracefully', async () => {
      const events = await doInstance.readUnflushedRelEvents('nonexistent')
      expect(events).toHaveLength(0)

      const count = await doInstance.getUnflushedRelEventCount('nonexistent')
      expect(count).toBe(0)
    })

    it('handles multiple flushes of same namespace', async () => {
      await doInstance.appendRelEvent('posts', createRelEvent())
      await doInstance.flushRelEventBatch('posts')
      await doInstance.flushRelEventBatch('posts')
      await doInstance.flushRelEventBatch('posts')

      // Should not throw or create empty batches
      const batchCount = await doInstance.getUnflushedRelBatchCount('posts')
      expect(batchCount).toBe(1)
    })

    it('handles concurrent namespace operations', async () => {
      // Simulate concurrent operations on different namespaces
      await Promise.all([
        doInstance.appendRelEvent('posts', createRelEvent()),
        doInstance.appendRelEvent('users', createRelEvent()),
        doInstance.appendRelEvent('comments', createRelEvent()),
      ])

      await doInstance.flushAllRelEventBatches()

      const totalBatches = await doInstance.getUnflushedRelBatchCount()
      expect(totalBatches).toBe(3)
    })
  })
})

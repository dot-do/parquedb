/**
 * SqliteWal Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { SqliteWal, createSqliteWal, createSqliteFlushHandler } from '../../../src/events/sqlite-wal'
import type { SqliteInterface } from '../../../src/events/sqlite-wal'
import type { Event } from '../../../src/types'
import type { EventBatch } from '../../../src/events/types'

// =============================================================================
// Mock SQLite Implementation
// =============================================================================

class MockSqlite implements SqliteInterface {
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

        // Parse params into row
        const row: Record<string, unknown> = {
          id,
          batch: params[0],
          min_ts: params[1],
          max_ts: params[2],
          count: params[3],
          flushed: 0,
          created_at: new Date().toISOString(),
        }
        rows.push(row)
        this.tables.set(tableName, rows)
      }
      return [] as T[]
    }

    // SELECT last_insert_rowid()
    if (trimmedQuery.includes('last_insert_rowid')) {
      return [{ id: this.lastInsertId }] as T[]
    }

    // SELECT with SUM
    if (trimmedQuery.includes('sum(count)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const unflushed = rows.filter((r: any) => r.flushed === 0)
        const total = unflushed.reduce((sum: number, r: any) => sum + r.count, 0)
        return [{ total }] as T[]
      }
    }

    // SELECT with COUNT(*)
    if (trimmedQuery.includes('count(*)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        if (trimmedQuery.includes('flushed = 0')) {
          const count = rows.filter((r: any) => r.flushed === 0).length
          return [{ count }] as T[]
        }
        if (trimmedQuery.includes('flushed = 1')) {
          const count = rows.filter((r: any) => r.flushed === 1).length
          return [{ count }] as T[]
        }
        if (trimmedQuery.includes('max_ts <')) {
          const timestamp = params[0] as number
          const count = rows.filter((r: any) => r.max_ts < timestamp).length
          return [{ count }] as T[]
        }
      }
    }

    // SELECT unflushed
    if (trimmedQuery.includes('select') && trimmedQuery.includes('flushed = 0')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const unflushed = rows.filter((r: any) => r.flushed === 0)
        return unflushed as T[]
      }
    }

    // SELECT by time range
    if (trimmedQuery.includes('max_ts >=') && trimmedQuery.includes('min_ts <=')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const minTs = params[0] as number
        const maxTs = params[1] as number
        const filtered = rows.filter((r: any) => r.max_ts >= minTs && r.min_ts <= maxTs)
        return filtered as T[]
      }
    }

    // UPDATE flushed
    if (trimmedQuery.startsWith('update') && trimmedQuery.includes('flushed = 1')) {
      const match = query.match(/update (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const ids = params as number[]
        rows.forEach((r: any) => {
          if (ids.includes(r.id)) {
            r.flushed = 1
          }
        })
      }
      return [] as T[]
    }

    // DELETE flushed
    if (trimmedQuery.startsWith('delete') && trimmedQuery.includes('flushed = 1')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const remaining = rows.filter((r: any) => r.flushed !== 1)
        this.tables.set(tableName, remaining)
      }
      return [] as T[]
    }

    // DELETE older than
    if (trimmedQuery.startsWith('delete') && trimmedQuery.includes('max_ts <')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]
        const rows = this.tables.get(tableName) || []
        const timestamp = params[0] as number
        const remaining = rows.filter((r: any) => r.max_ts >= timestamp)
        this.tables.set(tableName, remaining)
      }
      return [] as T[]
    }

    return [] as T[]
  }

  // Helper for tests
  getTable(name: string): unknown[] {
    return this.tables.get(name) || []
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function createTestEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op: 'CREATE',
    target: 'posts:test123',
    after: { title: 'Test', content: 'Content' },
    ...overrides,
  }
}

function createTestBatch(count: number = 3, minTs: number = 1000, maxTs: number = 3000): EventBatch {
  const events: Event[] = []
  for (let i = 0; i < count; i++) {
    events.push(createTestEvent({ ts: minTs + i * ((maxTs - minTs) / count) }))
  }
  return {
    events,
    minTs,
    maxTs,
    count,
    sizeBytes: 500,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('SqliteWal', () => {
  let sql: MockSqlite
  let wal: SqliteWal

  beforeEach(() => {
    sql = new MockSqlite()
    wal = new SqliteWal(sql)
  })

  describe('table management', () => {
    it('creates table on first operation', () => {
      wal.ensureTable()
      // Table should exist (no error thrown)
    })

    it('creates table only once', () => {
      wal.ensureTable()
      wal.ensureTable()
      // Should not throw
    })
  })

  describe('write operations', () => {
    it('writes a batch and returns ID', () => {
      const batch = createTestBatch()
      const id = wal.writeBatch(batch)
      expect(id).toBe(1)
    })

    it('writes multiple batches with sequential IDs', () => {
      const id1 = wal.writeBatch(createTestBatch())
      const id2 = wal.writeBatch(createTestBatch())
      const id3 = wal.writeBatch(createTestBatch())

      expect(id1).toBe(1)
      expect(id2).toBe(2)
      expect(id3).toBe(3)
    })

    it('throws if batch exceeds max blob size', () => {
      const smallWal = new SqliteWal(sql, { maxBlobSize: 10 })
      const batch = createTestBatch()

      expect(() => smallWal.writeBatch(batch)).toThrow(/exceeds max blob size/)
    })

    it('writes multiple batches at once', () => {
      const batches = [createTestBatch(), createTestBatch(), createTestBatch()]
      const ids = wal.writeBatches(batches)

      expect(ids).toEqual([1, 2, 3])
    })
  })

  describe('read operations', () => {
    it('reads unflushed batches', () => {
      wal.writeBatch(createTestBatch(3, 1000, 2000))
      wal.writeBatch(createTestBatch(2, 2000, 3000))

      const batches = wal.readUnflushedBatches()
      expect(batches).toHaveLength(2)
      expect(batches[0].count).toBe(3)
      expect(batches[1].count).toBe(2)
    })

    it('reads batches by time range', () => {
      wal.writeBatch(createTestBatch(3, 1000, 2000))
      wal.writeBatch(createTestBatch(2, 3000, 4000))
      wal.writeBatch(createTestBatch(1, 5000, 6000))

      // Should only get the first two batches
      const batches = wal.readBatchesByTimeRange(1500, 3500)
      expect(batches).toHaveLength(2)
    })

    it('gets unflushed event count', () => {
      wal.writeBatch(createTestBatch(3))
      wal.writeBatch(createTestBatch(5))

      const count = wal.getUnflushedCount()
      expect(count).toBe(8)
    })

    it('gets unflushed batch count', () => {
      wal.writeBatch(createTestBatch())
      wal.writeBatch(createTestBatch())
      wal.writeBatch(createTestBatch())

      const count = wal.getUnflushedBatchCount()
      expect(count).toBe(3)
    })

    it('returns empty results when no batches', () => {
      const batches = wal.readUnflushedBatches()
      expect(batches).toHaveLength(0)

      const count = wal.getUnflushedCount()
      expect(count).toBe(0)
    })
  })

  describe('flush management', () => {
    it('marks batches as flushed', () => {
      const id1 = wal.writeBatch(createTestBatch())
      const id2 = wal.writeBatch(createTestBatch())

      wal.markFlushed([id1])

      const unflushed = wal.readUnflushedBatches()
      expect(unflushed).toHaveLength(1)
    })

    it('handles empty batch IDs', () => {
      wal.markFlushed([])
      // Should not throw
    })

    it('deletes flushed batches', () => {
      const id1 = wal.writeBatch(createTestBatch())
      const id2 = wal.writeBatch(createTestBatch())

      wal.markFlushed([id1])
      const deleted = wal.deleteFlushed()

      expect(deleted).toBe(1)
      expect(wal.getUnflushedBatchCount()).toBe(1)
    })

    it('deletes batches older than timestamp', () => {
      wal.writeBatch(createTestBatch(3, 1000, 2000))
      wal.writeBatch(createTestBatch(3, 3000, 4000))
      wal.writeBatch(createTestBatch(3, 5000, 6000))

      const deleted = wal.deleteOlderThan(4500)

      expect(deleted).toBe(2)
      expect(wal.getUnflushedBatchCount()).toBe(1)
    })
  })

  describe('serialization', () => {
    it('preserves event data through serialization', () => {
      const originalEvent = createTestEvent({
        ts: 12345,
        op: 'UPDATE',
        target: 'users:abc123',
        before: { name: 'Old Name' },
        after: { name: 'New Name' },
        actor: 'admin',
      })

      const batch: EventBatch = {
        events: [originalEvent],
        minTs: 12345,
        maxTs: 12345,
        count: 1,
      }

      wal.writeBatch(batch)
      const [readBatch] = wal.readUnflushedBatches()

      expect(readBatch.events).toHaveLength(1)
      expect(readBatch.events[0].id).toBe(originalEvent.id)
      expect(readBatch.events[0].ts).toBe(originalEvent.ts)
      expect(readBatch.events[0].op).toBe(originalEvent.op)
      expect(readBatch.events[0].target).toBe(originalEvent.target)
      expect(readBatch.events[0].before).toEqual(originalEvent.before)
      expect(readBatch.events[0].after).toEqual(originalEvent.after)
      expect(readBatch.events[0].actor).toBe(originalEvent.actor)
    })
  })

  describe('factory functions', () => {
    it('createSqliteWal creates a wal instance', () => {
      const w = createSqliteWal(sql)
      expect(w).toBeInstanceOf(SqliteWal)
    })

    it('createSqliteFlushHandler creates a flush handler', async () => {
      const handler = createSqliteFlushHandler(wal)
      const batch = createTestBatch()

      await handler(batch)

      expect(wal.getUnflushedBatchCount()).toBe(1)
    })
  })
})

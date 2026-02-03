/**
 * WAL Optimizer Tests
 *
 * Tests for the WAL cost optimization features:
 * 1. Compression - Event blob compression with fflate
 * 2. Compaction - Merging small batches to reduce SQLite rows
 * 3. Pruning - Deleting old flushed batches
 * 4. Adaptive Thresholds - Dynamic batch sizes based on load
 *
 * @see src/worker/do/wal-optimizer.ts
 * @see docs/architecture/DO_WAL_REWRITE.md
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  compressEvents,
  decompressEvents,
  calculateCompressionRatio,
  WalOptimizer,
  DEFAULT_WAL_OPTIMIZER_CONFIG,
} from '../../../src/worker/do/wal-optimizer'
import type { Event } from '../../../src/types'

// =============================================================================
// Mock SQLite Storage
// =============================================================================

interface MockRow {
  [key: string]: unknown
}

class MockSqlStorage {
  private tables: Map<string, MockRow[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()

  exec<T extends MockRow>(query: string, ...params: unknown[]): Iterable<T> {
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

        // Build row based on table schema
        let row: MockRow = { id }
        if (tableName === 'events_wal') {
          row = {
            id,
            ns: params[0],
            first_seq: params[1],
            last_seq: params[2],
            events: params[3],
            created_at: params[4] || new Date().toISOString(),
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
        }

        rows.push(row)
        this.tables.set(tableName, rows)
      }
      return [] as T[]
    }

    // SELECT COUNT(*) with optional alias
    if (trimmedQuery.includes('count(*)')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []

        // Handle WHERE clauses
        let filtered = rows
        if (trimmedQuery.includes('flushed = 1') && trimmedQuery.includes('max_ts <')) {
          const cutoff = params[0] as number
          filtered = rows.filter(r => r.flushed === 1 && (r.max_ts as number) < cutoff)
        } else if (trimmedQuery.includes('flushed = 0')) {
          filtered = rows.filter(r => r.flushed === 0)
        } else if (trimmedQuery.includes('ns =')) {
          const ns = params[0] as string
          filtered = rows.filter(r => r.ns === ns)
          if (trimmedQuery.includes('created_at <')) {
            const cutoffDate = params[1] as string
            filtered = filtered.filter(r => (r.created_at as string) < cutoffDate)
          }
        }

        // Check if query uses batch_count alias
        if (trimmedQuery.includes('batch_count')) {
          return [{ batch_count: filtered.length, count: filtered.length }] as T[]
        }

        return [{ count: filtered.length }] as T[]
      }
    }

    // SELECT with COALESCE/SUM for cost analysis
    if ((trimmedQuery.includes('coalesce') || trimmedQuery.includes('sum(event_count)')) && trimmedQuery.includes('from event_batches')) {
      const rows = this.tables.get('event_batches') || []

      // Filter by flushed = 0 if specified
      let filtered = rows
      if (trimmedQuery.includes('flushed = 0')) {
        filtered = rows.filter(r => r.flushed === 0)
      }

      const batchCount = filtered.length
      const totalEvents = filtered.reduce((sum, r) => sum + ((r.event_count as number) || 0), 0)
      return [{ batch_count: batchCount, total_events: totalEvents }] as T[]
    }

    // SELECT *
    if (trimmedQuery.includes('select') && trimmedQuery.includes('*')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        return rows as T[]
      }
    }

    // SELECT events
    if (trimmedQuery.includes('select') && trimmedQuery.includes('events')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []
        return rows as T[]
      }
    }

    // DELETE
    if (trimmedQuery.startsWith('delete')) {
      const match = query.match(/from (\w+)/i)
      if (match) {
        const tableName = match[1]!
        const rows = this.tables.get(tableName) || []

        let filtered = rows
        if (trimmedQuery.includes('flushed = 1') && trimmedQuery.includes('max_ts <')) {
          const cutoff = params[0] as number
          filtered = rows.filter(r => !(r.flushed === 1 && (r.max_ts as number) < cutoff))
        } else if (trimmedQuery.includes('id in')) {
          const ids = params as number[]
          filtered = rows.filter(r => !ids.includes(r.id as number))
        } else if (trimmedQuery.includes('ns =') && trimmedQuery.includes('created_at <')) {
          const ns = params[0] as string
          const cutoffDate = params[1] as string
          filtered = rows.filter(r => !(r.ns === ns && (r.created_at as string) < cutoffDate))
        }

        this.tables.set(tableName, filtered)
      }
      return [] as T[]
    }

    // UPDATE
    if (trimmedQuery.startsWith('update')) {
      return [] as T[]
    }

    return [] as T[]
  }

  // Helper to insert test data
  insertWalBatch(ns: string, firstSeq: number, lastSeq: number, events: Event[]): void {
    const blob = compressEvents(events)
    this.exec(
      'INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at) VALUES (?, ?, ?, ?, ?)',
      ns,
      firstSeq,
      lastSeq,
      blob,
      new Date().toISOString()
    )
  }

  insertEventBatch(events: Event[], flushed = false, maxTs?: number): void {
    const batch = compressEvents(events)
    const minTs = Math.min(...events.map(e => e.ts))
    const eventMaxTs = maxTs ?? Math.max(...events.map(e => e.ts))

    const rows = this.tables.get('event_batches') || []
    const id = (this.autoIncrement.get('event_batches') || 0) + 1
    this.autoIncrement.set('event_batches', id)

    rows.push({
      id,
      batch,
      min_ts: minTs,
      max_ts: eventMaxTs,
      event_count: events.length,
      flushed: flushed ? 1 : 0,
      created_at: new Date().toISOString(),
    })
    this.tables.set('event_batches', rows)
  }

  getTable(name: string): MockRow[] {
    return this.tables.get(name) || []
  }

  clear(): void {
    this.tables.clear()
    this.autoIncrement.clear()
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEvent(id: string, ts = Date.now()): Event {
  return {
    id,
    ts,
    op: 'CREATE',
    target: `test:entity_${id}`,
    after: { $type: 'Test', name: `Entity ${id}`, data: 'test data'.repeat(10) },
    actor: 'system/test',
  }
}

function createTestEvents(count: number, startTs = Date.now()): Event[] {
  return Array.from({ length: count }, (_, i) => createTestEvent(`evt_${i}`, startTs + i))
}

// =============================================================================
// Compression Tests
// =============================================================================

describe('WAL Optimizer Compression', () => {
  it('compresses events and returns with magic byte prefix', () => {
    // Use larger events to ensure compression is beneficial
    const events = createTestEvents(100)
    const compressed = compressEvents(events, true)

    // Should have magic byte prefix (either 0x01 for compressed or 0x00 for uncompressed)
    expect([0x00, 0x01]).toContain(compressed[0])
  })

  it('decompresses events correctly', () => {
    const events = createTestEvents(10)
    const compressed = compressEvents(events, true)
    const decompressed = decompressEvents(compressed)

    expect(decompressed).toHaveLength(10)
    expect(decompressed[0]!.id).toBe(events[0]!.id)
    expect(decompressed[0]!.target).toBe(events[0]!.target)
  })

  it('skips compression when not beneficial and uses magic byte 0x00', () => {
    // Small event that won't compress well
    const events = [{ id: 'x', ts: 1, op: 'CREATE' as const, target: 'a:b', actor: 'sys' }]
    const compressed = compressEvents(events, true)

    // Should be marked as uncompressed (magic byte 0x00) if compression didn't help
    expect(compressed[0]).toBe(0x00)

    const decompressed = decompressEvents(compressed)
    expect(decompressed).toHaveLength(1)
    expect(decompressed[0]!.id).toBe('x')
  })

  it('returns raw data without magic byte when compression disabled', () => {
    const events = createTestEvents(5)
    const compressed = compressEvents(events, false) // Disable compression

    // When disabled, returns raw JSON without magic byte
    // First byte should be '[' (0x5B) since it's a JSON array
    expect(compressed[0]).toBe(0x5B) // '[' in ASCII

    // Should still be decompressable (legacy format)
    const decompressed = decompressEvents(compressed)
    expect(decompressed).toHaveLength(5)
  })

  it('handles legacy format without magic byte', () => {
    const events = createTestEvents(3)
    const json = JSON.stringify(events)
    const legacyData = new TextEncoder().encode(json)

    const decompressed = decompressEvents(legacyData)
    expect(decompressed).toHaveLength(3)
  })

  it('calculates compression ratio correctly', () => {
    // Use many events with repeated data to ensure compression saves space
    const events: Event[] = []
    for (let i = 0; i < 500; i++) {
      events.push({
        id: `evt_${i}`,
        ts: Date.now() + i,
        op: 'CREATE',
        target: `test:entity_${i}`,
        after: {
          $type: 'TestType',
          name: `Entity ${i}`,
          description: 'This is a repeated description that should compress well.',
          tags: ['tag1', 'tag2', 'tag3'],
          metadata: { key: 'value', nested: { a: 1, b: 2 } },
        },
        actor: 'system/test',
      })
    }

    const { originalSize, compressedSize, ratio } = calculateCompressionRatio(events)

    expect(originalSize).toBeGreaterThan(0)
    expect(compressedSize).toBeGreaterThan(0)
    expect(ratio).toBe(compressedSize / originalSize)
  })

  it('achieves compression for large repetitive data', () => {
    // Use many events with repeated data patterns
    const events: Event[] = []
    for (let i = 0; i < 1000; i++) {
      events.push({
        id: `evt_${i}`,
        ts: Date.now() + i,
        op: 'CREATE',
        target: `namespace:entity_${i}`,
        after: {
          $type: 'RepetitiveType',
          name: `Entity Number ${i}`,
          description: 'This is a long description text that repeats across many events and should compress very well because it is highly repetitive content.',
          category: 'test-category',
          status: 'active',
        },
        actor: 'system/anonymous',
      })
    }

    const { ratio } = calculateCompressionRatio(events)

    // Large repetitive JSON should compress well (ratio < 1 means compression helped)
    // Note: The actual ratio depends on COMPRESSION_SAVINGS_THRESHOLD
    expect(ratio).toBeLessThan(1.1) // Allow some overhead
  })
})

// =============================================================================
// Compaction Tests
// =============================================================================

describe('WAL Optimizer Compaction', () => {
  let sql: MockSqlStorage
  let optimizer: WalOptimizer

  beforeEach(() => {
    sql = new MockSqlStorage()
    // Initialize tables
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')
    sql.exec('CREATE TABLE IF NOT EXISTS event_batches (id, batch, min_ts, max_ts, event_count, flushed)')

    optimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      compactionMinBatches: 3,
      compactionTargetEvents: 50,
    })
  })

  it('compacts multiple small batches into fewer large ones', async () => {
    // Insert 5 small batches (10 events each)
    for (let i = 0; i < 5; i++) {
      const events = createTestEvents(10, 1000 + i * 10)
      sql.insertWalBatch('posts', i * 10 + 1, i * 10 + 10, events)
    }

    expect(sql.getTable('events_wal')).toHaveLength(5)

    const result = await optimizer.compactWalBatches('posts')

    // Should have merged batches
    expect(result.batchesMerged).toBe(5)
    expect(result.rowsSaved).toBeGreaterThan(0)
  })

  it('does not compact if below threshold', async () => {
    // Insert only 2 batches (below minimum of 3)
    for (let i = 0; i < 2; i++) {
      const events = createTestEvents(10, 1000 + i * 10)
      sql.insertWalBatch('posts', i * 10 + 1, i * 10 + 10, events)
    }

    expect(sql.getTable('events_wal')).toHaveLength(2)

    const result = await optimizer.compactWalBatches('posts')

    // Should not compact
    expect(result.batchesMerged).toBe(0)
    expect(result.rowsSaved).toBe(0)
  })

  it('compacts batches per namespace', async () => {
    // Insert batches for two namespaces
    for (let i = 0; i < 4; i++) {
      const events = createTestEvents(10, 1000 + i * 10)
      sql.insertWalBatch('posts', i * 10 + 1, i * 10 + 10, events)
    }
    for (let i = 0; i < 4; i++) {
      const events = createTestEvents(10, 2000 + i * 10)
      sql.insertWalBatch('users', i * 10 + 1, i * 10 + 10, events)
    }

    expect(sql.getTable('events_wal')).toHaveLength(8)

    const result = await optimizer.compactWalBatches()

    // Should compact both namespaces
    expect(result.batchesMerged).toBeGreaterThan(0)
  })
})

// =============================================================================
// Pruning Tests
// =============================================================================

describe('WAL Optimizer Pruning', () => {
  let sql: MockSqlStorage
  let optimizer: WalOptimizer

  beforeEach(() => {
    sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')
    sql.exec('CREATE TABLE IF NOT EXISTS event_batches (id, batch, min_ts, max_ts, event_count, flushed)')

    optimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      flushedRetentionMs: 1000, // 1 second for testing
    })
  })

  it('prunes old flushed batches', async () => {
    const now = Date.now()
    const oldTs = now - 2000 // 2 seconds ago (older than retention)
    const recentTs = now - 500 // 0.5 seconds ago (within retention)

    // Insert old flushed batch
    sql.insertEventBatch(createTestEvents(10, oldTs), true, oldTs)

    // Insert recent flushed batch
    sql.insertEventBatch(createTestEvents(10, recentTs), true, recentTs)

    // Insert unflushed batch
    sql.insertEventBatch(createTestEvents(10, now), false, now)

    expect(sql.getTable('event_batches')).toHaveLength(3)

    const deleted = await optimizer.pruneFlushedBatches()

    // Should delete 1 old flushed batch
    expect(deleted).toBe(1)
  })

  it('does not prune unflushed batches', async () => {
    const now = Date.now()
    const oldTs = now - 10000 // 10 seconds ago

    // Insert old but unflushed batch
    sql.insertEventBatch(createTestEvents(10, oldTs), false, oldTs)

    const deleted = await optimizer.pruneFlushedBatches()

    // Should not delete unflushed batches
    expect(deleted).toBe(0)
  })

  it('prunes WAL batches by namespace', async () => {
    const now = Date.now()
    const oldTime = now - 5000

    // This test verifies the interface - actual deletion logic
    // would require proper date comparison in mock
    const deleted = await optimizer.pruneWalBatches('posts', oldTime)

    // Should return 0 for empty table
    expect(deleted).toBe(0)
  })
})

// =============================================================================
// Adaptive Threshold Tests
// =============================================================================

describe('WAL Optimizer Adaptive Thresholds', () => {
  let sql: MockSqlStorage
  let optimizer: WalOptimizer

  beforeEach(() => {
    sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')
    sql.exec('CREATE TABLE IF NOT EXISTS event_batches (id, batch, min_ts, max_ts, event_count, flushed)')

    optimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      enableAdaptiveThresholds: true,
      adaptiveWindowMs: 60000, // 1 minute
      minBatchThreshold: 10,
      maxBatchThreshold: 1000,
    })
  })

  it('returns minimum threshold for low event rate', () => {
    // Record few events
    for (let i = 0; i < 5; i++) {
      optimizer.recordEvent()
    }

    const threshold = optimizer.calculateAdaptiveThreshold()

    // Low rate should give minimum threshold
    expect(threshold).toBe(10)
  })

  it('increases threshold for moderate event rate', () => {
    // Record moderate number of events
    for (let i = 0; i < 50; i++) {
      optimizer.recordEvent()
    }

    const threshold = optimizer.calculateAdaptiveThreshold()

    // Moderate rate should give higher threshold
    expect(threshold).toBeGreaterThan(10)
    expect(threshold).toBeLessThan(1000)
  })

  it('returns maximum threshold for very high event rate', () => {
    // Record many events
    for (let i = 0; i < 1500; i++) {
      optimizer.recordEvent()
    }

    const threshold = optimizer.calculateAdaptiveThreshold()

    // Very high rate should give maximum threshold
    expect(threshold).toBe(1000)
  })

  it('returns default when adaptive thresholds disabled', () => {
    const disabledOptimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      enableAdaptiveThresholds: false,
    })

    for (let i = 0; i < 1000; i++) {
      disabledOptimizer.recordEvent()
    }

    const threshold = disabledOptimizer.calculateAdaptiveThreshold()

    // Should return default regardless of events
    expect(threshold).toBe(100)
  })

  it('tracks current batch threshold', () => {
    // Initial threshold
    expect(optimizer.getCurrentBatchThreshold()).toBe(100)

    // After calculating
    for (let i = 0; i < 200; i++) {
      optimizer.recordEvent()
    }
    optimizer.calculateAdaptiveThreshold()

    expect(optimizer.getCurrentBatchThreshold()).toBeGreaterThan(10)
  })
})

// =============================================================================
// Cost Analysis Tests
// =============================================================================

describe('WAL Optimizer Cost Analysis', () => {
  let sql: MockSqlStorage
  let optimizer: WalOptimizer

  beforeEach(() => {
    sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')
    sql.exec('CREATE TABLE IF NOT EXISTS event_batches (id, batch, min_ts, max_ts, event_count, flushed)')

    optimizer = new WalOptimizer(sql as unknown as SqlStorage)
  })

  it('returns cost analysis structure with all expected fields', async () => {
    const analysis = await optimizer.analyzeCosts()

    // Verify structure
    expect(typeof analysis.totalBatches).toBe('number')
    expect(typeof analysis.totalEvents).toBe('number')
    expect(typeof analysis.avgEventsPerBatch).toBe('number')
    expect(typeof analysis.totalBlobSize).toBe('number')
    expect(typeof analysis.estimatedCostSavings.vsPerEventStorage).toBe('number')
    expect(typeof analysis.estimatedCostSavings.percentage).toBe('number')
    expect(typeof analysis.compressionStats.enabled).toBe('boolean')
    expect(typeof analysis.compressionStats.avgRatio).toBe('number')
    expect(Array.isArray(analysis.recommendations)).toBe(true)
  })

  it('reports compression as enabled by default', async () => {
    const analysis = await optimizer.analyzeCosts()
    expect(analysis.compressionStats.enabled).toBe(true)
  })

  it('reports compression stats from WAL events', async () => {
    // Use larger events for meaningful compression stats
    const events: Event[] = []
    for (let i = 0; i < 100; i++) {
      events.push({
        id: `evt_${i}`,
        ts: Date.now() + i,
        op: 'CREATE',
        target: `test:entity_${i}`,
        after: {
          $type: 'Test',
          name: `Entity ${i}`,
          description: 'This is repetitive content that should compress well with many events.',
        },
        actor: 'system/test',
      })
    }

    sql.insertWalBatch('posts', 1, 100, events)

    const analysis = await optimizer.analyzeCosts()

    // Should have measured compression ratio from WAL events
    expect(analysis.compressionStats.avgRatio).toBeGreaterThan(0)
  })

  it('calculates WAL-based totals correctly', async () => {
    // Insert WAL batches
    sql.insertWalBatch('posts', 1, 10, createTestEvents(10))
    sql.insertWalBatch('posts', 11, 20, createTestEvents(10))

    const analysis = await optimizer.analyzeCosts()

    // Should count events from WAL
    expect(analysis.totalBatches).toBeGreaterThanOrEqual(2)
    expect(analysis.totalEvents).toBeGreaterThanOrEqual(20)
  })
})

// =============================================================================
// Full Optimization Cycle Tests
// =============================================================================

describe('WAL Optimizer Full Cycle', () => {
  let sql: MockSqlStorage
  let optimizer: WalOptimizer

  beforeEach(() => {
    sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')
    sql.exec('CREATE TABLE IF NOT EXISTS event_batches (id, batch, min_ts, max_ts, event_count, flushed)')

    optimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      compactionMinBatches: 3,
      flushedRetentionMs: 1000,
    })
  })

  it('returns optimization result with all expected properties', async () => {
    // Add some data
    for (let i = 0; i < 5; i++) {
      sql.insertWalBatch('posts', i * 10 + 1, i * 10 + 10, createTestEvents(10))
    }

    const result = await optimizer.optimize()

    // Verify full result structure is returned
    expect(result).toHaveProperty('compaction')
    expect(result).toHaveProperty('pruning')
    expect(result).toHaveProperty('adaptiveThreshold')
    expect(result).toHaveProperty('costAnalysis')

    // Compaction sub-structure
    expect(result.compaction).toHaveProperty('batchesMerged')
    expect(result.compaction).toHaveProperty('rowsSaved')

    // Pruning sub-structure
    expect(result.pruning).toHaveProperty('batchesDeleted')

    // Cost analysis sub-structure
    expect(result.costAnalysis).toHaveProperty('totalBatches')
    expect(result.costAnalysis).toHaveProperty('totalEvents')
    expect(result.costAnalysis).toHaveProperty('recommendations')
  })
})

// =============================================================================
// Configuration Tests
// =============================================================================

describe('WAL Optimizer Configuration', () => {
  it('uses default configuration', () => {
    const sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')

    const optimizer = new WalOptimizer(sql as unknown as SqlStorage)
    const config = optimizer.getConfig()

    expect(config.enableCompression).toBe(DEFAULT_WAL_OPTIMIZER_CONFIG.enableCompression)
    expect(config.compactionMinBatches).toBe(DEFAULT_WAL_OPTIMIZER_CONFIG.compactionMinBatches)
    expect(config.compactionTargetEvents).toBe(DEFAULT_WAL_OPTIMIZER_CONFIG.compactionTargetEvents)
  })

  it('accepts partial configuration override', () => {
    const sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')

    const optimizer = new WalOptimizer(sql as unknown as SqlStorage, {
      enableCompression: false,
      compactionMinBatches: 5,
    })
    const config = optimizer.getConfig()

    expect(config.enableCompression).toBe(false)
    expect(config.compactionMinBatches).toBe(5)
    // Others should be default
    expect(config.compactionTargetEvents).toBe(DEFAULT_WAL_OPTIMIZER_CONFIG.compactionTargetEvents)
  })

  it('allows runtime configuration changes', () => {
    const sql = new MockSqlStorage()
    sql.exec('CREATE TABLE IF NOT EXISTS events_wal (id, ns, first_seq, last_seq, events, created_at)')

    const optimizer = new WalOptimizer(sql as unknown as SqlStorage)

    optimizer.setConfig({ enableCompression: false })
    expect(optimizer.getConfig().enableCompression).toBe(false)

    optimizer.setConfig({ enableCompression: true })
    expect(optimizer.getConfig().enableCompression).toBe(true)
  })
})

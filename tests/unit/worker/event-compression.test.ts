/**
 * Event Batch Compression Tests (RED phase - TDD)
 *
 * These tests verify that event batches stored in events_wal use compression
 * instead of raw JSON, providing 40-60% storage savings.
 *
 * Issue: parquedb-vd7j.1
 * Context: Events serialized as JSON with no compression (event-wal.ts lines 362-363)
 *
 * Expected compression options:
 * - MessagePack for binary serialization (more compact than JSON)
 * - gzip/fflate for additional compression
 * - Snappy for fast compression (already available in deps)
 *
 * These tests will FAIL until compression is implemented.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import type { Event } from '../../../src/types'

// =============================================================================
// Mock SQLite for capturing stored data
// =============================================================================

interface StoredWalRow {
  ns: string
  first_seq: number
  last_seq: number
  events: Uint8Array
  created_at: string
}

class MockSqliteWithCapture {
  private tables: Map<string, unknown[]> = new Map()
  private autoIncrement: Map<string, number> = new Map()

  /** Captured events_wal inserts for inspection */
  public capturedWalInserts: StoredWalRow[] = []

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

    // INSERT into events_wal - capture the data
    if (trimmedQuery.startsWith('insert into') && trimmedQuery.includes('events_wal')) {
      const id = (this.autoIncrement.get('events_wal') || 0) + 1
      this.autoIncrement.set('events_wal', id)

      const row: StoredWalRow = {
        ns: params[0] as string,
        first_seq: params[1] as number,
        last_seq: params[2] as number,
        events: params[3] as Uint8Array,
        created_at: params[4] as string || new Date().toISOString(),
      }

      this.capturedWalInserts.push(row)

      const rows = this.tables.get('events_wal') || []
      rows.push({ id, ...row })
      this.tables.set('events_wal', rows)
      return [] as T[]
    }

    // SELECT MAX(last_seq) for sequence initialization
    if (trimmedQuery.includes('max(last_seq)')) {
      return [] as T[]
    }

    return [] as T[]
  }

  clear(): void {
    this.tables.clear()
    this.autoIncrement.clear()
    this.capturedWalInserts = []
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Generate a realistic event batch for testing compression ratios
 */
function generateTestEvents(count: number): Omit<Event, 'id'>[] {
  const events: Omit<Event, 'id'>[] = []

  for (let i = 0; i < count; i++) {
    events.push({
      ts: Date.now() + i,
      op: i % 3 === 0 ? 'CREATE' : i % 3 === 1 ? 'UPDATE' : 'DELETE',
      target: `users:user_${i.toString().padStart(6, '0')}`,
      before: i % 3 !== 0 ? {
        $id: `user_${i.toString().padStart(6, '0')}`,
        $type: 'User',
        name: `User Number ${i}`,
        email: `user${i}@example.com`,
        createdAt: new Date(Date.now() - 86400000).toISOString(),
        updatedAt: new Date(Date.now() - 3600000).toISOString(),
        status: 'active',
        loginCount: i * 10,
        preferences: {
          theme: 'dark',
          notifications: true,
          language: 'en-US',
        },
      } : undefined,
      after: i % 3 !== 2 ? {
        $id: `user_${i.toString().padStart(6, '0')}`,
        $type: 'User',
        name: `User Number ${i}`,
        email: `user${i}@example.com`,
        createdAt: new Date(Date.now() - 86400000).toISOString(),
        updatedAt: new Date().toISOString(),
        status: 'active',
        loginCount: i * 10 + 1,
        preferences: {
          theme: 'dark',
          notifications: true,
          language: 'en-US',
        },
      } : undefined,
      actor: 'system',
    })
  }

  return events
}

/**
 * Check if data appears to be compressed (not raw JSON)
 */
function isCompressed(data: Uint8Array): boolean {
  // Raw JSON starts with '[' (0x5B) for arrays or '{' (0x7B) for objects
  const firstByte = data[0]

  // Check for common compression magic bytes:
  // - gzip: 0x1F 0x8B
  // - zlib: 0x78 (various second bytes)
  // - MessagePack array: 0x90-0x9F (fixarray) or 0xDC/0xDD (array16/32)
  // - Snappy: various markers
  // - LZ4: 0x04 0x22 0x4D 0x18 (frame format)

  // Gzip magic
  if (data[0] === 0x1F && data[1] === 0x8B) return true

  // Zlib magic (common headers)
  if (data[0] === 0x78 && (data[1] === 0x01 || data[1] === 0x9C || data[1] === 0x5E || data[1] === 0xDA)) return true

  // MessagePack fixarray (small arrays)
  if (firstByte !== undefined && firstByte >= 0x90 && firstByte <= 0x9F) return true

  // MessagePack array16
  if (data[0] === 0xDC) return true

  // MessagePack array32
  if (data[0] === 0xDD) return true

  // LZ4 frame format
  if (data[0] === 0x04 && data[1] === 0x22 && data[2] === 0x4D && data[3] === 0x18) return true

  // Snappy framing format magic
  if (data[0] === 0xFF && data[1] === 0x06 && data[2] === 0x00 && data[3] === 0x00) return true

  // If it starts with JSON array/object markers, it's NOT compressed
  if (firstByte === 0x5B || firstByte === 0x7B) return false

  // If it's valid UTF-8 text starting with '[' or '{', it's not compressed
  try {
    const text = new TextDecoder().decode(data.slice(0, 100))
    if (text.startsWith('[') || text.startsWith('{')) return false
  } catch {
    // Decoding failed, likely binary/compressed
  }

  // Unknown format - could be compressed, but we can't confirm
  return false
}

/**
 * Attempt to decode compressed events
 */
async function tryDecompressEvents(data: Uint8Array): Promise<Event[] | null> {
  // Try to use the exported decompressEvents function
  try {
    const { decompressEvents } = await import('../../../src/worker/do/event-wal')
    const events = decompressEvents(data)
    if (events.length > 0) return events
  } catch {
    // decompressEvents not available or failed
  }

  // Try raw JSON as fallback
  try {
    const text = new TextDecoder().decode(data)
    const parsed = JSON.parse(text)
    if (Array.isArray(parsed)) return parsed as Event[]
  } catch {
    // Not raw JSON
  }

  return null
}

// =============================================================================
// Tests
// =============================================================================

describe('Event Batch Compression', () => {
  let mockSql: MockSqliteWithCapture
  let counters: Map<string, number>

  beforeEach(() => {
    mockSql = new MockSqliteWithCapture()
    counters = new Map()

    // Initialize schema
    mockSql.exec(`
      CREATE TABLE IF NOT EXISTS events_wal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ns TEXT NOT NULL,
        first_seq INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        events BLOB NOT NULL,
        created_at TEXT NOT NULL
      )
    `)
  })

  describe('compression format', () => {
    it('should store event batches in compressed format (not raw JSON)', async () => {
      // Import the actual EventWalManager to test
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Add enough events to trigger a flush
      const testEvents = generateTestEvents(10)
      for (const event of testEvents) {
        await walManager.appendEventWithSeq('users', event)
      }

      // Force flush
      await walManager.flushNsEventBatch('users')

      // Check the stored data
      expect(mockSql.capturedWalInserts.length).toBeGreaterThan(0)

      const storedData = mockSql.capturedWalInserts[0]!.events
      expect(storedData).toBeInstanceOf(Uint8Array)

      // FAILING ASSERTION: Currently stores raw JSON, should be compressed
      expect(isCompressed(storedData)).toBe(true)
    })

    it('should use MessagePack or gzip compression', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Add events and flush
      for (const event of generateTestEvents(5)) {
        await walManager.appendEventWithSeq('posts', event)
      }
      await walManager.flushNsEventBatch('posts')

      const storedData = mockSql.capturedWalInserts[0]!.events
      const firstByte = storedData[0]
      const secondByte = storedData[1]

      // Check for known compression formats
      const isGzip = firstByte === 0x1F && secondByte === 0x8B
      const isZlib = firstByte === 0x78
      const isMsgPackArray = (firstByte !== undefined && firstByte >= 0x90 && firstByte <= 0x9F) ||
        firstByte === 0xDC ||
        firstByte === 0xDD
      const isLZ4 = firstByte === 0x04 && secondByte === 0x22
      const isSnappy = firstByte === 0xFF && secondByte === 0x06

      // FAILING ASSERTION: Should be one of these compressed formats
      expect(isGzip || isZlib || isMsgPackArray || isLZ4 || isSnappy).toBe(true)
    })
  })

  describe('compression efficiency', () => {
    it('should achieve at least 30% size reduction compared to raw JSON', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Generate a larger batch for meaningful compression testing
      const testEvents = generateTestEvents(50)

      for (const event of testEvents) {
        await walManager.appendEventWithSeq('orders', event)
      }
      await walManager.flushNsEventBatch('orders')

      const storedData = mockSql.capturedWalInserts[0]!.events

      // Calculate what raw JSON would be
      // We need to add IDs like the real implementation does
      const eventsWithIds = testEvents.map((e, i) => ({ ...e, id: `evt_${i}` }))
      const rawJsonSize = new TextEncoder().encode(JSON.stringify(eventsWithIds)).length
      const compressedSize = storedData.length

      // Calculate compression ratio
      const compressionRatio = 1 - (compressedSize / rawJsonSize)

      console.log(`Raw JSON size: ${rawJsonSize} bytes`)
      console.log(`Compressed size: ${compressedSize} bytes`)
      console.log(`Compression ratio: ${(compressionRatio * 100).toFixed(1)}%`)

      // FAILING ASSERTION: Currently no compression, ratio is ~0%
      // Target: at least 30% reduction (compression ratio >= 0.30)
      expect(compressionRatio).toBeGreaterThanOrEqual(0.30)
    })

    it('should compress repetitive data efficiently (40%+ for typical event patterns)', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Generate events with highly repetitive structure (typical pattern)
      const repetitiveEvents: Omit<Event, 'id'>[] = []
      for (let i = 0; i < 100; i++) {
        repetitiveEvents.push({
          ts: 1700000000000 + i * 1000,
          op: 'UPDATE',
          target: `items:item_${String(i).padStart(5, '0')}`,
          before: {
            $id: `item_${String(i).padStart(5, '0')}`,
            $type: 'Item',
            name: 'Product Name',
            price: 99.99,
            stock: 100 + i,
            category: 'electronics',
            tags: ['featured', 'sale', 'popular'],
          },
          after: {
            $id: `item_${String(i).padStart(5, '0')}`,
            $type: 'Item',
            name: 'Product Name',
            price: 99.99,
            stock: 100 + i + 1,
            category: 'electronics',
            tags: ['featured', 'sale', 'popular'],
          },
          actor: 'inventory-sync',
        })
      }

      for (const event of repetitiveEvents) {
        await walManager.appendEventWithSeq('items', event)
      }
      await walManager.flushAllNsEventBatches()

      // Sum up all stored batches for this namespace
      const itemsBatches = mockSql.capturedWalInserts.filter(r => r.ns === 'items')
      const totalCompressedSize = itemsBatches.reduce((sum, r) => sum + r.events.length, 0)

      // Calculate raw JSON size
      const eventsWithIds = repetitiveEvents.map((e, i) => ({ ...e, id: `evt_${i}` }))
      const rawJsonSize = new TextEncoder().encode(JSON.stringify(eventsWithIds)).length

      const compressionRatio = 1 - (totalCompressedSize / rawJsonSize)

      console.log(`Repetitive data - Raw: ${rawJsonSize}, Compressed: ${totalCompressedSize}`)
      console.log(`Compression ratio: ${(compressionRatio * 100).toFixed(1)}%`)

      // FAILING ASSERTION: Repetitive data should compress at 40%+ with gzip
      expect(compressionRatio).toBeGreaterThanOrEqual(0.40)
    })
  })

  describe('decompression and deserialization', () => {
    it('should correctly deserialize compressed events back to original data', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Create specific test events
      const originalEvents: Omit<Event, 'id'>[] = [
        {
          ts: 1700000000000,
          op: 'CREATE',
          target: 'users:u1',
          after: { $id: 'u1', $type: 'User', name: 'Alice', email: 'alice@example.com' },
          actor: 'test',
        },
        {
          ts: 1700000001000,
          op: 'UPDATE',
          target: 'users:u1',
          before: { $id: 'u1', $type: 'User', name: 'Alice', email: 'alice@example.com' },
          after: { $id: 'u1', $type: 'User', name: 'Alice Smith', email: 'alice@example.com' },
          actor: 'test',
        },
      ]

      for (const event of originalEvents) {
        await walManager.appendEventWithSeq('users', event)
      }
      await walManager.flushNsEventBatch('users')

      const storedData = mockSql.capturedWalInserts[0]!.events

      // Attempt to decompress (this will need to use the same compression method)
      // For now, we'll test that the decompressed data matches original structure
      const decompressed = await tryDecompressEvents(storedData)

      // FAILING ASSERTION: tryDecompressEvents returns null for compressed data
      // until we implement proper decompression
      expect(decompressed).not.toBeNull()

      if (decompressed) {
        expect(decompressed.length).toBe(2)
        expect(decompressed[0]!.op).toBe('CREATE')
        expect(decompressed[0]!.target).toBe('users:u1')
        expect(decompressed[1]!.op).toBe('UPDATE')
      }
    })

    it('should provide a decompress utility that matches the compression format', async () => {
      // This test verifies that there's a corresponding decompression function
      // that can be used to read events back from the WAL

      // Try to import decompress utility (should exist after implementation)
      try {
        const { decompressEvents } = await import('../../../src/worker/do/event-wal')

        // If it exists, it should be a function
        expect(typeof decompressEvents).toBe('function')

        // Test with sample compressed data
        const testData = new Uint8Array([0x1F, 0x8B]) // gzip magic + dummy
        // This would fail without proper implementation
        expect(decompressEvents).toBeDefined()
      } catch (error) {
        // FAILING: decompressEvents doesn't exist yet
        expect.fail('decompressEvents utility function not found in event-wal.ts')
      }
    })
  })

  describe('edge cases', () => {
    it('should handle empty event batches', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Flush without adding events
      await walManager.flushNsEventBatch('empty')

      // Should not insert anything
      expect(mockSql.capturedWalInserts.length).toBe(0)
    })

    it('should handle single event compression', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      await walManager.appendEventWithSeq('single', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'single:s1',
        after: { $id: 's1', name: 'test' },
        actor: 'test',
      })
      await walManager.flushNsEventBatch('single')

      const storedData = mockSql.capturedWalInserts[0]!.events

      // Even single events should be compressed
      // FAILING: Currently raw JSON
      expect(isCompressed(storedData)).toBe(true)
    })

    it('should handle events with special characters and unicode', async () => {
      const { EventWalManager } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      await walManager.appendEventWithSeq('unicode', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'unicode:u1',
        after: {
          $id: 'u1',
          name: '日本語テスト',
          emoji: '🎉🚀💻',
          special: 'line1\nline2\ttab',
          quotes: '"quoted" and \'single\'',
        },
        actor: 'test',
      })
      await walManager.flushNsEventBatch('unicode')

      const storedData = mockSql.capturedWalInserts[0]!.events

      // Should still be compressed
      expect(isCompressed(storedData)).toBe(true)

      // And decompressible back to original
      const decompressed = await tryDecompressEvents(storedData)
      expect(decompressed).not.toBeNull()
      if (decompressed) {
        expect(decompressed[0]!.after?.name).toBe('日本語テスト')
        expect(decompressed[0]!.after?.emoji).toBe('🎉🚀💻')
      }
    })

    it('should handle large events that exceed typical batch thresholds', async () => {
      const { EventWalManager, EVENT_BATCH_SIZE_THRESHOLD } = await import('../../../src/worker/do/event-wal')

      const walManager = new EventWalManager(mockSql as unknown as SqlStorage, counters)

      // Create an event with large payload
      const largeData = 'x'.repeat(100000) // 100KB of data
      await walManager.appendEventWithSeq('large', {
        ts: Date.now(),
        op: 'CREATE',
        target: 'large:l1',
        after: { $id: 'l1', data: largeData },
        actor: 'test',
      })

      // Should have auto-flushed due to size
      expect(mockSql.capturedWalInserts.length).toBeGreaterThan(0)

      const storedData = mockSql.capturedWalInserts[0]!.events
      const rawSize = new TextEncoder().encode(JSON.stringify([{ id: 'l1', ts: Date.now(), op: 'CREATE', target: 'large:l1', after: { $id: 'l1', data: largeData }, actor: 'test' }])).length

      // Large repetitive data should compress very well
      expect(storedData.length).toBeLessThan(rawSize * 0.1) // 90%+ compression for repetitive data
    })
  })
})

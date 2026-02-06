/**
 * DO Workflow Integration Tests
 *
 * Tests the combined DOCompactor + DOReadPath lifecycle:
 *   Write via WAL -> Read via DOReadPath -> Compact via DOCompactor -> Read from R2 Parquet
 *
 * Uses mocked R2Bucket and SQLite (via in-memory SqliteWal) to test the full
 * production DO lifecycle without requiring vitest-pool-workers or real bindings.
 *
 * Run with: pnpm test tests/engine/do-workflow
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DOCompactor } from '@/engine/do-compactor'
import { DOReadPath } from '@/engine/do-read-path'
import { SqliteWal } from '@/engine/sqlite-wal'
import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Mock SqlStorage (in-memory SQLite replacement)
// =============================================================================

/**
 * In-memory implementation of the SqlStorage interface used by SqliteWal.
 * Simulates the Cloudflare DO ctx.storage.sql interface with a simple
 * table-like structure backed by arrays.
 */
class MockSqlStorage {
  private rows: Array<{
    id: number
    ts: number
    kind: string
    batch: string
    row_count: number
    flushed: number
  }> = []
  private nextId = 1
  private tableCreated = false

  /** Test helper: total number of rows including flushed (for verifying cleanup). */
  get totalRowCount(): number {
    return this.rows.length
  }

  exec(query: string, ...bindings: unknown[]): { toArray(): unknown[] } {
    const q = query.trim()

    // CREATE TABLE
    if (q.startsWith('CREATE TABLE')) {
      this.tableCreated = true
      return { toArray: () => [] }
    }

    // INSERT
    if (q.startsWith('INSERT INTO wal')) {
      const [ts, kind, batch, row_count] = bindings as [number, string, string, number]
      this.rows.push({
        id: this.nextId++,
        ts,
        kind,
        batch,
        row_count,
        flushed: 0,
      })
      return { toArray: () => [] }
    }

    // SELECT with kind filter (getBatches)
    if (q.includes('SELECT id, ts, batch, row_count FROM wal WHERE kind = ?')) {
      const kind = bindings[0] as string
      const result = this.rows
        .filter((r) => r.kind === kind && r.flushed === 0)
        .sort((a, b) => a.id - b.id)
        .map((r) => ({ id: r.id, ts: r.ts, batch: r.batch, row_count: r.row_count }))
      return { toArray: () => result }
    }

    // SELECT all unflushed batches (getAllBatches)
    if (q.includes('SELECT id, ts, kind, batch, row_count FROM wal WHERE flushed = 0')) {
      const result = this.rows
        .filter((r) => r.flushed === 0)
        .sort((a, b) => a.id - b.id)
        .map((r) => ({ id: r.id, ts: r.ts, kind: r.kind, batch: r.batch, row_count: r.row_count }))
      return { toArray: () => result }
    }

    // SELECT SUM (getUnflushedCount)
    if (q.includes('SUM(row_count)') && q.includes('kind = ?')) {
      const kind = bindings[0] as string
      const count = this.rows
        .filter((r) => r.kind === kind && r.flushed === 0)
        .reduce((sum, r) => sum + r.row_count, 0)
      return { toArray: () => [{ count }] }
    }

    if (q.includes('SUM(row_count)') && !q.includes('kind = ?')) {
      const count = this.rows
        .filter((r) => r.flushed === 0)
        .reduce((sum, r) => sum + r.row_count, 0)
      return { toArray: () => [{ count }] }
    }

    // SELECT DISTINCT kind (getUnflushedTables)
    if (q.includes('SELECT DISTINCT kind')) {
      const kinds = [...new Set(this.rows.filter((r) => r.flushed === 0).map((r) => r.kind))].sort()
      return { toArray: () => kinds.map((kind) => ({ kind })) }
    }

    // UPDATE (markFlushed)
    if (q.startsWith('UPDATE wal SET flushed = 1')) {
      const ids = bindings as number[]
      for (const row of this.rows) {
        if (ids.includes(row.id)) {
          row.flushed = 1
        }
      }
      return { toArray: () => [] }
    }

    // DELETE (cleanup)
    if (q.startsWith('DELETE FROM wal WHERE flushed = 1')) {
      this.rows = this.rows.filter((r) => r.flushed !== 1)
      return { toArray: () => [] }
    }

    return { toArray: () => [] }
  }
}

// =============================================================================
// Mock R2Bucket
// =============================================================================

/**
 * In-memory R2Bucket mock that stores objects as ArrayBuffers in a Map.
 * Implements the get/put/list/delete subset needed by DOCompactor and DOReadPath.
 */
class MockR2Bucket {
  private objects = new Map<string, ArrayBuffer>()

  async get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null> {
    const data = this.objects.get(key)
    if (!data) return null
    return {
      arrayBuffer: async () => data,
    }
  }

  async put(key: string, value: ArrayBuffer | ReadableStream | string | Blob): Promise<void> {
    if (value instanceof ArrayBuffer) {
      this.objects.set(key, value)
    } else if (typeof value === 'string') {
      const encoder = new TextEncoder()
      this.objects.set(key, encoder.encode(value).buffer as ArrayBuffer)
    } else {
      throw new Error('MockR2Bucket: unsupported value type')
    }
  }

  async list(): Promise<{ objects: Array<{ key: string }> }> {
    return {
      objects: [...this.objects.keys()].map((key) => ({ key })),
    }
  }

  async delete(key: string): Promise<void> {
    this.objects.delete(key)
  }

  /** Test helper: check if a key exists */
  has(key: string): boolean {
    return this.objects.has(key)
  }

  /** Test helper: get raw ArrayBuffer */
  getRaw(key: string): ArrayBuffer | undefined {
    return this.objects.get(key)
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/** Create a fresh WAL + bucket + DOCompactor + DOReadPath for each test. */
function createTestContext() {
  const sql = new MockSqlStorage()
  const bucket = new MockR2Bucket() as unknown as R2Bucket
  const wal = new SqliteWal(sql as never)
  const compactor = new DOCompactor(wal, bucket)
  const reader = new DOReadPath(wal, bucket)
  return { sql, bucket: bucket as unknown as MockR2Bucket & R2Bucket, wal, compactor, reader }
}

// =============================================================================
// Tests
// =============================================================================

describe('DO Workflow Integration (DOCompactor + DOReadPath)', () => {
  // =========================================================================
  // Test 1: Write -> Read sees WAL data
  // =========================================================================
  describe('Write -> Read sees WAL data', () => {
    it('DOReadPath.find() returns WAL data when R2 is empty', async () => {
      const { wal, reader } = createTestContext()

      // Write entities to WAL
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

      // Read via DOReadPath - should return WAL data (no R2 Parquet yet)
      const results = await reader.find('users')

      expect(results).toHaveLength(2)
      // Results are sorted by $id
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice')
      expect(results[1].$id).toBe('u2')
      expect(results[1].name).toBe('Bob')
    })

    it('DOReadPath.getById() returns WAL entity when R2 is empty', async () => {
      const { wal, reader } = createTestContext()

      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })

      const alice = await reader.getById('users', 'u1')
      expect(alice).not.toBeNull()
      expect(alice!.$id).toBe('u1')
      expect(alice!.name).toBe('Alice')
    })

    it('DOReadPath.find() returns empty when no WAL and no R2', async () => {
      const { reader } = createTestContext()

      const results = await reader.find('users')
      expect(results).toHaveLength(0)
    })
  })

  // =========================================================================
  // Test 2: Write -> Compact -> Read sees Parquet data
  // =========================================================================
  describe('Write -> Compact -> Read sees Parquet data', () => {
    it('compactTable writes Parquet to R2, then DOReadPath reads it back', async () => {
      const { wal, compactor, reader, bucket } = createTestContext()

      // 1. Write entities to WAL
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

      // 2. Compact: WAL -> R2 Parquet
      const compactResult = await compactor.compactTable('users')
      expect(compactResult).not.toBeNull()
      expect(compactResult!.count).toBe(2)
      expect(compactResult!.flushed).toBe(2)

      // 3. Verify R2 has the Parquet file with PAR1 magic bytes
      const r2Key = 'data/users.parquet'
      expect(bucket.has(r2Key)).toBe(true)
      const rawBuffer = bucket.getRaw(r2Key)!
      const magic = new Uint8Array(rawBuffer.slice(0, 4))
      // PAR1 magic: 0x50, 0x41, 0x52, 0x31
      expect(magic[0]).toBe(0x50) // P
      expect(magic[1]).toBe(0x41) // A
      expect(magic[2]).toBe(0x52) // R
      expect(magic[3]).toBe(0x31) // 1

      // 4. Read via DOReadPath - WAL is now flushed, so data comes from R2
      const results = await reader.find('users')
      expect(results).toHaveLength(2)
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice')
      expect(results[1].$id).toBe('u2')
      expect(results[1].name).toBe('Bob')
    })

    it('WAL batches are marked flushed after compaction', async () => {
      const { wal, compactor } = createTestContext()

      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })

      // Before compaction: 1 unflushed
      expect(wal.getUnflushedCount()).toBe(1)

      await compactor.compactTable('users')

      // After compaction: 0 unflushed
      expect(wal.getUnflushedCount()).toBe(0)
    })
  })

  // =========================================================================
  // Test 3: Write -> Compact -> More writes -> Read sees merged data
  // =========================================================================
  describe('Write -> Compact -> More writes -> Read sees merged data', () => {
    it('DOReadPath merges R2 Parquet with new WAL entries', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Initial write + compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      // 2. More writes after compaction (these go to WAL only)
      wal.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 2000, name: 'Charlie' })

      // 3. Read should merge R2 (u1, u2) + WAL (u3)
      const results = await reader.find('users')
      expect(results).toHaveLength(3)
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice')
      expect(results[1].$id).toBe('u2')
      expect(results[1].name).toBe('Bob')
      expect(results[2].$id).toBe('u3')
      expect(results[2].name).toBe('Charlie')
    })

    it('getById returns entity from WAL when it is newer than R2', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Compact initial data
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. New WAL write for a different entity
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 2000, name: 'Bob' })

      // 3. getById for the R2 entity should still work
      const alice = await reader.getById('users', 'u1')
      expect(alice).not.toBeNull()
      expect(alice!.name).toBe('Alice')

      // 4. getById for the WAL entity should also work
      const bob = await reader.getById('users', 'u2')
      expect(bob).not.toBeNull()
      expect(bob!.name).toBe('Bob')
    })

    it('second compaction merges both old R2 data and new WAL entries', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. First compaction
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. More writes
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 2000, name: 'Bob' })

      // 3. Second compaction merges R2 (u1) + WAL (u2) -> R2 (u1, u2)
      const result = await compactor.compactTable('users')
      expect(result).not.toBeNull()
      expect(result!.count).toBe(2)

      // 4. Read from compacted data -- both should be present
      const results = await reader.find('users')
      expect(results).toHaveLength(2)
      expect(results[0].$id).toBe('u1')
      expect(results[1].$id).toBe('u2')
    })
  })

  // =========================================================================
  // Test 4: Update after compaction
  // =========================================================================
  describe('Update after compaction', () => {
    it('WAL update with higher $v overrides compacted R2 data', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', email: 'alice@example.com' })
      await compactor.compactTable('users')

      // 2. Update in WAL with higher $v
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Smith', email: 'alice@example.com' })

      // 3. DOReadPath should return the updated version
      const results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice Smith')
      expect(results[0].$v).toBe(2)
      expect(results[0].$op).toBe('u')
    })

    it('getById returns updated entity from WAL over compacted R2', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. Update in WAL
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Updated' })

      // 3. getById should return updated version
      const alice = await reader.getById('users', 'u1')
      expect(alice).not.toBeNull()
      expect(alice!.name).toBe('Alice Updated')
      expect(alice!.$v).toBe(2)
    })

    it('compaction after update produces correct merged state', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      // 2. Update u1 in WAL
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Smith' })

      // 3. Compact again - should merge R2 (u1 v1, u2 v1) + WAL (u1 v2)
      const result = await compactor.compactTable('users')
      expect(result!.count).toBe(2) // u1 (updated) + u2

      // 4. Read from fresh compacted data
      const results = await reader.find('users')
      expect(results).toHaveLength(2)

      const alice = results.find((r) => r.$id === 'u1')!
      expect(alice.name).toBe('Alice Smith')
      expect(alice.$v).toBe(2)

      const bob = results.find((r) => r.$id === 'u2')!
      expect(bob.name).toBe('Bob')
      expect(bob.$v).toBe(1)
    })
  })

  // =========================================================================
  // Test 5: Delete after compaction
  // =========================================================================
  describe('Delete after compaction', () => {
    it('WAL tombstone ($op=d) removes compacted entity from read results', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      // 2. Delete u1 in WAL
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

      // 3. DOReadPath.find() should not return deleted entity
      const results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('u2')
      expect(results[0].name).toBe('Bob')
    })

    it('getById returns null for entity deleted in WAL after compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. Delete in WAL
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

      // 3. getById should return null
      const result = await reader.getById('users', 'u1')
      expect(result).toBeNull()
    })

    it('compaction after delete removes entity from R2 Parquet', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      // 2. Delete u1 in WAL
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

      // 3. Compact again - should remove u1 from R2
      const result = await compactor.compactTable('users')
      expect(result!.count).toBe(1) // Only Bob remains

      // 4. Read from fresh compacted data
      const results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('u2')
    })

    it('delete all entities leaves empty result', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. Delete in WAL
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

      // 3. Read should return empty
      const results = await reader.find('users')
      expect(results).toHaveLength(0)
    })
  })

  // =========================================================================
  // Relationship workflow
  // =========================================================================
  describe('Relationship workflow', () => {
    it('Write rels -> Read from WAL -> Compact -> Read from R2', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Write rels to WAL
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('rels', { $op: 'l', $ts: 1001, f: 'u1', p: 'posts', r: 'author', t: 'p2' })

      // 2. Read from WAL
      const walRels = await reader.findRels()
      expect(walRels).toHaveLength(2)

      // 3. Compact
      const result = await compactor.compactRels()
      expect(result).not.toBeNull()
      expect(result!.count).toBe(2)

      // 4. Read from R2 (WAL is flushed)
      const r2Rels = await reader.findRels()
      expect(r2Rels).toHaveLength(2)
    })

    it('unlink in WAL removes compacted rel from read results', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create rels and compact
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('rels', { $op: 'l', $ts: 1001, f: 'u1', p: 'posts', r: 'author', t: 'p2' })
      await compactor.compactRels()

      // 2. Unlink p1 in WAL
      wal.append('rels', { $op: 'u', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })

      // 3. Read should show only p2
      const rels = await reader.findRels()
      expect(rels).toHaveLength(1)
      expect(rels[0].t).toBe('p2')
    })

    it('findRels filters by fromId across R2 + WAL', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Compact rels for u1
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      // 2. WAL rels for u2
      wal.append('rels', { $op: 'l', $ts: 2000, f: 'u2', p: 'posts', r: 'author', t: 'p2' })

      // 3. Filter by u1 should return only R2 rel
      const u1Rels = await reader.findRels('u1')
      expect(u1Rels).toHaveLength(1)
      expect(u1Rels[0].t).toBe('p1')

      // 4. Filter by u2 should return only WAL rel
      const u2Rels = await reader.findRels('u2')
      expect(u2Rels).toHaveLength(1)
      expect(u2Rels[0].t).toBe('p2')
    })
  })

  // =========================================================================
  // compactAll workflow
  // =========================================================================
  describe('compactAll workflow', () => {
    it('compacts data, rels, and events in one call', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Write data, rels, and events
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })
      wal.append('rels', { $op: 'l', $ts: 1002, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('events', { id: 'e1', ts: 1003, op: 'c', ns: 'users', eid: 'u1' })

      // Compact all
      const result = await compactor.compactAll()
      expect(result.tables.get('users')).toBe(1)
      expect(result.tables.get('posts')).toBe(1)
      expect(result.rels).toBe(1)
      expect(result.events).toBe(1)

      // All WAL data should be flushed
      expect(wal.getUnflushedCount()).toBe(0)

      // Read all data back
      const users = await reader.find('users')
      expect(users).toHaveLength(1)
      expect(users[0].name).toBe('Alice')

      const posts = await reader.find('posts')
      expect(posts).toHaveLength(1)
      expect(posts[0].title).toBe('Hello')

      const rels = await reader.findRels()
      expect(rels).toHaveLength(1)

      const events = await reader.findEvents()
      expect(events).toHaveLength(1)
    })
  })

  // =========================================================================
  // Multi-cycle workflow
  // =========================================================================
  describe('Multi-cycle workflow', () => {
    it('full lifecycle: create -> compact -> update -> compact -> delete -> compact -> read', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Cycle 1: Create
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      let results = await reader.find('users')
      expect(results).toHaveLength(2)

      // Cycle 2: Update u1
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Smith' })
      await compactor.compactTable('users')

      results = await reader.find('users')
      expect(results).toHaveLength(2)
      expect(results.find((r) => r.$id === 'u1')!.name).toBe('Alice Smith')

      // Cycle 3: Delete u2
      wal.append('users', { $id: 'u2', $op: 'd', $v: 2, $ts: 3000 })
      await compactor.compactTable('users')

      results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice Smith')

      // Cycle 4: Create new entity
      wal.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 4000, name: 'Charlie' })
      await compactor.compactTable('users')

      results = await reader.find('users')
      expect(results).toHaveLength(2)
      expect(results[0].$id).toBe('u1')
      expect(results[1].$id).toBe('u3')
    })

    it('interleaved data and relationship writes across multiple compactions', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Step 1: Create users and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      await compactor.compactTable('users')

      // Step 2: Create posts (not yet compacted)
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 2000, title: 'Hello World' })

      // Step 3: Link u1 -> p1 and compact rels
      wal.append('rels', { $op: 'l', $ts: 2001, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      // Verify: users from R2, posts from WAL, rels from R2
      const users = await reader.find('users')
      expect(users).toHaveLength(2)

      const posts = await reader.find('posts')
      expect(posts).toHaveLength(1)
      expect(posts[0].title).toBe('Hello World')

      const rels = await reader.findRels('u1')
      expect(rels).toHaveLength(1)
      expect(rels[0].t).toBe('p1')

      // Step 4: Compact posts now
      await compactor.compactTable('posts')

      // Everything should still be readable from R2
      const postsAfter = await reader.find('posts')
      expect(postsAfter).toHaveLength(1)
      expect(postsAfter[0].title).toBe('Hello World')
    })
  })

  // =========================================================================
  // Filter with merged data
  // =========================================================================
  describe('Filter with merged data', () => {
    it('filter works across R2 + WAL data', async () => {
      const { wal, compactor, reader } = createTestContext()

      // R2 data (after compaction)
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', role: 'admin' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob', role: 'user' })
      await compactor.compactTable('users')

      // WAL data (not yet compacted)
      wal.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 2000, name: 'Charlie', role: 'admin' })

      // Filter for admins - should get Alice (R2) + Charlie (WAL)
      const admins = await reader.find('users', { role: 'admin' })
      expect(admins).toHaveLength(2)
      expect(admins.map((a) => a.name).sort()).toEqual(['Alice', 'Charlie'])

      // Filter for users - should get Bob (R2) only
      const users = await reader.find('users', { role: 'user' })
      expect(users).toHaveLength(1)
      expect(users[0].name).toBe('Bob')
    })
  })

  // =========================================================================
  // WAL cleanup after compaction
  // =========================================================================
  describe('WAL cleanup after compaction', () => {
    it('compactTable removes flushed WAL entries from SQLite', async () => {
      const { sql, wal, compactor } = createTestContext()

      // Write data to WAL
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })

      // Before compaction: 2 rows in SQLite
      expect(sql.totalRowCount).toBe(2)

      // Compact: should mark flushed AND cleanup
      await compactor.compactTable('users')

      // After compaction: flushed rows should be deleted, not just marked
      expect(sql.totalRowCount).toBe(0)
      expect(wal.getUnflushedCount()).toBe(0)

      // replayUnflushed should return empty
      expect(wal.replayUnflushed('users')).toHaveLength(0)
    })

    it('compactRels removes flushed WAL entries from SQLite', async () => {
      const { sql, wal, compactor } = createTestContext()

      // Write rels to WAL
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('rels', { $op: 'l', $ts: 1001, f: 'u1', p: 'posts', r: 'author', t: 'p2' })

      // Before compaction: 2 rows in SQLite
      expect(sql.totalRowCount).toBe(2)

      // Compact rels
      await compactor.compactRels()

      // After compaction: flushed rows should be deleted
      expect(sql.totalRowCount).toBe(0)
      expect(wal.replayUnflushed('rels')).toHaveLength(0)
    })

    it('compactEvents removes flushed WAL entries from SQLite', async () => {
      const { sql, wal, compactor } = createTestContext()

      // Write events to WAL
      wal.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e2', ts: 1001, op: 'c', ns: 'users', eid: 'u2' })

      // Before compaction: 2 rows in SQLite
      expect(sql.totalRowCount).toBe(2)

      // Compact events
      await compactor.compactEvents()

      // After compaction: flushed rows should be deleted
      expect(sql.totalRowCount).toBe(0)
      expect(wal.replayUnflushed('events')).toHaveLength(0)
    })

    it('compactAll removes all flushed WAL entries from SQLite', async () => {
      const { sql, wal, compactor } = createTestContext()

      // Write data, rels, and events to WAL
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })
      wal.append('rels', { $op: 'l', $ts: 1002, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('events', { id: 'e1', ts: 1003, op: 'c', ns: 'users', eid: 'u1' })

      // Before compaction: 4 rows in SQLite
      expect(sql.totalRowCount).toBe(4)

      // Compact all
      await compactor.compactAll()

      // After compaction: all flushed rows should be deleted
      expect(sql.totalRowCount).toBe(0)
      expect(wal.getUnflushedCount()).toBe(0)
    })

    it('cleanup only removes flushed rows, preserving unflushed ones', async () => {
      const { sql, wal, compactor } = createTestContext()

      // Write data for two tables
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })

      // Before compaction: 2 rows total
      expect(sql.totalRowCount).toBe(2)

      // Compact only users (not posts)
      await compactor.compactTable('users')

      // After compaction: users row deleted, posts row still present
      expect(sql.totalRowCount).toBe(1)
      expect(wal.getUnflushedCount()).toBe(1) // posts still unflushed
      expect(wal.replayUnflushed('users')).toHaveLength(0)
      expect(wal.replayUnflushed('posts')).toHaveLength(1)
    })
  })
})

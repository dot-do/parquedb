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

  // =========================================================================
  // Events workflow (full lifecycle)
  // =========================================================================
  describe('Events workflow', () => {
    it('Write events -> Read from WAL -> Compact -> Read from R2', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Write events to WAL
      wal.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e2', ts: 1001, op: 'c', ns: 'posts', eid: 'p1' })

      // 2. Read from WAL - both events should be present
      const walEvents = await reader.findEvents()
      expect(walEvents).toHaveLength(2)
      expect(walEvents[0].id).toBe('e1')
      expect(walEvents[1].id).toBe('e2')

      // 3. Compact events
      const result = await compactor.compactEvents()
      expect(result).not.toBeNull()
      expect(result!.count).toBe(2)

      // 4. Read from R2 (WAL is flushed)
      const r2Events = await reader.findEvents()
      expect(r2Events).toHaveLength(2)
      expect(r2Events[0].id).toBe('e1')
      expect(r2Events[1].id).toBe('e2')
    })

    it('Events append-only: compacted + new WAL events are concatenated', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Write initial events and compact
      wal.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e2', ts: 1001, op: 'c', ns: 'users', eid: 'u2' })
      await compactor.compactEvents()

      // 2. Write more events to WAL (not compacted)
      wal.append('events', { id: 'e3', ts: 2000, op: 'u', ns: 'users', eid: 'u1' })

      // 3. Read should merge R2 (e1, e2) + WAL (e3) = 3 events
      const events = await reader.findEvents()
      expect(events).toHaveLength(3)

      // 4. Verify sorted by ts
      const timestamps = events.map((e) => e.ts as number)
      expect(timestamps[0]).toBeLessThanOrEqual(timestamps[1])
      expect(timestamps[1]).toBeLessThanOrEqual(timestamps[2])
    })

    it('second compaction appends new events to existing R2 events', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. First compaction
      wal.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' })
      await compactor.compactEvents()

      // 2. More events + second compaction
      wal.append('events', { id: 'e2', ts: 2000, op: 'u', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e3', ts: 3000, op: 'd', ns: 'users', eid: 'u1' })
      const result = await compactor.compactEvents()
      expect(result!.count).toBe(3) // All three events appended

      // 3. Read back - all 3 events
      const events = await reader.findEvents()
      expect(events).toHaveLength(3)
      expect(events[0].id).toBe('e1')
      expect(events[1].id).toBe('e2')
      expect(events[2].id).toBe('e3')
    })
  })

  // =========================================================================
  // Batch writes through full lifecycle
  // =========================================================================
  describe('Batch writes through lifecycle', () => {
    it('appendBatch -> compact -> read back all entities', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Write a batch of entities
      wal.appendBatch('users', [
        { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
        { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' },
        { $id: 'u3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie' },
      ])

      // Read from WAL
      const walResults = await reader.find('users')
      expect(walResults).toHaveLength(3)

      // Compact
      const result = await compactor.compactTable('users')
      expect(result!.count).toBe(3)
      expect(result!.flushed).toBe(1) // Single batch = 1 WAL entry

      // Read from R2
      const r2Results = await reader.find('users')
      expect(r2Results).toHaveLength(3)
      expect(r2Results[0].$id).toBe('u1')
      expect(r2Results[1].$id).toBe('u2')
      expect(r2Results[2].$id).toBe('u3')
    })

    it('appendBatch for rels -> compact -> read back', async () => {
      const { wal, compactor, reader } = createTestContext()

      wal.appendBatch('rels', [
        { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
        { $op: 'l', $ts: 1001, f: 'u1', p: 'posts', r: 'author', t: 'p2' },
        { $op: 'l', $ts: 1002, f: 'u2', p: 'posts', r: 'author', t: 'p3' },
      ])

      // Read from WAL
      const walRels = await reader.findRels()
      expect(walRels).toHaveLength(3)

      // Compact
      await compactor.compactRels()

      // Read from R2
      const r2Rels = await reader.findRels()
      expect(r2Rels).toHaveLength(3)

      // Filter by fromId
      const u1Rels = await reader.findRels('u1')
      expect(u1Rels).toHaveLength(2)
      const u2Rels = await reader.findRels('u2')
      expect(u2Rels).toHaveLength(1)
    })
  })

  // =========================================================================
  // shouldCompact threshold interaction
  // =========================================================================
  describe('shouldCompact threshold interaction', () => {
    it('shouldCompact returns false before writes and true after sufficient writes', () => {
      const { wal, compactor } = createTestContext()

      // No data: should not compact
      expect(compactor.shouldCompact(1)).toBe(false)
      expect(compactor.shouldCompact()).toBe(false) // default threshold = 100

      // Add some data
      for (let i = 0; i < 5; i++) {
        wal.append('users', { $id: `u${i}`, $op: 'c', $v: 1, $ts: 1000 + i, name: `User${i}` })
      }

      // Below default threshold (100) but above 5
      expect(compactor.shouldCompact(5)).toBe(true)
      expect(compactor.shouldCompact(6)).toBe(false)
      expect(compactor.shouldCompact()).toBe(false)
    })

    it('shouldCompact returns false after compaction clears WAL', async () => {
      const { wal, compactor } = createTestContext()

      // Write 10 entities
      for (let i = 0; i < 10; i++) {
        wal.append('users', { $id: `u${i}`, $op: 'c', $v: 1, $ts: 1000 + i, name: `User${i}` })
      }

      expect(compactor.shouldCompact(5)).toBe(true)

      // Compact
      await compactor.compactTable('users')

      // After compaction, WAL is empty
      expect(compactor.shouldCompact(1)).toBe(false)
    })

    it('shouldCompact accounts for data across multiple tables', () => {
      const { wal, compactor } = createTestContext()

      // 3 users + 3 posts + 2 rels + 2 events = 10 total
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
      wal.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1003, title: 'Hello' })
      wal.append('posts', { $id: 'p2', $op: 'c', $v: 1, $ts: 1004, title: 'World' })
      wal.append('posts', { $id: 'p3', $op: 'c', $v: 1, $ts: 1005, title: 'Foo' })
      wal.append('rels', { $op: 'l', $ts: 1006, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('rels', { $op: 'l', $ts: 1007, f: 'u2', p: 'posts', r: 'author', t: 'p2' })
      wal.append('events', { id: 'e1', ts: 1008, op: 'c', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e2', ts: 1009, op: 'c', ns: 'posts', eid: 'p1' })

      expect(compactor.shouldCompact(10)).toBe(true)
      expect(compactor.shouldCompact(11)).toBe(false)
    })
  })

  // =========================================================================
  // Re-creation after delete
  // =========================================================================
  describe('Re-creation after delete', () => {
    it('entity can be re-created with same $id after delete + compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Create and compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      // 2. Delete and compact
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })
      await compactor.compactTable('users')

      // Verify deleted
      const deleted = await reader.find('users')
      expect(deleted).toHaveLength(0)

      // 3. Re-create same ID with higher $v
      wal.append('users', { $id: 'u1', $op: 'c', $v: 3, $ts: 3000, name: 'Alice Reborn' })

      // 4. Read via DOReadPath should see the re-created entity
      const results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('u1')
      expect(results[0].name).toBe('Alice Reborn')
      expect(results[0].$v).toBe(3)
    })

    it('re-created entity survives another compaction cycle', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Create -> Compact -> Delete -> Compact -> Re-create -> Compact
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      await compactor.compactTable('users')

      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })
      await compactor.compactTable('users')

      wal.append('users', { $id: 'u1', $op: 'c', $v: 3, $ts: 3000, name: 'Alice Reborn' })
      await compactor.compactTable('users')

      // Should survive the final compaction
      const results = await reader.find('users')
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Alice Reborn')
      expect(results[0].$v).toBe(3)
    })
  })

  // =========================================================================
  // Multiple WAL mutations before compaction
  // =========================================================================
  describe('Multiple WAL mutations before compaction', () => {
    it('multiple updates to same entity in WAL: highest $v wins after compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Multiple mutations to same entity without compacting in between
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice v2' })
      wal.append('users', { $id: 'u1', $op: 'u', $v: 3, $ts: 3000, name: 'Alice v3' })

      // Before compaction: read should see the v3 version
      const preCompact = await reader.find('users')
      expect(preCompact).toHaveLength(1)
      expect(preCompact[0].name).toBe('Alice v3')
      expect(preCompact[0].$v).toBe(3)

      // Compact all three at once
      await compactor.compactTable('users')

      // After compaction: still v3
      const postCompact = await reader.find('users')
      expect(postCompact).toHaveLength(1)
      expect(postCompact[0].name).toBe('Alice v3')
      expect(postCompact[0].$v).toBe(3)
    })

    it('create + delete in same WAL batch: entity is absent from results', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Create and immediately delete without compacting
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

      // Read should return empty (delete wins)
      const results = await reader.find('users')
      expect(results).toHaveLength(0)

      // Compact: should produce empty Parquet (deleted entity removed)
      const result = await compactor.compactTable('users')
      expect(result!.count).toBe(0) // No live entities

      // Post-compact read still empty
      const postCompact = await reader.find('users')
      expect(postCompact).toHaveLength(0)
    })

    it('link + unlink same rel in WAL: rel is absent from results', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Link and unlink same relationship in WAL without compacting
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('rels', { $op: 'u', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })

      // Read should show no rels (unlink wins)
      const results = await reader.findRels()
      expect(results).toHaveLength(0)

      // After compaction, still no rels
      await compactor.compactRels()
      const postCompact = await reader.findRels()
      expect(postCompact).toHaveLength(0)
    })
  })

  // =========================================================================
  // Multiple tables with interleaved compaction timing
  // =========================================================================
  describe('Multiple tables with interleaved compaction', () => {
    it('compacting one table does not affect unflushed data in another', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Write to both tables
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })

      // Compact only users
      await compactor.compactTable('users')

      // Users should be in R2, posts still in WAL
      const users = await reader.find('users')
      expect(users).toHaveLength(1)
      expect(users[0].name).toBe('Alice')

      const posts = await reader.find('posts')
      expect(posts).toHaveLength(1)
      expect(posts[0].title).toBe('Hello')

      // WAL should only have posts data
      expect(wal.getUnflushedCount()).toBe(1)
      expect(wal.getUnflushedTables()).toEqual(['posts'])
    })

    it('staggered compaction: users first, then posts, then rels', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Write all types
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1001, title: 'Hello' })
      wal.append('rels', { $op: 'l', $ts: 1002, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('events', { id: 'e1', ts: 1003, op: 'c', ns: 'users', eid: 'u1' })

      // Compact users first
      await compactor.compactTable('users')
      expect(wal.getUnflushedCount()).toBe(3) // posts + rels + events

      // Write more users (these go to WAL)
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 2000, name: 'Bob' })

      // Compact posts
      await compactor.compactTable('posts')
      expect(wal.getUnflushedCount()).toBe(3) // rels + events + u2

      // Compact rels
      await compactor.compactRels()
      expect(wal.getUnflushedCount()).toBe(2) // events + u2

      // Compact events
      await compactor.compactEvents()
      expect(wal.getUnflushedCount()).toBe(1) // only u2

      // Read back all data - verify each came from its appropriate source
      const users = await reader.find('users')
      expect(users).toHaveLength(2) // u1 from R2 + u2 from WAL

      const posts = await reader.find('posts')
      expect(posts).toHaveLength(1)

      const rels = await reader.findRels()
      expect(rels).toHaveLength(1)

      const events = await reader.findEvents()
      expect(events).toHaveLength(1)

      // Final compaction for u2
      await compactor.compactTable('users')
      expect(wal.getUnflushedCount()).toBe(0)

      // Everything now from R2
      const finalUsers = await reader.find('users')
      expect(finalUsers).toHaveLength(2)
    })
  })

  // =========================================================================
  // Relationship re-link after unlink
  // =========================================================================
  describe('Relationship re-link after unlink', () => {
    it('rel can be re-linked after unlink + compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // 1. Link and compact
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      // 2. Unlink and compact
      wal.append('rels', { $op: 'u', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      // Verify unlinked
      const unlinked = await reader.findRels()
      expect(unlinked).toHaveLength(0)

      // 3. Re-link with newer timestamp
      wal.append('rels', { $op: 'l', $ts: 3000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })

      // 4. Read should see the re-linked relationship
      const relinked = await reader.findRels()
      expect(relinked).toHaveLength(1)
      expect(relinked[0].f).toBe('u1')
      expect(relinked[0].t).toBe('p1')
      expect(relinked[0].$op).toBe('l')
    })

    it('re-linked relationship survives compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // Link -> Compact -> Unlink -> Compact -> Re-link -> Compact
      wal.append('rels', { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      wal.append('rels', { $op: 'u', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      wal.append('rels', { $op: 'l', $ts: 3000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      await compactor.compactRels()

      const results = await reader.findRels()
      expect(results).toHaveLength(1)
      expect(results[0].$op).toBe('l')
      expect(results[0].$ts).toBe(3000)
    })
  })

  // =========================================================================
  // End-to-end: Full DO lifecycle simulation
  // =========================================================================
  describe('Full DO lifecycle simulation', () => {
    it('simulates realistic DO workflow: writes, reads, periodic compaction', async () => {
      const { wal, compactor, reader } = createTestContext()

      // --- Phase 1: Initial writes ---
      wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', role: 'admin' })
      wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob', role: 'user' })
      wal.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 1002, title: 'First Post', authorId: 'u1' })
      wal.append('rels', { $op: 'l', $ts: 1003, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
      wal.append('events', { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e2', ts: 1001, op: 'c', ns: 'users', eid: 'u2' })
      wal.append('events', { id: 'e3', ts: 1002, op: 'c', ns: 'posts', eid: 'p1' })

      // All reads from WAL
      expect((await reader.find('users'))).toHaveLength(2)
      expect((await reader.find('posts'))).toHaveLength(1)
      expect((await reader.findRels('u1'))).toHaveLength(1)
      expect((await reader.findEvents())).toHaveLength(3)

      // --- Phase 2: First compaction (simulate alarm trigger) ---
      expect(compactor.shouldCompact(5)).toBe(true)
      await compactor.compactAll()
      expect(compactor.shouldCompact(1)).toBe(false)

      // All reads now from R2
      expect((await reader.find('users'))).toHaveLength(2)
      expect((await reader.find('posts'))).toHaveLength(1)
      expect((await reader.findRels('u1'))).toHaveLength(1)
      expect((await reader.findEvents())).toHaveLength(3)

      // --- Phase 3: More writes (buffered in WAL) ---
      wal.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Smith', role: 'admin' })
      wal.append('posts', { $id: 'p2', $op: 'c', $v: 1, $ts: 2001, title: 'Second Post', authorId: 'u1' })
      wal.append('rels', { $op: 'l', $ts: 2002, f: 'u1', p: 'posts', r: 'author', t: 'p2' })
      wal.append('events', { id: 'e4', ts: 2000, op: 'u', ns: 'users', eid: 'u1' })
      wal.append('events', { id: 'e5', ts: 2001, op: 'c', ns: 'posts', eid: 'p2' })

      // Reads merge R2 + WAL
      const users = await reader.find('users')
      expect(users).toHaveLength(2)
      expect(users.find((u) => u.$id === 'u1')!.name).toBe('Alice Smith')
      expect(users.find((u) => u.$id === 'u2')!.name).toBe('Bob')

      const posts = await reader.find('posts')
      expect(posts).toHaveLength(2)

      const u1Rels = await reader.findRels('u1')
      expect(u1Rels).toHaveLength(2) // p1 (R2) + p2 (WAL)

      const events = await reader.findEvents()
      expect(events).toHaveLength(5) // 3 (R2) + 2 (WAL)

      // --- Phase 4: Delete a user ---
      wal.append('users', { $id: 'u2', $op: 'd', $v: 2, $ts: 3000 })
      wal.append('events', { id: 'e6', ts: 3000, op: 'd', ns: 'users', eid: 'u2' })

      // u2 should be gone
      const usersAfterDelete = await reader.find('users')
      expect(usersAfterDelete).toHaveLength(1)
      expect(usersAfterDelete[0].$id).toBe('u1')

      // Filter still works across merged data
      const admins = await reader.find('users', { role: 'admin' })
      expect(admins).toHaveLength(1)
      expect(admins[0].name).toBe('Alice Smith')

      // --- Phase 5: Final compaction ---
      await compactor.compactAll()

      // Final verification: all data consistent after compaction
      const finalUsers = await reader.find('users')
      expect(finalUsers).toHaveLength(1)
      expect(finalUsers[0].$id).toBe('u1')
      expect(finalUsers[0].name).toBe('Alice Smith')

      const finalPosts = await reader.find('posts')
      expect(finalPosts).toHaveLength(2)

      const finalRels = await reader.findRels('u1')
      expect(finalRels).toHaveLength(2)

      const finalEvents = await reader.findEvents()
      expect(finalEvents).toHaveLength(6) // All 6 events preserved (append-only)

      // WAL completely empty
      expect(wal.getUnflushedCount()).toBe(0)
    })
  })
})

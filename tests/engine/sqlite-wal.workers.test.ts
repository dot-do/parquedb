/**
 * SqliteWal Workers Tests
 *
 * Tests the SQLite WAL adapter for MergeTree engine using REAL Cloudflare
 * DO SQLite bindings via vitest-pool-workers. No mocking.
 *
 * Run with: npx vitest run --project 'engine:workers'
 */

import { env } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface TestEnv {
  MERGETREE: DurableObjectNamespace
}

interface MergeTreeStub {
  append(table: string, line: Record<string, unknown>): Promise<void>
  appendBatch(table: string, lines: Record<string, unknown>[]): Promise<void>
  getBatches(table: string): Promise<Array<{ id: number; ts: number; batch: string; row_count: number }>>
  getAllBatches(): Promise<Array<{ id: number; ts: number; kind: string; batch: string; row_count: number }>>
  markFlushed(batchIds: number[]): Promise<void>
  cleanup(): Promise<void>
  getUnflushedCount(): Promise<number>
  getUnflushedCountForTable(table: string): Promise<number>
  getUnflushedTables(): Promise<string[]>
  replayUnflushed(table: string): Promise<Record<string, unknown>[]>
}

const testEnv = env as unknown as TestEnv

function getStub(name?: string): MergeTreeStub {
  const id = testEnv.MERGETREE.idFromName(name ?? `test-${Date.now()}-${Math.random()}`)
  return testEnv.MERGETREE.get(id) as unknown as MergeTreeStub
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('SqliteWal (real DO SQLite)', () => {

  // 1. append() stores a single mutation
  it('append stores a single mutation', async () => {
    const stub = getStub()
    await stub.append('users', { $id: 'u1', $op: 'c', name: 'Alice' })

    const batches = await stub.getBatches('users')
    expect(batches).toHaveLength(1)
    expect(batches[0].row_count).toBe(1)

    const parsed = JSON.parse(batches[0].batch)
    expect(parsed).toHaveLength(1)
    expect(parsed[0]).toEqual({ $id: 'u1', $op: 'c', name: 'Alice' })
  })

  // 2. appendBatch() stores multiple lines as one batch
  it('appendBatch stores multiple lines as one batch', async () => {
    const stub = getStub()
    const lines = [
      { $id: 'u1', $op: 'c', name: 'Alice' },
      { $id: 'u2', $op: 'c', name: 'Bob' },
      { $id: 'u3', $op: 'c', name: 'Charlie' },
    ]
    await stub.appendBatch('users', lines)

    const batches = await stub.getBatches('users')
    expect(batches).toHaveLength(1)
    expect(batches[0].row_count).toBe(3)

    const parsed = JSON.parse(batches[0].batch)
    expect(parsed).toHaveLength(3)
    expect(parsed[0].name).toBe('Alice')
    expect(parsed[1].name).toBe('Bob')
    expect(parsed[2].name).toBe('Charlie')
  })

  // 3. getBatches() returns unflushed batches for table
  it('getBatches returns unflushed batches for table', async () => {
    const stub = getStub()
    await stub.append('orders', { $id: 'o1', total: 100 })
    await stub.append('orders', { $id: 'o2', total: 200 })

    const batches = await stub.getBatches('orders')
    expect(batches).toHaveLength(2)

    // Each batch should have expected fields
    for (const batch of batches) {
      expect(batch.id).toBeTypeOf('number')
      expect(batch.ts).toBeTypeOf('number')
      expect(batch.row_count).toBe(1)
      expect(batch.batch).toBeTypeOf('string')
    }
  })

  // 4. getBatches() returns empty for unknown table
  it('getBatches returns empty array for unknown table', async () => {
    const stub = getStub()
    await stub.append('users', { $id: 'u1', name: 'Alice' })

    const batches = await stub.getBatches('nonexistent')
    expect(batches).toHaveLength(0)
    expect(batches).toEqual([])
  })

  // 5. markFlushed() marks batches as flushed (excluded from getBatches)
  it('markFlushed excludes batches from getBatches', async () => {
    const stub = getStub()
    await stub.append('users', { $id: 'u1', name: 'Alice' })
    await stub.append('users', { $id: 'u2', name: 'Bob' })
    await stub.append('users', { $id: 'u3', name: 'Charlie' })

    const batches = await stub.getBatches('users')
    expect(batches).toHaveLength(3)

    // Flush the first two batches
    await stub.markFlushed([batches[0].id, batches[1].id])

    const remaining = await stub.getBatches('users')
    expect(remaining).toHaveLength(1)
    expect(JSON.parse(remaining[0].batch)[0].name).toBe('Charlie')
  })

  // 6. cleanup() removes flushed batches
  it('cleanup removes flushed batches from storage', async () => {
    const stub = getStub()
    await stub.append('users', { $id: 'u1', name: 'Alice' })
    await stub.append('users', { $id: 'u2', name: 'Bob' })

    const batches = await stub.getBatches('users')

    // Mark first batch as flushed
    await stub.markFlushed([batches[0].id])

    // Cleanup removes flushed rows
    await stub.cleanup()

    // The unflushed batch should still be there
    const remaining = await stub.getBatches('users')
    expect(remaining).toHaveLength(1)
    expect(JSON.parse(remaining[0].batch)[0].name).toBe('Bob')

    // Count should only reflect remaining
    const count = await stub.getUnflushedCount()
    expect(count).toBe(1)
  })

  // 7. getUnflushedCount() returns total pending count
  it('getUnflushedCount returns total pending row count', async () => {
    const stub = getStub()

    // Start with zero
    expect(await stub.getUnflushedCount()).toBe(0)

    // Single-line appends
    await stub.append('users', { $id: 'u1' })
    await stub.append('posts', { $id: 'p1' })
    expect(await stub.getUnflushedCount()).toBe(2)

    // Batch append adds row_count of the batch
    await stub.appendBatch('orders', [{ $id: 'o1' }, { $id: 'o2' }, { $id: 'o3' }])
    expect(await stub.getUnflushedCount()).toBe(5)
  })

  // 8. getUnflushedCountForTable() returns per-table count
  it('getUnflushedCountForTable returns per-table count', async () => {
    const stub = getStub()

    await stub.append('users', { $id: 'u1' })
    await stub.appendBatch('users', [{ $id: 'u2' }, { $id: 'u3' }])
    await stub.append('posts', { $id: 'p1' })

    expect(await stub.getUnflushedCountForTable('users')).toBe(3)
    expect(await stub.getUnflushedCountForTable('posts')).toBe(1)
    expect(await stub.getUnflushedCountForTable('nonexistent')).toBe(0)
  })

  // 9. getUnflushedTables() returns tables with pending data
  it('getUnflushedTables returns tables with pending data', async () => {
    const stub = getStub()

    // No tables initially
    expect(await stub.getUnflushedTables()).toEqual([])

    await stub.append('users', { $id: 'u1' })
    await stub.append('posts', { $id: 'p1' })
    await stub.append('orders', { $id: 'o1' })

    const tables = await stub.getUnflushedTables()
    expect(tables).toHaveLength(3)
    // Sorted alphabetically
    expect(tables).toEqual(['orders', 'posts', 'users'])
  })

  // 10. replayUnflushed() returns all lines in order
  it('replayUnflushed returns all lines in insertion order', async () => {
    const stub = getStub()

    await stub.append('users', { $id: 'u1', name: 'Alice' })
    await stub.appendBatch('users', [
      { $id: 'u2', name: 'Bob' },
      { $id: 'u3', name: 'Charlie' },
    ])
    await stub.append('users', { $id: 'u4', name: 'Diana' })

    const lines = await stub.replayUnflushed('users')
    expect(lines).toHaveLength(4)
    expect(lines[0]).toEqual({ $id: 'u1', name: 'Alice' })
    expect(lines[1]).toEqual({ $id: 'u2', name: 'Bob' })
    expect(lines[2]).toEqual({ $id: 'u3', name: 'Charlie' })
    expect(lines[3]).toEqual({ $id: 'u4', name: 'Diana' })
  })

  // 11. Multiple tables are isolated
  it('multiple tables are isolated from each other', async () => {
    const stub = getStub()

    await stub.append('users', { $id: 'u1', name: 'Alice' })
    await stub.append('posts', { $id: 'p1', title: 'Hello' })
    await stub.append('users', { $id: 'u2', name: 'Bob' })

    // Users should only see user data
    const userBatches = await stub.getBatches('users')
    expect(userBatches).toHaveLength(2)
    expect(JSON.parse(userBatches[0].batch)[0].$id).toBe('u1')
    expect(JSON.parse(userBatches[1].batch)[0].$id).toBe('u2')

    // Posts should only see post data
    const postBatches = await stub.getBatches('posts')
    expect(postBatches).toHaveLength(1)
    expect(JSON.parse(postBatches[0].batch)[0].$id).toBe('p1')

    // Replay should be isolated too
    const userLines = await stub.replayUnflushed('users')
    expect(userLines).toHaveLength(2)
    expect(userLines.every(l => (l as any).$id.startsWith('u'))).toBe(true)

    const postLines = await stub.replayUnflushed('posts')
    expect(postLines).toHaveLength(1)
    expect((postLines[0] as any).$id).toBe('p1')

    // Marking users as flushed should not affect posts
    const userIds = userBatches.map(b => b.id)
    await stub.markFlushed(userIds)

    expect(await stub.getUnflushedCountForTable('users')).toBe(0)
    expect(await stub.getUnflushedCountForTable('posts')).toBe(1)
  })

  // 12. WAL survives DO stub re-creation (persistence test)
  it('WAL persists across DO stub re-creation', async () => {
    const doName = `persist-test-${Date.now()}-${Math.random()}`

    // First stub: write data
    const stub1 = getStub(doName)
    await stub1.append('users', { $id: 'u1', name: 'Alice' })
    await stub1.appendBatch('users', [
      { $id: 'u2', name: 'Bob' },
      { $id: 'u3', name: 'Charlie' },
    ])

    // Verify data is there
    expect(await stub1.getUnflushedCount()).toBe(3)

    // Second stub: same DO name should see the same data
    const stub2 = getStub(doName)
    const batches = await stub2.getBatches('users')
    expect(batches).toHaveLength(2)

    const count = await stub2.getUnflushedCount()
    expect(count).toBe(3)

    const lines = await stub2.replayUnflushed('users')
    expect(lines).toHaveLength(3)
    expect(lines[0]).toEqual({ $id: 'u1', name: 'Alice' })
    expect(lines[1]).toEqual({ $id: 'u2', name: 'Bob' })
    expect(lines[2]).toEqual({ $id: 'u3', name: 'Charlie' })
  })

  // 13. Batch ordering is preserved (FIFO)
  it('batch ordering is preserved in FIFO order', async () => {
    const stub = getStub()

    // Insert 10 sequential appends
    for (let i = 0; i < 10; i++) {
      await stub.append('events', { seq: i, data: `event-${i}` })
    }

    const batches = await stub.getBatches('events')
    expect(batches).toHaveLength(10)

    // IDs should be monotonically increasing
    for (let i = 1; i < batches.length; i++) {
      expect(batches[i].id).toBeGreaterThan(batches[i - 1].id)
    }

    // Data should be in insertion order
    const lines = await stub.replayUnflushed('events')
    expect(lines).toHaveLength(10)
    for (let i = 0; i < 10; i++) {
      expect((lines[i] as any).seq).toBe(i)
      expect((lines[i] as any).data).toBe(`event-${i}`)
    }
  })

  // 14. getAllBatches returns batches across all tables
  it('getAllBatches returns batches across all tables', async () => {
    const stub = getStub()

    await stub.append('users', { $id: 'u1' })
    await stub.append('posts', { $id: 'p1' })
    await stub.append('orders', { $id: 'o1' })

    const allBatches = await stub.getAllBatches()
    expect(allBatches).toHaveLength(3)

    // Each batch should include the kind field
    const kinds = allBatches.map(b => b.kind).sort()
    expect(kinds).toEqual(['orders', 'posts', 'users'])
  })

  // 15. markFlushed with empty array is a no-op
  it('markFlushed with empty array is a no-op', async () => {
    const stub = getStub()

    await stub.append('users', { $id: 'u1' })

    // Should not throw
    await stub.markFlushed([])

    // Data should still be there
    expect(await stub.getUnflushedCount()).toBe(1)
  })

})

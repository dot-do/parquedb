/**
 * DOReadPath Workers Tests
 *
 * Tests the three-way merge-on-read (R2 Parquet + SQLite WAL) using REAL
 * Cloudflare DO SQLite + R2 bindings via vitest-pool-workers. No mocking.
 *
 * Run with: pnpm vitest run --project 'engine:workers' tests/engine/do-read-path.workers.test.ts
 */

import { env } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DataLine {
  $id: string
  $op: 'c' | 'u' | 'd'
  $v: number
  $ts: number
  [key: string]: unknown
}

interface RelLine {
  $op: 'l' | 'u'
  $ts: number
  f: string
  p: string
  r: string
  t: string
}

interface TestEnv {
  MERGETREE: DurableObjectNamespace
  BUCKET: R2Bucket
}

interface MergeTreeStub {
  // WAL methods
  append(table: string, line: Record<string, unknown>): Promise<void>
  appendBatch(table: string, lines: Record<string, unknown>[]): Promise<void>
  getBatches(table: string): Promise<Array<{ id: number; ts: number; batch: string; row_count: number }>>
  replayUnflushed(table: string): Promise<Record<string, unknown>[]>
  getUnflushedCount(): Promise<number>

  // DOReadPath methods
  find(table: string, filter?: Record<string, unknown>): Promise<DataLine[]>
  getById(table: string, id: string): Promise<DataLine | null>
  findRels(fromId?: string): Promise<RelLine[]>
  findEvents(): Promise<Record<string, unknown>[]>

  // R2 seeding
  seedR2Data(table: string, data: DataLine[]): Promise<void>
  seedR2Rels(rels: RelLine[]): Promise<void>
  seedR2Events(events: Record<string, unknown>[]): Promise<void>
}

const testEnv = env as unknown as TestEnv

let testCounter = 0
function getStub(name?: string): MergeTreeStub {
  const id = testEnv.MERGETREE.idFromName(name ?? `read-test-${++testCounter}-${Date.now()}`)
  return testEnv.MERGETREE.get(id) as unknown as MergeTreeStub
}

// ---------------------------------------------------------------------------
// Tests: find()
// ---------------------------------------------------------------------------

describe('DOReadPath.find()', () => {
  // 1. find returns empty array when no data exists
  it('returns empty array when no data exists', async () => {
    const stub = getStub()
    const results = await stub.find('users')
    expect(results).toEqual([])
  })

  // 2. find returns data from R2 Parquet only (no WAL)
  it('returns data from R2 Parquet only', async () => {
    const stub = getStub()

    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' },
    ])

    const results = await stub.find('users')
    expect(results).toHaveLength(2)
    expect(results[0].$id).toBe('u1')
    expect(results[0].name).toBe('Alice')
    expect(results[1].$id).toBe('u2')
    expect(results[1].name).toBe('Bob')
  })

  // 3. find returns data from WAL only (no R2)
  it('returns data from WAL only', async () => {
    const stub = getStub()

    await stub.append('posts', { $id: 'p1', $op: 'c', $v: 1, $ts: 2000, title: 'Hello' })
    await stub.append('posts', { $id: 'p2', $op: 'c', $v: 1, $ts: 2001, title: 'World' })

    const results = await stub.find('posts')
    expect(results).toHaveLength(2)

    // Results are sorted by $id
    const titles = results.map((r) => r.title)
    expect(titles).toContain('Hello')
    expect(titles).toContain('World')
  })

  // 4. find merges R2 + WAL (WAL overrides by higher $v)
  it('merges R2 + WAL with WAL overriding by higher $v', async () => {
    const stub = getStub()

    // R2 has Alice v1
    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' },
    ])

    // WAL has Alice v2 (updated name)
    await stub.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Smith' })

    const results = await stub.find('users')
    expect(results).toHaveLength(2)

    const alice = results.find((r) => r.$id === 'u1')
    expect(alice).toBeDefined()
    expect(alice!.name).toBe('Alice Smith')
    expect(alice!.$v).toBe(2)

    // Bob should still be from R2 unchanged
    const bob = results.find((r) => r.$id === 'u2')
    expect(bob).toBeDefined()
    expect(bob!.name).toBe('Bob')
    expect(bob!.$v).toBe(1)
  })

  // 5. find handles deletes in WAL (removes from results)
  it('handles deletes in WAL', async () => {
    const stub = getStub()

    // R2 has two entities
    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' },
    ])

    // WAL deletes u1
    await stub.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

    const results = await stub.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].$id).toBe('u2')
    expect(results[0].name).toBe('Bob')
  })

  // 6. find applies simple equality filter
  it('applies simple equality filter', async () => {
    const stub = getStub()

    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', role: 'admin' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob', role: 'user' },
      { $id: 'u3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie', role: 'admin' },
    ])

    const admins = await stub.find('users', { role: 'admin' })
    expect(admins).toHaveLength(2)
    expect(admins.map((a) => a.name).sort()).toEqual(['Alice', 'Charlie'])

    const users = await stub.find('users', { role: 'user' })
    expect(users).toHaveLength(1)
    expect(users[0].name).toBe('Bob')
  })
})

// ---------------------------------------------------------------------------
// Tests: getById()
// ---------------------------------------------------------------------------

describe('DOReadPath.getById()', () => {
  // 7. getById returns entity from R2
  it('returns entity from R2', async () => {
    const stub = getStub()

    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' },
    ])

    const alice = await stub.getById('users', 'u1')
    expect(alice).not.toBeNull()
    expect(alice!.$id).toBe('u1')
    expect(alice!.name).toBe('Alice')
  })

  // 8. getById returns entity from WAL (overrides R2)
  it('returns entity from WAL overriding R2', async () => {
    const stub = getStub()

    // R2 has Alice v1
    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
    ])

    // WAL has Alice v2
    await stub.append('users', { $id: 'u1', $op: 'u', $v: 2, $ts: 2000, name: 'Alice Updated' })

    const alice = await stub.getById('users', 'u1')
    expect(alice).not.toBeNull()
    expect(alice!.name).toBe('Alice Updated')
    expect(alice!.$v).toBe(2)
  })

  // 9. getById returns null for deleted entity
  it('returns null for deleted entity', async () => {
    const stub = getStub()

    // R2 has Alice
    await stub.seedR2Data('users', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' },
    ])

    // WAL deletes Alice
    await stub.append('users', { $id: 'u1', $op: 'd', $v: 2, $ts: 2000 })

    const result = await stub.getById('users', 'u1')
    expect(result).toBeNull()
  })

  // 10. getById returns null for non-existent entity
  it('returns null for non-existent entity', async () => {
    const stub = getStub()
    const result = await stub.getById('users', 'nonexistent')
    expect(result).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// Tests: findRels()
// ---------------------------------------------------------------------------

describe('DOReadPath.findRels()', () => {
  // 11. findRels merges R2 + WAL relationships
  it('merges R2 + WAL relationships', async () => {
    const stub = getStub()

    // R2 has one relationship
    await stub.seedR2Rels([
      { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
    ])

    // WAL has another relationship
    await stub.append('rels', { $op: 'l', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p2' })

    const rels = await stub.findRels()
    expect(rels).toHaveLength(2)

    const targets = rels.map((r) => r.t).sort()
    expect(targets).toEqual(['p1', 'p2'])
  })

  // 12. findRels filters out unlinks
  it('filters out unlinks', async () => {
    const stub = getStub()

    // R2 has a link
    await stub.seedR2Rels([
      { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
      { $op: 'l', $ts: 1001, f: 'u1', p: 'posts', r: 'author', t: 'p2' },
    ])

    // WAL unlinks p1
    await stub.append('rels', { $op: 'u', $ts: 2000, f: 'u1', p: 'posts', r: 'author', t: 'p1' })

    const rels = await stub.findRels()
    expect(rels).toHaveLength(1)
    expect(rels[0].t).toBe('p2')
  })

  // 13. findRels filters by fromId
  it('filters by fromId', async () => {
    const stub = getStub()

    await stub.seedR2Rels([
      { $op: 'l', $ts: 1000, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
      { $op: 'l', $ts: 1001, f: 'u2', p: 'posts', r: 'author', t: 'p2' },
    ])

    const u1Rels = await stub.findRels('u1')
    expect(u1Rels).toHaveLength(1)
    expect(u1Rels[0].f).toBe('u1')
    expect(u1Rels[0].t).toBe('p1')

    const u2Rels = await stub.findRels('u2')
    expect(u2Rels).toHaveLength(1)
    expect(u2Rels[0].f).toBe('u2')
    expect(u2Rels[0].t).toBe('p2')
  })
})

// ---------------------------------------------------------------------------
// Tests: findEvents()
// ---------------------------------------------------------------------------

describe('DOReadPath.findEvents()', () => {
  // 14. findEvents concatenates R2 + WAL events
  it('concatenates R2 + WAL events sorted by ts', async () => {
    const stub = getStub()

    // R2 has older events
    await stub.seedR2Events([
      { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' },
      { id: 'e2', ts: 1002, op: 'c', ns: 'users', eid: 'u2' },
    ])

    // WAL has newer event
    await stub.append('events', { id: 'e3', ts: 2000, op: 'u', ns: 'users', eid: 'u1' })

    const events = await stub.findEvents()
    expect(events).toHaveLength(3)

    // Should be sorted by ts
    const timestamps = events.map((e) => {
      const ts = e.ts
      return typeof ts === 'bigint' ? Number(ts) : (ts as number)
    })
    expect(timestamps[0]).toBeLessThanOrEqual(timestamps[1] as number)
    expect(timestamps[1]).toBeLessThanOrEqual(timestamps[2] as number)
  })
})

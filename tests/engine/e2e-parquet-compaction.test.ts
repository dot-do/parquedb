/**
 * E2E Parquet Compaction Integration Test
 *
 * Validates the most critical production path end-to-end:
 *   write entities -> compact to real Parquet via ParquetStorageAdapter
 *   -> re-open engine -> read from Parquet + buffer merge
 *
 * This test differs from engine-parquet-e2e.test.ts by additionally exercising:
 * - Relationship compaction through real Parquet (via hybridCompactRels)
 * - Event compaction through real Parquet (via hybridCompactEvents)
 * - The full hybridCompactAll flow across data, rels, and events
 * - Buffer + Parquet merge: writing more entities after compaction and
 *   verifying that find() returns both compacted (Parquet) and new (JSONL) data
 * - Multi-table entity compaction with separate Parquet files per table
 *
 * Beads issue: zou5.24
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { ParquetStorageAdapter } from '@/engine/parquet-adapter'
import { JsonlWriter } from '@/engine/jsonl-writer'
import {
  hybridCompactRels,
  hybridCompactEvents,
  hybridCompactAll,
} from '@/engine/storage-adapters'
import type { RelLine } from '@/engine/types'

// =============================================================================
// Setup / Teardown
// =============================================================================

let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-e2e-compaction-'))
})

afterEach(async () => {
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// Helpers
// =============================================================================

/** PAR1 magic bytes at both header and footer of a valid Parquet file */
const PAR1_MAGIC = [0x50, 0x41, 0x52, 0x31]

/**
 * Assert that a file at the given path starts and ends with PAR1 magic bytes,
 * confirming it is a valid Parquet file.
 */
async function assertParquetFile(path: string): Promise<void> {
  const data = await readFile(path)
  expect(data.byteLength).toBeGreaterThanOrEqual(8)
  // Header: PAR1
  expect(data[0]).toBe(PAR1_MAGIC[0])
  expect(data[1]).toBe(PAR1_MAGIC[1])
  expect(data[2]).toBe(PAR1_MAGIC[2])
  expect(data[3]).toBe(PAR1_MAGIC[3])
  // Footer: PAR1
  const len = data.byteLength
  expect(data[len - 4]).toBe(PAR1_MAGIC[0])
  expect(data[len - 3]).toBe(PAR1_MAGIC[1])
  expect(data[len - 2]).toBe(PAR1_MAGIC[2])
  expect(data[len - 1]).toBe(PAR1_MAGIC[3])
}

/**
 * Write RelLines to rels.jsonl via JsonlWriter.
 * The engine does not manage relationship writes directly, so we use
 * the JSONL writer to simulate relationship mutations.
 */
async function writeRels(dir: string, rels: RelLine[]): Promise<void> {
  const writer = new JsonlWriter(join(dir, 'rels.jsonl'))
  for (const rel of rels) {
    await writer.append(rel)
  }
  await writer.close()
}

// =============================================================================
// Test Suite: Full lifecycle with entities, relationships, and events
// =============================================================================

describe('E2E Parquet Compaction - full lifecycle', () => {
  it('write entities to multiple tables -> compactAll -> re-open -> verify all data survived roundtrip', async () => {
    // Phase 1: Create entities across multiple tables
    const engine1 = new ParqueEngine({ dataDir })

    const alice = await engine1.create('users', {
      name: 'Alice',
      age: 30,
      email: 'alice@example.com',
      active: true,
    })

    const bob = await engine1.create('users', {
      name: 'Bob',
      age: 25,
      tags: ['developer', 'remote'],
    })

    const post1 = await engine1.create('posts', {
      title: 'Hello World',
      body: 'My first post',
      authorId: alice.$id,
    })

    const post2 = await engine1.create('posts', {
      title: 'Advanced TypeScript',
      body: 'Generics deep dive',
      authorId: bob.$id,
      tags: ['typescript', 'tutorial'],
    })

    const comment1 = await engine1.create('comments', {
      body: 'Great post!',
      postId: post1.$id,
      authorId: bob.$id,
    })

    // Phase 2: Compact all tables to Parquet
    await engine1.compactAll()

    // Phase 3: Verify real Parquet files exist for each table
    await assertParquetFile(join(dataDir, 'users.parquet'))
    await assertParquetFile(join(dataDir, 'posts.parquet'))
    await assertParquetFile(join(dataDir, 'comments.parquet'))

    await engine1.close()

    // Phase 4: Create a NEW engine instance pointing to same directory
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    // Phase 5: Verify all entities survived the roundtrip
    const users = await engine2.find('users')
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob'])

    const aliceFound = await engine2.get('users', alice.$id)
    expect(aliceFound).not.toBeNull()
    expect(aliceFound!.name).toBe('Alice')
    expect(aliceFound!.age).toBe(30)
    expect(aliceFound!.email).toBe('alice@example.com')
    expect(aliceFound!.active).toBe(true)
    expect(aliceFound!.$v).toBe(1)
    expect(aliceFound!.$op).toBe('c')

    const bobFound = await engine2.get('users', bob.$id)
    expect(bobFound).not.toBeNull()
    expect(bobFound!.tags).toEqual(['developer', 'remote'])

    const posts = await engine2.find('posts')
    expect(posts).toHaveLength(2)
    expect(posts.map(p => p.title).sort()).toEqual(['Advanced TypeScript', 'Hello World'])

    const post2Found = await engine2.get('posts', post2.$id)
    expect(post2Found).not.toBeNull()
    expect(post2Found!.tags).toEqual(['typescript', 'tutorial'])
    expect(post2Found!.authorId).toBe(bob.$id)

    const comments = await engine2.find('comments')
    expect(comments).toHaveLength(1)
    expect(comments[0].body).toBe('Great post!')
    expect(comments[0].postId).toBe(post1.$id)

    await engine2.close()
  })

  it('Parquet files are NOT valid JSON (confirms binary format)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.compact('users')

    const rawContent = await readFile(join(dataDir, 'users.parquet'), 'utf-8')
    expect(() => JSON.parse(rawContent)).toThrow()

    await engine.close()
  })
})

// =============================================================================
// Test Suite: Buffer + Parquet merge (write more after compact)
// =============================================================================

describe('E2E Parquet Compaction - buffer + Parquet merge', () => {
  it('write -> compact -> write more -> find() returns both compacted and buffered entities', async () => {
    const engine = new ParqueEngine({ dataDir })

    // Write initial entities and compact to Parquet
    const alice = await engine.create('users', { name: 'Alice', role: 'admin' })
    const bob = await engine.create('users', { name: 'Bob', role: 'user' })
    await engine.compact('users')

    // Write more entities AFTER compaction (these remain in JSONL buffer)
    const charlie = await engine.create('users', { name: 'Charlie', role: 'moderator' })
    const diana = await engine.create('users', { name: 'Diana', role: 'user' })

    // find() should merge Parquet (Alice, Bob) + buffer (Charlie, Diana)
    const allUsers = await engine.find('users')
    expect(allUsers).toHaveLength(4)
    expect(allUsers.map(u => u.name).sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana'])

    // get() should work for both Parquet and buffer entities
    expect((await engine.get('users', alice.$id))!.name).toBe('Alice')
    expect((await engine.get('users', charlie.$id))!.name).toBe('Charlie')

    // Filtering should work across merged results
    const regularUsers = await engine.find('users', { role: 'user' })
    expect(regularUsers).toHaveLength(2)
    expect(regularUsers.map(u => u.name).sort()).toEqual(['Bob', 'Diana'])

    await engine.close()
  })

  it('compact -> write more -> close -> re-open -> JSONL overlay merges with Parquet', async () => {
    // Session 1: create, compact, write more, then close WITHOUT second compact
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('items', { title: 'Item A', qty: 10 })
    await engine1.create('items', { title: 'Item B', qty: 20 })
    await engine1.compact('items')

    await engine1.create('items', { title: 'Item C', qty: 30 })
    await engine1.create('items', { title: 'Item D', qty: 40 })
    await engine1.close()

    // Session 2: init should merge Parquet (A, B) + leftover JSONL (C, D)
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const items = await engine2.find('items')
    expect(items).toHaveLength(4)
    expect(items.map(i => i.title).sort()).toEqual([
      'Item A', 'Item B', 'Item C', 'Item D',
    ])

    // Verify data integrity from both sources
    const itemA = items.find(i => i.title === 'Item A')!
    expect(itemA.qty).toBe(10)
    const itemD = items.find(i => i.title === 'Item D')!
    expect(itemD.qty).toBe(40)

    await engine2.close()
  })

  it('update after compaction: buffer overlay wins over Parquet data', async () => {
    const engine = new ParqueEngine({ dataDir })

    const entity = await engine.create('users', { name: 'Alice', age: 30, score: 100 })
    await engine.compact('users')

    // Update after compaction (stays in JSONL buffer)
    const updated = await engine.update('users', entity.$id, {
      $set: { name: 'Alice Smith', age: 31 },
      $inc: { score: 50 },
    })
    expect(updated.$v).toBe(2)

    // get() should return the updated version
    const found = await engine.get('users', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Alice Smith')
    expect(found!.age).toBe(31)
    expect(found!.score).toBe(150)
    expect(found!.$v).toBe(2)

    await engine.close()
  })

  it('delete after compaction: tombstone suppresses Parquet entity', async () => {
    const engine = new ParqueEngine({ dataDir })

    const alice = await engine.create('users', { name: 'Alice' })
    const bob = await engine.create('users', { name: 'Bob' })
    await engine.compact('users')

    await engine.delete('users', alice.$id)

    expect(await engine.get('users', alice.$id)).toBeNull()
    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Bob')

    await engine.close()
  })

  it('delete -> close -> re-open: JSONL tombstone suppresses Parquet entity across restart', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const entity = await engine1.create('users', { name: 'Ghost' })
    await engine1.create('users', { name: 'Alive' })
    await engine1.compact('users')

    // Delete without compacting again
    await engine1.delete('users', entity.$id)
    await engine1.close()

    // Re-init: JSONL tombstone should override Parquet entity
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    expect(await engine2.get('users', entity.$id)).toBeNull()
    const results = await engine2.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alive')

    await engine2.close()
  })
})

// =============================================================================
// Test Suite: Double compaction cycle
// =============================================================================

describe('E2E Parquet Compaction - double compaction cycle', () => {
  it('write -> compact -> write -> compact -> re-init: all data intact through two Parquet cycles', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    // First batch + compact
    await engine1.create('logs', { message: 'Log 1', level: 'info' })
    await engine1.create('logs', { message: 'Log 2', level: 'warn' })
    const firstCount = await engine1.compact('logs')
    expect(firstCount).toBe(2)

    // Second batch + compact (merges into existing Parquet)
    await engine1.create('logs', { message: 'Log 3', level: 'error' })
    await engine1.create('logs', { message: 'Log 4', level: 'debug' })
    const secondCount = await engine1.compact('logs')
    expect(secondCount).toBe(4) // All 4 merged

    await engine1.close()

    // Re-init and verify all data survived two compaction cycles
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const logs = await engine2.find('logs')
    expect(logs).toHaveLength(4)
    expect(logs.map(l => l.message).sort()).toEqual([
      'Log 1', 'Log 2', 'Log 3', 'Log 4',
    ])

    const errorLog = logs.find(l => l.level === 'error')!
    expect(errorLog.message).toBe('Log 3')

    await engine2.close()
  })

  it('update survives double compaction and re-init', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    const entity = await engine1.create('users', { name: 'Bob', level: 1 })
    await engine1.compact('users')

    await engine1.update('users', entity.$id, { $set: { name: 'Bob Senior', level: 5 } })

    // Second compaction merges the update into Parquet
    const count = await engine1.compact('users')
    expect(count).toBe(1)

    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const found = await engine2.get('users', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Bob Senior')
    expect(found!.level).toBe(5)
    expect(found!.$v).toBe(2)

    await engine2.close()
  })

  it('delete survives double compaction: entity removed from Parquet output', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    const entity = await engine1.create('users', { name: 'ToBeDeleted' })
    await engine1.create('users', { name: 'Survivor' })
    await engine1.compact('users')

    await engine1.delete('users', entity.$id)

    // Second compaction: tombstone merges, entity is excluded
    const count = await engine1.compact('users')
    expect(count).toBe(1) // Only 'Survivor' remains

    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    expect(await engine2.get('users', entity.$id)).toBeNull()
    const results = await engine2.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Survivor')

    await engine2.close()
  })
})

// =============================================================================
// Test Suite: Relationship Parquet roundtrip
// =============================================================================

describe('E2E Parquet Compaction - relationships', () => {
  it('write rels to JSONL -> compact via ParquetStorageAdapter -> read back from Parquet', async () => {
    const adapter = new ParquetStorageAdapter()
    const ts = Date.now()

    // Write relationship mutations to rels.jsonl
    const rels: RelLine[] = [
      { $op: 'l', $ts: ts, f: 'user:u1', p: 'posts', r: 'author', t: 'post:p1' },
      { $op: 'l', $ts: ts, f: 'user:u1', p: 'posts', r: 'author', t: 'post:p2' },
      { $op: 'l', $ts: ts, f: 'user:u2', p: 'follows', r: 'followers', t: 'user:u1' },
    ]
    await writeRels(dataDir, rels)

    // Compact using hybridCompactRels with real Parquet adapter
    const count = await hybridCompactRels(dataDir, adapter)
    expect(count).toBe(3)

    // Verify Parquet file was created
    await assertParquetFile(join(dataDir, 'rels.parquet'))

    // Read back directly from Parquet and verify
    const result = await adapter.readRels(join(dataDir, 'rels.parquet'))
    expect(result).toHaveLength(3)

    const u1Posts = result.filter(r => r.f === 'user:u1' && r.p === 'posts')
    expect(u1Posts).toHaveLength(2)
    expect(u1Posts.map(r => r.t).sort()).toEqual(['post:p1', 'post:p2'])

    const follows = result.find(r => r.p === 'follows')!
    expect(follows.f).toBe('user:u2')
    expect(follows.r).toBe('followers')
    expect(follows.t).toBe('user:u1')
    expect(follows.$op).toBe('l')
    expect(typeof follows.$ts).toBe('number')
  })

  it('link then unlink: unlinked rel is excluded after compaction', async () => {
    const adapter = new ParquetStorageAdapter()
    const ts = Date.now()

    const rels: RelLine[] = [
      { $op: 'l', $ts: ts, f: 'u1', p: 'likes', r: 'likedBy', t: 'p1' },
      { $op: 'l', $ts: ts, f: 'u1', p: 'likes', r: 'likedBy', t: 'p2' },
      // Unlink p2
      { $op: 'u', $ts: ts + 1, f: 'u1', p: 'likes', r: 'likedBy', t: 'p2' },
    ]
    await writeRels(dataDir, rels)

    const count = await hybridCompactRels(dataDir, adapter)
    expect(count).toBe(1)

    const result = await adapter.readRels(join(dataDir, 'rels.parquet'))
    expect(result).toHaveLength(1)
    expect(result[0].f).toBe('u1')
    expect(result[0].t).toBe('p1')
  })

  it('double compaction of rels: second compaction merges new rels into existing Parquet', async () => {
    const adapter = new ParquetStorageAdapter()
    const ts = Date.now()

    // First batch
    const rels1: RelLine[] = [
      { $op: 'l', $ts: ts, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
    ]
    await writeRels(dataDir, rels1)

    const count1 = await hybridCompactRels(dataDir, adapter)
    expect(count1).toBe(1)

    // Second batch
    const rels2: RelLine[] = [
      { $op: 'l', $ts: ts + 1, f: 'u1', p: 'posts', r: 'author', t: 'p2' },
      { $op: 'l', $ts: ts + 1, f: 'u2', p: 'follows', r: 'followers', t: 'u1' },
    ]
    await writeRels(dataDir, rels2)

    const count2 = await hybridCompactRels(dataDir, adapter)
    expect(count2).toBe(3) // p1 from first batch + p2, follows from second

    // Verify all rels survived
    const result = await adapter.readRels(join(dataDir, 'rels.parquet'))
    expect(result).toHaveLength(3)
    expect(result.map(r => `${r.f}:${r.p}:${r.t}`).sort()).toEqual([
      'u1:posts:p1',
      'u1:posts:p2',
      'u2:follows:u1',
    ])
  })
})

// =============================================================================
// Test Suite: Event Parquet roundtrip
// =============================================================================

describe('E2E Parquet Compaction - events', () => {
  it('engine writes events to events.jsonl -> compact via ParquetStorageAdapter -> verify event Parquet', async () => {
    const adapter = new ParquetStorageAdapter()

    // Use the engine to create entities (this writes events to events.jsonl)
    const engine = new ParqueEngine({ dataDir })
    const alice = await engine.create('users', { name: 'Alice' })
    await engine.update('users', alice.$id, { $set: { name: 'Alice Smith' } })
    const bob = await engine.create('users', { name: 'Bob' })
    await engine.delete('users', bob.$id)
    await engine.close()

    // Compact events via hybridCompactEvents with real Parquet adapter
    const count = await hybridCompactEvents(dataDir, adapter)
    // 4 events: create Alice, update Alice, create Bob, delete Bob
    expect(count).toBe(4)

    // The compacted file should be written as events.compacted
    // (events use .compacted extension per compactor-events.ts)
    const compactedPath = join(dataDir, 'events.compacted')
    const events = await adapter.readEvents(compactedPath)
    expect(events).toHaveLength(4)

    // Verify event ordering by timestamp (sorted ascending)
    for (let i = 1; i < events.length; i++) {
      expect(events[i].ts).toBeGreaterThanOrEqual(events[i - 1].ts)
    }

    // Verify event operations
    const ops = events.map(e => e.op)
    expect(ops).toEqual(['c', 'u', 'c', 'd'])

    // Verify the create event for Alice
    const createAlice = events.find(e => e.op === 'c' && e.eid === alice.$id)!
    expect(createAlice).toBeDefined()
    expect(createAlice.ns).toBe('users')
    expect(createAlice.after).toEqual({ name: 'Alice' })

    // Verify the update event
    const updateAlice = events.find(e => e.op === 'u')!
    expect(updateAlice.eid).toBe(alice.$id)
    expect(updateAlice.before).toEqual({ name: 'Alice' })
    expect(updateAlice.after).toEqual({ name: 'Alice Smith' })

    // Verify the delete event
    const deleteBob = events.find(e => e.op === 'd')!
    expect(deleteBob.eid).toBe(bob.$id)
    expect(deleteBob.before).toEqual({ name: 'Bob' })
  })
})

// =============================================================================
// Test Suite: hybridCompactAll - unified compaction of data, rels, and events
// =============================================================================

describe('E2E Parquet Compaction - hybridCompactAll', () => {
  it('compact data + rels + events in one call via ParquetStorageAdapter', async () => {
    const adapter = new ParquetStorageAdapter()
    const ts = Date.now()

    // Write entities via the engine
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })
    await engine.create('posts', { title: 'Hello World' })
    await engine.close()

    // Write rels manually to rels.jsonl
    const rels: RelLine[] = [
      { $op: 'l', $ts: ts, f: 'u1', p: 'posts', r: 'author', t: 'p1' },
      { $op: 'l', $ts: ts, f: 'u2', p: 'follows', r: 'followers', t: 'u1' },
    ]
    await writeRels(dataDir, rels)

    // Compact all in one call
    const result = await hybridCompactAll(dataDir, ['users', 'posts'], adapter)

    // Verify data compaction results
    expect(result.data.get('users')).toBe(2)
    expect(result.data.get('posts')).toBe(1)

    // Verify rels compaction
    expect(result.rels).toBe(2)

    // Verify events compaction (engine created 3 entities = 3 events)
    expect(result.events).toBe(3)

    // Verify real Parquet files exist
    await assertParquetFile(join(dataDir, 'users.parquet'))
    await assertParquetFile(join(dataDir, 'posts.parquet'))
    await assertParquetFile(join(dataDir, 'rels.parquet'))

    // Verify data roundtrip: re-open engine and read from Parquet
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const users = await engine2.find('users')
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob'])

    const posts = await engine2.find('posts')
    expect(posts).toHaveLength(1)
    expect(posts[0].title).toBe('Hello World')

    // Verify rels roundtrip
    const relsResult = await adapter.readRels(join(dataDir, 'rels.parquet'))
    expect(relsResult).toHaveLength(2)

    // Verify events roundtrip
    const eventsResult = await adapter.readEvents(join(dataDir, 'events.compacted'))
    expect(eventsResult).toHaveLength(3)
    expect(eventsResult.every(e => e.op === 'c')).toBe(true)

    await engine2.close()
  })
})

// =============================================================================
// Test Suite: Parquet adapter direct read/write roundtrip
// =============================================================================

describe('E2E Parquet Compaction - ParquetStorageAdapter direct roundtrip', () => {
  it('writeData + readData preserves all field types through Parquet', async () => {
    const adapter = new ParquetStorageAdapter()
    const path = join(dataDir, 'test.parquet')

    const data = [
      {
        $id: 'e1', $op: 'c' as const, $v: 1, $ts: Date.now(),
        name: 'Alice',
        age: 30,
        active: true,
        scores: [95, 88, 72],
        settings: { theme: 'dark', notifications: { email: true, push: false } },
        metadata: { joined: '2024-01-15', referral: null },
        bio: 'A string with "quotes" and special\ncharacters',
      },
      {
        $id: 'e2', $op: 'c' as const, $v: 1, $ts: Date.now(),
        name: 'Bob',
        tags: ['developer', 'remote'],
        emptyObj: {},
      },
    ]

    await adapter.writeData(path, data)
    await assertParquetFile(path)

    const result = await adapter.readData(path)
    expect(result).toHaveLength(2)

    const e1 = result.find(r => r.$id === 'e1')!
    expect(e1.name).toBe('Alice')
    expect(e1.age).toBe(30)
    expect(e1.active).toBe(true)
    expect(e1.scores).toEqual([95, 88, 72])
    expect(e1.settings).toEqual({ theme: 'dark', notifications: { email: true, push: false } })
    expect(e1.metadata).toEqual({ joined: '2024-01-15', referral: null })
    expect(e1.bio).toBe('A string with "quotes" and special\ncharacters')

    const e2 = result.find(r => r.$id === 'e2')!
    expect(e2.tags).toEqual(['developer', 'remote'])
  })

  it('writeRels + readRels roundtrip preserves relationship data', async () => {
    const adapter = new ParquetStorageAdapter()
    const path = join(dataDir, 'rels.parquet')
    const ts = Date.now()

    const rels: RelLine[] = [
      { $op: 'l', $ts: ts, f: 'user:u1', p: 'posts', r: 'author', t: 'post:p1' },
      { $op: 'l', $ts: ts + 1, f: 'user:u1', p: 'posts', r: 'author', t: 'post:p2' },
      { $op: 'l', $ts: ts + 2, f: 'user:u2', p: 'follows', r: 'followers', t: 'user:u1' },
    ]

    await adapter.writeRels(path, rels)
    await assertParquetFile(path)

    const result = await adapter.readRels(path)
    expect(result).toHaveLength(3)

    // Verify each rel
    for (let i = 0; i < rels.length; i++) {
      expect(result[i].f).toBe(rels[i].f)
      expect(result[i].p).toBe(rels[i].p)
      expect(result[i].r).toBe(rels[i].r)
      expect(result[i].t).toBe(rels[i].t)
      expect(result[i].$op).toBe(rels[i].$op)
      expect(typeof result[i].$ts).toBe('number')
    }
  })

  it('readData returns empty array for nonexistent file', async () => {
    const adapter = new ParquetStorageAdapter()
    const result = await adapter.readData(join(dataDir, 'nonexistent.parquet'))
    expect(result).toEqual([])
  })

  it('readRels returns empty array for nonexistent file', async () => {
    const adapter = new ParquetStorageAdapter()
    const result = await adapter.readRels(join(dataDir, 'nonexistent.parquet'))
    expect(result).toEqual([])
  })

  it('readEvents returns empty array for nonexistent file', async () => {
    const adapter = new ParquetStorageAdapter()
    const result = await adapter.readEvents(join(dataDir, 'nonexistent.parquet'))
    expect(result).toEqual([])
  })
})

// =============================================================================
// Test Suite: Diverse data types survive Parquet roundtrip
// =============================================================================

describe('E2E Parquet Compaction - data type fidelity', () => {
  it('strings, numbers, booleans, arrays, nested objects, and nulls survive engine -> Parquet -> engine roundtrip', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    const entity = await engine1.create('diverse', {
      str: 'hello world',
      num: 42,
      float: 3.14159,
      bool: true,
      boolFalse: false,
      arr: [1, 'two', true, null],
      nested: {
        a: 1,
        b: { c: 'deep', d: [10, 20] },
      },
      nullField: null,
      emptyStr: '',
      zero: 0,
    })

    await engine1.compact('diverse')
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const found = await engine2.get('diverse', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.str).toBe('hello world')
    expect(found!.num).toBe(42)
    expect(found!.float).toBe(3.14159)
    expect(found!.bool).toBe(true)
    expect(found!.boolFalse).toBe(false)
    expect(found!.arr).toEqual([1, 'two', true, null])
    expect(found!.nested).toEqual({
      a: 1,
      b: { c: 'deep', d: [10, 20] },
    })
    expect(found!.nullField).toBeNull()
    expect(found!.emptyStr).toBe('')
    expect(found!.zero).toBe(0)

    await engine2.close()
  })
})

// =============================================================================
// Test Suite: Verify disk layout after compaction
// =============================================================================

describe('E2E Parquet Compaction - disk layout verification', () => {
  it('after compactAll, each table has its own .parquet file and JSONL is empty/fresh', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('posts', { title: 'Hello' })
    await engine.create('comments', { body: 'Nice' })

    await engine.compactAll()
    await engine.close()

    // List all files in data dir
    const files = await readdir(dataDir)

    // Parquet files should exist for each table
    expect(files).toContain('users.parquet')
    expect(files).toContain('posts.parquet')
    expect(files).toContain('comments.parquet')

    // JSONL files should still exist (fresh/empty after rotation)
    // events.jsonl should also exist (engine writes events for each create)
    expect(files).toContain('events.jsonl')

    // No .compacting files should remain (compaction completed cleanly)
    const compactingFiles = files.filter(f => f.endsWith('.compacting'))
    expect(compactingFiles).toHaveLength(0)

    // No .tmp files should remain
    const tmpFiles = files.filter(f => f.endsWith('.tmp'))
    expect(tmpFiles).toHaveLength(0)
  })
})

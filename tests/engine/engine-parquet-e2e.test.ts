/**
 * End-to-End Parquet Compaction Integration Test
 *
 * Validates the full production-critical path:
 *   write entities to JSONL -> compact to real Parquet -> re-open engine -> read from Parquet + buffer merge
 *
 * Unlike engine-local.test.ts which uses a JSON-based StorageAdapter, these tests
 * exercise the real Parquet read/write cycle via ParquetStorageAdapter, ensuring
 * data survives the JSONL -> Parquet -> buffer merge round-trip with correct types
 * and values.
 *
 * Key scenarios tested:
 * - Full lifecycle: create diverse entities, compact to Parquet, re-init, verify find() and get()
 * - Merge-on-read: Parquet + JSONL buffer entities merged correctly within a session
 * - Update after compaction: buffer overlay wins over Parquet data
 * - Delete after compaction: tombstone suppresses Parquet entity
 * - Relationship compaction: rels survive Parquet round-trip
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { ParquetStorageAdapter } from '@/engine/parquet-adapter'
import type { DataLine } from '@/engine/types'

// =============================================================================
// Setup / Teardown
// =============================================================================

let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-parquet-e2e-'))
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
 * Assert that a file at the given path starts and ends with PAR1 magic bytes.
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

// =============================================================================
// Full Lifecycle Test
// =============================================================================

describe('Parquet E2E - full lifecycle', () => {
  it('create diverse entities, compact to Parquet, re-init, verify find() and get()', async () => {
    // Phase 1: Create entities with diverse data types
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

    const charlie = await engine1.create('users', {
      name: 'Charlie',
      age: 35,
      settings: { theme: 'dark', notifications: { email: true, push: false } },
    })

    const diana = await engine1.create('users', {
      name: 'Diana',
      age: 28,
      scores: [95, 88, 72],
      metadata: { joined: '2024-01-15', referral: null },
    })

    const eve = await engine1.create('users', {
      name: 'Eve',
      age: 42,
      bio: 'A string with "quotes" and special\ncharacters',
    })

    // Phase 2: Trigger compaction
    const count = await engine1.compact('users')
    expect(count).toBe(5)

    // Phase 3: Verify real Parquet file exists with PAR1 magic bytes
    const parquetPath = join(dataDir, 'users.parquet')
    await assertParquetFile(parquetPath)

    // Phase 4: Verify the file is NOT valid JSON (it is binary Parquet)
    const rawContent = await readFile(parquetPath, 'utf-8')
    expect(() => JSON.parse(rawContent)).toThrow()

    await engine1.close()

    // Phase 5: Re-initialize engine from same directory
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    // Phase 6: Verify find() returns all 5 entities
    const allUsers = await engine2.find('users')
    expect(allUsers).toHaveLength(5)
    expect(allUsers.map(u => u.name).sort()).toEqual([
      'Alice', 'Bob', 'Charlie', 'Diana', 'Eve',
    ])

    // Phase 7: Verify get() by ID returns correct entity with all fields
    const aliceFound = await engine2.get('users', alice.$id)
    expect(aliceFound).not.toBeNull()
    expect(aliceFound!.$id).toBe(alice.$id)
    expect(aliceFound!.name).toBe('Alice')
    expect(aliceFound!.age).toBe(30)
    expect(aliceFound!.email).toBe('alice@example.com')
    expect(aliceFound!.active).toBe(true)
    expect(aliceFound!.$v).toBe(1)
    expect(aliceFound!.$op).toBe('c')
    expect(typeof aliceFound!.$ts).toBe('number')

    const bobFound = await engine2.get('users', bob.$id)
    expect(bobFound).not.toBeNull()
    expect(bobFound!.name).toBe('Bob')
    expect(bobFound!.tags).toEqual(['developer', 'remote'])

    const charlieFound = await engine2.get('users', charlie.$id)
    expect(charlieFound).not.toBeNull()
    expect(charlieFound!.settings).toEqual({
      theme: 'dark',
      notifications: { email: true, push: false },
    })

    const dianaFound = await engine2.get('users', diana.$id)
    expect(dianaFound).not.toBeNull()
    expect(dianaFound!.scores).toEqual([95, 88, 72])
    expect(dianaFound!.metadata).toEqual({ joined: '2024-01-15', referral: null })

    const eveFound = await engine2.get('users', eve.$id)
    expect(eveFound).not.toBeNull()
    expect(eveFound!.bio).toBe('A string with "quotes" and special\ncharacters')

    await engine2.close()
  })

  it('Parquet file is readable by ParquetStorageAdapter directly', async () => {
    const engine = new ParqueEngine({ dataDir })

    await engine.create('products', { name: 'Widget', price: 9.99, inStock: true })
    await engine.create('products', { name: 'Gadget', price: 29.99, inStock: false })
    await engine.create('products', { name: 'Doohickey', price: 4.50, categories: ['tools', 'misc'] })

    await engine.compact('products')
    await engine.close()

    // Read directly with ParquetStorageAdapter (bypassing engine)
    const adapter = new ParquetStorageAdapter()
    const rows = await adapter.readData(join(dataDir, 'products.parquet'))

    expect(rows).toHaveLength(3)
    expect(rows.every(r => typeof r.$id === 'string')).toBe(true)
    expect(rows.every(r => typeof r.$op === 'string')).toBe(true)
    expect(rows.every(r => typeof r.$v === 'number')).toBe(true)
    expect(rows.every(r => typeof r.$ts === 'number')).toBe(true)

    const widget = rows.find(r => r.name === 'Widget')!
    expect(widget.price).toBe(9.99)
    expect(widget.inStock).toBe(true)

    const doohickey = rows.find(r => r.name === 'Doohickey')!
    expect(doohickey.categories).toEqual(['tools', 'misc'])
  })
})

// =============================================================================
// Merge-on-read Test
// =============================================================================

describe('Parquet E2E - merge-on-read', () => {
  it('find() merges Parquet entities with post-compaction JSONL buffer entities', async () => {
    const engine = new ParqueEngine({ dataDir })

    // Create initial batch and compact to Parquet
    const alice = await engine.create('users', { name: 'Alice', role: 'admin' })
    const bob = await engine.create('users', { name: 'Bob', role: 'user' })
    await engine.compact('users')

    // Verify Parquet file was written
    await assertParquetFile(join(dataDir, 'users.parquet'))

    // Add more entities AFTER compaction (these stay in JSONL buffer)
    const charlie = await engine.create('users', { name: 'Charlie', role: 'moderator' })
    const diana = await engine.create('users', { name: 'Diana', role: 'user' })

    // Verify find() returns all 4 entities (Parquet + buffer merged)
    const allUsers = await engine.find('users')
    expect(allUsers).toHaveLength(4)
    expect(allUsers.map(u => u.name).sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana'])

    // Verify get() works for entities from both sources
    expect(await engine.get('users', alice.$id)).not.toBeNull()
    expect((await engine.get('users', alice.$id))!.name).toBe('Alice')
    expect(await engine.get('users', charlie.$id)).not.toBeNull()
    expect((await engine.get('users', charlie.$id))!.name).toBe('Charlie')

    // Verify filtering works across merged results
    const admins = await engine.find('users', { role: 'admin' })
    expect(admins).toHaveLength(1)
    expect(admins[0].name).toBe('Alice')

    const users = await engine.find('users', { role: 'user' })
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name).sort()).toEqual(['Bob', 'Diana'])

    await engine.close()
  })

  it('re-init merges Parquet file with leftover JSONL correctly', async () => {
    // First session: create, compact, write more, then close WITHOUT second compact
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('items', { title: 'Item A', qty: 10 })
    await engine1.create('items', { title: 'Item B', qty: 20 })
    await engine1.compact('items')

    await engine1.create('items', { title: 'Item C', qty: 30 })
    await engine1.create('items', { title: 'Item D', qty: 40 })
    await engine1.close()

    // Second session: init should merge Parquet (A, B) + JSONL (C, D)
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const items = await engine2.find('items')
    expect(items).toHaveLength(4)
    expect(items.map(i => i.title).sort()).toEqual(['Item A', 'Item B', 'Item C', 'Item D'])

    // Verify data integrity from both sources
    const itemA = items.find(i => i.title === 'Item A')!
    expect(itemA.qty).toBe(10)
    const itemD = items.find(i => i.title === 'Item D')!
    expect(itemD.qty).toBe(40)

    await engine2.close()
  })
})

// =============================================================================
// Update After Compaction Test
// =============================================================================

describe('Parquet E2E - update after compaction', () => {
  it('update within same session: buffer overlay wins over compacted Parquet data', async () => {
    const engine = new ParqueEngine({ dataDir })

    const entity = await engine.create('users', { name: 'Alice', age: 30, score: 100 })
    await engine.compact('users')

    // Update after compaction (stays in JSONL buffer)
    const updated = await engine.update('users', entity.$id, {
      $set: { name: 'Alice Smith', age: 31 },
      $inc: { score: 50 },
    })

    expect(updated.$v).toBe(2)

    // Verify get() returns updated version
    const found = await engine.get('users', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Alice Smith')
    expect(found!.age).toBe(31)
    expect(found!.score).toBe(150)
    expect(found!.$v).toBe(2)

    // Verify find() returns updated version
    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice Smith')
    expect(results[0].$v).toBe(2)

    await engine.close()
  })

  it('update survives second compaction and re-init', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    const entity = await engine1.create('users', { name: 'Bob', level: 1 })
    await engine1.compact('users')

    await engine1.update('users', entity.$id, { $set: { name: 'Bob Senior', level: 5 } })

    // Second compaction should merge the update into Parquet
    const count = await engine1.compact('users')
    expect(count).toBe(1) // One entity (updated version)

    await engine1.close()

    // Re-init and verify
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const found = await engine2.get('users', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Bob Senior')
    expect(found!.level).toBe(5)
    expect(found!.$v).toBe(2)

    await engine2.close()
  })

  it('multiple updates after compaction: only latest version returned', async () => {
    const engine = new ParqueEngine({ dataDir })

    const entity = await engine.create('counters', { name: 'hits', value: 0 })
    await engine.compact('counters')

    // Three successive updates
    await engine.update('counters', entity.$id, { $inc: { value: 10 } })
    await engine.update('counters', entity.$id, { $inc: { value: 20 } })
    await engine.update('counters', entity.$id, { $inc: { value: 30 } })

    const found = await engine.get('counters', entity.$id)
    expect(found).not.toBeNull()
    expect(found!.value).toBe(60) // 0 + 10 + 20 + 30
    expect(found!.$v).toBe(4) // 1 (create) + 3 (updates)

    await engine.close()
  })
})

// =============================================================================
// Delete After Compaction Test
// =============================================================================

describe('Parquet E2E - delete after compaction', () => {
  it('delete within same session: entity no longer returned by find() or get()', async () => {
    const engine = new ParqueEngine({ dataDir })

    const alice = await engine.create('users', { name: 'Alice' })
    const bob = await engine.create('users', { name: 'Bob' })
    await engine.compact('users')

    // Delete Alice after compaction
    await engine.delete('users', alice.$id)

    // get() should return null for deleted entity
    expect(await engine.get('users', alice.$id)).toBeNull()

    // find() should only return Bob
    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Bob')

    // Bob should still be accessible
    const bobFound = await engine.get('users', bob.$id)
    expect(bobFound).not.toBeNull()
    expect(bobFound!.name).toBe('Bob')

    await engine.close()
  })

  it('delete survives second compaction: tombstone removes entity from Parquet', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    const entity = await engine1.create('users', { name: 'ToBeDeleted' })
    await engine1.create('users', { name: 'Survivor' })
    await engine1.compact('users')

    await engine1.delete('users', entity.$id)

    // Second compaction: tombstone merges and entity is excluded
    const count = await engine1.compact('users')
    expect(count).toBe(1) // Only 'Survivor' remains

    await engine1.close()

    // Re-init: verify entity is gone
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    expect(await engine2.get('users', entity.$id)).toBeNull()
    const results = await engine2.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Survivor')

    await engine2.close()
  })

  it('delete then re-init without second compact: JSONL tombstone suppresses Parquet entity', async () => {
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
// Relationship Compaction Test
// =============================================================================

describe('Parquet E2E - relationship compaction', () => {
  it('writeRels + readRels round-trip through ParquetStorageAdapter', async () => {
    const adapter = new ParquetStorageAdapter()
    const relsPath = join(dataDir, 'rels.parquet')

    const rels = [
      { $op: 'l' as const, $ts: Date.now(), f: 'user:u1', p: 'posts', r: 'author', t: 'post:p1' },
      { $op: 'l' as const, $ts: Date.now(), f: 'user:u1', p: 'posts', r: 'author', t: 'post:p2' },
      { $op: 'l' as const, $ts: Date.now(), f: 'user:u2', p: 'follows', r: 'followers', t: 'user:u1' },
    ]

    await adapter.writeRels(relsPath, rels)

    // Verify it is a real Parquet file
    await assertParquetFile(relsPath)

    // Read back and verify
    const result = await adapter.readRels(relsPath)
    expect(result).toHaveLength(3)

    expect(result[0].f).toBe('user:u1')
    expect(result[0].p).toBe('posts')
    expect(result[0].r).toBe('author')
    expect(result[0].t).toBe('post:p1')
    expect(result[0].$op).toBe('l')
    expect(typeof result[0].$ts).toBe('number')

    expect(result[2].f).toBe('user:u2')
    expect(result[2].p).toBe('follows')
    expect(result[2].r).toBe('followers')
    expect(result[2].t).toBe('user:u1')
  })
})

// =============================================================================
// Multi-table Compaction Test
// =============================================================================

describe('Parquet E2E - multi-table compaction', () => {
  it('compactAll produces separate Parquet files per table, all readable after re-init', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    await engine1.create('users', { name: 'Alice', role: 'admin' })
    await engine1.create('users', { name: 'Bob', role: 'user' })
    await engine1.create('posts', { title: 'Hello World', body: 'First post' })
    await engine1.create('comments', { body: 'Great post!', author: 'Bob' })

    await engine1.compactAll()

    // All Parquet files should exist and have correct magic bytes
    await assertParquetFile(join(dataDir, 'users.parquet'))
    await assertParquetFile(join(dataDir, 'posts.parquet'))
    await assertParquetFile(join(dataDir, 'comments.parquet'))

    await engine1.close()

    // Re-init and verify all tables
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const users = await engine2.find('users')
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob'])

    const posts = await engine2.find('posts')
    expect(posts).toHaveLength(1)
    expect(posts[0].title).toBe('Hello World')
    expect(posts[0].body).toBe('First post')

    const comments = await engine2.find('comments')
    expect(comments).toHaveLength(1)
    expect(comments[0].body).toBe('Great post!')
    expect(comments[0].author).toBe('Bob')

    await engine2.close()
  })
})

// =============================================================================
// Double Compaction Cycle Test
// =============================================================================

describe('Parquet E2E - double compaction cycle', () => {
  it('write -> compact -> write -> compact -> re-init: all data intact through two Parquet cycles', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    // First batch + compact
    await engine1.create('logs', { message: 'Log 1', level: 'info' })
    await engine1.create('logs', { message: 'Log 2', level: 'warn' })
    const firstCount = await engine1.compact('logs')
    expect(firstCount).toBe(2)

    // Second batch + compact (merge into existing Parquet)
    await engine1.create('logs', { message: 'Log 3', level: 'error' })
    await engine1.create('logs', { message: 'Log 4', level: 'debug' })
    const secondCount = await engine1.compact('logs')
    expect(secondCount).toBe(4) // All 4 merged

    await engine1.close()

    // Re-init and verify
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const logs = await engine2.find('logs')
    expect(logs).toHaveLength(4)
    expect(logs.map(l => l.message).sort()).toEqual(['Log 1', 'Log 2', 'Log 3', 'Log 4'])

    // Verify individual fields
    const errorLog = logs.find(l => l.level === 'error')!
    expect(errorLog.message).toBe('Log 3')

    await engine2.close()
  })
})

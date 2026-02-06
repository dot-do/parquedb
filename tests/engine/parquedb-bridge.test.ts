/**
 * Wave 6.3: ParqueDB Bridge Adapter Tests
 *
 * Tests EngineDB — a MongoDB-style wrapper around ParqueEngine that provides:
 * - Proxy-based collection access (db.Users -> db.collection('users'))
 * - EngineCollection CRUD: create, find, findOne, get, update, delete, count
 * - Lifecycle: init(), compact(), close()
 * - Data persistence across close + re-init cycles
 */

import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { EngineDB, EngineCollection } from '@/engine/parquedb-adapter'
import { ParqueEngine } from '@/engine/engine'

let db: EngineDB
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-bridge-test-'))
  db = new EngineDB({ dataDir })
})

afterEach(async () => {
  await db.close()
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// EngineDB Basics
// =============================================================================

describe('EngineDB basics', () => {
  it('constructor creates EngineDB with ParqueEngine', () => {
    expect(db).toBeDefined()
    expect(db.engine).toBeInstanceOf(ParqueEngine)
  })

  it('init() initializes without error', async () => {
    await expect(db.init()).resolves.toBeUndefined()
  })

  it('close() closes without error', async () => {
    await expect(db.close()).resolves.toBeUndefined()
  })

  it('tables returns empty array initially', () => {
    expect(db.tables).toEqual([])
  })
})

// =============================================================================
// Collection CRUD
// =============================================================================

describe('Collection CRUD', () => {
  it('collection("users").create() creates entity with $id', async () => {
    const entity = await db.collection('users').create({ name: 'Alice' })

    expect(entity).toBeDefined()
    expect(entity.$id).toBeDefined()
    expect(typeof entity.$id).toBe('string')
    expect(entity.$id.length).toBeGreaterThan(0)
    expect(entity.name).toBe('Alice')
    expect(entity.$op).toBe('c')
    expect(entity.$v).toBe(1)
  })

  it('collection("users").find() returns created entities', async () => {
    const coll = db.collection('users')
    await coll.create({ name: 'Alice' })
    await coll.create({ name: 'Bob' })

    const results = await coll.find()
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name)).toContain('Alice')
    expect(results.map(r => r.name)).toContain('Bob')
  })

  it('collection("users").findOne() returns first match', async () => {
    const coll = db.collection('users')
    await coll.create({ name: 'Alice', role: 'admin' })
    await coll.create({ name: 'Bob', role: 'user' })

    const result = await coll.findOne({ role: 'admin' })
    expect(result).not.toBeNull()
    expect(result!.name).toBe('Alice')
  })

  it('collection("users").get(id) returns entity by ID', async () => {
    const coll = db.collection('users')
    const created = await coll.create({ name: 'Alice' })

    const fetched = await coll.get(created.$id)
    expect(fetched).not.toBeNull()
    expect(fetched!.$id).toBe(created.$id)
    expect(fetched!.name).toBe('Alice')
  })

  it('collection("users").get(id) returns null for missing', async () => {
    const coll = db.collection('users')

    const result = await coll.get('nonexistent-id')
    expect(result).toBeNull()
  })

  it('collection("users").update(id, { $set }) updates entity', async () => {
    const coll = db.collection('users')
    const created = await coll.create({ name: 'Alice' })

    const updated = await coll.update(created.$id, { $set: { name: 'Alice Smith' } })
    expect(updated.$id).toBe(created.$id)
    expect(updated.name).toBe('Alice Smith')
    expect(updated.$v).toBe(2)
    expect(updated.$op).toBe('u')
  })

  it('collection("users").delete(id) removes entity', async () => {
    const coll = db.collection('users')
    const created = await coll.create({ name: 'Alice' })

    await coll.delete(created.$id)

    const result = await coll.get(created.$id)
    expect(result).toBeNull()
  })

  it('collection("users").count() returns correct count', async () => {
    const coll = db.collection('users')

    expect(await coll.count()).toBe(0)

    await coll.create({ name: 'Alice' })
    await coll.create({ name: 'Bob' })

    expect(await coll.count()).toBe(2)
  })

  it('collection("users").createMany() batch creates', async () => {
    const coll = db.collection('users')
    const results = await coll.createMany([
      { name: 'Alice' },
      { name: 'Bob' },
      { name: 'Charlie' },
    ])

    expect(results).toHaveLength(3)
    expect(results[0].name).toBe('Alice')
    expect(results[1].name).toBe('Bob')
    expect(results[2].name).toBe('Charlie')

    // Each has a unique $id
    const ids = new Set(results.map(r => r.$id))
    expect(ids.size).toBe(3)

    // All are in the buffer
    const all = await coll.find()
    expect(all).toHaveLength(3)
  })

  it('collection("users").getMany() returns multiple', async () => {
    const coll = db.collection('users')
    const a = await coll.create({ name: 'Alice' })
    const b = await coll.create({ name: 'Bob' })

    const results = await coll.getMany([a.$id, 'nonexistent', b.$id])
    expect(results).toHaveLength(3)
    expect(results[0]).not.toBeNull()
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]).toBeNull()
    expect(results[2]).not.toBeNull()
    expect(results[2]!.name).toBe('Bob')
  })
})

// =============================================================================
// Proxy Access
// =============================================================================

describe('Proxy access', () => {
  it('db.Users returns a collection (create works through proxy)', async () => {
    const entity = await (db as any).Users.create({ name: 'Alice' })

    expect(entity).toBeDefined()
    expect(entity.$id).toBeDefined()
    expect(entity.name).toBe('Alice')

    // Verify it went into the 'users' table (lowercase)
    const found = await db.collection('users').find()
    expect(found).toHaveLength(1)
    expect(found[0].name).toBe('Alice')
  })

  it('db.Posts returns a different collection', async () => {
    await (db as any).Users.create({ name: 'Alice' })
    await (db as any).Posts.create({ title: 'Hello' })

    const users = await db.collection('users').find()
    const posts = await db.collection('posts').find()

    expect(users).toHaveLength(1)
    expect(users[0].name).toBe('Alice')
    expect(posts).toHaveLength(1)
    expect(posts[0].title).toBe('Hello')
  })

  it('multiple collections work independently', async () => {
    await (db as any).Users.create({ name: 'Alice' })
    await (db as any).Users.create({ name: 'Bob' })
    await (db as any).Posts.create({ title: 'Post 1' })
    await (db as any).Comments.create({ text: 'Nice!' })

    expect(await db.collection('users').count()).toBe(2)
    expect(await db.collection('posts').count()).toBe(1)
    expect(await db.collection('comments').count()).toBe(1)
  })
})

// =============================================================================
// Lifecycle
// =============================================================================

describe('Lifecycle', () => {
  it('data persists after close + re-init', async () => {
    // Create data
    await db.collection('users').create({ name: 'Alice' })
    await db.collection('users').create({ name: 'Bob' })
    await db.close()

    // Re-open with a new EngineDB instance pointing to the same directory
    const db2 = new EngineDB({ dataDir })
    await db2.init()

    const users = await db2.collection('users').find()
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name)).toContain('Alice')
    expect(users.map(u => u.name)).toContain('Bob')

    await db2.close()
  })

  it('compact() runs without error', async () => {
    await db.collection('users').create({ name: 'Alice' })
    await expect(db.compact()).resolves.toBeUndefined()
  })
})

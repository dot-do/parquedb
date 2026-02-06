import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { TableBuffer } from '@/engine/buffer'
import { matchesFilter } from '@/engine/filter'
import type { DataLine } from '@/engine/types'

/**
 * DataLine Redesign Test Suite (zou5.14)
 *
 * Verifies that DataLine supports explicit $data field for user data,
 * eliminating the need for unsafe double-casts in engine internals.
 *
 * Key design decisions:
 * - $data: Record<string, unknown> holds all user fields
 * - System fields ($id, $op, $v, $ts) remain at top level
 * - For backward compatibility, user fields are also accessible flat on the object
 * - Engine internals use entity.$data instead of (entity as unknown as Record<string,unknown>)
 */

// =============================================================================
// 1. DataLine type shape
// =============================================================================

describe('DataLine $data field', () => {
  it('holds system fields at top level plus user data in $data', () => {
    const line: DataLine = {
      $id: 'test-1',
      $op: 'c',
      $v: 1,
      $ts: Date.now(),
      $data: { name: 'Alice', age: 30 },
    }

    expect(line.$id).toBe('test-1')
    expect(line.$op).toBe('c')
    expect(line.$v).toBe(1)
    expect(line.$data).toEqual({ name: 'Alice', age: 30 })
    expect(line.$data.name).toBe('Alice')
    expect(line.$data.age).toBe(30)
  })

  it('defaults $data to empty object for tombstone (delete)', () => {
    const tombstone: DataLine = {
      $id: 'test-1',
      $op: 'd',
      $v: 2,
      $ts: Date.now(),
      $data: {},
    }

    expect(tombstone.$data).toEqual({})
  })

  it('$data can hold nested objects', () => {
    const line: DataLine = {
      $id: 'test-1',
      $op: 'c',
      $v: 1,
      $ts: Date.now(),
      $data: {
        name: 'Alice',
        address: { city: 'NYC', zip: '10001' },
      },
    }

    expect(line.$data.address).toEqual({ city: 'NYC', zip: '10001' })
  })
})

// =============================================================================
// 2. Engine create operations populate $data
// =============================================================================

describe('ParqueEngine create populates $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('create returns entity with $data containing user fields', async () => {
    const result = await engine.create('users', { name: 'Alice', age: 30 })

    expect(result.$id).toBeDefined()
    expect(result.$op).toBe('c')
    expect(result.$v).toBe(1)
    expect(result.$data).toBeDefined()
    expect(result.$data.name).toBe('Alice')
    expect(result.$data.age).toBe(30)
  })

  it('create does not put system fields in $data', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    expect(result.$data).toBeDefined()
    expect(result.$data.$id).toBeUndefined()
    expect(result.$data.$op).toBeUndefined()
    expect(result.$data.$v).toBeUndefined()
    expect(result.$data.$ts).toBeUndefined()
  })

  it('createMany returns entities with $data', async () => {
    const results = await engine.createMany('users', [
      { name: 'Alice' },
      { name: 'Bob' },
    ])

    expect(results[0].$data.name).toBe('Alice')
    expect(results[1].$data.name).toBe('Bob')
  })

  it('backward compat: user fields also accessible flat on entity', async () => {
    const result = await engine.create('users', { name: 'Alice', age: 30 })

    // Flat access still works for backward compatibility
    expect(result.name).toBe('Alice')
    expect(result.age).toBe(30)
  })
})

// =============================================================================
// 3. Engine update operations use $data
// =============================================================================

describe('ParqueEngine update uses $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('$set updates fields in $data', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })
    const updated = await engine.update('users', created.$id, {
      $set: { name: 'Alice Smith' },
    })

    expect(updated.$data.name).toBe('Alice Smith')
    expect(updated.$data.age).toBe(30) // Unchanged field preserved
  })

  it('$inc updates numeric fields in $data', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })
    const updated = await engine.update('users', created.$id, {
      $inc: { age: 1 },
    })

    expect(updated.$data.age).toBe(31)
  })

  it('$unset removes fields from $data', async () => {
    const created = await engine.create('users', { name: 'Alice', email: 'a@b.com' })
    const updated = await engine.update('users', created.$id, {
      $unset: { email: true },
    })

    expect(updated.$data.name).toBe('Alice')
    expect(updated.$data.email).toBeUndefined()
    expect('email' in updated.$data).toBe(false)
  })
})

// =============================================================================
// 4. Filter evaluation looks in $data
// =============================================================================

describe('Filter evaluation with $data', () => {
  it('matchesFilter resolves user fields from $data', () => {
    const entity: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: Date.now(),
      $data: { name: 'Alice', age: 30 },
    }

    expect(matchesFilter(entity, { name: 'Alice' })).toBe(true)
    expect(matchesFilter(entity, { age: { $gt: 25 } })).toBe(true)
    expect(matchesFilter(entity, { name: 'Bob' })).toBe(false)
  })

  it('matchesFilter resolves dot-notation paths in $data', () => {
    const entity: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: Date.now(),
      $data: { address: { city: 'NYC' } },
    }

    expect(matchesFilter(entity, { 'address.city': 'NYC' })).toBe(true)
    expect(matchesFilter(entity, { 'address.city': 'LA' })).toBe(false)
  })

  it('matchesFilter still matches system fields directly', () => {
    const entity: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      $data: { name: 'Alice' },
    }

    expect(matchesFilter(entity, { $id: 'u1' })).toBe(true)
    expect(matchesFilter(entity, { $op: 'c' })).toBe(true)
    expect(matchesFilter(entity, { $v: 1 })).toBe(true)
  })
})

// =============================================================================
// 5. Buffer scan with $data entities
// =============================================================================

describe('TableBuffer scan with $data entities', () => {
  let buffer: TableBuffer

  beforeEach(() => {
    buffer = new TableBuffer()
  })

  it('scan filters entities using $data fields', () => {
    buffer.set({
      $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(),
      $data: { name: 'Alice', role: 'admin' },
      name: 'Alice', role: 'admin',
    })
    buffer.set({
      $id: 'u2', $op: 'c', $v: 1, $ts: Date.now(),
      $data: { name: 'Bob', role: 'user' },
      name: 'Bob', role: 'user',
    })

    const results = buffer.scan({ role: 'admin' })
    expect(results).toHaveLength(1)
    expect(results[0].$id).toBe('u1')
  })

  it('count works with $data entities', () => {
    buffer.set({
      $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(),
      $data: { name: 'Alice', role: 'admin' },
      name: 'Alice', role: 'admin',
    })
    buffer.set({
      $id: 'u2', $op: 'c', $v: 1, $ts: Date.now(),
      $data: { name: 'Bob', role: 'user' },
      name: 'Bob', role: 'user',
    })

    expect(buffer.count({ role: 'admin' })).toBe(1)
    expect(buffer.count()).toBe(2)
  })
})

// =============================================================================
// 6. Engine find/sort uses $data (no double-casts)
// =============================================================================

describe('ParqueEngine find/sort with $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
    await engine.create('users', { $id: 'u1', name: 'Alice', age: 30 })
    await engine.create('users', { $id: 'u2', name: 'Bob', age: 25 })
    await engine.create('users', { $id: 'u3', name: 'Charlie', age: 35 })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('find filters using $data fields', async () => {
    const results = await engine.find('users', { name: 'Alice' })
    expect(results).toHaveLength(1)
    expect(results[0].$data.name).toBe('Alice')
  })

  it('find sorts by $data fields', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 } })
    expect(results).toHaveLength(3)
    expect(results[0].$data.name).toBe('Bob')    // age 25
    expect(results[1].$data.name).toBe('Alice')  // age 30
    expect(results[2].$data.name).toBe('Charlie') // age 35
  })

  it('findOne returns entity with $data', async () => {
    const result = await engine.findOne('users', { name: 'Bob' })
    expect(result).not.toBeNull()
    expect(result!.$data.name).toBe('Bob')
  })

  it('count works with $data-based filter', async () => {
    const count = await engine.count('users', { age: { $gt: 28 } })
    expect(count).toBe(2)
  })
})

// =============================================================================
// 7. Event extraction uses $data
// =============================================================================

describe('Event extraction uses $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('create event after contains data from $data', async () => {
    await engine.create('users', { name: 'Alice', age: 30 })

    // Read the events.jsonl to verify event content
    const content = await readFile(join(dataDir, 'events.jsonl'), 'utf-8')
    const events = content.split('\n').filter(l => l.trim()).map(l => JSON.parse(l))

    expect(events[0].after).toEqual({ name: 'Alice', age: 30 })
  })

  it('update event has before and after from $data', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.update('users', created.$id, { $set: { name: 'Alice Smith' } })

    const content = await readFile(join(dataDir, 'events.jsonl'), 'utf-8')
    const events = content.split('\n').filter(l => l.trim()).map(l => JSON.parse(l))

    expect(events[1].before).toEqual({ name: 'Alice' })
    expect(events[1].after).toEqual({ name: 'Alice Smith' })
  })
})

// =============================================================================
// 8. JSONL serialization includes $data
// =============================================================================

describe('JSONL serialization with $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('JSONL DataLine includes $data field', async () => {
    await engine.create('users', { name: 'Alice', age: 30 })

    const content = await readFile(join(dataDir, 'users.jsonl'), 'utf-8')
    const line = JSON.parse(content.trim())

    expect(line.$data).toBeDefined()
    expect(line.$data.name).toBe('Alice')
    expect(line.$data.age).toBe(30)
  })

  it('JSONL tombstone has empty $data', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    const content = await readFile(join(dataDir, 'users.jsonl'), 'utf-8')
    const lines = content.split('\n').filter(l => l.trim()).map(l => JSON.parse(l))
    const tombstone = lines[1]

    expect(tombstone.$op).toBe('d')
    // Tombstone has empty $data (no user data) and no flat user fields
    expect(tombstone.$data).toEqual({})
    expect(tombstone.name).toBeUndefined()
  })
})

// =============================================================================
// 9. Parquet round-trip with $data
// =============================================================================

describe('Parquet round-trip preserves $data', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-dataline-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('compact and reload preserves $data fields', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', age: 30 })
    await engine.create('users', { $id: 'u2', name: 'Bob', age: 25 })

    // Compact to Parquet
    await engine.compact('users')

    // Create new engine and init (reload from Parquet)
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const alice = await engine2.get('users', 'u1')
    expect(alice).not.toBeNull()
    expect(alice!.$data).toBeDefined()
    expect(alice!.$data.name).toBe('Alice')
    expect(alice!.$data.age).toBe(30)

    const bob = await engine2.get('users', 'u2')
    expect(bob).not.toBeNull()
    expect(bob!.$data).toBeDefined()
    expect(bob!.$data.name).toBe('Bob')

    await engine2.close()
  })
})

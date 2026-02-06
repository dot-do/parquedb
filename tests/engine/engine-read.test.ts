import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'

/**
 * ParqueEngine Read Path Test Suite
 *
 * Tests the buffer scan & filter operations: find, findOne, count.
 * Each test uses a fresh temp directory and a new ParqueEngine instance
 * seeded with consistent test data.
 *
 * For this wave (Wave 3.1), reads only come from the in-memory buffer.
 * Parquet merge will be added in Wave 3.2.
 */

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-read-'))
  engine = new ParqueEngine({ dataDir })

  // Seed test data
  await engine.create('users', { $id: 'u1', name: 'Alice', age: 30, role: 'admin' })
  await engine.create('users', { $id: 'u2', name: 'Bob', age: 25, role: 'user' })
  await engine.create('users', { $id: 'u3', name: 'Charlie', age: 35, role: 'admin' })
  await engine.create('users', { $id: 'u4', name: 'Diana', age: 28, role: 'user' })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// find() basics
// =============================================================================

describe('ParqueEngine - find() basics', () => {
  it('returns all entities when no filter is provided', async () => {
    const results = await engine.find('users')

    expect(results).toHaveLength(4)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Bob')
    expect(names).toContain('Charlie')
    expect(names).toContain('Diana')
  })

  it('filters by simple field equality', async () => {
    const results = await engine.find('users', { role: 'admin' })

    expect(results).toHaveLength(2)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Charlie')
  })

  it('filters with $gt operator', async () => {
    const results = await engine.find('users', { age: { $gt: 28 } })

    expect(results).toHaveLength(2)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Charlie')
  })

  it('filters with $gte operator', async () => {
    const results = await engine.find('users', { age: { $gte: 28 } })

    expect(results).toHaveLength(3)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Charlie')
    expect(names).toContain('Diana')
  })

  it('filters with $in operator', async () => {
    const results = await engine.find('users', { name: { $in: ['Alice', 'Bob'] } })

    expect(results).toHaveLength(2)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Bob')
  })

  it('filters with $exists: false', async () => {
    const results = await engine.find('users', { email: { $exists: false } })

    expect(results).toHaveLength(4)
  })

  it('returns empty array for non-existent table', async () => {
    const results = await engine.find('unknown')

    expect(results).toEqual([])
  })
})

// =============================================================================
// find() with options
// =============================================================================

describe('ParqueEngine - find() with options', () => {
  it('limits results with limit option', async () => {
    const results = await engine.find('users', {}, { limit: 2 })

    expect(results).toHaveLength(2)
  })

  it('skips results with skip option', async () => {
    const results = await engine.find('users', {}, { skip: 2 })

    expect(results).toHaveLength(2)
  })

  it('combines skip and limit', async () => {
    const all = await engine.find('users')
    const results = await engine.find('users', {}, { skip: 1, limit: 2 })

    expect(results).toHaveLength(2)
    // Should return the 2nd and 3rd entity from the full list
    expect(results[0].$id).toBe(all[1].$id)
    expect(results[1].$id).toBe(all[2].$id)
  })

  it('sorts ascending by a numeric field', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 } })

    expect(results).toHaveLength(4)
    expect(results[0].name).toBe('Bob')    // age 25
    expect(results[1].name).toBe('Diana')  // age 28
    expect(results[2].name).toBe('Alice')  // age 30
    expect(results[3].name).toBe('Charlie') // age 35
  })

  it('sorts descending by a numeric field', async () => {
    const results = await engine.find('users', {}, { sort: { age: -1 } })

    expect(results).toHaveLength(4)
    expect(results[0].name).toBe('Charlie') // age 35
    expect(results[1].name).toBe('Alice')  // age 30
    expect(results[2].name).toBe('Diana')  // age 28
    expect(results[3].name).toBe('Bob')    // age 25
  })

  it('combines filter and sort', async () => {
    const results = await engine.find('users', { role: 'admin' }, { sort: { age: 1 } })

    expect(results).toHaveLength(2)
    expect(results[0].name).toBe('Alice')   // age 30
    expect(results[1].name).toBe('Charlie') // age 35
  })
})

// =============================================================================
// find() excludes tombstones
// =============================================================================

describe('ParqueEngine - find() excludes tombstones', () => {
  it('excludes deleted entities from results', async () => {
    await engine.delete('users', 'u2')

    const results = await engine.find('users')

    expect(results).toHaveLength(3)
    const names = results.map(r => r.name)
    expect(names).not.toContain('Bob')
  })

  it('filter matching deleted entity returns empty', async () => {
    await engine.delete('users', 'u2')

    const results = await engine.find('users', { name: 'Bob' })

    expect(results).toEqual([])
  })
})

// =============================================================================
// findOne()
// =============================================================================

describe('ParqueEngine - findOne()', () => {
  it('returns the matching entity', async () => {
    const result = await engine.findOne('users', { name: 'Alice' })

    expect(result).not.toBeNull()
    expect(result!.$id).toBe('u1')
    expect(result!.name).toBe('Alice')
  })

  it('returns first matching entity when multiple match', async () => {
    const result = await engine.findOne('users', { role: 'admin' })

    expect(result).not.toBeNull()
    // Should return either Alice or Charlie (whichever comes first in buffer)
    expect(['Alice', 'Charlie']).toContain(result!.name)
  })

  it('returns null when no entity matches', async () => {
    const result = await engine.findOne('users', { name: 'Nobody' })

    expect(result).toBeNull()
  })

  it('returns null for non-existent table', async () => {
    const result = await engine.findOne('unknown', {})

    expect(result).toBeNull()
  })
})

// =============================================================================
// count()
// =============================================================================

describe('ParqueEngine - count()', () => {
  it('counts all entities when no filter is provided', async () => {
    const count = await engine.count('users')

    expect(count).toBe(4)
  })

  it('counts entities matching a filter', async () => {
    const count = await engine.count('users', { role: 'admin' })

    expect(count).toBe(2)
  })

  it('counts entities matching comparison operators', async () => {
    const count = await engine.count('users', { age: { $gt: 30 } })

    expect(count).toBe(1)
  })

  it('decrements after delete', async () => {
    await engine.delete('users', 'u2')

    const count = await engine.count('users')

    expect(count).toBe(3)
  })

  it('returns 0 for non-existent table', async () => {
    const count = await engine.count('unknown')

    expect(count).toBe(0)
  })
})

// =============================================================================
// Logical operators ($or, $and)
// =============================================================================

describe('ParqueEngine - logical operators', () => {
  it('supports $or to match any condition', async () => {
    const results = await engine.find('users', {
      $or: [{ name: 'Alice' }, { name: 'Bob' }],
    })

    expect(results).toHaveLength(2)
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Bob')
  })

  it('supports $and to require all conditions', async () => {
    const results = await engine.find('users', {
      $and: [{ role: 'admin' }, { age: { $gt: 32 } }],
    })

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Charlie')
  })
})

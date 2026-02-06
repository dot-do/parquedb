import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import type { DataLine } from '@/engine/types'

/**
 * ParqueEngine Get By ID Test Suite
 *
 * Tests the O(1) fast path for retrieving entities by ID.
 * Currently reads from the in-memory TableBuffer only.
 * Async signature allows Parquet fallback to be added later.
 *
 * Methods under test:
 * - get(table, id)       → DataLine | null
 * - getMany(table, ids)  → (DataLine | null)[]
 */

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-get-'))
  engine = new ParqueEngine({ dataDir })

  await engine.create('users', { $id: 'u1', name: 'Alice', age: 30 })
  await engine.create('users', { $id: 'u2', name: 'Bob', age: 25 })
  await engine.create('users', { $id: 'u3', name: 'Charlie', age: 35 })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// get()
// =============================================================================

describe('ParqueEngine - get', () => {
  it('returns entity by ID', async () => {
    const result = await engine.get('users', 'u1')

    expect(result).not.toBeNull()
    expect(result!.name).toBe('Alice')
  })

  it('returns entity with correct system and data fields', async () => {
    const result = await engine.get('users', 'u1')

    expect(result).not.toBeNull()
    expect(result!.$id).toBe('u1')
    expect(result!.$v).toBe(1)
    expect(result!.$ts).toBeDefined()
    expect(typeof result!.$ts).toBe('number')
    expect(result!.name).toBe('Alice')
    expect(result!.age).toBe(30)
  })

  it('returns null for nonexistent ID', async () => {
    const result = await engine.get('users', 'nonexistent')

    expect(result).toBeNull()
  })

  it('returns null for nonexistent table', async () => {
    const result = await engine.get('unknown-table', 'u1')

    expect(result).toBeNull()
  })

  it('returns null for deleted entity (tombstone)', async () => {
    await engine.delete('users', 'u2')

    const result = await engine.get('users', 'u2')

    expect(result).toBeNull()
  })

  it('returns updated entity with incremented $v after update', async () => {
    await engine.update('users', 'u1', { $set: { name: 'Alice Updated' } })

    const result = await engine.get('users', 'u1')

    expect(result).not.toBeNull()
    expect(result!.name).toBe('Alice Updated')
    expect(result!.$v).toBe(2)
  })

  it('returns re-created entity after delete and re-create with same $id', async () => {
    await engine.delete('users', 'u2')

    // Verify it's gone
    expect(await engine.get('users', 'u2')).toBeNull()

    // Re-create with same ID
    await engine.create('users', { $id: 'u2', name: 'Bob Reborn', age: 99 })

    const result = await engine.get('users', 'u2')

    expect(result).not.toBeNull()
    expect(result!.$id).toBe('u2')
    expect(result!.name).toBe('Bob Reborn')
    expect(result!.age).toBe(99)
  })
})

// =============================================================================
// getMany()
// =============================================================================

describe('ParqueEngine - getMany', () => {
  it('returns all requested entities', async () => {
    const results = await engine.getMany('users', ['u1', 'u2', 'u3'])

    expect(results).toHaveLength(3)
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]!.name).toBe('Bob')
    expect(results[2]!.name).toBe('Charlie')
  })

  it('returns subset of entities in requested order', async () => {
    const results = await engine.getMany('users', ['u1', 'u3'])

    expect(results).toHaveLength(2)
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]!.name).toBe('Charlie')
  })

  it('returns null for missing IDs, preserving order', async () => {
    const results = await engine.getMany('users', ['u1', 'nonexistent', 'u3'])

    expect(results).toHaveLength(3)
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]).toBeNull()
    expect(results[2]!.name).toBe('Charlie')
  })

  it('returns empty array for empty ID list', async () => {
    const results = await engine.getMany('users', [])

    expect(results).toEqual([])
  })

  it('returns array of nulls for nonexistent table', async () => {
    const results = await engine.getMany('unknown', ['u1'])

    expect(results).toHaveLength(1)
    expect(results[0]).toBeNull()
  })

  it('returns null for deleted entities in multi-get', async () => {
    await engine.delete('users', 'u2')

    const results = await engine.getMany('users', ['u1', 'u2', 'u3'])

    expect(results).toHaveLength(3)
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]).toBeNull()
    expect(results[2]!.name).toBe('Charlie')
  })

  it('preserves order of requested IDs', async () => {
    const results = await engine.getMany('users', ['u3', 'u1', 'u2'])

    expect(results).toHaveLength(3)
    expect(results[0]!.name).toBe('Charlie')
    expect(results[1]!.name).toBe('Alice')
    expect(results[2]!.name).toBe('Bob')
  })
})

// =============================================================================
// Performance
// =============================================================================

describe('ParqueEngine - get performance', () => {
  it('get() on large buffer completes in < 5ms', async () => {
    // Create 1000 entities
    const items: Record<string, unknown>[] = []
    for (let i = 0; i < 1000; i++) {
      items.push({ $id: `perf-${i}`, name: `User ${i}`, score: i })
    }
    await engine.createMany('perf', items)

    // Warm up
    await engine.get('perf', 'perf-500')

    // Measure
    const start = performance.now()
    const result = await engine.get('perf', 'perf-500')
    const elapsed = performance.now() - start

    expect(result).not.toBeNull()
    expect(result!.name).toBe('User 500')
    expect(elapsed).toBeLessThan(5)
  })
})

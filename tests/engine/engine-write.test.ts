import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import type { DataLine, EventLine } from '@/engine/types'

/**
 * ParqueEngine Write Path Test Suite
 *
 * Tests the core entity write operations: create, createMany, update, delete.
 * Each test uses a fresh temp directory and a new ParqueEngine instance.
 *
 * The engine coordinates writes across:
 * - {dataDir}/{table}.jsonl   (DataLine records)
 * - {dataDir}/events.jsonl    (EventLine CDC/audit records)
 * - In-memory TableBuffer     (for fast reads after writes)
 */

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-test-'))
  engine = new ParqueEngine({ dataDir })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// Helper: parse JSONL file into array of objects
// =============================================================================

async function readJsonl<T>(filePath: string): Promise<T[]> {
  const content = await readFile(filePath, 'utf-8')
  return content
    .split('\n')
    .filter(line => line.trim() !== '')
    .map(line => JSON.parse(line) as T)
}

// =============================================================================
// Create Operations
// =============================================================================

describe('ParqueEngine - create', () => {
  it('returns entity with $id, $v=1, $op="c", $ts', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    expect(result.$id).toBeDefined()
    expect(typeof result.$id).toBe('string')
    expect(result.$id.length).toBeGreaterThan(0)
    expect(result.$op).toBe('c')
    expect(result.$v).toBe(1)
    expect(result.$ts).toBeDefined()
    expect(typeof result.$ts).toBe('number')
    expect(result.name).toBe('Alice')
  })

  it('auto-generates $id if not provided', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    expect(result.$id).toBeDefined()
    expect(typeof result.$id).toBe('string')
    expect(result.$id.length).toBeGreaterThan(0)
  })

  it('uses provided $id if given', async () => {
    const result = await engine.create('users', { $id: 'custom-id', name: 'Bob' })

    expect(result.$id).toBe('custom-id')
    expect(result.name).toBe('Bob')
  })

  it('appends DataLine to {dataDir}/users.jsonl', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    const lines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(lines).toHaveLength(1)
    expect(lines[0].$id).toBe(result.$id)
    expect(lines[0].$op).toBe('c')
    expect(lines[0].$v).toBe(1)
    expect(lines[0].name).toBe('Alice')
  })

  it('appends EventLine to {dataDir}/events.jsonl with op="c"', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(1)
    expect(events[0].op).toBe('c')
    expect(events[0].ns).toBe('users')
    expect(events[0].eid).toBe(result.$id)
    expect(events[0].after).toEqual({ name: 'Alice' })
    expect(events[0].before).toBeUndefined()
  })

  it('updates in-memory buffer', async () => {
    const result = await engine.create('users', { name: 'Alice' })

    const buffered = engine.getBuffer('users').get(result.$id)
    expect(buffered).toBeDefined()
    expect(buffered!.$id).toBe(result.$id)
    expect(buffered!.name).toBe('Alice')
    expect(buffered!.$op).toBe('c')
    expect(buffered!.$v).toBe(1)
  })

  it('sets $ts to approximately Date.now()', async () => {
    const before = Date.now()
    const result = await engine.create('users', { name: 'Alice' })
    const after = Date.now()

    expect(result.$ts).toBeGreaterThanOrEqual(before)
    expect(result.$ts).toBeLessThanOrEqual(after + 100)
  })
})

// =============================================================================
// createMany Operations
// =============================================================================

describe('ParqueEngine - createMany', () => {
  it('creates multiple entities in one batch', async () => {
    const results = await engine.createMany('users', [
      { name: 'A' },
      { name: 'B' },
      { name: 'C' },
    ])

    expect(results).toHaveLength(3)
    expect(results[0].name).toBe('A')
    expect(results[1].name).toBe('B')
    expect(results[2].name).toBe('C')

    // Each should have unique $id
    const ids = results.map(r => r.$id)
    expect(new Set(ids).size).toBe(3)

    // All should be version 1, op 'c'
    for (const r of results) {
      expect(r.$op).toBe('c')
      expect(r.$v).toBe(1)
    }
  })

  it('writes all DataLines to JSONL file', async () => {
    await engine.createMany('users', [
      { name: 'A' },
      { name: 'B' },
      { name: 'C' },
    ])

    const lines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(lines).toHaveLength(3)
    expect(lines.map(l => l.name)).toEqual(['A', 'B', 'C'])
  })

  it('writes all EventLines to events.jsonl', async () => {
    await engine.createMany('users', [
      { name: 'A' },
      { name: 'B' },
      { name: 'C' },
    ])

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(3)
    for (const event of events) {
      expect(event.op).toBe('c')
      expect(event.ns).toBe('users')
    }
  })
})

// =============================================================================
// Update Operations
// =============================================================================

describe('ParqueEngine - update', () => {
  it('returns updated entity with $v=2 after $set', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    const updated = await engine.update('users', created.$id, {
      $set: { name: 'Alice Smith' },
    })

    expect(updated.$id).toBe(created.$id)
    expect(updated.$v).toBe(2)
    expect(updated.$op).toBe('u')
    expect(updated.name).toBe('Alice Smith')
  })

  it('appends DataLine with op="u" and full entity state', async () => {
    const created = await engine.create('users', { name: 'Alice', email: 'alice@a.co' })
    await engine.update('users', created.$id, {
      $set: { name: 'Alice Smith' },
    })

    const lines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(lines).toHaveLength(2)

    const updateLine = lines[1]
    expect(updateLine.$op).toBe('u')
    expect(updateLine.$v).toBe(2)
    expect(updateLine.name).toBe('Alice Smith')
    // Full entity state: email should still be present
    expect(updateLine.email).toBe('alice@a.co')
  })

  it('appends EventLine with before and after states', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.update('users', created.$id, {
      $set: { name: 'Alice Smith' },
    })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)

    const updateEvent = events[1]
    expect(updateEvent.op).toBe('u')
    expect(updateEvent.ns).toBe('users')
    expect(updateEvent.eid).toBe(created.$id)
    expect(updateEvent.before).toEqual({ name: 'Alice' })
    expect(updateEvent.after).toEqual({ name: 'Alice Smith' })
  })

  it('increments $v on each update', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    const updated1 = await engine.update('users', created.$id, {
      $set: { name: 'Alice v2' },
    })
    const updated2 = await engine.update('users', created.$id, {
      $set: { name: 'Alice v3' },
    })

    expect(created.$v).toBe(1)
    expect(updated1.$v).toBe(2)
    expect(updated2.$v).toBe(3)
  })

  it('throws error for non-existent entity', async () => {
    await expect(
      engine.update('users', 'nonexistent-id', { $set: { name: 'Ghost' } })
    ).rejects.toThrow()
  })

  it('supports $inc operator to increment numeric fields', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })
    const updated = await engine.update('users', created.$id, {
      $inc: { age: 1 },
    })

    expect(updated.age).toBe(31)
  })

  it('$inc defaults to 0 for missing fields', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    const updated = await engine.update('users', created.$id, {
      $inc: { score: 5 },
    })

    expect(updated.score).toBe(5)
  })

  it('supports $unset operator to remove fields', async () => {
    const created = await engine.create('users', { name: 'Alice', email: 'alice@a.co' })
    const updated = await engine.update('users', created.$id, {
      $unset: { email: true },
    })

    expect(updated.name).toBe('Alice')
    expect(updated.email).toBeUndefined()
    expect('email' in updated).toBe(false)
  })
})

// =============================================================================
// Delete Operations
// =============================================================================

describe('ParqueEngine - delete', () => {
  it('returns void', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    const result = await engine.delete('users', created.$id)

    expect(result).toBeUndefined()
  })

  it('appends DataLine with op="d" (tombstone)', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    const lines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(lines).toHaveLength(2)

    const deleteLine = lines[1]
    expect(deleteLine.$id).toBe(created.$id)
    expect(deleteLine.$op).toBe('d')
    expect(deleteLine.$v).toBe(2)
    // Tombstone should not have entity data fields
    expect(deleteLine.name).toBeUndefined()
  })

  it('appends EventLine with op="d" and before state', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)

    const deleteEvent = events[1]
    expect(deleteEvent.op).toBe('d')
    expect(deleteEvent.ns).toBe('users')
    expect(deleteEvent.eid).toBe(created.$id)
    expect(deleteEvent.before).toEqual({ name: 'Alice' })
    expect(deleteEvent.after).toBeUndefined()
  })

  it('marks entity as tombstone in buffer', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    expect(engine.getBuffer('users').isTombstone(created.$id)).toBe(true)
  })

  it('throws error for non-existent entity', async () => {
    await expect(
      engine.delete('users', 'nonexistent-id')
    ).rejects.toThrow()
  })

  it('isTombstone returns true after delete', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    expect(engine.getBuffer('users').isTombstone(created.$id)).toBe(true)
  })
})

// =============================================================================
// ID Generation
// =============================================================================

describe('ParqueEngine - ID generation', () => {
  it('auto-generated IDs are unique', async () => {
    const ids: string[] = []
    for (let i = 0; i < 100; i++) {
      const result = await engine.create('users', { name: `user-${i}` })
      ids.push(result.$id)
    }

    expect(new Set(ids).size).toBe(100)
  })

  it('auto-generated IDs are sortable (lexicographic order matches creation order)', async () => {
    const ids: string[] = []
    for (let i = 0; i < 10; i++) {
      const result = await engine.create('users', { name: `user-${i}` })
      ids.push(result.$id)
    }

    const sorted = [...ids].sort()
    expect(sorted).toEqual(ids)
  })
})

// =============================================================================
// System Field Protection
// =============================================================================

describe('ParqueEngine - system field protection', () => {
  it('$set cannot overwrite $id', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.update('users', 'u1', { $set: { $id: 'evil', name: 'Bob' } })
    const result = await engine.get('users', 'u1')
    expect(result).toBeDefined()
    expect(result!.$id).toBe('u1')  // NOT 'evil'
    expect(result!.name).toBe('Bob')  // data field update works
  })

  it('$inc cannot increment $v or $ts', async () => {
    await engine.create('users', { $id: 'u2', name: 'Alice', score: 10 })
    await engine.update('users', 'u2', { $inc: { $v: 100, score: 5 } })
    const result = await engine.get('users', 'u2')
    expect(result!.$v).toBe(2)  // Normal version bump, NOT 102
    expect(result!.score).toBe(15)  // data field increment works
  })

  it('$unset cannot remove $id or $op', async () => {
    await engine.create('users', { $id: 'u3', name: 'Alice' })
    await engine.update('users', 'u3', { $unset: { $id: true, name: true } })
    const result = await engine.get('users', 'u3')
    expect(result!.$id).toBe('u3')  // system field preserved
    expect(result!.name).toBeUndefined()  // data field removed
  })
})

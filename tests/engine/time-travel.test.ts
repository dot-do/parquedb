/**
 * ParqueEngine Time-Travel Test Suite
 *
 * Tests event-based time travel capabilities:
 * - Event log integrity (correct before/after state for each operation)
 * - Schema time-travel via SchemaRegistry.getAt()
 * - Event replay from JSONL files
 */

import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { SchemaRegistry } from '@/engine/schema'
import { replay } from '@/engine/jsonl-reader'
import type { DataLine, EventLine, SchemaLine } from '@/engine/types'
import { isEventLine, isSchemaLine } from '@/engine/jsonl'

// =============================================================================
// Helpers
// =============================================================================

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-timetravel-'))
  engine = new ParqueEngine({ dataDir })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

/** Parse JSONL file into array of objects */
async function readJsonl<T>(filePath: string): Promise<T[]> {
  const content = await readFile(filePath, 'utf-8')
  return content
    .split('\n')
    .filter(line => line.trim() !== '')
    .map(line => JSON.parse(line) as T)
}

// =============================================================================
// Event Log Integrity
// =============================================================================

describe('Time-Travel: Event Log Integrity', () => {
  it('1. create entity -> events.jsonl has CREATE event with after state', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(1)

    const event = events[0]
    expect(event.op).toBe('c')
    expect(event.ns).toBe('users')
    expect(event.eid).toBe(created.$id)
    expect(event.after).toEqual({ name: 'Alice', age: 30 })
    expect(event.before).toBeUndefined()
    expect(typeof event.ts).toBe('number')
    expect(typeof event.id).toBe('string')
  })

  it('2. update entity -> events.jsonl has UPDATE event with before AND after', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })
    await engine.update('users', created.$id, { $set: { name: 'Alice Smith', age: 31 } })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)

    const updateEvent = events[1]
    expect(updateEvent.op).toBe('u')
    expect(updateEvent.ns).toBe('users')
    expect(updateEvent.eid).toBe(created.$id)
    expect(updateEvent.before).toEqual({ name: 'Alice', age: 30 })
    expect(updateEvent.after).toEqual({ name: 'Alice Smith', age: 31 })
  })

  it('3. delete entity -> events.jsonl has DELETE event with before state', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })
    await engine.delete('users', created.$id)

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)

    const deleteEvent = events[1]
    expect(deleteEvent.op).toBe('d')
    expect(deleteEvent.ns).toBe('users')
    expect(deleteEvent.eid).toBe(created.$id)
    expect(deleteEvent.before).toEqual({ name: 'Alice', age: 30 })
    expect(deleteEvent.after).toBeUndefined()
  })

  it('4. schema change -> events.jsonl has SCHEMA event (via SchemaRegistry)', () => {
    const registry = new SchemaRegistry()

    const event = registry.define('users', { name: 'string', email: 'string' })

    expect(event.op).toBe('s')
    expect(event.ns).toBe('users')
    expect(event.schema).toEqual({ name: 'string', email: 'string' })
    expect(typeof event.id).toBe('string')
    expect(typeof event.ts).toBe('number')
  })
})

// =============================================================================
// Schema Time-Travel
// =============================================================================

describe('Time-Travel: Schema Time-Travel', () => {
  it('5. define schema at t1 -> evolve at t2 -> getAt(t1) returns original schema', () => {
    const registry = new SchemaRegistry()

    // Define at t=1000
    registry.replayEvent({
      id: 'ev1',
      ts: 1000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string' },
    })

    // Evolve at t=2000
    registry.replayEvent({
      id: 'ev2',
      ts: 2000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string', role: 'string' },
      migration: { added: ['role'] },
    })

    // At t=1000, original schema
    const schemaAtT1 = registry.getAt('users', 1000)
    expect(schemaAtT1).toEqual({ name: 'string', email: 'string' })

    // At t=1500, still original (before evolution)
    const schemaAtT1_5 = registry.getAt('users', 1500)
    expect(schemaAtT1_5).toEqual({ name: 'string', email: 'string' })

    // At t=2000, evolved schema
    const schemaAtT2 = registry.getAt('users', 2000)
    expect(schemaAtT2).toEqual({ name: 'string', email: 'string', role: 'string' })
  })

  it('6. multiple schema evolutions -> getAt any point returns correct version', () => {
    const registry = new SchemaRegistry()

    // v1 at t=1000
    registry.replayEvent({
      id: 'ev1',
      ts: 1000,
      op: 's',
      ns: 'users',
      schema: { name: 'string' },
    })

    // v2 at t=2000 -- add email
    registry.replayEvent({
      id: 'ev2',
      ts: 2000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string' },
      migration: { added: ['email'] },
    })

    // v3 at t=3000 -- add role, drop email
    registry.replayEvent({
      id: 'ev3',
      ts: 3000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', role: 'string' },
      migration: { added: ['role'], dropped: ['email'] },
    })

    // v4 at t=4000 -- rename name to fullName
    registry.replayEvent({
      id: 'ev4',
      ts: 4000,
      op: 's',
      ns: 'users',
      schema: { fullName: 'string', role: 'string' },
      migration: { renamed: { name: 'fullName' } },
    })

    // Before any schema
    expect(registry.getAt('users', 500)).toBeUndefined()

    // At t=1000 (v1)
    expect(registry.getAt('users', 1000)).toEqual({ name: 'string' })

    // At t=1500 (between v1 and v2)
    expect(registry.getAt('users', 1500)).toEqual({ name: 'string' })

    // At t=2000 (v2)
    expect(registry.getAt('users', 2000)).toEqual({ name: 'string', email: 'string' })

    // At t=2500 (between v2 and v3)
    expect(registry.getAt('users', 2500)).toEqual({ name: 'string', email: 'string' })

    // At t=3000 (v3)
    expect(registry.getAt('users', 3000)).toEqual({ name: 'string', role: 'string' })

    // At t=4000 (v4)
    expect(registry.getAt('users', 4000)).toEqual({ fullName: 'string', role: 'string' })

    // Far future
    expect(registry.getAt('users', 9999999)).toEqual({ fullName: 'string', role: 'string' })
  })
})

// =============================================================================
// Event Replay
// =============================================================================

describe('Time-Travel: Event Replay', () => {
  it('7. write events -> replay events.jsonl -> can reconstruct what happened in order', async () => {
    // Perform a series of operations
    const alice = await engine.create('users', { name: 'Alice' })
    const bob = await engine.create('users', { name: 'Bob' })
    await engine.update('users', alice.$id, { $set: { name: 'Alice Smith' } })
    await engine.delete('users', bob.$id)

    await engine.close()

    // Replay the events
    const events = await replay<EventLine>(join(dataDir, 'events.jsonl'))

    expect(events).toHaveLength(4)

    // Event 1: Create Alice
    expect(events[0].op).toBe('c')
    expect(events[0].eid).toBe(alice.$id)
    expect(events[0].after).toEqual({ name: 'Alice' })

    // Event 2: Create Bob
    expect(events[1].op).toBe('c')
    expect(events[1].eid).toBe(bob.$id)
    expect(events[1].after).toEqual({ name: 'Bob' })

    // Event 3: Update Alice
    expect(events[2].op).toBe('u')
    expect(events[2].eid).toBe(alice.$id)
    expect(events[2].before).toEqual({ name: 'Alice' })
    expect(events[2].after).toEqual({ name: 'Alice Smith' })

    // Event 4: Delete Bob
    expect(events[3].op).toBe('d')
    expect(events[3].eid).toBe(bob.$id)
    expect(events[3].before).toEqual({ name: 'Bob' })
  })

  it('8. events are ordered by timestamp', async () => {
    // Create several entities with small delays
    await engine.create('users', { name: 'First' })
    await engine.create('users', { name: 'Second' })
    await engine.create('users', { name: 'Third' })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))

    // All timestamps should be monotonically non-decreasing
    for (let i = 1; i < events.length; i++) {
      expect(events[i].ts).toBeGreaterThanOrEqual(events[i - 1].ts)
    }
  })
})

// =============================================================================
// Event + Data JSONL Correlation
// =============================================================================

describe('Time-Travel: Event + Data Correlation', () => {
  it('event IDs are unique', async () => {
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })
    await engine.create('users', { name: 'Charlie' })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    const ids = events.map(e => e.id)
    expect(new Set(ids).size).toBe(3)
  })

  it('update event before state matches data at time of update', async () => {
    const created = await engine.create('users', { name: 'Alice', score: 0 })

    await engine.update('users', created.$id, { $inc: { score: 10 } })
    await engine.update('users', created.$id, { $inc: { score: 5 } })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))

    // First update: before has score=0, after has score=10
    expect(events[1].before).toEqual({ name: 'Alice', score: 0 })
    expect(events[1].after).toEqual({ name: 'Alice', score: 10 })

    // Second update: before has score=10, after has score=15
    expect(events[2].before).toEqual({ name: 'Alice', score: 10 })
    expect(events[2].after).toEqual({ name: 'Alice', score: 15 })
  })

  it('can reconstruct entity state at any point by replaying events', async () => {
    const created = await engine.create('users', { $id: 'u1', name: 'Alice', score: 0 })
    await engine.update('users', 'u1', { $set: { score: 10 } })
    await engine.update('users', 'u1', { $set: { score: 25, name: 'Alice Pro' } })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))

    // Reconstruct state at each point by examining after
    // After create: { name: 'Alice', score: 0 }
    expect(events[0].after).toEqual({ name: 'Alice', score: 0 })

    // After first update: { name: 'Alice', score: 10 }
    expect(events[1].after).toEqual({ name: 'Alice', score: 10 })

    // After second update: { name: 'Alice Pro', score: 25 }
    expect(events[2].after).toEqual({ name: 'Alice Pro', score: 25 })
  })
})

// =============================================================================
// Schema Events via SchemaRegistry
// =============================================================================

describe('Time-Travel: Schema Events', () => {
  it('define produces a SchemaLine event', () => {
    const registry = new SchemaRegistry()

    const event = registry.define('users', { name: 'string', email: 'string' })

    expect(event.op).toBe('s')
    expect(event.ns).toBe('users')
    expect(event.schema).toEqual({ name: 'string', email: 'string' })
    expect(event.migration).toBeUndefined()
  })

  it('evolve produces a SchemaLine event with migration', () => {
    const registry = new SchemaRegistry()

    registry.define('users', { name: 'string' })
    const event = registry.evolve('users', { added: ['email'] })

    expect(event.op).toBe('s')
    expect(event.ns).toBe('users')
    expect(event.schema).toEqual({ name: 'string', email: 'string' })
    expect(event.migration).toEqual({ added: ['email'] })
  })

  it('schema history is preserved in order', () => {
    const registry = new SchemaRegistry()

    registry.replayEvent({
      id: 'ev1', ts: 1000, op: 's', ns: 'users',
      schema: { name: 'string' },
    })
    registry.replayEvent({
      id: 'ev2', ts: 2000, op: 's', ns: 'users',
      schema: { name: 'string', email: 'string' },
    })
    registry.replayEvent({
      id: 'ev3', ts: 3000, op: 's', ns: 'users',
      schema: { name: 'string', email: 'string', role: 'string' },
    })

    const history = registry.getHistory('users')
    expect(history).toHaveLength(3)
    expect(history[0].ts).toBe(1000)
    expect(history[1].ts).toBe(2000)
    expect(history[2].ts).toBe(3000)
  })

  it('replaying events out of order still sorts correctly for getAt', () => {
    const registry = new SchemaRegistry()

    // Replay out of order
    registry.replayEvent({
      id: 'ev3', ts: 3000, op: 's', ns: 'users',
      schema: { name: 'string', email: 'string', role: 'string' },
    })
    registry.replayEvent({
      id: 'ev1', ts: 1000, op: 's', ns: 'users',
      schema: { name: 'string' },
    })
    registry.replayEvent({
      id: 'ev2', ts: 2000, op: 's', ns: 'users',
      schema: { name: 'string', email: 'string' },
    })

    // Despite out-of-order replay, getAt should still return correct results
    expect(registry.getAt('users', 1500)).toEqual({ name: 'string' })
    expect(registry.getAt('users', 2500)).toEqual({ name: 'string', email: 'string' })
    expect(registry.getAt('users', 3500)).toEqual({ name: 'string', email: 'string', role: 'string' })
  })
})

// =============================================================================
// Data JSONL Replay
// =============================================================================

describe('Time-Travel: Data JSONL Replay', () => {
  it('replay data JSONL recovers full entity history', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.update('users', 'u1', { $set: { name: 'Alice Smith' } })
    await engine.update('users', 'u1', { $set: { name: 'Alice Jones' } })

    await engine.close()

    const dataLines = await replay<DataLine>(join(dataDir, 'users.jsonl'))

    expect(dataLines).toHaveLength(3)
    expect(dataLines[0].$op).toBe('c')
    expect(dataLines[0].name).toBe('Alice')
    expect(dataLines[0].$v).toBe(1)

    expect(dataLines[1].$op).toBe('u')
    expect(dataLines[1].name).toBe('Alice Smith')
    expect(dataLines[1].$v).toBe(2)

    expect(dataLines[2].$op).toBe('u')
    expect(dataLines[2].name).toBe('Alice Jones')
    expect(dataLines[2].$v).toBe(3)
  })

  it('replay missing file returns empty array', async () => {
    const lines = await replay(join(dataDir, 'nonexistent.jsonl'))
    expect(lines).toEqual([])
  })
})

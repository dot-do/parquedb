import { describe, it, expect, beforeEach } from 'vitest'
import { SchemaRegistry } from '@/engine/schema'
import type { SchemaLine, Migration } from '@/engine/types'

/**
 * SchemaRegistry Test Suite
 *
 * Tests the in-memory schema registry that manages schema definitions,
 * schema evolution (migrations), time-travel queries, and schema inference.
 *
 * The registry produces SchemaLine events for every schema change, which
 * can be serialized to the events.jsonl log.
 */

describe('SchemaRegistry', () => {
  let registry: SchemaRegistry

  beforeEach(() => {
    registry = new SchemaRegistry()
  })

  // ===========================================================================
  // 1-6. Define schema
  // ===========================================================================
  describe('define()', () => {
    it('returns a SchemaLine event', () => {
      const event = registry.define('users', { name: 'string', email: 'string' })
      expect(event).toBeDefined()
      expect(event.op).toBe('s')
      expect(event.ns).toBe('users')
      expect(event.schema).toEqual({ name: 'string', email: 'string' })
    })

    it('returned SchemaLine has no migration field', () => {
      const event = registry.define('users', { name: 'string', email: 'string' })
      expect(event.migration).toBeUndefined()
    })

    it('returned SchemaLine has auto-generated non-empty id', () => {
      const event = registry.define('users', { name: 'string', email: 'string' })
      expect(event.id).toBeDefined()
      expect(typeof event.id).toBe('string')
      expect(event.id.length).toBeGreaterThan(0)
    })

    it('returned SchemaLine has ts set to approximately Date.now()', () => {
      const before = Date.now()
      const event = registry.define('users', { name: 'string', email: 'string' })
      const after = Date.now()
      expect(event.ts).toBeGreaterThanOrEqual(before)
      expect(event.ts).toBeLessThanOrEqual(after)
    })

    it('after define, get() returns the schema', () => {
      registry.define('users', { name: 'string', email: 'string' })
      expect(registry.get('users')).toEqual({ name: 'string', email: 'string' })
    })

    it('define for same table overwrites previous schema', () => {
      registry.define('users', { name: 'string' })
      registry.define('users', { username: 'string', age: 'int' })
      expect(registry.get('users')).toEqual({ username: 'string', age: 'int' })
    })
  })

  // ===========================================================================
  // 7-9. Get schema
  // ===========================================================================
  describe('get() and has()', () => {
    it('get() returns undefined for unregistered table', () => {
      expect(registry.get('unknown')).toBeUndefined()
    })

    it('get() returns the latest schema after define', () => {
      registry.define('users', { name: 'string', email: 'string' })
      const schema = registry.get('users')
      expect(schema).toEqual({ name: 'string', email: 'string' })
    })

    it('has() returns true after define, false before', () => {
      expect(registry.has('users')).toBe(false)
      registry.define('users', { name: 'string' })
      expect(registry.has('users')).toBe(true)
    })
  })

  // ===========================================================================
  // 10-13. Evolve schema (add field)
  // ===========================================================================
  describe('evolve() - add field', () => {
    beforeEach(() => {
      registry.define('users', { name: 'string', email: 'string' })
    })

    it('returns a SchemaLine event', () => {
      const event = registry.evolve('users', { added: ['role'], default: { role: 'member' } })
      expect(event).toBeDefined()
      expect(event.op).toBe('s')
      expect(event.ns).toBe('users')
    })

    it('returned SchemaLine has full updated schema including new field', () => {
      const event = registry.evolve('users', { added: ['role'], default: { role: 'member' } })
      expect(event.schema).toEqual({ name: 'string', email: 'string', role: 'string' })
    })

    it('migration field records the addition', () => {
      const event = registry.evolve('users', { added: ['role'], default: { role: 'member' } })
      expect(event.migration).toEqual({ added: ['role'], default: { role: 'member' } })
    })

    it('after evolve, get() includes the new field', () => {
      registry.evolve('users', { added: ['role'], default: { role: 'member' } })
      const schema = registry.get('users')
      expect(schema).toEqual({ name: 'string', email: 'string', role: 'string' })
    })
  })

  // ===========================================================================
  // 14-16. Evolve schema (rename field)
  // ===========================================================================
  describe('evolve() - rename field', () => {
    beforeEach(() => {
      registry.define('users', { name: 'string', email: 'string' })
    })

    it('returns a SchemaLine event', () => {
      const event = registry.evolve('users', { renamed: { name: 'displayName' } })
      expect(event).toBeDefined()
      expect(event.op).toBe('s')
    })

    it('new schema has renamed field but not old field', () => {
      const event = registry.evolve('users', { renamed: { name: 'displayName' } })
      expect(event.schema).toHaveProperty('displayName', 'string')
      expect(event.schema).not.toHaveProperty('name')
      expect(event.schema).toHaveProperty('email', 'string')
    })

    it('migration field records the rename', () => {
      const event = registry.evolve('users', { renamed: { name: 'displayName' } })
      expect(event.migration).toEqual({ renamed: { name: 'displayName' } })
    })
  })

  // ===========================================================================
  // 17-19. Evolve schema (drop field)
  // ===========================================================================
  describe('evolve() - drop field', () => {
    beforeEach(() => {
      registry.define('users', { name: 'string', email: 'string' })
    })

    it('returns a SchemaLine event', () => {
      const event = registry.evolve('users', { dropped: ['email'] })
      expect(event).toBeDefined()
      expect(event.op).toBe('s')
    })

    it('new schema no longer has the dropped field', () => {
      const event = registry.evolve('users', { dropped: ['email'] })
      expect(event.schema).not.toHaveProperty('email')
      expect(event.schema).toEqual({ name: 'string' })
    })

    it('migration field records the drop', () => {
      const event = registry.evolve('users', { dropped: ['email'] })
      expect(event.migration).toEqual({ dropped: ['email'] })
    })
  })

  // ===========================================================================
  // 20-22. Evolve schema (change type)
  // ===========================================================================
  describe('evolve() - change type', () => {
    beforeEach(() => {
      registry.define('users', { name: 'string', age: 'int' })
    })

    it('returns a SchemaLine event', () => {
      const event = registry.evolve('users', { changed: { age: 'float' } })
      expect(event).toBeDefined()
      expect(event.op).toBe('s')
    })

    it('schema field type is updated', () => {
      const event = registry.evolve('users', { changed: { age: 'float' } })
      expect(event.schema).toEqual({ name: 'string', age: 'float' })
    })

    it('migration records the type change', () => {
      const event = registry.evolve('users', { changed: { age: 'float' } })
      expect(event.migration).toEqual({ changed: { age: 'float' } })
    })
  })

  // ===========================================================================
  // 23-26. Evolve errors
  // ===========================================================================
  describe('evolve() - errors', () => {
    it('throws error when evolving an unregistered table', () => {
      expect(() => registry.evolve('unknown', { added: ['role'] })).toThrow()
    })

    it('throws error when adding a field that already exists', () => {
      registry.define('users', { name: 'string', email: 'string' })
      expect(() => registry.evolve('users', { added: ['name'] })).toThrow()
    })

    it('throws error when renaming a field that does not exist', () => {
      registry.define('users', { name: 'string', email: 'string' })
      expect(() => registry.evolve('users', { renamed: { nonexistent: 'newName' } })).toThrow()
    })

    it('throws error when dropping a field that does not exist', () => {
      registry.define('users', { name: 'string', email: 'string' })
      expect(() => registry.evolve('users', { dropped: ['nonexistent'] })).toThrow()
    })
  })

  // ===========================================================================
  // 27-29. Time-travel
  // ===========================================================================
  describe('getAt() - time-travel', () => {
    it('returns original schema for timestamp between define and evolve', () => {
      // Use explicit timestamps via replayEvent for deterministic tests
      const defineEvent: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
      }
      const evolveEvent: SchemaLine = {
        id: 'ev2',
        ts: 2000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string', role: 'string' },
        migration: { added: ['role'] },
      }

      registry.replayEvent(defineEvent)
      registry.replayEvent(evolveEvent)

      const schema = registry.getAt('users', 1500)
      expect(schema).toEqual({ name: 'string', email: 'string' })
    })

    it('returns evolved schema for timestamp after evolve', () => {
      const defineEvent: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
      }
      const evolveEvent: SchemaLine = {
        id: 'ev2',
        ts: 2000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string', role: 'string' },
        migration: { added: ['role'] },
      }

      registry.replayEvent(defineEvent)
      registry.replayEvent(evolveEvent)

      const schema = registry.getAt('users', 2500)
      expect(schema).toEqual({ name: 'string', email: 'string', role: 'string' })
    })

    it('returns undefined for timestamp before first definition', () => {
      const defineEvent: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
      }

      registry.replayEvent(defineEvent)

      const schema = registry.getAt('users', 500)
      expect(schema).toBeUndefined()
    })
  })

  // ===========================================================================
  // 30-32. Replay from events
  // ===========================================================================
  describe('replayEvent()', () => {
    it('applies a SchemaLine to rebuild state', () => {
      const event: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
      }
      registry.replayEvent(event)
      expect(registry.get('users')).toEqual({ name: 'string', email: 'string' })
    })

    it('replaying multiple events rebuilds the full history for time-travel', () => {
      const event1: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string' },
      }
      const event2: SchemaLine = {
        id: 'ev2',
        ts: 2000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
        migration: { added: ['email'] },
      }
      const event3: SchemaLine = {
        id: 'ev3',
        ts: 3000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string', role: 'string' },
        migration: { added: ['role'] },
      }

      registry.replayEvent(event1)
      registry.replayEvent(event2)
      registry.replayEvent(event3)

      // Time travel to each point
      expect(registry.getAt('users', 1500)).toEqual({ name: 'string' })
      expect(registry.getAt('users', 2500)).toEqual({ name: 'string', email: 'string' })
      expect(registry.getAt('users', 3500)).toEqual({ name: 'string', email: 'string', role: 'string' })
    })

    it('getHistory() returns all schema versions in order', () => {
      const event1: SchemaLine = {
        id: 'ev1',
        ts: 1000,
        op: 's',
        ns: 'users',
        schema: { name: 'string' },
      }
      const event2: SchemaLine = {
        id: 'ev2',
        ts: 2000,
        op: 's',
        ns: 'users',
        schema: { name: 'string', email: 'string' },
        migration: { added: ['email'] },
      }

      registry.replayEvent(event1)
      registry.replayEvent(event2)

      const history = registry.getHistory('users')
      expect(history).toHaveLength(2)
      expect(history[0].ts).toBe(1000)
      expect(history[0].schema).toEqual({ name: 'string' })
      expect(history[1].ts).toBe(2000)
      expect(history[1].schema).toEqual({ name: 'string', email: 'string' })
    })
  })

  // ===========================================================================
  // 33-34. Field type inference
  // ===========================================================================
  describe('inferType()', () => {
    it('infers string type', () => {
      expect(SchemaRegistry.inferType('hello')).toBe('string')
    })

    it('infers int type for integers', () => {
      expect(SchemaRegistry.inferType(42)).toBe('int')
    })

    it('infers float type for floating-point numbers', () => {
      expect(SchemaRegistry.inferType(3.14)).toBe('float')
    })

    it('infers boolean type', () => {
      expect(SchemaRegistry.inferType(true)).toBe('boolean')
      expect(SchemaRegistry.inferType(false)).toBe('boolean')
    })

    it('infers object type for plain objects', () => {
      expect(SchemaRegistry.inferType({ a: 1 })).toBe('object')
    })

    it('infers array type for arrays', () => {
      expect(SchemaRegistry.inferType([1, 2, 3])).toBe('array')
    })

    it('infers null type for null', () => {
      expect(SchemaRegistry.inferType(null)).toBe('null')
    })

    it('infers null type for undefined', () => {
      expect(SchemaRegistry.inferType(undefined)).toBe('null')
    })
  })

  describe('inferSchema()', () => {
    it('examines an array of entities and produces a schema', () => {
      const entities = [
        { name: 'Alice', age: 30, active: true },
        { name: 'Bob', age: 25, email: 'bob@example.com' },
        { name: 'Charlie', age: 35, active: false, scores: [1, 2, 3] },
      ]

      const schema = SchemaRegistry.inferSchema(entities)
      expect(schema).toEqual({
        name: 'string',
        age: 'int',
        active: 'boolean',
        email: 'string',
        scores: 'array',
      })
    })

    it('returns empty schema for empty array', () => {
      const schema = SchemaRegistry.inferSchema([])
      expect(schema).toEqual({})
    })

    it('handles entities with nested objects', () => {
      const entities = [
        { name: 'Alice', address: { city: 'NYC' } },
      ]
      const schema = SchemaRegistry.inferSchema(entities)
      expect(schema).toEqual({ name: 'string', address: 'object' })
    })

    it('handles entities with float values', () => {
      const entities = [
        { price: 9.99, quantity: 5 },
      ]
      const schema = SchemaRegistry.inferSchema(entities)
      expect(schema).toEqual({ price: 'float', quantity: 'int' })
    })
  })
})

/**
 * EventLine Type Safety Tests
 *
 * Verifies that the EventLine type is used throughout the event pipeline
 * instead of Record<string, unknown>. These tests ensure:
 *
 * 1. EventLine has all expected fields
 * 2. Functions accepting/returning events use EventLine
 * 3. Type narrowing works correctly between EventLine and SchemaLine
 * 4. The event pipeline preserves EventLine structure end-to-end
 */

import { describe, it, expect } from 'vitest'
import type { EventLine, SchemaLine, EventOp } from '@/engine/types'
import { isEventLine, isSchemaLine } from '@/engine/jsonl'
import { mergeEvents } from '@/engine/merge-events'

// =============================================================================
// EventLine shape tests
// =============================================================================

describe('EventLine type shape', () => {
  it('has all required fields', () => {
    const event: EventLine = {
      id: 'evt-001',
      ts: 1738857600000,
      op: 'c',
      ns: 'users',
      eid: 'user-001',
    }

    expect(event.id).toBe('evt-001')
    expect(event.ts).toBe(1738857600000)
    expect(event.op).toBe('c')
    expect(event.ns).toBe('users')
    expect(event.eid).toBe('user-001')
  })

  it('has optional before field', () => {
    const event: EventLine = {
      id: 'evt-002',
      ts: 1738857600000,
      op: 'u',
      ns: 'users',
      eid: 'user-001',
      before: { name: 'Old Name' },
    }

    expect(event.before).toEqual({ name: 'Old Name' })
  })

  it('has optional after field', () => {
    const event: EventLine = {
      id: 'evt-003',
      ts: 1738857600000,
      op: 'c',
      ns: 'users',
      eid: 'user-001',
      after: { name: 'Alice' },
    }

    expect(event.after).toEqual({ name: 'Alice' })
  })

  it('has optional actor field', () => {
    const event: EventLine = {
      id: 'evt-004',
      ts: 1738857600000,
      op: 'u',
      ns: 'users',
      eid: 'user-001',
      actor: 'admin-1',
    }

    expect(event.actor).toBe('admin-1')
  })

  it('op field accepts only c, u, d', () => {
    const ops: EventOp[] = ['c', 'u', 'd', 's']
    const eventOps = ops.filter((op): op is EventLine['op'] => op === 'c' || op === 'u' || op === 'd')
    expect(eventOps).toEqual(['c', 'u', 'd'])
  })

  it('update event has both before and after', () => {
    const event: EventLine = {
      id: 'evt-005',
      ts: 1738857600000,
      op: 'u',
      ns: 'users',
      eid: 'user-001',
      before: { name: 'Old' },
      after: { name: 'New' },
      actor: 'system',
    }

    expect(event.before).toEqual({ name: 'Old' })
    expect(event.after).toEqual({ name: 'New' })
    expect(event.actor).toBe('system')
  })

  it('delete event has before but no after', () => {
    const event: EventLine = {
      id: 'evt-006',
      ts: 1738857600000,
      op: 'd',
      ns: 'users',
      eid: 'user-001',
      before: { name: 'Deleted User' },
    }

    expect(event.before).toEqual({ name: 'Deleted User' })
    expect(event.after).toBeUndefined()
  })
})

// =============================================================================
// Type narrowing tests (EventLine vs SchemaLine)
// =============================================================================

describe('EventLine / SchemaLine type narrowing', () => {
  it('isEventLine correctly identifies EventLine', () => {
    const event: EventLine = {
      id: 'evt-001',
      ts: 1000,
      op: 'c',
      ns: 'users',
      eid: 'u1',
      after: { name: 'Alice' },
    }

    expect(isEventLine(event)).toBe(true)
    expect(isSchemaLine(event)).toBe(false)
  })

  it('isSchemaLine correctly identifies SchemaLine', () => {
    const schema: SchemaLine = {
      id: 'sch-001',
      ts: 1000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string' },
    }

    expect(isSchemaLine(schema)).toBe(true)
    expect(isEventLine(schema)).toBe(false)
  })

  it('type guard narrows EventLine fields correctly', () => {
    const line: EventLine | SchemaLine = {
      id: 'evt-001',
      ts: 1000,
      op: 'c',
      ns: 'users',
      eid: 'u1',
      after: { name: 'Alice' },
    } as EventLine | SchemaLine

    if (isEventLine(line)) {
      // After narrowing, EventLine-specific fields are accessible
      expect(line.eid).toBe('u1')
      expect(line.after).toEqual({ name: 'Alice' })
    } else {
      // Should not reach here
      expect.unreachable('Expected EventLine, got SchemaLine')
    }
  })
})

// =============================================================================
// mergeEvents type tests
// =============================================================================

describe('mergeEvents uses EventLine', () => {
  it('accepts and returns EventLine arrays', () => {
    const base: EventLine[] = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
      { id: 'e3', ts: 300, op: 'c', ns: 'users', eid: 'u3', after: { name: 'Charlie' } },
    ]

    const overlay: EventLine[] = [
      { id: 'e2', ts: 200, op: 'c', ns: 'users', eid: 'u2', after: { name: 'Bob' } },
    ]

    const result = mergeEvents(base, overlay)

    expect(result).toHaveLength(3)
    // Result is sorted by ts
    expect(result[0].ts).toBe(100)
    expect(result[1].ts).toBe(200)
    expect(result[2].ts).toBe(300)

    // Each result retains EventLine fields
    expect(result[0].id).toBe('e1')
    expect(result[1].id).toBe('e2')
    expect(result[2].id).toBe('e3')
  })

  it('handles mixed EventLine and SchemaLine arrays', () => {
    const base: Array<EventLine | SchemaLine> = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ]

    const overlay: Array<EventLine | SchemaLine> = [
      { id: 's1', ts: 200, op: 's', ns: 'users', schema: { name: 'string' } },
    ]

    const result = mergeEvents(base, overlay)

    expect(result).toHaveLength(2)
    expect(result[0].ts).toBe(100)
    expect(result[1].ts).toBe(200)
  })

  it('preserves all EventLine fields through merge', () => {
    const event: EventLine = {
      id: 'e1',
      ts: 100,
      op: 'u',
      ns: 'users',
      eid: 'u1',
      before: { name: 'Old' },
      after: { name: 'New' },
      actor: 'admin',
    }

    const result = mergeEvents([event], [])

    expect(result).toHaveLength(1)
    const merged = result[0] as EventLine
    expect(merged.id).toBe('e1')
    expect(merged.ts).toBe(100)
    expect(merged.op).toBe('u')
    expect(merged.ns).toBe('users')
    expect(merged.eid).toBe('u1')
    expect(merged.before).toEqual({ name: 'Old' })
    expect(merged.after).toEqual({ name: 'New' })
    expect(merged.actor).toBe('admin')
  })
})

// =============================================================================
// encodeEventsToParquet type tests
// =============================================================================

describe('encodeEventsToParquet accepts EventLine', () => {
  it('accepts EventLine array parameter', async () => {
    const { encodeEventsToParquet } = await import('@/engine/parquet-encoders')

    const events: EventLine[] = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
      { id: 'e2', ts: 200, op: 'u', ns: 'users', eid: 'u1', before: { name: 'Alice' }, after: { name: 'Alice S.' }, actor: 'admin' },
    ]

    const buffer = await encodeEventsToParquet(events)
    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
  })
})

// =============================================================================
// EventStorageAdapter type tests
// =============================================================================

describe('EventStorageAdapter uses EventLine', () => {
  it('FullStorageAdapter readEvents/writeEvents use EventLine', async () => {
    const { MemoryStorageAdapter } = await import('@/engine/storage-adapters')

    const adapter = new MemoryStorageAdapter()

    const events: EventLine[] = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ]

    await adapter.writeEvents('events.parquet', events)
    const read = await adapter.readEvents('events.parquet')

    expect(read).toHaveLength(1)
    expect(read[0].id).toBe('e1')
    expect(read[0].ts).toBe(100)
  })
})

/**
 * JSONL Serialization, Deserialization & Type Guard Tests
 *
 * Comprehensive tests for the jsonl.ts module which provides the
 * foundational type system for the MergeTree engine's JSONL format.
 *
 * Covers:
 * - isDataLine type guard
 * - isRelLine type guard
 * - isEventLine type guard
 * - isSchemaLine type guard
 * - serializeLine serialization
 * - deserializeLine deserialization
 * - Roundtrip correctness for all line types
 * - Edge cases and cross-type discrimination
 */

import { describe, it, expect } from 'vitest'
import {
  serializeLine,
  deserializeLine,
  isDataLine,
  isRelLine,
  isEventLine,
  isSchemaLine,
} from '@/engine/jsonl'
import type { DataLine, RelLine, EventLine, SchemaLine, Line } from '@/engine/types'

// =============================================================================
// Test Fixtures
// =============================================================================

const validDataLineCreate: DataLine = {
  $id: 'user_01',
  $op: 'c',
  $v: 1,
  $ts: 1738857600000,
  name: 'Alice',
  email: 'alice@example.com',
}

const validDataLineUpdate: DataLine = {
  $id: 'user_01',
  $op: 'u',
  $v: 2,
  $ts: 1738857600050,
  name: 'Alice Smith',
  email: 'alice@example.com',
}

const validDataLineDelete: DataLine = {
  $id: 'user_01',
  $op: 'd',
  $v: 3,
  $ts: 1738857600099,
}

const validRelLineLink: RelLine = {
  $op: 'l',
  $ts: 1738857600000,
  f: 'user_01',
  p: 'author',
  r: 'posts',
  t: 'post_01',
}

const validRelLineUnlink: RelLine = {
  $op: 'u',
  $ts: 1738857600050,
  f: 'user_01',
  p: 'author',
  r: 'posts',
  t: 'post_01',
}

const validEventLine: EventLine = {
  id: 'evt_01',
  ts: 1738857600000,
  op: 'c',
  ns: 'users',
  eid: 'user_01',
  after: { name: 'Alice' },
}

const validEventLineUpdate: EventLine = {
  id: 'evt_02',
  ts: 1738857600050,
  op: 'u',
  ns: 'users',
  eid: 'user_01',
  before: { name: 'Alice' },
  after: { name: 'Alice Smith' },
}

const validEventLineDelete: EventLine = {
  id: 'evt_03',
  ts: 1738857600099,
  op: 'd',
  ns: 'users',
  eid: 'user_01',
  before: { name: 'Alice Smith' },
}

const validSchemaLine: SchemaLine = {
  id: 'sch_01',
  ts: 1738857600000,
  op: 's',
  ns: 'users',
  schema: { name: 'string', email: 'string' },
}

const validSchemaLineWithMigration: SchemaLine = {
  id: 'sch_02',
  ts: 1738857700000,
  op: 's',
  ns: 'users',
  schema: { name: 'string', email: 'string', role: 'string' },
  migration: { added: ['role'], default: { role: 'user' } },
}

// =============================================================================
// isDataLine
// =============================================================================

describe('isDataLine', () => {
  it('returns true for a create DataLine', () => {
    expect(isDataLine(validDataLineCreate)).toBe(true)
  })

  it('returns true for an update DataLine', () => {
    expect(isDataLine(validDataLineUpdate)).toBe(true)
  })

  it('returns true for a delete DataLine', () => {
    expect(isDataLine(validDataLineDelete)).toBe(true)
  })

  it('returns true for DataLine with extra data fields', () => {
    const line: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      name: 'Alice',
      age: 25,
      tags: ['admin', 'user'],
      nested: { foo: 'bar' },
    }
    expect(isDataLine(line)).toBe(true)
  })

  it('returns true for DataLine with minimal system fields', () => {
    const line: DataLine = { $id: 'u1', $op: 'c', $v: 1, $ts: 1000 }
    expect(isDataLine(line)).toBe(true)
  })

  it('returns false for a RelLine (link)', () => {
    expect(isDataLine(validRelLineLink)).toBe(false)
  })

  it('returns false for a RelLine (unlink)', () => {
    expect(isDataLine(validRelLineUnlink)).toBe(false)
  })

  it('returns false for an EventLine', () => {
    expect(isDataLine(validEventLine)).toBe(false)
  })

  it('returns false for a SchemaLine', () => {
    expect(isDataLine(validSchemaLine)).toBe(false)
  })

  it('discriminates based on $op value -- rejects link op', () => {
    // A line that has $id and $v but $op is 'l' (link op, not a valid DataOp)
    const hybrid = { $id: 'u1', $op: 'l', $v: 1, $ts: 1000 } as unknown as Line
    expect(isDataLine(hybrid)).toBe(false)
  })

  it('discriminates based on $op value -- rejects schema op', () => {
    const hybrid = { $id: 'u1', $op: 's', $v: 1, $ts: 1000 } as unknown as Line
    expect(isDataLine(hybrid)).toBe(false)
  })
})

// =============================================================================
// isRelLine
// =============================================================================

describe('isRelLine', () => {
  it('returns true for a link RelLine', () => {
    expect(isRelLine(validRelLineLink)).toBe(true)
  })

  it('returns true for an unlink RelLine', () => {
    expect(isRelLine(validRelLineUnlink)).toBe(true)
  })

  it('returns false for a DataLine', () => {
    expect(isRelLine(validDataLineCreate)).toBe(false)
  })

  it('returns false for an EventLine', () => {
    expect(isRelLine(validEventLine)).toBe(false)
  })

  it('returns false for a SchemaLine', () => {
    expect(isRelLine(validSchemaLine)).toBe(false)
  })

  it('returns false when f is missing', () => {
    const partial = { $op: 'l', $ts: 1000, p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isRelLine(partial)).toBe(false)
  })

  it('returns false when p is missing', () => {
    const partial = { $op: 'l', $ts: 1000, f: 'u1', r: 'posts', t: 'p1' } as unknown as Line
    expect(isRelLine(partial)).toBe(false)
  })

  it('returns false when r is missing', () => {
    const partial = { $op: 'l', $ts: 1000, f: 'u1', p: 'author', t: 'p1' } as unknown as Line
    expect(isRelLine(partial)).toBe(false)
  })

  it('returns false when t is missing', () => {
    const partial = { $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts' } as unknown as Line
    expect(isRelLine(partial)).toBe(false)
  })

  it('returns false when $op is missing', () => {
    const partial = { $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isRelLine(partial)).toBe(false)
  })

  it('returns false when $op is not a RelOp', () => {
    // Has all rel fields but $op is 'c' (DataOp)
    const hybrid = { $op: 'c', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isRelLine(hybrid)).toBe(false)
  })

  it('returns false when $op is d (delete is not a RelOp)', () => {
    const hybrid = { $op: 'd', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isRelLine(hybrid)).toBe(false)
  })
})

// =============================================================================
// isEventLine
// =============================================================================

describe('isEventLine', () => {
  it('returns true for a create EventLine', () => {
    expect(isEventLine(validEventLine)).toBe(true)
  })

  it('returns true for an update EventLine', () => {
    expect(isEventLine(validEventLineUpdate)).toBe(true)
  })

  it('returns true for a delete EventLine', () => {
    expect(isEventLine(validEventLineDelete)).toBe(true)
  })

  it('returns true for EventLine with actor field', () => {
    const line: EventLine = {
      id: 'evt_04',
      ts: 1738857600000,
      op: 'c',
      ns: 'users',
      eid: 'user_01',
      actor: 'system',
    }
    expect(isEventLine(line)).toBe(true)
  })

  it('returns false for a DataLine', () => {
    expect(isEventLine(validDataLineCreate)).toBe(false)
  })

  it('returns false for a RelLine', () => {
    expect(isEventLine(validRelLineLink)).toBe(false)
  })

  it('returns false for a SchemaLine (op is s, not c/u/d)', () => {
    expect(isEventLine(validSchemaLine)).toBe(false)
  })

  it('returns false when id is missing', () => {
    const partial = { ts: 1000, op: 'c', ns: 'users', eid: 'u1' } as unknown as Line
    expect(isEventLine(partial)).toBe(false)
  })

  it('returns false when ns is missing', () => {
    const partial = { id: 'e1', ts: 1000, op: 'c', eid: 'u1' } as unknown as Line
    expect(isEventLine(partial)).toBe(false)
  })

  it('returns false when eid is missing', () => {
    const partial = { id: 'e1', ts: 1000, op: 'c', ns: 'users' } as unknown as Line
    expect(isEventLine(partial)).toBe(false)
  })

  it('returns false when op is missing', () => {
    const partial = { id: 'e1', ts: 1000, ns: 'users', eid: 'u1' } as unknown as Line
    expect(isEventLine(partial)).toBe(false)
  })

  it('returns false when op is s (schema op, not event op)', () => {
    const hybrid = { id: 'e1', ts: 1000, op: 's', ns: 'users', eid: 'u1' } as unknown as Line
    expect(isEventLine(hybrid)).toBe(false)
  })
})

// =============================================================================
// isSchemaLine
// =============================================================================

describe('isSchemaLine', () => {
  it('returns true for a valid SchemaLine', () => {
    expect(isSchemaLine(validSchemaLine)).toBe(true)
  })

  it('returns true for SchemaLine with migration', () => {
    expect(isSchemaLine(validSchemaLineWithMigration)).toBe(true)
  })

  it('returns false for a DataLine', () => {
    expect(isSchemaLine(validDataLineCreate)).toBe(false)
  })

  it('returns false for a RelLine', () => {
    expect(isSchemaLine(validRelLineLink)).toBe(false)
  })

  it('returns false for an EventLine (op is c, not s)', () => {
    expect(isSchemaLine(validEventLine)).toBe(false)
  })

  it('returns false when id is missing', () => {
    const partial = { ts: 1000, op: 's', ns: 'users', schema: { name: 'string' } } as unknown as Line
    expect(isSchemaLine(partial)).toBe(false)
  })

  it('returns false when ns is missing', () => {
    const partial = { id: 's1', ts: 1000, op: 's', schema: { name: 'string' } } as unknown as Line
    expect(isSchemaLine(partial)).toBe(false)
  })

  it('returns false when schema is missing', () => {
    const partial = { id: 's1', ts: 1000, op: 's', ns: 'users' } as unknown as Line
    expect(isSchemaLine(partial)).toBe(false)
  })

  it('returns false when op is not s', () => {
    const hybrid = { id: 's1', ts: 1000, op: 'c', ns: 'users', schema: { name: 'string' } } as unknown as Line
    expect(isSchemaLine(hybrid)).toBe(false)
  })
})

// =============================================================================
// Edge Cases: empty object, null, undefined, extra fields
// =============================================================================

describe('edge cases', () => {
  it('empty object is rejected by all type guards', () => {
    const empty = {} as unknown as Line
    expect(isDataLine(empty)).toBe(false)
    expect(isRelLine(empty)).toBe(false)
    expect(isEventLine(empty)).toBe(false)
    expect(isSchemaLine(empty)).toBe(false)
  })

  it('object with only unrelated keys is rejected by all type guards', () => {
    const obj = { foo: 'bar', baz: 42 } as unknown as Line
    expect(isDataLine(obj)).toBe(false)
    expect(isRelLine(obj)).toBe(false)
    expect(isEventLine(obj)).toBe(false)
    expect(isSchemaLine(obj)).toBe(false)
  })

  it('object with extra fields beyond DataLine still passes isDataLine', () => {
    const obj = { $id: 'u1', $op: 'c' as const, $v: 1, $ts: 1000, extraField: true, anotherExtra: [1, 2, 3] }
    expect(isDataLine(obj as unknown as Line)).toBe(true)
  })

  it('object with extra fields beyond RelLine still passes isRelLine', () => {
    const obj = { $op: 'l' as const, $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1', extra: 'value' }
    expect(isRelLine(obj as unknown as Line)).toBe(true)
  })

  it('object with extra fields beyond EventLine still passes isEventLine', () => {
    const obj = { id: 'e1', ts: 1000, op: 'c' as const, ns: 'users', eid: 'u1', customMeta: { x: 1 } }
    expect(isEventLine(obj as unknown as Line)).toBe(true)
  })

  it('object with extra fields beyond SchemaLine still passes isSchemaLine', () => {
    const obj = { id: 's1', ts: 1000, op: 's' as const, ns: 'users', schema: { name: 'string' }, extra: true }
    expect(isSchemaLine(obj as unknown as Line)).toBe(true)
  })
})

// =============================================================================
// Ambiguous Objects: fields from multiple line types
// =============================================================================

describe('ambiguous objects', () => {
  it('object with DataLine + RelLine fields: isDataLine true if $op matches DataOp', () => {
    // Has $id, $v (DataLine), plus f, p, r, t (RelLine), with $op=c (DataOp)
    const obj = { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isDataLine(obj)).toBe(true)
    // isRelLine should be false because $op='c' is not a RelOp
    expect(isRelLine(obj)).toBe(false)
  })

  it('object with DataLine + RelLine fields: isRelLine true if $op matches RelOp', () => {
    // Has $id, $v (DataLine), plus f, p, r, t (RelLine), with $op=l (RelOp)
    const obj = { $id: 'u1', $op: 'l', $v: 1, $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' } as unknown as Line
    expect(isDataLine(obj)).toBe(false)
    expect(isRelLine(obj)).toBe(true)
  })

  it('object with EventLine + SchemaLine fields: isEventLine true if op matches EventOp', () => {
    // Has id, ns, eid (EventLine), plus schema (SchemaLine), with op=c (EventOp)
    const obj = { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1', schema: { name: 'string' } } as unknown as Line
    expect(isEventLine(obj)).toBe(true)
    // isSchemaLine should be false because op='c' is not SchemaOp
    expect(isSchemaLine(obj)).toBe(false)
  })

  it('object with EventLine + SchemaLine fields: isSchemaLine true if op=s', () => {
    // Has id, ns, schema (SchemaLine), plus eid (EventLine), with op=s
    const obj = { id: 's1', ts: 1000, op: 's', ns: 'users', eid: 'u1', schema: { name: 'string' } } as unknown as Line
    expect(isSchemaLine(obj)).toBe(true)
    // isEventLine should be false because op='s' is not EventOp
    expect(isEventLine(obj)).toBe(false)
  })

  it('object with all fields from all types: discrimination via $op', () => {
    // Frankenstein object with fields from every line type
    const obj = {
      $id: 'u1', $op: 'c', $v: 1, $ts: 1000,
      f: 'u1', p: 'author', r: 'posts', t: 'p1',
      id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1', schema: { name: 'string' },
    } as unknown as Line
    // isDataLine: has $id, $v, $op='c' (valid DataOp) => true
    expect(isDataLine(obj)).toBe(true)
    // isRelLine: has f, p, r, t, $op='c' (not a RelOp) => false
    expect(isRelLine(obj)).toBe(false)
    // isEventLine: has id, ns, eid, op='c' (valid EventOp) => true
    expect(isEventLine(obj)).toBe(true)
    // isSchemaLine: has id, ns, schema, op='c' (not 's') => false
    expect(isSchemaLine(obj)).toBe(false)
  })
})

// =============================================================================
// Cross-type Discrimination
// =============================================================================

describe('cross-type discrimination', () => {
  it('each line type matches exactly one type guard', () => {
    const lines: Line[] = [
      validDataLineCreate,
      validDataLineUpdate,
      validDataLineDelete,
      validRelLineLink,
      validRelLineUnlink,
      validEventLine,
      validEventLineUpdate,
      validEventLineDelete,
      validSchemaLine,
      validSchemaLineWithMigration,
    ]

    for (const line of lines) {
      const matches = [
        isDataLine(line),
        isRelLine(line),
        isEventLine(line),
        isSchemaLine(line),
      ].filter(Boolean)
      expect(matches).toHaveLength(1)
    }
  })

  it('DataLine create matches only isDataLine', () => {
    expect(isDataLine(validDataLineCreate)).toBe(true)
    expect(isRelLine(validDataLineCreate)).toBe(false)
    expect(isEventLine(validDataLineCreate)).toBe(false)
    expect(isSchemaLine(validDataLineCreate)).toBe(false)
  })

  it('RelLine link matches only isRelLine', () => {
    expect(isDataLine(validRelLineLink)).toBe(false)
    expect(isRelLine(validRelLineLink)).toBe(true)
    expect(isEventLine(validRelLineLink)).toBe(false)
    expect(isSchemaLine(validRelLineLink)).toBe(false)
  })

  it('EventLine matches only isEventLine', () => {
    expect(isDataLine(validEventLine)).toBe(false)
    expect(isRelLine(validEventLine)).toBe(false)
    expect(isEventLine(validEventLine)).toBe(true)
    expect(isSchemaLine(validEventLine)).toBe(false)
  })

  it('SchemaLine matches only isSchemaLine', () => {
    expect(isDataLine(validSchemaLine)).toBe(false)
    expect(isRelLine(validSchemaLine)).toBe(false)
    expect(isEventLine(validSchemaLine)).toBe(false)
    expect(isSchemaLine(validSchemaLine)).toBe(true)
  })
})

// =============================================================================
// serializeLine
// =============================================================================

describe('serializeLine', () => {
  it('produces a string', () => {
    const result = serializeLine(validDataLineCreate)
    expect(typeof result).toBe('string')
  })

  it('produces valid JSON (ignoring trailing newline)', () => {
    const result = serializeLine(validDataLineCreate)
    expect(() => JSON.parse(result.trimEnd())).not.toThrow()
  })

  it('appends a trailing newline', () => {
    const result = serializeLine(validDataLineCreate)
    expect(result.endsWith('\n')).toBe(true)
  })

  it('has exactly one trailing newline', () => {
    const result = serializeLine(validDataLineCreate)
    expect(result.endsWith('\n\n')).toBe(false)
  })

  it('does not contain embedded newlines within the JSON', () => {
    const lineWithNewlineInData: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      bio: 'line one\nline two\nline three',
    }
    const result = serializeLine(lineWithNewlineInData)
    // Only the trailing newline should be present; embedded ones are escaped
    const withoutTrailing = result.slice(0, -1)
    expect(withoutTrailing.includes('\n')).toBe(false)
  })

  it('omits undefined values', () => {
    const line: EventLine = {
      id: 'e1',
      ts: 1000,
      op: 'c',
      ns: 'users',
      eid: 'u1',
      before: undefined,
      after: { name: 'Alice' },
      actor: undefined,
    }
    const result = serializeLine(line)
    const parsed = JSON.parse(result)
    expect(parsed).not.toHaveProperty('before')
    expect(parsed).not.toHaveProperty('actor')
    expect(parsed).toHaveProperty('after')
  })

  it('serializes all four line types without error', () => {
    expect(() => serializeLine(validDataLineCreate)).not.toThrow()
    expect(() => serializeLine(validRelLineLink)).not.toThrow()
    expect(() => serializeLine(validEventLine)).not.toThrow()
    expect(() => serializeLine(validSchemaLine)).not.toThrow()
  })

  it('preserves numeric values', () => {
    const result = serializeLine(validDataLineCreate)
    const parsed = JSON.parse(result)
    expect(parsed.$v).toBe(1)
    expect(parsed.$ts).toBe(1738857600000)
  })

  it('preserves nested objects in data fields', () => {
    const line: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      address: { city: 'Portland', state: 'OR', zip: '97201' },
    }
    const result = serializeLine(line)
    const parsed = JSON.parse(result)
    expect(parsed.address).toEqual({ city: 'Portland', state: 'OR', zip: '97201' })
  })

  it('preserves arrays in data fields', () => {
    const line: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      tags: ['admin', 'user'],
    }
    const result = serializeLine(line)
    const parsed = JSON.parse(result)
    expect(parsed.tags).toEqual(['admin', 'user'])
  })
})

// =============================================================================
// deserializeLine
// =============================================================================

describe('deserializeLine', () => {
  it('deserializes a valid JSON string to a Line', () => {
    const json = '{"$id":"u1","$op":"c","$v":1,"$ts":1000}'
    const result = deserializeLine(json)
    expect(result.$op).toBe('c')
    expect((result as DataLine).$id).toBe('u1')
  })

  it('handles trailing newline', () => {
    const json = '{"$id":"u1","$op":"c","$v":1,"$ts":1000}\n'
    const result = deserializeLine(json)
    expect((result as DataLine).$id).toBe('u1')
  })

  it('handles trailing carriage return + newline', () => {
    const json = '{"$id":"u1","$op":"c","$v":1,"$ts":1000}\r\n'
    const result = deserializeLine(json)
    expect((result as DataLine).$id).toBe('u1')
  })

  it('handles trailing whitespace', () => {
    const json = '{"$id":"u1","$op":"c","$v":1,"$ts":1000}   \n'
    const result = deserializeLine(json)
    expect((result as DataLine).$id).toBe('u1')
  })

  it('handles no trailing newline', () => {
    const json = '{"$id":"u1","$op":"c","$v":1,"$ts":1000}'
    const result = deserializeLine(json)
    expect((result as DataLine).$id).toBe('u1')
  })

  it('throws on invalid JSON', () => {
    expect(() => deserializeLine('not json')).toThrow()
  })

  it('throws on empty string', () => {
    expect(() => deserializeLine('')).toThrow()
  })

  it('throws on whitespace-only string', () => {
    expect(() => deserializeLine('   \n')).toThrow()
  })

  it('deserializes a RelLine', () => {
    const json = '{"$op":"l","$ts":1000,"f":"u1","p":"author","r":"posts","t":"p1"}\n'
    const result = deserializeLine(json)
    expect(isRelLine(result)).toBe(true)
    expect((result as RelLine).f).toBe('u1')
    expect((result as RelLine).t).toBe('p1')
  })

  it('deserializes an EventLine', () => {
    const json = '{"id":"e1","ts":1000,"op":"c","ns":"users","eid":"u1","after":{"name":"Alice"}}\n'
    const result = deserializeLine(json)
    expect(isEventLine(result)).toBe(true)
    expect((result as EventLine).eid).toBe('u1')
  })

  it('deserializes a SchemaLine', () => {
    const json = '{"id":"s1","ts":1000,"op":"s","ns":"users","schema":{"name":"string"}}\n'
    const result = deserializeLine(json)
    expect(isSchemaLine(result)).toBe(true)
    expect((result as SchemaLine).schema).toEqual({ name: 'string' })
  })
})

// =============================================================================
// Roundtrip: serializeLine -> deserializeLine
// =============================================================================

describe('serializeLine / deserializeLine roundtrip', () => {
  it('roundtrips a create DataLine', () => {
    const deserialized = deserializeLine(serializeLine(validDataLineCreate))
    expect(deserialized).toEqual(validDataLineCreate)
  })

  it('roundtrips an update DataLine', () => {
    const deserialized = deserializeLine(serializeLine(validDataLineUpdate))
    expect(deserialized).toEqual(validDataLineUpdate)
  })

  it('roundtrips a delete DataLine', () => {
    const deserialized = deserializeLine(serializeLine(validDataLineDelete))
    expect(deserialized).toEqual(validDataLineDelete)
  })

  it('roundtrips a link RelLine', () => {
    const deserialized = deserializeLine(serializeLine(validRelLineLink))
    expect(deserialized).toEqual(validRelLineLink)
  })

  it('roundtrips an unlink RelLine', () => {
    const deserialized = deserializeLine(serializeLine(validRelLineUnlink))
    expect(deserialized).toEqual(validRelLineUnlink)
  })

  it('roundtrips a create EventLine', () => {
    const deserialized = deserializeLine(serializeLine(validEventLine))
    expect(deserialized).toEqual(validEventLine)
  })

  it('roundtrips an update EventLine with before/after', () => {
    const deserialized = deserializeLine(serializeLine(validEventLineUpdate))
    expect(deserialized).toEqual(validEventLineUpdate)
  })

  it('roundtrips a delete EventLine with before', () => {
    const deserialized = deserializeLine(serializeLine(validEventLineDelete))
    expect(deserialized).toEqual(validEventLineDelete)
  })

  it('roundtrips a SchemaLine', () => {
    const deserialized = deserializeLine(serializeLine(validSchemaLine))
    expect(deserialized).toEqual(validSchemaLine)
  })

  it('roundtrips a SchemaLine with migration', () => {
    const deserialized = deserializeLine(serializeLine(validSchemaLineWithMigration))
    expect(deserialized).toEqual(validSchemaLineWithMigration)
  })

  it('preserves type guard correctness after roundtrip', () => {
    const allLines: Line[] = [
      validDataLineCreate,
      validRelLineLink,
      validEventLine,
      validSchemaLine,
    ]

    for (const original of allLines) {
      const roundtripped = deserializeLine(serializeLine(original))

      // The same type guard should match before and after roundtrip
      expect(isDataLine(roundtripped)).toBe(isDataLine(original))
      expect(isRelLine(roundtripped)).toBe(isRelLine(original))
      expect(isEventLine(roundtripped)).toBe(isEventLine(original))
      expect(isSchemaLine(roundtripped)).toBe(isSchemaLine(original))
    }
  })

  it('roundtrips DataLine with complex nested data', () => {
    const complex: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      name: 'Alice',
      metadata: {
        nested: { deep: { value: [1, 2, 3] } },
        nullField: null,
        boolField: true,
        numField: 3.14159,
      },
    }
    const deserialized = deserializeLine(serializeLine(complex))
    expect(deserialized).toEqual(complex)
  })

  it('roundtrips DataLine with special characters in string values', () => {
    const special: DataLine = {
      $id: 'u1',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      bio: 'line1\nline2\ttab\r\nwindows',
      emoji: 'hello \u{1F600}',
      quotes: 'she said "hello"',
      backslash: 'path\\to\\file',
    }
    const deserialized = deserializeLine(serializeLine(special))
    expect(deserialized).toEqual(special)
  })
})

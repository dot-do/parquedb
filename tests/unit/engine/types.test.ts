/**
 * JSONL Types & Serialization Tests
 *
 * Tests for the MergeTree engine's JSONL line types:
 * - DataLine: entity mutations (create/update/delete)
 * - RelLine: relationship mutations (link/unlink)
 * - EventLine: CDC/audit events
 * - SchemaLine: schema definitions/migrations
 *
 * Also tests serialization/deserialization and type guards.
 */

import { describe, it, expect } from 'vitest'
import type { DataLine, RelLine, EventLine, SchemaLine, Line, Migration } from '@/engine/types'
import {
  serializeLine,
  deserializeLine,
  isDataLine,
  isRelLine,
  isEventLine,
  isSchemaLine,
} from '@/engine/jsonl'

// =============================================================================
// Test Fixtures
// =============================================================================

const NOW = 1738857600000

const createDataLine: DataLine = {
  $id: '01J5AAAA0000000000000001',
  $op: 'c',
  $v: 1,
  $ts: NOW,
  name: 'Alice',
  email: 'alice@a.co',
}

const updateDataLine: DataLine = {
  $id: '01J5AAAA0000000000000001',
  $op: 'u',
  $v: 2,
  $ts: NOW + 50,
  name: 'Alice Smith',
  email: 'alice@a.co',
}

const deleteDataLine: DataLine = {
  $id: '01J5BBBB0000000000000002',
  $op: 'd',
  $v: 2,
  $ts: NOW + 99,
}

const linkRelLine: RelLine = {
  $op: 'l',
  $ts: NOW,
  f: '01J5AAAA0000000000000001',
  p: 'author',
  r: 'posts',
  t: '01J5XXXX0000000000000010',
}

const unlinkRelLine: RelLine = {
  $op: 'u',
  $ts: NOW + 50,
  f: '01J5AAAA0000000000000001',
  p: 'author',
  r: 'posts',
  t: '01J5XXXX0000000000000010',
}

const createEventLine: EventLine = {
  id: '01J5ZZZZ0000000000000020',
  ts: NOW,
  op: 'c',
  ns: 'users',
  eid: '01J5AAAA0000000000000001',
  after: { name: 'Alice', email: 'alice@a.co' },
}

const updateEventLine: EventLine = {
  id: '01J5ZZZZ0000000000000021',
  ts: NOW + 50,
  op: 'u',
  ns: 'users',
  eid: '01J5AAAA0000000000000001',
  before: { name: 'Alice' },
  after: { name: 'Alice Smith' },
  actor: 'user:admin',
}

const deleteEventLine: EventLine = {
  id: '01J5ZZZZ0000000000000022',
  ts: NOW + 99,
  op: 'd',
  ns: 'users',
  eid: '01J5BBBB0000000000000002',
  before: { name: 'Bob' },
}

const initialSchemaLine: SchemaLine = {
  id: '01J5SSSS0000000000000030',
  ts: NOW,
  op: 's',
  ns: 'users',
  schema: { name: 'string', email: 'string', age: 'int' },
}

const migrationSchemaLine: SchemaLine = {
  id: '01J5SSSS0000000000000031',
  ts: NOW + 100000,
  op: 's',
  ns: 'users',
  schema: { name: 'string', email: 'string', age: 'int', role: 'string?' },
  migration: {
    added: ['role'],
    default: { role: 'member' },
  },
}

// =============================================================================
// DataLine Type Tests
// =============================================================================

describe('DataLine', () => {
  it('should represent a create operation', () => {
    expect(createDataLine.$id).toBe('01J5AAAA0000000000000001')
    expect(createDataLine.$op).toBe('c')
    expect(createDataLine.$v).toBe(1)
    expect(createDataLine.$ts).toBe(NOW)
    expect(createDataLine.name).toBe('Alice')
    expect(createDataLine.email).toBe('alice@a.co')
  })

  it('should represent an update operation', () => {
    expect(updateDataLine.$op).toBe('u')
    expect(updateDataLine.$v).toBe(2)
    expect(updateDataLine.name).toBe('Alice Smith')
  })

  it('should represent a delete operation with no extra fields', () => {
    expect(deleteDataLine.$op).toBe('d')
    expect(deleteDataLine.$v).toBe(2)
    // Delete lines should only have $id, $op, $v, $ts - no entity fields
    const keys = Object.keys(deleteDataLine)
    expect(keys).toEqual(['$id', '$op', '$v', '$ts'])
  })
})

// =============================================================================
// RelLine Type Tests
// =============================================================================

describe('RelLine', () => {
  it('should represent a link operation', () => {
    expect(linkRelLine.$op).toBe('l')
    expect(linkRelLine.f).toBe('01J5AAAA0000000000000001')
    expect(linkRelLine.p).toBe('author')
    expect(linkRelLine.r).toBe('posts')
    expect(linkRelLine.t).toBe('01J5XXXX0000000000000010')
    expect(linkRelLine.$ts).toBe(NOW)
  })

  it('should represent an unlink operation', () => {
    expect(unlinkRelLine.$op).toBe('u')
    expect(unlinkRelLine.$ts).toBe(NOW + 50)
    expect(unlinkRelLine.f).toBe('01J5AAAA0000000000000001')
    expect(unlinkRelLine.t).toBe('01J5XXXX0000000000000010')
  })
})

// =============================================================================
// EventLine Type Tests
// =============================================================================

describe('EventLine', () => {
  it('should represent a create event', () => {
    expect(createEventLine.id).toBe('01J5ZZZZ0000000000000020')
    expect(createEventLine.ts).toBe(NOW)
    expect(createEventLine.op).toBe('c')
    expect(createEventLine.ns).toBe('users')
    expect(createEventLine.eid).toBe('01J5AAAA0000000000000001')
    expect(createEventLine.after).toEqual({ name: 'Alice', email: 'alice@a.co' })
    expect(createEventLine.before).toBeUndefined()
    expect(createEventLine.actor).toBeUndefined()
  })

  it('should represent an update event with before/after and actor', () => {
    expect(updateEventLine.op).toBe('u')
    expect(updateEventLine.before).toEqual({ name: 'Alice' })
    expect(updateEventLine.after).toEqual({ name: 'Alice Smith' })
    expect(updateEventLine.actor).toBe('user:admin')
  })

  it('should represent a delete event with only before state', () => {
    expect(deleteEventLine.op).toBe('d')
    expect(deleteEventLine.before).toEqual({ name: 'Bob' })
    expect(deleteEventLine.after).toBeUndefined()
  })
})

// =============================================================================
// SchemaLine Type Tests
// =============================================================================

describe('SchemaLine', () => {
  it('should represent an initial schema definition', () => {
    expect(initialSchemaLine.op).toBe('s')
    expect(initialSchemaLine.ns).toBe('users')
    expect(initialSchemaLine.schema).toEqual({ name: 'string', email: 'string', age: 'int' })
    expect(initialSchemaLine.migration).toBeUndefined()
  })

  it('should represent a schema migration with added fields and defaults', () => {
    expect(migrationSchemaLine.op).toBe('s')
    expect(migrationSchemaLine.schema).toEqual({
      name: 'string', email: 'string', age: 'int', role: 'string?',
    })
    expect(migrationSchemaLine.migration).toBeDefined()
    expect(migrationSchemaLine.migration!.added).toEqual(['role'])
    expect(migrationSchemaLine.migration!.default).toEqual({ role: 'member' })
  })

  it('should support all migration fields', () => {
    const fullMigration: Migration = {
      added: ['status'],
      dropped: ['legacy_field'],
      renamed: { old_name: 'new_name' },
      changed: { age: 'bigint' },
      default: { status: 'active' },
    }
    expect(fullMigration.added).toEqual(['status'])
    expect(fullMigration.dropped).toEqual(['legacy_field'])
    expect(fullMigration.renamed).toEqual({ old_name: 'new_name' })
    expect(fullMigration.changed).toEqual({ age: 'bigint' })
    expect(fullMigration.default).toEqual({ status: 'active' })
  })
})

// =============================================================================
// Serialization Tests
// =============================================================================

describe('serializeLine', () => {
  it('should produce a valid JSON string with trailing newline', () => {
    const result = serializeLine(createDataLine)
    expect(result.endsWith('\n')).toBe(true)
    // Should be parseable JSON (without the trailing newline)
    expect(() => JSON.parse(result.trimEnd())).not.toThrow()
  })

  it('should not contain embedded newlines within the JSON', () => {
    const result = serializeLine(createDataLine)
    // Strip the trailing newline and check there are no others
    const jsonPart = result.slice(0, -1)
    expect(jsonPart.includes('\n')).toBe(false)
    expect(jsonPart.includes('\r')).toBe(false)
  })

  it('should serialize a DataLine create correctly', () => {
    const result = serializeLine(createDataLine)
    const parsed = JSON.parse(result)
    expect(parsed.$id).toBe('01J5AAAA0000000000000001')
    expect(parsed.$op).toBe('c')
    expect(parsed.$v).toBe(1)
    expect(parsed.$ts).toBe(NOW)
    expect(parsed.name).toBe('Alice')
    expect(parsed.email).toBe('alice@a.co')
  })

  it('should serialize a DataLine delete with only system fields', () => {
    const result = serializeLine(deleteDataLine)
    const parsed = JSON.parse(result)
    expect(Object.keys(parsed)).toEqual(['$id', '$op', '$v', '$ts'])
  })

  it('should serialize a RelLine link correctly', () => {
    const result = serializeLine(linkRelLine)
    const parsed = JSON.parse(result)
    expect(parsed.$op).toBe('l')
    expect(parsed.f).toBe('01J5AAAA0000000000000001')
    expect(parsed.p).toBe('author')
    expect(parsed.r).toBe('posts')
    expect(parsed.t).toBe('01J5XXXX0000000000000010')
  })

  it('should serialize an EventLine correctly', () => {
    const result = serializeLine(updateEventLine)
    const parsed = JSON.parse(result)
    expect(parsed.id).toBe('01J5ZZZZ0000000000000021')
    expect(parsed.op).toBe('u')
    expect(parsed.ns).toBe('users')
    expect(parsed.before).toEqual({ name: 'Alice' })
    expect(parsed.after).toEqual({ name: 'Alice Smith' })
    expect(parsed.actor).toBe('user:admin')
  })

  it('should serialize a SchemaLine with migration correctly', () => {
    const result = serializeLine(migrationSchemaLine)
    const parsed = JSON.parse(result)
    expect(parsed.op).toBe('s')
    expect(parsed.ns).toBe('users')
    expect(parsed.schema).toEqual({ name: 'string', email: 'string', age: 'int', role: 'string?' })
    expect(parsed.migration.added).toEqual(['role'])
    expect(parsed.migration.default).toEqual({ role: 'member' })
  })

  it('should omit undefined optional fields', () => {
    const result = serializeLine(createEventLine)
    const parsed = JSON.parse(result)
    expect('before' in parsed).toBe(false)
    expect('actor' in parsed).toBe(false)
  })
})

// =============================================================================
// Deserialization Tests
// =============================================================================

describe('deserializeLine', () => {
  it('should roundtrip a DataLine create', () => {
    const serialized = serializeLine(createDataLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(createDataLine)
  })

  it('should roundtrip a DataLine update', () => {
    const serialized = serializeLine(updateDataLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(updateDataLine)
  })

  it('should roundtrip a DataLine delete', () => {
    const serialized = serializeLine(deleteDataLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(deleteDataLine)
  })

  it('should roundtrip a RelLine link', () => {
    const serialized = serializeLine(linkRelLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(linkRelLine)
  })

  it('should roundtrip a RelLine unlink', () => {
    const serialized = serializeLine(unlinkRelLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(unlinkRelLine)
  })

  it('should roundtrip an EventLine', () => {
    const serialized = serializeLine(updateEventLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(updateEventLine)
  })

  it('should roundtrip a SchemaLine', () => {
    const serialized = serializeLine(migrationSchemaLine)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(migrationSchemaLine)
  })

  it('should handle strings with trailing newline', () => {
    const json = '{"$id":"01J5A","$op":"c","$v":1,"$ts":1738857600000,"name":"Test"}\n'
    const result = deserializeLine(json)
    expect(result.$id).toBe('01J5A')
  })

  it('should handle strings without trailing newline', () => {
    const json = '{"$id":"01J5A","$op":"c","$v":1,"$ts":1738857600000,"name":"Test"}'
    const result = deserializeLine(json)
    expect((result as DataLine).$id).toBe('01J5A')
  })
})

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('isDataLine', () => {
  it('should return true for DataLine create', () => {
    expect(isDataLine(createDataLine)).toBe(true)
  })

  it('should return true for DataLine update', () => {
    expect(isDataLine(updateDataLine)).toBe(true)
  })

  it('should return true for DataLine delete', () => {
    expect(isDataLine(deleteDataLine)).toBe(true)
  })

  it('should return false for RelLine', () => {
    expect(isDataLine(linkRelLine)).toBe(false)
  })

  it('should return false for EventLine', () => {
    expect(isDataLine(createEventLine)).toBe(false)
  })

  it('should return false for SchemaLine', () => {
    expect(isDataLine(initialSchemaLine)).toBe(false)
  })
})

describe('isRelLine', () => {
  it('should return true for RelLine link', () => {
    expect(isRelLine(linkRelLine)).toBe(true)
  })

  it('should return true for RelLine unlink', () => {
    expect(isRelLine(unlinkRelLine)).toBe(true)
  })

  it('should return false for DataLine', () => {
    expect(isRelLine(createDataLine)).toBe(false)
  })

  it('should return false for EventLine', () => {
    expect(isRelLine(createEventLine)).toBe(false)
  })

  it('should return false for SchemaLine', () => {
    expect(isRelLine(initialSchemaLine)).toBe(false)
  })
})

describe('isEventLine', () => {
  it('should return true for EventLine create', () => {
    expect(isEventLine(createEventLine)).toBe(true)
  })

  it('should return true for EventLine update', () => {
    expect(isEventLine(updateEventLine)).toBe(true)
  })

  it('should return true for EventLine delete', () => {
    expect(isEventLine(deleteEventLine)).toBe(true)
  })

  it('should return false for DataLine', () => {
    expect(isEventLine(createDataLine)).toBe(false)
  })

  it('should return false for RelLine', () => {
    expect(isEventLine(linkRelLine)).toBe(false)
  })

  it('should return false for SchemaLine', () => {
    expect(isEventLine(initialSchemaLine)).toBe(false)
  })
})

describe('isSchemaLine', () => {
  it('should return true for SchemaLine without migration', () => {
    expect(isSchemaLine(initialSchemaLine)).toBe(true)
  })

  it('should return true for SchemaLine with migration', () => {
    expect(isSchemaLine(migrationSchemaLine)).toBe(true)
  })

  it('should return false for DataLine', () => {
    expect(isSchemaLine(createDataLine)).toBe(false)
  })

  it('should return false for RelLine', () => {
    expect(isSchemaLine(linkRelLine)).toBe(false)
  })

  it('should return false for EventLine', () => {
    expect(isSchemaLine(createEventLine)).toBe(false)
  })
})

// =============================================================================
// Edge Case Tests
// =============================================================================

describe('Edge cases', () => {
  it('should handle entity fields with special characters', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000001',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      bio: 'She said "hello" & waved <goodbye>',
      path: 'C:\\Users\\alice\\docs',
      url: 'https://example.com/path?q=1&r=2',
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle unicode characters', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000002',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      name: 'Taro Yamada',
      greeting: 'Hola, como estas?',
      emoji: 'Hello world! \ud83c\udf0d',
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle nested objects in entity fields', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000003',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      profile: {
        address: {
          street: '123 Main St',
          city: 'Anytown',
        },
        tags: ['admin', 'user'],
      },
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle empty string fields', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000004',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      name: '',
      description: '',
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle numeric and boolean entity fields', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000005',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      age: 42,
      score: 99.5,
      active: true,
      deleted: false,
      balance: 0,
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle null values in entity fields', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000006',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      name: 'Alice',
      middleName: null,
    }
    const serialized = serializeLine(line)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(line)
  })

  it('should handle fields containing newline characters in values', () => {
    const line: DataLine = {
      $id: '01J5EDGE0000000000000007',
      $op: 'c',
      $v: 1,
      $ts: NOW,
      bio: 'Line 1\nLine 2\nLine 3',
      note: 'Tab\there\rand carriage return',
    }
    const serialized = serializeLine(line)
    // The serialized form should not have raw newlines (JSON.stringify escapes them)
    const jsonPart = serialized.slice(0, -1)
    expect(jsonPart.includes('\n')).toBe(false)
    expect(jsonPart.includes('\r')).toBe(false)
    // But deserialized values should preserve them
    const deserialized = deserializeLine(serialized) as DataLine
    expect(deserialized.bio).toBe('Line 1\nLine 2\nLine 3')
    expect(deserialized.note).toBe('Tab\there\rand carriage return')
  })

  it('should correctly distinguish RelLine unlink from DataLine update (both $op "u")', () => {
    // RelLine unlink has $op: 'u' -- same string as DataLine update $op: 'u'
    // They are distinguished by their structure, not $op value alone
    const relUnlink: RelLine = { $op: 'u', $ts: NOW, f: 'id1', p: 'pred', r: 'rev', t: 'id2' }
    const dataUpdate: DataLine = { $id: 'id1', $op: 'u', $v: 2, $ts: NOW, name: 'Updated' }

    expect(isRelLine(relUnlink)).toBe(true)
    expect(isDataLine(relUnlink)).toBe(false)
    expect(isDataLine(dataUpdate)).toBe(true)
    expect(isRelLine(dataUpdate)).toBe(false)
  })

  it('should roundtrip EventLine with all optional fields present', () => {
    const fullEvent: EventLine = {
      id: '01J5FULL0000000000000001',
      ts: NOW,
      op: 'u',
      ns: 'orders',
      eid: '01J5ORDER000000000000001',
      before: { status: 'pending', total: 100 },
      after: { status: 'shipped', total: 100 },
      actor: 'system:cron',
    }
    const serialized = serializeLine(fullEvent)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(fullEvent)
  })

  it('should roundtrip SchemaLine with full migration', () => {
    const schema: SchemaLine = {
      id: '01J5MIGR0000000000000001',
      ts: NOW,
      op: 's',
      ns: 'products',
      schema: { name: 'string', price: 'float', sku: 'string', category: 'string' },
      migration: {
        added: ['category'],
        dropped: ['legacy_code'],
        renamed: { old_sku: 'sku' },
        changed: { price: 'float' },
        default: { category: 'uncategorized' },
      },
    }
    const serialized = serializeLine(schema)
    const deserialized = deserializeLine(serialized)
    expect(deserialized).toEqual(schema)
  })
})

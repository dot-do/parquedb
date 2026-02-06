/**
 * Parquet Encoders Test Suite
 *
 * Tests the canonical encoding functions in parquet-encoders.ts.
 * These are the single source of truth for encoding data, rels, and events
 * to Parquet buffers. All other modules (compaction-worker, parquet-adapter,
 * format plugins) should import from this module.
 *
 * Verifies:
 * - All 3 encoding functions produce valid Parquet (PAR1 magic bytes)
 * - Roundtrip encoding/decoding preserves data correctly
 * - Edge cases (empty input, missing fields, nested data)
 */

import { describe, it, expect } from 'vitest'
import {
  encodeDataToParquet,
  encodeRelsToParquet,
  encodeEventsToParquet,
} from '@/engine/parquet-encoders'
import type { DataLine, RelLine } from '@/engine/types'
import { makeLine, decodeParquet, toNumber } from './helpers'

// =============================================================================
// Helpers
// =============================================================================

/** Verify a buffer starts with PAR1 magic bytes */
function expectParquetMagic(buffer: ArrayBuffer): void {
  const view = new Uint8Array(buffer)
  expect(view[0]).toBe(0x50) // P
  expect(view[1]).toBe(0x41) // A
  expect(view[2]).toBe(0x52) // R
  expect(view[3]).toBe(0x31) // 1
}

// =============================================================================
// encodeDataToParquet
// =============================================================================

describe('parquet-encoders: encodeDataToParquet', () => {
  it('1. produces a valid Parquet buffer with PAR1 magic bytes', async () => {
    const data = [makeLine({ $id: 'u1', $ts: 1000, name: 'Alice' })]
    const buffer = await encodeDataToParquet(data)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
    expectParquetMagic(buffer)
  })

  it('2. roundtrips data entities correctly', async () => {
    const data = [
      makeLine({ $id: 'u2', $ts: 2000, name: 'Bob', age: 30 }),
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', age: 25 }),
      makeLine({ $id: 'u3', $op: 'd', $v: 2, $ts: 3000 }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    // Sorted by $id
    expect(rows).toHaveLength(3)
    expect(rows[0].$id).toBe('u1')
    expect(rows[1].$id).toBe('u2')
    expect(rows[2].$id).toBe('u3')

    // System fields preserved
    expect(rows[0].$op).toBe('c')
    expect(rows[0].$v).toBe(1)
    expect(toNumber(rows[0].$ts)).toBe(1000)

    // Data fields packed into $data JSON
    const u1Data = JSON.parse(rows[0].$data as string)
    expect(u1Data.name).toBe('Alice')
    expect(u1Data.age).toBe(25)

    const u2Data = JSON.parse(rows[1].$data as string)
    expect(u2Data.name).toBe('Bob')
    expect(u2Data.age).toBe(30)

    // Delete op has empty data
    const u3Data = JSON.parse(rows[2].$data as string)
    expect(Object.keys(u3Data)).toHaveLength(0)
  })

  it('3. preserves nested data fields (objects and arrays)', async () => {
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        name: 'Alice',
        address: { street: '123 Main St', city: 'Springfield' },
        tags: ['admin', 'user'],
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(1)
    const parsed = JSON.parse(rows[0].$data as string)
    expect(parsed.name).toBe('Alice')
    expect(parsed.address).toEqual({ street: '123 Main St', city: 'Springfield' })
    expect(parsed.tags).toEqual(['admin', 'user'])
  })

  it('4. handles empty input array', async () => {
    const buffer = await encodeDataToParquet([])
    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
    expectParquetMagic(buffer)

    const rows = await decodeParquet(buffer)
    expect(rows).toHaveLength(0)
  })

  it('5. stores $ts as DOUBLE (not INT32) to preserve precision', async () => {
    const ts = 1738857600000 // Large timestamp
    const data = [makeLine({ $id: 'u1', $ts: ts })]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    expect(toNumber(rows[0].$ts)).toBe(ts)
  })
})

// =============================================================================
// encodeRelsToParquet
// =============================================================================

describe('parquet-encoders: encodeRelsToParquet', () => {
  it('6. produces a valid Parquet buffer with PAR1 magic bytes', async () => {
    const rels: RelLine[] = [
      { $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' },
    ]

    const buffer = await encodeRelsToParquet(rels)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
    expectParquetMagic(buffer)
  })

  it('7. roundtrips relationship data correctly', async () => {
    const rels: RelLine[] = [
      { $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' },
      { $op: 'u', $ts: 2000, f: 'u1', p: 'author', r: 'posts', t: 'p2' },
      { $op: 'l', $ts: 3000, f: 'u2', p: 'follows', r: 'followers', t: 'u1' },
    ]

    const buffer = await encodeRelsToParquet(rels)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(3)

    expect(rows[0].$op).toBe('l')
    expect(toNumber(rows[0].$ts)).toBe(1000)
    expect(rows[0].f).toBe('u1')
    expect(rows[0].p).toBe('author')
    expect(rows[0].r).toBe('posts')
    expect(rows[0].t).toBe('p1')

    expect(rows[1].$op).toBe('u')
    expect(toNumber(rows[1].$ts)).toBe(2000)
    expect(rows[1].t).toBe('p2')

    expect(rows[2].f).toBe('u2')
    expect(rows[2].p).toBe('follows')
    expect(rows[2].r).toBe('followers')
  })

  it('8. handles empty rels array', async () => {
    const buffer = await encodeRelsToParquet([])
    expectParquetMagic(buffer)

    const rows = await decodeParquet(buffer)
    expect(rows).toHaveLength(0)
  })
})

// =============================================================================
// encodeEventsToParquet
// =============================================================================

describe('parquet-encoders: encodeEventsToParquet', () => {
  it('9. produces a valid Parquet buffer with PAR1 magic bytes', async () => {
    const events = [
      { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' }, actor: 'system' },
    ]

    const buffer = await encodeEventsToParquet(events)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)
    expectParquetMagic(buffer)
  })

  it('10. roundtrips event data correctly', async () => {
    const events = [
      {
        id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1',
        after: { name: 'Alice', age: 25 }, actor: 'admin',
      },
      {
        id: 'e2', ts: 2000, op: 'u', ns: 'users', eid: 'u1',
        before: { name: 'Alice' }, after: { name: 'Alice Smith' }, actor: 'admin',
      },
      {
        id: 'e3', ts: 3000, op: 'd', ns: 'posts', eid: 'p1',
        before: { title: 'Hello' },
      },
    ]

    const buffer = await encodeEventsToParquet(events)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(3)

    // Create event
    expect(rows[0].id).toBe('e1')
    expect(toNumber(rows[0].ts)).toBe(1000)
    expect(rows[0].op).toBe('c')
    expect(rows[0].ns).toBe('users')
    expect(rows[0].eid).toBe('u1')
    expect(JSON.parse(rows[0].after as string)).toEqual({ name: 'Alice', age: 25 })
    expect(rows[0].before).toBe('')
    expect(rows[0].actor).toBe('admin')

    // Update event with before and after
    expect(rows[1].id).toBe('e2')
    expect(JSON.parse(rows[1].before as string)).toEqual({ name: 'Alice' })
    expect(JSON.parse(rows[1].after as string)).toEqual({ name: 'Alice Smith' })

    // Delete event with no actor
    expect(rows[2].id).toBe('e3')
    expect(rows[2].op).toBe('d')
    expect(rows[2].actor).toBe('')
  })

  it('11. handles events with missing optional fields', async () => {
    const events = [
      { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' },
    ]

    const buffer = await encodeEventsToParquet(events)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(1)
    expect(rows[0].id).toBe('e1')
    expect(rows[0].before).toBe('')
    expect(rows[0].after).toBe('')
    expect(rows[0].actor).toBe('')
  })

  it('12. handles empty events array', async () => {
    const buffer = await encodeEventsToParquet([])
    expectParquetMagic(buffer)

    const rows = await decodeParquet(buffer)
    expect(rows).toHaveLength(0)
  })
})

// =============================================================================
// Cross-cutting concerns
// =============================================================================

describe('parquet-encoders: cross-cutting', () => {
  it('13. concurrent encoding calls produce independent valid results', async () => {
    const data = [makeLine({ $id: 'a1', $ts: 1000, name: 'First' })]
    const rels: RelLine[] = [{ $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' }]
    const events = [{ id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' }]

    const [dataBuf, relsBuf, eventsBuf] = await Promise.all([
      encodeDataToParquet(data),
      encodeRelsToParquet(rels),
      encodeEventsToParquet(events),
    ])

    // All produce valid Parquet
    for (const buf of [dataBuf, relsBuf, eventsBuf]) {
      expect(buf.byteLength).toBeGreaterThan(0)
      expectParquetMagic(buf)
    }

    // Verify distinct data in each
    const dataRows = await decodeParquet(dataBuf)
    expect(dataRows[0].$id).toBe('a1')

    const relsRows = await decodeParquet(relsBuf)
    expect(relsRows[0].f).toBe('u1')

    const eventsRows = await decodeParquet(eventsBuf)
    expect(eventsRows[0].id).toBe('e1')
  })

  it('14. encodeDataToParquet excludes only system fields from $data', async () => {
    // Verify that $id, $op, $v, $ts are NOT in $data but all other fields are
    const data = [
      makeLine({
        $id: 'u1',
        $ts: 1000,
        customField: 'yes',
        anotherField: 42,
      }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)
    const parsed = JSON.parse(rows[0].$data as string)

    // System fields should NOT appear in $data
    expect(parsed.$id).toBeUndefined()
    expect(parsed.$op).toBeUndefined()
    expect(parsed.$v).toBeUndefined()
    expect(parsed.$ts).toBeUndefined()

    // Custom fields should appear in $data
    expect(parsed.customField).toBe('yes')
    expect(parsed.anotherField).toBe(42)
  })
})

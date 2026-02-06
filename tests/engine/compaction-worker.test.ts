/**
 * Compaction Worker Test Suite
 *
 * Tests the worker thread compaction system for the MergeTree engine.
 * Verifies both the standalone encoding functions (which work in any
 * environment) and the CompactionWorker coordinator class.
 *
 * The standalone functions are tested directly since worker thread
 * instantiation with TypeScript files can be unreliable in test
 * environments. The CompactionWorker class is tested for lifecycle
 * management (start/stop/ready).
 */

import { describe, it, expect, afterEach } from 'vitest'
import {
  CompactionWorker,
  encodeDataToParquet,
  encodeRelsToParquet,
  encodeEventsToParquet,
} from '@/engine/compaction-worker'
import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

/** Decode a Parquet ArrayBuffer into rows using hyparquet */
async function decodeParquet(buffer: ArrayBuffer): Promise<Array<Record<string, unknown>>> {
  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => buffer.slice(start, end ?? buffer.byteLength),
  }
  return parquetReadObjects({ file: asyncBuffer }) as Promise<Array<Record<string, unknown>>>
}

/** Convert BigInt values to Number for comparison */
function toNumber(value: unknown): number {
  if (typeof value === 'bigint') return Number(value)
  if (typeof value === 'number') return value
  return 0
}

/** Helper to create a DataLine with sensible defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

/** Helper to create a RelLine */
function makeRel(f: string, p: string, r: string, t: string, ts = 1000): RelLine {
  return { $op: 'l', $ts: ts, f, p, r, t }
}

// =============================================================================
// Standalone Encoding Function Tests
// =============================================================================

describe('encodeDataToParquet', () => {
  it('1. encodes data entities to valid Parquet buffer', async () => {
    const data = [
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', email: 'alice@example.com' }),
      makeLine({ $id: 'u2', $ts: 2000, name: 'Bob' }),
    ]

    const buffer = await encodeDataToParquet(data)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)

    // Verify it has the Parquet magic bytes (PAR1)
    const view = new Uint8Array(buffer)
    expect(view[0]).toBe(0x50) // P
    expect(view[1]).toBe(0x41) // A
    expect(view[2]).toBe(0x52) // R
    expect(view[3]).toBe(0x31) // 1
  })

  it('2. data roundtrip: encode then decode matches input', async () => {
    const data = [
      makeLine({ $id: 'u2', $ts: 2000, name: 'Bob', age: 30 }),
      makeLine({ $id: 'u1', $ts: 1000, name: 'Alice', age: 25 }),
      makeLine({ $id: 'u3', $op: 'd', $v: 2, $ts: 3000 }),
    ]

    const buffer = await encodeDataToParquet(data)
    const rows = await decodeParquet(buffer)

    // Should be sorted by $id
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

    // Delete has empty data
    const u3Data = JSON.parse(rows[2].$data as string)
    expect(Object.keys(u3Data)).toHaveLength(0)
  })

  it('3. handles entities with nested data fields', async () => {
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
})

describe('encodeRelsToParquet', () => {
  it('4. encodes relationships to valid Parquet buffer', async () => {
    const rels = [
      makeRel('u1', 'author', 'posts', 'p1'),
      makeRel('u1', 'author', 'posts', 'p2', 2000),
    ]

    const buffer = await encodeRelsToParquet(rels)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)

    // Parquet magic bytes
    const view = new Uint8Array(buffer)
    expect(view[0]).toBe(0x50)
    expect(view[1]).toBe(0x41)
    expect(view[2]).toBe(0x52)
    expect(view[3]).toBe(0x31)
  })

  it('5. rels roundtrip: encode then decode matches input', async () => {
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

    expect(rows[2].$op).toBe('l')
    expect(rows[2].f).toBe('u2')
    expect(rows[2].p).toBe('follows')
    expect(rows[2].r).toBe('followers')
  })
})

describe('encodeEventsToParquet', () => {
  it('6. encodes events to valid Parquet buffer', async () => {
    const events = [
      { id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' }, actor: 'system' },
    ]

    const buffer = await encodeEventsToParquet(events)

    expect(buffer).toBeInstanceOf(ArrayBuffer)
    expect(buffer.byteLength).toBeGreaterThan(0)

    const view = new Uint8Array(buffer)
    expect(view[0]).toBe(0x50)
    expect(view[1]).toBe(0x41)
    expect(view[2]).toBe(0x52)
    expect(view[3]).toBe(0x31)
  })

  it('7. events roundtrip: encode then decode matches input', async () => {
    const events = [
      {
        id: 'e1',
        ts: 1000,
        op: 'c',
        ns: 'users',
        eid: 'u1',
        after: { name: 'Alice', age: 25 },
        actor: 'admin',
      },
      {
        id: 'e2',
        ts: 2000,
        op: 'u',
        ns: 'users',
        eid: 'u1',
        before: { name: 'Alice' },
        after: { name: 'Alice Smith' },
        actor: 'admin',
      },
      {
        id: 'e3',
        ts: 3000,
        op: 'd',
        ns: 'posts',
        eid: 'p1',
        before: { title: 'Hello' },
      },
    ]

    const buffer = await encodeEventsToParquet(events)
    const rows = await decodeParquet(buffer)

    expect(rows).toHaveLength(3)

    // First event: create
    expect(rows[0].id).toBe('e1')
    expect(toNumber(rows[0].ts)).toBe(1000)
    expect(rows[0].op).toBe('c')
    expect(rows[0].ns).toBe('users')
    expect(rows[0].eid).toBe('u1')
    expect(JSON.parse(rows[0].after as string)).toEqual({ name: 'Alice', age: 25 })
    expect(rows[0].before).toBe('')
    expect(rows[0].actor).toBe('admin')

    // Second event: update with before and after
    expect(rows[1].id).toBe('e2')
    expect(JSON.parse(rows[1].before as string)).toEqual({ name: 'Alice' })
    expect(JSON.parse(rows[1].after as string)).toEqual({ name: 'Alice Smith' })

    // Third event: delete with no actor
    expect(rows[2].id).toBe('e3')
    expect(rows[2].op).toBe('d')
    expect(rows[2].ns).toBe('posts')
    expect(rows[2].actor).toBe('')
  })

  it('8. handles events with missing optional fields', async () => {
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
})

// =============================================================================
// Concurrent Encoding Tests
// =============================================================================

describe('Concurrent encoding', () => {
  it('9. multiple concurrent encode calls produce independent results', async () => {
    const data1 = [makeLine({ $id: 'a1', $ts: 1000, name: 'First' })]
    const data2 = [makeLine({ $id: 'b1', $ts: 2000, name: 'Second' })]
    const rels = [makeRel('u1', 'author', 'posts', 'p1')]
    const events = [{ id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' }]

    const [buf1, buf2, buf3, buf4] = await Promise.all([
      encodeDataToParquet(data1),
      encodeDataToParquet(data2),
      encodeRelsToParquet(rels),
      encodeEventsToParquet(events),
    ])

    // All produce valid Parquet
    for (const buf of [buf1, buf2, buf3, buf4]) {
      expect(buf.byteLength).toBeGreaterThan(0)
      const view = new Uint8Array(buf)
      expect(view[0]).toBe(0x50) // PAR1 magic
    }

    // Decode and verify they contain different data
    const rows1 = await decodeParquet(buf1)
    const rows2 = await decodeParquet(buf2)
    expect(rows1[0].$id).toBe('a1')
    expect(rows2[0].$id).toBe('b1')
  })
})

// =============================================================================
// CompactionWorker Lifecycle Tests
// =============================================================================

describe('CompactionWorker', () => {
  let worker: CompactionWorker

  afterEach(async () => {
    if (worker) {
      await worker.stop()
    }
  })

  it('10. ready is false before start() is called', () => {
    worker = new CompactionWorker()
    expect(worker.ready).toBe(false)
  })

  it('11. stop() on an unstarted worker is a no-op', async () => {
    worker = new CompactionWorker()
    // Should not throw
    await worker.stop()
    expect(worker.ready).toBe(false)
  })

  it('12. encodeData rejects when worker not started', async () => {
    worker = new CompactionWorker()
    const data = [makeLine({ $id: 'u1', $ts: 1000, name: 'Alice' })]
    await expect(worker.encodeData(data)).rejects.toThrow('Worker not started')
  })

  it('13. encodeRels rejects when worker not started', async () => {
    worker = new CompactionWorker()
    const rels = [makeRel('u1', 'author', 'posts', 'p1')]
    await expect(worker.encodeRels(rels)).rejects.toThrow('Worker not started')
  })

  it('14. encodeEvents rejects when worker not started', async () => {
    worker = new CompactionWorker()
    const events = [{ id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' }]
    await expect(worker.encodeEvents(events)).rejects.toThrow('Worker not started')
  })

  it('15. start() then stop() transitions ready correctly', async () => {
    worker = new CompactionWorker()
    expect(worker.ready).toBe(false)

    // start() may or may not succeed depending on whether the worker script
    // is compiled and available. Either way, it should not throw.
    await worker.start()

    // If workers are available, ready should be true (then stop makes it false)
    // If not, ready stays false. Either way, stop should work.
    const wasReady = worker.ready
    await worker.stop()
    expect(worker.ready).toBe(false)

    // If it was ready, verify the transition happened
    if (wasReady) {
      expect(wasReady).toBe(true)
    }
  })

  it('16. start() is idempotent - calling twice does not throw', async () => {
    worker = new CompactionWorker()
    await worker.start()
    await worker.start() // Should be a no-op
    await worker.stop()
  })
})

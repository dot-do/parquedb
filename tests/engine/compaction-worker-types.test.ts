/**
 * Compaction Worker Type Tests
 *
 * Verifies that the CompactionRequest discriminated union narrows
 * the `payload` type correctly based on the `type` discriminant.
 *
 * These are compile-time type checks: if the types are wrong, the
 * file will fail to compile (and thus the test suite will fail).
 * Runtime assertions confirm the narrowing works at the value level too.
 */

import { describe, it, expect, expectTypeOf } from 'vitest'
import type { CompactionRequest } from '@/engine/compaction-worker'
import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Discriminated Union Narrowing Tests
// =============================================================================

describe('CompactionRequest discriminated union', () => {
  it('1. encode-data narrows payload to DataLine[]', () => {
    const req: CompactionRequest = {
      type: 'encode-data',
      id: 'req-1',
      payload: [{ $id: 'u1', $op: 'c' as const, $v: 1, $ts: 1000, name: 'Alice' }],
    }

    if (req.type === 'encode-data') {
      // TypeScript should narrow payload to DataLine[]
      expectTypeOf(req.payload).toEqualTypeOf<DataLine[]>()
      expect(req.payload[0].$id).toBe('u1')
    }
  })

  it('2. encode-rels narrows payload to RelLine[]', () => {
    const req: CompactionRequest = {
      type: 'encode-rels',
      id: 'req-2',
      payload: [{ $op: 'l' as const, $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' }],
    }

    if (req.type === 'encode-rels') {
      // TypeScript should narrow payload to RelLine[]
      expectTypeOf(req.payload).toEqualTypeOf<RelLine[]>()
      expect(req.payload[0].f).toBe('u1')
      expect(req.payload[0].p).toBe('author')
    }
  })

  it('3. encode-events narrows payload to Record<string, unknown>[]', () => {
    const req: CompactionRequest = {
      type: 'encode-events',
      id: 'req-3',
      payload: [{ id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' }],
    }

    if (req.type === 'encode-events') {
      // TypeScript should narrow payload to Record<string, unknown>[]
      expectTypeOf(req.payload).toEqualTypeOf<Record<string, unknown>[]>()
      expect(req.payload[0].id).toBe('e1')
    }
  })

  it('4. switch statement exhaustively narrows all variants', () => {
    function processRequest(req: CompactionRequest): string {
      switch (req.type) {
        case 'encode-data': {
          // payload is DataLine[]
          const firstId: string = req.payload[0]?.$id ?? 'none'
          return `data:${firstId}`
        }
        case 'encode-rels': {
          // payload is RelLine[]
          const predicate: string = req.payload[0]?.p ?? 'none'
          return `rels:${predicate}`
        }
        case 'encode-events': {
          // payload is Record<string, unknown>[]
          const eventId = (req.payload[0]?.id as string) ?? 'none'
          return `events:${eventId}`
        }
        default: {
          // Exhaustiveness check: if we reach here, the union is not exhaustive
          const _exhaustive: never = req
          return _exhaustive
        }
      }
    }

    expect(processRequest({
      type: 'encode-data',
      id: 'r1',
      payload: [{ $id: 'u1', $op: 'c', $v: 1, $ts: 1000 }],
    })).toBe('data:u1')

    expect(processRequest({
      type: 'encode-rels',
      id: 'r2',
      payload: [{ $op: 'l', $ts: 1000, f: 'u1', p: 'author', r: 'posts', t: 'p1' }],
    })).toBe('rels:author')

    expect(processRequest({
      type: 'encode-events',
      id: 'r3',
      payload: [{ id: 'e1', ts: 1000, op: 'c', ns: 'users', eid: 'u1' }],
    })).toBe('events:e1')
  })

  it('5. CompactionRequest is a union type, not a single interface', () => {
    // Verify it is a union -- assigning a mismatched payload should be caught
    // by TypeScript at compile time. At runtime, we verify the structure.
    const dataReq: CompactionRequest = {
      type: 'encode-data',
      id: 'r1',
      payload: [{ $id: 'u1', $op: 'c', $v: 1, $ts: 1000 }],
    }
    expect(dataReq.type).toBe('encode-data')

    const relsReq: CompactionRequest = {
      type: 'encode-rels',
      id: 'r2',
      payload: [{ $op: 'l', $ts: 1000, f: 'a', p: 'b', r: 'c', t: 'd' }],
    }
    expect(relsReq.type).toBe('encode-rels')

    const eventsReq: CompactionRequest = {
      type: 'encode-events',
      id: 'r3',
      payload: [{ id: 'e1' }],
    }
    expect(eventsReq.type).toBe('encode-events')
  })

  it('6. all variants share the id field', () => {
    // The id field is common across all variants
    const requests: CompactionRequest[] = [
      { type: 'encode-data', id: 'a', payload: [] },
      { type: 'encode-rels', id: 'b', payload: [] },
      { type: 'encode-events', id: 'c', payload: [] },
    ]

    // id is accessible on the union without narrowing
    const ids = requests.map(r => r.id)
    expect(ids).toEqual(['a', 'b', 'c'])
  })
})

import { describe, it, expect } from 'vitest'
import { mergeResults } from '@/engine/merge'
import type { DataLine } from '@/engine/types'

/**
 * mergeResults() Tie-Breaking Tests
 *
 * Validates the ReplacingMergeTree invariant: when both Parquet and buffer
 * have the same $id, version comparison determines the winner. On ties
 * (same $v), the buffer always wins because it represents more recent data.
 *
 * These tests complement the main merge.test.ts suite by exhaustively
 * covering all tie-breaking and version-conflict scenarios.
 */

/** Helper to create a DataLine with sensible defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

describe('mergeResults tie-breaking', () => {
  // ===========================================================================
  // Same $id, same $v: buffer wins (core tie-breaking invariant)
  // ===========================================================================
  describe('same $id, same $v: buffer wins', () => {
    it('buffer wins when both have $v=1 with different data', () => {
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, $ts: 1700000000000, name: 'Parquet-Alice', email: 'parquet@example.com' }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 1, $ts: 1700000001000, name: 'Buffer-Alice', email: 'buffer@example.com' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Buffer-Alice')
      expect(result[0].email).toBe('buffer@example.com')
      expect(result[0].$ts).toBe(1700000001000)
    })

    it('buffer wins when both have $v=5 (higher version tie)', () => {
      const parquet = [
        makeLine({ $id: 'item-99', $v: 5, $op: 'u', name: 'Parquet-V5', status: 'old' }),
      ]
      const buffer = [
        makeLine({ $id: 'item-99', $v: 5, $op: 'u', name: 'Buffer-V5', status: 'new' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Buffer-V5')
      expect(result[0].status).toBe('new')
      expect(result[0].$v).toBe(5)
    })

    it('result is idempotent when both have same $v and same data', () => {
      const sharedData = {
        $id: 'user-1',
        $v: 1,
        $op: 'c' as const,
        $ts: 1700000000000,
        name: 'Alice',
        email: 'alice@example.com',
      }
      const parquet = [makeLine(sharedData)]
      const buffer = [makeLine(sharedData)]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      // No duplicates, and data is intact
      expect(result[0].$id).toBe('user-1')
      expect(result[0].name).toBe('Alice')
      expect(result[0].email).toBe('alice@example.com')
      expect(result[0].$v).toBe(1)
    })
  })

  // ===========================================================================
  // Same $id, buffer has higher $v: buffer wins (standard case)
  // ===========================================================================
  describe('same $id, buffer has higher $v: buffer wins', () => {
    it('buffer $v=2 overrides parquet $v=1', () => {
      const parquet = [
        makeLine({ $id: 'e1', $v: 1, name: 'Original' }),
      ]
      const buffer = [
        makeLine({ $id: 'e1', $v: 2, $op: 'u', name: 'Updated' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Updated')
      expect(result[0].$v).toBe(2)
    })

    it('buffer $v=10 overrides parquet $v=3 (large version gap)', () => {
      const parquet = [
        makeLine({ $id: 'e1', $v: 3, $op: 'u', name: 'V3-Parquet' }),
      ]
      const buffer = [
        makeLine({ $id: 'e1', $v: 10, $op: 'u', name: 'V10-Buffer' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('V10-Buffer')
      expect(result[0].$v).toBe(10)
    })
  })

  // ===========================================================================
  // Same $id, parquet has higher $v: parquet wins (rollback scenario)
  // ===========================================================================
  describe('same $id, parquet has higher $v: parquet wins (rollback)', () => {
    it('parquet $v=3 beats buffer $v=1', () => {
      const parquet = [
        makeLine({ $id: 'e1', $v: 3, $op: 'u', name: 'Parquet-V3' }),
      ]
      const buffer = [
        makeLine({ $id: 'e1', $v: 1, name: 'Buffer-V1' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Parquet-V3')
      expect(result[0].$v).toBe(3)
    })

    it('parquet $v=100 beats buffer $v=50 (large version gap)', () => {
      const parquet = [
        makeLine({ $id: 'e1', $v: 100, $op: 'u', name: 'Parquet-V100' }),
      ]
      const buffer = [
        makeLine({ $id: 'e1', $v: 50, $op: 'u', name: 'Buffer-V50' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Parquet-V100')
      expect(result[0].$v).toBe(100)
    })
  })

  // ===========================================================================
  // Tombstone ($op='d') at same $v: buffer tombstone wins
  // ===========================================================================
  describe('tombstone at same $v: buffer tombstone wins', () => {
    it('buffer tombstone at $v=1 suppresses parquet create at $v=1', () => {
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, $op: 'c', name: 'Alice' }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 1, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
    })

    it('buffer tombstone at $v=5 suppresses parquet update at $v=5', () => {
      const parquet = [
        makeLine({ $id: 'item-42', $v: 5, $op: 'u', name: 'Active-Item' }),
      ]
      const buffer = [
        makeLine({ $id: 'item-42', $v: 5, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
    })

    it('buffer tombstone at same $v suppresses parquet, other entities remain', () => {
      const parquet = [
        makeLine({ $id: 'a', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'b', $v: 2, name: 'Bob' }),
        makeLine({ $id: 'c', $v: 1, name: 'Charlie' }),
      ]
      const buffer = [
        makeLine({ $id: 'b', $v: 2, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(2)
      expect(result.map(r => r.$id)).toEqual(['a', 'c'])
    })
  })

  // ===========================================================================
  // Buffer tombstone vs parquet create at same $v
  // ===========================================================================
  describe('buffer tombstone vs parquet create at same $v', () => {
    it('tombstone wins: entity is excluded from results', () => {
      const parquet = [
        makeLine({ $id: 'new-entity', $v: 1, $op: 'c', name: 'Fresh Create', email: 'new@example.com' }),
      ]
      const buffer = [
        makeLine({ $id: 'new-entity', $v: 1, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
      expect(result.find(r => r.$id === 'new-entity')).toBeUndefined()
    })

    it('tombstone at higher $v also wins over parquet create', () => {
      const parquet = [
        makeLine({ $id: 'new-entity', $v: 1, $op: 'c', name: 'Created' }),
      ]
      const buffer = [
        makeLine({ $id: 'new-entity', $v: 2, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
    })

    it('parquet create wins when buffer tombstone has lower $v', () => {
      // Edge case: compacted Parquet has re-created the entity at a higher version
      const parquet = [
        makeLine({ $id: 'new-entity', $v: 3, $op: 'c', name: 'Re-created' }),
      ]
      const buffer = [
        makeLine({ $id: 'new-entity', $v: 1, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Re-created')
      expect(result[0].$v).toBe(3)
    })
  })

  // ===========================================================================
  // Multiple overlapping entities: correct per-entity resolution
  // ===========================================================================
  describe('multiple overlapping entities: per-entity resolution', () => {
    it('each entity resolved independently with mixed version scenarios', () => {
      const parquet = [
        makeLine({ $id: 'a', $v: 1, name: 'A-parquet' }),    // buffer has higher $v -> buffer wins
        makeLine({ $id: 'b', $v: 3, name: 'B-parquet' }),    // parquet has higher $v -> parquet wins
        makeLine({ $id: 'c', $v: 2, name: 'C-parquet' }),    // same $v tie -> buffer wins
        makeLine({ $id: 'd', $v: 1, name: 'D-parquet' }),    // buffer tombstone at same $v -> deleted
        makeLine({ $id: 'e', $v: 4, name: 'E-parquet' }),    // no buffer entry -> parquet survives
      ]
      const buffer = [
        makeLine({ $id: 'a', $v: 2, $op: 'u', name: 'A-buffer' }),
        makeLine({ $id: 'b', $v: 1, name: 'B-buffer' }),
        makeLine({ $id: 'c', $v: 2, $op: 'u', name: 'C-buffer' }),
        makeLine({ $id: 'd', $v: 1, $op: 'd' }),
        makeLine({ $id: 'f', $v: 1, $op: 'c', name: 'F-buffer-only' }),  // only in buffer
      ]
      const result = mergeResults(parquet, buffer)

      // d is deleted, so 5 remain: a, b, c, e, f
      expect(result).toHaveLength(5)
      expect(result.map(r => r.$id)).toEqual(['a', 'b', 'c', 'e', 'f'])

      // a: buffer wins (higher $v)
      const a = result.find(r => r.$id === 'a')!
      expect(a.name).toBe('A-buffer')
      expect(a.$v).toBe(2)

      // b: parquet wins (higher $v)
      const b = result.find(r => r.$id === 'b')!
      expect(b.name).toBe('B-parquet')
      expect(b.$v).toBe(3)

      // c: buffer wins (tie at $v=2)
      const c = result.find(r => r.$id === 'c')!
      expect(c.name).toBe('C-buffer')
      expect(c.$v).toBe(2)

      // e: parquet only (no conflict)
      const e = result.find(r => r.$id === 'e')!
      expect(e.name).toBe('E-parquet')
      expect(e.$v).toBe(4)

      // f: buffer only (no conflict)
      const f = result.find(r => r.$id === 'f')!
      expect(f.name).toBe('F-buffer-only')
      expect(f.$v).toBe(1)
    })

    it('all entities at same $v: buffer wins for every overlapping entry', () => {
      const parquet = [
        makeLine({ $id: 'x', $v: 1, name: 'X-parquet' }),
        makeLine({ $id: 'y', $v: 1, name: 'Y-parquet' }),
        makeLine({ $id: 'z', $v: 1, name: 'Z-parquet' }),
      ]
      const buffer = [
        makeLine({ $id: 'x', $v: 1, name: 'X-buffer' }),
        makeLine({ $id: 'y', $v: 1, name: 'Y-buffer' }),
        makeLine({ $id: 'z', $v: 1, name: 'Z-buffer' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(3)
      expect(result[0].name).toBe('X-buffer')
      expect(result[1].name).toBe('Y-buffer')
      expect(result[2].name).toBe('Z-buffer')
    })

    it('mixed tombstones and updates at same $v across multiple entities', () => {
      const parquet = [
        makeLine({ $id: 'a', $v: 2, $op: 'u', name: 'A-alive' }),
        makeLine({ $id: 'b', $v: 2, $op: 'u', name: 'B-alive' }),
        makeLine({ $id: 'c', $v: 2, $op: 'u', name: 'C-alive' }),
      ]
      const buffer = [
        makeLine({ $id: 'a', $v: 2, $op: 'd' }),              // tombstone at same $v -> deleted
        makeLine({ $id: 'b', $v: 2, $op: 'u', name: 'B-updated' }),  // update at same $v -> buffer wins
        makeLine({ $id: 'c', $v: 3, $op: 'd' }),              // tombstone at higher $v -> deleted
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('b')
      expect(result[0].name).toBe('B-updated')
    })
  })

  // ===========================================================================
  // Edge cases around version boundaries
  // ===========================================================================
  describe('version boundary edge cases', () => {
    it('$v=0 in both: buffer wins', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 0, name: 'Parquet-V0' })]
      const buffer = [makeLine({ $id: 'e1', $v: 0, name: 'Buffer-V0' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Buffer-V0')
    })

    it('buffer tombstone at $v=0 suppresses parquet at $v=0', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 0, name: 'Parquet-V0' })]
      const buffer = [makeLine({ $id: 'e1', $v: 0, $op: 'd' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
    })

    it('consecutive versions: buffer $v=N wins over parquet $v=N-1', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 99, name: 'V99' })]
      const buffer = [makeLine({ $id: 'e1', $v: 100, $op: 'u', name: 'V100' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('V100')
    })
  })
})

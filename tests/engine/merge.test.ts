import { describe, it, expect } from 'vitest'
import { mergeResults } from '@/engine/merge'
import type { DataLine } from '@/engine/types'

/**
 * mergeResults() Test Suite
 *
 * Tests the merge logic that combines buffer results (in-memory JSONL mutations)
 * with Parquet results (compacted data) using ReplacingMergeTree semantics:
 * - For each $id, the entry with the highest $v wins
 * - Ties go to the buffer (assumed more recent)
 * - Tombstones ($op='d') are excluded from final results
 * - Output is sorted by $id for deterministic results
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

describe('mergeResults', () => {
  // ===========================================================================
  // Basic merge
  // ===========================================================================
  describe('basic merge', () => {
    it('1. returns empty array when both inputs are empty', () => {
      const result = mergeResults([], [])
      expect(result).toEqual([])
    })

    it('2. returns parquet as-is when buffer is empty', () => {
      const parquet = [
        makeLine({ $id: 'a', name: 'Alice' }),
        makeLine({ $id: 'b', name: 'Bob' }),
      ]
      const result = mergeResults(parquet, [])
      expect(result).toHaveLength(2)
      expect(result.map(r => r.$id)).toEqual(['a', 'b'])
    })

    it('3. returns buffer as-is when parquet is empty', () => {
      const buffer = [
        makeLine({ $id: 'x', name: 'Xena' }),
        makeLine({ $id: 'y', name: 'Yuri' }),
      ]
      const result = mergeResults([], buffer)
      expect(result).toHaveLength(2)
      expect(result.map(r => r.$id)).toEqual(['x', 'y'])
    })

    it('4. returns all entities when parquet and buffer have disjoint $ids', () => {
      const parquet = [
        makeLine({ $id: 'a', name: 'Alice' }),
        makeLine({ $id: 'b', name: 'Bob' }),
      ]
      const buffer = [
        makeLine({ $id: 'c', name: 'Charlie' }),
        makeLine({ $id: 'd', name: 'Diana' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(4)
      expect(result.map(r => r.$id)).toEqual(['a', 'b', 'c', 'd'])
    })

    it('5. buffer version wins when same $id exists in both (higher $v)', () => {
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, name: 'Alice' }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 2, $op: 'u', name: 'Alice Updated' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice Updated')
      expect(result[0].$v).toBe(2)
    })
  })

  // ===========================================================================
  // Dedup by $id
  // ===========================================================================
  describe('dedup by $id', () => {
    it('6. buffer $v=2 overrides parquet $v=1', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 1, name: 'Old' })]
      const buffer = [makeLine({ $id: 'e1', $v: 2, $op: 'u', name: 'New' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('New')
      expect(result[0].$v).toBe(2)
    })

    it('7. buffer $v=3 overrides parquet $v=2', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 2, name: 'V2' })]
      const buffer = [makeLine({ $id: 'e1', $v: 3, $op: 'u', name: 'V3' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('V3')
      expect(result[0].$v).toBe(3)
    })

    it('8. multiple overlapping $ids: each resolved independently', () => {
      const parquet = [
        makeLine({ $id: 'a', $v: 1, name: 'A-old' }),
        makeLine({ $id: 'b', $v: 1, name: 'B-old' }),
        makeLine({ $id: 'c', $v: 1, name: 'C-old' }),
      ]
      const buffer = [
        makeLine({ $id: 'a', $v: 2, $op: 'u', name: 'A-new' }),
        makeLine({ $id: 'c', $v: 3, $op: 'u', name: 'C-new' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(3)

      const aEntity = result.find(r => r.$id === 'a')!
      expect(aEntity.name).toBe('A-new')
      expect(aEntity.$v).toBe(2)

      const bEntity = result.find(r => r.$id === 'b')!
      expect(bEntity.name).toBe('B-old')
      expect(bEntity.$v).toBe(1)

      const cEntity = result.find(r => r.$id === 'c')!
      expect(cEntity.name).toBe('C-new')
      expect(cEntity.$v).toBe(3)
    })

    it('9. parquet wins when it has higher $v than buffer', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 2, name: 'Parquet-V2' })]
      const buffer = [makeLine({ $id: 'e1', $v: 1, name: 'Buffer-V1' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Parquet-V2')
      expect(result[0].$v).toBe(2)
    })
  })

  // ===========================================================================
  // Tombstone handling
  // ===========================================================================
  describe('tombstone handling', () => {
    it('10. buffer tombstone suppresses parquet entity with same $id', () => {
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, name: 'Alice' }),
        makeLine({ $id: 'user-2', $v: 1, name: 'Bob' }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 2, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('user-2')
    })

    it('11. buffer tombstone for $id not in parquet: entity excluded', () => {
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, name: 'Alice' }),
      ]
      const buffer = [
        makeLine({ $id: 'ghost', $v: 1, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('user-1')
      expect(result.find(r => r.$id === 'ghost')).toBeUndefined()
    })

    it('12. multiple tombstones suppress corresponding parquet entities', () => {
      const parquet = [
        makeLine({ $id: 'a', $v: 1, name: 'A' }),
        makeLine({ $id: 'b', $v: 1, name: 'B' }),
        makeLine({ $id: 'c', $v: 1, name: 'C' }),
        makeLine({ $id: 'd', $v: 1, name: 'D' }),
      ]
      const buffer = [
        makeLine({ $id: 'a', $v: 2, $op: 'd' }),
        makeLine({ $id: 'c', $v: 2, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(2)
      expect(result.map(r => r.$id)).toEqual(['b', 'd'])
    })
  })

  // ===========================================================================
  // Mixed scenarios
  // ===========================================================================
  describe('mixed scenarios', () => {
    it('13. 3 parquet + 2 buffer (1 overlap, 1 new, 1 tombstone) merges correctly', () => {
      const parquet = [
        makeLine({ $id: 'p1', $v: 1, name: 'P1' }),
        makeLine({ $id: 'p2', $v: 1, name: 'P2' }),
        makeLine({ $id: 'p3', $v: 1, name: 'P3' }),
      ]
      const buffer = [
        // overlap: updates p1
        makeLine({ $id: 'p1', $v: 2, $op: 'u', name: 'P1-updated' }),
        // new entity
        makeLine({ $id: 'b1', $v: 1, $op: 'c', name: 'B1-new' }),
        // tombstone: deletes p3
        makeLine({ $id: 'p3', $v: 2, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)

      // Expected: p1 (updated), p2 (original), b1 (new) = 3 entities
      // p3 is deleted
      expect(result).toHaveLength(3)
      expect(result.map(r => r.$id)).toEqual(['b1', 'p1', 'p2'])

      const p1 = result.find(r => r.$id === 'p1')!
      expect(p1.name).toBe('P1-updated')
      expect(p1.$v).toBe(2)

      const p2 = result.find(r => r.$id === 'p2')!
      expect(p2.name).toBe('P2')

      const b1 = result.find(r => r.$id === 'b1')!
      expect(b1.name).toBe('B1-new')
    })

    it('14. large merge: 1000 parquet + 100 buffer (10 overlaps) has correct count', () => {
      const parquet: DataLine[] = []
      for (let i = 0; i < 1000; i++) {
        parquet.push(makeLine({ $id: `p-${String(i).padStart(4, '0')}`, $v: 1, name: `Entity-${i}` }))
      }

      const buffer: DataLine[] = []
      // 10 overlaps (update existing parquet entities)
      for (let i = 0; i < 10; i++) {
        buffer.push(makeLine({ $id: `p-${String(i).padStart(4, '0')}`, $v: 2, $op: 'u', name: `Updated-${i}` }))
      }
      // 90 new entities
      for (let i = 0; i < 90; i++) {
        buffer.push(makeLine({ $id: `b-${String(i).padStart(4, '0')}`, $v: 1, $op: 'c', name: `New-${i}` }))
      }

      const result = mergeResults(parquet, buffer)
      // 1000 parquet + 90 new buffer = 1090 (10 overlaps deduplicated)
      expect(result).toHaveLength(1090)

      // Verify one of the overlapping entities was updated
      const updated = result.find(r => r.$id === 'p-0000')!
      expect(updated.name).toBe('Updated-0')
      expect(updated.$v).toBe(2)
    })

    it('15. entities from buffer preserve all fields (no data loss)', () => {
      const parquet: DataLine[] = []
      const buffer = [
        makeLine({
          $id: 'user-1',
          $v: 1,
          $ts: 1700000000000,
          name: 'Alice',
          email: 'alice@example.com',
          age: 30,
          tags: ['admin', 'user'],
          metadata: { role: 'superadmin', level: 5 },
        }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0]).toEqual(buffer[0])
    })

    it('16. entities from parquet preserve all fields (no data loss)', () => {
      const parquet = [
        makeLine({
          $id: 'user-1',
          $v: 1,
          $ts: 1700000000000,
          name: 'Alice',
          email: 'alice@example.com',
          age: 30,
          tags: ['admin', 'user'],
          metadata: { role: 'superadmin', level: 5 },
        }),
      ]
      const buffer: DataLine[] = []
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0]).toEqual(parquet[0])
    })
  })

  // ===========================================================================
  // Order
  // ===========================================================================
  describe('order', () => {
    it('17. results are sorted by $id for deterministic output', () => {
      const parquet = [
        makeLine({ $id: 'zebra', name: 'Z' }),
        makeLine({ $id: 'alpha', name: 'A' }),
      ]
      const buffer = [
        makeLine({ $id: 'mango', name: 'M' }),
        makeLine({ $id: 'banana', name: 'B' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result.map(r => r.$id)).toEqual(['alpha', 'banana', 'mango', 'zebra'])
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================
  describe('edge cases', () => {
    it('18. same $id with same $v: buffer wins (more recent)', () => {
      const parquet = [makeLine({ $id: 'e1', $v: 1, name: 'Parquet-version' })]
      const buffer = [makeLine({ $id: 'e1', $v: 1, name: 'Buffer-version' })]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Buffer-version')
    })

    it('19. empty parquet, buffer with creates and tombstones: only creates returned', () => {
      const buffer = [
        makeLine({ $id: 'a', $v: 1, $op: 'c', name: 'Created' }),
        makeLine({ $id: 'b', $v: 1, $op: 'd' }),
        makeLine({ $id: 'c', $v: 1, $op: 'c', name: 'Also Created' }),
        makeLine({ $id: 'd', $v: 1, $op: 'd' }),
      ]
      const result = mergeResults([], buffer)
      expect(result).toHaveLength(2)
      expect(result.map(r => r.$id)).toEqual(['a', 'c'])
    })

    it('20. parquet entity updated then deleted in buffer: excluded', () => {
      // The buffer contains the final state: a tombstone at $v=3
      // This simulates an entity that was created ($v=1 in parquet),
      // updated ($v=2), then deleted ($v=3) — buffer holds the final tombstone
      const parquet = [
        makeLine({ $id: 'user-1', $v: 1, name: 'Alice' }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 3, $op: 'd' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Version tie-breaking (zou5.26)
  // ===========================================================================
  describe('version tie-breaking', () => {
    it('21. same $id, same $v, different data: buffer version wins', () => {
      // When both parquet and buffer have the same $id at the same $v,
      // the buffer entry wins because it is overlaid second (>= comparison).
      const parquet = [
        makeLine({ $id: 'user-1', $v: 3, $op: 'u', name: 'Parquet-Name', score: 100 }),
      ]
      const buffer = [
        makeLine({ $id: 'user-1', $v: 3, $op: 'u', name: 'Buffer-Name', score: 200 }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('user-1')
      expect(result[0].name).toBe('Buffer-Name')
      expect(result[0].score).toBe(200)
      expect(result[0].$v).toBe(3)
    })

    it('22. parquet has higher $v than buffer: parquet version wins', () => {
      // When parquet has been compacted with a more recent version than
      // what exists in the buffer (e.g. stale buffer after partial replay),
      // parquet's higher version takes precedence.
      const parquet = [
        makeLine({ $id: 'item-42', $v: 5, $op: 'u', name: 'Parquet-V5', status: 'compacted' }),
      ]
      const buffer = [
        makeLine({ $id: 'item-42', $v: 2, $op: 'u', name: 'Buffer-V2', status: 'stale' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('item-42')
      expect(result[0].name).toBe('Parquet-V5')
      expect(result[0].status).toBe('compacted')
      expect(result[0].$v).toBe(5)
    })

    it('23. buffer has higher $v than parquet: buffer version wins', () => {
      // The most common scenario: buffer contains mutations that haven't
      // been compacted yet, so buffer's version is higher.
      const parquet = [
        makeLine({ $id: 'item-42', $v: 1, $op: 'c', name: 'Original', status: 'created' }),
      ]
      const buffer = [
        makeLine({ $id: 'item-42', $v: 4, $op: 'u', name: 'Updated-3-Times', status: 'active' }),
      ]
      const result = mergeResults(parquet, buffer)
      expect(result).toHaveLength(1)
      expect(result[0].$id).toBe('item-42')
      expect(result[0].name).toBe('Updated-3-Times')
      expect(result[0].status).toBe('active')
      expect(result[0].$v).toBe(4)
    })
  })
})

import { describe, it, expect, beforeEach } from 'vitest'
import { TableBuffer } from '@/engine/buffer'
import type { DataLine } from '@/engine/types'

/**
 * TableBuffer Test Suite
 *
 * Tests the in-memory buffer that holds recent mutations before Parquet compaction.
 * The buffer is a Map-based store keyed by entity $id, supporting:
 * - CRUD operations with version tracking
 * - Tombstone-based deletes (entries with $op: 'd')
 * - Filtered scans with MongoDB-style operators
 */

function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

describe('TableBuffer', () => {
  let buffer: TableBuffer

  beforeEach(() => {
    buffer = new TableBuffer()
  })

  // =========================================================================
  // 1. Construction
  // =========================================================================
  describe('construction', () => {
    it('creates an empty buffer', () => {
      const buf = new TableBuffer()
      expect(buf.size).toBe(0)
      expect(buf.liveSize).toBe(0)
    })
  })

  // =========================================================================
  // 2. set(entity)
  // =========================================================================
  describe('set()', () => {
    it('stores entity by $id and retrieves with get()', () => {
      const entity = makeLine({ $id: 'user-1', name: 'Alice' })
      buffer.set(entity)
      expect(buffer.get('user-1')).toEqual(entity)
    })
  })

  // =========================================================================
  // 3. get(id)
  // =========================================================================
  describe('get()', () => {
    it('returns entity when found', () => {
      const entity = makeLine({ $id: 'user-1', name: 'Alice' })
      buffer.set(entity)
      expect(buffer.get('user-1')).toEqual(entity)
    })

    it('returns undefined when not found', () => {
      expect(buffer.get('nonexistent')).toBeUndefined()
    })
  })

  // =========================================================================
  // 4. get(id) on tombstone
  // =========================================================================
  describe('get() on tombstone', () => {
    it('returns the tombstone object, not undefined', () => {
      const entity = makeLine({ $id: 'user-1', name: 'Alice' })
      buffer.set(entity)
      buffer.delete('user-1', 2, Date.now())

      const result = buffer.get('user-1')
      expect(result).toBeDefined()
      expect(result!.$op).toBe('d')
      expect(result!.$id).toBe('user-1')
    })
  })

  // =========================================================================
  // 5. delete(id)
  // =========================================================================
  describe('delete()', () => {
    it('creates tombstone entry with correct fields', () => {
      const entity = makeLine({ $id: 'user-1', name: 'Alice' })
      buffer.set(entity)

      const version = 2
      const ts = 1700000000000
      buffer.delete('user-1', version, ts)

      const tombstone = buffer.get('user-1')
      expect(tombstone).toBeDefined()
      expect(tombstone!.$id).toBe('user-1')
      expect(tombstone!.$op).toBe('d')
      expect(tombstone!.$v).toBe(version)
      expect(tombstone!.$ts).toBe(ts)
    })

    it('creates tombstone even for non-existent entity', () => {
      buffer.delete('ghost-1', 1, Date.now())
      const tombstone = buffer.get('ghost-1')
      expect(tombstone).toBeDefined()
      expect(tombstone!.$op).toBe('d')
    })
  })

  // =========================================================================
  // 6. has(id)
  // =========================================================================
  describe('has()', () => {
    it('returns true for live entities', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      expect(buffer.has('user-1')).toBe(true)
    })

    it('returns true for tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())
      expect(buffer.has('user-1')).toBe(true)
    })

    it('returns false for missing entries', () => {
      expect(buffer.has('nonexistent')).toBe(false)
    })
  })

  // =========================================================================
  // 7. isTombstone(id)
  // =========================================================================
  describe('isTombstone()', () => {
    it('returns true for deleted entries', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())
      expect(buffer.isTombstone('user-1')).toBe(true)
    })

    it('returns false for live entries', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      expect(buffer.isTombstone('user-1')).toBe(false)
    })

    it('returns false for missing entries', () => {
      expect(buffer.isTombstone('nonexistent')).toBe(false)
    })
  })

  // =========================================================================
  // 8. scan() - live entities only
  // =========================================================================
  describe('scan()', () => {
    it('returns all live entities excluding tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie' }))
      buffer.delete('user-2', 2, Date.now())

      const results = buffer.scan()
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-3')
      expect(results.map(e => e.$id)).not.toContain('user-2')
    })

    it('returns empty array when buffer is empty', () => {
      expect(buffer.scan()).toEqual([])
    })

    it('returns empty array when all entries are tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())
      expect(buffer.scan()).toEqual([])
    })
  })

  // =========================================================================
  // 9. scan(filter) - simple equality filter
  // =========================================================================
  describe('scan(filter) - simple equality', () => {
    it('returns entities matching a simple field equality filter', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', role: 'admin' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', role: 'user' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', role: 'admin' }))

      const results = buffer.scan({ name: 'Alice' })
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('user-1')
    })

    it('returns entities matching multiple field equality filters', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', role: 'admin' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', role: 'admin' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', role: 'user' }))

      const results = buffer.scan({ role: 'admin' })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-2')
    })

    it('does not return tombstoned entities even if they match filter', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())

      const results = buffer.scan({ name: 'Alice' })
      expect(results).toHaveLength(0)
    })
  })

  // =========================================================================
  // 10. scan() with nested filters
  // =========================================================================
  describe('scan(filter) - dot notation and comparison operators', () => {
    beforeEach(() => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', age: 30, address: { city: 'NYC', zip: '10001' } }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', age: 25, address: { city: 'LA', zip: '90001' } }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', age: 35, address: { city: 'NYC', zip: '10002' } }))
    })

    it('supports dot-notation for nested fields', () => {
      const results = buffer.scan({ 'address.city': 'NYC' })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-3')
    })

    it('supports $eq operator', () => {
      const results = buffer.scan({ name: { $eq: 'Bob' } })
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('user-2')
    })

    it('supports $ne operator', () => {
      const results = buffer.scan({ name: { $ne: 'Alice' } })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-2')
      expect(results.map(e => e.$id)).toContain('user-3')
    })

    it('supports $gt operator', () => {
      const results = buffer.scan({ age: { $gt: 28 } })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-3')
    })

    it('supports $gte operator', () => {
      const results = buffer.scan({ age: { $gte: 30 } })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-3')
    })

    it('supports $lt operator', () => {
      const results = buffer.scan({ age: { $lt: 30 } })
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('user-2')
    })

    it('supports $lte operator', () => {
      const results = buffer.scan({ age: { $lte: 30 } })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-2')
    })

    it('supports $in operator', () => {
      const results = buffer.scan({ name: { $in: ['Alice', 'Charlie'] } })
      expect(results).toHaveLength(2)
      expect(results.map(e => e.$id)).toContain('user-1')
      expect(results.map(e => e.$id)).toContain('user-3')
    })

    it('supports $exists operator (true)', () => {
      buffer.set(makeLine({ $id: 'user-4', name: 'Diana' }))
      const results = buffer.scan({ age: { $exists: true } })
      expect(results).toHaveLength(3)
      expect(results.map(e => e.$id)).not.toContain('user-4')
    })

    it('supports $exists operator (false)', () => {
      buffer.set(makeLine({ $id: 'user-4', name: 'Diana' }))
      const results = buffer.scan({ age: { $exists: false } })
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('user-4')
    })

    it('supports combined dot-notation and comparison operators', () => {
      const results = buffer.scan({ 'address.city': 'NYC', age: { $gt: 31 } })
      expect(results).toHaveLength(1)
      expect(results[0].$id).toBe('user-3')
    })
  })

  // =========================================================================
  // 10b. count() - optimized counting without materialization
  // =========================================================================
  describe('count()', () => {
    it('counts all live entities when no filter is provided', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie' }))

      expect(buffer.count()).toBe(3)
    })

    it('excludes tombstones from count', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie' }))
      buffer.delete('user-2', 2, Date.now())

      expect(buffer.count()).toBe(2)
    })

    it('returns 0 when buffer is empty', () => {
      expect(buffer.count()).toBe(0)
    })

    it('returns 0 when all entries are tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())

      expect(buffer.count()).toBe(0)
    })

    it('counts entities matching a simple equality filter', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', role: 'admin' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', role: 'user' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', role: 'admin' }))

      expect(buffer.count({ role: 'admin' })).toBe(2)
    })

    it('counts entities matching comparison operators', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', age: 30 }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', age: 25 }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', age: 35 }))

      expect(buffer.count({ age: { $gt: 28 } })).toBe(2)
      expect(buffer.count({ age: { $lte: 25 } })).toBe(1)
    })

    it('returns same result as scan().length', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', role: 'admin', age: 30 }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob', role: 'user', age: 25 }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie', role: 'admin', age: 35 }))
      buffer.delete('user-2', 2, Date.now())

      const filter = { role: 'admin' }
      expect(buffer.count(filter)).toBe(buffer.scan(filter).length)

      expect(buffer.count()).toBe(buffer.scan().length)

      const noMatch = { role: 'superadmin' }
      expect(buffer.count(noMatch)).toBe(buffer.scan(noMatch).length)
    })

    it('does not count tombstoned entities even if they match filter', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())

      expect(buffer.count({ name: 'Alice' })).toBe(0)
    })
  })

  // =========================================================================
  // 11. clear()
  // =========================================================================
  describe('clear()', () => {
    it('removes all entries and size becomes 0', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.delete('user-1', 2, Date.now())

      expect(buffer.size).toBe(2)
      buffer.clear()
      expect(buffer.size).toBe(0)
      expect(buffer.liveSize).toBe(0)
      expect(buffer.get('user-1')).toBeUndefined()
      expect(buffer.get('user-2')).toBeUndefined()
    })
  })

  // =========================================================================
  // 12. size
  // =========================================================================
  describe('size', () => {
    it('returns count of all entries including tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.delete('user-1', 2, Date.now())

      expect(buffer.size).toBe(2)
    })
  })

  // =========================================================================
  // 13. liveSize
  // =========================================================================
  describe('liveSize', () => {
    it('returns count of live entities only, excluding tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.set(makeLine({ $id: 'user-3', name: 'Charlie' }))
      buffer.delete('user-2', 2, Date.now())

      expect(buffer.liveSize).toBe(2)
    })

    it('returns 0 when all entries are tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.delete('user-1', 2, Date.now())
      expect(buffer.liveSize).toBe(0)
    })
  })

  // =========================================================================
  // 14. entries()
  // =========================================================================
  describe('entries()', () => {
    it('iterates all entries including tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice' }))
      buffer.set(makeLine({ $id: 'user-2', name: 'Bob' }))
      buffer.delete('user-2', 2, Date.now())

      const entries = [...buffer.entries()]
      expect(entries).toHaveLength(2)

      const ids = entries.map(([id]) => id)
      expect(ids).toContain('user-1')
      expect(ids).toContain('user-2')

      const tombstone = entries.find(([id]) => id === 'user-2')
      expect(tombstone).toBeDefined()
      expect(tombstone![1].$op).toBe('d')
    })
  })

  // =========================================================================
  // 15. Overwrite
  // =========================================================================
  describe('overwrite', () => {
    it('setting same $id replaces previous entry', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', $v: 1 }))
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice Updated', $v: 2, $op: 'u' }))

      expect(buffer.size).toBe(1)
      const entity = buffer.get('user-1')
      expect(entity!.name).toBe('Alice Updated')
      expect(entity!.$v).toBe(2)
      expect(entity!.$op).toBe('u')
    })
  })

  // =========================================================================
  // 16. Delete then set
  // =========================================================================
  describe('delete then set', () => {
    it('re-creating a deleted entity replaces the tombstone', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', $v: 1 }))
      buffer.delete('user-1', 2, Date.now())

      expect(buffer.isTombstone('user-1')).toBe(true)

      buffer.set(makeLine({ $id: 'user-1', name: 'Alice Reborn', $v: 3, $op: 'c' }))

      expect(buffer.isTombstone('user-1')).toBe(false)
      expect(buffer.get('user-1')!.name).toBe('Alice Reborn')
      expect(buffer.get('user-1')!.$v).toBe(3)
      expect(buffer.size).toBe(1)
      expect(buffer.liveSize).toBe(1)
    })
  })

  // =========================================================================
  // 17. Version tracking
  // =========================================================================
  describe('version tracking', () => {
    it('preserves $v from the entity', () => {
      const entity = makeLine({ $id: 'user-1', name: 'Alice', $v: 42 })
      buffer.set(entity)

      expect(buffer.get('user-1')!.$v).toBe(42)
    })

    it('preserves $v through updates', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', $v: 1 }))
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice v2', $v: 2, $op: 'u' }))
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice v3', $v: 3, $op: 'u' }))

      expect(buffer.get('user-1')!.$v).toBe(3)
    })

    it('preserves $v on tombstones', () => {
      buffer.set(makeLine({ $id: 'user-1', name: 'Alice', $v: 1 }))
      buffer.delete('user-1', 5, Date.now())

      expect(buffer.get('user-1')!.$v).toBe(5)
    })
  })
})

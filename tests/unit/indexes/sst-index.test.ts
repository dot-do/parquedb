/**
 * Tests for SST Index
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { SSTIndex } from '@/indexes/secondary/sst'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

describe('SSTIndex', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_price',
      type: 'sst',
      fields: [{ path: 'price' }],
    }
  })

  describe('basic operations', () => {
    it('inserts and looks up values', () => {
      const index = new SSTIndex(storage, 'products', definition)

      index.insert(100, 'doc1', 0, 0)
      index.insert(200, 'doc2', 0, 1)
      index.insert(100, 'doc3', 0, 2)

      const result = index.lookup(100)
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc3')

      const result200 = index.lookup(200)
      expect(result200.docIds).toHaveLength(1)
      expect(result200.docIds).toContain('doc2')

      const result300 = index.lookup(300)
      expect(result300.docIds).toHaveLength(0)
    })

    it('handles string values', () => {
      const strDef: IndexDefinition = {
        name: 'idx_name',
        type: 'sst',
        fields: [{ path: 'name' }],
      }
      const index = new SSTIndex(storage, 'users', strDef)

      index.insert('alice', 'doc1', 0, 0)
      index.insert('bob', 'doc2', 0, 1)
      index.insert('alice', 'doc3', 0, 2)

      expect(index.lookup('alice').docIds).toHaveLength(2)
      expect(index.lookup('bob').docIds).toHaveLength(1)
    })

    it('handles date values', () => {
      const dateDef: IndexDefinition = {
        name: 'idx_created',
        type: 'sst',
        fields: [{ path: 'createdAt' }],
      }
      const index = new SSTIndex(storage, 'events', dateDef)

      const d1 = new Date('2024-01-01')
      const d2 = new Date('2024-02-01')

      index.insert(d1, 'doc1', 0, 0)
      index.insert(d2, 'doc2', 0, 1)

      expect(index.lookup(d1).docIds).toHaveLength(1)
      expect(index.lookup(d2).docIds).toHaveLength(1)
    })
  })

  describe('range queries', () => {
    let index: SSTIndex

    beforeEach(() => {
      index = new SSTIndex(storage, 'products', definition)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)
      index.insert(30, 'doc3', 0, 2)
      index.insert(40, 'doc4', 0, 3)
      index.insert(50, 'doc5', 0, 4)
    })

    it('handles $gt', () => {
      const result = index.range({ $gt: 30 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc4')
      expect(result.docIds).toContain('doc5')
    })

    it('handles $gte', () => {
      const result = index.range({ $gte: 30 })
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('doc3')
      expect(result.docIds).toContain('doc4')
      expect(result.docIds).toContain('doc5')
    })

    it('handles $lt', () => {
      const result = index.range({ $lt: 30 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc2')
    })

    it('handles $lte', () => {
      const result = index.range({ $lte: 30 })
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc2')
      expect(result.docIds).toContain('doc3')
    })

    it('handles combined $gte and $lt (half-open range)', () => {
      const result = index.range({ $gte: 20, $lt: 40 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc2')
      expect(result.docIds).toContain('doc3')
    })

    it('handles combined $gt and $lte', () => {
      const result = index.range({ $gt: 20, $lte: 40 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc3')
      expect(result.docIds).toContain('doc4')
    })

    it('handles combined $gte and $lte (closed range)', () => {
      const result = index.range({ $gte: 20, $lte: 40 })
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('doc2')
      expect(result.docIds).toContain('doc3')
      expect(result.docIds).toContain('doc4')
    })

    it('handles empty range', () => {
      const result = index.range({ $gt: 50 })
      expect(result.docIds).toHaveLength(0)
    })

    it('handles range with no lower bound', () => {
      const result = index.range({ $lte: 20 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc2')
    })

    it('handles range with no upper bound', () => {
      const result = index.range({ $gte: 40 })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc4')
      expect(result.docIds).toContain('doc5')
    })

    it('handles invalid range (lower > upper)', () => {
      const result = index.range({ $gte: 50, $lte: 10 })
      expect(result.docIds).toHaveLength(0)
    })
  })

  describe('min/max', () => {
    it('returns min value', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(30, 'doc3', 0, 2)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)

      const min = index.min()
      expect(min).not.toBeNull()
      expect(min!.value).toBe(10)
    })

    it('returns max value', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(10, 'doc1', 0, 0)
      index.insert(30, 'doc3', 0, 2)
      index.insert(20, 'doc2', 0, 1)

      const max = index.max()
      expect(max).not.toBeNull()
      expect(max!.value).toBe(30)
    })

    it('returns null for empty index', () => {
      const index = new SSTIndex(storage, 'products', definition)
      expect(index.min()).toBeNull()
      expect(index.max()).toBeNull()
    })
  })

  describe('scan', () => {
    it('returns all entries in sorted order', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(30, 'doc3', 0, 2)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)

      const result = index.scan()
      expect(result.docIds).toEqual(['doc1', 'doc2', 'doc3'])
    })

    it('supports limit and offset', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)
      index.insert(30, 'doc3', 0, 2)
      index.insert(40, 'doc4', 0, 3)
      index.insert(50, 'doc5', 0, 4)

      const result = index.scan({ offset: 1, limit: 2 })
      expect(result.docIds).toEqual(['doc2', 'doc3'])
    })
  })

  describe('remove', () => {
    it('removes entries', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(100, 'doc1', 0, 0)
      index.insert(100, 'doc2', 0, 1)

      expect(index.size).toBe(2)

      const removed = index.remove(100, 'doc1')
      expect(removed).toBe(true)
      expect(index.size).toBe(1)

      const result = index.lookup(100)
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds).toContain('doc2')
    })

    it('returns false when entry not found', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(100, 'doc1', 0, 0)

      expect(index.remove(100, 'doc2')).toBe(false)
      expect(index.remove(200, 'doc1')).toBe(false)
    })
  })

  describe('update', () => {
    it('updates row location', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(100, 'doc1', 0, 0)

      const updated = index.update(100, 'doc1', 1, 100)
      expect(updated).toBe(true)

      const result = index.lookup(100)
      expect(result.rowGroups).toContain(1)
    })
  })

  describe('buildFromArray', () => {
    it('builds index from documents', () => {
      const index = new SSTIndex(storage, 'products', definition)

      const docs = [
        { doc: { price: 30 }, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
        { doc: { price: 10 }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { price: 20 }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(3)
      expect(index.min()!.value).toBe(10)
      expect(index.max()!.value).toBe(30)
    })

    it('handles nested fields', () => {
      const nestedDef: IndexDefinition = {
        name: 'idx_nested',
        type: 'sst',
        fields: [{ path: 'item.price' }],
      }
      const index = new SSTIndex(storage, 'orders', nestedDef)

      const docs = [
        { doc: { item: { price: 100 } }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { item: { price: 50 } }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ]

      index.buildFromArray(docs)

      expect(index.lookup(100).docIds).toContain('doc1')
      expect(index.lookup(50).docIds).toContain('doc2')
    })
  })

  describe('persistence', () => {
    it('saves and loads index', async () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)
      index.insert(30, 'doc3', 1, 0)

      await index.save()

      const loaded = new SSTIndex(storage, 'products', definition)
      await loaded.load()

      expect(loaded.size).toBe(3)
      expect(loaded.min()!.value).toBe(10)
      expect(loaded.max()!.value).toBe(30)

      const rangeResult = loaded.range({ $gte: 15, $lte: 25 })
      expect(rangeResult.docIds).toHaveLength(1)
      expect(rangeResult.docIds).toContain('doc2')
    })

    it('handles empty index', async () => {
      const index = new SSTIndex(storage, 'products', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })
  })

  describe('string range queries', () => {
    it('handles string ranges', () => {
      const strDef: IndexDefinition = {
        name: 'idx_name',
        type: 'sst',
        fields: [{ path: 'name' }],
      }
      const index = new SSTIndex(storage, 'users', strDef)

      index.insert('alice', 'doc1', 0, 0)
      index.insert('bob', 'doc2', 0, 1)
      index.insert('charlie', 'doc3', 0, 2)
      index.insert('david', 'doc4', 0, 3)

      // Names between 'b' and 'd' (exclusive)
      const result = index.range({ $gte: 'b', $lt: 'd' })
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc2') // bob
      expect(result.docIds).toContain('doc3') // charlie
    })
  })

  describe('out-of-order inserts', () => {
    it('correctly sorts entries inserted out of order', () => {
      const index = new SSTIndex(storage, 'products', definition)

      // Insert in random order
      index.insert(50, 'doc5', 0, 4)
      index.insert(10, 'doc1', 0, 0)
      index.insert(40, 'doc4', 0, 3)
      index.insert(20, 'doc2', 0, 1)
      index.insert(30, 'doc3', 0, 2)

      // Range query should work correctly
      const result = index.range({ $gte: 20, $lte: 40 })
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('doc2')
      expect(result.docIds).toContain('doc3')
      expect(result.docIds).toContain('doc4')

      // Min/max should be correct
      expect(index.min()!.value).toBe(10)
      expect(index.max()!.value).toBe(50)
    })
  })

  describe('statistics', () => {
    it('reports correct statistics', () => {
      const index = new SSTIndex(storage, 'products', definition)
      index.insert(10, 'doc1', 0, 0)
      index.insert(20, 'doc2', 0, 1)
      index.insert(30, 'doc3', 0, 2)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(3)
      expect(stats.sizeBytes).toBeGreaterThan(0)
      expect(stats.levels).toBe(1)
    })
  })
})

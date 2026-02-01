/**
 * Tests for Hash Index
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { HashIndex } from '../../../src/indexes/secondary/hash'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { IndexDefinition } from '../../../src/indexes/types'

describe('HashIndex', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_status',
      type: 'hash',
      fields: [{ path: 'status' }],
    }
  })

  describe('basic operations', () => {
    it('inserts and looks up string values', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('pending', 'doc2', 0, 1)
      index.insert('completed', 'doc3', 0, 2)

      const pending = index.lookup('pending')
      expect(pending.docIds).toHaveLength(2)
      expect(pending.docIds).toContain('doc1')
      expect(pending.docIds).toContain('doc2')

      const completed = index.lookup('completed')
      expect(completed.docIds).toHaveLength(1)
      expect(completed.docIds).toContain('doc3')

      const cancelled = index.lookup('cancelled')
      expect(cancelled.docIds).toHaveLength(0)
    })

    it('inserts and looks up number values', () => {
      const index = new HashIndex(storage, 'products', {
        name: 'idx_category',
        type: 'hash',
        fields: [{ path: 'categoryId' }],
      })

      index.insert(1, 'doc1', 0, 0)
      index.insert(1, 'doc2', 0, 1)
      index.insert(2, 'doc3', 0, 2)

      const cat1 = index.lookup(1)
      expect(cat1.docIds).toHaveLength(2)

      const cat2 = index.lookup(2)
      expect(cat2.docIds).toHaveLength(1)
    })

    it('handles null values', () => {
      const index = new HashIndex(storage, 'users', definition)

      index.insert(null, 'doc1', 0, 0)
      index.insert(null, 'doc2', 0, 1)
      index.insert('active', 'doc3', 0, 2)

      const nullResults = index.lookup(null)
      expect(nullResults.docIds).toHaveLength(2)
    })

    it('reports correct statistics', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('pending', 'doc2', 0, 1)
      index.insert('completed', 'doc3', 0, 2)

      expect(index.size).toBe(3)
      expect(index.uniqueKeyCount).toBe(2)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(3)
      expect(stats.uniqueKeys).toBe(2)
      expect(stats.sizeBytes).toBeGreaterThan(0)
    })
  })

  describe('lookupIn', () => {
    it('looks up multiple values at once', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('processing', 'doc2', 0, 1)
      index.insert('completed', 'doc3', 0, 2)
      index.insert('cancelled', 'doc4', 0, 3)

      const result = index.lookupIn(['pending', 'completed'])
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc3')
    })

    it('handles values not in index', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)

      const result = index.lookupIn(['pending', 'nonexistent'])
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds).toContain('doc1')
    })
  })

  describe('remove', () => {
    it('removes entries by value and docId', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('pending', 'doc2', 0, 1)

      expect(index.size).toBe(2)

      const removed = index.remove('pending', 'doc1')
      expect(removed).toBe(true)
      expect(index.size).toBe(1)

      const result = index.lookup('pending')
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds).toContain('doc2')
    })

    it('returns false when entry not found', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)

      expect(index.remove('pending', 'doc2')).toBe(false)
      expect(index.remove('completed', 'doc1')).toBe(false)
    })

    it('cleans up empty buckets', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      expect(index.uniqueKeyCount).toBe(1)

      index.remove('pending', 'doc1')
      expect(index.uniqueKeyCount).toBe(0)
    })
  })

  describe('update', () => {
    it('updates row location', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)

      const updated = index.update('pending', 'doc1', 1, 100)
      expect(updated).toBe(true)

      const result = index.lookup('pending')
      expect(result.rowGroups).toContain(1)
    })

    it('returns false when entry not found', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)

      expect(index.update('pending', 'doc2', 1, 100)).toBe(false)
    })
  })

  describe('exists', () => {
    it('checks if value exists', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)

      expect(index.exists('pending')).toBe(true)
      expect(index.exists('completed')).toBe(false)
    })
  })

  describe('clear', () => {
    it('removes all entries', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('completed', 'doc2', 0, 1)

      expect(index.size).toBe(2)

      index.clear()

      expect(index.size).toBe(0)
      expect(index.uniqueKeyCount).toBe(0)
    })
  })

  describe('buildFromArray', () => {
    it('builds index from document array', () => {
      const index = new HashIndex(storage, 'orders', definition)

      const docs = [
        { doc: { $id: 'order1', status: 'pending' }, docId: 'order1', rowGroup: 0, rowOffset: 0 },
        { doc: { $id: 'order2', status: 'pending' }, docId: 'order2', rowGroup: 0, rowOffset: 1 },
        { doc: { $id: 'order3', status: 'completed' }, docId: 'order3', rowGroup: 0, rowOffset: 2 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(3)
      expect(index.lookup('pending').docIds).toHaveLength(2)
      expect(index.lookup('completed').docIds).toHaveLength(1)
    })

    it('handles nested field paths', () => {
      const nestedDef: IndexDefinition = {
        name: 'idx_nested',
        type: 'hash',
        fields: [{ path: 'user.role' }],
      }

      const index = new HashIndex(storage, 'items', nestedDef)

      const docs = [
        { doc: { user: { role: 'admin' } }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { user: { role: 'user' } }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { doc: { user: { role: 'admin' } }, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
      ]

      index.buildFromArray(docs)

      expect(index.lookup('admin').docIds).toHaveLength(2)
      expect(index.lookup('user').docIds).toHaveLength(1)
    })

    it('skips documents with missing fields when not sparse', () => {
      const index = new HashIndex(storage, 'orders', definition)

      const docs = [
        { doc: { $id: 'order1', status: 'pending' }, docId: 'order1', rowGroup: 0, rowOffset: 0 },
        { doc: { $id: 'order2' }, docId: 'order2', rowGroup: 0, rowOffset: 1 }, // missing status
        { doc: { $id: 'order3', status: 'completed' }, docId: 'order3', rowGroup: 0, rowOffset: 2 },
      ]

      index.buildFromArray(docs)

      expect(index.size).toBe(2) // order2 skipped
    })
  })

  describe('composite keys', () => {
    it('indexes multiple fields', () => {
      const compositeDef: IndexDefinition = {
        name: 'idx_composite',
        type: 'hash',
        fields: [{ path: 'tenantId' }, { path: 'status' }],
      }

      const index = new HashIndex(storage, 'orders', compositeDef)

      const docs = [
        { doc: { tenantId: 'A', status: 'pending' }, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { doc: { tenantId: 'A', status: 'pending' }, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { doc: { tenantId: 'A', status: 'completed' }, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
        { doc: { tenantId: 'B', status: 'pending' }, docId: 'doc4', rowGroup: 0, rowOffset: 3 },
      ]

      index.buildFromArray(docs)

      expect(index.lookup(['A', 'pending']).docIds).toHaveLength(2)
      expect(index.lookup(['A', 'completed']).docIds).toHaveLength(1)
      expect(index.lookup(['B', 'pending']).docIds).toHaveLength(1)
      expect(index.lookup(['B', 'completed']).docIds).toHaveLength(0)
    })
  })

  describe('persistence', () => {
    it('saves and loads index', async () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('pending', 'doc2', 0, 1)
      index.insert('completed', 'doc3', 1, 0)

      await index.save()

      // Create new index instance and load
      const loaded = new HashIndex(storage, 'orders', definition)
      await loaded.load()

      expect(loaded.size).toBe(3)
      expect(loaded.lookup('pending').docIds).toHaveLength(2)
      expect(loaded.lookup('completed').docIds).toHaveLength(1)
    })

    it('handles empty index', async () => {
      const index = new HashIndex(storage, 'orders', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })
  })

  describe('row group tracking', () => {
    it('returns unique row groups for lookups', () => {
      const index = new HashIndex(storage, 'orders', definition)

      index.insert('pending', 'doc1', 0, 0)
      index.insert('pending', 'doc2', 0, 1)
      index.insert('pending', 'doc3', 1, 0)
      index.insert('pending', 'doc4', 1, 1)
      index.insert('pending', 'doc5', 2, 0)

      const result = index.lookup('pending')
      expect(result.docIds).toHaveLength(5)
      expect(result.rowGroups).toHaveLength(3)
      expect(result.rowGroups).toContain(0)
      expect(result.rowGroups).toContain(1)
      expect(result.rowGroups).toContain(2)
    })
  })
})

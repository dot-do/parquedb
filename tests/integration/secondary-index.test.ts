/**
 * Integration Tests for Secondary Indexes
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { HashIndex } from '../../src/indexes/secondary/hash'
import { SSTIndex } from '../../src/indexes/secondary/sst'
import { IndexManager } from '../../src/indexes/manager'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { IndexDefinition } from '../../src/indexes/types'

describe('Secondary Index Integration', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('HashIndex with real data patterns', () => {
    it('indexes and queries IMDB title types', () => {
      const definition: IndexDefinition = {
        name: 'idx_titleType',
        type: 'hash',
        fields: [{ path: '$index_titleType' }],
      }

      const index = new HashIndex(storage, 'titles', definition)

      // Simulate IMDB title data
      const titles = [
        { $id: 'tt001', $index_titleType: 'movie', name: 'The Matrix' },
        { $id: 'tt002', $index_titleType: 'movie', name: 'Inception' },
        { $id: 'tt003', $index_titleType: 'tvSeries', name: 'Breaking Bad' },
        { $id: 'tt004', $index_titleType: 'short', name: 'Short Film' },
        { $id: 'tt005', $index_titleType: 'movie', name: 'Interstellar' },
      ]

      index.buildFromArray(
        titles.map((doc, i) => ({
          doc,
          docId: doc.$id,
          rowGroup: 0,
          rowOffset: i,
        }))
      )

      // Query for movies
      const movies = index.lookup('movie')
      expect(movies.docIds).toHaveLength(3)
      expect(movies.docIds).toContain('tt001')
      expect(movies.docIds).toContain('tt002')
      expect(movies.docIds).toContain('tt005')

      // Query for TV series
      const tvSeries = index.lookup('tvSeries')
      expect(tvSeries.docIds).toHaveLength(1)
      expect(tvSeries.docIds).toContain('tt003')
    })

    it('handles O*NET occupation data with SOC codes', () => {
      const definition: IndexDefinition = {
        name: 'idx_majorGroup',
        type: 'hash',
        fields: [{ path: '$index_majorGroup' }],
      }

      const index = new HashIndex(storage, 'occupations', definition)

      // Simulate O*NET occupation data
      const occupations = [
        { $id: 'occ1', $index_majorGroup: 15, name: 'Software Developer' },
        { $id: 'occ2', $index_majorGroup: 15, name: 'Data Scientist' },
        { $id: 'occ3', $index_majorGroup: 29, name: 'Nurse' },
        { $id: 'occ4', $index_majorGroup: 15, name: 'Web Developer' },
      ]

      index.buildFromArray(
        occupations.map((doc, i) => ({
          doc,
          docId: doc.$id,
          rowGroup: 0,
          rowOffset: i,
        }))
      )

      // Query for major group 15 (Computer occupations)
      const techJobs = index.lookup(15)
      expect(techJobs.docIds).toHaveLength(3)

      // Query for major group 29 (Healthcare)
      const healthcareJobs = index.lookup(29)
      expect(healthcareJobs.docIds).toHaveLength(1)
    })
  })

  describe('SSTIndex with range queries', () => {
    it('supports price range queries', () => {
      const definition: IndexDefinition = {
        name: 'idx_price',
        type: 'sst',
        fields: [{ path: 'price' }],
      }

      const index = new SSTIndex(storage, 'products', definition)

      const products = [
        { $id: 'p1', price: 9.99, name: 'Widget A' },
        { $id: 'p2', price: 24.99, name: 'Widget B' },
        { $id: 'p3', price: 49.99, name: 'Gadget A' },
        { $id: 'p4', price: 99.99, name: 'Gadget B' },
        { $id: 'p5', price: 149.99, name: 'Device A' },
      ]

      index.buildFromArray(
        products.map((doc, i) => ({
          doc,
          docId: doc.$id,
          rowGroup: 0,
          rowOffset: i,
        }))
      )

      // Products under $50
      const cheap = index.range({ $lt: 50 })
      expect(cheap.docIds).toHaveLength(3)

      // Products $25-$100
      const midRange = index.range({ $gte: 25, $lte: 100 })
      expect(midRange.docIds).toHaveLength(2)
      expect(midRange.docIds).toContain('p3')
      expect(midRange.docIds).toContain('p4')

      // Products over $100
      const expensive = index.range({ $gt: 100 })
      expect(expensive.docIds).toHaveLength(1)
      expect(expensive.docIds).toContain('p5')
    })

    it('supports date range queries', () => {
      const definition: IndexDefinition = {
        name: 'idx_createdAt',
        type: 'sst',
        fields: [{ path: 'createdAt' }],
      }

      const index = new SSTIndex(storage, 'events', definition)

      const events = [
        { $id: 'e1', createdAt: new Date('2024-01-01'), name: 'Event 1' },
        { $id: 'e2', createdAt: new Date('2024-02-15'), name: 'Event 2' },
        { $id: 'e3', createdAt: new Date('2024-03-30'), name: 'Event 3' },
        { $id: 'e4', createdAt: new Date('2024-06-01'), name: 'Event 4' },
      ]

      index.buildFromArray(
        events.map((doc, i) => ({
          doc,
          docId: doc.$id,
          rowGroup: 0,
          rowOffset: i,
        }))
      )

      // Events in Q1 2024
      const q1 = index.range({
        $gte: new Date('2024-01-01'),
        $lt: new Date('2024-04-01'),
      })
      expect(q1.docIds).toHaveLength(3)

      // Events after March
      const afterMarch = index.range({ $gt: new Date('2024-03-31') })
      expect(afterMarch.docIds).toHaveLength(1)
      expect(afterMarch.docIds).toContain('e4')
    })
  })

  describe('IndexManager', () => {
    it('selects best index for filter', async () => {
      const manager = new IndexManager(storage)

      // Create a hash index
      await manager.createIndex('orders', {
        name: 'idx_status',
        type: 'hash',
        fields: [{ path: 'status' }],
      })

      // Create an SST index
      await manager.createIndex('orders', {
        name: 'idx_total',
        type: 'sst',
        fields: [{ path: 'total' }],
      })

      // Equality filter should select hash index
      const eqPlan = await manager.selectIndex('orders', { status: 'pending' })
      expect(eqPlan).not.toBeNull()
      expect(eqPlan?.type).toBe('hash')
      expect(eqPlan?.index.name).toBe('idx_status')

      // Range filter should select SST index
      const rangePlan = await manager.selectIndex('orders', { total: { $gte: 100 } })
      expect(rangePlan).not.toBeNull()
      expect(rangePlan?.type).toBe('sst')
      expect(rangePlan?.index.name).toBe('idx_total')
    })

    it('persists indexes', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('products', {
        name: 'idx_category',
        type: 'hash',
        fields: [{ path: 'category' }],
      })

      await manager.save()

      // Load in a new manager
      const loaded = new IndexManager(storage)
      await loaded.load()

      const indexes = await loaded.listIndexes('products')
      expect(indexes).toHaveLength(1)
      expect(indexes[0].definition.name).toBe('idx_category')
    })

    it('drops indexes', async () => {
      const manager = new IndexManager(storage)

      await manager.createIndex('users', {
        name: 'idx_email',
        type: 'hash',
        fields: [{ path: 'email' }],
      })

      const before = await manager.listIndexes('users')
      expect(before).toHaveLength(1)

      await manager.dropIndex('users', 'idx_email')

      const after = await manager.listIndexes('users')
      expect(after).toHaveLength(0)
    })
  })

  describe('Composite indexes', () => {
    it('supports multi-field hash index', () => {
      const definition: IndexDefinition = {
        name: 'idx_tenant_status',
        type: 'hash',
        fields: [{ path: 'tenantId' }, { path: 'status' }],
      }

      const index = new HashIndex(storage, 'orders', definition)

      const orders = [
        { $id: 'o1', tenantId: 'A', status: 'pending' },
        { $id: 'o2', tenantId: 'A', status: 'completed' },
        { $id: 'o3', tenantId: 'B', status: 'pending' },
        { $id: 'o4', tenantId: 'A', status: 'pending' },
      ]

      index.buildFromArray(
        orders.map((doc, i) => ({
          doc,
          docId: doc.$id,
          rowGroup: 0,
          rowOffset: i,
        }))
      )

      // Query for tenant A with pending status
      const result = index.lookup(['A', 'pending'])
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('o1')
      expect(result.docIds).toContain('o4')
    })
  })
})

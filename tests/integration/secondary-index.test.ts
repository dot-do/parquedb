/**
 * Integration Tests for Secondary Indexes
 *
 * NOTE: SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for range queries.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { HashIndex } from '../../src/indexes/secondary/hash'
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

  // NOTE: SST index tests removed - range queries now use native parquet predicate pushdown

  describe('IndexManager', () => {
    it('selects best index for filter', async () => {
      const manager = new IndexManager(storage)

      // Create a hash index
      await manager.createIndex('orders', {
        name: 'idx_status',
        type: 'hash',
        fields: [{ path: 'status' }],
      })

      // Equality filter should select hash index
      const eqPlan = await manager.selectIndex('orders', { status: 'pending' })
      expect(eqPlan).not.toBeNull()
      expect(eqPlan?.type).toBe('hash')
      expect(eqPlan?.index.name).toBe('idx_status')

      // Range filter no longer uses SST indexes - use parquet predicate pushdown instead
      const rangePlan = await manager.selectIndex('orders', { total: { $gte: 100 } })
      expect(rangePlan).toBeNull() // No index for range queries
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

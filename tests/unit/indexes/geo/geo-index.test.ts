import { describe, it, expect, beforeEach, vi } from 'vitest'
import { GeoIndex } from '../../../../src/indexes/geo/geo-index'
import type { StorageBackend } from '../../../../src/types/storage'
import type { IndexDefinition } from '../../../../src/indexes/types'

// Mock storage backend
function createMockStorage(): StorageBackend {
  const files = new Map<string, Uint8Array>()

  return {
    read: vi.fn(async (path: string) => {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    }),
    write: vi.fn(async (path: string, data: Uint8Array) => {
      files.set(path, data)
    }),
    exists: vi.fn(async (path: string) => files.has(path)),
    delete: vi.fn(async (path: string) => {
      files.delete(path)
    }),
    list: vi.fn(async () => Array.from(files.keys())),
    stat: vi.fn(async (path: string) => {
      const data = files.get(path)
      if (!data) return null
      return { size: data.length, modifiedAt: new Date() }
    }),
  }
}

const mockDefinition: IndexDefinition = {
  name: 'geo_location',
  type: 'geo',
  fields: [{ path: 'location' }],
}

describe('GeoIndex', () => {
  let storage: StorageBackend
  let index: GeoIndex

  beforeEach(() => {
    storage = createMockStorage()
    index = new GeoIndex(storage, 'test', mockDefinition, '')
  })

  describe('insert', () => {
    it('inserts a point', async () => {
      await index.load()
      index.insert('doc1', 37.7749, -122.4194, 0, 0)

      const entry = index.getEntry('doc1')
      expect(entry).toBeDefined()
      expect(entry?.lat).toBe(37.7749)
      expect(entry?.lng).toBe(-122.4194)
    })

    it('computes geohash on insert', async () => {
      await index.load()
      index.insert('doc1', 37.7749, -122.4194, 0, 0)

      const entry = index.getEntry('doc1')
      expect(entry?.geohash).toBeDefined()
      expect(entry?.geohash.length).toBe(6) // Default bucket precision
    })

    it('updates existing entry', async () => {
      await index.load()
      index.insert('doc1', 37.7749, -122.4194, 0, 0)
      index.insert('doc1', 40.7128, -74.0060, 0, 1)

      const entry = index.getEntry('doc1')
      expect(entry?.lat).toBe(40.7128)
      expect(entry?.lng).toBe(-74.0060)
    })

    it('stores row group and offset', async () => {
      await index.load()
      index.insert('doc1', 0, 0, 5, 100)

      const entry = index.getEntry('doc1')
      expect(entry?.rowGroup).toBe(5)
      expect(entry?.rowOffset).toBe(100)
    })
  })

  describe('remove', () => {
    it('removes an existing entry', async () => {
      await index.load()
      index.insert('doc1', 37.7749, -122.4194, 0, 0)
      expect(index.remove('doc1')).toBe(true)
      expect(index.getEntry('doc1')).toBeUndefined()
    })

    it('returns false for non-existent entry', async () => {
      await index.load()
      expect(index.remove('nonexistent')).toBe(false)
    })
  })

  describe('search', () => {
    beforeEach(async () => {
      await index.load()

      // Insert some test points around San Francisco
      const points = [
        { id: 'sf_downtown', lat: 37.7749, lng: -122.4194 },
        { id: 'sf_mission', lat: 37.7599, lng: -122.4148 },
        { id: 'sf_marina', lat: 37.8024, lng: -122.4382 },
        { id: 'oakland', lat: 37.8044, lng: -122.2712 },
        { id: 'palo_alto', lat: 37.4419, lng: -122.1430 },
        { id: 'los_angeles', lat: 34.0522, lng: -118.2437 },
      ]

      points.forEach((p, i) => {
        index.insert(p.id, p.lat, p.lng, 0, i)
      })
    })

    it('finds nearby points', () => {
      // Search from SF downtown
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 5000, // 5km
      })

      expect(results.docIds).toContain('sf_downtown')
      expect(results.docIds).toContain('sf_mission')
      expect(results.docIds).toContain('sf_marina')
      expect(results.docIds).not.toContain('oakland')
      expect(results.docIds).not.toContain('los_angeles')
    })

    it('orders by distance', () => {
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 50000,
      })

      // Downtown should be first (closest to itself)
      expect(results.docIds[0]).toBe('sf_downtown')
      expect(results.distances[0]).toBe(0)

      // Each subsequent distance should be greater or equal
      for (let i = 1; i < results.distances.length; i++) {
        expect(results.distances[i]).toBeGreaterThanOrEqual(results.distances[i - 1])
      }
    })

    it('respects maxDistance', () => {
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 2000,
      })

      // Only very close points
      expect(results.docIds).toContain('sf_downtown')
      expect(results.docIds).not.toContain('oakland')
    })

    it('respects minDistance', () => {
      const results = index.search(37.7749, -122.4194, {
        minDistance: 1000,
        maxDistance: 10000,
      })

      // Should exclude the center point (sf_downtown)
      expect(results.docIds).not.toContain('sf_downtown')
      expect(results.docIds).toContain('sf_mission')
    })

    it('respects limit', () => {
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 100000,
        limit: 3,
      })

      expect(results.docIds.length).toBeLessThanOrEqual(3)
    })

    it('returns empty for no matches', () => {
      const results = index.search(0, 0, {
        maxDistance: 1000,
      })

      expect(results.docIds).toHaveLength(0)
      expect(results.distances).toHaveLength(0)
    })

    it('includes row groups in results', () => {
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 50000,
      })

      expect(results.rowGroups.length).toBe(results.docIds.length)
      results.rowGroups.forEach(rg => {
        expect(typeof rg).toBe('number')
      })
    })

    it('tracks entries scanned', () => {
      const results = index.search(37.7749, -122.4194, {
        maxDistance: 50000,
      })

      expect(results.entriesScanned).toBeGreaterThan(0)
    })
  })

  describe('persistence', () => {
    it('saves and loads correctly', async () => {
      await index.load()

      // Insert data
      index.insert('doc1', 37.7749, -122.4194, 0, 0)
      index.insert('doc2', 40.7128, -74.0060, 1, 5)

      // Save
      await index.save()

      // Create new index and load
      const index2 = new GeoIndex(storage, 'test', mockDefinition, '')
      await index2.load()

      // Verify data persisted
      expect(index2.getEntry('doc1')?.lat).toBe(37.7749)
      expect(index2.getEntry('doc2')?.lat).toBe(40.7128)
    })

    it('handles empty save', async () => {
      await index.load()
      await index.save()

      const index2 = new GeoIndex(storage, 'test', mockDefinition, '')
      await index2.load()

      expect(index2.getAllDocIds().size).toBe(0)
    })
  })

  describe('getAllDocIds', () => {
    it('returns all indexed document IDs', async () => {
      await index.load()
      index.insert('doc1', 0, 0, 0, 0)
      index.insert('doc2', 1, 1, 0, 1)
      index.insert('doc3', 2, 2, 0, 2)

      const ids = index.getAllDocIds()
      expect(ids.size).toBe(3)
      expect(ids.has('doc1')).toBe(true)
      expect(ids.has('doc2')).toBe(true)
      expect(ids.has('doc3')).toBe(true)
    })
  })

  describe('getStats', () => {
    it('returns correct entry count', async () => {
      await index.load()
      index.insert('doc1', 0, 0, 0, 0)
      index.insert('doc2', 1, 1, 0, 1)

      const stats = index.getStats()
      expect(stats.entryCount).toBe(2)
    })

    it('returns bucket count as unique keys', async () => {
      await index.load()

      // Insert points in different geohash cells
      index.insert('doc1', 0, 0, 0, 0)
      index.insert('doc2', 45, 90, 0, 1)

      const stats = index.getStats()
      expect(stats.uniqueKeys).toBeGreaterThanOrEqual(2)
    })

    it('estimates size in bytes', async () => {
      await index.load()
      index.insert('doc1', 0, 0, 0, 0)

      const stats = index.getStats()
      expect(stats.sizeBytes).toBeGreaterThan(0)
    })
  })

  describe('clear', () => {
    it('removes all entries', async () => {
      await index.load()
      index.insert('doc1', 0, 0, 0, 0)
      index.insert('doc2', 1, 1, 0, 1)

      index.clear()

      expect(index.getAllDocIds().size).toBe(0)
      expect(index.getStats().entryCount).toBe(0)
    })
  })
})

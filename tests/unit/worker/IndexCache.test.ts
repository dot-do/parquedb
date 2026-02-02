/**
 * IndexCache Tests
 *
 * Tests for the IndexCache component that manages FTS index loading and caching
 * in Worker environments.
 *
 * NOTE: Hash indexes have been removed - equality queries now use native parquet
 * predicate pushdown on $index_* columns.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  IndexCache,
  type IndexStorageAdapter,
  type IndexCatalog,
  type IndexCatalogEntry,
} from '@/worker/IndexCache'

// =============================================================================
// Mock Storage Adapter
// =============================================================================

/**
 * Create a mock storage adapter for testing
 */
function createMockStorage(files: Map<string, Uint8Array> = new Map()): IndexStorageAdapter {
  return {
    async read(path: string): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) {
        throw new Error(`File not found: ${path}`)
      }
      return data
    },
    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },
  }
}

/**
 * Create a mock index catalog
 */
function createMockCatalog(entries: IndexCatalogEntry[]): Uint8Array {
  const catalog: IndexCatalog = {
    version: 2,
    indexes: entries,
  }
  return new TextEncoder().encode(JSON.stringify(catalog))
}

// =============================================================================
// Test Suites
// =============================================================================

describe('IndexCache', () => {
  let storage: IndexStorageAdapter
  let files: Map<string, Uint8Array>

  beforeEach(() => {
    files = new Map()
    storage = createMockStorage(files)
  })

  // ===========================================================================
  // Catalog Loading Tests
  // ===========================================================================

  describe('loadCatalog', () => {
    it('should return empty array when catalog does not exist', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('nonexistent-dataset')

      expect(entries).toEqual([])
    })

    it('should load and parse catalog correctly', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'name_fts',
          type: 'fts',
          field: 'name',
          path: 'indexes/fts/name.fts.json',
          sizeBytes: 2048,
          entryCount: 200,
        },
      ]

      files.set('test-dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('test-dataset')

      expect(entries).toHaveLength(1)
      expect(entries[0].name).toBe('name_fts')
      expect(entries[0].type).toBe('fts')
    })

    it('should cache catalog after first load', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'test',
          type: 'fts',
          field: 'test',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
      ]

      files.set('cached-dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const readSpy = vi.fn(storage.read.bind(storage))
      const spiedStorage = { ...storage, read: readSpy }
      const cache = new IndexCache(spiedStorage)

      // First load
      await cache.loadCatalog('cached-dataset')
      expect(readSpy).toHaveBeenCalledTimes(1)

      // Second load should use cache
      await cache.loadCatalog('cached-dataset')
      expect(readSpy).toHaveBeenCalledTimes(1)
    })

    it('should filter out non-FTS indexes from catalog', async () => {
      // Simulating old catalog with hash indexes that should be filtered
      const mixedCatalog = {
        version: 2,
        indexes: [
          {
            name: 'titleType',
            type: 'hash',
            field: '$index_titleType',
            path: 'indexes/secondary/titleType.hash.idx',
            sizeBytes: 1024,
            entryCount: 100,
          },
          {
            name: 'name_fts',
            type: 'fts',
            field: 'name',
            path: 'indexes/fts/name.fts.json',
            sizeBytes: 2048,
            entryCount: 200,
          },
        ],
      }

      files.set('mixed-dataset/indexes/_catalog.json', new TextEncoder().encode(JSON.stringify(mixedCatalog)))

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('mixed-dataset')

      // Should only include FTS index
      expect(entries).toHaveLength(1)
      expect(entries[0].name).toBe('name_fts')
      expect(entries[0].type).toBe('fts')
    })
  })

  // ===========================================================================
  // Index Field Lookup Tests
  // ===========================================================================

  describe('getIndexForField', () => {
    it('should return null when no index exists for field', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'name_fts',
          type: 'fts',
          field: 'name',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
      ]

      files.set('test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entry = await cache.getIndexForField('test', 'nonexistent')

      expect(entry).toBeNull()
    })

    it('should return correct index entry for field', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'name_fts',
          type: 'fts',
          field: 'name',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
        {
          name: 'description_fts',
          type: 'fts',
          field: 'description',
          path: 'desc.idx',
          sizeBytes: 200,
          entryCount: 20,
        },
      ]

      files.set('test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entry = await cache.getIndexForField('test', 'description')

      expect(entry).not.toBeNull()
      expect(entry!.name).toBe('description_fts')
      expect(entry!.type).toBe('fts')
    })
  })

  // ===========================================================================
  // Index Selection Tests
  // ===========================================================================

  describe('selectIndex', () => {
    beforeEach(() => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'name_fts',
          type: 'fts',
          field: 'name',
          path: 'indexes/fts/name.fts.json',
          sizeBytes: 4096,
          entryCount: 500,
        },
      ]

      files.set('indexed-dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))
    })

    it('should return null when no catalog exists', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('empty-dataset', { status: 'active' })

      expect(selected).toBeNull()
    })

    it('should return null when filter has no indexed fields', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { nonIndexedField: 'value' })

      expect(selected).toBeNull()
    })

    it('should return null for equality conditions (hash indexes removed)', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { name: 'test' })

      // Equality conditions now use native parquet predicate pushdown, not secondary indexes
      expect(selected).toBeNull()
    })

    it('should select FTS index for $text operator', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { $text: { $search: 'inception' } })

      expect(selected).not.toBeNull()
      expect(selected!.type).toBe('fts')
      expect(selected!.entry.name).toBe('name_fts')
    })

    it('should skip logical operators when selecting index', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', {
        $and: [{ $text: { $search: 'test' } }],
      })

      // $and is a logical operator, so it won't match directly
      expect(selected).toBeNull()
    })
  })

  // ===========================================================================
  // Cache Management Tests
  // ===========================================================================

  describe('cache management', () => {
    it('should track cache statistics', async () => {
      const cache = new IndexCache(storage, { maxCacheBytes: 1024 * 1024 })
      const stats = cache.getCacheStats()

      expect(stats.catalogCount).toBe(0)
      expect(stats.indexCount).toBe(0)
      expect(stats.cacheBytes).toBe(0)
      expect(stats.maxBytes).toBe(1024 * 1024)
    })

    it('should clear all caches', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'test',
          type: 'fts',
          field: 'test',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
      ]

      files.set('dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      await cache.loadCatalog('dataset')

      let stats = cache.getCacheStats()
      expect(stats.catalogCount).toBe(1)

      cache.clearCache()

      stats = cache.getCacheStats()
      expect(stats.catalogCount).toBe(0)
      expect(stats.indexCount).toBe(0)
      expect(stats.cacheBytes).toBe(0)
    })
  })

  // ===========================================================================
  // FTS Index Tests
  // ===========================================================================

  describe('getFTSIndex', () => {
    it('should return null when no FTS index exists', async () => {
      // Empty catalog
      files.set('no-fts/indexes/_catalog.json', createMockCatalog([]))

      const cache = new IndexCache(storage)
      const ftsIndex = await cache.getFTSIndex('no-fts')

      expect(ftsIndex).toBeNull()
    })

    it('should return FTS index entry when available', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'name_fts',
          type: 'fts',
          field: 'name',
          path: 'indexes/fts/name.fts.json',
          sizeBytes: 4096,
          entryCount: 500,
        },
      ]

      files.set('with-fts/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const ftsIndex = await cache.getFTSIndex('with-fts')

      expect(ftsIndex).not.toBeNull()
      expect(ftsIndex!.type).toBe('fts')
      expect(ftsIndex!.name).toBe('name_fts')
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('should handle catalog read errors gracefully', async () => {
      const errorStorage: IndexStorageAdapter = {
        async read(_path: string): Promise<Uint8Array> {
          throw new Error('Storage error')
        },
        async exists(_path: string): Promise<boolean> {
          return true
        },
      }

      const cache = new IndexCache(errorStorage)
      const entries = await cache.loadCatalog('error-dataset')

      expect(entries).toEqual([])
    })

    it('should handle invalid catalog version', async () => {
      const invalidCatalog = { version: 99, indexes: [] }
      files.set('invalid/indexes/_catalog.json', new TextEncoder().encode(JSON.stringify(invalidCatalog)))

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('invalid')

      expect(entries).toEqual([])
    })
  })
})

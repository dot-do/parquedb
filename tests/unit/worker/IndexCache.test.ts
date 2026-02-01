/**
 * IndexCache Tests
 *
 * Tests for the IndexCache component that manages secondary index loading,
 * caching, and bloom filter pre-filtering in Worker environments.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  IndexCache,
  type IndexStorageAdapter,
  type IndexCatalog,
  type IndexCatalogEntry,
  type ShardedIndexManifest,
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

/**
 * Create a mock sharded index manifest
 */
function createMockManifest(config: Partial<ShardedIndexManifest>): Uint8Array {
  const manifest: ShardedIndexManifest = {
    version: 3,
    type: 'hash',
    field: '$index_titleType',
    sharding: 'by-value',
    compact: true,
    shards: [],
    ...config,
  }
  return new TextEncoder().encode(JSON.stringify(manifest))
}

/**
 * Create a v3 compact shard format
 * Format: [version (1 byte)][flags (1 byte)][entryCount (4 bytes BE)][entries...]
 * Entry: [rowGroup (2 bytes BE)][rowOffset (varint)][docIdLen (1 byte)][docId (string)]
 */
function createCompactShard(entries: Array<{ docId: string; rowGroup: number; rowOffset: number }>): Uint8Array {
  // Calculate size needed
  let size = 6 // header: version + flags + entryCount
  for (const e of entries) {
    size += 2 // rowGroup
    size += 1 // varint rowOffset (assuming < 128)
    size += 1 // docIdLen
    size += e.docId.length // docId
  }

  const buffer = new Uint8Array(size)
  const view = new DataView(buffer.buffer)
  let offset = 0

  // Header
  buffer[offset++] = 3 // version
  buffer[offset++] = 0 // flags (no keyHash)
  view.setUint32(offset, entries.length, false) // big endian
  offset += 4

  // Entries
  for (const e of entries) {
    view.setUint16(offset, e.rowGroup, false) // big endian
    offset += 2
    buffer[offset++] = e.rowOffset // varint (simple case)
    buffer[offset++] = e.docId.length
    for (let i = 0; i < e.docId.length; i++) {
      buffer[offset++] = e.docId.charCodeAt(i)
    }
  }

  return buffer
}

/**
 * Create a mock bloom filter in PQBF format
 * Header: PQBF (4 bytes) + version (2 bytes) + numHashes (2 bytes) + filterSize (4 bytes) + numRowGroups (2 bytes) + reserved (2 bytes)
 * Data: value bloom filter bits
 */
function createMockBloomFilter(values: unknown[], numHashes: number = 3, filterSizeBytes: number = 128): Uint8Array {
  const header = new Uint8Array(16)
  const view = new DataView(header.buffer)

  // Magic: PQBF
  header[0] = 0x50 // P
  header[1] = 0x51 // Q
  header[2] = 0x42 // B
  header[3] = 0x46 // F

  // Version
  view.setUint16(4, 1, false) // big endian

  // numHashes
  view.setUint16(6, numHashes, false) // big endian

  // filterSize
  view.setUint32(8, filterSizeBytes, false) // big endian

  // numRowGroups (not used in basic tests)
  view.setUint16(12, 0, false)

  // Create filter bits
  const bits = new Uint8Array(filterSizeBytes)

  // Simple MurmurHash3 implementation for test setup
  function hashValue(value: unknown, seed: number): number {
    let encoded: Uint8Array
    if (value === null || value === undefined) {
      encoded = new Uint8Array([0])
    } else if (typeof value === 'number') {
      const buf = new ArrayBuffer(8)
      const dv = new DataView(buf)
      dv.setFloat64(0, value, false)
      encoded = new Uint8Array(buf)
    } else if (typeof value === 'boolean') {
      encoded = new Uint8Array([value ? 1 : 0])
    } else if (typeof value === 'string') {
      encoded = new TextEncoder().encode(value)
    } else {
      encoded = new TextEncoder().encode(JSON.stringify(value))
    }

    // Simple hash for testing
    let hash = seed
    for (let i = 0; i < encoded.length; i++) {
      hash = ((hash << 5) + hash) ^ encoded[i]
      hash = hash >>> 0 // Convert to unsigned
    }
    return hash
  }

  // Add values to bloom filter
  const filterBits = filterSizeBytes * 8
  for (const value of values) {
    const h1 = hashValue(value, 0)
    const h2 = hashValue(value, h1)

    for (let i = 0; i < numHashes; i++) {
      const hash = ((h1 + i * h2) >>> 0) % filterBits
      const byteIndex = Math.floor(hash / 8)
      const bitOffset = hash % 8
      bits[byteIndex] |= 1 << bitOffset
    }
  }

  // Combine header and bits
  const result = new Uint8Array(header.length + bits.length)
  result.set(header, 0)
  result.set(bits, header.length)

  return result
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
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'indexes/secondary/titleType.hash.idx',
          sizeBytes: 1024,
          entryCount: 100,
        },
        {
          name: 'startYear',
          type: 'sst',
          field: '$index_startYear',
          path: 'indexes/secondary/startYear.sst.idx',
          sizeBytes: 2048,
          entryCount: 200,
        },
      ]

      files.set('test-dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('test-dataset')

      expect(entries).toHaveLength(2)
      expect(entries[0].name).toBe('titleType')
      expect(entries[0].type).toBe('hash')
      expect(entries[1].name).toBe('startYear')
      expect(entries[1].type).toBe('sst')
    })

    it('should cache catalog after first load', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'test',
          type: 'hash',
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

    it('should detect sharded indexes from manifestPath', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'indexes/secondary/titleType',
          sizeBytes: 0,
          entryCount: 1000,
          sharded: true,
          manifestPath: 'indexes/secondary/titleType/_manifest.json',
        },
      ]

      files.set('sharded-dataset/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sharded-dataset')

      expect(entries[0].sharded).toBe(true)
      expect(entries[0].manifestPath).toBe('indexes/secondary/titleType/_manifest.json')
    })
  })

  // ===========================================================================
  // Index Field Lookup Tests
  // ===========================================================================

  describe('getIndexForField', () => {
    it('should return null when no index exists for field', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
      ]

      files.set('test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entry = await cache.getIndexForField('test', '$index_nonexistent')

      expect(entry).toBeNull()
    })

    it('should return correct index entry for field', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
        {
          name: 'startYear',
          type: 'sst',
          field: '$index_startYear',
          path: 'year.idx',
          sizeBytes: 200,
          entryCount: 20,
        },
      ]

      files.set('test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      const cache = new IndexCache(storage)
      const entry = await cache.getIndexForField('test', '$index_startYear')

      expect(entry).not.toBeNull()
      expect(entry!.name).toBe('startYear')
      expect(entry!.type).toBe('sst')
    })
  })

  // ===========================================================================
  // Index Selection Tests
  // ===========================================================================

  describe('selectIndex', () => {
    beforeEach(() => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'indexes/secondary/titleType.hash.idx',
          sizeBytes: 1024,
          entryCount: 100,
        },
        {
          name: 'startYear',
          type: 'sst',
          field: '$index_startYear',
          path: 'indexes/secondary/startYear.sst.idx',
          sizeBytes: 2048,
          entryCount: 200,
        },
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

    it('should select hash index for equality condition', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { $index_titleType: 'movie' })

      expect(selected).not.toBeNull()
      expect(selected!.type).toBe('hash')
      expect(selected!.entry.name).toBe('titleType')
      expect(selected!.condition).toBe('movie')
    })

    it('should select hash index for $eq condition', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { $index_titleType: { $eq: 'movie' } })

      expect(selected).not.toBeNull()
      expect(selected!.type).toBe('hash')
    })

    it('should select hash index for $in condition', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { $index_titleType: { $in: ['movie', 'short'] } })

      expect(selected).not.toBeNull()
      expect(selected!.type).toBe('hash')
    })

    it('should select SST index for range condition', async () => {
      const cache = new IndexCache(storage)
      const selected = await cache.selectIndex('indexed-dataset', { $index_startYear: { $gte: 2000, $lt: 2020 } })

      expect(selected).not.toBeNull()
      expect(selected!.type).toBe('sst')
      expect(selected!.entry.name).toBe('startYear')
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
        $and: [{ $index_titleType: 'movie' }],
      })

      // $and is a logical operator, so it won't match a hash index directly
      expect(selected).toBeNull()
    })
  })

  // ===========================================================================
  // Sharded Index Tests
  // ===========================================================================

  describe('loadManifest', () => {
    it('should load and parse manifest correctly', async () => {
      const manifest = createMockManifest({
        type: 'hash',
        field: '$index_titleType',
        shards: [
          { name: 'movie', path: 'movie.bin', value: 'movie', entryCount: 500 },
          { name: 'short', path: 'short.bin', value: 'short', entryCount: 200 },
        ],
      })

      files.set('sharded/indexes/secondary/titleType/_manifest.json', manifest)

      const cache = new IndexCache(storage)
      const loaded = await cache.loadManifest('sharded', 'indexes/secondary/titleType/_manifest.json')

      expect(loaded.type).toBe('hash')
      expect(loaded.shards).toHaveLength(2)
      expect(loaded.shards[0].value).toBe('movie')
      expect(loaded.shards[1].value).toBe('short')
    })

    it('should cache manifest after first load', async () => {
      const manifest = createMockManifest({ shards: [] })
      files.set('cached/indexes/secondary/test/_manifest.json', manifest)

      const readSpy = vi.fn(storage.read.bind(storage))
      const spiedStorage = { ...storage, read: readSpy }
      const cache = new IndexCache(spiedStorage)

      await cache.loadManifest('cached', 'indexes/secondary/test/_manifest.json')
      expect(readSpy).toHaveBeenCalledTimes(1)

      await cache.loadManifest('cached', 'indexes/secondary/test/_manifest.json')
      expect(readSpy).toHaveBeenCalledTimes(1)
    })
  })

  describe('loadShardedHashIndex', () => {
    it('should load compact v3 format shard', async () => {
      const shardData = createCompactShard([
        { docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { docId: 'doc3', rowGroup: 1, rowOffset: 0 },
      ])

      files.set('test/shard.bin', shardData)

      const cache = new IndexCache(storage)
      const entries = await cache.loadShardedHashIndex('test/shard.bin', true)

      expect(entries).toHaveLength(3)
      expect(entries[0]).toEqual({ docId: 'doc1', rowGroup: 0, rowOffset: 0 })
      expect(entries[1]).toEqual({ docId: 'doc2', rowGroup: 0, rowOffset: 1 })
      expect(entries[2]).toEqual({ docId: 'doc3', rowGroup: 1, rowOffset: 0 })
    })

    it('should load JSON format shard when not compact', async () => {
      const entries = [
        { docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { docId: 'doc2', rowGroup: 1, rowOffset: 5 },
      ]
      files.set('test/shard.json', new TextEncoder().encode(JSON.stringify(entries)))

      const cache = new IndexCache(storage)
      const loaded = await cache.loadShardedHashIndex('test/shard.json', false)

      expect(loaded).toHaveLength(2)
      expect(loaded[0].docId).toBe('doc1')
      expect(loaded[1].docId).toBe('doc2')
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
      expect(stats.manifestCount).toBe(0)
      expect(stats.bloomFilterCount).toBe(0)
      expect(stats.cacheBytes).toBe(0)
      expect(stats.maxBytes).toBe(1024 * 1024)
    })

    it('should clear all caches', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'test',
          type: 'hash',
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
      expect(stats.manifestCount).toBe(0)
      expect(stats.bloomFilterCount).toBe(0)
      expect(stats.cacheBytes).toBe(0)
    })
  })

  // ===========================================================================
  // Sharded Hash Lookup with Bloom Filter Tests
  // ===========================================================================

  describe('executeHashLookup with sharded index', () => {
    beforeEach(() => {
      // Set up a sharded index with bloom filter
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'titleType',
          type: 'hash',
          field: '$index_titleType',
          path: 'indexes/secondary/titleType',
          sizeBytes: 0,
          entryCount: 1000,
          sharded: true,
          manifestPath: 'indexes/secondary/titleType/_manifest.json',
        },
      ]

      files.set('sharded-test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      // Manifest without bloom filter (to avoid hash function mismatch in tests)
      const manifest = createMockManifest({
        type: 'hash',
        field: '$index_titleType',
        // No bloomPath - tests core sharded index functionality without bloom pre-filtering
        shards: [
          { name: 'movie', path: 'movie.bin', value: 'movie', entryCount: 3 },
          { name: 'short', path: 'short.bin', value: 'short', entryCount: 2 },
        ],
      })
      files.set('sharded-test/indexes/secondary/titleType/_manifest.json', manifest)

      // Shard files
      files.set(
        'sharded-test/indexes/secondary/titleType/movie.bin',
        createCompactShard([
          { docId: 'tt001', rowGroup: 0, rowOffset: 0 },
          { docId: 'tt002', rowGroup: 0, rowOffset: 1 },
          { docId: 'tt003', rowGroup: 1, rowOffset: 0 },
        ])
      )

      files.set(
        'sharded-test/indexes/secondary/titleType/short.bin',
        createCompactShard([
          { docId: 'tt010', rowGroup: 0, rowOffset: 10 },
          { docId: 'tt011', rowGroup: 2, rowOffset: 0 },
        ])
      )
    })

    it('should execute sharded hash lookup and return matching docs', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sharded-test')
      const entry = entries[0]

      const result = await cache.executeHashLookup('sharded-test', entry, 'movie')

      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('tt001')
      expect(result.docIds).toContain('tt002')
      expect(result.docIds).toContain('tt003')
      expect(result.rowGroups).toEqual([0, 1])
      expect(result.exact).toBe(true)
    })

    it('should handle $in operator with sharded index', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sharded-test')
      const entry = entries[0]

      const result = await cache.executeHashLookup('sharded-test', entry, { $in: ['movie', 'short'] })

      expect(result.docIds).toHaveLength(5)
      expect(result.docIds).toContain('tt001')
      expect(result.docIds).toContain('tt010')
      expect(result.rowGroups).toContain(0)
      expect(result.rowGroups).toContain(1)
      expect(result.rowGroups).toContain(2)
    })

    it('should return empty result when value has no matching shard', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sharded-test')
      const entry = entries[0]

      // Query for a value that doesn't have a corresponding shard
      const result = await cache.executeHashLookup('sharded-test', entry, 'documentary')

      // Shard lookup will return empty because there's no shard for 'documentary'
      expect(result.docIds).toHaveLength(0)
    })

    it('should handle value not in any shard', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sharded-test')
      const entry = entries[0]

      const result = await cache.executeHashLookup('sharded-test', entry, 'tvSeries')

      expect(result.docIds).toHaveLength(0)
      expect(result.rowGroups).toHaveLength(0)
    })
  })

  // ===========================================================================
  // Bloom Filter Integration Tests
  // ===========================================================================

  describe('bloom filter pre-filtering', () => {
    beforeEach(() => {
      // Set up a sharded index with bloom filter
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'status',
          type: 'hash',
          field: '$index_status',
          path: 'indexes/secondary/status',
          sizeBytes: 0,
          entryCount: 100,
          sharded: true,
          manifestPath: 'indexes/secondary/status/_manifest.json',
        },
      ]

      files.set('bloom-test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      // Manifest with bloom filter
      const manifest = createMockManifest({
        type: 'hash',
        field: '$index_status',
        bloomPath: '_bloom.bin',
        shards: [
          { name: 'active', path: 'active.bin', value: 'active', entryCount: 2 },
        ],
      })
      files.set('bloom-test/indexes/secondary/status/_manifest.json', manifest)

      // Create a bloom filter with 'active' added
      // Use the production PQBF format that the actual code expects
      const bloomFilter = createMockBloomFilter(['active'])
      files.set('bloom-test/indexes/secondary/status/_bloom.bin', bloomFilter)

      // Shard file
      files.set(
        'bloom-test/indexes/secondary/status/active.bin',
        createCompactShard([
          { docId: 'doc1', rowGroup: 0, rowOffset: 0 },
          { docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        ])
      )
    })

    it('should continue to shard lookup when bloom filter fails to load', async () => {
      // Remove bloom filter to simulate load failure
      files.delete('bloom-test/indexes/secondary/status/_bloom.bin')

      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('bloom-test')
      const entry = entries[0]

      // Should still work even without bloom filter
      const result = await cache.executeHashLookup('bloom-test', entry, 'active')

      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('doc1')
      expect(result.docIds).toContain('doc2')
    })

    it('should return empty result when value not in any shard (bloom filter may or may not filter)', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('bloom-test')
      const entry = entries[0]

      // Query for a value that doesn't exist - shard lookup will return empty
      const result = await cache.executeHashLookup('bloom-test', entry, 'inactive')

      expect(result.docIds).toHaveLength(0)
    })
  })

  // ===========================================================================
  // FTS Index Tests
  // ===========================================================================

  describe('getFTSIndex', () => {
    it('should return null when no FTS index exists', async () => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'test',
          type: 'hash',
          field: 'test',
          path: 'test.idx',
          sizeBytes: 100,
          entryCount: 10,
        },
      ]

      files.set('no-fts/indexes/_catalog.json', createMockCatalog(catalogEntries))

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
  // SST Index Lookup Tests
  // ===========================================================================

  describe('executeSSTLookup with sharded index', () => {
    beforeEach(() => {
      const catalogEntries: IndexCatalogEntry[] = [
        {
          name: 'startYear',
          type: 'sst',
          field: '$index_startYear',
          path: 'indexes/secondary/startYear',
          sizeBytes: 0,
          entryCount: 1000,
          sharded: true,
          manifestPath: 'indexes/secondary/startYear/_manifest.json',
        },
      ]

      files.set('sst-test/indexes/_catalog.json', createMockCatalog(catalogEntries))

      // Manifest with range-based sharding
      const manifest: ShardedIndexManifest = {
        version: 3,
        type: 'sst',
        field: '$index_startYear',
        sharding: 'by-range',
        compact: true,
        shards: [
          { name: '1990-1999', path: '1990s.bin', value: '1990-1999', entryCount: 100, minValue: 1990, maxValue: 1999 },
          { name: '2000-2009', path: '2000s.bin', value: '2000-2009', entryCount: 150, minValue: 2000, maxValue: 2009 },
          { name: '2010-2019', path: '2010s.bin', value: '2010-2019', entryCount: 200, minValue: 2010, maxValue: 2019 },
        ],
      }
      files.set('sst-test/indexes/secondary/startYear/_manifest.json', new TextEncoder().encode(JSON.stringify(manifest)))

      // Shard files
      files.set(
        'sst-test/indexes/secondary/startYear/1990s.bin',
        createCompactShard([
          { docId: 'movie1990', rowGroup: 0, rowOffset: 0 },
          { docId: 'movie1995', rowGroup: 0, rowOffset: 5 },
        ])
      )

      files.set(
        'sst-test/indexes/secondary/startYear/2000s.bin',
        createCompactShard([
          { docId: 'movie2000', rowGroup: 1, rowOffset: 0 },
          { docId: 'movie2005', rowGroup: 1, rowOffset: 5 },
        ])
      )

      files.set(
        'sst-test/indexes/secondary/startYear/2010s.bin',
        createCompactShard([
          { docId: 'movie2010', rowGroup: 2, rowOffset: 0 },
          { docId: 'movie2015', rowGroup: 2, rowOffset: 5 },
        ])
      )
    })

    it('should execute range query on sharded SST index', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sst-test')
      const entry = entries[0]

      // Query for years >= 2000 and < 2015
      const result = await cache.executeSSTLookup('sst-test', entry, { $gte: 2000, $lt: 2015 })

      // Should include 2000s and part of 2010s shards
      expect(result.docIds.length).toBeGreaterThan(0)
      expect(result.exact).toBe(true)
    })

    it('should skip shards outside query range', async () => {
      const cache = new IndexCache(storage)
      const entries = await cache.loadCatalog('sst-test')
      const entry = entries[0]

      // Query for years < 1990 (no matching shards)
      const result = await cache.executeSSTLookup('sst-test', entry, { $lt: 1990 })

      expect(result.docIds).toHaveLength(0)
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

    it('should throw on invalid manifest version', async () => {
      const invalidManifest = { version: 99, type: 'hash', field: 'test', sharding: 'by-value', compact: true, shards: [] }
      files.set('invalid/manifest/_manifest.json', new TextEncoder().encode(JSON.stringify(invalidManifest)))

      const cache = new IndexCache(storage)

      await expect(cache.loadManifest('invalid', 'manifest/_manifest.json')).rejects.toThrow(
        'Unsupported manifest version: 99'
      )
    })
  })
})

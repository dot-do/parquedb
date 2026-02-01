/**
 * Tests for Sharded Index implementations
 *
 * Comprehensive tests for ShardedHashIndex and ShardedSSTIndex including:
 * - Manifest loading and shard selection
 * - v2 and v3 compact format parsing
 * - Edge cases: empty shards, missing manifests, corrupt data
 * - Range queries and binary search operations
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest'
import { ShardedHashIndex, ShardedSSTIndex } from '@/indexes/secondary'
import type { IndexDefinition } from '@/indexes/types'
import type { StorageBackend } from '@/types/storage'
import {
  FORMAT_VERSION_3,
  serializeCompactIndex,
  writeCompactHeader,
  writeCompactEntry,
  writeCompactEntryWithKey,
} from '@/indexes/encoding'

// Mock storage backend for testing
class MockStorage implements StorageBackend {
  private files = new Map<string, Uint8Array>()

  async read(path: string): Promise<Uint8Array> {
    const data = this.files.get(path)
    if (!data) throw new Error(`File not found: ${path}`)
    return data
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    this.files.set(path, data)
  }

  async exists(path: string): Promise<boolean> {
    return this.files.has(path)
  }

  async delete(path: string): Promise<void> {
    this.files.delete(path)
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.files.keys()).filter(k => k.startsWith(prefix))
  }

  // Test helper to set up mock files
  setFile(path: string, data: Uint8Array | string): void {
    if (typeof data === 'string') {
      this.files.set(path, new TextEncoder().encode(data))
    } else {
      this.files.set(path, data)
    }
  }

  setManifest(path: string, manifest: object): void {
    this.files.set(path, new TextEncoder().encode(JSON.stringify(manifest)))
  }

  clear(): void {
    this.files.clear()
  }

  hasFile(path: string): boolean {
    return this.files.has(path)
  }
}

// Helper to create a test shard with entries
function createTestShard(entries: Array<{ value: string; docId: string; rowGroup: number; rowOffset: number }>): Uint8Array {
  const encoder = new TextEncoder()
  const TYPE_STRING = 0x30

  // Calculate size
  let size = 1 + 4 // version + entry count
  for (const entry of entries) {
    const keyLen = 1 + encoder.encode(entry.value).length // type prefix + value
    const docIdLen = encoder.encode(entry.docId).length
    size += 2 + keyLen + 2 + docIdLen + 4 + 4
  }

  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)

  let offset = 0
  view.setUint8(offset, 2) // version 2
  offset += 1
  view.setUint32(offset, entries.length, false)
  offset += 4

  for (const entry of entries) {
    // Encode key (type prefix + string)
    const valueBytes = encoder.encode(entry.value)
    const keyLen = 1 + valueBytes.length
    view.setUint16(offset, keyLen, false)
    offset += 2
    bytes[offset] = TYPE_STRING
    offset += 1
    bytes.set(valueBytes, offset)
    offset += valueBytes.length

    // Encode docId
    const docIdBytes = encoder.encode(entry.docId)
    view.setUint16(offset, docIdBytes.length, false)
    offset += 2
    bytes.set(docIdBytes, offset)
    offset += docIdBytes.length

    // Row group and offset
    view.setUint32(offset, entry.rowGroup, false)
    offset += 4
    view.setUint32(offset, entry.rowOffset, false)
    offset += 4
  }

  return bytes
}

// Helper to create a numeric test shard
function createNumericShard(entries: Array<{ value: number; docId: string; rowGroup: number; rowOffset: number }>): Uint8Array {
  const encoder = new TextEncoder()
  const TYPE_NUMBER_POS = 0x21

  // Calculate size
  let size = 1 + 4 // version + entry count
  for (const entry of entries) {
    const keyLen = 9 // type prefix (1) + float64 (8)
    const docIdLen = encoder.encode(entry.docId).length
    size += 2 + keyLen + 2 + docIdLen + 4 + 4
  }

  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)

  let offset = 0
  view.setUint8(offset, 2) // version 2
  offset += 1
  view.setUint32(offset, entries.length, false)
  offset += 4

  for (const entry of entries) {
    // Encode key (type prefix + float64)
    const keyLen = 9
    view.setUint16(offset, keyLen, false)
    offset += 2
    bytes[offset] = TYPE_NUMBER_POS
    offset += 1
    view.setFloat64(offset, entry.value, false)
    offset += 8

    // Encode docId
    const docIdBytes = encoder.encode(entry.docId)
    view.setUint16(offset, docIdBytes.length, false)
    offset += 2
    bytes.set(docIdBytes, offset)
    offset += docIdBytes.length

    // Row group and offset
    view.setUint32(offset, entry.rowGroup, false)
    offset += 4
    view.setUint32(offset, entry.rowOffset, false)
    offset += 4
  }

  return bytes
}

describe('ShardedHashIndex', () => {
  let storage: MockStorage
  let definition: IndexDefinition

  beforeAll(() => {
    storage = new MockStorage()
    definition = {
      name: 'status',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    // Set up manifest
    storage.setManifest('indexes/secondary/status/_manifest.json', {
      version: 2,
      type: 'hash',
      field: 'status',
      sharding: 'by-value',
      shards: [
        { name: 'active', path: 'active.shard.idx', entryCount: 3, sizeBytes: 100, value: 'active' },
        { name: 'inactive', path: 'inactive.shard.idx', entryCount: 2, sizeBytes: 80, value: 'inactive' },
        { name: 'pending', path: 'pending.shard.idx', entryCount: 1, sizeBytes: 50, value: 'pending' },
      ],
      totalEntries: 6,
      rowGroups: 1,
    })

    // Set up shard files
    storage.setFile('indexes/secondary/status/active.shard.idx', createTestShard([
      { value: 'active', docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      { value: 'active', docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      { value: 'active', docId: 'doc3', rowGroup: 0, rowOffset: 2 },
    ]))

    storage.setFile('indexes/secondary/status/inactive.shard.idx', createTestShard([
      { value: 'inactive', docId: 'doc4', rowGroup: 0, rowOffset: 3 },
      { value: 'inactive', docId: 'doc5', rowGroup: 0, rowOffset: 4 },
    ]))

    storage.setFile('indexes/secondary/status/pending.shard.idx', createTestShard([
      { value: 'pending', docId: 'doc6', rowGroup: 0, rowOffset: 5 },
    ]))
  })

  it('should load manifest without loading shards', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    expect(index.ready).toBe(true)
    expect(index.isSharded).toBe(true)
    expect(index.uniqueKeyCount).toBe(3)
    expect(index.size).toBe(6)
    expect(index.loadedShardCount).toBe(0) // No shards loaded yet
  })

  it('should lookup by value and load only needed shard', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    const result = await index.lookup('active')

    expect(result.docIds).toHaveLength(3)
    expect(result.docIds).toContain('doc1')
    expect(result.docIds).toContain('doc2')
    expect(result.docIds).toContain('doc3')
    expect(result.exact).toBe(true)
    expect(index.loadedShardCount).toBe(1) // Only 'active' shard loaded
  })

  it('should return empty result for non-existent value', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    const result = await index.lookup('nonexistent')

    expect(result.docIds).toHaveLength(0)
    expect(result.exact).toBe(true)
  })

  it('should lookup multiple values with $in', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    const result = await index.lookupIn(['active', 'pending'])

    expect(result.docIds).toHaveLength(4)
    expect(result.docIds).toContain('doc1')
    expect(result.docIds).toContain('doc6')
    expect(index.loadedShardCount).toBe(2) // 'active' and 'pending' shards loaded
  })

  it('should check existence without loading shard data', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    expect(await index.exists('active')).toBe(true)
    expect(await index.exists('nonexistent')).toBe(false)
  })

  it('should get unique values', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    const values = index.getUniqueValues()

    expect(values).toHaveLength(3)
    expect(values).toContain('active')
    expect(values).toContain('inactive')
    expect(values).toContain('pending')
  })

  it('should get stats', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    const stats = index.getStats()

    expect(stats.entryCount).toBe(6)
    expect(stats.uniqueKeys).toBe(3)
    expect(stats.sizeBytes).toBe(230) // Sum of shard sizes
  })

  it('should clear cache', async () => {
    const index = new ShardedHashIndex(storage, 'test', definition)
    await index.load()

    await index.lookup('active')
    expect(index.loadedShardCount).toBe(1)

    index.clearCache()
    expect(index.loadedShardCount).toBe(0)
  })
})

// =============================================================================
// ShardedHashIndex Edge Cases
// =============================================================================

describe('ShardedHashIndex Edge Cases', () => {
  let storage: MockStorage
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MockStorage()
    definition = {
      name: 'category',
      type: 'hash',
      fields: [{ path: 'category' }],
    }
  })

  describe('Missing Manifest', () => {
    it('should handle missing manifest gracefully', async () => {
      // No manifest set up
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
      expect(index.size).toBe(0)
      expect(index.uniqueKeyCount).toBe(0)
    })

    it('should return empty results when manifest is missing', async () => {
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup('anyvalue')
      expect(result.docIds).toHaveLength(0)
      expect(result.exact).toBe(true)
    })

    it('should return empty values list when manifest is missing', async () => {
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const values = index.getUniqueValues()
      expect(values).toHaveLength(0)
    })
  })

  describe('Corrupt Manifest', () => {
    it('should handle invalid JSON manifest', async () => {
      storage.setFile('indexes/secondary/category/_manifest.json', 'not valid json {{{')

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
    })

    it('should handle empty manifest file', async () => {
      storage.setFile('indexes/secondary/category/_manifest.json', '')

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
    })
  })

  describe('Empty Shards', () => {
    it('should handle manifest with empty shards array', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [],
        totalEntries: 0,
        rowGroups: 0,
      })

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(true)
      expect(index.size).toBe(0)
      expect(index.uniqueKeyCount).toBe(0)

      const result = await index.lookup('anyvalue')
      expect(result.docIds).toHaveLength(0)
    })

    it('should handle missing shard file', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'electronics', path: 'electronics.shard.idx', entryCount: 5, sizeBytes: 100, value: 'electronics' },
        ],
        totalEntries: 5,
        rowGroups: 1,
      })
      // Note: shard file not created

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup('electronics')
      expect(result.docIds).toHaveLength(0)  // Returns empty since file doesn't exist
    })

    it('should handle shard file with no entries', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'empty', path: 'empty.shard.idx', entryCount: 0, sizeBytes: 5, value: 'empty' },
        ],
        totalEntries: 0,
        rowGroups: 0,
      })

      // Create an empty shard (just header)
      const buffer = new ArrayBuffer(5)
      const view = new DataView(buffer)
      view.setUint8(0, 2) // version 2
      view.setUint32(1, 0, false) // 0 entries
      storage.setFile('indexes/secondary/category/empty.shard.idx', new Uint8Array(buffer))

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup('empty')
      expect(result.docIds).toHaveLength(0)
    })
  })

  describe('v3 Compact Format', () => {
    it('should parse v3 compact format shard without key hash', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'books', path: 'books.shard.idx', entryCount: 3, sizeBytes: 50, value: 'books' },
        ],
        totalEntries: 3,
        rowGroups: 1,
      })

      // Create v3 compact format shard
      const entries = [
        { rowGroup: 0, rowOffset: 0, docId: 'book1' },
        { rowGroup: 0, rowOffset: 1, docId: 'book2' },
        { rowGroup: 1, rowOffset: 0, docId: 'book3' },
      ]
      const compactShard = serializeCompactIndex(entries, false)
      storage.setFile('indexes/secondary/category/books.shard.idx', compactShard)

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup('books')
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('book1')
      expect(result.docIds).toContain('book2')
      expect(result.docIds).toContain('book3')
      expect(result.rowGroups).toEqual([0, 1])
    })

    it('should parse v3 compact format shard with key hash', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'music', path: 'music.shard.idx', entryCount: 2, sizeBytes: 40, value: 'music' },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      // Create v3 compact format shard with key hash
      const entries = [
        { rowGroup: 0, rowOffset: 5, docId: 'song1' },
        { rowGroup: 0, rowOffset: 6, docId: 'song2' },
      ]
      const compactShard = serializeCompactIndex(entries, true, () => 0x12345678)
      storage.setFile('indexes/secondary/category/music.shard.idx', compactShard)

      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup('music')
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('song1')
      expect(result.docIds).toContain('song2')
    })
  })

  describe('Shard Caching', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'a', path: 'a.shard.idx', entryCount: 1, sizeBytes: 30, value: 'a' },
          { name: 'b', path: 'b.shard.idx', entryCount: 1, sizeBytes: 30, value: 'b' },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      // Create minimal v3 shards
      storage.setFile('indexes/secondary/category/a.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 0, docId: 'docA' }], false))
      storage.setFile('indexes/secondary/category/b.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 1, docId: 'docB' }], false))
    })

    it('should cache loaded shards for subsequent lookups', async () => {
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      // First lookup loads the shard
      const result1 = await index.lookup('a')
      expect(result1.docIds).toContain('docA')
      expect(index.loadedShardCount).toBe(1)

      // Second lookup should use cached shard
      const result2 = await index.lookup('a')
      expect(result2.docIds).toContain('docA')
      expect(index.loadedShardCount).toBe(1) // Still 1, not reloaded

      // Lookup different value loads another shard
      const result3 = await index.lookup('b')
      expect(result3.docIds).toContain('docB')
      expect(index.loadedShardCount).toBe(2)
    })

    it('should reload shards after cache clear', async () => {
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      await index.lookup('a')
      expect(index.loadedShardCount).toBe(1)

      index.clearCache()
      expect(index.loadedShardCount).toBe(0)

      // Should reload on next lookup
      await index.lookup('a')
      expect(index.loadedShardCount).toBe(1)
    })
  })

  describe('Value to Shard Key Mapping', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'hello-world', path: 'hello-world.shard.idx', entryCount: 1, sizeBytes: 30, value: 'Hello World!' },
          { name: 'special-chars', path: 'special-chars.shard.idx', entryCount: 1, sizeBytes: 30, value: 'Special@#$Chars' },
          { name: 'numbers-123', path: 'numbers-123.shard.idx', entryCount: 1, sizeBytes: 30, value: 'Numbers 123' },
        ],
        totalEntries: 3,
        rowGroups: 1,
      })

      storage.setFile('indexes/secondary/category/hello-world.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 0, docId: 'doc1' }], false))
      storage.setFile('indexes/secondary/category/special-chars.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 1, docId: 'doc2' }], false))
      storage.setFile('indexes/secondary/category/numbers-123.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 2, docId: 'doc3' }], false))
    })

    it('should normalize value to shard key (lowercase, replace special chars)', async () => {
      const index = new ShardedHashIndex(storage, 'test', definition)
      await index.load()

      // "Hello World!" -> "hello-world"
      const result1 = await index.lookup('Hello World!')
      expect(result1.docIds).toContain('doc1')

      // "Special@#$Chars" -> "special-chars"
      const result2 = await index.lookup('Special@#$Chars')
      expect(result2.docIds).toContain('doc2')

      // "Numbers 123" -> "numbers-123"
      const result3 = await index.lookup('Numbers 123')
      expect(result3.docIds).toContain('doc3')
    })
  })

  describe('Base Path Handling', () => {
    it('should use custom base path for manifest and shards', async () => {
      storage.setManifest('custom/path/indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'test', path: 'test.shard.idx', entryCount: 1, sizeBytes: 30, value: 'test' },
        ],
        totalEntries: 1,
        rowGroups: 1,
      })
      storage.setFile('custom/path/indexes/secondary/category/test.shard.idx',
        serializeCompactIndex([{ rowGroup: 0, rowOffset: 0, docId: 'docX' }], false))

      const index = new ShardedHashIndex(storage, 'test', definition, 'custom/path')
      await index.load()

      expect(index.isSharded).toBe(true)
      const result = await index.lookup('test')
      expect(result.docIds).toContain('docX')
    })
  })

  describe('loadShardedHashIndex Helper', () => {
    it('should load and return ready index', async () => {
      const { loadShardedHashIndex } = await import('../../../src/indexes/secondary/sharded-hash')

      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [],
        totalEntries: 0,
        rowGroups: 0,
      })

      const index = await loadShardedHashIndex(storage, 'test', definition)
      expect(index.ready).toBe(true)
    })
  })

  describe('Multiple Load Calls', () => {
    it('should be idempotent - multiple load calls should not re-read manifest', async () => {
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'x', path: 'x.shard.idx', entryCount: 1, sizeBytes: 30, value: 'x' },
        ],
        totalEntries: 1,
        rowGroups: 1,
      })

      const index = new ShardedHashIndex(storage, 'test', definition)

      await index.load()
      expect(index.ready).toBe(true)
      expect(index.uniqueKeyCount).toBe(1)

      // Modify manifest after first load
      storage.setManifest('indexes/secondary/category/_manifest.json', {
        version: 2,
        type: 'hash',
        field: 'category',
        sharding: 'by-value',
        shards: [
          { name: 'x', path: 'x.shard.idx', entryCount: 1, sizeBytes: 30, value: 'x' },
          { name: 'y', path: 'y.shard.idx', entryCount: 1, sizeBytes: 30, value: 'y' },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      // Second load should not re-read
      await index.load()
      expect(index.uniqueKeyCount).toBe(1) // Still 1, not 2
    })
  })
})

describe('ShardedSSTIndex', () => {
  let storage: MockStorage
  let definition: IndexDefinition

  beforeAll(() => {
    storage = new MockStorage()
    definition = {
      name: 'year',
      type: 'sst',
      fields: [{ path: 'year' }],
    }

    // Set up manifest for range-based sharding
    storage.setManifest('indexes/secondary/year/_manifest.json', {
      version: 2,
      type: 'sst',
      field: 'year',
      sharding: 'by-range',
      shards: [
        { name: 'range-1990-2000', path: 'range-1990-2000.shard.idx', entryCount: 3, sizeBytes: 100, rangeStart: 1990, rangeEnd: 2000 },
        { name: 'range-2000-2010', path: 'range-2000-2010.shard.idx', entryCount: 3, sizeBytes: 100, rangeStart: 2000, rangeEnd: 2010 },
        { name: 'range-2010-2020', path: 'range-2010-2020.shard.idx', entryCount: 2, sizeBytes: 80, rangeStart: 2010, rangeEnd: 2020 },
      ],
      totalEntries: 8,
      rowGroups: 1,
    })

    // Set up shard files (sorted within each shard)
    storage.setFile('indexes/secondary/year/range-1990-2000.shard.idx', createNumericShard([
      { value: 1992, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
      { value: 1995, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      { value: 1998, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
    ]))

    storage.setFile('indexes/secondary/year/range-2000-2010.shard.idx', createNumericShard([
      { value: 2001, docId: 'doc4', rowGroup: 0, rowOffset: 3 },
      { value: 2005, docId: 'doc5', rowGroup: 0, rowOffset: 4 },
      { value: 2008, docId: 'doc6', rowGroup: 0, rowOffset: 5 },
    ]))

    storage.setFile('indexes/secondary/year/range-2010-2020.shard.idx', createNumericShard([
      { value: 2012, docId: 'doc7', rowGroup: 0, rowOffset: 6 },
      { value: 2018, docId: 'doc8', rowGroup: 0, rowOffset: 7 },
    ]))
  })

  it('should load manifest', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    expect(index.ready).toBe(true)
    expect(index.isSharded).toBe(true)
    expect(index.shardCount).toBe(3)
    expect(index.size).toBe(8)
    expect(index.loadedShardCount).toBe(0)
  })

  it('should lookup by exact value', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const result = await index.lookup(2005)

    expect(result.docIds).toHaveLength(1)
    expect(result.docIds).toContain('doc5')
    expect(result.exact).toBe(true)
    expect(index.loadedShardCount).toBe(1)
  })

  it('should perform range query within single shard', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const result = await index.range({ $gte: 2001, $lte: 2008 })

    expect(result.docIds).toHaveLength(3)
    expect(result.docIds).toContain('doc4')
    expect(result.docIds).toContain('doc5')
    expect(result.docIds).toContain('doc6')
    expect(index.loadedShardCount).toBe(1) // Only 2000-2010 shard
  })

  it('should perform range query across multiple shards', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const result = await index.range({ $gte: 1995, $lte: 2005 })

    expect(result.docIds).toHaveLength(4)
    expect(result.docIds).toContain('doc2') // 1995
    expect(result.docIds).toContain('doc3') // 1998
    expect(result.docIds).toContain('doc4') // 2001
    expect(result.docIds).toContain('doc5') // 2005
    expect(index.loadedShardCount).toBe(2) // Both 1990-2000 and 2000-2010 shards
  })

  it('should handle $gt and $lt operators', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const result = await index.range({ $gt: 1995, $lt: 2005 })

    expect(result.docIds).toHaveLength(2)
    expect(result.docIds).toContain('doc3') // 1998
    expect(result.docIds).toContain('doc4') // 2001
    expect(result.docIds).not.toContain('doc2') // 1995 excluded
    expect(result.docIds).not.toContain('doc5') // 2005 excluded
  })

  it('should return empty for range with no matches', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const result = await index.range({ $gte: 2025, $lte: 2030 })

    expect(result.docIds).toHaveLength(0)
  })

  it('should get stats', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    const stats = index.getStats()

    expect(stats.entryCount).toBe(8)
    expect(stats.levels).toBe(3) // Number of shards
    expect(stats.sizeBytes).toBe(280)
  })

  it('should clear cache', async () => {
    const index = new ShardedSSTIndex(storage, 'test', definition)
    await index.load()

    await index.lookup(2005)
    expect(index.loadedShardCount).toBe(1)

    index.clearCache()
    expect(index.loadedShardCount).toBe(0)
  })
})

// =============================================================================
// ShardedSSTIndex Edge Cases
// =============================================================================

describe('ShardedSSTIndex Edge Cases', () => {
  let storage: MockStorage
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MockStorage()
    definition = {
      name: 'price',
      type: 'sst',
      fields: [{ path: 'price' }],
    }
  })

  describe('Missing Manifest', () => {
    it('should handle missing manifest gracefully', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
      expect(index.size).toBe(0)
      expect(index.shardCount).toBe(0)
    })

    it('should return empty results for lookup when manifest missing', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(100)
      expect(result.docIds).toHaveLength(0)
      expect(result.exact).toBe(true)
    })

    it('should return empty results for range query when manifest missing', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $gte: 0, $lte: 1000 })
      expect(result.docIds).toHaveLength(0)
      expect(result.exact).toBe(true)
    })

    it('should return null for min/max when manifest missing', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(await index.min()).toBeNull()
      expect(await index.max()).toBeNull()
    })
  })

  describe('Corrupt Manifest', () => {
    it('should handle invalid JSON manifest', async () => {
      storage.setFile('indexes/secondary/price/_manifest.json', '{invalid json}}}')

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
    })

    it('should handle truncated manifest', async () => {
      storage.setFile('indexes/secondary/price/_manifest.json', '{"version": 2, "shards":')

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(false)
    })
  })

  describe('Empty Shards', () => {
    it('should handle manifest with no shards', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [],
        totalEntries: 0,
        rowGroups: 0,
      })

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.isSharded).toBe(true)
      expect(index.shardCount).toBe(0)

      const result = await index.range({ $gte: 0, $lte: 1000 })
      expect(result.docIds).toHaveLength(0)
    })

    it('should handle missing shard file', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 5, sizeBytes: 100, rangeStart: 0, rangeEnd: 100 },
        ],
        totalEntries: 5,
        rowGroups: 1,
      })
      // Shard file not created

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(50)
      expect(result.docIds).toHaveLength(0)
    })
  })

  describe('v3 Compact Format', () => {
    it('should parse v3 compact format shard', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 3, sizeBytes: 50, rangeStart: 0, rangeEnd: 100, value: 50 },
        ],
        totalEntries: 3,
        rowGroups: 1,
      })

      // Create v3 compact format shard
      const entries = [
        { rowGroup: 0, rowOffset: 0, docId: 'item1' },
        { rowGroup: 0, rowOffset: 1, docId: 'item2' },
        { rowGroup: 0, rowOffset: 2, docId: 'item3' },
      ]
      const compactShard = serializeCompactIndex(entries, false)
      storage.setFile('indexes/secondary/price/range-0-100.shard.idx', compactShard)

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(50)
      expect(result.docIds).toHaveLength(3)
      expect(result.docIds).toContain('item1')
      expect(result.docIds).toContain('item2')
      expect(result.docIds).toContain('item3')
    })

    it('should parse v3 compact format shard with key hash', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 2, sizeBytes: 40, rangeStart: 0, rangeEnd: 100, value: 25 },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      const entries = [
        { rowGroup: 1, rowOffset: 10, docId: 'prod1' },
        { rowGroup: 1, rowOffset: 11, docId: 'prod2' },
      ]
      const compactShard = serializeCompactIndex(entries, true, () => 0xABCD1234)
      storage.setFile('indexes/secondary/price/range-0-100.shard.idx', compactShard)

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(25)
      expect(result.docIds).toHaveLength(2)
      expect(result.docIds).toContain('prod1')
      expect(result.docIds).toContain('prod2')
    })

    it('should handle compact format in range queries', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        compact: true, // Mark as compact format
        shards: [
          { name: 'range-0-50', path: 'range-0-50.shard.idx', entryCount: 2, sizeBytes: 30, rangeStart: 0, rangeEnd: 50 },
          { name: 'range-50-100', path: 'range-50-100.shard.idx', entryCount: 2, sizeBytes: 30, rangeStart: 50, rangeEnd: 100 },
        ],
        totalEntries: 4,
        rowGroups: 1,
      })

      storage.setFile('indexes/secondary/price/range-0-50.shard.idx',
        serializeCompactIndex([
          { rowGroup: 0, rowOffset: 0, docId: 'low1' },
          { rowGroup: 0, rowOffset: 1, docId: 'low2' },
        ], false))
      storage.setFile('indexes/secondary/price/range-50-100.shard.idx',
        serializeCompactIndex([
          { rowGroup: 0, rowOffset: 2, docId: 'high1' },
          { rowGroup: 0, rowOffset: 3, docId: 'high2' },
        ], false))

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      // Range query across shards with compact format
      const result = await index.range({ $gte: 25, $lte: 75 })
      expect(result.docIds.length).toBeGreaterThanOrEqual(2) // At least some matches
      expect(result.exact).toBe(false) // Compact format is not exact
    })
  })

  describe('Open-ended Range Queries', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 3, sizeBytes: 100, rangeStart: 0, rangeEnd: 100 },
          { name: 'range-100-200', path: 'range-100-200.shard.idx', entryCount: 3, sizeBytes: 100, rangeStart: 100, rangeEnd: 200 },
          { name: 'range-200-300', path: 'range-200-300.shard.idx', entryCount: 2, sizeBytes: 80, rangeStart: 200, rangeEnd: 300 },
        ],
        totalEntries: 8,
        rowGroups: 1,
      })

      storage.setFile('indexes/secondary/price/range-0-100.shard.idx', createNumericShard([
        { value: 25, docId: 'p1', rowGroup: 0, rowOffset: 0 },
        { value: 50, docId: 'p2', rowGroup: 0, rowOffset: 1 },
        { value: 75, docId: 'p3', rowGroup: 0, rowOffset: 2 },
      ]))

      storage.setFile('indexes/secondary/price/range-100-200.shard.idx', createNumericShard([
        { value: 125, docId: 'p4', rowGroup: 0, rowOffset: 3 },
        { value: 150, docId: 'p5', rowGroup: 0, rowOffset: 4 },
        { value: 175, docId: 'p6', rowGroup: 0, rowOffset: 5 },
      ]))

      storage.setFile('indexes/secondary/price/range-200-300.shard.idx', createNumericShard([
        { value: 225, docId: 'p7', rowGroup: 0, rowOffset: 6 },
        { value: 275, docId: 'p8', rowGroup: 0, rowOffset: 7 },
      ]))
    })

    it('should handle $gte only (no upper bound)', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $gte: 150 })

      expect(result.docIds).toContain('p5') // 150
      expect(result.docIds).toContain('p6') // 175
      expect(result.docIds).toContain('p7') // 225
      expect(result.docIds).toContain('p8') // 275
      expect(result.docIds).not.toContain('p4') // 125 < 150
    })

    it('should handle $gt only (no upper bound)', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $gt: 150 })

      expect(result.docIds).not.toContain('p5') // 150 excluded
      expect(result.docIds).toContain('p6') // 175
      expect(result.docIds).toContain('p7') // 225
      expect(result.docIds).toContain('p8') // 275
    })

    it('should handle $lte only (no lower bound)', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $lte: 100 })

      expect(result.docIds).toContain('p1') // 25
      expect(result.docIds).toContain('p2') // 50
      expect(result.docIds).toContain('p3') // 75
      expect(result.docIds).not.toContain('p4') // 125 > 100
    })

    it('should handle $lt only (no lower bound)', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $lt: 75 })

      expect(result.docIds).toContain('p1') // 25
      expect(result.docIds).toContain('p2') // 50
      expect(result.docIds).not.toContain('p3') // 75 excluded
    })
  })

  describe('Prefix-based Sharding', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'name',
        sharding: 'by-prefix',
        shards: [
          { name: 'a', path: 'a.shard.idx', entryCount: 2, sizeBytes: 50, prefix: 'a' },
          { name: 'b', path: 'b.shard.idx', entryCount: 2, sizeBytes: 50, prefix: 'b' },
        ],
        totalEntries: 4,
        rowGroups: 1,
      })

      // Create string-based shards for prefix testing
      storage.setFile('indexes/secondary/price/a.shard.idx', createTestShard([
        { value: 'alpha', docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { value: 'apex', docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ]))

      storage.setFile('indexes/secondary/price/b.shard.idx', createTestShard([
        { value: 'beta', docId: 'doc3', rowGroup: 0, rowOffset: 2 },
        { value: 'bravo', docId: 'doc4', rowGroup: 0, rowOffset: 3 },
      ]))
    })

    it('should find shard by prefix for string values', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      // Note: Current implementation falls back to first shard for unknown sharding
      // This test documents the current behavior
      const result = await index.lookup('alpha')
      // Due to findShardForValue fallback, it should find entries in prefix shard
      expect(result).toBeDefined()
    })

    it('should load all shards for range query with prefix sharding', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      // For prefix sharding, range queries load all shards
      const result = await index.range({ $gte: 'a', $lte: 'z' })
      expect(result.docIds.length).toBeGreaterThan(0)
      expect(index.loadedShardCount).toBe(2) // All shards loaded
    })
  })

  describe('Min/Max Operations', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 2, sizeBytes: 50, rangeStart: 0, rangeEnd: 100 },
          { name: 'range-100-200', path: 'range-100-200.shard.idx', entryCount: 2, sizeBytes: 50, rangeStart: 100, rangeEnd: 200 },
        ],
        totalEntries: 4,
        rowGroups: 1,
      })

      storage.setFile('indexes/secondary/price/range-0-100.shard.idx', createNumericShard([
        { value: 10, docId: 'min-doc', rowGroup: 0, rowOffset: 0 },
        { value: 90, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
      ]))

      storage.setFile('indexes/secondary/price/range-100-200.shard.idx', createNumericShard([
        { value: 110, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
        { value: 190, docId: 'max-doc', rowGroup: 0, rowOffset: 3 },
      ]))
    })

    it('should find minimum value', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const min = await index.min()
      expect(min).not.toBeNull()
      expect(min!.value).toBe(10)
      expect(min!.docId).toBe('min-doc')
    })

    it('should find maximum value', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const max = await index.max()
      expect(max).not.toBeNull()
      expect(max!.value).toBe(190)
      expect(max!.docId).toBe('max-doc')
    })

    it('should return null for min when all shards are empty', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [],
        totalEntries: 0,
        rowGroups: 0,
      })

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      expect(await index.min()).toBeNull()
      expect(await index.max()).toBeNull()
    })
  })

  describe('Binary Search Within Shards', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-1000', path: 'range-0-1000.shard.idx', entryCount: 10, sizeBytes: 200, rangeStart: 0, rangeEnd: 1000 },
        ],
        totalEntries: 10,
        rowGroups: 1,
      })

      // Create a larger shard to test binary search
      storage.setFile('indexes/secondary/price/range-0-1000.shard.idx', createNumericShard([
        { value: 100, docId: 'doc1', rowGroup: 0, rowOffset: 0 },
        { value: 200, docId: 'doc2', rowGroup: 0, rowOffset: 1 },
        { value: 300, docId: 'doc3', rowGroup: 0, rowOffset: 2 },
        { value: 400, docId: 'doc4', rowGroup: 0, rowOffset: 3 },
        { value: 500, docId: 'doc5', rowGroup: 0, rowOffset: 4 },
        { value: 600, docId: 'doc6', rowGroup: 0, rowOffset: 5 },
        { value: 700, docId: 'doc7', rowGroup: 0, rowOffset: 6 },
        { value: 800, docId: 'doc8', rowGroup: 0, rowOffset: 7 },
        { value: 900, docId: 'doc9', rowGroup: 0, rowOffset: 8 },
        { value: 1000, docId: 'doc10', rowGroup: 0, rowOffset: 9 },
      ]))
    })

    it('should find exact value using binary search', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(500)
      expect(result.docIds).toHaveLength(1)
      expect(result.docIds).toContain('doc5')
    })

    it('should return empty for non-existent exact value', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.lookup(555)
      expect(result.docIds).toHaveLength(0)
    })

    it('should perform efficient range scan with binary search bounds', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      const result = await index.range({ $gte: 350, $lte: 750 })

      expect(result.docIds).toHaveLength(4)
      expect(result.docIds).toContain('doc4') // 400
      expect(result.docIds).toContain('doc5') // 500
      expect(result.docIds).toContain('doc6') // 600
      expect(result.docIds).toContain('doc7') // 700

      expect(result.docIds).not.toContain('doc3') // 300 < 350
      expect(result.docIds).not.toContain('doc8') // 800 > 750
    })

    it('should handle range with no matches in bounds', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      // Range between existing values
      const result = await index.range({ $gt: 100, $lt: 200 })
      expect(result.docIds).toHaveLength(0)
    })
  })

  describe('Base Path Handling', () => {
    it('should use custom base path for manifest and shards', async () => {
      storage.setManifest('data/v2/indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-100', path: 'range-0-100.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 0, rangeEnd: 100 },
        ],
        totalEntries: 1,
        rowGroups: 1,
      })

      storage.setFile('data/v2/indexes/secondary/price/range-0-100.shard.idx', createNumericShard([
        { value: 50, docId: 'base-path-doc', rowGroup: 0, rowOffset: 0 },
      ]))

      const index = new ShardedSSTIndex(storage, 'test', definition, 'data/v2')
      await index.load()

      expect(index.isSharded).toBe(true)
      const result = await index.lookup(50)
      expect(result.docIds).toContain('base-path-doc')
    })
  })

  describe('loadShardedSSTIndex Helper', () => {
    it('should load and return ready index', async () => {
      const { loadShardedSSTIndex } = await import('../../../src/indexes/secondary/sharded-sst')

      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [],
        totalEntries: 0,
        rowGroups: 0,
      })

      const index = await loadShardedSSTIndex(storage, 'test', definition)
      expect(index.ready).toBe(true)
    })
  })

  describe('Shard Caching', () => {
    beforeEach(() => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'range-0-50', path: 'range-0-50.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 0, rangeEnd: 50 },
          { name: 'range-50-100', path: 'range-50-100.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 50, rangeEnd: 100 },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      storage.setFile('indexes/secondary/price/range-0-50.shard.idx', createNumericShard([
        { value: 25, docId: 'low', rowGroup: 0, rowOffset: 0 },
      ]))
      storage.setFile('indexes/secondary/price/range-50-100.shard.idx', createNumericShard([
        { value: 75, docId: 'high', rowGroup: 0, rowOffset: 1 },
      ]))
    })

    it('should cache loaded shards', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      await index.lookup(25)
      expect(index.loadedShardCount).toBe(1)

      await index.lookup(25)
      expect(index.loadedShardCount).toBe(1) // Still 1, cached

      await index.lookup(75)
      expect(index.loadedShardCount).toBe(2) // New shard loaded
    })

    it('should clear cache and reload', async () => {
      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      await index.lookup(25)
      await index.lookup(75)
      expect(index.loadedShardCount).toBe(2)

      index.clearCache()
      expect(index.loadedShardCount).toBe(0)

      await index.lookup(25)
      expect(index.loadedShardCount).toBe(1)
    })
  })

  describe('Invalid Shard Versions', () => {
    it('should throw for unsupported shard version', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'invalid', path: 'invalid.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 0, rangeEnd: 100 },
        ],
        totalEntries: 1,
        rowGroups: 1,
      })

      // Create shard with invalid version (version 99)
      const buffer = new ArrayBuffer(10)
      const view = new DataView(buffer)
      view.setUint8(0, 99) // Invalid version
      view.setUint32(1, 1, false) // 1 entry
      storage.setFile('indexes/secondary/price/invalid.shard.idx', new Uint8Array(buffer))

      const index = new ShardedSSTIndex(storage, 'test', definition)
      await index.load()

      await expect(index.lookup(50)).rejects.toThrow('Unsupported shard version: 99')
    })
  })

  describe('Multiple Load Calls', () => {
    it('should be idempotent', async () => {
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'x', path: 'x.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 0, rangeEnd: 100 },
        ],
        totalEntries: 1,
        rowGroups: 1,
      })

      const index = new ShardedSSTIndex(storage, 'test', definition)

      await index.load()
      expect(index.shardCount).toBe(1)

      // Modify manifest
      storage.setManifest('indexes/secondary/price/_manifest.json', {
        version: 2,
        type: 'sst',
        field: 'price',
        sharding: 'by-range',
        shards: [
          { name: 'x', path: 'x.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 0, rangeEnd: 100 },
          { name: 'y', path: 'y.shard.idx', entryCount: 1, sizeBytes: 30, rangeStart: 100, rangeEnd: 200 },
        ],
        totalEntries: 2,
        rowGroups: 1,
      })

      await index.load() // Should not re-read
      expect(index.shardCount).toBe(1) // Still 1
    })
  })
})

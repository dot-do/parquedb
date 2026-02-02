/**
 * Tests for Sharded Index implementations
 *
 * Comprehensive tests for ShardedHashIndex including:
 * - Manifest loading and shard selection
 * - v2 and v3 compact format parsing
 * - Edge cases: empty shards, missing manifests, corrupt data
 *
 * NOTE: SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for range queries.
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest'
import { ShardedHashIndex } from '@/indexes/secondary'
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

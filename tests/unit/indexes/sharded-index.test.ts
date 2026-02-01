/**
 * Tests for Sharded Index implementations
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { ShardedHashIndex, ShardedSSTIndex } from '../../../src/indexes/secondary'
import type { IndexDefinition } from '../../../src/indexes/types'
import type { StorageBackend } from '../../../src/types/storage'

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

/**
 * Error Path and Recovery Tests
 *
 * Tests for graceful handling of:
 * - Corrupted Parquet files
 * - Invalid index data
 * - Malformed events/manifests
 * - Large file handling
 * - Storage failures
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { MemoryBackend, FileNotFoundError } from '../../src/storage/MemoryBackend'
import { HashIndex } from '../../src/indexes/secondary/hash'
import { SSTIndex } from '../../src/indexes/secondary/sst'
import { IndexBloomFilter, BloomFilter } from '../../src/indexes/bloom/bloom-filter'
import {
  FORMAT_VERSION_1,
  FORMAT_VERSION_2,
  FORMAT_VERSION_3,
  readVarint,
  readCompactHeader,
  readCompactEntry,
  deserializeCompactIndex,
} from '../../src/indexes/encoding'
import type { IndexDefinition } from '../../src/indexes/types'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock MemoryBackend that can simulate failures
 */
class FailingBackend extends MemoryBackend {
  private readFailures: Map<string, Error> = new Map()
  private writeFailures: Map<string, Error> = new Map()
  private failAllReads: Error | null = null
  private failAllWrites: Error | null = null

  failReadFor(path: string, error: Error): void {
    this.readFailures.set(path, error)
  }

  failWriteFor(path: string, error: Error): void {
    this.writeFailures.set(path, error)
  }

  failAllReadsWithError(error: Error): void {
    this.failAllReads = error
  }

  failAllWritesWithError(error: Error): void {
    this.failAllWrites = error
  }

  clearFailures(): void {
    this.readFailures.clear()
    this.writeFailures.clear()
    this.failAllReads = null
    this.failAllWrites = null
  }

  override async read(path: string): Promise<Uint8Array> {
    if (this.failAllReads) throw this.failAllReads
    const failure = this.readFailures.get(path)
    if (failure) throw failure
    return super.read(path)
  }

  override async write(path: string, data: Uint8Array): Promise<{ etag: string; size: number }> {
    if (this.failAllWrites) throw this.failAllWrites
    const failure = this.writeFailures.get(path)
    if (failure) throw failure
    return super.write(path, data)
  }
}

/**
 * Create test data
 */
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

// =============================================================================
// Corrupted Parquet File Tests
// =============================================================================

describe('Corrupted Parquet Files', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('Invalid Magic Bytes', () => {
    it('should reject file with wrong magic bytes at start', async () => {
      // Valid Parquet files start with "PAR1" (0x50 0x41 0x52 0x31)
      // Write a file with wrong magic
      const invalidMagic = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04])
      await storage.write('invalid.parquet', invalidMagic)

      const data = await storage.read('invalid.parquet')

      // Check that magic bytes are invalid
      const magic = String.fromCharCode(...data.slice(0, 4))
      expect(magic).not.toBe('PAR1')
    })

    it('should reject file with wrong magic bytes at end', async () => {
      // Valid Parquet files end with "PAR1" as well
      const data = new Uint8Array(100)
      // Set start magic correctly
      data[0] = 0x50 // P
      data[1] = 0x41 // A
      data[2] = 0x52 // R
      data[3] = 0x31 // 1
      // But end magic is wrong
      data[96] = 0x00
      data[97] = 0x00
      data[98] = 0x00
      data[99] = 0x00

      await storage.write('wrong_end_magic.parquet', data)

      const readData = await storage.read('wrong_end_magic.parquet')
      const endMagic = String.fromCharCode(...readData.slice(-4))
      expect(endMagic).not.toBe('PAR1')
    })
  })

  describe('Truncated Files', () => {
    it('should handle empty file', async () => {
      await storage.write('empty.parquet', new Uint8Array(0))

      const data = await storage.read('empty.parquet')
      expect(data.length).toBe(0)

      // A file this small cannot be valid Parquet
      expect(data.length).toBeLessThan(4) // Less than magic bytes
    })

    it('should handle file smaller than minimum header', async () => {
      // Parquet minimum header is at least 8 bytes (magic + footer length)
      const tinyFile = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // Just magic, no footer
      await storage.write('tiny.parquet', tinyFile)

      const data = await storage.read('tiny.parquet')
      expect(data.length).toBe(4)
      // File is too small to have valid footer metadata
    })

    it('should handle file with truncated footer', async () => {
      // Create a file that has magic but footer length points beyond file
      const truncated = new Uint8Array(20)
      // Start magic
      truncated[0] = 0x50
      truncated[1] = 0x41
      truncated[2] = 0x52
      truncated[3] = 0x31
      // End magic
      truncated[16] = 0x50
      truncated[17] = 0x41
      truncated[18] = 0x52
      truncated[19] = 0x31
      // Footer length would be at bytes 12-15, set it to a huge value
      const view = new DataView(truncated.buffer)
      view.setUint32(12, 1000000, true) // Footer size bigger than file

      await storage.write('truncated.parquet', truncated)

      const data = await storage.read('truncated.parquet')
      const footerSize = new DataView(data.buffer, data.byteOffset).getUint32(12, true)
      expect(footerSize).toBeGreaterThan(data.length)
    })
  })

  describe('Corrupted Content', () => {
    it('should handle file with random bytes', async () => {
      // Random garbage data
      const garbage = new Uint8Array(1000)
      for (let i = 0; i < garbage.length; i++) {
        garbage[i] = Math.floor(Math.random() * 256)
      }

      await storage.write('garbage.parquet', garbage)

      // Should be able to read the raw bytes
      const data = await storage.read('garbage.parquet')
      expect(data.length).toBe(1000)

      // But the magic check should fail
      const magic = String.fromCharCode(...data.slice(0, 4))
      // Extremely unlikely to be PAR1 by chance
      expect(magic === 'PAR1').toBe(false)
    })

    it('should handle file with partial valid header', async () => {
      // Start with valid magic, then garbage
      const partialValid = new Uint8Array(100)
      partialValid[0] = 0x50 // P
      partialValid[1] = 0x41 // A
      partialValid[2] = 0x52 // R
      partialValid[3] = 0x31 // 1
      // Rest is zeros (invalid)

      await storage.write('partial.parquet', partialValid)

      const data = await storage.read('partial.parquet')
      const magic = String.fromCharCode(...data.slice(0, 4))
      expect(magic).toBe('PAR1')
      // But the rest is not valid Parquet metadata
    })
  })
})

// =============================================================================
// Invalid Index Data Tests
// =============================================================================

describe('Invalid Index Data', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }
  })

  describe('Invalid Version', () => {
    it('should handle unsupported index version', async () => {
      // Create index data with invalid version
      const invalidVersion = new Uint8Array([
        0xFF, // Invalid version (255)
        0x00, 0x00, 0x00, 0x00, // entry count = 0
      ])

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', invalidVersion)

      const index = new HashIndex(storage, 'test', definition)

      // Load should handle invalid version gracefully
      await index.load()

      // Index should be empty but ready (graceful recovery)
      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })

    it('should handle version 0 (reserved)', async () => {
      const version0 = new Uint8Array([
        0x00, // Version 0 (should be invalid)
        0x00, 0x00, 0x00, 0x00,
      ])

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', version0)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should recover gracefully
      expect(index.ready).toBe(true)
    })

    it('should handle future version (forward compatibility)', async () => {
      const futureVersion = new Uint8Array([
        0x10, // Future version 16
        0x00, 0x00, 0x00, 0x00,
      ])

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', futureVersion)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should recover gracefully with empty index
      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })
  })

  describe('Truncated Index Data', () => {
    it('should handle index data shorter than header', async () => {
      // Index header should be at least 5 bytes (version + entry count)
      const tooShort = new Uint8Array([0x01, 0x00]) // Only 2 bytes

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', tooShort)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should recover gracefully
      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })

    it('should handle index with declared entries but missing data', async () => {
      const buffer = new ArrayBuffer(5)
      const view = new DataView(buffer)
      const bytes = new Uint8Array(buffer)

      bytes[0] = FORMAT_VERSION_1 // Version 1
      view.setUint32(1, 100, false) // Claims 100 entries but has no data

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', bytes)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should recover gracefully
      expect(index.ready).toBe(true)
    })

    it('should handle partial entry data', async () => {
      // Create valid header with 1 entry, but truncate the entry data
      const buffer = new ArrayBuffer(10)
      const view = new DataView(buffer)
      const bytes = new Uint8Array(buffer)

      bytes[0] = FORMAT_VERSION_1
      view.setUint32(1, 1, false) // 1 entry
      // Entry should start at offset 5, but we only have 5 more bytes
      // A complete entry needs: keyLen(2) + key(?) + docIdLen(2) + docId(?) + rowGroup(4) + rowOffset(4)
      view.setUint16(5, 100, false) // Claims key is 100 bytes, but we don't have that

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', bytes)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should recover gracefully
      expect(index.ready).toBe(true)
    })
  })

  describe('Corrupted Entry Data', () => {
    it('should handle entry with zero-length key', async () => {
      // Build a v1 index entry with zero-length key
      // Size: version(1) + entryCount(4) + keyLen(2) + docIdLen(2) + docId(4) + rowGroup(4) + rowOffset(4) = 21
      const buffer = new ArrayBuffer(21)
      const view = new DataView(buffer)
      const bytes = new Uint8Array(buffer)

      let offset = 0
      bytes[offset++] = FORMAT_VERSION_1
      view.setUint32(offset, 1, false) // 1 entry
      offset += 4

      // Entry with zero-length key
      view.setUint16(offset, 0, false) // keyLen = 0
      offset += 2
      // No key bytes
      view.setUint16(offset, 4, false) // docIdLen = 4
      offset += 2
      bytes.set(new TextEncoder().encode('doc1'), offset)
      offset += 4
      view.setUint32(offset, 0, false) // rowGroup
      offset += 4
      view.setUint32(offset, 0, false) // rowOffset

      await storage.write('indexes/secondary/test.idx_test.idx.parquet', bytes)

      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Should load but handle zero-length key gracefully
      expect(index.ready).toBe(true)
    })
  })
})

// =============================================================================
// Malformed Manifest Tests
// =============================================================================

describe('Malformed Manifests', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('Invalid JSON', () => {
    it('should handle completely invalid JSON', async () => {
      await storage.write('manifest.json', createTestData('not valid json at all'))

      const data = await storage.read('manifest.json')
      const text = new TextDecoder().decode(data)

      expect(() => JSON.parse(text)).toThrow()
    })

    it('should handle truncated JSON', async () => {
      await storage.write('manifest.json', createTestData('{"version": 1, "segments":'))

      const data = await storage.read('manifest.json')
      const text = new TextDecoder().decode(data)

      expect(() => JSON.parse(text)).toThrow()
    })

    it('should handle JSON with missing required fields', async () => {
      const incompleteManifest = { version: 1 } // Missing dataset, segments, etc.
      await storage.write('manifest.json', createTestData(JSON.stringify(incompleteManifest)))

      const data = await storage.read('manifest.json')
      const parsed = JSON.parse(new TextDecoder().decode(data))

      expect(parsed.version).toBe(1)
      expect(parsed.segments).toBeUndefined()
      expect(parsed.dataset).toBeUndefined()
    })

    it('should handle JSON with wrong types', async () => {
      const wrongTypes = {
        version: 'one', // Should be number
        segments: 'not an array', // Should be array
        nextSeq: 'abc', // Should be number
      }
      await storage.write('manifest.json', createTestData(JSON.stringify(wrongTypes)))

      const data = await storage.read('manifest.json')
      const parsed = JSON.parse(new TextDecoder().decode(data))

      expect(typeof parsed.version).toBe('string')
      expect(Array.isArray(parsed.segments)).toBe(false)
    })

    it('should handle JSON with invalid segment entries', async () => {
      const invalidSegments = {
        version: 1,
        dataset: 'test',
        segments: [
          { seq: 1, path: 'valid.parquet', minTs: 1000, maxTs: 2000, count: 10, sizeBytes: 1024, createdAt: Date.now() },
          { /* missing all fields */ },
          null,
          'not an object',
        ],
        nextSeq: 2,
        totalEvents: 10,
      }
      await storage.write('manifest.json', createTestData(JSON.stringify(invalidSegments)))

      const data = await storage.read('manifest.json')
      const parsed = JSON.parse(new TextDecoder().decode(data))

      expect(parsed.segments.length).toBe(4)
      expect(parsed.segments[1]).toEqual({})
      expect(parsed.segments[2]).toBeNull()
      expect(parsed.segments[3]).toBe('not an object')
    })
  })

  describe('Binary Garbage', () => {
    it('should fail to parse binary data as JSON', async () => {
      const binary = new Uint8Array([0x00, 0xFF, 0xFE, 0x80, 0x90])
      await storage.write('manifest.json', binary)

      const data = await storage.read('manifest.json')
      const text = new TextDecoder().decode(data)

      expect(() => JSON.parse(text)).toThrow()
    })
  })
})

// =============================================================================
// Bloom Filter Error Tests
// =============================================================================

describe('Bloom Filter Errors', () => {
  describe('Invalid Buffer', () => {
    it('should reject buffer smaller than header', () => {
      const tooSmall = new Uint8Array(10) // Header is 16 bytes

      expect(() => IndexBloomFilter.fromBuffer(tooSmall)).toThrow('buffer too small')
    })

    it('should reject buffer with invalid magic', () => {
      const wrongMagic = new Uint8Array(20)
      // Set wrong magic bytes
      wrongMagic[0] = 0x00
      wrongMagic[1] = 0x00
      wrongMagic[2] = 0x00
      wrongMagic[3] = 0x00

      expect(() => IndexBloomFilter.fromBuffer(wrongMagic)).toThrow('bad magic')
    })

    it('should reject unsupported version', () => {
      const wrongVersion = new Uint8Array(20)
      // Correct magic "PQBF"
      wrongVersion[0] = 0x50
      wrongVersion[1] = 0x51
      wrongVersion[2] = 0x42
      wrongVersion[3] = 0x46
      // Wrong version (high bytes)
      const view = new DataView(wrongVersion.buffer)
      view.setUint16(4, 99, false) // Version 99

      expect(() => IndexBloomFilter.fromBuffer(wrongVersion)).toThrow('Unsupported bloom filter version')
    })
  })

  describe('Basic BloomFilter', () => {
    it('should handle empty filter', () => {
      const filter = new BloomFilter(100)

      expect(filter.mightContain('anything')).toBe(false)
      expect(filter.estimateCount()).toBe(0)
    })

    it('should handle filter with all bits set', () => {
      const filter = new BloomFilter(10) // Very small filter

      // Add many items to fill the filter
      for (let i = 0; i < 1000; i++) {
        filter.add(`item_${i}`)
      }

      // Filter should be saturated - everything looks like it might be present
      expect(filter.mightContain('random_value')).toBe(true)
      expect(filter.estimateFPR()).toBeGreaterThan(0.5)
    })
  })
})

// =============================================================================
// Encoding Error Tests
// =============================================================================

describe('Encoding Errors', () => {
  describe('Varint Reading', () => {
    it('should throw on varint extending beyond buffer', () => {
      // Varint with continuation bit set but no more bytes
      const truncated = new Uint8Array([0x80]) // High bit set, needs more

      expect(() => readVarint(truncated, 0)).toThrow('extends beyond buffer')
    })

    it('should throw on varint too large', () => {
      // Create a varint that would overflow (more than 5 bytes for 32-bit)
      const tooLarge = new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80, 0x01])

      expect(() => readVarint(tooLarge, 0)).toThrow('too large')
    })

    it('should read valid single-byte varint', () => {
      const data = new Uint8Array([0x7F]) // 127
      const result = readVarint(data, 0)

      expect(result.value).toBe(127)
      expect(result.bytesRead).toBe(1)
    })

    it('should read valid multi-byte varint', () => {
      const data = new Uint8Array([0x80, 0x01]) // 128
      const result = readVarint(data, 0)

      expect(result.value).toBe(128)
      expect(result.bytesRead).toBe(2)
    })
  })

  describe('Compact Header Reading', () => {
    it('should read valid header', () => {
      const buffer = new Uint8Array(6)
      buffer[0] = FORMAT_VERSION_3 // version
      buffer[1] = 0x01 // flags: hasKeyHash
      const view = new DataView(buffer.buffer)
      view.setUint32(2, 100, false) // entry count

      const result = readCompactHeader(buffer, 0)

      expect(result.header.version).toBe(FORMAT_VERSION_3)
      expect(result.header.hasKeyHash).toBe(true)
      expect(result.header.entryCount).toBe(100)
      expect(result.bytesRead).toBe(6)
    })
  })

  describe('Compact Index Deserialization', () => {
    it('should throw on wrong version', () => {
      const buffer = new Uint8Array(6)
      buffer[0] = FORMAT_VERSION_1 // v1, not v3
      buffer[1] = 0x00
      const view = new DataView(buffer.buffer)
      view.setUint32(2, 0, false)

      expect(() => deserializeCompactIndex(buffer)).toThrow('Unsupported compact index version')
    })
  })
})

// =============================================================================
// Storage Failure Tests
// =============================================================================

describe('Storage Failures', () => {
  let storage: FailingBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new FailingBackend()
    definition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }
  })

  describe('Read Failures', () => {
    it('should handle read failure gracefully', async () => {
      const index = new HashIndex(storage, 'test', definition)

      // Inject read failure
      storage.failReadFor(
        'indexes/secondary/test.idx_test.idx.parquet',
        new Error('Network timeout')
      )

      // Load should handle the error gracefully
      await index.load()

      // Index should be ready but empty
      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })

    it('should handle file not found', async () => {
      const index = new HashIndex(storage, 'test', definition)

      // No file exists - should handle gracefully
      await index.load()

      expect(index.ready).toBe(true)
      expect(index.size).toBe(0)
    })
  })

  describe('Write Failures', () => {
    it('should propagate write failure', async () => {
      const index = new HashIndex(storage, 'test', definition)
      await index.load()

      // Add some data
      index.insert('pending', 'doc1', 0, 0)

      // Inject write failure
      storage.failWriteFor(
        'indexes/secondary/test.idx_test.idx.parquet',
        new Error('Disk full')
      )

      // Save should throw
      await expect(index.save()).rejects.toThrow('Disk full')
    })
  })

  describe('Complete Storage Failure', () => {
    it('should handle all reads failing', async () => {
      storage.failAllReadsWithError(new Error('Storage unavailable'))

      const index = new HashIndex(storage, 'test', definition)

      // Load should handle gracefully
      await index.load()
      expect(index.ready).toBe(true)
    })

    it('should handle all writes failing', async () => {
      storage.failAllWritesWithError(new Error('Storage unavailable'))

      const index = new HashIndex(storage, 'test', definition)
      await index.load()
      index.insert('pending', 'doc1', 0, 0)

      // Save should throw
      await expect(index.save()).rejects.toThrow('Storage unavailable')
    })
  })
})

// =============================================================================
// Large File Handling Tests
// =============================================================================

describe('Large File Handling', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should handle large index with many entries', async () => {
    const definition: IndexDefinition = {
      name: 'idx_large',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    // Insert many entries
    const count = 10000
    for (let i = 0; i < count; i++) {
      index.insert(`status_${i % 100}`, `doc_${i}`, Math.floor(i / 1000), i % 1000)
    }

    expect(index.size).toBe(count)

    // Save and reload
    await index.save()

    const loaded = new HashIndex(storage, 'test', definition)
    await loaded.load()

    expect(loaded.size).toBe(count)
  })

  it('should handle large document IDs', async () => {
    const definition: IndexDefinition = {
      name: 'idx_long_ids',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    // Very long document ID (but still under 255 bytes for compact format)
    const longId = 'doc_' + 'x'.repeat(200)
    index.insert('active', longId, 0, 0)

    expect(index.size).toBe(1)

    const result = index.lookup('active')
    expect(result.docIds).toContain(longId)
  })

  it('should handle large key values', async () => {
    const definition: IndexDefinition = {
      name: 'idx_long_keys',
      type: 'hash',
      fields: [{ path: 'longField' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    // Very long key value
    const longKey = 'value_' + 'y'.repeat(1000)
    index.insert(longKey, 'doc1', 0, 0)

    expect(index.size).toBe(1)

    const result = index.lookup(longKey)
    expect(result.docIds).toContain('doc1')
  })

  it('should handle many unique keys', async () => {
    const definition: IndexDefinition = {
      name: 'idx_many_keys',
      type: 'hash',
      fields: [{ path: 'uniqueId' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    // Insert entries with unique keys
    const count = 5000
    for (let i = 0; i < count; i++) {
      index.insert(`unique_${i}`, `doc_${i}`, 0, i)
    }

    expect(index.size).toBe(count)
    expect(index.uniqueKeyCount).toBe(count)

    // Verify lookups work
    const result = index.lookup('unique_2500')
    expect(result.docIds).toContain('doc_2500')
  })
})

// =============================================================================
// Empty/Zero-Byte File Tests
// =============================================================================

describe('Empty and Zero-Byte Files', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should handle empty index file', async () => {
    await storage.write('indexes/secondary/test.idx_test.idx.parquet', new Uint8Array(0))

    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)
    await index.load()

    // Should recover gracefully
    expect(index.ready).toBe(true)
    expect(index.size).toBe(0)
  })

  it('should handle zero-filled file', async () => {
    const zeros = new Uint8Array(100)
    await storage.write('indexes/secondary/test.idx_test.idx.parquet', zeros)

    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)
    await index.load()

    // Version 0 is invalid, should recover
    expect(index.ready).toBe(true)
    expect(index.size).toBe(0)
  })

  it('should handle single-byte file', async () => {
    await storage.write('indexes/secondary/test.idx_test.idx.parquet', new Uint8Array([0x01]))

    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)
    await index.load()

    // Too short to be valid, should recover
    expect(index.ready).toBe(true)
    expect(index.size).toBe(0)
  })
})

// =============================================================================
// Graceful Degradation Tests
// =============================================================================

describe('Graceful Degradation', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should allow operations after failed load', async () => {
    // Create invalid index data
    await storage.write('indexes/secondary/test.idx_test.idx.parquet', new Uint8Array([0xFF]))

    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)
    await index.load()

    // Index should be usable even after failed load
    expect(index.ready).toBe(true)

    // Should be able to insert
    index.insert('pending', 'doc1', 0, 0)
    expect(index.size).toBe(1)

    // Should be able to lookup
    const result = index.lookup('pending')
    expect(result.docIds).toContain('doc1')

    // Should be able to save
    await index.save()

    // Should be able to reload
    const reloaded = new HashIndex(storage, 'test', definition)
    await reloaded.load()
    expect(reloaded.size).toBe(1)
  })

  it('should continue working after partial data corruption', async () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)
    await index.load()

    // Add valid data
    index.insert('pending', 'doc1', 0, 0)
    index.insert('completed', 'doc2', 0, 1)
    await index.save()

    // Corrupt part of the saved file
    const data = await storage.read('indexes/secondary/test.idx_test.idx.parquet')
    const corrupted = new Uint8Array(data)
    // Corrupt some bytes in the middle (but keep header valid)
    if (corrupted.length > 20) {
      corrupted[15] = 0xFF
      corrupted[16] = 0xFF
    }
    await storage.write('indexes/secondary/test.idx_test.idx.parquet', corrupted)

    // Reload - may lose some data but should not crash
    const reloaded = new HashIndex(storage, 'test', definition)
    await reloaded.load()

    // Should still be ready
    expect(reloaded.ready).toBe(true)
  })
})

// =============================================================================
// Concurrent Access Error Tests
// =============================================================================

describe('Concurrent Access Errors', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should handle multiple simultaneous loads', async () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    // Multiple load calls should not cause issues
    await Promise.all([
      index.load(),
      index.load(),
      index.load(),
    ])

    expect(index.ready).toBe(true)
  })

  it('should handle saves during load', async () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    // Pre-populate with data
    const index1 = new HashIndex(storage, 'test', definition)
    index1.insert('pending', 'doc1', 0, 0)
    await index1.save()

    // Now create two instances
    const index2 = new HashIndex(storage, 'test', definition)
    const index3 = new HashIndex(storage, 'test', definition)

    // Load and save concurrently
    await Promise.all([
      index2.load(),
      index3.load().then(() => {
        index3.insert('completed', 'doc2', 0, 1)
        return index3.save()
      }),
    ])

    // Both should be ready
    expect(index2.ready).toBe(true)
    expect(index3.ready).toBe(true)
  })
})

// =============================================================================
// Edge Case Tests
// =============================================================================

describe('Edge Cases', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('should handle null values in index', () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    index.insert(null, 'doc1', 0, 0)
    index.insert(null, 'doc2', 0, 1)

    const result = index.lookup(null)
    expect(result.docIds).toHaveLength(2)
  })

  it('should handle special characters in keys', () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    const specialKeys = [
      '',
      ' ',
      '\n',
      '\t',
      '\0',
      'unicode: \u{1F600}',
      '中文',
      '\x00\x01\x02',
    ]

    for (let i = 0; i < specialKeys.length; i++) {
      index.insert(specialKeys[i], `doc_${i}`, 0, i)
    }

    expect(index.size).toBe(specialKeys.length)

    for (let i = 0; i < specialKeys.length; i++) {
      const result = index.lookup(specialKeys[i])
      expect(result.docIds).toContain(`doc_${i}`)
    }
  })

  it('should handle negative numbers', () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'count' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    index.insert(-1, 'doc1', 0, 0)
    index.insert(-999999, 'doc2', 0, 1)
    index.insert(Number.MIN_SAFE_INTEGER, 'doc3', 0, 2)

    expect(index.lookup(-1).docIds).toContain('doc1')
    expect(index.lookup(-999999).docIds).toContain('doc2')
    expect(index.lookup(Number.MIN_SAFE_INTEGER).docIds).toContain('doc3')
  })

  it('should handle floating point numbers', () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'score' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    index.insert(3.14159, 'doc1', 0, 0)
    index.insert(0.1 + 0.2, 'doc2', 0, 1) // Known floating point issue
    index.insert(Number.MAX_VALUE, 'doc3', 0, 2)
    index.insert(Number.MIN_VALUE, 'doc4', 0, 3)

    expect(index.lookup(3.14159).docIds).toContain('doc1')
    expect(index.lookup(Number.MAX_VALUE).docIds).toContain('doc3')
    expect(index.lookup(Number.MIN_VALUE).docIds).toContain('doc4')
  })

  it('should handle boolean values', () => {
    const definition: IndexDefinition = {
      name: 'idx_test',
      type: 'hash',
      fields: [{ path: 'active' }],
    }

    const index = new HashIndex(storage, 'test', definition)

    index.insert(true, 'doc1', 0, 0)
    index.insert(false, 'doc2', 0, 1)

    expect(index.lookup(true).docIds).toContain('doc1')
    expect(index.lookup(false).docIds).toContain('doc2')
    expect(index.lookup(true).docIds).not.toContain('doc2')
  })
})

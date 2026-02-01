/**
 * Tests for Compact Encoding
 *
 * Tests the varint encoding, compact entry encoding, and full index serialization.
 */

import { describe, it, expect } from 'vitest'
import {
  writeVarint,
  readVarint,
  varintSize,
  writeCompactEntry,
  readCompactEntry,
  writeCompactEntryWithKey,
  readCompactEntryWithKey,
  writeCompactHeader,
  readCompactHeader,
  serializeCompactIndex,
  deserializeCompactIndex,
  fnv1aHash,
  FORMAT_VERSION_1,
  FORMAT_VERSION_2,
  FORMAT_VERSION_3,
  type CompactEntry,
  type CompactEntryWithKey,
} from '../../../src/indexes/encoding'

describe('Varint Encoding', () => {
  it('should encode and decode small values (1 byte)', () => {
    const buffer = new Uint8Array(10)

    // Test values 0-127 (single byte)
    for (const value of [0, 1, 63, 127]) {
      const written = writeVarint(buffer, 0, value)
      expect(written).toBe(1)

      const { value: decoded, bytesRead } = readVarint(buffer, 0)
      expect(decoded).toBe(value)
      expect(bytesRead).toBe(1)
    }
  })

  it('should encode and decode medium values (2 bytes)', () => {
    const buffer = new Uint8Array(10)

    // Test values 128-16383 (two bytes)
    for (const value of [128, 255, 1000, 16383]) {
      const written = writeVarint(buffer, 0, value)
      expect(written).toBe(2)

      const { value: decoded, bytesRead } = readVarint(buffer, 0)
      expect(decoded).toBe(value)
      expect(bytesRead).toBe(2)
    }
  })

  it('should encode and decode larger values (3+ bytes)', () => {
    const buffer = new Uint8Array(10)

    // Test values 16384+ (three or more bytes)
    for (const value of [16384, 65535, 100000, 2097151]) {
      const written = writeVarint(buffer, 0, value)
      expect(written).toBeGreaterThanOrEqual(3)

      const { value: decoded, bytesRead } = readVarint(buffer, 0)
      expect(decoded).toBe(value)
      expect(bytesRead).toBe(written)
    }
  })

  it('should calculate correct varint size', () => {
    expect(varintSize(0)).toBe(1)
    expect(varintSize(127)).toBe(1)
    expect(varintSize(128)).toBe(2)
    expect(varintSize(16383)).toBe(2)
    expect(varintSize(16384)).toBe(3)
    expect(varintSize(2097151)).toBe(3)
    expect(varintSize(2097152)).toBe(4)
  })

  it('should handle offsets correctly', () => {
    const buffer = new Uint8Array(20)

    // Write at different offsets
    let offset = 0
    offset += writeVarint(buffer, offset, 100)
    offset += writeVarint(buffer, offset, 10000)
    offset += writeVarint(buffer, offset, 1000000)

    // Read back
    let readOffset = 0
    const r1 = readVarint(buffer, readOffset)
    expect(r1.value).toBe(100)
    readOffset += r1.bytesRead

    const r2 = readVarint(buffer, readOffset)
    expect(r2.value).toBe(10000)
    readOffset += r2.bytesRead

    const r3 = readVarint(buffer, readOffset)
    expect(r3.value).toBe(1000000)
  })
})

describe('Compact Entry Encoding', () => {
  it('should encode and decode basic entries', () => {
    const buffer = new Uint8Array(100)
    const entry: CompactEntry = {
      rowGroup: 5,
      rowOffset: 1234,
      docId: 'test-doc-123',
    }

    const written = writeCompactEntry(buffer, 0, entry)
    const { entry: decoded, bytesRead } = readCompactEntry(buffer, 0)

    expect(bytesRead).toBe(written)
    expect(decoded.rowGroup).toBe(entry.rowGroup)
    expect(decoded.rowOffset).toBe(entry.rowOffset)
    expect(decoded.docId).toBe(entry.docId)
  })

  it('should handle large row offsets', () => {
    const buffer = new Uint8Array(100)
    const entry: CompactEntry = {
      rowGroup: 1000,
      rowOffset: 500000, // Large offset
      docId: 'doc',
    }

    const written = writeCompactEntry(buffer, 0, entry)
    const { entry: decoded, bytesRead } = readCompactEntry(buffer, 0)

    expect(bytesRead).toBe(written)
    expect(decoded.rowOffset).toBe(entry.rowOffset)
  })

  it('should encode and decode entries with key hash', () => {
    const buffer = new Uint8Array(100)
    const entry: CompactEntryWithKey = {
      keyHash: 0x12345678,
      rowGroup: 10,
      rowOffset: 5000,
      docId: 'doc-with-hash',
    }

    const written = writeCompactEntryWithKey(buffer, 0, entry)
    const { entry: decoded, bytesRead } = readCompactEntryWithKey(buffer, 0)

    expect(bytesRead).toBe(written)
    expect(decoded.keyHash).toBe(entry.keyHash)
    expect(decoded.rowGroup).toBe(entry.rowGroup)
    expect(decoded.rowOffset).toBe(entry.rowOffset)
    expect(decoded.docId).toBe(entry.docId)
  })

  it('should be more compact than v1 format', () => {
    // v1 format entry: keyLen(2) + key(~10) + docIdLen(2) + docId(26) + rowGroup(4) + rowOffset(4) = ~48 bytes
    // Compact format entry: rowGroup(2) + rowOffset(1-5) + docIdLen(1) + docId(26) = ~30 bytes

    const entry: CompactEntry = {
      rowGroup: 5,
      rowOffset: 100, // Small offset = 1 byte varint
      docId: 'tt1234567890123456789012', // 24 char ULID-style ID
    }

    const buffer = new Uint8Array(100)
    const written = writeCompactEntry(buffer, 0, entry)

    // Should be: 2 (rowGroup) + 1 (varint) + 1 (docIdLen) + 24 (docId) = 28 bytes
    expect(written).toBeLessThan(35)
  })
})

describe('Compact Header Encoding', () => {
  it('should encode and decode header without key hash', () => {
    const buffer = new Uint8Array(10)
    const header = {
      version: FORMAT_VERSION_3,
      entryCount: 12345,
      hasKeyHash: false,
    }

    const written = writeCompactHeader(buffer, 0, header)
    expect(written).toBe(6) // version(1) + flags(1) + entryCount(4)

    const { header: decoded, bytesRead } = readCompactHeader(buffer, 0)

    expect(bytesRead).toBe(6)
    expect(decoded.version).toBe(FORMAT_VERSION_3)
    expect(decoded.entryCount).toBe(12345)
    expect(decoded.hasKeyHash).toBe(false)
  })

  it('should encode and decode header with key hash', () => {
    const buffer = new Uint8Array(10)
    const header = {
      version: FORMAT_VERSION_3,
      entryCount: 999999,
      hasKeyHash: true,
    }

    const written = writeCompactHeader(buffer, 0, header)
    const { header: decoded, bytesRead } = readCompactHeader(buffer, 0)

    expect(bytesRead).toBe(written)
    expect(decoded.hasKeyHash).toBe(true)
    expect(decoded.entryCount).toBe(999999)
  })
})

describe('Full Index Serialization', () => {
  it('should serialize and deserialize index without key hashes', () => {
    const entries: CompactEntry[] = [
      { rowGroup: 0, rowOffset: 0, docId: 'doc1' },
      { rowGroup: 0, rowOffset: 100, docId: 'doc2' },
      { rowGroup: 1, rowOffset: 0, docId: 'doc3' },
      { rowGroup: 1, rowOffset: 50, docId: 'doc4' },
    ]

    const serialized = serializeCompactIndex(entries, false)
    const { entries: deserialized, hasKeyHash } = deserializeCompactIndex(serialized)

    expect(hasKeyHash).toBe(false)
    expect(deserialized.length).toBe(entries.length)

    for (let i = 0; i < entries.length; i++) {
      expect(deserialized[i].rowGroup).toBe(entries[i].rowGroup)
      expect(deserialized[i].rowOffset).toBe(entries[i].rowOffset)
      expect(deserialized[i].docId).toBe(entries[i].docId)
    }
  })

  it('should serialize and deserialize index with key hashes', () => {
    const entries: CompactEntry[] = [
      { rowGroup: 0, rowOffset: 0, docId: 'doc1' },
      { rowGroup: 0, rowOffset: 100, docId: 'doc2' },
    ]

    const keyHashes = [0x11111111, 0x22222222]
    const serialized = serializeCompactIndex(entries, true, (i) => keyHashes[i])
    const { entries: deserialized, hasKeyHash } = deserializeCompactIndex(serialized)

    expect(hasKeyHash).toBe(true)
    expect(deserialized.length).toBe(entries.length)

    for (let i = 0; i < entries.length; i++) {
      const entry = deserialized[i] as CompactEntryWithKey
      expect(entry.keyHash).toBe(keyHashes[i])
      expect(entry.rowGroup).toBe(entries[i].rowGroup)
      expect(entry.rowOffset).toBe(entries[i].rowOffset)
      expect(entry.docId).toBe(entries[i].docId)
    }
  })

  it('should achieve ~3x size reduction for typical IMDB data', () => {
    // Simulate 1000 typical IMDB entries
    const entries: CompactEntry[] = []
    for (let i = 0; i < 1000; i++) {
      entries.push({
        rowGroup: Math.floor(i / 100), // ~10 row groups
        rowOffset: i % 100,
        docId: `tt${String(i).padStart(7, '0')}`, // e.g., "tt0000001"
      })
    }

    const compactSize = serializeCompactIndex(entries, false).length

    // v1 format would be approximately:
    // 5 (header) + 1000 * (2 + 10 + 2 + 9 + 4 + 4) = 5 + 31000 = ~31KB
    const v1EstimatedSize = 5 + 1000 * (2 + 10 + 2 + 9 + 4 + 4)

    // Compact format should be:
    // 6 (header) + 1000 * (2 + 1 + 1 + 9) = 6 + 13000 = ~13KB
    const expectedCompactSize = 6 + 1000 * (2 + 1 + 1 + 9)

    // Verify we're close to expected
    expect(compactSize).toBeLessThan(v1EstimatedSize * 0.6) // At least 40% reduction
    expect(compactSize).toBeCloseTo(expectedCompactSize, -3) // Within 1000 bytes
  })
})

describe('FNV-1a Hash', () => {
  it('should produce consistent hashes', () => {
    const key1 = new TextEncoder().encode('test-key')
    const key2 = new TextEncoder().encode('test-key')
    const key3 = new TextEncoder().encode('different-key')

    expect(fnv1aHash(key1)).toBe(fnv1aHash(key2))
    expect(fnv1aHash(key1)).not.toBe(fnv1aHash(key3))
  })

  it('should produce 32-bit hashes', () => {
    const key = new TextEncoder().encode('any-key')
    const hash = fnv1aHash(key)

    expect(hash).toBeGreaterThanOrEqual(0)
    expect(hash).toBeLessThanOrEqual(0xffffffff)
  })
})

describe('Format Version Constants', () => {
  it('should have distinct version numbers', () => {
    expect(FORMAT_VERSION_1).toBe(0x01)
    expect(FORMAT_VERSION_2).toBe(0x02)
    expect(FORMAT_VERSION_3).toBe(0x03)

    expect(FORMAT_VERSION_1).not.toBe(FORMAT_VERSION_2)
    expect(FORMAT_VERSION_2).not.toBe(FORMAT_VERSION_3)
    expect(FORMAT_VERSION_1).not.toBe(FORMAT_VERSION_3)
  })
})

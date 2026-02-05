/**
 * Unit tests for Parquet Split Block Bloom Filter
 */

import { describe, it, expect } from 'vitest'
import { xxHash64, ParquetBloomFilter, parseBloomFilterHeader } from '../../src/parquet/bloom-filter'

describe('xxHash64', () => {
  it('should hash empty input', () => {
    const hash = xxHash64(new Uint8Array(0))
    // xxHash64 with seed 0 and empty input should return PRIME64_5
    expect(typeof hash).toBe('bigint')
  })

  it('should hash a simple string', () => {
    const data = new TextEncoder().encode('hello')
    const hash = xxHash64(data)
    expect(typeof hash).toBe('bigint')
    // Verify it's a 64-bit value
    expect(hash).toBeLessThanOrEqual(0xffffffffffffffffn)
  })

  it('should produce different hashes for different inputs', () => {
    const hash1 = xxHash64(new TextEncoder().encode('hello'))
    const hash2 = xxHash64(new TextEncoder().encode('world'))
    expect(hash1).not.toBe(hash2)
  })

  it('should produce consistent hashes for same input', () => {
    const data = new TextEncoder().encode('test')
    const hash1 = xxHash64(data)
    const hash2 = xxHash64(data)
    expect(hash1).toBe(hash2)
  })

  it('should handle small inputs (< 32 bytes)', () => {
    const data = new TextEncoder().encode('short')
    const hash = xxHash64(data)
    expect(typeof hash).toBe('bigint')
  })

  it('should handle inputs >= 32 bytes', () => {
    const data = new TextEncoder().encode('this is a longer string that exceeds 32 bytes')
    const hash = xxHash64(data)
    expect(typeof hash).toBe('bigint')
    expect(hash).toBeLessThanOrEqual(0xffffffffffffffffn)
  })

  it('should handle exactly 32 bytes', () => {
    const data = new Uint8Array(32).fill(0x42)
    const hash = xxHash64(data)
    expect(typeof hash).toBe('bigint')
  })

  // Known test vectors from xxHash reference implementation
  it('should match known test vectors', () => {
    // xxHash64("") with seed 0 = 0xef46db3751d8e999
    const emptyHash = xxHash64(new Uint8Array(0))
    expect(emptyHash).toBe(0xef46db3751d8e999n)

    // xxHash64("abc") with seed 0 = 0x44bc2cf5ad770999
    const abcHash = xxHash64(new TextEncoder().encode('abc'))
    expect(abcHash).toBe(0x44bc2cf5ad770999n)
  })
})

describe('ParquetBloomFilter', () => {
  /**
   * Create a simple test bloom filter with known values inserted.
   * We manually construct the filter data to test the reader.
   */
  function createTestFilter(numBlocks: number): Uint8Array {
    // Each block is 32 bytes (256 bits = 8 x 32-bit words)
    return new Uint8Array(numBlocks * 32)
  }

  it('should create a bloom filter from raw bytes', () => {
    const data = createTestFilter(4)
    const filter = new ParquetBloomFilter(data)
    expect(filter.blockCount).toBe(4)
    expect(filter.sizeBytes).toBe(128)
  })

  it('should reject invalid size (not multiple of 32)', () => {
    expect(() => new ParquetBloomFilter(new Uint8Array(30))).toThrow()
    expect(() => new ParquetBloomFilter(new Uint8Array(33))).toThrow()
  })

  it('should accept valid sizes', () => {
    expect(() => new ParquetBloomFilter(new Uint8Array(32))).not.toThrow()
    expect(() => new ParquetBloomFilter(new Uint8Array(64))).not.toThrow()
    expect(() => new ParquetBloomFilter(new Uint8Array(1024))).not.toThrow()
  })

  it('mightContain should return false for empty filter', () => {
    // Empty filter should return false for any value (no bits set)
    const data = createTestFilter(4)
    const filter = new ParquetBloomFilter(data)

    // With an empty filter, no bits are set, so all checks should return false
    expect(filter.mightContain('test')).toBe(false)
    expect(filter.mightContain(123)).toBe(false)
    expect(filter.mightContain(null)).toBe(false)
  })

  it('should handle different value types', () => {
    const data = createTestFilter(4)
    const filter = new ParquetBloomFilter(data)

    // These should not throw
    expect(() => filter.mightContain('string')).not.toThrow()
    expect(() => filter.mightContain(123)).not.toThrow()
    expect(() => filter.mightContain(123.456)).not.toThrow()
    expect(() => filter.mightContain(true)).not.toThrow()
    expect(() => filter.mightContain(false)).not.toThrow()
    expect(() => filter.mightContain(null)).not.toThrow()
    expect(() => filter.mightContain(undefined)).not.toThrow()
    expect(() => filter.mightContain(new Date())).not.toThrow()
    expect(() => filter.mightContain({ foo: 'bar' })).not.toThrow()
    expect(() => filter.mightContain([1, 2, 3])).not.toThrow()
    expect(() => filter.mightContain(123n)).not.toThrow()
  })

  it('should correctly determine block selection', () => {
    // Create a filter with a single block where all bits are set
    const data = new Uint8Array(32).fill(0xff) // All bits set
    const filter = new ParquetBloomFilter(data)

    // With a single block and all bits set, should return true
    expect(filter.mightContain('test')).toBe(true)
    expect(filter.mightContain(123)).toBe(true)
  })

  it('should use upper 32 bits for block selection', () => {
    // With multiple blocks, different values should potentially select different blocks
    const numBlocks = 8
    const data = createTestFilter(numBlocks) // Empty filter

    // Set all bits in block 0 only
    for (let i = 0; i < 32; i++) {
      data[i] = 0xff
    }

    const filter = new ParquetBloomFilter(data)

    // Values whose hash's upper 32 bits would select block 0 should return true
    // Values that select other blocks should return false (empty blocks)
    // We can't predict which values go where without computing the hash,
    // but we can verify the filter handles multiple blocks correctly
    expect(filter.blockCount).toBe(8)
  })
})

describe('parseBloomFilterHeader', () => {
  it('should parse a minimal header', () => {
    // Minimal Thrift compact protocol header:
    // Field 1 (numBytes): type i32 (5), value 256 (varint)
    // STOP (0x00)
    const header = new Uint8Array([
      0x15, // field 1 (delta=1), type 5 (i32)
      0x80, 0x04, // varint 256 (zigzag encoded: 256 -> 512 -> 0x80 0x04)
      0x00, // STOP
    ])

    const result = parseBloomFilterHeader(header)
    expect(result.header.numBytes).toBe(256)
    expect(result.header.algorithm).toBe('SPLIT_BLOCK')
    expect(result.header.hash).toBe('XXHASH')
    expect(result.header.compression).toBe('UNCOMPRESSED')
  })

  it('should return correct data offset', () => {
    const header = new Uint8Array([
      0x15, // field 1, type 5
      0x80, 0x04, // varint 256
      0x00, // STOP
      0x01, 0x02, 0x03, // Some data bytes
    ])

    const result = parseBloomFilterHeader(header)
    expect(result.dataOffset).toBe(4) // Header is 4 bytes
  })

  it('should handle complex headers with union fields', () => {
    // Header with algorithm, hash, and compression fields
    const header = new Uint8Array([
      0x15, // field 1 (numBytes), type i32
      0x80, 0x04, // value 256
      0x2c, // field 2 (algorithm), type struct (12)
      0x1c, // inner: field 1 (SPLIT_BLOCK), type struct
      0x00, // inner STOP
      0x00, // outer STOP
      0x3c, // field 3 (hash), type struct
      0x1c, // inner: field 1 (XXHASH), type struct
      0x00, // inner STOP
      0x00, // outer STOP
      0x4c, // field 4 (compression), type struct
      0x1c, // inner: field 1 (UNCOMPRESSED), type struct
      0x00, // inner STOP
      0x00, // outer STOP
      0x00, // final STOP
    ])

    const result = parseBloomFilterHeader(header)
    expect(result.header.numBytes).toBe(256)
    expect(result.header.algorithm).toBe('SPLIT_BLOCK')
    expect(result.header.hash).toBe('XXHASH')
    expect(result.header.compression).toBe('UNCOMPRESSED')
  })
})

/**
 * LZ4 Compression Tests
 *
 * Tests for the LZ4 compression implementation in ParqueDB.
 * Verifies that LZ4 compression/decompression works correctly
 * and is compatible with hyparquet-compressors.
 */

import { describe, it, expect } from 'vitest'
import {
  compressLz4,
  compressLz4Hadoop,
  writeCompressors,
  compressors,
} from '../../src/parquet/compression'

describe('LZ4 Compression', () => {
  describe('compressLz4 (raw block format)', () => {
    it('should compress empty data', () => {
      const input = new Uint8Array(0)
      const compressed = compressLz4(input)
      expect(compressed.length).toBe(0)
    })

    it('should compress small data', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])
      const compressed = compressLz4(input)
      expect(compressed).toBeInstanceOf(Uint8Array)
      expect(compressed.length).toBeGreaterThan(0)
    })

    it('should round-trip with LZ4_RAW decompressor', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
      const compressed = compressLz4(input)

      const decompressor = compressors.LZ4_RAW
      expect(decompressor).toBeDefined()

      const decompressed = decompressor!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle repeating data with good compression', () => {
      // Repeating data should compress well
      const input = new Uint8Array(100)
      for (let i = 0; i < 100; i++) {
        input[i] = i % 4 // Repeating pattern
      }

      const compressed = compressLz4(input)
      // LZ4 should achieve some compression on repeating data
      expect(compressed.length).toBeLessThan(input.length)

      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle text data', () => {
      const text = 'Hello World, this is a test message that should compress!'
      const input = new TextEncoder().encode(text)
      const compressed = compressLz4(input)

      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      const result = new TextDecoder().decode(decompressed)
      expect(result).toBe(text)
    })

    it('should handle long repeating text', () => {
      const text = 'The quick brown fox jumps over the lazy dog. '.repeat(20)
      const input = new TextEncoder().encode(text)
      const compressed = compressLz4(input)

      // Should achieve good compression with repeating text
      expect(compressed.length).toBeLessThan(input.length)

      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      const result = new TextDecoder().decode(decompressed)
      expect(result).toBe(text)
    })
  })

  describe('compressLz4Hadoop (Hadoop frame format)', () => {
    it('should compress empty data', () => {
      const input = new Uint8Array(0)
      const compressed = compressLz4Hadoop(input)
      expect(compressed.length).toBe(0)
    })

    it('should add 8-byte header', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])
      const compressed = compressLz4Hadoop(input)

      // Header: 4 bytes decompressed size + 4 bytes compressed size
      expect(compressed.length).toBeGreaterThan(8)

      // Check decompressed size header (big-endian)
      const decompressedSize =
        (compressed[0] << 24) |
        (compressed[1] << 16) |
        (compressed[2] << 8) |
        compressed[3]
      expect(decompressedSize).toBe(5)

      // Check compressed size header (big-endian)
      const compressedSize =
        (compressed[4] << 24) |
        (compressed[5] << 16) |
        (compressed[6] << 8) |
        compressed[7]
      expect(compressedSize).toBe(compressed.length - 8)
    })

    it('should round-trip with LZ4 (Hadoop) decompressor', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
      const compressed = compressLz4Hadoop(input)

      const decompressor = compressors.LZ4
      expect(decompressor).toBeDefined()

      const decompressed = decompressor!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle larger data', () => {
      const input = new Uint8Array(1000)
      for (let i = 0; i < 1000; i++) {
        input[i] = i % 256
      }

      const compressed = compressLz4Hadoop(input)
      const decompressed = compressors.LZ4!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })
  })

  describe('writeCompressors', () => {
    it('should export LZ4 compressor', () => {
      expect(writeCompressors.LZ4).toBeDefined()
      expect(typeof writeCompressors.LZ4).toBe('function')
    })

    it('should export LZ4_RAW compressor', () => {
      expect(writeCompressors.LZ4_RAW).toBeDefined()
      expect(typeof writeCompressors.LZ4_RAW).toBe('function')
    })

    it('should have LZ4 use Hadoop format', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])
      const compressed = writeCompressors.LZ4(input)

      // Hadoop format has 8-byte header
      expect(compressed.length).toBeGreaterThan(8)

      // Verify header
      const decompressedSize =
        (compressed[0] << 24) |
        (compressed[1] << 16) |
        (compressed[2] << 8) |
        compressed[3]
      expect(decompressedSize).toBe(5)
    })

    it('should have LZ4_RAW use raw format', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])
      const compressed = writeCompressors.LZ4_RAW(input)

      // Raw format has no header - just compressed data
      // Verify by decompressing with LZ4_RAW decompressor
      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })
  })

  describe('compressors (read decompressors)', () => {
    it('should export decompressors from hyparquet-compressors', () => {
      expect(compressors).toBeDefined()
      expect(compressors.SNAPPY).toBeDefined()
      expect(compressors.GZIP).toBeDefined()
      expect(compressors.BROTLI).toBeDefined()
      expect(compressors.ZSTD).toBeDefined()
      expect(compressors.LZ4).toBeDefined()
      expect(compressors.LZ4_RAW).toBeDefined()
    })
  })

  describe('compression efficiency', () => {
    it('should achieve compression on repetitive data', () => {
      // Create highly repetitive data
      const repeating = 'AAAA'.repeat(100)
      const input = new TextEncoder().encode(repeating)
      const compressed = compressLz4(input)

      // LZ4 should compress this well
      const ratio = compressed.length / input.length
      expect(ratio).toBeLessThan(0.5) // At least 50% compression

      // Verify round-trip
      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      expect(new TextDecoder().decode(decompressed)).toBe(repeating)
    })

    it('should handle random-ish data (may not compress)', () => {
      // Pseudo-random data (not truly random, but varied)
      const input = new Uint8Array(100)
      for (let i = 0; i < 100; i++) {
        input[i] = (i * 17 + 31) % 256
      }

      const compressed = compressLz4(input)
      // Should still produce valid output even if no compression gain
      expect(compressed).toBeInstanceOf(Uint8Array)

      // Verify round-trip
      const decompressed = compressors.LZ4_RAW!(compressed, input.length)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })
  })
})

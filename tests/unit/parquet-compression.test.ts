/**
 * Parquet Compression Default Tests
 *
 * Tests for verifying that ParqueDB uses UNCOMPRESSED by default
 * for Cloudflare Workers where storage is cheap and CPU is expensive.
 *
 * Context: Decompression overhead hurts latency on Cloudflare Workers.
 * Storage is cheap, so we prefer UNCOMPRESSED by default.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParquetWriter, writeParquet } from '@/parquet/writer'
import { ParquetReader } from '@/parquet/reader'
import { MemoryBackend } from '@/storage/MemoryBackend'
import { DEFAULT_COMPRESSION } from '@/constants'
import type { ParquetSchema } from '@/parquet/types'

// =============================================================================
// Test Fixtures
// =============================================================================

const TEST_SCHEMA: ParquetSchema = {
  id: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: true },
  value: { type: 'INT64', optional: true },
}

const TEST_DATA = [
  { id: '1', name: 'Alice', value: 100 },
  { id: '2', name: 'Bob', value: 200 },
  { id: '3', name: 'Charlie', value: 300 },
]

// =============================================================================
// Default Compression Tests
// =============================================================================

describe('Parquet Compression Defaults', () => {
  let storage: MemoryBackend
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    reader = new ParquetReader({ storage })
  })

  describe('DEFAULT_COMPRESSION constant', () => {
    it('should be set to none (UNCOMPRESSED)', () => {
      // Cloudflare Workers: storage is cheap, CPU is expensive
      // Decompression overhead hurts latency
      expect(DEFAULT_COMPRESSION).toBe('none')
    })
  })

  describe('ParquetWriter default behavior', () => {
    it('should use UNCOMPRESSED by default', async () => {
      // Create writer with no compression option specified
      const writer = new ParquetWriter(storage)

      // Write test data
      await writer.write('data/test.parquet', TEST_DATA, TEST_SCHEMA)

      // Read the file metadata to verify compression
      const metadata = await reader.readMetadata('data/test.parquet')

      // Check that all column chunks use UNCOMPRESSED
      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('UNCOMPRESSED')
        }
      }
    })

    it('should allow explicit compression override to snappy', async () => {
      const writer = new ParquetWriter(storage, { compression: 'snappy' })

      await writer.write('data/snappy.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/snappy.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('SNAPPY')
        }
      }
    })

    it('should allow explicit compression override to lz4', async () => {
      const writer = new ParquetWriter(storage, { compression: 'lz4' })

      await writer.write('data/lz4.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/lz4.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('LZ4')
        }
      }
    })

    it('should allow explicit compression override to gzip', async () => {
      const writer = new ParquetWriter(storage, { compression: 'gzip' })

      await writer.write('data/gzip.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/gzip.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('GZIP')
        }
      }
    })

    it('should allow explicit compression override to zstd', async () => {
      const writer = new ParquetWriter(storage, { compression: 'zstd' })

      await writer.write('data/zstd.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/zstd.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('ZSTD')
        }
      }
    })

    it('should allow explicit none compression option', async () => {
      const writer = new ParquetWriter(storage, { compression: 'none' })

      await writer.write('data/uncompressed.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/uncompressed.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('UNCOMPRESSED')
        }
      }
    })
  })

  describe('writeParquet convenience function', () => {
    it('should use UNCOMPRESSED by default', async () => {
      // Call without compression option
      await writeParquet(storage, 'data/default.parquet', TEST_DATA, TEST_SCHEMA)

      const metadata = await reader.readMetadata('data/default.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('UNCOMPRESSED')
        }
      }
    })

    it('should respect explicit compression option', async () => {
      await writeParquet(storage, 'data/lz4.parquet', TEST_DATA, TEST_SCHEMA, {
        compression: 'lz4',
      })

      const metadata = await reader.readMetadata('data/lz4.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('LZ4')
        }
      }
    })
  })

  describe('per-write compression override', () => {
    it('should allow overriding default at write time', async () => {
      // Writer with default (none)
      const writer = new ParquetWriter(storage)

      // Override to lz4 for this write
      await writer.write('data/override.parquet', TEST_DATA, TEST_SCHEMA, {
        compression: 'lz4',
      })

      const metadata = await reader.readMetadata('data/override.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('LZ4')
        }
      }
    })

    it('should allow overriding explicit compression at write time', async () => {
      // Writer with lz4
      const writer = new ParquetWriter(storage, { compression: 'lz4' })

      // Override to none for this write
      await writer.write('data/override-none.parquet', TEST_DATA, TEST_SCHEMA, {
        compression: 'none',
      })

      const metadata = await reader.readMetadata('data/override-none.parquet')

      for (const rowGroup of metadata.rowGroups) {
        for (const column of rowGroup.columns) {
          expect(column.codec).toBe('UNCOMPRESSED')
        }
      }
    })
  })
})

// =============================================================================
// Read Performance Comparison (Informational)
// =============================================================================

describe('Compression Read Performance Comparison', () => {
  let storage: MemoryBackend
  let reader: ParquetReader

  beforeEach(() => {
    storage = new MemoryBackend()
    reader = new ParquetReader({ storage })
  })

  it('should demonstrate that uncompressed files can be read', async () => {
    const writer = new ParquetWriter(storage, { compression: 'none' })
    await writer.write('data/uncompressed.parquet', TEST_DATA, TEST_SCHEMA)

    const data = await reader.read<typeof TEST_DATA[0]>('data/uncompressed.parquet')

    expect(data).toHaveLength(TEST_DATA.length)
    expect(data[0].id).toBe('1')
    expect(data[0].name).toBe('Alice')
    expect(data[0].value).toBe(100)
  })

  it('should demonstrate that lz4 compressed files can be read', async () => {
    const writer = new ParquetWriter(storage, { compression: 'lz4' })
    await writer.write('data/lz4.parquet', TEST_DATA, TEST_SCHEMA)

    const data = await reader.read<typeof TEST_DATA[0]>('data/lz4.parquet')

    expect(data).toHaveLength(TEST_DATA.length)
    expect(data[0].id).toBe('1')
    expect(data[0].name).toBe('Alice')
    expect(data[0].value).toBe(100)
  })

  it('should show uncompressed files are larger but faster to read', async () => {
    // Generate larger dataset for meaningful comparison
    const largeData = Array.from({ length: 1000 }, (_, i) => ({
      id: `id-${i}`,
      name: `User ${i} with a longer name for better compression ratio`,
      value: i * 100,
    }))

    // Write uncompressed
    const uncompressedWriter = new ParquetWriter(storage, { compression: 'none' })
    await uncompressedWriter.write('data/large-uncompressed.parquet', largeData, TEST_SCHEMA)
    const uncompressedData = await storage.read('data/large-uncompressed.parquet')

    // Write with lz4
    const lz4Writer = new ParquetWriter(storage, { compression: 'lz4' })
    await lz4Writer.write('data/large-lz4.parquet', largeData, TEST_SCHEMA)
    const lz4Data = await storage.read('data/large-lz4.parquet')

    // Uncompressed should be larger
    expect(uncompressedData.length).toBeGreaterThan(lz4Data.length)

    // Both should be readable
    const uncompressedRead = await reader.read('data/large-uncompressed.parquet')
    const lz4Read = await reader.read('data/large-lz4.parquet')

    expect(uncompressedRead).toHaveLength(1000)
    expect(lz4Read).toHaveLength(1000)
  })
})

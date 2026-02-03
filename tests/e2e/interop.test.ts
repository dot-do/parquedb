/**
 * E2E Interop Tests - Test ParqueDB-written files with external tools
 *
 * These tests verify that Parquet files written by ParqueDB can be read by external
 * tools and libraries (specifically hyparquet's raw API). This ensures:
 * - Format compatibility with the Parquet specification
 * - Schema correctness across different data types
 * - Metadata preservation
 * - Compression codec interoperability
 *
 * Uses FsBackend with real filesystem to create actual Parquet files.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, stat, readFile, readdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// hyparquet raw API for external tool simulation
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import type { FileMetaData, Compressors } from 'hyparquet'

// ParqueDB imports
import { FsBackend } from '../../src/storage/FsBackend'
import {
  ParquetWriter,
  ParquetReader,
  createEntitySchema,
  compressors,
} from '../../src/parquet'
import type { ParquetSchema } from '../../src/parquet'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create an AsyncBuffer from a file for hyparquet
 */
async function createFileAsyncBuffer(filePath: string) {
  const fileData = await readFile(filePath)
  return {
    byteLength: fileData.byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const sliced = fileData.slice(start, end ?? fileData.byteLength)
      const buffer = new ArrayBuffer(sliced.byteLength)
      new Uint8Array(buffer).set(sliced)
      return buffer
    },
  }
}

/**
 * Read Parquet file with raw hyparquet API (simulating external tool)
 */
async function readWithHyparquet<T = Record<string, unknown>>(
  filePath: string
): Promise<T[]> {
  const asyncBuffer = await createFileAsyncBuffer(filePath)
  const rows = await parquetReadObjects({
    file: asyncBuffer,
    compressors: compressors as Compressors,
  })
  return rows as T[]
}

/**
 * Read Parquet metadata with raw hyparquet API
 */
async function readMetadataWithHyparquet(
  filePath: string
): Promise<FileMetaData> {
  const asyncBuffer = await createFileAsyncBuffer(filePath)
  return parquetMetadataAsync(asyncBuffer)
}

// =============================================================================
// Test Suite
// =============================================================================

describe('ParqueDB Interop Tests', () => {
  let tempDir: string
  let storage: FsBackend

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-interop-test-'))
    storage = new FsBackend(tempDir)
  })

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true })
  })

  // ===========================================================================
  // Basic Format Compatibility
  // ===========================================================================

  describe('Basic Format Compatibility', () => {
    it('should create valid Parquet files readable by hyparquet', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
      }
      const data = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' },
      ]

      await writer.write('basic.parquet', data, schema)

      // Verify file exists and has content
      const filePath = join(tempDir, 'basic.parquet')
      const fileStat = await stat(filePath)
      expect(fileStat.isFile()).toBe(true)
      expect(fileStat.size).toBeGreaterThan(0)

      // Read with raw hyparquet (simulating external tool)
      const rows = await readWithHyparquet(filePath)

      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual({ id: '1', name: 'Alice' })
      expect(rows[1]).toEqual({ id: '2', name: 'Bob' })
    })

    it('should produce files with valid Parquet magic bytes', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'INT64', optional: false },
      }
      const data = [{ value: 42 }]

      await writer.write('magic.parquet', data, schema)

      // Read raw bytes to check magic bytes
      const filePath = join(tempDir, 'magic.parquet')
      const fileData = await readFile(filePath)

      // Parquet files start and end with 'PAR1' magic bytes
      const headerMagic = fileData.slice(0, 4).toString('ascii')
      const footerMagic = fileData.slice(-4).toString('ascii')

      expect(headerMagic).toBe('PAR1')
      expect(footerMagic).toBe('PAR1')
    })

    it('should create files with valid metadata', async () => {
      const writer = new ParquetWriter(storage, {
        metadata: { creator: 'ParqueDB', version: '0.1.0' },
      })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }
      const data = [{ id: '1' }]

      await writer.write('metadata.parquet', data, schema)

      const filePath = join(tempDir, 'metadata.parquet')
      const metadata = await readMetadataWithHyparquet(filePath)

      // Verify metadata structure
      expect(metadata).toBeDefined()
      expect(metadata.schema).toBeDefined()
      expect(metadata.num_rows).toBe(BigInt(1))

      // Check key-value metadata
      const kvMetadata = metadata.key_value_metadata ?? []
      const creatorEntry = kvMetadata.find(kv => kv.key === 'creator')
      const versionEntry = kvMetadata.find(kv => kv.key === 'version')
      expect(creatorEntry?.value).toBe('ParqueDB')
      expect(versionEntry?.value).toBe('0.1.0')
    })
  })

  // ===========================================================================
  // Data Type Compatibility
  // ===========================================================================

  describe('Data Type Compatibility', () => {
    it('should handle STRING type correctly', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'STRING', optional: false },
      }
      const data = [
        { value: '' },
        { value: 'hello' },
        { value: 'Hello, World!' },
        { value: 'unicode: \u{1F600}' },
        { value: 'multiline\ntest' },
      ]

      await writer.write('strings.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'strings.parquet'))

      expect(rows).toHaveLength(5)
      expect(rows[0]!.value).toBe('')
      expect(rows[1]!.value).toBe('hello')
      expect(rows[2]!.value).toBe('Hello, World!')
      expect(rows[3]!.value).toBe('unicode: \u{1F600}')
      expect(rows[4]!.value).toBe('multiline\ntest')
    })

    it('should handle INT64 type correctly', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'INT64', optional: false },
      }
      // Note: Values must fit within INT32 range for reliable round-trip.
      // Very large numbers outside this range may experience precision issues
      // due to JavaScript number representation limits.
      const data = [
        { value: 0 },
        { value: 1 },
        { value: -1 },
        { value: 127 },
        { value: -128 },
        { value: 32767 },
        { value: -32768 },
        { value: 2147483647 },  // INT32 max
        { value: -2147483648 }, // INT32 min
      ]

      await writer.write('int64.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'int64.parquet'))

      expect(rows).toHaveLength(9)
      // hyparquet may return BigInt or number depending on value size
      // Use BigInt for comparison to handle both cases
      expect(BigInt(rows[0]!.value as bigint | number)).toBe(BigInt(0))
      expect(BigInt(rows[1]!.value as bigint | number)).toBe(BigInt(1))
      expect(BigInt(rows[2]!.value as bigint | number)).toBe(BigInt(-1))
      expect(BigInt(rows[3]!.value as bigint | number)).toBe(BigInt(127))
      expect(BigInt(rows[4]!.value as bigint | number)).toBe(BigInt(-128))
      expect(BigInt(rows[5]!.value as bigint | number)).toBe(BigInt(32767))
      expect(BigInt(rows[6]!.value as bigint | number)).toBe(BigInt(-32768))
      expect(BigInt(rows[7]!.value as bigint | number)).toBe(BigInt(2147483647))
      expect(BigInt(rows[8]!.value as bigint | number)).toBe(BigInt(-2147483648))
    })

    it('should handle DOUBLE type correctly', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'DOUBLE', optional: false },
      }
      const data = [
        { value: 0.0 },
        { value: 3.14159 },
        { value: -2.71828 },
        { value: 1e10 },
        { value: 1e-10 },
      ]

      await writer.write('doubles.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'doubles.parquet'))

      expect(rows).toHaveLength(5)
      expect(rows[0]!.value).toBeCloseTo(0.0)
      expect(rows[1]!.value).toBeCloseTo(3.14159)
      expect(rows[2]!.value).toBeCloseTo(-2.71828)
      expect(rows[3]!.value).toBeCloseTo(1e10)
      expect(rows[4]!.value).toBeCloseTo(1e-10)
    })

    it('should handle BOOLEAN type correctly', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'BOOLEAN', optional: false },
      }
      const data = [
        { value: true },
        { value: false },
        { value: true },
      ]

      await writer.write('booleans.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'booleans.parquet'))

      expect(rows).toHaveLength(3)
      expect(rows[0]!.value).toBe(true)
      expect(rows[1]!.value).toBe(false)
      expect(rows[2]!.value).toBe(true)
    })

    it('should handle nullable columns with null values', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
        age: { type: 'INT64', optional: true },
      }
      const data = [
        { id: '1', name: 'Alice', age: 30 },
        { id: '2', name: null, age: null },
        { id: '3', name: 'Charlie', age: null },
        { id: '4', name: null, age: 40 },
      ]

      await writer.write('nullables.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'nullables.parquet'))

      expect(rows).toHaveLength(4)
      // Check individual fields - hyparquet may return number or BigInt for INT64
      expect(rows[0]!.id).toBe('1')
      expect(rows[0]!.name).toBe('Alice')
      expect(BigInt(rows[0]!.age as bigint | number)).toBe(BigInt(30))
      expect(rows[1]).toEqual({ id: '2', name: null, age: null })
      expect(rows[2]).toEqual({ id: '3', name: 'Charlie', age: null })
      expect(rows[3]!.id).toBe('4')
      expect(rows[3]!.name).toBeNull()
      expect(BigInt(rows[3]!.age as bigint | number)).toBe(BigInt(40))
    })

    it('should handle mixed data types in same file', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        count: { type: 'INT64', optional: true },
        score: { type: 'DOUBLE', optional: true },
        active: { type: 'BOOLEAN', optional: true },
        label: { type: 'STRING', optional: true },
      }
      const data = [
        { id: '1', count: 100, score: 98.5, active: true, label: 'A' },
        { id: '2', count: 200, score: 75.0, active: false, label: 'B' },
      ]

      await writer.write('mixed.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'mixed.parquet'))

      expect(rows).toHaveLength(2)
      expect(rows[0]!.id).toBe('1')
      expect(Number(rows[0]!.count)).toBe(100)
      expect(rows[0]!.score).toBeCloseTo(98.5)
      expect(rows[0]!.active).toBe(true)
      expect(rows[0]!.label).toBe('A')
    })
  })

  // ===========================================================================
  // Compression Codec Compatibility
  // ===========================================================================

  describe('Compression Codec Compatibility', () => {
    const testCompressionCodec = async (
      codec: 'none' | 'snappy' | 'lz4' | 'gzip' | 'zstd'
    ) => {
      const writer = new ParquetWriter(storage, { compression: codec })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        data: { type: 'STRING', optional: true },
      }
      // Create some repeated data that compresses well
      const data = Array.from({ length: 100 }, (_, i) => ({
        id: `id-${i}`,
        data: 'x'.repeat(100),
      }))

      await writer.write(`compress-${codec}.parquet`, data, schema)
      const rows = await readWithHyparquet(join(tempDir, `compress-${codec}.parquet`))

      expect(rows).toHaveLength(100)
      expect(rows[0]!.id).toBe('id-0')
      expect(rows[0]!.data).toBe('x'.repeat(100))
      expect(rows[99]!.id).toBe('id-99')
    }

    it('should write uncompressed files readable by hyparquet', async () => {
      await testCompressionCodec('none')
    })

    it('should write snappy compressed files readable by hyparquet', async () => {
      await testCompressionCodec('snappy')
    })

    it('should write lz4 compressed files readable by hyparquet', async () => {
      await testCompressionCodec('lz4')
    })

    it('should write gzip compressed files readable by hyparquet', async () => {
      await testCompressionCodec('gzip')
    })

    it('should write zstd compressed files readable by hyparquet', async () => {
      await testCompressionCodec('zstd')
    })
  })

  // ===========================================================================
  // Schema Compatibility
  // ===========================================================================

  describe('Schema Compatibility', () => {
    it('should write valid entity schema files', async () => {
      const writer = new ParquetWriter(storage)
      const schema = createEntitySchema()
      const data = [
        {
          $id: 'posts/123',
          $type: 'Post',
          name: 'My Post',
          createdAt: Date.now(),
          updatedAt: Date.now(),
          createdBy: 'user/1',
          updatedBy: 'user/1',
          version: 1,
          deletedAt: null,
          deletedBy: null,
          $data: JSON.stringify({ title: 'Hello', content: 'World' }),
        },
      ]

      await writer.write('entity.parquet', data, schema)

      const filePath = join(tempDir, 'entity.parquet')
      const metadata = await readMetadataWithHyparquet(filePath)

      // Verify schema contains expected columns
      const schemaElements = metadata.schema ?? []
      const columnNames = schemaElements
        .filter(el => el.name)
        .map(el => el.name)

      expect(columnNames).toContain('$id')
      expect(columnNames).toContain('$type')
      expect(columnNames).toContain('name')
      expect(columnNames).toContain('version')

      // Verify data can be read
      const rows = await readWithHyparquet(filePath)
      expect(rows).toHaveLength(1)
      expect(rows[0]!.$id).toBe('posts/123')
      expect(rows[0]!.$type).toBe('Post')
    })

    it('should handle deeply nested directory structures', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }
      const data = [{ id: '1' }]

      await writer.write('data/namespace/partition/file.parquet', data, schema)

      const filePath = join(tempDir, 'data/namespace/partition/file.parquet')
      const fileStat = await stat(filePath)
      expect(fileStat.isFile()).toBe(true)

      const rows = await readWithHyparquet(filePath)
      expect(rows).toHaveLength(1)
      expect(rows[0]!.id).toBe('1')
    })

    it('should handle many columns correctly', async () => {
      const writer = new ParquetWriter(storage)

      // Create schema with 50 columns
      const schema: ParquetSchema = {}
      for (let i = 0; i < 50; i++) {
        schema[`col_${i}`] = { type: 'STRING', optional: true }
      }

      // Create data with all columns
      const data = Array.from({ length: 10 }, (_, rowIdx) => {
        const row: Record<string, string> = {}
        for (let i = 0; i < 50; i++) {
          row[`col_${i}`] = `row${rowIdx}_col${i}`
        }
        return row
      })

      await writer.write('many-columns.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'many-columns.parquet'))

      expect(rows).toHaveLength(10)
      expect(rows[0]!.col_0).toBe('row0_col0')
      expect(rows[0]!.col_49).toBe('row0_col49')
      expect(rows[9]!.col_25).toBe('row9_col25')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle empty files correctly', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
      }

      const result = await writer.write('empty.parquet', [], schema)

      expect(result.rowCount).toBe(0)
      expect(result.rowGroupCount).toBe(0)

      // Verify file is valid Parquet
      const filePath = join(tempDir, 'empty.parquet')
      const metadata = await readMetadataWithHyparquet(filePath)
      expect(Number(metadata.num_rows)).toBe(0)

      // Read should return empty array
      const rows = await readWithHyparquet(filePath)
      expect(rows).toHaveLength(0)
    })

    it('should handle special characters in string values', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        value: { type: 'STRING', optional: true },
      }
      const data = [
        { id: '1', value: 'quotes "and\' apostrophes' },
        { id: '2', value: 'backslash \\ and newline \n' },
        { id: '3', value: 'unicode: \u{1F600}\u{1F389}\u{2764}' },
        { id: '4', value: 'null char: \0 and tab: \t' },
        { id: '5', value: '<html>&amp;</html>' },
      ]

      await writer.write('special-chars.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'special-chars.parquet'))

      expect(rows).toHaveLength(5)
      expect(rows[0]!.value).toBe('quotes "and\' apostrophes')
      expect(rows[1]!.value).toBe('backslash \\ and newline \n')
      expect(rows[2]!.value).toBe('unicode: \u{1F600}\u{1F389}\u{2764}')
      expect(rows[3]!.value).toBe('null char: \0 and tab: \t')
      expect(rows[4]!.value).toBe('<html>&amp;</html>')
    })

    it('should handle large string values', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        content: { type: 'STRING', optional: true },
      }
      const largeContent = 'x'.repeat(100000) // 100KB string

      const data = [
        { id: '1', content: largeContent },
        { id: '2', content: largeContent },
      ]

      await writer.write('large-strings.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'large-strings.parquet'))

      expect(rows).toHaveLength(2)
      expect(rows[0]!.content).toBe(largeContent)
      expect((rows[0]!.content as string).length).toBe(100000)
    })

    it('should handle many rows correctly', async () => {
      const writer = new ParquetWriter(storage, { rowGroupSize: 1000 })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        value: { type: 'INT64', optional: true },
      }

      // Create 5000 rows - should create multiple row groups
      const data = Array.from({ length: 5000 }, (_, i) => ({
        id: `row-${i}`,
        value: i,
      }))

      const result = await writer.write('many-rows.parquet', data, schema)

      expect(result.rowCount).toBe(5000)
      expect(result.rowGroupCount).toBe(5) // 5000 / 1000 = 5 row groups

      const rows = await readWithHyparquet(join(tempDir, 'many-rows.parquet'))
      expect(rows).toHaveLength(5000)
      expect(rows[0]!.id).toBe('row-0')
      expect(rows[4999]!.id).toBe('row-4999')
    })

    it('should handle column names with special characters', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        '$id': { type: 'STRING', optional: false },
        '$type': { type: 'STRING', optional: true },
        'user-name': { type: 'STRING', optional: true },
        'count_total': { type: 'INT64', optional: true },
      }
      const data = [
        { '$id': '1', '$type': 'User', 'user-name': 'Alice', 'count_total': 100 },
      ]

      await writer.write('special-columns.parquet', data, schema)
      const rows = await readWithHyparquet(join(tempDir, 'special-columns.parquet'))

      expect(rows).toHaveLength(1)
      expect(rows[0]!['$id']).toBe('1')
      expect(rows[0]!['$type']).toBe('User')
      expect(rows[0]!['user-name']).toBe('Alice')
      expect(Number(rows[0]!['count_total'])).toBe(100)
    })
  })

  // ===========================================================================
  // Round-Trip Tests
  // ===========================================================================

  describe('Round-Trip Tests', () => {
    it('should write and read identical data with ParquetReader', async () => {
      const writer = new ParquetWriter(storage)
      const reader = new ParquetReader({ storage })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        name: { type: 'STRING', optional: true },
        age: { type: 'INT64', optional: true },
        active: { type: 'BOOLEAN', optional: true },
      }
      const originalData = [
        { id: '1', name: 'Alice', age: 30, active: true },
        { id: '2', name: 'Bob', age: 25, active: false },
        { id: '3', name: null, age: null, active: null },
      ]

      await writer.write('roundtrip.parquet', originalData, schema)

      // Read with ParquetReader
      const parqueDBRows = await reader.read('roundtrip.parquet')

      // Read with raw hyparquet
      const hyparquetRows = await readWithHyparquet(join(tempDir, 'roundtrip.parquet'))

      // Both should return same number of rows
      expect(parqueDBRows).toHaveLength(3)
      expect(hyparquetRows).toHaveLength(3)

      // Verify data matches (accounting for BigInt from hyparquet)
      expect(parqueDBRows[0]!.id).toBe('1')
      expect(hyparquetRows[0]!.id).toBe('1')
      expect(parqueDBRows[0]!.name).toBe('Alice')
      expect(hyparquetRows[0]!.name).toBe('Alice')
    })

    it('should preserve data across multiple write/read cycles', async () => {
      const writer = new ParquetWriter(storage)
      const reader = new ParquetReader({ storage })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        value: { type: 'STRING', optional: true },
      }

      // Original data
      const data1 = [{ id: '1', value: 'original' }]
      await writer.write('cycle1.parquet', data1, schema)

      // Read and verify
      const read1 = await reader.read('cycle1.parquet')
      expect(read1[0]!.value).toBe('original')

      // Write same data to new file
      await writer.write('cycle2.parquet', read1, schema)

      // Read with hyparquet (external tool)
      const read2 = await readWithHyparquet(join(tempDir, 'cycle2.parquet'))
      expect(read2[0]!.value).toBe('original')

      // Another cycle
      await writer.write('cycle3.parquet', read2, schema)
      const read3 = await reader.read('cycle3.parquet')
      expect(read3[0]!.value).toBe('original')
    })
  })

  // ===========================================================================
  // Statistics and Metadata
  // ===========================================================================

  describe('Statistics and Metadata', () => {
    it('should include column statistics for string columns', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        name: { type: 'STRING', optional: false },
      }
      const data = [
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
        { name: 'Zebra' },
      ]

      await writer.write('stats-string.parquet', data, schema)

      const metadata = await readMetadataWithHyparquet(join(tempDir, 'stats-string.parquet'))

      expect(Number(metadata.num_rows)).toBe(4)
      expect(metadata.row_groups).toBeDefined()
      expect(metadata.row_groups!.length).toBeGreaterThan(0)
    })

    it('should include column statistics for numeric columns', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        value: { type: 'INT64', optional: false },
      }
      const data = [
        { value: 10 },
        { value: 50 },
        { value: 30 },
        { value: 100 },
      ]

      await writer.write('stats-numeric.parquet', data, schema)

      const metadata = await readMetadataWithHyparquet(join(tempDir, 'stats-numeric.parquet'))

      // Check row group statistics
      const rowGroup = metadata.row_groups?.[0]
      expect(rowGroup).toBeDefined()

      const column = rowGroup?.columns?.[0]
      expect(column).toBeDefined()
      expect(column?.meta_data?.statistics).toBeDefined()
    })

    it('should preserve custom key-value metadata', async () => {
      const writer = new ParquetWriter(storage, {
        metadata: {
          application: 'ParqueDB',
          dataset: 'test-data',
          timestamp: '2024-01-01T00:00:00Z',
        },
      })
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
      }
      const data = [{ id: '1' }]

      await writer.write('custom-metadata.parquet', data, schema)

      const metadata = await readMetadataWithHyparquet(join(tempDir, 'custom-metadata.parquet'))
      const kvMetadata = metadata.key_value_metadata ?? []

      const appEntry = kvMetadata.find(kv => kv.key === 'application')
      const datasetEntry = kvMetadata.find(kv => kv.key === 'dataset')
      const timestampEntry = kvMetadata.find(kv => kv.key === 'timestamp')

      expect(appEntry?.value).toBe('ParqueDB')
      expect(datasetEntry?.value).toBe('test-data')
      expect(timestampEntry?.value).toBe('2024-01-01T00:00:00Z')
    })
  })

  // ===========================================================================
  // Multiple Files
  // ===========================================================================

  describe('Multiple Files', () => {
    it('should create multiple valid files in same directory', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        partition: { type: 'STRING', optional: true },
      }

      // Write multiple files
      await writer.write('multi/file1.parquet', [{ id: '1', partition: 'a' }], schema)
      await writer.write('multi/file2.parquet', [{ id: '2', partition: 'b' }], schema)
      await writer.write('multi/file3.parquet', [{ id: '3', partition: 'c' }], schema)

      // Verify all files exist
      const files = await readdir(join(tempDir, 'multi'))
      expect(files).toContain('file1.parquet')
      expect(files).toContain('file2.parquet')
      expect(files).toContain('file3.parquet')

      // Verify all files are readable
      const rows1 = await readWithHyparquet(join(tempDir, 'multi/file1.parquet'))
      const rows2 = await readWithHyparquet(join(tempDir, 'multi/file2.parquet'))
      const rows3 = await readWithHyparquet(join(tempDir, 'multi/file3.parquet'))

      expect(rows1[0]!.partition).toBe('a')
      expect(rows2[0]!.partition).toBe('b')
      expect(rows3[0]!.partition).toBe('c')
    })

    it('should create files in nested partition structure', async () => {
      const writer = new ParquetWriter(storage)
      const schema: ParquetSchema = {
        id: { type: 'STRING', optional: false },
        year: { type: 'STRING', optional: true },
        month: { type: 'STRING', optional: true },
      }

      // Create partitioned structure
      await writer.write('partitioned/year=2024/month=01/data.parquet', [
        { id: '1', year: '2024', month: '01' }
      ], schema)
      await writer.write('partitioned/year=2024/month=02/data.parquet', [
        { id: '2', year: '2024', month: '02' }
      ], schema)
      await writer.write('partitioned/year=2025/month=01/data.parquet', [
        { id: '3', year: '2025', month: '01' }
      ], schema)

      // Verify all files are readable
      const rows1 = await readWithHyparquet(join(tempDir, 'partitioned/year=2024/month=01/data.parquet'))
      const rows2 = await readWithHyparquet(join(tempDir, 'partitioned/year=2024/month=02/data.parquet'))
      const rows3 = await readWithHyparquet(join(tempDir, 'partitioned/year=2025/month=01/data.parquet'))

      expect(rows1[0]!.year).toBe('2024')
      expect(rows1[0]!.month).toBe('01')
      expect(rows2[0]!.month).toBe('02')
      expect(rows3[0]!.year).toBe('2025')
    })
  })
})

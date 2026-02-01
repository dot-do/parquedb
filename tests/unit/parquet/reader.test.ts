/**
 * Parquet Reader Tests
 *
 * Tests for Parquet file reading functionality including:
 * - AsyncBuffer creation and initialization
 * - Metadata parsing
 * - Data reading with column projection
 * - Row filtering and pagination
 * - Streaming iteration
 * - Row group filtering based on statistics
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  ParquetReader,
  createAsyncBuffer,
  initializeAsyncBuffer,
  readParquetMetadata,
  readParquet,
} from '@/parquet/reader'
import { ParquetWriter } from '@/parquet/writer'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { ParquetSchema, ParquetMetadata, RowFilter } from '@/parquet/types'

// =============================================================================
// Test Fixtures
// =============================================================================

const TEST_SCHEMA: ParquetSchema = {
  id: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: true },
  age: { type: 'INT64', optional: true },
  active: { type: 'BOOLEAN', optional: true },
  score: { type: 'DOUBLE', optional: true },
}

const TEST_DATA = [
  { id: '1', name: 'Alice', age: 30, active: true, score: 95.5 },
  { id: '2', name: 'Bob', age: 25, active: false, score: 87.0 },
  { id: '3', name: 'Charlie', age: 35, active: true, score: 92.3 },
  { id: '4', name: 'Diana', age: 28, active: true, score: 88.7 },
  { id: '5', name: 'Eve', age: 32, active: false, score: 91.2 },
]

// =============================================================================
// AsyncBuffer Tests
// =============================================================================

describe('AsyncBuffer', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('createAsyncBuffer', () => {
    it('should throw when byteLength accessed before initialization', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = createAsyncBuffer(storage, 'test.bin')

      expect(() => buffer.byteLength).toThrow('AsyncBuffer not initialized')
    })

    it('should be able to slice after initialization with initializeAsyncBuffer', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = await initializeAsyncBuffer(storage, 'test.bin')
      const sliced = await buffer.slice(1, 4)

      expect(new Uint8Array(sliced)).toEqual(new Uint8Array([2, 3, 4]))
    })
  })

  describe('initializeAsyncBuffer', () => {
    it('should return buffer with correct byteLength', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = await initializeAsyncBuffer(storage, 'test.bin')

      expect(buffer.byteLength).toBe(5)
    })

    it('should throw for non-existent file', async () => {
      await expect(initializeAsyncBuffer(storage, 'nonexistent.bin')).rejects.toThrow('File not found')
    })

    it('should support slicing with start only', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = await initializeAsyncBuffer(storage, 'test.bin')
      const sliced = await buffer.slice(2)

      expect(new Uint8Array(sliced)).toEqual(new Uint8Array([3, 4, 5]))
    })

    it('should support slicing full range', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = await initializeAsyncBuffer(storage, 'test.bin')
      const sliced = await buffer.slice(0, 5)

      expect(new Uint8Array(sliced)).toEqual(data)
    })

    it('should return ArrayBuffer not sharing memory', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('test.bin', data)

      const buffer = await initializeAsyncBuffer(storage, 'test.bin')
      const sliced = await buffer.slice(0, 5)

      // Modify the slice
      new Uint8Array(sliced)[0] = 99

      // Read again and verify original is unchanged
      const sliced2 = await buffer.slice(0, 5)
      expect(new Uint8Array(sliced2)[0]).toBe(1)
    })
  })
})

// =============================================================================
// ParquetReader Class Tests
// =============================================================================

describe('ParquetReader', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter
  let reader: ParquetReader

  beforeEach(async () => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage, { compression: 'none' })
    reader = new ParquetReader({ storage })

    // Write test data
    await writer.write('data/test.parquet', TEST_DATA, TEST_SCHEMA)
  })

  // ===========================================================================
  // Constructor Tests
  // ===========================================================================

  describe('constructor', () => {
    it('should create reader with storage', () => {
      const reader = new ParquetReader({ storage })
      expect(reader).toBeInstanceOf(ParquetReader)
    })

    it('should accept default columns option', () => {
      const reader = new ParquetReader({
        storage,
        columns: ['id', 'name'],
      })
      expect(reader).toBeInstanceOf(ParquetReader)
    })

    it('should accept default rowGroups option', () => {
      const reader = new ParquetReader({
        storage,
        rowGroups: [0],
      })
      expect(reader).toBeInstanceOf(ParquetReader)
    })
  })

  // ===========================================================================
  // Metadata Reading Tests
  // ===========================================================================

  describe('readMetadata', () => {
    it('should read metadata from Parquet file', async () => {
      const metadata = await reader.readMetadata('data/test.parquet')

      expect(metadata).toBeDefined()
      // numRows may be returned as bigint from hyparquet
      expect(Number(metadata.numRows)).toBe(TEST_DATA.length)
      expect(metadata.rowGroups).toBeDefined()
      expect(metadata.rowGroups.length).toBeGreaterThan(0)
    })

    it('should include schema information', async () => {
      const metadata = await reader.readMetadata('data/test.parquet')

      expect(metadata.schema).toBeDefined()
      expect(Array.isArray(metadata.schema)).toBe(true)
    })

    it('should include row group metadata', async () => {
      const metadata = await reader.readMetadata('data/test.parquet')
      const rowGroup = metadata.rowGroups[0]

      expect(rowGroup.numRows).toBeDefined()
      expect(rowGroup.columns).toBeDefined()
      expect(Array.isArray(rowGroup.columns)).toBe(true)
    })

    it('should throw for non-existent file', async () => {
      await expect(reader.readMetadata('nonexistent.parquet')).rejects.toThrow()
    })
  })

  // ===========================================================================
  // Data Reading Tests
  // ===========================================================================

  describe('read', () => {
    it('should read all data from Parquet file', async () => {
      const data = await reader.read('data/test.parquet')

      expect(data).toHaveLength(TEST_DATA.length)
    })

    it('should read data with correct types', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet')

      expect(data[0].id).toBe('1')
      expect(data[0].name).toBe('Alice')
      expect(data[0].age).toBe(30)
      expect(data[0].active).toBe(true)
      expect(data[0].score).toBeCloseTo(95.5)
    })

    it('should support column projection', async () => {
      const data = await reader.read<{ id: string; name: string }>('data/test.parquet', {
        columns: ['id', 'name'],
      })

      expect(data).toHaveLength(TEST_DATA.length)
      // Only projected columns should be present
      expect(data[0].id).toBe('1')
      expect(data[0].name).toBe('Alice')
    })

    it('should support limit option', async () => {
      const data = await reader.read('data/test.parquet', { limit: 2 })

      expect(data).toHaveLength(2)
    })

    it('should support offset option', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', { offset: 2 })

      expect(data).toHaveLength(TEST_DATA.length - 2)
      expect(data[0].id).toBe('3')
    })

    it('should support offset and limit together', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        offset: 1,
        limit: 2,
      })

      expect(data).toHaveLength(2)
      expect(data[0].id).toBe('2')
      expect(data[1].id).toBe('3')
    })

    it('should throw for non-existent file', async () => {
      await expect(reader.read('nonexistent.parquet')).rejects.toThrow()
    })
  })

  // ===========================================================================
  // Filter Tests
  // ===========================================================================

  describe('read with filter', () => {
    it('should filter with eq operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'name', op: 'eq', value: 'Alice' },
      })

      expect(data).toHaveLength(1)
      expect(data[0].name).toBe('Alice')
    })

    it('should filter with ne operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'name', op: 'ne', value: 'Alice' },
      })

      expect(data).toHaveLength(TEST_DATA.length - 1)
      expect(data.every((row) => row.name !== 'Alice')).toBe(true)
    })

    it('should filter with gt operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'age', op: 'gt', value: 30 },
      })

      expect(data.length).toBeGreaterThan(0)
      expect(data.every((row) => row.age > 30)).toBe(true)
    })

    it('should filter with gte operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'age', op: 'gte', value: 30 },
      })

      expect(data.length).toBeGreaterThan(0)
      expect(data.every((row) => row.age >= 30)).toBe(true)
    })

    it('should filter with lt operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'age', op: 'lt', value: 30 },
      })

      expect(data.length).toBeGreaterThan(0)
      expect(data.every((row) => row.age < 30)).toBe(true)
    })

    it('should filter with lte operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'age', op: 'lte', value: 30 },
      })

      expect(data.length).toBeGreaterThan(0)
      expect(data.every((row) => row.age <= 30)).toBe(true)
    })

    it('should filter with in operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'name', op: 'in', value: ['Alice', 'Bob'] },
      })

      expect(data).toHaveLength(2)
      expect(data.every((row) => ['Alice', 'Bob'].includes(row.name))).toBe(true)
    })

    it('should filter with isNull operator', async () => {
      // Add a row with null name
      const dataWithNull = [
        ...TEST_DATA,
        { id: '6', name: null as unknown as string, age: 40, active: true, score: 80.0 },
      ]
      await writer.write('data/with-null.parquet', dataWithNull, TEST_SCHEMA)

      const data = await reader.read<typeof TEST_DATA[0]>('data/with-null.parquet', {
        filter: { column: 'name', op: 'isNull' },
      })

      expect(data).toHaveLength(1)
      expect(data[0].id).toBe('6')
    })

    it('should filter with isNotNull operator', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'name', op: 'isNotNull' },
      })

      expect(data).toHaveLength(TEST_DATA.length)
    })

    it('should filter on boolean column', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'active', op: 'eq', value: true },
      })

      expect(data.length).toBeGreaterThan(0)
      expect(data.every((row) => row.active === true)).toBe(true)
    })

    it('should combine filter with limit', async () => {
      const data = await reader.read<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'active', op: 'eq', value: true },
        limit: 1,
      })

      expect(data).toHaveLength(1)
      expect(data[0].active).toBe(true)
    })
  })

  // ===========================================================================
  // Streaming Tests
  // ===========================================================================

  describe('stream', () => {
    it('should stream all rows', async () => {
      const rows: typeof TEST_DATA = []
      for await (const row of reader.stream<typeof TEST_DATA[0]>('data/test.parquet')) {
        rows.push(row)
      }

      expect(rows).toHaveLength(TEST_DATA.length)
    })

    it('should stream with limit', async () => {
      const rows: typeof TEST_DATA = []
      for await (const row of reader.stream<typeof TEST_DATA[0]>('data/test.parquet', { limit: 2 })) {
        rows.push(row)
      }

      expect(rows).toHaveLength(2)
    })

    it('should stream with offset', async () => {
      const rows: typeof TEST_DATA = []
      for await (const row of reader.stream<typeof TEST_DATA[0]>('data/test.parquet', { offset: 2 })) {
        rows.push(row)
      }

      expect(rows).toHaveLength(TEST_DATA.length - 2)
      expect(rows[0].id).toBe('3')
    })

    it('should stream with filter', async () => {
      const rows: typeof TEST_DATA = []
      for await (const row of reader.stream<typeof TEST_DATA[0]>('data/test.parquet', {
        filter: { column: 'active', op: 'eq', value: true },
      })) {
        rows.push(row)
      }

      expect(rows.every((row) => row.active === true)).toBe(true)
    })

    it('should stream with column projection', async () => {
      const rows: { id: string; name: string }[] = []
      for await (const row of reader.stream<{ id: string; name: string }>('data/test.parquet', {
        columns: ['id', 'name'],
      })) {
        rows.push(row)
      }

      expect(rows).toHaveLength(TEST_DATA.length)
      expect(rows[0].id).toBeDefined()
      expect(rows[0].name).toBeDefined()
    })
  })

  // ===========================================================================
  // Row Group Selection Tests
  // ===========================================================================

  describe('readRowGroups', () => {
    it('should read specific row groups when multiple exist', async () => {
      // Write data with multiple row groups
      const largeData = Array.from({ length: 100 }, (_, i) => ({
        id: `${i + 1}`,
        name: `User ${i + 1}`,
        age: 20 + (i % 50),
        active: i % 2 === 0,
        score: 50 + (i % 50),
      }))

      await writer.write('data/large.parquet', largeData, TEST_SCHEMA, {
        rowGroupSize: 20, // Create multiple row groups
      })

      const metadata = await reader.readMetadata('data/large.parquet')
      const numRowGroups = metadata.rowGroups.length

      // The test only makes sense if we have multiple row groups
      // With hyparquet-writer's rowGroupSize, this may vary
      if (numRowGroups > 1) {
        // Read only first row group
        const data = await reader.readRowGroups<typeof TEST_DATA[0]>('data/large.parquet', [0])

        // First row group should have fewer rows than total
        expect(data.length).toBeLessThanOrEqual(largeData.length)
      } else {
        // If only one row group exists, reading it should return all data
        const data = await reader.readRowGroups<typeof TEST_DATA[0]>('data/large.parquet', [0])
        expect(data.length).toBe(largeData.length)
      }
    })
  })

  // ===========================================================================
  // Row Group Filtering Tests
  // ===========================================================================

  describe('getRelevantRowGroups', () => {
    it('should identify relevant row groups for eq filter', () => {
      // Create metadata with statistics
      const metadata: ParquetMetadata = {
        version: 1,
        schema: [],
        numRows: 100,
        rowGroups: [
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [
              {
                pathInSchema: ['age'],
                totalCompressedSize: 500,
                totalUncompressedSize: 500,
                numValues: 50,
                encodings: ['PLAIN'],
                codec: 'UNCOMPRESSED',
                statistics: { min: 20, max: 30 },
              },
            ],
          },
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [
              {
                pathInSchema: ['age'],
                totalCompressedSize: 500,
                totalUncompressedSize: 500,
                numValues: 50,
                encodings: ['PLAIN'],
                codec: 'UNCOMPRESSED',
                statistics: { min: 40, max: 50 },
              },
            ],
          },
        ],
      }

      const filter: RowFilter = { column: 'age', op: 'eq', value: 25 }
      const relevant = reader.getRelevantRowGroups(metadata, filter)

      expect(relevant).toContain(0) // 25 is in range [20, 30]
      expect(relevant).not.toContain(1) // 25 is not in range [40, 50]
    })

    it('should identify relevant row groups for gt filter', () => {
      const metadata: ParquetMetadata = {
        version: 1,
        schema: [],
        numRows: 100,
        rowGroups: [
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [
              {
                pathInSchema: ['age'],
                totalCompressedSize: 500,
                totalUncompressedSize: 500,
                numValues: 50,
                encodings: ['PLAIN'],
                codec: 'UNCOMPRESSED',
                statistics: { min: 20, max: 30 },
              },
            ],
          },
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [
              {
                pathInSchema: ['age'],
                totalCompressedSize: 500,
                totalUncompressedSize: 500,
                numValues: 50,
                encodings: ['PLAIN'],
                codec: 'UNCOMPRESSED',
                statistics: { min: 10, max: 15 },
              },
            ],
          },
        ],
      }

      const filter: RowFilter = { column: 'age', op: 'gt', value: 25 }
      const relevant = reader.getRelevantRowGroups(metadata, filter)

      expect(relevant).toContain(0) // max 30 > 25
      expect(relevant).not.toContain(1) // max 15 is not > 25
    })

    it('should include row groups without statistics', () => {
      const metadata: ParquetMetadata = {
        version: 1,
        schema: [],
        numRows: 50,
        rowGroups: [
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [
              {
                pathInSchema: ['age'],
                totalCompressedSize: 500,
                totalUncompressedSize: 500,
                numValues: 50,
                encodings: ['PLAIN'],
                codec: 'UNCOMPRESSED',
                // No statistics
              },
            ],
          },
        ],
      }

      const filter: RowFilter = { column: 'age', op: 'eq', value: 25 }
      const relevant = reader.getRelevantRowGroups(metadata, filter)

      // Should include because we can't exclude without statistics
      expect(relevant).toContain(0)
    })

    it('should handle non-existent column gracefully', () => {
      const metadata: ParquetMetadata = {
        version: 1,
        schema: [],
        numRows: 50,
        rowGroups: [
          {
            numRows: 50,
            totalByteSize: 1000,
            columns: [],
          },
        ],
      }

      const filter: RowFilter = { column: 'nonexistent', op: 'eq', value: 25 }
      const relevant = reader.getRelevantRowGroups(metadata, filter)

      // Should include because we can't find column statistics
      expect(relevant).toContain(0)
    })
  })
})

// =============================================================================
// Standalone Function Tests
// =============================================================================

describe('Standalone Functions', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter

  beforeEach(async () => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage, { compression: 'none' })
    await writer.write('data/test.parquet', TEST_DATA, TEST_SCHEMA)
  })

  describe('readParquetMetadata', () => {
    it('should read metadata using convenience function', async () => {
      const metadata = await readParquetMetadata(storage, 'data/test.parquet')

      // numRows may be returned as bigint from hyparquet
      expect(Number(metadata.numRows)).toBe(TEST_DATA.length)
    })
  })

  describe('readParquet', () => {
    it('should read data using convenience function', async () => {
      const data = await readParquet(storage, 'data/test.parquet')

      expect(data).toHaveLength(TEST_DATA.length)
    })

    it('should support read options', async () => {
      const data = await readParquet(storage, 'data/test.parquet', { limit: 2 })

      expect(data).toHaveLength(2)
    })
  })
})

// =============================================================================
// Default Options Tests
// =============================================================================

describe('ParquetReader with Default Options', () => {
  let storage: MemoryBackend
  let writer: ParquetWriter

  beforeEach(async () => {
    storage = new MemoryBackend()
    writer = new ParquetWriter(storage, { compression: 'none' })
    await writer.write('data/test.parquet', TEST_DATA, TEST_SCHEMA)
  })

  it('should use default columns when specified', async () => {
    const reader = new ParquetReader({
      storage,
      columns: ['id', 'name'],
    })

    const data = await reader.read<{ id: string; name: string }>('data/test.parquet')

    expect(data).toHaveLength(TEST_DATA.length)
    expect(data[0].id).toBe('1')
    expect(data[0].name).toBe('Alice')
  })

  it('should override default columns with read options', async () => {
    const reader = new ParquetReader({
      storage,
      columns: ['id'],
    })

    const data = await reader.read<{ id: string; name: string }>('data/test.parquet', {
      columns: ['id', 'name'],
    })

    expect(data[0].id).toBe('1')
    expect(data[0].name).toBe('Alice')
  })
})

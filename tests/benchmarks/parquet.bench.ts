/**
 * Parquet I/O Benchmarks for ParqueDB
 *
 * Measures performance of Parquet file operations:
 * - Write small files (1KB-10KB)
 * - Write medium files (100KB-1MB)
 * - Read with predicate pushdown
 * - Row group selection
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { ParquetWriter } from '../../src/parquet/writer'
import { ParquetReader, initializeAsyncBuffer } from '../../src/parquet/reader'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { StorageBackend } from '../../src/types/storage'
import type { ParquetSchema } from '../../src/parquet/types'
import {
  generateTestData,
  randomString,
  randomInt,
  formatBytes,
} from './setup'

// =============================================================================
// Test Schema for Parquet
// =============================================================================

const entitySchema: ParquetSchema = {
  ns: { type: 'STRING', optional: false },
  id: { type: 'STRING', optional: false },
  type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  data: { type: 'STRING', optional: true }, // JSON encoded
  createdAt: { type: 'TIMESTAMP_MILLIS', optional: false },
  updatedAt: { type: 'TIMESTAMP_MILLIS', optional: false },
  version: { type: 'INT32', optional: false },
  // Shredded fields for common queries
  status: { type: 'STRING', optional: true },
  views: { type: 'INT32', optional: true },
  price: { type: 'DOUBLE', optional: true },
  active: { type: 'BOOLEAN', optional: true },
}

const postSchema: ParquetSchema = {
  id: { type: 'STRING', optional: false },
  title: { type: 'STRING', optional: false },
  content: { type: 'STRING', optional: false },
  status: { type: 'STRING', optional: false },
  views: { type: 'INT32', optional: false },
  likes: { type: 'INT32', optional: false },
  publishedAt: { type: 'TIMESTAMP_MILLIS', optional: true },
  authorId: { type: 'STRING', optional: true },
  tags: { type: 'STRING', optional: true }, // JSON array
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate entity records for Parquet
 */
function generateEntityRecords(count: number, contentSize: 'small' | 'medium' | 'large' = 'small') {
  const contentSizes = {
    small: 50,      // ~50 bytes content
    medium: 500,    // ~500 bytes content
    large: 5000,    // ~5KB content
  }

  const records = []
  const now = Date.now()

  for (let i = 0; i < count; i++) {
    records.push({
      ns: 'posts',
      id: `post-${i}`,
      type: 'Post',
      name: `Post ${i}`,
      data: JSON.stringify({
        title: `Post Title ${i}`,
        content: randomString(contentSizes[contentSize]),
        excerpt: randomString(100),
      }),
      createdAt: now - randomInt(0, 86400000),
      updatedAt: now,
      version: 1,
      status: ['draft', 'published', 'archived'][i % 3],
      views: randomInt(0, 100000),
      price: null,
      active: i % 2 === 0,
    })
  }

  return records
}

/**
 * Generate post records for Parquet
 */
function generatePostRecords(count: number, contentSize: number = 500) {
  const records = []
  const now = Date.now()
  const statuses = ['draft', 'published', 'archived']
  const tags = ['tech', 'science', 'arts', 'sports', 'business', 'featured', 'trending']

  for (let i = 0; i < count; i++) {
    records.push({
      id: `post-${i}`,
      title: `Post Title ${i}: ${randomString(30)}`,
      content: randomString(contentSize),
      status: statuses[i % 3],
      views: randomInt(0, 100000),
      likes: randomInt(0, 5000),
      publishedAt: i % 2 === 0 ? now - randomInt(0, 86400000 * 30) : null,
      authorId: `user-${i % 100}`,
      tags: JSON.stringify(
        Array.from({ length: randomInt(1, 5) }, () => tags[randomInt(0, tags.length - 1)])
      ),
    })
  }

  return records
}

// =============================================================================
// Parquet Write Benchmarks
// =============================================================================

describe('Parquet I/O Benchmarks', () => {
  describe('Write Operations', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter

    beforeEach(() => {
      storage = new MemoryBackend()
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 10000,
      })
    })

    // Small files (1KB-10KB)
    bench('write small file - 10 entities (~1KB)', async () => {
      const data = generateEntityRecords(10, 'small')
      await writer.write('test/small-10.parquet', data, entitySchema)
    })

    bench('write small file - 50 entities (~5KB)', async () => {
      const data = generateEntityRecords(50, 'small')
      await writer.write('test/small-50.parquet', data, entitySchema)
    })

    bench('write small file - 100 entities (~10KB)', async () => {
      const data = generateEntityRecords(100, 'small')
      await writer.write('test/small-100.parquet', data, entitySchema)
    })

    // Medium files (100KB-1MB)
    bench('write medium file - 500 entities (~100KB)', async () => {
      const data = generateEntityRecords(500, 'medium')
      await writer.write('test/medium-500.parquet', data, entitySchema)
    })

    bench('write medium file - 1000 entities (~200KB)', async () => {
      const data = generateEntityRecords(1000, 'medium')
      await writer.write('test/medium-1000.parquet', data, entitySchema)
    })

    bench('write medium file - 5000 entities (~1MB)', async () => {
      const data = generateEntityRecords(5000, 'medium')
      await writer.write('test/medium-5000.parquet', data, entitySchema)
    }, { iterations: 10 })

    // Large content
    bench('write large content - 100 entities with 5KB each (~500KB)', async () => {
      const data = generateEntityRecords(100, 'large')
      await writer.write('test/large-content.parquet', data, entitySchema)
    })

    // Compression comparison
    bench('write with snappy compression', async () => {
      const snappyWriter = new ParquetWriter(storage, { compression: 'snappy' })
      const data = generateEntityRecords(1000, 'medium')
      await snappyWriter.write('test/snappy.parquet', data, entitySchema)
    })

    bench('write with gzip compression', async () => {
      const gzipWriter = new ParquetWriter(storage, { compression: 'gzip' })
      const data = generateEntityRecords(1000, 'medium')
      await gzipWriter.write('test/gzip.parquet', data, entitySchema)
    })

    bench('write uncompressed', async () => {
      const noCompWriter = new ParquetWriter(storage, { compression: 'none' })
      const data = generateEntityRecords(1000, 'medium')
      await noCompWriter.write('test/uncompressed.parquet', data, entitySchema)
    })

    // Row group size impact
    bench('write with small row groups (100)', async () => {
      const smallRgWriter = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 100,
      })
      const data = generateEntityRecords(1000, 'small')
      await smallRgWriter.write('test/small-rg.parquet', data, entitySchema)
    })

    bench('write with large row groups (10000)', async () => {
      const largeRgWriter = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 10000,
      })
      const data = generateEntityRecords(1000, 'small')
      await largeRgWriter.write('test/large-rg.parquet', data, entitySchema)
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('Read Operations', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    let reader: ParquetReader

    // Paths for pre-written test files
    const smallFilePath = 'read-test/small.parquet'
    const mediumFilePath = 'read-test/medium.parquet'
    const largeFilePath = 'read-test/large.parquet'
    const multiRowGroupPath = 'read-test/multi-rg.parquet'

    beforeAll(async () => {
      storage = new MemoryBackend()
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 1000,
      })
      reader = new ParquetReader({ storage })

      // Pre-write test files
      // Small file: 100 records
      await writer.write(smallFilePath, generatePostRecords(100, 200), postSchema)

      // Medium file: 5000 records
      await writer.write(mediumFilePath, generatePostRecords(5000, 500), postSchema)

      // Large file: 20000 records
      await writer.write(largeFilePath, generatePostRecords(20000, 500), postSchema)

      // Multi row-group file: 10000 records with 500 per row group
      const multiRgWriter = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 500,
      })
      await multiRgWriter.write(multiRowGroupPath, generatePostRecords(10000, 300), postSchema)
    })

    // Basic read operations
    bench('read small file (100 records)', async () => {
      await reader.read(smallFilePath)
    })

    bench('read medium file (5000 records)', async () => {
      await reader.read(mediumFilePath)
    })

    bench('read large file (20000 records)', async () => {
      await reader.read(largeFilePath)
    }, { iterations: 5 })

    // Column projection
    bench('read with column projection (2 columns)', async () => {
      await reader.read(mediumFilePath, {
        columns: ['id', 'status'],
      })
    })

    bench('read with column projection (4 columns)', async () => {
      await reader.read(mediumFilePath, {
        columns: ['id', 'title', 'status', 'views'],
      })
    })

    bench('read all columns', async () => {
      await reader.read(mediumFilePath)
    })

    // Row limit
    bench('read with limit 10', async () => {
      await reader.read(mediumFilePath, { limit: 10 })
    })

    bench('read with limit 100', async () => {
      await reader.read(mediumFilePath, { limit: 100 })
    })

    bench('read with limit 1000', async () => {
      await reader.read(mediumFilePath, { limit: 1000 })
    })

    // Metadata reading
    bench('read metadata only (small)', async () => {
      await reader.readMetadata(smallFilePath)
    })

    bench('read metadata only (large)', async () => {
      await reader.readMetadata(largeFilePath)
    })

    bench('read schema from file', async () => {
      const metadata = await reader.readMetadata(mediumFilePath)
      // Schema is included in metadata
    })
  })

  // ===========================================================================
  // Predicate Pushdown
  // ===========================================================================

  describe('Predicate Pushdown', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    let reader: ParquetReader
    const testFilePath = 'predicate-test/data.parquet'

    beforeAll(async () => {
      storage = new MemoryBackend()
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 1000,
        statistics: true, // Enable statistics for predicate pushdown
      })
      reader = new ParquetReader({ storage })

      // Write test file with statistics
      await writer.write(testFilePath, generatePostRecords(10000, 300), postSchema)
    })

    bench('read without filter (baseline)', async () => {
      await reader.read(testFilePath, { limit: 100 })
    })

    bench('read with equality filter (status)', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'status', op: 'eq', value: 'published' },
        limit: 100,
      })
    })

    bench('read with range filter (views > 50000)', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'views', op: 'gt', value: 50000 },
        limit: 100,
      })
    })

    bench('read with range filter (views >= 10000)', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'views', op: 'gte', value: 10000 },
        limit: 100,
      })
    })

    bench('read with null check filter (isNotNull)', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'publishedAt', op: 'isNotNull' },
        limit: 100,
      })
    })

    bench('read with less than filter (views < 50000)', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'views', op: 'lt', value: 50000 },
        limit: 100,
      })
    })

    bench('read with not equal filter', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'status', op: 'ne', value: 'draft' },
        limit: 100,
      })
    })

    bench('read with in filter', async () => {
      await reader.read(testFilePath, {
        filter: { column: 'status', op: 'in', value: ['published', 'archived'] },
        limit: 100,
      })
    })
  })

  // ===========================================================================
  // Row Group Selection
  // ===========================================================================

  describe('Row Group Selection', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    let reader: ParquetReader
    const multiRgPath = 'rowgroup-test/multi.parquet'

    beforeAll(async () => {
      storage = new MemoryBackend()

      // Create file with many row groups
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 500, // Small row groups for testing
        statistics: true,
      })
      reader = new ParquetReader({ storage })

      // Write 20 row groups (10000 records / 500 per group)
      await writer.write(multiRgPath, generatePostRecords(10000, 300), postSchema)
    })

    bench('read all row groups', async () => {
      await reader.read(multiRgPath)
    }, { iterations: 5 })

    bench('read specific row groups [0]', async () => {
      await reader.read(multiRgPath, {
        rowGroups: [0],
      })
    })

    bench('read specific row groups [0, 1, 2]', async () => {
      await reader.read(multiRgPath, {
        rowGroups: [0, 1, 2],
      })
    })

    bench('read specific row groups [5, 10, 15]', async () => {
      await reader.read(multiRgPath, {
        rowGroups: [5, 10, 15],
      })
    })

    bench('read row groups with predicate pushdown', async () => {
      // This should skip row groups where statistics indicate no matches
      await reader.read(multiRgPath, {
        filter: { column: 'views', op: 'gt', value: 90000 },
      })
    })

    bench('read row groups with columns + filter', async () => {
      await reader.read(multiRgPath, {
        columns: ['id', 'status', 'views'],
        filter: { column: 'status', op: 'eq', value: 'published' },
      })
    })
  })

  // ===========================================================================
  // Streaming Read
  // ===========================================================================

  describe('Streaming Read', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    let reader: ParquetReader
    const streamFilePath = 'stream-test/data.parquet'

    beforeAll(async () => {
      storage = new MemoryBackend()
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 1000,
      })
      reader = new ParquetReader({ storage })

      // Write test file
      await writer.write(streamFilePath, generatePostRecords(5000, 300), postSchema)
    })

    bench('stream all rows', async () => {
      let count = 0
      for await (const row of reader.stream(streamFilePath)) {
        count++
      }
    })

    bench('stream with early termination (first 100)', async () => {
      let count = 0
      for await (const row of reader.stream(streamFilePath)) {
        count++
        if (count >= 100) break
      }
    })

    bench('stream with column projection', async () => {
      let count = 0
      for await (const row of reader.stream(streamFilePath, {
        columns: ['id', 'status'],
      })) {
        count++
      }
    })

    bench('stream with filter', async () => {
      let count = 0
      for await (const row of reader.stream(streamFilePath, {
        filter: { column: 'status', op: 'eq', value: 'published' },
      })) {
        count++
      }
    })
  })

  // ===========================================================================
  // Append Operations
  // ===========================================================================

  describe('Append Operations', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    const appendFilePath = 'append-test/data.parquet'

    beforeEach(async () => {
      storage = new MemoryBackend()
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 1000,
      })

      // Create initial file
      await writer.write(appendFilePath, generatePostRecords(1000, 300), postSchema)
    })

    bench('append 10 records', async () => {
      const newData = generatePostRecords(10, 300)
      await writer.append(appendFilePath, newData)
    })

    bench('append 100 records', async () => {
      const newData = generatePostRecords(100, 300)
      await writer.append(appendFilePath, newData)
    })

    bench('append 1000 records', async () => {
      const newData = generatePostRecords(1000, 300)
      await writer.append(appendFilePath, newData)
    })
  })

  // ===========================================================================
  // Compaction
  // ===========================================================================

  describe('Compaction Operations', () => {
    let storage: MemoryBackend
    let writer: ParquetWriter
    const compactFilePath = 'compact-test/data.parquet'

    beforeEach(async () => {
      storage = new MemoryBackend()

      // Create file with many small row groups (simulating many appends)
      writer = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 100, // Very small row groups
      })

      // Write initial data (will create many row groups)
      await writer.write(compactFilePath, generatePostRecords(2000, 200), postSchema)
    })

    bench('compact file (reduce row groups)', async () => {
      const compactWriter = new ParquetWriter(storage, {
        compression: 'snappy',
        rowGroupSize: 1000, // Larger row groups
      })
      await compactWriter.compact(compactFilePath)
    })

    bench('rewrite with better compression', async () => {
      const gzipWriter = new ParquetWriter(storage, {
        compression: 'gzip',
        rowGroupSize: 1000,
      })
      // Read then rewrite with different settings
      const reader = new ParquetReader({ storage })
      const data = await reader.read(compactFilePath)
      await gzipWriter.write('compact-test/rewritten.parquet', data, postSchema)
    })
  })
})

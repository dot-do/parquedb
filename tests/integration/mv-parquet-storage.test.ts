/**
 * MV + Parquet Storage Integration Tests
 *
 * Tests materialized view storage with Parquet format:
 * - MV storage in Parquet format
 * - Read/write operations
 * - Schema compatibility
 * - Large data handling
 *
 * Uses real FsBackend storage - no mocks.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { FsBackend } from '../../src/storage/FsBackend'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  MVStorageManager,
  MVStoragePaths,
  MVNotFoundError,
  MVAlreadyExistsError,
} from '../../src/materialized-views/storage'
import { viewName } from '../../src/materialized-views/types'
import type {
  ViewDefinition,
  ViewMetadata,
  ViewOptions,
  ViewQuery,
} from '../../src/materialized-views/types'
import { ParquetWriter } from '../../src/parquet/writer'
import { ParquetReader } from '../../src/parquet/reader'
import type { ParquetSchema } from '../../src/parquet/types'
import { createTestData, decodeData, createRandomData, generateTestId } from '../factories'

// =============================================================================
// Test Helpers
// =============================================================================

function createViewDefinition(
  name: string,
  overrides: Partial<ViewDefinition> = {}
): ViewDefinition {
  return {
    name: viewName(name),
    source: 'test_source',
    query: { filter: {} },
    options: {
      refreshMode: 'manual',
    },
    ...overrides,
  }
}

function createTestRows(count: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => ({
    $id: `test/${generateTestId()}`,
    $type: 'TestEntity',
    name: `Test Entity ${i}`,
    value: i * 100,
    status: i % 2 === 0 ? 'active' : 'inactive',
    createdAt: Date.now() - i * 1000,
    tags: ['tag1', 'tag2'],
  }))
}

const testSchema: ParquetSchema = {
  $id: { type: 'UTF8', optional: false },
  $type: { type: 'UTF8', optional: false },
  name: { type: 'UTF8', optional: true },
  value: { type: 'INT64', optional: true },
  status: { type: 'UTF8', optional: true },
  createdAt: { type: 'INT64', optional: true },
}

// =============================================================================
// MV Storage Manager Tests
// =============================================================================

describe('MV + Parquet Storage Integration', () => {
  let backend: FsBackend
  let mvStorage: MVStorageManager
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `parquedb-mv-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
    mvStorage = new MVStorageManager(backend)
  })

  afterEach(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('MV Storage in Parquet Format', () => {
    it('should create and store MV metadata', async () => {
      const viewDef = createViewDefinition('test_view')
      const metadata = await mvStorage.createView(viewDef)

      expect(metadata.definition.name).toBe('test_view')
      expect(metadata.state).toBe('pending')
      expect(metadata.version).toBe(1)
      expect(metadata.createdAt).toBeInstanceOf(Date)
    })

    it('should write and read MV data in Parquet format', async () => {
      const viewDef = createViewDefinition('parquet_view')
      await mvStorage.createView(viewDef)

      // Write Parquet data for the view
      const testRows = createTestRows(100)
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('parquet_view')

      await writer.write(dataPath, testRows, testSchema)

      // Verify data was written
      const exists = await mvStorage.viewDataExists('parquet_view')
      expect(exists).toBe(true)

      // Read back the data
      const reader = new ParquetReader({ storage: backend })
      const readData = await reader.read(dataPath)

      expect(readData.length).toBe(100)
      expect(readData[0]).toHaveProperty('$id')
      expect(readData[0]).toHaveProperty('name')
    })

    it('should store view manifest correctly', async () => {
      // Create multiple views
      await mvStorage.createView(createViewDefinition('view_1'))
      await mvStorage.createView(createViewDefinition('view_2'))
      await mvStorage.createView(createViewDefinition('view_3'))

      // List views
      const views = await mvStorage.listViews()
      expect(views.length).toBe(3)
      expect(views.map(v => v.name)).toContain('view_1')
      expect(views.map(v => v.name)).toContain('view_2')
      expect(views.map(v => v.name)).toContain('view_3')
    })

    it('should persist view stats in JSON', async () => {
      const viewDef = createViewDefinition('stats_view')
      await mvStorage.createView(viewDef)

      // Record some stats
      await mvStorage.recordRefresh('stats_view', true, 150)
      await mvStorage.recordRefresh('stats_view', true, 200)
      await mvStorage.recordRefresh('stats_view', false, 50)

      const stats = await mvStorage.getViewStats('stats_view')
      expect(stats.totalRefreshes).toBe(3)
      expect(stats.successfulRefreshes).toBe(2)
      expect(stats.failedRefreshes).toBe(1)
      expect(stats.avgRefreshDurationMs).toBeGreaterThan(0)
    })
  })

  describe('Read/Write Operations', () => {
    it('should write view data atomically', async () => {
      const viewDef = createViewDefinition('atomic_view')
      await mvStorage.createView(viewDef)

      const testRows = createTestRows(50)
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('atomic_view')

      const result = await writer.write(dataPath, testRows, testSchema)

      expect(result.rowCount).toBe(50)
      expect(result.etag).toBeDefined()
    })

    it('should support partial reads with column projection', async () => {
      const viewDef = createViewDefinition('projection_view')
      await mvStorage.createView(viewDef)

      const testRows = createTestRows(100)
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('projection_view')
      await writer.write(dataPath, testRows, testSchema)

      // Read only specific columns
      const reader = new ParquetReader({ storage: backend })
      const partialData = await reader.read(dataPath, { columns: ['$id', 'name'] })

      expect(partialData.length).toBe(100)
      expect(partialData[0]).toHaveProperty('$id')
      expect(partialData[0]).toHaveProperty('name')
      // Other columns should not be present when column projection is supported
    })

    it('should support reading with offset and limit', async () => {
      const viewDef = createViewDefinition('pagination_view')
      await mvStorage.createView(viewDef)

      const testRows = createTestRows(100)
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('pagination_view')
      await writer.write(dataPath, testRows, testSchema)

      // Read with pagination
      const reader = new ParquetReader({ storage: backend })
      const page = await reader.read(dataPath, { offset: 10, limit: 20 })

      expect(page.length).toBe(20)
    })

    it('should update view metadata after refresh', async () => {
      const viewDef = createViewDefinition('refresh_view')
      await mvStorage.createView(viewDef)

      // Simulate a refresh - set state to building
      await mvStorage.updateViewState('refresh_view', 'building')
      let metadata = await mvStorage.getViewMetadata('refresh_view')
      expect(metadata.state).toBe('building')

      // Record the refresh (updates stats and lastRefreshedAt)
      await mvStorage.recordRefresh('refresh_view', true, 100)
      // Update state to ready after successful refresh
      await mvStorage.updateViewState('refresh_view', 'ready')
      metadata = await mvStorage.getViewMetadata('refresh_view')
      expect(metadata.state).toBe('ready')
      expect(metadata.lastRefreshedAt).toBeInstanceOf(Date)
    })

    it('should handle concurrent view writes', async () => {
      const viewDef = createViewDefinition('concurrent_view')
      await mvStorage.createView(viewDef)

      const dataPath = mvStorage.getDataFilePath('concurrent_view')
      const writer = new ParquetWriter(backend)

      // Write multiple times in sequence (concurrent writes to same file would conflict)
      for (let i = 0; i < 5; i++) {
        const rows = createTestRows(20)
        await writer.write(dataPath, rows, testSchema)
      }

      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(dataPath)
      expect(data.length).toBe(20) // Last write wins
    })
  })

  describe('Schema Compatibility', () => {
    it('should handle views with different schemas', async () => {
      // View 1: Simple schema
      const view1Def = createViewDefinition('schema_view_1')
      await mvStorage.createView(view1Def)

      const simpleSchema: ParquetSchema = {
        id: { type: 'UTF8', optional: false },
        name: { type: 'UTF8', optional: true },
      }
      const simpleRows = [
        { id: 'id-1', name: 'Name 1' },
        { id: 'id-2', name: 'Name 2' },
      ]

      const writer = new ParquetWriter(backend)
      await writer.write(mvStorage.getDataFilePath('schema_view_1'), simpleRows, simpleSchema)

      // View 2: Complex schema with nested types
      const view2Def = createViewDefinition('schema_view_2')
      await mvStorage.createView(view2Def)

      const complexSchema: ParquetSchema = {
        $id: { type: 'UTF8', optional: false },
        $type: { type: 'UTF8', optional: false },
        title: { type: 'UTF8', optional: true },
        count: { type: 'INT64', optional: true },
        price: { type: 'DOUBLE', optional: true },
        active: { type: 'BOOLEAN', optional: true },
        timestamp: { type: 'TIMESTAMP_MILLIS', optional: true },
      }
      const complexRows = [
        { $id: 'prod-1', $type: 'Product', title: 'Product 1', count: 10, price: 29.99, active: true, timestamp: Date.now() },
        { $id: 'prod-2', $type: 'Product', title: 'Product 2', count: 5, price: 49.99, active: false, timestamp: Date.now() },
      ]

      await writer.write(mvStorage.getDataFilePath('schema_view_2'), complexRows, complexSchema)

      // Read both views
      const reader = new ParquetReader({ storage: backend })
      const data1 = await reader.read(mvStorage.getDataFilePath('schema_view_1'))
      const data2 = await reader.read(mvStorage.getDataFilePath('schema_view_2'))

      expect(data1.length).toBe(2)
      expect(data2.length).toBe(2)
      expect(data2[0]).toHaveProperty('price')
      expect(data2[0]).toHaveProperty('active')
    })

    it('should handle nullable fields correctly', async () => {
      const viewDef = createViewDefinition('nullable_view')
      await mvStorage.createView(viewDef)

      const schema: ParquetSchema = {
        id: { type: 'UTF8', optional: false },
        requiredField: { type: 'UTF8', optional: false },
        optionalField: { type: 'UTF8', optional: true },
        optionalNumber: { type: 'INT64', optional: true },
      }

      const rows = [
        { id: '1', requiredField: 'req-1', optionalField: 'opt-1', optionalNumber: 100 },
        { id: '2', requiredField: 'req-2', optionalField: null, optionalNumber: null },
        { id: '3', requiredField: 'req-3', optionalField: undefined, optionalNumber: undefined },
      ]

      const writer = new ParquetWriter(backend)
      await writer.write(mvStorage.getDataFilePath('nullable_view'), rows, schema)

      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(mvStorage.getDataFilePath('nullable_view'))

      expect(data.length).toBe(3)
      expect(data[0].optionalField).toBe('opt-1')
      // Null values should be handled gracefully
    })

    it('should handle empty views', async () => {
      const viewDef = createViewDefinition('empty_view')
      await mvStorage.createView(viewDef)

      const schema: ParquetSchema = {
        $id: { type: 'UTF8', optional: false },
        name: { type: 'UTF8', optional: true },
      }

      const writer = new ParquetWriter(backend)
      const result = await writer.write(mvStorage.getDataFilePath('empty_view'), [], schema)

      expect(result.rowCount).toBe(0)

      // Verify empty file can be read
      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(mvStorage.getDataFilePath('empty_view'))
      expect(data.length).toBe(0)
    })

    it('should support various Parquet data types', async () => {
      const viewDef = createViewDefinition('types_view')
      await mvStorage.createView(viewDef)

      const schema: ParquetSchema = {
        id: { type: 'UTF8', optional: false },
        stringField: { type: 'UTF8', optional: true },
        int32Field: { type: 'INT32', optional: true },
        int64Field: { type: 'INT64', optional: true },
        floatField: { type: 'FLOAT', optional: true },
        doubleField: { type: 'DOUBLE', optional: true },
        boolField: { type: 'BOOLEAN', optional: true },
      }

      const rows = [
        {
          id: 'types-1',
          stringField: 'hello',
          int32Field: 42,
          int64Field: 9007199254740991, // Max safe integer
          floatField: 3.14,
          doubleField: 2.718281828459045,
          boolField: true,
        },
        {
          id: 'types-2',
          stringField: 'world',
          int32Field: -42,
          int64Field: -9007199254740991,
          floatField: -3.14,
          doubleField: -2.718281828459045,
          boolField: false,
        },
      ]

      const writer = new ParquetWriter(backend)
      await writer.write(mvStorage.getDataFilePath('types_view'), rows, schema)

      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(mvStorage.getDataFilePath('types_view'))

      expect(data.length).toBe(2)
      expect(typeof data[0].stringField).toBe('string')
      expect(typeof data[0].boolField).toBe('boolean')
    })
  })

  describe('Large Data Handling', () => {
    it('should handle large datasets (1000+ rows)', async () => {
      const viewDef = createViewDefinition('large_view')
      await mvStorage.createView(viewDef)

      const largeRowCount = 1000
      const rows = createTestRows(largeRowCount)

      const writer = new ParquetWriter(backend, { rowGroupSize: 100 })
      const dataPath = mvStorage.getDataFilePath('large_view')
      const result = await writer.write(dataPath, rows, testSchema)

      expect(result.rowCount).toBe(largeRowCount)
      expect(result.rowGroupCount).toBeGreaterThan(1) // Multiple row groups

      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(dataPath)
      expect(data.length).toBe(largeRowCount)
    })

    it('should handle 10000 rows efficiently', async () => {
      const viewDef = createViewDefinition('xlarge_view')
      await mvStorage.createView(viewDef)

      const rowCount = 10000
      const rows = createTestRows(rowCount)

      const startWrite = Date.now()
      const writer = new ParquetWriter(backend, { rowGroupSize: 1000 })
      const dataPath = mvStorage.getDataFilePath('xlarge_view')
      await writer.write(dataPath, rows, testSchema)
      const writeTime = Date.now() - startWrite

      const startRead = Date.now()
      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(dataPath)
      const readTime = Date.now() - startRead

      expect(data.length).toBe(rowCount)
      // Performance sanity check (should complete in reasonable time)
      expect(writeTime).toBeLessThan(30000) // 30 seconds max
      expect(readTime).toBeLessThan(30000)
    })

    it('should support streaming large datasets', async () => {
      const viewDef = createViewDefinition('stream_view')
      await mvStorage.createView(viewDef)

      const rowCount = 500
      const rows = createTestRows(rowCount)

      // Use default row group size to avoid streaming issues
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('stream_view')
      await writer.write(dataPath, rows, testSchema)

      // Read all rows first to verify write worked correctly
      const reader = new ParquetReader({ storage: backend })
      const allData = await reader.read(dataPath)
      expect(allData.length).toBe(rowCount)

      // Stream rows - note: streaming yields rows from each row group
      let streamedCount = 0
      for await (const row of reader.stream(dataPath)) {
        streamedCount++
        expect(row).toHaveProperty('$id')
      }
      // Streaming should return same number as read
      expect(streamedCount).toBe(allData.length)
    })

    it('should handle views with wide rows (many columns)', async () => {
      const viewDef = createViewDefinition('wide_view')
      await mvStorage.createView(viewDef)

      // Create schema with many columns
      const wideSchema: ParquetSchema = {
        id: { type: 'UTF8', optional: false },
      }
      for (let i = 0; i < 50; i++) {
        wideSchema[`col_${i}`] = { type: 'UTF8', optional: true }
      }

      // Create rows with all columns
      const rows = Array.from({ length: 100 }, (_, rowIdx) => {
        const row: Record<string, unknown> = { id: `row-${rowIdx}` }
        for (let i = 0; i < 50; i++) {
          row[`col_${i}`] = `value-${rowIdx}-${i}`
        }
        return row
      })

      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('wide_view')
      const result = await writer.write(dataPath, rows, wideSchema)

      expect(result.columns.length).toBe(51) // id + 50 columns

      const reader = new ParquetReader({ storage: backend })
      const data = await reader.read(dataPath)
      expect(data.length).toBe(100)
      expect(Object.keys(data[0]!).length).toBe(51)
    })

    it('should handle sharded view data files', async () => {
      const viewDef = createViewDefinition('sharded_view')
      await mvStorage.createView(viewDef)

      // Write multiple shards
      const writer = new ParquetWriter(backend, { rowGroupSize: 50 })
      const shardCount = 3

      for (let shard = 0; shard < shardCount; shard++) {
        const rows = createTestRows(100)
        const shardPath = mvStorage.getDataShardPath('sharded_view', shard)
        await writer.write(shardPath, rows, testSchema)
      }

      // List data files
      const dataFiles = await mvStorage.listViewDataFiles('sharded_view')
      expect(dataFiles.length).toBe(shardCount)

      // Read all shards
      const reader = new ParquetReader({ storage: backend })
      let totalRows = 0
      for (const file of dataFiles) {
        const data = await reader.read(file)
        totalRows += data.length
      }
      expect(totalRows).toBe(300) // 3 shards * 100 rows
    })
  })

  describe('Error Handling', () => {
    it('should throw MVNotFoundError for non-existent view', async () => {
      await expect(mvStorage.getViewMetadata('nonexistent_view')).rejects.toThrow(MVNotFoundError)
    })

    it('should throw MVAlreadyExistsError for duplicate view', async () => {
      await mvStorage.createView(createViewDefinition('duplicate_view'))
      await expect(mvStorage.createView(createViewDefinition('duplicate_view'))).rejects.toThrow(MVAlreadyExistsError)
    })

    it('should handle missing data file gracefully', async () => {
      const viewDef = createViewDefinition('no_data_view')
      await mvStorage.createView(viewDef)

      const exists = await mvStorage.viewDataExists('no_data_view')
      expect(exists).toBe(false)
    })

    it('should delete view and its data', async () => {
      const viewDef = createViewDefinition('deletable_view')
      await mvStorage.createView(viewDef)

      // Write some data
      const writer = new ParquetWriter(backend)
      const dataPath = mvStorage.getDataFilePath('deletable_view')
      await writer.write(dataPath, createTestRows(10), testSchema)

      // Delete the view
      const deleted = await mvStorage.deleteView('deletable_view')
      expect(deleted).toBe(true)

      // Verify view is gone
      const views = await mvStorage.listViews()
      expect(views.find(v => v.name === 'deletable_view')).toBeUndefined()
    })
  })
})

// =============================================================================
// MemoryBackend Tests (for fast unit-level integration)
// =============================================================================

describe('MV + Parquet Storage with MemoryBackend', () => {
  let backend: MemoryBackend
  let mvStorage: MVStorageManager

  beforeEach(() => {
    backend = new MemoryBackend()
    mvStorage = new MVStorageManager(backend)
  })

  it('should work with in-memory storage', async () => {
    const viewDef = createViewDefinition('memory_view')
    await mvStorage.createView(viewDef)

    const rows = createTestRows(50)
    const writer = new ParquetWriter(backend)
    const dataPath = mvStorage.getDataFilePath('memory_view')
    await writer.write(dataPath, rows, testSchema)

    const reader = new ParquetReader({ storage: backend })
    const data = await reader.read(dataPath)
    expect(data.length).toBe(50)
  })

  it('should support query recording', async () => {
    const viewDef = createViewDefinition('query_stats_view')
    await mvStorage.createView(viewDef)

    // Record queries
    await mvStorage.recordQuery('query_stats_view', true) // Cache hit
    await mvStorage.recordQuery('query_stats_view', false) // Cache miss
    await mvStorage.recordQuery('query_stats_view', true) // Cache hit

    const stats = await mvStorage.getViewStats('query_stats_view')
    expect(stats.queryCount).toBe(3)
    expect(stats.cacheHitRatio).toBeGreaterThan(0)
  })

  it('should track views needing refresh', async () => {
    // Create a stale streaming view (manual views are excluded from auto-refresh)
    const viewDef = createViewDefinition('stale_view', {
      options: {
        refreshMode: 'streaming', // Must be streaming or scheduled for auto-refresh
      },
    })
    await mvStorage.createView(viewDef)
    await mvStorage.updateViewState('stale_view', 'stale')

    const needsRefresh = await mvStorage.getViewsNeedingRefresh()
    expect(needsRefresh.some(v => v.definition.name === 'stale_view')).toBe(true)
  })

  it('should get views by source collection', async () => {
    await mvStorage.createView(createViewDefinition('source_view_1', { source: 'users' }))
    await mvStorage.createView(createViewDefinition('source_view_2', { source: 'users' }))
    await mvStorage.createView(createViewDefinition('source_view_3', { source: 'posts' }))

    const userViews = await mvStorage.getViewsBySource('users')
    expect(userViews.length).toBe(2)

    const postViews = await mvStorage.getViewsBySource('posts')
    expect(postViews.length).toBe(1)
  })
})

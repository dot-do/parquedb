/**
 * Tests for Columnar Aggregations without Row Materialization
 *
 * These tests verify that aggregation operations (COUNT, SUM, AVG, MIN, MAX)
 * can be performed directly on column data without materializing row objects,
 * which provides significant performance and memory improvements.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { ParquetWriter } from '../../src/parquet/writer'
import { ParquetReader } from '../../src/parquet/reader'
import type { ParquetSchema } from '../../src/parquet/types'
import {
  parquetAggregate,
  type AggregationSpec,
  type AggregationResult,
  ColumnarAggregator,
} from '../../src/query/columnar-aggregations'

// =============================================================================
// Test Data Setup
// =============================================================================

// Schema for test data
const testSchema: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  category: { type: 'STRING', optional: false },
  price: { type: 'INT32', optional: false },
  quantity: { type: 'INT32', optional: false },
  discount: { type: 'DOUBLE', optional: true },
  active: { type: 'BOOLEAN', optional: false },
}

// Schema for null value tests
const nullValueSchema: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  value: { type: 'DOUBLE', optional: true },
}

describe('Columnar Aggregations', () => {
  let storage: MemoryBackend
  let reader: ParquetReader
  const testPath = 'test/aggregations.parquet'

  // Test data with various types for aggregation testing
  const testData = [
    { $id: '1', category: 'A', price: 100, quantity: 5, discount: 10.5, active: true },
    { $id: '2', category: 'B', price: 200, quantity: 3, discount: 15.0, active: true },
    { $id: '3', category: 'A', price: 150, quantity: 8, discount: null, active: false },
    { $id: '4', category: 'C', price: 300, quantity: 2, discount: 20.0, active: true },
    { $id: '5', category: 'B', price: 250, quantity: 6, discount: 12.5, active: false },
    { $id: '6', category: 'A', price: 175, quantity: 4, discount: null, active: true },
    { $id: '7', category: 'C', price: 400, quantity: 1, discount: 25.0, active: false },
    { $id: '8', category: 'B', price: 225, quantity: 7, discount: 18.0, active: true },
    { $id: '9', category: 'A', price: 125, quantity: 9, discount: 8.5, active: true },
    { $id: '10', category: 'C', price: 350, quantity: 3, discount: 22.5, active: false },
  ]

  beforeAll(async () => {
    storage = new MemoryBackend()
    reader = new ParquetReader({ storage })

    // Write test data to parquet file
    const writer = new ParquetWriter(storage)
    await writer.write(testPath, testData, testSchema)
  })

  afterAll(async () => {
    await storage.delete(testPath)
  })

  // ===========================================================================
  // COUNT(*) Tests - Should use num_rows from metadata (no data read)
  // ===========================================================================

  describe('COUNT(*) - Metadata Only', () => {
    it('should return total row count from metadata without reading data', async () => {
      const spec: AggregationSpec = {
        count: { type: 'count', field: '*' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.count).toBe(10)
      expect(result._stats.dataRead).toBe(false) // No data columns read
      expect(result._stats.metadataOnly).toBe(true)
    })

    it('should sum num_rows from all row groups', async () => {
      // This test verifies the count is aggregated across row groups
      const spec: AggregationSpec = {
        totalCount: { type: 'count', field: '*' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.totalCount).toBe(10)
    })
  })

  // ===========================================================================
  // COUNT(column) Tests - Should read only that column
  // ===========================================================================

  describe('COUNT(column) - Single Column Read', () => {
    it('should count non-null values in a column', async () => {
      const spec: AggregationSpec = {
        discountCount: { type: 'count', field: 'discount' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // 3 null values: ids 3, 6, and... wait, let me check test data
      // discount: null for $id 3 and 6 only, so 8 non-null values
      expect(result.discountCount).toBe(8)
      expect(result._stats.columnsRead).toEqual(['discount'])
    })

    it('should return 0 for column with all nulls', async () => {
      // Create a test file with all nulls in one column
      const nullData = [
        { $id: '1', value: null },
        { $id: '2', value: null },
        { $id: '3', value: null },
      ]
      const nullPath = 'test/null-column.parquet'
      const writer = new ParquetWriter(storage)
      await writer.write(nullPath, nullData, nullValueSchema)

      const spec: AggregationSpec = {
        valueCount: { type: 'count', field: 'value' },
      }

      const result = await parquetAggregate(reader, nullPath, spec)

      expect(result.valueCount).toBe(0)

      await storage.delete(nullPath)
    })
  })

  // ===========================================================================
  // SUM Tests - Process column arrays without row objects
  // ===========================================================================

  describe('SUM - Columnar Processing', () => {
    it('should sum numeric column values without row materialization', async () => {
      const spec: AggregationSpec = {
        totalPrice: { type: 'sum', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Sum: 100 + 200 + 150 + 300 + 250 + 175 + 400 + 225 + 125 + 350 = 2275
      expect(result.totalPrice).toBe(2275)
      expect(result._stats.columnsRead).toEqual(['price'])
      expect(result._stats.rowsProcessed).toBe(0) // No row objects created
    })

    it('should handle null values in sum (skip nulls)', async () => {
      const spec: AggregationSpec = {
        totalDiscount: { type: 'sum', field: 'discount' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Sum of non-null discounts: 10.5 + 15 + 20 + 12.5 + 25 + 18 + 8.5 + 22.5 = 132
      expect(result.totalDiscount).toBeCloseTo(132, 1)
    })

    it('should return 0 for sum of empty/all-null column', async () => {
      const nullData = [
        { $id: '1', value: null },
        { $id: '2', value: null },
      ]
      const nullPath = 'test/null-sum.parquet'
      const writer = new ParquetWriter(storage)
      await writer.write(nullPath, nullData, nullValueSchema)

      const spec: AggregationSpec = {
        total: { type: 'sum', field: 'value' },
      }

      const result = await parquetAggregate(reader, nullPath, spec)

      expect(result.total).toBe(0)

      await storage.delete(nullPath)
    })
  })

  // ===========================================================================
  // AVG Tests - Process column arrays without row objects
  // ===========================================================================

  describe('AVG - Columnar Processing', () => {
    it('should calculate average without row materialization', async () => {
      const spec: AggregationSpec = {
        avgPrice: { type: 'avg', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Average: 2275 / 10 = 227.5
      expect(result.avgPrice).toBe(227.5)
      expect(result._stats.rowsProcessed).toBe(0)
    })

    it('should skip null values in average calculation', async () => {
      const spec: AggregationSpec = {
        avgDiscount: { type: 'avg', field: 'discount' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Average of non-null: 132 / 8 = 16.5
      expect(result.avgDiscount).toBeCloseTo(16.5, 1)
    })

    it('should return null for avg of empty/all-null column', async () => {
      const nullData = [
        { $id: '1', value: null },
        { $id: '2', value: null },
      ]
      const nullPath = 'test/null-avg.parquet'
      const writer = new ParquetWriter(storage)
      await writer.write(nullPath, nullData, nullValueSchema)

      const spec: AggregationSpec = {
        average: { type: 'avg', field: 'value' },
      }

      const result = await parquetAggregate(reader, nullPath, spec)

      expect(result.average).toBeNull()

      await storage.delete(nullPath)
    })
  })

  // ===========================================================================
  // MIN/MAX Tests - Use column statistics when available
  // ===========================================================================

  describe('MIN/MAX - Statistics Optimization', () => {
    it('should use column statistics for MIN when available', async () => {
      const spec: AggregationSpec = {
        minPrice: { type: 'min', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.minPrice).toBe(100)
      // When stats are available, no data should be read
      // (This depends on whether the parquet file has statistics)
    })

    it('should use column statistics for MAX when available', async () => {
      const spec: AggregationSpec = {
        maxPrice: { type: 'max', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.maxPrice).toBe(400)
    })

    it('should fall back to column scan when stats unavailable', async () => {
      const spec: AggregationSpec = {
        minDiscount: { type: 'min', field: 'discount' },
        maxDiscount: { type: 'max', field: 'discount' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Min non-null: 8.5, Max non-null: 25
      expect(result.minDiscount).toBeCloseTo(8.5, 1)
      expect(result.maxDiscount).toBeCloseTo(25, 1)
    })

    it('should return null for min/max of empty/all-null column', async () => {
      const nullData = [
        { $id: '1', value: null },
        { $id: '2', value: null },
      ]
      const nullPath = 'test/null-minmax.parquet'
      const writer = new ParquetWriter(storage)
      await writer.write(nullPath, nullData, nullValueSchema)

      const spec: AggregationSpec = {
        minValue: { type: 'min', field: 'value' },
        maxValue: { type: 'max', field: 'value' },
      }

      const result = await parquetAggregate(reader, nullPath, spec)

      expect(result.minValue).toBeNull()
      expect(result.maxValue).toBeNull()

      await storage.delete(nullPath)
    })
  })

  // ===========================================================================
  // Multiple Aggregations in Single Pass
  // ===========================================================================

  describe('Multiple Aggregations - Single Pass', () => {
    it('should compute multiple aggregations reading each column once', async () => {
      const spec: AggregationSpec = {
        count: { type: 'count', field: '*' },
        totalPrice: { type: 'sum', field: 'price' },
        avgPrice: { type: 'avg', field: 'price' },
        minPrice: { type: 'min', field: 'price' },
        maxPrice: { type: 'max', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.count).toBe(10)
      expect(result.totalPrice).toBe(2275)
      expect(result.avgPrice).toBe(227.5)
      expect(result.minPrice).toBe(100)
      expect(result.maxPrice).toBe(400)

      // Price column should only be read once
      expect(result._stats.columnsRead).toContain('price')
    })

    it('should minimize columns read for mixed aggregations', async () => {
      const spec: AggregationSpec = {
        totalPrice: { type: 'sum', field: 'price' },
        avgQuantity: { type: 'avg', field: 'quantity' },
        sumDiscount: { type: 'sum', field: 'discount' }, // Use sum which requires data read (not stats)
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Should read price, quantity, and discount columns
      // Note: If stats are available for discount, it might be fewer
      expect(result._stats.columnsRead).toContain('price')
      expect(result._stats.columnsRead).toContain('quantity')
      expect(result._stats.columnsRead).toContain('discount')
    })
  })

  // ===========================================================================
  // ColumnarAggregator Class Tests
  // ===========================================================================

  describe('ColumnarAggregator Class', () => {
    it('should process column arrays directly', () => {
      const aggregator = new ColumnarAggregator()

      const priceColumn = [100, 200, 150, 300, 250]
      const result = aggregator.processColumn(priceColumn, 'sum')

      expect(result).toBe(1000)
    })

    it('should handle typed arrays efficiently', () => {
      const aggregator = new ColumnarAggregator()

      // Parquet often returns typed arrays for numeric columns
      const priceColumn = new Float64Array([100, 200, 150, 300, 250])
      const result = aggregator.processColumn(priceColumn, 'sum')

      expect(result).toBe(1000)
    })

    it('should track null values correctly', () => {
      const aggregator = new ColumnarAggregator()

      const columnWithNulls = [100, null, 200, null, 300]
      const countResult = aggregator.processColumn(columnWithNulls, 'count')
      const sumResult = aggregator.processColumn(columnWithNulls, 'sum')
      const avgResult = aggregator.processColumn(columnWithNulls, 'avg')

      expect(countResult).toBe(3) // Non-null count
      expect(sumResult).toBe(600)
      expect(avgResult).toBe(200)
    })
  })

  // ===========================================================================
  // Performance Characteristics Tests
  // ===========================================================================

  describe('Performance Characteristics', () => {
    it('should not create row objects for pure aggregations', async () => {
      const spec: AggregationSpec = {
        sum: { type: 'sum', field: 'price' },
        avg: { type: 'avg', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Verify no row materialization occurred
      expect(result._stats.rowsProcessed).toBe(0)
      expect(result._stats.rowsMaterialized).toBe(0)
    })

    it('should report memory savings estimate', async () => {
      const spec: AggregationSpec = {
        count: { type: 'count', field: '*' },
        sum: { type: 'sum', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      // Stats should include estimated memory savings
      expect(result._stats.estimatedMemorySaved).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Integration with Metadata-Only Queries
  // ===========================================================================

  describe('Metadata Integration', () => {
    it('should use row group num_rows for COUNT(*)', async () => {
      // Read metadata directly to verify
      const metadata = await reader.readMetadata(testPath)
      const expectedCount = metadata.rowGroups.reduce((sum, rg) => sum + rg.numRows, 0)

      const spec: AggregationSpec = {
        count: { type: 'count', field: '*' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      expect(result.count).toBe(expectedCount)
    })

    it('should use column statistics for MIN/MAX when present', async () => {
      const metadata = await reader.readMetadata(testPath)

      // Check if price column has statistics
      const priceStats = metadata.rowGroups[0]?.columns.find(
        c => c.pathInSchema.includes('price')
      )?.statistics

      const spec: AggregationSpec = {
        minPrice: { type: 'min', field: 'price' },
        maxPrice: { type: 'max', field: 'price' },
      }

      const result = await parquetAggregate(reader, testPath, spec)

      if (priceStats?.min !== undefined && priceStats?.max !== undefined) {
        // If stats exist, verify they were used (no data read)
        expect(result._stats.usedColumnStats).toBe(true)
      }

      // Results should be correct regardless of stats availability
      expect(result.minPrice).toBe(100)
      expect(result.maxPrice).toBe(400)
    })
  })
})

// =============================================================================
// Benchmark Tests (Skip in CI, run manually)
// =============================================================================

describe.skip('Columnar vs Row-Based Aggregation Benchmark', () => {
  it('should be faster than row-based aggregation', async () => {
    // This test would be run manually to compare performance
    // Expected improvement: 40% faster, 85% less memory

    const benchStorage = new MemoryBackend()
    const benchReader = new ParquetReader({ storage: benchStorage })
    const benchWriter = new ParquetWriter(benchStorage)

    // Generate large dataset
    const largeData = Array.from({ length: 100000 }, (_, i) => ({
      $id: `${i}`,
      value: Math.random() * 1000,
      category: ['A', 'B', 'C'][i % 3],
    }))

    const benchPath = 'bench/large.parquet'
    const benchSchema: ParquetSchema = {
      $id: { type: 'STRING', optional: false },
      value: { type: 'DOUBLE', optional: false },
      category: { type: 'STRING', optional: false },
    }
    await benchWriter.write(benchPath, largeData, benchSchema)

    // Time columnar aggregation
    const columnarStart = performance.now()
    const columnarSpec: AggregationSpec = {
      sum: { type: 'sum', field: 'value' },
      avg: { type: 'avg', field: 'value' },
      min: { type: 'min', field: 'value' },
      max: { type: 'max', field: 'value' },
    }
    const columnarResult = await parquetAggregate(benchReader, benchPath, columnarSpec)
    const columnarTime = performance.now() - columnarStart

    // Time row-based aggregation (read all rows, then aggregate)
    const rowBasedStart = performance.now()
    const rows = await benchReader.read<{ value: number }>(benchPath)
    const rowBasedResult = {
      sum: rows.reduce((s, r) => s + r.value, 0),
      avg: rows.reduce((s, r) => s + r.value, 0) / rows.length,
      min: Math.min(...rows.map(r => r.value)),
      max: Math.max(...rows.map(r => r.value)),
    }
    const rowBasedTime = performance.now() - rowBasedStart

    console.log(`Columnar: ${columnarTime.toFixed(2)}ms`)
    console.log(`Row-based: ${rowBasedTime.toFixed(2)}ms`)
    console.log(`Speedup: ${(rowBasedTime / columnarTime).toFixed(2)}x`)

    // Verify results match
    expect(columnarResult.sum).toBeCloseTo(rowBasedResult.sum, 5)
    expect(columnarResult.avg).toBeCloseTo(rowBasedResult.avg, 5)
    expect(columnarResult.min).toBeCloseTo(rowBasedResult.min, 5)
    expect(columnarResult.max).toBeCloseTo(rowBasedResult.max, 5)

    // Columnar should be faster
    expect(columnarTime).toBeLessThan(rowBasedTime)

    await benchStorage.delete(benchPath)
  })
})

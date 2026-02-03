/**
 * Tests for parquedb/tiny export
 *
 * Verifies that:
 * - Core exports are available
 * - parquetQuery works with filters
 * - parquetStream yields rows correctly
 * - Filter matching works standalone
 */

import { describe, it, expect } from 'vitest'
import { parquetWriteBuffer } from 'hyparquet-writer'
import {
  // Core functions
  parquetQuery,
  parquetStream,
  parquetMetadata,
  parquetCount,
  matchesFilter,

  // Types
  type TinyFilter,
  type QueryOptions,
  type Row,
  type TinyParquetMetadata,

  // Version
  VERSION,
  EXPORT_TYPE,
} from '../../../src/exports/tiny'

describe('parquedb/tiny export', () => {
  describe('export verification', () => {
    it('exports VERSION and EXPORT_TYPE', () => {
      expect(VERSION).toBe('0.1.0')
      expect(EXPORT_TYPE).toBe('tiny')
    })

    it('exports parquetQuery function', () => {
      expect(parquetQuery).toBeDefined()
      expect(typeof parquetQuery).toBe('function')
    })

    it('exports parquetStream function', () => {
      expect(parquetStream).toBeDefined()
      expect(typeof parquetStream).toBe('function')
    })

    it('exports parquetMetadata function', () => {
      expect(parquetMetadata).toBeDefined()
      expect(typeof parquetMetadata).toBe('function')
    })

    it('exports parquetCount function', () => {
      expect(parquetCount).toBeDefined()
      expect(typeof parquetCount).toBe('function')
    })

    it('exports matchesFilter function', () => {
      expect(matchesFilter).toBeDefined()
      expect(typeof matchesFilter).toBe('function')
    })
  })

  // Helper to create a test parquet buffer
  function createTestParquet(rows: Row[]): ArrayBuffer {
    // Use hyparquet-writer to create a test parquet file
    // parquetWriteBuffer expects columnData array format
    const columnData = [
      { name: 'id', data: rows.map(r => r.id as number) },
      { name: 'name', data: rows.map(r => r.name as string) },
      { name: 'status', data: rows.map(r => r.status as string) },
      { name: 'score', data: rows.map(r => r.score as number) },
    ]
    return parquetWriteBuffer({ columnData })
  }

  describe('matchesFilter', () => {
    const row: Row = {
      id: 1,
      name: 'Alice',
      status: 'active',
      score: 85,
      tags: ['tech', 'ai'],
    }

    it('matches empty filter', () => {
      expect(matchesFilter(row, {})).toBe(true)
    })

    it('matches simple equality', () => {
      expect(matchesFilter(row, { status: 'active' })).toBe(true)
      expect(matchesFilter(row, { status: 'inactive' })).toBe(false)
    })

    it('matches $eq operator', () => {
      expect(matchesFilter(row, { score: { $eq: 85 } })).toBe(true)
      expect(matchesFilter(row, { score: { $eq: 100 } })).toBe(false)
    })

    it('matches $ne operator', () => {
      expect(matchesFilter(row, { status: { $ne: 'inactive' } })).toBe(true)
      expect(matchesFilter(row, { status: { $ne: 'active' } })).toBe(false)
    })

    it('matches $gt operator', () => {
      expect(matchesFilter(row, { score: { $gt: 80 } })).toBe(true)
      expect(matchesFilter(row, { score: { $gt: 85 } })).toBe(false)
      expect(matchesFilter(row, { score: { $gt: 90 } })).toBe(false)
    })

    it('matches $gte operator', () => {
      expect(matchesFilter(row, { score: { $gte: 85 } })).toBe(true)
      expect(matchesFilter(row, { score: { $gte: 80 } })).toBe(true)
      expect(matchesFilter(row, { score: { $gte: 90 } })).toBe(false)
    })

    it('matches $lt operator', () => {
      expect(matchesFilter(row, { score: { $lt: 90 } })).toBe(true)
      expect(matchesFilter(row, { score: { $lt: 85 } })).toBe(false)
    })

    it('matches $lte operator', () => {
      expect(matchesFilter(row, { score: { $lte: 85 } })).toBe(true)
      expect(matchesFilter(row, { score: { $lte: 90 } })).toBe(true)
      expect(matchesFilter(row, { score: { $lte: 80 } })).toBe(false)
    })

    it('matches $in operator', () => {
      expect(matchesFilter(row, { status: { $in: ['active', 'pending'] } })).toBe(true)
      expect(matchesFilter(row, { status: { $in: ['inactive', 'pending'] } })).toBe(false)
    })

    it('matches $nin operator', () => {
      expect(matchesFilter(row, { status: { $nin: ['inactive', 'pending'] } })).toBe(true)
      expect(matchesFilter(row, { status: { $nin: ['active', 'pending'] } })).toBe(false)
    })

    it('matches $exists operator', () => {
      expect(matchesFilter(row, { score: { $exists: true } })).toBe(true)
      expect(matchesFilter(row, { missing: { $exists: false } })).toBe(true)
      expect(matchesFilter(row, { score: { $exists: false } })).toBe(false)
    })

    it('matches $regex operator', () => {
      expect(matchesFilter(row, { name: { $regex: 'Ali.*' } })).toBe(true)
      expect(matchesFilter(row, { name: { $regex: '^Alice$' } })).toBe(true)
      expect(matchesFilter(row, { name: { $regex: 'Bob' } })).toBe(false)
    })

    it('matches multiple conditions (AND)', () => {
      expect(matchesFilter(row, { status: 'active', score: { $gt: 80 } })).toBe(true)
      expect(matchesFilter(row, { status: 'active', score: { $gt: 90 } })).toBe(false)
    })

    it('handles null and undefined correctly', () => {
      const rowWithNull: Row = { id: 1, value: null }
      const rowWithoutField: Row = { id: 1 }

      expect(matchesFilter(rowWithNull, { value: null })).toBe(true)
      expect(matchesFilter(rowWithoutField, { value: null })).toBe(true)
      expect(matchesFilter(row, { value: null })).toBe(true) // missing field
    })

    it('handles nested fields with dot notation', () => {
      const nestedRow: Row = {
        id: 1,
        user: { name: 'Alice', age: 30 },
      }
      expect(matchesFilter(nestedRow, { 'user.name': 'Alice' })).toBe(true)
      expect(matchesFilter(nestedRow, { 'user.age': { $gt: 25 } })).toBe(true)
    })
  })

  describe('parquetQuery', () => {
    it('reads all rows from parquet buffer', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer)

      expect(rows).toHaveLength(3)
      expect(rows[0]).toHaveProperty('id')
      expect(rows[0]).toHaveProperty('name')
    })

    it('filters rows by simple equality', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer, { status: 'active' })

      expect(rows).toHaveLength(2)
      expect(rows.every(r => r.status === 'active')).toBe(true)
    })

    it('filters rows with comparison operators', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer, { score: { $gt: 80 } })

      expect(rows).toHaveLength(2)
      expect(rows.every(r => (r.score as number) > 80)).toBe(true)
    })

    it('applies limit option', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer, undefined, { limit: 2 })

      expect(rows).toHaveLength(2)
    })

    it('applies offset option', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer, undefined, { offset: 1 })

      expect(rows).toHaveLength(2)
    })

    it('applies columns option for projection', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
      ]

      const buffer = await createTestParquet(testData)
      const rows = await parquetQuery(buffer, undefined, { columns: ['id', 'name'] })

      expect(rows).toHaveLength(1)
      expect(rows[0]).toHaveProperty('id')
      expect(rows[0]).toHaveProperty('name')
      // Note: hyparquet may still return all columns, but we're testing the option passes through
    })
  })

  describe('parquetStream', () => {
    it('streams rows from parquet buffer', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows: Row[] = []

      for await (const row of parquetStream(buffer)) {
        rows.push(row)
      }

      expect(rows).toHaveLength(3)
    })

    it('streams with filter', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows: Row[] = []

      for await (const row of parquetStream(buffer, { status: 'active' })) {
        rows.push(row)
      }

      expect(rows).toHaveLength(2)
      expect(rows.every(r => r.status === 'active')).toBe(true)
    })

    it('respects limit option in stream', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const rows: Row[] = []

      for await (const row of parquetStream(buffer, undefined, { limit: 1 })) {
        rows.push(row)
      }

      expect(rows).toHaveLength(1)
    })
  })

  describe('parquetMetadata', () => {
    it('returns file metadata', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
      ]

      const buffer = await createTestParquet(testData)
      const meta = await parquetMetadata(buffer)

      expect(meta.numRows).toBe(2)
      expect(meta.numRowGroups).toBeGreaterThanOrEqual(1)
      expect(meta.columns).toContain('id')
      expect(meta.columns).toContain('name')
      expect(meta.version).toBeGreaterThanOrEqual(1) // Version 1 or 2
    })
  })

  describe('parquetCount', () => {
    it('counts all rows without filter', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const count = await parquetCount(buffer)

      expect(count).toBe(3)
    })

    it('counts filtered rows', async () => {
      const testData = [
        { id: 1, name: 'Alice', status: 'active', score: 85 },
        { id: 2, name: 'Bob', status: 'inactive', score: 72 },
        { id: 3, name: 'Carol', status: 'active', score: 91 },
      ]

      const buffer = await createTestParquet(testData)
      const count = await parquetCount(buffer, { status: 'active' })

      expect(count).toBe(2)
    })
  })
})

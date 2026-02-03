/**
 * Tests for Iceberg Predicate Pushdown
 *
 * Tests the predicate pushdown utilities for IcebergBackend including:
 * - Filter to predicates extraction
 * - Row group statistics filtering
 * - Projection pushdown
 * - Limit pushdown
 */

import { describe, it, expect } from 'vitest'
import {
  extractPredicatesFromFilter,
  canSkipRowGroup,
  canSkipManifest,
  canSkipManifestEntry,
  getRequiredColumns,
  calculateReadLimit,
  hasEnoughForLimit,
  createPushdownStats,
  ENTITY_CORE_COLUMNS,
  type StatisticsPredicate,
} from '../../../src/backends/iceberg-pushdown'
import type { Filter } from '../../../src/types/filter'
import type { RowGroupMetadata } from '../../../src/parquet/types'
import type { ManifestEntry, ManifestFile } from '@dotdo/iceberg'

// =============================================================================
// extractPredicatesFromFilter Tests
// =============================================================================

describe('extractPredicatesFromFilter', () => {
  describe('comparison operators', () => {
    it('extracts equality predicate from direct value', () => {
      const filter: Filter = { status: 'active' }
      const result = extractPredicatesFromFilter(filter)

      expect(result.hasPushdown).toBe(true)
      expect(result.predicates).toHaveLength(1)
      expect(result.predicates[0]).toEqual({
        column: 'status',
        op: 'eq',
        value: 'active',
      })
    })

    it('extracts $eq operator', () => {
      const filter: Filter = { age: { $eq: 25 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'age',
        op: 'eq',
        value: 25,
      })
    })

    it('extracts $ne operator', () => {
      const filter: Filter = { status: { $ne: 'deleted' } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'status',
        op: 'ne',
        value: 'deleted',
      })
    })

    it('extracts $gt operator', () => {
      const filter: Filter = { score: { $gt: 100 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'score',
        op: 'gt',
        value: 100,
      })
    })

    it('extracts $gte operator', () => {
      const filter: Filter = { age: { $gte: 18 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'age',
        op: 'gte',
        value: 18,
      })
    })

    it('extracts $lt operator', () => {
      const filter: Filter = { price: { $lt: 1000 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'price',
        op: 'lt',
        value: 1000,
      })
    })

    it('extracts $lte operator', () => {
      const filter: Filter = { rating: { $lte: 5.0 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'rating',
        op: 'lte',
        value: 5.0,
      })
    })

    it('extracts $in operator', () => {
      const filter: Filter = { category: { $in: ['A', 'B', 'C'] } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'category',
        op: 'in',
        value: ['A', 'B', 'C'],
      })
    })

    it('handles multiple operators on same column', () => {
      const filter: Filter = { age: { $gte: 18, $lt: 65 } }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(2)
      expect(result.predicates).toContainEqual({ column: 'age', op: 'gte', value: 18 })
      expect(result.predicates).toContainEqual({ column: 'age', op: 'lt', value: 65 })
    })

    it('handles Date values', () => {
      const date = new Date('2024-01-15')
      const filter: Filter = { createdAt: date }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toContainEqual({
        column: 'createdAt',
        op: 'eq',
        value: date,
      })
    })
  })

  describe('logical operators', () => {
    it('extracts predicates from $and conditions', () => {
      const filter: Filter = {
        $and: [
          { status: 'published' },
          { views: { $gte: 100 } },
        ],
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(2)
      expect(result.predicates).toContainEqual({ column: 'status', op: 'eq', value: 'published' })
      expect(result.predicates).toContainEqual({ column: 'views', op: 'gte', value: 100 })
    })

    it('handles nested $and', () => {
      const filter: Filter = {
        $and: [
          { $and: [{ a: 1 }, { b: 2 }] },
          { c: 3 },
        ],
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(3)
    })

    it('skips $or operator (requires row-level evaluation)', () => {
      const filter: Filter = {
        $or: [{ status: 'published' }, { featured: true }],
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips $not operator', () => {
      const filter: Filter = {
        $not: { status: 'draft' },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips $nor operator', () => {
      const filter: Filter = {
        $nor: [{ a: 1 }, { b: 2 }],
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })
  })

  describe('skipped conditions', () => {
    it('skips $text operator', () => {
      const filter: Filter = {
        $text: { $search: 'hello world' },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips $vector operator', () => {
      const filter: Filter = {
        $vector: { query: [1, 2, 3], field: 'embedding', topK: 10 },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips nested fields (dot notation)', () => {
      const filter: Filter = {
        'metadata.readTime': { $gt: 5 },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips null values', () => {
      const filter: Filter = {
        email: null,
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })

    it('skips $regex operator', () => {
      const filter: Filter = {
        name: { $regex: '^John' },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicates).toHaveLength(0)
    })
  })

  describe('predicate columns tracking', () => {
    it('tracks unique columns involved in predicates', () => {
      const filter: Filter = {
        age: { $gte: 18, $lt: 65 },
        status: 'active',
        score: { $gt: 100 },
      }
      const result = extractPredicatesFromFilter(filter)

      expect(result.predicateColumns).toContain('age')
      expect(result.predicateColumns).toContain('status')
      expect(result.predicateColumns).toContain('score')
      expect(result.predicateColumns).toHaveLength(3)
    })
  })

  describe('empty/invalid filters', () => {
    it('returns empty for empty filter', () => {
      const result = extractPredicatesFromFilter({})
      expect(result.hasPushdown).toBe(false)
      expect(result.predicates).toHaveLength(0)
    })

    it('returns empty for null filter', () => {
      const result = extractPredicatesFromFilter(null as unknown as Filter)
      expect(result.hasPushdown).toBe(false)
    })

    it('returns empty for undefined filter', () => {
      const result = extractPredicatesFromFilter(undefined)
      expect(result.hasPushdown).toBe(false)
    })
  })
})

// =============================================================================
// canSkipRowGroup Tests
// =============================================================================

describe('canSkipRowGroup', () => {
  function createRowGroup(columnStats: Record<string, { min: unknown; max: unknown }>): RowGroupMetadata {
    return {
      numRows: 1000,
      totalByteSize: 10000,
      columns: Object.entries(columnStats).map(([name, stats]) => ({
        pathInSchema: [name],
        totalCompressedSize: 1000,
        totalUncompressedSize: 1000,
        numValues: 1000,
        encodings: ['PLAIN' as const],
        codec: 'SNAPPY' as const,
        statistics: {
          min: stats.min,
          max: stats.max,
        },
      })),
    }
  }

  describe('equality predicates', () => {
    it('skips row group when value is below min', () => {
      const rowGroup = createRowGroup({ age: { min: 20, max: 50 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 10 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('skips row group when value is above max', () => {
      const rowGroup = createRowGroup({ age: { min: 20, max: 50 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 60 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip when value is within range', () => {
      const rowGroup = createRowGroup({ age: { min: 20, max: 50 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 35 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('does not skip when value equals min', () => {
      const rowGroup = createRowGroup({ age: { min: 20, max: 50 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 20 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('does not skip when value equals max', () => {
      const rowGroup = createRowGroup({ age: { min: 20, max: 50 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 50 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })
  })

  describe('comparison predicates', () => {
    it('skips for $gt when max <= value', () => {
      const rowGroup = createRowGroup({ score: { min: 10, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'gt', value: 100 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip for $gt when max > value', () => {
      const rowGroup = createRowGroup({ score: { min: 10, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'gt', value: 99 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('skips for $gte when max < value', () => {
      const rowGroup = createRowGroup({ score: { min: 10, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'gte', value: 101 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip for $gte when max >= value', () => {
      const rowGroup = createRowGroup({ score: { min: 10, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'gte', value: 100 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('skips for $lt when min >= value', () => {
      const rowGroup = createRowGroup({ score: { min: 50, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'lt', value: 50 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip for $lt when min < value', () => {
      const rowGroup = createRowGroup({ score: { min: 50, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'lt', value: 51 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('skips for $lte when min > value', () => {
      const rowGroup = createRowGroup({ score: { min: 50, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'lte', value: 49 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip for $lte when min <= value', () => {
      const rowGroup = createRowGroup({ score: { min: 50, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'score', op: 'lte', value: 50 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })
  })

  describe('$ne predicate', () => {
    it('skips when min === max === excluded value', () => {
      const rowGroup = createRowGroup({ status: { min: 'deleted', max: 'deleted' } })
      const predicates: StatisticsPredicate[] = [{ column: 'status', op: 'ne', value: 'deleted' }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip when range contains other values', () => {
      const rowGroup = createRowGroup({ status: { min: 'active', max: 'published' } })
      const predicates: StatisticsPredicate[] = [{ column: 'status', op: 'ne', value: 'draft' }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })
  })

  describe('$in predicate', () => {
    it('skips when all values in set are outside range', () => {
      const rowGroup = createRowGroup({ category: { min: 'M', max: 'Z' } })
      const predicates: StatisticsPredicate[] = [
        { column: 'category', op: 'in', value: ['A', 'B', 'C'] },
      ]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })

    it('does not skip when any value could be in range', () => {
      const rowGroup = createRowGroup({ category: { min: 'A', max: 'M' } })
      const predicates: StatisticsPredicate[] = [
        { column: 'category', op: 'in', value: ['K', 'L', 'Z'] },
      ]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })
  })

  describe('string comparisons', () => {
    it('correctly compares string values', () => {
      const rowGroup = createRowGroup({ name: { min: 'Alice', max: 'Charlie' } })

      // Bob is in range
      expect(canSkipRowGroup(rowGroup, [{ column: 'name', op: 'eq', value: 'Bob' }])).toBe(false)

      // Zoe is above range
      expect(canSkipRowGroup(rowGroup, [{ column: 'name', op: 'eq', value: 'Zoe' }])).toBe(true)
    })
  })

  describe('missing statistics', () => {
    it('does not skip when column has no statistics', () => {
      const rowGroup: RowGroupMetadata = {
        numRows: 1000,
        totalByteSize: 10000,
        columns: [{
          pathInSchema: ['age'],
          totalCompressedSize: 1000,
          totalUncompressedSize: 1000,
          numValues: 1000,
          encodings: ['PLAIN'],
          codec: 'SNAPPY',
          // No statistics
        }],
      }
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 25 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })

    it('does not skip when column not found', () => {
      const rowGroup = createRowGroup({ other: { min: 1, max: 100 } })
      const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 25 }]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(false)
    })
  })

  describe('multiple predicates', () => {
    it('skips if ANY predicate indicates skip', () => {
      const rowGroup = createRowGroup({
        age: { min: 20, max: 50 },
        score: { min: 0, max: 100 },
      })
      const predicates: StatisticsPredicate[] = [
        { column: 'age', op: 'eq', value: 35 }, // In range - would not skip
        { column: 'score', op: 'gt', value: 200 }, // Above max - would skip
      ]

      expect(canSkipRowGroup(rowGroup, predicates)).toBe(true)
    })
  })
})

// =============================================================================
// Projection Pushdown Tests
// =============================================================================

describe('getRequiredColumns', () => {
  it('returns core entity columns when no filter', () => {
    const columns = getRequiredColumns()

    expect(columns).toEqual(expect.arrayContaining([...ENTITY_CORE_COLUMNS]))
  })

  it('includes filter columns when in shredColumns', () => {
    const filter: Filter = { status: 'active', score: { $gt: 100 } }
    const shredColumns = new Set(['status', 'score'])

    const columns = getRequiredColumns(filter, undefined, shredColumns)

    expect(columns).toContain('status')
    expect(columns).toContain('score')
  })

  it('excludes filter columns not in shredColumns', () => {
    const filter: Filter = { customField: 'value' }
    const shredColumns = new Set(['status'])

    const columns = getRequiredColumns(filter, undefined, shredColumns)

    expect(columns).not.toContain('customField')
  })

  it('includes projection columns when in shredColumns', () => {
    const options = { project: { status: 1, name: 1 } }
    const shredColumns = new Set(['status'])

    const columns = getRequiredColumns(undefined, options, shredColumns)

    expect(columns).toContain('status')
  })
})

// =============================================================================
// Limit Pushdown Tests
// =============================================================================

describe('calculateReadLimit', () => {
  it('returns undefined when no limit specified', () => {
    expect(calculateReadLimit()).toBeUndefined()
    expect(calculateReadLimit({})).toBeUndefined()
  })

  it('calculates limit with buffer for deduplication', () => {
    const limit = calculateReadLimit({ limit: 10 })

    // Should be at least 10 (the limit) plus some buffer
    expect(limit).toBeGreaterThanOrEqual(10)
    expect(limit).toBeLessThan(20) // Reasonable upper bound
  })

  it('includes skip in calculation', () => {
    const limitWithSkip = calculateReadLimit({ skip: 50, limit: 10 })
    const limitWithoutSkip = calculateReadLimit({ limit: 10 })

    expect(limitWithSkip).toBeGreaterThan(limitWithoutSkip!)
  })
})

describe('hasEnoughForLimit', () => {
  it('returns false when no limit specified', () => {
    expect(hasEnoughForLimit(100)).toBe(false)
    expect(hasEnoughForLimit(100, {})).toBe(false)
  })

  it('returns true when collected enough entities', () => {
    // With limit=10 and some buffer, we should have enough at ~15
    expect(hasEnoughForLimit(15, { limit: 10 })).toBe(true)
  })

  it('returns false when sort is specified (need all entities)', () => {
    expect(hasEnoughForLimit(1000, { limit: 10, sort: { createdAt: -1 } })).toBe(false)
  })

  it('returns false when not enough entities', () => {
    expect(hasEnoughForLimit(5, { limit: 10 })).toBe(false)
  })
})

// =============================================================================
// Statistics Tracking Tests
// =============================================================================

describe('createPushdownStats', () => {
  it('creates stats object with zero values', () => {
    const stats = createPushdownStats()

    expect(stats).toEqual({
      totalManifests: 0,
      skippedManifests: 0,
      totalDataFiles: 0,
      skippedDataFiles: 0,
      totalRowGroups: 0,
      skippedRowGroups: 0,
      projectedColumns: 0,
      limitPushdown: false,
    })
  })
})

// =============================================================================
// Manifest/Entry Pushdown Tests
// =============================================================================

describe('canSkipManifest', () => {
  it('returns false for unpartitioned tables', () => {
    const manifest: ManifestFile = {
      'manifest-path': '/path/to/manifest.avro',
      'manifest-length': 1000,
      'partition-spec-id': 0,
      content: 0,
      'sequence-number': 1,
      'min-sequence-number': 1,
      'added-snapshot-id': 12345,
    }
    const predicates: StatisticsPredicate[] = [{ column: 'age', op: 'eq', value: 25 }]

    // Currently always returns false for unpartitioned tables
    expect(canSkipManifest(manifest, predicates)).toBe(false)
  })
})

describe('canSkipManifestEntry', () => {
  // Helper to create UTF-8 encoded string bounds
  function encodeStringBound(value: string): Uint8Array {
    return new TextEncoder().encode(value)
  }

  // Helper to create 4-byte little-endian integer bounds
  function encodeIntBound(value: number): Uint8Array {
    const buffer = new ArrayBuffer(4)
    const view = new DataView(buffer)
    view.setInt32(0, value, true)
    return new Uint8Array(buffer)
  }

  it('returns false when no bounds are available', () => {
    const entry: ManifestEntry = {
      status: 0,
      'snapshot-id': 12345,
      'sequence-number': 1,
      'file-sequence-number': 1,
      'data-file': {
        content: 0,
        'file-path': '/path/to/data.parquet',
        'file-format': 'PARQUET',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 10000,
      },
    }
    const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'eq', value: 'Alice' }]

    expect(canSkipManifestEntry(entry, predicates)).toBe(false)
  })

  it('returns false for unknown columns', () => {
    const entry: ManifestEntry = {
      status: 0,
      'snapshot-id': 12345,
      'sequence-number': 1,
      'file-sequence-number': 1,
      'data-file': {
        content: 0,
        'file-path': '/path/to/data.parquet',
        'file-format': 'PARQUET',
        partition: {},
        'record-count': 1000,
        'file-size-in-bytes': 10000,
        'lower-bounds': { 1: encodeStringBound('aaa') },
        'upper-bounds': { 1: encodeStringBound('zzz') },
      },
    }
    const predicates: StatisticsPredicate[] = [{ column: 'unknownField', op: 'eq', value: 'test' }]

    expect(canSkipManifestEntry(entry, predicates)).toBe(false)
  })

  describe('with string bounds (e.g., $id, name, $type)', () => {
    function createEntryWithStringBounds(fieldId: number, min: string, max: string): ManifestEntry {
      return {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          'lower-bounds': { [fieldId]: encodeStringBound(min) },
          'upper-bounds': { [fieldId]: encodeStringBound(max) },
        },
      }
    }

    it('skips when equality value is below range (name field)', () => {
      // name has field ID 3, bounds are ['M', 'Z']
      const entry = createEntryWithStringBounds(3, 'M', 'Z')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'eq', value: 'Alice' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('skips when equality value is above range (name field)', () => {
      const entry = createEntryWithStringBounds(3, 'A', 'M')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'eq', value: 'Zoe' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip when equality value is within range', () => {
      const entry = createEntryWithStringBounds(3, 'A', 'Z')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'eq', value: 'John' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })

    it('skips for $gt when max <= value', () => {
      const entry = createEntryWithStringBounds(3, 'A', 'M')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'gt', value: 'M' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip for $gt when max > value', () => {
      const entry = createEntryWithStringBounds(3, 'A', 'Z')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'gt', value: 'M' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })

    it('skips for $lt when min >= value', () => {
      const entry = createEntryWithStringBounds(3, 'M', 'Z')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'lt', value: 'M' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('skips for $in when all values are outside range', () => {
      const entry = createEntryWithStringBounds(3, 'M', 'Z')
      const predicates: StatisticsPredicate[] = [
        { column: 'name', op: 'in', value: ['Alice', 'Bob', 'Charlie'] },
      ]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip for $in when any value could be in range', () => {
      const entry = createEntryWithStringBounds(3, 'M', 'Z')
      const predicates: StatisticsPredicate[] = [
        { column: 'name', op: 'in', value: ['Alice', 'Nancy', 'Zoe'] },
      ]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })
  })

  describe('with integer bounds (version field)', () => {
    function createEntryWithIntBounds(min: number, max: number): ManifestEntry {
      return {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          'lower-bounds': { 10: encodeIntBound(min) }, // version has field ID 10
          'upper-bounds': { 10: encodeIntBound(max) },
        },
      }
    }

    it('skips when equality value is below range', () => {
      const entry = createEntryWithIntBounds(5, 10)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'eq', value: 2 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('skips when equality value is above range', () => {
      const entry = createEntryWithIntBounds(1, 5)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'eq', value: 10 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip when equality value is within range', () => {
      const entry = createEntryWithIntBounds(1, 10)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'eq', value: 5 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })

    it('skips for $gte when max < value', () => {
      const entry = createEntryWithIntBounds(1, 5)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'gte', value: 10 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip for $gte when max >= value', () => {
      const entry = createEntryWithIntBounds(1, 10)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'gte', value: 10 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })

    it('skips for $lte when min > value', () => {
      const entry = createEntryWithIntBounds(10, 20)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'lte', value: 5 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip for $lte when min <= value', () => {
      const entry = createEntryWithIntBounds(5, 20)
      const predicates: StatisticsPredicate[] = [{ column: 'version', op: 'lte', value: 5 }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })
  })

  describe('with $id field (entity ID filtering)', () => {
    function createEntryWithIdBounds(min: string, max: string): ManifestEntry {
      return {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          'lower-bounds': { 1: encodeStringBound(min) }, // $id has field ID 1
          'upper-bounds': { 1: encodeStringBound(max) },
        },
      }
    }

    it('skips when $id value is below range', () => {
      const entry = createEntryWithIdBounds('users/m', 'users/z')
      const predicates: StatisticsPredicate[] = [{ column: '$id', op: 'eq', value: 'users/alice' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip when $id could be in range', () => {
      const entry = createEntryWithIdBounds('users/a', 'users/z')
      const predicates: StatisticsPredicate[] = [{ column: '$id', op: 'eq', value: 'users/john' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })
  })

  describe('with $ne predicate', () => {
    function createEntryWithNameBounds(min: string, max: string): ManifestEntry {
      return {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          'lower-bounds': { 3: encodeStringBound(min) },
          'upper-bounds': { 3: encodeStringBound(max) },
        },
      }
    }

    it('skips when min === max === excluded value', () => {
      const entry = createEntryWithNameBounds('Alice', 'Alice')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'ne', value: 'Alice' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })

    it('does not skip when range contains other values', () => {
      const entry = createEntryWithNameBounds('Alice', 'Bob')
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'ne', value: 'Alice' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(false)
    })
  })

  describe('multiple predicates', () => {
    it('skips if ANY predicate indicates skip', () => {
      const entry: ManifestEntry = {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          'lower-bounds': {
            3: encodeStringBound('A'),  // name
            10: encodeIntBound(1),       // version
          },
          'upper-bounds': {
            3: encodeStringBound('M'),  // name
            10: encodeIntBound(5),       // version
          },
        },
      }
      const predicates: StatisticsPredicate[] = [
        { column: 'name', op: 'eq', value: 'John' },      // In range - would not skip
        { column: 'version', op: 'gt', value: 10 },       // Above max - would skip
      ]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })
  })

  describe('with pre-decoded string bounds', () => {
    it('handles already decoded string bounds', () => {
      const entry: ManifestEntry = {
        status: 0,
        'snapshot-id': 12345,
        'sequence-number': 1,
        'file-sequence-number': 1,
        'data-file': {
          content: 0,
          'file-path': '/path/to/data.parquet',
          'file-format': 'PARQUET',
          partition: {},
          'record-count': 1000,
          'file-size-in-bytes': 10000,
          // Bounds already decoded as strings (some Avro decoders do this)
          'lower-bounds': { 3: 'M' },
          'upper-bounds': { 3: 'Z' },
        },
      }
      const predicates: StatisticsPredicate[] = [{ column: 'name', op: 'eq', value: 'Alice' }]

      expect(canSkipManifestEntry(entry, predicates)).toBe(true)
    })
  })
})

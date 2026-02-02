/**
 * QueryExecutor Tests
 *
 * Tests for the QueryExecutor component that handles query planning,
 * index selection, predicate pushdown, and result processing.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { Filter, FieldFilter, FieldOperator } from '@/types/filter'
import type { FindOptions, SortSpec, Projection } from '@/types/options'
import { createSafeRegex } from '@/utils/safe-regex'

// =============================================================================
// Mock Types (matching QueryExecutor internals)
// =============================================================================

interface RowGroupMetadata {
  index: number
  numRows: number
  offset: number
  compressedSize: number
  totalSize: number
  columnStats: Record<string, ColumnStats>
}

interface ColumnStats {
  name: string
  min?: unknown
  max?: unknown
  nullCount: number
  distinctCount?: number
  hasStats: boolean
}

interface ParquetMetadata {
  numRows: number
  rowGroups: RowGroupMetadata[]
  columns: Array<{ name: string; physicalType: string; logicalType?: string; encoding: string; compression: string }>
  schema: { fields: Array<{ name: string; type: string; nullable: boolean }> }
  keyValueMetadata?: Record<string, string>
}

interface QueryStats {
  rowGroupsScanned: number
  rowGroupsSkipped: number
  rowsScanned: number
  rowsReturned: number
  bytesRead: number
  executionTimeMs: number
  usedBloomFilter: boolean
  cacheHit: boolean
}

// =============================================================================
// Helper Functions (extracted from QueryExecutor for unit testing)
// =============================================================================

/**
 * Check if a row group might contain matching rows based on column statistics
 */
function rowGroupMightMatch(rowGroup: RowGroupMetadata, filter: Filter): boolean {
  for (const [field, condition] of Object.entries(filter)) {
    if (field.startsWith('$')) continue

    const stats = rowGroup.columnStats[field]
    if (!stats || !stats.hasStats) continue

    if (!conditionMightMatch(stats, condition)) {
      return false
    }
  }

  return true
}

/**
 * Check if a condition might match given column statistics
 */
function conditionMightMatch(stats: ColumnStats, condition: FieldFilter): boolean {
  // Handle direct equality
  if (typeof condition !== 'object' || condition === null) {
    if (stats.min !== undefined && stats.min !== null && stats.max !== undefined && stats.max !== null) {
      const minNum = stats.min as number
      const maxNum = stats.max as number
      const condNum = condition as number
      return condNum >= minNum && condNum <= maxNum
    }
    return true
  }

  const operator = condition as FieldOperator

  // Handle comparison operators
  if ('$eq' in operator) {
    if (stats.min !== undefined && stats.min !== null && stats.max !== undefined && stats.max !== null) {
      const eqVal = operator.$eq as number
      const minNum = stats.min as number
      const maxNum = stats.max as number
      return eqVal >= minNum && eqVal <= maxNum
    }
  }

  if ('$gt' in operator) {
    if (stats.max !== undefined && stats.max !== null) {
      const gtVal = operator.$gt as number
      const maxNum = stats.max as number
      return maxNum > gtVal
    }
  }

  if ('$gte' in operator) {
    if (stats.max !== undefined && stats.max !== null) {
      const gteVal = operator.$gte as number
      const maxNum = stats.max as number
      return maxNum >= gteVal
    }
  }

  if ('$lt' in operator) {
    if (stats.min !== undefined && stats.min !== null) {
      const ltVal = operator.$lt as number
      const minNum = stats.min as number
      return minNum < ltVal
    }
  }

  if ('$lte' in operator) {
    if (stats.min !== undefined && stats.min !== null) {
      const lteVal = operator.$lte as number
      const minNum = stats.min as number
      return minNum <= lteVal
    }
  }

  if ('$in' in operator) {
    const values = operator.$in as number[]
    if (stats.min !== undefined && stats.min !== null && stats.max !== undefined && stats.max !== null) {
      const minNum = stats.min as number
      const maxNum = stats.max as number
      return values.some((v) => v >= minNum && v <= maxNum)
    }
  }

  return true
}

/**
 * Select row groups that might contain matching rows
 */
function selectRowGroups(metadata: ParquetMetadata, filter: Filter): RowGroupMetadata[] {
  return metadata.rowGroups.filter((rg) => rowGroupMightMatch(rg, filter))
}

/**
 * Check if bloom filter can be used for this filter
 */
function canUseBloomFilter(filter: Filter): boolean {
  return (
    'id' in filter &&
    (typeof filter.id === 'string' || (typeof filter.id === 'object' && filter.id !== null && '$eq' in filter.id))
  )
}

/**
 * Extract $id filter for predicate pushdown
 */
function extractIdFilter(filter: Filter): Filter | null {
  if ('$id' in filter) {
    return { $id: filter.$id }
  }

  if (filter.$or) {
    const idConditions = filter.$or.filter((f) => '$id' in f)
    if (idConditions.length > 0) {
      return { $or: idConditions }
    }
  }

  if (filter.$and) {
    const idCondition = filter.$and.find((f) => '$id' in f)
    if (idCondition) {
      return idCondition
    }
  }

  return null
}

/**
 * Remove $id filter from original filter
 */
function removeIdFilter(filter: Filter): Filter {
  const result: Filter = {}

  for (const [key, value] of Object.entries(filter)) {
    if (key === '$id') continue
    if (key === '$or' && Array.isArray(value)) {
      const nonIdConditions = value.filter((f) => !('$id' in f))
      if (nonIdConditions.length > 0) {
        result.$or = nonIdConditions
      }
    } else if (key === '$and' && Array.isArray(value)) {
      const nonIdConditions = value.filter((f) => !('$id' in f))
      if (nonIdConditions.length > 0) {
        result.$and = nonIdConditions
      }
    } else {
      (result as Record<string, unknown>)[key] = value
    }
  }

  return result
}

/**
 * Check if a record matches a filter
 */
function matchesFilter(record: unknown, filter: Filter): boolean {
  const rec = record as Record<string, unknown>

  if (filter.$and) {
    return filter.$and.every((f) => matchesFilter(record, f))
  }

  if (filter.$or) {
    return filter.$or.some((f) => matchesFilter(record, f))
  }

  if (filter.$not) {
    return !matchesFilter(record, filter.$not)
  }

  if (filter.$nor) {
    return !filter.$nor.some((f) => matchesFilter(record, f))
  }

  const logicalOperators = new Set(['$and', '$or', '$not', '$nor'])
  for (const [field, condition] of Object.entries(filter)) {
    if (logicalOperators.has(field)) continue

    const value = rec[field]
    if (!matchesCondition(value, condition)) {
      return false
    }
  }

  return true
}

/**
 * Check if a value matches a condition
 */
function matchesCondition(value: unknown, condition: FieldFilter): boolean {
  if (typeof condition !== 'object' || condition === null) {
    return value === condition
  }

  const operator = condition as FieldOperator

  if ('$eq' in operator) return value === operator.$eq
  if ('$ne' in operator) return value !== operator.$ne
  if ('$gt' in operator) return (value as number) > (operator.$gt as number)
  if ('$gte' in operator) return (value as number) >= (operator.$gte as number)
  if ('$lt' in operator) return (value as number) < (operator.$lt as number)
  if ('$lte' in operator) return (value as number) <= (operator.$lte as number)
  if ('$in' in operator) return (operator.$in as unknown[]).includes(value)
  if ('$nin' in operator) return !(operator.$nin as unknown[]).includes(value)

  if ('$regex' in operator) {
    const regex = createSafeRegex(operator.$regex as string, operator.$options)
    return regex.test(value as string)
  }
  if ('$startsWith' in operator) {
    return (value as string).startsWith(operator.$startsWith)
  }
  if ('$endsWith' in operator) {
    return (value as string).endsWith(operator.$endsWith)
  }
  if ('$contains' in operator) {
    return (value as string).includes(operator.$contains)
  }

  if ('$all' in operator) {
    const arr = value as unknown[]
    return (operator.$all as unknown[]).every((v) => arr.includes(v))
  }
  if ('$size' in operator) {
    return (value as unknown[]).length === operator.$size
  }

  if ('$exists' in operator) {
    return operator.$exists ? value !== undefined : value === undefined
  }

  return true
}

/**
 * Apply sort to results
 */
function applySort<T>(results: T[], sort: SortSpec): T[] {
  return results.sort((a, b) => {
    for (const [field, direction] of Object.entries(sort)) {
      const aVal = (a as Record<string, unknown>)[field] as number | string | null | undefined
      const bVal = (b as Record<string, unknown>)[field] as number | string | null | undefined

      if (aVal === bVal) continue

      if (aVal == null && bVal == null) continue
      if (aVal == null) return 1
      if (bVal == null) return -1

      const cmp = aVal < bVal ? -1 : 1
      const dir = direction === 'asc' || direction === 1 ? 1 : -1

      return cmp * dir
    }
    return 0
  })
}

/**
 * Apply projection to results
 */
function applyProjection<T>(results: T[], projection: Projection): T[] {
  const includeFields = Object.entries(projection)
    .filter(([, v]) => v === 1 || v === true)
    .map(([k]) => k)

  const excludeFields = Object.entries(projection)
    .filter(([, v]) => v === 0 || v === false)
    .map(([k]) => k)

  return results.map((item) => {
    const rec = item as Record<string, unknown>

    if (includeFields.length > 0) {
      const projected: Record<string, unknown> = {}
      for (const field of includeFields) {
        if (field in rec) {
          projected[field] = rec[field]
        }
      }
      return projected as T
    } else if (excludeFields.length > 0) {
      const projected = { ...rec }
      for (const field of excludeFields) {
        delete projected[field]
      }
      return projected as T
    }

    return item
  })
}

/**
 * Get required columns from filter and projection
 */
function getRequiredColumns(filter: Filter, projection?: Projection): string[] {
  const columns = new Set<string>()

  for (const field of Object.keys(filter)) {
    if (!field.startsWith('$')) {
      columns.add(field)
    }
  }

  if (projection) {
    for (const [field, value] of Object.entries(projection)) {
      if (value === 1 || value === true) {
        columns.add(field)
      }
    }
  }

  return Array.from(columns)
}

// =============================================================================
// Test Suites
// =============================================================================

describe('QueryExecutor - Predicate Pushdown', () => {
  // ===========================================================================
  // Row Group Selection Tests
  // ===========================================================================

  describe('selectRowGroups', () => {
    const createMetadata = (rowGroups: RowGroupMetadata[]): ParquetMetadata => ({
      numRows: rowGroups.reduce((sum, rg) => sum + rg.numRows, 0),
      rowGroups,
      columns: [],
      schema: { fields: [] },
    })

    it('should return all row groups when filter is empty', () => {
      const metadata = createMetadata([
        { index: 0, numRows: 1000, offset: 0, compressedSize: 1024, totalSize: 2048, columnStats: {} },
        { index: 1, numRows: 1000, offset: 1024, compressedSize: 1024, totalSize: 2048, columnStats: {} },
      ])

      const selected = selectRowGroups(metadata, {})

      expect(selected).toHaveLength(2)
    })

    it('should skip row groups based on equality filter', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 1990, max: 1999, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 2000, max: 2009, nullCount: 0, hasStats: true } },
        },
        {
          index: 2,
          numRows: 1000,
          offset: 2048,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 2010, max: 2019, nullCount: 0, hasStats: true } },
        },
      ])

      const selected = selectRowGroups(metadata, { year: 2005 })

      expect(selected).toHaveLength(1)
      expect(selected[0].index).toBe(1)
    })

    it('should skip row groups based on $gt filter', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { score: { name: 'score', min: 0, max: 50, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { score: { name: 'score', min: 51, max: 100, nullCount: 0, hasStats: true } },
        },
      ])

      const selected = selectRowGroups(metadata, { score: { $gt: 75 } })

      expect(selected).toHaveLength(1)
      expect(selected[0].index).toBe(1)
    })

    it('should skip row groups based on $lt filter', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { score: { name: 'score', min: 0, max: 50, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { score: { name: 'score', min: 51, max: 100, nullCount: 0, hasStats: true } },
        },
      ])

      const selected = selectRowGroups(metadata, { score: { $lt: 25 } })

      expect(selected).toHaveLength(1)
      expect(selected[0].index).toBe(0)
    })

    it('should skip row groups based on range filter', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 1990, max: 1999, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 2000, max: 2009, nullCount: 0, hasStats: true } },
        },
        {
          index: 2,
          numRows: 1000,
          offset: 2048,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 2010, max: 2019, nullCount: 0, hasStats: true } },
        },
      ])

      const selected = selectRowGroups(metadata, { year: { $gte: 2005, $lte: 2015 } })

      expect(selected).toHaveLength(2)
      expect(selected.map((rg) => rg.index)).toEqual([1, 2])
    })

    it('should skip row groups based on $in filter', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { category: { name: 'category', min: 1, max: 3, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { category: { name: 'category', min: 4, max: 6, nullCount: 0, hasStats: true } },
        },
        {
          index: 2,
          numRows: 1000,
          offset: 2048,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { category: { name: 'category', min: 7, max: 9, nullCount: 0, hasStats: true } },
        },
      ])

      const selected = selectRowGroups(metadata, { category: { $in: [2, 5] } })

      expect(selected).toHaveLength(2)
      expect(selected.map((rg) => rg.index)).toEqual([0, 1])
    })

    it('should include row groups when stats are missing', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 1990, max: 1999, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: {}, // No stats for any column
        },
      ])

      const selected = selectRowGroups(metadata, { year: 2005 })

      // Should include row group 1 because stats are missing
      expect(selected).toHaveLength(1)
      expect(selected[0].index).toBe(1)
    })

    it('should include row groups when hasStats is false', () => {
      const metadata = createMetadata([
        {
          index: 0,
          numRows: 1000,
          offset: 0,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: 1990, max: 1999, nullCount: 0, hasStats: true } },
        },
        {
          index: 1,
          numRows: 1000,
          offset: 1024,
          compressedSize: 1024,
          totalSize: 2048,
          columnStats: { year: { name: 'year', min: undefined, max: undefined, nullCount: 0, hasStats: false } },
        },
      ])

      const selected = selectRowGroups(metadata, { year: 2005 })

      expect(selected).toHaveLength(1)
      expect(selected[0].index).toBe(1)
    })
  })

  // ===========================================================================
  // Condition Matching Tests
  // ===========================================================================

  describe('conditionMightMatch', () => {
    const stats: ColumnStats = {
      name: 'score',
      min: 10,
      max: 90,
      nullCount: 5,
      hasStats: true,
    }

    it('should match direct equality within range', () => {
      expect(conditionMightMatch(stats, 50)).toBe(true)
    })

    it('should not match direct equality outside range', () => {
      expect(conditionMightMatch(stats, 5)).toBe(false)
      expect(conditionMightMatch(stats, 95)).toBe(false)
    })

    it('should match $eq within range', () => {
      expect(conditionMightMatch(stats, { $eq: 50 })).toBe(true)
    })

    it('should not match $eq outside range', () => {
      expect(conditionMightMatch(stats, { $eq: 5 })).toBe(false)
    })

    it('should match $gt when max > value', () => {
      expect(conditionMightMatch(stats, { $gt: 50 })).toBe(true)
    })

    it('should not match $gt when max <= value', () => {
      expect(conditionMightMatch(stats, { $gt: 95 })).toBe(false)
    })

    it('should match $gte when max >= value', () => {
      expect(conditionMightMatch(stats, { $gte: 90 })).toBe(true)
    })

    it('should not match $gte when max < value', () => {
      expect(conditionMightMatch(stats, { $gte: 95 })).toBe(false)
    })

    it('should match $lt when min < value', () => {
      expect(conditionMightMatch(stats, { $lt: 50 })).toBe(true)
    })

    it('should not match $lt when min >= value', () => {
      expect(conditionMightMatch(stats, { $lt: 5 })).toBe(false)
    })

    it('should match $lte when min <= value', () => {
      expect(conditionMightMatch(stats, { $lte: 10 })).toBe(true)
    })

    it('should not match $lte when min > value', () => {
      expect(conditionMightMatch(stats, { $lte: 5 })).toBe(false)
    })

    it('should match $in when at least one value is in range', () => {
      expect(conditionMightMatch(stats, { $in: [5, 50, 95] })).toBe(true)
    })

    it('should not match $in when no values are in range', () => {
      expect(conditionMightMatch(stats, { $in: [5, 95, 100] })).toBe(false)
    })

    it('should match unknown operators (fallback to true)', () => {
      expect(conditionMightMatch(stats, { $regex: 'test' })).toBe(true)
    })
  })
})

describe('QueryExecutor - Filter Matching', () => {
  // ===========================================================================
  // Filter Matching Tests
  // ===========================================================================

  describe('matchesFilter', () => {
    it('should match empty filter', () => {
      expect(matchesFilter({ name: 'test' }, {})).toBe(true)
    })

    it('should match direct equality', () => {
      expect(matchesFilter({ status: 'active' }, { status: 'active' })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { status: 'active' })).toBe(false)
    })

    it('should match $eq operator', () => {
      expect(matchesFilter({ count: 10 }, { count: { $eq: 10 } })).toBe(true)
      expect(matchesFilter({ count: 5 }, { count: { $eq: 10 } })).toBe(false)
    })

    it('should match $ne operator', () => {
      expect(matchesFilter({ status: 'active' }, { status: { $ne: 'inactive' } })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { status: { $ne: 'inactive' } })).toBe(false)
    })

    it('should match comparison operators', () => {
      const record = { score: 75 }
      expect(matchesFilter(record, { score: { $gt: 50 } })).toBe(true)
      expect(matchesFilter(record, { score: { $gt: 80 } })).toBe(false)
      expect(matchesFilter(record, { score: { $gte: 75 } })).toBe(true)
      expect(matchesFilter(record, { score: { $lt: 100 } })).toBe(true)
      expect(matchesFilter(record, { score: { $lt: 50 } })).toBe(false)
      expect(matchesFilter(record, { score: { $lte: 75 } })).toBe(true)
    })

    it('should match $in operator', () => {
      expect(matchesFilter({ status: 'active' }, { status: { $in: ['active', 'pending'] } })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { status: { $in: ['active', 'pending'] } })).toBe(false)
    })

    it('should match $nin operator', () => {
      expect(matchesFilter({ status: 'active' }, { status: { $nin: ['inactive', 'deleted'] } })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { status: { $nin: ['inactive', 'deleted'] } })).toBe(false)
    })

    it('should match $exists operator', () => {
      expect(matchesFilter({ name: 'test' }, { name: { $exists: true } })).toBe(true)
      expect(matchesFilter({ other: 'value' }, { name: { $exists: true } })).toBe(false)
      expect(matchesFilter({ other: 'value' }, { name: { $exists: false } })).toBe(true)
    })

    it('should match $regex operator', () => {
      expect(matchesFilter({ name: 'Hello World' }, { name: { $regex: 'world', $options: 'i' } })).toBe(true)
      expect(matchesFilter({ name: 'Hello' }, { name: { $regex: 'world', $options: 'i' } })).toBe(false)
    })

    it('should match $startsWith operator', () => {
      expect(matchesFilter({ name: 'Hello World' }, { name: { $startsWith: 'Hello' } })).toBe(true)
      expect(matchesFilter({ name: 'World Hello' }, { name: { $startsWith: 'Hello' } })).toBe(false)
    })

    it('should match $endsWith operator', () => {
      expect(matchesFilter({ name: 'Hello World' }, { name: { $endsWith: 'World' } })).toBe(true)
      expect(matchesFilter({ name: 'World Hello' }, { name: { $endsWith: 'World' } })).toBe(false)
    })

    it('should match $contains operator', () => {
      expect(matchesFilter({ name: 'Hello World' }, { name: { $contains: 'lo Wo' } })).toBe(true)
      expect(matchesFilter({ name: 'Hello World' }, { name: { $contains: 'xyz' } })).toBe(false)
    })

    it('should match $all operator', () => {
      expect(matchesFilter({ tags: ['a', 'b', 'c'] }, { tags: { $all: ['a', 'b'] } })).toBe(true)
      expect(matchesFilter({ tags: ['a', 'b'] }, { tags: { $all: ['a', 'c'] } })).toBe(false)
    })

    it('should match $size operator', () => {
      expect(matchesFilter({ tags: ['a', 'b', 'c'] }, { tags: { $size: 3 } })).toBe(true)
      expect(matchesFilter({ tags: ['a', 'b'] }, { tags: { $size: 3 } })).toBe(false)
    })

    it('should match $and operator', () => {
      expect(matchesFilter({ a: 1, b: 2 }, { $and: [{ a: 1 }, { b: 2 }] })).toBe(true)
      expect(matchesFilter({ a: 1, b: 3 }, { $and: [{ a: 1 }, { b: 2 }] })).toBe(false)
    })

    it('should match $or operator', () => {
      expect(matchesFilter({ status: 'active' }, { $or: [{ status: 'active' }, { status: 'pending' }] })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { $or: [{ status: 'active' }, { status: 'pending' }] })).toBe(false)
    })

    it('should match $not operator', () => {
      expect(matchesFilter({ status: 'active' }, { $not: { status: 'inactive' } })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { $not: { status: 'inactive' } })).toBe(false)
    })

    it('should match $nor operator', () => {
      expect(matchesFilter({ status: 'active' }, { $nor: [{ status: 'inactive' }, { status: 'deleted' }] })).toBe(true)
      expect(matchesFilter({ status: 'inactive' }, { $nor: [{ status: 'inactive' }, { status: 'deleted' }] })).toBe(
        false
      )
    })

    it('should match multiple field conditions', () => {
      const record = { name: 'test', score: 75, status: 'active' }
      expect(matchesFilter(record, { name: 'test', score: { $gte: 50 } })).toBe(true)
      expect(matchesFilter(record, { name: 'test', score: { $gte: 80 } })).toBe(false)
    })
  })
})

describe('QueryExecutor - ID Filter Extraction', () => {
  // ===========================================================================
  // ID Filter Extraction Tests
  // ===========================================================================

  describe('extractIdFilter', () => {
    it('should extract direct $id filter', () => {
      const filter = { $id: 'doc123', status: 'active' }
      const extracted = extractIdFilter(filter)

      expect(extracted).toEqual({ $id: 'doc123' })
    })

    it('should return null when no $id filter', () => {
      const filter = { status: 'active', name: 'test' }
      const extracted = extractIdFilter(filter)

      expect(extracted).toBeNull()
    })

    it('should extract $id from $or conditions', () => {
      const filter = {
        $or: [{ $id: 'doc1' }, { $id: 'doc2' }, { name: 'test' }],
      }
      const extracted = extractIdFilter(filter)

      expect(extracted).toEqual({ $or: [{ $id: 'doc1' }, { $id: 'doc2' }] })
    })

    it('should extract $id from $and conditions', () => {
      const filter = {
        $and: [{ $id: 'doc1' }, { status: 'active' }],
      }
      const extracted = extractIdFilter(filter)

      expect(extracted).toEqual({ $id: 'doc1' })
    })

    it('should return null for $or without $id conditions', () => {
      const filter = {
        $or: [{ status: 'active' }, { status: 'pending' }],
      }
      const extracted = extractIdFilter(filter)

      expect(extracted).toBeNull()
    })
  })

  describe('removeIdFilter', () => {
    it('should remove direct $id filter', () => {
      const filter = { $id: 'doc123', status: 'active' }
      const remaining = removeIdFilter(filter)

      expect(remaining).toEqual({ status: 'active' })
      expect('$id' in remaining).toBe(false)
    })

    it('should remove $id from $or conditions', () => {
      const filter = {
        $or: [{ $id: 'doc1' }, { $id: 'doc2' }, { name: 'test' }],
      }
      const remaining = removeIdFilter(filter)

      expect(remaining).toEqual({ $or: [{ name: 'test' }] })
    })

    it('should remove $id from $and conditions', () => {
      const filter = {
        $and: [{ $id: 'doc1' }, { status: 'active' }],
      }
      const remaining = removeIdFilter(filter)

      expect(remaining).toEqual({ $and: [{ status: 'active' }] })
    })

    it('should preserve filter without $id', () => {
      const filter = { status: 'active', name: 'test' }
      const remaining = removeIdFilter(filter)

      expect(remaining).toEqual(filter)
    })

    it('should remove $or if all conditions are $id', () => {
      const filter = {
        $or: [{ $id: 'doc1' }, { $id: 'doc2' }],
      }
      const remaining = removeIdFilter(filter)

      expect('$or' in remaining).toBe(false)
    })
  })
})

describe('QueryExecutor - Post-Processing', () => {
  // ===========================================================================
  // Sorting Tests
  // ===========================================================================

  describe('applySort', () => {
    it('should sort by single field ascending', () => {
      const data = [{ name: 'c', score: 3 }, { name: 'a', score: 1 }, { name: 'b', score: 2 }]
      const sorted = applySort(data, { name: 'asc' })

      expect(sorted.map((r) => r.name)).toEqual(['a', 'b', 'c'])
    })

    it('should sort by single field descending', () => {
      const data = [{ name: 'a', score: 1 }, { name: 'c', score: 3 }, { name: 'b', score: 2 }]
      const sorted = applySort(data, { name: 'desc' })

      expect(sorted.map((r) => r.name)).toEqual(['c', 'b', 'a'])
    })

    it('should sort by numeric value with 1/-1 notation', () => {
      const data = [{ score: 75 }, { score: 25 }, { score: 50 }]
      const sorted = applySort(data, { score: 1 })

      expect(sorted.map((r) => r.score)).toEqual([25, 50, 75])

      const sortedDesc = applySort([...data], { score: -1 })
      expect(sortedDesc.map((r) => r.score)).toEqual([75, 50, 25])
    })

    it('should sort by multiple fields', () => {
      const data = [
        { category: 'b', score: 2 },
        { category: 'a', score: 1 },
        { category: 'a', score: 2 },
        { category: 'b', score: 1 },
      ]
      const sorted = applySort(data, { category: 'asc', score: 'desc' })

      expect(sorted).toEqual([
        { category: 'a', score: 2 },
        { category: 'a', score: 1 },
        { category: 'b', score: 2 },
        { category: 'b', score: 1 },
      ])
    })

    it('should handle null values (nulls last)', () => {
      const data = [{ name: 'b' }, { name: null }, { name: 'a' }]
      const sorted = applySort(data, { name: 'asc' })

      expect(sorted.map((r) => r.name)).toEqual(['a', 'b', null])
    })

    it('should handle undefined values (undefined last)', () => {
      const data = [{ name: 'b' }, {}, { name: 'a' }]
      const sorted = applySort(data as Array<{ name?: string }>, { name: 'asc' })

      expect(sorted.map((r) => r.name)).toEqual(['a', 'b', undefined])
    })
  })

  // ===========================================================================
  // Projection Tests
  // ===========================================================================

  describe('applyProjection', () => {
    it('should include only specified fields', () => {
      const data = [{ a: 1, b: 2, c: 3 }]
      const projected = applyProjection(data, { a: 1, b: 1 })

      expect(projected[0]).toEqual({ a: 1, b: 2 })
    })

    it('should exclude specified fields', () => {
      const data = [{ a: 1, b: 2, c: 3 }]
      const projected = applyProjection(data, { b: 0 })

      expect(projected[0]).toEqual({ a: 1, c: 3 })
    })

    it('should handle boolean projection values', () => {
      const data = [{ a: 1, b: 2, c: 3 }]
      const included = applyProjection(data, { a: true, b: true })
      const excluded = applyProjection(data, { c: false })

      expect(included[0]).toEqual({ a: 1, b: 2 })
      expect(excluded[0]).toEqual({ a: 1, b: 2 })
    })

    it('should return original item when projection is empty', () => {
      const data = [{ a: 1, b: 2, c: 3 }]
      const projected = applyProjection(data, {})

      expect(projected[0]).toEqual({ a: 1, b: 2, c: 3 })
    })

    it('should handle missing fields gracefully', () => {
      const data = [{ a: 1 }]
      const projected = applyProjection(data, { a: 1, b: 1 })

      expect(projected[0]).toEqual({ a: 1 })
    })
  })

  // ===========================================================================
  // Required Columns Tests
  // ===========================================================================

  describe('getRequiredColumns', () => {
    it('should extract columns from filter', () => {
      const columns = getRequiredColumns({ name: 'test', score: { $gte: 50 } })

      expect(columns).toContain('name')
      expect(columns).toContain('score')
    })

    it('should skip logical operators', () => {
      const columns = getRequiredColumns({ $and: [{ name: 'test' }] })

      expect(columns).not.toContain('$and')
    })

    it('should extract columns from projection', () => {
      const columns = getRequiredColumns({}, { name: 1, email: 1 })

      expect(columns).toContain('name')
      expect(columns).toContain('email')
    })

    it('should combine filter and projection columns', () => {
      const columns = getRequiredColumns({ status: 'active' }, { name: 1, email: 1 })

      expect(columns).toContain('status')
      expect(columns).toContain('name')
      expect(columns).toContain('email')
    })

    it('should not include excluded projection fields', () => {
      const columns = getRequiredColumns({}, { password: 0 })

      expect(columns).not.toContain('password')
    })
  })
})

describe('QueryExecutor - Bloom Filter Usage', () => {
  // ===========================================================================
  // Bloom Filter Usage Tests
  // ===========================================================================

  describe('canUseBloomFilter', () => {
    it('should return true for direct id equality', () => {
      expect(canUseBloomFilter({ id: 'doc123' })).toBe(true)
    })

    it('should return true for $eq on id', () => {
      expect(canUseBloomFilter({ id: { $eq: 'doc123' } })).toBe(true)
    })

    it('should return false when id is not in filter', () => {
      expect(canUseBloomFilter({ status: 'active' })).toBe(false)
    })

    it('should return false for $in on id', () => {
      expect(canUseBloomFilter({ id: { $in: ['doc1', 'doc2'] } })).toBe(false)
    })

    it('should return false for range operators on id', () => {
      expect(canUseBloomFilter({ id: { $gt: 'doc100' } })).toBe(false)
    })
  })
})

describe('QueryExecutor - Index Selection Integration', () => {
  // ===========================================================================
  // Index Selection Logic Tests
  // ===========================================================================

  describe('index selection logic', () => {
    it('should identify equality conditions for hash index', () => {
      const isEquality = (condition: unknown): boolean => {
        if (condition === null || typeof condition !== 'object') {
          return true // Direct value comparison
        }
        const obj = condition as Record<string, unknown>
        if ('$eq' in obj) return true
        if ('$in' in obj) return true
        if ('$gt' in obj || '$gte' in obj || '$lt' in obj || '$lte' in obj) {
          return false
        }
        return true
      }

      expect(isEquality('movie')).toBe(true)
      expect(isEquality({ $eq: 'movie' })).toBe(true)
      expect(isEquality({ $in: ['movie', 'short'] })).toBe(true)
      expect(isEquality({ $gt: 2000 })).toBe(false)
      expect(isEquality({ $gte: 2000, $lt: 2010 })).toBe(false)
    })

    it('should identify range conditions (for parquet predicate pushdown)', () => {
      // NOTE: SST indexes removed - range conditions now use parquet predicate pushdown
      const isRange = (condition: unknown): boolean => {
        if (typeof condition !== 'object' || condition === null) {
          return false
        }
        const obj = condition as Record<string, unknown>
        return '$gt' in obj || '$gte' in obj || '$lt' in obj || '$lte' in obj
      }

      expect(isRange('movie')).toBe(false)
      expect(isRange({ $eq: 2005 })).toBe(false)
      expect(isRange({ $gt: 2000 })).toBe(true)
      expect(isRange({ $gte: 2000, $lt: 2010 })).toBe(true)
    })

    it('should identify FTS queries', () => {
      const isFTS = (filter: Filter): boolean => {
        return (
          '$text' in filter &&
          typeof filter.$text === 'object' &&
          filter.$text !== null &&
          '$search' in (filter.$text as object)
        )
      }

      expect(isFTS({ $text: { $search: 'inception' } })).toBe(true)
      expect(isFTS({ name: 'inception' })).toBe(false)
      expect(isFTS({ $text: 'inception' })).toBe(false)
    })
  })
})

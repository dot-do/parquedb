/**
 * Tests for Predicate Pushdown for Typed Mode Reads
 *
 * Tests the conversion of ParqueDB MongoDB-style filters to Parquet predicates
 * that can be used with hyparquet's parquetQuery() for efficient filtering.
 */

import { describe, it, expect } from 'vitest'
import {
  filterToPredicates,
  predicatesToQueryFilter,
  analyzeFilterForPushdown,
  extractNonPushableFilter,
  canFullyPushdown,
  getPredicateColumns,
  mergePredicates,
  hasPushableConditions,
  type ParquetPredicate,
} from '../../../src/query/predicate-pushdown'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// filterToPredicates Tests
// =============================================================================

describe('filterToPredicates', () => {
  describe('comparison operators', () => {
    it('converts implicit equality to $eq predicate', () => {
      const filter: Filter = { status: 'active' }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'status',
        op: 'eq',
        value: 'active',
      })
    })

    it('converts explicit $eq operator', () => {
      const filter: Filter = { age: { $eq: 25 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'age',
        op: 'eq',
        value: 25,
      })
    })

    it('converts $gt operator', () => {
      const filter: Filter = { score: { $gt: 100 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'score',
        op: 'gt',
        value: 100,
      })
    })

    it('converts $gte operator', () => {
      const filter: Filter = { age: { $gte: 18 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'age',
        op: 'gte',
        value: 18,
      })
    })

    it('converts $lt operator', () => {
      const filter: Filter = { price: { $lt: 1000 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'price',
        op: 'lt',
        value: 1000,
      })
    })

    it('converts $lte operator', () => {
      const filter: Filter = { rating: { $lte: 5.0 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'rating',
        op: 'lte',
        value: 5.0,
      })
    })

    it('converts $in operator', () => {
      const filter: Filter = { category: { $in: ['A', 'B', 'C'] } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'category',
        op: 'in',
        value: ['A', 'B', 'C'],
      })
    })

    it('converts $ne operator', () => {
      const filter: Filter = { status: { $ne: 'deleted' } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'status',
        op: 'ne',
        value: 'deleted',
      })
    })

    it('handles multiple operators on same column', () => {
      const filter: Filter = { age: { $gte: 18, $lt: 65 } }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(2)
      expect(predicates).toContainEqual({ column: 'age', op: 'gte', value: 18 })
      expect(predicates).toContainEqual({ column: 'age', op: 'lt', value: 65 })
    })

    it('handles numeric equality', () => {
      const filter: Filter = { count: 42 }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'count',
        op: 'eq',
        value: 42,
      })
    })

    it('handles boolean equality', () => {
      const filter: Filter = { active: true }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'active',
        op: 'eq',
        value: true,
      })
    })

    it('handles Date values', () => {
      const date = new Date('2024-01-15')
      const filter: Filter = { createdAt: date }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(1)
      expect(predicates[0]).toEqual({
        column: 'createdAt',
        op: 'eq',
        value: date,
      })
    })
  })

  describe('multiple fields', () => {
    it('extracts predicates from multiple fields', () => {
      const filter: Filter = {
        status: 'published',
        views: { $gte: 100 },
        rating: { $gt: 4 },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(3)
      expect(predicates).toContainEqual({ column: 'status', op: 'eq', value: 'published' })
      expect(predicates).toContainEqual({ column: 'views', op: 'gte', value: 100 })
      expect(predicates).toContainEqual({ column: 'rating', op: 'gt', value: 4 })
    })
  })

  describe('$and handling', () => {
    it('extracts predicates from $and conditions', () => {
      const filter: Filter = {
        $and: [
          { status: 'published' },
          { views: { $gte: 100 } },
        ],
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(2)
      expect(predicates).toContainEqual({ column: 'status', op: 'eq', value: 'published' })
      expect(predicates).toContainEqual({ column: 'views', op: 'gte', value: 100 })
    })

    it('combines top-level and $and predicates', () => {
      const filter: Filter = {
        active: true,
        $and: [
          { age: { $gte: 18 } },
          { score: { $lt: 100 } },
        ],
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(3)
      expect(predicates).toContainEqual({ column: 'active', op: 'eq', value: true })
      expect(predicates).toContainEqual({ column: 'age', op: 'gte', value: 18 })
      expect(predicates).toContainEqual({ column: 'score', op: 'lt', value: 100 })
    })

    it('handles nested $and', () => {
      const filter: Filter = {
        $and: [
          { $and: [{ a: 1 }, { b: 2 }] },
          { c: 3 },
        ],
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(3)
      expect(predicates).toContainEqual({ column: 'a', op: 'eq', value: 1 })
      expect(predicates).toContainEqual({ column: 'b', op: 'eq', value: 2 })
      expect(predicates).toContainEqual({ column: 'c', op: 'eq', value: 3 })
    })
  })

  describe('skipped conditions', () => {
    it('skips $or operator (needs row-level evaluation)', () => {
      const filter: Filter = {
        $or: [
          { status: 'published' },
          { featured: true },
        ],
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips $not operator', () => {
      const filter: Filter = {
        $not: { status: 'draft' },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips $nor operator', () => {
      const filter: Filter = {
        $nor: [{ a: 1 }, { b: 2 }],
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips $text operator', () => {
      const filter: Filter = {
        $text: { $search: 'hello world' },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips $vector operator', () => {
      const filter: Filter = {
        $vector: { $near: [1, 2, 3], $k: 10, $field: 'embedding' },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips $geo operator', () => {
      const filter: Filter = {
        $geo: { $near: { lng: -73.97, lat: 40.77 } },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips nested fields (dot notation)', () => {
      const filter: Filter = {
        'metadata.readTime': { $gt: 5 },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('skips null values', () => {
      const filter: Filter = {
        email: null,
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })

    it('does NOT skip non-pushable operators like $regex', () => {
      // $regex is NOT pushed down - it can't use statistics
      const filter: Filter = {
        name: { $regex: '^John' },
      }
      const predicates = filterToPredicates(filter)

      // No predicates - $regex is not pushable
      expect(predicates).toHaveLength(0)
    })

    it('does NOT skip $nin operator from pushdown', () => {
      // $nin is NOT pushed down - it can't use statistics effectively
      const filter: Filter = {
        status: { $nin: ['deleted', 'archived'] },
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(0)
    })
  })

  describe('typed columns filtering', () => {
    it('only extracts predicates for typed columns when specified', () => {
      const filter: Filter = {
        age: { $gte: 18 },
        status: 'active',
        score: { $gt: 100 },
      }
      const typedColumns = new Set(['age', 'status'])
      const predicates = filterToPredicates(filter, typedColumns)

      expect(predicates).toHaveLength(2)
      expect(predicates).toContainEqual({ column: 'age', op: 'gte', value: 18 })
      expect(predicates).toContainEqual({ column: 'status', op: 'eq', value: 'active' })
      // score is NOT included (not a typed column)
    })

    it('includes all columns when typedColumns not specified', () => {
      const filter: Filter = {
        age: { $gte: 18 },
        status: 'active',
      }
      const predicates = filterToPredicates(filter)

      expect(predicates).toHaveLength(2)
    })
  })

  describe('empty/invalid filters', () => {
    it('returns empty array for empty filter', () => {
      const predicates = filterToPredicates({})
      expect(predicates).toHaveLength(0)
    })

    it('returns empty array for null filter', () => {
      const predicates = filterToPredicates(null as unknown as Filter)
      expect(predicates).toHaveLength(0)
    })

    it('returns empty array for undefined filter', () => {
      const predicates = filterToPredicates(undefined as unknown as Filter)
      expect(predicates).toHaveLength(0)
    })
  })
})

// =============================================================================
// predicatesToQueryFilter Tests
// =============================================================================

describe('predicatesToQueryFilter', () => {
  it('converts single equality predicate to direct value', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'status', op: 'eq', value: 'active' },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ status: 'active' })
  })

  it('converts $gt predicate to operator object', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'age', op: 'gt', value: 18 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ age: { $gt: 18 } })
  })

  it('converts $gte predicate', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'score', op: 'gte', value: 100 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ score: { $gte: 100 } })
  })

  it('converts $lt predicate', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'price', op: 'lt', value: 1000 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ price: { $lt: 1000 } })
  })

  it('converts $lte predicate', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'rating', op: 'lte', value: 5 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ rating: { $lte: 5 } })
  })

  it('converts $in predicate', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'category', op: 'in', value: ['A', 'B', 'C'] },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ category: { $in: ['A', 'B', 'C'] } })
  })

  it('converts $ne predicate', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'status', op: 'ne', value: 'deleted' },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ status: { $ne: 'deleted' } })
  })

  it('combines multiple predicates on same column', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'age', op: 'gte', value: 18 },
      { column: 'age', op: 'lt', value: 65 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({ age: { $gte: 18, $lt: 65 } })
  })

  it('handles predicates on multiple columns', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'status', op: 'eq', value: 'active' },
      { column: 'age', op: 'gte', value: 18 },
      { column: 'score', op: 'gt', value: 100 },
    ]
    const filter = predicatesToQueryFilter(predicates)

    expect(filter).toEqual({
      status: 'active',
      age: { $gte: 18 },
      score: { $gt: 100 },
    })
  })

  it('returns empty object for empty predicates', () => {
    const filter = predicatesToQueryFilter([])
    expect(filter).toEqual({})
  })
})

// =============================================================================
// analyzeFilterForPushdown Tests
// =============================================================================

describe('analyzeFilterForPushdown', () => {
  it('identifies pushable predicates', () => {
    const filter: Filter = {
      age: { $gte: 18 },
      status: 'active',
    }
    const typedColumns = new Set(['age', 'status'])

    const result = analyzeFilterForPushdown(filter, typedColumns)

    expect(result.canPushdown).toBe(true)
    expect(result.pushdownPredicates).toHaveLength(2)
    expect(result.pushdownColumns).toContain('age')
    expect(result.pushdownColumns).toContain('status')
  })

  it('separates non-pushable conditions into remainingFilter', () => {
    const filter: Filter = {
      age: { $gte: 18 },
      name: { $regex: '^John' },
    }
    const typedColumns = new Set(['age', 'name'])

    const result = analyzeFilterForPushdown(filter, typedColumns)

    expect(result.canPushdown).toBe(true)
    expect(result.pushdownPredicates).toHaveLength(1)
    expect(result.remainingFilter).toEqual({ name: { $regex: '^John' } })
  })

  it('handles filter with only non-pushable conditions', () => {
    const filter: Filter = {
      name: { $regex: '^John' },
      tags: { $all: ['a', 'b'] },
    }
    const typedColumns = new Set(['name', 'tags'])

    const result = analyzeFilterForPushdown(filter, typedColumns)

    expect(result.canPushdown).toBe(false)
    expect(result.pushdownPredicates).toHaveLength(0)
    expect(Object.keys(result.remainingFilter).length).toBeGreaterThan(0)
  })

  it('handles non-typed columns', () => {
    const filter: Filter = {
      age: { $gte: 18 },
      customField: 'value',
    }
    const typedColumns = new Set(['age']) // customField is not typed

    const result = analyzeFilterForPushdown(filter, typedColumns)

    expect(result.pushdownPredicates).toHaveLength(1)
    expect(result.pushdownColumns).toEqual(['age'])
    expect(result.remainingFilter).toEqual({ customField: 'value' })
  })
})

// =============================================================================
// extractNonPushableFilter Tests
// =============================================================================

describe('extractNonPushableFilter', () => {
  it('extracts $regex conditions', () => {
    const filter: Filter = {
      name: { $regex: '^John' },
    }
    const typedColumns = new Set(['name'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ name: { $regex: '^John' } })
  })

  it('extracts $startsWith conditions', () => {
    const filter: Filter = {
      name: { $startsWith: 'Hello' },
    }
    const typedColumns = new Set(['name'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ name: { $startsWith: 'Hello' } })
  })

  it('extracts $all array operator', () => {
    const filter: Filter = {
      tags: { $all: ['a', 'b'] },
    }
    const typedColumns = new Set(['tags'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ tags: { $all: ['a', 'b'] } })
  })

  it('extracts $elemMatch operator', () => {
    const filter: Filter = {
      items: { $elemMatch: { price: { $gt: 100 } } },
    }
    const typedColumns = new Set(['items'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ items: { $elemMatch: { price: { $gt: 100 } } } })
  })

  it('keeps $or in remaining filter', () => {
    const filter: Filter = {
      $or: [{ a: 1 }, { b: 2 }],
    }
    const typedColumns = new Set(['a', 'b'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ $or: [{ a: 1 }, { b: 2 }] })
  })

  it('keeps $not in remaining filter', () => {
    const filter: Filter = {
      $not: { status: 'draft' },
    }
    const typedColumns = new Set(['status'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ $not: { status: 'draft' } })
  })

  it('keeps $text operator', () => {
    const filter: Filter = {
      $text: { $search: 'hello' },
    }
    const typedColumns = new Set<string>()

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ $text: { $search: 'hello' } })
  })

  it('keeps non-typed columns', () => {
    const filter: Filter = {
      typedCol: 'value',
      untypedCol: 'other',
    }
    const typedColumns = new Set(['typedCol'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({ untypedCol: 'other' })
  })

  it('processes $and recursively', () => {
    const filter: Filter = {
      $and: [
        { name: { $regex: '^John' } },
        { age: { $gte: 18 } }, // pushable - should not be in remaining
      ],
    }
    const typedColumns = new Set(['name', 'age'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    // Only non-pushable $regex should remain
    expect(remaining).toEqual({ $and: [{ name: { $regex: '^John' } }] })
  })

  it('returns empty filter when all pushable', () => {
    const filter: Filter = {
      age: { $gte: 18 },
      status: 'active',
    }
    const typedColumns = new Set(['age', 'status'])

    const remaining = extractNonPushableFilter(filter, typedColumns)

    expect(remaining).toEqual({})
  })
})

// =============================================================================
// canFullyPushdown Tests
// =============================================================================

describe('canFullyPushdown', () => {
  it('returns true for simple pushable filter', () => {
    const filter: Filter = { status: 'active' }
    const typedColumns = new Set(['status'])

    expect(canFullyPushdown(filter, typedColumns)).toBe(true)
  })

  it('returns true for complex range filter', () => {
    const filter: Filter = {
      age: { $gte: 18, $lt: 65 },
      score: { $gt: 100 },
    }
    const typedColumns = new Set(['age', 'score'])

    expect(canFullyPushdown(filter, typedColumns)).toBe(true)
  })

  it('returns false when filter has $regex', () => {
    const filter: Filter = {
      name: { $regex: '^John' },
    }
    const typedColumns = new Set(['name'])

    expect(canFullyPushdown(filter, typedColumns)).toBe(false)
  })

  it('returns false when filter has $or', () => {
    const filter: Filter = {
      $or: [{ a: 1 }, { b: 2 }],
    }
    const typedColumns = new Set(['a', 'b'])

    expect(canFullyPushdown(filter, typedColumns)).toBe(false)
  })

  it('returns false when filter has non-typed columns', () => {
    const filter: Filter = {
      typedCol: 'value',
      untypedCol: 'other',
    }
    const typedColumns = new Set(['typedCol'])

    expect(canFullyPushdown(filter, typedColumns)).toBe(false)
  })
})

// =============================================================================
// Helper Functions Tests
// =============================================================================

describe('getPredicateColumns', () => {
  it('extracts unique column names', () => {
    const predicates: ParquetPredicate[] = [
      { column: 'age', op: 'gte', value: 18 },
      { column: 'age', op: 'lt', value: 65 },
      { column: 'status', op: 'eq', value: 'active' },
    ]

    const columns = getPredicateColumns(predicates)

    expect(columns).toHaveLength(2)
    expect(columns).toContain('age')
    expect(columns).toContain('status')
  })

  it('returns empty array for empty predicates', () => {
    const columns = getPredicateColumns([])
    expect(columns).toEqual([])
  })
})

describe('mergePredicates', () => {
  it('merges multiple predicate arrays', () => {
    const arr1: ParquetPredicate[] = [{ column: 'a', op: 'eq', value: 1 }]
    const arr2: ParquetPredicate[] = [{ column: 'b', op: 'gt', value: 2 }]
    const arr3: ParquetPredicate[] = [{ column: 'c', op: 'lt', value: 3 }]

    const merged = mergePredicates(arr1, arr2, arr3)

    expect(merged).toHaveLength(3)
    expect(merged).toContainEqual({ column: 'a', op: 'eq', value: 1 })
    expect(merged).toContainEqual({ column: 'b', op: 'gt', value: 2 })
    expect(merged).toContainEqual({ column: 'c', op: 'lt', value: 3 })
  })

  it('handles empty arrays', () => {
    const merged = mergePredicates([], [], [])
    expect(merged).toEqual([])
  })
})

describe('hasPushableConditions', () => {
  it('returns true for direct equality', () => {
    expect(hasPushableConditions({ status: 'active' })).toBe(true)
  })

  it('returns true for $eq operator', () => {
    expect(hasPushableConditions({ status: { $eq: 'active' } })).toBe(true)
  })

  it('returns true for $gt operator', () => {
    expect(hasPushableConditions({ age: { $gt: 18 } })).toBe(true)
  })

  it('returns true for $gte operator', () => {
    expect(hasPushableConditions({ age: { $gte: 18 } })).toBe(true)
  })

  it('returns true for $lt operator', () => {
    expect(hasPushableConditions({ age: { $lt: 65 } })).toBe(true)
  })

  it('returns true for $lte operator', () => {
    expect(hasPushableConditions({ age: { $lte: 65 } })).toBe(true)
  })

  it('returns true for $in operator', () => {
    expect(hasPushableConditions({ status: { $in: ['a', 'b'] } })).toBe(true)
  })

  it('returns false for $regex only', () => {
    expect(hasPushableConditions({ name: { $regex: '^John' } })).toBe(false)
  })

  it('returns false for empty filter', () => {
    expect(hasPushableConditions({})).toBe(false)
  })

  it('returns false for null filter', () => {
    expect(hasPushableConditions(null as unknown as Filter)).toBe(false)
  })

  it('returns true for pushable condition in $and', () => {
    expect(hasPushableConditions({
      $and: [{ age: { $gte: 18 } }],
    })).toBe(true)
  })

  it('returns false for $or only (not recursively checked)', () => {
    // $or at top level doesn't trigger pushable check
    expect(hasPushableConditions({
      $or: [{ age: { $gte: 18 } }],
    })).toBe(false)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration: filterToPredicates + predicatesToQueryFilter', () => {
  it('roundtrips simple equality filter', () => {
    const original: Filter = { status: 'active' }
    const predicates = filterToPredicates(original)
    const queryFilter = predicatesToQueryFilter(predicates)

    expect(queryFilter).toEqual({ status: 'active' })
  })

  it('roundtrips range query', () => {
    const original: Filter = { age: { $gte: 18, $lt: 65 } }
    const predicates = filterToPredicates(original)
    const queryFilter = predicatesToQueryFilter(predicates)

    expect(queryFilter).toEqual({ age: { $gte: 18, $lt: 65 } })
  })

  it('roundtrips complex filter', () => {
    const original: Filter = {
      status: 'published',
      views: { $gte: 100 },
      rating: { $gt: 4 },
      category: { $in: ['tech', 'news'] },
    }
    const predicates = filterToPredicates(original)
    const queryFilter = predicatesToQueryFilter(predicates)

    expect(queryFilter).toEqual({
      status: 'published',
      views: { $gte: 100 },
      rating: { $gt: 4 },
      category: { $in: ['tech', 'news'] },
    })
  })

  it('preserves types in roundtrip', () => {
    const date = new Date('2024-01-15')
    const original: Filter = {
      count: 42,
      active: true,
      createdAt: date,
    }
    const predicates = filterToPredicates(original)
    const queryFilter = predicatesToQueryFilter(predicates)

    expect(queryFilter).toEqual({
      count: 42,
      active: true,
      createdAt: date,
    })
  })
})

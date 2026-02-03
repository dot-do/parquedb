/**
 * Tests for Materialized View Aggregations
 */

import { describe, it, expect } from 'vitest'
import {
  // Type guards
  isCountExpr,
  isSumExpr,
  isAvgExpr,
  isMinExpr,
  isMaxExpr,
  isFirstExpr,
  isLastExpr,
  isStdDevExpr,
  isTimeGrouping,
  isBucketGrouping,

  // Computation functions
  computeCount,
  computeSum,
  computeAvg,
  computeMin,
  computeMax,
  computeFirst,
  computeLast,
  computeStdDev,
  computeStdDevSamp,
  computeAggregate,

  // GroupBy functions
  computeGroupKey,
  groupDocuments,

  // Main executor
  executeMVAggregation,

  // Pipeline conversion
  toAggregationPipeline,

  // Incremental support
  isIncrementallyUpdatable,
  mergeAggregates,

  // Validation
  AggregationValidationError,
  validateMVAggregation,

  // Types
  type MVAggregationDefinition,
  type Document,
} from '../../../src/materialized-views/aggregations'

// =============================================================================
// Test Data
// =============================================================================

const sampleOrders: Document[] = [
  { $id: '1', status: 'completed', customer: 'alice', total: 100, createdAt: new Date('2024-01-15') },
  { $id: '2', status: 'completed', customer: 'bob', total: 200, createdAt: new Date('2024-01-15') },
  { $id: '3', status: 'pending', customer: 'alice', total: 150, createdAt: new Date('2024-01-16') },
  { $id: '4', status: 'completed', customer: 'charlie', total: 300, createdAt: new Date('2024-02-01') },
  { $id: '5', status: 'cancelled', customer: 'bob', total: 50, createdAt: new Date('2024-02-01') },
]

const sampleProducts: Document[] = [
  { $id: 'p1', category: 'electronics', price: 999, rating: 4.5 },
  { $id: 'p2', category: 'electronics', price: 499, rating: 4.2 },
  { $id: 'p3', category: 'clothing', price: 79, rating: 4.8 },
  { $id: 'p4', category: 'clothing', price: 129, rating: 3.9 },
  { $id: 'p5', category: 'books', price: 29, rating: 4.6 },
]

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isCountExpr', () => {
    it('returns true for valid count expressions', () => {
      expect(isCountExpr({ $count: '*' })).toBe(true)
      expect(isCountExpr({ $count: '$field' })).toBe(true)
    })

    it('returns false for invalid expressions', () => {
      expect(isCountExpr({ $sum: 1 })).toBe(false)
      expect(isCountExpr(null)).toBe(false)
      expect(isCountExpr({})).toBe(false)
    })
  })

  describe('isSumExpr', () => {
    it('returns true for valid sum expressions', () => {
      expect(isSumExpr({ $sum: '$total' })).toBe(true)
      expect(isSumExpr({ $sum: 1 })).toBe(true)
    })

    it('returns false for invalid expressions', () => {
      expect(isSumExpr({ $count: '*' })).toBe(false)
      expect(isSumExpr({ $sum: null })).toBe(false)
    })
  })

  describe('isAvgExpr', () => {
    it('returns true for valid avg expressions', () => {
      expect(isAvgExpr({ $avg: '$price' })).toBe(true)
    })

    it('returns false for invalid expressions', () => {
      expect(isAvgExpr({ $avg: 1 })).toBe(false)
      expect(isAvgExpr({ $sum: '$total' })).toBe(false)
    })
  })

  describe('isMinExpr', () => {
    it('returns true for valid min expressions', () => {
      expect(isMinExpr({ $min: '$price' })).toBe(true)
    })

    it('returns false for invalid expressions', () => {
      expect(isMinExpr({ $min: 1 })).toBe(false)
    })
  })

  describe('isMaxExpr', () => {
    it('returns true for valid max expressions', () => {
      expect(isMaxExpr({ $max: '$price' })).toBe(true)
    })

    it('returns false for invalid expressions', () => {
      expect(isMaxExpr({ $max: 1 })).toBe(false)
    })
  })

  describe('isTimeGrouping', () => {
    it('returns true for time grouping specs', () => {
      expect(isTimeGrouping({ $dateField: '$createdAt', $datePart: 'day' })).toBe(true)
    })

    it('returns false for other specs', () => {
      expect(isTimeGrouping('$status')).toBe(false)
      expect(isTimeGrouping({ $field: '$price', $boundaries: [0, 100] })).toBe(false)
    })
  })

  describe('isBucketGrouping', () => {
    it('returns true for bucket grouping specs', () => {
      expect(isBucketGrouping({ $field: '$price', $boundaries: [0, 100, 500] })).toBe(true)
    })

    it('returns false for other specs', () => {
      expect(isBucketGrouping('$status')).toBe(false)
      expect(isBucketGrouping({ $dateField: '$createdAt', $datePart: 'day' })).toBe(false)
    })
  })
})

// =============================================================================
// Computation Function Tests
// =============================================================================

describe('Computation Functions', () => {
  describe('computeCount', () => {
    it('counts all documents with $count: "*"', () => {
      expect(computeCount(sampleOrders, { $count: '*' })).toBe(5)
    })

    it('counts non-null values for a field', () => {
      const docs = [
        { value: 1 },
        { value: null },
        { value: 2 },
        { other: 3 },
      ]
      expect(computeCount(docs, { $count: '$value' })).toBe(2)
    })

    it('returns 0 for empty array', () => {
      expect(computeCount([], { $count: '*' })).toBe(0)
    })
  })

  describe('computeSum', () => {
    it('sums field values', () => {
      expect(computeSum(sampleOrders, { $sum: '$total' })).toBe(800)
    })

    it('counts when $sum: 1', () => {
      expect(computeSum(sampleOrders, { $sum: 1 })).toBe(5)
    })

    it('multiplies count by constant', () => {
      expect(computeSum(sampleOrders, { $sum: 10 })).toBe(50)
    })

    it('ignores non-numeric values', () => {
      const docs = [{ val: 10 }, { val: 'string' }, { val: 20 }]
      expect(computeSum(docs, { $sum: '$val' })).toBe(30)
    })

    it('returns 0 for empty array', () => {
      expect(computeSum([], { $sum: '$total' })).toBe(0)
    })
  })

  describe('computeAvg', () => {
    it('computes average of field values', () => {
      expect(computeAvg(sampleOrders, { $avg: '$total' })).toBe(160)
    })

    it('returns null for empty array', () => {
      expect(computeAvg([], { $avg: '$total' })).toBeNull()
    })

    it('ignores non-numeric values', () => {
      const docs = [{ val: 10 }, { val: 'string' }, { val: 30 }]
      expect(computeAvg(docs, { $avg: '$val' })).toBe(20)
    })
  })

  describe('computeMin', () => {
    it('finds minimum value', () => {
      expect(computeMin(sampleOrders, { $min: '$total' })).toBe(50)
    })

    it('returns null for empty array', () => {
      expect(computeMin([], { $min: '$total' })).toBeNull()
    })

    it('handles string values', () => {
      const docs = [{ name: 'charlie' }, { name: 'alice' }, { name: 'bob' }]
      expect(computeMin(docs, { $min: '$name' })).toBe('alice')
    })
  })

  describe('computeMax', () => {
    it('finds maximum value', () => {
      expect(computeMax(sampleOrders, { $max: '$total' })).toBe(300)
    })

    it('returns null for empty array', () => {
      expect(computeMax([], { $max: '$total' })).toBeNull()
    })

    it('handles string values', () => {
      const docs = [{ name: 'alice' }, { name: 'charlie' }, { name: 'bob' }]
      expect(computeMax(docs, { $max: '$name' })).toBe('charlie')
    })
  })

  describe('computeFirst', () => {
    it('returns first value', () => {
      expect(computeFirst(sampleOrders, { $first: '$status' })).toBe('completed')
    })

    it('returns null for empty array', () => {
      expect(computeFirst([], { $first: '$status' })).toBeNull()
    })
  })

  describe('computeLast', () => {
    it('returns last value', () => {
      expect(computeLast(sampleOrders, { $last: '$status' })).toBe('cancelled')
    })

    it('returns null for empty array', () => {
      expect(computeLast([], { $last: '$status' })).toBeNull()
    })
  })

  describe('computeStdDev', () => {
    it('computes population standard deviation', () => {
      const docs = [{ val: 2 }, { val: 4 }, { val: 4 }, { val: 4 }, { val: 5 }, { val: 5 }, { val: 7 }, { val: 9 }]
      const stdDev = computeStdDev(docs, '$val')
      expect(stdDev).toBeCloseTo(2, 1)
    })

    it('returns null for empty array', () => {
      expect(computeStdDev([], '$val')).toBeNull()
    })
  })

  describe('computeStdDevSamp', () => {
    it('computes sample standard deviation', () => {
      const docs = [{ val: 2 }, { val: 4 }, { val: 4 }, { val: 4 }, { val: 5 }, { val: 5 }, { val: 7 }, { val: 9 }]
      const stdDev = computeStdDevSamp(docs, '$val')
      expect(stdDev).toBeCloseTo(2.14, 1)
    })

    it('returns null for single element', () => {
      expect(computeStdDevSamp([{ val: 5 }], '$val')).toBeNull()
    })
  })

  describe('computeAggregate', () => {
    it('dispatches to correct computation function', () => {
      expect(computeAggregate(sampleOrders, { $count: '*' })).toBe(5)
      expect(computeAggregate(sampleOrders, { $sum: '$total' })).toBe(800)
      expect(computeAggregate(sampleOrders, { $avg: '$total' })).toBe(160)
      expect(computeAggregate(sampleOrders, { $min: '$total' })).toBe(50)
      expect(computeAggregate(sampleOrders, { $max: '$total' })).toBe(300)
    })
  })
})

// =============================================================================
// GroupBy Tests
// =============================================================================

describe('GroupBy Functions', () => {
  describe('computeGroupKey', () => {
    it('computes key for simple field grouping', () => {
      const doc = { status: 'completed', customer: 'alice' }
      const key = computeGroupKey(doc, ['status'])
      expect(key).toEqual({ status: 'completed' })
    })

    it('computes key for multiple fields', () => {
      const doc = { status: 'completed', customer: 'alice' }
      const key = computeGroupKey(doc, ['status', 'customer'])
      expect(key).toEqual({ status: 'completed', customer: 'alice' })
    })

    it('computes key for time grouping', () => {
      const doc = { createdAt: new Date('2024-01-15') }
      const key = computeGroupKey(doc, [
        { $dateField: '$createdAt', $datePart: 'month', $as: 'month' },
      ])
      expect(key).toEqual({ month: 1 })
    })

    it('computes key for bucket grouping', () => {
      const doc = { price: 150 }
      const key = computeGroupKey(doc, [
        { $field: '$price', $boundaries: [0, 100, 500, 1000], $as: 'priceRange' },
      ])
      expect(key).toEqual({ priceRange: 100 })
    })
  })

  describe('groupDocuments', () => {
    it('groups documents by single field', () => {
      const groups = groupDocuments(sampleOrders, ['status'])
      expect(groups.size).toBe(3)
      expect(groups.get(JSON.stringify({ status: 'completed' }))?.docs.length).toBe(3)
      expect(groups.get(JSON.stringify({ status: 'pending' }))?.docs.length).toBe(1)
      expect(groups.get(JSON.stringify({ status: 'cancelled' }))?.docs.length).toBe(1)
    })

    it('groups documents by multiple fields', () => {
      const groups = groupDocuments(sampleOrders, ['status', 'customer'])
      expect(groups.size).toBe(5)
    })
  })
})

// =============================================================================
// Main Executor Tests
// =============================================================================

describe('executeMVAggregation', () => {
  it('computes aggregates without groupBy', () => {
    const definition: MVAggregationDefinition = {
      compute: {
        totalOrders: { $count: '*' },
        totalRevenue: { $sum: '$total' },
        avgOrder: { $avg: '$total' },
      },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents).toHaveLength(1)
    expect(result.documents[0]).toMatchObject({
      _id: null,
      totalOrders: 5,
      totalRevenue: 800,
      avgOrder: 160,
    })
    expect(result.stats.inputCount).toBe(5)
    expect(result.stats.groupCount).toBe(1)
  })

  it('computes aggregates with groupBy', () => {
    const definition: MVAggregationDefinition = {
      groupBy: ['status'],
      compute: {
        count: { $count: '*' },
        total: { $sum: '$total' },
      },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents).toHaveLength(3)
    expect(result.stats.groupCount).toBe(3)

    const completedGroup = result.documents.find(d => d.status === 'completed')
    expect(completedGroup).toMatchObject({ count: 3, total: 600 })
  })

  it('applies $match filter before aggregation', () => {
    const definition: MVAggregationDefinition = {
      match: { status: 'completed' },
      groupBy: ['customer'],
      compute: {
        count: { $count: '*' },
        total: { $sum: '$total' },
      },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.stats.matchedCount).toBe(3)
    expect(result.documents).toHaveLength(3) // alice, bob, charlie
  })

  it('applies $having filter after aggregation', () => {
    const definition: MVAggregationDefinition = {
      groupBy: ['status'],
      compute: {
        count: { $count: '*' },
        total: { $sum: '$total' },
      },
      having: { count: { $gt: 1 } },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents).toHaveLength(1) // Only 'completed' has count > 1
    expect(result.documents[0]?.status).toBe('completed')
  })

  it('applies sort to results', () => {
    const definition: MVAggregationDefinition = {
      groupBy: ['status'],
      compute: {
        total: { $sum: '$total' },
      },
      sort: { total: -1 },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents[0]?.status).toBe('completed') // 600
    expect(result.documents[1]?.status).toBe('pending') // 150
    expect(result.documents[2]?.status).toBe('cancelled') // 50
  })

  it('applies limit to results', () => {
    const definition: MVAggregationDefinition = {
      groupBy: ['status'],
      compute: {
        total: { $sum: '$total' },
      },
      sort: { total: -1 },
      limit: 2,
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents).toHaveLength(2)
  })

  it('handles time-based grouping', () => {
    const definition: MVAggregationDefinition = {
      groupBy: [{ $dateField: '$createdAt', $datePart: 'month', $as: 'month' }],
      compute: {
        count: { $count: '*' },
      },
    }

    const result = executeMVAggregation(sampleOrders, definition)
    expect(result.documents).toHaveLength(2) // January and February
  })
})

// =============================================================================
// Pipeline Conversion Tests
// =============================================================================

describe('toAggregationPipeline', () => {
  it('converts simple aggregation to pipeline', () => {
    const definition: MVAggregationDefinition = {
      compute: {
        count: { $count: '*' },
        total: { $sum: '$total' },
      },
    }

    const pipeline = toAggregationPipeline(definition)
    expect(pipeline).toHaveLength(1)
    expect(pipeline[0]).toHaveProperty('$group')
  })

  it('includes $match stage when match is defined', () => {
    const definition: MVAggregationDefinition = {
      match: { status: 'completed' },
      compute: {
        count: { $count: '*' },
      },
    }

    const pipeline = toAggregationPipeline(definition)
    expect(pipeline).toHaveLength(2)
    expect(pipeline[0]).toHaveProperty('$match')
  })

  it('includes $match stage for having clause', () => {
    const definition: MVAggregationDefinition = {
      compute: {
        count: { $count: '*' },
      },
      having: { count: { $gt: 5 } },
    }

    const pipeline = toAggregationPipeline(definition)
    expect(pipeline).toHaveLength(2)
    expect(pipeline[1]).toHaveProperty('$match', { count: { $gt: 5 } })
  })

  it('includes $sort and $limit stages', () => {
    const definition: MVAggregationDefinition = {
      compute: {
        count: { $count: '*' },
      },
      sort: { count: -1 },
      limit: 10,
    }

    const pipeline = toAggregationPipeline(definition)
    expect(pipeline).toHaveLength(3)
    expect(pipeline[1]).toHaveProperty('$sort')
    expect(pipeline[2]).toHaveProperty('$limit', 10)
  })
})

// =============================================================================
// Incremental Support Tests
// =============================================================================

describe('Incremental Support', () => {
  describe('isIncrementallyUpdatable', () => {
    it('returns true for sum, count, min, max, avg', () => {
      const definition: MVAggregationDefinition = {
        compute: {
          count: { $count: '*' },
          total: { $sum: '$total' },
          avg: { $avg: '$total' },
          min: { $min: '$total' },
          max: { $max: '$total' },
        },
      }
      expect(isIncrementallyUpdatable(definition)).toBe(true)
    })

    it('returns false for first, last, stdDev', () => {
      expect(isIncrementallyUpdatable({
        compute: { first: { $first: '$val' } },
      })).toBe(false)

      expect(isIncrementallyUpdatable({
        compute: { last: { $last: '$val' } },
      })).toBe(false)

      expect(isIncrementallyUpdatable({
        compute: { stdDev: { $stdDev: '$val' } },
      })).toBe(false)
    })
  })

  describe('mergeAggregates', () => {
    it('merges count aggregates', () => {
      const existing = { count: 10 }
      const delta = { count: 5 }
      const compute = { count: { $count: '*' } as const }

      const merged = mergeAggregates(existing, delta, compute)
      expect(merged.count).toBe(15)
    })

    it('merges sum aggregates', () => {
      const existing = { total: 100 }
      const delta = { total: 50 }
      const compute = { total: { $sum: '$price' } as const }

      const merged = mergeAggregates(existing, delta, compute)
      expect(merged.total).toBe(150)
    })

    it('merges min aggregates', () => {
      const existing = { minPrice: 10 }
      const delta = { minPrice: 5 }
      const compute = { minPrice: { $min: '$price' } as const }

      const merged = mergeAggregates(existing, delta, compute)
      expect(merged.minPrice).toBe(5)
    })

    it('merges max aggregates', () => {
      const existing = { maxPrice: 100 }
      const delta = { maxPrice: 150 }
      const compute = { maxPrice: { $max: '$price' } as const }

      const merged = mergeAggregates(existing, delta, compute)
      expect(merged.maxPrice).toBe(150)
    })
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('Validation', () => {
  describe('validateMVAggregation', () => {
    it('throws for empty compute', () => {
      expect(() => validateMVAggregation({ compute: {} })).toThrow(AggregationValidationError)
    })

    it('throws for invalid aggregate expression', () => {
      expect(() =>
        validateMVAggregation({
          compute: { invalid: { $invalid: '$field' } as any },
        })
      ).toThrow(AggregationValidationError)
    })

    it('passes for valid definition', () => {
      expect(() =>
        validateMVAggregation({
          groupBy: ['status'],
          compute: {
            count: { $count: '*' },
            total: { $sum: '$price' },
          },
          having: { count: { $gt: 0 } },
          sort: { total: -1 },
        })
      ).not.toThrow()
    })
  })
})

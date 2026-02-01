/**
 * Aggregation Types Tests
 *
 * Tests for type guards and type definitions
 */

import { describe, it, expect } from 'vitest'
import {
  isMatchStage,
  isGroupStage,
  isSortStage,
  isLimitStage,
  isSkipStage,
  isProjectStage,
  isUnwindStage,
  isLookupStage,
  isCountStage,
  isAddFieldsStage,
  isSetStage,
  isUnsetStage,
  isReplaceRootStage,
  isFacetStage,
  isBucketStage,
  isSampleStage,
  type AggregationStage,
} from '../../../src/aggregation'

describe('Type Guards', () => {
  describe('isMatchStage', () => {
    it('should return true for $match stage', () => {
      const stage: AggregationStage = { $match: { status: 'active' } }
      expect(isMatchStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $limit: 10 }
      expect(isMatchStage(stage)).toBe(false)
    })
  })

  describe('isGroupStage', () => {
    it('should return true for $group stage', () => {
      const stage: AggregationStage = { $group: { _id: '$status', count: { $sum: 1 } } }
      expect(isGroupStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: { status: 'active' } }
      expect(isGroupStage(stage)).toBe(false)
    })
  })

  describe('isSortStage', () => {
    it('should return true for $sort stage', () => {
      const stage: AggregationStage = { $sort: { createdAt: -1 } }
      expect(isSortStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $limit: 10 }
      expect(isSortStage(stage)).toBe(false)
    })
  })

  describe('isLimitStage', () => {
    it('should return true for $limit stage', () => {
      const stage: AggregationStage = { $limit: 10 }
      expect(isLimitStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $skip: 5 }
      expect(isLimitStage(stage)).toBe(false)
    })
  })

  describe('isSkipStage', () => {
    it('should return true for $skip stage', () => {
      const stage: AggregationStage = { $skip: 5 }
      expect(isSkipStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $limit: 10 }
      expect(isSkipStage(stage)).toBe(false)
    })
  })

  describe('isProjectStage', () => {
    it('should return true for $project stage', () => {
      const stage: AggregationStage = { $project: { title: 1, status: 1 } }
      expect(isProjectStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isProjectStage(stage)).toBe(false)
    })
  })

  describe('isUnwindStage', () => {
    it('should return true for $unwind stage with string', () => {
      const stage: AggregationStage = { $unwind: '$tags' }
      expect(isUnwindStage(stage)).toBe(true)
    })

    it('should return true for $unwind stage with options', () => {
      const stage: AggregationStage = { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } }
      expect(isUnwindStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isUnwindStage(stage)).toBe(false)
    })
  })

  describe('isLookupStage', () => {
    it('should return true for $lookup stage', () => {
      const stage: AggregationStage = {
        $lookup: {
          from: 'users',
          localField: 'authorId',
          foreignField: '_id',
          as: 'author',
        },
      }
      expect(isLookupStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isLookupStage(stage)).toBe(false)
    })
  })

  describe('isCountStage', () => {
    it('should return true for $count stage', () => {
      const stage: AggregationStage = { $count: 'total' }
      expect(isCountStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isCountStage(stage)).toBe(false)
    })
  })

  describe('isAddFieldsStage', () => {
    it('should return true for $addFields stage', () => {
      const stage: AggregationStage = { $addFields: { isNew: true } }
      expect(isAddFieldsStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $set: { isNew: true } }
      expect(isAddFieldsStage(stage)).toBe(false)
    })
  })

  describe('isSetStage', () => {
    it('should return true for $set stage', () => {
      const stage: AggregationStage = { $set: { isNew: true } }
      expect(isSetStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $addFields: { isNew: true } }
      expect(isSetStage(stage)).toBe(false)
    })
  })

  describe('isUnsetStage', () => {
    it('should return true for $unset stage with string', () => {
      const stage: AggregationStage = { $unset: 'password' }
      expect(isUnsetStage(stage)).toBe(true)
    })

    it('should return true for $unset stage with array', () => {
      const stage: AggregationStage = { $unset: ['password', 'secret'] }
      expect(isUnsetStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isUnsetStage(stage)).toBe(false)
    })
  })

  describe('isReplaceRootStage', () => {
    it('should return true for $replaceRoot stage', () => {
      const stage: AggregationStage = { $replaceRoot: { newRoot: '$nested' } }
      expect(isReplaceRootStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isReplaceRootStage(stage)).toBe(false)
    })
  })

  describe('isFacetStage', () => {
    it('should return true for $facet stage', () => {
      const stage: AggregationStage = {
        $facet: {
          byStatus: [{ $group: { _id: '$status', count: { $sum: 1 } } }],
          recent: [{ $sort: { createdAt: -1 } }, { $limit: 5 }],
        },
      }
      expect(isFacetStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isFacetStage(stage)).toBe(false)
    })
  })

  describe('isBucketStage', () => {
    it('should return true for $bucket stage', () => {
      const stage: AggregationStage = {
        $bucket: {
          groupBy: '$price',
          boundaries: [0, 100, 200, 500],
          default: 'Other',
        },
      }
      expect(isBucketStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isBucketStage(stage)).toBe(false)
    })
  })

  describe('isSampleStage', () => {
    it('should return true for $sample stage', () => {
      const stage: AggregationStage = { $sample: { size: 5 } }
      expect(isSampleStage(stage)).toBe(true)
    })

    it('should return false for other stages', () => {
      const stage: AggregationStage = { $match: {} }
      expect(isSampleStage(stage)).toBe(false)
    })
  })
})

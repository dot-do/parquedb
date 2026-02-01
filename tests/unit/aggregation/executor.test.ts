/**
 * Aggregation Executor Tests
 *
 * Tests for the aggregation pipeline executor including:
 * - Pipeline stages ($match, $group, $sort, $project, etc.)
 * - Accumulator operators ($sum, $avg, $min, $max, etc.)
 * - Expression evaluation
 * - Edge cases and error handling
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  executeAggregation,
  AggregationExecutor,
  type AggregationStage,
} from '../../../src/aggregation'

// =============================================================================
// Test Data
// =============================================================================

interface Post {
  $id: string
  $type: string
  name: string
  title: string
  status: 'draft' | 'published' | 'archived'
  views: number
  tags: string[]
  author: string
  createdAt: Date
}

function createTestData(): Post[] {
  return [
    {
      $id: 'posts/1',
      $type: 'Post',
      name: 'post-1',
      title: 'Introduction to TypeScript',
      status: 'published',
      views: 1500,
      tags: ['tech', 'typescript', 'tutorial'],
      author: 'alice',
      createdAt: new Date('2024-01-15'),
    },
    {
      $id: 'posts/2',
      $type: 'Post',
      name: 'post-2',
      title: 'Advanced React Patterns',
      status: 'published',
      views: 2500,
      tags: ['tech', 'react', 'patterns'],
      author: 'bob',
      createdAt: new Date('2024-02-20'),
    },
    {
      $id: 'posts/3',
      $type: 'Post',
      name: 'post-3',
      title: 'Draft Post',
      status: 'draft',
      views: 100,
      tags: ['draft'],
      author: 'alice',
      createdAt: new Date('2024-03-01'),
    },
    {
      $id: 'posts/4',
      $type: 'Post',
      name: 'post-4',
      title: 'Database Design',
      status: 'published',
      views: 800,
      tags: ['tech', 'database'],
      author: 'charlie',
      createdAt: new Date('2024-01-30'),
    },
    {
      $id: 'posts/5',
      $type: 'Post',
      name: 'post-5',
      title: 'Archived Content',
      status: 'archived',
      views: 50,
      tags: ['old'],
      author: 'bob',
      createdAt: new Date('2023-06-15'),
    },
  ]
}

// =============================================================================
// executeAggregation Tests
// =============================================================================

describe('executeAggregation', () => {
  let testData: Post[]

  beforeEach(() => {
    testData = createTestData()
  })

  // ===========================================================================
  // $match Stage
  // ===========================================================================

  describe('$match stage', () => {
    it('should filter documents by equality', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published' } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(3)
      expect(results.every((r: any) => r.status === 'published')).toBe(true)
    })

    it('should filter with comparison operators', () => {
      const pipeline: AggregationStage[] = [
        { $match: { views: { $gt: 1000 } } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(2)
      expect(results.every((r: any) => r.views > 1000)).toBe(true)
    })

    it('should filter with $in operator', () => {
      const pipeline: AggregationStage[] = [
        { $match: { author: { $in: ['alice', 'bob'] } } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(4)
    })

    it('should filter with multiple conditions (implicit $and)', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published', views: { $gte: 1000 } } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(2)
    })

    it('should filter with $or operator', () => {
      const pipeline: AggregationStage[] = [
        {
          $match: {
            $or: [
              { status: 'draft' },
              { views: { $gt: 2000 } },
            ],
          },
        },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(2)
    })

    it('should return empty array when no documents match', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'nonexistent' } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(0)
    })
  })

  // ===========================================================================
  // $group Stage
  // ===========================================================================

  describe('$group stage', () => {
    it('should group by field', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ]

      const results = executeAggregation<{ _id: string; count: number }>(testData, pipeline)

      expect(results).toHaveLength(3) // published, draft, archived
      const published = results.find(r => r._id === 'published')
      expect(published?.count).toBe(3)
    })

    it('should group all documents with _id: null', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: null, totalViews: { $sum: '$views' } } },
      ]

      const results = executeAggregation<{ _id: null; totalViews: number }>(testData, pipeline)

      expect(results).toHaveLength(1)
      expect(results[0]._id).toBeNull()
      expect(results[0].totalViews).toBe(1500 + 2500 + 100 + 800 + 50)
    })

    it('should calculate $sum of field values', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$author', totalViews: { $sum: '$views' } } },
      ]

      const results = executeAggregation<{ _id: string; totalViews: number }>(testData, pipeline)

      const alice = results.find(r => r._id === 'alice')
      expect(alice?.totalViews).toBe(1500 + 100) // posts 1 and 3
    })

    it('should calculate $avg', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', avgViews: { $avg: '$views' } } },
      ]

      const results = executeAggregation<{ _id: string; avgViews: number }>(testData, pipeline)

      const published = results.find(r => r._id === 'published')
      expect(published?.avgViews).toBe((1500 + 2500 + 800) / 3)
    })

    it('should calculate $min', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', minViews: { $min: '$views' } } },
      ]

      const results = executeAggregation<{ _id: string; minViews: number }>(testData, pipeline)

      const published = results.find(r => r._id === 'published')
      expect(published?.minViews).toBe(800)
    })

    it('should calculate $max', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', maxViews: { $max: '$views' } } },
      ]

      const results = executeAggregation<{ _id: string; maxViews: number }>(testData, pipeline)

      const published = results.find(r => r._id === 'published')
      expect(published?.maxViews).toBe(2500)
    })

    it('should use $first accumulator', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', firstTitle: { $first: '$title' } } },
      ]

      const results = executeAggregation<{ _id: string; firstTitle: string }>(testData, pipeline)

      expect(results.every(r => typeof r.firstTitle === 'string')).toBe(true)
    })

    it('should use $last accumulator', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$status', lastTitle: { $last: '$title' } } },
      ]

      const results = executeAggregation<{ _id: string; lastTitle: string }>(testData, pipeline)

      expect(results.every(r => typeof r.lastTitle === 'string')).toBe(true)
    })

    it('should use $push accumulator', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: '$author', titles: { $push: '$title' } } },
      ]

      const results = executeAggregation<{ _id: string; titles: string[] }>(testData, pipeline)

      const alice = results.find(r => r._id === 'alice')
      expect(alice?.titles).toHaveLength(2)
      expect(alice?.titles).toContain('Introduction to TypeScript')
    })

    it('should use $addToSet accumulator', () => {
      const pipeline: AggregationStage[] = [
        { $group: { _id: null, uniqueStatuses: { $addToSet: '$status' } } },
      ]

      const results = executeAggregation<{ _id: null; uniqueStatuses: string[] }>(testData, pipeline)

      expect(results[0].uniqueStatuses).toHaveLength(3)
    })

    it('should support multiple accumulators', () => {
      const pipeline: AggregationStage[] = [
        {
          $group: {
            _id: '$status',
            count: { $sum: 1 },
            totalViews: { $sum: '$views' },
            avgViews: { $avg: '$views' },
          },
        },
      ]

      const results = executeAggregation<{
        _id: string
        count: number
        totalViews: number
        avgViews: number
      }>(testData, pipeline)

      const published = results.find(r => r._id === 'published')
      expect(published?.count).toBe(3)
      expect(published?.totalViews).toBe(4800)
      expect(published?.avgViews).toBe(1600)
    })
  })

  // ===========================================================================
  // $sort Stage
  // ===========================================================================

  describe('$sort stage', () => {
    it('should sort ascending', () => {
      const pipeline: AggregationStage[] = [
        { $sort: { views: 1 } },
      ]

      const results = executeAggregation<Post>(testData, pipeline)

      for (let i = 1; i < results.length; i++) {
        expect(results[i].views).toBeGreaterThanOrEqual(results[i - 1].views)
      }
    })

    it('should sort descending', () => {
      const pipeline: AggregationStage[] = [
        { $sort: { views: -1 } },
      ]

      const results = executeAggregation<Post>(testData, pipeline)

      for (let i = 1; i < results.length; i++) {
        expect(results[i].views).toBeLessThanOrEqual(results[i - 1].views)
      }
    })

    it('should sort by multiple fields', () => {
      const pipeline: AggregationStage[] = [
        { $sort: { status: 1, views: -1 } },
      ]

      const results = executeAggregation<Post>(testData, pipeline)

      // Archived comes first (alphabetically), then draft, then published
      expect(results[0].status).toBe('archived')
      expect(results[1].status).toBe('draft')
    })

    it('should handle string sorting', () => {
      const pipeline: AggregationStage[] = [
        { $sort: { author: 1 } },
      ]

      const results = executeAggregation<Post>(testData, pipeline)

      expect(results[0].author).toBe('alice')
    })
  })

  // ===========================================================================
  // $limit and $skip Stages
  // ===========================================================================

  describe('$limit stage', () => {
    it('should limit results', () => {
      const pipeline: AggregationStage[] = [
        { $limit: 2 },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(2)
    })

    it('should return all if limit exceeds count', () => {
      const pipeline: AggregationStage[] = [
        { $limit: 100 },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(5)
    })
  })

  describe('$skip stage', () => {
    it('should skip documents', () => {
      const pipeline: AggregationStage[] = [
        { $skip: 2 },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(3)
    })

    it('should return empty if skip exceeds count', () => {
      const pipeline: AggregationStage[] = [
        { $skip: 100 },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(0)
    })
  })

  describe('$skip and $limit together', () => {
    it('should paginate results', () => {
      const pipeline: AggregationStage[] = [
        { $sort: { views: -1 } },
        { $skip: 1 },
        { $limit: 2 },
      ]

      const results = executeAggregation<Post>(testData, pipeline)

      expect(results).toHaveLength(2)
      expect(results[0].views).toBe(1500) // Second highest after skip
    })
  })

  // ===========================================================================
  // $project Stage
  // ===========================================================================

  describe('$project stage', () => {
    it('should include specified fields', () => {
      const pipeline: AggregationStage[] = [
        { $project: { title: 1, views: 1 } },
      ]

      const results = executeAggregation<{ title: string; views: number }>(testData, pipeline)

      expect(results[0]).toHaveProperty('title')
      expect(results[0]).toHaveProperty('views')
      expect(results[0]).not.toHaveProperty('status')
    })

    it('should exclude specified fields', () => {
      const pipeline: AggregationStage[] = [
        { $project: { tags: 0, createdAt: 0 } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results[0]).toHaveProperty('title')
      expect(results[0]).not.toHaveProperty('tags')
      expect(results[0]).not.toHaveProperty('createdAt')
    })
  })

  // ===========================================================================
  // $unwind Stage
  // ===========================================================================

  describe('$unwind stage', () => {
    it('should unwind array field', () => {
      const pipeline: AggregationStage[] = [
        { $match: { name: 'post-1' } },
        { $unwind: '$tags' },
      ]

      const results = executeAggregation<Post & { tags: string }>(testData, pipeline)

      expect(results).toHaveLength(3) // 3 tags
      expect(results.every(r => typeof r.tags === 'string')).toBe(true)
    })

    it('should exclude empty arrays by default', () => {
      const dataWithEmpty = [
        ...testData,
        {
          $id: 'posts/6',
          $type: 'Post',
          name: 'post-6',
          title: 'No Tags',
          status: 'draft' as const,
          views: 0,
          tags: [],
          author: 'nobody',
          createdAt: new Date(),
        },
      ]

      const pipeline: AggregationStage[] = [
        { $unwind: '$tags' },
      ]

      const results = executeAggregation(dataWithEmpty, pipeline)

      // Should not include the empty-tags document
      expect(results.every((r: any) => r.tags !== undefined && r.tags !== null)).toBe(true)
    })

    it('should preserve null/empty with option', () => {
      const dataWithEmpty = [
        ...testData,
        {
          $id: 'posts/6',
          $type: 'Post',
          name: 'post-6',
          title: 'No Tags',
          status: 'draft' as const,
          views: 0,
          tags: [],
          author: 'nobody',
          createdAt: new Date(),
        },
      ]

      const pipeline: AggregationStage[] = [
        { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } },
      ]

      const results = executeAggregation(dataWithEmpty, pipeline)

      // Should include the empty-tags document
      expect(results.some((r: any) => r.name === 'post-6')).toBe(true)
    })
  })

  // ===========================================================================
  // $count Stage
  // ===========================================================================

  describe('$count stage', () => {
    it('should count documents', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published' } },
        { $count: 'publishedCount' },
      ]

      const results = executeAggregation<{ publishedCount: number }>(testData, pipeline)

      expect(results).toHaveLength(1)
      expect(results[0].publishedCount).toBe(3)
    })

    it('should count all documents', () => {
      const pipeline: AggregationStage[] = [
        { $count: 'total' },
      ]

      const results = executeAggregation<{ total: number }>(testData, pipeline)

      expect(results[0].total).toBe(5)
    })
  })

  // ===========================================================================
  // $addFields and $set Stages
  // ===========================================================================

  describe('$addFields stage', () => {
    it('should add new fields', () => {
      const pipeline: AggregationStage[] = [
        { $addFields: { isPopular: true, category: 'blog' } },
      ]

      const results = executeAggregation<Post & { isPopular: boolean; category: string }>(testData, pipeline)

      expect(results.every(r => r.isPopular === true)).toBe(true)
      expect(results.every(r => r.category === 'blog')).toBe(true)
    })
  })

  describe('$set stage', () => {
    it('should work as alias for $addFields', () => {
      const pipeline: AggregationStage[] = [
        { $set: { processed: true } },
      ]

      const results = executeAggregation<Post & { processed: boolean }>(testData, pipeline)

      expect(results.every(r => r.processed === true)).toBe(true)
    })
  })

  // ===========================================================================
  // $unset Stage
  // ===========================================================================

  describe('$unset stage', () => {
    it('should remove single field', () => {
      const pipeline: AggregationStage[] = [
        { $unset: 'tags' },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results.every((r: any) => !('tags' in r))).toBe(true)
    })

    it('should remove multiple fields', () => {
      const pipeline: AggregationStage[] = [
        { $unset: ['tags', 'createdAt'] },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results.every((r: any) => !('tags' in r) && !('createdAt' in r))).toBe(true)
    })
  })

  // ===========================================================================
  // $sample Stage
  // ===========================================================================

  describe('$sample stage', () => {
    it('should return specified number of documents', () => {
      const pipeline: AggregationStage[] = [
        { $sample: { size: 2 } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(2)
    })

    it('should return all if size exceeds count', () => {
      const pipeline: AggregationStage[] = [
        { $sample: { size: 100 } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(5)
    })
  })

  // ===========================================================================
  // $facet Stage
  // ===========================================================================

  describe('$facet stage', () => {
    it('should execute multiple pipelines', () => {
      const pipeline: AggregationStage[] = [
        {
          $facet: {
            byStatus: [
              { $group: { _id: '$status', count: { $sum: 1 } } },
            ],
            topViews: [
              { $sort: { views: -1 } },
              { $limit: 2 },
            ],
          },
        },
      ]

      const results = executeAggregation<{
        byStatus: { _id: string; count: number }[]
        topViews: Post[]
      }>(testData, pipeline)

      expect(results).toHaveLength(1)
      expect(results[0].byStatus).toHaveLength(3)
      expect(results[0].topViews).toHaveLength(2)
    })
  })

  // ===========================================================================
  // Complex Pipelines
  // ===========================================================================

  describe('complex pipelines', () => {
    it('should execute multi-stage analytics pipeline', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published' } },
        { $group: { _id: '$author', totalViews: { $sum: '$views' }, postCount: { $sum: 1 } } },
        { $sort: { totalViews: -1 } },
        { $limit: 5 },
      ]

      const results = executeAggregation<{
        _id: string
        totalViews: number
        postCount: number
      }>(testData, pipeline)

      expect(results[0].totalViews).toBeGreaterThanOrEqual(results[1]?.totalViews ?? 0)
    })

    it('should execute tag frequency analysis', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published' } },
        { $unwind: '$tags' },
        { $group: { _id: '$tags', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ]

      const results = executeAggregation<{ _id: string; count: number }>(testData, pipeline)

      expect(results[0]._id).toBe('tech') // Most common tag in published posts
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle empty input', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'published' } },
      ]

      const results = executeAggregation([], pipeline)

      expect(results).toHaveLength(0)
    })

    it('should handle empty pipeline', () => {
      const results = executeAggregation(testData, [])

      expect(results).toHaveLength(5)
    })

    it('should handle $group with no matching documents', () => {
      const pipeline: AggregationStage[] = [
        { $match: { status: 'nonexistent' } },
        { $group: { _id: '$author', count: { $sum: 1 } } },
      ]

      const results = executeAggregation(testData, pipeline)

      expect(results).toHaveLength(0)
    })
  })
})

// =============================================================================
// AggregationExecutor Class Tests
// =============================================================================

describe('AggregationExecutor', () => {
  let testData: Post[]

  beforeEach(() => {
    testData = createTestData()
  })

  it('should execute pipeline', () => {
    const executor = new AggregationExecutor(testData, [
      { $match: { status: 'published' } },
    ])

    const results = executor.execute<Post>()

    expect(results).toHaveLength(3)
  })

  it('should support adding stages', () => {
    const executor = new AggregationExecutor(testData, [])
      .addStage({ $match: { status: 'published' } })
      .addStage({ $sort: { views: -1 } })
      .addStage({ $limit: 2 })

    const results = executor.execute<Post>()

    expect(results).toHaveLength(2)
    expect(results[0].views).toBe(2500)
  })

  it('should return pipeline', () => {
    const pipeline: AggregationStage[] = [
      { $match: { status: 'published' } },
      { $limit: 5 },
    ]
    const executor = new AggregationExecutor(testData, pipeline)

    expect(executor.getPipeline()).toEqual(pipeline)
  })

  it('should explain pipeline execution', () => {
    const executor = new AggregationExecutor(testData, [
      { $match: { status: 'published' } },
      { $sort: { views: -1 } },
      { $limit: 2 },
    ])

    const explain = executor.explain()

    expect(explain.stages).toHaveLength(3)
    expect(explain.stages[0].name).toBe('$match')
    expect(explain.stages[0].inputCount).toBe(5)
    expect(explain.stages[0].outputCount).toBe(3)
    expect(explain.totalDocuments).toBe(2)
  })
})

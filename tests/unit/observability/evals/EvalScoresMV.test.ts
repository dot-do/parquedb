/**
 * Tests for EvalScoresMV - Eval Scores Materialized View
 *
 * Tests the aggregation of evaluation scores, trend tracking,
 * and query capabilities of the EvalScoresMV class.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  EvalScoresMV,
  createEvalScoresMV,
  type EvalScoreAggregate,
  type EvalScoresMVConfig,
  type AggregationDimension,
} from '../../../../src/observability/evals'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockScore {
  $id: string
  timestamp: Date
  runId: number
  suiteId: number
  suiteName: string
  evalId: number
  scorerName: string
  score: number
  description?: string
  modelId?: string
  promptId?: string
  metadata?: Record<string, unknown>
}

interface MockAggregate {
  $id: string
  dateKey: string
  scorerName: string
  suiteName?: string
  modelId?: string
  promptId?: string
  granularity: string
  scoreCount: number
  scoreSum: number
  scoreAvg: number
  scoreMin: number
  scoreMax: number
  scoreStdDev: number
  scoreVariance: number
  distribution: number[]
  p50Score: number
  p90Score: number
  p95Score: number
  p99Score: number
  uniqueEvalCount: number
  uniqueRunCount: number
  uniqueSuiteCount: number
  excellentCount: number
  goodCount: number
  fairCount: number
  poorCount: number
  createdAt: Date
  updatedAt: Date
  version: number
  [key: string]: unknown
}

function createMockParqueDB(initialScores: MockScore[] = [], initialAggregates: MockAggregate[] = []) {
  const scores = [...initialScores]
  const aggregates = [...initialAggregates]

  return {
    collection: (name: string) => ({
      find: vi.fn().mockImplementation(async (filter: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
        if (name === 'eval_scores') {
          let results = [...scores]

          // Apply timestamp filter
          if (filter.timestamp) {
            const tsFilter = filter.timestamp as { $gte?: Date }
            if (tsFilter.$gte) {
              const gteTime = tsFilter.$gte instanceof Date ? tsFilter.$gte.getTime() : new Date(tsFilter.$gte).getTime()
              results = results.filter(score => {
                const scoreTime = score.timestamp instanceof Date ? score.timestamp.getTime() : new Date(score.timestamp).getTime()
                return scoreTime >= gteTime
              })
            }
          }

          // Apply sort
          if (options?.sort?.timestamp) {
            results.sort((a, b) => {
              const diff = new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
              return options.sort!.timestamp === 1 ? diff : -diff
            })
          }

          // Apply limit
          if (options?.limit) {
            results = results.slice(0, options.limit)
          }

          return results
        }

        if (name === 'eval_score_aggregates') {
          let results = [...aggregates]

          // Apply filters
          if (filter.dateKey) {
            if (typeof filter.dateKey === 'string') {
              results = results.filter(agg => agg.dateKey === filter.dateKey)
            } else if (typeof filter.dateKey === 'object') {
              const dk = filter.dateKey as { $gte?: string; $lte?: string }
              if (dk.$gte) {
                results = results.filter(agg => agg.dateKey >= dk.$gte!)
              }
              if (dk.$lte) {
                results = results.filter(agg => agg.dateKey <= dk.$lte!)
              }
            }
          }

          if (filter.scorerName) {
            results = results.filter(agg => agg.scorerName === filter.scorerName)
          }

          if (filter.suiteName) {
            results = results.filter(agg => agg.suiteName === filter.suiteName)
          }

          if (filter.modelId) {
            results = results.filter(agg => agg.modelId === filter.modelId)
          }

          if (filter.promptId) {
            results = results.filter(agg => agg.promptId === filter.promptId)
          }

          if (filter.granularity) {
            results = results.filter(agg => agg.granularity === filter.granularity)
          }

          // Apply sort
          if (options?.sort) {
            const sortKey = Object.keys(options.sort)[0]!
            const sortOrder = options.sort[sortKey]
            results.sort((a, b) => {
              const aVal = a[sortKey]
              const bVal = b[sortKey]
              if (typeof aVal === 'string' && typeof bVal === 'string') {
                return sortOrder === 1 ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
              }
              return sortOrder === 1 ? Number(aVal) - Number(bVal) : Number(bVal) - Number(aVal)
            })
          }

          // Apply limit
          if (options?.limit) {
            results = results.slice(0, options.limit)
          }

          return results
        }

        return []
      }),

      create: vi.fn().mockImplementation(async (data: Record<string, unknown>) => {
        const id = `${name}/${Date.now()}`
        const created = { ...data, $id: id }
        if (name === 'eval_score_aggregates') {
          aggregates.push(created as MockAggregate)
        }
        return created
      }),

      update: vi.fn().mockImplementation(async (id: string, update: Record<string, unknown>) => {
        if (name === 'eval_score_aggregates') {
          const idx = aggregates.findIndex(a => a.$id.endsWith(`/${id}`))
          if (idx >= 0 && update.$set) {
            Object.assign(aggregates[idx], update.$set)
          }
        }
        return { matchedCount: 1, modifiedCount: 1 }
      }),

      findOne: vi.fn().mockImplementation(async () => null),
    }),

    // Expose internal state for testing
    __scores: scores,
    __aggregates: aggregates,
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('EvalScoresMV', () => {
  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      expect(mv).toBeInstanceOf(EvalScoresMV)
    })

    it('should accept custom configuration', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createEvalScoresMV>[0]
      const config: EvalScoresMVConfig = {
        sourceCollection: 'custom_scores',
        targetCollection: 'custom_aggregates',
        granularity: 'hour',
        batchSize: 500,
        aggregationDimensions: ['scorerName', 'modelId'],
      }

      const mv = new EvalScoresMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.sourceCollection).toBe('custom_scores')
      expect(resolvedConfig.targetCollection).toBe('custom_aggregates')
      expect(resolvedConfig.granularity).toBe('hour')
      expect(resolvedConfig.batchSize).toBe(500)
      expect(resolvedConfig.aggregationDimensions).toEqual(['scorerName', 'modelId'])
    })
  })

  describe('createEvalScoresMV factory', () => {
    it('should create an EvalScoresMV instance', () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = createEvalScoresMV(db)

      expect(mv).toBeInstanceOf(EvalScoresMV)
    })
  })

  describe('refresh', () => {
    it('should return success with zero records when no scores exist', async () => {
      const db = createMockParqueDB() as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(0)
      expect(result.aggregatesUpdated).toBe(0)
    })

    it('should process scores and create aggregates', async () => {
      // Use relative dates to avoid issues with maxAge filtering
      const now = new Date()
      const scores: MockScore[] = [
        {
          $id: 'eval_scores/1',
          timestamp: new Date(now.getTime() - 1000 * 60 * 60), // 1 hour ago
          runId: 1,
          suiteId: 1,
          suiteName: 'test-suite',
          evalId: 1,
          scorerName: 'accuracy',
          score: 0.85,
        },
        {
          $id: 'eval_scores/2',
          timestamp: new Date(now.getTime() - 1000 * 60 * 30), // 30 min ago
          runId: 1,
          suiteId: 1,
          suiteName: 'test-suite',
          evalId: 2,
          scorerName: 'accuracy',
          score: 0.90,
        },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const result = await mv.refresh()

      expect(result.success).toBe(true)
      expect(result.recordsProcessed).toBe(2)
      expect(result.aggregatesUpdated).toBe(1)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)

      // Check aggregates were created
      expect(mockDb.__aggregates.length).toBe(1)
      const agg = mockDb.__aggregates[0]
      expect(agg.scorerName).toBe('accuracy')
      expect(agg.scoreCount).toBe(2)
      expect(agg.scoreAvg).toBeCloseTo(0.875, 5)
    })

    it('should handle errors gracefully', async () => {
      const db = {
        collection: () => ({
          find: vi.fn().mockRejectedValue(new Error('Database error')),
        }),
      } as unknown as Parameters<typeof createEvalScoresMV>[0]

      const mv = new EvalScoresMV(db)
      const result = await mv.refresh()

      expect(result.success).toBe(false)
      expect(result.error).toBe('Database error')
    })

    it('should calculate score statistics correctly', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T11:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.85 },
        { $id: 'eval_scores/3', timestamp: new Date('2026-02-03T12:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 3, scorerName: 'accuracy', score: 0.90 },
        { $id: 'eval_scores/4', timestamp: new Date('2026-02-03T13:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 4, scorerName: 'accuracy', score: 0.95 },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.scoreCount).toBe(4)
      expect(agg.scoreSum).toBeCloseTo(3.5, 5)
      expect(agg.scoreAvg).toBeCloseTo(0.875, 5)
      expect(agg.scoreMin).toBe(0.80)
      expect(agg.scoreMax).toBe(0.95)
      expect(agg.scoreStdDev).toBeGreaterThan(0)
    })

    it('should calculate percentiles correctly', async () => {
      // Create 100 scores from 0.01 to 1.00
      const scores: MockScore[] = []
      for (let i = 1; i <= 100; i++) {
        scores.push({
          $id: `eval_scores/${i}`,
          timestamp: new Date('2026-02-03T10:00:00Z'),
          runId: 1,
          suiteId: 1,
          suiteName: 'suite',
          evalId: i,
          scorerName: 'accuracy',
          score: i / 100,
        })
      }

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.p50Score).toBeCloseTo(0.50, 1)
      expect(agg.p90Score).toBeCloseTo(0.90, 1)
      expect(agg.p95Score).toBeCloseTo(0.95, 1)
      expect(agg.p99Score).toBeCloseTo(0.99, 1)
    })

    it('should track outcome counts (excellent/good/fair/poor)', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.95 }, // excellent
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.92 }, // excellent
        { $id: 'eval_scores/3', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 3, scorerName: 'accuracy', score: 0.75 }, // good
        { $id: 'eval_scores/4', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 4, scorerName: 'accuracy', score: 0.55 }, // fair
        { $id: 'eval_scores/5', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 5, scorerName: 'accuracy', score: 0.30 }, // poor
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.excellentCount).toBe(2) // >= 0.9
      expect(agg.goodCount).toBe(1) // >= 0.7 and < 0.9
      expect(agg.fairCount).toBe(1) // >= 0.5 and < 0.7
      expect(agg.poorCount).toBe(1) // < 0.5
    })

    it('should create distribution histogram', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.05 },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.15 },
        { $id: 'eval_scores/3', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 3, scorerName: 'accuracy', score: 0.95 },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.distribution).toHaveLength(10)
      expect(agg.distribution[0]).toBe(1) // 0-0.1 bucket
      expect(agg.distribution[1]).toBe(1) // 0.1-0.2 bucket
      expect(agg.distribution[9]).toBe(1) // 0.9-1.0 bucket
    })

    it('should track unique eval/run/suite counts', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.85 },
        { $id: 'eval_scores/3', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 2, suiteId: 2, suiteName: 'suite2', evalId: 3, scorerName: 'accuracy', score: 0.90 },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      const agg = mockDb.__aggregates[0]
      expect(agg.uniqueEvalCount).toBe(3)
      expect(agg.uniqueRunCount).toBe(2)
      expect(agg.uniqueSuiteCount).toBe(2)
    })

    it('should create separate aggregates for different scorers', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'relevance', score: 0.90 },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      expect(mockDb.__aggregates.length).toBe(2)
      const accuracyAgg = mockDb.__aggregates.find(a => a.scorerName === 'accuracy')
      const relevanceAgg = mockDb.__aggregates.find(a => a.scorerName === 'relevance')
      expect(accuracyAgg).toBeDefined()
      expect(relevanceAgg).toBeDefined()
    })

    it('should create separate aggregates for different dates', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-01T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-02T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.90 },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      await mv.refresh()

      expect(mockDb.__aggregates.length).toBe(2)
      const feb1Agg = mockDb.__aggregates.find(a => a.dateKey === '2026-02-01')
      const feb2Agg = mockDb.__aggregates.find(a => a.dateKey === '2026-02-02')
      expect(feb1Agg).toBeDefined()
      expect(feb2Agg).toBeDefined()
    })

    it('should aggregate by modelId when configured', async () => {
      const scores: MockScore[] = [
        { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80, modelId: 'gpt-4' },
        { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.90, modelId: 'claude-3' },
      ]

      const mockDb = createMockParqueDB(scores)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db, {
        aggregationDimensions: ['scorerName', 'modelId'],
      })

      await mv.refresh()

      expect(mockDb.__aggregates.length).toBe(2)
      const gpt4Agg = mockDb.__aggregates.find(a => a.modelId === 'gpt-4')
      const claudeAgg = mockDb.__aggregates.find(a => a.modelId === 'claude-3')
      expect(gpt4Agg).toBeDefined()
      expect(claudeAgg).toBeDefined()
    })
  })

  describe('getAggregates', () => {
    it('should query aggregates with default options', async () => {
      const aggregates: MockAggregate[] = [
        createMockAggregate('2026-02-03', 'accuracy'),
        createMockAggregate('2026-02-03', 'relevance'),
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const results = await mv.getAggregates()

      expect(results.length).toBe(2)
    })

    it('should filter by scorerName', async () => {
      const aggregates: MockAggregate[] = [
        createMockAggregate('2026-02-03', 'accuracy'),
        createMockAggregate('2026-02-03', 'relevance'),
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const results = await mv.getAggregates({ scorerName: 'accuracy' })

      expect(results.length).toBe(1)
      expect(results[0].scorerName).toBe('accuracy')
    })

    it('should filter by date range', async () => {
      const aggregates: MockAggregate[] = [
        createMockAggregate('2026-02-01', 'accuracy'),
        createMockAggregate('2026-02-02', 'accuracy'),
        createMockAggregate('2026-02-03', 'accuracy'),
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const results = await mv.getAggregates({
        from: new Date('2026-02-02'),
        to: new Date('2026-02-02'),
      })

      expect(results.length).toBe(1)
      expect(results[0].dateKey).toBe('2026-02-02')
    })

    it('should respect limit option', async () => {
      const aggregates: MockAggregate[] = [
        createMockAggregate('2026-02-01', 'accuracy'),
        createMockAggregate('2026-02-02', 'accuracy'),
        createMockAggregate('2026-02-03', 'accuracy'),
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const results = await mv.getAggregates({ limit: 2 })

      expect(results.length).toBe(2)
    })
  })

  describe('getSummary', () => {
    it('should return a summary with zero values when no aggregates exist', async () => {
      const mockDb = createMockParqueDB([], [])
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const summary = await mv.getSummary()

      expect(summary.totalScores).toBe(0)
      expect(summary.globalAverageScore).toBe(0)
    })

    it('should aggregate statistics across all records', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 10, scoreAvg: 0.80, scoreMin: 0.70, scoreMax: 0.90 },
        { ...createMockAggregate('2026-02-02', 'accuracy'), scoreCount: 5, scoreAvg: 0.85, scoreMin: 0.75, scoreMax: 0.95 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const summary = await mv.getSummary()

      expect(summary.totalScores).toBe(15)
      // Weighted average: (10*0.80 + 5*0.85) / 15 = 12.25 / 15 = 0.8166...
      expect(summary.globalAverageScore).toBeCloseTo(0.8167, 3)
      expect(summary.globalMinScore).toBe(0.70)
      expect(summary.globalMaxScore).toBe(0.95)
    })

    it('should provide breakdown by scorer', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 10, scoreAvg: 0.80, scoreMin: 0.70, scoreMax: 0.90 },
        { ...createMockAggregate('2026-02-01', 'relevance'), scoreCount: 5, scoreAvg: 0.85, scoreMin: 0.75, scoreMax: 0.95 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const summary = await mv.getSummary()

      expect(summary.byScorer['accuracy']).toBeDefined()
      expect(summary.byScorer['accuracy'].scoreCount).toBe(10)
      expect(summary.byScorer['accuracy'].averageScore).toBeCloseTo(0.80, 5)

      expect(summary.byScorer['relevance']).toBeDefined()
      expect(summary.byScorer['relevance'].scoreCount).toBe(5)
    })

    it('should track unique counts', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), suiteName: 'suite1', modelId: 'gpt-4' },
        { ...createMockAggregate('2026-02-01', 'relevance'), suiteName: 'suite2', modelId: 'claude-3' },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const summary = await mv.getSummary()

      expect(summary.uniqueScorers).toBe(2)
      expect(summary.uniqueSuites).toBe(2)
      expect(summary.uniqueModels).toBe(2)
    })
  })

  describe('getTrends', () => {
    it('should return trend data points', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 10, scoreAvg: 0.75 },
        { ...createMockAggregate('2026-02-02', 'accuracy'), scoreCount: 10, scoreAvg: 0.80 },
        { ...createMockAggregate('2026-02-03', 'accuracy'), scoreCount: 10, scoreAvg: 0.85 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const trends = await mv.getTrends({ scorerName: 'accuracy' })

      expect(trends.length).toBe(3)
      expect(trends[0].dateKey).toBe('2026-02-01')
      expect(trends[0].averageScore).toBeCloseTo(0.75, 5)
      expect(trends[2].averageScore).toBeCloseTo(0.85, 5)
    })

    it('should filter by scorer name', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 10, scoreAvg: 0.75 },
        { ...createMockAggregate('2026-02-01', 'relevance'), scoreCount: 10, scoreAvg: 0.85 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const trends = await mv.getTrends({ scorerName: 'accuracy' })

      expect(trends.length).toBe(1)
      expect(trends[0].averageScore).toBeCloseTo(0.75, 5)
    })
  })

  describe('getScorerPerformance', () => {
    it('should return scorer performance rankings', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 10, scoreAvg: 0.80, excellentCount: 3, poorCount: 1 },
        { ...createMockAggregate('2026-02-01', 'relevance'), scoreCount: 10, scoreAvg: 0.90, excellentCount: 8, poorCount: 0 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const performances = await mv.getScorerPerformance()

      expect(performances.length).toBe(2)
      // Should be sorted by average score descending
      expect(performances[0].scorerName).toBe('relevance')
      expect(performances[0].averageScore).toBeCloseTo(0.90, 5)
      expect(performances[1].scorerName).toBe('accuracy')
    })

    it('should calculate excellent and poor rates', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), scoreCount: 100, scoreAvg: 0.80, excellentCount: 30, poorCount: 10 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const performances = await mv.getScorerPerformance()

      expect(performances[0].excellentRate).toBeCloseTo(0.30, 5)
      expect(performances[0].poorRate).toBeCloseTo(0.10, 5)
    })
  })

  describe('compareScores', () => {
    it('should compare two configurations', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), modelId: 'gpt-4', scoreCount: 50, scoreAvg: 0.80 },
        { ...createMockAggregate('2026-02-01', 'accuracy'), modelId: 'claude-3', scoreCount: 50, scoreAvg: 0.85 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const comparison = await mv.compareScores(
        { name: 'GPT-4', modelId: 'gpt-4' },
        { name: 'Claude-3', modelId: 'claude-3' }
      )

      expect(comparison.baseline).toBe('GPT-4')
      expect(comparison.comparison).toBe('Claude-3')
      expect(comparison.baselineAvg).toBeCloseTo(0.80, 5)
      expect(comparison.comparisonAvg).toBeCloseTo(0.85, 5)
      expect(comparison.absoluteDiff).toBeCloseTo(0.05, 5)
      expect(comparison.relativeDiff).toBeCloseTo(6.25, 1) // 5% of 80% = 6.25%
      expect(comparison.isSignificant).toBe(true) // > 5% and >= 30 samples
    })

    it('should not be significant with small sample size', async () => {
      const aggregates: MockAggregate[] = [
        { ...createMockAggregate('2026-02-01', 'accuracy'), modelId: 'gpt-4', scoreCount: 10, scoreAvg: 0.80 },
        { ...createMockAggregate('2026-02-01', 'accuracy'), modelId: 'claude-3', scoreCount: 10, scoreAvg: 0.90 },
      ]

      const mockDb = createMockParqueDB([], aggregates)
      const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
      const mv = new EvalScoresMV(db)

      const comparison = await mv.compareScores(
        { name: 'GPT-4', modelId: 'gpt-4' },
        { name: 'Claude-3', modelId: 'claude-3' }
      )

      expect(comparison.isSignificant).toBe(false) // < 30 samples
    })
  })
})

// =============================================================================
// Date Key Generation Tests
// =============================================================================

describe('date key generation', () => {
  it('should group by day correctly', async () => {
    const scores: MockScore[] = [
      { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T00:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
      { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T23:59:59Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.90 },
    ]

    const mockDb = createMockParqueDB(scores)
    const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
    const mv = new EvalScoresMV(db, { granularity: 'day' })

    await mv.refresh()

    // Both should be in the same day aggregate
    expect(mockDb.__aggregates.length).toBe(1)
    expect(mockDb.__aggregates[0].dateKey).toBe('2026-02-03')
    expect(mockDb.__aggregates[0].scoreCount).toBe(2)
  })

  it('should group by hour correctly', async () => {
    const scores: MockScore[] = [
      { $id: 'eval_scores/1', timestamp: new Date('2026-02-03T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
      { $id: 'eval_scores/2', timestamp: new Date('2026-02-03T10:59:59Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.85 },
      { $id: 'eval_scores/3', timestamp: new Date('2026-02-03T11:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 3, scorerName: 'accuracy', score: 0.90 },
    ]

    const mockDb = createMockParqueDB(scores)
    const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
    const mv = new EvalScoresMV(db, { granularity: 'hour' })

    await mv.refresh()

    // Should be 2 aggregates: 10:xx and 11:xx
    expect(mockDb.__aggregates.length).toBe(2)
    expect(mockDb.__aggregates.some(a => a.dateKey === '2026-02-03T10')).toBe(true)
    expect(mockDb.__aggregates.some(a => a.dateKey === '2026-02-03T11')).toBe(true)
  })

  it('should group by month correctly', async () => {
    const scores: MockScore[] = [
      { $id: 'eval_scores/1', timestamp: new Date('2026-02-01T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
      { $id: 'eval_scores/2', timestamp: new Date('2026-02-28T23:59:59Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.90 },
    ]

    const mockDb = createMockParqueDB(scores)
    const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
    const mv = new EvalScoresMV(db, { granularity: 'month' })

    await mv.refresh()

    // Both should be in the same month aggregate
    expect(mockDb.__aggregates.length).toBe(1)
    expect(mockDb.__aggregates[0].dateKey).toBe('2026-02')
    expect(mockDb.__aggregates[0].scoreCount).toBe(2)
  })

  it('should group by week correctly', async () => {
    // ISO week: Feb 3, 2026 is Week 6
    const scores: MockScore[] = [
      { $id: 'eval_scores/1', timestamp: new Date('2026-02-02T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 1, scorerName: 'accuracy', score: 0.80 },
      { $id: 'eval_scores/2', timestamp: new Date('2026-02-08T10:00:00Z'), runId: 1, suiteId: 1, suiteName: 'suite', evalId: 2, scorerName: 'accuracy', score: 0.90 },
    ]

    const mockDb = createMockParqueDB(scores)
    const db = mockDb as unknown as Parameters<typeof createEvalScoresMV>[0]
    const mv = new EvalScoresMV(db, { granularity: 'week' })

    await mv.refresh()

    // Feb 2 is Week 6, Feb 8 is Week 6 too (same week)
    expect(mockDb.__aggregates.length).toBe(1)
    expect(mockDb.__aggregates[0].dateKey).toMatch(/2026-W06/)
  })
})

// =============================================================================
// Helper Functions
// =============================================================================

function createMockAggregate(dateKey: string, scorerName: string): MockAggregate {
  const now = new Date()
  return {
    $id: `eval_score_aggregates/${Date.now()}_${Math.random()}`,
    $type: 'EvalScoreAggregate',
    name: `${scorerName} (${dateKey})`,
    dateKey,
    granularity: 'day',
    scorerName,
    scoreCount: 10,
    scoreSum: 8.5,
    scoreAvg: 0.85,
    scoreMin: 0.70,
    scoreMax: 0.95,
    scoreStdDev: 0.08,
    scoreVariance: 0.0064,
    distribution: [0, 0, 0, 0, 0, 1, 2, 4, 2, 1],
    p50Score: 0.85,
    p90Score: 0.92,
    p95Score: 0.94,
    p99Score: 0.95,
    uniqueEvalCount: 10,
    uniqueRunCount: 2,
    uniqueSuiteCount: 1,
    excellentCount: 3,
    goodCount: 4,
    fairCount: 2,
    poorCount: 1,
    createdAt: now,
    updatedAt: now,
    version: 1,
  }
}

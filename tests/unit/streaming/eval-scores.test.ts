/**
 * Tests for EvalScores Materialized View
 *
 * Tests cover:
 * - Score extraction from events
 * - Indexing and queries
 * - Statistics computation
 * - Trend analysis
 * - Memory management
 */

import { describe, test, expect, beforeEach } from 'vitest'
import {
  EvalScoresMV,
  createEvalScoresMV,
} from '../../../src/streaming/eval-scores'
import type { Event, EventOp } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  id: string,
  data: Record<string, unknown>,
  op: EventOp = 'CREATE',
  ts?: number
): Event {
  return {
    id,
    ts: ts ?? Date.now(),
    op,
    target: 'evalite_scores:' + id,
    after: data,
    actor: 'test:user',
  }
}

function createScoreEvent(
  id: string,
  runId: number,
  suiteName: string,
  scorerName: string,
  score: number,
  extra?: Record<string, unknown>
): Event {
  return createEvent(id, {
    runId,
    suiteName,
    scorerName,
    score,
    ...extra,
  })
}

// =============================================================================
// EvalScoresMV Tests
// =============================================================================

describe('EvalScoresMV', () => {
  let mv: EvalScoresMV

  beforeEach(() => {
    mv = createEvalScoresMV()
  })

  describe('Event Processing', () => {
    test('processes score events with all required fields', async () => {
      const event = createScoreEvent('s1', 1, 'test.eval.ts', 'accuracy', 0.85)
      await mv.process([event])

      expect(mv.count()).toBe(1)
      const scores = mv.getScores()
      expect(scores[0]?.score).toBe(0.85)
      expect(scores[0]?.runId).toBe(1)
      expect(scores[0]?.suiteName).toBe('test.eval.ts')
      expect(scores[0]?.scorerName).toBe('accuracy')
    })

    test('processes score events with $type: EvalScore', async () => {
      const event = createEvent('s1', {
        $type: 'EvalScore',
        runId: 1,
        suiteName: 'test.eval.ts',
        scorerName: 'factuality',
        score: 0.92,
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
    })

    test('processes score events with alternative field names', async () => {
      const event = createEvent('s1', {
        run_id: 1,
        suite_name: 'test.eval.ts',
        scorer_name: 'relevance',
        score: 0.75,
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
      const score = mv.getScore('s1')
      expect(score?.runId).toBe(1)
      expect(score?.suiteName).toBe('test.eval.ts')
      expect(score?.scorerName).toBe('relevance')
    })

    test('ignores events without score field', async () => {
      const event = createEvent('s1', {
        runId: 1,
        suiteName: 'test.eval.ts',
        scorerName: 'accuracy',
        // missing score field
      })
      await mv.process([event])

      expect(mv.count()).toBe(0)
    })

    test('ignores events without required fields', async () => {
      const event = createEvent('s1', {
        score: 0.85,
        // missing suiteName and scorerName
      })
      await mv.process([event])

      expect(mv.count()).toBe(0)
    })

    test('processes multiple events in batch', async () => {
      const events = [
        createScoreEvent('s1', 1, 'suite-a', 'accuracy', 0.8),
        createScoreEvent('s2', 1, 'suite-a', 'relevance', 0.9),
        createScoreEvent('s3', 1, 'suite-b', 'accuracy', 0.75),
      ]
      await mv.process(events)

      expect(mv.count()).toBe(3)
    })

    test('deduplicates events by ID', async () => {
      const event1 = createScoreEvent('same-id', 1, 'suite', 'scorer', 0.8)
      const event2 = createScoreEvent('same-id', 1, 'suite', 'scorer', 0.9)

      await mv.process([event1])
      await mv.process([event2])

      expect(mv.count()).toBe(1)
      expect(mv.getScore('same-id')?.score).toBe(0.8)
    })
  })

  describe('Score Extraction', () => {
    test('extracts description', async () => {
      const event = createScoreEvent('s1', 1, 'suite', 'scorer', 0.85, {
        description: 'Score based on factual accuracy',
      })
      await mv.process([event])

      const score = mv.getScore('s1')
      expect(score?.description).toBe('Score based on factual accuracy')
    })

    test('extracts evalId', async () => {
      const event = createScoreEvent('s1', 1, 'suite', 'scorer', 0.85, {
        evalId: 42,
      })
      await mv.process([event])

      const score = mv.getScore('s1')
      expect(score?.evalId).toBe(42)
    })

    test('extracts metadata', async () => {
      const event = createScoreEvent('s1', 1, 'suite', 'scorer', 0.85, {
        input: { prompt: 'What is 2+2?' },
        output: { answer: '4' },
        expected: { answer: '4' },
        duration: 150,
        tokens: 25,
      })
      await mv.process([event])

      const score = mv.getScore('s1')
      expect(score?.metadata?.input).toEqual({ prompt: 'What is 2+2?' })
      expect(score?.metadata?.output).toEqual({ answer: '4' })
      expect(score?.metadata?.duration).toBe(150)
      expect(score?.metadata?.tokens).toBe(25)
    })

    test('extracts score from nested result object', async () => {
      const event = createEvent('s1', {
        suiteName: 'test',
        scorerName: 'scorer',
        result: { score: 0.95 },
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
      expect(mv.getScore('s1')?.score).toBe(0.95)
    })
  })

  describe('Querying', () => {
    beforeEach(async () => {
      const events = [
        createScoreEvent('s1', 1, 'sentiment.eval.ts', 'accuracy', 0.8),
        createScoreEvent('s2', 1, 'sentiment.eval.ts', 'relevance', 0.85),
        createScoreEvent('s3', 1, 'qa.eval.ts', 'accuracy', 0.9),
        createScoreEvent('s4', 2, 'sentiment.eval.ts', 'accuracy', 0.82),
        createScoreEvent('s5', 2, 'qa.eval.ts', 'accuracy', 0.88),
      ]
      await mv.process(events)
    })

    test('getScores returns all scores (most recent first)', () => {
      const scores = mv.getScores()
      expect(scores).toHaveLength(5)
      expect(scores[0]?.id).toBe('s5')
      expect(scores[4]?.id).toBe('s1')
    })

    test('getScores respects limit', () => {
      const scores = mv.getScores(2)
      expect(scores).toHaveLength(2)
      expect(scores[0]?.id).toBe('s5')
      expect(scores[1]?.id).toBe('s4')
    })

    test('getScoresByRun returns correct scores', () => {
      const run1Scores = mv.getScoresByRun(1)
      expect(run1Scores).toHaveLength(3)

      const run2Scores = mv.getScoresByRun(2)
      expect(run2Scores).toHaveLength(2)
    })

    test('getScoresBySuite returns correct scores', () => {
      const sentimentScores = mv.getScoresBySuite('sentiment.eval.ts')
      expect(sentimentScores).toHaveLength(3)

      const qaScores = mv.getScoresBySuite('qa.eval.ts')
      expect(qaScores).toHaveLength(2)
    })

    test('getScoresByScorer returns correct scores', () => {
      const accuracyScores = mv.getScoresByScorer('accuracy')
      expect(accuracyScores).toHaveLength(4)

      const relevanceScores = mv.getScoresByScorer('relevance')
      expect(relevanceScores).toHaveLength(1)
    })

    test('getScoresByEval returns correct scores', async () => {
      const events = [
        createScoreEvent('e1s1', 1, 'suite', 'scorer1', 0.8, { evalId: 10 }),
        createScoreEvent('e1s2', 1, 'suite', 'scorer2', 0.9, { evalId: 10 }),
        createScoreEvent('e2s1', 1, 'suite', 'scorer1', 0.7, { evalId: 20 }),
      ]
      mv.clear()
      await mv.process(events)

      const eval10Scores = mv.getScoresByEval(10)
      expect(eval10Scores).toHaveLength(2)
    })

    test('getScore returns single score by ID', () => {
      const score = mv.getScore('s3')
      expect(score).toBeDefined()
      expect(score?.score).toBe(0.9)
    })

    test('getScore returns undefined for unknown ID', () => {
      const score = mv.getScore('unknown')
      expect(score).toBeUndefined()
    })

    test('getScoresInRange filters by timestamp', async () => {
      const now = Date.now()
      mv.clear()

      const events = [
        createScoreEvent('t1', 1, 'suite', 'scorer', 0.8),
        createScoreEvent('t2', 1, 'suite', 'scorer', 0.85),
        createScoreEvent('t3', 1, 'suite', 'scorer', 0.9),
      ]
      // Set specific timestamps
      events[0]!.ts = now - 60000 // 1 minute ago
      events[1]!.ts = now - 30000 // 30 seconds ago
      events[2]!.ts = now // now

      await mv.process(events)

      const recentScores = mv.getScoresInRange(now - 45000, now)
      expect(recentScores).toHaveLength(2)
    })
  })

  describe('Statistics', () => {
    beforeEach(async () => {
      const events = [
        createScoreEvent('s1', 1, 'suite-a', 'accuracy', 0.8),
        createScoreEvent('s2', 1, 'suite-a', 'accuracy', 0.85),
        createScoreEvent('s3', 1, 'suite-a', 'accuracy', 0.9),
        createScoreEvent('s4', 1, 'suite-b', 'relevance', 0.7),
        createScoreEvent('s5', 2, 'suite-a', 'accuracy', 0.88),
      ]
      await mv.process(events)
    })

    test('getStats returns correct totals', () => {
      const stats = mv.getStats()
      expect(stats.totalScores).toBe(5)
      expect(stats.uniqueRuns).toBe(2)
      expect(stats.uniqueSuites).toBe(2)
      expect(stats.uniqueScorers).toBe(2)
    })

    test('getStats returns correct global average', () => {
      const stats = mv.getStats()
      // (0.8 + 0.85 + 0.9 + 0.7 + 0.88) / 5 = 0.826
      expect(stats.globalAverageScore).toBeCloseTo(0.826, 2)
    })

    test('getStats returns byScorer statistics', () => {
      const stats = mv.getStats()
      expect(stats.byScorer.accuracy.count).toBe(4)
      expect(stats.byScorer.accuracy.average).toBeCloseTo(0.8575, 2)
      expect(stats.byScorer.relevance.count).toBe(1)
      expect(stats.byScorer.relevance.average).toBe(0.7)
    })

    test('getStats returns bySuite statistics', () => {
      const stats = mv.getStats()
      expect(stats.bySuite['suite-a'].count).toBe(4)
      expect(stats.bySuite['suite-b'].count).toBe(1)
    })

    test('getStats returns byRun statistics', () => {
      const stats = mv.getStats()
      expect(stats.byRun[1]!.count).toBe(4)
      expect(stats.byRun[2]!.count).toBe(1)
    })

    test('getStats includes time range', () => {
      const stats = mv.getStats()
      expect(stats.timeRange.start).toBeLessThanOrEqual(stats.timeRange.end)
    })

    test('getScorerStats returns correct statistics', () => {
      const stats = mv.getScorerStats('accuracy')
      expect(stats).not.toBeNull()
      expect(stats!.count).toBe(4)
      expect(stats!.min).toBe(0.8)
      expect(stats!.max).toBe(0.9)
    })

    test('getScorerStats returns null for unknown scorer', () => {
      const stats = mv.getScorerStats('unknown')
      expect(stats).toBeNull()
    })

    test('getSuiteStats returns correct statistics', () => {
      const stats = mv.getSuiteStats('suite-a')
      expect(stats).not.toBeNull()
      expect(stats!.count).toBe(4)
    })

    test('getRunStats returns correct statistics', () => {
      const stats = mv.getRunStats(1)
      expect(stats).not.toBeNull()
      expect(stats!.count).toBe(4)
    })

    test('statistics include score distribution', () => {
      const stats = mv.getScorerStats('accuracy')
      expect(stats!.distribution).toHaveLength(10)
      // All scores are between 0.8 and 0.9, so they should be in bucket 8 (0.8-0.9)
      const totalInDistribution = stats!.distribution.reduce((a, b) => a + b, 0)
      expect(totalInDistribution).toBe(4)
    })

    test('statistics include standard deviation', () => {
      const stats = mv.getScorerStats('accuracy')
      expect(stats!.stdDev).toBeGreaterThan(0)
      expect(stats!.stdDev).toBeLessThan(1)
    })
  })

  describe('Score Trends', () => {
    test('getScoreTrends returns trend points', async () => {
      const now = Date.now()
      const hourAgo = now - 3600000
      const twoHoursAgo = now - 7200000

      const events = [
        { ...createScoreEvent('t1', 1, 'suite', 'scorer', 0.7), ts: twoHoursAgo },
        { ...createScoreEvent('t2', 2, 'suite', 'scorer', 0.8), ts: hourAgo },
        { ...createScoreEvent('t3', 3, 'suite', 'scorer', 0.9), ts: now },
      ]
      await mv.process(events as Event[])

      const trends = mv.getScoreTrends({ bucketSizeMs: 3600000 })
      expect(trends.length).toBeGreaterThan(0)
    })

    test('getScoreTrends filters by scorer name', async () => {
      const events = [
        createScoreEvent('t1', 1, 'suite', 'accuracy', 0.8),
        createScoreEvent('t2', 1, 'suite', 'relevance', 0.7),
        createScoreEvent('t3', 1, 'suite', 'accuracy', 0.9),
      ]
      await mv.process(events)

      const trends = mv.getScoreTrends({ scorerName: 'accuracy' })
      expect(trends.length).toBeGreaterThan(0)
      // All trend points should be based on accuracy scores only
    })

    test('getScoreTrends filters by suite name', async () => {
      const events = [
        createScoreEvent('t1', 1, 'suite-a', 'scorer', 0.8),
        createScoreEvent('t2', 1, 'suite-b', 'scorer', 0.7),
        createScoreEvent('t3', 1, 'suite-a', 'scorer', 0.9),
      ]
      await mv.process(events)

      const trends = mv.getScoreTrends({ suiteName: 'suite-a' })
      expect(trends.length).toBeGreaterThan(0)
    })

    test('getScoreTrends includes min/max scores per bucket', async () => {
      const now = Date.now()
      const events = [
        { ...createScoreEvent('t1', 1, 'suite', 'scorer', 0.6), ts: now },
        { ...createScoreEvent('t2', 1, 'suite', 'scorer', 0.9), ts: now },
        { ...createScoreEvent('t3', 1, 'suite', 'scorer', 0.75), ts: now },
      ]
      await mv.process(events as Event[])

      const trends = mv.getScoreTrends({ bucketSizeMs: 3600000 })
      expect(trends[0]?.minScore).toBe(0.6)
      expect(trends[0]?.maxScore).toBe(0.9)
    })
  })

  describe('Memory Management', () => {
    test('enforces maxScores limit', async () => {
      const mv = createEvalScoresMV({ maxScores: 5 })

      const events = Array.from({ length: 10 }, (_, i) =>
        createScoreEvent(`s${i}`, 1, 'suite', 'scorer', 0.5 + i * 0.05)
      )

      await mv.process(events)

      expect(mv.count()).toBe(5)
      // Should have kept the most recent 5
      expect(mv.getScore('s5')).toBeDefined()
      expect(mv.getScore('s9')).toBeDefined()
      expect(mv.getScore('s0')).toBeUndefined()
    })

    test('clear removes all scores', async () => {
      await mv.process([createScoreEvent('s1', 1, 'suite', 'scorer', 0.8)])
      expect(mv.count()).toBe(1)

      mv.clear()

      expect(mv.count()).toBe(0)
      expect(mv.getScores()).toHaveLength(0)
    })

    test('clear resets all indexes', async () => {
      await mv.process([
        createScoreEvent('s1', 1, 'suite', 'scorer', 0.8),
      ])

      mv.clear()

      expect(mv.getScoresByRun(1)).toHaveLength(0)
      expect(mv.getScoresBySuite('suite')).toHaveLength(0)
      expect(mv.getScoresByScorer('scorer')).toHaveLength(0)
    })
  })

  describe('Custom Configuration', () => {
    test('accepts custom source namespaces', () => {
      const customMV = createEvalScoresMV({
        sourceNamespaces: ['custom-scores', 'eval-scores'],
      })

      expect(customMV.sourceNamespaces).toEqual(['custom-scores', 'eval-scores'])
    })

    test('accepts custom distribution buckets', async () => {
      const customMV = createEvalScoresMV({
        distributionBuckets: 5,
      })

      await customMV.process([
        createScoreEvent('s1', 1, 'suite', 'scorer', 0.85),
      ])

      const stats = customMV.getScorerStats('scorer')
      expect(stats?.distribution).toHaveLength(5)
    })
  })

  describe('Helper Methods', () => {
    test('getScorerNames returns unique scorer names', async () => {
      const events = [
        createScoreEvent('s1', 1, 'suite', 'accuracy', 0.8),
        createScoreEvent('s2', 1, 'suite', 'relevance', 0.7),
        createScoreEvent('s3', 1, 'suite', 'accuracy', 0.9),
      ]
      await mv.process(events)

      const scorers = mv.getScorerNames()
      expect(scorers).toContain('accuracy')
      expect(scorers).toContain('relevance')
      expect(scorers).toHaveLength(2)
    })

    test('getSuiteNames returns unique suite names', async () => {
      const events = [
        createScoreEvent('s1', 1, 'suite-a', 'scorer', 0.8),
        createScoreEvent('s2', 1, 'suite-b', 'scorer', 0.7),
        createScoreEvent('s3', 1, 'suite-a', 'scorer', 0.9),
      ]
      await mv.process(events)

      const suites = mv.getSuiteNames()
      expect(suites).toContain('suite-a')
      expect(suites).toContain('suite-b')
      expect(suites).toHaveLength(2)
    })

    test('getRunIds returns unique run IDs sorted descending', async () => {
      const events = [
        createScoreEvent('s1', 3, 'suite', 'scorer', 0.8),
        createScoreEvent('s2', 1, 'suite', 'scorer', 0.7),
        createScoreEvent('s3', 2, 'suite', 'scorer', 0.9),
      ]
      await mv.process(events)

      const runIds = mv.getRunIds()
      expect(runIds).toEqual([3, 2, 1])
    })
  })

  describe('MVHandler Interface', () => {
    test('has correct name', () => {
      expect(mv.name).toBe('EvalScores')
    })

    test('has default source namespaces', () => {
      expect(mv.sourceNamespaces).toEqual(['evalite_scores', 'scores'])
    })

    test('process returns a promise', async () => {
      const result = mv.process([createScoreEvent('s1', 1, 'suite', 'scorer', 0.8)])
      expect(result).toBeInstanceOf(Promise)
      await result
    })
  })
})

// =============================================================================
// Integration with StreamingRefreshEngine
// =============================================================================

describe('EvalScoresMV Integration', () => {
  test('can be used with streaming engine', async () => {
    const { createStreamingRefreshEngine, createEvalScoresMV } = await import('../../../src/streaming')

    const mv = createEvalScoresMV()
    const engine = createStreamingRefreshEngine()

    engine.registerMV(mv)
    await engine.start()

    // Simulate a score event
    await engine.processEvent({
      id: 'int-1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'evalite_scores:int-1',
      after: {
        runId: 1,
        suiteName: 'integration.eval.ts',
        scorerName: 'accuracy',
        score: 0.95,
      },
      actor: 'test',
    })

    await engine.flush()
    await engine.stop()

    expect(mv.count()).toBe(1)
    expect(mv.getScore('int-1')?.score).toBe(0.95)
  })

  test('processes scores from multiple namespaces', async () => {
    const { createStreamingRefreshEngine, createEvalScoresMV } = await import('../../../src/streaming')

    const mv = createEvalScoresMV({
      sourceNamespaces: ['evalite_scores', 'scores', 'custom_scores'],
    })
    const engine = createStreamingRefreshEngine()

    engine.registerMV(mv)
    await engine.start()

    // Simulate events from different namespaces
    await engine.processEvent({
      id: 's1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'evalite_scores:s1',
      after: { runId: 1, suiteName: 'test', scorerName: 'scorer', score: 0.8 },
      actor: 'test',
    })

    await engine.processEvent({
      id: 's2',
      ts: Date.now(),
      op: 'CREATE',
      target: 'scores:s2',
      after: { runId: 1, suiteName: 'test', scorerName: 'scorer', score: 0.85 },
      actor: 'test',
    })

    await engine.processEvent({
      id: 's3',
      ts: Date.now(),
      op: 'CREATE',
      target: 'custom_scores:s3',
      after: { runId: 1, suiteName: 'test', scorerName: 'scorer', score: 0.9 },
      actor: 'test',
    })

    await engine.flush()
    await engine.stop()

    expect(mv.count()).toBe(3)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('EvalScoresMV Edge Cases', () => {
  let mv: EvalScoresMV

  beforeEach(() => {
    mv = createEvalScoresMV()
  })

  test('handles score value of 0', async () => {
    const event = createScoreEvent('s1', 1, 'suite', 'scorer', 0)
    await mv.process([event])

    expect(mv.count()).toBe(1)
    expect(mv.getScore('s1')?.score).toBe(0)
  })

  test('handles score value of 1', async () => {
    const event = createScoreEvent('s1', 1, 'suite', 'scorer', 1)
    await mv.process([event])

    expect(mv.count()).toBe(1)
    expect(mv.getScore('s1')?.score).toBe(1)
  })

  test('handles scores outside 0-1 range', async () => {
    const event = createScoreEvent('s1', 1, 'suite', 'scorer', 1.5)
    await mv.process([event])

    expect(mv.count()).toBe(1)
    expect(mv.getScore('s1')?.score).toBe(1.5)
  })

  test('handles empty suite name gracefully', async () => {
    const event = createEvent('s1', {
      runId: 1,
      suiteName: '',
      scorerName: 'scorer',
      score: 0.8,
    })
    await mv.process([event])

    // Empty string is falsy, so event should be ignored
    expect(mv.count()).toBe(0)
  })

  test('handles empty scorer name gracefully', async () => {
    const event = createEvent('s1', {
      runId: 1,
      suiteName: 'suite',
      scorerName: '',
      score: 0.8,
    })
    await mv.process([event])

    // Empty string is falsy, so event should be ignored
    expect(mv.count()).toBe(0)
  })

  test('handles very large number of scores', async () => {
    const mv = createEvalScoresMV({ maxScores: 1000 })

    const events = Array.from({ length: 1000 }, (_, i) =>
      createScoreEvent(`s${i}`, Math.floor(i / 100), 'suite', 'scorer', Math.random())
    )

    await mv.process(events)

    expect(mv.count()).toBe(1000)
    const stats = mv.getStats()
    expect(stats.uniqueRuns).toBe(10)
  })

  test('statistics cache is invalidated on new score', async () => {
    await mv.process([createScoreEvent('s1', 1, 'suite', 'scorer', 0.8)])
    const stats1 = mv.getStats()

    await mv.process([createScoreEvent('s2', 1, 'suite', 'scorer', 0.9)])
    const stats2 = mv.getStats()

    expect(stats2.totalScores).toBe(2)
    expect(stats2.globalAverageScore).not.toBe(stats1.globalAverageScore)
  })

  test('getScoreTrends returns empty array for no scores', () => {
    const trends = mv.getScoreTrends()
    expect(trends).toEqual([])
  })

  test('handles DELETE events (no processing)', async () => {
    const event: Event = {
      id: 's1',
      ts: Date.now(),
      op: 'DELETE',
      target: 'evalite_scores:s1',
      before: { runId: 1, suiteName: 'suite', scorerName: 'scorer', score: 0.8 },
      actor: 'test',
    }
    await mv.process([event])

    // DELETE events don't have 'after' so they shouldn't be processed
    expect(mv.count()).toBe(0)
  })
})

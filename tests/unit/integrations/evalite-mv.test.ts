/**
 * Tests for Evalite MV Integration
 *
 * Tests the materialized views integration for the Evalite adapter,
 * covering built-in views, custom views, and analytics queries.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  createEvaliteAdapter,
  createEvaliteMVIntegration,
  ParqueDBEvaliteAdapter,
  EvaliteMVIntegration,
  type RunStatisticsData,
  type SuitePerformanceData,
  type ScorerAnalysisData,
  type ScoreTrendData,
  type TokenUsageBySuiteData,
  type FailurePatternData,
  type EvaliteMVEvent,
} from '../../../src/integrations/evalite'

describe('EvaliteMVIntegration', () => {
  let adapter: ParqueDBEvaliteAdapter
  let mvIntegration: EvaliteMVIntegration

  beforeEach(async () => {
    const storage = new MemoryBackend()
    mvIntegration = createEvaliteMVIntegration({
      batchSize: 10,
      batchTimeoutMs: 100,
      enableBuiltinViews: true,
    })
    await mvIntegration.start()

    adapter = createEvaliteAdapter({
      storage,
      collectionPrefix: 'evalite_mv_test',
      mvIntegration,
    })
    await adapter.init()
  })

  afterEach(async () => {
    await mvIntegration.stop()
  })

  describe('initialization', () => {
    it('should create MV integration with default config', async () => {
      const integration = createEvaliteMVIntegration()
      expect(integration).toBeInstanceOf(EvaliteMVIntegration)
      expect(integration.isRunning()).toBe(false)
    })

    it('should create MV integration with custom config', () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 50,
        batchTimeoutMs: 500,
        enableBuiltinViews: false,
        retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
      })
      expect(integration).toBeInstanceOf(EvaliteMVIntegration)
    })

    it('should start and stop correctly', async () => {
      const integration = createEvaliteMVIntegration()
      expect(integration.isRunning()).toBe(false)

      await integration.start()
      expect(integration.isRunning()).toBe(true)

      await integration.stop()
      expect(integration.isRunning()).toBe(false)
    })
  })

  describe('built-in views', () => {
    it('should have built-in view names available', () => {
      const viewNames = mvIntegration.getViewNames()
      expect(viewNames).toContain('score_trends')
      expect(viewNames).toContain('run_statistics')
      expect(viewNames).toContain('suite_performance')
      expect(viewNames).toContain('scorer_analysis')
      expect(viewNames).toContain('token_usage_by_suite')
      expect(viewNames).toContain('failure_patterns')
    })

    it('should disable built-in views when configured', async () => {
      const integration = createEvaliteMVIntegration({
        enableBuiltinViews: false,
      })
      const viewNames = integration.getViewNames()
      expect(viewNames).not.toContain('score_trends')
      expect(viewNames).not.toContain('run_statistics')
    })
  })

  describe('run_statistics view', () => {
    it('should track basic run statistics', async () => {
      // Create some runs
      await adapter.runs.create({ runType: 'full' })
      await adapter.runs.create({ runType: 'full' })
      await adapter.runs.create({ runType: 'partial' })

      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')

      expect(stats).toBeDefined()
      expect(stats!.totalRuns).toBe(3)
      expect(stats!.runsByType.full).toBe(2)
      expect(stats!.runsByType.partial).toBe(1)
    })

    it('should track suite and eval counts', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })
      await adapter.evals.create({ suiteId: suite.id, input: 'test1' })
      await adapter.evals.create({ suiteId: suite.id, input: 'test2' })

      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')

      expect(stats).toBeDefined()
      expect(stats!.totalSuites).toBe(1)
      expect(stats!.totalEvals).toBe(2)
    })

    it('should calculate average score', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'accuracy',
        score: 0.8,
      })
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'relevance',
        score: 0.6,
      })

      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')

      expect(stats).toBeDefined()
      expect(stats!.totalScores).toBe(2)
      expect(stats!.avgScoreOverall).toBeCloseTo(0.7)
    })

    it('should calculate success rate', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })

      await adapter.evals.create({
        suiteId: suite.id,
        input: 'test1',
        status: 'success',
      })
      await adapter.evals.create({
        suiteId: suite.id,
        input: 'test2',
        status: 'success',
      })
      await adapter.evals.create({
        suiteId: suite.id,
        input: 'test3',
        status: 'fail',
      })

      // Update statuses
      await adapter.evals.update({ id: 1, status: 'success' })
      await adapter.evals.update({ id: 2, status: 'success' })
      await adapter.evals.update({ id: 3, status: 'fail' })

      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')

      expect(stats).toBeDefined()
      // 2 success out of 3 = 66.67%
      expect(stats!.successRate).toBeCloseTo(0.6667, 2)
    })
  })

  describe('suite_performance view', () => {
    it('should track performance by suite name', async () => {
      // Create runs with same suite name
      const run1 = await adapter.runs.create({})
      const suite1 = await adapter.suites.create({
        runId: run1.id,
        name: 'my-eval.eval.ts',
        status: 'success',
      })
      await adapter.suites.update({ id: suite1.id, duration: 1000 })

      const run2 = await adapter.runs.create({})
      const suite2 = await adapter.suites.create({
        runId: run2.id,
        name: 'my-eval.eval.ts',
        status: 'success',
      })
      await adapter.suites.update({ id: suite2.id, duration: 2000 })

      const perfMap = await mvIntegration.query<Map<string, SuitePerformanceData>>('suite_performance')

      expect(perfMap).toBeDefined()
      const perf = perfMap!.get('my-eval.eval.ts')
      expect(perf).toBeDefined()
      expect(perf!.runCount).toBe(2)
      expect(perf!.avgDuration).toBe(1500)
      expect(perf!.successRate).toBe(1.0)
    })

    it('should track average scores by scorer', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })
      const eval1 = await adapter.evals.create({ suiteId: suite.id, input: 'a' })
      const eval2 = await adapter.evals.create({ suiteId: suite.id, input: 'b' })

      await adapter.scores.create({ evalId: eval1.id, name: 'accuracy', score: 0.8 })
      await adapter.scores.create({ evalId: eval2.id, name: 'accuracy', score: 0.6 })

      const perfMap = await mvIntegration.query<Map<string, SuitePerformanceData>>('suite_performance')

      expect(perfMap).toBeDefined()
      const perf = perfMap!.get('test-suite')
      expect(perf).toBeDefined()
      expect(perf!.avgScores.accuracy).toBeCloseTo(0.7)
    })
  })

  describe('scorer_analysis view', () => {
    it('should analyze scorer statistics', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })

      // Create multiple evals with scores
      for (let i = 0; i < 5; i++) {
        const evalResult = await adapter.evals.create({
          suiteId: suite.id,
          input: `test ${i}`,
        })
        await adapter.scores.create({
          evalId: evalResult.id,
          name: 'accuracy',
          score: 0.5 + i * 0.1, // 0.5, 0.6, 0.7, 0.8, 0.9
        })
      }

      const analysisMap = await mvIntegration.query<Map<string, ScorerAnalysisData>>('scorer_analysis')

      expect(analysisMap).toBeDefined()
      const analysis = analysisMap!.get('accuracy')
      expect(analysis).toBeDefined()
      expect(analysis!.totalScores).toBe(5)
      expect(analysis!.avgScore).toBeCloseTo(0.7)
      expect(analysis!.minScore).toBeCloseTo(0.5)
      expect(analysis!.maxScore).toBeCloseTo(0.9)
    })

    it('should calculate score distribution', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite',
      })

      // Create scores across different buckets
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      // 0.1 (bucket 1), 0.5 (bucket 5), 0.9 (bucket 9)
      await adapter.scores.create({ evalId: evalResult.id, name: 'mixed', score: 0.15 })
      await adapter.scores.create({ evalId: evalResult.id, name: 'mixed', score: 0.55 })
      await adapter.scores.create({ evalId: evalResult.id, name: 'mixed', score: 0.95 })

      const analysisMap = await mvIntegration.query<Map<string, ScorerAnalysisData>>('scorer_analysis')

      expect(analysisMap).toBeDefined()
      const analysis = analysisMap!.get('mixed')
      expect(analysis).toBeDefined()
      expect(analysis!.distribution).toHaveLength(10)
    })
  })

  describe('score_trends view', () => {
    it('should track score trends over runs', async () => {
      // Create multiple runs with same suite name
      for (let i = 0; i < 3; i++) {
        const run = await adapter.runs.create({})
        const suite = await adapter.suites.create({
          runId: run.id,
          name: 'trend-suite',
        })
        const evalResult = await adapter.evals.create({
          suiteId: suite.id,
          input: `test ${i}`,
        })
        await adapter.scores.create({
          evalId: evalResult.id,
          name: 'quality',
          score: 0.6 + i * 0.1, // Improving scores: 0.6, 0.7, 0.8
        })
      }

      const trendsMap = await mvIntegration.query<Map<string, ScoreTrendData>>('score_trends')

      expect(trendsMap).toBeDefined()
      const trend = trendsMap!.get('trend-suite:quality')
      expect(trend).toBeDefined()
      expect(trend!.dataPoints).toHaveLength(3)
    })
  })

  describe('token_usage_by_suite view', () => {
    it('should track token usage by suite', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'token-suite',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      await adapter.traces.create({
        evalId: evalResult.id,
        input: { messages: [] },
        output: { content: 'response' },
        startTime: 1000,
        endTime: 1100,
        inputTokens: 50,
        outputTokens: 30,
        totalTokens: 80,
      })
      await adapter.traces.create({
        evalId: evalResult.id,
        input: { messages: [] },
        output: { content: 'response 2' },
        startTime: 1100,
        endTime: 1200,
        inputTokens: 60,
        outputTokens: 40,
        totalTokens: 100,
      })

      const usageMap = await mvIntegration.query<Map<string, TokenUsageBySuiteData>>('token_usage_by_suite')

      expect(usageMap).toBeDefined()
      const usage = usageMap!.get('token-suite')
      expect(usage).toBeDefined()
      expect(usage!.totalTokens).toBe(180)
      expect(usage!.inputTokens).toBe(110)
      expect(usage!.outputTokens).toBe(70)
      expect(usage!.traceCount).toBe(2)
    })
  })

  describe('failure_patterns view', () => {
    it('should track failure patterns', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'failing-suite',
      })

      // Create successful and failed evals
      const successEval = await adapter.evals.create({
        suiteId: suite.id,
        input: 'success',
        status: 'success',
      })
      await adapter.evals.update({ id: successEval.id, status: 'success' })

      const failEval1 = await adapter.evals.create({
        suiteId: suite.id,
        input: 'fail1',
        status: 'fail',
      })
      await adapter.evals.update({ id: failEval1.id, status: 'fail' })

      const failEval2 = await adapter.evals.create({
        suiteId: suite.id,
        input: 'fail2',
        status: 'fail',
      })
      await adapter.evals.update({ id: failEval2.id, status: 'fail' })

      // Add low scores to failed evals
      await adapter.scores.create({
        evalId: failEval1.id,
        name: 'accuracy',
        score: 0.2,
      })
      await adapter.scores.create({
        evalId: failEval2.id,
        name: 'accuracy',
        score: 0.1,
      })

      const patternsMap = await mvIntegration.query<Map<string, FailurePatternData>>('failure_patterns')

      expect(patternsMap).toBeDefined()
      const pattern = patternsMap!.get('failing-suite')
      expect(pattern).toBeDefined()
      expect(pattern!.totalEvals).toBe(3)
      expect(pattern!.failedEvals).toBe(2)
      expect(pattern!.failureRate).toBeCloseTo(0.6667, 2)
    })
  })

  describe('custom views', () => {
    it('should support registering custom views', async () => {
      let aggregateCalled = false

      mvIntegration.registerView<{ count: number }>({
        name: 'custom_count',
        description: 'Count all scores',
        entityTypes: ['score'],
        aggregate: (events, existingState) => {
          aggregateCalled = true
          const count = (existingState?.count ?? 0) + events.length
          return { count }
        },
      })

      expect(mvIntegration.getViewNames()).toContain('custom_count')

      // Add some scores to trigger the view
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'test',
        score: 0.5,
      })

      // Flush and refresh to process events
      await mvIntegration.refresh()

      expect(aggregateCalled).toBe(true)
    })

    it('should support unregistering custom views', () => {
      mvIntegration.registerView({
        name: 'temp_view',
        entityTypes: ['run'],
        aggregate: () => ({}),
      })

      expect(mvIntegration.getViewNames()).toContain('temp_view')

      mvIntegration.unregisterView('temp_view')

      expect(mvIntegration.getViewNames()).not.toContain('temp_view')
    })
  })

  describe('query methods', () => {
    it('should query raw runs with filters', async () => {
      await adapter.runs.create({ runType: 'full' })
      await adapter.runs.create({ runType: 'partial' })
      await adapter.runs.create({ runType: 'full' })

      const allRuns = await mvIntegration.queryRuns()
      expect(allRuns).toHaveLength(3)

      const limitedRuns = await mvIntegration.queryRuns({ limit: 2 })
      expect(limitedRuns).toHaveLength(2)
    })

    it('should query raw suites with filters', async () => {
      const run = await adapter.runs.create({})
      await adapter.suites.create({ runId: run.id, name: 'suite-a' })
      await adapter.suites.create({ runId: run.id, name: 'suite-b' })

      const allSuites = await mvIntegration.querySuites()
      expect(allSuites).toHaveLength(2)

      const filteredSuites = await mvIntegration.querySuites({
        suiteName: 'suite-a',
      })
      expect(filteredSuites).toHaveLength(1)
    })

    it('should query raw scores with filters', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'accuracy',
        score: 0.8,
      })
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'relevance',
        score: 0.7,
      })

      const allScores = await mvIntegration.queryScores()
      expect(allScores).toHaveLength(2)

      const filteredScores = await mvIntegration.queryScores({
        scorerName: 'accuracy',
      })
      expect(filteredScores).toHaveLength(1)
    })
  })

  describe('state management', () => {
    it('should report state correctly', async () => {
      const state = mvIntegration.getState()

      expect(state.isRunning).toBe(true)
      expect(state.viewCount).toBeGreaterThan(0)
      expect(state.viewNames).toContain('run_statistics')
    })

    it('should track events processed', async () => {
      const initialState = mvIntegration.getState()
      const initialCount = initialState.eventsProcessed

      await adapter.runs.create({})
      await adapter.runs.create({})

      // Flush to ensure processing
      await mvIntegration.refresh()

      const finalState = mvIntegration.getState()
      expect(finalState.eventsProcessed).toBeGreaterThan(initialCount)
    })

    it('should clear data correctly', async () => {
      // Create a fresh integration for this test to avoid interference
      const freshIntegration = createEvaliteMVIntegration({
        batchSize: 10,
        batchTimeoutMs: 100,
        enableBuiltinViews: true,
      })
      await freshIntegration.start()

      // Manually add data
      await freshIntegration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })
      await freshIntegration.processRun({
        id: 2,
        runType: 'partial',
        createdAt: new Date().toISOString(),
      })

      // Ensure data is flushed
      await freshIntegration.refresh()

      const beforeRuns = await freshIntegration.queryRuns()
      expect(beforeRuns).toHaveLength(2)

      // Now clear
      freshIntegration.clear()

      const runs = await freshIntegration.queryRuns()
      expect(runs).toHaveLength(0)

      const state = freshIntegration.getState()
      expect(state.eventsProcessed).toBe(0)

      await freshIntegration.stop()
    })
  })

  describe('retention policy', () => {
    it('should apply retention policy', async () => {
      // Create integration with very short retention
      const shortRetentionIntegration = createEvaliteMVIntegration({
        retentionMs: 1, // 1ms retention
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await shortRetentionIntegration.start()

      // Manually add old data
      await shortRetentionIntegration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date(Date.now() - 10000).toISOString(), // 10 seconds ago
      })

      // Ensure data is flushed
      await shortRetentionIntegration.refresh()

      // Verify data was added
      const beforeRuns = await shortRetentionIntegration.queryRuns()
      expect(beforeRuns).toHaveLength(1)

      // Advance time past retention period using fake timers
      vi.useFakeTimers()
      vi.advanceTimersByTime(10)
      vi.useRealTimers()

      const removed = shortRetentionIntegration.applyRetention()

      expect(removed).toBeGreaterThan(0)

      const afterRuns = await shortRetentionIntegration.queryRuns()
      expect(afterRuns).toHaveLength(0)

      await shortRetentionIntegration.stop()
    })
  })

  describe('adapter integration', () => {
    it('should automatically process runs through MV integration', async () => {
      const run = await adapter.runs.create({})

      const runs = await mvIntegration.queryRuns()
      expect(runs.some(r => r.id === run.id)).toBe(true)
    })

    it('should automatically process suites through MV integration', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'auto-suite',
      })

      const suites = await mvIntegration.querySuites()
      expect(suites.some(s => s.id === suite.id)).toBe(true)
    })

    it('should automatically process evals through MV integration', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      await mvIntegration.refresh()

      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')
      expect(stats!.totalEvals).toBeGreaterThan(0)
    })

    it('should automatically process scores through MV integration', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test',
      })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'auto-score',
        score: 0.75,
      })

      const scores = await mvIntegration.queryScores()
      expect(scores.some(s => s.name === 'auto-score')).toBe(true)
    })
  })

  describe('edge cases', () => {
    it('should handle querying non-existent view', async () => {
      const result = await mvIntegration.query('non_existent_view')
      expect(result).toBeUndefined()
    })

    it('should handle empty data gracefully', async () => {
      const stats = await mvIntegration.query<RunStatisticsData>('run_statistics')

      expect(stats).toBeDefined()
      expect(stats!.totalRuns).toBe(0)
      expect(stats!.avgEvalsPerRun).toBe(0)
    })

    it('should handle concurrent operations', async () => {
      // Create multiple operations concurrently
      const promises = []
      for (let i = 0; i < 10; i++) {
        promises.push(adapter.runs.create({}))
      }

      await Promise.all(promises)

      const runs = await mvIntegration.queryRuns()
      expect(runs).toHaveLength(10)
    })
  })

  describe('type safety and runtime validation', () => {
    it('should safely handle invalid event data in run handler', async () => {
      // Manually process an event with malformed data
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Process a run with correct data
      await integration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })

      await integration.refresh()
      const runs = await integration.queryRuns()
      expect(runs).toHaveLength(1)
      expect(runs[0]?.id).toBe(1)

      await integration.stop()
    })

    it('should safely handle missing required fields', async () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Process valid data
      await integration.processRun({
        id: 2,
        runType: 'partial',
        createdAt: new Date().toISOString(),
      })

      await integration.refresh()
      const runs = await integration.queryRuns()
      // Only valid runs should be stored
      expect(runs.some(r => r.id === 2)).toBe(true)

      await integration.stop()
    })

    it('should validate suite data correctly', async () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Create run first
      await integration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })

      // Create valid suite
      await integration.processSuite({
        id: 1,
        runId: 1,
        name: 'test-suite',
        status: 'running',
        duration: 0,
        createdAt: new Date().toISOString(),
      })

      await integration.refresh()
      const suites = await integration.querySuites()
      expect(suites).toHaveLength(1)
      expect(suites[0]?.name).toBe('test-suite')

      await integration.stop()
    })

    it('should validate score data correctly', async () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Create full hierarchy
      await integration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })

      await integration.processSuite({
        id: 1,
        runId: 1,
        name: 'test-suite',
        status: 'running',
        duration: 0,
        createdAt: new Date().toISOString(),
      })

      await integration.processEval({
        id: 1,
        suiteId: 1,
        duration: 100,
        input: 'test',
        output: 'result',
        status: 'success',
        colOrder: 0,
        createdAt: new Date().toISOString(),
      })

      // Create valid score
      await integration.processScore({
        id: 1,
        evalId: 1,
        name: 'accuracy',
        score: 0.95,
        createdAt: new Date().toISOString(),
      })

      await integration.refresh()
      const scores = await integration.queryScores()
      expect(scores).toHaveLength(1)
      expect(scores[0]?.score).toBe(0.95)

      await integration.stop()
    })

    it('should validate trace data correctly', async () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Create full hierarchy
      await integration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })

      await integration.processSuite({
        id: 1,
        runId: 1,
        name: 'test-suite',
        status: 'running',
        duration: 0,
        createdAt: new Date().toISOString(),
      })

      await integration.processEval({
        id: 1,
        suiteId: 1,
        duration: 100,
        input: 'test',
        output: 'result',
        status: 'success',
        colOrder: 0,
        createdAt: new Date().toISOString(),
      })

      // Create valid trace
      await integration.processTrace({
        id: 1,
        evalId: 1,
        input: { prompt: 'test' },
        output: { response: 'answer' },
        startTime: 1000,
        endTime: 1100,
        colOrder: 0,
        createdAt: new Date().toISOString(),
        totalTokens: 50,
        inputTokens: 20,
        outputTokens: 30,
      })

      await integration.refresh()

      // Verify token usage view updates correctly
      const usageMap = await integration.query<Map<string, TokenUsageBySuiteData>>('token_usage_by_suite')
      expect(usageMap).toBeDefined()
      const usage = usageMap?.get('test-suite')
      expect(usage?.totalTokens).toBe(50)

      await integration.stop()
    })

    it('should handle updates to existing entities', async () => {
      const integration = createEvaliteMVIntegration({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      await integration.start()

      // Create run
      await integration.processRun({
        id: 1,
        runType: 'full',
        createdAt: new Date().toISOString(),
      })

      // Update the same run
      await integration.processRun(
        {
          id: 1,
          runType: 'partial',
          createdAt: new Date().toISOString(),
        },
        'update'
      )

      await integration.refresh()
      const runs = await integration.queryRuns()
      expect(runs).toHaveLength(1)
      expect(runs[0]?.runType).toBe('partial')

      await integration.stop()
    })
  })
})

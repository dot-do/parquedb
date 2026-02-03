/**
 * Tests for Evalite Integration Adapter
 *
 * Tests the ParqueDB adapter for Evalite's Storage interface,
 * covering runs, suites, evals, scores, and traces.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  ParqueDBEvaliteAdapter,
  createEvaliteAdapter,
} from '../../../src/integrations/evalite'

describe('ParqueDBEvaliteAdapter', () => {
  let adapter: ParqueDBEvaliteAdapter

  beforeEach(async () => {
    const storage = new MemoryBackend()
    adapter = createEvaliteAdapter({
      storage,
      collectionPrefix: 'evalite_test',
      debug: false,
    })
    await adapter.init()
  })

  describe('initialization', () => {
    it('should create adapter with default config', () => {
      const storage = new MemoryBackend()
      const adapter = createEvaliteAdapter({ storage })
      expect(adapter).toBeInstanceOf(ParqueDBEvaliteAdapter)
      expect(adapter.getConfig().collectionPrefix).toBe('evalite')
    })

    it('should create adapter with custom config', () => {
      const storage = new MemoryBackend()
      const adapter = createEvaliteAdapter({
        storage,
        collectionPrefix: 'my_evals',
        debug: true,
      })
      expect(adapter.getConfig().collectionPrefix).toBe('my_evals')
      expect(adapter.getConfig().debug).toBe(true)
    })
  })

  describe('runs', () => {
    it('should create a run with default type', async () => {
      const run = await adapter.runs.create({})
      expect(run.id).toBe(1)
      expect(run.runType).toBe('full')
      expect(run.createdAt).toBeDefined()
    })

    it('should create a run with partial type', async () => {
      const run = await adapter.runs.create({ runType: 'partial' })
      expect(run.id).toBe(1)
      expect(run.runType).toBe('partial')
    })

    it('should increment run IDs', async () => {
      const run1 = await adapter.runs.create({})
      const run2 = await adapter.runs.create({})
      const run3 = await adapter.runs.create({})

      expect(run1.id).toBe(1)
      expect(run2.id).toBe(2)
      expect(run3.id).toBe(3)
    })

    it('should get many runs', async () => {
      await adapter.runs.create({ runType: 'full' })
      await adapter.runs.create({ runType: 'partial' })
      await adapter.runs.create({ runType: 'full' })

      const allRuns = await adapter.runs.getMany()
      expect(allRuns).toHaveLength(3)

      const fullRuns = await adapter.runs.getMany({ runType: 'full' })
      expect(fullRuns).toHaveLength(2)
    })

    it('should support pagination', async () => {
      for (let i = 0; i < 5; i++) {
        await adapter.runs.create({})
      }

      const page1 = await adapter.runs.getMany({ limit: 2 })
      expect(page1).toHaveLength(2)

      const page2 = await adapter.runs.getMany({ limit: 2, offset: 2 })
      expect(page2).toHaveLength(2)
    })
  })

  describe('suites', () => {
    it('should create a suite', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test-suite.eval.ts',
      })

      expect(suite.id).toBe(1)
      expect(suite.runId).toBe(run.id)
      expect(suite.name).toBe('test-suite.eval.ts')
      expect(suite.status).toBe('running')
      expect(suite.duration).toBe(0)
    })

    it('should update a suite', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'my-suite',
      })

      const updated = await adapter.suites.update({
        id: suite.id,
        status: 'success',
        duration: 1500,
      })

      expect(updated.status).toBe('success')
      expect(updated.duration).toBe(1500)
    })

    it('should get suites by run ID', async () => {
      const run1 = await adapter.runs.create({})
      const run2 = await adapter.runs.create({})

      await adapter.suites.create({ runId: run1.id, name: 'suite-a' })
      await adapter.suites.create({ runId: run1.id, name: 'suite-b' })
      await adapter.suites.create({ runId: run2.id, name: 'suite-c' })

      const run1Suites = await adapter.suites.getMany({ runId: run1.id })
      expect(run1Suites).toHaveLength(2)

      const run2Suites = await adapter.suites.getMany({ runId: run2.id })
      expect(run2Suites).toHaveLength(1)
    })
  })

  describe('evals', () => {
    it('should create an eval', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })

      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: { prompt: 'Hello' },
        output: { response: 'Hi there!' },
        expected: { response: 'Hello!' },
      })

      expect(evalResult.id).toBe(1)
      expect(evalResult.suiteId).toBe(suite.id)
      expect(evalResult.input).toEqual({ prompt: 'Hello' })
      expect(evalResult.output).toEqual({ response: 'Hi there!' })
      expect(evalResult.expected).toEqual({ response: 'Hello!' })
      expect(evalResult.status).toBe('running')
    })

    it('should update an eval', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      const updated = await adapter.evals.update({
        id: evalResult.id,
        output: 'result',
        status: 'success',
        duration: 250,
      })

      expect(updated.output).toBe('result')
      expect(updated.status).toBe('success')
      expect(updated.duration).toBe(250)
    })

    it('should get evals with scores and traces', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
        output: 'result',
      })

      // Add scores
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'accuracy',
        score: 0.85,
      })
      await adapter.scores.create({
        evalId: evalResult.id,
        name: 'relevance',
        score: 0.92,
      })

      // Add trace
      await adapter.traces.create({
        evalId: evalResult.id,
        input: { messages: [] },
        output: { content: 'response' },
        startTime: 1000,
        endTime: 1250,
        totalTokens: 100,
      })

      // Get eval with scores and traces
      const evals = await adapter.evals.getMany({
        suiteId: suite.id,
        includeScores: true,
        includeTraces: true,
      })

      expect(evals).toHaveLength(1)
      expect(evals[0]!.scores).toHaveLength(2)
      expect(evals[0]!.traces).toHaveLength(1)
    })
  })

  describe('scores', () => {
    it('should create a score', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
      })

      const score = await adapter.scores.create({
        evalId: evalResult.id,
        name: 'factuality',
        score: 0.75,
        description: 'How factually accurate the response is',
      })

      expect(score.id).toBe(1)
      expect(score.evalId).toBe(evalResult.id)
      expect(score.name).toBe('factuality')
      expect(score.score).toBe(0.75)
      expect(score.description).toBe('How factually accurate the response is')
    })

    it('should get scores by eval ID', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const eval1 = await adapter.evals.create({ suiteId: suite.id, input: 'a' })
      const eval2 = await adapter.evals.create({ suiteId: suite.id, input: 'b' })

      await adapter.scores.create({ evalId: eval1.id, name: 'score1', score: 0.5 })
      await adapter.scores.create({ evalId: eval1.id, name: 'score2', score: 0.6 })
      await adapter.scores.create({ evalId: eval2.id, name: 'score1', score: 0.7 })

      const eval1Scores = await adapter.scores.getMany({ evalId: eval1.id })
      expect(eval1Scores).toHaveLength(2)

      const eval2Scores = await adapter.scores.getMany({ evalId: eval2.id })
      expect(eval2Scores).toHaveLength(1)
    })

    it('should get scores by name', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({ suiteId: suite.id, input: 'x' })

      await adapter.scores.create({ evalId: evalResult.id, name: 'accuracy', score: 0.8 })
      await adapter.scores.create({ evalId: evalResult.id, name: 'relevance', score: 0.9 })
      await adapter.scores.create({ evalId: evalResult.id, name: 'accuracy', score: 0.85 })

      const accuracyScores = await adapter.scores.getMany({ name: 'accuracy' })
      expect(accuracyScores).toHaveLength(2)
    })
  })

  describe('traces', () => {
    it('should create a trace', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({ suiteId: suite.id, input: 'x' })

      const trace = await adapter.traces.create({
        evalId: evalResult.id,
        input: { messages: [{ role: 'user', content: 'Hello' }] },
        output: { content: 'Hi!' },
        startTime: 1704067200000,
        endTime: 1704067200500,
        inputTokens: 10,
        outputTokens: 5,
        totalTokens: 15,
      })

      expect(trace.id).toBe(1)
      expect(trace.evalId).toBe(evalResult.id)
      expect(trace.inputTokens).toBe(10)
      expect(trace.outputTokens).toBe(5)
      expect(trace.totalTokens).toBe(15)
    })

    it('should get traces by eval ID', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({ runId: run.id, name: 'test' })
      const evalResult = await adapter.evals.create({ suiteId: suite.id, input: 'x' })

      await adapter.traces.create({
        evalId: evalResult.id,
        input: 'call 1',
        output: 'response 1',
        startTime: 1000,
        endTime: 1100,
        colOrder: 0,
      })
      await adapter.traces.create({
        evalId: evalResult.id,
        input: 'call 2',
        output: 'response 2',
        startTime: 1100,
        endTime: 1200,
        colOrder: 1,
      })

      const traces = await adapter.traces.getMany({ evalId: evalResult.id })
      expect(traces).toHaveLength(2)
      expect(traces[0]!.colOrder).toBe(0)
      expect(traces[1]!.colOrder).toBe(1)
    })
  })

  describe('getRunWithResults', () => {
    it('should return null for non-existent run', async () => {
      const result = await adapter.getRunWithResults(999)
      expect(result).toBeNull()
    })

    it('should return run with full results', async () => {
      // Create run
      const run = await adapter.runs.create({})

      // Create suite
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'my-eval.eval.ts',
        status: 'success',
      })

      // Create evals with scores and traces
      const eval1 = await adapter.evals.create({
        suiteId: suite.id,
        input: 'input 1',
        output: 'output 1',
        status: 'success',
      })

      const eval2 = await adapter.evals.create({
        suiteId: suite.id,
        input: 'input 2',
        output: 'output 2',
        status: 'fail',
      })

      // Add scores
      await adapter.scores.create({ evalId: eval1.id, name: 'accuracy', score: 0.9 })
      await adapter.scores.create({ evalId: eval2.id, name: 'accuracy', score: 0.3 })

      // Add traces
      await adapter.traces.create({
        evalId: eval1.id,
        input: 'llm input',
        output: 'llm output',
        startTime: 1000,
        endTime: 1100,
        totalTokens: 50,
      })

      // Get run with results
      const result = await adapter.getRunWithResults(run.id)

      expect(result).not.toBeNull()
      expect(result!.id).toBe(run.id)
      expect(result!.suites).toHaveLength(1)
      expect(result!.suites[0]!.evals).toHaveLength(2)

      // Check stats
      expect(result!.stats.totalSuites).toBe(1)
      expect(result!.stats.totalEvals).toBe(2)
      expect(result!.stats.successCount).toBe(1)
      expect(result!.stats.failCount).toBe(1)
      expect(result!.stats.totalTokens).toBe(50)
    })
  })

  describe('getScoreHistory', () => {
    it('should return score history for evaluation name', async () => {
      // Create multiple runs with same suite name
      for (let i = 0; i < 3; i++) {
        const run = await adapter.runs.create({})
        const suite = await adapter.suites.create({
          runId: run.id,
          name: 'sentiment-analysis.eval.ts',
          status: 'success',
        })

        const evalResult = await adapter.evals.create({
          suiteId: suite.id,
          input: `test ${i}`,
          output: `result ${i}`,
          status: 'success',
        })

        await adapter.scores.create({
          evalId: evalResult.id,
          name: 'accuracy',
          score: 0.7 + i * 0.1, // 0.7, 0.8, 0.9
        })
      }

      const history = await adapter.getScoreHistory('sentiment-analysis.eval.ts')

      expect(history).toHaveLength(3)
      expect(history[0]!.averageScore).toBeCloseTo(0.7)
      expect(history[2]!.averageScore).toBeCloseTo(0.9)
    })

    it('should filter by scorer name', async () => {
      const run = await adapter.runs.create({})
      const suite = await adapter.suites.create({
        runId: run.id,
        name: 'test.eval.ts',
        status: 'success',
      })

      const evalResult = await adapter.evals.create({
        suiteId: suite.id,
        input: 'test',
        output: 'result',
        status: 'success',
      })

      await adapter.scores.create({ evalId: evalResult.id, name: 'accuracy', score: 0.9 })
      await adapter.scores.create({ evalId: evalResult.id, name: 'relevance', score: 0.5 })

      const history = await adapter.getScoreHistory('test.eval.ts', {
        scorerName: 'accuracy',
      })

      expect(history).toHaveLength(1)
      expect(history[0]!.averageScore).toBe(0.9)
    })
  })

  describe('saveResults convenience method', () => {
    it('should save complete results for a run', async () => {
      const run = await adapter.runs.create({})

      await adapter.saveResults(run.id, 'qa-eval.eval.ts', [
        {
          input: { question: 'What is 2+2?' },
          output: { answer: '4' },
          expected: { answer: '4' },
          scores: [
            { name: 'correctness', score: 1.0 },
            { name: 'format', score: 0.9 },
          ],
          traces: [
            {
              input: { prompt: 'Calculate 2+2' },
              output: { result: '4' },
              startTime: 1000,
              endTime: 1100,
              totalTokens: 20,
            },
          ],
        },
        {
          input: { question: 'What is the capital of France?' },
          output: { answer: 'Paris' },
          expected: { answer: 'Paris' },
          scores: [
            { name: 'correctness', score: 1.0 },
            { name: 'format', score: 1.0 },
          ],
        },
      ])

      // Verify the run has results
      const result = await adapter.getRunWithResults(run.id)

      expect(result).not.toBeNull()
      expect(result!.suites).toHaveLength(1)
      expect(result!.suites[0]!.name).toBe('qa-eval.eval.ts')
      expect(result!.suites[0]!.status).toBe('success')
      expect(result!.suites[0]!.evals).toHaveLength(2)
      expect(result!.stats.totalEvals).toBe(2)
    })
  })

  describe('async dispose', () => {
    it('should support async dispose', async () => {
      const storage = new MemoryBackend()
      const adapter = createEvaliteAdapter({ storage })

      await adapter.init()
      await adapter[Symbol.asyncDispose]()

      // Should not throw
    })
  })

  describe('getDB and getConfig', () => {
    it('should expose underlying db instance', () => {
      const db = adapter.getDB()
      expect(db).toBeDefined()
    })

    it('should expose resolved config', () => {
      const config = adapter.getConfig()
      expect(config.collectionPrefix).toBe('evalite_test')
      expect(config.debug).toBe(false)
    })
  })
})

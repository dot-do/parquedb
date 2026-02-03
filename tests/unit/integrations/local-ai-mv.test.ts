/**
 * Local AI Observability Materialized Views Tests
 *
 * Comprehensive tests for the Local AI Observability MVs that auto-materialize
 * AI analytics from AI SDK and evalite to local filesystem storage.
 *
 * Tests cover:
 * - AIRequestsMV: All AI requests with latency, tokens, cost
 * - AIUsageMV: Aggregated usage by model/provider/day
 * - GeneratedContentMV: Captured AI-generated content
 * - EvalScoresMV: Evalite evaluation scores
 * - Integration with FsBackend for local development
 * - Mock AI event processing
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { FsBackend } from '../../../src/storage/FsBackend'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { ParqueDB } from '../../../src/ParqueDB'
import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
} from '../../../src/materialized-views/streaming'
import type { Event, EventOp } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a temporary directory for testing
 */
async function createTempDir(): Promise<string> {
  const tempDir = join(tmpdir(), `parquedb-ai-mv-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(tempDir, { recursive: true })
  return tempDir
}

/**
 * Clean up a temporary directory
 */
async function cleanupTempDir(dir: string): Promise<void> {
  try {
    await fs.rm(dir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
}

/**
 * Create a mock AI request event
 */
function createAIRequestEvent(opts: {
  id?: string
  modelId?: string
  providerId?: string
  requestType?: 'generate' | 'stream'
  promptTokens?: number
  completionTokens?: number
  totalTokens?: number
  latencyMs?: number
  cached?: boolean
  finishReason?: string
  error?: { name: string; message: string }
  timestamp?: Date
}): Event {
  const id = opts.id ?? `req_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()

  return {
    id: `evt_${Date.now()}`,
    ts: timestamp.getTime(),
    op: 'CREATE',
    target: `ai_requests:${id}`,
    after: {
      $id: `ai_requests/${id}`,
      $type: 'AIRequest',
      name: `request-${id}`,
      requestType: opts.requestType ?? 'generate',
      modelId: opts.modelId ?? 'gpt-4',
      providerId: opts.providerId ?? 'openai',
      promptTokens: opts.promptTokens ?? 100,
      completionTokens: opts.completionTokens ?? 50,
      totalTokens: opts.totalTokens ?? 150,
      latencyMs: opts.latencyMs ?? 500,
      cached: opts.cached ?? false,
      finishReason: opts.finishReason ?? 'stop',
      error: opts.error,
      timestamp,
    },
    actor: 'test:user',
  }
}

/**
 * Create a mock AI generated content event
 */
function createGeneratedContentEvent(opts: {
  id?: string
  modelId?: string
  contentType?: 'text' | 'object' | 'embedding'
  prompt?: string
  content?: unknown
  schema?: string
  tokens?: number
  timestamp?: Date
}): Event {
  const id = opts.id ?? `gen_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()

  return {
    id: `evt_${Date.now()}`,
    ts: timestamp.getTime(),
    op: 'CREATE',
    target: `generated_content:${id}`,
    after: {
      $id: `generated_content/${id}`,
      $type: 'GeneratedContent',
      name: `content-${id}`,
      modelId: opts.modelId ?? 'gpt-4',
      contentType: opts.contentType ?? 'text',
      prompt: opts.prompt ?? 'Hello, world!',
      content: opts.content ?? 'Hello! How can I help you today?',
      schema: opts.schema,
      tokens: opts.tokens ?? 50,
      timestamp,
    },
    actor: 'test:user',
  }
}

/**
 * Create a mock evalite score event
 */
function createEvalScoreEvent(opts: {
  id?: string
  runId?: number
  suiteName?: string
  scorerName?: string
  score?: number
  timestamp?: Date
}): Event {
  const id = opts.id ?? `score_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const timestamp = opts.timestamp ?? new Date()

  return {
    id: `evt_${Date.now()}`,
    ts: timestamp.getTime(),
    op: 'CREATE',
    target: `eval_scores:${id}`,
    after: {
      $id: `eval_scores/${id}`,
      $type: 'EvalScore',
      name: opts.scorerName ?? 'accuracy',
      runId: opts.runId ?? 1,
      suiteName: opts.suiteName ?? 'test-suite.eval.ts',
      scorerName: opts.scorerName ?? 'accuracy',
      score: opts.score ?? 0.85,
      timestamp,
    },
    actor: 'test:user',
  }
}

/**
 * Create a mock MV handler for AI requests
 */
function createAIRequestsMVHandler(): MVHandler & {
  processedEvents: Event[][]
  aggregatedUsage: Map<string, { requests: number; tokens: number; latency: number }>
} {
  const processedEvents: Event[][] = []
  const aggregatedUsage = new Map<string, { requests: number; tokens: number; latency: number }>()

  return {
    processedEvents,
    aggregatedUsage,
    name: 'AIRequestsMV',
    sourceNamespaces: ['ai_requests'],
    async process(events: Event[]): Promise<void> {
      processedEvents.push([...events])

      // Aggregate by model
      for (const event of events) {
        if (event.op === 'CREATE' && event.after) {
          const modelId = (event.after as { modelId?: string }).modelId ?? 'unknown'
          const totalTokens = (event.after as { totalTokens?: number }).totalTokens ?? 0
          const latencyMs = (event.after as { latencyMs?: number }).latencyMs ?? 0

          const existing = aggregatedUsage.get(modelId) ?? { requests: 0, tokens: 0, latency: 0 }
          aggregatedUsage.set(modelId, {
            requests: existing.requests + 1,
            tokens: existing.tokens + totalTokens,
            latency: existing.latency + latencyMs,
          })
        }
      }
    },
  }
}

/**
 * Create a mock MV handler for AI usage aggregation
 */
function createAIUsageMVHandler(): MVHandler & {
  dailyUsage: Map<string, {
    modelId: string
    providerId: string
    date: string
    requestCount: number
    totalTokens: number
    totalLatencyMs: number
    cacheHits: number
    cacheMisses: number
    errorCount: number
  }>
} {
  const dailyUsage = new Map<string, {
    modelId: string
    providerId: string
    date: string
    requestCount: number
    totalTokens: number
    totalLatencyMs: number
    cacheHits: number
    cacheMisses: number
    errorCount: number
  }>()

  return {
    dailyUsage,
    name: 'AIUsageMV',
    sourceNamespaces: ['ai_requests'],
    async process(events: Event[]): Promise<void> {
      for (const event of events) {
        if (event.op === 'CREATE' && event.after) {
          const data = event.after as {
            modelId?: string
            providerId?: string
            totalTokens?: number
            latencyMs?: number
            cached?: boolean
            error?: unknown
            timestamp?: Date
          }

          const modelId = data.modelId ?? 'unknown'
          const providerId = data.providerId ?? 'unknown'
          const date = data.timestamp
            ? new Date(data.timestamp).toISOString().split('T')[0]
            : new Date().toISOString().split('T')[0]
          const key = `${modelId}:${providerId}:${date}`

          const existing = dailyUsage.get(key) ?? {
            modelId,
            providerId,
            date: date ?? '',
            requestCount: 0,
            totalTokens: 0,
            totalLatencyMs: 0,
            cacheHits: 0,
            cacheMisses: 0,
            errorCount: 0,
          }

          dailyUsage.set(key, {
            ...existing,
            requestCount: existing.requestCount + 1,
            totalTokens: existing.totalTokens + (data.totalTokens ?? 0),
            totalLatencyMs: existing.totalLatencyMs + (data.latencyMs ?? 0),
            cacheHits: existing.cacheHits + (data.cached ? 1 : 0),
            cacheMisses: existing.cacheMisses + (data.cached ? 0 : 1),
            errorCount: existing.errorCount + (data.error ? 1 : 0),
          })
        }
      }
    },
  }
}

/**
 * Create a mock MV handler for generated content
 */
function createGeneratedContentMVHandler(): MVHandler & {
  capturedContent: Array<{
    id: string
    modelId: string
    contentType: string
    content: unknown
    timestamp: Date
  }>
} {
  const capturedContent: Array<{
    id: string
    modelId: string
    contentType: string
    content: unknown
    timestamp: Date
  }> = []

  return {
    capturedContent,
    name: 'GeneratedContentMV',
    sourceNamespaces: ['generated_content'],
    async process(events: Event[]): Promise<void> {
      for (const event of events) {
        if (event.op === 'CREATE' && event.after) {
          const data = event.after as {
            $id?: string
            modelId?: string
            contentType?: string
            content?: unknown
            timestamp?: Date
          }

          capturedContent.push({
            id: data.$id ?? 'unknown',
            modelId: data.modelId ?? 'unknown',
            contentType: data.contentType ?? 'text',
            content: data.content,
            timestamp: data.timestamp ?? new Date(),
          })
        }
      }
    },
  }
}

/**
 * Create a mock MV handler for eval scores
 */
function createEvalScoresMVHandler(): MVHandler & {
  scores: Array<{
    runId: number
    suiteName: string
    scorerName: string
    score: number
    timestamp: Date
  }>
  trends: Map<string, { avgScore: number; count: number }>
} {
  const scores: Array<{
    runId: number
    suiteName: string
    scorerName: string
    score: number
    timestamp: Date
  }> = []
  const trends = new Map<string, { avgScore: number; count: number }>()

  return {
    scores,
    trends,
    name: 'EvalScoresMV',
    sourceNamespaces: ['eval_scores'],
    async process(events: Event[]): Promise<void> {
      for (const event of events) {
        if (event.op === 'CREATE' && event.after) {
          const data = event.after as {
            runId?: number
            suiteName?: string
            scorerName?: string
            score?: number
            timestamp?: Date
          }

          const scoreEntry = {
            runId: data.runId ?? 0,
            suiteName: data.suiteName ?? 'unknown',
            scorerName: data.scorerName ?? 'unknown',
            score: data.score ?? 0,
            timestamp: data.timestamp ?? new Date(),
          }

          scores.push(scoreEntry)

          // Update trends
          const trendKey = `${scoreEntry.suiteName}:${scoreEntry.scorerName}`
          const existing = trends.get(trendKey) ?? { avgScore: 0, count: 0 }
          const newCount = existing.count + 1
          const newAvg = (existing.avgScore * existing.count + scoreEntry.score) / newCount
          trends.set(trendKey, { avgScore: newAvg, count: newCount })
        }
      }
    },
  }
}

// =============================================================================
// Test Suites
// =============================================================================

describe('Local AI Observability MVs', () => {
  let engine: StreamingRefreshEngine
  let tempDir: string

  beforeEach(async () => {
    engine = createStreamingRefreshEngine({
      batchSize: 10,
      batchTimeoutMs: 100,
    })
  })

  afterEach(async () => {
    if (engine) {
      await engine.stop()
    }
    if (tempDir) {
      await cleanupTempDir(tempDir)
    }
  })

  describe('AIRequestsMV', () => {
    it('should process AI request events', async () => {
      const handler = createAIRequestsMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const event = createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 150,
        latencyMs: 500,
      })

      await engine.processEvent(event)
      await engine.flush()

      expect(handler.processedEvents.length).toBe(1)
      expect(handler.processedEvents[0]![0]).toEqual(event)
    })

    it('should aggregate usage by model', async () => {
      const handler = createAIRequestsMVHandler()
      engine.registerMV(handler)
      await engine.start()

      // Process multiple requests for different models
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        totalTokens: 100,
        latencyMs: 500,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        totalTokens: 200,
        latencyMs: 600,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-3.5-turbo',
        totalTokens: 50,
        latencyMs: 200,
      }))
      await engine.flush()

      // Check aggregated stats
      const gpt4Stats = handler.aggregatedUsage.get('gpt-4')
      expect(gpt4Stats).toBeDefined()
      expect(gpt4Stats!.requests).toBe(2)
      expect(gpt4Stats!.tokens).toBe(300)
      expect(gpt4Stats!.latency).toBe(1100)

      const gpt35Stats = handler.aggregatedUsage.get('gpt-3.5-turbo')
      expect(gpt35Stats).toBeDefined()
      expect(gpt35Stats!.requests).toBe(1)
      expect(gpt35Stats!.tokens).toBe(50)
    })

    it('should track cached vs non-cached requests', async () => {
      const usageHandler = createAIUsageMVHandler()
      engine.registerMV(usageHandler)
      await engine.start()

      const today = new Date()
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        cached: true,
        timestamp: today,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        cached: false,
        timestamp: today,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        cached: true,
        timestamp: today,
      }))
      await engine.flush()

      const dateStr = today.toISOString().split('T')[0]
      const usage = usageHandler.dailyUsage.get(`gpt-4:openai:${dateStr}`)
      expect(usage).toBeDefined()
      expect(usage!.cacheHits).toBe(2)
      expect(usage!.cacheMisses).toBe(1)
    })

    it('should track error counts', async () => {
      const usageHandler = createAIUsageMVHandler()
      engine.registerMV(usageHandler)
      await engine.start()

      const today = new Date()
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        timestamp: today,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        error: { name: 'APIError', message: 'Rate limited' },
        timestamp: today,
      }))
      await engine.flush()

      const dateStr = today.toISOString().split('T')[0]
      const usage = usageHandler.dailyUsage.get(`gpt-4:openai:${dateStr}`)
      expect(usage).toBeDefined()
      expect(usage!.errorCount).toBe(1)
      expect(usage!.requestCount).toBe(2)
    })

    it('should handle stream and generate request types', async () => {
      const handler = createAIRequestsMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createAIRequestEvent({
        requestType: 'generate',
        modelId: 'gpt-4',
      }))
      await engine.processEvent(createAIRequestEvent({
        requestType: 'stream',
        modelId: 'gpt-4',
      }))
      await engine.flush()

      expect(handler.processedEvents.length).toBeGreaterThanOrEqual(1)
      const allEvents = handler.processedEvents.flat()
      const requestTypes = allEvents.map(e =>
        (e.after as { requestType?: string })?.requestType
      )
      expect(requestTypes).toContain('generate')
      expect(requestTypes).toContain('stream')
    })
  })

  describe('AIUsageMV', () => {
    it('should aggregate usage by model, provider, and date', async () => {
      const handler = createAIUsageMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const day1 = new Date('2024-01-15')
      const day2 = new Date('2024-01-16')

      // Day 1: Multiple requests
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 100,
        timestamp: day1,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 200,
        timestamp: day1,
      }))

      // Day 2: Single request
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 150,
        timestamp: day2,
      }))
      await engine.flush()

      const usage1 = handler.dailyUsage.get('gpt-4:openai:2024-01-15')
      expect(usage1).toBeDefined()
      expect(usage1!.requestCount).toBe(2)
      expect(usage1!.totalTokens).toBe(300)

      const usage2 = handler.dailyUsage.get('gpt-4:openai:2024-01-16')
      expect(usage2).toBeDefined()
      expect(usage2!.requestCount).toBe(1)
      expect(usage2!.totalTokens).toBe(150)
    })

    it('should aggregate across multiple providers', async () => {
      const handler = createAIUsageMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const today = new Date()
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 100,
        timestamp: today,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        totalTokens: 200,
        timestamp: today,
      }))
      await engine.flush()

      const dateStr = today.toISOString().split('T')[0]
      expect(handler.dailyUsage.has(`gpt-4:openai:${dateStr}`)).toBe(true)
      expect(handler.dailyUsage.has(`claude-3-opus:anthropic:${dateStr}`)).toBe(true)
    })

    it('should calculate total latency', async () => {
      const handler = createAIUsageMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const today = new Date()
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        latencyMs: 500,
        timestamp: today,
      }))
      await engine.processEvent(createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        latencyMs: 700,
        timestamp: today,
      }))
      await engine.flush()

      const dateStr = today.toISOString().split('T')[0]
      const usage = handler.dailyUsage.get(`gpt-4:openai:${dateStr}`)
      expect(usage).toBeDefined()
      expect(usage!.totalLatencyMs).toBe(1200)
    })
  })

  describe('GeneratedContentMV', () => {
    it('should capture generated text content', async () => {
      const handler = createGeneratedContentMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createGeneratedContentEvent({
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'This is a generated response.',
      }))
      await engine.flush()

      expect(handler.capturedContent.length).toBe(1)
      expect(handler.capturedContent[0]!.contentType).toBe('text')
      expect(handler.capturedContent[0]!.content).toBe('This is a generated response.')
    })

    it('should capture structured object content', async () => {
      const handler = createGeneratedContentMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const structuredContent = {
        name: 'John Doe',
        age: 30,
        skills: ['typescript', 'python'],
      }

      await engine.processEvent(createGeneratedContentEvent({
        modelId: 'gpt-4',
        contentType: 'object',
        content: structuredContent,
        schema: 'UserProfile',
      }))
      await engine.flush()

      expect(handler.capturedContent.length).toBe(1)
      expect(handler.capturedContent[0]!.contentType).toBe('object')
      expect(handler.capturedContent[0]!.content).toEqual(structuredContent)
    })

    it('should track content by model', async () => {
      const handler = createGeneratedContentMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createGeneratedContentEvent({
        modelId: 'gpt-4',
        content: 'Content from GPT-4',
      }))
      await engine.processEvent(createGeneratedContentEvent({
        modelId: 'claude-3-opus',
        content: 'Content from Claude',
      }))
      await engine.flush()

      expect(handler.capturedContent.length).toBe(2)
      const models = handler.capturedContent.map(c => c.modelId)
      expect(models).toContain('gpt-4')
      expect(models).toContain('claude-3-opus')
    })

    it('should handle embedding content type', async () => {
      const handler = createGeneratedContentMVHandler()
      engine.registerMV(handler)
      await engine.start()

      const embedding = Array.from({ length: 10 }, () => Math.random())

      await engine.processEvent(createGeneratedContentEvent({
        modelId: 'text-embedding-ada-002',
        contentType: 'embedding',
        content: embedding,
      }))
      await engine.flush()

      expect(handler.capturedContent.length).toBe(1)
      expect(handler.capturedContent[0]!.contentType).toBe('embedding')
      expect(Array.isArray(handler.capturedContent[0]!.content)).toBe(true)
    })
  })

  describe('EvalScoresMV', () => {
    it('should capture eval scores', async () => {
      const handler = createEvalScoresMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvalScoreEvent({
        runId: 1,
        suiteName: 'qa-eval.eval.ts',
        scorerName: 'accuracy',
        score: 0.85,
      }))
      await engine.flush()

      expect(handler.scores.length).toBe(1)
      expect(handler.scores[0]!.score).toBe(0.85)
      expect(handler.scores[0]!.suiteName).toBe('qa-eval.eval.ts')
    })

    it('should track score trends by suite and scorer', async () => {
      const handler = createEvalScoresMVHandler()
      engine.registerMV(handler)
      await engine.start()

      // Multiple scores for the same suite/scorer
      await engine.processEvent(createEvalScoreEvent({
        suiteName: 'sentiment-analysis.eval.ts',
        scorerName: 'accuracy',
        score: 0.7,
      }))
      await engine.processEvent(createEvalScoreEvent({
        suiteName: 'sentiment-analysis.eval.ts',
        scorerName: 'accuracy',
        score: 0.8,
      }))
      await engine.processEvent(createEvalScoreEvent({
        suiteName: 'sentiment-analysis.eval.ts',
        scorerName: 'accuracy',
        score: 0.9,
      }))
      await engine.flush()

      const trend = handler.trends.get('sentiment-analysis.eval.ts:accuracy')
      expect(trend).toBeDefined()
      expect(trend!.count).toBe(3)
      expect(trend!.avgScore).toBeCloseTo(0.8, 2)
    })

    it('should track multiple scorers separately', async () => {
      const handler = createEvalScoresMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvalScoreEvent({
        suiteName: 'qa-eval.eval.ts',
        scorerName: 'accuracy',
        score: 0.9,
      }))
      await engine.processEvent(createEvalScoreEvent({
        suiteName: 'qa-eval.eval.ts',
        scorerName: 'relevance',
        score: 0.7,
      }))
      await engine.flush()

      expect(handler.trends.has('qa-eval.eval.ts:accuracy')).toBe(true)
      expect(handler.trends.has('qa-eval.eval.ts:relevance')).toBe(true)

      const accuracyTrend = handler.trends.get('qa-eval.eval.ts:accuracy')
      const relevanceTrend = handler.trends.get('qa-eval.eval.ts:relevance')

      expect(accuracyTrend!.avgScore).toBe(0.9)
      expect(relevanceTrend!.avgScore).toBe(0.7)
    })

    it('should track scores across multiple runs', async () => {
      const handler = createEvalScoresMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createEvalScoreEvent({
        runId: 1,
        suiteName: 'test.eval.ts',
        score: 0.7,
      }))
      await engine.processEvent(createEvalScoreEvent({
        runId: 2,
        suiteName: 'test.eval.ts',
        score: 0.8,
      }))
      await engine.processEvent(createEvalScoreEvent({
        runId: 3,
        suiteName: 'test.eval.ts',
        score: 0.9,
      }))
      await engine.flush()

      expect(handler.scores.length).toBe(3)
      const runIds = handler.scores.map(s => s.runId)
      expect(runIds).toContain(1)
      expect(runIds).toContain(2)
      expect(runIds).toContain(3)
    })
  })

  describe('Multiple MVs Integration', () => {
    it('should process events for multiple MVs simultaneously', async () => {
      const aiRequestsHandler = createAIRequestsMVHandler()
      const aiUsageHandler = createAIUsageMVHandler()

      engine.registerMV(aiRequestsHandler)
      engine.registerMV(aiUsageHandler)
      await engine.start()

      const event = createAIRequestEvent({
        modelId: 'gpt-4',
        providerId: 'openai',
        totalTokens: 150,
        latencyMs: 500,
      })

      await engine.processEvent(event)
      await engine.flush()

      // Both MVs should have processed the event
      expect(aiRequestsHandler.processedEvents.length).toBeGreaterThanOrEqual(1)

      const dateStr = new Date().toISOString().split('T')[0]
      const usage = aiUsageHandler.dailyUsage.get(`gpt-4:openai:${dateStr}`)
      expect(usage).toBeDefined()
    })

    it('should route events to correct MVs based on namespace', async () => {
      const aiRequestsHandler = createAIRequestsMVHandler()
      const generatedContentHandler = createGeneratedContentMVHandler()
      const evalScoresHandler = createEvalScoresMVHandler()

      engine.registerMV(aiRequestsHandler)
      engine.registerMV(generatedContentHandler)
      engine.registerMV(evalScoresHandler)
      await engine.start()

      // Send events to different namespaces
      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createGeneratedContentEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createEvalScoreEvent({ score: 0.9 }))
      await engine.flush()

      // Each MV should only receive its relevant events
      expect(aiRequestsHandler.processedEvents.flat().length).toBe(1)
      expect(generatedContentHandler.capturedContent.length).toBe(1)
      expect(evalScoresHandler.scores.length).toBe(1)
    })
  })

  describe('FsBackend Integration', () => {
    beforeEach(async () => {
      tempDir = await createTempDir()
    })

    it('should work with FsBackend for local storage', async () => {
      const storage = new FsBackend(tempDir)
      const db = new ParqueDB({ storage })

      // Create some AI log data
      await db.create('ai_logs', {
        $type: 'AILog',
        name: 'log-1',
        timestamp: new Date(),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        cached: false,
      })

      await db.create('ai_logs', {
        $type: 'AILog',
        name: 'log-2',
        timestamp: new Date(),
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'stream',
        latencyMs: 700,
        cached: true,
      })

      // Query the logs
      const result = await db.find('ai_logs', { modelId: 'gpt-4' })
      expect(result.items.length).toBe(2)
    })

    it('should persist AI analytics data to filesystem', async () => {
      const storage = new FsBackend(tempDir)
      const db = new ParqueDB({ storage })

      // Create multiple entries
      for (let i = 0; i < 5; i++) {
        await db.create('ai_requests', {
          $type: 'AIRequest',
          name: `request-${i}`,
          modelId: 'gpt-4',
          totalTokens: 100 + i * 10,
          latencyMs: 500 + i * 100,
        })
      }

      // Verify data is in the first instance
      const result = await db.find('ai_requests', {})
      expect(result.items.length).toBe(5)

      // Verify the storage backend has data written
      // ParqueDB writes entity data to data/{ns}/data.json
      const dataFileExists = await storage.exists('data/ai_requests/data.json')
      expect(dataFileExists).toBe(true)

      // Read and verify the data file contains our entities
      const dataFile = await storage.read('data/ai_requests/data.json')
      const dataContent = new TextDecoder().decode(dataFile)
      const entities = JSON.parse(dataContent) as unknown[]
      expect(entities.length).toBe(5)

      // Verify each entity has the expected structure
      for (const entity of entities) {
        expect((entity as { $type: string }).$type).toBe('AIRequest')
        expect((entity as { modelId: string }).modelId).toBe('gpt-4')
      }
    })

    it('should support querying by date range', async () => {
      const storage = new FsBackend(tempDir)
      const db = new ParqueDB({ storage })

      const day1 = new Date('2024-01-15T10:00:00Z')
      const day2 = new Date('2024-01-16T10:00:00Z')
      const day3 = new Date('2024-01-17T10:00:00Z')

      await db.create('ai_requests', {
        $type: 'AIRequest',
        name: 'request-1',
        modelId: 'gpt-4',
        requestTimestamp: day1,
      })
      await db.create('ai_requests', {
        $type: 'AIRequest',
        name: 'request-2',
        modelId: 'gpt-4',
        requestTimestamp: day2,
      })
      await db.create('ai_requests', {
        $type: 'AIRequest',
        name: 'request-3',
        modelId: 'gpt-4',
        requestTimestamp: day3,
      })

      // Query for a specific date range
      const result = await db.find('ai_requests', {
        requestTimestamp: {
          $gte: new Date('2024-01-16T00:00:00Z'),
          $lt: new Date('2024-01-17T00:00:00Z'),
        },
      })
      expect(result.items.length).toBe(1)
    })
  })

  describe('Error Handling', () => {
    it('should continue processing after MV handler error', async () => {
      const failingHandler: MVHandler = {
        name: 'FailingMV',
        sourceNamespaces: ['ai_requests'],
        async process(_events: Event[]): Promise<void> {
          throw new Error('Simulated failure')
        },
      }

      const workingHandler = createAIRequestsMVHandler()

      engine.registerMV(failingHandler)
      engine.registerMV(workingHandler)
      await engine.start()

      const errors: Error[] = []
      engine.onError((err) => errors.push(err))

      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.flush()

      // The working handler should still process events
      expect(workingHandler.processedEvents.length).toBeGreaterThanOrEqual(1)
      // The failing handler should have generated errors
      expect(errors.length).toBeGreaterThan(0)
    })

    it('should track failed batch statistics', async () => {
      const failingHandler: MVHandler = {
        name: 'FailingMV',
        sourceNamespaces: ['ai_requests'],
        async process(_events: Event[]): Promise<void> {
          throw new Error('Simulated failure')
        },
      }

      engine.registerMV(failingHandler)
      await engine.start()

      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.failedBatches).toBeGreaterThan(0)
    })
  })

  describe('Statistics and Monitoring', () => {
    it('should track events by namespace', async () => {
      const aiHandler = createAIRequestsMVHandler()
      const contentHandler = createGeneratedContentMVHandler()

      engine.registerMV(aiHandler)
      engine.registerMV(contentHandler)
      await engine.start()

      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createGeneratedContentEvent({ modelId: 'gpt-4' }))
      await engine.flush()

      const stats = engine.getStats()
      expect(stats.eventsByNamespace['ai_requests']).toBe(2)
      expect(stats.eventsByNamespace['generated_content']).toBe(1)
    })

    it('should track events by MV', async () => {
      const aiHandler = createAIRequestsMVHandler()
      const usageHandler = createAIUsageMVHandler()

      engine.registerMV(aiHandler)
      engine.registerMV(usageHandler)
      await engine.start()

      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.flush()

      const stats = engine.getStats()
      // Both MVs track ai_requests, so both should receive the events
      expect(stats.eventsByMV['AIRequestsMV']).toBe(2)
      expect(stats.eventsByMV['AIUsageMV']).toBe(2)
    })

    it('should reset statistics', async () => {
      const handler = createAIRequestsMVHandler()
      engine.registerMV(handler)
      await engine.start()

      await engine.processEvent(createAIRequestEvent({ modelId: 'gpt-4' }))
      await engine.flush()

      let stats = engine.getStats()
      expect(stats.eventsReceived).toBe(1)

      engine.resetStats()

      stats = engine.getStats()
      expect(stats.eventsReceived).toBe(0)
    })
  })
})

describe('MemoryBackend Integration', () => {
  let db: ParqueDB

  beforeEach(() => {
    const storage = new MemoryBackend()
    db = new ParqueDB({ storage })
  })

  it('should store and query AI request logs', async () => {
    await db.create('ai_requests', {
      $type: 'AIRequest',
      name: 'req-1',
      modelId: 'gpt-4',
      providerId: 'openai',
      totalTokens: 150,
      latencyMs: 500,
      cached: false,
    })

    await db.create('ai_requests', {
      $type: 'AIRequest',
      name: 'req-2',
      modelId: 'gpt-3.5-turbo',
      providerId: 'openai',
      totalTokens: 100,
      latencyMs: 300,
      cached: true,
    })

    const allRequests = await db.find('ai_requests', {})
    expect(allRequests.items.length).toBe(2)

    const gpt4Requests = await db.find('ai_requests', { modelId: 'gpt-4' })
    expect(gpt4Requests.items.length).toBe(1)

    const cachedRequests = await db.find('ai_requests', { cached: true })
    expect(cachedRequests.items.length).toBe(1)
  })

  it('should store and query generated content', async () => {
    await db.create('generated_content', {
      $type: 'GeneratedContent',
      name: 'content-1',
      modelId: 'gpt-4',
      contentType: 'text',
      content: 'Hello, world!',
    })

    await db.create('generated_content', {
      $type: 'GeneratedContent',
      name: 'content-2',
      modelId: 'gpt-4',
      contentType: 'object',
      content: { key: 'value' },
    })

    const textContent = await db.find('generated_content', { contentType: 'text' })
    expect(textContent.items.length).toBe(1)

    const objectContent = await db.find('generated_content', { contentType: 'object' })
    expect(objectContent.items.length).toBe(1)
  })

  it('should store and query eval scores', async () => {
    await db.create('eval_scores', {
      $type: 'EvalScore',
      name: 'score-1',
      runId: 1,
      suiteName: 'test.eval.ts',
      scorerName: 'accuracy',
      score: 0.85,
    })

    await db.create('eval_scores', {
      $type: 'EvalScore',
      name: 'score-2',
      runId: 1,
      suiteName: 'test.eval.ts',
      scorerName: 'relevance',
      score: 0.9,
    })

    const accuracyScores = await db.find('eval_scores', { scorerName: 'accuracy' })
    expect(accuracyScores.items.length).toBe(1)
    expect((accuracyScores.items[0] as { score: number }).score).toBe(0.85)
  })

  it('should support aggregation queries', async () => {
    // Create multiple AI requests
    for (let i = 0; i < 10; i++) {
      await db.create('ai_requests', {
        $type: 'AIRequest',
        name: `req-${i}`,
        modelId: i < 5 ? 'gpt-4' : 'gpt-3.5-turbo',
        totalTokens: 100 + i * 10,
        latencyMs: 500 + i * 50,
      })
    }

    // Query counts by model
    const gpt4Requests = await db.find('ai_requests', { modelId: 'gpt-4' })
    const gpt35Requests = await db.find('ai_requests', { modelId: 'gpt-3.5-turbo' })

    expect(gpt4Requests.items.length).toBe(5)
    expect(gpt35Requests.items.length).toBe(5)

    // Calculate total tokens manually (since we don't have built-in aggregation)
    const totalGpt4Tokens = gpt4Requests.items.reduce(
      (sum, item) => sum + ((item as { totalTokens?: number }).totalTokens ?? 0),
      0
    )
    expect(totalGpt4Tokens).toBe(100 + 110 + 120 + 130 + 140)
  })
})

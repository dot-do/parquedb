/**
 * AIRequests Materialized View
 *
 * A streaming materialized view for tracking and analyzing AI/LLM requests.
 * Provides real-time analytics for:
 * - Request latency tracking (p50, p95, p99)
 * - Token usage aggregation (prompt, completion, total)
 * - Cost tracking and estimation
 * - Model usage distribution
 * - Cache hit/miss ratios
 * - Error rate monitoring
 *
 * @example
 * ```typescript
 * import { createAIRequestsMV, recordAIRequest } from 'parquedb/streaming'
 *
 * // Create the MV
 * const mv = createAIRequestsMV(db, {
 *   refreshMode: 'streaming',
 *   maxStalenessMs: 1000,
 * })
 *
 * // Record requests (usually done via AI SDK middleware)
 * await recordAIRequest(db, {
 *   modelId: 'gpt-4',
 *   providerId: 'openai',
 *   requestType: 'generate',
 *   latencyMs: 850,
 *   promptTokens: 150,
 *   completionTokens: 200,
 *   cached: false,
 * })
 *
 * // Query aggregated metrics
 * const metrics = await getAIMetrics(db, {
 *   since: new Date(Date.now() - 3600000), // Last hour
 *   groupBy: 'model',
 * })
 * ```
 *
 * @packageDocumentation
 */

import type { ParqueDB } from '../ParqueDB'
import type { ViewDefinition, ViewOptions } from '../materialized-views/types'
import { viewName } from '../materialized-views/types'
import { asCreatedRecord, asTypedResults } from '../types/cast'
import { logger } from '../utils/logger'

// =============================================================================
// Constants
// =============================================================================

/** Default collection name for storing raw AI requests */
export const DEFAULT_AI_REQUESTS_COLLECTION = 'ai_requests'

/** Default collection name for aggregated metrics */
export const DEFAULT_AI_METRICS_COLLECTION = 'ai_request_metrics'

/** Default flush interval in milliseconds */
export const DEFAULT_AI_FLUSH_INTERVAL_MS = 5000

/** Default buffer size before flush */
export const DEFAULT_AI_BUFFER_SIZE = 100

/** Default max retry attempts for failed flushes */
export const DEFAULT_AI_MAX_FLUSH_RETRIES = 3

// =============================================================================
// Cost Configuration
// =============================================================================

/**
 * Token pricing configuration for a model
 * Prices are in USD per 1,000 tokens
 */
export interface ModelPricing {
  /** Cost per 1,000 input/prompt tokens */
  promptPer1k: number
  /** Cost per 1,000 output/completion tokens */
  completionPer1k: number
}

/**
 * Default pricing for common models (USD per 1,000 tokens)
 * These are approximate prices and should be updated based on actual provider pricing
 */
export const DEFAULT_MODEL_PRICING: Record<string, ModelPricing> = {
  // OpenAI Models
  'gpt-4': { promptPer1k: 0.03, completionPer1k: 0.06 },
  'gpt-4-turbo': { promptPer1k: 0.01, completionPer1k: 0.03 },
  'gpt-4-turbo-preview': { promptPer1k: 0.01, completionPer1k: 0.03 },
  'gpt-4o': { promptPer1k: 0.005, completionPer1k: 0.015 },
  'gpt-4o-mini': { promptPer1k: 0.00015, completionPer1k: 0.0006 },
  'gpt-3.5-turbo': { promptPer1k: 0.0005, completionPer1k: 0.0015 },
  'gpt-3.5-turbo-16k': { promptPer1k: 0.003, completionPer1k: 0.004 },

  // Anthropic Models
  'claude-3-opus': { promptPer1k: 0.015, completionPer1k: 0.075 },
  'claude-3-sonnet': { promptPer1k: 0.003, completionPer1k: 0.015 },
  'claude-3-haiku': { promptPer1k: 0.00025, completionPer1k: 0.00125 },
  'claude-3.5-sonnet': { promptPer1k: 0.003, completionPer1k: 0.015 },
  'claude-3.5-haiku': { promptPer1k: 0.0008, completionPer1k: 0.004 },
  'claude-opus-4-5': { promptPer1k: 0.015, completionPer1k: 0.075 },
  'claude-sonnet-4': { promptPer1k: 0.003, completionPer1k: 0.015 },

  // Google Models
  'gemini-pro': { promptPer1k: 0.00025, completionPer1k: 0.0005 },
  'gemini-pro-vision': { promptPer1k: 0.00025, completionPer1k: 0.0005 },
  'gemini-1.5-pro': { promptPer1k: 0.00125, completionPer1k: 0.005 },
  'gemini-1.5-flash': { promptPer1k: 0.000075, completionPer1k: 0.0003 },

  // Mistral Models
  'mistral-small': { promptPer1k: 0.002, completionPer1k: 0.006 },
  'mistral-medium': { promptPer1k: 0.0027, completionPer1k: 0.0081 },
  'mistral-large': { promptPer1k: 0.008, completionPer1k: 0.024 },

  // Default fallback for unknown models
  'default': { promptPer1k: 0.001, completionPer1k: 0.002 },
}

// =============================================================================
// Request Schema Types
// =============================================================================

/**
 * AI request type
 */
export type AIRequestType = 'generate' | 'stream' | 'embed' | 'completion' | 'chat'

/**
 * AI provider identifier
 */
export type AIProvider =
  | 'openai'
  | 'anthropic'
  | 'google'
  | 'mistral'
  | 'cohere'
  | 'replicate'
  | 'huggingface'
  | 'ollama'
  | 'azure'
  | 'aws-bedrock'
  | 'workers-ai'
  | 'custom'

/**
 * Raw AI request record
 */
export interface AIRequest {
  /** Unique request ID */
  requestId: string

  /** Request timestamp */
  timestamp: Date

  /** Model identifier (e.g., 'gpt-4', 'claude-3-sonnet') */
  modelId: string

  /** Provider identifier */
  providerId: AIProvider | string

  /** Request type */
  requestType: AIRequestType

  /** Total latency in milliseconds */
  latencyMs: number

  /** Number of prompt/input tokens */
  promptTokens: number

  /** Number of completion/output tokens */
  completionTokens: number

  /** Total tokens (prompt + completion) */
  totalTokens: number

  /** Estimated cost in USD */
  costUSD: number

  /** Whether the response was served from cache */
  cached: boolean

  /** Whether the request succeeded */
  success: boolean

  /** Finish reason (e.g., 'stop', 'length', 'tool-calls', 'error') */
  finishReason?: string | undefined

  /** Error message (if request failed) */
  error?: string | undefined

  /** Error code (if applicable) */
  errorCode?: string | undefined

  /** Temperature setting used */
  temperature?: number | undefined

  /** Max tokens setting used */
  maxTokens?: number | undefined

  /** Whether tools were used */
  toolsUsed?: boolean | undefined

  /** Number of tool calls made */
  toolCallCount?: number | undefined

  /** User or session identifier */
  userId?: string | undefined

  /** Application or service identifier */
  appId?: string | undefined

  /** Environment (e.g., 'production', 'development', 'staging') */
  environment?: string | undefined

  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Input for recording an AI request
 */
export interface RecordAIRequestInput {
  /** Model identifier */
  modelId: string

  /** Provider identifier */
  providerId: AIProvider | string

  /** Request type */
  requestType: AIRequestType

  /** Total latency in milliseconds */
  latencyMs: number

  /** Number of prompt/input tokens (optional, will be 0 if not provided) */
  promptTokens?: number | undefined

  /** Number of completion/output tokens (optional, will be 0 if not provided) */
  completionTokens?: number | undefined

  /** Whether the response was served from cache */
  cached?: boolean | undefined

  /** Whether the request succeeded (defaults to true) */
  success?: boolean | undefined

  /** Finish reason */
  finishReason?: string | undefined

  /** Error message (if request failed) */
  error?: string | undefined

  /** Error code */
  errorCode?: string | undefined

  /** Temperature setting used */
  temperature?: number | undefined

  /** Max tokens setting used */
  maxTokens?: number | undefined

  /** Whether tools were used */
  toolsUsed?: boolean | undefined

  /** Number of tool calls made */
  toolCallCount?: number | undefined

  /** User or session identifier */
  userId?: string | undefined

  /** Application or service identifier */
  appId?: string | undefined

  /** Environment */
  environment?: string | undefined

  /** Custom request ID (auto-generated if not provided) */
  requestId?: string | undefined

  /** Custom timestamp (defaults to now) */
  timestamp?: Date | undefined

  /** Custom cost override (otherwise calculated from tokens) */
  costUSD?: number | undefined

  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// Aggregated Metrics Types
// =============================================================================

/**
 * Time bucket for aggregation
 */
export type AITimeBucket = 'minute' | 'hour' | 'day' | 'week' | 'month'

/**
 * Aggregated AI request metrics
 */
export interface AIMetrics {
  /** Aggregation bucket start time */
  bucketStart: Date

  /** Aggregation bucket end time */
  bucketEnd: Date

  /** Time bucket size */
  timeBucket: AITimeBucket

  /** Grouping key (model, provider, etc.) */
  groupBy?: string | undefined

  /** Group value */
  groupValue?: string | undefined

  /** Total request count */
  totalRequests: number

  /** Successful requests */
  successCount: number

  /** Failed requests */
  errorCount: number

  /** Error rate (0-1) */
  errorRate: number

  /** Cache hit count */
  cacheHits: number

  /** Cache miss count */
  cacheMisses: number

  /** Cache hit ratio (0-1) */
  cacheHitRatio: number

  /** Token statistics */
  tokens: {
    /** Total prompt tokens */
    totalPromptTokens: number
    /** Total completion tokens */
    totalCompletionTokens: number
    /** Total tokens (all types) */
    totalTokens: number
    /** Average prompt tokens per request */
    avgPromptTokens: number
    /** Average completion tokens per request */
    avgCompletionTokens: number
    /** Average total tokens per request */
    avgTotalTokens: number
  }

  /** Cost statistics */
  cost: {
    /** Total cost in USD */
    totalCostUSD: number
    /** Average cost per request in USD */
    avgCostUSD: number
    /** Cost from cached responses (savings) */
    cacheSavingsUSD: number
  }

  /** Latency statistics */
  latency: {
    /** Minimum latency */
    min: number
    /** Maximum latency */
    max: number
    /** Average latency */
    avg: number
    /** Median latency (p50) */
    p50: number
    /** 95th percentile latency */
    p95: number
    /** 99th percentile latency */
    p99: number
  }

  /** Request type breakdown */
  requestTypes: Record<AIRequestType, number>

  /** Model breakdown */
  models: Record<string, number>

  /** Provider breakdown */
  providers: Record<string, number>

  /** Finish reason breakdown */
  finishReasons: Record<string, number>
}

/**
 * Options for querying AI request metrics
 */
export interface GetAIMetricsOptions {
  /** Collection name for raw requests */
  collection?: string | undefined

  /** Start time for query (inclusive) */
  since?: Date | undefined

  /** End time for query (exclusive) */
  until?: Date | undefined

  /** Time bucket for aggregation */
  timeBucket?: AITimeBucket | undefined

  /** Field to group by */
  groupBy?: 'model' | 'provider' | 'requestType' | 'userId' | 'appId' | 'environment' | undefined

  /** Filter by specific model */
  modelId?: string | undefined

  /** Filter by provider */
  providerId?: AIProvider | string | undefined

  /** Filter by request type */
  requestType?: AIRequestType | undefined

  /** Filter by user ID */
  userId?: string | undefined

  /** Filter by app ID */
  appId?: string | undefined

  /** Filter by environment */
  environment?: string | undefined

  /** Include only cached/uncached requests */
  cachedOnly?: boolean | undefined

  /** Include only successful/failed requests */
  successOnly?: boolean | undefined

  /** Limit number of results */
  limit?: number | undefined

  /** Custom model pricing (overrides defaults) */
  pricing?: Record<string, ModelPricing> | undefined
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a unique AI request ID
 */
export function generateAIRequestId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `ai_${timestamp}_${random}`
}

/**
 * Calculate cost from token counts and model pricing
 */
export function calculateCost(
  modelId: string,
  promptTokens: number,
  completionTokens: number,
  customPricing?: Record<string, ModelPricing>
): number {
  // Try custom pricing first, then default, then fallback
  const pricing = customPricing?.[modelId] ??
    DEFAULT_MODEL_PRICING[modelId] ??
    findMatchingPricing(modelId, customPricing) ??
    DEFAULT_MODEL_PRICING['default']!

  const promptCost = (promptTokens / 1000) * pricing.promptPer1k
  const completionCost = (completionTokens / 1000) * pricing.completionPer1k

  return promptCost + completionCost
}

/**
 * Find pricing for a model by partial match
 */
function findMatchingPricing(
  modelId: string,
  customPricing?: Record<string, ModelPricing>
): ModelPricing | undefined {
  const allPricing = { ...DEFAULT_MODEL_PRICING, ...customPricing }
  const modelLower = modelId.toLowerCase()

  for (const [key, pricing] of Object.entries(allPricing)) {
    if (key === 'default') continue
    if (modelLower.includes(key.toLowerCase()) || key.toLowerCase().includes(modelLower)) {
      return pricing
    }
  }

  return undefined
}

/**
 * Calculate percentile from sorted array of numbers
 */
export function percentile(sortedArr: number[], p: number): number {
  if (sortedArr.length === 0) return 0
  if (sortedArr.length === 1) return sortedArr[0]!

  const index = (p / 100) * (sortedArr.length - 1)
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) return sortedArr[lower]!

  const lowerValue = sortedArr[lower]!
  const upperValue = sortedArr[upper]!
  return lowerValue + (upperValue - lowerValue) * (index - lower)
}

/**
 * Get bucket start time based on time bucket
 */
export function getAIBucketStart(date: Date, bucket: AITimeBucket): Date {
  const d = new Date(date)

  switch (bucket) {
    case 'minute':
      d.setSeconds(0, 0)
      break
    case 'hour':
      d.setMinutes(0, 0, 0)
      break
    case 'day':
      d.setHours(0, 0, 0, 0)
      break
    case 'week': {
      const day = d.getDay()
      d.setDate(d.getDate() - day)
      d.setHours(0, 0, 0, 0)
      break
    }
    case 'month':
      d.setDate(1)
      d.setHours(0, 0, 0, 0)
      break
  }

  return d
}

/**
 * Get bucket end time based on time bucket
 */
export function getAIBucketEnd(date: Date, bucket: AITimeBucket): Date {
  const d = getAIBucketStart(date, bucket)

  switch (bucket) {
    case 'minute':
      d.setMinutes(d.getMinutes() + 1)
      break
    case 'hour':
      d.setHours(d.getHours() + 1)
      break
    case 'day':
      d.setDate(d.getDate() + 1)
      break
    case 'week':
      d.setDate(d.getDate() + 7)
      break
    case 'month':
      d.setMonth(d.getMonth() + 1)
      break
  }

  return d
}

// =============================================================================
// Request Recording
// =============================================================================

/**
 * Record a single AI request
 *
 * @param db - ParqueDB instance
 * @param input - Request data to record
 * @param options - Optional configuration
 * @returns Created request record
 *
 * @example
 * ```typescript
 * await recordAIRequest(db, {
 *   modelId: 'gpt-4',
 *   providerId: 'openai',
 *   requestType: 'generate',
 *   latencyMs: 850,
 *   promptTokens: 150,
 *   completionTokens: 200,
 *   cached: false,
 * })
 * ```
 */
export async function recordAIRequest(
  db: ParqueDB,
  input: RecordAIRequestInput,
  options?: {
    collection?: string | undefined
    pricing?: Record<string, ModelPricing> | undefined
  }
): Promise<AIRequest> {
  const collection = options?.collection ?? DEFAULT_AI_REQUESTS_COLLECTION
  const promptTokens = input.promptTokens ?? 0
  const completionTokens = input.completionTokens ?? 0
  const totalTokens = promptTokens + completionTokens

  const costUSD = input.costUSD ?? calculateCost(
    input.modelId,
    promptTokens,
    completionTokens,
    options?.pricing
  )

  const request: AIRequest = {
    requestId: input.requestId ?? generateAIRequestId(),
    timestamp: input.timestamp ?? new Date(),
    modelId: input.modelId,
    providerId: input.providerId,
    requestType: input.requestType,
    latencyMs: input.latencyMs,
    promptTokens,
    completionTokens,
    totalTokens,
    costUSD,
    cached: input.cached ?? false,
    success: input.success ?? true,
    finishReason: input.finishReason,
    error: input.error,
    errorCode: input.errorCode,
    temperature: input.temperature,
    maxTokens: input.maxTokens,
    toolsUsed: input.toolsUsed,
    toolCallCount: input.toolCallCount,
    userId: input.userId,
    appId: input.appId,
    environment: input.environment,
    metadata: input.metadata,
  }

  const created = await db.collection(collection).create({
    $type: 'AIRequest',
    name: request.requestId,
    ...request,
  })

  return asCreatedRecord<AIRequest>(created)
}

/**
 * Record multiple AI requests in a batch
 *
 * @param db - ParqueDB instance
 * @param inputs - Array of request data to record
 * @param options - Optional configuration
 * @returns Array of created request records
 */
export async function recordAIRequests(
  db: ParqueDB,
  inputs: RecordAIRequestInput[],
  options?: {
    collection?: string | undefined
    pricing?: Record<string, ModelPricing> | undefined
  }
): Promise<AIRequest[]> {
  const collection = options?.collection ?? DEFAULT_AI_REQUESTS_COLLECTION

  const requests: AIRequest[] = inputs.map(input => {
    const promptTokens = input.promptTokens ?? 0
    const completionTokens = input.completionTokens ?? 0
    const totalTokens = promptTokens + completionTokens

    const costUSD = input.costUSD ?? calculateCost(
      input.modelId,
      promptTokens,
      completionTokens,
      options?.pricing
    )

    return {
      requestId: input.requestId ?? generateAIRequestId(),
      timestamp: input.timestamp ?? new Date(),
      modelId: input.modelId,
      providerId: input.providerId,
      requestType: input.requestType,
      latencyMs: input.latencyMs,
      promptTokens,
      completionTokens,
      totalTokens,
      costUSD,
      cached: input.cached ?? false,
      success: input.success ?? true,
      finishReason: input.finishReason,
      error: input.error,
      errorCode: input.errorCode,
      temperature: input.temperature,
      maxTokens: input.maxTokens,
      toolsUsed: input.toolsUsed,
      toolCallCount: input.toolCallCount,
      userId: input.userId,
      appId: input.appId,
      environment: input.environment,
      metadata: input.metadata,
    }
  })

  const created = await db.collection(collection).createMany(
    requests.map(r => ({
      $type: 'AIRequest',
      name: r.requestId,
      ...r,
    }))
  )

  return asTypedResults<AIRequest>(created)
}

// =============================================================================
// Metrics Aggregation
// =============================================================================

/**
 * Calculate latency statistics from an array of requests
 */
function calculateLatencyStats(requests: AIRequest[]): AIMetrics['latency'] {
  if (requests.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 }
  }

  const latencies = requests.map(r => r.latencyMs).sort((a, b) => a - b)
  const sum = latencies.reduce((a, b) => a + b, 0)

  return {
    min: latencies[0]!,
    max: latencies[latencies.length - 1]!,
    avg: sum / latencies.length,
    p50: percentile(latencies, 50),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
  }
}

/**
 * Calculate token statistics from an array of requests
 */
function calculateTokenStats(requests: AIRequest[]): AIMetrics['tokens'] {
  if (requests.length === 0) {
    return {
      totalPromptTokens: 0,
      totalCompletionTokens: 0,
      totalTokens: 0,
      avgPromptTokens: 0,
      avgCompletionTokens: 0,
      avgTotalTokens: 0,
    }
  }

  const totalPromptTokens = requests.reduce((sum, r) => sum + r.promptTokens, 0)
  const totalCompletionTokens = requests.reduce((sum, r) => sum + r.completionTokens, 0)
  const totalTokens = totalPromptTokens + totalCompletionTokens

  return {
    totalPromptTokens,
    totalCompletionTokens,
    totalTokens,
    avgPromptTokens: totalPromptTokens / requests.length,
    avgCompletionTokens: totalCompletionTokens / requests.length,
    avgTotalTokens: totalTokens / requests.length,
  }
}

/**
 * Calculate cost statistics from an array of requests
 */
function calculateCostStats(requests: AIRequest[]): AIMetrics['cost'] {
  if (requests.length === 0) {
    return {
      totalCostUSD: 0,
      avgCostUSD: 0,
      cacheSavingsUSD: 0,
    }
  }

  const nonCachedRequests = requests.filter(r => !r.cached)
  const cachedRequests = requests.filter(r => r.cached)

  const totalCostUSD = nonCachedRequests.reduce((sum, r) => sum + r.costUSD, 0)

  // Calculate savings from cache (estimated cost of cached requests if they weren't cached)
  const cacheSavingsUSD = cachedRequests.reduce((sum, r) => sum + r.costUSD, 0)

  return {
    totalCostUSD,
    avgCostUSD: requests.length > 0 ? totalCostUSD / requests.length : 0,
    cacheSavingsUSD,
  }
}

/**
 * Aggregate requests into metrics
 */
function aggregateAIRequests(
  requests: AIRequest[],
  timeBucket: AITimeBucket,
  groupBy?: string,
  groupValue?: string
): AIMetrics {
  if (requests.length === 0) {
    const now = new Date()
    return {
      bucketStart: getAIBucketStart(now, timeBucket),
      bucketEnd: getAIBucketEnd(now, timeBucket),
      timeBucket,
      groupBy,
      groupValue,
      totalRequests: 0,
      successCount: 0,
      errorCount: 0,
      errorRate: 0,
      cacheHits: 0,
      cacheMisses: 0,
      cacheHitRatio: 0,
      tokens: calculateTokenStats([]),
      cost: calculateCostStats([]),
      latency: calculateLatencyStats([]),
      requestTypes: {} as Record<AIRequestType, number>,
      models: {},
      providers: {},
      finishReasons: {},
    }
  }

  // Get time bucket from first request
  const firstRequest = requests[0]!
  const bucketStart = getAIBucketStart(firstRequest.timestamp, timeBucket)
  const bucketEnd = getAIBucketEnd(firstRequest.timestamp, timeBucket)

  // Count success/error
  const successCount = requests.filter(r => r.success).length
  const errorCount = requests.filter(r => !r.success).length

  // Count cache hits/misses
  const cacheHits = requests.filter(r => r.cached).length
  const cacheMisses = requests.filter(r => !r.cached).length

  // Count by request type
  const requestTypes: Record<AIRequestType, number> = {} as Record<AIRequestType, number>
  for (const r of requests) {
    requestTypes[r.requestType] = (requestTypes[r.requestType] || 0) + 1
  }

  // Count by model
  const models: Record<string, number> = {}
  for (const r of requests) {
    models[r.modelId] = (models[r.modelId] || 0) + 1
  }

  // Count by provider
  const providers: Record<string, number> = {}
  for (const r of requests) {
    providers[r.providerId] = (providers[r.providerId] || 0) + 1
  }

  // Count by finish reason
  const finishReasons: Record<string, number> = {}
  for (const r of requests) {
    if (r.finishReason) {
      finishReasons[r.finishReason] = (finishReasons[r.finishReason] || 0) + 1
    }
  }

  return {
    bucketStart,
    bucketEnd,
    timeBucket,
    groupBy,
    groupValue,
    totalRequests: requests.length,
    successCount,
    errorCount,
    errorRate: requests.length > 0 ? errorCount / requests.length : 0,
    cacheHits,
    cacheMisses,
    cacheHitRatio: requests.length > 0 ? cacheHits / requests.length : 0,
    tokens: calculateTokenStats(requests),
    cost: calculateCostStats(requests),
    latency: calculateLatencyStats(requests),
    requestTypes,
    models,
    providers,
    finishReasons,
  }
}

/**
 * Get aggregated AI request metrics
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Aggregated request metrics
 *
 * @example
 * ```typescript
 * // Get hourly metrics for the last 24 hours
 * const metrics = await getAIMetrics(db, {
 *   since: new Date(Date.now() - 24 * 60 * 60 * 1000),
 *   timeBucket: 'hour',
 * })
 *
 * // Get metrics grouped by model
 * const modelMetrics = await getAIMetrics(db, {
 *   since: new Date(Date.now() - 3600000),
 *   groupBy: 'model',
 * })
 *
 * // Get metrics for a specific provider
 * const openaiMetrics = await getAIMetrics(db, {
 *   providerId: 'openai',
 *   timeBucket: 'hour',
 * })
 * ```
 */
export async function getAIMetrics(
  db: ParqueDB,
  options?: GetAIMetricsOptions
): Promise<AIMetrics[]> {
  const collection = options?.collection ?? DEFAULT_AI_REQUESTS_COLLECTION
  const timeBucket = options?.timeBucket ?? 'hour'

  // Build filter
  const filter: Record<string, unknown> = {}

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  if (options?.modelId) {
    filter.modelId = options.modelId
  }

  if (options?.providerId) {
    filter.providerId = options.providerId
  }

  if (options?.requestType) {
    filter.requestType = options.requestType
  }

  if (options?.userId) {
    filter.userId = options.userId
  }

  if (options?.appId) {
    filter.appId = options.appId
  }

  if (options?.environment) {
    filter.environment = options.environment
  }

  if (options?.cachedOnly !== undefined) {
    filter.cached = options.cachedOnly
  }

  if (options?.successOnly !== undefined) {
    filter.success = options.successOnly
  }

  // Fetch requests
  const requests = asTypedResults<AIRequest>(await db.collection(collection).find(filter, {
    limit: options?.limit ?? 10000,
    sort: { timestamp: 1 },
  }))

  // Group by time bucket
  const bucketMap = new Map<string, AIRequest[]>()
  const BUCKET_DELIMITER = '||'

  // Determine groupBy field name
  const groupByField = options?.groupBy === 'model' ? 'modelId' :
    options?.groupBy === 'provider' ? 'providerId' :
    options?.groupBy

  for (const request of requests) {
    const bucketKey = getAIBucketStart(request.timestamp, timeBucket).toISOString()

    // If grouping, add group value to key
    let fullKey = bucketKey
    if (groupByField) {
      const groupValue = String((request as Record<string, unknown>)[groupByField] ?? 'unknown')
      fullKey = `${bucketKey}${BUCKET_DELIMITER}${groupValue}`
    }

    if (!bucketMap.has(fullKey)) {
      bucketMap.set(fullKey, [])
    }
    bucketMap.get(fullKey)!.push(request)
  }

  // Aggregate each bucket
  const metrics: AIMetrics[] = []

  for (const [key, bucketRequests] of bucketMap) {
    let groupValue: string | undefined
    if (groupByField) {
      const delimiterIndex = key.indexOf(BUCKET_DELIMITER)
      if (delimiterIndex !== -1) {
        groupValue = key.slice(delimiterIndex + BUCKET_DELIMITER.length)
      }
    }

    metrics.push(aggregateAIRequests(
      bucketRequests,
      timeBucket,
      options?.groupBy,
      groupValue
    ))
  }

  // Sort by bucket start time
  metrics.sort((a, b) => a.bucketStart.getTime() - b.bucketStart.getTime())

  return metrics
}

/**
 * Get real-time metrics for the current time bucket
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Current metrics
 */
export async function getCurrentAIMetrics(
  db: ParqueDB,
  options?: {
    collection?: string | undefined
    timeBucket?: AITimeBucket | undefined
    groupBy?: GetAIMetricsOptions['groupBy'] | undefined
  }
): Promise<AIMetrics[]> {
  const timeBucket = options?.timeBucket ?? 'minute'
  const now = new Date()

  return getAIMetrics(db, {
    collection: options?.collection,
    since: getAIBucketStart(now, timeBucket),
    until: getAIBucketEnd(now, timeBucket),
    timeBucket,
    groupBy: options?.groupBy,
  })
}

/**
 * Get cost summary for AI requests
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Cost summary by model and provider
 */
export async function getAICostSummary(
  db: ParqueDB,
  options?: {
    collection?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
  }
): Promise<{
  totalCostUSD: number
  cacheSavingsUSD: number
  byModel: Record<string, { cost: number; requests: number; tokens: number }>
  byProvider: Record<string, { cost: number; requests: number; tokens: number }>
  topExpensive: Array<{
    requestId: string
    modelId: string
    costUSD: number
    totalTokens: number
    timestamp: Date
  }>
}> {
  const collection = options?.collection ?? DEFAULT_AI_REQUESTS_COLLECTION

  const filter: Record<string, unknown> = {}

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  const requests = asTypedResults<AIRequest>(await db.collection(collection).find(filter, {
    limit: options?.limit ?? 10000,
    sort: { timestamp: -1 },
  }))

  // Calculate totals
  const nonCachedRequests = requests.filter(r => !r.cached)
  const cachedRequests = requests.filter(r => r.cached)

  const totalCostUSD = nonCachedRequests.reduce((sum, r) => sum + r.costUSD, 0)
  const cacheSavingsUSD = cachedRequests.reduce((sum, r) => sum + r.costUSD, 0)

  // Aggregate by model
  const byModel: Record<string, { cost: number; requests: number; tokens: number }> = {}
  for (const r of requests) {
    if (!byModel[r.modelId]) {
      byModel[r.modelId] = { cost: 0, requests: 0, tokens: 0 }
    }
    byModel[r.modelId]!.cost += r.cached ? 0 : r.costUSD
    byModel[r.modelId]!.requests += 1
    byModel[r.modelId]!.tokens += r.totalTokens
  }

  // Aggregate by provider
  const byProvider: Record<string, { cost: number; requests: number; tokens: number }> = {}
  for (const r of requests) {
    if (!byProvider[r.providerId]) {
      byProvider[r.providerId] = { cost: 0, requests: 0, tokens: 0 }
    }
    byProvider[r.providerId]!.cost += r.cached ? 0 : r.costUSD
    byProvider[r.providerId]!.requests += 1
    byProvider[r.providerId]!.tokens += r.totalTokens
  }

  // Get top expensive requests
  const topExpensive = [...nonCachedRequests]
    .sort((a, b) => b.costUSD - a.costUSD)
    .slice(0, 10)
    .map(r => ({
      requestId: r.requestId,
      modelId: r.modelId,
      costUSD: r.costUSD,
      totalTokens: r.totalTokens,
      timestamp: r.timestamp,
    }))

  return {
    totalCostUSD,
    cacheSavingsUSD,
    byModel,
    byProvider,
    topExpensive,
  }
}

/**
 * Get error summary for AI requests
 *
 * @param db - ParqueDB instance
 * @param options - Query options
 * @returns Error summary by model and error type
 */
export async function getAIErrorSummary(
  db: ParqueDB,
  options?: {
    collection?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
  }
): Promise<{
  totalErrors: number
  errorRate: number
  byModel: Record<string, number>
  byProvider: Record<string, number>
  byErrorCode: Record<string, number>
  recentErrors: Array<{
    requestId: string
    timestamp: Date
    modelId: string
    providerId: string
    error?: string | undefined
    errorCode?: string | undefined
  }>
}> {
  const collection = options?.collection ?? DEFAULT_AI_REQUESTS_COLLECTION

  // First get total count for error rate calculation
  const allFilter: Record<string, unknown> = {}
  if (options?.since || options?.until) {
    allFilter.timestamp = {}
    if (options?.since) {
      (allFilter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (allFilter.timestamp as Record<string, unknown>).$lt = options.until
    }
  }

  const allRequests = asTypedResults<AIRequest>(await db.collection(collection).find(allFilter, {
    limit: options?.limit ?? 10000,
  }))

  // Get error requests
  const errorFilter = {
    ...allFilter,
    success: false,
  }

  const errorRequests = asTypedResults<AIRequest>(await db.collection(collection).find(errorFilter, {
    limit: options?.limit ?? 1000,
    sort: { timestamp: -1 },
  }))

  // Aggregate by model
  const byModel: Record<string, number> = {}
  for (const r of errorRequests) {
    byModel[r.modelId] = (byModel[r.modelId] || 0) + 1
  }

  // Aggregate by provider
  const byProvider: Record<string, number> = {}
  for (const r of errorRequests) {
    byProvider[r.providerId] = (byProvider[r.providerId] || 0) + 1
  }

  // Aggregate by error code
  const byErrorCode: Record<string, number> = {}
  for (const r of errorRequests) {
    const code = r.errorCode ?? 'UNKNOWN'
    byErrorCode[code] = (byErrorCode[code] || 0) + 1
  }

  // Get recent errors
  const recentErrors = errorRequests.slice(0, 10).map(r => ({
    requestId: r.requestId,
    timestamp: r.timestamp,
    modelId: r.modelId,
    providerId: r.providerId,
    error: r.error,
    errorCode: r.errorCode,
  }))

  return {
    totalErrors: errorRequests.length,
    errorRate: allRequests.length > 0 ? errorRequests.length / allRequests.length : 0,
    byModel,
    byProvider,
    byErrorCode,
    recentErrors,
  }
}

// =============================================================================
// Materialized View Definition
// =============================================================================

/**
 * Options for creating the AIRequests materialized view
 */
export interface AIRequestsMVOptions extends Omit<ViewOptions, 'populateOnCreate'> {
  /** Collection name for raw requests */
  requestsCollection?: string | undefined
  /** Collection name for aggregated metrics */
  metricsCollection?: string | undefined
  /** Custom model pricing */
  pricing?: Record<string, ModelPricing> | undefined
}

/**
 * Create the AIRequests materialized view definition
 *
 * This creates a view definition that aggregates AI requests into
 * time-bucketed metrics. Use with the materialized views system to
 * automatically maintain aggregated analytics.
 *
 * @param options - View options
 * @returns View definition
 *
 * @example
 * ```typescript
 * // Create the MV with streaming refresh
 * const viewDef = createAIRequestsMV({
 *   refreshMode: 'streaming',
 *   maxStalenessMs: 1000,
 * })
 *
 * // Register with the view manager (when implemented)
 * // await db.createView(viewDef)
 * ```
 */
export function createAIRequestsMV(
  options?: AIRequestsMVOptions
): ViewDefinition {
  const requestsCollection = options?.requestsCollection ?? DEFAULT_AI_REQUESTS_COLLECTION
  const metricsCollection = options?.metricsCollection ?? DEFAULT_AI_METRICS_COLLECTION

  return {
    name: viewName('ai_requests_metrics'),
    source: requestsCollection,
    query: {
      pipeline: [
        // Match all requests
        { $match: {} },
        // Group by hour and calculate aggregates
        {
          $group: {
            _id: {
              hour: '$timestamp',
              modelId: '$modelId',
              providerId: '$providerId',
            },
            totalRequests: { $sum: 1 },
            successCount: {
              $sum: { $cond: ['$success', 1, 0] },
            },
            errorCount: {
              $sum: { $cond: ['$success', 0, 1] },
            },
            cacheHits: {
              $sum: { $cond: ['$cached', 1, 0] },
            },
            totalPromptTokens: { $sum: '$promptTokens' },
            totalCompletionTokens: { $sum: '$completionTokens' },
            totalTokens: { $sum: '$totalTokens' },
            totalCostUSD: { $sum: '$costUSD' },
            avgLatency: { $avg: '$latencyMs' },
            minLatency: { $min: '$latencyMs' },
            maxLatency: { $max: '$latencyMs' },
          },
        },
        // Sort by time
        { $sort: { '_id.hour': -1 } },
      ],
    },
    options: {
      refreshMode: options?.refreshMode ?? 'streaming',
      refreshStrategy: options?.refreshStrategy ?? 'incremental',
      maxStalenessMs: options?.maxStalenessMs ?? 5000,
      schedule: options?.schedule,
      indexes: ['bucketStart', 'modelId', 'providerId', 'appId'],
      description: 'Aggregated AI request metrics for analytics',
      tags: ['analytics', 'ai', 'llm', 'requests', 'monitoring'],
      metadata: {
        metricsCollection,
        requestsCollection,
        pricing: options?.pricing,
      },
    },
  }
}

// =============================================================================
// Request Buffer (for high-throughput scenarios)
// =============================================================================

/**
 * Request with retry tracking
 */
interface BufferedRequest extends RecordAIRequestInput {
  _retryCount?: number | undefined
}

/**
 * Buffered AI request writer for high-throughput scenarios
 *
 * Buffers requests in memory and flushes to storage in batches
 * to reduce write overhead.
 */
export class AIRequestBuffer {
  private buffer: BufferedRequest[] = []
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private flushPromise: Promise<void> | null = null
  private droppedCount = 0

  constructor(
    private db: ParqueDB,
    private options: {
      collection?: string | undefined
      maxBufferSize?: number | undefined
      flushIntervalMs?: number | undefined
      pricing?: Record<string, ModelPricing> | undefined
      /** Maximum number of flush retries before dropping requests (default: 3) */
      maxFlushRetries?: number | undefined
      /** Callback when requests are dropped due to max retries */
      onDrop?: ((requests: RecordAIRequestInput[], error: Error) => void) | undefined
    } = {}
  ) {
    this.options.maxBufferSize = options.maxBufferSize ?? DEFAULT_AI_BUFFER_SIZE
    this.options.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_AI_FLUSH_INTERVAL_MS
    this.options.maxFlushRetries = options.maxFlushRetries ?? DEFAULT_AI_MAX_FLUSH_RETRIES
  }

  /**
   * Add a request to the buffer
   */
  async add(input: RecordAIRequestInput): Promise<void> {
    this.buffer.push(input)

    if (this.buffer.length >= (this.options.maxBufferSize ?? DEFAULT_AI_BUFFER_SIZE)) {
      await this.flush()
    }
  }

  /**
   * Start periodic flush timer
   */
  startTimer(): void {
    if (this.flushTimer) return

    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0 && !this.flushPromise) {
        this.flush().catch(err => {
          logger.error('[AIRequestBuffer] Flush failed:', err)
        })
      }
    }, this.options.flushIntervalMs ?? DEFAULT_AI_FLUSH_INTERVAL_MS)
  }

  /**
   * Stop periodic flush timer
   */
  stopTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Flush all buffered requests to storage
   */
  async flush(): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise
      return
    }

    if (this.buffer.length === 0) return

    const toFlush = this.buffer
    this.buffer = []

    const maxRetries = this.options.maxFlushRetries ?? DEFAULT_AI_MAX_FLUSH_RETRIES

    this.flushPromise = recordAIRequests(this.db, toFlush, {
      collection: this.options.collection,
      pricing: this.options.pricing,
    }).then(() => {
      this.flushPromise = null
    }).catch(err => {
      this.flushPromise = null

      // Track retries and drop requests that exceed max retries
      const toRetry: BufferedRequest[] = []
      const toDrop: BufferedRequest[] = []

      for (const request of toFlush) {
        const retryCount = (request._retryCount ?? 0) + 1
        if (retryCount < maxRetries) {
          toRetry.push({ ...request, _retryCount: retryCount })
        } else {
          toDrop.push(request)
        }
      }

      // Put retriable requests back in buffer
      if (toRetry.length > 0) {
        this.buffer.unshift(...toRetry)
      }

      // Handle dropped requests
      if (toDrop.length > 0) {
        this.droppedCount += toDrop.length
        if (this.options.onDrop) {
          this.options.onDrop(toDrop, err instanceof Error ? err : new Error(String(err)))
        } else {
          logger.error(`[AIRequestBuffer] Dropped ${toDrop.length} requests after ${maxRetries} retries:`, err)
        }
      }

      throw err
    })

    await this.flushPromise
  }

  /**
   * Close the buffer - flush remaining requests and stop timer
   */
  async close(): Promise<void> {
    this.stopTimer()
    await this.flush()
  }

  /**
   * Get current buffer size
   */
  getBufferSize(): number {
    return this.buffer.length
  }

  /**
   * Get count of dropped requests (requests that exceeded max retries)
   */
  getDroppedCount(): number {
    return this.droppedCount
  }
}

/**
 * Create an AI request buffer for high-throughput scenarios
 */
export function createAIRequestBuffer(
  db: ParqueDB,
  options?: {
    collection?: string | undefined
    maxBufferSize?: number | undefined
    flushIntervalMs?: number | undefined
    pricing?: Record<string, ModelPricing> | undefined
    autoStart?: boolean | undefined
    /** Maximum number of flush retries before dropping requests (default: 3) */
    maxFlushRetries?: number | undefined
    /** Callback when requests are dropped due to max retries */
    onDrop?: ((requests: RecordAIRequestInput[], error: Error) => void) | undefined
  }
): AIRequestBuffer {
  const buffer = new AIRequestBuffer(db, options)
  if (options?.autoStart ?? true) {
    buffer.startTimer()
  }
  return buffer
}

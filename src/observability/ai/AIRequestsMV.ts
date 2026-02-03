/**
 * AIRequestsMV - Materialized View for AI Request Tracking
 *
 * Provides detailed tracking and analysis of AI/LLM API requests including:
 * - Request/response metadata storage
 * - Filtering by model, provider, time range, status
 * - Aggregation and statistics
 * - Error tracking
 * - Performance metrics
 * - Content sampling for prompts/completions
 *
 * Unlike AIUsageMV which aggregates by time periods, AIRequestsMV provides
 * access to individual request records with filtering capabilities.
 *
 * @example
 * ```typescript
 * import { AIRequestsMV } from 'parquedb/observability'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 * const requestsMV = new AIRequestsMV(db, {
 *   collection: 'ai_requests',
 *   maxAgeMs: 7 * 24 * 60 * 60 * 1000, // 7 days
 *   contentSampling: {
 *     enabled: true,
 *     sampleRate: 0.1, // Sample 10% of requests
 *     sampleAllErrors: true, // Always sample errors
 *     maxPromptChars: 500,
 *     maxCompletionChars: 1000,
 *   },
 * })
 *
 * // Record a request
 * await requestsMV.record({
 *   modelId: 'gpt-4',
 *   providerId: 'openai',
 *   requestType: 'generate',
 *   latencyMs: 850,
 *   promptTokens: 150,
 *   completionTokens: 200,
 *   promptSample: 'What is the meaning of life?',
 *   completionSample: 'The meaning of life is...',
 * })
 *
 * // Query requests
 * const requests = await requestsMV.find({
 *   modelId: 'gpt-4',
 *   from: new Date('2026-02-01'),
 * })
 *
 * // Get statistics
 * const stats = await requestsMV.getStats({
 *   from: new Date('2026-02-01'),
 * })
 * ```
 *
 * @module observability/ai/AIRequestsMV
 */

import type { ParqueDB } from '../../ParqueDB'
import type { ModelPricing, MultiTenantConfig } from './types'
import { DEFAULT_MODEL_PRICING } from './types'
import { AI_REQUESTS_MAX_AGE_MS, MAX_BATCH_SIZE, DEFAULT_PAGE_SIZE, DJB2_INITIAL } from '../../constants'
import { asCreatedRecord, asTypedResults, asTypedResult } from '../../types/cast'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_COLLECTION = 'ai_requests'
const DEFAULT_MAX_AGE_MS = AI_REQUESTS_MAX_AGE_MS
const DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE
const DEFAULT_LIMIT = DEFAULT_PAGE_SIZE

// =============================================================================
// Types
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
  | 'groq'
  | 'cohere'
  | 'replicate'
  | 'huggingface'
  | 'ollama'
  | 'azure'
  | 'aws-bedrock'
  | 'workers-ai'
  | 'custom'
  | string

/**
 * Status of an AI request
 */
export type AIRequestStatus = 'success' | 'error' | 'cancelled' | 'timeout'

/**
 * Stored AI request record
 */
export interface AIRequestRecord {
  /** Unique request ID */
  $id: string
  /** Type identifier */
  $type: 'AIRequest'
  /** Display name */
  name: string
  /** Request ID (user-facing, may be custom) */
  requestId: string
  /** Request timestamp */
  timestamp: Date
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: AIProvider
  /** Request type */
  requestType: AIRequestType
  /** Request status */
  status: AIRequestStatus
  /** Total latency in milliseconds */
  latencyMs: number
  /** Number of prompt/input tokens */
  promptTokens: number
  /** Number of completion/output tokens */
  completionTokens: number
  /** Total tokens */
  totalTokens: number
  /** Estimated cost in USD */
  estimatedCost: number
  /** Whether response was cached */
  cached: boolean
  /** Finish reason */
  finishReason?: string | undefined
  /** Error message (if failed) */
  error?: string | undefined
  /** Error code (if failed) */
  errorCode?: string | undefined
  /** Temperature setting */
  temperature?: number | undefined
  /** Max tokens setting */
  maxTokens?: number | undefined
  /** Whether tools were used */
  toolsUsed?: boolean | undefined
  /** Number of tool calls */
  toolCallCount?: number | undefined
  /** User identifier */
  userId?: string | undefined
  /** Application identifier */
  appId?: string | undefined
  /** Environment */
  environment?: string | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
  /** When the record was created */
  createdAt: Date
  // Content sampling fields
  /** Sampled prompt content (if sampling enabled) */
  promptSample?: string | undefined
  /** Sampled completion content (if sampling enabled) */
  completionSample?: string | undefined
  /** Prompt content fingerprint for deduplication */
  promptFingerprint?: string | undefined
  /** Completion content fingerprint for deduplication */
  completionFingerprint?: string | undefined
  /** Content ID for correlation with GeneratedContentMV */
  contentId?: string | undefined
  /** Tenant identifier (for multi-tenant deployments) */
  tenantId?: string | undefined
}

/**
 * Input for recording an AI request
 */
export interface RecordAIRequestInput {
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: AIProvider
  /** Request type */
  requestType: AIRequestType
  /** Total latency in milliseconds */
  latencyMs: number
  /** Number of prompt tokens (optional) */
  promptTokens?: number | undefined
  /** Number of completion tokens (optional) */
  completionTokens?: number | undefined
  /** Whether request succeeded (default: true) */
  success?: boolean | undefined
  /** Whether response was cached (default: false) */
  cached?: boolean | undefined
  /** Finish reason */
  finishReason?: string | undefined
  /** Error message */
  error?: string | undefined
  /** Error code */
  errorCode?: string | undefined
  /** Temperature setting */
  temperature?: number | undefined
  /** Max tokens setting */
  maxTokens?: number | undefined
  /** Whether tools were used */
  toolsUsed?: boolean | undefined
  /** Number of tool calls */
  toolCallCount?: number | undefined
  /** User identifier */
  userId?: string | undefined
  /** Application identifier */
  appId?: string | undefined
  /** Environment */
  environment?: string | undefined
  /** Custom request ID */
  requestId?: string | undefined
  /** Custom timestamp */
  timestamp?: Date | undefined
  /** Custom cost override */
  estimatedCost?: number | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
  // Content sampling fields
  /** Prompt content to sample (optional, requires sampling to be enabled) */
  promptSample?: string | undefined
  /** Completion content to sample (optional, requires sampling to be enabled) */
  completionSample?: string | undefined
  /** Content ID for correlation with GeneratedContentMV */
  contentId?: string | undefined
  /** Tenant identifier (overrides config tenantId for this record) */
  tenantId?: string | undefined
}

/**
 * Query options for finding AI requests
 */
export interface AIRequestsQueryOptions {
  /** Filter by model ID */
  modelId?: string | undefined
  /** Filter by provider ID */
  providerId?: AIProvider | undefined
  /** Filter by request type */
  requestType?: AIRequestType | undefined
  /** Filter by status */
  status?: AIRequestStatus | undefined
  /** Filter by user ID */
  userId?: string | undefined
  /** Filter by app ID */
  appId?: string | undefined
  /** Filter by environment */
  environment?: string | undefined
  /** Start time (inclusive) */
  from?: Date | undefined
  /** End time (exclusive) */
  to?: Date | undefined
  /** Include only cached requests */
  cachedOnly?: boolean | undefined
  /** Include only requests with errors */
  errorsOnly?: boolean | undefined
  /** Include only requests with sampled content */
  hasSampledContent?: boolean | undefined
  /** Maximum results */
  limit?: number | undefined
  /** Skip first N results */
  offset?: number | undefined
  /** Sort field */
  sort?: 'timestamp' | '-timestamp' | 'latencyMs' | '-latencyMs' | 'estimatedCost' | '-estimatedCost' | undefined
  /** Filter by tenant ID (overrides config tenantId for this query) */
  tenantId?: string | undefined
  /** Query all tenants (requires allowCrossTenantQueries: true) */
  allTenants?: boolean | undefined
}

/**
 * Statistics for AI requests
 */
export interface AIRequestsStats {
  /** Total number of requests */
  totalRequests: number
  /** Successful requests */
  successCount: number
  /** Failed requests */
  errorCount: number
  /** Error rate (0-1) */
  errorRate: number
  /** Cached responses */
  cacheHits: number
  /** Cache hit ratio (0-1) */
  cacheHitRatio: number
  /** Token statistics */
  tokens: {
    totalPromptTokens: number
    totalCompletionTokens: number
    totalTokens: number
    avgPromptTokens: number
    avgCompletionTokens: number
    avgTotalTokens: number
  }
  /** Cost statistics */
  cost: {
    totalCost: number
    avgCost: number
    cacheSavings: number
  }
  /** Latency statistics */
  latency: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
  }
  /** Breakdown by model */
  byModel: Record<string, { count: number; cost: number; avgLatency: number }>
  /** Breakdown by provider */
  byProvider: Record<string, { count: number; cost: number; avgLatency: number }>
  /** Breakdown by request type */
  byRequestType: Record<string, number>
  /** Time range */
  timeRange: { from: Date; to: Date }
}

/**
 * Content redactor function type
 * Can be sync or async, receives content and returns redacted content
 */
export type ContentRedactor = (content: string) => string | Promise<string>

/**
 * Configuration for content sampling
 */
export interface ContentSamplingConfig {
  /** Whether content sampling is enabled (default: false) */
  enabled: boolean
  /** Sample rate (0-1) - fraction of requests to sample (default: 0) */
  sampleRate?: number | undefined
  /** Maximum characters for prompt sample (default: unlimited) */
  maxPromptChars?: number | undefined
  /** Maximum characters for completion sample (default: unlimited) */
  maxCompletionChars?: number | undefined
  /** Sample all error requests regardless of sample rate (default: false) */
  sampleAllErrors?: boolean | undefined
  /** Generate content fingerprint for deduplication (default: false) */
  generateFingerprint?: boolean | undefined
  /** Redactor function for PII detection/removal */
  redactor?: ContentRedactor | undefined
}

/**
 * Resolved content sampling configuration with defaults
 */
export interface ResolvedContentSamplingConfig {
  enabled: boolean
  sampleRate: number
  maxPromptChars?: number | undefined
  maxCompletionChars?: number | undefined
  sampleAllErrors: boolean
  generateFingerprint: boolean
  redactor?: ContentRedactor | undefined
}

/**
 * Configuration for AIRequestsMV
 *
 * Extends MultiTenantConfig for multi-tenant deployments.
 */
export interface AIRequestsMVConfig extends MultiTenantConfig {
  /** Collection name for storing requests (default: 'ai_requests') */
  collection?: string | undefined
  /** Maximum age of requests to keep (default: 30 days) */
  maxAgeMs?: number | undefined
  /** Batch size for operations (default: 1000) */
  batchSize?: number | undefined
  /** Custom model pricing */
  customPricing?: ModelPricing[] | undefined
  /** Whether to merge with default pricing (default: true) */
  mergeWithDefaultPricing?: boolean | undefined
  /** Enable debug logging */
  debug?: boolean | undefined
  /** Content sampling configuration */
  contentSampling?: ContentSamplingConfig | undefined
}

/**
 * Resolved configuration with defaults
 */
export interface ResolvedAIRequestsMVConfig {
  collection: string
  maxAgeMs: number
  batchSize: number
  pricing: Map<string, ModelPricing>
  debug: boolean
  contentSampling: ResolvedContentSamplingConfig
  /** Tenant identifier */
  tenantId?: string | undefined
  /** Whether to use tenant-scoped storage paths */
  tenantScopedStorage: boolean
  /** Whether cross-tenant queries are allowed */
  allowCrossTenantQueries: boolean
}

/**
 * Result of a cleanup operation
 */
export interface CleanupResult {
  /** Whether cleanup completed successfully */
  success: boolean
  /** Total records deleted */
  deletedCount: number
  /** Duration of cleanup in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string | undefined
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a unique AI request ID
 */
export function generateRequestId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `ai_${timestamp}_${random}`
}

/**
 * Simple hash function for content fingerprinting (djb2 algorithm)
 */
export function hashContent(content: string): string {
  let hash = DJB2_INITIAL
  for (let i = 0; i < content.length; i++) {
    hash = ((hash << 5) + hash) ^ content.charCodeAt(i)
  }
  return (hash >>> 0).toString(36)
}

/**
 * Build pricing lookup map
 */
function buildPricingMap(
  customPricing: ModelPricing[] | undefined,
  mergeWithDefault: boolean
): Map<string, ModelPricing> {
  const map = new Map<string, ModelPricing>()

  if (mergeWithDefault) {
    for (const pricing of DEFAULT_MODEL_PRICING) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  if (customPricing) {
    for (const pricing of customPricing) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  return map
}

/**
 * Calculate cost from tokens and pricing
 */
function calculateCost(
  modelId: string,
  providerId: string,
  promptTokens: number,
  completionTokens: number,
  pricingMap: Map<string, ModelPricing>
): number {
  const pricing = pricingMap.get(`${modelId}:${providerId}`)
  if (!pricing) {
    // Try to find by partial match
    for (const [key, p] of pricingMap) {
      if (key.startsWith(modelId) || modelId.startsWith(key.split(':')[0]!)) {
        const inputCost = (promptTokens / 1_000_000) * p.inputPricePerMillion
        const outputCost = (completionTokens / 1_000_000) * p.outputPricePerMillion
        return inputCost + outputCost
      }
    }
    return 0
  }

  const inputCost = (promptTokens / 1_000_000) * pricing.inputPricePerMillion
  const outputCost = (completionTokens / 1_000_000) * pricing.outputPricePerMillion
  return inputCost + outputCost
}

/**
 * Calculate percentile from sorted array
 */
function percentile(sortedArr: number[], p: number): number {
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
 * Resolve content sampling configuration with defaults
 */
function resolveContentSamplingConfig(config?: ContentSamplingConfig): ResolvedContentSamplingConfig {
  if (!config) {
    return {
      enabled: false,
      sampleRate: 0,
      sampleAllErrors: false,
      generateFingerprint: false,
    }
  }

  return {
    enabled: config.enabled,
    sampleRate: config.sampleRate ?? 0,
    maxPromptChars: config.maxPromptChars,
    maxCompletionChars: config.maxCompletionChars,
    sampleAllErrors: config.sampleAllErrors ?? false,
    generateFingerprint: config.generateFingerprint ?? false,
    redactor: config.redactor,
  }
}

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: AIRequestsMVConfig): ResolvedAIRequestsMVConfig {
  const tenantScopedStorage = config.tenantScopedStorage ?? false
  const tenantId = config.tenantId

  // Build tenant-scoped collection name if enabled
  const baseCollection = config.collection ?? DEFAULT_COLLECTION
  const collection = tenantScopedStorage && tenantId
    ? `tenant_${tenantId}/${baseCollection}`
    : baseCollection

  return {
    collection,
    maxAgeMs: config.maxAgeMs ?? DEFAULT_MAX_AGE_MS,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    pricing: buildPricingMap(config.customPricing, config.mergeWithDefaultPricing ?? true),
    debug: config.debug ?? false,
    contentSampling: resolveContentSamplingConfig(config.contentSampling),
    tenantId,
    tenantScopedStorage,
    allowCrossTenantQueries: config.allowCrossTenantQueries ?? false,
  }
}

// =============================================================================
// AIRequestsMV Class
// =============================================================================

/**
 * AIRequestsMV - Materialized View for AI Request Tracking
 *
 * Provides comprehensive tracking and analysis of AI/LLM API requests.
 */
export class AIRequestsMV {
  private readonly db: ParqueDB
  private readonly config: ResolvedAIRequestsMVConfig

  /**
   * Create a new AIRequestsMV instance
   *
   * @param db - ParqueDB instance
   * @param config - Configuration options
   */
  constructor(db: ParqueDB, config: AIRequestsMVConfig = {}) {
    this.db = db
    this.config = resolveConfig(config)
  }

  /**
   * Determine if content should be sampled for a request
   */
  private shouldSampleContent(isError: boolean): boolean {
    const { enabled, sampleRate, sampleAllErrors } = this.config.contentSampling

    if (!enabled) return false

    // Always sample errors if configured to do so
    if (isError && sampleAllErrors) return true

    // Sample based on rate
    if (sampleRate <= 0) return false
    if (sampleRate >= 1) return true

    return Math.random() < sampleRate
  }

  /**
   * Process content for sampling (truncate, redact, fingerprint)
   */
  private async processContent(
    content: string | undefined,
    maxChars: number | undefined,
    generateFingerprint: boolean,
    redactor?: ContentRedactor
  ): Promise<{ sample?: string | undefined; fingerprint?: string | undefined }> {
    if (!content) return {}

    let processed = content

    // Apply redactor if configured
    if (redactor) {
      const result = redactor(processed)
      processed = result instanceof Promise ? await result : result
    }

    // Truncate if needed
    if (maxChars !== undefined && processed.length > maxChars) {
      processed = processed.substring(0, maxChars)
    }

    const result: { sample?: string | undefined; fingerprint?: string | undefined } = {
      sample: processed,
    }

    // Generate fingerprint if requested
    if (generateFingerprint) {
      result.fingerprint = hashContent(content) // Use original content for fingerprint
    }

    return result
  }

  /**
   * Record a single AI request
   *
   * @param input - Request data
   * @returns Created request record
   */
  async record(input: RecordAIRequestInput): Promise<AIRequestRecord> {
    const collection = this.db.collection(this.config.collection)
    const now = new Date()

    const promptTokens = input.promptTokens ?? 0
    const completionTokens = input.completionTokens ?? 0
    const totalTokens = promptTokens + completionTokens

    const estimatedCost = input.estimatedCost ?? calculateCost(
      input.modelId,
      input.providerId,
      promptTokens,
      completionTokens,
      this.config.pricing
    )

    const requestId = input.requestId ?? generateRequestId()
    const timestamp = input.timestamp ?? now

    const isError = input.success === false
    const status: AIRequestStatus = isError
      ? (input.errorCode === 'TIMEOUT' ? 'timeout' : 'error')
      : 'success'

    // Process content sampling
    const shouldSample = this.shouldSampleContent(isError)
    const { contentSampling } = this.config

    let promptSample: string | undefined
    let completionSample: string | undefined
    let promptFingerprint: string | undefined
    let completionFingerprint: string | undefined

    if (shouldSample && input.promptSample) {
      const processed = await this.processContent(
        input.promptSample,
        contentSampling.maxPromptChars,
        contentSampling.generateFingerprint,
        contentSampling.redactor
      )
      promptSample = processed.sample
      promptFingerprint = processed.fingerprint
    }

    if (shouldSample && input.completionSample) {
      const processed = await this.processContent(
        input.completionSample,
        contentSampling.maxCompletionChars,
        contentSampling.generateFingerprint,
        contentSampling.redactor
      )
      completionSample = processed.sample
      completionFingerprint = processed.fingerprint
    }

    // Use input tenantId or fall back to config tenantId
    const tenantId = input.tenantId ?? this.config.tenantId

    const data: Omit<AIRequestRecord, '$id'> = {
      $type: 'AIRequest',
      name: requestId,
      requestId,
      timestamp,
      modelId: input.modelId,
      providerId: input.providerId,
      requestType: input.requestType,
      status,
      latencyMs: input.latencyMs,
      promptTokens,
      completionTokens,
      totalTokens,
      estimatedCost,
      cached: input.cached ?? false,
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
      createdAt: now,
      promptSample,
      completionSample,
      promptFingerprint,
      completionFingerprint,
      contentId: shouldSample ? input.contentId : undefined,
      tenantId,
    }

    const created = await collection.create(data as Record<string, unknown>)
    return asCreatedRecord<AIRequestRecord>(created)
  }

  /**
   * Record multiple AI requests in batch
   *
   * @param inputs - Array of request data
   * @returns Array of created request records
   */
  async recordMany(inputs: RecordAIRequestInput[]): Promise<AIRequestRecord[]> {
    const collection = this.db.collection(this.config.collection)
    const now = new Date()

    const records = await Promise.all(inputs.map(async input => {
      const promptTokens = input.promptTokens ?? 0
      const completionTokens = input.completionTokens ?? 0
      const totalTokens = promptTokens + completionTokens

      const estimatedCost = input.estimatedCost ?? calculateCost(
        input.modelId,
        input.providerId,
        promptTokens,
        completionTokens,
        this.config.pricing
      )

      const requestId = input.requestId ?? generateRequestId()
      const timestamp = input.timestamp ?? now

      const isError = input.success === false
      const status: AIRequestStatus = isError
        ? (input.errorCode === 'TIMEOUT' ? 'timeout' : 'error')
        : 'success'

      // Process content sampling
      const shouldSample = this.shouldSampleContent(isError)
      const { contentSampling } = this.config

      let promptSample: string | undefined
      let completionSample: string | undefined
      let promptFingerprint: string | undefined
      let completionFingerprint: string | undefined

      if (shouldSample && input.promptSample) {
        const processed = await this.processContent(
          input.promptSample,
          contentSampling.maxPromptChars,
          contentSampling.generateFingerprint,
          contentSampling.redactor
        )
        promptSample = processed.sample
        promptFingerprint = processed.fingerprint
      }

      if (shouldSample && input.completionSample) {
        const processed = await this.processContent(
          input.completionSample,
          contentSampling.maxCompletionChars,
          contentSampling.generateFingerprint,
          contentSampling.redactor
        )
        completionSample = processed.sample
        completionFingerprint = processed.fingerprint
      }

      // Use input tenantId or fall back to config tenantId
      const tenantId = input.tenantId ?? this.config.tenantId

      return {
        $type: 'AIRequest',
        name: requestId,
        requestId,
        timestamp,
        modelId: input.modelId,
        providerId: input.providerId,
        requestType: input.requestType,
        status,
        latencyMs: input.latencyMs,
        promptTokens,
        completionTokens,
        totalTokens,
        estimatedCost,
        cached: input.cached ?? false,
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
        createdAt: now,
        promptSample,
        completionSample,
        promptFingerprint,
        completionFingerprint,
        contentId: shouldSample ? input.contentId : undefined,
        tenantId,
      }
    }))

    const created = await collection.createMany(records as Record<string, unknown>[])
    return asTypedResults<AIRequestRecord>(created)
  }

  /**
   * Find AI requests matching the query options
   *
   * @param options - Query options
   * @returns Array of matching request records
   */
  async find(options: AIRequestsQueryOptions = {}): Promise<AIRequestRecord[]> {
    const collection = this.db.collection(this.config.collection)
    const filter: Record<string, unknown> = {}

    // Apply tenant filtering
    if (!options.allTenants) {
      // Use query tenantId, fall back to config tenantId
      const tenantId = options.tenantId ?? this.config.tenantId
      if (tenantId && !this.config.tenantScopedStorage) {
        filter.tenantId = tenantId
      }
    } else if (!this.config.allowCrossTenantQueries) {
      // allTenants requested but not allowed
      throw new Error('Cross-tenant queries are not enabled. Set allowCrossTenantQueries: true in config.')
    }

    // Apply filters
    if (options.modelId) {
      filter.modelId = options.modelId
    }

    if (options.providerId) {
      filter.providerId = options.providerId
    }

    if (options.requestType) {
      filter.requestType = options.requestType
    }

    if (options.status) {
      filter.status = options.status
    }

    if (options.userId) {
      filter.userId = options.userId
    }

    if (options.appId) {
      filter.appId = options.appId
    }

    if (options.environment) {
      filter.environment = options.environment
    }

    if (options.from || options.to) {
      filter.timestamp = {}
      if (options.from) {
        (filter.timestamp as Record<string, unknown>).$gte = options.from
      }
      if (options.to) {
        (filter.timestamp as Record<string, unknown>).$lt = options.to
      }
    }

    if (options.cachedOnly !== undefined) {
      filter.cached = options.cachedOnly
    }

    if (options.errorsOnly) {
      filter.status = 'error'
    }

    if (options.hasSampledContent) {
      filter.promptSample = { $exists: true, $ne: null }
    }

    // Build sort
    const sortField = options.sort?.replace('-', '') ?? 'timestamp'
    const sortOrder = options.sort?.startsWith('-') ? -1 : 1

    const results = await collection.find(filter, {
      limit: options.limit ?? DEFAULT_LIMIT,
      sort: { [sortField]: sortOrder },
    })

    return asTypedResults<AIRequestRecord>(results)
  }

  /**
   * Get a single request by ID
   *
   * @param requestId - The request ID
   * @returns The request record or null
   */
  async findOne(requestId: string): Promise<AIRequestRecord | null> {
    const collection = this.db.collection(this.config.collection)
    const results = await collection.find({ requestId }, { limit: 1 })

    if (results.length === 0) {
      return null
    }

    return asTypedResult<AIRequestRecord>(results[0])
  }

  /**
   * Get aggregated statistics for AI requests
   *
   * @param options - Query options to filter requests
   * @returns Aggregated statistics
   */
  async getStats(options: AIRequestsQueryOptions = {}): Promise<AIRequestsStats> {
    // Get all matching requests (up to a reasonable limit)
    const requests = await this.find({
      ...options,
      limit: options.limit ?? 10000,
    })

    if (requests.length === 0) {
      const now = new Date()
      return {
        totalRequests: 0,
        successCount: 0,
        errorCount: 0,
        errorRate: 0,
        cacheHits: 0,
        cacheHitRatio: 0,
        tokens: {
          totalPromptTokens: 0,
          totalCompletionTokens: 0,
          totalTokens: 0,
          avgPromptTokens: 0,
          avgCompletionTokens: 0,
          avgTotalTokens: 0,
        },
        cost: {
          totalCost: 0,
          avgCost: 0,
          cacheSavings: 0,
        },
        latency: {
          min: 0,
          max: 0,
          avg: 0,
          p50: 0,
          p95: 0,
          p99: 0,
        },
        byModel: {},
        byProvider: {},
        byRequestType: {},
        timeRange: { from: options.from ?? now, to: options.to ?? now },
      }
    }

    // Calculate counts
    const successCount = requests.filter(r => r.status === 'success').length
    const errorCount = requests.filter(r => r.status === 'error' || r.status === 'timeout').length
    const cacheHits = requests.filter(r => r.cached).length

    // Calculate token stats
    const totalPromptTokens = requests.reduce((sum, r) => sum + r.promptTokens, 0)
    const totalCompletionTokens = requests.reduce((sum, r) => sum + r.completionTokens, 0)
    const totalTokens = totalPromptTokens + totalCompletionTokens

    // Calculate cost stats
    const nonCachedRequests = requests.filter(r => !r.cached)
    const cachedRequests = requests.filter(r => r.cached)
    const totalCost = nonCachedRequests.reduce((sum, r) => sum + r.estimatedCost, 0)
    const cacheSavings = cachedRequests.reduce((sum, r) => sum + r.estimatedCost, 0)

    // Calculate latency stats
    const latencies = requests.map(r => r.latencyMs).sort((a, b) => a - b)
    const totalLatency = latencies.reduce((sum, l) => sum + l, 0)

    // Calculate breakdowns
    const byModel: Record<string, { count: number; cost: number; avgLatency: number; totalLatency: number }> = {}
    const byProvider: Record<string, { count: number; cost: number; avgLatency: number; totalLatency: number }> = {}
    const byRequestType: Record<string, number> = {}

    for (const r of requests) {
      // By model
      if (!byModel[r.modelId]) {
        byModel[r.modelId] = { count: 0, cost: 0, avgLatency: 0, totalLatency: 0 }
      }
      byModel[r.modelId]!.count++
      byModel[r.modelId]!.cost += r.cached ? 0 : r.estimatedCost
      byModel[r.modelId]!.totalLatency += r.latencyMs

      // By provider
      if (!byProvider[r.providerId]) {
        byProvider[r.providerId] = { count: 0, cost: 0, avgLatency: 0, totalLatency: 0 }
      }
      byProvider[r.providerId]!.count++
      byProvider[r.providerId]!.cost += r.cached ? 0 : r.estimatedCost
      byProvider[r.providerId]!.totalLatency += r.latencyMs

      // By request type
      byRequestType[r.requestType] = (byRequestType[r.requestType] ?? 0) + 1
    }

    // Calculate averages for breakdowns
    for (const modelId of Object.keys(byModel)) {
      byModel[modelId]!.avgLatency = byModel[modelId]!.totalLatency / byModel[modelId]!.count
    }
    for (const providerId of Object.keys(byProvider)) {
      byProvider[providerId]!.avgLatency = byProvider[providerId]!.totalLatency / byProvider[providerId]!.count
    }

    // Clean up internal totalLatency field
    const cleanByModel: Record<string, { count: number; cost: number; avgLatency: number }> = {}
    for (const [k, v] of Object.entries(byModel)) {
      cleanByModel[k] = { count: v.count, cost: v.cost, avgLatency: v.avgLatency }
    }
    const cleanByProvider: Record<string, { count: number; cost: number; avgLatency: number }> = {}
    for (const [k, v] of Object.entries(byProvider)) {
      cleanByProvider[k] = { count: v.count, cost: v.cost, avgLatency: v.avgLatency }
    }

    // Get time range from requests
    const timestamps = requests.map(r => r.timestamp.getTime())
    const minTs = Math.min(...timestamps)
    const maxTs = Math.max(...timestamps)

    return {
      totalRequests: requests.length,
      successCount,
      errorCount,
      errorRate: requests.length > 0 ? errorCount / requests.length : 0,
      cacheHits,
      cacheHitRatio: requests.length > 0 ? cacheHits / requests.length : 0,
      tokens: {
        totalPromptTokens,
        totalCompletionTokens,
        totalTokens,
        avgPromptTokens: requests.length > 0 ? totalPromptTokens / requests.length : 0,
        avgCompletionTokens: requests.length > 0 ? totalCompletionTokens / requests.length : 0,
        avgTotalTokens: requests.length > 0 ? totalTokens / requests.length : 0,
      },
      cost: {
        totalCost,
        avgCost: requests.length > 0 ? totalCost / requests.length : 0,
        cacheSavings,
      },
      latency: {
        min: latencies[0] ?? 0,
        max: latencies[latencies.length - 1] ?? 0,
        avg: requests.length > 0 ? totalLatency / requests.length : 0,
        p50: percentile(latencies, 50),
        p95: percentile(latencies, 95),
        p99: percentile(latencies, 99),
      },
      byModel: cleanByModel,
      byProvider: cleanByProvider,
      byRequestType,
      timeRange: {
        from: options.from ?? new Date(minTs),
        to: options.to ?? new Date(maxTs),
      },
    }
  }

  /**
   * Get error summary
   *
   * @param options - Query options to filter requests
   * @returns Error summary
   */
  async getErrorSummary(options: Omit<AIRequestsQueryOptions, 'errorsOnly' | 'status'> = {}): Promise<{
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
    // Get all requests for error rate calculation
    const allRequests = await this.find({
      ...options,
      limit: options.limit ?? 10000,
    })

    // Get error requests
    const errorRequests = allRequests.filter(r => r.status === 'error' || r.status === 'timeout')

    const byModel: Record<string, number> = {}
    const byProvider: Record<string, number> = {}
    const byErrorCode: Record<string, number> = {}

    for (const r of errorRequests) {
      byModel[r.modelId] = (byModel[r.modelId] ?? 0) + 1
      byProvider[r.providerId] = (byProvider[r.providerId] ?? 0) + 1
      const code = r.errorCode ?? 'UNKNOWN'
      byErrorCode[code] = (byErrorCode[code] ?? 0) + 1
    }

    const recentErrors = errorRequests
      .sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())
      .slice(0, 10)
      .map(r => ({
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

  /**
   * Get cost summary
   *
   * @param options - Query options to filter requests
   * @returns Cost summary
   */
  async getCostSummary(options: AIRequestsQueryOptions = {}): Promise<{
    totalCost: number
    cacheSavings: number
    byModel: Record<string, { cost: number; requests: number; tokens: number }>
    byProvider: Record<string, { cost: number; requests: number; tokens: number }>
    topExpensive: Array<{
      requestId: string
      modelId: string
      cost: number
      totalTokens: number
      timestamp: Date
    }>
  }> {
    const requests = await this.find({
      ...options,
      limit: options.limit ?? 10000,
    })

    const nonCachedRequests = requests.filter(r => !r.cached)
    const cachedRequests = requests.filter(r => r.cached)

    const totalCost = nonCachedRequests.reduce((sum, r) => sum + r.estimatedCost, 0)
    const cacheSavings = cachedRequests.reduce((sum, r) => sum + r.estimatedCost, 0)

    const byModel: Record<string, { cost: number; requests: number; tokens: number }> = {}
    const byProvider: Record<string, { cost: number; requests: number; tokens: number }> = {}

    for (const r of requests) {
      // By model
      if (!byModel[r.modelId]) {
        byModel[r.modelId] = { cost: 0, requests: 0, tokens: 0 }
      }
      byModel[r.modelId]!.cost += r.cached ? 0 : r.estimatedCost
      byModel[r.modelId]!.requests++
      byModel[r.modelId]!.tokens += r.totalTokens

      // By provider
      if (!byProvider[r.providerId]) {
        byProvider[r.providerId] = { cost: 0, requests: 0, tokens: 0 }
      }
      byProvider[r.providerId]!.cost += r.cached ? 0 : r.estimatedCost
      byProvider[r.providerId]!.requests++
      byProvider[r.providerId]!.tokens += r.totalTokens
    }

    const topExpensive = [...nonCachedRequests]
      .sort((a, b) => b.estimatedCost - a.estimatedCost)
      .slice(0, 10)
      .map(r => ({
        requestId: r.requestId,
        modelId: r.modelId,
        cost: r.estimatedCost,
        totalTokens: r.totalTokens,
        timestamp: r.timestamp,
      }))

    return {
      totalCost,
      cacheSavings,
      byModel,
      byProvider,
      topExpensive,
    }
  }

  /**
   * Delete old requests beyond the max age
   *
   * Uses batch delete for efficiency (O(1) vs O(n) individual deletes).
   *
   * @param options - Optional cleanup options
   * @returns Cleanup result with deleted count and details
   */
  async cleanup(options?: {
    /** Override the default max age */
    maxAgeMs?: number | undefined
    /** Progress callback */
    onProgress?: ((progress: { deletedSoFar: number; percentage: number }) => void) | undefined
  }): Promise<CleanupResult> {
    const collection = this.db.collection(this.config.collection)
    const maxAgeMs = options?.maxAgeMs ?? this.config.maxAgeMs
    const cutoffDate = new Date(Date.now() - maxAgeMs)
    const startTime = Date.now()

    try {
      // Count total to delete
      const filter = { timestamp: { $lt: cutoffDate } }
      const totalToDelete = await collection.count(filter)

      if (totalToDelete === 0) {
        return {
          success: true,
          deletedCount: 0,
          durationMs: Date.now() - startTime,
        }
      }

      // Use batch delete for efficiency
      const result = await collection.deleteMany(filter, { hard: true })

      options?.onProgress?.({
        deletedSoFar: result.deletedCount,
        percentage: 100,
      })

      return {
        success: true,
        deletedCount: result.deletedCount,
        durationMs: Date.now() - startTime,
      }
    } catch (error) {
      return {
        success: false,
        deletedCount: 0,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Get a cleanup scheduler for background retention management
   *
   * @param options - Scheduler options
   * @returns Cleanup scheduler handle
   */
  getRetentionManager(): import('../retention').RetentionManager {
    // Lazy import to avoid circular dependencies
    const { RetentionManager } = require('../retention')
    return new RetentionManager(this.db, {
      collection: this.config.collection,
      maxAgeMs: this.config.maxAgeMs,
      timestampField: 'timestamp',
      debug: this.config.debug,
    })
  }

  /**
   * Get the current configuration
   */
  getConfig(): ResolvedAIRequestsMVConfig {
    return { ...this.config, contentSampling: { ...this.config.contentSampling } }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create an AIRequestsMV instance
 *
 * @param db - ParqueDB instance
 * @param config - Configuration options
 * @returns AIRequestsMV instance
 *
 * @example
 * ```typescript
 * const requestsMV = createAIRequestsMV(db)
 * await requestsMV.record({ modelId: 'gpt-4', ... })
 * const stats = await requestsMV.getStats()
 * ```
 */
export function createAIRequestsMV(db: ParqueDB, config: AIRequestsMVConfig = {}): AIRequestsMV {
  return new AIRequestsMV(db, config)
}

/**
 * AIUsageMV - Materialized View for AI API Usage Tracking
 *
 * Aggregates AI API requests by model, provider, and time period to provide
 * cost estimates, usage statistics, and performance metrics.
 *
 * Features:
 * - Aggregation by model/provider/day (configurable granularity)
 * - Cost estimation based on token usage and model pricing
 * - Latency statistics (avg, min, max, percentiles)
 * - Error rate tracking
 * - Incremental refresh support
 *
 * @example
 * ```typescript
 * import { AIUsageMV } from 'parquedb/observability'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 * const usageMV = new AIUsageMV(db, {
 *   granularity: 'day',
 *   customPricing: [
 *     { modelId: 'my-fine-tuned-model', providerId: 'openai', inputPricePerMillion: 5.00, outputPricePerMillion: 15.00 }
 *   ]
 * })
 *
 * // Refresh the view (process new logs)
 * await usageMV.refresh()
 *
 * // Query usage
 * const usage = await usageMV.getUsage({ modelId: 'gpt-4', from: new Date('2026-02-01') })
 *
 * // Get summary
 * const summary = await usageMV.getSummary({ from: new Date('2026-02-01') })
 * ```
 *
 * @module observability/ai/AIUsageMV
 */

import type { ParqueDB } from '../../ParqueDB'
import type {
  AIUsageMVConfig,
  ResolvedAIUsageMVConfig,
  ModelPricing,
  AIUsageAggregate,
  AIUsageSummary,
  AIUsageQueryOptions,
  RefreshResult,
  TimeGranularity,
  TenantUsageSummary,
  CrossTenantAggregate,
} from './types'
import { DEFAULT_MODEL_PRICING } from './types'
import { AI_USAGE_MAX_AGE_MS, MAX_BATCH_SIZE } from '../../constants'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_SOURCE_COLLECTION = 'ai_logs'
const DEFAULT_TARGET_COLLECTION = 'ai_usage'
const DEFAULT_GRANULARITY: TimeGranularity = 'day'
const DEFAULT_MAX_AGE_MS = AI_USAGE_MAX_AGE_MS
const DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE

/**
 * Maximum reservoir size for latency sampling
 * This provides a good balance between accuracy and memory usage
 */
const LATENCY_RESERVOIR_SIZE = 1000

// =============================================================================
// Working Aggregate Type (includes latency samples for percentile calculation)
// =============================================================================

/**
 * Working aggregate that includes latency samples for percentile calculation
 * This is used during aggregation and converted to AIUsageAggregate when persisted
 */
interface WorkingAggregate extends AIUsageAggregate {
  /** Latency samples for percentile calculation (reservoir sampling) */
  _latencySamples: number[]
  /** Count of total items seen (for reservoir sampling) */
  _sampleCount: number
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a date key based on granularity
 */
function generateDateKey(date: Date, granularity: TimeGranularity): string {
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  const hour = String(date.getUTCHours()).padStart(2, '0')

  switch (granularity) {
    case 'hour':
      return `${year}-${month}-${day}T${hour}`
    case 'day':
      return `${year}-${month}-${day}`
    case 'week': {
      // ISO week: Get the Thursday of the current week
      const d = new Date(date)
      d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
      const weekNum = Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7)
      return `${d.getUTCFullYear()}-W${String(weekNum).padStart(2, '0')}`
    }
    case 'month':
      return `${year}-${month}`
    default:
      return `${year}-${month}-${day}`
  }
}

/**
 * Generate aggregate ID from model, provider, and date key
 */
function generateAggregateId(modelId: string, providerId: string, dateKey: string): string {
  // Normalize IDs to be safe for use in keys
  const safeModelId = (modelId ?? 'unknown').replace(/[^a-zA-Z0-9-_.]/g, '_')
  const safeProviderId = (providerId ?? 'unknown').replace(/[^a-zA-Z0-9-_.]/g, '_')
  return `${safeModelId}_${safeProviderId}_${dateKey}`
}

/**
 * Calculate cost based on token usage and pricing
 */
function calculateCost(
  promptTokens: number,
  completionTokens: number,
  pricing: ModelPricing | undefined
): { inputCost: number; outputCost: number; totalCost: number } {
  if (!pricing) {
    return { inputCost: 0, outputCost: 0, totalCost: 0 }
  }

  const inputCost = (promptTokens / 1_000_000) * pricing.inputPricePerMillion
  const outputCost = (completionTokens / 1_000_000) * pricing.outputPricePerMillion

  return {
    inputCost,
    outputCost,
    totalCost: inputCost + outputCost,
  }
}

/**
 * Normalize model ID for pricing lookup
 *
 * Handles variations like 'gpt-4-0613', 'gpt-4-turbo-2024-04-09' -> 'gpt-4', 'gpt-4-turbo'
 */
function normalizeModelId(modelId: string): string {
  if (!modelId) return 'unknown'

  // Remove date suffixes (e.g., -0613, -2024-04-09)
  let normalized = modelId.replace(/-\d{4}(-\d{2}(-\d{2})?)?$/, '')
  normalized = normalized.replace(/-\d{4}$/, '')

  // Common normalizations
  const mappings: Record<string, string> = {
    'gpt-4-turbo-preview': 'gpt-4-turbo',
    'gpt-4-vision-preview': 'gpt-4-turbo',
    'claude-3-opus-20240229': 'claude-3-opus',
    'claude-3-sonnet-20240229': 'claude-3-sonnet',
    'claude-3-haiku-20240307': 'claude-3-haiku',
    'claude-3-5-sonnet-20240620': 'claude-3-5-sonnet',
    'claude-3-5-sonnet-20241022': 'claude-3-5-sonnet',
  }

  return mappings[normalized] || normalized
}

/**
 * Build pricing lookup map
 */
function buildPricingMap(
  customPricing: ModelPricing[] | undefined,
  mergeWithDefault: boolean
): Map<string, ModelPricing> {
  const map = new Map<string, ModelPricing>()

  // Add default pricing first (if merging)
  if (mergeWithDefault) {
    for (const pricing of DEFAULT_MODEL_PRICING) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  // Override with custom pricing
  if (customPricing) {
    for (const pricing of customPricing) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  return map
}

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: AIUsageMVConfig): ResolvedAIUsageMVConfig {
  const tenantScopedStorage = config.tenantScopedStorage ?? false
  const tenantId = config.tenantId

  // Build tenant-scoped collection names if enabled
  const baseSourceCollection = config.sourceCollection ?? DEFAULT_SOURCE_COLLECTION
  const baseTargetCollection = config.targetCollection ?? DEFAULT_TARGET_COLLECTION

  const sourceCollection = tenantScopedStorage && tenantId
    ? `tenant_${tenantId}/${baseSourceCollection}`
    : baseSourceCollection
  const targetCollection = tenantScopedStorage && tenantId
    ? `tenant_${tenantId}/${baseTargetCollection}`
    : baseTargetCollection

  return {
    sourceCollection,
    targetCollection,
    granularity: config.granularity ?? DEFAULT_GRANULARITY,
    pricing: buildPricingMap(config.customPricing, config.mergeWithDefaultPricing ?? true),
    pricingService: config.pricingService,
    maxAgeMs: config.maxAgeMs ?? DEFAULT_MAX_AGE_MS,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    debug: config.debug ?? false,
    tenantId,
    tenantScopedStorage,
    allowCrossTenantQueries: config.allowCrossTenantQueries ?? false,
  }
}

// =============================================================================
// AIUsageMV Class
// =============================================================================

/**
 * AIUsageMV - Materialized View for AI API Usage Tracking
 *
 * Aggregates AI API logs into usage summaries by model, provider, and time period.
 * Supports incremental refresh and configurable granularity.
 */
export class AIUsageMV {
  private readonly db: ParqueDB
  private readonly config: ResolvedAIUsageMVConfig
  private lastRefreshTime?: Date

  /**
   * Create a new AIUsageMV instance
   *
   * @param db - ParqueDB instance
   * @param config - Configuration options
   */
  constructor(db: ParqueDB, config: AIUsageMVConfig = {}) {
    this.db = db
    this.config = resolveConfig(config)
  }

  /**
   * Get pricing information for a model
   *
   * If a pricingService is configured, it will be used for dynamic pricing.
   * Otherwise, falls back to the static pricing map.
   */
  getPricing(modelId: string, providerId: string): ModelPricing | undefined {
    // Use pricing service if available (dynamic pricing)
    if (this.config.pricingService) {
      return this.config.pricingService.getPricing(modelId, providerId)
    }

    // Fall back to static pricing map
    const normalizedModelId = normalizeModelId(modelId)
    return this.config.pricing.get(`${normalizedModelId}:${providerId}`)
  }

  /**
   * Refresh the materialized view
   *
   * Processes new AI log entries and updates usage aggregates.
   * Supports incremental refresh - only processes logs since last refresh.
   *
   * @param options - Refresh options
   * @returns Refresh result with statistics
   */
  async refresh(options: { full?: boolean } = {}): Promise<RefreshResult> {
    const startTime = Date.now()
    let recordsProcessed = 0
    let aggregatesUpdated = 0

    try {
      const sourceCollection = this.db.collection(this.config.sourceCollection)
      const targetCollection = this.db.collection(this.config.targetCollection)

      // Determine time range for logs to process
      const now = new Date()
      const maxAge = new Date(now.getTime() - this.config.maxAgeMs)

      // Build filter for logs to process
      const filter: Record<string, unknown> = {
        timestamp: { $gte: maxAge },
      }

      // For incremental refresh, only get logs since last refresh
      if (!options.full && this.lastRefreshTime) {
        filter.timestamp = { $gte: this.lastRefreshTime }
      }

      // Apply tenant filter if configured (unless using tenant-scoped storage)
      if (this.config.tenantId && !this.config.tenantScopedStorage) {
        filter.tenantId = this.config.tenantId
      }

      // Fetch logs in batches
      const logs = await sourceCollection.find(filter, {
        limit: this.config.batchSize,
        sort: { timestamp: 1 },
      })

      if (logs.length === 0) {
        return {
          success: true,
          recordsProcessed: 0,
          aggregatesUpdated: 0,
          durationMs: Date.now() - startTime,
        }
      }

      // Group logs by aggregate key
      const aggregates = new Map<string, WorkingAggregate>()

      for (const log of logs) {
        recordsProcessed++

        const modelId = ((log as Record<string, unknown>).modelId as string) ?? 'unknown'
        const providerId = ((log as Record<string, unknown>).providerId as string) ?? 'unknown'
        const timestamp = new Date((log as Record<string, unknown>).timestamp as string | Date)
        const dateKey = generateDateKey(timestamp, this.config.granularity)
        const aggregateId = generateAggregateId(modelId, providerId, dateKey)

        // Get or create aggregate
        let aggregate = aggregates.get(aggregateId)
        if (!aggregate) {
          // Try to load existing aggregate from DB
          // Include tenantId in the lookup if not using scoped storage
          const aggregateFilter: Record<string, unknown> = { dateKey, modelId, providerId }
          if (this.config.tenantId && !this.config.tenantScopedStorage) {
            aggregateFilter.tenantId = this.config.tenantId
          }
          const existingAggregates = await targetCollection.find(aggregateFilter, { limit: 1 })
          if (existingAggregates.length > 0) {
            aggregate = normalizeAggregateFromDB(existingAggregates[0] as unknown as AIUsageAggregate)
          } else {
            aggregate = createEmptyAggregate(aggregateId, modelId, providerId, dateKey, this.config.granularity, this.config.tenantId)
          }
          aggregates.set(aggregateId, aggregate)
        }

        // Update aggregate with log entry
        // Use bound getPricing method to support both static and dynamic pricing
        updateAggregateFromLog(aggregate, log as Record<string, unknown>, (modelId, providerId) => this.getPricing(modelId, providerId))
      }

      // Save all aggregates
      for (const aggregate of aggregates.values()) {
        aggregate.updatedAt = now
        aggregate.version++

        // Calculate percentiles from latency samples
        const percentiles = calculatePercentiles(aggregate._latencySamples)
        aggregate.p50LatencyMs = percentiles.p50
        aggregate.p90LatencyMs = percentiles.p90
        aggregate.p95LatencyMs = percentiles.p95
        aggregate.p99LatencyMs = percentiles.p99

        // Check if this aggregate exists
        // Include tenantId in the lookup if not using scoped storage
        const existingFilter: Record<string, unknown> = { dateKey: aggregate.dateKey, modelId: aggregate.modelId, providerId: aggregate.providerId }
        if (this.config.tenantId && !this.config.tenantScopedStorage) {
          existingFilter.tenantId = this.config.tenantId
        }
        const existing = await targetCollection.find(existingFilter, { limit: 1 })

        if (existing.length > 0) {
          const existingId = ((existing[0] as Record<string, unknown>).$id as string).split('/').pop()
          if (existingId) {
            await targetCollection.update(existingId, {
              $set: {
                requestCount: aggregate.requestCount,
                successCount: aggregate.successCount,
                errorCount: aggregate.errorCount,
                cachedCount: aggregate.cachedCount,
                generateCount: aggregate.generateCount,
                streamCount: aggregate.streamCount,
                totalPromptTokens: aggregate.totalPromptTokens,
                totalCompletionTokens: aggregate.totalCompletionTokens,
                totalTokens: aggregate.totalTokens,
                avgTokensPerRequest: aggregate.avgTokensPerRequest,
                totalLatencyMs: aggregate.totalLatencyMs,
                avgLatencyMs: aggregate.avgLatencyMs,
                minLatencyMs: aggregate.minLatencyMs,
                maxLatencyMs: aggregate.maxLatencyMs,
                p50LatencyMs: aggregate.p50LatencyMs,
                p90LatencyMs: aggregate.p90LatencyMs,
                p95LatencyMs: aggregate.p95LatencyMs,
                p99LatencyMs: aggregate.p99LatencyMs,
                estimatedInputCost: aggregate.estimatedInputCost,
                estimatedOutputCost: aggregate.estimatedOutputCost,
                estimatedTotalCost: aggregate.estimatedTotalCost,
                updatedAt: aggregate.updatedAt,
                version: aggregate.version,
              },
            })
          }
        } else {
          // Build create payload with optional tenantId
          const createPayload: Record<string, unknown> = {
            $type: 'AIUsage',
            name: `${aggregate.modelId}/${aggregate.providerId} (${aggregate.dateKey})`,
            modelId: aggregate.modelId,
            providerId: aggregate.providerId,
            dateKey: aggregate.dateKey,
            granularity: aggregate.granularity,
            requestCount: aggregate.requestCount,
            successCount: aggregate.successCount,
            errorCount: aggregate.errorCount,
            cachedCount: aggregate.cachedCount,
            generateCount: aggregate.generateCount,
            streamCount: aggregate.streamCount,
            totalPromptTokens: aggregate.totalPromptTokens,
            totalCompletionTokens: aggregate.totalCompletionTokens,
            totalTokens: aggregate.totalTokens,
            avgTokensPerRequest: aggregate.avgTokensPerRequest,
            totalLatencyMs: aggregate.totalLatencyMs,
            avgLatencyMs: aggregate.avgLatencyMs,
            minLatencyMs: aggregate.minLatencyMs,
            maxLatencyMs: aggregate.maxLatencyMs,
            p50LatencyMs: aggregate.p50LatencyMs,
            p90LatencyMs: aggregate.p90LatencyMs,
            p95LatencyMs: aggregate.p95LatencyMs,
            p99LatencyMs: aggregate.p99LatencyMs,
            estimatedInputCost: aggregate.estimatedInputCost,
            estimatedOutputCost: aggregate.estimatedOutputCost,
            estimatedTotalCost: aggregate.estimatedTotalCost,
            createdAt: aggregate.createdAt,
            updatedAt: aggregate.updatedAt,
            version: aggregate.version,
          }
          // Add tenantId if configured
          if (aggregate.tenantId) {
            createPayload.tenantId = aggregate.tenantId
          }
          await targetCollection.create(createPayload)
        }
        aggregatesUpdated++
      }

      this.lastRefreshTime = now

      return {
        success: true,
        recordsProcessed,
        aggregatesUpdated,
        durationMs: Date.now() - startTime,
      }
    } catch (error) {
      return {
        success: false,
        recordsProcessed,
        aggregatesUpdated,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Query AI usage aggregates
   *
   * @param options - Query options
   * @returns Array of usage aggregates
   */
  async getUsage(options: AIUsageQueryOptions = {}): Promise<AIUsageAggregate[]> {
    const targetCollection = this.db.collection(this.config.targetCollection)

    // Build filter
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

    if (options.modelId) {
      filter.modelId = options.modelId
    }

    if (options.providerId) {
      filter.providerId = options.providerId
    }

    if (options.granularity) {
      filter.granularity = options.granularity
    }

    if (options.from || options.to) {
      const dateKeyFilter: Record<string, string> = {}
      if (options.from) {
        dateKeyFilter.$gte = generateDateKey(options.from, options.granularity ?? this.config.granularity)
      }
      if (options.to) {
        dateKeyFilter.$lte = generateDateKey(options.to, options.granularity ?? this.config.granularity)
      }
      filter.dateKey = dateKeyFilter
    }

    // Build sort
    const sortField = options.sort?.replace('-', '') ?? 'dateKey'
    const sortOrder = options.sort?.startsWith('-') ? -1 : 1

    const results = await targetCollection.find(filter, {
      limit: options.limit ?? 100,
      sort: { [sortField]: sortOrder },
    })

    return results as unknown as AIUsageAggregate[]
  }

  /**
   * Get usage summary across all models/providers
   *
   * @param options - Query options for time range
   * @returns Usage summary
   */
  async getSummary(options: { from?: Date; to?: Date } = {}): Promise<AIUsageSummary> {
    const aggregates = await this.getUsage({
      from: options.from,
      to: options.to,
      limit: 10000, // Get all for summary
    })

    const summary: AIUsageSummary = {
      totalRequests: 0,
      totalSuccessful: 0,
      totalErrors: 0,
      errorRate: 0,
      totalTokens: 0,
      totalPromptTokens: 0,
      totalCompletionTokens: 0,
      avgLatencyMs: 0,
      estimatedTotalCost: 0,
      byModel: {},
      byProvider: {},
      timeRange: {
        from: options.from ?? new Date(0),
        to: options.to ?? new Date(),
      },
    }

    let totalLatencyMs = 0

    for (const agg of aggregates) {
      summary.totalRequests += agg.requestCount
      summary.totalSuccessful += agg.successCount
      summary.totalErrors += agg.errorCount
      summary.totalTokens += agg.totalTokens
      summary.totalPromptTokens += agg.totalPromptTokens
      summary.totalCompletionTokens += agg.totalCompletionTokens
      summary.estimatedTotalCost += agg.estimatedTotalCost
      totalLatencyMs += agg.totalLatencyMs

      // By model
      if (!summary.byModel[agg.modelId]) {
        summary.byModel[agg.modelId] = {
          requestCount: 0,
          totalTokens: 0,
          estimatedCost: 0,
          avgLatencyMs: 0,
        }
      }
      summary.byModel[agg.modelId].requestCount += agg.requestCount
      summary.byModel[agg.modelId].totalTokens += agg.totalTokens
      summary.byModel[agg.modelId].estimatedCost += agg.estimatedTotalCost

      // By provider
      if (!summary.byProvider[agg.providerId]) {
        summary.byProvider[agg.providerId] = {
          requestCount: 0,
          totalTokens: 0,
          estimatedCost: 0,
          avgLatencyMs: 0,
        }
      }
      summary.byProvider[agg.providerId].requestCount += agg.requestCount
      summary.byProvider[agg.providerId].totalTokens += agg.totalTokens
      summary.byProvider[agg.providerId].estimatedCost += agg.estimatedTotalCost
    }

    // Calculate averages
    if (summary.totalRequests > 0) {
      summary.avgLatencyMs = totalLatencyMs / summary.totalRequests
      summary.errorRate = summary.totalErrors / summary.totalRequests
    }

    // Calculate per-model averages
    for (const modelId of Object.keys(summary.byModel)) {
      const modelAggs = aggregates.filter(a => a.modelId === modelId)
      const modelTotalLatency = modelAggs.reduce((sum, a) => sum + a.totalLatencyMs, 0)
      if (summary.byModel[modelId].requestCount > 0) {
        summary.byModel[modelId].avgLatencyMs = modelTotalLatency / summary.byModel[modelId].requestCount
      }
    }

    // Calculate per-provider averages
    for (const providerId of Object.keys(summary.byProvider)) {
      const providerAggs = aggregates.filter(a => a.providerId === providerId)
      const providerTotalLatency = providerAggs.reduce((sum, a) => sum + a.totalLatencyMs, 0)
      if (summary.byProvider[providerId].requestCount > 0) {
        summary.byProvider[providerId].avgLatencyMs = providerTotalLatency / summary.byProvider[providerId].requestCount
      }
    }

    return summary
  }

  /**
   * Get daily cost breakdown for a time period
   *
   * @param options - Query options
   * @returns Array of daily costs
   */
  async getDailyCosts(options: { from?: Date; to?: Date; modelId?: string; providerId?: string } = {}): Promise<Array<{
    dateKey: string
    totalCost: number
    inputCost: number
    outputCost: number
    totalTokens: number
    requestCount: number
  }>> {
    const aggregates = await this.getUsage({
      ...options,
      granularity: 'day',
      sort: 'dateKey',
      limit: 365, // Up to a year
    })

    // Group by date key (in case of multiple models)
    const byDate = new Map<string, {
      dateKey: string
      totalCost: number
      inputCost: number
      outputCost: number
      totalTokens: number
      requestCount: number
    }>()

    for (const agg of aggregates) {
      const existing = byDate.get(agg.dateKey) ?? {
        dateKey: agg.dateKey,
        totalCost: 0,
        inputCost: 0,
        outputCost: 0,
        totalTokens: 0,
        requestCount: 0,
      }

      existing.totalCost += agg.estimatedTotalCost
      existing.inputCost += agg.estimatedInputCost
      existing.outputCost += agg.estimatedOutputCost
      existing.totalTokens += agg.totalTokens
      existing.requestCount += agg.requestCount

      byDate.set(agg.dateKey, existing)
    }

    return Array.from(byDate.values())
  }

  /**
   * Get usage summary for a specific tenant
   *
   * Provides detailed usage information for billing and quota tracking.
   *
   * @param tenantId - Tenant identifier (defaults to config tenantId)
   * @param options - Query options for time range
   * @returns Tenant usage summary
   */
  async getTenantSummary(
    tenantId?: string,
    options: { from?: Date; to?: Date } = {}
  ): Promise<TenantUsageSummary> {
    const effectiveTenantId = tenantId ?? this.config.tenantId
    if (!effectiveTenantId) {
      throw new Error('Tenant ID is required for getTenantSummary')
    }

    const aggregates = await this.getUsage({
      tenantId: effectiveTenantId,
      from: options.from,
      to: options.to,
      limit: 10000,
    })

    // Calculate totals
    let totalRequests = 0
    let successCount = 0
    let errorCount = 0
    let totalTokens = 0
    let promptTokens = 0
    let completionTokens = 0
    let totalCost = 0
    let totalLatency = 0
    let cacheHits = 0

    const costByModel: Record<string, number> = {}
    const costByProvider: Record<string, number> = {}

    for (const agg of aggregates) {
      totalRequests += agg.requestCount
      successCount += agg.successCount
      errorCount += agg.errorCount
      totalTokens += agg.totalTokens
      promptTokens += agg.totalPromptTokens
      completionTokens += agg.totalCompletionTokens
      totalCost += agg.estimatedTotalCost
      totalLatency += agg.totalLatencyMs
      cacheHits += agg.cachedCount

      costByModel[agg.modelId] = (costByModel[agg.modelId] ?? 0) + agg.estimatedTotalCost
      costByProvider[agg.providerId] = (costByProvider[agg.providerId] ?? 0) + agg.estimatedTotalCost
    }

    return {
      tenantId: effectiveTenantId,
      period: {
        from: options.from ?? new Date(0),
        to: options.to ?? new Date(),
      },
      totalRequests,
      successCount,
      errorCount,
      errorRate: totalRequests > 0 ? errorCount / totalRequests : 0,
      tokens: {
        total: totalTokens,
        prompt: promptTokens,
        completion: completionTokens,
      },
      cost: {
        total: totalCost,
        byModel: costByModel,
        byProvider: costByProvider,
      },
      avgLatencyMs: totalRequests > 0 ? totalLatency / totalRequests : 0,
      cacheHitRatio: totalRequests > 0 ? cacheHits / totalRequests : 0,
    }
  }

  /**
   * Get cross-tenant aggregates for platform analytics
   *
   * Returns usage aggregates grouped by tenant for platform operators.
   * Requires allowCrossTenantQueries: true in config.
   *
   * @param options - Query options
   * @returns Array of cross-tenant aggregates
   */
  async getCrossTenantAggregates(options: {
    from?: Date
    to?: Date
    granularity?: 'day' | 'week' | 'month'
  } = {}): Promise<CrossTenantAggregate[]> {
    if (!this.config.allowCrossTenantQueries) {
      throw new Error('Cross-tenant queries are not enabled. Set allowCrossTenantQueries: true in config.')
    }

    const aggregates = await this.getUsage({
      allTenants: true,
      from: options.from,
      to: options.to,
      granularity: options.granularity,
      limit: 10000,
    })

    // Group by tenant and date
    const byTenantDate = new Map<string, CrossTenantAggregate>()

    for (const agg of aggregates) {
      const tenantId = agg.tenantId ?? 'unknown'
      const key = `${tenantId}:${agg.dateKey}`

      const existing = byTenantDate.get(key)
      if (existing) {
        existing.requestCount += agg.requestCount
        existing.totalTokens += agg.totalTokens
        existing.totalPromptTokens += agg.totalPromptTokens
        existing.totalCompletionTokens += agg.totalCompletionTokens
        existing.estimatedTotalCost += agg.estimatedTotalCost
        existing.errorCount += agg.errorCount
        existing.cacheHits += agg.cachedCount
        // Recalculate average latency
        const totalLatency = existing.avgLatencyMs * (existing.requestCount - agg.requestCount) + agg.totalLatencyMs
        existing.avgLatencyMs = totalLatency / existing.requestCount
      } else {
        byTenantDate.set(key, {
          tenantId,
          dateKey: agg.dateKey,
          requestCount: agg.requestCount,
          totalTokens: agg.totalTokens,
          totalPromptTokens: agg.totalPromptTokens,
          totalCompletionTokens: agg.totalCompletionTokens,
          estimatedTotalCost: agg.estimatedTotalCost,
          avgLatencyMs: agg.avgLatencyMs,
          errorCount: agg.errorCount,
          cacheHits: agg.cachedCount,
        })
      }
    }

    return Array.from(byTenantDate.values())
  }

  /**
   * Get the current configuration
   */
  getConfig(): ResolvedAIUsageMVConfig {
    return { ...this.config }
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Normalize an aggregate loaded from the database
 *
 * Handles edge cases like:
 * - minLatencyMs being null/undefined (from Infinity serialization)
 * - Missing numeric fields defaulting to 0
 * - Ensuring all expected fields exist
 * - Initializing latency samples for reservoir sampling
 */
function normalizeAggregateFromDB(aggregate: AIUsageAggregate): WorkingAggregate {
  // Handle minLatencyMs - Infinity doesn't survive JSON serialization
  // It becomes null, and Math.min(null, x) returns 0 which is incorrect
  if (aggregate.minLatencyMs === null || aggregate.minLatencyMs === undefined || !Number.isFinite(aggregate.minLatencyMs)) {
    aggregate.minLatencyMs = aggregate.requestCount > 0 ? aggregate.minLatencyMs ?? Infinity : Infinity
    // If we have requests but minLatencyMs is invalid, we can't recover the true min
    // So we set it to Infinity to ensure new values will be properly tracked
    if (!Number.isFinite(aggregate.minLatencyMs)) {
      aggregate.minLatencyMs = Infinity
    }
  }

  // Ensure numeric fields have valid values
  aggregate.requestCount = aggregate.requestCount ?? 0
  aggregate.successCount = aggregate.successCount ?? 0
  aggregate.errorCount = aggregate.errorCount ?? 0
  aggregate.cachedCount = aggregate.cachedCount ?? 0
  aggregate.generateCount = aggregate.generateCount ?? 0
  aggregate.streamCount = aggregate.streamCount ?? 0
  aggregate.totalPromptTokens = aggregate.totalPromptTokens ?? 0
  aggregate.totalCompletionTokens = aggregate.totalCompletionTokens ?? 0
  aggregate.totalTokens = aggregate.totalTokens ?? 0
  aggregate.avgTokensPerRequest = aggregate.avgTokensPerRequest ?? 0
  aggregate.totalLatencyMs = aggregate.totalLatencyMs ?? 0
  aggregate.avgLatencyMs = aggregate.avgLatencyMs ?? 0
  aggregate.maxLatencyMs = aggregate.maxLatencyMs ?? 0
  aggregate.estimatedInputCost = aggregate.estimatedInputCost ?? 0
  aggregate.estimatedOutputCost = aggregate.estimatedOutputCost ?? 0
  aggregate.estimatedTotalCost = aggregate.estimatedTotalCost ?? 0
  aggregate.version = aggregate.version ?? 0

  // Convert to working aggregate with latency samples
  // Note: We lose historical samples when loading from DB, so percentiles
  // will be recalculated from new samples only. For more accurate historical
  // percentiles, consider storing a T-digest or DDSketch in the DB.
  return {
    ...aggregate,
    _latencySamples: [],
    _sampleCount: 0,
  }
}

/**
 * Create an empty working aggregate
 */
function createEmptyAggregate(
  id: string,
  modelId: string,
  providerId: string,
  dateKey: string,
  granularity: TimeGranularity,
  tenantId?: string
): WorkingAggregate {
  const now = new Date()
  return {
    $id: id,
    $type: 'AIUsage',
    name: `${modelId}/${providerId} (${dateKey})`,
    modelId,
    providerId,
    dateKey,
    granularity,
    requestCount: 0,
    successCount: 0,
    errorCount: 0,
    cachedCount: 0,
    generateCount: 0,
    streamCount: 0,
    totalPromptTokens: 0,
    totalCompletionTokens: 0,
    totalTokens: 0,
    avgTokensPerRequest: 0,
    totalLatencyMs: 0,
    avgLatencyMs: 0,
    minLatencyMs: Infinity,
    maxLatencyMs: 0,
    estimatedInputCost: 0,
    estimatedOutputCost: 0,
    estimatedTotalCost: 0,
    createdAt: now,
    updatedAt: now,
    version: 0,
    tenantId,
    _latencySamples: [],
    _sampleCount: 0,
  }
}

/**
 * Pricing lookup function type
 */
type PricingLookup = (modelId: string, providerId: string) => ModelPricing | undefined

/**
 * Update an aggregate from a log entry
 */
function updateAggregateFromLog(
  aggregate: WorkingAggregate,
  log: Record<string, unknown>,
  getPricing: PricingLookup
): void {
  // Update request counts
  aggregate.requestCount++

  const hasError = log.error !== undefined && log.error !== null
  if (hasError) {
    aggregate.errorCount++
  } else {
    aggregate.successCount++
  }

  if (log.cached === true) {
    aggregate.cachedCount++
  }

  if (log.requestType === 'generate') {
    aggregate.generateCount++
  } else if (log.requestType === 'stream') {
    aggregate.streamCount++
  }

  // Update token usage
  const usage = log.usage as { promptTokens?: number; completionTokens?: number; totalTokens?: number } | undefined
  if (usage) {
    const promptTokens = usage.promptTokens ?? 0
    const completionTokens = usage.completionTokens ?? 0
    const totalTokens = usage.totalTokens ?? (promptTokens + completionTokens)

    aggregate.totalPromptTokens += promptTokens
    aggregate.totalCompletionTokens += completionTokens
    aggregate.totalTokens += totalTokens

    // Calculate cost using pricing lookup (supports both static and dynamic pricing)
    const pricing = getPricing(aggregate.modelId, aggregate.providerId)
    const cost = calculateCost(promptTokens, completionTokens, pricing)

    aggregate.estimatedInputCost += cost.inputCost
    aggregate.estimatedOutputCost += cost.outputCost
    aggregate.estimatedTotalCost += cost.totalCost
  }

  // Update latency stats
  const latencyMs = typeof log.latencyMs === 'number' ? log.latencyMs : 0
  aggregate.totalLatencyMs += latencyMs
  aggregate.minLatencyMs = Math.min(aggregate.minLatencyMs, latencyMs)
  aggregate.maxLatencyMs = Math.max(aggregate.maxLatencyMs, latencyMs)

  // Reservoir sampling for latency percentiles
  addLatencySample(aggregate, latencyMs)

  // Update averages
  if (aggregate.requestCount > 0) {
    aggregate.avgLatencyMs = aggregate.totalLatencyMs / aggregate.requestCount
    aggregate.avgTokensPerRequest = aggregate.totalTokens / aggregate.requestCount
  }
}

/**
 * Add a latency sample using reservoir sampling (Algorithm R)
 *
 * This maintains a representative sample of latencies with O(1) space complexity.
 * Each element has an equal probability of being in the reservoir.
 */
function addLatencySample(aggregate: WorkingAggregate, latencyMs: number): void {
  aggregate._sampleCount++

  if (aggregate._latencySamples.length < LATENCY_RESERVOIR_SIZE) {
    // Reservoir not full - add directly
    aggregate._latencySamples.push(latencyMs)
  } else {
    // Reservoir full - randomly replace an element
    // Each new element has k/n probability of being included (where k = reservoir size, n = total count)
    const randomIndex = Math.floor(Math.random() * aggregate._sampleCount)
    if (randomIndex < LATENCY_RESERVOIR_SIZE) {
      aggregate._latencySamples[randomIndex] = latencyMs
    }
  }
}

/**
 * Calculate percentiles from latency samples
 *
 * Uses the "nearest rank" method for percentile calculation.
 */
function calculatePercentiles(samples: number[]): {
  p50: number | undefined
  p90: number | undefined
  p95: number | undefined
  p99: number | undefined
} {
  if (samples.length === 0) {
    return { p50: undefined, p90: undefined, p95: undefined, p99: undefined }
  }

  // Sort samples in ascending order
  const sorted = [...samples].sort((a, b) => a - b)
  const n = sorted.length

  /**
   * Calculate percentile using the "nearest rank" method
   * Percentile p means p% of values are at or below this value
   */
  const getPercentile = (p: number): number => {
    if (n === 1) return sorted[0]
    // Use ceiling to get the rank (1-indexed), then convert to 0-indexed
    const rank = Math.ceil((p / 100) * n)
    // Clamp to valid array bounds
    const index = Math.min(Math.max(rank - 1, 0), n - 1)
    return sorted[index]
  }

  return {
    p50: getPercentile(50),
    p90: getPercentile(90),
    p95: getPercentile(95),
    p99: getPercentile(99),
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create an AIUsageMV instance
 *
 * @param db - ParqueDB instance
 * @param config - Configuration options
 * @returns AIUsageMV instance
 *
 * @example
 * ```typescript
 * const usageMV = createAIUsageMV(db)
 * await usageMV.refresh()
 * const summary = await usageMV.getSummary()
 * ```
 */
export function createAIUsageMV(db: ParqueDB, config: AIUsageMVConfig = {}): AIUsageMV {
  return new AIUsageMV(db, config)
}

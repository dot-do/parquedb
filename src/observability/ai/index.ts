/**
 * AI Observability Module for ParqueDB
 *
 * Provides materialized views and utilities for tracking AI API usage,
 * costs, and performance metrics.
 *
 * @example
 * ```typescript
 * import { AIUsageMV, createAIUsageMV } from 'parquedb/observability/ai'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 *
 * // Create AI usage materialized view
 * const usageMV = createAIUsageMV(db, {
 *   granularity: 'day',
 *   customPricing: [
 *     // Override pricing for custom/fine-tuned models
 *     { modelId: 'ft:gpt-3.5-turbo', providerId: 'openai', inputPricePerMillion: 3.00, outputPricePerMillion: 6.00 }
 *   ]
 * })
 *
 * // Refresh to process new logs
 * await usageMV.refresh()
 *
 * // Get usage summary
 * const summary = await usageMV.getSummary({
 *   from: new Date('2026-02-01'),
 *   to: new Date('2026-02-03')
 * })
 *
 * console.log(`Total cost: $${summary.estimatedTotalCost.toFixed(2)}`)
 * console.log(`Total tokens: ${summary.totalTokens.toLocaleString()}`)
 * ```
 *
 * @module observability/ai
 */

// Types
export type {
  // Model Pricing
  ModelPricing,

  // Token Usage
  TokenUsage,
  AIRequest,

  // Aggregation Types
  TimeGranularity,
  AIUsageAggregate,
  AIUsageSummary,

  // Configuration
  AIUsageMVConfig,
  ResolvedAIUsageMVConfig,

  // Query/Filter
  AIUsageQueryOptions,

  // Results
  RefreshResult,
} from './types'

// Constants
export { DEFAULT_MODEL_PRICING } from './types'

// AIUsageMV
export { AIUsageMV, createAIUsageMV } from './AIUsageMV'

// AIRequestsMV
export {
  AIRequestsMV,
  createAIRequestsMV,
  generateRequestId,
  type AIRequestType,
  type AIProvider,
  type AIRequestStatus,
  type AIRequestRecord,
  type RecordAIRequestInput,
  type AIRequestsQueryOptions,
  type AIRequestsStats,
  type AIRequestsMVConfig,
  type ResolvedAIRequestsMVConfig,
  type CleanupResult as AIRequestsCleanupResult,
} from './AIRequestsMV'

// GeneratedContentMV
export {
  GeneratedContentMV,
  createGeneratedContentMV,
  generateContentId,
  hashContent,
  type GeneratedContentType,
  type ContentClassification,
  type FinishReason,
  type GeneratedContentRecord,
  type RecordContentInput,
  type ContentQueryOptions,
  type ContentStats,
  type GeneratedContentMVConfig,
  type ResolvedContentMVConfig,
  type CleanupResult as ContentCleanupResult,
} from './GeneratedContentMV'

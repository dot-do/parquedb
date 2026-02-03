/**
 * AI Observability Module for ParqueDB
 *
 * Provides materialized views and utilities for tracking AI API usage,
 * costs, and performance metrics.
 *
 * @example Basic Usage with AIUsageMV
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
 * @example Auto-updating Model Pricing
 * ```typescript
 * import { createModelPricingService, createAIUsageMV } from 'parquedb/observability/ai'
 * import { DB } from 'parquedb'
 *
 * // Create pricing service with auto-refresh
 * const pricingService = createModelPricingService({
 *   refreshIntervalMs: 24 * 60 * 60 * 1000, // Daily refresh
 *   enterpriseOverrides: [
 *     // Custom pricing for enterprise agreements
 *     { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 }
 *   ]
 * })
 *
 * // Start auto-refresh (fetches from pricing API)
 * await pricingService.startAutoRefresh()
 *
 * // Use with AIUsageMV for accurate cost tracking
 * const usageMV = createAIUsageMV(DB(), {
 *   pricingService: pricingService // Dynamic pricing instead of static
 * })
 *
 * // Get current pricing for a model
 * const gpt4Pricing = pricingService.getPricing('gpt-4', 'openai')
 * console.log(`GPT-4 input: $${gpt4Pricing?.inputPricePerMillion}/1M tokens`)
 *
 * // Check pricing status
 * const status = pricingService.getStatus()
 * console.log(`Cache version: ${status.cacheVersion}, entries: ${status.entryCount}`)
 *
 * // Stop auto-refresh when done
 * pricingService.stopAutoRefresh()
 * ```
 *
 * @module observability/ai
 */

// Types
export type {
  // Model Pricing
  ModelPricing,
  PricingProvider,

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

  // Multi-Tenancy
  MultiTenantConfig,
  MultiTenantQueryOptions,
  CrossTenantAggregate,
  TenantUsageSummary,
  TenantQuota,
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
  hashContent as hashRequestContent,
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
  type ContentSamplingConfig,
  type ResolvedContentSamplingConfig,
  type ContentRedactor,
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

// Model Pricing Service - Auto-updating pricing with API refresh, caching, and enterprise overrides
export {
  ModelPricingService,
  createModelPricingService,
  getDefaultPricingService,
  resetDefaultPricingService,
  type PricingSource,
  type PricingWithMetadata,
  type PricingCache,
  type PricingFetchResult,
  type ModelPricingServiceConfig,
  type ResolvedPricingServiceConfig,
  type PricingServiceStatus,
} from './pricing'

// Anomaly Detection
export {
  AnomalyDetector,
  createAnomalyDetector,
  createAnomalyDetectorWithWebhook,
  createObservationFromMetrics,
  DEFAULT_ANOMALY_THRESHOLDS,
  type AnomalySeverity,
  type AnomalyType,
  type AnomalyEvent,
  type WindowStats,
  type AnomalyObservation,
  type AnomalyThresholds,
  type AnomalyDetectorConfig,
  type ResolvedAnomalyDetectorConfig,
  type AnomalyDetectorStats,
} from './anomaly-detection'

// Rate Limit Metrics - Token/cost rate limiting awareness for AI workloads
export {
  RateLimitMetrics,
  createRateLimitMetrics,
  createRateLimitMetricsWithWebhook,
  DEFAULT_RATE_LIMIT_THRESHOLDS,
  type RateLimitAlertSeverity,
  type RateLimitMetricType,
  type ThresholdConfig,
  type RateLimitThresholds,
  type RateLimitAlert,
  type RateLimitObservation,
  type RateSnapshot,
  type AggregatedRateSnapshot,
  type RateLimitStats,
  type RateLimitMetricsConfig,
  type ResolvedRateLimitMetricsConfig,
} from './rate-limit-metrics'

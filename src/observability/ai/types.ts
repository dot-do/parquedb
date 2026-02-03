/**
 * AI Observability Types for ParqueDB
 *
 * Type definitions for AI usage tracking, cost estimation, and analytics.
 * These types support the AIUsageMV and other AI observability materialized views.
 *
 * @module observability/ai/types
 */

// =============================================================================
// Model Pricing Types
// =============================================================================

/**
 * Pricing information for an AI model
 *
 * Prices are specified per 1 million tokens.
 */
export interface ModelPricing {
  /** Model identifier (e.g., 'gpt-4', 'claude-3-opus') */
  modelId: string
  /** Provider identifier (e.g., 'openai', 'anthropic') */
  providerId: string
  /** Price per 1M input/prompt tokens in USD */
  inputPricePerMillion: number
  /** Price per 1M output/completion tokens in USD */
  outputPricePerMillion: number
  /** Optional display name for the model */
  displayName?: string | undefined
  /** Whether this model supports streaming */
  supportsStreaming?: boolean | undefined
  /** Context window size in tokens */
  contextWindow?: number | undefined
  /** When this pricing was last updated */
  updatedAt?: Date | undefined
}

/**
 * Default pricing for common AI models (as of early 2026)
 *
 * NOTE: Prices change frequently. Override with custom pricing for accuracy.
 */
export const DEFAULT_MODEL_PRICING: ModelPricing[] = [
  // OpenAI Models
  { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 30.00, outputPricePerMillion: 60.00, contextWindow: 8192 },
  { modelId: 'gpt-4-turbo', providerId: 'openai', inputPricePerMillion: 10.00, outputPricePerMillion: 30.00, contextWindow: 128000 },
  { modelId: 'gpt-4-turbo-preview', providerId: 'openai', inputPricePerMillion: 10.00, outputPricePerMillion: 30.00, contextWindow: 128000 },
  { modelId: 'gpt-4o', providerId: 'openai', inputPricePerMillion: 2.50, outputPricePerMillion: 10.00, contextWindow: 128000 },
  { modelId: 'gpt-4o-mini', providerId: 'openai', inputPricePerMillion: 0.15, outputPricePerMillion: 0.60, contextWindow: 128000 },
  { modelId: 'gpt-3.5-turbo', providerId: 'openai', inputPricePerMillion: 0.50, outputPricePerMillion: 1.50, contextWindow: 16385 },
  { modelId: 'o1', providerId: 'openai', inputPricePerMillion: 15.00, outputPricePerMillion: 60.00, contextWindow: 200000 },
  { modelId: 'o1-mini', providerId: 'openai', inputPricePerMillion: 3.00, outputPricePerMillion: 12.00, contextWindow: 128000 },
  { modelId: 'o1-preview', providerId: 'openai', inputPricePerMillion: 15.00, outputPricePerMillion: 60.00, contextWindow: 128000 },
  { modelId: 'o3-mini', providerId: 'openai', inputPricePerMillion: 1.10, outputPricePerMillion: 4.40, contextWindow: 200000 },

  // Anthropic Models
  { modelId: 'claude-3-opus', providerId: 'anthropic', inputPricePerMillion: 15.00, outputPricePerMillion: 75.00, contextWindow: 200000 },
  { modelId: 'claude-3-sonnet', providerId: 'anthropic', inputPricePerMillion: 3.00, outputPricePerMillion: 15.00, contextWindow: 200000 },
  { modelId: 'claude-3-haiku', providerId: 'anthropic', inputPricePerMillion: 0.25, outputPricePerMillion: 1.25, contextWindow: 200000 },
  { modelId: 'claude-3-5-sonnet', providerId: 'anthropic', inputPricePerMillion: 3.00, outputPricePerMillion: 15.00, contextWindow: 200000 },
  { modelId: 'claude-3-5-haiku', providerId: 'anthropic', inputPricePerMillion: 0.80, outputPricePerMillion: 4.00, contextWindow: 200000 },
  { modelId: 'claude-opus-4', providerId: 'anthropic', inputPricePerMillion: 15.00, outputPricePerMillion: 75.00, contextWindow: 200000 },
  { modelId: 'claude-opus-4-5', providerId: 'anthropic', inputPricePerMillion: 15.00, outputPricePerMillion: 75.00, contextWindow: 200000 },
  { modelId: 'claude-sonnet-4', providerId: 'anthropic', inputPricePerMillion: 3.00, outputPricePerMillion: 15.00, contextWindow: 200000 },

  // Google Models
  { modelId: 'gemini-1.5-pro', providerId: 'google', inputPricePerMillion: 3.50, outputPricePerMillion: 10.50, contextWindow: 2097152 },
  { modelId: 'gemini-1.5-flash', providerId: 'google', inputPricePerMillion: 0.075, outputPricePerMillion: 0.30, contextWindow: 1048576 },
  { modelId: 'gemini-2.0-flash', providerId: 'google', inputPricePerMillion: 0.10, outputPricePerMillion: 0.40, contextWindow: 1048576 },

  // Mistral Models
  { modelId: 'mistral-large', providerId: 'mistral', inputPricePerMillion: 4.00, outputPricePerMillion: 12.00, contextWindow: 128000 },
  { modelId: 'mistral-medium', providerId: 'mistral', inputPricePerMillion: 2.70, outputPricePerMillion: 8.10, contextWindow: 32000 },
  { modelId: 'mistral-small', providerId: 'mistral', inputPricePerMillion: 1.00, outputPricePerMillion: 3.00, contextWindow: 32000 },

  // Groq Models (hosted open source)
  { modelId: 'llama-3.1-70b', providerId: 'groq', inputPricePerMillion: 0.59, outputPricePerMillion: 0.79, contextWindow: 131072 },
  { modelId: 'llama-3.1-8b', providerId: 'groq', inputPricePerMillion: 0.05, outputPricePerMillion: 0.08, contextWindow: 131072 },
  { modelId: 'mixtral-8x7b', providerId: 'groq', inputPricePerMillion: 0.24, outputPricePerMillion: 0.24, contextWindow: 32768 },
]

// =============================================================================
// AI Request Types
// =============================================================================

/**
 * Token usage for a single AI request
 */
export interface TokenUsage {
  /** Number of input/prompt tokens */
  promptTokens: number
  /** Number of output/completion tokens */
  completionTokens: number
  /** Total tokens (promptTokens + completionTokens) */
  totalTokens: number
}

/**
 * AI request record (source data for materialized views)
 *
 * This matches the LogEntry from ai-sdk/types.ts
 */
export interface AIRequest {
  /** Unique request ID */
  id: string
  /** Timestamp of the request */
  timestamp: Date
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: string
  /** Request type */
  requestType: 'generate' | 'stream'
  /** Token usage */
  usage?: TokenUsage | undefined
  /** Latency in milliseconds */
  latencyMs: number
  /** Whether response was cached */
  cached: boolean
  /** Finish reason */
  finishReason?: string | undefined
  /** Error information (if failed) */
  error?: {
    name: string
    message: string
  } | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
}

// =============================================================================
// AI Usage Aggregation Types
// =============================================================================

/**
 * Time granularity for usage aggregation
 */
export type TimeGranularity = 'hour' | 'day' | 'week' | 'month'

/**
 * Aggregated AI usage for a time period
 *
 * This is the output schema for AIUsageMV.
 */
export interface AIUsageAggregate {
  /** Unique aggregate ID (e.g., 'gpt-4_openai_2026-02-03') */
  $id: string
  /** Type identifier */
  $type: 'AIUsage'
  /** Display name */
  name: string
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: string
  /** Date key for aggregation (YYYY-MM-DD for daily, YYYY-MM for monthly, etc.) */
  dateKey: string
  /** Time granularity */
  granularity: TimeGranularity

  // Request Counts
  /** Total number of requests */
  requestCount: number
  /** Number of successful requests */
  successCount: number
  /** Number of failed requests */
  errorCount: number
  /** Number of cached responses */
  cachedCount: number
  /** Number of generate requests */
  generateCount: number
  /** Number of stream requests */
  streamCount: number

  // Token Usage
  /** Total prompt/input tokens */
  totalPromptTokens: number
  /** Total completion/output tokens */
  totalCompletionTokens: number
  /** Total tokens (prompt + completion) */
  totalTokens: number
  /** Average tokens per request */
  avgTokensPerRequest: number

  // Latency Statistics
  /** Total latency in milliseconds (sum) */
  totalLatencyMs: number
  /** Average latency per request */
  avgLatencyMs: number
  /** Minimum latency observed */
  minLatencyMs: number
  /** Maximum latency observed */
  maxLatencyMs: number
  /** P50 latency estimate */
  p50LatencyMs?: number | undefined
  /** P90 latency estimate */
  p90LatencyMs?: number | undefined
  /** P95 latency estimate */
  p95LatencyMs?: number | undefined
  /** P99 latency estimate */
  p99LatencyMs?: number | undefined

  // Cost Estimates
  /** Estimated input token cost in USD */
  estimatedInputCost: number
  /** Estimated output token cost in USD */
  estimatedOutputCost: number
  /** Total estimated cost in USD */
  estimatedTotalCost: number

  // Metadata
  /** When this aggregate was first created */
  createdAt: Date
  /** When this aggregate was last updated */
  updatedAt: Date
  /** Version for optimistic concurrency */
  version: number
  /** Tenant identifier (for multi-tenant deployments) */
  tenantId?: string | undefined
}

/**
 * Summary of AI usage across all models/providers
 */
export interface AIUsageSummary {
  /** Total requests across all models */
  totalRequests: number
  /** Total successful requests */
  totalSuccessful: number
  /** Total failed requests */
  totalErrors: number
  /** Overall error rate (0-1) */
  errorRate: number
  /** Total tokens used */
  totalTokens: number
  /** Total prompt tokens */
  totalPromptTokens: number
  /** Total completion tokens */
  totalCompletionTokens: number
  /** Average latency across all requests */
  avgLatencyMs: number
  /** Total estimated cost */
  estimatedTotalCost: number
  /** Breakdown by model */
  byModel: Record<string, {
    requestCount: number
    totalTokens: number
    estimatedCost: number
    avgLatencyMs: number
  }>
  /** Breakdown by provider */
  byProvider: Record<string, {
    requestCount: number
    totalTokens: number
    estimatedCost: number
    avgLatencyMs: number
  }>
  /** Time range of the summary */
  timeRange: {
    from: Date
    to: Date
  }
}

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Interface for a pricing provider
 *
 * This allows injection of a ModelPricingService or custom pricing provider
 * for dynamic pricing updates.
 */
export interface PricingProvider {
  /**
   * Get pricing for a model
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @returns Pricing information or undefined if not found
   */
  getPricing(modelId: string, providerId: string): ModelPricing | undefined
}

/**
 * Configuration options for AIUsageMV
 *
 * Extends MultiTenantConfig for multi-tenant deployments.
 *
 * @example
 * ```typescript
 * // Basic usage
 * const mv = createAIUsageMV(db, {
 *   granularity: 'day',
 * })
 *
 * // Multi-tenant usage
 * const mv = createAIUsageMV(db, {
 *   tenantId: 'tenant-123',
 *   tenantScopedStorage: true,
 * })
 * ```
 */
export interface AIUsageMVConfig extends MultiTenantConfig {
  /** Collection name for source AI logs (default: 'ai_logs') */
  sourceCollection?: string | undefined
  /** Collection name for aggregated usage (default: 'ai_usage') */
  targetCollection?: string | undefined
  /** Time granularity for aggregation (default: 'day') */
  granularity?: TimeGranularity | undefined
  /** Custom model pricing (overrides defaults) */
  customPricing?: ModelPricing[] | undefined
  /** Whether to merge with default pricing (default: true) */
  mergeWithDefaultPricing?: boolean | undefined
  /**
   * Dynamic pricing provider (e.g., ModelPricingService)
   *
   * When provided, pricing is fetched from this service instead of
   * static customPricing/defaultPricing. This enables auto-updating pricing.
   *
   * @example
   * ```typescript
   * const pricingService = createModelPricingService()
   * await pricingService.startAutoRefresh()
   *
   * const usageMV = createAIUsageMV(db, {
   *   pricingService: pricingService
   * })
   * ```
   */
  pricingService?: PricingProvider | undefined
  /** Maximum age of logs to process in milliseconds (default: 30 days) */
  maxAgeMs?: number | undefined
  /** Batch size for processing (default: 1000) */
  batchSize?: number | undefined
  /** Whether to enable debug logging (default: false) */
  debug?: boolean | undefined
}

/**
 * Resolved configuration with defaults applied
 */
export interface ResolvedAIUsageMVConfig {
  sourceCollection: string
  targetCollection: string
  granularity: TimeGranularity
  /** Static pricing map (used when pricingService is not provided) */
  pricing: Map<string, ModelPricing>
  /** Dynamic pricing provider (takes precedence over static pricing) */
  pricingService?: PricingProvider | undefined
  maxAgeMs: number
  batchSize: number
  debug: boolean
  /** Tenant identifier */
  tenantId?: string | undefined
  /** Whether to use tenant-scoped storage paths */
  tenantScopedStorage: boolean
  /** Whether cross-tenant queries are allowed */
  allowCrossTenantQueries: boolean
}

// =============================================================================
// Query/Filter Types
// =============================================================================

/**
 * Filter options for querying AI usage
 */
export interface AIUsageQueryOptions extends MultiTenantQueryOptions {
  /** Filter by model ID */
  modelId?: string | undefined
  /** Filter by provider ID */
  providerId?: string | undefined
  /** Start date (inclusive) */
  from?: Date | undefined
  /** End date (inclusive) */
  to?: Date | undefined
  /** Time granularity */
  granularity?: TimeGranularity | undefined
  /** Maximum results to return */
  limit?: number | undefined
  /** Sort order */
  sort?: 'dateKey' | '-dateKey' | 'estimatedTotalCost' | '-estimatedTotalCost' | undefined
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * Result of a refresh operation
 */
export interface RefreshResult {
  /** Whether the refresh was successful */
  success: boolean
  /** Number of source records processed */
  recordsProcessed: number
  /** Number of aggregates created/updated */
  aggregatesUpdated: number
  /** Duration of the refresh in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string | undefined
}

// =============================================================================
// Multi-Tenancy Types
// =============================================================================

/**
 * Configuration for multi-tenant observability MVs
 *
 * Provides tenant isolation, scoped storage, and cross-tenant analytics
 * for SaaS deployments where multiple customers share infrastructure.
 *
 * @example
 * ```typescript
 * // Basic tenant isolation
 * const mv = createAIUsageMV(db, {
 *   tenantId: 'tenant-123',
 * })
 *
 * // Tenant-scoped storage (separate collections per tenant)
 * const mv = createAIUsageMV(db, {
 *   tenantId: 'tenant-123',
 *   tenantScopedStorage: true, // Collection becomes tenant_tenant-123/ai_usage
 * })
 *
 * // Platform operator with cross-tenant access
 * const mv = createAIUsageMV(db, {
 *   allowCrossTenantQueries: true,
 * })
 * const allTenantStats = await mv.getUsage({ allTenants: true })
 * ```
 */
export interface MultiTenantConfig {
  /**
   * Tenant identifier for data isolation
   *
   * When set, all records are tagged with this tenant ID and queries
   * are automatically filtered to this tenant's data only.
   */
  tenantId?: string | undefined

  /**
   * Enable tenant-scoped storage paths
   *
   * When true, collections are prefixed with `tenant_{tenantId}/`
   * providing physical isolation of data per tenant.
   *
   * @default false
   *
   * @example
   * // With tenantId='acme', collection='ai_usage'
   * // Actual collection path: 'tenant_acme/ai_usage'
   */
  tenantScopedStorage?: boolean | undefined

  /**
   * Allow queries across multiple tenants
   *
   * When true, queries can use `allTenants: true` option to retrieve
   * data from all tenants. This is intended for platform operators
   * who need cross-tenant analytics.
   *
   * WARNING: Enable only for platform-level admin interfaces.
   *
   * @default false
   */
  allowCrossTenantQueries?: boolean | undefined
}

/**
 * Query options that include multi-tenant filtering
 */
export interface MultiTenantQueryOptions {
  /**
   * Filter by tenant ID (overrides config tenantId for this query)
   */
  tenantId?: string | undefined

  /**
   * Query all tenants (requires allowCrossTenantQueries: true)
   *
   * When true, returns data from all tenants. The tenantId field
   * will be included in results for filtering/grouping.
   */
  allTenants?: boolean | undefined
}

/**
 * Cross-tenant aggregate statistics
 *
 * Used by platform operators to analyze usage across all tenants.
 */
export interface CrossTenantAggregate {
  /** Tenant identifier */
  tenantId: string
  /** Date key for the aggregate period */
  dateKey: string
  /** Total number of requests */
  requestCount: number
  /** Total tokens used */
  totalTokens: number
  /** Total prompt tokens */
  totalPromptTokens: number
  /** Total completion tokens */
  totalCompletionTokens: number
  /** Estimated total cost in USD */
  estimatedTotalCost: number
  /** Average latency in milliseconds */
  avgLatencyMs: number
  /** Error count */
  errorCount: number
  /** Cache hit count */
  cacheHits: number
}

/**
 * Per-tenant usage summary
 *
 * Provides a summary view of a tenant's AI usage for billing,
 * quota management, and analytics dashboards.
 */
export interface TenantUsageSummary {
  /** Tenant identifier */
  tenantId: string
  /** Summary time period */
  period: {
    from: Date
    to: Date
  }
  /** Total requests in period */
  totalRequests: number
  /** Successful requests */
  successCount: number
  /** Failed requests */
  errorCount: number
  /** Error rate (0-1) */
  errorRate: number
  /** Token usage */
  tokens: {
    total: number
    prompt: number
    completion: number
  }
  /** Cost in USD */
  cost: {
    total: number
    byModel: Record<string, number>
    byProvider: Record<string, number>
  }
  /** Average latency in milliseconds */
  avgLatencyMs: number
  /** Cache hit ratio (0-1) */
  cacheHitRatio: number
  /** Quota information (if quotas are configured) */
  quota?: TenantQuota | undefined
}

/**
 * Tenant quota configuration and usage
 *
 * Used for rate limiting and cost controls.
 */
export interface TenantQuota {
  /** Maximum requests per day */
  maxRequestsPerDay?: number | undefined
  /** Current requests today */
  currentRequestsToday?: number | undefined
  /** Maximum tokens per day */
  maxTokensPerDay?: number | undefined
  /** Current tokens today */
  currentTokensToday?: number | undefined
  /** Maximum cost per day in USD */
  maxCostPerDay?: number | undefined
  /** Current cost today */
  currentCostToday?: number | undefined
  /** Maximum cost per month in USD */
  maxCostPerMonth?: number | undefined
  /** Current cost this month */
  currentCostThisMonth?: number | undefined
  /** Whether tenant has exceeded any quota */
  isQuotaExceeded?: boolean | undefined
  /** Which quota was exceeded */
  exceededQuota?: 'requests' | 'tokens' | 'dailyCost' | 'monthlyCost' | undefined
}

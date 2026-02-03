/**
 * Vercel AI SDK Integration for ParqueDB
 *
 * Provides middleware for the Vercel AI SDK that enables:
 * - Response caching: Store LLM responses to reduce API calls and costs
 * - Request logging: Log all AI requests for debugging, analytics, and compliance
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { createParqueDBMiddleware } from 'parquedb/integrations'
 * import { wrapLanguageModel } from 'ai'
 * import { openai } from '@ai-sdk/openai'
 *
 * const db = DB()
 *
 * // Create middleware with caching and logging
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   cache: {
 *     enabled: true,
 *     ttlSeconds: 3600,
 *   },
 *   logging: {
 *     enabled: true,
 *     level: 'standard',
 *   },
 * })
 *
 * // Wrap your model with the middleware
 * const model = wrapLanguageModel({
 *   model: openai('gpt-4'),
 *   middleware,
 * })
 *
 * // Use as normal - responses are cached and logged automatically
 * import { generateText } from 'ai'
 * const result = await generateText({ model, prompt: 'Hello!' })
 * ```
 *
 * @packageDocumentation
 */

// Main middleware factory
export { createParqueDBMiddleware } from './middleware'

// Utility functions
export {
  hashParams,
  isExpired,
  queryCacheEntries,
  queryLogEntries,
  clearExpiredCache,
  getCacheStats,
} from './middleware'

// Types
export type {
  // Configuration types
  ParqueDBMiddlewareOptions,
  CacheConfig,
  LoggingConfig,

  // Storage types
  CacheEntry,
  LogEntry,

  // AI SDK compatible types
  LanguageModelV3Middleware,
  LanguageModelCallOptions,
  LanguageModelGenerateResult,
  LanguageModelStreamResult,
  LanguageModel,
} from './types'

// AI Observability MV Integration
export {
  AIObservabilityMVIntegration,
  createAIObservabilityMVs,
  queryBuiltinView,
} from './observability-mv'

export type {
  AIObservabilityConfig,
  AIObservabilityState,
  AIAnalyticsView,
  ModelUsageData,
  HourlyRequestData,
  ErrorRateData,
  LatencyPercentileData,
  CacheHitRateData,
  TokenUsageData,
  BuiltinViewName,
  BuiltinViewDataMap,
} from './observability-mv'

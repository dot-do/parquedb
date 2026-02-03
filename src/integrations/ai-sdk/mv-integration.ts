/**
 * AI SDK Stream Collections for ParqueDB
 *
 * Exports stream collections that can be imported and added to a ParqueDB schema.
 * These collections automatically wire up ingestion from the AI SDK middleware
 * via the `$ingest: 'ai-sdk'` directive.
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { AIRequests, Generations } from 'parquedb/ai-sdk'
 *
 * const db = DB({
 *   // Import stream collections - ingestion is auto-wired
 *   AIRequests,
 *   Generations,
 *
 *   // Create MVs from the stream collections using $from
 *   DailyUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }, 'modelId'],
 *     $compute: {
 *       count: { $count: '*' },
 *       totalTokens: { $sum: 'tokens' },
 *       avgLatency: { $avg: 'latencyMs' },
 *     },
 *   },
 *
 *   AIErrors: {
 *     $from: 'AIRequests',
 *     $filter: { error: { $exists: true } },
 *   },
 *
 *   GeneratedObjects: {
 *     $from: 'Generations',
 *     $filter: { contentType: 'object' },
 *   },
 * }, { storage })
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Stream Collection Definitions
// =============================================================================

/**
 * AIRequests stream collection
 *
 * Captures all AI SDK requests (generate/stream) with metadata.
 * Use this as a source for analytics MVs.
 *
 * The `$ingest: 'ai-sdk'` directive automatically wires up the middleware
 * to write to this collection when it's included in your schema.
 *
 * @example
 * ```typescript
 * import { AIRequests } from 'parquedb/ai-sdk'
 *
 * const db = DB({
 *   AIRequests,
 *
 *   // MV for daily usage
 *   DailyAIUsage: {
 *     $from: 'AIRequests',
 *     $groupBy: [{ date: '$timestamp' }, 'modelId'],
 *     $compute: {
 *       requestCount: { $count: '*' },
 *       totalTokens: { $sum: 'tokens' },
 *       avgLatency: { $avg: 'latencyMs' },
 *       errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
 *     },
 *   },
 * }, { storage })
 * ```
 */
export const AIRequests = {
  /** Entity type identifier */
  $type: 'AIRequest',

  /** Model ID used for the request (e.g., 'gpt-4', 'claude-3-opus') */
  modelId: 'string!',

  /** Provider ID (e.g., 'openai', 'anthropic') */
  providerId: 'string!',

  /** Request type: 'generate' or 'stream' */
  requestType: 'string!',

  /** Total tokens used (prompt + completion) */
  tokens: 'int?',

  /** Request latency in milliseconds */
  latencyMs: 'int!',

  /** Whether response was served from cache */
  cached: 'boolean!',

  /** Error information if request failed */
  error: 'variant?',

  /** Request timestamp */
  timestamp: 'timestamp!',

  /** Ingestion source - wires up AI SDK middleware */
  $ingest: 'ai-sdk',
} as const

/**
 * Generations stream collection
 *
 * Captures generated content (text or structured objects) from AI SDK.
 * Use this as a source for content analytics MVs.
 *
 * The `$ingest: 'ai-sdk'` directive automatically wires up the middleware
 * to write to this collection when it's included in your schema.
 *
 * @example
 * ```typescript
 * import { Generations } from 'parquedb/ai-sdk'
 *
 * const db = DB({
 *   Generations,
 *
 *   // MV for generated objects by type
 *   GeneratedObjects: {
 *     $from: 'Generations',
 *     $filter: { contentType: 'object' },
 *   },
 *
 *   // MV for content stats by model
 *   ContentByModel: {
 *     $from: 'Generations',
 *     $groupBy: ['modelId', 'contentType'],
 *     $compute: {
 *       count: { $count: '*' },
 *       totalTokens: { $sum: 'tokens' },
 *     },
 *   },
 * }, { storage })
 * ```
 */
export const Generations = {
  /** Entity type identifier */
  $type: 'Generation',

  /** Model ID used for generation */
  modelId: 'string!',

  /** Content type: 'text' or 'object' */
  contentType: 'string!',

  /** Generated content (text string or structured object) */
  content: 'variant!',

  /** Original prompt (if captured) */
  prompt: 'string?',

  /** Tokens used for this generation */
  tokens: 'int?',

  /** Generation timestamp */
  timestamp: 'timestamp!',

  /** Ingestion source - wires up AI SDK middleware */
  $ingest: 'ai-sdk',
} as const

// =============================================================================
// Type Exports
// =============================================================================

/**
 * Type for the AIRequests collection schema
 */
export type AIRequestsCollection = typeof AIRequests

/**
 * Type for the Generations collection schema
 */
export type GenerationsCollection = typeof Generations

/**
 * AI Request entity (row in AIRequests collection)
 */
export interface AIRequest {
  $id: string
  $type: 'AIRequest'
  name: string
  modelId: string
  providerId: string
  requestType: 'generate' | 'stream'
  tokens?: number
  latencyMs: number
  cached: boolean
  error?: {
    name: string
    message: string
    stack?: string
  }
  timestamp: Date
  createdAt: Date
  createdBy?: string
  updatedAt?: Date
  updatedBy?: string
}

/**
 * Generation entity (row in Generations collection)
 */
export interface Generation {
  $id: string
  $type: 'Generation'
  name: string
  modelId: string
  contentType: 'text' | 'object'
  content: unknown
  prompt?: string
  tokens?: number
  timestamp: Date
  createdAt: Date
  createdBy?: string
  updatedAt?: Date
  updatedBy?: string
}

// =============================================================================
// MV Example Types (for documentation)
// =============================================================================

/**
 * Example: Daily AI usage aggregation
 *
 * This is the shape of data in a DailyAIUsage MV defined as:
 * ```typescript
 * DailyAIUsage: {
 *   $from: 'AIRequests',
 *   $groupBy: [{ date: '$timestamp' }, 'modelId'],
 *   $compute: {
 *     requestCount: { $count: '*' },
 *     totalTokens: { $sum: 'tokens' },
 *     avgLatency: { $avg: 'latencyMs' },
 *     errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
 *   },
 * }
 * ```
 */
export interface DailyAIUsageRow {
  date: string // ISO date string (truncated to day)
  modelId: string
  requestCount: number
  totalTokens: number
  avgLatency: number
  errorCount: number
}

/**
 * Example: Model error rates
 *
 * This is the shape of data in a ModelErrorRates MV defined as:
 * ```typescript
 * ModelErrorRates: {
 *   $from: 'AIRequests',
 *   $groupBy: ['modelId'],
 *   $compute: {
 *     totalRequests: { $count: '*' },
 *     errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
 *     errorRate: { $avg: { $cond: [{ $exists: '$error' }, 1, 0] } },
 *   },
 * }
 * ```
 */
export interface ModelErrorRateRow {
  modelId: string
  totalRequests: number
  errorCount: number
  errorRate: number
}

/**
 * Example: Cache hit rates
 *
 * This is the shape of data in a CacheHitRates MV defined as:
 * ```typescript
 * CacheHitRates: {
 *   $from: 'AIRequests',
 *   $groupBy: ['modelId'],
 *   $compute: {
 *     totalRequests: { $count: '*' },
 *     cachedRequests: { $sum: { $cond: ['$cached', 1, 0] } },
 *     hitRate: { $avg: { $cond: ['$cached', 1, 0] } },
 *   },
 * }
 * ```
 */
export interface CacheHitRateRow {
  modelId: string
  totalRequests: number
  cachedRequests: number
  hitRate: number
}

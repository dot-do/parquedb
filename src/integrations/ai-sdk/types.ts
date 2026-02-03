/**
 * Type definitions for AI SDK Middleware Integration
 *
 * These types mirror the Vercel AI SDK middleware interface for compatibility.
 * The AI SDK package is an optional peer dependency.
 *
 * @packageDocumentation
 */

import type { ParqueDB } from '../../ParqueDB'

// =============================================================================
// Middleware Configuration Types
// =============================================================================

/**
 * Cache configuration options
 */
export interface CacheConfig {
  /** Enable response caching (default: false) */
  enabled: boolean
  /** Time-to-live for cached entries in seconds (default: 3600 = 1 hour) */
  ttlSeconds?: number | undefined
  /** Collection name for cache storage (default: 'ai_cache') */
  collection?: string | undefined
  /** Hash function for generating cache keys (default: SHA-256 based) */
  hashFn?: ((params: unknown) => string | Promise<string>) | undefined
  /** Fields to exclude from cache key generation (e.g., 'temperature' for deterministic caching) */
  excludeFromKey?: string[] | undefined
}

/**
 * Logging configuration options
 */
export interface LoggingConfig {
  /** Enable request/response logging (default: false) */
  enabled: boolean
  /** Collection name for log storage (default: 'ai_logs') */
  collection?: string | undefined
  /** Log level: 'minimal' (prompt/response only), 'standard' (+ metadata), 'verbose' (everything) */
  level?: 'minimal' | 'standard' | 'verbose' | undefined
  /** Custom metadata to include with each log entry */
  metadata?: Record<string, unknown> | undefined
  /** Callback for custom log processing (runs after database write) */
  onLog?: ((entry: LogEntry) => void | Promise<void>) | undefined
}

/**
 * Options for creating ParqueDB AI SDK middleware
 */
export interface ParqueDBMiddlewareOptions {
  /** ParqueDB instance for storage */
  db: ParqueDB
  /** Cache configuration */
  cache?: CacheConfig | undefined
  /** Logging configuration */
  logging?: LoggingConfig | undefined
}

// =============================================================================
// Storage Types
// =============================================================================

/**
 * Cached LLM response entry
 */
export interface CacheEntry {
  /** Cache key (hash of request parameters) */
  key: string
  /** Original request parameters (for debugging/inspection) */
  params: Record<string, unknown>
  /** Cached response data */
  response: unknown
  /** Model ID used for the request */
  modelId?: string | undefined
  /** Cache hit count (incremented on each hit) */
  hitCount: number
  /** When this entry was created */
  createdAt: Date
  /** When this entry expires */
  expiresAt: Date
  /** When this entry was last accessed */
  lastAccessedAt: Date
}

/**
 * Log entry for AI requests
 */
export interface LogEntry {
  /** Unique log ID */
  $id: string
  /** Log type */
  $type: string
  /** Log name (model + timestamp) */
  name: string
  /** Timestamp of the request */
  timestamp: Date
  /** Model ID */
  modelId?: string | undefined
  /** Provider ID */
  providerId?: string | undefined
  /** Request type: 'generate' or 'stream' */
  requestType: 'generate' | 'stream'
  /** Prompt messages (if logging level allows) */
  prompt?: unknown | undefined
  /** Response data (if logging level allows) */
  response?: unknown | undefined
  /** Response text (extracted for convenience) */
  responseText?: string | undefined
  /** Token usage information */
  usage?: {
    promptTokens?: number | undefined
    completionTokens?: number | undefined
    totalTokens?: number | undefined
  } | undefined
  /** Request latency in milliseconds */
  latencyMs: number
  /** Whether the request was cached */
  cached: boolean
  /** Finish reason (e.g., 'stop', 'length', 'tool-calls') */
  finishReason?: string | undefined
  /** Additional metadata */
  metadata?: Record<string, unknown> | undefined
  /** Error information (if request failed) */
  error?: {
    name: string
    message: string
    stack?: string | undefined
  } | undefined
}

// =============================================================================
// AI SDK Middleware Types (Compatible Interface)
// =============================================================================

/**
 * Language model call options (subset of AI SDK types)
 */
export interface LanguageModelCallOptions {
  /** Input prompt/messages */
  prompt?: unknown | undefined
  /** Maximum tokens to generate */
  maxTokens?: number | undefined
  /** Temperature for sampling */
  temperature?: number | undefined
  /** Top-p sampling */
  topP?: number | undefined
  /** Stop sequences */
  stopSequences?: string[] | undefined
  /** Tool definitions */
  tools?: unknown[] | undefined
  /** Additional options */
  [key: string]: unknown
}

/**
 * Generate result from language model (subset of AI SDK types)
 */
export interface LanguageModelGenerateResult {
  /** Generated text */
  text?: string | undefined
  /** Finish reason */
  finishReason?: string | undefined
  /** Token usage */
  usage?: {
    promptTokens?: number | undefined
    completionTokens?: number | undefined
    totalTokens?: number | undefined
  } | undefined
  /** Tool calls made */
  toolCalls?: unknown[] | undefined
  /** Raw response */
  response?: unknown | undefined
  /** Additional properties */
  [key: string]: unknown
}

/**
 * Stream result from language model (subset of AI SDK types)
 */
export interface LanguageModelStreamResult {
  /** ReadableStream of chunks */
  stream: ReadableStream<unknown>
  /** Promise that resolves when stream completes */
  response?: Promise<{
    text?: string | undefined
    finishReason?: string | undefined
    usage?: {
      promptTokens?: number | undefined
      completionTokens?: number | undefined
      totalTokens?: number | undefined
    } | undefined
  }> | undefined
  /** Additional properties */
  [key: string]: unknown
}

/**
 * Language model interface (subset of AI SDK types)
 */
export interface LanguageModel {
  /** Specification version */
  specificationVersion?: string | undefined
  /** Model ID */
  modelId?: string | undefined
  /** Provider */
  provider?: string | undefined
  /** Model capabilities */
  capabilities?: {
    streaming?: boolean | undefined
    tools?: boolean | undefined
    images?: boolean | undefined
  } | undefined
}

/**
 * LanguageModelV3Middleware compatible interface
 *
 * This interface matches the Vercel AI SDK middleware specification
 * for use with wrapLanguageModel().
 */
export interface LanguageModelV3Middleware {
  /** Specification version (must be 'v3') */
  specificationVersion?: 'v3' | undefined

  /**
   * Transform parameters before they reach the language model
   */
  transformParams?: ((options: {
    type: 'generate' | 'stream'
    params: LanguageModelCallOptions
    model: LanguageModel
  }) => LanguageModelCallOptions | Promise<LanguageModelCallOptions>) | undefined

  /**
   * Wrap the generate operation
   */
  wrapGenerate?: ((options: {
    doGenerate: () => Promise<LanguageModelGenerateResult>
    doStream: () => Promise<LanguageModelStreamResult>
    params: LanguageModelCallOptions
    model: LanguageModel
  }) => Promise<LanguageModelGenerateResult>) | undefined

  /**
   * Wrap the stream operation
   */
  wrapStream?: ((options: {
    doGenerate: () => Promise<LanguageModelGenerateResult>
    doStream: () => Promise<LanguageModelStreamResult>
    params: LanguageModelCallOptions
    model: LanguageModel
  }) => Promise<LanguageModelStreamResult>) | undefined

  /**
   * Override the provider ID
   */
  overrideProvider?: ((options: { model: LanguageModel }) => string) | undefined

  /**
   * Override the model ID
   */
  overrideModelId?: ((options: { model: LanguageModel }) => string) | undefined
}

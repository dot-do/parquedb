/**
 * ParqueDB Middleware for Vercel AI SDK
 *
 * Provides caching and logging middleware for language model operations.
 * Works with the AI SDK's wrapLanguageModel() function.
 *
 * @example
 * ```typescript
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
 *     ttlSeconds: 3600,  // 1 hour
 *   },
 *   logging: {
 *     enabled: true,
 *     level: 'standard',
 *   },
 * })
 *
 * // Wrap your model
 * const model = wrapLanguageModel({
 *   model: openai('gpt-4'),
 *   middleware,
 * })
 *
 * // Use as normal - responses will be cached and logged
 * const result = await generateText({ model, prompt: 'Hello!' })
 * ```
 *
 * @packageDocumentation
 */

import type {
  ParqueDBMiddlewareOptions,
  LanguageModelV3Middleware,
  LanguageModelCallOptions,
  LanguageModelGenerateResult,
  LanguageModelStreamResult,
  LanguageModel,
  CacheEntry,
  LogEntry,
} from './types'
import { logger } from '../../utils/logger'
import { asTypedResult, asTypedResults } from '../../types/cast'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_CACHE_TTL_SECONDS = 3600 // 1 hour
const DEFAULT_CACHE_COLLECTION = 'ai_cache'
const DEFAULT_LOG_COLLECTION = 'ai_logs'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a deterministic hash of the request parameters for cache key
 *
 * Uses a simple string-based hash for compatibility across environments.
 * For production use with large payloads, consider using Web Crypto API.
 */
export async function hashParams(
  params: LanguageModelCallOptions,
  modelId: string | undefined,
  excludeFields?: string[]
): Promise<string> {
  // Create a copy of params without excluded fields
  const hashableParams = { ...params }

  if (excludeFields) {
    for (const field of excludeFields) {
      delete hashableParams[field]
    }
  }

  // Add model ID to ensure cache is model-specific
  const toHash = JSON.stringify({
    modelId,
    params: hashableParams,
  })

  // Use simple hash for universal compatibility
  // Based on djb2 algorithm
  let hash = 5381
  for (let i = 0; i < toHash.length; i++) {
    hash = ((hash << 5) + hash) ^ toHash.charCodeAt(i)
  }

  // Convert to base36 for compact representation
  return `cache_${(hash >>> 0).toString(36)}`
}

/**
 * Check if a cache entry has expired
 */
export function isExpired(entry: CacheEntry): boolean {
  return new Date() > new Date(entry.expiresAt)
}

/**
 * Calculate expiration date based on TTL
 */
function getExpirationDate(ttlSeconds: number): Date {
  return new Date(Date.now() + ttlSeconds * 1000)
}

/**
 * Extract text from a generate result
 */
function extractResponseText(result: LanguageModelGenerateResult): string | undefined {
  // Try common response text locations
  return result.text ??
    (result.response as { text?: string | undefined })?.text ??
    undefined
}

/**
 * Safely serialize params for logging (handle circular refs, truncate large data)
 */
function serializeForLog(data: unknown, maxLength = 10000): unknown {
  try {
    const str = JSON.stringify(data)
    if (str.length > maxLength) {
      return { _truncated: true, _length: str.length, _preview: str.slice(0, 500) }
    }
    return JSON.parse(str)
  } catch {
    return { _error: 'Could not serialize', _type: typeof data }
  }
}

// =============================================================================
// Middleware Factory
// =============================================================================

/**
 * Create ParqueDB middleware for Vercel AI SDK
 *
 * The middleware provides:
 * - Response caching: Store and retrieve LLM responses to reduce API calls
 * - Request logging: Log all AI requests for debugging and analytics
 *
 * @param options - Middleware configuration options
 * @returns LanguageModelV3Middleware compatible with AI SDK's wrapLanguageModel()
 *
 * @example
 * ```typescript
 * // Basic usage with caching only
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   cache: { enabled: true },
 * })
 *
 * // With logging only
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   logging: { enabled: true, level: 'verbose' },
 * })
 *
 * // With both caching and logging
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   cache: {
 *     enabled: true,
 *     ttlSeconds: 7200,  // 2 hours
 *     collection: 'my_ai_cache',
 *   },
 *   logging: {
 *     enabled: true,
 *     level: 'standard',
 *     collection: 'my_ai_logs',
 *     metadata: { app: 'my-app', version: '1.0.0' },
 *   },
 * })
 * ```
 */
export function createParqueDBMiddleware(
  options: ParqueDBMiddlewareOptions
): LanguageModelV3Middleware {
  const { db, cache, logging } = options

  // Resolve configuration with defaults
  const cacheEnabled = cache?.enabled ?? false
  const cacheTTL = cache?.ttlSeconds ?? DEFAULT_CACHE_TTL_SECONDS
  const cacheCollection = cache?.collection ?? DEFAULT_CACHE_COLLECTION
  const cacheHashFn = cache?.hashFn ?? ((params: unknown) => hashParams(params as LanguageModelCallOptions, undefined, cache?.excludeFromKey))
  const cacheExcludeFields = cache?.excludeFromKey

  const loggingEnabled = logging?.enabled ?? false
  const logCollection = logging?.collection ?? DEFAULT_LOG_COLLECTION
  const logLevel = logging?.level ?? 'standard'
  const logMetadata = logging?.metadata ?? {}
  const onLog = logging?.onLog

  return {
    specificationVersion: 'v3',

    /**
     * Wrap generate operations with caching and logging
     */
    wrapGenerate: async ({ doGenerate, params, model }) => {
      const startTime = Date.now()
      let cached = false
      let cacheKey: string | undefined

      // Try to get from cache
      if (cacheEnabled) {
        try {
          cacheKey = await (typeof cacheHashFn === 'function'
            ? cacheHashFn(params)
            : hashParams(params, model.modelId, cacheExcludeFields))

          const cacheResult = await db.collection(cacheCollection).findOne({ key: cacheKey })

          if (cacheResult && !isExpired(asTypedResult<CacheEntry>(cacheResult))) {
            // Update hit count and last accessed time
            const localId = (cacheResult.$id as string).split('/').pop()
            if (localId) {
              await db.collection(cacheCollection).update(localId, {
                $inc: { hitCount: 1 },
                $set: { lastAccessedAt: new Date() },
              }).catch(() => {
                // Silently ignore update errors
              })
            }

            cached = true
            const result = asTypedResult<CacheEntry>(cacheResult).response as LanguageModelGenerateResult

            // Log the cache hit if logging is enabled
            if (loggingEnabled) {
              await logRequest({
                db,
                collection: logCollection,
                level: logLevel,
                metadata: logMetadata,
                onLog,
                entry: {
                  requestType: 'generate',
                  modelId: model.modelId,
                  providerId: model.provider,
                  prompt: params.prompt,
                  response: result,
                  responseText: extractResponseText(result),
                  usage: result.usage,
                  latencyMs: Date.now() - startTime,
                  cached: true,
                  finishReason: result.finishReason,
                },
              })
            }

            return result
          }
        } catch (error) {
          // Cache lookup failed - proceed without cache
          logger.warn('[ParqueDB AI Middleware] Cache lookup failed:', error)
        }
      }

      // Execute the actual generate call
      let result: LanguageModelGenerateResult
      let error: Error | undefined

      try {
        result = await doGenerate()
      } catch (e) {
        error = e instanceof Error ? e : new Error(String(e))
        throw error
      } finally {
        const endTime = Date.now()
        const latencyMs = endTime - startTime

        // Log the request (success or failure)
        if (loggingEnabled) {
          await logRequest({
            db,
            collection: logCollection,
            level: logLevel,
            metadata: logMetadata,
            onLog,
            entry: {
              requestType: 'generate',
              modelId: model.modelId,
              providerId: model.provider,
              prompt: params.prompt,
              response: error ? undefined : result!,
              responseText: error ? undefined : extractResponseText(result!),
              usage: error ? undefined : result!.usage,
              latencyMs,
              cached: false,
              finishReason: error ? undefined : result!.finishReason,
              error: error ? {
                name: error.name,
                message: error.message,
                stack: error.stack,
              } : undefined,
            },
          })
        }

        // Cache the result (only on success)
        if (cacheEnabled && !error && cacheKey) {
          try {
            await db.collection(cacheCollection).create({
              $type: 'AICache',
              name: `cache-${cacheKey}`,
              key: cacheKey,
              params: serializeForLog(params),
              response: result!,
              modelId: model.modelId,
              hitCount: 0,
              expiresAt: getExpirationDate(cacheTTL),
              lastAccessedAt: new Date(),
            })
          } catch (cacheError) {
            // Cache write failed - log but don't fail the request
            logger.warn('[ParqueDB AI Middleware] Cache write failed:', cacheError)
          }
        }
      }

      return result!
    },

    /**
     * Wrap stream operations with logging
     *
     * Note: Streaming responses are not cached by default because:
     * 1. Streams are consumed once and cannot be replayed without buffering
     * 2. Buffering defeats the purpose of streaming (lower latency)
     *
     * Logging captures the stream metadata but not individual chunks.
     */
    wrapStream: async ({ doStream, params, model }) => {
      const startTime = Date.now()

      // Execute the actual stream call
      let streamResult: LanguageModelStreamResult
      let error: Error | undefined

      try {
        streamResult = await doStream()
      } catch (e) {
        error = e instanceof Error ? e : new Error(String(e))

        // Log the error
        if (loggingEnabled) {
          await logRequest({
            db,
            collection: logCollection,
            level: logLevel,
            metadata: logMetadata,
            onLog,
            entry: {
              requestType: 'stream',
              modelId: model.modelId,
              providerId: model.provider,
              prompt: params.prompt,
              latencyMs: Date.now() - startTime,
              cached: false,
              error: {
                name: error.name,
                message: error.message,
                stack: error.stack,
              },
            },
          })
        }

        throw error
      }

      // Log when stream completes (if response promise is available)
      if (loggingEnabled && streamResult.response) {
        // Don't await - let the stream continue while we set up logging
        streamResult.response.then(async (finalResponse) => {
          await logRequest({
            db,
            collection: logCollection,
            level: logLevel,
            metadata: logMetadata,
            onLog,
            entry: {
              requestType: 'stream',
              modelId: model.modelId,
              providerId: model.provider,
              prompt: params.prompt,
              responseText: finalResponse.text,
              usage: finalResponse.usage,
              latencyMs: Date.now() - startTime,
              cached: false,
              finishReason: finalResponse.finishReason,
            },
          })
        }).catch((e) => {
          // Stream failed during consumption - log the error
          logRequest({
            db,
            collection: logCollection,
            level: logLevel,
            metadata: logMetadata,
            onLog,
            entry: {
              requestType: 'stream',
              modelId: model.modelId,
              providerId: model.provider,
              prompt: params.prompt,
              latencyMs: Date.now() - startTime,
              cached: false,
              error: {
                name: 'StreamError',
                message: e instanceof Error ? e.message : String(e),
              },
            },
          })
        })
      } else if (loggingEnabled) {
        // No response promise - log immediately with what we have
        await logRequest({
          db,
          collection: logCollection,
          level: logLevel,
          metadata: logMetadata,
          onLog,
          entry: {
            requestType: 'stream',
            modelId: model.modelId,
            providerId: model.provider,
            prompt: params.prompt,
            latencyMs: Date.now() - startTime,
            cached: false,
          },
        })
      }

      return streamResult
    },
  }
}

// =============================================================================
// Internal Helpers
// =============================================================================

interface LogRequestOptions {
  db: ParqueDBMiddlewareOptions['db']
  collection: string
  level: 'minimal' | 'standard' | 'verbose'
  metadata: Record<string, unknown>
  onLog?: ((entry: LogEntry) => void | Promise<void>) | undefined
  entry: Omit<LogEntry, '$id' | '$type' | 'name' | 'timestamp' | 'metadata'>
}

/**
 * Log a request to the database
 */
async function logRequest(options: LogRequestOptions): Promise<void> {
  const { db, collection, level, metadata, onLog, entry } = options

  // Build log entry based on log level
  const logData: Record<string, unknown> = {
    $type: 'AILog',
    name: `log-${entry.modelId ?? 'unknown'}-${Date.now()}`,
    timestamp: new Date(),
    requestType: entry.requestType,
    modelId: entry.modelId,
    providerId: entry.providerId,
    latencyMs: entry.latencyMs,
    cached: entry.cached,
    finishReason: entry.finishReason,
    metadata,
  }

  // Add fields based on log level
  if (level === 'standard' || level === 'verbose') {
    logData.usage = entry.usage
    logData.responseText = entry.responseText
  }

  if (level === 'verbose') {
    logData.prompt = serializeForLog(entry.prompt)
    logData.response = serializeForLog(entry.response)
  }

  // Always log errors
  if (entry.error) {
    logData.error = entry.error
  }

  try {
    const createdEntry = await db.collection(collection).create(logData as Record<string, unknown>)

    // Call custom log handler if provided
    if (onLog) {
      await onLog(asTypedResult<LogEntry>(createdEntry))
    }
  } catch (error) {
    // Don't let logging errors break the request
    logger.warn('[ParqueDB AI Middleware] Failed to log request:', error)
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Query cached responses from the database
 *
 * @example
 * ```typescript
 * // Get recent cache entries
 * const entries = await queryCacheEntries(db, {
 *   limit: 100,
 *   sortBy: 'hitCount',
 *   sortOrder: 'desc',
 * })
 *
 * // Get entries for a specific model
 * const gpt4Entries = await queryCacheEntries(db, {
 *   modelId: 'gpt-4',
 *   includeExpired: false,
 * })
 * ```
 */
export async function queryCacheEntries(
  db: ParqueDBMiddlewareOptions['db'],
  options?: {
    collection?: string | undefined
    modelId?: string | undefined
    limit?: number | undefined
    sortBy?: 'createdAt' | 'hitCount' | 'lastAccessedAt' | undefined
    sortOrder?: 'asc' | 'desc' | undefined
    includeExpired?: boolean | undefined
  }
): Promise<CacheEntry[]> {
  const collection = options?.collection ?? DEFAULT_CACHE_COLLECTION
  const filter: Record<string, unknown> = {}

  if (options?.modelId) {
    filter.modelId = options.modelId
  }

  if (!options?.includeExpired) {
    filter.expiresAt = { $gt: new Date() }
  }

  const sortField = options?.sortBy ?? 'createdAt'
  const sortOrder = options?.sortOrder === 'asc' ? 1 : -1

  const results = await db.collection(collection).find(filter, {
    limit: options?.limit ?? 100,
    sort: { [sortField]: sortOrder },
  })

  return asTypedResults<CacheEntry>(results.items)
}

/**
 * Query log entries from the database
 *
 * @example
 * ```typescript
 * // Get recent logs
 * const logs = await queryLogEntries(db, {
 *   limit: 50,
 * })
 *
 * // Get error logs only
 * const errorLogs = await queryLogEntries(db, {
 *   errorsOnly: true,
 * })
 *
 * // Get logs for a specific model
 * const gpt4Logs = await queryLogEntries(db, {
 *   modelId: 'gpt-4',
 *   since: new Date(Date.now() - 24 * 60 * 60 * 1000), // Last 24 hours
 * })
 * ```
 */
export async function queryLogEntries(
  db: ParqueDBMiddlewareOptions['db'],
  options?: {
    collection?: string | undefined
    modelId?: string | undefined
    requestType?: 'generate' | 'stream' | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
    errorsOnly?: boolean | undefined
    cachedOnly?: boolean | undefined
  }
): Promise<LogEntry[]> {
  const collection = options?.collection ?? DEFAULT_LOG_COLLECTION
  const filter: Record<string, unknown> = {}

  if (options?.modelId) {
    filter.modelId = options.modelId
  }

  if (options?.requestType) {
    filter.requestType = options.requestType
  }

  if (options?.since || options?.until) {
    filter.timestamp = {}
    if (options?.since) {
      (filter.timestamp as Record<string, unknown>).$gte = options.since
    }
    if (options?.until) {
      (filter.timestamp as Record<string, unknown>).$lte = options.until
    }
  }

  if (options?.errorsOnly) {
    filter.error = { $exists: true }
  }

  if (options?.cachedOnly) {
    filter.cached = true
  }

  const results = await db.collection(collection).find(filter, {
    limit: options?.limit ?? 100,
    sort: { timestamp: -1 },
  })

  return asTypedResults<LogEntry>(results.items)
}

/**
 * Clear expired cache entries
 *
 * @example
 * ```typescript
 * // Clean up expired entries
 * const deleted = await clearExpiredCache(db)
 * console.log(`Deleted ${deleted} expired cache entries`)
 * ```
 */
export async function clearExpiredCache(
  db: ParqueDBMiddlewareOptions['db'],
  options?: {
    collection?: string | undefined
  }
): Promise<number> {
  const collection = options?.collection ?? DEFAULT_CACHE_COLLECTION

  const result = await db.collection(collection).deleteMany({
    expiresAt: { $lt: new Date() },
  }, { hard: true })

  return result.deletedCount
}

/**
 * Get cache statistics
 *
 * @example
 * ```typescript
 * const stats = await getCacheStats(db)
 * console.log(`Total entries: ${stats.totalEntries}`)
 * console.log(`Total hits: ${stats.totalHits}`)
 * console.log(`Hit rate: ${stats.hitRate.toFixed(2)}%`)
 * ```
 */
export async function getCacheStats(
  db: ParqueDBMiddlewareOptions['db'],
  options?: {
    collection?: string | undefined
  }
): Promise<{
  totalEntries: number
  activeEntries: number
  expiredEntries: number
  totalHits: number
  oldestEntry?: Date | undefined
  newestEntry?: Date | undefined
}> {
  const collection = options?.collection ?? DEFAULT_CACHE_COLLECTION
  const now = new Date()

  const result = await db.collection(collection).find({}, { limit: 10000 })
  const allEntries = asTypedResults<CacheEntry>(result.items)

  let totalHits = 0
  let activeCount = 0
  let expiredCount = 0
  let oldestEntry: Date | undefined
  let newestEntry: Date | undefined

  for (const entry of allEntries) {
    totalHits += entry.hitCount

    if (new Date(entry.expiresAt) > now) {
      activeCount++
    } else {
      expiredCount++
    }

    const createdAt = new Date(entry.createdAt)
    if (!oldestEntry || createdAt < oldestEntry) {
      oldestEntry = createdAt
    }
    if (!newestEntry || createdAt > newestEntry) {
      newestEntry = createdAt
    }
  }

  return {
    totalEntries: allEntries.length,
    activeEntries: activeCount,
    expiredEntries: expiredCount,
    totalHits,
    oldestEntry,
    newestEntry,
  }
}

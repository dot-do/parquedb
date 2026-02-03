/**
 * AI SDK Middleware - Materialized Views Integration
 *
 * Provides local AI observability by streaming AI SDK log entries to
 * in-memory materialized views. This enables real-time analytics without
 * requiring external storage or database connections.
 *
 * Features:
 * - In-memory log storage with optional persistence
 * - Builtin views for common analytics (usage, errors, latency, etc.)
 * - Custom view registration for specialized analytics
 * - Retention policy support for memory management
 *
 * @example
 * ```typescript
 * import { createAIObservabilityMVs, createParqueDBMiddleware } from 'parquedb/ai-sdk'
 *
 * // Create the MV integration
 * const observability = createAIObservabilityMVs({
 *   enableBuiltinViews: true,
 *   retentionMs: 24 * 60 * 60 * 1000, // 24 hours
 * })
 *
 * await observability.start()
 *
 * // Wire to middleware via onLog callback
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   logging: {
 *     enabled: true,
 *     level: 'standard',
 *     onLog: (entry) => observability.processLogEntry(entry),
 *   },
 * })
 *
 * // Query analytics
 * const usage = await observability.query('model_usage')
 * ```
 *
 * @packageDocumentation
 */

import type { LogEntry } from './types'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for AI Observability MV Integration
 */
export interface AIObservabilityConfig {
  /**
   * Base path for optional file persistence
   * @default '.parquedb/ai-observability'
   */
  basePath?: string

  /**
   * Number of entries to batch before triggering view updates
   * @default 10
   */
  batchSize?: number

  /**
   * Maximum time (ms) to wait before processing a partial batch
   * @default 1000
   */
  batchTimeoutMs?: number

  /**
   * Enable builtin analytics views
   * @default true
   */
  enableBuiltinViews?: boolean

  /**
   * Retention period for log entries (ms)
   * Entries older than this will be removed on applyRetention()
   * @default undefined (no automatic retention)
   */
  retentionMs?: number
}

/**
 * Definition of an analytics view
 */
export interface AIAnalyticsView<T> {
  /** Unique name for the view */
  name: string

  /** Human-readable description */
  description?: string

  /**
   * Aggregate function that processes log entries
   * @param entries - Batch of log entries to process
   * @param existing - Current aggregated state (undefined on first call)
   * @returns Updated aggregated state
   */
  aggregate: (entries: LogEntry[], existing: T | undefined) => T
}

/**
 * State of the integration
 */
export interface AIObservabilityState {
  /** Whether the integration is running */
  isRunning: boolean

  /** Total entries processed */
  entriesProcessed: number

  /** Number of registered views */
  viewCount: number

  /** Names of registered views */
  viewNames: string[]

  /** Current buffer size */
  bufferSize: number

  /** Start time (if running) */
  startedAt: number | null

  /** Last entry processed time */
  lastEntryAt: number | null
}

// =============================================================================
// Builtin View Data Types
// =============================================================================

/**
 * Model usage aggregation data
 */
export interface ModelUsageData {
  requestCount: number
  errorCount: number
  totalTokens: number
  avgLatencyMs: number
  cachedCount: number
  lastRequestAt: Date
}

/**
 * Hourly request aggregation data
 */
export interface HourlyRequestData {
  requestCount: number
  generateCount: number
  streamCount: number
  cachedCount: number
  errorCount: number
  totalTokens: number
  avgLatencyMs: number
}

/**
 * Error rate data by model
 */
export interface ErrorRateData {
  totalRequests: number
  errorCount: number
  errorRate: number
  errorsByType: Record<string, number>
}

/**
 * Latency percentile data
 */
export interface LatencyPercentileData {
  min: number
  max: number
  p50: number
  p90: number
  p95: number
  p99: number
  samples: number[]
}

/**
 * Cache hit rate data
 */
export interface CacheHitRateData {
  totalRequests: number
  cachedRequests: number
  hitRate: number
  byModel: Record<string, { cached: number; total: number; rate: number }>
}

/**
 * Token usage data
 */
export interface TokenUsageData {
  totalPromptTokens: number
  totalCompletionTokens: number
  totalTokens: number
  byModel: Record<string, {
    promptTokens: number
    completionTokens: number
    totalTokens: number
  }>
}

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG: Required<AIObservabilityConfig> = {
  basePath: '.parquedb/ai-observability',
  batchSize: 10,
  batchTimeoutMs: 1000,
  enableBuiltinViews: true,
  retentionMs: 0, // 0 means no automatic retention
}

// =============================================================================
// Builtin Views
// =============================================================================

function createModelUsageView(): AIAnalyticsView<Map<string, ModelUsageData>> {
  return {
    name: 'model_usage',
    description: 'Usage statistics aggregated by model',
    aggregate: (entries, existing = new Map()) => {
      for (const entry of entries) {
        const modelId = entry.modelId ?? 'unknown'
        const current = existing.get(modelId) || {
          requestCount: 0,
          errorCount: 0,
          totalTokens: 0,
          avgLatencyMs: 0,
          cachedCount: 0,
          lastRequestAt: new Date(0),
        }

        const totalLatency = current.avgLatencyMs * current.requestCount
        current.requestCount++
        current.avgLatencyMs = (totalLatency + entry.latencyMs) / current.requestCount

        if (entry.error) {
          current.errorCount++
        }

        if (entry.cached) {
          current.cachedCount++
        }

        if (entry.usage?.totalTokens) {
          current.totalTokens += entry.usage.totalTokens
        }

        if (new Date(entry.timestamp) > current.lastRequestAt) {
          current.lastRequestAt = new Date(entry.timestamp)
        }

        existing.set(modelId, current)
      }
      return existing
    },
  }
}

function createHourlyRequestsView(): AIAnalyticsView<Map<string, HourlyRequestData>> {
  return {
    name: 'hourly_requests',
    description: 'Request counts aggregated by hour',
    aggregate: (entries, existing = new Map()) => {
      for (const entry of entries) {
        const date = new Date(entry.timestamp)
        const hourStart = new Date(
          date.getFullYear(),
          date.getMonth(),
          date.getDate(),
          date.getHours()
        )
        const key = hourStart.toISOString()

        const current = existing.get(key) || {
          requestCount: 0,
          generateCount: 0,
          streamCount: 0,
          cachedCount: 0,
          errorCount: 0,
          totalTokens: 0,
          avgLatencyMs: 0,
        }

        const totalLatency = current.avgLatencyMs * current.requestCount
        current.requestCount++
        current.avgLatencyMs = (totalLatency + entry.latencyMs) / current.requestCount

        if (entry.requestType === 'generate') {
          current.generateCount++
        } else if (entry.requestType === 'stream') {
          current.streamCount++
        }

        if (entry.cached) {
          current.cachedCount++
        }

        if (entry.error) {
          current.errorCount++
        }

        if (entry.usage?.totalTokens) {
          current.totalTokens += entry.usage.totalTokens
        }

        existing.set(key, current)
      }
      return existing
    },
  }
}

function createErrorRatesView(): AIAnalyticsView<Map<string, ErrorRateData>> {
  return {
    name: 'error_rates',
    description: 'Error rates aggregated by model',
    aggregate: (entries, existing = new Map()) => {
      for (const entry of entries) {
        const modelId = entry.modelId ?? 'unknown'
        const current = existing.get(modelId) || {
          totalRequests: 0,
          errorCount: 0,
          errorRate: 0,
          errorsByType: {},
        }

        current.totalRequests++

        if (entry.error) {
          current.errorCount++
          const errorType = entry.error.name ?? 'UnknownError'
          current.errorsByType[errorType] = (current.errorsByType[errorType] || 0) + 1
        }

        current.errorRate = current.errorCount / current.totalRequests

        existing.set(modelId, current)
      }
      return existing
    },
  }
}

function createLatencyPercentilesView(): AIAnalyticsView<Map<string, LatencyPercentileData>> {
  return {
    name: 'latency_percentiles',
    description: 'Latency percentiles by model',
    aggregate: (entries, existing = new Map()) => {
      for (const entry of entries) {
        const modelId = entry.modelId ?? 'unknown'
        const current = existing.get(modelId) || {
          min: Infinity,
          max: -Infinity,
          p50: 0,
          p90: 0,
          p95: 0,
          p99: 0,
          samples: [],
        }

        current.samples.push(entry.latencyMs)
        current.min = Math.min(current.min, entry.latencyMs)
        current.max = Math.max(current.max, entry.latencyMs)

        // Recalculate percentiles
        const sorted = [...current.samples].sort((a, b) => a - b)
        const len = sorted.length

        current.p50 = sorted[Math.floor(len * 0.5)] ?? 0
        current.p90 = sorted[Math.floor(len * 0.9)] ?? 0
        current.p95 = sorted[Math.floor(len * 0.95)] ?? 0
        current.p99 = sorted[Math.floor(len * 0.99)] ?? 0

        existing.set(modelId, current)
      }
      return existing
    },
  }
}

function createCacheHitRatesView(): AIAnalyticsView<CacheHitRateData> {
  return {
    name: 'cache_hit_rates',
    description: 'Cache hit rates overall and by model',
    aggregate: (entries, existing) => {
      const current = existing || {
        totalRequests: 0,
        cachedRequests: 0,
        hitRate: 0,
        byModel: {},
      }

      for (const entry of entries) {
        const modelId = entry.modelId ?? 'unknown'

        current.totalRequests++
        if (entry.cached) {
          current.cachedRequests++
        }
        current.hitRate = current.cachedRequests / current.totalRequests

        // By model
        if (!current.byModel[modelId]) {
          current.byModel[modelId] = { cached: 0, total: 0, rate: 0 }
        }
        current.byModel[modelId].total++
        if (entry.cached) {
          current.byModel[modelId].cached++
        }
        current.byModel[modelId].rate =
          current.byModel[modelId].cached / current.byModel[modelId].total
      }

      return current
    },
  }
}

function createTokenUsageView(): AIAnalyticsView<TokenUsageData> {
  return {
    name: 'token_usage',
    description: 'Token usage overall and by model',
    aggregate: (entries, existing) => {
      const current = existing || {
        totalPromptTokens: 0,
        totalCompletionTokens: 0,
        totalTokens: 0,
        byModel: {},
      }

      for (const entry of entries) {
        const modelId = entry.modelId ?? 'unknown'
        const usage = entry.usage

        if (usage) {
          current.totalPromptTokens += usage.promptTokens ?? 0
          current.totalCompletionTokens += usage.completionTokens ?? 0
          current.totalTokens += usage.totalTokens ?? 0

          if (!current.byModel[modelId]) {
            current.byModel[modelId] = {
              promptTokens: 0,
              completionTokens: 0,
              totalTokens: 0,
            }
          }
          current.byModel[modelId].promptTokens += usage.promptTokens ?? 0
          current.byModel[modelId].completionTokens += usage.completionTokens ?? 0
          current.byModel[modelId].totalTokens += usage.totalTokens ?? 0
        }
      }

      return current
    },
  }
}

// =============================================================================
// AIObservabilityMVIntegration Class
// =============================================================================

/**
 * AI Observability Materialized Views Integration
 *
 * Provides real-time AI analytics by streaming log entries to in-memory
 * materialized views. Can be used with the AI SDK middleware's onLog callback
 * to automatically capture and analyze AI operations.
 */
export class AIObservabilityMVIntegration {
  private config: Required<AIObservabilityConfig>
  private running = false
  private startedAt: number | null = null
  private lastEntryAt: number | null = null

  // Raw log storage
  private logs: LogEntry[] = []
  private entriesProcessed = 0

  // Entry buffer for batching
  private buffer: LogEntry[] = []
  private batchTimer: ReturnType<typeof setTimeout> | null = null

  // Registered views
  private views = new Map<string, AIAnalyticsView<unknown>>()
  private viewData = new Map<string, unknown>()

  constructor(config: AIObservabilityConfig = {}) {
    this.config = {
      ...DEFAULT_CONFIG,
      ...config,
    }

    if (this.config.enableBuiltinViews) {
      this.registerBuiltinViews()
    }
  }

  // ===========================================================================
  // Public API - Lifecycle
  // ===========================================================================

  /**
   * Start the integration
   */
  async start(): Promise<void> {
    if (this.running) {
      return
    }

    this.running = true
    this.startedAt = Date.now()
  }

  /**
   * Stop the integration gracefully
   *
   * Flushes any buffered entries before stopping.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return
    }

    // Flush remaining buffer
    if (this.buffer.length > 0) {
      await this.flushBuffer()
    }

    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

    this.running = false
  }

  /**
   * Check if the integration is running
   */
  isRunning(): boolean {
    return this.running
  }

  // ===========================================================================
  // Public API - Log Processing
  // ===========================================================================

  /**
   * Process a single log entry
   *
   * This is the main entry point for receiving log entries from the middleware.
   * Entries are batched for efficiency before being processed by views.
   *
   * @param entry - The log entry from AI SDK middleware
   */
  async processLogEntry(entry: LogEntry): Promise<void> {
    if (!this.running) {
      throw new Error('AIObservabilityMVIntegration is not running')
    }

    this.buffer.push(entry)
    this.lastEntryAt = Date.now()

    // Check if we should flush
    if (this.buffer.length >= this.config.batchSize) {
      await this.flushBuffer()
    } else if (!this.batchTimer) {
      // Set timeout for partial batch
      this.batchTimer = setTimeout(() => {
        this.flushBuffer().catch(() => {
          // Ignore errors in timer callback
        })
      }, this.config.batchTimeoutMs)
    }
  }

  // ===========================================================================
  // Public API - Views
  // ===========================================================================

  /**
   * Register a custom analytics view
   *
   * @param view - The view definition
   */
  registerView<T>(view: AIAnalyticsView<T>): void {
    this.views.set(view.name, view as AIAnalyticsView<unknown>)
  }

  /**
   * Unregister a view
   *
   * @param name - The view name
   */
  unregisterView(name: string): void {
    this.views.delete(name)
    this.viewData.delete(name)
  }

  /**
   * Get all registered view names
   */
  getViewNames(): string[] {
    return Array.from(this.views.keys())
  }

  /**
   * Query a view's current data
   *
   * @param name - The view name
   * @returns The view's aggregated data, or undefined if view doesn't exist
   */
  async query<T>(name: string): Promise<T | undefined> {
    return this.viewData.get(name) as T | undefined
  }

  // ===========================================================================
  // Public API - Raw Logs
  // ===========================================================================

  /**
   * Query raw log entries
   *
   * @param options - Query options
   * @returns Filtered log entries
   */
  async queryLogs(options: {
    modelId?: string
    requestType?: 'generate' | 'stream'
    since?: Date
    until?: Date
    limit?: number
    errorsOnly?: boolean
    cachedOnly?: boolean
  } = {}): Promise<LogEntry[]> {
    let result = [...this.logs]

    // Apply filters
    if (options.modelId) {
      result = result.filter(e => e.modelId === options.modelId)
    }

    if (options.requestType) {
      result = result.filter(e => e.requestType === options.requestType)
    }

    if (options.since) {
      result = result.filter(e => new Date(e.timestamp) >= options.since!)
    }

    if (options.until) {
      result = result.filter(e => new Date(e.timestamp) <= options.until!)
    }

    if (options.errorsOnly) {
      result = result.filter(e => e.error !== undefined)
    }

    if (options.cachedOnly) {
      result = result.filter(e => e.cached === true)
    }

    // Sort by timestamp descending
    result.sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    )

    // Apply limit
    if (options.limit !== undefined) {
      result = result.slice(0, options.limit)
    }

    return result
  }

  // ===========================================================================
  // Public API - State & Maintenance
  // ===========================================================================

  /**
   * Get the current state of the integration
   */
  getState(): AIObservabilityState {
    return {
      isRunning: this.running,
      entriesProcessed: this.entriesProcessed,
      viewCount: this.views.size,
      viewNames: Array.from(this.views.keys()),
      bufferSize: this.buffer.length,
      startedAt: this.startedAt,
      lastEntryAt: this.lastEntryAt,
    }
  }

  /**
   * Clear all data (logs and view data)
   */
  clear(): void {
    this.logs = []
    this.buffer = []
    this.entriesProcessed = 0
    this.viewData.clear()
  }

  /**
   * Apply retention policy and remove old entries
   *
   * @returns Number of entries removed
   */
  applyRetention(): number {
    if (!this.config.retentionMs || this.config.retentionMs <= 0) {
      return 0
    }

    const cutoff = new Date(Date.now() - this.config.retentionMs)
    const originalLength = this.logs.length

    this.logs = this.logs.filter(e => new Date(e.timestamp) >= cutoff)

    return originalLength - this.logs.length
  }

  /**
   * Refresh all views by reprocessing all logs
   *
   * This is useful after clearing and re-adding data, or after
   * registering new views that need to catch up on historical data.
   */
  async refresh(): Promise<void> {
    // Clear view data
    this.viewData.clear()

    // Reprocess all logs through all views
    for (const [name, view] of this.views) {
      const result = view.aggregate(this.logs, undefined)
      this.viewData.set(name, result)
    }
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Register builtin analytics views
   */
  private registerBuiltinViews(): void {
    this.registerView(createModelUsageView())
    this.registerView(createHourlyRequestsView())
    this.registerView(createErrorRatesView())
    this.registerView(createLatencyPercentilesView())
    this.registerView(createCacheHitRatesView())
    this.registerView(createTokenUsageView())
  }

  /**
   * Flush the buffer and process entries
   */
  private async flushBuffer(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

    if (this.buffer.length === 0) {
      return
    }

    // Snapshot and clear buffer
    const entries = this.buffer.splice(0, this.buffer.length)

    // Store in raw logs
    this.logs.push(...entries)
    this.entriesProcessed += entries.length

    // Process through all views
    for (const [name, view] of this.views) {
      const existing = this.viewData.get(name)
      const result = view.aggregate(entries, existing)
      this.viewData.set(name, result)
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new AI Observability MV Integration
 *
 * @param config - Configuration options
 * @returns A new AIObservabilityMVIntegration instance
 *
 * @example
 * ```typescript
 * const observability = createAIObservabilityMVs({
 *   enableBuiltinViews: true,
 *   retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
 * })
 *
 * await observability.start()
 *
 * // Use with middleware
 * const middleware = createParqueDBMiddleware({
 *   db,
 *   logging: {
 *     enabled: true,
 *     onLog: (entry) => observability.processLogEntry(entry),
 *   },
 * })
 * ```
 */
export function createAIObservabilityMVs(
  config: AIObservabilityConfig = {}
): AIObservabilityMVIntegration {
  return new AIObservabilityMVIntegration(config)
}

// =============================================================================
// Type-safe Query Helper
// =============================================================================

/**
 * Builtin view name type for type-safe queries
 */
export type BuiltinViewName =
  | 'model_usage'
  | 'hourly_requests'
  | 'error_rates'
  | 'latency_percentiles'
  | 'cache_hit_rates'
  | 'token_usage'

/**
 * Map of builtin view names to their data types
 */
export interface BuiltinViewDataMap {
  model_usage: Map<string, ModelUsageData>
  hourly_requests: Map<string, HourlyRequestData>
  error_rates: Map<string, ErrorRateData>
  latency_percentiles: Map<string, LatencyPercentileData>
  cache_hit_rates: CacheHitRateData
  token_usage: TokenUsageData
}

/**
 * Query a builtin view with type-safe return type
 *
 * @param integration - The AI Observability integration
 * @param viewName - Name of the builtin view
 * @returns The view's aggregated data
 *
 * @example
 * ```typescript
 * const usage = await queryBuiltinView(observability, 'model_usage')
 * // Type is: Map<string, ModelUsageData> | undefined
 *
 * const tokenUsage = await queryBuiltinView(observability, 'token_usage')
 * // Type is: TokenUsageData | undefined
 * ```
 */
export async function queryBuiltinView<K extends BuiltinViewName>(
  integration: AIObservabilityMVIntegration,
  viewName: K
): Promise<BuiltinViewDataMap[K] | undefined> {
  return integration.query<BuiltinViewDataMap[K]>(viewName)
}

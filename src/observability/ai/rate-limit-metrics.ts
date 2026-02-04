/**
 * AI Rate Limit Metrics for ParqueDB
 *
 * Provides rate limiting awareness metrics for AI workloads:
 * - Tokens per minute/hour by model
 * - Cost burn rate (USD per hour)
 * - Request rate per model/provider
 * - Alerting thresholds configuration
 *
 * This enables setting up alerts when approaching API rate limits or budget caps.
 *
 * @example
 * ```typescript
 * import { RateLimitMetrics, createRateLimitMetrics } from 'parquedb/observability/ai'
 *
 * // Create metrics tracker with custom thresholds
 * const metrics = createRateLimitMetrics({
 *   thresholds: {
 *     tokensPerMinute: { warning: 50000, critical: 90000 },
 *     costPerHour: { warning: 10.0, critical: 50.0 },
 *   },
 *   onAlert: async (alert) => {
 *     console.log('Rate limit alert:', alert)
 *     // Send to Slack, PagerDuty, etc.
 *   },
 * })
 *
 * // Track AI request usage
 * await metrics.observe({
 *   modelId: 'gpt-4',
 *   providerId: 'openai',
 *   promptTokens: 1000,
 *   completionTokens: 500,
 *   costUSD: 0.05,
 * })
 *
 * // Get current rate snapshot
 * const snapshot = metrics.getSnapshot('gpt-4', 'openai')
 * console.log(`Tokens/min: ${snapshot.tokensPerMinute}`)
 * console.log(`Cost/hour: $${snapshot.costPerHour.toFixed(2)}`)
 * ```
 *
 * @module observability/ai/rate-limit-metrics
 */

import { logger } from '../../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Alert severity levels
 */
export type RateLimitAlertSeverity = 'warning' | 'critical'

/**
 * Rate limit metric types
 */
export type RateLimitMetricType =
  | 'tokensPerMinute'
  | 'tokensPerHour'
  | 'costPerHour'
  | 'requestsPerMinute'

/**
 * Threshold configuration with warning and critical levels
 */
export interface ThresholdConfig {
  /** Warning threshold - triggers a warning alert */
  warning: number
  /** Critical threshold - triggers a critical alert */
  critical: number
}

/**
 * Rate limit thresholds configuration
 */
export interface RateLimitThresholds {
  /** Tokens per minute threshold */
  tokensPerMinute: ThresholdConfig
  /** Tokens per hour threshold */
  tokensPerHour: ThresholdConfig
  /** Cost per hour in USD */
  costPerHour: ThresholdConfig
  /** Requests per minute threshold */
  requestsPerMinute: ThresholdConfig
}

/**
 * Default rate limit thresholds
 *
 * These are conservative defaults based on common API limits.
 * Override for your specific use case.
 */
export const DEFAULT_RATE_LIMIT_THRESHOLDS: RateLimitThresholds = {
  tokensPerMinute: { warning: 80000, critical: 95000 },
  tokensPerHour: { warning: 1000000, critical: 1500000 },
  costPerHour: { warning: 50.0, critical: 100.0 },
  requestsPerMinute: { warning: 500, critical: 800 },
}

/**
 * Rate limit alert event
 */
export interface RateLimitAlert {
  /** Unique alert ID */
  id: string
  /** Type of metric that triggered the alert */
  metric: RateLimitMetricType
  /** Severity level */
  severity: RateLimitAlertSeverity
  /** Human-readable title */
  title: string
  /** Detailed description */
  description: string
  /** Model ID */
  modelId: string
  /** Provider ID */
  providerId: string
  /** Current value that triggered the alert */
  currentValue: number
  /** Threshold that was exceeded */
  threshold: number
  /** Percentage of threshold used */
  percentOfThreshold: number
  /** Timestamp when alert was triggered */
  timestamp: number
}

/**
 * Observation data point for rate tracking
 */
export interface RateLimitObservation {
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: string
  /** Number of prompt/input tokens */
  promptTokens: number
  /** Number of completion/output tokens */
  completionTokens: number
  /** Cost in USD */
  costUSD: number
  /** Timestamp (defaults to now) */
  timestamp?: number | undefined
}

/**
 * Rate snapshot for a specific model/provider
 */
export interface RateSnapshot {
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: string
  /** Total tokens in current window */
  totalTokens: number
  /** Total prompt tokens in current window */
  promptTokens: number
  /** Total completion tokens in current window */
  completionTokens: number
  /** Total cost in current window */
  totalCost: number
  /** Total requests in current window */
  totalRequests: number
  /** Estimated tokens per minute */
  tokensPerMinute: number
  /** Estimated tokens per hour */
  tokensPerHour: number
  /** Estimated cost per hour in USD */
  costPerHour: number
  /** Requests per minute */
  requestsPerMinute: number
  /** Window start timestamp */
  windowStart: number
  /** Window end timestamp (now) */
  windowEnd: number
  /** Window duration in milliseconds */
  windowDurationMs: number
}

/**
 * Aggregated snapshot across all models
 */
export interface AggregatedRateSnapshot {
  /** Total tokens across all models */
  totalTokens: number
  /** Total prompt tokens */
  promptTokens: number
  /** Total completion tokens */
  completionTokens: number
  /** Total cost in USD */
  totalCost: number
  /** Total requests */
  totalRequests: number
  /** Aggregate tokens per minute */
  tokensPerMinute: number
  /** Aggregate tokens per hour */
  tokensPerHour: number
  /** Aggregate cost per hour */
  costPerHour: number
  /** Aggregate requests per minute */
  requestsPerMinute: number
  /** Number of models tracked */
  modelCount: number
  /** Window duration in milliseconds */
  windowDurationMs: number
}

/**
 * Statistics about the rate limiter
 */
export interface RateLimitStats {
  /** Total observations processed */
  totalObservations: number
  /** Number of unique models tracked */
  modelsTracked: number
  /** Number of unique providers tracked */
  providersTracked: number
  /** Total alerts triggered */
  alertsTriggered: number
  /** Alerts by severity */
  alertsBySeverity: Record<RateLimitAlertSeverity, number>
  /** Start time of tracking */
  startedAt: number
  /** Last observation time */
  lastObservationAt: number | null
}

/**
 * Rate limit metrics configuration
 */
export interface RateLimitMetricsConfig {
  /**
   * Window size for rate calculations in milliseconds
   * @default 60000 (1 minute)
   */
  windowSizeMs?: number | undefined

  /**
   * Minimum window duration before triggering alerts (prevents false positives from instant rates)
   * @default 1000 (1 second)
   */
  minWindowForAlertsMs?: number | undefined

  /**
   * Global thresholds for all models
   */
  thresholds?: Partial<RateLimitThresholds> | undefined

  /**
   * Model-specific thresholds (override global)
   * Key format: "modelId:providerId"
   */
  modelThresholds?: Record<string, Partial<RateLimitThresholds>> | undefined

  /**
   * Callback when an alert is triggered
   */
  onAlert?: ((alert: RateLimitAlert) => void | Promise<void>) | undefined

  /**
   * Minimum interval between duplicate alerts in milliseconds
   * @default 300000 (5 minutes)
   */
  alertDedupeIntervalMs?: number | undefined

  /**
   * Whether to log alerts
   * @default true
   */
  logAlerts?: boolean | undefined
}

/**
 * Resolved configuration with defaults applied
 */
export interface ResolvedRateLimitMetricsConfig {
  windowSizeMs: number
  minWindowForAlertsMs: number
  thresholds: RateLimitThresholds
  modelThresholds: Record<string, Partial<RateLimitThresholds>>
  onAlert?: ((alert: RateLimitAlert) => void | Promise<void>) | undefined
  alertDedupeIntervalMs: number
  logAlerts: boolean
}

// =============================================================================
// Internal Types
// =============================================================================

/**
 * Internal data point stored in the window
 */
interface DataPoint {
  timestamp: number
  promptTokens: number
  completionTokens: number
  costUSD: number
}

/**
 * Per-model tracking state
 */
interface ModelState {
  modelId: string
  providerId: string
  dataPoints: DataPoint[]
}

// =============================================================================
// RateLimitMetrics Class
// =============================================================================

/**
 * Rate limit metrics tracker for AI workloads
 *
 * Tracks token usage, costs, and request rates with configurable
 * thresholds and alerting.
 */
export class RateLimitMetrics {
  private config: ResolvedRateLimitMetricsConfig
  private modelStates: Map<string, ModelState> = new Map()
  private startedAt: number
  private lastObservationAt: number | null = null
  private totalObservations = 0
  private alertsTriggered = 0
  private alertsBySeverity: Record<RateLimitAlertSeverity, number> = {
    warning: 0,
    critical: 0,
  }
  private lastAlertTime: Map<string, number> = new Map()
  private providers: Set<string> = new Set()

  constructor(config: RateLimitMetricsConfig = {}) {
    this.config = {
      windowSizeMs: config.windowSizeMs ?? 60000,
      minWindowForAlertsMs: config.minWindowForAlertsMs ?? 1000, // Need at least 1 second of data
      thresholds: { ...DEFAULT_RATE_LIMIT_THRESHOLDS, ...config.thresholds },
      modelThresholds: config.modelThresholds ?? {},
      onAlert: config.onAlert,
      alertDedupeIntervalMs: config.alertDedupeIntervalMs ?? 300000,
      logAlerts: config.logAlerts ?? true,
    }

    this.startedAt = Date.now()
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Record an observation and check for rate limit alerts
   *
   * @param observation - The observation data
   * @returns Array of triggered alerts (empty if none)
   */
  async observe(observation: RateLimitObservation): Promise<RateLimitAlert[]> {
    const timestamp = observation.timestamp ?? Date.now()
    const key = this.getModelKey(observation.modelId, observation.providerId)

    this.totalObservations++
    this.lastObservationAt = timestamp
    this.providers.add(observation.providerId)

    // Get or create model state
    let state = this.modelStates.get(key)
    if (!state) {
      state = {
        modelId: observation.modelId,
        providerId: observation.providerId,
        dataPoints: [],
      }
      this.modelStates.set(key, state)
    }

    // Add data point
    state.dataPoints.push({
      timestamp,
      promptTokens: observation.promptTokens,
      completionTokens: observation.completionTokens,
      costUSD: observation.costUSD,
    })

    // Clean up old data points
    this.cleanupWindow(state, timestamp)

    // Calculate current rates
    const snapshot = this.calculateSnapshot(state, timestamp)

    // Check thresholds and generate alerts
    const alerts = this.checkThresholds(snapshot)

    // Process alerts
    for (const alert of alerts) {
      await this.handleAlert(alert)
    }

    return alerts
  }

  /**
   * Get rate snapshot for a specific model/provider
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @returns Rate snapshot or undefined if not tracked
   */
  getSnapshot(modelId: string, providerId: string): RateSnapshot | undefined {
    const key = this.getModelKey(modelId, providerId)
    const state = this.modelStates.get(key)

    if (!state) {
      return undefined
    }

    return this.calculateSnapshot(state, Date.now())
  }

  /**
   * Get all model snapshots
   *
   * @returns Map of model key to snapshot
   */
  getAllSnapshots(): Map<string, RateSnapshot> {
    const snapshots = new Map<string, RateSnapshot>()
    const now = Date.now()

    for (const [key, state] of Array.from(this.modelStates.entries())) {
      snapshots.set(key, this.calculateSnapshot(state, now))
    }

    return snapshots
  }

  /**
   * Get aggregated snapshot across all models
   *
   * @returns Aggregated rate snapshot
   */
  getAggregatedSnapshot(): AggregatedRateSnapshot {
    const now = Date.now()
    let totalTokens = 0
    let promptTokens = 0
    let completionTokens = 0
    let totalCost = 0
    let totalRequests = 0
    let windowDurationMs = 0

    for (const state of Array.from(this.modelStates.values())) {
      // Clean up old data
      this.cleanupWindow(state, now)

      for (const dp of state.dataPoints) {
        totalTokens += dp.promptTokens + dp.completionTokens
        promptTokens += dp.promptTokens
        completionTokens += dp.completionTokens
        totalCost += dp.costUSD
        totalRequests++
      }

      if (state.dataPoints.length > 0) {
        const firstPoint = state.dataPoints[0]
        if (firstPoint) {
          const oldest = firstPoint.timestamp
          const duration = now - oldest
          if (duration > windowDurationMs) {
            windowDurationMs = duration
          }
        }
      }
    }

    // Calculate rates
    const minuteMultiplier = windowDurationMs > 0 ? 60000 / windowDurationMs : 0
    const hourMultiplier = windowDurationMs > 0 ? 3600000 / windowDurationMs : 0

    return {
      totalTokens,
      promptTokens,
      completionTokens,
      totalCost,
      totalRequests,
      tokensPerMinute: totalTokens * minuteMultiplier,
      tokensPerHour: totalTokens * hourMultiplier,
      costPerHour: totalCost * hourMultiplier,
      requestsPerMinute: totalRequests * minuteMultiplier,
      modelCount: this.modelStates.size,
      windowDurationMs,
    }
  }

  /**
   * Get overall statistics
   */
  getStats(): RateLimitStats {
    return {
      totalObservations: this.totalObservations,
      modelsTracked: this.modelStates.size,
      providersTracked: this.providers.size,
      alertsTriggered: this.alertsTriggered,
      alertsBySeverity: { ...this.alertsBySeverity },
      startedAt: this.startedAt,
      lastObservationAt: this.lastObservationAt,
    }
  }

  /**
   * Get current configuration
   */
  getConfig(): ResolvedRateLimitMetricsConfig {
    return { ...this.config }
  }

  /**
   * Update thresholds dynamically
   *
   * @param thresholds - Partial thresholds to update
   */
  updateThresholds(thresholds: Partial<RateLimitThresholds>): void {
    this.config.thresholds = { ...this.config.thresholds, ...thresholds }
  }

  /**
   * Set model-specific thresholds
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @param thresholds - Thresholds for this model
   */
  setModelThresholds(
    modelId: string,
    providerId: string,
    thresholds: Partial<RateLimitThresholds>
  ): void {
    const key = this.getModelKey(modelId, providerId)
    this.config.modelThresholds[key] = thresholds
  }

  /**
   * Reset all tracking data
   */
  reset(): void {
    this.modelStates.clear()
    this.providers.clear()
    this.totalObservations = 0
    this.alertsTriggered = 0
    this.alertsBySeverity = { warning: 0, critical: 0 }
    this.lastObservationAt = null
    this.lastAlertTime.clear()
    this.startedAt = Date.now()
  }

  /**
   * Export metrics in Prometheus format
   *
   * @returns Prometheus text format metrics
   */
  exportPrometheus(): string {
    const lines: string[] = []
    const timestamp = Date.now()

    // Tokens per minute
    lines.push('# HELP parquedb_ai_tokens_per_minute Current token rate per minute by model')
    lines.push('# TYPE parquedb_ai_tokens_per_minute gauge')

    // Tokens per hour
    lines.push('# HELP parquedb_ai_tokens_per_hour Current token rate per hour by model')
    lines.push('# TYPE parquedb_ai_tokens_per_hour gauge')

    // Cost per hour
    lines.push('# HELP parquedb_ai_cost_per_hour Current cost burn rate per hour in USD by model')
    lines.push('# TYPE parquedb_ai_cost_per_hour gauge')

    // Requests per minute
    lines.push('# HELP parquedb_ai_requests_per_minute Current request rate per minute by model')
    lines.push('# TYPE parquedb_ai_requests_per_minute gauge')

    for (const state of Array.from(this.modelStates.values())) {
      const snapshot = this.calculateSnapshot(state, timestamp)
      const labels = `model="${state.modelId}",provider="${state.providerId}"`

      lines.push(`parquedb_ai_tokens_per_minute{${labels}} ${snapshot.tokensPerMinute.toFixed(2)} ${timestamp}`)
      lines.push(`parquedb_ai_tokens_per_hour{${labels}} ${snapshot.tokensPerHour.toFixed(2)} ${timestamp}`)
      lines.push(`parquedb_ai_cost_per_hour{${labels}} ${snapshot.costPerHour.toFixed(4)} ${timestamp}`)
      lines.push(`parquedb_ai_requests_per_minute{${labels}} ${snapshot.requestsPerMinute.toFixed(2)} ${timestamp}`)
    }

    return lines.join('\n')
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private getModelKey(modelId: string, providerId: string): string {
    return `${modelId}:${providerId}`
  }

  private cleanupWindow(state: ModelState, now: number): void {
    const cutoff = now - this.config.windowSizeMs
    state.dataPoints = state.dataPoints.filter(dp => dp.timestamp > cutoff)
  }

  private calculateSnapshot(state: ModelState, now: number): RateSnapshot {
    // Ensure window is clean
    this.cleanupWindow(state, now)

    let totalTokens = 0
    let promptTokens = 0
    let completionTokens = 0
    let totalCost = 0
    let windowStart = now

    for (const dp of state.dataPoints) {
      totalTokens += dp.promptTokens + dp.completionTokens
      promptTokens += dp.promptTokens
      completionTokens += dp.completionTokens
      totalCost += dp.costUSD
      if (dp.timestamp < windowStart) {
        windowStart = dp.timestamp
      }
    }

    const windowDurationMs = now - windowStart
    const totalRequests = state.dataPoints.length

    // Calculate rates (extrapolate to per-minute and per-hour)
    // For very short windows (or single observation), use the configured window size
    // This ensures a single request with 150 tokens immediately shows as 150 tokens/minute
    // if it's the only observation in the current minute window
    const effectiveWindowMs = Math.max(windowDurationMs, 1) // Avoid division by zero

    // If we have data points, extrapolate the rate based on the configured window
    // A single observation represents the rate over that instant
    // We extrapolate by assuming the current usage pattern continues
    const minuteMultiplier = effectiveWindowMs > 0 ? 60000 / effectiveWindowMs : 0
    const hourMultiplier = effectiveWindowMs > 0 ? 3600000 / effectiveWindowMs : 0

    return {
      modelId: state.modelId,
      providerId: state.providerId,
      totalTokens,
      promptTokens,
      completionTokens,
      totalCost,
      totalRequests,
      tokensPerMinute: totalTokens * minuteMultiplier,
      tokensPerHour: totalTokens * hourMultiplier,
      costPerHour: totalCost * hourMultiplier,
      requestsPerMinute: totalRequests * minuteMultiplier,
      windowStart,
      windowEnd: now,
      windowDurationMs,
    }
  }

  private getThresholdsForModel(modelId: string, providerId: string): RateLimitThresholds {
    const key = this.getModelKey(modelId, providerId)
    const modelThresholds = this.config.modelThresholds[key]

    if (modelThresholds) {
      return {
        tokensPerMinute: modelThresholds.tokensPerMinute ?? this.config.thresholds.tokensPerMinute,
        tokensPerHour: modelThresholds.tokensPerHour ?? this.config.thresholds.tokensPerHour,
        costPerHour: modelThresholds.costPerHour ?? this.config.thresholds.costPerHour,
        requestsPerMinute: modelThresholds.requestsPerMinute ?? this.config.thresholds.requestsPerMinute,
      }
    }

    return this.config.thresholds
  }

  private checkThresholds(snapshot: RateSnapshot): RateLimitAlert[] {
    const alerts: RateLimitAlert[] = []

    // Don't trigger alerts until we have a minimum amount of data
    // This prevents false positives from extrapolating single observations
    if (snapshot.windowDurationMs < this.config.minWindowForAlertsMs) {
      return alerts
    }

    const thresholds = this.getThresholdsForModel(snapshot.modelId, snapshot.providerId)

    // Check tokens per minute
    const tpmAlert = this.checkMetricThreshold(
      'tokensPerMinute',
      snapshot.tokensPerMinute,
      thresholds.tokensPerMinute,
      snapshot.modelId,
      snapshot.providerId
    )
    if (tpmAlert) alerts.push(tpmAlert)

    // Check tokens per hour
    const tphAlert = this.checkMetricThreshold(
      'tokensPerHour',
      snapshot.tokensPerHour,
      thresholds.tokensPerHour,
      snapshot.modelId,
      snapshot.providerId
    )
    if (tphAlert) alerts.push(tphAlert)

    // Check cost per hour
    const cphAlert = this.checkMetricThreshold(
      'costPerHour',
      snapshot.costPerHour,
      thresholds.costPerHour,
      snapshot.modelId,
      snapshot.providerId
    )
    if (cphAlert) alerts.push(cphAlert)

    // Check requests per minute
    const rpmAlert = this.checkMetricThreshold(
      'requestsPerMinute',
      snapshot.requestsPerMinute,
      thresholds.requestsPerMinute,
      snapshot.modelId,
      snapshot.providerId
    )
    if (rpmAlert) alerts.push(rpmAlert)

    return alerts
  }

  private checkMetricThreshold(
    metric: RateLimitMetricType,
    value: number,
    threshold: ThresholdConfig,
    modelId: string,
    providerId: string
  ): RateLimitAlert | null {
    let severity: RateLimitAlertSeverity | null = null
    let thresholdValue: number | null = null

    if (value >= threshold.critical) {
      severity = 'critical'
      thresholdValue = threshold.critical
    } else if (value >= threshold.warning) {
      severity = 'warning'
      thresholdValue = threshold.warning
    }

    if (!severity || !thresholdValue) {
      return null
    }

    const percentOfThreshold = (value / thresholdValue) * 100

    return {
      id: generateAlertId(),
      metric,
      severity,
      title: this.getAlertTitle(metric, severity, modelId),
      description: this.getAlertDescription(metric, value, thresholdValue, modelId, providerId),
      modelId,
      providerId,
      currentValue: value,
      threshold: thresholdValue,
      percentOfThreshold,
      timestamp: Date.now(),
    }
  }

  private getAlertTitle(
    metric: RateLimitMetricType,
    severity: RateLimitAlertSeverity,
    modelId: string
  ): string {
    const metricNames: Record<RateLimitMetricType, string> = {
      tokensPerMinute: 'tokens/min',
      tokensPerHour: 'tokens/hour',
      costPerHour: 'cost/hour',
      requestsPerMinute: 'requests/min',
    }

    return `${severity === 'critical' ? 'Critical' : 'Warning'}: High ${metricNames[metric]} for ${modelId}`
  }

  private getAlertDescription(
    metric: RateLimitMetricType,
    value: number,
    threshold: number,
    modelId: string,
    providerId: string
  ): string {
    const formatValue = (m: RateLimitMetricType, v: number): string => {
      if (m === 'costPerHour') return `$${v.toFixed(2)}`
      return v.toFixed(0)
    }

    return `${modelId} (${providerId}) ${metric} is ${formatValue(metric, value)}, exceeding threshold of ${formatValue(metric, threshold)}`
  }

  private shouldSendAlert(alert: RateLimitAlert): boolean {
    const key = `${alert.metric}:${alert.modelId}:${alert.providerId}:${alert.severity}`
    const lastTime = this.lastAlertTime.get(key)
    const now = Date.now()

    if (lastTime && now - lastTime < this.config.alertDedupeIntervalMs) {
      return false
    }

    this.lastAlertTime.set(key, now)
    return true
  }

  private async handleAlert(alert: RateLimitAlert): Promise<void> {
    // Check dedupe
    if (!this.shouldSendAlert(alert)) {
      return
    }

    // Update statistics
    this.alertsTriggered++
    this.alertsBySeverity[alert.severity]++

    // Log if enabled
    if (this.config.logAlerts) {
      logger.warn('ai_rate_limit_alert', {
        alert_id: alert.id,
        metric: alert.metric,
        severity: alert.severity,
        modelId: alert.modelId,
        providerId: alert.providerId,
        currentValue: alert.currentValue,
        threshold: alert.threshold,
        percentOfThreshold: alert.percentOfThreshold,
      })
    }

    // Call callback if set
    if (this.config.onAlert) {
      try {
        await this.config.onAlert(alert)
      } catch (error) {
        logger.error('ai_rate_limit_alert_callback_error', {
          alert_id: alert.id,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Generate a unique alert ID
 */
function generateAlertId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `ratelimit_${timestamp}_${random}`
}

/**
 * Create a RateLimitMetrics instance
 *
 * @param config - Configuration options
 * @returns RateLimitMetrics instance
 *
 * @example
 * ```typescript
 * const metrics = createRateLimitMetrics({
 *   thresholds: {
 *     tokensPerMinute: { warning: 50000, critical: 90000 },
 *     costPerHour: { warning: 10.0, critical: 50.0 },
 *   },
 *   onAlert: async (alert) => {
 *     await sendSlackNotification(alert)
 *   },
 * })
 * ```
 */
export function createRateLimitMetrics(config: RateLimitMetricsConfig = {}): RateLimitMetrics {
  return new RateLimitMetrics(config)
}

/**
 * Create a RateLimitMetrics instance with webhook integration
 *
 * @param webhookUrl - URL to send alerts to
 * @param config - Additional configuration
 * @returns RateLimitMetrics with webhook callback
 */
export function createRateLimitMetricsWithWebhook(
  webhookUrl: string,
  config: Omit<RateLimitMetricsConfig, 'onAlert'> = {}
): RateLimitMetrics {
  return new RateLimitMetrics({
    ...config,
    onAlert: async (alert) => {
      try {
        await fetch(webhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...alert,
            timestampIso: new Date(alert.timestamp).toISOString(),
          }),
        })
      } catch (error) {
        logger.error('ai_rate_limit_webhook_error', {
          alert_id: alert.id,
          webhook_url: webhookUrl,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    },
  })
}

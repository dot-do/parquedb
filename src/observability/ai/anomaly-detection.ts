/**
 * AI Anomaly Detection for ParqueDB
 *
 * Provides anomaly detection capabilities for AI workload metrics:
 * - Latency spike detection (standard deviation based)
 * - Cost anomaly alerts
 * - Error rate threshold violations
 * - Token usage outliers
 * - Model performance degradation
 *
 * Uses rolling window statistics for establishing baselines and
 * standard deviation based thresholds for anomaly detection.
 *
 * @example
 * ```typescript
 * import { AnomalyDetector, createAnomalyDetector } from 'parquedb/observability/ai'
 *
 * // Create detector with custom thresholds
 * const detector = createAnomalyDetector({
 *   windowSize: 100,
 *   latencyThresholdStdDev: 2.5,
 *   errorRateThreshold: 0.1,
 *   onAnomaly: async (anomaly) => {
 *     console.log('Anomaly detected:', anomaly)
 *     // Send to Slack, PagerDuty, etc.
 *   },
 * })
 *
 * // Process AI request metrics
 * detector.observe({
 *   modelId: 'gpt-4',
 *   latencyMs: 5000, // High latency
 *   costUSD: 0.15,
 *   errorRate: 0.05,
 *   tokenUsage: 1500,
 * })
 * ```
 *
 * @module observability/ai/anomaly-detection
 */

import { logger } from '../../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Anomaly severity levels
 */
export type AnomalySeverity = 'info' | 'warning' | 'critical'

/**
 * Types of anomalies that can be detected
 */
export type AnomalyType =
  | 'latency_spike'
  | 'cost_anomaly'
  | 'error_rate_violation'
  | 'token_usage_outlier'
  | 'performance_degradation'
  | 'success_rate_drop'
  | 'cache_hit_drop'

/**
 * Detected anomaly event
 */
export interface AnomalyEvent {
  /** Unique anomaly ID */
  id: string
  /** Type of anomaly */
  type: AnomalyType
  /** Severity level */
  severity: AnomalySeverity
  /** Human-readable title */
  title: string
  /** Detailed description */
  description: string
  /** Model ID (if applicable) */
  modelId?: string | undefined
  /** Provider ID (if applicable) */
  providerId?: string | undefined
  /** Current value that triggered the anomaly */
  currentValue: number
  /** Expected/baseline value */
  expectedValue: number
  /** Threshold that was exceeded */
  threshold: number
  /** How many standard deviations from mean */
  stdDeviations?: number | undefined
  /** Window statistics at time of detection */
  windowStats?: WindowStats | undefined
  /** Timestamp when anomaly was detected */
  timestamp: number
  /** Additional context */
  context?: Record<string, unknown> | undefined
}

/**
 * Rolling window statistics for a metric
 */
export interface WindowStats {
  /** Number of samples in window */
  count: number
  /** Sum of values */
  sum: number
  /** Sum of squared values (for variance calculation) */
  sumSquared: number
  /** Minimum value */
  min: number
  /** Maximum value */
  max: number
  /** Mean (average) value */
  mean: number
  /** Standard deviation */
  stdDev: number
  /** Variance */
  variance: number
}

/**
 * Observation data point for anomaly detection
 */
export interface AnomalyObservation {
  /** Model identifier */
  modelId?: string | undefined
  /** Provider identifier */
  providerId?: string | undefined
  /** Request latency in milliseconds */
  latencyMs?: number | undefined
  /** Cost in USD */
  costUSD?: number | undefined
  /** Error rate (0-1) */
  errorRate?: number | undefined
  /** Token usage count */
  tokenUsage?: number | undefined
  /** Success rate (0-1) */
  successRate?: number | undefined
  /** Cache hit rate (0-1) */
  cacheHitRate?: number | undefined
  /** Request count (for rate-based metrics) */
  requestCount?: number | undefined
  /** Timestamp (defaults to now) */
  timestamp?: number | undefined
  /** Additional context to include in anomaly events */
  context?: Record<string, unknown> | undefined
}

/**
 * Anomaly detection thresholds configuration
 */
export interface AnomalyThresholds {
  /**
   * Number of standard deviations for latency spike detection
   * @default 2.5
   */
  latencyStdDevThreshold: number

  /**
   * Number of standard deviations for cost anomaly detection
   * @default 3.0
   */
  costStdDevThreshold: number

  /**
   * Absolute error rate threshold (0-1) for warning
   * @default 0.05
   */
  errorRateWarningThreshold: number

  /**
   * Absolute error rate threshold (0-1) for critical
   * @default 0.1
   */
  errorRateCriticalThreshold: number

  /**
   * Number of standard deviations for token usage outlier detection
   * @default 2.5
   */
  tokenUsageStdDevThreshold: number

  /**
   * Minimum success rate before triggering warning
   * @default 0.95
   */
  minSuccessRateWarning: number

  /**
   * Minimum success rate before triggering critical
   * @default 0.9
   */
  minSuccessRateCritical: number

  /**
   * Minimum cache hit rate before triggering warning
   * @default 0.5
   */
  minCacheHitRateWarning: number

  /**
   * Minimum latency samples before anomaly detection is active
   * @default 10
   */
  minSamplesForDetection: number

  /**
   * Latency threshold (ms) for absolute spike detection (regardless of baseline)
   * @default 10000 (10 seconds)
   */
  absoluteLatencyThreshold: number

  /**
   * Cost threshold (USD) for absolute anomaly detection (regardless of baseline)
   * @default 1.0
   */
  absoluteCostThreshold: number
}

/**
 * Default anomaly thresholds
 */
export const DEFAULT_ANOMALY_THRESHOLDS: AnomalyThresholds = {
  latencyStdDevThreshold: 2.5,
  costStdDevThreshold: 3.0,
  errorRateWarningThreshold: 0.05,
  errorRateCriticalThreshold: 0.1,
  tokenUsageStdDevThreshold: 2.5,
  minSuccessRateWarning: 0.95,
  minSuccessRateCritical: 0.9,
  minCacheHitRateWarning: 0.5,
  minSamplesForDetection: 10,
  absoluteLatencyThreshold: 10000,
  absoluteCostThreshold: 1.0,
}

/**
 * Anomaly detector configuration
 */
export interface AnomalyDetectorConfig {
  /**
   * Size of the rolling window for baseline calculation
   * @default 100
   */
  windowSize?: number | undefined

  /**
   * Anomaly detection thresholds
   */
  thresholds?: Partial<AnomalyThresholds> | undefined

  /**
   * Callback when anomaly is detected
   */
  onAnomaly?: ((anomaly: AnomalyEvent) => void | Promise<void>) | undefined

  /**
   * Whether to log anomalies to console
   * @default true
   */
  logAnomalies?: boolean | undefined

  /**
   * Minimum interval between alerts of same type (ms)
   * @default 300000 (5 minutes)
   */
  dedupeIntervalMs?: number | undefined

  /**
   * Whether to track per-model statistics
   * @default true
   */
  perModelStats?: boolean | undefined
}

/**
 * Resolved configuration with defaults applied
 */
export interface ResolvedAnomalyDetectorConfig {
  windowSize: number
  thresholds: AnomalyThresholds
  onAnomaly?: ((anomaly: AnomalyEvent) => void | Promise<void>) | undefined
  logAnomalies: boolean
  dedupeIntervalMs: number
  perModelStats: boolean
}

/**
 * Anomaly detector statistics
 */
export interface AnomalyDetectorStats {
  /** Total observations processed */
  totalObservations: number
  /** Total anomalies detected */
  totalAnomalies: number
  /** Anomalies by type */
  anomaliesByType: Record<AnomalyType, number>
  /** Anomalies by severity */
  anomaliesBySeverity: Record<AnomalySeverity, number>
  /** Current window stats for each metric */
  metricStats: {
    latency: WindowStats | null
    cost: WindowStats | null
    tokenUsage: WindowStats | null
    errorRate: WindowStats | null
    successRate: WindowStats | null
    cacheHitRate: WindowStats | null
  }
  /** Per-model statistics (if enabled) */
  modelStats?: Map<string, {
    latency: WindowStats | null
    cost: WindowStats | null
  }> | undefined
  /** Start time of the detector */
  startedAt: number
  /** Last observation time */
  lastObservationAt: number | null
  /** Last anomaly time */
  lastAnomalyAt: number | null
}

// =============================================================================
// Rolling Window Statistics
// =============================================================================

/**
 * Rolling window for computing statistics incrementally
 */
class RollingWindow {
  private values: number[] = []
  private sum = 0
  private sumSquared = 0
  private maxSize: number

  constructor(maxSize: number) {
    this.maxSize = maxSize
  }

  /**
   * Add a value to the window
   */
  add(value: number): void {
    this.values.push(value)
    this.sum += value
    this.sumSquared += value * value

    // Remove oldest value if window is full
    if (this.values.length > this.maxSize) {
      const removed = this.values.shift()!
      this.sum -= removed
      this.sumSquared -= removed * removed
    }
  }

  /**
   * Get current window statistics
   */
  getStats(): WindowStats {
    const count = this.values.length
    if (count === 0) {
      return {
        count: 0,
        sum: 0,
        sumSquared: 0,
        min: 0,
        max: 0,
        mean: 0,
        stdDev: 0,
        variance: 0,
      }
    }

    const mean = this.sum / count
    const variance = count > 1
      ? (this.sumSquared / count) - (mean * mean)
      : 0
    const stdDev = Math.sqrt(Math.max(0, variance)) // Clamp to avoid NaN from floating point errors

    return {
      count,
      sum: this.sum,
      sumSquared: this.sumSquared,
      min: Math.min(...this.values),
      max: Math.max(...this.values),
      mean,
      variance,
      stdDev,
    }
  }

  /**
   * Get the number of values in the window
   */
  size(): number {
    return this.values.length
  }

  /**
   * Clear the window
   */
  clear(): void {
    this.values = []
    this.sum = 0
    this.sumSquared = 0
  }
}

// =============================================================================
// AnomalyDetector Class
// =============================================================================

/**
 * Anomaly detector for AI workload metrics
 *
 * Uses rolling window statistics and standard deviation based thresholds
 * to detect anomalies in AI API usage patterns.
 */
export class AnomalyDetector {
  private config: ResolvedAnomalyDetectorConfig
  private startedAt: number

  // Global windows for aggregate metrics
  private latencyWindow: RollingWindow
  private costWindow: RollingWindow
  private tokenUsageWindow: RollingWindow
  private errorRateWindow: RollingWindow
  private successRateWindow: RollingWindow
  private cacheHitRateWindow: RollingWindow

  // Per-model windows
  private modelLatencyWindows: Map<string, RollingWindow> = new Map()
  private modelCostWindows: Map<string, RollingWindow> = new Map()

  // Statistics tracking
  private totalObservations = 0
  private totalAnomalies = 0
  private anomaliesByType: Record<AnomalyType, number> = {
    latency_spike: 0,
    cost_anomaly: 0,
    error_rate_violation: 0,
    token_usage_outlier: 0,
    performance_degradation: 0,
    success_rate_drop: 0,
    cache_hit_drop: 0,
  }
  private anomaliesBySeverity: Record<AnomalySeverity, number> = {
    info: 0,
    warning: 0,
    critical: 0,
  }
  private lastObservationAt: number | null = null
  private lastAnomalyAt: number | null = null

  // Deduplication tracking
  private lastAlertTime: Map<string, number> = new Map()

  constructor(config: AnomalyDetectorConfig = {}) {
    this.config = {
      windowSize: config.windowSize ?? 100,
      thresholds: { ...DEFAULT_ANOMALY_THRESHOLDS, ...config.thresholds },
      onAnomaly: config.onAnomaly,
      logAnomalies: config.logAnomalies ?? true,
      dedupeIntervalMs: config.dedupeIntervalMs ?? 300000,
      perModelStats: config.perModelStats ?? true,
    }

    this.startedAt = Date.now()

    // Initialize windows
    this.latencyWindow = new RollingWindow(this.config.windowSize)
    this.costWindow = new RollingWindow(this.config.windowSize)
    this.tokenUsageWindow = new RollingWindow(this.config.windowSize)
    this.errorRateWindow = new RollingWindow(this.config.windowSize)
    this.successRateWindow = new RollingWindow(this.config.windowSize)
    this.cacheHitRateWindow = new RollingWindow(this.config.windowSize)
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Observe a new data point and check for anomalies
   *
   * @param observation - The observation data
   * @returns Array of detected anomalies (empty if none)
   */
  async observe(observation: AnomalyObservation): Promise<AnomalyEvent[]> {
    const anomalies: AnomalyEvent[] = []
    const timestamp = observation.timestamp ?? Date.now()

    this.totalObservations++
    this.lastObservationAt = timestamp

    // Get or create per-model windows
    let modelLatencyWindow: RollingWindow | undefined
    let modelCostWindow: RollingWindow | undefined

    if (this.config.perModelStats && observation.modelId) {
      if (!this.modelLatencyWindows.has(observation.modelId)) {
        this.modelLatencyWindows.set(observation.modelId, new RollingWindow(this.config.windowSize))
        this.modelCostWindows.set(observation.modelId, new RollingWindow(this.config.windowSize))
      }
      modelLatencyWindow = this.modelLatencyWindows.get(observation.modelId)
      modelCostWindow = this.modelCostWindows.get(observation.modelId)
    }

    // Check latency
    if (observation.latencyMs !== undefined) {
      const latencyAnomaly = this.checkLatency(
        observation.latencyMs,
        observation,
        timestamp,
        modelLatencyWindow
      )
      if (latencyAnomaly) anomalies.push(latencyAnomaly)

      // Add to windows after checking
      this.latencyWindow.add(observation.latencyMs)
      modelLatencyWindow?.add(observation.latencyMs)
    }

    // Check cost
    if (observation.costUSD !== undefined) {
      const costAnomaly = this.checkCost(
        observation.costUSD,
        observation,
        timestamp,
        modelCostWindow
      )
      if (costAnomaly) anomalies.push(costAnomaly)

      // Add to windows after checking
      this.costWindow.add(observation.costUSD)
      modelCostWindow?.add(observation.costUSD)
    }

    // Check token usage
    if (observation.tokenUsage !== undefined) {
      const tokenAnomaly = this.checkTokenUsage(observation.tokenUsage, observation, timestamp)
      if (tokenAnomaly) anomalies.push(tokenAnomaly)
      this.tokenUsageWindow.add(observation.tokenUsage)
    }

    // Check error rate
    if (observation.errorRate !== undefined) {
      const errorAnomaly = this.checkErrorRate(observation.errorRate, observation, timestamp)
      if (errorAnomaly) anomalies.push(errorAnomaly)
      this.errorRateWindow.add(observation.errorRate)
    }

    // Check success rate
    if (observation.successRate !== undefined) {
      const successAnomaly = this.checkSuccessRate(observation.successRate, observation, timestamp)
      if (successAnomaly) anomalies.push(successAnomaly)
      this.successRateWindow.add(observation.successRate)
    }

    // Check cache hit rate
    if (observation.cacheHitRate !== undefined) {
      const cacheAnomaly = this.checkCacheHitRate(observation.cacheHitRate, observation, timestamp)
      if (cacheAnomaly) anomalies.push(cacheAnomaly)
      this.cacheHitRateWindow.add(observation.cacheHitRate)
    }

    // Process detected anomalies
    for (const anomaly of anomalies) {
      await this.handleAnomaly(anomaly)
    }

    return anomalies
  }

  /**
   * Get current statistics
   */
  getStats(): AnomalyDetectorStats {
    const stats: AnomalyDetectorStats = {
      totalObservations: this.totalObservations,
      totalAnomalies: this.totalAnomalies,
      anomaliesByType: { ...this.anomaliesByType },
      anomaliesBySeverity: { ...this.anomaliesBySeverity },
      metricStats: {
        latency: this.latencyWindow.size() > 0 ? this.latencyWindow.getStats() : null,
        cost: this.costWindow.size() > 0 ? this.costWindow.getStats() : null,
        tokenUsage: this.tokenUsageWindow.size() > 0 ? this.tokenUsageWindow.getStats() : null,
        errorRate: this.errorRateWindow.size() > 0 ? this.errorRateWindow.getStats() : null,
        successRate: this.successRateWindow.size() > 0 ? this.successRateWindow.getStats() : null,
        cacheHitRate: this.cacheHitRateWindow.size() > 0 ? this.cacheHitRateWindow.getStats() : null,
      },
      startedAt: this.startedAt,
      lastObservationAt: this.lastObservationAt,
      lastAnomalyAt: this.lastAnomalyAt,
    }

    if (this.config.perModelStats) {
      stats.modelStats = new Map()
      for (const [modelId, latencyWindow] of this.modelLatencyWindows) {
        const costWindow = this.modelCostWindows.get(modelId)
        stats.modelStats.set(modelId, {
          latency: latencyWindow.size() > 0 ? latencyWindow.getStats() : null,
          cost: costWindow && costWindow.size() > 0 ? costWindow.getStats() : null,
        })
      }
    }

    return stats
  }

  /**
   * Get window statistics for a specific metric
   */
  getMetricStats(metric: 'latency' | 'cost' | 'tokenUsage' | 'errorRate' | 'successRate' | 'cacheHitRate'): WindowStats | null {
    const windowMap = {
      latency: this.latencyWindow,
      cost: this.costWindow,
      tokenUsage: this.tokenUsageWindow,
      errorRate: this.errorRateWindow,
      successRate: this.successRateWindow,
      cacheHitRate: this.cacheHitRateWindow,
    }
    const window = windowMap[metric]
    return window.size() > 0 ? window.getStats() : null
  }

  /**
   * Get window statistics for a specific model
   */
  getModelStats(modelId: string): { latency: WindowStats | null; cost: WindowStats | null } | null {
    const latencyWindow = this.modelLatencyWindows.get(modelId)
    const costWindow = this.modelCostWindows.get(modelId)

    if (!latencyWindow && !costWindow) return null

    return {
      latency: latencyWindow && latencyWindow.size() > 0 ? latencyWindow.getStats() : null,
      cost: costWindow && costWindow.size() > 0 ? costWindow.getStats() : null,
    }
  }

  /**
   * Clear all windows and reset statistics
   */
  reset(): void {
    this.latencyWindow.clear()
    this.costWindow.clear()
    this.tokenUsageWindow.clear()
    this.errorRateWindow.clear()
    this.successRateWindow.clear()
    this.cacheHitRateWindow.clear()

    this.modelLatencyWindows.clear()
    this.modelCostWindows.clear()

    this.totalObservations = 0
    this.totalAnomalies = 0
    this.anomaliesByType = {
      latency_spike: 0,
      cost_anomaly: 0,
      error_rate_violation: 0,
      token_usage_outlier: 0,
      performance_degradation: 0,
      success_rate_drop: 0,
      cache_hit_drop: 0,
    }
    this.anomaliesBySeverity = { info: 0, warning: 0, critical: 0 }
    this.lastObservationAt = null
    this.lastAnomalyAt = null
    this.lastAlertTime.clear()
    this.startedAt = Date.now()
  }

  /**
   * Update configuration thresholds
   */
  updateThresholds(thresholds: Partial<AnomalyThresholds>): void {
    this.config.thresholds = { ...this.config.thresholds, ...thresholds }
  }

  /**
   * Set anomaly callback
   */
  setAnomalyCallback(callback: (anomaly: AnomalyEvent) => void | Promise<void>): void {
    this.config.onAnomaly = callback
  }

  // ===========================================================================
  // Private Methods - Anomaly Detection
  // ===========================================================================

  private checkLatency(
    latencyMs: number,
    observation: AnomalyObservation,
    timestamp: number,
    modelWindow?: RollingWindow
  ): AnomalyEvent | null {
    const { thresholds } = this.config

    // Check absolute threshold first
    if (latencyMs >= thresholds.absoluteLatencyThreshold) {
      return this.createAnomaly({
        type: 'latency_spike',
        severity: 'critical',
        title: 'Critical latency spike detected',
        description: `Latency of ${latencyMs}ms exceeds absolute threshold of ${thresholds.absoluteLatencyThreshold}ms`,
        currentValue: latencyMs,
        expectedValue: thresholds.absoluteLatencyThreshold,
        threshold: thresholds.absoluteLatencyThreshold,
        observation,
        timestamp,
      })
    }

    // Check standard deviation based threshold
    const globalStats = this.latencyWindow.getStats()
    const modelStats = modelWindow?.getStats()

    // Use model-specific stats if available and has enough samples
    const stats = (modelStats && modelStats.count >= thresholds.minSamplesForDetection)
      ? modelStats
      : globalStats

    if (stats.count < thresholds.minSamplesForDetection) {
      return null // Not enough data for statistical detection
    }

    if (stats.stdDev === 0) {
      return null // No variance, can't detect anomaly
    }

    const stdDeviations = (latencyMs - stats.mean) / stats.stdDev

    if (stdDeviations >= thresholds.latencyStdDevThreshold) {
      const severity: AnomalySeverity = stdDeviations >= thresholds.latencyStdDevThreshold * 1.5
        ? 'critical'
        : stdDeviations >= thresholds.latencyStdDevThreshold * 1.2
        ? 'warning'
        : 'info'

      return this.createAnomaly({
        type: 'latency_spike',
        severity,
        title: `Latency spike detected${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Latency of ${latencyMs}ms is ${stdDeviations.toFixed(1)} standard deviations above mean (${stats.mean.toFixed(1)}ms)`,
        currentValue: latencyMs,
        expectedValue: stats.mean,
        threshold: stats.mean + (thresholds.latencyStdDevThreshold * stats.stdDev),
        stdDeviations,
        windowStats: stats,
        observation,
        timestamp,
      })
    }

    return null
  }

  private checkCost(
    costUSD: number,
    observation: AnomalyObservation,
    timestamp: number,
    modelWindow?: RollingWindow
  ): AnomalyEvent | null {
    const { thresholds } = this.config

    // Check absolute threshold first
    if (costUSD >= thresholds.absoluteCostThreshold) {
      return this.createAnomaly({
        type: 'cost_anomaly',
        severity: 'critical',
        title: 'High cost request detected',
        description: `Cost of $${costUSD.toFixed(4)} exceeds threshold of $${thresholds.absoluteCostThreshold.toFixed(2)}`,
        currentValue: costUSD,
        expectedValue: thresholds.absoluteCostThreshold,
        threshold: thresholds.absoluteCostThreshold,
        observation,
        timestamp,
      })
    }

    // Check standard deviation based threshold
    const globalStats = this.costWindow.getStats()
    const modelStats = modelWindow?.getStats()

    const stats = (modelStats && modelStats.count >= thresholds.minSamplesForDetection)
      ? modelStats
      : globalStats

    if (stats.count < thresholds.minSamplesForDetection || stats.stdDev === 0) {
      return null
    }

    const stdDeviations = (costUSD - stats.mean) / stats.stdDev

    if (stdDeviations >= thresholds.costStdDevThreshold) {
      const severity: AnomalySeverity = stdDeviations >= thresholds.costStdDevThreshold * 1.5
        ? 'critical'
        : 'warning'

      return this.createAnomaly({
        type: 'cost_anomaly',
        severity,
        title: `Cost anomaly detected${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Cost of $${costUSD.toFixed(4)} is ${stdDeviations.toFixed(1)} standard deviations above mean ($${stats.mean.toFixed(4)})`,
        currentValue: costUSD,
        expectedValue: stats.mean,
        threshold: stats.mean + (thresholds.costStdDevThreshold * stats.stdDev),
        stdDeviations,
        windowStats: stats,
        observation,
        timestamp,
      })
    }

    return null
  }

  private checkTokenUsage(
    tokenUsage: number,
    observation: AnomalyObservation,
    timestamp: number
  ): AnomalyEvent | null {
    const { thresholds } = this.config
    const stats = this.tokenUsageWindow.getStats()

    if (stats.count < thresholds.minSamplesForDetection || stats.stdDev === 0) {
      return null
    }

    const stdDeviations = (tokenUsage - stats.mean) / stats.stdDev

    if (stdDeviations >= thresholds.tokenUsageStdDevThreshold) {
      const severity: AnomalySeverity = stdDeviations >= thresholds.tokenUsageStdDevThreshold * 1.5
        ? 'critical'
        : 'warning'

      return this.createAnomaly({
        type: 'token_usage_outlier',
        severity,
        title: `High token usage detected${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Token usage of ${tokenUsage} is ${stdDeviations.toFixed(1)} standard deviations above mean (${stats.mean.toFixed(0)})`,
        currentValue: tokenUsage,
        expectedValue: stats.mean,
        threshold: stats.mean + (thresholds.tokenUsageStdDevThreshold * stats.stdDev),
        stdDeviations,
        windowStats: stats,
        observation,
        timestamp,
      })
    }

    return null
  }

  private checkErrorRate(
    errorRate: number,
    observation: AnomalyObservation,
    timestamp: number
  ): AnomalyEvent | null {
    const { thresholds } = this.config

    // Check critical threshold
    if (errorRate >= thresholds.errorRateCriticalThreshold) {
      return this.createAnomaly({
        type: 'error_rate_violation',
        severity: 'critical',
        title: `Critical error rate${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Error rate of ${(errorRate * 100).toFixed(1)}% exceeds critical threshold of ${(thresholds.errorRateCriticalThreshold * 100).toFixed(1)}%`,
        currentValue: errorRate,
        expectedValue: thresholds.errorRateCriticalThreshold,
        threshold: thresholds.errorRateCriticalThreshold,
        observation,
        timestamp,
      })
    }

    // Check warning threshold
    if (errorRate >= thresholds.errorRateWarningThreshold) {
      return this.createAnomaly({
        type: 'error_rate_violation',
        severity: 'warning',
        title: `Elevated error rate${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Error rate of ${(errorRate * 100).toFixed(1)}% exceeds warning threshold of ${(thresholds.errorRateWarningThreshold * 100).toFixed(1)}%`,
        currentValue: errorRate,
        expectedValue: thresholds.errorRateWarningThreshold,
        threshold: thresholds.errorRateWarningThreshold,
        observation,
        timestamp,
      })
    }

    return null
  }

  private checkSuccessRate(
    successRate: number,
    observation: AnomalyObservation,
    timestamp: number
  ): AnomalyEvent | null {
    const { thresholds } = this.config

    // Check critical threshold
    if (successRate < thresholds.minSuccessRateCritical) {
      return this.createAnomaly({
        type: 'success_rate_drop',
        severity: 'critical',
        title: `Critical success rate drop${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Success rate of ${(successRate * 100).toFixed(1)}% is below critical threshold of ${(thresholds.minSuccessRateCritical * 100).toFixed(1)}%`,
        currentValue: successRate,
        expectedValue: thresholds.minSuccessRateCritical,
        threshold: thresholds.minSuccessRateCritical,
        observation,
        timestamp,
      })
    }

    // Check warning threshold
    if (successRate < thresholds.minSuccessRateWarning) {
      return this.createAnomaly({
        type: 'success_rate_drop',
        severity: 'warning',
        title: `Success rate below target${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Success rate of ${(successRate * 100).toFixed(1)}% is below target of ${(thresholds.minSuccessRateWarning * 100).toFixed(1)}%`,
        currentValue: successRate,
        expectedValue: thresholds.minSuccessRateWarning,
        threshold: thresholds.minSuccessRateWarning,
        observation,
        timestamp,
      })
    }

    return null
  }

  private checkCacheHitRate(
    cacheHitRate: number,
    observation: AnomalyObservation,
    timestamp: number
  ): AnomalyEvent | null {
    const { thresholds } = this.config
    const stats = this.cacheHitRateWindow.getStats()

    // Only alert if we have a baseline and the rate has dropped significantly
    if (stats.count < thresholds.minSamplesForDetection) {
      return null
    }

    // Check if current rate is below warning threshold and below mean
    if (cacheHitRate < thresholds.minCacheHitRateWarning && cacheHitRate < stats.mean * 0.8) {
      return this.createAnomaly({
        type: 'cache_hit_drop',
        severity: 'warning',
        title: `Cache hit rate drop${observation.modelId ? ` for ${observation.modelId}` : ''}`,
        description: `Cache hit rate of ${(cacheHitRate * 100).toFixed(1)}% is below target of ${(thresholds.minCacheHitRateWarning * 100).toFixed(1)}% (baseline: ${(stats.mean * 100).toFixed(1)}%)`,
        currentValue: cacheHitRate,
        expectedValue: stats.mean,
        threshold: thresholds.minCacheHitRateWarning,
        windowStats: stats,
        observation,
        timestamp,
      })
    }

    return null
  }

  // ===========================================================================
  // Private Methods - Helpers
  // ===========================================================================

  private createAnomaly(params: {
    type: AnomalyType
    severity: AnomalySeverity
    title: string
    description: string
    currentValue: number
    expectedValue: number
    threshold: number
    stdDeviations?: number | undefined
    windowStats?: WindowStats | undefined
    observation: AnomalyObservation
    timestamp: number
  }): AnomalyEvent {
    return {
      id: generateAnomalyId(),
      type: params.type,
      severity: params.severity,
      title: params.title,
      description: params.description,
      modelId: params.observation.modelId,
      providerId: params.observation.providerId,
      currentValue: params.currentValue,
      expectedValue: params.expectedValue,
      threshold: params.threshold,
      stdDeviations: params.stdDeviations,
      windowStats: params.windowStats,
      timestamp: params.timestamp,
      context: params.observation.context,
    }
  }

  private shouldSendAlert(anomaly: AnomalyEvent): boolean {
    const key = `${anomaly.type}:${anomaly.modelId ?? 'global'}:${anomaly.severity}`
    const lastTime = this.lastAlertTime.get(key)
    const now = Date.now()

    // Always send critical alerts
    if (anomaly.severity === 'critical') {
      this.lastAlertTime.set(key, now)
      return true
    }

    // Dedupe non-critical alerts
    if (lastTime && now - lastTime < this.config.dedupeIntervalMs) {
      return false
    }

    this.lastAlertTime.set(key, now)
    return true
  }

  private async handleAnomaly(anomaly: AnomalyEvent): Promise<void> {
    // Update statistics
    this.totalAnomalies++
    this.anomaliesByType[anomaly.type]++
    this.anomaliesBySeverity[anomaly.severity]++
    this.lastAnomalyAt = anomaly.timestamp

    // Log if enabled
    if (this.config.logAnomalies) {
      logger.warn('ai_anomaly_detected', {
        anomaly_id: anomaly.id,
        type: anomaly.type,
        severity: anomaly.severity,
        title: anomaly.title,
        modelId: anomaly.modelId,
        currentValue: anomaly.currentValue,
        expectedValue: anomaly.expectedValue,
        threshold: anomaly.threshold,
      })
    }

    // Call callback if set and alert should be sent
    if (this.config.onAnomaly && this.shouldSendAlert(anomaly)) {
      try {
        await this.config.onAnomaly(anomaly)
      } catch (error) {
        logger.error('ai_anomaly_callback_error', {
          anomaly_id: anomaly.id,
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
 * Generate a unique anomaly ID
 */
function generateAnomalyId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `anomaly_${timestamp}_${random}`
}

/**
 * Create an anomaly detector instance
 *
 * @param config - Configuration options
 * @returns AnomalyDetector instance
 *
 * @example
 * ```typescript
 * const detector = createAnomalyDetector({
 *   windowSize: 100,
 *   thresholds: {
 *     latencyStdDevThreshold: 2.5,
 *     errorRateCriticalThreshold: 0.1,
 *   },
 *   onAnomaly: async (anomaly) => {
 *     await sendSlackAlert(anomaly)
 *   },
 * })
 * ```
 */
export function createAnomalyDetector(config: AnomalyDetectorConfig = {}): AnomalyDetector {
  return new AnomalyDetector(config)
}

/**
 * Create an anomaly detector with webhook integration
 *
 * @param webhookUrl - URL to send anomaly notifications
 * @param config - Additional configuration
 * @returns AnomalyDetector with webhook callback
 */
export function createAnomalyDetectorWithWebhook(
  webhookUrl: string,
  config: Omit<AnomalyDetectorConfig, 'onAnomaly'> = {}
): AnomalyDetector {
  return new AnomalyDetector({
    ...config,
    onAnomaly: async (anomaly) => {
      try {
        await fetch(webhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...anomaly,
            timestampIso: new Date(anomaly.timestamp).toISOString(),
          }),
        })
      } catch (error) {
        logger.error('ai_anomaly_webhook_error', {
          anomaly_id: anomaly.id,
          webhook_url: webhookUrl,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    },
  })
}

// =============================================================================
// Integration Helpers
// =============================================================================

/**
 * Create an observation from AIMetrics data
 */
export function createObservationFromMetrics(metrics: {
  modelId?: string | undefined
  providerId?: string | undefined
  latency?: { avg?: number | undefined; p50?: number | undefined; p99?: number | undefined } | undefined
  errorRate?: number | undefined
  tokens?: { avgTotalTokens?: number | undefined; totalTokens?: number | undefined } | undefined
  cost?: { avgCostUSD?: number | undefined; totalCostUSD?: number | undefined } | undefined
  cacheHitRatio?: number | undefined
  successRate?: number | undefined
  totalRequests?: number | undefined
}): AnomalyObservation {
  return {
    modelId: metrics.modelId,
    providerId: metrics.providerId,
    latencyMs: metrics.latency?.avg ?? metrics.latency?.p50,
    errorRate: metrics.errorRate,
    tokenUsage: metrics.tokens?.avgTotalTokens ?? metrics.tokens?.totalTokens,
    costUSD: metrics.cost?.avgCostUSD ?? metrics.cost?.totalCostUSD,
    cacheHitRate: metrics.cacheHitRatio,
    successRate: metrics.successRate ?? (metrics.errorRate !== undefined ? 1 - metrics.errorRate : undefined),
    requestCount: metrics.totalRequests,
  }
}

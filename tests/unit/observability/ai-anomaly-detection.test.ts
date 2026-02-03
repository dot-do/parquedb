/**
 * AI Anomaly Detection Tests
 *
 * Comprehensive tests for the AI anomaly detection module:
 * - Rolling window statistics
 * - Latency spike detection
 * - Cost anomaly detection
 * - Error rate threshold violations
 * - Token usage outlier detection
 * - Success rate drops
 * - Cache hit rate monitoring
 * - Callback integration
 * - Deduplication
 *
 * Issue: parquedb-xxu2.19 - Add anomaly detection for AI workload metrics
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

import {
  AnomalyDetector,
  createAnomalyDetector,
  createAnomalyDetectorWithWebhook,
  createObservationFromMetrics,
  DEFAULT_ANOMALY_THRESHOLDS,
  type AnomalyEvent,
  type AnomalyObservation,
  type AnomalyThresholds,
} from '../../../src/observability/ai/anomaly-detection'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a series of normal observations to establish baseline
 */
function createBaselineObservations(
  count: number,
  options: {
    meanLatency?: number
    latencyStdDev?: number
    meanCost?: number
    costStdDev?: number
    meanTokens?: number
    tokensStdDev?: number
  } = {}
): AnomalyObservation[] {
  const meanLatency = options.meanLatency ?? 500
  const latencyStdDev = options.latencyStdDev ?? 100
  const meanCost = options.meanCost ?? 0.01
  const costStdDev = options.costStdDev ?? 0.002
  const meanTokens = options.meanTokens ?? 500
  const tokensStdDev = options.tokensStdDev ?? 100

  const observations: AnomalyObservation[] = []
  for (let i = 0; i < count; i++) {
    // Generate normally distributed random values
    observations.push({
      modelId: 'gpt-4',
      providerId: 'openai',
      latencyMs: meanLatency + (Math.random() - 0.5) * 2 * latencyStdDev,
      costUSD: meanCost + (Math.random() - 0.5) * 2 * costStdDev,
      tokenUsage: Math.round(meanTokens + (Math.random() - 0.5) * 2 * tokensStdDev),
      errorRate: 0.02 + Math.random() * 0.02, // 2-4% error rate
      successRate: 0.96 + Math.random() * 0.02, // 96-98% success rate
      cacheHitRate: 0.6 + Math.random() * 0.1, // 60-70% cache hit rate
    })
  }
  return observations
}

// =============================================================================
// Rolling Window Statistics Tests
// =============================================================================

describe('AnomalyDetector - Rolling Window Statistics', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({ windowSize: 10 })
  })

  it('should initialize with empty statistics', () => {
    const stats = detector.getStats()

    expect(stats.totalObservations).toBe(0)
    expect(stats.totalAnomalies).toBe(0)
    expect(stats.metricStats.latency).toBeNull()
    expect(stats.metricStats.cost).toBeNull()
  })

  it('should track observations and compute statistics', async () => {
    const observations = createBaselineObservations(20, {
      meanLatency: 500,
      latencyStdDev: 50,
    })

    for (const obs of observations) {
      await detector.observe(obs)
    }

    const stats = detector.getStats()
    expect(stats.totalObservations).toBe(20)

    const latencyStats = stats.metricStats.latency
    expect(latencyStats).not.toBeNull()
    expect(latencyStats!.count).toBe(10) // Window size is 10
    expect(latencyStats!.mean).toBeGreaterThan(400)
    expect(latencyStats!.mean).toBeLessThan(600)
    expect(latencyStats!.stdDev).toBeGreaterThan(0)
  })

  it('should maintain rolling window of correct size', async () => {
    const detector = createAnomalyDetector({ windowSize: 5 })

    // Add 10 observations
    for (let i = 0; i < 10; i++) {
      await detector.observe({ latencyMs: 100 + i * 10 })
    }

    const latencyStats = detector.getMetricStats('latency')
    expect(latencyStats).not.toBeNull()
    expect(latencyStats!.count).toBe(5)

    // Window should contain last 5 values: 150, 160, 170, 180, 190
    // Mean = (150+160+170+180+190)/5 = 170
    expect(latencyStats!.mean).toBeCloseTo(170, 1)
  })

  it('should compute standard deviation correctly', async () => {
    const detector = createAnomalyDetector({ windowSize: 5 })

    // Add values with known std dev
    const values = [100, 200, 300, 400, 500]
    for (const v of values) {
      await detector.observe({ latencyMs: v })
    }

    const stats = detector.getMetricStats('latency')
    expect(stats).not.toBeNull()

    // Mean = 300, variance = ((100-300)^2 + ... + (500-300)^2) / 5 = 20000
    // StdDev = sqrt(20000) = 141.42
    expect(stats!.mean).toBeCloseTo(300, 1)
    expect(stats!.stdDev).toBeCloseTo(141.42, 1)
  })

  it('should reset statistics correctly', async () => {
    for (let i = 0; i < 10; i++) {
      await detector.observe({ latencyMs: 100 + i * 10 })
    }

    expect(detector.getStats().totalObservations).toBe(10)

    detector.reset()

    const stats = detector.getStats()
    expect(stats.totalObservations).toBe(0)
    expect(stats.metricStats.latency).toBeNull()
  })
})

// =============================================================================
// Latency Spike Detection Tests
// =============================================================================

describe('AnomalyDetector - Latency Spike Detection', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      windowSize: 20,
      thresholds: {
        latencyStdDevThreshold: 2.5,
        absoluteLatencyThreshold: 5000,
        minSamplesForDetection: 10,
      },
    })
  })

  it('should detect latency spikes based on standard deviation', async () => {
    // Establish baseline with low latency
    const baseline = createBaselineObservations(15, {
      meanLatency: 500,
      latencyStdDev: 50, // Tight distribution
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    // Now observe a spike (5 std devs above mean)
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      latencyMs: 800, // Well above mean + 2.5*stdDev
    })

    expect(anomalies.length).toBeGreaterThan(0)
    expect(anomalies[0]!.type).toBe('latency_spike')
    expect(anomalies[0]!.currentValue).toBe(800)
  })

  it('should detect absolute latency threshold violations', async () => {
    // Even without baseline, should detect absolute threshold
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      latencyMs: 6000, // Above 5000ms absolute threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('latency_spike')
    expect(anomalies[0]!.severity).toBe('critical')
  })

  it('should not flag normal latency variations', async () => {
    const baseline = createBaselineObservations(15, {
      meanLatency: 500,
      latencyStdDev: 100, // Wider distribution
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    // Observe a value within 1 std dev
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      latencyMs: 550,
    })

    expect(anomalies.length).toBe(0)
  })

  it('should not detect anomalies with insufficient samples', async () => {
    // Only add 5 observations (below minSamplesForDetection)
    for (let i = 0; i < 5; i++) {
      await detector.observe({ latencyMs: 500 })
    }

    // Try to detect with a high latency
    const anomalies = await detector.observe({
      latencyMs: 1000, // Would be anomaly with enough baseline
    })

    // Should not detect due to insufficient samples (unless absolute threshold)
    const latencyAnomalies = anomalies.filter(a => a.type === 'latency_spike')
    expect(latencyAnomalies.length).toBe(0)
  })

  it('should include window stats in anomaly events', async () => {
    const baseline = createBaselineObservations(15, {
      meanLatency: 500,
      latencyStdDev: 50,
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    const anomalies = await detector.observe({
      latencyMs: 800,
    })

    expect(anomalies.length).toBeGreaterThan(0)
    expect(anomalies[0]!.windowStats).toBeDefined()
    expect(anomalies[0]!.stdDeviations).toBeDefined()
    expect(anomalies[0]!.stdDeviations).toBeGreaterThan(2.5)
  })
})

// =============================================================================
// Cost Anomaly Detection Tests
// =============================================================================

describe('AnomalyDetector - Cost Anomaly Detection', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      windowSize: 20,
      thresholds: {
        costStdDevThreshold: 3.0,
        absoluteCostThreshold: 0.5,
        minSamplesForDetection: 10,
      },
    })
  })

  it('should detect cost anomalies based on standard deviation', async () => {
    // Establish baseline with low cost
    const baseline = createBaselineObservations(15, {
      meanCost: 0.01,
      costStdDev: 0.002,
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    // Observe a cost spike
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      costUSD: 0.03, // Way above mean + 3*stdDev
    })

    expect(anomalies.length).toBeGreaterThan(0)
    const costAnomaly = anomalies.find(a => a.type === 'cost_anomaly')
    expect(costAnomaly).toBeDefined()
    expect(costAnomaly!.currentValue).toBe(0.03)
  })

  it('should detect absolute cost threshold violations', async () => {
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      costUSD: 0.6, // Above $0.50 absolute threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('cost_anomaly')
    expect(anomalies[0]!.severity).toBe('critical')
  })

  it('should not flag normal cost variations', async () => {
    const baseline = createBaselineObservations(15, {
      meanCost: 0.01,
      costStdDev: 0.002,
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    const anomalies = await detector.observe({
      costUSD: 0.012, // Normal variation
    })

    const costAnomalies = anomalies.filter(a => a.type === 'cost_anomaly')
    expect(costAnomalies.length).toBe(0)
  })
})

// =============================================================================
// Error Rate Threshold Tests
// =============================================================================

describe('AnomalyDetector - Error Rate Thresholds', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      thresholds: {
        errorRateWarningThreshold: 0.05,
        errorRateCriticalThreshold: 0.1,
      },
    })
  })

  it('should detect warning level error rate', async () => {
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      errorRate: 0.07, // 7% - above warning threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('error_rate_violation')
    expect(anomalies[0]!.severity).toBe('warning')
  })

  it('should detect critical level error rate', async () => {
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      errorRate: 0.15, // 15% - above critical threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('error_rate_violation')
    expect(anomalies[0]!.severity).toBe('critical')
  })

  it('should not flag normal error rates', async () => {
    const anomalies = await detector.observe({
      errorRate: 0.03, // 3% - normal
    })

    const errorAnomalies = anomalies.filter(a => a.type === 'error_rate_violation')
    expect(errorAnomalies.length).toBe(0)
  })
})

// =============================================================================
// Token Usage Outlier Tests
// =============================================================================

describe('AnomalyDetector - Token Usage Outliers', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      windowSize: 20,
      thresholds: {
        tokenUsageStdDevThreshold: 2.5,
        minSamplesForDetection: 10,
      },
    })
  })

  it('should detect token usage outliers', async () => {
    // Establish baseline
    const baseline = createBaselineObservations(15, {
      meanTokens: 500,
      tokensStdDev: 50,
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    // Observe a token spike
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      tokenUsage: 800, // Well above mean + 2.5*stdDev
    })

    expect(anomalies.length).toBeGreaterThan(0)
    const tokenAnomaly = anomalies.find(a => a.type === 'token_usage_outlier')
    expect(tokenAnomaly).toBeDefined()
  })

  it('should not flag normal token variations', async () => {
    const baseline = createBaselineObservations(15, {
      meanTokens: 500,
      tokensStdDev: 100,
    })

    for (const obs of baseline) {
      await detector.observe(obs)
    }

    const anomalies = await detector.observe({
      tokenUsage: 550,
    })

    const tokenAnomalies = anomalies.filter(a => a.type === 'token_usage_outlier')
    expect(tokenAnomalies.length).toBe(0)
  })
})

// =============================================================================
// Success Rate & Cache Hit Rate Tests
// =============================================================================

describe('AnomalyDetector - Success Rate', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      thresholds: {
        minSuccessRateWarning: 0.95,
        minSuccessRateCritical: 0.9,
      },
    })
  })

  it('should detect warning level success rate drop', async () => {
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      successRate: 0.93, // Below 95% warning threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('success_rate_drop')
    expect(anomalies[0]!.severity).toBe('warning')
  })

  it('should detect critical level success rate drop', async () => {
    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      successRate: 0.85, // Below 90% critical threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('success_rate_drop')
    expect(anomalies[0]!.severity).toBe('critical')
  })

  it('should not flag healthy success rates', async () => {
    const anomalies = await detector.observe({
      successRate: 0.98,
    })

    const successAnomalies = anomalies.filter(a => a.type === 'success_rate_drop')
    expect(successAnomalies.length).toBe(0)
  })
})

describe('AnomalyDetector - Cache Hit Rate', () => {
  let detector: AnomalyDetector

  beforeEach(() => {
    detector = createAnomalyDetector({
      windowSize: 20,
      thresholds: {
        minCacheHitRateWarning: 0.5,
        minSamplesForDetection: 10,
      },
    })
  })

  it('should detect cache hit rate drop after baseline established', async () => {
    // Establish good baseline
    for (let i = 0; i < 15; i++) {
      await detector.observe({ cacheHitRate: 0.7 + Math.random() * 0.1 })
    }

    // Observe a drop
    const anomalies = await detector.observe({
      cacheHitRate: 0.3, // Well below baseline and threshold
    })

    expect(anomalies.length).toBe(1)
    expect(anomalies[0]!.type).toBe('cache_hit_drop')
    expect(anomalies[0]!.severity).toBe('warning')
  })

  it('should not alert without sufficient baseline', async () => {
    const anomalies = await detector.observe({
      cacheHitRate: 0.3,
    })

    const cacheAnomalies = anomalies.filter(a => a.type === 'cache_hit_drop')
    expect(cacheAnomalies.length).toBe(0)
  })
})

// =============================================================================
// Per-Model Statistics Tests
// =============================================================================

describe('AnomalyDetector - Per-Model Statistics', () => {
  it('should track per-model statistics when enabled', async () => {
    const detector = createAnomalyDetector({
      windowSize: 10,
      perModelStats: true,
    })

    // Add observations for different models
    for (let i = 0; i < 10; i++) {
      await detector.observe({ modelId: 'gpt-4', latencyMs: 500 + i * 10 })
      await detector.observe({ modelId: 'claude-3', latencyMs: 300 + i * 5 })
    }

    const gpt4Stats = detector.getModelStats('gpt-4')
    const claudeStats = detector.getModelStats('claude-3')

    expect(gpt4Stats).not.toBeNull()
    expect(claudeStats).not.toBeNull()

    expect(gpt4Stats!.latency!.mean).toBeGreaterThan(claudeStats!.latency!.mean)
  })

  it('should use model-specific stats for anomaly detection', async () => {
    const detector = createAnomalyDetector({
      windowSize: 15,
      perModelStats: true,
      thresholds: {
        latencyStdDevThreshold: 2.5,
        minSamplesForDetection: 10,
      },
    })

    // GPT-4: high latency baseline
    for (let i = 0; i < 12; i++) {
      await detector.observe({ modelId: 'gpt-4', latencyMs: 1000 + Math.random() * 100 })
    }

    // Claude: low latency baseline
    for (let i = 0; i < 12; i++) {
      await detector.observe({ modelId: 'claude-3', latencyMs: 300 + Math.random() * 50 })
    }

    // This would be normal for GPT-4 but anomaly for Claude
    const anomalies = await detector.observe({
      modelId: 'claude-3',
      latencyMs: 600, // High for Claude's baseline
    })

    const latencyAnomalies = anomalies.filter(a => a.type === 'latency_spike')
    expect(latencyAnomalies.length).toBe(1)
    expect(latencyAnomalies[0]!.modelId).toBe('claude-3')
  })
})

// =============================================================================
// Callback & Deduplication Tests
// =============================================================================

describe('AnomalyDetector - Callbacks', () => {
  it('should call onAnomaly callback when anomaly detected', async () => {
    const detectedAnomalies: AnomalyEvent[] = []

    const detector = createAnomalyDetector({
      onAnomaly: (anomaly) => {
        detectedAnomalies.push(anomaly)
      },
    })

    await detector.observe({
      errorRate: 0.15, // Will trigger critical alert
    })

    expect(detectedAnomalies.length).toBe(1)
    expect(detectedAnomalies[0]!.type).toBe('error_rate_violation')
  })

  it('should handle async callbacks', async () => {
    vi.useFakeTimers()
    try {
      let callbackCalled = false

      const detector = createAnomalyDetector({
        onAnomaly: async () => {
          await vi.advanceTimersByTimeAsync(10)
          callbackCalled = true
        },
      })

      await detector.observe({
        errorRate: 0.15,
      })

      expect(callbackCalled).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it('should deduplicate alerts based on type and model', async () => {
    const detectedAnomalies: AnomalyEvent[] = []

    const detector = createAnomalyDetector({
      dedupeIntervalMs: 1000,
      onAnomaly: (anomaly) => {
        detectedAnomalies.push(anomaly)
      },
    })

    // First alert should go through
    await detector.observe({
      modelId: 'gpt-4',
      errorRate: 0.07, // Warning level
    })

    expect(detectedAnomalies.length).toBe(1)

    // Second identical alert should be deduped
    await detector.observe({
      modelId: 'gpt-4',
      errorRate: 0.07,
    })

    expect(detectedAnomalies.length).toBe(1) // Still 1

    // Different model should still alert
    await detector.observe({
      modelId: 'claude-3',
      errorRate: 0.07,
    })

    expect(detectedAnomalies.length).toBe(2)
  })

  it('should always send critical alerts', async () => {
    const detectedAnomalies: AnomalyEvent[] = []

    const detector = createAnomalyDetector({
      dedupeIntervalMs: 60000, // Long dedupe interval
      onAnomaly: (anomaly) => {
        detectedAnomalies.push(anomaly)
      },
    })

    // First critical alert
    await detector.observe({ errorRate: 0.15 })
    expect(detectedAnomalies.length).toBe(1)

    // Second critical alert - should still go through
    await detector.observe({ errorRate: 0.15 })
    expect(detectedAnomalies.length).toBe(2)
  })
})

// =============================================================================
// Integration Helper Tests
// =============================================================================

describe('createObservationFromMetrics', () => {
  it('should create observation from metrics data', () => {
    const observation = createObservationFromMetrics({
      modelId: 'gpt-4',
      providerId: 'openai',
      latency: { avg: 500, p50: 450, p99: 1200 },
      errorRate: 0.05,
      tokens: { avgTotalTokens: 800, totalTokens: 8000 },
      cost: { avgCostUSD: 0.02, totalCostUSD: 0.2 },
      cacheHitRatio: 0.6,
      totalRequests: 100,
    })

    expect(observation.modelId).toBe('gpt-4')
    expect(observation.providerId).toBe('openai')
    expect(observation.latencyMs).toBe(500)
    expect(observation.errorRate).toBe(0.05)
    expect(observation.tokenUsage).toBe(800)
    expect(observation.costUSD).toBe(0.02)
    expect(observation.cacheHitRate).toBe(0.6)
    expect(observation.requestCount).toBe(100)
    expect(observation.successRate).toBe(0.95)
  })

  it('should handle missing fields gracefully', () => {
    const observation = createObservationFromMetrics({
      modelId: 'gpt-4',
    })

    expect(observation.modelId).toBe('gpt-4')
    expect(observation.latencyMs).toBeUndefined()
    expect(observation.errorRate).toBeUndefined()
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory Functions', () => {
  describe('createAnomalyDetector', () => {
    it('should create detector with default config', () => {
      const detector = createAnomalyDetector()
      expect(detector).toBeInstanceOf(AnomalyDetector)
    })

    it('should create detector with custom thresholds', () => {
      const detector = createAnomalyDetector({
        windowSize: 50,
        thresholds: {
          latencyStdDevThreshold: 3.0,
          errorRateCriticalThreshold: 0.2,
        },
      })

      const stats = detector.getStats()
      expect(stats.startedAt).toBeGreaterThan(0)
    })
  })

  describe('createAnomalyDetectorWithWebhook', () => {
    it('should create detector with webhook callback', async () => {
      const fetchMock = vi.fn().mockResolvedValue({ ok: true })
      vi.stubGlobal('fetch', fetchMock)

      const detector = createAnomalyDetectorWithWebhook('https://example.com/webhook', {
        logAnomalies: false,
      })

      await detector.observe({
        errorRate: 0.15,
      })

      expect(fetchMock).toHaveBeenCalledWith(
        'https://example.com/webhook',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        })
      )

      vi.unstubAllGlobals()
    })
  })
})

// =============================================================================
// Statistics Tracking Tests
// =============================================================================

describe('AnomalyDetector - Statistics', () => {
  it('should track anomalies by type', async () => {
    const detector = createAnomalyDetector({
      logAnomalies: false,
    })

    await detector.observe({ errorRate: 0.15 })
    await detector.observe({ successRate: 0.85 })
    await detector.observe({ latencyMs: 15000 }) // Absolute threshold

    const stats = detector.getStats()

    expect(stats.totalAnomalies).toBe(3)
    expect(stats.anomaliesByType.error_rate_violation).toBe(1)
    expect(stats.anomaliesByType.success_rate_drop).toBe(1)
    expect(stats.anomaliesByType.latency_spike).toBe(1)
  })

  it('should track anomalies by severity', async () => {
    const detector = createAnomalyDetector({
      logAnomalies: false,
    })

    await detector.observe({ errorRate: 0.07 }) // Warning
    await detector.observe({ errorRate: 0.15 }) // Critical
    await detector.observe({ errorRate: 0.15 }) // Critical

    const stats = detector.getStats()

    expect(stats.anomaliesBySeverity.warning).toBe(1)
    expect(stats.anomaliesBySeverity.critical).toBe(2)
  })

  it('should track last observation and anomaly times', async () => {
    const detector = createAnomalyDetector({
      logAnomalies: false,
    })

    const before = Date.now()
    await detector.observe({ latencyMs: 100 })
    await detector.observe({ errorRate: 0.15 })
    const after = Date.now()

    const stats = detector.getStats()

    expect(stats.lastObservationAt).toBeGreaterThanOrEqual(before)
    expect(stats.lastObservationAt).toBeLessThanOrEqual(after)
    expect(stats.lastAnomalyAt).toBeGreaterThanOrEqual(before)
    expect(stats.lastAnomalyAt).toBeLessThanOrEqual(after)
  })
})

// =============================================================================
// Threshold Configuration Tests
// =============================================================================

describe('AnomalyDetector - Threshold Configuration', () => {
  it('should use default thresholds', () => {
    expect(DEFAULT_ANOMALY_THRESHOLDS.latencyStdDevThreshold).toBe(2.5)
    expect(DEFAULT_ANOMALY_THRESHOLDS.costStdDevThreshold).toBe(3.0)
    expect(DEFAULT_ANOMALY_THRESHOLDS.errorRateWarningThreshold).toBe(0.05)
    expect(DEFAULT_ANOMALY_THRESHOLDS.errorRateCriticalThreshold).toBe(0.1)
    expect(DEFAULT_ANOMALY_THRESHOLDS.minSamplesForDetection).toBe(10)
  })

  it('should allow updating thresholds at runtime', async () => {
    const detector = createAnomalyDetector({
      thresholds: {
        errorRateWarningThreshold: 0.05,
      },
      logAnomalies: false,
    })

    // Below threshold initially
    let anomalies = await detector.observe({ errorRate: 0.03 })
    expect(anomalies.length).toBe(0)

    // Update threshold to lower value
    detector.updateThresholds({ errorRateWarningThreshold: 0.02 })

    // Now 0.03 is above threshold
    anomalies = await detector.observe({ errorRate: 0.03 })
    expect(anomalies.length).toBe(1)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('AnomalyDetector - Edge Cases', () => {
  it('should handle zero variance gracefully', async () => {
    const detector = createAnomalyDetector({
      windowSize: 10,
      thresholds: { minSamplesForDetection: 5 },
    })

    // All same values -> zero variance
    for (let i = 0; i < 10; i++) {
      await detector.observe({ latencyMs: 500 })
    }

    // Should not throw or detect false positive
    const anomalies = await detector.observe({ latencyMs: 600 })

    // No statistical anomaly possible with zero variance
    // (only absolute threshold could trigger)
    const latencyAnomalies = anomalies.filter(a => a.type === 'latency_spike')
    expect(latencyAnomalies.length).toBe(0)
  })

  it('should handle negative values gracefully', async () => {
    const detector = createAnomalyDetector()

    // Should not throw
    const anomalies = await detector.observe({
      latencyMs: -100, // Invalid but should handle gracefully
      costUSD: -0.01,
    })

    expect(detector.getStats().totalObservations).toBe(1)
  })

  it('should handle very large values', async () => {
    const detector = createAnomalyDetector()

    const anomalies = await detector.observe({
      latencyMs: Number.MAX_SAFE_INTEGER,
      tokenUsage: Number.MAX_SAFE_INTEGER,
    })

    // Should trigger absolute threshold
    const latencyAnomalies = anomalies.filter(a => a.type === 'latency_spike')
    expect(latencyAnomalies.length).toBe(1)
  })

  it('should include model and provider in anomaly events', async () => {
    const detector = createAnomalyDetector()

    const anomalies = await detector.observe({
      modelId: 'gpt-4',
      providerId: 'openai',
      errorRate: 0.15,
    })

    expect(anomalies[0]!.modelId).toBe('gpt-4')
    expect(anomalies[0]!.providerId).toBe('openai')
  })

  it('should include context in anomaly events', async () => {
    const detector = createAnomalyDetector()

    const anomalies = await detector.observe({
      errorRate: 0.15,
      context: { requestId: 'req-123', userId: 'user-456' },
    })

    expect(anomalies[0]!.context).toEqual({ requestId: 'req-123', userId: 'user-456' })
  })
})

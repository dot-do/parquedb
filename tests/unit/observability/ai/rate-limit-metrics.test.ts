/**
 * Tests for AI Rate Limit Metrics
 *
 * Tests the rate limiting metrics tracker for AI workloads including:
 * - Tokens per minute/hour by model
 * - Cost burn rate (USD per hour)
 * - Request rate per model/provider
 * - Alerting thresholds and alerts
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  RateLimitMetrics,
  createRateLimitMetrics,
  DEFAULT_RATE_LIMIT_THRESHOLDS,
  type RateLimitMetricsConfig,
  type RateLimitThresholds,
  type RateLimitAlert,
  type RateSnapshot,
  type RateLimitObservation,
} from '../../../../src/observability/ai/rate-limit-metrics'

describe('RateLimitMetrics', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-02-03T10:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const metrics = new RateLimitMetrics()
      expect(metrics).toBeInstanceOf(RateLimitMetrics)
    })

    it('should accept custom configuration', () => {
      const config: RateLimitMetricsConfig = {
        windowSizeMs: 30000, // 30 seconds
        thresholds: {
          tokensPerMinute: { warning: 5000, critical: 10000 },
          tokensPerHour: { warning: 50000, critical: 100000 },
          costPerHour: { warning: 1.0, critical: 5.0 },
          requestsPerMinute: { warning: 50, critical: 100 },
        },
      }

      const metrics = new RateLimitMetrics(config)
      const resolvedConfig = metrics.getConfig()

      expect(resolvedConfig.windowSizeMs).toBe(30000)
      expect(resolvedConfig.thresholds.tokensPerMinute.warning).toBe(5000)
    })
  })

  describe('createRateLimitMetrics factory', () => {
    it('should create a RateLimitMetrics instance', () => {
      const metrics = createRateLimitMetrics()
      expect(metrics).toBeInstanceOf(RateLimitMetrics)
    })
  })

  describe('observe', () => {
    it('should record an observation', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const stats = metrics.getStats()
      expect(stats.totalObservations).toBe(1)
    })

    it('should track tokens by model', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 200,
        completionTokens: 100,
        costUSD: 0.02,
      })

      const snapshot = metrics.getSnapshot('gpt-4', 'openai')
      expect(snapshot).toBeDefined()
      expect(snapshot!.totalTokens).toBe(450) // 100+50+200+100
    })

    it('should track separate models independently', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      metrics.observe({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        promptTokens: 200,
        completionTokens: 100,
        costUSD: 0.02,
      })

      const gpt4Snapshot = metrics.getSnapshot('gpt-4', 'openai')
      const claudeSnapshot = metrics.getSnapshot('claude-3-opus', 'anthropic')

      expect(gpt4Snapshot!.totalTokens).toBe(150)
      expect(claudeSnapshot!.totalTokens).toBe(300)
    })

    it('should return alerts when thresholds are exceeded', async () => {
      const metrics = new RateLimitMetrics({
        thresholds: {
          // With a single observation at t=0, 150 tokens extrapolates to 150*60000 tokens/min
          // So we need thresholds that would be exceeded by the extrapolated rate
          tokensPerMinute: { warning: 1000000, critical: 5000000 },
          tokensPerHour: { warning: 10000000, critical: 50000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 10000, critical: 50000 },
        },
      })

      // First observation - give it time to settle
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Advance time so we have a real window
      vi.advanceTimersByTime(10000) // 10 seconds

      // Now set lower thresholds
      metrics.updateThresholds({
        tokensPerMinute: { warning: 100, critical: 200 },
      })

      // Second observation should trigger alerts
      const alerts = await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50, // 300 total tokens in 10 seconds = 1800 tokens/min
        costUSD: 0.01,
      })

      expect(alerts.length).toBeGreaterThan(0)
      const tokenAlert = alerts.find(a => a.metric === 'tokensPerMinute')
      expect(tokenAlert).toBeDefined()
      // 300 tokens in 10 seconds = 1800 tokens/min > 200 critical
      expect(tokenAlert!.severity).toBe('critical')
    })
  })

  describe('getSnapshot', () => {
    it('should return undefined for unknown model', () => {
      const metrics = new RateLimitMetrics()
      const snapshot = metrics.getSnapshot('unknown', 'unknown')
      expect(snapshot).toBeUndefined()
    })

    it('should include rate calculations', () => {
      const metrics = new RateLimitMetrics()

      // Observe multiple times over 30 seconds
      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      vi.advanceTimersByTime(30000) // 30 seconds

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const snapshot = metrics.getSnapshot('gpt-4', 'openai')
      expect(snapshot).toBeDefined()
      expect(snapshot!.tokensPerMinute).toBeDefined()
      expect(snapshot!.tokensPerHour).toBeDefined()
      expect(snapshot!.costPerHour).toBeDefined()
      expect(snapshot!.requestsPerMinute).toBeDefined()
    })

    it('should calculate tokens per minute correctly', () => {
      const metrics = new RateLimitMetrics()

      // Observe 600 tokens per call, 2 calls in 30 seconds
      // That's 1200 tokens in 30 seconds = 2400 tokens/minute
      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 300,
        completionTokens: 300,
        costUSD: 0.01,
      })

      vi.advanceTimersByTime(30000) // 30 seconds

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 300,
        completionTokens: 300,
        costUSD: 0.01,
      })

      const snapshot = metrics.getSnapshot('gpt-4', 'openai')
      // With 2 observations over 30 seconds, rate is ~2400 tokens/min
      expect(snapshot!.tokensPerMinute).toBeGreaterThan(0)
    })
  })

  describe('getAllSnapshots', () => {
    it('should return all model snapshots', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      metrics.observe({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        promptTokens: 200,
        completionTokens: 100,
        costUSD: 0.02,
      })

      const snapshots = metrics.getAllSnapshots()
      expect(snapshots.size).toBe(2)
      expect(snapshots.has('gpt-4:openai')).toBe(true)
      expect(snapshots.has('claude-3-opus:anthropic')).toBe(true)
    })
  })

  describe('getAggregatedSnapshot', () => {
    it('should aggregate across all models', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      metrics.observe({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        promptTokens: 200,
        completionTokens: 100,
        costUSD: 0.02,
      })

      const aggregated = metrics.getAggregatedSnapshot()
      expect(aggregated.totalTokens).toBe(450) // 150 + 300
      expect(aggregated.totalCost).toBe(0.03)
      expect(aggregated.totalRequests).toBe(2)
    })
  })

  describe('thresholds and alerts', () => {
    it('should trigger warning alerts at warning threshold', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // High thresholds initially
          tokensPerMinute: { warning: 100000, critical: 500000 },
          tokensPerHour: { warning: 1000000, critical: 5000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 1000, critical: 5000 },
        },
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First observation to establish baseline (50 tokens)
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 25,
        completionTokens: 25,
        costUSD: 0.001,
      })

      // Advance 30 seconds
      vi.advanceTimersByTime(30000)

      // Lower thresholds for testing
      // 50 tokens in 30 seconds = 100 tokens/min
      // Warning at 80, critical at 200 -> should get warning (100 > 80, < 200)
      metrics.updateThresholds({
        tokensPerMinute: { warning: 80, critical: 200 },
      })

      // Second observation (50 more tokens = 100 total in 30 sec = 200 tokens/min)
      // This should trigger warning (200 > 80) but NOT critical (200 == 200 threshold)
      // Actually 200 >= 200 would be critical. Let me adjust thresholds.
      // 100 tokens in 30 seconds = 200 tokens/min
      // Warning at 150, critical at 500 -> 200 > 150 = warning
      metrics.updateThresholds({
        tokensPerMinute: { warning: 150, critical: 500 },
      })

      // Second observation
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 25,
        completionTokens: 25,
        costUSD: 0.001,
      })

      expect(alerts.length).toBeGreaterThan(0)
      expect(alerts.some(a => a.severity === 'warning')).toBe(true)
    })

    it('should trigger critical alerts at critical threshold', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // Very high thresholds initially
          tokensPerMinute: { warning: 100000, critical: 500000 },
          tokensPerHour: { warning: 1000000, critical: 5000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 1000, critical: 5000 },
        },
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First observation
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Advance time to have a real window (minimum 1 second required)
      vi.advanceTimersByTime(10000) // 10 seconds

      // Lower thresholds so we get critical alerts
      // 150 tokens in 10 sec = 900 tokens/min > 100 critical
      metrics.updateThresholds({
        tokensPerMinute: { warning: 50, critical: 100 },
      })

      // Second observation
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const criticalAlerts = alerts.filter(a => a.severity === 'critical')
      expect(criticalAlerts.length).toBeGreaterThan(0)
    })

    it('should include model info in alerts', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // Very high thresholds initially
          tokensPerMinute: { warning: 100000, critical: 500000 },
          tokensPerHour: { warning: 1000000, critical: 5000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 1000, critical: 5000 },
        },
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First observation to establish baseline
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Advance time (min 1 second required for alerts)
      vi.advanceTimersByTime(10000) // 10 seconds

      // Lower thresholds
      metrics.updateThresholds({
        tokensPerMinute: { warning: 50, critical: 100 },
      })

      // Second observation triggers alert
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      expect(alerts.length).toBeGreaterThan(0)
      expect(alerts[0].modelId).toBe('gpt-4')
      expect(alerts[0].providerId).toBe('openai')
    })

    it('should deduplicate alerts within interval', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // Use high thresholds initially to avoid instant alerts
          tokensPerMinute: { warning: 100000, critical: 500000 },
          tokensPerHour: { warning: 1000000, critical: 5000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 1000, critical: 5000 },
        },
        alertDedupeIntervalMs: 60000, // 1 minute
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First observation - no alerts with high thresholds
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Advance time to have a reasonable window
      vi.advanceTimersByTime(30000) // 30 seconds

      // Now lower thresholds to trigger alerts
      // 150 tokens in 30 seconds = 300 tokens/min
      metrics.updateThresholds({
        tokensPerMinute: { warning: 200, critical: 500 },
      })

      // This should trigger alerts
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const firstAlertCount = alerts.length
      expect(firstAlertCount).toBeGreaterThan(0)

      // Second observation within dedupe window should not trigger duplicate alerts
      vi.advanceTimersByTime(10000) // 10 more seconds

      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Same threshold alerts should be deduped
      expect(alerts.length).toBe(firstAlertCount)
    })

    it('should send alerts after dedupe interval expires', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        windowSizeMs: 120000, // 2 minute window so data persists
        thresholds: {
          // Very high thresholds initially
          tokensPerMinute: { warning: 100000, critical: 500000 },
          tokensPerHour: { warning: 1000000, critical: 5000000 },
          costPerHour: { warning: 100.0, critical: 500.0 },
          requestsPerMinute: { warning: 1000, critical: 5000 },
        },
        alertDedupeIntervalMs: 60000, // 1 minute
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First observation (no alerts yet - need time window)
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Advance time to establish window
      vi.advanceTimersByTime(10000) // 10 seconds

      // Lower thresholds to trigger alerts
      metrics.updateThresholds({
        tokensPerMinute: { warning: 50, critical: 500 },
      })

      // Second observation triggers alerts
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const firstAlertCount = alerts.length
      expect(firstAlertCount).toBeGreaterThan(0)

      // Advance past dedupe interval but not past window
      vi.advanceTimersByTime(70000) // 70 seconds (total: 80 seconds, window is 120 seconds)

      // Add another observation to keep window active and trigger new alerts
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // Should have more alerts now that dedupe has expired
      expect(alerts.length).toBeGreaterThan(firstAlertCount)
    })
  })

  describe('window management', () => {
    it('should expire old data points', () => {
      const metrics = new RateLimitMetrics({
        windowSizeMs: 60000, // 1 minute window
      })

      // Add observation
      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 1000,
        completionTokens: 500,
        costUSD: 0.10,
      })

      let snapshot = metrics.getSnapshot('gpt-4', 'openai')
      expect(snapshot!.totalTokens).toBe(1500)

      // Advance past window
      vi.advanceTimersByTime(70000) // 70 seconds

      // Add another observation to trigger cleanup
      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      snapshot = metrics.getSnapshot('gpt-4', 'openai')
      // Old data should be expired, only new observation counted
      expect(snapshot!.totalTokens).toBe(150)
    })
  })

  describe('getStats', () => {
    it('should return overall statistics', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      metrics.observe({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        promptTokens: 200,
        completionTokens: 100,
        costUSD: 0.02,
      })

      const stats = metrics.getStats()

      expect(stats.totalObservations).toBe(2)
      expect(stats.modelsTracked).toBe(2)
      expect(stats.providersTracked).toBe(2)
      expect(stats.alertsTriggered).toBeGreaterThanOrEqual(0)
    })
  })

  describe('reset', () => {
    it('should clear all data', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      let stats = metrics.getStats()
      expect(stats.totalObservations).toBe(1)

      metrics.reset()

      stats = metrics.getStats()
      expect(stats.totalObservations).toBe(0)
      expect(stats.modelsTracked).toBe(0)
    })
  })

  describe('updateThresholds', () => {
    it('should update thresholds dynamically', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // Very high thresholds to prevent alerts initially
          tokensPerMinute: { warning: 1000000, critical: 5000000 },
          tokensPerHour: { warning: 10000000, critical: 50000000 },
          costPerHour: { warning: 1000.0, critical: 5000.0 },
          requestsPerMinute: { warning: 100000, critical: 500000 },
        },
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // This observation should not trigger alerts with very high thresholds
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      expect(alerts.length).toBe(0)

      // Advance time to have a real window
      vi.advanceTimersByTime(30000) // 30 seconds

      // Update to lower thresholds
      // 150 tokens in 30 sec = 300 tokens/min
      metrics.updateThresholds({
        tokensPerMinute: { warning: 200, critical: 400 },
      })

      // Next observation should trigger alerts
      // Total: 300 tokens in 30 sec = 600 tokens/min > 400 critical
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      expect(alerts.length).toBeGreaterThan(0)
    })
  })

  describe('per-model thresholds', () => {
    it('should support model-specific thresholds', async () => {
      const alerts: RateLimitAlert[] = []
      const metrics = new RateLimitMetrics({
        thresholds: {
          // Very high global thresholds - should not trigger for any model
          tokensPerMinute: { warning: 1000000, critical: 5000000 },
          tokensPerHour: { warning: 10000000, critical: 50000000 },
          costPerHour: { warning: 1000.0, critical: 5000.0 },
          requestsPerMinute: { warning: 100000, critical: 500000 },
        },
        modelThresholds: {
          // GPT-4 has much lower thresholds
          'gpt-4:openai': {
            tokensPerMinute: { warning: 200, critical: 500 },
          },
        },
        onAlert: (alert) => {
          alerts.push(alert)
        },
      })

      // First establish a time window
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      vi.advanceTimersByTime(30000) // 30 seconds

      // GPT-4 should trigger alert (300 tokens/30s = 600 tokens/min > 200 warning)
      await metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      expect(alerts.some(a => a.modelId === 'gpt-4')).toBe(true)

      // Claude should not trigger alert (using global thresholds which are very high)
      await metrics.observe({
        modelId: 'claude-3-opus',
        providerId: 'anthropic',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      // No new alerts for Claude (global thresholds are very high)
      expect(alerts.filter(a => a.modelId === 'claude-3-opus').length).toBe(0)
    })
  })

  describe('Prometheus export', () => {
    it('should export metrics in Prometheus format', () => {
      const metrics = new RateLimitMetrics()

      metrics.observe({
        modelId: 'gpt-4',
        providerId: 'openai',
        promptTokens: 100,
        completionTokens: 50,
        costUSD: 0.01,
      })

      const prometheus = metrics.exportPrometheus()

      expect(prometheus).toContain('parquedb_ai_tokens_per_minute')
      expect(prometheus).toContain('parquedb_ai_cost_per_hour')
      expect(prometheus).toContain('parquedb_ai_requests_per_minute')
      expect(prometheus).toContain('model="gpt-4"')
      expect(prometheus).toContain('provider="openai"')
    })
  })
})

describe('DEFAULT_RATE_LIMIT_THRESHOLDS', () => {
  it('should have reasonable default values', () => {
    expect(DEFAULT_RATE_LIMIT_THRESHOLDS.tokensPerMinute.warning).toBeGreaterThan(0)
    expect(DEFAULT_RATE_LIMIT_THRESHOLDS.tokensPerMinute.critical).toBeGreaterThan(
      DEFAULT_RATE_LIMIT_THRESHOLDS.tokensPerMinute.warning
    )

    expect(DEFAULT_RATE_LIMIT_THRESHOLDS.costPerHour.warning).toBeGreaterThan(0)
    expect(DEFAULT_RATE_LIMIT_THRESHOLDS.costPerHour.critical).toBeGreaterThan(
      DEFAULT_RATE_LIMIT_THRESHOLDS.costPerHour.warning
    )
  })
})

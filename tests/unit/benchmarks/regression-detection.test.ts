/**
 * Regression Detection Tests
 *
 * Tests for the detectRegressions() function in tests/e2e/benchmarks/utils.ts
 * Validates detection of performance regressions, severity classification,
 * and graceful handling of edge cases.
 */

import { describe, it, expect } from 'vitest'
import { detectRegressions } from '../../e2e/benchmarks/utils'
import type {
  E2EBenchmarkSuiteResult,
  E2EBenchmarkConfig,
  RegressionThresholds,
} from '../../e2e/benchmarks/types'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a minimal E2EBenchmarkSuiteResult for testing
 */
function createMockResult(overrides: {
  avgLatencyMs?: number
  p95LatencyMs?: number
  overallThroughput?: number
  coldStartOverheadMs?: number
}): E2EBenchmarkSuiteResult {
  const config: E2EBenchmarkConfig = {
    url: 'https://test.example.com',
    datasets: ['test'],
    iterations: 5,
    warmup: 2,
    maxQueries: 10,
    concurrency: 1,
    output: 'table',
    verbose: false,
    includeColdStart: true,
    includeCacheTests: true,
    timeout: 30000,
  }

  return {
    config,
    metadata: {
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 1000,
      workerUrl: 'https://test.example.com',
      runnerVersion: '1.0.0',
    },
    health: {
      success: true,
      latencyMs: 50,
    },
    coldStart: overrides.coldStartOverheadMs !== undefined
      ? {
          coldLatencyMs: 200 + overrides.coldStartOverheadMs,
          warmLatencyMs: 200,
          overheadMs: overrides.coldStartOverheadMs,
          overheadPercent: (overrides.coldStartOverheadMs / 200) * 100,
          warmSamples: 5,
        }
      : undefined,
    datasets: [],
    summary: {
      totalTests: 10,
      passedTests: 10,
      failedTests: 0,
      avgLatencyMs: overrides.avgLatencyMs ?? 100,
      p95LatencyMs: overrides.p95LatencyMs ?? 150,
      overallThroughput: overrides.overallThroughput ?? 100,
    },
  }
}

// =============================================================================
// Test Cases
// =============================================================================

describe('detectRegressions', () => {
  // ---------------------------------------------------------------------------
  // Test Case 1: Detect p50 regression above threshold (20% increase)
  // ---------------------------------------------------------------------------
  describe('p50 (average) latency regression detection', () => {
    it('detects regression when p50 latency increases by more than 20%', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 125 }) // 25% increase

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(true)
      const avgLatencyMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgLatencyMetric).toBeDefined()
      expect(avgLatencyMetric?.isRegression).toBe(true)
      expect(avgLatencyMetric?.changePercent).toBeCloseTo(25, 1)
    })

    it('does not flag regression when p50 latency increase is at threshold (20%)', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 120 }) // Exactly 20% increase

      const result = detectRegressions(current, baseline)

      const avgLatencyMetric = result.metrics.find((m) => m.name === 'Average Latency')
      // At exactly 20%, it should NOT be flagged (change must be > threshold, not >=)
      expect(avgLatencyMetric?.isRegression).toBe(false)
    })

    it('does not flag regression when p50 latency increase is below threshold', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 115 }) // 15% increase

      const result = detectRegressions(current, baseline)

      const avgLatencyMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgLatencyMetric?.isRegression).toBe(false)
    })

    it('detects improvement (negative change) as non-regression', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 80 }) // 20% improvement

      const result = detectRegressions(current, baseline)

      const avgLatencyMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgLatencyMetric?.isRegression).toBe(false)
      expect(avgLatencyMetric?.changePercent).toBeCloseTo(-20, 1)
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 2: Detect p95 regression above threshold (25% default, task says 30%)
  // ---------------------------------------------------------------------------
  describe('p95 latency regression detection', () => {
    it('detects regression when p95 latency increases by more than 25%', () => {
      const baseline = createMockResult({ p95LatencyMs: 100 })
      const current = createMockResult({ p95LatencyMs: 130 }) // 30% increase

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(true)
      const p95Metric = result.metrics.find((m) => m.name === 'P95 Latency')
      expect(p95Metric).toBeDefined()
      expect(p95Metric?.isRegression).toBe(true)
      expect(p95Metric?.changePercent).toBeCloseTo(30, 1)
    })

    it('detects regression with custom 30% threshold', () => {
      const baseline = createMockResult({ p95LatencyMs: 100 })
      const current = createMockResult({ p95LatencyMs: 128 }) // 28% increase

      const customThresholds: RegressionThresholds = {
        latencyP50: 20,
        latencyP95: 30,  // 30% threshold as per task
        latencyP99: 30,
        coldStartOverhead: 50,
        cacheHitRate: -10,
        throughput: -15,
      }

      const result = detectRegressions(current, baseline, customThresholds)

      const p95Metric = result.metrics.find((m) => m.name === 'P95 Latency')
      // 28% is below 30% threshold, should not be regression
      expect(p95Metric?.isRegression).toBe(false)
    })

    it('does not flag regression when p95 latency is at threshold', () => {
      const baseline = createMockResult({ p95LatencyMs: 100 })
      const current = createMockResult({ p95LatencyMs: 125 }) // Exactly 25% increase

      const result = detectRegressions(current, baseline)

      const p95Metric = result.metrics.find((m) => m.name === 'P95 Latency')
      expect(p95Metric?.isRegression).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 3: Detect throughput regression below threshold (15% decrease)
  // ---------------------------------------------------------------------------
  describe('throughput regression detection', () => {
    it('detects regression when throughput decreases by more than 15%', () => {
      const baseline = createMockResult({ overallThroughput: 100 })
      const current = createMockResult({ overallThroughput: 80 }) // 20% decrease

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(true)
      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      expect(throughputMetric).toBeDefined()
      expect(throughputMetric?.isRegression).toBe(true)
      expect(throughputMetric?.changePercent).toBeCloseTo(-20, 1)
    })

    it('does not flag regression when throughput decrease is at threshold (15%)', () => {
      const baseline = createMockResult({ overallThroughput: 100 })
      const current = createMockResult({ overallThroughput: 85 }) // Exactly 15% decrease

      const result = detectRegressions(current, baseline)

      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      // -15% is not less than -15%, so should not be flagged
      expect(throughputMetric?.isRegression).toBe(false)
    })

    it('does not flag regression when throughput decrease is below threshold', () => {
      const baseline = createMockResult({ overallThroughput: 100 })
      const current = createMockResult({ overallThroughput: 90 }) // 10% decrease

      const result = detectRegressions(current, baseline)

      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      expect(throughputMetric?.isRegression).toBe(false)
    })

    it('detects improvement (throughput increase) as non-regression', () => {
      const baseline = createMockResult({ overallThroughput: 100 })
      const current = createMockResult({ overallThroughput: 120 }) // 20% improvement

      const result = detectRegressions(current, baseline)

      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      expect(throughputMetric?.isRegression).toBe(false)
      expect(throughputMetric?.changePercent).toBeCloseTo(20, 1)
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 4: No regression when within thresholds
  // ---------------------------------------------------------------------------
  describe('no regression when within thresholds', () => {
    it('reports no regression when all metrics are within acceptable thresholds', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 110,  // 10% increase (threshold 20%)
        p95LatencyMs: 165,  // 10% increase (threshold 25%)
        overallThroughput: 95,  // 5% decrease (threshold -15%)
      })

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(false)
      expect(result.severity).toBe('none')
      expect(result.message).toBe('All metrics within acceptable thresholds')
    })

    it('reports no regression when metrics improve', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 80,   // 20% improvement
        p95LatencyMs: 120,  // 20% improvement
        overallThroughput: 120,  // 20% improvement
      })

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(false)
      expect(result.severity).toBe('none')
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 5: Handle missing baseline gracefully
  // ---------------------------------------------------------------------------
  describe('handling missing baseline data', () => {
    it('handles missing avgLatencyMs in baseline', () => {
      const baseline = createMockResult({ avgLatencyMs: 0 }) // 0 is falsy
      const current = createMockResult({ avgLatencyMs: 100 })

      // The function checks `if (baseline.summary.avgLatencyMs && current.summary.avgLatencyMs)`
      // so 0 will be treated as "missing" and skipped
      const result = detectRegressions(current, baseline)

      const avgLatencyMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgLatencyMetric).toBeUndefined()
    })

    it('handles missing throughput in baseline', () => {
      const baseline = createMockResult({ overallThroughput: 0 })
      const current = createMockResult({ overallThroughput: 100 })

      const result = detectRegressions(current, baseline)

      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      expect(throughputMetric).toBeUndefined()
    })

    it('handles missing cold start data in baseline', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 100, coldStartOverheadMs: 50 })

      const result = detectRegressions(current, baseline)

      const coldStartMetric = result.metrics.find((m) => m.name === 'Cold Start Overhead')
      expect(coldStartMetric).toBeUndefined()
    })

    it('handles missing cold start data in current', () => {
      const baseline = createMockResult({ avgLatencyMs: 100, coldStartOverheadMs: 50 })
      const current = createMockResult({ avgLatencyMs: 100 })

      const result = detectRegressions(current, baseline)

      const coldStartMetric = result.metrics.find((m) => m.name === 'Cold Start Overhead')
      expect(coldStartMetric).toBeUndefined()
    })

    it('returns empty metrics array when all baseline data is missing', () => {
      const baseline = createMockResult({
        avgLatencyMs: 0,
        p95LatencyMs: 0,
        overallThroughput: 0,
      })
      const current = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })

      const result = detectRegressions(current, baseline)

      expect(result.metrics.length).toBe(0)
      expect(result.hasRegression).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 6: Handle partial baseline (some queries/metrics missing)
  // ---------------------------------------------------------------------------
  describe('handling partial baseline data', () => {
    it('compares only metrics available in both baseline and current', () => {
      // Baseline has avg latency but no throughput
      const baseline = createMockResult({ avgLatencyMs: 100, overallThroughput: 0 })
      // Current has both
      const current = createMockResult({ avgLatencyMs: 150, overallThroughput: 100 })

      const result = detectRegressions(current, baseline)

      // Should only have avg latency and p95 latency metrics (throughput skipped due to baseline = 0)
      const metricNames = result.metrics.map((m) => m.name)
      expect(metricNames).toContain('Average Latency')
      expect(metricNames).not.toContain('Throughput')
    })

    it('correctly identifies regression in available metrics only', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 0, // Missing throughput
      })
      const current = createMockResult({
        avgLatencyMs: 125, // 25% increase - regression
        p95LatencyMs: 160, // ~7% increase - no regression
        overallThroughput: 100,
      })

      const result = detectRegressions(current, baseline)

      expect(result.hasRegression).toBe(true)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.isRegression).toBe(true)

      const p95Metric = result.metrics.find((m) => m.name === 'P95 Latency')
      expect(p95Metric?.isRegression).toBe(false)
    })
  })

  // ---------------------------------------------------------------------------
  // Test Case 7: Severity classification (warning vs critical)
  // ---------------------------------------------------------------------------
  describe('severity classification', () => {
    it('classifies as "none" when no regressions', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })

      const result = detectRegressions(current, baseline)

      expect(result.severity).toBe('none')
      expect(result.hasRegression).toBe(false)
    })

    it('classifies as "minor" when 1 metric shows regression', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 125, // 25% increase - regression
        p95LatencyMs: 150, // no change
        overallThroughput: 100, // no change
      })

      const result = detectRegressions(current, baseline)

      expect(result.severity).toBe('minor')
      expect(result.hasRegression).toBe(true)
      expect(result.message).toContain('1 metric(s) show regression')
    })

    it('classifies as "moderate" when 2 metrics show regression', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 125, // 25% increase - regression
        p95LatencyMs: 200, // 33% increase - regression
        overallThroughput: 100, // no change
      })

      const result = detectRegressions(current, baseline)

      expect(result.severity).toBe('moderate')
      expect(result.hasRegression).toBe(true)
      expect(result.message).toContain('2 metric(s) show regression')
    })

    it('classifies as "severe" when 3+ metrics show regression', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 125, // 25% increase - regression
        p95LatencyMs: 200, // 33% increase - regression
        overallThroughput: 80, // 20% decrease - regression
      })

      const result = detectRegressions(current, baseline)

      expect(result.severity).toBe('severe')
      expect(result.hasRegression).toBe(true)
      expect(result.message).toContain('3 metric(s) show regression')
    })

    it('includes regressed metric names in message', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 125,
        p95LatencyMs: 200,
        overallThroughput: 100,
      })

      const result = detectRegressions(current, baseline)

      expect(result.message).toContain('Average Latency')
      expect(result.message).toContain('P95 Latency')
    })
  })

  // ---------------------------------------------------------------------------
  // Additional Edge Cases
  // ---------------------------------------------------------------------------
  describe('edge cases', () => {
    it('handles very small latency values without division errors', () => {
      const baseline = createMockResult({ avgLatencyMs: 0.1 })
      const current = createMockResult({ avgLatencyMs: 0.15 }) // 50% increase

      const result = detectRegressions(current, baseline)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.changePercent).toBeCloseTo(50, 1)
      expect(avgMetric?.isRegression).toBe(true)
    })

    it('handles very large latency values', () => {
      const baseline = createMockResult({ avgLatencyMs: 10000 })
      const current = createMockResult({ avgLatencyMs: 12500 }) // 25% increase

      const result = detectRegressions(current, baseline)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.changePercent).toBeCloseTo(25, 1)
      expect(avgMetric?.isRegression).toBe(true)
    })

    it('calculates correct change values', () => {
      const baseline = createMockResult({
        avgLatencyMs: 100,
        p95LatencyMs: 150,
        overallThroughput: 100,
      })
      const current = createMockResult({
        avgLatencyMs: 120,
        p95LatencyMs: 180,
        overallThroughput: 90,
      })

      const result = detectRegressions(current, baseline)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.baseline).toBe(100)
      expect(avgMetric?.current).toBe(120)
      expect(avgMetric?.change).toBe(20) // current - baseline
      expect(avgMetric?.changePercent).toBe(20)

      const throughputMetric = result.metrics.find((m) => m.name === 'Throughput')
      expect(throughputMetric?.baseline).toBe(100)
      expect(throughputMetric?.current).toBe(90)
      expect(throughputMetric?.change).toBe(-10)
      expect(throughputMetric?.changePercent).toBe(-10)
    })

    it('sets version strings in result', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 100 })

      const result = detectRegressions(current, baseline)

      expect(result.baselineVersion).toBe('baseline')
      expect(result.currentVersion).toBe('current')
    })

    it('includes threshold values in metric results', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 125 })

      const result = detectRegressions(current, baseline)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.threshold).toBe(20) // Default threshold for latencyP50
    })

    it('uses custom thresholds when provided', () => {
      const baseline = createMockResult({ avgLatencyMs: 100 })
      const current = createMockResult({ avgLatencyMs: 125 }) // 25% increase

      const customThresholds: RegressionThresholds = {
        latencyP50: 30, // More lenient
        latencyP95: 25,
        latencyP99: 30,
        coldStartOverhead: 50,
        cacheHitRate: -10,
        throughput: -15,
      }

      const result = detectRegressions(current, baseline, customThresholds)

      const avgMetric = result.metrics.find((m) => m.name === 'Average Latency')
      expect(avgMetric?.isRegression).toBe(false) // 25% < 30% threshold
      expect(avgMetric?.threshold).toBe(30)
    })
  })

  // ---------------------------------------------------------------------------
  // Cold Start Regression Detection
  // ---------------------------------------------------------------------------
  describe('cold start overhead regression detection', () => {
    it('detects cold start overhead regression above 50% threshold', () => {
      const baseline = createMockResult({ coldStartOverheadMs: 100 })
      const current = createMockResult({ coldStartOverheadMs: 160 }) // 60% increase

      const result = detectRegressions(current, baseline)

      const coldStartMetric = result.metrics.find((m) => m.name === 'Cold Start Overhead')
      expect(coldStartMetric).toBeDefined()
      expect(coldStartMetric?.isRegression).toBe(true)
      expect(coldStartMetric?.changePercent).toBeCloseTo(60, 1)
    })

    it('does not flag cold start overhead at threshold', () => {
      const baseline = createMockResult({ coldStartOverheadMs: 100 })
      const current = createMockResult({ coldStartOverheadMs: 150 }) // Exactly 50% increase

      const result = detectRegressions(current, baseline)

      const coldStartMetric = result.metrics.find((m) => m.name === 'Cold Start Overhead')
      expect(coldStartMetric?.isRegression).toBe(false)
    })
  })
})

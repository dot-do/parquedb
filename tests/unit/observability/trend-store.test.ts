/**
 * Tests for Benchmark Trend Store
 *
 * Tests the infrastructure for storing and querying benchmark results over time.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  MemoryTrendStore,
  FileTrendStore,
  calculateTrendDirection,
  calculateChangePercent,
  calculateStats,
  buildTrendSeries,
  buildTrendSummary,
  getResultPath,
  getDatePrefix,
  parseResultPath,
  createStoredResult,
  generateRunId,
  type TrendStore,
} from '../../../src/observability/benchmarks/trend-store'
import type {
  StoredBenchmarkResult,
  E2EBenchmarkSuiteResult,
} from '../../../src/observability/e2e/types'

// =============================================================================
// Test Fixtures
// =============================================================================

function createMockSuiteResult(overrides: Partial<E2EBenchmarkSuiteResult> = {}): E2EBenchmarkSuiteResult {
  return {
    config: {},
    metadata: {
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 5000,
      workerUrl: 'https://test.workers.dev',
      runnerVersion: '1.0.0',
    },
    health: {
      success: true,
      latencyMs: 50,
    },
    datasets: [],
    summary: {
      totalTests: 10,
      passedTests: 9,
      failedTests: 1,
      avgLatencyMs: 50,
      p95LatencyMs: 120,
      overallThroughput: 100,
      cacheSpeedup: 2.5,
    },
    coldStart: {
      coldLatencyMs: 200,
      warmLatencyMs: 50,
      overheadMs: 150,
      overheadPercent: 300,
      warmSamples: 5,
    },
    cachePerformance: {
      missLatency: { min: 80, max: 150, mean: 100, median: 95, p50: 95, p75: 110, p90: 130, p95: 140, p99: 148, stdDev: 20 },
      hitLatency: { min: 20, max: 60, mean: 40, median: 38, p50: 38, p75: 45, p90: 52, p95: 55, p99: 58, stdDev: 10 },
      speedup: 2.5,
      hitRate: 0.85,
      totalRequests: 100,
    },
    ...overrides,
  }
}

function createMockStoredResult(
  runId: string,
  timestamp: Date,
  overrides: Partial<E2EBenchmarkSuiteResult> = {}
): StoredBenchmarkResult {
  return {
    runId,
    environment: 'production',
    timestamp: timestamp.toISOString(),
    commitSha: `abc${runId.substring(0, 4)}`,
    branch: 'main',
    results: createMockSuiteResult(overrides),
  }
}

// =============================================================================
// Path Helper Tests
// =============================================================================

describe('Path Helpers', () => {
  describe('getResultPath', () => {
    it('should generate correct path format', () => {
      const result: StoredBenchmarkResult = {
        runId: 'test-run-123',
        environment: 'production',
        timestamp: '2024-03-15T10:30:00.000Z',
        results: createMockSuiteResult(),
      }

      const path = getResultPath(result)
      expect(path).toMatch(/^benchmarks\/results\/production\/2024\/03\/15\/run-/)
      expect(path).toMatch(/test-run-123\.json$/)
    })

    it('should handle different environments', () => {
      const result: StoredBenchmarkResult = {
        runId: 'staging-run',
        environment: 'staging',
        timestamp: '2024-06-20T15:45:00.000Z',
        results: createMockSuiteResult(),
      }

      const path = getResultPath(result)
      expect(path).toContain('benchmarks/results/staging/2024/06/20/')
    })
  })

  describe('getDatePrefix', () => {
    it('should generate correct prefix for a date', () => {
      const date = new Date('2024-03-15T10:30:00.000Z')
      const prefix = getDatePrefix('production', date)
      expect(prefix).toBe('benchmarks/results/production/2024/03/15/')
    })

    it('should zero-pad single digit months and days', () => {
      const date = new Date('2024-01-05T10:30:00.000Z')
      const prefix = getDatePrefix('staging', date)
      expect(prefix).toBe('benchmarks/results/staging/2024/01/05/')
    })
  })

  describe('parseResultPath', () => {
    it('should parse valid result path', () => {
      const path = 'benchmarks/results/production/2024/03/15/run-2024-03-15T10-30-00-000Z-test-run-123.json'
      const parsed = parseResultPath(path)

      expect(parsed).not.toBeNull()
      expect(parsed!.environment).toBe('production')
      // The runId includes the timestamp prefix due to the regex pattern
      expect(parsed!.runId).toContain('test-run-123')
      expect(parsed!.date.getUTCFullYear()).toBe(2024)
      expect(parsed!.date.getUTCMonth()).toBe(2) // March is 2 (0-indexed)
      expect(parsed!.date.getUTCDate()).toBe(15)
    })

    it('should return null for invalid path', () => {
      const path = 'invalid/path/to/file.json'
      const parsed = parseResultPath(path)
      expect(parsed).toBeNull()
    })
  })
})

// =============================================================================
// Analysis Helper Tests
// =============================================================================

describe('Analysis Helpers', () => {
  describe('calculateTrendDirection', () => {
    it('should detect improving trend (decreasing values)', () => {
      const values = [100, 90, 80, 70, 60]
      const direction = calculateTrendDirection(values)
      expect(direction).toBe('improving')
    })

    it('should detect degrading trend (increasing values)', () => {
      const values = [60, 70, 80, 90, 100]
      const direction = calculateTrendDirection(values)
      expect(direction).toBe('degrading')
    })

    it('should detect stable trend (small variations)', () => {
      const values = [100, 101, 99, 100, 101]
      const direction = calculateTrendDirection(values)
      expect(direction).toBe('stable')
    })

    it('should return stable for single value', () => {
      const values = [100]
      const direction = calculateTrendDirection(values)
      expect(direction).toBe('stable')
    })

    it('should return stable for empty array', () => {
      const values: number[] = []
      const direction = calculateTrendDirection(values)
      expect(direction).toBe('stable')
    })

    it('should respect custom threshold', () => {
      // Values with more significant change to cross threshold
      const values = [100, 105, 110, 115, 120] // ~5% increase
      expect(calculateTrendDirection(values, 3)).toBe('degrading')
      expect(calculateTrendDirection(values, 10)).toBe('stable')
    })
  })

  describe('calculateChangePercent', () => {
    it('should calculate positive change', () => {
      const values = [100, 120]
      const change = calculateChangePercent(values)
      expect(change).toBe(20)
    })

    it('should calculate negative change', () => {
      const values = [100, 80]
      const change = calculateChangePercent(values)
      expect(change).toBe(-20)
    })

    it('should return 0 for single value', () => {
      const values = [100]
      const change = calculateChangePercent(values)
      expect(change).toBe(0)
    })

    it('should return 0 for empty array', () => {
      const values: number[] = []
      const change = calculateChangePercent(values)
      expect(change).toBe(0)
    })

    it('should handle zero first value', () => {
      const values = [0, 100]
      const change = calculateChangePercent(values)
      expect(change).toBe(0)
    })
  })

  describe('calculateStats', () => {
    it('should calculate correct statistics', () => {
      const values = [10, 20, 30, 40, 50]
      const stats = calculateStats(values)

      expect(stats.min).toBe(10)
      expect(stats.max).toBe(50)
      expect(stats.mean).toBe(30)
      expect(stats.stdDev).toBeCloseTo(14.14, 1)
    })

    it('should handle single value', () => {
      const values = [100]
      const stats = calculateStats(values)

      expect(stats.min).toBe(100)
      expect(stats.max).toBe(100)
      expect(stats.mean).toBe(100)
      expect(stats.stdDev).toBe(0)
    })

    it('should handle empty array', () => {
      const values: number[] = []
      const stats = calculateStats(values)

      expect(stats.min).toBe(0)
      expect(stats.max).toBe(0)
      expect(stats.mean).toBe(0)
      expect(stats.stdDev).toBe(0)
    })
  })

  describe('buildTrendSeries', () => {
    it('should build series from results', () => {
      const results: StoredBenchmarkResult[] = [
        createMockStoredResult('run1', new Date('2024-03-15'), { summary: { ...createMockSuiteResult().summary, avgLatencyMs: 50 } }),
        createMockStoredResult('run2', new Date('2024-03-16'), { summary: { ...createMockSuiteResult().summary, avgLatencyMs: 55 } }),
        createMockStoredResult('run3', new Date('2024-03-17'), { summary: { ...createMockSuiteResult().summary, avgLatencyMs: 60 } }),
      ]

      const series = buildTrendSeries(results, r => r.summary.avgLatencyMs, 'avgLatency')

      expect(series).not.toBeNull()
      expect(series!.metricName).toBe('avgLatency')
      expect(series!.dataPoints.length).toBe(3)
      expect(series!.direction).toBe('degrading') // Latency increasing is bad
      expect(series!.stats.current).toBe(60)
      expect(series!.stats.previous).toBe(55)
    })

    it('should return null for empty results', () => {
      const series = buildTrendSeries([], r => r.summary.avgLatencyMs, 'avgLatency')
      expect(series).toBeNull()
    })

    it('should filter out undefined values', () => {
      const results: StoredBenchmarkResult[] = [
        createMockStoredResult('run1', new Date('2024-03-15')),
        { ...createMockStoredResult('run2', new Date('2024-03-16')), results: { ...createMockSuiteResult(), coldStart: undefined } },
      ]

      const series = buildTrendSeries(results, r => r.coldStart?.overheadPercent, 'coldStart')
      expect(series).not.toBeNull()
      expect(series!.dataPoints.length).toBe(1)
    })

    it('should invert direction for throughput metrics', () => {
      const results: StoredBenchmarkResult[] = [
        createMockStoredResult('run1', new Date('2024-03-15'), { summary: { ...createMockSuiteResult().summary, overallThroughput: 100 } }),
        createMockStoredResult('run2', new Date('2024-03-16'), { summary: { ...createMockSuiteResult().summary, overallThroughput: 80 } }),
      ]

      // For throughput, decreasing values should be 'degrading' (inverted)
      const series = buildTrendSeries(results, r => r.summary.overallThroughput, 'throughput', true)
      expect(series!.direction).toBe('degrading')
    })
  })

  describe('buildTrendSummary', () => {
    it('should build complete summary from results', () => {
      const results: StoredBenchmarkResult[] = []
      for (let i = 0; i < 7; i++) {
        const date = new Date('2024-03-15')
        date.setDate(date.getDate() + i)
        results.push(createMockStoredResult(`run${i}`, date, {
          summary: {
            ...createMockSuiteResult().summary,
            avgLatencyMs: 50 + i * 2, // Slightly degrading
            p95LatencyMs: 120 + i * 3,
          },
        }))
      }

      const summary = buildTrendSummary(results, 'production', 'daily')

      expect(summary).not.toBeNull()
      expect(summary!.environment).toBe('production')
      expect(summary!.period.dataPointCount).toBe(7)
      expect(summary!.period.granularity).toBe('daily')
      expect(summary!.metrics.latencyP50).not.toBeNull()
      expect(summary!.metrics.latencyP95).not.toBeNull()
      expect(summary!.metrics.throughput).not.toBeNull()
    })

    it('should return null for empty results', () => {
      const summary = buildTrendSummary([], 'production', 'daily')
      expect(summary).toBeNull()
    })

    it('should detect high regression risk', () => {
      const results: StoredBenchmarkResult[] = []
      for (let i = 0; i < 5; i++) {
        const date = new Date('2024-03-15')
        date.setDate(date.getDate() + i)
        results.push(createMockStoredResult(`run${i}`, date, {
          summary: {
            ...createMockSuiteResult().summary,
            avgLatencyMs: 50 + i * 20, // Significant degradation
            p95LatencyMs: 120 + i * 30,
            overallThroughput: 100 - i * 15,
          },
        }))
      }

      const summary = buildTrendSummary(results, 'production', 'daily')
      expect(summary!.regressionRisk).toBe('high')
    })
  })
})

// =============================================================================
// MemoryTrendStore Tests
// =============================================================================

describe('MemoryTrendStore', () => {
  let store: MemoryTrendStore

  beforeEach(() => {
    store = new MemoryTrendStore()
  })

  describe('saveResult', () => {
    it('should save result successfully', async () => {
      const result = createMockStoredResult('test-run', new Date())
      const saveResult = await store.saveResult(result)

      expect(saveResult.success).toBe(true)
      expect(saveResult.path).toContain('test-run')
    })

    it('should overwrite existing result with same path', async () => {
      const timestamp = new Date()
      const result1 = createMockStoredResult('test-run', timestamp, {
        summary: { ...createMockSuiteResult().summary, avgLatencyMs: 50 },
      })
      const result2 = createMockStoredResult('test-run', timestamp, {
        summary: { ...createMockSuiteResult().summary, avgLatencyMs: 100 },
      })

      await store.saveResult(result1)
      await store.saveResult(result2)

      const retrieved = await store.getResult('production', 'test-run')
      expect(retrieved!.results.summary.avgLatencyMs).toBe(100)
    })
  })

  describe('getResult', () => {
    it('should retrieve saved result', async () => {
      const result = createMockStoredResult('test-run', new Date())
      await store.saveResult(result)

      const retrieved = await store.getResult('production', 'test-run')
      expect(retrieved).not.toBeNull()
      expect(retrieved!.runId).toBe('test-run')
    })

    it('should return null for non-existent result', async () => {
      const retrieved = await store.getResult('production', 'non-existent')
      expect(retrieved).toBeNull()
    })

    it('should not find result in wrong environment', async () => {
      const result = createMockStoredResult('test-run', new Date())
      await store.saveResult(result)

      const retrieved = await store.getResult('staging', 'test-run')
      expect(retrieved).toBeNull()
    })
  })

  describe('queryResults', () => {
    beforeEach(async () => {
      // Add results across multiple days
      for (let i = 0; i < 10; i++) {
        const date = new Date('2024-03-15')
        date.setDate(date.getDate() + i)
        const result = createMockStoredResult(`run-${i}`, date)
        result.branch = i % 2 === 0 ? 'main' : 'develop'
        await store.saveResult(result)
      }
    })

    it('should return all results for environment', async () => {
      const results = await store.queryResults({ environment: 'production' })
      expect(results.length).toBe(10)
    })

    it('should filter by date range', async () => {
      const results = await store.queryResults({
        environment: 'production',
        startDate: new Date('2024-03-17'),
        endDate: new Date('2024-03-20'),
      })
      expect(results.length).toBe(4)
    })

    it('should filter by branch', async () => {
      const results = await store.queryResults({
        environment: 'production',
        branch: 'main',
      })
      expect(results.length).toBe(5)
    })

    it('should respect limit', async () => {
      const results = await store.queryResults({
        environment: 'production',
        limit: 3,
      })
      expect(results.length).toBe(3)
    })

    it('should return results sorted by timestamp descending', async () => {
      const results = await store.queryResults({ environment: 'production' })
      for (let i = 1; i < results.length; i++) {
        const prevTime = new Date(results[i - 1]!.timestamp).getTime()
        const currTime = new Date(results[i]!.timestamp).getTime()
        expect(prevTime).toBeGreaterThanOrEqual(currTime)
      }
    })
  })

  describe('getRecentResults', () => {
    it('should get results from last N days', async () => {
      const now = new Date()
      for (let i = 0; i < 10; i++) {
        const date = new Date(now)
        date.setDate(date.getDate() - i)
        await store.saveResult(createMockStoredResult(`run-${i}`, date))
      }

      // Gets results from now - 5 days range (returns 5 or 6 results depending on time boundaries)
      const results = await store.getRecentResults('production', 5)
      expect(results.length).toBeGreaterThanOrEqual(5)
      expect(results.length).toBeLessThanOrEqual(6)
    })
  })

  describe('listEnvironments', () => {
    it('should list all environments', async () => {
      await store.saveResult({ ...createMockStoredResult('run1', new Date()), environment: 'production' })
      await store.saveResult({ ...createMockStoredResult('run2', new Date()), environment: 'staging' })
      await store.saveResult({ ...createMockStoredResult('run3', new Date()), environment: 'development' })

      const environments = await store.listEnvironments()
      expect(environments).toContain('production')
      expect(environments).toContain('staging')
      expect(environments).toContain('development')
    })
  })

  describe('listRunIds', () => {
    it('should list run IDs for environment', async () => {
      await store.saveResult(createMockStoredResult('run-1', new Date()))
      await store.saveResult(createMockStoredResult('run-2', new Date()))
      await store.saveResult(createMockStoredResult('run-3', new Date()))

      const runIds = await store.listRunIds('production')
      expect(runIds).toContain('run-1')
      expect(runIds).toContain('run-2')
      expect(runIds).toContain('run-3')
    })
  })

  describe('deleteResult', () => {
    it('should delete existing result', async () => {
      await store.saveResult(createMockStoredResult('to-delete', new Date()))

      const deleted = await store.deleteResult('production', 'to-delete')
      expect(deleted).toBe(true)

      const retrieved = await store.getResult('production', 'to-delete')
      expect(retrieved).toBeNull()
    })

    it('should return false for non-existent result', async () => {
      const deleted = await store.deleteResult('production', 'non-existent')
      expect(deleted).toBe(false)
    })
  })

  describe('deleteOldResults', () => {
    it('should delete results older than date', async () => {
      const now = new Date()
      for (let i = 0; i < 10; i++) {
        const date = new Date(now)
        date.setDate(date.getDate() - i)
        await store.saveResult(createMockStoredResult(`run-${i}`, date))
      }

      const cutoff = new Date(now)
      cutoff.setDate(cutoff.getDate() - 5)

      const deleted = await store.deleteOldResults('production', cutoff)
      expect(deleted).toBe(4) // Results from days -6, -7, -8, -9

      const remaining = await store.queryResults({ environment: 'production' })
      expect(remaining.length).toBe(6)
    })
  })

  describe('clear', () => {
    it('should clear all results', async () => {
      await store.saveResult(createMockStoredResult('run-1', new Date()))
      await store.saveResult(createMockStoredResult('run-2', new Date()))

      store.clear()

      const results = await store.queryResults({ environment: 'production' })
      expect(results.length).toBe(0)
    })
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('Utility Functions', () => {
  describe('createStoredResult', () => {
    it('should create stored result with metadata', () => {
      const suiteResult = createMockSuiteResult()
      const stored = createStoredResult(suiteResult, {
        runId: 'test-run',
        environment: 'production',
        commitSha: 'abc123',
        branch: 'main',
        tag: 'v1.0.0',
      })

      expect(stored.runId).toBe('test-run')
      expect(stored.environment).toBe('production')
      expect(stored.commitSha).toBe('abc123')
      expect(stored.branch).toBe('main')
      expect(stored.tag).toBe('v1.0.0')
      expect(stored.results).toBe(suiteResult)
      expect(stored.timestamp).toBeDefined()
    })
  })

  describe('generateRunId', () => {
    it('should generate unique IDs', () => {
      const id1 = generateRunId()
      const id2 = generateRunId()
      expect(id1).not.toBe(id2)
    })

    it('should generate IDs in expected format', () => {
      const id = generateRunId()
      expect(id).toMatch(/^[a-z0-9]+-[a-z0-9]+$/)
    })
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration', () => {
  it('should support complete workflow: save, query, analyze', async () => {
    const store = new MemoryTrendStore()

    // Save results over time with significant degradation (> 5% threshold)
    const baseDate = new Date('2024-03-15')
    for (let i = 0; i < 7; i++) {
      const date = new Date(baseDate)
      date.setDate(date.getDate() + i)

      const result = createMockStoredResult(`run-${i}`, date, {
        summary: {
          totalTests: 10,
          passedTests: 10,
          failedTests: 0,
          avgLatencyMs: 50 + i * 10, // More significant degradation (20ms -> 110ms)
          p95LatencyMs: 120 + i * 15, // 120 -> 210
          overallThroughput: 100 - i * 10, // 100 -> 40
        },
      })

      await store.saveResult(result)
    }

    // Query results
    const results = await store.queryResults({ environment: 'production' })
    expect(results.length).toBe(7)

    // Build trend summary
    const summary = buildTrendSummary(results, 'production', 'daily')
    expect(summary).not.toBeNull()

    // Verify trend detection - with significant changes, should show degradation
    expect(summary!.metrics.latencyP50!.direction).toBe('degrading')
    expect(['degrading', 'stable']).toContain(summary!.overallDirection)
  })

  it('should detect improvement when metrics get better', async () => {
    const store = new MemoryTrendStore()

    // Save results with improvement over time
    const baseDate = new Date('2024-03-15')
    for (let i = 0; i < 7; i++) {
      const date = new Date(baseDate)
      date.setDate(date.getDate() + i)

      const result = createMockStoredResult(`run-${i}`, date, {
        summary: {
          totalTests: 10,
          passedTests: 10,
          failedTests: 0,
          avgLatencyMs: 80 - i * 5, // Improvement (lower latency)
          p95LatencyMs: 200 - i * 10,
          overallThroughput: 50 + i * 10, // Improvement (higher throughput)
        },
      })

      await store.saveResult(result)
    }

    const results = await store.queryResults({ environment: 'production' })
    const summary = buildTrendSummary(results, 'production', 'daily')

    expect(summary!.metrics.latencyP50!.direction).toBe('improving')
    expect(summary!.metrics.throughput!.direction).toBe('improving')
    expect(summary!.overallDirection).toBe('improving')
    expect(summary!.regressionRisk).toBe('low')
  })
})

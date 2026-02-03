/**
 * Prometheus Metrics Tests
 *
 * Tests for the PrometheusMetrics class and related utilities.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  PrometheusMetrics,
  getGlobalMetrics,
  resetGlobalMetrics,
  createPrometheusMetrics,
  incrementCounter,
  setGauge,
  observeHistogram,
  startTimer,
  exportMetrics,
  PARQUEDB_METRICS,
  DEFAULT_DURATION_BUCKETS,
} from '../../../src/observability/prometheus'

describe('PrometheusMetrics', () => {
  let metrics: PrometheusMetrics

  beforeEach(() => {
    metrics = new PrometheusMetrics()
  })

  afterEach(() => {
    resetGlobalMetrics()
  })

  describe('Counter Operations', () => {
    it('should increment a counter', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(1)
    })

    it('should increment a counter by a specific value', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' }, 5)
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(5)
    })

    it('should accumulate counter increments', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(3)
    })

    it('should track counters with different labels separately', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.increment('requests_total', { method: 'POST', namespace: 'users', status: '201' })
      metrics.increment('requests_total', { method: 'GET', namespace: 'posts', status: '200' })

      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(1)
      expect(metrics.getCounter('requests_total', { method: 'POST', namespace: 'users', status: '201' })).toBe(1)
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'posts', status: '200' })).toBe(1)
    })

    it('should return 0 for non-existent counter', () => {
      expect(metrics.getCounter('nonexistent_counter', {})).toBe(0)
    })
  })

  describe('Gauge Operations', () => {
    it('should set a gauge value', () => {
      metrics.set('entities_total', 1000, { namespace: 'users' })
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1000)
    })

    it('should overwrite gauge value on subsequent sets', () => {
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.set('entities_total', 1500, { namespace: 'users' })
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1500)
    })

    it('should increment a gauge', () => {
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.incrementGauge('entities_total', { namespace: 'users' }, 100)
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1100)
    })

    it('should decrement a gauge', () => {
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.decrementGauge('entities_total', { namespace: 'users' }, 100)
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(900)
    })

    it('should track gauges with different labels separately', () => {
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.set('entities_total', 500, { namespace: 'posts' })

      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1000)
      expect(metrics.getGauge('entities_total', { namespace: 'posts' })).toBe(500)
    })

    it('should return 0 for non-existent gauge', () => {
      expect(metrics.getGauge('nonexistent_gauge', {})).toBe(0)
    })
  })

  describe('Histogram Operations', () => {
    it('should observe histogram values', () => {
      metrics.observe('request_duration_seconds', 0.125, { method: 'GET', namespace: 'users' })
      const output = metrics.export()

      expect(output).toContain('parquedb_request_duration_seconds_sum')
      expect(output).toContain('parquedb_request_duration_seconds_count')
      expect(output).toContain('parquedb_request_duration_seconds_bucket')
    })

    it('should accumulate histogram observations', () => {
      metrics.observe('request_duration_seconds', 0.1, { method: 'GET', namespace: 'users' })
      metrics.observe('request_duration_seconds', 0.2, { method: 'GET', namespace: 'users' })
      metrics.observe('request_duration_seconds', 0.3, { method: 'GET', namespace: 'users' })

      const output = metrics.export()
      // Sum should be 0.6
      expect(output).toContain('parquedb_request_duration_seconds_sum{method="GET",namespace="users"} 0.6')
      // Count should be 3
      expect(output).toContain('parquedb_request_duration_seconds_count{method="GET",namespace="users"} 3')
    })

    it('should place values in correct buckets', () => {
      // Observe values that fall into different buckets
      metrics.observe('request_duration_seconds', 0.005, { method: 'GET', namespace: 'users' }) // <= 0.005
      metrics.observe('request_duration_seconds', 0.05, { method: 'GET', namespace: 'users' }) // <= 0.05
      metrics.observe('request_duration_seconds', 0.5, { method: 'GET', namespace: 'users' }) // <= 0.5

      const output = metrics.export()
      // Bucket le=0.005 should have 1 (cumulative)
      expect(output).toContain('le="0.005"} 1')
      // Bucket le=0.05 should have 2 (cumulative)
      expect(output).toContain('le="0.05"} 2')
      // Bucket le=0.5 should have 3 (cumulative)
      expect(output).toContain('le="0.5"} 3')
    })

    it('should include +Inf bucket with total count', () => {
      metrics.observe('request_duration_seconds', 100, { method: 'GET', namespace: 'users' })
      const output = metrics.export()
      expect(output).toContain('le="+Inf"} 1')
    })
  })

  describe('Timer Operations', () => {
    it('should time a synchronous function', async () => {
      const result = await metrics.time('request_duration_seconds', () => {
        // Simulate some work
        let sum = 0
        for (let i = 0; i < 1000; i++) {
          sum += i
        }
        return sum
      }, { method: 'GET', namespace: 'users' })

      expect(result).toBe(499500)
      const output = metrics.export()
      expect(output).toContain('parquedb_request_duration_seconds_count{method="GET",namespace="users"} 1')
    })

    it('should time an async function', async () => {
      const result = await metrics.time('request_duration_seconds', async () => {
        await new Promise(resolve => setTimeout(resolve, 10))
        return 'done'
      }, { method: 'GET', namespace: 'users' })

      expect(result).toBe('done')
      const output = metrics.export()
      expect(output).toContain('parquedb_request_duration_seconds_count{method="GET",namespace="users"} 1')
    })

    it('should use startTimer for manual timing', async () => {
      const timer = metrics.startTimer('request_duration_seconds', { method: 'GET', namespace: 'users' })
      await new Promise(resolve => setTimeout(resolve, 10))
      const duration = timer.end()

      expect(duration).toBeGreaterThan(0.009) // At least 9ms
      const output = metrics.export()
      expect(output).toContain('parquedb_request_duration_seconds_count{method="GET",namespace="users"} 1')
    })
  })

  describe('Export', () => {
    it('should export metrics in Prometheus format', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.observe('request_duration_seconds', 0.125, { method: 'GET', namespace: 'users' })

      const output = metrics.export()

      // Check counter export
      expect(output).toContain('# HELP parquedb_requests_total')
      expect(output).toContain('# TYPE parquedb_requests_total counter')
      expect(output).toContain('parquedb_requests_total{method="GET",namespace="users",status="200"} 1')

      // Check gauge export
      expect(output).toContain('# HELP parquedb_entities_total')
      expect(output).toContain('# TYPE parquedb_entities_total gauge')
      expect(output).toContain('parquedb_entities_total{namespace="users"} 1000')

      // Check histogram export
      expect(output).toContain('# HELP parquedb_request_duration_seconds')
      expect(output).toContain('# TYPE parquedb_request_duration_seconds histogram')
    })

    it('should escape special characters in labels', () => {
      metrics.increment('requests_total', {
        method: 'GET',
        namespace: 'users',
        status: '200',
        path: '/api/v1/users?name="test"'
      })

      const output = metrics.export()
      expect(output).toContain('path="/api/v1/users?name=\\"test\\""')
    })

    it('should handle empty metrics gracefully', () => {
      const output = metrics.export()
      expect(output).toBe('')
    })
  })

  describe('Reset', () => {
    it('should reset all metrics', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.set('entities_total', 1000, { namespace: 'users' })
      metrics.observe('request_duration_seconds', 0.125, { method: 'GET', namespace: 'users' })

      metrics.reset()

      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(0)
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(0)
      expect(metrics.export()).toBe('')
    })

    it('should reset a specific metric', () => {
      metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      metrics.increment('cache_hits_total', { cache: 'query' })

      metrics.resetMetric('requests_total')

      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(0)
      expect(metrics.getCounter('cache_hits_total', { cache: 'query' })).toBe(1)
    })
  })

  describe('Configuration', () => {
    it('should apply default labels to all metrics', () => {
      const customMetrics = new PrometheusMetrics({
        defaultLabels: { environment: 'test', version: '1.0.0' },
      })

      customMetrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      const output = customMetrics.export()

      expect(output).toContain('environment="test"')
      expect(output).toContain('version="1.0.0"')
    })

    it('should use custom prefix', () => {
      const customMetrics = new PrometheusMetrics({
        prefix: 'myapp',
      })

      customMetrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
      const output = customMetrics.export()

      expect(output).toContain('myapp_requests_total')
      expect(output).not.toContain('parquedb_requests_total')
    })

    it('should use custom histogram buckets', () => {
      const customMetrics = new PrometheusMetrics({
        defaultBuckets: [0.01, 0.1, 1, 10],
      })

      customMetrics.register({
        name: 'custom_histogram',
        help: 'Custom histogram',
        type: 'histogram',
      })

      customMetrics.observe('custom_histogram', 0.5, {})
      const output = customMetrics.export()

      expect(output).toContain('le="0.01"')
      expect(output).toContain('le="0.1"')
      expect(output).toContain('le="1"')
      expect(output).toContain('le="10"')
      expect(output).not.toContain('le="0.001"')
    })
  })
})

describe('Global Metrics', () => {
  beforeEach(() => {
    resetGlobalMetrics()
  })

  afterEach(() => {
    resetGlobalMetrics()
  })

  it('should return the same instance on multiple calls', () => {
    const metrics1 = getGlobalMetrics()
    const metrics2 = getGlobalMetrics()
    expect(metrics1).toBe(metrics2)
  })

  it('should use convenience functions with global instance', () => {
    incrementCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })
    setGauge('entities_total', 1000, { namespace: 'users' })
    observeHistogram('request_duration_seconds', 0.125, { method: 'GET', namespace: 'users' })

    const output = exportMetrics()
    expect(output).toContain('parquedb_requests_total')
    expect(output).toContain('parquedb_entities_total')
    expect(output).toContain('parquedb_request_duration_seconds')
  })

  it('should reset global metrics', () => {
    incrementCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })
    resetGlobalMetrics()

    const output = exportMetrics()
    expect(output).toBe('')
  })

  it('should create independent instances with createPrometheusMetrics', () => {
    const metrics1 = createPrometheusMetrics()
    const metrics2 = createPrometheusMetrics()

    metrics1.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })

    expect(metrics1.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(1)
    expect(metrics2.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(0)
  })

  it('should use startTimer convenience function', async () => {
    const timer = startTimer('request_duration_seconds', { method: 'GET', namespace: 'users' })
    await new Promise(resolve => setTimeout(resolve, 5))
    timer.end()

    const output = exportMetrics()
    expect(output).toContain('parquedb_request_duration_seconds_count{method="GET",namespace="users"} 1')
  })
})

describe('Standard Metrics Definitions', () => {
  it('should define all standard metrics', () => {
    const metricNames = PARQUEDB_METRICS.map(m => m.name)

    expect(metricNames).toContain('requests_total')
    expect(metricNames).toContain('request_duration_seconds')
    expect(metricNames).toContain('entities_total')
    expect(metricNames).toContain('storage_bytes')
    expect(metricNames).toContain('compaction_runs_total')
    expect(metricNames).toContain('cache_hits_total')
    expect(metricNames).toContain('cache_misses_total')
    expect(metricNames).toContain('write_operations_total')
    expect(metricNames).toContain('read_operations_total')
    expect(metricNames).toContain('errors_total')
  })

  it('should have appropriate types for each metric', () => {
    const requestsMetric = PARQUEDB_METRICS.find(m => m.name === 'requests_total')
    expect(requestsMetric?.type).toBe('counter')

    const entitiesMetric = PARQUEDB_METRICS.find(m => m.name === 'entities_total')
    expect(entitiesMetric?.type).toBe('gauge')

    const durationMetric = PARQUEDB_METRICS.find(m => m.name === 'request_duration_seconds')
    expect(durationMetric?.type).toBe('histogram')
  })

  it('should have default duration buckets', () => {
    expect(DEFAULT_DURATION_BUCKETS).toContain(0.001) // 1ms
    expect(DEFAULT_DURATION_BUCKETS).toContain(0.01) // 10ms
    expect(DEFAULT_DURATION_BUCKETS).toContain(0.1) // 100ms
    expect(DEFAULT_DURATION_BUCKETS).toContain(1) // 1s
    expect(DEFAULT_DURATION_BUCKETS).toContain(10) // 10s
  })
})

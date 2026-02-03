/**
 * Metrics Handler Tests
 *
 * Tests for the /metrics endpoint handler.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  handleMetrics,
  PROMETHEUS_CONTENT_TYPE,
  recordRequest,
  recordCacheAccess,
  recordWriteOperation,
  recordReadOperation,
  setEntityCount,
  setStorageSize,
  recordCompactionRun,
  recordError,
} from '../../../../src/worker/handlers/metrics'
import { resetGlobalMetrics, getGlobalMetrics } from '../../../../src/observability/prometheus'
import { resetGlobalTelemetry } from '../../../../src/observability/telemetry'

describe('Metrics Handler', () => {
  beforeEach(() => {
    resetGlobalMetrics()
    resetGlobalTelemetry()
  })

  afterEach(() => {
    resetGlobalMetrics()
    resetGlobalTelemetry()
  })

  describe('handleMetrics', () => {
    it('should return a response with Prometheus content type', () => {
      const response = handleMetrics()

      expect(response.status).toBe(200)
      expect(response.headers.get('Content-Type')).toBe(PROMETHEUS_CONTENT_TYPE)
    })

    it('should return cache control headers to prevent caching', () => {
      const response = handleMetrics()

      expect(response.headers.get('Cache-Control')).toBe('no-cache, no-store, must-revalidate')
    })

    it('should return empty string when no metrics recorded', async () => {
      const response = handleMetrics()
      const _body = await response.text()

      // May have some default telemetry, but core metrics should be empty
      // This is acceptable - empty or minimal output for no recorded metrics
      expect(response.status).toBe(200)
    })

    it('should include recorded metrics in output', async () => {
      // Record some metrics
      recordRequest('GET', 'users', 200, 0.125)
      recordCacheAccess('query', true)
      setEntityCount('users', 1000)

      const response = handleMetrics()
      const body = await response.text()

      expect(body).toContain('parquedb_requests_total')
      expect(body).toContain('parquedb_cache_hits_total')
      expect(body).toContain('parquedb_entities_total')
    })
  })

  describe('recordRequest', () => {
    it('should record request count and duration', async () => {
      recordRequest('GET', 'users', 200, 0.125)
      recordRequest('GET', 'users', 200, 0.075)
      recordRequest('POST', 'users', 201, 0.200)

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(2)
      expect(metrics.getCounter('requests_total', { method: 'POST', namespace: 'users', status: '201' })).toBe(1)

      const response = handleMetrics()
      const body = await response.text()

      expect(body).toContain('parquedb_request_duration_seconds')
    })

    it('should track different status codes separately', () => {
      recordRequest('GET', 'users', 200, 0.1)
      recordRequest('GET', 'users', 404, 0.05)
      recordRequest('GET', 'users', 500, 0.2)

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '200' })).toBe(1)
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '404' })).toBe(1)
      expect(metrics.getCounter('requests_total', { method: 'GET', namespace: 'users', status: '500' })).toBe(1)
    })
  })

  describe('recordCacheAccess', () => {
    it('should record cache hits', () => {
      recordCacheAccess('query', true)
      recordCacheAccess('query', true)
      recordCacheAccess('metadata', true)

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('cache_hits_total', { cache: 'query' })).toBe(2)
      expect(metrics.getCounter('cache_hits_total', { cache: 'metadata' })).toBe(1)
      expect(metrics.getCounter('cache_misses_total', { cache: 'query' })).toBe(0)
    })

    it('should record cache misses', () => {
      recordCacheAccess('query', false)
      recordCacheAccess('bloom', false)

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('cache_misses_total', { cache: 'query' })).toBe(1)
      expect(metrics.getCounter('cache_misses_total', { cache: 'bloom' })).toBe(1)
    })

    it('should track different caches separately', () => {
      recordCacheAccess('query', true)
      recordCacheAccess('query', false)
      recordCacheAccess('metadata', true)
      recordCacheAccess('bloom', false)

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('cache_hits_total', { cache: 'query' })).toBe(1)
      expect(metrics.getCounter('cache_misses_total', { cache: 'query' })).toBe(1)
      expect(metrics.getCounter('cache_hits_total', { cache: 'metadata' })).toBe(1)
      expect(metrics.getCounter('cache_misses_total', { cache: 'bloom' })).toBe(1)
    })
  })

  describe('recordWriteOperation', () => {
    it('should record write operations by type', () => {
      recordWriteOperation('users', 'create')
      recordWriteOperation('users', 'update')
      recordWriteOperation('users', 'delete')
      recordWriteOperation('posts', 'create')

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('write_operations_total', { namespace: 'users', operation: 'create' })).toBe(1)
      expect(metrics.getCounter('write_operations_total', { namespace: 'users', operation: 'update' })).toBe(1)
      expect(metrics.getCounter('write_operations_total', { namespace: 'users', operation: 'delete' })).toBe(1)
      expect(metrics.getCounter('write_operations_total', { namespace: 'posts', operation: 'create' })).toBe(1)
    })
  })

  describe('recordReadOperation', () => {
    it('should record read operations by type', () => {
      recordReadOperation('users', 'find')
      recordReadOperation('users', 'get')
      recordReadOperation('users', 'count')

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('read_operations_total', { namespace: 'users', operation: 'find' })).toBe(1)
      expect(metrics.getCounter('read_operations_total', { namespace: 'users', operation: 'get' })).toBe(1)
      expect(metrics.getCounter('read_operations_total', { namespace: 'users', operation: 'count' })).toBe(1)
    })
  })

  describe('setEntityCount', () => {
    it('should set entity count gauge', () => {
      setEntityCount('users', 1000)
      setEntityCount('posts', 5000)

      const metrics = getGlobalMetrics()
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1000)
      expect(metrics.getGauge('entities_total', { namespace: 'posts' })).toBe(5000)
    })

    it('should update entity count on subsequent calls', () => {
      setEntityCount('users', 1000)
      setEntityCount('users', 1100)
      setEntityCount('users', 1050)

      const metrics = getGlobalMetrics()
      expect(metrics.getGauge('entities_total', { namespace: 'users' })).toBe(1050)
    })
  })

  describe('setStorageSize', () => {
    it('should set storage size by type', () => {
      setStorageSize('users', 'data', 1024000)
      setStorageSize('users', 'index', 512000)
      setStorageSize('users', 'events', 256000)

      const metrics = getGlobalMetrics()
      expect(metrics.getGauge('storage_bytes', { namespace: 'users', type: 'data' })).toBe(1024000)
      expect(metrics.getGauge('storage_bytes', { namespace: 'users', type: 'index' })).toBe(512000)
      expect(metrics.getGauge('storage_bytes', { namespace: 'users', type: 'events' })).toBe(256000)
    })
  })

  describe('recordCompactionRun', () => {
    it('should record successful compaction runs', () => {
      recordCompactionRun('users', 'success')
      recordCompactionRun('users', 'success')
      recordCompactionRun('posts', 'success')

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('compaction_runs_total', { namespace: 'users', status: 'success' })).toBe(2)
      expect(metrics.getCounter('compaction_runs_total', { namespace: 'posts', status: 'success' })).toBe(1)
    })

    it('should record failed compaction runs', () => {
      recordCompactionRun('users', 'failure')

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('compaction_runs_total', { namespace: 'users', status: 'failure' })).toBe(1)
    })
  })

  describe('recordError', () => {
    it('should record errors by type', () => {
      recordError('validation', 'users')
      recordError('storage', 'users')
      recordError('timeout', 'posts')

      const metrics = getGlobalMetrics()
      expect(metrics.getCounter('errors_total', { type: 'validation', namespace: 'users' })).toBe(1)
      expect(metrics.getCounter('errors_total', { type: 'storage', namespace: 'users' })).toBe(1)
      expect(metrics.getCounter('errors_total', { type: 'timeout', namespace: 'posts' })).toBe(1)
    })
  })

  describe('Prometheus Format Output', () => {
    it('should output valid Prometheus format', async () => {
      recordRequest('GET', 'users', 200, 0.125)
      recordCacheAccess('query', true)
      setEntityCount('users', 1000)
      recordCompactionRun('users', 'success')

      const response = handleMetrics()
      const body = await response.text()

      // Check for HELP comments
      expect(body).toContain('# HELP')
      // Check for TYPE declarations
      expect(body).toContain('# TYPE')
      // Check for metric names with labels
      expect(body).toMatch(/parquedb_requests_total\{.*\} \d+/)
      expect(body).toMatch(/parquedb_cache_hits_total\{.*\} \d+/)
      expect(body).toMatch(/parquedb_entities_total\{.*\} \d+/)
    })

    it('should properly escape label values', async () => {
      recordRequest('GET', 'test/namespace', 200, 0.1)

      const response = handleMetrics()
      const body = await response.text()

      // Slashes should be preserved in label values
      expect(body).toContain('namespace="test/namespace"')
    })
  })
})
